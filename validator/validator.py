"""
Schema Validation Service.

Consumes telemetry.raw, validates against JSON Schema, and routes events
to telemetry.validated or telemetry.dlq with error metadata.
"""

import json
import sys
import time
import threading

import jsonschema
from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition
from prometheus_client import start_http_server

sys.path.insert(0, "/app")
from shared.logging_config import setup_logging
from shared.signals import GracefulShutdown
from validator.config import ValidatorConfig
from validator import metrics

logger = setup_logging("validator")


class SchemaValidator:
    """Validates telemetry events and routes them to validated or DLQ topics."""

    def __init__(self, config: ValidatorConfig):
        self.config = config
        self.shutdown = GracefulShutdown()
        self.shutdown.register()

        # Load JSON Schema
        with open(config.SCHEMA_PATH) as f:
            self.schema = json.load(f)
        self.json_validator = jsonschema.Draft7Validator(self.schema)
        logger.info(f"Loaded schema from {config.SCHEMA_PATH}")

        # Consumer
        self.consumer = Consumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": config.CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "fetch.max.bytes": 10485760,
            "max.partition.fetch.bytes": 1048576,
            "partition.assignment.strategy": "cooperative-sticky",
        })

        # Producer for validated and DLQ topics
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 5,
            "batch.size": 65536,
        })

        self._pending_offsets: dict[tuple[str, int], int] = {}

    def _validate_event(self, payload: bytes) -> tuple[bool, dict | None, str | None]:
        """Validate a single event against the JSON Schema.

        Returns:
            (is_valid, parsed_event_or_None, error_reason_or_None)
        """
        start = time.monotonic()
        try:
            event = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            metrics.VALIDATION_LATENCY.observe(time.monotonic() - start)
            return False, None, f"invalid_json: {str(e)[:200]}"

        errors = list(self.json_validator.iter_errors(event))
        latency = time.monotonic() - start
        metrics.VALIDATION_LATENCY.observe(latency)

        if errors:
            reasons = "; ".join(e.message[:100] for e in errors[:3])
            return False, event, f"schema_violation: {reasons}"

        return True, event, None

    def _produce_validated(self, key: bytes | None, value: bytes, headers: list | None):
        """Produce a validated event to the validated topic."""
        self.producer.produce(
            topic=self.config.VALID_TOPIC,
            key=key,
            value=value,
            headers=headers,
        )

    def _produce_dlq(self, key: bytes | None, value: bytes, error_reason: str,
                     source_topic: str, source_partition: int, source_offset: int):
        """Produce a rejected event to the DLQ with error metadata in headers."""
        headers = [
            ("error_reason", error_reason.encode("utf-8")),
            ("source_topic", source_topic.encode("utf-8")),
            ("source_partition", str(source_partition).encode("utf-8")),
            ("source_offset", str(source_offset).encode("utf-8")),
        ]
        self.producer.produce(
            topic=self.config.DLQ_TOPIC,
            key=key,
            value=value,
            headers=headers,
        )

    def _on_assign(self, consumer, partitions):
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")

    def _on_revoke(self, consumer, partitions):
        logger.info(f"Partitions revoked: {[p.partition for p in partitions]}")
        # Commit current offsets before partition revocation
        self._commit_offsets()

    def _commit_offsets(self):
        """Commit accumulated offsets."""
        if not self._pending_offsets:
            return
        offsets = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in self._pending_offsets.items()
        ]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
            self._pending_offsets.clear()
        except Exception as e:
            logger.error(f"Offset commit failed: {e}")

    def run(self):
        """Main consumer loop."""
        config = self.config
        self.consumer.subscribe(
            [config.INPUT_TOPIC],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        # Start Prometheus metrics server
        start_http_server(config.METRICS_PORT)
        logger.info(f"Metrics server started on port {config.METRICS_PORT}")
        logger.info("Validator consumer started, waiting for messages...")

        last_commit_time = time.monotonic()
        commit_interval = 5.0  # Commit every 5 seconds

        while not self.shutdown.is_set():
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            metrics.EVENTS_CONSUMED.inc()

            # Validate
            is_valid, event, error_reason = self._validate_event(msg.value())

            if is_valid:
                self._produce_validated(
                    key=msg.key(),
                    value=msg.value(),
                    headers=msg.headers(),
                )
                metrics.VALIDATED_TOTAL.inc()
            else:
                reason_label = error_reason.split(":")[0] if error_reason else "unknown"
                self._produce_dlq(
                    key=msg.key(),
                    value=msg.value(),
                    error_reason=error_reason or "unknown",
                    source_topic=msg.topic(),
                    source_partition=msg.partition(),
                    source_offset=msg.offset(),
                )
                metrics.REJECTED_TOTAL.labels(reason=reason_label).inc()

            # Track offset for this partition
            self._pending_offsets[(msg.topic(), msg.partition())] = msg.offset()

            # Flush producer periodically to ensure downstream delivery
            self.producer.poll(0)

            # Commit offsets periodically
            now = time.monotonic()
            if now - last_commit_time >= commit_interval:
                self.producer.flush(timeout=10)
                self._commit_offsets()
                last_commit_time = now

        # Graceful shutdown
        logger.info("Shutting down validator...")
        self.producer.flush(timeout=30)
        self._commit_offsets()
        self.consumer.close()
        logger.info("Validator shut down cleanly")


def main():
    from scripts.wait_for_kafka import wait_for_kafka
    config = ValidatorConfig()

    if not wait_for_kafka(config.KAFKA_BOOTSTRAP_SERVERS):
        logger.error("Kafka not available, exiting")
        sys.exit(1)

    validator = SchemaValidator(config)
    validator.run()


if __name__ == "__main__":
    main()
