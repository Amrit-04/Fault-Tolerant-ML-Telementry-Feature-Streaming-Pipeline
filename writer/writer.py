"""
Feature Writer Service.

Consumes validated telemetry events and persists them to PostgreSQL
with write-through caching to Redis. Implements at-least-once delivery
with idempotent writes for effectively-exactly-once results.
"""

import json
import sys
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition
from prometheus_client import start_http_server

sys.path.insert(0, "/app")
from shared.logging_config import setup_logging
from shared.signals import GracefulShutdown
from writer.config import WriterConfig
from writer.db import DatabaseWriter
from writer.cache import CacheWriter
from writer import metrics

logger = setup_logging("writer")


class FeatureWriter:
    """Consumes validated events, writes to Postgres + Redis."""

    def __init__(self, config: WriterConfig):
        self.config = config
        self.shutdown = GracefulShutdown()
        self.shutdown.register()

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

        self.db = DatabaseWriter(config.POSTGRES_DSN)
        self.cache = CacheWriter(config.REDIS_URL, config.CACHE_TTL_SECONDS)
        self._pending_offsets: dict[tuple[str, int], int] = {}
        self._batch: list[dict] = []

    def _on_assign(self, consumer, partitions):
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")

    def _on_revoke(self, consumer, partitions):
        logger.info(f"Partitions revoked: {[p.partition for p in partitions]}")
        # Flush and commit before revocation
        self._flush_batch()
        self._commit_offsets()

    def _commit_offsets(self):
        """Commit accumulated offsets to Kafka."""
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

    def _parse_event(self, msg) -> dict | None:
        """Deserialize a Kafka message into an event dict."""
        try:
            event = json.loads(msg.value())
            return {
                "event_id": event["event_id"],
                "entity_id": event["entity_id"],
                "timestamp": event["timestamp"],
                "feature_vector": event["feature_vector"],
                "schema_version": event["schema_version"],
            }
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse event at offset {msg.offset()}: {e}")
            return None

    def _flush_batch(self):
        """Write the accumulated batch to Postgres and Redis."""
        if not self._batch:
            return

        batch = self._batch
        self._batch = []

        # Retry loop with exponential backoff
        delay = self.config.RETRY_BASE_DELAY
        for attempt in range(1, self.config.RETRY_MAX_ATTEMPTS + 1):
            try:
                # Write to Postgres first (source of truth)
                inserted = self.db.write_batch(batch)
                logger.debug(
                    f"Wrote batch: {len(batch)} events, {inserted} new rows",
                    extra={"batch_size": len(batch)},
                )
                break
            except Exception as e:
                if attempt == self.config.RETRY_MAX_ATTEMPTS:
                    logger.error(f"Postgres write failed after {attempt} attempts, giving up: {e}")
                    raise
                logger.warning(
                    f"Postgres write attempt {attempt} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
                delay = min(delay * 2, self.config.RETRY_MAX_DELAY)

        # Write-through cache (non-fatal on failure)
        try:
            self.cache.update_latest_features(batch)
        except Exception as e:
            logger.warning(f"Redis cache update failed (non-fatal): {e}")

    def run(self):
        """Main consumer loop."""
        config = self.config

        # Connect to backends
        self.db.connect()
        try:
            self.cache.connect()
        except Exception as e:
            logger.warning(f"Redis not available at startup (non-fatal): {e}")

        self.consumer.subscribe(
            [config.INPUT_TOPIC],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        # Start Prometheus metrics server
        start_http_server(config.METRICS_PORT)
        logger.info(f"Metrics server started on port {config.METRICS_PORT}")
        logger.info("Feature writer started, waiting for messages...")

        last_commit_time = time.monotonic()
        commit_interval = 5.0

        while not self.shutdown.is_set():
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                # No message — flush any partial batch
                if self._batch:
                    self._flush_batch()
                    self._commit_offsets()
                    last_commit_time = time.monotonic()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            metrics.EVENTS_CONSUMED.inc()

            event = self._parse_event(msg)
            if event is None:
                continue

            self._batch.append(event)
            self._pending_offsets[(msg.topic(), msg.partition())] = msg.offset()

            # Flush when batch is full
            if len(self._batch) >= config.BATCH_SIZE:
                self._flush_batch()

            # Commit offsets periodically
            now = time.monotonic()
            if now - last_commit_time >= commit_interval:
                self._flush_batch()
                self._commit_offsets()
                last_commit_time = now

        # Graceful shutdown
        logger.info("Shutting down feature writer...")
        self._flush_batch()
        self._commit_offsets()
        self.consumer.close()
        self.db.close()
        self.cache.close()
        logger.info("Feature writer shut down cleanly")


def main():
    from scripts.wait_for_kafka import wait_for_kafka
    config = WriterConfig()

    if not wait_for_kafka(config.KAFKA_BOOTSTRAP_SERVERS):
        logger.error("Kafka not available, exiting")
        sys.exit(1)

    writer = FeatureWriter(config)
    writer.run()


if __name__ == "__main__":
    main()
