"""
ML Telemetry Producer Simulator.

Generates synthetic telemetry events and publishes them to Kafka.
Supports configurable throughput, burst patterns, and malformed event injection.
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaError

sys.path.insert(0, "/app")
from shared.logging_config import setup_logging
from shared.signals import GracefulShutdown
from producer.config import ProducerConfig

logger = setup_logging("producer")


class TelemetryProducer:
    """High-throughput telemetry event producer with malformed event injection."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": config.COMPRESSION,
            "batch.size": config.BATCH_SIZE,
            "linger.ms": config.LINGER_MS,
            "queue.buffering.max.messages": config.QUEUE_BUFFERING_MAX_MESSAGES,
            "queue.buffering.max.kbytes": config.BUFFER_MEMORY // 1024,
            "delivery.timeout.ms": 30000,
            "retries": 5,
        })
        # Pre-generate entity IDs for consistent key distribution
        self.entity_ids = [f"entity-{i:06d}" for i in range(config.ENTITY_CARDINALITY)]
        self.stats = {
            "valid_produced": 0,
            "malformed_produced": 0,
            "delivery_errors": 0,
            "total_bytes": 0,
        }
        self.shutdown = GracefulShutdown()
        self.shutdown.register()

    def _generate_valid_event(self) -> dict:
        """Generate a valid telemetry event."""
        entity_id = random.choice(self.entity_ids)
        return {
            "event_id": str(uuid.uuid4()),
            "entity_id": entity_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "feature_vector": [
                round(random.gauss(0, 1), 6) for _ in range(self.config.NUM_FEATURES)
            ],
            "schema_version": 1,
        }

    def _generate_malformed_event(self) -> dict:
        """Generate a malformed event with random defects."""
        event = self._generate_valid_event()
        defect = random.choice(["missing_field", "wrong_type", "out_of_range", "extra_field", "bad_vector"])

        if defect == "missing_field":
            field = random.choice(["event_id", "entity_id", "timestamp", "feature_vector"])
            del event[field]
        elif defect == "wrong_type":
            event["feature_vector"] = "not_a_list"
        elif defect == "out_of_range":
            event["feature_vector"] = [float("inf")] * self.config.NUM_FEATURES
        elif defect == "extra_field":
            event["unexpected_field"] = "surprise"
        elif defect == "bad_vector":
            event["feature_vector"] = [1.0]  # Too few items

        return event

    def _delivery_callback(self, err, msg):
        """Track delivery success/failure."""
        if err is not None:
            self.stats["delivery_errors"] += 1
            if self.stats["delivery_errors"] % 1000 == 1:
                logger.error(f"Delivery failed: {err}", extra={"topic": msg.topic()})

    def _produce_event(self, event: dict, is_malformed: bool):
        """Serialize and produce a single event."""
        entity_id = event.get("entity_id", "unknown")
        payload = json.dumps(event).encode("utf-8")

        try:
            self.producer.produce(
                topic=self.config.TOPIC,
                key=entity_id.encode("utf-8"),
                value=payload,
                callback=self._delivery_callback,
            )
            self.stats["total_bytes"] += len(payload)
            if is_malformed:
                self.stats["malformed_produced"] += 1
            else:
                self.stats["valid_produced"] += 1
        except BufferError:
            # Local queue full — flush and retry
            self.producer.flush(timeout=5)
            self.producer.produce(
                topic=self.config.TOPIC,
                key=entity_id.encode("utf-8"),
                value=payload,
                callback=self._delivery_callback,
            )

    def run(self):
        """Main producer loop — rate-limited event generation."""
        cfg = self.config
        target_rate = int(cfg.RATE * cfg.BURST_MULTIPLIER)
        duration = cfg.DURATION
        malformed_pct = cfg.MALFORMED_PCT / 100.0

        logger.info(
            f"Starting producer: rate={target_rate} eps, duration={duration}s, "
            f"malformed={cfg.MALFORMED_PCT}%, entities={cfg.ENTITY_CARDINALITY}, "
            f"features={cfg.NUM_FEATURES}"
        )

        start_time = time.monotonic()
        last_report_time = start_time
        events_since_report = 0
        batch_interval = 0.01  # 10ms batching window

        while not self.shutdown.is_set():
            elapsed = time.monotonic() - start_time
            if elapsed >= duration:
                break

            # Calculate how many events to produce in this batch
            batch_start = time.monotonic()
            events_in_batch = max(1, int(target_rate * batch_interval))

            for _ in range(events_in_batch):
                is_malformed = random.random() < malformed_pct
                if is_malformed:
                    event = self._generate_malformed_event()
                else:
                    event = self._generate_valid_event()
                self._produce_event(event, is_malformed)
                events_since_report += 1

            # Trigger delivery reports
            self.producer.poll(0)

            # Rate limiting: sleep if we're ahead of schedule
            batch_elapsed = time.monotonic() - batch_start
            sleep_time = batch_interval - batch_elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Periodic progress report (every 5 seconds)
            now = time.monotonic()
            if now - last_report_time >= 5.0:
                actual_rate = events_since_report / (now - last_report_time)
                total = self.stats["valid_produced"] + self.stats["malformed_produced"]
                logger.info(
                    f"Progress: {total} events produced, "
                    f"current rate: {actual_rate:.0f} eps, "
                    f"elapsed: {elapsed:.1f}s/{duration}s",
                    extra={"rate": round(actual_rate, 1), "event_count": total},
                )
                events_since_report = 0
                last_report_time = now

        # Flush remaining messages
        logger.info("Flushing remaining messages...")
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")

        total_elapsed = time.monotonic() - start_time
        total_events = self.stats["valid_produced"] + self.stats["malformed_produced"]
        avg_rate = total_events / total_elapsed if total_elapsed > 0 else 0

        summary = {
            "valid_produced": self.stats["valid_produced"],
            "malformed_produced": self.stats["malformed_produced"],
            "total_produced": total_events,
            "delivery_errors": self.stats["delivery_errors"],
            "total_bytes": self.stats["total_bytes"],
            "duration_seconds": round(total_elapsed, 2),
            "average_rate_eps": round(avg_rate, 1),
            "target_rate_eps": target_rate,
        }

        logger.info(f"Producer finished: {json.dumps(summary)}")

        # Write summary for reconciliation
        with open("/tmp/producer_summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        return summary


def parse_args():
    parser = argparse.ArgumentParser(description="ML Telemetry Producer Simulator")
    parser.add_argument("--rate", type=int, help="Events per second")
    parser.add_argument("--duration", type=int, help="Duration in seconds")
    parser.add_argument("--burst-multiplier", type=float, help="Burst rate multiplier")
    parser.add_argument("--malformed-pct", type=float, help="Percentage of malformed events")
    parser.add_argument("--entity-cardinality", type=int, help="Number of unique entities")
    parser.add_argument("--num-features", type=int, help="Feature vector size (8-16)")
    return parser.parse_args()


def main():
    args = parse_args()
    config = ProducerConfig()

    # CLI args override env vars
    if args.rate is not None:
        config.RATE = args.rate
    if args.duration is not None:
        config.DURATION = args.duration
    if args.burst_multiplier is not None:
        config.BURST_MULTIPLIER = args.burst_multiplier
    if args.malformed_pct is not None:
        config.MALFORMED_PCT = args.malformed_pct
    if args.entity_cardinality is not None:
        config.ENTITY_CARDINALITY = args.entity_cardinality
    if args.num_features is not None:
        config.NUM_FEATURES = args.num_features

    # Wait for Kafka
    from scripts.wait_for_kafka import wait_for_kafka
    if not wait_for_kafka(config.KAFKA_BOOTSTRAP_SERVERS):
        logger.error("Kafka not available, exiting")
        sys.exit(1)

    producer = TelemetryProducer(config)
    producer.run()


if __name__ == "__main__":
    main()
