"""Prometheus metrics for the schema validation service."""

from prometheus_client import Counter, Histogram, Gauge

VALIDATED_TOTAL = Counter(
    "validator_validated_total",
    "Total events that passed schema validation",
)

REJECTED_TOTAL = Counter(
    "validator_rejected_total",
    "Total events rejected by schema validation",
    ["reason"],
)

VALIDATION_LATENCY = Histogram(
    "validator_validation_latency_seconds",
    "Time spent validating a single event",
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1),
)

EVENTS_CONSUMED = Counter(
    "validator_events_consumed_total",
    "Total events consumed from raw topic",
)

BATCH_SIZE = Histogram(
    "validator_batch_size",
    "Number of events per poll batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000),
)

CONSUMER_LAG = Gauge(
    "validator_consumer_lag",
    "Consumer lag (estimated from highwater - offset)",
    ["partition"],
)
