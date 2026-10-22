"""Prometheus metrics for the feature writer service."""

from prometheus_client import Counter, Histogram, Gauge

WRITES_TOTAL = Counter(
    "writer_writes_total",
    "Total events written to database",
)

DUPLICATES_SKIPPED = Counter(
    "writer_duplicates_skipped_total",
    "Events skipped due to ON CONFLICT (duplicates)",
)

WRITE_BATCH_SIZE = Histogram(
    "writer_write_batch_size",
    "Number of events per write batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000),
)

WRITE_LATENCY = Histogram(
    "writer_write_latency_seconds",
    "Time spent writing a batch to Postgres",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

CACHE_WRITE_LATENCY = Histogram(
    "writer_cache_write_latency_seconds",
    "Time spent updating Redis cache per batch",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5),
)

ERRORS_TOTAL = Counter(
    "writer_errors_total",
    "Total write errors",
    ["target"],  # "postgres" or "redis"
)

EVENTS_CONSUMED = Counter(
    "writer_events_consumed_total",
    "Total events consumed from validated topic",
)

CONSUMER_LAG = Gauge(
    "writer_consumer_lag",
    "Consumer lag per partition",
    ["partition"],
)
