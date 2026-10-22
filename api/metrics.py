"""Prometheus metrics for the feature serving API."""

from prometheus_client import Counter, Histogram

REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds",
    "HTTP request latency",
    ["method", "endpoint", "status_code"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

CACHE_HITS = Counter(
    "api_cache_hits_total",
    "Number of requests served from Redis cache",
)

CACHE_MISSES = Counter(
    "api_cache_misses_total",
    "Number of requests that required a Postgres fallback",
)

BATCH_REQUEST_SIZE = Histogram(
    "api_batch_request_size",
    "Number of entity IDs per batch request",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500),
)

FEATURES_NOT_FOUND = Counter(
    "api_features_not_found_total",
    "Number of entity lookups that returned no data",
)
