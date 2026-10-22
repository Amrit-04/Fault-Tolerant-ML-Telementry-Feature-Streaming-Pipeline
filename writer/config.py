"""Configuration for the feature writer service."""

import os


class WriterConfig:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    INPUT_TOPIC: str = "telemetry.validated"
    CONSUMER_GROUP: str = "feature-writer-group"
    MAX_POLL_RECORDS: int = int(os.getenv("WRITER_MAX_POLL_RECORDS", "1000"))
    BATCH_SIZE: int = int(os.getenv("WRITER_BATCH_SIZE", "500"))

    POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "postgresql://pipeline:pipeline@postgres:5432/pipeline")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "3600"))

    METRICS_PORT: int = 8002
    RETRY_BASE_DELAY: float = 0.5
    RETRY_MAX_DELAY: float = 30.0
    RETRY_MAX_ATTEMPTS: int = 10
