"""Configuration for the feature serving API."""

import os


class APIConfig:
    POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "postgresql://pipeline:pipeline@postgres:5432/pipeline")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    CACHE_ENABLED: bool = os.getenv("CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "3600"))
    HOST: str = os.getenv("API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("API_PORT", "8000"))
