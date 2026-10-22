"""Redis write-through cache operations for the feature writer."""

import json
import time
import logging
from typing import Sequence

import redis

from writer import metrics

logger = logging.getLogger("writer.cache")


class CacheWriter:
    """Handles write-through caching of latest features per entity to Redis."""

    def __init__(self, redis_url: str, ttl_seconds: int = 3600):
        self.redis_url = redis_url
        self.ttl_seconds = ttl_seconds
        self._client: redis.Redis | None = None

    def connect(self):
        """Establish Redis connection."""
        self._client = redis.Redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        self._client.ping()
        logger.info("Connected to Redis")

    def ensure_connected(self):
        """Reconnect if the connection was lost."""
        if self._client is None:
            self.connect()
            return
        try:
            self._client.ping()
        except (redis.ConnectionError, redis.TimeoutError):
            logger.warning("Redis connection lost, reconnecting...")
            self.connect()

    def update_latest_features(self, events: Sequence[dict]):
        """Update Redis cache with the latest feature vector per entity.

        Uses pipelining for efficiency. Each entity's latest feature is cached
        as a JSON string with a configurable TTL.
        """
        if not events:
            return

        # Determine latest event per entity
        latest_per_entity: dict[str, dict] = {}
        for e in events:
            eid = e["entity_id"]
            if eid not in latest_per_entity or e["timestamp"] > latest_per_entity[eid]["timestamp"]:
                latest_per_entity[eid] = e

        self.ensure_connected()
        start = time.monotonic()

        try:
            pipe = self._client.pipeline(transaction=False)
            for entity_id, event in latest_per_entity.items():
                cache_key = f"entity:features:{entity_id}"
                cache_value = json.dumps({
                    "entity_id": entity_id,
                    "event_id": event["event_id"],
                    "timestamp": event["timestamp"],
                    "feature_vector": event["feature_vector"],
                })
                pipe.set(cache_key, cache_value, ex=self.ttl_seconds)
            pipe.execute()

            latency = time.monotonic() - start
            metrics.CACHE_WRITE_LATENCY.observe(latency)

        except (redis.ConnectionError, redis.TimeoutError) as e:
            metrics.ERRORS_TOTAL.labels(target="redis").inc()
            logger.error(f"Redis write failed: {e}")
            # Redis failures are non-fatal — Postgres is source of truth
            self._client = None

    def close(self):
        """Close the Redis connection."""
        if self._client:
            self._client.close()
            logger.info("Redis connection closed")
