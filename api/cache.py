"""Redis cache client with Postgres fallback for the feature serving API."""

import json
import logging
from typing import Optional

import redis
import psycopg
from psycopg.rows import dict_row

from api import metrics as m

logger = logging.getLogger("api.cache")


class FeatureCache:
    """Read-through cache: Redis first, Postgres fallback, repopulate on miss."""

    def __init__(
        self,
        redis_url: str,
        postgres_dsn: str,
        cache_enabled: bool = True,
        ttl_seconds: int = 3600,
    ):
        self.redis_url = redis_url
        self.postgres_dsn = postgres_dsn
        self.cache_enabled = cache_enabled
        self.ttl_seconds = ttl_seconds
        self._redis: Optional[redis.Redis] = None
        self._pg: Optional[psycopg.Connection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self):
        """Open Redis and Postgres connections."""
        if self.cache_enabled:
            self._redis = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            try:
                self._redis.ping()
                logger.info("Connected to Redis")
            except Exception as e:
                logger.warning(f"Redis unavailable at startup: {e}")
                self._redis = None

        self._pg = psycopg.connect(
            self.postgres_dsn,
            autocommit=True,
            row_factory=dict_row,
        )
        logger.info("Connected to PostgreSQL")

    def close(self):
        """Close connections gracefully."""
        if self._redis:
            try:
                self._redis.close()
            except Exception:
                pass
        if self._pg and not self._pg.closed:
            self._pg.close()

    def health(self) -> dict:
        """Return health status of both backends."""
        status = {"redis": "disabled", "postgres": "unknown"}
        if self.cache_enabled and self._redis:
            try:
                self._redis.ping()
                status["redis"] = "ok"
            except Exception as e:
                status["redis"] = f"error: {e}"
        if self._pg and not self._pg.closed:
            try:
                with self._pg.cursor() as cur:
                    cur.execute("SELECT 1")
                status["postgres"] = "ok"
            except Exception as e:
                status["postgres"] = f"error: {e}"
        return status

    # ------------------------------------------------------------------
    # Single-entity lookup
    # ------------------------------------------------------------------

    def get_features(self, entity_id: str) -> Optional[dict]:
        """Fetch features for a single entity.

        1. Try Redis (if cache enabled).
        2. Fall back to Postgres.
        3. Repopulate Redis on cache miss.
        """
        if self.cache_enabled and self._redis:
            cached = self._redis_get(entity_id)
            if cached is not None:
                m.CACHE_HITS.inc()
                cached_dict = dict(cached)
                cached_dict["source"] = "cache"
                return cached_dict
        m.CACHE_MISSES.inc()

        row = self._pg_get(entity_id)
        if row and self.cache_enabled and self._redis:
            # Store in cache without source field
            to_cache = {
                "entity_id": row["entity_id"],
                "event_id": row["event_id"],
                "timestamp": row["timestamp"],
                "feature_vector": row["feature_vector"],
            }
            self._redis_set(entity_id, to_cache)
        if row is None:
            m.FEATURES_NOT_FOUND.inc()
            return None

        row_dict = dict(row)
        row_dict["source"] = "database"
        return row_dict

    # ------------------------------------------------------------------
    # Batch lookup
    # ------------------------------------------------------------------

    def get_features_batch(self, entity_ids: list[str]) -> dict[str, Optional[dict]]:
        """Fetch features for multiple entities efficiently.

        Uses Redis pipeline for cache lookups, then fetches misses from Postgres.
        """
        m.BATCH_REQUEST_SIZE.observe(len(entity_ids))
        results: dict[str, Optional[dict]] = {}
        cache_misses: list[str] = []

        # Step 1: Redis batch lookup
        if self.cache_enabled and self._redis:
            try:
                pipe = self._redis.pipeline(transaction=False)
                for eid in entity_ids:
                    pipe.get(f"entity:features:{eid}")
                values = pipe.execute()
                for eid, val in zip(entity_ids, values):
                    if val is not None:
                        item = json.loads(val)
                        item["source"] = "cache"
                        results[eid] = item
                        m.CACHE_HITS.inc()
                    else:
                        cache_misses.append(eid)
                        m.CACHE_MISSES.inc()
            except Exception as e:
                logger.warning(f"Redis batch get failed: {e}")
                cache_misses = entity_ids
        else:
            cache_misses = entity_ids
            m.CACHE_MISSES.inc(len(entity_ids))

        # Step 2: Postgres fallback for misses
        if cache_misses:
            pg_rows = self._pg_get_batch(cache_misses)
            # Repopulate cache
            if self.cache_enabled and self._redis and pg_rows:
                try:
                    pipe = self._redis.pipeline(transaction=False)
                    for eid, row in pg_rows.items():
                        to_cache = {
                            "entity_id": row["entity_id"],
                            "event_id": row["event_id"],
                            "timestamp": row["timestamp"],
                            "feature_vector": row["feature_vector"],
                        }
                        pipe.set(
                            f"entity:features:{eid}",
                            json.dumps(to_cache),
                            ex=self.ttl_seconds,
                        )
                    pipe.execute()
                except Exception as e:
                    logger.warning(f"Redis cache repopulation failed: {e}")

            for eid in cache_misses:
                row = pg_rows.get(eid)
                if row is not None:
                    row_dict = dict(row)
                    row_dict["source"] = "database"
                    results[eid] = row_dict
                else:
                    results[eid] = None
                    m.FEATURES_NOT_FOUND.inc()

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _redis_get(self, entity_id: str) -> Optional[dict]:
        try:
            val = self._redis.get(f"entity:features:{entity_id}")
            return json.loads(val) if val else None
        except Exception as e:
            logger.warning(f"Redis get failed for {entity_id}: {e}")
            return None

    def _redis_set(self, entity_id: str, data: dict):
        try:
            self._redis.set(
                f"entity:features:{entity_id}",
                json.dumps(data),
                ex=self.ttl_seconds,
            )
        except Exception as e:
            logger.warning(f"Redis set failed for {entity_id}: {e}")

    def _pg_get(self, entity_id: str) -> Optional[dict]:
        """Fetch latest features for one entity from Postgres."""
        if not self._pg or self._pg.closed:
            self._reconnect_pg()
        try:
            with self._pg.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity_id, event_id::text, timestamp::text, feature_vector
                    FROM entity_latest_features
                    WHERE entity_id = %s
                    """,
                    (entity_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Postgres get failed for {entity_id}: {e}")
            self._reconnect_pg()
            return None

    def _pg_get_batch(self, entity_ids: list[str]) -> dict[str, dict]:
        """Fetch latest features for multiple entities from Postgres."""
        if not self._pg or self._pg.closed:
            self._reconnect_pg()
        try:
            with self._pg.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity_id, event_id::text, timestamp::text, feature_vector
                    FROM entity_latest_features
                    WHERE entity_id = ANY(%s)
                    """,
                    (entity_ids,),
                )
                rows = cur.fetchall()
                return {row["entity_id"]: dict(row) for row in rows}
        except Exception as e:
            logger.error(f"Postgres batch get failed: {e}")
            self._reconnect_pg()
            return {}

    def _reconnect_pg(self):
        """Attempt to reconnect to Postgres."""
        try:
            if self._pg and not self._pg.closed:
                self._pg.close()
            self._pg = psycopg.connect(
                self.postgres_dsn,
                autocommit=True,
                row_factory=dict_row,
            )
            logger.info("Reconnected to PostgreSQL")
        except Exception as e:
            logger.error(f"Postgres reconnection failed: {e}")
            self._pg = None
