"""PostgreSQL batch write operations for the feature writer."""

import time
import logging
from typing import Sequence

import psycopg
from psycopg.rows import dict_row

from writer import metrics

logger = logging.getLogger("writer.db")


class DatabaseWriter:
    """Handles batched, idempotent writes to PostgreSQL."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn: psycopg.Connection | None = None

    def connect(self):
        """Establish database connection."""
        self._conn = psycopg.connect(
            self.dsn,
            autocommit=False,
            row_factory=dict_row,
        )
        logger.info("Connected to PostgreSQL")

    def ensure_connected(self):
        """Reconnect if the connection was lost."""
        if self._conn is None or self._conn.closed:
            self.connect()

    def write_batch(self, events: Sequence[dict]) -> int:
        """Write a batch of events to Postgres.

        Uses executemany with ON CONFLICT DO NOTHING for idempotency.
        Also upserts entity_latest_features.

        Returns:
            Number of new rows inserted (excludes duplicates).
        """
        if not events:
            return 0

        self.ensure_connected()
        start = time.monotonic()

        try:
            with self._conn.cursor() as cur:
                # Batch insert into feature_events (idempotent)
                insert_sql = """
                    INSERT INTO feature_events
                        (event_id, entity_id, timestamp, feature_vector, schema_version)
                    VALUES
                        (%(event_id)s, %(entity_id)s, %(timestamp)s,
                         %(feature_vector)s::double precision[], %(schema_version)s)
                    ON CONFLICT (event_id, timestamp) DO NOTHING
                """
                cur.executemany(insert_sql, events)
                inserted = cur.rowcount

                # Upsert entity_latest_features — use a temp approach with
                # the latest timestamp per entity in this batch
                latest_per_entity: dict[str, dict] = {}
                for e in events:
                    eid = e["entity_id"]
                    if eid not in latest_per_entity or e["timestamp"] > latest_per_entity[eid]["timestamp"]:
                        latest_per_entity[eid] = e

                upsert_sql = """
                    INSERT INTO entity_latest_features
                        (entity_id, event_id, timestamp, feature_vector)
                    VALUES
                        (%(entity_id)s, %(event_id)s, %(timestamp)s,
                         %(feature_vector)s::double precision[])
                    ON CONFLICT (entity_id) DO UPDATE
                        SET event_id = EXCLUDED.event_id,
                            timestamp = EXCLUDED.timestamp,
                            feature_vector = EXCLUDED.feature_vector,
                            updated_at = NOW()
                        WHERE EXCLUDED.timestamp > entity_latest_features.timestamp
                """
                cur.executemany(upsert_sql, list(latest_per_entity.values()))

            self._conn.commit()

            latency = time.monotonic() - start
            metrics.WRITE_LATENCY.observe(latency)
            metrics.WRITE_BATCH_SIZE.observe(len(events))
            metrics.WRITES_TOTAL.inc(inserted)
            metrics.DUPLICATES_SKIPPED.inc(len(events) - inserted)

            return inserted

        except Exception as e:
            self._conn.rollback()
            metrics.ERRORS_TOTAL.labels(target="postgres").inc()
            logger.error(f"Postgres write failed: {e}")
            # Mark connection as potentially broken
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            raise

    def close(self):
        """Close the database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed")
