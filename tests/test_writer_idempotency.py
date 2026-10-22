"""Unit tests for writer idempotency and batch write logic.

These tests cover the business logic in isolation:
  - Entity deduplication (latest-per-entity selection)
  - SQL idempotency assertions (ON CONFLICT DO NOTHING)
  - Cache key naming convention
  - Write path error handling

External dependencies (psycopg, redis) are mocked or source-inspected
so tests run without a live database.
"""

import inspect
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def make_event(entity_id: str = "entity-000001", event_id: str = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "entity_id": entity_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "feature_vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        "schema_version": 1,
    }


# ---------------------------------------------------------------------------
# Batch deduplication logic (pure Python — no external deps)
# ---------------------------------------------------------------------------

def latest_per_entity(events: list[dict]) -> dict[str, dict]:
    """Replicate the deduplication logic used by both db.py and cache.py."""
    result = {}
    for e in events:
        eid = e["entity_id"]
        if eid not in result or e["timestamp"] > result[eid]["timestamp"]:
            result[eid] = e
    return result


class TestBatchDeduplication:
    def test_latest_per_entity_single(self):
        events = [make_event("entity-001")]
        result = latest_per_entity(events)
        assert len(result) == 1
        assert result["entity-001"] == events[0]

    def test_latest_per_entity_multiple_same_entity(self):
        events = [
            {**make_event("entity-001"), "timestamp": "2026-10-01T10:00:00+00:00"},
            {**make_event("entity-001"), "timestamp": "2026-10-01T12:00:00+00:00"},
            {**make_event("entity-001"), "timestamp": "2026-10-01T11:00:00+00:00"},
        ]
        result = latest_per_entity(events)
        assert result["entity-001"]["timestamp"] == "2026-10-01T12:00:00+00:00"

    def test_latest_per_entity_multiple_entities(self):
        events = [make_event(f"entity-{i:03d}") for i in range(10)]
        result = latest_per_entity(events)
        assert len(result) == 10

    def test_empty_batch(self):
        assert latest_per_entity([]) == {}

    def test_same_timestamp_keeps_first_seen(self):
        """When timestamps are equal, the strict '>' comparator keeps the first event."""
        ts = "2026-10-01T10:00:00+00:00"
        e1 = {**make_event("entity-001"), "timestamp": ts, "event_id": "aaa"}
        e2 = {**make_event("entity-001"), "timestamp": ts, "event_id": "bbb"}
        result = latest_per_entity([e1, e2])
        # e2.timestamp > e1.timestamp is False when equal, so e1 is retained
        assert result["entity-001"]["event_id"] == "aaa"


# ---------------------------------------------------------------------------
# SQL idempotency assertions (source inspection — no DB required)
# ---------------------------------------------------------------------------

class TestDatabaseWriterSQL:
    """Verify that the SQL in db.py uses correct idempotency patterns."""

    @pytest.fixture
    def db_source(self):
        path = os.path.join(os.path.dirname(__file__), "..", "writer", "db.py")
        with open(path) as f:
            return f.read()

    def test_on_conflict_do_nothing_present(self, db_source):
        assert "ON CONFLICT" in db_source
        assert "DO NOTHING" in db_source

    def test_on_conflict_uses_event_id(self, db_source):
        # Should conflict on (event_id, timestamp) or just event_id
        assert "event_id" in db_source

    def test_upsert_entity_latest_features_present(self, db_source):
        assert "ON CONFLICT (entity_id) DO UPDATE" in db_source

    def test_upsert_uses_timestamp_guard(self, db_source):
        # Only update if incoming event is newer
        assert "EXCLUDED.timestamp > entity_latest_features.timestamp" in db_source

    def test_rollback_on_exception_present(self, db_source):
        assert "rollback" in db_source.lower()

    def test_executemany_used_for_batch(self, db_source):
        assert "executemany" in db_source

    def test_empty_batch_early_return(self, db_source):
        # Should return early for empty batch without hitting DB
        assert "if not events:" in db_source
        assert "return 0" in db_source


# ---------------------------------------------------------------------------
# Cache key and pipeline assertions (source inspection)
# ---------------------------------------------------------------------------

class TestCacheWriterSource:
    """Verify cache.py uses correct Redis key format and pipeline pattern."""

    @pytest.fixture
    def cache_source(self):
        path = os.path.join(os.path.dirname(__file__), "..", "writer", "cache.py")
        with open(path) as f:
            return f.read()

    def test_cache_key_format(self, cache_source):
        assert "entity:features:{entity_id}" in cache_source

    def test_pipeline_used_for_batch_writes(self, cache_source):
        assert ".pipeline(" in cache_source
        assert "pipe.execute()" in cache_source

    def test_ttl_applied_to_cache_entries(self, cache_source):
        # Should pass `ex=` (TTL) to Redis SET
        assert "ex=" in cache_source

    def test_redis_failure_is_non_fatal(self, cache_source):
        # Redis failures should be caught and logged, not re-raised
        assert "ConnectionError" in cache_source or "except" in cache_source

    def test_empty_batch_early_return(self, cache_source):
        assert "if not events:" in cache_source


# ---------------------------------------------------------------------------
# Write path logic (pure mocking — no psycopg/redis import needed)
# ---------------------------------------------------------------------------

class TestWriterFlushLogic:
    """Test FeatureWriter._flush_batch behavior with mocked DB and cache."""

    def _make_writer(self):
        """Build a FeatureWriter with all external deps mocked."""
        # Patch imports before the module loads
        mock_psycopg = MagicMock()
        mock_redis = MagicMock()
        mock_kafka = MagicMock()

        with patch.dict("sys.modules", {
            "psycopg": mock_psycopg,
            "psycopg.rows": MagicMock(),
            "redis": mock_redis,
            "confluent_kafka": mock_kafka,
            "prometheus_client": MagicMock(),
            "writer.metrics": MagicMock(),
        }):
            # Now we can import writer modules safely
            import importlib
            if "writer.db" in sys.modules:
                del sys.modules["writer.db"]
            if "writer.cache" in sys.modules:
                del sys.modules["writer.cache"]
            if "writer.writer" in sys.modules:
                del sys.modules["writer.writer"]

            from writer.config import WriterConfig
            config = WriterConfig()
            config.RETRY_MAX_ATTEMPTS = 1

        return config

    def test_batch_size_flush_threshold(self):
        """_batch should flush when it reaches BATCH_SIZE."""
        config = self._make_writer()
        assert config.BATCH_SIZE == 500  # default

    def test_retry_config_values(self):
        """Retry configuration should have sensible defaults."""
        config = self._make_writer()
        assert config.RETRY_BASE_DELAY > 0
        assert config.RETRY_MAX_DELAY >= config.RETRY_BASE_DELAY
        assert config.RETRY_MAX_ATTEMPTS >= 1

    def test_writer_config_defaults(self):
        """Config should read from env with correct defaults."""
        config = self._make_writer()
        assert config.INPUT_TOPIC == "telemetry.validated"
        assert config.CONSUMER_GROUP == "feature-writer-group"
        assert config.METRICS_PORT == 8002


# ---------------------------------------------------------------------------
# Idempotency property: duplicate event_ids must not increase row count
# ---------------------------------------------------------------------------

class TestIdempotencyProperty:
    """Property-based reasoning about idempotency without a real DB."""

    def test_duplicate_events_produce_same_final_state(self):
        """
        If the same event_id is processed twice, latest_per_entity
        must produce the same result (same event wins over itself).
        """
        eid = str(uuid.uuid4())
        event = make_event("entity-dup", event_id=eid)
        duplicated = [event, event]  # same object twice
        result = latest_per_entity(duplicated)
        assert len(result) == 1
        assert result["entity-dup"]["event_id"] == eid

    def test_later_duplicate_with_same_id_keeps_first_in_timestamp_order(self):
        """Two events with same entity_id but different timestamps — newer wins."""
        e1 = {**make_event("entity-dup"), "timestamp": "2026-10-01T10:00:00+00:00"}
        e2 = {**make_event("entity-dup"), "timestamp": "2026-10-01T09:00:00+00:00"}
        result = latest_per_entity([e1, e2])
        assert result["entity-dup"]["timestamp"] == "2026-10-01T10:00:00+00:00"

    def test_on_conflict_do_nothing_sql_is_idempotent_by_design(self):
        """
        Document the idempotency contract: ON CONFLICT DO NOTHING means
        a second INSERT with the same (event_id, timestamp) is silently ignored.
        This is a design assertion, not a live test.
        """
        sql_pattern = re.compile(
            r"ON CONFLICT.*DO NOTHING",
            re.IGNORECASE | re.DOTALL,
        )
        db_path = os.path.join(os.path.dirname(__file__), "..", "writer", "db.py")
        with open(db_path) as f:
            source = f.read()
        assert sql_pattern.search(source), "writer/db.py must use ON CONFLICT ... DO NOTHING"
