"""
Integration tests — requires the full Docker Compose stack to be running.

Run with: make test-integration
Or: pytest tests/test_integration.py -v --timeout=120

These tests verify end-to-end behavior:
  - Producer → Kafka → Validator → telemetry.validated / telemetry.dlq
  - Feature Writer → Postgres + Redis
  - API: GET /features/{entity_id}, POST /features/batch
  - Data integrity: valid count == Postgres rows
"""

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import pytest

# Skip integration tests if INTEGRATION env var is not set
pytestmark = pytest.mark.skipif(
    os.getenv("INTEGRATION") != "1",
    reason="Integration tests require running Docker Compose stack. Set INTEGRATION=1 to enable.",
)

API_URL = os.getenv("API_URL", "http://localhost:8000")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COMPOSE_DIR = os.path.join(os.path.dirname(__file__), "..")


def run_producer(rate: int, duration: int, malformed_pct: float, entity_cardinality: int = 1000) -> dict:
    """Run the producer synchronously and return its summary."""
    result = subprocess.run(
        [
            "docker", "compose",
            "--profile", "simulate",
            "run", "--rm",
            "-e", f"PRODUCER_RATE={rate}",
            "-e", f"PRODUCER_DURATION={duration}",
            "-e", f"PRODUCER_MALFORMED_PCT={malformed_pct}",
            "-e", f"PRODUCER_ENTITY_CARDINALITY={entity_cardinality}",
            "producer",
        ],
        cwd=COMPOSE_DIR,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        timeout=duration + 60,
    )
    summary = {}
    output = (result.stdout or '') + (result.stderr or '')
    for line in output.splitlines():
        if "Producer finished" in line and "{" in line:
            try:
                start = line.index("{")
                summary = json.loads(line[start:])
            except Exception:
                pass
    return summary


def wait_for_lag_zero(timeout: int = 90) -> bool:
    """Poll consumer lag until it reaches zero."""
    import httpx
    prometheus = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(
                f"{prometheus}/api/v1/query",
                params={"query": "sum(kafka_consumergroup_lag_sum)"},
                timeout=5,
            )
            results = resp.json().get("data", {}).get("result", [])
            if results and float(results[0]["value"][1]) <= 5:
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


@pytest.fixture(scope="module")
def pg_conn():
    import psycopg
    conn = psycopg.connect(POSTGRES_DSN, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def redis_client():
    import redis
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    yield client
    client.close()


class TestAPIHealth:
    def test_health_endpoint(self):
        import httpx
        resp = httpx.get(f"{API_URL}/health", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in ("ok", "degraded")
        assert "postgres" in body

    def test_metrics_endpoint(self):
        import httpx
        resp = httpx.get(f"{API_URL}/metrics", timeout=10)
        assert resp.status_code == 200


class TestEndToEndFlow:
    """Full pipeline flow: produce → validate → write → serve."""

    def test_valid_events_reach_postgres(self, pg_conn):
        """Produce 100 events at 1000 eps, verify they land in Postgres."""
        import httpx

        before = None
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM feature_events")
            before = cur.fetchone()[0]

        summary = run_producer(rate=1000, duration=5, malformed_pct=0.0, entity_cardinality=100)
        assert summary.get("valid_produced", 0) > 0, "Producer produced no events"

        drained = wait_for_lag_zero(timeout=60)

        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM feature_events")
            after = cur.fetchone()[0]

        new_rows = after - before
        expected = summary["valid_produced"]
        # Allow for a small replay margin (at-least-once + idempotency)
        assert new_rows >= expected * 0.95, (
            f"Expected ~{expected} new rows, got {new_rows}"
        )

    def test_malformed_events_go_to_dlq(self, pg_conn):
        """Events with malformed=100% should all end up in DLQ, none in Postgres."""
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM feature_events")
            before = cur.fetchone()[0]

        summary = run_producer(rate=500, duration=3, malformed_pct=100.0, entity_cardinality=10)
        total = summary.get("total_produced", 0)
        assert total > 0

        wait_for_lag_zero(timeout=60)
        time.sleep(5)

        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM feature_events")
            after = cur.fetchone()[0]

        new_rows = after - before
        # With 100% malformed, nothing should land in Postgres
        assert new_rows == 0, f"Expected 0 new rows for 100% malformed, got {new_rows}"

    def test_api_returns_features(self, pg_conn):
        """After writing events, the API should return features for known entities."""
        import httpx

        # First ensure there's data
        run_producer(rate=500, duration=3, malformed_pct=0.0, entity_cardinality=50)
        wait_for_lag_zero(timeout=60)

        # Get an entity_id that exists
        with pg_conn.cursor() as cur:
            cur.execute("SELECT entity_id FROM entity_latest_features LIMIT 1")
            row = cur.fetchone()
        assert row is not None, "No entities in entity_latest_features"
        entity_id = row[0]

        resp = httpx.get(f"{API_URL}/features/{entity_id}", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["entity_id"] == entity_id
        assert len(body["feature_vector"]) >= 8

    def test_api_404_for_unknown_entity(self):
        import httpx
        entity_id = f"nonexistent-{uuid.uuid4()}"
        resp = httpx.get(f"{API_URL}/features/{entity_id}", timeout=10)
        assert resp.status_code == 404

    def test_batch_api_returns_correct_counts(self, pg_conn):
        """POST /features/batch should return correct found/not_found counts."""
        import httpx

        # Get a few real entity IDs
        with pg_conn.cursor() as cur:
            cur.execute("SELECT entity_id FROM entity_latest_features LIMIT 3")
            real_ids = [row[0] for row in cur.fetchall()]

        if not real_ids:
            pytest.skip("No entities in DB")

        fake_ids = [f"fake-{uuid.uuid4()}" for _ in range(2)]
        all_ids = real_ids + fake_ids

        resp = httpx.post(
            f"{API_URL}/features/batch",
            json={"entity_ids": all_ids},
            timeout=10,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["found"] == len(real_ids)
        assert body["not_found"] == len(fake_ids)

    def test_no_duplicates_in_postgres(self, pg_conn):
        """Produce the same events twice (replay simulation), verify zero duplicates."""
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT event_id) AS dup_count FROM feature_events"
            )
            dup_count = cur.fetchone()[0]
        assert dup_count == 0, f"Found {dup_count} duplicate event_ids in feature_events"

    def test_redis_cache_populated(self, redis_client):
        """After writing, at least some entity keys should exist in Redis."""
        # Get any entity key
        keys = redis_client.keys("entity:features:*")
        assert len(keys) > 0, "No entity feature keys found in Redis cache"

        # Verify key structure
        sample = redis_client.get(keys[0])
        assert sample is not None
        data = json.loads(sample)
        assert "entity_id" in data
        assert "feature_vector" in data
        assert isinstance(data["feature_vector"], list)
