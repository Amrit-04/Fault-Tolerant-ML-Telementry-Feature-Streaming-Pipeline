"""Unit tests for the Feature Serving API.

Uses sys.modules patching to avoid requiring psycopg, redis, and confluent-kafka
to be installed in the local test environment (they are only needed in Docker).

The FastAPI app under test is constructed without the lifespan (no real DB/Redis
connections) — instead we inject a mock FeatureCache directly into app.state.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Patch heavy C-extension deps before any api.* import
# ---------------------------------------------------------------------------

_MOCKS = {
    "redis": MagicMock(),
    "psycopg": MagicMock(),
    "psycopg.rows": MagicMock(),
    "confluent_kafka": MagicMock(),
}
for _name, _mock in _MOCKS.items():
    sys.modules.setdefault(_name, _mock)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Build a test-only FastAPI app (no lifespan, injected cache)
# ---------------------------------------------------------------------------

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# Import routes (safe after mocking)
from api.routes import router


def _build_test_app(mock_cache) -> FastAPI:
    """Create a minimal FastAPI app with mocked state — no lifespan."""
    app = FastAPI()
    app.include_router(router)
    app.state.cache = mock_cache

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_feature_data(entity_id: str = "entity-001") -> dict:
    return {
        "entity_id": entity_id,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "feature_vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cache():
    cache = MagicMock()
    cache.cache_enabled = True
    cache.health.return_value = {"redis": "ok", "postgres": "ok"}
    return cache


@pytest.fixture
def client(mock_cache):
    from fastapi.testclient import TestClient
    app = _build_test_app(mock_cache)
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c, mock_cache


# ---------------------------------------------------------------------------
# GET /features/{entity_id}
# ---------------------------------------------------------------------------

class TestGetFeatures:
    def test_found(self, client):
        c, cache = client
        data = make_feature_data("entity-001")
        cache.get_features.return_value = data

        resp = c.get("/features/entity-001")
        assert resp.status_code == 200
        body = resp.json()
        assert body["entity_id"] == "entity-001"
        assert "feature_vector" in body
        assert len(body["feature_vector"]) == 8

    def test_not_found(self, client):
        c, cache = client
        cache.get_features.return_value = None

        resp = c.get("/features/nonexistent-entity")
        assert resp.status_code == 404

    def test_response_has_source_field(self, client):
        c, cache = client
        cache.get_features.return_value = make_feature_data("entity-001")

        resp = c.get("/features/entity-001")
        assert "source" in resp.json()

    def test_entity_id_passed_to_cache(self, client):
        c, cache = client
        cache.get_features.return_value = None

        c.get("/features/my-entity-123")
        cache.get_features.assert_called_once_with("my-entity-123")

    def test_feature_vector_is_list(self, client):
        c, cache = client
        cache.get_features.return_value = make_feature_data("entity-001")

        resp = c.get("/features/entity-001")
        assert isinstance(resp.json()["feature_vector"], list)


# ---------------------------------------------------------------------------
# POST /features/batch
# ---------------------------------------------------------------------------

class TestBatchFeatures:
    def test_batch_all_found(self, client):
        c, cache = client
        ids = ["entity-001", "entity-002"]
        cache.get_features_batch.return_value = {
            eid: make_feature_data(eid) for eid in ids
        }

        resp = c.post("/features/batch", json={"entity_ids": ids})
        assert resp.status_code == 200
        body = resp.json()
        assert body["found"] == 2
        assert body["not_found"] == 0

    def test_batch_partial_miss(self, client):
        c, cache = client
        cache.get_features_batch.return_value = {
            "entity-001": make_feature_data("entity-001"),
            "entity-999": None,
        }

        resp = c.post(
            "/features/batch",
            json={"entity_ids": ["entity-001", "entity-999"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["found"] == 1
        assert body["not_found"] == 1
        assert body["results"]["entity-999"] is None

    def test_batch_empty_list_rejected(self, client):
        c, cache = client
        resp = c.post("/features/batch", json={"entity_ids": []})
        assert resp.status_code == 422  # Pydantic min_length=1

    def test_batch_too_many_ids(self, client):
        c, cache = client
        ids = [f"entity-{i}" for i in range(501)]
        resp = c.post("/features/batch", json={"entity_ids": ids})
        assert resp.status_code == 422  # Pydantic max_length=500


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_healthy(self, client):
        c, cache = client
        cache.health.return_value = {"redis": "ok", "postgres": "ok"}

        resp = c.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_degraded(self, client):
        c, cache = client
        cache.health.return_value = {"redis": "error: refused", "postgres": "ok"}

        resp = c.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"

    def test_redis_disabled_is_ok(self, client):
        c, cache = client
        cache.health.return_value = {"redis": "disabled", "postgres": "ok"}

        resp = c.get("/health")
        assert resp.json()["status"] == "ok"

    def test_health_contains_redis_field(self, client):
        c, cache = client
        resp = c.get("/health")
        assert "redis" in resp.json()

    def test_health_contains_postgres_field(self, client):
        c, cache = client
        resp = c.get("/health")
        assert "postgres" in resp.json()


# ---------------------------------------------------------------------------
# GET /metrics
# ---------------------------------------------------------------------------

class TestMetricsEndpoint:
    def test_metrics_endpoint_returns_200(self, client):
        c, cache = client
        resp = c.get("/metrics")
        assert resp.status_code == 200

    def test_metrics_returns_nonempty_content(self, client):
        c, cache = client
        resp = c.get("/metrics")
        assert len(resp.content) > 0


# ---------------------------------------------------------------------------
# Route source assertions (static — no runtime deps)
# ---------------------------------------------------------------------------

class TestRouteSource:
    @pytest.fixture
    def routes_source(self):
        path = os.path.join(os.path.dirname(__file__), "..", "api", "routes.py")
        with open(path) as f:
            return f.read()

    def test_get_features_endpoint_defined(self, routes_source):
        assert '"/features/{entity_id}"' in routes_source

    def test_batch_endpoint_defined(self, routes_source):
        assert '"/features/batch"' in routes_source

    def test_health_endpoint_defined(self, routes_source):
        assert '"/health"' in routes_source

    def test_request_latency_metric_tracked(self, routes_source):
        assert "REQUEST_LATENCY" in routes_source

    def test_404_on_miss(self, routes_source):
        assert "404" in routes_source

    def test_cache_source_redis_first_strategy(self):
        path = os.path.join(os.path.dirname(__file__), "..", "api", "cache.py")
        with open(path) as f:
            source = f.read()
        # Redis lookup happens before Postgres fallback
        redis_pos = source.find("_redis_get")
        pg_pos = source.find("_pg_get")
        assert redis_pos < pg_pos, "Redis lookup should come before Postgres fallback"

    def test_cache_repopulation_on_miss(self):
        path = os.path.join(os.path.dirname(__file__), "..", "api", "cache.py")
        with open(path) as f:
            source = f.read()
        # After Postgres fallback, should repopulate Redis
        assert "_redis_set" in source or "redis_set" in source
