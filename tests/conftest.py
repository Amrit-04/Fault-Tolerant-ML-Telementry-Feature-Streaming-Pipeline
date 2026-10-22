"""Shared fixtures for all tests."""

import json
import os
import sys
import uuid
from datetime import datetime, timezone

import pytest

# Ensure /app paths work when running tests locally
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def valid_event():
    """A minimal valid telemetry event."""
    return {
        "event_id": str(uuid.uuid4()),
        "entity_id": "entity-000001",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "feature_vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        "schema_version": 1,
    }


@pytest.fixture
def schema_path():
    """Path to the JSON Schema file."""
    return os.path.join(os.path.dirname(__file__), "..", "schemas", "telemetry_v1.json")
