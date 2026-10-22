"""Unit tests for schema validation logic."""

import copy
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import pytest
import jsonschema

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def schema(schema_path):
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def validator(schema):
    return jsonschema.Draft7Validator(schema)


class TestValidEvents:
    def test_valid_event_passes(self, validator, valid_event):
        errors = list(validator.iter_errors(valid_event))
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_valid_event_8_features(self, validator, valid_event):
        valid_event["feature_vector"] = [0.1] * 8
        errors = list(validator.iter_errors(valid_event))
        assert errors == []

    def test_valid_event_16_features(self, validator, valid_event):
        valid_event["feature_vector"] = [0.5] * 16
        errors = list(validator.iter_errors(valid_event))
        assert errors == []

    def test_valid_event_negative_features(self, validator, valid_event):
        valid_event["feature_vector"] = [-0.999] * 10
        errors = list(validator.iter_errors(valid_event))
        assert errors == []

    def test_valid_event_schema_version_1(self, validator, valid_event):
        valid_event["schema_version"] = 1
        assert list(validator.iter_errors(valid_event)) == []

    def test_valid_event_schema_version_100(self, validator, valid_event):
        valid_event["schema_version"] = 100
        assert list(validator.iter_errors(valid_event)) == []


class TestInvalidEvents:
    def test_missing_event_id(self, validator, valid_event):
        del valid_event["event_id"]
        assert list(validator.iter_errors(valid_event)) != []

    def test_missing_entity_id(self, validator, valid_event):
        del valid_event["entity_id"]
        assert list(validator.iter_errors(valid_event)) != []

    def test_missing_timestamp(self, validator, valid_event):
        del valid_event["timestamp"]
        assert list(validator.iter_errors(valid_event)) != []

    def test_missing_feature_vector(self, validator, valid_event):
        del valid_event["feature_vector"]
        assert list(validator.iter_errors(valid_event)) != []

    def test_missing_schema_version(self, validator, valid_event):
        del valid_event["schema_version"]
        assert list(validator.iter_errors(valid_event)) != []

    def test_feature_vector_too_short(self, validator, valid_event):
        valid_event["feature_vector"] = [1.0, 2.0]  # < 8 items
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_feature_vector_too_long(self, validator, valid_event):
        valid_event["feature_vector"] = [0.5] * 17  # > 16 items
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_feature_vector_not_list(self, validator, valid_event):
        valid_event["feature_vector"] = "not_a_list"
        assert list(validator.iter_errors(valid_event)) != []

    def test_feature_vector_out_of_range(self, validator, valid_event):
        valid_event["feature_vector"] = [1e7] * 10  # > 1e6 max
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_additional_properties_rejected(self, validator, valid_event):
        valid_event["unexpected_field"] = "surprise"
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_invalid_event_id_format(self, validator, valid_event):
        valid_event["event_id"] = "not-a-uuid"
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_schema_version_zero(self, validator, valid_event):
        valid_event["schema_version"] = 0
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_schema_version_too_high(self, validator, valid_event):
        valid_event["schema_version"] = 101
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_entity_id_empty_string(self, validator, valid_event):
        valid_event["entity_id"] = ""
        errors = list(validator.iter_errors(valid_event))
        assert errors != []

    def test_feature_vector_contains_string(self, validator, valid_event):
        valid_event["feature_vector"] = ["a", "b"] * 4
        errors = list(validator.iter_errors(valid_event))
        assert errors != []


class TestValidationService:
    """Tests for the SchemaValidator class (isolated, no Kafka)."""

    def _make_validator(self, schema_path):
        """Instantiate the validator's core logic without Kafka."""
        import jsonschema
        with open(schema_path) as f:
            schema = json.load(f)
        return jsonschema.Draft7Validator(schema)

    def _validate(self, validator, payload: bytes):
        """Mimic the validator's _validate_event logic."""
        try:
            event = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return False, None, f"invalid_json: {e}"

        errors = list(validator.iter_errors(event))
        if errors:
            reasons = "; ".join(e.message[:100] for e in errors[:3])
            return False, event, f"schema_violation: {reasons}"
        return True, event, None

    def test_valid_json_event(self, schema_path, valid_event):
        v = self._make_validator(schema_path)
        ok, event, reason = self._validate(v, json.dumps(valid_event).encode())
        assert ok is True
        assert reason is None

    def test_invalid_json_bytes(self, schema_path):
        v = self._make_validator(schema_path)
        ok, event, reason = self._validate(v, b"{not valid json}")
        assert ok is False
        assert reason.startswith("invalid_json")

    def test_schema_violation_reason_prefix(self, schema_path, valid_event):
        v = self._make_validator(schema_path)
        del valid_event["event_id"]
        ok, event, reason = self._validate(v, json.dumps(valid_event).encode())
        assert ok is False
        assert reason.startswith("schema_violation")
