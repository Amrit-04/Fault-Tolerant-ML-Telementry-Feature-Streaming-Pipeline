"""Configuration for the schema validation service."""

import os


class ValidatorConfig:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    INPUT_TOPIC: str = "telemetry.raw"
    VALID_TOPIC: str = "telemetry.validated"
    DLQ_TOPIC: str = "telemetry.dlq"
    CONSUMER_GROUP: str = "validator-group"
    MAX_POLL_RECORDS: int = int(os.getenv("VALIDATOR_MAX_POLL_RECORDS", "1000"))
    METRICS_PORT: int = 8001
    SCHEMA_PATH: str = os.getenv("SCHEMA_PATH", "/app/schemas/telemetry_v1.json")
