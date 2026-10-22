"""Configuration for the telemetry producer."""

import os


class ProducerConfig:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    TOPIC: str = "telemetry.raw"
    RATE: int = int(os.getenv("PRODUCER_RATE", "10000"))
    DURATION: int = int(os.getenv("PRODUCER_DURATION", "60"))
    BURST_MULTIPLIER: float = float(os.getenv("PRODUCER_BURST_MULTIPLIER", "1.0"))
    MALFORMED_PCT: float = float(os.getenv("PRODUCER_MALFORMED_PCT", "0"))
    ENTITY_CARDINALITY: int = int(os.getenv("PRODUCER_ENTITY_CARDINALITY", "100000"))
    NUM_FEATURES: int = int(os.getenv("PRODUCER_NUM_FEATURES", "10"))

    # Kafka producer tuning
    BATCH_SIZE: int = 65536          # 64 KB batch
    LINGER_MS: int = 5               # Wait up to 5ms to fill batch
    COMPRESSION: str = "snappy"
    BUFFER_MEMORY: int = 134217728   # 128 MB buffer
    QUEUE_BUFFERING_MAX_MESSAGES: int = 500000
