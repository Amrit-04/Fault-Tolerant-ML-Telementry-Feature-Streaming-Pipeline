"""Wait for Kafka broker(s) to be ready before starting services."""

import os
import sys
import time
import socket
import logging


def wait_for_kafka(
    bootstrap_servers: str | None = None,
    timeout: int = 120,
    interval: int = 2,
) -> bool:
    """Block until at least one Kafka broker accepts TCP connections."""
    servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    brokers = [s.strip() for s in servers.split(",")]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logger = logging.getLogger("wait-for-kafka")

    deadline = time.time() + timeout
    logger.info(f"Waiting for Kafka brokers: {brokers} (timeout={timeout}s)")

    while time.time() < deadline:
        for broker in brokers:
            host, port = broker.rsplit(":", 1)
            try:
                sock = socket.create_connection((host, int(port)), timeout=2)
                sock.close()
                logger.info(f"Kafka broker {broker} is ready")
                return True
            except (socket.error, OSError):
                pass
        time.sleep(interval)

    logger.error(f"Kafka brokers not ready after {timeout}s")
    return False


if __name__ == "__main__":
    if not wait_for_kafka():
        sys.exit(1)
