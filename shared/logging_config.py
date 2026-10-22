"""Structured JSON logging configuration for all pipeline services."""

import logging
import json
import sys
import os
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def __init__(self, service_name: str = "unknown"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        # Include any extra fields passed via `extra={}`
        for key in ("event_count", "rate", "duration", "batch_size", "entity_id",
                     "error_reason", "latency_ms", "topic", "partition", "offset"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)
        return json.dumps(log_entry, default=str)


class PlainFormatter(logging.Formatter):
    """Simple human-readable formatter for local development."""

    def __init__(self, service_name: str = "unknown"):
        super().__init__(
            fmt=f"%(asctime)s [{service_name}] %(levelname)-8s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging(service_name: str, level: str | None = None) -> logging.Logger:
    """Configure root logger and return a named logger for the service."""
    log_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    log_format = os.getenv("LOG_FORMAT", "json").lower()

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))

    # Clear existing handlers
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    if log_format == "json":
        handler.setFormatter(JSONFormatter(service_name))
    else:
        handler.setFormatter(PlainFormatter(service_name))

    root.addHandler(handler)

    # Suppress noisy libraries
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logging.getLogger(service_name)
