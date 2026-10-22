"""Graceful shutdown signal handling for all pipeline services."""

import signal
import threading
import logging

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Thread-safe shutdown coordinator.

    Usage:
        shutdown = GracefulShutdown()
        shutdown.register()

        while not shutdown.is_set():
            # do work
            pass

        # cleanup
    """

    def __init__(self):
        self._event = threading.Event()

    def register(self):
        """Register SIGTERM and SIGINT handlers."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        logger.info("Graceful shutdown handlers registered")

    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating graceful shutdown...")
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    def set(self):
        self._event.set()
