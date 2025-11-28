"""Lightweight background scheduler that runs inside the Flask web process."""
from __future__ import annotations

import logging
import threading
from contextlib import suppress
from typing import Optional

LOGGER = logging.getLogger("postly.self_cron")


class SelfCron:
    """Run periodic jobs inside the Flask/Gunicorn process without extra services."""

    def __init__(self, app, interval_seconds: int = 60):
        self._app = app
        self._interval = max(5, interval_seconds)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        LOGGER.info("Starting SelfCron loop (interval=%ss)", self._interval)
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="SelfCron", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._thread:
            return
        LOGGER.info("Stopping SelfCron loop")
        self._stop.set()
        with suppress(RuntimeError):
            self._thread.join(timeout=self._interval)
        self._thread = None

    def _run(self) -> None:
        # Run once immediately, then wait between ticks
        while not self._stop.is_set():
            self._tick_once()
            self._stop.wait(self._interval)

    def _tick_once(self) -> None:
        try:
            with self._app.app_context():
                from tasks import check_and_publish_scheduled_posts
                check_and_publish_scheduled_posts()
        except ModuleNotFoundError:
            LOGGER.exception("Tasks module not found; skipping SelfCron tick")
        except Exception:
            LOGGER.exception("Unhandled error inside SelfCron tick")


def create_self_cron(app, interval_seconds: int) -> SelfCron:
    cron = SelfCron(app, interval_seconds=interval_seconds)
    return cron
