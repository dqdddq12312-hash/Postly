"""Standalone scheduler runner for Render/Heroku-style worker dynos."""
import logging
import time
from typing import Optional

from tasks import setup_scheduler

HEARTBEAT_SECONDS = 60
RETRY_DELAY_SECONDS = 15

logging.basicConfig(
    level=logging.INFO,
    format="[SCHEDULER] %(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def _sleep_forever(interval: int) -> None:
    while True:
        time.sleep(interval)


def main() -> None:
    """Boot the APScheduler worker and keep it alive forever."""
    while True:
        scheduler = None
        try:
            logger.info("Booting APScheduler worker ...")
            scheduler = setup_scheduler()
            if not scheduler:
                logger.error("setup_scheduler() returned None; retrying in %ss", RETRY_DELAY_SECONDS)
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            logger.info("Scheduler is running; entering heartbeat loop")
            _sleep_forever(HEARTBEAT_SECONDS)
        except KeyboardInterrupt:
            logger.info("Scheduler runner interrupted. Shutting down ...")
            if scheduler:
                scheduler.shutdown(wait=False)
            break
        except Exception:
            logger.exception("Scheduler runner crashed; restarting in %ss", RETRY_DELAY_SECONDS)
            if scheduler:
                try:
                    scheduler.shutdown(wait=False)
                except Exception:
                    logger.exception("Error shutting down scheduler after crash")
            time.sleep(RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    main()
