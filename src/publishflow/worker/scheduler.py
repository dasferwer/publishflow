import logging
import time

from publishflow.db import SessionLocal
from publishflow.services import publish_due_articles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    while True:
        try:
            with SessionLocal() as db:
                published = publish_due_articles(db)
            if published:
                logger.info("Published %s scheduled article(s)", published)
        except Exception:
            logger.exception("Scheduler iteration failed")
        time.sleep(1)


if __name__ == "__main__":
    run()
