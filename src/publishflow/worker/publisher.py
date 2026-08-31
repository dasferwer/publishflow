import logging
import time
from datetime import UTC, datetime

import pika
from sqlalchemy import select

from publishflow.broker import connect, declare_topology, publish_event
from publishflow.db import SessionLocal
from publishflow.models import OutboxEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def publish_batch(channel: pika.channel.Channel, batch_size: int = 100) -> int:
    published = 0
    with SessionLocal.begin() as db:
        events = list(
            db.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        for event in events:
            try:
                publish_event(
                    channel,
                    event.event_type,
                    {"event_id": str(event.id), "event_type": event.event_type, **event.payload},
                )
                event.published_at = datetime.now(UTC)
                event.attempts += 1
                event.last_error = None
                published += 1
            except Exception as exc:
                event.attempts += 1
                event.last_error = str(exc)[:1000]
                logger.exception("Failed to publish outbox event %s", event.id)
    return published


def run() -> None:
    while True:
        connection: pika.BlockingConnection | None = None
        try:
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
            while connection.is_open:
                published = publish_batch(channel)
                connection.process_data_events(time_limit=0)
                time.sleep(0.25 if published else 1.0)
        except Exception:
            logger.exception("Outbox publisher lost its connection; retrying")
            time.sleep(3)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    run()
