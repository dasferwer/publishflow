import json
import logging
import time
from uuid import UUID

import pika

from publishflow.broker import IMPORT_QUEUE, INVALID_QUEUE, connect, declare_topology
from publishflow.db import SessionLocal
from publishflow.models import ImportJob, OutboxEvent
from publishflow.services import process_next_import

logger = logging.getLogger(__name__)


def consume_one(channel: pika.channel.Channel) -> bool:
    method, _properties, body = channel.basic_get(IMPORT_QUEUE, auto_ack=False)
    if method is None:
        return False
    try:
        payload = json.loads(body)
        if not isinstance(payload, dict) or not all(
            isinstance(payload.get(key), str) for key in ("event_id", "job_id")
        ):
            raise ValueError("ID события и задания должны быть строками")
        event_id, job_id = UUID(payload["event_id"]), UUID(payload["job_id"])
        with SessionLocal() as db:
            event = db.get(OutboxEvent, event_id)
            if (
                event is None
                or event.event_type != "content.import.requested"
                or event.payload.get("job_id") != str(job_id)
                or db.get(ImportJob, job_id) is None
            ):
                raise ValueError("Задание не соответствует событию outbox")
    except (ValueError, KeyError, TypeError):
        channel.basic_publish(
            exchange="",
            routing_key=INVALID_QUEUE,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )
    # Задание хранится в БД и не зависит от повторной доставки сообщения.
    channel.basic_ack(method.delivery_tag)
    return True


def run() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    while True:
        connection = None
        try:
            process_next_import()
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
            channel.confirm_delivery()
            while connection.is_open:
                consumed = consume_one(channel)
                worked = process_next_import()
                connection.process_data_events(time_limit=0 if consumed or worked else 1)
        except Exception as exc:
            logger.warning("import_retry kind=%s", type(exc).__name__)
            time.sleep(3)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    run()
