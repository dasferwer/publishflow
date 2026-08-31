import json
import logging
import time
from uuid import UUID

import pika

from publishflow.broker import IMPORT_QUEUE, connect, declare_topology
from publishflow.db import SessionLocal
from publishflow.services import process_import_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def handle_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    del properties
    try:
        payload = json.loads(body)
        with SessionLocal() as db:
            process_import_job(db, UUID(payload["job_id"]))
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        logger.exception("Import job failed")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def run() -> None:
    while True:
        connection: pika.BlockingConnection | None = None
        try:
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=IMPORT_QUEUE, on_message_callback=handle_message)
            logger.info("Import worker is ready")
            channel.start_consuming()
        except Exception:
            logger.exception("Import worker lost its connection; retrying")
            time.sleep(3)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    run()
