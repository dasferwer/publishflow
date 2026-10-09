"""Явно включаемое ограничение демонстрационного журнала article-событий."""

import argparse
import base64
import json
import re
import sys
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import Request, urlopen
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from publishflow.broker import ARTICLE_QUEUE
from publishflow.config import Settings, get_settings
from publishflow.db import SessionLocal
from publishflow.models import OutboxEvent

POLICY_NAME = "publishflow-article-retention"
POLICY_PATTERN = r"^publishflow\.article-events$"


def retention_cutoff(settings: Settings, now: datetime) -> datetime:
    if settings.article_retention_seconds is None:
        raise ValueError("Retention отключён: задайте ARTICLE_RETENTION_SECONDS")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Время должно содержать часовой пояс")
    return now.astimezone(UTC) - timedelta(seconds=settings.article_retention_seconds)


def policy_definition(settings: Settings) -> dict[str, Any]:
    retention_cutoff(settings, datetime.now(UTC))
    assert settings.article_retention_seconds is not None
    return {
        "pattern": POLICY_PATTERN,
        "apply-to": "queues",
        "priority": 100,
        "definition": {
            "message-ttl": settings.article_retention_seconds * 1000,
            "max-length-bytes": settings.article_queue_max_bytes,
            "overflow": "drop-head",
        },
    }


def policy_request(
    settings: Settings,
    method: str,
    body: dict[str, Any] | None = None,
    *,
    allow_missing: bool = False,
    list_policies: bool = False,
) -> Any:
    broker = urlsplit(settings.rabbitmq_url)
    management = urlsplit(settings.rabbitmq_management_url)
    if management.scheme not in {"http", "https"} or not management.hostname:
        raise ValueError("Некорректный RABBITMQ_MANAGEMENT_URL")
    if management.username or management.password or management.query or management.fragment:
        raise ValueError("Management URL не должен содержать реквизиты или query/fragment")
    credentials = f"{unquote(broker.username or '')}:{unquote(broker.password or '')}"
    vhost = quote(unquote(broker.path[1:]) or "/", safe="")
    resource = f"policies/{vhost}" if list_policies else f"policies/{vhost}/{POLICY_NAME}"
    request = Request(
        settings.rabbitmq_management_url.rstrip("/") + "/api/" + resource,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(credentials.encode()).decode(),
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=5) as response:
            content = response.read()
            return json.loads(content) if content else None
    except HTTPError as exc:
        if allow_missing and exc.code == 404:
            return None
        raise RuntimeError(f"RabbitMQ management: HTTP {exc.code}") from None


def check_policy_scope(settings: Settings) -> None:
    existing = policy_request(settings, "GET", allow_missing=True)
    if existing is not None and (
        existing.get("pattern") != POLICY_PATTERN or existing.get("apply-to") != "queues"
    ):
        raise ValueError("Policy с этим именем имеет другую область действия")


def apply_policy(settings: Settings) -> None:
    definition = policy_definition(settings)
    check_policy_scope(settings)
    # Читаем определения: статистика очереди может отставать от смены policy.
    for current in policy_request(settings, "GET", list_policies=True):
        if (
            current["name"] != POLICY_NAME
            and current.get("apply-to") in ("all", "queues", "classic_queues", "quorum_queues")
            and re.search(current["pattern"], ARTICLE_QUEUE)
        ):
            raise ValueError("У article-events уже другая policy: сначала согласуйте её замену")
    policy_request(settings, "PUT", definition)


def remove_policy(settings: Settings) -> None:
    check_policy_scope(settings)
    policy_request(settings, "DELETE", allow_missing=True)


def cleanup_batch(
    db: Session, settings: Settings, *, now: datetime, dry_run: bool = True
) -> list[UUID]:
    cutoff = retention_cutoff(settings, now)
    query = (
        select(OutboxEvent.id)
        .where(
            OutboxEvent.event_type.startswith("article."),
            OutboxEvent.published_at < cutoff,
        )
        .order_by(OutboxEvent.published_at, OutboxEvent.id)
        .limit(settings.article_retention_batch_size)
    )
    if not dry_run:
        query = query.with_for_update(skip_locked=True)
    ids = list(db.scalars(query))
    if ids and not dry_run:
        db.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(ids)))
    return ids


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Применить policy и удалить один пакет")
    mode.add_argument(
        "--dry-run", action="store_true", help="Предпросмотр без записей (по умолчанию)"
    )
    mode.add_argument(
        "--remove-policy", action="store_true", help="Отключить только policy брокера"
    )
    parser.add_argument("--now", type=datetime.fromisoformat, help="Фиксированное время с timezone")
    args = parser.parse_args()
    settings = get_settings()
    if args.remove_policy:
        remove_policy(settings)
        print(json.dumps({"policy_removed": POLICY_NAME}))
        return
    now = args.now or datetime.now(UTC)
    cutoff = retention_cutoff(settings, now)
    policy = policy_definition(settings)
    # Policy и SQL не атомарны: отказ брокера должен предшествовать удалению outbox.
    if args.apply:
        apply_policy(settings)
    with SessionLocal.begin() as db:
        ids = cleanup_batch(db, settings, now=now, dry_run=not args.apply)
    print(
        json.dumps(
            {
                "dry_run": not args.apply,
                "cutoff": cutoff.isoformat(),
                "policy": policy,
                "event_ids": [str(event_id) for event_id in ids],
                "count": len(ids),
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Не выводим URL/пароли даже при ошибке подключения к БД или HTTP API.
        print(f"Retention не завершён: {type(exc).__name__}", file=sys.stderr)
        raise SystemExit(1) from None
