from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from publishflow.config import Settings, get_settings
from publishflow.db import SessionLocal
from publishflow.models import OutboxEvent

NOW = datetime(2020, 10, 9, 12, tzinfo=UTC)


def settings(**overrides):
    return Settings(**{**get_settings().model_dump(), **overrides})


def test_retention_is_disabled_until_explicitly_configured():
    assert settings().article_retention_seconds is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"article_retention_seconds": 0},
        {"article_retention_seconds": -1},
        {"article_queue_max_bytes": 0},
        {"article_retention_batch_size": 0},
        {"article_retention_batch_size": 10001},
    ],
)
def test_retention_rejects_unsafe_limits(overrides):
    with pytest.raises(ValidationError):
        settings(**overrides)


def test_cleanup_preserves_pending_recent_boundary_and_import_events():
    from publishflow.retention import cleanup_batch

    marker = str(uuid4())
    cutoff = NOW - timedelta(seconds=60)
    specs = [
        ("article.created", cutoff - timedelta(seconds=2)),
        ("article.updated", cutoff - timedelta(seconds=1)),
        ("article.published", cutoff),
        ("article.created", NOW),
        ("article.created", None),
        ("content.import.requested", cutoff - timedelta(days=1)),
    ]
    with SessionLocal.begin() as db:
        events = [
            OutboxEvent(
                event_type=kind,
                payload={"marker": marker},
                created_at=cutoff - timedelta(days=10),
                published_at=published_at,
            )
            for kind, published_at in specs
        ]
        db.add_all(events)
        db.flush()
        ids = [event.id for event in events]
    configured = settings(article_retention_seconds=60, article_retention_batch_size=1)
    with SessionLocal.begin() as db:
        preview = cleanup_batch(db, configured, now=NOW, dry_run=True)
        assert preview == [ids[0]]
    with SessionLocal() as db:
        assert all(db.get(OutboxEvent, event_id) is not None for event_id in ids)
    with SessionLocal.begin() as db:
        assert cleanup_batch(db, configured, now=NOW, dry_run=False) == [ids[0]]
    with SessionLocal.begin() as db:
        assert cleanup_batch(db, configured, now=NOW, dry_run=False) == [ids[1]]
    with SessionLocal() as db:
        assert all(db.get(OutboxEvent, event_id) is not None for event_id in ids[2:])


def test_cleanup_skips_locked_event_and_rolls_back():
    from publishflow.retention import cleanup_batch

    with SessionLocal.begin() as db:
        event = OutboxEvent(
            event_type="article.archived",
            payload={},
            published_at=NOW - timedelta(days=1),
        )
        db.add(event)
        db.flush()
        event_id = event.id
    configured = settings(article_retention_seconds=60)
    with SessionLocal.begin() as locker:
        locker.scalar(select(OutboxEvent).where(OutboxEvent.id == event_id).with_for_update())
        with SessionLocal.begin() as db:
            assert event_id not in cleanup_batch(db, configured, now=NOW, dry_run=False)
    with SessionLocal() as db:
        assert event_id in cleanup_batch(db, configured, now=NOW, dry_run=False)
        db.rollback()
        assert db.get(OutboxEvent, event_id) is not None


def test_cleanup_disabled_and_naive_clock_refuse_deletion():
    from publishflow.retention import cleanup_batch

    with SessionLocal.begin() as db:
        with pytest.raises(ValueError, match="отключ"):
            cleanup_batch(db, settings(), now=NOW, dry_run=False)
        with pytest.raises(ValueError, match="часов"):
            cleanup_batch(
                db,
                settings(article_retention_seconds=60),
                now=datetime(2026, 10, 9),
                dry_run=False,
            )


def test_broker_policy_bounds_article_queue_without_touching_imports():
    import time

    from publishflow.broker import ARTICLE_QUEUE, IMPORT_QUEUE, connect, declare_topology
    from publishflow.retention import apply_policy, remove_policy

    configured = settings(article_retention_seconds=1, article_queue_max_bytes=20)
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        channel.queue_purge(ARTICLE_QUEUE)
        channel.queue_purge(IMPORT_QUEUE)
        for body in (b"0000000000", b"1111111111", b"2222222222"):
            channel.basic_publish(exchange="", routing_key=ARTICLE_QUEUE, body=body, mandatory=True)
        apply_policy(configured)
        channel.basic_publish(exchange="", routing_key=IMPORT_QUEUE, body=b"import", mandatory=True)
        assert channel.basic_get(ARTICLE_QUEUE, auto_ack=True)[2] == b"1111111111"
        assert channel.basic_get(ARTICLE_QUEUE, auto_ack=True)[2] == b"2222222222"
        assert channel.basic_get(ARTICLE_QUEUE, auto_ack=True)[0] is None
        channel.basic_publish(
            exchange="", routing_key=ARTICLE_QUEUE, body=b"expires", mandatory=True
        )
        time.sleep(1.2)
        assert channel.basic_get(ARTICLE_QUEUE, auto_ack=True)[0] is None
        assert channel.basic_get(IMPORT_QUEUE, auto_ack=True)[2] == b"import"
    finally:
        remove_policy(configured)
        connection.close()


def test_cli_dry_run_does_not_change_rows_or_broker_policy():
    import json
    import os
    import subprocess
    import sys

    from publishflow.retention import policy_request

    with SessionLocal.begin() as db:
        event = OutboxEvent(
            id=UUID("00000000-0000-0000-0000-000000000003"),
            event_type="article.created",
            payload={},
            published_at=NOW - timedelta(days=2),
        )
        db.add(event)
    env = {**os.environ, "PUBLISHFLOW_ARTICLE_RETENTION_SECONDS": "60"}
    result = subprocess.run(
        [sys.executable, "-m", "publishflow.retention", "--now", NOW.isoformat()],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    output = json.loads(result.stdout)
    assert output["dry_run"] is True
    assert output["cutoff"] == (NOW - timedelta(seconds=60)).isoformat()
    assert str(event.id) in output["event_ids"]
    with SessionLocal() as db:
        assert db.get(OutboxEvent, event.id) is not None
    assert policy_request(settings(), "GET", allow_missing=True) is None


def test_cli_apply_deletes_one_batch_and_policy_can_be_removed():
    import json
    import os
    import subprocess
    import sys

    from publishflow.broker import connect, declare_topology
    from publishflow.retention import policy_request

    connection = connect()
    try:
        declare_topology(connection.channel())
    finally:
        connection.close()
    with SessionLocal.begin() as db:
        event = OutboxEvent(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            event_type="article.created",
            payload={},
            published_at=NOW - timedelta(days=100),
        )
        db.add(event)
    env = {
        **os.environ,
        "PUBLISHFLOW_ARTICLE_RETENTION_SECONDS": "60",
        "PUBLISHFLOW_ARTICLE_RETENTION_BATCH_SIZE": "1",
    }
    command = [sys.executable, "-m", "publishflow.retention"]
    try:
        result = subprocess.run(
            [*command, "--apply", "--now", NOW.isoformat()],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        assert json.loads(result.stdout)["event_ids"] == [str(event.id)]
        with SessionLocal() as db:
            assert db.get(OutboxEvent, event.id) is None
        assert policy_request(settings(), "GET")["definition"]["message-ttl"] == 60000
    finally:
        subprocess.run([*command, "--remove-policy"], env=env, check=True, capture_output=True)
    assert policy_request(settings(), "GET", allow_missing=True) is None


def test_cli_broker_failure_precedes_outbox_delete():
    import os
    import subprocess
    import sys

    with SessionLocal.begin() as db:
        event = OutboxEvent(
            event_type="article.created",
            payload={},
            published_at=NOW - timedelta(days=3),
        )
        db.add(event)
        db.flush()
        event_id = event.id
    env = {
        **os.environ,
        "PUBLISHFLOW_ARTICLE_RETENTION_SECONDS": "60",
        "PUBLISHFLOW_RABBITMQ_MANAGEMENT_URL": "http://rabbitmq-test:1",
    }
    result = subprocess.run(
        [sys.executable, "-m", "publishflow.retention", "--apply", "--now", NOW.isoformat()],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "publishflow:publishflow@" not in result.stderr
    with SessionLocal() as db:
        assert db.get(OutboxEvent, event_id) is not None


def test_policy_refuses_to_overwrite_or_remove_foreign_scope():
    from publishflow.broker import connect, declare_topology
    from publishflow.retention import apply_policy, policy_request, remove_policy

    configured = settings(article_retention_seconds=60)
    connection = connect()
    try:
        declare_topology(connection.channel())
    finally:
        connection.close()
    policy_request(
        configured,
        "PUT",
        {
            "pattern": "^publishflow\\.imports$",
            "apply-to": "queues",
            "priority": 100,
            "definition": {"message-ttl": 60000},
        },
    )
    try:
        with pytest.raises(ValueError, match="область"):
            apply_policy(configured)
        with pytest.raises(ValueError, match="область"):
            remove_policy(configured)
        assert policy_request(configured, "GET")["pattern"] == r"^publishflow\.imports$"
    finally:
        policy_request(configured, "DELETE")


def test_policy_conflict_is_detected_even_when_queue_statistics_are_stale(monkeypatch):
    import httpx2

    from publishflow import retention

    configured = settings(article_retention_seconds=60)
    request = retention.policy_request

    def stale_queue_statistics(settings, method, body=None, **kwargs):
        response = request(settings, method, body, **kwargs)
        if kwargs.get("queue"):
            response["policy"] = None
        return response

    # Реальное наблюдение: queue details могут отставать от policy PUT.
    monkeypatch.setattr(retention, "policy_request", stale_queue_statistics)
    with httpx2.Client(
        base_url=configured.rabbitmq_management_url,
        auth=("publishflow", "publishflow"),
    ) as client:
        path = "/api/policies/%2F/codex-retention-foreign"
        client.put(
            path,
            json={
                "pattern": r"^publishflow\.article-events$",
                "apply-to": "queues",
                "priority": 10,
                "definition": {"message-ttl": 30000},
            },
        ).raise_for_status()
        try:
            with pytest.raises(ValueError, match="другая policy"):
                retention.apply_policy(configured)
            assert request(configured, "GET", allow_missing=True) is None
            assert client.get(path).json()["definition"]["message-ttl"] == 30000
        finally:
            retention.remove_policy(configured)
            client.delete(path).raise_for_status()
