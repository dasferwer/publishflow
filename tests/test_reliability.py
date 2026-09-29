from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, update

from publishflow import services
from publishflow.db import SessionLocal
from publishflow.models import Article, ArticleHistory, ArticleVersion, ImportJob, OutboxEvent


def parallel(*operations):
    barrier = Barrier(len(operations))

    def run(operation):
        barrier.wait(timeout=10)
        return operation()

    with ThreadPoolExecutor(max_workers=len(operations)) as pool:
        return list(pool.map(run, operations))


def headers(user_headers, revision):
    return {**user_headers, "If-Match": f'"{revision}"'}


def test_concurrent_edits_preserve_versions(client, register_author, create_article):
    owner = register_author()
    article = create_article(owner["headers"])
    path = f"/api/v1/articles/{article['id']}"
    replies = parallel(
        *(
            lambda title=title: client.patch(
                path, headers=headers(owner["headers"], 1), json={"title": title}
            )
            for title in ["Первый вариант", "Второй вариант"]
        )
    )
    assert sorted(reply.status_code for reply in replies) == [200, 412]
    current = client.get(path, headers=owner["headers"]).json()
    assert current["revision"] == 2 and current["current_version"] == 2
    assert [v["version"] for v in current["versions"]] == [1, 2]
    assert (
        client.patch(path, headers=owner["headers"], json={"title": "Без ревизии"}).status_code
        == 428
    )
    assert (
        client.patch(
            path, headers={**owner["headers"], "If-Match": "*"}, json={"title": "Неверная ревизия"}
        ).status_code
        == 400
    )


def test_edit_races_submission(client, register_author, create_article):
    owner = register_author()
    article = create_article(owner["headers"])
    path = f"/api/v1/articles/{article['id']}"
    replies = parallel(
        lambda: client.patch(
            path, headers=headers(owner["headers"], 1), json={"title": "Новая тема"}
        ),
        lambda: client.post(path + "/submit", headers=headers(owner["headers"], 1)),
    )
    assert sorted(r.status_code for r in replies) == [200, 412]
    actual = client.get(path, headers=owner["headers"]).json()
    assert (actual["status"], actual["current_version"]) in [("draft", 2), ("in_review", 1)]


def approved_article(client, owner, article, editor_headers):
    path = f"/api/v1/articles/{article['id']}"
    assert client.post(path + "/submit", headers=headers(owner["headers"], 1)).status_code == 200
    assert (
        client.post(
            path + "/review",
            headers=headers(editor_headers, 2),
            json={"action": "request_changes", "reason": "Нужно уточнить выводы"},
        ).status_code
        == 200
    )
    assert (
        client.patch(
            path,
            headers=headers(owner["headers"], 3),
            json={"body": "Уточнённый текст с подтверждёнными выводами."},
        ).status_code
        == 200
    )
    assert client.post(path + "/submit", headers=headers(owner["headers"], 4)).status_code == 200
    assert (
        client.post(
            path + "/review", headers=headers(editor_headers, 2), json={"action": "approve"}
        ).status_code
        == 412
    )
    result = client.post(
        path + "/review", headers=headers(editor_headers, 5), json={"action": "approve"}
    )
    assert result.status_code == 200
    assert result.json()["approved_version"] == 2
    return path


def test_scheduler_races_manual_publication(
    client, register_author, create_article, editor_headers
):
    owner = register_author()
    article = create_article(owner["headers"])
    path = approved_article(client, owner, article, editor_headers)
    reply = client.post(
        path + "/schedule",
        headers=headers(editor_headers, 6),
        json={
            "publish_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    assert reply.status_code == 200
    with SessionLocal.begin() as db:
        db.execute(
            update(Article)
            .where(Article.id == UUID(article["id"]))
            .values(scheduled_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    def scheduler():
        with SessionLocal() as db:
            return services.publish_due_articles(db)

    manual, scheduled = parallel(
        lambda: client.post(path + "/publish", headers=headers(editor_headers, 7)), scheduler
    )
    assert (manual.status_code, scheduled) in [(200, 0), (412, 1)]
    public = client.get("/api/v1/public/articles/" + article["slug"]).json()
    assert public["version"] == 2
    assert public["body"].startswith("Уточнённый текст")
    with SessionLocal() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(
                    OutboxEvent.event_type == "article.published",
                    OutboxEvent.payload["article_id"].astext == article["id"],
                )
            )
            == 1
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(ArticleHistory)
                .where(
                    ArticleHistory.article_id == UUID(article["id"]),
                    ArticleHistory.to_status == "published",
                )
            )
            == 1
        )


def make_import(client, editor_headers, size=2):
    response = client.post(
        "/api/v1/import-jobs",
        headers=editor_headers,
        json={
            "items": [
                {
                    "slug": "import-" + uuid4().hex,
                    "title": f"Материал {i}",
                    "summary": "Проверка импорта",
                    "body": "Полный текст импортируемого материала.",
                }
                for i in range(size)
            ]
        },
    )
    assert response.status_code == 202
    return UUID(response.json()["id"])


def test_import_resumes_after_committed_row(client, editor_headers, monkeypatch):
    job_id = make_import(client, editor_headers)
    original = services.add_outbox
    count = 0

    def fail_second(db, event_type, payload):
        nonlocal count
        count += 1
        if count == 2:
            raise RuntimeError("Сбой второй строки")
        original(db, event_type, payload)

    with monkeypatch.context() as patch:
        patch.setattr(services, "add_outbox", fail_second)
        with pytest.raises(RuntimeError, match="Сбой второй строки"):
            services.process_next_import()
    with SessionLocal.begin() as db:
        job = services.get_import_job(db, job_id)
        assert job.processed_items == 1 and job.attempts == 1
        first_id = job.items[0].article_id
        assert first_id is not None and job.items[1].article_id is None
        job.next_attempt_at = datetime.now(UTC)
    assert services.process_next_import()
    with SessionLocal() as db:
        job = services.get_import_job(db, job_id)
        assert job.status == "completed" and job.processed_items == 2
        assert job.items[0].article_id == first_id
        assert (
            db.scalar(
                select(func.count())
                .select_from(ArticleVersion)
                .where(ArticleVersion.article_id == first_id)
            )
            == 1
        )


def test_two_workers_do_not_duplicate_import(client, editor_headers):
    job_id = make_import(client, editor_headers, size=8)

    def work():
        with SessionLocal() as db:
            return services.process_import_job(db, job_id).processed_items

    assert parallel(work, work) == [8, 8]
    with SessionLocal() as db:
        job = services.get_import_job(db, job_id)
        assert len({item.article_id for item in job.items}) == 8
        assert job.failed_items == 0


def test_import_failure_budget_and_manual_retry(client, editor_headers, monkeypatch):
    job_id = make_import(client, editor_headers, size=1)

    def fail(*args, **kwargs):
        raise RuntimeError("Временный сбой")

    with monkeypatch.context() as patch:
        patch.setattr(services, "process_import_job", fail)
        for _ in range(5):
            with pytest.raises(RuntimeError, match="Временный сбой"):
                services.process_next_import()
            with SessionLocal.begin() as db:
                db.get(ImportJob, job_id).next_attempt_at = datetime.now(UTC)
    with SessionLocal() as db:
        assert db.get(ImportJob, job_id).status == "failed"
    response = client.post(
        f"/api/v1/import-jobs/{job_id}/retry",
        headers=editor_headers,
        json={"reason": "Зависимость восстановлена"},
    )
    assert response.status_code == 200
    assert services.process_next_import()
    with SessionLocal() as db:
        assert db.get(ImportJob, job_id).status == "completed"
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(
                    OutboxEvent.payload["job_id"].astext == str(job_id),
                    OutboxEvent.payload["reason"].astext == "Зависимость восстановлена",
                )
            )
            == 1
        )


def test_broker_confirms_routing_and_quarantine(client, editor_headers):
    import pika

    from publishflow.broker import (
        ARTICLE_QUEUE,
        IMPORT_QUEUE,
        INVALID_QUEUE,
        connect,
        declare_topology,
    )
    from publishflow.config import get_settings
    from publishflow.worker.importer import consume_one
    from publishflow.worker.publisher import publish_batch

    assert "rabbitmq-test" in get_settings().rabbitmq_url
    make_import(client, editor_headers, size=1)
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        for queue in (ARTICLE_QUEUE, IMPORT_QUEUE, INVALID_QUEUE):
            channel.queue_purge(queue)
        assert publish_batch(channel) > 0
        assert consume_one(channel)
        while consume_one(channel):
            pass
        for invalid_body in (b"{broken", b'{"event_id": 1, "job_id": 2}', b"null", b"[]"):
            channel.basic_publish(
                exchange="",
                routing_key=IMPORT_QUEUE,
                body=invalid_body,
                properties=pika.BasicProperties(delivery_mode=2),
                mandatory=True,
            )
            assert consume_one(channel)
            method, _, body = channel.basic_get(INVALID_QUEUE, auto_ack=True)
            assert method is not None and body == invalid_body
        method, _, _ = channel.basic_get(ARTICLE_QUEUE, auto_ack=True)
        assert method is not None
    finally:
        connection.close()
    assert services.process_next_import()


def test_failed_publish_remains_in_outbox(monkeypatch):
    from publishflow.worker import publisher

    with SessionLocal.begin() as db:
        event = OutboxEvent(event_type="article.created", payload={"article_id": str(uuid4())})
        db.add(event)
        db.flush()
        event_id = event.id

    def fail(*args):
        raise RuntimeError("Нет подтверждения брокера")

    monkeypatch.setattr(publisher, "publish_event", fail)
    publisher.publish_batch(object())
    with SessionLocal() as db:
        event = db.get(OutboxEvent, event_id)
        assert event.published_at is None and event.attempts == 1
