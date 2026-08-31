from collections.abc import Callable
from typing import Any

from fastapi.testclient import TestClient


def test_article_version_and_workflow(
    client: TestClient,
    register_author: Callable[[], dict[str, Any]],
    create_article: Callable[[dict[str, str]], dict[str, Any]],
    editor_headers: dict[str, str],
) -> None:
    author = register_author()
    article = create_article(author["headers"])
    article_id = article["id"]

    update = client.patch(
        f"/api/v1/articles/{article_id}",
        headers=author["headers"],
        json={"body": "The revised body explains retries, idempotency and monitoring clearly."},
    )
    assert update.status_code == 200
    assert update.json()["current_version"] == 2

    versions = client.get(f"/api/v1/articles/{article_id}/versions", headers=author["headers"])
    assert versions.status_code == 200
    assert [item["version"] for item in versions.json()] == [1, 2]

    submitted = client.post(f"/api/v1/articles/{article_id}/submit", headers=author["headers"])
    assert submitted.json()["status"] == "in_review"

    approved = client.post(
        f"/api/v1/articles/{article_id}/review",
        headers=editor_headers,
        json={"action": "approve"},
    )
    assert approved.json()["status"] == "approved"

    published = client.post(f"/api/v1/articles/{article_id}/publish", headers=editor_headers)
    assert published.json()["status"] == "published"

    public = client.get(f"/api/v1/public/articles/{article['slug']}")
    assert public.status_code == 200
    assert public.json()["version"] == 2
    assert "idempotency" in public.json()["body"]


def test_author_cannot_read_another_authors_draft(
    client: TestClient,
    register_author: Callable[[], dict[str, Any]],
    create_article: Callable[[dict[str, str]], dict[str, Any]],
) -> None:
    owner = register_author()
    stranger = register_author()
    article = create_article(owner["headers"])

    response = client.get(f"/api/v1/articles/{article['id']}", headers=stranger["headers"])

    assert response.status_code == 403


def test_request_changes_requires_reason(
    client: TestClient,
    register_author: Callable[[], dict[str, Any]],
    create_article: Callable[[dict[str, str]], dict[str, Any]],
    editor_headers: dict[str, str],
) -> None:
    author = register_author()
    article = create_article(author["headers"])
    client.post(f"/api/v1/articles/{article['id']}/submit", headers=author["headers"])

    response = client.post(
        f"/api/v1/articles/{article['id']}/review",
        headers=editor_headers,
        json={"action": "request_changes"},
    )

    assert response.status_code == 422
