from collections.abc import Callable, Generator
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from publishflow.config import get_settings
from publishflow.db import engine
from publishflow.main import app
from publishflow.seed import seed_database


@pytest.fixture(scope="session", autouse=True)
def reset_database() -> Generator[None, None, None]:
    with engine.begin() as connection:
        connection.execute(
            text(
                "TRUNCATE TABLE outbox_events, import_items, import_jobs, article_history, "
                "article_versions, articles, users RESTART IDENTITY CASCADE"
            )
        )
    seed_database()
    yield


@pytest.fixture(scope="session")
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as test_client:
        yield test_client


def login(client: TestClient, email: str, password: str) -> dict[str, str]:
    response = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.fixture(scope="session")
def editor_headers(client: TestClient) -> dict[str, str]:
    settings = get_settings()
    return login(
        client,
        str(settings.editor_email),
        settings.editor_password.get_secret_value(),
    )


@pytest.fixture
def register_author(client: TestClient) -> Callable[[], dict[str, Any]]:
    def factory() -> dict[str, Any]:
        email = f"author-{uuid4()}@example.com"
        password = "StrongPass123!"
        response = client.post(
            "/api/v1/auth/register",
            json={"email": email, "full_name": "Test Author", "password": password},
        )
        assert response.status_code == 201
        return {
            "id": response.json()["id"],
            "email": email,
            "headers": login(client, email, password),
        }

    return factory


@pytest.fixture
def create_article(client: TestClient) -> Callable[[dict[str, str]], dict[str, Any]]:
    def factory(headers: dict[str, str]) -> dict[str, Any]:
        slug = f"article-{uuid4().hex[:10]}"
        response = client.post(
            "/api/v1/articles",
            headers=headers,
            json={
                "slug": slug,
                "title": "Reliable background processing",
                "summary": "A concise guide to resilient worker design.",
                "body": "This body contains enough detail to create the first immutable version.",
            },
        )
        assert response.status_code == 201
        return response.json()

    return factory
