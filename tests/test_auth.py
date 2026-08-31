from collections.abc import Callable
from typing import Any

from fastapi.testclient import TestClient


def test_register_login_and_me(
    client: TestClient, register_author: Callable[[], dict[str, Any]]
) -> None:
    author = register_author()

    response = client.get("/api/v1/users/me", headers=author["headers"])

    assert response.status_code == 200
    assert response.json()["email"] == author["email"]
    assert response.json()["role"] == "author"


def test_protected_route_requires_token(client: TestClient) -> None:
    assert client.get("/api/v1/articles").status_code == 401
