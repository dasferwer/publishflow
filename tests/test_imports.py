from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from publishflow.db import SessionLocal
from publishflow.models import ImportStatus
from publishflow.services import get_import_job, process_import_job


def test_import_job_keeps_valid_items_when_one_slug_exists(
    client: TestClient, editor_headers: dict[str, str]
) -> None:
    duplicate_slug = f"duplicate-{uuid4().hex[:8]}"
    existing = client.post(
        "/api/v1/articles",
        headers=editor_headers,
        json={
            "slug": duplicate_slug,
            "title": "Existing article",
            "summary": "This slug is already reserved.",
            "body": "The existing article ensures one import row fails safely.",
        },
    )
    assert existing.status_code == 201

    valid_slug = f"imported-{uuid4().hex[:8]}"
    response = client.post(
        "/api/v1/import-jobs",
        headers=editor_headers,
        json={
            "items": [
                {
                    "slug": duplicate_slug,
                    "title": "Duplicate",
                    "summary": "This item must fail without aborting the job.",
                    "body": "A duplicate slug is handled inside a nested database transaction.",
                },
                {
                    "slug": valid_slug,
                    "title": "Imported article",
                    "summary": "This item must be imported successfully.",
                    "body": "A valid article is stored even though another item in the job fails.",
                },
            ]
        },
    )
    assert response.status_code == 202
    job_id = UUID(response.json()["id"])

    with SessionLocal() as db:
        processed = process_import_job(db, job_id)

    assert processed.status == ImportStatus.COMPLETED_WITH_ERRORS
    assert processed.processed_items == 2
    assert processed.failed_items == 1
    assert [item.status.value for item in processed.items] == ["failed", "imported"]

    with SessionLocal() as db:
        repeated = process_import_job(db, job_id)
        loaded = get_import_job(db, job_id)

    assert repeated.processed_items == 2
    assert loaded.failed_items == 1
