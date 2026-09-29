"""Проверить сохранность данных при откате и повторном применении новой миграции."""

import hashlib
import json
import subprocess

from sqlalchemy import text

from publishflow.db import engine

TABLES = (
    "users",
    "articles",
    "article_versions",
    "article_history",
    "import_jobs",
    "import_items",
    "outbox_events",
)


def snapshot() -> str:
    values = {}
    with engine.connect() as connection:
        for table in TABLES:
            rows = list(
                connection.execute(text(f"SELECT to_jsonb(t) FROM {table} t ORDER BY id")).scalars()
            )
            excluded = (
                {"revision", "approved_version", "published_version"}
                if table == "articles"
                else set()
            )
            if table == "import_jobs":
                excluded = {"attempts", "next_attempt_at"}
            values[table] = [
                {key: value for key, value in row.items() if key not in excluded} for row in rows
            ]
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def main() -> None:
    if engine.url.database != "publishflow_test":
        raise RuntimeError("Проверка разрешена только в БД publishflow_test")
    before = snapshot()
    subprocess.run(["alembic", "downgrade", "20260831_0001"], check=True)
    subprocess.run(["alembic", "upgrade", "head"], check=True)
    if snapshot() != before:
        raise RuntimeError("Исторические данные изменились при миграции")
    print("Бизнес-данные сохранены после отката и повторного применения миграции")


if __name__ == "__main__":
    main()
