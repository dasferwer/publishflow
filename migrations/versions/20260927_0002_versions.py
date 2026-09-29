"""Зафиксировать согласованную версию и состояние восстановления импорта."""

import sqlalchemy as sa
from alembic import op

revision = "20260927_0002"
down_revision = "20260831_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "articles", sa.Column("revision", sa.Integer(), nullable=False, server_default="1")
    )
    op.add_column("articles", sa.Column("approved_version", sa.Integer()))
    op.add_column("articles", sa.Column("published_version", sa.Integer()))
    op.execute(
        "UPDATE articles SET approved_version = current_version "
        "WHERE status IN ('approved','scheduled','published','archived')"
    )
    op.execute(
        "UPDATE articles SET published_version = current_version "
        "WHERE status IN ('published','archived')"
    )
    op.create_check_constraint("ck_articles_positive_revision", "articles", "revision > 0")
    op.create_check_constraint(
        "ck_articles_approved_version",
        "articles",
        "status NOT IN ('approved','scheduled','published','archived') OR "
        "(approved_version IS NOT NULL AND approved_version = current_version)",
    )
    op.create_check_constraint(
        "ck_articles_published_version",
        "articles",
        "status NOT IN ('published','archived') OR "
        "(published_version IS NOT NULL AND published_version = approved_version)",
    )
    op.add_column(
        "import_jobs", sa.Column("attempts", sa.Integer(), nullable=False, server_default="0")
    )
    op.add_column(
        "import_jobs",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_import_jobs_due", "import_jobs", ["status", "next_attempt_at"])


def downgrade() -> None:
    op.drop_index("ix_import_jobs_due", "import_jobs")
    op.drop_column("import_jobs", "next_attempt_at")
    op.drop_column("import_jobs", "attempts")
    for name in (
        "ck_articles_published_version",
        "ck_articles_approved_version",
        "ck_articles_positive_revision",
    ):
        op.drop_constraint(name, "articles")
    for name in ("published_version", "approved_version", "revision"):
        op.drop_column("articles", name)
