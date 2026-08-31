from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from publishflow.db import Base


class UserRole(StrEnum):
    AUTHOR = "author"
    EDITOR = "editor"
    ADMIN = "admin"


class ArticleStatus(StrEnum):
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    CHANGES_REQUESTED = "changes_requested"
    APPROVED = "approved"
    SCHEDULED = "scheduled"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class ImportStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


class ImportItemStatus(StrEnum):
    PENDING = "pending"
    IMPORTED = "imported"
    FAILED = "failed"


class UUIDMixin:
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class User(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("role IN ('author', 'editor', 'admin')", name="ck_users_valid_role"),
        UniqueConstraint("email", name="users_email_key"),
    )

    email: Mapped[str] = mapped_column(String(320), nullable=False)
    full_name: Mapped[str] = mapped_column(String(120), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(String(20), default=UserRole.AUTHOR, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Article(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "articles"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'in_review', 'changes_requested', 'approved', "
            "'scheduled', 'published', 'archived')",
            name="ck_articles_valid_status",
        ),
        CheckConstraint("current_version > 0", name="ck_articles_positive_version"),
        UniqueConstraint("slug", name="articles_slug_key"),
        Index("ix_articles_author_created", "author_id", "created_at"),
        Index("ix_articles_status_schedule", "status", "scheduled_at"),
    )

    slug: Mapped[str] = mapped_column(String(160), nullable=False)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[ArticleStatus] = mapped_column(
        String(30), default=ArticleStatus.DRAFT, nullable=False
    )
    author_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    current_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    author: Mapped[User] = relationship()
    versions: Mapped[list["ArticleVersion"]] = relationship(
        back_populates="article", order_by="ArticleVersion.version"
    )
    history: Mapped[list["ArticleHistory"]] = relationship(
        back_populates="article", order_by="ArticleHistory.created_at"
    )


class ArticleVersion(UUIDMixin, Base):
    __tablename__ = "article_versions"
    __table_args__ = (
        CheckConstraint("version > 0", name="ck_article_versions_positive_version"),
        UniqueConstraint("article_id", "version", name="article_versions_article_version_key"),
    )

    article_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("articles.id", ondelete="CASCADE"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    article: Mapped[Article] = relationship(back_populates="versions")
    creator: Mapped[User] = relationship()


class ArticleHistory(UUIDMixin, Base):
    __tablename__ = "article_history"
    __table_args__ = (Index("ix_article_history_article_created", "article_id", "created_at"),)

    article_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("articles.id", ondelete="CASCADE"), nullable=False
    )
    actor_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(80), nullable=False)
    from_status: Mapped[str | None] = mapped_column(String(30))
    to_status: Mapped[str | None] = mapped_column(String(30))
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    article: Mapped[Article] = relationship(back_populates="history")
    actor: Mapped[User | None] = relationship()


class ImportJob(UUIDMixin, Base):
    __tablename__ = "import_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'completed', 'completed_with_errors', 'failed')",
            name="ck_import_jobs_valid_status",
        ),
        Index("ix_import_jobs_requester_created", "requested_by", "created_at"),
    )

    requested_by: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[ImportStatus] = mapped_column(
        String(40), default=ImportStatus.PENDING, nullable=False
    )
    total_items: Mapped[int] = mapped_column(Integer, nullable=False)
    processed_items: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_items: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    requester: Mapped[User] = relationship()
    items: Mapped[list["ImportItem"]] = relationship(
        back_populates="job", order_by="ImportItem.position"
    )


class ImportItem(UUIDMixin, Base):
    __tablename__ = "import_items"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'imported', 'failed')",
            name="ck_import_items_valid_status",
        ),
        UniqueConstraint("job_id", "position", name="import_items_job_position_key"),
    )

    job_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("import_jobs.id", ondelete="CASCADE"), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    slug: Mapped[str] = mapped_column(String(160), nullable=False)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[ImportItemStatus] = mapped_column(
        String(20), default=ImportItemStatus.PENDING, nullable=False
    )
    article_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("articles.id", ondelete="SET NULL")
    )
    error: Mapped[str | None] = mapped_column(Text)

    job: Mapped[ImportJob] = relationship(back_populates="items")


class OutboxEvent(UUIDMixin, Base):
    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_unpublished", "published_at", "created_at"),)

    event_type: Mapped[str] = mapped_column(String(120), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
