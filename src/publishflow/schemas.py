from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator

from publishflow.models import ArticleStatus, ImportItemStatus, ImportStatus, UserRole


class UserCreate(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=2, max_length=120)
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: EmailStr
    full_name: str
    role: UserRole
    is_active: bool
    created_at: datetime


class ArticleCreate(BaseModel):
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", min_length=3, max_length=160)
    title: str = Field(min_length=3, max_length=240)
    summary: str = Field(min_length=3, max_length=500)
    body: str = Field(min_length=10, max_length=100_000)


class ArticleUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=3, max_length=240)
    summary: str | None = Field(default=None, min_length=3, max_length=500)
    body: str | None = Field(default=None, min_length=10, max_length=100_000)

    @model_validator(mode="after")
    def require_change(self) -> "ArticleUpdate":
        if self.title is None and self.summary is None and self.body is None:
            raise ValueError("At least one field must be changed")
        return self


class ArticleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    slug: str
    title: str
    summary: str
    status: ArticleStatus
    author_id: UUID
    current_version: int
    scheduled_at: datetime | None
    published_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ArticleList(BaseModel):
    items: list[ArticleRead]
    total: int
    limit: int
    offset: int


class VersionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    article_id: UUID
    version: int
    title: str
    summary: str
    body: str
    created_by: UUID
    created_at: datetime


class HistoryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    actor_id: UUID | None
    action: str
    from_status: str | None
    to_status: str | None
    details: dict[str, object]
    created_at: datetime


class ArticleDetail(ArticleRead):
    versions: list[VersionRead]
    history: list[HistoryRead]


class ReviewRequest(BaseModel):
    action: Literal["approve", "request_changes"]
    reason: str | None = Field(default=None, max_length=1000)

    @model_validator(mode="after")
    def require_reason_for_changes(self) -> "ReviewRequest":
        if self.action == "request_changes" and not self.reason:
            raise ValueError("A reason is required when requesting changes")
        return self


class ScheduleRequest(BaseModel):
    publish_at: datetime


class PublicArticle(BaseModel):
    slug: str
    title: str
    summary: str
    body: str
    version: int
    published_at: datetime


class ImportArticle(BaseModel):
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", min_length=3, max_length=160)
    title: str = Field(min_length=3, max_length=240)
    summary: str = Field(min_length=3, max_length=500)
    body: str = Field(min_length=10, max_length=100_000)


class ImportJobCreate(BaseModel):
    items: list[ImportArticle] = Field(min_length=1, max_length=100)


class ImportItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    position: int
    slug: str
    title: str
    status: ImportItemStatus
    article_id: UUID | None
    error: str | None


class ImportJobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    requested_by: UUID
    status: ImportStatus
    total_items: int
    processed_items: int
    failed_items: int
    error: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    items: list[ImportItemRead]
