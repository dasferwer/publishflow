from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from publishflow.broker import check_connection
from publishflow.dependencies import CurrentUser, DbSession, EditorUser
from publishflow.models import (
    Article,
    ArticleStatus,
    ArticleVersion,
    ImportJob,
    ImportStatus,
    User,
    UserRole,
)
from publishflow.schemas import (
    ArticleCreate,
    ArticleDetail,
    ArticleList,
    ArticleRead,
    ArticleUpdate,
    ImportJobCreate,
    ImportJobRead,
    LoginRequest,
    PublicArticle,
    ReviewRequest,
    ScheduleRequest,
    TokenResponse,
    UserCreate,
    UserRead,
    VersionRead,
)
from publishflow.security import create_access_token, hash_password, verify_password
from publishflow.services import (
    EDITOR_ROLES,
    add_outbox,
    archive_article,
    count_articles,
    create_article,
    create_import_job,
    get_article,
    get_import_job,
    now_utc,
    publish_article,
    require_article_access,
    review_article,
    schedule_article,
    submit_article,
    update_article,
)

router = APIRouter()


def expected_revision(if_match: Annotated[str | None, Header()] = None) -> int:
    if if_match is None:
        raise HTTPException(status_code=428, detail="Передайте If-Match с текущей ревизией")
    value = if_match[1:-1]
    if (
        len(if_match) > 20
        or not if_match.startswith('"')
        or not if_match.endswith('"')
        or not value.isascii()
        or not value.isdigit()
        or int(value) < 1
    ):
        raise HTTPException(status_code=400, detail='Ожидается If-Match: "1"')
    return int(value)


Revision = Annotated[int, Depends(expected_revision)]


@router.get("/health", tags=["service"])
def health(db: DbSession) -> dict[str, str]:
    database = "ok"
    rabbitmq = "ok"
    try:
        db.execute(text("SELECT 1"))
    except Exception:  # pragma: no cover - ответ health при сбое зависимости
        database = "error"
    try:
        check_connection()
    except Exception:  # pragma: no cover - ответ health при сбое зависимости
        rabbitmq = "error"
    if database != "ok" or rabbitmq != "ok":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"database": database, "rabbitmq": rabbitmq},
        )
    return {"status": "ok", "database": database, "rabbitmq": rabbitmq}


@router.post(
    "/api/v1/auth/register",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    tags=["auth"],
)
def register(data: UserCreate, db: DbSession) -> User:
    user = User(
        email=str(data.email).lower(),
        full_name=data.full_name,
        password_hash=hash_password(data.password),
        role=UserRole.AUTHOR,
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Email already registered") from None
    db.refresh(user)
    return user


@router.post("/api/v1/auth/login", response_model=TokenResponse, tags=["auth"])
def login(data: LoginRequest, db: DbSession) -> TokenResponse:
    user = db.scalar(select(User).where(User.email == str(data.email).lower()))
    if user is None or not user.is_active or not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return TokenResponse(access_token=create_access_token(user.id, str(user.role)))


@router.get("/api/v1/users/me", response_model=UserRead, tags=["auth"])
def read_current_user(current_user: CurrentUser) -> User:
    return current_user


@router.post(
    "/api/v1/articles",
    response_model=ArticleRead,
    status_code=status.HTTP_201_CREATED,
    tags=["articles"],
)
def create_article_endpoint(data: ArticleCreate, db: DbSession, user: CurrentUser) -> Article:
    return create_article(db, user, data)


@router.get("/api/v1/articles", response_model=ArticleList, tags=["articles"])
def list_articles(
    db: DbSession,
    user: CurrentUser,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    article_status: Annotated[ArticleStatus | None, Query(alias="status")] = None,
) -> ArticleList:
    statement = select(Article)
    if user.role not in EDITOR_ROLES:
        statement = statement.where(Article.author_id == user.id)
    if article_status is not None:
        statement = statement.where(Article.status == article_status)
    items = list(
        db.scalars(statement.order_by(Article.created_at.desc()).offset(offset).limit(limit))
    )
    return ArticleList(
        items=[ArticleRead.model_validate(item) for item in items],
        total=count_articles(db, user, article_status),
        limit=limit,
        offset=offset,
    )


@router.get("/api/v1/articles/{article_id}", response_model=ArticleDetail, tags=["articles"])
def read_article(article_id: UUID, db: DbSession, user: CurrentUser) -> Article:
    article = get_article(db, article_id, details=True)
    require_article_access(article, user)
    return article


@router.patch("/api/v1/articles/{article_id}", response_model=ArticleRead, tags=["articles"])
def update_article_endpoint(
    article_id: UUID, data: ArticleUpdate, db: DbSession, user: CurrentUser, revision: Revision
) -> Article:
    return update_article(db, get_article(db, article_id), user, data, revision)


@router.get(
    "/api/v1/articles/{article_id}/versions",
    response_model=list[VersionRead],
    tags=["articles"],
)
def list_versions(article_id: UUID, db: DbSession, user: CurrentUser) -> list[ArticleVersion]:
    article = get_article(db, article_id)
    require_article_access(article, user)
    return list(
        db.scalars(
            select(ArticleVersion)
            .where(ArticleVersion.article_id == article.id)
            .order_by(ArticleVersion.version)
        )
    )


@router.post(
    "/api/v1/articles/{article_id}/submit",
    response_model=ArticleRead,
    tags=["workflow"],
)
def submit_article_endpoint(
    article_id: UUID, db: DbSession, user: CurrentUser, revision: Revision
) -> Article:
    return submit_article(db, get_article(db, article_id), user, revision)


@router.post(
    "/api/v1/articles/{article_id}/review",
    response_model=ArticleRead,
    tags=["workflow"],
)
def review_article_endpoint(
    article_id: UUID, data: ReviewRequest, db: DbSession, editor: EditorUser, revision: Revision
) -> Article:
    return review_article(
        db, get_article(db, article_id), editor, data.action, data.reason, revision
    )


@router.post(
    "/api/v1/articles/{article_id}/schedule",
    response_model=ArticleRead,
    tags=["workflow"],
)
def schedule_article_endpoint(
    article_id: UUID, data: ScheduleRequest, db: DbSession, editor: EditorUser, revision: Revision
) -> Article:
    return schedule_article(db, get_article(db, article_id), editor, data.publish_at, revision)


@router.post(
    "/api/v1/articles/{article_id}/publish",
    response_model=ArticleRead,
    tags=["workflow"],
)
def publish_article_endpoint(
    article_id: UUID, db: DbSession, editor: EditorUser, revision: Revision
) -> Article:
    return publish_article(db, get_article(db, article_id), editor, revision)


@router.post(
    "/api/v1/articles/{article_id}/archive",
    response_model=ArticleRead,
    tags=["workflow"],
)
def archive_article_endpoint(
    article_id: UUID, db: DbSession, editor: EditorUser, revision: Revision
) -> Article:
    return archive_article(db, get_article(db, article_id), editor, revision)


@router.get("/api/v1/public/articles", response_model=list[PublicArticle], tags=["public"])
def list_public_articles(
    db: DbSession, limit: int = Query(default=20, ge=1, le=100)
) -> list[PublicArticle]:
    rows = db.execute(
        select(Article, ArticleVersion)
        .join(
            ArticleVersion,
            (ArticleVersion.article_id == Article.id)
            & (ArticleVersion.version == Article.published_version),
        )
        .where(Article.status == ArticleStatus.PUBLISHED)
        .order_by(Article.published_at.desc())
        .limit(limit)
    ).all()
    return [
        PublicArticle(
            slug=article.slug,
            title=version.title,
            summary=version.summary,
            body=version.body,
            version=version.version,
            published_at=article.published_at,
        )
        for article, version in rows
    ]


@router.get("/api/v1/public/articles/{slug}", response_model=PublicArticle, tags=["public"])
def read_public_article(slug: str, db: DbSession) -> PublicArticle:
    row = db.execute(
        select(Article, ArticleVersion)
        .join(
            ArticleVersion,
            (ArticleVersion.article_id == Article.id)
            & (ArticleVersion.version == Article.published_version),
        )
        .where(Article.slug == slug, Article.status == ArticleStatus.PUBLISHED)
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Published article not found")
    article, version = row
    return PublicArticle(
        slug=article.slug,
        title=version.title,
        summary=version.summary,
        body=version.body,
        version=version.version,
        published_at=article.published_at,
    )


@router.post(
    "/api/v1/import-jobs",
    response_model=ImportJobRead,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["imports"],
)
def create_import_job_endpoint(data: ImportJobCreate, db: DbSession, editor: EditorUser) -> object:
    return create_import_job(db, editor, data)


@router.get("/api/v1/import-jobs", response_model=list[ImportJobRead], tags=["imports"])
def list_import_jobs(db: DbSession, editor: EditorUser) -> list[object]:
    del editor
    return list(
        db.scalars(
            select(ImportJob)
            .options(selectinload(ImportJob.items))
            .order_by(ImportJob.created_at.desc())
            .limit(100)
        )
    )


@router.get("/api/v1/import-jobs/{job_id}", response_model=ImportJobRead, tags=["imports"])
def read_import_job(job_id: UUID, db: DbSession, editor: EditorUser) -> object:
    del editor
    return get_import_job(db, job_id)


class RetryImportRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    reason: str = Field(min_length=5, max_length=500)


@router.post("/api/v1/import-jobs/{job_id}/retry", response_model=ImportJobRead, tags=["imports"])
def retry_import(
    job_id: UUID, data: RetryImportRequest, db: DbSession, editor: EditorUser
) -> ImportJob:
    job = db.scalar(select(ImportJob).where(ImportJob.id == job_id).with_for_update())
    if job is None:
        raise HTTPException(status_code=404, detail="Задание импорта не найдено")
    if job.status != ImportStatus.FAILED:
        raise HTTPException(status_code=409, detail="Повтор разрешён только после сбоя задания")
    job.status = ImportStatus.PENDING
    job.attempts, job.error = 0, None
    job.next_attempt_at = now_utc()
    add_outbox(
        db,
        "content.import.requested",
        {
            "job_id": str(job.id),
            "actor_id": str(editor.id),
            "reason": data.reason,
        },
    )
    db.commit()
    return get_import_job(db, job.id)
