from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from publishflow.models import (
    Article,
    ArticleHistory,
    ArticleStatus,
    ArticleVersion,
    ImportItem,
    ImportItemStatus,
    ImportJob,
    ImportStatus,
    OutboxEvent,
    User,
    UserRole,
)
from publishflow.schemas import ArticleCreate, ArticleUpdate, ImportJobCreate

EDITOR_ROLES = {UserRole.EDITOR, UserRole.ADMIN}


def now_utc() -> datetime:
    return datetime.now(UTC)


def add_outbox(db: Session, event_type: str, payload: dict[str, object]) -> None:
    db.add(OutboxEvent(event_type=event_type, payload=payload))


def add_history(
    db: Session,
    article: Article,
    actor_id: UUID | None,
    action: str,
    from_status: ArticleStatus | None = None,
    to_status: ArticleStatus | None = None,
    details: dict[str, object] | None = None,
) -> None:
    db.add(
        ArticleHistory(
            article_id=article.id,
            actor_id=actor_id,
            action=action,
            from_status=str(from_status) if from_status else None,
            to_status=str(to_status) if to_status else None,
            details=details or {},
        )
    )


def get_article(db: Session, article_id: UUID, *, details: bool = False) -> Article:
    statement = select(Article).where(Article.id == article_id)
    if details:
        statement = statement.options(selectinload(Article.versions), selectinload(Article.history))
    article = db.scalar(statement)
    if article is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Article not found")
    return article


def require_article_access(article: Article, user: User) -> None:
    if user.role not in EDITOR_ROLES and article.author_id != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")


def lock_article(db: Session, article: Article, user: User, revision: int) -> None:
    require_article_access(article, user)
    db.refresh(article, with_for_update=True)
    if article.revision != revision:
        raise HTTPException(
            status_code=412, detail="Материал уже изменён; получите актуальную ревизию"
        )


def article_payload(article: Article) -> dict[str, object]:
    return {
        "article_id": str(article.id),
        "slug": article.slug,
        "status": str(article.status),
        "version": article.current_version,
        "revision": article.revision,
        "approved_version": article.approved_version,
    }


def create_article(db: Session, user: User, data: ArticleCreate) -> Article:
    article = Article(
        slug=data.slug,
        title=data.title,
        summary=data.summary,
        author_id=user.id,
    )
    db.add(article)
    try:
        db.flush()
    except IntegrityError as exc:
        db.rollback()
        if getattr(getattr(exc.orig, "diag", None), "constraint_name", None) != "articles_slug_key":
            raise
        raise HTTPException(status_code=409, detail="Slug уже занят") from exc
    db.add(
        ArticleVersion(
            article_id=article.id,
            version=1,
            title=data.title,
            summary=data.summary,
            body=data.body,
            created_by=user.id,
        )
    )
    add_history(db, article, user.id, "created", to_status=ArticleStatus.DRAFT)
    add_outbox(db, "article.created", article_payload(article))
    db.commit()
    db.refresh(article)
    return article


def update_article(
    db: Session, article: Article, user: User, data: ArticleUpdate, revision: int
) -> Article:
    lock_article(db, article, user, revision)
    if article.status not in {ArticleStatus.DRAFT, ArticleStatus.CHANGES_REQUESTED}:
        raise HTTPException(status_code=409, detail="Only editable drafts can be changed")
    current = db.scalar(
        select(ArticleVersion).where(
            ArticleVersion.article_id == article.id,
            ArticleVersion.version == article.current_version,
        )
    )
    if current is None:
        raise RuntimeError("Current article version is missing")
    new_version = article.current_version + 1
    title = data.title if data.title is not None else current.title
    summary = data.summary if data.summary is not None else current.summary
    body = data.body if data.body is not None else current.body
    article.title = title
    article.summary = summary
    article.current_version = new_version
    article.revision += 1
    article.approved_version = None
    db.add(
        ArticleVersion(
            article_id=article.id,
            version=new_version,
            title=title,
            summary=summary,
            body=body,
            created_by=user.id,
        )
    )
    add_history(db, article, user.id, "version_created", details={"version": new_version})
    add_outbox(db, "article.version.created", article_payload(article))
    db.commit()
    db.refresh(article)
    return article


def transition_article(
    db: Session,
    article: Article,
    actor: User | None,
    to_status: ArticleStatus,
    action: str,
    details: dict[str, object] | None = None,
) -> Article:
    old_status = article.status
    article.revision += 1
    article.status = to_status
    if to_status == ArticleStatus.PUBLISHED:
        article.published_version = article.approved_version
        article.published_at = now_utc()
        article.scheduled_at = None
    add_history(
        db,
        article,
        actor.id if actor else None,
        action,
        old_status,
        to_status,
        {**(details or {}), "version": article.current_version, "revision": article.revision},
    )
    add_outbox(db, f"article.{to_status}", article_payload(article))
    db.commit()
    db.refresh(article)
    return article


def submit_article(db: Session, article: Article, user: User, revision: int) -> Article:
    lock_article(db, article, user, revision)
    if article.status not in {ArticleStatus.DRAFT, ArticleStatus.CHANGES_REQUESTED}:
        raise HTTPException(status_code=409, detail="Article cannot be submitted from this state")
    return transition_article(db, article, user, ArticleStatus.IN_REVIEW, "submitted")


def review_article(
    db: Session, article: Article, editor: User, action: str, reason: str | None, revision: int
) -> Article:
    lock_article(db, article, editor, revision)
    if article.status != ArticleStatus.IN_REVIEW:
        raise HTTPException(status_code=409, detail="Article is not awaiting review")
    if action == "approve":
        article.approved_version = article.current_version
        return transition_article(db, article, editor, ArticleStatus.APPROVED, "approved")
    return transition_article(
        db,
        article,
        editor,
        ArticleStatus.CHANGES_REQUESTED,
        "changes_requested",
        {"reason": reason or ""},
    )


def schedule_article(
    db: Session, article: Article, editor: User, publish_at: datetime, revision: int
) -> Article:
    lock_article(db, article, editor, revision)
    if article.status != ArticleStatus.APPROVED:
        raise HTTPException(status_code=409, detail="Only approved articles can be scheduled")
    if publish_at.tzinfo is None or publish_at <= now_utc():
        raise HTTPException(
            status_code=422, detail="publish_at must be a future timezone-aware time"
        )
    article.scheduled_at = publish_at
    return transition_article(
        db,
        article,
        editor,
        ArticleStatus.SCHEDULED,
        "scheduled",
        {"publish_at": publish_at.isoformat()},
    )


def publish_article(db: Session, article: Article, editor: User, revision: int) -> Article:
    lock_article(db, article, editor, revision)
    if article.status not in {ArticleStatus.APPROVED, ArticleStatus.SCHEDULED}:
        raise HTTPException(status_code=409, detail="Article cannot be published from this state")
    return transition_article(db, article, editor, ArticleStatus.PUBLISHED, "published")


def archive_article(db: Session, article: Article, editor: User, revision: int) -> Article:
    lock_article(db, article, editor, revision)
    if article.status != ArticleStatus.PUBLISHED:
        raise HTTPException(status_code=409, detail="Only published articles can be archived")
    return transition_article(db, article, editor, ArticleStatus.ARCHIVED, "archived")


def create_import_job(db: Session, editor: User, data: ImportJobCreate) -> ImportJob:
    job = ImportJob(requested_by=editor.id, total_items=len(data.items))
    db.add(job)
    db.flush()
    for position, item in enumerate(data.items, start=1):
        db.add(
            ImportItem(
                job_id=job.id,
                position=position,
                slug=item.slug,
                title=item.title,
                summary=item.summary,
                body=item.body,
            )
        )
    add_outbox(db, "content.import.requested", {"job_id": str(job.id)})
    db.commit()
    return get_import_job(db, job.id)


def get_import_job(db: Session, job_id: UUID) -> ImportJob:
    job = db.scalar(
        select(ImportJob).where(ImportJob.id == job_id).options(selectinload(ImportJob.items))
    )
    if job is None:
        raise HTTPException(status_code=404, detail="Import job not found")
    return job


def process_import_job(db: Session, job_id: UUID, *, max_items: int = 100) -> ImportJob:
    for _ in range(max_items):
        job = db.scalar(
            select(ImportJob)
            .where(ImportJob.id == job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if job is None:
            raise LookupError("Задание импорта не найдено")
        if job.status in {
            ImportStatus.COMPLETED,
            ImportStatus.COMPLETED_WITH_ERRORS,
            ImportStatus.FAILED,
        }:
            return get_import_job(db, job.id)
        if job.next_attempt_at > now_utc():
            return get_import_job(db, job.id)
        item = db.scalar(
            select(ImportItem)
            .where(ImportItem.job_id == job.id, ImportItem.status == ImportItemStatus.PENDING)
            .order_by(ImportItem.position)
            .limit(1)
        )
        if item is None:
            break
        job.status = ImportStatus.PROCESSING
        job.started_at = job.started_at or now_utc()
        try:
            with db.begin_nested():
                article = Article(
                    slug=item.slug,
                    title=item.title,
                    summary=item.summary,
                    author_id=job.requested_by,
                )
                db.add(article)
                db.flush()
                db.add(
                    ArticleVersion(
                        article_id=article.id,
                        version=1,
                        title=item.title,
                        summary=item.summary,
                        body=item.body,
                        created_by=job.requested_by,
                    )
                )
                add_history(
                    db, article, job.requested_by, "imported", to_status=ArticleStatus.DRAFT
                )
                add_outbox(db, "article.imported", article_payload(article))
                db.flush()
            item.status, item.article_id = ImportItemStatus.IMPORTED, article.id
        except IntegrityError as exc:
            if (
                getattr(getattr(exc.orig, "diag", None), "constraint_name", None)
                != "articles_slug_key"
            ):
                raise
            item.status, item.error = ImportItemStatus.FAILED, "Slug уже занят"
            job.failed_items += 1
        job.processed_items += 1
        job.error = None
        if job.processed_items == job.total_items:
            job.status = (
                ImportStatus.COMPLETED_WITH_ERRORS if job.failed_items else ImportStatus.COMPLETED
            )
            job.completed_at = now_utc()
        # Прогресс строки фиксируется вместе со статьёй: падение не откатывает готовые строки.
        db.commit()
    return get_import_job(db, job_id)


def process_next_import() -> bool:
    from publishflow.db import SessionLocal

    job_id = None
    try:
        with SessionLocal() as db:
            job_id = db.scalar(
                select(ImportJob.id)
                .where(
                    ImportJob.status.in_([ImportStatus.PENDING, ImportStatus.PROCESSING]),
                    ImportJob.next_attempt_at <= now_utc(),
                )
                .order_by(ImportJob.next_attempt_at, ImportJob.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if job_id is None:
                return False
            process_import_job(db, job_id, max_items=10)
        return True
    except Exception:
        if job_id is not None:
            with SessionLocal.begin() as db:
                job = db.scalar(select(ImportJob).where(ImportJob.id == job_id).with_for_update())
                if job is not None and job.status in {
                    ImportStatus.PENDING,
                    ImportStatus.PROCESSING,
                }:
                    job.attempts += 1
                    job.error = "Сбой обработки; подробности в журнале worker"
                    job.next_attempt_at = now_utc() + timedelta(seconds=min(300, 2**job.attempts))
                    if job.attempts >= 5:
                        job.status = ImportStatus.FAILED
        raise


def publish_due_articles(db: Session) -> int:
    articles = list(
        db.scalars(
            select(Article)
            .where(
                Article.status == ArticleStatus.SCHEDULED,
                Article.scheduled_at <= now_utc(),
            )
            .order_by(Article.scheduled_at, Article.id)
            .limit(100)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    )
    for article in articles:
        old_status = article.status
        article.revision += 1
        article.status = ArticleStatus.PUBLISHED
        article.published_version = article.approved_version
        article.published_at = now_utc()
        article.scheduled_at = None
        add_history(
            db, article, None, "published_by_scheduler", old_status, ArticleStatus.PUBLISHED
        )
        add_outbox(db, "article.published", article_payload(article))
    db.commit()
    return len(articles)


def count_articles(db: Session, user: User, article_status: ArticleStatus | None = None) -> int:
    statement = select(func.count(Article.id))
    if user.role not in EDITOR_ROLES:
        statement = statement.where(Article.author_id == user.id)
    if article_status is not None:
        statement = statement.where(Article.status == article_status)
    return int(db.scalar(statement) or 0)
