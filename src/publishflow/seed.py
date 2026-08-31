import logging

from sqlalchemy import select

from publishflow.config import get_settings
from publishflow.db import SessionLocal
from publishflow.models import User, UserRole
from publishflow.security import hash_password

logger = logging.getLogger(__name__)


def ensure_user(email: str, full_name: str, password: str, role: UserRole) -> None:
    with SessionLocal.begin() as db:
        user = db.scalar(select(User).where(User.email == email.lower()))
        if user is None:
            db.add(
                User(
                    email=email.lower(),
                    full_name=full_name,
                    password_hash=hash_password(password),
                    role=role,
                )
            )


def seed_database() -> None:
    settings = get_settings()
    ensure_user(
        str(settings.admin_email),
        "PublishFlow Admin",
        settings.admin_password.get_secret_value(),
        UserRole.ADMIN,
    )
    ensure_user(
        str(settings.editor_email),
        "PublishFlow Editor",
        settings.editor_password.get_secret_value(),
        UserRole.EDITOR,
    )
    logger.info("Seed completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_database()
