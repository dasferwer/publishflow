from functools import lru_cache

from pydantic import EmailStr, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="PUBLISHFLOW_",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "PublishFlow API"
    app_version: str = "0.1.0"
    database_url: str
    rabbitmq_url: str
    jwt_secret: SecretStr
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    admin_email: EmailStr = "admin@example.com"
    admin_password: SecretStr = SecretStr("ChangeMe123!")
    editor_email: EmailStr = "editor@example.com"
    editor_password: SecretStr = SecretStr("ChangeMe123!")
    log_level: str = "INFO"
    rabbitmq_management_url: str = "http://rabbitmq:15672"
    article_retention_seconds: int | None = Field(default=None, gt=0)
    article_queue_max_bytes: int = Field(default=10485760, gt=0)
    article_retention_batch_size: int = Field(default=1000, gt=0, le=10000)

    @field_validator("article_retention_seconds", mode="before")
    @classmethod
    def empty_retention_is_disabled(cls, value: object) -> object:
        return None if value == "" else value


@lru_cache
def get_settings() -> Settings:
    return Settings()
