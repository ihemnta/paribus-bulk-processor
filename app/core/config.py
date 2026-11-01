"""
Centralized configuration for the Paribus bulk processing service.

Environment variables are the source of truth for deployment-specific
settings. Business logic must not hardcode external service URLs.
"""

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated application settings loaded from the environment."""

    app_name: str = "Paribus Bulk Processor"
    app_env: str = "local"
    log_level: str = "INFO"
    admin_api_key: str | None = None
    enable_real_upstream_writes: bool = False

    upstream_base_url: str = "https://hospital-directory.onrender.com"

    upstream_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        le=120,
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

    max_csv_bytes: int = Field(
        default=1_048_576,
        ge=1024,
        le=10_485_760,
    )

    database_url: str = Field(
        ...,
        description="Async SQLAlchemy PostgreSQL connection URL.",
    )

    upstream_max_connections: int = Field(
        default=5,
        ge=1,
        le=20,
    )

    worker_lease_seconds: int = Field(
        default=120,
        ge=30,
        le=900,
    )

    worker_poll_seconds: float = Field(
        default=2.0,
        ge=0.25,
        le=60,
    )

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        """Use the asyncpg dialect for PostgreSQL URLs."""
        if value.startswith("postgres://"):
            return "postgresql+asyncpg://" + value[len("postgres://") :]

        if value.startswith("postgresql://"):
            return "postgresql+asyncpg://" + value[len("postgresql://") :]

        return value


@lru_cache
def get_settings() -> Settings:
    """
    Return the cached application configuration.

    Caching avoids repeatedly parsing environment settings and keeps
    configuration consistent throughout the application lifecycle.
    """
    return Settings()
