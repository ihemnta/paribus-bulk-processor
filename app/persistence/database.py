"""
Asynchronous PostgreSQL engine and session management.

The engine manages pooled database connections.
Each request or background task must obtain its own session.

Transaction commits belong to the repository/service layer,
not to the session dependency.
"""

import logging
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
    pool_timeout=15,
)

SessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db_session() -> AsyncIterator[AsyncSession]:
    """
    Yield a new database session for a single unit of work.

    The caller is responsible for defining the transaction boundary.
    Closing the session rolls back any uncommitted transaction.

    Never share the same AsyncSession across concurrent tasks.
    """
    async with SessionFactory() as session:
        yield session


async def check_database_connection() -> bool:
    """
    Verify that PostgreSQL accepts a simple SQL query.

    This is a connectivity probe, not a replacement for
    checking whether migrations are up to date.
    """
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

        logger.info("database.connection.succeeded")
        return True

    except Exception:
        logger.exception("database.connection.failed")
        return False
