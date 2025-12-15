"""
Database session management for async SQLAlchemy operations.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlmodel import SQLModel

from ..config.settings import settings

logger = logging.getLogger(__name__)


# Create async engine with connection pooling optimizations
# FIX: Increased pool size to handle concurrent scraping (was 5/10, caused exhaustion)
engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_size=20,         # Increased from 5 to handle concurrent operations
    max_overflow=30,      # Increased from 10 (total max: 50 connections)
    pool_pre_ping=True,   # Detect stale connections before use
    pool_recycle=3600,    # Recycle connections every hour
    pool_timeout=30,      # Wait max 30s for connection from pool
    connect_args={
        "command_timeout": 30,  # Timeout for individual queries (asyncpg)
        "server_settings": {
            "statement_timeout": "30000",  # PostgreSQL statement timeout (ms)
        },
    },
)

# Session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """Initialize database tables."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def close_db():
    """Close database connections."""
    await engine.dispose()


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session.

    FIX: Removed asyncio.wait_for() wrapper on commit() to prevent race condition.
    asyncio.wait_for() can cancel a commit mid-flight, causing partial writes.
    PostgreSQL statement_timeout (30s) is already configured in engine settings
    to handle stuck transactions safely at the database level.
    """
    async with async_session_factory() as session:
        try:
            yield session
            # Let PostgreSQL handle timeout via statement_timeout setting
            await session.commit()
        except Exception as e:
            logger.error(f"Database session error, rolling back: {e}")
            await session.rollback()
            raise


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database sessions.

    Use get_session() for non-FastAPI code (scheduler, orchestrator, etc).
    Use get_db() only as a FastAPI Depends() injection.

    FIX: Removed asyncio.wait_for() wrapper on commit() (see get_session).
    """
    async with async_session_factory() as session:
        try:
            yield session
            # Let PostgreSQL handle timeout via statement_timeout setting
            await session.commit()
        except Exception as e:
            logger.error(f"Database session error, rolling back: {e}")
            await session.rollback()
            raise
