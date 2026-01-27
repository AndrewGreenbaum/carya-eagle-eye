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
# FIX 2026-01: Increased pool to 100 total (was 50, caused exhaustion at 2x scale)
# Railway PostgreSQL allows up to 100 connections by default
engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_size=30,         # Base pool size (persistent connections)
    max_overflow=70,      # Overflow connections (total max: 100)
    pool_pre_ping=True,   # Detect stale connections before use
    pool_recycle=1800,    # Recycle connections every 30 min (was 1hr, helps with stale)
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


def get_pool_status() -> dict:
    """Get connection pool status for monitoring.

    Returns dict with:
    - pool_size: Base number of persistent connections
    - max_overflow: Additional connections allowed beyond pool_size
    - checked_in: Connections currently available in pool
    - checked_out: Connections currently in use
    - overflow: Current overflow connections in use
    - total_connections: checked_out + checked_in

    Use in /health endpoint to monitor pool exhaustion before it causes failures.
    """
    pool = engine.pool
    return {
        "pool_size": pool.size(),
        "max_overflow": pool.overflow(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "total_connections": pool.checkedin() + pool.checkedout(),
    }


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
