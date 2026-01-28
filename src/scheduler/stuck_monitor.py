"""
StuckScanMonitor - Background task to detect and clean up silently crashed scans.

FIX 2026-01: Catches failure modes where everything fails:
- OOM kills (SIGKILL) - process dies instantly, no handlers run
- Process crash/segfault
- DB pool exhaustion preventing ScanJobGuard from updating
- Railway container restarts

The monitor runs every 5 minutes and marks jobs as failed if:
1. status = 'running'
2. last_heartbeat is older than STUCK_THRESHOLD (2 minutes)

This provides a guaranteed cleanup within 7 minutes of a crash:
- Up to 5 minutes for monitor cycle
- Up to 2 minutes for heartbeat to become stale
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from ..config.settings import settings

logger = logging.getLogger(__name__)

# How often to check for stuck scans (seconds)
# FIX 2026-01: Reduced from 300s (5 min) to 60s (1 min) for faster detection
MONITOR_INTERVAL = 60  # 1 minute

# How long without heartbeat before a job is considered stuck (seconds)
# FIX 2026-01: Reduced from 120s (2 min) to 90s for faster detection
STUCK_THRESHOLD = 90  # 90 seconds

# Background task reference
_monitor_task: Optional[asyncio.Task] = None
_monitor_engine = None
_monitor_session_factory = None


def _create_monitor_engine():
    """Create a dedicated DB engine for the monitor.

    Isolated from main pool to ensure monitoring continues even during
    pool exhaustion events.
    """
    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
        connect_args={
            "command_timeout": 30,
            "server_settings": {
                "statement_timeout": "30000",
            },
        },
    )


async def _monitor_loop():
    """Main monitor loop - runs every MONITOR_INTERVAL seconds."""
    global _monitor_engine, _monitor_session_factory

    logger.info(
        f"StuckScanMonitor started (interval={MONITOR_INTERVAL}s, threshold={STUCK_THRESHOLD}s)"
    )

    # Initialize engine on first run
    if _monitor_engine is None:
        _monitor_engine = _create_monitor_engine()
        _monitor_session_factory = async_sessionmaker(
            _monitor_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    while True:
        try:
            await asyncio.sleep(MONITOR_INTERVAL)
            await _check_for_stuck_scans()
        except asyncio.CancelledError:
            logger.info("StuckScanMonitor received cancellation")
            break
        except Exception as e:
            # Log but continue - monitor should be resilient
            logger.error(f"StuckScanMonitor error: {e}", exc_info=True)
            # Wait a bit before retrying to avoid tight error loops
            await asyncio.sleep(60)


async def _check_for_stuck_scans():
    """Check for and mark stuck scan jobs.

    A job is considered stuck if:
    - status = 'running'
    - last_heartbeat < NOW() - STUCK_THRESHOLD

    Note: Also handles jobs where last_heartbeat is NULL (legacy jobs
    created before heartbeat feature).
    """
    try:
        async with _monitor_session_factory() as session:
            # Find stuck jobs
            # Include NULL heartbeat case for jobs that never got a heartbeat
            # Note: Use f-string for interval since SQLAlchemy doesn't support
            # parameterized intervals in PostgreSQL
            result = await session.execute(
                text(f"""
                    SELECT id, job_id, started_at, last_heartbeat
                    FROM scan_jobs
                    WHERE status = 'running'
                    AND (
                        last_heartbeat < NOW() - INTERVAL '{STUCK_THRESHOLD} seconds'
                        OR (last_heartbeat IS NULL AND started_at < NOW() - INTERVAL '{STUCK_THRESHOLD} seconds')
                    )
                """)
            )
            stuck_jobs = result.fetchall()

            if not stuck_jobs:
                logger.debug("StuckScanMonitor: No stuck jobs found")
                return

            # Mark each stuck job as failed
            for job in stuck_jobs:
                job_id = job[0]
                job_name = job[1]
                started_at = job[2]
                last_heartbeat = job[3]

                # Calculate how long ago the heartbeat stopped
                if last_heartbeat:
                    stale_seconds = (datetime.now(timezone.utc) - last_heartbeat.replace(tzinfo=timezone.utc)).total_seconds()
                    detail = f"heartbeat stale for {stale_seconds:.0f}s"
                else:
                    stale_seconds = (datetime.now(timezone.utc) - started_at.replace(tzinfo=timezone.utc)).total_seconds()
                    detail = f"no heartbeat, started {stale_seconds:.0f}s ago"

                error_message = f"Process died unexpectedly ({detail})"

                await session.execute(
                    text("""
                        UPDATE scan_jobs
                        SET status = 'failed',
                            error_message = :error_message,
                            completed_at = NOW()
                        WHERE id = :job_id
                        AND status = 'running'
                    """),
                    {"job_id": job_id, "error_message": error_message}
                )
                await session.commit()

                logger.warning(
                    f"STUCK_SCAN_DETECTED: Marked scan_job_id={job_id} (job_id={job_name}) "
                    f"as failed. {detail}"
                )

    except Exception as e:
        logger.error(f"Failed to check for stuck scans: {e}", exc_info=True)


async def start_stuck_monitor():
    """Start the stuck scan monitor background task.

    Called from application lifespan startup.
    """
    global _monitor_task

    if _monitor_task is not None and not _monitor_task.done():
        logger.warning("StuckScanMonitor already running")
        return

    _monitor_task = asyncio.create_task(
        _monitor_loop(),
        name="stuck_scan_monitor"
    )
    logger.info("StuckScanMonitor task created")


async def stop_stuck_monitor():
    """Stop the stuck scan monitor and cleanup.

    Called from application lifespan shutdown.
    """
    global _monitor_task, _monitor_engine

    if _monitor_task is not None:
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
        _monitor_task = None
        logger.info("StuckScanMonitor task stopped")

    if _monitor_engine is not None:
        await _monitor_engine.dispose()
        _monitor_engine = None
        logger.info("StuckScanMonitor engine disposed")


async def check_stuck_scans_now():
    """Manually trigger a stuck scan check (for testing/debugging).

    Returns the number of stuck scans that were marked as failed.
    """
    global _monitor_engine, _monitor_session_factory

    if _monitor_engine is None:
        _monitor_engine = _create_monitor_engine()
        _monitor_session_factory = async_sessionmaker(
            _monitor_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    # Get count before
    async with _monitor_session_factory() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM scan_jobs WHERE status = 'running'")
        )
        running_before = result.scalar()

    # Run check
    await _check_for_stuck_scans()

    # Get count after
    async with _monitor_session_factory() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM scan_jobs WHERE status = 'running'")
        )
        running_after = result.scalar()

    return running_before - running_after
