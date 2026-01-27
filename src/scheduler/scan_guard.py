"""
ScanJobGuard - Safety harness for scan job execution.

FIX 2026-01: Prevents silent death of scan jobs by providing:
1. Isolated DB connection (separate from main pool)
2. Background heartbeat updates every 30 seconds
3. Retry logic with exponential backoff for DB operations
4. Signal handlers for graceful shutdown on SIGTERM/SIGINT

Usage:
    async with ScanJobGuard(scan_job_id, job_id) as guard:
        # Execute phases 0-4
        guard.set_success()  # Call on successful completion

On any unhandled exception, the guard automatically marks the job as failed.
"""

import asyncio
import atexit
import logging
import random
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from ..config.settings import settings

logger = logging.getLogger(__name__)

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 30

# Max retries for DB operations
MAX_DB_RETRIES = 5

# Base delay for exponential backoff (seconds)
BASE_RETRY_DELAY = 0.5


def _create_isolated_engine():
    """Create a separate DB engine with minimal pool for guard operations.

    This engine is isolated from the main pool to ensure status updates
    can succeed even when the main pool is exhausted.
    """
    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_size=1,          # Single connection for guard
        max_overflow=1,       # Allow one overflow if needed
        pool_pre_ping=True,   # Detect stale connections
        pool_recycle=600,     # Recycle every 10 min
        pool_timeout=10,      # Short timeout - fail fast
        connect_args={
            "command_timeout": 10,  # Short command timeout
            "server_settings": {
                "statement_timeout": "10000",  # 10s statement timeout
            },
        },
    )


class ScanJobGuard:
    """Context manager that guards scan job execution with heartbeat and failure handling."""

    def __init__(self, scan_job_id: int, job_id: str):
        """
        Initialize the guard.

        Args:
            scan_job_id: Database ID of the ScanJob record
            job_id: Human-readable job ID (e.g., "20260127_143000")
        """
        self.scan_job_id = scan_job_id
        self.job_id = job_id
        self._engine = None
        self._session_factory = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._success = False
        self._error_message: Optional[str] = None
        self._shutdown_requested = False
        self._original_sigterm = None
        self._original_sigint = None

    async def __aenter__(self):
        """Enter the guard context - start heartbeat and register handlers."""
        # Create isolated engine
        self._engine = _create_isolated_engine()
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Set initial heartbeat
        await self._update_heartbeat()

        # Start background heartbeat task
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"heartbeat_{self.job_id}"
        )

        # Register signal handlers
        self._register_signal_handlers()

        logger.info(f"[{self.job_id}] ScanJobGuard active (scan_job_id={self.scan_job_id})")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the guard context - update final status and cleanup."""
        # Stop heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Restore signal handlers
        self._restore_signal_handlers()

        # Determine final status
        if exc_type is not None:
            # Exception occurred - mark as failed
            error_msg = str(exc_val) if exc_val else f"{exc_type.__name__}"
            await self._update_status("failed", error_msg[:500])
            logger.error(f"[{self.job_id}] Scan failed with exception: {error_msg}")
        elif self._error_message:
            # Manual error set via set_failed()
            await self._update_status("failed", self._error_message)
            logger.info(f"[{self.job_id}] Scan marked as failed: {self._error_message}")
        elif self._success:
            # Success set via set_success()
            # Note: Don't update status here - jobs.py handles success update with full stats
            logger.info(f"[{self.job_id}] ScanJobGuard exiting (success path)")
        else:
            # No explicit success/failure - treat as incomplete
            await self._update_status("failed", "Job exited without completing")
            logger.warning(f"[{self.job_id}] Scan exited without explicit success/failure")

        # Cleanup engine
        if self._engine:
            await self._engine.dispose()
            self._engine = None

        # Don't suppress exceptions
        return False

    def set_success(self):
        """Mark the job as successful (call before exiting context)."""
        self._success = True

    def set_failed(self, error_message: str):
        """Mark the job as failed with a specific error message."""
        self._error_message = error_message[:500]

    async def _heartbeat_loop(self):
        """Background task that updates heartbeat every HEARTBEAT_INTERVAL seconds."""
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self._shutdown_requested:
                    break
                await self._update_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log but continue - heartbeat failure shouldn't kill the job
                logger.warning(f"[{self.job_id}] Heartbeat update failed: {e}")

    async def _update_heartbeat(self):
        """Update the last_heartbeat timestamp with retry logic."""
        await self._execute_with_retry(
            "UPDATE scan_jobs SET last_heartbeat = NOW() WHERE id = :scan_job_id",
            {"scan_job_id": self.scan_job_id},
            operation_name="heartbeat"
        )
        logger.debug(f"[{self.job_id}] Heartbeat updated for scan_job_id={self.scan_job_id}")

    async def _update_status(self, status: str, error_message: Optional[str] = None):
        """Update the job status with retry logic."""
        params = {
            "scan_job_id": self.scan_job_id,
            "status": status,
            "completed_at": datetime.now(timezone.utc),
        }

        if error_message:
            query = """
                UPDATE scan_jobs
                SET status = :status,
                    completed_at = :completed_at,
                    error_message = :error_message,
                    last_heartbeat = NOW()
                WHERE id = :scan_job_id
            """
            params["error_message"] = error_message
        else:
            query = """
                UPDATE scan_jobs
                SET status = :status,
                    completed_at = :completed_at,
                    last_heartbeat = NOW()
                WHERE id = :scan_job_id
            """

        success = await self._execute_with_retry(
            query, params, operation_name="status_update"
        )

        if not success:
            # All retries failed - log critical error
            logger.critical(
                f"SCAN_JOB_GUARD_FAILURE: Could not update scan_job_id={self.scan_job_id} "
                f"to status={status}. Job may appear stuck."
            )

    async def _execute_with_retry(
        self,
        query: str,
        params: dict,
        operation_name: str
    ) -> bool:
        """Execute a DB query with exponential backoff retry.

        Returns True if successful, False if all retries exhausted.
        """
        last_error = None

        for attempt in range(MAX_DB_RETRIES):
            try:
                async with self._session_factory() as session:
                    await session.execute(text(query), params)
                    await session.commit()
                    return True
            except Exception as e:
                last_error = e
                if attempt < MAX_DB_RETRIES - 1:
                    # Calculate delay with jitter
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    delay *= random.uniform(0.9, 1.1)
                    logger.warning(
                        f"[{self.job_id}] {operation_name} failed (attempt {attempt + 1}/{MAX_DB_RETRIES}): {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)

        logger.error(
            f"[{self.job_id}] {operation_name} failed after {MAX_DB_RETRIES} attempts: {last_error}"
        )
        return False

    def _register_signal_handlers(self):
        """Register handlers for SIGTERM and SIGINT for graceful shutdown."""
        try:
            loop = asyncio.get_running_loop()

            # Store original handlers
            self._original_sigterm = signal.getsignal(signal.SIGTERM)
            self._original_sigint = signal.getsignal(signal.SIGINT)

            # Register new handlers
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._handle_signal(s))
                )

            logger.debug(f"[{self.job_id}] Signal handlers registered")
        except Exception as e:
            # Signal handling may not work in all environments (e.g., Windows, some containers)
            logger.warning(f"[{self.job_id}] Could not register signal handlers: {e}")

    def _restore_signal_handlers(self):
        """Restore original signal handlers."""
        try:
            loop = asyncio.get_running_loop()

            for sig, original in [
                (signal.SIGTERM, self._original_sigterm),
                (signal.SIGINT, self._original_sigint),
            ]:
                if original is not None:
                    loop.remove_signal_handler(sig)
                    if original != signal.SIG_DFL:
                        signal.signal(sig, original)
        except Exception as e:
            logger.warning(f"[{self.job_id}] Could not restore signal handlers: {e}")

    async def _handle_signal(self, sig: signal.Signals):
        """Handle shutdown signal - mark job as failed and request shutdown."""
        sig_name = signal.Signals(sig).name
        logger.warning(f"[{self.job_id}] Received {sig_name}, marking job as failed")

        self._shutdown_requested = True
        self._error_message = f"Process received {sig_name} signal"

        # Update status immediately (don't wait for context exit)
        await self._update_status("failed", self._error_message)


@asynccontextmanager
async def guarded_scan(scan_job_id: Optional[int], job_id: str):
    """
    Context manager for guarded scan execution.

    If scan_job_id is None (DB record creation failed), runs unguarded.

    Usage:
        async with guarded_scan(scan_job_db_id, job_id) as guard:
            # Phases 0-4
            if guard:
                guard.set_success()
    """
    if scan_job_id is None:
        # No DB record - run unguarded
        logger.warning(f"[{job_id}] Running unguarded (no scan_job_id)")
        yield None
    else:
        async with ScanJobGuard(scan_job_id, job_id) as guard:
            yield guard
