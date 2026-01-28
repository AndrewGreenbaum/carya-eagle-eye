"""
APScheduler job definitions for automated scraping.

Runs scraping every 4 hours of all 18 VC funds with:
- Staggered execution to avoid rate limits
- Error isolation per fund
- Webhook notifications on completion
- Metrics logging
- Automatic enrichment of new deals
- External sources (Brave Search, SEC EDGAR, etc.)
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Set
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from ..harvester import scrape_all_funds, get_implemented_scrapers
from ..harvester.base_scraper import NormalizedArticle
from ..config.settings import settings
from ..config.funds import FUND_REGISTRY
from .notifications import send_scrape_summary

logger = logging.getLogger(__name__)


# =============================================================================
# JOB TRACKER CLASS - Encapsulates per-job state
# FIX 2026-01: Replaces module-level globals to prevent state corruption
# between concurrent jobs and improve testability.
# =============================================================================

class JobTracker:
    """
    Encapsulates state for a single scraping job run.

    FIX 2026-01: Replaces module-level globals (_global_seen_urls, _global_content_hashes)
    which could cause state corruption between concurrent runs.

    Features:
    - URL deduplication (tracks seen URLs within a single run)
    - Content hash deduplication (catches syndicated articles with different URLs)
    - Thread-safe with async lock
    - Event-based blocking during cache clearing to prevent race conditions

    Usage:
        tracker = JobTracker()
        await tracker.check_url_seen(url)  # Returns True if seen before
        await tracker.check_content_seen(text)  # Returns True if seen before
        await tracker.clear()  # Clear all state for new job
    """

    def __init__(self):
        self._seen_urls: Set[str] = set()
        self._content_hashes: Set[str] = set()
        self._lock = asyncio.Lock()
        self._clearing_event = asyncio.Event()
        self._clearing_event.set()  # Initially not clearing (set = allow processing)

    async def check_url_seen(self, url: str) -> bool:
        """
        Check if URL has been seen in this run (cross-source dedup).

        Returns True if URL was already seen (should skip).
        Adds URL to set if not seen.
        """
        await self._clearing_event.wait()

        normalized = normalize_url(url)

        async with self._lock:
            if normalized in self._seen_urls:
                return True
            self._seen_urls.add(normalized)
            return False

    async def add_content_hash(self, fingerprint: str) -> None:
        """Add a content hash to the in-memory set."""
        # FIX: Wait for clearing to complete before adding (prevents race condition
        # where hash is added during/after clear() completes)
        await self._clearing_event.wait()
        async with self._lock:
            self._content_hashes.add(fingerprint)

    async def is_content_hash_seen(self, fingerprint: str) -> bool:
        """Check if content hash exists in in-memory set."""
        # FIX: Wait for clearing to complete before checking (prevents race condition
        # where check happens during clear() and returns stale result)
        await self._clearing_event.wait()
        async with self._lock:
            return fingerprint in self._content_hashes

    async def clear(self) -> None:
        """Clear all state. Called at start of each scheduled run."""
        self._clearing_event.clear()
        try:
            async with self._lock:
                url_count = len(self._seen_urls)
                hash_count = len(self._content_hashes)
                self._seen_urls.clear()
                self._content_hashes.clear()
            if url_count > 0 or hash_count > 0:
                logger.info(f"JobTracker cleared: {url_count} URLs, {hash_count} content hashes")
        finally:
            self._clearing_event.set()

    async def get_stats(self) -> Dict[str, int]:
        """Get current tracking stats.

        FIX: Made async and added lock acquisition to prevent data race
        when reading from sets during concurrent access.
        """
        async with self._lock:
            return {
                "urls_tracked": len(self._seen_urls),
                "content_hashes_tracked": len(self._content_hashes),
            }


# Global tracker instance - replaced at start of each job
# Using a single instance allows backward compatibility with existing function calls
_job_tracker = JobTracker()

# FIX 2026-01: Per-source timeout to prevent job hangs on slow HTTP responses
# Without this, a single slow source can hang the entire job indefinitely
SOURCE_SCRAPE_TIMEOUT = 60.0  # 60 seconds per source (covers HTTP + processing)


async def with_timeout(coro, timeout: float, source_name: str):
    """Wrap a coroutine with a timeout and return a standardized result.

    Args:
        coro: The coroutine to execute
        timeout: Timeout in seconds
        source_name: Name of the source for error reporting

    Returns:
        The coroutine result on success, or (source_name, {"error": "timeout"}) on timeout
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"SCRAPER_TIMEOUT: {source_name} timed out after {timeout}s")
        return (source_name, {"error": f"timeout after {timeout}s", "articles_found": 0})


# =============================================================================
# RESILIENT PIPELINE HELPERS (FIX 2026-01)
# =============================================================================
# These functions implement memory-safe batch processing and strict timeouts
# to prevent OOM crashes and indefinite hangs on Railway.
# =============================================================================


async def fetch_with_timeout(coro, name: str = "request") -> Optional[Any]:
    """Wrap a single request with strict per-request timeout.

    Unlike with_timeout() which is for entire sources, this is for individual
    requests (article fetches, filing fetches) within a source.

    Args:
        coro: The coroutine to execute (e.g., fetch_article, fetch_filing)
        name: Descriptive name for logging (e.g., "brave_article:techcrunch.com/...")

    Returns:
        The coroutine result on success, or None on timeout/error.
        Returning None allows callers to skip failed requests gracefully.
    """
    timeout = settings.per_request_timeout
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"TIMEOUT_SKIP: {name} exceeded {timeout}s - skipping")
        return None
    except Exception as e:
        logger.warning(f"REQUEST_SKIP: {name} failed: {e} - skipping")
        return None


class SourceCircuitBreaker:
    """Circuit breaker to disable sources that are consistently failing.

    FIX 2026-01: Prevents wasting time on broken sources during a scan.
    After threshold consecutive failures, the source is disabled for the
    remainder of the scan. Resets at the start of each new scan.

    Usage:
        circuit_breaker = SourceCircuitBreaker()

        for article in articles:
            if circuit_breaker.is_disabled("brave_search"):
                continue
            try:
                result = await fetch_article(article)
                circuit_breaker.record_success("brave_search")
            except Exception:
                circuit_breaker.record_error("brave_search")
    """

    def __init__(self, threshold: Optional[int] = None):
        """Initialize circuit breaker.

        Args:
            threshold: Number of consecutive errors before disabling source.
                      Defaults to settings.circuit_breaker_threshold.
        """
        self._threshold = threshold or settings.circuit_breaker_threshold
        self._error_counts: Dict[str, int] = {}
        self._disabled: Set[str] = set()

    def record_error(self, source: str) -> None:
        """Record an error for a source. May trigger circuit open."""
        self._error_counts[source] = self._error_counts.get(source, 0) + 1
        if self._error_counts[source] >= self._threshold:
            if source not in self._disabled:
                self._disabled.add(source)
                logger.warning(
                    f"CIRCUIT_OPEN: {source} disabled after "
                    f"{self._threshold} consecutive errors"
                )

    def record_success(self, source: str) -> None:
        """Record a success for a source. Resets error count."""
        self._error_counts[source] = 0

    def is_disabled(self, source: str) -> bool:
        """Check if a source is disabled."""
        return source in self._disabled

    def reset(self) -> None:
        """Reset all circuit breaker state. Called at start of each scan."""
        if self._disabled:
            logger.info(
                f"CIRCUIT_RESET: Re-enabling {len(self._disabled)} sources: "
                f"{', '.join(self._disabled)}"
            )
        self._error_counts.clear()
        self._disabled.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get current circuit breaker stats for monitoring."""
        return {
            "disabled_sources": list(self._disabled),
            "error_counts": dict(self._error_counts),
            "threshold": self._threshold,
        }


# Global circuit breaker instance - reset at start of each job
_circuit_breaker = SourceCircuitBreaker()


async def filter_articles_streaming(
    articles: List[NormalizedArticle],
    source_name: str,
) -> tuple[List[NormalizedArticle], Dict[str, int]]:
    """Filter articles through all pre-extraction filters without intermediate lists.

    FIX 2026-01: Reduces memory by not creating 5 separate list copies during filtering.
    Previous approach: 5 filtering stages each created a new list.
    New approach: Single pass through articles, yielding only those that pass all filters.

    Args:
        articles: List of articles to filter
        source_name: Name of source for logging and source-specific rules

    Returns:
        Tuple of (filtered_articles, stats_dict) where stats_dict contains:
        - total: Number of input articles
        - skipped_url: URL dedup rejects
        - skipped_source: Source-specific filter rejects
        - skipped_title: Title filter rejects
        - skipped_content: Content hash dedup rejects
        - passed: Number that passed all filters
    """
    stats = {
        "total": len(articles),
        "skipped_url": 0,
        "skipped_source": 0,
        "skipped_title": 0,
        "skipped_content": 0,
        "passed": 0,
    }

    if not articles:
        return [], stats

    # Pass 1: Filter by URL dedup, source rules, and title
    # These are fast in-memory checks
    url_and_title_passed = []
    for article in articles:
        # URL dedup check
        if await check_global_url_seen(article.url):
            stats["skipped_url"] += 1
            continue

        # Source-specific filter
        should_skip, reason = should_skip_by_source(source_name, article.url, article.title)
        if should_skip:
            stats["skipped_source"] += 1
            logger.debug(f"[{source_name}] Skipping by source rule: {reason}")
            continue

        # Title filter (skip non-announcements)
        if is_non_announcement_title(article.title):
            stats["skipped_title"] += 1
            continue

        # Title filter (require funding signals)
        if not is_likely_funding_from_title(article.title):
            stats["skipped_title"] += 1
            continue

        url_and_title_passed.append(article)

    # Pass 2: Content hash dedup (requires DB query, so batch it)
    if url_and_title_passed:
        filtered_articles, content_skipped = await batch_check_content_seen(url_and_title_passed)
        stats["skipped_content"] = content_skipped
        stats["passed"] = len(filtered_articles)
    else:
        filtered_articles = []

    if stats["total"] > 0:
        logger.info(
            f"[{source_name}] Filter stats: total={stats['total']}, "
            f"url_dedup={stats['skipped_url']}, source={stats['skipped_source']}, "
            f"title={stats['skipped_title']}, content={stats['skipped_content']}, "
            f"passed={stats['passed']}"
        )

    return filtered_articles, stats


async def process_articles_batched(
    articles: List[NormalizedArticle],
    source_name: str,
    scan_job_id: Optional[int],
    process_fn,
) -> Dict[str, int]:
    """Process articles in memory-safe batches.

    FIX 2026-01: Processes articles in chunks of BATCH_SIZE (default 50) to prevent
    memory accumulation. After each batch, results are aggregated and batch data is
    explicitly released.

    Args:
        articles: List of articles to process
        source_name: Name of source for logging
        scan_job_id: Optional scan job ID for linking deals
        process_fn: Async function to process a single article.
                   Should return extraction result or raise exception.

    Returns:
        Dict with processing stats (deals_saved, errors, etc.)
    """
    batch_size = settings.batch_size
    max_concurrent = settings.max_concurrent_extractions

    stats = {
        "articles_processed": 0,
        "deals_saved": 0,
        "errors": 0,
        "batches_completed": 0,
    }

    if not articles:
        return stats

    total_batches = (len(articles) + batch_size - 1) // batch_size

    for batch_num, i in enumerate(range(0, len(articles), batch_size), 1):
        batch = articles[i:i + batch_size]

        # Process batch with limited concurrency
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_one(article):
            async with semaphore:
                return await process_fn(article)

        # Execute batch
        batch_results = await asyncio.gather(
            *[process_one(a) for a in batch],
            return_exceptions=True
        )

        # Aggregate results
        batch_deals = 0
        batch_errors = 0
        for result in batch_results:
            if isinstance(result, Exception):
                batch_errors += 1
                logger.debug(f"[{source_name}] Batch {batch_num} exception: {result}")
            elif result:  # Truthy result means deal saved
                batch_deals += 1

        stats["articles_processed"] += len(batch)
        stats["deals_saved"] += batch_deals
        stats["errors"] += batch_errors
        stats["batches_completed"] += 1

        logger.info(
            f"[{source_name}] Batch {batch_num}/{total_batches}: "
            f"processed={len(batch)}, deals={batch_deals}, errors={batch_errors}"
        )

        # Explicitly release batch memory
        del batch
        del batch_results

    return stats

# Tracking parameters to strip from URLs for normalization
TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'gclsrc', 'dclid', 'msclkid',
    'ref', 'source', 'mc_cid', 'mc_eid',
    '_ga', '_gl', '_hsenc', '_hsmi',
    'ncid', 'sr_share', 'vero_id',
}


def normalize_url(url: str) -> str:
    """
    Normalize URL for deduplication by stripping tracking parameters.

    Examples:
        "https://example.com/article?utm_source=twitter&id=123"
        → "https://example.com/article?id=123"

    This catches more duplicates from different sources that link to
    the same article with different tracking params.
    """
    if not url:
        return url

    try:
        parsed = urlparse(url)

        # Parse query params and filter out tracking ones
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        filtered_params = {
            k: v for k, v in query_params.items()
            if k.lower() not in TRACKING_PARAMS
        }

        # Rebuild query string (sorted for consistency)
        new_query = urlencode(filtered_params, doseq=True) if filtered_params else ""

        # Rebuild URL without tracking params
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc.lower(),  # Lowercase domain
            parsed.path.rstrip('/'),  # Remove trailing slash
            parsed.params,
            new_query,
            '',  # Remove fragment
        ))

        return normalized
    except Exception as e:
        # If parsing fails, log and return original URL
        # FIX: Add debug logging instead of silently swallowing exception
        logger.debug(f"URL normalization failed for {url}: {e}")
        return url


async def check_global_url_seen(url: str) -> bool:
    """
    Check if URL has been seen in this run (cross-source dedup).

    Returns True if URL was already seen (should skip).
    Adds URL to set if not seen.

    FIX 2026-01: Now uses JobTracker class instead of module-level globals.
    """
    return await _job_tracker.check_url_seen(url)


async def clear_global_seen_urls() -> None:
    """Clear URL set. Called at start of each scheduled run.

    FIX 2026-01: Now handled by JobTracker.clear() - this function
    is kept for backward compatibility but just logs.
    """
    # Actual clearing is done in clear_job_tracker()
    pass  # JobTracker.clear() handles this now


async def clear_job_tracker() -> None:
    """Clear all job tracker state. Called at start of each scheduled run.

    FIX 2026-01: Single function to clear all job state instead of
    separate clear_global_seen_urls() and clear_global_content_hashes().
    Also cleans up expired entries from persistent cache and resets circuit breaker.
    """
    global _job_tracker, _circuit_breaker

    # Clear the in-memory tracker
    await _job_tracker.clear()

    # Reset circuit breaker for fresh run (re-enables any disabled sources)
    _circuit_breaker.reset()

    # Clean up expired entries from persistent cache
    try:
        from ..archivist.database import get_session
        from ..archivist.models import ContentHash
        from sqlmodel import delete

        async with get_session() as session:
            now = datetime.now(timezone.utc)
            stmt = delete(ContentHash).where(ContentHash.expires_at < now)
            result = await session.execute(stmt)
            expired_count = result.rowcount
            await session.commit()
            if expired_count > 0:
                logger.info(f"Cleaned up {expired_count} expired content hashes from persistent cache")
    except Exception as e:
        logger.warning(f"Failed to clean up expired content hashes: {e}")


def get_content_fingerprint(text: str) -> str:
    """
    Generate a fingerprint for article content.

    Uses first 500 + last 200 chars for fast fingerprinting.
    Catches syndicated articles that appear on different sites with different URLs.

    Added Jan 2026 to save ~40 Claude requests/scan.
    """
    from hashlib import md5
    if not text:
        return ""
    # Normalize whitespace for consistent hashing
    normalized = " ".join(text.split())
    if len(normalized) < 700:
        return md5(normalized.encode()).hexdigest()
    # Use start + end to catch both headline matches and body matches
    fingerprint_text = normalized[:500] + normalized[-200:]
    return md5(fingerprint_text.encode()).hexdigest()


async def check_global_content_seen(text: str, source_url: str = None) -> bool:
    """
    Check if content has been seen (syndicated article dedup).

    FIX 2026-01: Now uses JobTracker class instead of module-level globals.

    Check order:
    1. In-memory cache (fast path for same-run duplicates)
    2. Database cache (catches cross-run duplicates, 30-day TTL)

    Returns True if content was already seen (should skip).
    """
    fingerprint = get_content_fingerprint(text)
    if not fingerprint:
        return False  # Empty content, don't skip

    content_length = len(text) if text else 0

    # Fast path: check in-memory cache first
    if await _job_tracker.is_content_hash_seen(fingerprint):
        return True

    # Slow path: check persistent database cache
    try:
        from ..archivist.database import get_session
        from ..archivist.models import ContentHash
        from sqlmodel import select
        from datetime import timedelta

        async with get_session() as session:
            # Check if hash exists and not expired
            now = datetime.now(timezone.utc)
            stmt = select(ContentHash).where(
                ContentHash.content_hash == fingerprint,
                ContentHash.expires_at > now
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                # Found in persistent cache - check if new content is longer
                # FIX: Add minimum threshold to avoid 0 * 2 = 0 edge case
                if content_length > max(100, existing.content_length * 2):
                    # New content is 2x longer (and > 100 bytes), update and process
                    existing.content_length = content_length
                    existing.source_url = source_url
                    existing.expires_at = now + timedelta(days=30)
                    await session.commit()
                    logger.debug(f"Updating content hash with longer content: {fingerprint[:16]}...")
                    # Add to in-memory cache too
                    await _job_tracker.add_content_hash(fingerprint)
                    return False  # Process the longer version
                else:
                    # Already seen with similar/longer content
                    logger.debug(f"Content found in persistent cache: {fingerprint[:16]}...")
                    return True

            # Not in database - add it
            new_hash = ContentHash(
                content_hash=fingerprint,
                content_length=content_length,
                source_url=source_url,
                expires_at=now + timedelta(days=30)
            )
            session.add(new_hash)
            await session.commit()

    except Exception as e:
        # FIX 2026-01: Return False (process article) on DB error to prevent data loss
        # Previous "conservative" approach returned True (skip), which loses articles PERMANENTLY
        # Better to risk occasional duplicate processing (~$0.02) than lose articles entirely
        # Duplicates are caught by dedup logic anyway; lost articles are unrecoverable
        logger.warning(f"Persistent content cache check failed, processing anyway: {e}")
        return False

    # Add to in-memory cache
    await _job_tracker.add_content_hash(fingerprint)

    return False


async def clear_global_content_hashes() -> None:
    """Clear content hash cache. Backward compatibility wrapper.

    FIX 2026-01: Now handled by clear_job_tracker() - this function
    is kept for backward compatibility.
    """
    # Actual clearing is done in clear_job_tracker()
    pass


async def batch_check_content_seen(articles: list) -> tuple[list, int]:
    """
    Batch check if articles have been seen (syndicated article dedup).

    FIX 2026-01: Replaces per-article check_global_content_seen() calls with
    a single batched database query. Reduces N+1 query pattern from N DB calls
    to 1 DB call per batch.

    Args:
        articles: List of articles with .text and .url attributes

    Returns:
        Tuple of (new_articles, skipped_count) where:
        - new_articles: Articles that should be processed (not duplicates)
        - skipped_count: Number of articles skipped as duplicates

    Performance improvement:
    - Before: 50 articles = 50 DB queries (slow, N+1 pattern)
    - After: 50 articles = 1 DB query (fast, batched)
    """
    if not articles:
        return [], 0

    # Step 1: Calculate fingerprints for all articles
    article_fingerprints = []  # [(article, fingerprint, content_length)]
    for article in articles:
        fingerprint = get_content_fingerprint(article.text)
        if fingerprint:
            content_length = len(article.text) if article.text else 0
            article_fingerprints.append((article, fingerprint, content_length))
        else:
            # Empty content - include article but no fingerprint
            article_fingerprints.append((article, None, 0))

    # Step 2: Fast path - filter out in-memory cache hits
    # FIX: Track in-memory skips separately from DB skips for correct metrics on exception
    non_cached = []
    in_memory_skipped = 0
    for article, fingerprint, content_length in article_fingerprints:
        if fingerprint is None:
            non_cached.append((article, fingerprint, content_length))
        elif await _job_tracker.is_content_hash_seen(fingerprint):
            in_memory_skipped += 1
        else:
            non_cached.append((article, fingerprint, content_length))

    if not non_cached:
        return [], in_memory_skipped

    # Step 3: Batch query to database for remaining fingerprints
    fingerprints_to_check = [fp for _, fp, _ in non_cached if fp is not None]

    if not fingerprints_to_check:
        # No fingerprints to check - return all articles
        return [article for article, _, _ in non_cached], in_memory_skipped

    try:
        from ..archivist.database import get_session
        from ..archivist.models import ContentHash
        from sqlmodel import select
        from datetime import timedelta

        async with get_session() as session:
            now = datetime.now(timezone.utc)

            # Single batch query for all fingerprints
            stmt = select(ContentHash).where(
                ContentHash.content_hash.in_(fingerprints_to_check),
                ContentHash.expires_at > now
            )
            result = await session.execute(stmt)
            existing_hashes = {row.content_hash: row for row in result.scalars().all()}

            # Process each article
            # FIX: Track DB skips separately from in-memory skips for correct metrics on exception
            new_articles = []
            new_hashes_to_add = []
            db_skipped = 0

            for article, fingerprint, content_length in non_cached:
                if fingerprint is None:
                    # No fingerprint - include article
                    new_articles.append(article)
                    continue

                existing = existing_hashes.get(fingerprint)

                if existing:
                    # Found in persistent cache - check if new content is significantly longer
                    # FIX: Add minimum threshold to avoid 0 * 2 = 0 edge case
                    if content_length > max(100, existing.content_length * 2):
                        # New content is 2x longer (and > 100 bytes), update and process
                        existing.content_length = content_length
                        existing.source_url = article.url
                        existing.expires_at = now + timedelta(days=30)
                        logger.debug(f"Updating content hash with longer content: {fingerprint[:16]}...")
                        await _job_tracker.add_content_hash(fingerprint)
                        new_articles.append(article)
                    else:
                        # Already seen with similar/longer content - skip
                        logger.debug(f"Content found in persistent cache: {fingerprint[:16]}...")
                        db_skipped += 1
                else:
                    # Not in database - add it and include article
                    new_hash = ContentHash(
                        content_hash=fingerprint,
                        content_length=content_length,
                        source_url=article.url,
                        expires_at=now + timedelta(days=30)
                    )
                    new_hashes_to_add.append(new_hash)
                    await _job_tracker.add_content_hash(fingerprint)
                    new_articles.append(article)

            # Batch insert new hashes
            if new_hashes_to_add:
                session.add_all(new_hashes_to_add)

            await session.commit()

            # Combine in-memory and DB skips for total
            return new_articles, in_memory_skipped + db_skipped

    except Exception as e:
        # Don't fail if DB check fails - log and return all articles as non-duplicates
        logger.warning(f"Batch content cache check failed: {e}")
        # Add all fingerprints to in-memory cache
        for _, fingerprint, _ in non_cached:
            if fingerprint:
                await _job_tracker.add_content_hash(fingerprint)
        # FIX: Only return in-memory skips on exception (DB operations failed,
        # so any DB skips counted before the exception are unreliable)
        return [article for article, _, _ in non_cached], in_memory_skipped


# =============================================================================
# TOKEN OPTIMIZATION CONSTANTS
# =============================================================================

MAX_ARTICLES_PER_SOURCE = 100  # Increased for external sources (headline-only = cheaper processing)


def is_non_announcement_title(title: str) -> bool:
    """
    Check if article title is clearly NOT a funding announcement.

    Used as pre-filter to skip non-announcements BEFORE Claude call.
    Saves ~$3.75/month in token costs.

    Returns True if article should be SKIPPED.
    """
    if not title:
        return False  # Don't skip articles with no title

    title_lower = title.lower()

    # Skip patterns - these are almost never funding announcements
    skip_patterns = [
        # Year-end content
        'year in review', 'year-in-review', 'yearinreview',
        '2024 recap', '2025 recap', 'annual review', 'year end',
        'looking back at', 'best of 2024', 'best of 2025',
        # Portfolio updates without funding
        'portfolio update', 'portfolio spotlight', 'portfolio news',
        'company update:', 'quarterly update',
        # Interviews and Q&A (unless about funding)
        'interview with', 'q&a with', 'q&a:', 'in conversation with',
        'fireside chat', 'podcast:', 'episode:',
        # Events and conferences
        'demo day recap', 'conference recap', 'event recap',
        'webinar:', 'live event:', 'summit recap',
        # Opinion and analysis (not announcements)
        'opinion:', 'analysis:', 'predictions for', 'trends in',
        'what we learned', 'lessons from', 'how to',
        # Job postings and hiring (not funding)
        'we\'re hiring', 'join our team', 'career opportunities',
        'job opening', 'now hiring',
        # Exits and acquisitions (different from funding)
        'acquired by', 'acquisition of', 'merger with', 'ipo:',
    ]

    for pattern in skip_patterns:
        if pattern in title_lower:
            return True

    return False


def is_likely_funding_from_title(title: str) -> bool:
    """
    Check if article title suggests funding news.

    Used as pre-filter to skip non-funding articles BEFORE Claude call.
    Requires strong signal OR 2+ weak signals to reduce false positives.
    """
    if not title:
        return True  # Don't filter out articles with no title

    title_lower = title.lower()

    # Strong signals (1 is enough)
    strong_signals = [
        'series a', 'series b', 'series c', 'series d', 'seed round',
        'raises $', 'raised $', 'led by', 'funding round', 'pre-seed'
    ]
    if any(signal in title_lower for signal in strong_signals):
        return True

    # Weak signals (need 2+)
    weak_signals = [
        'funding', 'investment', 'venture', 'capital',
        'million', 'billion', 'backed', 'round', 'investor'
    ]
    return sum(1 for signal in weak_signals if signal in title_lower) >= 2


def should_skip_by_source(source_name: str, url: str, title: str) -> tuple[bool, str]:
    """
    Source-specific filtering to skip non-funding content.

    FIX (2026-01): Saves ~$5-10/month by skipping content that specific sources
    produce which is almost never funding announcements.

    Returns (should_skip, reason) tuple.
    """
    url_lower = url.lower() if url else ""
    title_lower = title.lower() if title else ""

    # Crunchbase: Skip reviews, jobs, company-about pages
    if source_name == "crunchbase" or "crunchbase.com" in url_lower:
        skip_patterns = [
            '/reviews/', '/jobs/', '/people/', '/acquisitions/',
            '/ipo/', '/hub/', '/lists/', '/discover/',
            '/organization/', '/event/', '/school/',
        ]
        for pattern in skip_patterns:
            if pattern in url_lower:
                return True, f"Crunchbase non-funding page: {pattern}"

    # YCombinator: Skip pitch pages, about pages, applications
    if source_name == "ycombinator" or "ycombinator.com" in url_lower:
        skip_patterns = [
            '/apply/', '/about/', '/blog/', '/library/',
            '/resources/', '/faq/', '/contact/',
        ]
        for pattern in skip_patterns:
            if pattern in url_lower:
                return True, f"YCombinator non-funding page: {pattern}"

        # Also skip if title suggests application/program info
        skip_titles = [
            'how to apply', 'application deadline', 'startup school',
            'office hours', 'yc library', 'founder resource',
        ]
        for pattern in skip_titles:
            if pattern in title_lower:
                return True, f"YCombinator non-funding title: {pattern}"

    # Hacker News: Skip discussion-only posts (no company website)
    if source_name == "hackernews" or "news.ycombinator.com" in url_lower:
        # Skip "Show HN" posts that are just discussions, not funding news
        if 'show hn:' in title_lower and 'funding' not in title_lower and 'raise' not in title_lower:
            return True, "HN Show post without funding context"

    # LinkedIn: Skip non-article content
    if "linkedin.com" in url_lower:
        skip_patterns = [
            '/jobs/', '/learning/', '/company/', '/school/',
            '/groups/', '/events/', '/pulse/topic/',
        ]
        for pattern in skip_patterns:
            if pattern in url_lower:
                return True, f"LinkedIn non-article page: {pattern}"

    return False, ""


async def enrich_new_deals(limit: int = 25):
    """
    Enrich newly scraped deals with website and CEO LinkedIn.

    Uses deal context (investor, founders, category) for accurate enrichment.
    Runs after each scrape job to populate company links.

    FIX: Uses per-deal transactions to prevent data loss if one enrichment fails.
    """
    try:
        from ..enrichment import enrich_company_with_context, DealContext
        from ..archivist import get_session, Deal, PortfolioCompany
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        import json

        # First, fetch all deals needing enrichment (read-only query)
        # FIX: Now also enriches deals that have website but missing LinkedIn
        # Priority: 1) No website, 2) Has founders but no LinkedIn, 3) No founders (discover CEO)
        async with get_session() as db:
            from sqlalchemy import or_, and_, not_, text

            query = (
                select(Deal)
                .options(
                    selectinload(Deal.company),
                    selectinload(Deal.investors),
                )
                .join(Deal.company)
                .where(
                    or_(
                        # Original: deals without website
                        (PortfolioCompany.website.is_(None)) | (PortfolioCompany.website == ""),
                        # NEW: deals with founders but no LinkedIn URL (text comparison)
                        and_(
                            Deal.founders_json.isnot(None),
                            Deal.founders_json != "[]",
                            not_(Deal.founders_json.like('%linkedin.com%'))
                        ),
                        # NEW: deals with no founders at all (need to discover CEO)
                        or_(Deal.founders_json.is_(None), Deal.founders_json == "[]"),
                    )
                )
                .order_by(Deal.created_at.desc())
                .limit(limit)
            )

            result = await db.execute(query)
            deals = result.scalars().all()

            if not deals:
                logger.info("No deals needing enrichment")
                return 0

            # Build list of (deal_id, company_id, company_name, context) for processing
            deals_to_enrich = []
            for deal in deals:
                company_name = deal.company.name if deal.company else None
                if not company_name or company_name.startswith("No "):
                    continue
                if company_name.lower() in ("unknown", "n/a", "<unknown>"):
                    continue

                founders = []
                if deal.founders_json:
                    try:
                        founders = json.loads(deal.founders_json)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        founders = []

                lead_investor = None
                for inv in deal.investors:
                    if inv.is_lead:
                        lead_investor = inv.investor_name
                        break

                context = DealContext(
                    company_name=company_name,
                    lead_investor=lead_investor,
                    founders=founders,
                    source_url=None,
                    enterprise_category=deal.enterprise_category,
                )

                deals_to_enrich.append({
                    "deal_id": deal.id,
                    "company_id": deal.company.id if deal.company else None,
                    "company_name": company_name,
                    "context": context,
                    "founders": founders,
                })

        # FIX: Parallel enrichment with semaphore - API calls only, batch DB writes
        # (was opening a new DB session per enrichment, causing pool exhaustion)
        # Now: 5 concurrent API calls, then batch persist in ONE session
        MAX_CONCURRENT_ENRICHMENT = 5
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ENRICHMENT)

        async def enrich_single_deal(deal_info: dict):
            """Enrich a single deal - API calls only, no DB access."""
            async with semaphore:
                try:
                    enrichment = await asyncio.wait_for(
                        enrich_company_with_context(deal_info["context"]),
                        timeout=15.0
                    )
                    # Return both deal_info and enrichment result
                    return (deal_info, enrichment)
                except asyncio.TimeoutError:
                    logger.warning(f"Enrichment timeout for {deal_info['company_name']}")
                    return (deal_info, None)
                except Exception as e:
                    logger.warning(f"Enrichment error for {deal_info['company_name']}: {e}")
                    return (deal_info, None)

        # Process all deals concurrently (limited by semaphore) - API calls only
        results = await asyncio.gather(
            *[enrich_single_deal(d) for d in deals_to_enrich],
            return_exceptions=True
        )

        # Filter out exceptions and None enrichments
        successful_enrichments = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Enrichment gather exception: {result}")
                continue
            deal_info, enrichment = result
            if enrichment is None:
                continue
            # Check if we have any useful data
            has_enrichment = (
                enrichment.website or
                enrichment.ceo_linkedin or
                enrichment.founder_linkedins
            )
            if has_enrichment:
                successful_enrichments.append((deal_info, enrichment))

        if not successful_enrichments:
            logger.info("No enrichment data found for any deals")
            return 0

        # FIX: Batch persist ALL enrichment results in ONE session (prevents pool exhaustion)
        enriched_count = 0
        async with get_session() as db:
            for deal_info, enrichment in successful_enrichments:
                try:
                    # Fetch fresh deal and company objects
                    deal = await db.get(Deal, deal_info["deal_id"])
                    if not deal or not deal.company:
                        continue

                    # Only update website if found
                    if enrichment.website:
                        deal.company.website = enrichment.website

                    # Update founders with LinkedIn URLs
                    founders = deal_info["founders"]
                    founders_updated = False

                    if enrichment.founder_linkedins or enrichment.ceo_linkedin:
                        # Update existing founders with their LinkedIn URLs
                        for f in founders:
                            founder_name = f.get("name", "")
                            if founder_name in enrichment.founder_linkedins:
                                if not f.get("linkedin_url"):
                                    f["linkedin_url"] = enrichment.founder_linkedins[founder_name]
                                    founders_updated = True

                        # Handle CEO LinkedIn - attach to existing founder OR create new entry
                        if enrichment.ceo_linkedin:
                            ceo_found = any(f.get("linkedin_url") for f in founders)
                            if not ceo_found:
                                # Try to attach to existing CEO/founder
                                attached = False
                                for f in founders:
                                    title_lower = f.get("title", "").lower()
                                    if any(t in title_lower for t in ("ceo", "chief executive", "founder", "co-founder", "cofounder")):
                                        if not f.get("linkedin_url"):
                                            f["linkedin_url"] = enrichment.ceo_linkedin
                                            founders_updated = True
                                            attached = True
                                        break

                                # If no founders exist OR no CEO title found, ADD the CEO as a new founder
                                if not attached and enrichment.ceo_name:
                                    founders.append({
                                        "name": enrichment.ceo_name,
                                        "title": "CEO",
                                        "linkedin_url": enrichment.ceo_linkedin,
                                    })
                                    founders_updated = True
                                    logger.info(f"Added discovered CEO to founders: {enrichment.ceo_name}")

                        if founders_updated:
                            deal.founders_json = json.dumps(founders)

                    enriched_count += 1

                except Exception as e:
                    logger.warning(
                        f"Error persisting enrichment for {deal_info['company_name']}: {e}",
                        exc_info=True,
                    )
                    continue

            # Single commit for all enrichments
            # (session context manager handles commit automatically)

        logger.info(f"Enriched {enriched_count} companies with website/LinkedIn data")
        return enriched_count

    except Exception as e:
        logger.error("Enrichment job failed: %s", e, exc_info=True)
        return 0

async def enrich_deal_dates(limit: int = 50) -> int:
    """
    Enrich deals with accurate announcement dates.

    Uses two sources:
    1. SEC Form D - official legal filing dates (highest confidence = 0.95)
    2. Brave Search - finds news articles with page_age metadata

    OPTIMIZED: Now uses 5 concurrent enrichments (was serial with 0.5s delays).
    50 deals: ~10s (was 25s+ serial)

    Returns:
        Number of deals with dates enriched
    """
    try:
        from ..enrichment.date_enrichment import DateEnrichmentClient, persist_deal_date
        from ..enrichment.sec_date_matcher import SECDateMatcher, persist_sec_date_match, DATE_CONFIDENCE
        from ..archivist.database import get_session
        from ..archivist.models import Deal, PortfolioCompany, DealInvestor, DateSource
        from sqlalchemy import select, or_
        from sqlalchemy.orm import selectinload
        from datetime import date, timedelta

        today = date.today()

        # Fetch deals needing date enrichment
        async with get_session() as db:
            query = (
                select(Deal)
                .options(
                    selectinload(Deal.company),
                    selectinload(Deal.investors),
                )
                .where(
                    or_(
                        Deal.announced_date.is_(None),
                        Deal.date_confidence < 0.7,
                    )
                )
                .order_by(Deal.created_at.desc())
                .limit(limit)
            )

            result = await db.execute(query)
            deals = result.scalars().all()

            if not deals:
                logger.info("No deals needing date enrichment")
                return 0

            # Build list for processing
            deals_to_enrich = []
            for deal in deals:
                company_name = deal.company.name if deal.company else None
                if not company_name or company_name.lower() in ("unknown", "n/a", "<unknown>"):
                    continue

                # Get lead investor name
                lead_investor = None
                for inv in deal.investors:
                    if inv.is_lead:
                        lead_investor = inv.investor_name
                        break

                deals_to_enrich.append({
                    "deal_id": deal.id,
                    "company_name": company_name,
                    "round_type": deal.round_type,
                    "lead_investor": lead_investor,
                    "amount": deal.amount,
                    "current_date": deal.announced_date,
                    "current_confidence": deal.date_confidence or 0.0,
                })

        logger.info(f"Processing {len(deals_to_enrich)} deals for date enrichment")

        # FIX: Parallel date enrichment with semaphore (was serial with 0.5s delays)
        # 5 concurrent = ~10s for 50 deals (vs 25s+ serial)
        MAX_CONCURRENT_DATE_ENRICHMENT = 5
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DATE_ENRICHMENT)
        enriched_count = [0]  # Use list for mutation in nested function
        count_lock = asyncio.Lock()

        async def enrich_single_deal(
            deal_info: dict,
            date_client: "DateEnrichmentClient",
            sec_matcher: "SECDateMatcher",
        ) -> bool:
            """Enrich a single deal's date with concurrent rate limiting."""
            async with semaphore:
                try:
                    found_date = None
                    date_source = None
                    confidence = 0.0
                    source_url = None

                    # Try 1: SEC Form D (highest confidence = 0.95)
                    try:
                        sec_match = await sec_matcher.search_company(
                            company_name=deal_info["company_name"],
                            amount=deal_info["amount"],
                            date_hint=deal_info["current_date"],
                        )
                        if sec_match and sec_match.is_confident_match:
                            found_date = sec_match.sec_filing_date
                            date_source = "sec_form_d"
                            confidence = DATE_CONFIDENCE["sec_form_d"]
                            source_url = sec_match.sec_filing_url

                            # Persist SEC match
                            await persist_sec_date_match(deal_info["deal_id"], sec_match)
                            logger.info(
                                f"SEC match for {deal_info['company_name']}: "
                                f"{found_date} (confidence={confidence})"
                            )
                    except Exception as e:
                        logger.debug(f"SEC lookup failed for {deal_info['company_name']}: {e}")

                    # Try 2: Brave Search (if no SEC match)
                    if not found_date:
                        try:
                            brave_result = await date_client.enrich_deal_date(
                                deal_id=deal_info["deal_id"],
                                company_name=deal_info["company_name"],
                                round_type=deal_info["round_type"],
                                lead_investor=deal_info["lead_investor"],
                            )
                            if brave_result.found_date:
                                found_date = brave_result.found_date
                                date_source = brave_result.date_source
                                source_url = brave_result.search_url

                                # Set confidence based on source
                                if brave_result.confidence == "high":
                                    confidence = 0.80
                                elif brave_result.confidence == "medium":
                                    confidence = 0.65
                                else:
                                    confidence = 0.50

                        except Exception as e:
                            logger.debug(f"Brave date lookup failed for {deal_info['company_name']}: {e}")

                    # Update deal if we found a better date
                    if found_date and confidence > deal_info["current_confidence"]:
                        async with get_session() as db:
                            deal = await db.get(Deal, deal_info["deal_id"])
                            if deal:
                                deal.announced_date = found_date
                                deal.date_confidence = confidence
                                deal.date_source_count = (deal.date_source_count or 0) + 1

                                if date_source == "sec_form_d" and source_url:
                                    deal.sec_filing_date = found_date
                                    deal.sec_filing_url = source_url

                                # Record in DateSource table
                                date_record = DateSource(
                                    deal_id=deal_info["deal_id"],
                                    source_type=date_source or "brave_search",
                                    source_url=source_url,
                                    extracted_date=found_date,
                                    confidence_score=confidence,
                                    is_primary=True,
                                )
                                db.add(date_record)

                                await db.commit()
                                async with count_lock:
                                    enriched_count[0] += 1
                                logger.info(
                                    f"Updated date for {deal_info['company_name']}: "
                                    f"{found_date} (source={date_source}, confidence={confidence:.2f})"
                                )
                                return True

                    # Small delay for rate limiting (inside semaphore)
                    await asyncio.sleep(0.1)
                    return False

                except Exception as e:
                    logger.warning(f"Error enriching date for {deal_info['company_name']}: {e}")
                    return False

        # Process all deals concurrently with shared clients
        async with DateEnrichmentClient() as date_client:
            async with SECDateMatcher() as sec_matcher:
                await asyncio.gather(
                    *[enrich_single_deal(d, date_client, sec_matcher) for d in deals_to_enrich],
                    return_exceptions=True
                )

        return enriched_count[0]

    except Exception as e:
        logger.error(f"Date enrichment job failed: {e}", exc_info=True)
        return 0


# Global scheduler instance
scheduler: Optional[AsyncIOScheduler] = None

# Job execution tracking (for observability)
_last_job_run: Optional[datetime] = None
_last_job_status: Optional[str] = None
_last_job_error: Optional[str] = None
_last_job_duration: Optional[float] = None
_current_scan_job_id: Optional[int] = None  # Track current scan for timeout handler


async def process_external_articles(
    articles: List[NormalizedArticle],
    source_name: str,
    scan_job_id: Optional[int] = None,
    skip_title_filter: bool = False,
) -> Dict[str, int]:
    """
    Process articles from external sources through extraction pipeline.

    This is the CRITICAL fix: external sources (Brave Search, SEC, etc.)
    were returning articles but never extracting deals from them.

    Args:
        articles: List of NormalizedArticle from external source
        source_name: Name of source for logging
        skip_title_filter: If True, bypass title-based filtering (for SEC, YC, etc.)

    Returns:
        Dict with processing stats
    """
    from ..analyst.extractor import extract_deal, set_extraction_context, clear_extraction_context, EXTERNAL_SOURCE_NAMES, EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD, get_extraction_stats, flush_token_usage_batch
    from ..archivist.storage import save_deal
    from ..archivist.database import get_session
    from ..archivist.models import Article
    from sqlalchemy import select

    # Set extraction context for token logging
    scan_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    set_extraction_context(source_name, scan_id)

    # FIX: Wrap in try/finally to ensure extraction context is always cleared
    # Without this, context leaks if exception occurs mid-processing
    try:
        return await _process_external_articles_impl(
            articles, source_name, scan_job_id, skip_title_filter
        )
    finally:
        clear_extraction_context()
        # Flush any remaining token usage records
        await flush_token_usage_batch(force=True)


async def _process_external_articles_impl(
    articles: List[NormalizedArticle],
    source_name: str,
    scan_job_id: Optional[int] = None,
    skip_title_filter: bool = False,
) -> Dict[str, int]:
    """Internal implementation of process_external_articles (extracted for try/finally wrapper)."""
    from ..analyst.extractor import extract_deal, EXTERNAL_SOURCE_NAMES, EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD, get_extraction_stats
    from ..archivist.storage import save_deal
    from ..archivist.database import get_session
    from ..archivist.models import Article
    from sqlalchemy import select

    stats = {
        "articles_received": len(articles),
        "articles_skipped_duplicate": 0,
        "articles_skipped_cross_source": 0,  # Phase 5: Cross-source dedup
        "articles_skipped_content_hash": 0,  # Syndicated article dedup (Jan 2026)
        "articles_skipped_title": 0,  # Title pre-filter
        "deals_extracted": 0,
        "deals_saved": 0,
        "leads_saved": 0,  # NEW: Track lead deals specifically
        "enterprise_ai_saved": 0,  # NEW: Track enterprise AI deals
        "errors": 0,
        # NEW: Confidence band distribution (for monitoring extraction quality)
        "confidence_bands": {
            "below_threshold": 0,  # 0.00-0.35: Below external threshold
            "borderline": 0,       # 0.35-0.50: Between thresholds
            "medium": 0,           # 0.50-0.65: Solid confidence
            "high": 0,             # 0.65-1.00: High confidence
        },
        # NEW: Rejection tracking (why deals weren't saved)
        "rejections": {
            "confidence_too_low": 0,
            "not_new_announcement": 0,
            "invalid_company_name": 0,
            "extraction_returned_none": 0,
        },
    }

    if not articles:
        return stats

    # OPTIMIZATION: Apply source limit
    if len(articles) > MAX_ARTICLES_PER_SOURCE:
        logger.info(f"[{source_name}] Limiting from {len(articles)} to {MAX_ARTICLES_PER_SOURCE} articles")
        articles = articles[:MAX_ARTICLES_PER_SOURCE]
        stats["articles_received"] = MAX_ARTICLES_PER_SOURCE

    logger.info(f"[{source_name}] Processing {len(articles)} articles through extraction pipeline")

    # Phase 5: Cross-source URL dedup (catches same article from different sources)
    # This runs BEFORE database check for efficiency
    cross_source_new = []
    for article in articles:
        if await check_global_url_seen(article.url):
            stats["articles_skipped_cross_source"] += 1
        else:
            cross_source_new.append(article)

    if stats["articles_skipped_cross_source"] > 0:
        logger.info(f"[{source_name}] Skipped {stats['articles_skipped_cross_source']} cross-source duplicates")

    articles = cross_source_new
    if not articles:
        logger.info(f"[{source_name}] All articles were cross-source duplicates")
        return stats

    # OPTIMIZATION: Batch check for duplicate URLs in single query (avoids N sessions)
    article_urls = [a.url for a in articles]
    existing_urls: set[str] = set()
    async with get_session() as session:
        # Also check normalized URLs to catch tracking param variants
        normalized_urls = [normalize_url(url) for url in article_urls]
        all_urls = set(article_urls) | set(normalized_urls)
        stmt = select(Article.url).where(Article.url.in_(all_urls))
        result = await session.execute(stmt)
        existing_urls = {row[0] for row in result.fetchall()}

    # Filter out duplicates (check both original and normalized)
    new_articles = []
    for a in articles:
        if a.url not in existing_urls and normalize_url(a.url) not in existing_urls:
            new_articles.append(a)
    stats["articles_skipped_duplicate"] = len(articles) - len(new_articles)

    if not new_articles:
        logger.info(f"[{source_name}] All {len(articles)} articles already processed")
        return stats

    # OPTIMIZATION: Title pre-filters - skip articles BEFORE Claude call
    # Filter 1: Skip non-announcement titles (year-in-review, interviews, etc.)
    # Filter 1.5: Source-specific filtering (FIX 2026-01: ~$5-10/month savings)
    # Skip content that specific sources produce which is almost never funding announcements
    if "articles_skipped_source_specific" not in stats:
        stats["articles_skipped_source_specific"] = 0

    source_filtered_articles = []
    for article in new_articles:
        should_skip, reason = should_skip_by_source(source_name, article.url, article.title)
        if should_skip:
            stats["articles_skipped_source_specific"] += 1
            logger.debug(f"[{source_name}] Skipping by source rule: {reason}")
        else:
            source_filtered_articles.append(article)

    if stats["articles_skipped_source_specific"] > 0:
        logger.info(f"[{source_name}] Skipped {stats['articles_skipped_source_specific']} articles by source-specific rules")

    new_articles = source_filtered_articles
    if not new_articles:
        logger.info(f"[{source_name}] No articles after source-specific filtering")
        return stats

    # Filter 2: Skip titles without funding signals
    # NOTE: Some sources (SEC, YCombinator) bypass title filtering since their
    # content is already funding-related but doesn't have typical news headlines
    if skip_title_filter:
        logger.info(f"[{source_name}] Skipping title filter (source content is pre-qualified)")
    else:
        filtered_articles = []
        skipped_non_announcement = 0
        for article in new_articles:
            # First check: Is this clearly NOT an announcement?
            if is_non_announcement_title(article.title):
                skipped_non_announcement += 1
                continue
            # Second check: Does it have funding signals?
            if is_likely_funding_from_title(article.title):
                filtered_articles.append(article)
            else:
                stats["articles_skipped_title"] += 1

        if skipped_non_announcement > 0:
            logger.info(f"[{source_name}] Skipped {skipped_non_announcement} non-announcement articles (year-in-review, interviews, etc.)")
        if stats["articles_skipped_title"] > 0:
            logger.info(f"[{source_name}] Skipped {stats['articles_skipped_title']} articles without funding signals")

        new_articles = filtered_articles

        if not new_articles:
            logger.info(f"[{source_name}] No articles passed title filter")
            return stats

    # OPTIMIZATION: Content hash dedup (Jan 2026) - catches syndicated articles with different URLs
    # Same article on TechCrunch + VentureBeat + Crunchbase = 3 Claude calls → now just 1
    # FIX (2026-01): Now uses persistent DB cache for cross-run dedup (~$10-15/month savings)
    # FIX (2026-01): Batched query replaces N per-article queries with 1 batch query
    content_dedup_articles, content_hash_skipped = await batch_check_content_seen(new_articles)
    stats["articles_skipped_content_hash"] = content_hash_skipped

    if stats["articles_skipped_content_hash"] > 0:
        logger.info(f"[{source_name}] Skipped {stats['articles_skipped_content_hash']} syndicated duplicates (same content, different URL)")

    new_articles = content_dedup_articles
    if not new_articles:
        logger.info(f"[{source_name}] All articles were content duplicates")
        return stats

    logger.info(f"[{source_name}] {len(new_articles)} articles to process (skipped {stats['articles_skipped_duplicate']} URL dupes, {stats['articles_skipped_content_hash']} content dupes, {stats['articles_skipped_title']} by title)")

    # OPTIMIZATION: Concurrent processing with semaphore (vs serial 0.5s per article)
    MAX_CONCURRENT = 5
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results_lock = asyncio.Lock()
    # FIX: Collect alerts to send OUTSIDE semaphore (was blocking 15s per alert)
    # FIX 2026-01: Limit alert queue to prevent unbounded memory growth
    # 10K articles could create 10K concurrent alert tasks without this limit
    MAX_ALERTS = 50
    pending_alerts = []
    alerts_lock = asyncio.Lock()

    async def process_article(article):
        async with semaphore:
            try:
                # Get fund config for context
                fund_config = FUND_REGISTRY.get(article.fund_slug) if article.fund_slug else None

                # Extract deal using Claude
                extraction = await extract_deal(
                    article_text=article.text,
                    source_url=article.url,
                    source_name=source_name,
                    fund_context=fund_config,
                )

                # Handle None returns
                if extraction is None:
                    logger.debug(f"[{source_name}] Extraction returned None for {article.url}")
                    async with results_lock:
                        stats["rejections"]["extraction_returned_none"] += 1
                    return None

                # Track confidence band distribution (for monitoring)
                async with results_lock:
                    conf = extraction.confidence_score
                    if conf < 0.35:
                        stats["confidence_bands"]["below_threshold"] += 1
                    elif conf < 0.50:
                        stats["confidence_bands"]["borderline"] += 1
                    elif conf < 0.65:
                        stats["confidence_bands"]["medium"] += 1
                    else:
                        stats["confidence_bands"]["high"] += 1

                # Skip non-announcement extractions (background mentions)
                if not extraction.is_new_announcement:
                    logger.debug(f"[{source_name}] Skipping non-announcement: {extraction.startup_name}")
                    async with results_lock:
                        stats["rejections"]["not_new_announcement"] += 1
                    return None

                # FIX: Reject invalid company names like <UNKNOWN>, etc.
                # These deals will likely be captured from another source with better content
                # Conservative list - only clear LLM placeholders
                invalid_names = {"<unknown>", "unknown", "n/a", "none", "tbd", ""}
                company_name = (extraction.startup_name or "").strip()
                if company_name.lower() in invalid_names or not company_name:
                    logger.warning(
                        f"[{source_name}] Skipping invalid company: '{extraction.startup_name}' - "
                        f"Amount: {extraction.amount}, Fund: {extraction.tracked_fund_name} - "
                        f"URL: {article.url}"
                    )
                    async with results_lock:
                        stats["rejections"]["invalid_company_name"] += 1
                    return None

                # Skip low-confidence extractions
                # Use lower threshold for external sources (headline-only, less context)
                is_external = source_name in EXTERNAL_SOURCE_NAMES
                confidence_threshold = (
                    EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD if is_external
                    else settings.extraction_confidence_threshold
                )
                if extraction.confidence_score < confidence_threshold:
                    logger.debug(f"[{source_name}] Low confidence ({extraction.confidence_score:.2f} < {confidence_threshold:.2f}): {extraction.startup_name}")
                    async with results_lock:
                        stats["rejections"]["confidence_too_low"] += 1
                    return None

                # Save to database
                alert_info = None
                async with get_session() as session:
                    deal, alert_info = await save_deal(
                        session=session,
                        extraction=extraction,
                        article_url=article.url,
                        article_title=article.title,
                        article_text=article.text,
                        source_fund_slug=article.fund_slug,
                        article_published_date=article.published_date,
                        scan_job_id=scan_job_id,
                        # Pass SEC amount if available (from NormalizedArticle)
                        sec_amount_usd=getattr(article, 'sec_amount_usd', None),
                        amount_source=getattr(article, 'amount_source', None),
                    )

                # Update stats thread-safely
                async with results_lock:
                    if deal:
                        stats["deals_saved"] += 1
                        stats["deals_extracted"] += 1
                        # Track lead and enterprise AI deals
                        if extraction.tracked_fund_is_lead:
                            stats["leads_saved"] += 1
                        if extraction.is_enterprise_ai:
                            stats["enterprise_ai_saved"] += 1
                        logger.info(
                            f"[{source_name}] Saved deal: {extraction.startup_name} "
                            f"(fund: {article.fund_slug or 'unknown'}, "
                            f"lead={extraction.tracked_fund_is_lead}, "
                            f"enterprise_ai={extraction.is_enterprise_ai})"
                        )
                    else:
                        logger.warning(
                            f"[{source_name}] save_deal returned None for {extraction.startup_name} "
                            f"(confidence={extraction.confidence_score:.2f}, "
                            f"is_new={extraction.is_new_announcement})"
                        )

                # FIX: Queue alert for sending OUTSIDE semaphore scope
                # This prevents 15s alert timeouts from blocking concurrent processing
                # FIX 2026-01: Check MAX_ALERTS limit to prevent unbounded queue
                if alert_info:
                    async with alerts_lock:
                        if len(pending_alerts) < MAX_ALERTS:
                            pending_alerts.append(alert_info)
                        else:
                            logger.warning(f"[{source_name}] Alert queue full ({MAX_ALERTS}), skipping alert for {alert_info.company_name}")

                return extraction

            except Exception as e:
                async with results_lock:
                    stats["errors"] += 1
                logger.error(
                    f"[{source_name}] FAILED to save deal from {article.url}: {e}",
                    exc_info=True
                )
                return None

    # Process all articles concurrently (limited by semaphore)
    results = await asyncio.gather(*[process_article(a) for a in new_articles], return_exceptions=True)

    # FIX 2026-01: Count exceptions in error stats (was only logging, not counting)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"[{source_name}] Unhandled exception processing article {i}: {result}")
            stats["errors"] += 1

    # FIX 2026-01: Send alerts in PARALLEL (was serial, blocking 15s per timeout)
    # This prevents slow alerts from blocking the entire scrape completion
    if pending_alerts:
        from .notifications import send_lead_deal_alert

        async def send_single_alert(alert_info):
            """Send a single alert with error handling."""
            try:
                await send_lead_deal_alert(
                    company_name=alert_info.company_name,
                    amount=alert_info.amount,
                    round_type=alert_info.round_type,
                    lead_investor=alert_info.lead_investor,
                    enterprise_category=alert_info.enterprise_category,
                    verification_snippet=alert_info.verification_snippet,
                )
            except Exception as alert_err:
                logger.error(
                    f"[{source_name}] Alert sending failed for {alert_info.company_name}: {alert_err}",
                    exc_info=True
                )

        # Send all alerts in parallel (return_exceptions=True prevents one failure from blocking others)
        await asyncio.gather(
            *[send_single_alert(alert) for alert in pending_alerts],
            return_exceptions=True
        )

    # Log confidence band distribution for monitoring
    bands = stats["confidence_bands"]
    total_extractions = sum(bands.values())
    if total_extractions > 0:
        logger.info(
            f"[{source_name}] CONFIDENCE_BANDS: "
            f"below_0.35={bands['below_threshold']} ({bands['below_threshold']*100/total_extractions:.0f}%), "
            f"0.35-0.50={bands['borderline']} ({bands['borderline']*100/total_extractions:.0f}%), "
            f"0.50-0.65={bands['medium']} ({bands['medium']*100/total_extractions:.0f}%), "
            f"0.65+={bands['high']} ({bands['high']*100/total_extractions:.0f}%)"
        )

    # Log rejection stats for debugging extraction quality
    rejections = stats["rejections"]
    total_rejections = sum(rejections.values())
    if total_rejections > 0:
        logger.info(
            f"[{source_name}] REJECTION_STATS: "
            f"confidence_too_low={rejections['confidence_too_low']}, "
            f"not_new_announcement={rejections['not_new_announcement']}, "
            f"invalid_company={rejections['invalid_company_name']}, "
            f"extraction_none={rejections['extraction_returned_none']}"
        )

    logger.info(
        f"[{source_name}] Extraction complete: "
        f"received={stats['articles_received']}, "
        f"cross_source={stats['articles_skipped_cross_source']}, "
        f"db_duplicates={stats['articles_skipped_duplicate']}, "
        f"content_hash={stats['articles_skipped_content_hash']}, "
        f"title_filtered={stats['articles_skipped_title']}, "
        f"extracted={stats['deals_extracted']}, "
        f"saved={stats['deals_saved']}, "
        f"leads={stats['leads_saved']}, "
        f"enterprise_ai={stats['enterprise_ai_saved']}, "
        f"errors={stats['errors']}"
    )

    return stats


async def process_stealth_signals(
    articles: List[NormalizedArticle],
    source_name: str,
    scan_job_id: Optional[int] = None,
) -> Dict[str, int]:
    """
    Process articles as stealth signals (pre-funding detection).

    Uses rule-based scoring instead of Claude extraction.
    Saves to stealth_signals table, NOT deals table.

    This saves ~$44/month in LLM costs for sources that produce 0 deals:
    - hackernews
    - ycombinator
    - github_trending
    - linkedin_jobs
    - delaware_corps

    Args:
        articles: List of NormalizedArticle from early signal source
        source_name: Name of source for logging
        scan_job_id: Optional scan job ID for tracking

    Returns:
        Dict with processing stats
    """
    from ..harvester.stealth_scorer import score_article
    from ..archivist.stealth_storage import save_stealth_signal
    from ..archivist.database import get_session

    stats = {
        "articles_received": len(articles),
        "articles_skipped_low_score": 0,
        "signals_saved": 0,
        "errors": 0,
        # For compatibility with external sources stats format
        "deals_saved": 0,
        "deals_extracted": 0,
    }

    if not articles:
        return stats

    # Apply source limit
    if len(articles) > MAX_ARTICLES_PER_SOURCE:
        logger.info(f"[{source_name}] Limiting from {len(articles)} to {MAX_ARTICLES_PER_SOURCE} articles")
        articles = articles[:MAX_ARTICLES_PER_SOURCE]
        stats["articles_received"] = MAX_ARTICLES_PER_SOURCE

    logger.info(f"[{source_name}] Processing {len(articles)} articles as stealth signals (no LLM)")

    # Minimum score threshold (skip low-quality signals)
    MIN_SCORE_THRESHOLD = 25

    async with get_session() as session:
        for article in articles:
            try:
                # Score the article using rule-based scoring
                scored = score_article(article, source_name)
                if not scored:
                    stats["errors"] += 1
                    continue

                # Skip low-score signals
                if scored.score < MIN_SCORE_THRESHOLD:
                    stats["articles_skipped_low_score"] += 1
                    continue

                # Save to stealth_signals table
                signal = await save_stealth_signal(
                    session=session,
                    company_name=scored.company_name,
                    source=source_name,
                    source_url=article.url,
                    score=scored.score,
                    signals=scored.signals,
                    metadata_json=scored.metadata,
                )

                if signal:
                    stats["signals_saved"] += 1

            except Exception as e:
                stats["errors"] += 1
                logger.warning(f"[{source_name}] Error processing stealth signal: {e}")

        # Commit all at once
        await session.commit()

    logger.info(
        f"[{source_name}] Stealth signals complete: "
        f"received={stats['articles_received']}, "
        f"saved={stats['signals_saved']}, "
        f"low_score={stats['articles_skipped_low_score']}, "
        f"errors={stats['errors']}"
    )

    # Set deals_saved for compatibility with results aggregation
    stats["deals_saved"] = 0  # Stealth signals don't create deals
    stats["deals_extracted"] = stats["signals_saved"]  # For stats display

    return stats


async def scrape_external_sources(days: int = 7, scan_job_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Scrape all external data sources and process through extraction pipeline.

    This is the KEY function that was missing from the scheduled job.
    External sources find articles about deals, but they must be extracted
    by Claude to become actual deal records in the database.

    OPTIMIZED: Free RSS sources now run in parallel (was serial = ~60s wasted).
    API-rate-limited sources (Brave, SEC, Twitter) still run serially.

    Sources scraped:
    1. Brave Search (with partner name queries for Benchmark, Khosla, etc.)
    2. SEC EDGAR (Form D filings)
    3. Google Alerts (RSS feeds)
    4. Twitter/X (API monitoring)
    5. TechCrunch RSS
    6. Fortune Term Sheet RSS
    7. LinkedIn Jobs (stealth detector)
    8. Crunchbase News RSS
    9. VentureBeat RSS
    10. Axios Pro Rata RSS
    11. StrictlyVC RSS
    12. Delaware Corps (new tech company formations)
    13. Y Combinator (Demo Day companies)
    14. GitHub Trending (dev tools before funding)
    15. Hacker News (launch posts)
    16. Tech Funding News (SaaS/AI/FinTech)
    17. Ventureburn (emerging markets/Africa)

    Args:
        days: Number of days to look back

    Returns:
        Dict with stats per source
    """
    results = {}
    total_deals_saved = 0

    # =========================================================================
    # PART 1: API-RATE-LIMITED SOURCES (run serially to respect rate limits)
    # FIX 2026-01: Added circuit breaker checks to skip sources with repeated failures
    # =========================================================================

    # 1. BRAVE SEARCH (most important - includes partner name queries)
    # FIX 2026-01: Added 120s timeout (Brave makes many API calls) + circuit breaker
    if settings.brave_search_key and not _circuit_breaker.is_disabled("brave_search"):
        try:
            from ..harvester.scrapers.brave_search import BraveSearchScraper

            async def _scrape_brave():
                freshness = "pd" if days <= 1 else ("pw" if days <= 7 else "pm")
                async with BraveSearchScraper() as scraper:
                    return await scraper.scrape_all(
                        freshness=freshness,
                        include_enterprise=True,
                        include_participation=False,  # Lead-only focus (Jan 2026) - saves ~80 requests/scan
                        include_stealth=True,
                        include_partner_names=True,  # Critical for Benchmark, Khosla, First Round
                    )

            articles = await asyncio.wait_for(_scrape_brave(), timeout=180.0)

            # Skip title filter - Brave queries are already targeted at funding news
            stats = await process_external_articles(articles, "brave_search", scan_job_id, skip_title_filter=True)
            results["brave_search"] = stats
            total_deals_saved += stats["deals_saved"]
            _circuit_breaker.record_success("brave_search")

        except asyncio.TimeoutError:
            logger.error("SCRAPER_TIMEOUT: brave_search timed out after 180s")
            results["brave_search"] = {"error": "timeout after 180s", "articles_found": 0}
            _circuit_breaker.record_error("brave_search")
        except Exception as e:
            logger.error("Brave Search scraping failed: %s", e)
            results["brave_search"] = {"error": str(e)}
            _circuit_breaker.record_error("brave_search")
    elif _circuit_breaker.is_disabled("brave_search"):
        results["brave_search"] = {"error": "circuit breaker open", "articles_found": 0}

    # 2. SEC EDGAR (free - Form D filings, rate limited)
    # FIX 2026-01: Added 180s timeout (SEC has many filings + rate limiting) + circuit breaker
    if not _circuit_breaker.is_disabled("sec_edgar"):
        try:
            from ..harvester.scrapers.sec_edgar import SECEdgarScraper

            async def _scrape_sec():
                async with SECEdgarScraper() as scraper:
                    filings = await scraper.fetch_recent_filings(hours=days * 24)
                    logger.info(f"SEC EDGAR: Processing {len(filings)} filings")

                    # FIX: Parallel SEC fetching with semaphore (was serial 1s per filing = 100s for 100 filings)
                    # SEC allows ~3 concurrent requests safely
                    # Now: 3 concurrent = ~35s for 100 filings (65% faster)
                    MAX_SEC_CONCURRENT = 3
                    sec_semaphore = asyncio.Semaphore(MAX_SEC_CONCURRENT)

                    # FIX 2026-01: Token bucket rate limiting - only sleep when needed
                    # Previous bug: sleep(1.0) inside semaphore serialized ALL requests
                    # Now: track last request time, only sleep if < 0.35s since last request
                    # With 3 concurrent + 0.35s min interval = ~8.5 requests/sec (SEC allows 10/sec)
                    _sec_last_request = [0.0]  # Use list for mutation in nested function
                    _sec_rate_lock = asyncio.Lock()
                    SEC_MIN_INTERVAL = 0.35  # Minimum seconds between requests

                    async def fetch_filing_with_limit(filing):
                        """Fetch filing details with token-bucket rate limiting."""
                        async with sec_semaphore:
                            # FIX 2026-01: Token bucket rate limiting - release lock BEFORE network call
                            # Previous bug: lock was held during fetch_filing_details(), serializing
                            # all requests (reduced effective concurrency from 3 to 1)
                            async with _sec_rate_lock:
                                now = time.monotonic()
                                elapsed = now - _sec_last_request[0]
                                if elapsed < SEC_MIN_INTERVAL:
                                    await asyncio.sleep(SEC_MIN_INTERVAL - elapsed)
                                # Update timestamp AFTER sleep, right before releasing lock
                                _sec_last_request[0] = time.monotonic()
                            # Lock released HERE - network call happens outside lock
                            result = await scraper.fetch_filing_details(filing)
                            if result is None:
                                return None
                            matched_fund = scraper.match_tracked_fund(result)
                            article = await scraper.to_normalized_article(result, matched_fund)
                            return article

                    # Fetch all filings concurrently (limited by semaphore)
                    results_list = await asyncio.gather(
                        *[fetch_filing_with_limit(f) for f in filings],
                        return_exceptions=True
                    )

                    # Filter out None results and exceptions
                    articles = []
                    for result in results_list:
                        if result is not None and not isinstance(result, Exception):
                            articles.append(result)
                        elif isinstance(result, Exception):
                            logger.warning(f"SEC filing fetch error: {result}")

                    logger.info(f"SEC EDGAR: Created {len(articles)} articles with full details")
                    return articles

            articles = await asyncio.wait_for(_scrape_sec(), timeout=180.0)

            # SEC filings are already funding-related, bypass title filter
            stats = await process_external_articles(articles, "sec_edgar", scan_job_id, skip_title_filter=True)
            results["sec_edgar"] = stats
            total_deals_saved += stats["deals_saved"]
            _circuit_breaker.record_success("sec_edgar")

        except asyncio.TimeoutError:
            logger.error("SCRAPER_TIMEOUT: sec_edgar timed out after 180s")
            results["sec_edgar"] = {"error": "timeout after 180s", "articles_found": 0}
            _circuit_breaker.record_error("sec_edgar")
        except Exception as e:
            logger.error("SEC EDGAR scraping failed: %s", e)
            results["sec_edgar"] = {"error": str(e)}
            _circuit_breaker.record_error("sec_edgar")
    else:
        results["sec_edgar"] = {"error": "circuit breaker open", "articles_found": 0}

    # 3. TWITTER (API rate limited)
    # FIX 2026-01: Added 60s timeout
    if settings.twitter_bearer_token:
        try:
            from ..harvester.scrapers.twitter_monitor import TwitterMonitor

            async def _scrape_twitter():
                async with TwitterMonitor() as monitor:
                    return await monitor.scrape_all(hours_back=days * 24)

            articles = await asyncio.wait_for(_scrape_twitter(), timeout=60.0)

            stats = await process_external_articles(articles, "twitter", scan_job_id)
            results["twitter"] = stats
            total_deals_saved += stats["deals_saved"]

        except asyncio.TimeoutError:
            logger.error("SCRAPER_TIMEOUT: twitter timed out after 60s")
            results["twitter"] = {"error": "timeout after 60s", "articles_found": 0}
        except Exception as e:
            logger.error("Twitter scraping failed: %s", e)
            results["twitter"] = {"error": str(e)}

    # 4. LINKEDIN JOBS / STEALTH DETECTOR (uses Brave Search - rate limited)
    # ROUTED TO STEALTH PIPELINE: This source produces 0 deals but good pre-funding signals
    # FIX 2026-01: Added 60s timeout
    if settings.brave_search_key:
        try:
            from ..harvester.scrapers.linkedin_jobs import LinkedInJobsScraper

            async def _scrape_linkedin_jobs():
                async with LinkedInJobsScraper() as scraper:
                    return await scraper.scrape_all()

            articles = await asyncio.wait_for(_scrape_linkedin_jobs(), timeout=60.0)

            # Route to stealth signals (saves ~$8/month in LLM costs)
            stats = await process_stealth_signals(articles, "linkedin_jobs", scan_job_id)
            results["linkedin_jobs"] = stats

        except asyncio.TimeoutError:
            logger.error("SCRAPER_TIMEOUT: linkedin_jobs timed out after 60s")
            results["linkedin_jobs"] = {"error": "timeout after 60s", "articles_found": 0}
        except Exception as e:
            logger.error("LinkedIn Jobs scraping failed: %s", e)
            results["linkedin_jobs"] = {"error": str(e)}

    # 5. DELAWARE CORPS (uses Brave Search - rate limited)
    # ROUTED TO STEALTH PIPELINE: This source produces 0 deals but good pre-funding signals
    # FIX 2026-01: Added 60s timeout
    if settings.brave_search_key:
        try:
            from ..harvester.scrapers.delaware_corps import DelawareCorpsScraper

            async def _scrape_delaware():
                async with DelawareCorpsScraper() as scraper:
                    return await scraper.scrape_all(days_back=days, min_score=3)

            articles = await asyncio.wait_for(_scrape_delaware(), timeout=60.0)

            # Route to stealth signals (saves ~$8/month in LLM costs)
            stats = await process_stealth_signals(articles, "delaware_corps", scan_job_id)
            results["delaware_corps"] = stats

        except asyncio.TimeoutError:
            logger.error("SCRAPER_TIMEOUT: delaware_corps timed out after 60s")
            results["delaware_corps"] = {"error": "timeout after 60s", "articles_found": 0}
        except Exception as e:
            logger.error("Delaware Corps scraping failed: %s", e)
            results["delaware_corps"] = {"error": str(e)}

    # =========================================================================
    # PART 2: FREE RSS/HTTP SOURCES (run in parallel - no rate limits)
    # FIX: Was running 12+ sources serially (~60s wasted)
    # Now: All run in parallel with asyncio.gather (~5-10s total)
    # =========================================================================

    async def scrape_google_alerts() -> tuple:
        """Scrape Google Alerts RSS feeds."""
        source_name = "google_alerts"
        if not settings.google_alerts_feeds:
            return source_name, {"articles_found": 0, "status": "not_configured"}
        try:
            from ..harvester.scrapers.google_alerts import GoogleAlertsScraper
            feed_urls = [f.strip() for f in settings.google_alerts_feeds.split(',') if f.strip()]
            async with GoogleAlertsScraper(feed_urls) as scraper:
                articles = await scraper.scrape_all()
            stats = await process_external_articles(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("Google Alerts scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_techcrunch() -> tuple:
        """Scrape TechCrunch RSS."""
        source_name = "techcrunch"
        try:
            from ..harvester.scrapers.techcrunch_rss import TechCrunchScraper
            async with TechCrunchScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            stats = await process_external_articles(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("TechCrunch scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_crunchbase() -> tuple:
        """Scrape Crunchbase News RSS."""
        source_name = "crunchbase_news"
        try:
            from ..harvester.scrapers.crunchbase_news import CrunchbaseNewsScraper
            async with CrunchbaseNewsScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            # Bypass title filter - Crunchbase is funding-focused
            stats = await process_external_articles(articles, source_name, scan_job_id, skip_title_filter=True)
            return source_name, stats
        except Exception as e:
            logger.error("Crunchbase News scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_venturebeat() -> tuple:
        """Scrape VentureBeat RSS."""
        source_name = "venturebeat"
        try:
            from ..harvester.scrapers.venturebeat import VentureBeatScraper
            async with VentureBeatScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            stats = await process_external_articles(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("VentureBeat scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_axios() -> tuple:
        """Scrape Axios Pro Rata RSS."""
        source_name = "axios_prorata"
        try:
            from ..harvester.scrapers.axios_prorata import AxiosProRataScraper
            async with AxiosProRataScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            stats = await process_external_articles(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("Axios Pro Rata scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_strictlyvc() -> tuple:
        """Scrape StrictlyVC RSS."""
        source_name = "strictlyvc"
        try:
            from ..harvester.scrapers.strictlyvc import StrictlyVCScraper
            async with StrictlyVCScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            stats = await process_external_articles(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("StrictlyVC scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_prwire() -> tuple:
        """Scrape PR Wire RSS (PRNewswire, GlobeNewswire, BusinessWire)."""
        source_name = "prwire"
        try:
            from ..harvester.scrapers.prwire_rss import PRWireRSSScraper
            async with PRWireRSSScraper() as scraper:
                articles = await scraper.scrape_all_feeds(hours_back=days * 24, fund_filter=True)
            stats = await process_external_articles(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("PR Wire RSS scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_google_news() -> tuple:
        """Scrape Google News RSS for external-only funds (Thrive, Benchmark, etc.)."""
        source_name = "google_news"
        try:
            from ..harvester.scrapers.google_news_rss import GoogleNewsRSSScraper
            async with GoogleNewsRSSScraper() as scraper:
                articles = await scraper.scrape_all(days_back=days)
            stats = await process_external_articles(articles, source_name, scan_job_id, skip_title_filter=True)
            return source_name, stats
        except Exception as e:
            logger.error("Google News RSS scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_ycombinator() -> tuple:
        """Scrape Y Combinator (Demo Day companies).

        ROUTED TO STEALTH PIPELINE: This source produces 0 deals but good pre-funding signals.
        YC Demo Day companies are excellent early signals (saves ~$10/month in LLM costs).
        """
        source_name = "ycombinator"
        try:
            from ..harvester.scrapers.ycombinator import YCombinatorScraper
            async with YCombinatorScraper() as scraper:
                articles = await scraper.scrape_all(filter_enterprise=True)
            stats = await process_stealth_signals(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("Y Combinator scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_github() -> tuple:
        """Scrape GitHub Trending (dev tools before funding).

        ROUTED TO STEALTH PIPELINE: This source produces 0 deals but good pre-funding signals.
        Trending repos are often VC-backed startups before announcement (saves ~$8/month in LLM costs).
        """
        source_name = "github_trending"
        try:
            from ..harvester.scrapers.github_trending import GitHubTrendingScraper
            async with GitHubTrendingScraper() as scraper:
                repos = await scraper.fetch_all_trending(since="daily")
                articles = await scraper.scrape_all(since="daily", filter_enterprise=True, repos=repos)
            stats = await process_stealth_signals(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("GitHub Trending scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_hackernews() -> tuple:
        """Scrape Hacker News (launch posts).

        ROUTED TO STEALTH PIPELINE: This source produces 0 deals but good pre-funding signals.
        Launch HN posts are often pre-funding companies (saves ~$10/month in LLM costs).
        """
        source_name = "hackernews"
        try:
            from ..harvester.scrapers.hackernews import HackerNewsScraper
            async with HackerNewsScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            stats = await process_stealth_signals(articles, source_name, scan_job_id)
            return source_name, stats
        except Exception as e:
            logger.error("Hacker News scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_tech_funding_news() -> tuple:
        """Scrape Tech Funding News RSS (SaaS/AI/FinTech)."""
        source_name = "tech_funding_news"
        try:
            from ..harvester.scrapers.tech_funding_news import TechFundingNewsScraper
            async with TechFundingNewsScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            # Bypass title filter - this is literally a funding news site
            stats = await process_external_articles(articles, source_name, scan_job_id, skip_title_filter=True)
            return source_name, stats
        except Exception as e:
            logger.error("Tech Funding News scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_ventureburn() -> tuple:
        """Scrape Ventureburn RSS (emerging markets/Africa)."""
        source_name = "ventureburn"
        try:
            from ..harvester.scrapers.ventureburn import VentureburnScraper
            async with VentureburnScraper() as scraper:
                articles = await scraper.scrape_all(hours_back=days * 24)
            # Bypass title filter - Ventureburn is funding-focused
            stats = await process_external_articles(articles, source_name, scan_job_id, skip_title_filter=True)
            return source_name, stats
        except Exception as e:
            logger.error("Ventureburn scraping failed: %s", e)
            return source_name, {"error": str(e)}

    async def scrape_portfolio_diff() -> tuple:
        """Scrape Portfolio Diff (stealth detection via portfolio page diffing)."""
        source_name = "portfolio_diff"
        try:
            from ..harvester.scrapers.portfolio_diff import PortfolioDiffScraper
            from ..archivist.storage import save_stealth_detection
            from ..archivist.database import get_session

            async with PortfolioDiffScraper() as scraper:
                articles = await scraper.scrape_all()

            # Save stealth detections with per-article transactions
            stealth_saved = 0
            for article in articles:
                try:
                    async with get_session() as session:
                        await save_stealth_detection(
                            session=session,
                            fund_slug=article.fund_slug,
                            detected_url=article.url,
                            company_name=article.title.split(":")[1].strip() if ":" in article.title else None,
                            notes=f"Source: Portfolio Diff | Tags: {', '.join(article.tags)}",
                        )
                    stealth_saved += 1
                except Exception as e:
                    logger.warning(f"Failed to save stealth detection for {article.url}: {e}")

            return source_name, {
                "articles_found": len(articles),
                "stealth_saved": stealth_saved,
            }

        except Exception as e:
            logger.error("Portfolio Diff scraping failed: %s", e)
            return source_name, {"error": str(e)}

    # Run all free sources in parallel with individual timeouts
    # FIX 2026-01: Each source has a 60s timeout to prevent job hangs
    # NOTE: Removed 3 dead RSS sources (Dec 2025):
    #   - scrape_venturebeat() - RSS blocked/empty
    #   - scrape_axios() - RSS 404, feed deprecated
    #   - scrape_strictlyvc() - RSS dead since April 2020
    logger.info("Phase 2b: Running 11 free RSS/HTTP sources in parallel (60s timeout each)...")
    parallel_tasks = [
        with_timeout(scrape_google_alerts(), SOURCE_SCRAPE_TIMEOUT, "google_alerts"),
        with_timeout(scrape_techcrunch(), SOURCE_SCRAPE_TIMEOUT, "techcrunch"),
        with_timeout(scrape_crunchbase(), SOURCE_SCRAPE_TIMEOUT, "crunchbase_news"),
        # scrape_venturebeat(),  # DISABLED: RSS blocked Dec 2025
        # scrape_axios(),  # DISABLED: RSS 404 Dec 2025
        # scrape_strictlyvc(),  # DISABLED: RSS dead since April 2020
        with_timeout(scrape_prwire(), SOURCE_SCRAPE_TIMEOUT, "prwire"),
        with_timeout(scrape_google_news(), SOURCE_SCRAPE_TIMEOUT, "google_news"),
        with_timeout(scrape_ycombinator(), SOURCE_SCRAPE_TIMEOUT, "ycombinator"),
        with_timeout(scrape_github(), SOURCE_SCRAPE_TIMEOUT, "github_trending"),
        with_timeout(scrape_hackernews(), SOURCE_SCRAPE_TIMEOUT, "hackernews"),
        with_timeout(scrape_tech_funding_news(), SOURCE_SCRAPE_TIMEOUT, "tech_funding_news"),
        with_timeout(scrape_ventureburn(), SOURCE_SCRAPE_TIMEOUT, "ventureburn"),
        with_timeout(scrape_portfolio_diff(), SOURCE_SCRAPE_TIMEOUT, "portfolio_diff"),
    ]

    parallel_results = await asyncio.gather(*parallel_tasks, return_exceptions=True)

    # Process parallel results
    for result in parallel_results:
        if isinstance(result, Exception):
            logger.error(f"Parallel scrape task failed: {result}")
            continue
        source_name, stats = result
        results[source_name] = stats
        if isinstance(stats, dict) and "deals_saved" in stats:
            total_deals_saved += stats["deals_saved"]

    # Fortune Term Sheet - DISABLED (Fortune blocked RSS access Dec 2024)
    results["fortune"] = {"articles_found": 0, "deals_saved": 0, "status": "disabled"}

    results["total_deals_saved"] = total_deals_saved

    # Phase 5: Calculate scraper health metrics
    total_sources = 0
    successful_sources = 0
    failed_sources = []
    zero_result_sources = []  # FIX: Track scrapers returning 0 results (potential broken selectors)
    total_articles_received = 0
    total_cross_source_skipped = 0
    total_leads_saved = 0
    total_enterprise_ai_saved = 0

    # NEW: Build per-source stats for detailed monitoring
    per_source_stats = {}

    # Aggregate confidence bands and rejections across all sources
    aggregate_confidence_bands = {
        "below_threshold": 0,
        "borderline": 0,
        "medium": 0,
        "high": 0,
    }
    aggregate_rejections = {
        "confidence_too_low": 0,
        "not_new_announcement": 0,
        "invalid_company_name": 0,
        "extraction_returned_none": 0,
    }

    for source_name, stats in results.items():
        if source_name == "total_deals_saved":
            continue
        total_sources += 1
        if isinstance(stats, dict) and "error" in stats:
            failed_sources.append(source_name)
        else:
            successful_sources += 1
            if isinstance(stats, dict):
                articles = stats.get("articles_received", 0)
                deals = stats.get("deals_saved", 0)
                leads = stats.get("leads_saved", 0)
                enterprise_ai = stats.get("enterprise_ai_saved", 0)

                total_articles_received += articles
                total_cross_source_skipped += stats.get("articles_skipped_cross_source", 0)
                total_leads_saved += leads
                total_enterprise_ai_saved += enterprise_ai

                # Build per-source stats
                per_source_stats[source_name] = {
                    "articles": articles,
                    "deals": deals,
                    "leads": leads,
                    "enterprise_ai": enterprise_ai,
                }

                # Aggregate confidence bands
                if "confidence_bands" in stats:
                    for band, count in stats["confidence_bands"].items():
                        if band in aggregate_confidence_bands:
                            aggregate_confidence_bands[band] += count

                # Aggregate rejections
                if "rejections" in stats:
                    for reason, count in stats["rejections"].items():
                        if reason in aggregate_rejections:
                            aggregate_rejections[reason] += count

                # FIX: Flag zero-result sources (may indicate broken selectors)
                # Skip sources that are expected to have 0 results sometimes
                expected_zero_sources = {"twitter", "linkedin_jobs", "fortune"}
                if articles == 0 and source_name not in expected_zero_sources:
                    zero_result_sources.append(source_name)

    # Calculate health rate
    health_rate = (successful_sources / total_sources * 100) if total_sources > 0 else 0

    # Get extraction filter stats (from extractor.py post-processing)
    from ..analyst.extractor import get_extraction_stats
    extraction_filter_stats = get_extraction_stats()

    # Add health metrics to results
    results["_health_metrics"] = {
        "total_sources": total_sources,
        "successful_sources": successful_sources,
        "failed_sources": failed_sources,
        "zero_result_sources": zero_result_sources,  # FIX: Track potential broken scrapers
        "health_rate_pct": round(health_rate, 1),
        "total_articles_received": total_articles_received,
        "cross_source_duplicates_caught": total_cross_source_skipped,
        # NEW: Detailed per-source stats for monitoring
        "per_source_stats": per_source_stats,
        "total_leads_saved": total_leads_saved,
        "total_enterprise_ai_saved": total_enterprise_ai_saved,
        # NEW: Aggregate confidence bands across all sources
        "confidence_bands": aggregate_confidence_bands,
        # NEW: Aggregate rejection stats across all sources
        "rejection_stats": aggregate_rejections,
        # NEW: Extraction filter stats (crypto, consumer AI, etc.)
        "extraction_filter_stats": extraction_filter_stats,
    }

    # Log health summary
    if failed_sources:
        logger.warning(
            f"External sources health: {successful_sources}/{total_sources} sources succeeded "
            f"({health_rate:.0f}%), FAILED: {', '.join(failed_sources)}"
        )

    # FIX: Log warning for zero-result sources (potential broken scrapers)
    if zero_result_sources:
        logger.warning(
            f"SCRAPER_HEALTH_ALERT: {len(zero_result_sources)} scrapers returned 0 results "
            f"(may indicate broken selectors): {', '.join(zero_result_sources)}"
        )

    if not failed_sources and not zero_result_sources:
        logger.info(
            f"External sources health: {successful_sources}/{total_sources} sources succeeded (100%), "
            f"total_articles={total_articles_received}, cross_source_dedup={total_cross_source_skipped}"
        )

    # Log extraction filter stats (for monitoring post-processing activity)
    if any(extraction_filter_stats.values()):
        logger.info(
            f"EXTRACTION_FILTER_STATS: "
            f"crypto={extraction_filter_stats['crypto_filtered']}, "
            f"consumer_ai={extraction_filter_stats['consumer_ai_filtered']}, "
            f"consumer_fintech={extraction_filter_stats['consumer_fintech_filtered']}, "
            f"company_rejected={extraction_filter_stats['company_name_rejected']}, "
            f"investors_removed={extraction_filter_stats['investors_removed']}, "
            f"background={extraction_filter_stats['background_mention_rejected']}, "
            f"article_title={extraction_filter_stats['article_title_rejected']}, "
            f"lead_downgraded={extraction_filter_stats['lead_evidence_downgraded']}, "
            f"amount_flagged={extraction_filter_stats['amount_flagged_for_review']}"
        )

    logger.info(f"External sources complete: {total_deals_saved} total deals saved")

    return results


# Job-level timeout (30 minutes max to prevent hanging forever)
JOB_TIMEOUT_SECONDS = 1800


async def scheduled_scrape_job(trigger: str = "scheduled"):
    """
    Main scheduled scraping job with timeout protection.

    Scrapes all 18 implemented funds with parallel execution,
    PLUS all external data sources (Brave Search, SEC, etc.).
    Runs every 4 hours.

    This is the FIXED version that includes external sources
    which contain partner name queries for funds like Benchmark,
    Khosla, and First Round that don't have useful website scrapers.

    FIX: Wrapped in job-level timeout to prevent hanging forever.
    If job exceeds 30 minutes, it's killed and logged.

    Args:
        trigger: How the job was triggered ("scheduled", "manual", "api")
    """
    try:
        await asyncio.wait_for(
            _scheduled_scrape_job_impl(trigger),
            timeout=JOB_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.error(
            f"SCRAPER_HEALTH_ALERT: Scheduled scrape job timed out after "
            f"{JOB_TIMEOUT_SECONDS // 60} minutes - job killed"
        )
        # Update global tracking for observability
        global _last_job_status, _last_job_error, _last_job_duration, _current_scan_job_id
        _last_job_status = "timeout"
        _last_job_error = f"Job timed out after {JOB_TIMEOUT_SECONDS} seconds"
        _last_job_duration = JOB_TIMEOUT_SECONDS

        # FIX: Update database to mark scan as failed (was missing, caused stuck scans)
        if _current_scan_job_id:
            try:
                from ..archivist.database import get_session
                from ..archivist.models import ScanJob
                from sqlalchemy import select

                async with get_session() as session:
                    result = await session.execute(
                        select(ScanJob).where(ScanJob.id == _current_scan_job_id)
                    )
                    scan_job = result.scalar_one_or_none()
                    if scan_job and scan_job.status == "running":
                        scan_job.status = "failed"
                        scan_job.error_message = f"Job timed out after {JOB_TIMEOUT_SECONDS} seconds"
                        scan_job.completed_at = datetime.now(timezone.utc)
                        await session.commit()
                        logger.info(f"Marked timed-out scan {_current_scan_job_id} as failed")
            except Exception as e:
                logger.error(f"Failed to update timed-out scan status: {e}")
            finally:
                _current_scan_job_id = None


async def _scheduled_scrape_job_impl(trigger: str = "scheduled"):
    """Internal implementation of scheduled_scrape_job (separated for timeout wrapper).

    FIX 2026-01: Now wrapped with ScanJobGuard for heartbeat monitoring and
    guaranteed status updates even on crash/OOM.
    """
    global _last_job_run, _last_job_status, _last_job_error, _last_job_duration, _current_scan_job_id

    job_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"[{job_id}] Starting scheduled scrape job (trigger={trigger})")

    # Track job execution
    _last_job_run = datetime.now(timezone.utc)
    _last_job_status = "running"
    _last_job_error = None

    # Create ScanJob record in database
    from ..archivist.models import ScanJob
    from ..archivist.database import get_session
    from .scan_guard import guarded_scan
    import json

    scan_job_db_id = None
    try:
        async with get_session() as session:
            scan_job = ScanJob(
                job_id=job_id,
                status="running",
                trigger=trigger,
                last_heartbeat=datetime.now(timezone.utc),  # FIX 2026-01: Initialize heartbeat
            )
            session.add(scan_job)
            await session.commit()
            await session.refresh(scan_job)
            scan_job_db_id = scan_job.id
            # Track in global for timeout handler access
            _current_scan_job_id = scan_job_db_id
            logger.info(f"[{job_id}] Created ScanJob record (id={scan_job_db_id})")
    except Exception as e:
        logger.warning(f"[{job_id}] Failed to create ScanJob record: {e}")

    # ===== PHASE 0: Clear caches for fresh run =====
    # Without this, duplicate detection prevents processing the same article
    # across different runs (100% data loss after first run)
    from ..analyst.extractor import clear_content_hash_cache, clear_extraction_stats, get_extraction_stats, flush_token_usage_batch
    from ..enrichment.brave_enrichment import cleanup_linkedin_cache

    clear_content_hash_cache()
    clear_extraction_stats()  # Clear extraction filter stats for fresh monitoring
    await clear_job_tracker()  # FIX 2026-01: Single function clears all job state
    await cleanup_linkedin_cache()  # Only remove expired entries, preserve valid cache
    logger.info(f"[{job_id}] Phase 0: Caches cleared (content hash, URL dedup, content dedup, LinkedIn cleanup)")

    start_time = datetime.now(timezone.utc)

    # FIX 2026-01: Wrap phase execution with ScanJobGuard for heartbeat monitoring
    # and guaranteed status updates even on crash/OOM
    async with guarded_scan(scan_job_db_id, job_id) as guard:
        # Pass task reference so guard can cancel on SIGTERM
        if guard:
            guard.set_scan_task(asyncio.current_task())
        try:
            # ===== PHASE 1: Scrape fund websites =====
            fund_slugs = get_implemented_scrapers()
            logger.info(f"[{job_id}] Phase 1: Scraping {len(fund_slugs)} fund websites")

            # Run scrapers with parallel=True, max 3 concurrent
            results = await scrape_all_funds(
                fund_slugs=fund_slugs,
                parallel=True,
                max_parallel_funds=3,
                scan_job_id=scan_job_db_id,
            )

            # Calculate Phase 1 stats
            fund_articles = sum(r.articles_found for r in results)
            fund_deals = sum(r.deals_saved for r in results)
            fund_errors = sum(len(r.errors) for r in results)

            logger.info(
                f"[{job_id}] Phase 1 complete: "
                f"articles={fund_articles}, deals={fund_deals}, errors={fund_errors}"
            )

            # Manual heartbeat to prevent false "stuck" detection during long scans
            if guard:
                await guard.heartbeat()

            # ===== PHASE 2: Scrape external sources (THE FIX) =====
            logger.info(f"[{job_id}] Phase 2: Scraping external sources (Brave Search, SEC, etc.)")

            external_results = await scrape_external_sources(days=7, scan_job_id=scan_job_db_id)
            external_deals = external_results.get("total_deals_saved", 0)

            logger.info(
                f"[{job_id}] Phase 2 complete: "
                f"external_deals={external_deals}"
            )

            # Manual heartbeat after Phase 2
            if guard:
                await guard.heartbeat()

            # ===== Calculate total stats =====
            total_deals = fund_deals + external_deals
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            logger.info(
                f"[{job_id}] Scrape complete: "
                f"fund_deals={fund_deals}, external_deals={external_deals}, "
                f"total_deals={total_deals}, duration={duration:.1f}s"
            )

            # ===== PHASE 3: Enrich new deals =====
            # Always run enrichment to catch up on any missing data
            logger.info(f"[{job_id}] Phase 3: Starting automatic enrichment (website + LinkedIn)")
            # FIX #6: Increased from 50 to 100 to improve coverage (was taking weeks at 50)
            enriched = await enrich_new_deals(limit=100)
            logger.info(f"[{job_id}] Enrichment complete: {enriched} companies updated")

            # Manual heartbeat after Phase 3
            if guard:
                await guard.heartbeat()

            # ===== PHASE 4: Enrich deal dates =====
            # FIX: Date enrichment was never running - all the code existed but wasn't called
            logger.info(f"[{job_id}] Phase 4: Starting date enrichment (Brave + SEC verification)")
            dates_enriched = await enrich_deal_dates(limit=50)
            logger.info(f"[{job_id}] Date enrichment complete: {dates_enriched} deals updated")

            # Manual heartbeat after Phase 4
            if guard:
                await guard.heartbeat()

            # Send notification
            await send_scrape_summary(
                job_id=job_id,
                results=results,
                duration_seconds=duration,
                external_results=external_results,
            )

            # Track job success
            _last_job_status = "success"
            _last_job_duration = duration
            logger.info(f"[{job_id}] Job completed successfully in {duration:.1f}s")

            # FIX 2026-01: Signal guard that job succeeded (prevents guard from marking as failed)
            if guard:
                guard.set_success()

            # Update ScanJob record with success
            if scan_job_db_id:
                try:
                    # Build fund scraper results dict (same format as external_results)
                    fund_results = {}
                    for r in results:
                        fund_results[r.fund_slug] = {
                            "articles_found": r.articles_found,
                            "articles_received": r.articles_found,  # For consistency
                            "deals_extracted": r.deals_extracted,
                            "deals_saved": r.deals_saved,
                            "articles_skipped_duplicate": r.articles_skipped_duplicate,
                            "errors": len(r.errors),
                            "error_message": r.errors[0] if r.errors else None,
                        }

                    # Merge fund results + external results
                    all_source_results = {**fund_results, **external_results}

                    # Calculate totals from all sources
                    total_articles = fund_articles
                    total_extracted = sum(r.deals_extracted for r in results)
                    total_saved = fund_deals
                    total_duplicates = sum(r.articles_skipped_duplicate for r in results)
                    total_errors = fund_errors

                    for source_name, stats in external_results.items():
                        if source_name.startswith("_") or source_name == "total_deals_saved":
                            continue
                        if isinstance(stats, dict) and "error" not in stats:
                            total_articles += stats.get("articles_received", 0)
                            total_extracted += stats.get("deals_extracted", 0)
                            total_saved += stats.get("deals_saved", 0)
                            total_duplicates += stats.get("articles_skipped_duplicate", 0)
                            total_errors += stats.get("errors", 0)

                    # Query actual deal counts from database (most accurate)
                    async with get_session() as session:
                        from sqlalchemy import select, func, Integer
                        from ..archivist.models import Deal

                        # Count lead deals and enterprise AI deals for this scan
                        count_result = await session.execute(
                            select(
                                func.count(Deal.id).label("total"),
                                func.sum(func.cast(Deal.is_lead_confirmed, Integer)).label("leads"),
                                func.sum(func.cast(Deal.is_enterprise_ai, Integer)).label("enterprise_ai"),
                            ).where(Deal.scan_job_id == scan_job_db_id)
                        )
                        counts = count_result.one()
                        lead_deals = int(counts.leads or 0)
                        enterprise_ai_deals = int(counts.enterprise_ai or 0)
                        actual_total_saved = int(counts.total or 0)

                        # Use actual count from DB (more reliable)
                        if actual_total_saved > 0:
                            total_saved = actual_total_saved

                        # Update the scan job record
                        stmt = select(ScanJob).where(ScanJob.id == scan_job_db_id)
                        result = await session.execute(stmt)
                        scan_job = result.scalar_one_or_none()
                        if scan_job:
                            scan_job.status = "success"
                            scan_job.completed_at = datetime.now(timezone.utc)
                            scan_job.duration_seconds = duration
                            scan_job.total_articles_found = total_articles
                            scan_job.total_deals_extracted = total_extracted
                            scan_job.total_deals_saved = total_saved
                            scan_job.total_duplicates_skipped = total_duplicates
                            scan_job.total_errors = total_errors
                            scan_job.lead_deals_found = lead_deals
                            scan_job.enterprise_ai_deals_found = enterprise_ai_deals
                            scan_job.source_results_json = json.dumps(all_source_results, default=str)
                            await session.commit()
                            logger.info(
                                f"[{job_id}] Updated ScanJob: saved={total_saved}, "
                                f"leads={lead_deals}, enterprise_ai={enterprise_ai_deals}"
                            )

                        # FIX (2026-01): Flush any remaining token usage records
                        await flush_token_usage_batch(force=True)
                except Exception as e:
                    logger.warning(f"[{job_id}] Failed to update ScanJob record: {e}")
                finally:
                    # Clear global scan ID tracker
                    _current_scan_job_id = None

        except Exception as e:
            logger.error(f"[{job_id}] Scrape job failed: {e}", exc_info=True)
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            # Track job failure
            _last_job_status = "failed"
            _last_job_error = str(e)
            _last_job_duration = duration

            # Update ScanJob record with failure
            # Note: ScanJobGuard will also try to mark as failed, but this provides
            # full statistics if the main pool is still available
            if scan_job_db_id:
                try:
                    async with get_session() as session:
                        from sqlalchemy import select
                        stmt = select(ScanJob).where(ScanJob.id == scan_job_db_id)
                        result = await session.execute(stmt)
                        scan_job = result.scalar_one_or_none()
                        if scan_job:
                            scan_job.status = "failed"
                            scan_job.completed_at = datetime.now(timezone.utc)
                            scan_job.duration_seconds = duration
                            scan_job.error_message = str(e)[:500]  # Truncate long errors
                            await session.commit()
                            logger.info(f"[{job_id}] Updated ScanJob record with failure")

                        # FIX (2026-01): Flush any remaining token usage records
                        await flush_token_usage_batch(force=True)
                except Exception as update_err:
                    # FIX 2026-01: If main pool fails, ScanJobGuard will handle status update
                    logger.warning(f"[{job_id}] Failed to update ScanJob failure: {update_err}")
                finally:
                    # Clear global scan ID tracker
                    _current_scan_job_id = None

            # Still try to send error notification
            await send_scrape_summary(
                job_id=job_id,
                results=[],
                duration_seconds=duration,
                error=str(e)
            )

            # Re-raise so guard can also mark as failed (backup mechanism)
            raise


def get_scan_schedule() -> tuple[CronTrigger, str]:
    """
    Get the cron trigger based on SCAN_FREQUENCY environment variable.

    Returns:
        tuple: (CronTrigger, description string)

    Supported frequencies:
        - "daily": Once per day at 9am EST
        - "3x_daily": Three times per day at 9am, 1pm, 6pm EST
        - "4_hourly": Every 4 hours (default, 6x/day)
    """
    from ..config.settings import settings

    frequency = settings.scan_frequency.lower().strip()

    if frequency == "daily":
        # Once per day at 9am EST
        return (
            CronTrigger(hour=9, minute=0, timezone="America/New_York"),
            "daily at 9am EST"
        )
    elif frequency == "3x_daily":
        # Three times per day: 9am, 1pm, 6pm EST
        return (
            CronTrigger(hour="9,13,18", minute=0, timezone="America/New_York"),
            "3x daily (9am, 1pm, 6pm EST)"
        )
    elif frequency == "4_hourly":
        # Every 4 hours (original behavior)
        return (
            CronTrigger(hour='*/4', minute=0, timezone="America/New_York"),
            "every 4 hours"
        )
    else:
        # Default to daily if unknown
        logger.warning(f"Unknown SCAN_FREQUENCY '{frequency}', defaulting to daily")
        return (
            CronTrigger(hour=9, minute=0, timezone="America/New_York"),
            "daily at 9am EST (default)"
        )


def setup_scheduler() -> AsyncIOScheduler:
    """
    Initialize and start the APScheduler.

    Configures:
    - Scrape job based on SCAN_FREQUENCY env var (daily, 3x_daily, 4_hourly)
    - Job store in memory (stateless)
    - Timezone: America/New_York (EST/EDT)
    """
    global scheduler

    scheduler = AsyncIOScheduler(
        timezone="America/New_York",
        job_defaults={
            "coalesce": False,  # Queue missed runs instead of dropping them
            "max_instances": 1,  # Only one instance at a time
            "misfire_grace_time": 600,  # 10 min grace for misfires (allows long runs)
        }
    )

    # Get schedule based on SCAN_FREQUENCY setting
    trigger, schedule_desc = get_scan_schedule()

    # Add scrape job with configured frequency
    scheduler.add_job(
        scheduled_scrape_job,
        trigger=trigger,
        id="scheduled_scrape",
        name=f"VC Fund Scrape ({schedule_desc})",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(f"Scheduler started with scan frequency: {schedule_desc}")

    # List scheduled jobs
    for job in scheduler.get_jobs():
        logger.info(f"Scheduled job: {job.id} - next run: {job.next_run_time}")

    return scheduler


def shutdown_scheduler():
    """Shutdown the scheduler without waiting for running jobs.

    FIX 2026-01: Changed from wait=True to wait=False to prevent blocking
    during deployment. Running scans are cancelled via ScanJobGuard signal
    handler before this is called.
    """
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown complete")
        scheduler = None
