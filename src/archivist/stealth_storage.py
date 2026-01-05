"""
Storage functions for stealth signals (pre-funding detection).

CRUD operations for the stealth_signals table:
- save_stealth_signal: Upsert a new signal
- get_stealth_signals: List signals with filters
- dismiss_signal: Hide a signal from the list
- link_to_deal: Connect a signal to a deal when company raises
- get_stealth_stats: Aggregate stats by source
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from sqlalchemy import select, func, update, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import StealthSignal, Deal

logger = logging.getLogger(__name__)


async def save_stealth_signal(
    session: AsyncSession,
    company_name: str,
    source: str,
    source_url: str,
    score: int,
    signals: Dict[str, Any],
    metadata_json: Optional[Dict[str, Any]] = None,
) -> Optional[StealthSignal]:
    """
    Save or update a stealth signal.

    Uses upsert (INSERT ON CONFLICT UPDATE) to handle duplicates by company+source.
    If signal already exists, updates score/signals/metadata if new score is higher.

    Args:
        session: Database session
        company_name: Company name (used for dedup)
        source: Source scraper (hackernews, ycombinator, github, linkedin, delaware)
        source_url: URL to the source
        score: Certainty score 0-100
        signals: Dict of signals that contributed to score
        metadata_json: Source-specific metadata (stars, upvotes, batch, etc.)

    Returns:
        StealthSignal if saved/updated, None if skipped
    """
    if not company_name or not source:
        logger.warning(f"Skipping signal with missing company_name or source")
        return None

    if score < 0 or score > 100:
        logger.warning(f"Invalid score {score} for {company_name}, clamping to 0-100")
        score = max(0, min(100, score))

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Use PostgreSQL upsert
    stmt = pg_insert(StealthSignal).values(
        company_name=company_name,
        source=source,
        source_url=source_url,
        score=score,
        signals=signals or {},
        metadata_json=metadata_json or {},
        spotted_at=now,
        dismissed=False,
        created_at=now,
    )

    # On conflict (company_name, source), always update with latest data
    # Use GREATEST to keep the highest score seen
    stmt = stmt.on_conflict_do_update(
        index_elements=['company_name', 'source'],
        set_={
            'score': func.greatest(StealthSignal.score, stmt.excluded.score),
            'signals': stmt.excluded.signals,
            'metadata_json': stmt.excluded.metadata_json,
            'source_url': stmt.excluded.source_url,
            'spotted_at': stmt.excluded.spotted_at,
        },
    )

    try:
        await session.execute(stmt)
        # Return a minimal object to indicate success (avoid extra query)
        # The caller only needs to know if it succeeded
        return StealthSignal(
            company_name=company_name,
            source=source,
            source_url=source_url,
            score=score,
            signals=signals or {},
            metadata_json=metadata_json or {},
        )
    except Exception as e:
        logger.error(f"Error saving stealth signal for {company_name}: {e}")
        return None


async def get_stealth_signals(
    session: AsyncSession,
    source: Optional[str] = None,
    min_score: int = 0,
    include_dismissed: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> tuple[List[StealthSignal], int]:
    """
    Get stealth signals with filtering and pagination.

    Args:
        session: Database session
        source: Filter by source (None = all sources)
        min_score: Minimum score threshold (default 0)
        include_dismissed: Include dismissed signals (default False)
        limit: Max results to return
        offset: Pagination offset

    Returns:
        Tuple of (signals list, total count)
    """
    # Build base query
    conditions = []
    if source:
        conditions.append(StealthSignal.source == source)
    if min_score > 0:
        conditions.append(StealthSignal.score >= min_score)
    if not include_dismissed:
        conditions.append(StealthSignal.dismissed == False)

    # Count total
    count_stmt = select(func.count(StealthSignal.id))
    if conditions:
        count_stmt = count_stmt.where(and_(*conditions))
    count_result = await session.execute(count_stmt)
    total = count_result.scalar() or 0

    # Fetch signals
    stmt = select(StealthSignal)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(StealthSignal.score.desc(), StealthSignal.spotted_at.desc())
    stmt = stmt.limit(limit).offset(offset)

    result = await session.execute(stmt)
    signals = list(result.scalars().all())

    return signals, total


async def dismiss_signal(
    session: AsyncSession,
    signal_id: int,
) -> bool:
    """
    Dismiss a stealth signal (hide from list).

    Args:
        session: Database session
        signal_id: ID of signal to dismiss

    Returns:
        True if dismissed, False if not found
    """
    stmt = (
        update(StealthSignal)
        .where(StealthSignal.id == signal_id)
        .values(dismissed=True)
    )
    result = await session.execute(stmt)
    return result.rowcount > 0


async def undismiss_signal(
    session: AsyncSession,
    signal_id: int,
) -> bool:
    """
    Undismiss a stealth signal (show in list again).

    Args:
        session: Database session
        signal_id: ID of signal to undismiss

    Returns:
        True if undismissed, False if not found
    """
    stmt = (
        update(StealthSignal)
        .where(StealthSignal.id == signal_id)
        .values(dismissed=False)
    )
    result = await session.execute(stmt)
    return result.rowcount > 0


async def link_to_deal(
    session: AsyncSession,
    signal_id: int,
    deal_id: int,
) -> bool:
    """
    Link a stealth signal to a deal (when company raises funding).

    Validates that the deal exists before linking.

    Args:
        session: Database session
        signal_id: ID of stealth signal
        deal_id: ID of deal to link to

    Returns:
        True if linked, False if signal or deal not found
    """
    # Verify deal exists
    deal_result = await session.execute(
        select(Deal.id).where(Deal.id == deal_id)
    )
    if not deal_result.scalar_one_or_none():
        logger.warning(f"Cannot link signal {signal_id} to non-existent deal {deal_id}")
        return False

    stmt = (
        update(StealthSignal)
        .where(StealthSignal.id == signal_id)
        .values(converted_deal_id=deal_id)
    )
    result = await session.execute(stmt)
    return result.rowcount > 0


async def get_stealth_stats(
    session: AsyncSession,
    include_dismissed: bool = False,
) -> Dict[str, Any]:
    """
    Get aggregate statistics for stealth signals.

    Returns:
        Dict with total count, counts by source, average score
    """
    conditions = []
    if not include_dismissed:
        conditions.append(StealthSignal.dismissed == False)

    # Total count
    count_stmt = select(func.count(StealthSignal.id))
    if conditions:
        count_stmt = count_stmt.where(and_(*conditions))
    count_result = await session.execute(count_stmt)
    total = count_result.scalar() or 0

    # Average score
    avg_stmt = select(func.avg(StealthSignal.score))
    if conditions:
        avg_stmt = avg_stmt.where(and_(*conditions))
    avg_result = await session.execute(avg_stmt)
    avg_score = avg_result.scalar() or 0

    # Count by source
    by_source_stmt = select(
        StealthSignal.source,
        func.count(StealthSignal.id).label('count')
    ).group_by(StealthSignal.source)
    if conditions:
        by_source_stmt = by_source_stmt.where(and_(*conditions))
    by_source_result = await session.execute(by_source_stmt)
    by_source = {row.source: row.count for row in by_source_result}

    # Converted count (linked to deals)
    converted_stmt = select(func.count(StealthSignal.id)).where(
        StealthSignal.converted_deal_id.isnot(None)
    )
    if conditions:
        converted_stmt = converted_stmt.where(and_(*conditions))
    converted_result = await session.execute(converted_stmt)
    converted = converted_result.scalar() or 0

    return {
        'total': total,
        'by_source': by_source,
        'avg_score': round(float(avg_score), 1),
        'converted': converted,
    }


async def get_signal_by_id(
    session: AsyncSession,
    signal_id: int,
) -> Optional[StealthSignal]:
    """
    Get a single stealth signal by ID.

    Args:
        session: Database session
        signal_id: Signal ID

    Returns:
        StealthSignal or None if not found
    """
    result = await session.execute(
        select(StealthSignal).where(StealthSignal.id == signal_id)
    )
    return result.scalar_one_or_none()
