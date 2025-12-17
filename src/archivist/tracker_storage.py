"""
Storage functions for CRM tracker items and configurable columns.

Handles CRUD operations for the Kanban-style deal pipeline tracker.
"""

import logging
import re
from datetime import datetime, date, timezone
from typing import Optional, List, Set

from sqlalchemy import select, update, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from .models import TrackerItem, TrackerColumn, Deal, PortfolioCompany

logger = logging.getLogger(__name__)


# Available colors for columns (Tailwind color names)
AVAILABLE_COLORS = {"slate", "blue", "amber", "emerald", "green", "red", "purple", "pink", "cyan", "orange"}


# ============================================
# Column Management Functions
# ============================================

async def get_valid_statuses(session: AsyncSession) -> Set[str]:
    """Get all valid status values from active tracker columns."""
    stmt = select(TrackerColumn.slug).where(TrackerColumn.is_active == True)
    result = await session.execute(stmt)
    return set(result.scalars().all())


async def get_tracker_columns(
    session: AsyncSession,
    include_inactive: bool = False,
) -> List[TrackerColumn]:
    """
    Get all tracker columns ordered by position.

    Args:
        session: Database session
        include_inactive: Include soft-deleted columns

    Returns:
        List of TrackerColumn records
    """
    stmt = select(TrackerColumn).order_by(TrackerColumn.position)
    if not include_inactive:
        stmt = stmt.where(TrackerColumn.is_active == True)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_tracker_column(
    session: AsyncSession,
    column_id: int,
) -> Optional[TrackerColumn]:
    """Get a single tracker column by ID."""
    stmt = select(TrackerColumn).where(TrackerColumn.id == column_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_tracker_column_by_slug(
    session: AsyncSession,
    slug: str,
) -> Optional[TrackerColumn]:
    """Get a tracker column by slug."""
    stmt = select(TrackerColumn).where(TrackerColumn.slug == slug)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


def slugify(text: str) -> str:
    """Convert text to a valid slug (lowercase, underscores, alphanumeric)."""
    # Convert to lowercase and replace spaces/hyphens with underscores
    slug = text.lower().strip()
    slug = re.sub(r'[\s\-]+', '_', slug)
    # Remove non-alphanumeric characters except underscores
    slug = re.sub(r'[^a-z0-9_]', '', slug)
    # Remove leading/trailing underscores
    slug = slug.strip('_')
    return slug or 'column'


async def generate_unique_slug(
    session: AsyncSession,
    base_slug: str,
) -> str:
    """Generate a unique slug by appending a number if needed."""
    slug = base_slug
    counter = 1
    while await get_tracker_column_by_slug(session, slug):
        slug = f"{base_slug}_{counter}"
        counter += 1
    return slug


async def create_tracker_column(
    session: AsyncSession,
    display_name: str,
    color: str = "slate",
    slug: Optional[str] = None,
) -> TrackerColumn:
    """
    Create a new tracker column.

    Args:
        session: Database session
        display_name: User-visible column name
        color: Tailwind color name
        slug: Optional custom slug (auto-generated from display_name if not provided)

    Returns:
        Created TrackerColumn
    """
    # Generate slug if not provided
    if not slug:
        base_slug = slugify(display_name)
        slug = await generate_unique_slug(session, base_slug)

    # Validate color
    if color not in AVAILABLE_COLORS:
        color = "slate"

    # Get max position to append at end
    stmt = select(func.max(TrackerColumn.position))
    result = await session.execute(stmt)
    max_pos = result.scalar_one_or_none() or -1

    column = TrackerColumn(
        slug=slug,
        display_name=display_name,
        color=color,
        position=max_pos + 1,
        is_active=True,
    )
    session.add(column)
    await session.flush()

    logger.info(f"Created tracker column: {display_name} ({slug}) at position {column.position}")
    return column


async def update_tracker_column(
    session: AsyncSession,
    column_id: int,
    display_name: Optional[str] = None,
    color: Optional[str] = None,
) -> Optional[TrackerColumn]:
    """
    Update a tracker column's display name or color.

    Args:
        session: Database session
        column_id: Column ID to update
        display_name: New display name (optional)
        color: New color (optional)

    Returns:
        Updated TrackerColumn or None if not found
    """
    column = await get_tracker_column(session, column_id)
    if not column:
        logger.warning(f"Tracker column not found: {column_id}")
        return None

    if display_name is not None:
        column.display_name = display_name

    if color is not None and color in AVAILABLE_COLORS:
        column.color = color

    column.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await session.flush()

    logger.info(f"Updated tracker column: {column_id}")
    return column


async def move_tracker_column(
    session: AsyncSession,
    column_id: int,
    new_position: int,
) -> Optional[TrackerColumn]:
    """
    Move a tracker column to a new position.

    Args:
        session: Database session
        column_id: Column ID to move
        new_position: Target position (0-indexed)

    Returns:
        Moved TrackerColumn or None if not found
    """
    column = await get_tracker_column(session, column_id)
    if not column:
        logger.warning(f"Tracker column not found: {column_id}")
        return None

    old_position = column.position

    if old_position == new_position:
        return column

    # Clamp new_position to valid range
    stmt = select(func.count(TrackerColumn.id)).where(TrackerColumn.is_active == True)
    result = await session.execute(stmt)
    max_pos = (result.scalar_one() or 1) - 1
    new_position = max(0, min(new_position, max_pos))

    if old_position < new_position:
        # Moving right: shift items between old and new positions left
        await session.execute(
            update(TrackerColumn)
            .where(TrackerColumn.is_active == True)
            .where(TrackerColumn.position > old_position)
            .where(TrackerColumn.position <= new_position)
            .values(position=TrackerColumn.position - 1)
        )
    else:
        # Moving left: shift items between new and old positions right
        await session.execute(
            update(TrackerColumn)
            .where(TrackerColumn.is_active == True)
            .where(TrackerColumn.position >= new_position)
            .where(TrackerColumn.position < old_position)
            .values(position=TrackerColumn.position + 1)
        )

    column.position = new_position
    column.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await session.flush()

    logger.info(f"Moved tracker column {column_id}: {old_position} -> {new_position}")
    return column


async def delete_tracker_column(
    session: AsyncSession,
    column_id: int,
) -> bool:
    """
    Delete a tracker column and move its items to the first column.

    Args:
        session: Database session
        column_id: Column ID to delete

    Returns:
        True if deleted, False if not found or is the last column
    """
    column = await get_tracker_column(session, column_id)
    if not column:
        logger.warning(f"Tracker column not found: {column_id}")
        return False

    # Check if this is the last column
    stmt = select(func.count(TrackerColumn.id)).where(TrackerColumn.is_active == True)
    result = await session.execute(stmt)
    count = result.scalar_one() or 0
    if count <= 1:
        logger.warning("Cannot delete the last tracker column")
        return False

    # Get the first column (lowest position) that isn't this one
    stmt = (
        select(TrackerColumn)
        .where(TrackerColumn.is_active == True)
        .where(TrackerColumn.id != column_id)
        .order_by(TrackerColumn.position)
        .limit(1)
    )
    result = await session.execute(stmt)
    target_column = result.scalar_one_or_none()

    if not target_column:
        logger.error("No target column found for moving items")
        return False

    # Move all items from deleted column to target column
    await session.execute(
        update(TrackerItem)
        .where(TrackerItem.status == column.slug)
        .values(status=target_column.slug)
    )

    # Close gap in positions
    await session.execute(
        update(TrackerColumn)
        .where(TrackerColumn.is_active == True)
        .where(TrackerColumn.position > column.position)
        .values(position=TrackerColumn.position - 1)
    )

    # Soft delete the column
    column.is_active = False
    column.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await session.flush()

    logger.info(f"Deleted tracker column: {column.slug}, items moved to {target_column.slug}")
    return True


async def get_column_item_counts(
    session: AsyncSession,
) -> dict:
    """Get count of items per column slug."""
    columns = await get_tracker_columns(session)
    counts = {}
    for col in columns:
        stmt = select(func.count(TrackerItem.id)).where(TrackerItem.status == col.slug)
        result = await session.execute(stmt)
        counts[col.slug] = result.scalar_one() or 0
    return counts


# ============================================
# Tracker Item Functions
# ============================================


async def get_tracker_items(
    session: AsyncSession,
    status: Optional[str] = None,
) -> List[TrackerItem]:
    """
    Get all tracker items, optionally filtered by status.

    Items are ordered by status (for grouping) then by position (for ordering within column).

    Args:
        session: Database session
        status: Optional status filter (any valid column slug)

    Returns:
        List of TrackerItem records
    """
    stmt = select(TrackerItem).order_by(TrackerItem.status, TrackerItem.position)

    if status:
        valid_statuses = await get_valid_statuses(session)
        if status not in valid_statuses:
            logger.warning(f"Invalid status filter: {status}")
            return []
        stmt = stmt.where(TrackerItem.status == status)

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_tracker_item(
    session: AsyncSession,
    item_id: int,
) -> Optional[TrackerItem]:
    """
    Get a single tracker item by ID.

    Args:
        session: Database session
        item_id: The tracker item ID

    Returns:
        TrackerItem if found, None otherwise
    """
    stmt = select(TrackerItem).where(TrackerItem.id == item_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def create_tracker_item(
    session: AsyncSession,
    company_name: str,
    status: str = "watching",
    round_type: Optional[str] = None,
    amount: Optional[str] = None,
    lead_investor: Optional[str] = None,
    website: Optional[str] = None,
    notes: Optional[str] = None,
    deal_id: Optional[int] = None,
) -> TrackerItem:
    """
    Create a new tracker item.

    Automatically assigns position at the end of the target column.

    Args:
        session: Database session
        company_name: Name of the company
        status: Initial Kanban column (default: first active column)
        round_type: Funding round type (seed, series_a, etc.)
        amount: Funding amount ("$50M")
        lead_investor: Lead investor name
        website: Company website URL
        notes: User notes
        deal_id: Optional link to existing Deal record

    Returns:
        Created TrackerItem
    """
    valid_statuses = await get_valid_statuses(session)
    if status not in valid_statuses:
        # Default to first column if invalid status
        columns = await get_tracker_columns(session)
        if columns:
            status = columns[0].slug
            logger.warning(f"Invalid status, defaulting to '{status}'")
        else:
            status = "watching"
            logger.warning("No columns found, defaulting to 'watching'")

    # Get max position in target column to append at end
    stmt = (
        select(func.max(TrackerItem.position))
        .where(TrackerItem.status == status)
    )
    result = await session.execute(stmt)
    max_pos = result.scalar_one_or_none() or -1

    item = TrackerItem(
        company_name=company_name,
        status=status,
        round_type=round_type,
        amount=amount,
        lead_investor=lead_investor,
        website=website,
        notes=notes,
        deal_id=deal_id,
        position=max_pos + 1,
    )
    session.add(item)
    await session.flush()

    logger.info(f"Created tracker item: {company_name} in '{status}' at position {item.position}")
    return item


async def bulk_create_tracker_items(
    session: AsyncSession,
    company_names: list[str],
    status: str = "watching",
) -> list[TrackerItem]:
    """
    Create multiple tracker items at once.

    All items are added to the same status column (default: watching).
    Skips empty names and duplicates within the batch.

    Args:
        session: Database session
        company_names: List of company names to add
        status: Initial Kanban column (default: watching)

    Returns:
        List of created TrackerItem objects
    """
    # Validate status
    valid_statuses = await get_valid_statuses(session)
    if status not in valid_statuses:
        columns = await get_tracker_columns(session)
        if columns:
            status = columns[0].slug
            logger.warning(f"Invalid status for bulk create, defaulting to '{status}'")
        else:
            status = "watching"

    # Clean and dedupe names
    seen = set()
    cleaned_names = []
    for name in company_names:
        clean_name = name.strip()
        if clean_name and clean_name.lower() not in seen:
            seen.add(clean_name.lower())
            cleaned_names.append(clean_name)

    if not cleaned_names:
        return []

    # Get current max position in target column
    stmt = (
        select(func.max(TrackerItem.position))
        .where(TrackerItem.status == status)
    )
    result = await session.execute(stmt)
    max_pos = result.scalar_one_or_none() or -1

    # Create all items
    created_items = []
    for i, name in enumerate(cleaned_names):
        item = TrackerItem(
            company_name=name,
            status=status,
            position=max_pos + 1 + i,
        )
        session.add(item)
        created_items.append(item)

    await session.flush()

    logger.info(f"Bulk created {len(created_items)} tracker items in '{status}'")
    return created_items


async def update_tracker_item(
    session: AsyncSession,
    item_id: int,
    **kwargs,
) -> Optional[TrackerItem]:
    """
    Update a tracker item's fields.

    Does NOT handle status changes (use move_tracker_item for that).

    Args:
        session: Database session
        item_id: The tracker item ID
        **kwargs: Fields to update (company_name, notes, last_contact_date, next_step, etc.)

    Returns:
        Updated TrackerItem if found, None otherwise
    """
    stmt = select(TrackerItem).where(TrackerItem.id == item_id)
    result = await session.execute(stmt)
    item = result.scalar_one_or_none()

    if not item:
        logger.warning(f"Tracker item not found: {item_id}")
        return None

    # Update allowed fields
    allowed_fields = {
        "company_name", "round_type", "amount", "lead_investor",
        "website", "notes", "last_contact_date", "next_step"
    }

    for key, value in kwargs.items():
        if key in allowed_fields and hasattr(item, key):
            setattr(item, key, value)

    item.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await session.flush()

    logger.info(f"Updated tracker item: {item_id}")
    return item


async def move_tracker_item(
    session: AsyncSession,
    item_id: int,
    new_status: str,
    new_position: int,
) -> Optional[TrackerItem]:
    """
    Move a tracker item to a new status/position (for drag-drop).

    Handles reordering of other items in both source and target columns.

    Args:
        session: Database session
        item_id: The tracker item ID
        new_status: Target Kanban column (column slug)
        new_position: Target position within column (0-indexed)

    Returns:
        Moved TrackerItem if found, None otherwise
    """
    valid_statuses = await get_valid_statuses(session)
    if new_status not in valid_statuses:
        logger.warning(f"Invalid status: {new_status}")
        return None

    stmt = select(TrackerItem).where(TrackerItem.id == item_id)
    result = await session.execute(stmt)
    item = result.scalar_one_or_none()

    if not item:
        logger.warning(f"Tracker item not found: {item_id}")
        return None

    old_status = item.status
    old_position = item.position

    # If moving within same column
    if old_status == new_status:
        if old_position == new_position:
            # No change needed
            return item

        if old_position < new_position:
            # Moving down: shift items between old and new positions up
            await session.execute(
                update(TrackerItem)
                .where(TrackerItem.status == old_status)
                .where(TrackerItem.position > old_position)
                .where(TrackerItem.position <= new_position)
                .values(position=TrackerItem.position - 1)
            )
        else:
            # Moving up: shift items between new and old positions down
            await session.execute(
                update(TrackerItem)
                .where(TrackerItem.status == old_status)
                .where(TrackerItem.position >= new_position)
                .where(TrackerItem.position < old_position)
                .values(position=TrackerItem.position + 1)
            )
    else:
        # Moving to different column

        # Close gap in old column
        await session.execute(
            update(TrackerItem)
            .where(TrackerItem.status == old_status)
            .where(TrackerItem.position > old_position)
            .values(position=TrackerItem.position - 1)
        )

        # Make room in new column
        await session.execute(
            update(TrackerItem)
            .where(TrackerItem.status == new_status)
            .where(TrackerItem.position >= new_position)
            .values(position=TrackerItem.position + 1)
        )

    # Update the item itself
    item.status = new_status
    item.position = new_position
    item.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

    await session.flush()

    logger.info(
        f"Moved tracker item {item_id}: '{old_status}'[{old_position}] -> '{new_status}'[{new_position}]"
    )
    return item


async def delete_tracker_item(
    session: AsyncSession,
    item_id: int,
) -> bool:
    """
    Delete a tracker item.

    Reorders remaining items in the column to close the gap.

    Args:
        session: Database session
        item_id: The tracker item ID

    Returns:
        True if deleted, False if not found
    """
    stmt = select(TrackerItem).where(TrackerItem.id == item_id)
    result = await session.execute(stmt)
    item = result.scalar_one_or_none()

    if not item:
        logger.warning(f"Tracker item not found for deletion: {item_id}")
        return False

    status = item.status
    position = item.position

    # Delete the item
    await session.delete(item)

    # Close gap in column
    await session.execute(
        update(TrackerItem)
        .where(TrackerItem.status == status)
        .where(TrackerItem.position > position)
        .values(position=TrackerItem.position - 1)
    )

    await session.flush()

    logger.info(f"Deleted tracker item: {item_id}")
    return True


async def create_tracker_from_deal(
    session: AsyncSession,
    deal_id: int,
    status: str = "watching",
) -> Optional[TrackerItem]:
    """
    Create a tracker item from an existing deal.

    Copies deal info (company name, round, amount, lead investor, website)
    and links the tracker item to the deal.

    Args:
        session: Database session
        deal_id: The Deal ID to create tracker from
        status: Initial Kanban column (default: watching)

    Returns:
        Created TrackerItem, or None if:
        - Deal not found
        - Deal already tracked (deal_id already in tracker_items)
    """
    # Check if deal is already tracked
    existing_stmt = select(TrackerItem).where(TrackerItem.deal_id == deal_id)
    existing_result = await session.execute(existing_stmt)
    if existing_result.scalar_one_or_none():
        logger.warning(f"Deal {deal_id} is already tracked")
        return None

    # Get deal with company info
    deal_stmt = select(Deal).where(Deal.id == deal_id)
    deal_result = await session.execute(deal_stmt)
    deal = deal_result.scalar_one_or_none()

    if not deal:
        logger.warning(f"Deal not found: {deal_id}")
        return None

    # Get company name
    company_stmt = select(PortfolioCompany).where(PortfolioCompany.id == deal.company_id)
    company_result = await session.execute(company_stmt)
    company = company_result.scalar_one_or_none()

    if not company:
        logger.warning(f"Company not found for deal: {deal_id}")
        return None

    # Create tracker item with deal info
    return await create_tracker_item(
        session=session,
        company_name=company.name,
        status=status,
        round_type=deal.round_type,
        amount=deal.amount,
        lead_investor=deal.lead_partner_name,
        website=company.website,
        deal_id=deal_id,
    )


async def get_tracker_items_count(
    session: AsyncSession,
    status: Optional[str] = None,
) -> int:
    """
    Get count of tracker items, optionally filtered by status.

    Args:
        session: Database session
        status: Optional status filter (column slug)

    Returns:
        Count of matching items
    """
    stmt = select(func.count(TrackerItem.id))

    if status:
        valid_statuses = await get_valid_statuses(session)
        if status not in valid_statuses:
            return 0
        stmt = stmt.where(TrackerItem.status == status)

    result = await session.execute(stmt)
    return result.scalar_one() or 0


async def get_tracker_stats(
    session: AsyncSession,
) -> dict:
    """
    Get statistics for the tracker dashboard.

    Returns:
        Dict with counts per status and total
    """
    stats = {"total": 0}
    columns = await get_tracker_columns(session)

    for column in columns:
        count = await get_tracker_items_count(session, column.slug)
        stats[column.slug] = count
        stats["total"] += count

    return stats
