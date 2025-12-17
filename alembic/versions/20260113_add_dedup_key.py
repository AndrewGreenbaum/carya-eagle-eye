"""Add dedup_key column to deals table for race condition prevention

Revision ID: 20260113_dedup_key
Revises: 20260107_amount_source
Create Date: 2026-01-13

FIX: Prevents race condition duplicates when processing deals in parallel.

Problem: find_duplicate_deal() runs before transaction commits.
When multiple articles about the same deal arrive simultaneously:
1. Request A checks for duplicates -> none found
2. Request B checks for duplicates -> none found (A hasn't committed)
3. Both requests commit -> duplicates created

Solution: Database-level unique constraint on dedup_key column.
- dedup_key = MD5 hash of normalized_company_name + round_type + date_bucket
- Date bucket = 3-day windows to catch near-duplicates
- INSERT ... ON CONFLICT DO NOTHING prevents concurrent inserts
"""
from alembic import op
import sqlalchemy as sa
import hashlib
import re

# revision identifiers, used by Alembic.
revision = '20260113_dedup_key'
down_revision = '20260107_amount_source'
branch_labels = None
depends_on = None


def normalize_company_name_for_key(name: str) -> str:
    """Normalize company name for dedup key (must match storage.py logic)."""
    name = name.lower().strip()
    if name.startswith("the "):
        name = name[4:]
    suffixes = [
        ", incorporated", " incorporated", ", technologies", " technologies",
        ", corporation", " corporation", ", limited", " limited",
        ", company", " company", ", inc.", " inc.", ", inc", " inc",
        ", llc", " llc", ", ltd.", " ltd.", ", ltd", " ltd",
        ", corp.", " corp.", ", corp", " corp", ", co.", " co.",
        ", co", " co", " labs", " lab", " tech", " ai",
    ]
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    return re.sub(r'[^a-z0-9]', '', name)


def make_dedup_key(company_name: str, round_type: str, announced_date) -> str:
    """Generate dedup key (must match storage.py logic)."""
    from datetime import date

    normalized_name = normalize_company_name_for_key(company_name)

    if announced_date:
        days_since_epoch = (announced_date - date(1970, 1, 1)).days
        date_bucket = days_since_epoch // 3
        date_str = str(date_bucket)
    else:
        # For NULL dates, use a special bucket that won't conflict
        # with dated deals but will catch concurrent NULL-date inserts
        today = date.today()
        week_bucket = (today - date(1970, 1, 1)).days // 7
        date_str = f"nodate_{week_bucket}"

    key_data = f"{normalized_name}|{round_type}|{date_str}"
    return hashlib.md5(key_data.encode()).hexdigest()


def upgrade():
    conn = op.get_bind()

    # Step 1: Add the dedup_key column (nullable initially)
    op.add_column('deals', sa.Column(
        'dedup_key',
        sa.String(32),
        nullable=True
    ))

    # Step 2: Backfill existing deals with their dedup_key
    # Using raw SQL for performance on large tables
    result = conn.execute(sa.text("""
        SELECT d.id, pc.name, d.round_type, d.announced_date
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
        WHERE d.dedup_key IS NULL
    """))

    # Calculate dedup_key for each deal
    deal_keys = []
    for row in result:
        deal_id, company_name, round_type, announced_date = row
        dedup_key = make_dedup_key(company_name, round_type, announced_date)
        deal_keys.append({"id": deal_id, "key": dedup_key})

    # Update deals with their keys
    for item in deal_keys:
        conn.execute(sa.text(
            "UPDATE deals SET dedup_key = :key WHERE id = :id"
        ), item)

    # Step 3: Find and remove duplicates (keep lowest ID = oldest deal)
    # Group by dedup_key and delete all but the minimum ID in each group
    duplicates = conn.execute(sa.text("""
        SELECT dedup_key, array_agg(id ORDER BY id) as ids, count(*) as cnt
        FROM deals
        WHERE dedup_key IS NOT NULL
        GROUP BY dedup_key
        HAVING count(*) > 1
    """))

    total_deleted = 0
    for row in duplicates:
        dedup_key, ids, cnt = row
        # Keep the first ID (lowest), delete the rest
        ids_to_delete = ids[1:]  # All except the first
        if ids_to_delete:
            # First, reassign articles from duplicate deals to the kept deal
            kept_id = ids[0]
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "UPDATE articles SET deal_id = :kept_id WHERE deal_id = :del_id"
                ), {"kept_id": kept_id, "del_id": del_id})

            # Delete deal_investors for duplicates
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "DELETE FROM deal_investors WHERE deal_id = :id"
                ), {"id": del_id})

            # Delete the duplicate deals
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "DELETE FROM deals WHERE id = :id"
                ), {"id": del_id})

            total_deleted += len(ids_to_delete)
            print(f"Removed {len(ids_to_delete)} duplicate(s) for dedup_key={dedup_key[:8]}... (kept deal #{kept_id})")

    if total_deleted > 0:
        print(f"Total duplicates removed: {total_deleted}")

    # Step 4: Create unique index on dedup_key
    # Now that duplicates are cleaned up, this will succeed
    op.create_index(
        'idx_deals_dedup_key',
        'deals',
        ['dedup_key'],
        unique=True
    )


def downgrade():
    op.drop_index('idx_deals_dedup_key', table_name='deals')
    op.drop_column('deals', 'dedup_key')
