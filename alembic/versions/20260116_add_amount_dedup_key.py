"""Add amount_dedup_key column for cross-round-type duplicate detection

Revision ID: 20260116_amount_dedup_key
Revises: 20260113_dedup_key
Create Date: 2026-01-16

FIX: Parloa duplicate bug - LLM classified same $350M deal as both "growth" and "series_d".

Problem: The existing dedup_key includes round_type, so deals with:
- Same company (Parloa)
- Same amount ($350M)
- Same date (2026-01-15)
- Different round_type (growth vs series_d)
...would have DIFFERENT dedup_keys and bypass the unique constraint.

Solution: Secondary dedup key based on amount instead of round_type.
- amount_dedup_key = MD5(normalized_company_name + amount_bucket + date_bucket)
- Amount bucket uses logarithmic buckets to catch similar amounts
- Catches duplicates where LLM assigns different round_types to same deal
"""
from alembic import op
import sqlalchemy as sa
import hashlib
import re
from datetime import date

# revision identifiers, used by Alembic.
revision = '20260116_amount_dedup_key'
down_revision = '20260113_dedup_key'
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


def make_amount_dedup_key(company_name: str, amount_usd: int, announced_date) -> str:
    """Generate amount-based dedup key (must match storage.py logic)."""
    normalized_name = normalize_company_name_for_key(company_name)

    # Calculate amount bucket (logarithmic buckets)
    if amount_usd < 10_000_000:  # $1M-$10M
        amount_bucket = amount_usd // 2_000_000
    elif amount_usd < 100_000_000:  # $10M-$100M
        amount_bucket = 5 + (amount_usd // 20_000_000)
    elif amount_usd < 1_000_000_000:  # $100M-$1B
        amount_bucket = 10 + (amount_usd // 100_000_000)
    else:  # >$1B
        amount_bucket = 20 + (amount_usd // 500_000_000)

    # Date bucket (same as dedup_key)
    if announced_date:
        days_since_epoch = (announced_date - date(1970, 1, 1)).days
        date_bucket = days_since_epoch // 3
        date_str = str(date_bucket)
    else:
        today = date.today()
        week_bucket = (today - date(1970, 1, 1)).days // 7
        date_str = f"nodate_{week_bucket}"

    key_data = f"{normalized_name}|amt{amount_bucket}|{date_str}"
    return hashlib.md5(key_data.encode()).hexdigest()


def upgrade():
    conn = op.get_bind()

    # Step 1: Add the amount_dedup_key column (nullable - deals without amounts won't have one)
    op.add_column('deals', sa.Column(
        'amount_dedup_key',
        sa.String(32),
        nullable=True
    ))

    # Step 2: Backfill existing deals with their amount_dedup_key
    # Only for deals with amount_usd >= $1M (the key requires meaningful amounts)
    result = conn.execute(sa.text("""
        SELECT d.id, pc.name, d.amount_usd, d.announced_date
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
        WHERE d.amount_usd IS NOT NULL AND d.amount_usd >= 1000000
    """))

    # Calculate amount_dedup_key for each deal
    deal_keys = []
    for row in result:
        deal_id, company_name, amount_usd, announced_date = row
        amount_dedup_key = make_amount_dedup_key(company_name, amount_usd, announced_date)
        deal_keys.append({"id": deal_id, "key": amount_dedup_key})

    # Update deals with their keys
    for item in deal_keys:
        conn.execute(sa.text(
            "UPDATE deals SET amount_dedup_key = :key WHERE id = :id"
        ), item)

    print(f"Backfilled {len(deal_keys)} deals with amount_dedup_key")

    # Step 3: Find duplicates by amount_dedup_key (same company + amount + date, different round)
    # This catches the Parloa case and similar issues
    duplicates = conn.execute(sa.text("""
        SELECT amount_dedup_key, array_agg(id ORDER BY id) as ids,
               array_agg(round_type ORDER BY id) as round_types,
               count(*) as cnt
        FROM deals
        WHERE amount_dedup_key IS NOT NULL
        GROUP BY amount_dedup_key
        HAVING count(*) > 1
    """))

    total_deleted = 0
    for row in duplicates:
        amount_key, ids, round_types, cnt = row
        # Keep the first ID (lowest = oldest), delete the rest
        kept_id = ids[0]
        ids_to_delete = ids[1:]

        if ids_to_delete:
            print(f"Found {cnt} duplicates with same amount_dedup_key={amount_key[:8]}...")
            print(f"  Keeping deal #{kept_id} ({round_types[0]}), removing: {list(zip(ids_to_delete, round_types[1:]))}")

            # Reassign articles from duplicate deals to the kept deal
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "UPDATE articles SET deal_id = :kept_id WHERE deal_id = :del_id"
                ), {"kept_id": kept_id, "del_id": del_id})

            # Delete from date_sources
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "DELETE FROM date_sources WHERE deal_id = :id"
                ), {"id": del_id})

            # Delete deal_investors for duplicates
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "DELETE FROM deal_investors WHERE deal_id = :id"
                ), {"id": del_id})

            # Delete tracker_items referencing duplicates
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "DELETE FROM tracker_items WHERE deal_id = :id"
                ), {"id": del_id})

            # Delete the duplicate deals
            for del_id in ids_to_delete:
                conn.execute(sa.text(
                    "DELETE FROM deals WHERE id = :id"
                ), {"id": del_id})

            total_deleted += len(ids_to_delete)

    if total_deleted > 0:
        print(f"Total cross-round-type duplicates removed: {total_deleted}")
    else:
        print("No cross-round-type duplicates found")

    # Step 4: Create index on amount_dedup_key (not unique - NULLs are allowed and common)
    # The uniqueness is enforced in code with ON CONFLICT handling
    op.create_index(
        'idx_deals_amount_dedup_key',
        'deals',
        ['amount_dedup_key'],
        unique=False
    )


def downgrade():
    op.drop_index('idx_deals_amount_dedup_key', table_name='deals')
    op.drop_column('deals', 'amount_dedup_key')
