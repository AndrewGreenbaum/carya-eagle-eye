"""Regenerate dedup_key and amount_dedup_key with fixed normalization

Revision ID: 20260122_regenerate_dedup_keys
Revises: 20260121_content_hashes
Create Date: 2026-01-22

FIX (2026-01): Fixes Bug 2 in company verification - normalization now strips
ALL matching suffixes instead of just one.

Previous bug: "Acme Labs AI" → "acmelabs" (only stripped " ai")
Fixed: "Acme Labs AI" → "acme" (strips " ai" then " labs")

Also expands suffix list to include missing suffixes from all three locations:
- storage.py's known_company_suffixes (x, go, ly, fy, ify)
- extractor.py's suffixes_to_strip (software, systems, healthcare, bio, etc.)

This migration regenerates all dedup_key and amount_dedup_key values using
the corrected normalization logic. No duplicates should be found since the
new normalization is more aggressive (strips more suffixes), which means
previously distinct keys may now collide.
"""
from alembic import op
import sqlalchemy as sa
import hashlib
import re
from datetime import date

# revision identifiers, used by Alembic.
revision = '20260122_regenerate_dedup_keys'
down_revision = '20260121_content_hashes'
branch_labels = None
depends_on = None


# UPDATED suffix list - matches the consolidated COMPANY_NAME_SUFFIXES in storage.py
COMPANY_NAME_SUFFIXES = [
    # Legal entity types (longer first to avoid partial matches)
    ", incorporated", " incorporated",
    ", technologies", " technologies",
    ", corporation", " corporation",
    ", limited", " limited",
    ", company", " company",
    ", inc.", " inc.",
    ", inc", " inc",
    ", llc", " llc",
    ", ltd.", " ltd.",
    ", ltd", " ltd",
    ", corp.", " corp.",
    ", corp", " corp",
    ", co.", " co.",
    ", co", " co",
    # Common tech company suffixes
    " labs", " lab",
    " tech",
    " ai",
    # Additional suffixes for dedup (from CLAUDE.md)
    " health", " cloud", " ml", " ops", " dev", " hq", " app", " io",
    # FIX (2026-01): Added from extractor.py's suffixes_to_strip
    " software", " systems", " healthcare", " bio", " therapeutics",
    " sciences", " data", " analytics", " intelligence", " platform",
    # FIX (2026-01): Added startup naming patterns from known_company_suffixes
    " x", " go", " ly", " fy", " ify",
]


def normalize_company_name_for_key(name: str) -> str:
    """
    Normalize company name for dedup key (FIXED version).

    FIX: Now strips ALL matching suffixes, not just one.
    """
    name = name.lower().strip()
    if name.startswith("the "):
        name = name[4:]

    # FIX: Strip ALL matching suffixes (not just one)
    # Loop until no more suffixes match
    changed = True
    while changed:
        changed = False
        for suffix in COMPANY_NAME_SUFFIXES:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                changed = True
                break  # Restart loop to check for more suffixes

    return re.sub(r'[^a-z0-9]', '', name)


def make_dedup_key(company_name: str, round_type: str, announced_date) -> str:
    """Generate dedup key with FIXED normalization."""
    normalized_name = normalize_company_name_for_key(company_name)

    if announced_date:
        days_since_epoch = (announced_date - date(1970, 1, 1)).days
        date_bucket = days_since_epoch // 3
        date_str = str(date_bucket)
    else:
        today = date.today()
        week_bucket = (today - date(1970, 1, 1)).days // 7
        date_str = f"nodate_{week_bucket}"

    key_data = f"{normalized_name}|{round_type}|{date_str}"
    return hashlib.md5(key_data.encode()).hexdigest()


def make_amount_dedup_key(company_name: str, amount_usd: int, announced_date) -> str:
    """Generate amount-based dedup key with FIXED normalization."""
    normalized_name = normalize_company_name_for_key(company_name)

    # Calculate amount bucket (logarithmic buckets) - matches storage.py
    # IMPROVED (2026-01): Lowered threshold from $1M to $250K for early-stage
    if amount_usd < 1_000_000:  # $250K-$1M (early-stage)
        amount_bucket = amount_usd // 250_000  # $250K increments
    elif amount_usd < 10_000_000:  # $1M-$10M
        amount_bucket = 4 + (amount_usd // 2_000_000)  # $2M increments, offset by 4
    elif amount_usd < 100_000_000:  # $10M-$100M
        amount_bucket = 9 + (amount_usd // 20_000_000)  # $20M increments, offset by 9
    elif amount_usd < 1_000_000_000:  # $100M-$1B
        amount_bucket = 14 + (amount_usd // 100_000_000)  # $100M increments, offset by 14
    else:  # >$1B
        amount_bucket = 24 + (amount_usd // 500_000_000)  # $500M increments, offset by 24

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

    print("Regenerating dedup keys with fixed normalization (strips ALL suffixes)...")

    # Step 1: Fetch all deals
    result = conn.execute(sa.text("""
        SELECT d.id, pc.name, d.round_type, d.announced_date, d.amount_usd,
               d.dedup_key as old_dedup_key, d.amount_dedup_key as old_amount_dedup_key
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
    """))

    deals = list(result)
    print(f"Processing {len(deals)} deals...")

    # Step 2: Calculate new keys for all deals
    dedup_key_updates = []
    amount_key_updates = []
    keys_changed = 0
    amount_keys_changed = 0

    for row in deals:
        deal_id, company_name, round_type, announced_date, amount_usd, old_key, old_amt_key = row

        # Calculate new dedup_key
        new_dedup_key = make_dedup_key(company_name, round_type, announced_date)
        if new_dedup_key != old_key:
            dedup_key_updates.append({"id": deal_id, "key": new_dedup_key})
            keys_changed += 1

        # Calculate new amount_dedup_key (if amount >= $250K)
        if amount_usd and amount_usd >= 250_000:
            new_amount_key = make_amount_dedup_key(company_name, amount_usd, announced_date)
            if new_amount_key != old_amt_key:
                amount_key_updates.append({"id": deal_id, "key": new_amount_key})
                amount_keys_changed += 1

    print(f"  {keys_changed} dedup_key values will change")
    print(f"  {amount_keys_changed} amount_dedup_key values will change")

    # Step 3: Drop unique constraint temporarily to allow updates
    # (some keys might temporarily collide during update)
    op.drop_index('idx_deals_dedup_key', table_name='deals')

    # Step 4: Update all changed dedup_keys
    for item in dedup_key_updates:
        conn.execute(sa.text(
            "UPDATE deals SET dedup_key = :key WHERE id = :id"
        ), item)

    # Step 5: Update all changed amount_dedup_keys
    for item in amount_key_updates:
        conn.execute(sa.text(
            "UPDATE deals SET amount_dedup_key = :key WHERE id = :id"
        ), item)

    # Step 6: Check for new duplicates created by the more aggressive normalization
    # For example, "Acme Labs" and "Acme Labs AI" might now have the same key
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
        # Keep the first ID (lowest = oldest), delete the rest
        kept_id = ids[0]
        ids_to_delete = ids[1:]

        if ids_to_delete:
            print(f"Found {cnt} duplicates with dedup_key={dedup_key[:8]}... (keeping #{kept_id})")

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
        print(f"Removed {total_deleted} duplicates discovered by new normalization")
    else:
        print("No new duplicates found")

    # Step 7: Recreate unique index
    op.create_index(
        'idx_deals_dedup_key',
        'deals',
        ['dedup_key'],
        unique=True
    )

    print("Dedup key regeneration complete!")


def downgrade():
    # Cannot reliably restore old keys - the old normalization logic was buggy
    # and we don't want to reintroduce that bug
    print("WARNING: Downgrade does not restore old dedup keys (they were buggy)")
    pass
