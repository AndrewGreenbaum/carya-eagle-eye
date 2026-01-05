#!/usr/bin/env python3
"""
Standalone migration script for adding amount_dedup_key column.

This script runs the migration without importing the full application,
which avoids Python version compatibility issues with dependencies.

Usage:
    DATABASE_URL=postgresql://... python3 scripts/run_amount_dedup_migration.py

Or on Railway:
    railway shell
    python3 scripts/run_amount_dedup_migration.py
"""

import os
import sys
import hashlib
import re
from datetime import date

# Try to get DATABASE_URL from environment
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Run with: railway shell")
    print("Then: python3 scripts/run_amount_dedup_migration.py")
    sys.exit(1)

# Convert to psycopg2 format if needed
if DATABASE_URL.startswith('postgresql+asyncpg://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')

import psycopg2
from psycopg2.extras import RealDictCursor


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


def run_migration():
    """Run the amount_dedup_key migration."""
    print(f"Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Check if column already exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'deals' AND column_name = 'amount_dedup_key'
        """)
        if cur.fetchone():
            print("Column 'amount_dedup_key' already exists. Checking for duplicates...")
        else:
            # Step 1: Add the amount_dedup_key column
            print("Step 1: Adding amount_dedup_key column...")
            cur.execute("""
                ALTER TABLE deals ADD COLUMN amount_dedup_key VARCHAR(32)
            """)
            print("  Column added.")

        # Step 2: Backfill existing deals with their amount_dedup_key
        print("Step 2: Backfilling existing deals...")
        cur.execute("""
            SELECT d.id, pc.name, d.amount_usd, d.announced_date
            FROM deals d
            JOIN portfolio_companies pc ON d.company_id = pc.id
            WHERE d.amount_usd IS NOT NULL AND d.amount_usd >= 1000000
            AND d.amount_dedup_key IS NULL
        """)

        deals = cur.fetchall()
        print(f"  Found {len(deals)} deals to backfill")

        for deal in deals:
            amount_dedup_key = make_amount_dedup_key(
                deal['name'],
                deal['amount_usd'],
                deal['announced_date']
            )
            cur.execute(
                "UPDATE deals SET amount_dedup_key = %s WHERE id = %s",
                (amount_dedup_key, deal['id'])
            )

        print(f"  Backfilled {len(deals)} deals")

        # Step 3: Find and remove duplicates
        print("Step 3: Finding cross-round-type duplicates...")
        cur.execute("""
            SELECT amount_dedup_key,
                   array_agg(id ORDER BY id) as ids,
                   array_agg(round_type ORDER BY id) as round_types,
                   count(*) as cnt
            FROM deals
            WHERE amount_dedup_key IS NOT NULL
            GROUP BY amount_dedup_key
            HAVING count(*) > 1
        """)

        duplicates = cur.fetchall()
        total_deleted = 0

        for dup in duplicates:
            amount_key = dup['amount_dedup_key']
            ids = dup['ids']
            round_types = dup['round_types']

            kept_id = ids[0]
            ids_to_delete = ids[1:]

            print(f"  Found duplicate: {amount_key[:8]}...")
            print(f"    Keeping deal #{kept_id} ({round_types[0]})")
            print(f"    Removing: {list(zip(ids_to_delete, round_types[1:]))}")

            for del_id in ids_to_delete:
                # Reassign articles
                cur.execute(
                    "UPDATE articles SET deal_id = %s WHERE deal_id = %s",
                    (kept_id, del_id)
                )

                # Delete from date_sources
                cur.execute(
                    "DELETE FROM date_sources WHERE deal_id = %s",
                    (del_id,)
                )

                # Delete deal_investors
                cur.execute(
                    "DELETE FROM deal_investors WHERE deal_id = %s",
                    (del_id,)
                )

                # Delete tracker_items
                cur.execute(
                    "DELETE FROM tracker_items WHERE deal_id = %s",
                    (del_id,)
                )

                # Delete the deal
                cur.execute(
                    "DELETE FROM deals WHERE id = %s",
                    (del_id,)
                )

                total_deleted += 1

        if total_deleted > 0:
            print(f"  Total duplicates removed: {total_deleted}")
        else:
            print("  No duplicates found")

        # Step 4: Create index if it doesn't exist
        print("Step 4: Creating index...")
        cur.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'deals' AND indexname = 'idx_deals_amount_dedup_key'
        """)
        if cur.fetchone():
            print("  Index already exists")
        else:
            cur.execute("""
                CREATE INDEX idx_deals_amount_dedup_key ON deals (amount_dedup_key)
            """)
            print("  Index created")

        # Commit all changes
        conn.commit()
        print("\nMigration completed successfully!")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: Migration failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_migration()
