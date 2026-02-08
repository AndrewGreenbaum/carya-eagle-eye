#!/usr/bin/env python3
"""
Cleanup script for fund structure deals that shouldn't be in the database.
Finds deals where the company name matches fund/LP/SPV patterns.

Usage:
    DATABASE_URL=postgresql://... python3 scripts/cleanup_fund_structures.py --dry-run
    DATABASE_URL=postgresql://... python3 scripts/cleanup_fund_structures.py

Or on Railway:
    railway shell
    python3 scripts/cleanup_fund_structures.py --dry-run
"""

import os
import sys
import argparse

# Try to get DATABASE_URL from environment
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Run with: railway shell")
    print("Then: python3 scripts/cleanup_fund_structures.py")
    sys.exit(1)

# Convert to psycopg2 format if needed
if DATABASE_URL.startswith('postgresql+asyncpg://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')

import psycopg2
from psycopg2.extras import RealDictCursor


def find_fund_structures(cur):
    """
    Find deals where the company name matches fund/LP/SPV patterns.

    Patterns:
    1. Names ending with "Fund" + optional roman numeral/number + optional LP/LLC
    2. Names ending with ", LP" or ", LLC" or ", LLP" (comma + legal suffix)
    3. Names ending with "LP"/"LLC"/"LLP" (no comma) AND containing fund indicator words
    4. Names matching SPV patterns
    5. Names ending with "Partners" + roman numeral/number
    6. Fund code patterns (XX-1234 Fund)
    7. Names containing fund-type suffixes (Growth Fund, Venture Fund, etc.)
    8. Names ending with "Investors"/"Investment" + LLC/LP/LLP
    """
    cur.execute("""
        SELECT
            d.id,
            pc.name as company_name,
            d.round_type,
            d.amount_usd,
            d.announced_date,
            d.created_at
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
        WHERE
            -- Pattern 1: "Fund" + optional roman numeral/number + optional LP/LLC
            pc.name ~* '\\mfund\\s*(i{1,3}|iv|vi{0,3}|ix|xi{0,3}|xiv|xvi{0,3}|x{1,2}|xv|[0-9]+)?\\s*,?\\s*(lp|llc|llp)?$'
            -- Pattern 2: ends with ", LP" / ", LLC" / ", LLP"
            OR pc.name ~* ',\\s*(lp|llc|llp)$'
            -- Pattern 3: ends with LP/LLC/LLP (no comma) + fund indicator words
            OR (
                pc.name ~* '\\s(llc|lp|llp)$'
                AND pc.name ~* '(ventures|capital|partners|holdings|management|advisors|investments|equity|asset|associates|investors|investment|fund)'
            )
            -- Pattern 4: SPV patterns
            OR pc.name ~* '\\mspv[\\s\\-]?([0-9]+|[ivxlc]+)?\\M'
            -- Pattern 5: "Partners" + roman numeral/number
            OR pc.name ~* '\\mpartners\\s+(i{1,3}|iv|vi{0,3}|ix|xi{0,3}|xiv|xvi{0,3}|x{1,2}|xv|[0-9]+)\\s*$'
            -- Pattern 6: Fund code format (XX-1234 Fund)
            OR (pc.name ~ '^[A-Z]{2,4}-[0-9]{3,}' AND pc.name ~* 'fund')
            -- Pattern 7: Fund-type suffixes
            OR pc.name ~* '\\m(growth|opportunity|credit|venture|emerging)\\s+fund\\M'
        ORDER BY pc.name, d.id
    """)
    return cur.fetchall()


def delete_fund_deal(cur, deal_id, company_name, dry_run=False):
    """Delete a fund structure deal and clean up related records."""
    if dry_run:
        cur.execute("SELECT count(*) FROM articles WHERE deal_id = %s", (deal_id,))
        article_count = cur.fetchone()['count']
        cur.execute("SELECT count(*) FROM deal_investors WHERE deal_id = %s", (deal_id,))
        investor_count = cur.fetchone()['count']
        cur.execute("SELECT count(*) FROM tracker_items WHERE deal_id = %s", (deal_id,))
        tracker_count = cur.fetchone()['count']
        print(f"    Would nullify {article_count} articles, delete {investor_count} investors, {tracker_count} tracker items")
        return

    # Set articles deal_id to NULL (not reassigning to another deal)
    cur.execute(
        "UPDATE articles SET deal_id = NULL WHERE deal_id = %s",
        (deal_id,)
    )
    articles_nulled = cur.rowcount

    # Delete from date_sources
    cur.execute(
        "DELETE FROM date_sources WHERE deal_id = %s",
        (deal_id,)
    )

    # Delete deal_investors
    cur.execute(
        "DELETE FROM deal_investors WHERE deal_id = %s",
        (deal_id,)
    )

    # Delete tracker_items
    cur.execute(
        "DELETE FROM tracker_items WHERE deal_id = %s",
        (deal_id,)
    )

    # Delete the deal
    cur.execute(
        "DELETE FROM deals WHERE id = %s",
        (deal_id,)
    )

    print(f"    Deleted deal #{deal_id}, nullified {articles_nulled} articles")


def run_cleanup(dry_run=False):
    """Run the fund structure cleanup."""
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    total_deleted = 0

    try:
        print("\n=== Finding fund structure deals ===")
        fund_deals = find_fund_structures(cur)

        if fund_deals:
            print(f"Found {len(fund_deals)} fund structure deals:\n")
            for deal in fund_deals:
                deal_id = deal['id']
                company = deal['company_name']
                round_type = deal['round_type'] or 'N/A'
                amount = deal['amount_usd']
                date = deal['announced_date']

                amount_str = f"${amount / 1_000_000:.1f}M" if amount else "N/A"
                date_str = str(date) if date else "N/A"

                print(f"  #{deal_id}: {company} | {round_type} | {amount_str} | {date_str}")

                if dry_run:
                    delete_fund_deal(cur, deal_id, company, dry_run=True)
                else:
                    delete_fund_deal(cur, deal_id, company, dry_run=False)
                    total_deleted += 1
        else:
            print("No fund structure deals found.")

        # === Summary ===
        print(f"\n=== Summary ===")
        if dry_run:
            print(f"DRY RUN: Would have deleted {len(fund_deals)} fund structure deals")
        else:
            print(f"Deleted {total_deleted} fund structure deals")
            conn.commit()
            print("Changes committed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: Cleanup failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cleanup fund structure deals from the database')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be deleted without actually deleting')
    args = parser.parse_args()

    run_cleanup(dry_run=args.dry_run)
