#!/usr/bin/env python3
"""
Backfill enterprise_category for deals with NULL category using keyword matching.
No LLM costs - pure keyword-based categorization.

Usage:
    DATABASE_URL=postgresql://... python3 scripts/backfill_enterprise_category.py --dry-run
    DATABASE_URL=postgresql://... python3 scripts/backfill_enterprise_category.py

Or on Railway:
    railway shell
    python3 scripts/backfill_enterprise_category.py --dry-run
"""

import os
import sys
import argparse

# Try to get DATABASE_URL from environment
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Run with: railway shell")
    print("Then: python3 scripts/backfill_enterprise_category.py")
    sys.exit(1)

# Convert to psycopg2 format if needed
if DATABASE_URL.startswith('postgresql+asyncpg://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')

import psycopg2
from psycopg2.extras import RealDictCursor

# Keyword categories: keyword -> category
# Order matters: first match wins, so more specific keywords go first
CATEGORY_KEYWORDS = {
    'crypto': [
        'blockchain', 'web3', 'defi', 'nft', 'cryptocurrency',
        'crypto', 'token', 'dao', 'decentralized',
    ],
    'fintech': [
        'bank', 'neobank', 'payment', 'lending', 'insurance',
        'fintech', 'credit', 'mortgage',
    ],
    'healthcare': [
        'biotech', 'pharma', 'clinical', 'medical', 'health',
        'therapeutic', 'drug', 'genomic',
    ],
    'hardware': [
        'chip', 'semiconductor', 'device', 'sensor', 'robotics',
        'drone', 'manufacturing',
    ],
}


def classify_deal(company_name, article_titles):
    """
    Classify a deal based on keyword matching against company name and article titles.
    Returns the category string or None if no match.
    """
    # Combine all text for matching
    text = company_name.lower()
    if article_titles:
        text += ' ' + ' '.join(t.lower() for t in article_titles if t)

    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return category

    return None


def find_uncategorized_deals(cur):
    """Find deals with NULL enterprise_category, with company name and article titles."""
    cur.execute("""
        SELECT
            d.id,
            pc.name as company_name,
            d.round_type,
            d.amount_usd,
            d.announced_date,
            array_agg(DISTINCT a.title) FILTER (WHERE a.title IS NOT NULL) as article_titles
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
        LEFT JOIN articles a ON a.deal_id = d.id
        WHERE d.enterprise_category IS NULL
        GROUP BY d.id, pc.name, d.round_type, d.amount_usd, d.announced_date
        ORDER BY d.id
    """)
    return cur.fetchall()


def run_backfill(dry_run=False):
    """Run the enterprise_category backfill."""
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    total_updated = 0
    category_counts = {}

    try:
        print("\n=== Finding uncategorized deals ===")
        deals = find_uncategorized_deals(cur)
        print(f"Found {len(deals)} deals with NULL enterprise_category\n")

        for deal in deals:
            deal_id = deal['id']
            company = deal['company_name']
            article_titles = deal['article_titles'] or []

            category = classify_deal(company, article_titles)

            if category:
                amount = deal['amount_usd']
                amount_str = f"${amount / 1_000_000:.1f}M" if amount else "N/A"
                print(f"  #{deal_id}: {company} | {amount_str} -> {category}")

                category_counts[category] = category_counts.get(category, 0) + 1

                if not dry_run:
                    cur.execute(
                        "UPDATE deals SET enterprise_category = %s WHERE id = %s",
                        (category, deal_id)
                    )
                    total_updated += 1

        # === Summary ===
        print(f"\n=== Summary ===")
        matched = sum(category_counts.values())
        unmatched = len(deals) - matched
        print(f"Total uncategorized deals: {len(deals)}")
        print(f"Matched by keywords: {matched}")
        print(f"No match (left as NULL): {unmatched}")

        if category_counts:
            print("\nBreakdown by category:")
            for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
                print(f"  {cat}: {count}")

        if dry_run:
            print(f"\nDRY RUN: Would have updated {matched} deals")
        else:
            print(f"\nUpdated {total_updated} deals")
            conn.commit()
            print("Changes committed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: Backfill failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Backfill enterprise_category for deals with NULL category'
    )
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be updated without actually updating')
    args = parser.parse_args()

    run_backfill(dry_run=args.dry_run)
