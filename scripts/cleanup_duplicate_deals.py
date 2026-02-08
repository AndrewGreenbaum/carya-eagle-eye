#!/usr/bin/env python3
"""
Cleanup script for duplicate deals in the database.

This script finds and removes duplicate deals based on:
1. Same company name + round_type + announced_date + amount (exact duplicates)
2. Same company name + all key fields NULL (incomplete extractions)
3. Same company name + amount + similar date, different round type (round mismatch)
4. Company names differing only by suffix (Inc, LLC, Labs, etc.) + round + date within 30 days
5. Same company + similar amount (+-15%) + date within 30 days (amount-based cross-round)

For each duplicate group, it keeps the oldest deal (lowest ID) and:
- Reassigns articles from duplicates to the kept deal
- Deletes related records (deal_investors, date_sources, tracker_items)
- Deletes the duplicate deals

Usage:
    DATABASE_URL=postgresql://... python3 scripts/cleanup_duplicate_deals.py
    DATABASE_URL=postgresql://... python3 scripts/cleanup_duplicate_deals.py --dry-run

Or on Railway:
    railway shell
    python3 scripts/cleanup_duplicate_deals.py
"""

import os
import sys
import argparse

# Try to get DATABASE_URL from environment
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Run with: railway shell")
    print("Then: python3 scripts/cleanup_duplicate_deals.py")
    sys.exit(1)

# Convert to psycopg2 format if needed
if DATABASE_URL.startswith('postgresql+asyncpg://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')

import psycopg2
from psycopg2.extras import RealDictCursor


def find_exact_duplicates(cur):
    """Find deals with same company + round_type + date."""
    cur.execute("""
        SELECT
            pc.name as company_name,
            d.round_type,
            d.announced_date,
            d.amount_usd,
            array_agg(d.id ORDER BY d.id) as ids,
            count(*) as cnt
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
        WHERE d.announced_date IS NOT NULL
        GROUP BY pc.name, d.round_type, d.announced_date, d.amount_usd
        HAVING count(*) > 1
        ORDER BY pc.name, d.announced_date
    """)
    return cur.fetchall()


def find_null_field_duplicates(cur):
    """Find deals with same company but all key fields NULL (incomplete extractions)."""
    cur.execute("""
        SELECT
            pc.name as company_name,
            array_agg(d.id ORDER BY d.id) as ids,
            count(*) as cnt
        FROM deals d
        JOIN portfolio_companies pc ON d.company_id = pc.id
        WHERE d.round_type IS NULL
          AND d.amount_usd IS NULL
          AND d.announced_date IS NULL
        GROUP BY pc.name
        HAVING count(*) > 1
        ORDER BY pc.name
    """)
    return cur.fetchall()


def find_round_mismatch_duplicates(cur):
    """Find deals with same company + amount + similar date but different round type."""
    cur.execute("""
        WITH deal_info AS (
            SELECT
                d.id,
                pc.name as company_name,
                d.round_type,
                d.announced_date,
                d.amount_usd
            FROM deals d
            JOIN portfolio_companies pc ON d.company_id = pc.id
            WHERE d.amount_usd IS NOT NULL
        )
        SELECT
            d1.company_name,
            d1.amount_usd,
            array_agg(DISTINCT d1.id ORDER BY d1.id) as ids,
            array_agg(DISTINCT d1.round_type) as round_types,
            min(d1.announced_date) as earliest_date
        FROM deal_info d1
        JOIN deal_info d2 ON
            d1.company_name = d2.company_name
            AND d1.amount_usd = d2.amount_usd
            AND d1.id < d2.id
            AND (d1.announced_date IS NULL
                 OR d2.announced_date IS NULL
                 OR ABS(d1.announced_date - d2.announced_date) <= 7)
            AND d1.round_type != d2.round_type
        GROUP BY d1.company_name, d1.amount_usd
        ORDER BY d1.company_name
    """)
    return cur.fetchall()


def find_fuzzy_name_duplicates(cur):
    """Find deals where company names differ only by suffix (Inc, LLC, Labs, etc.)."""
    cur.execute("""
        WITH normalized AS (
            SELECT
                d.id,
                pc.name as company_name,
                d.round_type,
                d.announced_date,
                d.amount_usd,
                d.company_id,
                TRIM(regexp_replace(
                    regexp_replace(
                        pc.name,
                        '\\s*(,?\\s*)?(Inc\\.?|LLC|Ltd\\.?|Corp\\.?|Co\\.?|Labs?|Tech|AI|Health|Cloud|ML|Ops|Dev|HQ|App|IO|Corporation|Incorporated|Limited|Company)\\s*$',
                        '', 'gi'
                    ),
                    '^The\\s+', '', 'i'
                )) as normalized_name
            FROM deals d
            JOIN portfolio_companies pc ON d.company_id = pc.id
        )
        SELECT
            n1.normalized_name,
            array_agg(DISTINCT n1.company_name) as original_names,
            n1.round_type,
            min(n1.announced_date) as earliest_date,
            array_agg(DISTINCT n1.id ORDER BY n1.id) as ids,
            count(DISTINCT n1.id) as cnt
        FROM normalized n1
        JOIN normalized n2 ON
            n1.normalized_name = n2.normalized_name
            AND n1.id < n2.id
            AND n1.company_name != n2.company_name
            AND (n1.round_type = n2.round_type OR n1.round_type IS NULL OR n2.round_type IS NULL)
            AND (n1.announced_date IS NULL
                 OR n2.announced_date IS NULL
                 OR ABS(n1.announced_date - n2.announced_date) <= 30)
        GROUP BY n1.normalized_name, n1.round_type
        HAVING count(DISTINCT n1.id) > 1
        ORDER BY n1.normalized_name
    """)
    return cur.fetchall()


def find_amount_based_duplicates(cur):
    """Find deals with same company + similar amount (+-15%) + date within 30 days."""
    cur.execute("""
        WITH deal_info AS (
            SELECT
                d.id,
                pc.name as company_name,
                d.round_type,
                d.announced_date,
                d.amount_usd,
                d.company_id
            FROM deals d
            JOIN portfolio_companies pc ON d.company_id = pc.id
            WHERE d.amount_usd IS NOT NULL
              AND d.amount_usd > 0
        )
        SELECT
            d1.company_name,
            d1.amount_usd as amount1,
            d2.amount_usd as amount2,
            d1.round_type as round1,
            d2.round_type as round2,
            d1.announced_date as date1,
            d2.announced_date as date2,
            ARRAY[d1.id, d2.id] as ids
        FROM deal_info d1
        JOIN deal_info d2 ON
            d1.company_id = d2.company_id
            AND d1.id < d2.id
            AND d1.amount_usd::float / d2.amount_usd::float BETWEEN 0.85 AND 1.15
            AND (d1.announced_date IS NULL
                 OR d2.announced_date IS NULL
                 OR ABS(d1.announced_date - d2.announced_date) <= 30)
        ORDER BY d1.company_name, d1.id
    """)
    return cur.fetchall()


def delete_duplicate(cur, kept_id, del_id, company_name, dry_run=False):
    """Delete a duplicate deal and reassign its articles."""
    if dry_run:
        # Just count what would be affected
        cur.execute("SELECT count(*) FROM articles WHERE deal_id = %s", (del_id,))
        article_count = cur.fetchone()['count']
        cur.execute("SELECT count(*) FROM deal_investors WHERE deal_id = %s", (del_id,))
        investor_count = cur.fetchone()['count']
        cur.execute("SELECT count(*) FROM tracker_items WHERE deal_id = %s", (del_id,))
        tracker_count = cur.fetchone()['count']
        print(f"    Would reassign {article_count} articles, delete {investor_count} investors, {tracker_count} tracker items")
        return

    # Reassign articles to kept deal
    cur.execute(
        "UPDATE articles SET deal_id = %s WHERE deal_id = %s",
        (kept_id, del_id)
    )
    articles_moved = cur.rowcount

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

    print(f"    Deleted deal #{del_id}, moved {articles_moved} articles to #{kept_id}")


def run_cleanup(dry_run=False):
    """Run the duplicate cleanup."""
    print(f"Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    total_deleted = 0
    # Track per-part stats for summary
    part_stats = {
        1: {'groups': 0, 'deals': 0},
        2: {'groups': 0, 'deals': 0},
        3: {'groups': 0, 'deals': 0},
        4: {'groups': 0, 'deals': 0},
        5: {'groups': 0, 'deals': 0},
    }
    # Track deal IDs already marked for deletion to avoid double-counting
    already_handled = set()

    try:
        # === Part 1: Exact duplicates (same company + round + date + amount) ===
        print("\n=== Part 1: Finding exact duplicates (same company + round + date + amount) ===")
        exact_dups = find_exact_duplicates(cur)

        if exact_dups:
            print(f"Found {len(exact_dups)} duplicate groups:")
            for dup in exact_dups:
                company = dup['company_name']
                round_type = dup['round_type']
                date = dup['announced_date']
                amount = dup['amount_usd']
                ids = dup['ids']

                kept_id = ids[0]
                ids_to_delete = [i for i in ids[1:] if i not in already_handled]
                if not ids_to_delete:
                    continue

                part_stats[1]['groups'] += 1
                part_stats[1]['deals'] += len(ids_to_delete)

                amount_str = f"${amount/1_000_000:.1f}M" if amount else "N/A"
                print(f"\n  {company} | {round_type} | {date} | {amount_str}")
                print(f"    Keeping deal #{kept_id}")
                print(f"    Removing: {ids_to_delete}")

                for del_id in ids_to_delete:
                    delete_duplicate(cur, kept_id, del_id, company, dry_run)
                    if not dry_run:
                        total_deleted += 1
                    already_handled.add(del_id)
        else:
            print("No exact duplicates found.")

        # === Part 2: Null-field duplicates (same company, all key fields NULL) ===
        print("\n=== Part 2: Finding null-field duplicates (same company, all key fields NULL) ===")
        null_dups = find_null_field_duplicates(cur)

        if null_dups:
            print(f"Found {len(null_dups)} duplicate groups:")
            for dup in null_dups:
                company = dup['company_name']
                ids = dup['ids']

                kept_id = ids[0]
                ids_to_delete = [i for i in ids[1:] if i not in already_handled]
                if not ids_to_delete:
                    continue

                part_stats[2]['groups'] += 1
                part_stats[2]['deals'] += len(ids_to_delete)

                print(f"\n  {company} | (incomplete extraction - all fields NULL)")
                print(f"    Keeping deal #{kept_id}")
                print(f"    Removing: {ids_to_delete}")

                for del_id in ids_to_delete:
                    delete_duplicate(cur, kept_id, del_id, company, dry_run)
                    if not dry_run:
                        total_deleted += 1
                    already_handled.add(del_id)
        else:
            print("No null-field duplicates found.")

        # === Part 3: Round-mismatch duplicates (same company + amount, different round) ===
        print("\n=== Part 3: Finding round-mismatch duplicates (same company + amount, different round type) ===")
        round_dups = find_round_mismatch_duplicates(cur)

        if round_dups:
            print(f"Found {len(round_dups)} potential round-mismatch duplicates:")
            for dup in round_dups:
                company = dup['company_name']
                amount = dup['amount_usd']
                ids = dup['ids']
                round_types = dup['round_types']

                # For round mismatches, we need to be more careful
                # Keep the one with the most specific round type
                round_priority = ['SERIES_A', 'SERIES_B', 'SERIES_C', 'SERIES_D', 'SERIES_E',
                                  'SEED', 'PRE_SEED', 'GROWTH', 'UNKNOWN']

                kept_id = ids[0]  # Default to oldest
                ids_to_delete = [i for i in ids[1:] if i not in already_handled]
                if not ids_to_delete:
                    continue

                part_stats[3]['groups'] += 1
                part_stats[3]['deals'] += len(ids_to_delete)

                amount_str = f"${amount/1_000_000:.1f}M" if amount else "N/A"
                print(f"\n  {company} | {amount_str} | rounds: {round_types}")
                print(f"    Keeping deal #{kept_id}")
                print(f"    Removing: {ids_to_delete}")

                for del_id in ids_to_delete:
                    delete_duplicate(cur, kept_id, del_id, company, dry_run)
                    if not dry_run:
                        total_deleted += 1
                    already_handled.add(del_id)
        else:
            print("No round-mismatch duplicates found.")

        # === Part 4: Fuzzy company name duplicates (name differs only by suffix) ===
        print("\n=== Part 4: Finding fuzzy company name duplicates (name differs by suffix only) ===")
        fuzzy_dups = find_fuzzy_name_duplicates(cur)

        if fuzzy_dups:
            print(f"Found {len(fuzzy_dups)} fuzzy name duplicate groups:")
            for dup in fuzzy_dups:
                normalized = dup['normalized_name']
                original_names = dup['original_names']
                round_type = dup['round_type']
                ids = dup['ids']

                kept_id = ids[0]
                ids_to_delete = [i for i in ids[1:] if i not in already_handled]
                if not ids_to_delete:
                    continue

                part_stats[4]['groups'] += 1
                part_stats[4]['deals'] += len(ids_to_delete)

                print(f"\n  Normalized: \"{normalized}\" | Original names: {original_names} | round: {round_type}")
                print(f"    Keeping deal #{kept_id}")
                print(f"    Removing: {ids_to_delete}")

                for del_id in ids_to_delete:
                    delete_duplicate(cur, kept_id, del_id, normalized, dry_run)
                    if not dry_run:
                        total_deleted += 1
                    already_handled.add(del_id)
        else:
            print("No fuzzy name duplicates found.")

        # === Part 5: Amount-based cross-round duplicates (same company + similar amount) ===
        print("\n=== Part 5: Finding amount-based cross-round duplicates (same company + amount +-15% + date +-30 days) ===")
        amount_dups = find_amount_based_duplicates(cur)

        if amount_dups:
            print(f"Found {len(amount_dups)} amount-based duplicate pairs:")
            for dup in amount_dups:
                company = dup['company_name']
                amount1 = dup['amount1']
                amount2 = dup['amount2']
                ids = dup['ids']

                kept_id = ids[0]
                ids_to_delete = [i for i in ids[1:] if i not in already_handled]
                if not ids_to_delete:
                    continue

                part_stats[5]['groups'] += 1
                part_stats[5]['deals'] += len(ids_to_delete)

                amt1_str = f"${amount1/1_000_000:.1f}M" if amount1 else "N/A"
                amt2_str = f"${amount2/1_000_000:.1f}M" if amount2 else "N/A"
                round1 = dup['round1'] or 'NULL'
                round2 = dup['round2'] or 'NULL'
                print(f"\n  {company} | {amt1_str} ({round1}) vs {amt2_str} ({round2})")
                print(f"    Keeping deal #{kept_id}")
                print(f"    Removing: {ids_to_delete}")

                for del_id in ids_to_delete:
                    delete_duplicate(cur, kept_id, del_id, company, dry_run)
                    if not dry_run:
                        total_deleted += 1
                    already_handled.add(del_id)
        else:
            print("No amount-based cross-round duplicates found.")

        # === Summary ===
        total_groups = sum(s['groups'] for s in part_stats.values())
        total_deals = sum(s['deals'] for s in part_stats.values())

        if dry_run:
            print(f"\n=== Dry Run Summary ===")
            print(f"Part 1 (Exact duplicates):        {part_stats[1]['groups']:>3} groups, {part_stats[1]['deals']:>4} deals to remove")
            print(f"Part 2 (Null-field duplicates):   {part_stats[2]['groups']:>3} groups, {part_stats[2]['deals']:>4} deals to remove")
            print(f"Part 3 (Round mismatch):          {part_stats[3]['groups']:>3} groups, {part_stats[3]['deals']:>4} deals to remove")
            print(f"Part 4 (Fuzzy name):              {part_stats[4]['groups']:>3} groups, {part_stats[4]['deals']:>4} deals to remove")
            print(f"Part 5 (Amount-based):            {part_stats[5]['groups']:>3} groups, {part_stats[5]['deals']:>4} deals to remove")
            print(f"Total:                            {total_groups:>3} groups, {total_deals:>4} deals to remove")
        else:
            print(f"\n=== Summary ===")
            print(f"Part 1 (Exact duplicates):        {part_stats[1]['groups']:>3} groups, {part_stats[1]['deals']:>4} deals removed")
            print(f"Part 2 (Null-field duplicates):   {part_stats[2]['groups']:>3} groups, {part_stats[2]['deals']:>4} deals removed")
            print(f"Part 3 (Round mismatch):          {part_stats[3]['groups']:>3} groups, {part_stats[3]['deals']:>4} deals removed")
            print(f"Part 4 (Fuzzy name):              {part_stats[4]['groups']:>3} groups, {part_stats[4]['deals']:>4} deals removed")
            print(f"Part 5 (Amount-based):            {part_stats[5]['groups']:>3} groups, {part_stats[5]['deals']:>4} deals removed")
            print(f"Total:                            {total_groups:>3} groups, {total_deals:>4} deals removed")
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
    parser = argparse.ArgumentParser(description='Cleanup duplicate deals in the database')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be deleted without actually deleting')
    args = parser.parse_args()

    run_cleanup(dry_run=args.dry_run)
