#!/usr/bin/env python3
"""
Process Crunchbase Pro CSV Export.

Simple workflow to avoid Cloudflare bot detection:
1. You export CSV from Crunchbase (click Export button)
2. Run this script with the CSV file path
3. Script sends structured deals to backend API

USAGE:
    python scripts/process_crunchbase_csv.py ~/Downloads/crunchbase_export.csv

    # Dry run (don't send to backend)
    python scripts/process_crunchbase_csv.py ~/Downloads/crunchbase_export.csv --dry-run
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

import httpx

# ----- Configuration -----

BACKEND_API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")
API_KEY = os.getenv("BUD_TRACKER_API_KEY", "dev-key")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ----- CSV Parsing -----

def parse_crunchbase_csv(csv_path: str) -> List[Dict[str, Any]]:
    """
    Parse Crunchbase CSV export into deal dictionaries.

    Expected columns (flexible - maps common variations):
    - Organization Name / Company Name
    - Funding Type / Round
    - Money Raised / Amount
    - Announced Date / Date
    - Lead Investors / Investor Names
    """
    deals = []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        # Log columns for debugging
        logger.info(f"CSV columns: {reader.fieldnames}")

        for row in reader:
            deal = _parse_row(row)
            if deal:
                deals.append(deal)

    return deals


def _parse_row(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Parse a single CSV row into a deal dictionary."""
    # Map column names (Crunchbase export format)
    company_name = (
        row.get("Organization Name") or
        row.get("Company Name") or
        row.get("Funded Organization Name") or
        ""
    ).strip()

    if not company_name:
        return None

    funding_type = (
        row.get("Funding Type") or
        row.get("Round") or
        ""
    ).strip()

    # Prefer USD amount if available
    amount = (
        row.get("Money Raised (in USD)") or
        row.get("Money Raised") or
        row.get("Amount") or
        ""
    ).strip()

    # Format amount as string with $ if it's a number
    if amount and amount.isdigit():
        amount_num = int(amount)
        if amount_num >= 1_000_000_000:
            amount = f"${amount_num / 1_000_000_000:.1f}B"
        elif amount_num >= 1_000_000:
            amount = f"${amount_num / 1_000_000:.0f}M"
        else:
            amount = f"${amount_num:,}"

    date_str = (
        row.get("Announced Date") or
        row.get("Date") or
        ""
    ).strip()

    # Lead Investors - this is the key column!
    lead_investors_raw = (
        row.get("Lead Investors") or
        ""
    ).strip()

    # All Investors (for participating)
    all_investors_raw = (
        row.get("Investor Names") or
        ""
    ).strip()

    # Parse lead investors (comma separated)
    lead_investors = []
    if lead_investors_raw:
        lead_investors = [inv.strip() for inv in lead_investors_raw.split(",") if inv.strip()]

    # Parse all investors and remove leads to get participating
    all_investors = []
    if all_investors_raw:
        all_investors = [inv.strip() for inv in all_investors_raw.split(",") if inv.strip()]

    # Participating = all investors minus lead investors
    lead_set = set(lead_investors)
    participating = [inv for inv in all_investors if inv not in lead_set]

    # Get source URL (Crunchbase company page)
    source_url = (
        row.get("Organization Name URL") or
        row.get("Transaction Name URL") or
        None
    )

    # Get company description
    description = (
        row.get("Organization Description") or
        ""
    ).strip() or None

    # Get industries (comma-separated list)
    industries_raw = (
        row.get("Organization Industries") or
        ""
    ).strip()
    industries = [ind.strip() for ind in industries_raw.split(",") if ind.strip()] if industries_raw else []

    # Get company website
    website = (
        row.get("Organization Website") or
        ""
    ).strip() or None

    # Parse date
    announced_date = _parse_date(date_str)

    # Normalize round type
    round_type = _normalize_round_type(funding_type)

    return {
        "startup_name": company_name,
        "round_type": round_type,
        "amount": amount if amount and amount != "-" else None,
        "announced_date": announced_date,
        "lead_investors": lead_investors,
        "participating_investors": participating,
        "source_url": source_url,
        "source": "crunchbase_csv",
        # New fields for AI classification
        "description": description,
        "industries": industries,
        "website": website,
    }


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date string to ISO format."""
    if not date_str or date_str == "-":
        return None

    formats = [
        "%b %d, %Y",    # "Dec 18, 2025"
        "%B %d, %Y",    # "December 18, 2025"
        "%Y-%m-%d",     # "2025-12-18"
        "%m/%d/%Y",     # "12/18/2025"
        "%d/%m/%Y",     # "18/12/2025"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def _normalize_round_type(funding_type: str) -> str:
    """Map Crunchbase funding type to our round types."""
    if not funding_type:
        return "unknown"

    ft = funding_type.lower().strip()

    # Handle "Series B - Company Name" format
    if " - " in ft:
        ft = ft.split(" - ")[0].strip()

    mapping = {
        "pre-seed": "pre_seed",
        "seed": "seed",
        "series a": "series_a",
        "series b": "series_b",
        "series c": "series_c",
        "series d": "series_d",
        "series e": "series_e_plus",
        "series f": "series_e_plus",
        "series g": "series_e_plus",
        "series h": "series_e_plus",
        "growth": "growth",
        "venture": "unknown",
        "venture - series unknown": "unknown",
        "debt financing": "debt",
        "convertible note": "seed",
        "angel": "pre_seed",
        "grant": "unknown",
    }

    return mapping.get(ft, "unknown")


# ----- API Client -----

BATCH_SIZE = 100  # Process deals in batches to avoid timeout

async def send_to_backend(deals: List[Dict], dry_run: bool = False) -> Dict:
    """POST deals to backend API in batches."""
    if dry_run:
        logger.info(f"DRY RUN: Would send {len(deals)} deals to backend")
        return {"dry_run": True, "deals_count": len(deals)}

    total_saved = 0
    total_duplicate = 0
    total_no_tracked = 0
    total_errors = []

    async with httpx.AsyncClient(timeout=120) as client:
        for i in range(0, len(deals), BATCH_SIZE):
            batch = deals[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(deals) + BATCH_SIZE - 1) // BATCH_SIZE

            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} deals)")

            try:
                response = await client.post(
                    f"{BACKEND_API_URL}/scrapers/crunchbase-direct",
                    json={"deals": batch},
                    headers={"X-API-Key": API_KEY},
                )
                response.raise_for_status()
                result = response.json()

                total_saved += result.get("deals_saved", 0)
                total_duplicate += result.get("deals_duplicate", 0)
                total_no_tracked += result.get("deals_no_tracked_fund", 0)
                if result.get("errors"):
                    total_errors.extend(result["errors"])

                logger.info(f"  Batch {batch_num}: {result.get('deals_saved', 0)} saved, "
                           f"{result.get('deals_duplicate', 0)} dupe, "
                           f"{result.get('deals_no_tracked_fund', 0)} no tracked")
            except Exception as e:
                logger.error(f"  Batch {batch_num} failed: {e}")
                total_errors.append(str(e))

    return {
        "deals_received": len(deals),
        "deals_saved": total_saved,
        "deals_duplicate": total_duplicate,
        "deals_no_tracked_fund": total_no_tracked,
        "errors": total_errors,
    }


# ----- Main -----

async def main(csv_path: str, dry_run: bool = False):
    """Process CSV and send to backend."""
    logger.info("=" * 60)
    logger.info("Crunchbase CSV Processor")
    logger.info(f"  CSV file: {csv_path}")
    logger.info(f"  Backend: {BACKEND_API_URL}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info("=" * 60)

    # Check file exists
    if not os.path.exists(csv_path):
        logger.error(f"File not found: {csv_path}")
        sys.exit(1)

    # Parse CSV
    deals = parse_crunchbase_csv(csv_path)
    logger.info(f"Parsed {len(deals)} deals from CSV")

    if not deals:
        logger.warning("No deals found in CSV")
        return

    # Show sample
    logger.info(f"Sample deal: {json.dumps(deals[0], indent=2)}")

    # Send to backend
    result = await send_to_backend(deals, dry_run=dry_run)
    logger.info(f"Result: {json.dumps(result, indent=2)}")

    logger.info("=" * 60)
    logger.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Crunchbase CSV export")
    parser.add_argument("csv_path", help="Path to Crunchbase CSV export file")
    parser.add_argument("--dry-run", action="store_true", help="Parse CSV but don't send to backend")
    args = parser.parse_args()

    asyncio.run(main(args.csv_path, dry_run=args.dry_run))
