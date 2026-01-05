#!/usr/bin/env python3
"""
Crunchbase Pro Scraper Bot - Local Mac runner.

Scrapes your Crunchbase Pro saved search table directly (avoids export quota limits).
Runs via cron daily and POSTs structured data to the backend API.

PREREQUISITES:
    pip install playwright httpx python-dotenv
    playwright install chromium

ENVIRONMENT VARIABLES (in .env):
    CRUNCHBASE_SEARCH_URL  - Your saved search URL from Crunchbase Pro
    BACKEND_API_URL        - Backend API URL (default: http://localhost:8000)
    BUD_TRACKER_API_KEY    - Your API key for the backend

USAGE:
    # Test run (headless=False to see browser)
    python scripts/crunchbase_bot.py --visible

    # Production run (headless)
    python scripts/crunchbase_bot.py

    # Dry run (scrape but don't send to backend)
    python scripts/crunchbase_bot.py --dry-run
"""

import asyncio
import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # dotenv optional

import httpx

# Playwright import (optional - gives helpful error if missing)
try:
    from playwright.async_api import async_playwright, Page, BrowserContext
    from playwright_stealth import Stealth
except ImportError:
    print("ERROR: Playwright not installed. Run: pip install playwright playwright-stealth && playwright install chromium")
    sys.exit(1)


# ----- Configuration -----

CRUNCHBASE_SEARCH_URL = os.getenv("CRUNCHBASE_SEARCH_URL", "")
BACKEND_API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")
API_KEY = os.getenv("BUD_TRACKER_API_KEY", "dev-key")

# Browser profile path - use project-local directory to avoid conflicts with running Chrome
# First run: You'll need to log into Crunchbase manually (session saved for future runs)
BROWSER_USER_DATA = str(PROJECT_ROOT / ".playwright-data")
CHROME_PROFILE = os.getenv("CHROME_PROFILE", "Default")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / "logs" / "crunchbase_bot.log", mode="a"),
    ]
)
logger = logging.getLogger(__name__)

# Ensure logs directory exists
(PROJECT_ROOT / "logs").mkdir(exist_ok=True)


# ----- Data Classes -----

@dataclass
class CrunchbaseDeal:
    """Structured deal from Crunchbase table row."""
    transaction_name: str
    organization_name: str
    funding_type: str
    money_raised: Optional[str]
    announced_date: Optional[str]
    investor_names: List[str]
    lead_investors: List[str]
    organization_url: Optional[str]


# ----- Scraper Class -----

class CrunchbaseTableScraper:
    """Scrapes Crunchbase saved search table using Playwright."""

    def __init__(self, search_url: str, headless: bool = True):
        self.search_url = search_url
        self.headless = headless
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._playwright = None

    async def __aenter__(self):
        """Launch browser with persistent profile (keeps login session)."""
        self._playwright = await async_playwright().start()

        # Ensure browser data directory exists
        Path(BROWSER_USER_DATA).mkdir(parents=True, exist_ok=True)

        # Use Playwright's Chromium with persistent context
        # First run: browser opens, you log into Crunchbase manually
        # Future runs: session is saved, auto-logged in
        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=BROWSER_USER_DATA,
            headless=self.headless,
            viewport={"width": 1920, "height": 1080},
            # Slow down for visibility during debugging
            slow_mo=100 if not self.headless else 0,
            # Additional args to appear more human-like
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
            # Set realistic user agent
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        self._page = await self._context.new_page()

        # Apply stealth to avoid bot detection
        stealth = Stealth(
            navigator_platform_override="MacIntel",  # Mac platform
            navigator_vendor_override="Google Inc.",
        )
        await stealth.apply_stealth_async(self._page)

        return self

    async def __aexit__(self, *args):
        """Clean up browser resources."""
        if self._context:
            await self._context.close()
        if self._playwright:
            await self._playwright.stop()

    async def scrape_all(self) -> List[CrunchbaseDeal]:
        """Scrape all pages of the saved search."""
        logger.info(f"Navigating to: {self.search_url}")
        # Use 'load' instead of 'networkidle' - Crunchbase keeps loading analytics
        await self._page.goto(self.search_url, wait_until="load", timeout=90000)

        # Give the page a moment to render React components
        await self._page.wait_for_timeout(3000)

        # Check if we need to log in
        await self._handle_login_if_needed()

        # Wait for table to load
        try:
            await self._page.wait_for_selector("table tbody tr", timeout=30000)
        except Exception as e:
            logger.error(f"Table not found - are you logged in? Error: {e}")
            # Take screenshot for debugging
            await self._page.screenshot(path=str(PROJECT_ROOT / "logs" / "crunchbase_error.png"))
            raise

        # Scrape all pages
        all_deals = []
        page_num = 1

        while True:
            logger.info(f"Scraping page {page_num}...")
            deals = await self._scrape_current_page()
            all_deals.extend(deals)
            logger.info(f"  Found {len(deals)} deals on page {page_num}")

            # Check for next page button
            next_btn = await self._page.query_selector('button[aria-label="Next"]:not([disabled])')
            if not next_btn:
                # Also try alternative selector
                next_btn = await self._page.query_selector('[data-test="pagination-next"]:not([disabled])')

            if not next_btn:
                logger.info("No more pages")
                break

            await next_btn.click()
            await self._page.wait_for_timeout(2000)  # Wait for table refresh
            page_num += 1

            # Safety limit
            if page_num > 20:
                logger.warning("Hit page limit (20), stopping")
                break

        return all_deals

    async def _handle_login_if_needed(self):
        """Check for login page or captcha and wait for manual intervention."""
        # Check page content for captcha indicators
        page_content = await self._page.content()
        page_content_lower = page_content.lower()

        captcha_indicators = [
            "verify you are human",
            "verify your session",
            "cloudflare",
            "cf-turnstile",
            "challenge-platform",
        ]

        has_captcha = any(indicator in page_content_lower for indicator in captcha_indicators)

        if has_captcha:
            logger.warning("=" * 60)
            logger.warning("CLOUDFLARE CAPTCHA DETECTED!")
            logger.warning("Please solve the captcha in the browser window.")
            logger.warning("Click the checkbox, then wait...")
            logger.warning("Waiting up to 3 minutes...")
            logger.warning("=" * 60)

            # Wait for captcha to disappear - check for table or login form
            try:
                await self._page.wait_for_selector(
                    "table, input[name='email'], .search-results, [class*='results']",
                    timeout=180000  # 3 min
                )
                logger.info("Captcha solved! Continuing...")
                await self._page.wait_for_timeout(3000)  # Let page fully load
            except Exception:
                # Take screenshot before failing
                await self._page.screenshot(path=str(PROJECT_ROOT / "logs" / "captcha_timeout.png"))
                logger.error("Captcha timeout - please try again")
                raise
            return

        # Check for common login indicators
        login_selectors = [
            'input[name="email"]',
            'input[type="password"]',
            'button:has-text("Log In")',
            'a:has-text("Sign In")',
        ]

        for selector in login_selectors:
            element = await self._page.query_selector(selector)
            if element:
                logger.warning("=" * 60)
                logger.warning("LOGIN REQUIRED!")
                logger.warning("Please log into Crunchbase in the browser window.")
                logger.warning("Your session will be saved for future runs.")
                logger.warning("Waiting up to 5 minutes for login...")
                logger.warning("=" * 60)

                # Wait for table to appear (indicating successful login)
                try:
                    await self._page.wait_for_selector("table", timeout=300000)  # 5 min
                    logger.info("Login successful! Continuing...")
                    await self._page.wait_for_timeout(2000)  # Let page settle
                except Exception:
                    logger.error("Login timeout - please try again")
                    raise
                break

    async def _scrape_current_page(self) -> List[CrunchbaseDeal]:
        """Extract deals from current table page."""
        rows = await self._page.query_selector_all("table tbody tr")
        deals = []

        for row in rows:
            try:
                deal = await self._parse_row(row)
                if deal:
                    deals.append(deal)
            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                continue

        return deals

    async def _parse_row(self, row) -> Optional[CrunchbaseDeal]:
        """Parse a single table row into a CrunchbaseDeal."""
        cells = await row.query_selector_all("td")
        if len(cells) < 5:
            return None

        # Get column texts - adjust indices based on your table columns
        # Typical: Transaction Name, Investor Names, Organization, Funding Type, Money Raised, Date
        transaction_name = await self._get_cell_text(cells[0])
        investor_names_raw = await self._get_cell_text(cells[1])
        organization_name = await self._get_cell_text(cells[2])
        funding_type = await self._get_cell_text(cells[3])
        money_raised = await self._get_cell_text(cells[4])
        announced_date = await self._get_cell_text(cells[5]) if len(cells) > 5 else None

        # Parse investors (comma-separated)
        all_investors = [inv.strip() for inv in investor_names_raw.split(",") if inv.strip()]

        # Try to identify lead investors (look for badges or first investor)
        lead_investors = await self._extract_lead_investors(cells[1], all_investors)

        # Get organization URL
        org_link = await cells[2].query_selector("a")
        organization_url = await org_link.get_attribute("href") if org_link else None

        return CrunchbaseDeal(
            transaction_name=transaction_name,
            organization_name=organization_name,
            funding_type=funding_type,
            money_raised=money_raised,
            announced_date=announced_date,
            investor_names=all_investors,
            lead_investors=lead_investors,
            organization_url=organization_url,
        )

    async def _get_cell_text(self, cell) -> str:
        """Get clean text content from a cell."""
        text = await cell.inner_text()
        return text.strip() if text else ""

    async def _extract_lead_investors(self, investor_cell, all_investors: List[str]) -> List[str]:
        """
        Extract lead investors from the investor cell.

        Crunchbase marks lead investors with badges or special styling.
        Falls back to first investor if no badge found.
        """
        # Look for lead investor badges/markers
        lead_badges = await investor_cell.query_selector_all('[class*="lead"], [data-lead="true"], .lead-investor')

        if lead_badges:
            leads = []
            for badge in lead_badges:
                text = await badge.inner_text()
                if text.strip():
                    leads.append(text.strip())
            if leads:
                return leads

        # Fallback: assume first investor is lead (common convention)
        return all_investors[:1] if all_investors else []


# ----- Data Mapping -----

def map_to_backend_schema(deals: List[CrunchbaseDeal]) -> List[dict]:
    """Convert Crunchbase deals to backend API format."""
    mapped = []

    for deal in deals:
        # Parse round type
        round_type = _normalize_round_type(deal.funding_type)

        # Parse date to ISO format
        announced_date = _parse_date(deal.announced_date)

        # Separate lead vs participating investors
        participating = [i for i in deal.investor_names if i not in deal.lead_investors]

        mapped.append({
            "startup_name": deal.organization_name,
            "round_type": round_type,
            "amount": deal.money_raised,
            "announced_date": announced_date,
            "lead_investors": deal.lead_investors,
            "participating_investors": participating,
            "source_url": f"https://www.crunchbase.com{deal.organization_url}" if deal.organization_url else None,
            "source": "crunchbase_pro",
        })

    return mapped


def _normalize_round_type(funding_type: str) -> str:
    """Map Crunchbase funding type to our RoundType values."""
    if not funding_type:
        return "unknown"

    ft = funding_type.lower().strip()

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
    }

    return mapping.get(ft, "unknown")


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse Crunchbase date format to ISO format."""
    if not date_str:
        return None

    # Try common formats
    formats = [
        "%b %d, %Y",    # "Dec 18, 2025"
        "%B %d, %Y",    # "December 18, 2025"
        "%Y-%m-%d",     # "2025-12-18"
        "%m/%d/%Y",     # "12/18/2025"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


# ----- API Client -----

async def send_to_backend(deals: List[dict], dry_run: bool = False) -> dict:
    """POST deals to backend API."""
    if dry_run:
        logger.info(f"DRY RUN: Would send {len(deals)} deals to backend")
        return {"dry_run": True, "deals_count": len(deals)}

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{BACKEND_API_URL}/scrapers/crunchbase-direct",
            json={"deals": deals},
            headers={"X-API-Key": API_KEY},
        )
        response.raise_for_status()
        return response.json()


# ----- Main Entry Point -----

async def main(visible: bool = False, dry_run: bool = False):
    """Main entry point."""
    if not CRUNCHBASE_SEARCH_URL:
        logger.error("CRUNCHBASE_SEARCH_URL not set in environment")
        logger.error("Set it in .env or export CRUNCHBASE_SEARCH_URL='your-url'")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Crunchbase Pro Scraper Bot Starting")
    logger.info(f"  Search URL: {CRUNCHBASE_SEARCH_URL[:50]}...")
    logger.info(f"  Backend: {BACKEND_API_URL}")
    logger.info(f"  Headless: {not visible}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info("=" * 60)

    try:
        async with CrunchbaseTableScraper(CRUNCHBASE_SEARCH_URL, headless=not visible) as scraper:
            deals = await scraper.scrape_all()
            logger.info(f"Scraped {len(deals)} deals from Crunchbase")

            if not deals:
                logger.warning("No deals found - check if filters are correct")
                return

            # Map to backend schema
            mapped_deals = map_to_backend_schema(deals)

            # Log sample for debugging
            if mapped_deals:
                logger.info(f"Sample deal: {json.dumps(mapped_deals[0], indent=2)}")

            # Send to backend
            result = await send_to_backend(mapped_deals, dry_run=dry_run)
            logger.info(f"Backend result: {json.dumps(result, indent=2)}")

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Crunchbase Pro Scraper Bot Complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crunchbase Pro Scraper Bot")
    parser.add_argument("--visible", action="store_true", help="Show browser window (for debugging)")
    parser.add_argument("--dry-run", action="store_true", help="Scrape but don't send to backend")
    args = parser.parse_args()

    asyncio.run(main(visible=args.visible, dry_run=args.dry_run))
