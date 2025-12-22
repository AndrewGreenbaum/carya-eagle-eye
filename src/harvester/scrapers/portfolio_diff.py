"""
VC Portfolio Page Diff - Catch Stealth Additions Before Announcements.

Monitors VC fund portfolio pages for NEW company additions.
VCs often add companies to their portfolio pages BEFORE public announcements.

How it works:
1. Scrape portfolio pages for all listed companies
2. Compare against previous snapshot
3. New companies = stealth additions (pre-announcement)
4. Store snapshot for next comparison

This is one of the BEST free sources for true stealth detection.

UPDATED: Now uses Playwright for JS-heavy pages (a16z, Felicis, Accel, etc.)
"""

import asyncio
import json
import logging
import re
import httpx
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import List, Optional, Dict, Set

from playwright.async_api import async_playwright, Browser, BrowserContext, Playwright

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..base_scraper import NormalizedArticle
from ...config.settings import settings
from ...archivist.database import get_session
from ...archivist.models import PortfolioSnapshot

logger = logging.getLogger(__name__)

# Funds that require JavaScript rendering (React/Vue SPAs)
# Based on empirical testing - these return 0-1 companies with simple HTTP
PLAYWRIGHT_REQUIRED_FUNDS = {
    "a16z",       # React SPA - portfolio loads dynamically
    "felicis",    # JS-heavy portfolio grid
    "accel",      # Dynamic company list
    "index",      # Client-side rendered
    "insight",    # JS portfolio page
    "greylock",   # Gatsby/React site
    "redpoint",   # React site
    "menlo",      # JS-rendered portfolio
    "gv",         # React portfolio
    "khosla",     # JS portfolio
    "benchmark",  # Dynamic portfolio
    "first_round", # JS portfolio
    # NOTE: Thrive removed - no public portfolio page
}

# Memory safety limit
MAX_COMPANIES_PER_FUND = 500

# Known public/established companies to NEVER flag as "stealth additions"
# These are well-known companies that VCs have invested in for years
# Prevents false alerts when portfolio pages are first scraped or re-scraped
KNOWN_PUBLIC_COMPANIES = {
    # FAANG and mega-caps
    "google", "alphabet", "meta", "facebook", "amazon", "apple", "microsoft",
    "nvidia", "netflix", "tesla", "twitter", "x",
    # Major public tech companies
    "airbnb", "doordash", "uber", "lyft", "pinterest", "snap", "snapchat",
    "spotify", "shopify", "stripe", "instacart", "coinbase", "robinhood",
    "snowflake", "datadog", "crowdstrike", "cloudflare", "mongodb", "elastic",
    "palantir", "unity", "roblox", "discord", "reddit", "linkedin",
    "salesforce", "workday", "servicenow", "atlassian", "slack", "zoom",
    "dropbox", "box", "docusign", "okta", "twilio", "sendgrid",
    "hubspot", "zendesk", "freshworks", "monday", "asana", "notion",
    "figma", "canva", "miro", "airtable", "webflow",
    "plaid", "brex", "ramp", "gusto", "rippling", "deel",
    "stripe", "square", "block", "paypal", "klarna", "affirm", "chime",
    # Legacy companies
    "whatsapp", "youtube", "instagram", "waze", "nest", "fitbit",
    "github", "gitlab", "bitbucket", "heroku", "vercel", "netlify",
    # Hardware/Other
    "spacex", "openai", "anthropic", "anduril",
    # Additional well-known companies
    "skype", "samsara", "pagerduty", "oculus", "databricks", "splunk",
    "wise", "retool", "linear", "vanta",
}

def _is_known_public_company(name: str) -> bool:
    """Check if company name matches a known public/established company."""
    name_lower = name.lower().strip()
    # Direct match
    if name_lower in KNOWN_PUBLIC_COMPANIES:
        return True
    # Check if name starts with known company (handles "Block (Square, Cash App...)")
    for known in KNOWN_PUBLIC_COMPANIES:
        if name_lower.startswith(known):
            return True
    return False

# Portfolio page URLs for tracked funds
# NOTE: Extract company names from img alt tags or specific CSS selectors
# Some funds don't have public portfolio pages (Thrive - just a splash page)
PORTFOLIO_URLS = {
    "a16z": {
        "url": "https://a16z.com/portfolio/",
        # a16z uses img alt tags for company names inside portfolio cards
        "selector": ".portfolio-card",
        "name_selector": ".logo-wrap img",  # Company name in img alt attribute
        "name_attr": "alt",  # Extract from alt attribute instead of text
    },
    "sequoia": {
        "url": "https://www.sequoiacap.com/our-companies/",
        "selector": "a[href*='/companies/']",
        "name_selector": None,  # Use link text directly
    },
    "founders_fund": {
        "url": "https://foundersfund.com/portfolio/",
        "selector": ".portfolio-company, .company, [class*='portfolio']",
        "name_selector": "h3, h4, .name, .title",
    },
    "benchmark": {
        "url": "https://www.benchmark.com/portfolio/",
        "selector": ".company, .portfolio-item, [class*='company']",
        "name_selector": "h3, h4, .name",
    },
    "greylock": {
        "url": "https://greylock.com/portfolio/",
        "selector": ".company-card, .portfolio-company, [class*='portfolio']",
        "name_selector": "h3, h4, .company-name",
    },
    "khosla": {
        "url": "https://www.khoslaventures.com/portfolio",
        "selector": ".portfolio-company, .company, [class*='portfolio']",
        "name_selector": "h3, h4, .name",
    },
    "index": {
        "url": "https://www.indexventures.com/companies/",
        "selector": ".company, .portfolio-item",
        "name_selector": "h3, h4, .name",
    },
    "insight": {
        "url": "https://www.insightpartners.com/portfolio/",
        "selector": ".portfolio-company, .company",
        "name_selector": "h3, h4, .name",
    },
    "bessemer": {
        "url": "https://www.bvp.com/portfolio",
        "selector": ".portfolio-company, .company",
        "name_selector": "h3, h4, .name",
    },
    "accel": {
        "url": "https://www.accel.com/companies",
        "selector": ".company, .portfolio-item",
        "name_selector": "h3, h4, .name",
    },
    "felicis": {
        "url": "https://www.felicis.com/portfolio",
        "selector": ".portfolio-company, .company",
        "name_selector": "h3, h4, .name",
    },
    "general_catalyst": {
        "url": "https://www.generalcatalyst.com/portfolio",
        "selector": ".portfolio-company, .company",
        "name_selector": "h3, h4, .name",
    },
    "first_round": {
        "url": "https://firstround.com/companies/",
        "selector": ".company, .portfolio-item",
        "name_selector": "h3, h4, .name",
    },
    "menlo": {
        "url": "https://www.menlovc.com/portfolio/",
        "selector": ".portfolio-company, .company",
        "name_selector": "h3, h4, .name",
    },
    "redpoint": {
        "url": "https://www.redpoint.com/companies/",
        "selector": ".company, .portfolio-item",
        "name_selector": "h3, h4, .name",
    },
    "usv": {
        "url": "https://www.usv.com/companies/",
        "selector": ".company, .portfolio-item",
        "name_selector": "h3, h4, .name",
    },
    # NOTE: Thrive has no public portfolio page - just a splash page
    # Portfolio tracking for Thrive done via Brave Search / news sources
    "gv": {
        "url": "https://www.gv.com/portfolio/",
        "selector": ".portfolio-company, .company",
        "name_selector": "h3, h4, .name",
    },
}

# Snapshots are stored in database (portfolio_snapshots table)
# This survives Railway redeploys and ensures accurate diff detection.


@dataclass
class PortfolioCompany:
    """Company found on a VC portfolio page."""
    name: str
    fund_slug: str
    url: Optional[str] = None
    description: Optional[str] = None
    first_seen: Optional[date] = None


@dataclass
class PortfolioDiff:
    """Diff between two portfolio snapshots."""
    fund_slug: str
    new_companies: List[PortfolioCompany]
    removed_companies: List[str]
    snapshot_date: date


class PortfolioDiffScraper:
    """
    Monitor VC portfolio pages for new company additions.

    Detects stealth additions by comparing current portfolio
    against stored snapshot from previous run.

    Uses Playwright for JS-heavy pages (React/Vue SPAs) and
    simple HTTP for static HTML pages.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
            follow_redirects=True,
        )
        # Playwright for JS-heavy pages
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def __aenter__(self):
        # Initialize Playwright for JS-heavy fund pages
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        self._context = await self._browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            java_script_enabled=True,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
        # Clean up Playwright
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _load_snapshot(self, fund_slug: str) -> Set[str]:
        """Load previous snapshot of company names from database."""
        async with get_session() as session:
            stmt = select(PortfolioSnapshot).where(PortfolioSnapshot.fund_slug == fund_slug)
            result = await session.execute(stmt)
            snapshot = result.scalar_one_or_none()

            if snapshot:
                try:
                    companies = json.loads(snapshot.companies_json)
                    return set(companies)
                except json.JSONDecodeError:
                    return set()
            return set()

    async def _save_snapshot(self, fund_slug: str, companies: Set[str]) -> None:
        """Save current snapshot of company names to database."""
        async with get_session() as session:
            companies_json = json.dumps(list(companies))

            # Use ON CONFLICT to upsert
            stmt = pg_insert(PortfolioSnapshot).values(
                fund_slug=fund_slug,
                companies_json=companies_json,
                updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            ).on_conflict_do_update(
                index_elements=['fund_slug'],
                set_={
                    'companies_json': companies_json,
                    'updated_at': datetime.now(timezone.utc).replace(tzinfo=None),
                }
            )
            await session.execute(stmt)
            await session.commit()

    async def _fetch_with_playwright(self, url: str, wait_selector: Optional[str] = None) -> str:
        """
        Fetch page using Playwright for JavaScript rendering.

        Args:
            url: URL to fetch
            wait_selector: CSS selector to wait for content load

        Returns:
            Rendered HTML string
        """
        page = await self._context.new_page()
        try:
            # Add anti-detection
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)

            await page.goto(url, wait_until='load', timeout=30000)

            # Wait for content selector if provided
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=5000)
                except Exception:
                    pass  # Continue even if selector not found

            # Wait for JS to render content
            await page.wait_for_timeout(2000)

            # Scroll to load lazy content
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(1000)

            return await page.content()
        finally:
            await page.close()

    async def scrape_portfolio_page(self, fund_slug: str) -> List[PortfolioCompany]:
        """
        Scrape a single fund's portfolio page for company names.

        Uses Playwright for JS-heavy pages (React/Vue SPAs) and
        simple HTTP for static HTML pages.
        """
        config = PORTFOLIO_URLS.get(fund_slug)
        if not config:
            return []

        try:
            # Use Playwright for JS-heavy pages
            if fund_slug in PLAYWRIGHT_REQUIRED_FUNDS:
                logger.info(f"Using Playwright for {fund_slug} (JS-heavy page)")
                html = await self._fetch_with_playwright(
                    config["url"],
                    wait_selector=config.get("selector")
                )
                soup = BeautifulSoup(html, 'lxml')
            else:
                response = await self.client.get(config["url"])
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
            companies = []
            seen_names = set()

            # Try multiple selector strategies
            selectors = [
                config["selector"],
                "a[href*='portfolio']",
                "a[href*='company']",
                ".company",
                "[class*='portfolio']",
                "article",
            ]

            for selector in selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        # Try to extract company name
                        name = None

                        # Check if we should extract from an attribute (e.g., img alt)
                        name_attr = config.get("name_attr")
                        name_selector = config.get("name_selector")

                        if name_selector:
                            name_elem = elem.select_one(name_selector)
                            if name_elem:
                                if name_attr:
                                    # Extract from attribute (e.g., img alt="Airbnb")
                                    name = name_elem.get(name_attr, "").strip()
                                else:
                                    name = name_elem.get_text(strip=True)

                        # Fallback to element text
                        if not name:
                            name = elem.get_text(strip=True)

                        # Clean up name
                        if name:
                            # Remove common suffixes
                            name = re.sub(r'\s*(Inc\.?|LLC|Ltd\.?|Corp\.?)$', '', name, flags=re.IGNORECASE)
                            name = name.strip()

                            # Skip if too short, too long, or already seen
                            if len(name) < 2 or len(name) > 100:
                                continue
                            if name.lower() in seen_names:
                                continue

                            # Skip common non-company text
                            skip_words = ['portfolio', 'companies', 'view all', 'load more', 'filter', 'sort']
                            if any(skip in name.lower() for skip in skip_words):
                                continue

                            seen_names.add(name.lower())

                            # Memory safety check
                            if len(companies) >= MAX_COMPANIES_PER_FUND:
                                logger.warning(f"Hit max companies limit ({MAX_COMPANIES_PER_FUND}) for {fund_slug}")
                                break

                            # Extract URL if available
                            url = None
                            link = elem.find('a') if elem.name != 'a' else elem
                            if link and link.get('href'):
                                href = link.get('href')
                                if href.startswith('/'):
                                    href = f"https://{config['url'].split('/')[2]}{href}"
                                url = href

                            companies.append(PortfolioCompany(
                                name=name,
                                fund_slug=fund_slug,
                                url=url,
                                first_seen=date.today(),
                            ))
                except Exception:
                    continue

                # If we found companies, stop trying other selectors
                # FIX: Lower threshold - some funds show fewer than 5 companies
                if len(companies) >= 1:
                    break

            return companies

        except Exception as e:
            logger.error(f"Error scraping {fund_slug} portfolio: {e}")
            return []

    async def get_portfolio_diff(self, fund_slug: str) -> PortfolioDiff:
        """
        Get diff between current portfolio and stored snapshot.

        IMPORTANT: On first-run (empty snapshot), we populate the snapshot
        but don't report any companies as "new" to avoid false alerts for
        established companies like Google, NVIDIA, Airbnb, etc.
        """
        # Load previous snapshot from database
        previous = await self._load_snapshot(fund_slug)
        is_first_run = len(previous) == 0

        # Scrape current portfolio
        current_companies = await self.scrape_portfolio_page(fund_slug)
        current_names = {c.name.lower() for c in current_companies}

        # Save current snapshot to database for next run
        await self._save_snapshot(fund_slug, current_names)

        # FIRST-RUN PROTECTION: If no previous snapshot, don't report any as "new"
        # This prevents false alerts when portfolio is first scraped
        if is_first_run:
            logger.info(
                f"First-run for {fund_slug}: populated snapshot with {len(current_names)} companies, "
                f"skipping 'new company' alerts to avoid false positives"
            )
            return PortfolioDiff(
                fund_slug=fund_slug,
                new_companies=[],  # No alerts on first run
                removed_companies=[],
                snapshot_date=date.today(),
            )

        # Find new companies (in current but not in previous)
        new_names = current_names - previous

        # BULK ALERT PROTECTION: If >10 "new" companies, likely a snapshot issue
        # (e.g., previous scrape failed, snapshot corrupted, selectors changed)
        # Skip alerting to avoid false positive spam
        MAX_NEW_COMPANIES_THRESHOLD = 10
        if len(new_names) > MAX_NEW_COMPANIES_THRESHOLD:
            logger.warning(
                f"Bulk alert protection for {fund_slug}: {len(new_names)} new companies detected "
                f"(threshold={MAX_NEW_COMPANIES_THRESHOLD}). Likely snapshot issue - skipping alerts."
            )
            return PortfolioDiff(
                fund_slug=fund_slug,
                new_companies=[],  # Skip bulk alerts
                removed_companies=[],
                snapshot_date=date.today(),
            )

        # Filter out known public/established companies
        new_companies = []
        filtered_count = 0
        for c in current_companies:
            if c.name.lower() in new_names:
                if _is_known_public_company(c.name):
                    logger.debug(f"Filtering known public company: {c.name}")
                    filtered_count += 1
                else:
                    new_companies.append(c)

        if filtered_count > 0:
            logger.info(f"Filtered {filtered_count} known public companies from {fund_slug} diff")

        # Find removed companies (in previous but not in current)
        removed_names = previous - current_names

        return PortfolioDiff(
            fund_slug=fund_slug,
            new_companies=new_companies,
            removed_companies=list(removed_names),
            snapshot_date=date.today(),
        )

    def diff_to_article(self, company: PortfolioCompany) -> NormalizedArticle:
        """Convert new portfolio company to NormalizedArticle."""
        text_parts = [
            f"[STEALTH PORTFOLIO ADDITION DETECTED]",
            f"",
            f"Fund: {company.fund_slug}",
            f"Company: {company.name}",
            f"First Seen: {company.first_seen}",
            f"",
            f"This company was just added to {company.fund_slug}'s portfolio page.",
            f"This often happens BEFORE any public funding announcement.",
        ]

        if company.url:
            text_parts.append(f"\nPortfolio URL: {company.url}")

        # Generate fund-specific URL if company URL not available
        fund_config = PORTFOLIO_URLS.get(company.fund_slug, {})
        fund_url = fund_config.get("url", f"https://{company.fund_slug}.com/portfolio/")
        fallback_url = f"{fund_url.rstrip('/')}#{company.name.lower().replace(' ', '-')}"

        return NormalizedArticle(
            url=company.url or fallback_url,
            title=f"Stealth Addition: {company.name} added to {company.fund_slug} portfolio",
            text="\n".join(text_parts),
            published_date=company.first_seen or date.today(),
            author=f"{company.fund_slug} Portfolio",
            tags=["portfolio_diff", "stealth", "pre_announcement", company.fund_slug],
            fund_slug=company.fund_slug,
            fetched_at=datetime.now(timezone.utc),
        )

    async def scrape_all(
        self,
        fund_slugs: Optional[List[str]] = None,
    ) -> List[NormalizedArticle]:
        """
        Scrape all portfolio pages and return new additions.

        Args:
            fund_slugs: Optional list of specific funds to check.
                       If None, checks all configured funds.

        Returns:
            List of NormalizedArticle for each new company detected.
        """
        if fund_slugs is None:
            fund_slugs = list(PORTFOLIO_URLS.keys())

        all_articles = []

        for fund_slug in fund_slugs:
            if fund_slug not in PORTFOLIO_URLS:
                continue

            diff = await self.get_portfolio_diff(fund_slug)

            # Convert new companies to articles
            for company in diff.new_companies:
                article = self.diff_to_article(company)
                all_articles.append(article)

            # Rate limit between funds (0.3s matches other scrapers)
            await asyncio.sleep(0.3)

        return all_articles

    async def get_full_snapshot(self) -> Dict[str, List[str]]:
        """
        Get current snapshot of all portfolio pages without diffing.
        Useful for initial population.
        """
        snapshots = {}

        for fund_slug in PORTFOLIO_URLS.keys():
            companies = await self.scrape_portfolio_page(fund_slug)
            snapshots[fund_slug] = [c.name for c in companies]
            await self._save_snapshot(fund_slug, {c.name.lower() for c in companies})
            await asyncio.sleep(0.3)

        return snapshots


# Convenience function
async def run_portfolio_diff_scraper(
    fund_slugs: Optional[List[str]] = None,
) -> List[NormalizedArticle]:
    """Run portfolio diff scraper and return new company articles."""
    async with PortfolioDiffScraper() as scraper:
        return await scraper.scrape_all(fund_slugs)
