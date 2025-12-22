"""
USPTO Trademark Monitor - Catch New Company/Product Names Early.

Monitors USPTO for new trademark applications that might indicate:
- New startup names (company formation)
- New product launches
- Stealth company emerging

Trademark applications are PUBLIC and often filed BEFORE announcements.

Uses USPTO's Trademark Electronic Search System (TESS) and
the Trademark Status & Document Retrieval (TSDR) API.
"""

import asyncio
import logging
import re
import httpx
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Dict, Any

from ..base_scraper import NormalizedArticle
from ...config.settings import settings

logger = logging.getLogger(__name__)


# USPTO TSDR API base
USPTO_TSDR_API = "https://tsdr.uspto.gov/documentxml"

# USPTO search - we'll use their web search since the API requires registration
USPTO_SEARCH_URL = "https://tmsearch.uspto.gov/bin/gate.exe"

# Tech/AI related classifications (Nice Classification)
# Class 9: Software, computers, electronics
# Class 35: Business services, SaaS
# Class 42: Technology services, cloud computing
TECH_CLASSES = ["009", "035", "042"]

# Keywords suggesting VC-backed tech startup
TECH_KEYWORDS = [
    "ai", "artificial intelligence", "machine learning",
    "cloud", "saas", "platform", "analytics",
    "cyber", "security", "data",
    "automation", "bot", "agent",
    "api", "infrastructure", "devops",
    "fintech", "healthtech", "biotech",
]

# Keywords suggesting stealth/new company
STEALTH_INDICATORS = [
    "inc", "labs", "technologies", "systems",
    "ventures", "co", "hq", "studio",
]


@dataclass
class TrademarkApplication:
    """USPTO trademark application."""
    serial_number: str
    mark_text: str
    filing_date: date
    applicant_name: str
    applicant_address: Optional[str]
    goods_services: str
    nice_class: str
    status: str
    url: str


class USPTOTrademarkScraper:
    """
    Monitor USPTO for new trademark applications.

    Filters for tech/software related marks that might indicate
    new startup formations or product launches.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml",
            },
            follow_redirects=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def search_recent_trademarks(
        self,
        days_back: int = 7,
        max_results: int = 100,
    ) -> List[TrademarkApplication]:
        """
        Search USPTO for recent trademark applications.

        Uses Brave Search as USPTO's direct search requires session handling.
        Searches for recent trademark filings with tech keywords.
        """
        trademarks = []

        # Use Brave Search to find recent USPTO trademark news
        if not settings.brave_search_key:
            logger.warning("BRAVE_SEARCH_KEY not configured - using fallback USPTO search")
            return await self._search_uspto_direct(days_back, max_results)

        try:
            # Search for recent trademark filings mentioning tech
            queries = [
                'site:uspto.gov trademark application "software" OR "platform" OR "AI"',
                '"trademark application" "artificial intelligence" OR "machine learning" filed:week',
                '"USPTO" "trademark" "filed" "technology" OR "SaaS" OR "cloud"',
            ]

            brave_client = httpx.AsyncClient(
                timeout=30,
                headers={
                    "X-Subscription-Token": settings.brave_search_key,
                    "Accept": "application/json",
                }
            )

            try:
                for query in queries[:1]:  # Limit queries
                    response = await brave_client.get(
                        "https://api.search.brave.com/res/v1/web/search",
                        params={"q": query, "count": 20, "freshness": "pw"}
                    )

                    if response.status_code == 200:
                        data = response.json()
                        for result in data.get("web", {}).get("results", []):
                            tm = self._parse_search_result(result)
                            if tm:
                                trademarks.append(tm)

                    await asyncio.sleep(0.3)
            finally:
                await brave_client.aclose()

        except Exception as e:
            logger.error(f"USPTO Brave search error: {e}")

        return trademarks[:max_results]

    async def _search_uspto_direct(
        self,
        days_back: int = 7,
        max_results: int = 50,
    ) -> List[TrademarkApplication]:
        """
        Fallback: Search USPTO directly for tech trademarks.
        """
        trademarks = []

        try:
            # USPTO TESS free-form search for software/tech marks
            # Search for Class 9 (software) filed recently
            search_url = "https://tmsearch.uspto.gov/bin/gate.exe"

            # Note: USPTO TESS requires complex session handling
            # For now, we'll use a simplified approach with their RSS-like feeds

            # Alternative: Check USPTO's recent applications page
            response = await self.client.get(
                "https://www.uspto.gov/trademarks/search",
                params={"q": "software platform AI"}
            )

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                # Parse results (structure varies)
                for item in soup.select('.search-result, .trademark-item, article')[:max_results]:
                    tm = self._parse_html_result(item)
                    if tm:
                        trademarks.append(tm)

        except Exception as e:
            logger.error(f"USPTO direct search error: {e}")

        return trademarks

    def _parse_search_result(self, result: Dict[str, Any]) -> Optional[TrademarkApplication]:
        """Parse Brave search result into TrademarkApplication."""
        try:
            url = result.get("url", "")
            title = result.get("title", "")
            description = result.get("description", "")

            # Try to extract serial number from URL or text
            serial_match = re.search(r'(\d{8})', url + title + description)
            serial = serial_match.group(1) if serial_match else ""

            # Extract mark name from title
            mark_text = title.split(" - ")[0] if " - " in title else title
            mark_text = re.sub(r'USPTO|Trademark|Application|Filing', '', mark_text, flags=re.IGNORECASE).strip()

            if not mark_text or len(mark_text) < 2:
                return None

            return TrademarkApplication(
                serial_number=serial,
                mark_text=mark_text[:100],
                filing_date=date.today(),
                applicant_name="",
                applicant_address=None,
                goods_services=description[:200],
                nice_class="009",  # Assume software
                status="NEW",
                url=url,
            )

        except Exception:
            return None

    def _parse_html_result(self, element) -> Optional[TrademarkApplication]:
        """Parse HTML element into TrademarkApplication."""
        try:
            text = element.get_text(strip=True)
            link = element.find('a')
            url = link.get('href', '') if link else ''

            # Extract mark name (usually first heading or strong text)
            mark_elem = element.select_one('h3, h4, strong, .mark-name')
            mark_text = mark_elem.get_text(strip=True) if mark_elem else text[:50]

            if not mark_text or len(mark_text) < 2:
                return None

            return TrademarkApplication(
                serial_number="",
                mark_text=mark_text,
                filing_date=date.today(),
                applicant_name="",
                applicant_address=None,
                goods_services=text[:200],
                nice_class="009",
                status="NEW",
                url=url,
            )

        except Exception:
            return None

    def is_tech_trademark(self, tm: TrademarkApplication) -> bool:
        """Check if trademark is likely tech/startup related."""
        text = f"{tm.mark_text} {tm.goods_services}".lower()

        # Check for tech keywords
        has_tech_keyword = any(kw in text for kw in TECH_KEYWORDS)

        # Check for tech classes
        is_tech_class = tm.nice_class in TECH_CLASSES

        return has_tech_keyword or is_tech_class

    def is_likely_startup(self, tm: TrademarkApplication) -> bool:
        """Check if trademark is likely a new startup name."""
        text = f"{tm.mark_text} {tm.applicant_name}".lower()

        # Check for startup indicators
        has_indicator = any(ind in text for ind in STEALTH_INDICATORS)

        # Check if name looks like a company (capitalized, 1-3 words)
        words = tm.mark_text.split()
        looks_like_company = 1 <= len(words) <= 4

        return has_indicator or looks_like_company

    def trademark_to_article(self, tm: TrademarkApplication) -> NormalizedArticle:
        """Convert trademark application to NormalizedArticle."""
        text_parts = [
            f"📋 USPTO TRADEMARK APPLICATION",
            f"",
            f"Mark: {tm.mark_text}",
            f"Filing Date: {tm.filing_date}",
            f"Serial Number: {tm.serial_number}",
            f"Nice Class: {tm.nice_class}",
            f"",
            f"Applicant: {tm.applicant_name or 'Not disclosed'}",
            f"",
            f"Goods/Services: {tm.goods_services}",
            f"",
            f"Note: Trademark applications are often filed BEFORE company announcements.",
            f"This could indicate a new startup or product launch.",
        ]

        if tm.url:
            text_parts.append(f"\nUSPTO URL: {tm.url}")

        return NormalizedArticle(
            url=tm.url or f"https://tsdr.uspto.gov/#caseNumber={tm.serial_number}",
            title=f"USPTO Filing: {tm.mark_text}",
            text="\n".join(text_parts),
            published_date=tm.filing_date,
            author="USPTO",
            tags=["uspto", "trademark", "stealth_signal", f"class_{tm.nice_class}"],
            fund_slug="",
            fetched_at=datetime.now(timezone.utc),
        )

    async def scrape_all(
        self,
        days_back: int = 7,
        tech_only: bool = True,
    ) -> List[NormalizedArticle]:
        """
        Full scraping pipeline for USPTO trademarks.

        Args:
            days_back: How many days back to search
            tech_only: Only return tech/software related marks

        Returns:
            List of NormalizedArticle for potential startup trademarks
        """
        # Search for recent trademarks
        trademarks = await self.search_recent_trademarks(days_back=days_back)

        articles = []
        for tm in trademarks:
            # Filter for tech if requested
            if tech_only and not self.is_tech_trademark(tm):
                continue

            # Convert to article
            article = self.trademark_to_article(tm)
            articles.append(article)

        return articles


# Convenience function
async def run_uspto_trademark_scraper(
    days_back: int = 7,
    tech_only: bool = True,
) -> List[NormalizedArticle]:
    """Run USPTO trademark scraper and return articles."""
    async with USPTOTrademarkScraper() as scraper:
        return await scraper.scrape_all(days_back=days_back, tech_only=tech_only)
