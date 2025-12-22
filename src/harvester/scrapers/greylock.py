"""
Greylock Partners Scraper.

Source: https://greylock.com/portfolio-news/
Format: PLAYWRIGHT (JavaScript-rendered content)

NOTE: The Greylock portfolio news page displays ALL content inline.
- Investment announcements are in h2 elements within .item/.item_small divs
- There are NO individual article URLs - all links point back to /portfolio-news/
- Content must be scraped directly from the page, not by following links

This scraper extracts inline investment announcements and creates
virtual article entries from the page content.
"""

import logging
from typing import List, Optional
import hashlib
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

from ..playwright_scraper import PlaywrightScraper
from ..base_scraper import RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY


class GreylockScraper(PlaywrightScraper):
    """
    Scraper for Greylock portfolio news page.

    Uses Playwright to render JavaScript-heavy pages.
    Extracts inline investment announcements (no individual article URLs).
    """

    # Known Greylock partners
    PARTNERS = [
        "reid hoffman",
        "david sze",
        "josh elman",
        "sarah guo",
        "saam motamedi",
        "seth rosenberg",
        "jerry chen",
        "asheem chandna",
        "mike duboe",
        "corinne riley",
    ]

    def __init__(self):
        super().__init__(FUND_REGISTRY["greylock"])
        self.base_url = "https://greylock.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        # Use /portfolio-news/ not /blog/portfolio-news/
        target_url = url or f"{self.base_url}/portfolio-news/"

        # Use Playwright to render the JavaScript content
        html = await self.fetch_rendered(
            target_url,
            wait_selector=".item, .item_small, h2, [class*='item']",
            wait_ms=5000
        )
        return html

    async def parse(self, html: str) -> List[RawArticle]:
        """
        Parse inline investment announcements from the portfolio news page.

        Since all content is inline (no individual article URLs), we create
        virtual articles from the h2 titles and surrounding content.
        """
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_titles = set()

        # Find all investment announcement items
        # Greylock uses .item and .item_small classes with h2 titles
        for item in soup.select(".item, .item_small"):
            # Get the h2 title
            title_el = item.select_one("h2")
            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            # Skip if we've seen this title
            if title in seen_titles:
                continue
            seen_titles.add(title)

            # Check if this is an investment/funding announcement
            if not self._is_investment_news(title):
                continue

            # Get the full text content of this item
            item_text = item.get_text(separator="\n", strip=True)

            # Extract company name from title
            company_name = self._extract_company_name(title)

            # Create a unique URL for this inline content
            # Use hash of company_name + title to prevent collision
            unique_key = f"{company_name}#{title}"
            content_hash = hashlib.md5(unique_key.encode()).hexdigest()[:8]
            virtual_url = f"{self.base_url}/portfolio-news/#{content_hash}"

            # Try to extract author/partner
            author = None
            author_el = item.select_one(
                "[class*='author'], [class*='Author'], .byline, "
                "[class*='partner'], [class*='Partner']"
            )
            if author_el:
                author = author_el.get_text(strip=True)

            # Create article from inline content
            # FIX: Use None for published_date instead of date.today()
            # date.today() caused duplicate detection issues when same content
            # was scraped on different days. Storage layer handles None dates.
            articles.append(RawArticle(
                url=virtual_url,
                title=title,
                html=str(item),
                published_date=None,  # Inline content has no date - let storage handle
                author=author,
                tags=["greylock", "investment", company_name.lower()] if company_name else ["greylock", "investment"],
            ))

        return articles

    def _is_investment_news(self, title: str) -> bool:
        """Check if title indicates an investment announcement."""
        title_lower = title.lower()
        investment_keywords = [
            "investment in",
            "investing in",
            "introducing",
            "our investment",
            "congrats",
            "congratulations",
            "welcome",
        ]
        return any(kw in title_lower for kw in investment_keywords)

    def _extract_company_name(self, title: str) -> Optional[str]:
        """Extract company name from title like 'Our Investment in Acme' or 'Introducing: Acme'."""
        import re

        # Pattern: "Our Investment in CompanyName"
        match = re.search(r'investment\s+in\s+([A-Z][A-Za-z0-9\s]+?)(?:\s*[-:!]|\s*$)', title, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Pattern: "Introducing CompanyName" or "Introducing: CompanyName"
        match = re.search(r'introducing[:\s]+([A-Z][A-Za-z0-9\s]+?)(?:\s*[-:!]|\s*$)', title, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Pattern: "Congrats, CompanyName!"
        match = re.search(r'congrats?,?\s+([A-Z][A-Za-z0-9\s]+?)(?:\s*[!]|\s*$)', title, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return None

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        """
        Normalize inline content - no need to fetch since content is already in HTML.
        """
        # Extract text from the HTML we already have
        soup = BeautifulSoup(raw.html, "lxml")
        text = soup.get_text(separator="\n", strip=True)

        # Add partner flag if applicable
        author = raw.author
        if author and self._is_partner_authored(author):
            text = f"[PARTNER POST: {author}]\n{text}"

        # Add investment context
        text = f"Greylock Investment Announcement: {raw.title}\n\n{text}"

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            author=author,
            tags=raw.tags,
        )

    def _is_partner_authored(self, author: str) -> bool:
        """Check if author is a Greylock partner."""
        author_lower = author.lower()
        return any(partner in author_lower for partner in self.PARTNERS)


def create_scraper() -> GreylockScraper:
    return GreylockScraper()
