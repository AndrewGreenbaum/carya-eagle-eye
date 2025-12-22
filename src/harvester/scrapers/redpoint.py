"""
Redpoint Ventures Scraper.

Source: https://www.redpoint.com/content-hub/
Format: PLAYWRIGHT (Gatsby/React - requires JavaScript rendering)

The content hub has funding news articles with card-module classes.
Filter for articles linking to /content-hub/category/funding-news/.

NOTE: This is a Gatsby/React site - requires Playwright to render
the dynamic card content. Simple HTML parsing returns empty results.
"""

import logging
from datetime import date
from typing import List, Optional
from bs4 import BeautifulSoup

from ..playwright_scraper import PlaywrightScraper
from ..base_scraper import RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY

logger = logging.getLogger(__name__)


class RedpointScraper(PlaywrightScraper):
    """
    Scraper for Redpoint Ventures content hub.

    Key: Uses www.redpoint.com (redirects from non-www).
    Uses Playwright to render Gatsby/React content.
    Looks for card-module elements with funding news.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["redpoint"])
        # IMPORTANT: Must use www. prefix - site redirects without it
        self.base_url = "https://www.redpoint.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        target_url = url or f"{self.base_url}/content-hub/"

        # Use Playwright to render the Gatsby/React content
        html = await self.fetch_rendered(
            target_url,
            wait_selector="[class*='card'], [class*='Card'], article, .content-item",
            wait_ms=3000  # Give Gatsby time to hydrate
        )
        return html

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        # Updated selectors for Redpoint's Gatsby-based site
        selectors = [
            "[class*='card-module']",  # Main card container
            "[class*='Card']",
            "article",
            ".content-item",
        ]

        for selector in selectors:
            for card in soup.select(selector):
                # Find title - Redpoint uses card-module--title class
                title_el = card.select_one(
                    "[class*='title'], h2, h3, h4, "
                    "[class*='Title'], [class*='headline']"
                )
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                if not title or len(title) < 10:
                    continue

                # Find link - prioritize content-hub links
                link_el = card.select_one(
                    "a[href*='/content-hub/'], "
                    "a[href*='/blog/'], "
                    "[class*='cardLink'], "
                    "a[href]"
                )
                if not link_el:
                    link_el = card.find_parent("a")
                if not link_el:
                    continue

                url = link_el.get("href", "")

                # Make absolute URL
                if url and not url.startswith("http"):
                    url = f"{self.base_url}{url}"

                # Skip category pages and non-content URLs
                if "/category/" in url and "funding-news" not in url:
                    continue
                if url in seen_urls or not url:
                    continue

                # Must be a Redpoint content URL
                if "redpoint.com" not in url:
                    continue

                seen_urls.add(url)

                # Extract date if available
                date_el = card.select_one("time, .date, [class*='date']")
                pub_date = None
                if date_el:
                    date_str = date_el.get("datetime") or date_el.get_text(strip=True)
                    pub_date = self._parse_date(date_str)

                # Extract category/overline for tagging
                tags = []
                overline_el = card.select_one(
                    "[class*='overline'], [class*='category'], "
                    "[class*='tag'], .category"
                )
                if overline_el:
                    tag_text = overline_el.get_text(strip=True).lower()
                    if tag_text:
                        tags.append(tag_text)

                # Check if this is funding news
                card_text = card.get_text(strip=True).lower()
                is_funding_news = (
                    "funding" in card_text or
                    "series" in card_text or
                    "investment" in card_text or
                    "leading" in title.lower() or
                    any("funding" in t for t in tags)
                )

                if is_funding_news:
                    tags.append("funding_news")

                articles.append(RawArticle(
                    url=url,
                    title=title,
                    html=str(card),
                    published_date=pub_date,
                    tags=tags,
                ))

        logger.info(f"Redpoint: Found {len(articles)} articles from content hub")
        return articles

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string to date object (consistent with base class)."""
        if not date_str:
            return None

        from datetime import datetime

        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%B %d, %Y",
            "%b %d, %Y",
            "%m/%d/%Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.date()
            except ValueError:
                continue

        return None

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        # OPTIMIZED: Use the card HTML we already scraped instead of re-fetching
        # This reduces runtime from 143s to ~3s (avoids 14 extra Playwright fetches)
        text = self._extract_text(raw.html)

        # Flag funding news articles
        if "funding_news" in (raw.tags or []):
            text = f"[FUNDING NEWS]\n{text}"

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            tags=raw.tags,
        )

    def _extract_text(self, html: str) -> str:
        """Extract readable text from HTML."""
        soup = BeautifulSoup(html, "lxml")

        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()

        # Get text
        text = soup.get_text(separator="\n", strip=True)

        # Clean up whitespace
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        return "\n".join(lines)


def create_scraper() -> RedpointScraper:
    return RedpointScraper()
