"""
GV (Google Ventures) Scraper.

Source: https://gv.com/news/
Format: HTML

CRITICAL: Exclude 'Visionary Holdings' (Ticker: GV).
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class GVScraper(SimpleHTMLScraper):
    """
    Scraper for GV (Google Ventures) news page.

    CRITICAL: Must exclude 'Visionary Holdings' which trades as NYSE:GV.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["gv"])
        self.base_url = "https://gv.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/news/"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".news-item",
            ".post",
            "[class*='news']",
            "[class*='article']",
            ".card",
        ]

        for selector in selectors:
            for card in soup.select(selector):
                title_el = card.select_one("h2, h3, h4, .title, [class*='title'], a")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                if not title or len(title) < 10:
                    continue

                # CRITICAL: Skip Visionary Holdings articles
                if self._is_visionary_holdings(title, str(card)):
                    continue

                link_el = card.select_one("a[href]")
                if not link_el:
                    link_el = card.find_parent("a")
                if not link_el:
                    continue

                url = link_el.get("href", "")

                if url and not url.startswith("http"):
                    url = f"{self.base_url}{url}"

                if url in seen_urls or not url:
                    continue
                seen_urls.add(url)

                date_el = card.select_one("time, .date, [class*='date']")
                pub_date = None
                if date_el:
                    date_str = date_el.get("datetime") or date_el.get_text(strip=True)
                    pub_date = self._parse_date(date_str)

                tags = []
                for tag_el in card.select(".tag, .category, [class*='tag']"):
                    tag_text = tag_el.get_text(strip=True).lower()
                    if tag_text:
                        tags.append(tag_text)

                articles.append(RawArticle(
                    url=url,
                    title=title,
                    html=str(card),
                    published_date=pub_date,
                    tags=tags,
                ))

        return articles

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        try:
            full_html = await self.fetch(raw.url)
            text = self._extract_text(full_html)

            # Double-check: Skip if Visionary Holdings
            if self._is_visionary_holdings(raw.title, text):
                text = ""  # Will be filtered as empty

        except Exception as e:
            logger.error(f"Error fetching {raw.url}: {e}", exc_info=True)
            text = self._extract_text(raw.html)

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            tags=raw.tags,
        )

    def _is_visionary_holdings(self, title: str, text: str) -> bool:
        """
        CRITICAL: Check if content is about Visionary Holdings (NYSE:GV).

        Must exclude to avoid confusion with Google Ventures.
        """
        combined = f"{title} {text}".lower()
        exclusion_keywords = [
            "visionary holdings",
            "nyse:gv",
            "nyse: gv",
            "ticker: gv",
            "stock: gv",
            "gv ticker",
            "visionary group",
        ]
        return any(keyword in combined for keyword in exclusion_keywords)


def create_scraper() -> GVScraper:
    return GVScraper()
