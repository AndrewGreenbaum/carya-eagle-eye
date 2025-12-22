"""
Index Ventures Scraper.

Source: https://indexventures.com/perspectives
Format: HTML

Distinguish 'Double Down' (follow-on) from new Leads.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class IndexVenturesScraper(SimpleHTMLScraper):
    """
    Scraper for Index Ventures perspectives/news page.

    Key: Distinguish 'Double Down' (follow-on) from new Leads.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["index"])
        self.base_url = "https://indexventures.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        target_url = url or f"{self.base_url}/perspectives"
        response = await self.client.get(target_url)
        response.raise_for_status()
        return response.text

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".perspective-card",
            ".post-card",
            "[class*='article']",
            "[class*='card']",
            ".insight",
        ]

        for selector in selectors:
            for card in soup.select(selector):
                title_el = card.select_one("h2, h3, h4, .title, [class*='title']")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                if not title or len(title) < 10:
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

                # Extract tags to identify "Double Down" follow-ons
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

            # Flag follow-on investments
            is_follow_on = self._is_follow_on(raw.title, text)
            if is_follow_on:
                text = f"[FOLLOW-ON INVESTMENT]\n{text}"

        except Exception as e:
            logger.warning(f"Error fetching {raw.url}: {e}")
            text = self._extract_text(raw.html)

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            tags=raw.tags,
        )

    def _is_follow_on(self, title: str, text: str) -> bool:
        """Detect if this is a follow-on investment vs. new lead."""
        combined = f"{title} {text}".lower()
        # NOTE: Removed "series b/c/d" - these are often NEW leads, not follow-ons
        # The extractor will verify lead status separately
        follow_on_signals = [
            "double down",
            "follow-on",
            "follow on",
            "additional investment",
            "continued support",
            "expanding our investment",
        ]
        return any(signal in combined for signal in follow_on_signals)


def create_scraper() -> IndexVenturesScraper:
    return IndexVenturesScraper()
