"""
Bessemer Venture Partners Scraper.

Source: https://bvp.com/news
Format: HTML

Flag 'BVP Forge' as PE/Buyout, not venture.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class BessemerScraper(SimpleHTMLScraper):
    """
    Scraper for Bessemer Venture Partners news page.

    Key: Flag 'BVP Forge' as PE/Buyout, not venture.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["bessemer"])
        self.base_url = "https://bvp.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/news"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".news-item",
            ".news-card",
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

                # Flag BVP Forge deals
                tags = []
                if "forge" in title.lower():
                    tags.append("bvp_forge")

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

            # Flag BVP Forge as PE
            if self._is_forge_deal(raw.title, text):
                text = f"[BVP FORGE - PE/BUYOUT]\n{text}"

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

    def _is_forge_deal(self, title: str, text: str) -> bool:
        """Check if this is a BVP Forge (PE) deal."""
        combined = f"{title} {text}".lower()
        return "bvp forge" in combined or "bessemer forge" in combined


def create_scraper() -> BessemerScraper:
    return BessemerScraper()
