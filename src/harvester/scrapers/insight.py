"""
Insight Partners Scraper.

Source: https://insightpartners.com/about-us/media/
Format: HTML

Filter 'ScaleUp' vs. standard rounds. Growth-stage focus.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class InsightScraper(SimpleHTMLScraper):
    """
    Scraper for Insight Partners media/news page.

    Growth-stage focus. Distinguish ScaleUp from standard rounds.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["insight"])
        self.base_url = "https://insightpartners.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        target_url = url or f"{self.base_url}/about-us/media/"
        response = await self.client.get(target_url)
        response.raise_for_status()
        return response.text

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".media-item",
            ".news-item",
            ".press-release",
            "[class*='media']",
            "[class*='news']",
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

                # Capture tags for ScaleUp detection
                tags = []
                for tag_el in card.select(".tag, .category, [class*='tag'], [class*='type']"):
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

            # Flag ScaleUp investments
            round_type = self._detect_round_type(raw.title, text)
            if round_type:
                text = f"[{round_type}]\n{text}"

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

    def _detect_round_type(self, title: str, text: str) -> Optional[str]:
        """Detect ScaleUp vs. standard growth investment."""
        combined = f"{title} {text}".lower()

        if "scaleup" in combined or "scale up" in combined:
            return "SCALEUP"
        elif "growth" in combined:
            return "GROWTH"
        elif "buyout" in combined or "acquisition" in combined:
            return "PE/BUYOUT"

        return None


def create_scraper() -> InsightScraper:
    return InsightScraper()
