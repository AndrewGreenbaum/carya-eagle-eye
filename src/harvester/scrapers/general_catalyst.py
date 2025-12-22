"""
General Catalyst Scraper.

Source: https://www.generalcatalyst.com/stories
Format: HTML

Stories page for news; watch for 'Hatching/Co-creation' (Incubations).
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class GeneralCatalystScraper(SimpleHTMLScraper):
    """
    Scraper for General Catalyst news page.

    Key: Watch for 'Hatching/Co-creation' (Incubations).
    """

    # Keywords indicating incubation/co-creation
    INCUBATION_KEYWORDS = [
        "hatching",
        "co-creation",
        "co-founded",
        "incubat",  # matches incubation, incubating, incubate
        "studio",
        "built with gc",
        "company creation",
        "company building",
        "venture creation",
        "co-built",
    ]

    def __init__(self):
        super().__init__(FUND_REGISTRY["general_catalyst"])
        self.base_url = "https://www.generalcatalyst.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        target_url = url or f"{self.base_url}/stories"
        response = await self.client.get(target_url)
        response.raise_for_status()
        return response.text

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".news-item",
            ".post-card",
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

                # Check for incubation signals in card
                card_text = card.get_text(strip=True).lower()
                tags = []
                if self._is_incubation(title, card_text):
                    tags.append("incubation")

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

            # Flag incubation/co-creation investments
            if self._is_incubation(raw.title, text):
                text = f"[GC INCUBATION/CO-CREATION]\n{text}"

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

    def _is_incubation(self, title: str, text: str) -> bool:
        """Check for incubation/co-creation signals."""
        combined = f"{title} {text}".lower()
        return any(keyword in combined for keyword in self.INCUBATION_KEYWORDS)


def create_scraper() -> GeneralCatalystScraper:
    return GeneralCatalystScraper()
