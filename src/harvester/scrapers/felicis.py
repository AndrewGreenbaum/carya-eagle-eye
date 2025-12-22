"""
Felicis Ventures Scraper.

Source: https://felicis.com/insights
Format: HTML

Look for 'Welcome to the family' keywords.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class FelicisScraper(SimpleHTMLScraper):
    """
    Scraper for Felicis Ventures insights page.

    Key: Look for 'Welcome to the family' signals indicating new portfolio companies.
    """

    # Keywords indicating new investment announcements
    WELCOME_KEYWORDS = [
        "welcome to the family",
        "welcome to the felicis family",
        "we're thrilled to announce",
        "we're excited to announce",
        "proud to announce",
        "joining the pack",
        "newest member",
        "new portfolio company",
        "thrilled to welcome",
        "excited to welcome",
    ]

    def __init__(self):
        super().__init__(FUND_REGISTRY["felicis"])
        self.base_url = "https://felicis.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/insights"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".insight",
            ".insight-card",
            ".post-card",
            "[class*='insight']",
            "[class*='post']",
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

                # Check for welcome keywords in card
                card_text = card.get_text(strip=True).lower()
                tags = []
                if self._has_welcome_signal(title, card_text):
                    tags.append("new_portfolio")

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

            # Flag new portfolio company announcements
            if self._has_welcome_signal(raw.title, text):
                text = f"[NEW PORTFOLIO COMPANY]\n{text}"

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

    def _has_welcome_signal(self, title: str, text: str) -> bool:
        """Check for 'Welcome to the family' signals."""
        combined = f"{title} {text}".lower()
        return any(keyword in combined for keyword in self.WELCOME_KEYWORDS)


def create_scraper() -> FelicisScraper:
    return FelicisScraper()
