"""
Sequoia Capital Scraper.

Source: https://sequoiacap.com/stories
Format: HTML (converted from Playwright - simpler HTTP approach)

Stories/news page with investment announcements.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class SequoiaScraper(SimpleHTMLScraper):
    """
    Scraper for Sequoia Capital news and portfolio announcements.

    Uses simple HTTP requests - converted from Playwright for Railway compatibility.
    Distinguishes from HongShan/Peak XV (formerly Sequoia China/India).
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["sequoia"])
        self.base_url = "https://sequoiacap.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/stories"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".story-card",
            "[class*='story']",
            "[class*='Story']",
            "[class*='post']",
            "[class*='Post']",
            "[class*='Card']",
            "[class*='card']",
            "a[href*='/article/']",
            "a[href*='/story/']",
        ]

        for selector in selectors:
            for card in soup.select(selector):
                title_el = card.select_one("h2, h3, h4, .title, [class*='title'], [class*='Title']")
                if not title_el:
                    # If card is an <a> tag, try getting title from its text
                    if card.name == "a":
                        title = card.get_text(strip=True)
                    else:
                        continue
                else:
                    title = title_el.get_text(strip=True)

                if not title or len(title) < 10:
                    continue

                # Filter out HongShan/Peak XV content
                if self._is_non_us(title):
                    continue

                # Extract link
                link_el = card.select_one("a[href]")
                if not link_el:
                    if card.name == "a":
                        link_el = card
                    else:
                        link_el = card.find_parent("a")
                if not link_el:
                    continue

                url = link_el.get("href", "")

                if url and not url.startswith("http"):
                    url = f"{self.base_url}{url}"

                # Skip non-sequoia URLs
                if "sequoiacap.com" not in url:
                    continue

                if url in seen_urls or not url:
                    continue
                seen_urls.add(url)

                # Extract date
                date_el = card.select_one("time, .date, [class*='date'], [class*='Date']")
                pub_date = None
                if date_el:
                    date_str = date_el.get("datetime") or date_el.get_text(strip=True)
                    pub_date = self._parse_date(date_str)

                articles.append(RawArticle(
                    url=url,
                    title=title,
                    html=str(card),
                    published_date=pub_date,
                ))

        return articles

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        try:
            full_html = await self.fetch(raw.url)
            soup = BeautifulSoup(full_html, "lxml")

            content_el = soup.select_one(
                "article, .story-content, .post-content, [class*='Content'], main"
            )

            if content_el:
                for el in content_el.select("nav, header, footer, .share, script, style"):
                    el.decompose()
                text = content_el.get_text(separator="\n", strip=True)
            else:
                text = self._extract_text(full_html)

            author = None
            author_el = soup.select_one(".author, [class*='author'], [class*='Author']")
            if author_el:
                author = author_el.get_text(strip=True)

        except Exception as e:
            logger.error(f"Error fetching {raw.url}: {e}", exc_info=True)
            text = self._extract_text(raw.html)
            author = None

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            author=author,
        )

    def _is_non_us(self, title: str) -> bool:
        """Check if content is from HongShan or Peak XV (non-US)."""
        non_us_keywords = [
            "hongshan", "peak xv", "sequoia india", "sequoia china",
            "sequoia southeast asia", "sequoia sea"
        ]
        title_lower = title.lower()
        return any(kw in title_lower for kw in non_us_keywords)


def create_scraper() -> SequoiaScraper:
    return SequoiaScraper()
