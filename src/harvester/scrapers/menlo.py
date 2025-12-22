"""
Menlo Ventures Scraper.

Source: https://menlovc.com/perspective/
Format: HTML

Perspective/blog page; focus on AI investments.
"""

import logging
import re
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY

logger = logging.getLogger(__name__)


class MenloScraper(SimpleHTMLScraper):
    """
    Scraper for Menlo Ventures news page.

    Key: Focus on AI-related investments.
    """

    # AI-related keywords for signal detection
    # FIX: Use word boundary matching for short keywords to avoid false positives
    # e.g., "ml" should not match "html"
    AI_KEYWORDS = [
        "artificial intelligence",
        "machine learning",
        "deep learning",
        "neural network",
        "large language model",
        "generative ai",
        "ai-powered",
        "ai-native",
        "ai infrastructure",
        "foundation model",
        "ml ops",
        "mlops",
    ]

    # Short keywords that need word boundary matching
    # FIX: "ml" and "llm" are short enough to cause false positives (e.g., "html")
    AI_KEYWORDS_WORD_BOUNDARY = ["ml", "llm", "ai"]

    def __init__(self):
        super().__init__(FUND_REGISTRY["menlo"])
        self.base_url = "https://menlovc.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/perspective/"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".news-item",
            ".post-card",
            "[class*='news']",
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

            # Flag AI-focused investments
            if self._is_ai_focused(raw.title, text):
                text = f"[AI INVESTMENT]\n{text}"

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

    def _is_ai_focused(self, title: str, text: str) -> bool:
        """Detect AI-focused investment signals.

        FIX: Uses word boundary matching for short keywords like 'ml' and 'llm'
        to avoid false positives (e.g., 'ml' matching 'html').
        """
        combined = f"{title} {text}".lower()

        # Check regular keywords (substring match is fine for longer terms)
        if any(keyword in combined for keyword in self.AI_KEYWORDS):
            return True

        # Check short keywords with word boundaries
        # FIX: Prevents "ml" from matching "html", "llm" from matching partial words
        for keyword in self.AI_KEYWORDS_WORD_BOUNDARY:
            pattern = rf'\b{re.escape(keyword)}\b'
            if re.search(pattern, combined):
                return True

        return False


def create_scraper() -> MenloScraper:
    return MenloScraper()
