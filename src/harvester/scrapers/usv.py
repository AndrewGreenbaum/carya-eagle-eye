"""
Union Square Ventures (USV) Scraper.

Source: https://usv.com/blog
Format: HTML

NLP must read body for 'We are leading...' phrasing.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup
import re

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


class USVScraper(SimpleHTMLScraper):
    """
    Scraper for Union Square Ventures blog.

    Key: Use NLP to detect 'We are leading...' phrasing to identify lead investments.
    """

    # Patterns indicating USV is leading
    # FIX: Made patterns more context-aware to avoid false positives
    # - First-person patterns (we/our) are safe since this is USV's blog
    # - Removed generic "leading a Series" which could match other funds
    # - All patterns now require explicit USV context or first-person voice
    LEAD_PATTERNS = [
        # First-person patterns (safe - this is USV's blog)
        r"we(?:'re| are) leading",
        r"we led (?:the |a |)",
        r"we're excited to lead",
        r"we are pleased to lead",
        r"our investment leads",
        r"our firm (?:is |)leading",
        # Explicit USV patterns
        r"usv (?:is |)lead(?:s|ing)",
        r"led by (?:usv|union square)",
        r"union square ventures (?:is |)leading",
        r"union square ventures led",
        r"usv led (?:the |a |)",
    ]

    def __init__(self):
        super().__init__(FUND_REGISTRY["usv"])
        self.base_url = "https://usv.com"
        self._lead_pattern = re.compile(
            "|".join(self.LEAD_PATTERNS),
            re.IGNORECASE
        )

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/blog"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        selectors = [
            "article",
            ".blog-post",
            ".post",
            "[class*='blog']",
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

                # Extract author (important for USV partner posts)
                author = None
                author_el = card.select_one(".author, [class*='author'], .byline")
                if author_el:
                    author = author_el.get_text(strip=True)

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
                    author=author,
                    tags=tags,
                ))

        return articles

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        try:
            full_html = await self.fetch(raw.url)
            text = self._extract_text(full_html)

            # Detect if USV is leading using NLP patterns
            is_leading = self._detect_lead_investment(text)
            if is_leading:
                text = f"[USV IS LEADING]\n{text}"

        except Exception as e:
            logger.error(f"Error fetching {raw.url}: {e}", exc_info=True)
            text = self._extract_text(raw.html)

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            author=raw.author,
            tags=raw.tags,
        )

    def _detect_lead_investment(self, text: str) -> bool:
        """
        Use NLP patterns to detect 'We are leading...' phrasing.

        Returns True if USV appears to be leading the investment.
        """
        return bool(self._lead_pattern.search(text))


def create_scraper() -> USVScraper:
    return USVScraper()
