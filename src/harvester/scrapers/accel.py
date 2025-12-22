"""
Accel Scraper.

Source: https://accel.com/news
Format: HTML

Identify London vs. US vs. India teams.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ...config.funds import FUND_REGISTRY


# CRITICAL: Accel Entertainment is a gaming/gambling company (NYSE: ACEL)
# that must be excluded to avoid false positives with Accel the VC firm.
# See CLAUDE.md: "Accel | accel | Exclude 'Accel Entertainment'"
ACCEL_ENTERTAINMENT_EXCLUSIONS = [
    "accel entertainment",
    "accel gaming",
    "distributed gaming",
    "video gaming terminal",
    "vgt",  # Video Gaming Terminal
    "acel",  # Stock ticker
    "nyse: acel",
    "nyse:acel",
    "casino",
    "slot machine",
    "gaming terminal",
]


class AccelScraper(SimpleHTMLScraper):
    """
    Scraper for Accel news page.

    Key: Identify regional teams (US, London, India).
    """

    # Regional indicators
    REGIONS = {
        "us": [
            "san francisco", "palo alto", "silicon valley", "new york",
            "boston", "los angeles", "seattle", "austin",
        ],
        "london": [
            "london", "uk", "united kingdom", "europe", "european",
            "berlin", "paris", "amsterdam",
        ],
        "india": [
            "india", "bangalore", "bengaluru", "mumbai", "delhi",
            "hyderabad", "chennai", "indian",
        ],
    }

    def __init__(self):
        super().__init__(FUND_REGISTRY["accel"])
        self.base_url = "https://accel.com"

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

                tags = []
                for tag_el in card.select(".tag, .category, [class*='tag'], [class*='region']"):
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

            # Detect regional team
            region = self._detect_region(raw.title, text)
            if region:
                text = f"[ACCEL {region.upper()}]\n{text}"

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

    def _should_filter(self, article: RawArticle) -> bool:
        """
        Check if article should be filtered.

        Extends base class filter to exclude Accel Entertainment articles.
        """
        # First check base class filters (negative keywords from fund config)
        if super()._should_filter(article):
            return True

        # CRITICAL: Filter out Accel Entertainment (gaming company) articles
        # See CLAUDE.md: "Accel | accel | Exclude 'Accel Entertainment'"
        if self._is_accel_entertainment(article.title, article.html):
            logger.info(f"Filtered Accel Entertainment article: {article.title}")
            return True

        return False

    def _is_accel_entertainment(self, title: str, text: str) -> bool:
        """
        Check if article is about Accel Entertainment (gaming company).

        CRITICAL: Accel Entertainment (NYSE: ACEL) is a gaming/gambling company
        that must be excluded to avoid false positives with Accel the VC firm.
        """
        combined = f"{title} {text}".lower()
        return any(term in combined for term in ACCEL_ENTERTAINMENT_EXCLUSIONS)

    def _detect_region(self, title: str, text: str) -> Optional[str]:
        """Detect which Accel regional team is involved."""
        combined = f"{title} {text}".lower()

        # Check each region
        region_scores = {}
        for region, keywords in self.REGIONS.items():
            score = sum(1 for kw in keywords if kw in combined)
            if score > 0:
                region_scores[region] = score

        if not region_scores:
            return None

        # Return region with highest score
        return max(region_scores, key=region_scores.get)


def create_scraper() -> AccelScraper:
    return AccelScraper()
