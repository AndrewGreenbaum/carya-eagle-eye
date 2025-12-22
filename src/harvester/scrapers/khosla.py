"""
Khosla Ventures Scraper.

DISABLED: The Khosla RSS feed (/posts/rss.xml) contains thought leadership and
opinion pieces, NOT deal announcements:
- "AI or Die" - opinion piece
- "Bloomberg interview with Vinod Khosla" - media appearance
- "Are AI Valuations Bonkers?" - market commentary

Khosla Ventures deals are captured via external sources:
- Brave Search (with partner name queries like "Vinod Khosla investment")
- TechCrunch RSS
- Fortune Term Sheet
- SEC EDGAR Form D filings

This scraper returns empty results to avoid polluting the database with blog posts.
"""

from typing import List, Optional

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY


class KhoslaScraper(SimpleHTMLScraper):
    """
    Khosla Ventures scraper - DISABLED.

    The RSS feed contains thought leadership, not deal announcements.
    Deals are captured via external sources (Brave Search, TechCrunch, Fortune).
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["khosla"])
        self.base_url = "https://www.khoslaventures.com"
        self.rss_url = "https://www.khoslaventures.com/posts/rss.xml"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Return empty content - scraper is disabled."""
        return ""

    async def parse(self, content: str) -> List[RawArticle]:
        """Return empty list - Khosla RSS has no deal announcements."""
        # The RSS feed contains:
        # - "AI or Die" - opinion piece
        # - "Bloomberg interview with Vinod Khosla" - media appearance
        # - "Are AI Valuations Bonkers?" - market commentary
        #
        # None of this is about Khosla's actual investments.
        # Deals are captured via: Brave Search, TechCrunch, Fortune, SEC EDGAR
        return []

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        """Should never be called since parse() returns empty."""
        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text="",
            published_date=raw.published_date,
        )


def create_scraper() -> KhoslaScraper:
    return KhoslaScraper()
