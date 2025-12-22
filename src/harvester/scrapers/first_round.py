"""
First Round Capital Scraper.

DISABLED: First Round Review (review.firstround.com) is a thought leadership blog,
NOT a portfolio/funding news page. All content is management advice, case studies,
and methodology articles - not deal announcements.

First Round Capital deals are captured via external sources:
- Brave Search (with partner name queries)
- TechCrunch RSS
- Fortune Term Sheet
- SEC EDGAR Form D filings

This scraper returns empty results to avoid polluting the database with blog posts.
"""

from typing import List, Optional

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY


class FirstRoundScraper(SimpleHTMLScraper):
    """
    First Round Capital scraper - DISABLED.

    First Round Review is thought leadership content, not deal announcements.
    Deals are captured via external sources (Brave Search, TechCrunch, Fortune).
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["first_round"])
        self.base_url = "https://review.firstround.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Return empty HTML - scraper is disabled."""
        return "<html><body><!-- First Round scraper disabled - use external sources --></body></html>"

    async def parse(self, html: str) -> List[RawArticle]:
        """Return empty list - First Round Review has no deal announcements."""
        # First Round Review is a thought leadership blog with:
        # - "Speed as a Habit" - management advice
        # - "25 Micro-Habits of High-Impact Managers" - leadership content
        # - "0-$5M: How to Identify Your ICP" - methodology articles
        #
        # None of this is about First Round's own deals.
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


def create_scraper() -> FirstRoundScraper:
    return FirstRoundScraper()
