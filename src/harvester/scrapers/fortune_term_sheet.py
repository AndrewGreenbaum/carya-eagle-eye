"""
Fortune Term Sheet RSS Scraper - Monitor Fortune's deal-focused newsletter.

Fortune Term Sheet is a daily newsletter covering venture capital, private equity,
and M&A deals. It's one of the best curated sources for funding news.

RSS Feed: https://fortune.com/section/term-sheet/feed/

STATUS: DISABLED (December 2024)
Fortune has blocked direct RSS access. This scraper will return 0 results.
Deal coverage is provided by alternative sources:
- Brave Search (queries for "Fortune Term Sheet" news)
- TechCrunch RSS
- Axios Pro Rata
"""

import asyncio
import logging
import feedparser
import httpx

logger = logging.getLogger(__name__)
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ..fund_matcher import match_fund_name
from ...config.settings import settings


# Fortune RSS feeds
# NOTE: Fortune has deprecated most RSS feeds. These may return 403/empty.
# Alternative: Use Brave Search with "Fortune Term Sheet" query instead.
FORTUNE_FEEDS = {
    # These URLs are kept for reference but Fortune has blocked direct RSS access
    # as of late 2024. The scraper will return 0 results but won't error.
}

# FIX #47: Fund patterns now consolidated in fund_matcher.py


@dataclass
class FortuneArticle:
    """Single article from Fortune RSS feed."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    author: Optional[str]
    categories: List[str]


class FortuneTermSheetScraper:
    """
    Scraper for Fortune Term Sheet and venture RSS feeds.

    Monitors deal-focused content for funding announcements.
    """

    def __init__(self, feeds: Optional[List[str]] = None):
        """
        Initialize with list of RSS feed URLs.

        Args:
            feeds: List of feed URLs. If None, uses default Fortune feeds.
        """
        self.feeds = feeds or list(FORTUNE_FEEDS.values())
        if not self.feeds:
            logger.warning("Fortune RSS feeds are empty - Fortune has deprecated direct RSS access")
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            }
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_feed(self, feed_url: str) -> List[FortuneArticle]:
        """Fetch and parse a single RSS feed with retry logic."""
        for attempt in range(3):
            try:
                response = await self.client.get(feed_url)
                response.raise_for_status()

                feed = feedparser.parse(response.text)

                # Check for malformed feed
                if feed.bozo:
                    logger.warning(f"Malformed Fortune feed {feed_url}: {feed.bozo_exception}")
                    return []

                items = []
                for entry in feed.entries:
                    # FIX: Skip entries without URL (empty URLs cause downstream issues)
                    if not entry.get('link'):
                        continue

                    # Parse publication date
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        # FIX: Check tuple length before unpacking (crash prevention)
                        if len(entry.published_parsed) >= 6:
                            try:
                                pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                            except (TypeError, ValueError):
                                pub_date = None

                    # Clean description
                    description = entry.get('summary', '') or entry.get('description', '')
                    if description:
                        soup = BeautifulSoup(description, 'lxml')
                        description = soup.get_text(strip=True)

                    # Extract categories
                    # FIX: Add try/except for iteration safety
                    categories = []
                    if hasattr(entry, 'tags'):
                        try:
                            categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
                        except (TypeError, AttributeError):
                            categories = []

                    # Get author
                    author = None
                    if hasattr(entry, 'author'):
                        author = entry.author

                    items.append(FortuneArticle(
                        title=entry.get('title', ''),
                        url=entry.get('link', ''),
                        description=description[:1500],
                        published=pub_date,
                        author=author,
                        categories=categories,
                    ))

                return items

            except httpx.HTTPError as e:
                if attempt < 2:
                    delay = 2 ** attempt
                    logger.warning(f"HTTP error fetching Fortune feed (attempt {attempt + 1}/3), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching Fortune feed after 3 attempts: {e}")
                    return []
            except Exception as e:
                logger.error(f"Unexpected error fetching Fortune feed {feed_url}: {e}", exc_info=True)
                return []

        return []

    async def fetch_all_feeds(self, hours_back: int = 168) -> List[FortuneArticle]:
        """Fetch all configured RSS feeds in parallel."""
        all_items = []
        seen_urls = set()
        # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

        # OPTIMIZATION: Fetch all feeds in parallel instead of sequentially
        tasks = [self.fetch_feed(feed_url) for feed_url in self.feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Feed fetch failed: {result}")
                continue
            for item in result:
                if item.url in seen_urls:
                    continue
                if item.published and item.published < cutoff:
                    continue

                seen_urls.add(item.url)
                all_items.append(item)

        return all_items

    def match_tracked_fund(self, article: FortuneArticle) -> Optional[str]:
        """Check if article mentions any tracked fund."""
        search_text = f"{article.title} {article.description}"
        # FIX #47: Use centralized fund_matcher instead of local patterns
        return match_fund_name(search_text)

    def is_funding_article(self, article: FortuneArticle) -> bool:
        """Check if article is about funding/deals."""
        funding_keywords = [
            "raises", "raised", "funding", "series", "seed", "million", "billion",
            "led by", "investment", "round", "valuation", "venture", "capital",
            "deal", "closes", "closed", "secures", "secured", "lands"
        ]
        text = f"{article.title} {article.description}".lower()
        return any(kw in text for kw in funding_keywords)

    async def fetch_full_article(self, url: str) -> Optional[str]:
        """Fetch full article content from URL."""
        try:
            response = await self.client.get(
                url,
                follow_redirects=True,
                timeout=15,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'lxml')

            # Remove unwanted elements
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                tag.decompose()

            # Fortune article selectors
            for selector in ['.article-body', '.entry-content', 'article', '.content-body', 'main']:
                content = soup.select_one(selector)
                if content:
                    text = content.get_text(separator='\n', strip=True)
                    if len(text) > 200:
                        return text[:8000]

            return None

        # FIX: Handle specific HTTP errors first, then general with exc_info
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching Fortune article {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching Fortune article {url}: {e}", exc_info=True)
            return None

    async def scrape_all(self, hours_back: int = 168) -> List[NormalizedArticle]:
        """
        Full scraping pipeline.

        Args:
            hours_back: Look back this many hours (default 7 days)

        Returns:
            List of normalized articles ready for extraction.

        NOTE: Fortune has blocked RSS access as of December 2024.
        This scraper will return 0 results. Deal coverage provided by:
        - Brave Search ("Fortune Term Sheet" query)
        - TechCrunch, Axios Pro Rata RSS feeds
        """
        if not FORTUNE_FEEDS:
            logger.warning("Fortune RSS feeds disabled (blocked by Fortune). Using alternative sources.")
            return []

        articles = await self.fetch_all_feeds(hours_back=hours_back)

        normalized = []
        for article in articles:
            # Only process funding-related articles
            if not self.is_funding_article(article):
                continue

            fund_slug = self.match_tracked_fund(article)

            # Fetch full article content
            full_text = await self.fetch_full_article(article.url)
            text = full_text or f"{article.title}\n\n{article.description}"

            # FIX: Skip articles without published dates (don't default to today)
            if not article.published:
                continue

            normalized.append(NormalizedArticle(
                url=article.url,
                title=article.title,
                text=text,
                published_date=article.published.date(),
                author=article.author,
                tags=['fortune', 'term_sheet'] + article.categories,
                fund_slug=fund_slug or "",
                # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
                fetched_at=datetime.now(timezone.utc),
            ))

            await asyncio.sleep(0.3)

        return normalized


# Convenience function
async def run_fortune_scraper(hours_back: int = 168) -> List[NormalizedArticle]:
    """Run Fortune Term Sheet scraper and return articles."""
    async with FortuneTermSheetScraper() as scraper:
        return await scraper.scrape_all(hours_back=hours_back)
