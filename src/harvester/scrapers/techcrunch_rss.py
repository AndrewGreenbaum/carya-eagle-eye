"""
TechCrunch RSS Scraper - Monitor TechCrunch Startups feed for funding news.

TechCrunch Startups RSS feed is one of the best sources for early funding
announcements. This scraper polls the RSS feed for new articles mentioning
tracked VC funds.

RSS Feeds:
- Startups: https://techcrunch.com/category/startups/feed/
- Venture: https://techcrunch.com/category/venture/feed/
"""

import asyncio
import feedparser
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
import logging

logger = logging.getLogger(__name__)
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ..fund_matcher import match_fund_name
from ...config.settings import settings
from ...config.funds import FUND_REGISTRY
from ...common.http_client import USER_AGENT_BOT


# TechCrunch RSS feeds
TECHCRUNCH_FEEDS = {
    "startups": "https://techcrunch.com/category/startups/feed/",
    "venture": "https://techcrunch.com/category/venture/feed/",
    "fundraising": "https://techcrunch.com/tag/fundraising/feed/",
}

# FIX #47: Fund patterns now consolidated in fund_matcher.py


@dataclass
class TechCrunchArticle:
    """Single article from TechCrunch RSS feed."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    author: Optional[str]
    categories: List[str]


class TechCrunchScraper:
    """
    Scraper for TechCrunch RSS feeds.

    Monitors startup and venture feeds for funding announcements.
    """

    def __init__(self, feeds: Optional[List[str]] = None):
        """
        Initialize with list of RSS feed URLs.

        Args:
            feeds: List of feed URLs. If None, uses default TechCrunch feeds.
        """
        self.feeds = feeds or list(TECHCRUNCH_FEEDS.values())
        self.client = httpx.AsyncClient(
            timeout=settings.request_timeout,
            headers={"User-Agent": USER_AGENT_BOT},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_feed(self, feed_url: str) -> List[TechCrunchArticle]:
        """Fetch and parse a single RSS feed with retry logic."""
        for attempt in range(3):
            try:
                response = await self.client.get(feed_url)
                response.raise_for_status()

                feed = feedparser.parse(response.text)

                # Check for malformed feed
                if feed.bozo:
                    logger.warning(f"Malformed TechCrunch feed {feed_url}: {feed.bozo_exception}")
                    return []

                items = []
                for entry in feed.entries:
                    # FIX: Skip entries without URLs (empty URLs cause downstream issues)
                    if not entry.get('link'):
                        continue

                    # Parse publication date
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        # FIX: Check tuple length before unpacking (crash prevention)
                        # FIX #14: Use timezone-aware datetime to prevent comparison issues
                        if len(entry.published_parsed) >= 6:
                            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)

                    # Clean description
                    description = entry.get('summary', '')
                    if description:
                        soup = BeautifulSoup(description, 'lxml')
                        description = soup.get_text(strip=True)

                    # Extract categories/tags
                    categories = []
                    if hasattr(entry, 'tags'):
                        categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]

                    # Get author
                    author = None
                    if hasattr(entry, 'author'):
                        author = entry.author

                    # FIX (2026-01): Validate title is not empty/whitespace
                    # Articles without titles lose context for fund matching
                    title = entry.get('title', '').strip()
                    if not title:
                        logger.debug(f"Skipping article with empty title: {entry.get('link', 'unknown')}")
                        continue

                    items.append(TechCrunchArticle(
                        title=title,
                        url=entry.get('link', ''),
                        description=description[:1000],
                        published=pub_date,
                        author=author,
                        categories=categories,
                    ))

                return items

            except httpx.HTTPError as e:
                if attempt < 2:
                    delay = 2 ** attempt
                    logger.warning(f"HTTP error fetching TechCrunch feed (attempt {attempt + 1}/3), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching TechCrunch feed after 3 attempts: {e}")
                    return []
            except Exception as e:
                logger.error(f"Unexpected error fetching TechCrunch feed {feed_url}: {e}", exc_info=True)
                return []

        return []

    async def fetch_all_feeds(self, hours_back: int = 168) -> List[TechCrunchArticle]:
        """Fetch all configured RSS feeds in parallel."""
        all_items = []
        seen_urls = set()
        # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

        # OPTIMIZATION: Fetch all feeds in parallel instead of sequentially
        # (was: sequential with 0.5s delay = 1.5s minimum for 3 feeds)
        tasks = [self.fetch_feed(feed_url) for feed_url in self.feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Feed fetch failed: {result}")
                continue
            for item in result:
                # Skip duplicates and old articles
                if item.url in seen_urls:
                    continue
                if item.published and item.published < cutoff:
                    continue

                seen_urls.add(item.url)
                all_items.append(item)

        return all_items

    def match_tracked_fund(self, article: TechCrunchArticle) -> Optional[str]:
        """Check if article mentions any tracked fund."""
        search_text = f"{article.title} {article.description}"
        # FIX #47: Use centralized fund_matcher instead of local patterns
        return match_fund_name(search_text)

    def is_funding_article(self, article: TechCrunchArticle) -> bool:
        """Check if article is about funding."""
        funding_keywords = [
            "raises", "raised", "funding", "series a", "series b", "series c",
            "series d", "seed", "million", "billion", "led by", "investment",
            "round", "valuation", "venture", "capital"
        ]
        text = f"{article.title} {article.description}".lower()
        return any(kw in text for kw in funding_keywords)

    async def fetch_full_article(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch full article content from URL with retry logic.

        FIX: Added retry logic for transient HTTP errors (5xx, timeouts).
        """
        for attempt in range(max_retries):
            try:
                response = await self.client.get(
                    url,
                    follow_redirects=True,
                    timeout=settings.request_timeout,
                )

                # 4xx errors - don't retry (article doesn't exist/paywall)
                if 400 <= response.status_code < 500:
                    logger.debug(f"HTTP {response.status_code} for {url} - not retrying")
                    return None

                # 5xx errors - retry with backoff
                if response.status_code >= 500:
                    if attempt < max_retries - 1:
                        delay = 2 ** attempt
                        logger.warning(f"HTTP {response.status_code} fetching {url}, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"HTTP {response.status_code} fetching {url} after {max_retries} attempts")
                        return None

                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'lxml')

                # Remove unwanted elements
                for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                    tag.decompose()

                # TechCrunch article selectors
                selectors_tried = ['.article-content', '.entry-content', 'article', '.post-content', 'main']
                for selector in selectors_tried:
                    content = soup.select_one(selector)
                    if content:
                        text = content.get_text(separator='\n', strip=True)
                        if len(text) > 200:
                            return text[:8000]

                # FIX (2026-01): Log when all selectors fail (helps debug site changes)
                logger.debug(
                    f"No content found for TechCrunch article {url} - "
                    f"all selectors failed: {selectors_tried}"
                )
                return None

            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{max_retries}), retrying in {delay}s")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Timeout fetching {url} after {max_retries} attempts")
                    return None
            except httpx.HTTPError as e:
                logger.debug(f"HTTP error fetching TechCrunch article {url}: {e}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error fetching TechCrunch article {url}: {e}", exc_info=True)
                return None

        return None

    async def scrape_all(self, hours_back: int = 168) -> List[NormalizedArticle]:
        """
        Full scraping pipeline with parallel article fetching.

        Args:
            hours_back: Look back this many hours (default 7 days)

        Returns:
            List of normalized articles ready for extraction.

        OPTIMIZED: Parallel article fetching with semaphore (was sequential).
        """
        articles = await self.fetch_all_feeds(hours_back=hours_back)

        # Filter to funding articles first
        funding_articles = [a for a in articles if self.is_funding_article(a)]

        if not funding_articles:
            return []

        # OPTIMIZATION: Parallel article fetching with rate limiting
        # Use 2x the default for TechCrunch as it handles high concurrency well
        semaphore = asyncio.Semaphore(settings.max_concurrent_articles * 2)

        async def fetch_article_with_limit(article: TechCrunchArticle) -> tuple:
            """Fetch article content with semaphore-based rate limiting.

            FIX: Moved delay outside semaphore to avoid blocking other concurrent tasks.
            """
            async with semaphore:
                full_text = await self.fetch_full_article(article.url)
            # Small delay AFTER releasing semaphore for politeness
            await asyncio.sleep(0.1)
            return (article, full_text)

        # Fetch all articles in parallel
        tasks = [fetch_article_with_limit(article) for article in funding_articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        normalized = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Article fetch failed: {result}")
                continue

            article, full_text = result

            # FIX: Use today as fallback for articles without publication dates
            # (was skipping these articles, losing 5-10% of content)
            # Since we're fetching fresh from RSS, missing date likely means today
            published_date = None
            if article.published:
                published_date = article.published.date()
            else:
                published_date = date.today()
                logger.warning(f"Article missing date, using today: {article.title[:60]}...")

            # Check for tracked fund mentions
            fund_slug = self.match_tracked_fund(article)

            text = full_text or f"{article.title}\n\n{article.description}"

            normalized.append(NormalizedArticle(
                url=article.url,
                title=article.title,
                text=text,
                published_date=published_date,
                author=article.author,
                tags=['techcrunch'] + article.categories,
                fund_slug=fund_slug or "",
                # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
                fetched_at=datetime.now(timezone.utc),
            ))

        return normalized


# Convenience function
async def run_techcrunch_scraper(hours_back: int = 168) -> List[NormalizedArticle]:
    """Run TechCrunch scraper and return articles."""
    async with TechCrunchScraper() as scraper:
        return await scraper.scrape_all(hours_back=hours_back)
