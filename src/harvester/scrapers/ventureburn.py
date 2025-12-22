"""
Ventureburn Scraper - Emerging Markets Startup News.

Ventureburn (ventureburn.com) covers startup news for emerging markets,
with focus on Africa, cryptocurrency, AI, and venture capital.

Key advantages:
- Emerging markets coverage (often missed by US-focused sources)
- AI and crypto startup funding
- Venture capital news
- WordPress RSS feed available

RSS Feed: https://ventureburn.com/feed/

Note: Contains significant crypto/price prediction content.
Filtered for funding-related articles only.
"""

import asyncio
import logging
import feedparser
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle

logger = logging.getLogger(__name__)
from ..fund_matcher import match_fund_name
from ...config.settings import settings


# Ventureburn RSS feeds
VENTUREBURN_FEEDS = {
    "main": "https://ventureburn.com/feed/",
}

# Additional category feeds (if available)
VENTUREBURN_CATEGORIES = [
    "https://ventureburn.com/category/startups/feed/",
    "https://ventureburn.com/category/venture-capital/feed/",
    "https://ventureburn.com/category/ai/feed/",
]

# FIX #47: Fund patterns now consolidated in fund_matcher.py

# Funding keywords
FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "series", "seed", "million", "billion",
    "led by", "investment", "round", "valuation", "venture", "capital",
    "secures", "closes", "bags", "snaps"
]

# Exclude crypto noise (price predictions, exchange reviews)
EXCLUDE_KEYWORDS = [
    "price prediction", "exchange review", "best crypto exchange",
    "how to buy", "trading guide", "price analysis", "referral code",
    "cloud mining", "daily returns"
]


@dataclass
class VentureburnArticle:
    """Single article from Ventureburn."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    author: Optional[str]
    categories: List[str]


class VentureburnScraper:
    """
    Scraper for Ventureburn RSS feed.

    Covers emerging markets startup and VC news.
    Filters out crypto price prediction noise.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_feed(self, feed_url: str) -> List[VentureburnArticle]:
        """Fetch and parse RSS feed with retry logic."""
        for attempt in range(3):
            try:
                response = await self.client.get(feed_url)
                response.raise_for_status()

                feed = feedparser.parse(response.text)

                # Check for malformed feed
                if feed.bozo:
                    logger.warning(f"Malformed Ventureburn feed: {feed.bozo_exception}")
                    return []

                articles = []

                for entry in feed.entries:
                    # Skip entries without valid URLs
                    url = (entry.get('link') or '').strip()
                    if not url or not url.startswith(('http://', 'https://')):
                        continue

                    # Parse publication date with fallbacks and safety
                    pub_date = None
                    for date_field in ['published_parsed', 'updated_parsed', 'created_parsed']:
                        date_tuple = getattr(entry, date_field, None)
                        if date_tuple and len(date_tuple) >= 6:
                            try:
                                pub_date = datetime(*date_tuple[:6], tzinfo=timezone.utc)
                                break
                            except (TypeError, ValueError):
                                continue

                    # Clean description
                    description = entry.get('summary', '') or entry.get('description', '')
                    if description:
                        soup = BeautifulSoup(description, 'lxml')
                        description = soup.get_text(strip=True)[:2000]

                    # Extract categories with safety
                    categories = []
                    if hasattr(entry, 'tags'):
                        try:
                            categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
                        except (TypeError, AttributeError):
                            pass

                    articles.append(VentureburnArticle(
                        title=entry.get('title', ''),
                        url=url,
                        description=description,
                        published=pub_date,
                        author=entry.get('author', None),
                        categories=categories,
                    ))

                return articles

            except httpx.HTTPError as e:
                if attempt < 2:
                    delay = 2 ** attempt
                    logger.warning(f"HTTP error fetching Ventureburn feed (attempt {attempt + 1}/3), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching Ventureburn feed after 3 attempts: {e}")
                    return []
            except Exception as e:
                logger.error(f"Unexpected error fetching Ventureburn feed: {e}", exc_info=True)
                return []

        return []

    async def fetch_all_feeds(self, hours_back: int = 168) -> List[VentureburnArticle]:
        """Fetch main feed and category feeds in parallel."""
        all_articles = []
        seen_urls = set()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

        # OPTIMIZATION: Fetch all feeds in parallel (main + categories)
        all_feed_urls = [VENTUREBURN_FEEDS["main"]] + VENTUREBURN_CATEGORIES
        tasks = [self.fetch_feed(url) for url in all_feed_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                # Category feeds may not exist - log at debug level
                logger.debug(f"Feed fetch failed (may not exist): {result}")
                continue
            for article in result:
                if article.url not in seen_urls:
                    if article.published and article.published < cutoff:
                        continue
                    seen_urls.add(article.url)
                    all_articles.append(article)

        return all_articles

    def is_funding_article(self, article: VentureburnArticle) -> bool:
        """Check if article is about funding (not crypto noise)."""
        text = f"{article.title} {article.description}".lower()

        # Exclude crypto noise
        if any(kw in text for kw in EXCLUDE_KEYWORDS):
            return False

        # Must have funding keywords
        return any(kw in text for kw in FUNDING_KEYWORDS)

    def match_tracked_fund(self, article: VentureburnArticle) -> Optional[str]:
        """Check if article mentions any tracked fund."""
        search_text = f"{article.title} {article.description}"
        # FIX #47: Use centralized fund_matcher instead of local patterns
        return match_fund_name(search_text)

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
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'ads']):
                tag.decompose()

            # Ventureburn article selectors
            for selector in ['.entry-content', '.post-content', 'article', '.content', 'main']:
                content = soup.select_one(selector)
                if content:
                    text = content.get_text(separator='\n', strip=True)
                    if len(text) > 200:
                        return text[:8000]

            return None

        except httpx.HTTPError as e:
            logger.debug(f"HTTP error fetching Ventureburn article {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching Ventureburn article {url}: {e}", exc_info=True)
            return None

    async def scrape_all(self, hours_back: int = 168) -> List[NormalizedArticle]:
        """
        Full scraping pipeline.

        Args:
            hours_back: Look back this many hours (default 7 days)

        Returns:
            List of NormalizedArticle objects for funding news.
        """
        articles = await self.fetch_all_feeds(hours_back=hours_back)

        normalized = []
        for article in articles:
            # Only process funding-related articles (filter crypto noise)
            if not self.is_funding_article(article):
                continue

            # Skip articles without publication dates
            if not article.published:
                logger.debug(f"Skipping article without date: {article.title}")
                continue

            # Fetch full article content
            full_text = await self.fetch_full_article(article.url)
            text = full_text or f"{article.title}\n\n{article.description}"

            fund_slug = self.match_tracked_fund(article)

            # Build tags
            tags = ['ventureburn', 'emerging_markets']
            tags.extend([c.lower().replace(' ', '_') for c in article.categories[:3]])

            normalized.append(NormalizedArticle(
                url=article.url,
                title=article.title,
                text=text,
                published_date=article.published.date(),
                author=article.author or "Ventureburn",
                tags=tags,
                fund_slug=fund_slug or "",
                fetched_at=datetime.now(timezone.utc),
            ))

            # Rate limit between article fetches
            await asyncio.sleep(0.3)

        logger.info(f"Scraped {len(normalized)} funding articles from Ventureburn")
        return normalized


# Convenience function
async def run_ventureburn_scraper(hours_back: int = 168) -> List[NormalizedArticle]:
    """Run Ventureburn scraper and return articles."""
    async with VentureburnScraper() as scraper:
        return await scraper.scrape_all(hours_back=hours_back)
