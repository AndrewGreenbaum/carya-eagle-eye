"""
Crunchbase News RSS Scraper - Monitor Crunchbase funding news.

Crunchbase News (news.crunchbase.com) is the editorial arm of Crunchbase,
providing in-depth coverage of startup funding rounds, exits, and trends.

Key advantages:
- Direct access to Crunchbase data and analysis
- High-quality funding news with detailed deal information
- Often includes specific funding amounts and lead investors
- Covers both early-stage and late-stage deals
- Fast news cycle for funding announcements

RSS Feeds:
- Main: https://news.crunchbase.com/feed/
- Venture: https://news.crunchbase.com/venture/feed/
- Startups: https://news.crunchbase.com/startups/feed/
"""

import asyncio
import logging
import feedparser
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Dict
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ..fund_matcher import match_fund_name
from ...config.settings import settings

logger = logging.getLogger(__name__)


# Crunchbase News RSS feeds - funding focused
CRUNCHBASE_NEWS_FEEDS = {
    "main": "https://news.crunchbase.com/feed/",
    "venture": "https://news.crunchbase.com/venture/feed/",
    "startups": "https://news.crunchbase.com/startups/feed/",
}

# Funding keywords to filter relevant articles
FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "series a", "series b", "series c",
    "series d", "seed", "million", "billion", "led by", "investment",
    "round", "valuation", "venture", "capital", "secures", "closes",
    "announces", "backs", "invests"
]

# Enterprise AI keywords for tagging
ENTERPRISE_AI_KEYWORDS = [
    "saas", "b2b", "enterprise", "ai", "infrastructure", "devops",
    "security", "cybersecurity", "fintech", "healthtech", "devtools",
    "platform", "api", "cloud", "automation", "agent", "llm"
]


@dataclass
class CrunchbaseArticle:
    """Single article from Crunchbase News RSS feed."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    author: Optional[str]
    categories: List[str]
    feed_category: str


class CrunchbaseNewsScraper:
    """
    Scraper for Crunchbase News RSS feeds.

    Monitors Crunchbase editorial content for funding announcements.
    High-quality source with excellent deal details.
    """

    def __init__(self, feeds: Optional[Dict[str, str]] = None):
        """
        Initialize with RSS feed URLs.

        Args:
            feeds: Dict mapping category to feed URL. If None, uses all default feeds.
        """
        self.feeds = feeds or CRUNCHBASE_NEWS_FEEDS
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

    async def fetch_feed(self, feed_url: str, category: str) -> List[CrunchbaseArticle]:
        """Fetch and parse a single RSS feed with retry logic."""
        for attempt in range(3):
            try:
                response = await self.client.get(feed_url)
                response.raise_for_status()

                feed = feedparser.parse(response.text)

                # Check for malformed feed
                if feed.bozo:
                    logger.warning(f"Malformed Crunchbase News feed {category}: {feed.bozo_exception}")
                    return []

                articles = []

                for entry in feed.entries:
                    # Skip entries without valid URLs
                    url = (entry.get('link') or '').strip()
                    if not url or not url.startswith(('http://', 'https://')):
                        continue

                    # Parse publication date with fallbacks
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

                    # Extract categories
                    categories = []
                    if hasattr(entry, 'tags'):
                        try:
                            categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
                        except (TypeError, AttributeError):
                            pass

                    articles.append(CrunchbaseArticle(
                        title=entry.get('title', ''),
                        url=entry.get('link', ''),
                        description=description,
                        published=pub_date,
                        author=entry.get('author', None),
                        categories=categories,
                        feed_category=category,
                    ))

                return articles

            except httpx.HTTPError as e:
                if attempt < 2:
                    delay = 2 ** attempt
                    logger.warning(f"HTTP error fetching Crunchbase News feed {category} (attempt {attempt + 1}/3), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching Crunchbase News feed {category} after 3 attempts: {e}")
                    return []
            except Exception as e:
                logger.error(f"Unexpected error fetching Crunchbase News feed {category}: {e}", exc_info=True)
                return []

        return []

    async def fetch_all_feeds(self, hours_back: int = 168) -> List[CrunchbaseArticle]:
        """Fetch all configured RSS feeds in parallel."""
        all_articles = []
        seen_urls = set()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

        # OPTIMIZATION: Fetch all feeds in parallel instead of sequentially
        tasks = [
            self.fetch_feed(feed_url, category)
            for category, feed_url in self.feeds.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Feed fetch failed: {result}")
                continue
            for article in result:
                if article.url in seen_urls:
                    continue
                if article.published and article.published < cutoff:
                    continue

                seen_urls.add(article.url)
                all_articles.append(article)

        logger.info(f"Fetched {len(all_articles)} unique articles from Crunchbase News")
        return all_articles

    def is_funding_article(self, article: CrunchbaseArticle) -> bool:
        """Check if article is about funding."""
        text = f"{article.title} {article.description}".lower()
        return any(kw in text for kw in FUNDING_KEYWORDS)

    def is_enterprise_ai(self, article: CrunchbaseArticle) -> bool:
        """Check if article is about Enterprise AI/B2B."""
        text = f"{article.title} {article.description}".lower()
        categories_text = " ".join(article.categories).lower()
        full_text = f"{text} {categories_text}"
        return any(kw in full_text for kw in ENTERPRISE_AI_KEYWORDS)

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

            # Crunchbase News article selectors
            for selector in ['.article-content', '.entry-content', 'article', '.post-content', 'main']:
                content = soup.select_one(selector)
                if content:
                    text = content.get_text(separator='\n', strip=True)
                    if len(text) > 200:
                        return text[:8000]

            return None

        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching Crunchbase News article {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching Crunchbase News article {url}: {e}", exc_info=True)
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
            # Only process funding-related articles
            if not self.is_funding_article(article):
                continue

            # Skip articles without publication dates
            if not article.published:
                logger.debug(f"Skipping article without date: {article.title}")
                continue

            # Match fund using centralized fund matcher
            full_content = f"{article.title} {article.description}"
            fund_slug = match_fund_name(full_content)

            is_enterprise = self.is_enterprise_ai(article)

            # Fetch full article content
            full_text = await self.fetch_full_article(article.url)
            text = full_text or f"{article.title}\n\n{article.description}"

            # Build tags
            tags = ['crunchbase_news', article.feed_category]
            tags.extend(article.categories[:3])
            if is_enterprise:
                tags.append('enterprise_ai')

            normalized.append(NormalizedArticle(
                url=article.url,
                title=article.title,
                text=text,
                published_date=article.published.date(),
                author=article.author or "Crunchbase News",
                tags=tags,
                fund_slug=fund_slug or "",
                fetched_at=datetime.now(timezone.utc),
            ))

            # Rate limit between article fetches
            await asyncio.sleep(0.3)

        logger.info(f"Scraped {len(normalized)} funding articles from Crunchbase News")
        return normalized


# Convenience function
async def run_crunchbase_news_scraper(hours_back: int = 168) -> List[NormalizedArticle]:
    """Run Crunchbase News scraper and return articles."""
    async with CrunchbaseNewsScraper() as scraper:
        return await scraper.scrape_all(hours_back=hours_back)
