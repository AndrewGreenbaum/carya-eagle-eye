"""
Tech Funding News Scraper - Premium Funding News Coverage.

Tech Funding News (techfundingnews.com) is one of the best sources for
startup funding news. Covers SaaS, AI, FinTech, and general tech funding.

Key advantages:
- High-quality funding news with deal details
- Multiple category feeds (SaaS, AI, FinTech, etc.)
- Often mentions lead investors by name
- Covers European and US deals
- Fast news cycle - often breaks stories early

RSS Feeds:
- Main: https://techfundingnews.com/feed/
- SaaS: https://techfundingnews.com/category/saas/feed/
- AI: https://techfundingnews.com/category/ai/feed/
- FinTech: https://techfundingnews.com/category/fintech/feed/
"""

import asyncio
import logging
import feedparser
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Dict
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

from ..base_scraper import NormalizedArticle
from ..fund_matcher import match_fund_name
from ...config.settings import settings


# Tech Funding News RSS feeds - category-specific for better signal
TECH_FUNDING_NEWS_FEEDS = {
    "main": "https://techfundingnews.com/feed/",
    "saas": "https://techfundingnews.com/category/saas/feed/",
    "ai": "https://techfundingnews.com/category/ai/feed/",
    "fintech": "https://techfundingnews.com/category/fintech/feed/",
    "cybersecurity": "https://techfundingnews.com/category/cybersecurity/feed/",
}

# FIX #47: Fund patterns now consolidated in fund_matcher.py

# Funding keywords to filter relevant articles
FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "series", "seed", "million", "billion",
    "led by", "investment", "round", "valuation", "venture", "capital",
    "secures", "bags", "snaps", "hits", "closes", "announces"
]

# Enterprise AI keywords
ENTERPRISE_AI_KEYWORDS = [
    "saas", "b2b", "enterprise", "ai", "infrastructure", "devops",
    "security", "cybersecurity", "fintech", "healthtech", "devtools",
    "platform", "api", "cloud", "automation", "agent", "llm"
]


@dataclass
class TechFundingArticle:
    """Single article from Tech Funding News."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    author: Optional[str]
    categories: List[str]
    feed_category: str  # Which feed it came from


class TechFundingNewsScraper:
    """
    Scraper for Tech Funding News RSS feeds.

    High-quality source for startup funding news with excellent
    coverage of lead investors and deal details.
    """

    def __init__(self, feeds: Optional[Dict[str, str]] = None):
        """
        Initialize with RSS feed URLs.

        Args:
            feeds: Dict mapping category to feed URL. If None, uses all default feeds.
        """
        self.feeds = feeds or TECH_FUNDING_NEWS_FEEDS
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

    async def fetch_feed(self, feed_url: str, category: str) -> List[TechFundingArticle]:
        """Fetch and parse a single RSS feed with retry logic."""
        for attempt in range(3):
            try:
                response = await self.client.get(feed_url)
                response.raise_for_status()

                feed = feedparser.parse(response.text)

                # Check for malformed feed
                if feed.bozo:
                    logger.warning(f"Malformed Tech Funding News feed {category}: {feed.bozo_exception}")
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

                    articles.append(TechFundingArticle(
                        title=entry.get('title', ''),
                        url=url,
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
                    logger.warning(f"HTTP error fetching Tech Funding News feed {category} (attempt {attempt + 1}/3), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching Tech Funding News feed {category} after 3 attempts: {e}")
                    return []
            except Exception as e:
                logger.error(f"Unexpected error fetching Tech Funding News feed {category}: {e}", exc_info=True)
                return []

        return []

    async def fetch_all_feeds(self, hours_back: int = 168) -> List[TechFundingArticle]:
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

        return all_articles

    def is_funding_article(self, article: TechFundingArticle) -> bool:
        """Check if article is about funding."""
        text = f"{article.title} {article.description}".lower()
        return any(kw in text for kw in FUNDING_KEYWORDS)

    def is_enterprise_ai(self, article: TechFundingArticle) -> bool:
        """Check if article is about Enterprise AI/B2B."""
        text = f"{article.title} {article.description}".lower()
        categories_text = " ".join(article.categories).lower()
        full_text = f"{text} {categories_text}"
        return any(kw in full_text for kw in ENTERPRISE_AI_KEYWORDS)

    def match_tracked_fund(self, article: TechFundingArticle) -> Optional[str]:
        """Check if article mentions any tracked fund."""
        search_text = f"{article.title} {article.description}"
        # FIX #47: Use centralized fund_matcher instead of local patterns
        return match_fund_name(search_text)

    def extract_funding_amount(self, text: str) -> Optional[str]:
        """Extract funding amount from text."""
        import re
        # Match patterns like $100M, €50M, $1.5B, etc.
        patterns = [
            r'[\$€£][\d,]+(?:\.\d+)?[MBmb](?:illion)?',
            r'[\$€£][\d,]+(?:\.\d+)?\s*(?:million|billion)',
            r'[\d,]+(?:\.\d+)?[MBmb]\s*(?:round|funding|raise)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None

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

            # Tech Funding News article selectors
            for selector in ['.entry-content', '.post-content', 'article', '.content', 'main']:
                content = soup.select_one(selector)
                if content:
                    text = content.get_text(separator='\n', strip=True)
                    if len(text) > 200:
                        return text[:8000]

            return None

        except httpx.HTTPError as e:
            logger.debug(f"HTTP error fetching Tech Funding News article {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching Tech Funding News article {url}: {e}", exc_info=True)
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

            # Fetch full article content
            full_text = await self.fetch_full_article(article.url)
            text = full_text or f"{article.title}\n\n{article.description}"

            fund_slug = self.match_tracked_fund(article)
            is_enterprise = self.is_enterprise_ai(article)
            amount = self.extract_funding_amount(text)

            # Build tags
            tags = ['tech_funding_news', article.feed_category]
            tags.extend(article.categories[:3])  # Add up to 3 article categories
            if is_enterprise:
                tags.append('enterprise_ai')
            if amount:
                tags.append(f'amount:{amount}')

            normalized.append(NormalizedArticle(
                url=article.url,
                title=article.title,
                text=text,
                published_date=article.published.date(),
                author=article.author or "Tech Funding News",
                tags=tags,
                fund_slug=fund_slug or "",
                fetched_at=datetime.now(timezone.utc),
            ))

            # Rate limit between article fetches
            await asyncio.sleep(0.3)

        logger.info(f"Scraped {len(normalized)} funding articles from Tech Funding News")
        return normalized


# Convenience function
async def run_tech_funding_news_scraper(hours_back: int = 168) -> List[NormalizedArticle]:
    """Run Tech Funding News scraper and return articles."""
    async with TechFundingNewsScraper() as scraper:
        return await scraper.scrape_all(hours_back=hours_back)
