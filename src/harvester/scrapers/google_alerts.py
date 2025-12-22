"""
Google Alerts RSS Scraper - Monitor funding news via Google Alerts.

Google Alerts can be configured to output RSS feeds for any search query.
This scraper polls those RSS feeds for new funding announcements.

SETUP:
1. Go to https://google.com/alerts
2. Create alerts for each tracked fund (e.g., "Sequoia Capital" "led" "funding")
3. Set delivery to "RSS feed" (click "Show options")
4. Copy the RSS feed URL
5. Add to GOOGLE_ALERTS_FEEDS in this file or via environment variable

RECOMMENDED: Create all 25 alerts (7 grouped + 18 individual) for maximum coverage.
"""

import asyncio
import logging
import feedparser
import httpx

logger = logging.getLogger(__name__)
from dataclasses import dataclass
from datetime import datetime, date, timezone
from urllib.parse import urlparse, parse_qs
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ..fund_matcher import match_fund_name
from ...config.settings import settings


# Concurrency limits for parallel fetching
MAX_CONCURRENT_FEEDS = 5
MAX_CONCURRENT_ARTICLES = 10


# OPTIMIZED: 7 grouped Google Alerts queries
# Creates comprehensive coverage with fewer alerts to manage
GROUPED_ALERT_QUERIES = {
    # Group A: Top-tier funds (Sequoia, a16z, Benchmark, Founders Fund)
    "vc_group_a": '("Sequoia Capital" OR "a16z" OR "Andreessen Horowitz" OR "Benchmark" OR "Founders Fund") "led" funding',

    # Group B: Major funds (Khosla, Index, Insight, Bessemer, Greylock)
    "vc_group_b": '("Khosla Ventures" OR "Index Ventures" OR "Insight Partners" OR "Bessemer" OR "Greylock") "led" funding',

    # Group C: Growth funds (GV, Menlo, USV, Thrive)
    "vc_group_c": '("GV" OR "Google Ventures" OR "Menlo Ventures" OR "USV" OR "Union Square Ventures" OR "Thrive Capital") "led" funding',

    # Group D: Early-stage funds (Accel, Felicis, General Catalyst, First Round, Redpoint)
    "vc_group_d": '("Accel" OR "Felicis" OR "General Catalyst" OR "First Round Capital" OR "Redpoint") "led" funding',

    # Enterprise AI funding (core focus)
    "enterprise_ai": '("Enterprise AI" OR "B2B AI" OR "LLMOps" OR "AI infrastructure") funding raised',

    # Stealth startup announcements (early signals)
    "stealth_mode": '("emerges from stealth" OR "exits stealth" OR "out of stealth") startup funding',

    # Series rounds with lead language
    "series_rounds": '("Series A" OR "Series B" OR "Series C") "led by" venture million',
}

# INDIVIDUAL: 18 fund-specific queries for granular coverage
# These catch edge cases the grouped queries might miss
INDIVIDUAL_ALERT_QUERIES = {
    "sequoia": '"Sequoia Capital" "led" funding',
    "a16z": '("Andreessen Horowitz" OR "a16z") "led" funding',
    "founders_fund": '"Founders Fund" "led" funding',
    "benchmark": '("Benchmark Capital" OR "Benchmark Partners") "led" funding',
    "khosla": '"Khosla Ventures" "led" funding',
    "index": '"Index Ventures" "led" funding',
    "insight": '"Insight Partners" "led" funding',
    "bessemer": '("Bessemer Venture Partners" OR "BVP") "led" funding',
    "greylock": '("Greylock Partners" OR "Greylock") "led" funding',
    "gv": '"Google Ventures" "led" funding',
    "menlo": '"Menlo Ventures" "led" funding',
    "thrive": '"Thrive Capital" "led" funding',
    "accel": '("Accel Partners" OR "Accel") "led" funding',
    "felicis": '"Felicis Ventures" "led" funding',
    "general_catalyst": '"General Catalyst" "led" funding',
    "first_round": '"First Round Capital" "led" funding',
    "redpoint": '"Redpoint Ventures" "led" funding',
    "usv": '"Union Square Ventures" "led" funding',
}

# Combined: All 25 queries for maximum coverage
ALL_ALERT_QUERIES = {**GROUPED_ALERT_QUERIES, **INDIVIDUAL_ALERT_QUERIES}

# Legacy alias for backwards compatibility
OPTIMAL_ALERT_QUERIES = GROUPED_ALERT_QUERIES
DEFAULT_ALERT_QUERIES = INDIVIDUAL_ALERT_QUERIES


@dataclass
class GoogleAlertItem:
    """Single item from Google Alerts RSS feed."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    source: str


class GoogleAlertsScraper:
    """
    Scraper for Google Alerts RSS feeds.

    Polls configured RSS feeds for new funding news.
    Optimized for parallel fetching with rate limiting.
    """

    def __init__(self, feed_urls: Optional[List[str]] = None):
        """
        Initialize with list of Google Alerts RSS feed URLs.

        Args:
            feed_urls: List of RSS feed URLs from Google Alerts.
                      If None, uses feeds from settings or defaults.
        """
        self.feed_urls = feed_urls or self._get_configured_feeds()
        # Increased connection pool for parallel fetching
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "BudTracker/1.0 (Investment Research)"},
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )

    def _get_configured_feeds(self) -> List[str]:
        """Get feed URLs from settings or environment."""
        # Check for comma-separated feed URLs in settings
        feeds_str = getattr(settings, 'google_alerts_feeds', '')
        if feeds_str:
            feeds = []
            for f in feeds_str.split(','):
                url = f.strip()
                if url:
                    # FIX: Validate URL format and domain
                    parsed = urlparse(url)
                    if not (parsed.scheme in ('http', 'https') and parsed.netloc):
                        logger.warning(f"Invalid Google Alerts feed URL (missing scheme or domain): {url}")
                        continue

                    # FIX: Verify it's a Google Alerts feed URL
                    # Valid formats: google.com/alerts, google.*/alerts
                    if 'google' not in parsed.netloc.lower() or '/alerts' not in url.lower():
                        logger.warning(
                            f"URL may not be a Google Alerts feed (expected google.com/alerts): {url}"
                        )
                        # Still add it but warn - could be intentional

                    feeds.append(url)
            return feeds
        return []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_feed(self, feed_url: str, max_retries: int = 3) -> List[GoogleAlertItem]:
        """Fetch and parse a single RSS feed with retry logic."""
        for attempt in range(max_retries):
            try:
                response = await self.client.get(feed_url)
                response.raise_for_status()

                # Parse RSS feed
                feed = feedparser.parse(response.text)

                # Check for malformed feed
                if feed.bozo:
                    logger.warning(f"Malformed Google Alerts feed: {feed.bozo_exception}")
                    return []

                items = []
                for entry in feed.entries:
                    # Skip entries without URLs (empty URLs cause downstream issues)
                    if not entry.get('link'):
                        logger.debug(f"Skipping entry without URL: {entry.get('title', 'No title')}")
                        continue

                    # Parse publication date with fallbacks
                    # FIX #14: Use timezone-aware datetime to prevent comparison issues
                    pub_date = None
                    for date_field in ['published_parsed', 'updated_parsed', 'created_parsed']:
                        date_tuple = getattr(entry, date_field, None)
                        if date_tuple and len(date_tuple) >= 6:
                            pub_date = datetime(*date_tuple[:6], tzinfo=timezone.utc)
                            break

                    # Clean up description (remove HTML if present)
                    description = entry.get('summary', '')
                    if description and ('<' in description and '>' in description):
                        try:
                            soup = BeautifulSoup(description, 'lxml')
                            description = soup.get_text(strip=True)
                        except Exception as e:
                            logger.debug(f"Failed to parse HTML description: {e}")
                            # Keep original description if parsing fails

                    # Extract source from title if present
                    title = entry.get('title', '')
                    source = ''
                    if ' - ' in title:
                        parts = title.rsplit(' - ', 1)
                        if len(parts) == 2:
                            title = parts[0]
                            source = parts[1]

                    items.append(GoogleAlertItem(
                        title=title,
                        url=entry.get('link', ''),
                        description=description[:500],  # Truncate long descriptions
                        published=pub_date,
                        source=source,
                    ))

                return items

            except httpx.HTTPError as e:
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"HTTP error fetching feed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching Google Alert feed after {max_retries} attempts: {e}")

            except Exception as e:
                logger.error(f"Unexpected error fetching Google Alert feed: {e}", exc_info=True)
                return []

        return []

    async def fetch_all_feeds(self) -> List[GoogleAlertItem]:
        """Fetch all configured RSS feeds in parallel with rate limiting."""
        if not self.feed_urls:
            return []

        all_items = []
        seen_urls = set()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_FEEDS)

        async def fetch_with_limit(feed_url: str) -> List[GoogleAlertItem]:
            async with semaphore:
                items = await self.fetch_feed(feed_url)
            # Rate limiting removed - only retry on errors (handled in fetch_feed)
            return items

        # Fetch all feeds in parallel
        tasks = [fetch_with_limit(url) for url in self.feed_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and deduplicate
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Feed fetch failed: {result}")
                continue
            for item in result:
                if item.url not in seen_urls:
                    seen_urls.add(item.url)
                    all_items.append(item)

        logger.info(f"Fetched {len(all_items)} unique articles from {len(self.feed_urls)} feeds")
        return all_items

    def _extract_real_url(self, url: str) -> str:
        """
        Extract actual article URL from Google redirect URLs.

        Google Alerts returns URLs like:
        https://www.google.com/url?rct=j&sa=t&url=https://example.com/article&...

        We need to extract the 'url' parameter to get the real article URL.
        """
        if not url:
            return url

        try:
            parsed = urlparse(url)

            # Check if this is a Google redirect URL
            if 'google.com/url' in url or 'google.com/alerts' in url:
                query_params = parse_qs(parsed.query)
                if 'url' in query_params and query_params['url']:
                    real_url = query_params['url'][0]
                    # Validate extracted URL has a valid scheme
                    if real_url and real_url.startswith(('http://', 'https://')):
                        logger.debug(f"Extracted real URL from Google redirect: {real_url[:100]}...")
                        return real_url
                    else:
                        logger.warning(f"Invalid URL extracted from Google redirect: {real_url[:50] if real_url else 'empty'}")

            return url
        except Exception as e:
            logger.warning(f"Error extracting URL from Google redirect: {e}")
            return url

    async def fetch_full_article(self, url: str, max_retries: int = 2) -> Optional[str]:
        """Fetch full article content from URL with retry logic."""
        # FIX: Extract actual URL from Google redirect URLs
        actual_url = self._extract_real_url(url)

        # Extended list of article content selectors (ordered by specificity)
        ARTICLE_SELECTORS = [
            # News site specific
            '[data-testid="article-body"]',  # Modern news sites
            '.article__body', '.article-content', '.article-text',
            '.story-body', '.story-content', '.story__body',
            '.entry-content', '.post-body', '.post-content',
            # General purpose
            'article', '[role="article"]',
            '.article', '.post', '.entry',
            'main article', 'main .content',
            # Fallbacks
            'main', '.content', '#content',
            '.body-content', '.page-content',
        ]

        for attempt in range(max_retries):
            try:
                response = await self.client.get(
                    actual_url,  # FIX: Use extracted URL, not Google redirect
                    follow_redirects=True,
                    timeout=15,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'lxml')

                # Remove unwanted elements
                for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'ads', 'noscript', 'iframe']):
                    tag.decompose()

                # Try extended list of article selectors
                for selector in ARTICLE_SELECTORS:
                    content = soup.select_one(selector)
                    if content:
                        text = content.get_text(separator='\n', strip=True)
                        if len(text) >= 500:  # Minimum 500 chars for meaningful LLM extraction
                            return text[:5000]

                # Fallback to body
                if soup.body:
                    return soup.body.get_text(separator='\n', strip=True)[:5000]

                return None

            except httpx.HTTPError as e:
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"HTTP error fetching article (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {actual_url}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error fetching article after {max_retries} attempts {actual_url}: {e}")
                    return None

            except Exception as e:
                logger.error(f"Unexpected error fetching article {actual_url}: {e}", exc_info=True)
                return None

        return None

    async def scrape_all(self) -> List[NormalizedArticle]:
        """
        Full scraping pipeline with parallel article fetching.

        Returns list of normalized articles ready for extraction.
        """
        if not self.feed_urls:
            logger.warning("No Google Alerts RSS feeds configured. Set GOOGLE_ALERTS_FEEDS environment variable.")
            return []

        # Fetch all alert items (parallel)
        items = await self.fetch_all_feeds()

        # Filter items with dates
        valid_items = []
        for item in items:
            if not item.published:
                logger.debug(f"Skipping article without date: {item.title}")
                continue
            valid_items.append(item)

        if not valid_items:
            logger.info("No valid articles found with publication dates")
            return []

        logger.info(f"Processing {len(valid_items)} articles with valid dates")

        # Fetch article content in parallel
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ARTICLES)

        async def fetch_article_with_limit(item: GoogleAlertItem) -> Tuple[GoogleAlertItem, Optional[str]]:
            async with semaphore:
                full_text = await self.fetch_full_article(item.url)
                return (item, full_text)

        tasks = [fetch_article_with_limit(item) for item in valid_items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Build normalized articles
        articles = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Article fetch failed: {result}")
                continue

            item, full_text = result
            text = full_text or f"{item.title}\n\n{item.description}"

            # Match fund from article content using centralized fund matcher
            full_content = f"{item.title} {item.description} {text}"
            fund_slug = match_fund_name(full_content)

            articles.append(NormalizedArticle(
                url=item.url,
                title=item.title,
                text=text,
                published_date=item.published.date(),
                author=None,
                tags=['google_alerts', item.source] if item.source else ['google_alerts'],
                fund_slug=fund_slug,
                fetched_at=datetime.now(timezone.utc),
            ))

        logger.info(f"Scraped {len(articles)} articles from Google Alerts")
        return articles


def get_alert_setup_instructions() -> str:
    """Return instructions for setting up Google Alerts."""
    instructions = """
=== GOOGLE ALERTS SETUP INSTRUCTIONS ===

SETUP TIME: ~15 minutes (one-time manual setup)
COST: FREE
RECOMMENDED: Create all 25 alerts for maximum coverage

1. Go to https://google.com/alerts (sign in with Google account)

2. CREATE GROUPED ALERTS (7 alerts - covers all funds efficiently):

"""
    for name, query in GROUPED_ALERT_QUERIES.items():
        instructions += f"   {name}:\n   {query}\n\n"

    instructions += """
3. CREATE INDIVIDUAL FUND ALERTS (18 alerts - catches edge cases):

"""
    for name, query in INDIVIDUAL_ALERT_QUERIES.items():
        instructions += f"   {name}:\n   {query}\n\n"

    instructions += """
4. For EACH alert:
   - Click "Show options" (dropdown arrow)
   - Set "How often" to "As-it-happens"
   - Set "Sources" to "News"
   - Set "Language" to "English"
   - Set "Region" to "United States"
   - Set "Deliver to" to "RSS feed" (CRITICAL!)
   - Click "Create Alert"

5. Copy RSS feed URLs:
   - After creating each alert, an RSS icon appears
   - Right-click the RSS icon → Copy link address
   - Save all 25 URLs

6. Add to Railway:
   railway variables set GOOGLE_ALERTS_FEEDS="url1,url2,url3,...,url25"

   Or set in Railway dashboard → Variables → Add:
   GOOGLE_ALERTS_FEEDS = url1,url2,url3,...,url25

7. Verify configuration:
   curl -X POST https://bud-tracker-backend-production.up.railway.app/scrapers/google-alerts -H "X-API-Key: dev-key"

Expected result: feeds_configured: 25, articles_found: 50+
"""
    return instructions


# Convenience function
async def run_google_alerts_scraper(feed_urls: Optional[List[str]] = None) -> List[NormalizedArticle]:
    """Run Google Alerts scraper and return articles."""
    async with GoogleAlertsScraper(feed_urls) as scraper:
        return await scraper.scrape_all()
