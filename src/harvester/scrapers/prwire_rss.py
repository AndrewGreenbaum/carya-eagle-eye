"""
PR Wire RSS Feed Scraper.

Scrapes RSS feeds from PR distribution services where funding announcements are published:
- PRNewswire (prnewswire.com)
- GlobeNewswire (globenewswire.com)

Note: BusinessWire deprecated their public RSS feeds (require authenticated PressPass).

These services are the primary distribution channels for official funding press releases.
The Brave News API often misses these - RSS feeds provide direct access.
"""

import asyncio
import logging
import random
import re
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

import feedparser
import httpx
from dataclasses import dataclass

from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ..fund_matcher import match_fund_name
from ...config.settings import settings
from ...common.http_client import USER_AGENT_BOT

logger = logging.getLogger(__name__)


# PR Wire RSS Feed URLs
# PRNewswire venture capital category
PRNEWSWIRE_FEEDS = [
    "https://www.prnewswire.com/rss/financial-services-latest-news/venture-capital-list.rss",
]

# GlobeNewswire financing and M&A feeds
GLOBENEWSWIRE_FEEDS = [
    "https://www.globenewswire.com/RssFeed/subjectcode/23-Financing%20Agreements/feedTitle/GlobeNewswire%20-%20Financing%20Agreements",
    "https://www.globenewswire.com/RssFeed/subjectcode/26-Mergers%20and%20Acquisitions/feedTitle/GlobeNewswire%20-%20Mergers%20and%20Acquisitions",
]

# All feeds combined (BusinessWire deprecated public RSS - requires PressPass auth)
ALL_PR_FEEDS = PRNEWSWIRE_FEEDS + GLOBENEWSWIRE_FEEDS


# Keywords to filter funding-related press releases
FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "series a", "series b", "series c", "series d",
    "seed round", "seed funding", "pre-seed", "venture", "investment", "million",
    "financing", "capital", "investors", "led by", "leads", "backed by",
]

# Keywords to identify tracked VC involvement
# FIX: Split into regular keywords (substring match ok) and short keywords (need word boundary)
# Short keywords like "gv" could match "given", "a16z" is distinctive enough but still benefits
TRACKED_FUND_KEYWORDS_LONG = [
    # Fund names (longer, safe for substring matching)
    "founders fund", "benchmark", "sequoia", "khosla", "index ventures",
    "andreessen horowitz", "insight partners", "bessemer",
    "redpoint", "greylock", "google ventures", "menlo ventures",
    "union square", "thrive capital", "accel", "felicis", "general catalyst",
    "first round",
    # Notable partners (longer names, safe for substring matching)
    "peter thiel", "bill gurley", "sarah tavel", "vinod khosla", "reid hoffman",
    "tomasz tunguz", "logan bartlett", "josh kopelman", "josh kushner",
]

# Short keywords that MUST use word boundary matching
# FIX: "gv" could match "given", "usv" could match "gusvex", etc.
TRACKED_FUND_KEYWORDS_SHORT = [
    "a16z", "gv", "usv", "bvp",
]


@dataclass
class PRWireArticle:
    """Article from PR wire RSS feed."""
    title: str
    url: str
    description: str
    published_date: Optional[datetime]
    source: str  # prnewswire, globenewswire, businesswire


class PRWireRSSScraper:
    """
    Scrape funding press releases from PR wire RSS feeds.

    These feeds are the primary source for official funding announcements.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=settings.request_timeout,
            headers={"User-Agent": USER_AGENT_BOT},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    def _parse_date(self, entry: Dict[str, Any]) -> Optional[datetime]:
        """Parse publication date from feed entry."""
        entry_title = entry.get('title', 'unknown')[:50]  # For logging

        # Try different date fields
        for field in ["published_parsed", "updated_parsed", "created_parsed"]:
            if hasattr(entry, field) and getattr(entry, field):
                try:
                    import time
                    ts = time.mktime(getattr(entry, field))
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
                except (TypeError, ValueError) as e:
                    # FIX (2026-01): Log failed date parsing attempts
                    logger.debug(f"Failed to parse {field} for '{entry_title}': {e}")
                    continue

        # Try string date fields
        for field in ["published", "updated", "created"]:
            if entry.get(field):
                try:
                    from dateutil import parser
                    return parser.parse(entry[field])
                except Exception as e:
                    # FIX (2026-01): Log failed date parsing attempts
                    logger.debug(f"Failed to parse date string '{entry.get(field)}' for '{entry_title}': {e}")
                    continue

        # FIX (2026-01): Log when no date could be parsed
        logger.debug(f"No parseable date found for article: '{entry_title}'")
        return None

    def _is_funding_related(self, title: str, description: str) -> bool:
        """Check if article is related to startup funding."""
        text = f"{title} {description}".lower()
        return any(kw in text for kw in FUNDING_KEYWORDS)

    def _has_tracked_fund(self, title: str, description: str) -> bool:
        """Check if article mentions a tracked VC fund or partner.

        FIX: Uses word boundary matching for short keywords to avoid false positives
        like "gv" matching "given" or "guv".
        FIX (2026-01): Also checks word-order variations for two-word fund names
        (e.g., "Ventures Index" as well as "Index Ventures").
        """
        text = f"{title} {description}".lower()

        # Check long keywords with simple substring match (safe)
        for kw in TRACKED_FUND_KEYWORDS_LONG:
            if kw in text:
                return True
            # FIX (2026-01): For two-word fund names, also check reversed word order
            # e.g., "Index Ventures" also matches "Ventures Index"
            words = kw.split()
            if len(words) == 2:
                reversed_kw = f"{words[1]} {words[0]}"
                if reversed_kw in text:
                    return True

        # Check short keywords with word boundary (prevents false positives)
        for kw in TRACKED_FUND_KEYWORDS_SHORT:
            pattern = rf'\b{re.escape(kw)}\b'
            if re.search(pattern, text):
                return True

        return False

    async def fetch_full_article(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch full article content from PR wire URL with retry logic.

        Implements exponential backoff with jitter for 5xx errors and timeouts.
        4xx errors fail fast since the article likely doesn't exist.
        """
        for attempt in range(max_retries):
            try:
                response = await self.client.get(
                    url,
                    follow_redirects=True,
                    timeout=settings.article_fetch_timeout,
                )

                # 4xx errors - fail fast (article doesn't exist/paywall)
                if 400 <= response.status_code < 500:
                    logger.debug(f"HTTP {response.status_code} for PR wire article {url}")
                    return None

                # 5xx errors - retry with backoff + jitter
                if response.status_code >= 500:
                    if attempt < max_retries - 1:
                        delay = (2 ** attempt) * random.uniform(0.9, 1.1)
                        logger.warning(
                            f"HTTP {response.status_code} fetching PR wire article {url}, "
                            f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"HTTP {response.status_code} fetching PR wire article {url} after {max_retries} attempts")
                        return None

                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'lxml')

                # Remove unwanted elements
                for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'ads']):
                    tag.decompose()

                # PR wire article selectors (PRNewswire, GlobeNewswire, BusinessWire)
                selectors_tried = [
                    '.release-body',  # PRNewswire
                    '.main-body',  # PRNewswire alt
                    '#content-body',  # GlobeNewswire
                    '.article-body',  # GlobeNewswire alt
                    '.bw-release-story',  # BusinessWire
                    '.entry-content',
                    'article',
                    '.post-content',
                    'main',
                ]
                for selector in selectors_tried:
                    content = soup.select_one(selector)
                    if content:
                        text = content.get_text(separator='\n', strip=True)
                        if len(text) > 200:
                            return text[:8000]

                # FIX (2026-01): Log when all selectors fail (helps debug site changes)
                logger.debug(
                    f"No content found for PR wire article {url} - "
                    f"all selectors failed: {selectors_tried}"
                )
                return None

            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    delay = (2 ** attempt) * random.uniform(0.9, 1.1)
                    logger.warning(
                        f"Timeout fetching PR wire article {url}, "
                        f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Timeout fetching PR wire article {url} after {max_retries} attempts")
                    return None

            except httpx.HTTPError as e:
                logger.debug(f"HTTP error fetching PR wire article {url}: {e}")
                return None

            except Exception as e:
                logger.error(f"Unexpected error fetching PR wire article {url}: {e}", exc_info=True)
                return None

        return None

    def _get_source_name(self, feed_url: str) -> str:
        """Get source name from feed URL."""
        if "prnewswire" in feed_url:
            return "prnewswire"
        elif "globenewswire" in feed_url:
            return "globenewswire"
        elif "businesswire" in feed_url:
            return "businesswire"
        return "prwire"

    async def fetch_feed(
        self,
        feed_url: str,
        hours_back: int = 168,  # 7 days
        max_retries: int = 3,
    ) -> List[PRWireArticle]:
        """Fetch and parse a single RSS feed with retry logic.

        Feed fetching is critical - implements exponential backoff with jitter.
        """
        for attempt in range(max_retries):
            try:
                response = await self.client.get(feed_url)

                # 5xx errors - retry with backoff + jitter
                if response.status_code >= 500:
                    if attempt < max_retries - 1:
                        delay = (2 ** attempt) * random.uniform(0.9, 1.1)
                        logger.warning(
                            f"HTTP {response.status_code} fetching feed {feed_url}, "
                            f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"HTTP {response.status_code} fetching feed {feed_url} after {max_retries} attempts")
                        return []

                response.raise_for_status()
                feed = feedparser.parse(response.text)

                if feed.bozo:
                    logger.warning(f"Malformed RSS feed: {feed_url} - {feed.bozo_exception}")

                cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                source = self._get_source_name(feed_url)
                articles = []

                for entry in feed.entries:
                    title = entry.get("title", "").strip()
                    link = entry.get("link", "")
                    description = entry.get("summary", entry.get("description", ""))
                    pub_date = self._parse_date(entry)

                    # FIX (2026-01): Skip articles with empty/whitespace titles
                    if not title:
                        logger.debug(f"Skipping article with empty title from {source}: {link}")
                        continue

                    # Skip old articles
                    if pub_date and pub_date < cutoff:
                        continue

                    # Skip non-funding articles
                    if not self._is_funding_related(title, description):
                        continue

                    articles.append(PRWireArticle(
                        title=title,
                        url=link,
                        description=description,
                        published_date=pub_date,
                        source=source,
                    ))

                logger.info(f"Found {len(articles)} funding articles from {source}")
                return articles

            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    delay = (2 ** attempt) * random.uniform(0.9, 1.1)
                    logger.warning(
                        f"Timeout fetching feed {feed_url}, "
                        f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Timeout fetching feed {feed_url} after {max_retries} attempts")
                    return []

            except Exception as e:
                logger.error(f"Error fetching PR wire feed {feed_url}: {e}", exc_info=True)
                return []

        return []

    async def scrape_all_feeds(
        self,
        hours_back: int = 168,
        fund_filter: bool = True,
    ) -> List[NormalizedArticle]:
        """
        Scrape all PR wire RSS feeds.

        Args:
            hours_back: How far back to look (default 7 days)
            fund_filter: If True, only return articles mentioning tracked funds

        Returns:
            List of NormalizedArticle objects ready for extraction
        """
        all_articles: List[PRWireArticle] = []
        seen_urls = set()

        # Fetch all feeds in parallel
        tasks = [self.fetch_feed(url, hours_back) for url in ALL_PR_FEEDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"PR wire feed error: {result}")
                continue
            for article in result:
                if article.url not in seen_urls:
                    seen_urls.add(article.url)
                    all_articles.append(article)

        logger.info(f"Total funding articles from PR wires: {len(all_articles)}")

        # Filter for tracked funds if requested
        if fund_filter:
            filtered = [
                a for a in all_articles
                if self._has_tracked_fund(a.title, a.description)
            ]
            logger.info(f"Articles mentioning tracked funds: {len(filtered)}")
            all_articles = filtered

        # Convert to NormalizedArticle with PARALLEL content fetch
        # FIX: Sequential fetching was O(N) with timeouts - now uses semaphore for parallelism
        valid_articles = [
            article for article in all_articles
            if (article.url or '').strip().startswith(('http://', 'https://'))
            and article.published_date
        ]

        # Log skipped articles
        skipped_count = len(all_articles) - len(valid_articles)
        if skipped_count > 0:
            logger.debug(f"Skipped {skipped_count} articles without valid URL or date")

        # Parallel fetch with semaphore (uses centralized settings)
        semaphore = asyncio.Semaphore(settings.max_concurrent_articles)

        async def fetch_and_normalize(article: PRWireArticle) -> Optional[NormalizedArticle]:
            """Fetch article content and normalize with rate limiting."""
            async with semaphore:
                full_text = await self.fetch_full_article(article.url)
                text = full_text or f"{article.title}\n\n{article.description}"

                # Match fund using centralized matcher
                fund_slug = match_fund_name(text)

                return NormalizedArticle(
                    title=article.title,
                    url=article.url,
                    text=text,
                    published_date=article.published_date.date(),
                    fund_slug=fund_slug or article.source,
                    fetched_at=datetime.now(timezone.utc),
                )

        # Run all fetches in parallel
        tasks = [fetch_and_normalize(article) for article in valid_articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and None results
        normalized: List[NormalizedArticle] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Error fetching PR wire article: {result}")
            elif result is not None:
                normalized.append(result)

        # SCRAPER_HEALTH_ALERT: Log warning when scraper returns 0 articles
        if not normalized:
            logger.warning(
                "SCRAPER_HEALTH_ALERT: prwire_rss returned 0 articles - "
                "RSS feeds may be unavailable or selectors may have changed"
            )

        logger.info(f"Scraped {len(normalized)} funding articles from PR wires")
        return normalized


async def scrape_prwire_feeds(
    hours_back: int = 168,
    fund_filter: bool = True,
) -> List[NormalizedArticle]:
    """
    Convenience function to scrape all PR wire RSS feeds.

    Args:
        hours_back: How far back to look (default 7 days)
        fund_filter: If True, only return articles mentioning tracked funds

    Returns:
        List of NormalizedArticle objects
    """
    async with PRWireRSSScraper() as scraper:
        return await scraper.scrape_all_feeds(hours_back, fund_filter)
