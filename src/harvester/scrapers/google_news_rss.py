"""
Google News RSS Scraper - Coverage for External-Only Funds.

Google News provides much better coverage than Brave Search API for finding
funding announcements about specific VC funds. This scraper creates RSS
feeds for funds that don't have scrapable portfolio pages.

Target funds (external-only or low coverage):
- Thrive Capital (0 deals - no portfolio page)
- Benchmark (0 deals - no portfolio page)
- First Round (0 deals - site doesn't announce deals)
- Greylock (1 deal - Playwright issues)
- GV (2 deals - limited site announcements)
- Founders Fund (3 deals - partial coverage)
- Redpoint (4 deals - React site issues)

RSS Format:
https://news.google.com/rss/search?q="Fund+Name"+funding&hl=en-US&gl=US&ceid=US:en

FIX (2026-01): Added full article fetching to get complete content instead of
just headlines. This enables proper LLM extraction of lead status and deal details.
"""

import asyncio
import base64
import feedparser
import httpx
import logging
import random
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qs, unquote
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ...config.settings import settings
from ...common.http_client import USER_AGENT_BROWSER

# Optional Playwright resolver for JS redirect following
try:
    from .playwright_resolver import PlaywrightResolver, PLAYWRIGHT_AVAILABLE
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PlaywrightResolver = None

logger = logging.getLogger(__name__)

# Article fetching settings - use centralized config
MAX_CONCURRENT_ARTICLES = settings.max_concurrent_articles
ARTICLE_FETCH_TIMEOUT = settings.article_fetch_timeout
ARTICLE_RATE_LIMIT_DELAY = settings.article_rate_limit_delay

# Article content selectors (ordered by specificity)
ARTICLE_SELECTORS = [
    # News site specific
    '[data-testid="article-body"]',
    '.article__body', '.article-content', '.article-text',
    '.story-body', '.story-content', '.story__body',
    '.post-content', '.post-body', '.post__content',
    '.entry-content', '.entry-body',
    # Generic content areas
    '[role="article"]', 'article', '.content', '#content',
    'main', '.main-content', '#main-content',
]


# External-only funds that need Google News coverage
# These funds either have no portfolio page or their scrapers aren't working
# UPDATED 2026-01: Expanded partner lists based on current team pages + additional query variants
GOOGLE_NEWS_FUNDS = {
    # EXTERNAL scraper type (no portfolio page)
    # Team: Josh Kushner (founder), Kareem Zaki, Nabil Mallick (COO), Belen Mella, Gaurav Ahuja
    "thrive": {
        "name": "Thrive Capital",
        "queries": [
            # Primary fund queries
            '"Thrive Capital" funding',
            '"Thrive Capital" leads investment',
            '"Thrive Capital" Series',
            '"Thrive Capital" raises',
            '"Thrive Capital" led',
            '"led by Thrive"',
            'raises million "Thrive Capital"',
            '"Thrive Capital" closes',
            '"Thrive Capital" backs',
            # Partner-specific queries
            '"Josh Kushner" investment startup',
            '"Josh Kushner" leads funding',
            '"Josh Kushner" venture',
            '"Kareem Zaki" investment',
            '"Kareem Zaki" startup',
            '"Nabil Mallick" investment',
            '"Gaurav Ahuja" investment Thrive',
            '"Belen Mella" investment',
        ],
        "exclude": ["Thrive Global", "Thrive IT", "Thrive Market", "Thrive Wellness", "Arianna Huffington"],
    },
    # EXTERNAL scraper type (no portfolio page)
    # Current GPs: Chetan Puttagunta, Peter Fenton, Eric Vishria
    # Former (still newsworthy): Bill Gurley, Sarah Tavel, Miles Grimshaw, Victor Lazarte
    "benchmark": {
        "name": "Benchmark Capital",
        "queries": [
            # Primary fund queries
            '"Benchmark Capital" funding',
            '"Benchmark" leads investment startup',
            '"Benchmark" Series A',
            '"Benchmark" Series B',
            '"led by Benchmark"',
            '"Benchmark" closes funding',
            '"Benchmark" backs startup',
            'raises million "Benchmark"',
            # Current GP queries
            '"Chetan Puttagunta" investment',
            '"Chetan Puttagunta" leads',
            '"Chetan Puttagunta" startup',
            '"Peter Fenton" investment',
            '"Peter Fenton" leads',
            '"Eric Vishria" investment',
            '"Eric Vishria" leads',
            # Former GPs (still referenced in deals)
            '"Bill Gurley" investment',
            '"Bill Gurley" leads',
            '"Sarah Tavel" investment',
            '"Victor Lazarte" investment',
            '"Miles Grimshaw" investment',
        ],
        "exclude": ["Benchmark Electronics", "Benchmark International", "Benchmark Mineral"],
    },
    # HTML scraper but site doesn't announce deals
    # Current: Meka Asonye, Josh Kopelman, Todd Jackson, Brett Berson, Bill Trenchard, Liz Wessel, Hayley Barna
    "first_round": {
        "name": "First Round Capital",
        "queries": [
            # Primary fund queries
            '"First Round Capital" funding',
            '"First Round Capital" leads',
            '"First Round" leads seed',
            '"First Round" Series A',
            '"led by First Round"',
            '"First Round" backs startup',
            '"First Round" closes',
            'raises million "First Round"',
            # Partner-specific queries
            '"Josh Kopelman" investment',
            '"Josh Kopelman" leads',
            '"Todd Jackson" investment startup',
            '"Brett Berson" investment',
            '"Bill Trenchard" investment',
            '"Meka Asonye" investment',
            '"Liz Wessel" investment',
            '"Hayley Barna" investment',
        ],
        "exclude": ["first round pick", "NFL", "NBA", "first round draft", "playoff"],
    },
    # PLAYWRIGHT but may have issues
    # Partners: Asheem Chandna, Jerry Chen, Mike Duboe, Reid Hoffman, Saam Motamedi, Seth Rosenberg
    # Venture Partners: Josh Elman, David Sze
    "greylock": {
        "name": "Greylock",
        "queries": [
            # Primary fund queries
            '"Greylock" funding startup',
            '"Greylock Partners" leads',
            '"Greylock" Series A',
            '"Greylock" Series B',
            '"led by Greylock"',
            '"Greylock" backs startup',
            '"Greylock" closes funding',
            'raises million "Greylock"',
            # Partner-specific queries
            '"Reid Hoffman" investment startup',
            '"Reid Hoffman" leads',
            '"Asheem Chandna" investment',
            '"Asheem Chandna" leads',
            '"Jerry Chen" investment Greylock',
            '"Saam Motamedi" investment',
            '"Seth Rosenberg" investment',
            '"Mike Duboe" investment',
            '"David Sze" investment',
            '"Josh Elman" investment startup',
            '"Corinne Riley" investment Greylock',
            '"Christine Kim" investment Greylock',
        ],
        "exclude": [],
    },
    # GPs: M.G. Siegler, Crystal Huang, Terri Burns, Krishna Yeshwant, Frédérique Dame, David Krane
    "gv": {
        "name": "GV (Google Ventures)",
        "queries": [
            # Primary fund queries
            '"Google Ventures" funding',
            '"Google Ventures" leads investment',
            '"GV" venture capital funding',
            '"GV" leads Series',
            '"led by GV"',
            '"led by Google Ventures"',
            '"Google Ventures" backs startup',
            '"Google Ventures" closes',
            'raises million "Google Ventures"',
            # Partner queries
            '"David Krane" investment GV',
            '"M.G. Siegler" investment',
            '"M.G. Siegler" startup',
            '"Crystal Huang" investment GV',
            '"Terri Burns" investment GV',
            '"Krishna Yeshwant" investment',
            '"Tyson Clark" investment GV',
            '"Brendan Bulik-Sullivan" investment',
        ],
        "exclude": ["NYSE:GV", "Visionary Holdings", "stock ticker GV", "GV stock"],
    },
    # Current: Peter Thiel, Napoleon Ta, Trae Stephens, Lauren Gross, Scott Nolan, John Luttig,
    #          Delian Asparouhov, Joey Krug
    # Former: Keith Rabois, Brian Singerman (still newsworthy)
    "founders_fund": {
        "name": "Founders Fund",
        "queries": [
            # Primary fund queries
            '"Founders Fund" funding',
            '"Founders Fund" leads investment',
            '"Founders Fund" Series',
            '"led by Founders Fund"',
            '"Founders Fund" backs startup',
            '"Founders Fund" closes',
            'raises million "Founders Fund"',
            # Current partner queries
            '"Peter Thiel" investment startup',
            '"Peter Thiel" leads',
            '"Napoleon Ta" investment',
            '"Trae Stephens" investment',
            '"Trae Stephens" leads',
            '"Delian Asparouhov" investment',
            '"Delian Asparouhov" startup',
            '"Scott Nolan" investment Founders Fund',
            '"John Luttig" investment',
            '"Joey Krug" investment',
            '"Lauren Gross" investment Founders Fund',
            # Former partners (still referenced)
            '"Keith Rabois" investment startup',
            '"Keith Rabois" leads',
            '"Brian Singerman" investment',
        ],
        "exclude": [],
    },
    "redpoint": {
        "name": "Redpoint Ventures",
        "queries": [
            # Primary fund queries
            '"Redpoint Ventures" funding',
            '"Redpoint" leads investment',
            '"Redpoint" Series A',
            '"Redpoint" Series B',
            '"led by Redpoint"',
            '"Redpoint" backs startup',
            '"Redpoint" closes',
            'raises million "Redpoint"',
            # Partner queries
            '"Satish Dharmaraj" investment',
            '"Tomasz Tunguz" investment',
            '"Tomasz Tunguz" leads',
            '"Logan Bartlett" investment',
            '"Erica Brescia" investment',
        ],
        "exclude": ["Redpoint Bio", "Redpoint Biopharmaceutical"],
    },
    # ADDED: Khosla Ventures for external coverage
    # Managing Directors: Vinod Khosla, Samir Kaul, Sven Strohband, David Weiden, Keith Rabois
    "khosla": {
        "name": "Khosla Ventures",
        "queries": [
            # Primary fund queries
            '"Khosla Ventures" funding',
            '"Khosla Ventures" leads',
            '"Khosla" Series A',
            '"Khosla" Series B',
            '"led by Khosla"',
            '"Khosla" backs startup',
            'raises million "Khosla"',
            # Partner queries
            '"Vinod Khosla" investment',
            '"Vinod Khosla" leads',
            '"Samir Kaul" investment',
            '"Sven Strohband" investment',
            '"David Weiden" investment Khosla',
            '"Keith Rabois" investment Khosla',
            '"Ethan Choi" investment Khosla',
        ],
        "exclude": [],
    },
}


def extract_real_url_from_google_news(google_news_url: str) -> Optional[str]:
    """
    Extract the actual article URL from a Google News redirect URL.

    Google News URLs look like:
    - Old format: https://news.google.com/rss/articles/CBMiWWh0dHBzOi8v...
    - New format: https://news.google.com/rss/articles/CBMieEFVX3lxTFB...

    The actual URL is encoded in various ways in the path.

    Returns:
        The actual article URL, or None if extraction fails.
    """
    try:
        if '/articles/' not in google_news_url:
            return None

        # Get the encoded part after /articles/
        match = re.search(r'/articles/([^?]+)', google_news_url)
        if not match:
            return None

        encoded = match.group(1)

        # Method 1: Try direct base64 decode (old format - URL directly encoded)
        for padding in ['', '=', '==', '===']:
            try:
                encoded_fixed = encoded.replace('-', '+').replace('_', '/')
                decoded = base64.b64decode(encoded_fixed + padding)
                decoded_str = decoded.decode('utf-8', errors='ignore')

                # Look for http URL in decoded string
                url_match = re.search(r'(https?://[^\s<>"\'\\x00-\\x1f]+)', decoded_str)
                if url_match:
                    url = url_match.group(1)
                    # Clean up any trailing garbage
                    url = re.sub(r'["\'>)\x00-\x1f]+$', '', url)
                    # Validate it looks like a real URL
                    if url.startswith('http') and len(url) > 25 and '.' in url:
                        return url
            except Exception:
                continue

        # Method 2: Try nested decoding (new format - sometimes double encoded)
        for padding1 in ['', '=', '==']:
            try:
                first_decode = base64.b64decode(
                    encoded.replace('-', '+').replace('_', '/') + padding1
                )
                # Look for another base64 string
                inner_match = re.search(rb'[A-Za-z0-9+/]{20,}={0,2}', first_decode)
                if inner_match:
                    inner_encoded = inner_match.group(0).decode('ascii')
                    for padding2 in ['', '=', '==']:
                        try:
                            inner_decoded = base64.b64decode(inner_encoded + padding2)
                            url_match = re.search(
                                r'(https?://[^\s<>"\'\\x00-\\x1f]+)',
                                inner_decoded.decode('utf-8', errors='ignore')
                            )
                            if url_match:
                                url = url_match.group(1)
                                url = re.sub(r'["\'>)\x00-\x1f]+$', '', url)
                                if url.startswith('http') and len(url) > 25:
                                    return url
                        except Exception:
                            continue
            except Exception:
                continue

        return None

    except Exception as e:
        logger.debug(f"Failed to extract real URL from {google_news_url}: {e}")
        return None


@dataclass
class GoogleNewsArticle:
    """Single article from Google News RSS."""
    title: str
    url: str  # Google News URL (unique identifier)
    real_url: Optional[str]  # Actual article URL (extracted from redirect)
    source_url: str  # Source domain URL (e.g., https://www.bisnow.com)
    description: str
    published: Optional[datetime]
    source: str
    fund_slug: str
    full_text: Optional[str] = None  # Full article content (fetched separately)


class GoogleNewsRSSScraper:
    """
    Scraper for Google News RSS feeds targeting specific VC funds.

    Google News RSS provides better coverage than Brave Search API because:
    1. Indexes more news sources
    2. Better freshness/recency
    3. More comprehensive for specific entity searches

    FIX (2026-01): Now fetches full article content instead of just using headlines.
    """

    def __init__(
        self,
        funds: Optional[List[str]] = None,
        fetch_full_articles: bool = True,
        use_playwright: bool = True,
    ):
        """
        Initialize with list of fund slugs to monitor.

        Args:
            funds: List of fund slugs. If None, uses all GOOGLE_NEWS_FUNDS.
            fetch_full_articles: If True, fetch full article content from source URLs.
            use_playwright: If True, use Playwright to resolve undecodable Google News URLs.
                           Requires playwright to be installed.

        FIX: HTTP clients are now lazily initialized in __aenter__ to prevent memory leaks
        when scraper is instantiated but not used as context manager.
        """
        self.funds = funds or list(GOOGLE_NEWS_FUNDS.keys())
        self.fetch_full_articles = fetch_full_articles
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        # FIX: Lazy initialization - clients created in __aenter__ to prevent memory leak
        self._client: Optional[httpx.AsyncClient] = None
        self._article_client: Optional[httpx.AsyncClient] = None
        # Playwright resolver for JS redirects (initialized in __aenter__)
        self._playwright_resolver: Optional['PlaywrightResolver'] = None
        self._entered = False  # Track if context manager was used

    def _create_client(self) -> httpx.AsyncClient:
        """Create a new HTTP client instance."""
        return httpx.AsyncClient(
            timeout=settings.request_timeout,
            headers={"User-Agent": USER_AGENT_BROWSER},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=True,
        )

    @property
    def client(self) -> httpx.AsyncClient:
        """Get the HTTP client, creating it lazily if needed.

        FIX 2026-01: Added lock to prevent TOCTOU race condition where multiple
        concurrent calls to this property could create multiple clients, causing
        memory leaks and socket exhaustion.

        Note: This is still a sync property. The lock acquisition happens on first
        access in a given event loop context. For truly concurrent-safe async code,
        prefer using __aenter__ context manager.
        """
        # Fast path: client exists and is open
        if self._client is not None and not self._client.is_closed:
            return self._client
        # Slow path: need to create client (only happens once per instance)
        # Note: This isn't truly thread-safe but is safe for single-threaded asyncio
        # For full safety, always use the async context manager
        self._client = self._create_client()
        return self._client

    async def __aenter__(self):
        self._entered = True
        # Ensure main client is created
        _ = self.client
        # Create article client
        self._article_client = httpx.AsyncClient(
            timeout=ARTICLE_FETCH_TIMEOUT,
            headers={"User-Agent": USER_AGENT_BROWSER},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            follow_redirects=True,
        )
        # Initialize Playwright resolver if enabled
        if self.use_playwright and PlaywrightResolver:
            self._playwright_resolver = PlaywrightResolver()
            await self._playwright_resolver.__aenter__()
            logger.info("Playwright resolver enabled for Google News URL resolution")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Explicitly close all HTTP clients and resources.

        FIX: Added explicit close method for cleanup when not using context manager.
        This prevents memory leaks from unclosed HTTP clients.
        """
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
        if self._article_client and not self._article_client.is_closed:
            await self._article_client.aclose()
            self._article_client = None
        if self._playwright_resolver:
            await self._playwright_resolver.__aexit__(None, None, None)
            self._playwright_resolver = None
        self._entered = False

    def __del__(self):
        """Destructor fallback to warn about unclosed clients.

        FIX 2026-01: If not used as context manager and close() not called,
        HTTP clients may leak. This logs a warning to help identify leaks.
        Note: Cannot actually close async clients from __del__ (no event loop).

        FIX 2026-01: Wrap in try-except to handle GC edge cases where:
        - Object is partially garbage collected
        - Logger may be unavailable during interpreter shutdown
        - Attributes may not exist if __init__ failed
        """
        try:
            if hasattr(self, '_client') and self._client and hasattr(self._client, 'is_closed'):
                if not self._client.is_closed:
                    import warnings
                    warnings.warn(
                        "GoogleNewsRSSScraper was not properly closed. "
                        "Use 'async with' context manager or call close() explicitly.",
                        ResourceWarning,
                        stacklevel=2
                    )
            if hasattr(self, '_article_client') and self._article_client and hasattr(self._article_client, 'is_closed'):
                if not self._article_client.is_closed:
                    import warnings
                    warnings.warn(
                        "GoogleNewsRSSScraper article client was not properly closed.",
                        ResourceWarning,
                        stacklevel=2
                    )
        except Exception:
            # Ignore all errors in __del__ - nothing we can do during GC
            pass

    async def fetch_article_content(self, url: str, max_retries: int = 2) -> Optional[str]:
        """
        Fetch full article content from URL.

        This is critical for proper deal extraction - headlines alone are not
        sufficient for the LLM to accurately determine lead investor status.

        Args:
            url: The article URL to fetch
            max_retries: Number of retry attempts

        Returns:
            Article text content, or None if fetch fails
        """
        if not self._article_client:
            return None

        for attempt in range(max_retries):
            try:
                resp = await self._article_client.get(url)
                if resp.status_code != 200:
                    logger.debug(f"HTTP {resp.status_code} for {url}")
                    return None

                soup = BeautifulSoup(resp.text, 'lxml')

                # Remove noise elements
                for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'iframe', 'noscript']):
                    tag.decompose()

                # Try article content selectors
                for selector in ARTICLE_SELECTORS:
                    element = soup.select_one(selector)
                    if element:
                        text = element.get_text(separator=' ', strip=True)
                        # Clean up whitespace
                        text = re.sub(r'\s+', ' ', text)
                        if len(text) > 200:  # Minimum viable content
                            logger.debug(f"Extracted {len(text)} chars from {url}")
                            return text[:4000]  # Truncate to 4000 chars

                # Fallback: get body text
                body = soup.find('body')
                if body:
                    text = body.get_text(separator=' ', strip=True)
                    text = re.sub(r'\s+', ' ', text)
                    if len(text) > 200:
                        return text[:4000]

                return None

            except httpx.TimeoutException:
                logger.debug(f"Timeout fetching {url} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
                return None

        return None

    async def resolve_google_news_url(self, google_news_url: str) -> Optional[str]:
        """
        Resolve a Google News URL to the actual article URL.

        Google News URLs use JavaScript redirects that can't be followed by httpx.
        This method tries to decode the URL from the base64-encoded path.

        Args:
            google_news_url: The Google News URL

        Returns:
            The actual article URL, or None if resolution fails
        """
        # Try to extract URL from the encoded path
        extracted = extract_real_url_from_google_news(google_news_url)
        if extracted:
            return extracted

        # Google News uses JS redirects - can't follow without browser
        return None

    async def search_for_article_url(self, title: str, source: str) -> Optional[str]:
        """
        Use web search to find the actual article URL based on title.

        When Google News URL resolution fails, we can search for the article
        by its title to find the original URL.

        Args:
            title: Article title
            source: Source publication name

        Returns:
            The actual article URL, or None if not found
        """
        if not self._article_client or not title:
            return None

        try:
            # Build a search query
            search_query = f'"{title}" site:{source.lower().replace(" ", "")}.com'

            # Use DuckDuckGo HTML search (no API key needed)
            params = {'q': search_query, 'kl': 'us-en'}
            resp = await self._article_client.get(
                f'https://html.duckduckgo.com/html/',
                params=params
            )

            if resp.status_code != 200:
                return None

            # Parse results to find article URL
            html = resp.text
            # Look for result links
            results = re.findall(r'href="([^"]+)"[^>]*class="result__url"', html)
            if results:
                return unquote(results[0])

            return None

        except Exception as e:
            logger.debug(f"Failed to search for article: {e}")
            return None

    def _build_feed_url(self, query: str) -> str:
        """Build Google News RSS URL for a query."""
        params = {
            "q": query,
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
        return f"https://news.google.com/rss/search?{urlencode(params)}"

    def _should_exclude(self, text: str, fund_slug: str) -> bool:
        """Check if article should be excluded based on fund-specific rules."""
        fund_config = GOOGLE_NEWS_FUNDS.get(fund_slug, {})
        exclude_terms = fund_config.get("exclude", [])

        text_lower = text.lower()
        for term in exclude_terms:
            if term.lower() in text_lower:
                return True
        return False

    async def fetch_feed(self, query: str, fund_slug: str, max_retries: int = 3) -> List[GoogleNewsArticle]:
        """Fetch and parse a Google News RSS feed with retry logic.

        Feed fetching is critical - 0 feeds = 0 articles for that fund.
        Implements exponential backoff for 5xx errors and timeouts.
        """
        feed_url = self._build_feed_url(query)

        for attempt in range(max_retries):
            try:
                response = await self.client.get(feed_url)

                # 5xx errors - retry with backoff + jitter
                if response.status_code >= 500:
                    if attempt < max_retries - 1:
                        # Add jitter (0.9-1.1x) to avoid thundering herd
                        delay = (2 ** attempt) * random.uniform(0.9, 1.1)
                        logger.warning(
                            f"HTTP {response.status_code} fetching feed for '{query}', "
                            f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"HTTP {response.status_code} fetching feed for '{query}' after {max_retries} attempts")
                        return []

                response.raise_for_status()

                feed = feedparser.parse(response.text)

                if feed.bozo:
                    logger.warning(f"Malformed Google News feed for '{query}': {feed.bozo_exception}")
                    return []

                items = []
                for entry in feed.entries:
                    # Skip entries without URLs
                    link = entry.get('link', '')
                    if not link:
                        continue

                    title = entry.get('title', '')
                    description = entry.get('summary', '')

                    # Clean HTML from description
                    if description:
                        soup = BeautifulSoup(description, 'lxml')
                        description = soup.get_text(strip=True)

                    # Check exclusions
                    combined_text = f"{title} {description}"
                    if self._should_exclude(combined_text, fund_slug):
                        logger.debug(f"Excluded article for {fund_slug}: {title[:50]}...")
                        continue

                    # Parse publication date
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        if len(entry.published_parsed) >= 6:
                            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)

                    # Extract source from title (Google News format: "Title - Source")
                    source = ""
                    if " - " in title:
                        parts = title.rsplit(" - ", 1)
                        if len(parts) == 2:
                            title = parts[0]
                            source = parts[1]

                    # Get source URL from RSS entry (e.g., https://www.bisnow.com)
                    source_entry = entry.get('source', {})
                    source_url = ""
                    if isinstance(source_entry, dict):
                        source_url = source_entry.get('href', '')

                    # Extract real URL from Google News redirect URL
                    real_url = extract_real_url_from_google_news(link)

                    items.append(GoogleNewsArticle(
                        title=title,
                        url=link,
                        real_url=real_url,
                        source_url=source_url,
                        description=description[:1000],
                        published=pub_date,
                        source=source,
                        fund_slug=fund_slug,
                    ))

                return items

            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    # Add jitter (0.9-1.1x) to avoid thundering herd
                    delay = (2 ** attempt) * random.uniform(0.9, 1.1)
                    logger.warning(
                        f"Timeout fetching feed for '{query}', "
                        f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Timeout fetching feed for '{query}' after {max_retries} attempts")
                    return []

            except httpx.HTTPError as e:
                logger.warning(f"HTTP error fetching Google News for '{query}': {e}")
                return []

            except Exception as e:
                logger.error(f"Error fetching Google News for '{query}': {e}", exc_info=True)
                return []

        return []  # Should not reach here, but safety return

    async def fetch_all_funds(self, days_back: int = 30) -> List[GoogleNewsArticle]:
        """Fetch Google News for all configured funds with rate limiting."""
        all_items: List[GoogleNewsArticle] = []
        seen_urls: set = set()
        seen_titles: set = set()  # Also dedupe by title (same article, different queries)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

        # Build all queries with fund context
        queries_with_context = []
        for fund_slug in self.funds:
            fund_config = GOOGLE_NEWS_FUNDS.get(fund_slug, {})
            queries = fund_config.get("queries", [])
            for query in queries:
                queries_with_context.append((query, fund_slug))

        # Rate-limited parallel fetching using semaphore
        semaphore = asyncio.Semaphore(settings.max_concurrent_feeds)

        async def fetch_with_limit(query: str, fund_slug: str) -> List[GoogleNewsArticle]:
            async with semaphore:
                result = await self.fetch_feed(query, fund_slug)
                await asyncio.sleep(0.2)  # Small delay between requests
                return result

        tasks = [fetch_with_limit(q, f) for q, f in queries_with_context]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Google News fetch failed: {result}")
                continue

            for item in result:
                # Skip duplicates by URL
                if item.url in seen_urls:
                    continue

                # Skip duplicates by title (same article from different queries)
                title_key = item.title.lower().strip()
                if title_key in seen_titles:
                    continue

                # Skip old articles
                if item.published and item.published < cutoff:
                    continue

                seen_urls.add(item.url)
                seen_titles.add(title_key)
                all_items.append(item)

        logger.info(f"Google News: Found {len(all_items)} unique articles for {len(self.funds)} funds")
        return all_items

    async def scrape_all(self, days_back: int = 30) -> List[NormalizedArticle]:
        """
        Full scraping pipeline for Google News RSS.

        FIX (2026-01): Now fetches full article content for better LLM extraction.
        Previously only used headlines which caused external-only funds (Thrive,
        Benchmark, First Round) to have 0 deals despite web searches finding real ones.

        Args:
            days_back: How many days back to look (default 30)

        Returns:
            List of normalized articles ready for extraction.
        """
        articles = await self.fetch_all_funds(days_back=days_back)

        # SCRAPER_HEALTH_ALERT: Log warning when scraper returns 0 articles
        if not articles:
            logger.warning(
                "SCRAPER_HEALTH_ALERT: google_news_rss returned 0 articles - "
                "RSS feeds may be unavailable or queries may need adjustment"
            )
            return []

        # Fetch full article content if enabled
        if self.fetch_full_articles:
            articles = await self._fetch_all_article_content(articles)

        normalized = []
        articles_with_content = 0
        articles_headline_only = 0

        for article in articles:
            # Use today as fallback for missing dates
            published_date = article.published.date() if article.published else date.today()

            # Build text - prefer full article content, fallback to headline
            fund_config = GOOGLE_NEWS_FUNDS.get(article.fund_slug, {})
            fund_name = fund_config.get("name", article.fund_slug)

            if article.full_text and len(article.full_text) > 200:
                # Full article content available - much better for LLM extraction
                text = f"""Headline: {article.title}
Source: {article.source or 'Unknown'}
Fund Context: {fund_name}

Article Content:
{article.full_text}"""
                articles_with_content += 1
            else:
                # Fallback to headline + description (less reliable)
                text_parts = [
                    f"Headline: {article.title}",
                    f"Source: {article.source}" if article.source else "",
                    f"Fund: {fund_name}",
                    "",
                    article.description if article.description else "",
                ]
                text = "\n".join(part for part in text_parts if part)
                articles_headline_only += 1

            # Use real URL if available, otherwise Google News URL
            # Real URL is better for deduplication against other sources
            article_url = article.real_url if article.real_url else article.url

            normalized.append(NormalizedArticle(
                url=article_url,
                title=article.title,
                text=text,
                published_date=published_date,
                author=article.source,
                tags=['google_news', article.fund_slug],
                fund_slug=article.fund_slug,
                fetched_at=datetime.now(timezone.utc),
            ))

        logger.info(
            f"Google News: Normalized {len(normalized)} articles "
            f"({articles_with_content} with full content, {articles_headline_only} headline-only)"
        )
        return normalized

    async def _fetch_all_article_content(self, articles: List[GoogleNewsArticle]) -> List[GoogleNewsArticle]:
        """
        Fetch full content for all articles in parallel.

        STRATEGY:
        1. First pass: Try base64 decoding for all URLs (fast, no network)
        2. Second pass: Use Playwright batch resolution for unresolved URLs
        3. Third pass: Fetch article content from resolved URLs

        Args:
            articles: List of GoogleNewsArticle with URLs

        Returns:
            Same articles with full_text populated where possible
        """
        if not articles:
            return articles

        # First pass: Try base64 decoding for all unresolved URLs
        unresolved_articles = []
        for article in articles:
            if not article.real_url:
                extracted = extract_real_url_from_google_news(article.url)
                if extracted:
                    article.real_url = extracted
                else:
                    unresolved_articles.append(article)

        decoded_count = len(articles) - len(unresolved_articles)
        logger.info(f"Google News: {decoded_count}/{len(articles)} URLs decoded from base64")

        # Second pass: Use Playwright for remaining unresolved URLs
        if unresolved_articles and self._playwright_resolver:
            logger.info(f"Using Playwright to resolve {len(unresolved_articles)} remaining URLs...")
            unresolved_urls = [a.url for a in unresolved_articles]
            resolved_map = await self._playwright_resolver.resolve_batch(
                unresolved_urls,
                max_concurrent=3,
                delay_between=0.3
            )
            # Update articles with resolved URLs
            for article in unresolved_articles:
                if article.url in resolved_map and resolved_map[article.url]:
                    article.real_url = resolved_map[article.url]

            playwright_resolved = sum(1 for a in unresolved_articles if a.real_url)
            logger.info(f"Playwright resolved {playwright_resolved}/{len(unresolved_articles)} URLs")

        # Third pass: Fetch article content from resolved URLs
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ARTICLES)

        async def fetch_with_limit(article: GoogleNewsArticle) -> GoogleNewsArticle:
            async with semaphore:
                if article.real_url:
                    content = await self.fetch_article_content(article.real_url)
                    if content:
                        article.full_text = content
                        logger.debug(f"Fetched {len(content)} chars for: {article.title[:50]}...")

                await asyncio.sleep(ARTICLE_RATE_LIMIT_DELAY)
                return article

        tasks = [fetch_with_limit(article) for article in articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return successful results
        fetched_articles = []
        success_count = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Article fetch failed: {result}")
            else:
                fetched_articles.append(result)
                if result.full_text:
                    success_count += 1

        # Log summary - headline-only articles are still valuable
        headline_only = len(fetched_articles) - success_count
        logger.info(
            f"Google News: {success_count} with full content, "
            f"{headline_only} headline-only (still processed)"
        )
        return fetched_articles


# Convenience function
async def run_google_news_scraper(days_back: int = 30) -> List[NormalizedArticle]:
    """Run Google News RSS scraper and return articles."""
    async with GoogleNewsRSSScraper() as scraper:
        return await scraper.scrape_all(days_back=days_back)
