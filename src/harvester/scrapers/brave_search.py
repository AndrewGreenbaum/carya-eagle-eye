"""
Brave Search API Scraper - Broad News Coverage.

Uses Brave Search API to find funding news articles with
"led by" language for tracked VC funds.

OPTIMIZED:
- Uses shared BraveClient with connection pooling
- Parallel query execution (3x concurrency)
- TTL caching for partner queries (1 hour)
- Reduced scrape time: 42s → ~15s

FIX (2026-01): Added full article fetching to get complete article
content instead of just search snippets. This enables proper LLM
extraction of lead investor and deal details.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Dict, Any, Tuple, TYPE_CHECKING

import httpx
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ...common.brave_client import get_brave_client, get_query_cache
from ...common.http_client import USER_AGENT_BOT, USER_AGENT_BROWSER
from ...config.settings import settings

logger = logging.getLogger(__name__)

# Memory safety limit (Brave API max is 100 per request)
MAX_RESULTS_PER_QUERY = 100

# Module-level shared HTTP client for article fetching (singleton pattern)
# Avoids creating duplicate clients per BraveSearchScraper instance
_article_fetch_client: Optional[httpx.AsyncClient] = None
_article_fetch_lock = asyncio.Lock()


async def _get_article_fetch_client() -> httpx.AsyncClient:
    """Get or create the shared HTTP client for article fetching."""
    global _article_fetch_client
    if _article_fetch_client is None or _article_fetch_client.is_closed:
        async with _article_fetch_lock:
            # Double-check after acquiring lock
            if _article_fetch_client is None or _article_fetch_client.is_closed:
                _article_fetch_client = httpx.AsyncClient(
                    timeout=ARTICLE_FETCH_TIMEOUT,
                    follow_redirects=True,
                    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                )
    return _article_fetch_client

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

# Fund name variations for search queries (primary name + alternates)
# Core 18 tracked funds
FUND_SEARCH_NAMES = {
    "founders_fund": ["Founders Fund", "Peter Thiel"],
    "benchmark": ["Benchmark", "Benchmark Capital"],
    "sequoia": ["Sequoia Capital", "Sequoia"],
    "khosla": ["Khosla Ventures", "Vinod Khosla"],
    "index": ["Index Ventures"],
    "a16z": ["Andreessen Horowitz", "a16z"],
    "insight": ["Insight Partners"],
    "bessemer": ["Bessemer Venture Partners", "Bessemer", "BVP"],
    "redpoint": ["Redpoint Ventures", "Redpoint"],
    "greylock": ["Greylock Partners", "Greylock"],
    "gv": ["Google Ventures", "GV"],
    "menlo": ["Menlo Ventures"],
    "usv": ["Union Square Ventures", "USV"],
    "thrive": ["Thrive Capital"],
    "accel": ["Accel Partners", "Accel"],
    "felicis": ["Felicis Ventures", "Felicis"],
    "general_catalyst": ["General Catalyst", "GC"],
    "first_round": ["First Round Capital", "First Round"],
}

# Exclusion terms to filter noise - enhanced for generic fund names
# NOTE: Be careful not to exclude common startup terms (e.g., don't exclude "accelerate" for Accel)
# FIXED 2026-01: GV exclusions were too aggressive, causing 0 leads. Relaxed to only exclude
# direct NYSE ticker references and known false positives.
EXCLUSION_TERMS = {
    # RELAXED: Only exclude direct ticker references, not general stock/dividend mentions
    # Articles about startups sometimes mention stock options, public markets, etc.
    "gv": ["-\"NYSE:GV\"", "-\"GV stock\"", "-\"Visionary Holdings\"", "-\"GV ticker\""],
    # FIXED: Removed "-wellness" - was too broad, excluded any article mentioning wellness sector
    "thrive": ["-\"Thrive Global\"", "-\"Thrive IT\"", "-\"Thrive Market\"", "-\"Thrive Wellness\""],
    "sequoia": ["-\"Sequoia National\"", "-HongShan", "-\"Peak XV\"", "-\"Sequoia India\"", "-\"Sequoia China\""],
    "benchmark": ["-benchmark.com", "-\"Benchmark International\"", "-\"Benchmark Electronics\""],  # Removed -benchmarking (too broad)
    "accel": ["-\"Accel Entertainment\""],  # Removed -accelerate/-acceleration/-accelerator (common startup terms)
    "index": ["-\"index fund\"", "-\"stock index\"", "-\"S&P index\"", "-\"market index\""],
    "insight": ["-\"market insight\"", "-\"business insight\"", "-\"consumer insight\""],  # Removed -insights (too broad)
    "menlo": ["-\"Menlo Park\"", "-\"Menlo College\"", "-\"Menlo School\""],
    "redpoint": ["-\"Redpoint Bio\"", "-\"Redpoint Positioning\""],
    "first_round": ["-\"first round pick\"", "-\"first round draft\"", "-NFL", "-NBA"],
}

# Partner name queries for funds that need external source coverage
# Only include funds with DISABLED scrapers or weak website coverage
# Funds with working scrapers (a16z, sequoia, etc.) are covered by fund-level queries
#
# STRATEGY: Cast a WIDE net - the LLM will determine lead status from full article
# Don't require "led by" in the query - many headlines say "backs", "invests", etc.
# but the article body reveals the fund actually led the round.
#
# SYNTAX FIX: Use AND grouping - "Name" AND (term1 OR term2)
# NOT: "Name" term1 OR term2 (parses as ("Name" term1) OR term2)
#
# UPDATED 2026-01: Expanded partner lists based on current team pages
PARTNER_QUERIES = {
    # DISABLED SCRAPER - needs full partner coverage
    # Benchmark partners: https://www.benchmark.com/people
    # Current GPs: Chetan Puttagunta, Peter Fenton, Eric Vishria
    # Former (still newsworthy): Bill Gurley, Sarah Tavel, Miles Grimshaw, Victor Lazarte
    "benchmark": [
        # Fund-level queries (BROAD - catch any funding mention)
        '"Benchmark Capital" AND (funding OR raises OR raised OR investment OR invests)',
        '"Benchmark" AND "Series A" AND (startup OR funding OR million)',
        '"Benchmark" AND "Series B" AND (startup OR funding OR million)',
        '"Benchmark" AND "seed" AND (startup OR funding OR million)',
        '"Benchmark" AND (backs OR backed OR invests) AND startup',
        '"Benchmark" AND (leads OR led) AND (funding OR round OR million)',
        # Current GP partner queries
        '"Chetan Puttagunta" AND (startup OR investment OR funding OR venture)',
        '"Peter Fenton" AND (startup OR investment OR funding OR venture)',
        '"Eric Vishria" AND (startup OR investment OR funding OR venture)',
        # Former GPs (still referenced in recent deals)
        '"Bill Gurley" AND (startup OR investment OR funding OR venture)',
        '"Sarah Tavel" AND (startup OR investment OR funding OR venture)',
        '"Miles Grimshaw" AND (startup OR investment OR funding OR venture)',
        '"Victor Lazarte" AND (startup OR investment OR funding OR venture)',
    ],
    # DISABLED SCRAPER - needs full partner coverage
    # First Round partners: https://firstround.com/team
    # Current: Meka Asonye, Josh Kopelman, Todd Jackson, Brett Berson, Bill Trenchard, Liz Wessel, Hayley Barna
    "first_round": [
        # Fund-level queries (BROAD - catch any funding mention)
        '"First Round Capital" AND (funding OR raises OR raised OR investment)',
        '"First Round Capital" AND "Series A" AND (startup OR million)',
        '"First Round" AND "seed" AND (startup OR funding OR raises)',
        '"First Round" AND (backs OR backed OR invests) AND startup',
        '"First Round" AND (leads OR led) AND (funding OR round)',
        # Current partner queries
        '"Josh Kopelman" AND (startup OR investment OR funding OR venture)',
        '"Todd Jackson" AND (startup OR investment OR funding OR venture)',
        '"Brett Berson" AND (startup OR investment OR funding OR venture)',
        '"Bill Trenchard" AND (startup OR investment OR funding OR venture)',
        '"Meka Asonye" AND (startup OR investment OR funding OR venture)',
        '"Liz Wessel" AND (startup OR investment OR funding OR venture)',
        '"Hayley Barna" AND (startup OR investment OR funding OR venture)',
    ],
    # DISABLED SCRAPER - needs full partner coverage
    # Khosla partners: https://www.khoslaventures.com/team
    # Managing Directors: Vinod Khosla, Samir Kaul, Sven Strohband, David Weiden, Keith Rabois
    # Key Investors: Ethan Choi, Kanu Gulati, Alex Morgan
    "khosla": [
        # Fund-level queries (BROAD - catch any funding mention)
        '"Khosla Ventures" AND (funding OR raises OR raised OR investment)',
        '"Khosla Ventures" AND "Series A" AND (startup OR million)',
        '"Khosla Ventures" AND "Series B" AND (startup OR million)',
        '"Khosla" AND (backs OR backed OR invests) AND startup',
        '"Khosla" AND (leads OR led) AND (funding OR round)',
        # Managing Director queries
        '"Vinod Khosla" AND (startup OR investment OR funding OR venture)',
        '"Samir Kaul" AND (startup OR investment OR funding OR venture)',
        '"Sven Strohband" AND (startup OR investment OR funding OR venture)',
        '"David Weiden" AND (startup OR investment OR funding OR Khosla)',
        '"Keith Rabois" AND Khosla AND (investment OR funding)',
        # Key investor queries
        '"Ethan Choi" AND Khosla AND (investment OR funding OR startup)',
        '"Kanu Gulati" AND (startup OR investment OR funding OR Khosla)',
    ],
    # DISABLED SCRAPER - needs full partner coverage
    # Thrive partners: https://www.thrivecap.com
    # Team: Josh Kushner (founder), Kareem Zaki, Nabil Mallick (COO), Belen Mella, Gaurav Ahuja, Joe Kahn, Kyle Yuan
    "thrive": [
        # Fund-level queries (BROAD - catch any funding mention)
        '"Thrive Capital" AND (funding OR raises OR raised OR investment)',
        '"Thrive Capital" AND (leads OR led) AND (funding OR investment OR round)',
        '"Thrive Capital" AND "Series A" AND (startup OR million)',
        '"Thrive Capital" AND "Series B" AND (startup OR million)',
        '"Thrive Capital" AND "seed" AND (startup OR million)',
        '"Thrive Capital" AND (backs OR backed OR invests)',
        '"led by Thrive" AND (funding OR million OR startup)',
        # Partner queries
        '"Josh Kushner" AND (startup OR investment OR funding OR venture)',
        '"Josh Kushner" AND (leads OR led) AND (funding OR investment)',
        '"Kareem Zaki" AND (startup OR investment OR funding OR venture)',
        '"Nabil Mallick" AND (startup OR investment OR funding OR Thrive)',
        '"Gaurav Ahuja" AND Thrive AND (investment OR funding)',
        '"Belen Mella" AND Thrive AND (investment OR funding)',
    ],
    # WEAK SCRAPER - needs partner backup
    # Founders Fund partners: https://foundersfund.com/team
    # Current: Peter Thiel, Napoleon Ta, Trae Stephens, Lauren Gross, Scott Nolan, John Luttig,
    #          Matias Van Thienen, Delian Asparouhov, Amin Mirzadegan, Joey Krug, Sean Liu
    # Former (removed from queries): Keith Rabois, Brian Singerman
    "founders_fund": [
        # Fund-level queries (BROAD - catch any funding mention)
        '"Founders Fund" AND (funding OR raises OR raised OR investment)',
        '"Founders Fund" AND "Series A" AND (startup OR million)',
        '"Founders Fund" AND "Series B" AND (startup OR million)',
        '"Founders Fund" AND (backs OR backed OR invests)',
        '"Founders Fund" AND (leads OR led) AND (funding OR round)',
        # Current partner queries
        '"Peter Thiel" AND (startup OR investment OR funding OR venture)',
        '"Napoleon Ta" AND (startup OR investment OR funding OR "Founders Fund")',
        '"Trae Stephens" AND (startup OR investment OR funding OR venture)',
        '"Delian Asparouhov" AND (startup OR investment OR funding OR venture)',
        '"Scott Nolan" AND (startup OR investment OR funding OR "Founders Fund")',
        '"John Luttig" AND (startup OR investment OR funding OR "Founders Fund")',
        '"Lauren Gross" AND "Founders Fund" AND (investment OR funding)',
        '"Joey Krug" AND (startup OR investment OR funding OR crypto OR "Founders Fund")',
        # NOTE: Former partners removed (Jan 2026) - their deals attribute to new funds
        # Keith Rabois now at Khosla/independent, Brian Singerman left FF
    ],
    # LOW COVERAGE - needs partner queries
    # FIXED 2026-01: Expanded GV queries - was getting 0 leads due to over-filtering
    # GV partners: https://gv.com/team (44 partners total)
    # Key GPs: David Krane, Krishna Yeshwant, M.G. Siegler, Crystal Huang, Terri Burns,
    #          Frédérique Dame, Sangeen Zeb, Michael McBride (AI focus), Tyson Clark
    "gv": [
        # Fund-level queries - consolidated Jan 2026 (was 9 queries, now 3)
        # Removed: Series A/B/seed (covered by partner queries), participation (disabled)
        '"Google Ventures" AND (funding OR raises OR raised OR investment)',
        '"Google Ventures" AND (leads OR led) AND (funding OR round OR million)',
        '"led by GV" AND (funding OR million OR startup)',
        # Key GP partner queries - David Krane is most prominent
        '"David Krane" AND (startup OR investment OR funding OR venture OR GV)',
        '"David Krane" AND (leads OR led) AND (funding OR investment)',
        # General Partners
        '"M.G. Siegler" AND (startup OR investment OR funding OR venture)',
        '"Crystal Huang" AND (startup OR investment OR funding OR venture OR GV)',
        '"Terri Burns" AND (startup OR investment OR funding OR venture)',
        '"Krishna Yeshwant" AND (startup OR investment OR funding OR GV)',
        '"Frédérique Dame" AND (startup OR investment OR funding OR venture)',
        '"Sangeen Zeb" AND (startup OR investment OR funding OR GV)',
        '"Tyson Clark" AND (startup OR investment OR funding OR GV)',
        # Michael McBride - AI focus (joined from GitLab)
        '"Michael McBride" AND GV AND (investment OR funding OR AI OR startup)',
        '"Michael McBride" AND "Google Ventures" AND (investment OR funding)',
        # Other partners
        '"Brendan Bulik-Sullivan" AND (startup OR investment OR funding OR GV)',
        '"John Lyman" AND GV AND (investment OR funding)',
        '"Issi Rozen" AND GV AND (investment OR funding OR startup)',
    ],
    # LOW COVERAGE - needs partner queries
    # FIXED 2026-01: Expanded Greylock queries - was getting only 1 lead
    # Greylock partners: https://greylock.com/team
    # Partners: Asheem Chandna, Jerry Chen, Mor Chen, Mike Duboe, Reid Hoffman, Christine Kim,
    #           Sophia Luo, Saam Motamedi, Corinne Riley, Jason Risch, Seth Rosenberg, Shreya Shekhar
    # Venture Partners: Josh Elman, John Lilly, Josh McFarland, James Slavet, Mustafa Suleyman, David Thacker
    # Advisory (removed from queries): Bill Helman, David Sze
    # High-profile EIRs (removed from queries): DJ Patil, Jeff Weiner
    "greylock": [
        # Fund-level queries - consolidated Jan 2026 (was 8 queries, now 3)
        # Removed: Series A/B/seed (covered by partner queries), participation (disabled)
        '"Greylock Partners" AND (funding OR raises OR raised OR investment)',
        '"Greylock Partners" AND (leads OR led) AND (funding OR round OR million)',
        '"led by Greylock" AND (funding OR million OR startup)',
        # Reid Hoffman - most prominent partner
        '"Reid Hoffman" AND (startup OR investment OR funding OR venture)',
        '"Reid Hoffman" AND (leads OR led) AND (funding OR investment)',
        # Current Partners
        '"Asheem Chandna" AND (startup OR investment OR funding OR venture)',
        '"Jerry Chen" AND (startup OR investment OR funding OR venture OR Greylock)',
        '"Mor Chen" AND (startup OR investment OR funding OR Greylock)',
        '"Saam Motamedi" AND (startup OR investment OR funding OR venture)',
        '"Seth Rosenberg" AND (startup OR investment OR funding OR venture)',
        '"Mike Duboe" AND (startup OR investment OR funding OR venture)',
        '"Corinne Riley" AND (startup OR investment OR funding OR Greylock)',
        '"Christine Kim" AND (startup OR investment OR funding OR Greylock)',
        '"Sophia Luo" AND (startup OR investment OR funding OR Greylock)',
        '"Jason Risch" AND (startup OR investment OR funding OR Greylock)',
        '"Shreya Shekhar" AND (startup OR investment OR funding OR Greylock)',
        # Venture Partners
        '"Josh Elman" AND (startup OR investment OR funding OR venture)',
        '"Mustafa Suleyman" AND (startup OR investment OR funding OR AI)',
        '"John Lilly" AND (startup OR investment OR funding OR Greylock)',
        '"David Thacker" AND (startup OR investment OR funding OR Greylock)',
        '"Josh McFarland" AND (startup OR investment OR funding OR Greylock)',
        # NOTE: Advisory/EIRs removed (Jan 2026) - they don't lead investment rounds
        # Removed: David Sze (advisory), DJ Patil (EIR), Jeff Weiner (EIR)
    ],
}

# Enterprise AI keywords
ENTERPRISE_KEYWORDS = [
    "AI infrastructure", "LLMOps", "MLOps", "vector database", "AI developer tools",
    "AI security startup", "cybersecurity AI",
    "AI SaaS startup", "enterprise AI startup", "B2B AI startup",
    "AI agents startup", "agentic AI", "AI automation startup", "AI copilot startup",
    "AI analytics startup", "enterprise search AI",
]

# Lead investor phrases - include past AND present tense variations
# FIXED: Added single-word "leads" and "led" for headlines like "Thrive Capital leads $34M"
LEAD_PHRASES = [
    "led by", "led the round", "led the investment",
    "leading the round", "leads the round", "leads the investment",
    "lead investor", "spearheaded by", "co-led by",
    "leads",  # Single word for headlines
    "led",    # Single word for headlines
]

# Participation phrases
PARTICIPATION_PHRASES = [
    "invests in", "invested in", "backs", "backed",
    "joins", "participated", "announces investment", "portfolio company",
]

# Funding keywords
FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "Series A",
    "Series B", "Series C", "seed round", "million",
]

# Stealth keywords
STEALTH_KEYWORDS = [
    "stealth mode", "emerges from stealth", "comes out of stealth",
    "exits stealth", "stealth startup",
]

# Regional queries
REGIONAL_QUERIES = {
    "europe": '"Series A" OR "Series B" Europe startup funding led',
    "uk": '"Series A" OR "Series B" UK startup funding led',
    "germany": '"Series A" OR "Series B" Germany startup funding led',
    "france": '"Series A" OR "Series B" France startup funding led',
    "asia": '"Series A" OR "Series B" Asia startup funding led',
    "india": '"Series A" OR "Series B" India startup funding led',
    "singapore": '"Series A" OR "Series B" Singapore startup funding led',
    "latam": '"Series A" OR "Series B" Latin America startup funding led',
    "brazil": '"Series A" OR "Series B" Brazil startup funding led',
    "israel": '"Series A" OR "Series B" Israel startup funding led',
}

# Early signal queries
EARLY_SIGNAL_QUERIES = [
    '"stealth startup" hiring AI engineer',
    '"backed by" "stealth" hiring',
    '"well-funded" startup hiring AI',
    '"recently raised" hiring engineering',
    'AI startup acquisition announced',
    '"acquired by" AI startup',
    '"announces expansion" AI startup',
    '"opens office" AI startup funded',
    '"hires" "former" Google AI startup',
    '"hires" "former" Meta AI startup',
    '"today announced" funding AI',
    '"secures funding" AI enterprise',
]

# Enterprise category queries
ENTERPRISE_CATEGORY_QUERIES = {
    "infrastructure": '"AI infrastructure" OR "LLMOps" OR "MLOps" OR "vector database"',
    "security": '"AI security" OR "cybersecurity AI" OR "AI threat detection"',
    "vertical_saas": '"AI SaaS" OR "enterprise AI" OR "B2B AI"',
    "agentic": '"AI agents" OR "agentic AI" OR "AI automation" OR "AI copilot"',
    "data_intelligence": '"AI analytics" OR "enterprise search AI" OR "knowledge management AI"',
}


@dataclass
class BraveSearchResult:
    """Single search result from Brave API."""
    title: str
    url: str
    description: str
    published_date: Optional[date]
    source: str
    extra_snippets: List[str]


class BraveSearchScraper:
    """
    Search for funding news via Brave Search API.

    OPTIMIZED:
    - Uses shared BraveClient (no duplicate HTTP client)
    - Parallel query execution with semaphore
    - TTL caching for partner queries
    """

    def __init__(self):
        self.rate_limit_delay = settings.brave_search_rate_limit_delay
        # Validate API key at init time for early failure
        if not settings.brave_search_key:
            logger.warning("BRAVE_SEARCH_KEY not configured - BraveSearchScraper will return empty results")

    async def __aenter__(self):
        # Uses module-level shared HTTP client - no instance client needed
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Shared client is managed at module level, not closed here
        pass

    async def fetch_full_article(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch full article content from URL with retry logic.

        This is critical for proper deal extraction - search snippets are too
        short for the LLM to accurately determine lead investor status.

        FIX: Added retry logic for 5xx errors and improved timeout handling.
        FIX: Uses shared module-level HTTP client to avoid resource leaks.
        """
        client = await _get_article_fetch_client()
        for attempt in range(max_retries):
            try:
                resp = await client.get(url, headers={
                    "User-Agent": USER_AGENT_BOT,
                    "Accept": "text/html,application/xhtml+xml",
                })

                # 4xx errors - don't retry (article doesn't exist/paywall)
                if 400 <= resp.status_code < 500:
                    logger.debug(f"HTTP {resp.status_code} for {url} - not retrying")
                    return None

                # 5xx errors - retry with backoff
                if resp.status_code >= 500:
                    if attempt < max_retries - 1:
                        delay = 2 ** attempt
                        logger.warning(f"HTTP {resp.status_code} fetching {url}, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.debug(f"HTTP {resp.status_code} for {url} after {max_retries} attempts")
                        return None

                if resp.status_code != 200:
                    logger.debug(f"HTTP {resp.status_code} for {url}")
                    return None

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Remove noise elements
                for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
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
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.debug(f"Timeout fetching {url} (attempt {attempt + 1}/{max_retries}), retrying in {delay}s")
                    await asyncio.sleep(delay)
                else:
                    logger.debug(f"Timeout fetching {url} after {max_retries} attempts")
                    return None
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
                return None

        return None

    def _build_fund_query(self, fund_slug: str, query_type: str = "lead") -> str:
        """Build optimized search query for a specific fund.

        IMPORTANT: Keep queries SHORT to avoid Brave API 422 errors.
        Use minimal OR clauses - the LLM will determine lead status from full article.
        """
        fund_names = FUND_SEARCH_NAMES.get(fund_slug, [])
        if not fund_names:
            return ""

        # Use only primary fund name to keep query short
        name_query = f'"{fund_names[0]}"'
        # Minimal funding keywords - LLM extracts details from full article
        funding_query = 'funding OR raises OR "Series A" OR "Series B"'
        exclusions = EXCLUSION_TERMS.get(fund_slug, [])
        exclusion_query = " ".join(exclusions) if exclusions else ""

        if query_type == "lead":
            # SIMPLIFIED: Just 3 core lead phrases to avoid 422 errors
            # The LLM will determine lead status from full article content
            lead_query = '"led by" OR leads OR led'
            query = f'{name_query} AND ({lead_query}) AND ({funding_query})'
        else:
            participation_query = 'backs OR invests OR invested'
            query = f'{name_query} AND ({participation_query}) AND ({funding_query})'

        if exclusion_query:
            query = f'{query} {exclusion_query}'

        return query

    def _build_enterprise_query(self, category: str = None) -> str:
        """Build Enterprise AI funding query.

        IMPORTANT: Keep queries SHORT to avoid Brave API 422 errors.
        """
        if category and category in ENTERPRISE_CATEGORY_QUERIES:
            keywords = ENTERPRISE_CATEGORY_QUERIES[category]
        else:
            # Simplified: Just core enterprise AI terms
            keywords = '"enterprise AI" OR "AI startup" OR "AI infrastructure"'

        # Simplified lead + funding query
        return f'({keywords}) AND (funding OR raises) AND (led OR leads)'

    def _build_stealth_query(self) -> str:
        """Build query for stealth startup funding news.

        IMPORTANT: Keep queries SHORT to avoid Brave API 422 errors.
        """
        return '"stealth startup" OR "emerges from stealth" AND funding'

    def _parse_news_results(self, data: Optional[Dict[str, Any]]) -> List[BraveSearchResult]:
        """Parse Brave news API response with validation."""
        if data is None:
            return []

        results = []

        if "error" in data:
            error_msg = data.get("error", {})
            if isinstance(error_msg, dict):
                logger.error(f"Brave API error: {error_msg.get('message', 'Unknown error')}")
            else:
                logger.error(f"Brave API error: {error_msg}")
            return []

        if "results" not in data:
            if "web" in data and "results" in data.get("web", {}):
                data["results"] = data["web"]["results"]
            else:
                return []

        raw_results = data.get("results", [])
        if not isinstance(raw_results, list):
            return []

        for item in raw_results:
            # Memory safety check
            if len(results) >= MAX_RESULTS_PER_QUERY:
                logger.warning(f"Hit max results limit ({MAX_RESULTS_PER_QUERY})")
                break

            try:
                url = item.get("url", "")
                if not url:
                    continue

                pub_date = None
                age = item.get("age", "")
                if age:
                    pub_date = self._parse_relative_date(age)

                meta_url = item.get("meta_url")
                source = meta_url.get("hostname", "") if isinstance(meta_url, dict) else ""

                results.append(BraveSearchResult(
                    title=item.get("title", ""),
                    url=url,
                    description=item.get("description", ""),
                    published_date=pub_date,
                    source=source,
                    extra_snippets=item.get("extra_snippets", []),
                ))
            except (TypeError, KeyError, AttributeError) as e:
                logger.warning(f"Error parsing Brave result: {e}")
                continue

        return results

    def _parse_relative_date(self, age_str: str) -> Optional[date]:
        """Parse relative date string like '2 days ago'.

        FIX: Uses UTC timezone instead of local timezone to ensure consistent
        date handling across different server locations.
        """
        try:
            age_lower = age_str.lower()
            # Use UTC date to avoid timezone-related off-by-one errors
            today = datetime.now(timezone.utc).date()

            if "hour" in age_lower or "minute" in age_lower:
                return today
            elif "day" in age_lower:
                days = int("".join(filter(str.isdigit, age_str)) or "1")
                return today - timedelta(days=days)
            elif "week" in age_lower:
                weeks = int("".join(filter(str.isdigit, age_str)) or "1")
                return today - timedelta(weeks=weeks)
            elif "month" in age_lower:
                months = int("".join(filter(str.isdigit, age_str)) or "1")
                return today - timedelta(days=months * 30)
            else:
                return today
        except Exception:
            return None

    async def search_news(
        self,
        query: str,
        count: int = 20,
        freshness: str = "pw",
        use_cache: bool = False,
    ) -> List[BraveSearchResult]:
        """Execute news search using shared client."""
        client = get_brave_client()
        data = await client.search_news(query, count, freshness, use_cache)
        return self._parse_news_results(data)

    async def search_all_funds_parallel(
        self,
        freshness: str = "pw",
        include_participation: bool = True,
    ) -> Dict[str, List[BraveSearchResult]]:
        """
        Search all 18 tracked funds with PARALLEL execution.

        OPTIMIZED: Uses batch queries with 3x concurrency.
        Before: 36 queries × 0.5s = 18s
        After: 36 queries / 3 × 0.3s ≈ 4s
        """
        # Build all queries upfront
        queries: List[Tuple[str, str, int, str, bool]] = []
        query_to_fund: Dict[str, Tuple[str, str]] = {}  # query -> (fund_slug, type)

        for fund_slug in FUND_SEARCH_NAMES.keys():
            # Lead query - API cost is per-query not per-result, so maximize coverage
            lead_query = self._build_fund_query(fund_slug, "lead")
            if lead_query:
                queries.append((lead_query, "news", 25, freshness, True))  # use_cache=True
                query_to_fund[lead_query] = (fund_slug, "lead")

            # Participation query
            if include_participation:
                part_query = self._build_fund_query(fund_slug, "participation")
                if part_query:
                    queries.append((part_query, "news", 25, freshness, True))  # use_cache=True
                    query_to_fund[part_query] = (fund_slug, "participation")

        # Execute in parallel
        client = get_brave_client()
        batch_results = await client.search_batch(queries, max_concurrent=3, delay_between=self.rate_limit_delay)

        # Process results
        results: Dict[str, List[BraveSearchResult]] = {}
        for query, data in batch_results.items():
            if query in query_to_fund:
                fund_slug, query_type = query_to_fund[query]
                key = f"{fund_slug}_participation" if query_type == "participation" else fund_slug
                results[key] = self._parse_news_results(data)

        return results

    # Funds with disabled/weak website scrapers - covered by partner queries instead
    EXTERNAL_ONLY_FUNDS = {"benchmark", "thrive", "first_round", "khosla", "founders_fund", "gv", "greylock"}

    async def search_partner_names_parallel(
        self,
        freshness: str = "pw",
    ) -> Dict[str, List[BraveSearchResult]]:
        """
        Search by partner names with PARALLEL execution and CACHING.

        OPTIMIZED:
        - Uses batch queries with 3x concurrency
        - News API only (Web API added marginal value for funding news)
        - Caches results for 12 hours

        COST OPTIMIZATION (Jan 2026): Removed Web API queries for external-only
        funds. Funding announcements are news events covered by News API.
        Web API returned ~80% duplicates. Savings: ~$9.50/month.
        """
        # Build all queries upfront
        queries: List[Tuple[str, str, int, str, bool]] = []
        query_to_fund: Dict[str, str] = {}  # query -> fund_slug

        for fund_slug, partner_queries in PARTNER_QUERIES.items():
            for query in partner_queries:
                # News API only - funding announcements are news events
                queries.append((query, "news", 25, freshness, True))
                query_to_fund[query] = fund_slug

        # Execute in parallel with caching
        client = get_brave_client()
        batch_results = await client.search_batch(queries, max_concurrent=3, delay_between=self.rate_limit_delay)

        # Process results, deduplicating by URL per fund
        results: Dict[str, List[BraveSearchResult]] = {}
        fund_seen_urls: Dict[str, set] = {}

        for query, data in batch_results.items():
            if query in query_to_fund:
                fund_slug = query_to_fund[query]
                if fund_slug not in results:
                    results[fund_slug] = []
                    fund_seen_urls[fund_slug] = set()

                # Parse results (works for both news and web responses)
                for result in self._parse_news_results(data):
                    if result.url not in fund_seen_urls[fund_slug]:
                        fund_seen_urls[fund_slug].add(result.url)
                        results[fund_slug].append(result)

        return results

    async def search_regional_parallel(
        self,
        freshness: str = "pw",
    ) -> Dict[str, List[BraveSearchResult]]:
        """Search regional markets with PARALLEL execution."""
        queries: List[Tuple[str, str, int, str, bool]] = []
        query_to_region: Dict[str, str] = {}

        for region, query in REGIONAL_QUERIES.items():
            queries.append((query, "news", 20, freshness, False))
            query_to_region[query] = region

        client = get_brave_client()
        batch_results = await client.search_batch(queries, max_concurrent=3, delay_between=self.rate_limit_delay)

        results: Dict[str, List[BraveSearchResult]] = {}
        for query, data in batch_results.items():
            if query in query_to_region:
                region = query_to_region[query]
                results[region] = self._parse_news_results(data)

        return results

    async def search_early_signals_parallel(
        self,
        freshness: str = "pd",
    ) -> List[BraveSearchResult]:
        """Search early signals with PARALLEL execution."""
        queries: List[Tuple[str, str, int, str, bool]] = [
            (query, "news", 15, freshness, True)  # use_cache=True
            for query in EARLY_SIGNAL_QUERIES
        ]

        client = get_brave_client()
        batch_results = await client.search_batch(queries, max_concurrent=3, delay_between=self.rate_limit_delay)

        all_results: List[BraveSearchResult] = []
        seen_urls: set = set()

        for data in batch_results.values():
            for result in self._parse_news_results(data):
                if result.url not in seen_urls:
                    seen_urls.add(result.url)
                    all_results.append(result)

        return all_results

    async def search_enterprise_ai(
        self,
        freshness: str = "pw",
    ) -> List[BraveSearchResult]:
        """Search for Enterprise AI funding news."""
        query = self._build_enterprise_query()
        # Reduced from 50 to 30 for token savings
        return await self.search_news(query, count=30, freshness=freshness)

    async def search_stealth(
        self,
        freshness: str = "pw",
    ) -> List[BraveSearchResult]:
        """Search for stealth startup funding news."""
        query = self._build_stealth_query()
        # Reduced from 30 to 20 for token savings
        return await self.search_news(query, count=20, freshness=freshness)

    async def to_normalized_article(
        self,
        result: BraveSearchResult,
        fund_slug: str = "",
        fetch_full_content: bool = True,
    ) -> NormalizedArticle:
        """Convert search result to NormalizedArticle.

        If fetch_full_content is True (default), fetches the full article HTML
        and extracts text. This is critical for proper LLM extraction - snippets
        are too short to determine lead investor status accurately.
        """
        # Try to fetch full article content
        text = None
        if fetch_full_content:
            text = await self.fetch_full_article(result.url)

        # Fallback to search snippets if fetch failed
        if not text:
            text_parts = [result.description]
            text_parts.extend(result.extra_snippets)
            text = "\n\n".join(filter(None, text_parts))
            logger.debug(f"Using snippet for {result.url} ({len(text)} chars)")

        return NormalizedArticle(
            url=result.url,
            title=result.title,
            text=text,
            published_date=result.published_date,
            author=result.source,
            tags=["brave_search", result.source],
            fund_slug=fund_slug,
            fetched_at=datetime.now(timezone.utc),
        )

    async def to_normalized_articles_batch(
        self,
        results: List[Tuple[BraveSearchResult, str]],
        extra_tags: Optional[List[str]] = None,
    ) -> List[NormalizedArticle]:
        """
        Convert multiple search results to NormalizedArticles in PARALLEL.

        FIX 2026-01: Article fetching was sequential, causing ~80s delay.
        Now uses semaphore-limited parallel fetching for 5x speedup.

        Args:
            results: List of (BraveSearchResult, fund_slug) tuples
            extra_tags: Optional tags to add to all articles

        Returns:
            List of NormalizedArticle objects
        """
        if not results:
            return []

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ARTICLES)

        async def fetch_one(result: BraveSearchResult, fund_slug: str) -> NormalizedArticle:
            """Fetch article with semaphore-based concurrency control.

            FIX: Moved delay outside semaphore to avoid blocking other concurrent tasks.
            """
            async with semaphore:
                article = await self.to_normalized_article(result, fund_slug)
                if extra_tags:
                    article.tags.extend(extra_tags)
            # Small delay AFTER releasing semaphore for politeness
            await asyncio.sleep(ARTICLE_RATE_LIMIT_DELAY)
            return article

        tasks = [fetch_one(result, fund_slug) for result, fund_slug in results]
        articles = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and log them
        valid_articles = []
        for i, article in enumerate(articles):
            if isinstance(article, Exception):
                logger.warning(f"Error fetching article: {article}")
            else:
                valid_articles.append(article)

        logger.info(f"Fetched {len(valid_articles)}/{len(results)} articles in parallel")
        return valid_articles

    async def scrape_all(
        self,
        freshness: str = "pw",
        include_enterprise: bool = True,
        include_participation: bool = True,
        include_stealth: bool = True,
        include_regional: bool = False,
        include_early_signals: bool = False,
        include_partner_names: bool = True,
    ) -> List[NormalizedArticle]:
        """
        Full scraping pipeline with PARALLEL execution.

        OPTIMIZED:
        - Parallel fund queries: 18s → 4s
        - Parallel partner queries: 23.5s → 5s (+ caching)
        - Parallel regional queries: 5s → 1s
        - Parallel article fetching: 80s → 16s (5x concurrent)
        - Total: ~42s → ~15s (queries) + ~16s (articles) = ~31s
        """
        # Phase 1: Collect all search results (parallel queries)
        # Store as (result, fund_slug, extra_tags) tuples for batch processing
        pending_results: List[Tuple[BraveSearchResult, str, List[str]]] = []
        seen_urls: set = set()

        def add_result(result: BraveSearchResult, fund_slug: str, extra_tags: List[str] = None):
            """Add result if URL not seen."""
            if result.url not in seen_urls:
                seen_urls.add(result.url)
                pending_results.append((result, fund_slug, extra_tags or []))

        # Parallel fund search (lead + participation)
        fund_results = await self.search_all_funds_parallel(freshness, include_participation)
        for fund_slug, results in fund_results.items():
            actual_fund_slug = fund_slug.replace("_participation", "")
            for result in results:
                add_result(result, actual_fund_slug)

        # Enterprise AI (single query)
        if include_enterprise:
            for result in await self.search_enterprise_ai(freshness):
                add_result(result, "")

        # Stealth startups (single query)
        if include_stealth:
            for result in await self.search_stealth(freshness):
                add_result(result, "", ["stealth"])

        # Parallel regional search
        if include_regional:
            regional_results = await self.search_regional_parallel(freshness)
            for region, results in regional_results.items():
                for result in results:
                    add_result(result, "", ["regional", region])

        # Parallel early signals search
        if include_early_signals:
            for result in await self.search_early_signals_parallel("pd"):
                add_result(result, "", ["early_signal"])

        # Parallel partner names search with caching
        if include_partner_names:
            partner_freshness = "pm" if freshness in ("pd", "pw") else freshness
            partner_results = await self.search_partner_names_parallel(partner_freshness)
            for fund_slug, results in partner_results.items():
                for result in results:
                    add_result(result, fund_slug, ["partner_search"])

        # Log query stats
        cache = get_query_cache()
        logger.info(f"Query cache size: {cache.size()} entries")
        logger.info(f"Found {len(pending_results)} unique URLs to fetch")

        # Phase 2: Fetch all articles in PARALLEL (5x speedup)
        # Group by extra_tags for efficient batch processing
        all_articles: List[NormalizedArticle] = []

        # Process in batches by tag type for cleaner logging
        tag_groups: Dict[str, List[Tuple[BraveSearchResult, str]]] = {}
        for result, fund_slug, extra_tags in pending_results:
            tag_key = ",".join(sorted(extra_tags)) if extra_tags else "default"
            if tag_key not in tag_groups:
                tag_groups[tag_key] = []
            tag_groups[tag_key].append((result, fund_slug))

        for tag_key, batch in tag_groups.items():
            extra_tags = tag_key.split(",") if tag_key != "default" else None
            articles = await self.to_normalized_articles_batch(batch, extra_tags)
            all_articles.extend(articles)

        # SCRAPER_HEALTH_ALERT: Log warning when scraper returns 0 articles
        if not all_articles:
            logger.warning(
                "SCRAPER_HEALTH_ALERT: brave_search returned 0 articles - "
                "API may be rate-limited or search queries may need adjustment"
            )

        return all_articles


# Convenience function
async def run_brave_search_scraper(freshness: str = "pw") -> List[NormalizedArticle]:
    """Run Brave Search scraper and return normalized articles."""
    async with BraveSearchScraper() as scraper:
        return await scraper.scrape_all(freshness=freshness)
