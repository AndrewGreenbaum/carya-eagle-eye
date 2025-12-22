"""
Delaware Division of Corporations - Catch New Tech Company Formations.

Monitors Delaware's Division of Corporations for new entity filings.
~67% of Fortune 500 and most VC-backed startups incorporate in Delaware.

Why Delaware?
- Business-friendly laws
- Court of Chancery (specialized business court)
- Privacy (officers/directors not public)
- Standard for VC investment

This scraper looks for:
1. New corporations with tech-sounding names
2. Entity name patterns (Labs, AI, Technologies, etc.)
3. Registered agent signals (startup-friendly agents)

Note: Delaware doesn't provide a public API, so we use:
1. Brave Search for recent Delaware incorporation news
2. Third-party aggregators that track filings
"""

import asyncio
import logging
import re
import httpx
from bs4 import BeautifulSoup
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Dict, Any

from ..base_scraper import NormalizedArticle
from ...common.brave_client import get_brave_client
from ...config.settings import settings

logger = logging.getLogger(__name__)

# Rate limiting and retry configuration for OpenCorporates (not Brave - uses shared client)
OPENCORP_REQUEST_DELAY = 0.5  # 500ms between OpenCorporates requests
MAX_RETRIES = 3  # Maximum retry attempts for transient failures
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier


# Tech company name patterns
TECH_NAME_PATTERNS = [
    r'\b(AI|ML|Labs?|Tech|Technologies|Systems|Software|Platform|Cloud|Data|Analytics|Cyber|Security)\b',
    r'\b(Automation|Robotics|Bio|Health|Med|Fin|Dev|Ops|Infrastructure)\b',
    r'\b(Intelligence|Agent|Bot|Quantum|Neural|Deep|Machine)\b',
]

# Registered agents popular with startups (signals VC-backed)
STARTUP_FRIENDLY_AGENTS = [
    "Corporation Service Company",
    "CSC",
    "CT Corporation",
    "Registered Agents Inc",
    "Harvard Business Services",
    "Delaware Registered Agent",
    "Incorp Services",
    "Northwest Registered Agent",
    "Rocket Lawyer",
    "LegalZoom",
    "Stripe Atlas",  # Huge signal for tech startups
    "Clerky",  # YC favorite
]

# Search query templates for finding Delaware incorporations
# Use get_delaware_queries() to get queries with current year
_DELAWARE_QUERY_TEMPLATES = [
    '"Delaware corporation" "artificial intelligence" formed {year}',
    '"incorporated in Delaware" startup AI OR "machine learning"',
    '"Delaware LLC" technology startup founded {year}',
    'site:icis.corp.delaware.gov new filing technology',
    '"Stripe Atlas" startup Delaware incorporated',
    '"Delaware C-Corp" seed funding OR "Series A"',
    '"newly incorporated" Delaware tech startup',
]


def get_delaware_queries() -> List[str]:
    """Get Delaware search queries with current year substituted."""
    current_year = date.today().year
    return [q.format(year=current_year) for q in _DELAWARE_QUERY_TEMPLATES]


def _get_brave_freshness(days_back: int) -> str:
    """Map days_back to Brave Search freshness parameter."""
    if days_back <= 1:
        return "pd"  # Past day
    elif days_back <= 7:
        return "pw"  # Past week
    else:
        return "pm"  # Past month


@dataclass
class DelawareEntity:
    """Delaware corporate entity filing."""
    entity_name: str
    entity_type: str  # Corporation, LLC, LP, etc.
    file_number: Optional[str]
    formation_date: Optional[date]
    registered_agent: Optional[str]
    status: str  # Active, Good Standing, etc.
    source_url: str

    # Signals
    has_tech_name: bool = False
    has_startup_agent: bool = False


class DelawareCorpsScraper:
    """
    Monitor Delaware Division of Corporations for new tech company formations.

    Since Delaware doesn't provide a public API, we use:
    1. Brave Search to find recent incorporation news
    2. Third-party aggregators
    3. News about companies incorporating
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml",
            },
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def _opencorp_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute OpenCorporates API request with retry logic.

        Handles: rate limiting (429), server errors (5xx), timeouts.
        Returns parsed JSON or None on failure.
        """
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                response = await self.client.get(url, params=params)

                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError as e:
                        logger.warning(f"OpenCorporates JSON parse error: {e}")
                        return None  # No retry for parse errors

                elif response.status_code == 429:
                    # Rate limited - use Retry-After or default wait
                    retry_after_header = response.headers.get("Retry-After", "30")
                    try:
                        retry_after = int(retry_after_header)
                    except ValueError:
                        logger.warning(f"Non-numeric Retry-After header: {retry_after_header}")
                        retry_after = 30
                    logger.warning(f"OpenCorporates rate limited (429), waiting {retry_after}s")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(retry_after)

                elif response.status_code in (401, 403):
                    logger.error(f"OpenCorporates auth error ({response.status_code})")
                    return None  # Auth errors won't recover

                elif response.status_code >= 500:
                    # Server error - retry with backoff
                    last_error = f"HTTP {response.status_code}"
                    if attempt < MAX_RETRIES - 1:
                        wait_time = OPENCORP_REQUEST_DELAY * (RETRY_BACKOFF ** attempt)
                        logger.warning(
                            f"OpenCorporates server error ({response.status_code}), "
                            f"retry {attempt + 1}/{MAX_RETRIES} in {wait_time}s"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.warning(
                            f"OpenCorporates server error ({response.status_code}), "
                            f"max retries exhausted"
                        )

                else:
                    logger.warning(f"OpenCorporates unexpected status {response.status_code}")
                    return None  # Unknown error - don't retry

            except httpx.TimeoutException:
                last_error = "timeout"
                if attempt < MAX_RETRIES - 1:
                    wait_time = OPENCORP_REQUEST_DELAY * (RETRY_BACKOFF ** attempt)
                    logger.warning(
                        f"OpenCorporates timeout, retry {attempt + 1}/{MAX_RETRIES} in {wait_time}s"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning("OpenCorporates timeout, max retries exhausted")

            except httpx.ConnectError as e:
                logger.error(f"OpenCorporates connection error: {e}")
                return None  # Network issues unlikely to resolve quickly

        if last_error:
            logger.warning(f"OpenCorporates request failed after {MAX_RETRIES} attempts: {last_error}")
        return None

    def _has_tech_name(self, name: str) -> bool:
        """Check if entity name suggests a tech company."""
        for pattern in TECH_NAME_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return True
        return False

    def _has_startup_agent(self, agent: str) -> bool:
        """Check if registered agent is startup-friendly."""
        if not agent:
            return False
        agent_lower = agent.lower()
        return any(sa.lower() in agent_lower for sa in STARTUP_FRIENDLY_AGENTS)

    def _deduplicate_entities(self, entities: List[DelawareEntity]) -> List[DelawareEntity]:
        """Deduplicate entities by normalized name."""
        seen_names = set()
        unique_entities = []
        for entity in entities:
            name_key = entity.entity_name.lower().strip()
            if name_key not in seen_names:
                seen_names.add(name_key)
                unique_entities.append(entity)
        return unique_entities

    async def search_recent_incorporations(
        self,
        days_back: int = 7,
    ) -> List[DelawareEntity]:
        """
        Search for recent Delaware incorporations via shared BraveClient.

        Args:
            days_back: How many days to look back (1=past day, 7=past week, 30+=past month)

        Returns:
            List of unique DelawareEntity objects (not limited - caller should filter/score)
        """
        entities = []

        brave_client = get_brave_client()
        if not brave_client.validate_api_key():
            logger.warning("BRAVE_SEARCH_KEY not configured for Delaware search")
            return entities

        # Get freshness based on days_back
        freshness = _get_brave_freshness(days_back)

        # Limit to 3 queries to balance coverage vs Brave API cost
        # Full 7 queries would ~2x API spend with diminishing returns
        # Primary signals (tech formation, startup incorporation, AI) covered
        queries = get_delaware_queries()
        for query in queries[:3]:
            # Use shared BraveClient (handles retry, rate limiting, caching)
            data = await brave_client.search_web(
                query=query,
                count=20,
                freshness=freshness,
                use_cache=True,  # Cache Delaware queries
            )

            if data is None:
                continue

            # Handle null results (key exists but value is None)
            results = data.get("web", {}).get("results") or []
            for result in results:
                entity = self._parse_search_result(result)
                if entity:
                    entities.append(entity)

            await brave_client.delay()  # Rate limit between queries

        # Deduplicate and return all (caller handles scoring/limiting)
        return self._deduplicate_entities(entities)

    def _parse_search_result(self, result: Dict[str, Any]) -> Optional[DelawareEntity]:
        """Parse Brave search result into DelawareEntity."""
        try:
            url = result.get("url", "")
            title = result.get("title", "")
            description = result.get("description", "")

            # Try to extract company name
            # Look for patterns like "Company Name Inc" or "Company Name, Inc."
            text = f"{title} {description}"

            # Try various patterns to extract company name
            name_patterns = [
                r'"([^"]+(?:Inc\.?|LLC|Corp\.?|Technologies|Labs|AI))"',
                r'([A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+)*(?:\s+(?:Inc\.?|LLC|Corp\.?|Technologies|Labs|AI)))',
                r'startup\s+([A-Z][a-zA-Z0-9]+)',
                r'company\s+([A-Z][a-zA-Z0-9]+)',
            ]

            entity_name = None
            for pattern in name_patterns:
                match = re.search(pattern, text)
                if match:
                    entity_name = match.group(1).strip()
                    break

            if not entity_name:
                # Use title as fallback, clean it up
                # FIX: Only strip common suffixes, not all dashes (preserves "Name-Finder" style names)
                entity_name = re.sub(
                    r'\s*[-|]\s*(Delaware|Corporation|Corp|Inc|LLC|Filing|Incorporation|State|Division).*$',
                    '',
                    title,
                    flags=re.IGNORECASE
                ).strip()

            if not entity_name or len(entity_name) < 2 or len(entity_name) > 100:
                return None

            # Check for tech indicators
            has_tech = self._has_tech_name(entity_name) or self._has_tech_name(text)

            # Only return if it has tech indicators
            if not has_tech:
                return None

            # Check for startup-friendly agent mentions
            has_startup_agent = any(
                agent.lower() in text.lower()
                for agent in STARTUP_FRIENDLY_AGENTS
            )

            return DelawareEntity(
                entity_name=entity_name,
                entity_type="Corporation",  # Assume corp unless specified
                file_number=None,
                formation_date=None,  # FIX: Don't assume today - unknown from search results
                registered_agent=None,
                status="Active",
                source_url=url,
                has_tech_name=has_tech,
                has_startup_agent=has_startup_agent,
            )

        except (TypeError, AttributeError, ValueError) as e:
            # FIX: Catch specific exceptions instead of broad Exception
            logger.debug(f"Error parsing Delaware search result: {e}")
            return None

    async def search_aggregators(self, max_pages: int = 1) -> List[DelawareEntity]:
        """
        Search third-party aggregators for Delaware filings.

        Note: Most aggregators require paid access. This searches
        free sources like OpenCorporates.

        Args:
            max_pages: Number of result pages to fetch (30 results/page)
        """
        entities = []

        for page in range(1, max_pages + 1):
            # Use shared retry logic via _opencorp_request()
            data = await self._opencorp_request(
                "https://api.opencorporates.com/v0.4/companies/search",
                params={
                    "q": "technology",
                    "jurisdiction_code": "us_de",
                    "order": "incorporation_date",
                    "per_page": 30,
                    "page": page,
                },
            )

            if data is None:
                break  # Request failed, stop pagination

            # Handle null companies (key exists but value is None)
            companies = data.get("results", {}).get("companies") or []
            if not companies:
                break  # No more results

            for company in companies:
                c = company.get("company", {})
                name = c.get("name", "")

                if self._has_tech_name(name):
                    inc_date = c.get("incorporation_date")
                    formation_date = None
                    if inc_date:
                        try:
                            formation_date = datetime.strptime(inc_date, "%Y-%m-%d").date()
                        except ValueError:
                            logger.debug(f"Invalid date format for {name}: {inc_date}")

                    entities.append(DelawareEntity(
                        entity_name=name,
                        entity_type=c.get("company_type", "Corporation"),
                        file_number=c.get("company_number"),
                        formation_date=formation_date,
                        registered_agent=c.get("registered_agent_name"),
                        status=c.get("current_status", "Active"),
                        source_url=c.get("opencorporates_url", ""),
                        has_tech_name=True,
                        has_startup_agent=self._has_startup_agent(
                            c.get("registered_agent_name", "")
                        ),
                    ))

            # Rate limit between pages
            if page < max_pages:
                await asyncio.sleep(OPENCORP_REQUEST_DELAY)

        return entities

    async def search_by_company_name(
        self,
        company_name: str,
        fuzzy_match: bool = True,
    ) -> Optional[DelawareEntity]:
        """
        Search for a specific company in Delaware registry.

        Used for cross-referencing SEC Form D filings with Delaware data.

        Args:
            company_name: Company name to search for
            fuzzy_match: If True, returns best match even if not exact

        Returns:
            DelawareEntity if found, None otherwise
        """
        if not company_name or len(company_name) < 2:
            return None

        # Clean company name for search
        clean_name = company_name.strip()
        # Remove common suffixes for better matching
        clean_name = re.sub(
            r'\s*(Inc\.?|LLC|Corp\.?|Corporation|Ltd\.?)$',
            '',
            clean_name,
            flags=re.IGNORECASE
        ).strip()

        # Use shared retry logic via _opencorp_request()
        data = await self._opencorp_request(
            "https://api.opencorporates.com/v0.4/companies/search",
            params={
                "q": clean_name,
                "jurisdiction_code": "us_de",
                "per_page": 5,
            },
        )

        if data is None:
            return None  # Request failed

        # Handle null companies (key exists but value is None)
        companies = data.get("results", {}).get("companies") or []

        for company in companies:
            c = company.get("company", {})
            name = c.get("name", "")

            # Check for exact or fuzzy match
            name_lower = name.lower()
            search_lower = clean_name.lower()

            is_match = False
            if search_lower in name_lower or name_lower in search_lower:
                is_match = True
            elif fuzzy_match:
                # Check if main words match
                search_words = set(search_lower.split())
                name_words = set(name_lower.split())
                if len(search_words & name_words) >= min(2, len(search_words)):
                    is_match = True

            if is_match:
                inc_date = c.get("incorporation_date")
                formation_date = None
                if inc_date:
                    try:
                        formation_date = datetime.strptime(inc_date, "%Y-%m-%d").date()
                    except ValueError:
                        logger.debug(f"Invalid date format for {company_name}: {inc_date}")

                agent_name = c.get("registered_agent_name", "")

                return DelawareEntity(
                    entity_name=name,
                    entity_type=c.get("company_type", "Corporation"),
                    file_number=c.get("company_number"),
                    formation_date=formation_date,
                    registered_agent=agent_name,
                    status=c.get("current_status", "Active"),
                    source_url=c.get("opencorporates_url", ""),
                    has_tech_name=self._has_tech_name(name),
                    has_startup_agent=self._has_startup_agent(agent_name),
                )

        return None  # No match found

    async def search_companies_batch(
        self,
        company_names: List[str],
        delay_seconds: float = 0.5,
    ) -> Dict[str, Optional[DelawareEntity]]:
        """
        Search for multiple companies in Delaware registry.

        Args:
            company_names: List of company names to search
            delay_seconds: Delay between API calls (rate limiting)

        Returns:
            Dict mapping company name to DelawareEntity (or None if not found)
        """
        results = {}
        found_count = 0

        for name in company_names:
            entity = await self.search_by_company_name(name)
            results[name] = entity

            if entity:
                found_count += 1
            else:
                logger.debug(f"Delaware lookup: no match for '{name}'")

            await asyncio.sleep(delay_seconds)  # Rate limit

        logger.info(f"Delaware batch lookup: {found_count}/{len(company_names)} found")
        return results

    def entity_to_article(self, entity: DelawareEntity) -> NormalizedArticle:
        """Convert Delaware entity to NormalizedArticle."""
        signals = []
        if entity.has_tech_name:
            signals.append("Tech company name pattern")
        if entity.has_startup_agent:
            signals.append("Startup-friendly registered agent")

        text_parts = [
            f"[DELAWARE INCORPORATION DETECTED]",
            f"",
            f"Entity: {entity.entity_name}",
            f"Type: {entity.entity_type}",
            f"Status: {entity.status}",
        ]

        if entity.file_number:
            text_parts.append(f"File Number: {entity.file_number}")
        if entity.formation_date:
            text_parts.append(f"Formation Date: {entity.formation_date}")
        if entity.registered_agent:
            text_parts.append(f"Registered Agent: {entity.registered_agent}")

        text_parts.extend([
            f"",
            f"Signals: {', '.join(signals) if signals else 'None'}",
            f"",
            f"Note: ~67% of VC-backed startups incorporate in Delaware.",
            f"New tech company formations may indicate upcoming funding.",
        ])

        if entity.source_url:
            text_parts.append(f"\nSource: {entity.source_url}")

        tags = ["delaware", "incorporation", "stealth_signal"]
        if entity.has_tech_name:
            tags.append("tech_name")
        if entity.has_startup_agent:
            tags.append("startup_agent")

        return NormalizedArticle(
            url=entity.source_url or f"https://icis.corp.delaware.gov/",
            title=f"Delaware Filing: {entity.entity_name}",
            text="\n".join(text_parts),
            # FIX: Use None instead of date.today() for unknown formation dates
            # (date.today() misleadingly marks entities as just formed)
            published_date=entity.formation_date,
            author="Delaware Division of Corporations",
            tags=tags,
            fund_slug="",
            # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
            fetched_at=datetime.now(timezone.utc),
        )

    def score_entity(self, entity: DelawareEntity) -> int:
        """
        Score entity for likelihood of being VC-backed startup.

        Higher score = more likely to be interesting.
        """
        score = 0

        # Tech name pattern (+3)
        if entity.has_tech_name:
            score += 3

        # Startup-friendly agent (+5 - strong signal)
        if entity.has_startup_agent:
            score += 5

        # Recent formation (+2)
        if entity.formation_date:
            days_old = (date.today() - entity.formation_date).days
            if days_old <= 30:
                score += 2
            elif days_old <= 90:
                score += 1

        # Name patterns that suggest AI/ML startup (+2)
        ai_patterns = ['ai', 'intelligence', 'neural', 'deep', 'machine', 'ml', 'llm', 'gpt']
        if any(p in entity.entity_name.lower() for p in ai_patterns):
            score += 2

        return score

    async def scrape_all(
        self,
        days_back: int = 7,
        min_score: int = 3,
        max_results: int = 50,
    ) -> List[NormalizedArticle]:
        """
        Full scraping pipeline for Delaware incorporations.

        Args:
            days_back: How many days back to search (1=past day, 7=past week, 30+=past month)
            min_score: Minimum score to include (filters noise)
            max_results: Maximum number of results to return (applied AFTER scoring)

        Returns:
            List of NormalizedArticle for potential startup incorporations, sorted by score
        """
        all_entities = []

        # Search via Brave (uses days_back for freshness)
        brave_entities = await self.search_recent_incorporations(days_back=days_back)
        all_entities.extend(brave_entities)

        # Search aggregators
        agg_entities = await self.search_aggregators()
        all_entities.extend(agg_entities)

        # Deduplicate using helper method
        unique_entities = self._deduplicate_entities(all_entities)

        # Score and sort (highest first)
        scored = [(e, self.score_entity(e)) for e in unique_entities]
        scored.sort(key=lambda x: x[1], reverse=True)

        # Filter by min_score, limit to max_results, convert to articles
        articles = []
        for entity, score in scored:
            if score >= min_score:
                article = self.entity_to_article(entity)
                articles.append(article)
                if len(articles) >= max_results:
                    break

        logger.info(f"Delaware scraper: {len(articles)} articles (score>={min_score}) from {len(unique_entities)} entities")
        return articles


# Convenience function
async def run_delaware_corps_scraper(
    days_back: int = 7,
    min_score: int = 3,
    max_results: int = 50,
) -> List[NormalizedArticle]:
    """Run Delaware Corps scraper and return articles."""
    async with DelawareCorpsScraper() as scraper:
        return await scraper.scrape_all(
            days_back=days_back,
            min_score=min_score,
            max_results=max_results,
        )
