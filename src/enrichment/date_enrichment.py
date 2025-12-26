"""
Date Enrichment via Brave Search.

Searches for funding announcement dates for deals missing announced_date.
Uses Brave Search to find news articles about the funding round and
extracts the announcement date from search result metadata.

OPTIMIZED: Uses shared BraveClient (no duplicate HTTP client or retry logic).
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, List, Dict, Tuple

from ..common.brave_client import get_brave_client
from ..config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class DateEnrichmentResult:
    """Result of date enrichment for a deal."""
    deal_id: int
    company_name: str
    round_type: str
    found_date: Optional[date] = None
    date_source: Optional[str] = None  # "page_age", "snippet", "title"
    search_url: Optional[str] = None  # URL where date was found
    confidence: str = "low"  # "high", "medium", "low"


# Relative date patterns
RELATIVE_DATE_PATTERNS = [
    # "2 days ago", "3 weeks ago", "1 month ago"
    (r"(\d+)\s*days?\s*ago", "days"),
    (r"(\d+)\s*weeks?\s*ago", "weeks"),
    (r"(\d+)\s*months?\s*ago", "months"),
    (r"(\d+)\s*years?\s*ago", "years"),
    # "yesterday", "today"
    (r"\byesterday\b", "yesterday"),
    (r"\btoday\b", "today"),
]

# Absolute date patterns (for snippet extraction)
ABSOLUTE_DATE_PATTERNS = [
    # "Dec 15, 2024", "December 15, 2024"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}",
    # "15 Dec 2024", "15 December 2024"
    r"\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{4}",
    # "2024-12-15" (ISO format)
    r"\d{4}-\d{2}-\d{2}",
    # "12/15/2024", "12-15-2024"
    r"\d{1,2}[/-]\d{1,2}[/-]\d{4}",
]

# Month name to number mapping
MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_relative_date(text: str) -> Optional[date]:
    """
    Parse relative date strings like "2 days ago", "3 weeks ago".

    Brave Search returns page_age in formats like:
    - "2 days ago"
    - "3 weeks ago"
    - "1 month ago"
    - "Dec 15, 2024"
    """
    if not text:
        return None

    text_lower = text.lower().strip()
    today = date.today()

    # Try relative patterns first
    for pattern, unit in RELATIVE_DATE_PATTERNS:
        match = re.search(pattern, text_lower)
        if match:
            if unit == "yesterday":
                return today - timedelta(days=1)
            elif unit == "today":
                return today
            else:
                try:
                    amount = int(match.group(1))
                    if unit == "days":
                        return today - timedelta(days=amount)
                    elif unit == "weeks":
                        return today - timedelta(weeks=amount)
                    elif unit == "months":
                        return today - timedelta(days=amount * 30)
                    elif unit == "years":
                        return today - timedelta(days=amount * 365)
                except (ValueError, IndexError):
                    pass

    # Try absolute date patterns
    return parse_absolute_date(text)


def parse_absolute_date(text: str) -> Optional[date]:
    """Parse absolute date strings like "Dec 15, 2024"."""
    if not text:
        return None

    text_lower = text.lower().strip()

    # Try ISO format first: 2024-12-15
    iso_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if iso_match:
        try:
            return date(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))
        except ValueError:
            pass

    # Try "Dec 15, 2024" format
    for month_abbr, month_num in MONTH_MAP.items():
        # Pattern: "Dec 15, 2024" or "December 15 2024"
        pattern = rf"\b{month_abbr}[a-z]*\.?\s*(\d{{1,2}}),?\s*(\d{{4}})"
        match = re.search(pattern, text_lower)
        if match:
            try:
                day = int(match.group(1))
                year = int(match.group(2))
                return date(year, month_num, day)
            except ValueError:
                continue

        # Pattern: "15 Dec 2024" or "15 December 2024"
        pattern = rf"(\d{{1,2}})\s*{month_abbr}[a-z]*\.?\s*(\d{{4}})"
        match = re.search(pattern, text_lower)
        if match:
            try:
                day = int(match.group(1))
                year = int(match.group(2))
                return date(year, month_num, day)
            except ValueError:
                continue

    # Try MM/DD/YYYY or MM-DD-YYYY
    us_match = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", text)
    if us_match:
        try:
            month = int(us_match.group(1))
            day = int(us_match.group(2))
            year = int(us_match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return date(year, month, day)
        except ValueError:
            pass

    return None


def extract_date_from_snippet(snippet: str) -> Optional[date]:
    """Extract a date from a search result snippet."""
    if not snippet:
        return None

    # Look for date patterns in the snippet
    for pattern in ABSOLUTE_DATE_PATTERNS:
        match = re.search(pattern, snippet, re.IGNORECASE)
        if match:
            extracted = parse_absolute_date(match.group(0))
            if extracted:
                # Validate: reject future dates and very old dates
                today = date.today()
                if extracted <= today and extracted >= today - timedelta(days=365):
                    return extracted

    return None


class DateEnrichmentClient:
    """
    Client for enriching deal dates via Brave Search.

    OPTIMIZED: Uses shared BraveClient instead of creating its own HTTP client.
    """

    def __init__(self):
        self.rate_limit_delay = settings.brave_search_rate_limit_delay

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass  # Client is shared, don't close it

    async def _search(self, query: str, count: int = 5) -> List[Dict]:
        """Execute Brave Search query using shared client."""
        client = get_brave_client()
        if not client.validate_api_key():
            logger.warning("BRAVE_SEARCH_KEY not configured for date enrichment")
            return []

        try:
            data = await asyncio.wait_for(
                client.search_web(query, count),
                timeout=settings.enrichment_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Brave search timeout for date query: {query[:50]}...")
            return []

        if data is None:
            return []

        return data.get("web", {}).get("results", [])

    async def find_announcement_date(
        self,
        company_name: str,
        round_type: str,
        lead_investor: Optional[str] = None,
    ) -> Tuple[Optional[date], Optional[str], Optional[str]]:
        """
        Search for funding announcement date.

        Returns:
            Tuple of (found_date, date_source, search_url)
        """
        if not company_name or company_name.lower() in ("<unknown>", "unknown", "n/a"):
            return (None, None, None)

        # Normalize round type for query
        round_query = round_type.replace("_", " ").title() if round_type else "funding"

        # Try multiple query strategies
        queries = [
            # Strategy 1: Company + round type + "raises"
            f'"{company_name}" {round_query} raises funding',
            # Strategy 2: Company + round + "announced"
            f'"{company_name}" {round_query} funding announced',
        ]

        # Add lead investor to first query if available
        if lead_investor:
            investor_short = lead_investor.split()[0]
            queries.insert(0, f'"{company_name}" {round_query} "{investor_short}" funding')

        for query in queries:
            results = await self._search(query, count=5)

            for result in results:
                url = result.get("url", "")
                title = result.get("title", "")
                description = result.get("description", "")
                page_age = result.get("page_age", "")

                # Skip if company name not in result
                combined_text = f"{title} {description}".lower()
                company_lower = company_name.lower()
                if company_lower not in combined_text and company_lower.split()[0] not in combined_text:
                    continue

                # Strategy 1: Try page_age first (most reliable from Brave)
                if page_age:
                    found_date = parse_relative_date(page_age)
                    if found_date:
                        return (found_date, "page_age", url)

                # Strategy 2: Extract date from snippet
                found_date = extract_date_from_snippet(description)
                if found_date:
                    return (found_date, "snippet", url)

                # Strategy 3: Extract date from title
                found_date = extract_date_from_snippet(title)
                if found_date:
                    return (found_date, "title", url)

            # Rate limit between queries
            await asyncio.sleep(self.rate_limit_delay)

        return (None, None, None)

    async def enrich_deal_date(
        self,
        deal_id: int,
        company_name: str,
        round_type: str,
        lead_investor: Optional[str] = None,
    ) -> DateEnrichmentResult:
        """
        Enrich a single deal with its announcement date.
        """
        result = DateEnrichmentResult(
            deal_id=deal_id,
            company_name=company_name,
            round_type=round_type,
        )

        found_date, date_source, search_url = await self.find_announcement_date(
            company_name, round_type, lead_investor
        )

        if found_date:
            result.found_date = found_date
            result.date_source = date_source
            result.search_url = search_url

            # Set confidence based on source
            if date_source == "page_age":
                result.confidence = "high"
            elif date_source == "snippet":
                result.confidence = "medium"
            else:
                result.confidence = "low"

            logger.info(
                f"Found date for {company_name}: {found_date} "
                f"(source: {date_source}, confidence: {result.confidence})"
            )

        return result


async def enrich_deal_date(
    deal_id: int,
    company_name: str,
    round_type: str,
    lead_investor: Optional[str] = None,
) -> DateEnrichmentResult:
    """Enrich a single deal with its announcement date."""
    async with DateEnrichmentClient() as client:
        return await client.enrich_deal_date(
            deal_id, company_name, round_type, lead_investor
        )


async def persist_deal_date(deal_id: int, announced_date: date) -> bool:
    """
    Persist the enriched date to the database.

    Args:
        deal_id: ID of the deal to update
        announced_date: The date to set

    Returns:
        True if update succeeded, False otherwise
    """
    from ..archivist.database import get_session
    from ..archivist.models import Deal

    try:
        async with get_session() as session:
            deal = await session.get(Deal, deal_id)
            if deal:
                deal.announced_date = announced_date
                await session.commit()
                logger.info(f"Updated date for deal {deal_id}: {announced_date}")
                return True
            return False
    except Exception as e:
        logger.error(f"Error persisting date for deal {deal_id}: {e}")
        return False


async def enrich_deals_dates_batch(
    deals: List[Dict],
    persist: bool = True,
    delay_seconds: float = 0.5,
    max_concurrent: int = 3,
) -> List[DateEnrichmentResult]:
    """
    Enrich multiple deals with dates.

    Args:
        deals: List of dicts with keys: deal_id, company_name, round_type, lead_investor
        persist: If True, save results to database
        delay_seconds: Delay between API calls
        max_concurrent: Max concurrent enrichments

    Returns:
        List of DateEnrichmentResult objects
    """
    results: List[DateEnrichmentResult] = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async def enrich_with_limit(deal: Dict) -> DateEnrichmentResult:
        async with semaphore:
            try:
                async with DateEnrichmentClient() as client:
                    result = await client.enrich_deal_date(
                        deal_id=deal["deal_id"],
                        company_name=deal["company_name"],
                        round_type=deal.get("round_type", "funding"),
                        lead_investor=deal.get("lead_investor"),
                    )

                    # Persist if found and requested
                    if persist and result.found_date:
                        await persist_deal_date(result.deal_id, result.found_date)

                    return result
            except Exception as e:
                logger.error(f"Error enriching date for deal {deal.get('deal_id')}: {e}")
                return DateEnrichmentResult(
                    deal_id=deal["deal_id"],
                    company_name=deal["company_name"],
                    round_type=deal.get("round_type", "funding"),
                )
            finally:
                # Rate limit between requests (inside semaphore to ensure spacing)
                await asyncio.sleep(delay_seconds)

    # Run enrichments with limited concurrency
    tasks = [enrich_with_limit(deal) for deal in deals]
    completed = await asyncio.gather(*tasks, return_exceptions=True)

    for item in completed:
        if isinstance(item, Exception):
            logger.error(f"Batch date enrichment error: {item}")
            continue
        results.append(item)

    return results
