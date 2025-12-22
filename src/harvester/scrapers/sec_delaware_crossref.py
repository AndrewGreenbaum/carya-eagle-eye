"""
SEC Form D + Delaware Cross-Reference - High Priority Lead Detection.

Combines SEC Form D filings with Delaware registry data to identify
high-priority VC-backed startup leads using multi-signal scoring.

Signals used:
- SEC Form D: stateOfIncorporation, totalOfferingAmount, isFirstSale, industryGroup
- Delaware Registry: registered agent (Stripe Atlas, Clerky = high signal)
- Fund matching: relatedPersons against tracked VC fund patterns

Scoring:
- 10+: High Priority Lead (DE + $1M+ + first sale + tech + VC/agent signal)
- 7-9: Strong Signal (DE + $500K+ + first sale + tech)
- 3-6: Watch List (DE + $500K+ + tech)
"""

import asyncio
import logging
import re
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, Dict, Tuple

from .sec_edgar import (
    SECEdgarScraper,
    FormDFiling,
    VC_LIKELY_INDUSTRIES,
    MIN_VC_AMOUNT,
    SEC_REQUEST_DELAY,
)
from .delaware_corps import DelawareCorpsScraper, DelawareEntity

logger = logging.getLogger(__name__)


# Priority levels
PRIORITY_HIGH = "HIGH"  # Score 10+
PRIORITY_STRONG = "STRONG"  # Score 7-9
PRIORITY_WATCH = "WATCH"  # Score 3-6


@dataclass
class HighPriorityLead:
    """A high-priority lead from SEC + Delaware cross-reference."""
    company_name: str
    cik: str
    filing_date: date
    filing_url: str

    # SEC Form D data
    state_of_incorporation: Optional[str]
    amount_raised: Optional[str]
    total_offering: Optional[str]
    is_first_sale: bool
    industry: Optional[str]
    investors: List[str]

    # Delaware registry data (if found)
    delaware_entity: Optional[DelawareEntity]

    # Scoring
    score: int
    priority: str  # HIGH, STRONG, WATCH
    score_breakdown: Dict[str, int]

    # Signals detected
    is_delaware: bool
    is_tech_industry: bool
    has_vc_fund_match: bool
    has_startup_agent: bool
    matched_fund_slug: Optional[str]


def parse_amount(amount_str: Optional[str]) -> int:
    """Parse amount string like '$1,000,000' to integer.

    Handles various formats:
    - '$1,000,000' -> 1000000
    - '$5M' or '$5 million' -> 5000000
    - '$2-5M' (range) -> 2000000 (uses lower bound)
    - '$1.5M' -> 1500000
    """
    if not amount_str:
        return 0
    try:
        clean = amount_str.replace("$", "").replace(",", "").strip()

        # FIX: Detect multiplier suffix BEFORE handling ranges
        # (so "$2-5M" correctly applies M to lower bound)
        multiplier = 1
        clean_lower = clean.lower()
        if "m" in clean_lower or "million" in clean_lower:
            multiplier = 1_000_000
            clean = re.sub(r'[mM](?:illion)?', '', clean).strip()
        elif "b" in clean_lower or "billion" in clean_lower:
            multiplier = 1_000_000_000
            clean = re.sub(r'[bB](?:illion)?', '', clean).strip()
        elif "k" in clean_lower:
            multiplier = 1_000
            clean = re.sub(r'[kK]', '', clean).strip()

        # Handle ranges like "$2-5" (after suffix removed) - extract lower bound
        if "-" in clean:
            parts = clean.split("-")
            clean = parts[0].strip()  # Use lower bound

        # Parse as float then convert (handles "$1.5M")
        return int(float(clean) * multiplier)
    except (ValueError, TypeError):
        return 0


def calculate_score(
    filing: FormDFiling,
    entity: Optional[DelawareEntity],
    matched_fund_slug: Optional[str],
) -> Tuple[int, Dict[str, int]]:
    """
    Calculate multi-signal score for a filing.

    Returns (total_score, breakdown_dict)
    """
    breakdown = {}
    score = 0

    # Delaware incorporation (+2)
    if filing.state_of_incorporation == "DE":
        breakdown["delaware"] = 2
        score += 2

    # First sale - new funding round (+3)
    if filing.is_first_sale:
        breakdown["first_sale"] = 3
        score += 3

    # Tech industry (+2)
    if filing.industry:
        for vc_industry in VC_LIKELY_INDUSTRIES:
            if vc_industry.lower() in filing.industry.lower():
                breakdown["tech_industry"] = 2
                score += 2
                break

    # Amount tiers
    amount = parse_amount(filing.total_offering) or parse_amount(filing.amount_raised)
    if amount >= 5_000_000:
        breakdown["amount_5m_plus"] = 3
        score += 3
    elif amount >= 1_000_000:
        breakdown["amount_1m_plus"] = 2
        score += 2
    elif amount >= 500_000:
        breakdown["amount_500k_plus"] = 1
        score += 1

    # VC fund match in relatedPersons (+5)
    if matched_fund_slug:
        breakdown["vc_fund_match"] = 5
        score += 5

    # Delaware registry signals
    if entity:
        # Startup-friendly agent (Stripe Atlas, Clerky) (+5)
        if entity.has_startup_agent:
            breakdown["startup_agent"] = 5
            score += 5
        # Tech company name (+2)
        if entity.has_tech_name:
            breakdown["tech_name"] = 2
            score += 2

    return score, breakdown


async def get_high_priority_leads(
    days_back: int = 30,
    min_score: int = 3,
    delaware_only: bool = True,
    tech_only: bool = True,
    min_amount: int = 500_000,
) -> List[HighPriorityLead]:
    """
    Cross-reference SEC Form D filings with Delaware registry.

    Returns high-priority leads based on multi-signal scoring.

    Args:
        days_back: How many days to look back for Form D filings
        min_score: Minimum score to include in results
        delaware_only: If True, only include Delaware-incorporated companies
        tech_only: If True, only include tech industry filings
        min_amount: Minimum offering amount to consider

    Returns:
        List of HighPriorityLead sorted by score (highest first)
    """
    leads = []

    # FIX: Use AsyncExitStack to properly handle nested context managers
    # This prevents resource leaks if inner context manager fails
    async with AsyncExitStack() as stack:
        sec_scraper = await stack.enter_async_context(SECEdgarScraper())
        de_scraper = await stack.enter_async_context(DelawareCorpsScraper())

        # Fetch Form D filings
        filings = await sec_scraper.fetch_recent_filings(hours=days_back * 24)
        logger.info(f"SEC-Delaware crossref: Processing {len(filings)} filings")

        for filing in filings:
            # Fetch filing details (amount, industry, state, etc.)
            # Note: fetch_filing_details now includes SEC_REQUEST_DELAY internally
            result = await sec_scraper.fetch_filing_details(filing)
            if result is None:
                continue  # Skip filings with missing required data
            filing = result
            # FIX: Removed redundant 0.2s sleep - SEC rate limiting is now in fetch_filing_details

            # Apply filters
            if delaware_only and filing.state_of_incorporation != "DE":
                continue

            if tech_only:
                is_tech = False
                if filing.industry:
                    for ind in VC_LIKELY_INDUSTRIES:
                        if ind.lower() in filing.industry.lower():
                            is_tech = True
                            break
                if not is_tech:
                    continue

            amount = parse_amount(filing.total_offering) or parse_amount(filing.amount_raised)
            if amount < min_amount:
                continue

            # Check for tracked fund match
            matched_fund_slug = sec_scraper.match_tracked_fund(filing)

            # Cross-reference with Delaware registry
            entity = None
            if filing.state_of_incorporation == "DE":
                entity = await de_scraper.search_by_company_name(filing.company_name)
                await asyncio.sleep(0.3)  # Rate limit OpenCorporates

            # Calculate score
            score, breakdown = calculate_score(filing, entity, matched_fund_slug)

            if score < min_score:
                continue

            # Determine priority
            if score >= 10:
                priority = PRIORITY_HIGH
            elif score >= 7:
                priority = PRIORITY_STRONG
            else:
                priority = PRIORITY_WATCH

            # Build lead object
            lead = HighPriorityLead(
                company_name=filing.company_name,
                cik=filing.cik,
                filing_date=filing.filing_date,
                filing_url=filing.filing_url,
                state_of_incorporation=filing.state_of_incorporation,
                amount_raised=filing.amount_raised,
                total_offering=filing.total_offering,
                is_first_sale=filing.is_first_sale,
                industry=filing.industry,
                investors=filing.investors,
                delaware_entity=entity,
                score=score,
                priority=priority,
                score_breakdown=breakdown,
                is_delaware=filing.state_of_incorporation == "DE",
                is_tech_industry="tech_industry" in breakdown,
                has_vc_fund_match=matched_fund_slug is not None,
                has_startup_agent=entity.has_startup_agent if entity else False,
                matched_fund_slug=matched_fund_slug,
            )

            leads.append(lead)

    # Sort by score (highest first)
    leads.sort(key=lambda x: x.score, reverse=True)

    return leads


async def run_crossref_scan(
    days_back: int = 30,
    min_score: int = 3,
) -> List[HighPriorityLead]:
    """
    Convenience function to run cross-reference scan.

    Returns high-priority leads from SEC + Delaware cross-reference.
    """
    return await get_high_priority_leads(
        days_back=days_back,
        min_score=min_score,
        delaware_only=True,
        tech_only=True,
        min_amount=500_000,
    )
