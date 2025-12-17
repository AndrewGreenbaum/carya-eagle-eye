"""
Storage pipeline for persisting extracted deals to the database.
"""

import hashlib
import json
import re
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Tuple
from sqlalchemy import select, nullslast
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# CONSOLIDATED (2026-01): Single source of truth for company name suffixes
# Previously duplicated in _normalize_company_name_for_dedup(), normalize_company_name(),
# company_names_match(), and extractor.py's _validate_company_in_text()
# Different suffix lists could cause dedup misses
#
# FIX (2026-01): Added missing suffixes from all three locations:
# - storage.py's known_company_suffixes (x, go, ly, fy, ify)
# - extractor.py's suffixes_to_strip (software, systems, healthcare, bio, therapeutics,
#   sciences, data, analytics, intelligence, platform)
COMPANY_NAME_SUFFIXES = [
    # Legal entity types (longer first to avoid partial matches)
    ", incorporated", " incorporated",
    ", technologies", " technologies",
    ", corporation", " corporation",
    ", limited", " limited",
    ", company", " company",
    ", inc.", " inc.",
    ", inc", " inc",
    ", llc", " llc",
    ", ltd.", " ltd.",
    ", ltd", " ltd",
    ", corp.", " corp.",
    ", corp", " corp",
    ", co.", " co.",
    ", co", " co",
    # Common tech company suffixes
    " labs", " lab",
    " tech",
    " ai",
    # Additional suffixes for dedup (from CLAUDE.md)
    " health", " cloud", " ml", " ops", " dev", " hq", " app", " io",
    # FIX (2026-01): Added from extractor.py's suffixes_to_strip
    " software", " systems", " healthcare", " bio", " therapeutics",
    " sciences", " data", " analytics", " intelligence", " platform",
    # FIX (2026-01): Added startup naming patterns from known_company_suffixes
    " x", " go", " ly", " fy", " ify",
]

# Derived set for word-only matching (no leading space/comma)
# Used by extractor.py's _validate_company_in_text() for company validation
COMPANY_NAME_SUFFIX_WORDS = frozenset(
    s.strip().lstrip(",").strip().lower() for s in COMPANY_NAME_SUFFIXES
)

# More restrictive set for company_names_match() to avoid false positives
# Does NOT include generic words like "data", "software", "systems" that could
# cause false matches (e.g., "Meta" vs "Metadata" should NOT match)
# Only includes suffixes that are clearly startup naming patterns
COMPANY_NAME_MATCH_SUFFIXES = frozenset({
    'ai', 'hq', 'app', 'io', 'labs', 'lab', 'tech',
    'health', 'cloud', 'ml', 'ops', 'dev',
    'x', 'go', 'ly', 'fy', 'ify'  # Common startup suffixes
})


def _normalize_company_name_for_dedup(name: str) -> str:
    """
    Normalize company name for dedup key generation.

    Uses shared COMPANY_NAME_SUFFIXES constant (consolidated to prevent discrepancies).

    FIX (2026-01): Now strips ALL matching suffixes, not just one.
    Previous bug: "Acme Labs AI" → "Acme Labs" (only stripped " ai")
    Fixed: "Acme Labs AI" → "Acme" (strips " ai" then " labs")
    """
    name = name.lower().strip()
    if name.startswith("the "):
        name = name[4:]

    # FIX: Strip ALL matching suffixes (not just one)
    # Loop until no more suffixes match
    changed = True
    while changed:
        changed = False
        for suffix in COMPANY_NAME_SUFFIXES:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                changed = True
                break  # Restart loop to check for more suffixes

    return re.sub(r'[^a-z0-9]', '', name)


def make_dedup_key(company_name: str, round_type: str, announced_date: Optional[date]) -> str:
    """
    Generate a deduplication key for a deal.

    This key is used for database-level uniqueness constraint to prevent
    race condition duplicates when processing deals in parallel.

    The key is based on:
    - Normalized company name (lowercase, no suffixes)
    - Round type
    - Date bucket (rounded to 3-day windows to catch near-duplicates)

    Returns:
        A 32-character hex string (MD5 hash)
    """
    # normalize_company_name is defined later in this file
    normalized_name = _normalize_company_name_for_dedup(company_name)

    # Round date to 3-day buckets to catch near-duplicates
    # e.g., Jan 1-3 all map to bucket 0, Jan 4-6 to bucket 1, etc.
    if announced_date:
        # Use days since epoch, rounded to 3-day buckets
        days_since_epoch = (announced_date - date(1970, 1, 1)).days
        date_bucket = days_since_epoch // 3
        date_str = str(date_bucket)
    else:
        # For deals without dates, use "nodate" + current week bucket
        # This catches race condition duplicates created within the same week
        today = date.today()
        week_bucket = (today - date(1970, 1, 1)).days // 7
        date_str = f"nodate_{week_bucket}"

    key_data = f"{normalized_name}|{round_type}|{date_str}"
    return hashlib.md5(key_data.encode()).hexdigest()


def make_amount_dedup_key(company_name: str, amount_usd: Optional[int], announced_date: Optional[date]) -> Optional[str]:
    """
    Generate a secondary deduplication key based on amount instead of round_type.

    This catches race condition duplicates where LLM classifies the SAME deal
    with DIFFERENT round types (e.g., "growth" vs "series_d" for Parloa $350M).

    The key is based on:
    - Normalized company name (lowercase, no suffixes)
    - Amount bucket (rounded to capture similar amounts)
    - Date bucket (rounded to 3-day windows)

    IMPROVED (2026-01): Lowered threshold from $1M to $250K to support early-stage deals.
    Many legitimate pre-seed rounds are in the $500K-$1M range.

    Amount buckets (logarithmic to handle wide range):
    - <$250K: no key (too small, too many potential false positives)
    - $250K-$1M: bucket by $250K increments (for pre-seed/angel rounds)
    - $1M-$10M: bucket by $2M increments
    - $10M-$100M: bucket by $20M increments
    - $100M-$1B: bucket by $100M increments
    - >$1B: bucket by $500M increments

    Returns:
        A 32-character hex string (MD5 hash) or None if amount is unknown/too small
    """
    # Only generate if we have a meaningful amount
    # IMPROVED (2026-01): Lowered threshold from $1M to $250K for early-stage dedup
    if not amount_usd or amount_usd < 250_000:
        return None

    normalized_name = _normalize_company_name_for_dedup(company_name)

    # Calculate amount bucket (logarithmic buckets)
    if amount_usd < 1_000_000:  # $250K-$1M (early-stage)
        amount_bucket = amount_usd // 250_000  # $250K increments
    elif amount_usd < 10_000_000:  # $1M-$10M
        amount_bucket = 4 + (amount_usd // 2_000_000)  # $2M increments, offset by 4
    elif amount_usd < 100_000_000:  # $10M-$100M
        amount_bucket = 9 + (amount_usd // 20_000_000)  # $20M increments, offset by 9
    elif amount_usd < 1_000_000_000:  # $100M-$1B
        amount_bucket = 14 + (amount_usd // 100_000_000)  # $100M increments, offset by 14
    else:  # >$1B
        amount_bucket = 24 + (amount_usd // 500_000_000)  # $500M increments, offset by 24

    # Date bucket (same as dedup_key)
    if announced_date:
        days_since_epoch = (announced_date - date(1970, 1, 1)).days
        date_bucket = days_since_epoch // 3
        date_str = str(date_bucket)
    else:
        today = date.today()
        week_bucket = (today - date(1970, 1, 1)).days // 7
        date_str = f"nodate_{week_bucket}"

    key_data = f"{normalized_name}|amt{amount_bucket}|{date_str}"
    return hashlib.md5(key_data.encode()).hexdigest()


# ----- URL Validation (FIX #1: Use shared module) -----
from ..common.url_utils import is_valid_url as _is_valid_url, sanitize_url as _sanitize_url


def _is_valid_amount(amount: Optional[str]) -> bool:
    """
    Check if amount string is a valid funding amount (not a placeholder).

    Returns False for:
    - None, empty string
    - "<UNKNOWN>", "None" (as string), "unknown", "undisclosed"
    - Any placeholder/unknown indicator

    Returns True for real amounts like "$50M", "100 million", etc.
    """
    if not amount:
        return False

    amount_lower = amount.lower().strip()

    # List of placeholder values that indicate no real amount
    placeholders = {
        "<unknown>",
        "unknown",
        "none",
        "null",
        "undisclosed",
        "not disclosed",
        "n/a",
        "na",
        "tbd",
        "confidential",
        "",
    }

    if amount_lower in placeholders:
        return False

    # Check for partial matches (e.g., "amount undisclosed")
    if any(p in amount_lower for p in ["unknown", "undisclosed", "not disclosed"]):
        return False

    # Valid amount should have a number
    if not re.search(r'\d', amount):
        return False

    return True


def _is_likely_placeholder_date(d: date) -> bool:
    """
    Check if a date looks like a placeholder or extraction error.

    FIX (2026-01): Added to handle the Emergent bug where Jan 1 was extracted
    as a placeholder date when the actual announcement was Jan 20.

    Returns True for dates that are likely placeholders:
    - Jan 1 of any year (very common default/placeholder)
    - First day of any month (sometimes used as a default when day is unknown)

    Returns False for dates that look like real announcements.
    """
    # Jan 1 is almost always a placeholder - real announcements rarely happen on Jan 1
    # (most companies are closed for New Year's)
    if d.month == 1 and d.day == 1:
        return True

    # First day of month is sometimes a default when the exact day is unknown
    # But this is less certain, so only flag it if it's also a quarter start
    # (Jan 1, Apr 1, Jul 1, Oct 1 are common fiscal quarter defaults)
    if d.day == 1 and d.month in (1, 4, 7, 10):
        return True

    return False


# FIX #20: Alert info returned from save_deal for post-commit sending
@dataclass
class LeadDealAlertInfo:
    """Info needed to send a lead deal alert after transaction commits."""
    company_name: str
    amount: Optional[str]
    round_type: str
    lead_investor: str
    enterprise_category: Optional[str]
    verification_snippet: Optional[str]

from .models import (
    Fund,
    PortfolioCompany,
    Deal,
    DealInvestor,
    Article,
    StealthDetection,
    CompanyAlias,
    DateSource,
)
from .database import get_session
from ..analyst.schemas import DealExtraction, LeadStatus, EnterpriseCategory
from ..config.funds import FUND_REGISTRY
# NOTE: fund_matcher import moved inside save_deal() to avoid circular import


def normalize_amount(amount_str: Optional[str]) -> Optional[int]:
    """
    Normalize amount string to integer (in USD equivalent).

    Examples:
        "$30M" → 30000000
        "$30 million" → 30000000
        "$30mn" → 30000000
        "$2.5B" → 2500000000
        "$500K" → 500000
        "$25-30 million" → 25000000 (takes first number)
        "€100 million" → 100000000 (treats as equivalent USD)
        "₹97 crore" → 11640000 (converts to USD equivalent)
        "INR 100 Cr" → 12000000 (converts to USD equivalent)
        None → None
    """
    if not amount_str:
        return None

    # Remove currency symbols and whitespace, lowercase
    clean = amount_str.replace("$", "").replace("€", "").replace("£", "").replace("₹", "")
    clean = clean.replace(",", "").strip().lower()

    # Remove currency prefixes (including "US $" which becomes "us " after $ removal)
    # Note: longer matches must come first (usd before us) due to regex alternation
    clean = re.sub(r'^(usd|us|eur|gbp|inr)\s*', '', clean)

    # FIX: Remove approximate/range prefixes that LLM might include
    clean = re.sub(r'^(approximately|approx\.?|around|about|up\s+to|nearly|over|more\s+than|less\s+than|~)\s*', '', clean)

    # Handle "undisclosed" or similar
    if "undisclosed" in clean or "unknown" in clean or "<unknown>" in clean:
        return None

    # Handle ranges like "25-30 million" - take the first number
    # Also handles "25 to 30 million"
    clean = re.sub(r'(\d+)\s*[-–—to]+\s*\d+', r'\1', clean)

    # Extract number and multiplier
    # Added "mn" as alternative for million (common in some sources)
    # Added "cr" and "crore" for Indian currency (1 crore ≈ $120K USD)
    match = re.match(r'([\d.]+)\s*(million|mn|mm|m|billion|bn|b|thousand|k|crore|cr|lakh|lac)?', clean)
    if not match:
        return None

    try:
        num = float(match.group(1))
    except ValueError:
        return None

    multiplier = match.group(2) or ""

    if multiplier in ("million", "m", "mn", "mm"):
        return int(num * 1_000_000)
    elif multiplier in ("billion", "b", "bn"):
        return int(num * 1_000_000_000)
    elif multiplier in ("thousand", "k"):
        return int(num * 1_000)
    elif multiplier in ("crore", "cr"):
        # 1 crore = 10 million INR
        # Conversion: 10,000,000 INR / 83 INR/USD = ~$120,482 USD
        # Using 83 INR/USD (Dec 2024 rate) - may drift ±10% over time
        # FIX: Updated to be more precise at current rates
        return int(num * 120_500)
    elif multiplier in ("lakh", "lac"):
        # 1 lakh = 100,000 INR
        # Conversion: 100,000 INR / 83 INR/USD = ~$1,205 USD
        return int(num * 1_205)
    else:
        # No multiplier detected - need to infer based on context
        # FIX (2026-01): Improved heuristics for common funding amount patterns
        # In funding context, numbers without multipliers follow patterns:
        #   - < 1000: almost always means millions (e.g., "raised 30" = $30M)
        #   - 1000-9999: likely thousands in early-stage (e.g., $5000 = $5K angel check)
        #   - 10000-999999: ambiguous - could be dollars or malformed millions
        #   - >= 1,000,000: already in dollars (e.g., "raised 5000000" for $5M)
        if num < 1000:
            # Small number without multiplier in funding context = millions
            # e.g., "raised 30" = $30M, "$50" = $50M
            logger.debug(f"Assuming {num} is in millions (no multiplier, funding context)")
            return int(num * 1_000_000)
        elif num >= 1_000_000:
            # Already looks like a dollar amount (e.g., 5000000 for $5M)
            return int(num)
        elif num < 10000:
            # 1000-9999: Likely early-stage in thousands (e.g., $5000 angel check)
            # Return as-is since it's a valid small dollar amount
            logger.debug(f"Amount {num} in 1K-10K range - treating as dollars")
            return int(num)
        else:
            # 10000-999999: Ambiguous range - could be dollars or malformed millions
            # IMPROVED: Check if it looks like a round number that should be millions
            # e.g., 50000 might be $50M, but 12345 is likely dollars
            if num % 10000 == 0:  # Round multiples of 10K (50000, 100000, etc.)
                # Likely intended as millions (e.g., 50000 = $50M typo)
                logger.warning(
                    f"Ambiguous round amount {num} without multiplier - "
                    f"might be ${num // 1000}K or ${num // 1000000}M"
                )
            return int(num)


def format_sec_amount(sec_amount_usd: int) -> str:
    """
    Format SEC Form D amount as human-readable string.

    SEC amounts are exact legal figures that need clean formatting.
    Strips trailing zeros from decimal portion before adding suffix.

    Examples:
        47500000 → "$47.5M"
        50000000 → "$50M"
        1000000000 → "$1B"
        1500000000 → "$1.5B"
        500000 → "$500,000"
    """
    if sec_amount_usd >= 1_000_000_000:
        num_str = f"{sec_amount_usd / 1_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"${num_str}B"
    elif sec_amount_usd >= 1_000_000:
        num_str = f"{sec_amount_usd / 1_000_000:.2f}".rstrip('0').rstrip('.')
        return f"${num_str}M"
    else:
        return f"${sec_amount_usd:,}"


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for fuzzy matching.

    Uses shared COMPANY_NAME_SUFFIXES constant (consolidated to prevent discrepancies
    with _normalize_company_name_for_dedup).

    FIX (2026-01): Now strips ALL matching suffixes, not just one.
    Previous bug: "Acme Labs AI" → "acmelabs" (only stripped " ai")
    Fixed: "Acme Labs AI" → "acme" (strips " ai" then " labs")

    Examples:
        "Valerie Health" → "valerie"
        "Valerie" → "valerie"
        "The AI Company, Inc." → "company"  # Note: "company" is in suffix list
        "OpenAI Technologies" → "open"  # Note: "ai" and "technologies" are suffixes
        "Anthropic, Inc." → "anthropic"
        "Acme Labs AI" → "acme"  # FIX: Now strips both suffixes
    """
    # Lowercase first
    name = name.lower().strip()

    # Remove "the" prefix (common in company names)
    if name.startswith("the "):
        name = name[4:]

    # FIX: Remove ALL matching suffixes (not just one)
    # Loop until no more suffixes match
    changed = True
    while changed:
        changed = False
        for suffix in COMPANY_NAME_SUFFIXES:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                changed = True
                break  # Restart loop to check for more suffixes

    # Remove non-alphanumeric characters
    name = re.sub(r'[^a-z0-9]', '', name)

    return name


def company_names_match(name1: str, name2: str) -> bool:
    """
    Check if two company names likely refer to the same company.

    Uses normalized comparison with strict suffix matching.
    Errs on the side of NOT matching (false negatives are better than false positives).

    Examples:
        ("Valerie Health", "Valerie") → True  (health is a known suffix)
        ("Ramp", "Ramp AI") → True  (ai is a known suffix)
        ("OpenAI", "OpenAPI") → False  (different names)
        ("Glean", "Glean AI") → True  (ai is a known suffix)
        ("Amazon", "Amazonia") → False  (ia is not a known suffix)
    """
    norm1 = normalize_company_name(name1)
    norm2 = normalize_company_name(name2)

    # Exact match after normalization
    if norm1 == norm2:
        return True

    # Empty check
    if not norm1 or not norm2:
        return False

    # One must be a prefix of the other (for short names)
    # This handles "Ramp" vs "Ramp AI" → "ramp" vs "rampai"
    shorter, longer = (norm1, norm2) if len(norm1) <= len(norm2) else (norm2, norm1)

    # Shorter name must be at least 3 chars to be a valid prefix
    if len(shorter) >= 3 and longer.startswith(shorter):
        # The suffix MUST be in our explicit list of known company suffixes
        # This is strict to avoid false positives like Amazon/Amazonia
        suffix = longer[len(shorter):]
        # FIX (2026-01): Use COMPANY_NAME_MATCH_SUFFIXES (more restrictive) instead of
        # COMPANY_NAME_SUFFIX_WORDS to avoid false positives like Meta/Metadata
        if suffix in COMPANY_NAME_MATCH_SUFFIXES:
            return True

    # NO fuzzy matching - too many false positives
    # If names don't match exactly or with known suffix, they're different companies
    return False


async def find_company_by_alias(
    session: AsyncSession,
    name: str,
) -> Optional[PortfolioCompany]:
    """
    Check if a company name is an alias for an existing company.

    This handles rebrands like "Bedrock Security" → "Bedrock Data".

    Returns:
        The PortfolioCompany if name is a known alias, None otherwise.
    """
    # Query aliases table with JOIN to get company in single query (avoids N+1)
    stmt = (
        select(PortfolioCompany)
        .join(CompanyAlias, CompanyAlias.company_id == PortfolioCompany.id)
        .where(CompanyAlias.alias_name.ilike(name))
    )
    result = await session.execute(stmt)
    company = result.scalar_one_or_none()

    if company:
        logger.info(f"Found company via alias: '{name}' → '{company.name}'")
        return company

    return None


async def company_names_match_with_aliases(
    session: AsyncSession,
    name1: str,
    name2: str,
) -> bool:
    """
    Check if two company names match, including via aliases.

    This extends company_names_match() with alias lookup for rebrands.

    Examples:
        ("Bedrock Security", "Bedrock Data") → True (if alias exists)
        ("Ramp", "Ramp AI") → True (known suffix)
        ("OpenAI", "OpenAPI") → False (different names)
    """
    # First try direct match
    if company_names_match(name1, name2):
        return True

    # Check if name1 is an alias for the company with name2
    alias_company = await find_company_by_alias(session, name1)
    if alias_company:
        if company_names_match(alias_company.name, name2):
            return True

    # Check if name2 is an alias for the company with name1
    alias_company = await find_company_by_alias(session, name2)
    if alias_company:
        if company_names_match(alias_company.name, name1):
            return True

    return False


async def create_company_alias(
    session: AsyncSession,
    company_id: int,
    alias_name: str,
    alias_type: str = "rebrand",
    effective_date: Optional[date] = None,
) -> Optional[CompanyAlias]:
    """
    Create a new company alias.

    Args:
        company_id: ID of the company this alias refers to
        alias_name: The alternative name (e.g., "Bedrock Security")
        alias_type: Type of alias (rebrand, dba, acquired_name, typo)
        effective_date: When the change happened (for rebrands)

    Returns:
        CompanyAlias if created, None if already exists
    """
    # Use ON CONFLICT to prevent race conditions between check and insert
    stmt = pg_insert(CompanyAlias).values(
        company_id=company_id,
        alias_name=alias_name,
        alias_type=alias_type,
        effective_date=effective_date,
    ).on_conflict_do_nothing(
        index_elements=['company_id', 'alias_name']
    ).returning(CompanyAlias)

    result = await session.execute(stmt)
    alias = result.scalar_one_or_none()

    if alias:
        logger.info(f"Created alias: {alias_name} → company #{company_id} (type={alias_type})")
        return alias
    else:
        logger.debug(f"Alias already exists: {alias_name} → company #{company_id}")
        return None


async def get_company_aliases(
    session: AsyncSession,
    company_id: int,
) -> list[CompanyAlias]:
    """Get all aliases for a company."""
    stmt = (
        select(CompanyAlias)
        .where(CompanyAlias.company_id == company_id)
        .order_by(CompanyAlias.created_at.desc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_or_create_company(
    session: AsyncSession,
    name: str,
    description: Optional[str] = None,
    sector: Optional[str] = None,
    website: Optional[str] = None,
    linkedin_url: Optional[str] = None,
) -> PortfolioCompany:
    """
    Get existing company or create new one.

    Uses INSERT ... ON CONFLICT to prevent race conditions where concurrent
    requests could create duplicate companies (e.g., 4 articles about
    Knight FinTech creating 4 separate company records).

    The unique constraint is on LOWER(name), so this handles case-insensitive
    matching (e.g., "Knight FinTech" and "knight fintech" are the same).
    """
    from sqlalchemy import func

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Sanitize URLs to prevent storing placeholders like "Not mentioned"
    website = _sanitize_url(website)
    linkedin_url = _sanitize_url(linkedin_url)

    # Use INSERT ... ON CONFLICT for atomic upsert
    # The unique index is on LOWER(name), so we use that for conflict detection
    stmt = pg_insert(PortfolioCompany).values(
        name=name,
        description=description,
        sector=sector,
        website=website,
        linkedin_url=linkedin_url,
        created_at=now,
        updated_at=now,
    ).on_conflict_do_update(
        index_elements=[func.lower(PortfolioCompany.name)],
        set_={
            # Only update fields if new value is provided AND existing is null
            'website': func.coalesce(PortfolioCompany.website, website),
            'linkedin_url': func.coalesce(PortfolioCompany.linkedin_url, linkedin_url),
            'description': func.coalesce(PortfolioCompany.description, description),
            'updated_at': now,
        }
    ).returning(PortfolioCompany)

    result = await session.execute(stmt)
    company = result.scalar_one()
    return company


async def get_or_create_fund(session: AsyncSession, slug: str) -> Optional[Fund]:
    """Get fund from database or create from registry."""
    if slug not in FUND_REGISTRY:
        return None

    stmt = select(Fund).where(Fund.slug == slug)
    result = await session.execute(stmt)
    fund = result.scalar_one_or_none()

    if fund:
        return fund

    fund_config = FUND_REGISTRY[slug]
    fund = Fund(
        slug=slug,
        name=fund_config.name,
        website=fund_config.ingestion_url,
    )
    session.add(fund)
    await session.flush()
    return fund


async def find_duplicate_deal(
    session: AsyncSession,
    company_name: str,
    round_type: str,
    amount: Optional[str],
    announced_date: Optional[date],
) -> Optional[Deal]:
    """
    Check if a similar deal already exists.

    Match criteria (SIX-TIER approach, executed in this order):

    TIER 0 - Exact Company Match (FIRST - catches race condition duplicates):
    - Company names match EXACTLY (case-insensitive)
    - Same round type
    - Date within ±3 days (or both null)
    - Most aggressive tier to catch parallel processing race conditions
    - Example: Torq Series D on Jan 11 vs Jan 12 from different sources

    TIER 4 - Prefix Match (catches name variations like MEQ Probe/MEQ Solutions):
    - Company names share same first 3 characters after normalization
    - EXACT amount match (within 5%)
    - EXACT date match
    - SAME round type
    - High confidence: all facts match except name suffix

    TIER 3 - Amount Match (catches same deal with different round type):
    - Company names match (fuzzy)
    - Same normalized amount (within 10% - tighter for cross-round matching)
    - Date within 30 days (narrower window for cross-round)
    - ANY round type (LLM often extracts "series_a" vs "growth" for same deal)

    TIER 2 - Same-Day Match (catches same-day duplicates from multiple sources):
    - Uses TARGETED SQL query (no LIMIT) to prevent missing duplicates
    - Company names match exactly (case-insensitive) OR normalized fuzzy match
    - Same announced date (exact match)
    - Any round type, any amount (news sources often report different details)

    TIER 2.5 - Round+Date Match (catches valuation vs funding confusion):
    - Company names match (fuzzy)
    - Same round type
    - Date within 30 days (extended from 7 days in 2026-01 to catch Emergent-style dupes)
    - Amount NOT required to match (catches $6.6B valuation vs $330M funding, or NULL amounts)

    TIER 1 - Strict Match (LAST - same company + same round + similar amount):
    - Company names match (using smart fuzzy matching)
    - Same round type
    - Same normalized amount (within 15% tolerance) OR one/both amounts missing
    - Date within 365 days (1 year) OR deals from last 365 days if no date

    Returns existing Deal if found, None otherwise.

    OPTIMIZED:
    - TIER 0 uses exact company name + round + date window (most aggressive)
    - TIER 4 uses exact amount+date+round for prefix name matching
    - TIER 3 uses targeted amount_usd filter for efficient matching
    - TIER 2 uses targeted date+company query (FIX: no LIMIT, prevents missed dupes)
    - TIER 2.5 uses round_type + date window for amount-agnostic matching
    - TIER 1 uses general query with LIMIT 200
    - Date fallback for deals without announced_date
    """
    from sqlalchemy import or_, func, and_

    # Normalize inputs for comparison
    normalized_amount = normalize_amount(amount)
    normalized_company = normalize_company_name(company_name)

    # =========================================================================
    # TIER 0 (NEW): Exact company name + same round + date within ±3 days
    # This is the most aggressive tier to catch race condition duplicates.
    # When multiple articles about the same deal are processed in parallel,
    # the duplicate check may run before the first deal is committed.
    # This tier catches: Torq Series D Jan 11 vs Jan 12, Protege Series A same day
    # =========================================================================
    tier0_stmt = (
        select(Deal, PortfolioCompany)
        .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
        .where(func.lower(PortfolioCompany.name) == company_name.lower().strip())
        .where(Deal.round_type == round_type)
    )

    # Date filter: within ±3 days, or both null (recent deals)
    if announced_date:
        tier0_stmt = tier0_stmt.where(
            or_(
                Deal.announced_date.between(
                    announced_date - timedelta(days=3),
                    announced_date + timedelta(days=3)
                ),
                # Also match deals with null date if they're recent
                and_(
                    Deal.announced_date.is_(None),
                    Deal.created_at >= (datetime.now(timezone.utc) - timedelta(days=7)).replace(tzinfo=None)
                )
            )
        )
    else:
        # No date provided: match recent deals with null date or recent dates
        recent_cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=14)).replace(tzinfo=None)
        tier0_stmt = tier0_stmt.where(
            or_(
                Deal.announced_date.is_(None),
                Deal.announced_date >= (date.today() - timedelta(days=14))
            )
        ).where(Deal.created_at >= recent_cutoff_dt)

    tier0_result = await session.execute(tier0_stmt)
    tier0_candidates = tier0_result.all()

    for deal, company in tier0_candidates:
        logger.info(
            f"Found duplicate deal (TIER 0 exact company+round): '{company_name}' {round_type} "
            f"matches '{company.name}' {deal.round_type} (deal #{deal.id}, {deal.amount}, "
            f"date: incoming={announced_date}, existing={deal.announced_date})"
        )
        return deal

    # =========================================================================
    # TIER 4: Exact match on amount+date+round with company prefix match
    # Catches name variations like "MEQ Probe" vs "MEQ Solutions" that share
    # the same prefix but have different suffixes not in our known list.
    # Very high confidence: exact amount + exact date + same round = same deal.
    # =========================================================================
    if normalized_amount and announced_date:
        # Very tight tolerance (5%) for this tier since we're relaxing name match
        amount_low = int(normalized_amount * 0.95)
        amount_high = int(normalized_amount * 1.05)

        tier4_stmt = (
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.amount_usd.between(amount_low, amount_high))
            .where(Deal.announced_date == announced_date)
            .where(Deal.round_type == round_type)
        )
        tier4_result = await session.execute(tier4_stmt)
        tier4_candidates = tier4_result.all()

        for deal, company in tier4_candidates:
            existing_normalized = normalize_company_name(company.name)
            # FIX: Require at least 60% prefix overlap (not just 3 chars)
            # This prevents false matches like "MEQ Probe" vs "MEQ Consulting"
            if len(normalized_company) >= 3 and len(existing_normalized) >= 3:
                shorter_len = min(len(normalized_company), len(existing_normalized))
                min_prefix_len = max(3, int(shorter_len * 0.6))
                if normalized_company[:min_prefix_len] == existing_normalized[:min_prefix_len]:
                    logger.warning(
                        f"Found duplicate (TIER 4 prefix+exact): '{company_name}' {round_type} "
                        f"matches '{company.name}' (deal #{deal.id}, amount={deal.amount}, "
                        f"date={deal.announced_date}) - same prefix '{normalized_company[:min_prefix_len]}' "
                        f"(min_prefix_len={min_prefix_len})"
                    )
                    return deal

    # =========================================================================
    # TIER 3.5 (NEW): Website match for same round+date range
    # If incoming deal has a website that matches an existing company's website,
    # they're the same company regardless of name variations.
    # =========================================================================
    # Note: This requires the incoming extraction to have a website, which
    # Crunchbase CSV import provides. We'd need to add website parameter to
    # find_duplicate_deal to use this tier.

    # =========================================================================
    # TIER 3: Targeted amount-based query (most efficient for cross-round)
    # Uses amount_usd column for SQL filtering before Python name matching
    # =========================================================================
    if normalized_amount:
        # Calculate 10% tolerance range for SQL WHERE clause
        amount_low = int(normalized_amount * 0.90)
        amount_high = int(normalized_amount * 1.10)

        tier3_stmt = (
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.amount_usd.between(amount_low, amount_high))
        )

        # Date filter for TIER 3: within 30 days OR recent if no date
        if announced_date:
            date_start = announced_date - timedelta(days=30)
            date_end = announced_date + timedelta(days=30)
            recent_cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=60)).replace(tzinfo=None)
            tier3_stmt = tier3_stmt.where(
                or_(
                    Deal.announced_date.between(date_start, date_end),
                    (Deal.announced_date.is_(None) & (Deal.created_at >= recent_cutoff_dt))
                )
            )
        else:
            # No date: only check recent deals (last 60 days)
            recent_cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=60)).replace(tzinfo=None)
            tier3_stmt = tier3_stmt.where(
                or_(
                    Deal.announced_date >= (date.today() - timedelta(days=60)),
                    (Deal.announced_date.is_(None) & (Deal.created_at >= recent_cutoff_dt))
                )
            )

        tier3_stmt = tier3_stmt.limit(100)  # Amount filter is selective enough
        tier3_result = await session.execute(tier3_stmt)
        tier3_candidates = tier3_result.all()

        for deal, company in tier3_candidates:
            if company_names_match(company_name, company.name):
                logger.info(
                    f"Found duplicate deal (TIER 3 cross-round): '{company_name}' {round_type} "
                    f"matches '{company.name}' {deal.round_type} "
                    f"(deal #{deal.id}, {deal.amount}, amount_usd={deal.amount_usd})"
                )
                return deal

    # =========================================================================
    # TIER 2 TARGETED: Same-day exact match (prevents LIMIT 200 bug)
    # This runs a focused query for exact date + normalized company name match
    # to avoid missing duplicates when deal count exceeds LIMIT
    # =========================================================================
    if announced_date:
        tier2_stmt = (
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.announced_date == announced_date)
            .where(func.lower(PortfolioCompany.name) == company_name.lower().strip())
        )
        tier2_result = await session.execute(tier2_stmt)
        tier2_candidates = tier2_result.all()

        for deal, company in tier2_candidates:
            # Direct match - same date and exact company name
            logger.info(
                f"Found same-day duplicate (TIER 2 targeted): '{company_name}' matches '{company.name}' "
                f"(deal #{deal.id}, {deal.round_type}, {deal.amount}, date={deal.announced_date})"
            )
            return deal

        # Also check with company_names_match for fuzzy match on same day
        # FIX: Use company_names_match() instead of exact normalized comparison
        # This catches "Leona" vs "Leona Health" where 'health' is a known suffix
        tier2_fuzzy_stmt = (
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.announced_date == announced_date)
        )
        tier2_fuzzy_result = await session.execute(tier2_fuzzy_stmt)
        tier2_fuzzy_candidates = tier2_fuzzy_result.all()

        for deal, company in tier2_fuzzy_candidates:
            if company_names_match(company_name, company.name):
                logger.info(
                    f"Found same-day duplicate (TIER 2 fuzzy): '{company_name}' matches '{company.name}' "
                    f"(deal #{deal.id}, {deal.round_type}, {deal.amount}, date={deal.announced_date})"
                )
                return deal

    # =========================================================================
    # TIER 2.5: Same company + same round + date within 30 days
    # Catches duplicates with wildly different amounts (e.g., valuation vs funding)
    # or when amount_usd is NULL on the existing deal (TIER 3 depends on amount_usd).
    #
    # EXTENDED (2026-01): Changed from ±7 days to ±30 days to catch deals like
    # Emergent Series B where one source reported Jan 1 and another Jan 20.
    # For the SAME round type, it's extremely rare to have two separate rounds
    # within 30 days - this is almost always the same deal with different dates.
    # =========================================================================
    if announced_date:
        tier25_stmt = (
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.round_type == round_type)
            .where(Deal.announced_date.between(
                announced_date - timedelta(days=30),
                announced_date + timedelta(days=30)
            ))
        )
        tier25_result = await session.execute(tier25_stmt)
        tier25_candidates = tier25_result.all()

        for deal, company in tier25_candidates:
            if company_names_match(company_name, company.name):
                # FIX: Add amount sanity check - if both have amounts, they should be within 5x
                # This prevents matching a $5M Series A with a $50M Series A extension
                # EXCEPTION: Allow valuation confusion (if larger amount >= $500M, likely valuation vs funding)
                if normalized_amount and deal.amount_usd:
                    larger_amount = max(normalized_amount, deal.amount_usd)
                    smaller_amount = min(normalized_amount, deal.amount_usd)
                    ratio = larger_amount / max(1, smaller_amount)
                    # Skip if ratio > 5x AND larger amount < $500M (unlikely to be valuation confusion)
                    if ratio > 5 and larger_amount < 500_000_000:
                        logger.debug(
                            f"TIER 2.5 skip: '{company_name}' amount ratio {ratio:.1f}x too high "
                            f"(incoming={normalized_amount}, existing={deal.amount_usd})"
                        )
                        continue
                logger.warning(
                    f"Found duplicate (TIER 2.5 round+date ±30d): '{company_name}' {round_type} "
                    f"matches '{company.name}' {deal.round_type} (deal #{deal.id}, "
                    f"amounts: incoming={amount}, existing={deal.amount}, "
                    f"dates: incoming={announced_date}, existing={deal.announced_date}) "
                    f"- same round type within 30 days"
                )
                return deal
    else:
        # FIX: Null-date handling for TIER 2.5
        # For null-date deals, check recent deals with same round (last 30 days by created_at)
        recent_cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=30)).replace(tzinfo=None)
        tier25_null_stmt = (
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.round_type == round_type)
            .where(Deal.created_at >= recent_cutoff_dt)
        )
        tier25_null_result = await session.execute(tier25_null_stmt)
        tier25_null_candidates = tier25_null_result.all()

        for deal, company in tier25_null_candidates:
            if company_names_match(company_name, company.name):
                # Apply same amount sanity check (with valuation confusion exception)
                if normalized_amount and deal.amount_usd:
                    larger_amount = max(normalized_amount, deal.amount_usd)
                    smaller_amount = min(normalized_amount, deal.amount_usd)
                    ratio = larger_amount / max(1, smaller_amount)
                    if ratio > 5 and larger_amount < 500_000_000:
                        logger.debug(
                            f"TIER 2.5 null-date skip: '{company_name}' amount ratio {ratio:.1f}x too high"
                        )
                        continue
                logger.warning(
                    f"Found duplicate (TIER 2.5 null-date): '{company_name}' {round_type} "
                    f"matches '{company.name}' {deal.round_type} (deal #{deal.id}, "
                    f"amounts: incoming={amount}, existing={deal.amount}) "
                    f"- same round type, recent deal (null incoming date)"
                )
                return deal

    # =========================================================================
    # TIER 1: General query for strict round match
    # FIX: Add round_type filter at SQL level to avoid LIMIT 200 missing duplicates
    # =========================================================================
    stmt = (
        select(Deal, PortfolioCompany)
        .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
        .where(Deal.round_type == round_type)
    )

    # Date filter with fallback for missing dates - 1-year window
    if announced_date:
        date_start = announced_date - timedelta(days=365)
        date_end = announced_date + timedelta(days=365)
        recent_cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=365)).replace(tzinfo=None)
        stmt = stmt.where(
            or_(
                Deal.announced_date.between(date_start, date_end),
                (Deal.announced_date.is_(None) & (Deal.created_at >= recent_cutoff_dt))
            )
        )
    else:
        recent_cutoff = date.today() - timedelta(days=365)
        recent_cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=365)).replace(tzinfo=None)
        stmt = stmt.where(
            or_(
                Deal.announced_date >= recent_cutoff,
                (Deal.announced_date.is_(None) & (Deal.created_at >= recent_cutoff_dt))
            )
        )

    stmt = stmt.limit(200)

    result = await session.execute(stmt)
    candidates = result.all()

    for deal, company in candidates:
        # Skip if company names don't match (required for TIER 1)
        if not company_names_match(company_name, company.name):
            continue

        # TIER 1: Same round type + amount match
        if deal.round_type == round_type:
            if normalized_amount and deal.amount:
                existing_amount = normalize_amount(deal.amount)
                if existing_amount:
                    tolerance = max(normalized_amount, existing_amount) * 0.15
                    if abs(normalized_amount - existing_amount) <= tolerance:
                        logger.info(
                            f"Found duplicate deal (TIER 1): '{company_name}' matches '{company.name}' "
                            f"(deal #{deal.id}, {deal.round_type}, {deal.amount})"
                        )
                        return deal
            else:
                # FIX: One or both amounts missing - require tighter date window (60 days)
                # This prevents matching a Jan 2025 Series A with a Dec 2025 undisclosed Series A
                if announced_date and deal.announced_date:
                    days_apart = abs((announced_date - deal.announced_date).days)
                    if days_apart > 60:
                        logger.debug(
                            f"TIER 1 no-amount skip: '{company_name}' {days_apart} days apart "
                            f"(incoming={announced_date}, existing={deal.announced_date})"
                        )
                        continue
                logger.info(
                    f"Found duplicate deal (TIER 1, no amount): '{company_name}' matches '{company.name}' "
                    f"(deal #{deal.id}, {deal.round_type})"
                )
                return deal

    return None


async def check_article_url_exists(session: AsyncSession, url: str) -> bool:
    """
    Check if an article with this URL already exists in the database.

    Used to prevent duplicate article records when linking to existing deals.

    FIX: Use EXISTS subquery instead of SELECT with LIMIT 1 for efficiency.
    EXISTS stops at first match without fetching row data.
    """
    from sqlalchemy import exists
    stmt = select(exists().where(Article.url == url))
    result = await session.execute(stmt)
    return result.scalar()


async def save_deal(
    session: AsyncSession,
    extraction: DealExtraction,
    article_url: str,
    article_title: str,
    article_text: Optional[str] = None,
    source_fund_slug: Optional[str] = None,
    article_published_date: Optional[date] = None,
    scan_job_id: Optional[int] = None,
    sec_amount_usd: Optional[int] = None,
    amount_source: Optional[str] = None,
) -> Tuple[Optional[Deal], Optional[LeadDealAlertInfo]]:
    """
    Save an extracted deal to the database.

    Creates:
    - PortfolioCompany (if not exists)
    - Deal record
    - DealInvestor records for all investors
    - Article record linked to the deal

    Args:
        sec_amount_usd: Official SEC Form D amount (if from SEC EDGAR)
        amount_source: Source of amount ("sec_form_d" | "article" | "crunchbase")
                      SEC amounts are prioritized over LLM-extracted amounts.

    Returns:
        Tuple of (Deal, LeadDealAlertInfo) where:
        - Deal is None if duplicate (article still linked to existing deal)
        - LeadDealAlertInfo is provided if this is a lead deal that needs an alert
          (FIX #20: alert should be sent AFTER transaction commits)
    """
    # Lazy import to avoid circular import (storage -> harvester -> orchestrator -> storage)
    from ..harvester.fund_matcher import match_fund_name

    # Determine announced_date for duplicate check
    # FIX: Improved date validation to prevent wrong dates
    # FIX: Track date source for proper confidence scoring
    announced_date = extraction.round_date
    date_source_type = None  # Will be set based on how we got the date
    date_confidence = 0.5  # Default confidence

    # Cross-validate: warn if LLM date differs significantly from article date
    if announced_date and article_published_date:
        diff_days = abs((announced_date - article_published_date).days)
        if diff_days > 14:  # More than 2 weeks difference
            logger.warning(
                f"Date mismatch for {extraction.startup_name}: "
                f"LLM extracted {announced_date}, article published {article_published_date}, "
                f"diff={diff_days} days - using LLM date but flagging for review"
            )
            # Lower confidence when there's a mismatch
            date_source_type = "article_body"
            date_confidence = 0.55
        else:
            # LLM date matches article date - higher confidence
            date_source_type = "article_body"
            date_confidence = 0.70

    elif announced_date:
        # LLM extracted date without article date to validate
        date_source_type = "article_body"
        date_confidence = 0.60

    # Fallback to article date only if:
    # 1. LLM didn't extract a date
    # 2. Article date is not in the future (prevents wrong indexing dates)
    if announced_date is None and article_published_date:
        today = date.today()
        if article_published_date <= today:
            announced_date = article_published_date
            date_source_type = "article_published"
            date_confidence = 0.40  # Lowest confidence - just article publish date
        else:
            logger.warning(
                f"Skipping future article date for {extraction.startup_name}: "
                f"{article_published_date} is after today ({today})"
            )

    # Check for duplicate deal BEFORE creating new records
    existing_deal = await find_duplicate_deal(
        session,
        company_name=extraction.startup_name,
        round_type=extraction.round_label.value,
        amount=extraction.amount,
        announced_date=announced_date,
    )

    if existing_deal:
        logger.info(
            f"Skipping duplicate deal: {extraction.startup_name} "
            f"{extraction.round_label.value} - matches deal #{existing_deal.id}"
        )

        # Cross-reference: Update date based on confidence, not just "earlier is better"
        # FIX (2026-01): Changed from preferring earlier dates to preferring higher-confidence dates.
        # The Emergent bug showed that wrong dates (Jan 1) can come first, and correct dates (Jan 20)
        # arrive later with higher confidence from authoritative sources like press releases.
        if announced_date and existing_deal.announced_date:
            date_diff = abs((announced_date - existing_deal.announced_date).days)
            existing_confidence = existing_deal.date_confidence or 0.5

            if date_diff <= 1:
                # Dates match (within 1 day tolerance) - this is CONFIRMATION
                # Increment source count and boost confidence (multi-source bonus)
                existing_deal.date_source_count = (existing_deal.date_source_count or 1) + 1
                # Multi-source bonus: +0.1 for each additional confirming source
                new_confidence = min(0.95, existing_confidence + 0.1)
                if new_confidence > existing_confidence:
                    logger.info(
                        f"Date confirmed for deal #{existing_deal.id}: "
                        f"{existing_deal.announced_date} (sources: {existing_deal.date_source_count}, "
                        f"confidence: {existing_confidence:.2f} → {new_confidence:.2f})"
                    )
                    existing_deal.date_confidence = new_confidence
                    existing_deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

            elif date_confidence > existing_confidence:
                # New date has higher confidence - prefer it regardless of earlier/later
                # This handles cases like Emergent where Jan 1 (low confidence guess) came first
                # and Jan 20 (high confidence from press release) came later
                logger.info(
                    f"Updating deal #{existing_deal.id} date: "
                    f"{existing_deal.announced_date} → {announced_date} "
                    f"(higher confidence: {existing_confidence:.2f} → {date_confidence:.2f})"
                )
                existing_deal.announced_date = announced_date
                existing_deal.date_confidence = date_confidence
                existing_deal.date_source_count = (existing_deal.date_source_count or 1) + 1
                existing_deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

            elif _is_likely_placeholder_date(existing_deal.announced_date) and not _is_likely_placeholder_date(announced_date):
                # Existing date looks like a placeholder (Jan 1, first of month, etc.)
                # and new date looks more specific - prefer the new date
                logger.info(
                    f"Updating deal #{existing_deal.id} date: "
                    f"{existing_deal.announced_date} → {announced_date} "
                    f"(existing date looks like placeholder)"
                )
                existing_deal.announced_date = announced_date
                existing_deal.date_confidence = max(existing_confidence, date_confidence)
                existing_deal.date_source_count = (existing_deal.date_source_count or 1) + 1
                existing_deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

            elif announced_date < existing_deal.announced_date and date_confidence >= existing_confidence - 0.1:
                # New date is earlier AND confidence is similar - prefer earlier (original logic)
                # Only apply this if confidence difference is small (within 0.1)
                logger.info(
                    f"Updating deal #{existing_deal.id} date: "
                    f"{existing_deal.announced_date} → {announced_date} (earlier source found)"
                )
                existing_deal.announced_date = announced_date
                existing_deal.date_confidence = max(existing_confidence, date_confidence)
                existing_deal.date_source_count = (existing_deal.date_source_count or 1) + 1
                existing_deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

        elif announced_date and not existing_deal.announced_date:
            # Deal had no date, now we have one
            logger.info(
                f"Setting deal #{existing_deal.id} date: {announced_date} (previously null)"
            )
            existing_deal.announced_date = announced_date
            existing_deal.date_confidence = date_confidence
            existing_deal.date_source_count = 1
            existing_deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

        # FIX (2026-01): Update amount if existing deal has no valid amount but new extraction does
        # This handles cases where first article (e.g., VC blog post) doesn't mention amount
        # but subsequent articles (e.g., press releases) do
        # FIX: Also update if existing amount is a placeholder like "<UNKNOWN>", "None", "undisclosed"
        # FIX (2026-01): SEC Form D amounts always override non-SEC amounts (higher authority)
        should_update_amount = False
        new_amount = extraction.amount
        new_amount_usd = normalize_amount(extraction.amount)
        new_amount_source = amount_source or "article"

        # SEC Form D amount provided - highest priority
        # OPTIMIZATION: Check if we need to update BEFORE formatting
        if sec_amount_usd and amount_source == "sec_form_d":
            # SEC overrides non-SEC amounts (regardless of existing amount validity)
            if existing_deal.amount_source != "sec_form_d":
                new_amount = format_sec_amount(sec_amount_usd)
                new_amount_usd = sec_amount_usd
                new_amount_source = "sec_form_d"
                should_update_amount = True
                logger.info(
                    f"SEC amount overriding deal #{existing_deal.id}: {new_amount} (${new_amount_usd:,}) "
                    f"replaces '{existing_deal.amount}' (source: {existing_deal.amount_source or 'unknown'})"
                )
        # Non-SEC: only update if existing is invalid/placeholder
        elif _is_valid_amount(new_amount) and not _is_valid_amount(existing_deal.amount):
            should_update_amount = True
            logger.info(
                f"Updating deal #{existing_deal.id} amount: {new_amount} "
                f"(previously '{existing_deal.amount}') for {extraction.startup_name}"
            )

        if should_update_amount:
            existing_deal.amount = new_amount
            existing_deal.amount_usd = new_amount_usd
            existing_deal.amount_source = new_amount_source
            existing_deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

        # FIX: Use ON CONFLICT DO NOTHING to prevent race condition
        # Between check_article_url_exists() and insert, another task could insert same URL
        stmt = pg_insert(Article).values(
            deal_id=existing_deal.id,
            url=article_url,
            title=article_title,
            source_fund_slug=source_fund_slug,
            extracted_text=article_text,
            is_processed=True,
        ).on_conflict_do_nothing(index_elements=['url'])
        result = await session.execute(stmt)
        if result.rowcount > 0:
            await session.flush()
            logger.debug(f"Linked new article to existing deal #{existing_deal.id}")
        else:
            logger.debug(f"Article URL already exists (concurrent insert): {article_url}")
        return None, None  # Signal that this was a duplicate

    # Get or create company with website and LinkedIn
    company = await get_or_create_company(
        session,
        name=extraction.startup_name,
        description=extraction.startup_description,
        website=extraction.company_website,
        linkedin_url=extraction.company_linkedin,
    )

    # Convert founders to JSON for storage
    founders_json = None
    if extraction.founders:
        founders_data = [
            {
                "name": f.name,
                "title": f.title,
                "linkedin_url": f.linkedin_url,
            }
            for f in extraction.founders
        ]
        founders_json = json.dumps(founders_data)

    # announced_date was already determined above for duplicate check

    # Determine best amount: SEC Form D > LLM extraction > none
    # SEC amounts are official legal filings with exact values
    # LLM extractions often round (e.g., "$47.5M" -> "$50M")
    final_amount: Optional[str] = None
    final_amount_usd: Optional[int] = None
    final_amount_source: Optional[str] = None

    if sec_amount_usd and amount_source == "sec_form_d":
        # Use official SEC amount (highest priority)
        final_amount = format_sec_amount(sec_amount_usd)
        final_amount_usd = sec_amount_usd
        final_amount_source = "sec_form_d"
        logger.info(
            f"Using SEC Form D amount: {final_amount} (${sec_amount_usd:,}) for {extraction.startup_name} "
            f"(LLM extracted: {extraction.amount})"
        )
    else:
        # Fall back to LLM extraction
        final_amount = extraction.amount
        final_amount_usd = normalize_amount(extraction.amount)
        final_amount_source = amount_source or "article"

    # FIX Jan 2026: Use dedup_key + ON CONFLICT to prevent race condition duplicates
    # This is a database-level safety net that catches duplicates even when
    # find_duplicate_deal() runs before the first deal is committed
    dedup_key = make_dedup_key(
        extraction.startup_name,
        extraction.round_label.value,
        announced_date
    )

    # FIX Jan 2026 (Parloa bug): Generate amount_dedup_key for same-deal-different-round-type detection
    # This catches cases where LLM classifies same deal as "growth" vs "series_d"
    amount_dedup_key = make_amount_dedup_key(
        extraction.startup_name,
        final_amount_usd,
        announced_date
    )

    # FIX Jan 2026 (Parloa bug): Check if a deal with same amount_dedup_key already exists
    # This catches duplicates where the round_type differs but company+amount+date match
    if amount_dedup_key:
        existing_by_amount = await session.execute(
            select(Deal).where(Deal.amount_dedup_key == amount_dedup_key)
        )
        existing_deal_by_amount = existing_by_amount.scalar_one_or_none()
        if existing_deal_by_amount:
            new_round = extraction.round_label.value
            existing_round = existing_deal_by_amount.round_type

            logger.warning(
                f"Found duplicate by amount_dedup_key: {extraction.startup_name} "
                f"{new_round} matches existing deal #{existing_deal_by_amount.id} "
                f"({existing_round}) - likely same deal with different round classification"
            )

            # Log if round types differ - may need manual review
            # Don't auto-upgrade: "growth" might be legitimately correct, not all large rounds are Series X
            if new_round != existing_round:
                logger.warning(
                    f"ROUND_TYPE_MISMATCH: Deal #{existing_deal_by_amount.id} has '{existing_round}' "
                    f"but new article says '{new_round}' - may need manual review"
                )

            # Link article to existing deal
            article_stmt = pg_insert(Article).values(
                deal_id=existing_deal_by_amount.id,
                url=article_url,
                title=article_title,
                source_fund_slug=source_fund_slug,
                extracted_text=article_text,
                is_processed=True,
            ).on_conflict_do_nothing(index_elements=['url'])
            await session.execute(article_stmt)
            await session.flush()
            return None, None  # Signal duplicate

    # Create deal with enterprise AI classification using atomic INSERT ... ON CONFLICT
    # If a concurrent request already inserted a deal with the same dedup_key,
    # this will return None and we'll link the article to the existing deal instead
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    deal_values = {
        "company_id": company.id,
        "dedup_key": dedup_key,
        "amount_dedup_key": amount_dedup_key,
        "round_type": extraction.round_label.value,
        "amount": final_amount,
        "amount_usd": final_amount_usd,
        "amount_source": final_amount_source,
        "valuation": extraction.valuation,
        "announced_date": announced_date,
        "date_confidence": date_confidence,
        "date_source_count": 1 if announced_date else 0,
        "is_lead_confirmed": extraction.tracked_fund_is_lead,
        "lead_partner_name": extraction.tracked_fund_partner,
        "verification_snippet": extraction.verification_snippet,
        "lead_evidence_weak": getattr(extraction, 'lead_evidence_weak', False),
        "enterprise_category": extraction.enterprise_category.value if extraction.enterprise_category else None,
        "is_enterprise_ai": extraction.is_enterprise_ai,
        "is_ai_deal": getattr(extraction, 'is_ai_deal', extraction.is_enterprise_ai),
        "founders_json": founders_json,
        "confidence_score": extraction.confidence_score,
        "amount_needs_review": getattr(extraction, 'amount_needs_review', False),
        "amount_review_reason": getattr(extraction, 'amount_review_reason', None),
        "thesis_drift_score": 0.0,
        "scan_job_id": scan_job_id,
        "created_at": now,
        "updated_at": now,
    }

    stmt = pg_insert(Deal).values(**deal_values).on_conflict_do_nothing(
        index_elements=['dedup_key']
    ).returning(Deal)

    result = await session.execute(stmt)
    deal = result.scalar_one_or_none()

    # If deal is None, a concurrent insert already created this deal
    # Find the existing deal by dedup_key and link the article to it
    if deal is None:
        logger.warning(
            f"Race condition duplicate prevented by dedup_key: {extraction.startup_name} "
            f"{extraction.round_label.value} (key={dedup_key})"
        )
        # Find the existing deal by dedup_key
        existing_stmt = select(Deal).where(Deal.dedup_key == dedup_key)
        existing_result = await session.execute(existing_stmt)
        existing_deal = existing_result.scalar_one_or_none()

        if existing_deal:
            # Link article to existing deal
            article_stmt = pg_insert(Article).values(
                deal_id=existing_deal.id,
                url=article_url,
                title=article_title,
                source_fund_slug=source_fund_slug,
                extracted_text=article_text,
                is_processed=True,
            ).on_conflict_do_nothing(index_elements=['url'])
            await session.execute(article_stmt)
            await session.flush()
            logger.info(f"Linked article to existing deal #{existing_deal.id} (race condition)")
        else:
            logger.error(f"Could not find deal with dedup_key={dedup_key} after conflict")

        return None, None  # Signal duplicate

    # FIX: Populate DateSource table for traceability
    if announced_date and date_source_type:
        date_source_record = DateSource(
            deal_id=deal.id,
            source_type=date_source_type,
            source_url=article_url,
            extracted_date=announced_date,
            confidence_score=date_confidence,
            is_primary=True,
        )
        session.add(date_source_record)

    # Add lead investors
    for investor in extraction.lead_investors:
        fund = None
        # Use centralized fund matcher (handles variants + negative keywords)
        fund_slug = match_fund_name(investor.name)
        if fund_slug:
            fund = await get_or_create_fund(session, fund_slug)

        deal_investor = DealInvestor(
            deal_id=deal.id,
            fund_id=fund.id if fund else None,
            investor_name=investor.name,
            is_lead=True,  # Trust LLM's lead determination from full article context
            is_tracked_fund=fund is not None,  # Use actual match result
            partner_name=investor.partner_name,
            role=investor.role.value,
        )
        session.add(deal_investor)

    # Add participating investors
    for investor in extraction.participating_investors:
        fund = None
        # Use centralized fund matcher (handles variants + negative keywords)
        fund_slug = match_fund_name(investor.name)
        if fund_slug:
            fund = await get_or_create_fund(session, fund_slug)

        deal_investor = DealInvestor(
            deal_id=deal.id,
            fund_id=fund.id if fund else None,
            investor_name=investor.name,
            is_lead=False,
            is_tracked_fund=fund is not None,  # Use actual match result
            partner_name=investor.partner_name,
            role=investor.role.value,
        )
        session.add(deal_investor)

    # FIX: Flush investors immediately to prevent data loss on mid-loop exceptions
    # Without this, investors added to session may not persist if an error occurs later
    try:
        await session.flush()
    except Exception as e:
        logger.error(f"Failed to flush investors for deal {deal.id}: {e}")
        # Investors already added will be committed with deal (best effort)

    # Create article record - use ON CONFLICT to handle race conditions
    # Even for new deals, another task could be inserting the same article URL
    stmt = pg_insert(Article).values(
        deal_id=deal.id,
        url=article_url,
        title=article_title,
        source_fund_slug=source_fund_slug,
        extracted_text=article_text,
        is_processed=True,
    ).on_conflict_do_nothing(index_elements=['url'])
    result = await session.execute(stmt)
    if result.rowcount == 0:
        logger.debug(f"Article URL already exists (concurrent insert): {article_url}")

    await session.flush()

    # FIX #20: Return alert info for lead deals instead of sending directly
    # Caller should send alert AFTER transaction commits to avoid alerting for rolled-back deals
    alert_info = None
    if extraction.tracked_fund_is_lead:
        # Find the lead investor name from extraction
        lead_investor_name = "Unknown"
        for inv in extraction.lead_investors:
            if inv.is_tracked_fund:
                lead_investor_name = inv.name
                break

        alert_info = LeadDealAlertInfo(
            company_name=extraction.startup_name,
            amount=extraction.amount,
            round_type=extraction.round_label.value,
            lead_investor=lead_investor_name,
            enterprise_category=extraction.enterprise_category.value if extraction.enterprise_category else None,
            verification_snippet=extraction.verification_snippet,
        )

    return deal, alert_info


async def get_deals(
    session: AsyncSession,
    fund_slug: Optional[str] = None,
    stage: Optional[str] = None,
    is_lead: Optional[bool] = None,
    is_enterprise_ai: Optional[bool] = None,
    is_ai_deal: Optional[bool] = None,
    enterprise_category: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Deal]:
    """
    Fetch deals from database with optional filters.

    Args:
        fund_slug: Filter by specific fund
        stage: Filter by round type (seed, series_a, etc.)
        is_lead: Filter by lead status (True = tracked fund led)
        is_enterprise_ai: Filter for Enterprise AI only (True = B2B AI)
        is_ai_deal: Filter for all AI deals (True = any AI, enterprise or consumer)
        enterprise_category: Filter by category (infrastructure, security, etc.)
        limit: Max results
        offset: Pagination offset
    """
    stmt = select(Deal).order_by(nullslast(Deal.announced_date.desc()), Deal.created_at.desc())

    if stage:
        stmt = stmt.where(Deal.round_type == stage)

    if is_lead is not None:
        stmt = stmt.where(Deal.is_lead_confirmed == is_lead)

    if is_ai_deal is not None:
        stmt = stmt.where(Deal.is_ai_deal == is_ai_deal)

    if is_enterprise_ai is not None:
        stmt = stmt.where(Deal.is_enterprise_ai == is_enterprise_ai)

    if enterprise_category:
        stmt = stmt.where(Deal.enterprise_category == enterprise_category)

    if fund_slug:
        # Join to deal_investors to filter by fund
        stmt = stmt.join(DealInvestor).join(Fund).where(Fund.slug == fund_slug)

    stmt = stmt.offset(offset).limit(limit)

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_deal_with_details(session: AsyncSession, deal_id: int) -> Optional[Deal]:
    """Get a deal with all related data."""
    stmt = select(Deal).where(Deal.id == deal_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def update_deal(
    session: AsyncSession,
    deal_id: int,
    company_name: Optional[str] = None,
    website: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    round_type: Optional[str] = None,
    amount: Optional[str] = None,
    amount_usd: Optional[int] = None,
    announced_date: Optional[date] = None,
    is_lead_confirmed: Optional[bool] = None,
    lead_partner_name: Optional[str] = None,
    enterprise_category: Optional[str] = None,
    is_enterprise_ai: Optional[bool] = None,
    is_ai_deal: Optional[bool] = None,  # FIX 2026-01: Allow updating AI classification
    founders_json: Optional[str] = None,
    amount_needs_review: Optional[bool] = None,
    amount_review_reason: Optional[str] = None,
) -> Optional[Deal]:
    """
    Update a deal and its associated company.

    Updates both Deal and PortfolioCompany records.
    Only updates fields that are explicitly provided (non-None).

    Args:
        session: Database session
        deal_id: The deal to update
        company_name: New company name
        website: Company website URL
        linkedin_url: Company LinkedIn URL
        round_type: Round type (seed, series_a, etc.)
        amount: Amount string (e.g., "$50M")
        amount_usd: Normalized amount in USD
        announced_date: Deal announcement date
        is_lead_confirmed: Whether tracked fund led
        lead_partner_name: Partner who led the deal
        enterprise_category: AI category classification
        is_enterprise_ai: Whether B2B Enterprise AI
        founders_json: JSON string of founders

    Returns:
        Updated Deal or None if not found
    """
    # Fetch deal with company
    stmt = select(Deal).where(Deal.id == deal_id)
    result = await session.execute(stmt)
    deal = result.scalar_one_or_none()

    if not deal:
        return None

    # Fetch associated company
    company_stmt = select(PortfolioCompany).where(PortfolioCompany.id == deal.company_id)
    company_result = await session.execute(company_stmt)
    company = company_result.scalar_one_or_none()

    # Update company fields
    if company:
        if company_name is not None:
            company.name = company_name
        if website is not None:
            # Sanitize URL to prevent storing placeholders
            sanitized = _sanitize_url(website)
            if sanitized:
                company.website = sanitized
        if linkedin_url is not None:
            # Sanitize URL to prevent storing placeholders
            sanitized = _sanitize_url(linkedin_url)
            if sanitized:
                company.linkedin_url = sanitized
        company.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Update deal fields
    if round_type is not None:
        deal.round_type = round_type
    if amount is not None:
        deal.amount = amount
    if amount_usd is not None:
        deal.amount_usd = amount_usd
    if announced_date is not None:
        deal.announced_date = announced_date
    if is_lead_confirmed is not None:
        deal.is_lead_confirmed = is_lead_confirmed
    if lead_partner_name is not None:
        deal.lead_partner_name = lead_partner_name
    if enterprise_category is not None:
        deal.enterprise_category = enterprise_category
    if is_enterprise_ai is not None:
        deal.is_enterprise_ai = is_enterprise_ai
    if is_ai_deal is not None:
        deal.is_ai_deal = is_ai_deal
    if founders_json is not None:
        deal.founders_json = founders_json
    if amount_needs_review is not None:
        deal.amount_needs_review = amount_needs_review
    if amount_review_reason is not None:
        deal.amount_review_reason = amount_review_reason

    deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

    await session.flush()
    return deal


async def seed_funds(session: AsyncSession) -> None:
    """Seed the database with all 18 tracked funds."""
    for slug, config in FUND_REGISTRY.items():
        await get_or_create_fund(session, slug)
    await session.commit()


async def save_stealth_detection(
    session: AsyncSession,
    fund_slug: str,
    detected_url: str,
    company_name: Optional[str] = None,
    notes: Optional[str] = None,
) -> StealthDetection:
    """
    Save a stealth detection to the database.

    Stealth detections track portfolio additions found via:
    - Portfolio page diffing
    - LinkedIn job scraping
    - SEC EDGAR Form D filings
    - Delaware corporate filings

    Args:
        session: Database session
        fund_slug: The fund that backed the company
        detected_url: URL where the stealth signal was found
        company_name: Name of the company (if known)
        notes: Additional context about the detection

    Returns:
        The created StealthDetection record
    """
    # FIX: Use ON CONFLICT to prevent race condition
    # Between check and insert, another task could insert same URL
    from sqlalchemy.sql import func

    stmt = pg_insert(StealthDetection).values(
        fund_slug=fund_slug,
        detected_url=detected_url,
        company_name=company_name,
        notes=notes,
    ).on_conflict_do_update(
        index_elements=['detected_url'],
        set_={
            # Only update if new values are provided and existing are null
            'company_name': func.coalesce(
                StealthDetection.company_name,
                company_name
            ),
            'notes': func.coalesce(
                StealthDetection.notes,
                notes
            ),
        }
    ).returning(StealthDetection)

    result = await session.execute(stmt)
    detection = result.scalar_one()
    return detection


async def get_stealth_detections(
    session: AsyncSession,
    fund_slug: Optional[str] = None,
    is_confirmed: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[StealthDetection]:
    """
    Fetch stealth detections with optional filters.

    Args:
        session: Database session
        fund_slug: Filter by specific fund
        is_confirmed: Filter by confirmation status
        limit: Max results
        offset: Pagination offset

    Returns:
        List of StealthDetection records
    """
    stmt = select(StealthDetection).order_by(StealthDetection.detected_at.desc())

    if fund_slug:
        stmt = stmt.where(StealthDetection.fund_slug == fund_slug)

    if is_confirmed is not None:
        stmt = stmt.where(StealthDetection.is_confirmed == is_confirmed)

    stmt = stmt.offset(offset).limit(limit)

    result = await session.execute(stmt)
    return list(result.scalars().all())
