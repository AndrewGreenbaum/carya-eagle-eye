"""
Company Enrichment via Brave Search.

Uses Brave Search to find:
- Company official website
- CEO/Founder LinkedIn profiles

OPTIMIZED: Uses shared BraveClient (no duplicate HTTP client or retry logic).
FIX #7: Added cache for failed LinkedIn lookups to avoid redundant API calls.
"""

import asyncio
import json
import logging
import re
from difflib import SequenceMatcher
from time import time

logger = logging.getLogger(__name__)

# FIX #7: Cache for failed LinkedIn lookups (saves Brave API queries)
# Key: "founder_name|company_name" -> timestamp of failed lookup
# Skip re-searching if failed within LINKEDIN_CACHE_TTL_SECONDS
_failed_linkedin_cache: dict[str, float] = {}
# FIX: asyncio.Lock for thread-safe cache access in concurrent enrichment
_failed_linkedin_cache_lock = asyncio.Lock()

# COST OPT (2026-01): Cache for successful LinkedIn lookups
# Key: "founder_name|company_name" -> (linkedin_url, timestamp)
# Same founder+company often appears in multiple deals
_successful_linkedin_cache: dict[str, tuple[str, float]] = {}
_successful_linkedin_cache_lock = asyncio.Lock()

# COST OPTIMIZATION (Jan 2026): Extended from 7 days to 30 days
# Founder LinkedIn profiles don't change frequently - safe to cache longer
# Estimated savings: $8-12/month in Brave API costs
LINKEDIN_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60  # 30 days


def _get_linkedin_cache_key(founder_name: str, company_name: str) -> str:
    """Generate cache key for LinkedIn lookup.

    FIXED (2026-01): Normalizes names by collapsing multiple spaces.
    This ensures "John  Smith" and "John Smith" use the same cache key.
    """
    # Normalize: lowercase, strip, collapse multiple spaces
    name = ' '.join(founder_name.lower().split())
    company = ' '.join(company_name.lower().split())
    return f"{name}|{company}"


async def _is_linkedin_cached_failure(founder_name: str, company_name: str) -> bool:
    """Check if this founder+company combo recently failed.

    Thread-safe with asyncio.Lock to prevent race conditions during
    concurrent enrichment tasks.
    """
    key = _get_linkedin_cache_key(founder_name, company_name)
    async with _failed_linkedin_cache_lock:
        if key not in _failed_linkedin_cache:
            return False
        # Check if TTL expired
        cached_time = _failed_linkedin_cache[key]
        if time() - cached_time > LINKEDIN_CACHE_TTL_SECONDS:
            del _failed_linkedin_cache[key]
            return False
        return True


async def _mark_linkedin_failure(founder_name: str, company_name: str):
    """Mark this founder+company combo as failed.

    Thread-safe with asyncio.Lock to prevent race conditions during
    concurrent enrichment tasks.
    """
    key = _get_linkedin_cache_key(founder_name, company_name)
    async with _failed_linkedin_cache_lock:
        _failed_linkedin_cache[key] = time()


async def _get_cached_linkedin_success(founder_name: str, company_name: str) -> str | None:
    """Get cached successful LinkedIn URL if available and not expired.

    Thread-safe with asyncio.Lock.
    """
    key = _get_linkedin_cache_key(founder_name, company_name)
    async with _successful_linkedin_cache_lock:
        if key not in _successful_linkedin_cache:
            return None
        linkedin_url, cached_time = _successful_linkedin_cache[key]
        if time() - cached_time > LINKEDIN_CACHE_TTL_SECONDS:
            del _successful_linkedin_cache[key]
            return None
        return linkedin_url


async def _mark_linkedin_success(founder_name: str, company_name: str, linkedin_url: str):
    """Cache a successful LinkedIn lookup.

    Thread-safe with asyncio.Lock.
    """
    key = _get_linkedin_cache_key(founder_name, company_name)
    async with _successful_linkedin_cache_lock:
        _successful_linkedin_cache[key] = (linkedin_url, time())


def clear_linkedin_cache():
    """Clear the failed LinkedIn cache (called at start of scheduled jobs).

    DEPRECATED: Use cleanup_linkedin_cache() instead to preserve valid TTL entries.
    """
    global _failed_linkedin_cache
    count = len(_failed_linkedin_cache)
    _failed_linkedin_cache = {}
    if count > 0:
        logger.info(f"Cleared LinkedIn cache ({count} entries)")


async def cleanup_linkedin_cache() -> int:
    """Remove only expired entries from LinkedIn caches (preserves valid TTL entries).

    This is the preferred method for cache maintenance at job start, as it
    preserves valid cached entries and only removes expired ones.

    Returns:
        Total number of expired entries removed from both caches.
    """
    now = time()
    total_expired = 0

    # Clean up failure cache
    async with _failed_linkedin_cache_lock:
        expired_keys = [
            k for k, ts in _failed_linkedin_cache.items()
            if now - ts > LINKEDIN_CACHE_TTL_SECONDS
        ]
        for k in expired_keys:
            del _failed_linkedin_cache[k]
        total_expired += len(expired_keys)
        failure_active = len(_failed_linkedin_cache)

    # Clean up success cache
    async with _successful_linkedin_cache_lock:
        expired_keys = [
            k for k, (url, ts) in _successful_linkedin_cache.items()
            if now - ts > LINKEDIN_CACHE_TTL_SECONDS
        ]
        for k in expired_keys:
            del _successful_linkedin_cache[k]
        total_expired += len(expired_keys)
        success_active = len(_successful_linkedin_cache)

    if total_expired:
        logger.info(f"LinkedIn cache cleanup: removed {total_expired} expired, {failure_active} failures + {success_active} successes active")
    else:
        logger.debug(f"LinkedIn cache cleanup: no expired entries, {failure_active} failures + {success_active} successes active")

    return total_expired


from dataclasses import dataclass, field
from typing import Optional, List, Dict
from urllib.parse import urlparse

from ..common.brave_client import get_brave_client
from ..config.settings import settings


@dataclass
class DealContext:
    """Context from deal for better enrichment."""
    company_name: str
    lead_investor: Optional[str] = None
    founders: List[Dict] = field(default_factory=list)  # [{"name": "...", "title": "..."}]
    source_url: Optional[str] = None
    enterprise_category: Optional[str] = None


@dataclass
class BraveEnrichmentResult:
    """Result of company enrichment."""
    company_name: str
    website: Optional[str] = None
    ceo_name: Optional[str] = None
    ceo_linkedin: Optional[str] = None
    company_linkedin: Optional[str] = None  # Company page: linkedin.com/company/...
    founder_linkedins: Dict[str, str] = field(default_factory=dict)  # {name: linkedin_url}
    enrichment_source: str = "brave_search"


# LinkedIn profile URL pattern (FIX #9: require 3+ char username)
LINKEDIN_PROFILE_PATTERN = r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]{3,}"

# LinkedIn company page URL pattern (FIX #9: require 2+ char slug)
LINKEDIN_COMPANY_PATTERN = r"https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9_-]{2,}"

# Slugs that look like company/product names, not person names
# These are often /in/ URLs that are actually company pages or job postings
# Only match if the slug EQUALS or ENDS WITH these patterns (avoid false positives)
NON_PERSON_SLUG_SUFFIXES = {
    'postings', 'jobs', 'careers', 'hiring', 'official',
    'team', 'admin', 'support', 'contact', 'sales',
    'recruiting', 'talent', 'openings',
}


def _extract_name_from_linkedin_title(title: str) -> Optional[str]:
    """
    Extract person's name from LinkedIn search result title.

    Handles various LinkedIn title formats:
    - "John Smith - CEO at Company | LinkedIn"
    - "John Smith | CEO - Company"
    - "John Smith, CEO at Company"
    - "John Smith • CEO at Company"
    - "John Smith – CEO at Company" (em dash)
    - "John Smith: Founder & CEO"

    Returns the name or None if parsing fails.
    """
    if not title:
        return None

    # Remove common suffixes first
    title = re.sub(r'\s*\|\s*LinkedIn\s*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*-\s*LinkedIn\s*$', '', title, flags=re.IGNORECASE)

    # FIXED (2026-01): Guard against empty/whitespace string after processing
    title = title.strip()
    if not title:
        return None

    # Try multiple delimiter patterns (in order of specificity)
    # Pattern 1: Name before common delimiters (-, |, •, –, —, :, ,)
    delimiters = [' - ', ' | ', ' • ', ' – ', ' — ', ': ', ', ']
    for delim in delimiters:
        if delim in title:
            name_part = title.split(delim)[0].strip()
            # Validate it looks like a name (2+ words, not too long)
            if name_part and len(name_part.split()) >= 2 and len(name_part) < 50:
                # Reject if it looks like a title/role, not a name
                role_indicators = ['ceo', 'cto', 'founder', 'president', 'director', 'head of', 'vp ']
                if not any(ind in name_part.lower() for ind in role_indicators):
                    return name_part

    # Pattern 2: Fallback - just take first two words if they look like a name
    words = title.split()
    if len(words) >= 2:
        potential_name = f"{words[0]} {words[1]}"
        # Basic validation: both parts should be capitalized and reasonable length
        if (words[0][0].isupper() and words[1][0].isupper() and
            len(words[0]) >= 2 and len(words[1]) >= 2 and
            len(potential_name) < 40):
            # Reject if either word looks like a role/generic term, not a name
            # EXPANDED (2026-01): Added more generic words to prevent false matches
            generic_words = {
                # Job titles
                'ceo', 'cto', 'cfo', 'coo', 'cmo', 'cpo', 'ciso', 'cro',
                'founder', 'founders', 'cofounder', 'president', 'chairman',
                'director', 'manager', 'engineer', 'developer', 'designer',
                'analyst', 'consultant', 'specialist', 'coordinator', 'associate',
                'head', 'chief', 'senior', 'junior', 'lead', 'principal', 'staff',
                'vice', 'executive', 'officer', 'partner', 'member',
                # Generic page/content words
                'profile', 'page', 'company', 'linkedin', 'view', 'about',
                'contact', 'support', 'admin', 'team', 'portal', 'official',
                'careers', 'jobs', 'hiring', 'recruiting', 'talent', 'hr',
                'sales', 'marketing', 'product', 'business', 'corporate',
                # Locations/regions
                'global', 'international', 'regional', 'local', 'national',
                'north', 'south', 'east', 'west', 'central', 'americas', 'emea', 'apac',
                # Departments
                'operations', 'finance', 'legal', 'technology', 'research',
                'customer', 'client', 'service', 'services', 'solutions',
            }
            if (words[0].lower() not in generic_words and
                words[1].lower() not in generic_words):
                # Additional check: name parts should be 2-15 chars (real names)
                if 2 <= len(words[0]) <= 15 and 2 <= len(words[1]) <= 15:
                    return potential_name

    return None


def _extract_name_from_linkedin_slug(linkedin_url: str) -> Optional[str]:
    """
    Extract person's name from LinkedIn URL slug as fallback.

    Examples:
    - "linkedin.com/in/john-smith" → "John Smith"
    - "linkedin.com/in/sarah-chen-ceo" → "Sarah Chen"
    - "linkedin.com/in/john-q-smith" → "John Smith" (skips middle initial)
    - "linkedin.com/in/jsmith123" → None (not a parseable name)

    Returns the name or None if parsing fails.
    """
    if not linkedin_url:
        return None

    # Extract the slug from URL
    # FIX: Anchor to domain boundary to prevent matching "notlinkedin.com"
    # Pattern allows: start, whitespace, :, or / before (optional www.) linkedin.com
    # This handles: linkedin.com, www.linkedin.com, https://linkedin.com, etc.
    match = re.search(r'(?:^|[\s/:])(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)', linkedin_url.lower())
    if not match:
        return None

    slug = match.group(1)

    # Split by hyphens
    parts = slug.split('-')

    # Filter out common suffixes that aren't name parts
    # Note: Single letters (a, b, c, q, etc.) are filtered as they're usually:
    # - Middle initials (we skip these for simplicity)
    # - Duplicate disambiguation (john-smith-a vs john-smith-b)
    non_name_parts = {
        'ceo', 'cto', 'cfo', 'coo', 'founder', 'cofounder', 'president',
        'md', 'mba', 'phd', 'dr', 'prof', 'jr', 'sr', 'iii', 'ii',
        '1', '2', '3', '123', '01', '02',
    }
    # Filter: not in non_name_parts, not a digit, and either 2+ chars OR keep single letters initially
    name_parts = []
    for p in parts:
        if not p or p in non_name_parts or p.isdigit():
            continue
        # Single letters are likely middle initials or suffixes - skip them
        if len(p) == 1:
            continue
        name_parts.append(p)

    # Need at least 2 parts for first + last name
    if len(name_parts) < 2:
        return None

    # Take first two parts as first and last name
    first_name = name_parts[0].capitalize()
    last_name = name_parts[1].capitalize()

    # Basic validation
    if len(first_name) < 2 or len(last_name) < 2:
        return None

    return f"{first_name} {last_name}"


def _names_match(name1: Optional[str], name2: Optional[str]) -> bool:
    """
    Check if two names likely refer to the same person.

    TIGHTENED (2026-01): Requires BOTH first AND last name to match.
    Previous logic allowed "James Smith" to match "James Johnson" because both had "James".

    This catches variations like "Jason Lynch" vs "Jason M. Lynch" but rejects
    "Jason Lynch" vs "Jason Smith".

    Returns True if names match, False otherwise.
    """
    if not name1 or not name2:
        return False

    parts1 = [p.lower().strip() for p in name1.split() if len(p) >= 2]
    parts2 = [p.lower().strip() for p in name2.split() if len(p) >= 2]

    if not parts1 or not parts2:
        return False

    # Need at least 2 parts (first + last name) for proper matching
    if len(parts1) < 2 or len(parts2) < 2:
        # Single name part - require exact match
        if len(parts1) == 1 and len(parts2) == 1:
            return parts1[0] == parts2[0]
        # One has full name, other has single - check if single appears in full
        single = parts1 if len(parts1) == 1 else parts2
        full = parts2 if len(parts1) == 1 else parts1
        return single[0] in full

    # TIGHTENED: Require BOTH first AND last name to match
    # First name is parts[0], last name is parts[-1] (handles middle names)
    first1, last1 = parts1[0], parts1[-1]
    first2, last2 = parts2[0], parts2[-1]

    # Both first and last must match (allow for minor variations)
    first_matches = (
        first1 == first2 or
        first1.startswith(first2) or first2.startswith(first1)  # Nick vs Nicholas
    )
    last_matches = last1 == last2

    return first_matches and last_matches


# Skip these domains (not startup company sites)
SKIP_DOMAINS = {
    # News/Media
    "crunchbase.com", "linkedin.com", "twitter.com", "facebook.com",
    "bloomberg.com", "techcrunch.com", "forbes.com", "reuters.com",
    "wikipedia.org", "pitchbook.com", "cbinsights.com", "tracxn.com",
    "owler.com", "zoominfo.com", "apollo.io", "glassdoor.com", "indeed.com",
    "ycombinator.com", "producthunt.com", "github.com", "youtube.com",
    "medium.com", "substack.com", "sec.gov", "businesswire.com", "prnewswire.com",
    # Entertainment/Sports (false positives)
    "spotify.com", "apple.com", "amazon.com", "google.com",
    "rangers.co.uk", "leonalewismusic.com", "apache.org",
    "music.com", "band.com", "artist.com",
    # Generic
    "wix.com", "squarespace.com", "wordpress.com", "blogspot.com",
}

# Known wrong matches to skip
KNOWN_FALSE_POSITIVES = {
    "maven": ["apache.org", "maven.apache.org"],
    "leona": ["leonalewis", "music", "leonalewismusic"],
    "ranger": ["rangers.co.uk", "rangerfc", "football", "soccer"],
    "unknown": ["weareunknown"],
    # Miden name collision: 0xMiden (blockchain) vs Miden (YC fintech) vs Miden.ai (solar)
    "miden": ["miden.co"],  # Skip African fintech when enriching blockchain Miden
    "0xmiden": ["miden.co"],  # Same - prefer miden.xyz
    # Graphite (code review) vs Graphite Health (healthcare IT)
    "graphite": ["graphitehealth", "graphitehealth.io", "graphitehealth.com"],
}

# Domain keywords that indicate wrong industry (unless company name contains them)
HEALTHCARE_DOMAIN_KEYWORDS = ["health", "medical", "clinic", "hospital", "pharma", "care"]

# Valid TLDs for startup websites
# FIX #7: added net, org, us, uk, eu, info
# FIX (2026-01): Added European country TLDs and industry-specific TLDs
VALID_TLDS = (
    # Common startup TLDs
    "com", "io", "ai", "co", "dev", "app", "tech", "cloud", "xyz", "me",
    "health", "bio", "so", "gg", "net", "org", "us", "uk", "eu", "info",
    # European country TLDs (many EU startups use these)
    "de", "ch", "nl", "fr", "se", "no", "at", "be", "fi", "dk", "es", "it", "pl",
    # Industry-specific TLDs
    "studio", "solutions", "systems", "ventures", "fund", "consulting",
)


class BraveEnrichmentClient:
    """
    Client for enriching company data via Brave Search.

    OPTIMIZED: Uses shared BraveClient instead of creating its own HTTP client.
    FIX #4: Added shared HTTP client for website verification.
    """

    def __init__(self):
        self.rate_limit_delay = settings.brave_search_rate_limit_delay
        self._http_client: Optional["httpx.AsyncClient"] = None
        # FIXED (2026-01): Lock to prevent race condition in HTTP client creation
        self._http_client_lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # FIX #4: Close HTTP client on exit
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _get_http_client(self) -> "httpx.AsyncClient":
        """Get or create shared HTTP client for website verification.

        FIXED (2026-01): Uses lock to prevent race condition when multiple
        coroutines call this concurrently. Also limits redirects to prevent
        redirect chain hijacking.
        """
        import httpx
        async with self._http_client_lock:
            if self._http_client is None:
                self._http_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(5.0),
                    follow_redirects=True,
                    max_redirects=3,  # FIX: Limit redirects to prevent hijacking
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                )
        return self._http_client

    def _is_valid_startup_website(self, url: str, company_name: str) -> bool:
        """Check if URL is a valid startup website (not a false positive)."""
        # Guard: reject empty/whitespace company names (prevents "" in domain matching)
        if not company_name or not company_name.strip():
            return False
        try:
            url_lower = url.lower()
            parsed = urlparse(url_lower)
            domain = parsed.netloc.replace("www.", "")

            # Skip known non-startup domains
            for skip in SKIP_DOMAINS:
                if skip in domain:
                    return False

            # Check known false positives for this company name
            company_lower = company_name.lower()
            for keyword, false_urls in KNOWN_FALSE_POSITIVES.items():
                if keyword in company_lower:
                    for false_url in false_urls:
                        if false_url in url_lower:
                            return False

            # Must have a reasonable TLD
            tld = domain.split(".")[-1]
            if tld not in VALID_TLDS:
                return False

            # Company name should appear in domain (stricter matching)
            company_slug = re.sub(r"[^a-z0-9]", "", company_lower)
            domain_slug = re.sub(r"[^a-z0-9]", "", domain.split(".")[0])

            # FIX #10: Get all significant words (including short ones like "AI")
            all_words = [re.sub(r"[^a-z0-9]", "", w) for w in company_lower.split()]
            all_words = [w for w in all_words if w]  # Remove empty strings
            # First word for longer-name matching (min 3 chars to be considered "significant")
            significant_words = [w for w in all_words if len(w) >= 3]
            first_word = significant_words[0] if significant_words else company_slug

            # FIX #11: Healthcare domain filter - only apply for longer company names
            # This prevents "Care" blocking "carehealth.io" but still blocks
            # "Graphite" → "graphitehealth.io"
            if len(company_slug) >= 5:
                for healthcare_kw in HEALTHCARE_DOMAIN_KEYWORDS:
                    if healthcare_kw in domain_slug and healthcare_kw not in company_slug:
                        return False

            # FIX (2026-01): Handle very short company names (2-3 chars) like "GV", "AI"
            # For these, require exact domain match to avoid false positives
            if len(company_slug) <= 3:
                if company_slug == domain_slug:
                    return True
                # Also check if company slug exactly equals domain (case: "GV" → "gv.com")
                # Already covered by above, but be explicit
                return False  # For short names, only exact match is safe

            # FIX: Stricter matching - require significant overlap
            # Option A: Full company slug appears in domain
            if company_slug in domain_slug:
                return True

            # Option B: Domain slug appears in company slug (for short domains like "gv.com")
            # Only allow this for domains with 4+ chars
            if len(domain_slug) >= 4 and domain_slug in company_slug:
                return True

            # FIX #10: Option C-alt: Any significant word (2+ chars) matches domain exactly
            # Handles "AI Corp" → "aicorp.com" where domain = "aicorp"
            for word in all_words:
                if len(word) >= 2 and word == domain_slug:
                    return True

            # Option C: First significant word (5+ chars) is in domain
            # This catches "Trade Republic" → "traderepublic.com"
            if len(first_word) >= 5 and first_word in domain_slug:
                # Also verify domain doesn't have extra unrelated words
                # e.g., reject "tradingview" for "Trade Republic"
                remaining = domain_slug.replace(first_word, "")
                # If remaining is short or empty, it's likely correct
                if len(remaining) <= 4:
                    return True
                # If remaining is in the company name, also OK
                if remaining and remaining in company_slug:
                    return True
                # Otherwise, reject - domain has extra unrelated content
                return False

            # Option D: Exact match
            if company_slug == domain_slug:
                return True

            return False
        except Exception:
            return False

    async def _verify_website_exists(self, url: str, company_name: Optional[str] = None) -> bool:
        """Verify that a website URL responds with 2xx status.

        Uses HEAD request with short timeout to minimize latency.
        Returns True if site responds, False otherwise.

        FIX #4: Uses shared HTTP client instead of creating new one each call.
        FIX (2026-01): Check final URL domain after redirects to prevent
        redirect chains from returning True (e.g., example.com → linkedin.com).
        """
        try:
            client = await self._get_http_client()
            response = await client.head(url)
            if response.status_code >= 400:
                # Some sites block HEAD, try GET
                response = await client.get(url)
                if response.status_code >= 400:
                    return False

            # FIX (2026-01): Check if final URL after redirects is still valid
            # Reject if redirected to a known non-startup domain
            final_url = str(response.url) if hasattr(response, 'url') else url
            final_domain = urlparse(final_url).netloc.replace("www.", "").lower()

            for skip_domain in SKIP_DOMAINS:
                if skip_domain in final_domain:
                    logger.debug(
                        f"Website verification failed: {url} redirected to "
                        f"invalid domain {final_domain}"
                    )
                    return False

            # If company_name provided, verify final domain matches company
            if company_name and not self._is_valid_startup_website(final_url, company_name):
                logger.debug(
                    f"Website verification failed: {url} redirected to "
                    f"{final_url} which doesn't match company {company_name}"
                )
                return False

            return True
        except Exception as e:
            logger.debug(f"Website verification failed for {url}: {e}")
            return False

    def _extract_linkedin_url(self, text: str) -> Optional[str]:
        """Extract LinkedIn profile URL from text.

        FIX #35: Validates URL is a profile page, not /jobs/, /posts/, etc.
        FIX #3: Full sanitization - checks placeholders + normalizes https://.
        FIX (Jan 2026): Reject slugs that look like company/product names.
        """
        # FIX #3: Check for placeholder text first
        text_lower = text.lower()
        if any(p in text_lower for p in ["not mentioned", "not specified", "unknown", "n/a"]):
            return None

        match = re.search(LINKEDIN_PROFILE_PATTERN, text)
        if match:
            url = match.group(0)
            # FIX #35: Reject non-profile LinkedIn URLs
            invalid_paths = ["/search", "/company/", "/jobs", "/posts", "/activity", "/pulse", "/learning"]
            if not any(path in url for path in invalid_paths):
                # FIX (Jan 2026): Reject slugs that look like company/product names
                # e.g., "linkedin.com/in/malpostings" is not a person
                slug_match = re.search(r'/in/([a-zA-Z0-9_-]+)', url.lower())
                if slug_match:
                    slug = slug_match.group(1)
                    # Check if slug equals or ends with non-person patterns
                    # Using suffix match to avoid false positives (e.g., "shresthakukreja" contains "hr")
                    for pattern in NON_PERSON_SLUG_SUFFIXES:
                        if slug == pattern or slug.endswith(pattern):
                            logger.debug(f"Rejecting non-person LinkedIn slug: {slug} (matches '{pattern}')")
                            return None
                # FIX #3: Normalize URL to https://
                if url.startswith("http://"):
                    url = "https://" + url[7:]
                return url
        return None

    def _extract_company_linkedin_url(self, text: str) -> Optional[str]:
        """Extract LinkedIn company page URL from text.

        Only accepts /company/ URLs, not /jobs/, /posts/, etc.
        FIX #3: Normalizes URL to ensure https:// prefix.
        """
        match = re.search(LINKEDIN_COMPANY_PATTERN, text)
        if match:
            url = match.group(0)
            # Reject non-company paths that might be appended
            invalid_suffixes = ["/jobs", "/posts", "/people", "/about", "/life"]
            if not any(suffix in text[match.end():match.end()+10] for suffix in invalid_suffixes):
                # FIX #3: Normalize URL to https://
                if url.startswith("http://"):
                    url = "https://" + url[7:]
                return url
        return None

    async def find_company_linkedin(self, company_name: str) -> Optional[str]:
        """Find company's LinkedIn page URL.

        Searches for the company's official LinkedIn company page.

        Args:
            company_name: Name of the company

        Returns:
            LinkedIn company page URL or None
        """
        query = f'site:linkedin.com/company "{company_name}"'
        results = await self._search(query, count=5)

        company_lower = company_name.lower()
        company_slug = re.sub(r'[^a-z0-9]', '', company_lower)

        for result in results:
            url = result.get("url", "")
            title = result.get("title", "").lower()

            # Must be a LinkedIn company URL
            linkedin_url = self._extract_company_linkedin_url(url)
            if not linkedin_url:
                continue

            # Extract company slug from LinkedIn URL path
            # e.g., "linkedin.com/company/openai" -> "openai"
            url_path_match = re.search(r'linkedin\.com/company/([a-z0-9_-]+)', url.lower())
            if not url_path_match:
                continue
            url_company_slug = url_path_match.group(1)

            # FIX #6 & #12: Stricter matching for short company names
            # Require company slug to be at least 4 chars for substring match
            # Otherwise require exact match or company name in title
            if len(company_slug) >= 4:
                # Longer names: allow substring match
                if company_slug in url_company_slug or url_company_slug in company_slug:
                    return linkedin_url
            else:
                # Short names (ai, gv, etc.): require exact match
                if company_slug == url_company_slug:
                    return linkedin_url

            # Also accept if company name appears in title
            if company_lower in title:
                return linkedin_url

        return None

    async def _search(self, query: str, count: int = 5, use_cache: bool = False) -> List[Dict]:
        """Execute Brave Search query using shared client.

        FIX #36: Wrapped with timeout to prevent hanging.
        Args:
            query: Search query string
            count: Number of results (default 5)
            use_cache: If True, use TTL cache for results (recommended for LinkedIn searches)
        """
        client = get_brave_client()
        if not client.validate_api_key():
            logger.warning("BRAVE_SEARCH_KEY not configured for enrichment")
            return []

        try:
            # FIX #36: Add timeout to prevent indefinite hangs
            data = await asyncio.wait_for(
                client.search_web(query, count, use_cache=use_cache),
                timeout=settings.enrichment_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Brave search timeout for query: {query[:50]}...")
            return []

        if data is None:
            return []

        return data.get("web", {}).get("results", [])

    async def find_website_with_context(
        self,
        company_name: str,
        investor: Optional[str] = None,
        category: Optional[str] = None
    ) -> Optional[str]:
        """Find official startup website using investor context for disambiguation."""
        if not company_name or company_name.lower() in ("<unknown>", "unknown", "n/a", "no funding deal"):
            return None

        # Build context-aware query
        query_parts = [f'"{company_name}"']

        # Add investor for disambiguation (most important!)
        if investor:
            investor_short = investor.split()[0] if investor else ""
            query_parts.append(f'"{investor_short}"')

        # Add category context
        if category:
            category_map = {
                "vertical_saas": "healthcare software",
                "infrastructure": "developer tools",
                "security": "cybersecurity",
                "agentic": "AI automation",
                "data_intelligence": "enterprise data",
            }
            query_parts.append(category_map.get(category, "startup"))
        else:
            query_parts.append("startup")

        query_parts.append("official website")
        query = " ".join(query_parts)

        results = await self._search(query, count=10)

        for result in results:
            url = result.get("url", "")
            if self._is_valid_startup_website(url, company_name):
                parsed = urlparse(url)
                website_url = f"https://{parsed.netloc}"
                # Verify website actually exists (with redirect domain check)
                if await self._verify_website_exists(website_url, company_name):
                    logger.info(f"Website verified for {company_name}: {website_url}")
                    return website_url
                else:
                    logger.warning(f"Website verification failed for {company_name}: {website_url}")

        # Fallback: try simpler query with investor
        if investor:
            query = f'"{company_name}" "{investor}" startup'
            results = await self._search(query, count=5)
            for result in results:
                url = result.get("url", "")
                if self._is_valid_startup_website(url, company_name):
                    parsed = urlparse(url)
                    website_url = f"https://{parsed.netloc}"
                    # Verify website actually exists (with redirect domain check)
                    if await self._verify_website_exists(website_url, company_name):
                        logger.info(f"Website verified for {company_name}: {website_url}")
                        return website_url

        return None

    async def find_founder_linkedin(
        self,
        founder_name: str,
        company_name: str
    ) -> Optional[str]:
        """Find LinkedIn for a specific founder by name."""
        if not founder_name or len(founder_name) < 3:
            return None

        # Skip generic names
        if founder_name.lower() in ("ceo", "founder", "unknown", "<unknown>"):
            return None

        # COST OPT: Check cache for recent successes first
        cached_url = await _get_cached_linkedin_success(founder_name, company_name)
        if cached_url:
            logger.debug(f"LinkedIn cache hit (success) for {founder_name}: {cached_url}")
            return cached_url

        # FIX #7: Check cache for recent failures
        if await _is_linkedin_cached_failure(founder_name, company_name):
            logger.debug(f"Skipping LinkedIn search for {founder_name} (cached failure)")
            return None

        # FIX #2: Target LinkedIn domain directly for better precision
        # COST OPT: Enable caching - same founder+company often appears in multiple deals
        query = f'site:linkedin.com/in "{founder_name}" "{company_name}"'
        results = await self._search(query, count=5, use_cache=True)

        # FIX #8: Track if we found any LinkedIn URLs (even if name didn't match)
        found_any_linkedin = False

        for result in results:
            url = result.get("url", "")
            title = result.get("title", "")
            description = result.get("description", "")

            # Must be a LinkedIn profile URL
            linkedin_url = self._extract_linkedin_url(url)
            if linkedin_url:
                found_any_linkedin = True

                # FIX (Jan 2026): Extract name from profile and use proper name matching
                # This prevents returning wrong person's LinkedIn
                profile_name = _extract_name_from_linkedin_title(title)
                if not profile_name:
                    profile_name = _extract_name_from_linkedin_slug(linkedin_url)

                # If we extracted a name from profile, verify it matches the founder
                if profile_name:
                    if _names_match(founder_name, profile_name):
                        logger.debug(f"LinkedIn name match: '{founder_name}' ~ '{profile_name}'")
                        await _mark_linkedin_success(founder_name, company_name, linkedin_url)
                        return linkedin_url
                    else:
                        logger.debug(f"LinkedIn name mismatch: looking for '{founder_name}', found '{profile_name}'")
                        continue  # Skip this result, try next one

                # Fallback: Check if name parts appear in title/description
                # (only if we couldn't extract a name from the profile)
                # FIX (Jan 2026): Require BOTH first AND last name to match
                # Single name matching caused mismatches (e.g., "Alfia Ilicheva" matching
                # description mentioning "Alfia" but URL for different person "dniewczas")
                name_parts = [p for p in founder_name.lower().split() if len(p) >= 3]
                text_to_check = f"{title} {description}".lower()

                # Require BOTH first AND last name to appear (stricter matching)
                if len(name_parts) >= 2 and all(part in text_to_check for part in name_parts[:2]):
                    logger.debug(f"LinkedIn fallback match: both '{name_parts[0]}' and '{name_parts[1]}' found in text")
                    await _mark_linkedin_success(founder_name, company_name, linkedin_url)
                    return linkedin_url
                else:
                    # Can't verify this is the right person - skip
                    logger.debug(f"LinkedIn skipped: couldn't verify '{founder_name}' from profile or text")
                    continue

        # FIX #8: Only do fallback if first search returned NO LinkedIn URLs at all
        # This avoids wasting API calls when we found LinkedIn profiles but just
        # couldn't match the name (likely wrong person)
        if not found_any_linkedin:
            # Fallback: search without company name but still target LinkedIn
            # COST OPT: Enable caching for fallback searches too
            query = f'site:linkedin.com/in "{founder_name}" founder startup'
            results = await self._search(query, count=3, use_cache=True)

            for result in results:
                url = result.get("url", "")
                linkedin_url = self._extract_linkedin_url(url)
                if linkedin_url:
                    # FIX (2026-01): Don't mark success here - return URL without caching.
                    # The caller (enrich_with_context) will validate and cache only if valid.
                    # Previously this caused cache poisoning: URLs were cached as "success"
                    # before validation, then rejected by validation, but cache was already poisoned.
                    return linkedin_url

        # FIX #7: Mark this as a failed search to avoid redundant future calls
        await _mark_linkedin_failure(founder_name, company_name)
        return None

    async def find_ceo_linkedin(self, company_name: str) -> tuple[Optional[str], Optional[str]]:
        """
        Find CEO/Founder LinkedIn with startup context.

        IMPROVED (Jan 2026):
        1. Robust name parsing from LinkedIn title (handles -, |, •, –, etc.)
        2. Fallback to URL slug extraction (/in/john-smith → "John Smith")
        3. Multiple search strategies for better coverage
        """
        if not company_name or company_name.lower() in ("<unknown>", "unknown", "n/a"):
            return (None, None)

        # FIX #2: Target LinkedIn domain directly for better precision
        query = f'site:linkedin.com/in "{company_name}" CEO founder startup'
        results = await self._search(query, count=5)

        for result in results:
            url = result.get("url", "")
            title = result.get("title", "")
            description = result.get("description", "")

            result_text = f"{title} {description}".lower()
            company_lower = company_name.lower()

            if company_lower not in result_text and company_lower.split()[0] not in result_text:
                continue

            linkedin_url = self._extract_linkedin_url(url)
            if linkedin_url:
                # IMPROVED: Try robust title parsing first
                name = _extract_name_from_linkedin_title(title)

                # FALLBACK: Try extracting from URL slug if title parsing fails
                if not name:
                    name = _extract_name_from_linkedin_slug(linkedin_url)
                    if name:
                        logger.debug(f"Extracted name from URL slug: {name} ({linkedin_url})")

                return (name, linkedin_url)

            linkedin_url = self._extract_linkedin_url(description)
            if linkedin_url:
                # Try to extract name from URL slug even for description URLs
                name = _extract_name_from_linkedin_slug(linkedin_url)
                return (name, linkedin_url)

        return (None, None)

    async def find_ceo_via_web_search(self, company_name: str) -> tuple[Optional[str], Optional[str]]:
        """
        Secondary search: Find CEO/founder via general web search.

        When LinkedIn-specific search fails, try a broader web search
        that may surface LinkedIn URLs in news articles, company pages, etc.

        Returns: (ceo_name, linkedin_url) or (None, None)
        """
        if not company_name or company_name.lower() in ("<unknown>", "unknown", "n/a"):
            return (None, None)

        # Broader web search (not site-restricted to LinkedIn)
        query = f'"{company_name}" CEO founder linkedin'
        results = await self._search(query, count=10)

        company_lower = company_name.lower()

        for result in results:
            url = result.get("url", "")
            title = result.get("title", "")
            description = result.get("description", "")
            result_text = f"{title} {description}".lower()

            # Must mention the company
            if company_lower not in result_text and company_lower.split()[0] not in result_text:
                continue

            # Look for LinkedIn URLs in the description (often cited in news articles)
            linkedin_url = self._extract_linkedin_url(description)
            if linkedin_url:
                name = _extract_name_from_linkedin_slug(linkedin_url)
                if name:
                    logger.info(f"Found CEO via web search for {company_name}: {name}")
                    return (name, linkedin_url)

            # Check if the URL itself is a LinkedIn profile
            linkedin_url = self._extract_linkedin_url(url)
            if linkedin_url:
                name = _extract_name_from_linkedin_title(title)
                if not name:
                    name = _extract_name_from_linkedin_slug(linkedin_url)
                if name:
                    logger.info(f"Found CEO via web search for {company_name}: {name}")
                    return (name, linkedin_url)

        return (None, None)

    async def enrich_with_context(self, context: DealContext) -> BraveEnrichmentResult:
        """Enrich company using full deal context for accuracy."""
        result = BraveEnrichmentResult(company_name=context.company_name)

        # Skip invalid names
        if not context.company_name or len(context.company_name) < 2:
            return result

        invalid_names = {"<unknown>", "unknown", "n/a", "no funding deal", "no specific funding deal"}
        if context.company_name.lower() in invalid_names:
            return result

        # Find website with investor context
        result.website = await self.find_website_with_context(
            context.company_name,
            investor=context.lead_investor,
            category=context.enterprise_category,
        )
        await asyncio.sleep(self.rate_limit_delay)

        # SKIP company LinkedIn - only founder LinkedIn matters for this use case
        # Company LinkedIn pages don't provide actionable contact info
        # result.company_linkedin = await self.find_company_linkedin(context.company_name)

        # Find LinkedIn for each known founder
        if context.founders:
            for founder in context.founders:
                founder_name = founder.get("name", "")
                if not founder_name or founder_name.lower() in ("ceo", "founder", "unknown"):
                    continue

                # Skip if already has LinkedIn
                if founder.get("linkedin_url"):
                    result.founder_linkedins[founder_name] = founder["linkedin_url"]
                    if not result.ceo_linkedin:
                        result.ceo_name = founder_name
                        result.ceo_linkedin = founder["linkedin_url"]
                    continue

                linkedin = await self.find_founder_linkedin(founder_name, context.company_name)
                if linkedin:
                    # Validate the LinkedIn URL before storing
                    validation = await validate_founder_linkedin(
                        founder_name=founder_name,
                        company_name=context.company_name,
                        linkedin_url=linkedin,
                        extracted_title=founder.get("title"),
                    )

                    # FIXED (2026-01): Only store if validation PASSED - no fallback
                    # Previously stored URLs when validation.profile_current_company was None,
                    # which allowed invalid URLs when company info couldn't be extracted
                    if validation.is_valid:
                        result.founder_linkedins[founder_name] = linkedin
                        if not result.ceo_linkedin:
                            result.ceo_name = founder_name
                            result.ceo_linkedin = linkedin
                        # FIX (2026-01): Cache success ONLY after validation passes
                        # This fixes cache poisoning from fallback searches
                        await _mark_linkedin_success(founder_name, context.company_name, linkedin)
                        logger.info(f"LinkedIn VALIDATED for {founder_name}: {linkedin}")
                    else:
                        reason = "wrong company" if validation.profile_current_company else "could not validate"
                        logger.warning(
                            f"LinkedIn REJECTED for {founder_name} at {context.company_name}: {reason} "
                            f"(profile shows '{validation.profile_current_company}' with title '{validation.profile_current_title}')"
                        )
                        # FIX (2026-01): Mark as failure to avoid re-searching invalid URLs
                        await _mark_linkedin_failure(founder_name, context.company_name)

                await asyncio.sleep(self.rate_limit_delay)

        # FIX: CEO fallback search when:
        # 1. NO founders were extracted (Claude failed to extract)
        # 2. OR founders exist but ALL have no LinkedIn (enrichment failed for all)
        # This ensures we always try to get at least one LinkedIn URL
        founders_all_missing_linkedin = (
            context.founders and
            all(not f.get("linkedin_url") for f in context.founders) and
            not result.founder_linkedins  # Enrichment also found nothing
        )

        if (not context.founders or founders_all_missing_linkedin) and not result.ceo_linkedin:
            reason = "No founders extracted" if not context.founders else "All founders missing LinkedIn"
            logger.info(f"{reason} for {context.company_name}, trying CEO fallback search")

            # Try primary LinkedIn search first
            ceo_name, linkedin_url = await self.find_ceo_linkedin(context.company_name)

            # IMPROVED (Jan 2026): If primary fails, try secondary web search
            if not linkedin_url or not ceo_name:
                logger.info(f"Primary CEO search failed for {context.company_name}, trying secondary web search")
                await asyncio.sleep(self.rate_limit_delay)
                ceo_name_web, linkedin_url_web = await self.find_ceo_via_web_search(context.company_name)
                if linkedin_url_web and ceo_name_web:
                    ceo_name, linkedin_url = ceo_name_web, linkedin_url_web

            if linkedin_url and ceo_name:
                # FIX (Jan 2026): Cross-validate found CEO name against existing founders
                # to prevent name/LinkedIn mismatches in the output
                matching_founder = None
                if context.founders:
                    for founder in context.founders:
                        existing_name = founder.get("name", "")
                        if _names_match(ceo_name, existing_name):
                            matching_founder = existing_name
                            logger.info(f"CEO fallback '{ceo_name}' matches existing founder '{existing_name}'")
                            break

                    # If found CEO doesn't match any existing founder, log warning
                    if not matching_founder:
                        existing_names = [f.get("name", "") for f in context.founders if f.get("name")]
                        logger.warning(
                            f"CEO fallback found '{ceo_name}' but doesn't match existing founders {existing_names} "
                            f"for {context.company_name} - adding as new founder"
                        )

                # Validate CEO LinkedIn
                validation = await validate_founder_linkedin(
                    founder_name=ceo_name,
                    company_name=context.company_name,
                    linkedin_url=linkedin_url,
                )
                # FIXED (2026-01): Only store if validation PASSED - no fallback
                if validation.is_valid:
                    # Use matching founder name if found, otherwise use the found name
                    final_name = matching_founder if matching_founder else ceo_name
                    result.ceo_name = final_name
                    result.ceo_linkedin = linkedin_url
                    result.founder_linkedins[final_name] = linkedin_url
                    # FIX (2026-01): Cache success ONLY after validation passes
                    await _mark_linkedin_success(final_name, context.company_name, linkedin_url)
                    logger.info(f"Found CEO for {context.company_name} via fallback: {final_name}")
                else:
                    reason = "wrong company" if validation.profile_current_company else "could not validate"
                    logger.warning(f"CEO LinkedIn rejected for {context.company_name}: {reason}")
                    # FIX (2026-01): Mark as failure to avoid re-searching invalid URLs
                    await _mark_linkedin_failure(ceo_name, context.company_name)
            await asyncio.sleep(self.rate_limit_delay)

        return result

    async def enrich(self, company_name: str) -> BraveEnrichmentResult:
        """Enrich company with website and CEO LinkedIn (legacy method)."""
        result = BraveEnrichmentResult(company_name=company_name)

        if not company_name or len(company_name) < 2:
            return result

        invalid_names = {"<unknown>", "unknown", "n/a", "no funding deal", "no specific funding deal"}
        if company_name.lower() in invalid_names:
            return result

        # Find website (without context - less accurate)
        result.website = await self.find_website_with_context(company_name)
        await asyncio.sleep(self.rate_limit_delay)

        # SKIP company LinkedIn - only founder LinkedIn matters
        # result.company_linkedin = await self.find_company_linkedin(company_name)

        # Find CEO LinkedIn
        ceo_name, linkedin_url = await self.find_ceo_linkedin(company_name)
        result.ceo_name = ceo_name
        result.ceo_linkedin = linkedin_url

        return result


async def enrich_company(company_name: str) -> BraveEnrichmentResult:
    """Enrich a single company with website and CEO LinkedIn."""
    async with BraveEnrichmentClient() as client:
        return await client.enrich(company_name)


async def enrich_company_with_context(context: DealContext) -> BraveEnrichmentResult:
    """Enrich a company using full deal context for better accuracy."""
    async with BraveEnrichmentClient() as client:
        return await client.enrich_with_context(context)


async def enrich_companies_batch(
    company_names: List[str],
    delay_seconds: float = 0.5,
    max_concurrent: int = 5,
) -> Dict[str, BraveEnrichmentResult]:
    """Enrich multiple companies with rate limiting.

    FIX #33: Use semaphore + burst pattern for parallel enrichment.
    """
    results: Dict[str, BraveEnrichmentResult] = {}
    semaphore = asyncio.Semaphore(max_concurrent)

    async def enrich_with_limit(name: str) -> tuple[str, BraveEnrichmentResult]:
        async with semaphore:
            try:
                async with BraveEnrichmentClient() as client:
                    result = await client.enrich(name)
            except Exception as e:
                logger.error(f"Error enriching {name}: {e}")
                result = BraveEnrichmentResult(company_name=name)
            finally:
                # FIX: Rate limit delay INSIDE semaphore to ensure proper spacing
                # between API calls (previous location caused all requests to fire at once)
                await asyncio.sleep(delay_seconds)
        return (name, result)

    # Run all enrichments in parallel (limited by semaphore)
    tasks = [enrich_with_limit(name) for name in company_names]
    completed = await asyncio.gather(*tasks, return_exceptions=True)

    for item in completed:
        if isinstance(item, Exception):
            logger.error(f"Batch enrichment error: {item}")
            continue
        name, result = item
        results[name] = result

    return results


async def persist_deal_linkedin_enrichment(
    deal_id: int,
    founders_with_linkedin: List[Dict],
) -> bool:
    """
    Persist LinkedIn enrichment results to a deal's founders_json.

    FIXED (2026-01): Now MERGES with existing founders instead of overwriting.
    This prevents data loss when enrichment returns a shorter list than original.

    Args:
        deal_id: ID of the deal to update
        founders_with_linkedin: Updated list of founder dicts with linkedin_url fields

    Returns:
        True if update succeeded, False otherwise
    """
    from ..archivist.database import get_session
    from ..archivist.models import Deal

    try:
        async with get_session() as session:
            deal = await session.get(Deal, deal_id)
            if not deal:
                return False

            # MERGE instead of overwrite - preserves existing founder data
            existing_founders = json.loads(deal.founders_json or "[]")
            existing_by_name = {
                f.get("name", "").lower(): f
                for f in existing_founders
                if f.get("name")
            }

            for founder in founders_with_linkedin:
                name_key = founder.get("name", "").lower()
                if not name_key:
                    continue

                if name_key in existing_by_name:
                    # Update existing: preserve old fields, add new non-empty values
                    existing_by_name[name_key].update({
                        k: v for k, v in founder.items() if v  # Only update non-empty values
                    })
                else:
                    # New founder from enrichment - add to list
                    existing_by_name[name_key] = founder

            deal.founders_json = json.dumps(list(existing_by_name.values()))
            await session.commit()
            logger.info(f"Updated LinkedIn for deal {deal_id} (merged {len(founders_with_linkedin)} enriched founders)")
            return True
    except Exception as e:
        logger.error(f"Error persisting LinkedIn for deal {deal_id}: {e}")
        return False


# Leadership titles that indicate CEO/founder roles
LEADERSHIP_TITLES = {
    "ceo", "chief executive", "founder", "co-founder", "cofounder",
    "president", "cto", "chief technology", "coo", "chief operating",
    "managing director", "general partner", "owner", "principal",
}

@dataclass
class LinkedInValidationResult:
    """Result of LinkedIn profile validation."""
    founder_name: str
    linkedin_url: str
    profile_current_company: Optional[str] = None
    profile_current_title: Optional[str] = None
    company_match: bool = False  # Does profile show this company?
    title_is_leadership: bool = False  # Is title CEO/founder/CTO?
    validation_method: str = "brave_search"

    @property
    def is_valid(self) -> bool:
        """Returns True if the LinkedIn profile validates as a leader at this company."""
        return self.company_match and self.title_is_leadership


def _is_leadership_title(title: str) -> bool:
    """Check if a title indicates a leadership role."""
    title_lower = title.lower()
    # Check for leadership patterns
    for leader in LEADERSHIP_TITLES:
        if leader in title_lower:
            return True
    return False


def _company_names_match(profile_company: str, expected_company: str) -> bool:
    """Check if two company names refer to the same company."""
    if not profile_company or not expected_company:
        return False

    # Normalize both names
    def normalize(name: str) -> str:
        name = name.lower()
        # Remove common suffixes
        name = re.sub(r'\s*(inc\.?|llc|corp\.?|ltd\.?|co\.?|lp)$', '', name, flags=re.I)
        # Remove "AI" suffix
        name = re.sub(r'\s*ai$', '', name, flags=re.I)
        # Remove punctuation and extra spaces
        name = re.sub(r'[^\w\s]', '', name)
        name = ' '.join(name.split())
        return name.strip()

    norm_profile = normalize(profile_company)
    norm_expected = normalize(expected_company)

    # Exact match
    if norm_profile == norm_expected:
        return True

    # One contains the other (handles "Bedrock" vs "Bedrock Data")
    if norm_profile in norm_expected or norm_expected in norm_profile:
        return True

    # Levenshtein-like similarity for typos
    ratio = SequenceMatcher(None, norm_profile, norm_expected).ratio()
    return ratio > 0.8


async def validate_founder_linkedin(
    founder_name: str,
    company_name: str,
    linkedin_url: str,
    extracted_title: Optional[str] = None,
) -> LinkedInValidationResult:
    """
    Validate that a LinkedIn profile matches the expected founder at a company.

    Uses Brave Search to fetch LinkedIn profile data (without requiring LinkedIn API).

    Args:
        founder_name: Expected founder's name
        company_name: Expected company name
        linkedin_url: LinkedIn profile URL to validate
        extracted_title: Title extracted from article (for comparison)

    Returns:
        LinkedInValidationResult with validation details
    """
    result = LinkedInValidationResult(
        founder_name=founder_name,
        linkedin_url=linkedin_url,
    )

    if not linkedin_url or "linkedin.com/in/" not in linkedin_url:
        return result

    # Extract profile path for search
    match = re.search(r'linkedin\.com/in/([a-zA-Z0-9_-]+)', linkedin_url)
    if not match:
        return result

    profile_path = match.group(1)

    # Search Brave for this specific LinkedIn profile
    client = get_brave_client()
    if not client.validate_api_key():
        logger.warning("BRAVE_SEARCH_KEY not configured for validation")
        return result

    try:
        query = f'site:linkedin.com/in/{profile_path}'
        data = await asyncio.wait_for(
            client.search_web(query, count=3),
            timeout=settings.enrichment_timeout,
        )

        if data is None:
            return result

        results = data.get("web", {}).get("results", [])

        for search_result in results:
            url = search_result.get("url", "")
            title = search_result.get("title", "")
            description = search_result.get("description", "")

            # Verify this is the right profile
            if profile_path.lower() not in url.lower():
                continue

            # Extract current company and title from LinkedIn snippet
            # Format: "Name - Title at Company | LinkedIn"
            # Or: "Name | Title - Company | LinkedIn"
            full_text = f"{title} {description}"

            # Try to extract company from description
            # LinkedIn descriptions often format as: "Title at Company"
            # Allow lowercase company names too (some snippets have lowercase)
            company_match = re.search(r'(?:at|@)\s+([A-Za-z][A-Za-z0-9\s&]+)', full_text)
            if company_match:
                result.profile_current_company = company_match.group(1).strip()

            # Try to extract title
            # Look for patterns like "CEO at", "Founder,", "CTO |"
            title_match = re.search(
                r'\b(CEO|CTO|COO|CFO|Founder|Co-Founder|President|Chief\s+\w+\s+Officer|Managing\s+Director|VP\s+of|Vice\s+President|Director\s+of|Head\s+of)\b',
                full_text,
                re.I
            )
            if title_match:
                result.profile_current_title = title_match.group(0).strip()

            # Validate company match
            if result.profile_current_company:
                result.company_match = _company_names_match(
                    result.profile_current_company,
                    company_name
                )

            # Validate title
            if result.profile_current_title:
                result.title_is_leadership = _is_leadership_title(result.profile_current_title)
            elif extracted_title:
                # Fall back to extracted title if we couldn't find one
                result.title_is_leadership = _is_leadership_title(extracted_title)
                if result.title_is_leadership:
                    result.profile_current_title = extracted_title

            # If we found data, break
            if result.profile_current_company or result.profile_current_title:
                break

        if result.is_valid:
            logger.info(
                f"LinkedIn validation PASSED for {founder_name}: "
                f"company={result.profile_current_company}, title={result.profile_current_title}"
            )
        elif result.company_match and not result.title_is_leadership:
            logger.warning(
                f"LinkedIn validation FAILED (non-leadership): {founder_name} at {company_name} "
                f"has title '{result.profile_current_title}'"
            )
        elif not result.company_match:
            logger.warning(
                f"LinkedIn validation FAILED (wrong company): {founder_name} expected at {company_name} "
                f"but profile shows '{result.profile_current_company}'"
            )

    except asyncio.TimeoutError:
        logger.warning(f"LinkedIn validation timeout for {linkedin_url}")
    except Exception as e:
        logger.error(f"LinkedIn validation error for {linkedin_url}: {e}")

    return result


async def validate_all_founders(
    company_name: str,
    founders: List[Dict],
) -> List[LinkedInValidationResult]:
    """
    Validate LinkedIn profiles for all founders.

    Args:
        company_name: Company name for validation
        founders: List of founder dicts with name, title, linkedin_url

    Returns:
        List of validation results
    """
    results = []

    for founder in founders:
        linkedin_url = founder.get("linkedin_url")
        if not linkedin_url:
            continue

        result = await validate_founder_linkedin(
            founder_name=founder.get("name", ""),
            company_name=company_name,
            linkedin_url=linkedin_url,
            extracted_title=founder.get("title"),
        )
        results.append(result)

        # Rate limiting
        await asyncio.sleep(0.5)

    return results


async def enrich_deal_founders_linkedin(
    deal_id: int,
    company_name: str,
    founders: List[Dict],
    persist: bool = True,
) -> List[Dict]:
    """
    Enrich founders with LinkedIn URLs and optionally persist to database.

    Args:
        deal_id: ID of the deal
        company_name: Name of the company
        founders: List of founder dicts [{"name": "...", "title": "...", "linkedin_url": None}]
        persist: If True, save results to database

    Returns:
        Updated list of founder dicts with linkedin_url fields populated
    """
    if not founders:
        return []

    updated_founders = []
    enriched_count = 0

    async with BraveEnrichmentClient() as client:
        for founder in founders:
            founder_copy = dict(founder)

            # Skip if already has LinkedIn
            if founder.get("linkedin_url"):
                updated_founders.append(founder_copy)
                continue

            founder_name = founder.get("name", "")
            if not founder_name or founder_name.lower() in ("ceo", "founder", "unknown"):
                updated_founders.append(founder_copy)
                continue

            # Find LinkedIn
            linkedin_url = await client.find_founder_linkedin(founder_name, company_name)
            if linkedin_url:
                founder_copy["linkedin_url"] = linkedin_url
                enriched_count += 1
                logger.info(f"Found LinkedIn for {founder_name} at {company_name}: {linkedin_url}")

            updated_founders.append(founder_copy)
            await asyncio.sleep(client.rate_limit_delay)

    # Persist results
    if persist and enriched_count > 0:
        await persist_deal_linkedin_enrichment(deal_id, updated_founders)

    return updated_founders


async def persist_founder_validation(
    deal_id: int,
    validation: LinkedInValidationResult,
) -> bool:
    """
    Persist a founder validation result to the database.

    Args:
        deal_id: ID of the deal
        validation: LinkedInValidationResult from validation

    Returns:
        True if persisted successfully, False otherwise
    """
    from datetime import datetime, timezone
    from ..archivist.database import get_session
    from ..archivist.models import FounderValidation

    try:
        async with get_session() as session:
            founder_val = FounderValidation(
                deal_id=deal_id,
                founder_name=validation.founder_name,
                linkedin_url=validation.linkedin_url,
                linkedin_current_company=validation.profile_current_company,
                linkedin_current_title=validation.profile_current_title,
                is_match=validation.company_match,
                title_is_leadership=validation.title_is_leadership,
                validated_at=datetime.now(timezone.utc),
                validation_method=validation.validation_method,
            )
            session.add(founder_val)
            await session.commit()
            logger.info(f"Persisted founder validation for deal {deal_id}: {validation.founder_name}")
            return True
    except Exception as e:
        logger.error(f"Error persisting founder validation for deal {deal_id}: {e}")
        return False


async def persist_all_founder_validations(
    deal_id: int,
    validations: List[LinkedInValidationResult],
    update_deal: bool = True,
) -> int:
    """
    Persist all founder validation results and optionally update the deal's founders_validated flag.

    Args:
        deal_id: ID of the deal
        validations: List of LinkedInValidationResult objects
        update_deal: If True, update deal.founders_validated based on results

    Returns:
        Number of validations persisted
    """
    from ..archivist.database import get_session
    from ..archivist.models import Deal

    persisted = 0
    for validation in validations:
        if await persist_founder_validation(deal_id, validation):
            persisted += 1

    # Update deal.founders_validated if all validations passed
    if update_deal and validations:
        all_valid = all(v.is_valid for v in validations)
        try:
            async with get_session() as session:
                deal = await session.get(Deal, deal_id)
                if deal:
                    deal.founders_validated = all_valid
                    await session.commit()
                    logger.info(f"Updated founders_validated for deal {deal_id}: {all_valid}")
        except Exception as e:
            logger.error(f"Error updating founders_validated for deal {deal_id}: {e}")

    return persisted
