"""
Shared URL validation and sanitization utilities.

FIX #1: Consolidates duplicate URL validation logic from:
- main.py (API validation)
- storage.py (database persistence)
- schemas.py (LLM extraction)

All modules should import from here for consistency.
"""

import re
from typing import Optional

# Invalid placeholder values that LLMs sometimes generate
INVALID_URL_PLACEHOLDERS = {
    "not mentioned", "not specified", "unknown", "n/a", "none", "",
    "<unknown>", "$unknown", "null", "undefined", "n.a.", "na",
    "not available", "not provided", "tbd", "tba", "pending",
    "no website", "no url", "unavailable", "not found",
}

# Patterns that indicate placeholder text (for substring matching)
PLACEHOLDER_PATTERNS = ["not mentioned", "not specified", "unknown", "n/a", "unavailable"]

# Domains that should be rejected as company websites
INVALID_WEBSITE_DOMAINS = {
    # Social media
    'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
    'youtube.com', 'tiktok.com', 'x.com',
    # Content platforms
    'medium.com', 'substack.com', 'blogspot.com', 'wordpress.com',
    # Code hosting (not company websites)
    'github.com', 'gitlab.com', 'bitbucket.org',
    # Data/news sites
    'crunchbase.com', 'pitchbook.com', 'techcrunch.com', 'bloomberg.com',
    'reuters.com', 'forbes.com', 'wsj.com', 'nytimes.com',
    # Search URLs
    'google.com/search', 'linkedin.com/search', 'bing.com/search',
}

# LinkedIn profile URL pattern (require 3+ char username)
LINKEDIN_PROFILE_PATTERN = re.compile(
    r'^https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]{3,}(?:/.*)?(?:\?.*)?$'
)

# LinkedIn company page URL pattern (require 2+ char slug)
LINKEDIN_COMPANY_PATTERN = re.compile(
    r'^https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9_-]{2,}(?:/.*)?(?:\?.*)?$'
)

# Valid URL pattern (basic structure check)
VALID_URL_PATTERN = re.compile(
    r'^https?://[a-zA-Z0-9][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/.*)?$'
)


def is_valid_url(url: Optional[str]) -> bool:
    """
    Check if URL is a real URL (not a placeholder or invalid).

    Args:
        url: URL string to validate

    Returns:
        True if URL is valid, False otherwise

    Examples:
        >>> is_valid_url("https://example.com")
        True
        >>> is_valid_url("Not mentioned")
        False
        >>> is_valid_url("n/a")
        False
        >>> is_valid_url("")
        False
    """
    if not url:
        return False

    url_lower = url.lower().strip()

    # Check exact placeholder matches
    if url_lower in INVALID_URL_PLACEHOLDERS:
        return False

    # Check for placeholder patterns (handles variations)
    if any(p in url_lower for p in PLACEHOLDER_PATTERNS):
        return False

    # Must start with http or www
    if url_lower.startswith("www."):
        return True  # Will be normalized to https://
    if not url_lower.startswith("http"):
        return False

    return True


def is_valid_website_url(url: Optional[str]) -> bool:
    """
    Check if URL is valid for a company website.

    More strict than is_valid_url() - also rejects social media,
    news sites, and search URLs.

    Args:
        url: URL string to validate

    Returns:
        True if valid company website, False otherwise
    """
    if not is_valid_url(url):
        return False

    url_lower = url.lower()

    # Check for invalid domains
    for domain in INVALID_WEBSITE_DOMAINS:
        if domain in url_lower:
            return False

    # Must match basic URL pattern
    if not VALID_URL_PATTERN.match(url):
        # Handle www. prefix
        if url_lower.startswith("www."):
            return VALID_URL_PATTERN.match("https://" + url) is not None
        return False

    return True


def is_valid_linkedin_profile(url: Optional[str]) -> bool:
    """
    Check if URL is a valid LinkedIn profile URL.

    Requires:
    - linkedin.com/in/ path
    - Username at least 3 characters

    Rejects:
    - /company/, /jobs/, /posts/ etc.
    - Short usernames (< 3 chars)

    Args:
        url: URL string to validate

    Returns:
        True if valid LinkedIn profile, False otherwise
    """
    if not url:
        return False

    return LINKEDIN_PROFILE_PATTERN.match(url) is not None


def is_valid_linkedin_company(url: Optional[str]) -> bool:
    """
    Check if URL is a valid LinkedIn company page URL.

    Requires:
    - linkedin.com/company/ path
    - Slug at least 2 characters

    Args:
        url: URL string to validate

    Returns:
        True if valid LinkedIn company page, False otherwise
    """
    if not url:
        return False

    return LINKEDIN_COMPANY_PATTERN.match(url) is not None


def sanitize_url(url: Optional[str]) -> Optional[str]:
    """
    Sanitize URL - return None if invalid, normalized URL otherwise.

    Normalizations applied:
    - Strip whitespace
    - Add https:// if starts with www.
    - Normalize http:// to https://

    Args:
        url: URL string to sanitize

    Returns:
        Normalized URL string or None if invalid

    Examples:
        >>> sanitize_url("www.example.com")
        "https://www.example.com"
        >>> sanitize_url("Not mentioned")
        None
        >>> sanitize_url("http://example.com")
        "https://example.com"
    """
    if not is_valid_url(url):
        return None

    url = url.strip()

    # Add https:// if starts with www.
    if url.lower().startswith("www."):
        url = "https://" + url

    # Normalize http:// to https://
    if url.lower().startswith("http://"):
        url = "https://" + url[7:]

    return url


def sanitize_linkedin_url(url: Optional[str]) -> Optional[str]:
    """
    Sanitize LinkedIn URL - validates and normalizes.

    Args:
        url: LinkedIn URL to sanitize

    Returns:
        Normalized LinkedIn URL or None if invalid
    """
    if not url:
        return None

    # First check if it's a placeholder
    if not is_valid_url(url):
        return None

    url = url.strip()

    # Must be a LinkedIn URL
    if 'linkedin.com' not in url.lower():
        return None

    # Normalize http:// to https://
    if url.lower().startswith("http://"):
        url = "https://" + url[7:]

    # Add https:// if missing
    if url.lower().startswith("www."):
        url = "https://" + url

    return url
