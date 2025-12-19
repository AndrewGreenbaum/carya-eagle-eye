"""
Shared HTTP Client Configuration.

Provides standardized HTTP client creation and User-Agent strings for all scrapers.
This ensures consistent behavior, easier maintenance, and proper identification.

Usage:
    from src.common.http_client import create_scraper_client, USER_AGENT_BOT, USER_AGENT_BROWSER

    # For API endpoints and friendly sites
    client = create_scraper_client(user_agent=USER_AGENT_BOT)

    # For web scraping that needs browser-like headers
    client = create_scraper_client(user_agent=USER_AGENT_BROWSER)
"""

import httpx
from typing import Optional

from ..config.settings import settings


# =============================================================================
# User-Agent Constants
# =============================================================================

# Bot identifier - Use for APIs and sites that allow bots
# Identifies our scraper clearly for transparency
USER_AGENT_BOT = "BudTracker/1.0 (Investment Research Bot)"

# Bot identifier with contact info - Required by SEC and some APIs
USER_AGENT_BOT_WITH_CONTACT = "BudTracker/1.0 (contact@budtracker.io; Investment Research)"

# Browser-like User-Agent - Use for web scraping that requires browser headers
# Based on Chrome 120 on macOS
USER_AGENT_BROWSER = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# =============================================================================
# HTTP Client Factory
# =============================================================================

def create_scraper_client(
    user_agent: str = USER_AGENT_BOT,
    timeout: Optional[float] = None,
    max_connections: int = 20,
    max_keepalive: int = 10,
    follow_redirects: bool = True,
    extra_headers: Optional[dict] = None,
) -> httpx.AsyncClient:
    """
    Create a standardized async HTTP client for scraping.

    Args:
        user_agent: User-Agent string (use constants above)
        timeout: Request timeout in seconds (default: settings.request_timeout)
        max_connections: Maximum concurrent connections
        max_keepalive: Maximum keepalive connections
        follow_redirects: Whether to follow HTTP redirects
        extra_headers: Additional headers to include

    Returns:
        Configured httpx.AsyncClient

    Example:
        async with create_scraper_client(user_agent=USER_AGENT_BROWSER) as client:
            response = await client.get(url)
    """
    headers = {"User-Agent": user_agent}
    if extra_headers:
        headers.update(extra_headers)

    return httpx.AsyncClient(
        timeout=timeout or settings.request_timeout,
        headers=headers,
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive,
        ),
        follow_redirects=follow_redirects,
    )


def create_article_client(
    user_agent: str = USER_AGENT_BROWSER,
    timeout: Optional[float] = None,
) -> httpx.AsyncClient:
    """
    Create a client optimized for fetching individual articles.

    Uses browser-like headers and shorter timeout by default.

    Args:
        user_agent: User-Agent string (default: browser-like)
        timeout: Request timeout (default: settings.article_fetch_timeout)

    Returns:
        Configured httpx.AsyncClient for article fetching
    """
    return create_scraper_client(
        user_agent=user_agent,
        timeout=timeout or settings.article_fetch_timeout,
        max_connections=20,
        max_keepalive=10,
    )
