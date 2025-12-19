"""
Common utilities and shared modules.
"""

from .brave_client import (
    BraveClient,
    BraveAPIError,
    get_brave_client,
    close_brave_client,
    BRAVE_NEWS_API,
    BRAVE_WEB_API,
)

from .http_client import (
    create_scraper_client,
    create_article_client,
    USER_AGENT_BOT,
    USER_AGENT_BOT_WITH_CONTACT,
    USER_AGENT_BROWSER,
)

__all__ = [
    # Brave client
    "BraveClient",
    "BraveAPIError",
    "get_brave_client",
    "close_brave_client",
    "BRAVE_NEWS_API",
    "BRAVE_WEB_API",
    # HTTP client utilities
    "create_scraper_client",
    "create_article_client",
    "USER_AGENT_BOT",
    "USER_AGENT_BOT_WITH_CONTACT",
    "USER_AGENT_BROWSER",
]
