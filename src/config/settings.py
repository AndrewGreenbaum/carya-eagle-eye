"""
Application settings loaded from environment variables.
"""

import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class Settings(BaseSettings):
    """Application configuration from environment."""

    # API Keys
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    brave_search_key: str = ""  # Brave Search API for news discovery
    firecrawl_api_key: str = ""  # Firecrawl for JS-heavy PR sites
    twitter_bearer_token: str = ""  # Twitter API v2 Bearer Token
    google_alerts_feeds: str = ""  # Comma-separated Google Alerts RSS feed URLs
    api_keys: str = "dev-key"  # Comma-separated valid API keys for authentication

    # Y Combinator Algolia credentials (public, from YC website)
    yc_algolia_app_id: str = "45BWZJ1SGC"
    yc_algolia_api_key: str = "MjBjYjRiMzY0NzdhZWY0NjExY2NhZjYxMGIxYjc2MTAwNWFkNTkwNTc4NjgxYjU0YzFhYTY2ZGQ5OGY5NDMxZnJlc3RyaWN0SW5kaWNlcz0lNUIlMjJZQ0NvbXBhbnlfcHJvZHVjdGlvbiUyMiUyQyUyMllDQ29tcGFueV9CeV9MYXVuY2hfRGF0ZV9wcm9kdWN0aW9uJTIyJTVEJnRhZ0ZpbHRlcnM9JTVCJTIyeWNkY19wdWJsaWMlMjIlNUQmYW5hbHl0aWNzVGFncz0lNUIlMjJ5Y2RjJTIyJTVE"

    @property
    def valid_api_keys(self) -> list[str]:
        """Get list of valid API keys."""
        return [k.strip() for k in self.api_keys.split(",") if k.strip()]

    # Frontend URL for CORS (production)
    frontend_url: str = ""  # e.g., https://your-frontend.railway.app

    # Database - Railway provides DATABASE_URL
    database_url: str = "postgresql+asyncpg://bud:tracker@localhost:5432/budtracker"

    @field_validator("database_url", mode="before")
    @classmethod
    def convert_database_url(cls, v: str) -> str:
        """Convert standard postgres:// URL to asyncpg format."""
        # Railway uses postgres:// or postgresql://, we need postgresql+asyncpg://
        if v.startswith("postgres://"):
            v = v.replace("postgres://", "postgresql+asyncpg://", 1)
        elif v.startswith("postgresql://") and "+asyncpg" not in v:
            v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    # LLM Settings
    llm_model: str = "claude-3-5-haiku-20241022"
    llm_model_fallback: str = "claude-sonnet-4-20250514"  # Higher-quality model for re-extraction
    llm_temperature: float = 0.0
    llm_max_tokens: int = 4096
    llm_timeout: int = 90  # Total timeout for Claude API calls (seconds)
    llm_connect_timeout: int = 30  # Connection timeout for Claude API (seconds)
    llm_max_retries: int = 3  # Max retries on transient errors
    extraction_confidence_threshold: float = 0.5  # Minimum confidence to keep extraction

    # Hybrid extraction: re-extract with Sonnet for low-confidence results
    hybrid_extraction_enabled: bool = True  # Enable Sonnet re-extraction
    # FIX (2026-01): Different thresholds for internal vs external sources
    # Internal sources (full articles) have more context, so narrow range is fine
    # External sources (headlines) need wider range since less context available
    hybrid_confidence_min: float = 0.45  # Min confidence for internal sources
    hybrid_confidence_min_external: float = 0.35  # Min confidence for external sources
    hybrid_confidence_max: float = 0.65  # Max confidence to consider for re-extraction

    # Embedding Settings
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536

    # Scraping Settings - Main HTTP client
    request_timeout: int = 30  # Main scraper timeout (seconds)
    article_fetch_timeout: int = 15  # Individual article fetch timeout (seconds)
    rate_limit_delay: float = 1.0  # Default delay between requests
    max_retries: int = 3  # Default retry attempts

    # Scraping Settings - Rate Limits (seconds)
    article_rate_limit_delay: float = 0.3  # Delay between article fetches
    feed_rate_limit_delay: float = 0.2  # Delay between feed fetches
    api_rate_limit_delay: float = 0.5  # Delay for API-heavy operations

    # Scraping Settings - Concurrency Limits
    max_concurrent_articles: int = 5  # Parallel article fetches
    max_concurrent_feeds: int = 5  # Parallel feed fetches
    max_concurrent_searches: int = 3  # Parallel search queries

    # Twitter Settings
    twitter_requests_per_run: int = 100  # Max API calls per scrape run (free tier: 1500/month)

    # Brave Search Settings
    brave_search_timeout: int = 30
    brave_search_max_retries: int = 3
    brave_search_rate_limit_delay: float = 0.3
    brave_search_backoff_base: float = 2.0  # Exponential backoff base

    # Deduplication
    similarity_threshold: float = 0.95
    dedup_window_days: int = 7

    # Concurrency Limits (configurable instead of hardcoded)
    max_concurrent_extractions: int = 5  # Claude API calls in parallel
    max_parallel_funds: int = 3  # Fund scrapers in parallel
    max_concurrent_brave_searches: int = 3  # Brave API calls in parallel
    enrichment_timeout: float = 30.0  # Timeout for enrichment API calls

    # Scheduler Settings
    # Options: "daily" (1x at 9am), "3x_daily" (9am, 1pm, 6pm), "4_hourly" (every 4 hours)
    scan_frequency: str = "daily"

    # Webhooks (optional)
    slack_webhook_url: str = ""
    discord_webhook_url: str = ""

    # Google Sheets (for feedback/flagging)
    google_sheets_credentials: str = ""  # JSON string of service account credentials
    google_sheets_id: str = ""  # Spreadsheet ID from the URL

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Create settings instance at import time (singleton pattern)
# All code should import: from ..config.settings import settings
settings = Settings()
