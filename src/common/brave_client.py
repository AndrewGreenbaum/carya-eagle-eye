"""
Shared Brave Search API Client.

Provides a reusable HTTP client with:
- Exponential backoff retry logic
- Rate limit (HTTP 429) handling
- Parallel query execution with semaphore
- TTL caching for partner queries
- Configurable timeouts and delays

Used by:
- brave_search.py (news search)
- brave_enrichment.py (company enrichment)
"""

import asyncio
import hashlib
import logging
import random
from time import time
from typing import Optional, Dict, Any, List, Tuple

import httpx

from ..config.settings import settings

logger = logging.getLogger(__name__)

# Brave Search API endpoints
BRAVE_NEWS_API = "https://api.search.brave.com/res/v1/news/search"
BRAVE_WEB_API = "https://api.search.brave.com/res/v1/web/search"


class BraveAPIError(Exception):
    """Raised when Brave API returns an error."""
    pass


class TTLCache:
    """Simple TTL cache for query results with automatic cleanup.

    FIX: Added asyncio.Lock for thread-safety in concurrent async access.
    """

    # Cleanup every N set operations to prevent unbounded growth
    CLEANUP_INTERVAL = 100

    def __init__(self, ttl_seconds: int = 3600):
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self._ttl = ttl_seconds
        self._operations_since_cleanup = 0
        self._lock = asyncio.Lock()  # FIX: Async-safe cache access

    def _make_key(self, query: str, search_type: str, freshness: str, count: int = 20) -> str:
        """Create cache key from query parameters."""
        key_str = f"{search_type}:{freshness}:{count}:{query}"
        return hashlib.md5(key_str.encode()).hexdigest()

    async def get(self, query: str, search_type: str, freshness: str, count: int = 20) -> Optional[Any]:
        """Get cached result if not expired. Async-safe."""
        key = self._make_key(query, search_type, freshness, count)
        async with self._lock:
            if key in self._cache:
                timestamp, value = self._cache[key]
                if time() - timestamp < self._ttl:
                    return value
                del self._cache[key]
            return None

    async def set(self, query: str, search_type: str, freshness: str, value: Any, count: int = 20):
        """Store result in cache. Async-safe."""
        key = self._make_key(query, search_type, freshness, count)
        async with self._lock:
            self._cache[key] = (time(), value)

            # Periodic cleanup to prevent unbounded memory growth
            self._operations_since_cleanup += 1
            if self._operations_since_cleanup >= self.CLEANUP_INTERVAL:
                self._cleanup_locked()
                self._operations_since_cleanup = 0

    def clear(self):
        """Clear all cached entries."""
        self._cache.clear()

    def _cleanup_locked(self) -> int:
        """Remove expired entries (called while lock is held)."""
        now = time()
        expired_keys = [k for k, (ts, _) in self._cache.items() if now - ts >= self._ttl]
        for k in expired_keys:
            del self._cache[k]
        if expired_keys:
            logger.debug(f"Cache cleanup: removed {len(expired_keys)} expired entries, size={len(self._cache)}")
        return len(expired_keys)

    async def cleanup(self) -> int:
        """Remove expired entries from cache. Returns number of entries removed."""
        async with self._lock:
            return self._cleanup_locked()

    def size(self) -> int:
        """Return number of cached entries."""
        return len(self._cache)


# Singleton cache instance (4 hour TTL - matches job run frequency)
# FIX: Reduced from 12hr to 4hr - 12hr was too long for fresh news results
# 4hr aligns with job frequency while still providing caching benefits within a run
_query_cache = TTLCache(ttl_seconds=14400)


def get_query_cache() -> TTLCache:
    """Get the shared query cache."""
    return _query_cache


def clear_query_cache():
    """Clear the query cache."""
    _query_cache.clear()


class BraveClient:
    """
    Shared async HTTP client for Brave Search API.

    Features:
    - Exponential backoff retry on failures
    - HTTP 429 rate limit handling with Retry-After
    - Parallel query execution with semaphore
    - Optional TTL caching
    - Configurable via settings
    """

    def __init__(self):
        self.api_key = settings.brave_search_key
        self.timeout = settings.brave_search_timeout
        self.max_retries = settings.brave_search_max_retries
        self.rate_limit_delay = settings.brave_search_rate_limit_delay
        self.backoff_base = settings.brave_search_backoff_base
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={
                    "X-Subscription-Token": self.api_key,
                    "Accept": "application/json",
                },
                # FIX: Add connection limits to prevent pool exhaustion under load
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def validate_api_key(self) -> bool:
        """Check if API key is configured."""
        if not self.api_key:
            logger.error("BRAVE_SEARCH_KEY not configured")
            return False
        return True

    async def request(
        self,
        url: str,
        params: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with exponential backoff retry logic.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            JSON response data or None on failure

        Handles:
        - HTTP 429 (rate limit) with Retry-After header
        - HTTP 5xx (server errors) with exponential backoff
        - Timeouts with retry
        - Network errors with retry
        """
        if not self.validate_api_key():
            return None

        client = await self._get_client()
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = await client.get(url, params=params)

                # Handle rate limiting (429)
                if response.status_code == 429:
                    # FIX: Retry-After can be int OR HTTP-date string
                    retry_after_header = response.headers.get("Retry-After", "60")
                    try:
                        retry_after = int(retry_after_header)
                    except ValueError:
                        # HTTP-date format (e.g., "Wed, 21 Oct 2025 07:28:00 GMT")
                        logger.warning(f"Non-numeric Retry-After header: {retry_after_header}")
                        retry_after = 60
                    logger.warning(
                        f"Brave API rate limited. Waiting {retry_after}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(retry_after)
                    continue

                # Handle server errors with backoff
                if response.status_code >= 500:
                    backoff = (self.backoff_base ** attempt) * random.uniform(0.9, 1.1)
                    logger.warning(
                        f"Brave API server error {response.status_code}. "
                        f"Retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(backoff)
                    continue

                response.raise_for_status()

                # FIX: Handle JSONDecodeError from malformed response
                try:
                    return response.json()
                except ValueError as e:
                    logger.warning(f"Brave API JSON decode error: {e}")
                    return None

            except httpx.TimeoutException:
                backoff = (self.backoff_base ** attempt) * random.uniform(0.9, 1.1)
                logger.warning(
                    f"Brave API timeout. Retrying in {backoff:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                last_error = "timeout"
                await asyncio.sleep(backoff)

            except httpx.HTTPStatusError as e:
                # Client errors (4xx except 429) - don't retry
                if 400 <= e.response.status_code < 500:
                    logger.error(f"Brave API client error: {e.response.status_code}")
                    return None
                last_error = str(e)

            except httpx.RequestError as e:
                # Network errors - retry with backoff
                backoff = (self.backoff_base ** attempt) * random.uniform(0.9, 1.1)
                logger.warning(
                    f"Brave API network error: {e}. Retrying in {backoff:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                last_error = str(e)
                await asyncio.sleep(backoff)

        logger.error(f"Brave API request failed after {self.max_retries} attempts: {last_error}")
        return None

    async def search_news(
        self,
        query: str,
        count: int = 20,
        freshness: str = "pw",
        use_cache: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute news search on Brave API.

        Args:
            query: Search query string
            count: Number of results (max 100)
            freshness: pd=past day, pw=past week, pm=past month
            use_cache: If True, check/store in TTL cache

        Returns:
            Raw API response or None on failure
        """
        # Check cache first
        if use_cache:
            cached = await _query_cache.get(query, "news", freshness, count)
            if cached is not None:
                logger.debug(f"Cache hit for news query: {query[:50]}...")
                return cached

        params = {
            "q": query,
            "count": count,
            "freshness": freshness,
            "text_decorations": False,
            "safesearch": "off",
        }
        result = await self.request(BRAVE_NEWS_API, params)

        # Store in cache
        if use_cache and result is not None:
            await _query_cache.set(query, "news", freshness, result, count)

        return result

    async def search_web(
        self,
        query: str,
        count: int = 20,
        freshness: Optional[str] = None,
        use_cache: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute web search on Brave API.

        Args:
            query: Search query string
            count: Number of results (max 100)
            freshness: Optional - pd=past day, pw=past week, pm=past month
            use_cache: If True, check/store in TTL cache

        Returns:
            Raw API response or None on failure
        """
        cache_freshness = freshness or "none"

        # Check cache first
        if use_cache:
            cached = await _query_cache.get(query, "web", cache_freshness, count)
            if cached is not None:
                logger.debug(f"Cache hit for web query: {query[:50]}...")
                return cached

        params = {
            "q": query,
            "count": count,
            "text_decorations": False,
            "safesearch": "off",
        }
        if freshness:
            params["freshness"] = freshness

        result = await self.request(BRAVE_WEB_API, params)

        # Store in cache
        if use_cache and result is not None:
            await _query_cache.set(query, "web", cache_freshness, result, count)

        return result

    async def search_batch(
        self,
        queries: List[Tuple[str, str, int, str, bool]],
        max_concurrent: int = 3,
        delay_between: float = 0.3,
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Execute multiple queries with controlled parallelism.

        Args:
            queries: List of (query, search_type, count, freshness, use_cache) tuples
                     search_type: "news" or "web"
            max_concurrent: Maximum concurrent requests (default 3)
            delay_between: Minimum delay between request STARTS (default 0.3)

        Returns:
            Dict mapping query string to result (or None on failure)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results: Dict[str, Optional[Dict[str, Any]]] = {}
        # Track last request time for proper rate limiting
        last_request_time = [0.0]  # Use list to allow mutation in nested function
        rate_lock = asyncio.Lock()

        async def search_with_limit(
            query: str,
            search_type: str,
            count: int,
            freshness: str,
            use_cache: bool,
        ) -> Tuple[str, Optional[Dict[str, Any]]]:
            async with semaphore:
                # Enforce minimum delay between request STARTS (not completions)
                async with rate_lock:
                    now = time()
                    elapsed = now - last_request_time[0]
                    if elapsed < delay_between:
                        await asyncio.sleep(delay_between - elapsed)
                    last_request_time[0] = time()

                try:
                    if search_type == "news":
                        result = await self.search_news(query, count, freshness, use_cache)
                    else:
                        result = await self.search_web(query, count, freshness or None, use_cache)
                    return (query, result)
                except Exception as e:
                    # Return exception with query context for better error logging
                    logger.error(f"Batch query error for '{query[:50]}...': {e}")
                    return (query, None)

        tasks = [
            search_with_limit(q, t, c, f, cache)
            for q, t, c, f, cache in queries
        ]

        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in task_results:
            if isinstance(res, Exception):
                # This shouldn't happen now since we catch in search_with_limit
                logger.error(f"Unexpected batch error: {res}")
            else:
                query, result = res
                results[query] = result

        return results

    async def delay(self):
        """Wait for configured rate limit delay between requests."""
        await asyncio.sleep(self.rate_limit_delay)


# Singleton instance for shared use
_client: Optional[BraveClient] = None


def get_brave_client() -> BraveClient:
    """Get shared Brave client instance."""
    global _client
    if _client is None:
        _client = BraveClient()
    return _client


async def close_brave_client():
    """Close the shared client (call on shutdown)."""
    global _client
    if _client:
        await _client.close()
        _client = None
