"""
Playwright-based URL Resolver - Follows JavaScript redirects.

Google News URLs use JavaScript redirects that can't be followed by httpx.
This module provides a Playwright-based resolver for batch URL resolution.

USAGE:
    async with PlaywrightResolver() as resolver:
        real_urls = await resolver.resolve_batch(google_news_urls, max_concurrent=5)
"""

import asyncio
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Try to import playwright, but don't fail if not installed
try:
    from playwright.async_api import async_playwright, Browser, Page, Playwright, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright not installed - PlaywrightResolver will be disabled")


class PlaywrightResolver:
    """
    Resolves Google News URLs by following JavaScript redirects.

    Google News wraps article URLs in their own domain with JS redirects.
    This class uses headless Chromium to capture the final destination URL.

    Optimizations:
    - Batch processing with page pool
    - Early termination on redirect detection
    - Timeout handling for slow sites
    """

    POOL_SIZE = 3
    RESOLVE_TIMEOUT_MS = 8000  # 8 seconds max per URL

    def __init__(self):
        self._playwright: Optional['Playwright'] = None
        self._browser: Optional['Browser'] = None
        self._context: Optional['BrowserContext'] = None
        self._page_pool: asyncio.Queue = asyncio.Queue(maxsize=self.POOL_SIZE)
        self._all_pages: set = set()

    @property
    def available(self) -> bool:
        """Check if Playwright is available."""
        return PLAYWRIGHT_AVAILABLE

    async def __aenter__(self):
        if not PLAYWRIGHT_AVAILABLE:
            return self

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-gpu',
                '--disable-extensions',
            ]
        )
        self._context = await self._browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            java_script_enabled=True,
        )

        # Pre-create page pool
        for _ in range(self.POOL_SIZE):
            page = await self._context.new_page()
            self._all_pages.add(page)
            await self._page_pool.put(page)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not PLAYWRIGHT_AVAILABLE:
            return

        # Clean up all pages
        for page in list(self._all_pages):
            try:
                await page.close()
            except Exception:
                pass
        self._all_pages.clear()

        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _get_page(self) -> Optional['Page']:
        """Get a page from the pool or create overflow page."""
        if not PLAYWRIGHT_AVAILABLE:
            return None

        try:
            return self._page_pool.get_nowait()
        except asyncio.QueueEmpty:
            # Create overflow page if pool exhausted
            if self._context:
                page = await self._context.new_page()
                self._all_pages.add(page)
                return page
            return None

    async def _return_page(self, page: 'Page'):
        """Return a page to the pool."""
        if not PLAYWRIGHT_AVAILABLE or page is None:
            return

        try:
            # Clear page state for reuse
            await page.goto('about:blank', timeout=2000)
            self._page_pool.put_nowait(page)
        except Exception:
            # Page may be corrupted, remove from pool
            self._all_pages.discard(page)
            try:
                await page.close()
            except Exception:
                pass

    async def resolve_url(self, google_news_url: str) -> Optional[str]:
        """
        Resolve a single Google News URL to the actual article URL.

        Args:
            google_news_url: The Google News redirect URL

        Returns:
            The actual article URL, or None if resolution fails
        """
        if not PLAYWRIGHT_AVAILABLE:
            return None

        # Skip non-Google News URLs
        if 'news.google.com' not in google_news_url:
            return google_news_url

        page = await self._get_page()
        if not page:
            return None

        final_url = None
        try:
            # Navigate and wait for redirect
            response = await page.goto(
                google_news_url,
                timeout=self.RESOLVE_TIMEOUT_MS,
                wait_until='domcontentloaded'
            )

            # Get final URL after redirects
            final_url = page.url

            # Validate that we actually redirected
            if 'news.google.com' in final_url:
                # Still on Google News - JS redirect may not have fired
                # Wait a bit and check again
                await asyncio.sleep(1)
                final_url = page.url

                if 'news.google.com' in final_url:
                    # Still stuck - resolution failed
                    final_url = None

            # Validate URL looks legitimate
            if final_url:
                parsed = urlparse(final_url)
                if not parsed.scheme or not parsed.netloc:
                    final_url = None
                # Filter out error pages
                if any(x in final_url.lower() for x in ['error', '404', 'not-found']):
                    final_url = None

        except asyncio.TimeoutError:
            logger.debug(f"Timeout resolving {google_news_url}")
        except Exception as e:
            logger.debug(f"Error resolving {google_news_url}: {e}")
        finally:
            await self._return_page(page)

        return final_url

    async def resolve_batch(
        self,
        urls: List[str],
        max_concurrent: int = 3,
        delay_between: float = 0.2
    ) -> Dict[str, Optional[str]]:
        """
        Resolve multiple Google News URLs in parallel.

        Args:
            urls: List of Google News URLs to resolve
            max_concurrent: Maximum concurrent resolutions
            delay_between: Delay between starting new resolutions

        Returns:
            Dict mapping original URL to resolved URL (None if failed)
        """
        if not PLAYWRIGHT_AVAILABLE:
            return {url: None for url in urls}

        results: Dict[str, Optional[str]] = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def resolve_with_limit(url: str):
            async with semaphore:
                result = await self.resolve_url(url)
                results[url] = result
                if delay_between > 0:
                    await asyncio.sleep(delay_between)

        # Process in parallel
        tasks = [resolve_with_limit(url) for url in urls]
        await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for v in results.values() if v is not None)
        logger.info(f"Playwright resolved {success_count}/{len(urls)} URLs")

        return results


async def resolve_google_news_urls_batch(
    urls: List[str],
    max_concurrent: int = 3
) -> Dict[str, Optional[str]]:
    """
    Convenience function to resolve Google News URLs using Playwright.

    Args:
        urls: List of Google News URLs
        max_concurrent: Maximum concurrent resolutions

    Returns:
        Dict mapping original URL to resolved URL
    """
    async with PlaywrightResolver() as resolver:
        if not resolver.available:
            logger.warning("Playwright not available - returning empty results")
            return {url: None for url in urls}
        return await resolver.resolve_batch(urls, max_concurrent)
