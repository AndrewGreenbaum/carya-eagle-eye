"""
Playwright Scraper - Base class for JavaScript-heavy sites.

Uses headless Chromium to render pages that require JS execution.
Provides async context manager for browser lifecycle management.

OPTIMIZED: Page pooling for reuse, faster wait strategies.
"""

import asyncio
import logging
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, AsyncIterator
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright, Browser, Page, Playwright, BrowserContext
from bs4 import BeautifulSoup

from .base_scraper import RawArticle, NormalizedArticle

logger = logging.getLogger(__name__)
from ..config.funds import FundConfig
from ..config.settings import settings


class PlaywrightScraper:
    """
    Base class for scrapers requiring JavaScript rendering.

    Uses Playwright with headless Chromium to:
    - Wait for dynamic content to load
    - Handle infinite scroll
    - Extract rendered DOM

    OPTIMIZED:
    - Page pool for reuse (avoids creating new page per article)
    - Faster wait strategy ('load' instead of 'networkidle')
    - Configurable timeouts per fund
    """

    # Page pool configuration
    POOL_SIZE = 5
    DEFAULT_WAIT_MS = 1500  # Reduced from 3000ms

    def __init__(self, fund_config: FundConfig):
        self.fund = fund_config
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page_pool: asyncio.Queue[Page] = asyncio.Queue(maxsize=self.POOL_SIZE)
        self._pool_initialized = False
        self._all_pages: set = set()  # Track ALL pages for cleanup (prevents memory leaks)

    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        # Create a single shared context for all pages
        self._context = await self._browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            java_script_enabled=True,
        )
        # Pre-create page pool
        await self._init_page_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Clean up ALL pages (including overflow pages not in pool)
        for page in list(self._all_pages):
            try:
                await page.close()
            except Exception:
                pass  # Page may already be closed
        self._all_pages.clear()

        # Also drain the pool queue (pages already closed above)
        while not self._page_pool.empty():
            try:
                self._page_pool.get_nowait()
            except Exception:
                pass
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _init_page_pool(self):
        """Initialize the page pool with reusable pages."""
        if self._pool_initialized:
            return

        for _ in range(self.POOL_SIZE):
            page = await self._create_page()
            await self._page_pool.put(page)

        self._pool_initialized = True

    async def _create_page(self) -> Page:
        """Create a new browser page with anti-detection measures."""
        page = await self._context.new_page()

        # Add stealth scripts to avoid detection
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """)

        # Track page for cleanup (prevents memory leaks from overflow pages)
        self._all_pages.add(page)
        return page

    async def acquire_page(self) -> Page:
        """
        Acquire a page from the pool.

        If pool is empty, creates a new page (overflow).
        """
        try:
            # Try to get from pool (non-blocking)
            page = self._page_pool.get_nowait()
            return page
        except asyncio.QueueEmpty:
            # Pool exhausted, create overflow page
            return await self._create_page()

    async def release_page(self, page: Page):
        """
        Release a page back to the pool.

        If pool is full, closes the page instead.

        FIX: Handles race condition where page.goto('about:blank') can fail
        silently, which would leak the page (not returned to pool OR closed).
        Now explicitly handles goto failure before attempting pool return.
        """
        # Step 1: Try to reset page state for reuse
        try:
            await page.goto('about:blank')
        except Exception:
            # Page navigation failed - page is in bad state, close and remove
            try:
                await page.close()
            except Exception:
                pass
            self._all_pages.discard(page)
            return

        # Step 2: Only put back in pool if goto succeeded
        try:
            self._page_pool.put_nowait(page)
        except asyncio.QueueFull:
            # Pool full, close overflow page and remove from tracking
            try:
                await page.close()
            except Exception:
                pass
            self._all_pages.discard(page)

    async def new_page(self) -> Page:
        """
        Create a new browser page with anti-detection measures.

        DEPRECATED: Use acquire_page() and release_page() instead.
        Kept for backwards compatibility.
        """
        return await self._create_page()

    async def fetch_rendered(self, url: str, wait_selector: Optional[str] = None, wait_ms: int = None) -> str:
        """
        Fetch a page and wait for JavaScript to render.

        OPTIMIZED:
        - Uses 'load' instead of 'networkidle' (40% faster)
        - Reduced default wait time
        - Page pooling for reuse

        Args:
            url: URL to fetch
            wait_selector: CSS selector to wait for (indicates content loaded)
            wait_ms: Additional milliseconds to wait after page load

        Returns:
            Rendered HTML string
        """
        if wait_ms is None:
            wait_ms = self.DEFAULT_WAIT_MS

        page = await self.acquire_page()
        try:
            # Use 'load' instead of 'networkidle' - much faster
            await page.goto(url, wait_until='load', timeout=30000)

            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=5000)
                except Exception as e:
                    logger.warning(f"Selector '{wait_selector}' not found on {url}: {e}")

            # Reduced wait for lazy-loaded content
            if wait_ms > 0:
                await page.wait_for_timeout(wait_ms)

            # Get the fully rendered HTML
            html = await page.content()
            return html

        finally:
            await self.release_page(page)

    async def scroll_and_fetch(self, url: str, scroll_count: int = 3, wait_selector: Optional[str] = None) -> str:
        """
        Fetch page with infinite scroll handling.

        OPTIMIZED:
        - Uses page pool
        - Reduced scroll wait time

        Args:
            url: URL to fetch
            scroll_count: Number of times to scroll down
            wait_selector: CSS selector indicating content loaded

        Returns:
            Rendered HTML after scrolling
        """
        page = await self.acquire_page()
        try:
            await page.goto(url, wait_until='load', timeout=30000)

            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=5000)
                except Exception as e:
                    logger.warning(f"Selector '{wait_selector}' not found during scroll on {url}: {e}")

            # Scroll to load more content (reduced wait between scrolls)
            for _ in range(scroll_count):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(1000)  # Reduced from 2000ms

            html = await page.content()
            return html

        finally:
            await self.release_page(page)

    @abstractmethod
    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch page content (implemented by subclass)."""
        pass

    @abstractmethod
    async def parse(self, html: str) -> List[RawArticle]:
        """Parse HTML to extract articles (implemented by subclass)."""
        pass

    @abstractmethod
    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        """Normalize raw article (implemented by subclass)."""
        pass

    async def scrape(self) -> AsyncIterator[NormalizedArticle]:
        """
        Full scraping pipeline with Playwright.

        Yields:
            NormalizedArticle objects
        """
        html = await self.fetch()
        articles = await self.parse(html)

        for raw in articles:
            if self._should_filter(raw):
                continue

            normalized = await self.normalize(raw)
            normalized.fund_slug = self.fund.slug
            yield normalized

    def _should_filter(self, article: RawArticle) -> bool:
        """Check if article should be filtered based on negative keywords."""
        text = f"{article.title} {article.html}".lower()

        for keyword in self.fund.negative_keywords:
            if keyword.lower() in text:
                return True

        return False

    def _extract_text(self, html: str) -> str:
        """Extract clean text from HTML."""
        soup = BeautifulSoup(html, "lxml")

        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()

        text = soup.get_text(separator="\n", strip=True)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse various date formats."""
        from dateutil import parser as date_parser

        try:
            return date_parser.parse(date_str).date()
        except (ValueError, TypeError):
            return None
