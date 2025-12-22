"""
Firecrawl Scraper - Bypass Bot Detection.

Uses Firecrawl API to scrape JavaScript-heavy PR sites
that have Cloudflare or other bot detection.

Target sites:
- PRNewswire (prnewswire.com)
- BusinessWire (businesswire.com)
- GlobeNewswire (globenewswire.com)
- Any JS-rendered funding announcement page

Advantages:
- Bypasses Cloudflare, Akamai, etc.
- Returns clean markdown text
- Handles JavaScript rendering
- More reliable than Playwright for some sites
"""

import asyncio
import logging
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ...config.settings import settings

logger = logging.getLogger(__name__)

# Memory safety limit
MAX_BATCH_RESULTS = 500

# Firecrawl API endpoints
FIRECRAWL_SCRAPE_URL = "https://api.firecrawl.dev/v1/scrape"
FIRECRAWL_CRAWL_URL = "https://api.firecrawl.dev/v1/crawl"

# Known PR sites that benefit from Firecrawl
PR_SITES = [
    "prnewswire.com",
    "businesswire.com",
    "globenewswire.com",
    "accesswire.com",
    "marketwatch.com",
    "benzinga.com",
]


@dataclass
class FirecrawlResult:
    """Result from Firecrawl API."""
    url: str
    title: str
    markdown: str  # Clean extracted content
    html: Optional[str] = None
    metadata: Dict[str, Any] = None
    scrape_time: float = 0.0


class FirecrawlScraper:
    """
    Scrape URLs using Firecrawl API.

    Firecrawl handles:
    - JavaScript rendering
    - Bot detection bypass (Cloudflare, etc.)
    - Clean markdown extraction
    - Metadata extraction
    """

    def __init__(self):
        self.api_key = settings.firecrawl_api_key
        self.client = httpx.AsyncClient(
            timeout=60,  # Firecrawl can take time for JS sites
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def scrape_url(
        self,
        url: str,
        formats: List[str] = ["markdown", "html"],
        wait_for: int = 5000,  # ms to wait for JS
    ) -> Optional[FirecrawlResult]:
        """
        Scrape a single URL using Firecrawl.

        Args:
            url: The URL to scrape
            formats: Output formats (markdown, html, rawHtml, links, screenshot)
            wait_for: Milliseconds to wait for JavaScript

        Returns:
            FirecrawlResult or None if failed
        """
        if not self.api_key:
            logger.warning("FIRECRAWL_API_KEY not configured - skipping scrape")
            return None

        payload = {
            "url": url,
            "formats": formats,
            "waitFor": wait_for,
            "onlyMainContent": True,  # Strip nav, footer, etc.
            "removeBase64Images": True,
            "timeout": 30000,
        }

        try:
            response = await self.client.post(FIRECRAWL_SCRAPE_URL, json=payload)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                logger.error(f"Firecrawl error: {data.get('error', 'Unknown error')}")
                return None

            result_data = data.get("data", {})

            return FirecrawlResult(
                url=url,
                title=result_data.get("metadata", {}).get("title", ""),
                markdown=result_data.get("markdown", ""),
                html=result_data.get("html"),
                metadata=result_data.get("metadata", {}),
                scrape_time=data.get("scrapeTime", 0),
            )

        except httpx.HTTPStatusError as e:
            logger.error(f"Firecrawl HTTP error for {url}: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Firecrawl error for {url}: {e}")
            return None

    async def scrape_batch(
        self,
        urls: List[str],
        max_concurrent: int = 3,
    ) -> List[FirecrawlResult]:
        """
        Scrape multiple URLs with rate limiting.

        Args:
            urls: List of URLs to scrape
            max_concurrent: Max concurrent requests

        Returns:
            List of successful FirecrawlResult objects
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results: List[FirecrawlResult] = []

        async def scrape_with_limit(url: str) -> Optional[FirecrawlResult]:
            async with semaphore:
                result = await self.scrape_url(url)
                await asyncio.sleep(0.5)  # Rate limit
                return result

        tasks = [scrape_with_limit(url) for url in urls]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in raw_results:
            # Memory safety check
            if len(results) >= MAX_BATCH_RESULTS:
                logger.warning(f"Hit max batch results limit ({MAX_BATCH_RESULTS})")
                break

            if isinstance(result, FirecrawlResult):
                results.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Batch scrape exception: {result}")

        return results

    def is_pr_site(self, url: str) -> bool:
        """Check if URL is from a known PR site."""
        url_lower = url.lower()
        return any(site in url_lower for site in PR_SITES)

    async def to_normalized_article(
        self,
        result: FirecrawlResult,
        fund_slug: str = "",
    ) -> NormalizedArticle:
        """Convert Firecrawl result to NormalizedArticle."""
        # Extract date from metadata if available
        pub_date = None
        if result.metadata:
            date_str = result.metadata.get("publishedDate") or result.metadata.get("date")
            if date_str:
                try:
                    from dateutil import parser
                    pub_date = parser.parse(date_str).date()
                except Exception:
                    pass

        # Use markdown as text (cleaner than HTML)
        text = result.markdown or ""

        # If markdown is empty, try to extract from HTML
        if not text and result.html:
            soup = BeautifulSoup(result.html, "lxml")
            text = soup.get_text(separator="\n", strip=True)

        # Extract source from URL
        source = ""
        for site in PR_SITES:
            if site in result.url.lower():
                source = site.split(".")[0].title()
                break

        return NormalizedArticle(
            url=result.url,
            title=result.title,
            text=text,
            published_date=pub_date,
            author=source or "Firecrawl",
            tags=["firecrawl", source.lower()] if source else ["firecrawl"],
            fund_slug=fund_slug,
            fetched_at=datetime.now(timezone.utc),
        )

    async def scrape_pr_urls(
        self,
        urls: List[str],
        fund_slug: str = "",
    ) -> List[NormalizedArticle]:
        """
        Scrape list of PR URLs and return normalized articles.

        Filters to only scrape known PR sites.

        Args:
            urls: List of URLs (will filter to PR sites only)
            fund_slug: Optional fund context

        Returns:
            List of NormalizedArticle objects
        """
        # Filter to PR sites only
        pr_urls = [url for url in urls if self.is_pr_site(url)]

        if not pr_urls:
            return []

        # Scrape all URLs
        results = await self.scrape_batch(pr_urls)

        # Convert to normalized articles
        articles = []
        for result in results:
            article = await self.to_normalized_article(result, fund_slug)
            articles.append(article)

        return articles


# Convenience functions
async def scrape_with_firecrawl(url: str) -> Optional[str]:
    """
    Quick function to scrape a single URL and get markdown text.

    Returns:
        Markdown text content or None if failed
    """
    async with FirecrawlScraper() as scraper:
        result = await scraper.scrape_url(url)
        return result.markdown if result else None


async def run_firecrawl_scraper(urls: List[str]) -> List[NormalizedArticle]:
    """Run Firecrawl scraper on list of URLs."""
    async with FirecrawlScraper() as scraper:
        return await scraper.scrape_pr_urls(urls)
