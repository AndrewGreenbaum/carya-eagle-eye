"""
Y Combinator Scraper - Monitor YC for Demo Day companies and funding news.

Y Combinator runs 2 batches per year (Winter & Summer), each with ~200 companies.
Demo Day = 200+ companies launching twice per year, often raising funding soon after.

Data Sources:
1. YC Company Directory via Algolia API
2. YC Blog RSS: https://www.ycombinator.com/blog/feed
3. YC Top Companies: https://www.ycombinator.com/topcompanies
"""

import asyncio
import logging
import re
import httpx
import feedparser

logger = logging.getLogger(__name__)
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ...config.settings import settings


# YC Algolia API configuration (from settings, with fallback to public defaults)
ALGOLIA_INDEX = "YCCompany_production"

# YC batch identifiers (format: "Winter 2024", "Summer 2024", etc.)
def get_current_batches() -> List[str]:
    """Get recent YC batch identifiers in Algolia format."""
    # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
    now = datetime.now(timezone.utc)
    year = now.year
    batches = []

    # FIX: Don't fetch future batches (year + 1 doesn't exist yet)
    # Current and previous batches (Winter, Summer, Fall)
    for y in range(year, year - 2, -1):
        batches.extend([f"Winter {y}", f"Fall {y}", f"Summer {y}"])

    return batches[:8]  # Last 8 batches


YC_URLS = {
    "blog_rss": "https://www.ycombinator.com/blog/rss/",
    "top_companies": "https://www.ycombinator.com/topcompanies",
}

# YC has its own partners who sometimes lead/participate
YC_INVESTORS = [
    "Y Combinator", "YC", "YC Continuity",
]


@dataclass
class YCCompany:
    """Y Combinator company information."""
    name: str
    url: str
    description: str
    batch: str  # e.g., "S24", "W24"
    industry: str
    location: Optional[str]
    website: Optional[str]
    status: Optional[str]  # Active, Acquired, Public, etc.


@dataclass
class YCBlogPost:
    """YC Blog post."""
    title: str
    url: str
    description: str
    published: Optional[datetime]
    author: Optional[str]


class YCombinatorScraper:
    """
    Scraper for Y Combinator companies and news.

    Uses Algolia API for company data (same as YC website).
    Tracks:
    - Demo Day batches (200+ companies twice/year)
    - YC blog RSS for announcements
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            }
        )
        self.current_batches = get_current_batches()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_companies_algolia(self, batch: str, hits_per_page: int = 100) -> List[YCCompany]:
        """Fetch companies from Algolia API for a specific batch with pagination."""
        try:
            # Validate Algolia config before using
            app_id = getattr(settings, 'yc_algolia_app_id', None)
            api_key = getattr(settings, 'yc_algolia_api_key', None)
            if not app_id or not api_key:
                logger.warning("YC Algolia config not set (yc_algolia_app_id/yc_algolia_api_key)")
                return []
            url = f"https://{app_id}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

            headers = {
                "X-Algolia-Application-Id": app_id,
                "X-Algolia-API-Key": api_key,
                "Content-Type": "application/json",
            }

            # Paginate through all results (YC batches have 200-400 companies)
            all_companies = []
            page = 0
            max_pages = 10  # Safety limit

            while page < max_pages:
                payload = {
                    "query": "",
                    "hitsPerPage": hits_per_page,
                    "page": page,
                    "facetFilters": [f"batch:{batch}"],
                }

                response = await self.client.post(url, json=payload, headers=headers)
                response.raise_for_status()

                data = response.json()
                hits = data.get("hits", [])

                if not hits:
                    break  # No more results

                for hit in hits:
                    # Skip companies without slug (creates malformed URL)
                    slug = hit.get("slug", "")
                    if not slug or not hit.get("name", "").strip():
                        continue

                    # Filter None values from industries list
                    industries_raw = hit.get("industries") or []
                    industries = ", ".join(i for i in industries_raw if i)

                    all_companies.append(YCCompany(
                        name=hit.get("name", "").strip(),
                        url=f"https://www.ycombinator.com/companies/{slug}",
                        description=hit.get("one_liner", ""),
                        batch=hit.get("batch", batch),
                        industry=industries,
                        location=hit.get("all_locations", ""),
                        website=hit.get("website", ""),
                        status=hit.get("status", ""),
                    ))

                # Check if we've fetched all results
                total_hits = data.get("nbHits", 0)
                fetched = (page + 1) * hits_per_page
                if fetched >= total_hits:
                    break

                page += 1
                await asyncio.sleep(0.2)  # Rate limit between pages

            if page > 0:
                logger.debug(f"YC batch {batch}: fetched {len(all_companies)} companies across {page + 1} pages")

            return all_companies

        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching YC companies from Algolia for batch {batch}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching YC companies from Algolia for batch {batch}: {e}", exc_info=True)
            return []

    async def fetch_recent_batch_companies(self) -> List[YCCompany]:
        """Fetch companies from recent YC batches using Algolia."""
        all_companies = []
        seen_names = set()

        for batch in self.current_batches:
            companies = await self.fetch_companies_algolia(batch)

            for company in companies:
                if company.name and company.name not in seen_names:
                    seen_names.add(company.name)
                    all_companies.append(company)

            logger.info(f"YC batch {batch}: {len(companies)} companies")
            await asyncio.sleep(0.5)  # Rate limit

        return all_companies

    async def fetch_blog_posts(self) -> List[YCBlogPost]:
        """Fetch recent YC blog posts from RSS feed."""
        posts = []

        try:
            response = await self.client.get(YC_URLS["blog_rss"])
            response.raise_for_status()

            feed = feedparser.parse(response.text)

            for entry in feed.entries[:20]:  # Last 20 posts
                # FIX: Skip entries without URL (downstream issues)
                if not entry.get('link'):
                    continue

                # Parse publication date
                pub_date = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    # FIX: Check tuple length before unpacking (crash prevention)
                    if len(entry.published_parsed) >= 6:
                        try:
                            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                        except (TypeError, ValueError):
                            pub_date = None

                # Get description
                description = entry.get('summary', '')
                if description:
                    soup = BeautifulSoup(description, 'lxml')
                    description = soup.get_text(strip=True)[:500]

                posts.append(YCBlogPost(
                    title=entry.get('title', ''),
                    url=entry.get('link', ''),
                    description=description,
                    published=pub_date,
                    author=entry.get('author', None),
                ))

        # FIX: Handle specific HTTP errors first, then general with exc_info
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching YC blog RSS: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching YC blog RSS: {e}", exc_info=True)

        return posts

    def is_enterprise_ai_company(self, company: YCCompany) -> bool:
        """Check if company is Enterprise AI focused."""
        enterprise_keywords = [
            "b2b", "enterprise", "saas", "infrastructure", "api", "platform",
            "developer", "devtools", "ai", "ml", "llm", "automation",
            "workflow", "agent", "copilot"
        ]
        text = f"{company.name} {company.description} {company.industry}".lower()
        return any(kw in text for kw in enterprise_keywords)

    async def company_to_article(self, company: YCCompany) -> NormalizedArticle:
        """Convert YC company to NormalizedArticle for tracking."""
        text_parts = [
            f"Y Combinator Company: {company.name}",
            f"Batch: {company.batch}",
            f"Description: {company.description}",
        ]

        if company.industry:
            text_parts.append(f"Industry: {company.industry}")
        if company.location:
            text_parts.append(f"Location: {company.location}")
        if company.website:
            text_parts.append(f"Website: {company.website}")

        text_parts.append(f"\nYC Profile: {company.url}")
        text_parts.append("\nNote: Y Combinator Demo Day company - potential early-stage investment opportunity")

        return NormalizedArticle(
            url=company.url,
            title=f"YC {company.batch}: {company.name} - {company.description[:100]}",
            text="\n".join(text_parts),
            # FIX: Use None instead of date.today() (YC companies don't have specific publish dates)
            published_date=None,
            author="Y Combinator",
            tags=['ycombinator', 'demo_day', company.batch, company.industry.lower()] if company.industry else ['ycombinator', 'demo_day', company.batch],
            fund_slug="",  # YC is the fund here
            # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
            fetched_at=datetime.now(timezone.utc),
        )

    async def scrape_all(self, include_all_batches: bool = False, filter_enterprise: bool = True) -> List[NormalizedArticle]:
        """
        Full scraping pipeline for Y Combinator.

        Args:
            include_all_batches: If True, fetch all recent batches. If False, only current.
            filter_enterprise: If True, only include Enterprise AI companies.

        Returns:
            List of NormalizedArticle objects for companies and blog posts.
        """
        articles = []

        # Get companies from recent batches
        companies = await self.fetch_recent_batch_companies()

        # Filter for Enterprise AI to avoid wasting Claude tokens on consumer companies
        enterprise_count = 0
        skipped_count = 0
        for company in companies:
            if filter_enterprise and not self.is_enterprise_ai_company(company):
                skipped_count += 1
                continue
            enterprise_count += 1
            article = await self.company_to_article(company)
            articles.append(article)

        if filter_enterprise:
            logger.info(f"YC companies: {enterprise_count} Enterprise AI, {skipped_count} skipped (consumer/other)")

        # Get blog posts for announcements
        blog_posts = await self.fetch_blog_posts()
        for post in blog_posts:
            # FIX: Skip blog posts without dates (don't default to today)
            if not post.published:
                continue

            articles.append(NormalizedArticle(
                url=post.url,
                title=post.title,
                text=f"{post.title}\n\n{post.description}",
                published_date=post.published.date(),
                author=post.author,
                tags=['ycombinator', 'blog'],
                fund_slug="",
                # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
                fetched_at=datetime.now(timezone.utc),
            ))

        return articles


# Convenience function
async def run_ycombinator_scraper() -> List[NormalizedArticle]:
    """Run Y Combinator scraper and return articles."""
    async with YCombinatorScraper() as scraper:
        return await scraper.scrape_all()
