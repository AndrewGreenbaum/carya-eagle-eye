"""
Scraper Orchestrator - Coordinates scraping across all 18 VC funds.

Handles:
- Running scrapers for configured funds
- Processing articles through extraction pipeline
- Storing deals in database
- Error handling and logging

OPTIMIZED:
- Concurrent article processing (5 parallel)
- Pre-extraction URL deduplication
- Metrics logging for performance monitoring
- Error isolation (one bad article doesn't crash fund)
"""

import asyncio
import logging
import time
from enum import Enum
from typing import List, Optional, Dict, Type, Set, Tuple
from datetime import datetime, timezone

import httpx


# FIX #24: Define consistent status values for extraction results
class ExtractionStatus(str, Enum):
    """Status codes for article extraction results."""
    SAVED = "saved"
    DUPLICATE = "duplicate"
    LOW_CONFIDENCE = "low_confidence"
    NOT_NEW_ANNOUNCEMENT = "not_new_announcement"
    INVALID_COMPANY_NAME = "invalid_company_name"  # FIX: Reject <UNKNOWN> etc.
    EXTRACTION_FAILED = "extraction_failed"
    ERROR = "error"  # Generic error


# Invalid company name patterns that should be rejected
# FIX: Prevent saving deals with placeholder/unknown company names
# Conservative list - only clear LLM placeholders, not generic names that could be real companies
INVALID_COMPANY_NAMES = {
    "<unknown>",  # LLM explicit "I don't know" response
    "unknown",    # Variation
    "n/a",        # Not applicable
    "none",       # Missing value
    "tbd",        # To be determined
    "",           # Empty string
}

# Patterns that indicate the LLM fabricated a company name instead of using <UNKNOWN>
# FIX (2026-01): Catch fabricated names like "Unnamed Patent AI Startup"
import re
FABRICATED_NAME_PATTERNS = [
    r'^unnamed\s+',           # "Unnamed X Startup"
    r'^untitled\s+',          # "Untitled Y Company"
    r'^unidentified\s+',      # "Unidentified Z Firm"
    r'^undisclosed\s+',       # "Undisclosed AI Company"
    r'\s+startup$',           # Ends with "Startup" (generic)
    r'^the\s+company$',       # "The Company"
    r'^a\s+new\s+',           # "A New AI Startup"
    r'^stealth\s+',           # "Stealth Mode Company"
]


def is_fabricated_company_name(name: str) -> bool:
    """
    Detect if the company name was fabricated by the LLM.

    When the LLM can't find a company name, it sometimes creates descriptive
    placeholder names like "Unnamed Patent AI Startup" instead of returning <UNKNOWN>.

    Returns True if the name appears to be fabricated, False otherwise.
    """
    if not name:
        return False

    name_lower = name.lower().strip()

    # Check exact invalid names
    if name_lower in {n.lower() for n in INVALID_COMPANY_NAMES}:
        return True

    # Check regex patterns
    for pattern in FABRICATED_NAME_PATTERNS:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return True

    return False


from sqlalchemy import select

from .base_scraper import BaseScraper, NormalizedArticle
from .scrapers.a16z import A16ZScraper
from .scrapers.sequoia import SequoiaScraper
from .scrapers.benchmark import BenchmarkScraper
from .scrapers.founders_fund import FoundersFundScraper
from .scrapers.thrive import ThriveScraper
from .scrapers.khosla import KhoslaScraper
from .scrapers.index_ventures import IndexVenturesScraper
from .scrapers.insight import InsightScraper
from .scrapers.bessemer import BessemerScraper
from .scrapers.redpoint import RedpointScraper
from .scrapers.greylock import GreylockScraper
from .scrapers.gv import GVScraper
from .scrapers.menlo import MenloScraper
from .scrapers.usv import USVScraper
from .scrapers.accel import AccelScraper
from .scrapers.felicis import FelicisScraper
from .scrapers.general_catalyst import GeneralCatalystScraper
from .scrapers.first_round import FirstRoundScraper
from ..config.funds import FUND_REGISTRY, FundConfig, ScraperType
from ..config.settings import settings
from ..analyst.extractor import (
    extract_deal, set_extraction_context, clear_extraction_context,
    EXTERNAL_ONLY_FUNDS, EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD
)
from ..archivist.storage import save_deal
from ..archivist.database import get_session
from ..archivist.models import Article

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Registry of implemented scrapers (All 18 VC funds)
SCRAPER_REGISTRY: Dict[str, Type[BaseScraper]] = {
    # Original 5
    "a16z": A16ZScraper,
    "sequoia": SequoiaScraper,
    "benchmark": BenchmarkScraper,
    "founders_fund": FoundersFundScraper,
    "thrive": ThriveScraper,
    # Additional 13
    "khosla": KhoslaScraper,
    "index": IndexVenturesScraper,
    "insight": InsightScraper,
    "bessemer": BessemerScraper,
    "redpoint": RedpointScraper,
    "greylock": GreylockScraper,
    "gv": GVScraper,
    "menlo": MenloScraper,
    "usv": USVScraper,
    "accel": AccelScraper,
    "felicis": FelicisScraper,
    "general_catalyst": GeneralCatalystScraper,
    "first_round": FirstRoundScraper,
}

# Concurrency settings
MAX_CONCURRENT_EXTRACTIONS = 5
RATE_LIMIT_DELAY = 0.5  # Reduced from 1.0s

# Retry and timeout settings
MAX_SCRAPER_RETRIES = 3
SCRAPER_RETRY_BACKOFF = 2.0  # Exponential backoff base
SCRAPER_TIMEOUT = 300  # 5 minutes max per fund scraper


class ScrapingResult:
    """Result of a scraping operation."""

    def __init__(self, fund_slug: str):
        self.fund_slug = fund_slug
        self.articles_found = 0
        self.articles_skipped_duplicate = 0
        self.articles_skipped_no_funding = 0
        self.articles_rejected_not_announcement = 0  # NEW: Track false positive rejections
        self.deals_extracted = 0
        self.deals_saved = 0
        self.errors: List[str] = []
        self.started_at = datetime.now(timezone.utc)
        self.completed_at: Optional[datetime] = None

    def complete(self):
        self.completed_at = datetime.now(timezone.utc)

    @property
    def duration_seconds(self) -> float:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0

    def log_metrics(self):
        """Log performance metrics."""
        logger.info(
            f"METRICS fund={self.fund_slug} "
            f"articles_found={self.articles_found} "
            f"skipped_duplicate={self.articles_skipped_duplicate} "
            f"skipped_no_funding={self.articles_skipped_no_funding} "
            f"rejected_not_announcement={self.articles_rejected_not_announcement} "
            f"deals_extracted={self.deals_extracted} "
            f"deals_saved={self.deals_saved} "
            f"errors={len(self.errors)} "
            f"duration_sec={self.duration_seconds:.2f}"
        )


async def check_url_exists(url: str) -> bool:
    """
    Check if an article URL already exists in the database.

    OPTIMIZATION: Avoids expensive Claude calls for duplicate articles.

    NOTE: For batch operations, use check_urls_exist_batch() instead.
    """
    try:
        async with get_session() as session:
            stmt = select(Article).where(Article.url == url).limit(1)
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None
    except Exception as e:
        logger.warning(f"Error checking URL existence: {e}")
        return False  # Assume not exists on error


async def check_urls_exist_batch(urls: List[str]) -> set:
    """
    Batch check which URLs already exist in the database.

    OPTIMIZATION: Single query instead of N queries (saves ~50 DB round-trips per scrape).

    Args:
        urls: List of URLs to check

    Returns:
        Set of URLs that already exist in database
    """
    if not urls:
        return set()

    try:
        async with get_session() as session:
            stmt = select(Article.url).where(Article.url.in_(urls))
            result = await session.execute(stmt)
            return {row[0] for row in result.fetchall()}
    except Exception as e:
        logger.warning(f"Error batch checking URL existence: {e}")
        return set()  # Assume none exist on error


async def process_article(
    article: NormalizedArticle,
    fund_config: FundConfig,
    scan_job_id: Optional[int] = None,
) -> Tuple[bool, str]:
    """
    Process a single article through extraction and storage.

    Returns:
        Tuple of (success: bool, status: ExtractionStatus)
        Status values are defined in ExtractionStatus enum
    """
    try:
        # FIX (2026-01): Detect if this is an external-only fund source
        # External sources get lower confidence threshold and skip background checks
        is_external_source = (
            fund_config.slug in EXTERNAL_ONLY_FUNDS or
            'google_news' in article.tags or
            fund_config.scraper_type == ScraperType.EXTERNAL
        )

        # Extract deal information using Claude
        # FIX (2026-01): Include article title in text to improve extraction
        # Titles often contain the company name when text is truncated
        article_content = article.text
        if article.title and article.title.strip():
            article_content = f"ARTICLE TITLE: {article.title.strip()}\n\n{article.text}"

        extraction = await extract_deal(
            article_text=article_content,
            source_url=article.url,
            source_name=fund_config.slug,
            fund_context=fund_config,
            is_external_source=is_external_source,
            article_published_date=article.published_date,
        )

        # FIX 1: Handle None return (API failures, timeouts, etc.)
        if extraction is None:
            logger.warning(f"Extraction failed (API error) for {article.url}")
            return False, ExtractionStatus.EXTRACTION_FAILED.value

        # NEW: Reject non-announcements (false positive prevention)
        # This catches articles that mention funding as background context
        if not extraction.is_new_announcement:
            rejection_reason = extraction.announcement_rejection_reason or "not a new funding announcement"
            # FIX #50: DEBUG level for per-article messages (reduces log noise)
            logger.debug(
                f"Rejecting non-announcement: {extraction.startup_name} - "
                f"Reason: {rejection_reason} - "
                f"URL: {article.url}"
            )
            return False, ExtractionStatus.NOT_NEW_ANNOUNCEMENT.value

        # FIX: Reject invalid company names like <UNKNOWN>, "the company", etc.
        # This catches cases where LLM couldn't identify the company but set is_new_announcement=True
        # These deals will likely be captured from another source with better content
        # FIX (2026-01): Also catch fabricated names like "Unnamed Patent AI Startup"
        company_name = (extraction.startup_name or "").strip()
        if not company_name or is_fabricated_company_name(company_name):
            # Log with enough detail to manually investigate if needed
            logger.warning(
                f"Skipping deal with invalid/fabricated company name: '{extraction.startup_name}' - "
                f"Amount: {extraction.amount}, Fund: {extraction.tracked_fund_name}, "
                f"URL: {article.url} - "
                f"(Deal may be captured from another source with better content)"
            )
            return False, ExtractionStatus.INVALID_COMPANY_NAME.value

        # FIX (2026-01): Use lower confidence threshold for external-only funds
        # These funds often have less structured content but are still valid deals
        confidence_threshold = (
            EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD if is_external_source
            else settings.extraction_confidence_threshold
        )

        # Skip low-confidence extractions
        if extraction.confidence_score < confidence_threshold:
            # FIX #50: DEBUG level for per-article messages (reduces log noise)
            logger.debug(
                f"Skipping low-confidence extraction: {extraction.startup_name} - "
                f"Score: {extraction.confidence_score:.2f} < {confidence_threshold} "
                f"({'external' if is_external_source else 'standard'}) - "
                f"URL: {article.url}"
            )
            return False, ExtractionStatus.LOW_CONFIDENCE.value

        # Save to database
        alert_info = None
        async with get_session() as session:
            deal, alert_info = await save_deal(
                session=session,
                extraction=extraction,
                article_url=article.url,
                article_title=article.title,
                article_text=article.text,
                source_fund_slug=article.fund_slug,
                article_published_date=article.published_date,
                scan_job_id=scan_job_id,
                # Pass SEC amount if available (from NormalizedArticle)
                sec_amount_usd=getattr(article, 'sec_amount_usd', None),
                amount_source=getattr(article, 'amount_source', None),
            )

            # Check if this was a duplicate (save_deal returns None for duplicates)
            if deal is None:
                # FIX #50: DEBUG level for per-article messages (reduces log noise)
                logger.debug(
                    f"Duplicate deal detected: {extraction.startup_name} - "
                    f"{extraction.round_label.value} - article linked to existing deal"
                )
                return False, ExtractionStatus.DUPLICATE.value

            logger.info(
                f"Saved deal: {extraction.startup_name} - "
                f"{extraction.round_label.value} - "
                f"Lead: {extraction.tracked_fund_is_lead}"
            )

        # FIX #20: Send alert AFTER transaction commits (session context exited)
        if alert_info:
            from ..scheduler.notifications import send_lead_deal_alert
            await send_lead_deal_alert(
                company_name=alert_info.company_name,
                amount=alert_info.amount,
                round_type=alert_info.round_type,
                lead_investor=alert_info.lead_investor,
                enterprise_category=alert_info.enterprise_category,
                verification_snippet=alert_info.verification_snippet,
            )

        return True, ExtractionStatus.SAVED.value

    except Exception as e:
        error_msg = f"Error processing article {article.url}: {e}"
        logger.error(error_msg)
        return False, ExtractionStatus.ERROR.value


async def scrape_fund(fund_slug: str, scan_job_id: Optional[int] = None) -> ScrapingResult:
    """
    Scrape a single fund and process all articles.

    OPTIMIZED:
    - Pre-extraction URL deduplication
    - Concurrent article processing
    - Error isolation per article

    Args:
        fund_slug: The slug of the fund to scrape (e.g., "a16z")
        scan_job_id: Optional ID of the scan job for linking deals

    Returns:
        ScrapingResult with statistics
    """
    result = ScrapingResult(fund_slug)
    start_time = time.perf_counter()

    # NOTE: Content hash cache is now cleared ONCE at job start in scheduler/jobs.py
    # Do NOT clear here - we want cross-source dedup within a single scrape run

    # Check if fund exists
    if fund_slug not in FUND_REGISTRY:
        result.errors.append(f"Unknown fund: {fund_slug}")
        result.complete()
        return result

    fund_config = FUND_REGISTRY[fund_slug]

    # Check if scraper is implemented
    if fund_slug not in SCRAPER_REGISTRY:
        result.errors.append(f"Scraper not implemented for: {fund_slug}")
        result.complete()
        return result

    scraper_class = SCRAPER_REGISTRY[fund_slug]

    # Set extraction context for token logging (tracks which fund is being processed)
    set_extraction_context(source_name=fund_slug)

    # Retry loop for transient failures
    for attempt in range(MAX_SCRAPER_RETRIES):
        try:
            async with scraper_class() as scraper:
                logger.info(f"Starting scrape for {fund_config.name} (attempt {attempt + 1}/{MAX_SCRAPER_RETRIES})")

                # Collect all articles with timeout protection
                articles: List[NormalizedArticle] = []

                async def collect_articles():
                    async for article in scraper.scrape():
                        articles.append(article)

                try:
                    await asyncio.wait_for(collect_articles(), timeout=SCRAPER_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.error(f"Scraper timeout for {fund_config.name} after {SCRAPER_TIMEOUT}s")
                    result.errors.append(f"Timeout after {SCRAPER_TIMEOUT}s")
                    # Continue with articles collected so far

                result.articles_found = len(articles)
                logger.info(f"Found {len(articles)} articles for {fund_config.name}")

            # Pre-extraction deduplication (OPTIMIZED: batch query instead of N queries)
            article_urls = [a.url for a in articles]
            existing_urls = await check_urls_exist_batch(article_urls)

            unique_articles: List[NormalizedArticle] = []
            for article in articles:
                if article.url in existing_urls:
                    result.articles_skipped_duplicate += 1
                    logger.debug(f"Skipping duplicate: {article.url}")
                else:
                    unique_articles.append(article)

            logger.info(
                f"After deduplication: {len(unique_articles)} unique articles "
                f"({result.articles_skipped_duplicate} duplicates skipped)"
            )

            # Process articles concurrently with semaphore
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)

            async def process_with_limit(article: NormalizedArticle) -> tuple[bool, str]:
                async with semaphore:
                    result = await process_article(article, fund_config, scan_job_id)
                # Rate limit delay AFTER releasing semaphore (avoids blocking other tasks)
                await asyncio.sleep(RATE_LIMIT_DELAY)
                return result

            # Run all extractions concurrently
            if unique_articles:
                tasks = [process_with_limit(article) for article in unique_articles]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results
                for i, res in enumerate(results):
                    if isinstance(res, Exception):
                        result.errors.append(f"Article {i}: {str(res)}")
                    else:
                        success, status = res
                        result.deals_extracted += 1
                        if success:
                            result.deals_saved += 1
                        elif status == ExtractionStatus.DUPLICATE.value:
                            # Deal-level duplicate (same company/round/amount)
                            result.articles_skipped_duplicate += 1
                        elif status == ExtractionStatus.LOW_CONFIDENCE.value:
                            result.articles_skipped_no_funding += 1
                        elif status == ExtractionStatus.NOT_NEW_ANNOUNCEMENT.value:
                            result.articles_rejected_not_announcement += 1
                        elif status in (ExtractionStatus.ERROR.value, ExtractionStatus.EXTRACTION_FAILED.value):
                            # FIX: Use enum values instead of string matching
                            result.errors.append(f"Article {unique_articles[i].url}: {status}")

                # If we get here, scraping succeeded - break retry loop
                break

        except (asyncio.TimeoutError, ConnectionError, TimeoutError, httpx.TimeoutException, httpx.ConnectError) as e:
            # Transient errors - retry with backoff
            if attempt < MAX_SCRAPER_RETRIES - 1:
                wait_time = SCRAPER_RETRY_BACKOFF ** attempt
                logger.warning(f"Transient error scraping {fund_slug} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                error_msg = f"Failed after {MAX_SCRAPER_RETRIES} attempts: {e}"
                logger.error(error_msg)
                result.errors.append(error_msg)
                break

        except Exception as e:
            # Non-transient errors - don't retry
            error_msg = f"Error scraping {fund_slug}: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)
            break

    # Clear extraction context after processing this fund
    clear_extraction_context()

    result.complete()
    result.log_metrics()

    return result


async def scrape_all_funds(
    fund_slugs: Optional[List[str]] = None,
    parallel: bool = False,
    max_parallel_funds: int = 3,
    scan_job_id: Optional[int] = None,
) -> List[ScrapingResult]:
    """
    Scrape multiple funds.

    OPTIMIZED:
    - Optional parallel fund scraping
    - Configurable parallelism level

    Args:
        fund_slugs: List of fund slugs to scrape. If None, scrapes all implemented funds.
        parallel: If True, scrape funds concurrently.
        max_parallel_funds: Maximum number of funds to scrape in parallel.
        scan_job_id: Optional ID of the scan job for linking deals.

    Returns:
        List of ScrapingResult objects
    """
    if fund_slugs is None:
        fund_slugs = list(SCRAPER_REGISTRY.keys())

    logger.info(f"Starting scrape for {len(fund_slugs)} funds: {fund_slugs}")

    if parallel:
        # Run funds in parallel with limited concurrency
        semaphore = asyncio.Semaphore(max_parallel_funds)

        async def scrape_with_limit(slug: str) -> ScrapingResult:
            async with semaphore:
                return await scrape_fund(slug, scan_job_id)

        tasks = [scrape_with_limit(slug) for slug in fund_slugs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_result = ScrapingResult(fund_slugs[i])
                error_result.errors.append(str(result))
                error_result.complete()
                processed_results.append(error_result)
            else:
                processed_results.append(result)
        return processed_results
    else:
        # Run scrapers sequentially
        results = []
        for slug in fund_slugs:
            result = await scrape_fund(slug, scan_job_id)
            results.append(result)
            # Delay between funds
            await asyncio.sleep(1.0)
        return results


async def run_scraper_cli(fund_slug: Optional[str] = None):
    """
    CLI entry point for running scrapers.

    Args:
        fund_slug: Specific fund to scrape, or None for all implemented funds.
    """
    if fund_slug:
        result = await scrape_fund(fund_slug)
        print(f"\n=== Scraping Results for {fund_slug} ===")
        print(f"Articles found: {result.articles_found}")
        print(f"Duplicates skipped: {result.articles_skipped_duplicate}")
        print(f"Deals extracted: {result.deals_extracted}")
        print(f"Deals saved: {result.deals_saved}")
        print(f"Errors: {len(result.errors)}")
        if result.errors:
            for error in result.errors:
                print(f"  - {error}")
        print(f"Duration: {result.duration_seconds:.2f}s")
    else:
        results = await scrape_all_funds()
        print("\n=== Scraping Summary ===")
        total_articles = sum(r.articles_found for r in results)
        total_deals = sum(r.deals_saved for r in results)
        total_errors = sum(len(r.errors) for r in results)
        total_duration = sum(r.duration_seconds for r in results)
        print(f"Total articles: {total_articles}")
        print(f"Total deals saved: {total_deals}")
        print(f"Total errors: {total_errors}")
        print(f"Total duration: {total_duration:.2f}s")


# List of implemented scrapers
def get_implemented_scrapers() -> List[str]:
    """Return list of fund slugs with implemented scrapers."""
    return list(SCRAPER_REGISTRY.keys())


def get_unimplemented_scrapers() -> List[str]:
    """Return list of fund slugs without implemented scrapers."""
    return [slug for slug in FUND_REGISTRY if slug not in SCRAPER_REGISTRY]
