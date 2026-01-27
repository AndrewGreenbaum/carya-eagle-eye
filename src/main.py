"""
The Bud Tracker - Main Application Entry Point

An autonomous intelligence system to monitor lead investor signals
from elite VC firms.

OPTIMIZED:
- Response caching for static endpoints
- GZip compression for large responses
- Pagination support for deals
- Batch extraction endpoint
- Improved error handling (no sample data fallback)
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
import time

logger = logging.getLogger(__name__)

from fastapi import FastAPI, HTTPException, Depends, Security, Query, Request, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, nullslast, nullsfirst
from sqlalchemy.orm import selectinload

from .config import settings, FUND_REGISTRY, get_all_funds
from .analyst import extract_deal, DealExtraction, LeadStatus
from .analyst.extractor import extract_deal_batch, clear_content_hash_cache, set_extraction_context, clear_extraction_context
from .archivist import (
    get_db,
    get_session,
    close_db,
    seed_funds,
    get_deals,
    Deal,
    DealInvestor,
    PortfolioCompany,
)
from .archivist.database import get_pool_status
from .archivist.tracker_storage import (
    get_tracker_items,
    get_tracker_item,
    create_tracker_item,
    bulk_create_tracker_items,
    update_tracker_item,
    move_tracker_item,
    delete_tracker_item,
    create_tracker_from_deal,
    get_tracker_stats,
    # Column management
    get_tracker_columns,
    get_tracker_column,
    create_tracker_column,
    update_tracker_column,
    move_tracker_column,
    delete_tracker_column,
    get_column_item_counts,
    AVAILABLE_COLORS,
)
from .common.brave_client import close_brave_client
from .archivist.models import Fund
from .archivist.models import Article
from .archivist.models import StealthDetection
from .harvester import (
    scrape_fund,
    scrape_all_funds,
    get_implemented_scrapers,
    get_unimplemented_scrapers,
)
from .scheduler import setup_scheduler, shutdown_scheduler, start_stuck_monitor, stop_stuck_monitor
from .scheduler import jobs as scheduler_module
import re
from urllib.parse import quote


# ----- URL Validation Helpers (FIX #1: Use shared module) -----
from src.common.url_utils import (
    is_valid_url,
    is_valid_website_url,
    is_valid_linkedin_profile,
    is_valid_linkedin_company,
    sanitize_url,
    sanitize_linkedin_url,
)


# ----- Caching Infrastructure -----

class SimpleCache:
    """Simple in-memory cache with TTL support."""

    def __init__(self):
        self._cache: Dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            value, expires_at = self._cache[key]
            if time.time() < expires_at:
                return value
            else:
                del self._cache[key]
        return None

    def set(self, key: str, value: Any, ttl_seconds: int = 3600):
        expires_at = time.time() + ttl_seconds
        self._cache[key] = (value, expires_at)

    def invalidate(self, key: str):
        self._cache.pop(key, None)

    def clear(self):
        self._cache.clear()


# Global cache instance
cache = SimpleCache()


# ----- API Key Security -----

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify API key for protected endpoints."""
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    # Validate against configured API keys
    if api_key not in settings.valid_api_keys:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key


def run_migrations():
    """Run Alembic migrations on startup.

    Handles the case where database schema exists but alembic_version is out of sync
    by stamping with the parent revision and retrying.
    """
    import subprocess
    import sys

    print("Running database migrations...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout
        )
        if result.returncode == 0:
            print("Database migrations completed successfully")
        elif "already exists" in result.stderr or "DuplicateColumn" in result.stderr or "DuplicateTable" in result.stderr:
            # Database schema exists but alembic_version is out of sync
            # Stamp with the revision before our new migration and retry
            print("Database schema exists, syncing alembic version...")
            stamp_result = subprocess.run(
                [sys.executable, "-m", "alembic", "stamp", "20260121_content_hashes"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if stamp_result.returncode == 0:
                # Now run only the new migration
                retry_result = subprocess.run(
                    [sys.executable, "-m", "alembic", "upgrade", "head"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                if retry_result.returncode == 0:
                    print("Database migrations completed successfully (after stamp)")
                else:
                    print(f"Migration warning after stamp: {retry_result.stderr[-500:]}")
            else:
                print(f"Could not stamp database: {stamp_result.stderr}")
        else:
            print(f"Migration warning: {result.stderr[-500:]}")
    except subprocess.TimeoutExpired:
        print("Warning: Migration timed out (database may be unavailable)")
    except Exception as e:
        print(f"Warning: Could not run migrations: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    print("Starting The Bud Tracker...")
    print(f"Tracking {len(FUND_REGISTRY)} VC funds")

    # Run database migrations first
    run_migrations()

    # Seed funds in database
    try:
        async with get_session() as session:
            await seed_funds(session)
            print("Database seeded with tracked funds")
    except Exception as e:
        print(f"Warning: Could not seed database: {e}")

    # Start scheduler for automated hourly scraping
    try:
        setup_scheduler()
        print("Scheduler started - hourly scraping enabled")
    except Exception as e:
        print(f"Warning: Could not start scheduler: {e}")

    # FIX 2026-01: Start stuck scan monitor (detects silently crashed scans)
    try:
        await start_stuck_monitor()
        print("StuckScanMonitor started - detecting crashed scans")
    except Exception as e:
        print(f"Warning: Could not start StuckScanMonitor: {e}")

    yield

    # Graceful shutdown - close all resources
    print("Shutting down...")
    shutdown_scheduler()

    # FIX 2026-01: Stop stuck scan monitor
    try:
        await stop_stuck_monitor()
        print("StuckScanMonitor stopped")
    except Exception as e:
        print(f"Warning: Error stopping StuckScanMonitor: {e}")

    # Close database connections
    try:
        await close_db()
        print("Database connections closed")
    except Exception as e:
        print(f"Warning: Error closing database: {e}")

    # Close Brave API client
    try:
        await close_brave_client()
        print("Brave client closed")
    except Exception as e:
        print(f"Warning: Error closing Brave client: {e}")


app = FastAPI(
    title="The Bud Tracker",
    description="Monitor lead investor signals from elite VC firms",
    version="0.2.0",  # Version bump for optimizations
    lifespan=lifespan
)

# GZip compression for responses > 500 bytes
app.add_middleware(GZipMiddleware, minimum_size=500)

# CORS middleware for frontend
# Build allowed origins dynamically (includes production frontend if configured)
_allowed_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",
]
if settings.frontend_url:
    _allowed_origins.append(settings.frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Aircraft tracking router
from .aircraft import router as aircraft_router
app.include_router(aircraft_router)


# ----- Response Models -----

class FundResponse(BaseModel):
    slug: str
    name: str
    ingestion_url: str
    scraper_type: str


class ExtractionRequest(BaseModel):
    text: str
    source_url: Optional[str] = None
    fund_slug: Optional[str] = None


class BatchExtractionRequest(BaseModel):
    """Request for batch extraction of multiple articles."""
    articles: List[ExtractionRequest] = Field(..., min_length=1, max_length=10)


class ExtractionResponse(BaseModel):
    startup_name: str
    round_label: str
    amount: Optional[str]
    tracked_fund_is_lead: bool
    tracked_fund_name: Optional[str]
    tracked_fund_role: Optional[str]
    confidence_score: float
    reasoning_summary: str
    # NEW: Announcement verification (false positive prevention)
    is_new_announcement: bool = False  # FIX: Safe default
    announcement_evidence: Optional[str] = None  # FIX 4: Add missing field
    announcement_rejection_reason: Optional[str] = None


class PoolStatus(BaseModel):
    """Database connection pool status for monitoring."""
    pool_size: int
    max_overflow: int
    checked_in: int
    checked_out: int
    total_connections: int


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    tracked_funds: int
    implemented_scrapers: int
    pool_status: Optional[PoolStatus] = None


class TokenUsageBySource(BaseModel):
    """Token usage breakdown by source."""
    source: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    total_tokens: int
    cost_usd: float
    call_count: int


class TokenUsageByDay(BaseModel):
    """Token usage breakdown by day."""
    date: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    total_tokens: int
    cost_usd: float
    call_count: int


class TokenUsageResponse(BaseModel):
    """Token usage summary response."""
    period: str
    start_date: str
    end_date: str
    total_input_tokens: int
    total_output_tokens: int
    total_cache_read_tokens: int
    total_cache_write_tokens: int
    total_tokens: int
    total_cost_usd: float
    total_calls: int
    by_source: List[TokenUsageBySource]
    by_day: List[TokenUsageByDay]


class FounderResponse(BaseModel):
    """Founder information for API response."""
    name: str
    title: Optional[str] = None
    linkedin_url: Optional[str] = None


class DealResponse(BaseModel):
    id: str
    startup_name: str
    investor_roles: List[str]
    investment_stage: str
    amount_invested: str
    date: str
    next_steps: Optional[str] = None
    # AI classification fields
    enterprise_category: Optional[str] = None
    is_enterprise_ai: bool = False
    is_ai_deal: bool = False
    lead_investor: Optional[str] = None
    lead_partner: Optional[str] = None
    verification_snippet: Optional[str] = None
    lead_evidence_weak: bool = False  # True if snippet lacks "led by" but Claude said lead
    # Amount validation flags
    amount_needs_review: bool = False  # True if amount seems suspicious (e.g., $2B Series A)
    amount_review_reason: Optional[str] = None  # Why amount needs review
    # Company links
    company_website: Optional[str] = None
    company_linkedin: Optional[str] = None
    # Source tracking
    source_url: Optional[str] = None
    source_name: Optional[str] = None
    # Founders
    founders: Optional[List[FounderResponse]] = None


class PaginatedDealsResponse(BaseModel):
    """Paginated response for deals."""
    deals: List[DealResponse]
    total: int
    limit: int
    offset: int
    has_more: bool


# Valid values for round_type and enterprise_category
VALID_ROUND_TYPES = {
    "pre_seed", "seed", "seed_plus_series_a", "series_a", "series_b",
    "series_c", "series_d", "series_e_plus", "growth", "debt", "unknown"
}
VALID_ENTERPRISE_CATEGORIES = {
    "infrastructure", "security", "vertical_saas", "agentic", "data_intelligence",
    "consumer_ai", "gaming_ai", "social_ai", "crypto", "fintech", "healthcare",
    "hardware", "saas", "other", "not_ai"
}


class FounderInput(BaseModel):
    """Validated founder input for deal updates."""
    name: str = Field(..., min_length=1, max_length=200, description="Founder's full name")
    title: Optional[str] = Field(default=None, max_length=100, description="Title/role")
    linkedin_url: Optional[str] = Field(default=None, max_length=500, description="LinkedIn profile URL")

    @field_validator('linkedin_url')
    @classmethod
    def validate_linkedin_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate LinkedIn URL format if provided."""
        if v is None or v.strip() == "":
            return None
        v = v.strip()
        if not v.startswith(('http://', 'https://')):
            v = f"https://{v}"
        # Must be a LinkedIn profile URL
        if 'linkedin.com/in/' not in v.lower():
            raise ValueError("LinkedIn URL must be a profile URL (linkedin.com/in/...)")
        return v


class UpdateDealRequest(BaseModel):
    """Request body for updating a deal.

    FIX 2026-01: Added input validation to prevent malformed data injection.
    """
    company_name: Optional[str] = Field(default=None, max_length=255)
    website: Optional[str] = Field(default=None, max_length=500)
    linkedin_url: Optional[str] = Field(default=None, max_length=500)
    round_type: Optional[str] = Field(default=None, max_length=50)
    amount: Optional[str] = Field(default=None, max_length=100)
    announced_date: Optional[str] = Field(default=None, max_length=10)  # ISO format: YYYY-MM-DD
    is_lead_confirmed: Optional[bool] = None
    lead_partner_name: Optional[str] = Field(default=None, max_length=255)
    enterprise_category: Optional[str] = Field(default=None, max_length=50)
    is_enterprise_ai: Optional[bool] = None
    is_ai_deal: Optional[bool] = None  # FIX 2026-01: Allow updating AI classification
    founders: Optional[List[FounderInput]] = None  # Validated founder list
    # Amount validation flags (for manual flagging)
    amount_needs_review: Optional[bool] = None
    amount_review_reason: Optional[str] = Field(default=None, max_length=500)

    @field_validator('round_type')
    @classmethod
    def validate_round_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate round_type is a known value."""
        if v is None:
            return None
        v = v.strip().lower()
        if v not in VALID_ROUND_TYPES:
            raise ValueError(f"Invalid round_type '{v}'. Valid values: {sorted(VALID_ROUND_TYPES)}")
        return v

    @field_validator('enterprise_category')
    @classmethod
    def validate_enterprise_category(cls, v: Optional[str]) -> Optional[str]:
        """Validate enterprise_category is a known value."""
        if v is None:
            return None
        v = v.strip().lower()
        if v not in VALID_ENTERPRISE_CATEGORIES:
            raise ValueError(f"Invalid enterprise_category '{v}'. Valid values: {sorted(VALID_ENTERPRISE_CATEGORIES)}")
        return v

    @field_validator('website')
    @classmethod
    def validate_website(cls, v: Optional[str]) -> Optional[str]:
        """Validate website URL format if provided."""
        if v is None or v.strip() == "":
            return None
        v = v.strip()
        if not v.startswith(('http://', 'https://')):
            v = f"https://{v}"
        # Basic URL format check
        if '.' not in v or len(v) < 10:
            raise ValueError("Invalid website URL format")
        return v

    @field_validator('linkedin_url')
    @classmethod
    def validate_linkedin_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate LinkedIn URL format if provided."""
        if v is None or v.strip() == "":
            return None
        v = v.strip()
        if not v.startswith(('http://', 'https://')):
            v = f"https://{v}"
        # Must be a LinkedIn URL
        if 'linkedin.com/' not in v.lower():
            raise ValueError("URL must be a LinkedIn URL")
        return v

    @field_validator('announced_date')
    @classmethod
    def validate_announced_date(cls, v: Optional[str]) -> Optional[str]:
        """Validate date is in ISO format YYYY-MM-DD."""
        if v is None or v.strip() == "":
            return None
        v = v.strip()
        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            raise ValueError("Date must be in ISO format: YYYY-MM-DD")
        # Verify it's a valid date
        from datetime import datetime
        try:
            datetime.strptime(v, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Invalid date")
        return v


# ----- Endpoints -----

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint with pool monitoring.

    FIX 2026-01: Added pool_status to detect connection exhaustion early.
    Pool status is always fresh (not cached) for accurate monitoring.
    """
    # Get fresh pool status (don't cache - need real-time monitoring)
    try:
        pool_info = get_pool_status()
        pool_status = PoolStatus(
            pool_size=pool_info["pool_size"],
            max_overflow=pool_info["max_overflow"],
            checked_in=pool_info["checked_in"],
            checked_out=pool_info["checked_out"],
            total_connections=pool_info["total_connections"],
        )
    except Exception as e:
        logger.warning(f"Failed to get pool status: {e}")
        pool_status = None

    response = HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        tracked_funds=len(FUND_REGISTRY),
        implemented_scrapers=len(get_implemented_scrapers()),
        pool_status=pool_status,
    )

    return response


@app.get("/docs/claude")
async def get_claude_docs():
    """Return CLAUDE.md content for documentation page."""
    import os
    # Try multiple paths for local dev and Docker deployment
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "CLAUDE.md"),  # Local dev
        "/app/CLAUDE.md",  # Docker/Railway
        "CLAUDE.md",  # Current working dir
    ]
    for claude_path in possible_paths:
        if os.path.exists(claude_path):
            try:
                with open(claude_path, "r") as f:
                    content = f.read()
                updated_at = os.path.getmtime(claude_path)
                return {"content": content, "updated_at": updated_at}
            except Exception:
                continue
    return {"content": "# Documentation not found", "updated_at": 0}


@app.get("/usage/tokens", response_model=TokenUsageResponse)
async def get_token_usage(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    _: str = Depends(verify_api_key)
):
    """
    Get token usage breakdown for cost analysis.

    Returns:
    - Total tokens used (input, output, cache)
    - Cost breakdown by source (brave_search, techcrunch, a16z, etc.)
    - Cost breakdown by day

    Pricing (Claude 3.5 Haiku):
    - Input: $0.25 per 1M tokens
    - Output: $1.25 per 1M tokens
    - Cache read: $0.025 per 1M tokens
    - Cache write: $0.3125 per 1M tokens
    """
    try:
        from .archivist.models import TokenUsage
        from sqlalchemy import cast, Date

        async with get_session() as session:
            # Calculate date range (use naive datetime for DB compatibility)
            end_date = datetime.now(timezone.utc).replace(tzinfo=None)
            start_date = end_date - timedelta(days=days)

            # Query all token usage in date range
            stmt = select(TokenUsage).where(TokenUsage.timestamp >= start_date)
            result = await session.execute(stmt)
            rows = result.scalars().all()

            # Aggregate totals
            total_input = sum(r.input_tokens for r in rows)
            total_output = sum(r.output_tokens for r in rows)
            total_cache_read = sum(r.cache_read_tokens for r in rows)
            total_cache_write = sum(r.cache_write_tokens for r in rows)
            total_cost = sum(r.estimated_cost_usd for r in rows)

            # Group by source
            by_source_dict: Dict[str, dict] = {}
            for r in rows:
                if r.source_name not in by_source_dict:
                    by_source_dict[r.source_name] = {
                        "input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0, "count": 0
                    }
                by_source_dict[r.source_name]["input"] += r.input_tokens
                by_source_dict[r.source_name]["output"] += r.output_tokens
                by_source_dict[r.source_name]["cache_read"] += r.cache_read_tokens
                by_source_dict[r.source_name]["cache_write"] += r.cache_write_tokens
                by_source_dict[r.source_name]["cost"] += r.estimated_cost_usd
                by_source_dict[r.source_name]["count"] += 1

            by_source = [
                TokenUsageBySource(
                    source=source,
                    input_tokens=data["input"],
                    output_tokens=data["output"],
                    cache_read_tokens=data["cache_read"],
                    cache_write_tokens=data["cache_write"],
                    total_tokens=data["input"] + data["output"],
                    cost_usd=round(data["cost"], 4),
                    call_count=data["count"]
                )
                for source, data in sorted(by_source_dict.items(), key=lambda x: x[1]["cost"], reverse=True)
            ]

            # Group by day
            by_day_dict: Dict[str, dict] = {}
            for r in rows:
                day_str = r.timestamp.strftime("%Y-%m-%d")
                if day_str not in by_day_dict:
                    by_day_dict[day_str] = {
                        "input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0, "count": 0
                    }
                by_day_dict[day_str]["input"] += r.input_tokens
                by_day_dict[day_str]["output"] += r.output_tokens
                by_day_dict[day_str]["cache_read"] += r.cache_read_tokens
                by_day_dict[day_str]["cache_write"] += r.cache_write_tokens
                by_day_dict[day_str]["cost"] += r.estimated_cost_usd
                by_day_dict[day_str]["count"] += 1

            by_day = [
                TokenUsageByDay(
                    date=day,
                    input_tokens=data["input"],
                    output_tokens=data["output"],
                    cache_read_tokens=data["cache_read"],
                    cache_write_tokens=data["cache_write"],
                    total_tokens=data["input"] + data["output"],
                    cost_usd=round(data["cost"], 4),
                    call_count=data["count"]
                )
                for day, data in sorted(by_day_dict.items(), reverse=True)
            ]

            return TokenUsageResponse(
                period=f"last_{days}_days",
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                total_input_tokens=total_input,
                total_output_tokens=total_output,
                total_cache_read_tokens=total_cache_read,
                total_cache_write_tokens=total_cache_write,
                total_tokens=total_input + total_output,
                total_cost_usd=round(total_cost, 4),
                total_calls=len(rows),
                by_source=by_source,
                by_day=by_day
            )

    except Exception as e:
        logger.error(f"Failed to get token usage: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get token usage: {str(e)}")


@app.get("/funds", response_model=List[FundResponse])
async def list_funds():
    """
    List all tracked VC funds (cached for 1 hour).

    This data is static, so caching is safe.
    """
    cached = cache.get("funds")
    if cached:
        return cached

    funds = [
        FundResponse(
            slug=f.slug,
            name=f.name,
            ingestion_url=f.ingestion_url,
            scraper_type=f.scraper_type.value
        )
        for f in get_all_funds()
    ]

    cache.set("funds", funds, ttl_seconds=3600)
    return funds


@app.get("/funds/{slug}", response_model=FundResponse)
async def get_fund(slug: str):
    """Get details for a specific fund."""
    if slug not in FUND_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Fund '{slug}' not found")

    f = FUND_REGISTRY[slug]
    return FundResponse(
        slug=f.slug,
        name=f.name,
        ingestion_url=f.ingestion_url,
        scraper_type=f.scraper_type.value
    )


@app.get("/deals", response_model=PaginatedDealsResponse)
async def list_deals(
    fund_slug: Optional[str] = Query(None, description="Filter by fund slug"),
    stage: Optional[str] = Query(None, description="Filter by investment stage"),
    is_lead: Optional[bool] = Query(None, description="Filter by lead status (True = tracked fund led)"),
    is_ai_deal: Optional[bool] = Query(None, description="Filter for all AI deals (True = any AI, enterprise or consumer)"),
    is_enterprise_ai: Optional[bool] = Query(None, description="Filter for Enterprise AI only (True = B2B AI)"),
    enterprise_category: Optional[str] = Query(None, description="Filter by category (infrastructure, security, vertical_saas, agentic, data_intelligence, consumer_ai, gaming_ai, social_ai)"),
    search: Optional[str] = Query(None, description="Search by company name"),
    sort_direction: str = Query("desc", description="Sort by announced date: 'asc' or 'desc'"),
    limit: int = Query(50, ge=1, le=200, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
):
    """
    List tracked deals with pagination.

    AI FILTERS:
    - is_ai_deal=true: All AI deals (enterprise + consumer)
    - is_enterprise_ai=true: Only B2B Enterprise AI deals
    - enterprise_category: Filter by specific category

    LEAD FILTER:
    - is_lead=true: Only deals where tracked fund LED the round
    """
    try:
        async with get_session() as session:
            # Build base query
            base_stmt = select(Deal)

            # Apply filters
            if stage:
                base_stmt = base_stmt.where(Deal.round_type == stage)
            if is_lead is not None:
                # FIX: Filter by whether a TRACKED FUND is the lead, not just is_lead_confirmed
                # is_lead_confirmed means "we know who led" (e.g., NEA)
                # But the filter should mean "a tracked fund led the round"
                if is_lead:
                    # Only show deals where a TRACKED FUND is the lead
                    base_stmt = base_stmt.where(
                        Deal.id.in_(
                            select(DealInvestor.deal_id)
                            .where(DealInvestor.is_lead == True)
                            .where(DealInvestor.is_tracked_fund == True)
                        )
                    )
                else:
                    # Show deals where NO tracked fund is the lead
                    base_stmt = base_stmt.where(
                        ~Deal.id.in_(
                            select(DealInvestor.deal_id)
                            .where(DealInvestor.is_lead == True)
                            .where(DealInvestor.is_tracked_fund == True)
                        )
                    )
            if is_ai_deal is not None:
                base_stmt = base_stmt.where(Deal.is_ai_deal == is_ai_deal)
            if is_enterprise_ai is not None:
                base_stmt = base_stmt.where(Deal.is_enterprise_ai == is_enterprise_ai)
            if enterprise_category:
                base_stmt = base_stmt.where(Deal.enterprise_category == enterprise_category)

            # Filter by fund_slug - join through DealInvestor to Fund
            if fund_slug:
                base_stmt = base_stmt.join(DealInvestor, Deal.id == DealInvestor.deal_id).join(
                    Fund, DealInvestor.fund_id == Fund.id
                ).where(Fund.slug == fund_slug).distinct()

            # Search by company name (case-insensitive)
            if search:
                base_stmt = base_stmt.join(PortfolioCompany, Deal.company_id == PortfolioCompany.id).where(
                    PortfolioCompany.name.ilike(f"%{search}%")
                )

            # Get total count for pagination
            count_stmt = select(func.count()).select_from(base_stmt.subquery())
            total_result = await session.execute(count_stmt)
            total = total_result.scalar() or 0

            # Query deals with eager loading
            stmt = base_stmt.options(
                selectinload(Deal.company),
                selectinload(Deal.investors),
                selectinload(Deal.articles),
            )

            # Apply sort direction
            if sort_direction == "asc":
                stmt = stmt.order_by(nullsfirst(Deal.announced_date.asc()), Deal.created_at.asc())
            else:
                stmt = stmt.order_by(nullslast(Deal.announced_date.desc()), Deal.created_at.desc())

            stmt = stmt.offset(offset).limit(limit)

            result = await session.execute(stmt)
            deals = result.scalars().all()

            # Build responses
            responses = []
            for deal in deals:
                # Build investor roles list and find lead investor
                investor_roles = []
                lead_investor_name = None
                for inv in deal.investors:
                    if inv.is_lead:
                        investor_roles.append("lead")
                        if inv.is_tracked_fund:
                            lead_investor_name = inv.investor_name
                    elif inv.is_tracked_fund:
                        investor_roles.append("non_lead")
                    else:
                        investor_roles.append("non_lead")

                # Handle stealth (no investors tracked)
                if not investor_roles:
                    investor_roles = ["stealth"]

                # Get company name
                company_name = deal.company.name if deal.company else "Unknown"

                # Parse founders from JSON - only use REAL LinkedIn URLs
                founders = None
                if deal.founders_json:
                    try:
                        founders_data = json.loads(deal.founders_json)
                        founders = []
                        for f in founders_data:
                            founder_name = f.get("name", "Unknown")
                            # Only use LinkedIn URL if it's a real direct profile URL
                            linkedin_url = f.get("linkedin_url")
                            if not is_valid_url(linkedin_url):
                                linkedin_url = None  # Return null, not a search URL
                            founders.append(FounderResponse(
                                name=founder_name,
                                title=f.get("title"),
                                linkedin_url=linkedin_url,
                            ))
                    except json.JSONDecodeError as e:
                        # FIX #18: Log JSON parsing failures with context
                        logger.warning("Invalid founders_json for deal %s: %s", deal.id, e)
                        founders = None

                # Get source info from articles
                source_url = None
                source_name = None
                if deal.articles:
                    first_article = deal.articles[0]
                    source_url = first_article.url
                    source_name = first_article.source_fund_slug or first_article.title

                # Company links - only use REAL URLs, not search URLs
                db_website = deal.company.website if deal.company else None
                db_linkedin = deal.company.linkedin_url if deal.company else None

                # Only return URL if it's a valid direct URL
                company_website = db_website if is_valid_url(db_website) else None
                company_linkedin = db_linkedin if is_valid_url(db_linkedin) else None

                responses.append(DealResponse(
                    id=str(deal.id),
                    startup_name=company_name,
                    investor_roles=investor_roles,
                    investment_stage=deal.round_type,
                    amount_invested=deal.amount or "Undisclosed",
                    date=deal.announced_date.isoformat() if deal.announced_date else "",
                    next_steps=None,
                    # AI classification fields
                    enterprise_category=deal.enterprise_category,
                    is_enterprise_ai=deal.is_enterprise_ai,
                    is_ai_deal=getattr(deal, 'is_ai_deal', deal.is_enterprise_ai),  # Fallback for older deals
                    lead_investor=lead_investor_name,
                    lead_partner=deal.lead_partner_name,
                    verification_snippet=deal.verification_snippet,
                    lead_evidence_weak=getattr(deal, 'lead_evidence_weak', False),
                    # Amount validation flags
                    amount_needs_review=getattr(deal, 'amount_needs_review', False),
                    amount_review_reason=getattr(deal, 'amount_review_reason', None),
                    # Company links (with fallback generation)
                    company_website=company_website,
                    company_linkedin=company_linkedin,
                    # Source tracking
                    source_url=source_url,
                    source_name=source_name,
                    # Founders (with LinkedIn search URLs)
                    founders=founders,
                ))

            return PaginatedDealsResponse(
                deals=responses,
                total=total,
                limit=limit,
                offset=offset,
                has_more=(offset + len(responses)) < total
            )

    except Exception as e:
        # FIX #13: Log error securely, don't expose details to client
        logger.error("Database query failed: %s", e, exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable"
        )


@app.get("/deals/export")
async def export_deals_csv(
    fund_slug: Optional[str] = Query(None, description="Filter by fund slug"),
    stage: Optional[str] = Query(None, description="Filter by investment stage"),
    is_lead: Optional[bool] = Query(None, description="Filter by lead status"),
    is_ai_deal: Optional[bool] = Query(None, description="Filter for all AI deals"),
    is_enterprise_ai: Optional[bool] = Query(None, description="Filter for Enterprise AI only"),
    enterprise_category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search by company name"),
):
    """
    Export deals as CSV file.

    Applies same filters as /deals endpoint.
    Returns a downloadable CSV file.
    """
    import csv
    import io
    from fastapi.responses import StreamingResponse

    try:
        async with get_session() as session:
            # Build query with filters
            stmt = (
                select(Deal)
                .options(
                    selectinload(Deal.company),
                    selectinload(Deal.investors),
                )
            )

            if stage:
                stmt = stmt.where(Deal.round_type == stage)
            if is_lead is not None:
                # FIX: Filter by whether a TRACKED FUND is the lead
                if is_lead:
                    stmt = stmt.where(
                        Deal.id.in_(
                            select(DealInvestor.deal_id)
                            .where(DealInvestor.is_lead == True)
                            .where(DealInvestor.is_tracked_fund == True)
                        )
                    )
                else:
                    stmt = stmt.where(
                        ~Deal.id.in_(
                            select(DealInvestor.deal_id)
                            .where(DealInvestor.is_lead == True)
                            .where(DealInvestor.is_tracked_fund == True)
                        )
                    )
            if is_ai_deal is not None:
                stmt = stmt.where(Deal.is_ai_deal == is_ai_deal)
            if is_enterprise_ai is not None:
                stmt = stmt.where(Deal.is_enterprise_ai == is_enterprise_ai)
            if enterprise_category:
                stmt = stmt.where(Deal.enterprise_category == enterprise_category)

            # Search by company name
            if search:
                stmt = stmt.join(PortfolioCompany, Deal.company_id == PortfolioCompany.id).where(
                    PortfolioCompany.name.ilike(f"%{search}%")
                )

            # Filter by fund_slug - join through DealInvestor to Fund
            if fund_slug:
                stmt = stmt.join(DealInvestor, Deal.id == DealInvestor.deal_id).join(
                    Fund, DealInvestor.fund_id == Fund.id
                ).where(Fund.slug == fund_slug).distinct()

            stmt = stmt.order_by(nullslast(Deal.announced_date.desc()), Deal.created_at.desc())
            result = await session.execute(stmt)
            deals = result.scalars().all()

            # Create CSV in memory
            output = io.StringIO()
            writer = csv.writer(output)

            # Header row
            writer.writerow([
                "Company",
                "Round",
                "Amount",
                "Date",
                "Lead Investor",
                "Lead Partner",
                "Enterprise Category",
                "Is Enterprise AI",
                "Verification Snippet",
            ])

            # Data rows
            for deal in deals:
                # Find lead investor
                lead_investor = None
                for inv in deal.investors:
                    if inv.is_lead and inv.is_tracked_fund:
                        lead_investor = inv.investor_name
                        break

                writer.writerow([
                    deal.company.name if deal.company else "Unknown",
                    deal.round_type,
                    deal.amount or "Undisclosed",
                    deal.announced_date.isoformat() if deal.announced_date else "",
                    lead_investor or "",
                    deal.lead_partner_name or "",
                    deal.enterprise_category or "",
                    "Yes" if deal.is_enterprise_ai else "No",
                    deal.verification_snippet or "",
                ])

            output.seek(0)

            # Return as streaming response with CSV content type
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=bud-tracker-deals-{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
                }
            )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/deals/{deal_id}", response_model=DealResponse)
async def update_deal_endpoint(
    deal_id: int,
    request: UpdateDealRequest,
):
    """
    Update a deal and its associated company.

    All fields are optional - only provided fields will be updated.
    """
    from .archivist.storage import update_deal
    from datetime import date as date_type

    try:
        async with get_session() as session:
            # Parse announced_date if provided
            announced_date = None
            if request.announced_date:
                try:
                    announced_date = date_type.fromisoformat(request.announced_date)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid date format: {request.announced_date}. Use YYYY-MM-DD."
                    )

            # Convert founders list to JSON string if provided
            founders_json = None
            if request.founders is not None:
                # Convert FounderInput models to dicts for JSON serialization
                founders_json = json.dumps([f.model_dump(exclude_none=True) for f in request.founders])

            # Update the deal
            deal = await update_deal(
                session=session,
                deal_id=deal_id,
                company_name=request.company_name,
                website=request.website,
                linkedin_url=request.linkedin_url,
                round_type=request.round_type,
                amount=request.amount,
                announced_date=announced_date,
                is_lead_confirmed=request.is_lead_confirmed,
                lead_partner_name=request.lead_partner_name,
                enterprise_category=request.enterprise_category,
                is_enterprise_ai=request.is_enterprise_ai,
                is_ai_deal=request.is_ai_deal,
                founders_json=founders_json,
                amount_needs_review=request.amount_needs_review,
                amount_review_reason=request.amount_review_reason,
            )

            if not deal:
                raise HTTPException(status_code=404, detail=f"Deal {deal_id} not found")

            await session.commit()

            # Reload with relationships
            stmt = (
                select(Deal)
                .where(Deal.id == deal_id)
                .options(
                    selectinload(Deal.company),
                    selectinload(Deal.investors),
                    selectinload(Deal.articles),
                )
            )
            result = await session.execute(stmt)
            deal = result.scalar_one()

            # Build response
            investor_roles = []
            lead_investor_name = None
            for inv in deal.investors:
                if inv.is_lead:
                    investor_roles.append("lead")
                    if inv.is_tracked_fund:
                        lead_investor_name = inv.investor_name
                else:
                    investor_roles.append("non_lead")

            if not investor_roles:
                investor_roles = ["stealth"]

            # Parse founders
            founders = None
            if deal.founders_json:
                try:
                    founders_data = json.loads(deal.founders_json)
                    founders = [
                        FounderResponse(
                            name=f.get("name", "Unknown"),
                            title=f.get("title"),
                            linkedin_url=f.get("linkedin_url") if is_valid_url(f.get("linkedin_url")) else None,
                        )
                        for f in founders_data
                    ]
                except json.JSONDecodeError:
                    founders = None

            # Get source info
            source_url = None
            source_name = None
            if deal.articles:
                first_article = deal.articles[0]
                source_url = first_article.url
                source_name = first_article.source_fund_slug or first_article.title

            return DealResponse(
                id=str(deal.id),
                startup_name=deal.company.name if deal.company else "Unknown",
                investor_roles=investor_roles,
                investment_stage=deal.round_type,
                amount_invested=deal.amount or "Undisclosed",
                date=deal.announced_date.isoformat() if deal.announced_date else "",
                next_steps=None,
                enterprise_category=deal.enterprise_category,
                is_enterprise_ai=deal.is_enterprise_ai,
                is_ai_deal=getattr(deal, 'is_ai_deal', deal.is_enterprise_ai),
                lead_investor=lead_investor_name,
                lead_partner=deal.lead_partner_name,
                verification_snippet=deal.verification_snippet,
                lead_evidence_weak=getattr(deal, 'lead_evidence_weak', False),
                amount_needs_review=getattr(deal, 'amount_needs_review', False),
                amount_review_reason=getattr(deal, 'amount_review_reason', None),
                company_website=deal.company.website if deal.company and is_valid_url(deal.company.website) else None,
                company_linkedin=deal.company.linkedin_url if deal.company and is_valid_url(deal.company.linkedin_url) else None,
                source_url=source_url,
                source_name=source_name,
                founders=founders,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update deal %s: %s", deal_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== Company Aliases ====================

class CompanyAliasResponse(BaseModel):
    id: int
    company_id: int
    alias_name: str
    alias_type: str
    effective_date: Optional[str] = None
    created_at: str


VALID_ALIAS_TYPES = {"rebrand", "dba", "acquired_name", "typo"}


class CompanyAliasCreate(BaseModel):
    alias_name: str = Field(..., min_length=1, max_length=255)
    alias_type: str = Field(default="rebrand", pattern="^(rebrand|dba|acquired_name|typo)$")
    effective_date: Optional[str] = None


class CompanyAliasListResponse(BaseModel):
    company_id: int
    company_name: str
    aliases: List[CompanyAliasResponse]


@app.get("/companies/{company_id}/aliases", response_model=CompanyAliasListResponse)
async def get_company_aliases_endpoint(
    company_id: int,
):
    """
    Get all aliases for a company.

    Aliases track rebrands, DBAs, and alternative names.
    For example: "Bedrock Security" → "Bedrock Data" (rebrand).
    """
    from .archivist.storage import get_company_aliases

    try:
        async with get_session() as session:
            # Get company
            company = await session.get(PortfolioCompany, company_id)
            if not company:
                raise HTTPException(status_code=404, detail="Company not found")

            # Get aliases
            aliases = await get_company_aliases(session, company_id)

            return CompanyAliasListResponse(
                company_id=company_id,
                company_name=company.name,
                aliases=[
                    CompanyAliasResponse(
                        id=a.id,
                        company_id=a.company_id,
                        alias_name=a.alias_name,
                        alias_type=a.alias_type,
                        effective_date=a.effective_date.isoformat() if a.effective_date else None,
                        created_at=a.created_at.isoformat() if a.created_at else "",
                    )
                    for a in aliases
                ]
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get company aliases: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/companies/{company_id}/aliases", response_model=CompanyAliasResponse)
async def create_company_alias_endpoint(
    company_id: int,
    alias: CompanyAliasCreate,
):
    """
    Create a new alias for a company.

    Alias types:
    - rebrand: Official name change (e.g., Bedrock Security → Bedrock Data)
    - dba: Doing business as (legal name differs from brand)
    - acquired_name: Name before acquisition
    - typo: Common misspelling to handle
    """
    from .archivist.storage import create_company_alias
    from datetime import date

    try:
        async with get_session() as session:
            # Verify company exists
            company = await session.get(PortfolioCompany, company_id)
            if not company:
                raise HTTPException(status_code=404, detail="Company not found")

            # Parse effective date if provided
            effective_date = None
            if alias.effective_date:
                try:
                    effective_date = date.fromisoformat(alias.effective_date)
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

            # Create alias
            new_alias = await create_company_alias(
                session,
                company_id=company_id,
                alias_name=alias.alias_name,
                alias_type=alias.alias_type,
                effective_date=effective_date,
            )

            if not new_alias:
                raise HTTPException(status_code=409, detail="Alias already exists")

            await session.commit()

            return CompanyAliasResponse(
                id=new_alias.id,
                company_id=new_alias.company_id,
                alias_name=new_alias.alias_name,
                alias_type=new_alias.alias_type,
                effective_date=new_alias.effective_date.isoformat() if new_alias.effective_date else None,
                created_at=new_alias.created_at.isoformat() if new_alias.created_at else "",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to create company alias: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/companies/{company_id}/aliases/{alias_id}")
async def delete_company_alias_endpoint(
    company_id: int,
    alias_id: int,
):
    """Delete a company alias."""
    from .archivist.models import CompanyAlias

    try:
        async with get_session() as session:
            # Get alias
            alias = await session.get(CompanyAlias, alias_id)
            if not alias:
                raise HTTPException(status_code=404, detail="Alias not found")

            if alias.company_id != company_id:
                raise HTTPException(status_code=400, detail="Alias does not belong to this company")

            await session.delete(alias)
            await session.commit()

            return {"status": "deleted", "alias_id": alias_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete company alias: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class StealthDetectionResponse(BaseModel):
    id: int
    fund_slug: str
    detected_url: str
    detected_at: str
    company_name: Optional[str]
    is_confirmed: bool
    notes: Optional[str]


class StealthDetectionsListResponse(BaseModel):
    detections: List[StealthDetectionResponse]
    total: int


@app.get("/stealth-detections", response_model=StealthDetectionsListResponse)
async def list_stealth_detections(
    fund_slug: Optional[str] = Query(None, description="Filter by fund slug"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """
    List stealth portfolio detections.

    Shows companies added to fund portfolios without press releases.
    """
    try:
        async with get_session() as session:
            stmt = select(StealthDetection).order_by(StealthDetection.detected_at.desc())

            if fund_slug:
                stmt = stmt.where(StealthDetection.fund_slug == fund_slug)

            # Get total count (direct count avoids subquery overhead)
            count_stmt = select(func.count(StealthDetection.id))
            if fund_slug:
                count_stmt = count_stmt.where(StealthDetection.fund_slug == fund_slug)
            total_result = await session.execute(count_stmt)
            total = total_result.scalar() or 0

            # Apply pagination
            stmt = stmt.offset(offset).limit(limit)
            result = await session.execute(stmt)
            detections = result.scalars().all()

            return StealthDetectionsListResponse(
                detections=[
                    StealthDetectionResponse(
                        id=d.id,
                        fund_slug=d.fund_slug,
                        detected_url=d.detected_url,
                        detected_at=d.detected_at.isoformat(),
                        company_name=d.company_name,
                        is_confirmed=d.is_confirmed,
                        notes=d.notes,
                    )
                    for d in detections
                ],
                total=total,
            )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# =============================================================================
# STEALTH SIGNALS (Pre-Funding Detection) Endpoints
# =============================================================================

class StealthSignalResponse(BaseModel):
    """Response model for a stealth signal."""
    id: int
    company_name: str
    source: str
    source_url: str
    score: int
    signals: dict
    metadata: dict
    spotted_at: str
    dismissed: bool
    converted_deal_id: Optional[int]
    created_at: str


class StealthSignalsListResponse(BaseModel):
    """Paginated list of stealth signals."""
    signals: List[StealthSignalResponse]
    total: int


class StealthSignalStatsResponse(BaseModel):
    """Statistics for stealth signals."""
    total: int
    by_source: dict
    avg_score: float
    converted: int


@app.get("/stealth-signals", response_model=StealthSignalsListResponse)
async def list_stealth_signals(
    source: Optional[str] = Query(None, description="Filter by source (hackernews, ycombinator, github, linkedin, delaware)"),
    min_score: int = Query(0, ge=0, le=100, description="Minimum score threshold"),
    include_dismissed: bool = Query(False, description="Include dismissed signals"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    List pre-funding stealth signals.

    These are early-stage company signals from sources that detect
    startups before they announce funding (YC batch, GitHub trending,
    HN launches, LinkedIn stealth hiring, Delaware filings).
    """
    try:
        from .archivist.stealth_storage import get_stealth_signals

        async with get_session() as session:
            signals, total = await get_stealth_signals(
                session=session,
                source=source,
                min_score=min_score,
                include_dismissed=include_dismissed,
                limit=limit,
                offset=offset,
            )

            return StealthSignalsListResponse(
                signals=[
                    StealthSignalResponse(
                        id=s.id,
                        company_name=s.company_name,
                        source=s.source,
                        source_url=s.source_url,
                        score=s.score,
                        signals=s.signals or {},
                        metadata=s.metadata_json or {},
                        spotted_at=s.spotted_at.isoformat() if s.spotted_at else "",
                        dismissed=s.dismissed,
                        converted_deal_id=s.converted_deal_id,
                        created_at=s.created_at.isoformat() if s.created_at else "",
                    )
                    for s in signals
                ],
                total=total,
            )

    except Exception as e:
        logger.error("Failed to list stealth signals: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/stealth-signals/stats", response_model=StealthSignalStatsResponse)
async def get_stealth_signal_stats(
    include_dismissed: bool = Query(False, description="Include dismissed signals in stats"),
):
    """
    Get aggregate statistics for stealth signals.

    Returns total count, counts by source, average score, and conversion count.
    """
    try:
        from .archivist.stealth_storage import get_stealth_stats

        async with get_session() as session:
            stats = await get_stealth_stats(
                session=session,
                include_dismissed=include_dismissed,
            )

            return StealthSignalStatsResponse(
                total=stats["total"],
                by_source=stats["by_source"],
                avg_score=stats["avg_score"],
                converted=stats["converted"],
            )

    except Exception as e:
        logger.error("Failed to get stealth signal stats: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/stealth-signals/{signal_id}/dismiss")
async def dismiss_stealth_signal(
    signal_id: int,
    _: str = Depends(verify_api_key),
):
    """
    Dismiss a stealth signal (hide from default list).

    Dismissed signals can be restored with the undismiss endpoint.
    """
    try:
        from .archivist.stealth_storage import dismiss_signal

        async with get_session() as session:
            success = await dismiss_signal(session, signal_id)
            await session.commit()

            if not success:
                raise HTTPException(status_code=404, detail="Signal not found")

            return {"success": True, "signal_id": signal_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to dismiss stealth signal: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/stealth-signals/{signal_id}/undismiss")
async def undismiss_stealth_signal(
    signal_id: int,
    _: str = Depends(verify_api_key),
):
    """
    Undismiss a stealth signal (restore to default list).
    """
    try:
        from .archivist.stealth_storage import undismiss_signal

        async with get_session() as session:
            success = await undismiss_signal(session, signal_id)
            await session.commit()

            if not success:
                raise HTTPException(status_code=404, detail="Signal not found")

            return {"success": True, "signal_id": signal_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to undismiss stealth signal: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/stealth-signals/{signal_id}/link/{deal_id}")
async def link_signal_to_deal(
    signal_id: int,
    deal_id: int,
    _: str = Depends(verify_api_key),
):
    """
    Link a stealth signal to a deal.

    Use this when a company from the stealth signals announces funding.
    This tracks which pre-funding signals correctly predicted deals.
    """
    try:
        from .archivist.stealth_storage import link_to_deal

        async with get_session() as session:
            success = await link_to_deal(session, signal_id, deal_id)
            await session.commit()

            if not success:
                raise HTTPException(
                    status_code=404,
                    detail="Signal or deal not found"
                )

            return {"success": True, "signal_id": signal_id, "deal_id": deal_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to link stealth signal to deal: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/extract", response_model=ExtractionResponse)
async def extract_funding_deal(
    request: ExtractionRequest,
    api_key: str = Depends(verify_api_key)
):
    """
    Extract funding deal information from article text.

    Uses Claude 3.5 Sonnet with chain-of-thought reasoning.
    """
    fund_context = None
    if request.fund_slug and request.fund_slug in FUND_REGISTRY:
        fund_context = FUND_REGISTRY[request.fund_slug]

    # Set extraction context for token logging (use fund_slug if provided, else 'api_extract')
    source_name = request.fund_slug if request.fund_slug else "api_extract"
    set_extraction_context(source_name)

    try:
        result = await extract_deal(
            article_text=request.text,
            source_url=request.source_url or "",
            source_name=source_name,
            fund_context=fund_context
        )

        return ExtractionResponse(
            startup_name=result.startup_name,
            round_label=result.round_label.value,
            amount=result.amount,
            tracked_fund_is_lead=result.tracked_fund_is_lead,
            tracked_fund_name=result.tracked_fund_name,
            tracked_fund_role=result.tracked_fund_role.value if result.tracked_fund_role else None,
            confidence_score=result.confidence_score,
            reasoning_summary=result.reasoning.final_reasoning[:500],
            is_new_announcement=result.is_new_announcement,
            announcement_evidence=result.announcement_evidence,
            announcement_rejection_reason=result.announcement_rejection_reason,
        )
    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        clear_extraction_context()


@app.post("/extract/batch", response_model=List[ExtractionResponse])
async def extract_funding_deals_batch(
    request: BatchExtractionRequest,
    api_key: str = Depends(verify_api_key)
):
    """
    Extract funding deals from multiple articles concurrently.

    OPTIMIZED:
    - Process up to 10 articles in parallel
    - Shared prompt cache across extractions
    - Early exit for non-funding content

    Maximum 10 articles per request.
    """
    # Convert to format expected by batch extractor
    articles = [
        {
            "text": article.text,
            "source_url": article.source_url or "",
            "fund_slug": article.fund_slug
        }
        for article in request.articles
    ]

    # Set extraction context for token logging
    set_extraction_context("api_batch")

    try:
        results = await extract_deal_batch(articles, max_concurrent=5)

        responses = []
        for result in results:
            # Skip None results (failed extractions or duplicates)
            if result is None:
                continue
            responses.append(ExtractionResponse(
                startup_name=result.startup_name,
                round_label=result.round_label.value,
                amount=result.amount,
                tracked_fund_is_lead=result.tracked_fund_is_lead,
                tracked_fund_name=result.tracked_fund_name,
                tracked_fund_role=result.tracked_fund_role.value if result.tracked_fund_role else None,
                confidence_score=result.confidence_score,
                reasoning_summary=result.reasoning.final_reasoning[:500],
                is_new_announcement=result.is_new_announcement,
                announcement_evidence=result.announcement_evidence,
                announcement_rejection_reason=result.announcement_rejection_reason,
            ))

        return responses

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        clear_extraction_context()


# ----- Scan Job Helpers for API Triggers -----


async def create_scan_job(trigger: str = "api") -> tuple[int, str]:
    """Create a ScanJob record for API-triggered scans.

    Returns:
        Tuple of (scan_job_db_id, job_id string)
    """
    from .archivist.models import ScanJob

    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    async with get_session() as session:
        scan_job = ScanJob(
            job_id=job_id,
            status="running",
            trigger=trigger,
        )
        session.add(scan_job)
        await session.commit()
        await session.refresh(scan_job)
        logger.info(f"[{job_id}] Created ScanJob record (id={scan_job.id}, trigger={trigger})")
        return scan_job.id, job_id


async def update_scan_job(
    scan_job_id: int,
    status: str,
    stats: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    start_time: Optional[datetime] = None,
) -> None:
    """Update ScanJob with final stats.

    Args:
        scan_job_id: Database ID of the ScanJob
        status: Final status (success, failed)
        stats: Optional dict with keys like articles_found, deals_saved, etc.
        error: Optional error message if status is failed
        start_time: Optional start time to calculate duration
    """
    from .archivist.models import ScanJob

    try:
        async with get_session() as session:
            scan_job = await session.get(ScanJob, scan_job_id)
            if not scan_job:
                logger.warning(f"ScanJob {scan_job_id} not found for update")
                return

            scan_job.status = status
            scan_job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

            if start_time:
                duration = (datetime.now(timezone.utc) - start_time.replace(tzinfo=timezone.utc)).total_seconds()
                scan_job.duration_seconds = duration

            if stats:
                scan_job.total_articles_found = stats.get("articles_found", 0)
                scan_job.total_deals_extracted = stats.get("deals_extracted", 0)
                scan_job.total_deals_saved = stats.get("deals_saved", 0)
                scan_job.total_duplicates_skipped = stats.get("articles_skipped_duplicate", 0)
                scan_job.total_errors = stats.get("errors", 0)
                scan_job.lead_deals_found = stats.get("lead_deals", 0)
                scan_job.enterprise_ai_deals_found = stats.get("enterprise_ai_deals", 0)

                # Store source-specific results as JSON
                if "source_results" in stats:
                    scan_job.source_results_json = json.dumps(stats["source_results"], default=str)

            if error:
                scan_job.error_message = error

            await session.commit()
            logger.info(f"Updated ScanJob {scan_job_id}: status={status}")
    except Exception as e:
        logger.warning(f"Failed to update ScanJob {scan_job_id}: {e}")


# ----- Scraping Endpoints -----

class ScraperStatusResponse(BaseModel):
    implemented: List[str]
    not_implemented: List[str]
    total_funds: int


class ScrapeResponse(BaseModel):
    fund_slug: str
    articles_found: int
    articles_skipped_duplicate: int = 0
    articles_rejected_not_announcement: int = 0  # NEW: Track false positive rejections
    deals_extracted: int = 0
    deals_saved: int
    errors: List[str]
    duration_seconds: float


class BatchScrapeRequest(BaseModel):
    """Request for scraping multiple funds."""
    fund_slugs: List[str] = Field(..., min_length=1, max_length=18)
    parallel: bool = Field(False, description="Run scrapers in parallel")


@app.get("/scrapers/status", response_model=ScraperStatusResponse)
async def get_scraper_status():
    """Get status of implemented scrapers (cached for 1 hour)."""
    cached = cache.get("scraper_status")
    if cached:
        return cached

    response = ScraperStatusResponse(
        implemented=get_implemented_scrapers(),
        not_implemented=get_unimplemented_scrapers(),
        total_funds=len(FUND_REGISTRY),
    )

    cache.set("scraper_status", response, ttl_seconds=3600)
    return response


@app.post("/scrapers/run/{fund_slug}", response_model=ScrapeResponse)
async def run_scraper(
    fund_slug: str,
    api_key: str = Depends(verify_api_key),
):
    """
    Run scraper for a specific fund.

    Requires API key. Scrapes the fund's news source and extracts deals.
    """
    if fund_slug not in FUND_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Fund '{fund_slug}' not found")

    if fund_slug not in get_implemented_scrapers():
        raise HTTPException(
            status_code=400,
            detail=f"Scraper not implemented for '{fund_slug}'"
        )

    # Create ScanJob record for tracking
    scan_job_id, job_id = await create_scan_job(trigger="api")
    start_time = datetime.now(timezone.utc)

    # FIX: Clear content hash cache before scraping to prevent false duplicates
    clear_content_hash_cache()

    # FIX #39: Invalidate cache BEFORE scraping to prevent stale data during operation
    cache.invalidate("deals")

    try:
        result = await scrape_fund(fund_slug, scan_job_id=scan_job_id)

        # Update ScanJob with success stats
        await update_scan_job(
            scan_job_id,
            status="success",
            stats={
                "articles_found": result.articles_found,
                "deals_extracted": result.deals_extracted,
                "deals_saved": result.deals_saved,
                "articles_skipped_duplicate": result.articles_skipped_duplicate,
                "errors": len(result.errors),
                "source_results": {fund_slug: {
                    "articles_found": result.articles_found,
                    "deals_extracted": result.deals_extracted,
                    "deals_saved": result.deals_saved,
                }},
            },
            start_time=start_time,
        )

        return ScrapeResponse(
            fund_slug=result.fund_slug,
            articles_found=result.articles_found,
            articles_skipped_duplicate=result.articles_skipped_duplicate,
            articles_rejected_not_announcement=result.articles_rejected_not_announcement,
            deals_extracted=result.deals_extracted,
            deals_saved=result.deals_saved,
            errors=result.errors,
            duration_seconds=result.duration_seconds,
        )
    except Exception as e:
        await update_scan_job(scan_job_id, status="failed", error=str(e), start_time=start_time)
        raise


@app.post("/scrapers/run", response_model=List[ScrapeResponse])
async def run_scrapers_batch(
    request: BatchScrapeRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Run scrapers for multiple funds.

    OPTIMIZED:
    - Optional parallel execution
    - Process up to 18 funds per request
    """
    # Validate all fund slugs
    for slug in request.fund_slugs:
        if slug not in FUND_REGISTRY:
            raise HTTPException(status_code=404, detail=f"Fund '{slug}' not found")
        if slug not in get_implemented_scrapers():
            raise HTTPException(
                status_code=400,
                detail=f"Scraper not implemented for '{slug}'"
            )

    # Create ScanJob record for tracking
    scan_job_id, job_id = await create_scan_job(trigger="api")
    start_time = datetime.now(timezone.utc)

    try:
        results = await scrape_all_funds(
            fund_slugs=request.fund_slugs,
            parallel=request.parallel,
            max_parallel_funds=3,
            scan_job_id=scan_job_id,
        )

        # Invalidate deals cache after scraping
        cache.invalidate("deals")

        # Build source results for ScanJob
        source_results = {}
        total_articles = 0
        total_extracted = 0
        total_saved = 0
        total_duplicates = 0
        total_errors = 0

        for r in results:
            source_results[r.fund_slug] = {
                "articles_found": r.articles_found,
                "deals_extracted": r.deals_extracted,
                "deals_saved": r.deals_saved,
            }
            total_articles += r.articles_found
            total_extracted += r.deals_extracted
            total_saved += r.deals_saved
            total_duplicates += r.articles_skipped_duplicate
            total_errors += len(r.errors)

        await update_scan_job(
            scan_job_id,
            status="success",
            stats={
                "articles_found": total_articles,
                "deals_extracted": total_extracted,
                "deals_saved": total_saved,
                "articles_skipped_duplicate": total_duplicates,
                "errors": total_errors,
                "source_results": source_results,
            },
            start_time=start_time,
        )

        return [
            ScrapeResponse(
                fund_slug=r.fund_slug,
                articles_found=r.articles_found,
                articles_skipped_duplicate=r.articles_skipped_duplicate,
                articles_rejected_not_announcement=r.articles_rejected_not_announcement,
                deals_extracted=r.deals_extracted,
                deals_saved=r.deals_saved,
                errors=r.errors,
                duration_seconds=r.duration_seconds,
            )
            for r in results
        ]
    except Exception as e:
        await update_scan_job(scan_job_id, status="failed", error=str(e), start_time=start_time)
        raise


# ----- Run All Scrapers -----

@app.post("/scrapers/run-all", response_model=List[ScrapeResponse])
async def run_all_scrapers(
    api_key: str = Depends(verify_api_key),
    parallel: bool = Query(True, description="Run in parallel"),
):
    """
    Run all implemented scrapers.

    This endpoint can be called by external cron services or manually.
    Scrapes all 18 VC funds with parallel execution.
    """
    # Create ScanJob record for tracking
    scan_job_id, job_id = await create_scan_job(trigger="api")
    start_time = datetime.now(timezone.utc)

    fund_slugs = get_implemented_scrapers()

    try:
        results = await scrape_all_funds(
            fund_slugs=fund_slugs,
            parallel=parallel,
            max_parallel_funds=3,
            scan_job_id=scan_job_id,
        )

        cache.invalidate("deals")

        # Build source results for ScanJob
        source_results = {}
        total_articles = 0
        total_extracted = 0
        total_saved = 0
        total_duplicates = 0
        total_errors = 0

        for r in results:
            source_results[r.fund_slug] = {
                "articles_found": r.articles_found,
                "deals_extracted": r.deals_extracted,
                "deals_saved": r.deals_saved,
            }
            total_articles += r.articles_found
            total_extracted += r.deals_extracted
            total_saved += r.deals_saved
            total_duplicates += r.articles_skipped_duplicate
            total_errors += len(r.errors)

        await update_scan_job(
            scan_job_id,
            status="success",
            stats={
                "articles_found": total_articles,
                "deals_extracted": total_extracted,
                "deals_saved": total_saved,
                "articles_skipped_duplicate": total_duplicates,
                "errors": total_errors,
                "source_results": source_results,
            },
            start_time=start_time,
        )

        return [
            ScrapeResponse(
                fund_slug=r.fund_slug,
                articles_found=r.articles_found,
                articles_skipped_duplicate=r.articles_skipped_duplicate,
                articles_rejected_not_announcement=r.articles_rejected_not_announcement,
                deals_extracted=r.deals_extracted,
                deals_saved=r.deals_saved,
                errors=r.errors,
                duration_seconds=r.duration_seconds,
            )
            for r in results
        ]
    except Exception as e:
        await update_scan_job(scan_job_id, status="failed", error=str(e), start_time=start_time)
        raise


# ----- Data Source Endpoints -----

class SECEdgarResponse(BaseModel):
    filings_found: int
    filings_with_tracked_funds: int
    articles_generated: int
    deals_saved: int = 0


@app.post("/scrapers/sec-edgar", response_model=SECEdgarResponse)
async def run_sec_edgar_scraper(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run SEC EDGAR Form D scraper.

    Fetches recent Form D filings (private placements) from SEC.
    This is the "first signal" - often appears before news.
    No API key required (public SEC data).
    """
    from .harvester.scrapers.sec_edgar import SECEdgarScraper
    from .scheduler.jobs import process_external_articles

    # Create ScanJob record for tracking
    scan_job_id, job_id = await create_scan_job(trigger="api")
    start_time = datetime.now(timezone.utc)

    try:
        async with SECEdgarScraper() as scraper:
            filings = await scraper.fetch_recent_filings(hours=hours)

            tracked_count = 0
            articles = []
            seen_ciks = set()  # Deduplication by CIK

            for filing in filings:
                # Skip duplicate CIKs
                if filing.cik in seen_ciks:
                    continue
                seen_ciks.add(filing.cik)

                # Fetch filing details (amount, industry, state, investors)
                # Note: fetch_filing_details has 1s rate limit built-in
                result = await scraper.fetch_filing_details(filing)
                if result is None:
                    continue  # Skip filings with missing required data
                filing = result

                fund_slug = scraper.match_tracked_fund(filing)
                if fund_slug:
                    tracked_count += 1
                article = await scraper.to_normalized_article(filing, fund_slug)
                articles.append(article)

        # Save articles to database (was missing - articles were discarded)
        stats = await process_external_articles(articles, "sec_edgar", scan_job_id=scan_job_id)
        cache.invalidate("deals")

        # Update ScanJob with success stats
        await update_scan_job(
            scan_job_id,
            status="success",
            stats={
                "articles_found": len(articles),
                "deals_extracted": stats.get("deals_extracted", 0),
                "deals_saved": stats.get("deals_saved", 0),
                "errors": 0,
                "source_results": {"sec_edgar": {
                    "filings_found": len(filings),
                    "filings_with_tracked_funds": tracked_count,
                    "articles_generated": len(articles),
                    "deals_saved": stats.get("deals_saved", 0),
                }},
            },
            start_time=start_time,
        )

        return SECEdgarResponse(
            filings_found=len(filings),
            filings_with_tracked_funds=tracked_count,
            articles_generated=len(articles),
            deals_saved=stats.get("deals_saved", 0),
        )

    except Exception as e:
        await update_scan_job(scan_job_id, status="failed", error=str(e), start_time=start_time)
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class BraveSearchResponse(BaseModel):
    queries_executed: int
    results_found: int
    articles_generated: int
    deals_extracted: Optional[int] = None  # Added - count of deals found by LLM
    deals_saved: Optional[int] = None  # Added - count of new deals saved


@app.post("/scrapers/brave-search", response_model=BraveSearchResponse)
async def run_brave_search_scraper_endpoint(
    freshness: str = Query("pw", description="pd=past day, pw=past week, pm=past month"),
    include_regional: bool = Query(False, description="Include 10 regional market searches"),
    include_early_signals: bool = Query(False, description="Include early signal detection (hiring, expansion)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Brave Search API scraper with expanded coverage options.

    Searches for funding news with "led by" AND participation language for tracked funds.
    Requires BRAVE_SEARCH_KEY to be configured.

    Query breakdown (base):
    - 18 fund lead queries
    - 18 fund participation queries
    - 1 enterprise AI query
    - 1 stealth query
    = 38 queries (base)

    Optional expansions:
    - include_regional: +10 queries (Europe, UK, Germany, France, Asia, India, Singapore, LATAM, Brazil, Israel)
    - include_early_signals: +12 queries (hiring signals, expansion news)

    Full expansion = 60 queries total
    """
    from .harvester.scrapers.brave_search import BraveSearchScraper

    if not settings.brave_search_key:
        raise HTTPException(status_code=400, detail="BRAVE_SEARCH_KEY not configured")

    try:
        async with BraveSearchScraper() as scraper:
            # Run full scrape with expanded options
            articles = await scraper.scrape_all(
                freshness=freshness,
                include_enterprise=True,
                include_participation=False,  # Lead-only focus (Jan 2026)
                include_stealth=True,
                include_regional=include_regional,
                include_early_signals=include_early_signals,
                include_partner_names=True,  # Critical for funds without public announcements
            )

        cache.invalidate("deals")

        # Calculate query count - now includes partner queries (participation disabled Jan 2026)
        queries_executed = 20  # Base: 18 lead + 1 enterprise + 1 stealth
        queries_executed += 57  # Partner name queries for all funds
        if include_regional:
            queries_executed += 10  # 10 regions
        if include_early_signals:
            queries_executed += 12  # 12 early signal queries

        # Process articles through extraction pipeline
        # Skip title filter - Brave queries are already targeted at funding news
        from .scheduler.jobs import process_external_articles
        stats = await process_external_articles(articles, "brave_search", skip_title_filter=True)

        return BraveSearchResponse(
            queries_executed=queries_executed,
            results_found=len(articles),  # Before dedup
            articles_generated=len(articles),
            deals_extracted=stats.get("deals_extracted", 0),
            deals_saved=stats.get("deals_saved", 0),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class FastScanResponse(BaseModel):
    """Response for fast-scan endpoint."""
    queries_executed: int
    results_found: int
    scan_type: str
    freshness: str


@app.post("/scrapers/fast-scan", response_model=FastScanResponse)
async def run_fast_scan_endpoint(
    api_key: str = Depends(verify_api_key),
):
    """
    Fast scan for real-time deal detection (run hourly).

    Lightweight scan using past-day freshness for faster detection:
    - 18 fund lead queries only (no participation)
    - 12 early signal queries (hiring, expansion)
    - 1 stealth query
    = 31 queries with pd (past day) freshness

    Designed to run every hour for near real-time detection.
    Use /scrapers/brave-search with full options for comprehensive weekly scans.
    """
    from .harvester.scrapers.brave_search import BraveSearchScraper

    if not settings.brave_search_key:
        raise HTTPException(status_code=400, detail="BRAVE_SEARCH_KEY not configured")

    try:
        async with BraveSearchScraper() as scraper:
            # Fast scan: lead queries + early signals only, past day
            articles = await scraper.scrape_all(
                freshness="pd",  # Past day for speed
                include_enterprise=False,  # Skip for speed
                include_participation=False,  # Skip for speed
                include_stealth=True,
                include_regional=False,
                include_early_signals=True,  # Key for fast detection
            )

        cache.invalidate("deals")

        # Query count: 18 lead + 1 stealth + 12 early signals = 31
        queries_executed = 31

        return FastScanResponse(
            queries_executed=queries_executed,
            results_found=len(articles),
            scan_type="fast",
            freshness="pd",
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class FirecrawlRequest(BaseModel):
    urls: List[str] = Field(..., min_length=1, max_length=10, description="URLs to scrape")


class FirecrawlResponse(BaseModel):
    urls_submitted: int
    urls_scraped: int
    articles_generated: int


@app.post("/scrapers/firecrawl", response_model=FirecrawlResponse)
async def run_firecrawl_scraper_endpoint(
    request: FirecrawlRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Scrape URLs using Firecrawl API.

    Use this for JS-heavy PR sites (PRNewswire, BusinessWire, etc.)
    that have bot detection. Returns clean markdown text.
    Requires FIRECRAWL_API_KEY to be configured.
    """
    from .harvester.scrapers.firecrawl_scraper import FirecrawlScraper

    if not settings.firecrawl_api_key:
        raise HTTPException(status_code=400, detail="FIRECRAWL_API_KEY not configured")

    try:
        async with FirecrawlScraper() as scraper:
            articles = await scraper.scrape_pr_urls(request.urls)

        return FirecrawlResponse(
            urls_submitted=len(request.urls),
            urls_scraped=len(articles),
            articles_generated=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Google Alerts Endpoints -----

class GoogleAlertsResponse(BaseModel):
    feeds_configured: int
    articles_found: int
    articles_skipped_duplicate: int = 0
    deals_extracted: int = 0
    deals_saved: int = 0
    errors: int = 0
    setup_instructions: Optional[str] = None


@app.post("/scrapers/google-alerts", response_model=GoogleAlertsResponse)
async def run_google_alerts_scraper(
    api_key: str = Depends(verify_api_key),
):
    """
    Run Google Alerts RSS scraper.

    Fetches articles from configured Google Alerts RSS feeds and processes
    them through the extraction pipeline to save deals.
    Set GOOGLE_ALERTS_FEEDS environment variable with comma-separated feed URLs.
    """
    from .harvester.scrapers.google_alerts import GoogleAlertsScraper, get_alert_setup_instructions
    from .scheduler.jobs import process_external_articles

    feed_urls = []
    if settings.google_alerts_feeds:
        feed_urls = [f.strip() for f in settings.google_alerts_feeds.split(',') if f.strip()]

    if not feed_urls:
        return GoogleAlertsResponse(
            feeds_configured=0,
            articles_found=0,
            setup_instructions=get_alert_setup_instructions(),
        )

    try:
        async with GoogleAlertsScraper(feed_urls) as scraper:
            articles = await scraper.scrape_all()

        # Process articles through extraction pipeline (CRITICAL FIX)
        stats = await process_external_articles(articles, "google_alerts")

        return GoogleAlertsResponse(
            feeds_configured=len(feed_urls),
            articles_found=len(articles),
            articles_skipped_duplicate=stats.get("articles_skipped_duplicate", 0),
            deals_extracted=stats.get("deals_extracted", 0),
            deals_saved=stats.get("deals_saved", 0),
            errors=stats.get("errors", 0),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/scrapers/google-alerts/setup")
async def get_google_alerts_setup():
    """Get setup instructions for Google Alerts."""
    from .harvester.scrapers.google_alerts import get_alert_setup_instructions
    return {"instructions": get_alert_setup_instructions()}


# ----- Twitter Monitor Endpoints -----

class TwitterMonitorResponse(BaseModel):
    tweets_found: int
    api_calls_used: int
    configured: bool
    setup_instructions: Optional[str] = None


@app.post("/scrapers/twitter", response_model=TwitterMonitorResponse)
async def run_twitter_monitor(
    hours_back: int = Query(168, ge=1, le=168, description="Hours to look back (max 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Twitter/X monitor for VC funding announcements.

    Monitors official VC accounts and searches for funding news.
    Requires TWITTER_BEARER_TOKEN environment variable.

    Free tier limit: ~100 API calls per run (conservative to stay under 1,500/month)
    """
    from .harvester.scrapers.twitter_monitor import TwitterMonitor, get_twitter_setup_instructions

    if not settings.twitter_bearer_token:
        return TwitterMonitorResponse(
            tweets_found=0,
            api_calls_used=0,
            configured=False,
            setup_instructions=get_twitter_setup_instructions(),
        )

    try:
        async with TwitterMonitor() as monitor:
            articles = await monitor.scrape_all(hours_back=hours_back)
            api_calls = monitor._request_count

        return TwitterMonitorResponse(
            tweets_found=len(articles),
            api_calls_used=api_calls,
            configured=True,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/scrapers/twitter/setup")
async def get_twitter_setup():
    """Get setup instructions for Twitter API."""
    from .harvester.scrapers.twitter_monitor import get_twitter_setup_instructions
    return {"instructions": get_twitter_setup_instructions()}


# ----- RSS Feed Scrapers -----

class TechCrunchResponse(BaseModel):
    articles_found: int
    funding_articles: int


@app.post("/scrapers/techcrunch", response_model=TechCrunchResponse)
async def run_techcrunch_scraper_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run TechCrunch RSS scraper.

    Fetches startup and venture articles from TechCrunch RSS feeds.
    Filters for funding-related content mentioning tracked funds.
    FREE - no API key required.
    """
    from .harvester.scrapers.techcrunch_rss import TechCrunchScraper

    try:
        async with TechCrunchScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        cache.invalidate("deals")

        return TechCrunchResponse(
            articles_found=len(articles),
            funding_articles=len([a for a in articles if a.fund_slug]),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class FortuneResponse(BaseModel):
    articles_found: int
    funding_articles: int


@app.post("/scrapers/fortune", response_model=FortuneResponse)
async def run_fortune_scraper_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Fortune Term Sheet RSS scraper.

    Fetches deal-focused content from Fortune's Term Sheet newsletter feed.
    One of the best curated sources for funding news.
    FREE - no API key required.
    """
    from .harvester.scrapers.fortune_term_sheet import FortuneTermSheetScraper

    try:
        async with FortuneTermSheetScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        cache.invalidate("deals")

        return FortuneResponse(
            articles_found=len(articles),
            funding_articles=len([a for a in articles if a.fund_slug]),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class GoogleNewsResponse(BaseModel):
    articles_found: int
    fund_matches: int
    funds_covered: List[str]


@app.post("/scrapers/google-news", response_model=GoogleNewsResponse)
async def run_google_news_scraper_endpoint(
    days_back: int = Query(30, ge=1, le=90, description="Days to look back (default 30)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Google News RSS scraper for external-only funds.

    Provides coverage for funds without scrapable portfolio pages:
    - Thrive Capital (0 deals - EXTERNAL)
    - Benchmark (0 deals - EXTERNAL)
    - First Round (0 deals - site doesn't announce)
    - Greylock, GV, Founders Fund, Redpoint (low coverage)

    Creates RSS feeds from Google News for each fund's name and partners.
    FREE - no API key required.
    """
    from .harvester.scrapers.google_news_rss import GoogleNewsRSSScraper, GOOGLE_NEWS_FUNDS
    from .scheduler.jobs import process_external_articles

    # Create ScanJob record for tracking
    scan_job_id, job_id = await create_scan_job(trigger="api")
    start_time = datetime.now(timezone.utc)

    try:
        async with GoogleNewsRSSScraper() as scraper:
            articles = await scraper.scrape_all(days_back=days_back)

        # Process through extraction pipeline
        stats = await process_external_articles(articles, "google_news", skip_title_filter=True, scan_job_id=scan_job_id)

        cache.invalidate("deals")

        # Update ScanJob with success stats
        await update_scan_job(
            scan_job_id,
            status="success",
            stats={
                "articles_found": len(articles),
                "deals_extracted": stats.get("deals_extracted", 0),
                "deals_saved": stats.get("deals_saved", 0),
                "errors": stats.get("errors", 0),
                "source_results": {"google_news": {
                    "articles_found": len(articles),
                    "fund_matches": len([a for a in articles if a.fund_slug]),
                    "deals_saved": stats.get("deals_saved", 0),
                }},
            },
            start_time=start_time,
        )

        return GoogleNewsResponse(
            articles_found=len(articles),
            fund_matches=len([a for a in articles if a.fund_slug]),
            funds_covered=list(GOOGLE_NEWS_FUNDS.keys()),
        )

    except Exception as e:
        await update_scan_job(scan_job_id, status="failed", error=str(e), start_time=start_time)
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Community/Early Signal Scrapers -----

class YCombinatorResponse(BaseModel):
    companies_found: int
    blog_posts_found: int
    total_articles: int


@app.post("/scrapers/ycombinator", response_model=YCombinatorResponse)
async def run_ycombinator_scraper_endpoint(
    api_key: str = Depends(verify_api_key),
):
    """
    Run Y Combinator scraper.

    Fetches companies from recent YC batches (Demo Day = 200+ companies twice/year).
    Also monitors YC blog for announcements.
    FREE - no API key required.
    """
    from .harvester.scrapers.ycombinator import YCombinatorScraper

    try:
        async with YCombinatorScraper() as scraper:
            articles = await scraper.scrape_all()

        # Count by type
        companies = len([a for a in articles if 'demo_day' in a.tags])
        blog_posts = len([a for a in articles if 'blog' in a.tags])

        cache.invalidate("deals")

        return YCombinatorResponse(
            companies_found=companies,
            blog_posts_found=blog_posts,
            total_articles=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class GitHubTrendingResponse(BaseModel):
    repos_found: int
    enterprise_devtools: int


@app.post("/scrapers/github-trending", response_model=GitHubTrendingResponse)
async def run_github_trending_scraper_endpoint(
    since: str = Query("daily", description="Time range: daily, weekly, monthly"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run GitHub Trending scraper.

    Fetches trending repositories - dev tools often appear before funding announcements.
    Filters for enterprise dev tools (AI, infrastructure, DevOps, etc.).
    FREE - no API key required.
    """
    from .harvester.scrapers.github_trending import GitHubTrendingScraper

    try:
        async with GitHubTrendingScraper() as scraper:
            # Fetch repos once, then pass to scrape_all to avoid double-fetch
            all_repos = await scraper.fetch_all_trending(since=since)
            # Pass pre-fetched repos to avoid fetching again
            articles = await scraper.scrape_all(since=since, filter_enterprise=True, repos=all_repos)

        return GitHubTrendingResponse(
            repos_found=len(all_repos),
            enterprise_devtools=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class HackerNewsResponse(BaseModel):
    launch_hn_posts: int
    show_hn_posts: int
    funding_posts: int
    total_articles: int


@app.post("/scrapers/hackernews", response_model=HackerNewsResponse)
async def run_hackernews_scraper_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Hacker News scraper.

    Monitors HN for Launch HN posts (startup launches) and funding news.
    Launch posts often precede/coincide with funding announcements.
    FREE - uses public HN Algolia API.
    """
    from .harvester.scrapers.hackernews import HackerNewsScraper

    try:
        async with HackerNewsScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        # Count by type
        launch_hn = len([a for a in articles if 'launch_hn' in a.tags])
        show_hn = len([a for a in articles if 'show_hn' in a.tags])
        funding = len([a for a in articles if a.fund_slug])

        return HackerNewsResponse(
            launch_hn_posts=launch_hn,
            show_hn_posts=show_hn,
            funding_posts=funding,
            total_articles=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- LinkedIn Jobs (Stealth Detection) -----

class LinkedInJobsResponse(BaseModel):
    queries_executed: int
    stealth_signals_found: int
    fund_matches: int
    total_jobs: int


@app.post("/scrapers/linkedin-jobs", response_model=LinkedInJobsResponse)
async def run_linkedin_jobs_scraper_endpoint(
    api_key: str = Depends(verify_api_key),
):
    """
    Run LinkedIn Jobs scraper for stealth startup detection.

    Searches for job posts mentioning:
    - "stealth startup" / "stealth mode"
    - "backed by [VC name]" for tracked funds
    - "recently funded" startups

    Uses Brave Search API (requires BRAVE_SEARCH_KEY).
    ~17 queries per run.
    """
    from .harvester.scrapers.linkedin_jobs import LinkedInJobsScraper, STEALTH_JOB_QUERIES
    from .archivist.storage import save_stealth_detection
    from .archivist.database import get_session

    if not settings.brave_search_key:
        raise HTTPException(status_code=400, detail="BRAVE_SEARCH_KEY not configured")

    try:
        async with LinkedInJobsScraper() as scraper:
            articles = await scraper.scrape_all()

        stealth_count = len([a for a in articles if 'stealth' in a.tags])
        fund_matches = len([a for a in articles if a.fund_slug])

        # Save stealth detections to database
        async with get_session() as session:
            for article in articles:
                if 'stealth' in article.tags or article.fund_slug:
                    await save_stealth_detection(
                        session=session,
                        fund_slug=article.fund_slug or "unknown",
                        detected_url=article.url,
                        company_name=article.title.replace("Stealth Signal: ", "").split(" - ")[0] if article.title else None,
                        notes=f"Source: LinkedIn Jobs | Tags: {', '.join(article.tags)}",
                    )

        return LinkedInJobsResponse(
            queries_executed=len(STEALTH_JOB_QUERIES),
            stealth_signals_found=stealth_count,
            fund_matches=fund_matches,
            total_jobs=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Tier 1 Stealth Detection Scrapers -----

class PortfolioDiffResponse(BaseModel):
    funds_checked: int
    new_companies_found: int
    total_articles: int


@app.post("/scrapers/portfolio-diff", response_model=PortfolioDiffResponse)
async def run_portfolio_diff_scraper_endpoint(
    fund_slugs: Optional[str] = Query(None, description="Comma-separated fund slugs (default: all 18)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run VC Portfolio Page Diff scraper.

    Monitors VC fund portfolio pages for new company additions.
    VCs often add companies to portfolios BEFORE public announcements.
    FREE - no API key required (scrapes public portfolio pages).

    Tier 1 stealth detection - one of the BEST free sources.
    """
    from .harvester.scrapers.portfolio_diff import PortfolioDiffScraper
    from .archivist.storage import save_stealth_detection
    from .archivist.database import get_session

    try:
        slugs = None
        if fund_slugs:
            slugs = [s.strip() for s in fund_slugs.split(',') if s.strip()]

        async with PortfolioDiffScraper() as scraper:
            articles = await scraper.scrape_all(fund_slugs=slugs)

        funds_checked = len(slugs) if slugs else 18  # 18 funds in PORTFOLIO_URLS

        # Save stealth detections to database
        async with get_session() as session:
            for article in articles:
                company_name = article.title.replace("Stealth Addition: ", "").split(" added to ")[0] if article.title else None
                await save_stealth_detection(
                    session=session,
                    fund_slug=article.fund_slug or "unknown",
                    detected_url=article.url,
                    company_name=company_name,
                    notes=f"Source: Portfolio Diff | Tags: {', '.join(article.tags)}",
                )

        cache.invalidate("deals")

        return PortfolioDiffResponse(
            funds_checked=funds_checked,
            new_companies_found=len(articles),
            total_articles=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class USPTOTrademarkResponse(BaseModel):
    trademarks_found: int
    tech_trademarks: int
    total_articles: int


@app.post("/scrapers/uspto-trademarks", response_model=USPTOTrademarkResponse)
async def run_uspto_trademark_scraper_endpoint(
    days_back: int = Query(7, ge=1, le=30, description="Days to look back"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run USPTO Trademark scraper.

    Monitors USPTO for new trademark applications that might indicate:
    - New startup names (company formation)
    - New product launches
    - Stealth companies emerging

    Trademark applications are PUBLIC and often filed BEFORE announcements.
    Uses Brave Search API (requires BRAVE_SEARCH_KEY).

    Filters for tech classes:
    - Class 9: Software, computers, electronics
    - Class 35: Business services, SaaS
    - Class 42: Technology services, cloud computing
    """
    from .harvester.scrapers.uspto_trademarks import USPTOTrademarkScraper

    if not settings.brave_search_key:
        raise HTTPException(status_code=400, detail="BRAVE_SEARCH_KEY not configured")

    try:
        async with USPTOTrademarkScraper() as scraper:
            # Get all trademarks first
            trademarks = await scraper.search_recent_trademarks(days_back=days_back)
            # Get filtered articles (tech only)
            articles = await scraper.scrape_all(days_back=days_back, tech_only=True)

        cache.invalidate("deals")

        return USPTOTrademarkResponse(
            trademarks_found=len(trademarks),
            tech_trademarks=len(articles),
            total_articles=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


class DelawareCorpsResponse(BaseModel):
    entities_found: int
    tech_entities: int
    high_signal_entities: int
    total_articles: int


@app.post("/scrapers/delaware-corps", response_model=DelawareCorpsResponse)
async def run_delaware_corps_scraper_endpoint(
    days_back: int = Query(7, ge=1, le=30, description="Days to look back"),
    min_score: int = Query(3, ge=0, le=10, description="Minimum score to include"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Delaware Division of Corporations scraper.

    Monitors Delaware for new tech company formations.
    ~67% of Fortune 500 and most VC-backed startups incorporate in Delaware.

    Scoring signals:
    - Tech company name pattern (+3)
    - Startup-friendly registered agent like Stripe Atlas, Clerky (+5)
    - Recent formation (+2)
    - AI/ML in name (+2)

    Uses Brave Search + OpenCorporates API.
    Requires BRAVE_SEARCH_KEY for best results.
    """
    from .harvester.scrapers.delaware_corps import DelawareCorpsScraper

    if not settings.brave_search_key:
        raise HTTPException(status_code=400, detail="BRAVE_SEARCH_KEY not configured")

    try:
        async with DelawareCorpsScraper() as scraper:
            # Get all articles (already filtered by score)
            articles = await scraper.scrape_all(days_back=days_back, min_score=min_score)

            # Also get raw entities for counts
            brave_entities = await scraper.search_recent_incorporations(days_back=days_back)
            agg_entities = await scraper.search_aggregators()

        all_entities = len(brave_entities) + len(agg_entities)
        tech_entities = len([e for e in brave_entities if e.has_tech_name] +
                          [e for e in agg_entities if e.has_tech_name])
        high_signal = len([e for e in brave_entities if e.has_startup_agent] +
                         [e for e in agg_entities if e.has_startup_agent])

        cache.invalidate("deals")

        return DelawareCorpsResponse(
            entities_found=all_entities,
            tech_entities=tech_entities,
            high_signal_entities=high_signal,
            total_articles=len(articles),
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- SEC + Delaware Cross-Reference -----

class LeadScoreBreakdown(BaseModel):
    delaware: Optional[int] = None
    first_sale: Optional[int] = None
    tech_industry: Optional[int] = None
    amount_5m_plus: Optional[int] = None
    amount_1m_plus: Optional[int] = None
    amount_500k_plus: Optional[int] = None
    vc_fund_match: Optional[int] = None
    startup_agent: Optional[int] = None
    tech_name: Optional[int] = None


class HighPriorityLeadResponse(BaseModel):
    company_name: str
    cik: str
    filing_date: str
    filing_url: str
    state_of_incorporation: Optional[str]
    amount_raised: Optional[str]
    total_offering: Optional[str]
    is_first_sale: bool
    industry: Optional[str]
    score: int
    priority: str
    score_breakdown: LeadScoreBreakdown
    is_delaware: bool
    is_tech_industry: bool
    has_vc_fund_match: bool
    has_startup_agent: bool
    matched_fund_slug: Optional[str]
    registered_agent: Optional[str]


class SECDelawareCrossrefResponse(BaseModel):
    leads_found: int
    high_priority: int
    strong_signal: int
    watch_list: int
    leads: List[HighPriorityLeadResponse]


@app.post("/scrapers/sec-delaware-crossref", response_model=SECDelawareCrossrefResponse)
async def run_sec_delaware_crossref_endpoint(
    days_back: int = Query(30, ge=1, le=90, description="Days to look back"),
    min_score: int = Query(3, ge=0, le=15, description="Minimum score to include"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run SEC Form D + Delaware cross-reference for high-priority leads.

    Combines SEC Form D filings with Delaware registry data using multi-signal scoring:

    **Scoring:**
    - Delaware incorporation: +2
    - First sale (new round): +3
    - Tech industry: +2
    - Amount $5M+: +3, $1M+: +2, $500K+: +1
    - Tracked VC fund match: +5
    - Startup-friendly agent (Stripe Atlas, Clerky): +5
    - Tech company name: +2

    **Priority Tiers:**
    - HIGH (10+): Strong VC-backed signals
    - STRONG (7-9): Good indicators
    - WATCH (3-6): Worth monitoring

    Uses SEC EDGAR (FREE) + OpenCorporates API (FREE with rate limits).
    """
    from .harvester.scrapers.sec_delaware_crossref import get_high_priority_leads

    try:
        leads = await get_high_priority_leads(
            days_back=days_back,
            min_score=min_score,
            delaware_only=True,
            tech_only=True,
            min_amount=500_000,
        )

        # Count by priority
        high_count = len([l for l in leads if l.priority == "HIGH"])
        strong_count = len([l for l in leads if l.priority == "STRONG"])
        watch_count = len([l for l in leads if l.priority == "WATCH"])

        # Convert to response format
        lead_responses = []
        for lead in leads:
            lead_responses.append(HighPriorityLeadResponse(
                company_name=lead.company_name,
                cik=lead.cik,
                filing_date=lead.filing_date.isoformat(),
                filing_url=lead.filing_url,
                state_of_incorporation=lead.state_of_incorporation,
                amount_raised=lead.amount_raised,
                total_offering=lead.total_offering,
                is_first_sale=lead.is_first_sale,
                industry=lead.industry,
                score=lead.score,
                priority=lead.priority,
                score_breakdown=LeadScoreBreakdown(**lead.score_breakdown),
                is_delaware=lead.is_delaware,
                is_tech_industry=lead.is_tech_industry,
                has_vc_fund_match=lead.has_vc_fund_match,
                has_startup_agent=lead.has_startup_agent,
                matched_fund_slug=lead.matched_fund_slug,
                registered_agent=lead.delaware_entity.registered_agent if lead.delaware_entity else None,
            ))

        cache.invalidate("deals")

        return SECDelawareCrossrefResponse(
            leads_found=len(leads),
            high_priority=high_count,
            strong_signal=strong_count,
            watch_list=watch_count,
            leads=lead_responses,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Tech Funding News -----

class TechFundingNewsResponse(BaseModel):
    feeds_scraped: int
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/tech-funding-news", response_model=TechFundingNewsResponse)
async def run_tech_funding_news_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Tech Funding News scraper.

    Premium source for startup funding news. Covers SaaS, AI, FinTech.
    Multiple category feeds for comprehensive coverage.
    FREE - uses public RSS feeds.
    """
    from .harvester.scrapers.tech_funding_news import TechFundingNewsScraper

    try:
        async with TechFundingNewsScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        fund_matches = len([a for a in articles if a.fund_slug])

        return TechFundingNewsResponse(
            feeds_scraped=5,  # main + 4 categories
            articles_found=len(articles),
            funding_articles=len(articles),  # Already filtered for funding
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Ventureburn -----

class VentureburnResponse(BaseModel):
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/ventureburn", response_model=VentureburnResponse)
async def run_ventureburn_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Ventureburn scraper.

    Covers emerging markets startup news (Africa, global).
    Filters out crypto noise, focuses on VC funding.
    FREE - uses public RSS feed.
    """
    from .harvester.scrapers.ventureburn import VentureburnScraper

    try:
        async with VentureburnScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        fund_matches = len([a for a in articles if a.fund_slug])

        return VentureburnResponse(
            articles_found=len(articles),
            funding_articles=len(articles),  # Already filtered
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Crunchbase News -----

class CrunchbaseNewsResponse(BaseModel):
    feeds_scraped: int
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/crunchbase-news", response_model=CrunchbaseNewsResponse)
async def run_crunchbase_news_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Crunchbase News scraper.

    High-quality funding news from Crunchbase's editorial team.
    Excellent for detailed deal information and lead investor mentions.
    FREE - uses public RSS feeds.
    """
    from .harvester.scrapers.crunchbase_news import CrunchbaseNewsScraper

    try:
        async with CrunchbaseNewsScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        fund_matches = len([a for a in articles if a.fund_slug])

        return CrunchbaseNewsResponse(
            feeds_scraped=3,  # main + venture + startups
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Crunchbase Pro Direct Ingestion -----

class CrunchbaseDealInput(BaseModel):
    """Input for a single Crunchbase deal from local bot."""
    startup_name: str
    round_type: str
    amount: Optional[str] = None
    announced_date: Optional[str] = None
    lead_investors: List[str] = Field(default_factory=list)
    participating_investors: List[str] = Field(default_factory=list)
    source_url: Optional[str] = None
    source: str = "crunchbase_pro"
    # New fields for AI classification and enrichment
    description: Optional[str] = None
    industries: List[str] = Field(default_factory=list)
    website: Optional[str] = None


class CrunchbaseBatchRequest(BaseModel):
    """Batch request for Crunchbase deals."""
    deals: List[CrunchbaseDealInput]


class CrunchbaseDirectResponse(BaseModel):
    """Response from Crunchbase direct ingestion."""
    deals_received: int
    deals_saved: int
    deals_duplicate: int
    deals_no_tracked_fund: int
    errors: List[str]


@app.post("/scrapers/crunchbase-direct", response_model=CrunchbaseDirectResponse)
async def ingest_crunchbase_deals_endpoint(
    request: CrunchbaseBatchRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Ingest pre-structured deals from Crunchbase Pro.

    This endpoint accepts structured deal data from a local Playwright bot
    that scrapes your Crunchbase Pro saved search. No LLM extraction needed
    since Crunchbase data is already structured.

    Only saves deals where one of the 18 tracked funds is the lead investor.

    Usage:
        POST with JSON: {"deals": [{"startup_name": "...", "round_type": "series_a", ...}]}
    """
    from .harvester.scrapers.crunchbase_direct import (
        process_crunchbase_deals,
        CrunchbaseDealInput as ProcessInput,
    )

    try:
        # Convert to internal format
        deals = [
            ProcessInput(
                startup_name=d.startup_name,
                round_type=d.round_type,
                amount=d.amount,
                announced_date=d.announced_date,
                lead_investors=d.lead_investors,
                participating_investors=d.participating_investors,
                source_url=d.source_url,
                source=d.source,
                # New fields for AI classification
                description=d.description,
                industries=d.industries,
                website=d.website,
            )
            for d in request.deals
        ]

        result = await process_crunchbase_deals(deals)

        return CrunchbaseDirectResponse(
            deals_received=result.deals_received,
            deals_saved=result.deals_saved,
            deals_duplicate=result.deals_duplicate,
            deals_no_tracked_fund=result.deals_no_tracked_fund,
            errors=result.errors,
        )

    except Exception as e:
        logger.error(f"Crunchbase direct ingestion failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ----- VentureBeat -----

class VentureBeatResponse(BaseModel):
    feeds_scraped: int
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/venturebeat", response_model=VentureBeatResponse)
async def run_venturebeat_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run VentureBeat scraper.

    Leading tech publication with strong AI and enterprise coverage.
    Excellent for AI/ML startup funding news.
    FREE - uses public RSS feeds.
    """
    from .harvester.scrapers.venturebeat import VentureBeatScraper

    try:
        async with VentureBeatScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        fund_matches = len([a for a in articles if a.fund_slug])

        return VentureBeatResponse(
            feeds_scraped=4,  # ai + business + funding + games
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Axios Pro Rata -----

class AxiosProRataResponse(BaseModel):
    feeds_scraped: int
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/axios-prorata", response_model=AxiosProRataResponse)
async def run_axios_prorata_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run Axios Pro Rata scraper.

    Dan Primack's legendary VC newsletter.
    One of the most respected sources for VC/PE deal news.
    FREE - uses public RSS feeds (if available).
    NOTE: Axios RSS may be deprecated - deals captured via Brave Search fallback.
    """
    from .harvester.scrapers.axios_prorata import AxiosProRataScraper

    try:
        async with AxiosProRataScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        fund_matches = len([a for a in articles if a.fund_slug])

        return AxiosProRataResponse(
            feeds_scraped=3,  # prorata + technology + deals
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- StrictlyVC -----

class StrictlyVCResponse(BaseModel):
    feeds_scraped: int
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/strictlyvc", response_model=StrictlyVCResponse)
async def run_strictlyvc_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run StrictlyVC scraper.

    Connie Loizos' highly respected VC newsletter.
    One of the best sources for VC funding news.
    FREE - uses public RSS feed.
    """
    from .harvester.scrapers.strictlyvc import StrictlyVCScraper

    try:
        async with StrictlyVCScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=hours_back)

        fund_matches = len([a for a in articles if a.fund_slug])

        return StrictlyVCResponse(
            feeds_scraped=1,  # main feed
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- PR Wire RSS (PRNewswire, GlobeNewswire) -----

class PRWireResponse(BaseModel):
    feeds_scraped: int
    articles_found: int
    funding_articles: int
    fund_matches: int


@app.post("/scrapers/prwire", response_model=PRWireResponse)
async def run_prwire_endpoint(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to look back (default 7 days)"),
    fund_filter: bool = Query(True, description="Only return articles mentioning tracked funds"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run PR Wire RSS scraper.

    Scrapes official funding press releases from:
    - PRNewswire (venture capital category)
    - GlobeNewswire (financing agreements + M&A)

    Note: BusinessWire deprecated public RSS (requires authenticated PressPass).

    These are the PRIMARY sources for official funding announcements.
    FREE - uses public RSS feeds.
    """
    from .harvester.scrapers.prwire_rss import PRWireRSSScraper

    try:
        async with PRWireRSSScraper() as scraper:
            articles = await scraper.scrape_all_feeds(
                hours_back=hours_back,
                fund_filter=fund_filter,
            )

        fund_matches = len([a for a in articles if a.fund_slug])

        return PRWireResponse(
            feeds_scraped=3,  # 1 PRNewswire + 2 GlobeNewswire
            articles_found=len(articles),
            funding_articles=len(articles),  # All are funding-related after filter
            fund_matches=fund_matches,
        )

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- All Sources Combined -----

class AllSourcesResponse(BaseModel):
    sec_edgar: SECEdgarResponse
    brave_search: Optional[BraveSearchResponse] = None
    google_alerts: Optional[GoogleAlertsResponse] = None
    twitter: Optional[TwitterMonitorResponse] = None
    techcrunch: Optional[TechCrunchResponse] = None
    fortune: Optional[FortuneResponse] = None
    ycombinator: Optional[YCombinatorResponse] = None
    github_trending: Optional[GitHubTrendingResponse] = None
    hackernews: Optional[HackerNewsResponse] = None
    linkedin_jobs: Optional[LinkedInJobsResponse] = None
    tech_funding_news: Optional[TechFundingNewsResponse] = None
    ventureburn: Optional[VentureburnResponse] = None
    crunchbase_news: Optional[CrunchbaseNewsResponse] = None
    venturebeat: Optional[VentureBeatResponse] = None
    axios_prorata: Optional[AxiosProRataResponse] = None
    strictlyvc: Optional[StrictlyVCResponse] = None
    prwire: Optional[PRWireResponse] = None  # PRNewswire, GlobeNewswire
    google_news: Optional[GoogleNewsResponse] = None  # External-only fund coverage
    # Tier 1 Stealth Detection
    portfolio_diff: Optional[PortfolioDiffResponse] = None
    uspto_trademarks: Optional[USPTOTrademarkResponse] = None
    delaware_corps: Optional[DelawareCorpsResponse] = None


@app.post("/scrapers/all-sources", response_model=AllSourcesResponse)
async def run_all_sources(
    days: int = Query(7, ge=1, le=30, description="Days to look back"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run all data source scrapers in parallel.

    Fetches from:
    1. SEC EDGAR (Form D filings) - always runs
    2. Brave Search (if configured)
    3. Google Alerts (if configured)
    4. Twitter (if configured)

    This is the recommended endpoint for comprehensive deal discovery.
    """
    from .harvester.scrapers.sec_edgar import SECEdgarScraper
    from .harvester.scrapers.brave_search import BraveSearchScraper
    from .harvester.scrapers.google_alerts import GoogleAlertsScraper
    from .harvester.scrapers.twitter_monitor import TwitterMonitor

    # Create ScanJob record for tracking
    scan_job_id, job_id = await create_scan_job(trigger="api")
    start_time = datetime.now(timezone.utc)

    # Track aggregate stats for ScanJob
    total_articles = 0
    total_deals_extracted = 0
    total_deals_saved = 0
    total_errors = 0

    results = {}

    # SEC EDGAR (always runs - free)
    try:
        async with SECEdgarScraper() as scraper:
            filings = await scraper.fetch_recent_filings(hours=days * 24)
            tracked_count = sum(1 for f in filings if scraper.match_tracked_fund(f))

        results["sec_edgar"] = SECEdgarResponse(
            filings_found=len(filings),
            filings_with_tracked_funds=tracked_count,
            articles_generated=len(filings),
        )
    except Exception:
        results["sec_edgar"] = SECEdgarResponse(filings_found=0, filings_with_tracked_funds=0, articles_generated=0)

    # Brave Search (if configured) - 2x coverage with participation + stealth
    if settings.brave_search_key:
        try:
            from .scheduler.jobs import process_external_articles

            freshness = "pd" if days <= 1 else ("pw" if days <= 7 else "pm")
            async with BraveSearchScraper() as scraper:
                articles = await scraper.scrape_all(
                    freshness=freshness,
                    include_enterprise=True,
                    include_participation=False,  # Lead-only focus (Jan 2026)
                    include_stealth=True,
                    include_partner_names=True,  # Critical for disabled fund scrapers
                )

            # Process articles through extraction pipeline
            # FIX: Was just counting articles, not actually extracting deals!
            # Skip title filter - Brave queries are already targeted at funding news
            stats = await process_external_articles(articles, "brave_search", skip_title_filter=True, scan_job_id=scan_job_id)

            total_articles += len(articles)
            total_deals_extracted += stats.get("deals_extracted", 0)
            total_deals_saved += stats.get("deals_saved", 0)

            results["brave_search"] = BraveSearchResponse(
                queries_executed=20,  # Was 38, now 20 after participation disabled
                results_found=len(articles),
                articles_generated=len(articles),
                deals_extracted=stats.get("deals_extracted", 0),
                deals_saved=stats.get("deals_saved", 0),
            )
        except Exception as e:
            logger.error(f"Brave Search error: {e}")
            total_errors += 1
            results["brave_search"] = None
    else:
        results["brave_search"] = None

    # Google Alerts (if configured)
    if settings.google_alerts_feeds:
        try:
            from .scheduler.jobs import process_external_articles
            feed_urls = [f.strip() for f in settings.google_alerts_feeds.split(',') if f.strip()]
            async with GoogleAlertsScraper(feed_urls) as scraper:
                articles = await scraper.scrape_all()

            # Process articles through extraction pipeline
            stats = await process_external_articles(articles, "google_alerts", scan_job_id=scan_job_id)

            results["google_alerts"] = GoogleAlertsResponse(
                feeds_configured=len(feed_urls),
                articles_found=len(articles),
                articles_skipped_duplicate=stats.get("articles_skipped_duplicate", 0),
                deals_extracted=stats.get("deals_extracted", 0),
                deals_saved=stats.get("deals_saved", 0),
                errors=stats.get("errors", 0),
            )
        except Exception:
            results["google_alerts"] = None
    else:
        results["google_alerts"] = None

    # Twitter (if configured)
    if settings.twitter_bearer_token:
        try:
            async with TwitterMonitor() as monitor:
                articles = await monitor.scrape_all(hours_back=days * 24)
                api_calls = monitor._request_count

            results["twitter"] = TwitterMonitorResponse(
                tweets_found=len(articles),
                api_calls_used=api_calls,
                configured=True,
            )
        except Exception:
            results["twitter"] = None
    else:
        results["twitter"] = None

    # TechCrunch RSS (always runs - free)
    try:
        from .harvester.scrapers.techcrunch_rss import TechCrunchScraper
        async with TechCrunchScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        results["techcrunch"] = TechCrunchResponse(
            articles_found=len(articles),
            funding_articles=len([a for a in articles if a.fund_slug]),
        )
    except Exception:
        results["techcrunch"] = None

    # Fortune Term Sheet (always runs - free)
    try:
        from .harvester.scrapers.fortune_term_sheet import FortuneTermSheetScraper
        async with FortuneTermSheetScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        results["fortune"] = FortuneResponse(
            articles_found=len(articles),
            funding_articles=len([a for a in articles if a.fund_slug]),
        )
    except Exception:
        results["fortune"] = None

    # Y Combinator (always runs - free)
    try:
        from .harvester.scrapers.ycombinator import YCombinatorScraper
        async with YCombinatorScraper() as scraper:
            articles = await scraper.scrape_all()
        companies = len([a for a in articles if 'demo_day' in a.tags])
        blog_posts = len([a for a in articles if 'blog' in a.tags])
        results["ycombinator"] = YCombinatorResponse(
            companies_found=companies,
            blog_posts_found=blog_posts,
            total_articles=len(articles),
        )
    except Exception:
        results["ycombinator"] = None

    # GitHub Trending (always runs - free)
    try:
        from .harvester.scrapers.github_trending import GitHubTrendingScraper
        async with GitHubTrendingScraper() as scraper:
            # Fetch once, pass to scrape_all to avoid double-fetch
            all_repos = await scraper.fetch_all_trending(since="daily")
            articles = await scraper.scrape_all(since="daily", filter_enterprise=True, repos=all_repos)
        results["github_trending"] = GitHubTrendingResponse(
            repos_found=len(all_repos),
            enterprise_devtools=len(articles),
        )
    except Exception:
        results["github_trending"] = None

    # Hacker News (always runs - free)
    try:
        from .harvester.scrapers.hackernews import HackerNewsScraper
        async with HackerNewsScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        launch_hn = len([a for a in articles if 'launch_hn' in a.tags])
        show_hn = len([a for a in articles if 'show_hn' in a.tags])
        funding = len([a for a in articles if a.fund_slug])
        results["hackernews"] = HackerNewsResponse(
            launch_hn_posts=launch_hn,
            show_hn_posts=show_hn,
            funding_posts=funding,
            total_articles=len(articles),
        )
    except Exception:
        results["hackernews"] = None

    # LinkedIn Jobs - Stealth Detection (if Brave Search configured)
    if settings.brave_search_key:
        try:
            from .harvester.scrapers.linkedin_jobs import LinkedInJobsScraper, STEALTH_JOB_QUERIES
            async with LinkedInJobsScraper() as scraper:
                articles = await scraper.scrape_all()
            stealth_count = len([a for a in articles if 'stealth' in a.tags])
            fund_matches = len([a for a in articles if a.fund_slug])
            results["linkedin_jobs"] = LinkedInJobsResponse(
                queries_executed=len(STEALTH_JOB_QUERIES),
                stealth_signals_found=stealth_count,
                fund_matches=fund_matches,
                total_jobs=len(articles),
            )
        except Exception:
            results["linkedin_jobs"] = None
    else:
        results["linkedin_jobs"] = None

    # Tech Funding News (always runs - free RSS)
    try:
        from .harvester.scrapers.tech_funding_news import TechFundingNewsScraper
        async with TechFundingNewsScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["tech_funding_news"] = TechFundingNewsResponse(
            feeds_scraped=5,
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception:
        results["tech_funding_news"] = None

    # Ventureburn (always runs - free RSS)
    try:
        from .harvester.scrapers.ventureburn import VentureburnScraper
        async with VentureburnScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["ventureburn"] = VentureburnResponse(
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception:
        results["ventureburn"] = None

    # Crunchbase News (always runs - free RSS)
    try:
        from .harvester.scrapers.crunchbase_news import CrunchbaseNewsScraper
        async with CrunchbaseNewsScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["crunchbase_news"] = CrunchbaseNewsResponse(
            feeds_scraped=3,
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception:
        results["crunchbase_news"] = None

    # VentureBeat (always runs - free RSS)
    try:
        from .harvester.scrapers.venturebeat import VentureBeatScraper
        async with VentureBeatScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["venturebeat"] = VentureBeatResponse(
            feeds_scraped=4,
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception:
        results["venturebeat"] = None

    # Axios Pro Rata (always runs - free RSS, may return 0 if deprecated)
    try:
        from .harvester.scrapers.axios_prorata import AxiosProRataScraper
        async with AxiosProRataScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["axios_prorata"] = AxiosProRataResponse(
            feeds_scraped=3,
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception:
        results["axios_prorata"] = None

    # StrictlyVC (always runs - free RSS)
    try:
        from .harvester.scrapers.strictlyvc import StrictlyVCScraper
        async with StrictlyVCScraper() as scraper:
            articles = await scraper.scrape_all(hours_back=days * 24)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["strictlyvc"] = StrictlyVCResponse(
            feeds_scraped=1,
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception:
        results["strictlyvc"] = None

    # PR Wire RSS (always runs - free RSS from PRNewswire, GlobeNewswire)
    try:
        from .harvester.scrapers.prwire_rss import PRWireRSSScraper
        async with PRWireRSSScraper() as scraper:
            articles = await scraper.scrape_all_feeds(hours_back=days * 24, fund_filter=True)
        fund_matches = len([a for a in articles if a.fund_slug])
        results["prwire"] = PRWireResponse(
            feeds_scraped=3,  # 1 PRNewswire + 2 GlobeNewswire
            articles_found=len(articles),
            funding_articles=len(articles),
            fund_matches=fund_matches,
        )
    except Exception as e:
        logger.warning(f"PR wire RSS error: {e}")
        results["prwire"] = None

    # Google News RSS (external-only fund coverage - Thrive, Benchmark, etc.)
    try:
        from .harvester.scrapers.google_news_rss import GoogleNewsRSSScraper, GOOGLE_NEWS_FUNDS
        async with GoogleNewsRSSScraper() as scraper:
            articles = await scraper.scrape_all(days_back=days)
        # Process through extraction pipeline
        stats = await process_external_articles(articles, "google_news", skip_title_filter=True, scan_job_id=scan_job_id)
        total_articles += len(articles)
        total_deals_extracted += stats.get("deals_extracted", 0)
        total_deals_saved += stats.get("deals_saved", 0)
        results["google_news"] = GoogleNewsResponse(
            articles_found=len(articles),
            fund_matches=len([a for a in articles if a.fund_slug]),
            funds_covered=list(GOOGLE_NEWS_FUNDS.keys()),
        )
    except Exception as e:
        logger.warning(f"Google News RSS error: {e}")
        total_errors += 1
        results["google_news"] = None

    # ----- Tier 1 Stealth Detection -----

    # Portfolio Diff (always runs - free, scrapes public portfolio pages)
    try:
        from .harvester.scrapers.portfolio_diff import PortfolioDiffScraper
        async with PortfolioDiffScraper() as scraper:
            articles = await scraper.scrape_all()
        results["portfolio_diff"] = PortfolioDiffResponse(
            funds_checked=18,
            new_companies_found=len(articles),
            total_articles=len(articles),
        )
    except Exception:
        results["portfolio_diff"] = None

    # USPTO Trademarks (if Brave Search configured)
    if settings.brave_search_key:
        try:
            from .harvester.scrapers.uspto_trademarks import USPTOTrademarkScraper
            async with USPTOTrademarkScraper() as scraper:
                trademarks = await scraper.search_recent_trademarks(days_back=days)
                articles = await scraper.scrape_all(days_back=days, tech_only=True)
            results["uspto_trademarks"] = USPTOTrademarkResponse(
                trademarks_found=len(trademarks),
                tech_trademarks=len(articles),
                total_articles=len(articles),
            )
        except Exception:
            results["uspto_trademarks"] = None
    else:
        results["uspto_trademarks"] = None

    # Delaware Corps (if Brave Search configured)
    if settings.brave_search_key:
        try:
            from .harvester.scrapers.delaware_corps import DelawareCorpsScraper
            async with DelawareCorpsScraper() as scraper:
                articles = await scraper.scrape_all(days_back=days, min_score=3)
                brave_entities = await scraper.search_recent_incorporations(days_back=days)
                agg_entities = await scraper.search_aggregators()
            all_entities = len(brave_entities) + len(agg_entities)
            tech_entities = len([e for e in brave_entities if e.has_tech_name] +
                              [e for e in agg_entities if e.has_tech_name])
            high_signal = len([e for e in brave_entities if e.has_startup_agent] +
                             [e for e in agg_entities if e.has_startup_agent])
            results["delaware_corps"] = DelawareCorpsResponse(
                entities_found=all_entities,
                tech_entities=tech_entities,
                high_signal_entities=high_signal,
                total_articles=len(articles),
            )
        except Exception:
            results["delaware_corps"] = None
    else:
        results["delaware_corps"] = None

    cache.invalidate("deals")

    # Update ScanJob with final stats
    await update_scan_job(
        scan_job_id,
        status="success",
        stats={
            "articles_found": total_articles,
            "deals_extracted": total_deals_extracted,
            "deals_saved": total_deals_saved,
            "errors": total_errors,
            "source_results": {k: v.model_dump() if v else None for k, v in results.items()},
        },
        start_time=start_time,
    )

    return AllSourcesResponse(**results)


# ----- Scheduler Status -----

class SchedulerJobStatus(BaseModel):
    id: str
    name: str
    next_run: Optional[str] = None


class SchedulerStatusResponse(BaseModel):
    status: str
    jobs: List[SchedulerJobStatus]
    scan_frequency: str = "unknown"  # daily, 3x_daily, 4_hourly
    # Job execution tracking
    last_run: Optional[str] = None
    last_status: Optional[str] = None
    last_error: Optional[str] = None
    last_duration_seconds: Optional[float] = None


@app.get("/scheduler/status", response_model=SchedulerStatusResponse)
async def get_scheduler_status():
    """Get scheduler status and next run times."""
    from .config.settings import settings

    sched = scheduler_module.scheduler
    if not sched:
        return SchedulerStatusResponse(
            status="not_initialized",
            jobs=[],
            scan_frequency=settings.scan_frequency,
        )

    jobs = []
    for job in sched.get_jobs():
        jobs.append(SchedulerJobStatus(
            id=job.id,
            name=job.name,
            next_run=job.next_run_time.isoformat() if job.next_run_time else None,
        ))

    # Get job execution tracking from scheduler module
    last_run = getattr(scheduler_module, '_last_job_run', None)
    last_status = getattr(scheduler_module, '_last_job_status', None)
    last_error = getattr(scheduler_module, '_last_job_error', None)
    last_duration = getattr(scheduler_module, '_last_job_duration', None)

    return SchedulerStatusResponse(
        status="running" if sched.running else "stopped",
        jobs=jobs,
        scan_frequency=settings.scan_frequency,
        last_run=last_run.isoformat() if last_run else None,
        last_status=last_status,
        last_error=last_error,
        last_duration_seconds=last_duration,
    )


class TriggerJobResponse(BaseModel):
    """Response for triggering the scheduled job."""
    status: str
    message: str
    job_id: Optional[str] = None


@app.post("/scheduler/trigger", response_model=TriggerJobResponse)
async def trigger_scheduled_job(
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key),
):
    """
    Manually trigger the scheduled scrape job.

    This runs the full pipeline:
    1. Scrape all 18 fund websites
    2. Scrape external sources (Brave, SEC, RSS feeds)
    3. Process through Claude extraction
    4. Save new deals to database
    5. Enrich deals (website + LinkedIn)

    The job runs in the background and can take 5-15 minutes.
    Check /scheduler/status for progress.
    """
    from .scheduler.jobs import scheduled_scrape_job

    # Run in background
    background_tasks.add_task(scheduled_scrape_job, "api")

    return TriggerJobResponse(
        status="started",
        message="Scheduled job triggered. Check /scheduler/status for progress.",
        job_id=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
    )


# ----- Scan History -----

class ScanDealResponse(BaseModel):
    """Simplified deal info for scan results."""
    id: str
    startup_name: str
    round_type: str
    amount: Optional[str] = None
    lead_investor: Optional[str] = None
    is_lead: bool = False
    is_enterprise_ai: bool = False
    enterprise_category: Optional[str] = None
    source_name: Optional[str] = None
    founders: Optional[List[FounderResponse]] = None


class ScanSourceStats(BaseModel):
    """Stats for a single source within a scan."""
    articles_found: int = 0
    deals_extracted: int = 0
    deals_saved: int = 0
    duplicates_skipped: int = 0
    errors: int = 0
    error_message: Optional[str] = None


class ScanResponse(BaseModel):
    """Response for a scan job."""
    id: int
    job_id: str
    started_at: str
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    status: str
    trigger: str
    total_articles_found: int = 0
    total_deals_extracted: int = 0
    total_deals_saved: int = 0
    total_duplicates_skipped: int = 0
    total_errors: int = 0
    lead_deals_found: int = 0
    enterprise_ai_deals_found: int = 0
    error_message: Optional[str] = None


class ScanDetailResponse(ScanResponse):
    """Detailed response including deals and per-source breakdown."""
    source_results: Optional[Dict[str, ScanSourceStats]] = None
    deals: List[ScanDealResponse] = []


class ScanListResponse(BaseModel):
    """Paginated list of scans."""
    scans: List[ScanResponse]
    total_count: int
    page: int
    limit: int


@app.get("/scans", response_model=ScanListResponse)
async def list_scans(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None, description="Filter by status (running, success, failed)"),
    api_key: str = Depends(verify_api_key),
):
    """List all scans with pagination, most recent first."""
    from .archivist.models import ScanJob

    async with get_session() as session:
        # Build query
        query = select(ScanJob).order_by(ScanJob.started_at.desc())

        if status:
            query = query.where(ScanJob.status == status)

        # Get total count
        count_query = select(func.count(ScanJob.id))
        if status:
            count_query = count_query.where(ScanJob.status == status)
        count_result = await session.execute(count_query)
        total_count = count_result.scalar() or 0

        # Get paginated results
        query = query.offset((page - 1) * limit).limit(limit)
        result = await session.execute(query)
        scans = result.scalars().all()

        scan_responses = []
        for scan in scans:
            scan_responses.append(ScanResponse(
                id=scan.id,
                job_id=scan.job_id,
                started_at=scan.started_at.isoformat() if scan.started_at else "",
                completed_at=scan.completed_at.isoformat() if scan.completed_at else None,
                duration_seconds=scan.duration_seconds,
                status=scan.status,
                trigger=scan.trigger,
                total_articles_found=scan.total_articles_found,
                total_deals_extracted=scan.total_deals_extracted,
                total_deals_saved=scan.total_deals_saved,
                total_duplicates_skipped=scan.total_duplicates_skipped,
                total_errors=scan.total_errors,
                lead_deals_found=scan.lead_deals_found,
                enterprise_ai_deals_found=scan.enterprise_ai_deals_found,
                error_message=scan.error_message,
            ))

        return ScanListResponse(
            scans=scan_responses,
            total_count=total_count,
            page=page,
            limit=limit,
        )


@app.get("/scans/{scan_id}", response_model=ScanDetailResponse)
async def get_scan_detail(
    scan_id: int,
    api_key: str = Depends(verify_api_key),
):
    """Get detailed scan info including deals found."""
    from .archivist.models import ScanJob

    async with get_session() as session:
        # Get scan
        scan_result = await session.execute(
            select(ScanJob).where(ScanJob.id == scan_id)
        )
        scan = scan_result.scalar_one_or_none()

        if not scan:
            raise HTTPException(status_code=404, detail="Scan not found")

        # Get deals for this scan
        deals_result = await session.execute(
            select(Deal, PortfolioCompany)
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .where(Deal.scan_job_id == scan_id)
            .order_by(Deal.created_at.desc())
        )
        deal_rows = deals_result.all()

        # Parse source results JSON
        source_results = None
        if scan.source_results_json:
            try:
                raw_results = json.loads(scan.source_results_json)
                source_results = {}
                for source_name, stats in raw_results.items():
                    if source_name == "total_deals_saved" or not isinstance(stats, dict):
                        continue
                    if "error" in stats:
                        source_results[source_name] = ScanSourceStats(
                            error_message=str(stats.get("error", ""))
                        )
                    else:
                        source_results[source_name] = ScanSourceStats(
                            articles_found=stats.get("articles_received", stats.get("articles_found", 0)),
                            deals_extracted=stats.get("deals_extracted", 0),
                            deals_saved=stats.get("deals_saved", 0),
                            duplicates_skipped=stats.get("articles_skipped_duplicate", 0),
                            errors=stats.get("errors", 0),
                        )
            except json.JSONDecodeError:
                pass

        # Build deal responses
        deal_responses = []
        for deal, company in deal_rows:
            # Get lead investor from deal_investors (use first() for co-lead deals)
            investors_result = await session.execute(
                select(DealInvestor)
                .where(DealInvestor.deal_id == deal.id)
                .where(DealInvestor.is_lead == True)
                .limit(1)
            )
            lead_investor = investors_result.scalar_one_or_none()

            # Parse founders
            founders = None
            if deal.founders_json:
                try:
                    founders_data = json.loads(deal.founders_json)
                    founders = [
                        FounderResponse(
                            name=f.get("name", ""),
                            title=f.get("title"),
                            linkedin_url=f.get("linkedin_url"),
                        )
                        for f in founders_data
                    ]
                except json.JSONDecodeError:
                    pass

            # Get source from article
            article_result = await session.execute(
                select(Article).where(Article.deal_id == deal.id).limit(1)
            )
            article = article_result.scalar_one_or_none()

            deal_responses.append(ScanDealResponse(
                id=str(deal.id),
                startup_name=company.name,
                round_type=deal.round_type,
                amount=deal.amount,
                lead_investor=lead_investor.investor_name if lead_investor else None,
                is_lead=deal.is_lead_confirmed,
                is_enterprise_ai=deal.is_enterprise_ai,
                enterprise_category=deal.enterprise_category,
                source_name=article.source_fund_slug if article else None,
                founders=founders,
            ))

        return ScanDetailResponse(
            id=scan.id,
            job_id=scan.job_id,
            started_at=scan.started_at.isoformat() if scan.started_at else "",
            completed_at=scan.completed_at.isoformat() if scan.completed_at else None,
            duration_seconds=scan.duration_seconds,
            status=scan.status,
            trigger=scan.trigger,
            total_articles_found=scan.total_articles_found,
            total_deals_extracted=scan.total_deals_extracted,
            total_deals_saved=scan.total_deals_saved,
            total_duplicates_skipped=scan.total_duplicates_skipped,
            total_errors=scan.total_errors,
            lead_deals_found=scan.lead_deals_found,
            enterprise_ai_deals_found=scan.enterprise_ai_deals_found,
            error_message=scan.error_message,
            source_results=source_results,
            deals=deal_responses,
        )


@app.post("/scans/backfill-counts")
async def backfill_scan_counts(api_key: str = Depends(verify_api_key)):
    """
    Backfill lead_deals_found and enterprise_ai_deals_found for existing scans.

    Run once to fix historical scan records that have 0 counts.
    """
    from .archivist.models import ScanJob
    from sqlalchemy import Integer

    updated = 0
    async with get_session() as session:
        # Get all scans with 0 lead/enterprise counts but have deals
        scans_result = await session.execute(
            select(ScanJob).where(
                ScanJob.lead_deals_found == 0,
                ScanJob.enterprise_ai_deals_found == 0,
            )
        )
        scans = scans_result.scalars().all()

        for scan in scans:
            # Count actual deals for this scan
            count_result = await session.execute(
                select(
                    func.count(Deal.id).label("total"),
                    func.sum(func.cast(Deal.is_lead_confirmed, Integer)).label("leads"),
                    func.sum(func.cast(Deal.is_enterprise_ai, Integer)).label("enterprise_ai"),
                ).where(Deal.scan_job_id == scan.id)
            )
            counts = count_result.one()
            lead_deals = int(counts.leads or 0)
            enterprise_ai_deals = int(counts.enterprise_ai or 0)
            total = int(counts.total or 0)

            if lead_deals > 0 or enterprise_ai_deals > 0 or total > 0:
                scan.lead_deals_found = lead_deals
                scan.enterprise_ai_deals_found = enterprise_ai_deals
                if total > scan.total_deals_saved:
                    scan.total_deals_saved = total
                updated += 1

        await session.commit()

    return {
        "status": "success",
        "scans_updated": updated,
    }


@app.post("/scans/cleanup-stale")
async def cleanup_stale_scans(api_key: str = Depends(verify_api_key)):
    """
    Mark scans stuck in 'running' for >1 hour as failed.

    Use this to clean up scans that timed out before the timeout fix was deployed.
    """
    from .archivist.models import ScanJob

    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

    async with get_session() as session:
        result = await session.execute(
            select(ScanJob).where(
                ScanJob.status == "running",
                ScanJob.started_at < cutoff
            )
        )
        stale_scans = result.scalars().all()

        scan_ids = []
        for scan in stale_scans:
            scan.status = "failed"
            scan.error_message = "Marked as failed by cleanup (stuck >1 hour)"
            scan.completed_at = datetime.now(timezone.utc)
            scan_ids.append(scan.id)

        await session.commit()

    return {
        "status": "success",
        "cleaned_up": len(scan_ids),
        "scan_ids": scan_ids,
    }


# ----- Cache Management -----

@app.post("/cache/clear")
async def clear_cache(api_key: str = Depends(verify_api_key)):
    """Clear all cached responses."""
    cache.clear()
    return {"status": "cache cleared"}


# ----- Company Enrichment -----

class EnrichmentResult(BaseModel):
    company_name: str
    website: Optional[str] = None
    company_linkedin: Optional[str] = None
    ceo_name: Optional[str] = None
    ceo_linkedin: Optional[str] = None


class EnrichmentResponse(BaseModel):
    companies_enriched: int
    companies_updated: int
    results: List[EnrichmentResult]


@app.post("/enrichment/company", response_model=EnrichmentResult)
async def enrich_single_company(
    company_name: str = Query(..., description="Company name to enrich"),
    api_key: str = Depends(verify_api_key),
):
    """
    Enrich a single company with website and CEO LinkedIn.

    Uses Brave Search to find:
    - Official company website
    - CEO/Founder LinkedIn profile
    """
    from .enrichment import enrich_company

    result = await enrich_company(company_name)

    return EnrichmentResult(
        company_name=result.company_name,
        website=result.website,
        company_linkedin=result.company_linkedin,
        ceo_name=result.ceo_name,
        ceo_linkedin=result.ceo_linkedin,
    )


@app.post("/enrichment/deals", response_model=EnrichmentResponse)
async def enrich_deals(
    limit: int = Query(10, ge=1, le=50, description="Max deals to enrich"),
    offset: int = Query(0, ge=0, description="Number of deals to skip for pagination"),
    skip_enriched: bool = Query(True, description="Skip deals that already have data"),
    force_update: bool = Query(False, description="Overwrite existing data with new enrichment"),
    api_key: str = Depends(verify_api_key),
    db: AsyncSession = Depends(get_db),
):
    """
    Enrich deals with missing website/LinkedIn data.

    Uses deal context (investor, founders, category) for accurate enrichment.
    Finds deals missing company_website or founder LinkedIn,
    then uses Brave Search to populate the data.

    Use force_update=true to overwrite existing (potentially wrong) data.
    """
    from .enrichment import enrich_company_with_context, DealContext
    import json as json_module

    # Find deals missing enrichment data
    query = (
        select(Deal)
        .options(
            selectinload(Deal.company),
            selectinload(Deal.investors),
            selectinload(Deal.articles),
        )
        .join(Deal.company)
    )

    if skip_enriched:
        # Get deals where website OR founders are missing/incomplete
        # This ensures we enrich both website and founder LinkedIn data
        query = query.where(
            (PortfolioCompany.website.is_(None)) | (PortfolioCompany.website == "") |
            (Deal.founders_json.is_(None)) | (Deal.founders_json == "[]") | (Deal.founders_json == "")
        )

    query = query.order_by(Deal.created_at.desc()).offset(offset).limit(limit)

    result = await db.execute(query)
    deals = result.scalars().all()

    results = []
    updated_count = 0

    for deal in deals:
        company_name = deal.company.name if deal.company else None
        if not company_name:
            continue

        try:
            # Build context from deal data
            founders = []
            if deal.founders_json:
                try:
                    founders = json_module.loads(deal.founders_json)
                except (json_module.JSONDecodeError, TypeError, ValueError):
                    founders = []

            # Extract lead investor from deal.investors relationship
            lead_investor = None
            for inv in deal.investors:
                if inv.is_lead:
                    lead_investor = inv.investor_name
                    break

            # Extract source URL from deal.articles relationship
            source_url = deal.articles[0].url if deal.articles else None

            context = DealContext(
                company_name=company_name,
                lead_investor=lead_investor,
                founders=founders,
                source_url=source_url,
                enterprise_category=deal.enterprise_category,
            )

            # Use context-aware enrichment
            enrichment = await enrich_company_with_context(context)

            # Update company in database
            if deal.company:
                # Validate and sanitize website URL before persistence
                if enrichment.website and (not deal.company.website or force_update):
                    sanitized_website = sanitize_url(enrichment.website)
                    if sanitized_website and is_valid_website_url(sanitized_website):
                        deal.company.website = sanitized_website
                        updated_count += 1
                    else:
                        logger.warning(
                            f"Rejecting invalid website URL for {company_name}: {enrichment.website}"
                        )

                # Validate and sanitize company LinkedIn URL before persistence
                if enrichment.company_linkedin and (not deal.company.linkedin_url or force_update):
                    sanitized_linkedin = sanitize_linkedin_url(enrichment.company_linkedin)
                    if sanitized_linkedin and is_valid_linkedin_company(sanitized_linkedin):
                        deal.company.linkedin_url = sanitized_linkedin
                    else:
                        logger.warning(
                            f"Rejecting invalid company LinkedIn URL for {company_name}: {enrichment.company_linkedin}"
                        )

                # Update founders_json with LinkedIn URLs (with validation)
                if enrichment.founder_linkedins or enrichment.ceo_linkedin:
                    # FIX (2026-01): Create case-insensitive lookup for founder LinkedIn URLs
                    # Previously case mismatch (e.g., "John Smith" vs "john smith") caused data loss
                    founder_linkedins_lower = {
                        k.lower(): v for k, v in (enrichment.founder_linkedins or {}).items()
                    }

                    # Update existing founders with LinkedIn URLs
                    for f in founders:
                        founder_name = f.get("name", "")
                        founder_name_lower = founder_name.lower()
                        if founder_linkedins_lower and founder_name_lower in founder_linkedins_lower:
                            if not f.get("linkedin_url") or force_update:
                                # Validate founder LinkedIn URL before persisting
                                raw_url = founder_linkedins_lower[founder_name_lower]
                                sanitized = sanitize_linkedin_url(raw_url)
                                if sanitized and is_valid_linkedin_profile(sanitized):
                                    f["linkedin_url"] = sanitized
                                else:
                                    logger.warning(
                                        f"Rejecting invalid founder LinkedIn for {founder_name}: {raw_url}"
                                    )

                    # Fallback: if no founder matched, add CEO
                    if enrichment.ceo_linkedin:
                        # Validate CEO LinkedIn URL first
                        sanitized_ceo_linkedin = sanitize_linkedin_url(enrichment.ceo_linkedin)
                        if sanitized_ceo_linkedin and is_valid_linkedin_profile(sanitized_ceo_linkedin):
                            ceo_found = any(f.get("linkedin_url") for f in founders)
                            if not ceo_found:
                                # FIX: If founders list is empty, CREATE a new founder entry
                                if not founders and enrichment.ceo_name:
                                    founders.append({
                                        "name": enrichment.ceo_name,
                                        "title": "CEO",
                                        "linkedin_url": sanitized_ceo_linkedin,
                                    })
                                    logger.info(f"Added CEO as founder for {company_name}: {enrichment.ceo_name}")
                                else:
                                    # Try to find existing CEO/founder entry to update
                                    # Use substring matching for compound titles like "Founder & CEO"
                                    for f in founders:
                                        title_lower = f.get("title", "").lower()
                                        if any(role in title_lower for role in ("ceo", "founder")):
                                            if not f.get("linkedin_url") or force_update:
                                                f["linkedin_url"] = sanitized_ceo_linkedin
                                            break
                        else:
                            logger.warning(
                                f"Rejecting invalid CEO LinkedIn for {company_name}: {enrichment.ceo_linkedin}"
                            )

                    deal.founders_json = json_module.dumps(founders)

            results.append(EnrichmentResult(
                company_name=company_name,
                website=enrichment.website,
                company_linkedin=enrichment.company_linkedin,
                ceo_name=enrichment.ceo_name,
                ceo_linkedin=enrichment.ceo_linkedin,
            ))

            # Rate limit
            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"Error enriching {company_name}: {e}")
            results.append(EnrichmentResult(company_name=company_name))

    await db.commit()

    return EnrichmentResponse(
        companies_enriched=len(results),
        companies_updated=updated_count,
        results=results,
    )


# ----- Date Backfill & Cleanup -----

class DateBackfillResponse(BaseModel):
    deals_processed: int
    deals_updated: int
    deals_skipped_no_article: int
    deals_skipped_already_has_date: int


@app.post("/enrichment/backfill-dates", response_model=DateBackfillResponse)
async def backfill_deal_dates(
    limit: int = Query(100, ge=1, le=500, description="Max deals to process"),
    api_key: str = Depends(verify_api_key),
):
    """
    Backfill missing announced_date for deals.

    Uses article.published_date as fallback for deals missing announced_date.
    This improves date coverage from ~50-60% to ~95%+.
    """
    from datetime import date

    processed = 0
    updated = 0
    skipped_no_article = 0
    skipped_has_date = 0

    try:
        async with get_session() as session:
            # Find deals missing announced_date
            stmt = (
                select(Deal)
                .options(selectinload(Deal.articles))
                .where(Deal.announced_date.is_(None))
                .order_by(Deal.created_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            deals = result.scalars().all()

            for deal in deals:
                processed += 1

                # Skip if deal already has a date (shouldn't happen with our query)
                if deal.announced_date:
                    skipped_has_date += 1
                    continue

                # Try to get date from linked article
                if deal.articles:
                    article = deal.articles[0]
                    if article.published_date:
                        deal.announced_date = article.published_date
                        updated += 1
                        continue

                # No article or no published_date
                skipped_no_article += 1

            await session.commit()

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    return DateBackfillResponse(
        deals_processed=processed,
        deals_updated=updated,
        deals_skipped_no_article=skipped_no_article,
        deals_skipped_already_has_date=skipped_has_date,
    )


class DateEnrichmentResultResponse(BaseModel):
    """Single deal date enrichment result."""
    deal_id: int
    company_name: str
    round_type: str
    found_date: Optional[str] = None
    date_source: Optional[str] = None
    search_url: Optional[str] = None
    confidence: str = "low"


class DateEnrichmentResponse(BaseModel):
    """Response for date enrichment via Brave Search."""
    deals_processed: int
    deals_enriched: int
    deals_not_found: int
    results: List[DateEnrichmentResultResponse]


@app.post("/enrichment/dates", response_model=DateEnrichmentResponse)
async def enrich_deal_dates(
    limit: int = Query(50, ge=1, le=200, description="Max deals to process"),
    force: bool = Query(False, description="Re-enrich deals that already have dates"),
    persist: bool = Query(True, description="Save results to database"),
    api_key: str = Depends(verify_api_key),
):
    """
    Enrich deals missing announced_date via Brave Search.

    Searches for funding announcement dates for deals where:
    - LLM couldn't extract a date from the article
    - Portfolio page scrapes had no article metadata
    - article.published_date fallback was also None

    Uses Brave Search to find the actual funding announcement and extracts
    the date from search result metadata (page_age) or snippets.

    Query strategies:
    1. "[Company] [Round] raises funding"
    2. "[Company] [Round] funding announced"
    3. "[Company] [Round] [Lead Investor] funding" (if lead investor known)
    """
    import asyncio as aio

    # Import with error handling
    try:
        from .enrichment.date_enrichment import DateEnrichmentClient
    except ImportError as e:
        logger.error("Failed to import DateEnrichmentClient: %s", e)
        raise HTTPException(status_code=500, detail=f"Import error: {e}")

    processed = 0
    enriched = 0
    not_found = 0
    results: List[DateEnrichmentResultResponse] = []

    try:
        async with get_session() as session:
            # Find deals missing announced_date (or all deals if force=True)
            if force:
                stmt = (
                    select(Deal)
                    .options(
                        selectinload(Deal.company),
                        selectinload(Deal.investors),
                    )
                    .order_by(Deal.created_at.desc())
                    .limit(limit)
                )
            else:
                stmt = (
                    select(Deal)
                    .options(
                        selectinload(Deal.company),
                        selectinload(Deal.investors),
                    )
                    .where(Deal.announced_date.is_(None))
                    .order_by(Deal.created_at.desc())
                    .limit(limit)
                )

            result = await session.execute(stmt)
            deals = result.scalars().all()

            if not deals:
                return DateEnrichmentResponse(
                    deals_processed=0,
                    deals_enriched=0,
                    deals_not_found=0,
                    results=[],
                )

            # Enrich each deal
            async with DateEnrichmentClient() as client:
                for deal in deals:
                    processed += 1

                    company_name = deal.company.name if deal.company else None
                    if not company_name or company_name.lower() in ("<unknown>", "unknown", "n/a"):
                        not_found += 1
                        continue

                    round_type = deal.round_type or "funding"

                    # Get lead investor if available
                    lead_investor = None
                    if deal.investors:
                        for inv in deal.investors:
                            if inv.is_lead:
                                lead_investor = inv.investor_name
                                break

                    # Enrich via Brave Search
                    enrichment_result = await client.enrich_deal_date(
                        deal_id=deal.id,
                        company_name=company_name,
                        round_type=round_type,
                        lead_investor=lead_investor,
                    )

                    # Build response
                    result_response = DateEnrichmentResultResponse(
                        deal_id=deal.id,
                        company_name=company_name,
                        round_type=round_type,
                        found_date=str(enrichment_result.found_date) if enrichment_result.found_date else None,
                        date_source=enrichment_result.date_source,
                        search_url=enrichment_result.search_url,
                        confidence=enrichment_result.confidence,
                    )
                    results.append(result_response)

                    if enrichment_result.found_date:
                        enriched += 1

                        # Persist to database directly (same session)
                        if persist:
                            deal.announced_date = enrichment_result.found_date
                            deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    else:
                        not_found += 1

                    # Rate limit between searches
                    await aio.sleep(0.5)

            # Commit all changes at once
            if persist:
                await session.commit()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Date enrichment failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Date enrichment error: {type(e).__name__}: {str(e)}")

    return DateEnrichmentResponse(
        deals_processed=processed,
        deals_enriched=enriched,
        deals_not_found=not_found,
        results=results,
    )


class CrossReferenceDatesResponse(BaseModel):
    deals_processed: int
    deals_updated: int
    deals_already_earliest: int
    details: list


@app.post("/deals/cross-reference-dates", response_model=CrossReferenceDatesResponse)
async def cross_reference_dates(
    limit: int = Query(500, ge=1, le=1000, description="Max deals to process"),
    api_key: str = Depends(verify_api_key),
):
    """
    Cross-reference deals with multiple articles to find the earliest announcement date.

    For each deal, checks all linked articles and updates announced_date to the earliest
    published_date found. This corrects cases where a deal was first scraped from a
    secondary source but later found in an earlier article.
    """
    from sqlalchemy import func

    processed = 0
    updated = 0
    already_earliest = 0
    details = []

    try:
        async with get_session() as session:
            # Find deals with their articles
            stmt = (
                select(Deal)
                .options(selectinload(Deal.articles))
                .options(selectinload(Deal.company))
                .order_by(Deal.created_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            deals = result.scalars().all()

            for deal in deals:
                processed += 1

                if not deal.articles:
                    continue

                # Find earliest date among all linked articles
                article_dates = [
                    a.published_date for a in deal.articles
                    if a.published_date is not None
                ]

                if not article_dates:
                    continue

                earliest_date = min(article_dates)

                # Update if earliest is before current announced_date
                if deal.announced_date:
                    if earliest_date < deal.announced_date:
                        company_name = deal.company.name if deal.company else "Unknown"
                        details.append({
                            "deal_id": deal.id,
                            "company": company_name,
                            "old_date": str(deal.announced_date),
                            "new_date": str(earliest_date),
                            "articles_checked": len(deal.articles),
                        })
                        deal.announced_date = earliest_date
                        deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                        updated += 1
                    else:
                        already_earliest += 1
                else:
                    # Deal had no date, set it
                    deal.announced_date = earliest_date
                    deal.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    updated += 1

            await session.commit()

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    return CrossReferenceDatesResponse(
        deals_processed=processed,
        deals_updated=updated,
        deals_already_earliest=already_earliest,
        details=details[:50],  # Limit details to 50 for response size
    )


class CleanupOldDealsResponse(BaseModel):
    deals_deleted: int
    cutoff_date: str


@app.post("/deals/cleanup-old", response_model=CleanupOldDealsResponse)
async def cleanup_old_deals(
    cutoff_date: str = Query("2023-12-01", description="Delete deals before this date (YYYY-MM-DD)"),
    include_undated: bool = Query(True, description="Also delete deals without dates that were created before cutoff"),
    api_key: str = Depends(verify_api_key),
):
    """
    Delete deals older than the cutoff date.

    Cleans up old data to focus on recent deals (past year).
    Use cutoff_date=2023-12-01 to keep only deals from the past year.
    Also deletes related articles and deal_investors.
    """
    from datetime import datetime as dt
    from sqlalchemy import delete

    try:
        cutoff = dt.strptime(cutoff_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    deleted_count = 0

    try:
        async with get_session() as session:
            # First, find deal IDs to delete
            stmt_find = select(Deal.id).where(Deal.announced_date < cutoff)
            result_find = await session.execute(stmt_find)
            deal_ids_dated = [row[0] for row in result_find.fetchall()]

            deal_ids_undated = []
            if include_undated:
                stmt_find2 = select(Deal.id).where(
                    Deal.announced_date.is_(None),
                    Deal.created_at < cutoff
                )
                result_find2 = await session.execute(stmt_find2)
                deal_ids_undated = [row[0] for row in result_find2.fetchall()]

            all_deal_ids = deal_ids_dated + deal_ids_undated

            if all_deal_ids:
                # Delete related articles first
                stmt_articles = delete(Article).where(Article.deal_id.in_(all_deal_ids))
                await session.execute(stmt_articles)

                # Delete related deal_investors
                stmt_investors = delete(DealInvestor).where(DealInvestor.deal_id.in_(all_deal_ids))
                await session.execute(stmt_investors)

                # Now delete the deals
                stmt_deals = delete(Deal).where(Deal.id.in_(all_deal_ids))
                result_deals = await session.execute(stmt_deals)
                deleted_count = result_deals.rowcount

            await session.commit()

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    cache.invalidate("deals")

    return CleanupOldDealsResponse(
        deals_deleted=deleted_count,
        cutoff_date=cutoff_date,
    )


@app.delete("/deals/{deal_id}")
async def delete_deal(
    deal_id: int,
    api_key: str = Depends(verify_api_key),
):
    """
    Delete a specific deal by ID.

    Also deletes related records from all tables with deal_id foreign key.
    """
    from sqlalchemy import delete, update
    from .archivist.models import DateSource, FounderValidation, StealthSignal

    try:
        async with get_session() as session:
            # Delete related articles first
            stmt_articles = delete(Article).where(Article.deal_id == deal_id)
            await session.execute(stmt_articles)

            # Delete related deal_investors
            stmt_investors = delete(DealInvestor).where(DealInvestor.deal_id == deal_id)
            await session.execute(stmt_investors)

            # Delete related date_sources
            stmt_dates = delete(DateSource).where(DateSource.deal_id == deal_id)
            await session.execute(stmt_dates)

            # Delete related founder_validations
            stmt_founders = delete(FounderValidation).where(FounderValidation.deal_id == deal_id)
            await session.execute(stmt_founders)

            # Unlink stealth_signals (set converted_deal_id to NULL instead of delete)
            stmt_stealth = update(StealthSignal).where(
                StealthSignal.converted_deal_id == deal_id
            ).values(converted_deal_id=None)
            await session.execute(stmt_stealth)

            # Delete the deal
            stmt_deal = delete(Deal).where(Deal.id == deal_id)
            result = await session.execute(stmt_deal)

            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"Deal {deal_id} not found")

            await session.commit()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    cache.invalidate("deals")

    return {"deleted": True, "deal_id": deal_id}


class LinkedInEnrichmentResponse(BaseModel):
    deals_processed: int
    founders_enriched: int
    linkedin_urls_found: int


@app.post("/enrichment/linkedin", response_model=LinkedInEnrichmentResponse)
async def enrich_founder_linkedin(
    limit: int = Query(50, ge=1, le=200, description="Max deals to process"),
    api_key: str = Depends(verify_api_key),
):
    """
    Enrich founders with LinkedIn URLs and persist to database.

    Finds deals with founders missing LinkedIn URLs,
    searches Brave for their profiles, and saves results.
    """
    from .enrichment.brave_enrichment import enrich_deal_founders_linkedin
    import json as json_module

    processed = 0
    founders_enriched = 0
    linkedin_found = 0

    try:
        async with get_session() as session:
            # Find deals with founders but missing LinkedIn URLs
            stmt = (
                select(Deal)
                .options(selectinload(Deal.company))
                .where(Deal.founders_json.isnot(None))
                .order_by(Deal.created_at.desc())
                .limit(limit * 2)  # Get more to filter
            )
            result = await session.execute(stmt)
            deals = result.scalars().all()

            for deal in deals:
                if processed >= limit:
                    break

                # Parse founders
                try:
                    founders = json_module.loads(deal.founders_json)
                except (json_module.JSONDecodeError, TypeError):
                    continue

                # Skip if all founders already have LinkedIn
                missing_linkedin = [f for f in founders if not f.get("linkedin_url")]
                if not missing_linkedin:
                    continue

                processed += 1
                company_name = deal.company.name if deal.company else "Unknown"

                # Enrich and persist
                updated_founders = await enrich_deal_founders_linkedin(
                    deal_id=deal.id,
                    company_name=company_name,
                    founders=founders,
                    persist=True,
                )

                # Count enriched
                for orig, updated in zip(founders, updated_founders):
                    if not orig.get("linkedin_url") and updated.get("linkedin_url"):
                        founders_enriched += 1
                        linkedin_found += 1

    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    return LinkedInEnrichmentResponse(
        deals_processed=processed,
        founders_enriched=founders_enriched,
        linkedin_urls_found=linkedin_found,
    )


class BackfillResponse(BaseModel):
    """Response from backfill operations."""
    status: str
    websites_found: int
    websites_failed: int
    linkedins_found: int
    linkedins_failed: int
    total_processed: int


class EnrichmentCoverageResponse(BaseModel):
    """Enrichment coverage statistics.

    FIX (2026-01): Added with_both and missing_both for complete coverage tracking.
    """
    total_deals: int
    with_website: int
    website_percentage: float
    with_founders: int
    with_linkedin: int
    linkedin_percentage: float
    missing_website: int
    missing_linkedin: int
    with_both: int  # Has both website AND at least one founder LinkedIn
    missing_both: int  # Missing both website AND founder LinkedIn


@app.get("/enrichment/stats", response_model=EnrichmentCoverageResponse)
async def get_enrichment_stats(
    api_key: str = Depends(verify_api_key),
):
    """
    Get current enrichment coverage statistics.

    Shows how many deals have websites, LinkedIn, etc.
    """
    from .harvester.backfill_enrichment import get_enrichment_coverage

    try:
        stats = await get_enrichment_coverage()
        return EnrichmentCoverageResponse(**stats)
    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/enrichment/backfill-all", response_model=BackfillResponse)
async def backfill_all_enrichment(
    limit: Optional[int] = Query(None, ge=1, le=500, description="Limit per category (None = all)"),
    dry_run: bool = Query(False, description="Preview without saving"),
    api_key: str = Depends(verify_api_key),
):
    """
    Run full backfill of ALL deals missing website or LinkedIn.

    WARNING: This may take a long time (1-2 hours for ~200 deals).
    Uses multiple fallback search strategies for hard-to-find companies.

    Targets:
    - Website: 95%+ coverage (MANDATORY)
    - LinkedIn: 50%+ coverage (best effort)
    """
    from .harvester.backfill_enrichment import backfill_all

    try:
        website_stats, linkedin_stats = await backfill_all(
            limit=limit,
            dry_run=dry_run,
        )

        return BackfillResponse(
            status="completed" if not dry_run else "dry_run",
            websites_found=website_stats.websites_found,
            websites_failed=website_stats.failed_website,
            linkedins_found=linkedin_stats.linkedins_found,
            linkedins_failed=linkedin_stats.failed_linkedin,
            total_processed=website_stats.total_processed + linkedin_stats.total_processed,
        )
    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/enrichment/backfill-websites", response_model=BackfillResponse)
async def backfill_websites_only(
    limit: Optional[int] = Query(None, ge=1, le=500, description="Limit (None = all)"),
    dry_run: bool = Query(False, description="Preview without saving"),
    api_key: str = Depends(verify_api_key),
):
    """
    Backfill ONLY website enrichment for deals missing websites.

    Uses multiple search strategies:
    1. Company + Investor context
    2. Company + Category
    3. Company + "official site"
    4. Company + startup
    5. Crunchbase reference
    """
    from .harvester.backfill_enrichment import backfill_websites

    try:
        stats = await backfill_websites(limit=limit, dry_run=dry_run)

        return BackfillResponse(
            status="completed" if not dry_run else "dry_run",
            websites_found=stats.websites_found,
            websites_failed=stats.failed_website,
            linkedins_found=0,
            linkedins_failed=0,
            total_processed=stats.total_processed,
        )
    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/enrichment/backfill-linkedin", response_model=BackfillResponse)
async def backfill_linkedin_only(
    limit: Optional[int] = Query(None, ge=1, le=500, description="Limit (None = all)"),
    dry_run: bool = Query(False, description="Preview without saving"),
    api_key: str = Depends(verify_api_key),
):
    """
    Backfill ONLY LinkedIn enrichment for founders missing LinkedIn URLs.

    Uses multiple search strategies:
    1. site:linkedin.com/in + founder + company
    2. Founder + company + CEO/founder
    3. Founder + investor context
    4. Founder + LinkedIn profile
    5. Just founder name
    """
    from .harvester.backfill_enrichment import backfill_linkedin

    try:
        stats = await backfill_linkedin(limit=limit, dry_run=dry_run)

        return BackfillResponse(
            status="completed" if not dry_run else "dry_run",
            websites_found=0,
            websites_failed=0,
            linkedins_found=stats.linkedins_found,
            linkedins_failed=stats.failed_linkedin,
            total_processed=stats.total_processed,
        )
    except Exception as e:
        logger.error("Request failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Tracker CRM Response Models -----

class TrackerItemResponse(BaseModel):
    """Response model for a single tracker item."""
    id: int
    company_name: str
    round_type: Optional[str] = None
    amount: Optional[str] = None
    lead_investor: Optional[str] = None
    website: Optional[str] = None
    status: str
    notes: Optional[str] = None
    last_contact_date: Optional[str] = None
    next_step: Optional[str] = None
    position: int
    deal_id: Optional[int] = None
    created_at: str
    updated_at: str


class TrackerItemsResponse(BaseModel):
    """Response model for list of tracker items."""
    items: List[TrackerItemResponse]
    total: int
    stats: Dict[str, int]


class CreateTrackerItemRequest(BaseModel):
    """Request model for creating a tracker item."""
    company_name: str
    round_type: Optional[str] = None
    amount: Optional[str] = None
    lead_investor: Optional[str] = None
    website: Optional[str] = None
    notes: Optional[str] = None
    status: str = "watching"


class BulkCreateTrackerItemsRequest(BaseModel):
    """Request model for bulk creating tracker items."""
    company_names: List[str]
    status: str = "watching"


class BulkCreateTrackerItemsResponse(BaseModel):
    """Response model for bulk create operation."""
    created: List[TrackerItemResponse]
    count: int


class UpdateTrackerItemRequest(BaseModel):
    """Request model for updating a tracker item."""
    company_name: Optional[str] = None
    round_type: Optional[str] = None
    amount: Optional[str] = None
    lead_investor: Optional[str] = None
    website: Optional[str] = None
    notes: Optional[str] = None
    status: Optional[str] = None
    last_contact_date: Optional[str] = None
    next_step: Optional[str] = None


class MoveTrackerItemRequest(BaseModel):
    """Request model for moving a tracker item (drag-drop)."""
    status: str
    position: int


class AddDealToTrackerRequest(BaseModel):
    """Request model for adding an existing deal to tracker."""
    deal_id: int
    status: str = "watching"


# ----- Tracker Column Models -----

class TrackerColumnResponse(BaseModel):
    """Response model for a tracker column."""
    id: int
    slug: str
    display_name: str
    color: str
    position: int
    is_active: bool = True


class TrackerColumnsResponse(BaseModel):
    """Response model for list of tracker columns."""
    columns: List[TrackerColumnResponse]
    item_counts: Dict[str, int]


class CreateTrackerColumnRequest(BaseModel):
    """Request model for creating a tracker column."""
    display_name: str
    color: str = "slate"
    slug: Optional[str] = None  # Auto-generated if not provided


class UpdateTrackerColumnRequest(BaseModel):
    """Request model for updating a tracker column."""
    display_name: Optional[str] = None
    color: Optional[str] = None


class MoveTrackerColumnRequest(BaseModel):
    """Request model for moving a tracker column."""
    position: int


def _map_tracker_column(column) -> TrackerColumnResponse:
    """Map TrackerColumn model to response."""
    return TrackerColumnResponse(
        id=column.id,
        slug=column.slug,
        display_name=column.display_name,
        color=column.color,
        position=column.position,
        is_active=column.is_active,
    )


def _map_tracker_item(item) -> TrackerItemResponse:
    """Map TrackerItem model to response."""
    return TrackerItemResponse(
        id=item.id,
        company_name=item.company_name,
        round_type=item.round_type,
        amount=item.amount,
        lead_investor=item.lead_investor,
        website=item.website,
        status=item.status,
        notes=item.notes,
        last_contact_date=item.last_contact_date.isoformat() if item.last_contact_date else None,
        next_step=item.next_step,
        position=item.position,
        deal_id=item.deal_id,
        created_at=item.created_at.isoformat(),
        updated_at=item.updated_at.isoformat(),
    )


# ----- Tracker Column Endpoints -----

@app.get("/tracker/columns", response_model=TrackerColumnsResponse)
async def list_tracker_columns():
    """
    List all tracker columns ordered by position.

    Returns column configuration for the Kanban board.
    """
    try:
        async with get_session() as session:
            columns = await get_tracker_columns(session)
            item_counts = await get_column_item_counts(session)
            return TrackerColumnsResponse(
                columns=[_map_tracker_column(col) for col in columns],
                item_counts=item_counts,
            )
    except Exception as e:
        logger.error("Failed to list tracker columns: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/tracker/columns", response_model=TrackerColumnResponse)
async def create_new_tracker_column(
    request: CreateTrackerColumnRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Create a new tracker column.

    Column is added at the end (rightmost position).
    """
    if not request.display_name or not request.display_name.strip():
        raise HTTPException(status_code=400, detail="Display name is required")

    if request.color and request.color not in AVAILABLE_COLORS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid color. Must be one of: {', '.join(sorted(AVAILABLE_COLORS))}"
        )

    try:
        async with get_session() as session:
            column = await create_tracker_column(
                session,
                display_name=request.display_name.strip(),
                color=request.color,
                slug=request.slug,
            )
            return _map_tracker_column(column)
    except Exception as e:
        logger.error("Failed to create tracker column: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/tracker/columns/{column_id}", response_model=TrackerColumnResponse)
async def update_existing_tracker_column(
    column_id: int,
    request: UpdateTrackerColumnRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Update a tracker column's display name or color.
    """
    if request.display_name is not None and not request.display_name.strip():
        raise HTTPException(status_code=400, detail="Display name cannot be empty")

    if request.color is not None and request.color not in AVAILABLE_COLORS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid color. Must be one of: {', '.join(sorted(AVAILABLE_COLORS))}"
        )

    try:
        async with get_session() as session:
            column = await update_tracker_column(
                session,
                column_id=column_id,
                display_name=request.display_name.strip() if request.display_name else None,
                color=request.color,
            )
            if not column:
                raise HTTPException(status_code=404, detail="Tracker column not found")
            return _map_tracker_column(column)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update tracker column: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/tracker/columns/{column_id}/move", response_model=TrackerColumnResponse)
async def move_existing_tracker_column(
    column_id: int,
    request: MoveTrackerColumnRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Move a tracker column to a new position.

    Position is 0-indexed (0 = leftmost).
    """
    try:
        async with get_session() as session:
            column = await move_tracker_column(
                session,
                column_id=column_id,
                new_position=request.position,
            )
            if not column:
                raise HTTPException(status_code=404, detail="Tracker column not found")
            return _map_tracker_column(column)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to move tracker column: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/tracker/columns/{column_id}")
async def delete_existing_tracker_column(
    column_id: int,
    api_key: str = Depends(verify_api_key),
):
    """
    Delete a tracker column.

    All items in this column are moved to the first (leftmost) column.
    Cannot delete the last remaining column.
    """
    try:
        async with get_session() as session:
            # Get column first to check item count
            column = await get_tracker_column(session, column_id)
            if not column:
                raise HTTPException(status_code=404, detail="Tracker column not found")

            success = await delete_tracker_column(session, column_id)
            if not success:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot delete the last column"
                )
            return {"message": f"Column '{column.display_name}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete tracker column: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/tracker/colors")
async def list_available_colors():
    """List available colors for tracker columns."""
    return {"colors": sorted(AVAILABLE_COLORS)}


# ----- Tracker CRM Endpoints -----

@app.get("/tracker", response_model=TrackerItemsResponse)
async def list_tracker_items(
    status: Optional[str] = Query(None, description="Filter by status (watching, reached_out, in_conversation, closing_spv)"),
):
    """
    List all tracker items for the CRM Kanban board.

    Returns items grouped by status column with position ordering.
    """
    try:
        async with get_session() as session:
            items = await get_tracker_items(session, status=status)
            stats = await get_tracker_stats(session)

            return TrackerItemsResponse(
                items=[_map_tracker_item(item) for item in items],
                total=len(items),
                stats=stats,
            )
    except Exception as e:
        logger.error("Failed to list tracker items: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/tracker/{item_id}", response_model=TrackerItemResponse)
async def get_single_tracker_item(item_id: int):
    """Get a single tracker item by ID."""
    try:
        async with get_session() as session:
            item = await get_tracker_item(session, item_id)
            if not item:
                raise HTTPException(status_code=404, detail="Tracker item not found")
            return _map_tracker_item(item)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get tracker item: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/tracker", response_model=TrackerItemResponse)
async def create_new_tracker_item(
    request: CreateTrackerItemRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Create a new tracker item (manual entry).

    Used when adding a company that wasn't found by the scraper.
    """
    try:
        async with get_session() as session:
            item = await create_tracker_item(
                session,
                company_name=request.company_name,
                status=request.status,
                round_type=request.round_type,
                amount=request.amount,
                lead_investor=request.lead_investor,
                website=request.website,
                notes=request.notes,
            )
            return _map_tracker_item(item)
    except Exception as e:
        logger.error("Failed to create tracker item: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/tracker/bulk", response_model=BulkCreateTrackerItemsResponse)
async def bulk_create_tracker_items_endpoint(
    request: BulkCreateTrackerItemsRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Bulk create tracker items from a list of company names.

    All items are added to the specified status column (default: watching).
    Duplicate names within the batch are automatically deduplicated.
    """
    try:
        async with get_session() as session:
            items = await bulk_create_tracker_items(
                session,
                company_names=request.company_names,
                status=request.status,
            )
            return BulkCreateTrackerItemsResponse(
                created=[_map_tracker_item(item) for item in items],
                count=len(items),
            )
    except Exception as e:
        logger.error("Failed to bulk create tracker items: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/tracker/{item_id}", response_model=TrackerItemResponse)
async def update_existing_tracker_item(
    item_id: int,
    request: UpdateTrackerItemRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Update a tracker item's fields.

    Use this for editing notes, next_step, last_contact_date, etc.
    For changing status/position, use POST /tracker/{item_id}/move instead.
    """
    try:
        async with get_session() as session:
            # Build updates dict, excluding None values
            updates = {}
            if request.company_name is not None:
                updates["company_name"] = request.company_name
            if request.round_type is not None:
                updates["round_type"] = request.round_type
            if request.amount is not None:
                updates["amount"] = request.amount
            if request.lead_investor is not None:
                updates["lead_investor"] = request.lead_investor
            if request.website is not None:
                updates["website"] = request.website
            if request.notes is not None:
                updates["notes"] = request.notes
            if request.status is not None:
                updates["status"] = request.status
            if request.next_step is not None:
                updates["next_step"] = request.next_step
            if request.last_contact_date is not None:
                from datetime import date as date_type
                # Handle empty string as null
                if request.last_contact_date.strip():
                    updates["last_contact_date"] = date_type.fromisoformat(request.last_contact_date)
                else:
                    updates["last_contact_date"] = None

            item = await update_tracker_item(session, item_id, **updates)
            if not item:
                raise HTTPException(status_code=404, detail="Tracker item not found")
            return _map_tracker_item(item)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error("Failed to update tracker item: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/tracker/{item_id}/move", response_model=TrackerItemResponse)
async def move_tracker_item_position(
    item_id: int,
    request: MoveTrackerItemRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Move a tracker item to a new status/position (drag-drop).

    Called when user drags a card to a different column or reorders within a column.
    Status validation is handled by move_tracker_item using dynamic column slugs.
    """
    try:
        async with get_session() as session:
            item = await move_tracker_item(session, item_id, request.status, request.position)
            if not item:
                raise HTTPException(status_code=404, detail="Tracker item not found")
            return _map_tracker_item(item)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to move tracker item: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/tracker/{item_id}")
async def delete_existing_tracker_item(
    item_id: int,
    api_key: str = Depends(verify_api_key),
):
    """Delete a tracker item from the pipeline."""
    try:
        async with get_session() as session:
            success = await delete_tracker_item(session, item_id)
            if not success:
                raise HTTPException(status_code=404, detail="Tracker item not found")
            return {"success": True, "message": f"Tracker item {item_id} deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete tracker item: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/tracker/from-deal", response_model=TrackerItemResponse)
async def add_deal_to_tracker(
    request: AddDealToTrackerRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Add an existing deal from the Dashboard to the Tracker.

    Copies deal info (company, round, amount, lead investor) and links to the deal.
    Returns error if deal not found or already tracked.
    Status validation is handled by create_tracker_from_deal using dynamic column slugs.
    """
    try:
        async with get_session() as session:
            item = await create_tracker_from_deal(session, request.deal_id, request.status)
            if not item:
                raise HTTPException(
                    status_code=400,
                    detail="Deal not found or already tracked"
                )
            return _map_tracker_item(item)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to add deal to tracker: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ----- Feedback Endpoints -----


class FlagRequest(BaseModel):
    """Request to flag a company with an error."""
    deal_id: Optional[int] = None
    company_name: str
    reason: Optional[str] = None
    source_url: Optional[str] = None
    reporter_email: Optional[str] = None


class SuggestionRequest(BaseModel):
    """Request to suggest a company or report feedback."""
    company_name: str
    details: Optional[str] = None
    reporter_email: Optional[str] = None
    suggestion_type: str = "missing_company"  # missing_company, error, other


class FeedbackResponse(BaseModel):
    """Response for feedback operations."""
    success: bool
    timestamp: str
    company_name: str
    message: str = "Thank you for your feedback!"


@app.post("/feedback/flag", response_model=FeedbackResponse)
async def flag_company(
    request: FlagRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Flag a company for review.

    Used when users spot errors with a company's deal information.
    Saves to PostgreSQL feedback table.
    """
    from .archivist.models import Feedback
    from .archivist.database import async_session_factory

    try:
        async with async_session_factory() as session:
            feedback = Feedback(
                feedback_type="flag",
                company_name=request.company_name,
                deal_id=request.deal_id,
                reason=request.reason,
                source_url=request.source_url,
                reporter_email=request.reporter_email,
            )
            session.add(feedback)
            await session.commit()
            await session.refresh(feedback)

            timestamp = feedback.created_at.isoformat()

        return FeedbackResponse(
            success=True,
            timestamp=timestamp,
            company_name=request.company_name,
            message="Thanks for flagging! We'll review this soon.",
        )
    except Exception as e:
        logger.error("Failed to flag company: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to submit flag")


@app.post("/feedback/suggestion", response_model=FeedbackResponse)
async def submit_suggestion(
    request: SuggestionRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Submit a suggestion (missing company, general feedback).

    Used when users want to suggest companies we should be tracking
    or report other issues. Saves to PostgreSQL feedback table.
    """
    from .archivist.models import Feedback
    from .archivist.database import async_session_factory

    try:
        async with async_session_factory() as session:
            feedback = Feedback(
                feedback_type="suggestion",
                company_name=request.company_name,
                reason=request.details,  # Store details in reason field
                suggestion_type=request.suggestion_type,
                reporter_email=request.reporter_email,
            )
            session.add(feedback)
            await session.commit()
            await session.refresh(feedback)

            timestamp = feedback.created_at.isoformat()

        return FeedbackResponse(
            success=True,
            timestamp=timestamp,
            company_name=request.company_name,
            message="Thanks for the suggestion! We'll look into it.",
        )
    except Exception as e:
        logger.error("Failed to submit suggestion: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to submit suggestion")


@app.get("/feedback")
async def list_feedback(
    reviewed: Optional[bool] = None,
    feedback_type: Optional[str] = None,
    limit: int = 50,
    api_key: str = Depends(verify_api_key),
):
    """
    List feedback entries for manual review.

    Args:
        reviewed: Filter by reviewed status (true/false)
        feedback_type: Filter by type ('flag' or 'suggestion')
        limit: Max number of entries to return
    """
    from sqlmodel import select
    from .archivist.models import Feedback
    from .archivist.database import async_session_factory

    try:
        async with async_session_factory() as session:
            query = select(Feedback).order_by(Feedback.created_at.desc()).limit(limit)

            if reviewed is not None:
                query = query.where(Feedback.reviewed == reviewed)
            if feedback_type:
                query = query.where(Feedback.feedback_type == feedback_type)

            result = await session.execute(query)
            items = result.scalars().all()

            return {
                "count": len(items),
                "items": [
                    {
                        "id": f.id,
                        "feedback_type": f.feedback_type,
                        "company_name": f.company_name,
                        "deal_id": f.deal_id,
                        "reason": f.reason,
                        "suggestion_type": f.suggestion_type,
                        "source_url": f.source_url,
                        "reporter_email": f.reporter_email,
                        "reviewed": f.reviewed,
                        "created_at": f.created_at.isoformat() if f.created_at else None,
                    }
                    for f in items
                ],
            }
    except Exception as e:
        logger.error("Failed to list feedback: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list feedback")


@app.patch("/feedback/{feedback_id}/reviewed")
async def mark_feedback_reviewed(
    feedback_id: int,
    reviewed: bool = Query(default=True),
    api_key: str = Depends(verify_api_key),
):
    """Mark a feedback entry as reviewed or unreviewed."""
    from sqlmodel import select
    from .archivist.models import Feedback
    from .archivist.database import async_session_factory

    try:
        async with async_session_factory() as session:
            result = await session.execute(
                select(Feedback).where(Feedback.id == feedback_id)
            )
            fb = result.scalar_one_or_none()

            if not fb:
                raise HTTPException(status_code=404, detail="Feedback not found")

            # Update fields
            fb.reviewed = reviewed
            if reviewed:
                # Use naive UTC datetime (model expects naive)
                fb.reviewed_at = datetime.utcnow()
            else:
                fb.reviewed_at = None

            session.add(fb)
            await session.commit()

            return {"success": True, "id": feedback_id, "reviewed": reviewed}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to mark feedback as reviewed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update feedback")


@app.delete("/feedback/{feedback_id}")
async def delete_feedback(
    feedback_id: int,
    api_key: str = Depends(verify_api_key),
):
    """Delete a feedback entry."""
    from sqlmodel import select
    from .archivist.models import Feedback
    from .archivist.database import async_session_factory

    try:
        async with async_session_factory() as session:
            result = await session.execute(
                select(Feedback).where(Feedback.id == feedback_id)
            )
            feedback = result.scalar_one_or_none()

            if not feedback:
                raise HTTPException(status_code=404, detail="Feedback not found")

            await session.delete(feedback)
            await session.commit()

            return {"success": True, "id": feedback_id, "deleted": True}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete feedback: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete feedback")


# ----- Admin: Run Migration -----

class MigrationResponse(BaseModel):
    success: bool
    message: str
    duplicates_removed: int = 0


@app.post("/admin/run-dedup-migration", response_model=MigrationResponse)
async def run_dedup_migration(
    api_key: str = Depends(verify_api_key),
):
    """
    Run the dedup_key migration manually.

    This adds the dedup_key column if not exists, backfills existing deals,
    removes duplicates, and creates the unique index.

    Should only be run once after deployment.
    """
    import hashlib
    import re
    from sqlalchemy import text

    def normalize_name(name: str) -> str:
        name = name.lower().strip()
        if name.startswith("the "):
            name = name[4:]
        suffixes = [
            ", incorporated", " incorporated", ", technologies", " technologies",
            ", corporation", " corporation", ", limited", " limited",
            ", company", " company", ", inc.", " inc.", ", inc", " inc",
            ", llc", " llc", ", ltd.", " ltd.", ", ltd", " ltd",
            ", corp.", " corp.", ", corp", " corp", ", co.", " co.",
            ", co", " co", " labs", " lab", " tech", " ai",
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        return re.sub(r'[^a-z0-9]', '', name)

    def make_key(company_name: str, round_type: str, announced_date) -> str:
        from datetime import date as date_type
        normalized = normalize_name(company_name)
        if announced_date:
            days = (announced_date - date_type(1970, 1, 1)).days
            bucket = days // 3
            date_str = str(bucket)
        else:
            today = date_type.today()
            week = (today - date_type(1970, 1, 1)).days // 7
            date_str = f"nodate_{week}"
        return hashlib.md5(f"{normalized}|{round_type}|{date_str}".encode()).hexdigest()

    duplicates_removed = 0

    try:
        async with get_session() as session:
            # Step 1: Add column if not exists
            await session.execute(text(
                "ALTER TABLE deals ADD COLUMN IF NOT EXISTS dedup_key VARCHAR(32)"
            ))
            await session.commit()
            logger.info("Added dedup_key column (or already exists)")

            # Step 2: Backfill existing deals
            result = await session.execute(text("""
                SELECT d.id, pc.name, d.round_type, d.announced_date
                FROM deals d
                JOIN portfolio_companies pc ON d.company_id = pc.id
                WHERE d.dedup_key IS NULL
            """))
            rows = result.fetchall()

            for row in rows:
                deal_id, company_name, round_type, announced_date = row
                key = make_key(company_name, round_type, announced_date)
                await session.execute(
                    text("UPDATE deals SET dedup_key = :key WHERE id = :id"),
                    {"key": key, "id": deal_id}
                )
            await session.commit()
            logger.info(f"Backfilled {len(rows)} deals with dedup_key")

            # Step 3: Find and remove duplicates
            dup_result = await session.execute(text("""
                SELECT dedup_key, array_agg(id ORDER BY id) as ids
                FROM deals
                WHERE dedup_key IS NOT NULL
                GROUP BY dedup_key
                HAVING count(*) > 1
            """))
            duplicates = dup_result.fetchall()

            for dup_row in duplicates:
                dedup_key, ids = dup_row
                kept_id = ids[0]
                ids_to_delete = ids[1:]

                for del_id in ids_to_delete:
                    # Reassign articles to kept deal
                    await session.execute(
                        text("UPDATE articles SET deal_id = :kept WHERE deal_id = :del"),
                        {"kept": kept_id, "del": del_id}
                    )
                    # Delete from all tables that reference deals (foreign keys)
                    await session.execute(
                        text("DELETE FROM date_sources WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM founder_validations WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM thesis_drift WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM deal_investors WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM tracker_items WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM stealth_signals WHERE converted_deal_id = :id"),
                        {"id": del_id}
                    )
                    # Finally delete the deal
                    await session.execute(
                        text("DELETE FROM deals WHERE id = :id"),
                        {"id": del_id}
                    )
                    duplicates_removed += 1

            await session.commit()
            logger.info(f"Removed {duplicates_removed} duplicate deals")

            # Step 4: Create unique index (if not exists)
            try:
                await session.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_deals_dedup_key ON deals(dedup_key)"
                ))
                await session.commit()
                logger.info("Created unique index on dedup_key")
            except Exception as idx_err:
                logger.warning(f"Index creation note: {idx_err}")

            return MigrationResponse(
                success=True,
                message=f"Migration complete. Backfilled {len(rows)} deals, removed {duplicates_removed} duplicates.",
                duplicates_removed=duplicates_removed,
            )

    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


@app.post("/admin/run-amount-dedup-migration", response_model=MigrationResponse)
async def run_amount_dedup_migration(
    api_key: str = Depends(verify_api_key),
):
    """
    Run the amount_dedup_key migration (Parloa duplicate fix).

    This adds the amount_dedup_key column if not exists, backfills existing deals,
    removes cross-round-type duplicates (same company+amount+date, different round_type),
    and creates an index.

    FIX Jan 2026: Catches duplicates where LLM assigns different round types to same deal.
    """
    import hashlib
    import re
    from sqlalchemy import text

    def normalize_name(name: str) -> str:
        name = name.lower().strip()
        if name.startswith("the "):
            name = name[4:]
        suffixes = [
            ", incorporated", " incorporated", ", technologies", " technologies",
            ", corporation", " corporation", ", limited", " limited",
            ", company", " company", ", inc.", " inc.", ", inc", " inc",
            ", llc", " llc", ", ltd.", " ltd.", ", ltd", " ltd",
            ", corp.", " corp.", ", corp", " corp", ", co.", " co.",
            ", co", " co", " labs", " lab", " tech", " ai",
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        return re.sub(r'[^a-z0-9]', '', name)

    def make_amount_key(company_name: str, amount_usd: int, announced_date) -> str:
        from datetime import date as date_type
        normalized = normalize_name(company_name)

        # Calculate amount bucket (logarithmic buckets)
        if amount_usd < 10_000_000:  # $1M-$10M
            amount_bucket = amount_usd // 2_000_000
        elif amount_usd < 100_000_000:  # $10M-$100M
            amount_bucket = 5 + (amount_usd // 20_000_000)
        elif amount_usd < 1_000_000_000:  # $100M-$1B
            amount_bucket = 10 + (amount_usd // 100_000_000)
        else:  # >$1B
            amount_bucket = 20 + (amount_usd // 500_000_000)

        if announced_date:
            days = (announced_date - date_type(1970, 1, 1)).days
            bucket = days // 3
            date_str = str(bucket)
        else:
            today = date_type.today()
            week = (today - date_type(1970, 1, 1)).days // 7
            date_str = f"nodate_{week}"

        return hashlib.md5(f"{normalized}|amt{amount_bucket}|{date_str}".encode()).hexdigest()

    duplicates_removed = 0
    backfilled = 0

    try:
        async with get_session() as session:
            # Step 1: Add column if not exists
            await session.execute(text(
                "ALTER TABLE deals ADD COLUMN IF NOT EXISTS amount_dedup_key VARCHAR(32)"
            ))
            await session.commit()
            logger.info("Added amount_dedup_key column (or already exists)")

            # Step 2: Backfill existing deals (only those with amount >= $1M)
            result = await session.execute(text("""
                SELECT d.id, pc.name, d.amount_usd, d.announced_date
                FROM deals d
                JOIN portfolio_companies pc ON d.company_id = pc.id
                WHERE d.amount_usd IS NOT NULL AND d.amount_usd >= 1000000
                AND d.amount_dedup_key IS NULL
            """))
            rows = result.fetchall()

            for row in rows:
                deal_id, company_name, amount_usd, announced_date = row
                key = make_amount_key(company_name, amount_usd, announced_date)
                await session.execute(
                    text("UPDATE deals SET amount_dedup_key = :key WHERE id = :id"),
                    {"key": key, "id": deal_id}
                )
                backfilled += 1
            await session.commit()
            logger.info(f"Backfilled {backfilled} deals with amount_dedup_key")

            # Step 3: Find and remove cross-round-type duplicates
            dup_result = await session.execute(text("""
                SELECT amount_dedup_key, array_agg(id ORDER BY id) as ids
                FROM deals
                WHERE amount_dedup_key IS NOT NULL
                GROUP BY amount_dedup_key
                HAVING count(*) > 1
            """))
            dup_rows = dup_result.fetchall()

            for dup_row in dup_rows:
                amount_key, ids = dup_row
                kept_id = ids[0]
                ids_to_delete = ids[1:]

                logger.info(f"Removing {len(ids_to_delete)} duplicates for amount_dedup_key={amount_key[:8]}... (keeping #{kept_id})")

                for del_id in ids_to_delete:
                    # Reassign articles to kept deal
                    await session.execute(
                        text("UPDATE articles SET deal_id = :kept_id WHERE deal_id = :del_id"),
                        {"kept_id": kept_id, "del_id": del_id}
                    )
                    # Delete from referencing tables
                    await session.execute(
                        text("DELETE FROM date_sources WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM deal_investors WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM tracker_items WHERE deal_id = :id"),
                        {"id": del_id}
                    )
                    await session.execute(
                        text("DELETE FROM stealth_signals WHERE converted_deal_id = :id"),
                        {"id": del_id}
                    )
                    # Delete the deal
                    await session.execute(
                        text("DELETE FROM deals WHERE id = :id"),
                        {"id": del_id}
                    )
                    duplicates_removed += 1

            await session.commit()
            logger.info(f"Removed {duplicates_removed} cross-round-type duplicates")

            # Step 4: Create index (not unique - NULLs are allowed)
            try:
                await session.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_deals_amount_dedup_key ON deals(amount_dedup_key)"
                ))
                await session.commit()
                logger.info("Created index on amount_dedup_key")
            except Exception as idx_err:
                logger.warning(f"Index creation note: {idx_err}")

            return MigrationResponse(
                success=True,
                message=f"Amount dedup migration complete. Backfilled {backfilled} deals, removed {duplicates_removed} duplicates.",
                duplicates_removed=duplicates_removed,
            )

    except Exception as e:
        logger.error(f"Amount dedup migration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


# ----- CLI Runner -----

def run_server():
    """Run the FastAPI server."""
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


if __name__ == "__main__":
    run_server()
