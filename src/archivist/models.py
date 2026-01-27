"""
Database models using SQLModel (SQLAlchemy + Pydantic).

Schema:
- Fund: The 18 tracked VC funds
- Partner: Partners at each fund
- PortfolioCompany: Startups that received funding
- Deal: Individual funding rounds
- DealInvestor: Join table linking deals to investors with role
- Article: Source articles with embeddings for deduplication
"""

from datetime import datetime, date, timezone
from typing import Optional, List
from sqlmodel import SQLModel, Field, Relationship
from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, DateTime, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
import uuid


def utc_now_naive() -> datetime:
    """Return current UTC time as timezone-naive datetime.

    PostgreSQL TIMESTAMP WITHOUT TIME ZONE columns require naive datetimes.
    Using timezone-aware datetimes causes asyncpg DataError.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Fund(SQLModel, table=True):
    """A tracked VC fund."""
    __tablename__ = "funds"

    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(unique=True, index=True)
    name: str
    website: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now_naive)

    # Relationships
    partners: List["Partner"] = Relationship(back_populates="fund")
    deals: List["DealInvestor"] = Relationship(back_populates="fund")


class Partner(SQLModel, table=True):
    """A partner at a VC fund."""
    __tablename__ = "partners"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    fund_id: int = Field(foreign_key="funds.id")
    linkedin_url: Optional[str] = None
    twitter_handle: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now_naive)

    # Relationships
    fund: Fund = Relationship(back_populates="partners")


class PortfolioCompany(SQLModel, table=True):
    """A startup that received funding."""
    __tablename__ = "portfolio_companies"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = None
    website: Optional[str] = None
    linkedin_url: Optional[str] = None  # Company LinkedIn page
    sector: Optional[str] = None
    founded_year: Optional[int] = None
    hq_location: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now_naive)
    updated_at: datetime = Field(default_factory=utc_now_naive)

    # Relationships
    deals: List["Deal"] = Relationship(back_populates="company")


class Deal(SQLModel, table=True):
    """A funding round."""
    __tablename__ = "deals"

    id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int = Field(foreign_key="portfolio_companies.id", index=True)

    # Deduplication key (FIX Jan 2026: prevent race condition duplicates)
    # Hash of normalized_company_name + round_type + date_bucket
    # Unique constraint prevents parallel inserts of same deal
    # FIX 2026-01: Added unique=True to actually enforce constraint (was only indexed, not unique)
    dedup_key: Optional[str] = Field(default=None, max_length=32, index=True, unique=True)

    # Secondary deduplication key based on amount (FIX Jan 2026: Parloa duplicate bug)
    # Hash of normalized_company_name + amount_bucket + date_bucket
    # Catches duplicates where LLM assigns different round_types to same deal
    # (e.g., "growth" vs "series_d" for the same $350M raise)
    amount_dedup_key: Optional[str] = Field(default=None, max_length=32, index=True)

    # Round details
    round_type: str  # seed, series_a, series_b, etc.
    amount: Optional[str] = None  # "$50M"
    amount_usd: Optional[int] = Field(default=None, sa_column=Column(BigInteger))  # 50000000 (normalized, BIGINT for >$2.1B deals)
    valuation: Optional[str] = None
    announced_date: Optional[date] = None

    # Lead investor tracking
    # FIX #43: Add index=True for frequently filtered fields
    is_lead_confirmed: bool = Field(default=False, index=True)  # True if we confirmed a tracked fund led
    lead_partner_name: Optional[str] = None  # Partner who led the deal
    verification_snippet: Optional[str] = None  # Exact quote proving lead status
    lead_evidence_weak: bool = Field(default=False)  # True if snippet lacks "led by" language (trusting Claude)

    # Amount validation (FIX 2026-01: detect market size confusion)
    amount_needs_review: bool = Field(default=False)  # True if amount seems suspicious (e.g., Series A >$100M)
    amount_review_reason: Optional[str] = None  # Why amount needs review (e.g., "may be confused with market size")
    amount_source: Optional[str] = None  # "sec_form_d" | "article" | "crunchbase" - source priority for amounts

    # AI classification
    enterprise_category: Optional[str] = None  # infrastructure, security, vertical_saas, agentic, data_intelligence, consumer_ai, gaming_ai, social_ai, not_ai
    is_enterprise_ai: bool = Field(default=False, index=True)  # True if B2B Enterprise AI (not consumer)
    is_ai_deal: bool = Field(default=False, index=True)  # True if AI company (enterprise OR consumer)

    # Founders (stored as JSON list)
    founders_json: Optional[str] = None  # JSON: [{"name": "...", "title": "...", "linkedin_url": "..."}]
    founders_validated: bool = Field(default=False)  # True if LinkedIn validation passed

    # Date verification
    date_confidence: float = Field(default=0.5)  # 0.0-1.0 confidence in announced_date
    date_source_count: int = Field(default=1)  # Number of sources agreeing on date
    sec_filing_date: Optional[date] = None  # Official SEC Form D filing date
    sec_filing_url: Optional[str] = None  # Link to SEC Form D

    # Quality scores
    confidence_score: float = 0.0
    thesis_drift_score: float = 0.0

    # Scan tracking
    scan_job_id: Optional[int] = Field(default=None, foreign_key="scan_jobs.id", index=True)

    # Metadata
    # FIX #43: Add index=True for date range queries
    created_at: datetime = Field(default_factory=utc_now_naive, index=True)
    updated_at: datetime = Field(default_factory=utc_now_naive)

    # Relationships
    company: PortfolioCompany = Relationship(back_populates="deals")
    investors: List["DealInvestor"] = Relationship(
        back_populates="deal",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    articles: List["Article"] = Relationship(
        back_populates="deal",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )


class DealInvestor(SQLModel, table=True):
    """Join table: which investors participated in which deals."""
    __tablename__ = "deal_investors"

    id: Optional[int] = Field(default=None, primary_key=True)
    deal_id: int = Field(foreign_key="deals.id")
    fund_id: Optional[int] = Field(default=None, foreign_key="funds.id")

    # Investor details (for non-tracked funds)
    investor_name: str
    is_lead: bool = False
    is_tracked_fund: bool = False
    partner_name: Optional[str] = None
    role: str = "participant"  # confirmed_lead, likely_lead, participant, unresolved

    # Relationships
    deal: Deal = Relationship(back_populates="investors")
    fund: Optional[Fund] = Relationship(back_populates="deals")


class Article(SQLModel, table=True):
    """Source article with embedding for deduplication."""
    __tablename__ = "articles"

    id: Optional[int] = Field(default=None, primary_key=True)
    deal_id: Optional[int] = Field(default=None, foreign_key="deals.id")

    # Article metadata
    url: str = Field(unique=True, index=True)
    title: str
    source_fund_slug: Optional[str] = None  # Which fund's feed we found it on
    published_date: Optional[date] = None
    fetched_at: datetime = Field(default_factory=utc_now_naive)

    # Content
    raw_html: Optional[str] = None
    extracted_text: Optional[str] = None
    summary: Optional[str] = None

    # Embedding for deduplication (1536 dimensions for text-embedding-3-small)
    # Note: Using Column directly for pgvector support
    embedding: Optional[List[float]] = Field(
        default=None,
        sa_column=Column(Vector(1536))
    )

    # Deduplication
    is_duplicate: bool = False
    duplicate_of_id: Optional[int] = Field(default=None, foreign_key="articles.id")

    # Processing status
    is_processed: bool = False
    processing_error: Optional[str] = None

    # Relationships
    deal: Optional[Deal] = Relationship(back_populates="articles")


class StealthDetection(SQLModel, table=True):
    """Track stealth portfolio additions detected via sitemap diffing."""
    __tablename__ = "stealth_detections"

    id: Optional[int] = Field(default=None, primary_key=True)
    fund_slug: str = Field(index=True)
    detected_url: str
    detected_at: datetime = Field(default_factory=utc_now_naive)
    company_name: Optional[str] = None
    is_confirmed: bool = False
    notes: Optional[str] = None


class PortfolioSnapshot(SQLModel, table=True):
    """Store portfolio page snapshots for diff detection.

    Replaces ephemeral /tmp/ storage with persistent database storage.
    This survives Railway redeploys and ensures accurate diff detection.
    """
    __tablename__ = "portfolio_snapshots"

    id: Optional[int] = Field(default=None, primary_key=True)
    fund_slug: str = Field(unique=True, index=True)  # One snapshot per fund
    companies_json: str  # JSON list of company names (lowercase)
    updated_at: datetime = Field(default_factory=utc_now_naive)


class ThesisDrift(SQLModel, table=True):
    """Track thesis drift scores over time."""
    __tablename__ = "thesis_drift"

    id: Optional[int] = Field(default=None, primary_key=True)
    fund_slug: str = Field(index=True)
    deal_id: int = Field(foreign_key="deals.id")
    drift_score: float
    sector_embedding: Optional[List[float]] = Field(
        default=None,
        sa_column=Column(Vector(1536))
    )
    calculated_at: datetime = Field(default_factory=utc_now_naive)


class TrackerColumn(SQLModel, table=True):
    """Configurable Kanban column for the tracker.

    Allows users to:
    - Rename columns
    - Reorder columns (change position)
    - Add new columns
    - Delete columns (soft delete via is_active)

    Colors are Tailwind color names: slate, blue, amber, emerald, green, red, purple, pink
    """
    __tablename__ = "tracker_columns"

    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(unique=True, index=True, max_length=50)
    display_name: str = Field(max_length=100)
    color: str = Field(default="slate", max_length=20)
    position: int = Field(default=0)
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=utc_now_naive)
    updated_at: datetime = Field(default_factory=utc_now_naive)


class TrackerItem(SQLModel, table=True):
    """CRM tracker item for managing deal pipeline.

    Represents a company being tracked through the investment pipeline.
    Can be linked to an existing Deal or created manually.

    Status field references TrackerColumn.slug for the column the item belongs to.
    """
    __tablename__ = "tracker_items"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Company info
    company_name: str = Field(max_length=255)
    round_type: Optional[str] = Field(default=None, max_length=50)  # seed, series_a, etc.
    amount: Optional[str] = Field(default=None, max_length=100)  # "$50M"
    lead_investor: Optional[str] = Field(default=None, max_length=255)
    website: Optional[str] = Field(default=None, max_length=500)

    # CRM/Pipeline fields
    status: str = Field(default="watching", max_length=50, index=True)
    notes: Optional[str] = None
    last_contact_date: Optional[date] = None
    next_step: Optional[str] = None
    position: int = Field(default=0)  # Order within column for drag-drop

    # Optional link to existing deal (for deals added from Dashboard)
    deal_id: Optional[int] = Field(default=None, foreign_key="deals.id")

    # Metadata - use sa_column for proper PostgreSQL TIMESTAMPTZ type
    created_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )
    updated_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )


class Feedback(SQLModel, table=True):
    """User feedback: flags and suggestions.

    Replaces Google Sheets integration - stores directly in PostgreSQL.

    feedback_type:
    - 'flag': Report a problem with a deal
    - 'suggestion': Suggest a missing company or report an error
    """
    __tablename__ = "feedback"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Feedback type and content
    feedback_type: str = Field(max_length=20, index=True)  # 'flag' or 'suggestion'
    company_name: str = Field(max_length=255)
    deal_id: Optional[int] = Field(default=None, foreign_key="deals.id")
    reason: Optional[str] = None  # Flag reason or suggestion details
    suggestion_type: Optional[str] = Field(default=None, max_length=50)  # 'missing_company', 'error', 'other'
    source_url: Optional[str] = None
    reporter_email: Optional[str] = Field(default=None, max_length=255)

    # Review tracking
    reviewed: bool = Field(default=False, index=True)
    reviewed_at: Optional[datetime] = None

    # Metadata
    created_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )


class TokenUsage(SQLModel, table=True):
    """Track Claude API token usage per extraction.

    Records input/output tokens for each Claude API call to enable:
    - Cost breakdown by source (brave_search, techcrunch, a16z, etc.)
    - Daily/weekly/monthly usage trends
    - Identifying which sources consume the most tokens

    Pricing (Claude 3.5 Haiku as of Dec 2024):
    - Input: $0.25 per 1M tokens
    - Output: $1.25 per 1M tokens
    - Cache read: $0.025 per 1M tokens (90% discount)
    - Cache write: $0.3125 per 1M tokens
    """
    __tablename__ = "token_usage"

    id: Optional[int] = Field(default=None, primary_key=True)
    # FIX: Use sa_column with timezone=True to match the database schema
    timestamp: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )

    # Source identification
    source_name: str = Field(index=True)  # "brave_search", "techcrunch", "a16z", etc.
    scan_id: Optional[str] = Field(default=None)  # Job ID for grouping
    article_url: Optional[str] = Field(default=None)

    # Token counts
    input_tokens: int = Field(default=0)
    output_tokens: int = Field(default=0)
    cache_read_tokens: int = Field(default=0)
    cache_write_tokens: int = Field(default=0)

    # Model info
    model: str = Field(default="claude-haiku-4-5-20251001")

    # Calculated cost (USD)
    estimated_cost_usd: float = Field(default=0.0)


class CompanyAlias(SQLModel, table=True):
    """Track company rebrands and alternative names.

    Enables deduplication across name changes (e.g., Bedrock Security → Bedrock Data).

    alias_type:
    - 'rebrand': Official name change
    - 'dba': Doing business as
    - 'acquired_name': Name before acquisition
    - 'typo': Common misspelling to handle
    """
    __tablename__ = "company_aliases"

    id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int = Field(foreign_key="portfolio_companies.id", index=True)
    alias_name: str = Field(index=True)  # The alternative name
    alias_type: str = Field(default="rebrand")  # rebrand, dba, acquired_name, typo
    effective_date: Optional[date] = None  # When the change happened
    created_at: datetime = Field(default_factory=utc_now_naive)


class DateSource(SQLModel, table=True):
    """Track dates from multiple sources with confidence scores.

    Enables multi-source verification: SEC Form D (0.95), press releases (0.85),
    article headlines (0.75), article body (0.60), article published date (0.40).

    When 2+ sources agree on a date, confidence gets a 0.1 bonus.
    """
    __tablename__ = "date_sources"

    id: Optional[int] = Field(default=None, primary_key=True)
    deal_id: int = Field(foreign_key="deals.id", index=True)
    source_type: str  # sec_form_d, press_release, article_headline, article_body, article_published
    source_url: Optional[str] = None
    extracted_date: date
    confidence_score: float = Field(default=0.5)
    is_primary: bool = Field(default=False)  # Currently used as deal.announced_date
    created_at: datetime = Field(default_factory=utc_now_naive)


class FounderValidation(SQLModel, table=True):
    """Track LinkedIn validation results for founders.

    Validates that extracted founders actually work at the company
    with matching titles. Catches errors like extracting a VP instead of CEO.
    """
    __tablename__ = "founder_validations"

    id: Optional[int] = Field(default=None, primary_key=True)
    deal_id: int = Field(foreign_key="deals.id", index=True)
    founder_name: str
    extracted_title: Optional[str] = None  # Title from article extraction
    linkedin_url: Optional[str] = None
    linkedin_current_company: Optional[str] = None  # Company from LinkedIn profile
    linkedin_current_title: Optional[str] = None  # Title from LinkedIn profile
    is_match: Optional[bool] = None  # Does LinkedIn match extracted company?
    title_is_leadership: Optional[bool] = None  # Is the title CEO/founder/CTO?
    validated_at: Optional[datetime] = None
    validation_method: str = Field(default="brave_search")  # brave_search, manual
    created_at: datetime = Field(default_factory=utc_now_naive)


class StealthSignal(SQLModel, table=True):
    """Pre-funding signals from early detection scrapers.

    Stores companies spotted BEFORE they announce funding:
    - hackernews: Launch HN posts, high-engagement discussions
    - ycombinator: Demo Day batch companies
    - github_trending: Viral dev tools and AI repos
    - linkedin_jobs: Stealth startup hiring signals
    - delaware_corps: New tech company incorporations

    These sources previously produced 0 deals through Claude extraction.
    Now they use rule-based scoring (0-100) and skip LLM calls entirely.

    When a company later raises funding, converted_deal_id links to the deal.
    """
    __tablename__ = "stealth_signals"

    id: Optional[int] = Field(default=None, primary_key=True)
    company_name: str = Field(max_length=200)
    source: str = Field(max_length=50, index=True)  # hackernews, ycombinator, github, linkedin, delaware
    source_url: str
    score: int = Field(default=0)  # 0-100 certainty score
    signals: dict = Field(default_factory=dict, sa_column=Column(JSONB))
    metadata_json: dict = Field(default_factory=dict, sa_column=Column(JSONB))
    spotted_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )
    dismissed: bool = Field(default=False)
    converted_deal_id: Optional[int] = Field(default=None, foreign_key="deals.id")
    created_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )


class ScanJob(SQLModel, table=True):
    """Track scheduled and manual scraping jobs.

    Provides visibility into what each scan found:
    - When it ran and how long it took
    - Articles found per source
    - Deals extracted and saved
    - Errors encountered

    The deals table has scan_job_id to link deals to their originating scan.
    """
    __tablename__ = "scan_jobs"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(unique=True, index=True)  # e.g., "20241225_160000"

    # Timing
    started_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True))
    )
    duration_seconds: Optional[float] = None

    # Status
    status: str = Field(default="running", index=True)  # running, success, failed
    error_message: Optional[str] = None

    # Summary stats
    total_articles_found: int = Field(default=0)
    total_deals_extracted: int = Field(default=0)
    total_deals_saved: int = Field(default=0)
    total_duplicates_skipped: int = Field(default=0)
    total_errors: int = Field(default=0)

    # Breakdown by type
    lead_deals_found: int = Field(default=0)
    enterprise_ai_deals_found: int = Field(default=0)

    # Detailed results per source (JSON)
    # {"brave_search": {"articles": 10, "deals": 2}, "techcrunch": {...}}
    source_results_json: Optional[str] = None

    # Trigger type
    trigger: str = Field(default="scheduled")  # scheduled, manual, api


class ContentHash(SQLModel, table=True):
    """Persistent content hash cache for cross-run deduplication.

    FIX (2026-01): Saves ~$10-15/month by preventing re-extraction of
    syndicated articles that were processed in a previous scan run.

    - 30-day TTL to catch syndication across days (same article on TechCrunch → VentureBeat)
    - Content length stored to prefer longer articles (full article > headline)
    - Index on (content_hash, expires_at) for efficient lookup + cleanup

    Usage:
        Check before LLM extraction: SELECT * FROM content_hashes
        WHERE content_hash = ? AND expires_at > NOW()
    """
    __tablename__ = "content_hashes"

    id: Optional[int] = Field(default=None, primary_key=True)
    content_hash: str = Field(max_length=32, index=True)  # SHA256 first 32 chars (128-bit)
    content_length: int = Field(default=0)  # For preferring longer content
    source_url: Optional[str] = Field(default=None, max_length=2000)  # Original URL (debugging)
    created_at: datetime = Field(
        default_factory=utc_now_naive,
        sa_column=Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    )
    expires_at: datetime = Field(
        sa_column=Column(DateTime(timezone=True), index=True)
    )  # created_at + 30 days
