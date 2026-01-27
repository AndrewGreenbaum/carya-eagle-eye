"""
Pydantic schemas for deal extraction with Instructor.

These schemas enforce structured JSON output from the LLM.
"""

from pydantic import BaseModel, Field, model_validator, field_validator
from typing import List, Optional
from enum import Enum
from datetime import date, timedelta
import logging
import re

from ..common.url_utils import INVALID_WEBSITE_DOMAINS

logger = logging.getLogger(__name__)

# LinkedIn profile URL pattern - must be /in/ not /company/, /jobs/, etc.
# FIX #5: Allow query params and trailing content (matches brave_enrichment.py)
# FIX #9: Require 3+ char username
LINKEDIN_PROFILE_PATTERN = re.compile(r'^https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]{3,}(?:/.*)?(?:\?.*)?$')

# Valid URL pattern for company websites
VALID_URL_PATTERN = re.compile(r'^https?://[a-zA-Z0-9][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/.*)?$')


class RoundType(str, Enum):
    """Investment round types."""
    PRE_SEED = "pre_seed"
    SEED = "seed"
    SEED_PLUS_SERIES_A = "seed_plus_series_a"  # Combined Seed + Series A (announced together)
    SERIES_A = "series_a"
    SERIES_B = "series_b"
    SERIES_C = "series_c"
    SERIES_D = "series_d"
    SERIES_E_PLUS = "series_e_plus"
    GROWTH = "growth"
    DEBT = "debt"
    UNKNOWN = "unknown"


class LeadStatus(str, Enum):
    """Confidence level for lead investor determination."""
    CONFIRMED_LEAD = "confirmed_lead"     # Explicit "led by" language
    LIKELY_LEAD = "likely_lead"           # Strong signals (priced, terms)
    PARTICIPANT = "participant"            # Explicitly participating
    UNRESOLVED = "unresolved"             # Ambiguous - "existing investors led"


class EnterpriseCategory(str, Enum):
    """AI categories for filtering deals (both enterprise and consumer)."""
    # Enterprise AI (B2B)
    INFRASTRUCTURE = "infrastructure"      # LLMOps, AI chips, MLOps, vector DBs
    SECURITY = "security"                  # AI-powered cybersecurity, threat detection
    VERTICAL_SAAS = "vertical_saas"        # AI for Healthcare, Finance, Legal, HR
    AGENTIC = "agentic"                    # Workflow automation, AI agents for teams
    DATA_INTELLIGENCE = "data_intelligence" # Enterprise search, analytics, knowledge mgmt
    # Consumer AI (B2C)
    CONSUMER_AI = "consumer_ai"            # Consumer-facing AI apps, personal assistants
    GAMING_AI = "gaming_ai"                # AI for gaming, game engines, NPCs
    SOCIAL_AI = "social_ai"                # AI for social/dating apps, content creation
    # Non-AI (specific categories for better classification)
    CRYPTO = "crypto"                      # Blockchain, Web3, DeFi, NFT, cryptocurrency
    FINTECH = "fintech"                    # Traditional fintech (neobanks, payments, lending)
    HEALTHCARE = "healthcare"              # Biotech, pharma, clinical (non-AI)
    HARDWARE = "hardware"                  # Semiconductors, devices, manufacturing
    SAAS = "saas"                          # Traditional SaaS without AI
    OTHER = "other"                        # Fallback for unclear cases
    NOT_AI = "not_ai"                      # Legacy: maps to "other" for backwards compat


class FounderInfo(BaseModel):
    """A founder of the startup."""
    name: str = Field(description="Founder's full name")
    title: Optional[str] = Field(
        default=None,
        description="Title/role (e.g., 'CEO', 'CTO', 'Co-founder')"
    )
    linkedin_url: Optional[str] = Field(
        default=None,
        description="LinkedIn profile URL if mentioned or can be inferred"
    )

    @field_validator('linkedin_url')
    @classmethod
    def validate_linkedin_url(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate LinkedIn URL is a valid profile URL.

        Rejects:
        - Non-LinkedIn URLs
        - Company pages (/company/)
        - Job listings (/jobs/)
        - Other non-profile LinkedIn pages

        Returns None for invalid URLs to allow extraction to continue.
        """
        if v is None or v == "":
            return None

        # Must be a LinkedIn profile URL
        if not LINKEDIN_PROFILE_PATTERN.match(v):
            # Check if it's a LinkedIn URL at all
            if 'linkedin.com' in v.lower():
                # It's LinkedIn but not a profile - reject silently
                logger.debug(f"Rejecting non-profile LinkedIn URL: {v}")
            else:
                # Not LinkedIn at all - LLM hallucinated
                logger.warning(f"Rejecting non-LinkedIn URL as linkedin_url: {v}")
            return None

        return v


class InvestorMention(BaseModel):
    """A single investor mentioned in the article."""
    name: str = Field(description="Investor/fund name as mentioned in text")
    role: LeadStatus = Field(description="Their role in this round")
    partner_name: Optional[str] = Field(
        default=None,
        description="Partner name if mentioned (e.g., 'Bill Gurley')"
    )
    is_tracked_fund: bool = Field(
        default=False,
        description="True if this is one of our 18 tracked funds"
    )


class ChainOfThought(BaseModel):
    """Simplified reasoning trace for the extraction decision.

    Optimized for token efficiency - only captures final reasoning.
    Saves ~100-200 tokens per extraction vs. full analysis fields.
    """
    final_reasoning: str = Field(
        description="Brief reasoning for the lead investor determination (1-2 sentences)"
    )


class DealExtraction(BaseModel):
    """
    Structured extraction of a funding deal from press release or news article.

    This is the core schema used with Instructor for LLM extraction.
    """
    # Core deal info
    startup_name: str = Field(description="Name of the company raising funding")
    startup_description: Optional[str] = Field(
        default=None,
        description="Brief description of what the startup does"
    )

    # Company links
    company_website: Optional[str] = Field(
        default=None,
        description="Company website URL if mentioned in the article"
    )
    company_linkedin: Optional[str] = Field(
        default=None,
        description="Company LinkedIn page URL if mentioned in the article"
    )

    # Founders
    founders: List[FounderInfo] = Field(
        default_factory=list,
        description="List of founders mentioned in the article with their LinkedIn URLs if available"
    )

    # Round details
    round_label: RoundType = Field(description="Type of funding round")
    amount: Optional[str] = Field(
        default=None,
        description="Amount raised (e.g., '$50M', '$50 million')"
    )
    valuation: Optional[str] = Field(
        default=None,
        description="Post-money valuation if disclosed"
    )
    round_date: Optional[date] = Field(
        default=None,
        description="Date of the announcement"
    )

    @field_validator('round_date')
    @classmethod
    def validate_date_recency(cls, v: Optional[date]) -> Optional[date]:
        """
        Reject invalid dates - likely LLM extraction errors.

        Rejects:
        - Future dates (LLM error)
        - Dates older than 365 days / 1 year (likely historical VC blog post being scraped)

        Returns None to fallback to article.published_date.
        """
        if v is None:
            return v

        today = date.today()

        # Reject future dates
        if v > today:
            logger.warning(f"Rejecting future date {v} - likely extraction error")
            return None

        # Reject dates older than 365 days / 1 year (likely historical mention)
        cutoff = today - timedelta(days=365)
        if v < cutoff:
            logger.warning(f"Rejecting old date {v} (>1 year) - likely historical VC blog post")
            return None

        return v

    @field_validator('company_website')
    @classmethod
    def validate_company_website(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate company website URL.

        Rejects:
        - Invalid URL format
        - Social media URLs (LinkedIn, Twitter, etc.)
        - News/database sites (Crunchbase, TechCrunch, etc.)

        Returns None for invalid URLs to allow extraction to continue.
        """
        if v is None or v == "":
            return None

        # Must be a valid URL format
        if not VALID_URL_PATTERN.match(v):
            logger.debug(f"Rejecting invalid website URL format: {v}")
            return None

        # Check for invalid domains (social media, news, etc.)
        v_lower = v.lower()
        for domain in INVALID_WEBSITE_DOMAINS:
            if domain in v_lower:
                logger.debug(f"Rejecting {domain} as company website: {v}")
                return None

        return v

    @field_validator('company_linkedin')
    @classmethod
    def validate_company_linkedin(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate company LinkedIn page URL.

        Must be a LinkedIn company page (/company/), not a profile (/in/).
        """
        if v is None or v == "":
            return None

        # Must be a LinkedIn company URL
        if 'linkedin.com/company/' not in v.lower():
            if 'linkedin.com' in v.lower():
                logger.debug(f"Rejecting non-company LinkedIn URL: {v}")
            else:
                logger.warning(f"Rejecting non-LinkedIn URL as company_linkedin: {v}")
            return None

        return v

    # Investors
    lead_investors: List[InvestorMention] = Field(
        description="Investors who led or co-led the round"
    )
    participating_investors: List[InvestorMention] = Field(
        description="Investors who participated but did not lead"
    )

    # Tracked fund analysis
    tracked_fund_is_lead: bool = Field(
        description="True ONLY if one of our 18 tracked funds led or co-led"
    )
    tracked_fund_name: Optional[str] = Field(
        default=None,
        description="Name of the tracked fund if involved"
    )
    tracked_fund_role: Optional[LeadStatus] = Field(
        default=None,
        description="Role of the tracked fund in this round"
    )
    tracked_fund_partner: Optional[str] = Field(
        default=None,
        description="Partner from tracked fund if mentioned"
    )

    # Reasoning
    reasoning: ChainOfThought = Field(
        description="Chain-of-thought reasoning for the extraction"
    )

    # AI Classification
    enterprise_category: EnterpriseCategory = Field(
        default=EnterpriseCategory.NOT_AI,
        description="Category of AI deal (enterprise, consumer, or not_ai)"
    )
    is_enterprise_ai: bool = Field(
        default=False,
        description="True if B2B Enterprise AI company (not consumer)"
    )
    is_ai_deal: bool = Field(
        default=False,
        description="True if this is an AI company (enterprise OR consumer)"
    )
    verification_snippet: Optional[str] = Field(
        default=None,
        description="Exact quote from article proving lead investor status (e.g., 'led by Sequoia')"
    )
    lead_evidence_weak: bool = Field(
        default=False,
        description="True if snippet lacks explicit 'led by' language but Claude determined lead status"
    )
    amount_needs_review: bool = Field(
        default=False,
        description="True if amount seems suspicious (e.g., Series A >$100M, possible market size confusion)"
    )
    amount_review_reason: Optional[str] = Field(
        default=None,
        description="Reason why amount needs review (e.g., 'Series A >$100M is unusual')"
    )

    # Quality signals
    # FIX 2026-01: Separated confidence into distinct components for clearer semantics
    # - extraction_confidence: Raw LLM confidence in extraction accuracy (before penalties)
    # - lead_evidence_score: Quality of lead investor evidence (0-1, higher = stronger evidence)
    # - confidence_score: Final combined score used for threshold decisions (backwards compatible)
    extraction_confidence: Optional[float] = Field(
        default=None,
        ge=0.0, le=1.0,
        description="Raw LLM confidence in extraction accuracy, before any penalties. "
                    "None if not set (legacy extractions)."
    )
    lead_evidence_score: Optional[float] = Field(
        default=None,
        ge=0.0, le=1.0,
        description="Quality score for lead investor evidence (0.0-1.0). "
                    "1.0 = explicit 'led by' in snippet, 0.5 = weak evidence (snippet but no lead language), "
                    "0.2 = very weak (no snippet provided at all). "
                    "None if not a lead deal or not yet calculated."
    )
    confidence_score: float = Field(
        ge=0.0, le=1.0,
        description="Final confidence score after penalties (0.0-1.0). Used for threshold decisions."
    )
    penalty_breakdown: Optional[dict] = Field(
        default=None,
        description="Breakdown of penalties applied: {founders_removed: 0.03, investors_removed: 0.05, weak_evidence: 0.08}"
    )
    # Note: thesis_drift_score removed - was never used in API/frontend

    # NEW: Announcement verification (prevents false positives from background mentions)
    is_new_announcement: bool = Field(
        default=False,  # FIX 2: Safe default - reject if unsure
        description="True ONLY if this article is ANNOUNCING a new funding round. "
                    "False if funding is mentioned as background/historical context "
                    "(e.g., 'backed by X' company descriptions, past funding references)."
    )
    announcement_evidence: Optional[str] = Field(
        default=None,
        description="Exact quote proving this is a NEW announcement "
                    "(e.g., 'today announced', 'just raised', 'closes $X round')"
    )
    announcement_rejection_reason: Optional[str] = Field(
        default=None,
        description="If is_new_announcement=False, explain why "
                    "(e.g., 'historical mention', 'company background', 'partnership announcement')"
    )

    # FIX 3: Validate announcement fields match is_new_announcement state
    @model_validator(mode='after')
    def validate_announcement_fields(self) -> 'DealExtraction':
        """
        Ensure evidence/reason fields are populated appropriately.

        CRITICAL: Prevents false positives where LLM claims new announcement
        without providing proof.
        """
        if self.is_new_announcement:
            # New announcements MUST have evidence - downgrade if missing
            if not self.announcement_evidence:
                # FIX: Don't accept is_new_announcement=True without proof
                # Downgrade to False instead of crashing extraction
                # FIX: Use module-level logger instead of creating new one each validation
                logger.warning(
                    f"Downgrading {self.startup_name}: is_new_announcement=True "
                    f"but no announcement_evidence provided"
                )
                self.is_new_announcement = False
                self.announcement_rejection_reason = (
                    "LLM claimed new announcement but provided no evidence"
                )
        else:
            # Non-announcements must have a rejection reason
            if not self.announcement_rejection_reason:
                self.announcement_rejection_reason = "Not a new funding announcement"
        return self


class ArticleAnalysis(BaseModel):
    """
    Full analysis of an article that may contain multiple deals.
    """
    source_url: str = Field(description="URL of the source article")
    title: str = Field(description="Article title")
    published_date: Optional[date] = Field(default=None)
    deals: List[DealExtraction] = Field(
        description="All funding deals mentioned in the article"
    )
    is_funding_news: bool = Field(
        description="True if this article is about funding/investment"
    )
    rejection_reason: Optional[str] = Field(
        default=None,
        description="If not funding news, why was it rejected"
    )
