"""
Lead Signal Extractor - The Intelligence Core.

Uses Instructor + Claude 3.5 Sonnet for structured extraction of funding deals
with chain-of-thought reasoning for lead vs. participation determination.

OPTIMIZED:
- Prompt caching for system message (20% token savings)
- Batch extraction for multiple articles
- Early exit for non-funding content
- Content hash deduplication to avoid duplicate API calls
- Token usage logging for cost tracking
"""

import asyncio
import hashlib
import logging
import math
import random
import re
import instructor
import httpx
from contextvars import ContextVar
from anthropic import Anthropic, APITimeoutError, APIError, RateLimitError
from instructor.core import InstructorRetryException
from typing import Optional, List, Set
from datetime import datetime, timezone, date, timedelta
from dateutil.relativedelta import relativedelta

# FIX (2026-01): Import consolidated suffix constant from storage
# This ensures _validate_company_in_text uses same suffixes as dedup normalization
from ..archivist.storage import COMPANY_NAME_SUFFIX_WORDS

logger = logging.getLogger(__name__)

# Task-local context for source tracking (async-safe with contextvars)
# Each asyncio task gets its own copy, preventing race conditions in parallel execution
_current_source_name: ContextVar[Optional[str]] = ContextVar('source_name', default=None)
_current_scan_id: ContextVar[Optional[str]] = ContextVar('scan_id', default=None)

# API call configuration - loaded from settings (see below after settings import)

# Session-based content hash cache (cleared between runs)
# Prevents duplicate Claude calls for same article content
# FIX (2026-01): Now stores (hash, length) to prefer longer content
# FIX (2026-01): Bounded to prevent unbounded memory growth


class BoundedContentHashCache:
    """Bounded LRU-like cache for content hashes to prevent memory leaks.

    When cache exceeds max_size, removes oldest 10% of entries.
    Supports dict-like access for backwards compatibility.
    """

    def __init__(self, max_size: int = 10000):
        self._cache: dict[str, int] = {}  # hash -> content length
        self._max_size = max_size
        self._insert_order: list[str] = []  # Track insertion order for LRU eviction

    def get(self, key: str, default: int | None = None) -> int | None:
        """Get content length for hash, or default if not found."""
        return self._cache.get(key, default)

    def __getitem__(self, key: str) -> int:
        """Dict-like access: cache[key]."""
        return self._cache[key]

    def __setitem__(self, key: str, value: int) -> None:
        """Dict-like assignment: cache[key] = value. Evicts old entries if needed."""
        if key not in self._cache:
            # Evict oldest 10% if at capacity
            if len(self._cache) >= self._max_size:
                evict_count = max(1, self._max_size // 10)
                for _ in range(evict_count):
                    if self._insert_order:
                        old_key = self._insert_order.pop(0)
                        self._cache.pop(old_key, None)
            self._insert_order.append(key)
        self._cache[key] = value

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        self._insert_order.clear()

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)


_content_hash_cache = BoundedContentHashCache(max_size=10000)
_content_hash_lock = asyncio.Lock()  # FIX: Async-safe cache access

# =============================================================================
# TOKEN USAGE BATCHING (reduces DB writes)
# =============================================================================
# FIX (2026-01): Batch token usage writes to reduce DB overhead
# Collects usage records and flushes every BATCH_SIZE or on explicit flush

TOKEN_USAGE_BATCH_SIZE = 10  # Flush every 10 extractions
_token_usage_batch: list[dict] = []  # Buffer for pending token usage records
_token_usage_batch_lock = asyncio.Lock()  # Async-safe batch access


# FIX: Register atexit handler to flush token usage on shutdown
# Without this, small scrapes (<10 articles) lose all token records
import atexit


def _sync_flush_tokens_at_exit() -> None:
    """Synchronous wrapper for atexit - flushes remaining token usage records.

    Called on interpreter shutdown. Uses synchronous DB access since
    asyncio event loop may not be running.
    """
    if not _token_usage_batch:
        return

    # Try to get or create an event loop for async flush
    try:
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Create a task if loop is running (rare at shutdown)
            loop.create_task(_async_flush_tokens_at_exit())
        else:
            # Most common case: no running loop, create one
            asyncio.run(_async_flush_tokens_at_exit())
    except Exception as e:
        # Best effort - log and continue shutdown
        logger.warning(f"Failed to flush token usage at exit: {e}")


async def _async_flush_tokens_at_exit() -> None:
    """Async implementation of atexit flush."""
    global _token_usage_batch

    if not _token_usage_batch:
        return

    try:
        from .schemas import DealExtraction  # Avoid circular import at module load
        from ..archivist.database import get_session
        from ..archivist.models import TokenUsage

        batch_to_flush = _token_usage_batch.copy()
        _token_usage_batch.clear()

        async with get_session() as session:
            for record in batch_to_flush:
                usage = TokenUsage(
                    timestamp=record["timestamp"],
                    source_name=record["source_name"],
                    scan_id=record["scan_id"],
                    article_url=record["article_url"],
                    input_tokens=record["input_tokens"],
                    output_tokens=record["output_tokens"],
                    cache_read_tokens=record["cache_read_tokens"],
                    cache_write_tokens=record["cache_write_tokens"],
                    model=record["model"],
                    estimated_cost_usd=record["estimated_cost_usd"]
                )
                session.add(usage)
            await session.commit()

        logger.info(f"Token usage flush at exit: {len(batch_to_flush)} records saved")
    except Exception as e:
        logger.error(f"Failed to flush token usage at exit: {e}")


atexit.register(_sync_flush_tokens_at_exit)

# Minimum content length improvement to replace cached version
# If new content is 2x longer, it's worth re-processing
MIN_LENGTH_IMPROVEMENT_RATIO = 2.0

# =============================================================================
# EXTRACTION STATS (for monitoring post-processing filter activity)
# =============================================================================

import threading

# Module-level stats for tracking post-processing filter activity
# Cleared at start of each scan via clear_extraction_stats()
_extraction_stats = {
    "crypto_filtered": 0,          # Deals reclassified as NOT_AI (crypto)
    "consumer_ai_filtered": 0,     # Deals reclassified as consumer AI
    "consumer_fintech_filtered": 0,  # Deals reclassified as consumer fintech
    "company_name_rejected": 0,    # Deals rejected for hallucinated company name
    "investors_removed": 0,        # Investors removed for not being in text
    "background_mention_rejected": 0,  # Deals rejected as background mentions
    "article_title_rejected": 0,   # Deals rejected for company name looking like title
    "lead_evidence_downgraded": 0, # Lead claims downgraded to LIKELY_LEAD
    "amount_flagged_for_review": 0,  # Amounts flagged for market size confusion
    "round_type_corrected": 0,     # Round types corrected to UNKNOWN (invalid enum)
    "founders_removed": 0,         # Founders removed for not being in text
    "relative_date_corrected": 0,  # Dates corrected from relative date phrases (e.g., "6 months ago")
    "fund_raise_rejected": 0,      # Deals rejected because startup name is a tracked VC fund
}
_extraction_stats_thread_lock = threading.Lock()


def increment_extraction_stat(stat_name: str, count: int = 1) -> None:
    """Thread-safe increment of extraction stat (synchronous for use in sync functions)."""
    with _extraction_stats_thread_lock:
        if stat_name in _extraction_stats:
            _extraction_stats[stat_name] += count


def get_extraction_stats() -> dict:
    """Get current extraction stats (for monitoring)."""
    with _extraction_stats_thread_lock:
        return _extraction_stats.copy()


def clear_extraction_stats() -> None:
    """Clear extraction stats. Call at start of each scan."""
    global _extraction_stats
    with _extraction_stats_thread_lock:
        stats_copy = _extraction_stats.copy()
        _extraction_stats = {k: 0 for k in _extraction_stats}
    total = sum(stats_copy.values())
    if total > 0:
        logger.info(
            f"Extraction stats cleared (previous run: "
            f"crypto={stats_copy['crypto_filtered']}, "
            f"consumer_ai={stats_copy['consumer_ai_filtered']}, "
            f"company_rejected={stats_copy['company_name_rejected']}, "
            f"background={stats_copy['background_mention_rejected']}, "
            f"lead_downgraded={stats_copy['lead_evidence_downgraded']})"
        )


from .schemas import DealExtraction, ArticleAnalysis, LeadStatus, RoundType, EnterpriseCategory
from ..config.settings import settings
from ..config.funds import FUND_REGISTRY, FundConfig, EXTERNAL_ONLY_FUNDS
# Note: match_fund_name is imported lazily in _verify_tracked_fund to avoid circular import

# EXTERNAL_ONLY_FUNDS is now imported from config.funds (single source of truth)
# See config/funds.py:get_external_only_fund_slugs() for the definition

# Source names that are external (used for auto-detection)
EXTERNAL_SOURCE_NAMES = frozenset({
    "google_news", "brave_search", "sec_edgar", "news_aggregator",
})

# Lower confidence threshold for external sources
# Standard is 0.5, but external sources often have less context
# FIX (2026-01): Raised from 0.35 to 0.40 to match system prompt
# (prompt says <0.35 = "Cannot verify as real funding" but threshold WAS 0.35)
EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD = 0.40

# FIX #42: Initialize module-level constants from settings (not hardcoded)
API_TIMEOUT_SECONDS = settings.llm_timeout
MAX_RETRIES = settings.llm_max_retries

# Initialize Instructor with Anthropic client
# FIX: Configure timeout at client level for reliable timeout handling
_anthropic_client = Anthropic(
    api_key=settings.anthropic_api_key,
    timeout=httpx.Timeout(settings.llm_timeout, connect=settings.llm_connect_timeout),
)
client = instructor.from_anthropic(_anthropic_client)

# Module-level Sonnet client for hybrid re-extraction (with same timeout config)
# Created once at module load for efficiency (not per-call)
_sonnet_anthropic_client = Anthropic(
    api_key=settings.anthropic_api_key,
    timeout=httpx.Timeout(settings.llm_timeout, connect=settings.llm_connect_timeout),
)
sonnet_client = instructor.from_anthropic(_sonnet_anthropic_client)

# Pre-built cached system message (optimization: created once, reused across calls)
# Anthropic's prompt caching uses content-based caching, but we save object creation overhead
CACHED_SYSTEM_MESSAGE = [
    {
        "type": "text",
        "text": None,  # Will be set after SYSTEM_PROMPT is defined
        "cache_control": {"type": "ephemeral"}
    }
]


SYSTEM_PROMPT = """You are a VC analyst extracting funding deals from articles. Extract:
1. Deal details (company, round, amount, date)
2. Lead vs participating investors (with proof snippet)
3. AI classification (enterprise, consumer, or not AI)
4. If any TRACKED FUND led the round

AI CLASSIFICATION (set is_ai_deal=True for ALL AI companies)

ENTERPRISE AI (is_enterprise_ai=True, is_ai_deal=True):
- infrastructure: LLMOps, MLOps, vector DBs, AI chips, GPU cloud, model serving, inference optimization, training infrastructure, embeddings, RAG, retrieval augmented generation, model fine-tuning, prompt engineering platforms, AI observability
- security: AI-powered cybersecurity, threat detection, security copilot, AI SOC, vulnerability detection, AI red team, deepfake detection, AI fraud detection
- vertical_saas: AI for Healthcare (clinical AI, medical imaging, drug discovery), Finance (AI underwriting, risk models, compliance AI), Legal (contract AI, legal research AI), HR (AI recruiting, talent AI), Manufacturing (predictive maintenance, quality AI), Supply chain AI, Logistics AI
- agentic: Workflow automation, AI agents for business, autonomous agents, agent orchestration, AI copilots for enterprise, AI assistants for work, RPA with AI, process automation
- data_intelligence: Enterprise search, AI analytics, knowledge management, document AI, data labeling, synthetic data, data quality, AI data pipelines, unstructured data processing, enterprise RAG

CONSUMER AI (is_enterprise_ai=False, is_ai_deal=True):
- consumer_ai: Consumer-facing AI apps, personal assistants, AI photo editors, AI art generators for consumers, AI tutors for individuals
- gaming_ai: AI for games, game engines, AI-powered NPCs
- social_ai: AI for social/dating apps, AI content creation for consumers

NOT AI (is_enterprise_ai=False, is_ai_deal=False):
- crypto: Blockchain, Web3, DeFi, NFT, cryptocurrency, token, stablecoin
- fintech: Neobank, payments, lending, trading, banking (non-AI traditional fintech)
- healthcare: Biotech, pharma, clinical trials, medical devices (non-AI)
- hardware: Semiconductors, chips, devices, manufacturing, physical products
- saas: Traditional SaaS software without AI features
- other: Unclear or doesn't fit above categories

IMPORTANT: Use "other" if uncertain about non-AI category. Only use specific categories (crypto, fintech, etc.) if keywords appear EXPLICITLY in the article text.

LEAD VERIFICATION (STRICT)
LEAD phrases: "led by", "led the", "leads", "co-led", "headed by", "spearheaded"
NOT LEAD: "backed by", "participated in", "invested in", "joined"

CRITICAL: Only set tracked_fund_is_lead=True if verification_snippet contains ACTUAL lead language.
If no lead phrase exists → put in participating_investors with role=participant, NOT lead_investors.

NEVER HALLUCINATE INVESTORS (CRITICAL - prevents wrong data):
- ONLY include investors that are EXPLICITLY named in the article text
- If NO investor names appear in article → lead_investors=[], participating_investors=[]
- Fund context (which feed we found this on) is NOT evidence of investment
- Do NOT assume a fund invested just because article was on their news feed
- If article says "raised $2M in seed funding" with no investors mentioned → leave investor lists EMPTY

VERIFICATION SNIPPET (CRITICAL FOR ACCURACY):
Extract a COMPLETE sentence (15-50 words) that proves lead status. Include:
- The fund name AND the company name
- The lead phrase ("led by", "leads", etc.)
- Context (amount, round type if in same sentence)

GOOD snippets:
- "The $50M Series A was led by Sequoia Capital, with participation from..."
- "Acme's seed round, led by a16z partner Marc Andreessen, closed at $12M"
- "Benchmark led the investment, marking the firm's first AI infrastructure bet"

BAD snippets (too short, missing context):
- "led by Sequoia" (no company name, no context)
- "Sequoia invested" (no lead language)
- "the round" (meaningless)

FOUNDER/CEO EXTRACTION (CRITICAL - extract at least ONE person when mentioned)
ALWAYS extract the CEO or lead founder. This is HIGH PRIORITY data.

Priority order:
1. CEO (highest priority)
2. CEO & Founder / CEO & Co-founder
3. Founder / Co-founder (if no CEO mentioned)
4. CTO, President, or other C-level IF they are also a founder
5. Any person described as "founder" or who "founded" the company

Extract ONE person with:
- name: Full name (first + last). "John Smith", not "John" or "Smith"
- title: Their actual title (CEO, Founder, CEO & Co-founder, CTO & Co-founder, etc.)
- linkedin_url: ONLY if explicitly in article text (rare)

FOUNDER EXAMPLES:
Article: "Acme, founded by Jane Doe and John Smith, raised $10M. Jane is CEO."
→ founders: [{name: "Jane Doe", title: "CEO"}]

Article: "CEO Sarah Johnson announced the funding"
→ founders: [{name: "Sarah Johnson", title: "CEO"}]

Article: "The startup, co-founded by Mike Chen, raised $5M"
→ founders: [{name: "Mike Chen", title: "Co-founder"}]

Article: "Alex Rivera founded Acme in 2023"
→ founders: [{name: "Alex Rivera", title: "Founder"}]

Article: "The startup raised $5M" (NO person mentioned anywhere)
→ founders: [] (empty ONLY if no founder/CEO name appears)

DO NOT extract:
- Investor partners (e.g., "Bill Gurley of Benchmark" - he's an investor, not a founder)
- Board members who aren't founders
- Random employees mentioned in quotes

IMPORTANT: Empty founders list should be RARE. Most funding articles mention the CEO or founder. Look carefully for any person associated with the startup being funded.

TRACKED FUNDS: Founders Fund, Benchmark, Sequoia, Khosla, Index, a16z, Insight Partners, Bessemer, Redpoint, Greylock, GV, Menlo, USV, Thrive, Accel, Felicis, General Catalyst, First Round

DISAMBIGUATION: Benchmark≠Benchmark International/Electronics, GV≠NYSE:GV, Thrive≠Thrive IT/Global, Sequoia=US only (not HongShan/Peak XV)

COMPANY NAME: Must be actual company, NOT article titles like "America's Construction Problem". If can't identify → startup_name="<UNKNOWN>", is_new_announcement=False

URLS: Only extract if EXPLICITLY in article text. Never guess. Set to null if not mentioned.

DATE EXTRACTION (IMPORTANT - affects data accuracy):
- Extract the ANNOUNCEMENT/CLOSING date of the funding round, NOT the article's publication date
- Look for phrases like: "announced today", "closed on", "raised on [date]", "as of [date]"
- Relative dates: "yesterday", "last week", "earlier this month" → calculate actual date
- If multiple dates in article, prefer the one closest to announcement language
- If no date can be determined → round_date=null (don't guess)
- Reject dates >1 year old (likely historical mention, not new announcement)

ROUND TYPES: pre_seed, seed, seed_plus_series_a (when Seed AND Series A announced together as single round), series_a, series_b, series_c, series_d, series_e_plus, growth, debt, unknown

AMOUNT EXTRACTION (CRITICAL - distinguish funding from valuation):
- Extract the FUNDING AMOUNT (money raised), NOT the valuation
- "raises $50M at a $500M valuation" → amount=$50M (NOT $500M)
- "valued at $6B" or "$6B valuation" → this is VALUATION, look for actual funding amount
- "Series B funding of $330M, valuing the company at $6.6B" → amount=$330M
- Headlines like "valued at $X billion" often bury the actual raise in the article
- If only valuation mentioned with no funding amount → amount=null
- Valuation keywords: "valued at", "valuation", "worth", "valued", "value of"
- Funding keywords: "raises", "raised", "secures", "secured", "closes", "funding round"

NEW ANNOUNCEMENT CHECK (CRITICAL - prevents false positives)
TRUE (is_new_announcement=True): Funding IS the article's primary topic
FALSE (is_new_announcement=False): Funding mentioned as background/context

ALWAYS SET FALSE FOR:
- Competitor comparisons: "X competes with Y, which raised $48M..." → Y is background
- Company profiles: "X, backed by Sequoia, launches..." → historical funding
- Partnership/product news mentioning past funding
- Any company that is NOT the article's primary subject

ONLY SET TRUE IF:
- Article headline/title is about THIS company's funding
- Direct announcement: "X raises $50M", "X closes Series A"
- Press release FROM the company raising funds

Set announcement_evidence (if true) or announcement_rejection_reason (if false).

EXAMPLE 1: "BuildOps, backed by Founders Fund, expands integration"
→ is_new_announcement=False, reason="backed by is historical, not new funding"

EXAMPLE 2: "Acme raises $50M Series B led by Sequoia"
→ is_new_announcement=True, evidence="raises $50M Series B led by Sequoia"

EXAMPLE 3: "ResolveAI raises $30M Series A; competes with Traversal, which raised $48M"
→ Extract ResolveAI (the PRIMARY subject raising funds)
→ IGNORE Traversal (background mention, not primary subject)

HEADLINE-ONLY CONTENT:
Some articles may only contain a headline with minimal context. Extract what you can:
- "Thrive Capital Leads $34M Investment Round" → tracked_fund=Thrive, amount=34M, is_lead=True
- "Acme raises $50M in Series B funding" → company=Acme, amount=50M, round=series_b
- If company name is missing from headline → startup_name="<UNKNOWN>", is_new_announcement=False
- Headlines with clear lead language ("leads", "led") ARE sufficient for is_lead=True

CONFIDENCE SCORING (0.0-1.0): Higher = more complete info. Low confidence (<0.35) for unclear company, no amount, or background mentions.

Return ONE deal per article (the primary/new announcement, not historical mentions).
Output valid JSON matching the schema."""

# Initialize the cached system message with the prompt text
CACHED_SYSTEM_MESSAGE[0]["text"] = SYSTEM_PROMPT


# =============================================================================
# TOKEN USAGE LOGGING
# =============================================================================

# Claude 3.5 Haiku pricing (October 2024)
# See: https://www.anthropic.com/pricing
HAIKU_INPUT_COST_PER_MILLION = 0.80  # $0.80 per 1M input tokens
HAIKU_OUTPUT_COST_PER_MILLION = 4.00  # $4.00 per 1M output tokens
HAIKU_CACHE_READ_COST_PER_MILLION = 0.08  # $0.08 per 1M cache read tokens (10% of input)
HAIKU_CACHE_WRITE_COST_PER_MILLION = 1.00  # $1.00 per 1M cache write tokens (125% of input)


def set_extraction_context(source_name: str, scan_id: Optional[str] = None) -> None:
    """Set the current extraction context for token logging.

    Call this before starting extraction to track which source is being processed.
    Uses contextvars for async-safe task-local storage.

    Args:
        source_name: Name of the source (e.g., "brave_search", "techcrunch", "a16z")
        scan_id: Optional job ID for grouping (e.g., scan timestamp)
    """
    _current_source_name.set(source_name)
    _current_scan_id.set(scan_id)


def clear_extraction_context() -> None:
    """Clear the extraction context after processing."""
    _current_source_name.set(None)
    _current_scan_id.set(None)


def _calculate_cost(
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0
) -> float:
    """Calculate estimated cost in USD for token usage."""
    cost = (
        (input_tokens * HAIKU_INPUT_COST_PER_MILLION / 1_000_000) +
        (output_tokens * HAIKU_OUTPUT_COST_PER_MILLION / 1_000_000) +
        (cache_read_tokens * HAIKU_CACHE_READ_COST_PER_MILLION / 1_000_000) +
        (cache_write_tokens * HAIKU_CACHE_WRITE_COST_PER_MILLION / 1_000_000)
    )
    return round(cost, 6)


async def flush_token_usage_batch(force: bool = False) -> None:
    """Flush pending token usage records to database.

    Args:
        force: If True, flush even if batch isn't full (used at end of scan)

    FIX (2026-01): Re-add on failure now happens under lock. The copy-and-clear
    was already atomic, but the failure path had a race condition:
    1. Thread A's DB write fails, needs to re-add batch
    2. Thread B adds new record between failure and re-add
    3. Records get out of order or lost on subsequent flush
    Now the re-add is protected by acquiring the lock first.
    """
    global _token_usage_batch

    # FIX: Hold lock during entire copy-and-clear to prevent race
    async with _token_usage_batch_lock:
        if not _token_usage_batch:
            return

        if not force and len(_token_usage_batch) < TOKEN_USAGE_BATCH_SIZE:
            return

        # Grab batch and clear buffer atomically (under lock)
        batch_to_flush = _token_usage_batch.copy()
        _token_usage_batch = []

    # Flush outside lock to avoid blocking other adds
    logger.debug(f"[TOKEN BATCH] Flushing {len(batch_to_flush)} token usage records")

    try:
        from ..archivist.database import get_session
        from ..archivist.models import TokenUsage

        async with get_session() as session:
            for record in batch_to_flush:
                usage = TokenUsage(
                    timestamp=record["timestamp"],
                    source_name=record["source_name"],
                    scan_id=record["scan_id"],
                    article_url=record["article_url"],
                    input_tokens=record["input_tokens"],
                    output_tokens=record["output_tokens"],
                    cache_read_tokens=record["cache_read_tokens"],
                    cache_write_tokens=record["cache_write_tokens"],
                    model=record["model"],
                    estimated_cost_usd=record["estimated_cost_usd"]
                )
                session.add(usage)

            await session.commit()

        total_cost = sum(r["estimated_cost_usd"] for r in batch_to_flush)
        logger.info(
            f"Token usage batch saved: {len(batch_to_flush)} records, "
            f"total cost=${total_cost:.4f}"
        )
    except Exception as e:
        logger.error(f"Failed to flush token usage batch: {e}", exc_info=True)
        # FIX (2026-01): Re-add failed records under lock to prevent race condition
        # Prepend failed records so they're retried first on next flush
        async with _token_usage_batch_lock:
            _token_usage_batch = batch_to_flush + _token_usage_batch
            logger.warning(f"[TOKEN BATCH] Re-queued {len(batch_to_flush)} failed records for retry")


async def log_token_usage(
    input_tokens: int,
    output_tokens: int,
    source_name: Optional[str] = None,
    scan_id: Optional[str] = None,
    article_url: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001",
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0
) -> None:
    """Log token usage to database for cost tracking (batched for efficiency).

    FIX (2026-01): Now batches writes every 10 extractions to reduce DB overhead.
    Call flush_token_usage_batch(force=True) at end of scan to ensure all records saved.

    Args:
        input_tokens: Number of input tokens used
        output_tokens: Number of output tokens generated
        source_name: Source identifier (uses context if not provided)
        scan_id: Job ID for grouping (uses context if not provided)
        article_url: URL of the article being processed
        model: Model name used
        cache_read_tokens: Tokens read from cache
        cache_write_tokens: Tokens written to cache
    """
    global _token_usage_batch

    # Use context values if not explicitly provided (ContextVar.get() for async safety)
    context_source = _current_source_name.get()
    context_scan_id = _current_scan_id.get()
    source = source_name or context_source or "unknown"
    job_id = scan_id or context_scan_id

    # DEBUG: Log detailed attribution info when falling back to unknown
    if source == "unknown":
        logger.warning(
            f"[TOKEN ATTRIBUTION] Falling back to 'unknown' - "
            f"source_name={repr(source_name)}, "
            f"context_source={repr(context_source)}, "
            f"article_url={article_url[:80] if article_url else 'None'}..."
        )

    cost = _calculate_cost(input_tokens, output_tokens, cache_read_tokens, cache_write_tokens)
    logger.debug(f"[TOKEN DEBUG] log_token_usage called: source={source}, in={input_tokens}, out={output_tokens}")

    # Create record for batch
    record = {
        "timestamp": datetime.now(timezone.utc),
        "source_name": source,
        "scan_id": job_id,
        "article_url": article_url[:2000] if article_url and len(article_url) > 2000 else article_url,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": cache_read_tokens,
        "cache_write_tokens": cache_write_tokens,
        "model": model,
        "estimated_cost_usd": cost
    }

    # Add to batch
    async with _token_usage_batch_lock:
        _token_usage_batch.append(record)
        batch_size = len(_token_usage_batch)

    # Flush if batch is full
    if batch_size >= TOKEN_USAGE_BATCH_SIZE:
        await flush_token_usage_batch()


# Keywords that indicate funding content (for early exit optimization)
# Split into strong (must have) and supporting (help confirm) keywords
# FIX (2026-01): Added "leads" for headline-only content like "Thrive Capital Leads $34M"
# FIX (2026-01): Added 11 missing common funding verbs that were causing false negatives
STRONG_FUNDING_KEYWORDS = [
    # Original keywords
    "raised", "raises", "funding", "series", "seed round", "led by", "leads",
    "investment round", "secures", "closes round", "announces funding",
    # NEW: Common funding verbs that were missing (causing false negatives)
    "closes", "gets", "bags", "nabs", "receives", "wins",
    "grabs", "pulls", "attracts", "lands", "raise",  # "raise" = noun form
    # FIX (2026-01): "led" standalone for "has led" phrasing (e.g., "GV has led a $40M investment")
    "led", "invests",
]
AMOUNT_KEYWORDS = ["million", "billion", "$", "mn", "m funding"]  # "mn" = Indian format
SUPPORTING_KEYWORDS = [
    "investment", "round", "venture", "capital", "investor",
    "announces", "valuation", "pre-seed", "series a", "series b", "series c"
]

# FIX: Pre-compute combined keyword list at module level (was created on every call)
ALL_FUNDING_KEYWORDS = STRONG_FUNDING_KEYWORDS + AMOUNT_KEYWORDS + SUPPORTING_KEYWORDS

# Pre-compile keyword patterns for efficiency (follows CRYPTO_KEYWORDS_PATTERN pattern)
_STRONG_FUNDING_ESCAPED = [re.escape(kw) for kw in STRONG_FUNDING_KEYWORDS]
STRONG_FUNDING_PATTERN = re.compile(
    r'(' + '|'.join(_STRONG_FUNDING_ESCAPED) + r')',
    re.IGNORECASE
)

_AMOUNT_KEYWORDS_ESCAPED = [re.escape(kw) for kw in AMOUNT_KEYWORDS]
AMOUNT_KEYWORDS_PATTERN = re.compile(
    r'(' + '|'.join(_AMOUNT_KEYWORDS_ESCAPED) + r')',
    re.IGNORECASE
)


def is_likely_funding_content(text: str) -> bool:
    """
    Quick check if text is likely about funding. Uses compiled regex.

    Used for early exit optimization - skip Claude call if text
    doesn't contain funding-related keywords.

    TIGHTENED: Requires either:
    - 1 strong keyword + 1 amount keyword (e.g., "raised $50 million")
    - 3+ total keywords (broader coverage)
    """
    # Check for strong keyword + amount (most reliable signal)
    has_strong = bool(STRONG_FUNDING_PATTERN.search(text))
    has_amount = bool(AMOUNT_KEYWORDS_PATTERN.search(text))
    if has_strong and has_amount:
        return True

    # Fallback: require 3+ keywords total
    all_matches = STRONG_FUNDING_PATTERN.findall(text) + AMOUNT_KEYWORDS_PATTERN.findall(text)
    return len(all_matches) >= 3


def _compute_content_hash(text: str) -> str:
    """Compute a hash of the article content for deduplication.

    FIX: Extended from 16 to 32 chars (128-bit) for lower collision risk.
    """
    # Normalize whitespace and lowercase for consistent hashing
    normalized = " ".join(text.lower().split())
    return hashlib.sha256(normalized.encode()).hexdigest()[:32]  # 128-bit


async def _is_duplicate_content(text: str) -> bool:
    """
    Check if we've already processed this content in this session.

    Returns True if duplicate (should skip), False if new or if this version
    is significantly longer than the cached version.

    FIX (2026-01): Now prefers longer content. If a headline-only version was
    processed first, a full article version will still be processed. This fixes
    the issue where Google News headlines blocked Brave Search full articles.

    FIX: Made async with lock for thread-safety in concurrent extraction.
    """
    content_hash = _compute_content_hash(text)
    text_len = len(text) if text else 0
    text_preview = text[:100].replace('\n', ' ') if text else "(empty)"

    async with _content_hash_lock:
        if content_hash in _content_hash_cache:
            cached_len = _content_hash_cache[content_hash]

            # If new content is significantly longer, process it anyway
            # This catches cases where headline was processed before full article
            if text_len > cached_len * MIN_LENGTH_IMPROVEMENT_RATIO:
                logger.info(
                    f"Replacing short cache entry with longer content - "
                    f"hash={content_hash}, cached_len={cached_len}, new_len={text_len}"
                )
                _content_hash_cache[content_hash] = text_len
                return False  # Process this longer version

            logger.debug(
                f"Duplicate content detected - hash={content_hash}, "
                f"cache_size={len(_content_hash_cache)}, len={text_len}, "
                f"text_preview='{text_preview}...'"
            )
            return True

        logger.debug(f"New content - hash={content_hash}, len={text_len}, adding to cache (size={len(_content_hash_cache)})")
        _content_hash_cache[content_hash] = text_len
        return False


def clear_content_hash_cache() -> None:
    """Clear the content hash cache. Call between scrape runs."""
    cache_size = len(_content_hash_cache)
    _content_hash_cache.clear()
    logger.info(f"Content hash cache cleared (had {cache_size} entries)")


# =============================================================================
# HYBRID EXTRACTION: Re-extract with Sonnet for low-confidence results
# =============================================================================

async def _reextract_with_sonnet(
    article_text: str,
    source_url: str,
    source_name: str,
    prompt: str,
    original_response: DealExtraction,
) -> Optional[DealExtraction]:
    """
    Re-extract a deal using Sonnet when Haiku's extraction has quality issues.

    Called when:
    - Internal sources: Confidence in range 0.45-0.65 (tighter threshold)
    - External sources: Confidence in range 0.35-0.65 (more lenient for headlines)
    - High-conf path: >0.65 + weak evidence + tracked lead
    - Deal appears valid (is_new_announcement=True)
    - Has quality issues (weak lead evidence OR no founders)

    Returns:
        Sonnet's extraction if it's better, None if Haiku's was better or error
    """
    try:
        # Use module-level sonnet_client (created at import with proper timeout config)
        # Make the call with Sonnet
        response, completion = sonnet_client.messages.create_with_completion(
            model=settings.llm_model_fallback,
            max_tokens=settings.llm_max_tokens,
            temperature=settings.llm_temperature,
            system=CACHED_SYSTEM_MESSAGE,
            messages=[{"role": "user", "content": prompt}],
            response_model=DealExtraction,
        )

        # Log token usage
        if completion and hasattr(completion, 'usage'):
            usage = completion.usage
            cache_read = getattr(usage, 'cache_read_input_tokens', 0) or 0
            cache_write = getattr(usage, 'cache_creation_input_tokens', 0) or 0
            logger.info(
                f"Sonnet re-extraction tokens: in={usage.input_tokens}, out={usage.output_tokens}"
            )
            await log_token_usage(
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                source_name=f"{source_name}_sonnet_reextract",
                article_url=source_url,
                model=settings.llm_model_fallback,
                cache_read_tokens=cache_read,
                cache_write_tokens=cache_write
            )

        # Apply same post-processing as Haiku (all validation steps)
        # This ensures fair comparison between Haiku and Sonnet results
        # FIX (2026-01): Added _validate_round_type and _validate_founders_in_text
        # which were missing, causing duplicate calls after Sonnet returned
        # FIX (2026-01): Wrap post-processing in try-catch to fall back to Haiku on errors
        try:
            response = _validate_company_in_text(response, article_text)
            response = _validate_startup_not_fund(response, article_text)
            response = _validate_round_type(response)
            response = _validate_investors_in_text(response, article_text)
            response = _validate_founders_in_text(response, article_text)
            response = _verify_tracked_fund(response, article_text)
        except Exception as post_proc_error:
            logger.error(
                f"HYBRID_FAILED: Sonnet post-processing error for {source_url}: {post_proc_error}",
                exc_info=True
            )
            # Fall back to Haiku result rather than failing entirely
            return None

        # Decide which result is better
        # Prefer Sonnet if: higher confidence OR more founders OR stronger lead evidence
        haiku_score = original_response.confidence_score
        sonnet_score = response.confidence_score

        haiku_founders = len(original_response.founders)
        sonnet_founders = len(response.founders)

        haiku_weak = original_response.lead_evidence_weak
        sonnet_weak = response.lead_evidence_weak

        # Calculate improvement
        confidence_improved = sonnet_score > haiku_score
        founders_improved = sonnet_founders > haiku_founders
        evidence_improved = haiku_weak and not sonnet_weak

        if confidence_improved or founders_improved or evidence_improved:
            logger.info(
                f"HYBRID: Sonnet improved extraction for {response.startup_name} - "
                f"confidence: {haiku_score:.2f}→{sonnet_score:.2f}, "
                f"founders: {haiku_founders}→{sonnet_founders}, "
                f"weak_evidence: {haiku_weak}→{sonnet_weak}"
            )
            return response
        else:
            logger.info(
                f"HYBRID: Sonnet did not improve, keeping Haiku result for {original_response.startup_name} - "
                f"confidence: {haiku_score:.2f}→{sonnet_score:.2f}"
            )
            return None

    except (APITimeoutError, APIError) as e:
        logger.warning(f"HYBRID: Sonnet re-extraction failed for {source_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"HYBRID: Unexpected error in Sonnet re-extraction: {e}")
        return None


# Keywords that indicate crypto/blockchain content (not Enterprise AI)
# OPTIMIZED: Added more keywords to catch crypto articles earlier (saves ~15% tokens)
CRYPTO_KEYWORDS = [
    # Core crypto terms
    "crypto", "blockchain", "web3", "defi", "nft", "token",
    "protocol", "layer 2", "l2", "dao", "smart contract",
    # Specific chains
    "ethereum", "bitcoin", "solana", "polygon", "avalanche",
    "arbitrum", "optimism", "base chain", "cosmos", "polkadot",
    # DeFi mechanics
    "decentralized", "on-chain", "tokenomics", "staking",
    "yield farming", "liquidity pool", "airdrop", "bridge",
    "validator", "consensus", "gas fee", "mint",
    # Tokenization (crypto asset tokenization, distinct from NLP tokenization)
    "tokenization", "tokenize", "tokenized",
    # Crypto-specific terms
    "wallet connect", "metamask", "dex", "amm", "tvl",
    "memecoin", "meme coin", "altcoin", "stablecoin",
]

# FIX (2026-01): Pre-compile crypto keyword regex for efficiency
# Was iterating through 35+ keywords for every article, now single regex search
# Escape special chars and join with | for alternation
_CRYPTO_KEYWORDS_ESCAPED = [re.escape(kw) for kw in CRYPTO_KEYWORDS]
CRYPTO_KEYWORDS_PATTERN = re.compile(
    r'(' + '|'.join(_CRYPTO_KEYWORDS_ESCAPED) + r')',
    re.IGNORECASE
)

# Company name patterns that indicate crypto (for stricter detection)
# FIX 2026-01: Removed ambiguous patterns that cause false positives
# - "chain" removed: matches "SupplyChain", "Blockchain" (valid AI companies)
# - "vault" removed: matches "DataVault" (valid)
# - "finance" removed: matches "MarketFinance" (valid)
# - "bridge" removed: matches "DataBridge" (valid)
# These are now caught by keyword density check (3+ crypto keywords in article)
CRYPTO_COMPANY_PATTERNS = [
    "0x",       # 0x prefix is explicitly crypto (0xMiden, 0xSplits, 0x Protocol)
    "defi",     # Explicit DeFi in name
    "nft",      # Explicit NFT in name
    "web3",     # Explicit Web3 in name
    "dao",      # DAO suffix is almost always crypto
    "swap",     # e.g., "Uniswap", "Sushiswap" - swap suffix is crypto-specific
]

# More ambiguous patterns that require word boundary matching
# These match if the word appears as a separate token, not embedded
CRYPTO_COMPANY_PATTERNS_STRICT = [
    r'\bprotocol\b',  # "Protocol" as standalone word (not "ProtocolLabs" or "DataProtocol")
    r'\btoken\b',      # "Token" as standalone word
    r'\bcoin\b',       # "Coin" as standalone word (not "Coinbase")
]

# URL patterns that indicate crypto websites
# FIX 2026-01: Removed "chain" (too broad - matches supplychain, markovchain, etc.)
# FIX 2026-01: Removed "blockchain" (could block legitimate AI+blockchain for provenance)
# These are now caught by keyword density check (3+ keywords) which is more accurate
CRYPTO_URL_PATTERNS = [
    ".eth", ".crypto", ".sol",  # Crypto TLDs (strong signal)
    "crypto", "defi", "nft", "web3",  # In domain name (strong signal)
    "coindesk", "cointelegraph", "theblock.co", "decrypt.co",  # Crypto news sites
    "token",  # Usually crypto-specific
]

# Known crypto/web3 companies that should NOT be classified as Enterprise AI
KNOWN_CRYPTO_COMPANIES = [
    "farcaster", "cork",
]

# Keywords that indicate Enterprise AI (B2B AI companies)
# FIX 2026-01: Added for better Enterprise AI detection - LLM sometimes misses these
ENTERPRISE_AI_KEYWORDS = [
    # Infrastructure
    "llmops", "mlops", "vector database", "vector db", "embeddings",
    "rag", "retrieval augmented", "model serving", "inference",
    "gpu cloud", "ai infrastructure", "ai platform", "model training",
    "fine-tuning", "fine tuning", "prompt engineering", "ai observability",
    "feature store", "ml platform", "ai deployment",
    # Security
    "ai security", "security ai", "cybersecurity ai", "ai soc",
    "threat detection", "security copilot", "ai fraud", "deepfake detection",
    # Vertical SaaS
    "clinical ai", "medical ai", "healthcare ai", "drug discovery",
    "ai underwriting", "compliance ai", "legal ai", "contract ai",
    "ai recruiting", "talent ai", "hr ai", "manufacturing ai",
    "supply chain ai", "logistics ai", "predictive maintenance",
    # Agentic
    "ai agent", "ai agents", "autonomous agent", "agent orchestration",
    "ai copilot", "enterprise copilot", "workflow automation",
    "process automation", "rpa", "business automation",
    # Data Intelligence
    "enterprise search", "ai analytics", "knowledge management",
    "document ai", "data labeling", "synthetic data", "unstructured data",
    "enterprise rag", "data quality", "ai data pipeline",
    # General B2B signals
    "b2b ai", "enterprise ai", "ai for enterprise", "ai-powered platform",
    "ai saas", "vertical ai", "industry ai", "enterprise software",
]

# Keywords that indicate consumer AI (NOT Enterprise AI)
# FIX: Detect consumer AI to prevent false Enterprise AI classification
CONSUMER_AI_KEYWORDS = [
    # Dating/social apps
    "dating app", "dating platform", "matchmaking", "relationship app",
    "social network", "social media app", "social platform",
    # Personal consumer apps
    "photo editor", "photo editing", "selfie", "face filter",
    "personal assistant", "life coach", "habit tracker",
    "fitness app", "workout app", "diet app", "meditation app",
    "journaling app", "diary app",
    # Consumer entertainment
    "game", "gaming", "mobile game", "casual game",
    "entertainment app", "music app", "video app", "streaming app",
    # Consumer-focused signals
    "b2c", "consumer app", "consumer product", "consumer-facing",
    "app store", "play store", "mobile-first", "consumer startup",
    "individual users", "personal use", "everyday users",
]

# Company name patterns that indicate consumer AI
CONSUMER_AI_COMPANY_PATTERNS = [
    "dating", "social", "selfie", "photo", "game", "gaming",
    "match", "love", "friend", "chat", "snap", "gram",
    "music", "video", "stream", "play", "fun",
]

# Keywords that indicate consumer fintech (NOT AI - traditional fintech, not AI-powered)
# FIX 2026-01: Neobrokers, trading apps, consumer banking are NOT Enterprise AI
CONSUMER_FINTECH_KEYWORDS = [
    # Stock trading / brokerage
    "neobroker", "neo-broker", "stock trading", "stock trading app",
    "retail trading", "retail investor", "retail brokerage",
    "commission-free trading", "zero-commission", "fractional shares",
    "robinhood competitor", "trading platform",
    # Consumer banking
    "neobank", "neo-bank", "challenger bank", "digital bank",
    "mobile banking", "consumer banking", "retail banking",
    "checking account", "savings account", "debit card",
    # Consumer payments
    "mobile payments", "peer-to-peer payments", "p2p payments",
    "money transfer", "remittance", "payment app",
    # Consumer lending
    "consumer lending", "personal loans", "buy now pay later", "bnpl",
    # Insurance
    "insurtech", "consumer insurance", "personal insurance",
]

# Company name patterns that indicate consumer fintech (NOT AI)
CONSUMER_FINTECH_COMPANY_PATTERNS = [
    "bank", "trade", "trading", "broker", "invest", "wealth",
    "pay", "money", "cash", "coin", "finance", "capital",
]

# Known consumer fintech companies that should NOT be classified as AI
KNOWN_CONSUMER_FINTECH = [
    "trade republic", "robinhood", "revolut", "n26", "chime",
    "sofi", "nubank", "monzo", "starling", "wise", "transferwise",
    "venmo", "cash app", "square", "stripe", "plaid",
    "klarna", "affirm", "afterpay", "clearpay",
    "webull", "public.com", "m1 finance", "acorns", "stash",
    "etoro", "freetrade", "wealthfront", "betterment",
]


def is_likely_crypto_article(text: str, source_url: str = "") -> bool:
    """
    PRE-CHECK: Detect if article is from a crypto news site BEFORE calling Claude.

    FIX (2026-01): Simplified to URL-only check. Previous keyword-count approach
    (3+ crypto keywords) was ineffective - most crypto headlines have only 1-2 keywords.

    Now only filters articles from known crypto news sites (strong signal).
    All other articles go to Claude for proper classification, then post-extraction
    _is_crypto_deal() marks crypto companies as is_ai_deal=False (but still saves them).

    This change:
    - Prevents false negatives on AI+blockchain companies
    - Saves all lead deals from tracked funds (even if crypto)
    - User can filter by is_ai_deal=True in frontend to see only AI deals
    """
    url_lower = source_url.lower() if source_url else ""

    # Only check URL patterns (crypto news sites are strong signals)
    # Keyword-count check removed - was catching almost nothing and risked false positives
    if url_lower:
        # Check for crypto news sites specifically (not general crypto terms in URL)
        crypto_news_sites = [
            "coindesk", "cointelegraph", "theblock.co", "decrypt.co",
            "cryptonews", "bitcoinmagazine", "cryptoslate", "coingecko",
        ]
        for site in crypto_news_sites:
            if site in url_lower:
                logger.debug(f"Pre-check: Skipping crypto news site article ({site})")
                return True

        # Check for crypto TLDs (very strong signal)
        crypto_tlds = [".eth", ".crypto", ".sol"]
        for tld in crypto_tlds:
            if tld in url_lower:
                logger.debug(f"Pre-check: Skipping crypto TLD article ({tld})")
                return True

    return False


def _is_crypto_deal(deal: DealExtraction, article_text: str) -> bool:
    """
    Detect if deal is crypto-related (should NOT be Enterprise AI).

    Called as post-extraction validation to catch crypto deals that the LLM
    might have incorrectly classified as Enterprise AI.

    Returns:
        True if deal is crypto-related, False otherwise
    """
    text_lower = article_text.lower()
    name_lower = deal.startup_name.lower() if deal.startup_name else ""

    # Check 0: Known crypto company (strongest signal)
    if any(known in name_lower for known in KNOWN_CRYPTO_COMPANIES):
        logger.debug(f"Crypto detected (known company): {deal.startup_name}")
        return True

    # Check 1a: Company name contains explicit crypto patterns (substring match OK)
    if any(pattern in name_lower for pattern in CRYPTO_COMPANY_PATTERNS):
        logger.debug(f"Crypto detected by company name pattern: {deal.startup_name}")
        return True

    # Check 1b: Company name contains strict patterns (word boundary required)
    # FIX 2026-01: Use regex for patterns that could cause false positives
    for pattern in CRYPTO_COMPANY_PATTERNS_STRICT:
        if re.search(pattern, name_lower):
            logger.debug(f"Crypto detected by strict company pattern: {deal.startup_name}")
            return True

    # Check 1c: Description contains crypto signals (lower threshold - descriptions are curated)
    desc_lower = deal.startup_description.lower() if deal.startup_description else ""
    if desc_lower:
        crypto_desc_keywords = len(CRYPTO_KEYWORDS_PATTERN.findall(desc_lower))
        if crypto_desc_keywords >= 1:
            # Only if NO AI signals in description (protect NLP tokenization companies)
            ai_desc_signals = ["nlp", "natural language", "machine learning", "neural",
                               "large language model", "llm", "text processing", "ai model"]
            if not any(sig in desc_lower for sig in ai_desc_signals):
                logger.debug(f"Crypto detected by description keywords ({crypto_desc_keywords}): {deal.startup_name}")
                return True

    # Check 2: Article is dominated by crypto keywords
    # IMPROVED (2026-01): Increased threshold from 3 to 4 keywords, and added
    # protection for legitimate AI companies that work with blockchain
    # (e.g., "blockchain AI infrastructure" should NOT be flagged)
    # FIX (2026-01): Use pre-compiled regex for efficiency (was 35+ string searches)
    crypto_count = len(CRYPTO_KEYWORDS_PATTERN.findall(text_lower))

    # Check for AI signals that protect against false crypto classification
    ai_signals = ["ai infrastructure", "machine learning", "llm", "large language model",
                  "ai platform", "enterprise ai", "mlops", "ai agent", "ai model"]
    has_ai_signals = any(sig in text_lower for sig in ai_signals)

    # Higher threshold (4+) if no AI signals, or very high threshold (6+) if AI signals present
    threshold = 6 if has_ai_signals else 4
    if crypto_count >= threshold:
        logger.debug(f"Crypto detected by keyword density ({crypto_count}/{threshold}): {deal.startup_name}")
        return True

    # Check 2b: Article contains crypto self-labeling phrases
    # These are phrases where the company/article explicitly identifies as crypto
    CRYPTO_SELF_LABELS = [
        "tokenization startup", "tokenization company", "tokenization platform",
        "tokenization protocol", "tokenization project",
        "blockchain startup", "blockchain company", "blockchain platform",
        "crypto startup", "crypto company", "crypto platform", "crypto project",
        "defi startup", "defi company", "defi platform", "defi protocol",
        "nft marketplace", "nft platform", "nft startup",
        "web3 startup", "web3 company", "web3 platform",
    ]
    if any(label in text_lower for label in CRYPTO_SELF_LABELS):
        # Protect NLP tokenization companies from false positives
        if not has_ai_signals:
            logger.debug(f"Crypto detected by self-label phrase: {deal.startup_name}")
            return True

    # Check 3: Lead investor has "crypto" in name (e.g., "a16z crypto")
    if deal.tracked_fund_name and "crypto" in deal.tracked_fund_name.lower():
        logger.debug(f"Crypto detected by investor name: {deal.tracked_fund_name}")
        return True

    # Check 4: Check lead investors in the extraction
    for investor in deal.lead_investors:
        if "crypto" in investor.name.lower():
            logger.debug(f"Crypto detected by lead investor: {investor.name}")
            return True

    return False


def _is_consumer_fintech_deal(deal: DealExtraction, article_text: str) -> bool:
    """
    Detect if deal is consumer fintech (should be NOT_AI, not Enterprise AI).

    Consumer fintech includes:
    - Neobrokers (Trade Republic, Robinhood, eToro)
    - Neobanks (N26, Revolut, Chime)
    - Consumer payment apps (Venmo, Cash App)
    - BNPL (Klarna, Affirm)

    These are NOT AI companies - they're traditional fintech serving consumers.
    The LLM sometimes confuses "Finance" in vertical_saas with consumer fintech.

    Returns:
        True if deal is consumer fintech, False otherwise
    """
    name_lower = deal.startup_name.lower() if deal.startup_name else ""
    text_lower = article_text.lower()
    desc_lower = deal.startup_description.lower() if deal.startup_description else ""

    # Check 1: Known consumer fintech company (strongest signal)
    for known in KNOWN_CONSUMER_FINTECH:
        if known in name_lower:
            logger.debug(f"Consumer fintech detected (known company): {deal.startup_name}")
            return True

    # Check 2: Company name contains fintech patterns AND article has fintech keywords
    # Need both to avoid false positives (e.g., "Capital" in AI company names)
    has_fintech_name = any(pattern in name_lower for pattern in CONSUMER_FINTECH_COMPANY_PATTERNS)
    fintech_keyword_count = sum(1 for kw in CONSUMER_FINTECH_KEYWORDS if kw in text_lower)

    if has_fintech_name and fintech_keyword_count >= 2:
        # Additional check: must NOT have strong AI signals
        ai_signals = ["artificial intelligence", "machine learning", "ai-powered", "ai platform",
                      "llm", "large language model", "generative ai", "neural network"]
        has_ai_signals = any(sig in text_lower for sig in ai_signals)

        if not has_ai_signals:
            logger.debug(
                f"Consumer fintech detected (name + {fintech_keyword_count} keywords): "
                f"{deal.startup_name}"
            )
            return True

    # Check 3: High density of fintech keywords (3+) without AI signals
    if fintech_keyword_count >= 3:
        ai_signals = ["artificial intelligence", "machine learning", "ai-powered", "ai platform",
                      "llm", "large language model", "generative ai", "neural network"]
        has_ai_signals = any(sig in text_lower for sig in ai_signals)

        if not has_ai_signals:
            logger.debug(
                f"Consumer fintech detected by keyword density ({fintech_keyword_count}): "
                f"{deal.startup_name}"
            )
            return True

    # Check 4: Description explicitly indicates consumer fintech
    fintech_desc_signals = [
        "neobroker", "neobank", "trading app", "stock trading",
        "retail banking", "consumer banking", "payment app",
        "buy now pay later", "bnpl", "challenger bank",
    ]
    if any(signal in desc_lower for signal in fintech_desc_signals):
        logger.debug(f"Consumer fintech detected by description: {deal.startup_name}")
        return True

    return False


# =============================================================================
# NON-AI CATEGORY VALIDATION (Grounded classification)
# =============================================================================

# Keywords for each non-AI category - only assign category if keywords appear in text
NON_AI_CATEGORY_KEYWORDS = {
    EnterpriseCategory.CRYPTO: [
        "crypto", "cryptocurrency", "blockchain", "web3", "defi",
        "nft", "token", "stablecoin", "bitcoin", "ethereum", "solana"
    ],
    EnterpriseCategory.FINTECH: [
        "neobank", "payments", "lending", "banking", "trading",
        "fintech", "payment", "credit", "loan", "brokerage", "neobroker"
    ],
    EnterpriseCategory.HEALTHCARE: [
        "biotech", "pharmaceutical", "pharma", "clinical", "medical",
        "drug", "therapy", "patient", "healthcare", "health care"
    ],
    EnterpriseCategory.HARDWARE: [
        "semiconductor", "chip", "hardware", "device", "manufacturing",
        "sensor", "robotics", "physical", "electronics"
    ],
    EnterpriseCategory.SAAS: [
        "saas", "software as a service", "subscription software",
        "cloud software", "enterprise software"
    ],
}


def _validate_non_ai_category_in_text(
    deal: DealExtraction,
    article_text: str,
) -> DealExtraction:
    """
    Validate that non-AI category keywords appear in article text.

    This ensures grounded classification - LLM can't assign crypto/fintech/etc
    categories unless those keywords actually appear in the article.

    If category keywords don't appear, downgrade to "other".
    """
    # Only validate non-AI categories
    if deal.is_ai_deal or deal.is_enterprise_ai:
        return deal

    # Skip if already OTHER or legacy NOT_AI
    if deal.enterprise_category in (EnterpriseCategory.OTHER, EnterpriseCategory.NOT_AI):
        return deal

    # Check if category keywords appear in text
    keywords = NON_AI_CATEGORY_KEYWORDS.get(deal.enterprise_category, [])
    if not keywords:
        return deal  # No keywords defined for this category

    text_lower = article_text.lower()
    desc_lower = deal.startup_description.lower() if deal.startup_description else ""

    # Check both article text and description
    has_keyword = any(kw in text_lower or kw in desc_lower for kw in keywords)

    if not has_keyword:
        old_category = deal.enterprise_category.value
        logger.info(
            f"Downgrading {deal.startup_name} from {old_category} to OTHER: "
            f"no {old_category} keywords found in text"
        )
        increment_extraction_stat("non_ai_category_downgraded")
        deal.enterprise_category = EnterpriseCategory.OTHER

    return deal


# =============================================================================
# AMOUNT VALIDATION (Detect market size confusion)
# =============================================================================

# Typical check sizes by round (upper bounds for flagging)
# UPDATED (2026-01): Increased thresholds to reflect 2025-2026 AI market conditions
# AI deals are running larger than historical averages due to infrastructure costs
ROUND_AMOUNT_THRESHOLDS = {
    RoundType.PRE_SEED: 10_000_000,      # $10M max for AI pre-seed (was $5M)
    RoundType.SEED: 50_000_000,          # $50M max for AI seed (was $25M)
    RoundType.SERIES_A: 200_000_000,     # $200M max for AI Series A (was $100M)
    RoundType.SERIES_B: 500_000_000,     # $500M max for AI Series B (was $250M)
    RoundType.SERIES_C: 1_000_000_000,   # $1B max for AI Series C (was $500M)
}

# Patterns that indicate market size / TAM (not funding amount)
MARKET_SIZE_PATTERNS = [
    r'\$\s*(\d+(?:\.\d+)?)\s*(?:billion|B)\s*(?:market|industry|opportunity|TAM|sector)',
    r'(?:market|industry|opportunity|TAM|sector)\s*(?:size|worth|valued at|of)\s*\$\s*(\d+(?:\.\d+)?)\s*(?:billion|B)',
    r'(?:spend|spending|spent)\s*(?:over|approximately|about|around)?\s*\$\s*(\d+(?:\.\d+)?)\s*(?:billion|B)',
    r'\$\s*(\d+(?:\.\d+)?)\s*(?:billion|B)\s*(?:annually|per year|a year)',
]


def _parse_amount_to_usd(amount_str: Optional[str]) -> Optional[int]:
    """Parse amount string to USD integer.

    Handles formats like: $50M, $50 million, 50M, $1.5B, etc.
    Returns None if parsing fails.
    """
    if not amount_str:
        return None

    # Clean the string
    amount_clean = amount_str.lower().strip()

    # Remove common prefixes
    for prefix in ['approximately', 'around', 'about', 'up to', 'over', '~', '$']:
        amount_clean = amount_clean.replace(prefix, '').strip()

    # Extract number and multiplier
    match = re.search(r'([\d,.]+)\s*(billion|b|million|m|mn)?', amount_clean)
    if not match:
        return None

    try:
        number = float(match.group(1).replace(',', ''))
        multiplier = match.group(2) or ''

        if multiplier in ('billion', 'b'):
            return int(number * 1_000_000_000)
        elif multiplier in ('million', 'm', 'mn'):
            return int(number * 1_000_000)
        elif number < 1000:
            # Small number without multiplier = assume millions (funding context)
            return int(number * 1_000_000)
        else:
            return int(number)
    except (ValueError, TypeError):
        return None


def _check_market_size_confusion(
    deal: DealExtraction,
    article_text: str,
) -> tuple[bool, Optional[str]]:
    """
    Check if extracted amount might be confused with market size.

    This catches cases like the Agency bug where:
    - Article mentions "$150 billion market size"
    - LLM extracts "$150 million" as funding amount

    Returns:
        (needs_review, reason): True if suspicious, with explanation
    """
    amount_usd = _parse_amount_to_usd(deal.amount)
    if not amount_usd:
        return False, None

    text_lower = article_text.lower()

    # Check 1: Round type vs typical check size
    threshold = ROUND_AMOUNT_THRESHOLDS.get(deal.round_label)
    if threshold and amount_usd > threshold:
        # Check if article mentions similar billion figure that could be confused
        for pattern in MARKET_SIZE_PATTERNS:
            matches = re.findall(pattern, text_lower)
            for match in matches:
                try:
                    billion_value = float(match)
                    # If extracted amount (in millions) matches a market size (in billions)
                    # e.g., extracted $150M, article has $150B market size
                    if abs(amount_usd / 1_000_000 - billion_value) < 1:  # Within $1M
                        return True, (
                            f"Amount ${amount_usd/1_000_000:.0f}M may be confused with "
                            f"${billion_value}B market size mentioned in article"
                        )
                except (ValueError, TypeError):
                    continue

        # Generic warning for unusually large amounts
        round_name = deal.round_label.value.replace('_', ' ').title()
        return True, (
            f"{round_name} of ${amount_usd/1_000_000:.0f}M exceeds typical threshold "
            f"(${threshold/1_000_000:.0f}M) - verify against source"
        )

    return False, None


def _validate_deal_amount(deal: DealExtraction, article_text: str) -> DealExtraction:
    """
    Post-process validation for deal amounts.

    Checks for:
    1. Unusually large amounts for the round type
    2. Possible confusion with market size figures

    Sets amount_needs_review=True if suspicious.
    """
    needs_review, reason = _check_market_size_confusion(deal, article_text)

    if needs_review:
        logger.warning(
            f"AMOUNT REVIEW NEEDED for {deal.startup_name}: {reason}"
        )
        deal.amount_needs_review = True
        deal.amount_review_reason = reason

    return deal


# =============================================================================
# RELATIVE DATE VALIDATION (FIX 2026-01)
# =============================================================================
# Patterns to detect relative date phrases in article text
# These indicate the article mentions a date relative to publication time
RELATIVE_DATE_PATTERNS = [
    (re.compile(r"(\d+)\s*months?\s*ago", re.IGNORECASE), "months"),
    (re.compile(r"(\d+)\s*years?\s*ago", re.IGNORECASE), "years"),
    (re.compile(r"(\d+)\s*weeks?\s*ago", re.IGNORECASE), "weeks"),
    (re.compile(r"last\s+year", re.IGNORECASE), "last_year"),
    (re.compile(r"earlier\s+this\s+year", re.IGNORECASE), "earlier_this_year"),
    (re.compile(r"late\s+last\s+year", re.IGNORECASE), "late_last_year"),
    (re.compile(r"early\s+last\s+year", re.IGNORECASE), "early_last_year"),
]


def _parse_relative_date(match: re.Match, unit: str, reference_date: date) -> Optional[date]:
    """
    Parse a relative date phrase into an actual date.

    Args:
        match: The regex match object
        unit: The type of relative date ("months", "years", "weeks", "last_year", etc.)
        reference_date: The date to calculate relative to (article publication or today)

    Returns:
        The calculated date, or None if unable to parse
    """
    try:
        if unit == "months":
            months_ago = int(match.group(1))
            return reference_date - relativedelta(months=months_ago)
        elif unit == "years":
            years_ago = int(match.group(1))
            return reference_date - relativedelta(years=years_ago)
        elif unit == "weeks":
            weeks_ago = int(match.group(1))
            return reference_date - timedelta(weeks=weeks_ago)
        elif unit == "last_year":
            return reference_date - relativedelta(years=1)
        elif unit == "earlier_this_year":
            # Assume mid-year (around 6 months ago relative to Dec/Jan)
            return date(reference_date.year, 6, 1)
        elif unit == "late_last_year":
            return date(reference_date.year - 1, 11, 1)
        elif unit == "early_last_year":
            return date(reference_date.year - 1, 2, 1)
    except (ValueError, OverflowError) as e:
        # FIX (2026-01): Upgrade from DEBUG to WARNING for visibility in monitoring
        # These failures indicate potential bugs in date parsing logic
        logger.warning(f"Failed to parse relative date: {e}")
    return None


def _validate_relative_date_extraction(
    deal: DealExtraction,
    article_text: str,
    article_published_date: Optional[date] = None,
) -> DealExtraction:
    """
    Detect when LLM extracted today's date but article mentions older relative dates.

    FIX (2026-01): When Claude sees "6 months ago" but doesn't know the current date,
    it might extract the wrong date. This post-processing step detects this case and
    attempts to correct the date.

    Args:
        deal: The extracted deal
        article_text: Original article text
        article_published_date: When the article was published (used as reference)

    Returns:
        Modified deal with corrected round_date if a relative date was detected
    """
    if not deal.round_date:
        return deal

    today = date.today()
    days_from_today = abs((deal.round_date - today).days)

    # Only check if date is suspiciously close to today (within 7 days)
    # If the LLM extracted a date from months/years ago, it's probably correct
    if days_from_today > 7:
        return deal

    # Use article publication date as reference, fallback to today
    reference_date = article_published_date or today

    # Check for relative date patterns that indicate older events
    text_lower = article_text.lower()
    for pattern, unit in RELATIVE_DATE_PATTERNS:
        match = pattern.search(text_lower)
        if match:
            # Found "X months ago" or similar - this is suspicious if date is near today
            matched_phrase = match.group(0)

            # Calculate what the date should be
            corrected_date = _parse_relative_date(match, unit, reference_date)

            if corrected_date and corrected_date != deal.round_date:
                # Check if the corrected date is significantly different (>30 days)
                days_diff = abs((corrected_date - deal.round_date).days)
                if days_diff > 30:
                    logger.warning(
                        f"RELATIVE DATE FIX for {deal.startup_name}: "
                        f"Article says '{matched_phrase}' but extracted {deal.round_date}. "
                        f"Correcting to {corrected_date} (reference: {reference_date})"
                    )
                    increment_extraction_stat("relative_date_corrected")
                    deal.round_date = corrected_date
                    break  # Only correct once

    return deal


def _is_consumer_ai_deal(deal: DealExtraction, article_text: str) -> bool:
    """
    Detect if deal is consumer AI (should NOT be Enterprise AI).

    Called as post-extraction validation to catch consumer AI deals that the LLM
    might have incorrectly classified as Enterprise AI.

    Consumer AI includes:
    - Dating/social apps with AI matching
    - Photo editors with AI filters
    - Gaming with AI NPCs
    - Personal assistant apps
    - B2C entertainment apps

    Returns:
        True if deal is consumer AI, False otherwise
    """
    # If already marked as consumer AI category, don't need to detect
    if deal.enterprise_category in (
        EnterpriseCategory.CONSUMER_AI,
        EnterpriseCategory.GAMING_AI,
        EnterpriseCategory.SOCIAL_AI,
    ):
        return True

    text_lower = article_text.lower()
    name_lower = deal.startup_name.lower() if deal.startup_name else ""
    desc_lower = deal.startup_description.lower() if deal.startup_description else ""

    # Enterprise signals that protect against false consumer classification
    # FIX 2026-01: Added protection for B2B companies with consumer-sounding names
    # e.g., "StreamData" (data streaming platform), "ChatBot AI" (enterprise chatbot)
    enterprise_signals = ["b2b", "enterprise", "business", "saas", "platform", "api", "infrastructure"]
    has_enterprise_signals = any(sig in desc_lower for sig in enterprise_signals)

    # Check 1: Company name contains consumer AI patterns
    # FIX 2026-01: Only trigger if NO enterprise signals in description
    if any(pattern in name_lower for pattern in CONSUMER_AI_COMPANY_PATTERNS):
        if has_enterprise_signals:
            logger.debug(f"Skipping consumer name match for {deal.startup_name} - has enterprise signals")
        else:
            logger.debug(f"Consumer AI detected by company name: {deal.startup_name}")
            return True

    # Check 2: Article is dominated by consumer AI keywords (3+ = likely consumer)
    consumer_count = sum(1 for kw in CONSUMER_AI_KEYWORDS if kw in text_lower)
    if consumer_count >= 3:
        logger.debug(f"Consumer AI detected by keyword density ({consumer_count}): {deal.startup_name}")
        return True

    # Check 3: Description explicitly mentions consumer focus
    consumer_desc_signals = [
        "consumer", "b2c", "dating", "social", "gaming", "game",
        "personal use", "individual users", "app store", "mobile app",
    ]
    if any(signal in desc_lower for signal in consumer_desc_signals):
        # Only trigger if NOT also mentioning B2B/enterprise signals
        # FIX 2026-01: Reuse has_enterprise_signals from Check 1 for consistency
        if not has_enterprise_signals:
            logger.debug(f"Consumer AI detected by description: {deal.startup_name}")
            return True

    return False


# Patterns that indicate text is an article title, not a company name
# FIX #34: Made patterns more specific to avoid rejecting valid company names
ARTICLE_TITLE_PATTERNS = [
    # Possessive patterns - "'s Challenge", "'s Next Act", "'s Biggest Problem"
    r"'s\s+(problem|challenge|crisis|issue|opportunity|dilemma|future|guide|next|biggest|latest|new|great)",
    # FIX #34: Only reject question words, not "the"/"a" which appear in valid names
    # Valid: "The Trade Desk", "A Cloud Guru" - these should NOT be rejected
    r"^(how|why|what|when|where|who|which|can|will|should|is|are|does|do)\s+",
    # "the" + article-specific words only (not generic "the company")
    r"^the\s+(problem|future|guide|art|science|rise|fall|state|end|best|worst|top|only|real|truth|power|secret|key|way|case|death|birth|age|era|dawn|race|battle|fight|war|promise|perils|myth|reality|answer|question)\b",
    # "a" + article-specific words only (not generic "a startup")
    r"^a\s+(guide|look|deep|new|better|simple|complete|brief|quick|closer|comprehensive|radical|bold|different|revolutionary)\b",
    # Ending patterns
    r"(problem|challenge|crisis|issue|opportunity|future|guide|revolution|transformation|disruption|solution|solved)$",
    # Listicle patterns
    r"^\d+\s+(ways?|tips|secrets|lessons|habits|mistakes|things|reasons|steps|strategies|trends|predictions|companies|startups)",
    # Action verb patterns (suggesting article themes)
    r"^(building|creating|scaling|growing|launching|transforming|disrupting|revolutionizing|rethinking|reimagining|solving|tackling|fixing|navigating)\s+",
    # Compound title patterns - "Problem-Solving AI", "Next-Gen Platform"
    r"(problem-solving|next-gen|cutting-edge|state-of-the-art|ground-breaking|game-changing)",
    # Editorial/opinion patterns
    r"^(why|how)\s+.*\s+(is|are|will|can|should|must|might)\b",
    # Colon/dash patterns suggesting subtitles
    r":\s+.{10,}$",  # Colon followed by substantial subtitle
]


def _looks_like_article_title(text: str) -> bool:
    """
    Check if text looks like an article title, not a company name.

    Used to catch false positives where LLM extracts headline as company name.
    E.g., "America's Construction Problem" should be rejected.
    """
    if not text or len(text) < 3:
        return True

    text_lower = text.lower().strip()

    # Check regex patterns
    for pattern in ARTICLE_TITLE_PATTERNS:
        if re.search(pattern, text_lower):
            return True

    # Company names are usually 1-3 words, titles are often longer
    # TIGHTENED (2026-01): Reduced from >5 to >4 words
    word_count = len(text.split())
    if word_count > 4:
        return True

    # Check for known invalid patterns - expanded list
    invalid_substrings = [
        # Core article words
        "problem", "challenge", "crisis", "issue", "opportunity", "dilemma",
        # Possessive article patterns
        "'s construction", "'s future", "'s challenge", "'s problem", "'s biggest",
        "'s latest", "'s next act", "'s new", "'s great",
        # Editorial phrases
        "the rise of", "the fall of", "the future of", "the state of",
        "the end of", "the death of", "the birth of", "the age of",
        # Transformation language
        "revolution", "transformation", "disruption", "reinvention",
        # How-to patterns
        "how to", "why you", "what you", "when to",
    ]
    if any(sub in text_lower for sub in invalid_substrings):
        return True

    return False


def _is_background_mention(
    company_name: str,
    article_text: str,
    is_external_source: bool = False,
) -> tuple[bool, str]:
    """
    Check if extracted company is likely a background mention, not the article's subject.

    Only call this when is_new_announcement=True (already checked at call site).

    Args:
        company_name: The extracted company name
        article_text: Full article text
        is_external_source: If True, use lenient checking (for external-only funds)

    Returns:
        (is_background, reason): True if background mention, with explanation

    Heuristics:
    - If company name not in first 500 chars (headline/lede), likely background
    - 500 chars accounts for wire service headers, bylines, dates
    - Exception: Very short articles where everything is "first 500 chars"
    - FIX (2026-01): Skip this check for external sources (Thrive, Benchmark, etc.)
      as they often have less structured content
    """
    if not company_name or not article_text:
        return False, ""

    # FIX (2026-01): Skip background check for external sources
    # These often have fund name in headline but company name later in article
    if is_external_source:
        logger.debug(f"Skipping background check for external source: {company_name}")
        return False, ""

    # TIGHTENED (2026-01): Use proportional headline check instead of fixed threshold
    # For short articles (<1000 chars), check first 40% of article
    # For medium articles (1000-2000), check first 500 chars
    # For long articles (>2000), check first 500 chars
    article_length = len(article_text)

    # Very short articles (<500 chars) - company should be obvious, skip check
    if article_length < 500:
        return False, ""

    # Determine headline area size based on article length
    if article_length < 1000:
        # Short articles: check first 40% (at least 200 chars)
        headline_size = max(200, int(article_length * 0.4))
    else:
        # Medium/long articles: check first 500 chars or first line
        first_line_end = article_text.find('\n')
        if first_line_end == -1:
            headline_size = 500
        else:
            headline_size = max(first_line_end, 500)

    headline_area = article_text[:headline_size].lower()

    company_lower = company_name.lower().strip()

    # Check if company name appears in headline/lede
    if company_lower not in headline_area:
        # Check for partial match (e.g., "Traversal" in "Traversal AI")
        company_words = company_lower.split()
        main_word = company_words[0] if company_words else company_lower
        if len(main_word) >= 4 and main_word not in headline_area:  # FIX: Require 4+ chars (was 2)
            return True, f"Company '{company_name}' not in article headline - likely background mention"

    return False, ""


# =============================================================================
# ARTICLE TRUNCATION (Token Optimization)
# =============================================================================

MAX_ARTICLE_CHARS = 4000  # ~1,200 tokens (3.3 chars/token avg)


def truncate_article_smart(text: str, max_chars: int = MAX_ARTICLE_CHARS) -> str:
    """
    Truncate article while preserving funding-relevant content.

    Strategy:
    - Keep first 70% of max_chars (funding info usually in first paragraphs)
    - Scan remaining text for $ amounts, "led by", "series" mentions
    - Append relevant snippets with context

    This reduces token usage by 50-60% while maintaining extraction quality.
    """
    if not text or len(text) <= max_chars:
        return text

    # Keep first 70% of allowed length
    first_portion = int(max_chars * 0.7)
    result = text[:first_portion]

    # Scan remaining text for key funding mentions
    remaining = text[first_portion:]
    funding_patterns = [
        r'\$[\d,]+\s*(?:million|billion|M|B)?',  # $50 million, $50M
        r'(?:raised|raises|securing|secures)\s+\$[\d,]+',  # raises $50M
        r'series\s+[a-z]',  # Series A, B, C
        r'(?:led|co-led)\s+by',  # led by Sequoia
        r'seed\s+(?:round|funding)',  # seed round
        r'pre-seed',  # pre-seed
    ]

    # Find and append relevant snippets
    added_snippets = set()  # Avoid duplicates
    for pattern in funding_patterns:
        for match in re.finditer(pattern, remaining, re.IGNORECASE):
            # Get surrounding context (100 chars before/after)
            start = max(0, match.start() - 100)
            end = min(len(remaining), match.end() + 100)
            snippet = remaining[start:end].strip()

            # Skip if we'd exceed limit or already added similar snippet
            snippet_key = snippet[:50]  # Use first 50 chars as key
            if snippet_key in added_snippets:
                continue
            if len(result) + len(snippet) + 10 > max_chars:
                break

            added_snippets.add(snippet_key)
            result += f"\n...\n{snippet}"

    if len(text) > len(result):
        result += "\n[TRUNCATED]"

    return result


def _sanitize_prompt_value(value: str, max_length: int = 500) -> str:
    """Sanitize a value for inclusion in a prompt to prevent injection attacks.

    FIX (2026-01): Prevents prompt injection via unescaped fund data.
    Strips control characters, limits length, and escapes special formatting.

    Args:
        value: The string value to sanitize
        max_length: Maximum length to truncate to (default 500 chars)

    Returns:
        Sanitized string safe for inclusion in prompts
    """
    if not value:
        return ""

    # Remove control characters (keep newlines and tabs for readability)
    sanitized = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', value)

    # Escape sequences that could be interpreted as prompt instructions
    # Replace triple backticks (code blocks), triple dashes (section breaks)
    sanitized = sanitized.replace('```', '`\u200b`\u200b`')  # Zero-width space break
    sanitized = sanitized.replace('---', '-\u200b-\u200b-')

    # Escape potential role/instruction markers (defense in depth)
    # Note: Fund config is from hardcoded FUND_REGISTRY, so this is precautionary
    # Use non-raw string for replacement so \u200b is interpreted as Unicode
    sanitized = re.sub(r'(?i)(SYSTEM|USER|ASSISTANT):', '\\1\u200b:', sanitized)
    sanitized = re.sub(r'(?i)<(/?)(instructions|system|prompt)', '<\\1\u200b\\2', sanitized)

    # Truncate to max length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "..."

    return sanitized.strip()


def build_extraction_prompt(
    article_text: str,
    fund_context: Optional[FundConfig] = None,
    source_url: str = "",
    article_published_date: Optional[date] = None,
) -> str:
    """Build the extraction prompt with optional fund-specific context and date reference."""

    # OPTIMIZATION: Truncate long articles to reduce token usage
    truncated_text = truncate_article_smart(article_text)

    # FIX (2026-01): Include date context for relative date extraction
    today = date.today()
    date_context = f"""
REFERENCE DATES (for calculating relative dates like "6 months ago"):
- Today's date: {today.strftime('%Y-%m-%d')}
- Article published: {article_published_date.strftime('%Y-%m-%d') if article_published_date else 'unknown'}
- Use article publication date as reference for relative dates in the article text
- Example: If article says "6 months ago" and was published on {today.strftime('%Y-%m-%d')}, calculate ~{(today - timedelta(days=180)).strftime('%B %Y')}
"""

    prompt = f"""Analyze this article and extract all funding deal information.

SOURCE URL: {source_url}
{date_context}
ARTICLE TEXT:
---
{truncated_text}
---

"""

    if fund_context:
        # FIX (2026-01): Sanitize fund data to prevent prompt injection
        # User-configurable fields (extraction_notes, negative_keywords, partner_names)
        # could contain malicious content that alters the prompt's behavior
        safe_name = _sanitize_prompt_value(fund_context.name, max_length=100)
        safe_notes = _sanitize_prompt_value(fund_context.extraction_notes or "", max_length=500)

        prompt += f"""
FUND-SPECIFIC CONTEXT:
This article was found via {safe_name}'s news feed.
Extraction notes: {safe_notes}
WARNING: This does NOT mean {safe_name} invested. Only include them if EXPLICITLY mentioned in article.
"""
        if fund_context.negative_keywords:
            # Sanitize each keyword individually
            safe_keywords = [_sanitize_prompt_value(kw, max_length=50) for kw in fund_context.negative_keywords]
            prompt += f"EXCLUDE if these appear: {', '.join(safe_keywords)}\n"
        if fund_context.partner_names:
            # Sanitize each partner name individually
            safe_partners = [_sanitize_prompt_value(p, max_length=100) for p in fund_context.partner_names]
            prompt += f"Known partners: {', '.join(safe_partners)}\n"

    prompt += """
Extract the deal(s) with full chain-of-thought reasoning.
Pay special attention to whether our tracked funds LED vs merely PARTICIPATED."""

    return prompt


async def extract_deal(
    article_text: str,
    source_url: str = "",
    source_name: str = "unknown",
    fund_context: Optional[FundConfig] = None,
    skip_funding_check: bool = False,
    skip_dedup_check: bool = False,
    is_external_source: bool = False,
    article_published_date: Optional[date] = None,
) -> Optional[DealExtraction]:
    """
    Extract a single funding deal from article text.

    OPTIMIZED:
    - Uses prompt caching for system message (20% token savings)
    - Early exit for non-funding content
    - Content hash deduplication to avoid duplicate API calls

    Args:
        article_text: The article content to analyze
        source_url: URL of the source article
        source_name: Source identifier for token logging (e.g., "brave_search", "a16z")
        fund_context: Optional fund-specific extraction context
        skip_funding_check: If True, skip the funding keyword check
        skip_dedup_check: If True, skip the duplicate content check
        is_external_source: If True, use lenient processing for external-only funds
        article_published_date: Publication date of the article (for relative date extraction)

    Returns:
        DealExtraction with structured deal information, or None if duplicate
    """
    # Auto-detect external source if not explicitly set
    # Check fund slug (for external-only funds like thrive, benchmark)
    if not is_external_source and source_name in EXTERNAL_ONLY_FUNDS:
        is_external_source = True
    # Check source name (for external sources like google_news, brave_search)
    if not is_external_source and source_name in EXTERNAL_SOURCE_NAMES:
        is_external_source = True
    # Check URL patterns (fallback for external source detection)
    if not is_external_source and ('news.google' in source_url or 'brave' in source_url.lower()):
        is_external_source = True
    # Content hash dedup: Skip if we've already processed identical content in this run
    # Cache is cleared ONCE at job start in scheduler/jobs.py for cross-source dedup
    if not skip_dedup_check and await _is_duplicate_content(article_text):
        logger.info(f"Skipping duplicate content for: {source_url}")
        return None

    # Early exit: check if text is likely about funding
    if not skip_funding_check and not is_likely_funding_content(article_text):
        # Return low-confidence empty extraction
        from .schemas import ChainOfThought, InvestorMention
        return DealExtraction(
            startup_name="Unknown",
            startup_description=None,
            round_label=RoundType.UNKNOWN,
            amount=None,
            valuation=None,
            round_date=None,
            lead_investors=[],
            participating_investors=[],
            tracked_fund_is_lead=False,
            tracked_fund_name=None,
            tracked_fund_role=None,
            tracked_fund_partner=None,
            enterprise_category=EnterpriseCategory.NOT_AI,
            is_enterprise_ai=False,
            is_ai_deal=False,
            verification_snippet=None,
            # FIX (2026-01): Differentiate early exit scores (was 0.1)
            # No funding keywords = lowest confidence (0.05)
            confidence_score=0.05,
            # FIX: Initialize extraction_confidence and penalty_breakdown for consistency
            extraction_confidence=0.05,
            penalty_breakdown={},
            # NEW: Mark as not a new announcement
            is_new_announcement=False,
            announcement_evidence=None,
            announcement_rejection_reason="No funding keywords detected - not a funding article",
            reasoning=ChainOfThought(
                final_reasoning="Skipped extraction - no funding content detected"
            )
        )

    # PRE-CHECK: Skip crypto-heavy articles before calling Claude (saves tokens)
    # Now also checks URL patterns for crypto websites
    if not skip_funding_check and is_likely_crypto_article(article_text, source_url):
        from .schemas import ChainOfThought
        return DealExtraction(
            startup_name="Unknown",
            startup_description=None,
            round_label=RoundType.UNKNOWN,
            amount=None,
            valuation=None,
            round_date=None,
            lead_investors=[],
            participating_investors=[],
            tracked_fund_is_lead=False,
            tracked_fund_name=None,
            tracked_fund_role=None,
            tracked_fund_partner=None,
            enterprise_category=EnterpriseCategory.NOT_AI,
            is_enterprise_ai=False,
            is_ai_deal=False,
            verification_snippet=None,
            # FIX (2026-01): Differentiate early exit scores (was 0.1)
            # Crypto detection = higher than no-keywords (0.15) since article is funding-related
            confidence_score=0.15,
            # FIX: Initialize extraction_confidence and penalty_breakdown for consistency
            extraction_confidence=0.15,
            penalty_breakdown={},
            is_new_announcement=False,
            announcement_evidence=None,
            announcement_rejection_reason="Crypto/blockchain article - not an AI company",
            reasoning=ChainOfThought(
                final_reasoning="Skipped extraction - crypto article detected"
            )
        )

    prompt = build_extraction_prompt(article_text, fund_context, source_url, article_published_date)

    # Use pre-built cached system message (optimization: created once at module load)
    # FIX: Add retry logic and error handling for API calls
    last_error = None
    article_len = len(article_text)

    for attempt in range(MAX_RETRIES + 1):
        try:
            # Use create_with_completion to get both parsed model AND raw response with usage
            response, completion = client.messages.create_with_completion(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                temperature=settings.llm_temperature,
                system=CACHED_SYSTEM_MESSAGE,
                messages=[{"role": "user", "content": prompt}],
                response_model=DealExtraction,
            )

            # Log token usage from the completion
            # FIX (2026-01): Changed from logger.info to logger.debug (reduce log spam)
            logger.debug(f"[TOKEN DEBUG] completion type={type(completion)}, has_usage={hasattr(completion, 'usage') if completion else 'None'}")
            if completion and hasattr(completion, 'usage'):
                usage = completion.usage
                # Extract cache tokens if available (Anthropic-specific)
                cache_read = getattr(usage, 'cache_read_input_tokens', 0) or 0
                cache_write = getattr(usage, 'cache_creation_input_tokens', 0) or 0
                logger.debug(f"Claude call tokens: in={usage.input_tokens}, out={usage.output_tokens}")
                try:
                    await log_token_usage(
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        source_name=source_name,
                        article_url=source_url,
                        model=settings.llm_model,
                        cache_read_tokens=cache_read,
                        cache_write_tokens=cache_write
                    )
                    logger.debug(f"[TOKEN DEBUG] log_token_usage completed successfully for source={source_name}")
                except Exception as e:
                    logger.error(f"[TOKEN DEBUG] log_token_usage raised: {e}", exc_info=True)
            else:
                # Debug: log when completion doesn't have usage
                logger.warning(
                    f"No usage data in completion: type={type(completion)}, "
                    f"has_usage={hasattr(completion, 'usage') if completion else 'N/A'}"
                )

            # Post-process: validate company name is in article (prevents hallucinated companies)
            response = _validate_company_in_text(response, article_text)

            # Post-process: reject fund raises (startup = tracked VC fund)
            response = _validate_startup_not_fund(response, article_text)

            # Post-process: validate round type is valid enum value (prevents dedup bypass)
            response = _validate_round_type(response)

            # Post-process: validate confidence score is in [0, 1] range
            response = _validate_confidence_score(response)

            # Post-process: validate investors are mentioned in article (prevents hallucination)
            response = _validate_investors_in_text(response, article_text)

            # Post-process: validate founders are mentioned in article (prevents hallucination)
            response = _validate_founders_in_text(response, article_text)

            # Post-process: verify tracked fund identification
            response = _verify_tracked_fund(response, article_text)

            # Post-process: detect and reclassify crypto deals
            if response and response.is_ai_deal and _is_crypto_deal(response, article_text):
                logger.info(
                    f"Reclassifying {response.startup_name} as CRYPTO: "
                    f"crypto/blockchain detected"
                )
                increment_extraction_stat("crypto_filtered")
                response.is_enterprise_ai = False
                response.is_ai_deal = False
                response.enterprise_category = EnterpriseCategory.CRYPTO

            # Post-process: detect and reclassify consumer AI deals
            # FIX: Consumer AI should not be counted as Enterprise AI
            if response and response.is_enterprise_ai and _is_consumer_ai_deal(response, article_text):
                logger.info(
                    f"Reclassifying {response.startup_name} as Consumer AI: "
                    f"consumer/gaming/social AI detected"
                )
                increment_extraction_stat("consumer_ai_filtered")
                response.is_enterprise_ai = False
                # Keep is_ai_deal=True (it's still AI, just not enterprise)
                if response.enterprise_category not in (
                    EnterpriseCategory.CONSUMER_AI,
                    EnterpriseCategory.GAMING_AI,
                    EnterpriseCategory.SOCIAL_AI,
                ):
                    response.enterprise_category = EnterpriseCategory.CONSUMER_AI

            # Post-process: detect and reclassify consumer fintech deals
            # FIX 2026-01: Neobrokers, neobanks are NOT AI companies
            # Trade Republic, Robinhood, etc. should be FINTECH, not vertical_saas
            if response and (response.is_ai_deal or response.is_enterprise_ai) and _is_consumer_fintech_deal(response, article_text):
                logger.info(
                    f"Reclassifying {response.startup_name} as FINTECH: "
                    f"consumer fintech (neobroker/neobank) detected"
                )
                increment_extraction_stat("consumer_fintech_filtered")
                response.is_enterprise_ai = False
                response.is_ai_deal = False
                response.enterprise_category = EnterpriseCategory.FINTECH

            # Post-process: validate non-AI category is grounded in article text
            # FIX 2026-01: Only assign specific non-AI categories if keywords appear
            if response and not response.is_ai_deal and not response.is_enterprise_ai:
                response = _validate_non_ai_category_in_text(response, article_text)

            # Post-process: validate company name isn't an article title
            if response and _looks_like_article_title(response.startup_name):
                logger.warning(
                    f"Rejecting article title as company name: '{response.startup_name}' "
                    f"from {source_url}"
                )
                increment_extraction_stat("article_title_rejected")
                response.is_new_announcement = False
                response.announcement_rejection_reason = (
                    f"Company name looks like article title: {response.startup_name}"
                )

            # Post-process: check if company is background mention (not in headline)
            # FIX (2026-01): Skip for external sources to avoid false rejections
            if response and response.is_new_announcement:
                is_background, reason = _is_background_mention(
                    response.startup_name,
                    article_text,
                    is_external_source=is_external_source,
                )
                if is_background:
                    logger.warning(
                        f"Rejecting background mention: '{response.startup_name}' "
                        f"from {source_url} - {reason}"
                    )
                    increment_extraction_stat("background_mention_rejected")
                    response.is_new_announcement = False
                    response.announcement_rejection_reason = reason

            # Post-process: validate deal amount (detect market size confusion)
            # FIX (2026-01): Catches cases like $150B market size → $150M funding
            if response and response.amount:
                old_needs_review = getattr(response, 'amount_needs_review', False)
                response = _validate_deal_amount(response, article_text)
                if response.amount_needs_review and not old_needs_review:
                    increment_extraction_stat("amount_flagged_for_review")

            # Post-process: validate relative date extraction
            # FIX (2026-01): Detects when LLM extracted wrong date from "6 months ago" etc.
            if response and response.round_date:
                response = _validate_relative_date_extraction(
                    response, article_text, article_published_date
                )

            # HYBRID EXTRACTION: Re-extract with Sonnet for low-confidence results
            # Only if: enabled, confidence in range, valid deal, has quality issues
            # FIX (2026-01): Different thresholds for internal vs external sources
            hybrid_min = (
                settings.hybrid_confidence_min_external if is_external_source
                else settings.hybrid_confidence_min
            )
            if (
                response
                and settings.hybrid_extraction_enabled
                and response.is_new_announcement
                and hybrid_min <= response.confidence_score <= settings.hybrid_confidence_max
                and (response.lead_evidence_weak or len(response.founders) == 0)
            ):
                logger.info(
                    f"HYBRID: Low confidence ({response.confidence_score:.2f}) for {response.startup_name}, "
                    f"weak_evidence={response.lead_evidence_weak}, founders={len(response.founders)} - "
                    f"re-extracting with {settings.llm_model_fallback}"
                )
                sonnet_response = await _reextract_with_sonnet(
                    article_text=article_text,
                    source_url=source_url,
                    source_name=source_name,
                    prompt=prompt,
                    original_response=response,
                )
                if sonnet_response:
                    # COST TRACKING (Jan 2026): Log Sonnet hit rate to measure ROI
                    # Track: did Sonnet upgrade weak evidence to confirmed lead?
                    # If <30% hit rate, consider disabling hybrid extraction
                    sonnet_improved = (
                        not sonnet_response.lead_evidence_weak
                        or len(sonnet_response.founders) > len(response.founders)
                        or sonnet_response.confidence_score > response.confidence_score + 0.1
                    )
                    logger.info(
                        f"HYBRID_RESULT: {sonnet_response.startup_name} | "
                        f"lead_confirmed={not sonnet_response.lead_evidence_weak} | "
                        f"founders={len(sonnet_response.founders)} | "
                        f"confidence={sonnet_response.confidence_score:.2f} | "
                        f"improved={sonnet_improved}"
                    )

                    response = sonnet_response

                    # FIX (2026-01): Validation steps 1-5 are now done inside _reextract_with_sonnet()
                    # Only apply steps 6-11 here (reclassification and additional checks)

                    # 6. Crypto reclassification
                    if response.is_ai_deal and _is_crypto_deal(response, article_text):
                        logger.info(f"HYBRID: Reclassifying {response.startup_name} as CRYPTO")
                        increment_extraction_stat("crypto_filtered")
                        response.is_enterprise_ai = False
                        response.is_ai_deal = False
                        response.enterprise_category = EnterpriseCategory.CRYPTO

                    # 7. Consumer AI reclassification
                    if response.is_enterprise_ai and _is_consumer_ai_deal(response, article_text):
                        logger.info(f"HYBRID: Reclassifying {response.startup_name} as Consumer AI")
                        increment_extraction_stat("consumer_ai_filtered")
                        response.is_enterprise_ai = False
                        if response.enterprise_category not in (
                            EnterpriseCategory.CONSUMER_AI,
                            EnterpriseCategory.GAMING_AI,
                            EnterpriseCategory.SOCIAL_AI,
                        ):
                            response.enterprise_category = EnterpriseCategory.CONSUMER_AI

                    # 8. Consumer fintech reclassification
                    if (response.is_ai_deal or response.is_enterprise_ai) and _is_consumer_fintech_deal(response, article_text):
                        logger.info(f"HYBRID: Reclassifying {response.startup_name} as FINTECH")
                        increment_extraction_stat("consumer_fintech_filtered")
                        response.is_enterprise_ai = False
                        response.is_ai_deal = False
                        response.enterprise_category = EnterpriseCategory.FINTECH

                    # 8b. Non-AI category validation (grounded classification)
                    if not response.is_ai_deal and not response.is_enterprise_ai:
                        response = _validate_non_ai_category_in_text(response, article_text)

                    # 9. Article title validation
                    if _looks_like_article_title(response.startup_name):
                        logger.warning(f"HYBRID: Rejecting article title as company name: '{response.startup_name}'")
                        increment_extraction_stat("article_title_rejected")
                        response.is_new_announcement = False
                        response.announcement_rejection_reason = f"Company name looks like article title: {response.startup_name}"

                    # 10. Background mention check
                    if response.is_new_announcement:
                        is_background, reason = _is_background_mention(response.startup_name, article_text, is_external_source)
                        if is_background:
                            logger.warning(f"HYBRID: Rejecting background mention: '{response.startup_name}' - {reason}")
                            increment_extraction_stat("background_mention_rejected")
                            response.is_new_announcement = False
                            response.announcement_rejection_reason = reason

                    # 11. Amount validation
                    if response.amount:
                        old_needs_review = getattr(response, 'amount_needs_review', False)
                        response = _validate_deal_amount(response, article_text)
                        if response.amount_needs_review and not old_needs_review:
                            increment_extraction_stat("amount_flagged_for_review")

                    # 12. Relative date validation
                    if response.round_date:
                        response = _validate_relative_date_extraction(
                            response, article_text, article_published_date
                        )
                else:
                    # COST TRACKING: Sonnet re-extraction failed
                    logger.warning(
                        f"HYBRID_FAILED: {response.startup_name} | "
                        f"Sonnet returned None - keeping Haiku result"
                    )

            # FIX (2026-01): Re-extract HIGH-confidence deals with weak lead evidence
            # Problem: If confidence=0.75 but lead_evidence_weak=True, Sonnet won't re-extract
            # because 0.75 > 0.65 (hybrid_confidence_max). This misses opportunities to
            # confirm lead status with the more capable model.
            elif (
                response
                and settings.hybrid_extraction_enabled
                and response.is_new_announcement
                and response.confidence_score > settings.hybrid_confidence_max
                and response.lead_evidence_weak
                and response.tracked_fund_is_lead
            ):
                logger.info(
                    f"HYBRID: High confidence ({response.confidence_score:.2f}) but weak lead evidence "
                    f"for {response.startup_name} - re-extracting with {settings.llm_model_fallback}"
                )
                sonnet_response = await _reextract_with_sonnet(
                    article_text=article_text,
                    source_url=source_url,
                    source_name=source_name,
                    prompt=prompt,
                    original_response=response,
                )
                if sonnet_response:
                    # Track: did Sonnet upgrade weak evidence to confirmed lead?
                    sonnet_improved = not sonnet_response.lead_evidence_weak
                    logger.info(
                        f"HYBRID_RESULT_HIGH_CONF: {sonnet_response.startup_name} | "
                        f"lead_confirmed={not sonnet_response.lead_evidence_weak} | "
                        f"confidence={sonnet_response.confidence_score:.2f} | "
                        f"improved={sonnet_improved}"
                    )

                    response = sonnet_response

                    # FIX (2026-01): Validation steps 1-5 are now done inside _reextract_with_sonnet()
                    # Only apply steps 6-11 here (reclassification and additional checks)

                    # Crypto/Consumer reclassification
                    if response.is_ai_deal and _is_crypto_deal(response, article_text):
                        increment_extraction_stat("crypto_filtered")
                        response.is_enterprise_ai = False
                        response.is_ai_deal = False
                        response.enterprise_category = EnterpriseCategory.CRYPTO

                    if response.is_enterprise_ai and _is_consumer_ai_deal(response, article_text):
                        increment_extraction_stat("consumer_ai_filtered")
                        response.is_enterprise_ai = False
                        if response.enterprise_category not in (
                            EnterpriseCategory.CONSUMER_AI,
                            EnterpriseCategory.GAMING_AI,
                            EnterpriseCategory.SOCIAL_AI,
                        ):
                            response.enterprise_category = EnterpriseCategory.CONSUMER_AI

                    if (response.is_ai_deal or response.is_enterprise_ai) and _is_consumer_fintech_deal(response, article_text):
                        increment_extraction_stat("consumer_fintech_filtered")
                        response.is_enterprise_ai = False
                        response.is_ai_deal = False
                        response.enterprise_category = EnterpriseCategory.FINTECH

                    # Non-AI category validation (grounded classification)
                    if not response.is_ai_deal and not response.is_enterprise_ai:
                        response = _validate_non_ai_category_in_text(response, article_text)

                    if _looks_like_article_title(response.startup_name):
                        increment_extraction_stat("article_title_rejected")
                        response.is_new_announcement = False
                        response.announcement_rejection_reason = f"Company name looks like article title: {response.startup_name}"

                    if response.is_new_announcement:
                        is_background, reason = _is_background_mention(response.startup_name, article_text, is_external_source)
                        if is_background:
                            increment_extraction_stat("background_mention_rejected")
                            response.is_new_announcement = False
                            response.announcement_rejection_reason = reason

                    if response.amount:
                        old_needs_review = getattr(response, 'amount_needs_review', False)
                        response = _validate_deal_amount(response, article_text)
                        if response.amount_needs_review and not old_needs_review:
                            increment_extraction_stat("amount_flagged_for_review")

                    # Relative date validation
                    if response.round_date:
                        response = _validate_relative_date_extraction(
                            response, article_text, article_published_date
                        )
                else:
                    logger.warning(
                        f"HYBRID_FAILED_HIGH_CONF: {response.startup_name} | "
                        f"Sonnet returned None - keeping Haiku result with weak evidence"
                    )

            return response

        except APITimeoutError as e:
            last_error = e
            backoff = 2 ** attempt
            # FIX (2026-01): Add jitter to prevent thundering herd
            jittered_backoff = backoff * random.uniform(0.9, 1.1)
            logger.warning(
                f"Claude API timeout (attempt {attempt + 1}/{MAX_RETRIES + 1}, "
                f"article_len={article_len}, backoff={jittered_backoff:.1f}s): {source_url}"
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(jittered_backoff)
            continue

        except RateLimitError as e:
            last_error = e
            backoff = 10 * (attempt + 1)  # 10s, 20s, 30s, 40s
            # FIX (2026-01): Add jitter to prevent thundering herd
            jittered_backoff = backoff * random.uniform(0.9, 1.1)
            logger.warning(
                f"Claude API rate limit (attempt {attempt + 1}/{MAX_RETRIES + 1}, "
                f"backoff={jittered_backoff:.1f}s): {e}"
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(jittered_backoff)
            continue

        except APIError as e:
            last_error = e
            logger.error(f"Claude API error (attempt {attempt + 1}/{MAX_RETRIES + 1}): {e}")
            if attempt < MAX_RETRIES and e.status_code >= 500:
                # FIX (2026-01): Add jitter to prevent thundering herd
                jittered_backoff = (2 ** attempt) * random.uniform(0.9, 1.1)
                await asyncio.sleep(jittered_backoff)  # Retry server errors
                continue
            break  # Don't retry client errors (4xx)

        except InstructorRetryException as e:
            # Instructor validation failed - log details to diagnose
            logger.error(
                f"Instructor validation failed for {source_url}: {e}\n"
                f"  Last attempt messages: {getattr(e, 'last_completion', 'N/A')}\n"
                f"  Validation errors: {getattr(e, 'errors', 'N/A')}"
            )
            # This is a validation error, not transient - don't retry
            return None

        except Exception as e:
            logger.error(f"Unexpected error during extraction for {source_url}: {type(e).__name__}: {e}")
            return None

    # All retries exhausted
    logger.error(
        f"Extraction failed after {MAX_RETRIES + 1} attempts for {source_url} "
        f"(article_len={article_len}): {type(last_error).__name__}: {last_error}"
    )
    return None


async def extract_deal_batch(
    articles: List[dict],
    max_concurrent: int = 5
) -> List[Optional[DealExtraction]]:
    """
    Extract deals from multiple articles concurrently.

    OPTIMIZED:
    - Concurrent processing with semaphore
    - Shared prompt cache across calls
    - Early exit for non-funding content

    Args:
        articles: List of dicts with keys: text, source_url, fund_slug (optional)
        max_concurrent: Maximum concurrent Claude API calls

    Returns:
        List of DealExtraction results (in same order as input)
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def extract_with_limit(article: dict) -> DealExtraction:
        async with semaphore:
            fund_context = None
            if article.get("fund_slug") and article["fund_slug"] in FUND_REGISTRY:
                fund_context = FUND_REGISTRY[article["fund_slug"]]

            return await extract_deal(
                article_text=article["text"],
                source_url=article.get("source_url", ""),
                source_name=article.get("fund_slug", "unknown"),
                fund_context=fund_context
            )

    # Run all extractions concurrently (limited by semaphore)
    results = await asyncio.gather(
        *[extract_with_limit(article) for article in articles],
        return_exceptions=True
    )

    # Handle any exceptions - log errors and skip failed extractions
    processed_results = []
    error_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            # Log the error and skip this extraction (return None)
            source_url = articles[i].get("source_url", "unknown")
            logger.error(f"Extraction failed for article {i} ({source_url}): {result}")
            processed_results.append(None)  # Caller should filter out None values
            error_count += 1
        else:
            processed_results.append(result)

    # FIX #22: Warn when failure rate exceeds 50% threshold
    total = len(articles)
    if total > 0 and error_count > 0:
        failure_rate = error_count / total
        if failure_rate > 0.5:
            logger.warning(
                f"High extraction failure rate: {error_count}/{total} ({failure_rate:.0%}) "
                f"articles failed - check Claude API status or rate limits"
            )

    return processed_results


async def extract_article(
    article_text: str,
    source_url: str = "",
    source_name: str = "unknown",
    title: str = "",
    fund_context: Optional[FundConfig] = None
) -> Optional[ArticleAnalysis]:
    """
    Analyze a full article that may contain multiple deals.

    Args:
        article_text: The article content to analyze
        source_url: URL of the source article
        source_name: Source identifier for token logging (e.g., "brave_search", "a16z")
        title: Article title
        fund_context: Optional fund-specific extraction context

    Returns:
        ArticleAnalysis with all deals found, or None if extraction failed
    """
    prompt = build_extraction_prompt(article_text, fund_context, source_url)

    # Use pre-built cached system message (optimization: created once at module load)
    last_error = None
    article_len = len(article_text)

    for attempt in range(MAX_RETRIES + 1):
        try:
            # Use create_with_completion to get both parsed model AND raw response with usage
            response, completion = client.messages.create_with_completion(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                temperature=settings.llm_temperature,
                system=CACHED_SYSTEM_MESSAGE,
                messages=[{"role": "user", "content": prompt}],
                response_model=ArticleAnalysis,
            )

            # Log token usage from the completion
            if completion and hasattr(completion, 'usage'):
                usage = completion.usage
                cache_read = getattr(usage, 'cache_read_input_tokens', 0) or 0
                cache_write = getattr(usage, 'cache_creation_input_tokens', 0) or 0
                logger.info(f"Claude article tokens: in={usage.input_tokens}, out={usage.output_tokens}")
                await log_token_usage(
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    source_name=source_name,
                    article_url=source_url,
                    model=settings.llm_model,
                    cache_read_tokens=cache_read,
                    cache_write_tokens=cache_write
                )
            else:
                logger.warning(f"No usage data in article completion: type={type(completion)}")

            # Post-process each deal
            for deal in response.deals:
                _verify_tracked_fund(deal)

            return response

        except APITimeoutError as e:
            last_error = e
            backoff = 2 ** attempt
            logger.warning(
                f"Claude API timeout (attempt {attempt + 1}/{MAX_RETRIES + 1}, "
                f"article_len={article_len}, backoff={backoff}s): {source_url}"
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(backoff)
            continue

        except RateLimitError as e:
            last_error = e
            backoff = 10 * (attempt + 1)
            logger.warning(
                f"Claude API rate limit (attempt {attempt + 1}/{MAX_RETRIES + 1}, "
                f"backoff={backoff}s): {e}"
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(backoff)
            continue

        except APIError as e:
            last_error = e
            logger.error(f"Claude API error (attempt {attempt + 1}/{MAX_RETRIES + 1}): {e}")
            if attempt < MAX_RETRIES and e.status_code >= 500:
                await asyncio.sleep(2 ** attempt)
                continue
            break

        except Exception as e:
            logger.error(f"Unexpected error during article extraction for {source_url}: {type(e).__name__}: {e}")
            return None

    logger.error(
        f"Article extraction failed after {MAX_RETRIES + 1} attempts for {source_url} "
        f"(article_len={article_len}): {type(last_error).__name__}: {last_error}"
    )
    return None


def _has_lead_language(snippet: str, article_text: str = "") -> bool:
    """
    Check if text contains lead investor language.

    IMPROVED (2026-01): Now checks BOTH verification snippet AND full article text.
    Previously only checked snippet, which missed cases where article said "led by"
    but LLM put different text in snippet.

    Args:
        snippet: The verification snippet extracted by LLM
        article_text: Full article text (optional, for additional validation)

    Returns:
        True if lead status is proven, False otherwise.
    """
    import re

    # Combine snippet and article text for checking
    # Check snippet first (higher weight), then article as fallback
    texts_to_check = []
    if snippet:
        texts_to_check.append(snippet.lower())
    if article_text:
        texts_to_check.append(article_text.lower())

    if not texts_to_check:
        return False

    # Strong lead phrases (require word boundaries to avoid false positives)
    lead_patterns = [
        r'\bled\s+by\b',           # 'led by'
        r'\bled\s+the\b',          # 'led the round'
        r'\bleads?\s+',            # 'lead ', 'leads '
        r'\bleading\b',            # 'leading'
        r'\bco-led\b',             # 'co-led'
        r'\bco-leads?\b',          # 'co-lead'
        r'\bheaded\s+by\b',        # 'headed by'
        r'\blead\s+investor\b',    # 'lead investor'
        r'\bspearheaded\b',        # 'spearheaded'
    ]

    for text in texts_to_check:
        for pattern in lead_patterns:
            if re.search(pattern, text):
                return True

    return False


def _investor_in_text(investor_name: str, text_lower: str) -> bool:
    """
    Check if an investor name (or any known alias) appears in article text.

    IMPROVED (2026-01): Uses word boundary matching for short investor names
    to prevent false positives (e.g., "Benchmark" matching "mark", "Index" matching "index.html").

    Handles:
    - Full name match (e.g., "Index Ventures")
    - Partial name match for multi-word names (e.g., "Sequoia" for "Sequoia Capital")
    - Known fund aliases (e.g., "a16z" for "Andreessen Horowitz")

    Args:
        investor_name: The investor name to search for
        text_lower: Lowercased article text

    Returns:
        True if investor appears in text (directly or via alias)
    """
    import re
    # Lazy import to avoid circular dependency
    from ..harvester.fund_matcher import FUND_NAME_VARIANTS

    inv_lower = investor_name.lower()

    def _check_with_boundary(name: str, text: str) -> bool:
        """Check if name appears in text with appropriate matching.

        Uses word boundary for short names (<8 chars) to prevent false positives.
        Uses substring for longer names which are more distinctive.

        FIX (2026-01): Excludes matches that are part of URLs to prevent false
        positives like "GV" matching "gv.com" or "a16z" matching "a16z.com".
        """
        if len(name) < 8:
            # Short names (GV, USV, Index, Accel) need word boundary
            # FIX: Exclude URL patterns - don't match if preceded by . or / or followed by .com/.org/etc
            # Pattern: name must NOT be preceded by [./] or followed by .[a-z]
            pattern = rf'(?<![./])\b{re.escape(name)}\b(?!\.[a-z])'
            return bool(re.search(pattern, text))
        else:
            # Longer names are distinctive enough for substring
            # But still exclude URL patterns
            pattern = rf'(?<![./]){re.escape(name)}(?!\.[a-z])'
            return bool(re.search(pattern, text))

    # Direct match (with word boundary for short names)
    if _check_with_boundary(inv_lower, text_lower):
        return True

    # Partial match for multi-word names (e.g., "Sequoia" for "Sequoia Capital")
    name_parts = inv_lower.split()
    if len(name_parts) > 1:
        for part in name_parts:
            if len(part) >= 4 and _check_with_boundary(part, text_lower):
                return True

    # Check known fund aliases (e.g., "a16z" ↔ "Andreessen Horowitz")
    for fund_slug, aliases in FUND_NAME_VARIANTS.items():
        # If investor matches any alias for this fund...
        if inv_lower in [a.lower() for a in aliases]:
            # ...check if ANY alias for this fund appears in text
            for alias in aliases:
                if _check_with_boundary(alias.lower(), text_lower):
                    return True

    return False


def _validate_company_in_text(deal: DealExtraction, article_text: str) -> DealExtraction:
    """
    Validate that the extracted company name actually appears in the article text.

    SAFEGUARD: Prevents hallucinated company names from being saved.

    This catches cases where Claude invents a company name based on context clues
    (e.g., combining investment thesis keywords like "Industrial AI" with real people
    to fabricate companies like "Modern Industrials").

    TIGHTENED (2026-01): Previous validation was too lenient - stripping "ai", "labs",
    "intelligence" etc. could leave common words that match too easily.

    Now requires:
    - For multi-word names: at least 2 significant words must appear
    - For single-word names: exact word boundary match (not substring)
    - Rejects common standalone words after stripping suffixes

    Args:
        deal: The extracted deal
        article_text: Original article text

    Returns:
        Modified deal with is_new_announcement=False if company not found
    """
    if not article_text or not deal.startup_name:
        return deal

    text_lower = article_text.lower()
    company_lower = deal.startup_name.lower().strip()

    # Direct match - company name appears in text
    if company_lower in text_lower:
        return deal

    # Extract main word(s) by removing common suffixes
    # These are often added by LLM based on context but may not be in article
    # FIX (2026-01): Use consolidated COMPANY_NAME_SUFFIX_WORDS from storage
    # This ensures _validate_company_in_text uses same suffixes as dedup normalization
    # Previously had its own hardcoded list that didn't match storage.py

    # Words that are too common to use as standalone validation
    # These can appear in many AI/tech articles without being the company name
    too_common_words = {
        'intelligence', 'analytics', 'platform', 'systems', 'solutions',
        'technologies', 'software', 'digital', 'global', 'modern', 'industrial',
        'capital', 'ventures', 'partners', 'group', 'labs', 'studio', 'works',
        'network', 'networks', 'media', 'services', 'consulting', 'research',
        'enterprise', 'automation', 'robotics', 'autonomous', 'machine', 'learning',
        'neural', 'cognitive', 'smart', 'advanced', 'next', 'future', 'first',
    }

    company_words = company_lower.split()

    # Filter out suffix words to get core name
    # FIX (2026-01): Use COMPANY_NAME_SUFFIX_WORDS instead of local suffixes_to_strip
    core_words = [
        w for w in company_words
        if w not in COMPANY_NAME_SUFFIX_WORDS and len(w) >= 3
    ]

    if not core_words:
        # All words are suffixes - use original first word if long enough
        if company_words and len(company_words[0]) >= 4:
            core_words = [company_words[0]]
        else:
            return deal  # Can't validate, allow it through

    # TIGHTENED: Filter out words that are too common to validate on their own
    significant_words = [w for w in core_words if w not in too_common_words]

    # If all core words are too common, require exact phrase match or consecutive words
    if not significant_words:
        # Try joining core words to find a more specific match
        phrase = ' '.join(core_words[:2]) if len(core_words) >= 2 else core_words[0]
        if phrase in text_lower:
            return deal
        # Single common word - require word boundary match (not just substring)
        for word in core_words:
            # FIX (2026-01): Skip empty strings - empty regex pattern matches everywhere
            if not word:
                # Log to help trace root cause - how did empty string get past len>=3 filter?
                logger.debug(
                    f"EMPTY_CORE_WORD: Skipping empty string in core_words for '{deal.startup_name}', "
                    f"full list: {core_words}"
                )
                continue
            # Use word boundary check: word must be surrounded by non-alphanumeric
            pattern = rf'(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])'
            if re.search(pattern, text_lower):
                return deal
        # No specific match found for common-word company
        logger.warning(
            f"REJECTING company with only common words: '{deal.startup_name}' - "
            f"core words {core_words} are too generic"
        )
        increment_extraction_stat("company_name_rejected")
        deal.is_new_announcement = False
        deal.announcement_rejection_reason = (
            f"Company name '{deal.startup_name}' contains only generic words - likely hallucinated"
        )
        return deal

    # TIGHTENED: For multi-word names, require at least 2 words to match
    # This prevents "Intelligence Labs" from matching just because "labs" is in text
    if len(significant_words) >= 2:
        words_found = sum(1 for w in significant_words if len(w) >= 4 and w in text_lower)
        if words_found >= 2:
            return deal
        # Fallback: check if first significant word (likely the unique name) appears
        if len(significant_words[0]) >= 5 and significant_words[0] in text_lower:
            return deal
    else:
        # Single significant word - require word boundary match for short words
        word = significant_words[0]
        if len(word) >= 6:
            # Longer words are distinctive enough for substring match
            if word in text_lower:
                return deal
        elif len(word) >= 4:
            # Shorter words need word boundary check
            pattern = rf'(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])'
            if re.search(pattern, text_lower):
                return deal

    # Company name not found - likely hallucinated
    logger.warning(
        f"REJECTING hallucinated company name: '{deal.startup_name}' - "
        f"core words {core_words} not found in article text"
    )
    increment_extraction_stat("company_name_rejected")
    deal.is_new_announcement = False
    deal.announcement_rejection_reason = (
        f"Company name '{deal.startup_name}' not found in article text - likely hallucinated"
    )

    return deal


def _validate_startup_not_fund(deal: DealExtraction, article_text: str) -> DealExtraction:
    """
    Reject deals where the startup name is actually a tracked VC fund or LP structure.
    Catches:
    - Fund raises (e.g., "a16z raises $15B Fund VII")
    - LP structures from SEC filings (e.g., "SP-1216 Fund I", "AU-0707 Fund III", "Feld Ventures Fund, LP")
    - SPV structures (e.g., "Perplexity SPV1 Emerging Global", "SpaceX SPV-2024")
    - LLC/LP entities without comma (e.g., "Midwest REO RR LLC")
    - Fund-like suffixes (e.g., "Partners III", "Emerging Global")
    """
    if not deal or not deal.startup_name:
        return deal

    name = deal.startup_name

    # Check for LP/fund structure names (SEC Form D filings for investment vehicles)
    # Pattern: "XX-1234 Fund I", "Name Fund III", "Name, LP", etc.
    # These are investment funds, not startups
    # Roman numerals: I-III, IV, V, VI-VIII, IX, X, XI-XIII, XIV, XV, XVI+
    ROMAN_NUMERALS = r'(?:i{1,3}|iv|vi{0,3}|ix|xi{0,3}|xiv|xvi{0,3}|x{1,2}|xv)'
    if re.search(rf'\bfund\s*({ROMAN_NUMERALS}|[0-9]+)?\s*,?\s*(lp|llc|llp)?$', name, re.IGNORECASE):
        logger.warning(
            f"Rejecting LP/fund structure: '{name}' - name ends with 'Fund' + roman numeral/LP"
        )
        increment_extraction_stat("fund_structure_rejected")
        deal.is_new_announcement = False
        deal.announcement_rejection_reason = (
            f"Name looks like LP/fund structure: {name} (likely SEC Form D for investment vehicle)"
        )
        return deal

    # Pattern: SPV (Special Purpose Vehicle) - catches "SPV1", "SPV-2024", "SPV I"
    # These are investment vehicle structures, not startups
    # SAFE: Real startups never have SPV in their name
    if re.search(r'\bspv[\s\-]?([0-9]+|[ivxlc]+)?\b', name, re.IGNORECASE):
        logger.warning(
            f"Rejecting SPV structure: '{name}' - contains SPV pattern"
        )
        increment_extraction_stat("fund_structure_rejected")
        deal.is_new_announcement = False
        deal.announcement_rejection_reason = (
            f"Name contains SPV (Special Purpose Vehicle): {name} (investment structure, not startup)"
        )
        return deal

    # Pattern: ends with ", LP" or ", LLC" WITH COMMA (typical fund legal entities)
    # The comma indicates formal legal name, not brand name
    # SAFE: Startups use brand names, not "Company, LLC"
    if re.search(r',\s*(lp|llc|llp)$', name, re.IGNORECASE):
        logger.warning(
            f"Rejecting LP entity: '{name}' - name ends with comma + legal entity suffix"
        )
        increment_extraction_stat("fund_structure_rejected")
        deal.is_new_announcement = False
        deal.announcement_rejection_reason = (
            f"Name looks like LP entity: {name} (likely fund structure, not startup)"
        )
        return deal

    # NOTE: We intentionally do NOT filter on " LLC" / " LP" without comma
    # Some real startups file SEC Form D as LLC before converting to C-corp
    # We'd rather have fund garbage than miss real startups

    # Pattern: fund code format (e.g., "SP-1216 Fund I", "AU-0707 Fund III")
    # SAFE: These are internal fund codes, never startup names
    if re.search(r'^[A-Z]{2,4}-\d{3,}', name) and 'fund' in name.lower():
        logger.warning(
            f"Rejecting fund code: '{name}' - matches fund code pattern"
        )
        increment_extraction_stat("fund_structure_rejected")
        deal.is_new_announcement = False
        deal.announcement_rejection_reason = (
            f"Name matches fund code pattern: {name}"
        )
        return deal

    from src.harvester.fund_matcher import match_fund_name

    # Check if startup name matches any tracked fund
    matched_slug = match_fund_name(deal.startup_name)
    if matched_slug:
        logger.warning(
            f"Rejecting fund raise: startup '{deal.startup_name}' matches "
            f"tracked fund '{matched_slug}'"
        )
        increment_extraction_stat("fund_raise_rejected")
        deal.is_new_announcement = False
        deal.announcement_rejection_reason = (
            f"Startup name matches tracked fund: {matched_slug} (likely fund raise, not portfolio deal)"
        )
        return deal

    # Secondary check: fund-raise keywords in article with large amounts
    # Catches phrases like "raised a new fund", "Fund VII", "LP commitments"
    text_lower = article_text.lower() if article_text else ""
    FUND_RAISE_PHRASES = [
        "raised a fund", "raised a new fund", "new fund",
        "fund i", "fund ii", "fund iii", "fund iv", "fund v",
        "fund vi", "fund vii", "fund viii", "fund ix", "fund x",
        "limited partners", "lp commitments", "fund size",
        "assets under management", "aum", "fundraising",
    ]
    if any(phrase in text_lower for phrase in FUND_RAISE_PHRASES):
        # Only reject if amount is very large (>$1B) — normal deals aren't this big
        amount_usd = _parse_amount_to_usd(deal.amount)
        if amount_usd and amount_usd >= 1_000_000_000:
            logger.warning(
                f"Rejecting likely fund raise: '{deal.startup_name}' "
                f"with fund-raise keywords and amount {deal.amount}"
            )
            increment_extraction_stat("fund_raise_rejected")
            deal.is_new_announcement = False
            deal.announcement_rejection_reason = (
                f"Likely fund raise: fund-raise keywords + amount {deal.amount} >= $1B"
            )

    return deal


def _validate_round_type(deal: DealExtraction) -> DealExtraction:
    """
    Validate that the extracted round_label is a valid RoundType enum value.

    SAFEGUARD: Prevents invalid round types from corrupting the database and
    bypassing deduplication logic (which depends on consistent round_type values).

    Invalid values like "growth_a", "seed_plus", or "pre_series_a" would:
    - Create inconsistent dedup_keys (different invalid values = different keys)
    - Bypass TIER 1 dedup (which requires matching round_type)
    - Corrupt analytics queries that filter by round_type

    Args:
        deal: The extracted deal

    Returns:
        Modified deal with valid round_type (UNKNOWN if original was invalid)
    """
    # Get valid round type values from enum
    valid_round_types = {rt.value for rt in RoundType}

    # Check if current round_label is valid
    # Note: Pydantic should have already coerced to enum, but LLM might return
    # invalid values that bypass validation in edge cases
    current_value = deal.round_label.value if isinstance(deal.round_label, RoundType) else str(deal.round_label)

    if current_value not in valid_round_types:
        logger.warning(
            f"CORRECTING invalid round_type for {deal.startup_name}: "
            f"'{current_value}' is not a valid RoundType - setting to UNKNOWN"
        )
        increment_extraction_stat("round_type_corrected")
        deal.round_label = RoundType.UNKNOWN

    return deal


def _validate_confidence_score(deal: DealExtraction) -> DealExtraction:
    """
    Validate and clamp confidence score to valid range [0, 1].

    SAFEGUARD (2026-01): LLM might return confidence scores outside the valid range.
    This prevents invalid scores from causing issues downstream (e.g., negative
    probabilities in filtering logic, or scores > 1 skewing statistics).

    FIX 2026-01: Now stores the raw LLM confidence in extraction_confidence before
    any penalties are applied. This separates extraction quality from lead evidence
    quality for clearer semantics.

    FIX 2026-01: Now catches NaN and infinity values which can corrupt downstream logic.

    Args:
        deal: The extracted deal

    Returns:
        Modified deal with confidence_score clamped to [0, 1] and extraction_confidence set
    """
    original_score = deal.confidence_score

    # FIX (2026-01): Catch NaN and infinity before other checks
    # These can corrupt downstream logic (comparisons, averages, etc.)
    if math.isnan(deal.confidence_score) or math.isinf(deal.confidence_score):
        deal.confidence_score = 0.0
        # ERROR level: NaN/Inf indicates a serious LLM parsing failure
        logger.error(
            f"INVALID_CONFIDENCE: NaN/Inf for {deal.startup_name}: "
            f"{original_score} -> 0.0 (LLM parsing failure)"
        )
    # Clamp to valid range
    elif deal.confidence_score < 0:
        deal.confidence_score = 0.0
        logger.warning(
            f"CLAMPING negative confidence score for {deal.startup_name}: "
            f"{original_score:.3f} -> 0.0"
        )
    elif deal.confidence_score > 1:
        deal.confidence_score = 1.0
        logger.warning(
            f"CLAMPING confidence score > 1 for {deal.startup_name}: "
            f"{original_score:.3f} -> 1.0"
        )

    # FIX 2026-01: Store the raw LLM confidence before any penalties
    # This is the extraction quality score, separate from lead evidence quality
    deal.extraction_confidence = deal.confidence_score

    # Initialize penalty breakdown tracking
    deal.penalty_breakdown = {}

    return deal


def _validate_founders_in_text(deal: DealExtraction, article_text: str) -> DealExtraction:
    """
    Validate that extracted founders are actually mentioned in the article text.

    SAFEGUARD: Prevents hallucinated founder names from being saved.

    This catches cases where Claude hallucinates founder names based on
    company description or investment thesis rather than actual article content.

    Args:
        deal: The extracted deal
        article_text: Original article text

    Returns:
        Modified deal with hallucinated founders removed
    """
    if not article_text or not deal.founders:
        return deal

    text_lower = article_text.lower()
    validated_founders = []
    removed_founders = []

    for founder in deal.founders:
        founder_name = founder.name.lower().strip() if founder.name else ""
        if not founder_name:
            continue

        # Check if founder name appears in article
        # Use word-based matching to avoid false positives
        name_parts = [p for p in founder_name.split() if len(p) >= 3]

        if not name_parts:
            # Very short name parts - require exact match
            if founder_name in text_lower:
                validated_founders.append(founder)
            else:
                removed_founders.append(founder.name)
            continue

        # Require at least 2 significant name parts to appear in text
        # This prevents "John" matching when only the last name "Smith" is in the article
        parts_found = sum(1 for part in name_parts if part in text_lower)

        if parts_found >= min(2, len(name_parts)):  # At least 2 parts, or all if fewer
            validated_founders.append(founder)
        else:
            removed_founders.append(founder.name)

    if removed_founders:
        logger.warning(
            f"REMOVING hallucinated founders for {deal.startup_name}: "
            f"{removed_founders} - not found in article text"
        )
        increment_extraction_stat("founders_removed", len(removed_founders))
        deal.founders = validated_founders

        # FIX (2026-01): Only penalize HALLUCINATED founders (extracted but not in text)
        # NOT missing founders (when article doesn't mention any founder)
        # Early-stage deals often don't name founders - that's fine (no penalty)
        # But fabricated founder names indicate LLM hallucination (small penalty)
        #
        # Key distinction:
        # - deal.founders=[] (no founders extracted) → no penalty
        # - deal.founders extracted but not in text → penalty per hallucinated name
        penalty = min(0.10, len(removed_founders) * 0.03)
        deal.confidence_score = max(0.0, deal.confidence_score - penalty)

        # FIX 2026-01: Track penalty in breakdown for debugging/analytics
        if deal.penalty_breakdown is not None:
            deal.penalty_breakdown["founders_removed"] = penalty

        logger.debug(
            f"Reduced confidence by {penalty:.2f} for {deal.startup_name} "
            f"(removed {len(removed_founders)} hallucinated founders)"
        )

    return deal


def _validate_investors_in_text(deal: DealExtraction, article_text: str) -> DealExtraction:
    """
    Validate that claimed investors are actually mentioned in the article text.

    SAFEGUARD: Prevents hallucinated investors from being saved.

    This catches cases where Claude hallucinates an investor based on fund context
    rather than actual article content. For example, if an article came from Index Ventures'
    news feed but doesn't mention Index Ventures, we should NOT list them as investor.

    Args:
        deal: The extracted deal
        article_text: Original article text

    Returns:
        Modified deal with hallucinated investors removed
    """
    if not article_text:
        return deal

    text_lower = article_text.lower()

    # Check lead investors
    validated_leads = []
    removed_leads = []
    for inv in deal.lead_investors:
        if _investor_in_text(inv.name, text_lower):
            validated_leads.append(inv)
        else:
            removed_leads.append(inv.name)

    # Log and update if investors were removed
    if removed_leads:
        logger.warning(
            f"REMOVING hallucinated lead investors for {deal.startup_name}: "
            f"{removed_leads} - not found in article text"
        )
        increment_extraction_stat("investors_removed", len(removed_leads))
        deal.lead_investors = validated_leads

        # FIX (2026-01): Reduce confidence when hallucinated investors are removed
        # Lead investors are more critical for deal validity, so larger penalty
        penalty = min(0.15, len(removed_leads) * 0.05)
        deal.confidence_score = max(0.0, deal.confidence_score - penalty)

        # FIX 2026-01: Track penalty in breakdown for debugging/analytics
        if deal.penalty_breakdown is not None:
            deal.penalty_breakdown["investors_removed"] = penalty

        logger.debug(
            f"Reduced confidence by {penalty:.2f} for {deal.startup_name} "
            f"(removed {len(removed_leads)} hallucinated lead investors)"
        )

        # If all lead investors removed, update tracked fund status
        if not validated_leads:
            deal.tracked_fund_is_lead = False
            deal.tracked_fund_name = None
            deal.tracked_fund_role = None

    # Check participating investors
    validated_participants = []
    removed_participants = []
    for inv in deal.participating_investors:
        if _investor_in_text(inv.name, text_lower):
            validated_participants.append(inv)
        else:
            removed_participants.append(inv.name)

    if removed_participants:
        logger.warning(
            f"REMOVING hallucinated participating investors for {deal.startup_name}: "
            f"{removed_participants} - not found in article text"
        )
        increment_extraction_stat("investors_removed", len(removed_participants))
        deal.participating_investors = validated_participants

    return deal


def _verify_tracked_fund(deal: DealExtraction, article_text: str = "") -> DealExtraction:
    """
    Post-process to verify tracked fund identification.

    STRICT VERIFICATION:
    - Only sets tracked_fund_is_lead=True if:
      1. A tracked fund is in lead_investors with CONFIRMED_LEAD role
      2. The verification_snippet contains actual lead language
    - Downgrades to participant if snippet is missing or invalid

    IMPROVED (2026-01): Now also checks full article text for lead language,
    not just the verification snippet. This catches cases where the article
    says "led by" but LLM puts different text in the snippet.

    Args:
        deal: The extracted deal
        article_text: Full article text (for additional lead language validation)

    This prevents false positives where the LLM claims lead status
    without proper evidence.
    """
    # Lazy import to avoid circular dependency
    from ..harvester.fund_matcher import match_fund_name

    # Track all matched funds for logging
    matched_leads = []
    matched_participants = []

    # Validate verification snippet - DOWNGRADE if missing or lacks lead language
    # FIX (2026-01): Previously just flagged weak evidence, now actually downgrades to LIKELY_LEAD
    # This reduces false positives where Claude claims lead status without proof
    # IMPROVED (2026-01): Now also checks full article text for lead language
    has_valid_snippet = _has_lead_language(deal.verification_snippet, article_text)

    # FIX 2026-01: Calculate lead_evidence_score based on verification quality
    # 1.0 = explicit "led by" in snippet, 0.5 = weak evidence, 0.0 = no evidence
    if deal.tracked_fund_is_lead:
        if has_valid_snippet:
            deal.lead_evidence_score = 1.0  # Strong evidence - explicit lead language
        elif deal.verification_snippet:
            deal.lead_evidence_score = 0.5  # Weak evidence - snippet but no lead language
        else:
            deal.lead_evidence_score = 0.2  # Very weak - no snippet at all
    else:
        deal.lead_evidence_score = None  # Not a lead deal

    if deal.tracked_fund_is_lead and deal.verification_snippet and not has_valid_snippet:
        logger.warning(
            f"DOWNGRADING lead claim for {deal.startup_name}: "
            f"snippet '{deal.verification_snippet[:80]}...' lacks explicit lead language"
        )
        increment_extraction_stat("lead_evidence_downgraded")
        deal.lead_evidence_weak = True  # Flag for frontend display
        # Set to LIKELY_LEAD since Claude claimed lead but snippet lacks proof
        deal.tracked_fund_role = LeadStatus.LIKELY_LEAD
        # FIX (2026-01): Reduce confidence when lead evidence is weak
        deal.confidence_score = max(0.0, deal.confidence_score - 0.08)

        # FIX 2026-01: Track penalty in breakdown for debugging/analytics
        if deal.penalty_breakdown is not None:
            deal.penalty_breakdown["weak_evidence"] = 0.08

    if deal.tracked_fund_is_lead and not deal.verification_snippet:
        logger.warning(
            f"DOWNGRADING lead claim for {deal.startup_name}: "
            f"no verification_snippet provided"
        )
        increment_extraction_stat("lead_evidence_downgraded")
        deal.lead_evidence_weak = True  # Flag for frontend display
        # Set to LIKELY_LEAD since Claude claimed lead but no snippet provided
        deal.tracked_fund_role = LeadStatus.LIKELY_LEAD
        # FIX (2026-01): Reduce confidence when lead evidence is weak
        deal.confidence_score = max(0.0, deal.confidence_score - 0.08)

        # FIX 2026-01: Track penalty in breakdown for debugging/analytics
        if deal.penalty_breakdown is not None:
            deal.penalty_breakdown["weak_evidence"] = 0.08

    # FIX: Track ALL confirmed lead funds to record co-leads
    confirmed_lead_funds: list[tuple[str, str, str]] = []  # (slug, name, partner)

    # Check ALL lead investors against our fund registry
    for investor in deal.lead_investors:
        # FIX (2026-01): Pass article_text to enable partner name → fund attribution
        # e.g., "Bill Gurley" → "benchmark" only when investment context is present
        matched_fund = match_fund_name(investor.name, context_text=article_text)
        # FIX: Validate matched_fund exists in FUND_REGISTRY before using
        if matched_fund and matched_fund in FUND_REGISTRY:
            investor.is_tracked_fund = True
            matched_leads.append((matched_fund, investor))

            # Track confirmed/likely leads with valid snippets
            if investor.role in (LeadStatus.CONFIRMED_LEAD, LeadStatus.LIKELY_LEAD) and has_valid_snippet:
                confirmed_lead_funds.append((
                    matched_fund,
                    FUND_REGISTRY[matched_fund].name,
                    investor.partner_name or ""
                ))
            elif not deal.tracked_fund_name:
                # Set fund name and partner
                deal.tracked_fund_name = FUND_REGISTRY[matched_fund].name
                deal.tracked_fund_partner = investor.partner_name
                # Only set to PARTICIPANT if not already flagged as weak evidence (LIKELY_LEAD)
                # FIX (2026-01): Previously overwrote LIKELY_LEAD from early check
                if not deal.lead_evidence_weak:
                    logger.info(
                        f"Setting {deal.startup_name} as participant: "
                        f"role={investor.role.value}, has_valid_snippet={has_valid_snippet}"
                    )
                    deal.tracked_fund_role = LeadStatus.PARTICIPANT
                # else: keep LIKELY_LEAD from early weak evidence check

        elif matched_fund:
            # Fund matched but not in registry - log warning
            logger.warning(f"Matched fund '{matched_fund}' not in FUND_REGISTRY")

    # FIX: Set deal-level fields to include ALL co-lead tracked funds
    if confirmed_lead_funds:
        deal.tracked_fund_is_lead = True
        # Join all co-lead fund names (e.g., "Sequoia, Benchmark")
        fund_names = [name for _, name, _ in confirmed_lead_funds]
        deal.tracked_fund_name = ", ".join(fund_names)
        deal.tracked_fund_role = LeadStatus.CONFIRMED_LEAD
        # Collect all partner names if present
        partner_names = [p for _, _, p in confirmed_lead_funds if p]
        deal.tracked_fund_partner = ", ".join(partner_names) if partner_names else None

    # Log if multiple tracked funds co-led
    if len(confirmed_lead_funds) > 1:
        logger.info(
            f"Multiple tracked funds co-led deal for {deal.startup_name}: "
            f"{[n for _, n, _ in confirmed_lead_funds]}"
        )

    # Also check ALL participating investors
    for investor in deal.participating_investors:
        # FIX (2026-01): Pass article_text to enable partner name → fund attribution
        matched_fund = match_fund_name(investor.name, context_text=article_text)
        # FIX: Validate matched_fund exists in FUND_REGISTRY before using
        if matched_fund and matched_fund in FUND_REGISTRY:
            investor.is_tracked_fund = True
            matched_participants.append((matched_fund, investor))
            # Only set deal-level fields if no lead was found
            if not deal.tracked_fund_is_lead and not deal.tracked_fund_name:
                deal.tracked_fund_name = FUND_REGISTRY[matched_fund].name
                deal.tracked_fund_role = LeadStatus.PARTICIPANT
                deal.tracked_fund_partner = investor.partner_name
        elif matched_fund:
            # Fund matched but not in registry - log warning
            logger.warning(f"Matched fund '{matched_fund}' not in FUND_REGISTRY")

    return deal


def check_negative_filters(text: str, fund: FundConfig) -> bool:
    """
    Check if text should be filtered out based on fund's negative keywords.

    Returns True if text should be EXCLUDED (contains negative keywords).
    """
    text_lower = text.lower()
    for keyword in fund.negative_keywords:
        if keyword.lower() in text_lower:
            return True
    return False


# Convenience function for testing
async def quick_extract(text: str) -> dict:
    """Quick extraction for testing - returns dict instead of model."""
    result = await extract_deal(text)
    return result.model_dump()
