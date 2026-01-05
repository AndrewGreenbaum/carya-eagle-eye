"""
Comprehensive tests for ALL 6 deduplication tiers.

Tier order (executed sequentially):
- TIER 0: Exact company + same round + date ±3 days (race condition prevention)
- TIER 4: Name prefix (3 chars) + amount ±5% + exact date + same round (name variations)
- TIER 3: Amount ±10% + company match + date ±30 days, ANY round (cross-round dupes)
- TIER 2: Same company + exact date, any round/amount (multi-source same-day)
- TIER 2.5: Company + same round + date ±30 days, any amount (EXTENDED 2026-01 from ±7 days)
- TIER 1: Company + round + amount ±15%, date ±365 days (standard dedup)

Run with: pytest tests/test_deduplication_all_tiers.py -v

Note: These tests require src.archivist imports which may need Python 3.10+.
Tests will be skipped if imports fail.

2026-01 FIX: Extended TIER 2.5 from ±7 to ±30 days to catch the Emergent/Khosla bug
where two deals (Jan 1 and Jan 20, 19 days apart) weren't detected as duplicates.
"""

import pytest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import List, Tuple, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import test helpers
from tests.test_helpers import skip_no_archivist, can_import_archivist

# Skip entire module if archivist imports fail (Python 3.9 compatibility)
pytestmark = [skip_no_archivist]

# Lazy imports - only executed if module isn't skipped
if can_import_archivist():
    from src.archivist.storage import (
        find_duplicate_deal,
        normalize_amount,
        normalize_company_name,
        company_names_match,
    )
else:
    # Dummy values for when imports fail (tests will be skipped anyway)
    find_duplicate_deal = None
    normalize_amount = None
    normalize_company_name = None
    company_names_match = None


# ============================================================================
# SHARED TEST UTILITIES
# ============================================================================

class BaseDeduplicationTest:
    """Base class with shared test utilities for all tier tests."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        session = AsyncMock()
        return session

    def create_mock_result(self, deals_list: List[Tuple]):
        """Create a properly mocked result object for SQLAlchemy."""
        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=deals_list)
        return mock_result

    def create_mock_deal(
        self,
        deal_id: int,
        company_name: str,
        round_type: str,
        amount: str,
        amount_usd: Optional[int],
        announced_date: Optional[date],
        created_at: Optional[datetime] = None
    ) -> Tuple:
        """Create a mock (deal, company) tuple."""
        deal = MagicMock()
        deal.id = deal_id
        deal.round_type = round_type
        deal.amount = amount
        deal.amount_usd = amount_usd
        deal.announced_date = announced_date
        deal.created_at = created_at or datetime.now(timezone.utc).replace(tzinfo=None)

        company = MagicMock()
        company.id = deal_id
        company.name = company_name

        return (deal, company)

    def setup_sequential_results(self, mock_session, results_sequence: List[List[Tuple]]):
        """
        Setup mock session to return different results for sequential queries.
        Each tier makes its own query, so we need to return results in order.

        results_sequence: List of results, one per execute() call
        """
        mock_results = [self.create_mock_result(r) for r in results_sequence]
        mock_session.execute.side_effect = mock_results


# ============================================================================
# TIER 0 TESTS: Exact company + same round + date ±3 days
# ============================================================================

class TestTier0(BaseDeduplicationTest):
    """
    TIER 0: Race condition prevention
    - Exact company name (case-insensitive)
    - Same round type
    - Date within ±3 days
    """

    @pytest.mark.asyncio
    async def test_tier0_exact_match(self, mock_session):
        """TIER 0 should match identical company/round/date."""
        existing = self.create_mock_deal(
            deal_id=100,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            amount_usd=140_000_000,
            announced_date=date(2026, 1, 11)
        )

        # TIER 0 returns match
        mock_session.execute.return_value = self.create_mock_result([existing])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        assert result is not None
        assert result.id == 100

    @pytest.mark.asyncio
    async def test_tier0_date_within_3_days(self, mock_session):
        """TIER 0 should match when date is within ±3 days."""
        existing = self.create_mock_deal(
            deal_id=100,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            amount_usd=140_000_000,
            announced_date=date(2026, 1, 11)
        )

        mock_session.execute.return_value = self.create_mock_result([existing])

        # Test +3 days boundary
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 14)  # 3 days later
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier0_case_insensitive(self, mock_session):
        """TIER 0 should match regardless of case."""
        existing = self.create_mock_deal(
            deal_id=100,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            amount_usd=140_000_000,
            announced_date=date(2026, 1, 11)
        )

        mock_session.execute.return_value = self.create_mock_result([existing])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="TORQ",  # Different case
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier0_no_match_different_round(self, mock_session):
        """TIER 0 should NOT match different round types."""
        # Return empty for all tier queries
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_c",  # Different from series_d
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier0_no_match_date_4_days_apart(self, mock_session):
        """TIER 0 should NOT match when date is 4+ days apart."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 15)  # 4 days from Jan 11
        )

        assert result is None


# ============================================================================
# TIER 4 TESTS: Name prefix + amount ±5% + exact date + same round
# ============================================================================

class TestTier4(BaseDeduplicationTest):
    """
    TIER 4: Name variation detection
    - Company names share first 3 characters (normalized)
    - Amount within ±5%
    - Exact date match
    - Same round type
    - Prefix match requires 60% overlap (FIX 2026-01: prevents "MEQ Probe" vs "MEQ Consulting" false matches)

    Example: "Acme Labs" vs "Acme Inc" - same company with different suffix (60% prefix "acme" matches)
    """

    @pytest.mark.asyncio
    async def test_tier4_name_variation_match(self, mock_session):
        """
        TIER 4 should match name variations with exact amount/date/round.
        Uses "Acme Labs" vs "Acme Inc" which have 60%+ prefix overlap.
        """
        existing = self.create_mock_deal(
            deal_id=200,
            company_name="Acme Inc",
            round_type="seed",
            amount="$5M",
            amount_usd=5_000_000,
            announced_date=date(2026, 1, 10)
        )

        # TIER 0 returns empty, TIER 4 returns match
        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [existing],  # TIER 4
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Acme Labs",  # "acme" prefix (4 chars) with 60% overlap
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None
        assert result.id == 200

    @pytest.mark.asyncio
    async def test_tier4_amount_within_5_percent(self, mock_session):
        """TIER 4 should match when amount is within ±5%."""
        existing = self.create_mock_deal(
            deal_id=200,
            company_name="Acme Inc",
            round_type="seed",
            amount="$5M",
            amount_usd=5_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [existing],  # TIER 4
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Acme Labs",
            round_type="seed",
            amount="$5.2M",  # 4% more - within 5% tolerance
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier4_no_match_amount_over_5_percent(self, mock_session):
        """TIER 4 should NOT match when amount differs by >5%."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="MEQ Probe",
            round_type="seed",
            amount="$6M",  # 20% more - exceeds 5% tolerance
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier4_no_match_different_date(self, mock_session):
        """TIER 4 requires exact date match."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="MEQ Probe",
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 11)  # Different date
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier4_no_match_different_round(self, mock_session):
        """TIER 4 requires same round type."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="MEQ Probe",
            round_type="series_a",  # Different round
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier4_no_match_prefix_too_short(self, mock_session):
        """TIER 4 requires at least 3-char prefix match."""
        mock_session.execute.return_value = self.create_mock_result([])

        # "AI Corp" vs "AI Labs" - only 2 chars match ("ai")
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="AI Labs",
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier4_no_match_different_prefix(self, mock_session):
        """TIER 4 should NOT match when prefixes don't match."""
        mock_session.execute.return_value = self.create_mock_result([])

        # "ABC Corp" vs "XYZ Corp" - different prefixes
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="XYZ Corp",
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None


# ============================================================================
# TIER 3 TESTS: Amount ±10% + company match + date ±30 days, ANY round
# ============================================================================

class TestTier3(BaseDeduplicationTest):
    """
    TIER 3: Cross-round duplicate detection
    - Company names match (fuzzy)
    - Amount within ±10%
    - Date within ±30 days
    - ANY round type (catches "seed" vs "series_a" confusion)
    """

    @pytest.mark.asyncio
    async def test_tier3_cross_round_match(self, mock_session):
        """
        TIER 3 should match same deal reported with different round types.
        Example: Same $10M deal reported as both "seed" and "series_a".
        """
        existing = self.create_mock_deal(
            deal_id=300,
            company_name="CrossRound Inc",
            round_type="seed",  # Original round type
            amount="$10M",
            amount_usd=10_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [existing],  # TIER 3
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound Inc",
            round_type="series_a",  # Different round type
            amount="$10M",  # Same amount
            announced_date=date(2026, 1, 12)  # Within 30 days
        )

        assert result is not None
        assert result.id == 300

    @pytest.mark.asyncio
    async def test_tier3_amount_within_10_percent(self, mock_session):
        """TIER 3 should match when amount is within ±10%."""
        existing = self.create_mock_deal(
            deal_id=300,
            company_name="CrossRound Inc",
            round_type="seed",
            amount="$10M",
            amount_usd=10_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [existing],  # TIER 3
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound Inc",
            round_type="series_a",
            amount="$11M",  # 10% more - at boundary
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier3_date_within_30_days(self, mock_session):
        """TIER 3 should match when date is within ±30 days."""
        existing = self.create_mock_deal(
            deal_id=300,
            company_name="CrossRound Inc",
            round_type="seed",
            amount="$10M",
            amount_usd=10_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [existing],  # TIER 3
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound Inc",
            round_type="series_a",
            amount="$10M",
            announced_date=date(2026, 2, 8)  # ~30 days later
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier3_company_fuzzy_match(self, mock_session):
        """TIER 3 should use fuzzy company name matching."""
        existing = self.create_mock_deal(
            deal_id=300,
            company_name="CrossRound AI",  # With suffix
            round_type="seed",
            amount="$10M",
            amount_usd=10_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [existing],  # TIER 3
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound",  # Without suffix
            round_type="series_a",
            amount="$10M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier3_no_match_amount_over_10_percent(self, mock_session):
        """TIER 3 should NOT match when amount differs by >10%."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound Inc",
            round_type="series_a",
            amount="$12M",  # 20% more than $10M - exceeds 10%
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier3_no_match_date_over_30_days(self, mock_session):
        """TIER 3 should NOT match when date is >30 days apart."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound Inc",
            round_type="series_a",
            amount="$10M",
            announced_date=date(2026, 3, 15)  # ~65 days later
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier3_no_match_null_amount(self, mock_session):
        """TIER 3 requires amount to match - null amounts skip this tier."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="CrossRound Inc",
            round_type="series_a",
            amount=None,  # No amount
            announced_date=date(2026, 1, 10)
        )

        # TIER 3 is skipped when no amount, falls through to other tiers
        assert result is None


# ============================================================================
# TIER 2 TESTS: Same company + exact date, any round/amount
# ============================================================================

class TestTier2(BaseDeduplicationTest):
    """
    TIER 2: Multi-source same-day detection
    - Company name exact match (case-insensitive) OR fuzzy match
    - Exact date match
    - Any round type
    - Any amount
    """

    @pytest.mark.asyncio
    async def test_tier2_same_day_exact_name(self, mock_session):
        """
        TIER 2 should match same company on same day regardless of round/amount.
        """
        existing = self.create_mock_deal(
            deal_id=400,
            company_name="SameDay Corp",
            round_type="seed",
            amount="$5M",
            amount_usd=5_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [existing],  # TIER 2 exact
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="SameDay Corp",
            round_type="series_a",  # Different round
            amount="$8M",  # Different amount
            announced_date=date(2026, 1, 10)  # Same day
        )

        assert result is not None
        assert result.id == 400

    @pytest.mark.asyncio
    async def test_tier2_same_day_fuzzy_name(self, mock_session):
        """TIER 2 should use fuzzy matching for company names."""
        existing = self.create_mock_deal(
            deal_id=400,
            company_name="SameDay AI",  # With suffix
            round_type="seed",
            amount="$5M",
            amount_usd=5_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [existing],  # TIER 2 fuzzy
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="SameDay",  # Without suffix
            round_type="series_a",
            amount="$8M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier2_no_match_different_day(self, mock_session):
        """TIER 2 requires exact date match."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="SameDay Corp",
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 11)  # Different day
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier2_no_match_different_company(self, mock_session):
        """TIER 2 should NOT match different companies on same day."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Different Corp",  # Different company
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier2_null_date_skipped(self, mock_session):
        """TIER 2 is skipped when no date provided."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="SameDay Corp",
            round_type="seed",
            amount="$5M",
            announced_date=None  # No date
        )

        # TIER 2 requires exact date, so it's skipped
        assert result is None


# ============================================================================
# TIER 2.5 TESTS: Company + same round + date ±30 days, any amount
# ============================================================================

class TestTier25(BaseDeduplicationTest):
    """
    TIER 2.5: Valuation vs funding confusion detection + date gap handling
    - Company names match (fuzzy)
    - Same round type
    - Date within ±30 days (EXTENDED from ±7 days in 2026-01 fix)
    - Amount NOT required to match

    Example: $6.6B valuation report vs $330M funding report for same deal
    Example: Emergent Series B reported Jan 1 vs Jan 20 (19 days apart)
    """

    @pytest.mark.asyncio
    async def test_tier25_valuation_funding_confusion(self, mock_session):
        """
        TIER 2.5 should catch when valuation is reported as funding amount.
        FIX 2026-01: Valuation confusion allowed when larger amount >= $500M
        """
        existing = self.create_mock_deal(
            deal_id=450,
            company_name="Valuation Corp",
            round_type="series_c",
            amount="$330M",  # Actual funding
            amount_usd=330_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [],  # TIER 2 fuzzy
            [existing],  # TIER 2.5 (valuation confusion allowed when >$500M)
            [],  # TIER 1 (not reached)
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Valuation Corp",
            round_type="series_c",
            amount="$6.6B",  # Valuation reported as funding (>$500M so allowed)
            announced_date=date(2026, 1, 12)  # 2 days later
        )

        assert result is not None
        assert result.id == 450

    @pytest.mark.asyncio
    async def test_tier25_date_within_30_days(self, mock_session):
        """TIER 2.5 should match when date is within ±30 days."""
        existing = self.create_mock_deal(
            deal_id=450,
            company_name="Valuation Corp",
            round_type="series_c",
            amount="$330M",
            amount_usd=330_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [],  # TIER 2 fuzzy
            [existing],  # TIER 2.5
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Valuation Corp",
            round_type="series_c",
            amount="$500M",
            announced_date=date(2026, 2, 8)  # 29 days later - within ±30 days
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier25_emergent_scenario_19_days_apart(self, mock_session):
        """
        CRITICAL: Test for Emergent Series B bug fix.
        Two articles about same deal reported 19 days apart should be caught.
        - Deal #3110: Emergent Series B $70M, 2026-01-01
        - Deal #3196: Emergent Series B $70M, 2026-01-20
        """
        existing = self.create_mock_deal(
            deal_id=3110,
            company_name="Emergent",
            round_type="series_b",
            amount="$70 million",
            amount_usd=70_000_000,  # May be NULL in real scenario
            announced_date=date(2026, 1, 1)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0 (dates 19 days apart > ±3 days)
            [],  # TIER 4 (no exact date match)
            [],  # TIER 3 (may fail if amount_usd is NULL)
            [],  # TIER 2 exact (different dates)
            [],  # TIER 2 fuzzy (different dates)
            [existing],  # TIER 2.5 (same round + ±30 days = CAUGHT!)
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Emergent",
            round_type="series_b",
            amount="$70M",
            announced_date=date(2026, 1, 20)  # 19 days later
        )

        assert result is not None, "Emergent duplicate should be caught by TIER 2.5 (±30 days)"
        assert result.id == 3110

    @pytest.mark.asyncio
    async def test_tier25_no_match_different_round(self, mock_session):
        """TIER 2.5 requires same round type."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Valuation Corp",
            round_type="series_d",  # Different round
            amount="$6.6B",
            announced_date=date(2026, 1, 12)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier25_no_match_date_over_30_days(self, mock_session):
        """TIER 2.5 should NOT match when date is >30 days apart."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Valuation Corp",
            round_type="series_c",
            amount="$6.6B",
            announced_date=date(2026, 2, 15)  # 36 days from Jan 10 - exceeds 30
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier25_boundary_exactly_30_days(self, mock_session):
        """TIER 2.5 should match at exactly 30 days boundary."""
        existing = self.create_mock_deal(
            deal_id=450,
            company_name="Boundary Corp",
            round_type="series_a",
            amount="$50M",
            amount_usd=50_000_000,
            announced_date=date(2026, 1, 1)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [],  # TIER 2 fuzzy
            [existing],  # TIER 2.5
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Boundary Corp",
            round_type="series_a",
            amount="$55M",
            announced_date=date(2026, 1, 31)  # Exactly 30 days
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier25_null_date_skipped(self, mock_session):
        """TIER 2.5 is skipped when no date provided."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Valuation Corp",
            round_type="series_c",
            amount="$330M",
            announced_date=None
        )

        assert result is None


# ============================================================================
# TIER 1 TESTS: Company + round + amount ±15%, date ±365 days
# ============================================================================

class TestTier1(BaseDeduplicationTest):
    """
    TIER 1: Standard deduplication (last resort)
    - Company names match (fuzzy)
    - Same round type
    - Amount within ±15%
    - Date within ±365 days
    """

    @pytest.mark.asyncio
    async def test_tier1_standard_match(self, mock_session):
        """TIER 1 should match with standard criteria."""
        existing = self.create_mock_deal(
            deal_id=500,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$20M",
            amount_usd=20_000_000,
            announced_date=date(2025, 6, 15)  # 6 months ago
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [],  # TIER 2 fuzzy
            [],  # TIER 2.5
            [existing],  # TIER 1
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$22M",  # 10% more - within 15%
            announced_date=date(2026, 1, 10)  # ~7 months later
        )

        assert result is not None
        assert result.id == 500

    @pytest.mark.asyncio
    async def test_tier1_amount_within_15_percent(self, mock_session):
        """TIER 1 should match when amount is within ±15%."""
        existing = self.create_mock_deal(
            deal_id=500,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$20M",
            amount_usd=20_000_000,
            announced_date=date(2025, 12, 1)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [],  # TIER 2 fuzzy
            [],  # TIER 2.5
            [existing],  # TIER 1
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$23M",  # 15% more - at boundary
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier1_date_within_365_days(self, mock_session):
        """TIER 1 should match when date is within ±365 days."""
        existing = self.create_mock_deal(
            deal_id=500,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$20M",
            amount_usd=20_000_000,
            announced_date=date(2025, 1, 15)  # ~360 days ago
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4
            [],  # TIER 3
            [],  # TIER 2 exact
            [],  # TIER 2 fuzzy
            [],  # TIER 2.5
            [existing],  # TIER 1
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$20M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier1_no_match_amount_over_15_percent(self, mock_session):
        """TIER 1 should NOT match when amount differs by >15%."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$25M",  # 25% more than $20M - exceeds 15%
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier1_no_match_different_round(self, mock_session):
        """TIER 1 requires same round type."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Standard Corp",
            round_type="series_b",  # Different round
            amount="$20M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier1_no_match_date_over_365_days(self, mock_session):
        """TIER 1 should NOT match when date is >365 days apart."""
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Standard Corp",
            round_type="series_a",
            amount="$20M",
            announced_date=date(2024, 1, 1)  # >365 days ago
        )

        assert result is None


# ============================================================================
# TIER ORDERING TESTS: Ensure higher tiers catch before lower tiers
# ============================================================================

class TestTierOrdering(BaseDeduplicationTest):
    """
    Test that tiers are executed in the correct order and higher tiers
    take priority over lower tiers.
    """

    @pytest.mark.asyncio
    async def test_tier0_catches_before_tier1(self, mock_session):
        """TIER 0 should catch duplicates before TIER 1 is checked."""
        existing = self.create_mock_deal(
            deal_id=100,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            amount_usd=140_000_000,
            announced_date=date(2026, 1, 11)
        )

        # TIER 0 returns match immediately
        mock_session.execute.return_value = self.create_mock_result([existing])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        # Should match via TIER 0
        assert result is not None
        assert result.id == 100

        # Should have only called execute once (TIER 0)
        # Note: This verifies TIER 0 short-circuits before checking other tiers
        assert mock_session.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_tier4_catches_name_variations_before_tier3(self, mock_session):
        """
        TIER 4 should catch name variations before TIER 3's cross-round logic.
        Uses names with 60%+ prefix overlap (FIX 2026-01).
        """
        existing = self.create_mock_deal(
            deal_id=200,
            company_name="Acme Inc",
            round_type="seed",
            amount="$5M",
            amount_usd=5_000_000,
            announced_date=date(2026, 1, 10)
        )

        # TIER 0 empty, TIER 4 matches
        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [existing],  # TIER 4
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Acme Labs",  # "acme" prefix with 60%+ overlap
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None
        assert result.id == 200

        # Should have stopped at TIER 4 (2 calls total)
        assert mock_session.execute.call_count == 2


# ============================================================================
# FALSE POSITIVE PREVENTION TESTS
# ============================================================================

class TestFalsePositivePrevention(BaseDeduplicationTest):
    """
    CRITICAL: Ensure deduplication doesn't accidentally block legitimate deals.
    These tests verify that similar but different deals are NOT deduplicated.
    """

    @pytest.mark.asyncio
    async def test_different_companies_same_day_same_round_same_amount(self, mock_session):
        """
        Two different companies raising same round with same amount on same day
        should NOT be deduplicated.
        """
        mock_session.execute.return_value = self.create_mock_result([])

        # Company A already exists, Company B is new
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Company B",  # Different from Company A
            round_type="series_a",
            amount="$30M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "Different companies should NOT be deduplicated"

    @pytest.mark.asyncio
    async def test_same_company_different_rounds_close_dates(self, mock_session):
        """
        Same company raising different rounds within days should NOT be deduplicated.
        Example: Seed on Jan 8, Series A on Jan 10.
        """
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="FastGrowth Inc",
            round_type="series_a",  # Different from seed
            amount="$20M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "Different rounds should NOT be deduplicated"

    @pytest.mark.asyncio
    async def test_similar_company_names_not_confused(self, mock_session):
        """
        Similar company names should NOT be confused.
        "Torq" ≠ "Torque", "OpenAI" ≠ "OpenAPI".
        """
        mock_session.execute.return_value = self.create_mock_result([])

        # "Torque" should not match "Torq"
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torque",  # Different from "Torq"
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        assert result is None, "Similar but different company names should NOT match"

    @pytest.mark.asyncio
    async def test_legitimate_extension_round_not_blocked(self, mock_session):
        """
        Legitimate extension rounds should NOT be blocked.
        Series A → Series A-1 extension is a new deal.
        """
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="ExtendCo",
            round_type="series_a_extension",  # Different from series_a
            amount="$10M",
            announced_date=date(2026, 1, 15)
        )

        assert result is None, "Extension rounds should NOT be blocked"

    @pytest.mark.asyncio
    async def test_same_company_same_round_much_later(self, mock_session):
        """
        Same company, same round type, but >1 year apart should NOT be deduplicated.
        """
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="LongTerm Corp",
            round_type="series_b",
            amount="$50M",
            announced_date=date(2024, 1, 10)  # 2 years ago
        )

        assert result is None, "Deals >365 days apart should NOT be deduplicated"

    @pytest.mark.asyncio
    async def test_substring_company_names_not_matched(self, mock_session):
        """
        Company names that are substrings should NOT match.
        "Air" ≠ "Airbnb", "Meta" ≠ "Metadata".
        """
        mock_session.execute.return_value = self.create_mock_result([])

        # "Airbnb" should not match "Air"
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Airbnb",
            round_type="series_a",
            amount="$100M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "Substring company names should NOT match"

    @pytest.mark.asyncio
    async def test_debt_vs_equity_not_confused(self, mock_session):
        """
        Debt round should NOT be confused with equity round.
        """
        mock_session.execute.return_value = self.create_mock_result([])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="MixedFunding Corp",
            round_type="debt",  # Different from series_a
            amount="$50M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "Debt should NOT match equity rounds"


# ============================================================================
# REAL-WORLD SCENARIOS
# ============================================================================

class TestRealWorldScenarios(BaseDeduplicationTest):
    """
    Test real-world duplicate scenarios that have occurred in production.
    """

    @pytest.mark.asyncio
    async def test_torq_multi_source_duplicate(self, mock_session):
        """
        Real scenario: Torq Series D had 3 duplicates from different sources.
        - Deal 2884: $140 million, Jan 11 (SecurityWeek)
        - Deal 2885: $140M, Jan 11 (Globes) - should be caught
        - Deal 2886: $140M, Jan 12 (Finsmes) - should be caught
        """
        original = self.create_mock_deal(
            deal_id=2884,
            company_name="Torq",
            round_type="series_d",
            amount="$140 million",
            amount_usd=140_000_000,
            announced_date=date(2026, 1, 11)
        )

        mock_session.execute.return_value = self.create_mock_result([original])

        # Second source (same day, different format)
        result1 = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )
        assert result1 is not None, "Same-day duplicate should be caught"

        # Third source (next day)
        result2 = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 12)
        )
        assert result2 is not None, "1-day-later duplicate should be caught"

    @pytest.mark.asyncio
    async def test_name_variation_duplicate_with_suffix(self, mock_session):
        """
        Real scenario: Company name variations with known suffixes.
        - Acme Labs
        - Acme Inc
        Same deal, different company suffix.
        FIX 2026-01: Requires 60% prefix overlap (not just 3 chars)
        """
        original = self.create_mock_deal(
            deal_id=1000,
            company_name="Acme Inc",
            round_type="seed",
            amount="$5M",
            amount_usd=5_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0 (different exact names)
            [original],  # TIER 4 (60% prefix "acme" + exact amount/date/round)
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Acme Labs",  # Different suffix but same company
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None, "Name variation should be caught by TIER 4"

    @pytest.mark.asyncio
    async def test_valuation_vs_funding_duplicate(self, mock_session):
        """
        Real scenario: Same deal reported with different amounts.
        - Source A: $330M funding (actual)
        - Source B: $6.6B valuation (incorrectly reported as funding)
        FIX 2026-01: Valuation confusion allowed when larger amount >= $500M
        """
        original = self.create_mock_deal(
            deal_id=1100,
            company_name="BigValuation Corp",
            round_type="series_c",
            amount="$330M",
            amount_usd=330_000_000,
            announced_date=date(2026, 1, 10)
        )

        self.setup_sequential_results(mock_session, [
            [],  # TIER 0
            [],  # TIER 4 (amount too different)
            [],  # TIER 3 (amount too different)
            [],  # TIER 2 exact (different day)
            [],  # TIER 2 fuzzy
            [original],  # TIER 2.5 (same round + date within 30 days, valuation confusion allowed)
            [],  # TIER 1 (not reached)
        ])

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="BigValuation Corp",
            round_type="series_c",
            amount="$6.6B",  # Valuation, not funding! (>$500M so allowed)
            announced_date=date(2026, 1, 12)  # 2 days later
        )

        assert result is not None, "Valuation/funding confusion should be caught by TIER 2.5"


# ============================================================================
# HELPER FUNCTION TESTS
# ============================================================================

class TestNormalizeAmount:
    """Test amount normalization helper function."""

    def test_millions(self):
        assert normalize_amount("$50M") == 50_000_000
        assert normalize_amount("$50 million") == 50_000_000
        assert normalize_amount("$50mn") == 50_000_000
        assert normalize_amount("$50mm") == 50_000_000

    def test_billions(self):
        assert normalize_amount("$1B") == 1_000_000_000
        assert normalize_amount("$1.5B") == 1_500_000_000
        assert normalize_amount("$2 billion") == 2_000_000_000
        assert normalize_amount("$2bn") == 2_000_000_000

    def test_thousands(self):
        assert normalize_amount("$500K") == 500_000
        assert normalize_amount("$500 thousand") == 500_000

    def test_currency_symbols(self):
        assert normalize_amount("$100M") == 100_000_000
        assert normalize_amount("€100 million") == 100_000_000
        assert normalize_amount("£50M") == 50_000_000

    def test_with_prefix(self):
        assert normalize_amount("USD $50M") == 50_000_000
        assert normalize_amount("US $50M") == 50_000_000

    def test_ranges(self):
        # Should take first number
        assert normalize_amount("$25-30 million") == 25_000_000
        assert normalize_amount("$25 to 30 million") == 25_000_000

    def test_invalid(self):
        assert normalize_amount(None) is None
        assert normalize_amount("") is None
        assert normalize_amount("undisclosed") is None
        assert normalize_amount("<UNKNOWN>") is None


class TestNormalizeCompanyName:
    """Test company name normalization helper function."""

    def test_basic(self):
        assert normalize_company_name("Torq") == "torq"
        assert normalize_company_name("TORQ") == "torq"
        assert normalize_company_name("  Torq  ") == "torq"

    def test_suffix_removal(self):
        assert normalize_company_name("Torq, Inc.") == "torq"
        assert normalize_company_name("Torq Inc") == "torq"
        assert normalize_company_name("Protege Labs") == "protege"
        assert normalize_company_name("Bluecopa AI") == "bluecopa"
        assert normalize_company_name("Tech Corp") == "tech"

    def test_the_prefix(self):
        assert normalize_company_name("The Company") == "company"
        assert normalize_company_name("The AI Lab") == "ai"

    def test_special_characters(self):
        assert normalize_company_name("Open-AI") == "openai"
        assert normalize_company_name("X.AI") == "xai"
        # FIX (2026-01): "ml" is now in suffix list, so it gets stripped
        # "AI & ML Corp" → "ai & ml" (strip corp) → "ai &" (strip ml) → "ai"
        assert normalize_company_name("AI & ML Corp") == "ai"


class TestCompanyNamesMatch:
    """Test company name matching helper function."""

    def test_exact_match(self):
        assert company_names_match("Torq", "Torq") is True
        assert company_names_match("Torq", "torq") is True
        assert company_names_match("TORQ", "torq") is True

    def test_known_suffix_match(self):
        assert company_names_match("Protege", "Protege AI") is True
        assert company_names_match("Bluecopa", "Bluecopa Tech") is True
        assert company_names_match("Ramp", "Ramp Labs") is True
        assert company_names_match("Valerie", "Valerie Health") is True

    def test_no_match_different_names(self):
        assert company_names_match("Torq", "Protege") is False
        assert company_names_match("Amazon", "Amazonia") is False
        assert company_names_match("OpenAI", "OpenAPI") is False
        assert company_names_match("Meta", "Metadata") is False

    def test_no_match_unknown_suffix(self):
        # "ia" is not a known suffix, so Amazon ≠ Amazonia
        assert company_names_match("Amazon", "Amazonia") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
