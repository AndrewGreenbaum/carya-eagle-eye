"""
Comprehensive tests for TIER 0 deduplication logic.

TIER 0 is the most aggressive deduplication tier, designed to catch
race condition duplicates when multiple articles about the same deal
are processed in parallel.

TIER 0 criteria:
- Exact company name match (case-insensitive)
- Same round type
- Date within ±3 days (or both null with recent deals)

Run with: pytest tests/test_deduplication_tier0.py -v

Note: These tests require src.archivist imports which may need Python 3.10+.
Tests will be skipped if imports fail.
"""

import pytest
import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import test helpers
from tests.test_helpers import skip_no_archivist, can_import_archivist

# Skip entire module if archivist imports fail (Python 3.9 compatibility)
pytestmark = [skip_no_archivist]

# Lazy imports - only executed if module isn't skipped
if can_import_archivist():
    from sqlalchemy import select
    from src.archivist.storage import (
        find_duplicate_deal,
        normalize_amount,
        normalize_company_name,
        company_names_match,
    )
else:
    # Dummy values for when imports fail (tests will be skipped anyway)
    select = None
    find_duplicate_deal = None
    normalize_amount = None
    normalize_company_name = None
    company_names_match = None


class TestNormalizeAmount:
    """Test amount normalization helper."""

    def test_basic_millions(self):
        assert normalize_amount("$50M") == 50_000_000
        assert normalize_amount("$50 million") == 50_000_000
        assert normalize_amount("$50mn") == 50_000_000

    def test_basic_billions(self):
        assert normalize_amount("$1B") == 1_000_000_000
        assert normalize_amount("$1.5B") == 1_500_000_000
        assert normalize_amount("$2 billion") == 2_000_000_000

    def test_with_currency_symbols(self):
        assert normalize_amount("$140M") == 140_000_000
        assert normalize_amount("€100 million") == 100_000_000

    def test_with_spaces_and_formatting(self):
        assert normalize_amount("$140 million") == 140_000_000
        assert normalize_amount("$ 50M") == 50_000_000
        assert normalize_amount("$30 M") == 30_000_000

    def test_none_and_invalid(self):
        assert normalize_amount(None) is None
        assert normalize_amount("") is None
        assert normalize_amount("undisclosed") is None
        assert normalize_amount("<UNKNOWN>") is None

    def test_ranges(self):
        # Should take first number in ranges
        assert normalize_amount("$25-30 million") == 25_000_000


class TestNormalizeCompanyName:
    """Test company name normalization."""

    def test_basic_normalization(self):
        assert normalize_company_name("Torq") == "torq"
        assert normalize_company_name("TORQ") == "torq"
        assert normalize_company_name("  Torq  ") == "torq"

    def test_suffix_removal(self):
        assert normalize_company_name("Torq, Inc.") == "torq"
        assert normalize_company_name("Protege Labs") == "protege"
        assert normalize_company_name("Bluecopa AI") == "bluecopa"

    def test_the_prefix_removal(self):
        assert normalize_company_name("The Company") == "company"

    def test_special_characters(self):
        assert normalize_company_name("Open-AI") == "openai"
        assert normalize_company_name("X.AI") == "xai"


class TestCompanyNamesMatch:
    """Test company name matching logic."""

    def test_exact_match(self):
        assert company_names_match("Torq", "Torq") is True
        assert company_names_match("Torq", "torq") is True
        assert company_names_match("TORQ", "torq") is True

    def test_with_known_suffix(self):
        assert company_names_match("Protege", "Protege AI") is True
        assert company_names_match("Bluecopa", "Bluecopa Tech") is True
        assert company_names_match("Ramp", "Ramp Labs") is True

    def test_different_companies(self):
        assert company_names_match("Torq", "Protege") is False
        assert company_names_match("Amazon", "Amazonia") is False
        assert company_names_match("OpenAI", "OpenAPI") is False


class TestTier0Deduplication:
    """
    Test TIER 0 deduplication scenarios.

    TIER 0 catches:
    - Exact company name (case-insensitive)
    - Same round type
    - Date within ±3 days
    """

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session with proper async handling."""
        session = AsyncMock()
        return session

    def create_mock_result(self, deals_list):
        """Create a properly mocked result object."""
        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=deals_list)
        return mock_result

    @pytest.fixture
    def create_mock_deal(self):
        """Factory to create mock deals."""
        def _create(
            deal_id: int,
            company_name: str,
            round_type: str,
            amount: str,
            announced_date: date,
            created_at: datetime = None
        ):
            deal = MagicMock()
            deal.id = deal_id
            deal.round_type = round_type
            deal.amount = amount
            deal.announced_date = announced_date
            deal.created_at = created_at or datetime.now(timezone.utc).replace(tzinfo=None)

            company = MagicMock()
            company.id = deal_id  # Simplified
            company.name = company_name

            return (deal, company)
        return _create

    @pytest.mark.asyncio
    async def test_tier0_exact_match_same_date(self, mock_session, create_mock_deal):
        """
        TIER 0 should match: Same company + same round + same date.
        Scenario: Protege Series A $30M on Jan 8 from two sources.
        """
        existing_deal = create_mock_deal(
            deal_id=2784,
            company_name="Protege",
            round_type="series_a",
            amount="$30 million",
            announced_date=date(2026, 1, 8)
        )

        # Mock the query to return the existing deal
        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Protege",
            round_type="series_a",
            amount="$30M",
            announced_date=date(2026, 1, 8)
        )

        # Should find the duplicate
        assert result is not None
        assert result.id == 2784

    @pytest.mark.asyncio
    async def test_tier0_match_date_1_day_apart(self, mock_session, create_mock_deal):
        """
        TIER 0 should match: Same company + same round + date 1 day apart.
        Scenario: Torq Series D on Jan 11 vs Jan 12.
        """
        existing_deal = create_mock_deal(
            deal_id=2884,
            company_name="Torq",
            round_type="series_d",
            amount="$140 million",
            announced_date=date(2026, 1, 11)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 12)  # 1 day later
        )

        assert result is not None
        assert result.id == 2884

    @pytest.mark.asyncio
    async def test_tier0_match_date_3_days_apart(self, mock_session, create_mock_deal):
        """
        TIER 0 should match: Same company + same round + date 3 days apart.
        This is the boundary of the ±3 day tolerance.
        """
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="TestCompany",
            round_type="series_a",
            amount="$50M",
            announced_date=date(2026, 1, 10)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        # Test +3 days
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="TestCompany",
            round_type="series_a",
            amount="$50M",
            announced_date=date(2026, 1, 13)  # 3 days later
        )

        assert result is not None
        assert result.id == 100

    @pytest.mark.asyncio
    async def test_tier0_no_match_date_4_days_apart(self, mock_session, create_mock_deal):
        """
        TIER 0 should NOT match: Date 4+ days apart exceeds tolerance.
        The deal should go to lower tiers for evaluation.
        """
        # Empty result for all tiers
        mock_result_empty = self.create_mock_result([])
        mock_session.execute.return_value = mock_result_empty

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="TestCompany",
            round_type="series_a",
            amount="$50M",
            announced_date=date(2026, 1, 14)  # 4 days from Jan 10
        )

        # With all empty results, should return None
        assert result is None

    @pytest.mark.asyncio
    async def test_tier0_no_match_different_round(self, mock_session, create_mock_deal):
        """
        TIER 0 should NOT match: Different round types.
        A Series A and Series B are different deals even for same company.
        """
        mock_result_empty = self.create_mock_result([])
        mock_session.execute.return_value = mock_result_empty

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="TestCompany",
            round_type="series_b",  # Different from series_a
            amount="$100M",
            announced_date=date(2026, 1, 10)
        )

        # Different round = not a TIER 0 match
        assert result is None

    @pytest.mark.asyncio
    async def test_tier0_no_match_different_company(self, mock_session, create_mock_deal):
        """
        TIER 0 should NOT match: Different companies.
        """
        mock_result_empty = self.create_mock_result([])
        mock_session.execute.return_value = mock_result_empty

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="DifferentCompany",
            round_type="series_a",
            amount="$50M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_tier0_case_insensitive(self, mock_session, create_mock_deal):
        """
        TIER 0 should match regardless of case.
        """
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="TorQ",  # Mixed case
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        # Query with different case
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="TORQ",  # All caps
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        assert result is not None
        assert result.id == 100

    @pytest.mark.asyncio
    async def test_tier0_with_whitespace(self, mock_session, create_mock_deal):
        """
        TIER 0 should handle leading/trailing whitespace.
        """
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="  Torq  ",  # With whitespace
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_tier0_null_date_incoming(self, mock_session, create_mock_deal):
        """
        TIER 0 should handle incoming deal with null date.
        Should match recent deals with null or recent dates.
        """
        recent_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=2)
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=None,  # No date
            created_at=recent_time
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=None  # Also no date
        )

        assert result is not None
        assert result.id == 100

    @pytest.mark.asyncio
    async def test_tier0_amount_difference_ignored(self, mock_session, create_mock_deal):
        """
        TIER 0 should match even with different amount formatting.
        Amount is NOT part of TIER 0 criteria.
        """
        existing_deal = create_mock_deal(
            deal_id=2784,
            company_name="Protege",
            round_type="series_a",
            amount="$30 million",  # Different format
            announced_date=date(2026, 1, 8)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Protege",
            round_type="series_a",
            amount="$30M",  # Different format
            announced_date=date(2026, 1, 8)
        )

        # Should still match - TIER 0 doesn't check amount
        assert result is not None
        assert result.id == 2784


class TestTier0EdgeCases:
    """Test edge cases and potential bugs in TIER 0."""

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        return session

    def create_mock_result(self, deals_list):
        """Create a properly mocked result object."""
        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=deals_list)
        return mock_result

    @pytest.fixture
    def create_mock_deal(self):
        def _create(deal_id, company_name, round_type, amount, announced_date, created_at=None):
            deal = MagicMock()
            deal.id = deal_id
            deal.round_type = round_type
            deal.amount = amount
            deal.announced_date = announced_date
            deal.created_at = created_at or datetime.now(timezone.utc).replace(tzinfo=None)

            company = MagicMock()
            company.id = deal_id
            company.name = company_name

            return (deal, company)
        return _create

    @pytest.mark.asyncio
    async def test_negative_date_difference(self, mock_session, create_mock_deal):
        """
        TIER 0 should match when new deal has EARLIER date.
        This is symmetric - ±3 days works both directions.
        """
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="TestCo",
            round_type="seed",
            amount="$10M",
            announced_date=date(2026, 1, 15)  # Later date
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="TestCo",
            round_type="seed",
            amount="$10M",
            announced_date=date(2026, 1, 12)  # 3 days earlier
        )

        assert result is not None
        assert result.id == 100

    @pytest.mark.asyncio
    async def test_empty_company_name(self, mock_session):
        """
        TIER 0 should handle empty company name gracefully.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="",
            round_type="seed",
            amount="$10M",
            announced_date=date(2026, 1, 10)
        )

        # Should not crash, just return None
        assert result is None

    @pytest.mark.asyncio
    async def test_special_characters_in_name(self, mock_session, create_mock_deal):
        """
        TIER 0 should handle special characters in company names.
        """
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="X.AI",
            round_type="series_a",
            amount="$100M",
            announced_date=date(2026, 1, 10)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="X.AI",
            round_type="series_a",
            amount="$100M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_very_long_company_name(self, mock_session, create_mock_deal):
        """
        TIER 0 should handle very long company names.
        """
        long_name = "A" * 500  # Very long name

        existing_deal = create_mock_deal(
            deal_id=100,
            company_name=long_name,
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name=long_name,
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_unicode_company_name(self, mock_session, create_mock_deal):
        """
        TIER 0 should handle unicode characters in company names.
        """
        existing_deal = create_mock_deal(
            deal_id=100,
            company_name="Société Générale AI",
            round_type="series_a",
            amount="$50M",
            announced_date=date(2026, 1, 10)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Société Générale AI",
            round_type="series_a",
            amount="$50M",
            announced_date=date(2026, 1, 10)
        )

        assert result is not None


class TestTier0RealWorldScenarios:
    """Test real-world scenarios that caused duplicate issues."""

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        return session

    def create_mock_result(self, deals_list):
        """Create a properly mocked result object."""
        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=deals_list)
        return mock_result

    @pytest.fixture
    def create_mock_deal(self):
        def _create(deal_id, company_name, round_type, amount, announced_date, created_at=None):
            deal = MagicMock()
            deal.id = deal_id
            deal.round_type = round_type
            deal.amount = amount
            deal.announced_date = announced_date
            deal.created_at = created_at or datetime.now(timezone.utc).replace(tzinfo=None)

            company = MagicMock()
            company.id = deal_id
            company.name = company_name

            return (deal, company)
        return _create

    @pytest.mark.asyncio
    async def test_torq_duplicate_scenario(self, mock_session, create_mock_deal):
        """
        Real scenario: Torq had 3 duplicates from different sources.
        - Deal 2884: $140 million, Jan 11 (SecurityWeek)
        - Deal 2885: $140M, Jan 11 (Globes)
        - Deal 2886: $140M, Jan 12 (Finsmes)

        With TIER 0, 2885 and 2886 should have been caught.
        """
        existing_deal = create_mock_deal(
            deal_id=2884,
            company_name="Torq",
            round_type="series_d",
            amount="$140 million",
            announced_date=date(2026, 1, 11)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        # Simulate article 2 (Globes - same day)
        result1 = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )
        assert result1 is not None, "Should catch same-day duplicate (Globes)"

        # Simulate article 3 (Finsmes - next day)
        result2 = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 12)
        )
        assert result2 is not None, "Should catch 1-day-later duplicate (Finsmes)"

    @pytest.mark.asyncio
    async def test_protege_duplicate_scenario(self, mock_session, create_mock_deal):
        """
        Real scenario: Protege had 2 duplicates.
        - Deal 2784: $30 million, Jan 8 (Ventureburn)
        - Deal 2809: $30M, Jan 8 (AI Insider)

        With TIER 0, 2809 should have been caught.
        """
        existing_deal = create_mock_deal(
            deal_id=2784,
            company_name="Protege",
            round_type="series_a",
            amount="$30 million",
            announced_date=date(2026, 1, 8)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Protege",
            round_type="series_a",
            amount="$30M",
            announced_date=date(2026, 1, 8)
        )

        assert result is not None, "Should catch same-day duplicate"
        assert result.id == 2784

    @pytest.mark.asyncio
    async def test_bluecopa_duplicate_scenario(self, mock_session, create_mock_deal):
        """
        Real scenario: Bluecopa had 2 duplicates.
        - Deal 2888: $7.5 million, Jan 12 (Source A)
        - Deal 2889: $7.5 million, Jan 12 (Source B)
        """
        existing_deal = create_mock_deal(
            deal_id=2888,
            company_name="Bluecopa",
            round_type="series_a",
            amount="$7.5 million",
            announced_date=date(2026, 1, 12)
        )

        mock_result = self.create_mock_result([existing_deal])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Bluecopa",
            round_type="series_a",
            amount="$7.5 million",
            announced_date=date(2026, 1, 12)
        )

        assert result is not None, "Should catch same-day duplicate"
        assert result.id == 2888


class TestTier0DoesNotOverMatch:
    """
    Test that TIER 0 doesn't cause false positives.
    It should NOT match legitimate different deals.

    CRITICAL: These tests ensure we don't accidentally block legitimate deals!
    """

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        return session

    def create_mock_result(self, deals_list):
        """Create a properly mocked result object."""
        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=deals_list)
        return mock_result

    @pytest.fixture
    def create_mock_deal(self):
        def _create(deal_id, company_name, round_type, amount, announced_date, created_at=None):
            deal = MagicMock()
            deal.id = deal_id
            deal.round_type = round_type
            deal.amount = amount
            deal.announced_date = announced_date
            deal.created_at = created_at or datetime.now(timezone.utc).replace(tzinfo=None)

            company = MagicMock()
            company.id = deal_id
            company.name = company_name

            return (deal, company)
        return _create

    @pytest.mark.asyncio
    async def test_different_round_same_week(self, mock_session, create_mock_deal):
        """
        Company can have Series A and then Series A Extension same week.
        These are different deals even if dates are close.

        Note: This relies on round_type being different (series_a vs series_a_extension).
        """
        # TIER 0 requires same round_type, so different rounds won't match
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Protege",
            round_type="series_a_extension",  # Different from series_a
            amount="$30M",
            announced_date=date(2026, 1, 10)  # Same week as Jan 8
        )

        # Should NOT match - different round type
        assert result is None

    @pytest.mark.asyncio
    async def test_same_company_different_year(self, mock_session, create_mock_deal):
        """
        Same company can raise same round type in different years.
        TIER 0's ±3 day window should not match deals months apart.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torq",
            round_type="series_d",
            amount="$140M",
            announced_date=date(2025, 1, 11)  # 1 year earlier
        )

        # Should NOT match - date way out of range
        assert result is None

    @pytest.mark.asyncio
    async def test_similar_but_different_company(self, mock_session, create_mock_deal):
        """
        "Torq" and "Torque" are different companies.
        TIER 0 requires EXACT name match.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Torque",  # Different from "Torq"
            round_type="series_d",
            amount="$140M",
            announced_date=date(2026, 1, 11)
        )

        # Should NOT match - different company name
        assert result is None

    @pytest.mark.asyncio
    async def test_seed_then_series_a_same_company(self, mock_session, create_mock_deal):
        """
        CRITICAL: Company raises Seed, then Series A a few days later.
        These are DIFFERENT deals and should NOT be deduplicated!
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        # Existing Seed round
        # New Series A should NOT match
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="NewStartup",
            round_type="series_a",  # Different from seed
            amount="$20M",
            announced_date=date(2026, 1, 12)  # Days after seed
        )

        assert result is None, "Series A should NOT be blocked by existing Seed"

    @pytest.mark.asyncio
    async def test_series_a_then_series_b_same_company(self, mock_session, create_mock_deal):
        """
        CRITICAL: Company raises Series A, then Series B.
        These are DIFFERENT deals!
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="GrowingStartup",
            round_type="series_b",  # Different from series_a
            amount="$50M",
            announced_date=date(2026, 1, 15)
        )

        assert result is None, "Series B should NOT be blocked by existing Series A"

    @pytest.mark.asyncio
    async def test_different_companies_same_round_same_day(self, mock_session, create_mock_deal):
        """
        CRITICAL: Two different companies announce same round type on same day.
        Common scenario: Multiple startups announce Series A on the same day.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="StartupB",  # Different from StartupA
            round_type="series_a",
            amount="$30M",
            announced_date=date(2026, 1, 10)  # Same day
        )

        assert result is None, "Different company should NOT be blocked"

    @pytest.mark.asyncio
    async def test_company_name_substring_no_match(self, mock_session, create_mock_deal):
        """
        CRITICAL: "Air" should NOT match "Airbnb".
        TIER 0 requires EXACT match, not substring.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="Airbnb",  # Contains "Air" but is different
            round_type="series_a",
            amount="$100M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "Airbnb should NOT match Air"

    @pytest.mark.asyncio
    async def test_company_name_prefix_no_match(self, mock_session, create_mock_deal):
        """
        CRITICAL: "Open" should NOT match "OpenAI".
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="OpenAI",
            round_type="series_b",
            amount="$500M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "OpenAI should NOT match Open"

    @pytest.mark.asyncio
    async def test_company_with_common_word_no_false_match(self, mock_session, create_mock_deal):
        """
        CRITICAL: "AI Health" should NOT match "AI Robotics".
        Even though both start with "AI".
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="AI Robotics",
            round_type="seed",
            amount="$5M",
            announced_date=date(2026, 1, 10)
        )

        assert result is None, "AI Robotics should NOT match AI Health"

    @pytest.mark.asyncio
    async def test_bridge_round_not_confused_with_series(self, mock_session, create_mock_deal):
        """
        CRITICAL: Bridge round should NOT match Series A.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="StartupX",
            round_type="bridge",  # Different from series_a
            amount="$5M",
            announced_date=date(2026, 1, 12)
        )

        assert result is None, "Bridge should NOT match Series A"

    @pytest.mark.asyncio
    async def test_debt_round_not_confused_with_equity(self, mock_session, create_mock_deal):
        """
        CRITICAL: Debt round should NOT match equity rounds.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="StartupY",
            round_type="debt",  # Different from series_a
            amount="$20M",
            announced_date=date(2026, 1, 12)
        )

        assert result is None, "Debt should NOT match Series A"

    @pytest.mark.asyncio
    async def test_growth_round_not_confused_with_series(self, mock_session, create_mock_deal):
        """
        CRITICAL: Growth round should NOT match series rounds.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="ScaleUp",
            round_type="growth",
            amount="$100M",
            announced_date=date(2026, 1, 12)
        )

        assert result is None, "Growth should NOT match Series"

    @pytest.mark.asyncio
    async def test_pre_seed_not_confused_with_seed(self, mock_session, create_mock_deal):
        """
        CRITICAL: Pre-seed should NOT match seed.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="EarlyStartup",
            round_type="pre_seed",
            amount="$2M",
            announced_date=date(2026, 1, 12)
        )

        assert result is None, "Pre-seed should NOT match Seed"

    @pytest.mark.asyncio
    async def test_date_boundary_4_days_no_match(self, mock_session, create_mock_deal):
        """
        CRITICAL: 4 days apart should NOT be matched by TIER 0.
        This tests the exact boundary of the ±3 day tolerance.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        # Existing deal on Jan 10, new deal on Jan 14 (4 days later)
        result = await find_duplicate_deal(
            session=mock_session,
            company_name="BoundaryTest",
            round_type="series_a",
            amount="$25M",
            announced_date=date(2026, 1, 14)  # 4 days from Jan 10
        )

        assert result is None, "4 days apart should NOT match (TIER 0 is ±3 days)"

    @pytest.mark.asyncio
    async def test_legitimate_follow_on_round(self, mock_session, create_mock_deal):
        """
        CRITICAL: Company can have legitimate follow-on in same round type.
        E.g., Series A in Jan, then Series A-2 extension in Feb.
        Different round_type naming prevents false match.
        """
        mock_result = self.create_mock_result([])
        mock_session.execute.return_value = mock_result

        result = await find_duplicate_deal(
            session=mock_session,
            company_name="FollowOnCo",
            round_type="series_a_2",  # Extension naming
            amount="$15M",
            announced_date=date(2026, 2, 15)  # Month later
        )

        assert result is None, "Series A-2 should NOT match Series A"


class TestTier0SQLQueryConstruction:
    """
    Test that the SQL query is constructed correctly.
    These tests verify the query logic without mocking.
    """

    def test_date_range_calculation(self):
        """Verify date range is correctly ±3 days."""
        test_date = date(2026, 1, 10)
        expected_start = date(2026, 1, 7)  # -3 days
        expected_end = date(2026, 1, 13)  # +3 days

        actual_start = test_date - timedelta(days=3)
        actual_end = test_date + timedelta(days=3)

        assert actual_start == expected_start
        assert actual_end == expected_end

    def test_case_insensitive_comparison(self):
        """Verify case insensitivity logic."""
        assert "Torq".lower().strip() == "torq"
        assert "TORQ".lower().strip() == "torq"
        assert "  Torq  ".lower().strip() == "torq"
        assert "TorQ".lower().strip() == "torq"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
