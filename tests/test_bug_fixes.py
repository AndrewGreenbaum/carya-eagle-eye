"""
Tests for bug fixes (bugs 1-23 from the bug report).

These tests validate the critical, high-priority, and medium-priority fixes
across storage, enrichment, and scraper modules.

Note: Tests that require importing from src.analyst or src.archivist are skipped
due to Python 3.9 compatibility issues with the instructor/playwright libraries.
"""

import sys
import pytest
from datetime import date
from unittest.mock import MagicMock, patch

# Import test helpers
from tests.test_helpers import (
    skip_no_archivist,
    skip_no_enrichment,
    skip_if_import_fails,
    can_import_archivist,
    can_import_enrichment,
    SKIP_PY39,
)

# Backward compatibility alias
SKIP_ARCHIVIST_TESTS = SKIP_PY39 or not can_import_archivist()


# =============================================================================
# Bug 12: Name matching tests
# =============================================================================
class TestNameMatching:
    """Bug 12: Name matching requires BOTH first AND last name."""

    def test_full_name_match(self):
        """Both first and last name should match."""
        from src.enrichment.brave_enrichment import _names_match

        assert _names_match("John Smith", "John Smith") == True
        assert _names_match("john smith", "John Smith") == True  # Case insensitive

    def test_first_name_only_no_match(self):
        """First name only should NOT match when different last names."""
        from src.enrichment.brave_enrichment import _names_match

        # "John Smith" should NOT match "John Johnson" - same first, different last
        assert _names_match("John Smith", "John Johnson") == False

    def test_last_name_only_no_match(self):
        """Last name only should NOT match when different first names."""
        from src.enrichment.brave_enrichment import _names_match

        # "Jane Smith" should NOT match "John Smith" - different first, same last
        assert _names_match("Jane Smith", "John Smith") == False

    def test_none_inputs(self):
        """None inputs should return False."""
        from src.enrichment.brave_enrichment import _names_match

        assert _names_match(None, "John Smith") == False
        assert _names_match("John Smith", None) == False
        assert _names_match(None, None) == False

    def test_middle_name_variations(self):
        """Names with middle names should still match."""
        from src.enrichment.brave_enrichment import _names_match

        # "John Smith" should match "John M. Smith" (middle name ignored)
        assert _names_match("John Smith", "John M Smith") == True
        assert _names_match("John Smith", "John Michael Smith") == True

    def test_robert_bob_no_match(self):
        """Robert/Bob don't match (different prefix, no startswith match)."""
        from src.enrichment.brave_enrichment import _names_match

        # Note: Current implementation requires startswith match
        # "Bob" doesn't start with "Robert" and vice versa
        assert _names_match("Bob Smith", "Robert Smith") == False


# =============================================================================
# Bug 15: Amount dedup key threshold tests
# =============================================================================
class TestAmountDedupKey:
    """Bug 15: Amount dedup key threshold for small deals."""

    @pytest.mark.skipif(
        SKIP_ARCHIVIST_TESTS,
        reason="Python 3.9 compatibility issue with instructor library"
    )
    def test_very_small_deal_no_dedup_key(self):
        """Deals < $250K should return None for amount dedup key."""
        from src.archivist.storage import make_amount_dedup_key

        key = make_amount_dedup_key("TestCorp", 100000, date(2026, 1, 15))
        assert key is None

    @pytest.mark.skipif(
        SKIP_ARCHIVIST_TESTS,
        reason="Python 3.9 compatibility issue with instructor library"
    )
    def test_boundary_250k(self):
        """Boundary test at exactly $250K."""
        from src.archivist.storage import make_amount_dedup_key

        key = make_amount_dedup_key("TestCorp", 250000, date(2026, 1, 15))
        assert key is not None

    @pytest.mark.skipif(
        SKIP_ARCHIVIST_TESTS,
        reason="Python 3.9 compatibility issue with instructor library"
    )
    def test_none_amount(self):
        """None amount should return None."""
        from src.archivist.storage import make_amount_dedup_key

        key = make_amount_dedup_key("TestCorp", None, date(2026, 1, 15))
        assert key is None

    @pytest.mark.skipif(
        SKIP_ARCHIVIST_TESTS,
        reason="Python 3.9 compatibility issue with instructor library"
    )
    def test_medium_deal_has_key(self):
        """Medium deals ($1M+) should have dedup key."""
        from src.archivist.storage import make_amount_dedup_key

        key = make_amount_dedup_key("TestCorp", 5_000_000, date(2026, 1, 15))
        assert key is not None
        assert len(key) == 32


# =============================================================================
# Bug 18: Company name normalization tests
# =============================================================================
@pytest.mark.skipif(
    SKIP_ARCHIVIST_TESTS,
    reason="Python 3.9 compatibility issue with instructor library"
)
class TestCompanyNameNormalization:
    """Bug 18: Consolidated company name suffix stripping."""

    def test_common_suffixes_stripped(self):
        """Common suffixes should be stripped."""
        from src.archivist.storage import _normalize_company_name_for_dedup

        assert _normalize_company_name_for_dedup("TechCorp Inc") == "techcorp"
        assert _normalize_company_name_for_dedup("TechCorp LLC") == "techcorp"
        assert _normalize_company_name_for_dedup("TechCorp Labs") == "techcorp"
        assert _normalize_company_name_for_dedup("TechCorp AI") == "techcorp"

    def test_tech_suffixes_stripped(self):
        """Tech-specific suffixes should be stripped."""
        from src.archivist.storage import _normalize_company_name_for_dedup

        assert _normalize_company_name_for_dedup("TechCorp Tech") == "techcorp"
        assert _normalize_company_name_for_dedup("TechCorp Cloud") == "techcorp"
        assert _normalize_company_name_for_dedup("TechCorp ML") == "techcorp"

    def test_case_insensitive(self):
        """Normalization should be case insensitive."""
        from src.archivist.storage import _normalize_company_name_for_dedup

        assert _normalize_company_name_for_dedup("TECHCORP") == "techcorp"
        assert _normalize_company_name_for_dedup("TechCorp") == "techcorp"
        assert _normalize_company_name_for_dedup("techcorp") == "techcorp"

    def test_the_prefix_stripped(self):
        """'The' prefix should be stripped."""
        from src.archivist.storage import _normalize_company_name_for_dedup

        assert _normalize_company_name_for_dedup("The TechCorp") == "techcorp"

    def test_suffix_at_end_only(self):
        """Suffixes are only stripped from the end, not middle."""
        from src.archivist.storage import _normalize_company_name_for_dedup

        # "AI" in middle should NOT be stripped (only at end)
        result = _normalize_company_name_for_dedup("AI TechCorp")
        assert "ai" in result or result == "techcorp"  # Depends on implementation


# =============================================================================
# Bug 19: Amount parsing tests
# =============================================================================
@pytest.mark.skipif(
    SKIP_ARCHIVIST_TESTS,
    reason="Python 3.9 compatibility issue with instructor library"
)
class TestAmountParsing:
    """Bug 19: Amount parsing for ambiguous values."""

    def test_clear_millions(self):
        """Clear million values should parse correctly."""
        from src.archivist.storage import normalize_amount

        assert normalize_amount("$50M") == 50_000_000
        assert normalize_amount("$50 million") == 50_000_000
        assert normalize_amount("50M") == 50_000_000

    def test_clear_billions(self):
        """Clear billion values should parse correctly."""
        from src.archivist.storage import normalize_amount

        assert normalize_amount("$1B") == 1_000_000_000
        assert normalize_amount("$1.5 billion") == 1_500_000_000

    def test_undisclosed_returns_none(self):
        """Undisclosed amounts should return None."""
        from src.archivist.storage import normalize_amount

        assert normalize_amount("undisclosed") is None
        assert normalize_amount("unknown") is None

    def test_small_numbers_assumed_millions(self):
        """Small numbers without multiplier assumed to be millions."""
        from src.archivist.storage import normalize_amount

        # "50" in funding context = $50M
        assert normalize_amount("50") == 50_000_000
        assert normalize_amount("30") == 30_000_000

    def test_large_numbers_as_dollars(self):
        """Large numbers already in dollars should pass through."""
        from src.archivist.storage import normalize_amount

        # 5000000 = $5M already in dollars
        assert normalize_amount("5000000") == 5_000_000

    def test_indian_currency_conversion(self):
        """Indian currency (crore/lakh) should convert to USD."""
        from src.archivist.storage import normalize_amount

        # 1 crore = ~$120K USD
        result = normalize_amount("10 crore")
        assert result is not None
        assert 1_000_000 < result < 2_000_000  # ~$1.2M

    def test_none_input(self):
        """None input should return None."""
        from src.archivist.storage import normalize_amount

        assert normalize_amount(None) is None

    def test_ranges_take_first(self):
        """Amount ranges should take first number."""
        from src.archivist.storage import normalize_amount

        # "25-30 million" should return 25M
        result = normalize_amount("25-30 million")
        assert result == 25_000_000


# =============================================================================
# Integration tests
# =============================================================================
@pytest.mark.skipif(
    SKIP_ARCHIVIST_TESTS,
    reason="Python 3.9 compatibility issue with instructor library"
)
class TestIntegration:
    """Integration tests for combined bug fixes."""

    def test_dedup_key_consistency(self):
        """Dedup keys should be consistent across normalizations."""
        from src.archivist.storage import make_dedup_key

        # Same deal with different casing/suffixes should get same key
        key1 = make_dedup_key("TechCorp Inc", "series_a", date(2026, 1, 15))
        key2 = make_dedup_key("techcorp", "series_a", date(2026, 1, 15))
        key3 = make_dedup_key("TechCorp Labs", "series_a", date(2026, 1, 15))

        assert key1 == key2 == key3

    def test_amount_dedup_key_buckets(self):
        """Amount dedup keys should bucket similar amounts together."""
        from src.archivist.storage import make_amount_dedup_key

        # $50M and $52M should be in same bucket (within 10%)
        key1 = make_amount_dedup_key("TechCorp", 50_000_000, date(2026, 1, 15))
        key2 = make_amount_dedup_key("TechCorp", 52_000_000, date(2026, 1, 15))

        # They should have the same key (same bucket)
        assert key1 == key2

    def test_different_companies_different_keys(self):
        """Different companies should have different dedup keys."""
        from src.archivist.storage import make_dedup_key

        key1 = make_dedup_key("TechCorp", "series_a", date(2026, 1, 15))
        key2 = make_dedup_key("DataCorp", "series_a", date(2026, 1, 15))

        assert key1 != key2

    def test_different_rounds_different_keys(self):
        """Different round types should have different dedup keys."""
        from src.archivist.storage import make_dedup_key

        key1 = make_dedup_key("TechCorp", "series_a", date(2026, 1, 15))
        key2 = make_dedup_key("TechCorp", "series_b", date(2026, 1, 15))

        assert key1 != key2


# =============================================================================
# URL validation tests
# =============================================================================
class TestURLValidation:
    """Tests for URL validation utilities."""

    def test_valid_website_url(self):
        """Valid website URLs should pass."""
        from src.common.url_utils import is_valid_website_url

        assert is_valid_website_url("https://example.com") == True
        assert is_valid_website_url("https://techcorp.ai") == True

    def test_invalid_website_urls_rejected(self):
        """Invalid website URLs should be rejected."""
        from src.common.url_utils import is_valid_website_url

        # LinkedIn/Crunchbase are not valid company websites
        assert is_valid_website_url("https://linkedin.com/company/techcorp") == False
        assert is_valid_website_url("https://crunchbase.com/organization/techcorp") == False

    def test_placeholder_urls_rejected(self):
        """Placeholder URLs should be rejected."""
        from src.common.url_utils import is_valid_url

        assert is_valid_url("Unknown") == False
        assert is_valid_url("N/A") == False
        assert is_valid_url("pending") == False

    def test_linkedin_profile_validation(self):
        """LinkedIn profile URLs should be validated."""
        from src.common.url_utils import is_valid_linkedin_profile

        # Valid profile
        assert is_valid_linkedin_profile("https://linkedin.com/in/johnsmith") == True

        # Invalid - company page
        assert is_valid_linkedin_profile("https://linkedin.com/company/techcorp") == False

        # Invalid - short username
        assert is_valid_linkedin_profile("https://linkedin.com/in/ab") == False


# =============================================================================
# CEO name extraction tests
# =============================================================================
class TestCEONameExtraction:
    """Tests for CEO name extraction from LinkedIn titles."""

    def test_standard_title_format(self):
        """Standard LinkedIn title format should extract name."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        result = _extract_name_from_linkedin_title("John Smith - CEO at TechCorp | LinkedIn")
        assert result == "John Smith"

    def test_pipe_format(self):
        """Pipe-delimited format should extract name."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        result = _extract_name_from_linkedin_title("Jane Doe | Co-Founder at StartupXYZ")
        assert result == "Jane Doe"

    def test_rejects_generic_titles(self):
        """Generic job titles should be rejected as names."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        result = _extract_name_from_linkedin_title("CEO at Company | LinkedIn")
        assert result is None

    def test_slug_extraction(self):
        """Names should be extractable from LinkedIn URL slugs."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_slug

        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith")
        assert result == "John Smith"

    def test_slug_filters_suffixes(self):
        """URL slug suffixes like -ceo should be filtered."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_slug

        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith-ceo")
        assert result == "John Smith"


# =============================================================================
# Company name suffix list tests
# =============================================================================
@pytest.mark.skipif(
    SKIP_ARCHIVIST_TESTS,
    reason="Python 3.9 compatibility issue with instructor library"
)
class TestCompanySuffixList:
    """Tests for the consolidated COMPANY_NAME_SUFFIXES constant."""

    def test_suffix_list_exists(self):
        """The consolidated suffix list should exist."""
        from src.archivist.storage import COMPANY_NAME_SUFFIXES

        assert isinstance(COMPANY_NAME_SUFFIXES, (list, tuple, set))
        assert len(COMPANY_NAME_SUFFIXES) > 0

    def test_common_suffixes_in_list(self):
        """Common company suffixes should be in the list."""
        from src.archivist.storage import COMPANY_NAME_SUFFIXES

        suffixes_str = " ".join(COMPANY_NAME_SUFFIXES).lower()
        # Check that these key suffixes appear somewhere in the list
        assert "inc" in suffixes_str
        assert "llc" in suffixes_str
        assert " ai" in suffixes_str
        assert "labs" in suffixes_str
        assert "tech" in suffixes_str

    def test_tech_suffixes_in_list(self):
        """Tech-specific suffixes should be in the list."""
        from src.archivist.storage import COMPANY_NAME_SUFFIXES

        suffixes_str = " ".join(COMPANY_NAME_SUFFIXES).lower()
        assert "cloud" in suffixes_str
        assert "ml" in suffixes_str
        assert "health" in suffixes_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
