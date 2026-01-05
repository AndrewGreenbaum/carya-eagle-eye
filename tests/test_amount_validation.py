"""
Tests for amount validation and deduplication update logic.

Covers:
1. _is_valid_amount() function for placeholder detection
2. normalize_amount() function for parsing
3. Amount update in deduplication flow

Note: These tests require src.archivist imports which may need Python 3.10+.
Tests will be skipped if imports fail.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import test helpers
from tests.test_helpers import skip_no_archivist, can_import_archivist

# Skip entire module if archivist imports fail (Python 3.9 compatibility)
pytestmark = [skip_no_archivist]

# Lazy imports - only executed if module isn't skipped
if can_import_archivist():
    from src.archivist.storage import _is_valid_amount, normalize_amount
else:
    # Dummy values for when imports fail (tests will be skipped anyway)
    _is_valid_amount = None
    normalize_amount = None


class TestIsValidAmount:
    """Tests for _is_valid_amount() placeholder detection."""

    # --- Should return False (placeholders/invalid) ---

    def test_none(self):
        assert _is_valid_amount(None) is False

    def test_empty_string(self):
        assert _is_valid_amount("") is False

    def test_whitespace_only(self):
        assert _is_valid_amount("   ") is False

    def test_unknown_angle_brackets(self):
        assert _is_valid_amount("<UNKNOWN>") is False

    def test_unknown_lowercase(self):
        assert _is_valid_amount("unknown") is False

    def test_unknown_uppercase(self):
        assert _is_valid_amount("UNKNOWN") is False

    def test_none_string(self):
        assert _is_valid_amount("None") is False

    def test_none_lowercase(self):
        assert _is_valid_amount("none") is False

    def test_null_string(self):
        assert _is_valid_amount("null") is False

    def test_undisclosed(self):
        assert _is_valid_amount("undisclosed") is False

    def test_undisclosed_uppercase(self):
        assert _is_valid_amount("UNDISCLOSED") is False

    def test_not_disclosed(self):
        assert _is_valid_amount("not disclosed") is False

    def test_amount_undisclosed(self):
        """Partial match for undisclosed in longer string."""
        assert _is_valid_amount("Amount undisclosed") is False

    def test_na(self):
        assert _is_valid_amount("N/A") is False

    def test_tbd(self):
        assert _is_valid_amount("TBD") is False

    def test_confidential(self):
        assert _is_valid_amount("confidential") is False

    def test_no_number(self):
        """String without any digits."""
        assert _is_valid_amount("some text") is False

    # --- Should return True (valid amounts) ---

    def test_simple_dollar_amount(self):
        assert _is_valid_amount("$50M") is True

    def test_dollar_million(self):
        assert _is_valid_amount("$50 million") is True

    def test_dollar_billion(self):
        assert _is_valid_amount("$2.5B") is True

    def test_number_only(self):
        assert _is_valid_amount("50") is True

    def test_number_with_million(self):
        assert _is_valid_amount("50 million") is True

    def test_euro_amount(self):
        assert _is_valid_amount("€100 million") is True

    def test_inr_crore(self):
        assert _is_valid_amount("₹97 crore") is True

    def test_range(self):
        assert _is_valid_amount("$25-30 million") is True

    def test_approximately(self):
        assert _is_valid_amount("approximately $50M") is True

    def test_mixed_case(self):
        assert _is_valid_amount("$50 Million") is True

    def test_with_commas(self):
        assert _is_valid_amount("$50,000,000") is True


class TestNormalizeAmount:
    """Tests for normalize_amount() parsing."""

    # --- Basic formats ---

    def test_dollar_m(self):
        assert normalize_amount("$30M") == 30_000_000

    def test_dollar_million(self):
        assert normalize_amount("$30 million") == 30_000_000

    def test_dollar_mn(self):
        assert normalize_amount("$30mn") == 30_000_000

    def test_dollar_billion(self):
        assert normalize_amount("$2.5B") == 2_500_000_000

    def test_dollar_thousand(self):
        assert normalize_amount("$500K") == 500_000

    # --- Currency symbols ---

    def test_euro(self):
        assert normalize_amount("€100 million") == 100_000_000

    def test_pound(self):
        assert normalize_amount("£50M") == 50_000_000

    def test_inr_crore(self):
        result = normalize_amount("₹97 crore")
        # 97 * 120,500 = 11,688,500
        assert 11_000_000 < result < 12_500_000

    # --- Ranges ---

    def test_range_takes_first(self):
        assert normalize_amount("$25-30 million") == 25_000_000

    def test_range_with_to(self):
        assert normalize_amount("25 to 30 million") == 25_000_000

    # --- Approximate prefixes ---

    def test_approximately(self):
        assert normalize_amount("approximately $50M") == 50_000_000

    def test_around(self):
        assert normalize_amount("around $50M") == 50_000_000

    def test_about(self):
        assert normalize_amount("about $50M") == 50_000_000

    # --- Invalid/placeholder inputs ---

    def test_none_input(self):
        assert normalize_amount(None) is None

    def test_empty_string(self):
        assert normalize_amount("") is None

    def test_unknown(self):
        assert normalize_amount("<UNKNOWN>") is None

    def test_undisclosed(self):
        assert normalize_amount("undisclosed") is None


class TestAmountUpdateLogic:
    """Tests for the amount update condition in deduplication."""

    def test_update_from_none_to_valid(self):
        """Should update when existing is None and new is valid."""
        existing = None
        new = "$50M"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is True

    def test_update_from_empty_to_valid(self):
        """Should update when existing is empty and new is valid."""
        existing = ""
        new = "$50M"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is True

    def test_update_from_unknown_to_valid(self):
        """Should update when existing is <UNKNOWN> and new is valid."""
        existing = "<UNKNOWN>"
        new = "$50M"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is True

    def test_update_from_none_string_to_valid(self):
        """Should update when existing is 'None' string and new is valid."""
        existing = "None"
        new = "$50M"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is True

    def test_update_from_undisclosed_to_valid(self):
        """Should update when existing is 'undisclosed' and new is valid."""
        existing = "undisclosed"
        new = "$50M"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is True

    def test_no_update_when_both_valid(self):
        """Should NOT update when existing already has valid amount."""
        existing = "$30M"
        new = "$50M"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is False

    def test_no_update_when_new_invalid(self):
        """Should NOT update when new amount is invalid."""
        existing = None
        new = "<UNKNOWN>"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is False

    def test_no_update_when_both_invalid(self):
        """Should NOT update when both are invalid."""
        existing = "<UNKNOWN>"
        new = "undisclosed"
        should_update = _is_valid_amount(new) and not _is_valid_amount(existing)
        assert should_update is False


class TestEdgeCases:
    """Edge cases and real-world scenarios."""

    def test_real_world_placeholders(self):
        """Real placeholder values seen in database."""
        placeholders = [
            "None",
            "<UNKNOWN>",
            "undisclosed",
            "not disclosed",
            "N/A",
        ]
        for p in placeholders:
            assert _is_valid_amount(p) is False, f"'{p}' should be invalid"

    def test_real_world_amounts(self):
        """Real amount values seen in database."""
        amounts = [
            "$50M",
            "$150 million",
            "$2.5B",
            "€35 million",
            "$25-30 million",
            "approximately $100M",
        ]
        for a in amounts:
            assert _is_valid_amount(a) is True, f"'{a}' should be valid"
            assert normalize_amount(a) is not None, f"'{a}' should normalize"

    def test_llm_output_variations(self):
        """Variations that LLM might output."""
        # Valid
        assert _is_valid_amount("$50 million USD") is True
        assert _is_valid_amount("USD 50 million") is True
        assert _is_valid_amount("50M") is True

        # Invalid
        assert _is_valid_amount("Amount not mentioned") is False
        assert _is_valid_amount("The amount was not disclosed") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
