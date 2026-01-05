"""
Comprehensive tests for CEO name extraction improvements (Jan 2026).

Tests cover:
1. Edge cases and boundary conditions
2. Unicode and special characters
3. Malformed inputs
4. Performance/efficiency
5. Integration scenarios

Note: These tests require src.enrichment imports.
Tests will be skipped if imports fail.
"""

import pytest
import sys
import os
import time
from unittest.mock import AsyncMock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import test helpers
from tests.test_helpers import skip_no_enrichment, can_import_enrichment

# Skip entire module if enrichment imports fail
pytestmark = [skip_no_enrichment]

# Lazy imports - only executed if module isn't skipped
if can_import_enrichment():
    from src.enrichment.brave_enrichment import (
        _extract_name_from_linkedin_title,
        _extract_name_from_linkedin_slug,
        BraveEnrichmentClient,
    )
else:
    # Dummy values for when imports fail (tests will be skipped anyway)
    _extract_name_from_linkedin_title = None
    _extract_name_from_linkedin_slug = None
    BraveEnrichmentClient = None


# =============================================================================
# EDGE CASES: _extract_name_from_linkedin_title
# =============================================================================

class TestTitleParsingEdgeCases:
    """Edge cases for LinkedIn title parsing."""

    # --- Empty/None inputs ---
    def test_none_input(self):
        assert _extract_name_from_linkedin_title(None) is None

    def test_empty_string(self):
        assert _extract_name_from_linkedin_title("") is None

    def test_whitespace_only(self):
        assert _extract_name_from_linkedin_title("   ") is None

    def test_single_space(self):
        assert _extract_name_from_linkedin_title(" ") is None

    # --- Single word inputs ---
    def test_single_word(self):
        assert _extract_name_from_linkedin_title("John") is None

    def test_single_word_with_delimiter(self):
        assert _extract_name_from_linkedin_title("John - CEO") is None

    # --- Unicode and special characters ---
    def test_unicode_name_accents(self):
        """Should handle accented characters."""
        result = _extract_name_from_linkedin_title("José García - CEO at Company")
        assert result == "José García"

    def test_unicode_name_chinese(self):
        """Chinese characters - fallback requires uppercase which doesn't apply."""
        # Chinese characters don't have uppercase, so fallback validation fails
        # The delimiter split works, but 王小明 only has 1 "word" when split by space
        result = _extract_name_from_linkedin_title("王小明 - CEO at Company")
        # Note: "王小明" is one word (no spaces), so it fails 2-word minimum
        assert result is None

    def test_unicode_name_arabic(self):
        """Should handle Arabic characters."""
        result = _extract_name_from_linkedin_title("محمد أحمد - CEO at Company")
        assert result == "محمد أحمد"

    def test_name_with_apostrophe(self):
        """Should handle names with apostrophes."""
        result = _extract_name_from_linkedin_title("O'Brien Smith - CEO at Company")
        assert result == "O'Brien Smith"

    def test_name_with_hyphen(self):
        """Should handle hyphenated names correctly."""
        # Note: "Mary-Jane Watson" has hyphen in name, but " - " is delimiter
        result = _extract_name_from_linkedin_title("Mary-Jane Watson - CEO at Company")
        assert result == "Mary-Jane Watson"

    # --- Multiple delimiters ---
    def test_multiple_dashes(self):
        """Multiple dashes - should take first split."""
        result = _extract_name_from_linkedin_title("John Smith - CEO - Company - LinkedIn")
        assert result == "John Smith"

    def test_mixed_delimiters(self):
        """Mixed delimiters - should use first matching."""
        result = _extract_name_from_linkedin_title("John Smith | CEO - Company")
        # ' - ' comes before ' | ' in delimiter list, but ' | ' appears first in string
        # Actually the code iterates through delimiters in order, so ' - ' is checked first
        # But "John Smith | CEO - Company".split(' - ') = ["John Smith | CEO", "Company"]
        # So it would try "John Smith | CEO" which has a pipe, so it continues
        # Then tries ' | ' which gives "John Smith"
        assert result == "John Smith"

    # --- Role words in name ---
    def test_name_containing_role_word(self):
        """Names that contain role words shouldn't be rejected if clearly a name."""
        # "Preston" contains "president" substring - should NOT be rejected
        result = _extract_name_from_linkedin_title("Preston Smith - CEO at Company")
        assert result == "Preston Smith"

    def test_name_founder_in_lastname(self):
        """Last names containing role words are rejected to avoid false positives."""
        # "Founders" as last name - rejected because it looks like a role word
        # This is a tradeoff: rare real names like "John Founders" are rejected
        # to avoid many false positives like "CEO Founder" being parsed as a name
        result = _extract_name_from_linkedin_title("John Founders - CEO at Company")
        # Rejected via delimiter split because 'founder' is in 'john founders'
        # AND rejected via fallback because 'founders' is in generic_words
        assert result is None

    # --- Very long inputs ---
    def test_very_long_title(self):
        """Should handle very long titles."""
        long_title = "A" * 100 + " " + "B" * 100 + " - CEO at Company | LinkedIn"
        result = _extract_name_from_linkedin_title(long_title)
        # Name part would be too long (>50 chars)
        assert result is None

    def test_reasonable_long_name(self):
        """Should accept reasonably long multi-word names."""
        result = _extract_name_from_linkedin_title("Alexander Sebastian Montgomery - CEO at Company")
        assert result == "Alexander Sebastian Montgomery"

    # --- Numbers in names ---
    def test_name_with_suffix_iii(self):
        """Should handle names with III suffix."""
        result = _extract_name_from_linkedin_title("John Smith III - CEO at Company")
        assert result == "John Smith III"

    def test_name_with_numbers(self):
        """Names with numbers (rare but possible)."""
        result = _extract_name_from_linkedin_title("John2 Smith - CEO at Company")
        assert result == "John2 Smith"

    # --- Lowercase edge cases ---
    def test_all_lowercase_name(self):
        """All lowercase - should fail fallback validation."""
        result = _extract_name_from_linkedin_title("john smith ceo")
        assert result is None

    def test_mixed_case_valid(self):
        """Mixed case that's valid."""
        result = _extract_name_from_linkedin_title("John SMITH - CEO")
        assert result == "John SMITH"

    # --- Special title formats ---
    def test_emoji_in_title(self):
        """Should handle emojis (skip them)."""
        result = _extract_name_from_linkedin_title("John Smith 🚀 - CEO at Company")
        assert result == "John Smith 🚀"

    def test_newline_in_title(self):
        """Should handle newlines."""
        result = _extract_name_from_linkedin_title("John Smith\n- CEO at Company")
        # Newline would break the parsing
        assert result is None or result == "John Smith"

    def test_tab_in_title(self):
        """Should handle tabs."""
        result = _extract_name_from_linkedin_title("John Smith\t- CEO at Company")
        assert result is None or "John" in (result or "")


# =============================================================================
# EDGE CASES: _extract_name_from_linkedin_slug
# =============================================================================

class TestSlugParsingEdgeCases:
    """Edge cases for LinkedIn URL slug parsing."""

    # --- Empty/None inputs ---
    def test_none_input(self):
        assert _extract_name_from_linkedin_slug(None) is None

    def test_empty_string(self):
        assert _extract_name_from_linkedin_slug("") is None

    # --- Malformed URLs ---
    def test_no_protocol(self):
        """URL without protocol."""
        result = _extract_name_from_linkedin_slug("linkedin.com/in/john-smith")
        assert result == "John Smith"

    def test_invalid_domain(self):
        """Non-LinkedIn domain."""
        result = _extract_name_from_linkedin_slug("https://notlinkedin.com/in/john-smith")
        assert result is None

    def test_company_url(self):
        """LinkedIn company URL (should fail)."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/company/acme-corp")
        assert result is None

    def test_jobs_url(self):
        """LinkedIn jobs URL (should fail)."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/jobs/view/123")
        assert result is None

    # --- Slug variations ---
    def test_underscores_instead_of_hyphens(self):
        """Underscores in slug."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john_smith")
        # Current implementation splits by hyphens only, so underscores won't work
        assert result is None

    def test_mixed_hyphens_underscores(self):
        """Mixed hyphens and underscores."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john_smith-ceo")
        # Would get ["john_smith", "ceo"], filter "ceo", left with ["john_smith"]
        # Only 1 part, fails
        assert result is None

    def test_very_long_slug(self):
        """Very long slug."""
        long_name = "john-" + "-".join(["middle"] * 20) + "-smith"
        result = _extract_name_from_linkedin_slug(f"https://linkedin.com/in/{long_name}")
        # Should get first two non-filtered parts: "john" and "middle"
        assert result == "John Middle"

    def test_all_filtered_parts(self):
        """All parts are filtered out."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/ceo-founder-mba-phd")
        assert result is None

    def test_only_numbers(self):
        """Only numbers in slug."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/12345-67890")
        assert result is None

    # --- Case handling ---
    def test_uppercase_slug(self):
        """Uppercase letters in slug (normalized by URL usually)."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/JOHN-SMITH")
        assert result == "John Smith"

    def test_mixed_case_slug(self):
        """Mixed case slug."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/John-Smith")
        assert result == "John Smith"

    # --- Special characters ---
    def test_unicode_in_slug(self):
        """Unicode characters are NOT supported in LinkedIn URL slugs.

        LinkedIn converts unicode names to ASCII equivalents:
        - José García → jose-garcia
        - Müller → muller

        Raw unicode in URLs would be percent-encoded, not raw characters.
        """
        # LinkedIn doesn't use raw unicode in slugs - they transliterate to ASCII
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/josé-garcía")
        # The regex only allows [a-zA-Z0-9_-], so unicode chars don't match
        assert result is None

    def test_percent_encoded_slug(self):
        """Percent-encoded characters."""
        # %20 is space, but LinkedIn doesn't use spaces
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john%20smith")
        # The regex won't match %20 properly
        assert result is None

    # --- Trailing characters ---
    def test_trailing_slash(self):
        """URL with trailing slash."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith/")
        assert result == "John Smith"

    def test_query_string(self):
        """URL with query string."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith?ref=nav")
        # The regex should still match the slug part
        assert result == "John Smith"

    def test_fragment(self):
        """URL with fragment."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith#about")
        assert result == "John Smith"


# =============================================================================
# BUG DETECTION TESTS
# =============================================================================

class TestPotentialBugs:
    """Tests specifically designed to find bugs."""

    def test_title_delimiter_edge_case(self):
        """Title where delimiter is part of name."""
        # "Anne-Marie" has hyphen but not " - " pattern
        result = _extract_name_from_linkedin_title("Anne-Marie Johnson - CEO")
        assert result == "Anne-Marie Johnson"

    def test_title_only_linkedin_suffix(self):
        """Title that is just 'LinkedIn'."""
        result = _extract_name_from_linkedin_title("LinkedIn")
        assert result is None

    def test_title_only_delimiter(self):
        """Title that is just a delimiter."""
        result = _extract_name_from_linkedin_title(" - ")
        assert result is None

    def test_slug_single_char_parts_only(self):
        """Slug with all single-char parts."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/j-s-m")
        assert result is None

    def test_slug_exact_boundary(self):
        """Slug with exactly 2-char parts."""
        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/jo-sm")
        assert result == "Jo Sm"

    def test_concurrent_access_safety(self):
        """Ensure functions are thread-safe (no global state mutation)."""
        import concurrent.futures

        def extract_title(i):
            return _extract_name_from_linkedin_title(f"User{i} Name{i} - CEO at Company{i}")

        def extract_slug(i):
            return _extract_name_from_linkedin_slug(f"https://linkedin.com/in/user{i}-name{i}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            title_results = list(executor.map(extract_title, range(100)))
            slug_results = list(executor.map(extract_slug, range(100)))

        # All should succeed
        for i, result in enumerate(title_results):
            assert result == f"User{i} Name{i}", f"Title failed at {i}: {result}"

        for i, result in enumerate(slug_results):
            assert result == f"User{i} Name{i}", f"Slug failed at {i}: {result}"


# =============================================================================
# PERFORMANCE TESTS
# =============================================================================

class TestPerformance:
    """Performance and efficiency tests."""

    def test_title_parsing_speed(self):
        """Title parsing should be fast."""
        title = "John Smith - CEO at Company | LinkedIn"
        start = time.perf_counter()
        for _ in range(10000):
            _extract_name_from_linkedin_title(title)
        elapsed = time.perf_counter() - start
        # Should complete 10k iterations in under 1 second
        assert elapsed < 1.0, f"Too slow: {elapsed:.3f}s for 10k iterations"

    def test_slug_parsing_speed(self):
        """Slug parsing should be fast."""
        url = "https://linkedin.com/in/john-smith-ceo"
        start = time.perf_counter()
        for _ in range(10000):
            _extract_name_from_linkedin_slug(url)
        elapsed = time.perf_counter() - start
        # Should complete 10k iterations in under 1 second
        assert elapsed < 1.0, f"Too slow: {elapsed:.3f}s for 10k iterations"

    def test_worst_case_title(self):
        """Worst case: very long title with no valid name."""
        title = " ".join(["word"] * 1000)  # 1000 words
        start = time.perf_counter()
        for _ in range(100):
            _extract_name_from_linkedin_title(title)
        elapsed = time.perf_counter() - start
        # Should still be reasonable
        assert elapsed < 1.0, f"Too slow with long input: {elapsed:.3f}s"

    def test_worst_case_slug(self):
        """Worst case: very long slug."""
        slug = "-".join(["part"] * 100)
        url = f"https://linkedin.com/in/{slug}"
        start = time.perf_counter()
        for _ in range(100):
            _extract_name_from_linkedin_slug(url)
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Too slow with long slug: {elapsed:.3f}s"


# =============================================================================
# INTEGRATION TESTS (Mock-based)
# =============================================================================

class TestFindCeoLinkedInIntegration:
    """Integration tests for find_ceo_linkedin method."""

    @pytest.mark.asyncio
    async def test_find_ceo_linkedin_with_valid_result(self):
        """Should extract name from title and URL."""
        client = BraveEnrichmentClient()

        mock_results = [
            {
                "url": "https://linkedin.com/in/john-smith-ceo",
                "title": "John Smith - CEO at Acme Corp | LinkedIn",
                "description": "CEO and founder of Acme Corp"
            }
        ]

        with patch.object(client, '_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = mock_results
            with patch.object(client, '_extract_linkedin_url', return_value="https://linkedin.com/in/john-smith-ceo"):
                name, url = await client.find_ceo_linkedin("Acme Corp")

        assert name == "John Smith"
        assert url == "https://linkedin.com/in/john-smith-ceo"

    @pytest.mark.asyncio
    async def test_find_ceo_linkedin_fallback_to_slug(self):
        """Should fallback to URL slug when title parsing fails."""
        client = BraveEnrichmentClient()

        mock_results = [
            {
                "url": "https://linkedin.com/in/jane-doe-ceo",
                "title": "CEO Profile",  # No valid name
                "description": "Jane is CEO at Acme"
            }
        ]

        with patch.object(client, '_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = mock_results
            with patch.object(client, '_extract_linkedin_url', return_value="https://linkedin.com/in/jane-doe-ceo"):
                name, url = await client.find_ceo_linkedin("Acme")

        # Should fallback to slug extraction
        assert name == "Jane Doe"
        assert url == "https://linkedin.com/in/jane-doe-ceo"

    @pytest.mark.asyncio
    async def test_find_ceo_linkedin_no_results(self):
        """Should return (None, None) when no results."""
        client = BraveEnrichmentClient()

        with patch.object(client, '_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = []
            name, url = await client.find_ceo_linkedin("NonexistentCompany")

        assert name is None
        assert url is None

    @pytest.mark.asyncio
    async def test_find_ceo_linkedin_invalid_company(self):
        """Should return (None, None) for invalid company names."""
        client = BraveEnrichmentClient()

        for invalid in ["<unknown>", "unknown", "n/a", "", None]:
            name, url = await client.find_ceo_linkedin(invalid)
            assert name is None
            assert url is None


class TestFindCeoViaWebSearchIntegration:
    """Integration tests for find_ceo_via_web_search method."""

    @pytest.mark.asyncio
    async def test_web_search_finds_linkedin_in_description(self):
        """Should find LinkedIn URL in search result description."""
        client = BraveEnrichmentClient()

        mock_results = [
            {
                "url": "https://techcrunch.com/article",
                "title": "Acme Corp raises $50M",
                "description": "CEO Bob Wilson (linkedin.com/in/bob-wilson) announced the funding"
            }
        ]

        with patch.object(client, '_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = mock_results
            with patch.object(client, '_extract_linkedin_url', side_effect=lambda x: "https://linkedin.com/in/bob-wilson" if "bob-wilson" in x else None):
                name, url = await client.find_ceo_via_web_search("Acme Corp")

        assert name == "Bob Wilson"
        assert url == "https://linkedin.com/in/bob-wilson"

    @pytest.mark.asyncio
    async def test_web_search_linkedin_url_as_result(self):
        """Should handle LinkedIn URL as the search result itself."""
        client = BraveEnrichmentClient()

        mock_results = [
            {
                "url": "https://linkedin.com/in/alice-jones",
                "title": "Alice Jones - Founder at Startup",
                "description": "Founder of Startup Inc"
            }
        ]

        with patch.object(client, '_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = mock_results
            with patch.object(client, '_extract_linkedin_url', side_effect=lambda x: x if "linkedin.com/in" in x else None):
                name, url = await client.find_ceo_via_web_search("Startup")

        assert name == "Alice Jones"
        assert url == "https://linkedin.com/in/alice-jones"


# =============================================================================
# REAL-WORLD EXAMPLES
# =============================================================================

class TestRealWorldExamples:
    """Tests with real-world LinkedIn title/URL formats."""

    @pytest.mark.parametrize("title,expected", [
        # Standard formats
        ("John Smith - CEO at Acme | LinkedIn", "John Smith"),
        ("Sarah Chen | Co-Founder - TechCorp", "Sarah Chen"),
        ("Michael Brown, Founder & CEO at StartupXYZ", "Michael Brown"),

        # International names
        ("François Dubois - CTO at EuroTech | LinkedIn", "François Dubois"),
        ("Müller Schmidt - CEO at GermanCo", "Müller Schmidt"),
        ("Tanaka Yuki - Founder at JapanAI", "Tanaka Yuki"),

        # Names with prefixes/suffixes
        ("Dr. James Wilson - CEO at HealthTech", "Dr. James Wilson"),
        ("Prof. Maria Santos - Founder at EduAI", "Prof. Maria Santos"),

        # Edge formats
        ("Bob Lee • CEO • StartupHub", "Bob Lee"),
        ("Anna Wang: Founder of AILabs", "Anna Wang"),
        ("Chris Park – CEO at DataCorp", "Chris Park"),

        # Should fail
        ("CEO at Company", None),
        ("Founder Profile | LinkedIn", None),
        ("View Profile", None),
    ])
    def test_real_titles(self, title, expected):
        assert _extract_name_from_linkedin_title(title) == expected

    @pytest.mark.parametrize("url,expected", [
        # Standard slugs
        ("https://linkedin.com/in/john-smith", "John Smith"),
        ("https://www.linkedin.com/in/sarah-chen", "Sarah Chen"),
        ("http://linkedin.com/in/bob-wilson", "Bob Wilson"),

        # With suffixes
        ("https://linkedin.com/in/john-smith-ceo", "John Smith"),
        ("https://linkedin.com/in/sarah-chen-founder-mba", "Sarah Chen"),
        ("https://linkedin.com/in/bob-wilson-phd-1", "Bob Wilson"),

        # International names (LinkedIn converts to ASCII)
        ("https://linkedin.com/in/jose-garcia", "Jose Garcia"),  # José → jose
        ("https://linkedin.com/in/muller-schmidt", "Muller Schmidt"),  # Müller → muller

        # Should fail
        ("https://linkedin.com/in/jsmith", None),  # No hyphen
        ("https://linkedin.com/company/acme", None),  # Company URL
        ("https://twitter.com/johnsmith", None),  # Wrong domain
        ("https://notlinkedin.com/in/john-smith", None),  # Fake domain
    ])
    def test_real_urls(self, url, expected):
        assert _extract_name_from_linkedin_slug(url) == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
