"""
Tests for CEO name extraction improvements (Jan 2026).

Tests:
1. _extract_name_from_linkedin_title() - Robust title parsing
2. _extract_name_from_linkedin_slug() - URL slug fallback
3. Integration tests for find_ceo_linkedin improvements

Note: These tests require src.enrichment imports.
Tests will be skipped if imports fail.
"""

import pytest
import sys
import os

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
    )
else:
    # Dummy values for when imports fail (tests will be skipped anyway)
    _extract_name_from_linkedin_title = None
    _extract_name_from_linkedin_slug = None


class TestExtractNameFromLinkedInTitle:
    """Test robust name parsing from LinkedIn search result titles."""

    def test_standard_dash_format(self):
        """Standard format: Name - Title | LinkedIn"""
        assert _extract_name_from_linkedin_title("John Smith - CEO at Company | LinkedIn") == "John Smith"

    def test_pipe_format(self):
        """Pipe format: Name | Title"""
        assert _extract_name_from_linkedin_title("Sarah Chen | CEO - Startup Inc") == "Sarah Chen"

    def test_comma_format(self):
        """Comma format: Name, Title"""
        assert _extract_name_from_linkedin_title("Michael Johnson, Founder & CEO at TechCo") == "Michael Johnson"

    def test_bullet_format(self):
        """Bullet format: Name • Title"""
        assert _extract_name_from_linkedin_title("Emily Davis • Co-founder at StartupXYZ") == "Emily Davis"

    def test_em_dash_format(self):
        """Em dash format: Name – Title"""
        assert _extract_name_from_linkedin_title("David Lee – CTO at AI Company") == "David Lee"

    def test_colon_format(self):
        """Colon format: Name: Title"""
        assert _extract_name_from_linkedin_title("Anna Wang: Founder & CEO") == "Anna Wang"

    def test_linkedin_suffix_removed(self):
        """Should strip | LinkedIn suffix before parsing."""
        assert _extract_name_from_linkedin_title("Bob Wilson - Engineer | LinkedIn") == "Bob Wilson"

    def test_dash_linkedin_suffix_removed(self):
        """Should strip - LinkedIn suffix before parsing."""
        assert _extract_name_from_linkedin_title("Lisa Park - Data Scientist - LinkedIn") == "Lisa Park"

    def test_fallback_first_two_words(self):
        """Fallback to first two capitalized words if no delimiter."""
        assert _extract_name_from_linkedin_title("Tom Baker CEO") == "Tom Baker"

    def test_rejects_role_as_name(self):
        """Should reject if first part looks like a role, not a name."""
        assert _extract_name_from_linkedin_title("CEO and Founder - John Smith") is None

    def test_rejects_single_word(self):
        """Should reject single-word names."""
        assert _extract_name_from_linkedin_title("John - CEO") is None

    def test_rejects_empty_input(self):
        """Should handle empty input."""
        assert _extract_name_from_linkedin_title("") is None
        assert _extract_name_from_linkedin_title(None) is None

    def test_rejects_too_long_name(self):
        """Should reject overly long name parts."""
        long_title = "A" * 60 + " - CEO at Company"
        assert _extract_name_from_linkedin_title(long_title) is None

    def test_three_word_name(self):
        """Should handle three-word names."""
        assert _extract_name_from_linkedin_title("Mary Jane Watson - CEO at Company") == "Mary Jane Watson"

    def test_case_sensitivity_fallback(self):
        """Fallback requires capitalized words."""
        assert _extract_name_from_linkedin_title("john smith ceo") is None


class TestExtractNameFromLinkedInSlug:
    """Test name extraction from LinkedIn URL slugs."""

    def test_standard_slug(self):
        """Standard hyphenated slug."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith") == "John Smith"

    def test_with_www(self):
        """URL with www prefix."""
        assert _extract_name_from_linkedin_slug("https://www.linkedin.com/in/sarah-chen") == "Sarah Chen"

    def test_http_url(self):
        """HTTP URL (not HTTPS)."""
        assert _extract_name_from_linkedin_slug("http://linkedin.com/in/michael-johnson") == "Michael Johnson"

    def test_filters_ceo_suffix(self):
        """Should filter out role suffixes."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith-ceo") == "John Smith"

    def test_filters_founder_suffix(self):
        """Should filter out founder suffix."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/sarah-chen-founder") == "Sarah Chen"

    def test_filters_mba_suffix(self):
        """Should filter out MBA suffix."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/bob-wilson-mba") == "Bob Wilson"

    def test_filters_numbers(self):
        """Should filter out trailing numbers."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith-123") == "John Smith"

    def test_rejects_username_only(self):
        """Should reject single-word usernames (no last name)."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/jsmith123") is None

    def test_rejects_too_short_parts(self):
        """Should reject if name parts are too short."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/j-s") is None

    def test_rejects_empty_input(self):
        """Should handle empty input."""
        assert _extract_name_from_linkedin_slug("") is None
        assert _extract_name_from_linkedin_slug(None) is None

    def test_rejects_non_linkedin_url(self):
        """Should reject non-LinkedIn URLs."""
        assert _extract_name_from_linkedin_slug("https://twitter.com/johnsmith") is None

    def test_rejects_company_url(self):
        """Should reject LinkedIn company URLs."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/company/acme-corp") is None

    def test_three_part_name(self):
        """Should take first two parts for three-part slugs."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/mary-jane-watson") == "Mary Jane"

    def test_filters_multiple_suffixes(self):
        """Should filter multiple role/credential suffixes."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith-cto-mba") == "John Smith"

    def test_case_normalization(self):
        """Should capitalize names properly."""
        assert _extract_name_from_linkedin_slug("https://linkedin.com/in/JOHN-SMITH") == "John Smith"


class TestIntegration:
    """Integration tests combining title and slug extraction."""

    def test_title_preferred_over_slug(self):
        """Title extraction should be preferred when both work."""
        title = "John Smith - CEO at Acme"
        url = "https://linkedin.com/in/john-smith-ceo"

        # Title gives full name with proper casing
        name_from_title = _extract_name_from_linkedin_title(title)
        name_from_slug = _extract_name_from_linkedin_slug(url)

        assert name_from_title == "John Smith"
        assert name_from_slug == "John Smith"

    def test_slug_fallback_when_title_fails(self):
        """Slug extraction should work when title parsing fails."""
        title = "CEO Profile"  # No valid name
        url = "https://linkedin.com/in/sarah-chen"

        name_from_title = _extract_name_from_linkedin_title(title)
        name_from_slug = _extract_name_from_linkedin_slug(url)

        assert name_from_title is None
        assert name_from_slug == "Sarah Chen"

    def test_both_fail_gracefully(self):
        """Should handle cases where both methods fail."""
        title = "Company Page"
        url = "https://linkedin.com/company/acme"

        name_from_title = _extract_name_from_linkedin_title(title)
        name_from_slug = _extract_name_from_linkedin_slug(url)

        assert name_from_title is None
        assert name_from_slug is None


class TestEdgeCases:
    """Edge cases and real-world examples."""

    def test_real_linkedin_title_1(self):
        """Real LinkedIn title format 1."""
        title = "Jane Doe - Co-Founder & CEO - StartupABC | LinkedIn"
        assert _extract_name_from_linkedin_title(title) == "Jane Doe"

    def test_real_linkedin_title_2(self):
        """Real LinkedIn title format 2."""
        title = "Dr. Robert Chen | Stanford Professor & AI Researcher"
        assert _extract_name_from_linkedin_title(title) == "Dr. Robert Chen"

    def test_real_linkedin_title_3(self):
        """Real LinkedIn title format with bullet."""
        title = "Maria Garcia • Founding Engineer at TechStartup"
        assert _extract_name_from_linkedin_title(title) == "Maria Garcia"

    def test_real_slug_with_middle_name(self):
        """Real slug with middle initial - should skip single letters."""
        url = "https://linkedin.com/in/john-q-smith"
        # Single-letter middle initials are skipped for reliability
        assert _extract_name_from_linkedin_slug(url) == "John Smith"

    def test_real_slug_with_credentials(self):
        """Real slug with multiple credentials."""
        url = "https://linkedin.com/in/sarah-jones-phd-mba-1"
        assert _extract_name_from_linkedin_slug(url) == "Sarah Jones"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
