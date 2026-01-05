"""
Additional validation function tests.

Tests for URL utilities, enrichment helpers, and scraper patterns
that work on Python 3.9 (no instructor library dependency).
"""

import pytest
from datetime import date


# =============================================================================
# URL Utilities Tests
# =============================================================================
class TestURLSanitization:
    """Tests for URL sanitization functions."""

    def test_sanitize_url_adds_https(self):
        """HTTP URLs should be upgraded to HTTPS."""
        from src.common.url_utils import sanitize_url

        result = sanitize_url("http://example.com")
        assert result == "https://example.com"

    def test_sanitize_url_adds_protocol(self):
        """URLs without protocol should get https://."""
        from src.common.url_utils import sanitize_url

        result = sanitize_url("www.example.com")
        assert result.startswith("https://")

    def test_sanitize_url_preserves_https(self):
        """HTTPS URLs should be preserved."""
        from src.common.url_utils import sanitize_url

        result = sanitize_url("https://example.com")
        assert result == "https://example.com"

    def test_sanitize_url_strips_whitespace(self):
        """Whitespace should be stripped."""
        from src.common.url_utils import sanitize_url

        result = sanitize_url("  https://example.com  ")
        assert result == "https://example.com"

    def test_sanitize_url_none_returns_none(self):
        """None input should return None."""
        from src.common.url_utils import sanitize_url

        result = sanitize_url(None)
        assert result is None

    def test_sanitize_url_empty_returns_none(self):
        """Empty string should return None."""
        from src.common.url_utils import sanitize_url

        result = sanitize_url("")
        assert result is None


class TestLinkedInURLValidation:
    """Tests for LinkedIn URL validation."""

    def test_valid_linkedin_profile(self):
        """Valid LinkedIn profile URLs should pass."""
        from src.common.url_utils import is_valid_linkedin_profile

        valid_urls = [
            "https://linkedin.com/in/johnsmith",
            "https://www.linkedin.com/in/jane-doe",
            "http://linkedin.com/in/bob-wilson-123",
            "https://linkedin.com/in/sarah_chen",
        ]
        for url in valid_urls:
            assert is_valid_linkedin_profile(url) == True, f"Should be valid: {url}"

    def test_invalid_linkedin_profile_company(self):
        """Company URLs should not be valid profiles."""
        from src.common.url_utils import is_valid_linkedin_profile

        assert is_valid_linkedin_profile("https://linkedin.com/company/techcorp") == False

    def test_invalid_linkedin_profile_short_username(self):
        """Very short usernames should be rejected."""
        from src.common.url_utils import is_valid_linkedin_profile

        assert is_valid_linkedin_profile("https://linkedin.com/in/ab") == False

    def test_invalid_linkedin_profile_jobs(self):
        """Job posting URLs should be rejected."""
        from src.common.url_utils import is_valid_linkedin_profile

        assert is_valid_linkedin_profile("https://linkedin.com/jobs/view/123") == False

    def test_valid_linkedin_company(self):
        """Valid LinkedIn company URLs should pass."""
        from src.common.url_utils import is_valid_linkedin_company

        assert is_valid_linkedin_company("https://linkedin.com/company/openai") == True
        assert is_valid_linkedin_company("https://www.linkedin.com/company/anthropic") == True

    def test_invalid_linkedin_company_profile(self):
        """Profile URLs should not be valid company pages."""
        from src.common.url_utils import is_valid_linkedin_company

        assert is_valid_linkedin_company("https://linkedin.com/in/johnsmith") == False


class TestWebsiteURLValidation:
    """Tests for website URL validation."""

    def test_valid_website_urls(self):
        """Normal company websites should be valid."""
        from src.common.url_utils import is_valid_website_url

        valid_urls = [
            "https://example.com",
            "https://techcorp.ai",
            "https://startup.io",
            "https://company.dev",
        ]
        for url in valid_urls:
            assert is_valid_website_url(url) == True, f"Should be valid: {url}"

    def test_invalid_website_linkedin(self):
        """LinkedIn URLs should not be valid company websites."""
        from src.common.url_utils import is_valid_website_url

        assert is_valid_website_url("https://linkedin.com/company/techcorp") == False

    def test_invalid_website_crunchbase(self):
        """Crunchbase URLs should not be valid company websites."""
        from src.common.url_utils import is_valid_website_url

        assert is_valid_website_url("https://crunchbase.com/organization/techcorp") == False

    def test_invalid_website_twitter(self):
        """Twitter URLs should not be valid company websites."""
        from src.common.url_utils import is_valid_website_url

        assert is_valid_website_url("https://twitter.com/techcorp") == False

    def test_invalid_website_github(self):
        """GitHub URLs should not be valid company websites."""
        from src.common.url_utils import is_valid_website_url

        assert is_valid_website_url("https://github.com/techcorp") == False


class TestPlaceholderRejection:
    """Tests for placeholder URL rejection."""

    def test_rejects_unknown(self):
        """'Unknown' should be rejected."""
        from src.common.url_utils import is_valid_url

        assert is_valid_url("Unknown") == False
        assert is_valid_url("unknown") == False
        assert is_valid_url("UNKNOWN") == False

    def test_rejects_na(self):
        """'N/A' variants should be rejected."""
        from src.common.url_utils import is_valid_url

        assert is_valid_url("N/A") == False
        assert is_valid_url("n/a") == False
        assert is_valid_url("NA") == False

    def test_rejects_pending(self):
        """'Pending' should be rejected."""
        from src.common.url_utils import is_valid_url

        assert is_valid_url("pending") == False
        assert is_valid_url("Pending") == False

    def test_rejects_none_string(self):
        """'None' string should be rejected."""
        from src.common.url_utils import is_valid_url

        assert is_valid_url("None") == False
        assert is_valid_url("none") == False


# =============================================================================
# CEO Name Extraction Tests
# =============================================================================
class TestCEONameExtractionEdgeCases:
    """Edge cases for CEO name extraction."""

    def test_title_with_multiple_delimiters(self):
        """Handle titles with multiple delimiter types."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        result = _extract_name_from_linkedin_title("John Smith - CEO | TechCorp - LinkedIn")
        assert result == "John Smith"

    def test_title_with_unicode_dash(self):
        """Handle Unicode dashes (em dash, en dash)."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        # Em dash
        result = _extract_name_from_linkedin_title("Jane Doe — Founder at Startup")
        assert result == "Jane Doe"

        # En dash
        result = _extract_name_from_linkedin_title("Bob Wilson – CEO at Company")
        assert result == "Bob Wilson"

    def test_title_with_bullet_point(self):
        """Handle bullet point delimiter."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        result = _extract_name_from_linkedin_title("Sarah Chen • Co-Founder • TechCorp")
        assert result == "Sarah Chen"

    def test_slug_with_numbers(self):
        """Handle slugs with numbers."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_slug

        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/john-smith-123456")
        assert result == "John Smith"

    def test_slug_with_credentials(self):
        """Handle slugs with credentials like MBA, PhD."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_slug

        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/jane-doe-mba")
        assert result == "Jane Doe"

        result = _extract_name_from_linkedin_slug("https://linkedin.com/in/bob-wilson-phd")
        assert result == "Bob Wilson"

    def test_slug_rejects_company_url(self):
        """Company URLs should return None."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_slug

        result = _extract_name_from_linkedin_slug("https://linkedin.com/company/openai")
        assert result is None

    def test_slug_rejects_jobs_url(self):
        """Jobs URLs should return None."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_slug

        result = _extract_name_from_linkedin_slug("https://linkedin.com/jobs/view/123")
        assert result is None


class TestNameMatchingEdgeCases:
    """Edge cases for name matching."""

    def test_three_word_names(self):
        """Handle names with three parts."""
        from src.enrichment.brave_enrichment import _names_match

        # Same three-word name
        assert _names_match("John Michael Smith", "John Michael Smith") == True

        # First and last match, middle different
        assert _names_match("John A Smith", "John B Smith") == True

    def test_hyphenated_last_names(self):
        """Handle hyphenated last names."""
        from src.enrichment.brave_enrichment import _names_match

        # Note: Current implementation may not handle hyphens perfectly
        # This documents expected behavior
        result = _names_match("Jane Smith-Jones", "Jane Smith-Jones")
        assert result == True

    def test_empty_string_names(self):
        """Empty strings should not match."""
        from src.enrichment.brave_enrichment import _names_match

        assert _names_match("", "John Smith") == False
        assert _names_match("John Smith", "") == False
        assert _names_match("", "") == False

    def test_whitespace_only_names(self):
        """Whitespace-only strings should not match."""
        from src.enrichment.brave_enrichment import _names_match

        assert _names_match("   ", "John Smith") == False
        assert _names_match("John Smith", "   ") == False


# =============================================================================
# Enrichment Skip Domain Tests
# =============================================================================
class TestSkipDomains:
    """Tests for skip domain checking in enrichment."""

    def test_linkedin_skipped(self):
        """LinkedIn should be in skip domains."""
        from src.enrichment.brave_enrichment import SKIP_DOMAINS

        assert any("linkedin" in d for d in SKIP_DOMAINS)

    def test_crunchbase_skipped(self):
        """Crunchbase should be in skip domains."""
        from src.enrichment.brave_enrichment import SKIP_DOMAINS

        assert any("crunchbase" in d for d in SKIP_DOMAINS)

    def test_twitter_skipped(self):
        """Twitter/X should be in skip domains."""
        from src.enrichment.brave_enrichment import SKIP_DOMAINS

        assert any("twitter" in d or "x.com" in d for d in SKIP_DOMAINS)


# =============================================================================
# Non-Person Slug Pattern Tests
# =============================================================================
class TestNonPersonSlugPatterns:
    """Tests for non-person LinkedIn slug detection."""

    def test_jobs_pattern_rejected(self):
        """Slugs ending in 'jobs' should be rejected."""
        from src.enrichment.brave_enrichment import NON_PERSON_SLUG_SUFFIXES

        assert "jobs" in NON_PERSON_SLUG_SUFFIXES

    def test_careers_pattern_rejected(self):
        """Slugs ending in 'careers' should be rejected."""
        from src.enrichment.brave_enrichment import NON_PERSON_SLUG_SUFFIXES

        assert "careers" in NON_PERSON_SLUG_SUFFIXES

    def test_hiring_pattern_rejected(self):
        """Slugs ending in 'hiring' should be rejected."""
        from src.enrichment.brave_enrichment import NON_PERSON_SLUG_SUFFIXES

        assert "hiring" in NON_PERSON_SLUG_SUFFIXES

    def test_official_pattern_rejected(self):
        """Slugs ending in 'official' should be rejected."""
        from src.enrichment.brave_enrichment import NON_PERSON_SLUG_SUFFIXES

        assert "official" in NON_PERSON_SLUG_SUFFIXES


# =============================================================================
# Generic Words Blocklist Tests
# =============================================================================
class TestGenericWordsBlocklist:
    """Tests for generic words blocklist in CEO extraction."""

    def test_common_titles_blocked(self):
        """Common job titles should be blocked."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        # These should return None because they're generic titles, not names
        assert _extract_name_from_linkedin_title("CEO | LinkedIn") is None
        assert _extract_name_from_linkedin_title("Founder - LinkedIn") is None

    def test_real_names_not_blocked(self):
        """Real names should not be blocked."""
        from src.enrichment.brave_enrichment import _extract_name_from_linkedin_title

        # These should return the actual name
        assert _extract_name_from_linkedin_title("John Smith - CEO | LinkedIn") == "John Smith"
        # Note: "Founder" as a last name would be blocked (it's a generic word)
        # Use a real surname instead
        assert _extract_name_from_linkedin_title("Jane Wilson - CTO | LinkedIn") == "Jane Wilson"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
