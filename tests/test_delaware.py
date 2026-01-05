"""Unit tests for Delaware scraper components.

Note: These tests require src.harvester imports which may need playwright.
Tests will be skipped if imports fail.
"""
import pytest
from datetime import date, timedelta

# Import test helpers
from tests.test_helpers import skip_no_harvester, can_import_harvester

# Skip entire module if harvester imports fail
pytestmark = [skip_no_harvester]

# Lazy imports - only executed if module isn't skipped
if can_import_harvester():
    from src.harvester.scrapers.delaware_corps import (
        DelawareEntity,
        DelawareCorpsScraper,
        TECH_NAME_PATTERNS,
        STARTUP_FRIENDLY_AGENTS,
    )
    from src.harvester.scrapers.sec_delaware_crossref import (
        parse_amount,
        calculate_score,
        PRIORITY_HIGH,
        PRIORITY_STRONG,
        PRIORITY_WATCH,
    )
    from src.harvester.scrapers.sec_edgar import FormDFiling
else:
    # Dummy classes for when imports fail (tests will be skipped anyway)
    DelawareEntity = None
    DelawareCorpsScraper = None
    TECH_NAME_PATTERNS = None
    STARTUP_FRIENDLY_AGENTS = None
    parse_amount = None
    calculate_score = None
    PRIORITY_HIGH = None
    PRIORITY_STRONG = None
    PRIORITY_WATCH = None
    FormDFiling = None


class TestAmountParsing:
    """Test parse_amount() edge cases."""

    def test_millions_short(self):
        assert parse_amount("$5M") == 5_000_000
        assert parse_amount("$1.5M") == 1_500_000
        assert parse_amount("$10m") == 10_000_000

    def test_millions_long(self):
        assert parse_amount("$5 million") == 5_000_000
        assert parse_amount("$2.5 Million") == 2_500_000

    def test_billions(self):
        assert parse_amount("$2B") == 2_000_000_000
        assert parse_amount("$1.2 billion") == 1_200_000_000
        assert parse_amount("$0.5b") == 500_000_000

    def test_thousands(self):
        assert parse_amount("$500K") == 500_000
        assert parse_amount("$750k") == 750_000

    def test_ranges_uses_lower_bound(self):
        # Takes lower bound for safety
        assert parse_amount("$2-5M") == 2_000_000
        assert parse_amount("$10-15 million") == 10_000_000
        assert parse_amount("$5-10M") == 5_000_000

    def test_plain_numbers(self):
        assert parse_amount("$1,000,000") == 1_000_000
        assert parse_amount("5000000") == 5_000_000
        assert parse_amount("$500,000") == 500_000

    def test_empty_input(self):
        assert parse_amount(None) == 0
        assert parse_amount("") == 0

    def test_invalid_input(self):
        assert parse_amount("undisclosed") == 0
        assert parse_amount("not a number") == 0
        assert parse_amount("$") == 0


class TestTechNameDetection:
    """Test tech company name pattern detection."""

    def test_ai_patterns(self):
        scraper = DelawareCorpsScraper()
        assert scraper._has_tech_name("Acme AI Labs")
        assert scraper._has_tech_name("DeepMind Technologies")
        assert scraper._has_tech_name("Neural Systems Inc")
        assert scraper._has_tech_name("Machine Intelligence Corp")

    def test_tech_suffixes(self):
        scraper = DelawareCorpsScraper()
        # Note: Patterns use word boundaries, so "Tech" must be a separate word
        assert scraper._has_tech_name("Cloud Tech Solutions")
        assert scraper._has_tech_name("Data Software Inc")
        assert scraper._has_tech_name("Cyber Security Corp")
        assert scraper._has_tech_name("Platform Labs")

    def test_infrastructure_patterns(self):
        scraper = DelawareCorpsScraper()
        assert scraper._has_tech_name("Acme Infrastructure")
        # Note: Patterns use word boundaries - "Dev" and "Ops" are separate patterns
        assert scraper._has_tech_name("Dev Ops Solutions")
        assert scraper._has_tech_name("Automation Systems")

    def test_non_tech_companies(self):
        scraper = DelawareCorpsScraper()
        assert not scraper._has_tech_name("Bob's Pizza LLC")
        assert not scraper._has_tech_name("Main Street Realty")
        assert not scraper._has_tech_name("Johnson & Associates")
        assert not scraper._has_tech_name("Smith Holdings")


class TestStartupAgentDetection:
    """Test startup-friendly registered agent detection."""

    def test_stripe_atlas(self):
        scraper = DelawareCorpsScraper()
        assert scraper._has_startup_agent("Stripe Atlas")
        assert scraper._has_startup_agent("Stripe Atlas Registered Agent LLC")
        assert scraper._has_startup_agent("STRIPE ATLAS")

    def test_clerky(self):
        scraper = DelawareCorpsScraper()
        assert scraper._has_startup_agent("Clerky")
        assert scraper._has_startup_agent("Clerky Inc")
        assert scraper._has_startup_agent("clerky")

    def test_other_startup_agents(self):
        scraper = DelawareCorpsScraper()
        assert scraper._has_startup_agent("LegalZoom Registered Agent")
        assert scraper._has_startup_agent("Harvard Business Services")
        assert scraper._has_startup_agent("Northwest Registered Agent")

    def test_generic_agents_not_matched(self):
        """Generic corporate agents should NOT be flagged as startup-friendly."""
        scraper = DelawareCorpsScraper()
        # Note: CT Corporation and CSC ARE in the list (they're common for startups too)
        # So we test truly generic agents
        assert not scraper._has_startup_agent("John Smith Attorney")
        assert not scraper._has_startup_agent("Random Agent Inc")

    def test_empty_agent(self):
        scraper = DelawareCorpsScraper()
        assert not scraper._has_startup_agent("")
        assert not scraper._has_startup_agent(None)


class TestEntityScoring:
    """Test DelawareCorpsScraper.score_entity() logic."""

    def test_high_score_ai_startup(self):
        """AI startup with recent formation and Stripe Atlas = high score."""
        scraper = DelawareCorpsScraper()
        entity = DelawareEntity(
            entity_name="Acme AI Labs Inc",
            entity_type="Corporation",
            file_number="12345",
            formation_date=date.today() - timedelta(days=10),
            registered_agent="Stripe Atlas",
            status="Active",
            source_url="https://example.com",
            has_tech_name=True,
            has_startup_agent=True,
        )
        score = scraper.score_entity(entity)
        # Tech name (+3) + Startup agent (+5) + Recent formation (+2) + AI name (+2)
        assert score >= 10

    def test_medium_score_tech_company(self):
        """Tech company without AI and generic agent = medium score."""
        scraper = DelawareCorpsScraper()
        entity = DelawareEntity(
            entity_name="CloudTech Solutions Inc",
            entity_type="Corporation",
            file_number="12345",
            formation_date=date.today() - timedelta(days=60),
            registered_agent="CT Corporation",
            status="Active",
            source_url="https://example.com",
            has_tech_name=True,
            has_startup_agent=False,
        )
        score = scraper.score_entity(entity)
        # Tech name (+3) + Moderate recent formation (+1)
        assert 3 <= score < 10

    def test_low_score_generic_company(self):
        """Non-tech company with old formation = low score."""
        scraper = DelawareCorpsScraper()
        entity = DelawareEntity(
            entity_name="Smith Holdings LLC",
            entity_type="LLC",
            file_number="12345",
            formation_date=date.today() - timedelta(days=200),
            registered_agent="CT Corporation",
            status="Active",
            source_url="https://example.com",
            has_tech_name=False,
            has_startup_agent=False,
        )
        score = scraper.score_entity(entity)
        assert score < 3


class TestCrossRefScoring:
    """Test calculate_score() from SEC-Delaware cross-reference."""

    def _make_filing(
        self,
        state: str = "DE",
        is_first_sale: bool = True,
        industry: str = "Technology",
        amount: str = "$5M",
    ) -> FormDFiling:
        """Create a test FormDFiling."""
        return FormDFiling(
            cik="0001234567",
            company_name="Test Company Inc",
            filing_date=date.today(),
            filing_url="https://sec.gov/filing",
            form_type="D",
            accession_number="0001234567-24-000001",
            state_of_incorporation=state,
            is_first_sale=is_first_sale,
            industry=industry,
            amount_raised=amount,
            total_offering=amount,
            investors=[],
        )

    def test_high_priority_score(self):
        """Delaware + first sale + tech + $5M+ + VC match + startup agent = HIGH."""
        filing = self._make_filing(
            state="DE",
            is_first_sale=True,
            industry="Technology",
            amount="$10M",
        )
        entity = DelawareEntity(
            entity_name="AI Labs Inc",
            entity_type="Corporation",
            file_number=None,
            formation_date=date.today() - timedelta(days=10),
            registered_agent="Stripe Atlas",
            status="Active",
            source_url="",
            has_tech_name=True,
            has_startup_agent=True,
        )
        score, breakdown = calculate_score(filing, entity, "sequoia")

        assert score >= 10
        assert "delaware" in breakdown
        assert "first_sale" in breakdown
        assert "tech_industry" in breakdown
        assert "amount_5m_plus" in breakdown
        assert "vc_fund_match" in breakdown
        assert "startup_agent" in breakdown

    def test_strong_priority_score(self):
        """Delaware + first sale + tech + $1M = STRONG (7-9)."""
        filing = self._make_filing(
            state="DE",
            is_first_sale=True,
            industry="Technology",
            amount="$1M",
        )
        score, breakdown = calculate_score(filing, None, None)

        # DE (+2) + first sale (+3) + tech (+2) + $1M (+2) = 9
        assert 7 <= score <= 9
        assert "delaware" in breakdown
        assert "first_sale" in breakdown
        assert "amount_1m_plus" in breakdown

    def test_watch_priority_score(self):
        """Delaware + $500K + tech but NOT first sale = WATCH (3-6)."""
        filing = self._make_filing(
            state="DE",
            is_first_sale=False,
            industry="Technology",
            amount="$500K",
        )
        score, breakdown = calculate_score(filing, None, None)

        # DE (+2) + tech (+2) + $500K (+1) = 5
        assert 3 <= score <= 6
        assert "delaware" in breakdown
        assert "first_sale" not in breakdown

    def test_low_score_non_delaware(self):
        """Non-Delaware = lower score."""
        filing = self._make_filing(
            state="CA",
            is_first_sale=True,
            industry="Technology",
            amount="$5M",
        )
        score, breakdown = calculate_score(filing, None, None)

        # No DE bonus, but still: first sale (+3) + tech (+2) + $5M (+3) = 8
        assert "delaware" not in breakdown
        assert score >= 7  # Still STRONG because of other signals

    def test_no_entity_signals(self):
        """When no Delaware entity found, no entity-based signals."""
        filing = self._make_filing()
        score, breakdown = calculate_score(filing, None, None)

        assert "startup_agent" not in breakdown
        assert "tech_name" not in breakdown


class TestDelawareEntityCreation:
    """Test DelawareEntity dataclass and conversions."""

    def test_entity_to_article(self):
        """Test entity_to_article() conversion."""
        scraper = DelawareCorpsScraper()
        entity = DelawareEntity(
            entity_name="Acme AI Labs Inc",
            entity_type="Corporation",
            file_number="12345",
            formation_date=date.today(),
            registered_agent="Stripe Atlas",
            status="Active",
            source_url="https://example.com/filing",
            has_tech_name=True,
            has_startup_agent=True,
        )

        article = scraper.entity_to_article(entity)

        assert "Acme AI Labs Inc" in article.title
        assert "DELAWARE INCORPORATION DETECTED" in article.text
        assert "Stripe Atlas" in article.text
        assert "delaware" in article.tags
        assert "tech_name" in article.tags
        assert "startup_agent" in article.tags
        assert article.published_date == date.today()

    def test_entity_to_article_no_formation_date(self):
        """Test entity_to_article() with None formation_date."""
        scraper = DelawareCorpsScraper()
        entity = DelawareEntity(
            entity_name="Unknown Corp",
            entity_type="Corporation",
            file_number=None,
            formation_date=None,  # Unknown date
            registered_agent=None,
            status="Active",
            source_url="",
            has_tech_name=False,
            has_startup_agent=False,
        )

        article = scraper.entity_to_article(entity)

        # Should not crash, published_date should be None
        assert article.published_date is None
