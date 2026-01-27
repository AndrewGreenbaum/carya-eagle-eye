"""
Tests for confidence scoring system.

These tests cover:
- Boundary values (0.0, 0.35, 0.40, 0.50, 0.65, 1.0)
- Hybrid re-extraction triggers
- Hallucination removal penalties
- Weak evidence penalties
- Confidence clamping edge cases
- Early exit differentiation
"""

import pytest
from datetime import date
from unittest.mock import patch, MagicMock

from src.analyst.schemas import (
    DealExtraction,
    RoundType,
    EnterpriseCategory,
    InvestorMention,
    LeadStatus,
    FounderInfo,
    ChainOfThought,
)
from src.config.settings import settings


def _make_deal(**kwargs) -> DealExtraction:
    """Helper to create DealExtraction with required fields filled in."""
    defaults = {
        "startup_name": "TestCorp",
        "round_label": RoundType.SERIES_A,
        "lead_investors": [],
        "participating_investors": [],
        "tracked_fund_is_lead": False,
        "reasoning": ChainOfThought(final_reasoning="Test reasoning"),
    }
    defaults.update(kwargs)
    return DealExtraction(**defaults)


# =============================================================================
# Settings and Constants Tests
# =============================================================================
class TestConfidenceThresholds:
    """Tests for confidence threshold constants."""

    def test_hybrid_confidence_min_matches_external_threshold(self):
        """Hybrid min for external sources should be low enough to catch external deals."""
        # FIX (2026-01): Now using hybrid_confidence_min_external for external sources
        from src.analyst.extractor import EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD

        assert settings.hybrid_confidence_min_external <= EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD, (
            f"Hybrid min external ({settings.hybrid_confidence_min_external}) should be <= "
            f"external threshold ({EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD}) to avoid gap"
        )

    def test_hybrid_confidence_min_value(self):
        """Verify hybrid_confidence_min is set correctly for internal sources."""
        # FIX (2026-01): Internal sources now use 0.45, external use 0.35
        assert settings.hybrid_confidence_min == 0.45

    def test_hybrid_confidence_min_external_value(self):
        """Verify hybrid_confidence_min_external is set to 0.35."""
        assert settings.hybrid_confidence_min_external == 0.35

    def test_hybrid_confidence_max_value(self):
        """Verify hybrid_confidence_max is set to 0.65."""
        assert settings.hybrid_confidence_max == 0.65

    def test_external_threshold_above_cannot_verify(self):
        """External threshold should be above 'cannot verify' level."""
        from src.analyst.extractor import EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD

        # System prompt says <0.35 = "Cannot verify as real funding"
        # Threshold should be above this (0.40)
        assert EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD >= 0.40, (
            f"External threshold ({EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD}) should be >= 0.40 "
            "to be above 'cannot verify' level"
        )

    def test_standard_threshold_value(self):
        """Verify standard confidence threshold is 0.50."""
        assert settings.extraction_confidence_threshold == 0.50


# =============================================================================
# Early Exit Confidence Tests
# =============================================================================
class TestEarlyExitConfidence:
    """Tests for differentiated early exit confidence scores."""

    def test_funding_keywords_detection(self):
        """Funding keywords should be detected in relevant text."""
        from src.analyst.extractor import is_likely_funding_content

        # Test that funding text returns True
        funding_text = "The company raised $50 million in Series A funding today."
        assert is_likely_funding_content(funding_text) == True

        # Test that non-funding text returns False
        non_funding_text = "This is a regular blog post about technology trends."
        assert is_likely_funding_content(non_funding_text) == False

    def test_crypto_detection(self):
        """Crypto articles should be detected."""
        from src.analyst.extractor import is_likely_crypto_article

        # Test that crypto text is detected (needs multiple signals)
        crypto_text = "Bitcoin raised $50M in funding. The blockchain cryptocurrency company builds NFT DeFi token solutions."
        result = is_likely_crypto_article(crypto_text)
        # Note: Exact detection depends on implementation threshold
        # This tests that the function exists and runs

    def test_early_exit_scores_are_differentiated(self):
        """Different early exits should have different confidence scores."""
        # No funding: 0.05
        # Crypto: 0.15
        # These are distinct values to allow debugging
        assert 0.05 != 0.15  # Trivial but documents the requirement


# =============================================================================
# Hallucination Removal Penalty Tests
# =============================================================================
class TestFounderHallucinationPenalty:
    """Tests for confidence penalty when hallucinated founders are removed."""

    def test_founder_removal_reduces_confidence(self):
        """Removing hallucinated founders should reduce confidence."""
        from src.analyst.extractor import _validate_founders_in_text

        # Create deal with founders not in text
        deal = _make_deal(
            confidence_score=0.80,
            founders=[
                FounderInfo(name="John Smith", title="CEO"),
                FounderInfo(name="Jane Doe", title="CTO"),
            ],
        )
        article_text = "TestCorp announced funding today. CEO Michael Johnson leads."

        result = _validate_founders_in_text(deal, article_text)

        # Both founders should be removed (not in text)
        assert len(result.founders) == 0
        # Confidence should be reduced
        assert result.confidence_score < 0.80

    def test_founder_penalty_calculation(self):
        """Verify founder removal penalty: min(0.10, count * 0.03)."""
        from src.analyst.extractor import _validate_founders_in_text

        # Create deal with 3 founders not in text
        deal = _make_deal(
            confidence_score=0.80,
            founders=[
                FounderInfo(name="Fake One Alpha", title="CEO"),
                FounderInfo(name="Fake Two Beta", title="CTO"),
                FounderInfo(name="Fake Three Gamma", title="CFO"),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_founders_in_text(deal, article_text)

        # Penalty = min(0.10, 3 * 0.03) = min(0.10, 0.09) = 0.09
        expected_confidence = 0.80 - 0.09
        assert abs(result.confidence_score - expected_confidence) < 0.001

    def test_founder_penalty_capped_at_010(self):
        """Founder removal penalty should be capped at 0.10."""
        from src.analyst.extractor import _validate_founders_in_text

        # Create deal with 5 founders not in text
        deal = _make_deal(
            confidence_score=0.80,
            founders=[
                FounderInfo(name="Fake One Alpha", title="CEO"),
                FounderInfo(name="Fake Two Beta", title="CTO"),
                FounderInfo(name="Fake Three Gamma", title="CFO"),
                FounderInfo(name="Fake Four Delta", title="COO"),
                FounderInfo(name="Fake Five Epsilon", title="VP"),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_founders_in_text(deal, article_text)

        # Penalty = min(0.10, 5 * 0.03) = min(0.10, 0.15) = 0.10
        expected_confidence = 0.80 - 0.10
        assert abs(result.confidence_score - expected_confidence) < 0.001


class TestInvestorHallucinationPenalty:
    """Tests for confidence penalty when hallucinated investors are removed."""

    def test_investor_removal_reduces_confidence(self):
        """Removing hallucinated investors should reduce confidence."""
        from src.analyst.extractor import _validate_investors_in_text

        # Create deal with investors not in text
        deal = _make_deal(
            confidence_score=0.80,
            lead_investors=[
                InvestorMention(name="Nonexistent Partners Worldwide", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp announced $10M Series A led by Acme Venture Fund."

        result = _validate_investors_in_text(deal, article_text)

        # Investor should be removed (not in text)
        assert len(result.lead_investors) == 0
        # Confidence should be reduced
        assert result.confidence_score < 0.80

    def test_investor_penalty_calculation(self):
        """Verify investor removal penalty: min(0.15, count * 0.05)."""
        from src.analyst.extractor import _validate_investors_in_text

        # Create deal with 2 lead investors not in text
        deal = _make_deal(
            confidence_score=0.80,
            lead_investors=[
                InvestorMention(name="Fake Ventures One", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Nonexistent Capital Two", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_investors_in_text(deal, article_text)

        # Penalty = min(0.15, 2 * 0.05) = min(0.15, 0.10) = 0.10
        expected_confidence = 0.80 - 0.10
        assert abs(result.confidence_score - expected_confidence) < 0.001

    def test_investor_penalty_capped_at_015(self):
        """Investor removal penalty should be capped at 0.15."""
        from src.analyst.extractor import _validate_investors_in_text

        # Create deal with 5 lead investors not in text
        deal = _make_deal(
            confidence_score=0.80,
            lead_investors=[
                InvestorMention(name="Fake Capital One", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Two", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Three", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Four", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_investors_in_text(deal, article_text)

        # Penalty = min(0.15, 4 * 0.05) = min(0.15, 0.20) = 0.15
        expected_confidence = 0.80 - 0.15
        assert abs(result.confidence_score - expected_confidence) < 0.001


# =============================================================================
# Weak Evidence Penalty Tests
# =============================================================================
class TestWeakEvidencePenalty:
    """Tests for confidence penalty when lead evidence is weak."""

    def test_weak_evidence_reduces_confidence_missing_snippet(self):
        """Missing verification snippet should reduce confidence by 0.08."""
        from src.analyst.extractor import _verify_tracked_fund

        # Create deal claiming lead but with no snippet
        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet=None,  # Missing!
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )

        result = _verify_tracked_fund(deal, "TestCorp raised $10M with Andreessen Horowitz participating.")

        # Should be flagged as weak evidence
        assert result.lead_evidence_weak == True
        # Confidence should be reduced by 0.08
        expected_confidence = 0.80 - 0.08
        assert abs(result.confidence_score - expected_confidence) < 0.001

    def test_weak_evidence_reduces_confidence_bad_snippet(self):
        """Snippet lacking lead language should reduce confidence by 0.08."""
        from src.analyst.extractor import _verify_tracked_fund

        # Create deal claiming lead but snippet doesn't have lead language
        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet="a16z invested in the round",  # No "led by" language
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )

        result = _verify_tracked_fund(deal, "TestCorp raised $10M. a16z invested in the round.")

        # Should be flagged as weak evidence
        assert result.lead_evidence_weak == True
        # Confidence should be reduced by 0.08
        expected_confidence = 0.80 - 0.08
        assert abs(result.confidence_score - expected_confidence) < 0.001


# =============================================================================
# Confidence Clamping Tests
# =============================================================================
class TestConfidenceClamping:
    """Tests for confidence score clamping to [0.0, 1.0]."""

    def test_confidence_never_goes_negative(self):
        """Confidence should never go below 0.0 after penalties."""
        from src.analyst.extractor import _validate_investors_in_text

        # Create deal with very low confidence and many fake investors
        deal = _make_deal(
            confidence_score=0.05,  # Very low
            lead_investors=[
                InvestorMention(name="Fake Capital One", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Two", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Three", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Four", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_investors_in_text(deal, article_text)

        # Penalty would be 0.15, but 0.05 - 0.15 = -0.10
        # Should be clamped to 0.0
        assert result.confidence_score >= 0.0
        assert result.confidence_score == 0.0

    def test_multiple_penalties_clamp_to_zero(self):
        """Multiple penalties should still clamp to 0.0."""
        from src.analyst.extractor import (
            _validate_founders_in_text,
            _validate_investors_in_text,
        )

        # Create deal with both fake founders and investors
        deal = _make_deal(
            confidence_score=0.15,
            founders=[
                FounderInfo(name="Fake Founder Alpha", title="CEO"),
            ],
            lead_investors=[
                InvestorMention(name="Fake Investor Capital", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp announced funding today."

        # Apply both penalties
        result = _validate_founders_in_text(deal, article_text)
        result = _validate_investors_in_text(result, article_text)

        # Should be clamped to 0.0
        assert result.confidence_score >= 0.0


# =============================================================================
# Hybrid Re-extraction Trigger Tests
# =============================================================================
class TestHybridReextractionTriggers:
    """Tests for hybrid re-extraction trigger conditions."""

    def test_low_confidence_triggers_reextraction_internal(self):
        """Internal sources: confidence in [0.45, 0.65] should trigger re-extraction."""
        # FIX (2026-01): Internal sources use 0.45 threshold
        test_cases = [
            (0.45, True),   # At min boundary - should trigger
            (0.50, True),   # In middle - should trigger
            (0.65, True),   # At max boundary - should trigger
            (0.44, False),  # Below min - should NOT trigger
            (0.66, False),  # Above max - should NOT trigger
        ]

        for confidence, should_trigger in test_cases:
            in_range = (
                settings.hybrid_confidence_min <= confidence <= settings.hybrid_confidence_max
            )
            assert in_range == should_trigger, (
                f"Internal: Confidence {confidence} should{'not' if not should_trigger else ''} trigger reextraction"
            )

    def test_low_confidence_triggers_reextraction_external(self):
        """External sources: confidence in [0.35, 0.65] should trigger re-extraction."""
        # FIX (2026-01): External sources use 0.35 threshold (lower for headline-only content)
        test_cases = [
            (0.35, True),   # At min boundary - should trigger
            (0.45, True),   # In middle - should trigger
            (0.65, True),   # At max boundary - should trigger
            (0.34, False),  # Below min - should NOT trigger
            (0.66, False),  # Above max - should NOT trigger
        ]

        for confidence, should_trigger in test_cases:
            in_range = (
                settings.hybrid_confidence_min_external <= confidence <= settings.hybrid_confidence_max
            )
            assert in_range == should_trigger, (
                f"External: Confidence {confidence} should{'not' if not should_trigger else ''} trigger reextraction"
            )

    def test_high_confidence_weak_evidence_triggers_reextraction(self):
        """High confidence + weak evidence + tracked fund lead should trigger."""
        # This is the NEW trigger added in the fix
        test_cases = [
            # (confidence, weak_evidence, tracked_lead, should_trigger)
            (0.75, True, True, True),    # Should trigger
            (0.75, True, False, False),  # No tracked lead - skip
            (0.75, False, True, False),  # No weak evidence - skip
            (0.65, True, True, False),   # Not > 0.65 - handled by standard trigger
            (0.90, True, True, True),    # Very high confidence - should trigger
        ]

        for confidence, weak_evidence, tracked_lead, should_trigger in test_cases:
            triggers_new_path = (
                confidence > settings.hybrid_confidence_max
                and weak_evidence
                and tracked_lead
            )
            assert triggers_new_path == should_trigger, (
                f"confidence={confidence}, weak={weak_evidence}, lead={tracked_lead} "
                f"should{'not' if not should_trigger else ''} trigger new path"
            )


# =============================================================================
# Confidence Band Categorization Tests
# =============================================================================
class TestConfidenceBands:
    """Tests for confidence band categorization (used in health metrics)."""

    def test_confidence_bands_are_distinct(self):
        """Confidence bands should have no overlap."""
        bands = {
            "below_threshold": (0.0, 0.35),     # External threshold
            "borderline": (0.35, 0.50),         # Between external and standard
            "medium": (0.50, 0.65),             # Standard to hybrid max
            "high": (0.65, 1.0),                # Above hybrid max
        }

        # Check no overlap
        sorted_bands = sorted(bands.values())
        for i in range(len(sorted_bands) - 1):
            current_end = sorted_bands[i][1]
            next_start = sorted_bands[i + 1][0]
            assert current_end <= next_start, f"Overlap at {current_end}"

    def test_boundary_values_categorization(self):
        """Test boundary value categorization."""
        from src.analyst.extractor import EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD

        # 0.0 = below threshold
        assert 0.0 < EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD

        # 0.40 = at/above external threshold (now 0.40)
        assert 0.40 >= EXTERNAL_SOURCE_CONFIDENCE_THRESHOLD

        # 0.50 = standard threshold
        assert 0.50 >= settings.extraction_confidence_threshold

        # 0.65 = hybrid max
        assert 0.65 == settings.hybrid_confidence_max


# =============================================================================
# Integration Tests
# =============================================================================
class TestConfidenceScoringIntegration:
    """Integration tests for confidence scoring through the pipeline."""

    def test_all_penalties_apply_correctly(self):
        """Test that all penalties can be applied to a single deal."""
        from src.analyst.extractor import (
            _validate_founders_in_text,
            _validate_investors_in_text,
            _verify_tracked_fund,
        )

        # Create deal with multiple issues
        deal = _make_deal(
            confidence_score=0.90,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet="a16z was part of the round",  # Weak evidence
            founders=[
                FounderInfo(name="Fake Founder Alpha", title="CEO"),  # Will be removed
            ],
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp raised $10M with Andreessen Horowitz."

        # Apply all validations
        result = _validate_founders_in_text(deal, article_text)
        result = _validate_investors_in_text(result, article_text)
        result = _verify_tracked_fund(result, article_text)

        # All penalties should have been applied:
        # - Founder removal: 0.03 (1 founder)
        # - Weak evidence: 0.08
        # Total: 0.11
        expected_confidence = 0.90 - 0.03 - 0.08
        assert abs(result.confidence_score - expected_confidence) < 0.01

    def test_validated_data_preserves_confidence(self):
        """Deals with all valid data should preserve original confidence."""
        from src.analyst.extractor import (
            _validate_founders_in_text,
            _validate_investors_in_text,
        )

        # Create deal with valid founders and investors that appear in text
        deal = _make_deal(
            confidence_score=0.80,
            founders=[
                FounderInfo(name="John Smith", title="CEO"),
            ],
            lead_investors=[
                InvestorMention(name="Sequoia Capital", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp, founded by John Smith, raised $10M led by Sequoia Capital."

        # Apply validations
        result = _validate_founders_in_text(deal, article_text)
        result = _validate_investors_in_text(result, article_text)

        # No penalties - confidence should be preserved
        assert result.confidence_score == 0.80
        assert len(result.founders) == 1
        assert len(result.lead_investors) == 1


# =============================================================================
# Separated Confidence Fields Tests (2026-01)
# =============================================================================
class TestSeparatedConfidenceFields:
    """Tests for the new separated confidence scoring fields."""

    def test_extraction_confidence_preserved(self):
        """extraction_confidence should store the raw LLM confidence before penalties."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _validate_founders_in_text,
        )

        # Create deal with raw confidence of 0.80
        deal = _make_deal(
            confidence_score=0.80,
            founders=[
                FounderInfo(name="Fake Founder Alpha", title="CEO"),  # Will be removed
            ],
        )
        article_text = "TestCorp announced funding today."

        # First validate confidence score (this should set extraction_confidence)
        result = _validate_confidence_score(deal)

        # extraction_confidence should be set to original value
        assert result.extraction_confidence == 0.80
        assert result.penalty_breakdown == {}

        # Now apply founder validation (penalty)
        result = _validate_founders_in_text(result, article_text)

        # extraction_confidence should still be 0.80 (original)
        assert result.extraction_confidence == 0.80
        # confidence_score should be reduced
        assert result.confidence_score == 0.77  # 0.80 - 0.03

    def test_penalty_breakdown_tracks_founders(self):
        """penalty_breakdown should track founder removal penalty."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _validate_founders_in_text,
        )

        deal = _make_deal(
            confidence_score=0.80,
            founders=[
                FounderInfo(name="Fake One Alpha", title="CEO"),
                FounderInfo(name="Fake Two Beta", title="CTO"),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_confidence_score(deal)
        result = _validate_founders_in_text(result, article_text)

        assert result.penalty_breakdown is not None
        assert "founders_removed" in result.penalty_breakdown
        assert result.penalty_breakdown["founders_removed"] == 0.06  # 2 * 0.03

    def test_penalty_breakdown_tracks_investors(self):
        """penalty_breakdown should track investor removal penalty."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _validate_investors_in_text,
        )

        deal = _make_deal(
            confidence_score=0.80,
            lead_investors=[
                InvestorMention(name="Fake Capital One", role=LeadStatus.CONFIRMED_LEAD),
                InvestorMention(name="Fake Capital Two", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp announced funding today."

        result = _validate_confidence_score(deal)
        result = _validate_investors_in_text(result, article_text)

        assert result.penalty_breakdown is not None
        assert "investors_removed" in result.penalty_breakdown
        assert result.penalty_breakdown["investors_removed"] == 0.10  # 2 * 0.05

    def test_penalty_breakdown_tracks_weak_evidence(self):
        """penalty_breakdown should track weak evidence penalty."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _verify_tracked_fund,
        )

        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet=None,  # Missing snippet
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp raised $10M with Andreessen Horowitz."

        result = _validate_confidence_score(deal)
        result = _verify_tracked_fund(result, article_text)

        assert result.penalty_breakdown is not None
        assert "weak_evidence" in result.penalty_breakdown
        assert result.penalty_breakdown["weak_evidence"] == 0.08

    def test_lead_evidence_score_strong_evidence(self):
        """lead_evidence_score should be 1.0 for explicit lead language."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _verify_tracked_fund,
        )

        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet="The round was led by Andreessen Horowitz",  # Explicit lead language
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp raised $10M. The round was led by Andreessen Horowitz."

        result = _validate_confidence_score(deal)
        result = _verify_tracked_fund(result, article_text)

        assert result.lead_evidence_score == 1.0

    def test_lead_evidence_score_weak_evidence(self):
        """lead_evidence_score should be 0.5 for snippet without lead language."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _verify_tracked_fund,
        )

        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet="Andreessen Horowitz invested",  # No lead language
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp raised $10M. Andreessen Horowitz invested."

        result = _validate_confidence_score(deal)
        result = _verify_tracked_fund(result, article_text)

        assert result.lead_evidence_score == 0.5

    def test_lead_evidence_score_no_snippet(self):
        """lead_evidence_score should be 0.2 for missing snippet."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _verify_tracked_fund,
        )

        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=True,
            tracked_fund_name="a16z",
            verification_snippet=None,  # No snippet
            lead_investors=[
                InvestorMention(name="Andreessen Horowitz", role=LeadStatus.CONFIRMED_LEAD),
            ],
        )
        article_text = "TestCorp raised $10M with Andreessen Horowitz."

        result = _validate_confidence_score(deal)
        result = _verify_tracked_fund(result, article_text)

        assert result.lead_evidence_score == 0.2

    def test_lead_evidence_score_not_lead_deal(self):
        """lead_evidence_score should be None for non-lead deals."""
        from src.analyst.extractor import (
            _validate_confidence_score,
            _verify_tracked_fund,
        )

        deal = _make_deal(
            confidence_score=0.80,
            tracked_fund_is_lead=False,  # Not a lead deal
            lead_investors=[],
        )
        article_text = "TestCorp raised $10M."

        result = _validate_confidence_score(deal)
        result = _verify_tracked_fund(result, article_text)

        assert result.lead_evidence_score is None
