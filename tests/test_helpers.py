"""
Shared test helpers and skip markers for test infrastructure.

This module can be explicitly imported by test files.
For pytest fixtures, see conftest.py.

Usage:
    from tests.test_helpers import skip_py39, skip_no_archivist, can_import_archivist
"""

import sys
import pytest


# =============================================================================
# Version checks
# =============================================================================
SKIP_PY39 = sys.version_info < (3, 10)
SKIP_PY310 = sys.version_info < (3, 11)


# =============================================================================
# Dependency checks
# =============================================================================
def has_playwright():
    """Check if playwright is installed."""
    try:
        import playwright
        return True
    except ImportError:
        return False


def has_postgres():
    """Check if asyncpg is installed (PostgreSQL driver)."""
    try:
        import asyncpg
        return True
    except ImportError:
        return False


def has_instructor():
    """Check if instructor library is installed (for LLM extraction)."""
    try:
        import instructor
        return True
    except (ImportError, TypeError):
        return False


def can_import_analyst():
    """Check if src.analyst can be imported (requires instructor + Python 3.10+)."""
    if SKIP_PY39:
        return False
    try:
        from src.analyst import extract_deal
        return True
    except (ImportError, TypeError, SyntaxError):
        return False


def can_import_harvester():
    """Check if src.harvester can be imported (may require playwright)."""
    try:
        from src.harvester.scrapers.delaware_corps import DelawareCorpsScraper
        return True
    except (ImportError, TypeError, SyntaxError):
        return False


def can_import_archivist():
    """Check if src.archivist can be imported (may require Python 3.10+)."""
    if SKIP_PY39:
        return False
    try:
        from src.archivist import storage
        return True
    except (ImportError, TypeError, SyntaxError):
        return False


def can_import_enrichment():
    """Check if src.enrichment can be imported."""
    try:
        from src.enrichment.brave_enrichment import _names_match
        return True
    except (ImportError, TypeError, SyntaxError):
        return False


# =============================================================================
# Skip markers
# =============================================================================
skip_py39 = pytest.mark.skipif(
    SKIP_PY39,
    reason="Requires Python 3.10+ (uses union type syntax: str | None)"
)

skip_py310 = pytest.mark.skipif(
    SKIP_PY310,
    reason="Requires Python 3.11+"
)

skip_no_playwright = pytest.mark.skipif(
    not has_playwright(),
    reason="Playwright not installed"
)

skip_no_postgres = pytest.mark.skipif(
    not has_postgres(),
    reason="PostgreSQL driver (asyncpg) not available"
)

skip_no_instructor = pytest.mark.skipif(
    not has_instructor(),
    reason="Instructor library not installed"
)

skip_no_analyst = pytest.mark.skipif(
    not can_import_analyst(),
    reason="Cannot import src.analyst (requires Python 3.10+ and instructor)"
)

skip_no_harvester = pytest.mark.skipif(
    not can_import_harvester(),
    reason="Cannot import src.harvester (may require playwright)"
)

skip_no_archivist = pytest.mark.skipif(
    not can_import_archivist(),
    reason="Cannot import src.archivist (requires Python 3.10+)"
)

skip_no_enrichment = pytest.mark.skipif(
    not can_import_enrichment(),
    reason="Cannot import src.enrichment"
)


# =============================================================================
# Skip decorator for imports
# =============================================================================
def skip_if_import_fails(module_path: str, reason: str = None):
    """
    Decorator to skip test if import fails.

    Usage:
        @skip_if_import_fails("src.analyst.extractor")
        def test_extractor():
            from src.analyst.extractor import extract_deal
            ...
    """
    try:
        __import__(module_path)
        return lambda f: f
    except (ImportError, TypeError, SyntaxError):
        skip_reason = reason or f"Module '{module_path}' not available"
        return pytest.mark.skip(reason=skip_reason)
