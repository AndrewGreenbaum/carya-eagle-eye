"""
Tests for batched content hash lookups.

These tests verify the batch_check_content_seen function works correctly
for deduplicating syndicated articles.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass


@dataclass
class MockArticle:
    """Mock article for testing."""
    text: str
    url: str


class TestBatchCheckContentSeen:
    """Tests for batch_check_content_seen function."""

    @pytest.mark.asyncio
    async def test_empty_list_returns_empty(self):
        """Empty article list should return empty results."""
        from src.scheduler.jobs import batch_check_content_seen

        result, skipped = await batch_check_content_seen([])

        assert result == []
        assert skipped == 0

    @pytest.mark.asyncio
    async def test_empty_content_not_filtered(self):
        """Articles with empty content should not be filtered."""
        from src.scheduler.jobs import batch_check_content_seen

        articles = [
            MockArticle(text="", url="https://example.com/1"),
            MockArticle(text="   ", url="https://example.com/2"),
        ]

        with patch('src.archivist.database.get_session') as mock_session:
            # Mock the database session context manager
            mock_ctx = AsyncMock()
            mock_session.return_value.__aenter__ = mock_ctx
            mock_session.return_value.__aexit__ = AsyncMock()

            # Mock execute to return empty results
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = []
            mock_ctx.return_value.execute = AsyncMock(return_value=mock_result)
            mock_ctx.return_value.add_all = MagicMock()
            mock_ctx.return_value.commit = AsyncMock()

            result, skipped = await batch_check_content_seen(articles)

            assert len(result) == 2
            assert skipped == 0

    @pytest.mark.asyncio
    async def test_fingerprint_generation(self):
        """Verify fingerprints are generated correctly."""
        from src.scheduler.jobs import get_content_fingerprint

        # Short content uses full text
        short_text = "This is a short article about AI funding."
        short_fp = get_content_fingerprint(short_text)
        assert short_fp is not None
        assert len(short_fp) == 32  # MD5 hex digest

        # Long content uses start + end
        long_text = "A" * 2000
        long_fp = get_content_fingerprint(long_text)
        assert long_fp is not None
        assert len(long_fp) == 32

        # Same content = same fingerprint
        fp1 = get_content_fingerprint("Test content")
        fp2 = get_content_fingerprint("Test content")
        assert fp1 == fp2

        # Different content = different fingerprint
        fp3 = get_content_fingerprint("Different content")
        assert fp1 != fp3

    @pytest.mark.asyncio
    async def test_in_memory_cache_hit(self):
        """Articles already in in-memory cache should be skipped."""
        from src.scheduler.jobs import batch_check_content_seen, _job_tracker, get_content_fingerprint

        # Clear and prepare
        await _job_tracker.clear()

        # Add a fingerprint to in-memory cache
        test_text = "This is a test article about funding."
        fingerprint = get_content_fingerprint(test_text)
        await _job_tracker.add_content_hash(fingerprint)

        articles = [
            MockArticle(text=test_text, url="https://example.com/1"),
            MockArticle(text="Different article content", url="https://example.com/2"),
        ]

        with patch('src.archivist.database.get_session') as mock_session:
            mock_ctx = AsyncMock()
            mock_session.return_value.__aenter__ = mock_ctx
            mock_session.return_value.__aexit__ = AsyncMock()

            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = []
            mock_ctx.return_value.execute = AsyncMock(return_value=mock_result)
            mock_ctx.return_value.add_all = MagicMock()
            mock_ctx.return_value.commit = AsyncMock()

            result, skipped = await batch_check_content_seen(articles)

            # First article should be skipped (in-memory hit)
            assert skipped == 1
            assert len(result) == 1
            assert result[0].text == "Different article content"

        # Cleanup
        await _job_tracker.clear()
