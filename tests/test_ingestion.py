"""Integration tests for the ingestion pipeline.

Skipped in CI (requires real API keys and a live Postgres DB).
Run locally with: uv run pytest tests/test_ingestion.py -v -s
"""

from __future__ import annotations

import os

import pytest

# Skip all tests in this module if running in CI or API keys are absent.
pytestmark = pytest.mark.skipif(
    os.environ.get("CI") == "true" or not os.environ.get("YOUTUBE_API_KEY"),
    reason="Integration tests require real API keys; skipped in CI",
)


@pytest.fixture
def settings():
    from lotad.config import get_settings

    return get_settings()


# ---------------------------------------------------------------------------
# TouhouDB client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lookup_nonexistent_video(settings):
    """An unknown YouTube video ID should return None, not raise."""
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    async with TouhouDBClient.from_settings(settings) as client:
        result = await client.lookup_by_youtube_url("xxxxxxxxxxx")
    assert result is None


@pytest.mark.asyncio
async def test_get_normalization_count_runs(settings):
    """get_normalization_count should return a non-negative int without crashing."""
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    async with TouhouDBClient.from_settings(settings) as client:
        count = await client.get_normalization_count("ORIGINAL_SONG", 1)
    assert isinstance(count, int)
    assert count >= 0


# ---------------------------------------------------------------------------
# Bulk playlist match
# ---------------------------------------------------------------------------

# MEGAMIX playlist — public, well-known, high TouhouDB coverage.
_MEGAMIX_PLAYLIST_ID = "PLuDYUKEqeoaxodcKdDwsnjUBt1ZMsOa8q"


@pytest.mark.asyncio
async def test_bulk_match_playlist_returns_matches(settings):
    """
    bulk_match_playlist should return a non-empty dict for MEGAMIX.

    Verifies:
    - The /api/songLists/import endpoint is reachable on TouhouDB.
    - Matched entries have non-empty string keys (video IDs) and int values
      (TouhouDB song IDs).
    - At least one video in MEGAMIX is known to TouhouDB.
    """
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    async with TouhouDBClient.from_settings(settings) as client:
        result = await client.bulk_match_playlist(_MEGAMIX_PLAYLIST_ID)

    assert isinstance(result, dict), "Expected dict[str, int]"
    assert len(result) > 0, "Expected at least one TouhouDB match in MEGAMIX"

    for video_id, song_id in result.items():
        assert isinstance(video_id, str) and len(video_id) == 11, (
            f"video_id should be an 11-char YouTube ID, got {video_id!r}"
        )
        assert isinstance(song_id, int) and song_id > 0, (
            f"song_id should be a positive int, got {song_id!r}"
        )


@pytest.mark.asyncio
async def test_bulk_match_playlist_pagination(settings):
    """
    bulk_match_playlist paginates correctly for a playlist with >10 videos.

    TouhouDB's /api/songLists/import returns only 10 items on the first page;
    subsequent pages use /api/songLists/import-songs.  This test confirms that
    pagination works and the result covers more than the first page.

    We use MEGAMIX (>200 videos) and verify that more than 10 matches are
    returned — i.e. at least one page beyond the first was fetched.
    """
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    async with TouhouDBClient.from_settings(settings) as client:
        result = await client.bulk_match_playlist(_MEGAMIX_PLAYLIST_ID)

    # MEGAMIX has ~200+ videos; even with modest TouhouDB coverage we should
    # exceed 10 matches, confirming that pagination returned more than page 1.
    assert len(result) > 10, f"Expected >10 matches to confirm pagination worked; got {len(result)}"


# ---------------------------------------------------------------------------
# YouTube client
# ---------------------------------------------------------------------------


def test_list_playlist_items_limit(settings):
    """Should yield exactly ``limit`` items from a known playlist."""
    from lotad.ingestion.youtube_client import YouTubeClient

    client = YouTubeClient(settings)
    items = list(
        client.list_playlist_items(
            "PLuDYUKEqeoaxodcKdDwsnjUBt1ZMsOa8q",
            limit=3,
        )
    )
    assert len(items) == 3
    for item in items:
        assert item.video_id
        assert len(item.video_id) == 11


# ---------------------------------------------------------------------------
# Full pipeline smoke test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_playlist_limit(settings):
    """Ingest the first 5 videos of MEGAMIX; expect no Python-level crash."""
    from lotad.ingestion.pipeline import IngestPipeline

    async with IngestPipeline(settings) as pipeline:
        stats = await pipeline.ingest_playlist(
            "PLuDYUKEqeoaxodcKdDwsnjUBt1ZMsOa8q",
            limit=5,
        )

    assert "matched" in stats
    assert "errors" in stats
    # Tolerate up to 3 network errors in 5 videos
    assert stats["errors"] <= 3


@pytest.mark.asyncio
async def test_ingest_single_video(settings):
    """Ingesting a single video ID should not raise."""
    from lotad.ingestion.pipeline import IngestPipeline
    from lotad.ingestion.youtube_client import PlaylistItem

    item = PlaylistItem(video_id="FtutLA63Cp8", title="Bad Apple!! feat.nomico")

    async with IngestPipeline(settings) as pipeline:
        # matched may be True or False depending on TouhouDB coverage
        matched = await pipeline.ingest_video(item)

    assert isinstance(matched, bool)
