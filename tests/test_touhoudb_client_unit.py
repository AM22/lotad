"""Unit tests for TouhouDBClient — no network or DB required."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from lotad.ingestion.touhoudb_models import SongDetail


def _song(id: int, original_version_id: int | None) -> SongDetail:
    return SongDetail(id=id, name=f"Song {id}", originalVersionId=original_version_id)


@pytest.mark.asyncio
async def test_resolve_original_chain_eastern_story_includes_parent():
    """When the chain resolves to テーマ・オブ・イースタンストーリー (TouhouDB ID 2445),
    the direct parent is also returned as a co-original."""
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    # Chain: fan arrangement (999) → ZUN remix (500, originalVersionId=2445)
    #        → Eastern Story (2445, originalVersionId=None)
    side_effects = {
        999: _song(999, original_version_id=500),
        500: _song(500, original_version_id=2445),
        2445: _song(2445, original_version_id=None),
    }

    async def fake_get_song(song_id, **_kwargs):
        return side_effects[song_id]

    async def fake_fetch_notes(song_id):
        return None

    with (
        patch.object(TouhouDBClient, "get_song", side_effect=fake_get_song),
        patch.object(TouhouDBClient, "_fetch_song_notes", side_effect=fake_fetch_notes),
    ):
        # We need a real client instance but won't make real HTTP calls.
        client = object.__new__(TouhouDBClient)
        result = await client.resolve_original_chain(999)

    assert 2445 in result, "Eastern Story leaf should always be included"
    assert 500 in result, "Direct parent of Eastern Story should be included as co-original"


@pytest.mark.asyncio
async def test_resolve_original_chain_non_eastern_story_unaffected():
    """A normal chain that does NOT end at 2445 should return only the leaf."""
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    side_effects = {
        100: _song(100, original_version_id=200),
        200: _song(200, original_version_id=None),
    }

    async def fake_get_song(song_id, **_kwargs):
        return side_effects[song_id]

    async def fake_fetch_notes(song_id):
        return None

    with (
        patch.object(TouhouDBClient, "get_song", side_effect=fake_get_song),
        patch.object(TouhouDBClient, "_fetch_song_notes", side_effect=fake_fetch_notes),
    ):
        client = object.__new__(TouhouDBClient)
        result = await client.resolve_original_chain(100)

    assert result == [200]
    assert 100 not in result


@pytest.mark.asyncio
async def test_resolve_original_chain_eastern_story_direct_parent_none():
    """If the chain starts directly at Eastern Story (no parent), only 2445 is returned."""
    from lotad.ingestion.touhoudb_client import TouhouDBClient

    async def fake_get_song(song_id, **_kwargs):
        return _song(2445, original_version_id=None)

    with patch.object(TouhouDBClient, "get_song", side_effect=fake_get_song):
        client = object.__new__(TouhouDBClient)
        result = await client.resolve_original_chain(2445)

    assert result == [2445]
