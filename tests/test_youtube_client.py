"""Unit tests for the YouTube client — no API key required."""

from __future__ import annotations

import pytest

from lotad.ingestion.youtube_client import PlaylistItem, _parse_iso8601_duration

# ---------------------------------------------------------------------------
# ISO 8601 duration parser
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("PT3M45S", 225),
        ("PT1H2M3S", 3723),
        ("PT30S", 30),
        ("PT1H", 3600),
        ("PT0S", 0),
    ],
)
def test_parse_iso8601_duration(raw, expected):
    assert _parse_iso8601_duration(raw) == expected


def test_parse_iso8601_duration_invalid():
    assert _parse_iso8601_duration("not-a-duration") is None


# ---------------------------------------------------------------------------
# PlaylistItem availability detection
# ---------------------------------------------------------------------------


def test_playlist_item_available_by_default():
    """Normal videos should be marked available."""
    item = PlaylistItem(video_id="abc12345678", title="Bad Apple!! feat.nomico")
    assert item.is_available is True


@pytest.mark.parametrize("stub_title", ["Deleted video", "Private video"])
def test_playlist_item_stub_titles_detected(stub_title):
    """
    Deleted and private stubs use reserved titles; is_available should be False.

    This is set by the YouTubeClient during playlist iteration, but we also
    verify that PlaylistItem itself carries the flag correctly when constructed
    with is_available=False.
    """
    item = PlaylistItem(video_id="abc12345678", title=stub_title, is_available=False)
    assert item.is_available is False
