"""Unit tests for the candidate finder — runs against an in-memory SQLite DB."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa

from lotad.contrib.candidate_finder import find_candidates
from lotad.db.models import (
    artists,
    metadata,
    playlist_songs,
    playlists,
    song_artists,
    songs,
    youtube_videos,
)


@pytest.fixture
def conn():
    """A connection backed by an in-memory SQLite DB.

    Creates only the tables this module touches (rather than all of
    `metadata.create_all`) because some unrelated columns use Postgres-specific
    types like ARRAY that SQLite can't render.
    """
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(
        engine,
        tables=[
            artists,
            songs,
            youtube_videos,
            playlists,
            song_artists,
            playlist_songs,
        ],
    )
    with engine.begin() as connection:
        # One playlist required since playlist_songs FKs to it.
        connection.execute(
            sa.insert(playlists).values(
                id=1,
                name="MEGAMIX",
                youtube_playlist_id="PL-test",
                display_order=1,
            )
        )
        yield connection


def _seed_song(conn, *, song_id: int, touhoudb_id: int | None, title: str = "Title") -> None:
    conn.execute(sa.insert(songs).values(id=song_id, touhoudb_id=touhoudb_id, title=title))


def _seed_video(
    conn,
    *,
    video_id: int,
    yt_id: str,
    channel: str | None = "Some Channel",
    available: bool = True,
) -> None:
    conn.execute(
        sa.insert(youtube_videos).values(
            id=video_id,
            video_id=yt_id,
            title=f"YT Title {yt_id}",
            channel_name=channel,
            channel_id="UC123",
            duration_seconds=240,
            is_available=available,
        )
    )


def _seed_playlist_song(
    conn,
    *,
    song_id: int,
    video_id: int,
    source_type: str = "INDIVIDUAL_VIDEO",
    removed_at=None,
    added_at=None,
) -> None:
    conn.execute(
        sa.insert(playlist_songs).values(
            song_id=song_id,
            playlist_id=1,
            youtube_video_id=video_id,
            source_type=source_type,
            removed_at=removed_at,
            added_at=added_at or datetime.now(UTC),
        )
    )


def _seed_artist(conn, *, artist_id: int, name: str, name_romanized: str | None = None) -> None:
    conn.execute(
        sa.insert(artists).values(
            id=artist_id,
            name=name,
            name_romanized=name_romanized,
            artist_type="CIRCLE",
        )
    )


def _link_artist(conn, *, song_id: int, artist_id: int, role: str = "ARRANGER") -> None:
    conn.execute(sa.insert(song_artists).values(song_id=song_id, artist_id=artist_id, role=role))


def test_returns_eligible_song_with_reprint_default(conn):
    _seed_song(conn, song_id=1, touhoudb_id=12345)
    _seed_video(conn, video_id=1, yt_id="abcdefghijk", channel="Random Reuploader")
    _seed_playlist_song(conn, song_id=1, video_id=1)

    result = find_candidates(conn)

    assert len(result) == 1
    cand = result[0]
    assert cand.song_id == 1
    assert cand.touhoudb_id == 12345
    assert cand.video_id == "abcdefghijk"
    assert cand.video_url == "https://youtu.be/abcdefghijk"
    assert cand.suggested_pv_type == "Reprint"


def test_skips_songs_without_touhoudb_id(conn):
    _seed_song(conn, song_id=1, touhoudb_id=None)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa")
    _seed_playlist_song(conn, song_id=1, video_id=1)
    assert find_candidates(conn) == []


def test_skips_composite_video_source(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa")
    _seed_playlist_song(conn, song_id=1, video_id=1, source_type="COMPOSITE_VIDEO")
    assert find_candidates(conn) == []


def test_skips_removed_playlist_entries(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa")
    _seed_playlist_song(conn, song_id=1, video_id=1, removed_at=datetime.now(UTC))
    assert find_candidates(conn) == []


def test_skips_unavailable_videos(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa", available=False)
    _seed_playlist_song(conn, song_id=1, video_id=1)
    assert find_candidates(conn) == []


def test_picks_most_recently_added_video_per_song(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="oldoldoldol")
    _seed_video(conn, video_id=2, yt_id="newnewnewne")
    yesterday = datetime.now(UTC) - timedelta(days=1)
    today = datetime.now(UTC)
    _seed_playlist_song(conn, song_id=1, video_id=1, added_at=yesterday)
    _seed_playlist_song(conn, song_id=1, video_id=2, added_at=today)

    result = find_candidates(conn)
    assert len(result) == 1
    assert result[0].video_id == "newnewnewne"


def test_classifies_pv_type_as_original_when_channel_matches_artist(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa", channel="Sound Holic Official")
    _seed_playlist_song(conn, song_id=1, video_id=1)
    _seed_artist(conn, artist_id=10, name="Sound Holic")
    _link_artist(conn, song_id=1, artist_id=10)

    result = find_candidates(conn)
    assert result[0].suggested_pv_type == "Original"


def test_classifies_pv_type_as_original_via_romanized_name(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa", channel="Alstroemeria Records")
    _seed_playlist_song(conn, song_id=1, video_id=1)
    _seed_artist(
        conn,
        artist_id=10,
        name="アルストロメリア・レコーズ",
        name_romanized="Alstroemeria Records",
    )
    _link_artist(conn, song_id=1, artist_id=10)

    result = find_candidates(conn)
    assert result[0].suggested_pv_type == "Original"


def test_classifies_pv_type_as_reprint_when_channel_unrelated(conn):
    _seed_song(conn, song_id=1, touhoudb_id=99)
    _seed_video(conn, video_id=1, yt_id="aaaaaaaaaaa", channel="2hu Catfish")
    _seed_playlist_song(conn, song_id=1, video_id=1)
    _seed_artist(conn, artist_id=10, name="Sound Holic")
    _link_artist(conn, song_id=1, artist_id=10)

    result = find_candidates(conn)
    assert result[0].suggested_pv_type == "Reprint"


def test_limit_caps_results(conn):
    for i in range(5):
        _seed_song(conn, song_id=i + 1, touhoudb_id=100 + i)
        _seed_video(conn, video_id=i + 1, yt_id=f"v{i:010d}")
        _seed_playlist_song(conn, song_id=i + 1, video_id=i + 1)

    assert len(find_candidates(conn, limit=2)) == 2
    assert len(find_candidates(conn, limit=None)) == 5
