"""Pure-function tests for the playlist-sync diff and CSV parsing helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from lotad.ingestion.youtube_client import PlaylistItem
from lotad.sync.metadata_refresh import read_song_ids_from_csv
from lotad.sync.playlist_sync import _compute_diff, _PlaylistSnapshot


def _item(video_id: str, title: str = "x", available: bool = True) -> PlaylistItem:
    return PlaylistItem(
        video_id=video_id,
        title=title,
        description="",
        channel_id="c",
        channel_name="C",
        duration_seconds=180,
        position=0,
        playlist_item_id=f"pi_{video_id}",
        is_available=available,
    )


def _snap(yt_ids: list[str], db_ids: list[str]) -> _PlaylistSnapshot:
    return _PlaylistSnapshot(
        playlist_db_id=1,
        playlist_name="REVAL",
        youtube_playlist_id="PL_X",
        yt_items={vid: _item(vid) for vid in yt_ids},
        db_rows={
            vid: {
                "playlist_song_id": 100 + i,
                "song_id": 200 + i,
                "yt_db_id": 300 + i,
                "video_id": vid,
                "is_available": True,
            }
            for i, vid in enumerate(db_ids)
        },
    )


def test_diff_no_changes() -> None:
    snap = _snap(["a", "b", "c"], ["a", "b", "c"])
    diff = _compute_diff(snap)
    assert diff.added_video_ids == set()
    assert diff.removed_video_ids == set()
    assert diff.kept_video_ids == {"a", "b", "c"}


def test_diff_pure_add() -> None:
    snap = _snap(["a", "b", "c"], ["a", "b"])
    diff = _compute_diff(snap)
    assert diff.added_video_ids == {"c"}
    assert diff.removed_video_ids == set()


def test_diff_pure_remove() -> None:
    snap = _snap(["a"], ["a", "b"])
    diff = _compute_diff(snap)
    assert diff.removed_video_ids == {"b"}
    assert diff.added_video_ids == set()


def test_diff_replace() -> None:
    snap = _snap(["a", "x"], ["a", "b"])
    diff = _compute_diff(snap)
    assert diff.added_video_ids == {"x"}
    assert diff.removed_video_ids == {"b"}
    assert diff.kept_video_ids == {"a"}


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def test_csv_parsing(tmp_path: Path) -> None:
    p = tmp_path / "ids.csv"
    p.write_text("song_id\n1\n2\n3\n", encoding="utf-8")
    assert read_song_ids_from_csv(p) == [1, 2, 3]


def test_csv_parsing_extra_columns(tmp_path: Path) -> None:
    p = tmp_path / "ids.csv"
    p.write_text("song_id,title\n10,Foo\n20,Bar\n", encoding="utf-8")
    assert read_song_ids_from_csv(p) == [10, 20]


def test_csv_parsing_missing_column(tmp_path: Path) -> None:
    p = tmp_path / "ids.csv"
    p.write_text("video_id\nabc\n", encoding="utf-8")
    with pytest.raises(ValueError, match="song_id"):
        read_song_ids_from_csv(p)


def test_csv_parsing_skips_blanks_and_garbage(tmp_path: Path) -> None:
    p = tmp_path / "ids.csv"
    p.write_text("song_id\n7\n\n  \nnot-an-int\n42\n", encoding="utf-8")
    assert read_song_ids_from_csv(p) == [7, 42]
