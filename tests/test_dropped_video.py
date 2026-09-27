"""Regression coverage for safely resolving modern and legacy dropped tasks."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from lotad.cli.tasks import _wizards
from lotad.db.models import SourceType, TaskStatus, TaskType, playlist_songs, tasks
from lotad.sync.playlist_sync import (
    PerPlaylistOutcome,
    _compute_diff,
    dismiss_dropped_video,
    resolve_dropped_video_to_unsaved,
)
from tests.test_playlist_sync import make_item, make_syncer, snapshot
from tests.test_playlist_sync import sync_db as sync_db


@pytest.mark.parametrize("resolve", [dismiss_dropped_video, resolve_dropped_video_to_unsaved])
@pytest.mark.parametrize("modern", [True, False])
def test_dropped_video_resolves_modern_and_legacy_tasks(sync_db, resolve, modern):
    with sync_db.begin() as conn:
        data = {"source_playlist_db_id": None, "playlist_db_id": 1, "video_id": "old"}
        if modern:
            data.update(playlist_song_ids=[1], song_ids=[1], source_playlist_db_id=1)
        conn.execute(
            tasks.insert().values(
                id=1,
                task_type=TaskType.DROPPED_VIDEO,
                title="Unavailable",
                data=data,
                related_video_id=1,
            )
        )
        assert resolve(conn, 1)
    with sync_db.connect() as conn:
        row = conn.execute(sa.select(playlist_songs)).mappings().one()
        task = conn.execute(sa.select(tasks)).mappings().one()
    if resolve is dismiss_dropped_video:
        assert row["removed_at"] is not None
        assert task["status"] == TaskStatus.DISMISSED
    else:
        assert row["playlist_id"] == 3
        assert row["source_type"] == SourceType.COMPOSITE_VIDEO
        assert row["youtube_timestamp_seconds"] == 42
        assert task["status"] == TaskStatus.RESOLVED


@pytest.mark.parametrize("resolve", [dismiss_dropped_video, resolve_dropped_video_to_unsaved])
def test_dropped_video_stale_task_preserves_replacement_and_stays_open(sync_db, resolve):
    with sync_db.begin() as conn:
        conn.execute(
            tasks.insert().values(
                id=1,
                task_type=TaskType.DROPPED_VIDEO,
                title="Dropped old video",
                data={"playlist_song_id": 1, "source_playlist_db_id": 1, "song_id": 1},
                related_song_id=1,
                related_video_id=1,
            )
        )
        conn.execute(playlist_songs.update().values(youtube_video_id=2))
        assert resolve(conn, 1) is False
    with sync_db.connect() as conn:
        row = conn.execute(sa.select(playlist_songs)).mappings().one()
        task = conn.execute(sa.select(tasks)).mappings().one()
    assert row["playlist_id"] == 1
    assert row["youtube_video_id"] == 2
    assert row["removed_at"] is None
    assert task["status"] == TaskStatus.OPEN


@pytest.mark.parametrize("resolve", [dismiss_dropped_video, resolve_dropped_video_to_unsaved])
def test_dropped_video_missing_row_leaves_task_open(sync_db, resolve):
    with sync_db.begin() as conn:
        conn.execute(
            tasks.insert().values(
                id=1,
                task_type=TaskType.DROPPED_VIDEO,
                title="Missing row",
                data={"playlist_song_id": 999, "source_playlist_db_id": 1},
                related_video_id=1,
            )
        )
        assert resolve(conn, 1) is False
        assert conn.execute(sa.select(tasks.c.status)).scalar_one() == TaskStatus.OPEN


@pytest.mark.parametrize("resolve", [dismiss_dropped_video, resolve_dropped_video_to_unsaved])
def test_dropped_video_deleted_in_place_task_resolves_every_composite_track(sync_db, resolve):
    with sync_db.begin() as conn:
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=2,
                playlist_id=1,
                youtube_video_id=1,
                source_type=SourceType.COMPOSITE_VIDEO,
                youtube_timestamp_seconds=100,
            )
        )
    syncer = make_syncer(sync_db, [make_item(available=False)])
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.begin() as conn:
        task_id = conn.execute(sa.select(tasks.c.id)).scalar_one()
        assert resolve(conn, task_id)
    with sync_db.connect() as conn:
        rows = conn.execute(sa.select(playlist_songs)).mappings().all()
    if resolve is dismiss_dropped_video:
        assert all(row["removed_at"] is not None for row in rows)
    else:
        assert all(row["playlist_id"] == 3 for row in rows)
        assert {row["youtube_timestamp_seconds"] for row in rows} == {42, 100}


def test_dropped_video_wizard_legacy_dismiss_updates_row_and_displays_position(
    sync_db, monkeypatch, capsys
):
    with sync_db.begin() as conn:
        conn.execute(
            tasks.insert().values(
                id=1,
                task_type=TaskType.DROPPED_VIDEO,
                title="Legacy unavailable task",
                data={"playlist_db_id": 1, "position": 4},
                related_video_id=1,
            )
        )
        task = dict(conn.execute(sa.select(tasks)).mappings().one())
    monkeypatch.setattr(_wizards, "get_engine", lambda: sync_db)
    monkeypatch.setattr(_wizards.click, "prompt", lambda *args, **kwargs: "D")
    _wizards._resolve_dropped_video(1, {"task": task, "video": None})
    with sync_db.connect() as conn:
        assert conn.execute(sa.select(playlist_songs.c.removed_at)).scalar_one() is not None
        assert conn.execute(sa.select(tasks.c.status)).scalar_one() == TaskStatus.DISMISSED
    assert "Playlist position: 5" in capsys.readouterr().out


def test_dropped_video_task_resolves_when_upload_returns(sync_db):
    syncer = make_syncer(sync_db, [make_item(available=False)])
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    syncer = make_syncer(sync_db, [make_item()])
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.begin() as conn:
        task = conn.execute(sa.select(tasks)).mappings().one()
        assert task["status"] == TaskStatus.RESOLVED
        assert dismiss_dropped_video(conn, task["id"]) is False


def test_dropped_video_ignored_unavailable_upload_does_not_recreate_task(sync_db):
    syncer = make_syncer(sync_db, [make_item(available=False)])
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.begin() as conn:
        conn.execute(tasks.update().values(status=TaskStatus.DISMISSED))
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(tasks)).scalar_one() == 1
