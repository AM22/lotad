"""Database transition regressions for playlist sync without external I/O."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa

from lotad.db.models import (
    SourceType,
    TaskStatus,
    playlist_songs,
    playlists,
    tasks,
    youtube_videos,
)
from lotad.ingestion.youtube_client import PlaylistItem
from lotad.sync import playlist_sync
from lotad.sync.playlist_sync import (
    PerPlaylistOutcome,
    SyncReport,
    _compute_diff,
    _PlaylistSyncer,
)


@pytest.fixture
def sync_db():
    engine = sa.create_engine("sqlite://")
    schema = sa.MetaData()
    sa.Table("songs", schema, sa.Column("id", sa.Integer, primary_key=True))
    for table in (playlists, youtube_videos, playlist_songs, tasks):
        table.to_metadata(schema)
    schema.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        for key in ("video", "song"):
            conn.exec_driver_sql(
                f"CREATE UNIQUE INDEX uq_tasks_open_{key} ON tasks(task_type, related_{key}_id) "
                f"WHERE status='OPEN' AND related_{key}_id IS NOT NULL "
                "AND task_type <> 'DROPPED_VIDEO'"
            )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX uq_tasks_open_dropped_playlist ON tasks (related_video_id, "
            "coalesce(json_extract(data, '$.source_playlist_db_id'), "
            "json_extract(data, '$.playlist_db_id'), '')) "
            "WHERE status='OPEN' AND related_video_id IS NOT NULL AND task_type='DROPPED_VIDEO'"
        )
        conn.execute(schema.tables["songs"].insert(), [{"id": 1}, {"id": 2}])
        conn.execute(
            playlists.insert(),
            [
                {"id": 1, "name": "high", "youtube_playlist_id": "PL1", "display_order": 1},
                {"id": 2, "name": "low", "youtube_playlist_id": "PL2", "display_order": 5},
                {
                    "id": 3,
                    "name": "unsaved",
                    "youtube_playlist_id": "__lotad_unsaved",
                    "display_order": 6,
                },
            ],
        )
        conn.execute(
            youtube_videos.insert(),
            [
                {
                    "id": 1,
                    "video_id": "old",
                    "title": "Old title",
                    "is_available": True,
                    "updated_at": datetime(2020, 1, 1),
                },
                {
                    "id": 2,
                    "video_id": "new",
                    "title": "New title",
                    "is_available": True,
                    "updated_at": datetime(2020, 1, 1),
                },
            ],
        )
        conn.execute(
            playlist_songs.insert().values(
                id=1,
                song_id=1,
                playlist_id=1,
                youtube_video_id=1,
                source_type=SourceType.COMPOSITE_VIDEO,
                youtube_timestamp_seconds=42,
            )
        )
    yield engine
    engine.dispose()


def make_item(video_id="old", available=True, title="Old title", position=0):
    return PlaylistItem(video_id=video_id, title=title, is_available=available, position=position)


def make_syncer(engine, items, limit=None):
    syncer = _PlaylistSyncer.__new__(_PlaylistSyncer)
    syncer._engine = engine
    syncer._limit = limit
    syncer._yt = SimpleNamespace(list_playlist_items=lambda _pid, limit: iter(items))
    return syncer


def snapshot(syncer, pid=1, name="high"):
    return syncer._snapshot({"id": pid, "name": name, "youtube_playlist_id": f"PL{pid}"})


def test_snapshot_partial_fetch_preserves_unseen_rows(sync_db):
    syncer = make_syncer(sync_db, [make_item("new")], limit=1)
    diff = _compute_diff(snapshot(syncer))
    assert diff.added_video_ids == {"new"}
    assert diff.removed_video_ids == set()


def test_handle_removal_does_not_remove_same_row_replacement(sync_db):
    syncer = make_syncer(sync_db, [make_item("new")])
    snap = snapshot(syncer)
    with sync_db.begin() as conn:
        conn.execute(playlist_songs.update().values(youtube_video_id=2))
    outcome = PerPlaylistOutcome()
    syncer._handle_removal(snap, "old", outcome)
    with sync_db.connect() as conn:
        row = conn.execute(sa.select(playlist_songs)).mappings().one()
        assert row["playlist_id"] == 1
        assert row["youtube_video_id"] == 2
        assert row["removed_at"] is None
        assert conn.execute(sa.select(sa.func.count()).select_from(tasks)).scalar_one() == 0
    assert outcome.same_song_swap == 1


def test_handle_removal_moves_all_composite_tracks_preserving_provenance(sync_db):
    with sync_db.begin() as conn:
        conn.execute(playlist_songs.update().values(playlist_id=2))
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=2,
                playlist_id=2,
                youtube_video_id=1,
                source_type=SourceType.COMPOSITE_VIDEO,
                youtube_timestamp_seconds=100,
            )
        )
    syncer = make_syncer(sync_db, [])
    snap = snapshot(syncer, pid=2, name="low")
    assert len(snap.db_rows["old"]) == 2
    syncer._handle_removal(snap, "old", PerPlaylistOutcome())
    with sync_db.connect() as conn:
        rows = (
            conn.execute(sa.select(playlist_songs).order_by(playlist_songs.c.id)).mappings().all()
        )
    assert [row["playlist_id"] for row in rows] == [3, 3]
    assert [row["source_type"] for row in rows] == [SourceType.COMPOSITE_VIDEO] * 2
    assert [row["youtube_timestamp_seconds"] for row in rows] == [42, 100]


def test_cross_playlist_move_keeps_all_composite_tracks(sync_db):
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
    syncer = make_syncer(sync_db, [])
    source = snapshot(syncer)
    syncer._yt = SimpleNamespace(list_playlist_items=lambda _pid, limit: iter([make_item()]))
    target = snapshot(syncer, pid=2, name="low")
    diffs = {1: _compute_diff(source), 2: _compute_diff(target)}
    report = SyncReport(per_playlist={"high": PerPlaylistOutcome(), "low": PerPlaylistOutcome()})
    syncer._resolve_cross_playlist_moves({1: source, 2: target}, diffs, report)
    with sync_db.connect() as conn:
        rows = (
            conn.execute(sa.select(playlist_songs).order_by(playlist_songs.c.id)).mappings().all()
        )
    assert [row["playlist_id"] for row in rows] == [2, 2]
    assert [row["youtube_timestamp_seconds"] for row in rows] == [42, 100]
    assert not diffs[1].removed_video_ids
    assert not diffs[2].added_video_ids


def test_cross_playlist_destination_replacement_survives_old_snapshot(sync_db):
    with sync_db.begin() as conn:
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=1,
                playlist_id=2,
                youtube_video_id=2,
                source_type=SourceType.INDIVIDUAL_VIDEO,
            )
        )
    syncer = make_syncer(sync_db, [])
    source = snapshot(syncer)
    syncer._yt = SimpleNamespace(list_playlist_items=lambda _pid, limit: iter([make_item()]))
    target = snapshot(syncer, pid=2, name="low")
    diffs = {1: _compute_diff(source), 2: _compute_diff(target)}
    report = SyncReport(per_playlist={"high": PerPlaylistOutcome(), "low": PerPlaylistOutcome()})
    syncer._resolve_cross_playlist_moves({1: source, 2: target}, diffs, report)
    syncer._handle_removal(target, "new", report.per_playlist["low"])
    with sync_db.connect() as conn:
        rows = (
            conn.execute(sa.select(playlist_songs).where(playlist_songs.c.removed_at.is_(None)))
            .mappings()
            .all()
        )
    assert len(rows) == 1
    assert rows[0]["playlist_id"] == 2
    assert rows[0]["youtube_video_id"] == 1
    assert rows[0]["source_type"] == SourceType.COMPOSITE_VIDEO
    assert rows[0]["youtube_timestamp_seconds"] == 42


def test_update_kept_videos_only_updates_timestamp_on_data_change(sync_db):
    syncer = make_syncer(sync_db, [make_item()])
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.connect() as conn:
        row = (
            conn.execute(sa.select(youtube_videos).where(youtube_videos.c.id == 1)).mappings().one()
        )
    assert row["last_checked_at"] is not None
    assert row["updated_at"] == datetime(2020, 1, 1)
    syncer._yt = SimpleNamespace(
        list_playlist_items=lambda _pid, limit: iter([make_item(title="Changed")])
    )
    snap = snapshot(syncer)
    syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.connect() as conn:
        row = (
            conn.execute(sa.select(youtube_videos).where(youtube_videos.c.id == 1)).mappings().one()
        )
    assert row["title"] == "Changed"
    assert row["updated_at"] > datetime(2020, 1, 1)


def test_update_kept_unavailable_creates_one_task_with_all_rows_and_position(sync_db):
    with sync_db.begin() as conn:
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=2,
                playlist_id=1,
                youtube_video_id=1,
                source_type=SourceType.COMPOSITE_VIDEO,
            )
        )
    syncer = make_syncer(sync_db, [make_item(available=False, title="Deleted video", position=7)])
    for _ in range(2):
        snap = snapshot(syncer)
        syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.connect() as conn:
        row = (
            conn.execute(sa.select(youtube_videos).where(youtube_videos.c.id == 1)).mappings().one()
        )
        task = conn.execute(sa.select(tasks)).mappings().one()
    assert row["title"] == "Old title"
    assert row["is_available"] is False
    assert task["status"] == TaskStatus.OPEN
    assert task["data"]["playlist_song_ids"] == [1, 2]
    assert task["data"]["source_playlist_db_id"] == 1
    assert task["data"]["position"] == 7
    assert task["data"]["title"] == "Old title"


def test_removal_keeps_separate_tasks_for_different_playlists(sync_db):
    with sync_db.begin() as conn:
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=2,
                playlist_id=2,
                youtube_video_id=1,
                source_type=SourceType.COMPOSITE_VIDEO,
            )
        )
    syncer = make_syncer(sync_db, [make_item(available=False)])
    for pid, name in ((1, "high"), (2, "low")):
        snap = snapshot(syncer, pid=pid, name=name)
        syncer._update_kept_videos(snap, _compute_diff(snap), PerPlaylistOutcome())
    with sync_db.connect() as conn:
        rows = conn.execute(sa.select(tasks)).mappings().all()
    assert len(rows) == 2
    assert {row["data"]["source_playlist_db_id"] for row in rows} == {1, 2}


@pytest.mark.asyncio
async def test_sync_run_same_playlist_replacement_stays_active(sync_db, monkeypatch):
    syncer = make_syncer(sync_db, [make_item("new", title="Replacement")])
    syncer._settings = object()
    syncer._retry_stubs = False
    monkeypatch.setattr(
        syncer,
        "_resolve_targets",
        lambda _ids: [
            {"id": 1, "name": "high", "youtube_playlist_id": "PL1"},
        ],
    )

    async def ingest_replacement(item, *, playlist_db_id):
        assert item.video_id == "new"
        with sync_db.begin() as conn:
            conn.execute(
                playlist_songs.update()
                .where(playlist_songs.c.playlist_id == playlist_db_id)
                .values(youtube_video_id=2)
            )
        return True

    factory = MagicMock()
    factory.return_value.__aenter__.return_value = SimpleNamespace(
        ingest_video=AsyncMock(side_effect=ingest_replacement),
    )
    monkeypatch.setattr(playlist_sync, "IngestPipeline", factory)
    report = await syncer.run(None)
    with sync_db.connect() as conn:
        row = conn.execute(sa.select(playlist_songs)).mappings().one()
    assert row["youtube_video_id"] == 2
    assert row["playlist_id"] == 1
    assert row["removed_at"] is None
    assert report.per_playlist["high"].same_song_swap == 1
    assert report.per_playlist["high"].added == 1


def test_cross_playlist_move_updates_unavailable_metadata_and_target_task(sync_db):
    syncer = make_syncer(sync_db, [])
    source = snapshot(syncer)
    syncer._yt = SimpleNamespace(
        list_playlist_items=lambda _pid, limit: iter(
            [
                make_item(available=False, title="Private video", position=8),
            ]
        )
    )
    target = snapshot(syncer, pid=2, name="low")
    diffs = {1: _compute_diff(source), 2: _compute_diff(target)}
    report = SyncReport(per_playlist={"high": PerPlaylistOutcome(), "low": PerPlaylistOutcome()})
    syncer._resolve_cross_playlist_moves({1: source, 2: target}, diffs, report)
    with sync_db.connect() as conn:
        row = (
            conn.execute(sa.select(youtube_videos).where(youtube_videos.c.id == 1)).mappings().one()
        )
        task = conn.execute(sa.select(tasks)).mappings().one()
    assert row["is_available"] is False
    assert row["title"] == "Old title"
    assert task["data"]["source_playlist_db_id"] == 2
    assert task["data"]["position"] == 8
    assert task["data"]["playlist_song_ids"] == [1]


def test_cross_playlist_reciprocal_same_song_swaps_preserve_both_rows(sync_db):
    with sync_db.begin() as conn:
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=1,
                playlist_id=2,
                youtube_video_id=2,
                source_type=SourceType.COMPOSITE_VIDEO,
                youtube_timestamp_seconds=99,
                rank=2,
            )
        )
    syncer = make_syncer(sync_db, [make_item("new")])
    first = snapshot(syncer)
    syncer._yt = SimpleNamespace(list_playlist_items=lambda _pid, limit: iter([make_item()]))
    second = snapshot(syncer, pid=2, name="low")
    diffs = {1: _compute_diff(first), 2: _compute_diff(second)}
    report = SyncReport(per_playlist={"high": PerPlaylistOutcome(), "low": PerPlaylistOutcome()})
    syncer._resolve_cross_playlist_moves({1: first, 2: second}, diffs, report)
    with sync_db.connect() as conn:
        rows = (
            conn.execute(sa.select(playlist_songs).order_by(playlist_songs.c.id)).mappings().all()
        )
    assert [
        (row["id"], row["playlist_id"], row["youtube_video_id"], row["removed_at"]) for row in rows
    ] == [(1, 2, 1, None), (2, 1, 2, None)]
    assert [row["youtube_timestamp_seconds"] for row in rows] == [42, 99]
    assert rows[1]["rank"] == 2
    assert not any(diff.added_video_ids or diff.removed_video_ids for diff in diffs.values())


def test_cross_playlist_move_batch_rolls_back_on_failure(sync_db, monkeypatch):
    with sync_db.begin() as conn:
        conn.execute(
            playlist_songs.insert().values(
                id=2,
                song_id=1,
                playlist_id=2,
                youtube_video_id=2,
                source_type=SourceType.INDIVIDUAL_VIDEO,
            )
        )
    syncer = make_syncer(sync_db, [make_item("new")])
    first = snapshot(syncer)
    syncer._yt = SimpleNamespace(list_playlist_items=lambda _pid, limit: iter([make_item()]))
    second = snapshot(syncer, pid=2, name="low")
    diffs = {1: _compute_diff(first), 2: _compute_diff(second)}
    report = SyncReport(per_playlist={"high": PerPlaylistOutcome(), "low": PerPlaylistOutcome()})
    move = syncer._move_playlist_song
    calls = 0

    def fail_second_move(conn, source, target, video_id):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise sa.exc.SQLAlchemyError("simulated write failure")
        move(conn, source, target, video_id)

    monkeypatch.setattr(syncer, "_move_playlist_song", fail_second_move)
    with pytest.raises(sa.exc.SQLAlchemyError, match="simulated write failure"):
        syncer._resolve_cross_playlist_moves({1: first, 2: second}, diffs, report)
    with sync_db.connect() as conn:
        rows = (
            conn.execute(sa.select(playlist_songs).order_by(playlist_songs.c.id)).mappings().all()
        )
    assert [(row["playlist_id"], row["youtube_video_id"], row["removed_at"]) for row in rows] == [
        (1, 1, None),
        (2, 2, None),
    ]
    assert diffs[1].added_video_ids == {"new"}
    assert diffs[2].added_video_ids == {"old"}


@pytest.mark.asyncio
async def test_sync_run_does_not_count_unmatched_video_as_added(sync_db, monkeypatch):
    syncer = make_syncer(sync_db, [make_item("new")], limit=1)
    syncer._settings = object()
    syncer._retry_stubs = False
    monkeypatch.setattr(
        syncer,
        "_resolve_targets",
        lambda _ids: [
            {"id": 1, "name": "high", "youtube_playlist_id": "PL1"},
        ],
    )
    factory = MagicMock()
    factory.return_value.__aenter__.return_value = SimpleNamespace(
        ingest_video=AsyncMock(return_value=False),
    )
    monkeypatch.setattr(playlist_sync, "IngestPipeline", factory)
    report = await syncer.run(None)
    assert report.per_playlist["high"].added == 0
    assert report.per_playlist["high"].unmatched == 1
