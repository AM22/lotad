"""Exercise task identity and the missing-original retry wizard without external I/O."""

from __future__ import annotations

from unittest.mock import MagicMock

import sqlalchemy as sa

from lotad.cli.tasks import _wizards
from lotad.db.models import TaskType, tasks
from lotad.ingestion.pipeline import IngestPipeline
from tests.test_playlist_sync import sync_db as sync_db


def test_ingestion_dropped_tasks_preserve_independent_playlist_context(sync_db):
    pipeline = object.__new__(IngestPipeline)
    with sync_db.begin() as conn:
        pipeline._create_task(
            TaskType.DROPPED_VIDEO,
            "First playlist",
            {"playlist_db_id": 1, "position": 4},
            conn,
            related_video_id=1,
        )
        pipeline._create_task(
            TaskType.DROPPED_VIDEO,
            "Second playlist",
            {"source_playlist_db_id": 2, "playlist_song_ids": [10], "position": 8},
            conn,
            related_video_id=1,
        )
        pipeline._create_task(
            TaskType.DROPPED_VIDEO,
            "Second playlist updated",
            {"source_playlist_db_id": 2, "position": 9},
            conn,
            related_video_id=1,
        )
        rows = conn.execute(sa.select(tasks).order_by(tasks.c.id)).mappings().all()
    assert len(rows) == 2
    assert rows[0]["title"] == "First playlist"
    assert rows[0]["data"]["position"] == 4
    assert rows[1]["title"] == "Second playlist updated"
    assert rows[1]["data"]["position"] == 9
    assert rows[1]["data"]["playlist_song_ids"] == [10]


def test_ingestion_legacy_dropped_task_updates_in_same_playlist(sync_db):
    pipeline = object.__new__(IngestPipeline)
    with sync_db.begin() as conn:
        conn.execute(
            tasks.insert().values(
                task_type=TaskType.DROPPED_VIDEO,
                title="Legacy",
                data={"playlist_db_id": 1},
                related_video_id=1,
            )
        )
        pipeline._create_task(
            TaskType.DROPPED_VIDEO,
            "Current",
            {"source_playlist_db_id": 1, "playlist_db_id": 1},
            conn,
            related_video_id=1,
        )
        row = conn.execute(sa.select(tasks)).mappings().one()
    assert row["title"] == "Current"


def test_fill_missing_info_retry_uses_open_transaction(sync_db, monkeypatch):
    retry = MagicMock()
    monkeypatch.setattr(_wizards, "get_engine", lambda: sync_db)
    monkeypatch.setattr(_wizards, "_resolve_original_song_chain_tasks", retry)
    monkeypatch.setattr(_wizards.click, "prompt", lambda *args, **kwargs: "R")

    def check_connection(conn):
        assert conn.in_transaction()
        return 0

    retry.side_effect = check_connection
    _wizards._resolve_fill_missing_info(
        1,
        {"task": {"data": {"original_touhoudb_ids": [200]}}},
    )
    retry.assert_called_once()
