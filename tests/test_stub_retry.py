"""Regression tests for atomic stub promotion with restrictive foreign keys."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy import Connection

from lotad.db.models import (
    SongType,
    TaskStatus,
    TaskType,
    album_tracks,
    albums,
    artists,
    characters,
    metadata,
    original_songs,
    physical_albums,
    physical_tracks,
    playlist_songs,
    playlists,
    song_artists,
    song_characters,
    song_languages,
    song_originals,
    song_tags,
    songs,
    tasks,
    youtube_videos,
)
from lotad.ingestion.http_client import CircuitBreakerOpen
from lotad.ingestion.touhoudb_models import SongDetail
from lotad.sync.stub_retry import iter_retry_candidates, replace_stub_with_song, retry_stub_song


@pytest.fixture
def conn() -> Iterator[Connection]:
    sqlite_metadata = sa.MetaData()
    for table in metadata.sorted_tables:
        copy = table.to_metadata(sqlite_metadata)
        for column in copy.c:
            if isinstance(column.type, sa.ARRAY):
                column.type = sa.JSON()
    task_table = sqlite_metadata.tables["tasks"]
    for column in ("related_song_id", "related_video_id"):
        sa.Index(
            f"uq_tasks_open_{column}",
            task_table.c.task_type,
            task_table.c[column],
            unique=True,
            sqlite_where=sa.and_(
                task_table.c.status == "OPEN",
                task_table.c[column].is_not(None),
                task_table.c.task_type != "DROPPED_VIDEO",
            ),
        )
    sa.Index(
        "uq_tasks_open_dropped_playlist",
        task_table.c.related_video_id,
        sa.func.coalesce(
            task_table.c.data["source_playlist_db_id"].as_string(),
            task_table.c.data["playlist_db_id"].as_string(),
        ),
        unique=True,
        sqlite_where=sa.and_(
            task_table.c.status == "OPEN",
            task_table.c.task_type == "DROPPED_VIDEO",
            task_table.c.related_video_id.is_not(None),
        ),
    )
    engine = sa.create_engine("sqlite://")
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        sqlite_metadata.create_all(connection)
        connection.commit()
        with connection.begin():
            connection.execute(
                songs.insert(),
                [
                    {
                        "id": 1,
                        "title": "Extracted title",
                        "touhoudb_id": None,
                        "duration_seconds": 201,
                        "notes": "Manual source note",
                        "arrangement_chronicle_url": "https://example.com/arrangement",
                    },
                    {
                        "id": 2,
                        "title": "Canonical title",
                        "touhoudb_id": 100,
                        "duration_seconds": 240,
                        "notes": "Upstream note",
                        "arrangement_chronicle_url": None,
                    },
                ],
            )
            yield connection
    engine.dispose()


def _seed_dependencies(conn: Connection) -> None:
    conn.execute(
        artists.insert(),
        [{"id": i, "name": f"Artist {i}", "artist_type": "INDIVIDUAL"} for i in (1, 2)],
    )
    conn.execute(original_songs.insert(), [{"id": i, "name": f"Original {i}"} for i in (1, 2)])
    conn.execute(characters.insert(), [{"id": i, "name": f"Character {i}"} for i in (1, 2)])
    conn.execute(albums.insert().values(id=1, title="Album"))
    conn.execute(physical_albums.insert().values(id=1, title="Physical album"))
    conn.execute(
        physical_tracks.insert().values(
            id=1, physical_album_id=1, title="Track", track_number=1, song_id=1
        )
    )
    conn.execute(album_tracks.insert().values(id=1, album_id=1, song_id=1, track_number=1))
    for table, key, extra in (
        (song_artists, "artist_id", {"role": "ARRANGER"}),
        (song_originals, "original_song_id", {"is_manual": False}),
        (song_characters, "character_id", {}),
    ):
        conn.execute(
            table.insert(),
            [
                {"song_id": 1, key: 1, **extra},
                {"song_id": 1, key: 2, **extra},
                {"song_id": 2, key: 1, **extra},
            ],
        )
    conn.execute(
        song_languages.insert(),
        [
            {"song_id": 1, "language": "JAPANESE"},
            {"song_id": 1, "language": "ENGLISH"},
            {"song_id": 2, "language": "JAPANESE"},
        ],
    )
    conn.execute(
        song_tags.insert(),
        [
            {"song_id": 1, "tag": "rock", "count": 1},
            {"song_id": 1, "tag": "piano", "count": 2},
            {"song_id": 2, "tag": "rock", "count": 10},
        ],
    )
    conn.execute(
        playlists.insert(),
        [
            {"id": i, "name": f"Playlist {i}", "youtube_playlist_id": f"pl{i}", "display_order": i}
            for i in (1, 2)
        ],
    )
    conn.execute(
        youtube_videos.insert(),
        [{"id": i, "video_id": f"video{i}", "is_available": True} for i in (1, 2)],
    )
    conn.execute(
        playlist_songs.insert(),
        [
            {
                "id": 1,
                "song_id": 1,
                "playlist_id": 1,
                "youtube_video_id": 1,
                "source_type": "INDIVIDUAL_VIDEO",
                "rank": 3,
                "removed_at": None,
            },
            {
                "id": 2,
                "song_id": 2,
                "playlist_id": 1,
                "youtube_video_id": 2,
                "source_type": "INDIVIDUAL_VIDEO",
                "rank": None,
                "removed_at": None,
            },
            {
                "id": 3,
                "song_id": 1,
                "playlist_id": 2,
                "youtube_video_id": 1,
                "source_type": "INDIVIDUAL_VIDEO",
                "rank": 7,
                "removed_at": None,
            },
            {
                "id": 4,
                "song_id": 1,
                "playlist_id": 2,
                "youtube_video_id": 1,
                "source_type": "INDIVIDUAL_VIDEO",
                "rank": 9,
                "removed_at": datetime(2020, 1, 1, tzinfo=UTC),
            },
        ],
    )
    conn.execute(
        tasks.insert(),
        [
            {
                "id": 1,
                "task_type": TaskType.FILL_MISSING_INFO,
                "title": "Stub originals",
                "related_song_id": 1,
                "status": TaskStatus.OPEN,
                "data": {"local_evidence": "kept"},
            },
            {
                "id": 2,
                "task_type": TaskType.FILL_MISSING_INFO,
                "title": "Canonical originals",
                "related_song_id": 2,
                "status": TaskStatus.OPEN,
                "data": {"canonical_evidence": "kept"},
            },
            {
                "id": 3,
                "task_type": TaskType.MISSING_CIRCLE,
                "title": "Local circle",
                "related_song_id": 1,
                "status": TaskStatus.OPEN,
                "data": {"song_id": 1, "song_ids": [1]},
            },
            {
                "id": 4,
                "task_type": TaskType.MISSING_CIRCLE,
                "title": "Historical circle",
                "related_song_id": 1,
                "status": TaskStatus.RESOLVED,
                "data": {"history": "kept"},
            },
        ],
    )


def test_replace_stub_with_song_transfers_all_dependencies(conn: Connection) -> None:
    _seed_dependencies(conn)
    replace_stub_with_song(1, 2, conn)
    assert conn.execute(sa.select(songs.c.id)).scalars().all() == [2]
    for table in (song_artists, song_originals, song_characters, song_languages, song_tags):
        rows = conn.execute(sa.select(table)).mappings().all()
        assert len(rows) == 2
        assert all(row["song_id"] == 2 for row in rows)
    assert (
        conn.execute(sa.select(song_tags.c.count).where(song_tags.c.tag == "rock")).scalar_one()
        == 10
    )
    assert all(conn.execute(sa.select(song_originals.c.is_manual)).scalars().all())
    for table in (album_tracks, physical_tracks):
        assert conn.execute(sa.select(table.c.song_id)).scalar_one() == 2
    assert conn.exec_driver_sql("PRAGMA foreign_key_check").all() == []

    canonical = conn.execute(sa.select(songs)).mappings().one()
    assert canonical["title"] == "Canonical title"
    assert canonical["duration_seconds"] == 240
    assert canonical["arrangement_chronicle_url"] == "https://example.com/arrangement"
    assert "Upstream note" in canonical["notes"]
    assert "Manual source note" in canonical["notes"]
    assert canonical["touhoudb_id"] == 100


def test_replace_stub_with_song_preserves_playlist_history_and_rank(conn: Connection) -> None:
    _seed_dependencies(conn)
    replace_stub_with_song(1, 2, conn)
    rows = {row["id"]: row for row in conn.execute(sa.select(playlist_songs)).mappings().all()}
    assert len(rows) == 4
    assert all(row["song_id"] == 2 for row in rows.values())
    assert rows[1]["removed_at"] is not None
    assert rows[1]["youtube_video_id"] == 1
    assert rows[2]["removed_at"] is None
    assert rows[2]["rank"] == 3
    assert rows[3]["removed_at"] is None
    assert rows[3]["rank"] == 7
    assert rows[4]["removed_at"].year == 2020


def test_replace_stub_with_song_preserves_colliding_task_history(conn: Connection) -> None:
    _seed_dependencies(conn)
    replace_stub_with_song(1, 2, conn)
    rows = {row["id"]: row for row in conn.execute(sa.select(tasks)).mappings().all()}
    assert len(rows) == 4
    assert all(row["related_song_id"] == 2 for row in rows.values())
    assert rows[1]["status"] == TaskStatus.DISMISSED
    assert rows[1]["data"] == {
        "local_evidence": "kept",
        "promoted_from_song_id": 1,
        "merged_into_task_id": 2,
    }
    assert rows[2]["status"] == TaskStatus.OPEN
    assert rows[2]["data"] == {"canonical_evidence": "kept"}
    assert rows[3]["status"] == TaskStatus.OPEN
    assert rows[3]["data"]["song_id"] == 2
    assert rows[3]["data"]["song_ids"] == [2]
    assert rows[4]["status"] == TaskStatus.RESOLVED
    assert rows[4]["data"]["history"] == "kept"


def _add_unknown_dependency(conn: Connection) -> None:
    # A future FK must cause a full rollback rather than a half-completed merge.
    conn.exec_driver_sql("CREATE TABLE future_dependency (song_id INTEGER REFERENCES songs(id))")
    conn.exec_driver_sql("INSERT INTO future_dependency VALUES (1)")


def test_replace_stub_with_song_rolls_back_all_changes_on_delete_failure(conn: Connection) -> None:
    _seed_dependencies(conn)
    _add_unknown_dependency(conn)
    with pytest.raises(sa.exc.IntegrityError):
        replace_stub_with_song(1, 2, conn)
    assert conn.execute(sa.select(songs.c.id).order_by(songs.c.id)).scalars().all() == [1, 2]
    assert conn.execute(sa.select(physical_tracks.c.song_id)).scalar_one() == 1
    assert (
        conn.execute(sa.select(tasks.c.status).where(tasks.c.id == 1)).scalar_one()
        == TaskStatus.OPEN
    )
    assert (
        conn.execute(sa.select(songs.c.notes).where(songs.c.id == 2)).scalar_one()
        == "Upstream note"
    )
    assert conn.execute(sa.select(sa.func.count()).select_from(song_artists)).scalar_one() == 3


@pytest.mark.parametrize("source,target", [(2, 1), (1, 99)])
def test_replace_stub_with_song_rejects_invalid_targets(
    conn: Connection, source: int, target: int
) -> None:
    with pytest.raises(ValueError):
        replace_stub_with_song(source, target, conn)
    assert conn.execute(sa.select(sa.func.count()).select_from(songs)).scalar_one() == 2


async def test_retry_stub_song_preserves_canonical_and_missing_metadata_tasks(
    conn: Connection,
) -> None:
    _seed_dependencies(conn)
    conn.execute(
        tasks.insert(),
        [
            {
                "id": 5,
                "task_type": TaskType.INGEST_FAILED,
                "title": "Stub failed",
                "related_song_id": 1,
                "related_video_id": None,
            },
            {
                "id": 6,
                "task_type": TaskType.INGEST_FAILED,
                "title": "Unassigned video",
                "related_song_id": None,
                "related_video_id": 1,
            },
            {
                "id": 7,
                "task_type": TaskType.INGEST_FAILED,
                "title": "Canonical needs review",
                "related_song_id": 2,
                "related_video_id": 2,
            },
        ],
    )
    tdb = AsyncMock()
    tdb.lookup_by_youtube_url.return_value = SongDetail(id=100, name="Canonical title")
    with patch("lotad.sync.stub_retry.apply_touhoudb_detail", new=AsyncMock(return_value=2)):
        assert await retry_stub_song(1, ["video1"], conn, tdb) == 2
    rows = {row["id"]: row for row in conn.execute(sa.select(tasks)).mappings().all()}
    assert rows[5]["status"] == TaskStatus.RESOLVED
    assert rows[5]["related_song_id"] == 2
    assert rows[6]["status"] == TaskStatus.RESOLVED
    assert rows[7]["status"] == TaskStatus.OPEN
    assert rows[2]["status"] == TaskStatus.OPEN


async def test_retry_stub_song_rolls_back_canonical_ingest_on_merge_failure(
    conn: Connection,
) -> None:
    _seed_dependencies(conn)
    _add_unknown_dependency(conn)
    tdb = AsyncMock()
    tdb.lookup_by_youtube_url.return_value = SongDetail(id=100, name="Refreshed title")

    async def apply_detail(*args: object, **kwargs: object) -> int:
        conn.execute(songs.update().where(songs.c.id == 2).values(title="Refreshed title"))
        return 2

    with (
        patch("lotad.sync.stub_retry.apply_touhoudb_detail", new=apply_detail),
        pytest.raises(sa.exc.IntegrityError),
    ):
        await retry_stub_song(1, ["video1"], conn, tdb)
    assert (
        conn.execute(sa.select(songs.c.title).where(songs.c.id == 2)).scalar_one()
        == "Canonical title"
    )
    assert conn.execute(sa.select(physical_tracks.c.song_id)).scalar_one() == 1


async def test_retry_stub_song_continues_after_network_failure(conn: Connection) -> None:
    tdb = AsyncMock()
    tdb.lookup_by_youtube_url.side_effect = [httpx.ReadTimeout("Unavailable"), None]
    assert await retry_stub_song(1, ["video1", "video2"], conn, tdb) is None
    assert tdb.lookup_by_youtube_url.await_count == 2


async def test_retry_stub_song_stops_for_open_circuit_and_exposes_programming_errors(
    conn: Connection,
) -> None:
    tdb = AsyncMock()
    tdb.lookup_by_youtube_url.side_effect = CircuitBreakerOpen("Unavailable")
    assert await retry_stub_song(1, ["video1", "video2"], conn, tdb) is None
    assert tdb.lookup_by_youtube_url.await_count == 1
    tdb.lookup_by_youtube_url.side_effect = TypeError("Bug")
    with pytest.raises(TypeError, match="Bug"):
        await retry_stub_song(1, ["video1"], conn, tdb)


def test_iter_retry_candidates_excludes_ineligible_and_deduplicates_videos(
    conn: Connection,
) -> None:
    _seed_dependencies(conn)
    conn.execute(
        songs.insert(),
        [
            {"id": 3, "title": "Original composition", "song_type": SongType.ORIGINAL},
            {"id": 4, "title": "Unavailable stub", "song_type": SongType.ARRANGEMENT},
        ],
    )
    conn.execute(youtube_videos.insert().values(id=3, video_id="unavailable", is_available=False))
    conn.execute(
        playlist_songs.insert(),
        [
            {
                "song_id": 3,
                "playlist_id": 1,
                "youtube_video_id": 1,
                "source_type": "INDIVIDUAL_VIDEO",
            },
            {
                "song_id": 4,
                "playlist_id": 1,
                "youtube_video_id": 3,
                "source_type": "INDIVIDUAL_VIDEO",
            },
        ],
    )
    assert iter_retry_candidates(conn) == [{"song_id": 1, "video_ids": ["video1"]}]


def test_replace_stub_with_song_preserves_simultaneous_removed_history(conn: Connection) -> None:
    _seed_dependencies(conn)
    removed_at = datetime(2020, 1, 1, tzinfo=UTC)
    conn.execute(
        playlist_songs.insert().values(
            id=5,
            song_id=2,
            playlist_id=2,
            youtube_video_id=2,
            source_type="INDIVIDUAL_VIDEO",
            removed_at=removed_at,
        )
    )
    replace_stub_with_song(1, 2, conn)
    rows = (
        conn.execute(sa.select(playlist_songs).where(playlist_songs.c.id.in_([4, 5])))
        .mappings()
        .all()
    )
    assert len(rows) == 2
    assert all(row["song_id"] == 2 for row in rows)
    assert rows[0]["removed_at"] != rows[1]["removed_at"]
    assert all(row["removed_at"].year == 2020 for row in rows)


async def test_retry_stub_song_keeps_new_missing_original_task_open(conn: Connection) -> None:
    tdb = AsyncMock()
    tdb.lookup_by_youtube_url.return_value = SongDetail(
        id=100, name="Canonical title", originalVersionId=999
    )
    tdb.resolve_original_chain.return_value = [999]
    assert await retry_stub_song(1, ["video1"], conn, tdb) == 2
    task = conn.execute(sa.select(tasks)).mappings().one()
    assert task["related_song_id"] == 2
    assert task["status"] == TaskStatus.OPEN
    assert task["auto_created_by"] == "stub_retry"
    assert task["data"] == {"song_id": 2, "original_touhoudb_ids": [999]}
    assert conn.execute(sa.select(songs.c.id)).scalars().all() == [2]


async def test_retry_stub_song_rolls_back_real_ingest_on_chain_failure(conn: Connection) -> None:
    tdb = AsyncMock()
    tdb.lookup_by_youtube_url.return_value = SongDetail(
        id=100, name="Refreshed title", originalVersionId=999
    )
    tdb.resolve_original_chain.side_effect = httpx.ReadTimeout("Chain unavailable")
    with pytest.raises(httpx.ReadTimeout):
        await retry_stub_song(1, ["video1"], conn, tdb)
    assert conn.execute(sa.select(songs.c.id).order_by(songs.c.id)).scalars().all() == [1, 2]
    assert (
        conn.execute(sa.select(songs.c.title).where(songs.c.id == 2)).scalar_one()
        == "Canonical title"
    )


@pytest.mark.parametrize("new_link", [True, False])
def test_replace_stub_with_song_updates_timestamp_only_for_new_links(
    conn: Connection, new_link: bool
) -> None:
    old_timestamp = datetime(2000, 1, 1)
    conn.execute(
        songs.update().values(notes=None, arrangement_chronicle_url=None, updated_at=old_timestamp)
    )
    conn.execute(
        song_languages.insert(),
        [
            {"song_id": 1, "language": "JAPANESE"},
            {"song_id": 2, "language": "ENGLISH" if new_link else "JAPANESE"},
        ],
    )
    replace_stub_with_song(1, 2, conn)
    actual = conn.execute(sa.select(songs.c.updated_at).where(songs.c.id == 2)).scalar_one()
    if new_link:
        assert actual > old_timestamp
    else:
        assert actual == old_timestamp


def test_replace_stub_with_song_keeps_distinct_dropped_tasks_open(conn: Connection) -> None:
    _seed_dependencies(conn)
    conn.execute(
        tasks.insert(),
        [
            {
                "id": 5,
                "task_type": TaskType.DROPPED_VIDEO,
                "title": "Stub drop",
                "related_song_id": 1,
                "related_video_id": 1,
                "data": {"song_id": 1, "song_ids": [1], "source_playlist_db_id": 1},
            },
            {
                "id": 6,
                "task_type": TaskType.DROPPED_VIDEO,
                "title": "Canonical drop",
                "related_song_id": 2,
                "related_video_id": 2,
                "data": {"song_id": 2, "song_ids": [2], "source_playlist_db_id": 2},
            },
        ],
    )
    replace_stub_with_song(1, 2, conn)
    rows = (
        conn.execute(sa.select(tasks).where(tasks.c.task_type == TaskType.DROPPED_VIDEO))
        .mappings()
        .all()
    )
    assert len(rows) == 2
    assert all(row["status"] == TaskStatus.OPEN for row in rows)
    assert all(row["related_song_id"] == 2 for row in rows)
    assert all(row["data"]["song_ids"] == [2] for row in rows)


def test_replace_stub_with_song_redirects_video_level_composite_payload(conn: Connection) -> None:
    _seed_dependencies(conn)
    conn.execute(
        tasks.insert().values(
            id=5,
            task_type=TaskType.DROPPED_VIDEO,
            title="Composite drop",
            related_video_id=1,
            data={"song_ids": [1, 2], "source_playlist_db_id": 1},
        )
    )
    replace_stub_with_song(1, 2, conn)
    row = conn.execute(sa.select(tasks).where(tasks.c.id == 5)).mappings().one()
    assert row["related_song_id"] is None
    assert row["data"]["song_ids"] == [2]
    assert row["status"] == TaskStatus.OPEN
