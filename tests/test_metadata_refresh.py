"""Exercise metadata refresh against real SQL writes and mocked TouhouDB HTTP."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
import sqlalchemy as sa
from sqlalchemy import Engine

from lotad.config import Settings
from lotad.db.models import (
    TaskType,
    artists,
    original_songs,
    playlist_songs,
    playlists,
    song_artists,
    song_characters,
    song_originals,
    song_tags,
    songs,
    tasks,
    works,
    youtube_videos,
)
from lotad.ingestion import touhoudb_client
from lotad.ingestion.mappers import map_song_to_db
from lotad.ingestion.touhoudb_models import SongDetail
from lotad.sync import metadata_refresh

_API_URL = "https://touhoudb.test/api"
_OLD_TIMESTAMP = datetime(2000, 1, 1)


@pytest.fixture
def refresh_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'refresh.sqlite'}")
    # These paths do not use PostgreSQL arrays; use the actual table definitions
    # so the tests exercise the production upserts and transaction boundaries.
    with engine.begin() as conn:
        for table in (
            works,
            artists,
            songs,
            original_songs,
            song_originals,
            song_artists,
            song_characters,
            song_tags,
            youtube_videos,
            playlists,
            playlist_songs,
            tasks,
        ):
            table.create(conn)
    monkeypatch.setattr(metadata_refresh, "get_engine", lambda: engine)
    yield engine
    engine.dispose()


@pytest.fixture
def refresh_settings() -> Settings:
    return Settings(
        _env_file=None,
        database_url="sqlite://",
        anthropic_api_key="unused",
        youtube_api_key="unused",
        touhoudb_base_url=_API_URL,
        touhoudb_max_retries=1,
    )


def _uncached_http_client(base_url: str, timeout: float, cache_dir: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=base_url, timeout=timeout)


@pytest.fixture
def upstream(monkeypatch: pytest.MonkeyPatch) -> Iterator[respx.MockRouter]:
    monkeypatch.setattr(touhoudb_client, "build_async_client", _uncached_http_client)
    with respx.mock(assert_all_called=False) as router:
        yield router


def _seed_song(engine: Engine, detail: SongDetail) -> int:
    with engine.begin() as conn:
        song_id = map_song_to_db(detail, conn)
        conn.execute(songs.update().where(songs.c.id == song_id).values(updated_at=_OLD_TIMESTAMP))
        return song_id


def _seed_original_links(
    engine: Engine,
    song_id: int,
    links: list[tuple[int, bool]],
) -> None:
    with engine.begin() as conn:
        for touhoudb_id, is_manual in links:
            original_id = conn.execute(
                original_songs.insert()
                .values(touhoudb_id=touhoudb_id, name=f"Original {touhoudb_id}")
                .returning(original_songs.c.id)
            ).scalar_one()
            conn.execute(
                song_originals.insert().values(
                    song_id=song_id,
                    original_song_id=original_id,
                    is_manual=is_manual,
                )
            )


def _original_links(engine: Engine, song_id: int) -> dict[int, bool]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(original_songs.c.touhoudb_id, song_originals.c.is_manual)
            .select_from(song_originals.join(original_songs))
            .where(song_originals.c.song_id == song_id)
        ).all()
        return {row.touhoudb_id: row.is_manual for row in rows}


def _song_response(router: respx.MockRouter, detail: SongDetail) -> None:
    router.get(f"{_API_URL}/songs/{detail.id}").respond(json=detail.model_dump(mode="json"))
    router.get(f"{_API_URL}/songs/{detail.id}/for-edit").respond(json={})


def _task_rows(engine: Engine, song_id: int, task_type: TaskType) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        return [
            dict(row)
            for row in conn.execute(
                sa.select(tasks).where(
                    tasks.c.related_song_id == song_id,
                    tasks.c.task_type == task_type,
                )
            ).mappings()
        ]


async def test_refresh_songs_replaces_corrected_originals_preserving_manual_links(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Arrangement"))
    _seed_original_links(refresh_engine, song_id, [(300, False), (400, True)])
    with refresh_engine.begin() as conn:
        conn.execute(original_songs.insert().values(touhoudb_id=200, name="Correct original"))
    _song_response(upstream, SongDetail(id=100, name="Arrangement", originalVersionId=200))
    _song_response(upstream, SongDetail(id=200, name="Correct original", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    assert report.refreshed_song_ids == [song_id]
    assert _original_links(refresh_engine, song_id) == {200: False, 400: True}


async def test_refresh_songs_clears_removed_upstream_original_preserving_manual_link(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Composition"))
    _seed_original_links(refresh_engine, song_id, [(300, False), (400, True)])
    _song_response(upstream, SongDetail(id=100, name="Composition", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    assert _original_links(refresh_engine, song_id) == {400: True}


async def test_refresh_songs_partial_originals_preserve_links_and_create_review_task(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Medley"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])
    with refresh_engine.begin() as conn:
        conn.execute(original_songs.insert().values(touhoudb_id=200, name="Known original"))
    _song_response(
        upstream,
        SongDetail.model_validate(
            {
                "id": 100,
                "name": "Medley",
                "originalVersionId": 200,
                "webLinks": [{"url": "https://touhoudb.com/S/999"}],
            }
        ),
    )
    _song_response(upstream, SongDetail(id=200, name="Known original", songType="Original"))
    _song_response(upstream, SongDetail(id=999, name="Uncatalogued original", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id, song_id], settings=refresh_settings)

    assert report.errors == 0
    assert _original_links(refresh_engine, song_id) == {200: False, 300: False}
    review_tasks = _task_rows(refresh_engine, song_id, TaskType.FILL_MISSING_INFO)
    assert len(review_tasks) == 1
    assert review_tasks[0]["auto_created_by"] == "metadata_refresh"
    assert 999 in review_tasks[0]["data"]["original_touhoudb_ids"]


async def test_refresh_songs_missing_original_creates_task(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Arrangement"))
    _song_response(upstream, SongDetail(id=100, name="Arrangement", originalVersionId=999))
    _song_response(upstream, SongDetail(id=999, name="Uncatalogued original", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    assert report.refreshed == 1
    review_tasks = _task_rows(refresh_engine, song_id, TaskType.FILL_MISSING_INFO)
    assert len(review_tasks) == 1
    assert review_tasks[0]["data"]["original_touhoudb_ids"] == [999]
    assert review_tasks[0]["auto_created_by"] == "metadata_refresh"


@pytest.mark.parametrize("failing_resource", ["original", "album"])
async def test_refresh_songs_upstream_failure_rolls_back_and_continues(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
    failing_resource: str,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Keep old title"))
    next_song_id = _seed_song(refresh_engine, SongDetail(id=101, name="Next old title"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])
    detail_data: dict[str, Any] = {"id": 100, "name": "Uncommitted title"}
    if failing_resource == "original":
        detail_data["originalVersionId"] = 200
        upstream.get(f"{_API_URL}/songs/200").respond(500)
    else:
        detail_data["albums"] = [{"id": 700, "name": "Unavailable album"}]
        upstream.get(f"{_API_URL}/albums/700").respond(500)
    _song_response(upstream, SongDetail.model_validate(detail_data))
    _song_response(upstream, SongDetail(id=101, name="Next new title"))

    report = await metadata_refresh.refresh_songs(
        [song_id, next_song_id], settings=refresh_settings
    )

    assert report.errors == 1
    assert report.refreshed == 1
    assert report.refreshed_song_ids == [next_song_id]
    assert _original_links(refresh_engine, song_id) == {300: False}
    with refresh_engine.connect() as conn:
        old_song = conn.execute(sa.select(songs).where(songs.c.id == song_id)).mappings().one()
        assert old_song["title"] == "Keep old title"
        assert old_song["updated_at"] == _OLD_TIMESTAMP
        assert (
            conn.execute(sa.select(songs.c.title).where(songs.c.id == next_song_id)).scalar_one()
            == "Next new title"
        )


@pytest.mark.parametrize("changed", [False, True])
async def test_refresh_songs_updated_at_tracks_metadata_changes(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
    changed: bool,
) -> None:
    detail = SongDetail(id=100, name="Same title")
    song_id = _seed_song(refresh_engine, detail)
    if changed:
        detail = detail.model_copy(update={"name": "Changed title"})
    _song_response(upstream, detail)

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    with refresh_engine.connect() as conn:
        updated_at = conn.execute(
            sa.select(songs.c.updated_at).where(songs.c.id == song_id)
        ).scalar_one()
    if changed:
        assert updated_at > _OLD_TIMESTAMP
    else:
        assert updated_at == _OLD_TIMESTAMP


async def test_refresh_songs_checks_missing_lyricist_without_video(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Vocal arrangement"))
    _song_response(
        upstream,
        SongDetail.model_validate(
            {
                "id": 100,
                "name": "Vocal arrangement",
                "artists": [
                    {
                        "artist": {"id": 50, "name": "Singer", "artistType": "Vocalist"},
                        "effectiveRoles": "Vocalist",
                    }
                ],
            }
        ),
    )

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    assert len(_task_rows(refresh_engine, song_id, TaskType.MISSING_LYRICIST)) == 1


async def test_refresh_songs_dry_run_keeps_database_unchanged(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Keep title"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])

    report = await metadata_refresh.refresh_songs(
        [song_id], settings=refresh_settings, dry_run=True
    )

    assert report.errors == 0
    assert report.refreshed == 1
    assert len(upstream.calls) == 0
    assert _original_links(refresh_engine, song_id) == {300: False}
    with refresh_engine.connect() as conn:
        row = conn.execute(sa.select(songs).where(songs.c.id == song_id)).mappings().one()
        assert row["title"] == "Keep title"
        assert row["updated_at"] == _OLD_TIMESTAMP
        assert conn.execute(sa.select(sa.func.count()).select_from(tasks)).scalar_one() == 0


@pytest.mark.parametrize("broken_chain", ["missing", "cycle"])
async def test_refresh_songs_incomplete_chain_preserves_existing_metadata(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
    broken_chain: str,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Keep old title"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])
    _song_response(upstream, SongDetail(id=100, name="Uncommitted title", originalVersionId=200))
    if broken_chain == "missing":
        upstream.get(f"{_API_URL}/songs/200").respond(404)
    else:
        _song_response(
            upstream, SongDetail(id=200, name="Circular reference", originalVersionId=100)
        )

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 1
    assert report.refreshed == 0
    assert _original_links(refresh_engine, song_id) == {300: False}
    with refresh_engine.connect() as conn:
        row = conn.execute(sa.select(songs).where(songs.c.id == song_id)).mappings().one()
        assert row["title"] == "Keep old title"
        assert row["updated_at"] == _OLD_TIMESTAMP


async def test_refresh_songs_shared_original_across_branches_is_not_a_cycle(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Arrangement"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])
    with refresh_engine.begin() as conn:
        conn.execute(original_songs.insert().values(touhoudb_id=200, name="Shared original"))
    _song_response(
        upstream,
        SongDetail.model_validate(
            {
                "id": 100,
                "name": "Arrangement",
                "originalVersionId": 200,
                "webLinks": [{"url": "https://touhoudb.com/S/201"}],
            }
        ),
    )
    _song_response(upstream, SongDetail(id=200, name="Shared original", songType="Original"))
    _song_response(upstream, SongDetail(id=201, name="Alternate version", originalVersionId=200))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    assert report.refreshed == 1
    assert _original_links(refresh_engine, song_id) == {200: False}


@pytest.mark.parametrize("notes_on_branch", [False, True])
async def test_refresh_songs_unavailable_original_notes_preserve_metadata(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
    notes_on_branch: bool,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Keep old title"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])
    with refresh_engine.begin() as conn:
        conn.execute(
            original_songs.insert(),
            [
                {"touhoudb_id": 200, "name": "First original"},
                {"touhoudb_id": 202, "name": "Second original"},
            ],
        )
    _song_response(
        upstream,
        SongDetail.model_validate(
            {
                "id": 100,
                "name": "Uncommitted title",
                "originalVersionId": 200,
                "webLinks": [{"url": "https://touhoudb.com/S/201"}],
            }
        ),
    )
    _song_response(upstream, SongDetail(id=200, name="First original", songType="Original"))
    _song_response(upstream, SongDetail(id=201, name="Other arrangement", originalVersionId=202))
    _song_response(upstream, SongDetail(id=202, name="Second original", songType="Original"))
    unavailable_notes_id = 201 if notes_on_branch else 100
    upstream.get(f"{_API_URL}/songs/{unavailable_notes_id}/for-edit").respond(404)

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 1
    assert report.refreshed == 0
    assert _original_links(refresh_engine, song_id) == {300: False}
    with refresh_engine.connect() as conn:
        row = conn.execute(sa.select(songs).where(songs.c.id == song_id)).mappings().one()
        assert row["title"] == "Keep old title"
        assert row["updated_at"] == _OLD_TIMESTAMP


async def test_refresh_songs_truncated_original_chain_preserves_metadata(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    song_id = _seed_song(refresh_engine, SongDetail(id=100, name="Keep old title"))
    _seed_original_links(refresh_engine, song_id, [(300, False)])
    _song_response(upstream, SongDetail(id=100, name="Uncommitted title", originalVersionId=200))
    # This exceeds the resolver's default depth without requiring real I/O.
    for upstream_id in range(200, 212):
        _song_response(
            upstream,
            SongDetail(
                id=upstream_id, name="Intermediate version", originalVersionId=upstream_id + 1
            ),
        )
    _song_response(upstream, SongDetail(id=212, name="Distant original", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 1
    assert report.refreshed == 0
    assert _original_links(refresh_engine, song_id) == {300: False}
    with refresh_engine.connect() as conn:
        row = conn.execute(sa.select(songs).where(songs.c.id == song_id)).mappings().one()
        assert row["title"] == "Keep old title"
        assert row["updated_at"] == _OLD_TIMESTAMP


@pytest.mark.parametrize("changed_relationship", [None, "artist", "tag", "original"])
async def test_refresh_songs_updated_at_tracks_relationship_changes(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
    changed_relationship: str | None,
) -> None:
    detail_data: dict[str, Any] = {
        "id": 100,
        "name": "Arrangement",
        "originalVersionId": 200,
        "artists": [
            {
                "artist": {"id": 50, "name": "Arranger", "artistType": "Producer"},
                "effectiveRoles": "Arranger",
            }
        ],
        "tags": [{"count": 1, "tag": {"id": 1, "name": "Rock"}}],
    }
    song_id = _seed_song(refresh_engine, SongDetail.model_validate(detail_data))
    _seed_original_links(refresh_engine, song_id, [(200, False)])
    if changed_relationship == "artist":
        detail_data["artists"][0]["artist"]["id"] = 51
    elif changed_relationship == "tag":
        detail_data["tags"][0]["count"] = 2
    elif changed_relationship == "original":
        detail_data["originalVersionId"] = 201
        with refresh_engine.begin() as conn:
            conn.execute(original_songs.insert().values(touhoudb_id=201, name="Corrected original"))
    _song_response(upstream, SongDetail.model_validate(detail_data))
    original_id = detail_data["originalVersionId"]
    _song_response(upstream, SongDetail(id=original_id, name="Original", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    with refresh_engine.connect() as conn:
        updated_at = conn.execute(
            sa.select(songs.c.updated_at).where(songs.c.id == song_id)
        ).scalar_one()
    if changed_relationship is None:
        assert updated_at == _OLD_TIMESTAMP
    else:
        assert updated_at > _OLD_TIMESTAMP


async def test_refresh_songs_preserves_manual_provenance_for_matching_upstream_link(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    detail = SongDetail(id=100, name="Arrangement", originalVersionId=200)
    song_id = _seed_song(refresh_engine, detail)
    _seed_original_links(refresh_engine, song_id, [(200, True)])
    _song_response(upstream, detail)
    _song_response(upstream, SongDetail(id=200, name="Original", songType="Original"))

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    assert _original_links(refresh_engine, song_id) == {200: True}
    with refresh_engine.connect() as conn:
        assert (
            conn.execute(sa.select(songs.c.updated_at).where(songs.c.id == song_id)).scalar_one()
            == _OLD_TIMESTAMP
        )


async def test_refresh_songs_stores_bilingual_notes_as_text(
    refresh_engine: Engine,
    refresh_settings: Settings,
    upstream: respx.MockRouter,
) -> None:
    detail = SongDetail.model_validate(
        {
            "id": 100,
            "name": "Arrangement",
            "notes": {"english": "English notes", "original": "日本語の備考"},
        }
    )
    song_id = _seed_song(refresh_engine, detail)
    _song_response(upstream, detail)

    report = await metadata_refresh.refresh_songs([song_id], settings=refresh_settings)

    assert report.errors == 0
    with refresh_engine.connect() as conn:
        row = conn.execute(sa.select(songs).where(songs.c.id == song_id)).mappings().one()
        assert row["notes"] == "English notes\n日本語の備考"
        assert row["updated_at"] == _OLD_TIMESTAMP
