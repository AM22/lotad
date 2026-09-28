"""Promote unmatched arrangement stubs when TouhouDB gains their videos."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

import httpx
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lotad.db.models import (
    SongType,
    TaskStatus,
    TaskType,
    album_tracks,
    physical_tracks,
    playlist_songs,
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
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.sync.touhoudb_ingest import apply_touhoudb_detail, make_task_creator

logger = logging.getLogger(__name__)


class RetryCandidate(TypedDict):
    song_id: int
    video_ids: list[str]


def iter_retry_candidates(conn: Connection) -> list[RetryCandidate]:
    """Return arrangement stubs with available videos in active playlists."""
    stmt = (
        sa.select(songs.c.id.label("song_id"), youtube_videos.c.video_id)
        .select_from(
            songs.join(playlist_songs, playlist_songs.c.song_id == songs.c.id).join(
                youtube_videos, playlist_songs.c.youtube_video_id == youtube_videos.c.id
            )
        )
        .where(
            songs.c.touhoudb_id.is_(None),
            songs.c.song_type != SongType.ORIGINAL,
            playlist_songs.c.removed_at.is_(None),
            youtube_videos.c.is_available.is_(True),
        )
        .distinct()
        .order_by(songs.c.id, youtube_videos.c.video_id)
    )
    by_song: dict[int, list[str]] = {}
    for row in conn.execute(stmt).mappings().all():
        by_song.setdefault(row["song_id"], []).append(row["video_id"])
    return [{"song_id": sid, "video_ids": vids} for sid, vids in by_song.items()]


async def retry_stub_song(
    song_id: int,
    video_ids: list[str],
    conn: Connection,
    tdb: TouhouDBClient,
) -> int | None:
    """Apply a video match and atomically transfer the stub's metadata and history."""
    detail = None
    matched_video_id: str | None = None
    for vid in video_ids:
        try:
            detail = await tdb.lookup_by_youtube_url(vid)
        except (httpx.HTTPError, ValidationError):
            logger.exception(f"lookup_by_youtube_url failed for video {vid!r}")
            continue
        except CircuitBreakerOpen:
            logger.warning("TouhouDB circuit breaker is open; deferring stub retry")
            return None
        if detail is not None:
            matched_video_id = vid
            break
    if detail is None:
        return None

    # A failed dependency transfer must also undo the canonical ingest and tasks.
    with conn.begin_nested():
        matched_song_id = await apply_touhoudb_detail(
            detail, conn, tdb, create_task=make_task_creator("stub_retry")
        )
        _resolve_open_tasks_for_stub(
            song_id=song_id,
            new_song_id=matched_song_id,
            matched_video_id=matched_video_id,
            conn=conn,
        )
        replace_stub_with_song(song_id, matched_song_id, conn)

    logger.info(
        f"Stub-retry hit: stub song {song_id} → matched song {matched_song_id} "
        f"(via video {matched_video_id!r})"
    )
    return matched_song_id


def replace_stub_with_song(stub_song_id: int, target_song_id: int, conn: Connection) -> None:
    """Merge all song dependencies while preserving canonical values and local history."""
    if stub_song_id == target_song_id:
        return
    with conn.begin_nested():
        rows = (
            conn.execute(
                sa.select(songs)
                .where(songs.c.id.in_([stub_song_id, target_song_id]))
                .order_by(songs.c.id)
                .with_for_update()
            )
            .mappings()
            .all()
        )
        by_id = {row["id"]: row for row in rows}
        if stub_song_id not in by_id or target_song_id not in by_id:
            raise ValueError("Both the stub and target song must exist")
        stub, target = by_id[stub_song_id], by_id[target_song_id]
        if stub["touhoudb_id"] is not None or target["touhoudb_id"] is None:
            raise ValueError("Stub promotion requires an unmatched stub and a canonical target")

        _merge_song_metadata(dict(stub), dict(target), conn)
        links_changed = False
        for table in (song_artists, song_originals, song_languages, song_characters, song_tags):
            links_changed = (
                _merge_song_links(table, stub_song_id, target_song_id, conn) or links_changed
            )
        if links_changed:
            conn.execute(
                songs.update().where(songs.c.id == target_song_id).values(updated_at=sa.func.now())
            )
        _redirect_tasks(stub_song_id, target_song_id, conn)
        _redirect_playlist_entries(stub_song_id, target_song_id, conn)
        for table in (album_tracks, physical_tracks):
            conn.execute(
                table.update().where(table.c.song_id == stub_song_id).values(song_id=target_song_id)
            )
        # Restrictive foreign keys still guard any future dependency we have not handled.
        conn.execute(songs.delete().where(songs.c.id == stub_song_id))


def _merge_song_metadata(stub: dict[str, Any], target: dict[str, Any], conn: Connection) -> None:
    values = {
        field: stub[field]
        for field in (
            "title_romanized",
            "duration_seconds",
            "publish_date",
            "arrangement_chronicle_url",
        )
        if target[field] is None and stub[field] is not None
    }
    if stub["notes"] and stub["notes"] != target["notes"]:
        values["notes"] = (
            f"{target['notes']}\n\nLocal notes from stub {stub['id']}:\n{stub['notes']}"
            if target["notes"]
            else stub["notes"]
        )
    if values:
        conn.execute(songs.update().where(songs.c.id == target["id"]).values(**values))


def _merge_song_links(
    table: sa.Table, stub_song_id: int, target_song_id: int, conn: Connection
) -> bool:
    changed = False
    rows = conn.execute(sa.select(table).where(table.c.song_id == stub_song_id)).mappings().all()
    for row in rows:
        values = dict(row) | {"song_id": target_song_id}
        if table is song_originals:
            # Stub originals came from local extraction/review and must survive upstream refresh.
            values["is_manual"] = True
            stmt = (
                pg_insert(table)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=["song_id", "original_song_id"],
                    set_={"is_manual": True},
                    where=table.c.is_manual.is_(False),
                )
            )
        else:
            stmt = pg_insert(table).values(**values).on_conflict_do_nothing()
        changed = conn.execute(stmt).rowcount > 0 or changed
    conn.execute(table.delete().where(table.c.song_id == stub_song_id))
    return changed


def _redirect_playlist_entries(stub_song_id: int, target_song_id: int, conn: Connection) -> None:
    target_rows = (
        conn.execute(
            sa.select(playlist_songs)
            .where(playlist_songs.c.song_id == target_song_id)
            .order_by(playlist_songs.c.id)
            .with_for_update()
        )
        .mappings()
        .all()
    )
    target_by_playlist: dict[int, Any] = {
        row["playlist_id"]: row for row in target_rows if row["removed_at"] is None
    }
    historical_keys = {
        (row["playlist_id"], row["removed_at"])
        for row in target_rows
        if row["removed_at"] is not None
    }
    stub_rows = (
        conn.execute(
            sa.select(playlist_songs)
            .where(playlist_songs.c.song_id == stub_song_id)
            .order_by(playlist_songs.c.id)
            .with_for_update()
        )
        .mappings()
        .all()
    )
    for row in stub_rows:
        values: dict[str, Any] = {"song_id": target_song_id}
        target = target_by_playlist.get(row["playlist_id"])
        if row["removed_at"] is None:
            if target is None:
                target_by_playlist[row["playlist_id"]] = row
            else:
                # Keep the duplicate upload in history, retaining a local rank if none is set.
                values["removed_at"] = datetime.now(UTC)
                if target["rank"] is None and row["rank"] is not None:
                    conn.execute(
                        playlist_songs.update()
                        .where(playlist_songs.c.id == target["id"])
                        .values(rank=row["rank"])
                    )
                    target_by_playlist[row["playlist_id"]] = dict(target) | {"rank": row["rank"]}
        removed_at = values.get("removed_at", row["removed_at"])
        if removed_at is not None:
            # The legacy uniqueness constraint includes removal time; retain simultaneous history.
            while (row["playlist_id"], removed_at) in historical_keys:
                removed_at += timedelta(microseconds=1)
            values["removed_at"] = removed_at
            historical_keys.add((row["playlist_id"], removed_at))
        conn.execute(
            playlist_songs.update().where(playlist_songs.c.id == row["id"]).values(**values)
        )


def _redirect_tasks(stub_song_id: int, target_song_id: int, conn: Connection) -> None:
    target_tasks = (
        conn.execute(
            sa.select(tasks.c.id, tasks.c.task_type)
            .where(tasks.c.related_song_id == target_song_id, tasks.c.status == TaskStatus.OPEN)
            .with_for_update()
        )
        .mappings()
        .all()
    )
    # Dropped tasks are keyed by upload and playlist, which the song merge does not change.
    open_by_type = {
        row["task_type"]: row["id"]
        for row in target_tasks
        if row["task_type"] != TaskType.DROPPED_VIDEO
    }
    stub_tasks = (
        conn.execute(
            sa.select(tasks)
            .where(
                sa.or_(
                    tasks.c.related_song_id == stub_song_id,
                    tasks.c.related_video_id.in_(
                        sa.select(playlist_songs.c.youtube_video_id).where(
                            playlist_songs.c.song_id == stub_song_id
                        )
                    ),
                )
            )
            .with_for_update()
        )
        .mappings()
        .all()
    )
    for row in stub_tasks:
        data = dict(row["data"])
        moves_song_link = row["related_song_id"] == stub_song_id
        if (
            not moves_song_link
            and data.get("song_id") != stub_song_id
            and stub_song_id not in data.get("song_ids", [])
        ):
            continue
        data["promoted_from_song_id"] = stub_song_id
        # Resolution wizards prefer these payload IDs over the task foreign key.
        for key in ("song_id", "resolved_song_id"):
            if data.get(key) == stub_song_id:
                data[key] = target_song_id
        if "song_ids" in data:
            data["song_ids"] = list(
                dict.fromkeys(
                    target_song_id if sid == stub_song_id else sid for sid in data["song_ids"]
                )
            )
        values: dict[str, Any] = {"data": data}
        if moves_song_link:
            values["related_song_id"] = target_song_id
        if (
            moves_song_link
            and row["status"] == TaskStatus.OPEN
            and row["task_type"] != TaskType.DROPPED_VIDEO
        ):
            if (existing_id := open_by_type.get(row["task_type"])) is not None:
                # Preserve the duplicate's payload and history without violating the OPEN index.
                data["merged_into_task_id"] = existing_id
                values.update(status=TaskStatus.DISMISSED, resolved_at=datetime.now(UTC))
            else:
                open_by_type[row["task_type"]] = row["id"]
        conn.execute(tasks.update().where(tasks.c.id == row["id"]).values(**values))


def _resolve_open_tasks_for_stub(
    *,
    song_id: int,
    new_song_id: int,
    matched_video_id: str | None,
    conn: Connection,
) -> None:
    yt_db_id = (
        conn.execute(
            sa.select(youtube_videos.c.id).where(youtube_videos.c.video_id == matched_video_id)
        ).scalar_one_or_none()
        if matched_video_id is not None
        else None
    )
    filters = [tasks.c.related_song_id == song_id]
    if yt_db_id is not None:
        filters.append(
            sa.and_(tasks.c.related_video_id == yt_db_id, tasks.c.related_song_id.is_(None))
        )
    # A successful match proves ingestion succeeded, but not that missing metadata was repaired.
    rows = (
        conn.execute(
            sa.select(tasks)
            .where(
                tasks.c.task_type == TaskType.INGEST_FAILED,
                tasks.c.status == TaskStatus.OPEN,
                sa.or_(*filters),
            )
            .with_for_update()
        )
        .mappings()
        .all()
    )
    for row in rows:
        conn.execute(
            tasks.update()
            .where(tasks.c.id == row["id"])
            .values(
                status=TaskStatus.RESOLVED,
                resolved_at=datetime.now(UTC),
                data=dict(row["data"])
                | {
                    "auto_resolved_by": "stub_retry",
                    "resolved_song_id": new_song_id,
                },
            )
        )
