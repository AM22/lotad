"""Shared primitive: apply a TouhouDB ``SongDetail`` to the LOTAD database.

Wraps ``map_song_to_db`` + album-track linking + original-chain resolution +
integrity checks.  This is the inner core of ``IngestPipeline.ingest_video``
factored out so it can also be called by:

- ``lotad/sync/metadata_refresh.py`` — re-applies fresh TouhouDB data to an
  existing song without touching ``playlist_songs``.
- ``lotad/sync/stub_retry.py`` — lands the canonical row before redirecting a
  stub's links onto it.

Key difference from ``IngestPipeline.ingest_video``: this helper does NOT
create ``playlist_songs`` rows or upsert ``youtube_videos``.  The caller is
responsible for those.  Tasks for missing originals / suspicious metadata
are still created via the supplied ``task_creator`` callback.
"""

from __future__ import annotations

from typing import Any, Protocol

import sqlalchemy as sa
from sqlalchemy import Connection

from lotad.db.models import TaskType, original_songs, song_originals, songs
from lotad.ingestion.mappers import (
    link_album_tracks,
    link_song_originals,
    map_album_to_db,
    map_song_to_db,
)
from lotad.ingestion.touhoudb_client import OriginalChainError, TouhouDBClient
from lotad.ingestion.touhoudb_models import SongDetail
from lotad.ingestion.youtube_client import PlaylistItem
from lotad.tasks.manager import create_task_idempotent

# Differences above 20% usually indicate a wrong match rather than silence or an intro.
_DURATION_MISMATCH_RATIO = 0.20


class TaskCreator(Protocol):
    """Signature of the per-pipeline task creator (matches IngestPipeline._create_task)."""

    def __call__(
        self,
        task_type: TaskType,
        title: str,
        data: dict[str, Any],
        conn: Connection,
        *,
        related_song_id: int | None = None,
        related_video_id: int | None = None,
    ) -> None: ...


def make_task_creator(auto_created_by: str) -> TaskCreator:
    """Adapt the pipeline callback order to the task manager's connection-first API."""

    def create_task(
        task_type: TaskType,
        title: str,
        data: dict[str, Any],
        conn: Connection,
        *,
        related_song_id: int | None = None,
        related_video_id: int | None = None,
    ) -> None:
        create_task_idempotent(
            conn,
            task_type,
            title,
            data,
            related_song_id=related_song_id,
            related_video_id=related_video_id,
            auto_created_by=auto_created_by,
        )

    return create_task


async def apply_touhoudb_detail(
    detail: SongDetail,
    conn: Connection,
    tdb: TouhouDBClient,
    *,
    create_task: TaskCreator | None = None,
    integrity_yt_video_id: int | None = None,
    integrity_item: PlaylistItem | None = None,
    is_composite: bool = False,
) -> int:
    """Apply upstream metadata and complete original sets in the caller's transaction."""
    song_id = map_song_to_db(detail, conn)

    # Let I/O and DB errors reach the caller's transaction boundary: a partially
    # applied refresh must not be counted as successful.
    for album_summary in detail.albums:
        album_detail = await tdb.get_album(album_summary.id)
        album_db_id = map_album_to_db(album_detail, conn)
        link_album_tracks(album_db_id, album_detail, conn)

    original_ids = []
    if detail.originalVersionId is not None:
        original_ids = list(dict.fromkeys(await tdb.resolve_original_chain(detail.id, strict=True)))
        if not original_ids:
            raise OriginalChainError(f"No originals resolved for song {detail.id}")
    _reconcile_originals(song_id, original_ids, conn, create_task=create_task)

    if create_task is not None:
        _run_integrity_checks(
            detail,
            song_id,
            integrity_yt_video_id,
            integrity_item,
            conn,
            create_task=create_task,
            is_composite=is_composite,
        )

    return song_id


def _reconcile_originals(
    song_id: int,
    original_ids: list[int],
    conn: Connection,
    *,
    create_task: TaskCreator | None,
) -> None:
    """Replace upstream links only when every resolved original is catalogued."""
    before = set(
        conn.execute(
            sa.select(song_originals.c.original_song_id).where(song_originals.c.song_id == song_id)
        ).scalars()
    )
    catalogued = set(
        conn.execute(
            sa.select(original_songs.c.touhoudb_id).where(
                original_songs.c.touhoudb_id.in_(original_ids)
            )
        ).scalars()
    )
    missing = sorted(set(original_ids) - catalogued)
    linked = link_song_originals(song_id, original_ids, conn)
    if missing:
        if create_task is not None:
            create_task(
                TaskType.FILL_MISSING_INFO,
                f"Original song chain not in DB for song {song_id}",
                {"song_id": song_id, "original_touhoudb_ids": missing},
                conn,
                related_song_id=song_id,
            )
    else:
        conn.execute(
            song_originals.delete().where(
                song_originals.c.song_id == song_id,
                song_originals.c.is_manual.is_(False),
                song_originals.c.original_song_id.not_in(linked),
            )
        )
    after = set(
        conn.execute(
            sa.select(song_originals.c.original_song_id).where(song_originals.c.song_id == song_id)
        ).scalars()
    )
    if before != after:
        conn.execute(songs.update().where(songs.c.id == song_id).values(updated_at=sa.func.now()))


def _run_integrity_checks(
    detail: SongDetail,
    song_id: int,
    yt_video_id: int | None,
    item: PlaylistItem | None,
    conn: Connection,
    *,
    create_task: TaskCreator,
    is_composite: bool,
) -> None:
    """Check upstream metadata consistently during ingestion and refresh."""
    if (
        not is_composite
        and yt_video_id is not None
        and item is not None
        and detail.lengthSeconds
        and item.duration_seconds
        and abs(detail.lengthSeconds - item.duration_seconds) / max(detail.lengthSeconds, 1)
        > _DURATION_MISMATCH_RATIO
    ):
        create_task(
            TaskType.SUSPICIOUS_METADATA,
            f"Duration mismatch for song {song_id}: "
            f"TouhouDB={detail.lengthSeconds}s YT={item.duration_seconds}s",
            {
                "song_id": song_id,
                "touhoudb_duration": detail.lengthSeconds,
                "youtube_duration": item.duration_seconds,
            },
            conn,
            related_song_id=song_id,
            related_video_id=yt_video_id,
        )

    if detail.has_lyrics:
        has_lyricist = any("Lyricist" in c.role_list for c in detail.artists)
        if not has_lyricist:
            create_task(
                TaskType.MISSING_LYRICIST,
                f"Song {song_id} has lyrics but no lyricist credited",
                {"song_id": song_id},
                conn,
                related_song_id=song_id,
            )
