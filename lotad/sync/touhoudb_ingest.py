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

import logging
from typing import Any, Protocol

from sqlalchemy import Connection

from lotad.db.models import TaskType
from lotad.ingestion.mappers import (
    link_album_tracks,
    link_song_originals,
    map_album_to_db,
    map_song_to_db,
)
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.touhoudb_models import SongDetail
from lotad.ingestion.youtube_client import PlaylistItem

logger = logging.getLogger(__name__)


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
    """
    Map a TouhouDB ``SongDetail`` into the LOTAD database.

    Returns the internal ``songs.id``.  Idempotent — re-running with the same
    detail overwrites TouhouDB-sourced fields and replaces ``song_artists`` /
    ``song_tags`` / ``song_languages``.

    Args:
        detail: TouhouDB song detail (output of ``tdb.get_song`` or similar).
        conn: open SQLAlchemy connection (caller manages the transaction).
        tdb: TouhouDB client for fetching album detail and resolving the
            original chain.
        create_task: callback for creating tasks (FILL_MISSING_INFO,
            SUSPICIOUS_METADATA, MISSING_LYRICIST).  If None, those tasks
            are skipped — useful for metadata refresh where we don't want
            to spam the task queue with already-known issues.
        integrity_yt_video_id: youtube_videos.id for duration-mismatch checks.
            If None, integrity checks are skipped.
        integrity_item: PlaylistItem to compare against detail.lengthSeconds.
        is_composite: when True, suppresses duration mismatch (the video
            covers many songs so a single-song duration always mismatches).
    """
    song_id = map_song_to_db(detail, conn)

    for album_summary in detail.albums:
        try:
            album_detail = await tdb.get_album(album_summary.id)
            album_db_id = map_album_to_db(album_detail, conn)
            link_album_tracks(album_db_id, album_detail, conn)
        except Exception:
            logger.exception(
                "Failed to ingest album touhoudb_id=%d for song %d — skipping",
                album_summary.id,
                song_id,
            )

    if detail.originalVersionId is not None:
        try:
            original_ids = await tdb.resolve_original_chain(detail.id)
            linked = link_song_originals(song_id, original_ids, conn)
            if not linked and create_task is not None:
                create_task(
                    TaskType.FILL_MISSING_INFO,
                    f"Original song chain not in DB for song {song_id}",
                    {"song_id": song_id, "original_touhoudb_ids": original_ids},
                    conn,
                    related_song_id=song_id,
                )
        except Exception:
            logger.exception("resolve_original_chain failed for song %d", song_id)

    if create_task is not None and integrity_yt_video_id is not None:
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


def _run_integrity_checks(
    detail: SongDetail,
    song_id: int,
    yt_video_id: int,
    item: PlaylistItem | None,
    conn: Connection,
    *,
    create_task: TaskCreator,
    is_composite: bool,
) -> None:
    """Mirror of ``IngestPipeline._integrity_checks``."""
    if (
        not is_composite
        and item is not None
        and detail.lengthSeconds
        and item.duration_seconds
        and abs(detail.lengthSeconds - item.duration_seconds) / max(detail.lengthSeconds, 1) > 0.20
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
