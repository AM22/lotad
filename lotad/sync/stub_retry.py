"""Retry TouhouDB matching for stub songs.

A "stub song" is a row with ``songs.touhoudb_id IS NULL`` — produced by
``ingest_song_from_llm_classification`` when the LLM extracted enough
metadata but no TouhouDB match existed at ingest time.  Some stubs will
never have a TouhouDB entry (genuine non-Touhou originals); others *might*
get one as TouhouDB grows.  We identify the latter as ``song_type != ORIGINAL``.

For each retry candidate, look up the associated YouTube video against
TouhouDB's ``/songs/byPv``.  On a hit, redirect the stub's ``playlist_songs``
and ``album_tracks`` rows to the canonical matched song, then delete the stub.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy import Connection

from lotad.db.models import (
    SongType,
    TaskStatus,
    TaskType,
    album_tracks,
    playlist_songs,
    songs,
    tasks,
    youtube_videos,
)
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.sync.touhoudb_ingest import apply_touhoudb_detail
from lotad.tasks.manager import create_task_idempotent

logger = logging.getLogger(__name__)


def iter_retry_candidates(conn: Connection) -> list[dict]:
    """Return stub song rows eligible for TouhouDB re-match.

    Each row carries: ``song_id``, ``video_ids`` (list of YouTube 11-char IDs
    associated via ``playlist_songs``).  Stubs with no associated video are
    excluded — there's nothing to look up.
    """
    stmt = (
        sa.select(
            songs.c.id.label("song_id"),
            youtube_videos.c.video_id,
        )
        .select_from(
            songs.join(playlist_songs, playlist_songs.c.song_id == songs.c.id).join(
                youtube_videos, playlist_songs.c.youtube_video_id == youtube_videos.c.id
            )
        )
        .where(
            sa.and_(
                songs.c.touhoudb_id.is_(None),
                songs.c.song_type != SongType.ORIGINAL,
                playlist_songs.c.removed_at.is_(None),
                youtube_videos.c.is_available.is_(True),
            )
        )
    )

    by_song: dict[int, list[str]] = {}
    for row in conn.execute(stmt).all():
        by_song.setdefault(row.song_id, []).append(row.video_id)

    return [{"song_id": sid, "video_ids": vids} for sid, vids in by_song.items()]


async def retry_stub_song(
    song_id: int,
    video_ids: list[str],
    conn: Connection,
    tdb: TouhouDBClient,
) -> int | None:
    """Attempt to match a stub song against TouhouDB.

    On match: applies the canonical TouhouDB detail, redirects ``playlist_songs``
    and ``album_tracks`` from the stub to the matched song, deletes the stub,
    and auto-resolves any open ``INGEST_FAILED`` / ``FILL_MISSING_INFO`` tasks
    pointing at the stub's video or song.  Returns the matched ``songs.id``.

    On no match: leaves the stub alone, returns ``None``.
    """
    detail = None
    matched_video_id: str | None = None
    for vid in video_ids:
        try:
            detail = await tdb.lookup_by_youtube_url(vid)
        except Exception:
            logger.exception("lookup_by_youtube_url failed for video %s", vid)
            continue
        if detail is not None:
            matched_video_id = vid
            break

    if detail is None:
        return None

    matched_song_id = await apply_touhoudb_detail(
        detail,
        conn,
        tdb,
        create_task=lambda *args, **kwargs: create_task_idempotent(
            conn, *args, auto_created_by="stub_retry", **kwargs
        ),
    )

    if matched_song_id != song_id:
        replace_stub_with_song(song_id, matched_song_id, conn)
    else:
        # The "stub" was actually already the canonical row (no upgrade needed)
        # — apply_touhoudb_detail just filled in the touhoudb_id.  Nothing to
        # redirect; just clean up tasks below.
        pass

    _resolve_open_tasks_for_stub(
        song_id=song_id,
        new_song_id=matched_song_id,
        matched_video_id=matched_video_id,
        conn=conn,
    )

    logger.info(
        "Stub-retry hit: stub song %d → matched song %d (via video %s)",
        song_id,
        matched_song_id,
        matched_video_id,
    )
    return matched_song_id


def replace_stub_with_song(stub_song_id: int, target_song_id: int, conn: Connection) -> None:
    """Re-point ``playlist_songs`` and ``album_tracks`` from stub to target.

    After redirection, deletes the stub row.  Defensive: skips delete if any
    rows still reference the stub (should never happen after the redirects).
    """
    if stub_song_id == target_song_id:
        return

    # Active rows in target's playlists, so we know which redirects would
    # collide with a UNIQUE (song_id, playlist_id, removed_at NULL) constraint.
    target_active_playlists = {
        row[0]
        for row in conn.execute(
            sa.select(playlist_songs.c.playlist_id).where(
                sa.and_(
                    playlist_songs.c.song_id == target_song_id,
                    playlist_songs.c.removed_at.is_(None),
                )
            )
        ).all()
    }

    stub_rows = list(
        conn.execute(
            sa.select(
                playlist_songs.c.id,
                playlist_songs.c.playlist_id,
                playlist_songs.c.removed_at,
            ).where(playlist_songs.c.song_id == stub_song_id)
        ).all()
    )

    for row in stub_rows:
        if row.removed_at is None and row.playlist_id in target_active_playlists:
            # Target already has an active row in this playlist; soft-delete
            # the stub's redirect to avoid violating the unique constraint.
            conn.execute(
                playlist_songs.update()
                .where(playlist_songs.c.id == row.id)
                .values(song_id=target_song_id, removed_at=datetime.now(UTC))
            )
        else:
            conn.execute(
                playlist_songs.update()
                .where(playlist_songs.c.id == row.id)
                .values(song_id=target_song_id)
            )

    conn.execute(
        album_tracks.update()
        .where(album_tracks.c.song_id == stub_song_id)
        .values(song_id=target_song_id)
    )

    # Defensive: only delete the stub if nothing references it anymore.
    remaining = conn.execute(
        sa.select(sa.func.count())
        .select_from(playlist_songs)
        .where(playlist_songs.c.song_id == stub_song_id)
    ).scalar_one()
    if remaining == 0:
        conn.execute(songs.delete().where(songs.c.id == stub_song_id))
    else:
        logger.warning(
            "Stub song %d still has %d playlist_songs references after redirect; not deleting",
            stub_song_id,
            remaining,
        )


def _resolve_open_tasks_for_stub(
    *,
    song_id: int,
    new_song_id: int,
    matched_video_id: str | None,
    conn: Connection,
) -> None:
    """Auto-resolve tasks made obsolete by the stub upgrade."""
    yt_db_id: int | None = None
    if matched_video_id is not None:
        row = conn.execute(
            sa.select(youtube_videos.c.id).where(youtube_videos.c.video_id == matched_video_id)
        ).one_or_none()
        if row is not None:
            yt_db_id = row[0]

    obsolete_types = (TaskType.INGEST_FAILED, TaskType.FILL_MISSING_INFO)
    filters = [
        sa.and_(
            tasks.c.task_type.in_(obsolete_types),
            tasks.c.status == TaskStatus.OPEN,
            tasks.c.related_song_id == song_id,
        ),
    ]
    if yt_db_id is not None:
        filters.append(
            sa.and_(
                tasks.c.task_type.in_(obsolete_types),
                tasks.c.status == TaskStatus.OPEN,
                tasks.c.related_video_id == yt_db_id,
            )
        )

    for predicate in filters:
        conn.execute(
            tasks.update()
            .where(predicate)
            .values(
                status=TaskStatus.RESOLVED,
                resolved_at=datetime.now(UTC),
                data=sa.cast(tasks.c.data, sa.dialects.postgresql.JSONB).op("||")(
                    sa.cast(
                        {
                            "auto_resolved_by": "stub_retry",
                            "resolved_song_id": new_song_id,
                        },
                        sa.dialects.postgresql.JSONB,
                    )
                ),
            )
        )
