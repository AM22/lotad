"""Task management: queries, updates, and per-type resolution logic.

All public functions accept an open SQLAlchemy ``Connection`` (Core, not ORM)
and operate within the caller's transaction.  The caller is responsible for
commit/rollback.

Pattern mirrors lotad/ingestion/mappers.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lotad.db.models import (
    SongRole,
    TaskStatus,
    TaskType,
    artists,
    playlist_songs,
    song_artists,
    songs,
    tasks,
    youtube_videos,
)

# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------


def list_tasks(
    conn: Connection,
    *,
    task_type: TaskType | None = None,
    status: TaskStatus | None = TaskStatus.OPEN,
    limit: int = 200,
    offset: int = 0,
) -> list[Any]:
    """Return task rows ordered by priority ASC, created_at ASC."""
    stmt = sa.select(tasks).order_by(tasks.c.priority.asc(), tasks.c.created_at.asc())
    if task_type is not None:
        stmt = stmt.where(tasks.c.task_type == task_type)
    if status is not None:
        stmt = stmt.where(tasks.c.status == status)
    stmt = stmt.limit(limit).offset(offset)
    return list(conn.execute(stmt).mappings().all())


def get_task(conn: Connection, task_id: int) -> Any | None:
    """Return a single task row by ID, or None."""
    row = conn.execute(sa.select(tasks).where(tasks.c.id == task_id)).mappings().one_or_none()
    return row


def get_task_with_context(conn: Connection, task_id: int) -> dict[str, Any] | None:
    """
    Return a dict with keys:
      'task'        — task row mapping
      'song'        — songs row mapping (or None)
      'video'       — youtube_videos row mapping (or None)
      'song_artists' — list of song_artists + artists row mappings
    """
    task_row = get_task(conn, task_id)
    if task_row is None:
        return None

    song_row = None
    if task_row["related_song_id"] is not None:
        song_row = (
            conn.execute(sa.select(songs).where(songs.c.id == task_row["related_song_id"]))
            .mappings()
            .one_or_none()
        )

    video_row = None
    if task_row["related_video_id"] is not None:
        video_row = (
            conn.execute(
                sa.select(youtube_videos).where(youtube_videos.c.id == task_row["related_video_id"])
            )
            .mappings()
            .one_or_none()
        )

    artist_rows: list[Any] = []
    if task_row["related_song_id"] is not None:
        artist_rows = list(
            conn.execute(
                sa.select(
                    song_artists.c.role,
                    artists.c.name,
                    artists.c.artist_type,
                    artists.c.touhoudb_id,
                )
                .join(artists, song_artists.c.artist_id == artists.c.id)
                .where(song_artists.c.song_id == task_row["related_song_id"])
                .order_by(song_artists.c.role)
            )
            .mappings()
            .all()
        )

    return {
        "task": task_row,
        "song": song_row,
        "video": video_row,
        "song_artists": artist_rows,
    }


def count_tasks_by_type(
    conn: Connection,
    *,
    status: TaskStatus | None = TaskStatus.OPEN,
) -> dict[str, int]:
    """Return {task_type: count} grouped by type, filtered by status."""
    stmt = sa.select(tasks.c.task_type, sa.func.count().label("cnt")).group_by(tasks.c.task_type)
    if status is not None:
        stmt = stmt.where(tasks.c.status == status)
    rows = conn.execute(stmt).all()
    return {row.task_type: row.cnt for row in rows}


# Tasks that time out this many times are excluded from the automatic enrich
# batch.  They can still be force-enriched with `lotad tasks enrich --id <id>`
# or resolved manually via `lotad tasks resolve <id>`.
ENRICH_FAIL_LIMIT = 3


def list_unenriched_ingest_failed(
    conn: Connection,
    *,
    limit: int = 50,
) -> list[Any]:
    """Return OPEN INGEST_FAILED tasks that have not yet been LLM-enriched.

    Tasks that have timed out ``ENRICH_FAIL_LIMIT`` or more times are excluded
    from the batch queue — they block the front of every run and waste tokens
    retrying queries that TouhouDB consistently cannot answer.  They remain
    OPEN so the user can still resolve them manually via ``tasks resolve``.
    """
    stmt = (
        sa.select(tasks)
        .where(
            sa.and_(
                tasks.c.task_type == TaskType.INGEST_FAILED,
                tasks.c.status == TaskStatus.OPEN,
                tasks.c.llm_enriched_at.is_(None),
                # Exclude tasks that have already hit the per-task timeout limit.
                # data->>'enrich_fail_count' is NULL for tasks that have never
                # been attempted, so the IS NULL branch keeps them eligible.
                sa.or_(
                    tasks.c.data.op("->>")(sa.literal("enrich_fail_count")).is_(None),
                    sa.cast(
                        tasks.c.data.op("->>")(sa.literal("enrich_fail_count")),
                        sa.Integer,
                    )
                    < ENRICH_FAIL_LIMIT,
                ),
            )
        )
        .order_by(tasks.c.created_at.asc())
        .limit(limit)
    )
    return list(conn.execute(stmt).mappings().all())


# ---------------------------------------------------------------------------
# Update functions
# ---------------------------------------------------------------------------


def update_task_status(
    conn: Connection,
    task_id: int,
    status: TaskStatus,
    *,
    resolved_at: datetime | None = None,
) -> None:
    """Update task status. Auto-sets resolved_at for RESOLVED/DISMISSED."""
    values: dict[str, Any] = {"status": status}
    if status in (TaskStatus.RESOLVED, TaskStatus.DISMISSED):
        values["resolved_at"] = resolved_at or datetime.now(UTC)
    conn.execute(tasks.update().where(tasks.c.id == task_id).values(**values))


def merge_task_data(
    conn: Connection,
    task_id: int,
    extra_data: dict[str, Any],
) -> None:
    """
    Merge extra_data into the task's data JSON column using PostgreSQL ||.
    Also sets llm_enriched_at=now() when LLM results are present.
    """
    extra_values: dict[str, Any] = {
        "data": sa.cast(tasks.c.data, JSONB).op("||")(sa.cast(extra_data, JSONB)),
    }
    if "llm_match" in extra_data or "llm_classification" in extra_data:
        extra_values["llm_enriched_at"] = datetime.now(UTC)

    conn.execute(tasks.update().where(tasks.c.id == task_id).values(**extra_values))


def dismiss_task(
    conn: Connection,
    task_id: int,
    *,
    note: str | None = None,
) -> None:
    """Set status=DISMISSED. Optionally records a dismiss_note in data."""
    if note:
        merge_task_data(conn, task_id, {"dismiss_note": note})
    update_task_status(conn, task_id, TaskStatus.DISMISSED)


def bulk_dismiss_by_type(
    conn: Connection,
    task_type: TaskType,
    *,
    status_filter: TaskStatus = TaskStatus.OPEN,
) -> int:
    """Bulk-dismiss all tasks of a given type. Returns count dismissed."""
    now = datetime.now(UTC)
    result = conn.execute(
        tasks.update()
        .where(
            sa.and_(
                tasks.c.task_type == task_type,
                tasks.c.status == status_filter,
            )
        )
        .values(status=TaskStatus.DISMISSED, resolved_at=now)
    )
    return result.rowcount


# ---------------------------------------------------------------------------
# Resolution functions
# ---------------------------------------------------------------------------


def resolve_ingest_failed(
    conn: Connection,
    task_id: int,
    *,
    song_id: int,
) -> None:
    """Mark INGEST_FAILED as RESOLVED, linking to the ingested song."""
    merge_task_data(conn, task_id, {"resolved_song_id": song_id})
    update_task_status(conn, task_id, TaskStatus.RESOLVED)


def resolve_suspicious_metadata(
    conn: Connection,
    task_id: int,
    *,
    action: str,
    corrected_duration: int | None = None,
) -> None:
    """
    Resolve SUSPICIOUS_METADATA.

    action:
      "accept_touhoudb" — no DB change to songs; task resolved as-is
      "accept_youtube"  — update songs.duration_seconds to youtube value
      "manual"          — update songs.duration_seconds to corrected_duration
    """
    task_row = get_task(conn, task_id)
    if task_row is None:
        return

    if action == "accept_youtube":
        youtube_dur = task_row["data"].get("youtube_duration")
        if youtube_dur is not None and task_row["related_song_id"] is not None:
            conn.execute(
                songs.update()
                .where(songs.c.id == task_row["related_song_id"])
                .values(duration_seconds=youtube_dur)
            )
    elif action == "manual" and corrected_duration is not None:
        if task_row["related_song_id"] is not None:
            conn.execute(
                songs.update()
                .where(songs.c.id == task_row["related_song_id"])
                .values(duration_seconds=corrected_duration)
            )

    merge_task_data(conn, task_id, {"resolution_action": action})
    update_task_status(conn, task_id, TaskStatus.RESOLVED)


def resolve_deduplicate_songs(
    conn: Connection,
    task_id: int,
    *,
    keep_both: bool = False,
    remove_playlist_id: int | None = None,
    song_id: int | None = None,
) -> None:
    """
    Resolve DEDUPLICATE_SONGS.

    keep_both=True  — dismiss task; both entries stay
    remove_playlist_id — soft-delete playlist_songs entry for that playlist
    """
    if keep_both:
        dismiss_task(conn, task_id, note="intentional duplicate across playlists")
        return

    if remove_playlist_id is not None and song_id is not None:
        conn.execute(
            playlist_songs.update()
            .where(
                sa.and_(
                    playlist_songs.c.song_id == song_id,
                    playlist_songs.c.playlist_id == remove_playlist_id,
                    playlist_songs.c.removed_at.is_(None),
                )
            )
            .values(removed_at=datetime.now(UTC))
        )
        merge_task_data(conn, task_id, {"removed_playlist_id": remove_playlist_id})
        update_task_status(conn, task_id, TaskStatus.RESOLVED)


def resolve_missing_lyricist(
    conn: Connection,
    task_id: int,
    *,
    lyricist_name: str | None,
    song_id: int,
) -> None:
    """
    Resolve MISSING_LYRICIST.

    lyricist_name=None — accepted as unknown/uncredited; task resolved.
    lyricist_name set  — upsert artist, insert song_artists row.
    """
    if lyricist_name:
        # Upsert artist by name (no touhoudb_id)
        artist_stmt = (
            pg_insert(artists)
            .values(
                name=lyricist_name,
                artist_type="INDIVIDUAL",
            )
            .on_conflict_do_nothing()
            .returning(artists.c.id)
        )
        row = conn.execute(artist_stmt).one_or_none()
        if row is None:
            # Artist already exists — fetch ID
            row = conn.execute(
                sa.select(artists.c.id).where(artists.c.name == lyricist_name)
            ).one_or_none()
        if row is not None:
            artist_id = row[0]
            conn.execute(
                pg_insert(song_artists)
                .values(song_id=song_id, artist_id=artist_id, role=SongRole.LYRICIST)
                .on_conflict_do_nothing()
            )
            merge_task_data(
                conn, task_id, {"added_lyricist": lyricist_name, "artist_id": artist_id}
            )

    update_task_status(conn, task_id, TaskStatus.RESOLVED)
