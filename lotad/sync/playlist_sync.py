"""Continuous playlist sync.

Diffs each tracked YouTube playlist against ``playlist_songs``, then routes
each change through one of the documented outcomes.  See plan
``it-s-a-bit-out-tidy-shore.md`` for the full decision tree.

Phases per run:
    1. Diff           — fetch YT, snapshot DB, compute added/removed/kept sets.
    2. Move resolution — collapse cross-playlist moves before any ingest fires.
    3. Add            — ingest remaining net-new videos via IngestPipeline.
    4. Remove         — walk the decision tree for remaining removed items.
    5. Dedup-task reconciliation — auto-resolve stale DEDUPLICATE_SONGS tasks.
    6. Stub retry     — opportunistically re-match stub songs (default-on).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import JSONB

from lotad.config import Settings, get_settings
from lotad.db.models import (
    SourceType,
    TaskStatus,
    TaskType,
    playlist_songs,
    playlists,
    tasks,
    youtube_videos,
)
from lotad.db.session import get_engine
from lotad.ingestion.pipeline import IngestPipeline
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.youtube_client import PlaylistItem, YouTubeClient
from lotad.sync.stub_retry import iter_retry_candidates, retry_stub_song
from lotad.tasks.manager import create_task_idempotent

logger = logging.getLogger(__name__)


# Playlists whose drops are silently moved to ``unsaved`` (case 4 in the
# decision tree).  Anything else creates a DROPPED_VIDEO task.
_LOW_TIER_DISPLAY_ORDERS = (4, 5)
_UNSAVED_DISPLAY_ORDER = 6


@dataclass
class PerPlaylistOutcome:
    added: int = 0
    moved_in: int = 0
    moved_out: int = 0
    same_song_swap: int = 0
    silent_drop: int = 0
    task_drop: int = 0
    dead_replacement: int = 0
    deleted_in_place: int = 0
    kept: int = 0
    errors: int = 0


@dataclass
class SyncReport:
    per_playlist: dict[str, PerPlaylistOutcome] = field(default_factory=dict)
    stub_promoted: int = 0
    stub_no_match: int = 0
    dedup_tasks_reconciled: int = 0
    errors: int = 0


@dataclass
class _PlaylistSnapshot:
    playlist_db_id: int
    playlist_name: str
    youtube_playlist_id: str
    yt_items: dict[str, PlaylistItem]  # video_id → item
    db_rows: dict[
        str, dict[str, Any]
    ]  # video_id → {playlist_song_id, song_id, yt_db_id, is_available}


@dataclass
class _Diff:
    added_video_ids: set[str]
    removed_video_ids: set[str]
    kept_video_ids: set[str]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def sync_playlists(
    playlist_ids: list[str] | None = None,
    *,
    settings: Settings | None = None,
    retry_stubs: bool = True,
    limit: int | None = None,
) -> SyncReport:
    """Sync the named YouTube playlists (default: all tracked) against LOTAD.

    ``playlist_ids`` is a list of YouTube playlist IDs; pass None to sync every
    playlist that has a row in the ``playlists`` table (excluding the synthetic
    ``unsaved`` playlist).
    """
    settings = settings or get_settings()
    syncer = _PlaylistSyncer(settings, retry_stubs=retry_stubs, limit=limit)
    return await syncer.run(playlist_ids)


# ---------------------------------------------------------------------------
# Implementation
# ---------------------------------------------------------------------------


class _PlaylistSyncer:
    def __init__(
        self,
        settings: Settings,
        *,
        retry_stubs: bool = True,
        limit: int | None = None,
    ) -> None:
        self._settings = settings
        self._retry_stubs = retry_stubs
        self._limit = limit
        self._engine = get_engine()
        self._yt = YouTubeClient(settings)

    async def run(self, playlist_ids: list[str] | None) -> SyncReport:
        report = SyncReport()
        targets = self._resolve_targets(playlist_ids)

        # Phase 1 — diff per playlist
        snapshots: dict[int, _PlaylistSnapshot] = {}
        diffs: dict[int, _Diff] = {}
        for tgt in targets:
            try:
                snap = self._snapshot(tgt)
                snapshots[snap.playlist_db_id] = snap
                diffs[snap.playlist_db_id] = _compute_diff(snap)
                report.per_playlist[snap.playlist_name] = PerPlaylistOutcome(
                    kept=len(diffs[snap.playlist_db_id].kept_video_ids)
                )
            except Exception:
                logger.exception("Snapshot failed for playlist %r", tgt)
                report.errors += 1
                continue

        if not snapshots:
            return report

        # Phase 1b — bulk update last_checked_at for kept rows + handle in-place
        # availability transitions (kept-but-now-unavailable).
        for pid, snap in snapshots.items():
            self._update_kept_videos(snap, diffs[pid], report.per_playlist[snap.playlist_name])

        # Phase 2 — cross-playlist move resolution
        self._resolve_cross_playlist_moves(snapshots, diffs, report)

        # Phase 3 — ingest remaining adds
        async with IngestPipeline(self._settings) as pipeline:
            for pid, snap in snapshots.items():
                outcome = report.per_playlist[snap.playlist_name]
                for video_id in list(diffs[pid].added_video_ids):
                    item = snap.yt_items[video_id]
                    try:
                        await pipeline.ingest_video(item, playlist_db_id=pid)
                        outcome.added += 1
                    except Exception:
                        logger.exception(
                            "Ingest failed for %s in playlist %s",
                            video_id,
                            snap.playlist_name,
                        )
                        outcome.errors += 1

        # Phase 4 — process remaining removals
        for pid, snap in snapshots.items():
            outcome = report.per_playlist[snap.playlist_name]
            for video_id in list(diffs[pid].removed_video_ids):
                self._handle_removal(snap, video_id, outcome)

        # Phase 5 — reconcile stale DEDUPLICATE_SONGS tasks
        report.dedup_tasks_reconciled = self._reconcile_dedup_tasks()

        # Phase 6 — stub retry
        if self._retry_stubs:
            async with TouhouDBClient.from_settings(self._settings) as tdb:
                report.stub_promoted, report.stub_no_match = await self._run_stub_retry(tdb)

        return report

    # ------------------------------------------------------------------
    # Phase helpers
    # ------------------------------------------------------------------

    def _resolve_targets(self, playlist_ids: list[str] | None) -> list[dict[str, Any]]:
        with self._engine.connect() as conn:
            stmt = sa.select(
                playlists.c.id,
                playlists.c.name,
                playlists.c.youtube_playlist_id,
                playlists.c.display_order,
            )
            if playlist_ids:
                stmt = stmt.where(playlists.c.youtube_playlist_id.in_(playlist_ids))
            else:
                # Skip the synthetic "unsaved" playlist (it has a sentinel
                # YouTube ID and no real playlist to fetch).
                stmt = stmt.where(playlists.c.youtube_playlist_id.notlike("__lotad_%"))
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def _snapshot(self, target: dict[str, Any]) -> _PlaylistSnapshot:
        items = {
            it.video_id: it
            for it in self._yt.list_playlist_items(target["youtube_playlist_id"], limit=self._limit)
        }
        with self._engine.connect() as conn:
            rows = (
                conn.execute(
                    sa.select(
                        playlist_songs.c.id.label("playlist_song_id"),
                        playlist_songs.c.song_id,
                        youtube_videos.c.id.label("yt_db_id"),
                        youtube_videos.c.video_id,
                        youtube_videos.c.is_available,
                    )
                    .select_from(
                        playlist_songs.join(
                            youtube_videos,
                            playlist_songs.c.youtube_video_id == youtube_videos.c.id,
                        )
                    )
                    .where(
                        sa.and_(
                            playlist_songs.c.playlist_id == target["id"],
                            playlist_songs.c.removed_at.is_(None),
                        )
                    )
                )
                .mappings()
                .all()
            )
        db_rows = {r["video_id"]: dict(r) for r in rows}
        return _PlaylistSnapshot(
            playlist_db_id=target["id"],
            playlist_name=target["name"],
            youtube_playlist_id=target["youtube_playlist_id"],
            yt_items=items,
            db_rows=db_rows,
        )

    def _update_kept_videos(
        self,
        snap: _PlaylistSnapshot,
        diff: _Diff,
        outcome: PerPlaylistOutcome,
    ) -> None:
        """Bump last_checked_at; flag deleted-in-place transitions.

        For a kept video where YouTube now reports is_available=False, do NOT
        overwrite the existing title/description/etc. — just flip is_available
        and bump last_checked_at, then idempotent-create a DROPPED_VIDEO task.
        """
        now = datetime.now(UTC)
        with self._engine.begin() as conn:
            for video_id in diff.kept_video_ids:
                yt_item = snap.yt_items[video_id]
                db_row = snap.db_rows[video_id]

                if yt_item.is_available:
                    # Standard refresh: update title/description/duration/channel,
                    # bump last_checked_at + updated_at.
                    conn.execute(
                        youtube_videos.update()
                        .where(youtube_videos.c.id == db_row["yt_db_id"])
                        .values(
                            title=yt_item.title,
                            channel_id=yt_item.channel_id or None,
                            channel_name=yt_item.channel_name or None,
                            description=yt_item.description or None,
                            duration_seconds=yt_item.duration_seconds,
                            is_available=True,
                            last_checked_at=now,
                            updated_at=sa.func.now(),
                        )
                    )
                    continue

                # Newly unavailable (or still unavailable from a prior sync).
                # Preserve the existing title/description/channel/duration —
                # only flip availability + checked-at.
                conn.execute(
                    youtube_videos.update()
                    .where(youtube_videos.c.id == db_row["yt_db_id"])
                    .values(
                        is_available=False,
                        last_checked_at=now,
                        updated_at=sa.func.now(),
                    )
                )
                if db_row["is_available"]:
                    # Just transitioned from available → unavailable.
                    create_task_idempotent(
                        conn,
                        TaskType.DROPPED_VIDEO,
                        f"Deleted or private video still in playlist: {video_id!r}",
                        {
                            "video_id": video_id,
                            "title": yt_item.title,
                            "playlist_db_id": snap.playlist_db_id,
                            "reason": "deleted",
                            "note": (
                                "YouTube returned a deleted/private stub on sync; "
                                "video is no longer accessible"
                            ),
                        },
                        related_video_id=db_row["yt_db_id"],
                        auto_created_by="playlist_sync",
                    )
                    outcome.deleted_in_place += 1

    def _resolve_cross_playlist_moves(
        self,
        snapshots: dict[int, _PlaylistSnapshot],
        diffs: dict[int, _Diff],
        report: SyncReport,
    ) -> None:
        """Phase 2: collapse video_ids that appear in both an added and removed set.

        These are user-initiated moves between playlists.  Update playlist_id
        in place; skip the add and remove phases for them.
        """
        added_index: dict[str, int] = {}
        for pid, diff in diffs.items():
            for vid in diff.added_video_ids:
                added_index[vid] = pid

        for pid, diff in diffs.items():
            for vid in list(diff.removed_video_ids):
                target_pid = added_index.get(vid)
                if target_pid is None or target_pid == pid:
                    continue
                # video_id is in playlist `pid`'s removed and `target_pid`'s added.
                source_snap = snapshots[pid]
                target_snap = snapshots[target_pid]
                source_outcome = report.per_playlist[source_snap.playlist_name]
                target_outcome = report.per_playlist[target_snap.playlist_name]
                self._move_playlist_song(source_snap, target_snap, vid)
                source_outcome.moved_out += 1
                target_outcome.moved_in += 1
                diff.removed_video_ids.discard(vid)
                diffs[target_pid].added_video_ids.discard(vid)

    def _move_playlist_song(
        self,
        source_snap: _PlaylistSnapshot,
        target_snap: _PlaylistSnapshot,
        video_id: str,
    ) -> None:
        ps_id = source_snap.db_rows[video_id]["playlist_song_id"]
        with self._engine.begin() as conn:
            conn.execute(
                playlist_songs.update()
                .where(playlist_songs.c.id == ps_id)
                .values(playlist_id=target_snap.playlist_db_id)
            )

    def _handle_removal(
        self,
        snap: _PlaylistSnapshot,
        video_id: str,
        outcome: PerPlaylistOutcome,
    ) -> None:
        """Phase 4 decision tree for a single (playlist, video_id) removal."""
        db_row = snap.db_rows[video_id]
        now = datetime.now(UTC)

        with self._engine.begin() as conn:
            # Case 6 — already-dead video that the user removed: confirms the
            # user replaced it elsewhere.  Soft-delete the row and auto-resolve
            # any open DROPPED_VIDEO task on this video.
            if not db_row["is_available"]:
                conn.execute(
                    playlist_songs.update()
                    .where(playlist_songs.c.id == db_row["playlist_song_id"])
                    .values(removed_at=now)
                )
                _auto_resolve_dropped_video_tasks(
                    conn,
                    yt_db_id=db_row["yt_db_id"],
                    note="auto-resolved on sync — user removed the dead stub from the playlist",
                )
                outcome.dead_replacement += 1
                return

            # Case 3 — same-song-different-video swap: the same song_id has
            # another active playlist_songs row anywhere.  We check after
            # phase 3's ingests so the new row is already in place.
            other_active = conn.execute(
                sa.select(sa.func.count())
                .select_from(playlist_songs)
                .where(
                    sa.and_(
                        playlist_songs.c.song_id == db_row["song_id"],
                        playlist_songs.c.id != db_row["playlist_song_id"],
                        playlist_songs.c.removed_at.is_(None),
                    )
                )
            ).scalar_one()
            if other_active > 0:
                conn.execute(
                    playlist_songs.update()
                    .where(playlist_songs.c.id == db_row["playlist_song_id"])
                    .values(removed_at=now)
                )
                outcome.same_song_swap += 1
                return

            # Case 4 / 5 — genuine drop.  Tier determines silent vs. task.
            display_order = conn.execute(
                sa.select(playlists.c.display_order).where(playlists.c.id == snap.playlist_db_id)
            ).scalar_one()
            unsaved_id = conn.execute(
                sa.select(playlists.c.id).where(playlists.c.display_order == _UNSAVED_DISPLAY_ORDER)
            ).scalar_one()

            if display_order in _LOW_TIER_DISPLAY_ORDERS:
                # Silent reassignment to "unsaved".
                _move_to_unsaved(conn, db_row["playlist_song_id"], unsaved_id)
                outcome.silent_drop += 1
                return

            # High-tier drop: create a DROPPED_VIDEO task; user resolves it.
            create_task_idempotent(
                conn,
                TaskType.DROPPED_VIDEO,
                f"Song removed from {snap.playlist_name}: video {video_id!r}",
                {
                    "video_id": video_id,
                    "song_id": db_row["song_id"],
                    "source_playlist_db_id": snap.playlist_db_id,
                    "playlist_song_id": db_row["playlist_song_id"],
                    "reason": "removed_from_playlist",
                },
                related_video_id=db_row["yt_db_id"],
                related_song_id=db_row["song_id"],
                auto_created_by="playlist_sync",
            )
            outcome.task_drop += 1

    def _reconcile_dedup_tasks(self) -> int:
        """Auto-resolve DEDUPLICATE_SONGS tasks whose state no longer holds.

        A dedup task created during phase 3 may have been invalidated by phase 4
        (e.g. the duplicating row was soft-deleted as a same-song swap).  If the
        related song now has only one active playlist_songs row, resolve the task.
        """
        resolved = 0
        with self._engine.begin() as conn:
            open_tasks = list(
                conn.execute(
                    sa.select(tasks.c.id, tasks.c.related_song_id).where(
                        sa.and_(
                            tasks.c.task_type == TaskType.DEDUPLICATE_SONGS,
                            tasks.c.status == TaskStatus.OPEN,
                            tasks.c.related_song_id.is_not(None),
                        )
                    )
                ).all()
            )
            for task_id, song_id in open_tasks:
                count = conn.execute(
                    sa.select(sa.func.count())
                    .select_from(playlist_songs)
                    .where(
                        sa.and_(
                            playlist_songs.c.song_id == song_id,
                            playlist_songs.c.removed_at.is_(None),
                        )
                    )
                ).scalar_one()
                if count <= 1:
                    conn.execute(
                        tasks.update()
                        .where(tasks.c.id == task_id)
                        .values(
                            status=TaskStatus.RESOLVED,
                            resolved_at=datetime.now(UTC),
                            data=sa.cast(tasks.c.data, JSONB).op("||")(
                                sa.cast(
                                    {
                                        "auto_resolved_by": "playlist_sync",
                                        "note": (
                                            "sync reconciled duplicate state — "
                                            "song now active in only one playlist"
                                        ),
                                    },
                                    JSONB,
                                )
                            ),
                        )
                    )
                    resolved += 1
        return resolved

    async def _run_stub_retry(self, tdb: TouhouDBClient) -> tuple[int, int]:
        promoted = 0
        no_match = 0
        with self._engine.connect() as conn:
            candidates = iter_retry_candidates(conn)
        for cand in candidates:
            try:
                with self._engine.begin() as conn:
                    result = await retry_stub_song(cand["song_id"], cand["video_ids"], conn, tdb)
                if result is not None:
                    promoted += 1
                else:
                    no_match += 1
            except Exception:
                logger.exception("Stub retry failed for song_id=%d", cand["song_id"])
                no_match += 1
        return promoted, no_match


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _compute_diff(snap: _PlaylistSnapshot) -> _Diff:
    yt_ids = set(snap.yt_items.keys())
    db_ids = set(snap.db_rows.keys())
    added = yt_ids - db_ids
    removed = db_ids - yt_ids
    kept = yt_ids & db_ids
    return _Diff(added_video_ids=added, removed_video_ids=removed, kept_video_ids=kept)


def _move_to_unsaved(conn: Connection, playlist_song_id: int, unsaved_playlist_id: int) -> None:
    """Reassign a playlist_songs row to the synthetic ``unsaved`` playlist.

    Uses an UPSERT-style fallback in case the same song is already in
    ``unsaved`` from a prior run: in that case soft-delete this row instead.
    """
    # Fetch song_id so we can check for an existing unsaved row.
    song_id = conn.execute(
        sa.select(playlist_songs.c.song_id).where(playlist_songs.c.id == playlist_song_id)
    ).scalar_one()

    existing_unsaved = conn.execute(
        sa.select(playlist_songs.c.id).where(
            sa.and_(
                playlist_songs.c.song_id == song_id,
                playlist_songs.c.playlist_id == unsaved_playlist_id,
                playlist_songs.c.removed_at.is_(None),
            )
        )
    ).first()

    if existing_unsaved is not None:
        conn.execute(
            playlist_songs.update()
            .where(playlist_songs.c.id == playlist_song_id)
            .values(removed_at=datetime.now(UTC))
        )
        return

    conn.execute(
        playlist_songs.update()
        .where(playlist_songs.c.id == playlist_song_id)
        .values(playlist_id=unsaved_playlist_id, source_type=SourceType.INDIVIDUAL_VIDEO)
    )


def _auto_resolve_dropped_video_tasks(conn: Connection, *, yt_db_id: int, note: str) -> None:
    conn.execute(
        tasks.update()
        .where(
            sa.and_(
                tasks.c.task_type == TaskType.DROPPED_VIDEO,
                tasks.c.status == TaskStatus.OPEN,
                tasks.c.related_video_id == yt_db_id,
            )
        )
        .values(
            status=TaskStatus.RESOLVED,
            resolved_at=datetime.now(UTC),
            data=sa.cast(tasks.c.data, JSONB).op("||")(
                sa.cast({"auto_resolved_by": "playlist_sync", "note": note}, JSONB)
            ),
        )
    )


# ---------------------------------------------------------------------------
# Drop-task resolution (called from the resolve wizard for DROPPED_VIDEO)
# ---------------------------------------------------------------------------


def resolve_dropped_video_to_unsaved(conn: Connection, task_id: int) -> bool:
    """Move the playlist_songs row referenced by a DROPPED_VIDEO task into ``unsaved``.

    Returns True on success.  Used by the existing ``lotad tasks resolve`` flow
    when the user accepts that the video is genuinely gone.
    """
    task_row = conn.execute(sa.select(tasks).where(tasks.c.id == task_id)).mappings().first()
    if task_row is None:
        return False
    data = task_row["data"] or {}
    ps_id = data.get("playlist_song_id")
    if ps_id is None:
        # Older DROPPED_VIDEO tasks (from M3) lack playlist_song_id; fall back
        # to song_id + source_playlist_db_id if present.
        song_id = data.get("song_id") or task_row["related_song_id"]
        source_pid = data.get("source_playlist_db_id")
        if song_id is None or source_pid is None:
            return False
        row = conn.execute(
            sa.select(playlist_songs.c.id).where(
                sa.and_(
                    playlist_songs.c.song_id == song_id,
                    playlist_songs.c.playlist_id == source_pid,
                    playlist_songs.c.removed_at.is_(None),
                )
            )
        ).first()
        if row is None:
            return False
        ps_id = row[0]

    unsaved_id = conn.execute(
        sa.select(playlists.c.id).where(playlists.c.display_order == _UNSAVED_DISPLAY_ORDER)
    ).scalar_one()
    _move_to_unsaved(conn, ps_id, unsaved_id)

    conn.execute(
        tasks.update()
        .where(tasks.c.id == task_id)
        .values(
            status=TaskStatus.RESOLVED,
            resolved_at=datetime.now(UTC),
            data=sa.cast(tasks.c.data, JSONB).op("||")(
                sa.cast({"resolution": "moved_to_unsaved"}, JSONB)
            ),
        )
    )
    return True


__all__ = [
    "PerPlaylistOutcome",
    "SyncReport",
    "resolve_dropped_video_to_unsaved",
    "sync_playlists",
]
