"""Reconcile playlist membership, video availability, and review tasks."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import anthropic
import httpx
import sqlalchemy as sa
from googleapiclient.errors import HttpError
from pydantic import ValidationError
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert
from tenacity import RetryError

from lotad.config import Settings, get_settings
from lotad.db.models import (
    TaskStatus,
    TaskType,
    playlist_songs,
    playlists,
    tasks,
    youtube_videos,
)
from lotad.db.session import get_engine
from lotad.ingestion.pipeline import IngestPipeline
from lotad.ingestion.touhoudb_client import OriginalChainError, TouhouDBClient
from lotad.ingestion.youtube_client import PlaylistItem, YouTubeClient
from lotad.sync.stub_retry import iter_retry_candidates, retry_stub_song

logger = logging.getLogger(__name__)


# Missing entries from playlist 3 / eval imply a listening decision once
# moves, replacement uploads, and known-unavailable entries are accounted for.
_LOW_TIER_DISPLAY_ORDERS = (4, 5)
_UNSAVED_DISPLAY_ORDER = 6


@dataclass
class PerPlaylistOutcome:
    added: int = 0
    unmatched: int = 0
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
    db_rows: dict[str, list[dict[str, Any]]]
    complete: bool = True


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
            except (HttpError, sa.exc.SQLAlchemyError):
                logger.exception("Snapshot failed for playlist %r", tgt)
                report.errors += 1
                continue

        if not snapshots:
            return report

        for pid, snap in snapshots.items():
            self._update_kept_videos(snap, diffs[pid], report.per_playlist[snap.playlist_name])

        # Resolve moves before ingestion can reuse or create association rows.
        self._resolve_cross_playlist_moves(snapshots, diffs, report)

        async with IngestPipeline(self._settings) as pipeline:
            for pid, snap in snapshots.items():
                outcome = report.per_playlist[snap.playlist_name]
                for video_id in list(diffs[pid].added_video_ids):
                    item = snap.yt_items[video_id]
                    try:
                        if await pipeline.ingest_video(item, playlist_db_id=pid):
                            outcome.added += 1
                        else:
                            outcome.unmatched += 1
                    except (
                        anthropic.APIError,
                        httpx.HTTPError,
                        sa.exc.SQLAlchemyError,
                        ValidationError,
                        RetryError,
                        OriginalChainError,
                    ):
                        logger.exception(
                            "Ingest failed for %s in playlist %s",
                            video_id,
                            snap.playlist_name,
                        )
                        outcome.errors += 1

        # Ingest replacements first so removals can recognize the surviving song.
        for pid, snap in snapshots.items():
            outcome = report.per_playlist[snap.playlist_name]
            for video_id in list(diffs[pid].removed_video_ids):
                self._handle_removal(snap, video_id, outcome)

        report.dedup_tasks_reconciled = self._reconcile_dedup_tasks()

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
                        youtube_videos.c.title,
                        youtube_videos.c.channel_id,
                        youtube_videos.c.channel_name,
                        youtube_videos.c.description,
                        youtube_videos.c.duration_seconds,
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
        db_rows: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            db_rows.setdefault(row["video_id"], []).append(dict(row))
        return _PlaylistSnapshot(
            playlist_db_id=target["id"],
            playlist_name=target["name"],
            youtube_playlist_id=target["youtube_playlist_id"],
            yt_items=items,
            db_rows=db_rows,
            complete=self._limit is None,
        )

    def _update_kept_videos(
        self,
        snap: _PlaylistSnapshot,
        diff: _Diff,
        outcome: PerPlaylistOutcome,
    ) -> None:
        """Refresh checked videos and retain usable context for unavailable uploads."""
        now = datetime.now(UTC)
        with self._engine.begin() as conn:
            for video_id in diff.kept_video_ids:
                yt_item = snap.yt_items[video_id]
                rows = snap.db_rows[video_id]
                db_row = rows[0]
                values: dict[str, Any] = {"is_available": yt_item.is_available}
                if yt_item.is_available:
                    values.update(
                        title=yt_item.title,
                        channel_id=yt_item.channel_id or None,
                        channel_name=yt_item.channel_name or None,
                        description=yt_item.description or None,
                        duration_seconds=yt_item.duration_seconds,
                    )
                # Polling does not imply a metadata change; unavailable stubs
                # must retain the last known title and description.
                values["updated_at"] = sa.case(
                    (
                        sa.or_(
                            *(
                                youtube_videos.c[key].is_distinct_from(value)
                                for key, value in values.items()
                            )
                        ),
                        now,
                    ),
                    else_=youtube_videos.c.updated_at,
                )
                values["last_checked_at"] = now
                conn.execute(
                    youtube_videos.update()
                    .where(youtube_videos.c.id == db_row["yt_db_id"])
                    .values(**values)
                )
                if yt_item.is_available:
                    _auto_resolve_dropped_video_tasks(
                        conn,
                        yt_db_id=db_row["yt_db_id"],
                        source_playlist_id=snap.playlist_db_id,
                        note="auto-resolved on sync — upload is available in its playlist again",
                    )
                else:
                    _create_dropped_video_task(
                        conn,
                        snap,
                        video_id,
                        rows,
                        reason="deleted",
                        new_transition=db_row["is_available"],
                        title=f"Deleted or private video still in playlist: {video_id!r}",
                        extra={
                            "title": db_row["title"],
                            "position": yt_item.position,
                            "playlist_item_id": yt_item.playlist_item_id,
                        },
                    )
                    if db_row["is_available"]:
                        outcome.deleted_in_place += 1

    def _resolve_cross_playlist_moves(
        self,
        snapshots: dict[int, _PlaylistSnapshot],
        diffs: dict[int, _Diff],
        report: SyncReport,
    ) -> None:
        """Apply cross-playlist moves atomically before additions and removals."""
        added_index: dict[str, list[int]] = {}
        for pid, diff in diffs.items():
            for vid in diff.added_video_ids:
                added_index.setdefault(vid, []).append(pid)

        planned: list[tuple[int, int, str]] = []
        for pid, diff in diffs.items():
            for vid in sorted(diff.removed_video_ids):
                targets = added_index.get(vid, [])
                if targets:
                    planned.append((pid, targets.pop(0), vid))

        applied: list[tuple[int, int, str]] = []
        with self._engine.begin() as conn:
            for pid, target_pid, vid in planned:
                expected = snapshots[pid].db_rows[vid]
                current = (
                    conn.execute(
                        sa.select(playlist_songs)
                        .where(
                            playlist_songs.c.id.in_([row["playlist_song_id"] for row in expected])
                        )
                        .with_for_update()
                    )
                    .mappings()
                    .all()
                )
                by_id = {row["id"]: row for row in current}
                if all(
                    row["playlist_song_id"] in by_id
                    and by_id[row["playlist_song_id"]]["removed_at"] is None
                    and by_id[row["playlist_song_id"]]["playlist_id"] == pid
                    and by_id[row["playlist_song_id"]]["song_id"] == row["song_id"]
                    and by_id[row["playlist_song_id"]]["youtube_video_id"] == row["yt_db_id"]
                    for row in expected
                ):
                    applied.append((pid, target_pid, vid))

            # Vacate all outgoing slots first so reciprocal same-song moves
            # cannot overwrite one another. The transaction restores every
            # active row at its destination, or rolls the entire batch back.
            moving_ids = [
                row["playlist_song_id"]
                for pid, _, vid in applied
                for row in snapshots[pid].db_rows[vid]
            ]
            if moving_ids:
                conn.execute(
                    playlist_songs.update()
                    .where(playlist_songs.c.id.in_(moving_ids))
                    .values(removed_at=datetime.now(UTC))
                )
            for pid, target_pid, vid in applied:
                self._move_playlist_song(conn, snapshots[pid], snapshots[target_pid], vid)

        for pid, target_pid, vid in applied:
            target_snap = snapshots[target_pid]
            self._update_kept_videos(
                target_snap,
                _Diff(set(), set(), {vid}),
                report.per_playlist[target_snap.playlist_name],
            )
            report.per_playlist[snapshots[pid].playlist_name].moved_out += 1
            report.per_playlist[target_snap.playlist_name].moved_in += 1
            diffs[pid].removed_video_ids.discard(vid)
            diffs[target_pid].added_video_ids.discard(vid)

    def _move_playlist_song(
        self,
        conn: Connection,
        source_snap: _PlaylistSnapshot,
        target_snap: _PlaylistSnapshot,
        video_id: str,
    ) -> None:
        for row in source_snap.db_rows[video_id]:
            _move_playlist_row(
                conn, row["playlist_song_id"], target_snap.playlist_db_id, replace_existing=True
            )
        _auto_resolve_dropped_video_tasks(
            conn,
            yt_db_id=source_snap.db_rows[video_id][0]["yt_db_id"],
            source_playlist_id=source_snap.playlist_db_id,
            note="auto-resolved on sync — video moved to another playlist",
        )
        moved_rows = (
            conn.execute(
                sa.select(
                    playlist_songs.c.id,
                    playlist_songs.c.song_id,
                ).where(
                    playlist_songs.c.playlist_id == target_snap.playlist_db_id,
                    playlist_songs.c.youtube_video_id
                    == source_snap.db_rows[video_id][0]["yt_db_id"],
                    playlist_songs.c.removed_at.is_(None),
                )
            )
            .mappings()
            .all()
        )
        originals = {row["song_id"]: row for row in source_snap.db_rows[video_id]}
        target_snap.db_rows[video_id] = [
            {**originals[row["song_id"]], "playlist_song_id": row["id"]} for row in moved_rows
        ]

    def _handle_removal(
        self,
        snap: _PlaylistSnapshot,
        video_id: str,
        outcome: PerPlaylistOutcome,
    ) -> None:
        """Apply the removal decision to every song represented by an upload."""
        now = datetime.now(UTC)
        pending_rows: list[dict[str, Any]] = []
        with self._engine.begin() as conn:
            for db_row in snap.db_rows[video_id]:
                current = (
                    conn.execute(
                        sa.select(playlist_songs).where(
                            playlist_songs.c.id == db_row["playlist_song_id"]
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if current is None or current["removed_at"] is not None:
                    continue
                if (
                    current["youtube_video_id"] != db_row["yt_db_id"]
                    or current["playlist_id"] != snap.playlist_db_id
                    or current["song_id"] != db_row["song_id"]
                ):
                    # Ingestion can reuse this row for a replacement upload;
                    # the snapshot must still match before removing it.
                    outcome.same_song_swap += 1
                    continue

                if not db_row["is_available"]:
                    conn.execute(
                        playlist_songs.update()
                        .where(playlist_songs.c.id == db_row["playlist_song_id"])
                        .values(removed_at=now)
                    )
                    outcome.dead_replacement += 1
                    continue

                other_active = conn.execute(
                    sa.select(playlist_songs.c.id)
                    .where(
                        playlist_songs.c.song_id == db_row["song_id"],
                        playlist_songs.c.id != db_row["playlist_song_id"],
                        playlist_songs.c.removed_at.is_(None),
                    )
                    .limit(1)
                ).first()
                if other_active is not None:
                    conn.execute(
                        playlist_songs.update()
                        .where(playlist_songs.c.id == db_row["playlist_song_id"])
                        .values(removed_at=now)
                    )
                    outcome.same_song_swap += 1
                    continue
                pending_rows.append(db_row)

            if not pending_rows:
                _auto_resolve_dropped_video_tasks(
                    conn,
                    yt_db_id=snap.db_rows[video_id][0]["yt_db_id"],
                    source_playlist_id=snap.playlist_db_id,
                    note="auto-resolved on sync — upload removed or replaced",
                )
                return

            display_order = conn.execute(
                sa.select(playlists.c.display_order).where(playlists.c.id == snap.playlist_db_id)
            ).scalar_one()
            if display_order in _LOW_TIER_DISPLAY_ORDERS:
                unsaved_id = conn.execute(
                    sa.select(playlists.c.id).where(
                        playlists.c.display_order == _UNSAVED_DISPLAY_ORDER
                    )
                ).scalar_one()
                for row in pending_rows:
                    _move_playlist_row(conn, row["playlist_song_id"], unsaved_id)
                outcome.silent_drop += 1
                return

            _create_dropped_video_task(
                conn,
                snap,
                video_id,
                pending_rows,
                reason="removed_from_playlist",
                title=f"Song removed from {snap.playlist_name}: video {video_id!r}",
            )
            outcome.task_drop += 1

    def _reconcile_dedup_tasks(self) -> int:
        """Resolve duplicate-song tasks when at most one active association remains."""
        resolved = 0
        with self._engine.begin() as conn:
            open_tasks = list(
                conn.execute(
                    sa.select(tasks.c.id, tasks.c.related_song_id, tasks.c.data).where(
                        sa.and_(
                            tasks.c.task_type == TaskType.DEDUPLICATE_SONGS,
                            tasks.c.status == TaskStatus.OPEN,
                            tasks.c.related_song_id.is_not(None),
                        )
                    )
                ).all()
            )
            for task_id, song_id, data in open_tasks:
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
                            data={
                                **(data or {}),
                                "auto_resolved_by": "playlist_sync",
                                "note": (
                                    "sync reconciled duplicate state — "
                                    "song now active in only one playlist"
                                ),
                            },
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
            except (
                httpx.HTTPError,
                sa.exc.SQLAlchemyError,
                ValidationError,
                RetryError,
                OriginalChainError,
            ):
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
    # A bounded fetch cannot prove that an unseen video has been removed.
    removed = db_ids - yt_ids if snap.complete else set()
    kept = yt_ids & db_ids
    return _Diff(added_video_ids=added, removed_video_ids=removed, kept_video_ids=kept)


def _move_playlist_row(
    conn: Connection,
    playlist_song_id: int,
    target_playlist_id: int,
    *,
    replace_existing: bool = False,
) -> None:
    """Move an active row while preserving composite provenance and timestamps."""
    row = (
        conn.execute(sa.select(playlist_songs).where(playlist_songs.c.id == playlist_song_id))
        .mappings()
        .one()
    )
    if row["playlist_id"] == target_playlist_id:
        return
    existing = conn.execute(
        sa.select(playlist_songs.c.id)
        .where(
            playlist_songs.c.song_id == row["song_id"],
            playlist_songs.c.playlist_id == target_playlist_id,
            playlist_songs.c.removed_at.is_(None),
        )
        .limit(1)
    ).first()
    if existing is not None and replace_existing:
        # The destination can already contain another upload of this song.
        # Preserve one active row and make its identity match the moved upload.
        conn.execute(
            playlist_songs.update()
            .where(playlist_songs.c.id == existing[0])
            .values(
                youtube_video_id=row["youtube_video_id"],
                youtube_timestamp_seconds=row["youtube_timestamp_seconds"],
                source_type=row["source_type"],
            )
        )
    values = (
        {"removed_at": datetime.now(UTC)}
        if existing is not None
        else {"playlist_id": target_playlist_id, "removed_at": None}
    )
    conn.execute(
        playlist_songs.update().where(playlist_songs.c.id == playlist_song_id).values(**values)
    )


def _create_dropped_video_task(
    conn: Connection,
    snap: _PlaylistSnapshot,
    video_id: str,
    rows: list[dict[str, Any]],
    *,
    reason: str,
    title: str,
    extra: dict[str, Any] | None = None,
    new_transition: bool = True,
) -> None:
    # One task covers all tracks of an upload within one playlist. The generic
    # song-level task key would conflate drops from different playlists.
    existing = (
        conn.execute(
            sa.select(tasks)
            .order_by(tasks.c.id.desc())
            .where(
                tasks.c.task_type == TaskType.DROPPED_VIDEO,
                tasks.c.related_video_id == rows[0]["yt_db_id"],
            )
        )
        .mappings()
        .all()
    )
    context: dict[str, Any] = {
        "video_id": video_id,
        "source_playlist_db_id": snap.playlist_db_id,
        "playlist_db_id": snap.playlist_db_id,
        "playlist_name": snap.playlist_name,
        "playlist_song_ids": [row["playlist_song_id"] for row in rows],
        "song_ids": [row["song_id"] for row in rows],
        "reason": reason,
        **(extra or {}),
    }
    if len(rows) == 1:
        context.update(playlist_song_id=rows[0]["playlist_song_id"], song_id=rows[0]["song_id"])
    for task in existing:
        data = task["data"] or {}
        if str(data.get("source_playlist_db_id") or data.get("playlist_db_id")) == str(
            snap.playlist_db_id
        ):
            if task["status"] not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
                if not new_transition:
                    return
                continue
            conn.execute(
                tasks.update()
                .where(tasks.c.id == task["id"])
                .values(
                    title=title,
                    data={**data, **context},
                )
            )
            return
    inserted = conn.execute(
        pg_insert(tasks)
        .values(
            task_type=TaskType.DROPPED_VIDEO,
            title=title,
            data=context,
            related_video_id=rows[0]["yt_db_id"],
            related_song_id=rows[0]["song_id"] if len(rows) == 1 else None,
            auto_created_by="playlist_sync",
        )
        .on_conflict_do_nothing()
        .returning(tasks.c.id)
    ).scalar_one_or_none()
    if inserted is None:
        # A simultaneous sync may have inserted the same playlist task while
        # we inspected existing rows. Read its committed row and merge once.
        source_key = sa.cast(
            sa.func.coalesce(
                tasks.c.data["source_playlist_db_id"].as_string(),
                tasks.c.data["playlist_db_id"].as_string(),
                "",
            ),
            sa.Text,
        )
        concurrent = (
            conn.execute(
                sa.select(tasks.c.id, tasks.c.data)
                .where(
                    tasks.c.task_type == TaskType.DROPPED_VIDEO,
                    tasks.c.status == TaskStatus.OPEN,
                    tasks.c.related_video_id == rows[0]["yt_db_id"],
                    source_key == str(snap.playlist_db_id),
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if concurrent is None:
            raise sa.exc.InvalidRequestError(
                "Dropped-video task insert conflicted without a matching source playlist task"
            )
        conn.execute(
            tasks.update()
            .where(tasks.c.id == concurrent["id"])
            .values(
                title=title,
                data={**(concurrent["data"] or {}), **context},
            )
        )


def _auto_resolve_dropped_video_tasks(
    conn: Connection, *, yt_db_id: int, source_playlist_id: int, note: str
) -> None:
    rows = (
        conn.execute(
            sa.select(tasks).where(
                tasks.c.task_type == TaskType.DROPPED_VIDEO,
                tasks.c.status.in_((TaskStatus.OPEN, TaskStatus.IN_PROGRESS)),
                tasks.c.related_video_id == yt_db_id,
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        data = row["data"] or {}
        if str(data.get("source_playlist_db_id") or data.get("playlist_db_id")) != str(
            source_playlist_id
        ):
            continue
        conn.execute(
            tasks.update()
            .where(tasks.c.id == row["id"])
            .values(
                status=TaskStatus.RESOLVED,
                resolved_at=datetime.now(UTC),
                data={**data, "auto_resolved_by": "playlist_sync", "note": note},
            )
        )


def _dropped_task_rows(conn: Connection, task_row: dict[str, Any]) -> list[dict[str, Any]]:
    """Find only active rows whose identity still matches the dropped upload."""
    data = task_row["data"] or {}
    source_pid = data.get("source_playlist_db_id") or data.get("playlist_db_id")
    row_ids = data.get("playlist_song_ids") or (
        [data["playlist_song_id"]] if data.get("playlist_song_id") is not None else []
    )
    video_id = task_row["related_video_id"]
    song_ids = data.get("song_ids") or (
        [data.get("song_id") or task_row["related_song_id"]]
        if data.get("song_id") or task_row["related_song_id"]
        else []
    )
    if not row_ids and (source_pid is None or (video_id is None and not song_ids)):
        return []
    stmt = sa.select(playlist_songs).where(playlist_songs.c.removed_at.is_(None))
    if row_ids:
        stmt = stmt.where(playlist_songs.c.id.in_(row_ids))
    if source_pid is not None:
        stmt = stmt.where(playlist_songs.c.playlist_id == source_pid)
    if video_id is not None:
        stmt = stmt.where(playlist_songs.c.youtube_video_id == video_id)
    elif data.get("video_id"):
        stmt = stmt.where(
            playlist_songs.c.youtube_video_id.in_(
                sa.select(youtube_videos.c.id).where(youtube_videos.c.video_id == data["video_id"])
            )
        )
    if song_ids:
        stmt = stmt.where(playlist_songs.c.song_id.in_(song_ids))
    return [dict(row) for row in conn.execute(stmt.with_for_update()).mappings().all()]


def _resolve_dropped_video(conn: Connection, task_id: int, *, to_unsaved: bool) -> bool:
    task_row = conn.execute(sa.select(tasks).where(tasks.c.id == task_id)).mappings().first()
    if (
        task_row is None
        or task_row["task_type"] != TaskType.DROPPED_VIDEO
        or task_row["status"] not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS)
    ):
        return False
    rows = _dropped_task_rows(conn, dict(task_row))
    if not rows:
        return False
    if to_unsaved:
        unsaved_id = conn.execute(
            sa.select(playlists.c.id).where(playlists.c.display_order == _UNSAVED_DISPLAY_ORDER)
        ).scalar_one()
        for row in rows:
            _move_playlist_row(conn, row["id"], unsaved_id)
    else:
        conn.execute(
            playlist_songs.update()
            .where(playlist_songs.c.id.in_([row["id"] for row in rows]))
            .values(removed_at=datetime.now(UTC))
        )
    conn.execute(
        tasks.update()
        .where(tasks.c.id == task_id)
        .values(
            status=TaskStatus.RESOLVED if to_unsaved else TaskStatus.DISMISSED,
            resolved_at=datetime.now(UTC),
            data={
                **(task_row["data"] or {}),
                "resolution": "moved_to_unsaved" if to_unsaved else "soft_deleted",
            },
        )
    )
    return True


def resolve_dropped_video_to_unsaved(conn: Connection, task_id: int) -> bool:
    """Move the still-matching dropped rows to unsaved and resolve their task."""
    return _resolve_dropped_video(conn, task_id, to_unsaved=True)


def dismiss_dropped_video(conn: Connection, task_id: int) -> bool:
    """Soft-delete the still-matching dropped rows and dismiss their task."""
    return _resolve_dropped_video(conn, task_id, to_unsaved=False)


__all__ = [
    "PerPlaylistOutcome",
    "SyncReport",
    "dismiss_dropped_video",
    "resolve_dropped_video_to_unsaved",
    "sync_playlists",
]
