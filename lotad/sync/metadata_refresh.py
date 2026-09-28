"""Refresh selected songs from TouhouDB and retry unmatched arrangement stubs."""

from __future__ import annotations

import csv
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import Connection
from sqlalchemy.exc import SQLAlchemyError

from lotad.config import Settings, get_settings
from lotad.db.models import SongRole, SongType, playlist_songs, song_artists, songs, youtube_videos
from lotad.db.session import get_engine
from lotad.ingestion.http_client import CircuitBreakerOpen
from lotad.ingestion.touhoudb_client import OriginalChainError, TouhouDBClient
from lotad.sync.stub_retry import retry_stub_song
from lotad.sync.touhoudb_ingest import apply_touhoudb_detail, make_task_creator

logger = logging.getLogger(__name__)


FILTER_NAMES = ("missing-lyricist", "zero-duration", "stub-retry")


@dataclass
class RefreshReport:
    refreshed: int = 0
    stub_promoted: int = 0
    stub_unchanged: int = 0
    skipped_no_upstream: int = 0
    errors: int = 0
    refreshed_song_ids: list[int] = field(default_factory=list)
    promoted_song_ids: list[int] = field(default_factory=list)


def select_songs_for_filter(name: str, conn: Connection) -> list[int]:
    """Return song IDs matching a named filter preset."""
    if name == "missing-lyricist":
        # has_lyrics=true AND no LYRICIST row in song_artists.
        lyricist_subq = sa.select(song_artists.c.song_id).where(
            song_artists.c.role == SongRole.LYRICIST
        )
        stmt = sa.select(songs.c.id).where(
            sa.and_(
                songs.c.has_lyrics.is_(True),
                songs.c.touhoudb_id.is_not(None),
                ~songs.c.id.in_(lyricist_subq),
            )
        )
    elif name == "zero-duration":
        stmt = sa.select(songs.c.id).where(
            sa.and_(
                songs.c.touhoudb_id.is_not(None),
                sa.or_(songs.c.duration_seconds.is_(None), songs.c.duration_seconds == 0),
            )
        )
    elif name == "stub-retry":
        stmt = sa.select(songs.c.id).where(
            sa.and_(
                songs.c.touhoudb_id.is_(None),
                songs.c.song_type != SongType.ORIGINAL,
            )
        )
    else:
        raise ValueError(f"Unknown filter: {name!r}. Valid: {FILTER_NAMES}")

    return [row[0] for row in conn.execute(stmt).all()]


def select_all_refreshable_songs(conn: Connection) -> list[int]:
    """Return all song IDs with a non-null TouhouDB linkage (eligible for refresh)."""
    return [
        row[0]
        for row in conn.execute(sa.select(songs.c.id).where(songs.c.touhoudb_id.is_not(None))).all()
    ]


def read_song_ids_from_csv(path: str | Path) -> list[int]:
    """Parse a CSV with a ``song_id`` column.  Tolerates extra columns/whitespace."""
    out: list[int] = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "song_id" not in reader.fieldnames:
            raise ValueError(f"CSV at {path!r} must have a 'song_id' column")
        for row in reader:
            raw = (row.get("song_id") or "").strip()
            if not raw:
                continue
            try:
                out.append(int(raw))
            except ValueError:
                logger.warning("Skipping non-integer song_id %r in %s", raw, path)
    return out


async def refresh_songs(
    song_ids: list[int],
    *,
    settings: Settings | None = None,
    dry_run: bool = False,
    progress_callback: Callable[[int, int, int], None] | None = None,
) -> RefreshReport:
    """Re-fetch TouhouDB data for each song in ``song_ids`` and re-apply.

    For songs with ``touhoudb_id IS NOT NULL``: fetches the detail and runs
    ``apply_touhoudb_detail`` (idempotent — overwrites TouhouDB-sourced fields).

    For stubs (``touhoudb_id IS NULL`` and ``song_type != ORIGINAL``):
    delegates to ``retry_stub_song``.

    Other rows are skipped with a logged reason.
    """
    settings = settings or get_settings()
    engine = get_engine()
    report = RefreshReport()

    async with TouhouDBClient.from_settings(settings) as tdb:
        for idx, song_id in enumerate(song_ids):
            if progress_callback is not None:
                progress_callback(idx, len(song_ids), song_id)

            refreshed = False
            promoted_id: int | None = None
            try:
                with engine.begin() as conn:
                    row = conn.execute(
                        sa.select(
                            songs.c.id,
                            songs.c.touhoudb_id,
                            songs.c.song_type,
                        ).where(songs.c.id == song_id)
                    ).one_or_none()
                    if row is None:
                        logger.warning("song_id=%d not found — skipping", song_id)
                        report.errors += 1
                        continue

                    if row.touhoudb_id is not None:
                        if dry_run:
                            report.refreshed += 1
                            report.refreshed_song_ids.append(song_id)
                            continue
                        detail = await tdb.get_song(row.touhoudb_id)
                        await apply_touhoudb_detail(
                            detail,
                            conn,
                            tdb,
                            create_task=make_task_creator("metadata_refresh"),
                        )
                        refreshed = True
                    elif row.song_type != SongType.ORIGINAL:
                        if dry_run:
                            report.stub_unchanged += 1
                            continue
                        vids = [
                            r[0]
                            for r in conn.execute(
                                sa.select(youtube_videos.c.video_id)
                                .select_from(
                                    playlist_songs.join(
                                        youtube_videos,
                                        playlist_songs.c.youtube_video_id == youtube_videos.c.id,
                                    )
                                )
                                .where(
                                    sa.and_(
                                        playlist_songs.c.song_id == song_id,
                                        playlist_songs.c.removed_at.is_(None),
                                        youtube_videos.c.is_available.is_(True),
                                    )
                                )
                            ).all()
                        ]
                        if not vids:
                            report.stub_unchanged += 1
                            continue
                        result = await retry_stub_song(song_id, vids, conn, tdb)
                        if result is not None:
                            promoted_id = result
                        else:
                            report.stub_unchanged += 1
                    else:
                        # Stub but ORIGINAL song_type — no upstream to refresh
                        logger.debug("song_id=%d is stub-original; nothing to refresh", song_id)
                        report.skipped_no_upstream += 1
                if refreshed:
                    report.refreshed += 1
                    report.refreshed_song_ids.append(song_id)
                if promoted_id is not None:
                    report.stub_promoted += 1
                    report.promoted_song_ids.append(promoted_id)
            except (
                httpx.HTTPError,
                ValidationError,
                SQLAlchemyError,
                CircuitBreakerOpen,
                OriginalChainError,
            ):
                logger.exception("Refresh failed for song_id=%d", song_id)
                report.errors += 1

    return report
