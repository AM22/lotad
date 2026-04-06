"""Core ingestion pipeline: YouTube playlist → TouhouDB → LOTAD database.

The pipeline processes one playlist item at a time:
1. Upsert the ``youtube_videos`` row.
2. Look up the song in TouhouDB — bulk-first, per-video fallback:
   a. Before the per-video loop, call ``bulk_match_playlist`` once for the
      whole playlist.  This uses the ``/api/songLists/import`` endpoints and
      reduces TouhouDB API calls from O(N) to O(ceil(N/50)) for the lookup
      phase.
   b. For each video: if the bulk map has a match, call ``get_song(id)`` for
      full detail (artists/tags/albums).  Otherwise fall back to the
      per-video ``/songs/byPv`` lookup.
3. On match: map the song (and album if album video) to the DB, then
   create a ``playlist_songs`` row.
4. On no match: create an ``INGEST_FAILED`` task for manual follow-up
   (LLM fallback will be wired in during M4).

Special cases:
- Deleted/private videos (``item.is_available = False``): a ``DROPPED_VIDEO``
  task is created immediately without attempting a TouhouDB lookup.

All DB writes for a single video are wrapped in a single transaction.
Failures on individual videos are caught, logged, and do not abort the run.
"""

from __future__ import annotations

import json
import logging
import re as _re
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
from lotad.ingestion.http_client import CircuitBreakerOpen
from lotad.ingestion.mappers import link_song_originals, map_song_to_db
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.touhoudb_models import SongDetail
from lotad.ingestion.youtube_client import PlaylistItem, YouTubeClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Album-video heuristics
# ---------------------------------------------------------------------------

# Patterns that reliably signal a multi-track composite video.
# Deliberately conservative — false positives (tagging a single track as
# composite) are worse than false negatives because they suppress the
# INDIVIDUAL_VIDEO source type.
#
# Excluded on purpose:
#   \bm3\b    — uploaders commonly include "M3" in single-song titles to
#               indicate the event where it was released (not that it's a
#               full album video)
#   \bc\d+\b  — same rationale for Comiket event numbers
_ALBUM_TITLE_PATTERNS = [
    r"\bfull\s+album\b",
    r"\bfull\s+arrange\b",
    # Crossfade / XFD demo videos — always composite
    r"\bxfd\b",
    r"\bx-?fade\b",
    r"\bcrossfade\b",
    r"クロスフェード",  # katakana "crossfade"
    # Composite title pattern: "Song A + Song B"
    # Space-plus-space is rare in single-song titles
    r" \+ ",
]

_ALBUM_RE = _re.compile(
    "|".join(_ALBUM_TITLE_PATTERNS),
    _re.IGNORECASE,
)

# 19:30 = 1170 seconds.  Chosen empirically:
#   - Longest known single arrangement: ~19:00 (OgS-ScUoN5M)
#   - Shortest known full-album video: ~20:00
# This threshold sits cleanly between the two populations.
_ALBUM_DURATION_THRESHOLD_SECONDS = 1170


def is_album_video(item: PlaylistItem) -> bool:
    """
    Return True if the playlist item looks like a multi-track composite video.

    Heuristics (any one is sufficient):
    - Duration ≥ 19:30 (1170 s)
    - Title contains a known composite-video indicator pattern

    Note: YouTube Premium / members-only videos may report duration_seconds=None
    even for long videos.  Those are treated as non-album (conservative default).
    """
    if (
        item.duration_seconds is not None
        and item.duration_seconds >= _ALBUM_DURATION_THRESHOLD_SECONDS
    ):
        return True
    return bool(_ALBUM_RE.search(item.title))


def extract_timestamps(description: str) -> list[tuple[int, str]]:
    """
    Parse timestamp lines from a YouTube video description.

    Recognises ``MM:SS`` and ``HH:MM:SS`` followed by a track title.
    Returns ``[(seconds, title), ...]`` sorted by timestamp ascending.

    TODO (M3 composite-video path): hook this up in ``ingest_video`` when
    ``is_album_video`` returns True.  The returned list should be passed to
    ``map_album_to_db`` so that ``album_tracks.youtube_timestamp_seconds`` is
    populated for each track, enabling per-track playback links.
    """
    pattern = _re.compile(
        r"(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\s+[.\-–—|]?\s*(.+)",
        _re.MULTILINE,
    )
    results: list[tuple[int, str]] = []
    for m in pattern.finditer(description):
        hours = int(m.group(1) or 0)
        minutes = int(m.group(2))
        seconds_part = int(m.group(3))
        title = m.group(4).strip()
        total = hours * 3600 + minutes * 60 + seconds_part
        results.append((total, title))
    return sorted(results)


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint(path: str) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_checkpoint(path: str, data: dict) -> None:
    Path(path).write_text(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------


class IngestPipeline:
    """
    Orchestrates ingestion of a YouTube playlist into LOTAD.

    Usage::

        async with IngestPipeline(settings) as pipeline:
            stats = await pipeline.ingest_playlist(playlist_id, limit=100)
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._engine = get_engine()

    async def __aenter__(self) -> IngestPipeline:
        self._tdb = TouhouDBClient.from_settings(self._settings)
        await self._tdb.__aenter__()
        self._yt = YouTubeClient(self._settings)
        return self

    async def __aexit__(self, *args) -> None:
        await self._tdb.__aexit__(*args)

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    async def ingest_playlist(
        self,
        playlist_id: str,
        *,
        resume: bool = False,
        limit: int | None = None,
        progress_callback=None,
    ) -> dict:
        """
        Ingest all videos in a YouTube playlist.

        Returns a stats dict: {matched, unmatched, errors, skipped}.
        """
        checkpoint = _load_checkpoint(self._settings.ingestion_checkpoint_path)
        resume_position = checkpoint.get(playlist_id, -1) if resume else -1

        # Resolve playlist DB id
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(playlists.c.id).where(playlists.c.youtube_playlist_id == playlist_id)
            ).one_or_none()
        playlist_db_id: int | None = row.id if row else None

        stats = {"matched": 0, "unmatched": 0, "errors": 0, "skipped": 0}
        processed = 0

        items = list(self._yt.list_playlist_items(playlist_id, limit=limit))
        total = len(items)

        # Bulk-match the whole playlist against TouhouDB in O(ceil(N/50)) calls.
        # Falls back to empty dict (per-video lookup) if the endpoint is
        # unavailable or raises an unexpected error.
        try:
            bulk_map = await self._tdb.bulk_match_playlist(playlist_id)
            logger.info(
                "Bulk pre-match: %d/%d videos have a TouhouDB entry",
                len(bulk_map),
                total,
            )
        except Exception:
            logger.warning(
                "bulk_match_playlist failed; falling back to per-video lookup for all %d items",
                total,
                exc_info=True,
            )
            bulk_map = {}

        for item in items:
            if resume and item.position <= resume_position:
                stats["skipped"] += 1
                continue

            if progress_callback:
                progress_callback(processed, total, item.title)

            try:
                matched = await self.ingest_video(
                    item,
                    playlist_db_id=playlist_db_id,
                    bulk_match=bulk_map,
                )
                if matched:
                    stats["matched"] += 1
                else:
                    stats["unmatched"] += 1
            except Exception:
                logger.exception("Error ingesting video %s (%r)", item.video_id, item.title)
                stats["errors"] += 1
                # Create INGEST_FAILED task
                self._create_ingest_failed_task(item)

            processed += 1

            # Save checkpoint every 10 videos
            if processed % 10 == 0:
                checkpoint[playlist_id] = item.position
                _save_checkpoint(self._settings.ingestion_checkpoint_path, checkpoint)

        # Final checkpoint save
        checkpoint[playlist_id] = items[-1].position if items else resume_position
        _save_checkpoint(self._settings.ingestion_checkpoint_path, checkpoint)

        return stats

    async def ingest_video(
        self,
        item: PlaylistItem,
        *,
        playlist_db_id: int | None = None,
        bulk_match: dict[str, int] | None = None,
    ) -> bool:
        """
        Ingest a single YouTube video.

        Args:
            item: playlist item from the YouTube client.
            playlist_db_id: internal DB id of the playlist this video belongs to.
            bulk_match: optional pre-computed dict of ``{video_id: touhoudb_song_id}``
                from ``bulk_match_playlist``.  When provided and the video has a
                match, ``get_song(id)`` is called instead of ``/songs/byPv``,
                saving one API round-trip per matched video.

        Returns True if a TouhouDB match was found, False otherwise.
        """
        with self._engine.begin() as conn:
            # 0. Deleted / private videos: record the stub and create a task.
            if not item.is_available:
                yt_video_id = self._upsert_youtube_video(item, conn)
                self._create_task(
                    TaskType.DROPPED_VIDEO,
                    f"Deleted or private video still in playlist: {item.video_id!r}",
                    {
                        "video_id": item.video_id,
                        "title": item.title,
                        "note": "YouTube returned a deleted/private stub; video is no longer accessible",
                    },
                    conn,
                    related_video_id=yt_video_id,
                )
                logger.info(
                    "Dropped video %s (%r) — created DROPPED_VIDEO task", item.video_id, item.title
                )
                return False

            # 1. Upsert youtube_videos row
            yt_video_id = self._upsert_youtube_video(item, conn)

            # 2. Look up in TouhouDB.
            #    If the bulk pre-match found this video, fetch full detail via
            #    get_song().  Otherwise fall back to the per-video /songs/byPv
            #    endpoint (covers songs added to TouhouDB after the bulk call,
            #    and the standalone ingest_video path used from the CLI).
            try:
                if bulk_match is not None and item.video_id in bulk_match:
                    song_detail = await self._tdb.get_song(bulk_match[item.video_id])
                else:
                    song_detail = await self._tdb.lookup_by_youtube_url(item.video_id)
            except CircuitBreakerOpen:
                logger.warning("TouhouDB circuit breaker open; skipping video %s", item.video_id)
                self._create_task(
                    TaskType.TOUHOUDB_UNREACHABLE,
                    f"Circuit breaker open for {item.video_id}",
                    {"video_id": item.video_id},
                    conn,
                    related_video_id=yt_video_id,
                )
                return False

            if song_detail is None:
                # No TouhouDB match — create a task for manual review
                self._create_task(
                    TaskType.INGEST_FAILED,
                    f"No TouhouDB match: {item.title!r}",
                    {"video_id": item.video_id, "title": item.title},
                    conn,
                    related_video_id=yt_video_id,
                )
                return False

            # 3. Map song to DB
            song_id = map_song_to_db(song_detail, conn)

            # 4. Resolve original chain + link
            if song_detail.originalVersionId is not None:
                try:
                    original_ids = await self._tdb.resolve_original_chain(
                        song_detail.originalVersionId
                    )
                    linked = link_song_originals(song_id, original_ids, conn)
                    if not linked:
                        # Original song not seeded yet
                        self._create_task(
                            TaskType.FILL_MISSING_INFO,
                            f"Original song chain not in DB for song {song_id}",
                            {
                                "song_id": song_id,
                                "original_touhoudb_ids": original_ids,
                            },
                            conn,
                            related_song_id=song_id,
                        )
                except Exception:
                    logger.exception("resolve_original_chain failed for song %d", song_id)

            # 5. Integrity checks
            self._integrity_checks(song_detail, song_id, yt_video_id, item, conn)

            # 6. Create playlist_songs row (if playlist known)
            if playlist_db_id is not None:
                source = (
                    SourceType.COMPOSITE_VIDEO
                    if is_album_video(item)
                    else SourceType.INDIVIDUAL_VIDEO
                )
                self._upsert_playlist_song(
                    song_id=song_id,
                    playlist_db_id=playlist_db_id,
                    yt_video_id=yt_video_id,
                    source=source,
                    conn=conn,
                )

        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upsert_youtube_video(self, item: PlaylistItem, conn: Connection) -> int:
        stmt = (
            pg_insert(youtube_videos)
            .values(
                video_id=item.video_id,
                title=item.title,
                channel_id=item.channel_id or None,
                channel_name=item.channel_name or None,
                description=item.description or None,
                duration_seconds=item.duration_seconds,
                is_available=item.is_available,
            )
            .on_conflict_do_update(
                index_elements=["video_id"],
                set_={
                    "title": item.title,
                    "channel_id": item.channel_id or None,
                    "channel_name": item.channel_name or None,
                    "duration_seconds": item.duration_seconds,
                    "is_available": item.is_available,
                    "updated_at": sa.func.now(),
                },
            )
            .returning(youtube_videos.c.id)
        )
        return conn.execute(stmt).scalar_one()

    def _upsert_playlist_song(
        self,
        *,
        song_id: int,
        playlist_db_id: int,
        yt_video_id: int,
        source: SourceType,
        conn: Connection,
    ) -> None:
        # Check for existing active entry (removed_at IS NULL)
        existing = conn.execute(
            sa.select(playlist_songs.c.id).where(
                sa.and_(
                    playlist_songs.c.song_id == song_id,
                    playlist_songs.c.playlist_id == playlist_db_id,
                    playlist_songs.c.removed_at.is_(None),
                )
            )
        ).one_or_none()

        if existing:
            # Deduplication: song already in this playlist — update source if
            # INDIVIDUAL_VIDEO is more specific than COMPOSITE_VIDEO.
            conn.execute(
                playlist_songs.update()
                .where(playlist_songs.c.id == existing.id)
                .values(
                    youtube_video_id=yt_video_id,
                    source_type=source,
                )
            )
            return

        # Check if song is in any other playlist (cross-playlist deduplication)
        other = conn.execute(
            sa.select(playlist_songs.c.id, playlist_songs.c.playlist_id).where(
                sa.and_(
                    playlist_songs.c.song_id == song_id,
                    playlist_songs.c.playlist_id != playlist_db_id,
                    playlist_songs.c.removed_at.is_(None),
                )
            )
        ).one_or_none()

        if other:
            self._create_task(
                TaskType.DEDUPLICATE_SONGS,
                f"Song {song_id} appears in multiple playlists",
                {
                    "song_id": song_id,
                    "existing_playlist_id": other.playlist_id,
                    "new_playlist_id": playlist_db_id,
                },
                conn,
                related_song_id=song_id,
            )

        conn.execute(
            pg_insert(playlist_songs)
            .values(
                song_id=song_id,
                playlist_id=playlist_db_id,
                youtube_video_id=yt_video_id,
                source_type=source,
            )
            .on_conflict_do_nothing()
        )

    def _create_task(
        self,
        task_type: TaskType,
        title: str,
        data: dict,
        conn: Connection,
        *,
        related_song_id: int | None = None,
        related_video_id: int | None = None,
    ) -> None:
        """Create a task row; idempotent — skips if an OPEN task of same type+song exists."""
        if related_song_id is not None:
            existing = conn.execute(
                sa.select(tasks.c.id).where(
                    sa.and_(
                        tasks.c.task_type == task_type,
                        tasks.c.related_song_id == related_song_id,
                        tasks.c.status == TaskStatus.OPEN,
                    )
                )
            ).one_or_none()
            if existing:
                return

        conn.execute(
            tasks.insert().values(
                task_type=task_type,
                title=title,
                data=data,
                related_song_id=related_song_id,
                related_video_id=related_video_id,
                auto_created_by="ingest_pipeline",
            )
        )

    def _create_ingest_failed_task(self, item: PlaylistItem) -> None:
        """Create an INGEST_FAILED task outside a transaction (best-effort)."""
        try:
            with self._engine.begin() as conn:
                self._create_task(
                    TaskType.INGEST_FAILED,
                    f"Exception during ingest: {item.title!r}",
                    {"video_id": item.video_id, "title": item.title},
                    conn,
                )
        except Exception:
            logger.exception("Could not create INGEST_FAILED task for %s", item.video_id)

    def _integrity_checks(
        self,
        detail: SongDetail,
        song_id: int,
        yt_video_id: int,
        item: PlaylistItem,
        conn: Connection,
    ) -> None:
        """Run metadata integrity checks; create tasks on violations."""
        # Duration mismatch: >20% difference
        if (
            detail.lengthSeconds
            and item.duration_seconds
            and abs(detail.lengthSeconds - item.duration_seconds) / max(detail.lengthSeconds, 1)
            > 0.20
        ):
            self._create_task(
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

        # Missing lyricist
        if detail.has_lyrics:
            has_lyricist = any("Lyricist" in c.role_list for c in detail.artists)
            if not has_lyricist:
                self._create_task(
                    TaskType.MISSING_LYRICIST,
                    f"Song {song_id} has lyrics but no lyricist credited",
                    {"song_id": song_id},
                    conn,
                    related_song_id=song_id,
                )

        # Missing circle
        has_circle = any(
            c.artist and c.artist.artistType.lower() == "circle" for c in detail.artists
        )
        if not has_circle:
            self._create_task(
                TaskType.MISSING_CIRCLE,
                f"Song {song_id} has no circle credited",
                {"song_id": song_id},
                conn,
                related_song_id=song_id,
            )
