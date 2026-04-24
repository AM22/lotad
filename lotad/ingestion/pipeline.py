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
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lotad.agents.llm_extractor import VideoType
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
from lotad.ingestion.mappers import (
    link_album_tracks,
    link_song_originals,
    map_album_to_db,
    map_song_to_db,
)
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

    Currently used to enrich ``INGEST_FAILED`` task data for unmatched album
    videos (M4 LLM fallback reads ``extracted_timestamps`` from task data).

    TODO (M3 composite-video path): also call this in the *matched* album path
    and pass the result to ``map_album_to_db`` so that
    ``album_tracks.youtube_timestamp_seconds`` is populated for each track,
    enabling per-track playback links.
    """
    # "MM:SS Title" — timestamp at start of chapter line (standard YouTube chapters)
    forward = _re.compile(r"(?:(\d{1,2}):)?(\d{1,2}):(\d{2})[ \t]+[.\-–—|]?[ \t]*(.+)")
    # "Title MM:SS" — timestamp at end of line (common album video format)
    reverse = _re.compile(
        r"^(.+?)\s+(?:(\d{1,2}):)?(\d{1,2}):(\d{2})\s*$",
        _re.MULTILINE,
    )

    def _to_seconds(h: str | None, m: str, s: str) -> int:
        return int(h or 0) * 3600 + int(m) * 60 + int(s)

    results: list[tuple[int, str]] = []
    for m in forward.finditer(description):
        results.append((_to_seconds(m.group(1), m.group(2), m.group(3)), m.group(4).strip()))

    if not results:
        for m in reverse.finditer(description):
            results.append((_to_seconds(m.group(2), m.group(3), m.group(4)), m.group(1).strip()))

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

    async def __aexit__(self, *args: object) -> None:
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
        progress_callback: Any | None = None,
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
        #
        # Both the bulk endpoint and the per-video /songs/byPv endpoint use
        # identical PV-lookup logic (exact match on pvService+pvId).  A miss
        # in the bulk result is therefore authoritative — calling /songs/byPv
        # for unmatched videos would never find anything new.
        #
        # Convention used throughout this method and ingest_video:
        #   bulk_map = None  →  bulk failed; fall back to per-video lookup
        #   bulk_map = dict  →  bulk succeeded; its result is authoritative
        #                       (matched → get_song, unmatched → INGEST_FAILED)
        bulk_map: dict[str, int] | None
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
            bulk_map = None

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
        video_type_hint: VideoType | None = None,
        youtube_timestamp_seconds: int | None = None,
    ) -> bool:
        """
        Ingest a single YouTube video.

        Args:
            item: playlist item from the YouTube client.
            playlist_db_id: internal DB id of the playlist this video belongs to.
            bulk_match: result of a prior ``bulk_match_playlist`` call, or ``None``
                if the bulk call failed / was not attempted.
                - ``None``           → per-video ``/songs/byPv`` fallback
                - ``{...}`` (dict)   → authoritative; video in map → ``get_song()``,
                                       video absent → immediate INGEST_FAILED (both
                                       endpoints use identical PV-lookup logic so a
                                       bulk miss is a definitive miss)
            video_type_hint: explicit video type from the LLM (used by the
                composite resolution path to override the ``is_album_video``
                heuristic and suppress spurious duration-mismatch tasks).
            youtube_timestamp_seconds: timestamp within the video where this
                particular song starts (for composite / album videos).  Written
                to ``playlist_songs.youtube_timestamp_seconds``.

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
                        "position": item.position,
                        "playlist_db_id": playlist_db_id,
                        "note": (
                            "YouTube returned a deleted/private stub;"
                            " video is no longer accessible"
                        ),
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
            #
            #   bulk_match is None   → bulk failed; use per-video /songs/byPv
            #   video_id IN bulk     → bulk matched; fetch full detail via get_song()
            #   video_id NOT IN bulk → bulk ran and found nothing; both endpoints
            #                          use identical PV-lookup logic so /songs/byPv
            #                          would return the same null — skip it and
            #                          treat as unmatched immediately
            try:
                if bulk_match is None:
                    song_detail = await self._tdb.lookup_by_youtube_url(item.video_id)
                elif item.video_id in bulk_match:
                    song_detail = await self._tdb.get_song(bulk_match[item.video_id])
                else:
                    song_detail = None  # authoritative miss from bulk
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
                # No TouhouDB match — create a task for manual review (M4 LLM
                # fallback will process these).  Pre-compute album heuristics
                # so M4 can use them without re-fetching YouTube metadata.
                album = is_album_video(item)
                task_data: dict = {
                    "video_id": item.video_id,
                    "title": item.title,
                    "is_album": album,
                    "playlist_db_id": playlist_db_id,
                }
                if album and item.description:
                    timestamps = extract_timestamps(item.description)
                    if timestamps:
                        task_data["extracted_timestamps"] = timestamps
                self._create_task(
                    TaskType.INGEST_FAILED,
                    f"No TouhouDB match: {item.title!r}",
                    task_data,
                    conn,
                    related_video_id=yt_video_id,
                )
                return False

            # 3. Map song to DB
            song_id = map_song_to_db(song_detail, conn)

            # 3.5. Ingest albums this song appears on
            for album_summary in song_detail.albums:
                try:
                    album_detail = await self._tdb.get_album(album_summary.id)
                    album_db_id = map_album_to_db(album_detail, conn)
                    n_linked = link_album_tracks(album_db_id, album_detail, conn)
                    logger.debug(
                        "Ingested album touhoudb_id=%d %r — linked %d tracks",
                        album_summary.id,
                        album_summary.name,
                        n_linked,
                    )
                except Exception:
                    logger.exception(
                        "Failed to ingest album touhoudb_id=%d for song %d — skipping",
                        album_summary.id,
                        song_id,
                    )

            # 4. Resolve original chain + link
            # Start the chain at the song itself, not at originalVersionId.
            # resolve_original_chain needs to pass song_detail as _parent_detail
            # when it recurses into the direct parent, so that the penultimate-node
            # scan for extra originals (medleys encoded in notes/webLinks) fires
            # with the correct context.  Starting at originalVersionId skips the
            # song entirely, leaving _parent_detail=None at the leaf.
            if song_detail.originalVersionId is not None:
                try:
                    original_ids = await self._tdb.resolve_original_chain(song_detail.id)
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

            # Determine composite flag: prefer explicit hint over heuristic.
            # Used by both integrity checks (suppress duration mismatch) and
            # the playlist_songs source_type assignment below.
            is_composite = (
                video_type_hint in (VideoType.FULL_ALBUM, VideoType.COMPOSITE_TRACKS)
                if video_type_hint is not None
                else is_album_video(item)
            )

            # 5. Integrity checks
            self._integrity_checks(
                song_detail, song_id, yt_video_id, item, conn, is_composite=is_composite
            )

            # 6. Create playlist_songs row (if playlist known)
            if playlist_db_id is not None:
                source = SourceType.COMPOSITE_VIDEO if is_composite else SourceType.INDIVIDUAL_VIDEO
                self._upsert_playlist_song(
                    song_id=song_id,
                    playlist_db_id=playlist_db_id,
                    yt_video_id=yt_video_id,
                    source=source,
                    youtube_timestamp_seconds=youtube_timestamp_seconds,
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
        youtube_timestamp_seconds: int | None = None,
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
                    youtube_timestamp_seconds=youtube_timestamp_seconds,
                )
            )
            return

        # Check if song is in any other playlist (cross-playlist deduplication)
        other = conn.execute(
            sa.select(
                playlist_songs.c.id,
                playlist_songs.c.playlist_id,
                playlist_songs.c.source_type,
            ).where(
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
                    "existing_source_type": other.source_type,
                    "new_playlist_id": playlist_db_id,
                    "new_source_type": source.value,
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
                youtube_timestamp_seconds=youtube_timestamp_seconds,
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
        """Create a task row; idempotent — skips if an OPEN task of same type+song/video exists."""
        dedup_filter = None
        if related_song_id is not None:
            dedup_filter = sa.and_(
                tasks.c.task_type == task_type,
                tasks.c.related_song_id == related_song_id,
                tasks.c.status == TaskStatus.OPEN,
            )
        elif related_video_id is not None:
            dedup_filter = sa.and_(
                tasks.c.task_type == task_type,
                tasks.c.related_video_id == related_video_id,
                tasks.c.status == TaskStatus.OPEN,
            )
        if dedup_filter is not None:
            existing = conn.execute(sa.select(tasks.c.id).where(dedup_filter)).first()
            if existing:
                conn.execute(
                    tasks.update().where(tasks.c.id == existing[0]).values(title=title, data=data)
                )
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
                row = conn.execute(
                    sa.select(youtube_videos.c.id).where(youtube_videos.c.video_id == item.video_id)
                ).one_or_none()
                yt_video_db_id = row[0] if row else None
                self._create_task(
                    TaskType.INGEST_FAILED,
                    f"Exception during ingest: {item.title!r}",
                    {"video_id": item.video_id, "title": item.title},
                    conn,
                    related_video_id=yt_video_db_id,
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
        *,
        is_composite: bool = False,
    ) -> None:
        """Run metadata integrity checks; create tasks on violations."""
        # Duration mismatch: >20% difference.
        # Suppressed for composite videos — the video duration covers many songs
        # so it will always mismatch the single-song TouhouDB duration.
        if (
            not is_composite
            and detail.lengthSeconds
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
