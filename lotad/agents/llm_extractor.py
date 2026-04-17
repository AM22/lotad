"""LLM-based video classification and TouhouDB matching.

Two-step pipeline:
  1. Claude API (tool-use, structured output) — classify the video type and
     extract structured search terms from the YouTube title + description.
  2. Python — search TouhouDB, score each candidate deterministically, pick
     the best match and assign a confidence level.

The LLM is NOT used for scoring; scoring is fully deterministic so it can
be audited and tuned without API cost.
"""

from __future__ import annotations

import asyncio
import difflib
import logging
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import anthropic
from pydantic import BaseModel, Field

from lotad.config import Settings
from lotad.db.models import ConfidenceLevel
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.touhoudb_models import AlbumDetail, SongDetail

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from lotad.ingestion.youtube_client import YouTubeClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Confidence thresholds
# ---------------------------------------------------------------------------

_HIGH_THRESHOLD = 0.80
_MEDIUM_THRESHOLD = 0.55


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class VideoType(StrEnum):
    SINGLE_SONG = "single_song"
    FULL_ALBUM = "full_album"
    COMPOSITE_TRACKS = "composite_tracks"


class TrackInfo(BaseModel):
    title: str
    circle_name: str | None = None
    timestamp_seconds: int | None = None


class VideoClassification(BaseModel):
    """Structured output from the LLM classification step."""

    video_type: VideoType
    confidence_in_classification: ConfidenceLevel = ConfidenceLevel.MEDIUM

    # Single song / per-track fields
    song_title: str | None = None
    circle_name: str | None = None
    arranger_names: list[str] = Field(default_factory=list)
    vocalist_names: list[str] = Field(default_factory=list)
    lyricist_names: list[str] = Field(default_factory=list)

    # Album / release context
    album_title: str | None = None
    release_date: str | None = None
    release_event: str | None = None
    original_song_names: list[str] = Field(default_factory=list)
    original_game_name: str | None = None
    bpm: int | None = None

    # Composite tracks
    tracks: list[TrackInfo] = Field(default_factory=list)

    # Whether the video is an original ZUN-sourced composition (not an arrangement).
    # Songs that fall through to stub insertion are disproportionately original
    # compositions since they often won't be on TouhouDB.
    # null = uncertain; true = original; false = arrangement.
    is_original_composition: bool | None = None

    # Shared
    extraction_notes: str | None = None


class CandidateMatch(BaseModel):
    touhoudb_id: int
    name: str
    artist_string: str = ""
    duration_seconds: int | None = None
    album_names: list[str] = Field(default_factory=list)
    score: float = 0.0


class MatchResult(BaseModel):
    """Final result returned by find_match()."""

    video_type: VideoType
    confidence: ConfidenceLevel
    best_match: CandidateMatch | None = None
    all_candidates: list[CandidateMatch] = Field(default_factory=list)
    classification: VideoClassification
    # Full album: list of TouhouDB song IDs for all tracks
    album_track_touhoudb_ids: list[int] = Field(default_factory=list)
    # Composite: per-track sub-results
    track_results: list[MatchResult] = Field(default_factory=list)
    score_breakdown: dict[str, float] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are a metadata extraction agent specialising in Touhou Project fan music.

## Domain knowledge

Touhou Project is a Japanese bullet-hell game series by ZUN. The fan community produces vast
amounts of arranged music ("arrangements") based on ZUN's original compositions. Key terms:

- **Circle**: A doujin music group or solo artist (e.g. "Sound Online", "Foreground Eclipse",
  "Shibayan Records"). Circles are the primary attribution unit.
- **Arrangement**: A fan remix/cover of a ZUN original theme.
- **ZUN original**: The source theme being arranged (e.g. "Faith is for the Transient People",
  "Beloved Tomboyish Girl").
- **XFD / crossfade**: A preview video containing 30-60s clips of multiple tracks from an album.
- **M3, Comiket (C73–C105)**: Doujin events where circles release new albums.

## Title patterns to recognise

- "Song Title / Circle Name", "[Touhou Vocal] Circle Name - Song Title", "[Touhou Vocal] Song Title [Circle Name]", "[Touhou Vocal] Song Title [Circle Name] Original Song Name", "Track Number. Song Name - Arranger(s)", "Vocalist ~ Song Name(Original Song Name)", etc.  — single song
- "Song Title [Album Name] / Circle Name" — single song with album context
- "Song A + Song B [Album]", "Song A × Song B [Circle]", "[Touhou Vocal] Circle | Song A & Song B [Language Subs]" — composite_tracks (multiple songs, one video)
- "Circle Name – Album Title (full album)", "Circle Name – Full Album XFD" — full_album

## Description patterns

Many descriptions are structured like:
  Title: ...
  Circle: ...
  Artist / Arranger: ...
  Album: ...
  Release Date: ...  (e.g. "12-31-2007 (Comiket 73)")
  Original: ...
  Source: ...

Extract all available fields. Leave fields null/empty when genuinely unknown — do not guess. 

Description may signal composite videos even if the title format looks like a single song. Presence of timestamps signals that the song could be (but is not guaranteed to be) a composite.
Example composite description:
Titles: 
Timestamp 1 - Track 1
Timestamp 2 - Track 2

## Classification rules

- **single_song**: One song, one video. Duration typically 2–7 minutes.
- **full_album**: An entire album in one video. Typically 20–80 minutes, often has a timestamped
  tracklist in the description, or "full album" / "XFD" / "クロスフェード" in the title.
- **composite_tracks**: Multiple distinct songs compiled into one video but NOT a full album
  (e.g. "Secret Garden + Scarlet Serenade", "Best of" selections of 2–5 tracks).
  Typically 5–20 minutes.

The pipeline heuristic (`is_album_hint`) may be inaccurate — use your own judgement.
"""

# Tool definition matching VideoClassification
_TOOL_DEF = {
    "name": "classify_video",
    "description": "Classify a YouTube Touhou music video and extract structured metadata.",
    "input_schema": {
        "type": "object",
        "properties": {
            "video_type": {
                "type": "string",
                "enum": ["single_song", "full_album", "composite_tracks"],
                "description": "Classification of the video content.",
            },
            "confidence_in_classification": {
                "type": "string",
                "enum": ["HIGH", "MEDIUM", "LOW"],
                "description": "Confidence in the video_type classification.",
            },
            "song_title": {
                "type": ["string", "null"],
                "description": "Song title for single_song, or null for full_album.",
            },
            "circle_name": {
                "type": ["string", "null"],
                "description": "Primary circle or doujin group name.",
            },
            "arranger_names": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Arranger / producer names.",
            },
            "vocalist_names": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Vocalist names.",
            },
            "lyricist_names": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Lyricist names.",
            },
            "album_title": {
                "type": ["string", "null"],
                "description": "Album title (for full_album or when mentioned in description).",
            },
            "release_date": {
                "type": ["string", "null"],
                "description": "Release date as ISO string (YYYY-MM-DD) or raw string.",
            },
            "release_event": {
                "type": ["string", "null"],
                "description": "Release event name e.g. 'Comiket 73', 'M3-2007秋'.",
            },
            "original_song_names": {
                "type": "array",
                "items": {"type": "string"},
                "description": "ZUN original theme names being arranged.",
            },
            "original_game_name": {
                "type": ["string", "null"],
                "description": "Source game name e.g. 'Mountain of Faith'.",
            },
            "bpm": {
                "type": ["integer", "null"],
                "description": "BPM if stated in description.",
            },
            "tracks": {
                "type": "array",
                "description": "For composite_tracks or full_album: individual track entries.",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "circle_name": {"type": ["string", "null"]},
                        "timestamp_seconds": {"type": ["integer", "null"]},
                    },
                    "required": ["title"],
                },
            },
            "is_original_composition": {
                "type": ["boolean", "null"],
                "description": (
                    "True if this is an original composition (not a Touhou arrangement). "
                    "Null if uncertain. Most videos in this pipeline are arrangements, "
                    "but some circles release original music too."
                ),
            },
            "extraction_notes": {
                "type": ["string", "null"],
                "description": "Notes on uncertainty or unusual patterns found.",
            },
        },
        "required": ["video_type", "confidence_in_classification"],
    },
}

# Tool the LLM can call instead of classify_video when title+description alone
# are not sufficient.  The pipeline responds by fetching YouTube comments and
# then calling Claude a second time with classify_video forced.
_REQUEST_CONTEXT_TOOL: dict = {
    "name": "request_more_context",
    "description": (
        "Call this when the YouTube title and description do not contain enough "
        "information to reliably classify the video or extract artist/original metadata. "
        "The pipeline will fetch the top YouTube comments for this video and provide "
        "them to you in a follow-up message, after which you MUST call classify_video.\n\n"
        "Good reasons to request context:\n"
        "- Description is empty or contains only a URL/social links\n"
        "- Title has two names with no structural markers (circle vs. vocalist ambiguous)\n"
        "- Video appears to be a medley/mashup but no original song names are visible\n\n"
        "Do NOT request context if you can already extract useful metadata — comments "
        "may be equally sparse."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "reason": {
                "type": "string",
                "description": (
                    "Brief explanation of what is missing (e.g. 'description is empty "
                    "and title has two names with no structural role markers — comments "
                    "may contain original song timestamps or circle attribution')."
                ),
            }
        },
        "required": ["reason"],
    },
}


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------


def _fuzzy_similarity(a: str, b: str) -> float:
    """SequenceMatcher ratio between two strings (case-insensitive, stripped)."""
    return difflib.SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _best_title_score(candidate: SongDetail, query_title: str) -> float:
    """Max similarity across candidate name and all additionalNames variants."""
    names = [candidate.name]
    for n in candidate.additionalNames.split(","):
        n = n.strip()
        if n:
            names.append(n)
    return max(_fuzzy_similarity(query_title, n) for n in names)


def _artist_string_score(candidate: SongDetail, circle_name: str) -> float:
    """
    Match circle_name against album artistStrings (primary) and song artistString (fallback).

    The song-level artistString is "Arranger feat. Vocalist" — it does not contain
    the circle name. Album artistStrings (e.g. "ShibayanRecords feat. various") carry
    the circle name in both romanized and Japanese forms and are the correct source.

    Normalises away spaces before substring checks so "Shibayan Records" matches
    "ShibayanRecords" (common TouhouDB style for circle names in album credits).
    """
    cn_lower = circle_name.lower().strip()
    cn_nospace = cn_lower.replace(" ", "")

    def _score_one(artist_string: str) -> float:
        if not artist_string:
            return 0.0
        as_lower = artist_string.lower()
        # Exact substring (with or without spaces) is a very strong signal
        if cn_lower in as_lower or cn_nospace in as_lower.replace(" ", ""):
            return 1.0
        return _fuzzy_similarity(circle_name, artist_string)

    # Primary: album-level artistStrings (contain the circle name)
    album_scores = [
        _score_one(album.artistString) for album in candidate.albums if album.artistString
    ]
    if album_scores:
        best_album = max(album_scores)
        if best_album >= 1.0:
            return 1.0
        # Fallback: song-level artistString (arranger feat. vocalist — weaker signal)
        return max(best_album, _score_one(candidate.artistString))

    # No album data — fall back to song-level only
    return _score_one(candidate.artistString)


def _duration_score(candidate_seconds: int | None, video_seconds: int | None) -> float:
    if candidate_seconds is None or video_seconds is None:
        return 0.0
    diff = abs(candidate_seconds - video_seconds) / max(candidate_seconds, 1)
    if diff <= 0.10:
        return 1.0
    if diff <= 0.20:
        return 0.5
    return 0.0


def _album_score(candidate: SongDetail, album_title: str) -> float:
    album_title_lower = album_title.lower().strip()
    for album in candidate.albums:
        if album_title_lower in album.name.lower():
            return 1.0
        if _fuzzy_similarity(album_title, album.name) > 0.85:
            return 0.85
    return 0.0


def _score_song_candidate(
    candidate: SongDetail,
    classification: VideoClassification,
    video_duration: int | None,
    *,
    confirmed_artist_id: int | None = None,
) -> tuple[float, dict[str, float]]:
    """
    Score a SongDetail against VideoClassification extracted terms.
    Returns (composite_score, breakdown_dict).

    Weights:
      title      0.35
      circle     0.25
      album      0.20
      duration   0.20

    If *confirmed_artist_id* is provided (meaning the search was already filtered
    by this TouhouDB artist ID), and the candidate's artist list contains that ID,
    the circle score is set to 1.0 — bypassing cross-script string comparison which
    fails e.g. for "Shibayan Records" vs "しばやん feat. 3L".
    """
    breakdown: dict[str, float] = {}

    title_q = classification.song_title or ""
    breakdown["title"] = _best_title_score(candidate, title_q) if title_q else 0.0

    circle_q = classification.circle_name or ""
    if confirmed_artist_id is not None and any(
        a.artist is not None and a.artist.id == confirmed_artist_id for a in candidate.artists
    ):
        # Artist was confirmed via ID-based API filter — no string comparison needed
        breakdown["circle"] = 1.0
    else:
        breakdown["circle"] = _artist_string_score(candidate, circle_q) if circle_q else 0.0

    album_q = classification.album_title or ""
    breakdown["album"] = _album_score(candidate, album_q) if album_q else 0.0

    breakdown["duration"] = _duration_score(candidate.lengthSeconds, video_duration)

    # Weighted sum (skip zero-weight dimensions when query field is absent)
    weight_title = 0.35 if title_q else 0.0
    weight_circle = 0.25 if circle_q else 0.0
    weight_album = 0.20 if album_q else 0.0
    weight_duration = 0.20

    total_weight = weight_title + weight_circle + weight_album + weight_duration
    if total_weight == 0:
        return 0.0, breakdown

    score = (
        breakdown["title"] * weight_title
        + breakdown["circle"] * weight_circle
        + breakdown["album"] * weight_album
        + breakdown["duration"] * weight_duration
    ) / total_weight

    return score, breakdown


def _confidence_from_score(score: float) -> ConfidenceLevel:
    if score >= _HIGH_THRESHOLD:
        return ConfidenceLevel.HIGH
    if score >= _MEDIUM_THRESHOLD:
        return ConfidenceLevel.MEDIUM
    return ConfidenceLevel.LOW


def _candidates_from_songs(
    songs: list[SongDetail],
    classification: VideoClassification,
    video_duration: int | None,
    *,
    confirmed_artist_id: int | None = None,
) -> list[CandidateMatch]:
    results = []
    for s in songs:
        score, breakdown = _score_song_candidate(
            s, classification, video_duration, confirmed_artist_id=confirmed_artist_id
        )
        results.append(
            CandidateMatch(
                touhoudb_id=s.id,
                name=s.name,
                artist_string=s.artistString,
                duration_seconds=s.lengthSeconds,
                album_names=[a.name for a in s.albums],
                score=score,
            )
        )
    results.sort(key=lambda c: c.score, reverse=True)
    return results


# ---------------------------------------------------------------------------
# LLMExtractor
# ---------------------------------------------------------------------------


class LLMExtractor:
    """
    Classify and match a YouTube video against TouhouDB.

    Usage::

        async with TouhouDBClient.from_settings(settings) as tdb:
            extractor = LLMExtractor(settings=settings, tdb_client=tdb)
            result = await extractor.find_match(
                title="...", description="...", duration_seconds=272
            )
    """

    def __init__(
        self,
        settings: Settings,
        tdb_client: TouhouDBClient,
        *,
        youtube_client: YouTubeClient | None = None,
    ) -> None:
        self._settings = settings
        self._tdb = tdb_client
        self._yt = youtube_client
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def classify_video(
        self,
        *,
        title: str,
        description: str,
        duration_seconds: int | None,
        channel_name: str | None = None,
        is_album_hint: bool = False,
        youtube_video_id: str | None = None,
    ) -> VideoClassification:
        """
        Call Claude to classify the video and extract structured search terms.

        Two-tool pattern (input suspension):
          - First call offers both ``classify_video`` and ``request_more_context``.
          - If the LLM signals that title + description are insufficient, fetch
            the top YouTube comments and continue the conversation, then force
            ``classify_video`` on a second pass.
          - If no ``youtube_video_id`` / ``YouTubeClient`` is available, or if
            comments are disabled, the second pass still runs — just without
            the extra context.
        """
        dur_str = (
            f"{duration_seconds // 60}:{duration_seconds % 60:02d}"
            if duration_seconds
            else "unknown"
        )
        user_message = (
            f"YouTube title: {title}\n"
            f"Channel: {channel_name or 'unknown'}\n"
            f"Duration: {dur_str}\n"
            f"Pipeline heuristic is_album_hint: {is_album_hint} (may be inaccurate)\n"
            f"\nDescription:\n{description}"
        )

        # First pass — offer both tools so the LLM can request comments if needed
        messages: list[dict] = [{"role": "user", "content": user_message}]
        response = await self._client.messages.create(
            model=self._settings.anthropic_model,
            max_tokens=2048,
            system=_SYSTEM_PROMPT,
            tools=[_TOOL_DEF, _REQUEST_CONTEXT_TOOL],
            tool_choice={"type": "any"},
            messages=messages,
        )

        # Check which tool was called
        tool_use_block = None
        for block in response.content:
            if block.type == "tool_use":
                tool_use_block = block
                break

        if tool_use_block is not None and tool_use_block.name == "request_more_context":
            reason = tool_use_block.input.get("reason", "")
            logger.info(
                "LLM requested more context for %r: %s",
                title,
                reason,
            )

            # Fetch comments (best-effort — empty string if unavailable)
            comments_text = ""
            if self._yt is not None and youtube_video_id is not None:
                comments = await asyncio.to_thread(
                    self._yt.get_video_comments,
                    youtube_video_id,
                    max_results=15,
                )
                if comments:
                    comments_text = "Top YouTube comments:\n" + "\n---\n".join(comments)
                    logger.debug("Fetched %d comments for %s", len(comments), youtube_video_id)
                else:
                    comments_text = "(Comments are disabled or unavailable for this video.)"
            else:
                comments_text = "(No YouTube client available — cannot fetch comments.)"

            # Continue the conversation: tool result → second pass (forced)
            messages = [
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": response.content},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use_block.id,
                            "content": comments_text,
                        }
                    ],
                },
            ]
            response = await self._client.messages.create(
                model=self._settings.anthropic_model,
                max_tokens=2048,
                system=_SYSTEM_PROMPT,
                tools=[_TOOL_DEF],
                tool_choice={"type": "tool", "name": "classify_video"},
                messages=messages,
            )
            # Re-scan for classify_video block in the new response
            tool_use_block = None
            for block in response.content:
                if block.type == "tool_use" and block.name == "classify_video":
                    tool_use_block = block
                    break

        # Extract classify_video input
        tool_input: dict[str, Any] = {}
        if tool_use_block is not None and tool_use_block.name == "classify_video":
            tool_input = tool_use_block.input

        if not tool_input:
            logger.warning("LLM returned no tool_use block for title=%r", title)
            return VideoClassification(
                video_type=VideoType.SINGLE_SONG,
                confidence_in_classification=ConfidenceLevel.LOW,
                extraction_notes="LLM returned no structured output",
            )

        # Map track dicts
        raw_tracks = tool_input.pop("tracks", []) or []
        tracks = [TrackInfo.model_validate(t) for t in raw_tracks]
        tool_input["tracks"] = tracks

        return VideoClassification.model_validate(tool_input)

    async def _resolve_artist_id(
        self,
        circle_name: str,
        conn: Connection | None,
    ) -> int | None:
        """
        Resolve a circle name to a TouhouDB artist ID.

        Search strategy (in order):
          1. Look up ``artists.touhoudb_id`` in our local DB by name (exact, then ILIKE).
             Most circles will already be present after bulk ingestion.
          2. Fall back to TouhouDB ``/api/artists`` text search.

        Returns the TouhouDB artist ID, or None if unresolvable.
        """
        import sqlalchemy as sa

        from lotad.db.models import artists as artists_table

        if conn is not None:
            # 1a. Exact name match
            row = conn.execute(
                sa.select(artists_table.c.touhoudb_id).where(
                    artists_table.c.name == circle_name,
                    artists_table.c.touhoudb_id.isnot(None),
                )
            ).one_or_none()
            if row:
                return row[0]

            # 1b. Case-insensitive ILIKE (handles minor capitalisation differences)
            row = conn.execute(
                sa.select(artists_table.c.touhoudb_id).where(
                    artists_table.c.name.ilike(circle_name),
                    artists_table.c.touhoudb_id.isnot(None),
                )
            ).one_or_none()
            if row:
                return row[0]

        # 2. TouhouDB artist search fallback
        try:
            results = await self._tdb.search_artists(circle_name, max_results=3)
        except Exception as exc:
            logger.warning(
                "TouhouDB artist search failed for %r: %s — proceeding without artist_id filter",
                circle_name,
                exc,
            )
            return None

        if not results:
            logger.debug("TouhouDB artist search returned no results for %r", circle_name)
            return None

        # Pick the result whose name is most similar to the query
        best = max(
            results,
            key=lambda a: _fuzzy_similarity(circle_name, a.name),
        )
        sim = _fuzzy_similarity(circle_name, best.name)
        if sim < 0.6:
            logger.debug(
                "Best TouhouDB artist match for %r is %r (sim=%.2f < 0.6) — skipping",
                circle_name,
                best.name,
                sim,
            )
            return None  # Not confident enough to use

        logger.debug(
            "TouhouDB artist search: %r → id=%d %r (sim=%.2f)",
            circle_name,
            best.id,
            best.name,
            sim,
        )
        return best.id

    async def find_match(
        self,
        *,
        title: str,
        description: str,
        duration_seconds: int | None,
        channel_name: str | None = None,
        is_album_hint: bool = False,
        conn: Connection | None = None,
        youtube_video_id: str | None = None,
    ) -> MatchResult:
        """
        Full matching pipeline:
          1. classify_video → VideoClassification (with optional comment fetch)
          2. Resolve circle name → TouhouDB artist ID (DB first, then TouhouDB search)
          3. Search TouhouDB based on video_type, filtered by artist ID when available
          4. Score candidates deterministically
          5. Return MatchResult with confidence
        """
        classification = await self.classify_video(
            title=title,
            description=description,
            duration_seconds=duration_seconds,
            channel_name=channel_name,
            is_album_hint=is_album_hint,
            youtube_video_id=youtube_video_id,
        )

        # Pre-resolve artist ID — used by all search paths
        artist_id: int | None = None
        if classification.circle_name:
            artist_id = await self._resolve_artist_id(classification.circle_name, conn)
            if artist_id:
                logger.debug(
                    "Resolved circle %r → TouhouDB artist_id=%d",
                    classification.circle_name,
                    artist_id,
                )
            else:
                logger.warning(
                    "Could not resolve artist_id for circle %r — will search by title only",
                    classification.circle_name,
                )

        vtype = classification.video_type

        if vtype == VideoType.FULL_ALBUM:
            return await self._match_full_album(classification, duration_seconds, artist_id)
        elif vtype == VideoType.COMPOSITE_TRACKS:
            return await self._match_composite(classification, duration_seconds, artist_id)
        else:
            return await self._match_single_song(classification, duration_seconds, artist_id)

    async def _match_single_song(
        self,
        classification: VideoClassification,
        video_duration: int | None,
        artist_id: int | None = None,
    ) -> MatchResult:
        """Search and score for a single-song video."""
        query = classification.song_title or ""
        if not query:
            return MatchResult(
                video_type=VideoType.SINGLE_SONG,
                confidence=ConfidenceLevel.LOW,
                classification=classification,
            )

        # Prefer artist_id filter (exact, supported by TouhouDB);
        # artistName text filter is NOT supported (returns 400), so fall
        # straight through to query-only when no artist_id is available.
        if artist_id is not None:
            songs = await self._tdb.search_songs(query, artist_id=artist_id)
            if not songs:
                # artist_id filter may be too narrow (e.g. circle ≠ primary artist);
                # retry without filter
                songs = await self._tdb.search_songs(query)
        else:
            songs = await self._tdb.search_songs(query)

        candidates = _candidates_from_songs(
            songs, classification, video_duration, confirmed_artist_id=artist_id
        )
        best = candidates[0] if candidates else None
        confidence = _confidence_from_score(best.score if best else 0.0)
        breakdown = {}
        if best and songs:
            _, breakdown = _score_song_candidate(
                next((s for s in songs if s.id == best.touhoudb_id), songs[0]),
                classification,
                video_duration,
                confirmed_artist_id=artist_id,
            )

        return MatchResult(
            video_type=VideoType.SINGLE_SONG,
            confidence=confidence,
            best_match=best if (best and best.score >= _MEDIUM_THRESHOLD) else None,
            all_candidates=candidates[:5],
            classification=classification,
            score_breakdown=breakdown,
        )

    async def _match_full_album(
        self,
        classification: VideoClassification,
        video_duration: int | None,
        artist_id: int | None = None,
    ) -> MatchResult:
        """Search TouhouDB for an album and return all track IDs."""
        query = classification.album_title or ""
        if not query:
            # No album title extracted; fall back to song-level search
            return await self._match_single_song(classification, video_duration, artist_id)

        if artist_id is not None:
            albums: list[AlbumDetail] = await self._tdb.search_albums(query, artist_id=artist_id)
            if not albums:
                albums = await self._tdb.search_albums(query)
        else:
            albums = await self._tdb.search_albums(query)

        if not albums:
            return MatchResult(
                video_type=VideoType.FULL_ALBUM,
                confidence=ConfidenceLevel.LOW,
                classification=classification,
            )

        # Score albums by title similarity (simple)
        def _album_name_score(album: AlbumDetail) -> float:
            names = [album.name]
            for n in album.additionalNames.split(","):
                n = n.strip()
                if n:
                    names.append(n)
            return max(_fuzzy_similarity(query, n) for n in names)

        best_album = max(albums, key=_album_name_score)
        score = _album_name_score(best_album)
        confidence = _confidence_from_score(score)

        track_ids = [t.song.id for t in best_album.tracks if t.song is not None]

        best_candidate = CandidateMatch(
            touhoudb_id=best_album.id,
            name=best_album.name,
            artist_string=best_album.artistString,
            duration_seconds=None,
            album_names=[],
            score=score,
        )

        return MatchResult(
            video_type=VideoType.FULL_ALBUM,
            confidence=confidence,
            best_match=best_candidate if score >= _MEDIUM_THRESHOLD else None,
            classification=classification,
            album_track_touhoudb_ids=track_ids,
            score_breakdown={"album_title": score},
        )

    async def _match_composite(
        self,
        classification: VideoClassification,
        video_duration: int | None,
        artist_id: int | None = None,
    ) -> MatchResult:
        """Match each track in a composite video independently."""
        track_results: list[MatchResult] = []

        for track in classification.tracks:
            per_track_cls = VideoClassification(
                video_type=VideoType.SINGLE_SONG,
                confidence_in_classification=classification.confidence_in_classification,
                song_title=track.title,
                circle_name=track.circle_name or classification.circle_name,
                album_title=classification.album_title,
            )
            result = await self._match_single_song(
                per_track_cls, video_duration=None, artist_id=artist_id
            )
            track_results.append(result)

        # Overall confidence = minimum across tracks
        all_confs = [r.confidence for r in track_results]
        conf_order = [ConfidenceLevel.HIGH, ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW]
        overall_conf = (
            min(all_confs, key=lambda c: conf_order.index(c)) if all_confs else ConfidenceLevel.LOW
        )

        return MatchResult(
            video_type=VideoType.COMPOSITE_TRACKS,
            confidence=overall_conf,
            classification=classification,
            track_results=track_results,
        )
