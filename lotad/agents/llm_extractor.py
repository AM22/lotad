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

import difflib
import enum
import logging
from typing import Any

import anthropic
from pydantic import BaseModel, Field

from lotad.config import Settings
from lotad.db.models import ConfidenceLevel
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.touhoudb_models import AlbumDetail, SongDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Confidence thresholds
# ---------------------------------------------------------------------------

_HIGH_THRESHOLD = 0.80
_MEDIUM_THRESHOLD = 0.55


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class VideoType(str, enum.Enum):
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
  These are full_album videos.
- **M3, Comiket (C73–C105)**: Doujin events where circles release new albums.

## Title patterns to recognise

- "Song Title / Circle Name" — single song
- "Song Title [Album Name] / Circle Name" — single song with album context
- "Song A + Song B [Album]" or "Song A × Song B" — composite_tracks (multiple songs, one video)
- "Circle Name – Album Title (full album)", "Circle Name – Full Album XFD" — full_album
- "Album Name クロスフェード", "Album Title M3-XXXX" — full_album (crossfade / event release)

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

## Classification rules

- **single_song**: One song, one video. Duration typically 2–7 minutes.
- **full_album**: An entire album in one video. Typically 20–80 minutes, often has a timestamped
  tracklist in the description, or "full album" / "XFD" / "クロスフェード" in the title.
- **composite_tracks**: Multiple distinct songs compiled into one video but NOT a full album
  (e.g. "Secret Garden + Scarlet Serenade", medleys, "Best of" selections of 2–5 tracks).
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
            "extraction_notes": {
                "type": ["string", "null"],
                "description": "Notes on uncertainty or unusual patterns found.",
            },
        },
        "required": ["video_type", "confidence_in_classification"],
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
    Substring / fuzzy match between circle_name and candidate.artistString.

    artistString is always populated (e.g. "Shibayan feat. itori") and is
    the most reliable way to get circle info from a SongDetail without
    traversing albums.
    """
    as_lower = candidate.artistString.lower()
    cn_lower = circle_name.lower().strip()
    # Exact substring is a very strong signal
    if cn_lower in as_lower:
        return 1.0
    # Fuzzy fallback
    return _fuzzy_similarity(circle_name, candidate.artistString)


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
) -> tuple[float, dict[str, float]]:
    """
    Score a SongDetail against VideoClassification extracted terms.
    Returns (composite_score, breakdown_dict).

    Weights:
      title      0.35
      circle     0.25
      album      0.20
      duration   0.20
    """
    breakdown: dict[str, float] = {}

    title_q = classification.song_title or ""
    breakdown["title"] = _best_title_score(candidate, title_q) if title_q else 0.0

    circle_q = classification.circle_name or ""
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
) -> list[CandidateMatch]:
    results = []
    for s in songs:
        score, breakdown = _score_song_candidate(s, classification, video_duration)
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

    def __init__(self, settings: Settings, tdb_client: TouhouDBClient) -> None:
        self._settings = settings
        self._tdb = tdb_client
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def classify_video(
        self,
        *,
        title: str,
        description: str,
        duration_seconds: int | None,
        channel_name: str | None = None,
        is_album_hint: bool = False,
    ) -> VideoClassification:
        """
        Call Claude to classify the video and extract structured search terms.
        Uses tool-use (forced) for reliable structured output.
        """
        dur_str = f"{duration_seconds // 60}:{duration_seconds % 60:02d}" if duration_seconds else "unknown"
        user_message = (
            f"YouTube title: {title}\n"
            f"Channel: {channel_name or 'unknown'}\n"
            f"Duration: {dur_str}\n"
            f"Pipeline heuristic is_album_hint: {is_album_hint} (may be inaccurate)\n"
            f"\nDescription (first 2000 chars):\n{description[:2000]}"
        )

        response = await self._client.messages.create(
            model=self._settings.anthropic_model,
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            tools=[_TOOL_DEF],
            tool_choice={"type": "tool", "name": "classify_video"},
            messages=[{"role": "user", "content": user_message}],
        )

        # Extract tool_use block
        tool_input: dict[str, Any] = {}
        for block in response.content:
            if block.type == "tool_use" and block.name == "classify_video":
                tool_input = block.input
                break

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

    async def find_match(
        self,
        *,
        title: str,
        description: str,
        duration_seconds: int | None,
        channel_name: str | None = None,
        is_album_hint: bool = False,
    ) -> MatchResult:
        """
        Full matching pipeline:
          1. classify_video → VideoClassification
          2. Search TouhouDB based on video_type
          3. Score candidates deterministically
          4. Return MatchResult with confidence
        """
        classification = await self.classify_video(
            title=title,
            description=description,
            duration_seconds=duration_seconds,
            channel_name=channel_name,
            is_album_hint=is_album_hint,
        )

        vtype = classification.video_type

        if vtype == VideoType.FULL_ALBUM:
            return await self._match_full_album(classification, duration_seconds)
        elif vtype == VideoType.COMPOSITE_TRACKS:
            return await self._match_composite(classification, duration_seconds)
        else:
            return await self._match_single_song(classification, duration_seconds)

    async def _match_single_song(
        self,
        classification: VideoClassification,
        video_duration: int | None,
    ) -> MatchResult:
        """Search and score for a single-song video."""
        query = classification.song_title or ""
        if not query:
            return MatchResult(
                video_type=VideoType.SINGLE_SONG,
                confidence=ConfidenceLevel.LOW,
                classification=classification,
            )

        songs = await self._tdb.search_songs(
            query, artist_name=classification.circle_name
        )

        # Fallback: if artist_name filter returned nothing, try without
        if not songs and classification.circle_name:
            songs = await self._tdb.search_songs(query)

        candidates = _candidates_from_songs(songs, classification, video_duration)
        best = candidates[0] if candidates else None
        confidence = _confidence_from_score(best.score if best else 0.0)
        breakdown = {}
        if best and songs:
            _, breakdown = _score_song_candidate(
                next((s for s in songs if s.id == best.touhoudb_id), songs[0]),
                classification,
                video_duration,
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
    ) -> MatchResult:
        """Search TouhouDB for an album and return all track IDs."""
        query = classification.album_title or ""
        if not query:
            # No album title extracted; fall back to song-level search
            return await self._match_single_song(classification, video_duration)

        albums: list[AlbumDetail] = await self._tdb.search_albums(
            query, artist_name=classification.circle_name
        )
        if not albums and classification.circle_name:
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

        track_ids = [
            t.song.id
            for t in best_album.tracks
            if t.song is not None
        ]

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
            result = await self._match_single_song(per_track_cls, video_duration=None)
            track_results.append(result)

        # Overall confidence = minimum across tracks
        all_confs = [r.confidence for r in track_results]
        conf_order = [ConfidenceLevel.HIGH, ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW]
        overall_conf = min(all_confs, key=lambda c: conf_order.index(c)) if all_confs else ConfidenceLevel.LOW

        return MatchResult(
            video_type=VideoType.COMPOSITE_TRACKS,
            confidence=overall_conf,
            classification=classification,
            track_results=track_results,
        )
