"""TouhouDB REST API client.

Usage::

    async with TouhouDBClient.from_settings(get_settings()) as client:
        detail = await client.get_song(12345)
        chain  = await client.resolve_original_chain(12345)

All public methods raise:
- ``CircuitBreakerOpen``  — breaker is open; don't retry
- ``httpx.HTTPStatusError`` — 4xx/5xx (non-retryable 4xx surfaced directly)
- ``httpx.NetworkError``    — network-level failure (after retries)
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential

from lotad.config import Settings
from lotad.ingestion.http_client import (
    CircuitBreaker,
    CircuitBreakerOpen,
    build_async_client,
    is_retryable,
)
from lotad.ingestion.touhoudb_models import (
    AlbumDetail,
    ArtistDetail,
    SongDetail,
)

logger = logging.getLogger(__name__)

_YOUTUBE_ID_RE = re.compile(
    r"(?:youtu\.be/|youtube\.com/(?:watch\?v=|shorts/|embed/))([A-Za-z0-9_-]{11})"
)

# Fields requested from TouhouDB for full song detail
_SONG_FIELDS = "Artists,Albums,Tags,PVs"
# Fields requested for album detail (includes Tracks)
_ALBUM_FIELDS = "Artists,Tags,Tracks"
# Fields requested for artist detail
_ARTIST_FIELDS = "Groups,Tags"


def _extract_youtube_id(url: str) -> str | None:
    """Extract 11-char YouTube video ID from various URL formats."""
    m = _YOUTUBE_ID_RE.search(url)
    if m:
        return m.group(1)
    # Bare 11-char ID passed directly
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", url):
        return url
    return None


class TouhouDBClient:
    """
    Async client for the TouhouDB REST API.

    Wraps an httpx client with:
    - RFC-7234 disk-backed HTTP cache (via hishel)
    - Exponential-backoff retry on transient failures
    - Circuit breaker to avoid hammering an unavailable API
    """

    def __init__(
        self,
        settings: Settings,
        *,
        cache_dir: str = ".cache/http",
    ) -> None:
        self._settings = settings
        self._cache_dir = cache_dir
        self._circuit_breaker = CircuitBreaker(settings.touhoudb_circuit_breaker_threshold)
        self._http: Any = None  # hishel.AsyncCacheClient, set in __aenter__

    @classmethod
    def from_settings(cls, settings: Settings, **kwargs: Any) -> TouhouDBClient:
        return cls(settings, **kwargs)

    async def __aenter__(self) -> TouhouDBClient:
        self._http = build_async_client(
            base_url=self._settings.touhoudb_base_url,
            timeout=self._settings.touhoudb_request_timeout,
            cache_dir=self._cache_dir,
        )
        await self._http.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._http is not None:
            await self._http.__aexit__(*args)
            self._http = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get(self, path: str, **params: Any) -> Any:
        """
        GET ``path`` with ``params``, respecting the circuit breaker and
        retrying on transient failures.

        Returns parsed JSON.  Raises on circuit-open or unrecoverable errors.
        """
        if self._circuit_breaker.is_open:
            raise CircuitBreakerOpen("TouhouDB circuit breaker is open; skipping network call")

        try:
            # AsyncRetrying handles intermediate retry waits + back-off.
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._settings.touhoudb_max_retries),
                wait=wait_exponential(multiplier=1, min=1, max=30),
                retry=retry_if_exception(is_retryable),
                reraise=True,
            ):
                with attempt:
                    response = await self._http.get(path, params=params)
                    response.raise_for_status()
                    # Record success here — the ``else`` clause on try/except
                    # is dead code when ``return`` is inside the try block.
                    self._circuit_breaker.record_success()
                    return response.json()
        except Exception:
            self._circuit_breaker.record_failure()
            raise

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def lookup_by_youtube_url(self, url: str) -> SongDetail | None:
        """
        Look up a song by YouTube URL (or bare video ID).

        Uses the ``/songs/byPv`` endpoint which matches on PV entries
        stored in TouhouDB.  Returns ``None`` if no match is found (404).
        """
        video_id = _extract_youtube_id(url)
        if not video_id:
            logger.debug("Could not extract YouTube video ID from %r", url)
            return None

        try:
            data = await self._get(
                "/songs/byPv",
                pvService="Youtube",
                pvId=video_id,
                fields=_SONG_FIELDS,
                lang="Default",
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                # 404 means "no match" — the API responded correctly, so
                # treat this as a success from the circuit-breaker's perspective.
                self._circuit_breaker.record_success()
                logger.debug("No TouhouDB match for YouTube video %s", video_id)
                return None
            raise
        return SongDetail.model_validate(data)

    async def get_song(self, song_id: int) -> SongDetail:
        """Fetch full song detail by TouhouDB song ID."""
        data = await self._get(f"/songs/{song_id}", fields=_SONG_FIELDS, lang="Default")
        return SongDetail.model_validate(data)

    async def get_album(self, album_id: int) -> AlbumDetail:
        """Fetch full album detail by TouhouDB album ID."""
        data = await self._get(f"/albums/{album_id}", fields=_ALBUM_FIELDS, lang="Default")
        return AlbumDetail.model_validate(data)

    async def get_artist(self, artist_id: int) -> ArtistDetail:
        """Fetch full artist detail by TouhouDB artist ID."""
        data = await self._get(f"/artists/{artist_id}", fields=_ARTIST_FIELDS, lang="Default")
        return ArtistDetail.model_validate(data)

    async def resolve_original_chain(
        self,
        song_id: int,
        *,
        _visited: frozenset[int] | None = None,
        _depth: int = 0,
        max_depth: int = 10,
    ) -> list[int]:
        """
        Recursively follow ``originalVersionId`` links until reaching root
        original(s) with no further original version.

        Returns a list of TouhouDB song IDs that are the "leaves" of the
        original chain (i.e. the actual Touhou source themes).

        Handles:
        - Cycles (via visited set)
        - Max depth (returns current node if depth exceeded)

        TODO (M5 — multiple originals): TouhouDB's schema only has one
        ``originalVersionId`` FK, but medleys and mashups draw from multiple
        Touhou source themes.  The additional originals are encoded in the
        song's description or unofficial links when the ``multiple originals``
        tag is present.  Once we reach the leaf node, check for that tag and,
        if set, parse the description / TouhouDB reference link to extract the
        full list of originals.  This requires either regex parsing of the
        description field or following the "TouhouDB" unofficial-link URL
        to a second song page.  Deferred to M5 alongside the character mapper.
        """
        if _visited is None:
            _visited = frozenset()
        if song_id in _visited or _depth >= max_depth:
            logger.warning(
                "Original chain for song %d: cycle or max depth reached at depth %d",
                song_id,
                _depth,
            )
            return [song_id]

        try:
            detail = await self.get_song(song_id)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return [song_id]
            raise

        if detail.originalVersionId is None:
            # This song IS the original (or has no linked original)
            return [song_id]

        _visited = _visited | {song_id}
        return await self.resolve_original_chain(
            detail.originalVersionId,
            _visited=_visited,
            _depth=_depth + 1,
            max_depth=max_depth,
        )

    async def lookup_playlist_bulk(
        self,
        playlist_url: str,
    ) -> list[SongDetail | None]:
        """
        Bulk-match all videos in a YouTube playlist against TouhouDB in one
        API call, returning a list in playlist order.

        Each element is a ``SongDetail`` if TouhouDB has a match for that
        position, or ``None`` if no match was found.

        TouhouDB exposes a playlist-import endpoint used by its "Create song
        list from YouTube playlist" UI feature.  One observed request from the
        browser is::

            GET /api/songs/import?url=<youtube-playlist-url>&fields=Artists,Tags,PVs

        The response shape is ``{"items": [{"matchedSong": {...} | null}]}``.

        TODO: Verify the exact endpoint path and response schema against the
        live TouhouDB API (the path above was inferred from browser devtools).
        Once confirmed, wire this into ``IngestPipeline.ingest_playlist`` as
        the primary match step: call this once for the whole playlist, then
        iterate items — calling ``get_song`` only for confirmed matches to
        fetch full detail (artists/tags/albums), and skipping the per-video
        ``lookup_by_youtube_url`` calls entirely for unmatched videos.
        This reduces TouhouDB API calls from O(N) to O(matched) for the lookup
        phase.
        """
        try:
            data = await self._get(
                "/songs/import",
                url=playlist_url,
                fields=_SONG_FIELDS,
                lang="Default",
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (404, 501):
                logger.warning(
                    "Bulk playlist import endpoint unavailable (status %d); "
                    "fall back to per-video lookup",
                    exc.response.status_code,
                )
                return []
            raise

        items = data.get("items", []) if isinstance(data, dict) else []
        results: list[SongDetail | None] = []
        for item in items:
            matched = item.get("matchedSong") if isinstance(item, dict) else None
            results.append(SongDetail.model_validate(matched) if matched else None)
        return results

    async def get_normalization_count(
        self,
        entity_type: str,
        entity_id: int,
    ) -> int:
        """
        Return the total arrangement count for an entity from TouhouDB.

        ``entity_type`` must be one of: ``"ORIGINAL_SONG"``, ``"ARTIST"``,
        ``"CIRCLE"``.

        Uses ``GET /api/songs`` with a filter and ``getTotalCount=true``,
        requesting zero items (only the count is needed).

        NOTE: Not called during ingestion.  This is infrastructure for M5's
        ``lotad/sync/normalization.py`` → ``NormalizationFetcher``, which
        populates the ``normalization_metrics`` table used by the scoring
        engine to weight scores by popularity.
        """
        params: dict[str, Any] = {
            "maxResults": 0,
            "getTotalCount": "true",
            "status": "Finished",
            "fields": "None",
        }

        if entity_type == "ORIGINAL_SONG":
            params["originalVersionId"] = entity_id
        elif entity_type in ("ARTIST", "CIRCLE"):
            params["artistId"] = entity_id
        else:
            raise ValueError(f"Unknown entity_type: {entity_type!r}")

        data = await self._get("/songs", **params)
        self._circuit_breaker.record_success()
        count = data.get("totalCount", 0) if isinstance(data, dict) else 0
        return int(count)
