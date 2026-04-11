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
    ImportedSongList,
    PartialImportedSongs,
    SongDetail,
    SongDetailList,
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
        if data is None:
            # TouhouDB returns HTTP 200 with a null body when no PV match is
            # found (rather than 404).  Treat this the same as 404.
            logger.debug("No TouhouDB match for YouTube video %s (null response)", video_id)
            return None
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

    async def bulk_match_playlist(
        self,
        playlist_id: str,
        *,
        max_results: int = 50,
    ) -> dict[str, int]:
        """
        Bulk-match all videos in a YouTube playlist against TouhouDB.

        Uses the ``/api/songLists/import`` endpoints (paginated) to retrieve
        TouhouDB match info for every video in the playlist in
        ``O(ceil(N / max_results))`` API calls instead of ``O(N)`` individual
        ``/songs/byPv`` calls.

        Returns a ``dict`` mapping YouTube video ID → TouhouDB song ID for
        every video that has a confirmed match.  Unmatched videos are omitted.

        The ``matchedSong`` returned by these endpoints is a basic
        ``SongForApiContract`` (no artists/tags/albums).  Callers should call
        ``get_song(id)`` for each matched ID to fetch full detail.

        Endpoint details (from VocaDB source, verified against TouhouDB)::

            # First page + playlist metadata
            GET /api/songLists/import?url=<yt-playlist-url>&parseAll=true
            → ImportedSongList  (top-level has .songs: PartialImportedSongs)

            # Subsequent pages
            GET /api/songLists/import-songs
                ?url=<yt-playlist-url>&pageToken=<token>&maxResults=<n>
            → PartialImportedSongs  (.nextPageToken is null on last page)

        Both endpoints are undocumented in Swagger (marked IgnoreApi=true in
        VocaDB source) but are stable UI-facing endpoints.

        Args:
            playlist_id: YouTube playlist ID (the ``PL…`` part, not a URL).
            max_results: items per page for subsequent pages (default 50).

        Raises:
            CircuitBreakerOpen: if the circuit breaker is open.
            httpx.HTTPStatusError: on unrecoverable API errors.
        """
        playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
        matched: dict[str, int] = {}

        # --- First page (also returns playlist metadata) ---
        try:
            first_data = await self._get(
                "/songLists/import",
                url=playlist_url,
                parseAll="true",
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (404, 501):
                logger.warning(
                    "songLists/import unavailable (status %d); falling back to per-video lookup",
                    exc.response.status_code,
                )
                return {}
            raise

        first = ImportedSongList.model_validate(first_data)
        for item in first.songs.items:
            if item.matchedSong is not None and item.pvId:
                matched[item.pvId] = item.matchedSong.id

        page_token: str | None = first.songs.nextPageToken

        # --- Subsequent pages ---
        while page_token:
            try:
                page_data = await self._get(
                    "/songLists/import-songs",
                    url=playlist_url,
                    pageToken=page_token,
                    maxResults=max_results,
                    parseAll="true",
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (404, 501):
                    logger.warning(
                        "songLists/import-songs unavailable (status %d); stopping pagination",
                        exc.response.status_code,
                    )
                    break
                raise

            page = PartialImportedSongs.model_validate(page_data)
            for item in page.items:
                if item.matchedSong is not None and item.pvId:
                    matched[item.pvId] = item.matchedSong.id
            page_token = page.nextPageToken

        logger.info(
            "bulk_match_playlist %s: %d/%d videos matched",
            playlist_id,
            len(matched),
            first.songs.totalCount,
        )
        return matched

    async def get_songs_by_artist(
        self,
        artist_id: int,
        song_type: str = "Original",
        max_results: int = 50,
    ) -> list[SongDetail]:
        """
        Fetch all songs by an artist filtered by song type.

        Paginates automatically through the full result set using
        ``GET /api/songs`` with ``artistId`` and ``songTypes`` filters.

        Args:
            artist_id: TouhouDB artist ID (e.g. 1 for ZUN, 45 for U2 Akiyama).
            song_type: TouhouDB song type filter (default "Original").
            max_results: Page size for each API call (default 50).

        Returns:
            List of full ``SongDetail`` objects (with Artists, Albums, Tags).
        """
        all_songs: list[SongDetail] = []
        start = 0

        while True:
            data = await self._get(
                "/songs",
                artistId=artist_id,
                songTypes=song_type,
                fields=_SONG_FIELDS,
                lang="Default",
                start=start,
                maxResults=max_results,
                getTotalCount="true",
            )
            page = SongDetailList.model_validate(data)
            all_songs.extend(page.items)

            fetched_so_far = start + len(page.items)
            if fetched_so_far >= page.totalCount or not page.items:
                break
            start = fetched_so_far

        logger.info(
            "get_songs_by_artist artist_id=%d type=%s: fetched %d songs",
            artist_id,
            song_type,
            len(all_songs),
        )
        return all_songs

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
