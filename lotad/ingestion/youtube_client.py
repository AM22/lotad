"""YouTube Data API v3 client for reading playlist contents.

Uses the official google-api-python-client (synchronous).  The async
wrapping is done at the CLI level via asyncio.run(); the YouTube API itself
is I/O-bound but not high-frequency so synchronous calls are fine here.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pydantic import BaseModel

from lotad.config import Settings

logger = logging.getLogger(__name__)


class PlaylistItem(BaseModel):
    """One item from a YouTube playlist."""

    model_config = {"extra": "ignore"}

    video_id: str
    title: str
    description: str = ""
    channel_id: str = ""
    channel_name: str = ""
    duration_seconds: int | None = None  # filled in by a separate videos.list call
    position: int = 0  # 0-based position in the playlist
    playlist_item_id: str = ""  # YouTube's playlist item resource ID
    # False when YouTube returns a "Deleted video" or "Private video" stub.
    # The video_id is still present in the playlist but the content is gone.
    is_available: bool = True


_ISO8601_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", re.IGNORECASE)


def _parse_iso8601_duration(duration: str) -> int | None:
    """Parse an ISO 8601 duration string (e.g. PT3M45S) into total seconds."""
    m = _ISO8601_DURATION_RE.fullmatch(duration.strip())
    if not m:
        return None
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


class YouTubeClient:
    """
    Thin wrapper around the YouTube Data API v3.

    Constructs the google-api-python-client service once and reuses it for
    all calls within a session.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service = build(
            "youtube",
            "v3",
            developerKey=settings.youtube_api_key,
            cache_discovery=False,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_playlist_items(
        self,
        playlist_id: str,
        *,
        limit: int | None = None,
    ) -> Iterator[PlaylistItem]:
        """
        Yield ``PlaylistItem`` for every video in the playlist.

        Makes paginated calls to ``playlistItems.list`` (max 50 per page),
        then a bulk ``videos.list`` call per page to populate
        ``duration_seconds``.

        Args:
            playlist_id: YouTube playlist ID (starts with PL…).
            limit: stop after this many items (useful for testing).
        """
        yielded = 0
        page_token: str | None = None

        while True:
            resp = (
                self._service.playlistItems()
                .list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=page_token,
                )
                .execute()
            )

            items = resp.get("items", [])
            if not items:
                break

            # Collect video IDs in this page for duration lookup
            video_ids: list[str] = []
            raw_items: list[dict] = []
            for item in items:
                snippet = item.get("snippet", {})
                vid = snippet.get("resourceId", {}).get("videoId", "")
                if not vid:
                    continue
                video_ids.append(vid)
                raw_items.append(item)

            # Bulk-fetch video durations
            durations = self._get_durations(video_ids) if video_ids else {}

            for item in raw_items:
                if limit is not None and yielded >= limit:
                    return

                snippet = item.get("snippet", {})
                vid = snippet.get("resourceId", {}).get("videoId", "")
                title = snippet.get("title", "")

                # YouTube returns stubs for deleted/private videos.  Both have
                # a reserved title and no videoOwnerChannelId.  We still yield
                # them so the pipeline can create a DROPPED_VIDEO task.
                is_available = title not in ("Deleted video", "Private video")

                yield PlaylistItem(
                    video_id=vid,
                    title=title,
                    description=snippet.get("description", ""),
                    channel_id=snippet.get("videoOwnerChannelId", ""),
                    channel_name=snippet.get("videoOwnerChannelTitle", ""),
                    duration_seconds=durations.get(vid),
                    position=snippet.get("position", yielded),
                    playlist_item_id=item.get("id", ""),
                    is_available=is_available,
                )
                yielded += 1

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    def get_video(self, video_id: str) -> PlaylistItem | None:
        """
        Fetch a single video's full metadata as a ``PlaylistItem``.

        Returns ``None`` if the video does not exist or the API call fails.
        """
        try:
            resp = (
                self._service.videos()
                .list(
                    part="snippet,contentDetails",
                    id=video_id,
                )
                .execute()
            )
        except HttpError as exc:
            logger.warning("videos.list failed for %s: %s", video_id, exc)
            return None

        items = resp.get("items", [])
        if not items:
            return None

        item = items[0]
        snippet = item.get("snippet", {})
        title = snippet.get("title", video_id)
        raw_duration = item.get("contentDetails", {}).get("duration", "")

        return PlaylistItem(
            video_id=video_id,
            title=title,
            description=snippet.get("description", ""),
            channel_id=snippet.get("channelId", ""),
            channel_name=snippet.get("channelTitle", ""),
            duration_seconds=_parse_iso8601_duration(raw_duration) if raw_duration else None,
            is_available=title not in ("Deleted video", "Private video"),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_durations(self, video_ids: list[str]) -> dict[str, int | None]:
        """Return {video_id: duration_seconds} for a batch of video IDs."""
        if not video_ids:
            return {}
        # videos.list accepts up to 50 IDs per call
        result: dict[str, int | None] = {}
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            try:
                resp = (
                    self._service.videos()
                    .list(
                        part="contentDetails",
                        id=",".join(batch),
                    )
                    .execute()
                )
                for item in resp.get("items", []):
                    vid = item["id"]
                    raw = item.get("contentDetails", {}).get("duration", "")
                    result[vid] = _parse_iso8601_duration(raw)
            except HttpError as exc:
                logger.warning("videos.list failed for batch: %s", exc)
                for vid in batch:
                    result[vid] = None
        return result
