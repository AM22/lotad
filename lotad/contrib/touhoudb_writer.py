"""Submit edits to TouhouDB on behalf of the authenticated session.

Two-step round-trip per song:

1. ``GET /api/songs/{id}/for-edit`` returns the editable contract — a JSON
   blob that mirrors ``SongForEditForApiContract``. We treat the payload as
   an opaque dict so unknown/forward-compatible fields survive the round
   trip untouched.
2. ``POST /api/songs/{id}`` accepts the mutated contract back as
   ``multipart/form-data`` with a single ``contract`` field whose value is
   ``JSON.stringify(contract)``. Setting ``Status: "Draft"`` parks the edit
   in the moderation queue so the operator can review on touhoudb.com
   before it goes live.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from lotad.contrib.touhoudb_session import TouhouDBSession

logger = logging.getLogger(__name__)

# Mirrors VocaDb.Model.Domain.EntryStatus (Draft=0, Finished=1, Approved=2, Locked=4).
# We always submit as Draft so a human reviews each edit on touhoudb.com.
_STATUS_DRAFT = "Draft"

# Mirrors VocaDb.Model.Domain.PVs.PVService — we only ever push YouTube PVs from LOTAD.
_PV_SERVICE_YOUTUBE = "Youtube"


class PVAlreadyPresent(Exception):
    """The TouhouDB song already has a YouTube PV; nothing to add."""

    def __init__(self, song_id: int, existing_video_ids: list[str]) -> None:
        super().__init__(f"Song {song_id} already has YouTube PV(s): {existing_video_ids!r}")
        self.song_id = song_id
        self.existing_video_ids = existing_video_ids


class TouhouDBWriter:
    """Edit operations for songs (and, eventually, albums/artists/lyrics).

    Construct with an already-authenticated ``TouhouDBSession``. Each method
    does the minimal round trip; callers compose them into bulk workflows.
    """

    def __init__(self, session: TouhouDBSession) -> None:
        self._session = session

    async def get_song_for_edit(self, song_id: int) -> dict[str, Any]:
        """Fetch the editable contract for a song. Round-trippable as-is."""
        response = await self._session.http.get(f"/songs/{song_id}/for-edit")
        response.raise_for_status()
        contract: dict[str, Any] = response.json()
        return contract

    async def submit_song_edit(
        self,
        contract: dict[str, Any],
        *,
        update_notes: str,
    ) -> int:
        """POST a (mutated) song contract back as a Draft edit. Returns song id."""
        contract = dict(contract)  # don't mutate caller's dict
        contract["status"] = _STATUS_DRAFT
        contract["updateNotes"] = update_notes

        song_id = contract.get("id")
        if not isinstance(song_id, int):
            raise ValueError(f"Contract missing valid integer 'id': {song_id!r}")

        # The repo's TS client posts FormData with a single field named 'contract'
        # whose value is the JSON-stringified contract. httpx's `files=` argument
        # gives us multipart/form-data; passing a (filename, content) tuple here
        # avoids it being interpreted as a file upload.
        files = {"contract": (None, json.dumps(contract))}
        try:
            response = await self._session.http.post(
                f"/songs/{song_id}",
                files=files,
                headers=self._session.auth_headers(),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "submit_song_edit failed for song %d: HTTP %d %s",
                song_id,
                exc.response.status_code,
                exc.response.text[:300],
            )
            raise
        # The endpoint returns the (possibly new) song id as a bare JSON int.
        body = response.text.strip()
        return int(body) if body else song_id

    async def add_youtube_pv(
        self,
        *,
        song_id: int,
        video_id: str,
        video_url: str,
        channel_name: str | None,
        video_title: str | None,
        video_length_seconds: int | None,
        pv_type: str,
        update_notes: str,
    ) -> int:
        """Append a YouTube PV to a song and submit the edit as a Draft.

        Raises ``PVAlreadyPresent`` if the freshly fetched contract already
        contains a YouTube PV — caller decides whether to log or skip.
        """
        contract = await self.get_song_for_edit(song_id)
        existing = _existing_youtube_pv_ids(contract)
        if existing:
            raise PVAlreadyPresent(song_id, existing)

        new_pv: dict[str, Any] = {
            "service": _PV_SERVICE_YOUTUBE,
            "pvType": pv_type,
            "pvId": video_id,
            "url": video_url,
            "name": video_title or "",
            "author": channel_name or "",
            "length": video_length_seconds or 0,
            "disabled": False,
            "thumbUrl": "",
        }
        pvs = contract.get("pvs")
        if not isinstance(pvs, list):
            pvs = []
        contract["pvs"] = [*pvs, new_pv]

        return await self.submit_song_edit(contract, update_notes=update_notes)


def _existing_youtube_pv_ids(contract: dict[str, Any]) -> list[str]:
    """Return YouTube PV ids already present on the contract (case-insensitive service match)."""
    out: list[str] = []
    for pv in contract.get("pvs") or []:
        service = str(pv.get("service", "")).lower()
        if service == _PV_SERVICE_YOUTUBE.lower():
            pv_id = pv.get("pvId") or ""
            out.append(pv_id)
    return out
