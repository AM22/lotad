"""Unit tests for TouhouDBWriter — respx-mocked, no network."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from lotad.contrib.touhoudb_session import TouhouDBSession
from lotad.contrib.touhoudb_writer import PVAlreadyPresent, TouhouDBWriter

BASE = "https://touhoudb.test/api"


def _session() -> TouhouDBSession:
    return TouhouDBSession(
        base_url=BASE,
        timeout=5.0,
        user_agent="lotad-test/0.0",
    )


def _empty_contract(song_id: int) -> dict:
    """Minimal contract shape — only the fields we touch."""
    return {
        "id": song_id,
        "name": "Test Song",
        "pvs": [],
        "artists": [],
    }


@respx.mock
async def test_get_song_for_edit_returns_raw_dict():
    respx.get(f"{BASE}/songs/42/for-edit").mock(
        return_value=httpx.Response(200, json=_empty_contract(42))
    )
    async with _session() as s:
        contract = await TouhouDBWriter(s).get_song_for_edit(42)
    assert contract["id"] == 42
    assert contract["pvs"] == []


@respx.mock
async def test_add_youtube_pv_appends_pv_and_submits_as_draft():
    respx.get(f"{BASE}/songs/42/for-edit").mock(
        return_value=httpx.Response(200, json=_empty_contract(42))
    )
    respx.get(f"{BASE}/antiforgery/token").mock(
        return_value=httpx.Response(204, headers={"set-cookie": "XSRF-TOKEN=tok-1; Path=/"})
    )
    edit_route = respx.post(f"{BASE}/songs/42").mock(return_value=httpx.Response(200, text="42"))

    async with _session() as s:
        await s.fetch_antiforgery_token()
        result = await TouhouDBWriter(s).add_youtube_pv(
            song_id=42,
            video_id="abcdefghijk",
            video_url="https://youtu.be/abcdefghijk",
            channel_name="Test Channel",
            video_title="My Test Upload",
            video_length_seconds=235,
            pv_type="Reprint",
            update_notes="Added via LOTAD test",
        )

    assert result == 42

    # Inspect what was actually posted.
    request = edit_route.calls.last.request
    assert request.headers["requestverificationtoken"] == "tok-1"
    assert request.headers["content-type"].startswith("multipart/form-data")

    # Pull the 'contract' field out of the multipart body.
    body = request.content.decode("utf-8")
    payload_marker = "\r\n\r\n"
    # The multipart body has one part: name="contract"
    # Find the JSON between the marker and the trailing boundary.
    json_start = body.index(payload_marker) + len(payload_marker)
    json_end = body.rindex("\r\n--")
    contract = json.loads(body[json_start:json_end])

    assert contract["status"] == "Draft"
    assert contract["updateNotes"] == "Added via LOTAD test"
    assert len(contract["pvs"]) == 1
    pv = contract["pvs"][0]
    assert pv["service"] == "Youtube"
    assert pv["pvType"] == "Reprint"
    assert pv["pvId"] == "abcdefghijk"
    assert pv["url"] == "https://youtu.be/abcdefghijk"
    assert pv["author"] == "Test Channel"
    assert pv["name"] == "My Test Upload"
    assert pv["length"] == 235


@respx.mock
async def test_add_youtube_pv_raises_when_already_present():
    contract_with_pv = _empty_contract(42)
    contract_with_pv["pvs"] = [{"service": "Youtube", "pvId": "existing123", "pvType": "Original"}]
    respx.get(f"{BASE}/songs/42/for-edit").mock(
        return_value=httpx.Response(200, json=contract_with_pv)
    )
    edit_route = respx.post(f"{BASE}/songs/42")

    async with _session() as s:
        # No antiforgery fetch needed — we should never get to the POST.
        with pytest.raises(PVAlreadyPresent) as exc_info:
            await TouhouDBWriter(s).add_youtube_pv(
                song_id=42,
                video_id="abcdefghijk",
                video_url="https://youtu.be/abcdefghijk",
                channel_name=None,
                video_title=None,
                video_length_seconds=None,
                pv_type="Reprint",
                update_notes="should not submit",
            )

    assert exc_info.value.song_id == 42
    assert "existing123" in exc_info.value.existing_video_ids
    assert edit_route.call_count == 0


@respx.mock
async def test_add_youtube_pv_ignores_non_youtube_pvs_when_checking():
    """Songs with NicoNico or Bilibili PVs but no YouTube should still be eligible."""
    contract = _empty_contract(42)
    contract["pvs"] = [{"service": "NicoNicoDouga", "pvId": "sm12345", "pvType": "Original"}]
    respx.get(f"{BASE}/songs/42/for-edit").mock(return_value=httpx.Response(200, json=contract))
    respx.get(f"{BASE}/antiforgery/token").mock(
        return_value=httpx.Response(204, headers={"set-cookie": "XSRF-TOKEN=tok; Path=/"})
    )
    respx.post(f"{BASE}/songs/42").mock(return_value=httpx.Response(200, text="42"))

    async with _session() as s:
        await s.fetch_antiforgery_token()
        result = await TouhouDBWriter(s).add_youtube_pv(
            song_id=42,
            video_id="newvideo123",
            video_url="https://youtu.be/newvideo123",
            channel_name="ch",
            video_title="t",
            video_length_seconds=120,
            pv_type="Reprint",
            update_notes="ok",
        )
    assert result == 42


@respx.mock
async def test_submit_song_edit_propagates_http_error():
    respx.get(f"{BASE}/antiforgery/token").mock(
        return_value=httpx.Response(204, headers={"set-cookie": "XSRF-TOKEN=tok; Path=/"})
    )
    respx.post(f"{BASE}/songs/42").mock(return_value=httpx.Response(403, text="forbidden"))

    async with _session() as s:
        await s.fetch_antiforgery_token()
        with pytest.raises(httpx.HTTPStatusError):
            await TouhouDBWriter(s).submit_song_edit(
                {"id": 42, "name": "t", "pvs": []},
                update_notes="fails",
            )
