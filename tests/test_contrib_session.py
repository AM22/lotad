"""Unit tests for TouhouDBSession — respx-mocked, no network."""

from __future__ import annotations

import httpx
import pytest
import respx

from lotad.contrib.touhoudb_session import (
    AuthenticationError,
    SessionNotLoaded,
    TouhouDBSession,
)

BASE = "https://touhoudb.test/api"


def _session() -> TouhouDBSession:
    return TouhouDBSession(
        base_url=BASE,
        timeout=5.0,
        user_agent="lotad-test/0.0",
    )


@respx.mock
async def test_login_success_sets_username_and_cookie():
    respx.post(f"{BASE}/users/login").mock(
        return_value=httpx.Response(
            200,
            json={"name": "test_user"},
            headers={"set-cookie": ".AspNetCore.Cookies=abc123; Path=/"},
        )
    )

    async with _session() as s:
        await s.login("test_user", "hunter2")

    assert s.username == "test_user"


@respx.mock
async def test_login_failure_raises_authentication_error():
    respx.post(f"{BASE}/users/login").mock(return_value=httpx.Response(401, text="bad credentials"))

    async with _session() as s:
        with pytest.raises(AuthenticationError, match="HTTP 401"):
            await s.login("test_user", "wrong")


@respx.mock
async def test_fetch_antiforgery_stores_token_from_cookie():
    respx.get(f"{BASE}/antiforgery/token").mock(
        return_value=httpx.Response(204, headers={"set-cookie": "XSRF-TOKEN=tok-xyz; Path=/"})
    )

    async with _session() as s:
        await s.fetch_antiforgery_token()
        assert s.auth_headers() == {"requestVerificationToken": "tok-xyz"}


@respx.mock
async def test_fetch_antiforgery_missing_cookie_raises():
    respx.get(f"{BASE}/antiforgery/token").mock(return_value=httpx.Response(204))

    async with _session() as s:
        with pytest.raises(AuthenticationError, match="XSRF-TOKEN"):
            await s.fetch_antiforgery_token()


@respx.mock
async def test_verify_returns_username():
    respx.get(f"{BASE}/users/current").mock(
        return_value=httpx.Response(200, json={"name": "test_user"})
    )

    async with _session() as s:
        name = await s.verify()
        assert name == "test_user"
        assert s.username == "test_user"


@respx.mock
async def test_verify_401_raises_authentication_error():
    respx.get(f"{BASE}/users/current").mock(return_value=httpx.Response(401))

    async with _session() as s:
        with pytest.raises(AuthenticationError, match="not authenticated"):
            await s.verify()


def test_auth_headers_without_token_raises():
    s = _session()
    # No __aenter__ here is fine — auth_headers() only inspects in-memory state.
    with pytest.raises(SessionNotLoaded):
        s.auth_headers()


@respx.mock
async def test_origin_header_sent_on_requests():
    route = respx.get(f"{BASE}/users/current").mock(
        return_value=httpx.Response(200, json={"name": "x"})
    )

    async with _session() as s:
        await s.verify()

    assert route.calls.last.request.headers["origin"] == "https://touhoudb.test"


@respx.mock
async def test_save_and_load_round_trip(tmp_path):
    # Set up session 1: log in, get antiforgery token, save.
    respx.post(f"{BASE}/users/login").mock(
        return_value=httpx.Response(
            200,
            json={"name": "test_user"},
            headers={"set-cookie": ".AspNetCore.Cookies=cookie-A; Path=/; Domain=touhoudb.test"},
        )
    )
    respx.get(f"{BASE}/antiforgery/token").mock(
        return_value=httpx.Response(
            204,
            headers={"set-cookie": "XSRF-TOKEN=tok-saved; Path=/; Domain=touhoudb.test"},
        )
    )

    path = tmp_path / "session.json"
    async with _session() as s:
        await s.login("test_user", "hunter2")
        await s.fetch_antiforgery_token()
        s.save(path)

    assert path.exists()

    # Set up session 2: load from disk and verify the loaded cookie is sent back.
    respx.get(f"{BASE}/users/current").mock(
        return_value=httpx.Response(200, json={"name": "test_user"})
    )
    async with _session() as s2:
        s2.load(path)
        # auth_headers proves the antiforgery token survived the round trip
        assert s2.auth_headers() == {"requestVerificationToken": "tok-saved"}
        assert s2.username == "test_user"
        # And the auth cookie was reattached to the jar
        await s2.verify()
        sent_cookie = respx.calls.last.request.headers.get("cookie", "")
        assert ".AspNetCore.Cookies=cookie-A" in sent_cookie


def test_load_missing_file_raises(tmp_path):
    s = _session()
    with pytest.raises(SessionNotLoaded, match="No saved session"):
        s.load(tmp_path / "does-not-exist.json")
