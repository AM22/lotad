"""Authenticated session for TouhouDB write endpoints.

TouhouDB (a VocaDB instance) protects edit endpoints with two layers:

1. **ASP.NET cookie auth** issued by ``POST /api/users/login``. The server
   replies with a ``Set-Cookie: .AspNetCore.Cookies=…`` header which we have
   to send back on every subsequent request.
2. **Anti-forgery token** fetched from ``GET /api/antiforgery/token``. The
   server sets an ``XSRF-TOKEN`` cookie; we also have to echo the value back
   in a ``requestVerificationToken`` request header on every state-changing
   POST. Header-cookie double-submit is the framework's CSRF defense.

Edit endpoints additionally check the ``Origin`` header against an allow-list
(``[OriginHeaderCheck]`` attribute on the controller), so we send
``Origin: https://touhoudb.com`` on every authenticated call.

The session deliberately does NOT use ``hishel`` caching that the read-side
client relies on — caching auth/identity endpoints would be a footgun, and the
single-user write flow benefits little from it anyway.
"""

from __future__ import annotations

import json
import logging
import time
from http.cookiejar import Cookie
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from lotad.config import Settings

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Login failed or the persisted session has expired."""


class SessionNotLoaded(Exception):
    """Operation requires a logged-in session but none was loaded."""


class TouhouDBSession:
    """Cookie + antiforgery-token holder for authenticated TouhouDB calls.

    Lifecycle::

        async with TouhouDBSession.from_settings(settings) as session:
            await session.login(username, password)
            await session.fetch_antiforgery_token()
            await session.verify()              # confirms cookie is valid
            session.save(settings.touhoudb_session_path)

        # Later, in another process:
        async with TouhouDBSession.from_settings(settings) as session:
            session.load(settings.touhoudb_session_path)
            await session.verify()
            # …pass `session` to TouhouDBWriter…
    """

    _LOGIN_PATH = "/users/login"
    _ANTIFORGERY_PATH = "/antiforgery/token"
    _CURRENT_USER_PATH = "/users/current"

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float,
        user_agent: str,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._origin = self._derive_origin(base_url)
        self._timeout = timeout
        self._user_agent = user_agent
        self._http: httpx.AsyncClient | None = None
        self._xsrf_token: str | None = None
        self._username: str | None = None

    @classmethod
    def from_settings(cls, settings: Settings) -> TouhouDBSession:
        return cls(
            base_url=settings.touhoudb_base_url,
            timeout=settings.touhoudb_request_timeout,
            user_agent=settings.touhoudb_edit_user_agent,
        )

    @staticmethod
    def _derive_origin(base_url: str) -> str:
        # base_url is e.g. "https://touhoudb.com/api"; the Origin header must be
        # the bare scheme+host ("https://touhoudb.com"). The OriginHeaderCheck
        # attribute on edit endpoints rejects requests without a matching origin.
        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid base_url for session: {base_url!r}")
        return f"{parsed.scheme}://{parsed.netloc}"

    async def __aenter__(self) -> TouhouDBSession:
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "application/json",
                "Origin": self._origin,
            },
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None:
            raise SessionNotLoaded("Session must be used as an async context manager")
        return self._http

    @property
    def username(self) -> str | None:
        return self._username

    def auth_headers(self) -> dict[str, str]:
        """Headers required for state-changing requests (POST/PUT/DELETE)."""
        if not self._xsrf_token:
            raise SessionNotLoaded(
                "Anti-forgery token missing — call fetch_antiforgery_token() or load() first"
            )
        return {"requestVerificationToken": self._xsrf_token}

    async def login(self, username: str, password: str, *, keep_logged_in: bool = True) -> None:
        """POST /api/users/login. Cookie jar gains the auth cookie on success."""
        body = {
            "userName": username,
            "password": password,
            "keepLoggedIn": keep_logged_in,
        }
        response = await self.http.post(self._LOGIN_PATH, json=body)
        if response.status_code != 200:
            # Avoid logging the password; surface the API's terse error if any.
            raise AuthenticationError(
                f"Login failed for {username!r}: HTTP {response.status_code} "
                f"{response.text[:200]!r}"
            )
        self._username = username

    async def fetch_antiforgery_token(self) -> None:
        """GET /api/antiforgery/token. Stores the XSRF-TOKEN cookie value."""
        response = await self.http.get(self._ANTIFORGERY_PATH)
        # The endpoint returns 204 No Content; the token rides on the
        # Set-Cookie header. httpx merges it into self.http.cookies for us.
        if response.status_code not in (200, 204):
            raise AuthenticationError(
                f"Failed to fetch antiforgery token: HTTP {response.status_code}"
            )
        token = self.http.cookies.get("XSRF-TOKEN")
        if not token:
            raise AuthenticationError(
                "Antiforgery endpoint did not set XSRF-TOKEN cookie; "
                "is the base URL correct and reachable?"
            )
        self._xsrf_token = token

    async def verify(self) -> str:
        """GET /api/users/current. Returns the verified username; raises on 401."""
        response = await self.http.get(self._CURRENT_USER_PATH)
        if response.status_code == 401:
            raise AuthenticationError("Session is not authenticated (HTTP 401)")
        response.raise_for_status()
        data = response.json()
        name = data.get("name") or data.get("Name")
        if not name:
            raise AuthenticationError(f"Unexpected /users/current response shape: {data!r}")
        self._username = name
        return name

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str | Path) -> None:
        """Write the cookie jar + antiforgery token to ``path`` as JSON.

        The file contains an authentication cookie — treat it as a secret. We
        deliberately do not chmod here (no-op on Windows anyway); the user
        controls the cache directory's permissions.
        """
        cookies: list[dict[str, Any]] = []
        for cookie in self.http.cookies.jar:
            cookies.append(
                {
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path,
                    "expires": cookie.expires,
                    "secure": cookie.secure,
                }
            )
        payload = {
            "cookies": cookies,
            "xsrf_token": self._xsrf_token,
            "username": self._username,
            "saved_at": int(time.time()),
        }
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.debug("Wrote TouhouDB session to %s", path)

    def load(self, path: str | Path) -> None:
        """Restore cookie jar + antiforgery token from a file written by ``save()``."""
        path = Path(path)
        if not path.exists():
            raise SessionNotLoaded(f"No saved session at {path}. Run `lotad contrib login` first.")
        payload = json.loads(path.read_text(encoding="utf-8"))
        for entry in payload.get("cookies", []):
            self.http.cookies.jar.set_cookie(_make_cookie(entry))
        self._xsrf_token = payload.get("xsrf_token")
        self._username = payload.get("username")
        if not self._xsrf_token:
            raise SessionNotLoaded(
                f"Session file {path} missing antiforgery token; re-run `lotad contrib login`."
            )


def _make_cookie(entry: dict[str, Any]) -> Cookie:
    """Reconstruct a stdlib Cookie from a save() dict."""
    return Cookie(
        version=0,
        name=entry["name"],
        value=entry["value"],
        port=None,
        port_specified=False,
        domain=entry["domain"],
        domain_specified=bool(entry["domain"]),
        domain_initial_dot=entry["domain"].startswith("."),
        path=entry["path"],
        path_specified=bool(entry["path"]),
        secure=bool(entry.get("secure", False)),
        expires=entry.get("expires"),
        discard=False,
        comment=None,
        comment_url=None,
        rest={},
        rfc2109=False,
    )
