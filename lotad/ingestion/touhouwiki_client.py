"""Async client for the en.touhouwiki.net MediaWiki API.

Used by ``lotad originals enrich`` to backfill ``stage`` / ``is_boss`` /
character mappings on ``original_songs``.  Mirrors the patterns in
``touhoudb_client.py``: hishel-cached httpx, tenacity retries, circuit breaker.

Two endpoints are exposed:

* :meth:`get_song_listing` — parses ``List_by_Song/<game>`` into themed
  sections with their wikitext bodies, ready for the heading-and-bold-title
  parsing in :mod:`lotad.ingestion.touhouwiki_parser`.
* :meth:`get_spell_card_owners` — fetches one stage's spell-card page and
  returns the unique set of card owners (used to assign candidate characters
  to non-boss stage themes).

The wiki cache is long-lived (content rarely changes) and the HTTP client's
disk cache lives at ``.cache/http/wiki`` so an enrich re-run is almost free.
"""

from __future__ import annotations

import logging
from typing import Any

from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential

from lotad.config import Settings
from lotad.ingestion.http_client import (
    CircuitBreaker,
    CircuitBreakerOpen,
    build_async_client,
    is_retryable,
)
from lotad.ingestion.touhouwiki_parser import split_sections_by_heading

logger = logging.getLogger(__name__)

_WIKI_API_BASE = "https://en.touhouwiki.net"
_API_PATH = "/w/api.php"


class TouhouWikiClient:
    """Thin async wrapper around the MediaWiki ``action=parse`` endpoint."""

    def __init__(
        self,
        settings: Settings,
        *,
        cache_dir: str = ".cache/http/wiki",
    ) -> None:
        self._settings = settings
        self._cache_dir = cache_dir
        self._circuit_breaker = CircuitBreaker(settings.touhoudb_circuit_breaker_threshold)
        self._http: Any = None  # hishel.AsyncCacheClient, set in __aenter__

    @classmethod
    def from_settings(cls, settings: Settings, **kwargs: Any) -> TouhouWikiClient:
        return cls(settings, **kwargs)

    async def __aenter__(self) -> TouhouWikiClient:
        self._http = build_async_client(
            base_url=_WIKI_API_BASE,
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
    # Internal
    # ------------------------------------------------------------------

    async def _get(self, **params: Any) -> Any:
        if self._circuit_breaker.is_open:
            raise CircuitBreakerOpen("Touhou Wiki circuit breaker is open; skipping network call")

        max_attempts = self._settings.touhoudb_max_retries
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=1, min=1, max=30),
                retry=retry_if_exception(is_retryable),
                reraise=True,
            ):
                with attempt:
                    n = attempt.retry_state.attempt_number
                    if n > 1:
                        logger.warning(
                            "TOUHOUWIKI: retry %d/%d for params=%r", n, max_attempts, params
                        )
                    else:
                        logger.debug("TOUHOUWIKI: GET params=%r", params)
                    response = await self._http.get(_API_PATH, params=params)
                    response.raise_for_status()
                    self._circuit_breaker.record_success()
                    return response.json()
        except Exception:
            self._circuit_breaker.record_failure()
            raise

    async def _fetch_wikitext(self, page_title: str) -> str | None:
        """Fetch the raw wikitext for a page, or None if it doesn't exist."""
        data = await self._get(
            action="parse",
            page=page_title,
            prop="wikitext",
            format="json",
            formatversion="2",
            redirects="1",
        )
        if isinstance(data, dict) and "error" in data:
            code = data["error"].get("code", "")
            # missingtitle / nosuchsection: legitimate "page not found"
            if code in {"missingtitle", "nosuchpage", "nosuchsection"}:
                logger.info("TOUHOUWIKI: page %r not found", page_title)
                return None
            logger.warning("TOUHOUWIKI: API error for %r: %r", page_title, data["error"])
            return None

        parse = data.get("parse") if isinstance(data, dict) else None
        if not parse:
            return None
        wikitext = parse.get("wikitext")
        if isinstance(wikitext, dict):
            # formatversion=1 wraps the text in {"*": "..."}; defend for both.
            wikitext = wikitext.get("*")
        if not isinstance(wikitext, str):
            return None
        return wikitext

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_song_listing(self, game_slug: str) -> dict[str, str] | None:
        """Fetch ``List_by_Song/<game_slug>`` and split into ``{heading: wikitext}``.

        Returns ``None`` if the page is missing.  Section keys are the raw
        heading text (e.g. ``"Stage 5 boss - Sakuya Izayoi's theme"``);
        callers feed each key through ``parse_section_heading``.
        """
        page = f"List_by_Song/{game_slug}"
        wikitext = await self._fetch_wikitext(page)
        if wikitext is None:
            return None
        return split_sections_by_heading(wikitext)

    async def get_spell_card_owners(self, game_slug: str, stage: int) -> list[str] | None:
        """Fetch ``<game>/Spell_Cards/<Stage>`` and return owner names in order.

        ``stage`` is the integer stage number from the heading parse:
        1–6 → ``Stage_<N>``; 7 → ``Extra``.  Phantasm pages are tried as
        a fallback when the caller knows the game has one.

        Returns ``None`` if no page was found at any of the candidate URLs;
        an empty list means the page exists but contains no spell cards.
        """
        from lotad.ingestion.touhouwiki_parser import parse_spell_card_owners

        candidates: list[str] = []
        if 1 <= stage <= 6:
            candidates.append(f"{game_slug}/Spell_Cards/Stage_{stage}")
        elif stage == 7:
            # Extra is by far the more common Stage-7 variant; Phantasm only
            # exists for PCB.  Both are tried; the first match wins.
            candidates.append(f"{game_slug}/Spell_Cards/Extra")
            candidates.append(f"{game_slug}/Spell_Cards/Phantasm")
        else:
            return None

        for page in candidates:
            wikitext = await self._fetch_wikitext(page)
            if wikitext is not None:
                return parse_spell_card_owners(wikitext)
        return None
