"""HTTP client foundation: RFC-7234 caching, exponential-backoff retry, circuit breaker.

Used by TouhouDBClient (and in future, any outbound HTTP call).
"""

from __future__ import annotations

import logging

import hishel
import httpx

logger = logging.getLogger(__name__)


class CircuitBreakerOpen(Exception):
    """Raised when the circuit breaker is open; no network call is made."""


class CircuitBreaker:
    """
    Counts consecutive final-failure events (i.e. after all retries exhausted).
    Once ``threshold`` consecutive failures are seen, ``is_open`` flips to True
    and every subsequent call raises ``CircuitBreakerOpen`` immediately without
    touching the network.  Resets to closed on a successful call or ``reset()``.
    """

    def __init__(self, threshold: int = 10) -> None:
        self._threshold = threshold
        self._consecutive_failures = 0
        self.is_open = False

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self.is_open = False

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold:
            self.is_open = True
            logger.warning(
                "Circuit breaker OPENED after %d consecutive final failures",
                self._consecutive_failures,
            )

    def reset(self) -> None:
        """Manually close the breaker (e.g. after an operator inspection)."""
        self._consecutive_failures = 0
        self.is_open = False
        logger.info("Circuit breaker manually reset")


def is_retryable(exc: BaseException) -> bool:
    """Return True if the exception should trigger a retry attempt."""
    if isinstance(exc, httpx.HTTPStatusError):
        # Retry on 429 Too Many Requests or any 5xx server error.
        # All other 4xx are not retryable (bad request, not found, etc.).
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    # Network errors, timeouts, and protocol errors are retryable.
    return isinstance(
        exc,
        (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError),
    )


def build_async_client(
    base_url: str,
    timeout: float = 30.0,
    cache_dir: str = ".cache/http",
) -> hishel.AsyncCacheClient:
    """
    Build an httpx-compatible async client with RFC-7234 disk-backed caching.

    The returned client is an async context manager; callers should use it with
    ``async with build_async_client(...) as client:``.
    """
    storage = hishel.AsyncFileStorage(base_path=cache_dir)
    controller = hishel.Controller(
        cacheable_methods=["GET"],
        cacheable_status_codes=[200, 203, 204, 206, 300, 301, 308],
        allow_stale=False,
        force_cache=False,
    )
    # max_keepalive_connections=0: disable connection pooling entirely.
    # TouhouDB enrich batches interleave ~3-10s Claude API calls between each
    # TouhouDB request, causing connections to sit idle long enough for the
    # server to close them.  When a timeout fires mid-request, httpcore can
    # also leave a connection in a bad state that poisons subsequent requests
    # from the pool.  With pooling disabled, every request gets a fresh TCP
    # connection — the handshake overhead (<50ms) is negligible at our request
    # rate of ~1 req/10s, and it eliminates the entire class of stale-socket
    # and bad-state-pool ReadTimeout failures we were observing.
    limits = httpx.Limits(
        max_keepalive_connections=0,
        max_connections=10,
    )
    return hishel.AsyncCacheClient(
        storage=storage,
        controller=controller,
        base_url=base_url,
        headers={
            "User-Agent": ("LOTAD/0.1 (personal Touhou arrangement database; contact via GitHub)"),
            "Accept": "application/json",
        },
        timeout=timeout,
        limits=limits,
    )
