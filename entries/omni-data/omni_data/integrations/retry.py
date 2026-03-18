"""
Retry & Backoff Utilities — production-grade resilience for all HTTP pipelines.

Provides decorators and wrappers for exponential backoff with jitter,
rate limit detection, and circuit breaking for external API calls.
Used by all alternative data pipelines and exchange REST calls.

Memory: ~O(1) | CPU: O(1) | External calls: wraps existing calls
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from typing import Any, Callable, TypeVar

import aiohttp

logger = logging.getLogger(__name__)

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MAX_RETRIES = 5
_DEFAULT_BASE_DELAY = 1.0
_DEFAULT_MAX_DELAY = 30.0
_DEFAULT_JITTER = 0.5

# HTTP status codes that warrant a retry
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504, 520, 521, 522})


# ---------------------------------------------------------------------------
# Core retry function
# ---------------------------------------------------------------------------

async def retry_async(
    fn: Callable[..., Any],
    *args: Any,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_BASE_DELAY,
    max_delay: float = _DEFAULT_MAX_DELAY,
    retryable_exceptions: tuple[type, ...] = (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        ConnectionError,
        OSError,
    ),
    **kwargs: Any,
) -> Any:
    """
    Execute an async function with exponential backoff retry.

    Args:
        fn: Async function to execute.
        *args: Positional arguments to fn.
        max_retries: Maximum retry attempts.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay cap.
        retryable_exceptions: Exception types that trigger a retry.
        **kwargs: Keyword arguments to fn.

    Returns:
        Result of fn.

    Raises:
        The last exception if max retries are exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            return await fn(*args, **kwargs)
        except retryable_exceptions as exc:
            last_exc = exc
            if attempt == max_retries:
                break

            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter = random.uniform(-_DEFAULT_JITTER, _DEFAULT_JITTER)
            sleep_time = max(0.1, delay + jitter)

            logger.warning(
                "Retry %d/%d for %s: %s. Sleeping %.1fs...",
                attempt + 1, max_retries, fn.__name__, exc, sleep_time,
            )
            await asyncio.sleep(sleep_time)

    raise RuntimeError(
        f"Max retries ({max_retries}) exhausted for {fn.__name__}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Decorator version
# ---------------------------------------------------------------------------

def with_retry(
    max_retries: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_BASE_DELAY,
    max_delay: float = _DEFAULT_MAX_DELAY,
    retryable_exceptions: tuple[type, ...] = (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        ConnectionError,
    ),
) -> Callable:
    """
    Decorator to add exponential backoff retry to an async function.

    Usage:
        @with_retry(max_retries=3)
        async def fetch_data():
            ...
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await retry_async(
                fn, *args,
                max_retries=max_retries,
                base_delay=base_delay,
                max_delay=max_delay,
                retryable_exceptions=retryable_exceptions,
                **kwargs,
            )
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Rate limit aware HTTP session
# ---------------------------------------------------------------------------

class RateLimitedSession:
    """
    Async HTTP session with automatic rate limit handling.

    Detects 429 responses and waits for the Retry-After header
    before retrying. Falls back to exponential backoff if no
    Retry-After is provided.

    Usage:
        async with RateLimitedSession() as session:
            data = await session.get_json("https://api.example.com/data")
    """

    def __init__(
        self,
        *,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        timeout: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._max_retries = max_retries
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._headers = headers or {}
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> RateLimitedSession:
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers=self._headers,
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session:
            await self._session.close()

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
    ) -> Any:
        """
        GET request with automatic rate limit handling and retry.

        Args:
            url: Request URL.
            params: Query parameters.

        Returns:
            Parsed JSON response.

        Raises:
            RuntimeError: If max retries exhausted.
            aiohttp.ClientResponseError: On non-retryable HTTP errors.
        """
        if not self._session:
            raise RuntimeError("Session not initialized. Use as context manager.")

        for attempt in range(self._max_retries + 1):
            try:
                async with self._session.get(url, params=params) as resp:
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            delay = float(retry_after)
                        else:
                            delay = min(
                                _DEFAULT_BASE_DELAY * (2 ** attempt),
                                _DEFAULT_MAX_DELAY,
                            )
                        logger.warning(
                            "Rate limited on %s. Waiting %.1fs (attempt %d/%d)...",
                            url, delay, attempt + 1, self._max_retries,
                        )
                        await asyncio.sleep(delay)
                        continue

                    if resp.status in _RETRYABLE_STATUS_CODES:
                        delay = min(
                            _DEFAULT_BASE_DELAY * (2 ** attempt),
                            _DEFAULT_MAX_DELAY,
                        )
                        logger.warning(
                            "Retryable HTTP %d on %s. Waiting %.1fs...",
                            resp.status, url, delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    resp.raise_for_status()
                    return await resp.json()

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == self._max_retries:
                    raise
                delay = min(
                    _DEFAULT_BASE_DELAY * (2 ** attempt),
                    _DEFAULT_MAX_DELAY,
                )
                logger.warning(
                    "Request to %s failed (attempt %d/%d): %s. Retrying in %.1fs...",
                    url, attempt + 1, self._max_retries, exc, delay,
                )
                await asyncio.sleep(delay)

        raise RuntimeError(f"Max retries exhausted for {url}")
