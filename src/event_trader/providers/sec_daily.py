"""Official EDGAR daily-index access for feed completeness checks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Protocol

import httpx

from ..backfill import parse_sec_quarter_index
from .sec import AsyncRateLimiter, SecProviderConfig, SecProviderError

SEC_DAILY_INDEX_ROOT = "https://www.sec.gov/Archives/edgar/daily-index"
_RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class DailyIndexSource(Protocol):
    async def accessions(self, session_date: date) -> tuple[str, ...]: ...


class SecDailyIndexProvider:
    """Fetch one official daily master index under the shared SEC rate limit."""

    def __init__(
        self,
        config: SecProviderConfig,
        *,
        client: httpx.AsyncClient | None = None,
        limiter: AsyncRateLimiter | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout_seconds),
            follow_redirects=True,
        )
        self._limiter = limiter or AsyncRateLimiter(config.requests_per_second)
        self._sleep = sleep

    async def __aenter__(self) -> SecDailyIndexProvider:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def accessions(self, session_date: date) -> tuple[str, ...]:
        url = daily_master_index_url(session_date)
        payload = await self._fetch(url)
        entries = parse_sec_quarter_index(payload, kind="master")
        return tuple(
            sorted(entry.accession_number for entry in entries if entry.filed_on == session_date)
        )

    async def _fetch(self, url: str) -> bytes:
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/plain, */*;q=0.1",
        }
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.get(url, headers=headers)
                if response.status_code in _RETRYABLE_STATUS_CODES:
                    if attempt >= self.config.max_retries:
                        response.raise_for_status()
                    await self._sleep(
                        min(
                            self.config.backoff_base_seconds * (2**attempt),
                            self.config.backoff_max_seconds,
                        )
                    )
                    continue
                response.raise_for_status()
                if len(response.content) > self.config.max_index_bytes:
                    raise SecProviderError("SEC daily index exceeds configured size limit")
                return response.content
            except httpx.TransportError as exc:
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                await self._sleep(
                    min(
                        self.config.backoff_base_seconds * (2**attempt),
                        self.config.backoff_max_seconds,
                    )
                )
        raise SecProviderError(f"SEC daily index request failed: {url}") from last_error


def daily_master_index_url(session_date: date) -> str:
    quarter = (session_date.month - 1) // 3 + 1
    stamp = session_date.strftime("%Y%m%d")
    return f"{SEC_DAILY_INDEX_ROOT}/{session_date.year}/QTR{quarter}/master.{stamp}.idx"


__all__ = ["DailyIndexSource", "SecDailyIndexProvider", "daily_master_index_url"]
