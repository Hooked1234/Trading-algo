from datetime import date

import httpx
import pytest

from event_trader.providers.sec import AsyncRateLimiter, SecProviderConfig
from event_trader.providers.sec_daily import SecDailyIndexProvider, daily_master_index_url


def test_daily_master_index_url_uses_calendar_quarter() -> None:
    assert daily_master_index_url(date(2026, 8, 25)).endswith(
        "/2026/QTR3/master.20260825.idx"
    )


@pytest.mark.asyncio
async def test_daily_index_returns_only_exact_8k_forms_for_date() -> None:
    payload = b"""Description: Master Index of EDGAR Dissemination Feed
CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2026-08-25|edgar/data/320193/0000320193-26-000018.txt
320193|Apple Inc.|8-K/A|2026-08-25|edgar/data/320193/0000320193-26-000019.txt
320193|Apple Inc.|10-Q|2026-08-25|edgar/data/320193/0000320193-26-000020.txt
320193|Apple Inc.|8-K|2026-08-24|edgar/data/320193/0000320193-26-000021.txt
"""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["User-Agent"] == "event-trader test@example.com"
        return httpx.Response(200, content=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = SecDailyIndexProvider(
            SecProviderConfig(
                user_agent="event-trader test@example.com",
                max_retries=0,
            ),
            client=client,
            limiter=AsyncRateLimiter(2),
        )
        values = await provider.accessions(date(2026, 8, 25))

    assert values == ("0000320193-26-000018", "0000320193-26-000019")
