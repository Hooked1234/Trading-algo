from datetime import UTC, datetime

import pytest

from event_trader.backfill import SecIndexEntry
from event_trader.providers.sec_history import (
    HistoricalSecFilingResolver,
    parse_historical_submission,
)


def test_historical_submission_uses_eastern_acceptance_and_filing_symbol() -> None:
    payload = b"""
<ACCEPTANCE-DATETIME>20240702101530
<ITEMS>2.02
<ITEMS>7.01
<dei:TradingSymbol contextRef="c">AAPL</dei:TradingSymbol>
<ix:nonNumeric name="dei:TradingSymbol" contextRef="c">AAPL</ix:nonNumeric>
"""

    parsed = parse_historical_submission(payload)

    assert parsed.accepted_at == datetime(2024, 7, 2, 14, 15, 30, tzinfo=UTC)
    assert parsed.items == ("2.02", "7.01")
    assert parsed.symbols == ("AAPL",)


@pytest.mark.asyncio
async def test_historical_resolver_builds_and_hydrates_point_in_time_event() -> None:
    entry = SecIndexEntry(
        cik="320193",
        company_name="Example",
        form="8-K",
        filed_on=datetime(2024, 7, 2).date(),
        filename="edgar/data/320193/0000320193-24-000001.txt",
        accession_number="0000320193-24-000001",
        index_kind="master",
    )
    seen = []

    async def fetch(_url: str) -> bytes:
        return (
            b"<ACCEPTANCE-DATETIME>20240702101530\n"
            b"<ITEMS>2.02\n"
            b"<dei:TradingSymbol>AAPL</dei:TradingSymbol>"
        )

    async def hydrate(event):
        seen.append(event)
        return event.model_copy(update={"complete": True})

    event = await HistoricalSecFilingResolver(
        fetch_submission=fetch,
        hydrate_filing=hydrate,
    )(entry)

    assert event.symbols == ("AAPL",)
    assert event.items == ("2.02",)
    assert event.complete
    assert seen == [event.model_copy(update={"complete": False})]
