from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from event_trader.domain import (
    DataSource,
    Direction,
    DocumentRef,
    EventSnapshot,
    EvidenceSpan,
    FilingEvent,
    InsightStatus,
    MarketSnapshot,
    Materiality,
    NewsInsight,
    PortfolioState,
    Quote,
)

UTC = UTC


@pytest.fixture
def decision_time() -> datetime:
    # 2026-08-25 09:45 America/New_York.
    return datetime(2026, 8, 25, 13, 45, tzinfo=UTC)


@pytest.fixture
def filing(decision_time: datetime) -> FilingEvent:
    digest = "a" * 64
    return FilingEvent(
        event_id="0000320193-26-000018",
        accession_number="0000320193-26-000018",
        cik="320193",
        form="8-K",
        items=("2.02", "9.01"),
        symbols=("AAPL",),
        accepted_at=decision_time - timedelta(minutes=10),
        first_seen_at=decision_time - timedelta(minutes=5),
        retrieved_at=decision_time - timedelta(minutes=4, seconds=50),
        documents=(
            DocumentRef(
                url="https://www.sec.gov/Archives/example.htm",
                kind="EX-99.1",
                sha256=digest,
            ),
        ),
    )


@pytest.fixture
def long_market(decision_time: datetime) -> MarketSnapshot:
    quote = Quote(
        symbol="AAPL",
        timestamp=decision_time,
        bid=Decimal("100.00"),
        ask=Decimal("100.10"),
        bid_size=500,
        ask_size=500,
        source=DataSource.REPLAY,
        feed="test-sip",
    )
    return MarketSnapshot(
        symbol="AAPL",
        as_of=decision_time,
        quote=quote,
        last=Decimal("101.00"),
        session_vwap=Decimal("100.00"),
        median_dollar_volume_20d=Decimal("100000000"),
        beta_adjusted_return_z=2.0,
        relative_volume=2.5,
        atr_5m=Decimal("1.00"),
        data_fresh=True,
        market_data_live=False,
        halted=False,
        shortable=True,
        shortable_shares=10_000,
        security_type="common_stock",
        primary_exchange="NASDAQ",
        us_listed=True,
    )


@pytest.fixture
def snapshot(filing: FilingEvent, long_market: MarketSnapshot) -> EventSnapshot:
    return EventSnapshot(
        filing=filing,
        market=long_market,
        document_text="Item 2.02. Results exceeded prior guidance.",
    )


@pytest.fixture
def long_insight(filing: FilingEvent) -> NewsInsight:
    return NewsInsight(
        event_id=filing.event_id,
        accession_number=filing.accession_number,
        status=InsightStatus.ACTIONABLE,
        category="earnings",
        direction=Direction.LONG,
        materiality=Materiality.HIGH,
        confidence=0.90,
        evidence=(
            EvidenceSpan(
                document_sha256=filing.documents[0].sha256,
                excerpt="Results exceeded prior guidance.",
            ),
        ),
        model_provider="fixture",
        model_name="fixture-v1",
        prompt_version="1",
    )


@pytest.fixture
def empty_portfolio(decision_time: datetime) -> PortfolioState:
    return PortfolioState(
        as_of=decision_time,
        nav=Decimal("100000"),
        peak_nav=Decimal("100000"),
        cash=Decimal("100000"),
        strategy_equity=Decimal("100000"),
        strategy_peak_equity=Decimal("100000"),
        strategy_realized_pnl_today=Decimal("0"),
        strategy_unrealized_pnl=Decimal("0"),
    )
