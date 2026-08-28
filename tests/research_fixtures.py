"""Shared point-in-time fixtures for the research-case and artifact tests."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import exchange_calendars as xcals

from event_trader.backfill import (
    CoverageRecord,
    CoverageStatus,
    FeatureHistoryCoverage,
    PointInTimeEligibility,
)
from event_trader.datasets import ParquetMarketDataLake
from event_trader.documents import FilingDocumentLoader
from event_trader.domain import Bar, DataSource, EvidenceSpan, Quote
from event_trader.research_cases import (
    HistoricalResearchCaseBuilder,
    HistoricalTradingState,
    TradingStateManifest,
)

_XNYS = xcals.get_calendar("XNYS")


def trading_state_manifest(*states: HistoricalTradingState) -> TradingStateManifest:
    return TradingStateManifest(
        source="test-halt-and-borrow-manifest",
        entries=tuple(states),
    ).sealed()


def previous_sessions(value: datetime, count: int) -> tuple[object, ...]:
    cursor: object = value.date().isoformat()
    sessions: list[object] = []
    for _ in range(count):
        cursor = _XNYS.previous_session(cursor)
        sessions.append(cursor)
    return tuple(reversed(sessions))


def market_history(
    decision_time: datetime,
) -> tuple[tuple[Bar, ...], tuple[Bar, ...]]:
    sessions = (*previous_sessions(decision_time, 20), decision_time.date().isoformat())
    asset: list[Bar] = []
    benchmark: list[Bar] = []
    for session_index, session in enumerate(sessions):
        opening = _XNYS.session_open(session).to_pydatetime().astimezone(UTC)
        closing = _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
        full_minutes = int((closing - opening).total_seconds() // 60)
        current = session_index == 20
        minute_count = 75 if current else full_minutes
        asset_step = (
            Decimal("0.10")
            if current
            else Decimal("0.002") + Decimal(session_index % 5) * Decimal("0.0004")
        )
        spy_step = (
            Decimal("0.01")
            if current
            else Decimal("0.001") + Decimal(session_index % 4) * Decimal("0.0003")
        )
        volume = 6_000 if current else 600
        for minute in range(1, minute_count + 1):
            timestamp = opening + timedelta(minutes=minute)
            for symbol, base, step, target in (
                ("AAPL", Decimal("100"), asset_step, asset),
                ("SPY", Decimal("400"), spy_step, benchmark),
            ):
                open_price = base + step * Decimal(minute - 1)
                close_price = open_price + step
                target.append(
                    Bar(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=open_price,
                        high=close_price + Decimal("0.02"),
                        low=open_price - Decimal("0.02"),
                        close=close_price,
                        volume=volume,
                        vwap=(open_price + close_price) / Decimal("2"),
                        source=DataSource.ALPACA_SIP,
                        feed="sip",
                    )
                )
    return tuple(asset), tuple(benchmark)


def case_inputs(tmp_path, filing, long_insight, decision_time):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    document_path = raw_root / "exhibit.html"
    document_content = b"<html><body>Results exceeded prior guidance.</body></html>"
    document_path.write_bytes(document_content)
    digest = hashlib.sha256(document_content).hexdigest()
    document = filing.documents[0].model_copy(
        update={"sha256": digest, "local_path": str(document_path)}
    )
    historical_filing = filing.model_copy(update={"documents": (document,)})
    insight = long_insight.model_copy(
        update={
            "evidence": (
                EvidenceSpan(
                    document_sha256=digest,
                    excerpt="Results exceeded prior guidance.",
                ),
            )
        }
    )
    asset, benchmark = market_history(decision_time)
    current = {bar.timestamp: bar for bar in asset}
    quotes = [
        Quote(
            symbol="AAPL",
            timestamp=decision_time,
            bid=current[decision_time].close - Decimal("0.01"),
            ask=current[decision_time].close + Decimal("0.01"),
            bid_size=10_000,
            ask_size=10_000,
            source=DataSource.ALPACA_SIP,
            feed="sip",
        ),
        Quote(
            symbol="AAPL",
            timestamp=decision_time + timedelta(seconds=4),
            bid=current[decision_time].close,
            ask=current[decision_time].close + Decimal("0.02"),
            bid_size=10_000,
            ask_size=10_000,
            source=DataSource.ALPACA_SIP,
            feed="sip",
        ),
    ]
    for minute in range(1, 61):
        timestamp = decision_time + timedelta(minutes=minute)
        midpoint = current[timestamp].close
        quotes.append(
            Quote(
                symbol="AAPL",
                timestamp=timestamp,
                bid=midpoint - Decimal("0.01"),
                ask=midpoint + Decimal("0.01"),
                bid_size=10_000,
                ask_size=10_000,
                source=DataSource.ALPACA_SIP,
                feed="sip",
            )
        )

    lake = ParquetMarketDataLake(tmp_path / "lake")
    lake.write_filings([historical_filing], batch_id="filing")
    lake.write_bars(asset, batch_id="asset")
    lake.write_bars(benchmark, batch_id="benchmark")
    lake.write_quotes(quotes, batch_id="quotes")
    eligibility = PointInTimeEligibility(
        accession_number=historical_filing.accession_number,
        symbol="AAPL",
        as_of=historical_filing.accepted_at,
        source="test-security-master",
        common_stock=True,
        us_listing=True,
        corporate_actions_complete=True,
    )
    coverage = CoverageRecord(
        record_id=f"coverage:{historical_filing.accession_number}:lag-5m",
        quarter="2026-Q3",
        accession_number=historical_filing.accession_number,
        symbol="AAPL",
        scenario="source_lag_5m_primary",
        lag_minutes=5,
        available_at=decision_time - timedelta(minutes=5),
        evaluation_at=decision_time,
        window_end=decision_time + timedelta(minutes=60),
        provider="alpaca",
        feed="sip",
        bundle_start=min(bar.timestamp for bar in asset),
        bundle_end=decision_time + timedelta(minutes=60),
        benchmark_symbol="SPY",
        status=CoverageStatus.AVAILABLE,
        bar_count=61,
        quote_count=len(quotes),
        bundle_bar_count=len(asset),
        bundle_quote_count=len(quotes),
        benchmark_bar_count=len(benchmark),
        scenario_covered=True,
        feature_history=FeatureHistoryCoverage(
            symbol_previous_sessions=20,
            benchmark_previous_sessions=20,
            symbol_same_slot_sessions=20,
            benchmark_same_slot_sessions=20,
            atr_source_minutes=75,
            confirmation_source_minutes=5,
            benchmark_confirmation_source_minutes=5,
            complete=True,
        ),
        eligibility=eligibility,
        tradable_coverage_complete=True,
        recorded_at=decision_time + timedelta(days=1),
    )
    state = HistoricalTradingState(
        symbol="AAPL",
        as_of=decision_time,
        known_at=decision_time,
        source="test-halt-and-borrow-manifest",
        halted=False,
        shortable=True,
        shortable_shares=10_000,
    )
    builder = HistoricalResearchCaseBuilder(
        data=lake,
        documents=FilingDocumentLoader(raw_root),
    )
    return builder, coverage, insight, state, lake
