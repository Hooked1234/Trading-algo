from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import exchange_calendars as xcals
import pytest

from event_trader.domain import Bar, DataSource
from event_trader.providers.ibkr_features import (
    IBKRLiveFeatureProvider,
    StaticSecurityDescriptor,
    StaticSecurityReference,
    feature_input_sha256,
)
from event_trader.providers.ibkr_market import (
    FeedStatus,
    IBKRMarketDataNotReady,
    IBKRMarketDataPayloadError,
)

_XNYS = xcals.get_calendar("XNYS")


def _sessions(value: datetime, count: int) -> tuple[object, ...]:
    cursor: object = value.date().isoformat()
    sessions: list[object] = []
    for _ in range(count):
        cursor = _XNYS.previous_session(cursor)
        sessions.append(cursor)
    return tuple(reversed(sessions))


def _history(
    as_of: datetime,
    *,
    source: DataSource = DataSource.IBKR,
    feed: str = "ibkr:live",
) -> dict[str, tuple[Bar, ...]]:
    """Twenty complete prior sessions plus the current session up to ``as_of``."""

    sessions = (*_sessions(as_of, 20), as_of.date().isoformat())
    bars: dict[str, list[Bar]] = {"AAPL": [], "SPY": []}
    for index, session in enumerate(sessions):
        opening = _XNYS.session_open(session).to_pydatetime().astimezone(UTC)
        closing = _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
        current = index == 20
        full_minutes = int((closing - opening).total_seconds() // 60)
        minute_count = (
            int((as_of - opening).total_seconds() // 60) if current else full_minutes
        )
        volume = 6_000 if current else 600
        # Vary the per-session drift so the abnormal-return residuals have
        # positive variance, as a real history does.
        asset_step = (
            Decimal("0.10")
            if current
            else Decimal("0.002") + Decimal(index % 5) * Decimal("0.0004")
        )
        spy_step = (
            Decimal("0.01")
            if current
            else Decimal("0.001") + Decimal(index % 4) * Decimal("0.0003")
        )
        for minute in range(1, minute_count + 1):
            timestamp = opening + timedelta(minutes=minute)
            for symbol, base, step in (
                ("AAPL", Decimal("100"), asset_step),
                ("SPY", Decimal("400"), spy_step),
            ):
                open_price = base + step * Decimal(minute - 1)
                close_price = open_price + step
                bars[symbol].append(
                    Bar(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=open_price,
                        high=close_price + Decimal("0.02"),
                        low=open_price - Decimal("0.02"),
                        close=close_price,
                        volume=volume,
                        vwap=(open_price + close_price) / Decimal("2"),
                        source=source,
                        feed=feed,
                    )
                )
    return {symbol: tuple(values) for symbol, values in bars.items()}


class StubBarBackend:
    def __init__(self, bars, *, status: FeedStatus = FeedStatus.LIVE) -> None:
        self.bars = bars
        self.status = status

    def minute_bars(self, symbol, *, start, end):
        return tuple(
            bar for bar in self.bars.get(symbol, ()) if start <= bar.timestamp <= end
        )

    def feed_status(self, symbol):
        del symbol
        return self.status


@pytest.fixture
def as_of() -> datetime:
    # 2026-08-25 10:45 America/New_York, a completed NYSE minute.
    return datetime(2026, 8, 25, 14, 45, tzinfo=UTC)


def _provider(bars, **kwargs) -> IBKRLiveFeatureProvider:
    return IBKRLiveFeatureProvider(
        bars,
        securities=StaticSecurityReference(
            StaticSecurityDescriptor(
                security_type="common_stock", primary_exchange="NASDAQ", us_listed=True
            )
        ),
        **kwargs,
    )


def test_runtime_features_carry_provider_feed_and_input_hash(filing, as_of) -> None:
    bars = _history(as_of)
    provider = _provider(StubBarBackend(bars))

    features = provider.build(filing, "AAPL", as_of=as_of)

    assert features.provider == "ibkr"
    assert features.feed == "ibkr:live"
    assert features.security_type == "common_stock"
    assert features.us_listed is True
    assert features.input_sha256 == feature_input_sha256(
        bars["AAPL"], bars["SPY"], as_of=as_of
    )
    assert features.as_of == as_of


def test_runtime_features_are_reproducible(filing, as_of) -> None:
    provider = _provider(StubBarBackend(_history(as_of)))

    first = provider.build(filing, "AAPL", as_of=as_of)
    second = provider.build(filing, "AAPL", as_of=as_of)

    assert first == second


def test_alpaca_bars_are_never_mixed_into_the_runtime(filing, as_of) -> None:
    bars = _history(as_of)
    mixed = dict(bars)
    mixed["SPY"] = tuple(
        bar.model_copy(update={"source": DataSource.ALPACA_SIP}) for bar in bars["SPY"]
    )
    provider = _provider(StubBarBackend(mixed))

    with pytest.raises(IBKRMarketDataPayloadError, match="must not mix"):
        provider.build(filing, "AAPL", as_of=as_of)


def test_two_feeds_in_one_snapshot_fail_closed(filing, as_of) -> None:
    bars = _history(as_of)
    mixed = dict(bars)
    mixed["SPY"] = tuple(
        bar.model_copy(update={"feed": "ibkr:delayed"}) for bar in bars["SPY"]
    )
    provider = _provider(StubBarBackend(mixed))

    with pytest.raises(IBKRMarketDataPayloadError, match="one feed"):
        provider.build(filing, "AAPL", as_of=as_of)


@pytest.mark.parametrize(
    "status",
    [FeedStatus.DELAYED, FeedStatus.FROZEN, FeedStatus.DELAYED_FROZEN, FeedStatus.UNKNOWN],
)
def test_a_non_live_feed_fails_closed(filing, as_of, status) -> None:
    provider = _provider(StubBarBackend(_history(as_of), status=status))

    with pytest.raises(IBKRMarketDataNotReady, match="not live"):
        provider.build(filing, "AAPL", as_of=as_of)


def test_stale_bars_fail_closed(filing, as_of) -> None:
    provider = _provider(
        StubBarBackend(_history(as_of)), max_bar_age=timedelta(seconds=30)
    )

    with pytest.raises(IBKRMarketDataNotReady, match="stale"):
        provider.build(filing, "AAPL", as_of=as_of + timedelta(minutes=5))


def test_future_bars_fail_closed(filing, as_of) -> None:
    bars = _history(as_of)
    future = dict(bars)
    future["AAPL"] = (
        *bars["AAPL"],
        bars["AAPL"][-1].model_copy(
            update={"timestamp": as_of + timedelta(minutes=5)}
        ),
    )

    class FutureBackend(StubBarBackend):
        def minute_bars(self, symbol, *, start, end):
            del start, end
            return self.bars.get(symbol, ())

    provider = _provider(FutureBackend(future))

    with pytest.raises(IBKRMarketDataPayloadError, match="from the future"):
        provider.build(filing, "AAPL", as_of=as_of)


def test_contradictory_duplicates_fail_closed(filing, as_of) -> None:
    bars = _history(as_of)
    conflicting = dict(bars)
    last = bars["AAPL"][-1]
    conflicting["AAPL"] = (
        *bars["AAPL"],
        last.model_copy(update={"close": last.close + Decimal("1")}),
    )
    provider = _provider(StubBarBackend(conflicting))

    with pytest.raises(IBKRMarketDataPayloadError, match="contradictory"):
        provider.build(filing, "AAPL", as_of=as_of)


def test_a_gap_in_the_session_fails_closed(filing, as_of) -> None:
    bars = _history(as_of)
    missing = dict(bars)
    dropped = bars["AAPL"][-5].timestamp
    missing["AAPL"] = tuple(bar for bar in bars["AAPL"] if bar.timestamp != dropped)
    provider = _provider(StubBarBackend(missing))

    with pytest.raises(IBKRMarketDataNotReady, match="incomplete"):
        provider.build(filing, "AAPL", as_of=as_of)


def test_an_empty_history_fails_closed(filing, as_of) -> None:
    provider = _provider(StubBarBackend({}))

    with pytest.raises(IBKRMarketDataNotReady, match="no runtime bars"):
        provider.build(filing, "AAPL", as_of=as_of)


def test_the_benchmark_itself_is_never_an_event_symbol(filing, as_of) -> None:
    provider = _provider(StubBarBackend(_history(as_of)))

    with pytest.raises(IBKRMarketDataPayloadError, match="non-SPY"):
        provider.build(filing, "SPY", as_of=as_of)


def test_a_naive_timestamp_fails_closed(filing, as_of) -> None:
    provider = _provider(StubBarBackend(_history(as_of)))

    with pytest.raises(IBKRMarketDataPayloadError, match="timezone-aware"):
        provider.build(filing, "AAPL", as_of=as_of.replace(tzinfo=None))
