from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from event_trader.domain import DataSource
from event_trader.providers.ibkr_bars import IBAPIBarHook
from event_trader.providers.ibkr_market import FeedStatus, IBKRMarketDataPayloadError


@dataclass
class StubBarData:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    wap: float


class StubClient:
    def __init__(self) -> None:
        self.history_requests: list[tuple] = []
        self.realtime_requests: list[tuple] = []
        self.fail = False

    def reqHistoricalData(self, *args):
        if self.fail:
            raise RuntimeError("gateway refused")
        self.history_requests.append(args)

    def reqRealTimeBars(self, *args):
        if self.fail:
            raise RuntimeError("gateway refused")
        self.realtime_requests.append(args)


def _contract(symbol: str) -> str:
    return f"contract:{symbol}"


def _hook(client: StubClient, **kwargs) -> IBAPIBarHook:
    return IBAPIBarHook(client, contract_factory=_contract, **kwargs)


MINUTE_START = datetime(2026, 8, 25, 14, 30, tzinfo=UTC)


def _historical(minute_start: datetime, close: float = 100.5, wap: float = 100.2) -> StubBarData:
    return StubBarData(
        date=str(int(minute_start.timestamp())),
        open=100.0,
        high=101.0,
        low=99.5,
        close=close,
        volume=1_000,
        wap=wap,
    )


def test_history_is_only_visible_once_it_is_complete() -> None:
    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("aapl"))

    hook.on_historical_data(request_id, _historical(MINUTE_START))

    assert hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1)) == ()
    hook.on_historical_data_end(request_id, "", "")
    bars = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))

    assert len(bars) == 1
    assert hook.history_complete("AAPL") is True


def test_a_historical_bar_price_stays_within_money_precision() -> None:
    """The historical path converts broker floats and must normalize them too.

    Only the live path was covered before, so removing the normalization from
    the historical conversion left the whole suite green - against the promise
    docs/testing.md makes about this exact class of change.
    """

    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("AAPL"))
    hook.on_historical_data(
        request_id, _historical(MINUTE_START, close=100.123456789, wap=100.987654321)
    )
    hook.on_historical_data_end(request_id, "", "")

    bar = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))[0]

    assert bar.close == Decimal("100.12345679")
    assert bar.vwap == Decimal("100.98765432")
    assert bar.close.as_tuple().exponent >= -8
    assert bar.vwap.as_tuple().exponent >= -8


def test_a_historical_bar_is_timestamped_at_its_completion() -> None:
    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("AAPL"))
    hook.on_historical_data(request_id, _historical(MINUTE_START))
    hook.on_historical_data_end(request_id, "", "")

    bar = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))[0]

    # IBKR stamps a bar by its start; the feature contract uses its completion.
    assert bar.timestamp == MINUTE_START + timedelta(minutes=1)
    assert bar.source is DataSource.IBKR
    assert bar.close == Decimal("100.5")
    assert bar.volume == 1_000


def test_volume_can_be_rescaled_for_a_lot_reporting_feed() -> None:
    client = StubClient()
    hook = _hook(client, volume_multiplier=100)
    request_id = int(hook.request_history("AAPL"))
    hook.on_historical_data(request_id, _historical(MINUTE_START))
    hook.on_historical_data_end(request_id, "", "")

    bar = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))[0]

    assert bar.volume == 100_000


def test_a_partial_live_minute_is_never_published() -> None:
    client = StubClient()
    hook = _hook(client)
    history_id = int(hook.request_history("AAPL"))
    hook.on_historical_data_end(history_id, "", "")
    realtime_id = int(hook.subscribe_realtime("AAPL"))

    for index in range(11):
        hook.on_realtime_bar(
            realtime_id,
            int((MINUTE_START + timedelta(seconds=5 * index)).timestamp()),
            100.0,
            100.4,
            99.9,
            100.2,
            50,
            100.1,
            7,
        )

    assert hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1)) == ()


def test_twelve_five_second_bars_become_one_completed_minute() -> None:
    client = StubClient()
    hook = _hook(client)
    history_id = int(hook.request_history("AAPL"))
    hook.on_market_data_type(history_id, 1)
    hook.on_historical_data_end(history_id, "", "")
    realtime_id = int(hook.subscribe_realtime("AAPL"))

    for index in range(12):
        hook.on_realtime_bar(
            realtime_id,
            int((MINUTE_START + timedelta(seconds=5 * index)).timestamp()),
            100.0 + index,
            100.5 + index,
            99.5 + index,
            100.2 + index,
            50,
            100.1 + index,
            7,
        )

    bars = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))

    assert len(bars) == 1
    minute = bars[0]
    assert minute.timestamp == MINUTE_START + timedelta(minutes=1)
    assert minute.open == Decimal("100.0")
    assert minute.close == Decimal("111.2")
    assert minute.high == Decimal("111.5")
    assert minute.low == Decimal("99.5")
    assert minute.volume == 600
    assert minute.feed == "ibkr:live"


def test_a_completed_minute_vwap_stays_within_money_precision() -> None:
    """The volume-weighted price is a division and can leave the contract.

    Eleven bars at 100.00 and one at 100.01, one share each, average to
    100.000833..., which Decimal carries to the context precision while
    ``Bar.vwap`` allows eight decimal places.
    """

    client = StubClient()
    hook = _hook(client)
    history_id = int(hook.request_history("AAPL"))
    hook.on_market_data_type(history_id, 1)
    hook.on_historical_data_end(history_id, "", "")
    realtime_id = int(hook.subscribe_realtime("AAPL"))

    for index in range(12):
        wap = 100.01 if index == 0 else 100.0
        hook.on_realtime_bar(
            realtime_id,
            int((MINUTE_START + timedelta(seconds=5 * index)).timestamp()),
            100.0,
            100.5,
            99.5,
            100.2,
            1,
            wap,
            7,
        )

    bars = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))

    assert len(bars) == 1
    assert bars[0].vwap == Decimal("100.00083333")
    assert bars[0].vwap.as_tuple().exponent >= -8


def test_the_feed_status_fails_closed_until_ibkr_confirms_it() -> None:
    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("AAPL"))

    assert hook.feed_status("AAPL") is FeedStatus.UNKNOWN
    hook.on_market_data_type(request_id, 3)
    assert hook.feed_status("AAPL") is FeedStatus.DELAYED
    hook.on_market_data_type(request_id, 1)
    assert hook.feed_status("AAPL") is FeedStatus.LIVE


def test_a_request_error_invalidates_the_history() -> None:
    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("AAPL"))
    hook.on_historical_data(request_id, _historical(MINUTE_START))
    hook.on_historical_data_end(request_id, "", "")
    assert hook.history_complete("AAPL") is True

    hook.on_error(request_id, 162, "historical data request pacing violation")

    assert hook.history_complete("AAPL") is False
    assert hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1)) == ()


def test_a_warning_does_not_invalidate_the_history() -> None:
    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("AAPL"))
    hook.on_historical_data(request_id, _historical(MINUTE_START))
    hook.on_historical_data_end(request_id, "", "")

    hook.on_error(request_id, 2104, "market data farm connection is OK")

    assert hook.history_complete("AAPL") is True


def test_repeated_requests_reuse_one_subscription() -> None:
    client = StubClient()
    hook = _hook(client)

    first = hook.request_history("AAPL")
    second = hook.request_history("aapl")
    live_first = hook.subscribe_realtime("AAPL")
    live_second = hook.subscribe_realtime("AAPL")

    assert first == second
    assert live_first == live_second
    assert len(client.history_requests) == 1
    assert len(client.realtime_requests) == 1


def test_a_refused_request_leaves_no_half_subscription() -> None:
    client = StubClient()
    client.fail = True
    hook = _hook(client)

    with pytest.raises(IBKRMarketDataPayloadError, match="history request failed"):
        hook.request_history("AAPL")
    with pytest.raises(IBKRMarketDataPayloadError, match="real-time bar request failed"):
        hook.subscribe_realtime("AAPL")

    client.fail = False
    assert hook.request_history("AAPL")


@pytest.mark.parametrize(
    "bad",
    [
        StubBarData("not-an-epoch", 100, 101, 99, 100, 10, 100),
        StubBarData(str(int(MINUTE_START.timestamp())), 0, 101, 99, 100, 10, 100),
        StubBarData(str(int(MINUTE_START.timestamp())), 100, 98, 99, 100, 10, 100),
        StubBarData(str(int(MINUTE_START.timestamp())), 100, 101, 99, 100, -5, 100),
    ],
)
def test_a_malformed_bar_fails_closed(bad) -> None:
    client = StubClient()
    hook = _hook(client)
    request_id = int(hook.request_history("AAPL"))

    with pytest.raises(IBKRMarketDataPayloadError):
        hook.on_historical_data(request_id, bad)


def test_bar_windows_require_timezone_aware_bounds() -> None:
    hook = _hook(StubClient())
    hook.request_history("AAPL")

    with pytest.raises(IBKRMarketDataPayloadError, match="timezone-aware"):
        hook.minute_bars("AAPL", start=MINUTE_START.replace(tzinfo=None), end=MINUTE_START)


def test_retained_history_is_bounded() -> None:
    client = StubClient()
    hook = _hook(client, retain_minutes=3)
    request_id = int(hook.request_history("AAPL"))
    for index in range(6):
        hook.on_historical_data(request_id, _historical(MINUTE_START + timedelta(minutes=index)))
    hook.on_historical_data_end(request_id, "", "")

    bars = hook.minute_bars("AAPL", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1))

    assert len(bars) == 3
    assert bars[0].timestamp == MINUTE_START + timedelta(minutes=4)


def test_an_unknown_symbol_has_no_bars_and_no_feed() -> None:
    hook = _hook(StubClient())

    assert hook.minute_bars("MSFT", start=MINUTE_START, end=MINUTE_START + timedelta(hours=1)) == ()
    assert hook.feed_status("MSFT") is FeedStatus.UNKNOWN
    assert hook.history_complete("MSFT") is False
