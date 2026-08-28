from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Event
from typing import Any

import pytest

from event_trader.domain import DataSource
from event_trader.providers.ibkr_market import (
    FeedStatus,
    IBAPIBackendHook,
    IBKRMarketDataError,
    IBKRMarketDataNotReady,
    IBKRMarketDataPayloadError,
    IBKRMarketDataProvider,
    PrecomputedMarketFeatures,
    SnapshotBuilder,
    TopOfBook,
)

NOW = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)


class FakeBackend:
    def __init__(self) -> None:
        self.connected = True
        self.subscriptions: list[str] = []
        self.unsubscriptions: list[str] = []
        self.updates: dict[str, TopOfBook] = {}

    def is_connected(self) -> bool:
        return self.connected

    def subscribe_top_of_book(self, symbol: str) -> str:
        self.subscriptions.append(symbol)
        return f"subscription:{symbol}"

    def unsubscribe_top_of_book(self, subscription_id: str) -> None:
        self.unsubscriptions.append(subscription_id)

    def latest_top_of_book(self, symbol: str) -> TopOfBook | None:
        return self.updates.get(symbol)


def live_update(**changes: Any) -> TopOfBook:
    base = TopOfBook(
        symbol="AAPL",
        timestamp=NOW - timedelta(seconds=1),
        bid=Decimal("100.00"),
        ask=Decimal("100.10"),
        bid_size=200,
        ask_size=300,
        feed_status=FeedStatus.LIVE,
        feed="ibkr:live",
        nbbo_confirmed=True,
        halted=False,
        shortable=True,
        shortable_shares=2_500,
    )
    return replace(base, **changes)


def subscribed_provider(
    backend: FakeBackend, *, max_quote_age: timedelta = timedelta(seconds=5)
) -> IBKRMarketDataProvider:
    provider = IBKRMarketDataProvider(
        backend,
        max_quote_age=max_quote_age,
        clock=lambda: NOW,
    )
    provider.subscribe(" aapl ")
    return provider


def features(**changes: Any) -> PrecomputedMarketFeatures:
    base = PrecomputedMarketFeatures(
        symbol="AAPL",
        as_of=NOW,
        last=Decimal("100.05"),
        session_vwap=Decimal("99.80"),
        median_dollar_volume_20d=Decimal("150000000"),
        beta_adjusted_return_z=1.4,
        relative_volume=2.1,
        atr_5m=Decimal("0.45"),
        security_type="common_stock",
        primary_exchange="NASDAQ",
        us_listed=True,
    )
    return replace(base, **changes)


def test_live_nbbo_subscription_returns_typed_quote_and_is_idempotent() -> None:
    backend = FakeBackend()
    backend.updates["AAPL"] = live_update()
    provider = subscribed_provider(backend)

    assert provider.subscribe("AAPL") == "subscription:AAPL"
    assert backend.subscriptions == ["AAPL"]

    readiness = provider.readiness("AAPL")
    quote = provider.get_latest_quote("AAPL", feed="ibkr:live")

    assert readiness.ready
    assert quote.source is DataSource.IBKR
    assert quote.feed == "ibkr:live"
    assert quote.bid == Decimal("100.00")
    assert quote.ask_size == 300


@pytest.mark.parametrize(
    ("change", "failed_check"),
    [
        ({"feed_status": FeedStatus.DELAYED, "feed": "ibkr:delayed"}, "live_feed"),
        ({"timestamp": NOW - timedelta(seconds=6)}, "fresh"),
        ({"nbbo_confirmed": None}, "nbbo_confirmed"),
        ({"halted": None}, "halt_status_confirmed"),
        ({"halted": True}, "not_halted"),
    ],
)
def test_readiness_blocks_unsafe_market_states(
    change: dict[str, object], failed_check: str
) -> None:
    backend = FakeBackend()
    backend.updates["AAPL"] = live_update(**change)
    provider = subscribed_provider(backend)

    readiness = provider.readiness("AAPL")

    assert not readiness.ready
    assert failed_check in {check.name for check in readiness.failures}
    with pytest.raises(IBKRMarketDataNotReady):
        provider.get_latest_quote("AAPL")


def test_snapshot_builder_propagates_market_safety_and_shortability() -> None:
    backend = FakeBackend()
    backend.updates["AAPL"] = live_update()
    provider = subscribed_provider(backend)
    builder = SnapshotBuilder(provider, clock=lambda: NOW)

    snapshot = builder.build("AAPL", features())

    assert snapshot.symbol == "AAPL"
    assert snapshot.quote.source is DataSource.IBKR
    assert snapshot.as_of == NOW
    assert snapshot.data_fresh
    assert snapshot.market_data_live
    assert not snapshot.halted
    assert snapshot.shortable
    assert snapshot.shortable_shares == 2_500
    assert snapshot.relative_volume == 2.1
    assert snapshot.security_type == "common_stock"
    assert snapshot.us_listed


def test_snapshot_builder_rejects_stale_or_invalid_features() -> None:
    backend = FakeBackend()
    backend.updates["AAPL"] = live_update()
    provider = subscribed_provider(backend)
    builder = SnapshotBuilder(provider, clock=lambda: NOW)

    with pytest.raises(IBKRMarketDataNotReady, match="feature age"):
        builder.build("AAPL", features(as_of=NOW - timedelta(minutes=2)))

    with pytest.raises(IBKRMarketDataPayloadError, match="invalid precomputed"):
        builder.build("AAPL", features(relative_volume=-0.1))

    with pytest.raises(IBKRMarketDataPayloadError, match="feature symbol"):
        builder.build("AAPL", features(symbol="MSFT"))


class FakeOfficialClient:
    def __init__(self) -> None:
        self.connected = True
        self.requests: list[tuple[int, object, str, bool, bool, list[object]]] = []
        self.cancellations: list[int] = []

    def isConnected(self) -> bool:
        return self.connected

    def reqMktData(
        self,
        request_id: int,
        contract: object,
        generic_ticks: str,
        snapshot: bool,
        regulatory_snapshot: bool,
        options: list[object],
    ) -> None:
        self.requests.append(
            (
                request_id,
                contract,
                generic_ticks,
                snapshot,
                regulatory_snapshot,
                options,
            )
        )

    def cancelMktData(self, request_id: int) -> None:
        self.cancellations.append(request_id)


def test_official_callback_hook_stays_fail_closed_until_confirmed() -> None:
    client = FakeOfficialClient()
    hook = IBAPIBackendHook(
        client,
        contract_factory=lambda symbol: {"symbol": symbol},
        clock=lambda: NOW,
    )
    provider = IBKRMarketDataProvider(hook, clock=lambda: NOW)

    subscription_id = provider.subscribe("AAPL")
    request_id = int(subscription_id)
    hook.on_tick_price(request_id, 1, Decimal("100.00"))
    hook.on_tick_price(request_id, 2, Decimal("100.10"))
    hook.on_tick_size(request_id, 0, 200)
    hook.on_tick_size(request_id, 3, 300)

    initial_failures = {check.name for check in provider.readiness("AAPL").failures}
    assert {"live_feed", "nbbo_confirmed", "halt_status_confirmed"} <= initial_failures
    assert client.requests[0][2] == "236"

    hook.on_market_data_type(request_id, 1)
    hook.confirm_nbbo(request_id, True)
    hook.on_tick_generic(request_id, 49, 0)
    hook.update_shortability(request_id, shortable=True, available_shares=1_000)

    assert provider.readiness("AAPL").ready
    assert provider.get_latest_quote("AAPL").feed == "ibkr:live"

    provider.unsubscribe("AAPL")
    assert client.cancellations == [request_id]


def test_official_callback_hook_reports_delayed_feed_as_not_ready() -> None:
    client = FakeOfficialClient()
    hook = IBAPIBackendHook(
        client,
        contract_factory=lambda symbol: {"symbol": symbol},
        clock=lambda: NOW,
    )
    provider = IBKRMarketDataProvider(hook, clock=lambda: NOW)
    request_id = int(provider.subscribe("AAPL"))
    hook.on_tick_price(request_id, 66, 100)
    hook.on_tick_price(request_id, 67, 100.1)
    hook.on_tick_size(request_id, 69, 100)
    hook.on_tick_size(request_id, 70, 100)
    hook.on_market_data_type(request_id, 3)
    hook.confirm_nbbo(request_id, True)
    hook.on_tick_generic(request_id, 49, 0)

    readiness = provider.readiness("AAPL")

    assert not readiness.ready
    assert "live_feed" in {check.name for check in readiness.failures}


def test_official_hook_cleans_up_after_contract_factory_failure() -> None:
    client = FakeOfficialClient()
    attempts = 0

    def contract_factory(symbol: str) -> object:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("contract unavailable")
        return {"symbol": symbol}

    hook = IBAPIBackendHook(client, contract_factory=contract_factory)

    with pytest.raises(IBKRMarketDataError, match="subscription failed"):
        hook.subscribe_top_of_book("AAPL")

    subscription_id = hook.subscribe_top_of_book("AAPL")

    assert subscription_id == "10000"
    assert len(client.requests) == 1


@pytest.mark.parametrize(
    ("halt_value", "halt_confirmed", "not_halted"),
    [
        (-1, False, False),
        (0, True, True),
        (1, True, False),
        (2, True, False),
    ],
)
def test_official_halted_generic_tick_is_interpreted_fail_closed(
    halt_value: int, halt_confirmed: bool, not_halted: bool
) -> None:
    client = FakeOfficialClient()
    hook = IBAPIBackendHook(
        client,
        contract_factory=lambda symbol: {"symbol": symbol},
        clock=lambda: NOW,
    )
    provider = IBKRMarketDataProvider(hook, clock=lambda: NOW)
    request_id = int(provider.subscribe("AAPL"))
    hook.on_tick_price(request_id, 1, 100)
    hook.on_tick_price(request_id, 2, 100.1)
    hook.on_tick_size(request_id, 0, 100)
    hook.on_tick_size(request_id, 3, 100)
    hook.on_market_data_type(request_id, 1)
    hook.confirm_nbbo(request_id, True)

    hook.on_tick_generic(request_id, 49, halt_value)
    checks = {check.name: check.ready for check in provider.readiness("AAPL").checks}

    assert checks["halt_status_confirmed"] is halt_confirmed
    assert checks["not_halted"] is not_halted


def test_official_connection_loss_invalidates_sticky_market_facts() -> None:
    client = FakeOfficialClient()
    hook = IBAPIBackendHook(
        client,
        contract_factory=lambda symbol: {"symbol": symbol},
        clock=lambda: NOW,
    )
    provider = IBKRMarketDataProvider(hook, clock=lambda: NOW)
    request_id = int(provider.subscribe("AAPL"))
    hook.on_tick_price(request_id, 1, 100)
    hook.on_tick_price(request_id, 2, 100.1)
    hook.on_tick_size(request_id, 0, 100)
    hook.on_tick_size(request_id, 3, 100)
    hook.on_market_data_type(request_id, 1)
    hook.confirm_nbbo(request_id, True)
    hook.on_tick_generic(request_id, 49, 0)
    assert provider.readiness("AAPL").ready

    hook.on_connection_lost()

    readiness = provider.readiness("AAPL")
    assert not readiness.ready
    assert {"update_present", "live_feed", "nbbo_confirmed"} <= {
        check.name for check in readiness.failures
    }


def test_concurrent_subscription_creates_only_one_backend_request() -> None:
    entered = Event()
    release = Event()

    class BlockingBackend(FakeBackend):
        def subscribe_top_of_book(self, symbol: str) -> str:
            self.subscriptions.append(symbol)
            entered.set()
            assert release.wait(timeout=1)
            return f"subscription:{symbol}"

    backend = BlockingBackend()
    provider = IBKRMarketDataProvider(backend, clock=lambda: NOW)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(provider.subscribe, "AAPL")
        assert entered.wait(timeout=1)
        second = pool.submit(provider.subscribe, "AAPL")
        release.set()

        assert first.result(timeout=1) == "subscription:AAPL"
        assert second.result(timeout=1) == "subscription:AAPL"

    assert backend.subscriptions == ["AAPL"]
