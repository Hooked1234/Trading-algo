"""Fail-closed IBKR real-time market-data provider.

The provider consumes a narrow injectable backend.  The optional official API
hook below can be wired into an existing ``EWrapper``/``EClient`` session; it
does not create a socket and remains not-ready until live feed, NBBO, quote
freshness, and halt state have all been confirmed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from threading import RLock
from typing import Any, Protocol, runtime_checkable

from pydantic import ValidationError

from event_trader.domain import DataSource, MarketSnapshot, Quote, utc_now

Clock = Callable[[], datetime]
ContractFactory = Callable[[str], Any]


class IBKRMarketDataError(RuntimeError):
    """Base class for real-time market-data failures."""


class IBKRMarketDataNotReady(IBKRMarketDataError):
    """A quote cannot safely drive a strategy or order decision."""


class IBKRMarketDataPayloadError(IBKRMarketDataError):
    """The backend supplied an internally inconsistent market state."""


class IBKRMarketDataDependencyError(IBKRMarketDataError):
    """The optional official API dependency is unavailable."""


class FeedStatus(StrEnum):
    UNKNOWN = "unknown"
    LIVE = "live"
    FROZEN = "frozen"
    DELAYED = "delayed"
    DELAYED_FROZEN = "delayed_frozen"


@dataclass(frozen=True, slots=True)
class TopOfBook:
    """Latest complete top-of-book state supplied by a backend."""

    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    bid_size: int
    ask_size: int
    feed_status: FeedStatus
    feed: str
    nbbo_confirmed: bool | None = None
    halted: bool | None = None
    shortable: bool | None = None
    shortable_shares: int | None = None


@runtime_checkable
class IBKRMarketDataBackend(Protocol):
    """Transport seam for TWS/IB Gateway and deterministic tests."""

    def is_connected(self) -> bool: ...

    def subscribe_top_of_book(self, symbol: str) -> str: ...

    def unsubscribe_top_of_book(self, subscription_id: str) -> None: ...

    def latest_top_of_book(self, symbol: str) -> TopOfBook | None: ...


@dataclass(frozen=True, slots=True)
class MarketDataCheck:
    name: str
    ready: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class MarketDataReadiness:
    symbol: str
    checked_at: datetime
    checks: tuple[MarketDataCheck, ...]

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.ready for check in self.checks)

    @property
    def failures(self) -> tuple[MarketDataCheck, ...]:
        return tuple(check for check in self.checks if not check.ready)

    def require(self) -> None:
        if self.ready:
            return
        details = "; ".join(
            f"{check.name}: {check.detail or 'not ready'}" for check in self.failures
        )
        raise IBKRMarketDataNotReady(details or "market data is not ready")


@dataclass(frozen=True, slots=True)
class PrecomputedMarketFeatures:
    """Deterministic quantitative features computed outside the provider."""

    symbol: str
    as_of: datetime
    last: Decimal
    session_vwap: Decimal
    median_dollar_volume_20d: Decimal
    beta_adjusted_return_z: float
    relative_volume: float
    atr_5m: Decimal
    security_type: str = "unknown"
    primary_exchange: str | None = None
    us_listed: bool = False
    # Runtime lineage: which provider and feed produced the bars behind these
    # numbers, and the content address of that exact input set.
    provider: str = "unknown"
    feed: str = "unknown"
    input_sha256: str = ""


class IBKRMarketDataProvider:
    """Validated real-time quote access over an injected IBKR backend."""

    def __init__(
        self,
        backend: IBKRMarketDataBackend,
        *,
        max_quote_age: timedelta = timedelta(seconds=5),
        future_tolerance: timedelta = timedelta(seconds=1),
        clock: Clock = utc_now,
    ) -> None:
        if max_quote_age <= timedelta(0):
            raise ValueError("max_quote_age must be positive")
        if future_tolerance < timedelta(0):
            raise ValueError("future_tolerance cannot be negative")
        self._backend = backend
        self._max_quote_age = max_quote_age
        self._future_tolerance = future_tolerance
        self._clock = clock
        self._lock = RLock()
        self._subscriptions: dict[str, str] = {}

    @staticmethod
    def _symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    def subscribe(self, symbol: str) -> str:
        normalized = self._symbol(symbol)
        with self._lock:
            existing = self._subscriptions.get(normalized)
            if existing is not None:
                return existing
            if not self._backend.is_connected():
                raise IBKRMarketDataNotReady("IB Gateway/TWS is disconnected")
            subscription_id = self._backend.subscribe_top_of_book(normalized)
            if not subscription_id:
                raise IBKRMarketDataPayloadError("backend returned an empty subscription id")
            self._subscriptions[normalized] = subscription_id
            return subscription_id

    def unsubscribe(self, symbol: str) -> None:
        normalized = self._symbol(symbol)
        with self._lock:
            subscription_id = self._subscriptions.pop(normalized, None)
            if subscription_id is not None:
                self._backend.unsubscribe_top_of_book(subscription_id)

    def _quote(self, update: TopOfBook) -> Quote:
        try:
            return Quote(
                symbol=update.symbol,
                timestamp=update.timestamp,
                bid=update.bid,
                ask=update.ask,
                bid_size=update.bid_size,
                ask_size=update.ask_size,
                source=DataSource.IBKR,
                feed=update.feed,
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise IBKRMarketDataPayloadError(
                f"invalid top-of-book payload for {update.symbol}"
            ) from exc

    def _evaluate(
        self,
        symbol: str,
        update: TopOfBook | None,
        *,
        now: datetime,
    ) -> MarketDataReadiness:
        subscribed = symbol in self._subscriptions
        connected = self._backend.is_connected()
        update_present = update is not None
        symbol_matches = update is not None and update.symbol == symbol
        live = update is not None and update.feed_status is FeedStatus.LIVE
        nbbo = update is not None and update.nbbo_confirmed is True
        halt_confirmed = update is not None and update.halted is not None
        not_halted = update is not None and update.halted is False

        quote_valid = False
        fresh = False
        quote_detail = "top-of-book is incomplete"
        freshness_detail = "quote timestamp is unavailable"
        if update is not None and symbol_matches:
            try:
                self._quote(update)
            except IBKRMarketDataPayloadError as exc:
                quote_detail = str(exc)
            else:
                quote_valid = True
                quote_detail = ""
                if (
                    update.timestamp.tzinfo is not None
                    and update.timestamp.utcoffset() is not None
                    and now.tzinfo is not None
                    and now.utcoffset() is not None
                ):
                    age = now - update.timestamp
                    fresh = -self._future_tolerance <= age <= self._max_quote_age
                    freshness_detail = (
                        "" if fresh else f"quote age {age.total_seconds():.3f}s is invalid"
                    )
                else:
                    freshness_detail = "quote and clock timestamps must be timezone-aware"

        checks = (
            MarketDataCheck("connected", connected, "IB Gateway/TWS is disconnected"),
            MarketDataCheck("subscribed", subscribed, "symbol is not subscribed"),
            MarketDataCheck("update_present", update_present, "no complete quote received"),
            MarketDataCheck("symbol_matches", symbol_matches, "backend quote symbol mismatch"),
            MarketDataCheck("quote_valid", quote_valid, quote_detail),
            MarketDataCheck(
                "live_feed",
                live,
                "feed is delayed, frozen, or unconfirmed",
            ),
            MarketDataCheck("nbbo_confirmed", nbbo, "NBBO is not confirmed"),
            MarketDataCheck("fresh", fresh, freshness_detail),
            MarketDataCheck(
                "halt_status_confirmed",
                halt_confirmed,
                "trading halt state is unconfirmed",
            ),
            MarketDataCheck("not_halted", not_halted, "security is halted"),
        )
        return MarketDataReadiness(symbol=symbol, checked_at=now, checks=checks)

    def readiness(self, symbol: str) -> MarketDataReadiness:
        normalized = self._symbol(symbol)
        with self._lock:
            update = self._backend.latest_top_of_book(normalized)
            return self._evaluate(normalized, update, now=self._clock())

    def current(self, symbol: str) -> tuple[Quote, TopOfBook]:
        """Return one internally consistent, fully ready market-data state."""

        normalized = self._symbol(symbol)
        with self._lock:
            update = self._backend.latest_top_of_book(normalized)
            self._evaluate(normalized, update, now=self._clock()).require()
            assert update is not None  # guaranteed by readiness
            return self._quote(update), update

    def get_latest_quote(self, symbol: str, *, feed: str | None = None) -> Quote:
        """Return a fresh live quote; backend truth overrides requested feed labels."""

        quote, update = self.current(symbol)
        if feed not in (None, "", "ibkr", "live", update.feed):
            raise IBKRMarketDataNotReady(
                f"requested feed {feed!r} does not match backend feed {update.feed!r}"
            )
        return quote

    def latest_quote(self, symbol: str, *, feed: str | None = None) -> Quote:
        return self.get_latest_quote(symbol, feed=feed)


class SnapshotBuilder:
    """Combine a ready quote with separately computed quantitative features."""

    def __init__(
        self,
        provider: IBKRMarketDataProvider,
        *,
        max_feature_age: timedelta = timedelta(minutes=1),
        future_tolerance: timedelta = timedelta(seconds=1),
        clock: Clock = utc_now,
    ) -> None:
        if max_feature_age <= timedelta(0):
            raise ValueError("max_feature_age must be positive")
        if future_tolerance < timedelta(0):
            raise ValueError("future_tolerance cannot be negative")
        self._provider = provider
        self._max_feature_age = max_feature_age
        self._future_tolerance = future_tolerance
        self._clock = clock

    def build(self, symbol: str, features: PrecomputedMarketFeatures) -> MarketSnapshot:
        quote, update = self._provider.current(symbol)
        feature_symbol = features.symbol.strip().upper()
        if not feature_symbol or feature_symbol != quote.symbol:
            raise IBKRMarketDataPayloadError("precomputed feature symbol does not match the quote")
        now = self._clock()
        if (
            features.as_of.tzinfo is None
            or features.as_of.utcoffset() is None
            or now.tzinfo is None
            or now.utcoffset() is None
        ):
            raise IBKRMarketDataPayloadError("feature and clock timestamps must be timezone-aware")
        feature_age = now - features.as_of
        if not -self._future_tolerance <= feature_age <= self._max_feature_age:
            raise IBKRMarketDataNotReady(
                f"feature age {feature_age.total_seconds():.3f}s is invalid"
            )

        shares = update.shortable_shares if update.shortable_shares is not None else 0
        if shares < 0:
            raise IBKRMarketDataPayloadError("shortable shares cannot be negative")
        if update.shortable is False and shares > 0:
            raise IBKRMarketDataPayloadError(
                "backend marked security unshortable but supplied available shares"
            )
        shortable = update.shortable is True or (update.shortable is None and shares > 0)
        try:
            return MarketSnapshot(
                symbol=quote.symbol,
                as_of=max(quote.timestamp, features.as_of),
                quote=quote,
                last=features.last,
                session_vwap=features.session_vwap,
                median_dollar_volume_20d=features.median_dollar_volume_20d,
                beta_adjusted_return_z=features.beta_adjusted_return_z,
                relative_volume=features.relative_volume,
                atr_5m=features.atr_5m,
                data_fresh=True,
                market_data_live=update.feed_status is FeedStatus.LIVE,
                halted=bool(update.halted),
                shortable=shortable,
                shortable_shares=shares,
                security_type=features.security_type,
                primary_exchange=features.primary_exchange,
                us_listed=features.us_listed,
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise IBKRMarketDataPayloadError(
                f"invalid precomputed market features for {quote.symbol}"
            ) from exc


@dataclass(slots=True)
class _HookState:
    symbol: str
    request_id: int
    bid: Decimal | None = None
    ask: Decimal | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    bid_at: datetime | None = None
    ask_at: datetime | None = None
    bid_size_at: datetime | None = None
    ask_size_at: datetime | None = None
    feed_status: FeedStatus = FeedStatus.UNKNOWN
    feed: str = "ibkr:unknown"
    nbbo_confirmed: bool | None = None
    halted: bool | None = None
    shortable: bool | None = None
    shortable_shares: int | None = None


def _official_stock_contract(symbol: str) -> Any:
    try:
        from ibapi.contract import Contract
    except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover
        raise IBKRMarketDataDependencyError(
            "optional dependency 'ibapi' is required for the official hook"
        ) from exc
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


class IBAPIBackendHook:
    """Callback accumulator for an already authenticated official API client.

    The owning ``EWrapper`` should forward ``marketDataType``, ``tickPrice``,
    ``tickSize`` and ``tickGeneric`` callbacks to the corresponding ``on_*``
    methods. SMART/NBBO and halt confirmation are explicit; absent callbacks
    therefore fail closed.
    """

    def __init__(
        self,
        client: Any,
        *,
        first_request_id: int = 10_000,
        contract_factory: ContractFactory = _official_stock_contract,
        clock: Clock = utc_now,
    ) -> None:
        if first_request_id < 0:
            raise ValueError("first_request_id cannot be negative")
        self._client = client
        self._contract_factory = contract_factory
        self._clock = clock
        self._next_request_id = first_request_id
        self._lock = RLock()
        self._by_symbol: dict[str, _HookState] = {}
        self._symbol_by_request: dict[int, str] = {}

    def is_connected(self) -> bool:
        try:
            return bool(self._client.isConnected())
        except (AttributeError, TypeError):
            return False

    def subscribe_top_of_book(self, symbol: str) -> str:
        with self._lock:
            existing = self._by_symbol.get(symbol)
            if existing is not None:
                return str(existing.request_id)
            try:
                contract = self._contract_factory(symbol)
            except Exception as exc:
                raise IBKRMarketDataError(
                    f"IBKR market-data subscription failed: {exc.__class__.__name__}"
                ) from exc
            request_id = self._next_request_id
            self._next_request_id += 1
            state = _HookState(symbol=symbol, request_id=request_id)
            self._by_symbol[symbol] = state
            self._symbol_by_request[request_id] = symbol
            try:
                # Generic tick 236 requests available shortable shares where entitled.
                self._client.reqMktData(request_id, contract, "236", False, False, [])
            except Exception as exc:
                self._by_symbol.pop(symbol, None)
                self._symbol_by_request.pop(request_id, None)
                raise IBKRMarketDataError(
                    f"IBKR market-data subscription failed: {exc.__class__.__name__}"
                ) from exc
            return str(request_id)

    def unsubscribe_top_of_book(self, subscription_id: str) -> None:
        try:
            request_id = int(subscription_id)
        except ValueError as exc:
            raise IBKRMarketDataPayloadError("invalid subscription id") from exc
        with self._lock:
            symbol = self._symbol_by_request.pop(request_id, None)
            if symbol is None:
                return
            self._by_symbol.pop(symbol, None)
        self._client.cancelMktData(request_id)

    def latest_top_of_book(self, symbol: str) -> TopOfBook | None:
        with self._lock:
            state = self._by_symbol.get(symbol)
            if state is None or any(
                value is None
                for value in (
                    state.bid,
                    state.ask,
                    state.bid_size,
                    state.ask_size,
                    state.bid_at,
                    state.ask_at,
                    state.bid_size_at,
                    state.ask_size_at,
                )
            ):
                return None
            timestamps = (
                state.bid_at,
                state.ask_at,
                state.bid_size_at,
                state.ask_size_at,
            )
            timestamp = min(value for value in timestamps if value is not None)
            assert state.bid is not None
            assert state.ask is not None
            assert state.bid_size is not None
            assert state.ask_size is not None
            return TopOfBook(
                symbol=state.symbol,
                timestamp=timestamp,
                bid=state.bid,
                ask=state.ask,
                bid_size=state.bid_size,
                ask_size=state.ask_size,
                feed_status=state.feed_status,
                feed=state.feed,
                nbbo_confirmed=state.nbbo_confirmed,
                halted=state.halted,
                shortable=state.shortable,
                shortable_shares=state.shortable_shares,
            )

    def _state_for_request(self, request_id: int) -> _HookState | None:
        symbol = self._symbol_by_request.get(request_id)
        return self._by_symbol.get(symbol) if symbol is not None else None

    def on_market_data_type(self, request_id: int, market_data_type: int) -> None:
        mapping = {
            1: FeedStatus.LIVE,
            2: FeedStatus.FROZEN,
            3: FeedStatus.DELAYED,
            4: FeedStatus.DELAYED_FROZEN,
        }
        with self._lock:
            state = self._state_for_request(request_id)
            if state is not None:
                state.feed_status = mapping.get(market_data_type, FeedStatus.UNKNOWN)
                state.feed = f"ibkr:{state.feed_status.value}"

    def on_tick_price(
        self,
        request_id: int,
        tick_type: int,
        price: float | Decimal,
        *,
        received_at: datetime | None = None,
    ) -> None:
        now = received_at or self._clock()
        with self._lock:
            state = self._state_for_request(request_id)
            if state is None:
                return
            if tick_type in {1, 66}:  # BID / DELAYED_BID
                state.bid = Decimal(str(price))
                state.bid_at = now
            elif tick_type in {2, 67}:  # ASK / DELAYED_ASK
                state.ask = Decimal(str(price))
                state.ask_at = now

    def on_tick_size(
        self,
        request_id: int,
        tick_type: int,
        size: int | Decimal,
        *,
        received_at: datetime | None = None,
    ) -> None:
        now = received_at or self._clock()
        try:
            decimal_size = Decimal(str(size))
            parsed = int(decimal_size)
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise IBKRMarketDataPayloadError("invalid market-data size") from exc
        if decimal_size != parsed:
            raise IBKRMarketDataPayloadError("market-data size must be an integer")
        with self._lock:
            state = self._state_for_request(request_id)
            if state is None:
                return
            if tick_type in {0, 69}:  # BID_SIZE / DELAYED_BID_SIZE
                state.bid_size = parsed
                state.bid_size_at = now
            elif tick_type in {3, 70}:  # ASK_SIZE / DELAYED_ASK_SIZE
                state.ask_size = parsed
                state.ask_size_at = now
            elif tick_type == 89:  # SHORTABLE_SHARES
                if parsed < 0:
                    state.shortable_shares = None
                    state.shortable = None
                else:
                    state.shortable_shares = parsed
                    state.shortable = parsed > 0

    def on_tick_generic(
        self,
        request_id: int,
        tick_type: int,
        value: float | Decimal,
    ) -> None:
        """Apply generic ticks needed for safety metadata.

        IBKR's HALTED tick (49) uses ``-1`` for unknown/unavailable, ``0`` for
        trading, and positive values for a halt. Unknown values remain
        unconfirmed so readiness fails closed.
        """

        if tick_type != 49:
            return
        try:
            parsed = Decimal(str(value))
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise IBKRMarketDataPayloadError("invalid trading-halt value") from exc
        if parsed == Decimal("0"):
            halted: bool | None = False
        elif parsed > 0:
            halted = True
        else:
            halted = None
        with self._lock:
            state = self._state_for_request(request_id)
            if state is not None:
                state.halted = halted

    def on_connection_lost(self) -> None:
        """Invalidate all sticky quote and safety facts after a disconnect."""

        with self._lock:
            for state in self._by_symbol.values():
                state.bid = None
                state.ask = None
                state.bid_size = None
                state.ask_size = None
                state.bid_at = None
                state.ask_at = None
                state.bid_size_at = None
                state.ask_size_at = None
                state.feed_status = FeedStatus.UNKNOWN
                state.feed = "ibkr:unknown"
                state.nbbo_confirmed = None
                state.halted = None
                state.shortable = None
                state.shortable_shares = None

    def confirm_nbbo(self, request_id: int, confirmed: bool) -> None:
        with self._lock:
            state = self._state_for_request(request_id)
            if state is not None:
                state.nbbo_confirmed = confirmed

    def confirm_halt(self, request_id: int, halted: bool) -> None:
        with self._lock:
            state = self._state_for_request(request_id)
            if state is not None:
                state.halted = halted

    def update_shortability(
        self, request_id: int, *, shortable: bool, available_shares: int | None
    ) -> None:
        if available_shares is not None and available_shares < 0:
            raise IBKRMarketDataPayloadError("available shares cannot be negative")
        with self._lock:
            state = self._state_for_request(request_id)
            if state is not None:
                state.shortable = shortable
                state.shortable_shares = available_shares
