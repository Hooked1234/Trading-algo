"""Native IBKR bar callbacks for the runtime feature set.

IBKR delivers one-minute history through ``historicalData`` and live data as
five-second bars through ``realtimeBar``.  This hook accumulates both into the
completed one-minute bars the feature contract requires.

Two conversions matter and are done explicitly here:

* IBKR timestamps a bar by its *start*; the feature contract uses the bar's
  completion instant, so one bar length is added.
* A live minute is emitted only once all twelve five-second bars have arrived.
  A partial minute is never published, because a partial bar would silently
  understate volume and range.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from threading import RLock
from typing import Any

from event_trader.domain import Bar, DataSource, utc_now

from .ibkr_market import (
    FeedStatus,
    IBKRMarketDataDependencyError,
    IBKRMarketDataPayloadError,
)

Clock = Callable[[], datetime]
ContractFactory = Callable[[str], Any]

MINUTE = timedelta(minutes=1)
_REALTIME_BAR_SECONDS = 5
_BARS_PER_MINUTE = 12
_FEED_BY_MARKET_DATA_TYPE = {
    1: FeedStatus.LIVE,
    2: FeedStatus.FROZEN,
    3: FeedStatus.DELAYED,
    4: FeedStatus.DELAYED_FROZEN,
}


def _official_stock_contract(symbol: str) -> Any:
    try:
        from ibapi.contract import Contract
    except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover
        raise IBKRMarketDataDependencyError(
            "optional dependency 'ibapi' is required for the official bar hook"
        ) from exc
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


@dataclass(slots=True)
class _PartialMinute:
    """Five-second bars collected for one not-yet-complete minute."""

    minute_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    notional: Decimal
    count: int = 1

    def absorb(
        self,
        *,
        open_: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: int,
        wap: Decimal,
    ) -> None:
        del open_
        self.high = max(self.high, high)
        self.low = min(self.low, low)
        self.close = close
        self.volume += volume
        self.notional += wap * volume
        self.count += 1

    def complete(self, symbol: str, feed: str) -> Bar:
        vwap = self.notional / self.volume if self.volume else None
        return Bar(
            symbol=symbol,
            timestamp=self.minute_start + MINUTE,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            vwap=vwap,
            source=DataSource.IBKR,
            feed=feed,
        )


@dataclass(slots=True)
class _SymbolBars:
    symbol: str
    history_request_id: int | None = None
    realtime_request_id: int | None = None
    feed_status: FeedStatus = FeedStatus.UNKNOWN
    history_complete: bool = False
    minutes: dict[datetime, Bar] = field(default_factory=dict)
    partial: _PartialMinute | None = None

    @property
    def feed(self) -> str:
        return f"ibkr:{self.feed_status.value}"


class IBAPIBarHook:
    """Callback accumulator that presents IBKR bars as an ``IBKRBarBackend``.

    The owning ``EWrapper`` forwards ``historicalData``, ``historicalDataEnd``,
    ``realtimeBar``, ``marketDataType`` and ``error`` to the ``on_*`` methods.
    Absent callbacks fail closed: an unknown feed status is never treated as live.
    """

    def __init__(
        self,
        client: Any,
        *,
        first_request_id: int = 20_000,
        contract_factory: ContractFactory = _official_stock_contract,
        clock: Clock = utc_now,
        volume_multiplier: int = 1,
        retain_minutes: int = 40 * 390,
    ) -> None:
        if first_request_id < 0:
            raise ValueError("first_request_id cannot be negative")
        if volume_multiplier <= 0:
            raise ValueError("volume multiplier must be positive")
        if retain_minutes <= 0:
            raise ValueError("retained minute count must be positive")
        self._client = client
        self._contract_factory = contract_factory
        self._clock = clock
        # IBKR reports share volume in lots for some historical feeds.  The
        # operator must verify this against a known session before trusting
        # liquidity filters; it is deliberately not guessed at runtime.
        self._volume_multiplier = volume_multiplier
        self._retain_minutes = retain_minutes
        self._next_request_id = first_request_id
        self._lock = RLock()
        self._by_symbol: dict[str, _SymbolBars] = {}
        self._symbol_by_request: dict[int, str] = {}

    # --------------------------------------------------------- subscriptions --

    def request_history(
        self,
        symbol: str,
        *,
        duration: str = "40 D",
        bar_size: str = "1 min",
    ) -> str:
        """Request completed one-minute history for ``symbol``."""

        normalized = _symbol(symbol)
        with self._lock:
            state = self._by_symbol.setdefault(normalized, _SymbolBars(symbol=normalized))
            if state.history_request_id is not None:
                return str(state.history_request_id)
            request_id = self._claim_request_id(normalized)
            state.history_request_id = request_id
            try:
                self._client.reqHistoricalData(
                    request_id,
                    self._contract_factory(normalized),
                    "",
                    duration,
                    bar_size,
                    "TRADES",
                    1,
                    2,
                    False,
                    [],
                )
            except Exception as exc:
                state.history_request_id = None
                self._symbol_by_request.pop(request_id, None)
                raise IBKRMarketDataPayloadError(
                    f"IBKR history request failed: {exc.__class__.__name__}"
                ) from exc
            return str(request_id)

    def subscribe_realtime(self, symbol: str) -> str:
        """Subscribe to five-second live bars aggregated into whole minutes."""

        normalized = _symbol(symbol)
        with self._lock:
            state = self._by_symbol.setdefault(normalized, _SymbolBars(symbol=normalized))
            if state.realtime_request_id is not None:
                return str(state.realtime_request_id)
            request_id = self._claim_request_id(normalized)
            state.realtime_request_id = request_id
            try:
                self._client.reqRealTimeBars(
                    request_id,
                    self._contract_factory(normalized),
                    _REALTIME_BAR_SECONDS,
                    "TRADES",
                    True,
                    [],
                )
            except Exception as exc:
                state.realtime_request_id = None
                self._symbol_by_request.pop(request_id, None)
                raise IBKRMarketDataPayloadError(
                    f"IBKR real-time bar request failed: {exc.__class__.__name__}"
                ) from exc
            return str(request_id)

    def _claim_request_id(self, symbol: str) -> int:
        request_id = self._next_request_id
        self._next_request_id += 1
        self._symbol_by_request[request_id] = symbol
        return request_id

    # ---------------------------------------------------------------- reads --

    def minute_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
    ) -> Sequence[Bar]:
        normalized = _symbol(symbol)
        if start.tzinfo is None or end.tzinfo is None:
            raise IBKRMarketDataPayloadError("bar window bounds must be timezone-aware")
        with self._lock:
            state = self._by_symbol.get(normalized)
            if state is None or not state.history_complete:
                return ()
            return tuple(
                state.minutes[timestamp]
                for timestamp in sorted(state.minutes)
                if start <= timestamp <= end
            )

    def feed_status(self, symbol: str) -> FeedStatus:
        with self._lock:
            state = self._by_symbol.get(_symbol(symbol))
            return state.feed_status if state is not None else FeedStatus.UNKNOWN

    def history_complete(self, symbol: str) -> bool:
        with self._lock:
            state = self._by_symbol.get(_symbol(symbol))
            return bool(state and state.history_complete)

    # ------------------------------------------------------------ callbacks --

    def on_market_data_type(self, request_id: int, market_data_type: int) -> None:
        with self._lock:
            state = self._state_for(request_id)
            if state is not None:
                state.feed_status = _FEED_BY_MARKET_DATA_TYPE.get(
                    market_data_type, FeedStatus.UNKNOWN
                )

    def on_historical_data(self, request_id: int, bar: Any) -> None:
        with self._lock:
            state = self._state_for(request_id)
            if state is None or state.history_request_id != request_id:
                return
            completed = self._historical_bar(state, bar)
            state.minutes[completed.timestamp] = completed
            self._trim(state)

    def on_historical_data_end(self, request_id: int, start: str, end: str) -> None:
        del start, end
        with self._lock:
            state = self._state_for(request_id)
            if state is not None and state.history_request_id == request_id:
                state.history_complete = True

    def on_realtime_bar(
        self,
        request_id: int,
        bar_start_epoch: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        wap: float,
        count: int,
    ) -> None:
        del count
        with self._lock:
            state = self._state_for(request_id)
            if state is None or state.realtime_request_id != request_id:
                return
            started = datetime.fromtimestamp(int(bar_start_epoch), tz=UTC)
            minute_start = started.replace(second=0, microsecond=0)
            values = _decimals(open_, high, low, close, wap)
            shares = int(volume) * self._volume_multiplier
            if shares < 0:
                raise IBKRMarketDataPayloadError("bar volume cannot be negative")
            partial = state.partial
            if partial is None or partial.minute_start != minute_start:
                state.partial = _PartialMinute(
                    minute_start=minute_start,
                    open=values["open"],
                    high=values["high"],
                    low=values["low"],
                    close=values["close"],
                    volume=shares,
                    notional=values["wap"] * shares,
                )
            else:
                partial.absorb(
                    open_=values["open"],
                    high=values["high"],
                    low=values["low"],
                    close=values["close"],
                    volume=shares,
                    wap=values["wap"],
                )
            current = state.partial
            assert current is not None
            if current.count >= _BARS_PER_MINUTE:
                completed = current.complete(state.symbol, state.feed)
                state.minutes[completed.timestamp] = completed
                state.partial = None
                self._trim(state)

    def on_error(self, request_id: int, code: int, message: str) -> None:
        """Drop a symbol's history on a request-level error so it fails closed."""

        del message
        if code < 2100:  # 2100+ are warnings, not request failures
            with self._lock:
                state = self._state_for(request_id)
                if state is not None and state.history_request_id == request_id:
                    state.history_complete = False

    # ----------------------------------------------------------- internals --

    def _state_for(self, request_id: int) -> _SymbolBars | None:
        symbol = self._symbol_by_request.get(request_id)
        return self._by_symbol.get(symbol) if symbol is not None else None

    def _historical_bar(self, state: _SymbolBars, bar: Any) -> Bar:
        try:
            started = _parse_bar_time(bar.date)
            values = _decimals(bar.open, bar.high, bar.low, bar.close, bar.wap)
            shares = int(bar.volume) * self._volume_multiplier
        except (AttributeError, TypeError, ValueError, InvalidOperation) as exc:
            raise IBKRMarketDataPayloadError("malformed IBKR historical bar") from exc
        if shares < 0:
            raise IBKRMarketDataPayloadError("bar volume cannot be negative")
        return Bar(
            symbol=state.symbol,
            timestamp=started + MINUTE,
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            volume=shares,
            vwap=values["wap"] if shares else None,
            source=DataSource.IBKR,
            feed=state.feed,
        )

    def _trim(self, state: _SymbolBars) -> None:
        excess = len(state.minutes) - self._retain_minutes
        if excess <= 0:
            return
        for timestamp in sorted(state.minutes)[:excess]:
            del state.minutes[timestamp]


def _symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise IBKRMarketDataPayloadError("symbol must not be empty")
    return normalized


def _decimals(
    open_: float, high: float, low: float, close: float, wap: float
) -> dict[str, Decimal]:
    values = {
        "open": Decimal(str(open_)),
        "high": Decimal(str(high)),
        "low": Decimal(str(low)),
        "close": Decimal(str(close)),
        "wap": Decimal(str(wap)),
    }
    if any(value <= 0 for value in values.values()):
        raise IBKRMarketDataPayloadError("bar prices must be positive")
    if values["high"] < values["low"]:
        raise IBKRMarketDataPayloadError("bar high cannot be below its low")
    return values


def _parse_bar_time(value: Any) -> datetime:
    """Parse an IBKR bar timestamp requested with ``formatDate=2`` (epoch seconds)."""

    if isinstance(value, int | float):
        return datetime.fromtimestamp(int(value), tz=UTC)
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=UTC)
    raise IBKRMarketDataPayloadError(
        "IBKR bars must be requested with formatDate=2 (epoch seconds)"
    )


__all__ = ["IBAPIBarHook"]
