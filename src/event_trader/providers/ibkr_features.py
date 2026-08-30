"""Runtime market features computed from IBKR bars only.

Version 1 keeps the historical and the runtime feed strictly apart: research
uses Alpaca SIP, the live path uses IBKR.  Mixing them inside one snapshot would
make a live decision incomparable to the backtest that authorised it, so a
mixed, delayed, stale, future-dated or self-contradictory bar set fails closed.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol

from event_trader.artifacts import canonical_hash
from event_trader.domain import Bar, DataSource, FilingEvent
from event_trader.features import build_feature_inputs, compute_market_features

from .ibkr_market import (
    FeedStatus,
    IBKRMarketDataNotReady,
    IBKRMarketDataPayloadError,
    PrecomputedMarketFeatures,
)

BENCHMARK_SYMBOL = "SPY"
_RUNTIME_PROVIDER = "ibkr"
_SESSION_LOOKBACK = timedelta(days=40)
_FEATURE_HASH_SCHEMA = "runtime-feature-input/1"


class IBKRBarBackend(Protocol):
    """Minute history and completed live bars for one symbol."""

    def minute_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
    ) -> Sequence[Bar]: ...

    def feed_status(self, symbol: str) -> FeedStatus: ...


class SecurityDescriptor(Protocol):
    @property
    def security_type(self) -> str: ...

    @property
    def primary_exchange(self) -> str | None: ...

    @property
    def us_listed(self) -> bool: ...


class SecurityReference(Protocol):
    def describe(self, symbol: str) -> SecurityDescriptor: ...


@dataclasses.dataclass(frozen=True, slots=True)
class StaticSecurityDescriptor:
    security_type: str = "unknown"
    primary_exchange: str | None = None
    us_listed: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class StaticSecurityReference:
    """Security master that answers the same way for every symbol.

    Useful for a single-symbol runtime and for tests; a real deployment resolves
    contract details per symbol.
    """

    descriptor: StaticSecurityDescriptor = dataclasses.field(
        default_factory=StaticSecurityDescriptor
    )

    def describe(self, symbol: str) -> StaticSecurityDescriptor:
        del symbol
        return self.descriptor


class IBKRLiveFeatureProvider:
    """Compute the runtime feature set from IBKR bars for the symbol and SPY."""

    def __init__(
        self,
        bars: IBKRBarBackend,
        *,
        securities: SecurityReference | None = None,
        max_bar_age: timedelta = timedelta(minutes=2),
        future_tolerance: timedelta = timedelta(seconds=1),
        lookback: timedelta = _SESSION_LOOKBACK,
    ) -> None:
        if max_bar_age <= timedelta(0):
            raise ValueError("max_bar_age must be positive")
        if future_tolerance < timedelta(0):
            raise ValueError("future_tolerance cannot be negative")
        if lookback <= timedelta(0):
            raise ValueError("lookback must be positive")
        self._bars = bars
        self._securities = securities
        self._max_bar_age = max_bar_age
        self._future_tolerance = future_tolerance
        self._lookback = lookback

    def build(
        self,
        filing: FilingEvent,
        symbol: str,
        *,
        as_of: datetime,
    ) -> PrecomputedMarketFeatures:
        del filing
        return self.build_symbol(symbol, as_of=as_of)

    def build_symbol(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> PrecomputedMarketFeatures:
        """Build the same IBKR-only features when no filing object is needed."""

        normalized = symbol.strip().upper()
        if not normalized or normalized == BENCHMARK_SYMBOL:
            raise IBKRMarketDataPayloadError("runtime features require a non-SPY event symbol")
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise IBKRMarketDataPayloadError("runtime feature time must be timezone-aware")

        self._ensure_subscribed(normalized)
        self._ensure_subscribed(BENCHMARK_SYMBOL)
        self._require_live_feed(normalized)
        self._require_live_feed(BENCHMARK_SYMBOL)
        start = as_of - self._lookback
        asset = self._load(normalized, start=start, end=as_of)
        benchmark = self._load(BENCHMARK_SYMBOL, start=start, end=as_of)
        feed = _single_feed(asset + benchmark)
        self._require_fresh(asset, as_of)
        self._require_fresh(benchmark, as_of)

        try:
            inputs = build_feature_inputs(
                symbol=normalized,
                symbol_one_minute_bars=asset,
                spy_one_minute_bars=benchmark,
                as_of=as_of,
            )
            features = compute_market_features(inputs)
        except (LookupError, TypeError, ValueError) as exc:
            raise IBKRMarketDataNotReady(
                f"runtime features for {normalized} are incomplete: {exc}"
            ) from exc

        descriptor: SecurityDescriptor = (
            self._securities.describe(normalized)
            if self._securities is not None
            else StaticSecurityDescriptor()
        )
        return dataclasses.replace(
            features,
            security_type=descriptor.security_type,
            primary_exchange=descriptor.primary_exchange,
            us_listed=descriptor.us_listed,
            provider=_RUNTIME_PROVIDER,
            feed=feed,
            input_sha256=feature_input_sha256(asset, benchmark, as_of=as_of),
        )

    def _ensure_subscribed(self, symbol: str) -> None:
        history = getattr(self._bars, "request_history", None)
        realtime = getattr(self._bars, "subscribe_realtime", None)
        if callable(history):
            history(symbol)
        if callable(realtime):
            realtime(symbol)

    def _require_live_feed(self, symbol: str) -> None:
        status = self._bars.feed_status(symbol)
        if status is not FeedStatus.LIVE:
            raise IBKRMarketDataNotReady(f"{symbol} runtime bars are {status.value}, not live")

    def _load(self, symbol: str, *, start: datetime, end: datetime) -> tuple[Bar, ...]:
        loaded = tuple(self._bars.minute_bars(symbol, start=start, end=end))
        if not loaded:
            raise IBKRMarketDataNotReady(f"no runtime bars available for {symbol}")
        if any(bar.symbol.upper() != symbol for bar in loaded):
            raise IBKRMarketDataPayloadError(f"runtime bars for {symbol} contain another symbol")
        if any(bar.source is not DataSource.IBKR for bar in loaded):
            raise IBKRMarketDataPayloadError(
                "runtime features must not mix IBKR with historical research data"
            )
        if any(bar.timestamp > end + self._future_tolerance for bar in loaded):
            raise IBKRMarketDataPayloadError(f"runtime bars for {symbol} are from the future")
        by_timestamp: dict[datetime, Bar] = {}
        for bar in loaded:
            previous = by_timestamp.get(bar.timestamp)
            if previous is not None and previous != bar:
                raise IBKRMarketDataPayloadError(
                    f"contradictory runtime bars for {symbol} at {bar.timestamp.isoformat()}"
                )
            by_timestamp[bar.timestamp] = bar
        return tuple(by_timestamp[timestamp] for timestamp in sorted(by_timestamp))

    def _require_fresh(self, bars: tuple[Bar, ...], as_of: datetime) -> None:
        latest = bars[-1].timestamp
        if as_of - latest > self._max_bar_age:
            raise IBKRMarketDataNotReady(
                f"latest runtime bar for {bars[-1].symbol} is stale by "
                f"{(as_of - latest).total_seconds():.0f}s"
            )


def feature_input_sha256(
    asset: Sequence[Bar],
    benchmark: Sequence[Bar],
    *,
    as_of: datetime,
) -> str:
    """Content address of the exact bar set a runtime decision was computed from."""

    return canonical_hash(
        {
            "schema": _FEATURE_HASH_SCHEMA,
            "as_of": as_of.isoformat(),
            "asset": [bar.model_dump(mode="json") for bar in asset],
            "benchmark": [bar.model_dump(mode="json") for bar in benchmark],
        }
    )


def _single_feed(bars: Sequence[Bar]) -> str:
    feeds = {bar.feed.strip().casefold() for bar in bars}
    if len(feeds) != 1:
        raise IBKRMarketDataPayloadError("runtime bars must all come from one feed")
    feed = feeds.pop()
    if not feed:
        raise IBKRMarketDataPayloadError("runtime bars require an explicit feed label")
    return feed


__all__ = [
    "BENCHMARK_SYMBOL",
    "IBKRBarBackend",
    "IBKRLiveFeatureProvider",
    "SecurityReference",
    "StaticSecurityDescriptor",
    "StaticSecurityReference",
    "feature_input_sha256",
]
