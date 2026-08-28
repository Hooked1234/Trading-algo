"""Deterministic point-in-time feature calculation shared by replay and live input."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from itertools import pairwise

import exchange_calendars as xcals
from pydantic import Field

from .domain import Bar, FrozenModel
from .indicators import (
    average_true_range,
    beta_adjusted_return_z,
    relative_volume,
    rolling_beta,
    session_vwap,
)
from .providers.ibkr_market import PrecomputedMarketFeatures


class PointInTimeReturn(FrozenModel):
    timestamp: datetime
    value: float


class PointInTimeVolume(FrozenModel):
    timestamp: datetime
    value: int = Field(ge=0)


@dataclass(frozen=True, slots=True)
class FeatureInputs:
    symbol: str
    session_one_minute_bars: Sequence[Bar]
    confirmation_bars: Sequence[Bar]
    atr_five_minute_bars: Sequence[Bar]
    previous_daily_bars: Sequence[Bar]
    same_slot_volumes: Sequence[PointInTimeVolume]
    asset_return_history: Sequence[PointInTimeReturn]
    spy_return_history: Sequence[PointInTimeReturn]
    spy_confirmation_return: PointInTimeReturn
    abnormal_return_history: Sequence[PointInTimeReturn]


_XNYS = xcals.get_calendar("XNYS")


def compute_market_features(inputs: FeatureInputs) -> PrecomputedMarketFeatures:
    """Calculate features from completed, point-in-time NYSE bars only.

    Intraday ``Bar.timestamp`` values are bar completion timestamps.  The one-minute
    sequence must cover the current session from the first completed minute through
    ``as_of``.  ATR input must be the exact latest fifteen completed five-minute bars.
    """

    symbol = inputs.symbol.strip().upper()
    if not symbol:
        raise ValueError("feature symbol must not be empty")
    required_bar_groups = (
        inputs.session_one_minute_bars,
        inputs.confirmation_bars,
        inputs.atr_five_minute_bars,
        inputs.previous_daily_bars,
    )
    if any(not group for group in required_bar_groups):
        raise ValueError("all feature bar groups require point-in-time data")
    if any(bar.symbol.upper() != symbol for group in required_bar_groups for bar in group):
        raise ValueError("feature bars must match the requested symbol")

    session = tuple(inputs.session_one_minute_bars)
    _require_strictly_increasing(session, "session one-minute bars")
    as_of = session[-1].timestamp
    expected_session = _completed_session_minute_ends(as_of)
    if tuple(bar.timestamp for bar in session) != expected_session:
        raise ValueError(
            "session one-minute bars must be complete and contiguous from the NYSE open"
        )

    confirmation = tuple(inputs.confirmation_bars)
    if len(confirmation) != 5:
        raise ValueError("confirmation requires exactly five completed one-minute bars")
    _require_strictly_increasing(confirmation, "confirmation bars")
    if confirmation != session[-5:]:
        raise ValueError("confirmation bars must be the final five contiguous session bars")

    atr_bars = tuple(inputs.atr_five_minute_bars)
    if len(atr_bars) != 15:
        raise ValueError("5-minute ATR requires exactly fifteen completed bars")
    _require_strictly_increasing(atr_bars, "5-minute ATR bars")
    expected_atr = _latest_five_minute_ends(as_of, count=15)
    if tuple(bar.timestamp for bar in atr_bars) != expected_atr:
        raise ValueError("5-minute ATR bars must be the latest complete NYSE bar history")

    daily = tuple(inputs.previous_daily_bars)
    if len(daily) != 20:
        raise ValueError("median dollar volume requires exactly 20 previous sessions")
    _require_strictly_increasing(daily, "previous daily bars")
    actual_daily_sessions = tuple(_market_date(bar.timestamp) for bar in daily)
    expected_daily_sessions = _previous_session_dates(_market_date(as_of), count=20)
    if actual_daily_sessions != expected_daily_sessions:
        raise ValueError("daily liquidity history must contain the exact 20 previous sessions")
    expected_history_ends = _previous_same_slot_ends(as_of, count=20)
    _require_timed_history(inputs.same_slot_volumes, expected_history_ends, "relative volume")
    _require_timed_history(
        inputs.asset_return_history,
        expected_history_ends,
        "asset return",
    )
    _require_timed_history(inputs.spy_return_history, expected_history_ends, "SPY return")
    _require_timed_history(
        inputs.abnormal_return_history,
        expected_history_ends,
        "abnormal return",
    )
    if inputs.spy_confirmation_return.timestamp != as_of:
        raise ValueError("SPY confirmation return must be timestamped at snapshot as_of")

    if any(bar.timestamp > as_of for group in required_bar_groups for bar in group):
        raise ValueError("feature inputs cannot contain look-ahead bars")

    first_price = confirmation[0].open
    last_price = confirmation[-1].close
    asset_return = float(last_price / first_price - Decimal("1"))
    beta = rolling_beta(
        [observation.value for observation in inputs.asset_return_history],
        [observation.value for observation in inputs.spy_return_history],
    )
    z_score = beta_adjusted_return_z(
        asset_return=asset_return,
        benchmark_return=inputs.spy_confirmation_return.value,
        beta=beta,
        historical_abnormal_returns=[
            observation.value for observation in inputs.abnormal_return_history
        ],
    )
    dollar_volumes = sorted(bar.close * bar.volume for bar in daily)
    median_dollar_volume = (dollar_volumes[9] + dollar_volumes[10]) / Decimal("2")
    current_volume = sum(bar.volume for bar in confirmation)
    return PrecomputedMarketFeatures(
        symbol=symbol,
        as_of=as_of,
        last=last_price,
        session_vwap=_money(session_vwap(inputs.session_one_minute_bars)),
        median_dollar_volume_20d=_money(median_dollar_volume),
        beta_adjusted_return_z=z_score,
        relative_volume=relative_volume(
            current_volume,
            [observation.value for observation in inputs.same_slot_volumes],
        ),
        atr_5m=_money(average_true_range(atr_bars)),
    )


def build_feature_inputs(
    *,
    symbol: str,
    symbol_one_minute_bars: Sequence[Bar],
    spy_one_minute_bars: Sequence[Bar],
    as_of: datetime,
) -> FeatureInputs:
    """Assemble the exact point-in-time feature contract from one-minute bars."""

    normalized = symbol.strip().upper()
    if not normalized or normalized == "SPY":
        raise ValueError("feature input builder requires a non-SPY event symbol")
    asset = _bar_index(symbol_one_minute_bars, symbol=normalized, as_of=as_of)
    benchmark = _bar_index(spy_one_minute_bars, symbol="SPY", as_of=as_of)

    session_ends = _completed_session_minute_ends(as_of)
    session = _bars_at(asset, session_ends, "current symbol session")
    confirmation = session[-5:]

    daily_bars: list[Bar] = []
    for session_date in _previous_session_dates(_market_date(as_of), count=20):
        opening, closing = _session_bounds(session_date)
        minute_count = int((closing - opening).total_seconds() // 60)
        minute_ends = tuple(
            opening + timedelta(minutes=minute)
            for minute in range(1, minute_count + 1)
        )
        daily_bars.append(_aggregate_bars(_bars_at(asset, minute_ends, "daily history")))

    same_slot_ends = _previous_same_slot_ends(as_of, count=20)
    asset_returns: list[PointInTimeReturn] = []
    spy_returns: list[PointInTimeReturn] = []
    same_slot_volumes: list[PointInTimeVolume] = []
    for timestamp in same_slot_ends:
        ends = tuple(timestamp - timedelta(minutes=offset) for offset in range(4, -1, -1))
        asset_window = _bars_at(asset, ends, "asset same-slot history")
        spy_window = _bars_at(benchmark, ends, "SPY same-slot history")
        asset_returns.append(
            PointInTimeReturn(timestamp=timestamp, value=_window_return(asset_window))
        )
        spy_returns.append(
            PointInTimeReturn(timestamp=timestamp, value=_window_return(spy_window))
        )
        same_slot_volumes.append(
            PointInTimeVolume(
                timestamp=timestamp,
                value=sum(bar.volume for bar in asset_window),
            )
        )

    beta = rolling_beta(
        [observation.value for observation in asset_returns],
        [observation.value for observation in spy_returns],
    )
    abnormal = tuple(
        PointInTimeReturn(
            timestamp=asset_observation.timestamp,
            value=asset_observation.value - beta * spy_observation.value,
        )
        for asset_observation, spy_observation in zip(
            asset_returns, spy_returns, strict=True
        )
    )

    atr_bars = tuple(
        _aggregate_bars(
            _bars_at(
                asset,
                tuple(end - timedelta(minutes=offset) for offset in range(4, -1, -1)),
                "5-minute ATR history",
            )
        )
        for end in _latest_five_minute_ends(as_of, count=15)
    )
    spy_confirmation = _bars_at(
        benchmark,
        tuple(bar.timestamp for bar in confirmation),
        "SPY confirmation",
    )
    return FeatureInputs(
        symbol=normalized,
        session_one_minute_bars=session,
        confirmation_bars=confirmation,
        atr_five_minute_bars=atr_bars,
        previous_daily_bars=tuple(daily_bars),
        same_slot_volumes=tuple(same_slot_volumes),
        asset_return_history=tuple(asset_returns),
        spy_return_history=tuple(spy_returns),
        spy_confirmation_return=PointInTimeReturn(
            timestamp=as_of,
            value=_window_return(spy_confirmation),
        ),
        abnormal_return_history=abnormal,
    )


def _bar_index(
    bars: Sequence[Bar],
    *,
    symbol: str,
    as_of: datetime,
) -> dict[datetime, Bar]:
    selected: dict[datetime, Bar] = {}
    source_feed: tuple[object, str] | None = None
    for bar in bars:
        if bar.symbol.strip().upper() != symbol:
            raise ValueError(f"one-minute history contains a non-{symbol} bar")
        if bar.timestamp > as_of:
            continue
        current_source_feed = (bar.source, bar.feed.casefold())
        if source_feed is None:
            source_feed = current_source_feed
        elif current_source_feed != source_feed:
            raise ValueError("one-minute history must use one provider/feed contract")
        previous = selected.get(bar.timestamp)
        if previous is not None and previous != bar:
            raise ValueError("one-minute history contains conflicting duplicate bars")
        selected[bar.timestamp] = bar
    if not selected:
        raise ValueError(f"one-minute history for {symbol} is empty at as_of")
    return selected


def _bars_at(
    indexed: dict[datetime, Bar],
    timestamps: Sequence[datetime],
    name: str,
) -> tuple[Bar, ...]:
    try:
        return tuple(indexed[timestamp] for timestamp in timestamps)
    except KeyError as exc:
        raise ValueError(f"{name} is incomplete at {exc.args[0].isoformat()}") from exc


def _aggregate_bars(bars: Sequence[Bar]) -> Bar:
    if not bars:
        raise ValueError("cannot aggregate an empty bar sequence")
    first, last = bars[0], bars[-1]
    volume = sum(bar.volume for bar in bars)
    weighted_value = sum(
        ((bar.vwap or bar.close) * bar.volume for bar in bars),
        Decimal("0"),
    )
    return Bar(
        symbol=first.symbol,
        timestamp=last.timestamp,
        open=first.open,
        high=max(bar.high for bar in bars),
        low=min(bar.low for bar in bars),
        close=last.close,
        volume=volume,
        vwap=_money(weighted_value / volume) if volume else last.close,
        source=first.source,
        feed=first.feed,
    )


def _window_return(bars: Sequence[Bar]) -> float:
    return float(bars[-1].close / bars[0].open - Decimal("1"))


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def _require_strictly_increasing(bars: Sequence[Bar], name: str) -> None:
    timestamps = tuple(bar.timestamp for bar in bars)
    if any(current <= previous for previous, current in pairwise(timestamps)):
        raise ValueError(f"{name} must be strictly chronological and unique")


def _require_timed_history(
    observations: Sequence[PointInTimeReturn | PointInTimeVolume],
    expected: tuple[datetime, ...],
    name: str,
) -> None:
    if tuple(observation.timestamp for observation in observations) != expected:
        raise ValueError(
            f"{name} history must contain the exact 20 previous NYSE same-slot observations"
        )


def _market_date(value: datetime) -> date:
    return value.astimezone(_XNYS.tz).date()


def _session_bounds(session_date: date) -> tuple[datetime, datetime]:
    label = session_date.isoformat()
    if not _XNYS.is_session(label):
        raise ValueError("feature timestamp must belong to an NYSE session")
    opening = _XNYS.session_open(label).to_pydatetime().astimezone(UTC)
    closing = _XNYS.session_close(label).to_pydatetime().astimezone(UTC)
    return opening, closing


def _completed_session_minute_ends(as_of: datetime) -> tuple[datetime, ...]:
    as_of_utc = as_of.astimezone(UTC)
    if as_of_utc.second or as_of_utc.microsecond:
        raise ValueError("feature timestamp must be aligned to a completed minute")
    opening, closing = _session_bounds(_market_date(as_of_utc))
    if not opening + timedelta(minutes=1) <= as_of_utc <= closing:
        raise ValueError("feature timestamp is outside regular NYSE trading hours")
    elapsed_minutes = int((as_of_utc - opening).total_seconds() // 60)
    return tuple(opening + timedelta(minutes=index) for index in range(1, elapsed_minutes + 1))


def _latest_five_minute_ends(as_of: datetime, *, count: int) -> tuple[datetime, ...]:
    if count <= 0:
        raise ValueError("five-minute history count must be positive")
    current_session = _market_date(as_of)
    upper_bound = as_of.astimezone(UTC)
    newest_first: list[datetime] = []
    while len(newest_first) < count:
        opening, closing = _session_bounds(current_session)
        effective_end = min(upper_bound, closing)
        completed = int((effective_end - opening).total_seconds() // 300)
        newest_first.extend(
            opening + timedelta(minutes=5 * index) for index in range(completed, 0, -1)
        )
        if len(newest_first) >= count:
            break
        previous = _XNYS.previous_session(current_session.isoformat())
        current_session = previous.date()
        upper_bound = _XNYS.session_close(previous).to_pydatetime().astimezone(UTC)
    return tuple(reversed(newest_first[:count]))


def _previous_session_dates(session_date: date, *, count: int) -> tuple[date, ...]:
    if count <= 0:
        raise ValueError("previous-session count must be positive")
    if not _XNYS.is_session(session_date.isoformat()):
        raise ValueError("feature timestamp must belong to an NYSE session")
    values: list[date] = []
    cursor = session_date.isoformat()
    for _ in range(count):
        previous = _XNYS.previous_session(cursor)
        values.append(previous.date())
        cursor = previous.isoformat()
    return tuple(reversed(values))


def _previous_same_slot_ends(as_of: datetime, *, count: int) -> tuple[datetime, ...]:
    session_date = _market_date(as_of)
    current_open, _ = _session_bounds(session_date)
    offset = as_of.astimezone(UTC) - current_open
    if offset <= timedelta(0):
        raise ValueError("feature as_of must follow the NYSE session open")
    return tuple(
        _session_bounds(previous)[0] + offset
        for previous in _previous_session_dates(session_date, count=count)
    )


__all__ = [
    "FeatureInputs",
    "PointInTimeReturn",
    "PointInTimeVolume",
    "build_feature_inputs",
    "compute_market_features",
]
