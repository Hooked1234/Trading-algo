from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import exchange_calendars as xcals
import pytest

from event_trader.domain import Bar, DataSource
from event_trader.features import (
    FeatureInputs,
    PointInTimeReturn,
    PointInTimeVolume,
    build_feature_inputs,
    compute_market_features,
)

_XNYS = xcals.get_calendar("XNYS")


def _bars(
    symbol: str,
    timestamps: tuple[datetime, ...],
    *,
    step: str = "0.10",
    volume: int = 1000,
) -> tuple[Bar, ...]:
    records = []
    price = Decimal("100")
    increment = Decimal(step)
    for timestamp in timestamps:
        close = price + increment
        records.append(
            Bar(
                symbol=symbol,
                timestamp=timestamp,
                open=price,
                high=max(price, close) + Decimal("0.05"),
                low=min(price, close) - Decimal("0.05"),
                close=close,
                volume=volume,
                vwap=(price + close) / Decimal("2"),
                source=DataSource.REPLAY,
                feed="sip",
            )
        )
        price = close
    return tuple(records)


def _previous_sessions(decision_time: datetime, count: int) -> tuple[object, ...]:
    cursor: object = decision_time.date().isoformat()
    sessions = []
    for _ in range(count):
        cursor = _XNYS.previous_session(cursor)
        sessions.append(cursor)
    return tuple(reversed(sessions))


def _valid_inputs(decision_time: datetime) -> FeatureInputs:
    session_label = decision_time.date().isoformat()
    opening = _XNYS.session_open(session_label).to_pydatetime().astimezone(UTC)
    session_timestamps = tuple(opening + timedelta(minutes=minute) for minute in range(1, 16))
    session = _bars("AAPL", session_timestamps)

    previous_session = _XNYS.previous_session(session_label)
    previous_close = _XNYS.session_close(previous_session).to_pydatetime().astimezone(UTC)
    prior_atr_timestamps = tuple(
        previous_close - timedelta(minutes=5 * offset) for offset in range(11, -1, -1)
    )
    current_atr_timestamps = tuple(opening + timedelta(minutes=minute) for minute in (5, 10, 15))
    atr = _bars("AAPL", prior_atr_timestamps + current_atr_timestamps)

    daily_timestamps = tuple(
        _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
        for session in _previous_sessions(decision_time, 20)
    )
    daily = _bars("AAPL", daily_timestamps, volume=250_000)
    history = [0.001 * ((index % 5) - 2) for index in range(20)]
    spy = [0.0005 * ((index % 5) - 2) for index in range(20)]
    same_slot_timestamps = tuple(
        _XNYS.session_open(session).to_pydatetime().astimezone(UTC) + (decision_time - opening)
        for session in _previous_sessions(decision_time, 20)
    )
    return FeatureInputs(
        symbol="AAPL",
        session_one_minute_bars=session,
        confirmation_bars=session[-5:],
        atr_five_minute_bars=atr,
        previous_daily_bars=daily,
        same_slot_volumes=[
            PointInTimeVolume(timestamp=timestamp, value=1000) for timestamp in same_slot_timestamps
        ],
        asset_return_history=[
            PointInTimeReturn(timestamp=timestamp, value=value)
            for timestamp, value in zip(same_slot_timestamps, history, strict=True)
        ],
        spy_return_history=[
            PointInTimeReturn(timestamp=timestamp, value=value)
            for timestamp, value in zip(same_slot_timestamps, spy, strict=True)
        ],
        spy_confirmation_return=PointInTimeReturn(
            timestamp=decision_time,
            value=0.001,
        ),
        abnormal_return_history=[
            PointInTimeReturn(
                timestamp=timestamp,
                value=0.001 * ((index % 7) - 3),
            )
            for index, timestamp in enumerate(same_slot_timestamps)
        ],
    )


def _raw_one_minute_history(
    decision_time: datetime,
) -> tuple[tuple[Bar, ...], tuple[Bar, ...]]:
    sessions = (*_previous_sessions(decision_time, 20), decision_time.date().isoformat())
    asset: list[Bar] = []
    spy: list[Bar] = []
    for session_index, session in enumerate(sessions):
        opening = _XNYS.session_open(session).to_pydatetime().astimezone(UTC)
        closing = _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
        full_minutes = int((closing - opening).total_seconds() // 60)
        minute_count = 15 if session_index == 20 else full_minutes
        spy_step = Decimal("0.0005") * Decimal(session_index + 1)
        asset_step = Decimal("0.0008") * Decimal(session_index + 1) + Decimal("0.0001") * Decimal(
            session_index % 3
        )
        for minute in range(1, minute_count + 1):
            timestamp = opening + timedelta(minutes=minute)
            for symbol, base, step, target in (
                ("AAPL", Decimal("100"), asset_step, asset),
                ("SPY", Decimal("400"), spy_step, spy),
            ):
                open_price = base + step * Decimal(minute - 1)
                close_price = open_price + step
                target.append(
                    Bar(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=open_price,
                        high=close_price + Decimal("0.01"),
                        low=open_price - Decimal("0.01"),
                        close=close_price,
                        volume=(10_000 if session_index == 20 else 1_000 + session_index),
                        vwap=(open_price + close_price) / Decimal("2"),
                        source=DataSource.ALPACA_SIP,
                        feed="sip",
                    )
                )
    return tuple(asset), tuple(spy)


def test_feature_engine_builds_complete_point_in_time_inputs(decision_time) -> None:
    inputs = _valid_inputs(decision_time)

    features = compute_market_features(inputs)

    assert features.symbol == "AAPL"
    assert features.as_of == decision_time
    assert features.relative_volume == 5
    assert features.median_dollar_volume_20d > Decimal("20000000")
    assert features.atr_5m > 0


def test_feature_input_builder_assembles_raw_minute_history_without_lookahead(
    decision_time,
) -> None:
    asset, spy = _raw_one_minute_history(decision_time)
    future = asset[-1].model_copy(
        update={
            "timestamp": decision_time + timedelta(minutes=1),
            "open": Decimal("999"),
            "high": Decimal("1001"),
            "low": Decimal("998"),
            "close": Decimal("1000"),
        }
    )

    baseline = build_feature_inputs(
        symbol="AAPL",
        symbol_one_minute_bars=asset,
        spy_one_minute_bars=spy,
        as_of=decision_time,
    )
    with_future = build_feature_inputs(
        symbol="AAPL",
        symbol_one_minute_bars=(*asset, future),
        spy_one_minute_bars=spy,
        as_of=decision_time,
    )

    assert with_future == baseline
    assert len(baseline.previous_daily_bars) == 20
    assert len(baseline.atr_five_minute_bars) == 15
    assert compute_market_features(baseline).as_of == decision_time


def test_feature_input_builder_rejects_missing_same_slot_minute(decision_time) -> None:
    asset, spy = _raw_one_minute_history(decision_time)
    missing_timestamp = _XNYS.session_open(
        _previous_sessions(decision_time, 20)[0]
    ).to_pydatetime().astimezone(UTC) + timedelta(minutes=13)

    with pytest.raises(ValueError, match="incomplete"):
        build_feature_inputs(
            symbol="AAPL",
            symbol_one_minute_bars=tuple(
                bar for bar in asset if bar.timestamp != missing_timestamp
            ),
            spy_one_minute_bars=spy,
            as_of=decision_time,
        )


def test_feature_engine_rejects_symbol_mismatch(decision_time) -> None:
    inputs = replace(_valid_inputs(decision_time), symbol="MSFT")

    with pytest.raises(ValueError, match="match"):
        compute_market_features(inputs)


def test_feature_engine_rejects_incomplete_session_history(decision_time) -> None:
    inputs = _valid_inputs(decision_time)
    incomplete = replace(
        inputs,
        session_one_minute_bars=(
            *inputs.session_one_minute_bars[:7],
            *inputs.session_one_minute_bars[8:],
        ),
    )

    with pytest.raises(ValueError, match="complete and contiguous"):
        compute_market_features(incomplete)


def test_feature_engine_requires_exact_final_five_confirmation_bars(decision_time) -> None:
    inputs = _valid_inputs(decision_time)
    stale_confirmation = replace(
        inputs,
        confirmation_bars=inputs.session_one_minute_bars[-6:-1],
    )

    with pytest.raises(ValueError, match="final five"):
        compute_market_features(stale_confirmation)


def test_feature_engine_requires_exact_five_minute_atr_history(decision_time) -> None:
    inputs = _valid_inputs(decision_time)
    bad_last_bar = inputs.atr_five_minute_bars[-1].model_copy(
        update={"timestamp": decision_time + timedelta(minutes=1)}
    )
    invalid_atr = replace(
        inputs,
        atr_five_minute_bars=(*inputs.atr_five_minute_bars[:-1], bad_last_bar),
    )

    with pytest.raises(ValueError, match="latest complete"):
        compute_market_features(invalid_atr)


def test_feature_engine_requires_exact_previous_twenty_sessions(decision_time) -> None:
    inputs = _valid_inputs(decision_time)
    older_sessions = _previous_sessions(decision_time, 21)[:20]
    stale_daily = _bars(
        "AAPL",
        tuple(
            _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
            for session in older_sessions
        ),
        volume=250_000,
    )

    with pytest.raises(ValueError, match="exact 20 previous sessions"):
        compute_market_features(replace(inputs, previous_daily_bars=stale_daily))


def test_feature_engine_requires_twenty_same_slot_observations(decision_time) -> None:
    valid = _valid_inputs(decision_time)
    inputs = replace(valid, same_slot_volumes=valid.same_slot_volumes[:-1])

    with pytest.raises(ValueError, match="exact 20 previous"):
        compute_market_features(inputs)


def test_feature_engine_rejects_lookahead_in_return_history(decision_time) -> None:
    valid = _valid_inputs(decision_time)
    future = valid.asset_return_history[-1].model_copy(
        update={"timestamp": decision_time + timedelta(minutes=1)}
    )

    with pytest.raises(ValueError, match="exact 20 previous"):
        compute_market_features(
            replace(
                valid,
                asset_return_history=(*valid.asset_return_history[:-1], future),
            )
        )
