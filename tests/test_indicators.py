from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from event_trader.domain import Bar, DataSource
from event_trader.indicators import (
    average_true_range,
    beta_adjusted_return_z,
    relative_volume,
    rolling_beta,
    session_vwap,
)


def _bar(index: int, close: str, volume: int = 100) -> Bar:
    value = Decimal(close)
    return Bar(
        symbol="TEST",
        timestamp=datetime(2026, 1, 2, 14, 30, tzinfo=UTC) + timedelta(minutes=5 * index),
        open=value,
        high=value + Decimal("1"),
        low=value - Decimal("1"),
        close=value,
        volume=volume,
        vwap=value,
        source=DataSource.REPLAY,
        feed="fixture",
    )


def test_session_vwap_is_volume_weighted() -> None:
    assert session_vwap([_bar(0, "10", 100), _bar(1, "20", 300)]) == Decimal("17.5")


def test_atr_uses_true_range() -> None:
    bars = [_bar(index, str(100 + index)) for index in range(15)]
    assert average_true_range(bars, periods=14) == Decimal("2")


def test_relative_volume_uses_median_slot() -> None:
    assert relative_volume(300, [90, 100, 110]) == 3.0


def test_beta_and_z_score_require_history() -> None:
    benchmark = [value / 10_000 for value in range(1, 21)]
    asset = [2 * value for value in benchmark]
    assert rolling_beta(asset, benchmark) == pytest.approx(2.0)
    z = beta_adjusted_return_z(
        asset_return=0.01,
        benchmark_return=0.002,
        beta=1.0,
        historical_abnormal_returns=[value / 10_000 for value in range(-10, 10)],
    )
    assert z > 1


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf")])
def test_beta_and_z_score_reject_non_finite_values(invalid: float) -> None:
    finite = [value / 10_000 for value in range(1, 21)]
    with pytest.raises(ValueError, match="finite"):
        rolling_beta([*finite[:-1], invalid], finite)
    with pytest.raises(ValueError, match="finite"):
        beta_adjusted_return_z(
            asset_return=invalid,
            benchmark_return=0.001,
            beta=1.0,
            historical_abnormal_returns=finite,
        )
