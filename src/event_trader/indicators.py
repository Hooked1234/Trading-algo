"""Pure indicator functions shared by historical and live paths."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

import numpy as np

from .domain import Bar


def session_vwap(bars: Sequence[Bar]) -> Decimal:
    total_volume = sum(bar.volume for bar in bars)
    if total_volume <= 0:
        raise ValueError("VWAP requires positive volume")
    weighted = sum((bar.vwap or bar.close) * bar.volume for bar in bars)
    return weighted / total_volume


def average_true_range(bars: Sequence[Bar], periods: int = 14) -> Decimal:
    if periods < 1:
        raise ValueError("periods must be positive")
    if len(bars) < periods + 1:
        raise ValueError("ATR requires periods + 1 bars")
    true_ranges: list[Decimal] = []
    for previous, current in zip(bars[-periods - 1 : -1], bars[-periods:], strict=True):
        true_ranges.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    return sum(true_ranges, Decimal("0")) / Decimal(periods)


def relative_volume(current_volume: int, same_slot_history: Sequence[int]) -> float:
    if current_volume < 0 or any(value < 0 for value in same_slot_history):
        raise ValueError("volume cannot be negative")
    if not same_slot_history:
        raise ValueError("relative volume requires history")
    median = float(np.median(np.asarray(same_slot_history, dtype=float)))
    if median <= 0:
        raise ValueError("historical median volume must be positive")
    return current_volume / median


def rolling_beta(asset_returns: Sequence[float], benchmark_returns: Sequence[float]) -> float:
    if len(asset_returns) != len(benchmark_returns) or len(asset_returns) < 20:
        raise ValueError("beta requires aligned history of at least 20 observations")
    asset = np.asarray(asset_returns, dtype=float)
    benchmark = np.asarray(benchmark_returns, dtype=float)
    if not np.isfinite(asset).all() or not np.isfinite(benchmark).all():
        raise ValueError("beta history must contain only finite returns")
    variance = float(np.var(benchmark, ddof=1))
    if variance <= 0:
        raise ValueError("benchmark variance must be positive")
    covariance = float(np.cov(asset, benchmark, ddof=1)[0, 1])
    beta = covariance / variance
    if not np.isfinite(beta):
        raise ValueError("beta must be finite")
    return beta


def beta_adjusted_return_z(
    *,
    asset_return: float,
    benchmark_return: float,
    beta: float,
    historical_abnormal_returns: Sequence[float],
) -> float:
    if len(historical_abnormal_returns) < 20:
        raise ValueError("z-score requires at least 20 historical windows")
    history = np.asarray(historical_abnormal_returns, dtype=float)
    current = np.asarray((asset_return, benchmark_return, beta), dtype=float)
    if not np.isfinite(current).all() or not np.isfinite(history).all():
        raise ValueError("z-score inputs must contain only finite returns")
    std = float(np.std(history, ddof=1))
    if std <= 0:
        raise ValueError("historical abnormal return volatility must be positive")
    abnormal = asset_return - beta * benchmark_return
    score = (abnormal - float(np.mean(history))) / std
    if not np.isfinite(score):
        raise ValueError("z-score must be finite")
    return score
