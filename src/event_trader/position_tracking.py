"""Reconstruct which persisted signal owns each reconciled broker position."""

from __future__ import annotations

from collections import defaultdict

from pydantic import Field

from .domain import (
    ExecutionReport,
    FrozenModel,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Signal,
)


class PositionSignalResolution(FrozenModel):
    signals: tuple[Signal, ...]
    issues: tuple[str, ...] = ()
    net_filled_by_signal: dict[str, int] = Field(default_factory=dict)


def resolve_position_signals(
    *,
    portfolio: PortfolioState,
    signals: tuple[Signal, ...],
    intents: tuple[OrderIntent, ...],
    reports: tuple[ExecutionReport, ...],
) -> PositionSignalResolution:
    """Match positions only when durable cumulative fills reconcile exactly."""

    signals_by_id = {signal.signal_id: signal for signal in signals}
    if len(signals_by_id) != len(signals):
        raise ValueError("signals must have unique ids")
    reports_by_order = {report.order_id: report for report in reports}
    if len(reports_by_order) != len(reports):
        raise ValueError("execution reports must have unique order ids")
    filled_by_signal: dict[str, int] = defaultdict(int)
    for intent in intents:
        if intent.submission_mode != "paper":
            continue
        report = reports_by_order.get(intent.order_id)
        if report is None or report.filled_quantity <= 0:
            continue
        if intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT}:
            filled_by_signal[intent.signal_id] += report.filled_quantity
        elif intent.side in {OrderSide.SELL, OrderSide.BUY_TO_COVER}:
            filled_by_signal[intent.signal_id] -= report.filled_quantity

    resolved: list[Signal] = []
    issues: list[str] = []
    for position in portfolio.positions:
        candidates = [
            signal
            for signal_id, quantity in filled_by_signal.items()
            if quantity == position.quantity
            and (signal := signals_by_id.get(signal_id)) is not None
            and signal.symbol.upper() == position.symbol.upper()
            and signal.direction is position.direction
        ]
        if len(candidates) == 1:
            resolved.append(candidates[0])
        elif not candidates:
            issues.append(f"POSITION_SIGNAL_UNRESOLVED:{position.symbol.upper()}")
        else:
            issues.append(f"POSITION_SIGNAL_AMBIGUOUS:{position.symbol.upper()}")
    return PositionSignalResolution(
        signals=tuple(resolved),
        issues=tuple(issues),
        net_filled_by_signal=dict(filled_by_signal),
    )


__all__ = ["PositionSignalResolution", "resolve_position_signals"]
