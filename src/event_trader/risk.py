"""Fail-closed position sizing and portfolio risk rules."""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_FLOOR, Decimal

from .domain import (
    Direction,
    ExecutionReport,
    ExecutionStatus,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    PendingOrderExposure,
    PortfolioState,
    RiskDecision,
    Signal,
)
from .risk_halt import InMemoryRiskHaltGuard, RiskHaltGuard


class RiskEngine:
    def __init__(
        self,
        *,
        risk_per_trade: Decimal = Decimal("0.005"),
        max_positions: int = 5,
        max_symbol_notional: Decimal = Decimal("0.15"),
        max_gross_exposure: Decimal = Decimal("0.75"),
        max_abs_net_exposure: Decimal = Decimal("0.40"),
        max_daily_loss: Decimal = Decimal("0.015"),
        max_drawdown: Decimal = Decimal("0.05"),
        strategy_nav: Decimal = Decimal("100000"),
        halt_guard: RiskHaltGuard | None = None,
    ) -> None:
        if not Decimal("0") < risk_per_trade <= Decimal("0.005"):
            raise ValueError("risk_per_trade must be in (0, 0.005]")
        if not 1 <= max_positions <= 5:
            raise ValueError("max_positions must be in [1, 5]")
        for name, value, maximum in (
            ("max_symbol_notional", max_symbol_notional, Decimal("0.15")),
            ("max_gross_exposure", max_gross_exposure, Decimal("0.75")),
            ("max_abs_net_exposure", max_abs_net_exposure, Decimal("0.40")),
            ("max_daily_loss", max_daily_loss, Decimal("0.015")),
            ("max_drawdown", max_drawdown, Decimal("0.05")),
        ):
            if not Decimal("0") < value <= maximum:
                raise ValueError(f"{name} must be in (0, {maximum}]")
        if not Decimal("0") < strategy_nav <= Decimal("100000"):
            raise ValueError("strategy_nav must be in (0, 100000]")
        self.risk_per_trade = risk_per_trade
        self.max_positions = max_positions
        self.max_symbol_notional = max_symbol_notional
        self.max_gross_exposure = max_gross_exposure
        self.max_abs_net_exposure = max_abs_net_exposure
        self.max_daily_loss = max_daily_loss
        self.max_drawdown = max_drawdown
        self.strategy_nav = strategy_nav
        self.halt_guard = halt_guard or InMemoryRiskHaltGuard()

    def assess(
        self,
        signal: Signal,
        portfolio: PortfolioState,
        market: MarketSnapshot,
        now: datetime,
    ) -> RiskDecision:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("risk decision time must be timezone-aware")
        reasons: list[str] = []
        if self.halt_guard.is_halted():
            reasons.append("RISK_HALT_LATCHED")
        if now >= signal.expires_at:
            reasons.append("SIGNAL_EXPIRED")
        if not portfolio.broker_connected:
            reasons.append("BROKER_DISCONNECTED")
        if not portfolio.reconciled:
            reasons.append("POSITION_MISMATCH")
        if not market.data_fresh:
            reasons.append("MARKET_DATA_STALE")
        if market.halted:
            reasons.append("SYMBOL_HALTED")
        if any(position.symbol == signal.symbol for position in portfolio.positions) or any(
            order.symbol == signal.symbol for order in portfolio.pending_orders
        ):
            reasons.append("DUPLICATE_SYMBOL_POSITION")
        if len(portfolio.positions) + len(portfolio.pending_orders) >= self.max_positions:
            reasons.append("MAX_POSITIONS")

        strategy_state_available = all(
            value is not None
            for value in (
                portfolio.strategy_equity,
                portfolio.strategy_peak_equity,
                portfolio.strategy_realized_pnl_today,
                portfolio.strategy_unrealized_pnl,
            )
        )
        if not strategy_state_available:
            reasons.append("STRATEGY_STATE_MISSING")
        strategy_equity = portfolio.strategy_equity or Decimal("0")
        strategy_peak = portfolio.strategy_peak_equity or Decimal("1")
        risk_nav = min(self.strategy_nav, portfolio.nav, strategy_equity)
        if strategy_state_available:
            daily_pnl = (portfolio.strategy_realized_pnl_today or Decimal("0")) + (
                portfolio.strategy_unrealized_pnl or Decimal("0")
            )
            if daily_pnl <= -(risk_nav * self.max_daily_loss):
                reasons.append("DAILY_LOSS_LIMIT")
                self.halt_guard.trip(reason="DAILY_LOSS_LIMIT", at=now)
            drawdown = (strategy_peak - strategy_equity) / strategy_peak
            if drawdown >= self.max_drawdown:
                reasons.append("DRAWDOWN_LIMIT")
                self.halt_guard.trip(reason="DRAWDOWN_LIMIT", at=now)
        if signal.direction is Direction.SHORT:
            if not market.shortable or market.shortable_shares <= 0:
                reasons.append("NOT_SHORTABLE")

        stop_distance = abs(signal.entry_limit - signal.stop_price)
        if stop_distance <= 0:
            reasons.append("INVALID_STOP_DISTANCE")
            risk_quantity = 0
        else:
            risk_quantity = int(
                (risk_nav * self.risk_per_trade / stop_distance).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )

        symbol_cap = int(
            (risk_nav * self.max_symbol_notional / signal.entry_limit).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
        quantity = max(0, min(risk_quantity, symbol_cap))
        if signal.direction is Direction.SHORT:
            quantity = min(quantity, market.shortable_shares)
        if quantity <= 0:
            reasons.append("POSITION_SIZE_ZERO")

        candidate_notional = signal.entry_limit * quantity
        gross = sum((position.notional for position in portfolio.positions), Decimal("0"))
        gross += sum((order.notional for order in portfolio.pending_orders), Decimal("0"))
        net = sum(
            (
                position.notional if position.direction is Direction.LONG else -position.notional
                for position in portfolio.positions
            ),
            Decimal("0"),
        )
        net += sum(
            (
                order.notional if order.direction is Direction.LONG else -order.notional
                for order in portfolio.pending_orders
            ),
            Decimal("0"),
        )
        signed_candidate = (
            candidate_notional if signal.direction is Direction.LONG else -candidate_notional
        )
        if gross + candidate_notional > risk_nav * self.max_gross_exposure:
            reasons.append("GROSS_EXPOSURE_LIMIT")
        if abs(net + signed_candidate) > risk_nav * self.max_abs_net_exposure:
            reasons.append("NET_EXPOSURE_LIMIT")

        approved = not reasons
        if not approved:
            quantity = 0
            candidate_notional = Decimal("0")
        return RiskDecision(
            signal_id=signal.signal_id,
            approved=approved,
            quantity=quantity,
            notional=candidate_notional,
            reason_codes=tuple(dict.fromkeys(reasons)) if reasons else ("APPROVED",),
            decided_at=now,
            limits={
                "risk_per_trade": float(self.risk_per_trade),
                "max_positions": float(self.max_positions),
                "max_symbol_notional": float(self.max_symbol_notional),
                "max_gross_exposure": float(self.max_gross_exposure),
                "max_abs_net_exposure": float(self.max_abs_net_exposure),
                "max_daily_loss": float(self.max_daily_loss),
                "max_drawdown": float(self.max_drawdown),
                "strategy_nav": float(self.strategy_nav),
                "effective_risk_nav": float(risk_nav),
            },
        )


def pending_entry_exposures(
    intents: tuple[OrderIntent, ...],
    reports: tuple[ExecutionReport, ...] = (),
) -> tuple[PendingOrderExposure, ...]:
    """Translate durable non-terminal entry intents into portfolio risk exposure."""

    exposures: list[PendingOrderExposure] = []
    reports_by_order = {report.order_id: report for report in reports}
    terminal = {
        ExecutionStatus.FILLED,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.REJECTED,
    }
    for intent in intents:
        if intent.submission_mode != "paper":
            continue
        report = reports_by_order.get(intent.order_id)
        if report is not None and report.status in terminal:
            continue
        if intent.side is OrderSide.BUY:
            direction = Direction.LONG
        elif intent.side is OrderSide.SELL_SHORT:
            direction = Direction.SHORT
        else:
            continue
        remaining_quantity = intent.quantity - (report.filled_quantity if report else 0)
        if remaining_quantity <= 0:
            continue
        exposures.append(
            PendingOrderExposure(
                order_id=intent.order_id,
                symbol=intent.symbol,
                direction=direction,
                notional=intent.limit_price * remaining_quantity,
            )
        )
    return tuple(exposures)
