from datetime import timedelta
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from event_trader.domain import (
    Direction,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Position,
)
from event_trader.risk import RiskEngine, pending_entry_exposures
from event_trader.risk_halt import InMemoryRiskHaltGuard
from event_trader.strategy import ContinuationStrategy


def test_approved_position_respects_risk_and_notional_caps(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    decision = RiskEngine().assess(signal, empty_portfolio, snapshot.market, decision_time)
    assert decision.approved
    assert decision.notional <= empty_portfolio.nav * Decimal("0.15")
    risk = Decimal(decision.quantity) * abs(signal.entry_limit - signal.stop_price)
    assert risk <= empty_portfolio.nav * Decimal("0.005")


def test_daily_loss_and_reconciliation_block_order(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    payload = empty_portfolio.model_dump()
    payload.update(strategy_realized_pnl_today=Decimal("-1500"), reconciled=False)
    portfolio = PortfolioState.model_validate(payload)
    decision = RiskEngine().assess(signal, portfolio, snapshot.market, decision_time)
    assert not decision.approved
    assert "DAILY_LOSS_LIMIT" in decision.reason_codes
    assert "POSITION_MISMATCH" in decision.reason_codes


def test_loss_limit_latches_until_manual_reset(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    guard = InMemoryRiskHaltGuard()
    engine = RiskEngine(halt_guard=guard)
    breached = empty_portfolio.model_copy(update={"strategy_realized_pnl_today": Decimal("-1500")})

    first = engine.assess(signal, breached, snapshot.market, decision_time)
    recovered = engine.assess(signal, empty_portfolio, snapshot.market, decision_time)

    assert "DAILY_LOSS_LIMIT" in first.reason_codes
    assert "RISK_HALT_LATCHED" in recovered.reason_codes
    guard.manual_reset()
    assert engine.assess(signal, empty_portfolio, snapshot.market, decision_time).approved


def test_missing_strategy_ledger_fails_closed_without_false_loss_trip(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    guard = InMemoryRiskHaltGuard()
    engine = RiskEngine(halt_guard=guard)
    portfolio = PortfolioState(
        as_of=empty_portfolio.as_of,
        nav=empty_portfolio.nav,
        peak_nav=empty_portfolio.peak_nav,
        cash=empty_portfolio.cash,
    )

    decision = engine.assess(signal, portfolio, snapshot.market, decision_time)

    assert not decision.approved
    assert "STRATEGY_STATE_MISSING" in decision.reason_codes
    assert "DAILY_LOSS_LIMIT" not in decision.reason_codes
    assert not guard.is_halted()


def test_expired_signal_is_blocked(snapshot, long_insight, empty_portfolio, decision_time) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    decision = RiskEngine().assess(
        signal,
        empty_portfolio,
        snapshot.market,
        signal.expires_at + timedelta(seconds=1),
    )
    assert not decision.approved
    assert decision.quantity == 0


def test_risk_engine_cannot_be_configured_above_registered_limits() -> None:
    with pytest.raises(ValueError, match="risk_per_trade"):
        RiskEngine(risk_per_trade=Decimal("0.006"))
    with pytest.raises(ValueError, match="max_positions"):
        RiskEngine(max_positions=6)
    with pytest.raises(ValueError, match="max_gross_exposure"):
        RiskEngine(max_gross_exposure=Decimal("0.76"))
    with pytest.raises(ValueError, match="strategy_nav"):
        RiskEngine(strategy_nav=Decimal("100001"))


def test_pending_orders_count_toward_exposure_and_duplicate_symbol(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    intent = OrderIntent(
        order_id="pending-aapl",
        idempotency_key="pending-aapl",
        signal_id="prior-signal",
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=100,
        limit_price=Decimal("100"),
        created_at=decision_time,
    )
    portfolio = empty_portfolio.model_copy(
        update={"pending_orders": pending_entry_exposures((intent,))}
    )

    decision = RiskEngine().assess(signal, portfolio, snapshot.market, decision_time)

    assert not decision.approved
    assert "DUPLICATE_SYMBOL_POSITION" in decision.reason_codes


def test_shadow_intents_never_reserve_broker_exposure(decision_time) -> None:
    intent = OrderIntent(
        order_id="shadow-aapl",
        idempotency_key="shadow-aapl",
        signal_id="shadow-signal",
        account_id="DU123456",
        submission_mode="shadow",
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=100,
        limit_price=Decimal("100"),
        created_at=decision_time,
    )

    assert pending_entry_exposures((intent,)) == ()


@given(
    nav=st.integers(min_value=10_000, max_value=1_000_000),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_approved_notional_never_exceeds_symbol_limit(
    nav, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    payload = empty_portfolio.model_dump()
    payload.update(nav=Decimal(nav), peak_nav=Decimal(nav), cash=Decimal(nav))
    portfolio = PortfolioState.model_validate(payload)
    decision = RiskEngine().assess(signal, portfolio, snapshot.market, decision_time)
    if decision.approved:
        assert decision.notional <= Decimal(nav) * Decimal("0.15")


@given(
    nav=st.integers(min_value=10_000, max_value=1_000_000),
    existing_quantity=st.integers(min_value=0, max_value=7_000),
    existing_is_long=st.booleans(),
    realized_loss_bps=st.integers(min_value=0, max_value=300),
    drawdown_bps=st.integers(min_value=0, max_value=1_000),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_approval_never_violates_portfolio_limits(
    nav,
    existing_quantity,
    existing_is_long,
    realized_loss_bps,
    drawdown_bps,
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    nav_decimal = Decimal(nav)
    positions = (
        (
            Position(
                symbol="MSFT",
                direction=Direction.LONG if existing_is_long else Direction.SHORT,
                quantity=existing_quantity,
                market_price=Decimal("100"),
                average_price=Decimal("100"),
            ),
        )
        if existing_quantity
        else ()
    )
    strategy_equity = min(nav_decimal, Decimal("100000"))
    strategy_peak = (
        strategy_equity / (Decimal("1") - Decimal(drawdown_bps) / Decimal("10000"))
    ).quantize(Decimal("0.01"))
    portfolio = PortfolioState(
        as_of=empty_portfolio.as_of,
        nav=nav_decimal,
        peak_nav=nav_decimal,
        cash=nav_decimal,
        positions=positions,
        strategy_equity=strategy_equity,
        strategy_peak_equity=strategy_peak,
        strategy_realized_pnl_today=-(
            strategy_equity * Decimal(realized_loss_bps) / Decimal("10000")
        ),
        strategy_unrealized_pnl=Decimal("0"),
    )

    decision = RiskEngine().assess(signal, portfolio, snapshot.market, decision_time)

    if decision.approved:
        gross = sum((position.notional for position in positions), Decimal("0"))
        net = sum(
            (
                position.notional if position.direction is Direction.LONG else -position.notional
                for position in positions
            ),
            Decimal("0"),
        )
        assert len(positions) + 1 <= 5
        assert decision.notional <= strategy_equity * Decimal("0.15")
        assert gross + decision.notional <= strategy_equity * Decimal("0.75")
        assert abs(net + decision.notional) <= strategy_equity * Decimal("0.40")
        assert portfolio.strategy_realized_pnl_today > -(strategy_equity * Decimal("0.015"))
        assert (strategy_peak - strategy_equity) / strategy_peak < Decimal("0.05")
