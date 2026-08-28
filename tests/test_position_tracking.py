from decimal import Decimal

from event_trader.domain import (
    Direction,
    ExecutionReport,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    Position,
)
from event_trader.position_tracking import resolve_position_signals
from event_trader.strategy import ContinuationStrategy


def test_partial_entry_and_exit_fills_resolve_remaining_position(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    entry = OrderIntent(
        order_id="entry",
        idempotency_key="entry",
        signal_id=signal.signal_id,
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("100.10"),
        created_at=decision_time,
    )
    exit_order = entry.model_copy(
        update={
            "order_id": "exit",
            "idempotency_key": "exit",
            "side": OrderSide.SELL,
            "quantity": 4,
        }
    )
    entry_report = ExecutionReport(
        order_id="entry",
        idempotency_key="entry",
        status=ExecutionStatus.CANCELLED,
        filled_quantity=10,
        average_fill_price=Decimal("100.10"),
        occurred_at=decision_time,
    )
    exit_report = ExecutionReport(
        order_id="exit",
        idempotency_key="exit",
        status=ExecutionStatus.FILLED,
        filled_quantity=4,
        average_fill_price=Decimal("100.00"),
        occurred_at=decision_time,
    )
    portfolio = empty_portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol="AAPL",
                    direction=Direction.LONG,
                    quantity=6,
                    market_price=Decimal("100"),
                    average_price=Decimal("100.10"),
                ),
            )
        }
    )

    resolution = resolve_position_signals(
        portfolio=portfolio,
        signals=(signal,),
        intents=(entry, exit_order),
        reports=(entry_report, exit_report),
    )

    assert resolution.signals == (signal,)
    assert resolution.net_filled_by_signal[signal.signal_id] == 6
    assert not resolution.issues


def test_unowned_broker_position_fails_closed(empty_portfolio) -> None:
    portfolio = empty_portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol="MSFT",
                    direction=Direction.SHORT,
                    quantity=5,
                    market_price=Decimal("100"),
                    average_price=Decimal("100"),
                ),
            )
        }
    )

    resolution = resolve_position_signals(
        portfolio=portfolio,
        signals=(),
        intents=(),
        reports=(),
    )

    assert resolution.signals == ()
    assert resolution.issues == ("POSITION_SIGNAL_UNRESOLVED:MSFT",)


def test_shadow_fills_cannot_claim_a_broker_position(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    intent = OrderIntent(
        order_id="shadow",
        idempotency_key="shadow",
        signal_id=signal.signal_id,
        account_id="DU123456",
        submission_mode="shadow",
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("100"),
        created_at=decision_time,
    )
    report = ExecutionReport(
        order_id="shadow",
        idempotency_key="shadow",
        status=ExecutionStatus.FILLED,
        filled_quantity=10,
        average_fill_price=Decimal("100"),
        occurred_at=decision_time,
    )
    portfolio = empty_portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol="AAPL",
                    direction=Direction.LONG,
                    quantity=10,
                    market_price=Decimal("100"),
                    average_price=Decimal("100"),
                ),
            )
        }
    )

    resolution = resolve_position_signals(
        portfolio=portfolio,
        signals=(signal,),
        intents=(intent,),
        reports=(report,),
    )

    assert resolution.signals == ()
    assert resolution.issues == ("POSITION_SIGNAL_UNRESOLVED:AAPL",)
