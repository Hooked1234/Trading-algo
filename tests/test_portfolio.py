from datetime import timedelta
from decimal import Decimal

import pytest

from event_trader.broker import InMemoryPaperBroker
from event_trader.domain import (
    Direction,
    ExecutionReport,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Position,
)
from event_trader.portfolio import (
    SQLiteStrategyLedger,
    StrategyPnLSynchronizer,
    StrategyPortfolioAssembler,
)
from event_trader.storage import SQLiteOperationalStore


def test_strategy_ledger_is_idempotent_and_resets_daily_pnl(tmp_path, decision_time) -> None:
    ledger = SQLiteStrategyLedger(
        tmp_path / "strategy.sqlite",
        strategy_id="sec-8k-continuation-v1",
    )
    ledger.initialize(at=decision_time)
    first = ledger.record_closed_trade(
        trade_id="trade-1", net_pnl=Decimal("125"), closed_at=decision_time
    )
    duplicate = ledger.record_closed_trade(
        trade_id="trade-1", net_pnl=Decimal("125"), closed_at=decision_time
    )
    next_day = ledger.mark_unrealized(pnl=Decimal("-25"), at=decision_time + timedelta(days=1))

    assert duplicate == first
    assert next_day.cumulative_realized_pnl == Decimal("125")
    assert next_day.realized_pnl_today == 0
    assert next_day.equity == Decimal("100100")
    ledger.close()


@pytest.mark.asyncio
async def test_portfolio_assembler_reserves_open_entry_exposure(tmp_path, decision_time) -> None:
    store = SQLiteOperationalStore(tmp_path / "state.sqlite", tmp_path / "raw")
    strategy = SQLiteStrategyLedger(
        tmp_path / "strategy.sqlite",
        strategy_id="sec-8k-continuation-v1",
    )
    strategy.initialize(at=decision_time)
    broker = InMemoryPaperBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )
    # The operational schema requires the referenced signal; this lightweight
    # store only needs to expose the list methods for this assembly test.
    intent = OrderIntent(
        order_id="entry-1",
        idempotency_key="event:AAPL:v1:DU123456:entry",
        signal_id="signal-1",
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("100"),
        created_at=decision_time,
    )

    class MemoryOrderStore:
        def __init__(self):
            self.reports = {}

        async def list_order_intents_since(self, since):
            del since
            return (intent,)

        async def list_execution_reports_since(self, since):
            del since
            return tuple(self.reports.values())

        async def save_execution_report(self, report):
            created = report.order_id not in self.reports
            self.reports[report.order_id] = report
            return created

    assembler = StrategyPortfolioAssembler(
        broker=broker,
        order_store=MemoryOrderStore(),
        strategy_ledger=strategy,
    )
    portfolio = await assembler.current(now=decision_time)

    assert portfolio.strategy_equity == Decimal("100000")
    assert portfolio.pending_orders[0].notional == Decimal("1000")
    store.close()
    strategy.close()


@pytest.mark.asyncio
async def test_portfolio_assembler_refreshes_strategy_mark(tmp_path, decision_time) -> None:
    strategy = SQLiteStrategyLedger(
        tmp_path / "strategy.sqlite",
        strategy_id="sec-8k-continuation-v1",
    )
    strategy.initialize(at=decision_time - timedelta(seconds=6))
    broker = InMemoryPaperBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )

    class EmptyOrderStore:
        async def list_order_intents_since(self, since):
            del since
            return ()

        async def list_execution_reports_since(self, since):
            del since
            return ()

        async def save_execution_report(self, report):
            del report
            return True

    assembler = StrategyPortfolioAssembler(
        broker=broker,
        order_store=EmptyOrderStore(),
        strategy_ledger=strategy,
    )

    portfolio = await assembler.current(now=decision_time)

    assert portfolio.strategy_equity == Decimal("100000")
    assert strategy.latest().as_of == decision_time
    strategy.close()


def test_pnl_synchronizer_records_realized_and_unrealized_once(tmp_path, decision_time) -> None:
    ledger = SQLiteStrategyLedger(
        tmp_path / "strategy.sqlite",
        strategy_id="sec-8k-continuation-v1",
    )
    synchronizer = StrategyPnLSynchronizer(ledger)
    entry = OrderIntent(
        order_id="entry-1",
        idempotency_key="event:AAPL:v1:DU123456:entry",
        signal_id="signal-1",
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("100"),
        created_at=decision_time,
    )
    exit_intent = OrderIntent(
        order_id="exit-1",
        idempotency_key="event:AAPL:v1:DU123456:exit",
        signal_id="signal-1",
        account_id="DU123456",
        submission_mode="paper",
        symbol="AAPL",
        side=OrderSide.SELL,
        quantity=4,
        limit_price=Decimal("102"),
        created_at=decision_time + timedelta(minutes=5),
    )
    reports = (
        ExecutionReport(
            order_id="entry-1",
            idempotency_key="event:AAPL:v1:DU123456:entry",
            status=ExecutionStatus.FILLED,
            filled_quantity=10,
            average_fill_price=Decimal("100"),
            fees=Decimal("1"),
            occurred_at=decision_time + timedelta(seconds=1),
        ),
        ExecutionReport(
            order_id="exit-1",
            idempotency_key="event:AAPL:v1:DU123456:exit",
            status=ExecutionStatus.FILLED,
            filled_quantity=4,
            average_fill_price=Decimal("102"),
            fees=Decimal("0.4"),
            occurred_at=decision_time + timedelta(minutes=5, seconds=1),
        ),
    )
    portfolio = PortfolioState(
        nav=Decimal("100000"),
        peak_nav=Decimal("100000"),
        cash=Decimal("99400"),
        positions=(
            Position(
                symbol="AAPL",
                direction=Direction.LONG,
                quantity=6,
                market_price=Decimal("101"),
                average_price=Decimal("100"),
            ),
        ),
        as_of=decision_time + timedelta(minutes=6),
    )

    first = synchronizer.synchronize(
        intents=(entry, exit_intent),
        reports=reports,
        portfolio=portfolio,
        now=decision_time + timedelta(minutes=6),
    )
    replay = synchronizer.synchronize(
        intents=(entry, exit_intent),
        reports=reports,
        portfolio=portfolio,
        now=decision_time + timedelta(minutes=6, seconds=1),
    )

    assert first.realized_pnl_today == Decimal("7.2")
    assert first.unrealized_pnl == Decimal("5.4")
    assert first.equity == Decimal("100012.6")
    assert replay.cumulative_realized_pnl == Decimal("7.2")
    assert replay.equity == Decimal("100012.6")
    ledger.close()


def test_pnl_synchronizer_rejects_unowned_broker_position(tmp_path, decision_time) -> None:
    ledger = SQLiteStrategyLedger(
        tmp_path / "strategy.sqlite",
        strategy_id="sec-8k-continuation-v1",
    )
    synchronizer = StrategyPnLSynchronizer(ledger)
    portfolio = PortfolioState(
        nav=Decimal("100000"),
        peak_nav=Decimal("100000"),
        cash=Decimal("99900"),
        positions=(
            Position(
                symbol="AAPL",
                direction=Direction.LONG,
                quantity=1,
                market_price=Decimal("100"),
                average_price=Decimal("100"),
            ),
        ),
        as_of=decision_time,
    )

    with pytest.raises(RuntimeError, match="unowned strategy exposure"):
        synchronizer.synchronize(
            intents=(),
            reports=(),
            portfolio=portfolio,
            now=decision_time,
        )
    ledger.close()
