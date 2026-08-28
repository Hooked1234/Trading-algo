from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from event_trader.broker import (
    BrokerReadiness,
    InMemoryPaperBroker,
    ReadinessCheck,
    ReadinessProfile,
    ReconciliationResult,
)
from event_trader.domain import (
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PortfolioState,
)
from event_trader.providers.ibkr import IBKRRecoveryIncomplete
from event_trader.startup import PaperRecoveryCoordinator, PaperStartupGate


async def test_startup_gate_reconciles_before_runtime(decision_time) -> None:
    broker = InMemoryPaperBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )

    class Store:
        def __init__(self):
            self.reports = []

        async def save_execution_report(self, report):
            self.reports.append(report)
            return True

    store = Store()
    restored = []

    async def restore():
        restored.append(True)

    gate = PaperStartupGate(
        broker=broker,
        store=store,
        clock=lambda: decision_time,
        restore=restore,
    )

    await gate()

    assert restored == [True]
    assert broker.readiness().ready


async def test_startup_gate_resamples_time_after_slow_reconciliation(decision_time) -> None:
    broker_ticks = iter(
        (
            decision_time,
            decision_time + timedelta(seconds=2),
            decision_time + timedelta(seconds=2),
        )
    )
    broker = InMemoryPaperBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: next(broker_ticks),
    )

    class Store:
        async def save_execution_report(self, _report):
            return True

    gate_ticks = iter((decision_time, decision_time + timedelta(seconds=3)))
    gate = PaperStartupGate(
        broker=broker,
        store=Store(),
        clock=lambda: next(gate_ticks),
    )

    await gate()


def _paper_intent(decision_time) -> OrderIntent:
    return OrderIntent(
        order_id="order-1",
        idempotency_key="signal-1:entry",
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


def _submitted(intent: OrderIntent) -> ExecutionReport:
    return ExecutionReport(
        order_id=intent.order_id,
        idempotency_key=intent.idempotency_key,
        status=ExecutionStatus.SUBMITTED,
        broker_order_id="42",
        occurred_at=intent.created_at,
    )


class _RecoveryBroker:
    account_id = "DU123456"

    def __init__(self, result: ReconciliationResult, events: list[str]) -> None:
        self.result = result
        self.events = events

    async def restore_from_storage(self, _store, *, max_orders: int = 1_000):
        self.events.append(f"restore:{max_orders}")
        return self.result.executions

    def readiness(
        self, profile: ReadinessProfile = ReadinessProfile.SUBMIT
    ) -> BrokerReadiness:
        self.events.append(f"readiness:{profile.value}")
        return BrokerReadiness(
            account_id=self.account_id,
            checked_at=self.result.reconciled_at,
            checks=(ReadinessCheck("ready", True),),
        )

    def reconcile(self) -> ReconciliationResult:
        self.events.append("reconcile")
        return self.result


class _RecoveryStore:
    def __init__(self, intents: tuple[OrderIntent, ...], events: list[str]) -> None:
        self.intents = intents
        self.events = events
        self.reports: dict[str, ExecutionReport] = {}

    async def save_execution_fill(self, fill: ExecutionFill) -> bool:
        self.events.append(f"fill:{fill.execution_id}")
        return True

    async def save_execution_report(self, report: ExecutionReport) -> bool:
        self.events.append(f"report:{report.order_id}")
        self.reports[report.order_id] = report
        return True

    async def list_order_intents(self, *, limit: int = 1_000):
        self.events.append(f"intents:{limit}")
        return self.intents

    async def get_execution_report(self, order_id: str):
        self.events.append(f"get:{order_id}")
        return self.reports.get(order_id)


class _RecoveryExecution:
    def __init__(self, broker, ledger, events: list[str]) -> None:
        self.broker = broker
        self.ledger = ledger
        self.events = events

    async def resume_persisted_workflow(self, intent, _latest):
        self.events.append(f"resume:{intent.order_id}")
        terminal = self.broker.result.executions[0].model_copy(
            update={"status": ExecutionStatus.CANCELLED}
        )
        self.broker.result = replace(
            self.broker.result,
            executions=(terminal,),
        )
        return ()


async def test_recovery_coordinator_persists_broker_truth_before_resume(
    decision_time,
) -> None:
    events: list[str] = []
    intent = _paper_intent(decision_time)
    submitted = _submitted(intent)
    fill = ExecutionFill(
        order_id=intent.order_id,
        execution_id="exec-1",
        broker_order_id="42",
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=4,
        price=Decimal("100"),
        cumulative_quantity=4,
        occurred_at=decision_time,
    )
    result = ReconciliationResult(
        account_id="DU123456",
        reconciled_at=decision_time,
        executions=(submitted,),
        fills=(fill,),
        portfolio=PortfolioState(
            as_of=decision_time,
            nav=Decimal("100000"),
            peak_nav=Decimal("100000"),
            cash=Decimal("99600"),
        ),
    )
    broker = _RecoveryBroker(result, events)
    store = _RecoveryStore((intent,), events)
    execution = _RecoveryExecution(broker, store, events)
    coordinator = PaperRecoveryCoordinator(
        broker=broker,
        store=store,
        execution_service=execution,
        clock=lambda: decision_time,
    )

    recovered = await coordinator()

    assert recovered.resumed_order_ids == (intent.order_id,)
    first_reconcile = events.index("reconcile")
    assert first_reconcile < events.index("fill:exec-1")
    assert events.index("fill:exec-1") < events.index("report:order-1")
    assert events.index("report:order-1") < events.index("resume:order-1")
    assert events.count("readiness:reconcile") == 2
    assert "readiness:submit" not in events


async def test_recovery_coordinator_holds_an_unknown_submission(decision_time) -> None:
    events: list[str] = []
    intent = _paper_intent(decision_time)
    result = ReconciliationResult(
        account_id="DU123456",
        reconciled_at=decision_time,
        executions=(),
        portfolio=PortfolioState(
            as_of=decision_time,
            nav=Decimal("100000"),
            peak_nav=Decimal("100000"),
            cash=Decimal("100000"),
        ),
    )
    broker = _RecoveryBroker(result, events)
    store = _RecoveryStore((intent,), events)
    execution = _RecoveryExecution(broker, store, events)
    coordinator = PaperRecoveryCoordinator(
        broker=broker,
        store=store,
        execution_service=execution,
        clock=lambda: decision_time,
    )

    with pytest.raises(IBKRRecoveryIncomplete, match="no reconciled execution report"):
        await coordinator()

    assert not any(event.startswith("resume:") for event in events)
