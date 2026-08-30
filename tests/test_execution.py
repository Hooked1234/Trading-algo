from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from event_trader.broker import (
    BrokerReadiness,
    ReadinessCheck,
    ReadinessProfile,
    ReconciliationResult,
)
from event_trader.domain import (
    Direction,
    ExecutionReport,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Position,
)
from event_trader.execution import (
    ExecutionReconciliationError,
    NonPaperIntentRejected,
    OrderIntentClaimLost,
    PaperExecutionService,
    PreSubmitGuardRejected,
    ReplacementSafetyError,
)
from event_trader.providers.ibkr import (
    IBKRBrokerAdapter,
    IBKRRemoteOrderSnapshot,
    NativeIBAPIBackend,
)

NOW = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)


def order_intent(
    *,
    order_id: str = "entry-1",
    idempotency_key: str = "signal-1:entry",
    side: OrderSide = OrderSide.BUY,
    quantity: int = 10,
) -> OrderIntent:
    return OrderIntent(
        order_id=order_id,
        idempotency_key=idempotency_key,
        signal_id="signal-1",
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol="AAPL",
        side=side,
        quantity=quantity,
        limit_price=Decimal("100"),
        created_at=NOW,
    )


def report(
    order: OrderIntent,
    *,
    status: ExecutionStatus,
    occurred_at: datetime,
    filled_quantity: int = 0,
) -> ExecutionReport:
    return ExecutionReport(
        order_id=order.order_id,
        idempotency_key=order.idempotency_key,
        status=status,
        filled_quantity=filled_quantity,
        average_fill_price=(Decimal("99.90") if filled_quantity else Decimal("0")),
        broker_order_id=f"broker:{order.order_id}",
        occurred_at=occurred_at,
    )


class MemoryExecutionLedger:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.intents: dict[str, OrderIntent] = {}
        self.reports: list[ExecutionReport] = []

    async def save_order_intent(self, intent: OrderIntent) -> bool:
        self.events.append(f"persist:{intent.order_id}")
        created = intent.order_id not in self.intents
        self.intents.setdefault(intent.order_id, intent)
        return created

    async def save_execution_report(self, value: ExecutionReport) -> bool:
        self.reports.append(value)
        return True

    async def get_order_intent_by_key(self, key: str) -> OrderIntent | None:
        return next(
            (intent for intent in self.intents.values() if intent.idempotency_key == key),
            None,
        )

    async def get_execution_report(self, order_id: str) -> ExecutionReport | None:
        return next(
            (value for value in reversed(self.reports) if value.order_id == order_id),
            None,
        )


class PartialFillBroker:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.submitted: list[OrderIntent] = []
        self.current: dict[str, ExecutionReport] = {}
        self.reconcile_count = 0
        self.portfolio = PortfolioState(
            as_of=NOW,
            nav=Decimal("100000"),
            peak_nav=Decimal("100000"),
            cash=Decimal("100000"),
        )

    def readiness(self, _profile: ReadinessProfile = ReadinessProfile.SUBMIT) -> BrokerReadiness:
        return BrokerReadiness(
            account_id="DU123456",
            checked_at=NOW,
            checks=(ReadinessCheck("ready", True),),
        )

    def submit(self, intent: OrderIntent) -> ExecutionReport:
        self.events.append(f"submit:{intent.order_id}")
        self.submitted.append(intent)
        submitted = report(
            intent,
            status=ExecutionStatus.SUBMITTED,
            occurred_at=intent.created_at + timedelta(milliseconds=1),
        )
        self.current[intent.order_id] = submitted
        return submitted

    def cancel(self, order_id: str) -> ExecutionReport:
        self.events.append(f"cancel:{order_id}")
        current = self.current[order_id].model_copy(
            update={
                "message": "cancel requested",
                "occurred_at": NOW + timedelta(seconds=3),
            }
        )
        self.current[order_id] = current
        return current

    def reconcile(self) -> ReconciliationResult:
        original = self.submitted[0]
        if self.reconcile_count == 0:
            current = report(
                original,
                status=ExecutionStatus.PARTIALLY_FILLED,
                filled_quantity=4,
                occurred_at=NOW + timedelta(seconds=2),
            )
        elif self.reconcile_count == 1:
            current = report(
                original,
                status=ExecutionStatus.CANCELLED,
                filled_quantity=4,
                occurred_at=NOW + timedelta(seconds=4),
            )
        else:
            replacement = self.submitted[1]
            current = report(
                replacement,
                status=ExecutionStatus.FILLED,
                filled_quantity=replacement.quantity,
                occurred_at=NOW + timedelta(seconds=6),
            )
        self.reconcile_count += 1
        self.current[current.order_id] = current
        return ReconciliationResult(
            account_id="DU123456",
            reconciled_at=current.occurred_at,
            executions=(current,),
            portfolio=self.portfolio,
        )


async def no_sleep(_seconds: float) -> None:
    return None


@pytest.mark.asyncio
async def test_partial_fill_reprice_uses_only_remaining_quantity_and_guard() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(intent: OrderIntent) -> bool:
        events.append(f"guard:{intent.order_id}")
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        events.append("reprice")
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
    )

    reports = await service.submit_with_one_reprice(order_intent())

    assert service.has_pre_submit_guard
    assert [intent.quantity for intent in broker.submitted] == [10, 6]
    assert [intent.quantity for intent in ledger.intents.values()] == [10, 6]
    cancelled = next(item for item in reports if item.status is ExecutionStatus.CANCELLED)
    assert cancelled.filled_quantity == 4
    assert cancelled.average_fill_price == Decimal("99.90")
    for submitted in broker.submitted:
        guard_index = events.index(f"guard:{submitted.order_id}")
        assert events[guard_index + 1] == f"persist:{submitted.order_id}"
        assert events[guard_index + 2] == f"submit:{submitted.order_id}"


@pytest.mark.asyncio
async def test_replacement_order_records_the_order_it_replaces() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
    )
    original = order_intent()

    await service.submit_with_one_reprice(original)

    first, replacement = broker.submitted
    assert first.reprice_generation == 0
    assert first.replaces_order_id is None
    assert replacement.reprice_generation == 1
    assert replacement.replaces_order_id == original.order_id


@pytest.mark.asyncio
async def test_restart_reuses_the_persisted_replacement_without_a_second_submit() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)
    original = order_intent()
    cancellation = report(
        original,
        status=ExecutionStatus.CANCELLED,
        filled_quantity=4,
        occurred_at=NOW + timedelta(seconds=4),
    )
    replacement = OrderIntent.model_validate(
        {
            **original.model_dump(),
            "order_id": "entry-1-r1",
            "idempotency_key": f"{original.idempotency_key}:r1",
            "quantity": 6,
            "created_at": cancellation.occurred_at,
            "replaces_order_id": original.order_id,
            "reprice_generation": 1,
        }
    )
    replacement_terminal = report(
        replacement,
        status=ExecutionStatus.CANCELLED,
        occurred_at=NOW + timedelta(seconds=5),
    )
    await ledger.save_order_intent(original)
    await ledger.save_order_intent(replacement)
    await ledger.save_execution_report(replacement_terminal)
    broker.submitted.append(replacement)
    broker.reconcile_count = 1
    reprices: list[str] = []

    async def repricer(symbol: str, _side: OrderSide) -> Decimal:
        reprices.append(symbol)
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
    )

    resumed = await service.resume_reprice_after_cancel(original, cancellation)

    assert resumed[-1].order_id == replacement.order_id
    assert resumed[-1].status is ExecutionStatus.CANCELLED
    assert not any(event.startswith("submit:") for event in events)
    assert reprices == []


@pytest.mark.asyncio
async def test_a_replacement_can_never_create_a_second_generation() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)
    original = order_intent()
    replacement = OrderIntent.model_validate(
        {
            **original.model_dump(),
            "order_id": "entry-1-r1",
            "idempotency_key": f"{original.idempotency_key}:r1",
            "replaces_order_id": original.order_id,
            "reprice_generation": 1,
        }
    )
    cancellation = report(
        replacement,
        status=ExecutionStatus.CANCELLED,
        occurred_at=NOW + timedelta(seconds=5),
    )

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
    )

    with pytest.raises(ReplacementSafetyError, match="cannot create another"):
        await service.resume_reprice_after_cancel(replacement, cancellation)


@pytest.mark.asyncio
async def test_pre_submit_guard_rejection_neither_persists_nor_submits() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def reject(intent: OrderIntent) -> bool:
        events.append(f"guard:{intent.order_id}")
        return False

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    guarded = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=reject,
    )
    unguarded = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
    )

    with pytest.raises(PreSubmitGuardRejected) as rejected:
        await guarded.submit_with_one_reprice(order_intent())

    assert rejected.value.reasons == ("PRE_SUBMIT_GUARD_REJECTED",)
    assert guarded.has_pre_submit_guard
    assert not unguarded.has_pre_submit_guard
    assert ledger.intents == {}
    assert broker.submitted == []
    assert events == ["guard:entry-1"]


@pytest.mark.asyncio
async def test_pre_submit_guard_requires_explicit_true() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def missing_decision(_intent: OrderIntent):
        return None

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=missing_decision,
    )

    with pytest.raises(PreSubmitGuardRejected) as rejected:
        await service.submit_with_one_reprice(order_intent())

    assert rejected.value.reasons == ("PRE_SUBMIT_GUARD_REJECTED",)
    assert ledger.intents == {}
    assert broker.submitted == []


@pytest.mark.asyncio
async def test_shadow_intent_never_reaches_guard_ledger_or_broker() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(_intent: OrderIntent) -> bool:
        events.append("guard")
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
    )
    shadow = order_intent().model_copy(
        update={"submission_mode": "shadow", "research_promotion_sha256": None}
    )

    with pytest.raises(NonPaperIntentRejected, match="not a paper-submission"):
        await service.submit_with_one_reprice(shadow)

    assert events == []
    assert ledger.intents == {}
    assert broker.submitted == []


@pytest.mark.asyncio
async def test_entry_promotion_must_match_execution_boundary() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(_intent: OrderIntent) -> bool:
        events.append("guard")
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="b" * 64,
        pre_submit_guard=guard,
    )

    with pytest.raises(NonPaperIntentRejected, match="configured research promotion"):
        await service.submit_with_one_reprice(order_intent())

    assert events == []
    assert ledger.intents == {}
    assert broker.submitted == []


class ImmediatelyFilledBroker(PartialFillBroker):
    def submit(self, intent: OrderIntent) -> ExecutionReport:
        self.events.append(f"submit:{intent.order_id}")
        self.submitted.append(intent)
        return report(
            intent,
            status=ExecutionStatus.FILLED,
            filled_quantity=intent.quantity,
            occurred_at=intent.created_at + timedelta(milliseconds=1),
        )


@pytest.mark.asyncio
async def test_preflight_persist_submit_is_serialized_across_workflows() -> None:
    events: list[str] = []
    broker = ImmediatelyFilledBroker(events)
    ledger = MemoryExecutionLedger(events)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    active_guards = 0
    maximum_active_guards = 0

    async def guard(intent: OrderIntent) -> bool:
        nonlocal active_guards, maximum_active_guards
        active_guards += 1
        maximum_active_guards = max(maximum_active_guards, active_guards)
        if intent.order_id == "entry-1":
            first_entered.set()
            await release_first.wait()
        active_guards -= 1
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
    )
    first = asyncio.create_task(service.submit_with_one_reprice(order_intent()))
    await first_entered.wait()
    second_intent = order_intent(
        order_id="entry-2",
        idempotency_key="signal-2:entry",
    )
    second = asyncio.create_task(service.submit_with_one_reprice(second_intent))
    await asyncio.sleep(0)

    assert maximum_active_guards == 1
    assert broker.submitted == []
    release_first.set()
    await asyncio.gather(first, second)

    assert maximum_active_guards == 1
    assert [item.order_id for item in broker.submitted] == ["entry-1", "entry-2"]


@pytest.mark.asyncio
async def test_atomic_intent_claim_allows_only_one_parallel_submit() -> None:
    events: list[str] = []
    broker = ImmediatelyFilledBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(_intent: OrderIntent) -> bool:
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
    )
    results = await asyncio.gather(
        service.submit_with_one_reprice(order_intent()),
        service.submit_with_one_reprice(order_intent()),
        return_exceptions=True,
    )

    assert sum(isinstance(item, OrderIntentClaimLost) for item in results) == 1
    assert [item.order_id for item in broker.submitted] == ["entry-1"]


@pytest.mark.asyncio
async def test_invalid_reprice_is_revalidated_before_replacement_submit() -> None:
    events: list[str] = []
    broker = PartialFillBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(_intent: OrderIntent) -> bool:
        return True

    async def invalid_reprice(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("0")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=invalid_reprice,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
    )

    with pytest.raises(ValidationError):
        await service.submit_with_one_reprice(order_intent())

    assert [item.order_id for item in broker.submitted] == ["entry-1"]
    assert "entry-1-r1" not in ledger.intents


class DelayedReplacementCancelBroker(PartialFillBroker):
    def reconcile(self) -> ReconciliationResult:
        original = self.submitted[0]
        if self.reconcile_count == 0:
            current = report(
                original,
                status=ExecutionStatus.PARTIALLY_FILLED,
                filled_quantity=4,
                occurred_at=NOW + timedelta(seconds=1),
            )
        elif self.reconcile_count == 1:
            current = report(
                original,
                status=ExecutionStatus.CANCELLED,
                filled_quantity=4,
                occurred_at=NOW + timedelta(seconds=2),
            )
        else:
            replacement = self.submitted[1]
            replacement_status = (
                ExecutionStatus.CANCELLED
                if self.reconcile_count == 4
                else ExecutionStatus.PARTIALLY_FILLED
            )
            current = report(
                replacement,
                status=replacement_status,
                filled_quantity=2,
                occurred_at=NOW + timedelta(seconds=self.reconcile_count + 1),
            )
        self.reconcile_count += 1
        self.current[current.order_id] = current
        return ReconciliationResult(
            account_id="DU123456",
            reconciled_at=current.occurred_at,
            executions=(current,),
            portfolio=self.portfolio,
        )


@pytest.mark.asyncio
async def test_replacement_cancel_is_polled_to_confirmed_terminal_state() -> None:
    events: list[str] = []
    broker = DelayedReplacementCancelBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(_intent: OrderIntent) -> bool:
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
        cancel_reconcile_attempts=2,
    )

    reports = await service.submit_with_one_reprice(order_intent())

    assert reports[-1].status is ExecutionStatus.CANCELLED
    assert reports[-1].filled_quantity == 2
    assert [event for event in events if event.startswith("cancel:")] == [
        "cancel:entry-1",
        "cancel:entry-1-r1",
    ]


class NeverConfirmsCancelBroker(PartialFillBroker):
    def reconcile(self) -> ReconciliationResult:
        current = self.current[self.submitted[0].order_id]
        current = current.model_copy(
            update={"occurred_at": current.occurred_at + timedelta(seconds=1)}
        )
        self.current[current.order_id] = current
        self.reconcile_count += 1
        return ReconciliationResult(
            account_id="DU123456",
            reconciled_at=current.occurred_at,
            executions=(current,),
            portfolio=self.portfolio,
        )


@pytest.mark.asyncio
async def test_cancel_confirmation_is_bounded_and_fails_closed() -> None:
    events: list[str] = []
    broker = NeverConfirmsCancelBroker(events)
    ledger = MemoryExecutionLedger(events)

    async def guard(_intent: OrderIntent) -> bool:
        return True

    async def repricer(_symbol: str, _side: OrderSide) -> Decimal:
        return Decimal("100.05")

    service = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=repricer,
        sleep=no_sleep,
        promotion_artifact_sha256="a" * 64,
        pre_submit_guard=guard,
        cancel_reconcile_attempts=2,
    )

    with pytest.raises(ExecutionReconciliationError, match="after 2"):
        await service.submit_with_one_reprice(order_intent())

    assert broker.reconcile_count == 3  # one wait refresh plus two cancel polls
    assert [event for event in events if event.startswith("cancel:")] == ["cancel:entry-1"]


def test_native_ibkr_cancelled_partial_fill_remains_terminal_and_cumulative() -> None:
    backend = NativeIBAPIBackend.without_transport(clock=lambda: NOW + timedelta(seconds=1))
    backend._intents = {9100: order_intent()}

    backend._on_order_status(9100, "Cancelled", Decimal("4"), 99.90)

    cancelled = backend._reports[9100]
    assert cancelled.status is ExecutionStatus.CANCELLED
    assert cancelled.filled_quantity == 4
    assert cancelled.average_fill_price == Decimal("99.9")

    backend._on_order_status(9100, "Cancelled", Decimal("10"), 99.80)

    filled = backend._reports[9100]
    assert filled.status is ExecutionStatus.FILLED
    assert filled.filled_quantity == 10


class ExposureBackend:
    def __init__(self) -> None:
        self.remote_reports: list[ExecutionReport] = []
        self.submitted: list[OrderIntent] = []
        self.cancellations: list[str] = []
        self.order_channel_ready = True
        self.live_data = True
        self.portfolio = PortfolioState(
            as_of=NOW,
            nav=Decimal("100000"),
            peak_nav=Decimal("100000"),
            cash=Decimal("100000"),
        )

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def is_connected(self) -> bool:
        return True

    def account_ids(self) -> tuple[str, ...]:
        return ("DU123456",)

    def ready_for_orders(self) -> bool:
        return self.order_channel_ready

    def market_data_live(self) -> bool:
        return self.live_data

    def order_scope_authoritative(self) -> bool:
        return True

    def submit_order(self, intent: OrderIntent) -> str:
        self.submitted.append(intent)
        return str(9200 + len(self.submitted))

    def cancel_order(self, broker_order_id: str) -> None:
        self.cancellations.append(broker_order_id)

    def reconcile_orders(
        self,
        _account_id: str,
        _known_orders: tuple[tuple[OrderIntent, ExecutionReport], ...],
    ) -> IBKRRemoteOrderSnapshot:
        return IBKRRemoteOrderSnapshot(
            reports=tuple(self.remote_reports),
            seen_order_ids=frozenset(report.order_id for report in self.remote_reports),
        )

    def portfolio_state(self, _account_id: str) -> PortfolioState:
        return self.portfolio


def test_ibkr_portfolio_reserves_only_remaining_open_entry_exposure() -> None:
    backend = ExposureBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        backend=backend,
        clock=lambda: NOW,
    )
    adapter.reconcile()
    entry = order_intent()
    entry_submitted = adapter.submit(entry)
    entry_partial = entry_submitted.model_copy(
        update={
            "status": ExecutionStatus.PARTIALLY_FILLED,
            "filled_quantity": 4,
            "average_fill_price": Decimal("99.90"),
            "occurred_at": NOW + timedelta(seconds=1),
        }
    )
    backend.remote_reports = [entry_partial]
    backend.portfolio = backend.portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol="AAPL",
                    direction=Direction.LONG,
                    quantity=4,
                    market_price=Decimal("100"),
                    average_price=Decimal("99.90"),
                ),
            )
        }
    )
    adapter.reconcile()

    exit_intent = order_intent(
        order_id="exit-1",
        idempotency_key="signal-1:exit",
        side=OrderSide.SELL,
        quantity=4,
    )
    exit_submitted = adapter.submit(exit_intent)
    backend.remote_reports = [entry_partial, exit_submitted]

    portfolio = adapter.reconcile().portfolio

    assert len(portfolio.pending_orders) == 1
    exposure = portfolio.pending_orders[0]
    assert exposure.order_id == entry.order_id
    assert exposure.notional == Decimal("600")


def test_ibkr_cancel_does_not_require_market_data_or_next_order_id() -> None:
    backend = ExposureBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        backend=backend,
        clock=lambda: NOW,
    )
    adapter.reconcile()
    submitted = adapter.submit(order_intent())
    backend.live_data = False
    backend.order_channel_ready = False

    cancellation = adapter.cancel(submitted.order_id)

    assert cancellation.message == "cancel requested"
    assert backend.cancellations == [submitted.broker_order_id]
