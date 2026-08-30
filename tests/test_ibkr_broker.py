from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from event_trader.broker import (
    BrokerNotReady,
    IdempotencyConflict,
    InMemoryPaperBroker,
    InvalidOrderTransition,
    PaperAccountGuard,
    PaperAccountViolation,
    ReadinessProfile,
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
from event_trader.providers import ibkr as ibkr_module
from event_trader.providers.ibkr import (
    IBKRBrokerAdapter,
    IBKRConnectionConfig,
    IBKRRecoveryIncomplete,
    IBKRRemoteOrderSnapshot,
    NativeIBAPIBackend,
)

NOW = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)


class IncrementingClock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        current = self.value
        self.value += timedelta(milliseconds=1)
        return current


def intent(
    *,
    order_id: str = "order-1",
    key: str = "event-1:AAPL:buy:v1",
    symbol: str = "AAPL",
    quantity: int = 10,
) -> OrderIntent:
    return OrderIntent(
        order_id=order_id,
        idempotency_key=key,
        signal_id="signal-1",
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol=symbol,
        side=OrderSide.BUY,
        quantity=quantity,
        limit_price=Decimal("100.00"),
        created_at=NOW,
    )


def portfolio_with_long(quantity: int) -> PortfolioState:
    return PortfolioState(
        as_of=NOW,
        nav=Decimal("100000"),
        peak_nav=Decimal("100000"),
        cash=Decimal("100000"),
        positions=(
            Position(
                symbol="AAPL",
                direction=Direction.LONG,
                quantity=quantity,
                market_price=Decimal("100"),
                average_price=Decimal("99.95"),
            ),
        ),
    )


class FakeIBKRBackend:
    def __init__(self) -> None:
        self.connected = True
        self.live_data = True
        self.order_channel = True
        self.authoritative_scope = True
        self.submissions: list[OrderIntent] = []
        self.cancellations: list[str] = []
        self.remote_reports: list[ExecutionReport] = []
        self.unknown_remote_order_ids: set[str] = set()
        self.execution_requests = 0
        self.portfolio = PortfolioState(
            as_of=NOW,
            nav=Decimal("100000"),
            peak_nav=Decimal("100000"),
            cash=Decimal("100000"),
        )

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def account_ids(self) -> tuple[str, ...]:
        return ("DU123456",)

    def ready_for_orders(self) -> bool:
        return self.connected and self.order_channel

    def market_data_live(self) -> bool:
        return self.live_data

    def order_scope_authoritative(self) -> bool:
        return self.authoritative_scope

    def submit_order(self, order: OrderIntent) -> str:
        self.submissions.append(order)
        return str(9000 + len(self.submissions))

    def cancel_order(self, broker_order_id: str) -> None:
        self.cancellations.append(broker_order_id)

    def reconcile_orders(
        self,
        account_id: str,
        _known_orders: tuple[tuple[OrderIntent, ExecutionReport], ...],
    ) -> IBKRRemoteOrderSnapshot:
        assert account_id == "DU123456"
        self.execution_requests += 1
        return IBKRRemoteOrderSnapshot(
            reports=tuple(self.remote_reports),
            seen_order_ids=frozenset(report.order_id for report in self.remote_reports),
            unknown_remote_order_ids=frozenset(self.unknown_remote_order_ids),
        )

    def portfolio_state(self, account_id: str) -> PortfolioState:
        assert account_id == "DU123456"
        return self.portfolio


def test_paper_account_guard_rejects_live_and_non_allowlisted_accounts() -> None:
    guard = PaperAccountGuard(["DU123456"])

    with pytest.raises(PaperAccountViolation, match="live execution is disabled"):
        guard.assert_paper("DU123456", "live")
    with pytest.raises(PaperAccountViolation, match="allowlist"):
        guard.assert_paper("U-LIVE-1", "paper")


def test_in_memory_broker_submit_and_cancel_are_idempotent() -> None:
    broker = InMemoryPaperBroker(account_id="DU123456", clock=IncrementingClock())
    order = intent()

    first = broker.submit(order)
    second = broker.submit(order)
    cancelled = broker.cancel(order.order_id)
    cancelled_again = broker.cancel(order.order_id)

    assert first == second
    assert first.status is ExecutionStatus.SUBMITTED
    assert cancelled == cancelled_again
    assert cancelled.status is ExecutionStatus.CANCELLED
    assert len(broker.reports) == 1


def test_in_memory_broker_rejects_idempotency_key_reuse() -> None:
    broker = InMemoryPaperBroker(account_id="DU123456", clock=IncrementingClock())
    broker.submit(intent())

    with pytest.raises(IdempotencyConflict, match="reused"):
        broker.submit(intent(symbol="MSFT"))


def test_order_state_machine_prevents_fill_regression() -> None:
    broker = InMemoryPaperBroker(account_id="DU123456", clock=IncrementingClock())
    submitted = broker.submit(intent())
    partial = ExecutionReport(
        order_id=submitted.order_id,
        idempotency_key=submitted.idempotency_key,
        status=ExecutionStatus.PARTIALLY_FILLED,
        filled_quantity=5,
        average_fill_price=Decimal("99.90"),
        broker_order_id=submitted.broker_order_id,
        occurred_at=NOW + timedelta(seconds=1),
    )
    broker.record_execution(partial)
    regressed = partial.model_copy(
        update={"filled_quantity": 4, "occurred_at": NOW + timedelta(seconds=2)}
    )

    with pytest.raises(InvalidOrderTransition, match="cannot decrease"):
        broker.record_execution(regressed)


def test_ibkr_adapter_requires_reconciliation_before_submit() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )

    assert not adapter.readiness().ready
    assert {check.name for check in adapter.readiness().failures} == {"reconciled"}
    adapter.reconcile()
    assert adapter.readiness().ready


def test_ibkr_readiness_profiles_keep_cancel_and_reconcile_available() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=lambda: NOW,
    )

    assert adapter.readiness(ReadinessProfile.RECONCILE).ready
    adapter.reconcile()
    backend.live_data = False
    backend.order_channel = False

    assert adapter.readiness(ReadinessProfile.RECONCILE).ready
    assert adapter.readiness(ReadinessProfile.CANCEL).ready
    assert not adapter.readiness(ReadinessProfile.EXIT).ready
    assert not adapter.readiness(ReadinessProfile.SUBMIT).ready


def test_ibkr_adapter_submits_and_cancels_only_once_per_order() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    adapter.reconcile()

    first = adapter.submit(intent())
    second = adapter.submit(intent())
    adapter.cancel(first.order_id)
    adapter.cancel(first.order_id)

    assert first == second
    assert len(backend.submissions) == 1
    assert backend.cancellations == [first.broker_order_id]


def test_ibkr_reconcile_applies_terminal_broker_report() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    adapter.reconcile()
    submitted = adapter.submit(intent())
    backend.remote_reports = [
        submitted.model_copy(
            update={
                "status": ExecutionStatus.FILLED,
                "filled_quantity": 10,
                "average_fill_price": Decimal("99.95"),
                "occurred_at": NOW + timedelta(seconds=1),
            }
        )
    ]
    backend.portfolio = portfolio_with_long(10)

    result = adapter.reconcile()

    assert result.executions[0].status is ExecutionStatus.FILLED
    assert result.portfolio.reconciled


def test_ibkr_reconcile_full_fill_overrides_cancel_race_status() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    adapter.reconcile()
    submitted = adapter.submit(intent())
    backend.remote_reports = [
        submitted.model_copy(
            update={
                "status": ExecutionStatus.CANCELLED,
                "filled_quantity": 10,
                "average_fill_price": Decimal("99.95"),
                "occurred_at": NOW + timedelta(seconds=1),
            }
        )
    ]
    backend.portfolio = portfolio_with_long(10)

    result = adapter.reconcile()

    assert result.executions[0].status is ExecutionStatus.FILLED
    assert result.executions[0].filled_quantity == 10


def test_ibkr_live_environment_is_rejected_before_backend_use() -> None:
    backend = FakeIBKRBackend()

    with pytest.raises(PaperAccountViolation, match="live execution is disabled"):
        IBKRBrokerAdapter(
            account_id="DU123456",
            paper_account_allowlist=["DU123456"],
            environment="live",
            backend=backend,
        )

    assert backend.submissions == []


def test_ibkr_adapter_never_submits_shadow_intent() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=lambda: NOW,
    )
    adapter.reconcile()
    shadow = intent().model_copy(
        update={"submission_mode": "shadow", "research_promotion_sha256": None}
    )

    with pytest.raises(PaperAccountViolation, match="shadow intents"):
        adapter.submit(shadow)

    assert backend.submissions == []


class FakeRecoveryStore:
    def __init__(
        self,
        intents: tuple[OrderIntent, ...],
        reports: dict[str, ExecutionReport | None],
        *,
        all_intents: tuple[OrderIntent, ...] | None = None,
    ) -> None:
        self.intents = intents
        self.all_intents = all_intents if all_intents is not None else intents
        self.reports = reports
        self.requested_limits: list[int] = []

    async def list_orders_for_reconciliation(
        self, *, limit: int = 1_000
    ) -> tuple[OrderIntent, ...]:
        self.requested_limits.append(limit)
        return self.intents[:limit]

    async def get_execution_report(self, order_id: str) -> ExecutionReport | None:
        return self.reports.get(order_id)

    async def list_order_intents(self, *, limit: int = 1_000) -> tuple[OrderIntent, ...]:
        return self.all_intents[:limit]


def submitted_report(order: OrderIntent) -> ExecutionReport:
    return ExecutionReport(
        order_id=order.order_id,
        idempotency_key=order.idempotency_key,
        status=ExecutionStatus.SUBMITTED,
        broker_order_id="9100",
        occurred_at=NOW + timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_ibkr_restore_unknown_outcome_confirms_open_order_without_submit() -> None:
    backend = FakeIBKRBackend()
    clock = IncrementingClock()
    clock.value = NOW + timedelta(seconds=10)
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=clock,
    )
    order = intent()
    store = FakeRecoveryStore((order,), {order.order_id: None})

    restored = await adapter.restore_from_storage(store)

    assert restored[0].status is ExecutionStatus.PENDING
    assert backend.submissions == []
    assert backend.execution_requests == 0

    remote_open = submitted_report(order)
    backend.remote_reports = [remote_open]
    result = adapter.reconcile()

    assert result.executions == (remote_open,)
    assert {check.name for check in adapter.readiness().failures} == {"recovery_orders_terminal"}
    with pytest.raises(BrokerNotReady, match="restored order"):
        adapter.submit(order)

    adapter.cancel(order.order_id)
    assert backend.cancellations == ["9100"]
    backend.remote_reports = [
        remote_open.model_copy(
            update={
                "status": ExecutionStatus.CANCELLED,
                "occurred_at": NOW + timedelta(seconds=20),
            }
        )
    ]

    adapter.reconcile()

    assert adapter.readiness().ready
    assert backend.submissions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("with_persisted_report", [False, True])
async def test_ibkr_restore_fails_closed_when_remote_omits_open_order(
    with_persisted_report: bool,
) -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    order = intent()
    report = submitted_report(order) if with_persisted_report else None
    store = FakeRecoveryStore((order,), {order.order_id: report})
    await adapter.restore_from_storage(store)

    with pytest.raises(IBKRRecoveryIncomplete, match="did not confirm"):
        adapter.reconcile()

    assert not adapter.readiness().ready
    assert backend.submissions == []


@pytest.mark.asyncio
async def test_ibkr_restore_accepts_partial_fill_snapshot_atomically() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    order = intent()
    partial = submitted_report(order).model_copy(
        update={
            "status": ExecutionStatus.PARTIALLY_FILLED,
            "filled_quantity": 4,
            "average_fill_price": Decimal("99.90"),
        }
    )
    store = FakeRecoveryStore((order,), {order.order_id: partial})

    first = await adapter.restore_from_storage(store)
    second = await adapter.restore_from_storage(store)

    assert first == second == (partial,)
    assert backend.submissions == []
    backend.remote_reports = [partial]
    backend.portfolio = portfolio_with_long(4)
    assert adapter.reconcile().executions == (partial,)


@pytest.mark.asyncio
async def test_ibkr_restore_accepts_direct_remote_fill_after_unknown_outcome() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    order = intent()
    await adapter.restore_from_storage(FakeRecoveryStore((order,), {order.order_id: None}))
    filled = submitted_report(order).model_copy(
        update={
            "status": ExecutionStatus.FILLED,
            "filled_quantity": order.quantity,
            "average_fill_price": Decimal("99.95"),
        }
    )
    backend.remote_reports = [filled]
    backend.portfolio = portfolio_with_long(10)

    result = adapter.reconcile()

    assert result.executions == (filled,)
    assert adapter.readiness().ready
    assert backend.submissions == []


@pytest.mark.asyncio
async def test_ibkr_restore_loads_terminal_history_for_position_reconciliation() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=IncrementingClock(),
    )
    order = intent()
    filled = submitted_report(order).model_copy(
        update={
            "status": ExecutionStatus.FILLED,
            "filled_quantity": order.quantity,
            "average_fill_price": Decimal("99.95"),
        }
    )
    store = FakeRecoveryStore(
        (),
        {order.order_id: filled},
        all_intents=(order,),
    )

    restored = await adapter.restore_from_storage(store)
    backend.remote_reports = [filled]
    backend.portfolio = portfolio_with_long(10)
    result = adapter.reconcile()

    assert restored == (filled,)
    assert result.portfolio.positions[0].quantity == 10
    assert adapter.readiness().ready


@pytest.mark.asyncio
async def test_ibkr_restore_excludes_shadow_intents_from_broker_state() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
    )
    shadow = intent().model_copy(
        update={"submission_mode": "shadow", "research_promotion_sha256": None}
    )
    store = FakeRecoveryStore(
        (shadow,),
        {shadow.order_id: None},
        all_intents=(shadow,),
    )

    restored = await adapter.restore_from_storage(store)
    result = adapter.reconcile()

    assert restored == ()
    assert result.executions == ()
    assert result.portfolio.pending_orders == ()


@pytest.mark.asyncio
async def test_ibkr_restore_rejects_truncated_recovery_set_before_hydration() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
    )
    first = intent()
    second = intent(order_id="order-2", key="event-2:MSFT:buy:v1", symbol="MSFT")
    store = FakeRecoveryStore(
        (first, second),
        {first.order_id: None, second.order_id: None},
    )

    with pytest.raises(IBKRRecoveryIncomplete, match="more than 1"):
        await adapter.restore_from_storage(store, max_orders=1)

    assert store.requested_limits == [2]
    assert adapter.reports == ()
    assert backend.submissions == []


def test_ibkr_allowlisted_live_account_id_is_still_rejected() -> None:
    backend = FakeIBKRBackend()

    with pytest.raises(PaperAccountViolation, match=r"DU<digits>"):
        IBKRBrokerAdapter(
            account_id="U123456",
            paper_account_allowlist=["U123456"],
            environment="paper",
            backend=backend,
        )

    assert backend.submissions == []


def test_ibkr_reconcile_blocks_unknown_remote_order_activity() -> None:
    backend = FakeIBKRBackend()
    backend.unknown_remote_order_ids.add("manual:77")
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
    )

    with pytest.raises(IBKRRecoveryIncomplete, match="unknown or manual"):
        adapter.reconcile()

    assert not adapter.readiness().ready


def test_ibkr_reconcile_blocks_non_authoritative_order_scope() -> None:
    backend = FakeIBKRBackend()
    backend.authoritative_scope = False
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
    )

    with pytest.raises(IBKRRecoveryIncomplete, match="authoritatively"):
        adapter.reconcile()

    assert {check.name for check in adapter.readiness().failures} >= {
        "account_order_scope_authoritative",
        "reconciled",
    }


def test_ibkr_reconcile_blocks_local_open_order_missing_remotely() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
    )
    adapter.reconcile()
    adapter.submit(intent())

    with pytest.raises(IBKRRecoveryIncomplete, match="local open"):
        adapter.reconcile()

    assert not adapter.readiness().ready


def test_ibkr_reconcile_blocks_position_mismatch() -> None:
    backend = FakeIBKRBackend()
    adapter = IBKRBrokerAdapter(
        account_id="DU123456",
        paper_account_allowlist=["DU123456"],
        backend=backend,
        clock=lambda: NOW,
    )
    adapter.reconcile()
    submitted = adapter.submit(intent())
    backend.remote_reports = [
        submitted.model_copy(
            update={
                "status": ExecutionStatus.FILLED,
                "filled_quantity": 10,
                "average_fill_price": Decimal("99.95"),
                "occurred_at": NOW + timedelta(seconds=1),
            }
        )
    ]

    with pytest.raises(IBKRRecoveryIncomplete, match="positions differ"):
        adapter.reconcile()

    assert not adapter.readiness().ready


def test_native_ibkr_reconciliation_requests_and_joins_all_order_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order = intent()
    submitted = submitted_report(order)
    backend = NativeIBAPIBackend.without_transport(
        IBKRConnectionConfig(client_id=0, timeout_seconds=0.1),
        clock=lambda: NOW + timedelta(seconds=2),
    )
    backend._next_order_id = 9000
    backend._reports = {9100: submitted}

    class FakeNativeClient:
        def __init__(self, owner: NativeIBAPIBackend) -> None:
            self.owner = owner
            self.calls: list[str] = []

        def isConnected(self) -> bool:
            return True

        def reqAllOpenOrders(self) -> None:
            self.calls.append("open")
            contract = SimpleNamespace(symbol="AAPL")
            remote_order = SimpleNamespace(
                account="DU123456",
                orderRef=order.idempotency_key,
                action="BUY",
                totalQuantity=10,
                filledQuantity=4,
                orderType="LMT",
                lmtPrice=100.0,
                tif="DAY",
            )
            state = SimpleNamespace(status="Submitted", avgFillPrice=99.9)
            self.owner._on_remote_order(
                9100,
                contract,
                remote_order,
                state,
                completed=False,
            )
            self.owner._on_open_order_end()

        def reqExecutions(self, req_id: int, execution_filter) -> None:
            self.calls.append("executions")
            assert execution_filter.acctCode == "DU123456"
            execution = SimpleNamespace(
                acctNumber="DU123456",
                orderId=9100,
                orderRef=order.idempotency_key,
                execId="exec-9100-1",
                shares=4,
                price=99.9,
                cumQty=4,
                avgPrice=99.9,
            )
            self.owner._on_exec_details(
                req_id,
                SimpleNamespace(symbol="AAPL"),
                execution,
            )
            self.owner._on_exec_details_end(req_id)

        def reqCompletedOrders(self, api_only: bool) -> None:
            self.calls.append("completed")
            assert api_only is False
            self.owner._on_completed_orders_end()

    class FakeExecutionFilter:
        acctCode = ""

    client = FakeNativeClient(backend)
    backend._client = client
    monkeypatch.setattr(ibkr_module, "_ExecutionFilter", FakeExecutionFilter)

    snapshot = backend.reconcile_orders(
        "DU123456",
        ((order, submitted),),
    )

    assert client.calls == ["open", "executions", "completed"]
    assert snapshot.complete
    assert snapshot.unknown_remote_order_ids == frozenset()
    assert snapshot.seen_order_ids == frozenset({order.order_id})
    assert snapshot.reports[0].status is ExecutionStatus.PARTIALLY_FILLED
    assert snapshot.reports[0].filled_quantity == 4
    assert backend._next_order_id == 9101


def test_native_ibkr_collector_marks_unbound_manual_order_unknown() -> None:
    backend = NativeIBAPIBackend.without_transport(clock=lambda: NOW)
    backend._next_order_id = 9000
    backend._reconciliation_account = "DU123456"
    manual_order = SimpleNamespace(
        account="DU123456",
        orderRef="",
        action="BUY",
        totalQuantity=1,
        filledQuantity=0,
        orderType="LMT",
        lmtPrice=100.0,
        tif="DAY",
    )

    backend._on_remote_order(
        9200,
        SimpleNamespace(symbol="AAPL"),
        manual_order,
        SimpleNamespace(status="Submitted", avgFillPrice=0),
        completed=False,
    )

    assert backend._unknown_remote_order_ids == {"9200"}
    assert backend._remote_seen_order_ids == set()


def test_native_callbacks_are_monotone_and_commissions_are_order_independent() -> None:
    order = intent()
    backend = NativeIBAPIBackend.without_transport(clock=IncrementingClock())
    backend._next_order_id = 9200
    backend._intents = {9100: order}

    backend._on_order_status(9100, "Filled", 10, 99.9)
    first = backend._reports[9100]
    backend._on_order_status(9100, "Submitted", 4, 99.8)
    assert backend._reports[9100] == first

    backend._on_commission_report(SimpleNamespace(execId="exec-2", commission=Decimal("0.75")))
    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        SimpleNamespace(
            acctNumber="DU123456",
            orderId=9100,
            orderRef=order.idempotency_key,
            execId="exec-1",
            shares=6,
            price=99.8,
            cumQty=6,
            avgPrice=99.8,
        ),
    )
    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        SimpleNamespace(
            acctNumber="DU123456",
            orderId=9100,
            orderRef=order.idempotency_key,
            execId="exec-2",
            shares=4,
            price=100.0,
            cumQty=10,
            avgPrice=99.88,
        ),
    )
    before_duplicate = backend._reports[9100]
    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        SimpleNamespace(
            acctNumber="DU123456",
            orderId=9100,
            orderRef=order.idempotency_key,
            execId="exec-2",
            shares=4,
            price=100.0,
            cumQty=10,
            avgPrice=99.88,
        ),
    )
    assert backend._reports[9100] == before_duplicate

    backend._on_commission_report(SimpleNamespace(execId="exec-1", commission=Decimal("1.25")))
    final = backend._reports[9100]
    assert final.status is ExecutionStatus.FILLED
    assert final.filled_quantity == 10
    assert final.fill_count == 2
    assert final.fees == Decimal("2.00")
    assert not final.pending_commission
    assert final.update_sequence > first.update_sequence
    assert len(backend._fills_by_execution_id) == 2
    backend._on_commission_report(SimpleNamespace(execId="exec-1", commission=Decimal("1.25")))
    assert backend._reports[9100] == final
    assert len(backend._fills_by_execution_id) == 2


def callback_backend(order: OrderIntent) -> NativeIBAPIBackend:
    """Backend double reduced to the state the native callbacks actually read."""

    backend = NativeIBAPIBackend.without_transport(clock=IncrementingClock())
    backend._next_order_id = 9200
    backend._intents = {9100: order}
    # A backend without transport is not connected; the tests that ask about
    # readiness have to state that they are simulating an open session.
    backend._client = SimpleNamespace(isConnected=lambda: True)
    return backend


def execution_details(
    order: OrderIntent,
    *,
    execution_id: str,
    shares: float,
    price: float,
    cumulative: float,
    average: float,
) -> SimpleNamespace:
    return SimpleNamespace(
        acctNumber=order.account_id,
        orderId=9100,
        orderRef=order.idempotency_key,
        execId=execution_id,
        shares=shares,
        price=price,
        cumQty=cumulative,
        avgPrice=average,
    )


def test_native_average_fill_price_stays_within_money_precision() -> None:
    """A weighted average that does not divide evenly is still a valid price.

    One share at 10.00 and two at 10.01 average to 10.00666..., which Decimal
    division carries to the context precision.  The money contract allows eight
    decimal places, so an unnormalized aggregate would be rejected by its own
    report model inside the broker callback thread.
    """

    order = intent(quantity=3)
    backend = callback_backend(order)

    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        execution_details(
            order,
            execution_id="exec-1",
            shares=1,
            price=10.00,
            cumulative=1,
            average=10.00,
        ),
    )
    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        execution_details(
            order,
            execution_id="exec-2",
            shares=2,
            price=10.01,
            cumulative=3,
            average=10.0066666,
        ),
    )

    report = backend._reports[9100]
    assert report.average_fill_price == Decimal("10.00666667")
    assert report.average_fill_price.as_tuple().exponent >= -8
    assert report.status is ExecutionStatus.FILLED
    assert report.filled_quantity == 3
    assert report.fill_count == 2
    assert not backend._callback_failures


def test_native_order_status_normalizes_a_broker_average_without_fills() -> None:
    """IBKR derives the average itself and sends it as a C double."""

    order = intent(quantity=3)
    backend = callback_backend(order)

    backend._on_order_status(9100, "Filled", 3, 10.006666666666666)

    report = backend._reports[9100]
    assert report.average_fill_price == Decimal("10.00666667")
    assert not backend._callback_failures


def test_native_order_status_refuses_an_average_below_the_money_contract() -> None:
    """An average that rounds to zero blocks the report instead of zeroing it.

    The single normalization sits after the price chain, so a sub-contract
    average from the broker is caught by the existing zero-price guard rather
    than by a second check in the parsing step.
    """

    order = intent(quantity=3)
    backend = callback_backend(order)

    backend._on_order_status(9100, "Filled", 3, 1e-9)

    assert 9100 not in backend._reports
    assert backend.deferred_inconsistencies == frozenset({"inconsistent:9100"})


def test_native_execution_details_refuse_a_price_below_the_money_contract() -> None:
    """A price that rounds to zero is refused, never stored as free shares."""

    order = intent(quantity=3)
    backend = callback_backend(order)

    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        execution_details(
            order,
            execution_id="exec-1",
            shares=1,
            price=1e-9,
            cumulative=1,
            average=1e-9,
        ),
    )

    assert not backend._fills_by_execution_id
    assert 9100 not in backend._reports
    assert backend.deferred_inconsistencies == frozenset({"9100"})


def test_native_execution_details_round_a_long_broker_price() -> None:
    order = intent(quantity=3)
    backend = callback_backend(order)

    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        execution_details(
            order,
            execution_id="exec-1",
            shares=1,
            price=0.1234567891234,
            cumulative=1,
            average=0.1234567891234,
        ),
    )

    fill = backend._fills_by_execution_id["exec-1"]
    assert fill.price == Decimal("0.12345679")
    assert backend._reports[9100].average_fill_price == Decimal("0.12345679")


def test_native_commission_report_normalizes_a_long_commission() -> None:
    """The commission is the one value a stored fill may still gain."""

    order = intent(quantity=3)
    backend = callback_backend(order)
    backend._on_exec_details(
        1,
        SimpleNamespace(symbol="AAPL"),
        execution_details(
            order,
            execution_id="exec-1",
            shares=1,
            price=99.80,
            cumulative=1,
            average=99.80,
        ),
    )

    backend._on_commission_report(SimpleNamespace(execId="exec-1", commission=0.1234567891234))

    fill = backend._fills_by_execution_id["exec-1"]
    assert fill.commission == Decimal("0.12345679")
    assert fill.commission_final
    report = backend._reports[9100]
    assert report.fees == Decimal("0.12345679")
    assert not report.pending_commission
    assert not backend._callback_failures


def test_native_callback_failure_latches_and_blocks_authoritative_operations() -> None:
    """Handling a callback fault must not be cheaper than crashing was.

    A raised callback ends the reader thread, which disconnects and therefore
    fails every later readiness check.  The latch has to reproduce that refusal;
    otherwise catching the error turns fail-closed into fail-silent.
    """

    order = intent()
    backend = callback_backend(order)
    assert backend.ready_for_orders()

    backend._record_callback_failure("orderStatus", ValueError("unusable payload"))

    assert backend.callback_failures == (("orderStatus", "ValueError"),)
    assert not backend.ready_for_orders()
    with pytest.raises(ibkr_module.IBKRTransportError, match="not authoritative"):
        backend.reconcile_orders(order.account_id, ())

    backend._on_order_status(9100, "Filled", 10, 99.9)
    assert not backend.ready_for_orders()


def test_native_discard_outside_reconciliation_reaches_the_next_one() -> None:
    """A contradictory callback during trading is decided by the next reconcile."""

    order = intent()
    backend = callback_backend(order)

    backend._on_order_status(9100, "Filled", 10, 99.9)
    backend._on_order_status(9100, "Submitted", 4, 99.8)

    assert backend.deferred_inconsistencies == frozenset({"inconsistent:9100"})
    assert backend._unknown_remote_order_ids == set()


def test_a_reconciliation_refused_during_setup_leaves_no_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refusal before the run starts must not mark the session as reconciling.

    The setup binds persisted orders to their broker ids and can refuse a
    non-numeric one.  If that refusal happened after the session was already
    marked, every later reconciliation would fail with "already active" and the
    adapter could never reconcile - or exit - again.
    """

    order = intent()
    unusable = ExecutionReport(
        order_id=order.order_id,
        idempotency_key=order.idempotency_key,
        status=ExecutionStatus.SUBMITTED,
        broker_order_id="memory:order-1",
        occurred_at=NOW,
    )
    backend = NativeIBAPIBackend.without_transport(
        IBKRConnectionConfig(client_id=0, timeout_seconds=0.05),
        clock=IncrementingClock(),
    )
    backend._client = SimpleNamespace(isConnected=lambda: True)
    backend._deferred_inconsistencies = {"inconsistent:9100"}
    monkeypatch.setattr(ibkr_module, "_ExecutionFilter", lambda: SimpleNamespace(acctCode=""))

    for _ in range(2):
        with pytest.raises(ibkr_module.IBKRTransportError, match="not numeric"):
            backend.reconcile_orders(order.account_id, ((order, unusable),))
        assert backend._reconciliation_account is None
        assert backend.deferred_inconsistencies == frozenset({"inconsistent:9100"})


def test_an_aborted_reconciliation_loses_nothing_and_wedges_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out reconciliation must be repeatable and must decide nothing.

    The run takes the remembered discards over into its own result set before it
    waits for the broker's end markers.  If it then aborts, those discards are
    still undecided, and the session must not stay marked as reconciling -
    otherwise one Gateway hiccup would block every later reconciliation.
    """

    class FakeExecutionFilter:
        def __init__(self) -> None:
            self.acctCode = ""

    class SilentClient:
        """Accepts every request and never delivers an end marker."""

        def __init__(self) -> None:
            self.calls: list[str] = []

        def isConnected(self) -> bool:
            return True

        def reqAllOpenOrders(self) -> None:
            self.calls.append("reqAllOpenOrders")

        def reqExecutions(self, *_args: object) -> None:
            self.calls.append("reqExecutions")

        def reqCompletedOrders(self, *_args: object) -> None:
            self.calls.append("reqCompletedOrders")

    monkeypatch.setattr(ibkr_module, "_ExecutionFilter", FakeExecutionFilter)

    backend = NativeIBAPIBackend.without_transport(
        IBKRConnectionConfig(client_id=0, timeout_seconds=0.05),
        clock=IncrementingClock(),
    )
    backend._client = SilentClient()
    backend._deferred_inconsistencies = {"inconsistent:9100"}

    with pytest.raises(ibkr_module.IBKRTransportError, match="timed out"):
        backend.reconcile_orders("DU123456", ())

    assert backend.deferred_inconsistencies == frozenset({"inconsistent:9100"})
    assert backend._reconciliation_account is None

    # A second attempt must fail the same way, not with "already active".
    with pytest.raises(ibkr_module.IBKRTransportError, match="timed out"):
        backend.reconcile_orders("DU123456", ())
    assert backend.deferred_inconsistencies == frozenset({"inconsistent:9100"})


def test_backend_without_transport_owns_its_state_but_no_session() -> None:
    """The callback reducers are testable without ibapi or a Gateway."""

    backend = NativeIBAPIBackend.without_transport(clock=IncrementingClock())

    assert not backend.is_connected()
    assert not backend.ready_for_orders()
    assert backend.callback_failures == ()
    assert backend.deferred_inconsistencies == frozenset()
    backend.disconnect()  # a backend without transport disconnects cleanly


def test_native_portfolio_normalizes_a_broker_average_cost() -> None:
    """IBKR derives the average cost of a partial position as a quotient."""

    backend = NativeIBAPIBackend.without_transport(clock=IncrementingClock())

    backend._on_portfolio(
        SimpleNamespace(symbol="AAPL"),
        3,
        100.5,
        10.006666666666666,
        "DU123456",
    )

    position = backend._positions["DU123456"]["AAPL"]
    assert position.average_price == Decimal("10.00666667")
    assert position.quantity == 3
    assert not backend.callback_failures


def test_native_portfolio_latches_instead_of_dropping_a_position() -> None:
    """A discarded position would surface later as a mismatch with the wrong cause.

    The position set feeds the broker/local comparison before every exit, so a
    silently dropped row turns into an unexplained reconciliation failure.
    """

    backend = NativeIBAPIBackend.without_transport(clock=IncrementingClock())

    backend._on_portfolio(
        SimpleNamespace(symbol="AAPL"),
        3,
        100.5,
        float("nan"),
        "DU123456",
    )

    assert "AAPL" not in backend._positions.get("DU123456", {})
    assert [source for source, _ in backend.callback_failures] == ["updatePortfolio"]


def test_native_order_status_preserves_persisted_fill_and_fee_evidence() -> None:
    order = intent()
    persisted = ExecutionReport(
        order_id=order.order_id,
        idempotency_key=order.idempotency_key,
        status=ExecutionStatus.FILLED,
        filled_quantity=10,
        average_fill_price=Decimal("99.90"),
        fees=Decimal("2.00"),
        broker_order_id="9100",
        occurred_at=NOW,
        fill_count=2,
        pending_commission=False,
        update_sequence=8,
    )
    backend = NativeIBAPIBackend.without_transport(clock=IncrementingClock())
    backend._intents = {9100: order}
    backend._reports = {9100: persisted}

    backend._on_order_status(9100, "Filled", 10, 99.9)

    merged = backend._reports[9100]
    assert merged.filled_quantity == persisted.filled_quantity
    assert merged.fill_count == persisted.fill_count
    assert merged.fees == persisted.fees
    assert not merged.pending_commission
    assert merged.update_sequence >= persisted.update_sequence
