from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from event_trader.domain import (
    ExecutionReport,
    ExecutionStatus,
    MarketSnapshot,
    OrderIntent,
    PortfolioState,
    Position,
    Signal,
)
from event_trader.execution import (
    ExecutionReconciliationError,
    OrderIntentClaimLost,
    PreSubmitGuardRejected,
)
from event_trader.monitor import ExitMonitor, ExitMonitorStatus
from event_trader.strategy import ContinuationStrategy


class MemoryMonitorLedger:
    def __init__(self) -> None:
        self.intents: dict[str, OrderIntent] = {}
        self.reports: dict[str, ExecutionReport] = {}

    async def get_order_intent_by_key(
        self, idempotency_key: str
    ) -> OrderIntent | None:
        return self.intents.get(idempotency_key)

    async def save(self, intent: OrderIntent) -> bool:
        created = intent.idempotency_key not in self.intents
        self.intents.setdefault(intent.idempotency_key, intent)
        return created

    async def get_execution_report(
        self, order_id: str
    ) -> ExecutionReport | None:
        return self.reports.get(order_id)

    async def save_report(self, report: ExecutionReport) -> None:
        self.reports[report.order_id] = report


class RecordingExitExecutor:
    def __init__(self, ledger: MemoryMonitorLedger) -> None:
        self.ledger = ledger
        self.calls: list[OrderIntent] = []

    @property
    def has_pre_submit_guard(self) -> bool:
        return True

    async def submit_with_one_reprice(
        self, intent: OrderIntent
    ) -> tuple[ExecutionReport, ...]:
        if not await self.ledger.save(intent):
            raise OrderIntentClaimLost(intent.order_id)
        self.calls.append(intent)
        submitted = ExecutionReport(
            order_id=intent.order_id,
            idempotency_key=intent.idempotency_key,
            status=ExecutionStatus.SUBMITTED,
            broker_order_id=f"paper:{intent.order_id}",
            occurred_at=intent.created_at,
        )
        await self.ledger.save_report(submitted)
        return (submitted,)

    async def reconcile_order(self, order_id: str) -> ExecutionReport:
        report = await self.ledger.get_execution_report(order_id)
        if report is None:
            raise ExecutionReconciliationError("missing test report")
        return report

    async def resume_reprice_after_cancel(
        self,
        intent: OrderIntent,
        cancellation: ExecutionReport,
    ) -> tuple[ExecutionReport, ...]:
        data = intent.model_dump()
        data.update(
            {
                "order_id": f"{intent.order_id}-r1",
                "idempotency_key": f"{intent.idempotency_key}:r1",
                "quantity": intent.quantity - cancellation.filled_quantity,
                "created_at": cancellation.occurred_at,
            }
        )
        return await self.submit_with_one_reprice(OrderIntent.model_validate(data))

    async def resume_persisted_workflow(
        self,
        intent: OrderIntent,
        latest_report: ExecutionReport | None = None,
    ) -> tuple[ExecutionReport, ...]:
        latest = latest_report or await self.ledger.get_execution_report(intent.order_id)
        if latest is None:
            raise ExecutionReconciliationError("missing test report")
        if (
            latest.status is ExecutionStatus.CANCELLED
            and not intent.idempotency_key.endswith(":r1")
        ):
            return await self.resume_reprice_after_cancel(intent, latest)
        return (latest,)


def signal_from(snapshot, long_insight, decision_time) -> Signal:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    return signal


def position_for(signal: Signal) -> Position:
    return Position(
        symbol=signal.symbol,
        direction=signal.direction,
        quantity=10,
        market_price=signal.entry_limit,
        average_price=signal.entry_limit,
    )


def fresh_market(
    market: MarketSnapshot,
    *,
    now: datetime,
    last: Decimal | None = None,
) -> MarketSnapshot:
    quote = market.quote.model_copy(update={"timestamp": now})
    return market.model_copy(
        update={
            "as_of": now,
            "quote": quote,
            "last": last if last is not None else market.last,
            "data_fresh": True,
            "market_data_live": True,
            "halted": False,
        }
    )


def portfolio_with(
    base: PortfolioState, position: Position, *, now: datetime
) -> PortfolioState:
    return base.model_copy(
        update={
            "as_of": now,
            "positions": (position,),
            "broker_connected": True,
            "reconciled": True,
        }
    )


def monitor_components() -> tuple[
    MemoryMonitorLedger, RecordingExitExecutor, ExitMonitor
]:
    ledger = MemoryMonitorLedger()
    executor = RecordingExitExecutor(ledger)
    monitor = ExitMonitor(
        account_id="DU123456",
        ledger=ledger,
        execution_service=executor,
    )
    return ledger, executor, monitor


@pytest.mark.asyncio
async def test_exit_monitor_uses_one_stable_key_across_stop_and_force_flat(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    ledger, executor, monitor = monitor_components()
    stop_time = decision_time + timedelta(minutes=10)
    stop_market = fresh_market(
        snapshot.market,
        now=stop_time,
        last=signal.stop_price,
    )

    first = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=stop_time),
        signals=(signal,),
        markets=(stop_market,),
        now=stop_time,
    )

    assert first.outcomes[0].status is ExitMonitorStatus.EXIT_SUBMITTED
    assert first.outcomes[0].exit_reason == "STOP_EXIT"
    first_key = first.outcomes[0].intent.idempotency_key

    force_flat_time = datetime(2026, 8, 25, 19, 55, tzinfo=UTC)
    second = await monitor.run_once(
        portfolio=portfolio_with(empty_portfolio, position, now=force_flat_time),
        signals=(signal,),
        markets=(fresh_market(snapshot.market, now=force_flat_time),),
        now=force_flat_time,
    )

    assert second.outcomes[0].status is ExitMonitorStatus.ALREADY_REQUESTED
    assert second.outcomes[0].exit_reason == "FORCE_FLAT_1555"
    assert second.outcomes[0].intent.idempotency_key == first_key
    assert len(executor.calls) == 1
    assert tuple(ledger.intents) == (first_key,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("now_factory", "expected_reason"),
    [
        (lambda value: value + timedelta(minutes=60), "TIME_EXIT"),
        (
            lambda _value: datetime(2026, 8, 25, 19, 55, tzinfo=UTC),
            "FORCE_FLAT_1555",
        ),
    ],
)
async def test_exit_monitor_submits_time_and_force_flat_exits(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
    now_factory,
    expected_reason: str,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    _, executor, monitor = monitor_components()
    now = now_factory(decision_time)

    cycle = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=now),
        signals=(signal,),
        markets=(fresh_market(snapshot.market, now=now),),
        now=now,
    )

    assert cycle.outcomes[0].status is ExitMonitorStatus.EXIT_SUBMITTED
    assert cycle.outcomes[0].exit_reason == expected_reason
    assert len(executor.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("include_signal", "include_market", "expected_code"),
    [
        (False, True, "MISSING_SIGNAL_STATE"),
        (True, False, "MISSING_MARKET_STATE"),
    ],
)
async def test_exit_monitor_reports_missing_matching_state_fail_closed(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
    include_signal: bool,
    include_market: bool,
    expected_code: str,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    _, executor, monitor = monitor_components()
    now = decision_time + timedelta(minutes=10)

    cycle = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=now),
        signals=(signal,) if include_signal else (),
        markets=(fresh_market(snapshot.market, now=now),)
        if include_market
        else (),
        now=now,
    )

    assert cycle.outcomes[0].status is ExitMonitorStatus.BLOCKED
    assert expected_code in cycle.outcomes[0].reason_codes
    assert executor.calls == []


@pytest.mark.asyncio
async def test_exit_monitor_blocks_stale_or_halted_market_state(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    _, executor, monitor = monitor_components()
    now = decision_time + timedelta(minutes=10)
    stale_halted = snapshot.market.model_copy(
        update={
            "market_data_live": True,
            "data_fresh": False,
            "halted": True,
        }
    )

    cycle = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=now),
        signals=(signal,),
        markets=(stale_halted,),
        now=now,
    )

    outcome = cycle.outcomes[0]
    assert outcome.status is ExitMonitorStatus.BLOCKED
    assert {"MARKET_NOT_FRESH", "MARKET_HALTED", "MARKET_STATE_STALE"} <= set(
        outcome.reason_codes
    )
    assert executor.calls == []


@pytest.mark.asyncio
async def test_exit_monitor_reports_unreconciled_empty_portfolio_at_cycle_level(
    empty_portfolio,
    decision_time,
) -> None:
    _, executor, monitor = monitor_components()
    unsafe_portfolio = empty_portfolio.model_copy(
        update={"reconciled": False, "broker_connected": False}
    )

    cycle = await monitor.run_cycle(
        portfolio=unsafe_portfolio,
        signals=(),
        markets=(),
        now=decision_time,
    )

    assert not cycle.ready
    assert {
        "PORTFOLIO_BROKER_DISCONNECTED",
        "PORTFOLIO_NOT_RECONCILED",
    } <= set(cycle.reason_codes)
    assert cycle.outcomes == ()
    assert executor.calls == []


@pytest.mark.asyncio
async def test_exit_monitor_resumes_reprice_after_persisted_partial_cancel(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    original_position = position_for(signal)
    ledger, executor, monitor = monitor_components()
    now = decision_time + timedelta(minutes=10)
    market = fresh_market(snapshot.market, now=now, last=signal.stop_price)
    first = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, original_position, now=now),
        signals=(signal,),
        markets=(market,),
        now=now,
    )
    base_intent = first.outcomes[0].intent
    assert base_intent is not None
    cancelled = ExecutionReport(
        order_id=base_intent.order_id,
        idempotency_key=base_intent.idempotency_key,
        status=ExecutionStatus.CANCELLED,
        filled_quantity=4,
        average_fill_price=Decimal("99.90"),
        broker_order_id="paper:base",
        occurred_at=now + timedelta(seconds=1),
    )
    await ledger.save_report(cancelled)
    remaining_position = original_position.model_copy(update={"quantity": 6})
    retry_time = now + timedelta(seconds=2)

    resumed = await monitor.run_cycle(
        portfolio=portfolio_with(
            empty_portfolio,
            remaining_position,
            now=retry_time,
        ),
        signals=(signal,),
        markets=(
            fresh_market(snapshot.market, now=retry_time, last=signal.stop_price),
        ),
        now=retry_time,
    )

    outcome = resumed.outcomes[0]
    assert outcome.status is ExitMonitorStatus.EXIT_SUBMITTED
    assert outcome.intent is not None
    assert outcome.intent.idempotency_key == f"{base_intent.idempotency_key}:r1"
    assert outcome.intent.quantity == 6
    assert [item.quantity for item in executor.calls] == [10, 6]


@pytest.mark.asyncio
async def test_exit_monitor_isolates_guard_failure_per_position(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    aapl_signal = signal_from(snapshot, long_insight, decision_time)
    msft_signal = aapl_signal.model_copy(
        update={"signal_id": "signal-msft", "symbol": "MSFT"}
    )
    aapl_position = position_for(aapl_signal)
    msft_position = position_for(msft_signal)
    now = decision_time + timedelta(minutes=10)
    aapl_market = fresh_market(
        snapshot.market,
        now=now,
        last=aapl_signal.stop_price,
    )
    msft_quote = snapshot.market.quote.model_copy(update={"symbol": "MSFT"})
    msft_market = fresh_market(
        snapshot.market.model_copy(
            update={"symbol": "MSFT", "quote": msft_quote}
        ),
        now=now,
        last=msft_signal.stop_price,
    )
    ledger = MemoryMonitorLedger()

    class SelectiveExecutor(RecordingExitExecutor):
        async def submit_with_one_reprice(
            self, intent: OrderIntent
        ) -> tuple[ExecutionReport, ...]:
            if intent.symbol == "AAPL":
                raise PreSubmitGuardRejected("blocked by refreshed risk")
            return await super().submit_with_one_reprice(intent)

    executor = SelectiveExecutor(ledger)
    monitor = ExitMonitor(
        account_id="DU123456",
        ledger=ledger,
        execution_service=executor,
    )
    portfolio = empty_portfolio.model_copy(
        update={
            "as_of": now,
            "positions": (aapl_position, msft_position),
            "broker_connected": True,
            "reconciled": True,
        }
    )

    cycle = await monitor.run_cycle(
        portfolio=portfolio,
        signals=(aapl_signal, msft_signal),
        markets=(aapl_market, msft_market),
        now=now,
    )

    assert [outcome.status for outcome in cycle.outcomes] == [
        ExitMonitorStatus.BLOCKED,
        ExitMonitorStatus.EXIT_SUBMITTED,
    ]
    assert cycle.outcomes[0].reason_codes == ("PRE_SUBMIT_GUARD_REJECTED",)
    assert [intent.symbol for intent in executor.calls] == ["MSFT"]


@pytest.mark.asyncio
async def test_exit_monitor_blocks_persisted_intent_with_unknown_remote_outcome(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    ledger, executor, monitor = monitor_components()
    now = decision_time + timedelta(minutes=10)
    market = fresh_market(snapshot.market, now=now, last=signal.stop_price)
    first = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=now),
        signals=(signal,),
        markets=(market,),
        now=now,
    )
    intent = first.outcomes[0].intent
    assert intent is not None
    ledger.reports.pop(intent.order_id)

    second = await monitor.run_cycle(
        portfolio=portfolio_with(
            empty_portfolio,
            position,
            now=now + timedelta(seconds=1),
        ),
        signals=(signal,),
        markets=(
            fresh_market(
                snapshot.market,
                now=now + timedelta(seconds=1),
                last=signal.stop_price,
            ),
        ),
        now=now + timedelta(seconds=1),
    )

    assert second.outcomes[0].status is ExitMonitorStatus.BLOCKED
    assert second.outcomes[0].reason_codes == ("EXIT_OUTCOME_UNKNOWN",)
    assert len(executor.calls) == 1


@pytest.mark.asyncio
async def test_open_exit_report_must_match_reconciled_remaining_position(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    _, executor, monitor = monitor_components()
    now = decision_time + timedelta(minutes=10)
    market = fresh_market(snapshot.market, now=now, last=signal.stop_price)
    await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=now),
        signals=(signal,),
        markets=(market,),
        now=now,
    )
    next_time = now + timedelta(seconds=1)

    cycle = await monitor.run_cycle(
        portfolio=portfolio_with(
            empty_portfolio,
            position.model_copy(update={"quantity": 9}),
            now=next_time,
        ),
        signals=(signal,),
        markets=(fresh_market(snapshot.market, now=next_time),),
        now=next_time,
    )

    assert cycle.outcomes[0].status is ExitMonitorStatus.BLOCKED
    assert cycle.outcomes[0].reason_codes == ("EXIT_REMAINDER_POSITION_MISMATCH",)
    assert len(executor.calls) == 1


@pytest.mark.asyncio
async def test_terminal_partial_exit_creates_one_stable_sequenced_attempt(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    ledger, executor, monitor = monitor_components()
    now = decision_time + timedelta(minutes=10)
    market = fresh_market(snapshot.market, now=now, last=signal.stop_price)
    first = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, position, now=now),
        signals=(signal,),
        markets=(market,),
        now=now,
    )
    base = first.outcomes[0].intent
    assert base is not None
    base_cancelled = ExecutionReport(
        order_id=base.order_id,
        idempotency_key=base.idempotency_key,
        status=ExecutionStatus.CANCELLED,
        filled_quantity=4,
        average_fill_price=Decimal("99.90"),
        broker_order_id="paper:base",
        occurred_at=now + timedelta(seconds=1),
    )
    await ledger.save_report(base_cancelled)
    remaining_six = position.model_copy(update={"quantity": 6})
    second_time = now + timedelta(seconds=2)
    second = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, remaining_six, now=second_time),
        signals=(signal,),
        markets=(fresh_market(snapshot.market, now=second_time),),
        now=second_time,
    )
    replacement = second.outcomes[0].intent
    assert replacement is not None
    replacement_cancelled = ExecutionReport(
        order_id=replacement.order_id,
        idempotency_key=replacement.idempotency_key,
        status=ExecutionStatus.CANCELLED,
        filled_quantity=2,
        average_fill_price=Decimal("99.85"),
        broker_order_id="paper:r1",
        occurred_at=second_time + timedelta(seconds=1),
    )
    await ledger.save_report(replacement_cancelled)
    remaining_four = position.model_copy(update={"quantity": 4})
    third_time = second_time + timedelta(seconds=2)

    third = await monitor.run_cycle(
        portfolio=portfolio_with(empty_portfolio, remaining_four, now=third_time),
        signals=(signal,),
        markets=(fresh_market(snapshot.market, now=third_time),),
        now=third_time,
    )
    fourth = await monitor.run_cycle(
        portfolio=portfolio_with(
            empty_portfolio,
            remaining_four,
            now=third_time + timedelta(seconds=1),
        ),
        signals=(signal,),
        markets=(fresh_market(snapshot.market, now=third_time + timedelta(seconds=1)),),
        now=third_time + timedelta(seconds=1),
    )

    follow_up = third.outcomes[0].intent
    assert follow_up is not None
    assert follow_up.idempotency_key == f"{base.idempotency_key}:a2"
    assert follow_up.quantity == 4
    assert fourth.outcomes[0].status is ExitMonitorStatus.ALREADY_REQUESTED
    assert [item.idempotency_key for item in executor.calls] == [
        base.idempotency_key,
        replacement.idempotency_key,
        follow_up.idempotency_key,
    ]


@pytest.mark.asyncio
async def test_storage_failure_for_one_position_does_not_suppress_other_stop(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    aapl_signal = signal_from(snapshot, long_insight, decision_time)
    msft_signal = aapl_signal.model_copy(
        update={"signal_id": "signal-msft-storage", "symbol": "MSFT"}
    )
    now = decision_time + timedelta(minutes=10)

    class FaultyLedger(MemoryMonitorLedger):
        async def get_order_intent_by_key(
            self, idempotency_key: str
        ) -> OrderIntent | None:
            if ":AAPL" in idempotency_key:
                raise RuntimeError("simulated storage failure")
            return await super().get_order_intent_by_key(idempotency_key)

    ledger = FaultyLedger()
    executor = RecordingExitExecutor(ledger)
    monitor = ExitMonitor(
        account_id="DU123456",
        ledger=ledger,
        execution_service=executor,
    )
    msft_quote = snapshot.market.quote.model_copy(update={"symbol": "MSFT"})
    msft_market = fresh_market(
        snapshot.market.model_copy(update={"symbol": "MSFT", "quote": msft_quote}),
        now=now,
        last=msft_signal.stop_price,
    )
    portfolio = empty_portfolio.model_copy(
        update={
            "as_of": now,
            "positions": (position_for(aapl_signal), position_for(msft_signal)),
            "broker_connected": True,
            "reconciled": True,
        }
    )

    cycle = await monitor.run_cycle(
        portfolio=portfolio,
        signals=(aapl_signal, msft_signal),
        markets=(
            fresh_market(
                snapshot.market,
                now=now,
                last=aapl_signal.stop_price,
            ),
            msft_market,
        ),
        now=now,
    )

    assert cycle.outcomes[0].reason_codes == ("EXIT_POSITION_EVALUATION_FAILED",)
    assert cycle.outcomes[1].status is ExitMonitorStatus.EXIT_SUBMITTED
    assert [item.symbol for item in executor.calls] == ["MSFT"]


@pytest.mark.asyncio
async def test_parallel_monitor_cycles_have_one_atomic_submission_winner(
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    signal = signal_from(snapshot, long_insight, decision_time)
    position = position_for(signal)
    now = decision_time + timedelta(minutes=10)
    ledger = MemoryMonitorLedger()

    class RacingExecutor(RecordingExitExecutor):
        def __init__(self, value: MemoryMonitorLedger) -> None:
            super().__init__(value)
            self.arrivals = 0
            self.release = asyncio.Event()

        async def submit_with_one_reprice(
            self, intent: OrderIntent
        ) -> tuple[ExecutionReport, ...]:
            self.arrivals += 1
            if self.arrivals == 2:
                self.release.set()
            await self.release.wait()
            if not await self.ledger.save(intent):
                raise OrderIntentClaimLost(intent.order_id)
            self.calls.append(intent)
            submitted = ExecutionReport(
                order_id=intent.order_id,
                idempotency_key=intent.idempotency_key,
                status=ExecutionStatus.SUBMITTED,
                broker_order_id=f"paper:{intent.order_id}",
                occurred_at=intent.created_at,
            )
            await self.ledger.save_report(submitted)
            return (submitted,)

    executor = RacingExecutor(ledger)
    monitor = ExitMonitor(
        account_id="DU123456",
        ledger=ledger,
        execution_service=executor,
    )
    portfolio = portfolio_with(empty_portfolio, position, now=now)
    market = fresh_market(snapshot.market, now=now, last=signal.stop_price)

    cycles = await asyncio.gather(
        monitor.run_cycle(
            portfolio=portfolio,
            signals=(signal,),
            markets=(market,),
            now=now,
        ),
        monitor.run_cycle(
            portfolio=portfolio,
            signals=(signal,),
            markets=(market,),
            now=now,
        ),
    )

    statuses = {cycle.outcomes[0].status for cycle in cycles}
    assert statuses == {
        ExitMonitorStatus.EXIT_SUBMITTED,
        ExitMonitorStatus.BLOCKED,
    }
    assert any(
        outcome.reason_codes == ("EXIT_SUBMISSION_CLAIM_LOST",)
        for cycle in cycles
        for outcome in cycle.outcomes
    )
    assert len(executor.calls) == 1
