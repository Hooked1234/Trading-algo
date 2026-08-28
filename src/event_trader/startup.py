"""Fail-closed startup reconciliation before the runtime loop may process entries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from .broker import Broker, ReadinessProfile, ReconciliationResult
from .domain import ExecutionFill, ExecutionReport, ExecutionStatus, OrderIntent
from .execution import PaperExecutionService
from .providers.ibkr import IBKRRecoveryIncomplete


class StartupExecutionStore(Protocol):
    async def save_execution_report(self, report: ExecutionReport) -> bool: ...


class PaperRecoveryStore(StartupExecutionStore, Protocol):
    async def save_execution_fill(self, fill: ExecutionFill) -> bool: ...

    async def list_order_intents(
        self, *, limit: int = 1_000
    ) -> tuple[OrderIntent, ...]: ...

    async def get_execution_report(
        self, order_id: str
    ) -> ExecutionReport | None: ...


class RecoverablePaperBroker(Broker, Protocol):
    async def restore_from_storage(
        self,
        store: object,
        *,
        max_orders: int = 1_000,
    ) -> tuple[ExecutionReport, ...]: ...


@dataclass(frozen=True, slots=True)
class PaperRecoveryResult:
    """Auditable result of the bounded paper-startup workflow."""

    restored: tuple[ExecutionReport, ...]
    reconciled: ReconciliationResult
    resumed_order_ids: tuple[str, ...]


RestoreHook = Callable[[], Awaitable[object]]
Clock = Callable[[], datetime]


class PaperStartupGate:
    """Restore local state, reconcile broker truth, and persist remote outcomes."""

    def __init__(
        self,
        *,
        broker: Broker,
        store: StartupExecutionStore,
        clock: Clock,
        restore: RestoreHook | None = None,
        max_portfolio_age: timedelta = timedelta(seconds=10),
    ) -> None:
        if max_portfolio_age <= timedelta(0):
            raise ValueError("startup portfolio age must be positive")
        self.broker = broker
        self.store = store
        self.clock = clock
        self.restore = restore
        self.max_portfolio_age = max_portfolio_age

    async def __call__(self) -> None:
        started_at = self.clock()
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise RuntimeError("startup clock must be timezone-aware")
        if self.restore is not None:
            await self.restore()
        reconciliation = await asyncio.to_thread(self.broker.reconcile)
        for report in reconciliation.executions:
            await self.store.save_execution_report(report)
        self.broker.readiness().require()
        portfolio = reconciliation.portfolio
        if not portfolio.broker_connected or not portfolio.reconciled:
            raise RuntimeError("broker portfolio is not reconciled")
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise RuntimeError("startup clock must be timezone-aware")
        if portfolio.as_of > now or now - portfolio.as_of > self.max_portfolio_age:
            raise RuntimeError("broker portfolio snapshot is stale or from the future")


class PaperRecoveryCoordinator:
    """Restore, reconcile, persist, and safely resume paper workflows once."""

    def __init__(
        self,
        *,
        broker: RecoverablePaperBroker,
        store: PaperRecoveryStore,
        execution_service: PaperExecutionService,
        clock: Clock,
        max_orders: int = 1_000,
        max_portfolio_age: timedelta = timedelta(seconds=10),
    ) -> None:
        if max_orders <= 0:
            raise ValueError("max_orders must be positive")
        if max_portfolio_age <= timedelta(0):
            raise ValueError("startup portfolio age must be positive")
        if execution_service.broker is not broker:
            raise ValueError("recovery coordinator and execution service must share a broker")
        if execution_service.ledger is not store:
            raise ValueError("recovery coordinator and execution service must share a store")
        self.broker = broker
        self.store = store
        self.execution_service = execution_service
        self.clock = clock
        self.max_orders = max_orders
        self.max_portfolio_age = max_portfolio_age

    async def __call__(self) -> PaperRecoveryResult:
        self._require_aware(self.clock())
        restored = await self.broker.restore_from_storage(
            self.store,
            max_orders=self.max_orders,
        )
        reconciled = await self._reconcile_and_persist()
        self._validate_portfolio(reconciled)

        intents = await self.store.list_order_intents(limit=self.max_orders + 1)
        paper_intents = tuple(
            intent for intent in intents if intent.submission_mode == "paper"
        )
        if len(intents) > self.max_orders or len(paper_intents) > self.max_orders:
            raise IBKRRecoveryIncomplete(
                f"more than {self.max_orders} paper orders require recovery"
            )
        replacement_by_predecessor = {
            intent.replaces_order_id: intent
            for intent in paper_intents
            if intent.replaces_order_id is not None
        }
        resumed: list[str] = []
        for intent in sorted(paper_intents, key=lambda item: (item.created_at, item.order_id)):
            if intent.order_id in replacement_by_predecessor:
                continue
            latest = await self.store.get_execution_report(intent.order_id)
            if latest is None:
                raise IBKRRecoveryIncomplete(
                    f"paper order {intent.order_id!r} has no reconciled execution report"
                )
            should_resume = latest.status not in {
                ExecutionStatus.FILLED,
                ExecutionStatus.CANCELLED,
                ExecutionStatus.REJECTED,
            } or (
                latest.status is ExecutionStatus.CANCELLED
                and intent.reprice_generation == 0
            )
            if not should_resume:
                continue
            await self.execution_service.resume_persisted_workflow(intent, latest)
            resumed.append(intent.order_id)

        final = await self._reconcile_and_persist()
        self._validate_portfolio(final)
        terminal = {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
        unresolved = tuple(
            report.order_id
            for report in final.executions
            if report.status not in terminal
        )
        if unresolved:
            raise IBKRRecoveryIncomplete(
                f"{len(unresolved)} paper order(s) remain non-terminal after recovery"
            )
        return PaperRecoveryResult(
            restored=restored,
            reconciled=final,
            resumed_order_ids=tuple(resumed),
        )

    async def _reconcile_and_persist(self) -> ReconciliationResult:
        self.broker.readiness(ReadinessProfile.RECONCILE).require()
        result = await asyncio.to_thread(self.broker.reconcile)
        for fill in result.fills:
            await self.store.save_execution_fill(fill)
        for report in result.executions:
            await self.store.save_execution_report(report)
        return result

    def _validate_portfolio(self, reconciliation: ReconciliationResult) -> None:
        portfolio = reconciliation.portfolio
        if not portfolio.broker_connected or not portfolio.reconciled:
            raise RuntimeError("broker portfolio is not reconciled")
        now = self.clock()
        self._require_aware(now)
        if portfolio.as_of > now or now - portfolio.as_of > self.max_portfolio_age:
            raise RuntimeError("broker portfolio snapshot is stale or from the future")

    @staticmethod
    def _require_aware(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError("startup clock must be timezone-aware")


__all__ = [
    "PaperRecoveryCoordinator",
    "PaperRecoveryResult",
    "PaperStartupGate",
]
