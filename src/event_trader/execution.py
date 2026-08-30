"""Paper order lifecycle, one-reprice policy, and deterministic exits."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Protocol

from .broker import Broker, ReadinessProfile
from .calendar import NyseSessionCalendar
from .domain import (
    Direction,
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    Position,
    Signal,
)


class ExecutionLedger(Protocol):
    async def save_order_intent(self, intent: OrderIntent) -> bool: ...

    async def save_execution_report(self, report: ExecutionReport) -> bool: ...

    async def save_execution_fill(self, fill: ExecutionFill) -> bool: ...

    async def get_order_intent_by_key(self, idempotency_key: str) -> OrderIntent | None: ...

    async def get_execution_report(self, order_id: str) -> ExecutionReport | None: ...


Repricer = Callable[[str, OrderSide], Awaitable[Decimal]]
Sleeper = Callable[[float], Awaitable[None]]
PreSubmitGuard = Callable[[OrderIntent], Awaitable[bool]]


class ExecutionError(RuntimeError):
    """Base class for a safely aborted execution workflow."""


class PreSubmitGuardRejected(ExecutionError):
    """A last-moment market or risk check rejected an order."""

    def __init__(self, reasons: tuple[str, ...] | str) -> None:
        # Re-raising from ``args`` would otherwise hand a single string back in,
        # and joining a string splits it into characters.
        self.reasons = (reasons,) if isinstance(reasons, str) else tuple(reasons)
        super().__init__(", ".join(self.reasons))


class ExecutionReconciliationError(ExecutionError):
    """The broker omitted an order expected by the local workflow."""


class OrderIntentClaimLost(ExecutionError):
    """Another worker durably claimed the same immutable order intent."""


class NonPaperIntentRejected(ExecutionError):
    """A shadow/research intent reached an execution boundary."""


class ReplacementSafetyError(ExecutionError):
    """A replacement lineage or restart invariant was violated."""


TERMINAL_STATUSES = frozenset(
    {ExecutionStatus.FILLED, ExecutionStatus.CANCELLED, ExecutionStatus.REJECTED}
)


class PaperExecutionService:
    """Persist before submit and never overlap an original and replacement order."""

    def __init__(
        self,
        *,
        broker: Broker,
        ledger: ExecutionLedger,
        repricer: Repricer,
        wait_seconds: float = 5.0,
        cancel_reconcile_attempts: int = 3,
        sleep: Sleeper = asyncio.sleep,
        pre_submit_guard: PreSubmitGuard | None = None,
        promotion_artifact_sha256: str | None = None,
    ) -> None:
        if wait_seconds <= 0:
            raise ValueError("wait_seconds must be positive")
        if cancel_reconcile_attempts <= 0:
            raise ValueError("cancel_reconcile_attempts must be positive")
        if promotion_artifact_sha256 is not None and (
            len(promotion_artifact_sha256) != 64
            or any(character not in "0123456789abcdef" for character in promotion_artifact_sha256)
        ):
            raise ValueError("promotion artifact SHA-256 must be lowercase hexadecimal")
        self.broker = broker
        self.ledger = ledger
        self.repricer = repricer
        self.wait_seconds = wait_seconds
        self.cancel_reconcile_attempts = cancel_reconcile_attempts
        self.sleep = sleep
        self._pre_submit_guard = pre_submit_guard
        self.promotion_artifact_sha256 = promotion_artifact_sha256
        self._submission_lock = asyncio.Lock()

    @property
    def has_pre_submit_guard(self) -> bool:
        """Whether every broker submission has a last-moment async guard."""

        return self._pre_submit_guard is not None

    async def submit_with_one_reprice(self, intent: OrderIntent) -> tuple[ExecutionReport, ...]:
        self._require_paper_intent(intent)
        profile = (
            ReadinessProfile.SUBMIT
            if intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT}
            else ReadinessProfile.EXIT
        )
        self.broker.readiness(profile).require()
        reports: list[ExecutionReport] = []
        report = await self._submit_persisted(intent)
        reports.append(report)
        if report.status in TERMINAL_STATUSES:
            return tuple(reports)
        reports.extend(await self._complete_open_order(intent, allow_reprice=True))
        return tuple(reports)

    async def resume_persisted_workflow(
        self,
        intent: OrderIntent,
        latest_report: ExecutionReport | None = None,
    ) -> tuple[ExecutionReport, ...]:
        """Continue a known workflow after restart without resubmitting its order."""

        self._require_paper_intent(intent)
        reports: list[ExecutionReport] = []
        latest = latest_report
        if (
            latest is None
            or latest.status not in TERMINAL_STATUSES
            or latest.status is ExecutionStatus.CANCELLED
        ):
            latest = await self.reconcile_order(intent.order_id)
            reports.append(latest)
        if latest.status in TERMINAL_STATUSES:
            if latest.status is ExecutionStatus.CANCELLED and not self._is_replacement(intent):
                reports.extend(await self.resume_reprice_after_cancel(intent, latest))
            return tuple(reports)

        reports.extend(
            await self._complete_open_order(
                intent,
                allow_reprice=not self._is_replacement(intent),
            )
        )
        return tuple(reports)

    async def resume_reprice_after_cancel(
        self,
        intent: OrderIntent,
        cancellation: ExecutionReport,
    ) -> tuple[ExecutionReport, ...]:
        """Resume the single deterministic reprice after a confirmed cancel.

        This is safe to call after restart when the base cancellation was
        persisted but the replacement intent was not yet created.
        """

        self._require_paper_intent(intent)
        if cancellation.order_id != intent.order_id:
            raise ValueError("cancellation order id does not match intent")
        if cancellation.idempotency_key != intent.idempotency_key:
            raise ValueError("cancellation idempotency key does not match intent")
        if cancellation.status is not ExecutionStatus.CANCELLED:
            raise ValueError("replacement requires a confirmed cancelled order")
        if self._is_replacement(intent):
            raise ReplacementSafetyError("a replacement order cannot create another replacement")
        remaining_quantity = intent.quantity - cancellation.filled_quantity
        if remaining_quantity <= 0:
            return ()
        replacement_key = f"{intent.idempotency_key}:r1"
        existing = await self.ledger.get_order_intent_by_key(replacement_key)
        if existing is not None:
            if (
                existing.reprice_generation != 1
                or existing.replaces_order_id != intent.order_id
                or existing.quantity != remaining_quantity
                or existing.side is not intent.side
                or existing.symbol != intent.symbol
            ):
                raise ReplacementSafetyError(
                    "persisted replacement does not match its cancelled predecessor"
                )
            latest = await self.ledger.get_execution_report(existing.order_id)
            resumed = await self.resume_persisted_workflow(existing, latest)
            return resumed or ((latest,) if latest is not None else ())
        new_price = await self.repricer(intent.symbol, intent.side)
        replacement_data = intent.model_dump()
        replacement_data.update(
            {
                "order_id": f"{intent.order_id}-r1",
                "idempotency_key": replacement_key,
                "quantity": remaining_quantity,
                "limit_price": new_price,
                "created_at": cancellation.occurred_at,
                "replaces_order_id": intent.order_id,
                "reprice_generation": intent.reprice_generation + 1,
            }
        )
        replacement = OrderIntent.model_validate(replacement_data)
        reports: list[ExecutionReport] = []
        replacement_report = await self._submit_persisted(replacement)
        reports.append(replacement_report)
        if replacement_report.status not in TERMINAL_STATUSES:
            reports.extend(await self._complete_open_order(replacement, allow_reprice=False))
        return tuple(reports)

    async def _complete_open_order(
        self,
        intent: OrderIntent,
        *,
        allow_reprice: bool,
    ) -> tuple[ExecutionReport, ...]:
        """Wait once, then cancel and reconcile to a bounded terminal outcome."""

        reports: list[ExecutionReport] = []
        await self.sleep(self.wait_seconds)
        latest = await self.reconcile_order(intent.order_id)
        reports.append(latest)
        if latest.status in TERMINAL_STATUSES:
            return tuple(reports)

        confirmations = await self._cancel_until_terminal(intent.order_id)
        reports.extend(confirmations)
        terminal = confirmations[-1]
        if allow_reprice and terminal.status is ExecutionStatus.CANCELLED:
            reports.extend(await self.resume_reprice_after_cancel(intent, terminal))
        return tuple(reports)

    async def _cancel_until_terminal(self, order_id: str) -> tuple[ExecutionReport, ...]:
        """Issue one idempotent cancel and require bounded broker confirmation."""

        reports: list[ExecutionReport] = []
        cancellation = self.broker.cancel(order_id)
        await self.ledger.save_execution_report(cancellation)
        reports.append(cancellation)
        for attempt in range(self.cancel_reconcile_attempts):
            confirmation = await self.reconcile_order(order_id)
            reports.append(confirmation)
            if confirmation.status in TERMINAL_STATUSES:
                return tuple(reports)
            if attempt + 1 < self.cancel_reconcile_attempts:
                await self.sleep(self.wait_seconds)
        raise ExecutionReconciliationError(
            f"broker did not confirm a terminal cancel outcome for {order_id!r} "
            f"after {self.cancel_reconcile_attempts} reconciliation attempts"
        )

    async def reconcile_order(self, order_id: str) -> ExecutionReport:
        """Refresh and persist one known order without submitting it."""

        reconciliation = self.broker.reconcile()
        for fill in reconciliation.fills:
            await self.ledger.save_execution_fill(fill)
        found: ExecutionReport | None = None
        for report in reconciliation.executions:
            await self.ledger.save_execution_report(report)
            if report.order_id == order_id:
                found = report
        if found is None:
            raise ExecutionReconciliationError(
                "broker reconciliation omitted a locally submitted order"
            )
        return found

    async def _submit_persisted(self, intent: OrderIntent) -> ExecutionReport:
        self._require_paper_intent(intent)
        self._require_entry_promotion(intent)
        async with self._submission_lock:
            if self._pre_submit_guard is not None:
                allowed = await self._pre_submit_guard(intent)
                if allowed is not True:
                    raise PreSubmitGuardRejected(("PRE_SUBMIT_GUARD_REJECTED",))
            created = await self.ledger.save_order_intent(intent)
            if not created:
                raise OrderIntentClaimLost(f"order intent {intent.order_id!r} was already claimed")
            report = self.broker.submit(intent)
        await self.ledger.save_execution_report(report)
        return report

    @staticmethod
    def _is_replacement(intent: OrderIntent) -> bool:
        return intent.reprice_generation > 0

    @staticmethod
    def _require_paper_intent(intent: OrderIntent) -> None:
        if intent.submission_mode != "paper":
            raise NonPaperIntentRejected(
                f"order {intent.order_id!r} is not a paper-submission intent"
            )

    def _require_entry_promotion(self, intent: OrderIntent) -> None:
        if intent.side not in {OrderSide.BUY, OrderSide.SELL_SHORT}:
            return
        if (
            self.promotion_artifact_sha256 is None
            or intent.research_promotion_sha256 != self.promotion_artifact_sha256
        ):
            raise NonPaperIntentRejected(
                f"order {intent.order_id!r} lacks the configured research promotion"
            )


class ExitPolicy:
    def __init__(self, calendar: NyseSessionCalendar | None = None) -> None:
        self.calendar = calendar or NyseSessionCalendar()

    def reason(
        self,
        *,
        signal: Signal,
        position: Position,
        market: MarketSnapshot,
        now: datetime,
    ) -> str | None:
        symbols = {
            position.symbol.strip().upper(),
            signal.symbol.strip().upper(),
            market.symbol.strip().upper(),
        }
        if len(symbols) != 1:
            raise ValueError("signal, position, and market symbols must match")
        if position.direction is not signal.direction:
            raise ValueError("position direction must match signal direction")
        if self.calendar.force_flat_due(now):
            return "FORCE_FLAT_1555"
        holding_deadline = signal.decided_at + timedelta(minutes=signal.holding_minutes)
        if now >= min(signal.expires_at, holding_deadline):
            return "TIME_EXIT"
        if signal.direction is Direction.LONG and market.last <= signal.stop_price:
            return "STOP_EXIT"
        if signal.direction is Direction.SHORT and market.last >= signal.stop_price:
            return "STOP_EXIT"
        return None

    def order(
        self,
        *,
        signal: Signal,
        position: Position,
        market: MarketSnapshot,
        account_id: str,
        now: datetime,
        idempotency_scope: str | None = None,
    ) -> OrderIntent | None:
        reason = self.reason(signal=signal, position=position, market=market, now=now)
        if reason is None:
            return None
        side = OrderSide.SELL if position.direction is Direction.LONG else OrderSide.BUY_TO_COVER
        limit_price = market.quote.bid if side is OrderSide.SELL else market.quote.ask
        idempotency_key = (
            idempotency_scope.strip()
            if idempotency_scope is not None
            else f"{signal.signal_id}:exit:{reason}"
        )
        if not idempotency_key:
            raise ValueError("idempotency_scope must not be empty")
        digest = sha256(idempotency_key.encode()).hexdigest()[:24]
        return OrderIntent(
            order_id=f"exit-{digest}",
            idempotency_key=idempotency_key,
            signal_id=signal.signal_id,
            account_id=account_id,
            submission_mode="paper",
            symbol=signal.symbol,
            side=side,
            quantity=position.quantity,
            limit_price=limit_price,
            created_at=now,
        )
