"""Paper-only broker contracts and deterministic replay implementation.

The execution boundary is intentionally small: callers can check readiness,
submit, cancel, and reconcile.  Every implementation must pass through an
explicit paper-account allowlist before it may touch a transport.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from threading import RLock
from typing import ClassVar, Protocol, runtime_checkable

from .domain import (
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PortfolioState,
    utc_now,
)

Clock = Callable[[], datetime]


class BrokerError(RuntimeError):
    """Base class for errors at the broker boundary."""


class PaperAccountViolation(BrokerError):
    """Raised before transport access when an account is not explicitly paper."""


class BrokerNotReady(BrokerError):
    """Raised when a submit or cancel is attempted without full readiness."""


class UnknownOrder(BrokerError):
    """Raised when an order identifier has never been registered locally."""


class IdempotencyConflict(BrokerError):
    """Raised when an idempotency key is reused for different order content."""


class InvalidOrderTransition(BrokerError):
    """Raised when an execution report would move an order backwards."""


class ReadinessProfile(StrEnum):
    """Operation whose fail-closed prerequisites are being evaluated."""

    SUBMIT = "submit"
    EXIT = "exit"
    CANCEL = "cancel"
    RECONCILE = "reconcile"


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    """One fail-closed prerequisite for broker operations."""

    name: str
    ready: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class BrokerReadiness:
    """Snapshot of all checks required before placing or cancelling an order."""

    account_id: str
    checked_at: datetime
    checks: tuple[ReadinessCheck, ...]

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.ready for check in self.checks)

    @property
    def failures(self) -> tuple[ReadinessCheck, ...]:
        return tuple(check for check in self.checks if not check.ready)

    def require(self) -> None:
        if self.ready:
            return
        reasons = "; ".join(
            f"{check.name}: {check.detail or 'not ready'}" for check in self.failures
        )
        raise BrokerNotReady(reasons or "broker readiness checks failed")


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Canonical local view after comparing broker and local execution state."""

    account_id: str
    reconciled_at: datetime
    executions: tuple[ExecutionReport, ...]
    portfolio: PortfolioState
    fills: tuple[ExecutionFill, ...] = ()


class PaperAccountGuard:
    """Hard gate that only permits explicitly allowlisted paper accounts.

    Account-name conventions (for example ``DU...``) are deliberately not used
    as proof.  They can change and are not a safe substitute for configuration.
    """

    def __init__(self, paper_account_allowlist: Iterable[str]) -> None:
        allowlist = frozenset(
            account.strip() for account in paper_account_allowlist if account.strip()
        )
        if not allowlist:
            raise ValueError("paper_account_allowlist must not be empty")
        self._allowlist = allowlist

    @property
    def allowlist(self) -> frozenset[str]:
        return self._allowlist

    def assert_paper(self, account_id: str, environment: str) -> None:
        if environment.strip().lower() != "paper":
            raise PaperAccountViolation(
                "live execution is disabled; environment must be exactly 'paper'"
            )
        if account_id not in self._allowlist:
            raise PaperAccountViolation(
                f"account {account_id!r} is not in the paper-account allowlist"
            )


@runtime_checkable
class Broker(Protocol):
    """Execution interface shared by IBKR, tests, and deterministic replays."""

    def readiness(self, profile: ReadinessProfile = ReadinessProfile.SUBMIT) -> BrokerReadiness:
        """Return fail-closed operational readiness."""

    def submit(self, intent: OrderIntent) -> ExecutionReport:
        """Submit once per idempotency key."""

    def cancel(self, order_id: str) -> ExecutionReport:
        """Cancel an existing non-terminal order idempotently."""

    def reconcile(self) -> ReconciliationResult:
        """Reconcile remote execution state with the canonical local state."""


class OrderStateMachine:
    """Thread-safe, monotonic and idempotent execution-report store."""

    _ALLOWED: ClassVar[dict[ExecutionStatus, frozenset[ExecutionStatus]]] = {
        ExecutionStatus.PENDING: frozenset(
            {
                ExecutionStatus.PENDING,
                ExecutionStatus.SUBMITTED,
                ExecutionStatus.CANCELLED,
                ExecutionStatus.REJECTED,
            }
        ),
        ExecutionStatus.SUBMITTED: frozenset(
            {
                ExecutionStatus.SUBMITTED,
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
                ExecutionStatus.CANCELLED,
                ExecutionStatus.REJECTED,
            }
        ),
        ExecutionStatus.PARTIALLY_FILLED: frozenset(
            {
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
                ExecutionStatus.CANCELLED,
            }
        ),
        ExecutionStatus.FILLED: frozenset({ExecutionStatus.FILLED}),
        # A cancel acknowledgement can race a final fill callback.  Broker
        # truth may therefore promote a cancelled order to fully filled.
        ExecutionStatus.CANCELLED: frozenset({ExecutionStatus.CANCELLED, ExecutionStatus.FILLED}),
        ExecutionStatus.REJECTED: frozenset({ExecutionStatus.REJECTED}),
    }
    _RESTORE_ALLOWED: ClassVar[dict[ExecutionStatus, frozenset[ExecutionStatus]]] = {
        ExecutionStatus.PENDING: frozenset(
            {
                ExecutionStatus.PENDING,
                ExecutionStatus.SUBMITTED,
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
                ExecutionStatus.CANCELLED,
                ExecutionStatus.REJECTED,
            }
        ),
        ExecutionStatus.SUBMITTED: _ALLOWED[ExecutionStatus.SUBMITTED],
        ExecutionStatus.PARTIALLY_FILLED: _ALLOWED[ExecutionStatus.PARTIALLY_FILLED],
        ExecutionStatus.FILLED: _ALLOWED[ExecutionStatus.FILLED],
        ExecutionStatus.CANCELLED: _ALLOWED[ExecutionStatus.CANCELLED],
        ExecutionStatus.REJECTED: _ALLOWED[ExecutionStatus.REJECTED],
    }

    def __init__(self) -> None:
        self._lock = RLock()
        self._intents: dict[str, OrderIntent] = {}
        self._reports: dict[str, ExecutionReport] = {}
        self._orders_by_key: dict[str, str] = {}

    def begin(self, intent: OrderIntent, occurred_at: datetime) -> tuple[ExecutionReport, bool]:
        """Register an intent and return ``(report, created)``.

        Replaying the exact intent returns the current canonical report.  Reuse
        of either the order id or idempotency key for different content fails.
        """

        with self._lock:
            existing_order_id = self._orders_by_key.get(intent.idempotency_key)
            if existing_order_id is not None:
                existing_intent = self._intents[existing_order_id]
                if existing_intent != intent:
                    raise IdempotencyConflict(
                        f"idempotency key {intent.idempotency_key!r} was reused"
                    )
                return self._reports[existing_order_id], False

            if intent.order_id in self._intents:
                raise IdempotencyConflict(
                    f"order id {intent.order_id!r} was reused with another key"
                )

            report = ExecutionReport(
                order_id=intent.order_id,
                idempotency_key=intent.idempotency_key,
                status=ExecutionStatus.PENDING,
                occurred_at=occurred_at,
            )
            self._intents[intent.order_id] = intent
            self._reports[intent.order_id] = report
            self._orders_by_key[intent.idempotency_key] = intent.order_id
            return report, True

    def transition(self, report: ExecutionReport) -> ExecutionReport:
        """Apply a monotonic report, returning the canonical stored instance."""

        with self._lock:
            current = self._reports.get(report.order_id)
            if current is None:
                raise UnknownOrder(report.order_id)
            if current.idempotency_key != report.idempotency_key:
                raise IdempotencyConflict(f"execution key does not match order {report.order_id!r}")
            if report.occurred_at < current.occurred_at:
                raise InvalidOrderTransition("execution timestamp cannot move backwards")
            if (
                current.broker_order_id is not None
                and report.broker_order_id is not None
                and current.broker_order_id != report.broker_order_id
            ):
                raise InvalidOrderTransition("broker order id cannot change")
            if report.status not in self._ALLOWED[current.status]:
                raise InvalidOrderTransition(
                    f"cannot move {report.order_id!r} from "
                    f"{current.status.value} to {report.status.value}"
                )

            intent = self._intents[report.order_id]
            if report.filled_quantity < current.filled_quantity:
                raise InvalidOrderTransition("filled quantity cannot decrease")
            if report.fees < current.fees:
                raise InvalidOrderTransition("execution fees cannot decrease")
            if report.fill_count < current.fill_count:
                raise InvalidOrderTransition("counted fills cannot decrease")
            if report.update_sequence < current.update_sequence:
                raise InvalidOrderTransition("broker update sequence cannot move backwards")
            if report.filled_quantity > intent.quantity:
                raise InvalidOrderTransition("filled quantity exceeds order quantity")
            if report.status is ExecutionStatus.FILLED:
                if report.filled_quantity != intent.quantity:
                    raise InvalidOrderTransition(
                        "filled status requires the complete order quantity"
                    )
                if report.average_fill_price <= 0:
                    raise InvalidOrderTransition(
                        "filled status requires a positive average fill price"
                    )
            if (
                report.status is ExecutionStatus.PARTIALLY_FILLED
                and not 0 < report.filled_quantity < intent.quantity
            ):
                raise InvalidOrderTransition(
                    "partial fill quantity must be between zero and order quantity"
                )

            if current.status is report.status and report == current:
                return current
            if (
                current.status
                in {
                    ExecutionStatus.FILLED,
                    ExecutionStatus.CANCELLED,
                    ExecutionStatus.REJECTED,
                }
                and current.status is report.status
                and not _adds_terminal_execution_evidence(current, report)
            ):
                return current

            self._reports[report.order_id] = report
            return report

    def restore(
        self,
        intent: OrderIntent,
        latest_report: ExecutionReport | None,
    ) -> ExecutionReport:
        """Hydrate one persisted order without invoking a broker transport.

        Restore accepts a direct persisted ``PENDING`` to partial/full-fill jump
        because a process may have crashed before storing intermediate broker
        callbacks. Normal runtime transitions remain deliberately stricter.
        """

        with self._lock:
            existing_order_id = self._orders_by_key.get(intent.idempotency_key)
            if existing_order_id is not None and existing_order_id != intent.order_id:
                raise IdempotencyConflict(f"idempotency key {intent.idempotency_key!r} was reused")
            existing_intent = self._intents.get(intent.order_id)
            if existing_intent is not None and existing_intent != intent:
                raise IdempotencyConflict(
                    f"order id {intent.order_id!r} was reused with another key"
                )

            candidate = latest_report or ExecutionReport(
                order_id=intent.order_id,
                idempotency_key=intent.idempotency_key,
                status=ExecutionStatus.PENDING,
                occurred_at=intent.created_at,
            )
            self._validate_restored_report(intent, candidate)

            current = self._reports.get(intent.order_id)
            if current is None:
                self._intents[intent.order_id] = intent
                self._reports[intent.order_id] = candidate
                self._orders_by_key[intent.idempotency_key] = intent.order_id
                return candidate
            if latest_report is None or candidate == current:
                return current
            if candidate.occurred_at < current.occurred_at:
                raise InvalidOrderTransition("execution timestamp cannot move backwards")
            if candidate.status not in self._RESTORE_ALLOWED[current.status]:
                raise InvalidOrderTransition(
                    f"cannot restore {candidate.order_id!r} from "
                    f"{current.status.value} to {candidate.status.value}"
                )
            if candidate.filled_quantity < current.filled_quantity:
                raise InvalidOrderTransition("filled quantity cannot decrease")
            if candidate.fees < current.fees:
                raise InvalidOrderTransition("execution fees cannot decrease")
            if candidate.fill_count < current.fill_count:
                raise InvalidOrderTransition("counted fills cannot decrease")
            if candidate.update_sequence < current.update_sequence:
                raise InvalidOrderTransition("broker update sequence cannot move backwards")
            if (
                current.broker_order_id is not None
                and candidate.broker_order_id is not None
                and current.broker_order_id != candidate.broker_order_id
            ):
                raise InvalidOrderTransition("broker order id cannot change")
            if current.status is candidate.status and candidate == current:
                return current
            if (
                current.status
                in {
                    ExecutionStatus.FILLED,
                    ExecutionStatus.CANCELLED,
                    ExecutionStatus.REJECTED,
                }
                and current.status is candidate.status
                and not _adds_terminal_execution_evidence(current, candidate)
            ):
                return current
            if current.broker_order_id is not None and candidate.broker_order_id is None:
                candidate = candidate.model_copy(
                    update={"broker_order_id": current.broker_order_id}
                )
            self._reports[intent.order_id] = candidate
            return candidate

    @staticmethod
    def _validate_restored_report(intent: OrderIntent, report: ExecutionReport) -> None:
        if report.order_id != intent.order_id:
            raise IdempotencyConflict("execution report order id does not match intent")
        if report.idempotency_key != intent.idempotency_key:
            raise IdempotencyConflict("execution report idempotency key does not match intent")
        if report.occurred_at < intent.created_at:
            raise InvalidOrderTransition("execution report predates its order intent")
        if report.broker_order_id is not None and not report.broker_order_id.strip():
            raise InvalidOrderTransition("broker order id cannot be empty")
        if report.filled_quantity > intent.quantity:
            raise InvalidOrderTransition("filled quantity exceeds order quantity")
        if report.filled_quantity > 0 and report.average_fill_price <= 0:
            raise InvalidOrderTransition("a fill requires a positive average fill price")
        if report.status is ExecutionStatus.FILLED and report.filled_quantity != intent.quantity:
            raise InvalidOrderTransition("filled status requires the complete order quantity")
        if (
            report.status is ExecutionStatus.PARTIALLY_FILLED
            and not 0 < report.filled_quantity < intent.quantity
        ):
            raise InvalidOrderTransition(
                "partial fill quantity must be between zero and order quantity"
            )
        if (
            report.status
            in {
                ExecutionStatus.PENDING,
                ExecutionStatus.SUBMITTED,
                ExecutionStatus.REJECTED,
            }
            and report.filled_quantity != 0
        ):
            raise InvalidOrderTransition(f"{report.status.value} report cannot contain fills")

    def current(self, order_id: str) -> ExecutionReport:
        with self._lock:
            try:
                return self._reports[order_id]
            except KeyError as exc:
                raise UnknownOrder(order_id) from exc

    def intent(self, order_id: str) -> OrderIntent:
        with self._lock:
            try:
                return self._intents[order_id]
            except KeyError as exc:
                raise UnknownOrder(order_id) from exc

    def reports(self) -> tuple[ExecutionReport, ...]:
        with self._lock:
            return tuple(
                sorted(
                    self._reports.values(),
                    key=lambda report: (report.occurred_at, report.order_id),
                )
            )


def _adds_terminal_execution_evidence(
    current: ExecutionReport,
    incoming: ExecutionReport,
) -> bool:
    """Ignore message/timestamp-only terminal replays but retain accounting facts."""

    return any(
        (
            incoming.broker_order_id != current.broker_order_id,
            incoming.filled_quantity != current.filled_quantity,
            incoming.average_fill_price != current.average_fill_price,
            incoming.fees != current.fees,
            incoming.slippage_bps != current.slippage_bps,
            incoming.fill_count != current.fill_count,
            incoming.pending_commission != current.pending_commission,
        )
    )


class InMemoryPaperBroker:
    """Deterministic paper broker for unit tests, replays, and dry runs."""

    def __init__(
        self,
        *,
        account_id: str = "paper",
        paper_account_allowlist: Iterable[str] | None = None,
        initial_cash: Decimal = Decimal("100000"),
        clock: Clock = utc_now,
    ) -> None:
        if initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        self.account_id = account_id
        self.environment = "paper"
        self._guard = PaperAccountGuard(
            paper_account_allowlist if paper_account_allowlist is not None else (account_id,)
        )
        self._clock = clock
        self._state = OrderStateMachine()
        self._connected = True
        self._reconciled = True
        now = self._clock()
        self._portfolio = PortfolioState(
            as_of=now,
            nav=initial_cash,
            peak_nav=initial_cash,
            cash=initial_cash,
            strategy_equity=min(initial_cash, Decimal("100000")),
            strategy_peak_equity=min(initial_cash, Decimal("100000")),
            strategy_realized_pnl_today=Decimal("0"),
            strategy_unrealized_pnl=Decimal("0"),
        )

    def readiness(self, profile: ReadinessProfile = ReadinessProfile.SUBMIT) -> BrokerReadiness:
        checks: list[ReadinessCheck] = []
        try:
            self._guard.assert_paper(self.account_id, self.environment)
        except PaperAccountViolation as exc:
            checks.append(ReadinessCheck("paper_account", False, str(exc)))
        else:
            checks.append(ReadinessCheck("paper_account", True))
        checks.append(ReadinessCheck("connected", self._connected, "broker disconnected"))
        if profile is not ReadinessProfile.RECONCILE:
            checks.append(
                ReadinessCheck("reconciled", self._reconciled, "state has not been reconciled")
            )
        return BrokerReadiness(self.account_id, self._clock(), tuple(checks))

    def submit(self, intent: OrderIntent) -> ExecutionReport:
        self._guard.assert_paper(intent.account_id, intent.environment)
        if intent.account_id != self.account_id:
            raise PaperAccountViolation("intent account differs from broker account")
        profile = (
            ReadinessProfile.SUBMIT
            if intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT}
            else ReadinessProfile.EXIT
        )
        self.readiness(profile).require()
        pending, created = self._state.begin(intent, self._clock())
        if not created:
            return pending
        return self._state.transition(
            pending.model_copy(
                update={
                    "status": ExecutionStatus.SUBMITTED,
                    "broker_order_id": f"memory:{intent.order_id}",
                    "occurred_at": self._clock(),
                }
            )
        )

    def submit_order(self, intent: OrderIntent) -> ExecutionReport:
        return self.submit(intent)

    def cancel(self, order_id: str) -> ExecutionReport:
        self._guard.assert_paper(self.account_id, self.environment)
        self.readiness(ReadinessProfile.CANCEL).require()
        current = self._state.current(order_id)
        if current.status in {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }:
            return current
        return self._state.transition(
            current.model_copy(
                update={
                    "status": ExecutionStatus.CANCELLED,
                    "occurred_at": self._clock(),
                }
            )
        )

    def cancel_order(self, order_id: str) -> ExecutionReport:
        return self.cancel(order_id)

    def record_execution(self, report: ExecutionReport) -> ExecutionReport:
        """Apply a replayed or simulated broker callback to an existing order."""

        return self._state.transition(report)

    def set_portfolio(self, portfolio: PortfolioState) -> None:
        """Replace portfolio state during a deterministic replay."""

        self._portfolio = portfolio

    def reconcile(self, remote_reports: Iterable[ExecutionReport] = ()) -> ReconciliationResult:
        self._guard.assert_paper(self.account_id, self.environment)
        if not self._connected:
            raise BrokerNotReady("broker disconnected")
        for report in remote_reports:
            self._state.transition(report)
        self._reconciled = True
        now = self._clock()
        self._portfolio = self._portfolio.model_copy(
            update={"as_of": now, "broker_connected": True, "reconciled": True}
        )
        return ReconciliationResult(
            account_id=self.account_id,
            reconciled_at=now,
            executions=self._state.reports(),
            portfolio=self._portfolio,
        )

    @property
    def reports(self) -> tuple[ExecutionReport, ...]:
        return self._state.reports()
