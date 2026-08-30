"""Persistent, repeatable exit monitoring for paper positions."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Protocol

from .broker import BrokerError
from .domain import (
    Direction,
    ExecutionReport,
    ExecutionStatus,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Position,
    Signal,
)
from .execution import (
    ExecutionError,
    ExecutionReconciliationError,
    ExitPolicy,
    OrderIntentClaimLost,
    PreSubmitGuardRejected,
)


class ExitMonitorLedger(Protocol):
    """Durable lookup used to suppress duplicate exit workflows after restart."""

    async def get_order_intent_by_key(self, idempotency_key: str) -> OrderIntent | None: ...

    async def get_execution_report(self, order_id: str) -> ExecutionReport | None: ...


class ExitExecutor(Protocol):
    ledger: object

    @property
    def has_pre_submit_guard(self) -> bool: ...

    async def submit_with_one_reprice(self, intent: OrderIntent) -> tuple[ExecutionReport, ...]: ...

    async def reconcile_order(self, order_id: str) -> ExecutionReport: ...

    async def resume_reprice_after_cancel(
        self,
        intent: OrderIntent,
        cancellation: ExecutionReport,
    ) -> tuple[ExecutionReport, ...]: ...

    async def resume_persisted_workflow(
        self,
        intent: OrderIntent,
        latest_report: ExecutionReport | None = None,
    ) -> tuple[ExecutionReport, ...]: ...


class ExitMonitorStatus(StrEnum):
    MONITORING = "monitoring"
    EXIT_SUBMITTED = "exit_submitted"
    ALREADY_REQUESTED = "already_requested"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class ExitMonitorOutcome:
    symbol: str
    status: ExitMonitorStatus
    exit_reason: str | None = None
    reason_codes: tuple[str, ...] = ()
    intent: OrderIntent | None = None
    execution_reports: tuple[ExecutionReport, ...] = ()


@dataclass(frozen=True, slots=True)
class ExitMonitorCycle:
    checked_at: datetime
    outcomes: tuple[ExitMonitorOutcome, ...]
    reason_codes: tuple[str, ...] = ()

    @property
    def ready(self) -> bool:
        return not self.reason_codes and not self.blocked

    @property
    def blocked(self) -> tuple[ExitMonitorOutcome, ...]:
        return tuple(
            outcome for outcome in self.outcomes if outcome.status is ExitMonitorStatus.BLOCKED
        )


class ExitMonitor:
    """Evaluate every open position once per externally scheduled cycle.

    The durable exit key is independent of trigger reason and market price. A
    later cycle therefore observes the original workflow instead of creating a
    second exit when, for example, a stop exit is followed by force-flat.
    """

    def __init__(
        self,
        *,
        account_id: str,
        ledger: ExitMonitorLedger,
        execution_service: ExitExecutor,
        exit_policy: ExitPolicy | None = None,
        max_state_age: timedelta = timedelta(seconds=10),
        future_tolerance: timedelta = timedelta(seconds=1),
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id must not be empty")
        if max_state_age <= timedelta(0):
            raise ValueError("max_state_age must be positive")
        if future_tolerance < timedelta(0):
            raise ValueError("future_tolerance cannot be negative")
        if execution_service.ledger is not ledger:
            raise ValueError("exit monitor and execution service must share a ledger")
        if not execution_service.has_pre_submit_guard:
            raise ValueError("exit monitor requires a pre-submit guard")
        self.account_id = account_id.strip()
        self.ledger = ledger
        self.execution_service = execution_service
        self.exit_policy = exit_policy or ExitPolicy()
        self.max_state_age = max_state_age
        self.future_tolerance = future_tolerance

    async def run_cycle(
        self,
        *,
        portfolio: PortfolioState,
        signals: Iterable[Signal],
        markets: Iterable[MarketSnapshot],
        now: datetime,
    ) -> ExitMonitorCycle:
        """Run one deterministic cycle; scheduling remains outside the monitor."""

        self._require_aware(now)
        signals_by_symbol: dict[str, list[Signal]] = defaultdict(list)
        markets_by_symbol: dict[str, list[MarketSnapshot]] = defaultdict(list)
        for signal in signals:
            signals_by_symbol[self._symbol(signal.symbol)].append(signal)
        for market in markets:
            markets_by_symbol[self._symbol(market.symbol)].append(market)

        portfolio_failures = self._portfolio_failures(portfolio, now)
        outcomes: list[ExitMonitorOutcome] = []
        for position in sorted(portfolio.positions, key=lambda item: item.symbol):
            symbol = position.symbol.strip().upper() or "<INVALID>"
            try:
                outcome = await self._evaluate_position(
                    position=position,
                    signals_by_symbol=signals_by_symbol,
                    markets_by_symbol=markets_by_symbol,
                    portfolio_failures=portfolio_failures,
                    now=now,
                )
            except PreSubmitGuardRejected as exc:
                outcome = self._blocked(symbol, exc.reasons)
            except OrderIntentClaimLost:
                outcome = self._blocked(symbol, ("EXIT_SUBMISSION_CLAIM_LOST",))
            except (BrokerError, ExecutionError):
                outcome = self._blocked(symbol, ("EXIT_EXECUTION_FAILED",))
            except Exception:
                # One corrupt symbol, storage fault, or provider bug must not
                # suppress independent stops/force-flat actions for others.
                outcome = self._blocked(
                    symbol,
                    ("EXIT_POSITION_EVALUATION_FAILED",),
                )
            outcomes.append(outcome)
        return ExitMonitorCycle(
            checked_at=now,
            outcomes=tuple(outcomes),
            reason_codes=portfolio_failures,
        )

    async def run_once(
        self,
        *,
        portfolio: PortfolioState,
        signals: Iterable[Signal],
        markets: Iterable[MarketSnapshot],
        now: datetime,
    ) -> ExitMonitorCycle:
        """Alias for schedulers that call their jobs ``run_once``."""

        return await self.run_cycle(
            portfolio=portfolio,
            signals=signals,
            markets=markets,
            now=now,
        )

    async def _evaluate_position(
        self,
        *,
        position: Position,
        signals_by_symbol: dict[str, list[Signal]],
        markets_by_symbol: dict[str, list[MarketSnapshot]],
        portfolio_failures: tuple[str, ...],
        now: datetime,
    ) -> ExitMonitorOutcome:
        symbol = self._symbol(position.symbol)
        if portfolio_failures:
            return self._blocked(symbol, portfolio_failures)

        signal_candidates = signals_by_symbol.get(symbol, [])
        market_candidates = markets_by_symbol.get(symbol, [])
        failures: list[str] = []
        if not signal_candidates:
            failures.append("MISSING_SIGNAL_STATE")
        elif len(signal_candidates) > 1:
            failures.append("AMBIGUOUS_SIGNAL_STATE")
        if not market_candidates:
            failures.append("MISSING_MARKET_STATE")
        elif len(market_candidates) > 1:
            failures.append("AMBIGUOUS_MARKET_STATE")
        if failures:
            return self._blocked(symbol, failures)

        signal = signal_candidates[0]
        market = market_candidates[0]
        failures.extend(self._signal_failures(signal, position, now))
        failures.extend(self._market_failures(market, now))
        if failures:
            return self._blocked(symbol, failures)

        exit_reason = self.exit_policy.reason(
            signal=signal,
            position=position,
            market=market,
            now=now,
        )
        durable_key = self._exit_key(signal, position)
        existing, attempt_number = await self._latest_attempt(durable_key)
        if existing is not None:
            return await self._existing_workflow_outcome(
                existing=existing,
                signal=signal,
                position=position,
                market=market,
                symbol=symbol,
                exit_reason=exit_reason,
                durable_key=durable_key,
                attempt_number=attempt_number,
                now=now,
            )
        if exit_reason is None:
            return ExitMonitorOutcome(
                symbol=symbol,
                status=ExitMonitorStatus.MONITORING,
            )

        intent = self.exit_policy.order(
            signal=signal,
            position=position,
            market=market,
            account_id=self.account_id,
            now=now,
            idempotency_scope=durable_key,
        )
        if intent is None:
            return self._blocked(symbol, ("EXIT_POLICY_INCONSISTENT",))
        reports = await self.execution_service.submit_with_one_reprice(intent)
        return ExitMonitorOutcome(
            symbol=symbol,
            status=ExitMonitorStatus.EXIT_SUBMITTED,
            exit_reason=exit_reason,
            intent=intent,
            execution_reports=reports,
        )

    def _portfolio_failures(self, portfolio: PortfolioState, now: datetime) -> tuple[str, ...]:
        failures: list[str] = []
        if not portfolio.broker_connected:
            failures.append("PORTFOLIO_BROKER_DISCONNECTED")
        if not portfolio.reconciled:
            failures.append("PORTFOLIO_NOT_RECONCILED")
        failures.extend(
            self._timestamp_failures(
                portfolio.as_of,
                now,
                stale_code="PORTFOLIO_STATE_STALE",
                future_code="PORTFOLIO_STATE_FROM_FUTURE",
            )
        )
        return tuple(failures)

    def _signal_failures(
        self, signal: Signal, position: Position, now: datetime
    ) -> tuple[str, ...]:
        failures: list[str] = []
        if self._symbol(signal.symbol) != self._symbol(position.symbol):
            failures.append("SIGNAL_SYMBOL_MISMATCH")
        if signal.direction is not position.direction:
            failures.append("SIGNAL_DIRECTION_MISMATCH")
        if signal.decided_at > now + self.future_tolerance:
            failures.append("SIGNAL_STATE_FROM_FUTURE")
        return tuple(failures)

    def _market_failures(self, market: MarketSnapshot, now: datetime) -> tuple[str, ...]:
        failures: list[str] = []
        if not market.data_fresh:
            failures.append("MARKET_NOT_FRESH")
        if not market.market_data_live:
            failures.append("MARKET_DATA_NOT_LIVE")
        if market.halted:
            failures.append("MARKET_HALTED")
        failures.extend(
            self._timestamp_failures(
                min(market.as_of, market.quote.timestamp),
                now,
                stale_code="MARKET_STATE_STALE",
                future_code="MARKET_STATE_FROM_FUTURE",
            )
        )
        return tuple(failures)

    def _timestamp_failures(
        self,
        value: datetime,
        now: datetime,
        *,
        stale_code: str,
        future_code: str,
    ) -> tuple[str, ...]:
        self._require_aware(value)
        age = now - value
        failures: list[str] = []
        if age > self.max_state_age:
            failures.append(stale_code)
        if age < -self.future_tolerance:
            failures.append(future_code)
        return tuple(failures)

    def _exit_key(self, signal: Signal, position: Position) -> str:
        return f"{signal.signal_id}:exit:{self.account_id}:{self._symbol(position.symbol)}"

    def _existing_intent_failures(
        self,
        intent: OrderIntent,
        signal: Signal,
        position: Position,
    ) -> tuple[str, ...]:
        expected_side = (
            OrderSide.SELL if position.direction is Direction.LONG else OrderSide.BUY_TO_COVER
        )
        failures: list[str] = []
        if intent.account_id != self.account_id:
            failures.append("EXIT_INTENT_ACCOUNT_CONFLICT")
        if intent.signal_id != signal.signal_id:
            failures.append("EXIT_INTENT_SIGNAL_CONFLICT")
        if self._symbol(intent.symbol) != self._symbol(position.symbol):
            failures.append("EXIT_INTENT_SYMBOL_CONFLICT")
        if intent.side is not expected_side:
            failures.append("EXIT_INTENT_SIDE_CONFLICT")
        return tuple(failures)

    async def _existing_workflow_outcome(
        self,
        *,
        existing: OrderIntent,
        signal: Signal,
        position: Position,
        market: MarketSnapshot,
        symbol: str,
        exit_reason: str | None,
        durable_key: str,
        attempt_number: int,
        now: datetime,
    ) -> ExitMonitorOutcome:
        identity_failures = self._existing_intent_failures(existing, signal, position)
        if identity_failures:
            return self._blocked(symbol, identity_failures, intent=existing)

        latest = await self._latest_report(existing)
        if latest is None:
            return self._blocked(
                symbol,
                ("EXIT_OUTCOME_UNKNOWN",),
                intent=existing,
            )
        if latest.status in {
            ExecutionStatus.PENDING,
            ExecutionStatus.SUBMITTED,
            ExecutionStatus.PARTIALLY_FILLED,
        }:
            expected_remaining = existing.quantity - latest.filled_quantity
            if expected_remaining != position.quantity:
                return self._blocked(
                    symbol,
                    ("EXIT_REMAINDER_POSITION_MISMATCH",),
                    intent=existing,
                )
            reports = await self.execution_service.resume_persisted_workflow(
                existing,
                latest,
            )
            active = await self.ledger.get_order_intent_by_key(f"{existing.idempotency_key}:r1")
            return ExitMonitorOutcome(
                symbol=symbol,
                status=(
                    ExitMonitorStatus.EXIT_SUBMITTED
                    if reports
                    and reports[-1].status
                    in {
                        ExecutionStatus.FILLED,
                        ExecutionStatus.CANCELLED,
                        ExecutionStatus.REJECTED,
                    }
                    else ExitMonitorStatus.ALREADY_REQUESTED
                ),
                exit_reason=exit_reason,
                intent=active or existing,
                execution_reports=reports or (latest,),
            )
        if latest.status is ExecutionStatus.FILLED:
            return self._blocked(
                symbol,
                ("EXIT_FILLED_BUT_POSITION_REMAINS",),
                intent=existing,
            )
        if latest.status is ExecutionStatus.REJECTED:
            return self._blocked(
                symbol,
                ("EXIT_REJECTED_WITH_OPEN_POSITION",),
                intent=existing,
            )

        replacement_key = f"{existing.idempotency_key}:r1"
        replacement = await self.ledger.get_order_intent_by_key(replacement_key)
        if replacement is None:
            remaining = existing.quantity - latest.filled_quantity
            if remaining <= 0:
                return self._blocked(
                    symbol,
                    ("EXIT_CANCELLED_BUT_POSITION_REMAINS",),
                    intent=existing,
                )
            if remaining != position.quantity:
                return self._blocked(
                    symbol,
                    ("EXIT_REMAINDER_POSITION_MISMATCH",),
                    intent=existing,
                )
            reports = await self.execution_service.resume_persisted_workflow(
                existing,
                latest,
            )
            replacement = await self.ledger.get_order_intent_by_key(replacement_key)
            return ExitMonitorOutcome(
                symbol=symbol,
                status=ExitMonitorStatus.EXIT_SUBMITTED,
                exit_reason=exit_reason,
                intent=replacement or existing,
                execution_reports=reports,
            )

        replacement_failures = self._existing_intent_failures(replacement, signal, position)
        if replacement_failures:
            return self._blocked(symbol, replacement_failures, intent=replacement)
        replacement_report = await self._latest_report(replacement)
        if replacement_report is None:
            return self._blocked(
                symbol,
                ("EXIT_REPRICE_OUTCOME_UNKNOWN",),
                intent=replacement,
            )
        if replacement_report.status in {
            ExecutionStatus.PENDING,
            ExecutionStatus.SUBMITTED,
            ExecutionStatus.PARTIALLY_FILLED,
        }:
            expected_remaining = (
                existing.quantity - latest.filled_quantity - replacement_report.filled_quantity
            )
            if expected_remaining != position.quantity:
                return self._blocked(
                    symbol,
                    ("EXIT_REMAINDER_POSITION_MISMATCH",),
                    intent=replacement,
                )
            reports = await self.execution_service.resume_persisted_workflow(
                replacement,
                replacement_report,
            )
            return ExitMonitorOutcome(
                symbol=symbol,
                status=(
                    ExitMonitorStatus.EXIT_SUBMITTED
                    if reports
                    and reports[-1].status
                    in {
                        ExecutionStatus.FILLED,
                        ExecutionStatus.CANCELLED,
                        ExecutionStatus.REJECTED,
                    }
                    else ExitMonitorStatus.ALREADY_REQUESTED
                ),
                exit_reason=exit_reason,
                intent=replacement,
                execution_reports=reports or (replacement_report,),
            )
        if replacement_report.status is ExecutionStatus.FILLED:
            return self._blocked(
                symbol,
                ("EXIT_FILLED_BUT_POSITION_REMAINS",),
                intent=replacement,
            )
        if replacement_report.status is ExecutionStatus.REJECTED:
            return self._blocked(
                symbol,
                ("EXIT_REPRICE_REJECTED_WITH_OPEN_POSITION",),
                intent=replacement,
            )

        total_filled = latest.filled_quantity + replacement_report.filled_quantity
        remaining = existing.quantity - total_filled
        if remaining <= 0:
            return self._blocked(
                symbol,
                ("EXIT_TERMINAL_BUT_POSITION_REMAINS",),
                intent=replacement,
            )
        if remaining != position.quantity:
            return self._blocked(
                symbol,
                ("EXIT_REMAINDER_POSITION_MISMATCH",),
                intent=replacement,
            )
        if total_filled <= 0:
            return self._blocked(
                symbol,
                ("EXIT_REPRICE_CANCELLED_WITHOUT_FILL",),
                intent=replacement,
            )

        next_key = self._attempt_key(durable_key, attempt_number + 1)
        follow_up = self._follow_up_intent(
            previous=existing,
            position=position,
            market=market,
            now=now,
            idempotency_key=next_key,
        )
        reports = await self.execution_service.submit_with_one_reprice(follow_up)
        return ExitMonitorOutcome(
            symbol=symbol,
            status=ExitMonitorStatus.EXIT_SUBMITTED,
            exit_reason=exit_reason or "PARTIAL_EXIT_CONTINUATION",
            intent=follow_up,
            execution_reports=reports,
        )

    async def _latest_attempt(self, durable_key: str) -> tuple[OrderIntent | None, int]:
        latest: OrderIntent | None = None
        attempt_number = 1
        while attempt_number <= 1_000:
            candidate = await self.ledger.get_order_intent_by_key(
                self._attempt_key(durable_key, attempt_number)
            )
            if candidate is None:
                return latest, max(1, attempt_number - 1)
            latest = candidate
            attempt_number += 1
        raise ExecutionError("exit attempt sequence exceeds safety limit")

    @staticmethod
    def _attempt_key(durable_key: str, attempt_number: int) -> str:
        if attempt_number <= 0:
            raise ValueError("attempt_number must be positive")
        return durable_key if attempt_number == 1 else f"{durable_key}:a{attempt_number}"

    @staticmethod
    def _follow_up_intent(
        *,
        previous: OrderIntent,
        position: Position,
        market: MarketSnapshot,
        now: datetime,
        idempotency_key: str,
    ) -> OrderIntent:
        limit_price = market.quote.bid if previous.side is OrderSide.SELL else market.quote.ask
        data = previous.model_dump()
        data.update(
            {
                "order_id": f"exit-{sha256(idempotency_key.encode()).hexdigest()[:24]}",
                "idempotency_key": idempotency_key,
                "quantity": position.quantity,
                "limit_price": limit_price,
                "created_at": now,
                "replaces_order_id": None,
                "reprice_generation": 0,
            }
        )
        return OrderIntent.model_validate(data)

    async def _latest_report(self, intent: OrderIntent) -> ExecutionReport | None:
        latest = await self.ledger.get_execution_report(intent.order_id)
        if latest is not None and latest.status in {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }:
            return latest
        try:
            return await self.execution_service.reconcile_order(intent.order_id)
        except ExecutionReconciliationError:
            return None

    @staticmethod
    def _symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @staticmethod
    def _require_aware(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("monitor timestamps must be timezone-aware")

    @staticmethod
    def _blocked(
        symbol: str,
        reason_codes: Iterable[str],
        *,
        intent: OrderIntent | None = None,
    ) -> ExitMonitorOutcome:
        return ExitMonitorOutcome(
            symbol=symbol,
            status=ExitMonitorStatus.BLOCKED,
            reason_codes=tuple(dict.fromkeys(reason_codes)),
            intent=intent,
        )
