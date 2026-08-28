"""Durable strategy equity ledger and broker/strategy portfolio assembly."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import ClassVar, Protocol

from pydantic import Field, model_validator

from .broker import Broker
from .calendar import NEW_YORK
from .domain import (
    Direction,
    ExecutionReport,
    ExecutionStatus,
    FrozenModel,
    OrderIntent,
    OrderSide,
    PortfolioState,
)
from .risk import pending_entry_exposures


class StrategyRiskSnapshot(FrozenModel):
    strategy_id: str = Field(min_length=1)
    session_date: date
    starting_nav: Decimal = Field(gt=0)
    cumulative_realized_pnl: Decimal = Decimal("0")
    realized_pnl_today: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    equity: Decimal = Field(gt=0)
    peak_equity: Decimal = Field(gt=0)
    as_of: datetime

    @model_validator(mode="after")
    def validate_equity(self) -> StrategyRiskSnapshot:
        expected = self.starting_nav + self.cumulative_realized_pnl + self.unrealized_pnl
        if self.equity != expected:
            raise ValueError("strategy equity does not reconcile to its P&L ledger")
        if self.peak_equity < self.equity:
            raise ValueError("strategy peak equity cannot be below equity")
        return self


class SQLiteStrategyLedger:
    """Idempotent closed-trade P&L plus current unrealized mark for one strategy."""

    def __init__(
        self,
        path: str | Path,
        *,
        strategy_id: str,
        starting_nav: Decimal = Decimal("100000"),
    ) -> None:
        if not strategy_id.strip():
            raise ValueError("strategy_id must not be empty")
        if not Decimal("0") < starting_nav <= Decimal("100000"):
            raise ValueError("starting NAV must be in (0, 100000]")
        target = Path(path)
        target.resolve().parent.mkdir(parents=True, exist_ok=True)
        self.strategy_id = strategy_id
        self.starting_nav = starting_nav
        self._connection = sqlite3.connect(str(target), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS strategy_risk_state (
                strategy_id TEXT PRIMARY KEY,
                session_date TEXT NOT NULL,
                starting_nav TEXT NOT NULL,
                cumulative_realized_pnl TEXT NOT NULL,
                realized_pnl_today TEXT NOT NULL,
                unrealized_pnl TEXT NOT NULL,
                equity TEXT NOT NULL,
                peak_equity TEXT NOT NULL,
                as_of_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS strategy_closed_trades (
                strategy_id TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                net_pnl TEXT NOT NULL,
                closed_at_utc TEXT NOT NULL,
                PRIMARY KEY (strategy_id, trade_id)
            );
            """
        )
        self._connection.commit()
        self._lock = threading.RLock()

    def initialize(self, *, at: datetime) -> StrategyRiskSnapshot:
        stamp = _utc(at)
        with self._lock, self._connection:
            existing = self._latest_locked()
            if existing is not None:
                if existing.starting_nav != self.starting_nav:
                    raise ValueError("persisted strategy starting NAV differs from configuration")
                return existing
            snapshot = StrategyRiskSnapshot(
                strategy_id=self.strategy_id,
                session_date=_session_date(stamp),
                starting_nav=self.starting_nav,
                equity=self.starting_nav,
                peak_equity=self.starting_nav,
                as_of=stamp,
            )
            self._write_locked(snapshot)
            return snapshot

    def record_closed_trade(
        self,
        *,
        trade_id: str,
        net_pnl: Decimal,
        closed_at: datetime,
    ) -> StrategyRiskSnapshot:
        if not trade_id.strip():
            raise ValueError("trade_id must not be empty")
        stamp = _utc(closed_at)
        with self._lock, self._connection:
            state = self._required_state_locked(stamp)
            inserted = self._connection.execute(
                """
                INSERT OR IGNORE INTO strategy_closed_trades(
                    strategy_id, trade_id, net_pnl, closed_at_utc
                ) VALUES (?, ?, ?, ?)
                """,
                (self.strategy_id, trade_id, str(net_pnl), stamp.isoformat()),
            ).rowcount
            if not inserted:
                row = self._connection.execute(
                    """
                    SELECT net_pnl, closed_at_utc FROM strategy_closed_trades
                    WHERE strategy_id = ? AND trade_id = ?
                    """,
                    (self.strategy_id, trade_id),
                ).fetchone()
                if (
                    row is None
                    or Decimal(row["net_pnl"]) != net_pnl
                    or datetime.fromisoformat(row["closed_at_utc"]) != stamp
                ):
                    raise ValueError("trade id was reused with different P&L")
                return state
            if stamp > state.as_of:
                state = _roll_session(state, stamp)
            cumulative = state.cumulative_realized_pnl + net_pnl
            realized_today = state.realized_pnl_today + (
                net_pnl if _session_date(stamp) == state.session_date else Decimal("0")
            )
            equity = state.starting_nav + cumulative + state.unrealized_pnl
            if equity <= 0:
                raise ValueError("strategy equity cannot become non-positive")
            updated = state.model_copy(
                update={
                    "cumulative_realized_pnl": cumulative,
                    "realized_pnl_today": realized_today,
                    "equity": equity,
                    "peak_equity": max(state.peak_equity, equity),
                    "as_of": max(state.as_of, stamp),
                }
            )
            self._write_locked(updated)
            return updated

    def mark_unrealized(self, *, pnl: Decimal, at: datetime) -> StrategyRiskSnapshot:
        stamp = _utc(at)
        with self._lock, self._connection:
            state = _roll_session(self._required_state_locked(stamp), stamp)
            if stamp < state.as_of:
                raise ValueError("unrealized mark cannot move backwards")
            equity = state.starting_nav + state.cumulative_realized_pnl + pnl
            if equity <= 0:
                raise ValueError("strategy equity cannot become non-positive")
            updated = state.model_copy(
                update={
                    "unrealized_pnl": pnl,
                    "equity": equity,
                    "peak_equity": max(state.peak_equity, equity),
                    "as_of": stamp,
                }
            )
            self._write_locked(updated)
            return updated

    def latest(self) -> StrategyRiskSnapshot | None:
        with self._lock:
            return self._latest_locked()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _required_state_locked(self, at: datetime) -> StrategyRiskSnapshot:
        state = self._latest_locked()
        return state if state is not None else self.initialize(at=at)

    def _latest_locked(self) -> StrategyRiskSnapshot | None:
        row = self._connection.execute(
            "SELECT * FROM strategy_risk_state WHERE strategy_id = ?",
            (self.strategy_id,),
        ).fetchone()
        if row is None:
            return None
        return StrategyRiskSnapshot(
            strategy_id=row["strategy_id"],
            session_date=date.fromisoformat(row["session_date"]),
            starting_nav=Decimal(row["starting_nav"]),
            cumulative_realized_pnl=Decimal(row["cumulative_realized_pnl"]),
            realized_pnl_today=Decimal(row["realized_pnl_today"]),
            unrealized_pnl=Decimal(row["unrealized_pnl"]),
            equity=Decimal(row["equity"]),
            peak_equity=Decimal(row["peak_equity"]),
            as_of=datetime.fromisoformat(row["as_of_utc"]),
        )

    def _write_locked(self, state: StrategyRiskSnapshot) -> None:
        self._connection.execute(
            """
            INSERT INTO strategy_risk_state(
                strategy_id, session_date, starting_nav, cumulative_realized_pnl,
                realized_pnl_today, unrealized_pnl, equity, peak_equity, as_of_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(strategy_id) DO UPDATE SET
                session_date = excluded.session_date,
                cumulative_realized_pnl = excluded.cumulative_realized_pnl,
                realized_pnl_today = excluded.realized_pnl_today,
                unrealized_pnl = excluded.unrealized_pnl,
                equity = excluded.equity,
                peak_equity = excluded.peak_equity,
                as_of_utc = excluded.as_of_utc
            """,
            (
                state.strategy_id,
                state.session_date.isoformat(),
                str(state.starting_nav),
                str(state.cumulative_realized_pnl),
                str(state.realized_pnl_today),
                str(state.unrealized_pnl),
                str(state.equity),
                str(state.peak_equity),
                state.as_of.isoformat(),
            ),
        )


class OrderExposureStore(Protocol):
    async def list_order_intents_since(self, since: datetime) -> tuple[OrderIntent, ...]: ...

    async def list_execution_reports_since(
        self, since: datetime
    ) -> tuple[ExecutionReport, ...]: ...

    async def save_execution_report(self, report: ExecutionReport) -> bool: ...


class StrategyPnLSynchronizer:
    """Project reconciled cumulative fills into the durable strategy risk ledger."""

    _TERMINAL: ClassVar[frozenset[ExecutionStatus]] = frozenset(
        {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
    )

    def __init__(self, ledger: SQLiteStrategyLedger) -> None:
        self.ledger = ledger

    def synchronize(
        self,
        *,
        intents: tuple[OrderIntent, ...],
        reports: tuple[ExecutionReport, ...],
        portfolio: PortfolioState,
        now: datetime,
    ) -> StrategyRiskSnapshot:
        stamp = _utc(now)
        reports_by_order = {report.order_id: report for report in reports}
        if len(reports_by_order) != len(reports):
            raise RuntimeError("strategy P&L reports must have unique order ids")
        paper_intents = tuple(intent for intent in intents if intent.submission_mode == "paper")
        grouped: dict[str, list[OrderIntent]] = {}
        for intent in paper_intents:
            grouped.setdefault(intent.signal_id, []).append(intent)

        terminal_realized: list[tuple[datetime, str, Decimal]] = []
        temporary_realized = Decimal("0")
        unrealized = Decimal("0")
        expected_positions: dict[tuple[str, Direction], int] = {}
        market_positions = {
            (position.symbol.upper(), position.direction): position
            for position in portfolio.positions
        }

        for signal_id, signal_intents in grouped.items():
            entries = [
                intent
                for intent in signal_intents
                if intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT}
            ]
            exits = [
                intent
                for intent in signal_intents
                if intent.side in {OrderSide.SELL, OrderSide.BUY_TO_COVER}
            ]
            entry_reports = [
                (intent, reports_by_order[intent.order_id])
                for intent in entries
                if intent.order_id in reports_by_order
                and reports_by_order[intent.order_id].filled_quantity > 0
            ]
            if not entry_reports:
                continue
            entry_quantity = sum(report.filled_quantity for _, report in entry_reports)
            entry_notional = sum(
                report.average_fill_price * report.filled_quantity
                for _, report in entry_reports
            )
            if entry_quantity <= 0 or entry_notional <= 0:
                raise RuntimeError("filled strategy entry has invalid execution economics")
            entry_average = entry_notional / entry_quantity
            entry_fees = sum((report.fees for _, report in entry_reports), Decimal("0"))
            entry_fee_per_share = entry_fees / entry_quantity
            entry_side = entries[0].side
            if any(intent.side is not entry_side for intent in entries):
                raise RuntimeError(f"strategy signal {signal_id} has conflicting entry sides")
            direction = (
                Direction.LONG if entry_side is OrderSide.BUY else Direction.SHORT
            )
            sign = Decimal("1") if direction is Direction.LONG else Decimal("-1")
            exited_quantity = 0
            for intent in exits:
                report = reports_by_order.get(intent.order_id)
                if report is None or report.filled_quantity <= 0:
                    continue
                exited_quantity += report.filled_quantity
                realized = (
                    sign
                    * (report.average_fill_price - entry_average)
                    * report.filled_quantity
                    - entry_fee_per_share * report.filled_quantity
                    - report.fees
                )
                if report.status in self._TERMINAL:
                    terminal_realized.append(
                        (report.occurred_at, intent.order_id, realized)
                    )
                else:
                    temporary_realized += realized
            if exited_quantity > entry_quantity:
                raise RuntimeError(f"strategy signal {signal_id} exits exceed entry fills")
            remaining = entry_quantity - exited_quantity
            symbol = entries[0].symbol.upper()
            if any(intent.symbol.upper() != symbol for intent in signal_intents):
                raise RuntimeError(f"strategy signal {signal_id} spans multiple symbols")
            if remaining > 0:
                expected_positions[(symbol, direction)] = (
                    expected_positions.get((symbol, direction), 0) + remaining
                )
                position = market_positions.get((symbol, direction))
                if position is None or position.quantity != expected_positions[(symbol, direction)]:
                    raise RuntimeError("strategy fills do not reconcile to broker positions")
                unrealized += (
                    sign * (position.market_price - entry_average) * remaining
                    - entry_fee_per_share * remaining
                )

        actual_positions = {
            key: position.quantity for key, position in market_positions.items()
        }
        if expected_positions != actual_positions:
            raise RuntimeError("broker positions contain unowned strategy exposure")

        self.ledger.initialize(at=stamp)
        for occurred_at, trade_id, pnl in sorted(terminal_realized):
            self.ledger.record_closed_trade(
                trade_id=trade_id,
                net_pnl=pnl,
                closed_at=occurred_at,
            )
        return self.ledger.mark_unrealized(
            pnl=unrealized + temporary_realized,
            at=stamp,
        )


class StrategyPortfolioAssembler:
    """Combine broker reconciliation, the strategy ledger, and reserved entries."""

    def __init__(
        self,
        *,
        broker: Broker,
        order_store: OrderExposureStore,
        strategy_ledger: SQLiteStrategyLedger,
        pnl_synchronizer: StrategyPnLSynchronizer | None = None,
        max_strategy_state_age: timedelta = timedelta(seconds=5),
    ) -> None:
        if max_strategy_state_age <= timedelta(0):
            raise ValueError("strategy state age must be positive")
        self.broker = broker
        self.order_store = order_store
        self.strategy_ledger = strategy_ledger
        self.pnl_synchronizer = pnl_synchronizer or StrategyPnLSynchronizer(strategy_ledger)
        self.max_strategy_state_age = max_strategy_state_age

    async def current(self, *, now: datetime) -> PortfolioState:
        stamp = _utc(now)
        reconciliation = await asyncio.to_thread(self.broker.reconcile)
        for report in reconciliation.executions:
            await self.order_store.save_execution_report(report)
        session_start = datetime.combine(
            stamp.astimezone(NEW_YORK).date(), time.min, tzinfo=NEW_YORK
        ).astimezone(UTC)
        intents, reports = await asyncio.gather(
            self.order_store.list_order_intents_since(session_start),
            self.order_store.list_execution_reports_since(session_start),
        )
        broker_portfolio = reconciliation.portfolio
        if not broker_portfolio.broker_connected or not broker_portfolio.reconciled:
            raise RuntimeError("broker portfolio is not reconciled")
        strategy = await asyncio.to_thread(
            self.pnl_synchronizer.synchronize,
            intents=intents,
            reports=reports,
            portfolio=broker_portfolio,
            now=stamp,
        )
        if strategy is None:
            raise RuntimeError("strategy risk ledger is not initialized")
        if strategy.as_of > stamp:
            raise RuntimeError("strategy risk ledger is from the future")
        if stamp - strategy.as_of > self.max_strategy_state_age:
            raise RuntimeError("strategy risk ledger is stale")
        return broker_portfolio.model_copy(
            update={
                "pending_orders": pending_entry_exposures(intents, reports),
                "strategy_equity": strategy.equity,
                "strategy_peak_equity": strategy.peak_equity,
                "strategy_realized_pnl_today": strategy.realized_pnl_today,
                "strategy_unrealized_pnl": strategy.unrealized_pnl,
            }
        )


def _roll_session(state: StrategyRiskSnapshot, at: datetime) -> StrategyRiskSnapshot:
    current_session = _session_date(at)
    if current_session < state.session_date:
        raise ValueError("strategy ledger session date cannot move backwards")
    if current_session == state.session_date:
        return state
    return state.model_copy(
        update={"session_date": current_session, "realized_pnl_today": Decimal("0")}
    )


def _session_date(value: datetime) -> date:
    return value.astimezone(NEW_YORK).date()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("strategy ledger timestamp must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "SQLiteStrategyLedger",
    "StrategyPnLSynchronizer",
    "StrategyPortfolioAssembler",
    "StrategyRiskSnapshot",
]
