"""Supervised async runtime for polling, entries, exits, and reconciliation.

The SEC poll, the entry/insight pipeline and the exit supervisor run as three
separate supervised tasks.  A thirty-second model timeout in the entry task
therefore cannot delay the one-second exit tick.  A critical failure in any task
blocks *new entries* only: exit supervision and warnings keep running.

A SQLite singleton lease makes a second daemon on the same state refuse to start.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol

from .calendar import NEW_YORK, NyseSessionCalendar
from .domain import (
    ExecutionReport,
    MarketSnapshot,
    OrderIntent,
    PortfolioState,
    Signal,
)
from .monitor import ExitMonitor, ExitMonitorCycle
from .orchestrator import PipelineOutcome
from .position_tracking import resolve_position_signals
from .providers.sec import SecCursor, SecPollResult
from .reconciliation import DailySecReconciler, SecReconciliationResult
from .session import TradingSession

DAEMON_LEASE_NAME = "trading.daemon"
_ENTRY_BLOCKING_PREFIXES = ("EXIT_MONITOR", "EXIT_MARKET", "POSITION_SIGNAL", "SEC_DAILY")


class DaemonAlreadyRunning(RuntimeError):
    """Another daemon already holds the singleton lease for this state."""


class FilingPoller(Protocol):
    async def poll(self, cursor: SecCursor | None = None) -> SecPollResult: ...


class RuntimeStore(Protocol):
    async def get_cursor(self, provider: str) -> str | None: ...

    async def save_poll(
        self,
        events: tuple,
        *,
        provider: str,
        cursor: str,
        outbox_topic: str = "filing.ingested",
    ) -> int: ...

    async def list_signals_since(self, since: datetime) -> tuple[Signal, ...]: ...

    async def list_order_intents_since(self, since: datetime) -> tuple[OrderIntent, ...]: ...

    async def list_execution_reports_since(
        self, since: datetime
    ) -> tuple[ExecutionReport, ...]: ...


class LeaseStore(Protocol):
    async def acquire_lease(
        self,
        name: str,
        *,
        holder: str,
        ttl: timedelta,
        now: datetime | None = None,
    ) -> bool: ...

    async def release_lease(self, name: str, *, holder: str) -> bool: ...


class RuntimeLease(Protocol):
    """Single-writer guard so two daemons never share one operational state."""

    @property
    def holder(self) -> str: ...

    async def acquire(self) -> bool: ...

    async def renew(self) -> bool: ...

    async def release(self) -> None: ...


class SQLiteSingletonLease:
    """Named, expiring lease backed by the operational SQLite store."""

    def __init__(
        self,
        store: LeaseStore,
        *,
        name: str = DAEMON_LEASE_NAME,
        holder: str | None = None,
        ttl: timedelta = timedelta(seconds=30),
    ) -> None:
        if ttl <= timedelta(0):
            raise ValueError("lease ttl must be positive")
        self._store = store
        self._name = name
        self._holder = holder or f"daemon-{uuid.uuid4().hex}"
        self._ttl = ttl

    @property
    def holder(self) -> str:
        return self._holder

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    async def acquire(self) -> bool:
        return await self._store.acquire_lease(self._name, holder=self._holder, ttl=self._ttl)

    async def renew(self) -> bool:
        return await self.acquire()

    async def release(self) -> None:
        await self._store.release_lease(self._name, holder=self._holder)


PortfolioProvider = Callable[[datetime], Awaitable[PortfolioState]]
ExitMarketProvider = Callable[[str, datetime], Awaitable[MarketSnapshot | None]]
LifecycleHook = Callable[[], Awaitable[None]]
WarningSink = Callable[[str], Awaitable[None] | None]
CriticalEventSink = Callable[[str], Awaitable[None] | None]
HeartbeatSink = Callable[[datetime], Awaitable[None] | None]
Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class RuntimeCycleResult:
    checked_at: datetime
    ingested_filings: int = 0
    entry_outcomes: tuple[PipelineOutcome, ...] = ()
    exit_cycle: ExitMonitorCycle | None = None
    sec_reconciliation: SecReconciliationResult | None = None
    critical_errors: tuple[str, ...] = ()


@dataclass(slots=True)
class _EntryGuard:
    """Track which supervised source currently forbids new entries."""

    reasons: dict[str, str] = field(default_factory=dict)

    def fail(self, source: str, reason: str) -> None:
        self.reasons[source] = reason

    def clear(self, source: str) -> None:
        self.reasons.pop(source, None)

    @property
    def blocked(self) -> bool:
        return bool(self.reasons)

    def summary(self) -> tuple[str, ...]:
        return tuple(sorted(self.reasons.values()))


class LocalTradingDaemon:
    """Run the local jobs as supervised tasks with auditable state transitions."""

    def __init__(
        self,
        *,
        poller: FilingPoller,
        store: RuntimeStore,
        trading_session: TradingSession,
        exit_monitor: ExitMonitor,
        portfolio_provider: PortfolioProvider,
        exit_market_provider: ExitMarketProvider,
        startup_check: LifecycleHook,
        daily_reconciler: DailySecReconciler | None = None,
        shutdown: LifecycleHook | None = None,
        warning_sink: WarningSink | None = None,
        critical_event_sink: CriticalEventSink | None = None,
        heartbeat_sink: HeartbeatSink | None = None,
        lease: RuntimeLease | None = None,
        clock: Clock | None = None,
        calendar: NyseSessionCalendar | None = None,
        sec_poll_interval: timedelta = timedelta(seconds=10),
        session_interval: timedelta = timedelta(seconds=1),
        exit_interval: timedelta = timedelta(seconds=1),
        entry_batch_size: int = 1,
    ) -> None:
        for name, interval in (
            ("SEC poll", sec_poll_interval),
            ("session", session_interval),
            ("exit", exit_interval),
        ):
            if interval <= timedelta(0):
                raise ValueError(f"{name} interval must be positive")
        if entry_batch_size <= 0:
            raise ValueError("entry batch size must be positive")
        self.poller = poller
        self.store = store
        self.trading_session = trading_session
        self.exit_monitor = exit_monitor
        self.portfolio_provider = portfolio_provider
        self.exit_market_provider = exit_market_provider
        self.startup_check = startup_check
        self.daily_reconciler = daily_reconciler
        self.shutdown = shutdown
        self.warning_sink = warning_sink
        self.critical_event_sink = critical_event_sink
        self.heartbeat_sink = heartbeat_sink
        self.lease = lease
        self.clock = clock or (lambda: datetime.now(UTC))
        self.calendar = calendar or NyseSessionCalendar()
        self.sec_poll_interval = sec_poll_interval
        self.session_interval = session_interval
        self.exit_interval = exit_interval
        self.entry_batch_size = entry_batch_size
        self._guard = _EntryGuard()

    @property
    def entries_blocked(self) -> bool:
        return self._guard.blocked

    # ------------------------------------------------------------ supervised --

    async def run(self, stop: asyncio.Event) -> None:
        """Run supervised loops until ``stop``; startup failure aborts everything."""

        if self.lease is not None and not await self.lease.acquire():
            raise DaemonAlreadyRunning(
                "another daemon already holds the operational singleton lease"
            )
        try:
            await self.startup_check()
            tasks = [
                asyncio.create_task(
                    self._supervise("exit", self.exit_interval, self._exit_tick, stop),
                    name="event-trader-exit",
                ),
                asyncio.create_task(
                    self._supervise("sec", self.sec_poll_interval, self._sec_tick, stop),
                    name="event-trader-sec",
                ),
                asyncio.create_task(
                    self._supervise("entry", self.session_interval, self._entry_tick, stop),
                    name="event-trader-entry",
                ),
                asyncio.create_task(
                    self._supervise(
                        "reconciliation",
                        timedelta(minutes=15),
                        self._reconciliation_tick,
                        stop,
                    ),
                    name="event-trader-reconciliation",
                ),
            ]
            if self.lease is not None:
                tasks.append(
                    asyncio.create_task(
                        self._supervise("lease", timedelta(seconds=5), self._lease_tick, stop),
                        name="event-trader-lease",
                    )
                )
            try:
                await stop.wait()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            if self.shutdown is not None:
                await self.shutdown()
            if self.lease is not None:
                with contextlib.suppress(Exception):
                    await self.lease.release()

    async def _supervise(
        self,
        source: str,
        interval: timedelta,
        tick: Callable[[datetime], Awaitable[tuple[str, ...]]],
        stop: asyncio.Event,
    ) -> None:
        """Repeat one job forever; a failed iteration is reported, never fatal."""

        seconds = interval.total_seconds()
        while not stop.is_set():
            started = self._now()
            try:
                errors = await tick(started)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors = (f"{source.upper()}_TASK_ERROR:{exc.__class__.__name__}",)
            if errors:
                self._guard.fail(source, errors[0])
                for error in dict.fromkeys(errors):
                    await self._report(error)
            else:
                self._guard.clear(source)
            elapsed = (self._now() - started).total_seconds()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=max(0.0, seconds - elapsed))

    async def _exit_tick(self, now: datetime) -> tuple[str, ...]:
        # The exit tick is the runtime heartbeat: it is the fastest loop and the
        # one that must never stop while a position could be open.
        await self._beat(now)
        _cycle, errors = await self._run_exits(now)
        return errors

    async def _sec_tick(self, now: datetime) -> tuple[str, ...]:
        del now
        _count, errors = await self._poll_sec()
        return errors

    async def _entry_tick(self, now: datetime) -> tuple[str, ...]:
        if self._guard.blocked:
            await self._report("ENTRIES_BLOCKED_BY_RUNTIME_GUARD")
            return ()
        _outcomes, errors = await self._run_entries(now)
        return errors

    async def _reconciliation_tick(self, now: datetime) -> tuple[str, ...]:
        target = self._daily_target(now)
        if target is None:
            return ()
        _result, errors = await self._reconcile_daily(target)
        return errors

    async def _lease_tick(self, now: datetime) -> tuple[str, ...]:
        del now
        assert self.lease is not None
        if await self.lease.renew():
            return ()
        return ("DAEMON_LEASE_LOST",)

    # ----------------------------------------------------------------- jobs --

    async def run_cycle(
        self,
        *,
        now: datetime,
        poll_sec: bool = True,
        process_entries: bool = True,
        monitor_exits: bool = True,
        reconciliation_date: date | None = None,
    ) -> RuntimeCycleResult:
        """Run one bounded operator cycle; each failed job is reported and isolated."""

        _require_aware(now)
        critical: list[str] = []
        ingested = 0
        entries: tuple[PipelineOutcome, ...] = ()
        exit_cycle: ExitMonitorCycle | None = None
        sec_result: SecReconciliationResult | None = None

        if reconciliation_date is not None and self.daily_reconciler is not None:
            sec_result, errors = await self._reconcile_daily(reconciliation_date)
            critical.extend(errors)

        if monitor_exits:
            await self._beat(now)
            exit_cycle, errors = await self._run_exits(now)
            critical.extend(errors)

        if poll_sec:
            ingested, errors = await self._poll_sec()
            critical.extend(errors)

        entries_blocked = any(error.startswith(_ENTRY_BLOCKING_PREFIXES) for error in critical)
        if process_entries and not entries_blocked:
            entries, errors = await self._run_entries(now)
            critical.extend(errors)
        elif process_entries:
            critical.append("ENTRIES_BLOCKED_BY_RUNTIME_GUARD")

        for error in tuple(dict.fromkeys(critical)):
            await self._report(error)
        return RuntimeCycleResult(
            checked_at=now,
            ingested_filings=ingested,
            entry_outcomes=entries,
            exit_cycle=exit_cycle,
            sec_reconciliation=sec_result,
            critical_errors=tuple(dict.fromkeys(critical)),
        )

    async def _reconcile_daily(
        self, target: date
    ) -> tuple[SecReconciliationResult | None, tuple[str, ...]]:
        if self.daily_reconciler is None:
            return None, ()
        try:
            result = await self.daily_reconciler.run(
                session_date=target,
                reconciled_at=self._now(),
            )
        except Exception as exc:
            return None, (f"SEC_DAILY_RECONCILIATION_ERROR:{exc.__class__.__name__}",)
        if not result.complete:
            return result, ("SEC_DAILY_RECONCILIATION_INCOMPLETE",)
        return result, ()

    async def _run_exits(self, now: datetime) -> tuple[ExitMonitorCycle | None, tuple[str, ...]]:
        try:
            cycle, resolution_issues = await self._monitor_positions(now)
        except Exception as exc:
            return None, (f"EXIT_MONITOR_ERROR:{exc.__class__.__name__}",)
        errors = list(resolution_issues)
        for blocked in cycle.blocked:
            errors.extend(
                f"EXIT_MONITOR_BLOCKED:{blocked.symbol}:{reason}" for reason in blocked.reason_codes
            )
        return cycle, tuple(errors)

    async def _poll_sec(self) -> tuple[int, tuple[str, ...]]:
        try:
            raw_cursor = await self.store.get_cursor("sec.latest")
            result = await self.poller.poll(SecCursor.from_json(raw_cursor))
            ingested = await self.store.save_poll(
                result.events,
                provider="sec.latest",
                cursor=result.cursor.to_json(),
            )
        except Exception as exc:
            return 0, (f"SEC_POLL_ERROR:{exc.__class__.__name__}",)
        return ingested, ()

    async def _run_entries(
        self, now: datetime
    ) -> tuple[tuple[PipelineOutcome, ...], tuple[str, ...]]:
        try:
            outcomes = await self._process_ready(now)
        except Exception as exc:
            return (), (f"ENTRY_SESSION_ERROR:{exc.__class__.__name__}",)
        return outcomes, ()

    async def _process_ready(self, now: datetime) -> tuple[PipelineOutcome, ...]:
        # One event per worker pass keeps a slow model call from holding a lease
        # over a queue of unrelated filings.
        return await self.trading_session.process_ready(now=now, limit=self.entry_batch_size)

    async def _monitor_positions(self, now: datetime) -> tuple[ExitMonitorCycle, tuple[str, ...]]:
        del now
        portfolio_requested_at = self._now()
        portfolio = await self.portfolio_provider(portfolio_requested_at)
        market_requested_at = self._now()
        session_start = datetime.combine(
            market_requested_at.astimezone(NEW_YORK).date(), time.min, tzinfo=NEW_YORK
        ).astimezone(UTC)
        signals, intents, reports = await asyncio.gather(
            self.store.list_signals_since(session_start),
            self.store.list_order_intents_since(session_start),
            self.store.list_execution_reports_since(session_start),
        )
        resolution = resolve_position_signals(
            portfolio=portfolio,
            signals=signals,
            intents=intents,
            reports=reports,
        )
        market_results = await asyncio.gather(
            *(
                self.exit_market_provider(position.symbol, market_requested_at)
                for position in portfolio.positions
            ),
            return_exceptions=True,
        )
        markets: list[MarketSnapshot] = []
        issues = list(resolution.issues)
        for position, result in zip(portfolio.positions, market_results, strict=True):
            if isinstance(result, BaseException):
                issues.append(f"EXIT_MARKET_ERROR:{position.symbol}:{result.__class__.__name__}")
            elif result is None:
                issues.append(f"EXIT_MARKET_UNAVAILABLE:{position.symbol}")
            else:
                markets.append(result)
        evaluation_at = self._now()
        cycle = await self.exit_monitor.run_cycle(
            portfolio=portfolio,
            signals=resolution.signals,
            markets=tuple(markets),
            now=evaluation_at,
        )
        return cycle, tuple(issues)

    def _daily_target(self, now: datetime) -> date | None:
        if self.daily_reconciler is None:
            return None
        local = now.astimezone(NEW_YORK)
        if self.calendar.is_session(local.date()) and local.time() >= time(18, 0):
            return local.date()
        return None

    async def _beat(self, now: datetime) -> None:
        if self.heartbeat_sink is None:
            return
        result = self.heartbeat_sink(now)
        if inspect.isawaitable(result):
            await result

    async def _report(self, message: str) -> None:
        await _invoke(self.warning_sink, message)
        await _invoke(self.critical_event_sink, message)

    def _now(self) -> datetime:
        value = self.clock()
        _require_aware(value)
        return value.astimezone(UTC)


async def _invoke(sink: Callable[[str], Awaitable[None] | None] | None, message: str) -> None:
    if sink is None:
        return
    result = sink(message)
    if inspect.isawaitable(result):
        await result


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("daemon timestamps must be timezone-aware")


__all__ = [
    "DAEMON_LEASE_NAME",
    "DaemonAlreadyRunning",
    "LocalTradingDaemon",
    "RuntimeCycleResult",
    "RuntimeLease",
    "SQLiteSingletonLease",
]
