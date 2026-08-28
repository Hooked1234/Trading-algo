"""Composition root for the shadow runtime.

Everything the shadow daemon needs is assembled here, once, from configuration
and injected collaborators.  The broker it receives cannot submit, and the
account it uses is a virtual shadow account, so no wiring mistake elsewhere can
reach a real order path.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

from .broker import Broker
from .calendar import NEW_YORK, NyseSessionCalendar
from .candidates import CandidateGate
from .config import Settings
from .domain import EventSnapshot, MarketSnapshot, OrderSide, PortfolioState
from .execution import PaperExecutionService
from .monitor import ExitMonitor
from .orchestrator import EventTradingOrchestrator
from .preflight import LiveOrderPreflight
from .promotion import ResearchPromotionArtifact
from .providers.insight import (
    InsightProvider,
    KeywordInsightProvider,
    QuantOnlyInsightProvider,
)
from .reconciliation import DailySecReconciler, SQLiteSecReconciliationLedger
from .reporting import build_daily_metrics, write_daily_report
from .risk import RiskEngine
from .service import (
    FilingPoller,
    LocalTradingDaemon,
    RuntimeCycleResult,
    SQLiteSingletonLease,
)
from .session import SnapshotFactory, TradingSession
from .shadow import (
    SHADOW_ACCOUNT_ID,
    NonSubmittingShadowBroker,
    shadow_pre_submit_guard,
    shadow_repricer,
)
from .startup import PaperRecoveryCoordinator
from .storage import SQLiteOperationalStore
from .strategy import ContinuationStrategy, QuantOnlyContinuationStrategy
from .warnings import LocalCriticalWarningSink

Clock = Callable[[], datetime]
ShadowVariant = Literal["quant-only", "keyword", "ai"]
ExitMarketProvider = Callable[[str, datetime], Awaitable[MarketSnapshot | None]]
PortfolioStateProvider = Callable[[datetime], Awaitable[PortfolioState]]


@dataclass(frozen=True, slots=True)
class ShadowRuntime:
    """One fully wired, non-submitting shadow session."""

    store: SQLiteOperationalStore
    daemon: LocalTradingDaemon
    session: TradingSession
    broker: NonSubmittingShadowBroker
    orchestrator: EventTradingOrchestrator

    async def run_once(self, *, now: datetime | None = None) -> RuntimeCycleResult:
        return await self.daemon.run_cycle(now=now or datetime.now(UTC))

    async def run(self, stop: asyncio.Event) -> None:
        await self.daemon.run(stop)


@dataclass(frozen=True, slots=True)
class PaperRuntime:
    """Fully wired IBKR paper runtime with mandatory startup recovery."""

    store: SQLiteOperationalStore
    daemon: LocalTradingDaemon
    session: TradingSession
    broker: Broker
    orchestrator: EventTradingOrchestrator
    recovery: PaperRecoveryCoordinator

    async def run_once(self, *, now: datetime | None = None) -> RuntimeCycleResult:
        await self.recovery()
        return await self.daemon.run_cycle(now=now or datetime.now(UTC))

    async def run(self, stop: asyncio.Event) -> None:
        await self.daemon.run(stop)


def risk_engine_from_settings(settings: Settings) -> RiskEngine:
    return RiskEngine(
        risk_per_trade=Decimal(str(settings.risk_per_trade)),
        max_positions=settings.max_positions,
        max_symbol_notional=Decimal(str(settings.max_symbol_notional)),
        max_gross_exposure=Decimal(str(settings.max_gross_exposure)),
        max_abs_net_exposure=Decimal(str(settings.max_abs_net_exposure)),
        max_daily_loss=Decimal(str(settings.max_daily_loss)),
        max_drawdown=Decimal(str(settings.max_drawdown)),
        strategy_nav=Decimal(str(settings.strategy_nav)),
    )


async def _no_exit_market(symbol: str, now: datetime) -> MarketSnapshot | None:
    """Shadow mode never opens a position, so no exit market is ever needed."""

    del symbol, now
    return None


def build_shadow_runtime(
    settings: Settings,
    *,
    store: SQLiteOperationalStore,
    poller: FilingPoller,
    snapshot_factory: SnapshotFactory,
    variant: ShadowVariant = "keyword",
    insight_provider: InsightProvider | None = None,
    exit_market_provider: ExitMarketProvider | None = None,
    warning_sink: Callable[[str], Awaitable[None] | None] | None = None,
    clock: Clock | None = None,
    calendar: NyseSessionCalendar | None = None,
    sec_poll_interval: timedelta | None = None,
    session_interval: timedelta = timedelta(seconds=1),
    exit_interval: timedelta = timedelta(seconds=1),
    use_lease: bool = True,
    write_report: bool = True,
) -> ShadowRuntime:
    """Assemble the shadow daemon; broker submission is unreachable by design."""

    tick = clock or (lambda: datetime.now(UTC))
    session_calendar = calendar or NyseSessionCalendar()
    if variant == "quant-only":
        # The quant-only runtime never constructs an insight at all; the
        # provider is only present so the pipeline type checks.
        provider: InsightProvider = insight_provider or QuantOnlyInsightProvider()
        strategy: ContinuationStrategy = QuantOnlyContinuationStrategy(session_calendar)
    elif variant == "ai":
        if insight_provider is None:
            raise ValueError("the AI shadow variant requires an explicit insight provider")
        provider = insight_provider
        strategy = ContinuationStrategy(session_calendar)
    else:
        # The keyword baseline keeps its own conservative input budget; the
        # configured Hermes budget belongs to the Hermes adapter alone.
        provider = insight_provider or KeywordInsightProvider()
        strategy = ContinuationStrategy(session_calendar)
    broker = NonSubmittingShadowBroker(
        strategy_nav=Decimal(str(settings.strategy_nav)),
        clock=tick,
    )
    execution = PaperExecutionService(
        broker=broker,
        ledger=store,
        repricer=shadow_repricer,
        pre_submit_guard=shadow_pre_submit_guard,
    )
    orchestrator = EventTradingOrchestrator(
        insight_provider=provider,
        strategy=strategy,
        risk_engine=risk_engine_from_settings(settings),
        ledger=store,
        broker=broker,
        execution_service=execution,
        account_id=SHADOW_ACCOUNT_ID,
        candidate_gate=CandidateGate(calendar=session_calendar),
        insight_store=store,
        ai_influences_orders=strategy.insight_influences_orders,
        execution_enabled=False,
    )
    session = TradingSession(
        store=store,
        snapshot_factory=snapshot_factory,
        portfolio_provider=lambda: _shadow_portfolio(broker, tick),
        orchestrator=orchestrator,
        calendar=session_calendar,
    )
    exit_monitor = ExitMonitor(
        account_id=SHADOW_ACCOUNT_ID,
        ledger=store,
        execution_service=execution,
    )

    async def portfolio_provider(_now: datetime) -> PortfolioState:
        return broker.portfolio_state()

    async def startup_check() -> None:
        # Shadow mode has no broker to reconcile with; the durable store must
        # merely be usable before any event is processed.
        await store.count_outbox()

    async def record_critical(message: str) -> None:
        await store.record_critical_event(message, occurred_at=tick())

    async def record_heartbeat(now: datetime) -> None:
        await store.record_heartbeat(now)

    async def write_session_report() -> None:
        """Write the hashed session report from durable state when the run ends."""

        if not write_report:
            return
        now = tick()
        session_date = now.astimezone(NEW_YORK).date()
        try:
            metrics = await build_daily_metrics(
                store,
                session_date=session_date,
                generated_at=now,
                calendar=session_calendar,
            )
            write_daily_report(metrics, settings.report_dir)
        except FileExistsError:
            # The session report is immutable; a second shutdown keeps the first.
            return
        except Exception as exc:
            await store.record_critical_event(
                "SESSION_REPORT_FAILED",
                detail=f"{exc.__class__.__name__}: {exc}",
                occurred_at=now,
            )

    return ShadowRuntime(
        store=store,
        daemon=LocalTradingDaemon(
            poller=poller,
            store=store,
            trading_session=session,
            exit_monitor=exit_monitor,
            portfolio_provider=portfolio_provider,
            exit_market_provider=exit_market_provider or _no_exit_market,
            startup_check=startup_check,
            shutdown=write_session_report,
            warning_sink=warning_sink,
            critical_event_sink=record_critical,
            heartbeat_sink=record_heartbeat,
            lease=SQLiteSingletonLease(store) if use_lease else None,
            clock=tick,
            calendar=session_calendar,
            sec_poll_interval=sec_poll_interval
            or timedelta(seconds=settings.sec_poll_seconds),
            session_interval=session_interval,
            exit_interval=exit_interval,
        ),
        session=session,
        broker=broker,
        orchestrator=orchestrator,
    )


def build_paper_runtime(
    settings: Settings,
    *,
    store: SQLiteOperationalStore,
    poller: FilingPoller,
    snapshot_factory: SnapshotFactory,
    broker: Broker,
    market_provider: ExitMarketProvider,
    sec_reconciliation: SQLiteSecReconciliationLedger,
    promotion_artifact: ResearchPromotionArtifact,
    runtime_experiment_manifest_sha256: str,
    runtime_dataset_manifest_sha256: str,
    runtime_code_revision_sha256: str,
    variant: ShadowVariant = "quant-only",
    insight_provider: InsightProvider | None = None,
    daily_reconciler: DailySecReconciler | None = None,
    warning_sink: Callable[[str], Awaitable[None] | None] | None = None,
    clock: Clock | None = None,
    calendar: NyseSessionCalendar | None = None,
    sec_poll_interval: timedelta | None = None,
    session_interval: timedelta = timedelta(seconds=1),
    exit_interval: timedelta = timedelta(seconds=1),
    use_lease: bool = True,
) -> PaperRuntime:
    """Assemble the only order-capable runtime; every safety dependency is required."""

    if settings.placeholder_credentials:
        raise ValueError("paper runtime requires a configured IBKR DU account")
    if not settings.paper_account_id.upper().startswith("DU"):
        raise ValueError("paper runtime requires an IBKR DU account")
    if broker.account_id != settings.paper_account_id:  # type: ignore[attr-defined]
        raise ValueError("paper runtime broker account differs from settings")
    tick = clock or (lambda: datetime.now(UTC))
    session_calendar = calendar or NyseSessionCalendar()
    if variant == "quant-only":
        provider: InsightProvider = insight_provider or QuantOnlyInsightProvider()
        strategy: ContinuationStrategy = QuantOnlyContinuationStrategy(session_calendar)
    elif variant == "ai":
        if insight_provider is None:
            raise ValueError("the AI paper variant requires an explicit insight provider")
        provider = insight_provider
        strategy = ContinuationStrategy(session_calendar)
    elif variant == "keyword":
        provider = insight_provider or KeywordInsightProvider()
        strategy = ContinuationStrategy(session_calendar)
    else:
        raise ValueError("paper variant must be quant-only, keyword or ai")

    async def reconciled_portfolio(_now: datetime) -> PortfolioState:
        result = await asyncio.to_thread(broker.reconcile)
        for fill in result.fills:
            await store.save_execution_fill(fill)
        for report in result.executions:
            await store.save_execution_report(report)
        return result.portfolio

    async def repricer(symbol: str, side: OrderSide) -> Decimal:
        market = await market_provider(symbol, tick())
        if market is None:
            raise RuntimeError("live market snapshot is unavailable for reprice")
        return (
            market.quote.ask
            if side in {OrderSide.BUY, OrderSide.BUY_TO_COVER}
            else market.quote.bid
        )

    preflight = LiveOrderPreflight(
        broker=broker,
        ledger=store,
        market_provider=market_provider,
        portfolio_provider=reconciled_portfolio,
        risk_engine=risk_engine_from_settings(settings),
        sec_reconciliation=sec_reconciliation,
        clock=tick,
        strategy=strategy,
        calendar=session_calendar,
    )
    execution = PaperExecutionService(
        broker=broker,
        ledger=store,
        repricer=repricer,
        pre_submit_guard=preflight,
        promotion_artifact_sha256=promotion_artifact.artifact_sha256,
    )

    async def refresh_snapshot(
        snapshot: EventSnapshot,
        at: datetime,
    ) -> EventSnapshot | None:
        return await snapshot_factory.build(snapshot.filing, as_of=at)

    orchestrator = EventTradingOrchestrator(
        insight_provider=provider,
        strategy=strategy,
        risk_engine=risk_engine_from_settings(settings),
        ledger=store,
        broker=broker,
        execution_service=execution,
        account_id=settings.paper_account_id,
        candidate_gate=CandidateGate(calendar=session_calendar),
        insight_store=store,
        promotion_artifact=promotion_artifact,
        runtime_experiment_manifest_sha256=runtime_experiment_manifest_sha256,
        runtime_dataset_manifest_sha256=runtime_dataset_manifest_sha256,
        runtime_code_revision_sha256=runtime_code_revision_sha256,
        ai_influences_orders=strategy.insight_influences_orders,
        snapshot_refresher=refresh_snapshot,
        portfolio_refresher=reconciled_portfolio,
        decision_clock=tick,
        execution_enabled=True,
    )

    async def session_portfolio() -> PortfolioState:
        return await reconciled_portfolio(tick())

    session = TradingSession(
        store=store,
        snapshot_factory=snapshot_factory,
        portfolio_provider=session_portfolio,
        orchestrator=orchestrator,
        calendar=session_calendar,
    )
    exit_monitor = ExitMonitor(
        account_id=settings.paper_account_id,
        ledger=store,
        execution_service=execution,
    )
    recovery = PaperRecoveryCoordinator(
        broker=broker,  # type: ignore[arg-type]
        store=store,
        execution_service=execution,
        clock=tick,
    )

    async def record_critical(message: str) -> None:
        await store.record_critical_event(message, occurred_at=tick())

    async def record_heartbeat(now: datetime) -> None:
        await store.record_heartbeat(now)

    return PaperRuntime(
        store=store,
        daemon=LocalTradingDaemon(
            poller=poller,
            store=store,
            trading_session=session,
            exit_monitor=exit_monitor,
            portfolio_provider=reconciled_portfolio,
            exit_market_provider=market_provider,
            startup_check=recovery,
            daily_reconciler=daily_reconciler,
            warning_sink=warning_sink,
            critical_event_sink=record_critical,
            heartbeat_sink=record_heartbeat,
            lease=SQLiteSingletonLease(store) if use_lease else None,
            clock=tick,
            calendar=session_calendar,
            sec_poll_interval=sec_poll_interval
            or timedelta(seconds=settings.sec_poll_seconds),
            session_interval=session_interval,
            exit_interval=exit_interval,
        ),
        session=session,
        broker=broker,
        orchestrator=orchestrator,
        recovery=recovery,
    )


async def _shadow_portfolio(
    broker: NonSubmittingShadowBroker, clock: Clock
) -> PortfolioState:
    del clock
    return broker.portfolio_state()


def default_warning_sink(settings: Settings) -> LocalCriticalWarningSink:
    return LocalCriticalWarningSink(settings.report_dir / "critical-warnings.jsonl")


__all__ = [
    "PaperRuntime",
    "ShadowRuntime",
    "ShadowVariant",
    "build_paper_runtime",
    "build_shadow_runtime",
    "default_warning_sink",
    "risk_engine_from_settings",
]
