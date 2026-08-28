"""One deterministic pipeline used by replay, shadow, and paper sessions.

A model is asked about a filing only after the deterministic candidate gate has
accepted it, and only when the configured strategy actually consumes insights.
After the model latency the gate runs again on a fresh snapshot, so a stale or
reversed market cannot ride in on an answer that was true a minute ago.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Literal, Protocol

from pydantic import Field

from .analysis import AnalysisKey
from .broker import Broker
from .candidates import CandidateDecision, CandidateGate
from .domain import (
    Direction,
    EventSnapshot,
    ExecutionReport,
    FrozenModel,
    NewsInsight,
    OrderIntent,
    OrderSide,
    PortfolioState,
    RiskDecision,
    Signal,
)
from .execution import PaperExecutionService
from .promotion import ResearchPromotionArtifact
from .providers.insight import InsightProvider
from .risk import RiskEngine
from .strategy import ContinuationStrategy


class PipelineLedger(Protocol):
    async def save_signal(self, signal: Signal) -> bool: ...

    async def save_risk_decision(self, decision: RiskDecision) -> bool: ...

    async def save_order_intent(self, intent: OrderIntent) -> bool: ...

    async def get_order_intent_by_key(self, idempotency_key: str) -> OrderIntent | None: ...


class InsightReader(Protocol):
    """Read side of the durable analysis store used to avoid repeat model calls."""

    async def get_insight(self, analysis_key: str) -> NewsInsight | None: ...


SnapshotRefresher = Callable[[EventSnapshot, datetime], Awaitable[EventSnapshot | None]]
PortfolioRefresher = Callable[[datetime], Awaitable[PortfolioState]]
DecisionClock = Callable[[], datetime]


class PipelineOutcome(FrozenModel):
    stage: str
    event_id: str = ""
    strategy_version: str = ""
    reasons: tuple[str, ...] = ()
    candidate: CandidateDecision | None = None
    insight: NewsInsight | None = None
    analysis_key: AnalysisKey | None = None
    reused_analysis: bool = False
    signal: Signal | None = None
    risk_decision: RiskDecision | None = None
    order_intent: OrderIntent | None = None
    execution_reports: tuple[ExecutionReport, ...] = ()
    shadow: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)


class EventTradingOrchestrator:
    def __init__(
        self,
        *,
        insight_provider: InsightProvider,
        strategy: ContinuationStrategy,
        risk_engine: RiskEngine,
        ledger: PipelineLedger,
        broker: Broker,
        execution_service: PaperExecutionService,
        account_id: str,
        candidate_gate: CandidateGate | None = None,
        insight_store: InsightReader | None = None,
        promotion_artifact: ResearchPromotionArtifact | None = None,
        runtime_experiment_manifest_sha256: str | None = None,
        runtime_dataset_manifest_sha256: str | None = None,
        runtime_code_revision_sha256: str | None = None,
        ai_influences_orders: bool = True,
        snapshot_refresher: SnapshotRefresher | None = None,
        portfolio_refresher: PortfolioRefresher | None = None,
        decision_clock: DecisionClock | None = None,
        max_quote_age: timedelta = timedelta(seconds=5),
        execution_enabled: bool = False,
    ) -> None:
        if max_quote_age <= timedelta(0):
            raise ValueError("maximum quote age must be positive")
        strategy_uses_insight = strategy.insight_influences_orders
        if ai_influences_orders != strategy_uses_insight:
            raise ValueError(
                "AI promotion mode must match whether the selected strategy consumes insights"
            )
        promotion_ready = promotion_artifact is not None and any(
            promotion_artifact.authorizes(
                strategy_version=strategy.version,
                direction=direction,
                ai_influences_orders=strategy_uses_insight,
            )
            for direction in promotion_artifact.enabled_directions
        )
        if execution_enabled and not promotion_ready:
            raise ValueError("paper execution requires a valid research promotion artifact")
        if execution_enabled and promotion_artifact is not None:
            runtime_fingerprints = (
                runtime_experiment_manifest_sha256,
                runtime_dataset_manifest_sha256,
                runtime_code_revision_sha256,
            )
            expected_fingerprints = (
                promotion_artifact.experiment_manifest_sha256,
                promotion_artifact.dataset_manifest_sha256,
                promotion_artifact.code_revision_sha256,
            )
            if runtime_fingerprints != expected_fingerprints:
                raise ValueError("runtime manifests do not match the promotion artifact")
        if execution_enabled and not account_id.upper().startswith("DU"):
            raise ValueError("paper execution requires an IBKR DU account")
        if execution_enabled and (snapshot_refresher is None or portfolio_refresher is None):
            raise ValueError("paper execution requires fresh snapshot and portfolio providers")
        if execution_enabled and decision_clock is None:
            raise ValueError("paper execution requires a real decision clock")
        if execution_enabled and not getattr(execution_service, "has_pre_submit_guard", False):
            raise ValueError("paper execution requires a pre-submit guard")
        if execution_enabled and (
            promotion_artifact is None
            or execution_service.promotion_artifact_sha256
            != promotion_artifact.artifact_sha256
        ):
            raise ValueError(
                "paper execution service must be bound to the promotion artifact"
            )
        if execution_service.ledger is not ledger:
            raise ValueError("pipeline and execution service must share one durable ledger")
        self.insight_provider = insight_provider
        self.strategy = strategy
        self.risk_engine = risk_engine
        self.ledger = ledger
        self.broker = broker
        self.execution_service = execution_service
        self.account_id = account_id
        self.candidate_gate = candidate_gate or CandidateGate()
        self.insight_store = insight_store
        self.promotion_artifact = promotion_artifact
        self.ai_influences_orders = strategy_uses_insight
        self.snapshot_refresher = snapshot_refresher
        self.portfolio_refresher = portfolio_refresher
        self.decision_clock = decision_clock
        self.max_quote_age = max_quote_age
        self.execution_enabled = execution_enabled

    async def process(
        self,
        snapshot: EventSnapshot,
        portfolio: PortfolioState,
        *,
        now: datetime,
    ) -> PipelineOutcome:
        event_id = snapshot.filing.event_id
        precheck_key = self._entry_order_key_for(
            event_id=event_id,
            symbol=snapshot.market.symbol,
            strategy_version=self.strategy.version,
        )
        existing_precheck = await self.ledger.get_order_intent_by_key(precheck_key)
        if existing_precheck is not None:
            return self._outcome(
                snapshot,
                stage="duplicate_event",
                reasons=("DUPLICATE_EVENT_ID",),
                order_intent=existing_precheck,
            )

        # Deterministic pre-selection: no model is asked about a rejected event.
        candidate = self.candidate_gate.evaluate(snapshot, now)
        if not candidate.accepted:
            return self._outcome(
                snapshot,
                stage="filtered",
                reasons=candidate.reason_codes,
                candidate=candidate,
            )

        insight, analysis_key, reused = await self._resolve_insight(snapshot)
        decision_time = self._decision_time(now, insight)
        if decision_time is None:
            return self._outcome(
                snapshot,
                stage="filtered",
                reasons=("DECISION_CLOCK_INVALID",),
                candidate=candidate,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
            )

        active_snapshot = snapshot
        if self.snapshot_refresher is not None:
            refreshed = await self.snapshot_refresher(snapshot, decision_time)
            if refreshed is None:
                return self._outcome(
                    snapshot,
                    stage="filtered",
                    reasons=("SNAPSHOT_REFRESH_UNAVAILABLE",),
                    candidate=candidate,
                    insight=insight,
                    analysis_key=analysis_key,
                    reused_analysis=reused,
                )
            if refreshed.filing != snapshot.filing:
                return self._outcome(
                    snapshot,
                    stage="filtered",
                    reasons=("SNAPSHOT_REFRESH_EVENT_MISMATCH",),
                    candidate=candidate,
                    insight=insight,
                    analysis_key=analysis_key,
                    reused_analysis=reused,
                )
            active_snapshot = refreshed
        market_reasons = self._market_timing_reasons(active_snapshot, decision_time)
        if market_reasons:
            return self._outcome(
                snapshot,
                stage="filtered",
                reasons=market_reasons,
                candidate=candidate,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
            )

        # The model latency has passed; the pre-selection must still hold.
        confirmed = self.candidate_gate.evaluate(active_snapshot, decision_time)
        if not confirmed.accepted:
            return self._outcome(
                snapshot,
                stage="filtered",
                reasons=confirmed.reason_codes,
                candidate=confirmed,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
            )
        if insight is not None and insight.direction is not confirmed.direction:
            return self._outcome(
                snapshot,
                stage="filtered",
                reasons=("INSIGHT_DIRECTION_MISMATCH",),
                candidate=confirmed,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
            )

        rejection_reasons = self.strategy.rejection_reasons(
            active_snapshot, insight, decision_time
        )
        signal = self.strategy.evaluate(active_snapshot, insight, decision_time)
        if signal is None:
            return self._outcome(
                snapshot,
                stage="filtered",
                reasons=rejection_reasons,
                candidate=confirmed,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
            )

        order_key = self._entry_order_key(signal)
        existing_intent = await self.ledger.get_order_intent_by_key(order_key)
        if existing_intent is not None:
            return self._outcome(
                snapshot,
                stage="duplicate_event",
                reasons=("DUPLICATE_EVENT_ID",),
                candidate=confirmed,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
                signal=signal,
                order_intent=existing_intent,
            )

        await self.ledger.save_signal(signal)
        active_portfolio = (
            await self.portfolio_refresher(decision_time)
            if self.portfolio_refresher is not None
            else portfolio
        )
        decision = self.risk_engine.assess(
            signal, active_portfolio, active_snapshot.market, decision_time
        )
        await self.ledger.save_risk_decision(decision)
        if not decision.approved:
            return self._outcome(
                snapshot,
                stage="risk_rejected",
                reasons=decision.reason_codes,
                candidate=confirmed,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
                signal=signal,
                risk_decision=decision,
            )

        promotion_authorized = self._promotion_authorizes(signal, insight)
        paper_authorized = promotion_authorized and self.execution_enabled
        intent = self._entry_intent(
            signal,
            decision,
            decision_time,
            submission_mode="paper" if paper_authorized else "shadow",
        )
        if not paper_authorized:
            await self.ledger.save_order_intent(intent)
            shadow_reason = (
                self._promotion_rejection_reason(signal, insight)
                if not promotion_authorized
                else "EXECUTION_DISABLED"
            )
            return self._outcome(
                snapshot,
                stage="shadow_order",
                reasons=(shadow_reason,),
                candidate=confirmed,
                insight=insight,
                analysis_key=analysis_key,
                reused_analysis=reused,
                signal=signal,
                risk_decision=decision,
                order_intent=intent,
            )

        reports = await self.execution_service.submit_with_one_reprice(intent)
        return self._outcome(
            snapshot,
            stage="paper_submitted",
            candidate=confirmed,
            insight=insight,
            analysis_key=analysis_key,
            reused_analysis=reused,
            signal=signal,
            risk_decision=decision,
            order_intent=intent,
            execution_reports=reports,
            shadow=False,
        )

    async def _resolve_insight(
        self, snapshot: EventSnapshot
    ) -> tuple[NewsInsight | None, AnalysisKey | None, bool]:
        """Return the pinned analysis, reusing a stored one before paying again."""

        if not self.ai_influences_orders:
            # The quant-only pipeline never constructs an insight at all.
            return None, None, False
        key = AnalysisKey.for_snapshot(snapshot, self.insight_provider.analysis_identity)
        if self.insight_store is not None:
            stored = await self.insight_store.get_insight(key.key)
            if stored is not None:
                return stored, key, True
        return await self.insight_provider.analyze(snapshot), key, False

    def _decision_time(self, now: datetime, insight: NewsInsight | None) -> datetime | None:
        if self.decision_clock is not None:
            decision_time = self.decision_clock()
        else:
            latency = timedelta(milliseconds=insight.latency_ms) if insight else timedelta(0)
            decision_time = now + latency
        if (
            decision_time.tzinfo is None
            or decision_time.utcoffset() is None
            or decision_time < now
        ):
            return None
        return decision_time

    def _outcome(
        self,
        snapshot: EventSnapshot,
        *,
        stage: str,
        reasons: tuple[str, ...] = (),
        **fields: object,
    ) -> PipelineOutcome:
        return PipelineOutcome(
            stage=stage,
            event_id=snapshot.filing.event_id,
            strategy_version=self.strategy.version,
            reasons=reasons,
            **fields,  # type: ignore[arg-type]
        )

    def _promotion_authorizes(self, signal: Signal, insight: NewsInsight | None) -> bool:
        if self.promotion_artifact is None:
            return False
        if not self.promotion_artifact.authorizes(
            strategy_version=signal.strategy_version,
            direction=signal.direction,
            ai_influences_orders=self.ai_influences_orders,
        ):
            return False
        if insight is None:
            return not self.ai_influences_orders
        return self.promotion_artifact.authorizes_insight(
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )

    def _promotion_rejection_reason(self, signal: Signal, insight: NewsInsight | None) -> str:
        artifact = self.promotion_artifact
        if artifact is None or not artifact.authorizes(
            strategy_version=signal.strategy_version,
            direction=signal.direction,
            ai_influences_orders=self.ai_influences_orders,
        ):
            return "RESEARCH_PROMOTION_PENDING"
        if insight is not None and not artifact.authorizes_insight(
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        ):
            return "AI_RUNTIME_VERSION_MISMATCH"
        return "RESEARCH_PROMOTION_PENDING"

    def _market_timing_reasons(
        self, snapshot: EventSnapshot, decision_time: datetime
    ) -> tuple[str, ...]:
        market = snapshot.market
        reasons: list[str] = []
        if market.as_of > decision_time or market.quote.timestamp > decision_time:
            reasons.append("MARKET_STATE_FROM_FUTURE")
        elif decision_time - market.quote.timestamp > self.max_quote_age:
            reasons.append("MARKET_DATA_STALE_AFTER_INSIGHT")
        if self.execution_enabled and not market.market_data_live:
            reasons.append("MARKET_DATA_NOT_LIVE")
        return tuple(dict.fromkeys(reasons))

    def _entry_intent(
        self,
        signal: Signal,
        decision: RiskDecision,
        now: datetime,
        *,
        submission_mode: Literal["paper", "shadow"],
    ) -> OrderIntent:
        if not decision.approved or decision.quantity <= 0:
            raise ValueError("cannot construct an order from a rejected risk decision")
        side = OrderSide.BUY if signal.direction is Direction.LONG else OrderSide.SELL_SHORT
        idempotency_key = self._entry_order_key(signal)
        digest = sha256(f"{self.account_id}:{idempotency_key}".encode()).hexdigest()[:24]
        return OrderIntent(
            order_id=f"entry-{digest}",
            idempotency_key=idempotency_key,
            signal_id=signal.signal_id,
            account_id=self.account_id,
            submission_mode=submission_mode,
            research_promotion_sha256=(
                self.promotion_artifact.artifact_sha256
                if submission_mode == "paper" and self.promotion_artifact is not None
                else None
            ),
            symbol=signal.symbol,
            side=side,
            quantity=decision.quantity,
            limit_price=signal.entry_limit,
            created_at=now,
        )

    def _entry_order_key(self, signal: Signal) -> str:
        # One entry at most for an event/symbol/strategy.  Direction and decision
        # time are deliberately excluded so a retry or contradictory model answer
        # can never create a second order for the same filing.
        return self._entry_order_key_for(
            event_id=signal.event_id,
            symbol=signal.symbol,
            strategy_version=signal.strategy_version,
        )

    def _entry_order_key_for(
        self, *, event_id: str, symbol: str, strategy_version: str
    ) -> str:
        return f"{event_id}:{symbol}:{strategy_version}:{self.account_id}:entry"


__all__ = [
    "EventTradingOrchestrator",
    "InsightReader",
    "PipelineLedger",
    "PipelineOutcome",
]
