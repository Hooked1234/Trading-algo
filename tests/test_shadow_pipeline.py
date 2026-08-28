from __future__ import annotations

from decimal import Decimal

import pytest

from event_trader.analysis import AnalysisIdentity, AnalysisKey
from event_trader.broker import InMemoryPaperBroker
from event_trader.candidates import CandidateGate
from event_trader.domain import Direction, NewsInsight
from event_trader.execution import PaperExecutionService
from event_trader.orchestrator import EventTradingOrchestrator
from event_trader.risk import RiskEngine
from event_trader.strategy import ContinuationStrategy, QuantOnlyContinuationStrategy


class CountingInsightProvider:
    """Insight provider that records how often it was actually asked."""

    def __init__(self, insight) -> None:
        self.insight = insight
        self.calls = 0

    @property
    def analysis_identity(self) -> AnalysisIdentity:
        return AnalysisIdentity(
            model_id=self.insight.model_id,
            prompt_version=self.insight.prompt_version,
            schema_version=self.insight.schema_version,
        )

    async def analyze(self, _snapshot):
        self.calls += 1
        return self.insight


class MemoryInsightStore:
    def __init__(self) -> None:
        self.insights: dict[str, NewsInsight] = {}

    async def get_insight(self, analysis_key: str) -> NewsInsight | None:
        return self.insights.get(analysis_key)


class MemoryLedger:
    def __init__(self) -> None:
        self.signals: dict[str, object] = {}
        self.risks: dict[str, object] = {}
        self.intents: dict[str, object] = {}

    async def save_signal(self, value):
        self.signals[value.signal_id] = value
        return True

    async def save_risk_decision(self, value):
        self.risks[value.signal_id] = value
        return True

    async def save_order_intent(self, value):
        self.intents[value.order_id] = value
        return True

    async def get_order_intent_by_key(self, idempotency_key):
        return next(
            (i for i in self.intents.values() if i.idempotency_key == idempotency_key),
            None,
        )

    async def save_execution_report(self, value):
        return True


class RefusingBroker(InMemoryPaperBroker):
    """Paper broker that fails the test outright if a submission is attempted."""

    def submit(self, intent):
        raise AssertionError(f"shadow mode must never submit {intent.order_id!r}")


async def _reprice(_symbol, _side) -> Decimal:
    raise AssertionError("shadow mode must never reprice")


async def _no_sleep(_seconds: float) -> None:
    return None


def _orchestrator(provider, decision_time, *, strategy=None, insight_store=None):
    ledger = MemoryLedger()
    broker = RefusingBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )
    execution = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=_reprice,
        sleep=_no_sleep,
    )
    resolved = strategy or ContinuationStrategy()
    return EventTradingOrchestrator(
        insight_provider=provider,
        strategy=resolved,
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
        candidate_gate=CandidateGate(),
        insight_store=insight_store,
        ai_influences_orders=resolved.insight_influences_orders,
    )


@pytest.mark.asyncio
async def test_a_rejected_candidate_never_reaches_the_model(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    provider = CountingInsightProvider(long_insight)
    orchestrator = _orchestrator(provider, decision_time)
    halted = snapshot.model_copy(
        update={"market": snapshot.market.model_copy(update={"halted": True})}
    )

    outcome = await orchestrator.process(halted, empty_portfolio, now=decision_time)

    assert provider.calls == 0
    assert outcome.stage == "filtered"
    assert "SYMBOL_HALTED" in outcome.reasons
    assert outcome.insight is None
    assert outcome.candidate is not None
    assert outcome.candidate.accepted is False


@pytest.mark.asyncio
async def test_quant_only_never_constructs_an_insight(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    provider = CountingInsightProvider(long_insight)
    orchestrator = _orchestrator(
        provider, decision_time, strategy=QuantOnlyContinuationStrategy()
    )

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert provider.calls == 0
    assert outcome.insight is None
    assert outcome.analysis_key is None
    assert outcome.stage == "shadow_order"


@pytest.mark.asyncio
async def test_a_retry_reuses_the_stored_analysis(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    provider = CountingInsightProvider(long_insight)
    store = MemoryInsightStore()
    key = AnalysisKey.for_snapshot(snapshot, provider.analysis_identity)
    store.insights[key.key] = long_insight
    orchestrator = _orchestrator(provider, decision_time, insight_store=store)

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert provider.calls == 0
    assert outcome.reused_analysis is True
    assert outcome.insight == long_insight
    assert outcome.analysis_key is not None
    assert outcome.analysis_key.key == key.key


@pytest.mark.asyncio
async def test_a_first_pass_pays_exactly_once(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    provider = CountingInsightProvider(long_insight)
    orchestrator = _orchestrator(provider, decision_time, insight_store=MemoryInsightStore())

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert provider.calls == 1
    assert outcome.reused_analysis is False


@pytest.mark.asyncio
async def test_a_contradicting_model_direction_stops_the_event(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    # The deterministic gate confirms LONG; the model says SHORT.
    contradicting = long_insight.model_copy(update={"direction": Direction.SHORT})
    provider = CountingInsightProvider(contradicting)
    orchestrator = _orchestrator(provider, decision_time)

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert provider.calls == 1
    assert outcome.stage == "filtered"
    assert outcome.reasons == ("INSIGHT_DIRECTION_MISMATCH",)


@pytest.mark.asyncio
async def test_the_gate_runs_again_on_the_refreshed_snapshot(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    provider = CountingInsightProvider(long_insight)

    async def refresher(original, at):
        del at
        return original.model_copy(
            update={"market": original.market.model_copy(update={"halted": True})}
        )

    ledger = MemoryLedger()
    broker = RefusingBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )
    orchestrator = EventTradingOrchestrator(
        insight_provider=provider,
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=PaperExecutionService(
            broker=broker, ledger=ledger, repricer=_reprice, sleep=_no_sleep
        ),
        account_id="DU123456",
        candidate_gate=CandidateGate(),
        snapshot_refresher=refresher,
    )

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert provider.calls == 1
    assert outcome.stage == "filtered"
    assert "SYMBOL_HALTED" in outcome.reasons
    assert outcome.candidate is not None
    assert outcome.candidate.accepted is False


@pytest.mark.asyncio
async def test_shadow_path_records_its_candidate_and_analysis_lineage(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    provider = CountingInsightProvider(long_insight)
    orchestrator = _orchestrator(provider, decision_time)

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert outcome.stage == "shadow_order"
    assert outcome.shadow is True
    assert outcome.event_id == snapshot.filing.event_id
    assert outcome.strategy_version == ContinuationStrategy.version
    assert outcome.candidate is not None and outcome.candidate.accepted
    assert outcome.analysis_key is not None
    assert outcome.analysis_key.input_sha256
    assert outcome.order_intent is not None
    assert outcome.order_intent.submission_mode == "shadow"
