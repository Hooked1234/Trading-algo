from decimal import Decimal

import pytest

from event_trader.analysis import AnalysisIdentity
from event_trader.broker import InMemoryPaperBroker
from event_trader.domain import Direction, Position
from event_trader.execution import ExitPolicy, PaperExecutionService
from event_trader.orchestrator import EventTradingOrchestrator
from event_trader.preflight import PreflightRejected
from event_trader.promotion import ResearchPromotionArtifact
from event_trader.risk import RiskEngine
from event_trader.strategy import ContinuationStrategy, QuantOnlyContinuationStrategy


class StaticInsightProvider:
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

    async def analyze(self, snapshot):
        self.calls += 1
        return self.insight


class MemoryLedger:
    def __init__(self) -> None:
        self.signals = {}
        self.risks = {}
        self.intents = {}
        self.reports = {}

    async def save_signal(self, value):
        created = value.signal_id not in self.signals
        self.signals[value.signal_id] = value
        return created

    async def save_risk_decision(self, value):
        created = value.signal_id not in self.risks
        self.risks[value.signal_id] = value
        return created

    async def save_order_intent(self, value):
        created = value.order_id not in self.intents
        if created:
            self.intents[value.order_id] = value
        return created

    async def get_order_intent_by_key(self, idempotency_key):
        return next(
            (
                intent
                for intent in self.intents.values()
                if intent.idempotency_key == idempotency_key
            ),
            None,
        )

    async def save_execution_report(self, value):
        created = value.order_id not in self.reports
        self.reports[value.order_id] = value
        return created


async def _no_sleep(_seconds: float) -> None:
    return None


async def _reprice(_symbol, _side) -> Decimal:
    return Decimal("100.11")


async def _pre_submit_guard(_intent) -> bool:
    return True


def _components(long_insight, decision_time, *, pre_submit_guard=_pre_submit_guard):
    ledger = MemoryLedger()
    broker = InMemoryPaperBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )
    execution = PaperExecutionService(
        broker=broker,
        ledger=ledger,
        repricer=_reprice,
        pre_submit_guard=pre_submit_guard,
        promotion_artifact_sha256=_promotion(decision_time).artifact_sha256,
        sleep=_no_sleep,
    )
    return ledger, broker, execution


def _promotion(decision_time) -> ResearchPromotionArtifact:
    return ResearchPromotionArtifact.create(
        experiment_id="sec-8k-v1-holdout",
        strategy_version=ContinuationStrategy.version,
        enabled_directions=(Direction.LONG, Direction.SHORT),
        ai_influences_orders=True,
        research_gate_passed=True,
        paired_improvement_passed=True,
        model_gate_passed=True,
        experiment_manifest_sha256="a" * 64,
        dataset_manifest_sha256="b" * 64,
        code_revision_sha256="c" * 64,
        research_result_sha256="d" * 64,
        research_evidence_sha256="0" * 64,
        paired_result_sha256="e" * 64,
        model_result_sha256="f" * 64,
        paired_evidence_sha256="1" * 64,
        model_evidence_sha256="2" * 64,
        model_id="fixture/fixture-v1",
        prompt_version="1",
        schema_version="1",
        created_at=decision_time,
    )


def _live_refreshers(snapshot, empty_portfolio):
    async def snapshot_refresher(value, now):
        assert value.filing == snapshot.filing
        quote = value.market.quote.model_copy(update={"timestamp": now})
        market = value.market.model_copy(
            update={"as_of": now, "quote": quote, "market_data_live": True}
        )
        return value.model_copy(update={"market": market})

    async def portfolio_refresher(now):
        return empty_portfolio.model_copy(update={"as_of": now})

    return snapshot_refresher, portfolio_refresher


@pytest.mark.asyncio
async def test_orchestrator_logs_shadow_order_before_gate(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    provider = StaticInsightProvider(long_insight)
    orchestrator = EventTradingOrchestrator(
        insight_provider=provider,
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
    )
    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)
    assert outcome.stage == "shadow_order"
    assert outcome.shadow
    assert not broker.reports
    assert len(ledger.intents) == 1
    assert outcome.order_intent is not None
    assert outcome.order_intent.submission_mode == "shadow"


def test_orchestrator_cannot_bypass_research_gate(long_insight, decision_time) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    with pytest.raises(ValueError, match="promotion artifact"):
        EventTradingOrchestrator(
            insight_provider=StaticInsightProvider(long_insight),
            strategy=ContinuationStrategy(),
            risk_engine=RiskEngine(),
            ledger=ledger,
            broker=broker,
            execution_service=execution,
            account_id="DU123456",
            execution_enabled=True,
        )


def test_ai_strategy_cannot_claim_quant_only_promotion_mode(long_insight, decision_time) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)

    with pytest.raises(ValueError, match="must match"):
        EventTradingOrchestrator(
            insight_provider=StaticInsightProvider(long_insight),
            strategy=ContinuationStrategy(),
            risk_engine=RiskEngine(),
            ledger=ledger,
            broker=broker,
            execution_service=execution,
            account_id="DU123456",
            ai_influences_orders=False,
        )


def test_quant_only_strategy_cannot_claim_ai_promotion_mode(long_insight, decision_time) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)

    with pytest.raises(ValueError, match="must match"):
        EventTradingOrchestrator(
            insight_provider=StaticInsightProvider(long_insight),
            strategy=QuantOnlyContinuationStrategy(),
            risk_engine=RiskEngine(),
            ledger=ledger,
            broker=broker,
            execution_service=execution,
            account_id="DU123456",
        )


def test_order_capable_orchestrator_requires_a_real_du_account_id(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """``DU`` as a prefix is not the rule; ``DU<digits>`` is.

    This check was the last place that still tested the prefix alone after the
    rule was unified, so an id like ``DUMMY123`` passed here while the same id
    was refused by configuration, composition and the broker adapter.
    """

    ledger, broker, execution = _components(long_insight, decision_time)
    snapshot_refresher, portfolio_refresher = _live_refreshers(snapshot, empty_portfolio)
    with pytest.raises(ValueError, match="DU account"):
        EventTradingOrchestrator(
            insight_provider=StaticInsightProvider(long_insight),
            strategy=ContinuationStrategy(),
            risk_engine=RiskEngine(),
            ledger=ledger,
            broker=broker,
            execution_service=execution,
            account_id="DUMMY123",
            promotion_artifact=_promotion(decision_time),
            runtime_experiment_manifest_sha256="a" * 64,
            runtime_dataset_manifest_sha256="b" * 64,
            runtime_code_revision_sha256="c" * 64,
            snapshot_refresher=snapshot_refresher,
            portfolio_refresher=portfolio_refresher,
            decision_clock=lambda: decision_time,
            execution_enabled=True,
        )


def test_promotion_cannot_be_reused_after_code_manifest_changes(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    snapshot_refresher, portfolio_refresher = _live_refreshers(snapshot, empty_portfolio)
    with pytest.raises(ValueError, match="runtime manifests"):
        EventTradingOrchestrator(
            insight_provider=StaticInsightProvider(long_insight),
            strategy=ContinuationStrategy(),
            risk_engine=RiskEngine(),
            ledger=ledger,
            broker=broker,
            execution_service=execution,
            account_id="DU123456",
            promotion_artifact=_promotion(decision_time),
            runtime_experiment_manifest_sha256="a" * 64,
            runtime_dataset_manifest_sha256="b" * 64,
            runtime_code_revision_sha256="0" * 64,
            snapshot_refresher=snapshot_refresher,
            portfolio_refresher=portfolio_refresher,
            decision_clock=lambda: decision_time,
            execution_enabled=True,
        )


@pytest.mark.asyncio
async def test_paper_path_cancels_then_reprices_once(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    snapshot_refresher, portfolio_refresher = _live_refreshers(snapshot, empty_portfolio)
    provider = StaticInsightProvider(long_insight)
    orchestrator = EventTradingOrchestrator(
        insight_provider=provider,
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
        promotion_artifact=_promotion(decision_time),
        runtime_experiment_manifest_sha256="a" * 64,
        runtime_dataset_manifest_sha256="b" * 64,
        runtime_code_revision_sha256="c" * 64,
        snapshot_refresher=snapshot_refresher,
        portfolio_refresher=portfolio_refresher,
        decision_clock=lambda: decision_time,
        execution_enabled=True,
    )
    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)
    assert outcome.stage == "paper_submitted"
    assert not outcome.shadow
    assert outcome.order_intent is not None
    assert outcome.order_intent.submission_mode == "paper"
    assert len(ledger.intents) == 2
    assert sum(order_id.endswith("-r1") for order_id in ledger.intents) == 1


@pytest.mark.asyncio
async def test_live_preflight_rejection_becomes_terminal_pipeline_outcome(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    async def reject(_intent) -> bool:
        raise PreflightRejected(("MARKET_DATA_STALE", "MAX_POSITIONS"))

    ledger, broker, execution = _components(
        long_insight,
        decision_time,
        pre_submit_guard=reject,
    )
    snapshot_refresher, portfolio_refresher = _live_refreshers(snapshot, empty_portfolio)
    orchestrator = EventTradingOrchestrator(
        insight_provider=StaticInsightProvider(long_insight),
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
        promotion_artifact=_promotion(decision_time),
        runtime_experiment_manifest_sha256="a" * 64,
        runtime_dataset_manifest_sha256="b" * 64,
        runtime_code_revision_sha256="c" * 64,
        snapshot_refresher=snapshot_refresher,
        portfolio_refresher=portfolio_refresher,
        decision_clock=lambda: decision_time,
        execution_enabled=True,
    )

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert outcome.stage == "preflight_rejected"
    assert outcome.reasons == ("MARKET_DATA_STALE", "MAX_POSITIONS")
    assert outcome.order_intent is not None
    assert ledger.intents == {}
    assert not broker.reports


@pytest.mark.asyncio
async def test_paper_promotion_is_bound_to_runtime_model_prompt_and_schema(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    snapshot_refresher, portfolio_refresher = _live_refreshers(snapshot, empty_portfolio)
    changed_prompt = long_insight.model_copy(update={"prompt_version": "unbenchmarked-v2"})
    orchestrator = EventTradingOrchestrator(
        insight_provider=StaticInsightProvider(changed_prompt),
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
        promotion_artifact=_promotion(decision_time),
        runtime_experiment_manifest_sha256="a" * 64,
        runtime_dataset_manifest_sha256="b" * 64,
        runtime_code_revision_sha256="c" * 64,
        snapshot_refresher=snapshot_refresher,
        portfolio_refresher=portfolio_refresher,
        decision_clock=lambda: decision_time,
        execution_enabled=True,
    )

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert outcome.stage == "shadow_order"
    assert outcome.reasons == ("AI_RUNTIME_VERSION_MISMATCH",)
    assert outcome.order_intent is not None
    assert outcome.order_intent.submission_mode == "shadow"
    assert not broker.reports


@pytest.mark.asyncio
async def test_duplicate_event_cannot_create_second_order(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    snapshot_refresher, portfolio_refresher = _live_refreshers(snapshot, empty_portfolio)
    provider = StaticInsightProvider(long_insight)
    orchestrator = EventTradingOrchestrator(
        insight_provider=provider,
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
        promotion_artifact=_promotion(decision_time),
        runtime_experiment_manifest_sha256="a" * 64,
        runtime_dataset_manifest_sha256="b" * 64,
        runtime_code_revision_sha256="c" * 64,
        snapshot_refresher=snapshot_refresher,
        portfolio_refresher=portfolio_refresher,
        decision_clock=lambda: decision_time,
        execution_enabled=True,
    )

    first = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)
    duplicate = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert first.stage == "paper_submitted"
    assert duplicate.stage == "duplicate_event"
    assert duplicate.reasons == ("DUPLICATE_EVENT_ID",)
    assert provider.calls == 1
    assert len(ledger.intents) == 2  # original plus its one permitted reprice


@pytest.mark.asyncio
async def test_model_latency_makes_unrefreshed_quote_fail_closed(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    ledger, broker, execution = _components(long_insight, decision_time)
    delayed = long_insight.model_copy(update={"latency_ms": 6_000})
    orchestrator = EventTradingOrchestrator(
        insight_provider=StaticInsightProvider(delayed),
        strategy=ContinuationStrategy(),
        risk_engine=RiskEngine(),
        ledger=ledger,
        broker=broker,
        execution_service=execution,
        account_id="DU123456",
    )

    outcome = await orchestrator.process(snapshot, empty_portfolio, now=decision_time)

    assert outcome.stage == "filtered"
    assert outcome.reasons == ("MARKET_DATA_STALE_AFTER_INSIGHT",)


def test_exit_policy_creates_stop_exit(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    position = Position(
        symbol="AAPL",
        direction=signal.direction,
        quantity=10,
        market_price=signal.stop_price,
        average_price=signal.entry_limit,
    )
    market = snapshot.market.model_copy(update={"last": signal.stop_price})
    intent = ExitPolicy().order(
        signal=signal,
        position=position,
        market=market,
        account_id="DU123456",
        now=decision_time,
    )
    assert intent is not None
    assert intent.idempotency_key.endswith("STOP_EXIT")
