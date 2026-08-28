from decimal import Decimal

import pytest

from event_trader.analysis import AnalysisIdentity
from event_trader.broker import InMemoryPaperBroker
from event_trader.execution import PaperExecutionService
from event_trader.orchestrator import EventTradingOrchestrator
from event_trader.risk import RiskEngine
from event_trader.session import TradingSession
from event_trader.storage import SQLiteOperationalStore
from event_trader.strategy import ContinuationStrategy


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

    async def analyze(self, _snapshot):
        self.calls += 1
        return self.insight


class StaticSnapshotFactory:
    def __init__(self, snapshot) -> None:
        self.snapshot = snapshot

    async def build(self, filing, *, as_of):
        market = self.snapshot.market.model_copy(update={"as_of": as_of})
        return self.snapshot.model_copy(update={"filing": filing, "market": market})


async def _no_sleep(_seconds: float) -> None:
    return None


async def _reprice(_symbol, _side) -> Decimal:
    return Decimal("100")


@pytest.mark.asyncio
async def test_session_consumes_filing_outbox_into_shadow_order(
    tmp_path,
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
) -> None:
    async with SQLiteOperationalStore(
        tmp_path / "state.sqlite",
        tmp_path / "raw",
        clock=lambda: decision_time,
    ) as store:
        assert await store.save_filing_event(snapshot.filing)
        broker = InMemoryPaperBroker(
            account_id="DU123456",
            paper_account_allowlist=("DU123456",),
            clock=lambda: decision_time,
        )
        execution = PaperExecutionService(
            broker=broker,
            ledger=store,
            repricer=_reprice,
            sleep=_no_sleep,
        )
        orchestrator = EventTradingOrchestrator(
            insight_provider=StaticInsightProvider(long_insight),
            strategy=ContinuationStrategy(),
            risk_engine=RiskEngine(),
            ledger=store,
            broker=broker,
            execution_service=execution,
            account_id="DU123456",
        )

        async def portfolio_provider():
            return empty_portfolio

        session = TradingSession(
            store=store,
            snapshot_factory=StaticSnapshotFactory(snapshot),
            portfolio_provider=portfolio_provider,
            orchestrator=orchestrator,
        )
        outcomes = await session.process_ready(now=decision_time)
        assert len(outcomes) == 1
        assert outcomes[0].stage == "shadow_order"
        assert await store.count_outbox(published=False) == 0
        assert len(await store.list_order_intents()) == 1
