from datetime import timedelta

import pytest

from event_trader.domain import NewsInsight
from event_trader.orchestrator import PipelineOutcome
from event_trader.replay import ReplayCase, ReplayEngine


class RecordingOrchestrator:
    def __init__(self) -> None:
        self.times = []

    async def process(self, snapshot, portfolio, *, now):
        self.times.append(now)
        return PipelineOutcome(
            stage="recorded",
            insight=NewsInsight.abstain(
                event_id=snapshot.filing.event_id,
                accession_number=snapshot.filing.accession_number,
                reason="replay_test",
            ),
            metadata={"nav": str(portfolio.nav)},
        )


@pytest.mark.asyncio
async def test_replay_orders_cases_chronologically(
    snapshot, empty_portfolio, decision_time
) -> None:
    orchestrator = RecordingOrchestrator()
    engine = ReplayEngine(orchestrator)
    later = decision_time + timedelta(minutes=1)

    result = await engine.run(
        [
            ReplayCase(
                decision_time=later,
                snapshot=snapshot,
                portfolio=empty_portfolio,
            ),
            ReplayCase(
                decision_time=decision_time,
                snapshot=snapshot,
                portfolio=empty_portfolio,
            ),
        ]
    )

    assert orchestrator.times == [decision_time, later]
    assert result.started_at == decision_time
    assert result.ended_at == later
    assert len(result.outcomes) == 2


@pytest.mark.asyncio
async def test_replay_rejects_empty_case_set() -> None:
    with pytest.raises(ValueError, match="at least one"):
        await ReplayEngine(RecordingOrchestrator()).run([])
