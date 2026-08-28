"""Chronological event replay through the production orchestration path."""

from __future__ import annotations

from datetime import datetime

from .domain import EventSnapshot, FrozenModel, PortfolioState
from .orchestrator import EventTradingOrchestrator, PipelineOutcome


class ReplayCase(FrozenModel):
    decision_time: datetime
    snapshot: EventSnapshot
    portfolio: PortfolioState


class ReplayResult(FrozenModel):
    outcomes: tuple[PipelineOutcome, ...]
    started_at: datetime
    ended_at: datetime


class ReplayEngine:
    def __init__(self, orchestrator: EventTradingOrchestrator) -> None:
        self.orchestrator = orchestrator

    async def run(self, cases: list[ReplayCase]) -> ReplayResult:
        if not cases:
            raise ValueError("replay requires at least one case")
        ordered = sorted(cases, key=lambda case: case.decision_time)
        outcomes = []
        for case in ordered:
            outcomes.append(
                await self.orchestrator.process(
                    case.snapshot,
                    case.portfolio,
                    now=case.decision_time,
                )
            )
        return ReplayResult(
            outcomes=tuple(outcomes),
            started_at=ordered[0].decision_time,
            ended_at=ordered[-1].decision_time,
        )
