"""Assemble one hashed backtest run from immutable case and insight artifacts.

A variant run never receives a hand-assembled list of trades.  It receives the
complete case artifact, evaluates every case exactly once, and records the
result — including filtered and rejected cases — inside a hashed run artifact.
"""

from __future__ import annotations

from .backtest import (
    BacktestRunArtifact,
    BacktestVariant,
    HistoricalBacktester,
)
from .costs import CostModel
from .insights import InsightArtifact
from .research_cases import ResearchCaseBuildArtifact
from .risk import RiskEngine
from .strategy import ContinuationStrategy, QuantOnlyContinuationStrategy


class ResearchRunIntegrityError(ValueError):
    """A run does not faithfully cover the cases it claims to cover."""


def build_backtest_run(
    case_artifact: ResearchCaseBuildArtifact,
    *,
    variant: BacktestVariant,
    insight_artifact: InsightArtifact | None = None,
    strategy: ContinuationStrategy | None = None,
    risk_engine: RiskEngine | None = None,
    cost_model: CostModel | None = None,
) -> BacktestRunArtifact:
    case_artifact.verify()
    if variant is BacktestVariant.QUANT_ONLY:
        if insight_artifact is not None:
            raise ResearchRunIntegrityError("the quant-only variant must not consume insights")
        resolved_strategy: ContinuationStrategy = strategy or QuantOnlyContinuationStrategy()
    else:
        if insight_artifact is None:
            raise ResearchRunIntegrityError(f"the {variant.value} variant requires insights")
        insight_artifact.verify()
        if insight_artifact.variant is not variant:
            raise ResearchRunIntegrityError("insight artifact belongs to a different variant")
        if insight_artifact.case_artifact_sha256 != case_artifact.artifact_sha256:
            raise ResearchRunIntegrityError("insight artifact is bound to different cases")
        unknown = set(insight_artifact.candidate_case_hashes) - set(case_artifact.case_hashes)
        if unknown:
            raise ResearchRunIntegrityError("insight artifact references unknown cases")
        resolved_strategy = strategy or ContinuationStrategy()

    model = cost_model or CostModel()
    backtester = HistoricalBacktester(
        strategy=resolved_strategy,
        risk_engine=risk_engine,
        cost_model=model,
    )
    insights = insight_artifact.by_case() if insight_artifact is not None else None
    outcomes = backtester.run(case_artifact.cases, insights)
    artifact = BacktestRunArtifact(
        variant=variant,
        strategy_version=resolved_strategy.version,
        cost_model_version=model.version,
        case_artifact_sha256=case_artifact.artifact_sha256,
        insight_artifact_sha256=(
            insight_artifact.artifact_sha256 if insight_artifact is not None else None
        ),
        case_hashes=case_artifact.case_hashes,
        outcomes=outcomes,
    )
    return artifact.sealed()


__all__ = ["ResearchRunIntegrityError", "build_backtest_run"]
