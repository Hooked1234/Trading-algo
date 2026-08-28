from __future__ import annotations

from datetime import timedelta

import pytest
from research_fixtures import case_inputs, trading_state_manifest

from event_trader.backtest import BacktestVariant, HistoricalBacktester
from event_trader.datasets import ParquetMarketDataLake
from event_trader.documents import FilingDocumentLoader
from event_trader.insights import InsightArtifactError, build_insight_artifact
from event_trader.research_cases import (
    HistoricalResearchCaseBuilder,
    ResearchCaseIntegrityError,
)
from event_trader.strategy import ContinuationStrategy


def test_research_case_builder_reconstructs_reproducible_closed_trade(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _ = case_inputs(
        tmp_path, filing, long_insight, decision_time
    )

    first = builder.build(coverage, state)
    second = builder.build(coverage, state)
    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(first, insight)

    assert first == second
    assert first.lineage is not None
    assert first.lineage.coverage_record_id == coverage.record_id
    assert first.entry_reprice is not None
    assert first.entry_reprice.attempted_at == decision_time + timedelta(seconds=5)
    assert len(first.exit_points) == 60
    assert outcome.stage == "closed_trade"
    assert outcome.trade is not None
    assert outcome.trade.metadata["case_input_sha256"] == first.lineage.case_input_sha256
    assert outcome.trade.metadata["coverage_record_id"] == coverage.record_id


def test_research_case_builder_rejects_unknown_halt_state(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, _ = case_inputs(
        tmp_path, filing, long_insight, decision_time
    )

    with pytest.raises(ResearchCaseIntegrityError, match="halt state is unknown"):
        builder.build(coverage, state.model_copy(update={"halted": None}))


def test_insight_artifact_revalidates_evidence_grounding(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _ = case_inputs(
        tmp_path, filing, long_insight, decision_time
    )
    case = builder.build(coverage, state)
    manifest = trading_state_manifest(state)
    artifact = builder.build_all([coverage], manifest, lag_minutes=5)
    bad_evidence = insight.evidence[0].model_copy(update={"excerpt": "Invented statement"})

    assert artifact.cases == (case,)
    with pytest.raises(InsightArtifactError, match="not grounded"):
        build_insight_artifact(
            artifact,
            [insight.model_copy(update={"evidence": (bad_evidence,)})],
            variant=BacktestVariant.AI,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )


def test_research_case_builder_rejects_a_missing_exit_minute(
    tmp_path, filing, long_insight, decision_time
) -> None:
    _, coverage, _insight, state, lake = case_inputs(
        tmp_path, filing, long_insight, decision_time
    )
    missing_at = decision_time + timedelta(minutes=17)
    original = lake.read_bars(
        "AAPL",
        start=coverage.bundle_start,
        end=coverage.bundle_end,
    )
    incomplete_lake = ParquetMarketDataLake(tmp_path / "incomplete-lake")
    incomplete_lake.write_filings(
        [lake.read_filing(coverage.accession_number)], batch_id="filing"
    )
    incomplete_lake.write_bars(
        (bar for bar in original if bar.timestamp != missing_at), batch_id="asset"
    )
    incomplete_lake.write_bars(
        lake.read_bars("SPY", start=coverage.bundle_start, end=coverage.bundle_end),
        batch_id="benchmark",
    )
    incomplete_lake.write_quotes(
        lake.read_quotes(
            "AAPL",
            start=decision_time - timedelta(seconds=5),
            end=coverage.window_end,
        ),
        batch_id="quotes",
    )
    incomplete_builder = HistoricalResearchCaseBuilder(
        data=incomplete_lake,
        documents=FilingDocumentLoader(tmp_path / "raw"),
    )

    with pytest.raises(ResearchCaseIntegrityError, match="missing exit bar"):
        incomplete_builder.build(coverage, state)
