from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError
from research_fixtures import case_inputs, trading_state_manifest

from event_trader.artifacts import ArtifactIntegrityError, read_artifact, write_artifact
from event_trader.backfill import CoverageStatus
from event_trader.backtest import BacktestRunArtifact, BacktestVariant
from event_trader.documents import FilingDocumentLoader
from event_trader.insights import (
    InsightArtifactError,
    build_insight_artifact,
    select_insight_candidates,
)
from event_trader.research import ResearchRunGateError, evaluate_research_run, paired_ai_gate
from event_trader.research_cases import (
    HistoricalResearchCaseBuilder,
    ResearchCaseBuildArtifact,
    ResearchCaseExcluded,
    ResearchCaseFailureKind,
    TradingStateManifest,
)
from event_trader.research_runs import build_backtest_run


def _artifacts(tmp_path, filing, long_insight, decision_time):
    builder, coverage, insight, state, lake = case_inputs(
        tmp_path, filing, long_insight, decision_time
    )
    manifest = trading_state_manifest(state)
    cases = builder.build_all([coverage], manifest, lag_minutes=5)
    return builder, coverage, insight, state, lake, manifest, cases


def _ai_artifact(cases, insight):
    return build_insight_artifact(
        cases,
        [insight],
        variant=BacktestVariant.AI,
        model_id=insight.model_id,
        prompt_version=insight.prompt_version,
        schema_version=insight.schema_version,
    )


def test_quant_and_ai_runs_share_one_case_identity(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )

    quant_run = build_backtest_run(cases, variant=BacktestVariant.QUANT_ONLY)
    ai_run = build_backtest_run(
        cases,
        variant=BacktestVariant.AI,
        insight_artifact=_ai_artifact(cases, insight),
    )

    assert cases.case_hashes == quant_run.case_hashes == ai_run.case_hashes
    assert quant_run.case_artifact_sha256 == ai_run.case_artifact_sha256
    assert quant_run.insight_artifact_sha256 is None
    assert ai_run.insight_artifact_sha256 is not None
    assert quant_run.strategy_version != ai_run.strategy_version


def test_case_hash_ignores_local_paths_and_ingestion_timestamps(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    case = cases.cases[0]
    assert case.lineage is not None

    # The same document, stored under a second raw root, with an ingestion
    # timestamp from a much later re-download.
    mirror_root = tmp_path / "raw-mirror"
    mirror_root.mkdir()
    original = tmp_path / "raw" / "exhibit.html"
    mirror = mirror_root / "copy.html"
    mirror.write_bytes(original.read_bytes())
    stored = lake.read_filing(coverage.accession_number)
    relocated = stored.model_copy(
        update={
            "documents": tuple(
                document.model_copy(update={"local_path": str(mirror)})
                for document in stored.documents
            ),
            "retrieved_at": stored.first_seen_at + timedelta(days=900),
        }
    )

    class _RelocatedLake:
        def __init__(self, inner):
            self._inner = inner

        def read_filing(self, accession_number, *, source=None, quarter=None):
            del accession_number, source, quarter
            return relocated

        def read_bars(self, *args, **kwargs):
            return self._inner.read_bars(*args, **kwargs)

        def read_quotes(self, *args, **kwargs):
            return self._inner.read_quotes(*args, **kwargs)

    relocated_builder = HistoricalResearchCaseBuilder(
        data=_RelocatedLake(lake),
        documents=FilingDocumentLoader(mirror_root),
    )
    rebuilt = relocated_builder.build(coverage, state)

    assert rebuilt.lineage is not None
    assert rebuilt.lineage.case_input_sha256 == case.lineage.case_input_sha256
    assert builder is not relocated_builder


def test_every_coverage_record_gets_exactly_one_typed_outcome(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, *_ = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    skipped = coverage.model_copy(
        update={
            "record_id": f"{coverage.record_id}:no-quotes",
            "status": CoverageStatus.MISSING_QUOTES,
            "tradable_coverage_complete": False,
        }
    )

    artifact = builder.build_all(
        [coverage, skipped],
        trading_state_manifest(state),
        lag_minutes=5,
    )

    assert artifact.coverage_count == 2
    assert len(artifact.cases) == 1
    assert len(artifact.failures) == 1
    assert artifact.failures[0].kind is ResearchCaseFailureKind.EXCLUDED
    assert artifact.failures[0].coverage_record_id == skipped.record_id


def test_unknown_borrow_evidence_allows_long_and_blocks_short(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, *_ = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )

    unknown_borrow = state.model_copy(
        update={"shortable": None, "shortable_shares": 0}
    )
    case = builder.build(coverage, unknown_borrow)

    assert case.snapshot.market.shortable is False
    assert case.snapshot.market.shortable_shares == 0
    assert case.snapshot.market.halted is False


def test_unknown_halt_evidence_excludes_the_case(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, *_ = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )

    with pytest.raises(ResearchCaseExcluded, match="halt state is unknown"):
        builder.build(coverage, state.model_copy(update={"halted": None}))


def test_manifest_ignores_evidence_that_was_only_knowable_later(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, *_ = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    late = state.model_copy(update={"known_at": decision_time + timedelta(minutes=1)})

    artifact = builder.build_all(
        [coverage],
        trading_state_manifest(late),
        lag_minutes=5,
    )

    assert artifact.cases == ()
    assert artifact.failures[0].kind is ResearchCaseFailureKind.EXCLUDED
    assert "no point-in-time trading state" in artifact.failures[0].reason


def test_trading_state_manifest_rejects_impossible_knowledge(decision_time) -> None:
    with pytest.raises(ValidationError, match="known before it is observed"):
        TradingStateManifest(
            source="test",
            entries=(
                {
                    "symbol": "AAPL",
                    "as_of": decision_time,
                    "known_at": decision_time - timedelta(seconds=1),
                    "source": "test",
                    "halted": False,
                },
            ),
        )


def test_insight_artifact_requires_one_record_per_candidate(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )

    assert len(select_insight_candidates(cases)) == 1
    with pytest.raises(InsightArtifactError, match="no pinned insight"):
        build_insight_artifact(
            cases,
            [],
            variant=BacktestVariant.AI,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )
    with pytest.raises(InsightArtifactError, match="not candidates"):
        build_insight_artifact(
            cases,
            [
                insight,
                insight.model_copy(update={"event_id": "0000320193-26-000099"}),
            ],
            variant=BacktestVariant.AI,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )


def test_quant_only_run_never_consumes_insights(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )

    with pytest.raises(ValueError, match="must not consume insights"):
        build_backtest_run(
            cases,
            variant=BacktestVariant.QUANT_ONLY,
            insight_artifact=_ai_artifact(cases, insight),
        )


def test_run_artifact_rejects_a_removed_outcome(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, _insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    run = build_backtest_run(cases, variant=BacktestVariant.QUANT_ONLY)

    with pytest.raises(ValidationError, match="exactly one outcome per case"):
        BacktestRunArtifact(
            variant=run.variant,
            strategy_version=run.strategy_version,
            cost_model_version=run.cost_model_version,
            case_artifact_sha256=run.case_artifact_sha256,
            case_hashes=run.case_hashes,
            outcomes=(),
        )


def test_gate_rejects_a_tampered_run_artifact(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, _insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    run = build_backtest_run(cases, variant=BacktestVariant.QUANT_ONLY)
    tampered = run.model_copy(update={"cost_model_version": "cost-model-v1/tampered"})

    with pytest.raises(ArtifactIntegrityError):
        evaluate_research_run(tampered)


def test_paired_gate_requires_one_shared_case_artifact(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, _state, _lake, manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    other_cases = builder.build_all(
        [coverage],
        manifest,
        lag_minutes=5,
        dataset_manifest_sha256="b" * 64,
    )
    quant_run = build_backtest_run(cases, variant=BacktestVariant.QUANT_ONLY)
    ai_run = build_backtest_run(
        other_cases,
        variant=BacktestVariant.AI,
        insight_artifact=_ai_artifact(other_cases, insight),
    )

    assert cases.artifact_sha256 != other_cases.artifact_sha256
    with pytest.raises(ResearchRunGateError, match="one research case artifact"):
        paired_ai_gate(quant_run, ai_run)


def test_artifacts_are_written_exclusively_and_reverify_on_read(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, _insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    target = tmp_path / "artifacts" / "cases.json"

    write_artifact(cases, target)
    reloaded = read_artifact(ResearchCaseBuildArtifact, target)

    assert reloaded == cases
    with pytest.raises(FileExistsError):
        write_artifact(cases, target)


def test_reading_a_manipulated_artifact_file_fails_closed(
    tmp_path, filing, long_insight, decision_time
) -> None:
    *_, _insight, _state, _lake, _manifest, cases = _artifacts(
        tmp_path, filing, long_insight, decision_time
    )
    target = tmp_path / "cases.json"
    write_artifact(cases, target)
    target.write_text(
        target.read_text(encoding="utf-8").replace('"coverage_count": 1', '"coverage_count": 0'),
        encoding="utf-8",
    )

    with pytest.raises((ArtifactIntegrityError, ValidationError)):
        read_artifact(ResearchCaseBuildArtifact, target)
