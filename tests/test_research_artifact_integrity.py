from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError
from research_fixtures import case_inputs, trading_state_manifest

from event_trader.artifacts import (
    ArtifactIntegrityError,
    canonical_hash,
    canonical_json,
    write_artifact,
)
from event_trader.backtest import BacktestVariant
from event_trader.datasets import (
    DatasetManifest,
    DatasetPartition,
    build_dataset_manifest,
)
from event_trader.domain import Direction, InsightStatus, NewsInsight
from event_trader.insights import InsightArtifact, InsightArtifactError, build_insight_artifact
from event_trader.research import ResearchRunGateError, paired_ai_gate
from event_trader.research_cases import (
    ResearchCaseFailureKind,
    ResearchCaseIntegrityError,
    TradingStateManifest,
)
from event_trader.research_runs import build_backtest_run
from event_trader.strategy import deterministic_direction


def _inputs(tmp_path, filing, long_insight, decision_time):
    builder, coverage, insight, state, lake = case_inputs(
        tmp_path, filing, long_insight, decision_time
    )
    return builder, coverage, insight, state, lake


# --------------------------------------------------------------- hashing ----


def test_canonical_json_is_stable_across_key_order() -> None:
    first = {"b": 1, "a": [3, 2]}
    second = {"a": [3, 2], "b": 1}

    assert canonical_json(first) == canonical_json(second)
    assert canonical_hash(first) == canonical_hash(second)


def test_dataset_manifest_hashes_every_partition(tmp_path, filing, long_insight, decision_time):
    _builder, _coverage, _insight, _state, lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )

    manifest = build_dataset_manifest(lake.root)
    reference = build_dataset_manifest(lake.root)

    assert manifest.partitions
    assert manifest == reference
    assert all(partition.size_bytes > 0 for partition in manifest.partitions)
    manifest.verify()


def test_dataset_manifest_requires_canonical_order() -> None:
    partitions = (
        DatasetPartition(path="b.parquet", sha256="b" * 64, size_bytes=1),
        DatasetPartition(path="a.parquet", sha256="a" * 64, size_bytes=1),
    )

    with pytest.raises(ValidationError, match="canonically ordered"):
        DatasetManifest(partitions=partitions)


def test_dataset_manifest_rejects_duplicate_partitions() -> None:
    partition = DatasetPartition(path="a.parquet", sha256="a" * 64, size_bytes=1)

    with pytest.raises(ValidationError, match="exactly once"):
        DatasetManifest(partitions=(partition, partition))


def test_write_artifact_refuses_an_unsealed_artifact(tmp_path) -> None:
    manifest = TradingStateManifest(source="unsealed")

    with pytest.raises(ArtifactIntegrityError):
        write_artifact(manifest, tmp_path / "manifest.json")
    assert not (tmp_path / "manifest.json").exists()


# ------------------------------------------------------- case integrity -----


def test_duplicate_coverage_record_ids_fail_closed(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )

    with pytest.raises(ResearchCaseIntegrityError, match="duplicate record ids"):
        builder.build_all(
            [coverage, coverage],
            trading_state_manifest(state),
            lag_minutes=5,
        )


def test_unregistered_lag_is_rejected_before_any_read(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, _coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )

    with pytest.raises(ValueError, match="lag is not registered"):
        builder.build_all([], trading_state_manifest(state), lag_minutes=7)


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"provider": "polygon"}, "provider does not match"),
        ({"feed": "iex"}, "requires the SIP feed"),
        ({"benchmark_symbol": "QQQ"}, "SPY benchmark"),
    ],
)
def test_coverage_provenance_must_match_the_reader(
    tmp_path, filing, long_insight, decision_time, update, message
) -> None:
    builder, coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )

    with pytest.raises(ResearchCaseIntegrityError, match=message):
        builder.build(coverage.model_copy(update=update), state)


def test_coverage_availability_must_match_its_own_lag(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    shifted = coverage.model_copy(
        update={"available_at": coverage.available_at - timedelta(minutes=1)}
    )

    with pytest.raises(ResearchCaseIntegrityError, match="does not match the lag"):
        builder.build(shifted, state)


def test_stale_trading_state_is_an_integrity_error(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    stale = state.model_copy(
        update={
            "as_of": decision_time - timedelta(minutes=1),
            "known_at": decision_time - timedelta(minutes=1),
        }
    )

    with pytest.raises(ResearchCaseIntegrityError, match="is stale"):
        builder.build(coverage, stale)


def test_a_data_defect_is_reported_as_an_integrity_failure(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    corrupted = coverage.model_copy(update={"provider": "polygon"})

    artifact = builder.build_all(
        [corrupted],
        trading_state_manifest(state),
        lag_minutes=5,
    )

    assert artifact.cases == ()
    assert artifact.failures[0].kind is ResearchCaseFailureKind.INTEGRITY_ERROR


# ---------------------------------------------------------- insights --------


def test_insight_artifact_pins_the_model_triple(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)

    with pytest.raises(ValidationError, match="pinned model"):
        build_insight_artifact(
            cases,
            [insight],
            variant=BacktestVariant.AI,
            model_id="other/model",
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )


def test_insight_artifact_accepts_a_pinned_abstention(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)
    abstention = NewsInsight.abstain(
        event_id=insight.event_id,
        accession_number=insight.accession_number,
        reason="INSUFFICIENT_EVIDENCE",
    )

    artifact = build_insight_artifact(
        cases,
        [abstention],
        variant=BacktestVariant.KEYWORD,
        model_id="keyword/none",
        prompt_version="1",
        schema_version="1",
    )
    run = build_backtest_run(
        cases,
        variant=BacktestVariant.KEYWORD,
        insight_artifact=artifact,
    )

    assert artifact.abstention_event_ids == (insight.event_id,)
    assert artifact.by_case()[cases.case_hashes[0]].status is InsightStatus.ABSTAIN
    assert run.trades == ()
    assert "INSIGHT_ABSTAINED" in run.outcomes[0].reasons


def test_insight_artifact_never_covers_the_quant_only_variant(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)

    with pytest.raises(ValidationError, match="never consumes insights"):
        InsightArtifact(
            variant=BacktestVariant.QUANT_ONLY,
            case_artifact_sha256=cases.artifact_sha256,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )


def test_insight_artifact_must_be_bound_to_the_evaluated_cases(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)
    other = builder.build_all(
        [coverage],
        trading_state_manifest(state),
        lag_minutes=5,
        dataset_manifest_sha256="c" * 64,
    )
    artifact = build_insight_artifact(
        other,
        [insight],
        variant=BacktestVariant.AI,
        model_id=insight.model_id,
        prompt_version=insight.prompt_version,
        schema_version=insight.schema_version,
    )

    with pytest.raises(ValueError, match="bound to different cases"):
        build_backtest_run(cases, variant=BacktestVariant.AI, insight_artifact=artifact)


def test_a_non_quant_run_requires_insights(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, _insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)

    with pytest.raises(ValueError, match="requires insights"):
        build_backtest_run(cases, variant=BacktestVariant.AI)


def test_grounding_rejects_an_insight_for_a_foreign_filing(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)
    foreign = insight.model_copy(update={"accession_number": "0000320193-26-000099"})

    with pytest.raises(InsightArtifactError, match="identity mismatch"):
        build_insight_artifact(
            cases,
            [foreign],
            variant=BacktestVariant.AI,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        )


# ------------------------------------------------------------- gates --------


def test_paired_gate_compares_two_complete_runs(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)
    quant_run = build_backtest_run(cases, variant=BacktestVariant.QUANT_ONLY)
    ai_run = build_backtest_run(
        cases,
        variant=BacktestVariant.AI,
        insight_artifact=build_insight_artifact(
            cases,
            [insight],
            variant=BacktestVariant.AI,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        ),
    )

    result = paired_ai_gate(quant_run, ai_run)

    assert result.baseline_version == quant_run.strategy_version
    assert result.candidate_version == ai_run.strategy_version
    # A single development-period case can never satisfy the registered
    # out-of-sample minimums, so the gate must not pass.
    assert result.passed is False


def test_paired_gate_rejects_swapped_variants(
    tmp_path, filing, long_insight, decision_time
) -> None:
    builder, coverage, insight, state, _lake = _inputs(
        tmp_path, filing, long_insight, decision_time
    )
    cases = builder.build_all([coverage], trading_state_manifest(state), lag_minutes=5)
    quant_run = build_backtest_run(cases, variant=BacktestVariant.QUANT_ONLY)
    ai_run = build_backtest_run(
        cases,
        variant=BacktestVariant.AI,
        insight_artifact=build_insight_artifact(
            cases,
            [insight],
            variant=BacktestVariant.AI,
            model_id=insight.model_id,
            prompt_version=insight.prompt_version,
            schema_version=insight.schema_version,
        ),
    )

    with pytest.raises(ResearchRunGateError, match="baseline must be the quant-only run"):
        paired_ai_gate(ai_run, quant_run)


# ---------------------------------------------------------- direction -------


@pytest.mark.parametrize(
    ("z_score", "last", "vwap", "expected"),
    [
        (2.0, "101.00", "100.00", Direction.LONG),
        (-2.0, "99.00", "100.00", Direction.SHORT),
        (2.0, "99.00", "100.00", Direction.NEUTRAL),
        (-2.0, "101.00", "100.00", Direction.NEUTRAL),
    ],
)
def test_deterministic_direction_needs_price_and_vwap_to_agree(
    snapshot, z_score, last, vwap, expected
) -> None:
    from decimal import Decimal

    market = snapshot.market.model_copy(
        update={
            "beta_adjusted_return_z": z_score,
            "last": Decimal(last),
            "session_vwap": Decimal(vwap),
        }
    )

    assert deterministic_direction(snapshot.model_copy(update={"market": market})) is expected
