from datetime import timedelta

import pytest
from pydantic import ValidationError

from event_trader.domain import Direction
from event_trader.promotion import (
    ResearchPromotionArtifact,
    load_promotion_artifact,
    write_promotion_artifact,
)


def _artifact(decision_time, **updates):
    values = {
        "experiment_id": "sec-8k-v1-holdout",
        "strategy_version": "sec-8k-continuation-v1",
        "enabled_directions": (Direction.LONG, Direction.SHORT),
        "ai_influences_orders": True,
        "research_gate_passed": True,
        "paired_improvement_passed": True,
        "model_gate_passed": True,
        "experiment_manifest_sha256": "a" * 64,
        "dataset_manifest_sha256": "b" * 64,
        "code_revision_sha256": "c" * 64,
        "research_result_sha256": "d" * 64,
        "research_evidence_sha256": "0" * 64,
        "paired_result_sha256": "e" * 64,
        "model_result_sha256": "f" * 64,
        "paired_evidence_sha256": "1" * 64,
        "model_evidence_sha256": "2" * 64,
        "model_id": "provider/model-version",
        "prompt_version": "prompt-v1",
        "schema_version": "1",
        "created_at": decision_time,
    }
    values.update(updates)
    return ResearchPromotionArtifact.create(**values)


def test_hashed_promotion_artifact_authorizes_only_bound_strategy(decision_time) -> None:
    artifact = _artifact(decision_time)

    assert artifact.authorizes(
        strategy_version="sec-8k-continuation-v1",
        direction=Direction.LONG,
        ai_influences_orders=True,
    )
    assert not artifact.authorizes(
        strategy_version="another-version",
        direction=Direction.LONG,
        ai_influences_orders=True,
    )
    assert artifact.authorizes_insight(
        model_id="provider/model-version",
        prompt_version="prompt-v1",
        schema_version="1",
    )
    assert not artifact.authorizes_insight(
        model_id="provider/model-version",
        prompt_version="unbenchmarked-v2",
        schema_version="1",
    )


def test_tampered_promotion_artifact_is_rejected(decision_time) -> None:
    artifact = _artifact(decision_time)
    payload = artifact.model_dump()
    payload["created_at"] = decision_time + timedelta(seconds=1)

    with pytest.raises(ValidationError, match="hash does not match"):
        ResearchPromotionArtifact.model_validate(payload)


def test_ai_promotion_requires_model_and_paired_evidence(decision_time) -> None:
    with pytest.raises(ValidationError, match="AI promotion requires"):
        _artifact(decision_time, model_result_sha256=None)


def test_failed_gate_never_authorizes(decision_time) -> None:
    artifact = _artifact(decision_time, research_gate_passed=False)

    assert not artifact.authorizes(
        strategy_version=artifact.strategy_version,
        direction=Direction.SHORT,
        ai_influences_orders=True,
    )


def test_promotion_artifact_is_written_immutably_and_revalidated(tmp_path, decision_time) -> None:
    artifact = _artifact(decision_time)
    target = write_promotion_artifact(artifact, tmp_path / "promotion.json")

    assert load_promotion_artifact(target) == artifact
    with pytest.raises(FileExistsError, match="immutable"):
        write_promotion_artifact(artifact, target)
