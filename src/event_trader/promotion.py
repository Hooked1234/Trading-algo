"""Content-addressed authorization artifact for paper-order promotion.

An in-memory boolean is deliberately insufficient to enable broker submissions.
The artifact binds the decision to one experiment, immutable data/code manifests,
the approved strategy directions, and (when applicable) the AI benchmark and
paired quant-only comparison.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from pydantic import Field, model_validator

from .domain import Direction, FrozenModel

Sha256 = str


class ResearchPromotionArtifact(FrozenModel):
    experiment_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    enabled_directions: tuple[Direction, ...]
    ai_influences_orders: bool
    research_gate_passed: bool
    paired_improvement_passed: bool = False
    model_gate_passed: bool = False
    experiment_manifest_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_manifest_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    code_revision_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    research_result_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    research_evidence_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    paired_result_sha256: Sha256 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    model_result_sha256: Sha256 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    paired_evidence_sha256: Sha256 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    model_evidence_sha256: Sha256 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    model_id: str | None = None
    prompt_version: str | None = None
    schema_version: str | None = None
    created_at: datetime
    artifact_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def create(
        cls,
        *,
        experiment_id: str,
        strategy_version: str,
        enabled_directions: tuple[Direction, ...],
        ai_influences_orders: bool,
        research_gate_passed: bool,
        experiment_manifest_sha256: str,
        dataset_manifest_sha256: str,
        code_revision_sha256: str,
        research_result_sha256: str,
        research_evidence_sha256: str,
        created_at: datetime,
        paired_improvement_passed: bool = False,
        model_gate_passed: bool = False,
        paired_result_sha256: str | None = None,
        model_result_sha256: str | None = None,
        paired_evidence_sha256: str | None = None,
        model_evidence_sha256: str | None = None,
        model_id: str | None = None,
        prompt_version: str | None = None,
        schema_version: str | None = None,
    ) -> ResearchPromotionArtifact:
        timestamp = _utc(created_at)
        payload = {
            "experiment_id": experiment_id,
            "strategy_version": strategy_version,
            "enabled_directions": tuple(enabled_directions),
            "ai_influences_orders": ai_influences_orders,
            "research_gate_passed": research_gate_passed,
            "paired_improvement_passed": paired_improvement_passed,
            "model_gate_passed": model_gate_passed,
            "experiment_manifest_sha256": experiment_manifest_sha256,
            "dataset_manifest_sha256": dataset_manifest_sha256,
            "code_revision_sha256": code_revision_sha256,
            "research_result_sha256": research_result_sha256,
            "research_evidence_sha256": research_evidence_sha256,
            "paired_result_sha256": paired_result_sha256,
            "model_result_sha256": model_result_sha256,
            "paired_evidence_sha256": paired_evidence_sha256,
            "model_evidence_sha256": model_evidence_sha256,
            "model_id": model_id,
            "prompt_version": prompt_version,
            "schema_version": schema_version,
            "created_at": timestamp,
        }
        return cls(**payload, artifact_sha256=_payload_hash(payload))

    @model_validator(mode="after")
    def validate_artifact(self) -> ResearchPromotionArtifact:
        if not self.enabled_directions:
            raise ValueError("promotion requires at least one enabled direction")
        if Direction.NEUTRAL in self.enabled_directions:
            raise ValueError("neutral cannot be promoted")
        if len(set(self.enabled_directions)) != len(self.enabled_directions):
            raise ValueError("enabled directions must be unique")
        ai_fields = (
            self.paired_result_sha256,
            self.model_result_sha256,
            self.paired_evidence_sha256,
            self.model_evidence_sha256,
            self.model_id,
            self.prompt_version,
            self.schema_version,
        )
        if self.ai_influences_orders and any(value is None for value in ai_fields):
            raise ValueError("AI promotion requires paired and model evidence")
        if not self.ai_influences_orders and any(value is not None for value in ai_fields):
            raise ValueError("quant-only promotion cannot carry AI evidence")
        if not self.ai_influences_orders and (
            self.paired_improvement_passed or self.model_gate_passed
        ):
            raise ValueError("quant-only promotion cannot pass AI gates")
        payload = self.model_dump(exclude={"artifact_sha256"})
        if self.artifact_sha256 != _payload_hash(payload):
            raise ValueError("promotion artifact hash does not match its contents")
        return self

    def authorizes(
        self,
        *,
        strategy_version: str,
        direction: Direction,
        ai_influences_orders: bool,
    ) -> bool:
        if direction is Direction.NEUTRAL:
            return False
        if strategy_version != self.strategy_version:
            return False
        if direction not in self.enabled_directions:
            return False
        if ai_influences_orders != self.ai_influences_orders:
            return False
        if not self.research_gate_passed:
            return False
        if self.ai_influences_orders:
            return self.paired_improvement_passed and self.model_gate_passed
        return True

    def authorizes_insight(
        self,
        *,
        model_id: str,
        prompt_version: str,
        schema_version: str,
    ) -> bool:
        """Bind an AI promotion to the exact runtime model/prompt/schema triple."""

        if not self.ai_influences_orders:
            return True
        return (
            model_id == self.model_id
            and prompt_version == self.prompt_version
            and schema_version == self.schema_version
        )


def _payload_hash(payload: dict[str, object]) -> str:
    serializable = dict(payload)
    directions = serializable.get("enabled_directions")
    if directions is not None:
        serializable["enabled_directions"] = [
            value.value if isinstance(value, Direction) else str(value) for value in directions
        ]
    created_at = serializable.get("created_at")
    if isinstance(created_at, datetime):
        serializable["created_at"] = _utc(created_at).isoformat(timespec="microseconds")
    encoded = json.dumps(
        serializable,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return sha256(encoded).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("promotion timestamp must be timezone-aware")
    return value.astimezone(UTC)


def write_promotion_artifact(
    artifact: ResearchPromotionArtifact, path: str | Path
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"promotion artifact is immutable: {target}")
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(artifact.model_dump_json(indent=2))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        os.replace(temporary_path, target)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return target


def load_promotion_artifact(path: str | Path) -> ResearchPromotionArtifact:
    return ResearchPromotionArtifact.model_validate_json(Path(path).read_text(encoding="utf-8"))


__all__ = [
    "ResearchPromotionArtifact",
    "load_promotion_artifact",
    "write_promotion_artifact",
]
