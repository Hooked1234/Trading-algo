"""Pinned insight evidence bound to an immutable set of research cases.

An insight artifact is produced once per variant and never regenerated inside a
backtest: every preselected candidate carries exactly one model answer or one
explicit abstention, so a rerun cannot quietly resample the model.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from pydantic import Field, model_validator

from .artifacts import HashedArtifact, Sha256
from .backtest import BacktestCase, BacktestVariant
from .documents import evidence_excerpt_occurs, verified_document_texts
from .domain import FrozenModel, InsightStatus, NewsInsight
from .research_cases import ResearchCaseBuildArtifact
from .strategy import QuantOnlyContinuationStrategy


class InsightArtifactError(ValueError):
    """An insight artifact does not completely and faithfully cover its cases."""


class InsightRecord(FrozenModel):
    case_input_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    event_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    insight: NewsInsight

    @model_validator(mode="after")
    def validate_identity(self) -> InsightRecord:
        if self.insight.event_id != self.event_id:
            raise ValueError("insight record and insight event identity must match")
        if self.insight.accession_number != self.accession_number:
            raise ValueError("insight record and insight accession must match")
        return self


class InsightArtifact(HashedArtifact):
    """One pinned answer or abstention per preselected candidate."""

    artifact_version: Literal["1"] = "1"
    variant: BacktestVariant
    case_artifact_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    model_id: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    candidate_case_hashes: tuple[str, ...] = ()
    records: tuple[InsightRecord, ...] = ()
    artifact_sha256: Sha256 = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_records(self) -> InsightArtifact:
        if self.variant is BacktestVariant.QUANT_ONLY:
            raise ValueError("the quant-only variant never consumes insights")
        if len(set(self.candidate_case_hashes)) != len(self.candidate_case_hashes):
            raise ValueError("insight candidates must be unique")
        record_hashes = tuple(record.case_input_sha256 for record in self.records)
        if len(set(record_hashes)) != len(record_hashes):
            raise ValueError("an insight artifact requires one record per case")
        if set(record_hashes) != set(self.candidate_case_hashes):
            raise ValueError("every preselected candidate requires exactly one insight record")
        for record in self.records:
            insight = record.insight
            if insight.status is InsightStatus.ABSTAIN:
                continue
            if insight.model_id != self.model_id:
                raise ValueError("actionable insights must come from the pinned model")
            if insight.prompt_version != self.prompt_version:
                raise ValueError("actionable insights must come from the pinned prompt")
            if insight.schema_version != self.schema_version:
                raise ValueError("actionable insights must come from the pinned schema")
        return self

    def by_case(self) -> dict[str, NewsInsight]:
        return {record.case_input_sha256: record.insight for record in self.records}

    @property
    def abstention_event_ids(self) -> tuple[str, ...]:
        return tuple(
            record.event_id
            for record in self.records
            if record.insight.status is InsightStatus.ABSTAIN
        )


def select_insight_candidates(
    artifact: ResearchCaseBuildArtifact,
    *,
    strategy: QuantOnlyContinuationStrategy | None = None,
) -> tuple[BacktestCase, ...]:
    """Preselect the cases a model may see, using price/volume evidence only.

    A case that the deterministic gate already rejects can never become a trade,
    so it never reaches a model.  This mirrors the live candidate gate and keeps
    the paired comparison honest: both variants see the same case universe.
    """

    gate = strategy or QuantOnlyContinuationStrategy()
    selected = tuple(
        case
        for case in artifact.cases
        if not gate.rejection_reasons(case.snapshot, None, case.decision_time)
    )
    return selected


def build_insight_artifact(
    artifact: ResearchCaseBuildArtifact,
    insights: Iterable[NewsInsight],
    *,
    variant: BacktestVariant,
    model_id: str,
    prompt_version: str,
    schema_version: str,
    candidates: Iterable[BacktestCase] | None = None,
) -> InsightArtifact:
    """Bind pinned insights to their cases and re-verify their grounding."""

    artifact.verify()
    selected = tuple(candidates) if candidates is not None else select_insight_candidates(artifact)
    cases_by_hash = {
        case.case_input_sha256: case for case in selected if case.case_input_sha256 is not None
    }
    if len(cases_by_hash) != len(selected):
        raise InsightArtifactError("every insight candidate requires complete case lineage")
    by_event: dict[str, NewsInsight] = {}
    for insight in insights:
        if insight.event_id in by_event:
            raise InsightArtifactError(
                f"duplicate insight for event {insight.event_id}"
            )
        by_event[insight.event_id] = insight

    records: list[InsightRecord] = []
    for case_hash, case in sorted(cases_by_hash.items()):
        filing = case.snapshot.filing
        insight = by_event.pop(filing.event_id, None)
        if insight is None:
            raise InsightArtifactError(
                f"no pinned insight or abstention for event {filing.event_id}"
            )
        _validate_grounding(case, insight)
        records.append(
            InsightRecord(
                case_input_sha256=case_hash,
                event_id=filing.event_id,
                accession_number=filing.accession_number,
                insight=insight,
            )
        )
    if by_event:
        raise InsightArtifactError(
            "insight artifact contains answers for events that are not candidates"
        )
    built = InsightArtifact(
        variant=variant,
        case_artifact_sha256=artifact.artifact_sha256,
        model_id=model_id,
        prompt_version=prompt_version,
        schema_version=schema_version,
        candidate_case_hashes=tuple(sorted(cases_by_hash)),
        records=tuple(records),
    )
    return built.sealed()


def _validate_grounding(case: BacktestCase, insight: NewsInsight) -> None:
    filing = case.snapshot.filing
    if (
        insight.event_id != filing.event_id
        or insight.accession_number != filing.accession_number
    ):
        raise InsightArtifactError("insight and filing identity mismatch")
    if insight.status is InsightStatus.ABSTAIN:
        # An abstention deliberately carries no evidence to ground.
        return
    document_texts = verified_document_texts(filing, case.snapshot.document_text)
    if not document_texts:
        raise InsightArtifactError("filing document boundaries are unverifiable")
    for span in insight.evidence:
        document = document_texts.get(span.document_sha256)
        if document is None or not evidence_excerpt_occurs(document, span.excerpt):
            raise InsightArtifactError("insight evidence is not grounded in the filing")


__all__ = [
    "InsightArtifact",
    "InsightArtifactError",
    "InsightRecord",
    "build_insight_artifact",
    "select_insight_candidates",
]
