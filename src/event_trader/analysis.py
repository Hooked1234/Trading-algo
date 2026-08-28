"""Stable identity for one filing analysis.

An analysis key binds the event, the exact filing documents, the model input and
the pinned model/prompt/schema triple.  Two runs that agree on all of those are
the same analysis, so a retried event reuses its stored answer instead of paying
for a second model call — and a changed document or prompt is never silently
served from the old one.
"""

from __future__ import annotations

from hashlib import sha256

from pydantic import Field, model_validator

from .artifacts import Sha256, canonical_hash
from .domain import EventSnapshot, FrozenModel

_ANALYSIS_KEY_SCHEMA = "filing-analysis/1"


class AnalysisIdentity(FrozenModel):
    """The pinned model, prompt and schema an insight provider will use."""

    model_id: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    schema_version: str = Field(default="1", min_length=1)


class AnalysisKey(FrozenModel):
    """Content-addressed identity of one filing analysis."""

    event_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    document_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    input_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    model_id: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_identity(self) -> AnalysisKey:
        if not self.model_id.strip() or "/" not in self.model_id:
            raise ValueError("model_id must be formatted as provider/model")
        return self

    @property
    def key(self) -> str:
        return canonical_hash(
            {
                "schema": _ANALYSIS_KEY_SCHEMA,
                "event_id": self.event_id,
                "accession_number": self.accession_number,
                "document_sha256": self.document_sha256,
                "input_sha256": self.input_sha256,
                "model_id": self.model_id,
                "prompt_version": self.prompt_version,
                "schema_version": self.schema_version,
            }
        )

    @classmethod
    def for_snapshot(
        cls,
        snapshot: EventSnapshot,
        identity: AnalysisIdentity,
    ) -> AnalysisKey:
        return cls(
            event_id=snapshot.filing.event_id,
            accession_number=snapshot.filing.accession_number,
            document_sha256=document_set_sha256(snapshot),
            input_sha256=model_input_sha256(snapshot),
            model_id=identity.model_id,
            prompt_version=identity.prompt_version,
            schema_version=identity.schema_version,
        )


def document_set_sha256(snapshot: EventSnapshot) -> str:
    """Content address of the filing's document set, independent of order."""

    return canonical_hash(sorted(document.sha256 for document in snapshot.filing.documents))


def model_input_sha256(snapshot: EventSnapshot) -> str:
    """Content address of the text a provider would be asked to analyse."""

    return sha256(snapshot.document_text.encode("utf-8")).hexdigest()


__all__ = [
    "AnalysisIdentity",
    "AnalysisKey",
    "document_set_sha256",
    "model_input_sha256",
]
