"""Deterministic pre-selection that runs before any model is asked anything.

The gate uses the same price/volume rules as the quant-only comparator, so a
candidate that can never become an order never costs a model call.  It is
evaluated twice per event: once on the ingestion snapshot and again on a fresh
snapshot after the model latency has elapsed.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import AwareDatetime, Field, model_validator

from .artifacts import Sha256, canonical_hash
from .calendar import NyseSessionCalendar
from .domain import Direction, EventSnapshot, FrozenModel
from .strategy import ContinuationStrategy, QuantOnlyContinuationStrategy, deterministic_direction

_SNAPSHOT_HASH_SCHEMA = "candidate-snapshot/1"


class CandidateDecision(FrozenModel):
    """One auditable pre-selection result for a filing at a point in time."""

    event_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    symbol: str = Field(min_length=1)
    accepted: bool
    direction: Direction
    reason_codes: tuple[str, ...] = ()
    evaluated_at: AwareDatetime
    snapshot_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_decision(self) -> CandidateDecision:
        expected = not self.reason_codes and self.direction is not Direction.NEUTRAL
        if self.accepted != expected:
            raise ValueError("candidate acceptance must follow from its reason codes")
        if self.accepted and self.direction is Direction.NEUTRAL:
            raise ValueError("an accepted candidate requires a resolved direction")
        return self


class CandidateGate:
    """Reject an event on price, volume and session evidence alone."""

    def __init__(
        self,
        *,
        strategy: ContinuationStrategy | None = None,
        calendar: NyseSessionCalendar | None = None,
    ) -> None:
        self._calendar = calendar or NyseSessionCalendar()
        self._strategy = strategy or QuantOnlyContinuationStrategy(self._calendar)

    def evaluate(self, snapshot: EventSnapshot, now: datetime) -> CandidateDecision:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("candidate decision time must be timezone-aware")
        direction = deterministic_direction(snapshot)
        reasons: list[str] = []
        if direction is Direction.NEUTRAL:
            # Price reaction and VWAP disagree, so no side is confirmed.
            reasons.append("DIRECTION_UNCONFIRMED")
        reasons.extend(self._strategy.quant_rejection_reasons(snapshot, direction, now))
        unique = tuple(dict.fromkeys(reasons))
        return CandidateDecision(
            event_id=snapshot.filing.event_id,
            accession_number=snapshot.filing.accession_number,
            symbol=snapshot.market.symbol,
            accepted=not unique and direction is not Direction.NEUTRAL,
            direction=direction,
            reason_codes=unique,
            evaluated_at=now,
            snapshot_sha256=snapshot_sha256(snapshot),
        )


def snapshot_sha256(snapshot: EventSnapshot) -> str:
    """Content address of the facts a candidate decision was based on."""

    filing = snapshot.filing
    payload = {
        "schema": _SNAPSHOT_HASH_SCHEMA,
        "event_id": filing.event_id,
        "accession_number": filing.accession_number,
        "form": filing.form,
        "items": list(filing.items),
        "documents": sorted(document.sha256 for document in filing.documents),
        "market": snapshot.market.model_dump(mode="json"),
    }
    return canonical_hash(payload)


__all__ = ["CandidateDecision", "CandidateGate", "snapshot_sha256"]
