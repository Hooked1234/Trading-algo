"""Point-in-time security eligibility loaded from a versioned local manifest."""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Mapping
from datetime import UTC, date, datetime
from itertools import pairwise
from pathlib import Path

from pydantic import AwareDatetime, Field, model_validator

from .backfill import PointInTimeEligibility
from .domain import FilingEvent, FrozenModel


class EligibilityManifestError(ValueError):
    """The manifest cannot establish deterministic point-in-time eligibility."""


class EligibilityInterval(FrozenModel):
    cik: str = Field(pattern=r"^\d{1,10}$")
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")
    valid_from: date
    valid_through: date | None = None
    known_at: AwareDatetime
    common_stock: bool | None = None
    us_listing: bool | None = None
    corporate_actions_complete: bool | None = None
    source: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_interval(self) -> EligibilityInterval:
        if self.valid_through is not None and self.valid_through < self.valid_from:
            raise ValueError("eligibility valid_through cannot precede valid_from")
        return self

    def applies(self, event: FilingEvent, symbol: str) -> bool:
        event_date = event.accepted_at.date()
        return (
            self.cik.lstrip("0") == event.cik.lstrip("0")
            and self.symbol == symbol
            and self.valid_from <= event_date
            and (self.valid_through is None or event_date <= self.valid_through)
            and self.known_at <= event.accepted_at
        )


class CsvEligibilityResolver:
    """Resolve historical eligibility without consulting a present-day universe."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        raw = self.path.read_bytes()
        self.manifest_sha256 = hashlib.sha256(raw).hexdigest()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise EligibilityManifestError("eligibility manifest must be UTF-8") from exc
        reader = csv.DictReader(text.splitlines())
        expected = {
            "cik",
            "symbol",
            "valid_from",
            "valid_through",
            "known_at",
            "common_stock",
            "us_listing",
            "corporate_actions_complete",
            "source",
        }
        if reader.fieldnames is None or set(reader.fieldnames) != expected:
            raise EligibilityManifestError(
                "eligibility manifest has an unexpected header; use the reference template"
            )
        self._records = tuple(
            self._parse_row(row, line=index) for index, row in enumerate(reader, 2)
        )
        self._validate_non_overlapping()

    def __call__(self, event: FilingEvent, symbol: str) -> PointInTimeEligibility | None:
        normalized = symbol.strip().upper()
        matches = tuple(record for record in self._records if record.applies(event, normalized))
        if not matches:
            return None
        if len(matches) != 1:
            raise EligibilityManifestError(
                f"ambiguous eligibility for {event.accession_number}/{normalized}"
            )
        record = matches[0]
        return PointInTimeEligibility(
            accession_number=event.accession_number,
            symbol=normalized,
            as_of=record.known_at,
            source=f"{record.source};manifest_sha256={self.manifest_sha256}",
            common_stock=record.common_stock,
            us_listing=record.us_listing,
            corporate_actions_complete=record.corporate_actions_complete,
            detail=(
                f"valid={record.valid_from.isoformat()}.."
                f"{record.valid_through.isoformat() if record.valid_through else 'open'}"
            ),
        )

    def _validate_non_overlapping(self) -> None:
        grouped: dict[tuple[str, str], list[EligibilityInterval]] = {}
        for record in self._records:
            grouped.setdefault((record.cik.lstrip("0"), record.symbol), []).append(record)
        for key, records in grouped.items():
            ordered = sorted(records, key=lambda record: record.valid_from)
            for previous, current in pairwise(ordered):
                if previous.valid_through is None or current.valid_from <= previous.valid_through:
                    raise EligibilityManifestError(
                        f"overlapping eligibility intervals for CIK {key[0]} symbol {key[1]}"
                    )

    @staticmethod
    def _parse_row(row: Mapping[str, str | None], *, line: int) -> EligibilityInterval:
        try:
            known_at = datetime.fromisoformat(_required(row, "known_at").replace("Z", "+00:00"))
            if known_at.tzinfo is None or known_at.utcoffset() is None:
                raise ValueError("known_at must be timezone-aware")
            valid_through = _optional(row, "valid_through")
            return EligibilityInterval(
                cik=_required(row, "cik").lstrip("0") or "0",
                symbol=_required(row, "symbol").upper(),
                valid_from=date.fromisoformat(_required(row, "valid_from")),
                valid_through=date.fromisoformat(valid_through) if valid_through else None,
                known_at=known_at.astimezone(UTC),
                common_stock=_optional_bool(row, "common_stock"),
                us_listing=_optional_bool(row, "us_listing"),
                corporate_actions_complete=_optional_bool(
                    row, "corporate_actions_complete"
                ),
                source=_required(row, "source"),
            )
        except (TypeError, ValueError) as exc:
            raise EligibilityManifestError(
                f"invalid eligibility manifest row at line {line}: {exc}"
            ) from exc


def _required(row: Mapping[str, str | None], field: str) -> str:
    value = row.get(field)
    if value is None or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _optional(row: Mapping[str, str | None], field: str) -> str | None:
    value = row.get(field)
    if value is None or not value.strip():
        return None
    return value.strip()


def _optional_bool(row: Mapping[str, str | None], field: str) -> bool | None:
    value = _optional(row, field)
    if value is None or value.lower() == "unknown":
        return None
    normalized = value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{field} must be true, false, unknown or blank")


__all__ = [
    "CsvEligibilityResolver",
    "EligibilityInterval",
    "EligibilityManifestError",
]
