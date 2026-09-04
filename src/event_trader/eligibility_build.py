"""Derive the point-in-time eligibility manifest from filing cover pages.

The manifest that :mod:`event_trader.eligibility` consumes has to be built from
evidence that was public no later than the filing it classifies.  The cover page
of the filing itself is exactly such a source: since the 2019 cover-page tagging
phase-in, an 8-K carries the registered security title, trading symbol and
exchange for every class registered under Section 12(b), and that evidence is
knowable at the moment the filing is accepted.

Two of the three confirmations follow from it.  ``corporate_actions_complete``
does not: nothing on a cover page establishes that the corporate-action history
of a symbol is complete.  This module therefore leaves that column unset rather
than inventing it, which keeps the affected coverage records an explicit gap
instead of a silent pass.
"""

from __future__ import annotations

import asyncio
import csv
import inspect
import re
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from itertools import groupby
from pathlib import Path

from pydantic import AwareDatetime, Field, ValidationError

from .backfill import SecIndexEntry, parse_sec_quarter_index, plan_sec_quarter_indexes
from .domain import FrozenModel
from .eligibility import EligibilityInterval
from .providers.sec_history import CoverPageSecurity, parse_historical_submission

Fetcher = Callable[[str], bytes | Awaitable[bytes]]

MANIFEST_HEADER = (
    "cik",
    "symbol",
    "valid_from",
    "valid_through",
    "known_at",
    "common_stock",
    "us_listing",
    "corporate_actions_complete",
    "source",
)
SOURCE_PREFIX = "sec-cover-page-dei"

_NON_COMMON_RE = re.compile(
    r"\b(?:warrants?|units?|rights?|preferred|preference|depositary|depository|notes?"
    r"|debentures?|bonds?|subordinated|contingent\s+value|purchase\s+contracts?)\b",
    flags=re.IGNORECASE,
)
_COMMON_RE = re.compile(
    r"\b(?:common\s+stock|common\s+shares?|ordinary\s+shares?"
    r"|shares?\s+of\s+beneficial\s+interest)\b",
    flags=re.IGNORECASE,
)
_US_EXCHANGE_RE = re.compile(
    r"\b(?:nyse|new\s+york\s+stock\s+exchange|nasdaq|cboe|bzx|batz"
    r"|iex|investors\s+exchange|nyse\s+american|nyse\s+arca|ltse"
    r"|long[-\s]term\s+stock\s+exchange|memx|members\s+exchange)\b",
    flags=re.IGNORECASE,
)


class CoverPageSecurityRecord(FrozenModel):
    """One registered security as a filing reported it, in serializable form."""

    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")
    security_title: str | None = None
    exchange: str | None = None

    @classmethod
    def from_security(cls, security: CoverPageSecurity) -> CoverPageSecurityRecord:
        return cls(
            symbol=security.symbol,
            security_title=security.security_title,
            exchange=security.exchange,
        )


class CoverPageFactRecord(FrozenModel):
    """Durable cover-page evidence of exactly one filing."""

    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    cik: str = Field(pattern=r"^\d{1,10}$")
    form: str = Field(min_length=1)
    filed_on: date
    accepted_at: AwareDatetime
    securities: tuple[CoverPageSecurityRecord, ...] = ()


class CoverPageObservation(FrozenModel):
    """One symbol's classification as a single filing established it."""

    cik: str = Field(pattern=r"^\d{1,10}$")
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    observed_on: date
    known_at: AwareDatetime
    common_stock: bool | None = None
    us_listing: bool | None = None


class CoverPageCollectionSummary(FrozenModel):
    """Outcome of one collector pass over the requested quarters."""

    quarters: int = Field(ge=0)
    discovered: int = Field(ge=0)
    already_collected: int = Field(ge=0)
    collected: int = Field(ge=0)
    without_securities: int = Field(ge=0)
    failed: int = Field(ge=0)
    failure_samples: tuple[str, ...] = ()


class EligibilityBuildSummary(FrozenModel):
    """Explicit accounting of what the derived manifest does and does not confirm."""

    filings: int = Field(ge=0)
    filings_without_securities: int = Field(ge=0)
    observations: int = Field(ge=0)
    symbols: int = Field(ge=0)
    intervals: int = Field(ge=0)
    common_stock_true: int = Field(ge=0)
    common_stock_false: int = Field(ge=0)
    common_stock_unknown: int = Field(ge=0)
    us_listing_true: int = Field(ge=0)
    us_listing_unknown: int = Field(ge=0)
    corporate_actions_unknown: int = Field(ge=0)


class CoverPageFactCollector:
    """Walk the official SEC quarterly indexes and record cover-page evidence.

    Only the submission itself is fetched, never the exhibits the market
    backfill hydrates, so this pass is one request per filing.  Every result is
    appended durably before the next filing is requested, which makes an
    interrupted multi-hour run resumable without re-fetching what it already
    has.  A filing that fails is deliberately not recorded, so a later run
    retries it instead of freezing a transport error into the manifest.
    """

    def __init__(
        self,
        *,
        index_fetcher: Fetcher,
        submission_fetcher: Fetcher,
        index_kind: str = "master",
        failure_sample_size: int = 5,
    ) -> None:
        self._index_fetcher = index_fetcher
        self._submission_fetcher = submission_fetcher
        self._index_kind = index_kind
        self._failure_sample_size = failure_sample_size

    async def run(
        self,
        *,
        start: date,
        end: date,
        output: str | Path,
    ) -> CoverPageCollectionSummary:
        target = Path(output)
        existing = await asyncio.to_thread(read_fact_records, target)
        collected_before = {record.accession_number for record in existing}
        plans = plan_sec_quarter_indexes(start, end)
        discovered = 0
        collected = 0
        without_securities = 0
        skipped = 0
        failures: list[str] = []

        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        for plan in plans:
            payload = await _maybe_await(self._index_fetcher(plan.url_for(self._index_kind)))
            entries = _unique_entries(
                entry
                for entry in parse_sec_quarter_index(payload, kind=self._index_kind)
                if plan.start <= entry.filed_on <= plan.end
            )
            discovered += len(entries)
            for entry in entries:
                if entry.accession_number in collected_before:
                    skipped += 1
                    continue
                try:
                    record = await self._collect(entry)
                except Exception as exc:
                    if len(failures) < self._failure_sample_size:
                        failures.append(f"{entry.accession_number}: {exc.__class__.__name__}")
                    continue
                await asyncio.to_thread(_append_record, target, record)
                collected_before.add(record.accession_number)
                collected += 1
                without_securities += 0 if record.securities else 1
        return CoverPageCollectionSummary(
            quarters=len(plans),
            discovered=discovered,
            already_collected=skipped,
            collected=collected,
            without_securities=without_securities,
            failed=discovered - skipped - collected,
            failure_samples=tuple(failures),
        )

    async def _collect(self, entry: SecIndexEntry) -> CoverPageFactRecord:
        payload = await _maybe_await(self._submission_fetcher(entry.archive_url))
        metadata = parse_historical_submission(payload)
        return CoverPageFactRecord(
            accession_number=entry.accession_number,
            cik=_normalize_cik(entry.cik),
            form=entry.form,
            filed_on=entry.filed_on,
            accepted_at=metadata.accepted_at,
            securities=tuple(
                CoverPageSecurityRecord.from_security(security) for security in metadata.securities
            ),
        )


def read_fact_records(path: str | Path) -> tuple[CoverPageFactRecord, ...]:
    """Read collected evidence and leave the file safe to append to.

    An interrupted run can leave the final line torn, and it can equally leave a
    complete record without its terminating newline.  Both are repaired here,
    before the collector appends anything: the torn line is dropped, and a valid
    but unterminated final record is terminated.  Without that second repair the
    next append would concatenate onto the last record and destroy two records
    at once.  A malformed line anywhere but at the end is corruption and is
    refused rather than silently discarded.
    """

    target = Path(path)
    if not target.is_file():
        return ()
    raw = target.read_text(encoding="utf-8")
    lines = [line for line in raw.splitlines() if line.strip()]
    records: list[CoverPageFactRecord] = []
    torn = False
    for index, line in enumerate(lines, 1):
        try:
            records.append(CoverPageFactRecord.model_validate_json(line))
        except ValidationError as exc:
            if index != len(lines):
                raise ValueError(f"cover-page fact file is corrupt at line {index}") from exc
            torn = True
    if torn or (raw and not raw.endswith("\n")):
        target.write_text(
            "".join(f"{item.model_dump_json()}\n" for item in records),
            encoding="utf-8",
            newline="\n",
        )
    return tuple(records)


def classify_common_stock(security_title: str | None) -> bool | None:
    """Classify a registered security title as common equity.

    Non-common markers are tested first: a unit made of one common share and
    half a warrant names common stock in its own title, and treating it as
    common equity would put a derivative into the research sample.
    """

    if security_title is None or not security_title.strip():
        return None
    if _NON_COMMON_RE.search(security_title):
        return False
    if _COMMON_RE.search(security_title):
        return True
    return None


def classify_us_listing(exchange: str | None) -> bool | None:
    """Confirm a US national securities exchange named on the cover page.

    Only a recognized exchange confirms the listing.  An unrecognized or absent
    value stays unknown instead of becoming a negative claim, because the value
    is free text and a spelling this list does not know is not evidence of a
    foreign listing.
    """

    if exchange is None or not exchange.strip():
        return None
    return True if _US_EXCHANGE_RE.search(exchange) else None


def observations_from_record(record: CoverPageFactRecord) -> tuple[CoverPageObservation, ...]:
    """Turn one filing's cover-page evidence into per-symbol observations."""

    return tuple(
        CoverPageObservation(
            cik=_normalize_cik(record.cik),
            symbol=security.symbol,
            accession_number=record.accession_number,
            observed_on=record.accepted_at.astimezone(UTC).date(),
            known_at=record.accepted_at,
            common_stock=classify_common_stock(security.security_title),
            us_listing=classify_us_listing(security.exchange),
        )
        for security in record.securities
    )


def build_eligibility_intervals(
    observations: Iterable[CoverPageObservation],
) -> tuple[EligibilityInterval, ...]:
    """Compress observations into non-overlapping intervals per CIK and symbol.

    An interval runs from the day its evidence was first reported until the day
    before the next filing reported something different.  Only the newest
    interval of a symbol stays open-ended.
    """

    grouped: dict[tuple[str, str], list[CoverPageObservation]] = defaultdict(list)
    for observation in observations:
        grouped[(observation.cik, observation.symbol)].append(observation)

    intervals: list[EligibilityInterval] = []
    for (cik, symbol), group in sorted(grouped.items(), key=lambda item: _group_order(item[0])):
        daily = _collapse_daily(group)
        runs = _runs(daily)
        for index, run in enumerate(runs):
            following = runs[index + 1][0] if index + 1 < len(runs) else None
            intervals.append(
                EligibilityInterval(
                    cik=cik,
                    symbol=symbol,
                    valid_from=run[0],
                    valid_through=(following - timedelta(days=1)) if following else None,
                    known_at=run[1],
                    common_stock=run[2],
                    us_listing=run[3],
                    corporate_actions_complete=None,
                    source=f"{SOURCE_PREFIX}:{run[4]}",
                )
            )
    return tuple(intervals)


def write_eligibility_manifest(
    intervals: Sequence[EligibilityInterval],
    path: str | Path,
) -> Path:
    """Write the manifest exclusively; an existing target is an error."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(MANIFEST_HEADER)
        for interval in intervals:
            writer.writerow(
                (
                    interval.cik,
                    interval.symbol,
                    interval.valid_from.isoformat(),
                    interval.valid_through.isoformat() if interval.valid_through else "",
                    interval.known_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                    _csv_bool(interval.common_stock),
                    _csv_bool(interval.us_listing),
                    _csv_bool(interval.corporate_actions_complete),
                    interval.source,
                )
            )
    return target


def summarize(
    records: Sequence[CoverPageFactRecord],
    observations: Sequence[CoverPageObservation],
    intervals: Sequence[EligibilityInterval],
) -> EligibilityBuildSummary:
    """Report what the derivation confirmed, refuted and left unknown."""

    return EligibilityBuildSummary(
        filings=len(records),
        filings_without_securities=sum(1 for record in records if not record.securities),
        observations=len(observations),
        symbols=len({(observation.cik, observation.symbol) for observation in observations}),
        intervals=len(intervals),
        common_stock_true=sum(1 for item in intervals if item.common_stock is True),
        common_stock_false=sum(1 for item in intervals if item.common_stock is False),
        common_stock_unknown=sum(1 for item in intervals if item.common_stock is None),
        us_listing_true=sum(1 for item in intervals if item.us_listing is True),
        us_listing_unknown=sum(1 for item in intervals if item.us_listing is None),
        corporate_actions_unknown=sum(
            1 for item in intervals if item.corporate_actions_complete is None
        ),
    )


def _collapse_daily(
    group: Sequence[CoverPageObservation],
) -> tuple[tuple[date, datetime, bool | None, bool | None, str], ...]:
    """Reduce a symbol's observations to one classification per calendar day.

    Two filings of the same day that disagree about a fact leave that fact
    unknown for the day.  The earliest acceptance of the day carries the
    ``known_at``, so every filing of that day can resolve against it.
    """

    per_day: dict[date, list[CoverPageObservation]] = defaultdict(list)
    for observation in group:
        per_day[observation.observed_on].append(observation)

    collapsed: list[tuple[date, datetime, bool | None, bool | None, str]] = []
    for day in sorted(per_day):
        items = sorted(per_day[day], key=lambda item: (item.known_at, item.accession_number))
        collapsed.append(
            (
                day,
                items[0].known_at,
                _agree(item.common_stock for item in items),
                _agree(item.us_listing for item in items),
                items[0].accession_number,
            )
        )
    return tuple(collapsed)


def _runs(
    daily: Sequence[tuple[date, datetime, bool | None, bool | None, str]],
) -> tuple[tuple[date, datetime, bool | None, bool | None, str], ...]:
    """Keep only the first day of every unchanged classification run."""

    return tuple(next(items) for _key, items in groupby(daily, key=lambda item: item[2:4]))


async def _maybe_await(value: bytes | Awaitable[bytes]) -> bytes:
    return await value if inspect.isawaitable(value) else value


def _append_record(target: Path, record: CoverPageFactRecord) -> None:
    """Append one durable line so an interrupted run resumes without a refetch."""

    with target.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(record.model_dump_json() + "\n")


def _unique_entries(entries: Iterable[SecIndexEntry]) -> tuple[SecIndexEntry, ...]:
    seen: dict[str, SecIndexEntry] = {}
    for entry in entries:
        seen.setdefault(entry.accession_number, entry)
    return tuple(seen.values())


def _agree(values: Iterable[bool | None]) -> bool | None:
    distinct = set(values)
    return distinct.pop() if len(distinct) == 1 else None


def _group_order(key: tuple[str, str]) -> tuple[int, str]:
    return int(key[0]), key[1]


def _normalize_cik(cik: str) -> str:
    return cik.lstrip("0") or "0"


def _csv_bool(value: bool | None) -> str:
    return "" if value is None else str(value).lower()


__all__ = [
    "MANIFEST_HEADER",
    "CoverPageCollectionSummary",
    "CoverPageFactCollector",
    "CoverPageFactRecord",
    "CoverPageObservation",
    "CoverPageSecurityRecord",
    "EligibilityBuildSummary",
    "build_eligibility_intervals",
    "classify_common_stock",
    "classify_us_listing",
    "observations_from_record",
    "read_fact_records",
    "summarize",
    "write_eligibility_manifest",
]
