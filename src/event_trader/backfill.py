"""Resumable point-in-time backfill for historical SEC 8-K filings.

The quarterly SEC indexes enumerate filings but deliberately do not provide a
security master.  A caller therefore injects a point-in-time ``FilingEvent``
resolver.  The runner only uses symbols present on that historical event; it
never consults a current index constituent list or silently manufactures a
return when coverage is missing.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import re
import uuid
from bisect import bisect_right
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import exchange_calendars as xcals
from pydantic import AwareDatetime, Field, model_validator

from event_trader.calendar import NyseSessionCalendar
from event_trader.datasets import ParquetMarketDataLake
from event_trader.domain import Bar, FilingEvent, FrozenModel, Quote
from event_trader.providers.market import MarketDataProvider

BACKFILL_START = date(2019, 1, 1)
BACKFILL_END = date(2026, 6, 30)
SEC_ARCHIVES_ROOT = "https://www.sec.gov/Archives"
SEC_FULL_INDEX_ROOT = f"{SEC_ARCHIVES_ROOT}/edgar/full-index"
IndexKind = Literal["master", "form"]


class BackfillError(RuntimeError):
    """Base class for historical-backfill failures."""


class SecIndexParseError(BackfillError):
    """A quarterly SEC index could not be parsed safely."""


class BackfillStateError(BackfillError):
    """Persisted checkpoint state is incompatible or corrupt."""


class BackfillMarketDataError(BackfillError):
    """A retryable market-data operation failed."""


@dataclass(frozen=True, slots=True, order=True)
class SecQuarter:
    """One calendar quarter used by the SEC full-index archive."""

    year: int
    quarter: int

    def __post_init__(self) -> None:
        if self.year < 1994:
            raise ValueError("SEC quarterly full-index years must be 1994 or later")
        if self.quarter not in {1, 2, 3, 4}:
            raise ValueError("quarter must be between 1 and 4")

    @property
    def key(self) -> str:
        return f"{self.year:04d}-Q{self.quarter}"

    @property
    def start(self) -> date:
        return date(self.year, ((self.quarter - 1) * 3) + 1, 1)

    @property
    def end(self) -> date:
        if self.quarter == 4:
            following = date(self.year + 1, 1, 1)
        else:
            following = date(self.year, (self.quarter * 3) + 1, 1)
        return following - timedelta(days=1)


@dataclass(frozen=True, slots=True)
class SecQuarterIndexPlan:
    """Inclusive requested slice and both official index URLs for a quarter."""

    quarter: SecQuarter
    start: date
    end: date
    master_url: str
    form_url: str

    def url_for(self, kind: IndexKind) -> str:
        if kind == "master":
            return self.master_url
        if kind == "form":
            return self.form_url
        raise ValueError(f"unsupported SEC index kind: {kind!r}")


def plan_sec_quarter_indexes(
    start: date = BACKFILL_START,
    end: date = BACKFILL_END,
) -> tuple[SecQuarterIndexPlan, ...]:
    """Split an inclusive date range into deterministic SEC-quarter requests."""

    if end < start:
        raise ValueError("backfill end must be on or after start")
    first_quarter = ((start.month - 1) // 3) + 1
    current = SecQuarter(start.year, first_quarter)
    plans: list[SecQuarterIndexPlan] = []
    while current.start <= end:
        base = f"{SEC_FULL_INDEX_ROOT}/{current.year}/QTR{current.quarter}"
        plans.append(
            SecQuarterIndexPlan(
                quarter=current,
                start=max(start, current.start),
                end=min(end, current.end),
                master_url=f"{base}/master.idx",
                form_url=f"{base}/form.idx",
            )
        )
        if current.quarter == 4:
            current = SecQuarter(current.year + 1, 1)
        else:
            current = SecQuarter(current.year, current.quarter + 1)
    return tuple(plans)


_ACCESSION_RE = re.compile(r"(?P<accession>\d{10}-\d{2}-\d{6})\.txt$")
_SAFE_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")
_FORMS = frozenset({"8-K", "8-K/A"})
_REQUIRED_AVAILABILITY_LAGS = frozenset({1, 3, 5, 10})
_XNYS = xcals.get_calendar("XNYS")


@dataclass(frozen=True, slots=True)
class SecIndexEntry:
    """Exact 8-K or 8-K/A row from an SEC master/form index."""

    cik: str
    company_name: str
    form: Literal["8-K", "8-K/A"]
    filed_on: date
    filename: str
    accession_number: str
    index_kind: IndexKind

    @property
    def archive_url(self) -> str:
        return f"{SEC_ARCHIVES_ROOT}/{self.filename.lstrip('/')}"


def parse_sec_quarter_index(
    payload: bytes | str,
    *,
    kind: IndexKind | None = None,
) -> tuple[SecIndexEntry, ...]:
    """Parse an official SEC ``master.idx`` or fixed-width ``form.idx``.

    Rows for every form other than exact ``8-K`` and ``8-K/A`` are discarded.
    Eligible but malformed rows fail closed instead of entering the research
    sample with an invented date, CIK, or accession.
    """

    text = payload.decode("latin-1") if isinstance(payload, bytes) else payload
    lines = text.splitlines()
    detected = kind or _detect_index_kind(lines)
    if detected == "master":
        raw_rows = _master_rows(lines)
    else:
        raw_rows = _form_rows(lines)

    entries: list[SecIndexEntry] = []
    for cik, company, form, filed_on, filename in raw_rows:
        normalized_form = form.strip().upper()
        if normalized_form not in _FORMS:
            continue
        entries.append(
            _build_index_entry(
                cik=cik,
                company=company,
                form=normalized_form,
                filed_on=filed_on,
                filename=filename,
                kind=detected,
            )
        )
    return tuple(entries)


def _detect_index_kind(lines: Sequence[str]) -> IndexKind:
    if any(line.strip().startswith("CIK|Company Name|Form Type|") for line in lines):
        return "master"
    if any(
        "Form Type" in line and "Company Name" in line and "File Name" in line for line in lines
    ):
        return "form"
    raise SecIndexParseError("SEC index header was not found")


def _master_rows(lines: Sequence[str]) -> tuple[tuple[str, str, str, str, str], ...]:
    header_index = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip().startswith("CIK|Company Name|Form Type|")
        ),
        None,
    )
    if header_index is None:
        raise SecIndexParseError("master.idx header was not found")
    rows: list[tuple[str, str, str, str, str]] = []
    for line in lines[header_index + 1 :]:
        if not line.strip() or line.lstrip().startswith("-"):
            continue
        fields = line.split("|", maxsplit=4)
        if len(fields) != 5:
            if any(form in line.upper().split() for form in _FORMS):
                raise SecIndexParseError("malformed eligible master.idx row")
            continue
        rows.append(cast(tuple[str, str, str, str, str], tuple(fields)))
    return tuple(rows)


def _form_rows(lines: Sequence[str]) -> tuple[tuple[str, str, str, str, str], ...]:
    header_index = next(
        (
            index
            for index, line in enumerate(lines)
            if "Form Type" in line and "Company Name" in line and "File Name" in line
        ),
        None,
    )
    if header_index is None:
        raise SecIndexParseError("form.idx header was not found")
    header = lines[header_index]
    starts = (
        header.find("Form Type"),
        header.find("Company Name"),
        header.find("CIK", header.find("Company Name") + len("Company Name")),
        header.find("Date Filed"),
        header.find("File Name"),
    )
    if any(offset < 0 for offset in starts) or tuple(sorted(starts)) != starts:
        raise SecIndexParseError("form.idx columns are invalid")
    form_start, company_start, cik_start, date_start, filename_start = starts
    rows: list[tuple[str, str, str, str, str]] = []
    for line in lines[header_index + 1 :]:
        if not line.strip() or line.lstrip().startswith("-"):
            continue
        if len(line) < filename_start:
            if line[form_start:company_start].strip().upper() in _FORMS:
                raise SecIndexParseError("malformed eligible form.idx row")
            continue
        rows.append(
            (
                line[cik_start:date_start].strip(),
                line[company_start:cik_start].strip(),
                line[form_start:company_start].strip(),
                line[date_start:filename_start].strip(),
                line[filename_start:].strip(),
            )
        )
    return tuple(rows)


def _build_index_entry(
    *,
    cik: str,
    company: str,
    form: str,
    filed_on: str,
    filename: str,
    kind: IndexKind,
) -> SecIndexEntry:
    normalized_cik = cik.strip()
    normalized_company = company.strip()
    normalized_filename = filename.strip().lstrip("/")
    if not normalized_cik.isdigit() or not normalized_company:
        raise SecIndexParseError("eligible SEC index row has an invalid CIK or company")
    try:
        parsed_date = date.fromisoformat(filed_on.strip())
    except ValueError as exc:
        raise SecIndexParseError("eligible SEC index row has an invalid filing date") from exc
    accession_match = _ACCESSION_RE.search(normalized_filename)
    if accession_match is None:
        raise SecIndexParseError("eligible SEC index row has no valid accession filename")
    return SecIndexEntry(
        cik=normalized_cik,
        company_name=normalized_company,
        form=cast(Literal["8-K", "8-K/A"], form),
        filed_on=parsed_date,
        filename=normalized_filename,
        accession_number=accession_match.group("accession"),
        index_kind=kind,
    )


class SourceAvailabilityScenario(FrozenModel):
    """Counterfactual delay between SEC acceptance and observable source data."""

    name: str = Field(min_length=1)
    lag_minutes: int = Field(gt=0)

    def available_at(self, event: FilingEvent) -> datetime:
        return event.accepted_at.astimezone(UTC) + timedelta(minutes=self.lag_minutes)


SOURCE_AVAILABILITY_SCENARIOS = (
    SourceAvailabilityScenario(name="source_lag_1m", lag_minutes=1),
    SourceAvailabilityScenario(name="source_lag_3m", lag_minutes=3),
    SourceAvailabilityScenario(name="source_lag_5m_primary", lag_minutes=5),
    SourceAvailabilityScenario(name="source_lag_10m", lag_minutes=10),
)


class CoverageStatus(StrEnum):
    AVAILABLE = "available"
    MISSING_SYMBOL = "missing_symbol"
    INVALID_SYMBOL = "invalid_symbol"
    MISSING_FILING = "missing_filing"
    MISSING_BARS = "missing_bars"
    MISSING_QUOTES = "missing_quotes"
    MISSING_BARS_AND_QUOTES = "missing_bars_and_quotes"
    INSUFFICIENT_EXIT_COVERAGE = "insufficient_exit_coverage"
    MISSING_FEATURE_HISTORY = "missing_feature_history"
    MISSING_POINT_IN_TIME_ELIGIBILITY = "missing_point_in_time_eligibility"
    POINT_IN_TIME_INELIGIBLE = "point_in_time_ineligible"
    PROVIDER_ERROR = "provider_error"


class FeatureHistoryCoverage(FrozenModel):
    """Scenario-specific evidence that required point-in-time features are buildable."""

    required_previous_sessions: int = Field(default=20, ge=20)
    symbol_previous_sessions: int = Field(ge=0)
    benchmark_previous_sessions: int = Field(ge=0)
    symbol_same_slot_sessions: int = Field(ge=0)
    benchmark_same_slot_sessions: int = Field(ge=0)
    atr_source_minutes: int = Field(ge=0)
    confirmation_source_minutes: int = Field(ge=0)
    benchmark_confirmation_source_minutes: int = Field(ge=0)
    complete: bool
    missing: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_completeness(self) -> FeatureHistoryCoverage:
        if self.complete == bool(self.missing):
            raise ValueError("feature history is complete exactly when no gaps are reported")
        return self


class PointInTimeEligibility(FrozenModel):
    """Historical security-master evidence; unknown values remain fail-closed."""

    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")
    as_of: AwareDatetime
    source: str = Field(min_length=1)
    common_stock: bool | None = None
    us_listing: bool | None = None
    corporate_actions_complete: bool | None = None
    detail: str | None = None

    @property
    def missing_confirmations(self) -> tuple[str, ...]:
        values = {
            "COMMON_STOCK": self.common_stock,
            "US_LISTING": self.us_listing,
            "CORPORATE_ACTIONS": self.corporate_actions_complete,
        }
        return tuple(name for name, value in values.items() if value is None)

    @property
    def ineligible_reasons(self) -> tuple[str, ...]:
        values = {
            "NOT_COMMON_STOCK": self.common_stock,
            "NOT_US_LISTED": self.us_listing,
            "CORPORATE_ACTIONS_INCOMPLETE": self.corporate_actions_complete,
        }
        return tuple(name for name, value in values.items() if value is False)

    @property
    def confirmed_eligible(self) -> bool:
        return (
            self.common_stock is True
            and self.us_listing is True
            and self.corporate_actions_complete is True
        )


class CoverageRecord(FrozenModel):
    """Auditable availability outcome; never interpreted as a zero return."""

    record_id: str = Field(min_length=1)
    quarter: str = Field(pattern=r"^\d{4}-Q[1-4]$")
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    symbol: str | None = None
    scenario: str | None = None
    lag_minutes: int | None = Field(default=None, gt=0)
    available_at: AwareDatetime | None = None
    evaluation_at: AwareDatetime | None = None
    window_end: AwareDatetime | None = None
    provider: str | None = None
    feed: str | None = None
    bundle_start: AwareDatetime | None = None
    bundle_end: AwareDatetime | None = None
    benchmark_symbol: str | None = None
    status: CoverageStatus
    bar_count: int = Field(default=0, ge=0)
    quote_count: int = Field(default=0, ge=0)
    bundle_bar_count: int = Field(default=0, ge=0)
    bundle_quote_count: int = Field(default=0, ge=0)
    benchmark_bar_count: int = Field(default=0, ge=0)
    scenario_covered: bool | None = None
    feature_history: FeatureHistoryCoverage | None = None
    eligibility: PointInTimeEligibility | None = None
    tradable_coverage_complete: bool = False
    detail: str | None = None
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def validate_scenario_fields(self) -> CoverageRecord:
        scenario_values = (
            self.scenario,
            self.lag_minutes,
            self.available_at,
            self.evaluation_at,
            self.window_end,
        )
        if any(value is not None for value in scenario_values) and any(
            value is None for value in scenario_values
        ):
            raise ValueError("scenario, lag and window timestamps must be supplied together")
        if (
            self.available_at is not None
            and self.evaluation_at is not None
            and self.evaluation_at < self.available_at
        ):
            raise ValueError("scenario evaluation cannot predate source availability")
        if (
            self.evaluation_at is not None
            and self.window_end is not None
            and self.window_end <= self.evaluation_at
        ):
            raise ValueError("coverage window must end after scenario evaluation")
        bundle_values = (self.bundle_start, self.bundle_end)
        if any(value is not None for value in bundle_values) and any(
            value is None for value in bundle_values
        ):
            raise ValueError("bundle timestamps must be supplied together")
        if (
            self.bundle_start is not None
            and self.bundle_end is not None
            and self.bundle_end <= self.bundle_start
        ):
            raise ValueError("bundle end must be after bundle start")
        if self.scenario is not None and (not self.provider or not self.feed):
            raise ValueError("scenario coverage requires provider and feed labels")
        if self.feature_history is not None and self.scenario is None:
            raise ValueError("feature history belongs to scenario coverage")
        if self.eligibility is not None and self.symbol != self.eligibility.symbol:
            raise ValueError("eligibility symbol must match coverage symbol")
        if self.tradable_coverage_complete and (
            self.status is not CoverageStatus.AVAILABLE
            or self.scenario_covered is not True
            or self.feature_history is None
            or not self.feature_history.complete
            or self.eligibility is None
            or not self.eligibility.confirmed_eligible
        ):
            raise ValueError(
                "tradable coverage requires complete market, feature and eligibility data"
            )
        if self.status is CoverageStatus.AVAILABLE and not self.tradable_coverage_complete:
            raise ValueError("available status requires complete tradable coverage")
        return self


class BackfillCheckpoint(FrozenModel):
    quarter: str = Field(pattern=r"^\d{4}-Q[1-4]$")
    range_start: date
    range_end: date
    processed_accessions: tuple[str, ...] = ()
    completed: bool = False
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_checkpoint(self) -> BackfillCheckpoint:
        if self.range_end < self.range_start:
            raise ValueError("checkpoint range is invalid")
        if len(set(self.processed_accessions)) != len(self.processed_accessions):
            raise ValueError("checkpoint accessions must be unique")
        return self


class BackfillStore(Protocol):
    async def load_checkpoint(self, quarter: str) -> BackfillCheckpoint | None:
        """Return the last committed accession state for one quarter."""

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        """Durably replace one quarter checkpoint."""

    async def save_coverage(self, record: CoverageRecord) -> None:
        """Durably upsert one deterministic coverage record."""


class JsonBackfillStore:
    """Small durable JSON store suitable for the single-process MVP.

    Coverage records are keyed by deterministic ``record_id`` values, so a
    crash after writing coverage but before advancing the accession checkpoint
    remains idempotent on restart.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self._lock = asyncio.Lock()

    async def load_checkpoint(self, quarter: str) -> BackfillCheckpoint | None:
        async with self._lock:
            state = await asyncio.to_thread(self._read_state)
        payload = state["checkpoints"].get(quarter)
        return BackfillCheckpoint.model_validate(payload) if payload is not None else None

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        async with self._lock:
            state = await asyncio.to_thread(self._read_state)
            state["checkpoints"][checkpoint.quarter] = checkpoint.model_dump(mode="json")
            await asyncio.to_thread(self._write_state, state)

    async def save_coverage(self, record: CoverageRecord) -> None:
        async with self._lock:
            state = await asyncio.to_thread(self._read_state)
            state["coverage"][record.record_id] = record.model_dump(mode="json")
            await asyncio.to_thread(self._write_state, state)

    async def list_coverage(self) -> tuple[CoverageRecord, ...]:
        async with self._lock:
            state = await asyncio.to_thread(self._read_state)
        return tuple(
            CoverageRecord.model_validate(payload)
            for _, payload in sorted(state["coverage"].items())
        )

    def _read_state(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {"checkpoints": {}, "coverage": {}}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BackfillStateError(f"cannot read backfill state: {self.path}") from exc
        if not isinstance(payload, dict):
            raise BackfillStateError("backfill state must be a JSON object")
        checkpoints = payload.get("checkpoints")
        coverage = payload.get("coverage")
        if not isinstance(checkpoints, dict) or not isinstance(coverage, dict):
            raise BackfillStateError("backfill state has an unsupported schema")
        return {"checkpoints": checkpoints, "coverage": coverage}

    def _write_state(self, state: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(
                json.dumps(state, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


IndexFetcher = Callable[[str], bytes | Awaitable[bytes]]
FilingResolver = Callable[[SecIndexEntry], FilingEvent | Awaitable[FilingEvent | None] | None]
EligibilityResolver = Callable[
    [FilingEvent, str],
    PointInTimeEligibility | Awaitable[PointInTimeEligibility | None] | None,
]


@dataclass(frozen=True, slots=True)
class BackfillConfig:
    start: date = BACKFILL_START
    end: date = BACKFILL_END
    index_kind: IndexKind = "master"
    availability_scenarios: tuple[SourceAvailabilityScenario, ...] = SOURCE_AVAILABILITY_SCENARIOS
    market_horizon_minutes: int = 60
    lookback_calendar_days: int = 45
    timeframe: str = "1Min"
    feed: str = "sip"
    provider_name: str = "alpaca"
    benchmark_symbol: str = "SPY"

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError("backfill end must be on or after start")
        if self.index_kind not in {"master", "form"}:
            raise ValueError("index_kind must be 'master' or 'form'")
        if self.market_horizon_minutes < 60:
            raise ValueError("event and exit coverage requires at least 60 minutes")
        if self.lookback_calendar_days < 45:
            raise ValueError("feature history requires at least 45 calendar days of lookback")
        if self.timeframe.strip().lower() != "1min":
            raise ValueError("feature backfill requires one-minute bars")
        if not self.feed.strip() or not self.provider_name.strip():
            raise ValueError("feed and provider_name must not be empty")
        if self.feed.strip().casefold() != "sip":
            raise ValueError("version 1 historical backfill requires the SIP feed")
        if self.benchmark_symbol.strip().upper() != "SPY":
            raise ValueError("version 1 requires SPY as the benchmark symbol")
        lags = tuple(item.lag_minutes for item in self.availability_scenarios)
        if not lags or len(lags) != len(set(lags)):
            raise ValueError("availability scenarios must contain unique lags")
        names = tuple(item.name for item in self.availability_scenarios)
        if len(names) != len(set(names)):
            raise ValueError("availability scenarios must contain unique names")
        if not _REQUIRED_AVAILABILITY_LAGS.issubset(lags):
            raise ValueError("availability scenarios must cover 1, 3, 5 and 10 minute lags")


class BackfillRunSummary(FrozenModel):
    quarter_count: int = Field(ge=0)
    completed_quarters: int = Field(ge=0)
    skipped_completed_quarters: int = Field(ge=0)
    discovered_accessions: int = Field(ge=0)
    processed_accessions: int = Field(ge=0)
    resumed_accessions: int = Field(ge=0)
    coverage_records: int = Field(ge=0)


class HistoricalBackfillRunner:
    """Enumerate SEC filings and persist one deduplicated bundle per filing/symbol."""

    def __init__(
        self,
        *,
        index_fetcher: IndexFetcher,
        filing_resolver: FilingResolver,
        eligibility_resolver: EligibilityResolver | None = None,
        market_data: MarketDataProvider,
        data_lake: ParquetMarketDataLake,
        store: BackfillStore,
        config: BackfillConfig | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._index_fetcher = index_fetcher
        self._filing_resolver = filing_resolver
        self._eligibility_resolver = eligibility_resolver
        self._market_data = market_data
        self._data_lake = data_lake
        self._store = store
        self._config = config or BackfillConfig()
        self._clock = clock or (lambda: datetime.now(UTC))

    async def run(self) -> BackfillRunSummary:
        plans = plan_sec_quarter_indexes(self._config.start, self._config.end)
        completed_quarters = 0
        skipped_quarters = 0
        discovered = 0
        processed = 0
        resumed = 0
        coverage_count = 0

        for plan in plans:
            checkpoint = await _maybe_await(self._store.load_checkpoint(plan.quarter.key))
            if checkpoint is not None:
                self._validate_checkpoint(checkpoint, plan)
                resumed += len(checkpoint.processed_accessions)
                if checkpoint.completed:
                    skipped_quarters += 1
                    continue
            else:
                checkpoint = BackfillCheckpoint(
                    quarter=plan.quarter.key,
                    range_start=plan.start,
                    range_end=plan.end,
                    updated_at=_utc_now(self._clock),
                )

            payload = await _maybe_await(self._index_fetcher(plan.url_for(self._config.index_kind)))
            entries = _dedupe_entries(
                entry
                for entry in parse_sec_quarter_index(payload, kind=self._config.index_kind)
                if plan.start <= entry.filed_on <= plan.end
            )
            discovered += len(entries)
            done = set(checkpoint.processed_accessions)

            for entry in entries:
                if entry.accession_number in done:
                    continue
                count = await self._process_accession(plan, entry)
                coverage_count += count
                processed += 1
                done.add(entry.accession_number)
                checkpoint = checkpoint.model_copy(
                    update={
                        "processed_accessions": tuple(
                            sorted((*checkpoint.processed_accessions, entry.accession_number))
                        ),
                        "updated_at": _utc_now(self._clock),
                    }
                )
                await _maybe_await(self._store.save_checkpoint(checkpoint))

            checkpoint = checkpoint.model_copy(
                update={"completed": True, "updated_at": _utc_now(self._clock)}
            )
            await _maybe_await(self._store.save_checkpoint(checkpoint))
            completed_quarters += 1

        return BackfillRunSummary(
            quarter_count=len(plans),
            completed_quarters=completed_quarters,
            skipped_completed_quarters=skipped_quarters,
            discovered_accessions=discovered,
            processed_accessions=processed,
            resumed_accessions=resumed,
            coverage_records=coverage_count,
        )

    async def _process_accession(
        self,
        plan: SecQuarterIndexPlan,
        entry: SecIndexEntry,
    ) -> int:
        event = await _maybe_await(self._filing_resolver(entry))
        if event is None:
            await self._save_coverage(
                _non_market_coverage(
                    plan,
                    entry,
                    status=CoverageStatus.MISSING_FILING,
                    detail="point-in-time filing details could not be resolved",
                    recorded_at=_utc_now(self._clock),
                )
            )
            return 1
        if event.accession_number != entry.accession_number or event.form != entry.form:
            raise BackfillStateError(
                "filing resolver returned an event for a different accession or form"
            )
        try:
            await asyncio.to_thread(
                self._data_lake.write_filings,
                (event,),
                batch_id=entry.accession_number,
            )
        except FileExistsError:
            # The immutable accession partition may precede a checkpoint update
            # when a process exits between those two durable writes.
            pass

        symbols = _historical_symbols(event.symbols)
        if not symbols:
            await self._save_coverage(
                _non_market_coverage(
                    plan,
                    entry,
                    status=CoverageStatus.MISSING_SYMBOL,
                    detail="historical filing has no point-in-time symbol mapping",
                    recorded_at=_utc_now(self._clock),
                )
            )
            return 1

        records = 0
        for symbol in symbols:
            if _SAFE_SYMBOL_RE.fullmatch(symbol) is None:
                await self._save_coverage(
                    _non_market_coverage(
                        plan,
                        entry,
                        status=CoverageStatus.INVALID_SYMBOL,
                        symbol=symbol,
                        detail="historical symbol is syntactically unsafe",
                        recorded_at=_utc_now(self._clock),
                    )
                )
                records += 1
                continue
            eligibility = await self._resolve_eligibility(event, symbol)
            scenario_records = await self._load_market_bundle(
                plan=plan,
                entry=entry,
                event=event,
                symbol=symbol,
                eligibility=eligibility,
            )
            for record in scenario_records:
                await self._save_coverage(record)
                records += 1
        return records

    async def _load_market_bundle(
        self,
        *,
        plan: SecQuarterIndexPlan,
        entry: SecIndexEntry,
        event: FilingEvent,
        symbol: str,
        eligibility: PointInTimeEligibility,
    ) -> tuple[CoverageRecord, ...]:
        scenarios = self._config.availability_scenarios
        scenario_windows = tuple(
            (
                scenario,
                (available_at := scenario.available_at(event)),
                (evaluation_at := _evaluation_session(available_at)[1]),
                evaluation_at + timedelta(minutes=self._config.market_horizon_minutes),
            )
            for scenario in scenarios
        )
        earliest_available = min(item[1] for item in scenario_windows)
        latest_window_end = max(item[3] for item in scenario_windows)
        bundle_start = event.accepted_at.astimezone(UTC) - timedelta(
            days=self._config.lookback_calendar_days
        )
        benchmark = self._config.benchmark_symbol.strip().upper()
        get_symbol_bars = partial(
            self._market_data.get_bars,
            symbol,
            start=bundle_start,
            end=latest_window_end,
            timeframe=self._config.timeframe,
            feed=self._config.feed,
        )
        get_quotes = partial(
            self._market_data.get_quotes,
            symbol,
            start=earliest_available,
            end=latest_window_end,
            feed=self._config.feed,
        )
        get_benchmark_bars = partial(
            self._market_data.get_bars,
            benchmark,
            start=bundle_start,
            end=latest_window_end,
            timeframe=self._config.timeframe,
            feed=self._config.feed,
        )
        try:
            if symbol == benchmark:
                symbol_result, quotes_result = await asyncio.gather(
                    asyncio.to_thread(get_symbol_bars),
                    asyncio.to_thread(get_quotes),
                )
                benchmark_result = symbol_result
            else:
                symbol_result, benchmark_result, quotes_result = await asyncio.gather(
                    asyncio.to_thread(get_symbol_bars),
                    asyncio.to_thread(get_benchmark_bars),
                    asyncio.to_thread(get_quotes),
                )
            symbol_bars = _deduplicate_market_records(
                symbol,
                tuple(await _maybe_await(symbol_result)),
                start=bundle_start,
                end=latest_window_end,
                feed=self._config.feed,
            )
            benchmark_bars = (
                symbol_bars
                if symbol == benchmark
                else _deduplicate_market_records(
                    benchmark,
                    tuple(await _maybe_await(benchmark_result)),
                    start=bundle_start,
                    end=latest_window_end,
                    feed=self._config.feed,
                )
            )
            quotes = _deduplicate_market_records(
                symbol,
                tuple(await _maybe_await(quotes_result)),
                start=earliest_available,
                end=latest_window_end,
                feed=self._config.feed,
            )
        except Exception as exc:
            for scenario, available_at, evaluation_at, window_end in scenario_windows:
                await self._save_coverage(
                    _scenario_coverage(
                        plan=plan,
                        entry=entry,
                        symbol=symbol,
                        scenario=scenario,
                        available_at=available_at,
                        evaluation_at=evaluation_at,
                        window_end=window_end,
                        provider=self._config.provider_name,
                        feed=self._config.feed,
                        bundle_start=bundle_start,
                        bundle_end=latest_window_end,
                        benchmark_symbol=benchmark,
                        eligibility=eligibility,
                        status=CoverageStatus.PROVIDER_ERROR,
                        scenario_covered=False,
                        recorded_at=_utc_now(self._clock),
                        detail=exc.__class__.__name__,
                    )
                )
            raise BackfillMarketDataError(
                f"market data failed for {entry.accession_number}/{symbol}"
            ) from exc

        batch_id = _batch_id(entry.accession_number, symbol)
        all_bars = symbol_bars if symbol == benchmark else (*symbol_bars, *benchmark_bars)
        await _write_idempotent(self._data_lake, all_bars, quotes, batch_id=batch_id)

        records: list[CoverageRecord] = []
        for scenario, available_at, evaluation_at, window_end in scenario_windows:
            scenario_bars = tuple(
                bar for bar in symbol_bars if evaluation_at <= bar.timestamp <= window_end
            )
            scenario_quotes = tuple(
                quote
                for quote in quotes
                if evaluation_at - timedelta(seconds=5) <= quote.timestamp <= window_end
            )
            feature_history = _feature_history_coverage(
                available_at=evaluation_at,
                symbol_bars=symbol_bars,
                benchmark_bars=benchmark_bars,
            )
            scenario_covered = _complete_exit_coverage(
                evaluation_at=evaluation_at,
                window_end=window_end,
                bars=scenario_bars,
                quotes=scenario_quotes,
            )
            if not scenario_bars and not scenario_quotes:
                status = CoverageStatus.MISSING_BARS_AND_QUOTES
            elif not scenario_bars:
                status = CoverageStatus.MISSING_BARS
            elif not scenario_quotes:
                status = CoverageStatus.MISSING_QUOTES
            elif not scenario_covered:
                status = CoverageStatus.INSUFFICIENT_EXIT_COVERAGE
            elif not feature_history.complete:
                status = CoverageStatus.MISSING_FEATURE_HISTORY
            elif eligibility.missing_confirmations:
                status = CoverageStatus.MISSING_POINT_IN_TIME_ELIGIBILITY
            elif not eligibility.confirmed_eligible:
                status = CoverageStatus.POINT_IN_TIME_INELIGIBLE
            else:
                status = CoverageStatus.AVAILABLE
            tradable_coverage_complete = status is CoverageStatus.AVAILABLE
            records.append(
                _scenario_coverage(
                    plan=plan,
                    entry=entry,
                    symbol=symbol,
                    scenario=scenario,
                    available_at=available_at,
                    evaluation_at=evaluation_at,
                    window_end=window_end,
                    provider=self._config.provider_name,
                    feed=self._config.feed,
                    bundle_start=bundle_start,
                    bundle_end=latest_window_end,
                    benchmark_symbol=benchmark,
                    eligibility=eligibility,
                    status=status,
                    bar_count=len(scenario_bars),
                    quote_count=len(scenario_quotes),
                    bundle_bar_count=len(symbol_bars),
                    bundle_quote_count=len(quotes),
                    benchmark_bar_count=len(benchmark_bars),
                    scenario_covered=scenario_covered,
                    feature_history=feature_history,
                    tradable_coverage_complete=tradable_coverage_complete,
                    recorded_at=_utc_now(self._clock),
                )
            )
        return tuple(records)

    async def _resolve_eligibility(
        self,
        event: FilingEvent,
        symbol: str,
    ) -> PointInTimeEligibility:
        if self._eligibility_resolver is None:
            return _unknown_eligibility(
                event,
                symbol,
                detail="point-in-time eligibility resolver is not configured",
            )
        try:
            resolved = await _maybe_await(self._eligibility_resolver(event, symbol))
        except Exception as exc:
            return _unknown_eligibility(
                event,
                symbol,
                detail=f"eligibility provider failed: {exc.__class__.__name__}",
            )
        if resolved is None:
            return _unknown_eligibility(
                event,
                symbol,
                detail="point-in-time eligibility could not be resolved",
            )
        if (
            resolved.accession_number != event.accession_number
            or resolved.symbol != symbol
            or resolved.as_of > event.accepted_at
        ):
            raise BackfillStateError(
                "eligibility resolver returned mismatched or future-dated evidence"
            )
        return resolved

    async def _save_coverage(self, record: CoverageRecord) -> None:
        await _maybe_await(self._store.save_coverage(record))

    @staticmethod
    def _validate_checkpoint(
        checkpoint: BackfillCheckpoint,
        plan: SecQuarterIndexPlan,
    ) -> None:
        if (
            checkpoint.quarter != plan.quarter.key
            or checkpoint.range_start != plan.start
            or checkpoint.range_end != plan.end
        ):
            raise BackfillStateError(
                f"checkpoint {checkpoint.quarter} does not match requested date slice"
            )


def _historical_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    """Normalize only symbols carried by the historical filing event."""

    return tuple(sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()}))


def _unknown_eligibility(
    event: FilingEvent,
    symbol: str,
    *,
    detail: str,
) -> PointInTimeEligibility:
    return PointInTimeEligibility(
        accession_number=event.accession_number,
        symbol=symbol,
        as_of=event.accepted_at,
        source="unresolved",
        detail=detail,
    )


def _dedupe_entries(entries: Iterable[SecIndexEntry]) -> tuple[SecIndexEntry, ...]:
    by_accession: dict[str, SecIndexEntry] = {}
    for entry in sorted(entries, key=lambda item: (item.filed_on, item.accession_number)):
        current = by_accession.get(entry.accession_number)
        if current is not None and current != entry:
            raise SecIndexParseError(f"conflicting SEC index rows for {entry.accession_number}")
        by_accession[entry.accession_number] = entry
    return tuple(by_accession.values())


def _non_market_coverage(
    plan: SecQuarterIndexPlan,
    entry: SecIndexEntry,
    *,
    status: CoverageStatus,
    recorded_at: datetime,
    symbol: str | None = None,
    detail: str | None = None,
) -> CoverageRecord:
    symbol_part = symbol or "_none"
    return CoverageRecord(
        record_id=f"{plan.quarter.key}:{entry.accession_number}:{symbol_part}:filing",
        quarter=plan.quarter.key,
        accession_number=entry.accession_number,
        symbol=symbol,
        status=status,
        detail=detail,
        recorded_at=recorded_at,
    )


def _scenario_coverage(
    *,
    plan: SecQuarterIndexPlan,
    entry: SecIndexEntry,
    symbol: str,
    scenario: SourceAvailabilityScenario,
    available_at: datetime,
    evaluation_at: datetime,
    window_end: datetime,
    provider: str,
    feed: str,
    bundle_start: datetime,
    bundle_end: datetime,
    benchmark_symbol: str,
    eligibility: PointInTimeEligibility,
    status: CoverageStatus,
    recorded_at: datetime,
    bar_count: int = 0,
    quote_count: int = 0,
    bundle_bar_count: int = 0,
    bundle_quote_count: int = 0,
    benchmark_bar_count: int = 0,
    scenario_covered: bool | None = None,
    feature_history: FeatureHistoryCoverage | None = None,
    tradable_coverage_complete: bool = False,
    detail: str | None = None,
) -> CoverageRecord:
    return CoverageRecord(
        record_id=(
            f"{plan.quarter.key}:{entry.accession_number}:{symbol}:lag-{scenario.lag_minutes}m"
        ),
        quarter=plan.quarter.key,
        accession_number=entry.accession_number,
        symbol=symbol,
        scenario=scenario.name,
        lag_minutes=scenario.lag_minutes,
        available_at=available_at,
        evaluation_at=evaluation_at,
        window_end=window_end,
        provider=provider,
        feed=feed,
        bundle_start=bundle_start,
        bundle_end=bundle_end,
        benchmark_symbol=benchmark_symbol,
        eligibility=eligibility,
        status=status,
        bar_count=bar_count,
        quote_count=quote_count,
        bundle_bar_count=bundle_bar_count,
        bundle_quote_count=bundle_quote_count,
        benchmark_bar_count=benchmark_bar_count,
        scenario_covered=scenario_covered,
        feature_history=feature_history,
        tradable_coverage_complete=tradable_coverage_complete,
        detail=detail,
        recorded_at=recorded_at,
    )


def _deduplicate_market_records[MarketRecord: (Bar, Quote)](
    symbol: str,
    records: Sequence[MarketRecord],
    *,
    start: datetime,
    end: datetime,
    feed: str,
) -> tuple[MarketRecord, ...]:
    """Validate provider lineage/window and collapse exact natural-key duplicates."""

    normalized_symbol = symbol.upper()
    by_key: dict[tuple[str, str, str, datetime], MarketRecord] = {}
    for record in records:
        if record.symbol.upper() != normalized_symbol:
            raise BackfillMarketDataError("market-data provider returned a different symbol")
        if record.feed.casefold() != feed.casefold():
            raise BackfillMarketDataError("market-data provider returned a different feed")
        if not start <= record.timestamp <= end:
            raise BackfillMarketDataError("market-data provider returned an out-of-window record")
        key = (
            record.source.value,
            record.feed.casefold(),
            record.symbol.upper(),
            record.timestamp,
        )
        existing = by_key.get(key)
        if existing is not None and existing != record:
            raise BackfillMarketDataError("market-data provider returned conflicting duplicates")
        by_key[key] = record
    return tuple(sorted(by_key.values(), key=lambda item: item.timestamp))


def _feature_history_coverage(
    *,
    available_at: datetime,
    symbol_bars: Sequence[Bar],
    benchmark_bars: Sequence[Bar],
) -> FeatureHistoryCoverage:
    evaluation_time = available_at.astimezone(UTC)
    reference_session = _session_containing_evaluation(evaluation_time)
    previous_sessions = _previous_sessions(reference_session, count=20)
    symbol_previous = _complete_session_count(symbol_bars, previous_sessions)
    benchmark_previous = _complete_session_count(benchmark_bars, previous_sessions)
    symbol_same_slot = _same_slot_session_count(
        symbol_bars,
        previous_sessions=previous_sessions,
        reference_session=reference_session,
        evaluation_time=evaluation_time,
    )
    benchmark_same_slot = _same_slot_session_count(
        benchmark_bars,
        previous_sessions=previous_sessions,
        reference_session=reference_session,
        evaluation_time=evaluation_time,
    )
    atr_minute_ends = _latest_rth_minute_ends(evaluation_time, count=75)
    confirmation_minute_ends = atr_minute_ends[-5:]
    atr_minutes = _covered_minute_count(symbol_bars, atr_minute_ends)
    confirmation_minutes = _covered_minute_count(symbol_bars, confirmation_minute_ends)
    benchmark_confirmation_minutes = _covered_minute_count(benchmark_bars, confirmation_minute_ends)

    missing: list[str] = []
    if symbol_previous < 20:
        missing.append("SYMBOL_PREVIOUS_20_SESSIONS")
    if benchmark_previous < 20:
        missing.append("BENCHMARK_PREVIOUS_20_SESSIONS")
    if symbol_same_slot < 20:
        missing.append("SYMBOL_SAME_SLOT_20_SESSIONS")
    if benchmark_same_slot < 20:
        missing.append("BENCHMARK_SAME_SLOT_20_SESSIONS")
    if atr_minutes < 75:
        missing.append("ATR_15_X_5M_SOURCE_HISTORY")
    if confirmation_minutes < 5:
        missing.append("SYMBOL_CONFIRMATION_5M")
    if benchmark_confirmation_minutes < 5:
        missing.append("BENCHMARK_CONFIRMATION_5M")
    return FeatureHistoryCoverage(
        symbol_previous_sessions=symbol_previous,
        benchmark_previous_sessions=benchmark_previous,
        symbol_same_slot_sessions=symbol_same_slot,
        benchmark_same_slot_sessions=benchmark_same_slot,
        atr_source_minutes=atr_minutes,
        confirmation_source_minutes=confirmation_minutes,
        benchmark_confirmation_source_minutes=benchmark_confirmation_minutes,
        complete=not missing,
        missing=tuple(missing),
    )


def _evaluation_session(value: datetime) -> tuple[object, datetime]:
    utc_value = value.astimezone(UTC)
    scheduled = NyseSessionCalendar().next_evaluation_time(utc_value)
    if scheduled is not None:
        market_day = _market_date(scheduled)
        return _XNYS.date_to_session(market_day.isoformat()), scheduled

    # A filing first observed after the entry cutoff but before the close is
    # retained as explicit, non-tradable coverage.  It must not be shifted to
    # the next session because the live path intentionally skips it.
    market_day = _market_date(utc_value)
    label = market_day.isoformat()
    if _XNYS.is_session(label):
        session = _XNYS.date_to_session(label)
        return session, utc_value
    raise BackfillStateError("historical evaluation time could not be scheduled")


def _previous_sessions(reference_session: object, *, count: int) -> tuple[object, ...]:
    sessions: list[object] = []
    cursor = reference_session
    for _ in range(count):
        cursor = _XNYS.previous_session(cursor)
        sessions.append(cursor)
    return tuple(reversed(sessions))


def _same_slot_session_count(
    bars: Sequence[Bar],
    *,
    previous_sessions: Sequence[object],
    reference_session: object,
    evaluation_time: datetime,
) -> int:
    reference_open = _XNYS.session_open(reference_session).to_pydatetime().astimezone(UTC)
    slot_minutes = max(1, int((evaluation_time - reference_open).total_seconds() // 60))
    unique_minutes = {bar.timestamp for bar in bars}
    covered = 0
    for session in previous_sessions:
        opening = _XNYS.session_open(session).to_pydatetime().astimezone(UTC)
        closing = _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
        session_minutes = int((closing - opening).total_seconds() // 60)
        required = min(slot_minutes, session_minutes)
        expected = {opening + timedelta(minutes=minute) for minute in range(1, required + 1)}
        covered += expected.issubset(unique_minutes)
    return covered


def _complete_session_count(
    bars: Sequence[Bar],
    sessions: Sequence[object],
) -> int:
    observed = {bar.timestamp for bar in bars}
    covered = 0
    for session in sessions:
        opening = _XNYS.session_open(session).to_pydatetime().astimezone(UTC)
        closing = _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
        session_minutes = int((closing - opening).total_seconds() // 60)
        expected = {opening + timedelta(minutes=minute) for minute in range(1, session_minutes + 1)}
        covered += expected.issubset(observed)
    return covered


def _latest_rth_minute_ends(upper_bound: datetime, *, count: int) -> tuple[datetime, ...]:
    if count <= 0:
        raise ValueError("minute-history count must be positive")
    session = _session_containing_evaluation(upper_bound)
    cursor = session
    effective_bound = upper_bound.astimezone(UTC).replace(second=0, microsecond=0)
    newest_first: list[datetime] = []
    while len(newest_first) < count:
        opening = _XNYS.session_open(cursor).to_pydatetime().astimezone(UTC)
        closing = _XNYS.session_close(cursor).to_pydatetime().astimezone(UTC)
        effective_end = min(effective_bound, closing)
        completed = max(0, int((effective_end - opening).total_seconds() // 60))
        newest_first.extend(
            opening + timedelta(minutes=minute) for minute in range(completed, 0, -1)
        )
        if len(newest_first) >= count:
            break
        cursor = _XNYS.previous_session(cursor)
        effective_bound = _XNYS.session_close(cursor).to_pydatetime().astimezone(UTC)
    return tuple(reversed(newest_first[:count]))


def _session_containing_evaluation(value: datetime) -> object:
    utc_value = value.astimezone(UTC)
    label = _market_date(utc_value).isoformat()
    if not _XNYS.is_session(label):
        raise BackfillStateError("historical evaluation must belong to an NYSE session")
    session = _XNYS.date_to_session(label)
    opening = _XNYS.session_open(session).to_pydatetime().astimezone(UTC)
    closing = _XNYS.session_close(session).to_pydatetime().astimezone(UTC)
    if not opening < utc_value <= closing:
        raise BackfillStateError("historical evaluation is outside regular trading hours")
    return session


def _covered_minute_count(bars: Sequence[Bar], expected: Sequence[datetime]) -> int:
    observed = {bar.timestamp for bar in bars}
    return len(observed & set(expected))


def _complete_exit_coverage(
    *,
    evaluation_at: datetime,
    window_end: datetime,
    bars: Sequence[Bar],
    quotes: Sequence[Quote],
) -> bool:
    """Require every completed minute and a quote no older than five seconds."""

    if window_end - evaluation_at < timedelta(minutes=60):
        return False
    expected_bar_ends = tuple(evaluation_at + timedelta(minutes=minute) for minute in range(1, 61))
    observed_bar_ends = {bar.timestamp for bar in bars}
    if not set(expected_bar_ends).issubset(observed_bar_ends):
        return False

    quote_times = tuple(sorted(quote.timestamp for quote in quotes))
    if not quote_times:
        return False
    for timestamp in (evaluation_at, *expected_bar_ends):
        index = bisect_right(quote_times, timestamp) - 1
        if index < 0 or timestamp - quote_times[index] > timedelta(seconds=5):
            return False
    return True


def _market_date(value: datetime) -> date:
    return value.astimezone(_XNYS.tz).date()


async def _write_idempotent(
    lake: ParquetMarketDataLake,
    bars: Sequence[Bar],
    quotes: Sequence[Quote],
    *,
    batch_id: str,
) -> None:
    """Write one immutable partition at a time and tolerate exact restart IDs."""

    for records, writer in (
        (bars, lake.write_bars),
        (quotes, lake.write_quotes),
    ):
        grouped: dict[tuple[str, str, date], list[Bar | Quote]] = {}
        for record in records:
            key = (record.source.value, record.symbol.upper(), record.timestamp.date())
            grouped.setdefault(key, []).append(record)
        for key, partition_records in grouped.items():
            partition_batch = f"{batch_id}-{key[2].isoformat()}"
            try:
                await asyncio.to_thread(
                    writer,
                    partition_records,
                    batch_id=partition_batch,
                )
            except FileExistsError:
                # A deterministic batch can already exist after a crash between
                # the Parquet write and the per-accession checkpoint commit.
                continue


def _batch_id(accession: str, symbol: str) -> str:
    safe_symbol = re.sub(r"[^A-Z0-9.-]", "_", symbol.upper())
    return f"{accession}-{safe_symbol}-point-in-time"


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    current = clock()
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("backfill clock must return a timezone-aware timestamp")
    return current.astimezone(UTC)


async def _maybe_await[T](value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await cast(Awaitable[T], value)
    return cast(T, value)
