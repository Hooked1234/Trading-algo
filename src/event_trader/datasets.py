"""Point-in-time research data lake and quality accounting."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, TypeVar

import duckdb
import polars as pl
from pydantic import Field, model_validator

from .artifacts import HashedArtifact, Sha256
from .domain import Bar, DataSource, FilingEvent, FrozenModel, Quote

MarketRecord = TypeVar("MarketRecord", Bar, Quote)
_SAFE_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")
_QUARTER = re.compile(r"^(\d{4})-Q([1-4])$")
REQUIRED_AVAILABILITY_LAGS = (1, 3, 5, 10)


class DatasetPartition(FrozenModel):
    """One immutable parquet partition file and its content address."""

    path: str = Field(min_length=1)
    sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)


class DatasetManifest(HashedArtifact):
    """Hashed inventory of the research data lake at one point in time."""

    artifact_version: Literal["1"] = "1"
    partitions: tuple[DatasetPartition, ...] = ()
    artifact_sha256: Sha256 = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_partitions(self) -> DatasetManifest:
        paths = tuple(partition.path for partition in self.partitions)
        if len(set(paths)) != len(paths):
            raise ValueError("a dataset manifest lists every partition exactly once")
        if list(paths) != sorted(paths):
            raise ValueError("dataset manifest partitions must be canonically ordered")
        return self


def build_dataset_manifest(root: Path) -> DatasetManifest:
    """Hash every parquet partition below ``root`` in canonical path order."""

    resolved = Path(root)
    partitions: list[DatasetPartition] = []
    for path in sorted(resolved.glob("**/*.parquet")):
        payload = path.read_bytes()
        partitions.append(
            DatasetPartition(
                path=path.relative_to(resolved).as_posix(),
                sha256=sha256(payload).hexdigest(),
                size_bytes=len(payload),
            )
        )
    manifest = DatasetManifest(partitions=tuple(partitions))
    return manifest.sealed()


class DatasetPeriod(FrozenModel):
    name: Literal["development", "validation", "holdout", "forward"]
    start: date
    end: date | None


PERIODS = (
    DatasetPeriod(name="development", start=date(2019, 1, 1), end=date(2023, 12, 31)),
    DatasetPeriod(name="validation", start=date(2024, 1, 1), end=date(2024, 12, 31)),
    DatasetPeriod(name="holdout", start=date(2025, 1, 1), end=date(2026, 6, 30)),
    DatasetPeriod(name="forward", start=date(2026, 7, 1), end=None),
)


def period_for(value: datetime) -> str:
    target = value.date()
    for period in PERIODS:
        if target >= period.start and (period.end is None or target <= period.end):
            return period.name
    raise ValueError("timestamp is before the registered sample")


class DataQualityReport(FrozenModel):
    generated_at: datetime
    filing_count: int
    unique_accessions: int
    duplicate_accessions: int
    incomplete_filings: int
    unmapped_symbols: int
    filings_by_period: dict[str, int]
    filings_by_form: dict[str, int]
    market_symbols: int
    missing_market_symbols: tuple[str, ...]
    coverage_record_count: int
    coverage_accessions: int
    coverage_by_status: dict[str, int]
    coverage_by_feed: dict[str, int]
    coverage_by_lag: dict[str, int]
    tradable_coverage_records: int
    missing_eligibility_records: int
    missing_feature_history_records: int
    insufficient_exit_horizon_records: int
    coverage_accounted: bool
    required_lag_minutes: tuple[int, ...] = REQUIRED_AVAILABILITY_LAGS
    tradable_coverage_by_lag: dict[str, int] = Field(default_factory=dict)
    filings_missing_lag_outcome: tuple[str, ...] = ()
    lag_accounting_complete: bool = False
    notes: tuple[str, ...]


def build_data_quality_report(
    filings: Iterable[FilingEvent],
    market_symbols: Iterable[str],
    coverage_records: Iterable[Any] = (),
    *,
    generated_at: datetime,
) -> DataQualityReport:
    records = list(filings)
    coverage = list(coverage_records)
    accessions = Counter(record.accession_number for record in records)
    normalized_market_symbols = {symbol.upper() for symbol in market_symbols}
    filing_symbols = {
        symbol.upper() for record in records for symbol in record.symbols if symbol.strip()
    }
    notes: list[str] = []
    if any(record.form == "8-K/A" for record in records):
        notes.append("8-K/A records are stored but excluded from trading")
    if not normalized_market_symbols:
        notes.append("No market data symbols supplied; coverage cannot be validated")
    if not coverage:
        notes.append("No backfill coverage records supplied; research readiness is unknown")
    coverage_accessions = {
        str(record.accession_number) for record in coverage if record.accession_number
    }
    scenario_records = [record for record in coverage if record.lag_minutes is not None]
    missing_feature_history = sum(
        record.feature_history is None or not record.feature_history.complete
        for record in scenario_records
    )
    missing_eligibility = sum(
        record.eligibility is None or bool(record.eligibility.missing_confirmations)
        for record in scenario_records
    )
    insufficient_exit_horizon = sum(
        record.evaluation_at is None
        or record.window_end is None
        or record.window_end - record.evaluation_at < timedelta(minutes=60)
        or record.scenario_covered is not True
        for record in scenario_records
    )
    filing_accessions = set(accessions)
    coverage_accounted = bool(coverage) and filing_accessions <= coverage_accessions
    if not coverage_accounted:
        notes.append("At least one filing has no explicit backfill coverage outcome")

    required_lags = REQUIRED_AVAILABILITY_LAGS
    lags_by_accession: dict[str, set[int]] = {}
    for record in scenario_records:
        lags_by_accession.setdefault(str(record.accession_number), set()).add(
            int(record.lag_minutes)
        )
    missing_lag_outcome = tuple(
        sorted(
            accession
            for accession in filing_accessions
            if not set(required_lags) <= lags_by_accession.get(accession, set())
        )
    )
    lag_accounting_complete = bool(records) and not missing_lag_outcome
    if missing_lag_outcome:
        notes.append(
            "At least one filing lacks a coverage outcome for every 1/3/5/10-minute lag"
        )
    return DataQualityReport(
        generated_at=generated_at,
        filing_count=len(records),
        unique_accessions=len(accessions),
        duplicate_accessions=sum(count - 1 for count in accessions.values() if count > 1),
        incomplete_filings=sum(not record.complete for record in records),
        unmapped_symbols=sum(not record.symbols for record in records),
        filings_by_period=dict(Counter(period_for(record.accepted_at) for record in records)),
        filings_by_form=dict(Counter(record.form for record in records)),
        market_symbols=len(normalized_market_symbols),
        missing_market_symbols=tuple(sorted(filing_symbols - normalized_market_symbols)),
        coverage_record_count=len(coverage),
        coverage_accessions=len(coverage_accessions),
        coverage_by_status=dict(Counter(record.status.value for record in coverage)),
        coverage_by_feed=dict(
            Counter(record.feed for record in scenario_records if record.feed is not None)
        ),
        coverage_by_lag=dict(
            Counter(str(record.lag_minutes) for record in scenario_records)
        ),
        tradable_coverage_records=sum(
            bool(record.tradable_coverage_complete) for record in scenario_records
        ),
        missing_eligibility_records=missing_eligibility,
        missing_feature_history_records=missing_feature_history,
        insufficient_exit_horizon_records=insufficient_exit_horizon,
        coverage_accounted=coverage_accounted,
        required_lag_minutes=required_lags,
        tradable_coverage_by_lag={
            str(lag): sum(
                bool(record.tradable_coverage_complete)
                for record in scenario_records
                if record.lag_minutes == lag
            )
            for lag in required_lags
        },
        filings_missing_lag_outcome=missing_lag_outcome,
        lag_accounting_complete=lag_accounting_complete,
        notes=tuple(notes),
    )


class ParquetMarketDataLake:
    """Append immutable, source-labelled partitions; never overwrite a partition."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def write_filings(self, filings: Iterable[FilingEvent], *, batch_id: str) -> tuple[Path, ...]:
        grouped: dict[tuple[str, int, int], list[FilingEvent]] = {}
        for filing in filings:
            accepted = filing.accepted_at
            quarter = ((accepted.month - 1) // 3) + 1
            key = (filing.source.value, accepted.year, quarter)
            grouped.setdefault(key, []).append(filing)
        written: list[Path] = []
        for (source, year, quarter), records in grouped.items():
            target = (
                self.root
                / "filings"
                / f"source={source}"
                / f"year={year}"
                / f"quarter={quarter}"
                / f"batch-{batch_id}.parquet"
            )
            if target.exists():
                raise FileExistsError(f"immutable partition already exists: {target}")
            target.parent.mkdir(parents=True, exist_ok=True)
            frame = pl.DataFrame([record.model_dump(mode="json") for record in records])
            frame.write_parquet(target, compression="zstd", statistics=True)
            written.append(target)
        return tuple(written)

    def write_bars(self, bars: Iterable[Bar], *, batch_id: str) -> tuple[Path, ...]:
        grouped: dict[tuple[str, str, date], list[Bar]] = {}
        for bar in bars:
            key = (bar.source.value, bar.symbol.upper(), bar.timestamp.date())
            grouped.setdefault(key, []).append(bar)
        written: list[Path] = []
        for (source, symbol, day), records in grouped.items():
            target = (
                self.root
                / "bars"
                / f"source={source}"
                / f"symbol={symbol}"
                / f"date={day.isoformat()}"
                / f"batch-{batch_id}.parquet"
            )
            if target.exists():
                raise FileExistsError(f"immutable partition already exists: {target}")
            target.parent.mkdir(parents=True, exist_ok=True)
            frame = pl.DataFrame([record.model_dump(mode="json") for record in records])
            frame.write_parquet(target, compression="zstd", statistics=True)
            written.append(target)
        return tuple(written)

    def write_quotes(self, quotes: Iterable[Quote], *, batch_id: str) -> tuple[Path, ...]:
        grouped: dict[tuple[str, str, date], list[Quote]] = {}
        for quote in quotes:
            key = (quote.source.value, quote.symbol.upper(), quote.timestamp.date())
            grouped.setdefault(key, []).append(quote)
        written: list[Path] = []
        for (source, symbol, day), records in grouped.items():
            target = (
                self.root
                / "quotes"
                / f"source={source}"
                / f"symbol={symbol}"
                / f"date={day.isoformat()}"
                / f"batch-{batch_id}.parquet"
            )
            if target.exists():
                raise FileExistsError(f"immutable partition already exists: {target}")
            target.parent.mkdir(parents=True, exist_ok=True)
            frame = pl.DataFrame([record.model_dump(mode="json") for record in records])
            frame.write_parquet(target, compression="zstd", statistics=True)
            written.append(target)
        return tuple(written)

    def read_filing(
        self,
        accession_number: str,
        *,
        source: DataSource = DataSource.SEC,
        quarter: str | None = None,
    ) -> FilingEvent:
        """Load one immutable filing and reject conflicting overlapping batches.

        ``quarter`` (``YYYY-Qn``) restricts the read to the one hive partition
        that can hold the record instead of scanning the whole collection.
        """

        root = self.root / "filings" / f"source={source.value}"
        if quarter is None:
            paths = tuple(root.glob("**/*.parquet"))
        else:
            match = _QUARTER.fullmatch(quarter.strip())
            if match is None:
                raise ValueError("filing quarter must be formatted as YYYY-Qn")
            partition = root / f"year={match.group(1)}" / f"quarter={match.group(2)}"
            paths = tuple(partition.glob("*.parquet"))
        matches = tuple(
            filing
            for filing in self._read_models(paths, FilingEvent)
            if filing.accession_number == accession_number
        )
        if not matches:
            raise LookupError(f"filing {accession_number} is not present in the data lake")
        unique = set(matches)
        if len(unique) != 1:
            raise ValueError(f"filing {accession_number} has conflicting immutable records")
        return next(iter(unique))

    def read_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        source: DataSource = DataSource.ALPACA_SIP,
        feed: str = "sip",
    ) -> tuple[Bar, ...]:
        """Load a validated, deduplicated point-in-time bar slice."""

        return self._read_market_records(
            Bar,
            symbol=symbol,
            start=start,
            end=end,
            source=source,
            feed=feed,
        )

    def read_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        source: DataSource = DataSource.ALPACA_SIP,
        feed: str = "sip",
    ) -> tuple[Quote, ...]:
        """Load a validated, deduplicated point-in-time quote slice."""

        return self._read_market_records(
            Quote,
            symbol=symbol,
            start=start,
            end=end,
            source=source,
            feed=feed,
        )

    def _read_market_records(
        self,
        model: type[MarketRecord],
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        source: DataSource,
        feed: str,
    ) -> tuple[MarketRecord, ...]:
        normalized = symbol.strip().upper()
        if not _SAFE_SYMBOL.fullmatch(normalized):
            raise ValueError("market-data symbol is syntactically unsafe")
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("market-data bounds must be timezone-aware")
        if end < start:
            raise ValueError("market-data end cannot predate start")
        normalized_feed = feed.strip().casefold()
        if not normalized_feed:
            raise ValueError("market-data feed must not be empty")
        collection = "bars" if model is Bar else "quotes"
        symbol_root = (
            self.root / collection / f"source={source.value}" / f"symbol={normalized}"
        )
        paths = _partition_paths(symbol_root, start, end)
        loaded = self._read_models(paths, model)
        if any(record.symbol.upper() != normalized for record in loaded):
            raise ValueError(f"{collection} partition contains a mismatched symbol")
        if any(record.source is not source for record in loaded):
            raise ValueError(f"{collection} partition contains a mismatched source")
        if any(record.feed.casefold() != normalized_feed for record in loaded):
            raise ValueError(f"{collection} partition contains a mismatched feed")
        selected = tuple(record for record in loaded if start <= record.timestamp <= end)
        by_timestamp: dict[datetime, MarketRecord] = {}
        for record in selected:
            previous = by_timestamp.get(record.timestamp)
            if previous is not None and previous != record:
                raise ValueError(
                    f"{collection} contain conflicting records at {record.timestamp.isoformat()}"
                )
            by_timestamp[record.timestamp] = record
        return tuple(by_timestamp[timestamp] for timestamp in sorted(by_timestamp))

    @staticmethod
    def _read_models[
        Record: (FilingEvent, Bar, Quote)
    ](paths: Iterable[Path], model: type[Record]) -> tuple[Record, ...]:
        records: list[Record] = []
        for path in sorted(paths):
            frame = pl.read_parquet(path, hive_partitioning=False)
            records.extend(model.model_validate(row) for row in frame.to_dicts())
        return tuple(records)

    def open_duckdb(self) -> duckdb.DuckDBPyConnection:
        connection = duckdb.connect(":memory:")
        filings_glob = (self.root / "filings" / "**" / "*.parquet").as_posix()
        bars_glob = (self.root / "bars" / "**" / "*.parquet").as_posix()
        quotes_glob = (self.root / "quotes" / "**" / "*.parquet").as_posix()
        if tuple((self.root / "filings").glob("**/*.parquet")):
            connection.read_parquet(filings_glob, hive_partitioning=True).create_view("filings")
        if tuple((self.root / "bars").glob("**/*.parquet")):
            connection.read_parquet(bars_glob, hive_partitioning=True).distinct().create_view(
                "bars"
            )
        if tuple((self.root / "quotes").glob("**/*.parquet")):
            connection.read_parquet(quotes_glob, hive_partitioning=True).distinct().create_view(
                "quotes"
            )
        return connection


def _partition_paths(symbol_root: Path, start: datetime, end: datetime) -> tuple[Path, ...]:
    """Return only the date partitions that can contain ``[start, end]``.

    Market data is partitioned by UTC date, so a point-in-time slice never has
    to open — or even stat — a partition outside its own window.
    """

    if not symbol_root.exists():
        return ()
    paths: list[Path] = []
    day = start.astimezone(UTC).date()
    last = end.astimezone(UTC).date()
    while day <= last:
        paths.extend((symbol_root / f"date={day.isoformat()}").glob("*.parquet"))
        day += timedelta(days=1)
    return tuple(paths)
