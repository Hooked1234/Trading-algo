from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from polars.exceptions import PolarsError

from event_trader.backfill import CoverageRecord, CoverageStatus
from event_trader.datasets import (
    ParquetMarketDataLake,
    build_data_quality_report,
    period_for,
)
from event_trader.domain import DataSource


def test_registered_sample_periods_are_frozen() -> None:
    assert period_for(datetime(2023, 12, 31, tzinfo=UTC)) == "development"
    assert period_for(datetime(2024, 1, 1, tzinfo=UTC)) == "validation"
    assert period_for(datetime(2026, 6, 30, tzinfo=UTC)) == "holdout"
    assert period_for(datetime(2026, 7, 1, tzinfo=UTC)) == "forward"


def test_quality_report_counts_duplicate_and_missing_symbol(filing, decision_time) -> None:
    duplicate = filing.model_copy(update={"event_id": "duplicate"})
    unmapped = filing.model_copy(
        update={
            "event_id": "unmapped",
            "accession_number": "0000320193-26-000019",
            "symbols": (),
        }
    )
    report = build_data_quality_report(
        [filing, duplicate, unmapped],
        [],
        generated_at=decision_time,
    )
    assert report.duplicate_accessions == 1
    assert report.unmapped_symbols == 1
    assert report.missing_market_symbols == ("AAPL",)
    assert not report.coverage_accounted


def test_quality_report_accounts_for_explicit_backfill_gaps(filing, decision_time) -> None:
    coverage = CoverageRecord(
        record_id="missing-symbol",
        quarter="2026-Q2",
        accession_number=filing.accession_number,
        status=CoverageStatus.MISSING_SYMBOL,
        recorded_at=decision_time,
    )

    report = build_data_quality_report(
        [filing],
        ["AAPL"],
        [coverage],
        generated_at=decision_time,
    )

    assert report.coverage_accounted
    assert report.coverage_by_status == {"missing_symbol": 1}
    assert report.tradable_coverage_records == 0


def test_quality_report_counts_non_contiguous_exit_coverage(filing, decision_time) -> None:
    coverage = CoverageRecord(
        record_id="incomplete-exit",
        quarter="2026-Q2",
        accession_number=filing.accession_number,
        symbol="AAPL",
        scenario="source_lag_5m_primary",
        lag_minutes=5,
        available_at=decision_time - timedelta(minutes=5),
        evaluation_at=decision_time,
        window_end=decision_time + timedelta(minutes=60),
        provider="alpaca",
        feed="sip",
        status=CoverageStatus.INSUFFICIENT_EXIT_COVERAGE,
        scenario_covered=False,
        recorded_at=decision_time,
    )

    report = build_data_quality_report(
        [filing],
        ["AAPL"],
        [coverage],
        generated_at=decision_time,
    )

    assert report.insufficient_exit_horizon_records == 1


def test_parquet_partitions_are_immutable(tmp_path, long_market) -> None:
    lake = ParquetMarketDataLake(tmp_path)
    first = lake.write_quotes([long_market.quote], batch_id="one")
    assert first[0].exists()
    with pytest.raises(FileExistsError):
        lake.write_quotes([long_market.quote], batch_id="one")
    connection = lake.open_duckdb()
    try:
        assert connection.execute("SELECT count(*) FROM quotes").fetchone()[0] == 1
    finally:
        connection.close()


def test_views_deduplicate_exact_records_from_overlapping_bundles(tmp_path, long_market) -> None:
    lake = ParquetMarketDataLake(tmp_path)
    lake.write_quotes([long_market.quote], batch_id="event-one")
    lake.write_quotes([long_market.quote], batch_id="event-two")

    assert len(tuple((tmp_path / "quotes").rglob("*.parquet"))) == 2
    connection = lake.open_duckdb()
    try:
        assert connection.execute("SELECT count(*) FROM quotes").fetchone()[0] == 1
    finally:
        connection.close()


def test_filing_partitions_are_queryable_and_immutable(tmp_path, filing) -> None:
    lake = ParquetMarketDataLake(tmp_path)
    written = lake.write_filings([filing], batch_id=filing.accession_number)

    assert len(written) == 1
    with pytest.raises(FileExistsError):
        lake.write_filings([filing], batch_id=filing.accession_number)
    connection = lake.open_duckdb()
    try:
        assert connection.sql("select count(*) from filings").fetchone() == (1,)
    finally:
        connection.close()


def test_data_lake_reads_exact_filing_and_deduplicated_market_slice(
    tmp_path, filing, long_market, decision_time
) -> None:
    lake = ParquetMarketDataLake(tmp_path)
    lake.write_filings([filing], batch_id="filing-one")
    lake.write_filings([filing], batch_id="filing-two")
    lake.write_quotes([long_market.quote], batch_id="quote-one")
    lake.write_quotes([long_market.quote], batch_id="quote-two")

    assert lake.read_filing(filing.accession_number) == filing
    assert lake.read_quotes(
        "aapl",
        start=decision_time - timedelta(seconds=5),
        end=decision_time,
        source=DataSource.REPLAY,
        feed="test-sip",
    ) == (long_market.quote,)


def test_data_lake_rejects_conflicting_market_records(tmp_path, long_market, decision_time) -> None:
    lake = ParquetMarketDataLake(tmp_path)
    conflict = long_market.quote.model_copy(update={"ask": long_market.quote.ask + Decimal("0.01")})
    lake.write_quotes([long_market.quote], batch_id="quote-one")
    lake.write_quotes([conflict], batch_id="quote-two")

    with pytest.raises(ValueError, match="conflicting records"):
        lake.read_quotes(
            "AAPL",
            start=decision_time - timedelta(seconds=5),
            end=decision_time,
            source=DataSource.REPLAY,
            feed="test-sip",
        )


def test_market_reads_only_open_the_requested_date_partitions(
    tmp_path, long_market, decision_time
) -> None:
    """A point-in-time slice must not touch partitions outside its window.

    An unreadable partition five days later proves the pruning: reading the
    decision day succeeds, reading across the poisoned day fails.
    """

    lake = ParquetMarketDataLake(tmp_path)
    lake.write_quotes([long_market.quote], batch_id="quote-one")
    poisoned_day = (decision_time + timedelta(days=5)).date().isoformat()
    poisoned = (
        tmp_path
        / "quotes"
        / f"source={DataSource.REPLAY.value}"
        / "symbol=AAPL"
        / f"date={poisoned_day}"
    )
    poisoned.mkdir(parents=True)
    (poisoned / "batch-corrupt.parquet").write_bytes(b"not a parquet file")

    assert lake.read_quotes(
        "AAPL",
        start=decision_time - timedelta(seconds=5),
        end=decision_time,
        source=DataSource.REPLAY,
        feed="test-sip",
    ) == (long_market.quote,)

    with pytest.raises(PolarsError):
        lake.read_quotes(
            "AAPL",
            start=decision_time - timedelta(seconds=5),
            end=decision_time + timedelta(days=6),
            source=DataSource.REPLAY,
            feed="test-sip",
        )


def test_filing_reads_can_be_pruned_to_one_quarter(tmp_path, filing) -> None:
    lake = ParquetMarketDataLake(tmp_path)
    lake.write_filings([filing], batch_id="filing-one")

    assert lake.read_filing(filing.accession_number, quarter="2026-Q3") == filing
    with pytest.raises(LookupError):
        lake.read_filing(filing.accession_number, quarter="2026-Q1")
    with pytest.raises(ValueError, match="YYYY-Qn"):
        lake.read_filing(filing.accession_number, quarter="2026-3")


def _lag_coverage(filing, decision_time, lag_minutes: int) -> CoverageRecord:
    available_at = filing.accepted_at + timedelta(minutes=lag_minutes)
    return CoverageRecord(
        record_id=f"{filing.accession_number}:lag-{lag_minutes}m",
        quarter="2026-Q3",
        accession_number=filing.accession_number,
        symbol="AAPL",
        scenario=f"source_lag_{lag_minutes}m",
        lag_minutes=lag_minutes,
        available_at=available_at,
        evaluation_at=available_at + timedelta(minutes=5),
        window_end=available_at + timedelta(minutes=65),
        provider="alpaca",
        feed="sip",
        status=CoverageStatus.INSUFFICIENT_EXIT_COVERAGE,
        scenario_covered=False,
        recorded_at=decision_time,
    )


def test_quality_report_requires_an_outcome_for_every_registered_lag(filing, decision_time) -> None:
    partial = [_lag_coverage(filing, decision_time, lag) for lag in (1, 3, 5)]

    incomplete = build_data_quality_report([filing], ["AAPL"], partial, generated_at=decision_time)
    complete = build_data_quality_report(
        [filing],
        ["AAPL"],
        [*partial, _lag_coverage(filing, decision_time, 10)],
        generated_at=decision_time,
    )

    assert incomplete.required_lag_minutes == (1, 3, 5, 10)
    assert incomplete.lag_accounting_complete is False
    assert incomplete.filings_missing_lag_outcome == (filing.accession_number,)
    assert complete.lag_accounting_complete is True
    assert complete.filings_missing_lag_outcome == ()
    assert complete.tradable_coverage_by_lag == {"1": 0, "3": 0, "5": 0, "10": 0}
