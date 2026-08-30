from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import exchange_calendars as xcals
import pytest

from event_trader.backfill import (
    BACKFILL_END,
    BACKFILL_START,
    SOURCE_AVAILABILITY_SCENARIOS,
    BackfillCheckpoint,
    BackfillConfig,
    BackfillMarketDataError,
    BackfillStateError,
    CoverageRecord,
    CoverageStatus,
    HistoricalBackfillRunner,
    JsonBackfillStore,
    PointInTimeEligibility,
    SecIndexEntry,
    parse_sec_quarter_index,
    plan_sec_quarter_indexes,
)
from event_trader.datasets import ParquetMarketDataLake
from event_trader.domain import Bar, DataSource, FilingEvent, Quote

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
ACCESSION_A = "0000000001-19-000001"
ACCESSION_B = "0000000002-19-000002"


def _master_index(*rows: str) -> bytes:
    return (
        "Description:           Master Index of EDGAR Dissemination Feed\n"
        "Last Data Received:    March 31, 2019\n"
        "Comments:              webmaster@sec.gov\n"
        "Anonymous FTP:         ftp://ftp.sec.gov/edgar/\n"
        "\n"
        "CIK|Company Name|Form Type|Date Filed|Filename\n"
        "--------------------------------------------------------------\n"
        f"{'\n'.join(rows)}\n"
    ).encode("latin-1")


def _master_row(
    cik: str,
    company: str,
    form: str,
    filed_on: str,
    accession: str,
) -> str:
    return f"{cik}|{company}|{form}|{filed_on}|edgar/data/{cik}/{accession}.txt"


def _form_index(*rows: tuple[str, str, str, str, str]) -> bytes:
    header = f"{'Form Type':<12}{'Company Name':<62}{'CIK':<12}{'Date Filed':<12}File Name"
    rendered = [
        f"{form:<12}{company:<62}{cik:<12}{filed_on:<12}{filename}"
        for form, company, cik, filed_on, filename in rows
    ]
    return (f"Form Index\n{header}\n{'-' * 130}\n{'\n'.join(rendered)}\n").encode("latin-1")


def _event(
    accession: str,
    *,
    cik: str,
    form: str = "8-K",
    symbols: tuple[str, ...] = ("AAPL",),
    accepted_at: datetime | None = None,
) -> FilingEvent:
    accepted = accepted_at or datetime(2019, 1, 3, 14, 30, tzinfo=UTC)
    return FilingEvent(
        event_id=accession,
        accession_number=accession,
        cik=cik,
        form=form,
        symbols=symbols,
        accepted_at=accepted,
        first_seen_at=accepted,
        retrieved_at=accepted,
    )


def _confirmed_eligibility(event: FilingEvent, symbol: str) -> PointInTimeEligibility:
    return PointInTimeEligibility(
        accession_number=event.accession_number,
        symbol=symbol,
        as_of=event.accepted_at,
        source="historical-security-master",
        common_stock=True,
        us_listing=True,
        corporate_actions_complete=True,
    )


class MemoryStore:
    def __init__(self) -> None:
        self.checkpoints: dict[str, BackfillCheckpoint] = {}
        self.coverage: dict[str, CoverageRecord] = {}

    async def load_checkpoint(self, quarter: str) -> BackfillCheckpoint | None:
        return self.checkpoints.get(quarter)

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        self.checkpoints[checkpoint.quarter] = checkpoint

    async def save_coverage(self, record: CoverageRecord) -> None:
        self.coverage[record.record_id] = record


class FakeMarketDataProvider:
    def __init__(
        self,
        *,
        empty: bool = False,
        fail_symbols: set[str] | None = None,
    ) -> None:
        self.empty = empty
        self.fail_symbols = fail_symbols or set()
        self.bar_calls: list[tuple[str, datetime, datetime, str, str]] = []
        self.quote_calls: list[tuple[str, datetime, datetime, str]] = []

    def get_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        feed: str = "sip",
    ) -> tuple[Bar, ...]:
        self.bar_calls.append((symbol, start, end, timeframe, feed))
        if symbol in self.fail_symbols:
            raise RuntimeError("temporary provider failure")
        if self.empty:
            return ()
        event_window_timestamp = end - timedelta(minutes=60)
        return (
            Bar(
                symbol=symbol,
                timestamp=event_window_timestamp,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.50"),
                volume=1_000,
                vwap=Decimal("100.25"),
                source=DataSource.ALPACA_SIP,
                feed=feed,
            ),
        )

    def get_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str = "sip",
    ) -> tuple[Quote, ...]:
        self.quote_calls.append((symbol, start, end, feed))
        if symbol in self.fail_symbols:
            raise RuntimeError("temporary provider failure")
        if self.empty:
            return ()
        return tuple(
            Quote(
                symbol=symbol,
                timestamp=start + timedelta(minutes=offset),
                bid=Decimal("100.00"),
                ask=Decimal("100.10"),
                bid_size=100,
                ask_size=100,
                source=DataSource.ALPACA_SIP,
                feed=feed,
            )
            for offset in (0, 2, 4, 9, 14)
        )

    def get_latest_quote(self, symbol: str, *, feed: str = "iex") -> Quote:
        raise AssertionError("historical backfill must not request a latest quote")


class CompleteFeatureMarketDataProvider(FakeMarketDataProvider):
    def get_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        feed: str = "sip",
    ) -> tuple[Bar, ...]:
        self.bar_calls.append((symbol, start, end, timeframe, feed))
        calendar = xcals.get_calendar("XNYS")
        reference = calendar.date_to_session("2019-01-03")
        sessions = []
        cursor = reference
        for _ in range(20):
            cursor = calendar.previous_session(cursor)
            sessions.append(cursor)
        timestamps = []
        for session in reversed(sessions):
            opening = calendar.session_open(session).to_pydatetime().astimezone(UTC)
            closing = calendar.session_close(session).to_pydatetime().astimezone(UTC)
            session_minutes = int((closing - opening).total_seconds() // 60)
            timestamps.extend(
                opening + timedelta(minutes=minute) for minute in range(1, session_minutes + 1)
            )
        reference_open = calendar.session_open(reference).to_pydatetime().astimezone(UTC)
        timestamps.extend(reference_open + timedelta(minutes=minute) for minute in range(1, 76))
        return tuple(
            Bar(
                symbol=symbol,
                timestamp=timestamp,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.50"),
                volume=1_000,
                vwap=Decimal("100.25"),
                source=DataSource.ALPACA_SIP,
                feed=feed,
            )
            for timestamp in sorted(set(timestamps))
            if start <= timestamp <= end
        )

    def get_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str = "sip",
    ) -> tuple[Quote, ...]:
        self.quote_calls.append((symbol, start, end, feed))
        count = int((end - start).total_seconds() // 60)
        return tuple(
            Quote(
                symbol=symbol,
                timestamp=start + timedelta(minutes=minute),
                bid=Decimal("100.00"),
                ask=Decimal("100.10"),
                bid_size=100,
                ask_size=100,
                source=DataSource.ALPACA_SIP,
                feed=feed,
            )
            for minute in range(count + 1)
        )


def test_parsers_enumerate_only_exact_8k_and_amendments() -> None:
    master = _master_index(
        _master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A),
        _master_row("2", "Beta Corp", "8-K/A", "2019-01-04", ACCESSION_B),
        _master_row("3", "Foreign Corp", "6-K", "2019-01-04", "0000000003-19-000003"),
        _master_row("4", "Lookalike Corp", "8-K12B", "2019-01-05", "0000000004-19-000004"),
    )
    form = _form_index(
        (
            "8-K",
            "Alpha Corp",
            "1",
            "2019-01-03",
            f"edgar/data/1/{ACCESSION_A}.txt",
        ),
        (
            "8-K/A",
            "Beta Corp",
            "2",
            "2019-01-04",
            f"edgar/data/2/{ACCESSION_B}.txt",
        ),
        (
            "10-K",
            "Ignored Corp",
            "3",
            "2019-01-05",
            "edgar/data/3/0000000003-19-000003.txt",
        ),
    )

    master_entries = parse_sec_quarter_index(master)
    form_entries = parse_sec_quarter_index(form)

    assert [(entry.form, entry.accession_number) for entry in master_entries] == [
        ("8-K", ACCESSION_A),
        ("8-K/A", ACCESSION_B),
    ]
    assert [(entry.form, entry.accession_number) for entry in form_entries] == [
        ("8-K", ACCESSION_A),
        ("8-K/A", ACCESSION_B),
    ]
    assert master_entries[0].archive_url.endswith(f"/edgar/data/1/{ACCESSION_A}.txt")
    assert form_entries[0].index_kind == "form"


def test_default_date_range_splits_into_inclusive_sec_quarters() -> None:
    plans = plan_sec_quarter_indexes()

    assert BACKFILL_START == date(2019, 1, 1)
    assert BACKFILL_END == date(2026, 6, 30)
    assert len(plans) == 30
    assert (plans[0].quarter.key, plans[0].start, plans[0].end) == (
        "2019-Q1",
        date(2019, 1, 1),
        date(2019, 3, 31),
    )
    assert (plans[-1].quarter.key, plans[-1].start, plans[-1].end) == (
        "2026-Q2",
        date(2026, 4, 1),
        date(2026, 6, 30),
    )
    assert plans[0].master_url.endswith("/2019/QTR1/master.idx")
    assert plans[-1].form_url.endswith("/2026/QTR2/form.idx")

    partial = plan_sec_quarter_indexes(date(2024, 2, 10), date(2024, 4, 2))
    assert [(plan.start, plan.end) for plan in partial] == [
        (date(2024, 2, 10), date(2024, 3, 31)),
        (date(2024, 4, 1), date(2024, 4, 2)),
    ]


def test_lag_scenarios_are_explicit_and_primary_is_five_minutes() -> None:
    event = _event(ACCESSION_A, cik="1")

    assert [scenario.lag_minutes for scenario in SOURCE_AVAILABILITY_SCENARIOS] == [
        1,
        3,
        5,
        10,
    ]
    assert [scenario.available_at(event) for scenario in SOURCE_AVAILABILITY_SCENARIOS] == [
        datetime(2019, 1, 3, 14, 31, tzinfo=UTC),
        datetime(2019, 1, 3, 14, 33, tzinfo=UTC),
        datetime(2019, 1, 3, 14, 35, tzinfo=UTC),
        datetime(2019, 1, 3, 14, 40, tzinfo=UTC),
    ]
    assert SOURCE_AVAILABILITY_SCENARIOS[2].name == "source_lag_5m_primary"


def test_backfill_config_requires_feature_lookback_and_all_lags() -> None:
    with pytest.raises(ValueError, match="45 calendar days"):
        BackfillConfig(lookback_calendar_days=44)
    with pytest.raises(ValueError, match="1, 3, 5 and 10"):
        BackfillConfig(availability_scenarios=SOURCE_AVAILABILITY_SCENARIOS[:3])
    with pytest.raises(ValueError, match="at least 60 minutes"):
        BackfillConfig(market_horizon_minutes=59)
    with pytest.raises(ValueError, match="SIP feed"):
        BackfillConfig(feed="iex")
    with pytest.raises(ValueError, match="SPY"):
        BackfillConfig(benchmark_symbol="QQQ")


@pytest.mark.asyncio
async def test_future_dated_eligibility_evidence_is_rejected(tmp_path: Path) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    market = FakeMarketDataProvider()

    def future_evidence(event: FilingEvent, symbol: str) -> PointInTimeEligibility:
        return _confirmed_eligibility(event, symbol).model_copy(
            update={"as_of": event.accepted_at + timedelta(seconds=1)}
        )

    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        eligibility_resolver=future_evidence,
        market_data=market,
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=MemoryStore(),
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    with pytest.raises(BackfillStateError, match="future-dated"):
        await runner.run()

    assert market.bar_calls == []
    assert market.quote_calls == []


@pytest.mark.asyncio
async def test_runner_resumes_per_accession_and_deduplicates_index_rows(
    tmp_path: Path,
) -> None:
    row_a = _master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A)
    payload = _master_index(
        row_a,
        row_a,
        _master_row("2", "Beta Corp", "8-K", "2019-01-04", ACCESSION_B),
    )
    fetch_calls: list[str] = []
    resolver_calls: list[str] = []
    store = MemoryStore()
    lake = ParquetMarketDataLake(tmp_path / "lake")
    events = {
        ACCESSION_A: _event(ACCESSION_A, cik="1", symbols=("AAPL",)),
        ACCESSION_B: _event(ACCESSION_B, cik="2", symbols=("MSFT",)),
    }

    async def fetch(url: str) -> bytes:
        fetch_calls.append(url)
        return payload

    async def resolve(entry: SecIndexEntry) -> FilingEvent:
        resolver_calls.append(entry.accession_number)
        return events[entry.accession_number]

    config = BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31))
    failing_market = FakeMarketDataProvider(fail_symbols={"MSFT"})
    first_runner = HistoricalBackfillRunner(
        index_fetcher=fetch,
        filing_resolver=resolve,
        market_data=failing_market,
        data_lake=lake,
        store=store,
        config=config,
        clock=lambda: NOW,
    )

    with pytest.raises(BackfillMarketDataError):
        await first_runner.run()

    checkpoint = store.checkpoints["2019-Q1"]
    assert checkpoint.processed_accessions == (ACCESSION_A,)
    assert checkpoint.completed is False

    second_market = FakeMarketDataProvider()
    second_runner = HistoricalBackfillRunner(
        index_fetcher=fetch,
        filing_resolver=resolve,
        market_data=second_market,
        data_lake=lake,
        store=store,
        config=config,
        clock=lambda: NOW,
    )
    summary = await second_runner.run()

    assert summary.discovered_accessions == 2
    assert summary.resumed_accessions == 1
    assert summary.processed_accessions == 1
    assert store.checkpoints["2019-Q1"].completed is True
    assert resolver_calls == [ACCESSION_A, ACCESSION_B, ACCESSION_B]
    assert len(fetch_calls) == 2
    assert [call[0] for call in second_market.bar_calls] == ["MSFT", "SPY"]
    assert len(second_market.quote_calls) == 1
    connection = lake.open_duckdb()
    try:
        assert connection.execute("SELECT count(*) FROM bars").fetchone()[0] == 3
        assert connection.execute("SELECT count(*) FROM quotes").fetchone()[0] == 10
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_missing_filing_symbol_is_coverage_not_a_market_request(
    tmp_path: Path,
) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    market = FakeMarketDataProvider()

    async def resolve(_: SecIndexEntry) -> FilingEvent:
        return _event(ACCESSION_A, cik="1", symbols=())

    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=resolve,
        market_data=market,
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )
    summary = await runner.run()

    assert summary.coverage_records == 1
    assert market.bar_calls == []
    assert market.quote_calls == []
    record = next(iter(store.coverage.values()))
    assert record.status is CoverageStatus.MISSING_SYMBOL
    assert record.symbol is None
    assert record.bar_count == record.quote_count == 0


@pytest.mark.asyncio
async def test_empty_market_payload_creates_coverage_for_every_lag_without_returns(
    tmp_path: Path,
) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    market = FakeMarketDataProvider(empty=True)

    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        eligibility_resolver=_confirmed_eligibility,
        market_data=market,
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )
    await runner.run()

    records = sorted(store.coverage.values(), key=lambda record: record.lag_minutes or 0)
    assert [record.lag_minutes for record in records] == [1, 3, 5, 10]
    assert all(record.status is CoverageStatus.MISSING_BARS_AND_QUOTES for record in records)
    assert all(record.bar_count == 0 and record.quote_count == 0 for record in records)
    assert [call[0] for call in market.bar_calls] == ["AAPL", "SPY"]
    expected_start = datetime(2018, 11, 19, 14, 30, tzinfo=UTC)
    expected_end = datetime(2019, 1, 3, 15, 45, tzinfo=UTC)
    assert all(call[1] == expected_start and call[2] == expected_end for call in market.bar_calls)
    assert market.quote_calls == [
        (
            "AAPL",
            datetime(2019, 1, 3, 14, 31, tzinfo=UTC),
            expected_end,
            "sip",
        )
    ]
    assert all(record.provider == "alpaca" and record.feed == "sip" for record in records)
    assert all(record.bundle_start == expected_start for record in records)
    assert all(record.bundle_end == expected_end for record in records)
    assert all(record.feature_history is not None for record in records)
    assert not (tmp_path / "lake" / "bars").exists()
    assert not (tmp_path / "lake" / "quotes").exists()


@pytest.mark.asyncio
async def test_after_hours_bundle_reaches_next_session_exit_window(tmp_path: Path) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    accepted_at = datetime(2019, 1, 3, 22, 0, tzinfo=UTC)
    store = MemoryStore()
    market = FakeMarketDataProvider(empty=True)
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(
            ACCESSION_A,
            cik="1",
            accepted_at=accepted_at,
        ),
        eligibility_resolver=_confirmed_eligibility,
        market_data=market,
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    expected_evaluation = datetime(2019, 1, 4, 14, 40, tzinfo=UTC)
    expected_end = expected_evaluation + timedelta(minutes=60)
    records = tuple(store.coverage.values())
    assert all(record.evaluation_at == expected_evaluation for record in records)
    assert all(record.window_end == expected_end for record in records)
    assert all(record.bundle_end == expected_end for record in records)
    assert all(call[2] == expected_end for call in market.bar_calls)
    assert market.quote_calls[0][1] == accepted_at + timedelta(minutes=1)
    assert market.quote_calls[0][2] == expected_end


@pytest.mark.asyncio
async def test_one_bundle_covers_all_scenarios_and_loads_spy(tmp_path: Path) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    market = FakeMarketDataProvider()
    lake = ParquetMarketDataLake(tmp_path / "lake")
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        market_data=market,
        data_lake=lake,
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    records = sorted(store.coverage.values(), key=lambda record: record.lag_minutes or 0)
    assert sorted(call[0] for call in market.bar_calls) == ["AAPL", "SPY"]
    assert len(market.quote_calls) == 1
    assert [record.quote_count for record in records] == [2, 2, 2, 1]
    assert all(record.bar_count == 1 for record in records)
    assert [record.evaluation_at for record in records] == [
        datetime(2019, 1, 3, 14, 40, tzinfo=UTC),
        datetime(2019, 1, 3, 14, 40, tzinfo=UTC),
        datetime(2019, 1, 3, 14, 40, tzinfo=UTC),
        datetime(2019, 1, 3, 14, 45, tzinfo=UTC),
    ]
    assert all(not record.scenario_covered for record in records)
    assert all(record.status is CoverageStatus.INSUFFICIENT_EXIT_COVERAGE for record in records)
    assert all(record.bundle_bar_count == 1 for record in records)
    assert all(record.bundle_quote_count == 5 for record in records)
    assert all(record.benchmark_bar_count == 1 for record in records)
    assert all(record.feature_history and record.feature_history.missing for record in records)
    assert all(record.eligibility is not None for record in records)
    assert all(
        record.eligibility and record.eligibility.missing_confirmations for record in records
    )
    assert all(not record.tradable_coverage_complete for record in records)
    assert not any("lag" in path.name for path in (tmp_path / "lake" / "bars").rglob("*.parquet"))
    connection = lake.open_duckdb()
    try:
        assert connection.execute("SELECT count(*) FROM bars").fetchone()[0] == 2
        assert connection.execute("SELECT count(*) FROM quotes").fetchone()[0] == 5
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_provider_duplicates_are_removed_before_parquet_write(tmp_path: Path) -> None:
    class DuplicateMarket(FakeMarketDataProvider):
        def get_bars(self, *args: object, **kwargs: object) -> tuple[Bar, ...]:
            records = super().get_bars(*args, **kwargs)  # type: ignore[arg-type]
            return (*records, *records)

        def get_quotes(self, *args: object, **kwargs: object) -> tuple[Quote, ...]:
            records = super().get_quotes(*args, **kwargs)  # type: ignore[arg-type]
            return (*records, *records)

    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    lake = ParquetMarketDataLake(tmp_path / "lake")
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        market_data=DuplicateMarket(),
        data_lake=lake,
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    records = tuple(store.coverage.values())
    assert all(record.bundle_bar_count == 1 for record in records)
    assert all(record.bundle_quote_count == 5 for record in records)
    connection = lake.open_duckdb()
    try:
        assert connection.execute("SELECT count(*) FROM bars").fetchone()[0] == 2
        assert connection.execute("SELECT count(*) FROM quotes").fetchone()[0] == 5
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_complete_point_in_time_history_is_reported_per_scenario(
    tmp_path: Path,
) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    market = CompleteFeatureMarketDataProvider()
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        eligibility_resolver=_confirmed_eligibility,
        market_data=market,
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    records = tuple(store.coverage.values())
    assert len(records) == 4
    assert all(record.status is CoverageStatus.AVAILABLE for record in records), records
    assert all(record.scenario_covered for record in records)
    assert all(record.tradable_coverage_complete for record in records)
    assert all(record.eligibility and record.eligibility.confirmed_eligible for record in records)
    for record in records:
        history = record.feature_history
        assert history is not None
        assert history.complete
        assert history.missing == ()
        assert history.symbol_previous_sessions == 20
        assert history.benchmark_previous_sessions == 20
        assert history.symbol_same_slot_sessions == 20
        assert history.benchmark_same_slot_sessions == 20
        assert history.atr_source_minutes >= 75


@pytest.mark.asyncio
async def test_missing_historical_security_eligibility_is_fail_closed(
    tmp_path: Path,
) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        market_data=CompleteFeatureMarketDataProvider(),
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    records = tuple(store.coverage.values())
    assert all(
        record.status is CoverageStatus.MISSING_POINT_IN_TIME_ELIGIBILITY for record in records
    ), records
    assert all(record.feature_history and record.feature_history.complete for record in records)
    assert all(record.eligibility is not None for record in records)
    assert all(
        record.eligibility
        and record.eligibility.missing_confirmations
        == ("COMMON_STOCK", "US_LISTING", "CORPORATE_ACTIONS")
        for record in records
    )
    assert all(not record.tradable_coverage_complete for record in records)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bars_empty", "quotes_empty", "expected_status"),
    [
        (True, False, CoverageStatus.MISSING_BARS),
        (False, True, CoverageStatus.MISSING_QUOTES),
    ],
)
async def test_partial_market_payload_remains_an_explicit_coverage_gap(
    tmp_path: Path,
    bars_empty: bool,
    quotes_empty: bool,
    expected_status: CoverageStatus,
) -> None:
    class PartialMarket(FakeMarketDataProvider):
        def get_bars(self, *args: object, **kwargs: object) -> tuple[Bar, ...]:
            if bars_empty:
                super().get_bars(*args, **kwargs)  # type: ignore[arg-type]
                return ()
            return super().get_bars(*args, **kwargs)  # type: ignore[arg-type]

        def get_quotes(self, *args: object, **kwargs: object) -> tuple[Quote, ...]:
            if quotes_empty:
                super().get_quotes(*args, **kwargs)  # type: ignore[arg-type]
                return ()
            return super().get_quotes(*args, **kwargs)  # type: ignore[arg-type]

    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=lambda _: _event(ACCESSION_A, cik="1"),
        market_data=PartialMarket(),
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    assert {record.status for record in store.coverage.values()} == {expected_status}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolved", "symbols", "expected_status"),
    [
        (False, (), CoverageStatus.MISSING_FILING),
        (True, ("../AAPL",), CoverageStatus.INVALID_SYMBOL),
    ],
)
async def test_unresolvable_or_unsafe_historical_mapping_is_coverage(
    tmp_path: Path,
    resolved: bool,
    symbols: tuple[str, ...],
    expected_status: CoverageStatus,
) -> None:
    payload = _master_index(_master_row("1", "Alpha Corp", "8-K", "2019-01-03", ACCESSION_A))
    store = MemoryStore()
    market = FakeMarketDataProvider()
    runner = HistoricalBackfillRunner(
        index_fetcher=lambda _: payload,
        filing_resolver=(
            (lambda _: _event(ACCESSION_A, cik="1", symbols=symbols))
            if resolved
            else (lambda _: None)
        ),
        market_data=market,
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    await runner.run()

    assert next(iter(store.coverage.values())).status is expected_status
    assert market.bar_calls == []
    assert market.quote_calls == []


@pytest.mark.asyncio
async def test_completed_quarter_resumes_without_fetching_or_rewriting(
    tmp_path: Path,
) -> None:
    store = MemoryStore()
    store.checkpoints["2019-Q1"] = BackfillCheckpoint(
        quarter="2019-Q1",
        range_start=date(2019, 1, 1),
        range_end=date(2019, 3, 31),
        processed_accessions=(ACCESSION_A,),
        completed=True,
        updated_at=NOW,
    )

    def unexpected_fetch(_: str) -> bytes:
        raise AssertionError("completed quarters must not be fetched")

    runner = HistoricalBackfillRunner(
        index_fetcher=unexpected_fetch,
        filing_resolver=lambda _: None,
        market_data=FakeMarketDataProvider(),
        data_lake=ParquetMarketDataLake(tmp_path / "lake"),
        store=store,
        config=BackfillConfig(start=date(2019, 1, 1), end=date(2019, 3, 31)),
        clock=lambda: NOW,
    )

    summary = await runner.run()

    assert summary.skipped_completed_quarters == 1
    assert summary.resumed_accessions == 1
    assert summary.processed_accessions == 0


@pytest.mark.asyncio
async def test_json_store_upserts_coverage_and_restores_checkpoint(tmp_path: Path) -> None:
    store = JsonBackfillStore(tmp_path / "state" / "backfill.json")
    checkpoint = BackfillCheckpoint(
        quarter="2019-Q1",
        range_start=date(2019, 1, 1),
        range_end=date(2019, 3, 31),
        processed_accessions=(ACCESSION_A,),
        updated_at=NOW,
    )
    record = CoverageRecord(
        record_id=f"2019-Q1:{ACCESSION_A}:_none:filing",
        quarter="2019-Q1",
        accession_number=ACCESSION_A,
        status=CoverageStatus.MISSING_SYMBOL,
        recorded_at=NOW,
    )

    await store.save_checkpoint(checkpoint)
    await store.save_coverage(record)
    await store.save_coverage(record.model_copy(update={"detail": "still missing"}))

    reloaded = JsonBackfillStore(tmp_path / "state" / "backfill.json")
    assert await reloaded.load_checkpoint("2019-Q1") == checkpoint
    coverage = await reloaded.list_coverage()
    assert len(coverage) == 1
    assert coverage[0].detail == "still missing"
