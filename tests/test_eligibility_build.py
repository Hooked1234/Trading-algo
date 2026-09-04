from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from event_trader.domain import DataSource, FilingEvent
from event_trader.eligibility import CsvEligibilityResolver
from event_trader.eligibility_build import (
    MANIFEST_HEADER,
    CoverPageFactCollector,
    CoverPageFactRecord,
    CoverPageSecurityRecord,
    _append_record,
    build_eligibility_intervals,
    classify_common_stock,
    classify_us_listing,
    observations_from_record,
    read_fact_records,
    summarize,
    write_eligibility_manifest,
)
from event_trader.providers.sec_history import (
    parse_cover_page_securities,
    parse_historical_submission,
)


def _registered_class(context: bytes, symbol: bytes, title: bytes) -> bytes:
    return (
        b'<dei:TradingSymbol contextRef="' + context + b'">' + symbol + b"</dei:TradingSymbol>"
        b'<dei:Security12bTitle contextRef="' + context + b'">' + title + b"</dei:Security12bTitle>"
        b'<dei:SecurityExchangeName contextRef="' + context + b'">NASDAQ'
        b"</dei:SecurityExchangeName>"
    )


# One SPAC filer with three registered classes; the unit and the warrant both
# name common stock inside their own titles.
_SPAC_COVER_PAGE = (
    b"<ACCEPTANCE-DATETIME>20210415090000\n<ITEMS>8.01\n"
    + _registered_class(b"cA", b"EXMP", b"Class A common stock, par value $0.0001 per share")
    + _registered_class(
        b"cU",
        b"EXMPU",
        b"Units, each consisting of one share of Class A common stock and one-half of one warrant",
    )
    + _registered_class(
        b"cW", b"EXMPW", b"Warrants, each exercisable for one share of Class A common stock"
    )
)


def _record(
    *,
    accession: str = "0000320193-24-000001",
    cik: str = "320193",
    accepted_at: datetime,
    securities: tuple[CoverPageSecurityRecord, ...],
) -> CoverPageFactRecord:
    return CoverPageFactRecord(
        accession_number=accession,
        cik=cik,
        form="8-K",
        filed_on=accepted_at.date(),
        accepted_at=accepted_at,
        securities=securities,
    )


def _common(symbol: str = "AAPL") -> CoverPageSecurityRecord:
    return CoverPageSecurityRecord(
        symbol=symbol,
        security_title="Common Stock, $0.00001 par value per share",
        exchange="Nasdaq Global Select Market",
    )


def test_cover_page_contexts_keep_each_registered_class_separate() -> None:
    securities = parse_cover_page_securities(_SPAC_COVER_PAGE)

    by_symbol = {security.symbol: security for security in securities}
    assert set(by_symbol) == {"EXMP", "EXMPU", "EXMPW"}
    assert classify_common_stock(by_symbol["EXMP"].security_title) is True
    # The unit and the warrant both name common stock in their own titles.
    assert classify_common_stock(by_symbol["EXMPU"].security_title) is False
    assert classify_common_stock(by_symbol["EXMPW"].security_title) is False


def test_inline_facts_are_read_and_stripped_of_presentation_markup() -> None:
    payload = (
        b"<ACCEPTANCE-DATETIME>20240702101530\n"
        b'<ix:nonNumeric name="dei:TradingSymbol" contextRef="c1">'
        b"<span>AAPL</span></ix:nonNumeric>"
        b'<ix:nonNumeric contextRef="c1" name="dei:Security12bTitle">'
        b"Common Stock, $0.00001 par&#160;value</ix:nonNumeric>"
        b'<ix:nonNumeric name="dei:SecurityExchangeName" contextRef="c1">'
        b"NASDAQ</ix:nonNumeric>"
    )

    (security,) = parse_cover_page_securities(payload)

    assert security.symbol == "AAPL"
    assert security.security_title == "Common Stock, $0.00001 par value"
    assert security.exchange == "NASDAQ"


def test_symbol_reported_with_conflicting_titles_keeps_the_fact_unknown() -> None:
    payload = (
        b"<ACCEPTANCE-DATETIME>20240702101530\n"
        b'<dei:TradingSymbol contextRef="c1">AAPL</dei:TradingSymbol>'
        b'<dei:Security12bTitle contextRef="c1">Common Stock</dei:Security12bTitle>'
        b'<dei:TradingSymbol contextRef="c2">AAPL</dei:TradingSymbol>'
        b'<dei:Security12bTitle contextRef="c2">Warrants</dei:Security12bTitle>'
    )

    (security,) = parse_cover_page_securities(payload)

    assert security.symbol == "AAPL"
    assert security.security_title is None
    assert classify_common_stock(security.security_title) is None


def test_a_contradiction_survives_every_later_agreeing_context() -> None:
    # Three contexts for one symbol, the middle one contradicting. Merging the
    # facts pairwise would let the third context overwrite the conflict, and the
    # outcome would depend on the order the filing happens to tag them in.
    payload = (
        b"<ACCEPTANCE-DATETIME>20240702101530\n"
        + _registered_class(b"c1", b"AAPL", b"Common Stock")
        + _registered_class(b"c2", b"AAPL", b"Warrants")
        + _registered_class(b"c3", b"AAPL", b"Common Stock")
    )

    (security,) = parse_cover_page_securities(payload)

    assert security.symbol == "AAPL"
    assert security.security_title is None
    assert classify_common_stock(security.security_title) is None


def test_submission_metadata_still_reports_the_filing_symbols() -> None:
    parsed = parse_historical_submission(_SPAC_COVER_PAGE)

    assert parsed.symbols == ("EXMP", "EXMPU", "EXMPW")
    assert parsed.items == ("8.01",)
    assert parsed.accepted_at == datetime(2021, 4, 15, 13, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Common Stock, par value $0.01", True),
        ("Common Shares of Beneficial Interest", True),
        ("Ordinary Shares, no par value", True),
        ("6.5% Series A Cumulative Redeemable Preferred Stock", False),
        ("Depositary Shares each representing 1/1000th interest", False),
        ("5.250% Senior Notes due 2030", False),
        ("Subscription Rights", False),
        ("", None),
        (None, None),
        ("Something the cover page invented", None),
    ],
)
def test_common_stock_classification(title: str | None, expected: bool | None) -> None:
    assert classify_common_stock(title) is expected


@pytest.mark.parametrize(
    ("exchange", "expected"),
    [
        ("New York Stock Exchange", True),
        ("NYSE American LLC", True),
        ("The Nasdaq Stock Market LLC", True),
        ("Cboe BZX Exchange, Inc.", True),
        ("", None),
        (None, None),
        ("An unrecognized venue", None),
    ],
)
def test_us_listing_classification(exchange: str | None, expected: bool | None) -> None:
    assert classify_us_listing(exchange) is expected


def test_intervals_close_on_the_day_before_the_next_change() -> None:
    unchanged = _record(
        accession="0000320193-24-000001",
        accepted_at=datetime(2024, 1, 10, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    repeated = _record(
        accession="0000320193-24-000002",
        accepted_at=datetime(2024, 3, 5, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    changed = _record(
        accession="0000320193-24-000003",
        accepted_at=datetime(2024, 6, 20, 14, 0, tzinfo=UTC),
        securities=(
            CoverPageSecurityRecord(
                symbol="AAPL",
                security_title="Warrants, each exercisable for one share",
                exchange="Nasdaq Global Select Market",
            ),
        ),
    )
    observations = [
        observation
        for record in (unchanged, repeated, changed)
        for observation in observations_from_record(record)
    ]

    intervals = build_eligibility_intervals(observations)

    assert len(intervals) == 2
    first, second = intervals
    assert first.valid_from == date(2024, 1, 10)
    assert first.valid_through == date(2024, 6, 19)
    assert first.known_at == datetime(2024, 1, 10, 14, 0, tzinfo=UTC)
    assert first.common_stock is True
    assert second.valid_from == date(2024, 6, 20)
    assert second.valid_through is None
    assert second.common_stock is False


def test_two_filings_of_one_day_that_disagree_leave_the_day_unknown() -> None:
    morning = _record(
        accession="0000320193-24-000001",
        accepted_at=datetime(2024, 1, 10, 12, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    afternoon = _record(
        accession="0000320193-24-000002",
        accepted_at=datetime(2024, 1, 10, 20, 0, tzinfo=UTC),
        securities=(
            CoverPageSecurityRecord(
                symbol="AAPL",
                security_title="Warrants, each exercisable for one share",
                exchange="Nasdaq Global Select Market",
            ),
        ),
    )
    observations = [
        observation
        for record in (morning, afternoon)
        for observation in observations_from_record(record)
    ]

    (interval,) = build_eligibility_intervals(observations)

    assert interval.common_stock is None
    # The earliest acceptance of the day carries the interval, so both filings
    # of that day can resolve against it.
    assert interval.known_at == datetime(2024, 1, 10, 12, 0, tzinfo=UTC)


def test_derived_manifest_resolves_the_filing_that_established_it(tmp_path: Path) -> None:
    accepted_at = datetime(2024, 1, 10, 14, 0, tzinfo=UTC)
    record = _record(accepted_at=accepted_at, securities=(_common(),))
    intervals = build_eligibility_intervals(observations_from_record(record))
    path = write_eligibility_manifest(intervals, tmp_path / "historical_eligibility.csv")
    event = FilingEvent(
        event_id="sec:0000320193-24-000001",
        accession_number="0000320193-24-000001",
        cik="0000320193",
        form="8-K",
        items=("2.02",),
        symbols=("AAPL",),
        accepted_at=accepted_at,
        first_seen_at=accepted_at,
        retrieved_at=accepted_at,
        source=DataSource.SEC,
    )

    resolved = CsvEligibilityResolver(path)(event, "AAPL")

    assert resolved is not None
    assert resolved.common_stock is True
    assert resolved.us_listing is True
    # The cover page cannot establish corporate-action completeness, so the
    # record must stay an explicit gap rather than a silent pass.
    assert resolved.corporate_actions_complete is None
    assert resolved.missing_confirmations == ("CORPORATE_ACTIONS",)
    assert not resolved.confirmed_eligible


def test_manifest_header_matches_the_resolver_contract(tmp_path: Path) -> None:
    record = _record(
        accepted_at=datetime(2024, 1, 10, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    path = write_eligibility_manifest(
        build_eligibility_intervals(observations_from_record(record)),
        tmp_path / "manifest.csv",
    )

    header = path.read_text(encoding="utf-8").splitlines()[0]

    assert header == ",".join(MANIFEST_HEADER)
    assert CsvEligibilityResolver(path).manifest_sha256


def test_manifest_write_never_overwrites(tmp_path: Path) -> None:
    target = tmp_path / "manifest.csv"
    target.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError):
        write_eligibility_manifest((), target)

    assert target.read_text(encoding="utf-8") == "existing"


def test_summary_accounts_for_filings_without_any_registered_security() -> None:
    with_security = _record(
        accepted_at=datetime(2024, 1, 10, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    without = _record(
        accession="0000320193-24-000002",
        accepted_at=datetime(2024, 2, 10, 14, 0, tzinfo=UTC),
        securities=(),
    )
    records = (with_security, without)
    observations = tuple(
        observation for record in records for observation in observations_from_record(record)
    )
    intervals = build_eligibility_intervals(observations)

    summary = summarize(records, observations, intervals)

    assert summary.filings == 2
    assert summary.filings_without_securities == 1
    assert summary.observations == 1
    assert summary.symbols == 1
    assert summary.intervals == 1
    assert summary.corporate_actions_unknown == 1


def test_read_fact_records_drops_and_truncates_one_torn_trailing_line(tmp_path: Path) -> None:
    path = tmp_path / "facts.jsonl"
    complete = _record(
        accepted_at=datetime(2024, 1, 10, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    path.write_text(
        complete.model_dump_json() + '\n{"accession_number": "0000320193-24-0000',
        encoding="utf-8",
    )

    records = read_fact_records(path)

    assert len(records) == 1
    assert read_fact_records(path) == records
    # The torn remainder has to be gone from the file, not merely skipped while
    # reading: the next append would otherwise concatenate onto it and corrupt
    # both records.
    assert path.read_text(encoding="utf-8") == complete.model_dump_json() + "\n"


def test_a_complete_but_unterminated_record_is_terminated_before_appending(
    tmp_path: Path,
) -> None:
    # An interrupted run can also stop after a complete JSON object but before
    # its newline. Reading has to terminate it, or the next append concatenates
    # onto it and both records are lost as one malformed final line.
    path = tmp_path / "facts.jsonl"
    first = _record(
        accepted_at=datetime(2024, 1, 10, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    second = _record(
        accession="0000320193-24-000002",
        accepted_at=datetime(2024, 1, 11, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    third = _record(
        accession="0000320193-24-000003",
        accepted_at=datetime(2024, 1, 12, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    path.write_text(
        first.model_dump_json() + "\n" + second.model_dump_json(),
        encoding="utf-8",
    )

    assert len(read_fact_records(path)) == 2
    assert path.read_text(encoding="utf-8").endswith("}\n")

    _append_record(path, third)

    assert [record.accession_number for record in read_fact_records(path)] == [
        first.accession_number,
        second.accession_number,
        third.accession_number,
    ]


def test_read_fact_records_refuses_corruption_before_the_last_line(tmp_path: Path) -> None:
    path = tmp_path / "facts.jsonl"
    complete = _record(
        accepted_at=datetime(2024, 1, 10, 14, 0, tzinfo=UTC),
        securities=(_common(),),
    )
    path.write_text("{not json}\n" + complete.model_dump_json() + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="corrupt at line 1"):
        read_fact_records(path)


_INDEX = (
    "CIK|Company Name|Form Type|Date Filed|Filename\n"
    "--------------------------------------------------------\n"
    "320193|Example Inc|8-K|2024-01-10|edgar/data/320193/0000320193-24-000001.txt\n"
    "320193|Example Inc|8-K|2024-01-11|edgar/data/320193/0000320193-24-000002.txt\n"
)
_SUBMISSION = (
    b"<ACCEPTANCE-DATETIME>20240110090000\n"
    b'<dei:TradingSymbol contextRef="c">AAPL</dei:TradingSymbol>'
    b'<dei:Security12bTitle contextRef="c">Common Stock</dei:Security12bTitle>'
    b'<dei:SecurityExchangeName contextRef="c">NASDAQ</dei:SecurityExchangeName>'
)


async def test_collector_resumes_without_refetching_what_it_already_has(tmp_path: Path) -> None:
    output = tmp_path / "facts.jsonl"
    fetched: list[str] = []

    async def fetch_index(_url: str) -> bytes:
        return _INDEX.encode("ascii")

    async def fetch_submission(url: str) -> bytes:
        fetched.append(url)
        return _SUBMISSION

    collector = CoverPageFactCollector(
        index_fetcher=fetch_index,
        submission_fetcher=fetch_submission,
    )

    first = await collector.run(start=date(2024, 1, 1), end=date(2024, 1, 31), output=output)
    second = await collector.run(start=date(2024, 1, 1), end=date(2024, 1, 31), output=output)

    assert first.discovered == 2
    assert first.collected == 2
    assert first.failed == 0
    assert second.collected == 0
    assert second.already_collected == 2
    assert len(fetched) == 2


async def test_collector_leaves_a_failed_filing_for_the_next_run(tmp_path: Path) -> None:
    output = tmp_path / "facts.jsonl"
    attempts: list[str] = []

    async def fetch_index(_url: str) -> bytes:
        return _INDEX.encode("ascii")

    async def fetch_submission(url: str) -> bytes:
        attempts.append(url)
        if len(attempts) == 1:
            raise TimeoutError("transport")
        return _SUBMISSION

    collector = CoverPageFactCollector(
        index_fetcher=fetch_index,
        submission_fetcher=fetch_submission,
    )

    first = await collector.run(start=date(2024, 1, 1), end=date(2024, 1, 31), output=output)
    second = await collector.run(start=date(2024, 1, 1), end=date(2024, 1, 31), output=output)

    assert first.collected == 1
    assert first.failed == 1
    assert first.failure_samples == ("0000320193-24-000001: TimeoutError",)
    # The failure was never written, so the second pass retries exactly it.
    assert second.collected == 1
    assert second.failed == 0
    assert len(read_fact_records(output)) == 2
