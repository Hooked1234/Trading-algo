from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from event_trader.domain import DocumentRef
from event_trader.providers.sec import (
    AsyncRateLimiter,
    SecCursor,
    SecParseError,
    SecProvider,
    SecProviderConfig,
    SecProviderError,
    default_document_selector,
    discover_filing_documents,
    extract_filing_symbols,
    parse_latest_filings_atom,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sec"
FIRST_SEEN = datetime(2026, 7, 30, 22, 0, tzinfo=UTC)
APPLE_ACCESSION = "0000320193-26-000018"
AMENDMENT_ACCESSION = "0001213900-25-029149"


class _NoWaitLimiter:
    async def acquire(self) -> None:
        return None


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_atom_parser_keeps_only_exact_8k_forms_and_utc_timestamps() -> None:
    events, feed_updated_at = parse_latest_filings_atom(
        _fixture("latest_8k.atom"),
        first_seen_at=FIRST_SEEN,
    )

    assert [event.accession_number for event in events] == [
        APPLE_ACCESSION,
        AMENDMENT_ACCESSION,
    ]
    apple, amendment = events
    assert apple.form == "8-K"
    assert amendment.form == "8-K/A"
    assert apple.cik == "0000320193"
    assert apple.items == ("2.02", "9.01")
    assert apple.accepted_at == datetime(2026, 7, 30, 20, 30, 28, tzinfo=UTC)
    assert apple.first_seen_at == FIRST_SEEN
    assert apple.retrieved_at == FIRST_SEEN
    assert apple.complete is False
    assert feed_updated_at == datetime(2026, 7, 30, 21, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "payload",
    [b"not xml", b"<entry xmlns='http://www.w3.org/2005/Atom' />"],
)
def test_atom_parser_rejects_non_feed_payloads(payload: bytes) -> None:
    with pytest.raises(SecParseError):
        parse_latest_filings_atom(payload, first_seen_at=FIRST_SEEN)


def test_atom_parser_rejects_unsafe_xml_and_naive_receipt_clock() -> None:
    unsafe = "<!DOCTYPE feed [<!ENTITY payload 'unsafe'>]><feed>&payload;</feed>"
    with pytest.raises(SecParseError, match="DTD or entity"):
        parse_latest_filings_atom(unsafe, first_seen_at=FIRST_SEEN)
    with pytest.raises(SecProviderError, match="timezone-aware"):
        parse_latest_filings_atom(
            _fixture("latest_8k.atom"),
            first_seen_at=datetime(2026, 7, 30, 22, 0),
        )


def test_cursor_validation_and_malformed_serialization_fail_closed() -> None:
    assert SecCursor.from_json(None) == SecCursor()
    with pytest.raises(ValueError, match="timezone-aware"):
        SecCursor(feed_updated_at=datetime(2026, 7, 30, 22, 0))
    with pytest.raises(ValueError, match="invalid accession"):
        SecCursor(seen_accessions=("not-an-accession",))
    with pytest.raises(SecParseError, match="serialized SEC cursor"):
        SecCursor.from_json("{not-json")


def test_document_discovery_normalizes_ixviewer_and_rejects_external_hosts() -> None:
    index_url = (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019326000018/"
        "0000320193-26-000018-index.htm"
    )

    candidates = discover_filing_documents(
        _fixture("filing_index.html"),
        index_url=index_url,
        form="8-K",
    )

    assert [candidate.kind for candidate in candidates] == [
        "8-K",
        "EX-99.1",
        "EX-990",
        "EX-101.INS",
    ]
    assert candidates[0].is_primary is True
    assert candidates[0].url == (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019326000018/aapl-20260730.htm"
    )
    assert default_document_selector(candidates[0]) is True
    assert default_document_selector(candidates[1]) is True
    assert default_document_selector(candidates[2]) is False
    assert default_document_selector(candidates[3]) is False
    assert all("example.invalid" not in candidate.url for candidate in candidates)


def test_filing_symbol_extraction_uses_only_semantic_xbrl_facts() -> None:
    payload = b"""
        <dei:TradingSymbol contextRef="c1"> aapl </dei:TradingSymbol>
        <ix:nonNumeric name="dei:TradingSymbol" contextRef="c2">BRK.B</ix:nonNumeric>
        <p>Nasdaq symbol SHOULD_NOT_BE_GUESSED</p>
        <dei:TradingSymbol contextRef="c3">N/A</dei:TradingSymbol>
    """

    assert extract_filing_symbols(payload) == ("AAPL", "BRK.B")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("primary_symbols", "exhibit_symbols", "expected"),
    [
        (b"", b'<ix:nonNumeric name="dei:TradingSymbol">AAPL</ix:nonNumeric>', ("AAPL",)),
        (
            b"<dei:TradingSymbol>MSFT</dei:TradingSymbol>",
            b'<ix:nonNumeric name="dei:TradingSymbol">AAPL</ix:nonNumeric>',
            (),
        ),
    ],
)
async def test_hydration_extracts_one_unambiguous_filing_symbol_or_fails_closed(
    primary_symbols: bytes,
    exhibit_symbols: bytes,
    expected: tuple[str, ...],
) -> None:
    events, _ = parse_latest_filings_atom(
        _fixture("latest_8k.atom"),
        first_seen_at=FIRST_SEEN,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(f"/{APPLE_ACCESSION}-index.htm"):
            return httpx.Response(200, content=_fixture("filing_index.html"), request=request)
        if path.endswith("/aapl-20260730.htm"):
            return httpx.Response(
                200, content=primary_symbols or b"<p>Primary</p>", request=request
            )
        if path.endswith("/aapl-ex991.htm"):
            return httpx.Response(200, content=exhibit_symbols, request=request)
        return httpx.Response(404, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = SecProvider(
            SecProviderConfig(user_agent="EventTrader test@example.com"),
            client=client,
            limiter=_NoWaitLimiter(),  # type: ignore[arg-type]
            clock=lambda: FIRST_SEEN,
        )
        hydrated = await provider.hydrate_documents(events[0])

    assert hydrated.complete is True
    assert hydrated.symbols == expected


@pytest.mark.asyncio
async def test_poll_hydrates_primary_and_ex99_with_cursor_and_dedupe() -> None:
    requests: list[httpx.Request] = []
    persisted: list[tuple[str, str, bytes, datetime]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if path == "/cgi-bin/browse-edgar":
            return httpx.Response(200, content=_fixture("latest_8k.atom"), request=request)
        if path.endswith(f"/{APPLE_ACCESSION}-index.htm"):
            return httpx.Response(200, content=_fixture("filing_index.html"), request=request)
        if path.endswith("/aapl-20260730.htm"):
            return httpx.Response(200, content=_fixture("primary_8k.html"), request=request)
        if path.endswith("/aapl-ex991.htm"):
            return httpx.Response(200, content=_fixture("exhibit_99_1.html"), request=request)
        return httpx.Response(404, request=request)

    async def persist_document(
        *,
        url: str,
        kind: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> DocumentRef:
        persisted.append((url, kind, content, retrieved_at))
        return DocumentRef(
            url=url,
            kind=kind,
            sha256=hashlib.sha256(content).hexdigest(),
            local_path=f"/raw/{hashlib.sha256(content).hexdigest()}.bin",
        )

    async def resolve_symbols(cik: str) -> tuple[str, ...]:
        assert cik == "0000320193"
        return ("aapl", "AAPL")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = SecProvider(
            SecProviderConfig(user_agent="EventTrader test@example.com"),
            client=client,
            limiter=_NoWaitLimiter(),  # type: ignore[arg-type]
            document_persistence=persist_document,
            symbol_resolver=resolve_symbols,
            clock=lambda: FIRST_SEEN,
        )
        initial_cursor = SecCursor(seen_accessions=(AMENDMENT_ACCESSION,))
        first = await provider.poll(initial_cursor)
        replay = await provider.poll(first.cursor)

    assert len(first.events) == 1
    event = first.events[0]
    assert event.accession_number == APPLE_ACCESSION
    assert event.symbols == ("AAPL",)
    assert event.complete is True
    assert [document.kind for document in event.documents] == ["8-K", "EX-99.1"]
    assert all(document.local_path for document in event.documents)
    assert [item[1] for item in persisted] == ["8-K", "EX-99.1"]
    assert replay.events == ()
    assert set(first.cursor.seen_accessions) == {APPLE_ACCESSION, AMENDMENT_ACCESSION}
    assert SecCursor.from_json(first.cursor.to_json()) == first.cursor

    feed_requests = [request for request in requests if request.url.path == "/cgi-bin/browse-edgar"]
    assert len(feed_requests) == 2
    assert all(
        request.headers["User-Agent"] == "EventTrader test@example.com" for request in requests
    )
    assert feed_requests[0].url.params["type"] == "8-K"
    assert not any(request.url.path.endswith("aapl-ex990.htm") for request in requests)
    assert not any(request.url.path.endswith("aapl-20260730_htm.xml") for request in requests)


@pytest.mark.asyncio
async def test_fetch_feed_retries_retryable_status_with_retry_after() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, content=_fixture("latest_8k.atom"), request=request)

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = SecProvider(
            SecProviderConfig(
                user_agent="EventTrader test@example.com",
                max_retries=1,
            ),
            client=client,
            limiter=_NoWaitLimiter(),  # type: ignore[arg-type]
            sleep=record_sleep,
            clock=lambda: FIRST_SEEN,
        )
        payload = await provider.fetch_feed()

    assert payload == _fixture("latest_8k.atom")
    assert attempts == 2
    assert sleeps == [0.0]


@pytest.mark.asyncio
async def test_fetch_feed_retries_network_timeout_with_backoff() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("simulated timeout", request=request)
        return httpx.Response(200, content=_fixture("latest_8k.atom"), request=request)

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = SecProvider(
            SecProviderConfig(user_agent="EventTrader test@example.com", max_retries=1),
            client=client,
            limiter=_NoWaitLimiter(),  # type: ignore[arg-type]
            sleep=record_sleep,
            clock=lambda: FIRST_SEEN,
        )
        payload = await provider.fetch_feed()

    assert payload == _fixture("latest_8k.atom")
    assert attempts == 2
    assert len(sleeps) == 1
    assert sleeps[0] > 0


@pytest.mark.asyncio
async def test_persistence_hook_mismatch_marks_document_incomplete() -> None:
    events, _ = parse_latest_filings_atom(
        _fixture("latest_8k.atom"),
        first_seen_at=FIRST_SEEN,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(f"/{APPLE_ACCESSION}-index.htm"):
            return httpx.Response(200, content=_fixture("filing_index.html"), request=request)
        if path.endswith("/aapl-20260730.htm"):
            return httpx.Response(200, content=_fixture("primary_8k.html"), request=request)
        if path.endswith("/aapl-ex991.htm"):
            return httpx.Response(200, content=_fixture("exhibit_99_1.html"), request=request)
        return httpx.Response(404, request=request)

    def bad_persistence(
        *,
        url: str,
        kind: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> DocumentRef:
        del content, retrieved_at
        return DocumentRef(url=url, kind=kind, sha256="0" * 64)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = SecProvider(
            SecProviderConfig(user_agent="EventTrader test@example.com"),
            client=client,
            limiter=_NoWaitLimiter(),  # type: ignore[arg-type]
            document_persistence=bad_persistence,
            clock=lambda: FIRST_SEEN,
        )
        hydrated = await provider.hydrate_documents(events[0])

    assert hydrated.complete is False
    assert hydrated.documents == ()


@pytest.mark.asyncio
async def test_failed_selected_document_marks_filing_incomplete() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/cgi-bin/browse-edgar":
            return httpx.Response(200, content=_fixture("latest_8k.atom"), request=request)
        if path.endswith(f"/{APPLE_ACCESSION}-index.htm"):
            return httpx.Response(200, content=_fixture("filing_index.html"), request=request)
        if path.endswith("/aapl-20260730.htm"):
            return httpx.Response(200, content=b"too large", request=request)
        if path.endswith("/aapl-ex991.htm"):
            return httpx.Response(503, request=request)
        return httpx.Response(404, request=request)

    async def no_sleep(_: float) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = SecProvider(
            SecProviderConfig(
                user_agent="EventTrader test@example.com",
                max_retries=0,
                max_document_bytes=4,
            ),
            client=client,
            limiter=_NoWaitLimiter(),  # type: ignore[arg-type]
            sleep=no_sleep,
            symbol_resolver=lambda _: ("AAPL",),
            clock=lambda: FIRST_SEEN,
        )
        result = await provider.poll(SecCursor(seen_accessions=(AMENDMENT_ACCESSION,)))

    assert len(result.events) == 1
    assert result.events[0].complete is False
    assert result.events[0].documents == ()
    assert result.events[0].symbols == ()
    assert APPLE_ACCESSION not in result.cursor.seen_accessions


@pytest.mark.asyncio
async def test_rate_limiter_spaces_request_starts_at_two_per_second() -> None:
    current = 10.0
    sleeps: list[float] = []

    def monotonic() -> float:
        return current

    async def advance(delay: float) -> None:
        nonlocal current
        sleeps.append(delay)
        current += delay

    limiter = AsyncRateLimiter(2, monotonic=monotonic, sleep=advance)
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert sleeps == pytest.approx([0.5, 0.5])


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: SecProviderConfig(user_agent=""), "User-Agent"),
        (
            lambda: SecProviderConfig(user_agent="test test@example.com", requests_per_second=2.1),
            "at most 2",
        ),
        (
            lambda: SecProviderConfig(user_agent="test test@example.com", timeout_seconds=0),
            "timeout",
        ),
        (lambda: SecProviderConfig(user_agent="test test@example.com", max_retries=-1), "negative"),
        (lambda: SecProviderConfig(user_agent="test test@example.com", feed_count=0), "feed_count"),
        (
            lambda: SecProviderConfig(
                user_agent="test test@example.com",
                feed_count=100,
                cursor_capacity=99,
            ),
            "cursor_capacity",
        ),
        (
            lambda: SecProviderConfig(
                user_agent="test test@example.com",
                max_document_bytes=0,
            ),
            "byte limits",
        ),
    ],
)
def test_provider_config_rejects_unsafe_request_policy(
    factory: Callable[[], SecProviderConfig],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        factory()
