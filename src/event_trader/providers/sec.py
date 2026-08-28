"""Asynchronous, replay-safe ingestion for SEC Form 8-K filings.

The provider deliberately uses only documented, public SEC surfaces: the
``Latest Filings`` Atom feed and filing archive pages.  It never treats the
EDGAR acceptance timestamp as the website availability timestamp; callers get
an independently clocked ``first_seen_at`` value for that purpose.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import inspect
import json
import random
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Protocol
from urllib.parse import parse_qs, urljoin, urlparse
from xml.etree import ElementTree

import httpx

from event_trader.domain import DataSource, DocumentRef, FilingEvent

SEC_BASE_URL = "https://www.sec.gov"
SEC_LATEST_FILINGS_URL = f"{SEC_BASE_URL}/cgi-bin/browse-edgar"
_ATOM_NAMESPACE = "{http://www.w3.org/2005/Atom}"
_ALLOWED_FORMS = frozenset({"8-K", "8-K/A"})
_ACCESSION_RE = re.compile(r"(?<!\d)(\d{10}-\d{2}-\d{6})(?!\d)")
_CIK_RE = re.compile(r"\((\d{1,10})\)")
_ITEM_RE = re.compile(r"\bItem\s+([0-9]{1,2}\.[0-9]{2})\s*:", re.IGNORECASE)
_FILED_RE = re.compile(r"\bFiled:\s*(\d{4}-\d{2}-\d{2})\b", re.IGNORECASE)
_TRADING_SYMBOL_TAG_RE = re.compile(
    rb"<(?:[A-Za-z_][A-Za-z0-9_.-]*:)?TradingSymbol\b[^>]*>\s*"
    rb"([^<]{1,64}?)\s*</",
    flags=re.IGNORECASE,
)
_INLINE_TRADING_SYMBOL_RE = re.compile(
    rb"<ix:(?:nonNumeric|nonFraction)\b[^>]*"
    rb"name=[\"'][^\"']*TradingSymbol[\"'][^>]*>\s*"
    rb"([^<]{1,64}?)\s*</ix:",
    flags=re.IGNORECASE,
)
_SAFE_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")
_NON_SYMBOL_VALUES = frozenset({"N/A", "NA", "NONE", "NOT APPLICABLE"})
_RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class SecProviderError(RuntimeError):
    """Base error for failures that prevent reliable SEC ingestion."""


class SecParseError(SecProviderError):
    """Raised when a SEC payload does not satisfy the expected contract."""


class SecDocumentTooLarge(SecProviderError):
    """Raised before retaining a document above the configured safety limit."""


@dataclass(frozen=True, slots=True)
class SecProviderConfig:
    """Operational limits for :class:`SecProvider`.

    SEC currently permits at most ten requests per second.  This provider is
    intentionally stricter and never allows configuration above two requests
    per second, leaving headroom for reconciliation or manual requests.
    """

    user_agent: str
    requests_per_second: float = 2.0
    timeout_seconds: float = 15.0
    max_retries: int = 4
    backoff_base_seconds: float = 0.25
    backoff_max_seconds: float = 8.0
    feed_count: int = 100
    cursor_capacity: int = 4_096
    max_feed_bytes: int = 2_000_000
    max_index_bytes: int = 5_000_000
    max_document_bytes: int = 20_000_000

    def __post_init__(self) -> None:
        if not self.user_agent.strip():
            raise ValueError("SEC requests require a declared User-Agent")
        if not 0 < self.requests_per_second <= 2:
            raise ValueError("requests_per_second must be greater than 0 and at most 2")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0")
        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if not 0 < self.feed_count <= 100:
            raise ValueError("feed_count must be between 1 and 100")
        if self.cursor_capacity < self.feed_count:
            raise ValueError("cursor_capacity must be at least feed_count")
        if min(self.max_feed_bytes, self.max_index_bytes, self.max_document_bytes) <= 0:
            raise ValueError("response byte limits must be greater than 0")


@dataclass(frozen=True, slots=True)
class SecCursor:
    """Serializable bounded cursor used for restart-safe feed deduplication."""

    feed_updated_at: datetime | None = None
    seen_accessions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.feed_updated_at is not None and self.feed_updated_at.tzinfo is None:
            raise ValueError("feed_updated_at must be timezone-aware")
        invalid = [value for value in self.seen_accessions if not _ACCESSION_RE.fullmatch(value)]
        if invalid:
            raise ValueError(f"invalid accession number in cursor: {invalid[0]}")

    def to_json(self) -> str:
        updated = _as_utc(self.feed_updated_at) if self.feed_updated_at else None
        return json.dumps(
            {
                "feed_updated_at": updated.isoformat().replace("+00:00", "Z") if updated else None,
                "seen_accessions": list(self.seen_accessions),
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, value: str | bytes | None) -> SecCursor:
        if not value:
            return cls()
        try:
            payload = json.loads(value)
            updated = (
                _parse_timestamp(payload["feed_updated_at"])
                if payload.get("feed_updated_at")
                else None
            )
            accessions = tuple(str(item) for item in payload.get("seen_accessions", ()))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SecParseError("invalid serialized SEC cursor") from exc
        return cls(feed_updated_at=updated, seen_accessions=accessions)


@dataclass(frozen=True, slots=True)
class SecPollResult:
    """New filings and the cursor that must be committed with them."""

    events: tuple[FilingEvent, ...]
    cursor: SecCursor


@dataclass(frozen=True, slots=True)
class SecDocumentCandidate:
    """A document advertised by a filing detail page."""

    url: str
    kind: str
    description: str = ""
    is_primary: bool = False


class DocumentPersistenceHook(Protocol):
    """Content-addressed persistence hook implemented by the operational store."""

    def __call__(
        self,
        *,
        url: str,
        kind: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> DocumentRef | Awaitable[DocumentRef]: ...


class SymbolResolver(Protocol):
    """Optional CIK-to-symbol resolver; it should itself be point-in-time safe."""

    def __call__(self, cik: str) -> Sequence[str] | Awaitable[Sequence[str]]: ...


type DocumentSelector = Callable[[SecDocumentCandidate], bool]
type Sleep = Callable[[float], Awaitable[None]]
type Clock = Callable[[], datetime]


class AsyncRateLimiter:
    """Simple fair limiter that spaces request starts across concurrent tasks."""

    def __init__(
        self,
        requests_per_second: float = 2.0,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if not 0 < requests_per_second <= 2:
            raise ValueError("requests_per_second must be greater than 0 and at most 2")
        self._interval = 1.0 / requests_per_second
        self._monotonic = monotonic
        self._sleep = sleep
        self._next_request_at = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = self._monotonic()
            delay = max(0.0, self._next_request_at - now)
            if delay:
                await self._sleep(delay)
                now = self._monotonic()
            self._next_request_at = max(now, self._next_request_at) + self._interval


class _NoopPersistence:
    def __call__(
        self,
        *,
        url: str,
        kind: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> DocumentRef:
        del retrieved_at
        return DocumentRef(
            url=url,
            kind=kind,
            sha256=hashlib.sha256(content).hexdigest(),
            local_path=None,
        )


@dataclass(slots=True)
class _TableCell:
    text: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)


class _FilingTableParser(HTMLParser):
    """Small tolerant parser for EDGAR's ``Document Format Files`` table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[_TableCell]] = []
        self._row: list[_TableCell] | None = None
        self._cell: _TableCell | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = _TableCell()
        elif tag == "a" and self._cell is not None:
            href = dict(attrs).get("href")
            if href:
                self._cell.links.append(href)

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.text.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            self._row.append(self._cell)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell = None


def default_document_selector(candidate: SecDocumentCandidate) -> bool:
    """Select the primary 8-K and generic press-release exhibits only."""

    kind = candidate.kind.upper()
    return candidate.is_primary or kind == "EX-99" or kind.startswith("EX-99.")


class SecProvider:
    """Poll and hydrate newly disseminated SEC 8-K/8-K/A filings."""

    def __init__(
        self,
        config: SecProviderConfig,
        *,
        client: httpx.AsyncClient | None = None,
        limiter: AsyncRateLimiter | None = None,
        document_persistence: DocumentPersistenceHook | None = None,
        document_selector: DocumentSelector = default_document_selector,
        symbol_resolver: SymbolResolver | None = None,
        clock: Clock | None = None,
        sleep: Sleep = asyncio.sleep,
        random_source: random.Random | None = None,
    ) -> None:
        self.config = config
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout_seconds),
            follow_redirects=True,
        )
        self._limiter = limiter or AsyncRateLimiter(config.requests_per_second)
        self._persist_document = document_persistence or _NoopPersistence()
        self._document_selector = document_selector
        self._symbol_resolver = symbol_resolver
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleep
        self._random = random_source or random.SystemRandom()
        self._cursor = SecCursor()

    async def __aenter__(self) -> SecProvider:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @property
    def cursor(self) -> SecCursor:
        return self._cursor

    async def poll(self, cursor: SecCursor | None = None) -> SecPollResult:
        """Return unseen filings in deterministic acceptance order.

        The returned cursor is updated only after a feed was parsed.  It is the
        caller's responsibility to commit events and the cursor atomically (or
        to tolerate replay, which both accession deduplication and the provided
        SQLite store support).
        """

        active_cursor = cursor if cursor is not None else self._cursor
        feed = await self.fetch_feed()
        first_seen_at = _require_clock_utc(self._clock())
        parsed_events, feed_updated_at = parse_latest_filings_atom(
            feed,
            first_seen_at=first_seen_at,
        )
        seen = set(active_cursor.seen_accessions)
        unseen = [event for event in parsed_events if event.accession_number not in seen]

        hydrated: list[FilingEvent] = []
        for event in unseen:
            event_with_symbols = await self._resolve_symbols(event)
            hydrated.append(await self.hydrate_documents(event_with_symbols))

        chronological = tuple(
            sorted(hydrated, key=lambda event: (event.accepted_at, event.event_id))
        )
        # Keep incomplete filings replayable: a transient index/document
        # failure must not advance the durable dedupe cursor past that filing.
        newest_first = [
            event.accession_number for event in reversed(chronological) if event.complete
        ]
        merged_accessions = _bounded_unique(
            [*newest_first, *active_cursor.seen_accessions],
            self.config.cursor_capacity,
        )
        next_cursor = SecCursor(
            feed_updated_at=max(
                filter(
                    None,
                    (active_cursor.feed_updated_at, feed_updated_at),
                ),
                default=None,
            ),
            seen_accessions=tuple(merged_accessions),
        )
        self._cursor = next_cursor
        return SecPollResult(events=chronological, cursor=next_cursor)

    async def fetch_feed(self) -> bytes:
        params = {
            "action": "getcurrent",
            "company": "",
            "count": str(self.config.feed_count),
            "dateb": "",
            "output": "atom",
            "owner": "include",
            "start": "0",
            "type": "8-K",
        }
        return await self._get_bytes(
            SEC_LATEST_FILINGS_URL,
            params=params,
            max_bytes=self.config.max_feed_bytes,
        )

    async def fetch_archive(self, url: str, *, max_bytes: int | None = None) -> bytes:
        """Fetch one official SEC archive object under the shared global limiter."""

        effective_limit = max_bytes or self.config.max_document_bytes
        if effective_limit <= 0 or effective_limit > self.config.max_document_bytes:
            raise ValueError("archive max_bytes exceeds the configured document limit")
        return await self._get_bytes(url, max_bytes=effective_limit)

    async def hydrate_documents(self, event: FilingEvent) -> FilingEvent:
        """Download the primary filing and selected EX-99.* exhibits."""

        index_url = _filing_index_url(event)
        refs: list[DocumentRef] = []
        filing_symbols: list[str] = []
        complete = True
        try:
            index_html = await self._get_bytes(index_url, max_bytes=self.config.max_index_bytes)
            candidates = discover_filing_documents(index_html, index_url=index_url, form=event.form)
        except (httpx.HTTPError, SecProviderError, UnicodeError):
            candidates = ()
            complete = False

        selected = [candidate for candidate in candidates if self._document_selector(candidate)]
        if not any(candidate.is_primary for candidate in selected):
            complete = False

        for candidate in selected:
            try:
                content = await self._get_bytes(
                    candidate.url,
                    max_bytes=self.config.max_document_bytes,
                )
                retrieved_at = _require_clock_utc(self._clock())
                ref_or_awaitable = self._persist_document(
                    url=candidate.url,
                    kind=candidate.kind,
                    content=content,
                    retrieved_at=retrieved_at,
                )
                ref = (
                    await ref_or_awaitable
                    if inspect.isawaitable(ref_or_awaitable)
                    else ref_or_awaitable
                )
                expected_hash = hashlib.sha256(content).hexdigest()
                if ref.sha256 != expected_hash:
                    raise SecProviderError(
                        "document persistence hook returned a mismatching SHA-256"
                    )
                if ref.url != candidate.url or ref.kind != candidate.kind:
                    raise SecProviderError("document persistence hook changed document identity")
                refs.append(ref)
                filing_symbols.extend(extract_filing_symbols(content))
            except (httpx.HTTPError, OSError, SecProviderError, ValueError):
                complete = False

        retrieved_at = _require_clock_utc(self._clock())
        # Do not persist a provisional resolver guess for an incomplete filing:
        # a missing exhibit may later reveal a conflicting security.  This also
        # prevents the operational store's replay enrichment from retaining a
        # symbol that was never verified against the complete selected filing.
        symbols = _reconcile_filing_symbols(event.symbols, filing_symbols) if complete else ()
        return event.model_copy(
            update={
                "documents": tuple(refs),
                "symbols": symbols,
                "retrieved_at": max(retrieved_at, event.first_seen_at),
                "complete": complete,
            }
        )

    async def _resolve_symbols(self, event: FilingEvent) -> FilingEvent:
        if self._symbol_resolver is None:
            return event
        resolved = self._symbol_resolver(event.cik)
        symbols = await resolved if inspect.isawaitable(resolved) else resolved
        normalized = tuple(
            dict.fromkeys(
                normalized_symbol
                for symbol in symbols
                if (normalized_symbol := _normalize_symbol(symbol)) is not None
            )
        )
        return event.model_copy(update={"symbols": normalized})

    async def _get_bytes(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        max_bytes: int,
    ) -> bytes:
        _validate_sec_url(url)
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/atom+xml, text/html, application/xhtml+xml, */*;q=0.1",
        }
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.get(url, params=params, headers=headers)
                if response.status_code in _RETRYABLE_STATUS_CODES:
                    if attempt >= self.config.max_retries:
                        response.raise_for_status()
                    await self._sleep(
                        self._retry_delay(attempt, response.headers.get("Retry-After"))
                    )
                    continue
                response.raise_for_status()
                declared_length = response.headers.get("Content-Length")
                if declared_length and int(declared_length) > max_bytes:
                    raise SecDocumentTooLarge(f"SEC response exceeds {max_bytes} bytes: {url}")
                if len(response.content) > max_bytes:
                    raise SecDocumentTooLarge(f"SEC response exceeds {max_bytes} bytes: {url}")
                return response.content
            except SecDocumentTooLarge:
                raise
            except httpx.TransportError as exc:
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                await self._sleep(self._retry_delay(attempt, None))
            except ValueError as exc:
                raise SecProviderError(f"invalid HTTP metadata received for {url}") from exc

        raise SecProviderError(f"SEC request failed after retries: {url}") from last_error

    def _retry_delay(self, attempt: int, retry_after: str | None) -> float:
        parsed = _parse_retry_after(retry_after, now=_require_clock_utc(self._clock()))
        if parsed is not None:
            return min(parsed, self.config.backoff_max_seconds)
        exponential = min(
            self.config.backoff_base_seconds * (2**attempt),
            self.config.backoff_max_seconds,
        )
        return exponential * (0.75 + self._random.random() * 0.5)


def parse_latest_filings_atom(
    payload: bytes | str,
    *,
    first_seen_at: datetime,
) -> tuple[tuple[FilingEvent, ...], datetime | None]:
    """Parse a SEC Latest Filings Atom response and filter exact 8-K forms."""

    first_seen_utc = _require_clock_utc(first_seen_at)
    raw_payload = payload.encode() if isinstance(payload, str) else payload
    if re.search(rb"<!\s*(?:DOCTYPE|ENTITY)\b", raw_payload, flags=re.IGNORECASE):
        raise SecParseError("SEC Atom XML must not declare a DTD or entity")
    try:
        # Input size is capped by the caller and entity/DTD declarations are
        # rejected above, avoiding ElementTree's unsafe expansion paths.
        root = ElementTree.fromstring(raw_payload)  # noqa: S314
    except ElementTree.ParseError as exc:
        raise SecParseError("invalid SEC Atom XML") from exc

    if root.tag != f"{_ATOM_NAMESPACE}feed":
        raise SecParseError("SEC response is not an Atom feed")

    feed_updated_text = root.findtext(f"{_ATOM_NAMESPACE}updated")
    feed_updated = _parse_timestamp(feed_updated_text) if feed_updated_text else None
    events_by_accession: dict[str, FilingEvent] = {}

    for entry in root.findall(f"{_ATOM_NAMESPACE}entry"):
        form = _entry_form(entry)
        if form not in _ALLOWED_FORMS:
            continue
        accession = _entry_accession(entry)
        accepted_text = entry.findtext(f"{_ATOM_NAMESPACE}updated")
        title = entry.findtext(f"{_ATOM_NAMESPACE}title") or ""
        summary = entry.findtext(f"{_ATOM_NAMESPACE}summary") or ""
        link = _entry_link(entry)
        cik = _entry_cik(title, link)
        if not accession or not accepted_text or not cik:
            continue
        try:
            accepted_at = _parse_timestamp(accepted_text)
        except SecParseError:
            continue
        cleaned_summary = html.unescape(summary)
        items = tuple(dict.fromkeys(_ITEM_RE.findall(cleaned_summary)))
        # Filing date is intentionally parsed only as a validation hint.  It is
        # not an event timestamp and must never drive a minute-level replay.
        filed_match = _FILED_RE.search(cleaned_summary)
        if filed_match:
            try:
                datetime.fromisoformat(filed_match.group(1))
            except ValueError:
                continue
        event = FilingEvent(
            event_id=f"sec:{accession}",
            accession_number=accession,
            cik=cik,
            form=form,
            items=items,
            symbols=(),
            accepted_at=accepted_at,
            first_seen_at=first_seen_utc,
            retrieved_at=first_seen_utc,
            documents=(),
            source=DataSource.SEC,
            complete=False,
        )
        existing = events_by_accession.get(accession)
        if existing is None or len(event.items) > len(existing.items):
            events_by_accession[accession] = event

    return tuple(events_by_accession.values()), feed_updated


def extract_filing_symbols(payload: bytes) -> tuple[str, ...]:
    """Extract only semantically tagged trading symbols from one SEC document.

    Free-form company names and exchange mentions are deliberately ignored:
    guessing from prose can map an issuer to the wrong security.  Both classic
    XBRL elements and Inline XBRL facts with a ``*TradingSymbol`` name are
    accepted, then normalized through a strict exchange-symbol grammar.
    """

    raw_values = (
        *_TRADING_SYMBOL_TAG_RE.findall(payload),
        *_INLINE_TRADING_SYMBOL_RE.findall(payload),
    )
    symbols: list[str] = []
    for raw_value in raw_values:
        decoded = html.unescape(raw_value.decode("utf-8", errors="replace"))
        symbol = _normalize_symbol(decoded)
        if symbol is not None and symbol not in symbols:
            symbols.append(symbol)
    return tuple(symbols)


def discover_filing_documents(
    payload: bytes | str,
    *,
    index_url: str,
    form: str,
) -> tuple[SecDocumentCandidate, ...]:
    """Extract safe primary/attachment URLs from an EDGAR filing index page."""

    _validate_sec_url(index_url)
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    parser = _FilingTableParser()
    parser.feed(text)
    candidates: list[SecDocumentCandidate] = []
    seen_urls: set[str] = set()

    for row in parser.rows:
        if len(row) < 4:
            continue
        description = _cell_text(row[1])
        kind = _cell_text(row[3]).upper()
        if not kind or not row[2].links:
            continue
        href = next((value for value in row[2].links if value), "")
        if not href:
            continue
        url = _normalize_document_url(index_url, href)
        try:
            _validate_sec_url(url)
        except SecProviderError:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        candidates.append(
            SecDocumentCandidate(
                url=url,
                kind=kind,
                description=description,
                is_primary=kind == form.upper(),
            )
        )
    return tuple(candidates)


def _entry_form(entry: ElementTree.Element) -> str | None:
    category = entry.find(f"{_ATOM_NAMESPACE}category")
    if category is not None:
        term = category.attrib.get("term", "").strip().upper()
        if term:
            return term
    title = (entry.findtext(f"{_ATOM_NAMESPACE}title") or "").strip().upper()
    return title.split(" - ", 1)[0] if " - " in title else None


def _entry_accession(entry: ElementTree.Element) -> str | None:
    for value in (
        entry.findtext(f"{_ATOM_NAMESPACE}id") or "",
        entry.findtext(f"{_ATOM_NAMESPACE}summary") or "",
        _entry_link(entry),
    ):
        match = _ACCESSION_RE.search(value)
        if match:
            return match.group(1)
    return None


def _entry_link(entry: ElementTree.Element) -> str:
    for link in entry.findall(f"{_ATOM_NAMESPACE}link"):
        if link.attrib.get("rel", "alternate") == "alternate" and link.attrib.get("href"):
            return link.attrib["href"]
    return ""


def _entry_cik(title: str, link: str) -> str | None:
    matches = _CIK_RE.findall(title)
    if matches:
        return matches[-1]
    path_match = re.search(r"/data/(\d{1,10})/", link)
    return path_match.group(1) if path_match else None


def _filing_index_url(event: FilingEvent) -> str:
    accession_compact = event.accession_number.replace("-", "")
    return (
        f"{SEC_BASE_URL}/Archives/edgar/data/{int(event.cik)}/"
        f"{accession_compact}/{event.accession_number}-index.htm"
    )


def _normalize_document_url(index_url: str, href: str) -> str:
    resolved = urljoin(index_url, href)
    parsed = urlparse(resolved)
    if parsed.path.startswith("/ixviewer/"):
        document = parse_qs(parsed.query).get("doc", [None])[0]
        if document:
            resolved = urljoin(SEC_BASE_URL, document)
    return resolved


def _validate_sec_url(url: str) -> None:
    parsed = urlparse(url)
    allowed_hosts = {"www.sec.gov", "sec.gov", "data.sec.gov"}
    if parsed.scheme != "https" or parsed.hostname not in allowed_hosts:
        raise SecProviderError(f"refusing non-SEC URL: {url}")


def _cell_text(cell: _TableCell) -> str:
    return " ".join("".join(cell.text).split())


def _normalize_symbol(value: str) -> str | None:
    normalized = " ".join(value.split()).upper()
    if normalized in _NON_SYMBOL_VALUES or not _SAFE_SYMBOL_RE.fullmatch(normalized):
        return None
    return normalized


def _reconcile_filing_symbols(
    resolved_symbols: Sequence[str],
    filing_symbols: Sequence[str],
) -> tuple[str, ...]:
    """Return one unambiguous point-in-time symbol or fail closed.

    Filing facts are authoritative.  A point-in-time CIK resolver may fill a
    filing that contains no semantic symbol fact, but conflicting or multiple
    candidates always produce an empty tuple.  The snapshot factory interprets
    that empty tuple as a coverage gap and cannot create a trading snapshot.
    """

    resolved = tuple(
        dict.fromkeys(
            normalized
            for value in resolved_symbols
            if (normalized := _normalize_symbol(value)) is not None
        )
    )
    reported = tuple(
        dict.fromkeys(
            normalized
            for value in filing_symbols
            if (normalized := _normalize_symbol(value)) is not None
        )
    )
    if len(reported) > 1:
        return ()
    if len(reported) == 1:
        return reported if not resolved or resolved == reported else ()
    return resolved if len(resolved) == 1 else ()


def _parse_timestamp(value: str | None) -> datetime:
    if not value:
        raise SecParseError("missing SEC timestamp")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SecParseError(f"invalid SEC timestamp: {value}") from exc
    if parsed.tzinfo is None:
        raise SecParseError(f"SEC timestamp has no timezone: {value}")
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _require_clock_utc(value: datetime) -> datetime:
    try:
        return _as_utc(value)
    except ValueError as exc:
        raise SecProviderError("provider clock must return a timezone-aware datetime") from exc


def _parse_retry_after(value: str | None, *, now: datetime) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (_as_utc(retry_at) - _as_utc(now)).total_seconds())


def _bounded_unique(values: Iterable[str], capacity: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        result.append(value)
        seen.add(value)
        if len(result) >= capacity:
            break
    return result


__all__ = [
    "SEC_LATEST_FILINGS_URL",
    "AsyncRateLimiter",
    "DocumentPersistenceHook",
    "SecCursor",
    "SecDocumentCandidate",
    "SecDocumentTooLarge",
    "SecParseError",
    "SecPollResult",
    "SecProvider",
    "SecProviderConfig",
    "SecProviderError",
    "default_document_selector",
    "discover_filing_documents",
    "extract_filing_symbols",
    "parse_latest_filings_atom",
]
