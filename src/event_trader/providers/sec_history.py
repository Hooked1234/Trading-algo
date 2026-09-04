"""Point-in-time SEC archive resolver for quarterly-index backfills."""

from __future__ import annotations

import html
import inspect
import re
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from zoneinfo import ZoneInfo

from event_trader.backfill import SecIndexEntry
from event_trader.domain import DataSource, FilingEvent

ArchiveFetcher = Callable[[str], bytes | Awaitable[bytes]]
FilingHydrator = Callable[[FilingEvent], FilingEvent | Awaitable[FilingEvent]]

_NEW_YORK = ZoneInfo("America/New_York")
_ACCEPTED_RE = re.compile(
    rb"(?:<ACCEPTANCE-DATETIME>|ACCEPTANCE-DATETIME\s*:)\s*(\d{14})",
    flags=re.IGNORECASE,
)
_ITEM_RE = re.compile(
    rb"(?:<ITEMS>|ITEM\s+INFORMATION\s*:)\s*(\d{1,2}\.\d{2})",
    flags=re.IGNORECASE,
)
_COVER_PAGE_FACTS = ("TradingSymbol", "Security12bTitle", "SecurityExchangeName")
_PLAIN_FACT_RE = re.compile(
    rb"<(?:[A-Za-z0-9_-]+:)?("
    + rb"|".join(name.encode("ascii") for name in _COVER_PAGE_FACTS)
    + rb")\b([^>]*)>([^<]{0,256})</",
    flags=re.IGNORECASE,
)
_INLINE_FACT_RE = re.compile(
    rb"<ix:(nonNumeric|nonFraction)\b([^>]*)>(.{0,512}?)</ix:\1>",
    flags=re.IGNORECASE | re.DOTALL,
)
_ATTRIBUTE_RE = re.compile(rb"([A-Za-z0-9_:.-]+)\s*=\s*[\"']([^\"']*)[\"']")
_MARKUP_RE = re.compile(rb"<[^>]*>")
_SAFE_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")
_COVER_PAGE_FACT_NAMES = frozenset(fact.casefold() for fact in _COVER_PAGE_FACTS)
_NO_CONTEXT = ""


@dataclass(frozen=True, slots=True)
class CoverPageSecurity:
    """One security registered under Section 12(b) as the filing itself reports it.

    The three facts come from the same XBRL context, so a filer with several
    registered classes cannot have the common-stock title of one class
    attributed to the symbol of another.  A fact the filing does not carry stays
    ``None``; it is never inferred from a sibling security.
    """

    symbol: str
    security_title: str | None = None
    exchange: str | None = None


class HistoricalSubmissionMetadata(tuple):
    """Parsed SEC submission facts without any current security-master lookup."""

    __slots__ = ()

    def __new__(
        cls,
        accepted_at: datetime,
        items: tuple[str, ...],
        symbols: tuple[str, ...],
        securities: tuple[CoverPageSecurity, ...] = (),
    ) -> HistoricalSubmissionMetadata:
        return tuple.__new__(cls, (accepted_at, items, symbols, securities))

    @property
    def accepted_at(self) -> datetime:
        return cast(datetime, self[0])

    @property
    def items(self) -> tuple[str, ...]:
        return cast(tuple[str, ...], self[1])

    @property
    def symbols(self) -> tuple[str, ...]:
        return cast(tuple[str, ...], self[2])

    @property
    def securities(self) -> tuple[CoverPageSecurity, ...]:
        return cast(tuple[CoverPageSecurity, ...], self[3])


def parse_historical_submission(payload: bytes) -> HistoricalSubmissionMetadata:
    """Parse only acceptance time, 8-K items and filing-reported securities."""

    accepted_match = _ACCEPTED_RE.search(payload)
    if accepted_match is None:
        raise ValueError("historical submission has no acceptance timestamp")
    local = datetime.strptime(accepted_match.group(1).decode("ascii"), "%Y%m%d%H%M%S").replace(
        tzinfo=_NEW_YORK
    )
    items = tuple(dict.fromkeys(match.decode("ascii") for match in _ITEM_RE.findall(payload)))
    securities = parse_cover_page_securities(payload)
    symbols = tuple(security.symbol for security in securities)
    return HistoricalSubmissionMetadata(local.astimezone(UTC), items, symbols, securities)


def parse_cover_page_securities(payload: bytes) -> tuple[CoverPageSecurity, ...]:
    """Group cover-page facts by their XBRL context into one entry per security.

    Both the plain instance form (``<dei:TradingSymbol contextRef=...>``) and the
    inline form (``<ix:nonNumeric name="dei:TradingSymbol" contextRef=...>``) are
    read.  A symbol reported from two contexts with conflicting titles or
    exchanges keeps the conflicting fact unknown rather than picking a winner.
    """

    contexts: dict[str, dict[str, str]] = {}
    for name, context, value in _iter_cover_page_facts(payload):
        contexts.setdefault(context, {}).setdefault(name, value)

    securities: dict[str, CoverPageSecurity] = {}
    for facts in contexts.values():
        symbol = facts.get("tradingsymbol", "").upper()
        if _SAFE_SYMBOL_RE.fullmatch(symbol) is None:
            continue
        candidate = CoverPageSecurity(
            symbol=symbol,
            security_title=facts.get("security12btitle"),
            exchange=facts.get("securityexchangename"),
        )
        existing = securities.get(symbol)
        securities[symbol] = (
            candidate if existing is None else _merge_securities(existing, candidate)
        )
    return tuple(securities.values())


def _merge_securities(first: CoverPageSecurity, second: CoverPageSecurity) -> CoverPageSecurity:
    return CoverPageSecurity(
        symbol=first.symbol,
        security_title=_agree(first.security_title, second.security_title),
        exchange=_agree(first.exchange, second.exchange),
    )


def _agree(first: str | None, second: str | None) -> str | None:
    """Keep a fact only while every context that reports it agrees."""

    if first is None or second is None:
        return first if second is None else second
    return first if first.casefold() == second.casefold() else None


def _iter_cover_page_facts(payload: bytes) -> Iterator[tuple[str, str, str]]:
    """Yield ``(fact name, context, value)`` for plain facts, then inline facts."""

    for raw_name, attributes, raw_value in _PLAIN_FACT_RE.findall(payload):
        value = _clean_text(raw_value)
        if value:
            yield raw_name.decode("ascii").casefold(), _context_of(attributes), value
    for _tag, attributes, raw_value in _INLINE_FACT_RE.findall(payload):
        parsed = _parse_attributes(attributes)
        name = parsed.get("name", "").rpartition(":")[2].casefold()
        if name not in _COVER_PAGE_FACT_NAMES:
            continue
        value = _clean_text(_MARKUP_RE.sub(b" ", raw_value))
        if value:
            yield name, parsed.get("contextref", _NO_CONTEXT), value


def _context_of(attributes: bytes) -> str:
    return _parse_attributes(attributes).get("contextref", _NO_CONTEXT)


def _parse_attributes(attributes: bytes) -> dict[str, str]:
    return {
        _decode(key).casefold(): _decode(value) for key, value in _ATTRIBUTE_RE.findall(attributes)
    }


def _decode(value: bytes) -> str:
    return value.decode("utf-8", errors="replace").strip()


def _clean_text(value: bytes) -> str:
    return " ".join(html.unescape(value.decode("utf-8", errors="replace")).split())


class HistoricalSecFilingResolver:
    """Resolve a quarterly-index row without consulting today's ticker universe."""

    def __init__(
        self,
        *,
        fetch_submission: ArchiveFetcher,
        hydrate_filing: FilingHydrator,
    ) -> None:
        self._fetch_submission = fetch_submission
        self._hydrate_filing = hydrate_filing

    async def __call__(self, entry: SecIndexEntry) -> FilingEvent:
        result = self._fetch_submission(entry.archive_url)
        payload = await result if inspect.isawaitable(result) else result
        metadata = parse_historical_submission(payload)
        event = FilingEvent(
            event_id=f"sec:{entry.accession_number}",
            accession_number=entry.accession_number,
            cik=entry.cik,
            form=entry.form,
            items=metadata.items,
            symbols=metadata.symbols,
            accepted_at=metadata.accepted_at,
            first_seen_at=metadata.accepted_at,
            retrieved_at=metadata.accepted_at,
            source=DataSource.SEC,
            complete=False,
        )
        hydrated = self._hydrate_filing(event)
        return await hydrated if inspect.isawaitable(hydrated) else hydrated
