"""Point-in-time SEC archive resolver for quarterly-index backfills."""

from __future__ import annotations

import html
import inspect
import re
from collections.abc import Awaitable, Callable
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
_TRADING_SYMBOL_TAG_RE = re.compile(
    rb"<(?:[A-Za-z0-9_-]+:)?TradingSymbol\b[^>]*>([^<]{1,64})</",
    flags=re.IGNORECASE,
)
_INLINE_TRADING_SYMBOL_RE = re.compile(
    rb"<ix:(?:nonNumeric|nonFraction)\b[^>]*"
    rb"name=[\"'][^\"']*TradingSymbol[\"'][^>]*>([^<]{1,64})</ix:",
    flags=re.IGNORECASE,
)
_SAFE_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")


class HistoricalSubmissionMetadata(tuple):
    """Parsed SEC submission facts without any current security-master lookup."""

    __slots__ = ()

    def __new__(
        cls,
        accepted_at: datetime,
        items: tuple[str, ...],
        symbols: tuple[str, ...],
    ) -> HistoricalSubmissionMetadata:
        return tuple.__new__(cls, (accepted_at, items, symbols))

    @property
    def accepted_at(self) -> datetime:
        return cast(datetime, self[0])

    @property
    def items(self) -> tuple[str, ...]:
        return cast(tuple[str, ...], self[1])

    @property
    def symbols(self) -> tuple[str, ...]:
        return cast(tuple[str, ...], self[2])


def parse_historical_submission(payload: bytes) -> HistoricalSubmissionMetadata:
    """Parse only acceptance time, 8-K items and filing-reported symbols."""

    accepted_match = _ACCEPTED_RE.search(payload)
    if accepted_match is None:
        raise ValueError("historical submission has no acceptance timestamp")
    local = datetime.strptime(accepted_match.group(1).decode("ascii"), "%Y%m%d%H%M%S").replace(
        tzinfo=_NEW_YORK
    )
    items = tuple(dict.fromkeys(match.decode("ascii") for match in _ITEM_RE.findall(payload)))
    raw_symbols = [
        *(_decode_symbol(value) for value in _TRADING_SYMBOL_TAG_RE.findall(payload)),
        *(_decode_symbol(value) for value in _INLINE_TRADING_SYMBOL_RE.findall(payload)),
    ]
    symbols = tuple(
        dict.fromkeys(symbol for symbol in raw_symbols if _SAFE_SYMBOL_RE.fullmatch(symbol))
    )
    return HistoricalSubmissionMetadata(local.astimezone(UTC), items, symbols)


def _decode_symbol(value: bytes) -> str:
    return html.unescape(value.decode("utf-8", errors="replace")).strip().upper()


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
