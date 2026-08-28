"""Market-data provider contracts and an Alpaca historical-data adapter."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from event_trader.domain import Bar, DataSource, Quote


class MarketDataError(RuntimeError):
    """Base class for provider failures safe to expose in logs."""


class MarketDataTransportError(MarketDataError):
    """The provider could not be reached."""


class MarketDataHTTPError(MarketDataError):
    """A provider returned a non-success response."""

    def __init__(self, status_code: int, url: str, body: str = "") -> None:
        self.status_code = status_code
        self.url = url
        self.body = body[:1000]
        suffix = f": {self.body}" if self.body else ""
        super().__init__(f"market-data HTTP {status_code} for {url}{suffix}")


class MarketDataPayloadError(MarketDataError):
    """A successful response did not match the expected provider schema."""


@dataclass(frozen=True, slots=True)
class HTTPResponse:
    status_code: int
    body: bytes


HTTPTransport = Callable[[str, Mapping[str, str], float], HTTPResponse]


@runtime_checkable
class MarketDataProvider(Protocol):
    """Data interface consumed by research and event evaluation."""

    def get_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        feed: str = "sip",
    ) -> tuple[Bar, ...]:
        """Return ascending historical bars with source metadata."""

    def get_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str = "sip",
    ) -> tuple[Quote, ...]:
        """Return ascending historical top-of-book quotes."""

    def get_latest_quote(self, symbol: str, *, feed: str = "iex") -> Quote:
        """Return the provider's latest top-of-book quote."""


def _urllib_transport(url: str, headers: Mapping[str, str], timeout: float) -> HTTPResponse:
    if urlsplit(url).scheme != "https":
        raise MarketDataTransportError("market-data transport requires HTTPS")
    request = Request(url=url, headers=dict(headers), method="GET")  # noqa: S310
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            return HTTPResponse(response.status, response.read())
    except HTTPError as exc:
        body = exc.read() if exc.fp is not None else b""
        return HTTPResponse(exc.code, body)
    except (TimeoutError, URLError, OSError) as exc:
        raise MarketDataTransportError(
            f"market-data transport failed for {url}: {exc.__class__.__name__}"
        ) from exc


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("market-data timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise MarketDataPayloadError("provider timestamp is missing or not a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MarketDataPayloadError(f"invalid provider timestamp {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketDataPayloadError("provider returned a timezone-naive timestamp")
    return parsed


_INTRADAY_TIMEFRAME = re.compile(r"^(?P<count>[1-9]\d*)(?P<unit>Min|Hour)$")


def _intraday_bar_duration(timeframe: str) -> timedelta:
    match = _INTRADAY_TIMEFRAME.fullmatch(timeframe.strip())
    if match is None:
        raise ValueError(
            "only explicit intraday Alpaca timeframes (NMin/NHour) are supported"
        )
    count = int(match.group("count"))
    return (
        timedelta(minutes=count)
        if match.group("unit") == "Min"
        else timedelta(hours=count)
    )


def _bar_completion_timestamp(value: Any, duration: timedelta) -> datetime:
    """Convert Alpaca's interval-start timestamp to the domain's bar-end timestamp."""

    return _parse_timestamp(value) + duration


def _decimal(value: Any, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise MarketDataPayloadError(f"invalid decimal field {field!r}") from exc


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise MarketDataPayloadError(f"invalid integer field {field!r}")
    try:
        parsed = int(value)
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise MarketDataPayloadError(f"invalid integer field {field!r}") from exc
    if parsed < 0:
        raise MarketDataPayloadError(f"field {field!r} cannot be negative")
    return parsed


class AlpacaMarketDataProvider:
    """Read-only Alpaca Market Data v2 adapter.

    ``feed`` is retained verbatim on every domain record.  The current domain
    enum identifies Alpaca data as ``ALPACA_SIP``; the exact entitlement (for
    example ``sip``, ``delayed_sip`` or ``iex``) remains distinguishable via the
    record's ``feed`` field.
    """

    def __init__(
        self,
        *,
        api_key: str,
        secret_key: str,
        base_url: str = "https://data.alpaca.markets",
        timeout: float = 10.0,
        page_size: int = 10_000,
        transport: HTTPTransport | None = None,
    ) -> None:
        if not api_key or not secret_key:
            raise ValueError("Alpaca API credentials must not be empty")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if not 1 <= page_size <= 10_000:
            raise ValueError("page_size must be between 1 and 10000")
        self._headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
            "Accept": "application/json",
        }
        self._redactions = (api_key, secret_key)
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._page_size = page_size
        self._transport = transport or _urllib_transport

    def _request(self, path: str, params: Mapping[str, Any]) -> dict[str, Any]:
        query = urlencode([(key, str(value)) for key, value in params.items() if value is not None])
        url = f"{self._base_url}{path}"
        if query:
            url = f"{url}?{query}"
        try:
            response = self._transport(url, self._headers, self._timeout)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataTransportError(
                f"market-data transport failed for {url}: {exc.__class__.__name__}"
            ) from exc
        if not 200 <= response.status_code < 300:
            body = response.body.decode("utf-8", errors="replace")
            for secret in self._redactions:
                body = body.replace(secret, "[REDACTED]")
            raise MarketDataHTTPError(response.status_code, url, body)
        try:
            payload = json.loads(response.body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise MarketDataPayloadError(
                f"market-data response for {url} is not valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise MarketDataPayloadError(f"market-data response for {url} must be a JSON object")
        return payload

    def _paged(
        self,
        path: str,
        *,
        collection_key: str,
        params: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        items: list[dict[str, Any]] = []
        page_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            page_params = dict(params)
            page_params["limit"] = self._page_size
            if page_token is not None:
                page_params["page_token"] = page_token
            payload = self._request(path, page_params)
            page_items = payload.get(collection_key)
            if not isinstance(page_items, list):
                raise MarketDataPayloadError(f"provider payload is missing list {collection_key!r}")
            for item in page_items:
                if not isinstance(item, dict):
                    raise MarketDataPayloadError(
                        f"provider {collection_key!r} item must be an object"
                    )
                items.append(item)
            next_token = payload.get("next_page_token")
            if next_token in (None, ""):
                break
            if not isinstance(next_token, str):
                raise MarketDataPayloadError("next_page_token must be a string")
            if next_token in seen_tokens:
                raise MarketDataPayloadError("provider repeated a pagination token")
            seen_tokens.add(next_token)
            page_token = next_token
        return tuple(items)

    @staticmethod
    def _symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @staticmethod
    def _source(_feed: str) -> DataSource:
        return DataSource.ALPACA_SIP

    def get_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        feed: str = "sip",
    ) -> tuple[Bar, ...]:
        normalized = self._symbol(symbol)
        if end <= start:
            raise ValueError("end must be after start")
        if not timeframe.strip() or not feed.strip():
            raise ValueError("timeframe and feed must not be empty")
        bar_duration = _intraday_bar_duration(timeframe)
        raw_bars = self._paged(
            f"/v2/stocks/{normalized}/bars",
            collection_key="bars",
            params={
                "start": _iso_utc(start),
                "end": _iso_utc(end),
                "timeframe": timeframe,
                "feed": feed,
                "sort": "asc",
            },
        )
        try:
            return tuple(
                Bar(
                    symbol=normalized,
                    timestamp=_bar_completion_timestamp(raw.get("t"), bar_duration),
                    open=_decimal(raw.get("o"), "open"),
                    high=_decimal(raw.get("h"), "high"),
                    low=_decimal(raw.get("l"), "low"),
                    close=_decimal(raw.get("c"), "close"),
                    volume=_integer(raw.get("v"), "volume"),
                    vwap=(_decimal(raw.get("vw"), "vwap") if raw.get("vw") is not None else None),
                    source=self._source(feed),
                    feed=feed,
                )
                for raw in raw_bars
            )
        except MarketDataPayloadError:
            raise
        except (ValueError, TypeError) as exc:
            raise MarketDataPayloadError(f"invalid bar payload for {normalized}") from exc

    def historical_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: str = "1Min",
        feed: str = "sip",
    ) -> tuple[Bar, ...]:
        return self.get_bars(symbol, start=start, end=end, timeframe=timeframe, feed=feed)

    def get_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str = "sip",
    ) -> tuple[Quote, ...]:
        normalized = self._symbol(symbol)
        if end <= start:
            raise ValueError("end must be after start")
        if not feed.strip():
            raise ValueError("feed must not be empty")
        raw_quotes = self._paged(
            f"/v2/stocks/{normalized}/quotes",
            collection_key="quotes",
            params={
                "start": _iso_utc(start),
                "end": _iso_utc(end),
                "feed": feed,
                "sort": "asc",
            },
        )
        return tuple(self._quote_from_payload(normalized, feed, raw) for raw in raw_quotes)

    def historical_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str = "sip",
    ) -> tuple[Quote, ...]:
        return self.get_quotes(symbol, start=start, end=end, feed=feed)

    def _quote_from_payload(self, symbol: str, feed: str, raw: Mapping[str, Any]) -> Quote:
        try:
            return Quote(
                symbol=symbol,
                timestamp=_parse_timestamp(raw.get("t")),
                bid=_decimal(raw.get("bp"), "bid"),
                ask=_decimal(raw.get("ap"), "ask"),
                bid_size=_integer(raw.get("bs"), "bid_size"),
                ask_size=_integer(raw.get("as"), "ask_size"),
                source=self._source(feed),
                feed=feed,
            )
        except MarketDataPayloadError:
            raise
        except (ValueError, TypeError) as exc:
            raise MarketDataPayloadError(f"invalid quote payload for {symbol}") from exc

    def get_latest_quote(self, symbol: str, *, feed: str = "iex") -> Quote:
        normalized = self._symbol(symbol)
        if not feed.strip():
            raise ValueError("feed must not be empty")
        payload = self._request(f"/v2/stocks/{normalized}/quotes/latest", {"feed": feed})
        raw_quote = payload.get("quote")
        if not isinstance(raw_quote, dict):
            raise MarketDataPayloadError("provider payload is missing quote object")
        return self._quote_from_payload(normalized, feed, raw_quote)

    def latest_quote(self, symbol: str, *, feed: str = "iex") -> Quote:
        return self.get_latest_quote(symbol, feed=feed)
