from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from event_trader.domain import DataSource
from event_trader.providers.market import (
    AlpacaMarketDataProvider,
    HTTPResponse,
    MarketDataHTTPError,
    MarketDataPayloadError,
)


class FakeTransport:
    def __init__(self, responses: list[HTTPResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Mapping[str, str], float]] = []

    def __call__(self, url: str, headers: Mapping[str, str], timeout: float) -> HTTPResponse:
        self.calls.append((url, headers, timeout))
        return self.responses.pop(0)


def response(payload: object, status: int = 200) -> HTTPResponse:
    return HTTPResponse(status, json.dumps(payload).encode())


def provider(transport: FakeTransport) -> AlpacaMarketDataProvider:
    return AlpacaMarketDataProvider(
        api_key="key-id",
        secret_key="super-secret",
        base_url="https://data.test",
        page_size=1,
        transport=transport,
    )


def test_alpaca_bars_paginate_and_preserve_feed_metadata() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "bars": [
                        {
                            "t": "2026-08-25T13:30:00Z",
                            "o": 100,
                            "h": 102,
                            "l": 99,
                            "c": 101,
                            "v": 1000,
                            "vw": 100.5,
                        }
                    ],
                    "next_page_token": "next-token",
                }
            ),
            response(
                {
                    "bars": [
                        {
                            "t": "2026-08-25T13:31:00Z",
                            "o": 101,
                            "h": 103,
                            "l": 100,
                            "c": 102,
                            "v": 1200,
                            "vw": 101.5,
                        }
                    ],
                    "next_page_token": None,
                }
            ),
        ]
    )

    bars = provider(transport).get_bars(
        " aapl ",
        start=datetime(2026, 8, 25, 13, 30, tzinfo=UTC),
        end=datetime(2026, 8, 25, 13, 32, tzinfo=UTC),
        feed="sip",
    )

    assert [bar.symbol for bar in bars] == ["AAPL", "AAPL"]
    assert [bar.feed for bar in bars] == ["sip", "sip"]
    assert all(bar.source is DataSource.ALPACA_SIP for bar in bars)
    assert [bar.timestamp for bar in bars] == [
        datetime(2026, 8, 25, 13, 31, tzinfo=UTC),
        datetime(2026, 8, 25, 13, 32, tzinfo=UTC),
    ]
    assert "page_token=next-token" in transport.calls[1][0]
    assert "feed=sip" in transport.calls[0][0]


def test_alpaca_historical_and_latest_quotes_are_typed() -> None:
    transport = FakeTransport(
        [
            response(
                {
                    "quotes": [
                        {
                            "t": "2026-08-25T13:30:00.123456Z",
                            "bp": 99.95,
                            "ap": 100.05,
                            "bs": 400,
                            "as": 500,
                        }
                    ]
                }
            ),
            response(
                {
                    "symbol": "AAPL",
                    "quote": {
                        "t": "2026-08-25T13:31:00Z",
                        "bp": 100,
                        "ap": 100.1,
                        "bs": 200,
                        "as": 300,
                    },
                }
            ),
        ]
    )
    adapter = provider(transport)

    quotes = adapter.get_quotes(
        "AAPL",
        start=datetime(2026, 8, 25, 13, 30, tzinfo=UTC),
        end=datetime(2026, 8, 25, 13, 31, tzinfo=UTC),
        feed="delayed_sip",
    )
    latest = adapter.get_latest_quote("AAPL", feed="iex")

    assert quotes[0].feed == "delayed_sip"
    assert quotes[0].source is DataSource.ALPACA_SIP
    assert latest.feed == "iex"
    assert latest.bid_size == 200


def test_alpaca_http_error_is_clean_and_does_not_leak_credentials() -> None:
    transport = FakeTransport(
        [
            HTTPResponse(
                403,
                b'{"message":"subscription does not permit feed: super-secret"}',
            )
        ]
    )
    adapter = provider(transport)

    with pytest.raises(MarketDataHTTPError) as captured:
        adapter.get_latest_quote("AAPL", feed="sip")

    error_text = str(captured.value)
    assert "HTTP 403" in error_text
    assert "subscription does not permit feed" in error_text
    assert "super-secret" not in error_text
    assert "APCA-API-SECRET-KEY" not in error_text


def test_alpaca_rejects_successful_but_malformed_payload() -> None:
    adapter = provider(FakeTransport([response({"unexpected": []})]))

    with pytest.raises(MarketDataPayloadError, match="missing quote"):
        adapter.get_latest_quote("AAPL")


def test_alpaca_rejects_timezone_naive_ranges_without_network() -> None:
    transport = FakeTransport([])
    adapter = provider(transport)

    with pytest.raises(ValueError, match="timezone-aware"):
        adapter.get_bars(
            "AAPL",
            start=datetime(2026, 8, 25, 13, 30),
            end=datetime(2026, 8, 25, 13, 31),
        )

    assert transport.calls == []


def test_alpaca_rejects_bar_timeframes_without_defined_completion_semantics() -> None:
    transport = FakeTransport([])
    adapter = provider(transport)

    with pytest.raises(ValueError, match="intraday Alpaca timeframes"):
        adapter.get_bars(
            "AAPL",
            start=datetime(2026, 8, 25, 13, 30, tzinfo=UTC),
            end=datetime(2026, 8, 25, 13, 31, tzinfo=UTC) + timedelta(days=1),
            timeframe="1Day",
        )

    assert transport.calls == []
