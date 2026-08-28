"""Fail-closed construction of live EventSnapshot records."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable
from datetime import datetime, timedelta
from typing import Protocol, cast

from .documents import DocumentIntegrityError, FilingDocumentLoader
from .domain import EventSnapshot, FilingEvent
from .providers.ibkr_market import (
    IBKRMarketDataError,
    IBKRMarketDataProvider,
    PrecomputedMarketFeatures,
    SnapshotBuilder,
)


class LiveFeatureProvider(Protocol):
    def build(
        self,
        filing: FilingEvent,
        symbol: str,
        *,
        as_of: datetime,
    ) -> PrecomputedMarketFeatures | Awaitable[PrecomputedMarketFeatures]: ...


class LiveEventSnapshotFactory:
    """Combine hash-verified filing text with ready IBKR market facts."""

    def __init__(
        self,
        *,
        documents: FilingDocumentLoader,
        market_data: IBKRMarketDataProvider,
        market_builder: SnapshotBuilder,
        features: LiveFeatureProvider,
        future_tolerance: timedelta = timedelta(seconds=1),
    ) -> None:
        if future_tolerance < timedelta(0):
            raise ValueError("future_tolerance cannot be negative")
        self._documents = documents
        self._market_data = market_data
        self._market_builder = market_builder
        self._features = features
        self._future_tolerance = future_tolerance

    async def build(
        self,
        filing: FilingEvent,
        *,
        as_of: datetime,
    ) -> EventSnapshot | None:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("snapshot time must be timezone-aware")
        if not filing.complete or len(filing.symbols) != 1 or not filing.documents:
            return None
        symbol = filing.symbols[0].upper()
        try:
            self._market_data.subscribe(symbol)
            feature_result = self._features.build(filing, symbol, as_of=as_of)
            features = (
                await cast(Awaitable[PrecomputedMarketFeatures], feature_result)
                if inspect.isawaitable(feature_result)
                else feature_result
            )
            market = self._market_builder.build(symbol, features)
            if market.as_of > as_of + self._future_tolerance:
                return None
            text = await asyncio.to_thread(self._documents.load_text, filing)
            return EventSnapshot(filing=filing, market=market, document_text=text)
        except (IBKRMarketDataError, DocumentIntegrityError, OSError, ValueError):
            return None
