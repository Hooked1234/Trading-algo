from datetime import timedelta
from hashlib import sha256

import pytest

from event_trader.documents import FilingDocumentLoader
from event_trader.domain import DocumentRef
from event_trader.providers.ibkr_market import PrecomputedMarketFeatures
from event_trader.snapshot import LiveEventSnapshotFactory


class StubMarketData:
    def __init__(self) -> None:
        self.symbols = []

    def subscribe(self, symbol):
        self.symbols.append(symbol)
        return symbol


class StubMarketBuilder:
    def __init__(self, market) -> None:
        self.market = market

    def build(self, symbol, features):
        assert symbol == features.symbol
        return self.market


class StubFeatures:
    def __init__(self, market) -> None:
        self.market = market

    async def build(self, filing, symbol, *, as_of):
        return PrecomputedMarketFeatures(
            symbol=symbol,
            as_of=as_of,
            last=self.market.last,
            session_vwap=self.market.session_vwap,
            median_dollar_volume_20d=self.market.median_dollar_volume_20d,
            beta_adjusted_return_z=self.market.beta_adjusted_return_z,
            relative_volume=self.market.relative_volume,
            atr_5m=self.market.atr_5m,
        )


@pytest.mark.asyncio
async def test_live_snapshot_requires_hashed_document_and_one_symbol(
    tmp_path, filing, long_market, decision_time
) -> None:
    content = b"<html><body>raised guidance</body></html>"
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    path = raw_root / "filing.html"
    path.write_bytes(content)
    document = DocumentRef(
        url="https://www.sec.gov/example",
        kind="8-K",
        sha256=sha256(content).hexdigest(),
        local_path=str(path),
    )
    complete = filing.model_copy(update={"documents": (document,)})
    market_data = StubMarketData()
    factory = LiveEventSnapshotFactory(
        documents=FilingDocumentLoader(raw_root),
        market_data=market_data,
        market_builder=StubMarketBuilder(long_market),
        features=StubFeatures(long_market),
    )

    snapshot = await factory.build(complete, as_of=decision_time)

    assert snapshot is not None
    assert snapshot.document_text.endswith("raised guidance")
    assert market_data.symbols == ["AAPL"]


@pytest.mark.asyncio
async def test_live_snapshot_fails_closed_on_future_market_or_multiple_symbols(
    tmp_path, filing, long_market, decision_time
) -> None:
    future_market = long_market.model_copy(update={"as_of": decision_time + timedelta(seconds=2)})
    factory = LiveEventSnapshotFactory(
        documents=FilingDocumentLoader(tmp_path),
        market_data=StubMarketData(),
        market_builder=StubMarketBuilder(future_market),
        features=StubFeatures(future_market),
    )
    multi_symbol = filing.model_copy(update={"symbols": ("AAPL", "MSFT")})

    assert await factory.build(multi_symbol, as_of=decision_time) is None
    assert await factory.build(filing, as_of=decision_time) is None
