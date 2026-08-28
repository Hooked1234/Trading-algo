from decimal import Decimal

import pytest

from event_trader.broker import InMemoryPaperBroker
from event_trader.calendar import NyseSessionCalendar
from event_trader.domain import Direction, OrderIntent, OrderSide, Position
from event_trader.preflight import LiveOrderPreflight, PreflightRejected
from event_trader.reconciliation import (
    SQLiteSecReconciliationLedger,
    reconcile_sec_accessions,
)
from event_trader.risk import RiskEngine
from event_trader.strategy import ContinuationStrategy


class Ledger:
    def __init__(self, signal, filing):
        self.signal = signal
        self.filing = filing

    async def get_signal(self, signal_id):
        return self.signal if signal_id == self.signal.signal_id else None

    async def get_filing(self, event_or_accession):
        if event_or_accession in {self.filing.event_id, self.filing.accession_number}:
            return self.filing
        return None


def _preflight(
    tmp_path,
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
    *,
    complete_sec=True,
):
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    market = snapshot.market.model_copy(update={"market_data_live": True})
    broker = InMemoryPaperBroker(
        account_id="DU123456",
        paper_account_allowlist=("DU123456",),
        clock=lambda: decision_time,
    )
    sec = SQLiteSecReconciliationLedger(tmp_path / "sec.sqlite")
    previous = NyseSessionCalendar().previous_session_date(decision_time)
    expected = () if complete_sec else ("0000320193-26-000099",)
    result = reconcile_sec_accessions(
        daily_index_accessions=expected,
        stored_filings=(),
        reconciled_at=decision_time,
    )
    sec.save(session_date=previous, result=result)

    async def market_provider(_symbol, _now):
        return market

    async def portfolio_provider(_now):
        return empty_portfolio

    guard = LiveOrderPreflight(
        broker=broker,
        ledger=Ledger(signal, snapshot.filing),
        market_provider=market_provider,
        portfolio_provider=portfolio_provider,
        risk_engine=RiskEngine(),
        sec_reconciliation=sec,
        clock=lambda: decision_time,
    )
    intent = OrderIntent(
        order_id="entry-1",
        idempotency_key="entry-1",
        signal_id=signal.signal_id,
        account_id="DU123456",
        submission_mode="paper",
        research_promotion_sha256="a" * 64,
        symbol=signal.symbol,
        side=OrderSide.BUY,
        quantity=1,
        limit_price=market.quote.ask,
        created_at=decision_time,
    )
    return guard, intent, sec, market


@pytest.mark.asyncio
async def test_entry_preflight_rechecks_current_market_portfolio_and_sec(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    guard, intent, sec, _ = _preflight(
        tmp_path, snapshot, long_insight, empty_portfolio, decision_time
    )
    await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_blocks_unresolved_sec_gap(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    guard, intent, sec, _ = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        complete_sec=False,
    )

    with pytest.raises(PreflightRejected, match="SEC_DAILY_RECONCILIATION_INCOMPLETE"):
        await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_exit_preflight_is_not_blocked_by_entry_research_guards(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    guard, intent, sec, market = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        complete_sec=False,
    )
    exit_intent = intent.model_copy(
        update={
            "order_id": "exit-1",
            "idempotency_key": "exit-1",
            "side": OrderSide.SELL,
            "limit_price": market.quote.bid,
        }
    )
    matching = empty_portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol=exit_intent.symbol,
                    direction=Direction.LONG,
                    quantity=exit_intent.quantity,
                    market_price=market.last,
                    average_price=market.last,
                ),
            )
        }
    )

    async def matching_portfolio(_now):
        return matching

    guard.portfolio_provider = matching_portfolio
    await guard(exit_intent)
    sec.close()


@pytest.mark.asyncio
async def test_exit_preflight_blocks_a_fresh_broker_position_mismatch(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    guard, intent, sec, market = _preflight(
        tmp_path, snapshot, long_insight, empty_portfolio, decision_time
    )
    exit_intent = intent.model_copy(
        update={
            "order_id": "exit-mismatch",
            "idempotency_key": "exit-mismatch",
            "side": OrderSide.SELL,
            "quantity": 2,
            "limit_price": market.quote.bid,
        }
    )
    mismatched = empty_portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol=exit_intent.symbol,
                    direction=Direction.LONG,
                    quantity=1,
                    market_price=market.last,
                    average_price=market.last,
                ),
            )
        }
    )

    async def mismatched_portfolio(_now):
        return mismatched

    guard.portfolio_provider = mismatched_portfolio
    with pytest.raises(PreflightRejected, match="EXIT_POSITION_QUANTITY_MISMATCH"):
        await guard(exit_intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_counts_existing_symbol_position(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    guard, intent, sec, _ = _preflight(
        tmp_path, snapshot, long_insight, empty_portfolio, decision_time
    )
    occupied = empty_portfolio.model_copy(
        update={
            "positions": (
                Position(
                    symbol="AAPL",
                    direction=Direction.LONG,
                    quantity=1,
                    market_price=Decimal("100"),
                    average_price=Decimal("100"),
                ),
            )
        }
    )

    async def occupied_portfolio(_now):
        return occupied

    guard.portfolio_provider = occupied_portfolio
    with pytest.raises(PreflightRejected, match="DUPLICATE_SYMBOL_POSITION"):
        await guard(intent)
    sec.close()
