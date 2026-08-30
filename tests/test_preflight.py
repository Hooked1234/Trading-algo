from datetime import timedelta
from decimal import Decimal

import pytest

from event_trader.broker import InMemoryPaperBroker
from event_trader.calendar import NyseSessionCalendar
from event_trader.domain import Direction, OrderIntent, OrderSide, Position
from event_trader.execution import PreSubmitGuardRejected
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
    market_update=None,
    no_market=False,
    limit_price=None,
    symbol=None,
):
    signal = ContinuationStrategy().evaluate(snapshot, long_insight, decision_time)
    assert signal is not None
    market = snapshot.market.model_copy(update={"market_data_live": True, **(market_update or {})})
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
        return None if no_market else market

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
        symbol=symbol or signal.symbol,
        side=OrderSide.BUY,
        quantity=1,
        limit_price=limit_price if limit_price is not None else market.quote.ask,
        created_at=decision_time,
    )
    return guard, intent, sec, market


MARKET_REJECTIONS = [
    pytest.param({}, "MARKET_SNAPSHOT_UNAVAILABLE", {"no_market": True}, id="no-snapshot"),
    pytest.param({"market_data_live": False}, "MARKET_DATA_NOT_LIVE", {}, id="delayed-data"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("market_update, reason, extra", MARKET_REJECTIONS)
async def test_entry_preflight_refuses_on_market_state(
    tmp_path,
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
    market_update,
    reason,
    extra,
) -> None:
    """Every market fact the guard re-reads has to be able to stop the order.

    This whole re-check was previously unmeasured: removing it left the full
    suite green, so the last line of defence before a live submission carried
    no test at all.
    """

    guard, intent, sec, _ = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        market_update=market_update or None,
        **extra,
    )
    with pytest.raises(PreflightRejected, match=reason):
        await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_refuses_a_limit_away_from_the_current_nbbo(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """A buy limit has to sit exactly on the ask the guard just re-read."""

    guard, intent, sec, _ = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        limit_price=Decimal("100.05"),
    )
    with pytest.raises(PreflightRejected, match="LIMIT_NOT_AT_CURRENT_NBBO"):
        await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_refuses_a_spread_beyond_twenty_basis_points(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """The spread limit is checked against the freshly re-read book.

    Asserted against ``_market_reasons`` rather than end-to-end on purpose: the
    strategy and the risk engine emit the same reason code, so an end-to-end
    assertion would stay green even with this check removed and would prove
    nothing about the guard.
    """

    guard, intent, sec, market = _preflight(
        tmp_path, snapshot, long_insight, empty_portfolio, decision_time
    )
    wide = market.quote.model_copy(update={"ask": Decimal("100.30")})
    wide_market = market.model_copy(update={"quote": wide})
    wide_intent = intent.model_copy(update={"limit_price": wide.ask})

    assert "SPREAD_TOO_WIDE" in guard._market_reasons(wide_intent, wide_market, decision_time)
    assert "SPREAD_TOO_WIDE" not in guard._market_reasons(intent, market, decision_time)
    sec.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "market_update, reason",
    [
        pytest.param({"halted": True}, "SYMBOL_HALTED", id="halted"),
        pytest.param({"data_fresh": False}, "MARKET_DATA_STALE", id="not-fresh"),
    ],
)
async def test_exit_preflight_refuses_on_market_state(
    tmp_path,
    snapshot,
    long_insight,
    empty_portfolio,
    decision_time,
    market_update,
    reason,
) -> None:
    """These two facts stop an exit, and only the guard can report them there.

    The exit path never consults the strategy or the risk engine.  Both codes
    are also produced by those two on the entry path, so this is the only route
    on which they can be attributed to the pre-submit guard itself.
    """

    guard, intent, sec, market = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        market_update=market_update,
    )
    exit_intent = intent.model_copy(
        update={
            "order_id": "exit-halted",
            "idempotency_key": "exit-halted",
            "side": OrderSide.SELL,
            "quantity": 1,
            "limit_price": market.quote.bid,
        }
    )
    held = empty_portfolio.model_copy(
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

    async def holding_portfolio(_now):
        return held

    guard.portfolio_provider = holding_portfolio
    with pytest.raises(PreflightRejected, match=reason):
        await guard(exit_intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_refuses_a_stale_quote(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """A quote older than the state age is not evidence about the current book."""

    old = snapshot.market.quote.model_copy(
        update={"timestamp": decision_time - timedelta(seconds=30)}
    )
    guard, intent, sec, _ = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        market_update={"quote": old},
    )
    with pytest.raises(PreflightRejected, match="MARKET_DATA_STALE"):
        await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_refuses_market_state_from_the_future(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """A snapshot dated ahead of the clock cannot describe the present."""

    guard, intent, sec, _ = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        market_update={"as_of": decision_time + timedelta(seconds=30)},
    )
    with pytest.raises(PreflightRejected, match="MARKET_STATE_FROM_FUTURE"):
        await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_refuses_a_snapshot_for_another_symbol(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """The re-read snapshot has to describe the symbol the order is for."""

    guard, intent, sec, _ = _preflight(
        tmp_path,
        snapshot,
        long_insight,
        empty_portfolio,
        decision_time,
        symbol="MSFT",
    )
    with pytest.raises(PreflightRejected, match="MARKET_SYMBOL_MISMATCH"):
        await guard(intent)
    sec.close()


@pytest.mark.asyncio
async def test_entry_preflight_rechecks_current_market_portfolio_and_sec(
    tmp_path, snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    guard, intent, sec, _ = _preflight(
        tmp_path, snapshot, long_insight, empty_portfolio, decision_time
    )
    # Without this the test could only fail on an exception, so a guard that
    # silently returned the wrong thing would stay invisible.
    assert await guard(intent) is True
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

    with pytest.raises(PreSubmitGuardRejected) as rejected:
        await guard(intent)
    assert isinstance(rejected.value, PreflightRejected)
    assert rejected.value.reasons == ("SEC_DAILY_RECONCILIATION_INCOMPLETE",)
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
    # The point of the test is that the exit is allowed, so it has to be
    # asserted: without this the test only proves that nothing raised.
    assert await guard(exit_intent) is True
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
