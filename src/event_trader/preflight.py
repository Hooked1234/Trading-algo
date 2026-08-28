"""Fresh, fail-closed checks executed immediately before every broker submit."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

from pydantic import ValidationError

from .broker import Broker, ReadinessProfile
from .calendar import NyseSessionCalendar
from .domain import (
    Direction,
    EventSnapshot,
    FilingEvent,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Signal,
)
from .reconciliation import SQLiteSecReconciliationLedger
from .risk import RiskEngine
from .strategy import ContinuationStrategy


class PreflightRejected(RuntimeError):
    """The order was not sent because a last-moment invariant failed."""

    def __init__(self, reasons: tuple[str, ...]) -> None:
        self.reasons = reasons
        super().__init__(", ".join(reasons))


class PreflightLedger(Protocol):
    async def get_signal(self, signal_id: str) -> Signal | None: ...

    async def get_filing(self, event_or_accession: str) -> FilingEvent | None: ...


MarketStateProvider = Callable[[str, datetime], Awaitable[MarketSnapshot | None]]
PortfolioStateProvider = Callable[[datetime], Awaitable[PortfolioState]]
Clock = Callable[[], datetime]


class LiveOrderPreflight:
    """Re-read quote, market state, portfolio, and risk directly before submit."""

    def __init__(
        self,
        *,
        broker: Broker,
        ledger: PreflightLedger,
        market_provider: MarketStateProvider,
        portfolio_provider: PortfolioStateProvider,
        risk_engine: RiskEngine,
        sec_reconciliation: SQLiteSecReconciliationLedger,
        clock: Clock,
        strategy: ContinuationStrategy | None = None,
        calendar: NyseSessionCalendar | None = None,
        max_state_age: timedelta = timedelta(seconds=5),
    ) -> None:
        if max_state_age <= timedelta(0):
            raise ValueError("preflight state age must be positive")
        self.broker = broker
        self.ledger = ledger
        self.market_provider = market_provider
        self.portfolio_provider = portfolio_provider
        self.risk_engine = risk_engine
        self.sec_reconciliation = sec_reconciliation
        self.clock = clock
        self.strategy = strategy or ContinuationStrategy()
        self.calendar = calendar or NyseSessionCalendar()
        self.max_state_age = max_state_age

    async def __call__(self, intent: OrderIntent) -> bool:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise PreflightRejected(("PREFLIGHT_CLOCK_INVALID",))
        is_entry = intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT}
        profile = ReadinessProfile.SUBMIT if is_entry else ReadinessProfile.EXIT
        self.broker.readiness(profile).require()
        market = await self.market_provider(intent.symbol, now)
        reasons = self._market_reasons(intent, market, now)
        portfolio = await self.portfolio_provider(now)
        reasons.extend(self._portfolio_reasons(portfolio, now))
        if not is_entry:
            reasons.extend(self._exit_position_reasons(intent, portfolio))
            if reasons:
                raise PreflightRejected(tuple(dict.fromkeys(reasons)))
            return True

        required_index_date = self.calendar.previous_session_date(now)
        if self.sec_reconciliation.orders_blocked(required_through=required_index_date):
            reasons.append("SEC_DAILY_RECONCILIATION_INCOMPLETE")
        signal = await self.ledger.get_signal(intent.signal_id)
        if signal is None:
            reasons.append("SIGNAL_NOT_FOUND")
        filing = await self.ledger.get_filing(signal.event_id) if signal is not None else None
        if filing is None:
            reasons.append("FILING_NOT_FOUND")
        if reasons or market is None or signal is None or filing is None:
            raise PreflightRejected(tuple(dict.fromkeys(reasons)))

        snapshot = EventSnapshot(filing=filing, market=market, document_text="preflight")
        reasons.extend(self.strategy.quant_rejection_reasons(snapshot, signal.direction, now))
        try:
            refreshed_signal = Signal.model_validate(
                {
                    **signal.model_dump(),
                    "entry_limit": (
                        market.quote.ask
                        if intent.side is OrderSide.BUY
                        else market.quote.bid
                    ),
                }
            )
        except (ValidationError, ValueError):
            reasons.append("REFRESHED_SIGNAL_INVALID")
            raise PreflightRejected(tuple(dict.fromkeys(reasons))) from None

        decision = self.risk_engine.assess(refreshed_signal, portfolio, market, now)
        if not decision.approved:
            reasons.extend(decision.reason_codes)
        elif intent.quantity > decision.quantity:
            reasons.append("PREFLIGHT_QUANTITY_EXCEEDS_RISK")
        if reasons:
            raise PreflightRejected(tuple(dict.fromkeys(reasons)))
        return True

    def _market_reasons(
        self,
        intent: OrderIntent,
        market: MarketSnapshot | None,
        now: datetime,
    ) -> list[str]:
        if market is None:
            return ["MARKET_SNAPSHOT_UNAVAILABLE"]
        reasons: list[str] = []
        if market.symbol != intent.symbol:
            reasons.append("MARKET_SYMBOL_MISMATCH")
        if market.as_of > now or market.quote.timestamp > now:
            reasons.append("MARKET_STATE_FROM_FUTURE")
        elif now - market.quote.timestamp > self.max_state_age:
            reasons.append("MARKET_DATA_STALE")
        if not market.data_fresh:
            reasons.append("MARKET_DATA_STALE")
        if not market.market_data_live:
            reasons.append("MARKET_DATA_NOT_LIVE")
        if market.halted:
            reasons.append("SYMBOL_HALTED")
        expected_limit = (
            market.quote.ask
            if intent.side in {OrderSide.BUY, OrderSide.BUY_TO_COVER}
            else market.quote.bid
        )
        if intent.limit_price != expected_limit:
            reasons.append("LIMIT_NOT_AT_CURRENT_NBBO")
        if intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT} and (
            market.quote.spread_bps > Decimal("20")
        ):
            reasons.append("SPREAD_TOO_WIDE")
        return list(dict.fromkeys(reasons))

    def _portfolio_reasons(
        self,
        portfolio: PortfolioState,
        now: datetime,
    ) -> list[str]:
        reasons: list[str] = []
        if not portfolio.broker_connected:
            reasons.append("PORTFOLIO_BROKER_DISCONNECTED")
        if not portfolio.reconciled:
            reasons.append("PORTFOLIO_NOT_RECONCILED")
        if portfolio.as_of > now:
            reasons.append("PORTFOLIO_STATE_FROM_FUTURE")
        elif now - portfolio.as_of > self.max_state_age:
            reasons.append("PORTFOLIO_STATE_STALE")
        return reasons

    @staticmethod
    def _exit_position_reasons(
        intent: OrderIntent,
        portfolio: PortfolioState,
    ) -> list[str]:
        positions = [
            position
            for position in portfolio.positions
            if position.symbol.strip().upper() == intent.symbol.strip().upper()
        ]
        if not positions:
            return ["EXIT_POSITION_NOT_FOUND"]
        position = positions[0]
        expected_side = (
            OrderSide.SELL
            if position.direction is Direction.LONG
            else OrderSide.BUY_TO_COVER
        )
        reasons: list[str] = []
        if intent.side is not expected_side:
            reasons.append("EXIT_POSITION_DIRECTION_MISMATCH")
        if intent.quantity != position.quantity:
            reasons.append("EXIT_POSITION_QUANTITY_MISMATCH")
        return reasons


__all__ = ["LiveOrderPreflight", "PreflightRejected"]
