"""A broker surface that structurally cannot reach an exchange.

Shadow mode must be provably harmless.  Rather than relying on a disabled flag,
the shadow runtime is handed a broker whose submission paths raise, and a
virtual account id that no IBKR allowlist will ever contain.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from .broker import (
    BrokerError,
    BrokerReadiness,
    ReadinessCheck,
    ReadinessProfile,
    ReconciliationResult,
)
from .domain import ExecutionReport, OrderIntent, PortfolioState, utc_now

SHADOW_ACCOUNT_ID = "SHADOW-VIRTUAL"


class ShadowSubmissionRefused(BrokerError):
    """Shadow mode has no broker connection and can never place an order."""


class NonSubmittingShadowBroker:
    """Broker protocol implementation with every write path removed."""

    def __init__(
        self,
        *,
        account_id: str = SHADOW_ACCOUNT_ID,
        strategy_nav: Decimal = Decimal("100000"),
        clock=None,
    ) -> None:
        if account_id.upper().startswith("DU"):
            raise ValueError("the shadow broker must not impersonate a paper account")
        if strategy_nav <= 0:
            raise ValueError("shadow NAV must be positive")
        self.account_id = account_id
        self.strategy_nav = strategy_nav
        self._clock = clock

    def readiness(self, _profile: ReadinessProfile = ReadinessProfile.SUBMIT) -> BrokerReadiness:
        return BrokerReadiness(
            account_id=self.account_id,
            checked_at=self._now(),
            checks=(
                ReadinessCheck(
                    name="broker_submission",
                    ready=False,
                    detail="shadow mode has no broker connection",
                ),
            ),
        )

    def submit(self, intent: OrderIntent) -> ExecutionReport:
        raise ShadowSubmissionRefused(f"shadow mode cannot submit order {intent.order_id!r}")

    def submit_order(self, intent: OrderIntent) -> ExecutionReport:
        return self.submit(intent)

    def cancel(self, order_id: str) -> ExecutionReport:
        raise ShadowSubmissionRefused(f"shadow mode cannot cancel order {order_id!r}")

    def cancel_order(self, order_id: str) -> ExecutionReport:
        return self.cancel(order_id)

    def reconcile(self) -> ReconciliationResult:
        return ReconciliationResult(
            account_id=self.account_id,
            reconciled_at=self._now(),
            executions=(),
            portfolio=self.portfolio_state(),
        )

    def portfolio_state(self, account_id: str | None = None) -> PortfolioState:
        """Return the virtual shadow account.

        ``broker_connected`` and ``reconciled`` describe whether the portfolio
        view can be trusted, not whether a socket is open.  A virtual account is
        its own source of truth, so both hold.  Safety comes from ``readiness``
        never passing and from every submission path raising.
        """

        del account_id
        now = self._now()
        return PortfolioState(
            as_of=now,
            nav=self.strategy_nav,
            peak_nav=self.strategy_nav,
            cash=self.strategy_nav,
            strategy_equity=self.strategy_nav,
            strategy_peak_equity=self.strategy_nav,
            strategy_realized_pnl_today=Decimal("0"),
            strategy_unrealized_pnl=Decimal("0"),
            broker_connected=True,
            reconciled=True,
        )

    def _now(self) -> datetime:
        if self._clock is None:
            return utc_now()
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("shadow broker clock must be timezone-aware")
        return value


async def shadow_repricer(intent: OrderIntent) -> OrderIntent:
    """Repricing is unreachable in shadow mode; reaching it is a defect."""

    raise ShadowSubmissionRefused(f"shadow mode cannot reprice order {intent.order_id!r}")


async def shadow_pre_submit_guard(intent: OrderIntent) -> bool:
    """Always refuse: the guard only exists so the exit monitor can be built."""

    del intent
    return False


__all__ = [
    "SHADOW_ACCOUNT_ID",
    "NonSubmittingShadowBroker",
    "ShadowSubmissionRefused",
    "shadow_pre_submit_guard",
    "shadow_repricer",
]
