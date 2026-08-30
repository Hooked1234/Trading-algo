from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from event_trader.domain import (
    Direction,
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    NewsInsight,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Quote,
    money,
)


def test_domain_models_are_frozen(filing) -> None:
    with pytest.raises(ValidationError):
        filing.form = "8-K/A"


def test_quote_computes_nbbo_spread(long_market) -> None:
    assert long_market.quote.midpoint == Decimal("100.05")
    assert Decimal("9.9") < long_market.quote.spread_bps < Decimal("10.1")


def test_quote_rejects_crossed_market(long_market) -> None:
    payload = long_market.quote.model_dump()
    payload.update(bid=Decimal("101"), ask=Decimal("100"))
    with pytest.raises(ValidationError, match="ask must"):
        Quote.model_validate(payload)


def test_abstention_requires_reason(filing) -> None:
    insight = NewsInsight.abstain(
        event_id=filing.event_id,
        accession_number=filing.accession_number,
        reason="timeout",
    )
    assert insight.direction is Direction.NEUTRAL
    assert insight.abstain_reason == "timeout"


def test_filing_rejects_retrieval_before_first_seen(filing) -> None:
    payload = filing.model_dump()
    payload["retrieved_at"] = filing.first_seen_at - timedelta(seconds=1)
    with pytest.raises(ValidationError, match="retrieved_at"):
        type(filing).model_validate(payload)


def test_domain_timestamps_are_normalized_to_utc(filing) -> None:
    payload = filing.model_dump()
    payload["accepted_at"] = datetime(2026, 8, 25, 9, 37, tzinfo=ZoneInfo("America/New_York"))
    normalized = type(filing).model_validate(payload)
    assert normalized.accepted_at.utcoffset() == timedelta(0)
    assert normalized.accepted_at.hour == 13


def test_portfolio_rejects_peak_nav_below_current_nav(empty_portfolio) -> None:
    with pytest.raises(ValidationError, match="peak NAV"):
        PortfolioState.model_validate(
            {**empty_portfolio.model_dump(), "peak_nav": empty_portfolio.nav - 1}
        )


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf")])
def test_market_snapshot_rejects_non_finite_quant_features(long_market, invalid: float) -> None:
    payload = long_market.model_dump()
    payload["beta_adjusted_return_z"] = invalid

    with pytest.raises(ValidationError, match="finite"):
        type(long_market).model_validate(payload)


def test_order_intent_defaults_to_non_brokerable_shadow(decision_time) -> None:
    payload = {
        "order_id": "entry-1",
        "idempotency_key": "entry-1",
        "signal_id": "signal-1",
        "account_id": "DU123456",
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "quantity": 1,
        "limit_price": Decimal("100"),
        "created_at": decision_time,
    }

    shadow = OrderIntent.model_validate(payload)
    assert shadow.submission_mode == "shadow"
    with pytest.raises(ValidationError, match="research-promotion authorization"):
        OrderIntent.model_validate({**payload, "submission_mode": "paper"})


def _fill_payload(decision_time: datetime) -> dict[str, object]:
    return {
        "order_id": "entry-1",
        "execution_id": "0000e0d5.68ab12cd.01.01",
        "broker_order_id": "417",
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "quantity": 4,
        "price": Decimal("100.10"),
        "cumulative_quantity": 4,
        "occurred_at": decision_time,
    }


def _report_payload(decision_time: datetime) -> dict[str, object]:
    return {
        "order_id": "entry-1",
        "idempotency_key": "entry-1",
        "status": ExecutionStatus.PARTIALLY_FILLED,
        "filled_quantity": 4,
        "average_fill_price": Decimal("100.10"),
        "occurred_at": decision_time,
    }


def test_execution_fill_accepts_a_complete_broker_fill(decision_time) -> None:
    fill = ExecutionFill.model_validate(_fill_payload(decision_time))

    assert fill.commission == Decimal("0")
    assert fill.commission_final is False


def test_execution_fill_rejects_cumulative_below_its_own_quantity(decision_time) -> None:
    payload = _fill_payload(decision_time)
    payload["cumulative_quantity"] = 3

    with pytest.raises(ValidationError, match="cumulative quantity"):
        ExecutionFill.model_validate(payload)


def test_execution_fill_rejects_a_commission_the_broker_has_not_confirmed(
    decision_time,
) -> None:
    payload = _fill_payload(decision_time)
    payload["commission"] = Decimal("0.35")

    with pytest.raises(ValidationError, match="reports it as final"):
        ExecutionFill.model_validate(payload)

    final = ExecutionFill.model_validate({**payload, "commission_final": True})
    assert final.commission == Decimal("0.35")


def test_execution_report_allows_an_aggregate_without_individual_fills(
    decision_time,
) -> None:
    """orderStatus reports a cumulative quantity without naming its fills."""

    report = ExecutionReport.model_validate(_report_payload(decision_time))

    assert report.fill_count == 0
    assert report.update_sequence == 0


def test_execution_report_rejects_more_counted_fills_than_shares(decision_time) -> None:
    payload = _report_payload(decision_time)
    payload["fill_count"] = 5

    with pytest.raises(ValidationError, match="fill count"):
        ExecutionReport.model_validate(payload)


def test_execution_report_rejects_costs_without_a_fill(decision_time) -> None:
    payload = _report_payload(decision_time)
    payload.update(
        status=ExecutionStatus.SUBMITTED,
        filled_quantity=0,
        average_fill_price=Decimal("0"),
    )

    with pytest.raises(ValidationError, match="unfilled order cannot carry fees"):
        ExecutionReport.model_validate({**payload, "fees": Decimal("0.35")})
    with pytest.raises(ValidationError, match="pending commission"):
        ExecutionReport.model_validate({**payload, "pending_commission": True})


def test_order_intent_requires_an_explicit_replacement_lineage(decision_time) -> None:
    payload = {
        "order_id": "entry-1-r1",
        "idempotency_key": "entry-1:r1",
        "signal_id": "signal-1",
        "account_id": "DU123456",
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "quantity": 1,
        "limit_price": Decimal("100"),
        "created_at": decision_time,
    }

    original = OrderIntent.model_validate(payload)
    assert original.reprice_generation == 0
    assert original.replaces_order_id is None

    replacement = OrderIntent.model_validate(
        {**payload, "replaces_order_id": "entry-1", "reprice_generation": 1}
    )
    assert replacement.reprice_generation == 1

    with pytest.raises(ValidationError, match="name exactly the order it replaces"):
        OrderIntent.model_validate({**payload, "reprice_generation": 1})
    with pytest.raises(ValidationError, match="name exactly the order it replaces"):
        OrderIntent.model_validate({**payload, "replaces_order_id": "entry-1"})
    with pytest.raises(ValidationError, match="cannot replace itself"):
        OrderIntent.model_validate(
            {**payload, "replaces_order_id": "entry-1-r1", "reprice_generation": 1}
        )


def test_order_intent_forbids_a_second_reprice_generation(decision_time) -> None:
    payload = {
        "order_id": "entry-1-r2",
        "idempotency_key": "entry-1:r1:r1",
        "signal_id": "signal-1",
        "account_id": "DU123456",
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "quantity": 1,
        "limit_price": Decimal("100"),
        "created_at": decision_time,
        "replaces_order_id": "entry-1-r1",
        "reprice_generation": 2,
    }

    with pytest.raises(ValidationError):
        OrderIntent.model_validate(payload)


def test_money_refuses_an_amount_it_cannot_represent() -> None:
    """An unrepresentable amount is refused, never rounded into something plausible.

    Every broker callback treats ``ArithmeticError`` as a fail-closed fact, so
    the guard has to raise inside that hierarchy rather than return a value the
    contract would reject one layer later.
    """

    assert money(Decimal("10.006666666666666666")) == Decimal("10.00666667")
    assert money(Decimal("999999999999.99999999")) == Decimal("999999999999.99999999")

    with pytest.raises(InvalidOperation):
        money(Decimal("9999999999999.99999999"))
    with pytest.raises(ArithmeticError):
        money(Decimal("1E+30"))
