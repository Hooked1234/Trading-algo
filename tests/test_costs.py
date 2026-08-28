from decimal import Decimal

import pytest

from event_trader.costs import CostModel


def test_cost_model_includes_commission_spread_and_slippage() -> None:
    base = CostModel().round_trip_cost(
        quantity=100,
        entry_price=Decimal("100"),
        exit_price=Decimal("101"),
        entry_spread_bps=Decimal("10"),
        exit_spread_bps=Decimal("10"),
    )
    stressed = CostModel().round_trip_cost(
        quantity=100,
        entry_price=Decimal("100"),
        exit_price=Decimal("101"),
        entry_spread_bps=Decimal("10"),
        exit_spread_bps=Decimal("10"),
        multiplier=Decimal("2"),
    )
    assert base > Decimal("0.70")
    assert stressed == base * 2


def test_cost_model_rejects_optimistic_negative_costs() -> None:
    with pytest.raises(ValueError, match="slippage"):
        CostModel(extra_slippage_bps_per_side=Decimal("-1"))
    with pytest.raises(ValueError, match="spread"):
        CostModel().round_trip_cost(
            quantity=1,
            entry_price=Decimal("10"),
            exit_price=Decimal("10"),
            entry_spread_bps=Decimal("-1"),
            exit_spread_bps=Decimal("1"),
        )
