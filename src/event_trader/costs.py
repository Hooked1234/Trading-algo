"""Conservative, explicit transaction cost assumptions for research."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256


@dataclass(frozen=True)
class CostModel:
    commission_per_share: Decimal = Decimal("0.0035")
    minimum_commission: Decimal = Decimal("0.35")
    extra_slippage_bps_per_side: Decimal = Decimal("5")

    def __post_init__(self) -> None:
        if self.commission_per_share < 0 or self.minimum_commission < 0:
            raise ValueError("commission assumptions cannot be negative")
        if self.extra_slippage_bps_per_side < 0:
            raise ValueError("slippage assumption cannot be negative")

    @property
    def version(self) -> str:
        """Content address of the cost assumptions bound into every run artifact."""

        payload = (
            f"commission_per_share={self.commission_per_share};"
            f"minimum_commission={self.minimum_commission};"
            f"extra_slippage_bps_per_side={self.extra_slippage_bps_per_side}"
        )
        return f"cost-model-v1/{sha256(payload.encode()).hexdigest()[:16]}"

    def round_trip_cost(
        self,
        *,
        quantity: int,
        entry_price: Decimal,
        exit_price: Decimal,
        entry_spread_bps: Decimal,
        exit_spread_bps: Decimal,
        multiplier: Decimal = Decimal("1"),
    ) -> Decimal:
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if min(entry_price, exit_price) <= 0:
            raise ValueError("prices must be positive")
        if min(entry_spread_bps, exit_spread_bps) < 0:
            raise ValueError("spread assumptions cannot be negative")
        if multiplier <= 0:
            raise ValueError("cost multiplier must be positive")
        commissions = max(
            self.minimum_commission,
            self.commission_per_share * quantity,
        ) * Decimal("2")
        entry_notional = entry_price * quantity
        exit_notional = exit_price * quantity
        spread_cost = entry_notional * entry_spread_bps / Decimal(
            "20000"
        ) + exit_notional * exit_spread_bps / Decimal("20000")
        slippage = (
            (entry_notional + exit_notional) * self.extra_slippage_bps_per_side / Decimal("10000")
        )
        return (commissions + spread_cost + slippage) * multiplier
