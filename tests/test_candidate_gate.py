from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from event_trader.candidates import CandidateGate, snapshot_sha256
from event_trader.domain import Direction


def _market(snapshot, **updates):
    return snapshot.model_copy(
        update={"market": snapshot.market.model_copy(update=updates)}
    )


def test_gate_accepts_a_confirmed_long_candidate(snapshot, decision_time) -> None:
    decision = CandidateGate().evaluate(snapshot, decision_time)

    assert decision.accepted is True
    assert decision.direction is Direction.LONG
    assert decision.reason_codes == ()
    assert decision.symbol == "AAPL"
    assert decision.snapshot_sha256 == snapshot_sha256(snapshot)


def test_gate_refuses_when_price_and_vwap_disagree(snapshot, decision_time) -> None:
    conflicting = _market(snapshot, last=Decimal("99.00"))

    decision = CandidateGate().evaluate(conflicting, decision_time)

    assert decision.accepted is False
    assert decision.direction is Direction.NEUTRAL
    assert "DIRECTION_UNCONFIRMED" in decision.reason_codes


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"halted": True}, "SYMBOL_HALTED"),
        ({"last": Decimal("4.00"), "session_vwap": Decimal("3.00")}, "PRICE_BELOW_MINIMUM"),
        ({"median_dollar_volume_20d": Decimal("1000")}, "DOLLAR_VOLUME_BELOW_MINIMUM"),
        ({"relative_volume": 1.0}, "RELATIVE_VOLUME_TOO_LOW"),
        ({"data_fresh": False}, "MARKET_DATA_STALE"),
        ({"security_type": "etf"}, "SECURITY_NOT_US_COMMON_STOCK"),
        ({"beta_adjusted_return_z": 0.5}, "ABNORMAL_RETURN_TOO_SMALL"),
    ],
)
def test_gate_reports_every_deterministic_rejection(
    snapshot, decision_time, updates, reason
) -> None:
    decision = CandidateGate().evaluate(_market(snapshot, **updates), decision_time)

    assert decision.accepted is False
    assert reason in decision.reason_codes


def test_gate_refuses_a_short_without_current_borrow(snapshot, decision_time) -> None:
    short_side = _market(
        snapshot,
        beta_adjusted_return_z=-2.0,
        last=Decimal("99.00"),
        session_vwap=Decimal("100.00"),
        shortable=False,
        shortable_shares=0,
    )

    decision = CandidateGate().evaluate(short_side, decision_time)

    assert decision.direction is Direction.SHORT
    assert decision.accepted is False
    assert "NOT_SHORTABLE" in decision.reason_codes


def test_gate_refuses_outside_the_entry_window(snapshot, decision_time) -> None:
    late = decision_time + timedelta(hours=6)
    shifted = snapshot.model_copy(
        update={
            "market": snapshot.market.model_copy(
                update={
                    "as_of": late,
                    "quote": snapshot.market.quote.model_copy(update={"timestamp": late}),
                }
            )
        }
    )

    decision = CandidateGate().evaluate(shifted, late)

    assert decision.accepted is False
    assert "OUTSIDE_ENTRY_WINDOW" in decision.reason_codes


def test_snapshot_hash_changes_with_the_market_but_not_with_the_clock(
    snapshot, decision_time
) -> None:
    gate = CandidateGate()
    first = gate.evaluate(snapshot, decision_time)
    same_market_later = gate.evaluate(snapshot, decision_time + timedelta(seconds=1))
    moved = gate.evaluate(_market(snapshot, relative_volume=9.0), decision_time)

    assert first.snapshot_sha256 == same_market_later.snapshot_sha256
    assert first.snapshot_sha256 != moved.snapshot_sha256


def test_gate_requires_a_timezone_aware_clock(snapshot, decision_time) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        CandidateGate().evaluate(snapshot, decision_time.replace(tzinfo=None))
