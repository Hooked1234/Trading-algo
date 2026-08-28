from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from event_trader.calendar import NyseSessionCalendar
from event_trader.domain import Direction, TradeResult
from event_trader.research import (
    ModelBenchmarkResult,
    PairedImprovementResult,
    ResearchGateEvaluator,
    ResearchGateResult,
    paired_variant_improvement,
    select_model,
)


def _passing_trades() -> list[TradeResult]:
    start = datetime(2025, 1, 2, 15, 0, tzinfo=UTC)
    trades = []
    calendar = NyseSessionCalendar()
    opened = start
    for index in range(240):
        while not calendar.is_session(opened.date()):
            opened += timedelta(days=1)
        trades.append(
            TradeResult(
                trade_id=str(index),
                symbol=f"S{index % 8}",
                direction=Direction.LONG if index % 2 == 0 else Direction.SHORT,
                category=f"C{index % 8}",
                opened_at=opened,
                closed_at=opened + timedelta(hours=1),
                net_pnl=Decimal("1"),
                return_bps=1,
                strategy_variant="v1",
                out_of_sample=True,
                metadata={
                    "event_id": f"event-{index}",
                    "accession_number": f"0000000001-25-{index:06d}",
                    "filing_accepted_at": (opened - timedelta(minutes=10)).isoformat(),
                    "coverage_record_id": f"coverage-{index}",
                    "scenario": "source_lag_5m_primary",
                    "provider": "alpaca",
                    "feed": "sip",
                    "availability_lag_minutes": 5,
                    "sample_period": "holdout",
                    "case_input_sha256": f"{index:064x}"[-64:],
                    "stress_net_pnl": Decimal("0.25"),
                },
            )
        )
        opened += timedelta(days=1)
    return trades


def test_research_gate_passes_stable_diversified_edge() -> None:
    result = ResearchGateEvaluator(bootstrap_iterations=500).evaluate(
        _passing_trades(), strategy_version="v1"
    )
    assert result.passed
    assert result.lower_confidence_bound > 0


def test_research_gate_fails_insufficient_sample() -> None:
    result = ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
        _passing_trades()[:40], strategy_version="v1"
    )
    assert not result.passed
    minimum = next(check for check in result.checks if check.name == "minimum_oos_trades")
    assert minimum.passed is False


def test_trade_result_is_not_oos_unless_explicitly_marked() -> None:
    data = _passing_trades()[0].model_dump()
    data.pop("out_of_sample")

    assert TradeResult.model_validate(data).out_of_sample is False


def test_forward_paper_results_cannot_be_relabelled_as_historical_oos() -> None:
    trades = _passing_trades()
    opened = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    trades[0] = trades[0].model_copy(
        update={
            "opened_at": opened,
            "closed_at": opened + timedelta(hours=1),
            "metadata": {
                **trades[0].metadata,
                "filing_accepted_at": (opened - timedelta(minutes=10)).isoformat(),
                "sample_period": "forward",
            },
        }
    )

    with pytest.raises(ValueError, match="filing-acceptance sample period"):
        ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
            trades, strategy_version="v1"
        )


def test_registered_test_period_cannot_be_hidden_as_in_sample() -> None:
    trades = _passing_trades()
    trades[0] = trades[0].model_copy(update={"out_of_sample": False})

    with pytest.raises(ValueError, match="derived from filing-acceptance sample period"):
        ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
            trades, strategy_version="v1"
        )


def test_research_gate_fails_when_stress_pnl_is_missing() -> None:
    trades = _passing_trades()
    trades[0] = trades[0].model_copy(
        update={
            "metadata": {
                key: value
                for key, value in trades[0].metadata.items()
                if key != "stress_net_pnl"
            }
        }
    )

    result = ResearchGateEvaluator(bootstrap_iterations=100).evaluate(trades, strategy_version="v1")

    stress = next(check for check in result.checks if check.name == "double_cost_stress")
    assert not result.passed
    assert not stress.passed
    assert stress.observed == "missing or invalid for 1 trades"


def test_research_gate_requires_fifty_independent_oos_days() -> None:
    unique_days = [trade.opened_at for trade in _passing_trades()[:40]]
    trades = []
    for index, trade in enumerate(_passing_trades()):
        opened_at = unique_days[index % 40] + timedelta(minutes=index // 40)
        trades.append(
            trade.model_copy(
                update={"opened_at": opened_at, "closed_at": opened_at + timedelta(hours=1)}
            )
        )

    result = ResearchGateEvaluator(bootstrap_iterations=100).evaluate(trades, strategy_version="v1")

    minimum_days = next(
        check for check in result.checks if check.name == "minimum_independent_oos_days"
    )
    assert not result.passed
    assert minimum_days.observed == 40
    assert not minimum_days.passed


def test_research_gate_rejects_duplicate_trade_ids() -> None:
    trades = _passing_trades()
    trades[1] = trades[1].model_copy(update={"trade_id": trades[0].trade_id})

    with pytest.raises(ValueError, match="unique trade_id"):
        ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
            trades, strategy_version="v1"
        )


def test_research_gate_rejects_duplicate_event_candidates() -> None:
    trades = _passing_trades()
    trades[1] = trades[1].model_copy(
        update={"metadata": {**trades[1].metadata, "event_id": " event-0 "}}
    )

    with pytest.raises(ValueError, match=r"unique metadata\.event_id"):
        ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
            trades, strategy_version="v1"
        )


@pytest.mark.parametrize("event_id", [None, "", "   "])
def test_research_gate_requires_non_empty_event_id(event_id: object) -> None:
    trades = _passing_trades()
    trades[0] = trades[0].model_copy(
        update={"metadata": {**trades[0].metadata, "event_id": event_id}}
    )

    with pytest.raises(ValueError, match=r"non-empty metadata\.event_id"):
        ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
            trades, strategy_version="v1"
        )


def test_research_gate_rejects_holding_period_above_sixty_minutes() -> None:
    trades = _passing_trades()
    trades[0] = trades[0].model_copy(
        update={"closed_at": trades[0].opened_at + timedelta(minutes=60, seconds=1)}
    )

    with pytest.raises(ValueError, match="must not exceed 60 minutes"):
        ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
            trades, strategy_version="v1"
        )


def test_research_gate_allows_exactly_sixty_minutes() -> None:
    ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
        _passing_trades(), strategy_version="v1"
    )


def test_model_selection_prioritizes_precision_then_cost() -> None:
    candidates = [
        ModelBenchmarkResult(
            model_id="cheap",
            reference_count=100,
            prediction_count=100,
            schema_valid_rate=1,
            actionable_precision=0.82,
            macro_f1=0.75,
            p95_latency_seconds=10,
            average_cost_eur=0.005,
        ),
        ModelBenchmarkResult(
            model_id="precise",
            reference_count=100,
            prediction_count=100,
            schema_valid_rate=1,
            actionable_precision=0.90,
            macro_f1=0.80,
            p95_latency_seconds=20,
            average_cost_eur=0.019,
        ),
    ]
    assert select_model(candidates).model_id == "precise"


def test_ai_variant_requires_positive_paired_increment() -> None:
    baseline = [
        trade.model_copy(update={"strategy_variant": "quant", "net_pnl": Decimal("1")})
        for trade in _passing_trades()[:120]
    ]
    candidate = [
        trade.model_copy(
            update={
                "trade_id": f"ai:{trade.trade_id}",
                "strategy_variant": "ai",
                "net_pnl": Decimal("2"),
            }
        )
        for trade in _passing_trades()[:120]
    ]
    result = paired_variant_improvement(
        baseline + candidate,
        baseline_version="quant",
        candidate_version="ai",
        iterations=500,
    )
    assert result.passed
    assert result.matched_trades == 120
    assert result.complete_pairing


def test_ai_abstention_is_included_as_zero_pnl() -> None:
    source = _passing_trades()[:120]
    baseline = [
        trade.model_copy(update={"strategy_variant": "quant", "net_pnl": Decimal("-1")})
        for trade in source
    ]
    candidate = [
        trade.model_copy(
            update={
                "trade_id": f"ai:{trade.trade_id}",
                "strategy_variant": "ai",
                "net_pnl": Decimal("0"),
            }
        )
        for trade in source[:100]
    ]
    abstentions = [str(trade.metadata["event_id"]) for trade in source[100:]]

    result = paired_variant_improvement(
        baseline + candidate,
        baseline_version="quant",
        candidate_version="ai",
        candidate_abstention_event_ids=abstentions,
        iterations=500,
    )

    assert result.passed
    assert result.complete_pairing
    assert result.matched_trades == 120
    assert result.mean_incremental_pnl == 1


def test_ai_variant_cannot_drop_unfavourable_baseline_pairs() -> None:
    baseline = [
        trade.model_copy(update={"strategy_variant": "quant"}) for trade in _passing_trades()[:120]
    ]
    candidate = [
        trade.model_copy(update={"strategy_variant": "ai", "net_pnl": Decimal("2")})
        for trade in _passing_trades()[:100]
    ]

    result = paired_variant_improvement(
        baseline + candidate,
        baseline_version="quant",
        candidate_version="ai",
        iterations=100,
    )

    assert not result.passed
    assert not result.complete_pairing


def test_paired_comparison_rejects_duplicate_event_ids() -> None:
    baseline = [
        trade.model_copy(update={"strategy_variant": "quant"}) for trade in _passing_trades()[:120]
    ]
    baseline[1] = baseline[1].model_copy(update={"metadata": baseline[0].metadata})
    candidate = [
        trade.model_copy(update={"strategy_variant": "ai"}) for trade in _passing_trades()[:120]
    ]

    with pytest.raises(ValueError, match="unique event ids"):
        paired_variant_improvement(
            baseline + candidate,
            baseline_version="quant",
            candidate_version="ai",
            iterations=100,
        )


def test_paired_comparison_requires_explicit_event_ids() -> None:
    baseline = [
        trade.model_copy(
            update={
                "strategy_variant": "quant",
                "metadata": {
                    key: value
                    for key, value in trade.metadata.items()
                    if key != "event_id"
                },
            }
        )
        for trade in _passing_trades()[:120]
    ]
    candidate = [
        trade.model_copy(update={"strategy_variant": "ai"}) for trade in _passing_trades()[:120]
    ]

    with pytest.raises(ValueError, match="event_id"):
        paired_variant_improvement(
            baseline + candidate,
            baseline_version="quant",
            candidate_version="ai",
            iterations=100,
        )


def test_paired_comparison_requires_fifty_independent_days() -> None:
    source = _passing_trades()[:120]
    start = datetime(2025, 1, 2, 15, 0, tzinfo=UTC)
    compressed = []
    for index, trade in enumerate(source):
        opened_at = start + timedelta(days=index % 40, minutes=index // 40)
        compressed.append(
            trade.model_copy(
                update={"opened_at": opened_at, "closed_at": opened_at + timedelta(hours=1)}
            )
        )
    baseline = [
        trade.model_copy(update={"strategy_variant": "quant", "net_pnl": Decimal("1")})
        for trade in compressed
    ]
    candidate = [
        trade.model_copy(update={"strategy_variant": "ai", "net_pnl": Decimal("2")})
        for trade in compressed
    ]

    result = paired_variant_improvement(
        baseline + candidate,
        baseline_version="quant",
        candidate_version="ai",
        iterations=100,
    )

    assert not result.passed
    assert result.complete_pairing


def test_concentration_uses_total_net_profit_not_only_winners() -> None:
    trades = _passing_trades()
    concentrated = [
        trade.model_copy(
            update={
                "net_pnl": Decimal("10") if trade.symbol == "S0" else Decimal("-0.1"),
                "metadata": {**trade.metadata, "stress_net_pnl": Decimal("0")},
            }
        )
        for trade in trades
    ]

    result = ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
        concentrated, strategy_version="v1"
    )

    symbol_check = next(check for check in result.checks if check.name == "symbol_concentration")
    assert not symbol_check.passed


def test_long_and_short_must_each_pass_stability_and_cost_gates() -> None:
    trades = [
        trade.model_copy(
            update={
                "net_pnl": Decimal("3")
                if trade.direction is Direction.LONG
                else Decimal("-1"),
                "metadata": {
                    **trade.metadata,
                    "stress_net_pnl": (
                        Decimal("1")
                        if trade.direction is Direction.LONG
                        else Decimal("-2")
                    ),
                },
            }
        )
        for trade in _passing_trades()
    ]

    result = ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
        trades, strategy_version="v1"
    )

    short_stress = next(
        check for check in result.checks if check.name == "short_double_cost_stress"
    )
    short_stability = next(
        check for check in result.checks if check.name == "short_chronological_stability"
    )
    assert not result.passed
    assert not short_stress.passed
    assert not short_stability.passed


def test_research_and_paired_pass_flags_cannot_be_hand_edited() -> None:
    research = ResearchGateEvaluator(bootstrap_iterations=100).evaluate(
        _passing_trades(), strategy_version="v1"
    )
    research_payload = research.model_dump()
    research_payload["passed"] = False
    with pytest.raises(ValidationError, match="derived from all checks"):
        ResearchGateResult.model_validate(research_payload)

    source = _passing_trades()[:120]
    paired = paired_variant_improvement(
        [
            *(
                trade.model_copy(update={"strategy_variant": "quant"})
                for trade in source
            ),
            *(
                trade.model_copy(
                    update={"strategy_variant": "ai", "net_pnl": Decimal("2")}
                )
                for trade in source
            ),
        ],
        baseline_version="quant",
        candidate_version="ai",
        iterations=100,
    )
    paired_payload = paired.model_dump()
    paired_payload["passed"] = False
    with pytest.raises(ValidationError, match="derived from paired evidence"):
        PairedImprovementResult.model_validate(paired_payload)
