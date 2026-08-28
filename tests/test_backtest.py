from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from event_trader.backtest import (
    BacktestCase,
    BacktestEntryPoint,
    BacktestExitPoint,
    HistoricalBacktester,
)
from event_trader.calendar import NyseSessionCalendar
from event_trader.domain import Bar, DataSource, PortfolioState, Quote
from event_trader.risk import RiskEngine
from event_trader.strategy import ContinuationStrategy, QuantOnlyContinuationStrategy


def _point(snapshot, timestamp, *, low: str, high: str, midpoint: str) -> BacktestExitPoint:
    mid = Decimal(midpoint)
    return BacktestExitPoint(
        timestamp=timestamp,
        bar=Bar(
            symbol=snapshot.market.symbol,
            timestamp=timestamp,
            open=mid,
            high=Decimal(high),
            low=Decimal(low),
            close=mid,
            volume=10_000,
            source=DataSource.REPLAY,
            feed="sip",
        ),
        quote=Quote(
            symbol=snapshot.market.symbol,
            timestamp=timestamp,
            bid=mid - Decimal("0.01"),
            ask=mid + Decimal("0.01"),
            bid_size=100,
            ask_size=100,
            source=DataSource.REPLAY,
            feed="sip",
        ),
    )


def _path(
    snapshot,
    decision_time,
    *,
    terminal_minute: int,
    terminal_low: str,
    terminal_high: str,
    terminal_midpoint: str,
) -> tuple[BacktestExitPoint, ...]:
    return tuple(
        _point(
            snapshot,
            decision_time + timedelta(minutes=minute),
            low=terminal_low if minute == terminal_minute else "100.00",
            high=terminal_high if minute == terminal_minute else "102.00",
            midpoint=terminal_midpoint if minute == terminal_minute else "101.00",
        )
        for minute in range(1, terminal_minute + 1)
    )


def test_backtest_closes_at_60_minutes_and_deducts_costs(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot,
        portfolio=empty_portfolio,
        exit_points=_path(
            snapshot,
            decision_time,
            terminal_minute=60,
            terminal_low="100.00",
            terminal_high="102.00",
            terminal_midpoint="101.50",
        ),
    )

    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(case, long_insight)

    assert outcome.stage == "closed_trade"
    assert outcome.trade is not None
    assert outcome.trade.out_of_sample is False
    assert outcome.trade.net_pnl < Decimal(outcome.trade.metadata["gross_pnl"])
    assert outcome.trade.metadata["exit_reason"] == "TIME_EXIT"
    assert outcome.trade.metadata["event_id"] == snapshot.filing.event_id
    assert Decimal(outcome.trade.metadata["stress_net_pnl"]) < outcome.trade.net_pnl


def test_backtest_stop_uses_worse_gapping_open(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot,
        portfolio=empty_portfolio,
        exit_points=_path(
            snapshot,
            decision_time,
            terminal_minute=5,
            terminal_low="97.00",
            terminal_high="99.00",
            terminal_midpoint="98.00",
        ),
    )

    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(case, long_insight)

    assert outcome.trade is not None
    assert outcome.trade.metadata["exit_reason"] == "STOP_EXIT"
    assert outcome.trade.metadata["exit_mid"] == "98.00"


def test_backtest_missing_exit_is_coverage_gap_not_zero_return(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot,
        portfolio=empty_portfolio,
        exit_points=(),
    )

    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(case, long_insight)

    assert outcome.stage == "coverage_gap"
    assert outcome.trade is None
    assert outcome.reasons == ("MISSING_EXIT_COVERAGE",)


def test_backtest_missing_intrahour_minute_is_coverage_gap(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    full_path = _path(
        snapshot,
        decision_time,
        terminal_minute=60,
        terminal_low="100.00",
        terminal_high="102.00",
        terminal_midpoint="101.50",
    )
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot,
        portfolio=empty_portfolio,
        exit_points=tuple(
            point
            for point in full_path
            if point.timestamp != decision_time + timedelta(minutes=17)
        ),
    )

    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(case, long_insight)

    assert outcome.stage == "coverage_gap"
    assert outcome.trade is None


def test_backtest_rejects_stale_decision_and_exit_quotes(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    stale_decision_quote = snapshot.market.quote.model_copy(
        update={"timestamp": decision_time - timedelta(seconds=6)}
    )
    stale_snapshot = snapshot.model_copy(
        update={
            "market": snapshot.market.model_copy(update={"quote": stale_decision_quote})
        }
    )
    with pytest.raises(ValueError, match="decision quote is stale"):
        BacktestCase(
            decision_time=decision_time,
            snapshot=stale_snapshot,
            portfolio=empty_portfolio,
            exit_points=(),
        )

    exit_time = decision_time + timedelta(minutes=1)
    fresh_point = _point(
        snapshot,
        exit_time,
        low="100",
        high="102",
        midpoint="101",
    )
    with pytest.raises(ValueError, match="exit quote is stale"):
        BacktestExitPoint(
            timestamp=exit_time,
            bar=fresh_point.bar,
            quote=fresh_point.quote.model_copy(
                update={"timestamp": exit_time - timedelta(seconds=6)}
            ),
        )


def test_quant_only_backtest_uses_deterministic_filing_item_category(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot,
        portfolio=empty_portfolio,
        exit_points=_path(
            snapshot,
            decision_time,
            terminal_minute=60,
            terminal_low="100.00",
            terminal_high="102.00",
            terminal_midpoint="101.50",
        ),
    )

    outcome = HistoricalBacktester(strategy=QuantOnlyContinuationStrategy()).run_case(case)

    assert outcome.trade is not None
    assert outcome.trade.category == "earnings"


def test_backtest_aborts_when_bounded_limit_and_reprice_do_not_fill(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    empty_quote = snapshot.market.quote.model_copy(
        update={"bid_size": 0, "ask_size": 0}
    )
    empty_snapshot = snapshot.model_copy(
        update={
            "market": snapshot.market.model_copy(update={"quote": empty_quote})
        }
    )
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=empty_snapshot,
        portfolio=empty_portfolio,
        exit_points=(),
    )

    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(case, long_insight)

    assert outcome.stage == "entry_unfilled"
    assert outcome.trade is None


def test_backtest_models_single_five_second_reprice(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    empty_quote = snapshot.market.quote.model_copy(
        update={"bid_size": 0, "ask_size": 0}
    )
    empty_snapshot = snapshot.model_copy(
        update={
            "market": snapshot.market.model_copy(update={"quote": empty_quote})
        }
    )
    replacement = snapshot.market.quote.model_copy(
        update={
            "timestamp": decision_time,
            "bid": Decimal("100.10"),
            "ask": Decimal("100.20"),
        }
    )
    case = BacktestCase(
        decision_time=decision_time,
        snapshot=empty_snapshot,
        portfolio=empty_portfolio,
        entry_reprice=BacktestEntryPoint(
            attempted_at=decision_time + timedelta(seconds=5),
            quote=replacement,
        ),
        exit_points=_path(
            snapshot,
            decision_time,
            terminal_minute=60,
            terminal_low="100.00",
            terminal_high="102.00",
            terminal_midpoint="101.50",
        ),
    )

    outcome = HistoricalBacktester(strategy=ContinuationStrategy()).run_case(case, long_insight)

    assert outcome.trade is not None
    assert outcome.trade.opened_at == decision_time + timedelta(seconds=5)
    assert outcome.trade.metadata["entry_attempts"] == 2


def test_backtest_sample_split_uses_filing_acceptance_across_year_boundary(
    snapshot, long_insight, empty_portfolio
) -> None:
    accepted_at = datetime(2023, 12, 31, 20, 0, tzinfo=UTC)
    decision_time = NyseSessionCalendar().next_evaluation_time(
        accepted_at + timedelta(minutes=5)
    )
    assert decision_time is not None
    historical_filing = snapshot.filing.model_copy(
        update={
            "accepted_at": accepted_at,
            "first_seen_at": accepted_at,
            "retrieved_at": accepted_at,
        }
    )
    historical_market = snapshot.market.model_copy(
        update={
            "as_of": decision_time,
            "quote": snapshot.market.quote.model_copy(update={"timestamp": decision_time}),
        }
    )

    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot.model_copy(
            update={"filing": historical_filing, "market": historical_market}
        ),
        portfolio=empty_portfolio.model_copy(update={"as_of": decision_time}),
        exit_points=(),
        out_of_sample=False,
    )

    assert case.out_of_sample is False


def test_backtest_carries_overlapping_positions_through_shared_risk_state(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    cases = []
    insights = {}
    for index in range(3):
        symbol = f"S{index}"
        accession = f"0000320193-26-{index + 10:06d}"
        filing = snapshot.filing.model_copy(
            update={
                "event_id": f"event-{index}",
                "accession_number": accession,
                "symbols": (symbol,),
            }
        )
        quote = snapshot.market.quote.model_copy(update={"symbol": symbol})
        market = snapshot.market.model_copy(update={"symbol": symbol, "quote": quote})
        event_snapshot = snapshot.model_copy(update={"filing": filing, "market": market})
        insights[filing.event_id] = long_insight.model_copy(
            update={"event_id": filing.event_id, "accession_number": accession}
        )
        cases.append(
            BacktestCase(
                decision_time=decision_time,
                snapshot=event_snapshot,
                portfolio=empty_portfolio,
                exit_points=_path(
                    event_snapshot,
                    decision_time,
                    terminal_minute=60,
                    terminal_low="100.00",
                    terminal_high="102.00",
                    terminal_midpoint="101.50",
                ),
            )
        )

    outcomes = HistoricalBacktester(strategy=ContinuationStrategy()).run(cases, insights)

    assert sum(outcome.stage == "closed_trade" for outcome in outcomes) == 2
    assert outcomes[-1].stage == "risk_rejected"
    assert "NET_EXPOSURE_LIMIT" in outcomes[-1].reasons


def test_backtest_requires_configured_sec_availability_lag(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    with pytest.raises(ValueError, match="historical evaluation time"):
        BacktestCase(
            decision_time=decision_time,
            snapshot=snapshot,
            portfolio=empty_portfolio,
            exit_points=(),
            availability_lag_minutes=10,
        )


def test_backtest_requires_live_equivalent_historical_evaluation_time(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    with pytest.raises(ValueError, match="live-equivalent"):
        BacktestCase(
            decision_time=decision_time - timedelta(minutes=1),
            snapshot=snapshot,
            portfolio=empty_portfolio,
            exit_points=(),
        )


def test_backtest_uses_counterfactual_availability_not_download_timestamp(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    historical_download = snapshot.filing.model_copy(
        update={"retrieved_at": decision_time + timedelta(days=1)}
    )

    case = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot.model_copy(update={"filing": historical_download}),
        portfolio=empty_portfolio,
        exit_points=(),
    )

    assert case.decision_time == decision_time


@pytest.mark.parametrize("future_field", ["snapshot", "quote", "portfolio"])
def test_backtest_rejects_future_decision_inputs(
    snapshot, long_insight, empty_portfolio, decision_time, future_field
) -> None:
    future = decision_time + timedelta(seconds=1)
    candidate_snapshot = snapshot
    candidate_portfolio = empty_portfolio
    if future_field == "snapshot":
        candidate_snapshot = snapshot.model_copy(
            update={"market": snapshot.market.model_copy(update={"as_of": future})}
        )
    elif future_field == "quote":
        candidate_snapshot = snapshot.model_copy(
            update={
                "market": snapshot.market.model_copy(
                    update={"quote": snapshot.market.quote.model_copy(update={"timestamp": future})}
                )
            }
        )
    else:
        candidate_portfolio = empty_portfolio.model_copy(update={"as_of": future})

    with pytest.raises(ValueError, match="cannot be from after"):
        BacktestCase(
            decision_time=decision_time,
            snapshot=candidate_snapshot,
            portfolio=candidate_portfolio,
            exit_points=(),
        )


def test_backtest_exit_bar_cannot_use_a_later_interval(snapshot, decision_time) -> None:
    point_time = decision_time + timedelta(minutes=5)
    with pytest.raises(ValueError, match="must end at"):
        BacktestExitPoint(
            timestamp=point_time,
            bar=_point(
                snapshot,
                point_time + timedelta(minutes=1),
                low="99",
                high="102",
                midpoint="101",
            ).bar,
            quote=_point(
                snapshot,
                point_time,
                low="99",
                high="102",
                midpoint="101",
            ).quote,
        )


class _RecordingRiskEngine(RiskEngine):
    """Risk engine that keeps the portfolio state it was asked to judge."""

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[PortfolioState] = []

    def assess(self, signal, portfolio, market, now):
        self.seen.append(portfolio)
        return super().assess(signal, portfolio, market, now)


def _later_case(snapshot, empty_portfolio, decision_time, *, symbol, minutes):
    """One case whose decision falls ``minutes`` after the reference decision."""

    accepted_at = decision_time + timedelta(minutes=minutes - 10)
    later = decision_time + timedelta(minutes=minutes)
    filing = snapshot.filing.model_copy(
        update={
            "event_id": f"event-{symbol}",
            "accession_number": "0000320193-26-000042",
            "symbols": (symbol,),
            "accepted_at": accepted_at,
            "first_seen_at": accepted_at,
            "retrieved_at": accepted_at,
        }
    )
    quote = snapshot.market.quote.model_copy(
        update={"symbol": symbol, "timestamp": later}
    )
    market = snapshot.market.model_copy(
        update={"symbol": symbol, "as_of": later, "quote": quote}
    )
    event_snapshot = snapshot.model_copy(update={"filing": filing, "market": market})
    return BacktestCase(
        decision_time=later,
        snapshot=event_snapshot,
        portfolio=empty_portfolio.model_copy(update={"as_of": later}),
        exit_points=_path(
            event_snapshot,
            later,
            terminal_minute=60,
            terminal_low="100.00",
            terminal_high="102.00",
            terminal_midpoint="101.50",
        ),
    )


def test_backtest_marks_open_positions_before_the_next_decision(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    first = BacktestCase(
        decision_time=decision_time,
        snapshot=snapshot,
        portfolio=empty_portfolio,
        exit_points=_path(
            snapshot,
            decision_time,
            terminal_minute=60,
            terminal_low="100.00",
            terminal_high="102.00",
            terminal_midpoint="101.50",
        ),
    )
    second = _later_case(
        snapshot, empty_portfolio, decision_time, symbol="MSFT", minutes=30
    )
    insights = {
        snapshot.filing.event_id: long_insight,
        second.snapshot.filing.event_id: long_insight.model_copy(
            update={
                "event_id": second.snapshot.filing.event_id,
                "accession_number": second.snapshot.filing.accession_number,
            }
        ),
    }
    engine = _RecordingRiskEngine()

    outcomes = HistoricalBacktester(
        strategy=ContinuationStrategy(), risk_engine=engine
    ).run([first, second], insights)

    opening_state, second_state = engine.seen
    assert opening_state.positions == ()
    assert opening_state.strategy_unrealized_pnl == Decimal("0")
    # The first position is still open when the second event is judged.
    assert len(second_state.positions) == 1
    held = second_state.positions[0]
    assert held.symbol == "AAPL"
    assert held.market_price == Decimal("101.00")
    assert held.average_price < held.market_price
    assert second_state.strategy_unrealized_pnl > Decimal("0")
    assert second_state.strategy_equity > Decimal("100000")
    assert second_state.strategy_peak_equity >= second_state.strategy_equity
    assert second_state.strategy_realized_pnl_today == Decimal("0")
    assert all(outcome.stage == "closed_trade" for outcome in outcomes)


def test_backtest_orders_simultaneous_events_deterministically(
    snapshot, long_insight, empty_portfolio, decision_time
) -> None:
    """Two events at one instant must be judged in a content-addressed order."""

    cases = []
    for index in range(2):
        symbol = f"T{index}"
        filing = snapshot.filing.model_copy(
            update={
                "event_id": f"event-{index}",
                "accession_number": f"0000320193-26-{index + 50:06d}",
                "symbols": (symbol,),
            }
        )
        quote = snapshot.market.quote.model_copy(update={"symbol": symbol})
        market = snapshot.market.model_copy(update={"symbol": symbol, "quote": quote})
        event_snapshot = snapshot.model_copy(update={"filing": filing, "market": market})
        cases.append(
            BacktestCase(
                decision_time=decision_time,
                snapshot=event_snapshot,
                portfolio=empty_portfolio,
                exit_points=_path(
                    event_snapshot,
                    decision_time,
                    terminal_minute=60,
                    terminal_low="100.00",
                    terminal_high="102.00",
                    terminal_midpoint="101.50",
                ),
            )
        )
    insights = {
        case.snapshot.filing.event_id: long_insight.model_copy(
            update={
                "event_id": case.snapshot.filing.event_id,
                "accession_number": case.snapshot.filing.accession_number,
            }
        )
        for case in cases
    }

    forward = HistoricalBacktester(strategy=ContinuationStrategy()).run(cases, insights)
    reversed_input = HistoricalBacktester(strategy=ContinuationStrategy()).run(
        list(reversed(cases)), insights
    )

    assert [outcome.event_id for outcome in forward] == ["event-0", "event-1"]
    assert forward == reversed_input
