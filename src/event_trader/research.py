"""Research promotion gates with reproducible, day-blocked bootstrap statistics."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import numpy as np
from pydantic import Field, model_validator

from .backtest import BacktestRunArtifact, BacktestVariant
from .calendar import NEW_YORK, NyseSessionCalendar
from .datasets import period_for
from .domain import Direction, FrozenModel, TradeResult


class GateCheck(FrozenModel):
    name: str
    passed: bool
    observed: float | int | str
    required: str


class ResearchGateResult(FrozenModel):
    passed: bool
    checks: tuple[GateCheck, ...]
    trade_count: int
    lower_confidence_bound: float
    strategy_version: str
    enabled_directions: tuple[Direction, ...]

    @model_validator(mode="after")
    def validate_derived_result(self) -> ResearchGateResult:
        if not self.checks or len({check.name for check in self.checks}) != len(self.checks):
            raise ValueError("research result requires unique gate checks")
        if self.passed != all(check.passed for check in self.checks):
            raise ValueError("research passed flag must be derived from all checks")
        minimum = next(
            (check for check in self.checks if check.name == "minimum_oos_trades"),
            None,
        )
        bootstrap = next(
            (check for check in self.checks if check.name == "bootstrap_net_expectancy"),
            None,
        )
        if minimum is None or minimum.observed != self.trade_count:
            raise ValueError("research trade count does not match its gate check")
        if bootstrap is None or bootstrap.observed != self.lower_confidence_bound:
            raise ValueError("research confidence bound does not match its gate check")
        return self


def _daily_block_bootstrap_lower_bound(
    trades: list[TradeResult],
    *,
    iterations: int = 10_000,
    seed: int = 8_001,
) -> float:
    by_day: dict[object, list[float]] = defaultdict(list)
    for trade in trades:
        by_day[trade.opened_at.date()].append(float(trade.net_pnl))
    days = tuple(sorted(by_day))
    if not days:
        return -1.0e308
    daily_pnls = [np.asarray(by_day[day], dtype=float) for day in days]
    rng = np.random.default_rng(seed)
    estimates = np.empty(iterations, dtype=float)
    for index in range(iterations):
        sampled = rng.integers(0, len(days), size=len(days))
        pnl = sum(float(np.sum(daily_pnls[item])) for item in sampled)
        count = sum(len(daily_pnls[item]) for item in sampled)
        estimates[index] = pnl / count if count else np.nan
    return float(np.nanquantile(estimates, 0.025))


def _positive_chronological_windows(trades: list[TradeResult], windows: int = 4) -> int:
    by_day: dict[date, Decimal] = defaultdict(Decimal)
    for trade in trades:
        by_day[trade.opened_at.date()] += trade.net_pnl
    ordered_days = sorted(by_day)
    if len(ordered_days) < windows:
        return 0
    return sum(
        sum((by_day[ordered_days[index]] for index in split), Decimal("0")) > 0
        for split in np.array_split(np.arange(len(ordered_days)), windows)
        if len(split)
    )


def _max_positive_concentration(trades: list[TradeResult], attribute: str) -> float:
    contributions: dict[str, Decimal] = defaultdict(Decimal)
    for trade in trades:
        contributions[str(getattr(trade, attribute))] += trade.net_pnl
    positive = [value for value in contributions.values() if value > 0]
    total_net = sum(contributions.values(), Decimal("0"))
    return float(max(positive) / total_net) if positive and total_net > 0 else 1.0


def _stressed_total(trades: Sequence[TradeResult]) -> tuple[Decimal | None, int]:
    total = Decimal("0")
    invalid = 0
    for trade in trades:
        raw_value = trade.metadata.get("stress_net_pnl")
        if raw_value is None:
            invalid += 1
            continue
        try:
            value = Decimal(str(raw_value))
        except (ArithmeticError, ValueError):
            invalid += 1
            continue
        if not value.is_finite():
            invalid += 1
            continue
        total += value
    return (total if invalid == 0 else None), invalid


class ResearchGateEvaluator:
    """Evaluate the pre-registered gate without optimizing its thresholds."""

    def __init__(self, *, bootstrap_iterations: int = 10_000, seed: int = 8_001) -> None:
        self.bootstrap_iterations = bootstrap_iterations
        self.seed = seed
        self.calendar = NyseSessionCalendar()

    def evaluate(
        self,
        trades: list[TradeResult],
        *,
        strategy_version: str,
        enabled_directions: tuple[Direction, ...] = (Direction.LONG, Direction.SHORT),
    ) -> ResearchGateResult:
        if not enabled_directions or Direction.NEUTRAL in enabled_directions:
            raise ValueError("enable at least one non-neutral direction")
        if len(set(enabled_directions)) != len(enabled_directions):
            raise ValueError("enabled directions must be unique")
        _validate_frozen_oos_labels(trades)
        eligible = [
            trade
            for trade in trades
            if trade.out_of_sample
            and trade.strategy_variant == strategy_version
            and trade.direction in enabled_directions
        ]
        _validate_gate_trade_integrity(eligible)
        for trade in eligible:
            opened = trade.opened_at.astimezone(NEW_YORK)
            closed = trade.closed_at.astimezone(NEW_YORK)
            if (
                not self.calendar.is_session(opened.date())
                or not time(9, 40) <= opened.time() <= time(14, 45)
                or closed.date() != opened.date()
                or closed.time() > time(15, 55)
            ):
                raise ValueError("OOS trades must obey the registered NYSE session rules")
        counts = Counter(trade.direction for trade in eligible)
        lower = _daily_block_bootstrap_lower_bound(
            eligible,
            iterations=self.bootstrap_iterations,
            seed=self.seed,
        )
        positive_windows = _positive_chronological_windows(eligible)
        independent_days = len({trade.opened_at.date() for trade in eligible})
        stressed_total, invalid_stress_records = _stressed_total(eligible)
        symbol_concentration = _max_positive_concentration(eligible, "symbol")
        category_concentration = _max_positive_concentration(eligible, "category")

        direction_checks: list[GateCheck] = []
        for direction in enabled_directions:
            directional = [trade for trade in eligible if trade.direction is direction]
            direction_lower = _daily_block_bootstrap_lower_bound(
                directional,
                iterations=self.bootstrap_iterations,
                seed=self.seed,
            )
            direction_stress, direction_invalid_stress = _stressed_total(directional)
            direction_checks.extend(
                (
                    GateCheck(
                        name=f"minimum_{direction.value}_trades",
                        passed=counts[direction] >= 50,
                        observed=counts[direction],
                        required=">= 50",
                    ),
                    GateCheck(
                        name=f"{direction.value}_bootstrap_net_expectancy",
                        passed=direction_lower > 0,
                        observed=direction_lower,
                        required="lower 95% confidence bound > 0",
                    ),
                    GateCheck(
                        name=f"{direction.value}_chronological_stability",
                        passed=_positive_chronological_windows(directional) >= 3,
                        observed=_positive_chronological_windows(directional),
                        required=">= 3 of 4 positive windows",
                    ),
                    GateCheck(
                        name=f"{direction.value}_double_cost_stress",
                        passed=(direction_stress is not None and direction_stress >= 0),
                        observed=(
                            float(direction_stress)
                            if direction_stress is not None
                            else (f"missing or invalid for {direction_invalid_stress} trades")
                        ),
                        required="complete stress P&L and >= 0 total",
                    ),
                    GateCheck(
                        name=f"{direction.value}_symbol_concentration",
                        passed=_max_positive_concentration(directional, "symbol") <= 0.25,
                        observed=_max_positive_concentration(directional, "symbol"),
                        required="<= 0.25",
                    ),
                    GateCheck(
                        name=f"{direction.value}_category_concentration",
                        passed=_max_positive_concentration(directional, "category") <= 0.25,
                        observed=_max_positive_concentration(directional, "category"),
                        required="<= 0.25",
                    ),
                )
            )
        checks = (
            GateCheck(
                name="minimum_oos_trades",
                passed=len(eligible) >= 200,
                observed=len(eligible),
                required=">= 200",
            ),
            GateCheck(
                name="minimum_independent_oos_days",
                passed=independent_days >= 50,
                observed=independent_days,
                required=">= 50",
            ),
            GateCheck(
                name="bootstrap_net_expectancy",
                passed=lower > 0,
                observed=lower,
                required="lower 95% confidence bound > 0",
            ),
            GateCheck(
                name="chronological_stability",
                passed=positive_windows >= 3,
                observed=positive_windows,
                required=">= 3 of 4 positive windows",
            ),
            GateCheck(
                name="double_cost_stress",
                passed=stressed_total is not None and stressed_total >= 0,
                observed=(
                    float(stressed_total)
                    if stressed_total is not None
                    else f"missing or invalid for {invalid_stress_records} trades"
                ),
                required="complete stress P&L and >= 0 total",
            ),
            GateCheck(
                name="symbol_concentration",
                passed=symbol_concentration <= 0.25,
                observed=symbol_concentration,
                required="<= 0.25",
            ),
            GateCheck(
                name="category_concentration",
                passed=category_concentration <= 0.25,
                observed=category_concentration,
                required="<= 0.25",
            ),
            *tuple(direction_checks),
        )
        return ResearchGateResult(
            passed=all(check.passed for check in checks),
            checks=checks,
            trade_count=len(eligible),
            lower_confidence_bound=lower,
            strategy_version=strategy_version,
            enabled_directions=enabled_directions,
        )


def _validate_gate_trade_integrity(trades: Sequence[TradeResult]) -> None:
    """Reject duplicated candidates and records outside the registered horizon."""

    trade_ids_by_variant: dict[str, set[str]] = defaultdict(set)
    event_ids_by_variant: dict[str, set[str]] = defaultdict(set)
    for trade in trades:
        trade_id = trade.trade_id.strip()
        if not trade_id:
            raise ValueError("every gate trade requires a non-empty trade_id")
        if trade_id in trade_ids_by_variant[trade.strategy_variant]:
            raise ValueError(
                "gate trades require unique trade_id values within each strategy_variant"
            )
        trade_ids_by_variant[trade.strategy_variant].add(trade_id)

        raw_event_id = trade.metadata.get("event_id")
        if not isinstance(raw_event_id, str) or not raw_event_id.strip():
            raise ValueError("every gate trade requires a non-empty metadata.event_id")
        event_id = raw_event_id.strip()
        if event_id in event_ids_by_variant[trade.strategy_variant]:
            raise ValueError(
                "gate trades require unique metadata.event_id values within each strategy_variant"
            )
        event_ids_by_variant[trade.strategy_variant].add(event_id)

        required_text = (
            "accession_number",
            "filing_accepted_at",
            "coverage_record_id",
            "scenario",
            "provider",
            "feed",
        )
        for field in required_text:
            value = trade.metadata.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"every gate trade requires non-empty metadata.{field}")
        case_hash = trade.metadata.get("case_input_sha256")
        if not isinstance(case_hash, str) or re.fullmatch(r"[0-9a-f]{64}", case_hash) is None:
            raise ValueError("every gate trade requires a valid metadata.case_input_sha256")
        if trade.metadata.get("availability_lag_minutes") not in {1, 3, 5, 10}:
            raise ValueError("every gate trade requires a registered availability lag")

        if trade.closed_at - trade.opened_at > timedelta(minutes=60):
            raise ValueError("gate trade holding duration must not exceed 60 minutes")


class PairedImprovementResult(FrozenModel):
    passed: bool
    matched_trades: int
    complete_pairing: bool
    mean_incremental_pnl: float
    lower_confidence_bound: float
    independent_days: int = Field(ge=0)
    baseline_version: str
    candidate_version: str

    @model_validator(mode="after")
    def validate_derived_result(self) -> PairedImprovementResult:
        expected = (
            self.complete_pairing
            and self.matched_trades >= 100
            and self.independent_days >= 50
            and self.lower_confidence_bound > 0
        )
        if self.passed != expected:
            raise ValueError("paired passed flag must be derived from paired evidence")
        return self


def paired_variant_improvement(
    trades: list[TradeResult],
    *,
    baseline_version: str,
    candidate_version: str,
    candidate_abstention_event_ids: Sequence[str] = (),
    iterations: int = 10_000,
    seed: int = 8_002,
) -> PairedImprovementResult:
    _validate_frozen_oos_labels(trades)
    baseline_records = [
        trade
        for trade in trades
        if trade.out_of_sample and trade.strategy_variant == baseline_version
    ]
    candidate_records = [
        trade
        for trade in trades
        if trade.out_of_sample and trade.strategy_variant == candidate_version
    ]
    baseline = _trades_by_event_id(baseline_records, baseline_version)
    candidate = _trades_by_event_id(candidate_records, candidate_version)
    abstentions = tuple(event_id.strip() for event_id in candidate_abstention_event_ids)
    if any(not event_id for event_id in abstentions):
        raise ValueError("candidate abstention event ids must not be empty")
    if len(set(abstentions)) != len(abstentions):
        raise ValueError("candidate abstention event ids must be unique")
    abstention_ids = set(abstentions)
    if candidate.keys() & abstention_ids:
        raise ValueError("an event cannot be both an AI trade and an AI abstention")

    candidate_event_ids = set(candidate) | abstention_ids
    complete_pairing = baseline.keys() == candidate_event_ids
    matched_ids = sorted(baseline.keys() & candidate_event_ids)
    incremental: list[TradeResult] = []
    for event_id in matched_ids:
        baseline_trade = baseline[event_id]
        candidate_trade = candidate.get(event_id)
        candidate_pnl = candidate_trade.net_pnl if candidate_trade is not None else Decimal("0")
        candidate_return = candidate_trade.return_bps if candidate_trade is not None else 0.0
        incremental.append(
            TradeResult(
                trade_id=f"paired:{event_id}",
                symbol=baseline_trade.symbol,
                direction=baseline_trade.direction,
                category=baseline_trade.category,
                opened_at=baseline_trade.opened_at,
                closed_at=baseline_trade.closed_at,
                net_pnl=candidate_pnl - baseline_trade.net_pnl,
                return_bps=candidate_return - baseline_trade.return_bps,
                strategy_variant="paired_increment",
                out_of_sample=True,
                metadata={"event_id": event_id},
            )
        )
    lower = _daily_block_bootstrap_lower_bound(
        incremental,
        iterations=iterations,
        seed=seed,
    )
    mean_incremental = (
        float(sum((trade.net_pnl for trade in incremental), Decimal("0"))) / len(incremental)
        if incremental
        else -1.0e308
    )
    independent_days = len({trade.opened_at.date() for trade in incremental})
    return PairedImprovementResult(
        passed=(
            complete_pairing and len(incremental) >= 100 and independent_days >= 50 and lower > 0
        ),
        matched_trades=len(incremental),
        complete_pairing=complete_pairing,
        mean_incremental_pnl=mean_incremental,
        lower_confidence_bound=lower,
        independent_days=independent_days,
        baseline_version=baseline_version,
        candidate_version=candidate_version,
    )


def _validate_frozen_oos_labels(trades: Sequence[TradeResult]) -> None:
    for trade in trades:
        sample_period = trade.metadata.get("sample_period")
        if sample_period not in {"development", "validation", "holdout", "forward"}:
            raise ValueError("every research trade requires a registered sample period")
        raw_accepted_at = trade.metadata.get("filing_accepted_at")
        if not isinstance(raw_accepted_at, str):
            raise ValueError("every research trade requires metadata.filing_accepted_at")
        try:
            accepted_at = datetime.fromisoformat(raw_accepted_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("metadata.filing_accepted_at must be an ISO timestamp") from exc
        if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
            raise ValueError("metadata.filing_accepted_at must be timezone-aware")
        if period_for(accepted_at) != sample_period:
            raise ValueError("sample period must be derived from filing acceptance")
        expected = sample_period in {"validation", "holdout"}
        if trade.out_of_sample != expected:
            raise ValueError(
                "out-of-sample status must be derived from filing-acceptance sample period"
            )


def _trades_by_event_id(
    trades: Sequence[TradeResult],
    strategy_version: str,
) -> dict[str, TradeResult]:
    indexed: dict[str, TradeResult] = {}
    for trade in trades:
        raw_event_id = trade.metadata.get("event_id")
        if not isinstance(raw_event_id, str) or not raw_event_id.strip():
            raise ValueError(
                f"paired comparison requires an event_id for every {strategy_version} record"
            )
        event_id = raw_event_id.strip()
        if event_id in indexed:
            raise ValueError(
                f"paired comparison requires unique event ids within {strategy_version}"
            )
        indexed[event_id] = trade
    return indexed


class ModelBenchmarkResult(FrozenModel):
    model_id: str
    reference_count: int = Field(ge=0)
    prediction_count: int = Field(ge=0)
    schema_valid_rate: float = Field(ge=0, le=1)
    actionable_precision: float = Field(ge=0, le=1)
    macro_f1: float = Field(ge=0, le=1)
    p95_latency_seconds: float = Field(ge=0)
    average_cost_eur: float = Field(ge=0)

    @property
    def passes(self) -> bool:
        return (
            self.reference_count >= 100
            and self.prediction_count == self.reference_count
            and self.schema_valid_rate == 1
            and self.actionable_precision >= 0.80
            and self.macro_f1 >= 0.70
            and self.p95_latency_seconds <= 30
            and self.average_cost_eur <= 0.02
        )


def select_model(results: list[ModelBenchmarkResult]) -> ModelBenchmarkResult | None:
    passing = [result for result in results if result.passes]
    if not passing:
        return None
    return min(
        passing,
        key=lambda result: (
            -result.actionable_precision,
            result.average_cost_eur,
            result.p95_latency_seconds,
            result.model_id,
        ),
    )


class ResearchRunGateError(ValueError):
    """A gate was asked to judge evidence that is not a complete run artifact."""


def evaluate_research_run(
    run: BacktestRunArtifact,
    *,
    enabled_directions: tuple[Direction, ...] = (Direction.LONG, Direction.SHORT),
    evaluator: ResearchGateEvaluator | None = None,
) -> ResearchGateResult:
    """Judge a whole run artifact; a free-standing trade list is never accepted."""

    run.verify()
    gate = evaluator or ResearchGateEvaluator()
    return gate.evaluate(
        list(run.trades),
        strategy_version=run.strategy_version,
        enabled_directions=enabled_directions,
    )


def paired_ai_gate(
    quant_run: BacktestRunArtifact,
    ai_run: BacktestRunArtifact,
    *,
    iterations: int = 10_000,
    seed: int = 8_002,
) -> PairedImprovementResult:
    """Compare an AI run against its quant-only baseline over identical cases."""

    quant_run.verify()
    ai_run.verify()
    if quant_run.variant is not BacktestVariant.QUANT_ONLY:
        raise ResearchRunGateError("the paired baseline must be the quant-only run")
    if ai_run.variant is not BacktestVariant.AI:
        raise ResearchRunGateError("the paired candidate must be the AI run")
    if quant_run.case_artifact_sha256 != ai_run.case_artifact_sha256:
        raise ResearchRunGateError("paired runs must share one research case artifact")
    if quant_run.case_hashes != ai_run.case_hashes:
        raise ResearchRunGateError("paired runs must cover identical case hashes")
    if quant_run.strategy_version == ai_run.strategy_version:
        raise ResearchRunGateError("paired runs must use distinct strategy versions")
    if quant_run.cost_model_version != ai_run.cost_model_version:
        raise ResearchRunGateError("paired runs must share one cost model")

    ai_trade_events = {
        str(trade.metadata.get("event_id")) for trade in ai_run.trades if trade.out_of_sample
    }
    abstentions = tuple(
        sorted(
            outcome.event_id
            for outcome in ai_run.outcomes
            if outcome.out_of_sample
            and outcome.trade is None
            and outcome.event_id not in ai_trade_events
        )
    )
    return paired_variant_improvement(
        list(quant_run.trades) + list(ai_run.trades),
        baseline_version=quant_run.strategy_version,
        candidate_version=ai_run.strategy_version,
        candidate_abstention_event_ids=abstentions,
        iterations=iterations,
        seed=seed,
    )
