"""Deterministic historical trade simulation using production strategy and risk rules.

The case objects are insight-free by construction.  A variant supplies its own
insights at run time, so the quant-only, keyword and AI runs are evaluated
against byte-identical cases.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from itertools import pairwise
from typing import Literal

from pydantic import Field, model_validator

from .artifacts import HashedArtifact, Sha256
from .calendar import NEW_YORK, NyseSessionCalendar
from .costs import CostModel
from .datasets import period_for
from .domain import (
    Bar,
    Direction,
    EventSnapshot,
    FrozenModel,
    NewsInsight,
    PortfolioState,
    Position,
    Quote,
    RiskDecision,
    Signal,
    TradeResult,
)
from .risk import RiskEngine
from .strategy import (
    ContinuationStrategy,
    QuantOnlyContinuationStrategy,
    filing_item_category,
)

_MAX_HISTORICAL_QUOTE_AGE = timedelta(seconds=5)
_STARTING_EQUITY = Decimal("100000")
# A daily-loss halt clears with the trading day; a drawdown halt requires an
# audited manual reset that a historical run must never fabricate.
_SESSION_SCOPED_HALTS = frozenset({"DAILY_LOSS_LIMIT"})


class BacktestVariant(StrEnum):
    QUANT_ONLY = "quant-only"
    KEYWORD = "keyword"
    AI = "ai"


class BacktestExitPoint(FrozenModel):
    """One point-in-time executable quote paired with its completed bar."""

    timestamp: datetime
    bar: Bar
    quote: Quote

    @model_validator(mode="after")
    def validate_alignment(self) -> BacktestExitPoint:
        if self.bar.symbol != self.quote.symbol:
            raise ValueError("bar and quote symbols must match")
        if self.bar.timestamp != self.timestamp:
            raise ValueError("completed exit bar must end at the exit point timestamp")
        if self.quote.timestamp > self.timestamp:
            raise ValueError("backtest exit inputs cannot be from the future")
        if self.timestamp - self.quote.timestamp > _MAX_HISTORICAL_QUOTE_AGE:
            raise ValueError("backtest exit quote is stale")
        if self.timestamp.second or self.timestamp.microsecond:
            raise ValueError("backtest exit points must be aligned to completed minutes")
        return self


class BacktestEntryPoint(FrozenModel):
    """A quote observable at the single permitted five-second reprice attempt."""

    attempted_at: datetime
    quote: Quote

    @model_validator(mode="after")
    def validate_quote(self) -> BacktestEntryPoint:
        if self.quote.timestamp > self.attempted_at:
            raise ValueError("backtest entry quote cannot be from the future")
        if self.attempted_at - self.quote.timestamp > _MAX_HISTORICAL_QUOTE_AGE:
            raise ValueError("backtest entry quote is stale")
        return self


class BacktestLineage(FrozenModel):
    coverage_record_id: str = Field(min_length=1)
    scenario: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    feed: str = Field(min_length=1)
    sample_period: Literal["development", "validation", "holdout", "forward"]
    case_input_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BacktestCase(FrozenModel):
    decision_time: datetime
    snapshot: EventSnapshot
    portfolio: PortfolioState
    entry_reprice: BacktestEntryPoint | None = None
    exit_points: tuple[BacktestExitPoint, ...]
    availability_lag_minutes: Literal[1, 3, 5, 10] = 5
    out_of_sample: bool = False
    lineage: BacktestLineage | None = None

    @model_validator(mode="after")
    def validate_exit_path(self) -> BacktestCase:
        filing = self.snapshot.filing
        sample_period = period_for(filing.accepted_at)
        expected_oos = sample_period in {"validation", "holdout"}
        if self.out_of_sample != expected_oos:
            raise ValueError("historical OOS status must be derived from filing acceptance")
        if self.lineage is not None and self.lineage.sample_period != sample_period:
            raise ValueError("backtest lineage sample period must match filing acceptance")
        if self.decision_time.second or self.decision_time.microsecond:
            raise ValueError("historical decision time must be aligned to a completed minute")
        # Operational retrieval timestamps record when the backfill process ran and
        # may be years after the event.  Historical availability is therefore an
        # explicit counterfactual: SEC acceptance plus the selected lag, followed
        # by the same five-minute/next-open scheduling rule as the live session.
        available_at = filing.accepted_at + timedelta(minutes=self.availability_lag_minutes)
        scheduled = NyseSessionCalendar().next_evaluation_time(available_at)
        expected_decision = scheduled if scheduled is not None else available_at
        if self.decision_time != expected_decision:
            raise ValueError(
                "decision time must equal the live-equivalent historical evaluation time"
            )
        if self.snapshot.market.as_of > self.decision_time:
            raise ValueError("market snapshot cannot be from after the decision time")
        if self.snapshot.market.quote.timestamp > self.decision_time:
            raise ValueError("decision quote cannot be from after the decision time")
        if (
            self.decision_time - self.snapshot.market.quote.timestamp
            > _MAX_HISTORICAL_QUOTE_AGE
        ):
            raise ValueError("historical decision quote is stale")
        if self.portfolio.as_of > self.decision_time:
            raise ValueError("portfolio state cannot be from after the decision time")

        symbol = self.snapshot.market.symbol
        if self.entry_reprice is not None:
            if self.entry_reprice.quote.symbol != symbol:
                raise ValueError("entry reprice quote must match the event symbol")
            if self.entry_reprice.attempted_at != self.decision_time + timedelta(seconds=5):
                raise ValueError("entry reprice must occur exactly five seconds after decision")
        if any(point.bar.symbol != symbol for point in self.exit_points):
            raise ValueError("all exit points must match the event symbol")
        if any(
            point.timestamp < self.decision_time
            or point.bar.timestamp < self.decision_time
            or point.quote.timestamp < self.decision_time
            for point in self.exit_points
        ):
            raise ValueError("exit inputs cannot predate the decision")
        timestamps = tuple(point.timestamp for point in self.exit_points)
        if any(current <= previous for previous, current in pairwise(timestamps)):
            raise ValueError("exit points must be strictly chronological and unique")
        return self

    @property
    def case_input_sha256(self) -> str | None:
        return self.lineage.case_input_sha256 if self.lineage is not None else None

    @property
    def ordering_key(self) -> tuple[datetime, str, str]:
        """Total order that stays stable when several events share an instant."""

        return (
            self.decision_time,
            self.case_input_sha256 or "",
            self.snapshot.filing.event_id,
        )


class BacktestOutcome(FrozenModel):
    event_id: str
    stage: str
    case_input_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    out_of_sample: bool = False
    reasons: tuple[str, ...] = ()
    signal: Signal | None = None
    risk_decision: RiskDecision | None = None
    trade: TradeResult | None = None


class BacktestRunArtifact(HashedArtifact):
    """Hashed evidence binding one variant run to its cases and their results."""

    artifact_version: Literal["1"] = "1"
    variant: BacktestVariant
    strategy_version: str = Field(min_length=1)
    cost_model_version: str = Field(min_length=1)
    case_artifact_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    insight_artifact_sha256: Sha256 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    case_hashes: tuple[str, ...] = ()
    outcomes: tuple[BacktestOutcome, ...] = ()
    artifact_sha256: Sha256 = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_run(self) -> BacktestRunArtifact:
        if len(set(self.case_hashes)) != len(self.case_hashes):
            raise ValueError("a backtest run requires unique case hashes")
        outcome_hashes = tuple(outcome.case_input_sha256 for outcome in self.outcomes)
        if any(value is None for value in outcome_hashes):
            raise ValueError("every backtest outcome must reference its case hash")
        if len(outcome_hashes) != len(self.case_hashes):
            raise ValueError("a backtest run requires exactly one outcome per case")
        if set(outcome_hashes) != set(self.case_hashes):
            raise ValueError("backtest outcomes and cases must describe the same case set")
        if (self.variant is BacktestVariant.QUANT_ONLY) != (
            self.insight_artifact_sha256 is None
        ):
            raise ValueError("only the quant-only variant runs without an insight artifact")
        return self

    @property
    def trades(self) -> tuple[TradeResult, ...]:
        return tuple(
            outcome.trade for outcome in self.outcomes if outcome.trade is not None
        )


@dataclass(slots=True)
class _OpenPosition:
    """One filled historical position with its own future mark prices."""

    trade: TradeResult
    quantity: int
    entry_price: Decimal
    marks: dict[datetime, Decimal] = field(default_factory=dict)

    def mark_at(self, now: datetime) -> Decimal:
        observable = [timestamp for timestamp in self.marks if timestamp <= now]
        if not observable:
            return self.entry_price
        return self.marks[max(observable)]

    def unrealized(self, now: datetime) -> Decimal:
        sign = Decimal("1") if self.trade.direction is Direction.LONG else Decimal("-1")
        return sign * (self.mark_at(now) - self.entry_price) * self.quantity


class _SessionRiskHaltGuard:
    """Live-equivalent halt latch for historical runs.

    A daily-loss halt blocks the remainder of that NYSE session and clears with
    the next one.  Every other halt latches for the whole run, because releasing
    it live requires an audited manual reset.
    """

    def __init__(self) -> None:
        self.reason: str | None = None
        self._session: date | None = None

    def observe(self, now: datetime) -> None:
        session = now.astimezone(NEW_YORK).date()
        if self._session is not None and session > self._session:
            if self.reason in _SESSION_SCOPED_HALTS:
                self.reason = None
        self._session = session

    def is_halted(self) -> bool:
        return self.reason is not None

    def trip(self, *, reason: str, at: datetime) -> None:
        self._session = at.astimezone(NEW_YORK).date()
        self.reason = self.reason or reason


class HistoricalBacktester:
    """Generate net TradeResult records without inventing unavailable exits.

    Gross returns use quote midpoints.  The cost model then applies the observed
    half-spreads, commissions and explicit slippage exactly once.  Stops use the
    worse of the registered stop and a gapping bar open.  ``run`` additionally
    carries open positions forward, so exposure, unrealised P&L, daily loss and
    drawdown are current before every new decision.
    """

    def __init__(
        self,
        *,
        strategy: ContinuationStrategy,
        risk_engine: RiskEngine | None = None,
        cost_model: CostModel | None = None,
        calendar: NyseSessionCalendar | None = None,
    ) -> None:
        self.strategy = strategy
        self._session_guard: _SessionRiskHaltGuard | None = None
        if risk_engine is None:
            self._session_guard = _SessionRiskHaltGuard()
            risk_engine = RiskEngine(halt_guard=self._session_guard)
        self.risk_engine = risk_engine
        self.cost_model = cost_model or CostModel()
        self.calendar = calendar or NyseSessionCalendar()
        self._open: list[_OpenPosition] = []
        self._closed: list[TradeResult] = []
        self._peak_equity = _STARTING_EQUITY

    # ------------------------------------------------------------------ run --

    def run(
        self,
        cases: Sequence[BacktestCase],
        insights: Mapping[str, NewsInsight] | None = None,
    ) -> tuple[BacktestOutcome, ...]:
        """Evaluate every case in one portfolio, in a stable chronological order."""

        ordered = sorted(cases, key=lambda case: case.ordering_key)
        self._reset()
        outcomes: list[BacktestOutcome] = []
        for case in ordered:
            if case.portfolio.positions or case.portfolio.pending_orders:
                raise ValueError(
                    "historical portfolio is derived by the backtester and must start empty"
                )
            self._advance_to(case.decision_time)
            if self._session_guard is not None:
                self._session_guard.observe(case.decision_time)
            portfolio = self._portfolio_at(case.decision_time)
            outcome = self.run_case(
                case.model_copy(update={"portfolio": portfolio}),
                self._insight_for(case, insights),
            )
            outcomes.append(outcome)
            if outcome.trade is not None:
                self._register(outcome.trade, case)
        return tuple(outcomes)

    def run_case(
        self,
        case: BacktestCase,
        insight: NewsInsight | None = None,
    ) -> BacktestOutcome:
        case_hash = case.case_input_sha256
        out_of_sample = case.out_of_sample
        signal = self.strategy.evaluate(case.snapshot, insight, case.decision_time)
        if signal is None:
            return BacktestOutcome(
                event_id=case.snapshot.filing.event_id,
                stage="filtered",
                case_input_sha256=case_hash,
                out_of_sample=out_of_sample,
                reasons=self.strategy.rejection_reasons(
                    case.snapshot, insight, case.decision_time
                ),
            )
        risk = self.risk_engine.assess(
            signal,
            case.portfolio,
            case.snapshot.market,
            case.decision_time,
        )
        if not risk.approved:
            return BacktestOutcome(
                event_id=signal.event_id,
                stage="risk_rejected",
                case_input_sha256=case_hash,
                out_of_sample=out_of_sample,
                reasons=risk.reason_codes,
                signal=signal,
                risk_decision=risk,
            )

        entry = self._select_entry(signal, case, risk.quantity)
        if entry is None:
            return BacktestOutcome(
                event_id=signal.event_id,
                stage="entry_unfilled",
                case_input_sha256=case_hash,
                out_of_sample=out_of_sample,
                reasons=("MARKETABLE_LIMIT_NOT_FILLED",),
                signal=signal,
                risk_decision=risk,
            )
        filled_quantity, entry_mid, entry_spread_bps, opened_at, entry_attempts = entry

        selected = self._select_exit(
            signal,
            tuple(point for point in case.exit_points if point.timestamp >= opened_at),
        )
        if selected is None:
            return BacktestOutcome(
                event_id=signal.event_id,
                stage="coverage_gap",
                case_input_sha256=case_hash,
                out_of_sample=out_of_sample,
                reasons=("MISSING_EXIT_COVERAGE",),
                signal=signal,
                risk_decision=risk,
            )
        point, exit_reason, exit_mid = selected
        sign = Decimal("1") if signal.direction is Direction.LONG else Decimal("-1")
        gross_pnl = sign * (exit_mid - entry_mid) * filled_quantity
        cost = self.cost_model.round_trip_cost(
            quantity=filled_quantity,
            entry_price=entry_mid,
            exit_price=exit_mid,
            entry_spread_bps=entry_spread_bps,
            exit_spread_bps=point.quote.spread_bps,
        )
        stressed_cost = self.cost_model.round_trip_cost(
            quantity=filled_quantity,
            entry_price=entry_mid,
            exit_price=exit_mid,
            entry_spread_bps=entry_spread_bps,
            exit_spread_bps=point.quote.spread_bps,
            multiplier=Decimal("2"),
        )
        entry_notional = entry_mid * filled_quantity
        trade_key = f"{signal.event_id}:{signal.symbol}:{case.decision_time.isoformat()}"
        category = (
            filing_item_category(case.snapshot.filing.items)
            if insight is None or isinstance(self.strategy, QuantOnlyContinuationStrategy)
            else insight.category
        )
        trade = TradeResult(
            trade_id=sha256(trade_key.encode()).hexdigest()[:32],
            symbol=signal.symbol,
            direction=signal.direction,
            category=category,
            opened_at=opened_at,
            closed_at=point.timestamp,
            net_pnl=gross_pnl - cost,
            return_bps=float((gross_pnl - cost) / entry_notional * Decimal("10000")),
            strategy_variant=signal.strategy_version,
            out_of_sample=case.out_of_sample,
            metadata={
                "gross_pnl": str(gross_pnl),
                "transaction_cost": str(cost),
                "stress_net_pnl": str(gross_pnl - stressed_cost),
                "exit_reason": exit_reason,
                "entry_mid": str(entry_mid),
                "exit_mid": str(exit_mid),
                "requested_quantity": risk.quantity,
                "filled_quantity": filled_quantity,
                "entry_attempts": entry_attempts,
                "event_id": signal.event_id,
                "accession_number": signal.accession_number,
                "filing_accepted_at": case.snapshot.filing.accepted_at.isoformat(),
                "availability_lag_minutes": case.availability_lag_minutes,
                "coverage_record_id": (
                    case.lineage.coverage_record_id if case.lineage is not None else None
                ),
                "scenario": case.lineage.scenario if case.lineage is not None else None,
                "provider": case.lineage.provider if case.lineage is not None else None,
                "feed": case.lineage.feed if case.lineage is not None else None,
                "sample_period": (
                    case.lineage.sample_period if case.lineage is not None else period_for(
                        case.snapshot.filing.accepted_at
                    )
                ),
                "case_input_sha256": case_hash,
            },
        )
        return BacktestOutcome(
            event_id=signal.event_id,
            stage="closed_trade",
            case_input_sha256=case_hash,
                out_of_sample=out_of_sample,
            signal=signal,
            risk_decision=risk,
            trade=trade,
        )

    # -------------------------------------------------------------- portfolio --

    def _reset(self) -> None:
        self._open = []
        self._closed = []
        self._peak_equity = _STARTING_EQUITY

    @staticmethod
    def _insight_for(
        case: BacktestCase,
        insights: Mapping[str, NewsInsight] | None,
    ) -> NewsInsight | None:
        """Look a case up by its content address, or by event id when unhashed."""

        if not insights:
            return None
        case_hash = case.case_input_sha256
        if case_hash is not None:
            return insights.get(case_hash)
        return insights.get(case.snapshot.filing.event_id)

    def _register(self, trade: TradeResult, case: BacktestCase) -> None:
        raw_quantity = trade.metadata.get("filled_quantity")
        raw_entry = trade.metadata.get("entry_mid")
        if not isinstance(raw_quantity, int) or raw_quantity <= 0 or raw_entry is None:
            raise ValueError("closed backtest trade lacks execution metadata")
        marks = {
            point.timestamp: point.bar.close
            for point in case.exit_points
            if trade.opened_at <= point.timestamp <= trade.closed_at
        }
        self._open.append(
            _OpenPosition(
                trade=trade,
                quantity=raw_quantity,
                entry_price=Decimal(str(raw_entry)),
                marks=marks,
            )
        )

    def _advance_to(self, now: datetime) -> None:
        """Close every position that matured before ``now`` and track the peak."""

        matured = sorted(
            (position for position in self._open if position.trade.closed_at <= now),
            key=lambda position: (position.trade.closed_at, position.trade.trade_id),
        )
        for position in matured:
            self._open.remove(position)
            self._closed.append(position.trade)
            self._peak_equity = max(
                self._peak_equity, self._equity_at(position.trade.closed_at)
            )
        self._peak_equity = max(self._peak_equity, self._equity_at(now))

    def _realized(self) -> Decimal:
        return sum((trade.net_pnl for trade in self._closed), Decimal("0"))

    def _unrealized_at(self, now: datetime) -> Decimal:
        return sum(
            (position.unrealized(now) for position in self._open),
            Decimal("0"),
        )

    def _equity_at(self, now: datetime) -> Decimal:
        return _STARTING_EQUITY + self._realized() + self._unrealized_at(now)

    def _portfolio_at(self, now: datetime) -> PortfolioState:
        local_date = now.astimezone(NEW_YORK).date()
        realized_today = sum(
            (
                trade.net_pnl
                for trade in self._closed
                if trade.closed_at.astimezone(NEW_YORK).date() == local_date
            ),
            Decimal("0"),
        )
        unrealized = self._unrealized_at(now)
        equity = _STARTING_EQUITY + self._realized() + unrealized
        if equity <= 0:
            raise ValueError("historical strategy equity became non-positive")
        peak = max(self._peak_equity, equity)
        positions = tuple(
            Position(
                symbol=position.trade.symbol,
                direction=position.trade.direction,
                quantity=position.quantity,
                market_price=position.mark_at(now),
                average_price=position.entry_price,
            )
            for position in sorted(self._open, key=lambda item: item.trade.symbol)
        )
        return PortfolioState(
            as_of=now,
            nav=equity,
            peak_nav=peak,
            cash=_STARTING_EQUITY + self._realized(),
            realized_pnl_today=realized_today,
            unrealized_pnl=unrealized,
            positions=positions,
            pending_orders=(),
            strategy_equity=equity,
            strategy_peak_equity=peak,
            strategy_realized_pnl_today=realized_today,
            strategy_unrealized_pnl=unrealized,
        )

    # ------------------------------------------------------------- execution --

    def _select_exit(
        self,
        signal: Signal,
        points: tuple[BacktestExitPoint, ...],
    ) -> tuple[BacktestExitPoint, str, Decimal] | None:
        points_by_time = {point.timestamp: point for point in points}
        expected = signal.decided_at.replace(second=0, microsecond=0)
        expected += timedelta(minutes=1)
        while expected <= signal.expires_at:
            point = points_by_time.get(expected)
            if point is None:
                return None
            if self.calendar.force_flat_due(point.timestamp):
                return point, "FORCE_FLAT_1555", point.quote.midpoint
            if signal.direction is Direction.LONG and point.bar.low <= signal.stop_price:
                return point, "STOP_EXIT", min(signal.stop_price, point.bar.open)
            if signal.direction is Direction.SHORT and point.bar.high >= signal.stop_price:
                return point, "STOP_EXIT", max(signal.stop_price, point.bar.open)
            if point.timestamp >= signal.expires_at:
                return point, "TIME_EXIT", point.quote.midpoint
            expected += timedelta(minutes=1)
        return None

    @staticmethod
    def _select_entry(
        signal: Signal,
        case: BacktestCase,
        requested_quantity: int,
    ) -> tuple[int, Decimal, Decimal, datetime, int] | None:
        attempts_with_quotes = ((case.decision_time, case.snapshot.market.quote),)
        if case.entry_reprice is not None:
            attempts_with_quotes += (
                (case.entry_reprice.attempted_at, case.entry_reprice.quote),
            )

        filled = 0
        weighted_mid = Decimal("0")
        weighted_spread = Decimal("0")
        last_fill_at = case.decision_time
        attempts = 0
        for attempted_at, quote in attempts_with_quotes:
            attempts += 1
            displayed = (
                quote.ask_size if signal.direction is Direction.LONG else quote.bid_size
            )
            quantity = min(requested_quantity - filled, displayed)
            if quantity <= 0:
                continue
            filled += quantity
            weighted_mid += quote.midpoint * quantity
            weighted_spread += quote.spread_bps * quantity
            last_fill_at = max(last_fill_at, attempted_at)
            if filled == requested_quantity:
                break
        if filled <= 0:
            return None
        return (
            filled,
            weighted_mid / filled,
            weighted_spread / filled,
            last_fill_at,
            attempts,
        )


__all__ = [
    "BacktestCase",
    "BacktestEntryPoint",
    "BacktestExitPoint",
    "BacktestLineage",
    "BacktestOutcome",
    "BacktestRunArtifact",
    "BacktestVariant",
    "HistoricalBacktester",
]
