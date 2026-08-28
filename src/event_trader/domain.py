"""Validated domain contracts shared by research, live data, and execution.

The models are deliberately immutable.  A replay and a live session therefore
consume the same timestamped facts instead of silently mutating state in place.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

Money = Annotated[Decimal, Field(gt=Decimal("0"), max_digits=20, decimal_places=8)]
NonNegativeMoney = Annotated[Decimal, Field(ge=Decimal("0"), max_digits=20, decimal_places=8)]
UtcTimestamp = AwareDatetime


class FrozenModel(BaseModel):
    """Base class for strict, immutable contracts."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )

    @field_validator("*", mode="after")
    @classmethod
    def normalize_direct_timestamps_to_utc(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            if value.utcoffset() is None:
                raise ValueError("timestamps must be timezone-aware")
            if value.utcoffset() != timedelta(0):
                return value.astimezone(UTC)
        return value


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"


class Materiality(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class InsightStatus(StrEnum):
    ACTIONABLE = "actionable"
    ABSTAIN = "abstain"


class DataSource(StrEnum):
    SEC = "sec"
    ALPACA_SIP = "alpaca_sip"
    IBKR = "ibkr"
    REPLAY = "replay"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    SELL_SHORT = "sell_short"
    BUY_TO_COVER = "buy_to_cover"


class ExecutionStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class DocumentRef(FrozenModel):
    url: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    local_path: str | None = None


class FilingEvent(FrozenModel):
    event_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    cik: str = Field(pattern=r"^\d{1,10}$")
    form: Literal["8-K", "8-K/A"]
    items: tuple[str, ...] = ()
    symbols: tuple[str, ...] = ()
    accepted_at: UtcTimestamp
    first_seen_at: UtcTimestamp
    retrieved_at: UtcTimestamp
    documents: tuple[DocumentRef, ...] = ()
    source: DataSource = DataSource.SEC
    complete: bool = True

    @model_validator(mode="after")
    def validate_timestamps(self) -> FilingEvent:
        if self.first_seen_at < self.accepted_at:
            raise ValueError("first_seen_at must be at or after accepted_at")
        if self.retrieved_at < self.first_seen_at:
            raise ValueError("retrieved_at must be at or after first_seen_at")
        return self


class Bar(FrozenModel):
    symbol: str = Field(min_length=1)
    timestamp: UtcTimestamp
    open: Money
    high: Money
    low: Money
    close: Money
    volume: int = Field(ge=0)
    vwap: Money | None = None
    source: DataSource
    feed: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_ohlc(self) -> Bar:
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high must be the greatest OHLC value")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("low must be the smallest OHLC value")
        return self


class Quote(FrozenModel):
    symbol: str = Field(min_length=1)
    timestamp: UtcTimestamp
    bid: Money
    ask: Money
    bid_size: int = Field(ge=0)
    ask_size: int = Field(ge=0)
    source: DataSource
    feed: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_market(self) -> Quote:
        if self.ask < self.bid:
            raise ValueError("ask must be greater than or equal to bid")
        return self

    @property
    def midpoint(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def spread_bps(self) -> Decimal:
        if self.midpoint == 0:
            return Decimal("0")
        return (self.ask - self.bid) / self.midpoint * Decimal("10000")


class MarketSnapshot(FrozenModel):
    symbol: str = Field(min_length=1)
    as_of: UtcTimestamp
    quote: Quote
    last: Money
    session_vwap: Money
    median_dollar_volume_20d: NonNegativeMoney
    beta_adjusted_return_z: float
    relative_volume: float = Field(ge=0)
    atr_5m: Money
    data_fresh: bool = True
    market_data_live: bool = False
    halted: bool = False
    shortable: bool = False
    shortable_shares: int = Field(default=0, ge=0)
    security_type: Literal["common_stock", "other", "unknown"] = "unknown"
    primary_exchange: str | None = None
    us_listed: bool = False

    @model_validator(mode="after")
    def validate_symbol(self) -> MarketSnapshot:
        if self.quote.symbol != self.symbol:
            raise ValueError("quote symbol must match snapshot symbol")
        if self.quote.timestamp > self.as_of:
            raise ValueError("quote timestamp cannot be after snapshot as_of")
        return self


class EvidenceSpan(FrozenModel):
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    excerpt: str = Field(min_length=1, max_length=500)


class NewsInsight(FrozenModel):
    event_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    status: InsightStatus
    category: str = Field(min_length=1)
    direction: Direction
    materiality: Materiality
    confidence: float = Field(ge=0, le=1)
    horizon_minutes: int = Field(default=60, ge=1, le=1440)
    evidence: tuple[EvidenceSpan, ...] = ()
    model_provider: str = Field(min_length=1)
    model_name: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    schema_version: str = Field(default="1", min_length=1)
    latency_ms: int = Field(default=0, ge=0)
    abstain_reason: str | None = None

    @property
    def model_id(self) -> str:
        """Canonical runtime identity used by the research promotion artifact."""

        return f"{self.model_provider}/{self.model_name}"

    @model_validator(mode="after")
    def validate_actionability(self) -> NewsInsight:
        if self.status is InsightStatus.ACTIONABLE and self.direction is Direction.NEUTRAL:
            raise ValueError("an actionable insight cannot be neutral")
        if self.status is InsightStatus.ACTIONABLE and not self.evidence:
            raise ValueError("an actionable insight requires grounded evidence")
        if self.status is InsightStatus.ABSTAIN and not self.abstain_reason:
            raise ValueError("an abstention requires a reason")
        if self.status is InsightStatus.ABSTAIN and self.direction is not Direction.NEUTRAL:
            raise ValueError("an abstention must be neutral")
        return self

    @classmethod
    def abstain(
        cls,
        *,
        event_id: str,
        accession_number: str,
        reason: str,
        model_provider: str = "none",
        model_name: str = "none",
        prompt_version: str = "1",
        latency_ms: int = 0,
    ) -> NewsInsight:
        return cls(
            event_id=event_id,
            accession_number=accession_number,
            status=InsightStatus.ABSTAIN,
            category="unknown",
            direction=Direction.NEUTRAL,
            materiality=Materiality.LOW,
            confidence=0,
            model_provider=model_provider,
            model_name=model_name,
            prompt_version=prompt_version,
            latency_ms=latency_ms,
            abstain_reason=reason,
        )


class EventSnapshot(FrozenModel):
    filing: FilingEvent
    market: MarketSnapshot
    document_text: str = Field(min_length=1, max_length=500_000)

    @model_validator(mode="after")
    def validate_event_symbol(self) -> EventSnapshot:
        if self.filing.symbols and self.market.symbol not in self.filing.symbols:
            raise ValueError("market symbol must belong to filing")
        return self


class Signal(FrozenModel):
    signal_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    symbol: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    direction: Direction
    decided_at: UtcTimestamp
    entry_limit: Money
    stop_price: Money
    expires_at: UtcTimestamp
    holding_minutes: int = Field(default=60, ge=1, le=390)
    quant_features: dict[str, float] = Field(default_factory=dict)
    insight_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_prices(self) -> Signal:
        if self.direction is Direction.NEUTRAL:
            raise ValueError("a signal cannot be neutral")
        if self.direction is Direction.LONG and self.stop_price >= self.entry_limit:
            raise ValueError("long stop must be below entry")
        if self.direction is Direction.SHORT and self.stop_price <= self.entry_limit:
            raise ValueError("short stop must be above entry")
        if self.expires_at <= self.decided_at:
            raise ValueError("signal must expire after it is decided")
        return self


class Position(FrozenModel):
    symbol: str
    direction: Direction
    quantity: int = Field(gt=0)
    market_price: Money
    average_price: Money

    @model_validator(mode="after")
    def validate_direction(self) -> Position:
        if self.direction is Direction.NEUTRAL:
            raise ValueError("a portfolio position cannot be neutral")
        return self

    @property
    def notional(self) -> Decimal:
        return self.market_price * self.quantity


class PendingOrderExposure(FrozenModel):
    order_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    direction: Direction
    notional: Money

    @model_validator(mode="after")
    def validate_direction(self) -> PendingOrderExposure:
        if self.direction is Direction.NEUTRAL:
            raise ValueError("pending order exposure cannot be neutral")
        return self


class PortfolioState(FrozenModel):
    as_of: UtcTimestamp
    nav: Money
    peak_nav: Money
    cash: Decimal
    realized_pnl_today: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    positions: tuple[Position, ...] = ()
    pending_orders: tuple[PendingOrderExposure, ...] = ()
    strategy_equity: Money | None = None
    strategy_peak_equity: Money | None = None
    strategy_realized_pnl_today: Decimal | None = None
    strategy_unrealized_pnl: Decimal | None = None
    broker_connected: bool = True
    reconciled: bool = True

    @model_validator(mode="after")
    def validate_portfolio(self) -> PortfolioState:
        if self.peak_nav < self.nav:
            raise ValueError("peak NAV cannot be below current NAV")
        symbols = [position.symbol.upper() for position in self.positions]
        if len(symbols) != len(set(symbols)):
            raise ValueError("portfolio positions must have unique symbols")
        pending_ids = [order.order_id for order in self.pending_orders]
        if len(pending_ids) != len(set(pending_ids)):
            raise ValueError("pending order ids must be unique")
        strategy_values = (
            self.strategy_equity,
            self.strategy_peak_equity,
            self.strategy_realized_pnl_today,
            self.strategy_unrealized_pnl,
        )
        if any(value is None for value in strategy_values) and any(
            value is not None for value in strategy_values
        ):
            raise ValueError("strategy risk state must be supplied atomically")
        if (
            self.strategy_equity is not None
            and self.strategy_peak_equity is not None
            and self.strategy_peak_equity < self.strategy_equity
        ):
            raise ValueError("strategy peak equity cannot be below strategy equity")
        return self


class RiskDecision(FrozenModel):
    signal_id: str
    approved: bool
    quantity: int = Field(default=0, ge=0)
    notional: NonNegativeMoney = Decimal("0")
    reason_codes: tuple[str, ...]
    decided_at: UtcTimestamp
    limits: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_decision(self) -> RiskDecision:
        if self.approved and (self.quantity <= 0 or self.notional <= 0):
            raise ValueError("approved risk decisions require a positive position")
        if not self.approved and (self.quantity != 0 or self.notional != 0):
            raise ValueError("rejected risk decisions cannot retain a position")
        return self


class OrderIntent(FrozenModel):
    order_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    environment: Literal["paper"] = "paper"
    submission_mode: Literal["paper", "shadow"] = "shadow"
    research_promotion_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    symbol: str = Field(min_length=1)
    side: OrderSide
    quantity: int = Field(gt=0)
    limit_price: Money
    created_at: UtcTimestamp
    time_in_force: Literal["DAY"] = "DAY"
    replaces_order_id: str | None = None
    reprice_generation: int = Field(default=0, ge=0, le=1)

    @model_validator(mode="after")
    def validate_replacement_lineage(self) -> OrderIntent:
        """Make the single permitted reprice an explicit fact, not a key suffix."""

        if (self.reprice_generation == 1) != (self.replaces_order_id is not None):
            raise ValueError("a repriced order must name exactly the order it replaces")
        if self.replaces_order_id is not None:
            if not self.replaces_order_id.strip():
                raise ValueError("replaced order id cannot be empty")
            if self.replaces_order_id == self.order_id:
                raise ValueError("an order cannot replace itself")
        return self

    @model_validator(mode="after")
    def validate_submission_authorization(self) -> OrderIntent:
        is_entry = self.side in {OrderSide.BUY, OrderSide.SELL_SHORT}
        if self.submission_mode == "shadow" and self.research_promotion_sha256 is not None:
            raise ValueError("shadow intents cannot carry paper-promotion authorization")
        if (
            self.submission_mode == "paper"
            and is_entry
            and self.research_promotion_sha256 is None
        ):
            raise ValueError("paper entry intents require research-promotion authorization")
        return self


class ExecutionReport(FrozenModel):
    order_id: str
    idempotency_key: str
    status: ExecutionStatus
    filled_quantity: int = Field(default=0, ge=0)
    average_fill_price: NonNegativeMoney = Decimal("0")
    fees: NonNegativeMoney = Decimal("0")
    slippage_bps: float = 0
    broker_order_id: str | None = None
    message: str | None = None
    occurred_at: UtcTimestamp
    fill_count: int = Field(default=0, ge=0)
    pending_commission: bool = False
    update_sequence: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_fill_accounting(self) -> ExecutionReport:
        """Aggregates may lag individual fills, but they can never contradict them.

        ``orderStatus`` reports a cumulative quantity without naming the fills
        that produced it, so ``fill_count`` may legitimately be zero while
        ``filled_quantity`` is positive.  The reverse is impossible: a counted
        fill moves at least one share and costs at most its own commission.
        """

        if self.fill_count > self.filled_quantity:
            raise ValueError("fill count cannot exceed the filled quantity")
        if self.filled_quantity == 0:
            if self.fees > 0:
                raise ValueError("an unfilled order cannot carry fees")
            if self.pending_commission:
                raise ValueError("an unfilled order cannot have a pending commission")
        return self


class ExecutionFill(FrozenModel):
    """One immutable broker fill, identified by the broker's own execution id.

    A fill is the only place where a share quantity, its price and its
    commission are known together.  ``ExecutionReport`` carries the aggregate;
    this model carries the evidence the aggregate was derived from.
    """

    order_id: str = Field(min_length=1)
    execution_id: str = Field(min_length=1)
    broker_order_id: str | None = None
    symbol: str = Field(min_length=1)
    side: OrderSide
    quantity: int = Field(gt=0)
    price: Money
    cumulative_quantity: int = Field(gt=0)
    commission: NonNegativeMoney = Decimal("0")
    commission_final: bool = False
    occurred_at: UtcTimestamp

    @model_validator(mode="after")
    def validate_fill(self) -> ExecutionFill:
        if self.cumulative_quantity < self.quantity:
            raise ValueError("cumulative quantity cannot be below this fill's quantity")
        if not self.commission_final and self.commission != 0:
            raise ValueError("a commission counts only once the broker reports it as final")
        if self.broker_order_id is not None and not self.broker_order_id.strip():
            raise ValueError("broker order id cannot be empty")
        return self


class TradeResult(FrozenModel):
    trade_id: str
    symbol: str
    direction: Direction
    category: str
    opened_at: UtcTimestamp
    closed_at: UtcTimestamp
    net_pnl: Decimal
    return_bps: float
    strategy_variant: str
    out_of_sample: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_trade(self) -> TradeResult:
        if self.direction is Direction.NEUTRAL:
            raise ValueError("a closed trade cannot be neutral")
        if self.closed_at <= self.opened_at:
            raise ValueError("trade close must be after trade open")
        return self


def utc_now() -> datetime:
    """Injectable default clock for edges where a provider must timestamp receipt."""

    return datetime.now(UTC)
