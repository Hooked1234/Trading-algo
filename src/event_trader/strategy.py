"""Pre-registered SEC 8-K continuation strategy."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256

from .calendar import NEW_YORK, NyseSessionCalendar
from .domain import (
    Direction,
    EventSnapshot,
    InsightStatus,
    Materiality,
    NewsInsight,
    Signal,
)

RELEVANT_ITEMS = frozenset({"1.01", "2.01", "2.02", "5.02", "7.01", "8.01"})
_ITEM_CATEGORY_PRIORITY = (
    ("2.02", "earnings"),
    ("2.01", "m_and_a"),
    ("1.01", "material_agreement"),
    ("5.02", "management"),
    ("7.01", "regulation_fd"),
    ("8.01", "other_material_event"),
)


def filing_item_category(items: tuple[str, ...]) -> str:
    """Deterministic event category available without reading filing text."""

    item_set = set(items)
    for item, category in _ITEM_CATEGORY_PRIORITY:
        if item in item_set:
            return category
    return "unclassified_item"


def deterministic_direction(snapshot: EventSnapshot) -> Direction:
    """Direction implied by price reaction alone, without any filing semantics.

    Both the historical quant-only comparator and the live candidate gate use
    this function, so a preselected candidate has exactly one direction.
    """

    market = snapshot.market
    if market.beta_adjusted_return_z >= 0 and market.last > market.session_vwap:
        return Direction.LONG
    if market.beta_adjusted_return_z < 0 and market.last < market.session_vwap:
        return Direction.SHORT
    return Direction.NEUTRAL


class ContinuationStrategy:
    """Deterministic gate around a versioned, structured text insight."""

    version = "sec-8k-continuation-v1"
    insight_influences_orders = True

    def __init__(self, calendar: NyseSessionCalendar | None = None) -> None:
        self.calendar = calendar or NyseSessionCalendar()

    def rejection_reasons(
        self,
        snapshot: EventSnapshot,
        insight: NewsInsight | None,
        now: datetime,
    ) -> tuple[str, ...]:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("strategy decision time must be timezone-aware")
        if insight is None:
            return ("INSIGHT_MISSING", *self.quant_rejection_reasons(
                snapshot, Direction.NEUTRAL, now
            ))
        reasons: list[str] = []
        filing = snapshot.filing
        if (
            insight.event_id != filing.event_id
            or insight.accession_number != filing.accession_number
        ):
            reasons.append("INSIGHT_EVENT_MISMATCH")
        if insight.status is not InsightStatus.ACTIONABLE:
            reasons.append("INSIGHT_ABSTAINED")
        if insight.materiality is not Materiality.HIGH:
            reasons.append("MATERIALITY_NOT_HIGH")
        if insight.direction is Direction.NEUTRAL:
            reasons.append("DIRECTION_NEUTRAL")
        if insight.confidence < 0.75:
            reasons.append("INSIGHT_CONFIDENCE_LOW")
        document_hashes = {document.sha256 for document in filing.documents}
        if not insight.evidence:
            reasons.append("INSIGHT_EVIDENCE_MISSING")
        elif any(span.document_sha256 not in document_hashes for span in insight.evidence):
            reasons.append("INSIGHT_EVIDENCE_MISMATCH")

        reasons.extend(self.quant_rejection_reasons(snapshot, insight.direction, now))
        return tuple(dict.fromkeys(reasons))

    def quant_rejection_reasons(
        self,
        snapshot: EventSnapshot,
        direction: Direction,
        now: datetime,
    ) -> tuple[str, ...]:
        filing = snapshot.filing
        market = snapshot.market
        reasons: list[str] = []

        if filing.form != "8-K":
            reasons.append("FORM_NOT_TRADABLE")
        if len(filing.symbols) != 1 or market.symbol not in filing.symbols:
            reasons.append("SYMBOL_MAPPING_UNRESOLVED")
        if market.security_type != "common_stock" or not market.us_listed:
            reasons.append("SECURITY_NOT_US_COMMON_STOCK")
        if not filing.complete:
            reasons.append("FILING_INCOMPLETE")
        if not RELEVANT_ITEMS.intersection(filing.items):
            reasons.append("ITEM_NOT_RELEVANT")
        if market.last < Decimal("5"):
            reasons.append("PRICE_BELOW_MINIMUM")
        if market.median_dollar_volume_20d < Decimal("20000000"):
            reasons.append("DOLLAR_VOLUME_BELOW_MINIMUM")
        if not market.data_fresh:
            reasons.append("MARKET_DATA_STALE")
        if market.halted:
            reasons.append("SYMBOL_HALTED")
        if market.quote.spread_bps > Decimal("20"):
            reasons.append("SPREAD_TOO_WIDE")
        if abs(market.beta_adjusted_return_z) < 1.5:
            reasons.append("ABNORMAL_RETURN_TOO_SMALL")
        if market.relative_volume < 2.0:
            reasons.append("RELATIVE_VOLUME_TOO_LOW")
        if direction is Direction.LONG:
            if market.quote.ask - market.atr_5m * Decimal("1.5") <= 0:
                reasons.append("INVALID_STOP_PRICE")
            if market.beta_adjusted_return_z < 1.5:
                reasons.append("RETURN_DIRECTION_MISMATCH")
            if market.last <= market.session_vwap:
                reasons.append("VWAP_DIRECTION_MISMATCH")
        elif direction is Direction.SHORT:
            if market.beta_adjusted_return_z > -1.5:
                reasons.append("RETURN_DIRECTION_MISMATCH")
            if market.last >= market.session_vwap:
                reasons.append("VWAP_DIRECTION_MISMATCH")
            if not market.shortable or market.shortable_shares <= 0:
                reasons.append("NOT_SHORTABLE")

        if not self.calendar.is_entry_window(now):
            reasons.append("OUTSIDE_ENTRY_WINDOW")

        filing_local = filing.first_seen_at.astimezone(NEW_YORK)
        now_local = now.astimezone(NEW_YORK)
        was_rth_event = (
            filing_local.date() == now_local.date()
            and (filing_local.hour > 9 or (filing_local.hour == 9 and filing_local.minute >= 30))
            and filing_local.hour < 16
        )
        if was_rth_event and now - filing.first_seen_at > timedelta(minutes=15):
            reasons.append("EVENT_TOO_OLD")

        return tuple(dict.fromkeys(reasons))

    def evaluate(
        self,
        snapshot: EventSnapshot,
        insight: NewsInsight | None,
        now: datetime,
    ) -> Signal | None:
        if insight is None or self.rejection_reasons(snapshot, insight, now):
            return None

        return _build_signal(
            snapshot,
            direction=insight.direction,
            now=now,
            strategy_version=self.version,
            insight_version=(
                f"{insight.model_provider}/{insight.model_name}/"
                f"{insight.prompt_version}/{insight.schema_version}"
            ),
        )


class QuantOnlyContinuationStrategy(ContinuationStrategy):
    """Price/volume-only comparator; it never consumes filing semantics."""

    version = "sec-8k-quant-only-v1"
    insight_influences_orders = False

    @staticmethod
    def _direction(snapshot: EventSnapshot) -> Direction:
        return Direction.LONG if snapshot.market.beta_adjusted_return_z >= 0 else Direction.SHORT


    def rejection_reasons(
        self,
        snapshot: EventSnapshot,
        insight: NewsInsight | None,
        now: datetime,
    ) -> tuple[str, ...]:
        del insight
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("strategy decision time must be timezone-aware")
        return self.quant_rejection_reasons(snapshot, self._direction(snapshot), now)

    def evaluate(
        self,
        snapshot: EventSnapshot,
        insight: NewsInsight | None,
        now: datetime,
    ) -> Signal | None:
        if self.rejection_reasons(snapshot, insight, now):
            return None
        return _build_signal(
            snapshot,
            direction=self._direction(snapshot),
            now=now,
            strategy_version=self.version,
            insight_version="quant-only/no-text/v1",
        )


def _build_signal(
    snapshot: EventSnapshot,
    *,
    direction: Direction,
    now: datetime,
    strategy_version: str,
    insight_version: str,
) -> Signal:
    market = snapshot.market
    entry = market.quote.ask if direction is Direction.LONG else market.quote.bid
    stop_offset = market.atr_5m * Decimal("1.5")
    raw_stop = entry - stop_offset if direction is Direction.LONG else entry + stop_offset
    stop = raw_stop.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    event_key = (
        f"{snapshot.filing.event_id}:{market.symbol}:{strategy_version}:"
        f"{direction.value}:{now.isoformat()}"
    )
    signal_id = sha256(event_key.encode()).hexdigest()[:32]

    local = now.astimezone(NEW_YORK)
    force_flat = local.replace(hour=15, minute=55, second=0, microsecond=0).astimezone(now.tzinfo)
    expires = min(now + timedelta(minutes=60), force_flat)
    return Signal(
        signal_id=signal_id,
        event_id=snapshot.filing.event_id,
        accession_number=snapshot.filing.accession_number,
        symbol=market.symbol,
        strategy_version=strategy_version,
        direction=direction,
        decided_at=now,
        entry_limit=entry,
        stop_price=stop,
        expires_at=expires,
        holding_minutes=60,
        quant_features={
            "beta_adjusted_return_z": market.beta_adjusted_return_z,
            "relative_volume": market.relative_volume,
            "spread_bps": float(market.quote.spread_bps),
            "session_vwap": float(market.session_vwap),
            "atr_5m": float(market.atr_5m),
        },
        insight_version=insight_version,
    )
