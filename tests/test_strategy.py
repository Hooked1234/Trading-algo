from datetime import timedelta
from decimal import Decimal

from event_trader.domain import Direction, EventSnapshot, InsightStatus, Materiality, NewsInsight
from event_trader.strategy import ContinuationStrategy, QuantOnlyContinuationStrategy


def test_eligible_long_event_creates_deterministic_signal(
    snapshot, long_insight, decision_time
) -> None:
    strategy = ContinuationStrategy()
    first = strategy.evaluate(snapshot, long_insight, decision_time)
    second = strategy.evaluate(snapshot, long_insight, decision_time)
    assert first is not None
    assert first == second
    assert first.direction is Direction.LONG
    assert first.entry_limit == snapshot.market.quote.ask
    assert first.stop_price == Decimal("98.60")


def test_invalid_insight_event_fails_closed(snapshot, long_insight, decision_time) -> None:
    payload = long_insight.model_dump()
    payload["event_id"] = "different-event"
    insight = NewsInsight.model_validate(payload)
    strategy = ContinuationStrategy()
    assert strategy.evaluate(snapshot, insight, decision_time) is None
    assert "INSIGHT_EVENT_MISMATCH" in strategy.rejection_reasons(snapshot, insight, decision_time)


def test_amendment_is_never_traded(snapshot, long_insight, decision_time) -> None:
    filing_payload = snapshot.filing.model_dump()
    filing_payload["form"] = "8-K/A"
    filing = type(snapshot.filing).model_validate(filing_payload)
    insight_payload = long_insight.model_dump()
    amended_snapshot = EventSnapshot(
        filing=filing,
        market=snapshot.market,
        document_text=snapshot.document_text,
    )
    insight_payload["event_id"] = filing.event_id
    insight = NewsInsight.model_validate(insight_payload)
    assert ContinuationStrategy().evaluate(amended_snapshot, insight, decision_time) is None


def test_low_materiality_and_old_event_are_rejected(snapshot, long_insight, decision_time) -> None:
    payload = long_insight.model_dump()
    payload.update(
        status=InsightStatus.ACTIONABLE,
        materiality=Materiality.LOW,
    )
    insight = NewsInsight.model_validate(payload)
    old_time = decision_time + timedelta(minutes=20)
    reasons = ContinuationStrategy().rejection_reasons(snapshot, insight, old_time)
    assert "MATERIALITY_NOT_HIGH" in reasons
    assert "EVENT_TOO_OLD" in reasons


def test_quant_only_baseline_does_not_require_actionable_text_insight(
    snapshot, filing, decision_time
) -> None:
    abstention = NewsInsight.abstain(
        event_id=filing.event_id,
        accession_number=filing.accession_number,
        reason="quant_only_baseline",
    )

    signal = QuantOnlyContinuationStrategy().evaluate(snapshot, abstention, decision_time)

    assert signal is not None
    assert signal.strategy_version == "sec-8k-quant-only-v1"
    assert signal.direction is Direction.LONG
    assert signal.insight_version == "quant-only/no-text/v1"
