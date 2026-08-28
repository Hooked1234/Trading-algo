from datetime import date, timedelta

import pytest

from event_trader.reporting import (
    DailyMetrics,
    evaluate_paper_acceptance,
    write_daily_report,
)


def test_daily_report_acceptance_and_immutability(tmp_path, decision_time) -> None:
    metrics = DailyMetrics(
        session_date=date(2026, 8, 25),
        generated_at=decision_time,
        expected_session_seconds=10_000,
        observed_live_seconds=9_950,
        filings_seen=5,
        feed_reconciliation_missing=0,
        candidates=2,
        insight_abstentions=1,
        signals=1,
        shadow_orders=1,
        submitted_orders=0,
        closed_trades=0,
        duplicate_orders=0,
        position_mismatches=0,
    )
    assert metrics.operational_acceptance
    target = write_daily_report(metrics, tmp_path)
    assert "PASS" in target.read_text(encoding="utf-8")
    with pytest.raises(FileExistsError):
        write_daily_report(metrics, tmp_path)


def test_paper_acceptance_requires_30_sessions_and_50_trades(decision_time) -> None:
    days = [
        DailyMetrics(
            session_date=date(2026, 1, 2) + timedelta(days=index),
            generated_at=decision_time,
            expected_session_seconds=1_000,
            observed_live_seconds=995,
            filings_seen=1,
            feed_reconciliation_missing=0,
            candidates=1,
            insight_abstentions=0,
            signals=1,
            shadow_orders=0,
            submitted_orders=2,
            closed_trades=2,
            duplicate_orders=0,
            position_mismatches=0,
        )
        for index in range(30)
    ]
    result = evaluate_paper_acceptance(days)
    assert result.passed
    assert result.closed_trades == 60


def test_paper_acceptance_fails_operational_error(decision_time) -> None:
    day = DailyMetrics(
        session_date=date(2026, 1, 2),
        generated_at=decision_time,
        expected_session_seconds=1_000,
        observed_live_seconds=980,
        filings_seen=0,
        feed_reconciliation_missing=1,
        candidates=0,
        insight_abstentions=0,
        signals=0,
        shadow_orders=0,
        submitted_orders=0,
        closed_trades=0,
        duplicate_orders=1,
        position_mismatches=1,
        critical_errors=("feed gap",),
    )
    result = evaluate_paper_acceptance([day])
    assert not result.passed
    assert "DUPLICATE_ORDERS" in result.reasons
    assert "SEC_RECONCILIATION_GAPS" in result.reasons


def test_paper_acceptance_rejects_duplicate_session_records(decision_time) -> None:
    day = DailyMetrics(
        session_date=date(2026, 1, 2),
        generated_at=decision_time,
        expected_session_seconds=1_000,
        observed_live_seconds=1_000,
        filings_seen=0,
        feed_reconciliation_missing=0,
        candidates=0,
        insight_abstentions=0,
        signals=0,
        shadow_orders=0,
        submitted_orders=0,
        closed_trades=0,
        duplicate_orders=0,
        position_mismatches=0,
    )
    with pytest.raises(ValueError, match="one immutable"):
        evaluate_paper_acceptance([day, day])
