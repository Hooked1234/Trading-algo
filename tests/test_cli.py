from datetime import timedelta
from decimal import Decimal

import pytest
import typer
from typer.testing import CliRunner

from event_trader.cli import (
    _require_paper_prerequisites,
    _validate_ai_pairing_lineage,
    app,
)
from event_trader.config import Settings
from event_trader.domain import (
    Direction,
    EvidenceSpan,
    InsightStatus,
    Materiality,
    NewsInsight,
    TradeResult,
)
from event_trader.strategy import QuantOnlyContinuationStrategy

runner = CliRunner()


def test_doctor_is_offline_and_live_is_unavailable() -> None:
    result = runner.invoke(app, ["doctor"], env={"TRADING_ENV_FILE": ""})
    assert result.exit_code == 0
    assert '"live_execution_available": false' in result.stdout


def test_cli_has_research_and_data_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "research-gate" in result.stdout
    assert "data-quality" in result.stdout
    assert "benchmark-models" in result.stdout
    assert "historical-backfill" in result.stdout
    assert "backfill-plan" in result.stdout
    assert "paper-acceptance" in result.stdout
    assert "create-promotion" in result.stdout
    assert "reconcile-sec-daily" in result.stdout
    assert "risk-halt-status" in result.stdout
    assert "risk-halt-reset" in result.stdout
    assert "run-paper" in result.stdout


def test_paper_prerequisites_fail_without_account_and_promotion(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("event_trader.cli.ibapi_available", lambda: True)
    settings = Settings(
        sec_user_agent="event-trader test@example.com",
        promotion_artifact_path=tmp_path / "missing-promotion.json",
    )

    with pytest.raises(typer.BadParameter, match="Paper mode cannot start without") as exc:
        _require_paper_prerequisites(settings)

    message = str(exc.value)
    assert "TRADING_PAPER_ACCOUNT_ID" in message
    assert "missing-promotion.json" in message


def _paired_trade(decision_time, *, event_id: str, version: str, category: str) -> TradeResult:
    accession = f"0000000001-26-{int(event_id[-1]):06d}"
    return TradeResult(
        trade_id=f"{version}:{event_id}",
        symbol="AAPL",
        direction=Direction.LONG,
        category=category,
        opened_at=decision_time,
        closed_at=decision_time + timedelta(minutes=60),
        net_pnl=Decimal("10"),
        return_bps=10,
        strategy_variant=version,
        out_of_sample=False,
        metadata={
            "event_id": event_id,
            "accession_number": accession,
            "filing_accepted_at": (decision_time - timedelta(minutes=10)).isoformat(),
            "coverage_record_id": f"coverage:{event_id}",
            "scenario": "source_lag_5m_primary",
            "provider": "alpaca",
            "feed": "sip",
            "availability_lag_minutes": 5,
            "sample_period": "forward",
            "case_input_sha256": "a" * 64,
            "stress_net_pnl": "5",
        },
    )


def _paired_insight(decision_time, *, event_id: str, actionable: bool) -> NewsInsight:
    del decision_time
    accession = f"0000000001-26-{int(event_id[-1]):06d}"
    if not actionable:
        return NewsInsight.abstain(
            event_id=event_id,
            accession_number=accession,
            reason="insufficient_evidence",
            model_provider="provider",
            model_name="model-v1",
            prompt_version="prompt-v1",
        )
    return NewsInsight(
        event_id=event_id,
        accession_number=accession,
        status=InsightStatus.ACTIONABLE,
        category="guidance",
        direction=Direction.LONG,
        materiality=Materiality.HIGH,
        confidence=0.9,
        evidence=(EvidenceSpan(document_sha256="a" * 64, excerpt="Raised guidance."),),
        model_provider="provider",
        model_name="model-v1",
        prompt_version="prompt-v1",
    )


def test_ai_pairing_abstentions_are_derived_from_pinned_insights(decision_time) -> None:
    baseline = [
        _paired_trade(
            decision_time,
            event_id=event_id,
            version=QuantOnlyContinuationStrategy.version,
            category="earnings",
        )
        for event_id in ("event-1", "event-2")
    ]
    candidate = baseline[0].model_copy(
        update={
            "trade_id": "ai:event-1",
            "strategy_variant": "sec-8k-continuation-v1",
            "category": "guidance",
        }
    )

    abstentions = _validate_ai_pairing_lineage(
        paired_trades=[*baseline, candidate],
        insights=[
            _paired_insight(decision_time, event_id="event-1", actionable=True),
            _paired_insight(decision_time, event_id="event-2", actionable=False),
        ],
        baseline_version=QuantOnlyContinuationStrategy.version,
        candidate_version="sec-8k-continuation-v1",
        model_id="provider/model-v1",
        prompt_version="prompt-v1",
        schema_version="1",
    )

    assert abstentions == ("event-2",)


def test_ai_pairing_rejects_hindsight_selected_missing_trade(decision_time) -> None:
    baseline = _paired_trade(
        decision_time,
        event_id="event-1",
        version=QuantOnlyContinuationStrategy.version,
        category="earnings",
    )

    with pytest.raises(typer.BadParameter, match="must be derived from the paired insights"):
        _validate_ai_pairing_lineage(
            paired_trades=[baseline],
            insights=[_paired_insight(decision_time, event_id="event-1", actionable=True)],
            baseline_version=QuantOnlyContinuationStrategy.version,
            candidate_version="sec-8k-continuation-v1",
            model_id="provider/model-v1",
            prompt_version="prompt-v1",
            schema_version="1",
        )
