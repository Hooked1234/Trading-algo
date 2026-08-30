"""Durable, hashed daily operating report built from the operational state.

The markdown file stays human-readable; the sibling JSON artifact carries the
metrics and a content address.  Operational acceptance is computed from those
hashed artifacts, never from a hand-written summary.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, model_validator

from .artifacts import HashedArtifact, Sha256, read_artifact, write_artifact
from .calendar import NEW_YORK, NyseSessionCalendar
from .domain import FrozenModel, InsightStatus, NewsInsight


class DailyMetrics(FrozenModel):
    session_date: date
    generated_at: datetime
    expected_session_seconds: int = Field(ge=0)
    observed_live_seconds: int = Field(ge=0)
    filings_seen: int = Field(ge=0)
    feed_reconciliation_missing: int = Field(ge=0)
    candidates: int = Field(ge=0)
    insight_abstentions: int = Field(ge=0)
    signals: int = Field(ge=0)
    shadow_orders: int = Field(ge=0)
    submitted_orders: int = Field(ge=0)
    closed_trades: int = Field(ge=0)
    duplicate_orders: int = Field(ge=0)
    position_mismatches: int = Field(ge=0)
    critical_errors: tuple[str, ...] = ()
    p95_decision_latency_seconds: float = Field(default=0, ge=0)
    model_cost_eur: float = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_session_observation(self) -> DailyMetrics:
        if self.observed_live_seconds > self.expected_session_seconds:
            raise ValueError("observed live time cannot exceed the expected session")
        return self

    @property
    def availability(self) -> float:
        if self.expected_session_seconds <= 0:
            return 0
        return min(1.0, self.observed_live_seconds / self.expected_session_seconds)

    @property
    def operational_acceptance(self) -> bool:
        return (
            self.availability >= 0.99
            and self.feed_reconciliation_missing == 0
            and self.duplicate_orders == 0
            and self.position_mismatches == 0
            and not self.critical_errors
        )


class PaperAcceptanceResult(FrozenModel):
    passed: bool
    session_count: int
    closed_trades: int
    weighted_availability: float
    duplicate_orders: int
    position_mismatches: int
    feed_reconciliation_missing: int
    critical_error_count: int
    reasons: tuple[str, ...]


def evaluate_paper_acceptance(days: list[DailyMetrics]) -> PaperAcceptanceResult:
    unique_days = {metrics.session_date: metrics for metrics in days}
    if len(unique_days) != len(days):
        raise ValueError("paper acceptance requires one immutable record per session date")
    records = list(unique_days.values())
    expected_seconds = sum(metrics.expected_session_seconds for metrics in records)
    observed_seconds = sum(metrics.observed_live_seconds for metrics in records)
    availability = observed_seconds / expected_seconds if expected_seconds else 0.0
    closed_trades = sum(metrics.closed_trades for metrics in records)
    duplicate_orders = sum(metrics.duplicate_orders for metrics in records)
    position_mismatches = sum(metrics.position_mismatches for metrics in records)
    feed_reconciliation_missing = sum(metrics.feed_reconciliation_missing for metrics in records)
    critical_errors = sum(len(metrics.critical_errors) for metrics in records)
    reasons: list[str] = []
    if len(records) < 30:
        reasons.append("FEWER_THAN_30_SESSIONS")
    if closed_trades < 50:
        reasons.append("FEWER_THAN_50_CLOSED_TRADES")
    if availability < 0.99:
        reasons.append("AVAILABILITY_BELOW_99_PERCENT")
    if duplicate_orders:
        reasons.append("DUPLICATE_ORDERS")
    if position_mismatches:
        reasons.append("POSITION_MISMATCHES")
    if feed_reconciliation_missing:
        reasons.append("SEC_RECONCILIATION_GAPS")
    if critical_errors:
        reasons.append("CRITICAL_ERRORS")
    return PaperAcceptanceResult(
        passed=not reasons,
        session_count=len(records),
        closed_trades=closed_trades,
        weighted_availability=availability,
        duplicate_orders=duplicate_orders,
        position_mismatches=position_mismatches,
        feed_reconciliation_missing=feed_reconciliation_missing,
        critical_error_count=critical_errors,
        reasons=tuple(reasons),
    )


def write_daily_report(metrics: DailyMetrics, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{metrics.session_date.isoformat()}.md"
    if target.exists():
        raise FileExistsError(f"daily report is immutable: {target}")
    lines = [
        f"# Trading operations — {metrics.session_date.isoformat()}",
        "",
        f"- Operational acceptance: {'PASS' if metrics.operational_acceptance else 'FAIL'}",
        f"- Session availability: {metrics.availability:.2%}",
        "- Filings / candidates / signals: "
        f"{metrics.filings_seen} / {metrics.candidates} / {metrics.signals}",
        f"- Shadow / submitted orders: {metrics.shadow_orders} / {metrics.submitted_orders}",
        f"- Closed trades: {metrics.closed_trades}",
        f"- SEC reconciliation missing: {metrics.feed_reconciliation_missing}",
        f"- Duplicate orders: {metrics.duplicate_orders}",
        f"- Position mismatches: {metrics.position_mismatches}",
        f"- p95 decision latency: {metrics.p95_decision_latency_seconds:.2f}s",
        f"- Model cost: EUR {metrics.model_cost_eur:.2f}",
        "",
        "## Critical errors",
        "",
    ]
    lines.extend(f"- {error}" for error in metrics.critical_errors)
    if not metrics.critical_errors:
        lines.append("- None")
    markdown = "\n".join(lines) + "\n"
    artifact = DailyReportArtifact(
        metrics=metrics,
        markdown_sha256=sha256(markdown.encode("utf-8")).hexdigest(),
    ).sealed()
    _markdown_path, artifact_path = report_paths(directory, metrics.session_date)
    write_artifact(artifact, artifact_path)
    target.write_text(markdown, encoding="utf-8")
    return target


class DailyReportArtifact(HashedArtifact):
    """Hashed evidence for one operating session."""

    artifact_version: Literal["1"] = "1"
    metrics: DailyMetrics
    markdown_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_sha256: Sha256 = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @property
    def session_date(self) -> date:
        return self.metrics.session_date


def report_paths(directory: Path, session_date: date) -> tuple[Path, Path]:
    stem = session_date.isoformat()
    return directory / f"{stem}.md", directory / f"{stem}.json"


def load_daily_reports(directory: Path) -> tuple[DailyReportArtifact, ...]:
    """Load and re-verify every hashed session artifact in ``directory``."""

    artifacts = [
        read_artifact(DailyReportArtifact, path) for path in sorted(Path(directory).glob("*.json"))
    ]
    return tuple(sorted(artifacts, key=lambda item: item.session_date))


class SessionStateReader(Protocol):
    """The operational state a session report is derived from."""

    async def list_filings_for_session(self, session_date: date) -> tuple[object, ...]: ...

    async def list_signals_since(self, since: datetime) -> tuple[object, ...]: ...

    async def list_order_intents_since(self, since: datetime) -> tuple[object, ...]: ...

    async def list_execution_reports_since(self, since: datetime) -> tuple[object, ...]: ...

    async def list_insights_since(self, since: datetime) -> tuple[NewsInsight, ...]: ...

    async def list_pipeline_outcomes_since(
        self, since: datetime
    ) -> tuple[tuple[str, str, str], ...]: ...

    async def list_critical_events(
        self, *, since: datetime | None = None
    ) -> tuple[dict[str, str], ...]: ...

    async def get_heartbeat(self, session_date: date) -> tuple[datetime, datetime, int] | None: ...


async def build_daily_metrics(
    store: SessionStateReader,
    *,
    session_date: date,
    generated_at: datetime,
    calendar: NyseSessionCalendar | None = None,
    model_cost_eur: float = 0.0,
) -> DailyMetrics:
    """Derive one session's metrics from durable state only."""

    session_calendar = calendar or NyseSessionCalendar()
    start = datetime.combine(session_date, time.min, tzinfo=NEW_YORK).astimezone(UTC)
    expected = _expected_session_seconds(session_calendar, session_date)
    heartbeat = await store.get_heartbeat(session_date)
    observed = 0
    if heartbeat is not None:
        first, last, _ticks = heartbeat
        observed = max(0, int((last - first).total_seconds()))
    filings = await store.list_filings_for_session(session_date)
    signals = await store.list_signals_since(start)
    intents = await store.list_order_intents_since(start)
    reports = await store.list_execution_reports_since(start)
    insights = await store.list_insights_since(start)
    outcomes = await store.list_pipeline_outcomes_since(start)
    critical = await store.list_critical_events(since=start)

    stages = [stage for _event_id, stage, _payload in outcomes]
    candidates = sum(_outcome_reached_a_candidate(payload) for _e, _s, payload in outcomes)
    return DailyMetrics(
        session_date=session_date,
        generated_at=generated_at,
        expected_session_seconds=expected,
        observed_live_seconds=min(observed, expected),
        filings_seen=len(filings),
        feed_reconciliation_missing=sum(
            1 for event in critical if event["code"].startswith("SEC_DAILY")
        ),
        candidates=candidates,
        insight_abstentions=sum(
            1 for insight in insights if insight.status is InsightStatus.ABSTAIN
        ),
        signals=len(signals),
        shadow_orders=sum(
            1 for intent in intents if getattr(intent, "submission_mode", "") == "shadow"
        ),
        submitted_orders=sum(
            1 for intent in intents if getattr(intent, "submission_mode", "") == "paper"
        ),
        closed_trades=sum(
            1
            for report in reports
            if getattr(getattr(report, "status", None), "value", "") == "filled"
        ),
        duplicate_orders=sum(1 for stage in stages if stage == "duplicate_event"),
        position_mismatches=sum(1 for event in critical if "POSITION" in event["code"]),
        critical_errors=tuple(dict.fromkeys(event["code"] for event in critical)),
        model_cost_eur=model_cost_eur,
    )


def _outcome_reached_a_candidate(payload: str) -> bool:
    try:
        decoded = json.loads(payload)
    except ValueError:
        return False
    candidate = decoded.get("candidate")
    return bool(candidate) and bool(candidate.get("accepted"))


def _expected_session_seconds(calendar: NyseSessionCalendar, session_date: date) -> int:
    if not calendar.is_session(session_date):
        return 0
    opening = datetime.combine(session_date, time(9, 30), tzinfo=NEW_YORK)
    closing = datetime.combine(session_date, time(16, 0), tzinfo=NEW_YORK)
    return int((closing - opening) / timedelta(seconds=1))


def acceptance_from_reports(
    artifacts: Iterable[DailyReportArtifact],
) -> PaperAcceptanceResult:
    """Evaluate operational acceptance from hashed session artifacts only."""

    records: Sequence[DailyReportArtifact] = tuple(artifacts)
    for artifact in records:
        artifact.verify()
    return evaluate_paper_acceptance([artifact.metrics for artifact in records])
