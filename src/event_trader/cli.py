"""Safe local operator CLI. No command can enable live execution."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer
from pydantic import TypeAdapter

from .artifacts import read_artifact, write_artifact
from .backfill import (
    BACKFILL_END,
    BACKFILL_START,
    BackfillConfig,
    CoverageRecord,
    HistoricalBackfillRunner,
    plan_sec_quarter_indexes,
)
from .backfill_store import SQLiteBackfillStore
from .backtest import BacktestRunArtifact, BacktestVariant
from .calendar import NyseSessionCalendar
from .composition import (
    PaperRuntime,
    ShadowRuntime,
    build_paper_runtime,
    build_shadow_runtime,
    default_warning_sink,
)
from .config import Settings
from .datasets import (
    DatasetManifest,
    ParquetMarketDataLake,
    build_data_quality_report,
    build_dataset_manifest,
)
from .documents import FilingDocumentLoader
from .domain import (
    Direction,
    FilingEvent,
    InsightStatus,
    Materiality,
    NewsInsight,
    TradeResult,
)
from .eligibility import CsvEligibilityResolver
from .insights import InsightArtifact, build_insight_artifact
from .labels import ReferenceLabel, ScoredPrediction, benchmark_predictions
from .promotion import (
    ResearchPromotionArtifact,
    load_promotion_artifact,
    write_promotion_artifact,
)
from .providers.ibkr import (
    IBKRBrokerAdapter,
    IBKRConnectionConfig,
    NativeIBAPIBackend,
    ibapi_available,
)
from .providers.ibkr_features import IBKRLiveFeatureProvider
from .providers.ibkr_market import IBKRMarketDataProvider, SnapshotBuilder
from .providers.market import AlpacaMarketDataProvider
from .providers.sec import AsyncRateLimiter, SecCursor, SecProvider, SecProviderConfig
from .providers.sec_daily import SecDailyIndexProvider
from .providers.sec_history import HistoricalSecFilingResolver
from .reconciliation import DailySecReconciler, SQLiteSecReconciliationLedger
from .reporting import (
    DailyMetrics,
    acceptance_from_reports,
    build_daily_metrics,
    evaluate_paper_acceptance,
    load_daily_reports,
    write_daily_report,
)
from .research import (
    ModelBenchmarkResult,
    PairedImprovementResult,
    ResearchGateEvaluator,
    ResearchGateResult,
    evaluate_research_run,
    paired_ai_gate,
    paired_variant_improvement,
    select_model,
)
from .research_cases import (
    HistoricalResearchCaseBuilder,
    ResearchCaseBuildArtifact,
    TradingStateManifest,
)
from .research_runs import build_backtest_run
from .risk_halt import SQLiteRiskHaltGuard
from .snapshot import LiveEventSnapshotFactory
from .storage import SQLiteOperationalStore
from .strategy import QuantOnlyContinuationStrategy

app = typer.Typer(
    name="event-trader",
    help="SEC 8-K research and IBKR paper-trading operator tools.",
    no_args_is_help=True,
)


def _print_model(model: object) -> None:
    if hasattr(model, "model_dump_json"):
        typer.echo(model.model_dump_json(indent=2))
    else:
        typer.echo(json.dumps(model, indent=2, default=str, sort_keys=True))


@app.command()
def doctor() -> None:
    """Inspect local readiness without network calls or secret disclosure."""

    settings = Settings()
    promotion_valid = False
    if settings.promotion_artifact_path.is_file():
        try:
            load_promotion_artifact(settings.promotion_artifact_path)
        except (OSError, ValueError):
            pass
        else:
            promotion_valid = True
    historical_eligibility_valid = False
    if settings.historical_eligibility_path.is_file():
        try:
            CsvEligibilityResolver(settings.historical_eligibility_path)
        except (OSError, ValueError):
            pass
        else:
            historical_eligibility_valid = True
    checks = {
        "python_3_12": sys.version_info[:2] == (3, 12),
        "environment_paper_only": settings.environment == "paper",
        "paper_account_configured": not settings.placeholder_credentials,
        "paper_account_allowlisted": (
            settings.paper_account_id.upper() in settings.allowed_paper_accounts
        ),
        "sec_user_agent_configured": (
            "example.invalid" not in settings.sec_user_agent
            and "local-contact" not in settings.sec_user_agent
        ),
        "alpaca_credentials_present": bool(settings.alpaca_api_key and settings.alpaca_api_secret),
        "ibapi_installed": ibapi_available(),
        "docker_available": shutil.which("docker") is not None,
        "research_promotion_artifact_valid": promotion_valid,
        "historical_eligibility_manifest_valid": historical_eligibility_valid,
        "shadow_runtime_assembled": True,
        "paper_execution_assembled": True,
        "live_execution_available": False,
    }
    _print_model(
        {
            "status": "ready_for_offline_research"
            if checks["python_3_12"] and checks["environment_paper_only"]
            else "setup_required",
            "checks": checks,
            "note": "External accounts are optional until their vertical slice is reached.",
        }
    )


@app.command("init-db")
def init_db() -> None:
    """Initialize the local SQLite ledger and raw SEC directory."""

    settings = Settings()
    store = SQLiteOperationalStore(settings.state_db_path, settings.raw_data_dir)
    store.close()
    typer.echo(f"Initialized operational store at {settings.state_db_path}")


async def _sec_once() -> int:
    settings = Settings()
    if "example.invalid" in settings.sec_user_agent:
        raise typer.BadParameter(
            "Configure TRADING_SEC_USER_AGENT locally before accessing SEC EDGAR."
        )
    async with SQLiteOperationalStore(
        settings.state_db_path,
        settings.raw_data_dir,
    ) as store:
        cursor = SecCursor.from_json(await store.get_cursor("sec.latest"))
        config = SecProviderConfig(
            user_agent=settings.sec_user_agent,
            requests_per_second=settings.sec_max_requests_per_second,
        )
        async with SecProvider(
            config,
            document_persistence=store.persist_document,
        ) as provider:
            result = await provider.poll(cursor)
        inserted = await store.save_poll(
            result.events,
            provider="sec.latest",
            cursor=result.cursor.to_json(),
        )
        return inserted


@app.command("sec-once")
def sec_once() -> None:
    """Poll and persist one current SEC 8-K feed page."""

    inserted = asyncio.run(_sec_once())
    typer.echo(f"Persisted {inserted} new filings")


@app.command("backfill-plan")
def backfill_plan(
    start: Annotated[str, typer.Option(help="Inclusive ISO date")] = str(BACKFILL_START),
    end: Annotated[str, typer.Option(help="Inclusive ISO date")] = str(BACKFILL_END),
) -> None:
    """Print the exact official SEC quarterly indexes without network access."""

    start_date = _parse_iso_date(start, "start")
    end_date = _parse_iso_date(end, "end")
    plans = plan_sec_quarter_indexes(start_date, end_date)
    _print_model(
        {
            "start": start_date,
            "end": end_date,
            "quarters": [
                {
                    "quarter": plan.quarter.key,
                    "slice_start": plan.start,
                    "slice_end": plan.end,
                    "master_url": plan.master_url,
                    "form_url": plan.form_url,
                }
                for plan in plans
            ],
        }
    )


async def _historical_backfill(start: date, end: date) -> object:
    settings = Settings()
    if start < BACKFILL_START or end > BACKFILL_END:
        raise typer.BadParameter(f"registered v1 range is {BACKFILL_START} through {BACKFILL_END}")
    if "example.invalid" in settings.sec_user_agent:
        raise typer.BadParameter("Configure TRADING_SEC_USER_AGENT before SEC backfill.")
    if settings.alpaca_api_key is None or settings.alpaca_api_secret is None:
        raise typer.BadParameter("Configure both Alpaca historical-data credentials locally.")

    sec_config = SecProviderConfig(
        user_agent=settings.sec_user_agent,
        requests_per_second=settings.sec_max_requests_per_second,
    )
    market_data = AlpacaMarketDataProvider(
        api_key=settings.alpaca_api_key.get_secret_value(),
        secret_key=settings.alpaca_api_secret.get_secret_value(),
        base_url=settings.alpaca_data_url,
    )
    async with SQLiteOperationalStore(
        settings.state_db_path,
        settings.raw_data_dir,
    ) as operational_store:
        async with SecProvider(
            sec_config,
            document_persistence=operational_store.persist_document,
        ) as sec:
            resolver = HistoricalSecFilingResolver(
                fetch_submission=sec.fetch_archive,
                hydrate_filing=sec.hydrate_documents,
            )

            async def fetch_index(url: str) -> bytes:
                return await sec.fetch_archive(url, max_bytes=sec_config.max_index_bytes)

            runner = HistoricalBackfillRunner(
                index_fetcher=fetch_index,
                filing_resolver=resolver,
                market_data=market_data,
                data_lake=ParquetMarketDataLake(settings.market_data_dir),
                store=SQLiteBackfillStore(settings.backfill_state_path),
                config=BackfillConfig(start=start, end=end, feed="sip"),
                eligibility_resolver=(
                    CsvEligibilityResolver(settings.historical_eligibility_path)
                    if settings.historical_eligibility_path.is_file()
                    else None
                ),
            )
            return await runner.run()


@app.command("historical-backfill")
def historical_backfill(
    start: Annotated[str, typer.Option(help="Inclusive ISO date")] = str(BACKFILL_START),
    end: Annotated[str, typer.Option(help="Inclusive ISO date")] = str(BACKFILL_END),
) -> None:
    """Run resumable SEC plus Alpaca-SIP backfill for the registered v1 range."""

    _print_model(
        asyncio.run(
            _historical_backfill(
                _parse_iso_date(start, "start"),
                _parse_iso_date(end, "end"),
            )
        )
    )


def _parse_iso_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{field} must be an ISO date (YYYY-MM-DD)") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_files(paths: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        payload = path.read_bytes()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _parse_directions(directions: str) -> tuple[Direction, ...]:
    try:
        enabled = tuple(
            Direction(item.strip().lower()) for item in directions.split(",") if item.strip()
        )
    except ValueError as exc:
        raise typer.BadParameter("directions must contain only long and/or short") from exc
    if not enabled or Direction.NEUTRAL in enabled:
        raise typer.BadParameter("enable at least one of long or short")
    return enabled


def _load_trading_state_manifest(path: Path) -> TradingStateManifest:
    manifest = TradingStateManifest.model_validate_json(path.read_text(encoding="utf-8"))
    if manifest.artifact_sha256 == "0" * 64:
        return manifest.sealed()
    manifest.verify()
    return manifest


@app.command("build-dataset-manifest")
def build_dataset_manifest_command(
    lake_root: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
    output: Annotated[Path, typer.Option(dir_okay=False)],
) -> None:
    """Hash every immutable parquet partition of the research data lake."""

    manifest = build_dataset_manifest(lake_root)
    write_artifact(manifest, output)
    typer.echo(manifest.artifact_sha256)


@app.command("build-research-cases")
def build_research_cases_command(
    coverage_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    lake_root: Annotated[Path, typer.Option(exists=True, file_okay=False)],
    raw_root: Annotated[Path, typer.Option(exists=True, file_okay=False)],
    trading_state_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    output: Annotated[Path, typer.Option(dir_okay=False)],
    lag_minutes: Annotated[int, typer.Option(help="One of 1, 3, 5, 10")] = 5,
    dataset_manifest: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
) -> None:
    """Build one insight-free case or one typed failure per coverage record."""

    coverage = TypeAdapter(list[CoverageRecord]).validate_json(
        coverage_json.read_text(encoding="utf-8")
    )
    manifest = _load_trading_state_manifest(trading_state_manifest)
    dataset_hash = (
        read_artifact(DatasetManifest, dataset_manifest).artifact_sha256
        if dataset_manifest is not None
        else None
    )
    builder = HistoricalResearchCaseBuilder(
        data=ParquetMarketDataLake(lake_root),
        documents=FilingDocumentLoader(raw_root),
    )
    try:
        artifact = builder.build_all(
            coverage,
            manifest,
            lag_minutes=lag_minutes,
            dataset_manifest_sha256=dataset_hash,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    write_artifact(artifact, output)
    _print_model(
        {
            "artifact_sha256": artifact.artifact_sha256,
            "coverage_count": artifact.coverage_count,
            "cases": len(artifact.cases),
            "failures": len(artifact.failures),
        }
    )


@app.command("build-insight-artifact")
def build_insight_artifact_command(
    insights_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    cases: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    output: Annotated[Path, typer.Option(dir_okay=False)],
    variant: Annotated[str, typer.Option(help="keyword or ai")] = "ai",
    model_id: Annotated[str, typer.Option()] = "none/none",
    prompt_version: Annotated[str, typer.Option()] = "1",
    schema_version: Annotated[str, typer.Option()] = "1",
) -> None:
    """Pin exactly one answer or abstention per preselected candidate."""

    case_artifact = read_artifact(ResearchCaseBuildArtifact, cases)
    insights = TypeAdapter(list[NewsInsight]).validate_json(
        insights_json.read_text(encoding="utf-8")
    )
    try:
        artifact = build_insight_artifact(
            case_artifact,
            insights,
            variant=BacktestVariant(variant),
            model_id=model_id,
            prompt_version=prompt_version,
            schema_version=schema_version,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    write_artifact(artifact, output)
    _print_model(
        {
            "artifact_sha256": artifact.artifact_sha256,
            "candidates": len(artifact.candidate_case_hashes),
            "abstentions": len(artifact.abstention_event_ids),
        }
    )


@app.command("run-backtest")
def run_backtest_command(
    cases: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    output: Annotated[Path, typer.Option(dir_okay=False)],
    variant: Annotated[str, typer.Option(help="quant-only, keyword or ai")] = "quant-only",
    insights: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
) -> None:
    """Evaluate every case of one artifact and record a hashed run."""

    case_artifact = read_artifact(ResearchCaseBuildArtifact, cases)
    insight_artifact = (
        read_artifact(InsightArtifact, insights) if insights is not None else None
    )
    try:
        run = build_backtest_run(
            case_artifact,
            variant=BacktestVariant(variant),
            insight_artifact=insight_artifact,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    write_artifact(run, output)
    _print_model(
        {
            "artifact_sha256": run.artifact_sha256,
            "strategy_version": run.strategy_version,
            "cost_model_version": run.cost_model_version,
            "cases": len(run.case_hashes),
            "trades": len(run.trades),
        }
    )


@app.command("research-gate")
def research_gate(
    run: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    directions: Annotated[str, typer.Option(help="Comma-separated: long,short")] = ("long,short"),
) -> None:
    """Evaluate the pre-registered gate on one complete backtest run artifact."""

    artifact = read_artifact(BacktestRunArtifact, run)
    result = evaluate_research_run(
        artifact,
        enabled_directions=_parse_directions(directions),
    )
    _print_model(result)
    if not result.passed:
        raise typer.Exit(code=2)


@app.command("paired-ai-gate")
def paired_ai_gate_command(
    quant_run: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    ai_run: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
) -> None:
    """Compare an AI run against its quant-only baseline over identical cases."""

    try:
        result = paired_ai_gate(
            read_artifact(BacktestRunArtifact, quant_run),
            read_artifact(BacktestRunArtifact, ai_run),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_model(result)
    if not result.passed:
        raise typer.Exit(code=2)


@app.command("create-promotion")
def create_promotion(
    research_result_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    research_trades_json: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    experiment_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    dataset_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    code_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    experiment_id: Annotated[str, typer.Option()],
    output: Annotated[Path | None, typer.Option(dir_okay=False)] = None,
    paired_result_json: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    model_result_json: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    paired_trades_json: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    paired_insights_json: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    labels_json: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
    predictions_json: Annotated[
        Path | None, typer.Option(exists=True, dir_okay=False)
    ] = None,
    prompt_version: Annotated[str | None, typer.Option()] = None,
    schema_version: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Create an immutable paper-promotion artifact from passed gate evidence."""

    settings = Settings()
    research = ResearchGateResult.model_validate_json(
        research_result_json.read_text(encoding="utf-8")
    )
    research_trades = TypeAdapter(list[TradeResult]).validate_json(
        research_trades_json.read_text(encoding="utf-8")
    )
    recomputed_research = ResearchGateEvaluator().evaluate(
        research_trades,
        strategy_version=research.strategy_version,
        enabled_directions=research.enabled_directions,
    )
    if recomputed_research != research:
        raise typer.BadParameter("research result does not match recomputed raw trades")
    if not research.passed:
        raise typer.BadParameter("research result did not pass")
    if (paired_result_json is None) != (model_result_json is None):
        raise typer.BadParameter("AI promotion requires both paired and model results")
    ai_influences_orders = paired_result_json is not None
    paired = (
        PairedImprovementResult.model_validate_json(
            paired_result_json.read_text(encoding="utf-8")
        )
        if paired_result_json is not None
        else None
    )
    model = (
        ModelBenchmarkResult.model_validate_json(model_result_json.read_text(encoding="utf-8"))
        if model_result_json is not None
        else None
    )
    ai_evidence_paths = (
        paired_trades_json,
        paired_insights_json,
        labels_json,
        predictions_json,
    )
    if ai_influences_orders and any(path is None for path in ai_evidence_paths):
        raise typer.BadParameter(
            "AI promotion requires paired trades, raw insights, labels and predictions"
        )
    if not ai_influences_orders and any(path is not None for path in ai_evidence_paths):
        raise typer.BadParameter("quant-only promotion cannot carry AI raw evidence")
    if (
        paired is not None
        and paired_trades_json is not None
        and paired_insights_json is not None
        and model is not None
        and prompt_version is not None
        and schema_version is not None
    ):
        paired_trades = TypeAdapter(list[TradeResult]).validate_json(
            paired_trades_json.read_text(encoding="utf-8")
        )
        paired_insights = TypeAdapter(list[NewsInsight]).validate_json(
            paired_insights_json.read_text(encoding="utf-8")
        )
        abstentions = _validate_ai_pairing_lineage(
            paired_trades=paired_trades,
            insights=paired_insights,
            baseline_version=QuantOnlyContinuationStrategy.version,
            candidate_version=paired.candidate_version,
            model_id=model.model_id,
            prompt_version=prompt_version,
            schema_version=schema_version,
        )
        recomputed_paired = paired_variant_improvement(
            paired_trades,
            baseline_version=QuantOnlyContinuationStrategy.version,
            candidate_version=paired.candidate_version,
            candidate_abstention_event_ids=abstentions,
        )
        if recomputed_paired != paired:
            raise typer.BadParameter("paired result does not match recomputed raw trades")
    if model is not None and labels_json is not None and predictions_json is not None:
        labels = TypeAdapter(list[ReferenceLabel]).validate_json(
            labels_json.read_text(encoding="utf-8")
        )
        predictions = TypeAdapter(list[ScoredPrediction]).validate_json(
            predictions_json.read_text(encoding="utf-8")
        )
        selected_model = select_model(benchmark_predictions(labels, predictions))
        if selected_model != model:
            raise typer.BadParameter("model result is not the recomputed selected benchmark")
    if paired is not None and (
        not paired.passed
        or paired.baseline_version != QuantOnlyContinuationStrategy.version
        or paired.candidate_version != research.strategy_version
    ):
        raise typer.BadParameter(
            "paired AI comparison must pass against the fixed quant-only baseline"
        )
    if model is not None and not model.passes:
        raise typer.BadParameter("model benchmark did not pass")
    if ai_influences_orders and (not prompt_version or not schema_version):
        raise typer.BadParameter("AI promotion requires prompt and schema versions")
    artifact = ResearchPromotionArtifact.create(
        experiment_id=experiment_id,
        strategy_version=research.strategy_version,
        enabled_directions=research.enabled_directions,
        ai_influences_orders=ai_influences_orders,
        research_gate_passed=research.passed,
        paired_improvement_passed=paired.passed if paired else False,
        model_gate_passed=model.passes if model else False,
        experiment_manifest_sha256=_sha256_file(experiment_manifest),
        dataset_manifest_sha256=_sha256_file(dataset_manifest),
        code_revision_sha256=_sha256_file(code_manifest),
        research_result_sha256=_sha256_file(research_result_json),
        research_evidence_sha256=_sha256_file(research_trades_json),
        paired_result_sha256=(
            _sha256_file(paired_result_json) if paired_result_json is not None else None
        ),
        model_result_sha256=(
            _sha256_file(model_result_json) if model_result_json is not None else None
        ),
        paired_evidence_sha256=(
            _sha256_files((paired_trades_json, paired_insights_json))
            if paired_trades_json is not None and paired_insights_json is not None
            else None
        ),
        model_evidence_sha256=(
            _sha256_files((labels_json, predictions_json))
            if labels_json is not None and predictions_json is not None
            else None
        ),
        model_id=model.model_id if model else None,
        prompt_version=prompt_version,
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )
    target = write_promotion_artifact(
        artifact, output or settings.promotion_artifact_path
    )
    typer.echo(str(target))


def _validate_ai_pairing_lineage(
    *,
    paired_trades: list[TradeResult],
    insights: list[NewsInsight],
    baseline_version: str,
    candidate_version: str,
    model_id: str,
    prompt_version: str,
    schema_version: str,
) -> tuple[str, ...]:
    """Derive AI abstentions from pinned raw insights, never from a manual list."""

    def index_trades(version: str) -> dict[str, TradeResult]:
        indexed: dict[str, TradeResult] = {}
        for trade in paired_trades:
            if trade.strategy_variant != version:
                continue
            event_id = trade.metadata.get("event_id")
            if not isinstance(event_id, str) or not event_id.strip():
                raise typer.BadParameter(f"{version} trades require metadata.event_id")
            normalized = event_id.strip()
            if normalized in indexed:
                raise typer.BadParameter(f"{version} trades require unique event ids")
            indexed[normalized] = trade
        return indexed

    baseline = index_trades(baseline_version)
    candidate = index_trades(candidate_version)
    if not baseline:
        raise typer.BadParameter("AI pairing requires quant-only baseline trades")
    insight_by_event: dict[str, NewsInsight] = {}
    for insight in insights:
        if insight.event_id in insight_by_event:
            raise typer.BadParameter("paired insights require unique event ids")
        if (
            insight.model_id != model_id
            or insight.prompt_version != prompt_version
            or insight.schema_version != schema_version
        ):
            raise typer.BadParameter(
                "paired insights must use the selected model, prompt and schema versions"
            )
        insight_by_event[insight.event_id] = insight
    if set(insight_by_event) != set(baseline):
        raise typer.BadParameter(
            "paired insights must cover every quant-only candidate exactly once"
        )

    expected_candidates: set[str] = set()
    abstentions: set[str] = set()
    for event_id, baseline_trade in baseline.items():
        insight = insight_by_event[event_id]
        accession = baseline_trade.metadata.get("accession_number")
        if accession is not None and accession != insight.accession_number:
            raise typer.BadParameter("paired insight accession does not match its event")
        actionable = (
            insight.status is InsightStatus.ACTIONABLE
            and insight.materiality is Materiality.HIGH
            and insight.confidence >= 0.75
            and insight.direction is baseline_trade.direction
            and bool(insight.evidence)
        )
        if actionable:
            expected_candidates.add(event_id)
        else:
            abstentions.add(event_id)

    if set(candidate) != expected_candidates:
        raise typer.BadParameter(
            "candidate trades and abstentions must be derived from the paired insights"
        )
    for event_id, candidate_trade in candidate.items():
        baseline_trade = baseline[event_id]
        insight = insight_by_event[event_id]
        if (
            candidate_trade.symbol != baseline_trade.symbol
            or candidate_trade.direction is not baseline_trade.direction
            or candidate_trade.opened_at != baseline_trade.opened_at
            or candidate_trade.closed_at != baseline_trade.closed_at
            or candidate_trade.net_pnl != baseline_trade.net_pnl
            or candidate_trade.return_bps != baseline_trade.return_bps
            or candidate_trade.category != insight.category
            or candidate_trade.metadata != baseline_trade.metadata
        ):
            raise typer.BadParameter(
                "AI candidate trade economics must match its quant-only event"
            )
    return tuple(sorted(abstentions))


async def _reconcile_sec_daily(target_date: date) -> object:
    settings = Settings()
    if "example.invalid" in settings.sec_user_agent:
        raise typer.BadParameter(
            "Configure TRADING_SEC_USER_AGENT before accessing SEC EDGAR."
        )
    config = SecProviderConfig(
        user_agent=settings.sec_user_agent,
        requests_per_second=settings.sec_max_requests_per_second,
    )
    ledger = SQLiteSecReconciliationLedger(settings.state_db_path)
    try:
        async with SQLiteOperationalStore(
            settings.state_db_path, settings.raw_data_dir
        ) as store:
            async with SecDailyIndexProvider(
                config,
                limiter=AsyncRateLimiter(settings.sec_max_requests_per_second),
            ) as source:
                return await DailySecReconciler(
                    source=source,
                    inventory=store,
                    ledger=ledger,
                ).run(session_date=target_date, reconciled_at=datetime.now(UTC))
    finally:
        ledger.close()


# --------------------------------------------------------------- shadow mode --


class _ShadowPrerequisiteError(typer.BadParameter):
    """The local runtime is assembled but its external inputs are missing."""


def _require_shadow_prerequisites(settings: Settings) -> None:
    missing: list[str] = []
    if "example.invalid" in settings.sec_user_agent:
        missing.append("TRADING_SEC_USER_AGENT (a real project name and contact address)")
    if not ibapi_available():
        missing.append("the optional 'ibkr' extra (uv sync --extra dev --extra ibkr)")
    if missing:
        raise _ShadowPrerequisiteError(
            "Shadow mode is assembled but cannot start without: "
            + "; ".join(missing)
            + ". See NEXT_STEPS.md; no order path is involved either way."
        )


def _shadow_market_stack(settings: Settings):
    """Build the IBKR-only runtime market stack for shadow mode.

    Requires a locally authenticated IB Gateway/TWS session; the adapter is
    read-only here because the shadow broker cannot submit anything.
    """

    from .providers.ibkr_bars import IBAPIBarHook
    from .providers.ibkr_market import IBAPIBackendHook

    backend = NativeIBAPIBackend(
        IBKRConnectionConfig(
            host=settings.ibkr_host,
            port=settings.ibkr_port,
            client_id=settings.ibkr_client_id,
        )
    )
    backend.connect()
    quotes = IBAPIBackendHook(backend.client)
    bars = IBAPIBarHook(backend.client)
    backend.attach_market_data_hooks(market_data=quotes, bars=bars)
    return backend, IBKRMarketDataProvider(quotes), IBKRLiveFeatureProvider(bars)


async def _build_shadow_runtime(
    settings: Settings, *, variant: str
) -> tuple[ShadowRuntime, SQLiteOperationalStore, object]:
    _require_shadow_prerequisites(settings)
    backend, market_provider, features = _shadow_market_stack(settings)
    store = SQLiteOperationalStore(settings.state_db_path, settings.raw_data_dir)
    config = SecProviderConfig(
        user_agent=settings.sec_user_agent,
        requests_per_second=settings.sec_max_requests_per_second,
    )
    poller = SecProvider(config, document_persistence=store.persist_document)
    snapshot_factory = LiveEventSnapshotFactory(
        documents=FilingDocumentLoader(settings.raw_data_dir),
        market_data=market_provider,
        market_builder=SnapshotBuilder(market_provider),
        features=features,
    )
    runtime = build_shadow_runtime(
        settings,
        store=store,
        poller=poller,
        snapshot_factory=snapshot_factory,
        variant=variant,  # type: ignore[arg-type]
        warning_sink=default_warning_sink(settings),
    )
    return runtime, store, backend


async def _run_shadow_once(variant: str) -> dict[str, object]:
    settings = Settings()
    runtime, store, backend = await _build_shadow_runtime(settings, variant=variant)
    try:
        result = await runtime.run_once()
        return {
            "checked_at": result.checked_at.isoformat(),
            "ingested_filings": result.ingested_filings,
            "entry_outcomes": [
                {"event_id": outcome.event_id, "stage": outcome.stage}
                for outcome in result.entry_outcomes
            ],
            "critical_errors": list(result.critical_errors),
            "broker_submission_possible": False,
        }
    finally:
        await store.aclose()
        backend.disconnect()


async def _run_shadow(variant: str) -> None:
    settings = Settings()
    runtime, store, backend = await _build_shadow_runtime(settings, variant=variant)
    stop = asyncio.Event()
    try:
        await runtime.run(stop)
    finally:
        await store.aclose()
        backend.disconnect()


@app.command("run-shadow-once")
def run_shadow_once(
    variant: Annotated[str, typer.Option(help="quant-only, keyword or ai")] = "keyword",
) -> None:
    """Run exactly one shadow cycle; the broker used cannot submit orders."""

    _print_model(asyncio.run(_run_shadow_once(variant)))


@app.command("run-shadow")
def run_shadow(
    variant: Annotated[str, typer.Option(help="quant-only, keyword or ai")] = "keyword",
) -> None:
    """Run supervised shadow mode until interrupted; no order path is reachable."""

    try:
        asyncio.run(_run_shadow(variant))
    except KeyboardInterrupt:  # pragma: no cover - interactive shutdown
        typer.echo("shadow mode stopped")


# --------------------------------------------------------------- paper mode --


class _PaperPrerequisiteError(typer.BadParameter):
    """The order-capable runtime is unavailable until every gate is explicit."""


def _require_paper_prerequisites(settings: Settings) -> ResearchPromotionArtifact:
    _require_shadow_prerequisites(settings)
    missing: list[str] = []
    if settings.placeholder_credentials:
        missing.append("TRADING_PAPER_ACCOUNT_ID (an allowlisted DU<digits> account)")
    if not settings.promotion_artifact_path.is_file():
        missing.append(str(settings.promotion_artifact_path))
    if missing:
        raise _PaperPrerequisiteError(
            "Paper mode cannot start without: " + "; ".join(missing)
        )
    try:
        return load_promotion_artifact(settings.promotion_artifact_path)
    except (OSError, ValueError) as exc:
        raise _PaperPrerequisiteError(
            "the configured research promotion artifact is invalid"
        ) from exc


async def _build_paper_runtime(
    settings: Settings,
    *,
    variant: str,
    experiment_manifest: Path,
    dataset_manifest: Path,
    code_manifest: Path,
) -> tuple[
    PaperRuntime,
    SQLiteOperationalStore,
    NativeIBAPIBackend,
    SQLiteSecReconciliationLedger,
]:
    promotion = _require_paper_prerequisites(settings)
    backend, market_provider, features = _shadow_market_stack(settings)
    broker = IBKRBrokerAdapter(
        account_id=settings.paper_account_id,
        paper_account_allowlist=settings.allowed_paper_accounts,
        backend=backend,
        connection=IBKRConnectionConfig(
            host=settings.ibkr_host,
            port=settings.ibkr_port,
            client_id=settings.ibkr_client_id,
        ),
    )
    store = SQLiteOperationalStore(settings.state_db_path, settings.raw_data_dir)
    sec_ledger = SQLiteSecReconciliationLedger(
        settings.state_db_path.with_name("sec-reconciliation.sqlite")
    )
    config = SecProviderConfig(
        user_agent=settings.sec_user_agent,
        requests_per_second=settings.sec_max_requests_per_second,
    )
    poller = SecProvider(config, document_persistence=store.persist_document)
    market_builder = SnapshotBuilder(market_provider)
    snapshot_factory = LiveEventSnapshotFactory(
        documents=FilingDocumentLoader(settings.raw_data_dir),
        market_data=market_provider,
        market_builder=market_builder,
        features=features,
    )

    async def current_market(symbol: str, now: datetime):
        try:
            market_provider.subscribe(symbol)
            computed = features.build_symbol(symbol, as_of=now)
            return market_builder.build(symbol, computed)
        except Exception:
            return None

    runtime = build_paper_runtime(
        settings,
        store=store,
        poller=poller,
        snapshot_factory=snapshot_factory,
        broker=broker,
        market_provider=current_market,
        sec_reconciliation=sec_ledger,
        promotion_artifact=promotion,
        runtime_experiment_manifest_sha256=_sha256_file(experiment_manifest),
        runtime_dataset_manifest_sha256=_sha256_file(dataset_manifest),
        runtime_code_revision_sha256=_sha256_file(code_manifest),
        variant=variant,  # type: ignore[arg-type]
        warning_sink=default_warning_sink(settings),
    )
    return runtime, store, backend, sec_ledger


async def _run_paper(
    *,
    variant: str,
    experiment_manifest: Path,
    dataset_manifest: Path,
    code_manifest: Path,
) -> None:
    settings = Settings()
    runtime, store, backend, sec_ledger = await _build_paper_runtime(
        settings,
        variant=variant,
        experiment_manifest=experiment_manifest,
        dataset_manifest=dataset_manifest,
        code_manifest=code_manifest,
    )
    stop = asyncio.Event()
    try:
        await runtime.run(stop)
    finally:
        await store.aclose()
        sec_ledger.close()
        backend.disconnect()


@app.command("run-paper")
def run_paper(
    experiment_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    dataset_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    code_manifest: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    variant: Annotated[str, typer.Option(help="quant-only, keyword or ai")] = "quant-only",
) -> None:
    """Run supervised IBKR paper mode after promotion, preflight, and recovery."""

    try:
        asyncio.run(
            _run_paper(
                variant=variant,
                experiment_manifest=experiment_manifest,
                dataset_manifest=dataset_manifest,
                code_manifest=code_manifest,
            )
        )
    except KeyboardInterrupt:  # pragma: no cover - interactive shutdown
        typer.echo("paper mode stopped")


@app.command("session-report")
def session_report(
    session: Annotated[str, typer.Option(help="ISO session date")],
) -> None:
    """Write the hashed daily report for one session from durable state."""

    settings = Settings()
    target_date = _parse_iso_date(session, "session")

    async def build() -> Path:
        async with SQLiteOperationalStore(
            settings.state_db_path, settings.raw_data_dir
        ) as store:
            metrics = await build_daily_metrics(
                store,
                session_date=target_date,
                generated_at=datetime.now(UTC),
            )
            return write_daily_report(metrics, settings.report_dir)

    typer.echo(str(asyncio.run(build())))


@app.command("acceptance-from-reports")
def acceptance_from_reports_command() -> None:
    """Evaluate paper acceptance from the hashed session artifacts only."""

    settings = Settings()
    artifacts = load_daily_reports(settings.report_dir)
    result = acceptance_from_reports(artifacts)
    _print_model(result)
    if not result.passed:
        raise typer.Exit(code=2)


@app.command("reconcile-sec-daily")
def reconcile_sec_daily(
    session_date: Annotated[str | None, typer.Option(help="NYSE session date")] = None,
) -> None:
    """Compare locally captured filings with one official EDGAR daily index."""

    target = (
        _parse_iso_date(session_date, "session-date")
        if session_date is not None
        else NyseSessionCalendar().previous_session_date(datetime.now(UTC))
    )
    result = asyncio.run(_reconcile_sec_daily(target))
    _print_model(result)
    if not result.complete:
        raise typer.Exit(code=2)


@app.command("risk-halt-status")
def risk_halt_status() -> None:
    """Show the durable strategy kill-switch without changing it."""

    guard = SQLiteRiskHaltGuard(Settings().state_db_path)
    try:
        status = guard.status()
    finally:
        guard.close()
    _print_model(
        {
            "active": status.active,
            "reason": status.reason,
            "changed_at": status.changed_at,
        }
    )


@app.command("risk-halt-reset")
def risk_halt_reset(
    note: Annotated[str, typer.Option(help="Required manual-review audit note")],
) -> None:
    """Manually reset the latched paper-strategy halt with an audit note."""

    if not note.strip():
        raise typer.BadParameter("a non-empty review note is required")
    guard = SQLiteRiskHaltGuard(Settings().state_db_path)
    try:
        guard.manual_reset(note=note, at=datetime.now(UTC))
    finally:
        guard.close()
    typer.echo("Risk halt reset recorded")


@app.command("data-quality")
def data_quality(
    filings_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    market_symbols_file: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
    coverage_json: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
) -> None:
    """Build a coverage report; missing data never becomes a zero-return trade."""

    filings = TypeAdapter(list[FilingEvent]).validate_json(filings_json.read_text(encoding="utf-8"))
    symbols = (
        market_symbols_file.read_text(encoding="utf-8").splitlines() if market_symbols_file else []
    )
    coverage = (
        TypeAdapter(list[CoverageRecord]).validate_json(
            coverage_json.read_text(encoding="utf-8")
        )
        if coverage_json is not None
        else []
    )
    report = build_data_quality_report(
        filings,
        symbols,
        coverage,
        generated_at=datetime.now(UTC),
    )
    _print_model(report)


@app.command("benchmark-models")
def benchmark_models(
    labels_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    predictions_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Score model predictions against the manually labelled reference set."""

    labels = TypeAdapter(list[ReferenceLabel]).validate_json(
        labels_json.read_text(encoding="utf-8")
    )
    predictions = TypeAdapter(list[ScoredPrediction]).validate_json(
        predictions_json.read_text(encoding="utf-8")
    )
    results = benchmark_predictions(labels, predictions)
    selected = select_model(results)
    _print_model(
        {
            "results": [result.model_dump(mode="json") for result in results],
            "selected": selected.model_dump(mode="json") if selected else None,
            "hermes_actionable": selected is not None,
        }
    )


@app.command("daily-report")
def daily_report(
    metrics_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Write one immutable daily operations report."""

    settings = Settings()
    metrics = DailyMetrics.model_validate_json(metrics_json.read_text(encoding="utf-8"))
    target = write_daily_report(metrics, settings.report_dir)
    typer.echo(str(target))


@app.command("paper-acceptance")
def paper_acceptance(
    metrics_json: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Evaluate the 30-session/50-trade operational paper acceptance gate."""

    metrics = TypeAdapter(list[DailyMetrics]).validate_json(
        metrics_json.read_text(encoding="utf-8")
    )
    result = evaluate_paper_acceptance(metrics)
    _print_model(result)
    if not result.passed:
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
