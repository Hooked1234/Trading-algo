from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from event_trader.analysis import AnalysisIdentity
from event_trader.composition import build_paper_runtime, build_shadow_runtime
from event_trader.config import Settings
from event_trader.domain import Direction
from event_trader.promotion import ResearchPromotionArtifact
from event_trader.providers.sec import SecCursor, SecPollResult
from event_trader.reconciliation import SQLiteSecReconciliationLedger
from event_trader.reporting import load_daily_reports
from event_trader.service import DaemonAlreadyRunning, SQLiteSingletonLease
from event_trader.shadow import NonSubmittingShadowBroker, ShadowSubmissionRefused
from event_trader.storage import SQLiteOperationalStore
from event_trader.strategy import QuantOnlyContinuationStrategy


class StaticInsightProvider:
    """Stand-in for the quarantined model adapter in shadow mode."""

    def __init__(self, insight) -> None:
        self.insight = insight
        self.calls = 0

    @property
    def analysis_identity(self) -> AnalysisIdentity:
        return AnalysisIdentity(
            model_id=self.insight.model_id,
            prompt_version=self.insight.prompt_version,
            schema_version=self.insight.schema_version,
        )

    async def analyze(self, _snapshot):
        self.calls += 1
        return self.insight


class SilentPoller:
    """SEC poller that returns nothing; ingestion is exercised elsewhere."""

    def __init__(self) -> None:
        self.calls = 0

    async def poll(self, cursor: SecCursor | None = None) -> SecPollResult:
        self.calls += 1
        return SecPollResult(events=(), cursor=cursor or SecCursor())


class StaticSnapshotFactory:
    def __init__(self, snapshot) -> None:
        self.snapshot = snapshot
        self.calls = 0

    async def build(self, filing, *, as_of):
        del filing, as_of
        self.calls += 1
        return self.snapshot


class SlowSnapshotFactory(StaticSnapshotFactory):
    """Stand-in for a blocking model call inside the entry task."""

    def __init__(self, snapshot, delay: float) -> None:
        super().__init__(snapshot)
        self.delay = delay

    async def build(self, filing, *, as_of):
        await asyncio.sleep(self.delay)
        return await super().build(filing, as_of=as_of)


def _settings(tmp_path) -> Settings:
    return Settings(
        state_db_path=tmp_path / "state.sqlite",
        raw_data_dir=tmp_path / "raw",
        report_dir=tmp_path / "reports",
    )


def _store(tmp_path, decision_time) -> SQLiteOperationalStore:
    return SQLiteOperationalStore(
        tmp_path / "state.sqlite",
        tmp_path / "raw",
        clock=lambda: decision_time,
    )


def _paper_promotion(decision_time) -> ResearchPromotionArtifact:
    return ResearchPromotionArtifact.create(
        experiment_id="paper-runtime-test",
        strategy_version=QuantOnlyContinuationStrategy.version,
        enabled_directions=(Direction.LONG, Direction.SHORT),
        ai_influences_orders=False,
        research_gate_passed=True,
        experiment_manifest_sha256="a" * 64,
        dataset_manifest_sha256="b" * 64,
        code_revision_sha256="c" * 64,
        research_result_sha256="d" * 64,
        research_evidence_sha256="e" * 64,
        created_at=decision_time,
    )


@pytest.mark.asyncio
async def test_paper_runtime_wires_promotion_preflight_and_recovery(
    tmp_path, snapshot, decision_time
) -> None:
    settings = Settings(
        paper_account_id="DU123456",
        allowed_paper_accounts=("DU123456",),
        state_db_path=tmp_path / "state.sqlite",
        raw_data_dir=tmp_path / "raw",
        report_dir=tmp_path / "reports",
    )
    broker = SimpleNamespace(account_id="DU123456")
    sec = SQLiteSecReconciliationLedger(tmp_path / "sec.sqlite")

    async def market_provider(_symbol, _now):
        return snapshot.market.model_copy(update={"market_data_live": True})

    async with _store(tmp_path, decision_time) as store:
        runtime = build_paper_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=StaticSnapshotFactory(snapshot),
            broker=broker,  # type: ignore[arg-type]
            market_provider=market_provider,
            sec_reconciliation=sec,
            promotion_artifact=_paper_promotion(decision_time),
            runtime_experiment_manifest_sha256="a" * 64,
            runtime_dataset_manifest_sha256="b" * 64,
            runtime_code_revision_sha256="c" * 64,
            clock=lambda: decision_time,
            use_lease=False,
        )

        assert runtime.orchestrator.execution_enabled
        assert runtime.orchestrator.execution_service.has_pre_submit_guard
        assert runtime.daemon.startup_check is runtime.recovery

    sec.close()


@pytest.mark.asyncio
async def test_paper_runtime_refuses_a_non_authoritative_client_id(
    tmp_path, snapshot, decision_time
) -> None:
    """A wrong client id is a configuration fault, not a readiness surprise.

    Readiness would refuse this session as well, but only after connecting and
    with a message about order scope.  The composition root owns the paper-only
    invariants, so it names the actual cause before any transport is touched.
    """

    settings = Settings(
        paper_account_id="DU123456",
        allowed_paper_accounts=("DU123456",),
        state_db_path=tmp_path / "state.sqlite",
        raw_data_dir=tmp_path / "raw",
        report_dir=tmp_path / "reports",
        ibkr_client_id=71,
    )
    broker = SimpleNamespace(account_id="DU123456")
    sec = SQLiteSecReconciliationLedger(tmp_path / "sec.sqlite")

    async def market_provider(_symbol, _now):
        return snapshot.market.model_copy(update={"market_data_live": True})

    async with _store(tmp_path, decision_time) as store:
        with pytest.raises(ValueError, match="client id 0"):
            build_paper_runtime(
                settings,
                store=store,
                poller=SilentPoller(),
                snapshot_factory=StaticSnapshotFactory(snapshot),
                broker=broker,  # type: ignore[arg-type]
                market_provider=market_provider,
                sec_reconciliation=sec,
                promotion_artifact=_paper_promotion(decision_time),
                runtime_experiment_manifest_sha256="a" * 64,
                runtime_dataset_manifest_sha256="b" * 64,
                runtime_code_revision_sha256="c" * 64,
                clock=lambda: decision_time,
                use_lease=False,
            )

    sec.close()


@pytest.mark.asyncio
async def test_shadow_cycle_records_an_outcome_without_touching_the_broker(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    settings = _settings(tmp_path)
    async with _store(tmp_path, decision_time) as store:
        assert await store.save_filing_event(snapshot.filing)
        provider = StaticInsightProvider(long_insight)
        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=StaticSnapshotFactory(snapshot),
            variant="ai",
            insight_provider=provider,
            clock=lambda: decision_time,
            use_lease=False,
            write_report=False,
        )

        result = await runtime.daemon.run_cycle(now=decision_time, poll_sec=False)

        assert len(result.entry_outcomes) == 1
        outcome = result.entry_outcomes[0]
        assert outcome.stage == "shadow_order"
        assert outcome.order_intent is not None
        assert outcome.order_intent.submission_mode == "shadow"
        stored = await store.get_pipeline_outcome(
            snapshot.filing.event_id, outcome.strategy_version
        )
        assert stored is not None
        assert json.loads(stored)["stage"] == "shadow_order"
        assert await store.count_outbox(published=False) == 0
        assert provider.calls == 1
        stored_insights = await store.list_insights_since(decision_time - timedelta(days=1))
        assert len(stored_insights) == 1
        with pytest.raises(ShadowSubmissionRefused):
            runtime.broker.submit(outcome.order_intent)


@pytest.mark.asyncio
async def test_quant_only_shadow_stores_no_insight(tmp_path, snapshot, decision_time) -> None:
    settings = _settings(tmp_path)
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=StaticSnapshotFactory(snapshot),
            variant="quant-only",
            clock=lambda: decision_time,
            use_lease=False,
            write_report=False,
        )

        await runtime.daemon.run_cycle(now=decision_time, poll_sec=False)

        assert await store.list_insights_since(decision_time - timedelta(days=1)) == ()


@pytest.mark.asyncio
async def test_every_outbox_event_ends_terminally(tmp_path, snapshot, decision_time) -> None:
    settings = _settings(tmp_path)
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)

        class NoSnapshot:
            async def build(self, filing, *, as_of):
                return None

        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=NoSnapshot(),
            clock=lambda: decision_time,
            use_lease=False,
            write_report=False,
        )

        result = await runtime.daemon.run_cycle(now=decision_time, poll_sec=False)

        # Unavailable market data is a typed, retryable failure — never a trade.
        assert result.entry_outcomes == ()
        assert await store.count_outbox(published=False) == 1
        assert (
            await store.get_pipeline_outcome(
                snapshot.filing.event_id, runtime.session.strategy_version
            )
            is None
        )


@pytest.mark.asyncio
async def test_a_second_daemon_is_refused_by_the_singleton_lease(
    tmp_path, snapshot, decision_time
) -> None:
    settings = _settings(tmp_path)
    async with _store(tmp_path, decision_time) as store:
        first = SQLiteSingletonLease(store, ttl=timedelta(seconds=30))
        assert await first.acquire() is True

        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=StaticSnapshotFactory(snapshot),
            clock=lambda: decision_time,
            write_report=False,
        )
        with pytest.raises(DaemonAlreadyRunning):
            await runtime.daemon.run(asyncio.Event())

        await first.release()
        assert await store.lease_holder("trading.daemon") is None


@pytest.mark.asyncio
async def test_a_blocking_entry_task_does_not_delay_the_exit_tick(
    tmp_path, snapshot, decision_time
) -> None:
    settings = _settings(tmp_path)
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=SlowSnapshotFactory(snapshot, delay=0.30),
            clock=lambda: decision_time,
            use_lease=False,
            write_report=False,
            session_interval=timedelta(seconds=1),
            exit_interval=timedelta(milliseconds=10),
            sec_poll_interval=timedelta(seconds=30),
        )
        ticks = 0
        original = runtime.daemon.exit_monitor.run_cycle

        async def counting(**kwargs):
            nonlocal ticks
            ticks += 1
            return await original(**kwargs)

        runtime.daemon.exit_monitor.run_cycle = counting  # type: ignore[method-assign]

        stop = asyncio.Event()
        task = asyncio.create_task(runtime.daemon.run(stop))
        await asyncio.sleep(0.35)
        stop.set()
        await task

        # The exit loop kept its own cadence while one entry was blocked.
        assert ticks >= 5


@pytest.mark.asyncio
async def test_a_failing_exit_task_blocks_entries_and_records_a_warning(
    tmp_path, snapshot, decision_time
) -> None:
    settings = _settings(tmp_path)
    warnings: list[str] = []

    async def warning_sink(message: str) -> None:
        warnings.append(message)

    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        factory = StaticSnapshotFactory(snapshot)
        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=factory,
            warning_sink=warning_sink,
            clock=lambda: decision_time,
            use_lease=False,
            write_report=False,
        )

        class BrokenMonitor:
            async def run_cycle(self, **_kwargs):
                raise RuntimeError("gateway lost")

        runtime.daemon.exit_monitor = BrokenMonitor()  # type: ignore[assignment]
        result = await runtime.daemon.run_cycle(now=decision_time, poll_sec=False)

        assert factory.calls == 0
        assert result.entry_outcomes == ()
        assert "ENTRIES_BLOCKED_BY_RUNTIME_GUARD" in result.critical_errors
        assert any(code.startswith("EXIT_MONITOR_ERROR") for code in result.critical_errors)
        assert warnings
        durable = await store.list_critical_events()
        assert any(event["code"].startswith("EXIT_MONITOR_ERROR") for event in durable)


@pytest.mark.asyncio
async def test_the_session_report_is_written_from_durable_state(
    tmp_path, snapshot, decision_time
) -> None:
    settings = _settings(tmp_path)
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        runtime = build_shadow_runtime(
            settings,
            store=store,
            poller=SilentPoller(),
            snapshot_factory=StaticSnapshotFactory(snapshot),
            clock=lambda: decision_time,
            use_lease=False,
            exit_interval=timedelta(milliseconds=10),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(runtime.daemon.run(stop))
        await asyncio.sleep(0.08)
        stop.set()
        await task

        reports = load_daily_reports(settings.report_dir)

        assert len(reports) == 1
        report = reports[0]
        report.verify()
        assert report.metrics.filings_seen == 1
        assert report.metrics.submitted_orders == 0
        assert report.metrics.shadow_orders >= 0
        assert report.markdown_sha256


def test_the_shadow_broker_never_impersonates_a_paper_account() -> None:
    with pytest.raises(ValueError, match="impersonate"):
        NonSubmittingShadowBroker(account_id="DU123456")
