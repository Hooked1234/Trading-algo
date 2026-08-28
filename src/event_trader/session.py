"""Restart-safe SEC outbox consumer for shadow and paper sessions.

Every claimed event ends durably: either as a recorded pipeline outcome or as a
typed, retryable failure.  The analysis, the final outcome and the outbox
completion are written in one transaction, so a crash can never leave a filing
that was charged for a model call without its outcome — and a retry reuses the
stored analysis instead of paying again.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Protocol

from .calendar import NyseSessionCalendar
from .domain import EventSnapshot, FilingEvent, PortfolioState
from .orchestrator import EventTradingOrchestrator, PipelineOutcome
from .storage import SQLiteOperationalStore, StorageIntegrityError


class SnapshotFactory(Protocol):
    async def build(self, filing: FilingEvent, *, as_of: datetime) -> EventSnapshot | None: ...


PortfolioProvider = Callable[[], Awaitable[PortfolioState]]

_RETRY_DELAY = timedelta(minutes=1)
_EVALUATION_GRACE = timedelta(minutes=15)


class TradingSession:
    def __init__(
        self,
        *,
        store: SQLiteOperationalStore,
        snapshot_factory: SnapshotFactory,
        portfolio_provider: PortfolioProvider,
        orchestrator: EventTradingOrchestrator,
        calendar: NyseSessionCalendar | None = None,
    ) -> None:
        self.store = store
        self.snapshot_factory = snapshot_factory
        self.portfolio_provider = portfolio_provider
        self.orchestrator = orchestrator
        self.calendar = calendar or NyseSessionCalendar()

    @property
    def strategy_version(self) -> str:
        return self.orchestrator.strategy.version

    async def process_ready(
        self, *, now: datetime, limit: int = 100
    ) -> tuple[PipelineOutcome, ...]:
        records = await self.store.claim_outbox(limit=limit, lease_seconds=60)
        outcomes: list[PipelineOutcome] = []
        for record in records:
            try:
                outcome = await self._process_record(record, now=now)
            except StorageIntegrityError as exc:
                # A conflicting outcome for one event means the pipeline is no
                # longer reproducible.  Stop the retry loop and keep the event.
                await self.store.record_critical_event(
                    "PIPELINE_OUTCOME_CONFLICT",
                    detail=f"{record.event_id}: {exc}",
                    occurred_at=now,
                )
                await self.store.mark_outbox_published(
                    record.id, record.lease_token, published_at=now
                )
                continue
            except Exception as exc:
                await self.store.mark_outbox_failed(
                    record.id,
                    record.lease_token,
                    f"processing_error:{exc.__class__.__name__}",
                    retry_at=now + _RETRY_DELAY,
                )
                continue
            if outcome is not None:
                outcomes.append(outcome)
        return tuple(outcomes)

    async def _process_record(self, record, *, now: datetime) -> PipelineOutcome | None:
        filing = await self.store.get_filing(record.event_id)
        if filing is None:
            await self.store.mark_outbox_failed(
                record.id,
                record.lease_token,
                "filing_missing",
                retry_at=now + _RETRY_DELAY,
            )
            return None

        evaluation_time = self.calendar.next_evaluation_time(filing.first_seen_at)
        if evaluation_time is None:
            await self._complete(
                record,
                stage="skipped",
                reason="OUTSIDE_ENTRY_SCHEDULE",
                now=now,
            )
            return None
        if now < evaluation_time:
            await self.store.mark_outbox_failed(
                record.id,
                record.lease_token,
                "evaluation_not_due",
                retry_at=evaluation_time,
            )
            return None
        if now > evaluation_time + _EVALUATION_GRACE:
            await self._complete(
                record,
                stage="skipped",
                reason="EVALUATION_WINDOW_EXPIRED",
                now=now,
            )
            return None

        snapshot = await self.snapshot_factory.build(filing, as_of=now)
        if snapshot is None:
            await self.store.mark_outbox_failed(
                record.id,
                record.lease_token,
                "snapshot_unavailable",
                retry_at=min(now + _RETRY_DELAY, evaluation_time + _EVALUATION_GRACE),
            )
            return None

        portfolio = await self.portfolio_provider()
        outcome = await self.orchestrator.process(snapshot, portfolio, now=now)
        await self.store.complete_event(
            event_id=record.event_id,
            strategy_version=outcome.strategy_version or self.strategy_version,
            stage=outcome.stage,
            outcome_json=outcome.model_dump_json(),
            insight=outcome.insight,
            analysis_key=outcome.analysis_key,
            outbox_id=record.id,
            lease_token=record.lease_token,
            published_at=now,
        )
        return outcome

    async def _complete(
        self,
        record,
        *,
        stage: str,
        reason: str,
        now: datetime,
    ) -> None:
        """Record a terminal non-trading outcome and close the outbox record."""

        outcome = PipelineOutcome(
            stage=stage,
            event_id=record.event_id,
            strategy_version=self.strategy_version,
            reasons=(reason,),
        )
        await self.store.complete_event(
            event_id=record.event_id,
            strategy_version=self.strategy_version,
            stage=stage,
            outcome_json=outcome.model_dump_json(),
            outbox_id=record.id,
            lease_token=record.lease_token,
            published_at=now,
        )


__all__ = ["TradingSession"]
