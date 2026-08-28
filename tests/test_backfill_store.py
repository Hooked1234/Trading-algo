from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from event_trader.backfill import BackfillCheckpoint, CoverageRecord, CoverageStatus
from event_trader.backfill_store import SQLiteBackfillStore


async def test_sqlite_backfill_store_upserts_without_rewriting_global_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "backfill.sqlite"
    store = SQLiteBackfillStore(path)
    checkpoint = BackfillCheckpoint(
        quarter="2026-Q2",
        range_start=date(2026, 4, 1),
        range_end=date(2026, 6, 30),
        processed_accessions=("0000320193-26-000001",),
        updated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    coverage = CoverageRecord(
        record_id="coverage-1",
        quarter="2026-Q2",
        accession_number="0000320193-26-000001",
        status=CoverageStatus.MISSING_SYMBOL,
        detail="missing",
        recorded_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    try:
        await store.save_checkpoint(checkpoint)
        await store.save_coverage(coverage)
        await store.save_coverage(coverage.model_copy(update={"detail": "still missing"}))
    finally:
        await store.aclose()

    reopened = SQLiteBackfillStore(path)
    try:
        assert await reopened.load_checkpoint("2026-Q2") == checkpoint
        records = await reopened.list_coverage()
        assert len(records) == 1
        assert records[0].detail == "still missing"
    finally:
        await reopened.aclose()
