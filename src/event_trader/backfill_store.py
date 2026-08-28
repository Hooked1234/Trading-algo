"""Scalable SQLite state for the multi-year historical backfill."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

from .backfill import BackfillCheckpoint, CoverageRecord


class SQLiteBackfillStore:
    """Constant-size upserts instead of rewriting a growing JSON document."""

    def __init__(self, path: str | Path) -> None:
        target = Path(path)
        target.resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(target), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS backfill_checkpoints (
                quarter TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS backfill_coverage (
                record_id TEXT PRIMARY KEY,
                quarter TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                recorded_at_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_backfill_coverage_quarter
                ON backfill_coverage(quarter, accession_number);
            CREATE INDEX IF NOT EXISTS idx_backfill_coverage_status
                ON backfill_coverage(status);
            """
        )
        self._connection.commit()
        self._lock = threading.RLock()

    async def load_checkpoint(self, quarter: str) -> BackfillCheckpoint | None:
        return await asyncio.to_thread(self._load_checkpoint, quarter)

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        await asyncio.to_thread(self._save_checkpoint, checkpoint)

    async def save_coverage(self, record: CoverageRecord) -> None:
        await asyncio.to_thread(self._save_coverage, record)

    async def list_coverage(self) -> tuple[CoverageRecord, ...]:
        return await asyncio.to_thread(self._list_coverage)

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _load_checkpoint(self, quarter: str) -> BackfillCheckpoint | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT payload_json FROM backfill_checkpoints WHERE quarter = ?",
                (quarter,),
            ).fetchone()
        return BackfillCheckpoint.model_validate_json(row["payload_json"]) if row else None

    def _save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO backfill_checkpoints(quarter, payload_json, updated_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(quarter) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    checkpoint.quarter,
                    checkpoint.model_dump_json(),
                    checkpoint.updated_at.isoformat(),
                ),
            )

    def _save_coverage(self, record: CoverageRecord) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO backfill_coverage(
                    record_id, quarter, accession_number, status,
                    payload_json, recorded_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_id) DO UPDATE SET
                    quarter = excluded.quarter,
                    accession_number = excluded.accession_number,
                    status = excluded.status,
                    payload_json = excluded.payload_json,
                    recorded_at_utc = excluded.recorded_at_utc
                """,
                (
                    record.record_id,
                    record.quarter,
                    record.accession_number,
                    record.status.value,
                    record.model_dump_json(),
                    record.recorded_at.isoformat(),
                ),
            )

    def _list_coverage(self) -> tuple[CoverageRecord, ...]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM backfill_coverage
                ORDER BY record_id
                """
            ).fetchall()
        return tuple(CoverageRecord.model_validate_json(row["payload_json"]) for row in rows)


__all__ = ["SQLiteBackfillStore"]
