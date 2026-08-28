"""Daily SEC feed/index completeness reconciliation."""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import threading
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

from .calendar import NEW_YORK
from .domain import FilingEvent, FrozenModel
from .providers.sec_daily import DailyIndexSource


class SecReconciliationResult(FrozenModel):
    reconciled_at: datetime
    expected_count: int
    stored_count: int
    missing_accessions: tuple[str, ...]
    unexpected_accessions: tuple[str, ...]
    duplicate_accessions: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.missing_accessions and not self.duplicate_accessions


def reconcile_sec_accessions(
    *,
    daily_index_accessions: Iterable[str],
    stored_filings: Iterable[FilingEvent],
    reconciled_at: datetime,
) -> SecReconciliationResult:
    expected = set(daily_index_accessions)
    stored_records = list(stored_filings)
    stored_counts = Counter(record.accession_number for record in stored_records)
    stored = set(stored_counts)
    return SecReconciliationResult(
        reconciled_at=reconciled_at,
        expected_count=len(expected),
        stored_count=len(stored),
        missing_accessions=tuple(sorted(expected - stored)),
        unexpected_accessions=tuple(sorted(stored - expected)),
        duplicate_accessions=tuple(
            sorted(accession for accession, count in stored_counts.items() if count > 1)
        ),
    )


class SQLiteSecReconciliationLedger:
    """Durable latest result plus immutable audit history for each SEC index date."""

    def __init__(self, path: str | Path) -> None:
        target = Path(path)
        target.resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(target), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sec_reconciliation_state (
                session_date TEXT PRIMARY KEY,
                complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
                reconciled_at_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sec_reconciliation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_date TEXT NOT NULL,
                complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
                reconciled_at_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                UNIQUE(session_date, payload_sha256)
            );
            """
        )
        self._connection.commit()
        self._lock = threading.RLock()

    def save(self, *, session_date: date, result: SecReconciliationResult) -> bool:
        payload = result.model_dump_json()
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self._lock, self._connection:
            inserted = self._connection.execute(
                """
                INSERT OR IGNORE INTO sec_reconciliation_history(
                    session_date, complete, reconciled_at_utc, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    session_date.isoformat(),
                    int(result.complete),
                    _utc(result.reconciled_at).isoformat(),
                    payload,
                    digest,
                ),
            ).rowcount
            self._connection.execute(
                """
                INSERT INTO sec_reconciliation_state(
                    session_date, complete, reconciled_at_utc, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(session_date) DO UPDATE SET
                    complete = excluded.complete,
                    reconciled_at_utc = excluded.reconciled_at_utc,
                    payload_json = excluded.payload_json,
                    payload_sha256 = excluded.payload_sha256
                """,
                (
                    session_date.isoformat(),
                    int(result.complete),
                    _utc(result.reconciled_at).isoformat(),
                    payload,
                    digest,
                ),
            )
        return bool(inserted)

    def get(self, session_date: date) -> SecReconciliationResult | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT payload_json FROM sec_reconciliation_state WHERE session_date = ?",
                (session_date.isoformat(),),
            ).fetchone()
        return SecReconciliationResult.model_validate_json(row["payload_json"]) if row else None

    def latest(self) -> tuple[date, SecReconciliationResult] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT session_date, payload_json
                FROM sec_reconciliation_state
                ORDER BY session_date DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return (
            date.fromisoformat(row["session_date"]),
            SecReconciliationResult.model_validate_json(row["payload_json"]),
        )

    def orders_blocked(self, *, required_through: date | None = None) -> bool:
        if required_through is not None:
            result = self.get(required_through)
            return result is None or not result.complete
        latest = self.latest()
        return latest is None or not latest[1].complete

    def close(self) -> None:
        with self._lock:
            self._connection.close()


class FilingInventory(Protocol):
    async def list_filings_for_session(
        self, session_date: date
    ) -> tuple[FilingEvent, ...]: ...


class DailySecReconciler:
    """Fetch, compare, and durably record one official EDGAR index date."""

    def __init__(
        self,
        *,
        source: DailyIndexSource,
        inventory: FilingInventory,
        ledger: SQLiteSecReconciliationLedger,
    ) -> None:
        self.source = source
        self.inventory = inventory
        self.ledger = ledger

    async def run(
        self, *, session_date: date, reconciled_at: datetime
    ) -> SecReconciliationResult:
        expected, inventory = await asyncio.gather(
            self.source.accessions(session_date),
            self.inventory.list_filings_for_session(session_date),
        )
        stored = tuple(
            filing
            for filing in inventory
            if filing.accepted_at.astimezone(NEW_YORK).date() == session_date
        )
        result = reconcile_sec_accessions(
            daily_index_accessions=expected,
            stored_filings=stored,
            reconciled_at=reconciled_at,
        )
        await asyncio.to_thread(self.ledger.save, session_date=session_date, result=result)
        return result


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("reconciliation timestamp must be timezone-aware")
    return value.astimezone(UTC)


__all__ = [
    "DailySecReconciler",
    "SQLiteSecReconciliationLedger",
    "SecReconciliationResult",
    "reconcile_sec_accessions",
]
