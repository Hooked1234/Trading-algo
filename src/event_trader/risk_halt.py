"""Latched risk stop requiring an explicit, audited manual reset."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol


class RiskHaltGuard(Protocol):
    def is_halted(self) -> bool: ...

    def trip(self, *, reason: str, at: datetime) -> None: ...


@dataclass(frozen=True, slots=True)
class RiskHaltState:
    active: bool
    reason: str | None
    changed_at: datetime | None


class InMemoryRiskHaltGuard:
    def __init__(self) -> None:
        self.reason: str | None = None

    def is_halted(self) -> bool:
        return self.reason is not None

    def trip(self, *, reason: str, at: datetime) -> None:
        del at
        self.reason = self.reason or reason

    def manual_reset(self) -> None:
        self.reason = None


class SQLiteRiskHaltGuard:
    """Durable single-strategy kill switch with reset audit history."""

    def __init__(self, path: str | Path) -> None:
        target = Path(path)
        target.resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(target), check_same_thread=False)
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS risk_halt_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                active INTEGER NOT NULL CHECK (active IN (0, 1)),
                reason TEXT,
                changed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS risk_halt_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL CHECK (action IN ('trip', 'manual_reset')),
                reason TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );
            """
        )
        self._connection.commit()
        self._lock = threading.RLock()

    def is_halted(self) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT active FROM risk_halt_state WHERE singleton = 1"
            ).fetchone()
        return row is not None and bool(row[0])

    def status(self) -> RiskHaltState:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT active, reason, changed_at
                FROM risk_halt_state
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return RiskHaltState(active=False, reason=None, changed_at=None)
        return RiskHaltState(
            active=bool(row[0]),
            reason=str(row[1]) if row[1] is not None else None,
            changed_at=datetime.fromisoformat(str(row[2])),
        )

    def trip(self, *, reason: str, at: datetime) -> None:
        stamp = _validated(at)
        if not reason.strip():
            raise ValueError("risk halt reason must not be empty")
        with self._lock, self._connection:
            current = self._connection.execute(
                "SELECT active FROM risk_halt_state WHERE singleton = 1"
            ).fetchone()
            if current is not None and current[0] == 1:
                return
            self._connection.execute(
                """
                INSERT INTO risk_halt_state(singleton, active, reason, changed_at)
                VALUES (1, 1, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    active = 1, reason = excluded.reason, changed_at = excluded.changed_at
                """,
                (reason, stamp),
            )
            self._connection.execute(
                "INSERT INTO risk_halt_audit(action, reason, occurred_at) VALUES ('trip', ?, ?)",
                (reason, stamp),
            )

    def manual_reset(self, *, note: str, at: datetime) -> None:
        stamp = _validated(at)
        if not note.strip():
            raise ValueError("manual reset requires an audit note")
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO risk_halt_state(singleton, active, reason, changed_at)
                VALUES (1, 0, NULL, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    active = 0, reason = NULL, changed_at = excluded.changed_at
                """,
                (stamp,),
            )
            self._connection.execute(
                """
                INSERT INTO risk_halt_audit(action, reason, occurred_at)
                VALUES ('manual_reset', ?, ?)
                """,
                (note, stamp),
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()


def _validated(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("risk halt timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()
