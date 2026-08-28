"""Persistent, conservative reservation guard for paid model calls."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from .domain import EventSnapshot, NewsInsight
from .providers.insight import InsightProvider

Clock = Callable[[], datetime]


class SQLiteModelBudgetLedger:
    """Reserve estimated cost atomically before a provider call.

    Reservations are charged even if a call fails. This is intentionally
    conservative because providers may bill failed or timed-out requests.
    """

    def __init__(self, database_path: str | Path) -> None:
        path = Path(database_path)
        path.resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(path), check_same_thread=False)
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS model_cost_reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reserved_at TEXT NOT NULL,
                event_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                amount_eur REAL NOT NULL CHECK (amount_eur >= 0)
            )
            """
        )
        self._connection.commit()
        self._lock = threading.RLock()

    async def reserve(
        self,
        *,
        now: datetime,
        event_id: str,
        provider: str,
        amount_eur: float,
        daily_limit_eur: float,
        monthly_limit_eur: float,
    ) -> bool:
        return await asyncio.to_thread(
            self._reserve_sync,
            now,
            event_id,
            provider,
            amount_eur,
            daily_limit_eur,
            monthly_limit_eur,
        )

    def _reserve_sync(
        self,
        now: datetime,
        event_id: str,
        provider: str,
        amount_eur: float,
        daily_limit_eur: float,
        monthly_limit_eur: float,
    ) -> bool:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("budget timestamp must be timezone-aware")
        if amount_eur < 0 or daily_limit_eur < 0 or monthly_limit_eur < 0:
            raise ValueError("costs and limits cannot be negative")
        stamp = now.astimezone(UTC)
        day_start = stamp.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = day_start.replace(day=1)
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            daily = self._spent_since(day_start)
            monthly = self._spent_since(month_start)
            if daily + amount_eur > daily_limit_eur:
                return False
            if monthly + amount_eur > monthly_limit_eur:
                return False
            self._connection.execute(
                """
                INSERT INTO model_cost_reservations
                    (reserved_at, event_id, provider, amount_eur)
                VALUES (?, ?, ?, ?)
                """,
                (stamp.isoformat(), event_id, provider, amount_eur),
            )
            return True

    def _spent_since(self, start: datetime) -> float:
        row = self._connection.execute(
            """
            SELECT COALESCE(SUM(amount_eur), 0)
            FROM model_cost_reservations
            WHERE reserved_at >= ?
            """,
            (start.isoformat(),),
        ).fetchone()
        return float(row[0])

    def close(self) -> None:
        with self._lock:
            self._connection.close()


class BudgetedInsightProvider:
    def __init__(
        self,
        *,
        provider: InsightProvider,
        ledger: SQLiteModelBudgetLedger,
        provider_name: str,
        reserved_cost_eur: float = 0.02,
        daily_limit_eur: float = 1.0,
        monthly_limit_eur: float = 30.0,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        if reserved_cost_eur < 0:
            raise ValueError("reserved_cost_eur cannot be negative")
        self.provider = provider
        self.ledger = ledger
        self.provider_name = provider_name
        self.reserved_cost_eur = reserved_cost_eur
        self.daily_limit_eur = daily_limit_eur
        self.monthly_limit_eur = monthly_limit_eur
        self.clock = clock

    async def analyze(self, snapshot: EventSnapshot) -> NewsInsight:
        allowed = await self.ledger.reserve(
            now=self.clock(),
            event_id=snapshot.filing.event_id,
            provider=self.provider_name,
            amount_eur=self.reserved_cost_eur,
            daily_limit_eur=self.daily_limit_eur,
            monthly_limit_eur=self.monthly_limit_eur,
        )
        if not allowed:
            return NewsInsight.abstain(
                event_id=snapshot.filing.event_id,
                accession_number=snapshot.filing.accession_number,
                reason="model_budget_exhausted",
                model_provider=self.provider_name,
                model_name="budget-guard",
            )
        return await self.provider.analyze(snapshot)
