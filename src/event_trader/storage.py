"""Durable operational state, raw SEC documents, and transactional outbox.

SQLite is intentionally used as a single-node operational store for the MVP.
Every public method that performs blocking I/O has an async wrapper so the
event loop is never blocked by filesystem or database work.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from event_trader.analysis import AnalysisKey
from event_trader.calendar import NEW_YORK
from event_trader.domain import (
    DocumentRef,
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    FilingEvent,
    InsightStatus,
    NewsInsight,
    OrderIntent,
    RiskDecision,
    Signal,
)

DEFAULT_OUTBOX_TOPIC = "filing.ingested"
SCHEMA_VERSION = 1
"""Operational schema version recorded in SQLite's ``PRAGMA user_version``.

Version 0 is a database written before schema versioning existed (Gate B).
Version 1 adds the per-fill ledger and the explicit reprice lineage.
"""
_OPEN_EXECUTION_STATUSES = (
    ExecutionStatus.PENDING,
    ExecutionStatus.SUBMITTED,
    ExecutionStatus.PARTIALLY_FILLED,
)
_ALLOWED_EXECUTION_TRANSITIONS = {
    ExecutionStatus.PENDING: frozenset(
        {
            ExecutionStatus.PENDING,
            ExecutionStatus.SUBMITTED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
    ),
    ExecutionStatus.SUBMITTED: frozenset(
        {
            ExecutionStatus.SUBMITTED,
            ExecutionStatus.PARTIALLY_FILLED,
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
    ),
    ExecutionStatus.PARTIALLY_FILLED: frozenset(
        {
            ExecutionStatus.PARTIALLY_FILLED,
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
        }
    ),
    ExecutionStatus.FILLED: frozenset({ExecutionStatus.FILLED}),
    ExecutionStatus.CANCELLED: frozenset({ExecutionStatus.CANCELLED, ExecutionStatus.FILLED}),
    ExecutionStatus.REJECTED: frozenset({ExecutionStatus.REJECTED}),
}


class StorageError(RuntimeError):
    """Base class for operational-store failures."""


class StorageIntegrityError(StorageError):
    """Raised when persisted bytes do not match their content address."""


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    id: int
    event_id: str
    topic: str
    payload: dict[str, Any]
    attempts: int
    created_at: datetime
    available_at: datetime
    lease_token: str
    locked_until: datetime


def _default_clock() -> datetime:
    return datetime.now(UTC)


class SQLiteOperationalStore:
    """Content-addressed raw storage plus SQLite event/outbox state."""

    def __init__(
        self,
        database_path: str | Path,
        raw_root: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be greater than 0")
        self.database_path = str(database_path)
        self.raw_root = Path(raw_root).resolve()
        self._clock = clock or _default_clock
        self._lock = threading.RLock()
        self._closed = False

        if self.database_path != ":memory:":
            Path(self.database_path).resolve().parent.mkdir(parents=True, exist_ok=True)
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.database_path,
            timeout=busy_timeout_ms / 1_000,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms:d}")
        if self.database_path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = NORMAL")
        try:
            self._initialize_schema()
            self._migrate_schema()
        except Exception:
            # A store that cannot vouch for its schema must not stay open and
            # hold the database file of a run that is about to fail anyway.
            self._connection.close()
            self._closed = True
            raise

    async def __aenter__(self) -> SQLiteOperationalStore:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    async def persist_document(
        self,
        *,
        url: str,
        kind: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> DocumentRef:
        """Persist bytes under their SHA-256 and return a validated reference."""

        return await asyncio.to_thread(
            self._persist_document_sync,
            url,
            kind,
            bytes(content),
            _utc(retrieved_at),
        )

    async def save_filing_event(
        self,
        event: FilingEvent,
        *,
        outbox_topic: str = DEFAULT_OUTBOX_TOPIC,
    ) -> bool:
        """Upsert one filing and enqueue it atomically.

        Returns ``True`` only for the first insertion.  A later complete replay
        may enrich an incomplete row without duplicating the outbox message.
        """

        if not outbox_topic.strip():
            raise ValueError("outbox_topic cannot be empty")
        normalized = _normalize_filing(event)
        return await asyncio.to_thread(
            self._save_filing_event_sync,
            normalized,
            outbox_topic,
        )

    async def save_event(
        self,
        event: FilingEvent,
        *,
        outbox_topic: str = DEFAULT_OUTBOX_TOPIC,
    ) -> bool:
        """Compatibility alias for event-oriented callers."""

        return await self.save_filing_event(event, outbox_topic=outbox_topic)

    async def save_poll(
        self,
        events: Sequence[FilingEvent],
        *,
        provider: str,
        cursor: str,
        outbox_topic: str = DEFAULT_OUTBOX_TOPIC,
    ) -> int:
        """Commit a parsed feed page and its cursor in one DB transaction."""

        if not provider.strip():
            raise ValueError("provider cannot be empty")
        if not outbox_topic.strip():
            raise ValueError("outbox_topic cannot be empty")
        normalized = tuple(_normalize_filing(event) for event in events)
        return await asyncio.to_thread(
            self._save_poll_sync,
            normalized,
            provider,
            cursor,
            outbox_topic,
        )

    async def get_filing(self, event_or_accession: str) -> FilingEvent | None:
        return await asyncio.to_thread(self._get_filing_sync, event_or_accession)

    async def list_filings(self, *, limit: int = 10_000) -> tuple[FilingEvent, ...]:
        _validate_limit(limit)
        return await asyncio.to_thread(self._list_filings_sync, limit)

    async def list_filings_for_session(self, session_date: date) -> tuple[FilingEvent, ...]:
        """Return the complete NY-local calendar day without a global row cap."""

        start = datetime.combine(session_date, time.min, tzinfo=NEW_YORK).astimezone(UTC)
        end = datetime.combine(
            session_date + timedelta(days=1), time.min, tzinfo=NEW_YORK
        ).astimezone(UTC)
        return await asyncio.to_thread(self._list_filings_between_sync, start, end)

    async def has_accession(self, accession_number: str) -> bool:
        return await asyncio.to_thread(self._has_accession_sync, accession_number)

    async def count_filings(self) -> int:
        return await asyncio.to_thread(self._scalar_count, "filings")

    async def save_signal(self, signal: Signal) -> bool:
        """Persist an immutable strategy decision before downstream risk checks."""

        return await asyncio.to_thread(self._save_signal_sync, _normalize_signal(signal))

    async def get_signal(self, signal_id: str) -> Signal | None:
        return await asyncio.to_thread(self._get_signal_sync, signal_id)

    async def list_signals(self, *, limit: int = 1_000) -> tuple[Signal, ...]:
        _validate_limit(limit)
        return await asyncio.to_thread(self._list_signals_sync, limit)

    async def list_signals_since(self, since: datetime) -> tuple[Signal, ...]:
        return await asyncio.to_thread(self._list_signals_since_sync, _utc(since))

    async def save_risk_decision(self, decision: RiskDecision) -> bool:
        """Persist one immutable fail-closed risk decision per signal."""

        normalized = _normalize_risk_decision(decision)
        return await asyncio.to_thread(self._save_risk_decision_sync, normalized)

    async def get_risk_decision(self, signal_id: str) -> RiskDecision | None:
        return await asyncio.to_thread(self._get_risk_decision_sync, signal_id)

    async def list_risk_decisions(self, *, limit: int = 1_000) -> tuple[RiskDecision, ...]:
        _validate_limit(limit)
        return await asyncio.to_thread(self._list_risk_decisions_sync, limit)

    async def save_order_intent(self, intent: OrderIntent) -> bool:
        """Persist an immutable order intent before any broker submission."""

        return await asyncio.to_thread(
            self._save_order_intent_sync,
            _normalize_order_intent(intent),
        )

    async def get_order_intent(self, order_id: str) -> OrderIntent | None:
        return await asyncio.to_thread(self._get_order_intent_sync, order_id)

    async def get_order_intent_by_key(self, idempotency_key: str) -> OrderIntent | None:
        return await asyncio.to_thread(
            self._get_order_intent_by_key_sync,
            idempotency_key,
        )

    async def list_order_intents(self, *, limit: int = 1_000) -> tuple[OrderIntent, ...]:
        _validate_limit(limit)
        return await asyncio.to_thread(self._list_order_intents_sync, limit)

    async def list_order_intents_since(self, since: datetime) -> tuple[OrderIntent, ...]:
        return await asyncio.to_thread(self._list_order_intents_since_sync, _utc(since))

    async def list_orders_for_reconciliation(
        self,
        *,
        limit: int = 1_000,
    ) -> tuple[OrderIntent, ...]:
        """Return orders with no terminal execution state after a restart."""

        _validate_limit(limit)
        return await asyncio.to_thread(self._list_orders_for_reconciliation_sync, limit)

    async def save_execution_report(self, report: ExecutionReport) -> bool:
        """Persist a monotonic broker state transition idempotently."""

        normalized = _normalize_execution_report(report)
        return await asyncio.to_thread(self._save_execution_report_sync, normalized)

    async def get_execution_report(self, order_id: str) -> ExecutionReport | None:
        return await asyncio.to_thread(self._get_execution_report_sync, order_id)

    async def list_execution_reports(
        self,
        *,
        limit: int = 1_000,
    ) -> tuple[ExecutionReport, ...]:
        _validate_limit(limit)
        return await asyncio.to_thread(self._list_execution_reports_sync, limit)

    async def list_execution_reports_since(self, since: datetime) -> tuple[ExecutionReport, ...]:
        return await asyncio.to_thread(self._list_execution_reports_since_sync, _utc(since))

    async def save_execution_fill(self, fill: ExecutionFill) -> bool:
        """Persist one broker fill idempotently under its own execution id."""

        return await asyncio.to_thread(self._save_execution_fill_sync, fill)

    async def list_execution_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        """Return every stored fill of one order in broker time order."""

        return await asyncio.to_thread(self._list_execution_fills_sync, order_id)

    async def list_execution_history(
        self,
        order_id: str,
        *,
        limit: int = 1_000,
    ) -> tuple[ExecutionReport, ...]:
        _validate_limit(limit)
        return await asyncio.to_thread(self._list_execution_history_sync, order_id, limit)

    async def set_cursor(self, provider: str, cursor: str) -> None:
        if not provider.strip():
            raise ValueError("provider cannot be empty")
        await asyncio.to_thread(self._set_cursor_sync, provider, cursor)

    async def get_cursor(self, provider: str) -> str | None:
        if not provider.strip():
            raise ValueError("provider cannot be empty")
        return await asyncio.to_thread(self._get_cursor_sync, provider)

    async def claim_outbox(
        self,
        *,
        limit: int = 100,
        lease_seconds: float = 60.0,
    ) -> tuple[OutboxRecord, ...]:
        """Lease ready outbox rows so concurrent publishers cannot duplicate them."""

        if limit <= 0:
            raise ValueError("limit must be greater than 0")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")
        return await asyncio.to_thread(self._claim_outbox_sync, limit, lease_seconds)

    async def mark_outbox_published(
        self,
        record_id: int,
        lease_token: str,
        *,
        published_at: datetime | None = None,
    ) -> None:
        timestamp = _utc(published_at or self._now())
        await asyncio.to_thread(
            self._mark_outbox_published_sync,
            record_id,
            lease_token,
            timestamp,
        )

    async def mark_outbox_failed(
        self,
        record_id: int,
        lease_token: str,
        error: str,
        *,
        retry_at: datetime | None = None,
    ) -> None:
        available_at = _utc(retry_at or self._now())
        await asyncio.to_thread(
            self._mark_outbox_failed_sync,
            record_id,
            lease_token,
            error,
            available_at,
        )

    async def save_insight(self, insight: NewsInsight, key: AnalysisKey) -> bool:
        """Store one immutable analysis; a repeat with the same content is a no-op."""

        return await asyncio.to_thread(self._save_insight_sync, insight, key)

    async def get_insight(self, analysis_key: str) -> NewsInsight | None:
        return await asyncio.to_thread(self._get_insight_sync, analysis_key)

    async def list_insights_since(self, since: datetime) -> tuple[NewsInsight, ...]:
        return await asyncio.to_thread(self._list_insights_since_sync, since)

    async def complete_event(
        self,
        *,
        event_id: str,
        strategy_version: str,
        stage: str,
        outcome_json: str,
        insight: NewsInsight | None = None,
        analysis_key: AnalysisKey | None = None,
        outbox_id: int | None = None,
        lease_token: str | None = None,
        published_at: datetime,
    ) -> None:
        """Persist analysis, final outcome and outbox completion atomically.

        Either all three land or none of them do, so a crash can never leave an
        event that was charged for a model call without its recorded outcome.
        """

        await asyncio.to_thread(
            self._complete_event_sync,
            event_id,
            strategy_version,
            stage,
            outcome_json,
            insight,
            analysis_key,
            outbox_id,
            lease_token,
            published_at,
        )

    async def get_pipeline_outcome(self, event_id: str, strategy_version: str) -> str | None:
        return await asyncio.to_thread(self._get_pipeline_outcome_sync, event_id, strategy_version)

    async def count_pipeline_outcomes(self) -> int:
        return await asyncio.to_thread(self._scalar_count, "pipeline_outcomes")

    async def acquire_lease(
        self,
        name: str,
        *,
        holder: str,
        ttl: timedelta,
        now: datetime | None = None,
    ) -> bool:
        """Take or renew a named singleton lease; a live foreign lease refuses."""

        return await asyncio.to_thread(self._acquire_lease_sync, name, holder, ttl, now)

    async def release_lease(self, name: str, *, holder: str) -> bool:
        return await asyncio.to_thread(self._release_lease_sync, name, holder)

    async def lease_holder(self, name: str) -> str | None:
        return await asyncio.to_thread(self._lease_holder_sync, name)

    async def record_critical_event(
        self,
        code: str,
        *,
        detail: str | None = None,
        occurred_at: datetime | None = None,
    ) -> int:
        return await asyncio.to_thread(self._record_critical_event_sync, code, detail, occurred_at)

    async def list_critical_events(
        self, *, since: datetime | None = None
    ) -> tuple[dict[str, str], ...]:
        return await asyncio.to_thread(self._list_critical_events_sync, since)

    async def list_pipeline_outcomes_since(
        self, since: datetime
    ) -> tuple[tuple[str, str, str], ...]:
        """Return ``(event_id, stage, payload_json)`` for outcomes since ``since``."""

        return await asyncio.to_thread(self._list_pipeline_outcomes_since_sync, since)

    async def record_heartbeat(self, now: datetime) -> None:
        """Record that the runtime was alive at ``now`` on its NYSE session date."""

        await asyncio.to_thread(self._record_heartbeat_sync, now)

    async def get_heartbeat(self, session_date: date) -> tuple[datetime, datetime, int] | None:
        return await asyncio.to_thread(self._get_heartbeat_sync, session_date)

    async def count_outbox(self, *, published: bool | None = None) -> int:
        return await asyncio.to_thread(self._count_outbox_sync, published)

    def _initialize_schema(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS filings (
            event_id TEXT PRIMARY KEY,
            accession_number TEXT NOT NULL UNIQUE,
            cik TEXT NOT NULL,
            form TEXT NOT NULL CHECK (form IN ('8-K', '8-K/A')),
            accepted_at_utc TEXT NOT NULL,
            first_seen_at_utc TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
            payload_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS raw_documents (
            sha256 TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            kind TEXT NOT NULL,
            local_path TEXT,
            byte_length INTEGER,
            retrieved_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS filing_documents (
            event_id TEXT NOT NULL REFERENCES filings(event_id) ON DELETE CASCADE,
            sha256 TEXT NOT NULL REFERENCES raw_documents(sha256),
            ordinal INTEGER NOT NULL,
            url TEXT NOT NULL,
            kind TEXT NOT NULL,
            local_path TEXT,
            PRIMARY KEY (event_id, sha256, url)
        );

        CREATE TABLE IF NOT EXISTS provider_cursors (
            provider TEXT PRIMARY KEY,
            cursor TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS signals (
            signal_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES filings(event_id),
            decided_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_signals_event
        ON signals (event_id, decided_at_utc, signal_id);

        CREATE TABLE IF NOT EXISTS risk_decisions (
            signal_id TEXT PRIMARY KEY REFERENCES signals(signal_id),
            approved INTEGER NOT NULL CHECK (approved IN (0, 1)),
            decided_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS order_intents (
            order_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            signal_id TEXT NOT NULL REFERENCES signals(signal_id),
            created_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            persisted_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_order_intents_signal
        ON order_intents (signal_id, created_at_utc, order_id);

        CREATE TABLE IF NOT EXISTS execution_reports (
            order_id TEXT PRIMARY KEY REFERENCES order_intents(order_id),
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN (
                    'pending', 'submitted', 'partially_filled',
                    'filled', 'cancelled', 'rejected'
                )
            ),
            filled_quantity INTEGER NOT NULL CHECK (filled_quantity >= 0),
            occurred_at_utc TEXT NOT NULL,
            broker_order_id TEXT UNIQUE,
            payload_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_execution_reports_status
        ON execution_reports (status, occurred_at_utc, order_id);

        CREATE TABLE IF NOT EXISTS execution_report_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL REFERENCES order_intents(order_id),
            payload_sha256 TEXT NOT NULL,
            occurred_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            persisted_at_utc TEXT NOT NULL,
            UNIQUE (order_id, payload_sha256)
        );

        CREATE INDEX IF NOT EXISTS idx_execution_history_order
        ON execution_report_history (order_id, occurred_at_utc, id);

        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL REFERENCES filings(event_id) ON DELETE CASCADE,
            topic TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            available_at_utc TEXT NOT NULL,
            locked_until_utc TEXT,
            lease_token TEXT,
            published_at_utc TEXT,
            last_error TEXT,
            UNIQUE (event_id, topic)
        );

        CREATE INDEX IF NOT EXISTS idx_outbox_ready
        ON outbox (published_at_utc, available_at_utc, locked_until_utc, id);

        CREATE TABLE IF NOT EXISTS insights (
            analysis_key TEXT PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES filings(event_id),
            accession_number TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('actionable', 'abstain')),
            model_id TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            document_sha256 TEXT NOT NULL,
            input_sha256 TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_insights_event
        ON insights (event_id, created_at_utc, analysis_key);

        CREATE TABLE IF NOT EXISTS pipeline_outcomes (
            event_id TEXT NOT NULL REFERENCES filings(event_id),
            strategy_version TEXT NOT NULL,
            stage TEXT NOT NULL,
            analysis_key TEXT REFERENCES insights(analysis_key),
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            recorded_at_utc TEXT NOT NULL,
            PRIMARY KEY (event_id, strategy_version)
        );

        CREATE INDEX IF NOT EXISTS idx_pipeline_outcomes_time
        ON pipeline_outcomes (recorded_at_utc, event_id);

        CREATE TABLE IF NOT EXISTS runtime_leases (
            name TEXT PRIMARY KEY,
            holder TEXT NOT NULL,
            acquired_at_utc TEXT NOT NULL,
            expires_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS critical_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            detail TEXT,
            occurred_at_utc TEXT NOT NULL,
            recorded_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_critical_events_time
        ON critical_events (occurred_at_utc, id);

        CREATE TABLE IF NOT EXISTS runtime_heartbeats (
            session_date TEXT PRIMARY KEY,
            first_seen_at_utc TEXT NOT NULL,
            last_seen_at_utc TEXT NOT NULL,
            ticks INTEGER NOT NULL CHECK (ticks > 0)
        );
        """
        with self._lock:
            self._ensure_open()
            self._connection.executescript(schema)
            self._connection.commit()

    def _migrate_schema(self) -> None:
        """Apply versioned schema steps and record the result in ``user_version``.

        A database written by a newer build is never opened.  An unknown schema
        cannot be reasoned about, and silently continuing on it would put the
        one state this system must not lose — order and fill truth — at risk.
        """

        with self._lock:
            self._ensure_open()
            row = self._connection.execute("PRAGMA user_version").fetchone()
            current = int(row[0])
            if current > SCHEMA_VERSION:
                raise StorageError(
                    f"database schema version {current} is newer than the supported "
                    f"version {SCHEMA_VERSION}"
                )
            if current == SCHEMA_VERSION:
                return
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                for version in range(current + 1, SCHEMA_VERSION + 1):
                    _SCHEMA_MIGRATIONS[version](self._connection)
                self._connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION:d}")
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

    def schema_version(self) -> int:
        """Return the schema version recorded in the open database."""

        with self._lock:
            self._ensure_open()
            return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def _persist_document_sync(
        self,
        url: str,
        kind: str,
        content: bytes,
        retrieved_at: datetime,
    ) -> DocumentRef:
        if not url.strip() or not kind.strip():
            raise ValueError("document URL and kind are required")
        digest = hashlib.sha256(content).hexdigest()
        target = self.raw_root / "sha256" / digest[:2] / f"{digest}.bin"

        with self._lock:
            self._ensure_open()
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                existing_digest = _hash_file(target)
                if existing_digest != digest:
                    raise StorageIntegrityError(f"content-addressed file is corrupt: {target}")
            else:
                temp_path: Path | None = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="wb",
                        dir=target.parent,
                        prefix=f".{digest}.",
                        suffix=".tmp",
                        delete=False,
                    ) as temporary:
                        temporary.write(content)
                        temporary.flush()
                        os.fsync(temporary.fileno())
                        temp_path = Path(temporary.name)
                    os.replace(temp_path, target)
                    temp_path = None
                finally:
                    if temp_path is not None:
                        temp_path.unlink(missing_ok=True)

            self._connection.execute(
                """
                INSERT INTO raw_documents (
                    sha256, url, kind, local_path, byte_length, retrieved_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(sha256) DO UPDATE SET
                    local_path = excluded.local_path,
                    byte_length = excluded.byte_length
                """,
                (
                    digest,
                    url,
                    kind,
                    str(target),
                    len(content),
                    _iso(retrieved_at),
                ),
            )
            self._connection.commit()

        return DocumentRef(url=url, kind=kind, sha256=digest, local_path=str(target))

    def _save_filing_event_sync(self, event: FilingEvent, topic: str) -> bool:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                inserted = self._upsert_filing_locked(event, topic)
                self._connection.commit()
                return inserted
            except Exception:
                self._connection.rollback()
                raise

    def _save_poll_sync(
        self,
        events: Sequence[FilingEvent],
        provider: str,
        cursor: str,
        topic: str,
    ) -> int:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                inserted = sum(self._upsert_filing_locked(event, topic) for event in events)
                self._upsert_cursor_locked(provider, cursor, _utc(self._now()))
                self._connection.commit()
                return inserted
            except Exception:
                self._connection.rollback()
                raise

    def _upsert_filing_locked(self, incoming: FilingEvent, topic: str) -> bool:
        existing_row = self._connection.execute(
            "SELECT payload_json FROM filings WHERE event_id = ? OR accession_number = ?",
            (incoming.event_id, incoming.accession_number),
        ).fetchone()
        inserted = existing_row is None
        event = incoming
        if existing_row is not None:
            existing = FilingEvent.model_validate_json(existing_row["payload_json"])
            identity_changed = (
                existing.event_id != incoming.event_id
                or existing.accession_number != incoming.accession_number
            )
            if identity_changed:
                raise StorageIntegrityError("event ID/accession collision")
            event = _merge_filing(existing, incoming)

        now = _utc(self._now())
        payload_json = event.model_dump_json()
        values = (
            event.event_id,
            event.accession_number,
            event.cik,
            event.form,
            _iso(event.accepted_at),
            _iso(event.first_seen_at),
            _iso(event.retrieved_at),
            int(event.complete),
            payload_json,
            _iso(now),
            _iso(now),
        )
        self._connection.execute(
            """
            INSERT INTO filings (
                event_id, accession_number, cik, form, accepted_at_utc,
                first_seen_at_utc, retrieved_at_utc, complete, payload_json,
                created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                cik = excluded.cik,
                form = excluded.form,
                accepted_at_utc = excluded.accepted_at_utc,
                first_seen_at_utc = excluded.first_seen_at_utc,
                retrieved_at_utc = excluded.retrieved_at_utc,
                complete = excluded.complete,
                payload_json = excluded.payload_json,
                updated_at_utc = excluded.updated_at_utc
            """,
            values,
        )
        self._replace_document_links_locked(event)

        if event.complete:
            self._connection.execute(
                """
                INSERT INTO outbox (
                    event_id, topic, payload_json, created_at_utc, available_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(event_id, topic) DO UPDATE SET
                    payload_json = CASE
                        WHEN outbox.published_at_utc IS NULL THEN excluded.payload_json
                        ELSE outbox.payload_json
                    END
                """,
                (event.event_id, topic, payload_json, _iso(now), _iso(now)),
            )
        return inserted

    def _replace_document_links_locked(self, event: FilingEvent) -> None:
        self._connection.execute(
            "DELETE FROM filing_documents WHERE event_id = ?",
            (event.event_id,),
        )
        for ordinal, document in enumerate(event.documents):
            self._connection.execute(
                """
                INSERT INTO raw_documents (
                    sha256, url, kind, local_path, byte_length, retrieved_at_utc
                ) VALUES (?, ?, ?, ?, NULL, ?)
                ON CONFLICT(sha256) DO UPDATE SET
                    local_path = COALESCE(raw_documents.local_path, excluded.local_path)
                """,
                (
                    document.sha256,
                    document.url,
                    document.kind,
                    document.local_path,
                    _iso(event.retrieved_at),
                ),
            )
            self._connection.execute(
                """
                INSERT INTO filing_documents (
                    event_id, sha256, ordinal, url, kind, local_path
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    document.sha256,
                    ordinal,
                    document.url,
                    document.kind,
                    document.local_path,
                ),
            )

    def _get_filing_sync(self, event_or_accession: str) -> FilingEvent | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM filings WHERE event_id = ? OR accession_number = ?",
                (event_or_accession, event_or_accession),
            ).fetchone()
        return FilingEvent.model_validate_json(row["payload_json"]) if row else None

    def _list_filings_sync(self, limit: int) -> tuple[FilingEvent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM filings
                ORDER BY accepted_at_utc, event_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(FilingEvent.model_validate_json(row["payload_json"]) for row in rows)

    def _list_filings_between_sync(self, start: datetime, end: datetime) -> tuple[FilingEvent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM filings
                WHERE accepted_at_utc >= ? AND accepted_at_utc < ?
                ORDER BY accepted_at_utc, event_id
                """,
                (start.isoformat(), end.isoformat()),
            ).fetchall()
        return tuple(FilingEvent.model_validate_json(row["payload_json"]) for row in rows)

    def _has_accession_sync(self, accession_number: str) -> bool:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT 1 FROM filings WHERE accession_number = ?",
                (accession_number,),
            ).fetchone()
            return row is not None

    def _save_signal_sync(self, signal: Signal) -> bool:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT payload_json FROM signals WHERE signal_id = ?",
                    (signal.signal_id,),
                ).fetchone()
                if row is not None:
                    existing = Signal.model_validate_json(row["payload_json"])
                    if existing != signal:
                        raise StorageIntegrityError(f"signal id {signal.signal_id!r} was reused")
                    self._connection.commit()
                    return False
                now = self._now()
                self._connection.execute(
                    """
                    INSERT INTO signals (
                        signal_id, event_id, decided_at_utc, payload_json, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        signal.signal_id,
                        signal.event_id,
                        _iso(signal.decided_at),
                        signal.model_dump_json(),
                        _iso(now),
                    ),
                )
                self._connection.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError(
                    "signal references a filing that has not been persisted"
                ) from exc
            except Exception:
                self._connection.rollback()
                raise

    def _get_signal_sync(self, signal_id: str) -> Signal | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM signals WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
        return Signal.model_validate_json(row["payload_json"]) if row else None

    def _list_signals_sync(self, limit: int) -> tuple[Signal, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM (
                    SELECT payload_json, decided_at_utc, signal_id
                    FROM signals
                    ORDER BY decided_at_utc DESC, signal_id DESC
                    LIMIT ?
                )
                ORDER BY decided_at_utc, signal_id
                """,
                (limit,),
            ).fetchall()
        return tuple(Signal.model_validate_json(row["payload_json"]) for row in rows)

    def _list_signals_since_sync(self, since: datetime) -> tuple[Signal, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM signals
                WHERE decided_at_utc >= ?
                ORDER BY decided_at_utc, signal_id
                """,
                (_iso(since),),
            ).fetchall()
        return tuple(Signal.model_validate_json(row["payload_json"]) for row in rows)

    def _save_risk_decision_sync(self, decision: RiskDecision) -> bool:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT payload_json FROM risk_decisions WHERE signal_id = ?",
                    (decision.signal_id,),
                ).fetchone()
                if row is not None:
                    existing = RiskDecision.model_validate_json(row["payload_json"])
                    if existing != decision:
                        raise StorageIntegrityError(
                            f"risk decision for signal {decision.signal_id!r} changed"
                        )
                    self._connection.commit()
                    return False
                self._connection.execute(
                    """
                    INSERT INTO risk_decisions (
                        signal_id, approved, decided_at_utc, payload_json, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        decision.signal_id,
                        int(decision.approved),
                        _iso(decision.decided_at),
                        decision.model_dump_json(),
                        _iso(self._now()),
                    ),
                )
                self._connection.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError(
                    "risk decision references a signal that has not been persisted"
                ) from exc
            except Exception:
                self._connection.rollback()
                raise

    def _get_risk_decision_sync(self, signal_id: str) -> RiskDecision | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM risk_decisions WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
        return RiskDecision.model_validate_json(row["payload_json"]) if row else None

    def _list_risk_decisions_sync(self, limit: int) -> tuple[RiskDecision, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM risk_decisions
                ORDER BY decided_at_utc, signal_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(RiskDecision.model_validate_json(row["payload_json"]) for row in rows)

    def _save_order_intent_sync(self, intent: OrderIntent) -> bool:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                rows = self._connection.execute(
                    """
                    SELECT order_id, idempotency_key, payload_json
                    FROM order_intents
                    WHERE order_id = ? OR idempotency_key = ?
                    """,
                    (intent.order_id, intent.idempotency_key),
                ).fetchall()
                if rows:
                    if len(rows) == 1:
                        existing = OrderIntent.model_validate_json(rows[0]["payload_json"])
                        if existing == intent:
                            self._connection.commit()
                            return False
                    raise StorageIntegrityError(
                        "order id or idempotency key was reused for different content"
                    )
                self._connection.execute(
                    """
                    INSERT INTO order_intents (
                        order_id, idempotency_key, signal_id, created_at_utc,
                        payload_json, persisted_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent.order_id,
                        intent.idempotency_key,
                        intent.signal_id,
                        _iso(intent.created_at),
                        intent.model_dump_json(),
                        _iso(self._now()),
                    ),
                )
                self._connection.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError(
                    "order intent is not unique or references an unknown signal"
                ) from exc
            except Exception:
                self._connection.rollback()
                raise

    def _get_order_intent_sync(self, order_id: str) -> OrderIntent | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM order_intents WHERE order_id = ?",
                (order_id,),
            ).fetchone()
        return OrderIntent.model_validate_json(row["payload_json"]) if row else None

    def _get_order_intent_by_key_sync(self, idempotency_key: str) -> OrderIntent | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM order_intents WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return OrderIntent.model_validate_json(row["payload_json"]) if row else None

    def _list_order_intents_sync(self, limit: int) -> tuple[OrderIntent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM (
                    SELECT payload_json, created_at_utc, order_id
                    FROM order_intents
                    ORDER BY created_at_utc DESC, order_id DESC
                    LIMIT ?
                )
                ORDER BY created_at_utc, order_id
                """,
                (limit,),
            ).fetchall()
        return tuple(OrderIntent.model_validate_json(row["payload_json"]) for row in rows)

    def _list_order_intents_since_sync(self, since: datetime) -> tuple[OrderIntent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM order_intents
                WHERE created_at_utc >= ?
                ORDER BY created_at_utc, order_id
                """,
                (_iso(since),),
            ).fetchall()
        return tuple(OrderIntent.model_validate_json(row["payload_json"]) for row in rows)

    def _list_orders_for_reconciliation_sync(self, limit: int) -> tuple[OrderIntent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT intent.payload_json
                FROM order_intents AS intent
                LEFT JOIN execution_reports AS report ON report.order_id = intent.order_id
                WHERE (report.status IS NULL OR report.status IN (?, ?, ?))
                  AND COALESCE(json_extract(intent.payload_json, '$.submission_mode'), 'shadow')
                    = 'paper'
                ORDER BY intent.created_at_utc, intent.order_id
                LIMIT ?
                """,
                (*[status.value for status in _OPEN_EXECUTION_STATUSES], limit),
            ).fetchall()
        return tuple(OrderIntent.model_validate_json(row["payload_json"]) for row in rows)

    def _save_execution_report_sync(self, incoming: ExecutionReport) -> bool:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                intent_row = self._connection.execute(
                    "SELECT payload_json FROM order_intents WHERE order_id = ?",
                    (incoming.order_id,),
                ).fetchone()
                if intent_row is None:
                    raise StorageIntegrityError(
                        "execution report arrived before its order intent was persisted"
                    )
                intent = OrderIntent.model_validate_json(intent_row["payload_json"])
                current_row = self._connection.execute(
                    "SELECT payload_json FROM execution_reports WHERE order_id = ?",
                    (incoming.order_id,),
                ).fetchone()
                current = (
                    ExecutionReport.model_validate_json(current_row["payload_json"])
                    if current_row
                    else None
                )
                canonical, changed = _canonical_execution_transition(intent, current, incoming)
                if not changed:
                    self._connection.commit()
                    return False
                if canonical.broker_order_id is not None:
                    collision = self._connection.execute(
                        """
                        SELECT order_id
                        FROM execution_reports
                        WHERE broker_order_id = ? AND order_id != ?
                        """,
                        (canonical.broker_order_id, canonical.order_id),
                    ).fetchone()
                    if collision is not None:
                        raise StorageIntegrityError("broker order id belongs to another order")

                payload_json = canonical.model_dump_json()
                now = self._now()
                self._connection.execute(
                    """
                    INSERT INTO execution_reports (
                        order_id, idempotency_key, status, filled_quantity,
                        occurred_at_utc, broker_order_id, payload_json, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        idempotency_key = excluded.idempotency_key,
                        status = excluded.status,
                        filled_quantity = excluded.filled_quantity,
                        occurred_at_utc = excluded.occurred_at_utc,
                        broker_order_id = excluded.broker_order_id,
                        payload_json = excluded.payload_json,
                        updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        canonical.order_id,
                        canonical.idempotency_key,
                        canonical.status.value,
                        canonical.filled_quantity,
                        _iso(canonical.occurred_at),
                        canonical.broker_order_id,
                        payload_json,
                        _iso(now),
                    ),
                )
                payload_sha256 = hashlib.sha256(payload_json.encode()).hexdigest()
                self._connection.execute(
                    """
                    INSERT INTO execution_report_history (
                        order_id, payload_sha256, occurred_at_utc,
                        payload_json, persisted_at_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        canonical.order_id,
                        payload_sha256,
                        _iso(canonical.occurred_at),
                        payload_json,
                        _iso(now),
                    ),
                )
                self._connection.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError("execution report violates stored identity") from exc
            except Exception:
                self._connection.rollback()
                raise

    def _save_execution_fill_sync(self, fill: ExecutionFill) -> bool:
        """Insert a fill once; allow exactly one later commission finalization.

        A broker resends executions after a reconnect, so an identical replay
        must be a no-op rather than an error.  The commission arrives in its
        own later callback and is the only value a stored fill may still gain.
        Any other disagreement means two different facts share one execution
        id, and that is never resolved by overwriting.
        """

        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                intent_row = self._connection.execute(
                    "SELECT payload_json FROM order_intents WHERE order_id = ?",
                    (fill.order_id,),
                ).fetchone()
                if intent_row is None:
                    raise StorageIntegrityError(
                        "execution fill arrived before its order intent was persisted"
                    )
                intent = OrderIntent.model_validate_json(intent_row["payload_json"])
                _validate_execution_fill(intent, fill)

                existing_row = self._connection.execute(
                    "SELECT payload_json FROM execution_fills WHERE execution_id = ?",
                    (fill.execution_id,),
                ).fetchone()
                if existing_row is not None:
                    existing = ExecutionFill.model_validate_json(existing_row["payload_json"])
                    if existing == fill:
                        self._connection.commit()
                        return False
                    if not _is_commission_finalization(existing, fill):
                        raise StorageIntegrityError("execution id was reused for a different fill")
                    self._connection.execute(
                        """
                        UPDATE execution_fills
                        SET commission = ?, commission_final = 1,
                            payload_json = ?, persisted_at_utc = ?
                        WHERE execution_id = ?
                        """,
                        (
                            str(fill.commission),
                            fill.model_dump_json(),
                            _iso(self._now()),
                            fill.execution_id,
                        ),
                    )
                    self._connection.commit()
                    return True

                self._connection.execute(
                    """
                    INSERT INTO execution_fills (
                        execution_id, order_id, broker_order_id, symbol, side,
                        quantity, price, cumulative_quantity, commission,
                        commission_final, occurred_at_utc, payload_json, persisted_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fill.execution_id,
                        fill.order_id,
                        fill.broker_order_id,
                        fill.symbol,
                        fill.side.value,
                        fill.quantity,
                        str(fill.price),
                        fill.cumulative_quantity,
                        str(fill.commission),
                        int(fill.commission_final),
                        _iso(fill.occurred_at),
                        fill.model_dump_json(),
                        _iso(self._now()),
                    ),
                )
                self._connection.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError("execution fill violates stored identity") from exc
            except Exception:
                self._connection.rollback()
                raise

    def _list_execution_fills_sync(self, order_id: str) -> tuple[ExecutionFill, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM execution_fills
                WHERE order_id = ?
                ORDER BY occurred_at_utc, execution_id
                """,
                (order_id,),
            ).fetchall()
        return tuple(ExecutionFill.model_validate_json(row["payload_json"]) for row in rows)

    def _get_execution_report_sync(self, order_id: str) -> ExecutionReport | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM execution_reports WHERE order_id = ?",
                (order_id,),
            ).fetchone()
        return ExecutionReport.model_validate_json(row["payload_json"]) if row else None

    def _list_execution_reports_sync(self, limit: int) -> tuple[ExecutionReport, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM (
                    SELECT payload_json, occurred_at_utc, order_id
                    FROM execution_reports
                    ORDER BY occurred_at_utc DESC, order_id DESC
                    LIMIT ?
                )
                ORDER BY occurred_at_utc, order_id
                """,
                (limit,),
            ).fetchall()
        return tuple(ExecutionReport.model_validate_json(row["payload_json"]) for row in rows)

    def _list_execution_reports_since_sync(self, since: datetime) -> tuple[ExecutionReport, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM execution_reports
                WHERE occurred_at_utc >= ?
                ORDER BY occurred_at_utc, order_id
                """,
                (_iso(since),),
            ).fetchall()
        return tuple(ExecutionReport.model_validate_json(row["payload_json"]) for row in rows)

    def _list_execution_history_sync(
        self,
        order_id: str,
        limit: int,
    ) -> tuple[ExecutionReport, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json
                FROM execution_report_history
                WHERE order_id = ?
                ORDER BY occurred_at_utc, id
                LIMIT ?
                """,
                (order_id, limit),
            ).fetchall()
        return tuple(ExecutionReport.model_validate_json(row["payload_json"]) for row in rows)

    def _scalar_count(self, table: str) -> int:
        queries = {
            "filings": "SELECT COUNT(*) AS count FROM filings",
            "outbox": "SELECT COUNT(*) AS count FROM outbox",
            "raw_documents": "SELECT COUNT(*) AS count FROM raw_documents",
        }
        if table not in queries:
            raise ValueError("unsupported table")
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(queries[table]).fetchone()
            return int(row["count"])

    def _set_cursor_sync(self, provider: str, cursor: str) -> None:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                self._upsert_cursor_locked(provider, cursor, _utc(self._now()))
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

    def _upsert_cursor_locked(self, provider: str, cursor: str, now: datetime) -> None:
        self._connection.execute(
            """
            INSERT INTO provider_cursors (provider, cursor, updated_at_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(provider) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at_utc = excluded.updated_at_utc
            """,
            (provider, cursor, _iso(now)),
        )

    def _get_cursor_sync(self, provider: str) -> str | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT cursor FROM provider_cursors WHERE provider = ?",
                (provider,),
            ).fetchone()
            return str(row["cursor"]) if row else None

    def _claim_outbox_sync(self, limit: int, lease_seconds: float) -> tuple[OutboxRecord, ...]:
        now = _utc(self._now())
        locked_until = now + timedelta(seconds=lease_seconds)
        lease_token = uuid.uuid4().hex
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                rows = self._connection.execute(
                    """
                    SELECT id
                    FROM outbox
                    WHERE published_at_utc IS NULL
                      AND available_at_utc <= ?
                      AND (locked_until_utc IS NULL OR locked_until_utc <= ?)
                    ORDER BY id
                    LIMIT ?
                    """,
                    (_iso(now), _iso(now), limit),
                ).fetchall()
                ids = [int(row["id"]) for row in rows]
                if not ids:
                    self._connection.commit()
                    return ()
                self._connection.executemany(
                    """
                    UPDATE outbox
                    SET lease_token = ?, locked_until_utc = ?, attempts = attempts + 1
                    WHERE id = ?
                    """,
                    ((lease_token, _iso(locked_until), record_id) for record_id in ids),
                )
                claimed = self._connection.execute(
                    """
                    SELECT id, event_id, topic, payload_json, attempts,
                           created_at_utc, available_at_utc
                    FROM outbox
                    WHERE lease_token = ?
                    ORDER BY id
                    """,
                    (lease_token,),
                ).fetchall()
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

        return tuple(
            OutboxRecord(
                id=int(row["id"]),
                event_id=str(row["event_id"]),
                topic=str(row["topic"]),
                payload=json.loads(row["payload_json"]),
                attempts=int(row["attempts"]),
                created_at=_parse_iso(row["created_at_utc"]),
                available_at=_parse_iso(row["available_at_utc"]),
                lease_token=lease_token,
                locked_until=locked_until,
            )
            for row in claimed
        )

    def _mark_outbox_published_sync(
        self,
        record_id: int,
        lease_token: str,
        published_at: datetime,
    ) -> None:
        with self._lock:
            self._ensure_open()
            cursor = self._connection.execute(
                """
                UPDATE outbox
                SET published_at_utc = ?, locked_until_utc = NULL,
                    lease_token = NULL, last_error = NULL
                WHERE id = ? AND lease_token = ? AND published_at_utc IS NULL
                """,
                (_iso(published_at), record_id, lease_token),
            )
            self._connection.commit()
            if cursor.rowcount != 1:
                raise StorageError("outbox lease is missing, expired, or already published")

    def _mark_outbox_failed_sync(
        self,
        record_id: int,
        lease_token: str,
        error: str,
        available_at: datetime,
    ) -> None:
        with self._lock:
            self._ensure_open()
            cursor = self._connection.execute(
                """
                UPDATE outbox
                SET available_at_utc = ?, locked_until_utc = NULL,
                    lease_token = NULL, last_error = ?
                WHERE id = ? AND lease_token = ? AND published_at_utc IS NULL
                """,
                (_iso(available_at), error[:2_000], record_id, lease_token),
            )
            self._connection.commit()
            if cursor.rowcount != 1:
                raise StorageError("outbox lease is missing, expired, or already published")

    # ------------------------------------------------------ insights ----

    def _save_insight_sync(self, insight: NewsInsight, key: AnalysisKey) -> bool:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                created = self._save_insight_locked(insight, key)
                self._connection.commit()
                return created
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError(
                    "insight references a filing that has not been persisted"
                ) from exc
            except Exception:
                self._connection.rollback()
                raise

    def _save_insight_locked(self, insight: NewsInsight, key: AnalysisKey) -> bool:
        _validate_insight_key(insight, key)
        payload = insight.model_dump_json()
        digest = hashlib.sha256(payload.encode()).hexdigest()
        analysis_key = key.key
        row = self._connection.execute(
            "SELECT payload_sha256 FROM insights WHERE analysis_key = ?",
            (analysis_key,),
        ).fetchone()
        if row is not None:
            if str(row["payload_sha256"]) != digest:
                raise StorageIntegrityError(
                    "a stored analysis cannot be replaced by a different answer"
                )
            return False
        self._connection.execute(
            """
            INSERT INTO insights (
                analysis_key, event_id, accession_number, status, model_id,
                prompt_version, schema_version, document_sha256, input_sha256,
                payload_json, payload_sha256, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                analysis_key,
                insight.event_id,
                insight.accession_number,
                insight.status.value,
                key.model_id,
                key.prompt_version,
                key.schema_version,
                key.document_sha256,
                key.input_sha256,
                payload,
                digest,
                _iso(self._now()),
            ),
        )
        return True

    def _get_insight_sync(self, analysis_key: str) -> NewsInsight | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT payload_json FROM insights WHERE analysis_key = ?",
                (analysis_key,),
            ).fetchone()
        return NewsInsight.model_validate_json(row["payload_json"]) if row else None

    def _list_insights_since_sync(self, since: datetime) -> tuple[NewsInsight, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT payload_json FROM insights
                WHERE created_at_utc >= ?
                ORDER BY created_at_utc, analysis_key
                """,
                (_iso(_utc(since)),),
            ).fetchall()
        return tuple(NewsInsight.model_validate_json(row["payload_json"]) for row in rows)

    def _complete_event_sync(
        self,
        event_id: str,
        strategy_version: str,
        stage: str,
        outcome_json: str,
        insight: NewsInsight | None,
        analysis_key: AnalysisKey | None,
        outbox_id: int | None,
        lease_token: str | None,
        published_at: datetime,
    ) -> None:
        if (insight is None) != (analysis_key is None):
            raise StorageError("an insight and its analysis key must be supplied together")
        if (outbox_id is None) != (lease_token is None):
            raise StorageError("an outbox id and its lease token must be supplied together")
        if not stage.strip() or not strategy_version.strip():
            raise StorageError("a pipeline outcome requires a stage and strategy version")
        stamp = _utc(published_at)
        digest = hashlib.sha256(outcome_json.encode()).hexdigest()
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                if insight is not None and analysis_key is not None:
                    if insight.event_id != event_id:
                        raise StorageIntegrityError(
                            "insight and outcome must describe the same event"
                        )
                    self._save_insight_locked(insight, analysis_key)
                row = self._connection.execute(
                    """
                    SELECT payload_sha256 FROM pipeline_outcomes
                    WHERE event_id = ? AND strategy_version = ?
                    """,
                    (event_id, strategy_version),
                ).fetchone()
                if row is None:
                    self._connection.execute(
                        """
                        INSERT INTO pipeline_outcomes (
                            event_id, strategy_version, stage, analysis_key,
                            payload_json, payload_sha256, recorded_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            strategy_version,
                            stage,
                            analysis_key.key if analysis_key is not None else None,
                            outcome_json,
                            digest,
                            _iso(stamp),
                        ),
                    )
                elif str(row["payload_sha256"]) != digest:
                    raise StorageIntegrityError(
                        "a recorded pipeline outcome cannot be replaced by a different one"
                    )
                if outbox_id is not None and lease_token is not None:
                    cursor = self._connection.execute(
                        """
                        UPDATE outbox
                        SET published_at_utc = ?, locked_until_utc = NULL,
                            lease_token = NULL, last_error = NULL
                        WHERE id = ? AND lease_token = ? AND published_at_utc IS NULL
                        """,
                        (_iso(stamp), outbox_id, lease_token),
                    )
                    if cursor.rowcount != 1:
                        raise StorageError("outbox lease is missing, expired, or already published")
                self._connection.commit()
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                raise StorageIntegrityError(
                    "pipeline outcome references state that has not been persisted"
                ) from exc
            except Exception:
                self._connection.rollback()
                raise

    def _get_pipeline_outcome_sync(self, event_id: str, strategy_version: str) -> str | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                """
                SELECT payload_json FROM pipeline_outcomes
                WHERE event_id = ? AND strategy_version = ?
                """,
                (event_id, strategy_version),
            ).fetchone()
        return str(row["payload_json"]) if row else None

    # -------------------------------------------------------- leases ----

    def _acquire_lease_sync(
        self,
        name: str,
        holder: str,
        ttl: timedelta,
        now: datetime | None,
    ) -> bool:
        if not name.strip() or not holder.strip():
            raise StorageError("a lease requires a name and a holder")
        if ttl <= timedelta(0):
            raise StorageError("a lease needs a positive time to live")
        stamp = _utc(now) if now is not None else self._now()
        expires = stamp + ttl
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT holder, expires_at_utc FROM runtime_leases WHERE name = ?",
                    (name,),
                ).fetchone()
                if (
                    row is not None
                    and str(row["holder"]) != holder
                    and _parse_iso(str(row["expires_at_utc"])) > stamp
                ):
                    self._connection.commit()
                    return False
                self._connection.execute(
                    """
                    INSERT INTO runtime_leases (name, holder, acquired_at_utc, expires_at_utc)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        holder = excluded.holder,
                        acquired_at_utc = excluded.acquired_at_utc,
                        expires_at_utc = excluded.expires_at_utc
                    """,
                    (name, holder, _iso(stamp), _iso(expires)),
                )
                self._connection.commit()
                return True
            except Exception:
                self._connection.rollback()
                raise

    def _release_lease_sync(self, name: str, holder: str) -> bool:
        with self._lock:
            self._ensure_open()
            cursor = self._connection.execute(
                "DELETE FROM runtime_leases WHERE name = ? AND holder = ?",
                (name, holder),
            )
            self._connection.commit()
            return cursor.rowcount == 1

    def _lease_holder_sync(self, name: str) -> str | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT holder FROM runtime_leases WHERE name = ?",
                (name,),
            ).fetchone()
        return str(row["holder"]) if row else None

    # ----------------------------------------------- critical events ----

    def _record_critical_event_sync(
        self,
        code: str,
        detail: str | None,
        occurred_at: datetime | None,
    ) -> int:
        if not code.strip():
            raise StorageError("a critical event requires a code")
        stamp = _utc(occurred_at) if occurred_at is not None else self._now()
        with self._lock:
            self._ensure_open()
            cursor = self._connection.execute(
                """
                INSERT INTO critical_events (code, detail, occurred_at_utc, recorded_at_utc)
                VALUES (?, ?, ?, ?)
                """,
                (code[:200], detail[:2_000] if detail else None, _iso(stamp), _iso(self._now())),
            )
            self._connection.commit()
            return int(cursor.lastrowid or 0)

    def _list_critical_events_sync(self, since: datetime | None) -> tuple[dict[str, str], ...]:
        query = """
            SELECT code, detail, occurred_at_utc FROM critical_events
            {where}
            ORDER BY occurred_at_utc, id
        """
        with self._lock:
            self._ensure_open()
            if since is None:
                rows = self._connection.execute(query.format(where="")).fetchall()
            else:
                rows = self._connection.execute(
                    query.format(where="WHERE occurred_at_utc >= ?"),
                    (_iso(_utc(since)),),
                ).fetchall()
        return tuple(
            {
                "code": str(row["code"]),
                "detail": str(row["detail"]) if row["detail"] is not None else "",
                "occurred_at": str(row["occurred_at_utc"]),
            }
            for row in rows
        )

    def _list_pipeline_outcomes_since_sync(
        self, since: datetime
    ) -> tuple[tuple[str, str, str], ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT event_id, stage, payload_json FROM pipeline_outcomes
                WHERE recorded_at_utc >= ?
                ORDER BY recorded_at_utc, event_id
                """,
                (_iso(_utc(since)),),
            ).fetchall()
        return tuple(
            (str(row["event_id"]), str(row["stage"]), str(row["payload_json"])) for row in rows
        )

    def _record_heartbeat_sync(self, now: datetime) -> None:
        stamp = _utc(now)
        session_date = stamp.astimezone(NEW_YORK).date().isoformat()
        with self._lock:
            self._ensure_open()
            self._connection.execute(
                """
                INSERT INTO runtime_heartbeats (
                    session_date, first_seen_at_utc, last_seen_at_utc, ticks
                ) VALUES (?, ?, ?, 1)
                ON CONFLICT(session_date) DO UPDATE SET
                    last_seen_at_utc = MAX(
                        runtime_heartbeats.last_seen_at_utc, excluded.last_seen_at_utc
                    ),
                    first_seen_at_utc = MIN(
                        runtime_heartbeats.first_seen_at_utc, excluded.first_seen_at_utc
                    ),
                    ticks = runtime_heartbeats.ticks + 1
                """,
                (session_date, _iso(stamp), _iso(stamp)),
            )
            self._connection.commit()

    def _get_heartbeat_sync(self, session_date: date) -> tuple[datetime, datetime, int] | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                """
                SELECT first_seen_at_utc, last_seen_at_utc, ticks
                FROM runtime_heartbeats WHERE session_date = ?
                """,
                (session_date.isoformat(),),
            ).fetchone()
        if row is None:
            return None
        return (
            _parse_iso(str(row["first_seen_at_utc"])),
            _parse_iso(str(row["last_seen_at_utc"])),
            int(row["ticks"]),
        )

    def _count_outbox_sync(self, published: bool | None) -> int:
        if published is True:
            query = "SELECT COUNT(*) AS count FROM outbox WHERE published_at_utc IS NOT NULL"
        elif published is False:
            query = "SELECT COUNT(*) AS count FROM outbox WHERE published_at_utc IS NULL"
        else:
            query = "SELECT COUNT(*) AS count FROM outbox"
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(query).fetchone()
            return int(row["count"])

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime):
            raise StorageError("store clock must return a datetime")
        try:
            return _utc(value)
        except ValueError as exc:
            raise StorageError("store clock must return a timezone-aware datetime") from exc

    def _ensure_open(self) -> None:
        if self._closed:
            raise StorageError("operational store is closed")


def _validate_insight_key(insight: NewsInsight, key: AnalysisKey) -> None:
    if insight.event_id != key.event_id:
        raise StorageIntegrityError("insight and analysis key describe different events")
    if insight.accession_number != key.accession_number:
        raise StorageIntegrityError("insight and analysis key describe different filings")
    if insight.status is InsightStatus.ACTIONABLE and insight.model_id != key.model_id:
        raise StorageIntegrityError("an actionable insight must match its pinned model")
    if insight.status is InsightStatus.ACTIONABLE and (
        insight.prompt_version != key.prompt_version or insight.schema_version != key.schema_version
    ):
        raise StorageIntegrityError("an actionable insight must match its pinned prompt schema")


def _validate_limit(limit: int) -> None:
    if limit <= 0:
        raise ValueError("limit must be greater than 0")


def _normalize_signal(signal: Signal) -> Signal:
    return signal.model_copy(
        update={
            "decided_at": _utc(signal.decided_at),
            "expires_at": _utc(signal.expires_at),
        }
    )


def _normalize_risk_decision(decision: RiskDecision) -> RiskDecision:
    return decision.model_copy(update={"decided_at": _utc(decision.decided_at)})


def _normalize_order_intent(intent: OrderIntent) -> OrderIntent:
    return intent.model_copy(update={"created_at": _utc(intent.created_at)})


def _normalize_execution_report(report: ExecutionReport) -> ExecutionReport:
    return report.model_copy(update={"occurred_at": _utc(report.occurred_at)})


def _canonical_execution_transition(
    intent: OrderIntent,
    current: ExecutionReport | None,
    incoming: ExecutionReport,
) -> tuple[ExecutionReport, bool]:
    if intent.submission_mode != "paper":
        raise StorageIntegrityError("shadow intents cannot receive execution reports")
    if incoming.order_id != intent.order_id:
        raise StorageIntegrityError("execution report order id does not match its intent")
    if incoming.idempotency_key != intent.idempotency_key:
        raise StorageIntegrityError("execution report idempotency key does not match its intent")
    if incoming.occurred_at < intent.created_at:
        raise StorageIntegrityError("execution report predates its order intent")
    if incoming.broker_order_id is not None and not incoming.broker_order_id.strip():
        raise StorageIntegrityError("broker order id cannot be empty")

    _validate_execution_quantities(intent, incoming)
    if current is None:
        return incoming, True
    if incoming.occurred_at < current.occurred_at:
        raise StorageIntegrityError("execution timestamp cannot move backwards")
    if incoming.status not in _ALLOWED_EXECUTION_TRANSITIONS[current.status]:
        raise StorageIntegrityError(
            f"execution status cannot move from {current.status.value} to {incoming.status.value}"
        )
    if incoming.filled_quantity < current.filled_quantity:
        raise StorageIntegrityError("filled quantity cannot decrease")
    if incoming.fees < current.fees:
        raise StorageIntegrityError("execution fees cannot decrease")
    if incoming.fill_count < current.fill_count:
        raise StorageIntegrityError("counted fills cannot decrease")
    if incoming.update_sequence < current.update_sequence:
        raise StorageIntegrityError("broker update sequence cannot move backwards")
    if (
        current.broker_order_id is not None
        and incoming.broker_order_id is not None
        and current.broker_order_id != incoming.broker_order_id
    ):
        raise StorageIntegrityError("broker order id cannot change")

    canonical = incoming
    if current.broker_order_id is not None and incoming.broker_order_id is None:
        canonical = incoming.model_copy(update={"broker_order_id": current.broker_order_id})
    if (
        current.status
        in {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
        and current.status is canonical.status
        and not _adds_terminal_execution_evidence(current, canonical)
    ):
        return current, False
    return canonical, canonical != current


def _adds_terminal_execution_evidence(
    current: ExecutionReport,
    incoming: ExecutionReport,
) -> bool:
    """Distinguish late accounting evidence from a duplicate terminal callback."""

    return any(
        (
            incoming.broker_order_id != current.broker_order_id,
            incoming.filled_quantity != current.filled_quantity,
            incoming.average_fill_price != current.average_fill_price,
            incoming.fees != current.fees,
            incoming.slippage_bps != current.slippage_bps,
            incoming.fill_count != current.fill_count,
            incoming.pending_commission != current.pending_commission,
        )
    )


def _validate_execution_quantities(intent: OrderIntent, report: ExecutionReport) -> None:
    if report.filled_quantity > intent.quantity:
        raise StorageIntegrityError("filled quantity exceeds order quantity")
    if report.filled_quantity > 0 and report.average_fill_price <= 0:
        raise StorageIntegrityError("a fill requires a positive average fill price")
    if report.status is ExecutionStatus.FILLED and report.filled_quantity != intent.quantity:
        raise StorageIntegrityError("filled status requires the complete order quantity")
    if report.status is ExecutionStatus.PARTIALLY_FILLED:
        if not 0 < report.filled_quantity < intent.quantity:
            raise StorageIntegrityError(
                "partial fill quantity must be between zero and order quantity"
            )
    if (
        report.status
        in {
            ExecutionStatus.PENDING,
            ExecutionStatus.SUBMITTED,
            ExecutionStatus.REJECTED,
        }
        and report.filled_quantity != 0
    ):
        raise StorageIntegrityError(f"{report.status.value} report cannot contain fills")


def _normalize_filing(event: FilingEvent) -> FilingEvent:
    return event.model_copy(
        update={
            "accepted_at": _utc(event.accepted_at),
            "first_seen_at": _utc(event.first_seen_at),
            "retrieved_at": _utc(event.retrieved_at),
        }
    )


def _merge_filing(existing: FilingEvent, incoming: FilingEvent) -> FilingEvent:
    if existing.accepted_at != incoming.accepted_at or existing.cik != incoming.cik:
        raise StorageIntegrityError("immutable filing identity metadata changed")
    documents: list[DocumentRef] = []
    seen: set[tuple[str, str, str]] = set()
    for document in (*existing.documents, *incoming.documents):
        key = (document.url, document.kind, document.sha256)
        if key not in seen:
            documents.append(document)
            seen.add(key)
    preferred = incoming if incoming.complete or not existing.complete else existing
    return preferred.model_copy(
        update={
            "items": tuple(dict.fromkeys((*existing.items, *incoming.items))),
            "symbols": tuple(dict.fromkeys((*existing.symbols, *incoming.symbols))),
            "first_seen_at": min(existing.first_seen_at, incoming.first_seen_at),
            "retrieved_at": max(existing.retrieved_at, incoming.retrieved_at),
            "documents": tuple(documents),
            "complete": existing.complete or incoming.complete,
        }
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _utc(parsed)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_execution_fill(intent: OrderIntent, fill: ExecutionFill) -> None:
    if intent.submission_mode != "paper":
        raise StorageIntegrityError("shadow intents cannot receive execution fills")
    if fill.symbol.upper() != intent.symbol.upper():
        raise StorageIntegrityError("execution fill symbol does not match its intent")
    if fill.side is not intent.side:
        raise StorageIntegrityError("execution fill side does not match its intent")
    if fill.cumulative_quantity > intent.quantity:
        raise StorageIntegrityError("cumulative fill quantity exceeds order quantity")
    if fill.occurred_at < intent.created_at:
        raise StorageIntegrityError("execution fill predates its order intent")


def _is_commission_finalization(existing: ExecutionFill, incoming: ExecutionFill) -> bool:
    """Whether ``incoming`` only adds the commission ``existing`` still lacked."""

    if existing.commission_final or not incoming.commission_final:
        return False
    finalized = existing.model_copy(
        update={"commission": incoming.commission, "commission_final": True}
    )
    return finalized == incoming


def _migration_1_execution_fills(connection: sqlite3.Connection) -> None:
    """Add the per-fill ledger and make the existing reprice lineage explicit.

    Before this version a replacement order was only recognizable by the
    ``:r1`` suffix of its idempotency key.  The suffix stays as the durable
    lookup handle, but the lineage now lives in the intent itself, so no code
    path has to infer it from a string again.
    """

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_fills (
            execution_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL REFERENCES order_intents(order_id),
            broker_order_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            price TEXT NOT NULL,
            cumulative_quantity INTEGER NOT NULL CHECK (cumulative_quantity > 0),
            commission TEXT NOT NULL,
            commission_final INTEGER NOT NULL CHECK (commission_final IN (0, 1)),
            occurred_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            persisted_at_utc TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_order
        ON execution_fills (order_id, occurred_at_utc, execution_id)
        """
    )

    rows = connection.execute(
        "SELECT order_id, idempotency_key, payload_json FROM order_intents"
    ).fetchall()
    suffix = ":r1"
    for row in rows:
        key = str(row["idempotency_key"])
        if not key.endswith(suffix):
            continue
        payload = json.loads(row["payload_json"])
        if payload.get("reprice_generation"):
            continue
        predecessor = connection.execute(
            "SELECT order_id FROM order_intents WHERE idempotency_key = ?",
            (key[: -len(suffix)],),
        ).fetchone()
        if predecessor is None:
            raise StorageIntegrityError(
                f"replacement order {row['order_id']} has no persisted predecessor"
            )
        payload["replaces_order_id"] = predecessor["order_id"]
        payload["reprice_generation"] = 1
        connection.execute(
            "UPDATE order_intents SET payload_json = ? WHERE order_id = ?",
            (json.dumps(payload), row["order_id"]),
        )


_SCHEMA_MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_1_execution_fills,
}


OperationalStore = SQLiteOperationalStore


__all__ = [
    "DEFAULT_OUTBOX_TOPIC",
    "SCHEMA_VERSION",
    "OperationalStore",
    "OutboxRecord",
    "SQLiteOperationalStore",
    "StorageError",
    "StorageIntegrityError",
]
