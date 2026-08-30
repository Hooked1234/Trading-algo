from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from event_trader.domain import (
    DataSource,
    Direction,
    DocumentRef,
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    FilingEvent,
    OrderIntent,
    OrderSide,
    RiskDecision,
    Signal,
)
from event_trader.storage import (
    SCHEMA_VERSION,
    SQLiteOperationalStore,
    StorageError,
    StorageIntegrityError,
)

NOW = datetime(2026, 7, 30, 21, 0, tzinfo=UTC)


def _event(
    *,
    accession: str = "0000320193-26-000018",
    complete: bool = False,
    documents: tuple[DocumentRef, ...] = (),
    items: tuple[str, ...] = ("2.02",),
    symbols: tuple[str, ...] = ("AAPL",),
    first_seen_at: datetime | None = None,
    retrieved_at: datetime | None = None,
) -> FilingEvent:
    first_seen = first_seen_at or NOW
    retrieved = retrieved_at or first_seen
    return FilingEvent(
        event_id=f"sec:{accession}",
        accession_number=accession,
        cik="0000320193",
        form="8-K",
        items=items,
        symbols=symbols,
        accepted_at=datetime(2026, 7, 30, 16, 30, 28, tzinfo=timezone(timedelta(hours=-4))),
        first_seen_at=first_seen,
        retrieved_at=retrieved,
        documents=documents,
        source=DataSource.SEC,
        complete=complete,
    )


def _store(tmp_path: Path, *, clock=lambda: NOW) -> SQLiteOperationalStore:
    return SQLiteOperationalStore(
        tmp_path / "operational.sqlite3",
        tmp_path / "raw",
        clock=clock,
    )


def _signal(**updates: object) -> Signal:
    values: dict[str, object] = {
        "signal_id": "signal-aapl-1",
        "event_id": "sec:0000320193-26-000018",
        "accession_number": "0000320193-26-000018",
        "symbol": "AAPL",
        "strategy_version": "sec-8k-continuation-v1",
        "direction": Direction.LONG,
        "decided_at": NOW + timedelta(minutes=1),
        "entry_limit": Decimal("100"),
        "stop_price": Decimal("95"),
        "expires_at": NOW + timedelta(minutes=61),
        "holding_minutes": 60,
        "quant_features": {"relative_volume": 2.5},
        "insight_version": "prompt-v1",
    }
    values.update(updates)
    return Signal(**values)


def _risk(**updates: object) -> RiskDecision:
    values: dict[str, object] = {
        "signal_id": "signal-aapl-1",
        "approved": True,
        "quantity": 10,
        "notional": Decimal("1000"),
        "reason_codes": ("APPROVED",),
        "decided_at": NOW + timedelta(minutes=2),
        "limits": {"risk_per_trade": 0.005},
    }
    values.update(updates)
    return RiskDecision(**values)


def _intent(**updates: object) -> OrderIntent:
    values: dict[str, object] = {
        "order_id": "order-aapl-1",
        "idempotency_key": "idem-aapl-1",
        "signal_id": "signal-aapl-1",
        "account_id": "paper-account",
        "submission_mode": "paper",
        "research_promotion_sha256": "a" * 64,
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "quantity": 10,
        "limit_price": Decimal("100"),
        "created_at": NOW + timedelta(minutes=3),
    }
    values.update(updates)
    if values["submission_mode"] == "shadow":
        values["research_promotion_sha256"] = None
    return OrderIntent(**values)


def _report(
    status: ExecutionStatus,
    *,
    minute: int,
    filled_quantity: int = 0,
    average_fill_price: Decimal = Decimal("0"),
    fees: Decimal = Decimal("0"),
    broker_order_id: str | None = "broker-101",
    **updates: object,
) -> ExecutionReport:
    values: dict[str, object] = {
        "order_id": "order-aapl-1",
        "idempotency_key": "idem-aapl-1",
        "status": status,
        "filled_quantity": filled_quantity,
        "average_fill_price": average_fill_price,
        "fees": fees,
        "broker_order_id": broker_order_id,
        "occurred_at": NOW + timedelta(minutes=minute),
    }
    values.update(updates)
    return ExecutionReport(**values)


@pytest.mark.asyncio
async def test_raw_document_persistence_is_content_addressed_and_idempotent(tmp_path: Path) -> None:
    content = b"immutable SEC document bytes"
    expected = hashlib.sha256(content).hexdigest()
    store = _store(tmp_path)
    try:
        first = await store.persist_document(
            url="https://www.sec.gov/Archives/example.htm",
            kind="8-K",
            content=content,
            retrieved_at=NOW,
        )
        second = await store.persist_document(
            url="https://www.sec.gov/Archives/example.htm",
            kind="8-K",
            content=content,
            retrieved_at=NOW,
        )

        assert first == second
        assert first.sha256 == expected
        assert first.local_path is not None
        target = Path(first.local_path)
        assert target == (tmp_path / "raw" / "sha256" / expected[:2] / f"{expected}.bin").resolve()
        assert await asyncio.to_thread(target.read_bytes) == content
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_raw_document_persistence_detects_tampering(tmp_path: Path) -> None:
    content = b"original bytes"
    store = _store(tmp_path)
    try:
        reference = await store.persist_document(
            url="https://www.sec.gov/Archives/example.htm",
            kind="8-K",
            content=content,
            retrieved_at=NOW,
        )
        assert reference.local_path is not None
        await asyncio.to_thread(Path(reference.local_path).write_bytes, b"tampered bytes")

        with pytest.raises(StorageIntegrityError, match="corrupt"):
            await store.persist_document(
                url=reference.url,
                kind=reference.kind,
                content=content,
                retrieved_at=NOW,
            )
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_filing_upsert_is_idempotent_and_enqueues_once(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        event = _event(
            complete=True,
            first_seen_at=datetime(2026, 7, 30, 17, 0, tzinfo=timezone(timedelta(hours=-4))),
            retrieved_at=datetime(2026, 7, 30, 17, 1, tzinfo=timezone(timedelta(hours=-4))),
        )
        assert await store.save_filing_event(event) is True
        assert await store.save_filing_event(event) is False

        persisted = await store.get_filing(event.accession_number)
        assert persisted is not None
        assert persisted.accepted_at == datetime(2026, 7, 30, 20, 30, 28, tzinfo=UTC)
        assert persisted.first_seen_at == datetime(2026, 7, 30, 21, 0, tzinfo=UTC)
        assert persisted.retrieved_at == datetime(2026, 7, 30, 21, 1, tzinfo=UTC)
        assert await store.has_accession(event.accession_number) is True
        assert await store.list_filings() == (persisted,)
        assert await store.count_filings() == 1
        assert await store.count_outbox(published=False) == 1
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_complete_replay_enriches_incomplete_filing_without_duplicate_outbox(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    try:
        incomplete = _event(complete=False, items=("2.02",), symbols=())
        content = b"primary filing"
        reference = await store.persist_document(
            url="https://www.sec.gov/Archives/primary.htm",
            kind="8-K",
            content=content,
            retrieved_at=NOW + timedelta(minutes=1),
        )
        complete = _event(
            complete=True,
            documents=(reference,),
            items=("9.01",),
            symbols=("AAPL",),
            first_seen_at=NOW + timedelta(seconds=5),
            retrieved_at=NOW + timedelta(minutes=1),
        )

        assert await store.save_filing_event(incomplete) is True
        assert await store.count_outbox() == 0
        assert await store.save_filing_event(complete) is False

        persisted = await store.get_filing(incomplete.event_id)
        assert persisted is not None
        assert persisted.complete is True
        assert persisted.items == ("2.02", "9.01")
        assert persisted.symbols == ("AAPL",)
        assert persisted.first_seen_at == NOW
        assert persisted.retrieved_at == NOW + timedelta(minutes=1)
        assert persisted.documents == (reference,)
        assert await store.count_filings() == 1
        assert await store.count_outbox() == 1

        claimed = await store.claim_outbox()
        assert claimed[0].payload["complete"] is True
        assert claimed[0].payload["documents"][0]["sha256"] == reference.sha256
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_save_poll_commits_events_and_serialized_cursor(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        inserted = await store.save_poll(
            (_event(),),
            provider="sec.latest",
            cursor='{"seen_accessions":["0000320193-26-000018"]}',
        )

        assert inserted == 1
        assert await store.count_filings() == 1
        assert await store.get_cursor("sec.latest") == (
            '{"seen_accessions":["0000320193-26-000018"]}'
        )
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_outbox_leases_retry_and_publish_without_double_claim(tmp_path: Path) -> None:
    current = NOW

    def clock() -> datetime:
        return current

    store = _store(tmp_path, clock=clock)
    try:
        await store.save_filing_event(_event(complete=True))
        first_claim = await store.claim_outbox(limit=1, lease_seconds=30)
        assert len(first_claim) == 1
        assert first_claim[0].attempts == 1
        assert await store.claim_outbox(limit=1, lease_seconds=30) == ()

        await store.mark_outbox_failed(
            first_claim[0].id,
            first_claim[0].lease_token,
            "temporary downstream error",
            retry_at=current,
        )
        second_claim = await store.claim_outbox(limit=1, lease_seconds=30)
        assert len(second_claim) == 1
        assert second_claim[0].attempts == 2
        assert second_claim[0].lease_token != first_claim[0].lease_token

        with pytest.raises(StorageError, match="lease"):
            await store.mark_outbox_published(second_claim[0].id, "wrong-token")

        await store.mark_outbox_published(
            second_claim[0].id,
            second_claim[0].lease_token,
            published_at=current,
        )
        assert await store.count_outbox(published=False) == 0
        assert await store.count_outbox(published=True) == 1
        assert await store.claim_outbox() == ()
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_operational_decisions_and_order_survive_restart(tmp_path: Path) -> None:
    database_path = tmp_path / "operational.sqlite3"
    raw_root = tmp_path / "raw"
    store = SQLiteOperationalStore(database_path, raw_root, clock=lambda: NOW)
    event = _event()
    signal = _signal()
    risk = _risk()
    intent = _intent()
    submitted = _report(ExecutionStatus.SUBMITTED, minute=4)
    try:
        await store.save_filing_event(event)
        assert await store.save_signal(signal) is True
        assert await store.save_signal(signal) is False
        assert await store.save_risk_decision(risk) is True
        assert await store.save_risk_decision(risk) is False
        assert await store.save_order_intent(intent) is True
        assert await store.save_order_intent(intent) is False
        assert await store.save_execution_report(submitted) is True
        assert await store.save_execution_report(submitted) is False
    finally:
        await store.aclose()

    reopened = SQLiteOperationalStore(database_path, raw_root, clock=lambda: NOW)
    try:
        assert await reopened.get_signal(signal.signal_id) == signal
        assert await reopened.list_signals() == (signal,)
        assert await reopened.get_risk_decision(signal.signal_id) == risk
        assert await reopened.list_risk_decisions() == (risk,)
        assert await reopened.get_order_intent(intent.order_id) == intent
        assert await reopened.get_order_intent_by_key(intent.idempotency_key) == intent
        assert await reopened.list_order_intents() == (intent,)
        assert await reopened.get_execution_report(intent.order_id) == submitted
        assert await reopened.list_execution_reports() == (submitted,)
        assert await reopened.list_execution_history(intent.order_id) == (submitted,)
        assert await reopened.list_orders_for_reconciliation() == (intent,)
    finally:
        await reopened.aclose()


@pytest.mark.asyncio
async def test_operational_identity_conflicts_fail_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        await store.save_filing_event(_event())
        signal = _signal()
        await store.save_signal(signal)
        await store.save_risk_decision(_risk())
        await store.save_order_intent(_intent())

        with pytest.raises(StorageIntegrityError, match="signal id"):
            await store.save_signal(_signal(symbol="MSFT"))
        with pytest.raises(StorageIntegrityError, match="risk decision"):
            await store.save_risk_decision(_risk(quantity=9, notional=Decimal("900")))
        with pytest.raises(StorageIntegrityError, match="idempotency key"):
            await store.save_order_intent(_intent(order_id="order-aapl-2"))
        with pytest.raises(StorageIntegrityError, match="order id"):
            await store.save_order_intent(_intent(idempotency_key="idem-aapl-2"))

        unknown_signal_order = _intent(
            order_id="order-unknown",
            idempotency_key="idem-unknown",
            signal_id="missing-signal",
        )
        with pytest.raises(StorageIntegrityError, match="unknown signal"):
            await store.save_order_intent(unknown_signal_order)
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_session_filing_inventory_is_date_scoped_without_global_limit(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first = _event()
    later = first.model_copy(
        update={
            "event_id": "event-next-day",
            "accession_number": "0000320193-26-000002",
            "accepted_at": first.accepted_at + timedelta(days=1),
            "first_seen_at": first.first_seen_at + timedelta(days=1),
            "retrieved_at": first.retrieved_at + timedelta(days=1),
        }
    )
    try:
        await store.save_filing_event(first)
        await store.save_filing_event(later)

        records = await store.list_filings_for_session(first.accepted_at.date())

        assert records == (first,)
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_execution_reports_are_monotone_idempotent_and_reconcilable(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    try:
        await store.save_filing_event(_event())
        await store.save_signal(_signal())

        with pytest.raises(StorageIntegrityError, match="before its order intent"):
            await store.save_execution_report(_report(ExecutionStatus.SUBMITTED, minute=4))

        intent = _intent()
        await store.save_order_intent(intent)
        pending = _report(
            ExecutionStatus.PENDING,
            minute=3,
            broker_order_id=None,
        )
        submitted = _report(ExecutionStatus.SUBMITTED, minute=4)
        partial = _report(
            ExecutionStatus.PARTIALLY_FILLED,
            minute=5,
            filled_quantity=4,
            average_fill_price=Decimal("99.90"),
            fees=Decimal("0.25"),
        )
        filled = _report(
            ExecutionStatus.FILLED,
            minute=6,
            filled_quantity=10,
            average_fill_price=Decimal("99.95"),
            fees=Decimal("0.50"),
        )

        assert await store.save_execution_report(pending) is True
        assert await store.save_execution_report(pending) is False
        assert await store.save_execution_report(submitted) is True
        assert await store.save_execution_report(partial) is True
        assert await store.list_orders_for_reconciliation() == (intent,)

        with pytest.raises(StorageIntegrityError, match="cannot move"):
            await store.save_execution_report(
                _report(ExecutionStatus.SUBMITTED, minute=6, broker_order_id="broker-101")
            )
        with pytest.raises(StorageIntegrityError, match="cannot decrease"):
            await store.save_execution_report(
                _report(
                    ExecutionStatus.PARTIALLY_FILLED,
                    minute=6,
                    filled_quantity=3,
                    average_fill_price=Decimal("99.90"),
                    fees=Decimal("0.25"),
                )
            )
        with pytest.raises(StorageIntegrityError, match="cannot move backwards"):
            await store.save_execution_report(
                _report(
                    ExecutionStatus.PARTIALLY_FILLED,
                    minute=4,
                    filled_quantity=5,
                    average_fill_price=Decimal("99.90"),
                    fees=Decimal("0.30"),
                )
            )

        assert await store.save_execution_report(filled) is True
        terminal_replay = filled.model_copy(
            update={
                "occurred_at": filled.occurred_at + timedelta(minutes=1),
                "message": "duplicate terminal callback",
            }
        )
        assert await store.save_execution_report(terminal_replay) is False
        assert await store.get_execution_report(intent.order_id) == filled
        assert await store.list_orders_for_reconciliation() == ()
        history = await store.list_execution_history(intent.order_id)
        assert [report.status for report in history] == [
            ExecutionStatus.PENDING,
            ExecutionStatus.SUBMITTED,
            ExecutionStatus.PARTIALLY_FILLED,
            ExecutionStatus.FILLED,
        ]
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_shadow_orders_are_not_broker_reconcilable_and_reject_reports(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    try:
        await store.save_filing_event(_event())
        await store.save_signal(_signal())
        shadow = _intent(
            order_id="shadow-order",
            idempotency_key="shadow-order",
            submission_mode="shadow",
        )
        await store.save_order_intent(shadow)

        assert await store.list_orders_for_reconciliation() == ()
        report = _report(ExecutionStatus.SUBMITTED, minute=4).model_copy(
            update={
                "order_id": shadow.order_id,
                "idempotency_key": shadow.idempotency_key,
            }
        )
        with pytest.raises(StorageIntegrityError, match="shadow intents"):
            await store.save_execution_report(report)
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_fill_accounting_and_callback_order_are_monotonic(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        await store.save_filing_event(_event())
        await store.save_signal(_signal())
        intent = _intent()
        await store.save_order_intent(intent)
        await store.save_execution_report(_report(ExecutionStatus.SUBMITTED, minute=4))
        await store.save_execution_report(
            _report(
                ExecutionStatus.PARTIALLY_FILLED,
                minute=5,
                filled_quantity=6,
                average_fill_price=Decimal("99.90"),
                fees=Decimal("0.35"),
                fill_count=2,
                update_sequence=7,
            )
        )

        with pytest.raises(StorageIntegrityError, match="counted fills cannot decrease"):
            await store.save_execution_report(
                _report(
                    ExecutionStatus.PARTIALLY_FILLED,
                    minute=6,
                    filled_quantity=6,
                    average_fill_price=Decimal("99.90"),
                    fees=Decimal("0.35"),
                    fill_count=1,
                    update_sequence=8,
                )
            )
        with pytest.raises(StorageIntegrityError, match="update sequence cannot move backwards"):
            await store.save_execution_report(
                _report(
                    ExecutionStatus.PARTIALLY_FILLED,
                    minute=6,
                    filled_quantity=6,
                    average_fill_price=Decimal("99.90"),
                    fees=Decimal("0.35"),
                    fill_count=2,
                    update_sequence=6,
                )
            )

        stored = await store.get_execution_report(intent.order_id)
        assert stored is not None
        assert stored.fill_count == 2
        assert stored.update_sequence == 7

        await store.save_execution_report(
            _report(
                ExecutionStatus.FILLED,
                minute=7,
                filled_quantity=10,
                average_fill_price=Decimal("99.95"),
                fees=Decimal("0.35"),
                fill_count=2,
                pending_commission=True,
                update_sequence=9,
            )
        )
        await store.save_execution_report(
            _report(
                ExecutionStatus.FILLED,
                minute=8,
                filled_quantity=10,
                average_fill_price=Decimal("99.95"),
                fees=Decimal("0.70"),
                fill_count=2,
                pending_commission=False,
                update_sequence=10,
            )
        )
        enriched = await store.get_execution_report(intent.order_id)
        assert enriched is not None
        assert enriched.status is ExecutionStatus.FILLED
        assert enriched.fees == Decimal("0.70")
        assert not enriched.pending_commission
    finally:
        await store.aclose()


def _fill(**updates: object) -> ExecutionFill:
    values: dict[str, object] = {
        "order_id": "order-aapl-1",
        "execution_id": "0000e0d5.68ab12cd.01.01",
        "broker_order_id": "broker-101",
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "quantity": 4,
        "price": Decimal("99.90"),
        "cumulative_quantity": 4,
        "occurred_at": NOW + timedelta(minutes=5),
    }
    values.update(updates)
    return ExecutionFill(**values)


async def _seed_order(store: SQLiteOperationalStore) -> OrderIntent:
    await store.save_filing_event(_event())
    await store.save_signal(_signal())
    intent = _intent()
    await store.save_order_intent(intent)
    return intent


@pytest.mark.asyncio
async def test_fresh_database_records_the_current_schema_version(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        assert store.schema_version() == SCHEMA_VERSION
        assert await store.list_execution_fills("order-aapl-1") == ()
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_operational_schema_contains_exactly_the_16_owned_tables(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    try:
        with sqlite3.connect(store.database_path) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            }

        assert tables == {
            "critical_events",
            "execution_fills",
            "execution_report_history",
            "execution_reports",
            "filing_documents",
            "filings",
            "insights",
            "order_intents",
            "outbox",
            "pipeline_outcomes",
            "provider_cursors",
            "raw_documents",
            "risk_decisions",
            "runtime_heartbeats",
            "runtime_leases",
            "signals",
        }
        assert len(tables) == 16
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_execution_fills_are_idempotent_and_gain_only_their_commission(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    try:
        intent = await _seed_order(store)
        fill = _fill()

        assert await store.save_execution_fill(fill) is True
        assert await store.save_execution_fill(fill) is False

        final = fill.model_copy(update={"commission": Decimal("0.35"), "commission_final": True})
        assert await store.save_execution_fill(final) is True
        assert await store.list_execution_fills(intent.order_id) == (final,)

        with pytest.raises(StorageIntegrityError, match="reused for a different fill"):
            await store.save_execution_fill(
                final.model_copy(update={"commission": Decimal("0.70")})
            )
        with pytest.raises(StorageIntegrityError, match="reused for a different fill"):
            await store.save_execution_fill(final.model_copy(update={"quantity": 3}))
    finally:
        await store.aclose()

    reopened = _store(tmp_path)
    try:
        assert await reopened.list_execution_fills(intent.order_id) == (final,)
        assert await reopened.save_execution_fill(final) is False
    finally:
        await reopened.aclose()


@pytest.mark.asyncio
async def test_execution_fill_must_agree_with_its_order_intent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        await _seed_order(store)

        with pytest.raises(StorageIntegrityError, match="before its order intent"):
            await store.save_execution_fill(_fill(order_id="order-unknown"))
        with pytest.raises(StorageIntegrityError, match="symbol does not match"):
            await store.save_execution_fill(_fill(symbol="MSFT"))
        with pytest.raises(StorageIntegrityError, match="side does not match"):
            await store.save_execution_fill(_fill(side=OrderSide.SELL))
        with pytest.raises(StorageIntegrityError, match="exceeds order quantity"):
            await store.save_execution_fill(_fill(quantity=11, cumulative_quantity=11))
        with pytest.raises(StorageIntegrityError, match="predates its order intent"):
            await store.save_execution_fill(_fill(occurred_at=NOW))
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_shadow_intents_cannot_receive_execution_fills(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        await store.save_filing_event(_event())
        await store.save_signal(_signal())
        await store.save_order_intent(_intent(submission_mode="shadow"))

        with pytest.raises(StorageIntegrityError, match="shadow intents"):
            await store.save_execution_fill(_fill())
    finally:
        await store.aclose()


def _downgrade_to_gate_b(database: Path) -> None:
    """Turn a current database back into the pre-versioning Gate-B shape."""

    connection = sqlite3.connect(database)
    try:
        connection.execute("DROP TABLE execution_fills")
        connection.execute("PRAGMA user_version = 0")
        rows = connection.execute("SELECT order_id, payload_json FROM order_intents").fetchall()
        for order_id, payload_json in rows:
            payload = json.loads(payload_json)
            payload.pop("replaces_order_id", None)
            payload.pop("reprice_generation", None)
            connection.execute(
                "UPDATE order_intents SET payload_json = ? WHERE order_id = ?",
                (json.dumps(payload), order_id),
            )
        connection.commit()
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_gate_b_database_is_migrated_and_reprice_lineage_backfilled(
    tmp_path: Path,
) -> None:
    database = tmp_path / "operational.sqlite3"
    store = _store(tmp_path)
    try:
        original = await _seed_order(store)
        replacement = _intent(
            order_id=f"{original.order_id}-r1",
            idempotency_key=f"{original.idempotency_key}:r1",
            quantity=6,
            replaces_order_id=original.order_id,
            reprice_generation=1,
        )
        await store.save_order_intent(replacement)
    finally:
        await store.aclose()

    _downgrade_to_gate_b(database)

    migrated = _store(tmp_path)
    try:
        assert migrated.schema_version() == SCHEMA_VERSION
        restored = await migrated.get_order_intent(replacement.order_id)
        assert restored is not None
        assert restored.reprice_generation == 1
        assert restored.replaces_order_id == original.order_id
        base = await migrated.get_order_intent(original.order_id)
        assert base is not None
        assert base.reprice_generation == 0
        assert await migrated.save_execution_fill(_fill()) is True
    finally:
        await migrated.aclose()


@pytest.mark.asyncio
async def test_replacement_without_a_predecessor_fails_the_migration(
    tmp_path: Path,
) -> None:
    database = tmp_path / "operational.sqlite3"
    store = _store(tmp_path)
    try:
        await store.save_filing_event(_event())
        await store.save_signal(_signal())
        await store.save_order_intent(
            _intent(order_id="order-orphan-r1", idempotency_key="idem-orphan:r1")
        )
    finally:
        await store.aclose()

    _downgrade_to_gate_b(database)

    with pytest.raises(StorageIntegrityError, match="no persisted predecessor"):
        _store(tmp_path)


def test_a_newer_schema_version_is_never_opened(tmp_path: Path) -> None:
    database = tmp_path / "operational.sqlite3"
    _store(tmp_path).close()
    connection = sqlite3.connect(database)
    try:
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1:d}")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(StorageError, match="newer than the supported"):
        _store(tmp_path)
