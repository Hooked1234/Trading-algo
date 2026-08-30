from datetime import timedelta

from event_trader.calendar import NEW_YORK
from event_trader.reconciliation import (
    DailySecReconciler,
    SQLiteSecReconciliationLedger,
    reconcile_sec_accessions,
)


def test_daily_sec_reconciliation_reports_missing_and_unexpected(filing, decision_time) -> None:
    result = reconcile_sec_accessions(
        daily_index_accessions=(filing.accession_number, "0000320193-26-000099"),
        stored_filings=(filing,),
        reconciled_at=decision_time,
    )
    assert not result.complete
    assert result.missing_accessions == ("0000320193-26-000099",)
    assert result.unexpected_accessions == ()


def test_daily_sec_reconciliation_is_complete(filing, decision_time) -> None:
    result = reconcile_sec_accessions(
        daily_index_accessions=(filing.accession_number,),
        stored_filings=(filing,),
        reconciled_at=decision_time,
    )
    assert result.complete


def test_reconciliation_gap_persists_and_blocks_orders(tmp_path, filing, decision_time) -> None:
    path = tmp_path / "reconciliation.sqlite"
    incomplete = reconcile_sec_accessions(
        daily_index_accessions=(filing.accession_number, "0000320193-26-000099"),
        stored_filings=(filing,),
        reconciled_at=decision_time,
    )
    ledger = SQLiteSecReconciliationLedger(path)
    ledger.save(session_date=decision_time.date(), result=incomplete)
    assert ledger.orders_blocked()
    ledger.close()

    reopened = SQLiteSecReconciliationLedger(path)
    assert reopened.orders_blocked()
    complete = reconcile_sec_accessions(
        daily_index_accessions=(filing.accession_number,),
        stored_filings=(filing,),
        reconciled_at=decision_time,
    )
    reopened.save(session_date=decision_time.date(), result=complete)
    assert not reopened.orders_blocked()
    assert reopened.orders_blocked(required_through=decision_time.date() + timedelta(days=1))
    reopened.close()


async def test_daily_reconciler_fetches_compares_and_persists(
    tmp_path, filing, decision_time
) -> None:
    session_date = filing.accepted_at.astimezone(NEW_YORK).date()

    class Source:
        async def accessions(self, requested_date):
            assert requested_date == session_date
            return (filing.accession_number,)

    class Inventory:
        async def list_filings_for_session(self, requested_date):
            assert requested_date == session_date
            return (filing,)

    ledger = SQLiteSecReconciliationLedger(tmp_path / "reconciliation.sqlite")
    result = await DailySecReconciler(source=Source(), inventory=Inventory(), ledger=ledger).run(
        session_date=session_date, reconciled_at=decision_time
    )

    assert result.complete
    assert ledger.latest() == (session_date, result)
    ledger.close()
