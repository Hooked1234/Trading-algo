from datetime import timedelta

from event_trader.risk_halt import SQLiteRiskHaltGuard


def test_risk_halt_survives_restart_and_requires_manual_note(
    tmp_path, decision_time
) -> None:
    path = tmp_path / "risk.sqlite"
    first = SQLiteRiskHaltGuard(path)
    first.trip(reason="DAILY_LOSS_LIMIT", at=decision_time)
    assert first.is_halted()
    assert first.status().reason == "DAILY_LOSS_LIMIT"
    first.close()

    reopened = SQLiteRiskHaltGuard(path)
    assert reopened.is_halted()
    reopened.manual_reset(note="reviewed paper positions", at=decision_time + timedelta(minutes=1))
    assert not reopened.is_halted()
    assert reopened.status().reason is None
    reopened.close()
