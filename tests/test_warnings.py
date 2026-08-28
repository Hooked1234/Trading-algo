from datetime import timedelta

from event_trader.warnings import LocalCriticalWarningSink


async def test_local_warning_journal_is_durable_and_throttled(
    tmp_path, decision_time
) -> None:
    current = decision_time

    def clock():
        return current

    target = tmp_path / "critical.jsonl"
    sink = LocalCriticalWarningSink(target, clock=clock)
    await sink("BROKER_DISCONNECTED")
    await sink("BROKER_DISCONNECTED")
    current += timedelta(minutes=5)
    await sink("BROKER_DISCONNECTED")

    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert all('"severity":"critical"' in line for line in lines)
