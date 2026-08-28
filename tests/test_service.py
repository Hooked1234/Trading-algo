import asyncio
from datetime import timedelta

import pytest

from event_trader.monitor import ExitMonitorCycle
from event_trader.providers.sec import SecCursor, SecPollResult
from event_trader.service import LocalTradingDaemon


class Store:
    def __init__(self):
        self.cursor = None
        self.saved = ()

    async def get_cursor(self, provider):
        assert provider == "sec.latest"
        return self.cursor

    async def save_poll(self, events, *, provider, cursor, outbox_topic="filing.ingested"):
        assert provider == "sec.latest"
        assert outbox_topic == "filing.ingested"
        self.cursor = cursor
        self.saved = events
        return len(events)

    async def list_signals_since(self, since):
        del since
        return ()

    async def list_order_intents_since(self, since):
        del since
        return ()

    async def list_execution_reports_since(self, since):
        del since
        return ()


class Poller:
    def __init__(self, filing):
        self.filing = filing

    async def poll(self, cursor=None):
        assert isinstance(cursor, SecCursor)
        return SecPollResult(
            events=(self.filing,),
            cursor=SecCursor(seen_accessions=(self.filing.accession_number,)),
        )


class Session:
    def __init__(self):
        self.calls = 0
        self.limits = []

    async def process_ready(self, *, now, limit=100):
        del now
        self.calls += 1
        self.limits.append(limit)
        return ()


class Monitor:
    async def run_cycle(self, *, portfolio, signals, markets, now):
        assert not portfolio.positions
        assert signals == ()
        assert markets == ()
        return ExitMonitorCycle(checked_at=now, outcomes=())


def _daemon(filing, empty_portfolio, decision_time, **updates):
    store = Store()
    session = Session()

    async def portfolio_provider(_now):
        return empty_portfolio

    async def market_provider(_symbol, _now):
        return None

    async def startup():
        return None

    values = {
        "poller": Poller(filing),
        "store": store,
        "trading_session": session,
        "exit_monitor": Monitor(),
        "portfolio_provider": portfolio_provider,
        "exit_market_provider": market_provider,
        "startup_check": startup,
        "clock": lambda: decision_time,
    }
    values.update(updates)
    return LocalTradingDaemon(**values), store, session


@pytest.mark.asyncio
async def test_runtime_cycle_polls_monitors_then_processes_entries(
    filing, empty_portfolio, decision_time
) -> None:
    daemon, store, session = _daemon(filing, empty_portfolio, decision_time)

    result = await daemon.run_cycle(now=decision_time)

    assert result.ingested_filings == 1
    assert store.saved == (filing,)
    assert result.exit_cycle is not None
    assert session.calls == 1
    assert not result.critical_errors


@pytest.mark.asyncio
async def test_exit_monitor_failure_blocks_new_entries(
    filing, empty_portfolio, decision_time
) -> None:
    class BrokenMonitor:
        async def run_cycle(self, **_kwargs):
            raise RuntimeError("monitor unavailable")

    warnings = []

    async def warning_sink(message):
        warnings.append(message)

    daemon, _, session = _daemon(
        filing,
        empty_portfolio,
        decision_time,
        exit_monitor=BrokenMonitor(),
        warning_sink=warning_sink,
    )

    result = await daemon.run_cycle(now=decision_time, poll_sec=False)

    assert session.calls == 0
    assert "ENTRIES_BLOCKED_BY_RUNTIME_GUARD" in result.critical_errors
    assert warnings == list(result.critical_errors)


@pytest.mark.asyncio
async def test_daemon_always_runs_startup_and_shutdown_hooks(
    filing, empty_portfolio, decision_time
) -> None:
    lifecycle = []

    async def startup():
        lifecycle.append("startup")

    async def shutdown():
        lifecycle.append("shutdown")

    daemon, _, _ = _daemon(
        filing,
        empty_portfolio,
        decision_time,
        startup_check=startup,
        shutdown=shutdown,
    )
    stop = asyncio.Event()
    stop.set()

    await daemon.run(stop)

    assert lifecycle == ["startup", "shutdown"]


@pytest.mark.asyncio
async def test_exit_monitor_resamples_time_after_portfolio_io(
    filing, empty_portfolio, decision_time
) -> None:
    ticks = iter(
        (
            decision_time,
            decision_time + timedelta(seconds=2),
            decision_time + timedelta(seconds=3),
        )
    )

    async def portfolio_provider(requested_at):
        return empty_portfolio.model_copy(
            update={"as_of": requested_at + timedelta(seconds=1)}
        )

    class TimingMonitor:
        async def run_cycle(self, *, portfolio, signals, markets, now):
            assert now >= portfolio.as_of
            return ExitMonitorCycle(checked_at=now, outcomes=())

    daemon, _, _ = _daemon(
        filing,
        empty_portfolio,
        decision_time,
        portfolio_provider=portfolio_provider,
        exit_monitor=TimingMonitor(),
        clock=lambda: next(ticks),
    )

    result = await daemon.run_cycle(
        now=decision_time,
        poll_sec=False,
        process_entries=False,
    )

    assert result.exit_cycle is not None
    assert result.exit_cycle.checked_at == decision_time + timedelta(seconds=3)


@pytest.mark.asyncio
async def test_one_entry_worker_leases_one_event_at_a_time(
    filing, empty_portfolio, decision_time
) -> None:
    daemon, _store, session = _daemon(filing, empty_portfolio, decision_time)

    await daemon.run_cycle(now=decision_time)

    # A slow model call must never hold a lease over a queue of other filings.
    assert session.limits == [1]
    assert daemon.entry_batch_size == 1


@pytest.mark.asyncio
async def test_a_larger_entry_batch_must_be_configured_explicitly(
    filing, empty_portfolio, decision_time
) -> None:
    daemon, _store, session = _daemon(
        filing, empty_portfolio, decision_time, entry_batch_size=5
    )

    await daemon.run_cycle(now=decision_time)

    assert session.limits == [5]


def test_an_empty_entry_batch_is_refused(filing, empty_portfolio, decision_time) -> None:
    with pytest.raises(ValueError, match="entry batch size"):
        _daemon(filing, empty_portfolio, decision_time, entry_batch_size=0)
