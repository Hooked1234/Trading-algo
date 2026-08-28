from datetime import timedelta

import pytest

from event_trader.budget import BudgetedInsightProvider, SQLiteModelBudgetLedger


class CountingProvider:
    def __init__(self, insight) -> None:
        self.insight = insight
        self.calls = 0

    async def analyze(self, _snapshot):
        self.calls += 1
        return self.insight


@pytest.mark.asyncio
async def test_model_budget_is_reserved_before_call(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    ledger = SQLiteModelBudgetLedger(tmp_path / "budget.sqlite")
    provider = CountingProvider(long_insight)
    guarded = BudgetedInsightProvider(
        provider=provider,
        ledger=ledger,
        provider_name="test",
        reserved_cost_eur=0.02,
        daily_limit_eur=0.02,
        monthly_limit_eur=1,
        clock=lambda: decision_time,
    )
    assert await guarded.analyze(snapshot) == long_insight
    rejected = await guarded.analyze(snapshot)
    assert rejected.abstain_reason == "model_budget_exhausted"
    assert provider.calls == 1
    ledger.close()


@pytest.mark.asyncio
async def test_daily_budget_resets_but_monthly_limit_remains(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    ledger = SQLiteModelBudgetLedger(tmp_path / "budget.sqlite")
    provider = CountingProvider(long_insight)
    moments = iter((decision_time, decision_time + timedelta(days=1)))
    guarded = BudgetedInsightProvider(
        provider=provider,
        ledger=ledger,
        provider_name="test",
        reserved_cost_eur=0.02,
        daily_limit_eur=0.02,
        monthly_limit_eur=0.03,
        clock=lambda: next(moments),
    )
    assert await guarded.analyze(snapshot) == long_insight
    rejected = await guarded.analyze(snapshot)
    assert rejected.abstain_reason == "model_budget_exhausted"
    ledger.close()
