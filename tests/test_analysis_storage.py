from __future__ import annotations

import json
from datetime import timedelta

import pytest

from event_trader.analysis import AnalysisIdentity, AnalysisKey, model_input_sha256
from event_trader.calendar import NEW_YORK
from event_trader.domain import InsightStatus, NewsInsight
from event_trader.storage import SQLiteOperationalStore, StorageError, StorageIntegrityError


def _store(tmp_path, decision_time) -> SQLiteOperationalStore:
    return SQLiteOperationalStore(
        tmp_path / "state.sqlite",
        tmp_path / "raw",
        clock=lambda: decision_time,
    )


def _key(snapshot, long_insight) -> AnalysisKey:
    return AnalysisKey.for_snapshot(
        snapshot,
        AnalysisIdentity(
            model_id=long_insight.model_id,
            prompt_version=long_insight.prompt_version,
            schema_version=long_insight.schema_version,
        ),
    )


def test_the_analysis_key_binds_documents_prompt_and_input(snapshot, long_insight) -> None:
    identity = AnalysisIdentity(
        model_id=long_insight.model_id,
        prompt_version=long_insight.prompt_version,
        schema_version=long_insight.schema_version,
    )
    base = AnalysisKey.for_snapshot(snapshot, identity)
    other_prompt = AnalysisKey.for_snapshot(
        snapshot, identity.model_copy(update={"prompt_version": "2"})
    )
    other_text = AnalysisKey.for_snapshot(
        snapshot.model_copy(update={"document_text": "Something else entirely."}),
        identity,
    )

    assert base.key == AnalysisKey.for_snapshot(snapshot, identity).key
    assert base.key != other_prompt.key
    assert base.key != other_text.key
    assert base.input_sha256 == model_input_sha256(snapshot)


def test_a_model_id_must_name_its_provider(snapshot, long_insight) -> None:
    with pytest.raises(ValueError, match="provider/model"):
        AnalysisKey.for_snapshot(
            snapshot,
            AnalysisIdentity(model_id="bare-name", prompt_version="1"),
        )


@pytest.mark.asyncio
async def test_a_stored_analysis_is_immutable(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        key = _key(snapshot, long_insight)

        assert await store.save_insight(long_insight, key) is True
        assert await store.save_insight(long_insight, key) is False
        assert await store.get_insight(key.key) == long_insight

        with pytest.raises(StorageIntegrityError, match="different answer"):
            await store.save_insight(long_insight.model_copy(update={"confidence": 0.5}), key)


@pytest.mark.asyncio
async def test_an_insight_must_match_its_own_key(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        key = _key(snapshot, long_insight)

        with pytest.raises(StorageIntegrityError, match="different events"):
            await store.save_insight(
                long_insight.model_copy(update={"event_id": "other-event"}), key
            )
        with pytest.raises(StorageIntegrityError, match="pinned model"):
            await store.save_insight(
                long_insight.model_copy(update={"model_name": "other-model"}), key
            )


@pytest.mark.asyncio
async def test_an_abstention_may_come_from_no_model_at_all(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        abstention = NewsInsight.abstain(
            event_id=snapshot.filing.event_id,
            accession_number=snapshot.filing.accession_number,
            reason="model_unavailable",
        )

        assert await store.save_insight(abstention, _key(snapshot, long_insight)) is True
        stored = await store.get_insight(_key(snapshot, long_insight).key)
        assert stored is not None
        assert stored.status is InsightStatus.ABSTAIN


@pytest.mark.asyncio
async def test_completion_writes_analysis_outcome_and_outbox_atomically(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        records = await store.claim_outbox(limit=1, lease_seconds=60)
        assert len(records) == 1
        key = _key(snapshot, long_insight)

        await store.complete_event(
            event_id=snapshot.filing.event_id,
            strategy_version="sec-8k-continuation-v1",
            stage="shadow_order",
            outcome_json=json.dumps({"stage": "shadow_order"}),
            insight=long_insight,
            analysis_key=key,
            outbox_id=records[0].id,
            lease_token=records[0].lease_token,
            published_at=decision_time,
        )

        assert await store.count_outbox(published=False) == 0
        assert await store.get_insight(key.key) == long_insight
        stored = await store.get_pipeline_outcome(
            snapshot.filing.event_id, "sec-8k-continuation-v1"
        )
        assert stored is not None and json.loads(stored)["stage"] == "shadow_order"


@pytest.mark.asyncio
async def test_a_failed_completion_leaves_nothing_behind(
    tmp_path, snapshot, long_insight, decision_time
) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        key = _key(snapshot, long_insight)

        # An expired or foreign lease token must roll the whole write back.
        with pytest.raises(StorageError, match="outbox lease"):
            await store.complete_event(
                event_id=snapshot.filing.event_id,
                strategy_version="sec-8k-continuation-v1",
                stage="shadow_order",
                outcome_json=json.dumps({"stage": "shadow_order"}),
                insight=long_insight,
                analysis_key=key,
                outbox_id=1,
                lease_token="not-a-real-lease",
                published_at=decision_time,
            )

        assert await store.get_insight(key.key) is None
        assert (
            await store.get_pipeline_outcome(snapshot.filing.event_id, "sec-8k-continuation-v1")
            is None
        )
        assert await store.count_outbox(published=False) == 1


@pytest.mark.asyncio
async def test_a_recorded_outcome_cannot_be_replaced(tmp_path, snapshot, decision_time) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.save_filing_event(snapshot.filing)
        for stage, payload in (("filtered", '{"stage":"filtered"}'),) * 2:
            await store.complete_event(
                event_id=snapshot.filing.event_id,
                strategy_version="v1",
                stage=stage,
                outcome_json=payload,
                published_at=decision_time,
            )

        with pytest.raises(StorageIntegrityError, match="cannot be replaced"):
            await store.complete_event(
                event_id=snapshot.filing.event_id,
                strategy_version="v1",
                stage="shadow_order",
                outcome_json='{"stage":"shadow_order"}',
                published_at=decision_time,
            )


@pytest.mark.asyncio
async def test_the_singleton_lease_refuses_a_second_live_holder(tmp_path, decision_time) -> None:
    async with _store(tmp_path, decision_time) as store:
        assert await store.acquire_lease(
            "trading.daemon", holder="a", ttl=timedelta(seconds=30), now=decision_time
        )
        assert not await store.acquire_lease(
            "trading.daemon", holder="b", ttl=timedelta(seconds=30), now=decision_time
        )
        assert await store.lease_holder("trading.daemon") == "a"

        # After expiry the lease is free again.
        assert await store.acquire_lease(
            "trading.daemon",
            holder="b",
            ttl=timedelta(seconds=30),
            now=decision_time + timedelta(minutes=1),
        )
        assert await store.release_lease("trading.daemon", holder="b") is True
        assert await store.lease_holder("trading.daemon") is None


@pytest.mark.asyncio
async def test_critical_events_and_heartbeats_are_durable(tmp_path, decision_time) -> None:
    async with _store(tmp_path, decision_time) as store:
        await store.record_critical_event(
            "EXIT_MONITOR_ERROR", detail="gateway lost", occurred_at=decision_time
        )
        await store.record_critical_event("SEC_POLL_ERROR", occurred_at=decision_time)
        await store.record_heartbeat(decision_time)
        await store.record_heartbeat(decision_time + timedelta(seconds=30))

        events = await store.list_critical_events(since=decision_time - timedelta(hours=1))
        heartbeat = await store.get_heartbeat(decision_time.astimezone(NEW_YORK).date())

        assert [event["code"] for event in events] == [
            "EXIT_MONITOR_ERROR",
            "SEC_POLL_ERROR",
        ]
        assert heartbeat is not None
        first, last, ticks = heartbeat
        assert ticks == 2
        assert (last - first) == timedelta(seconds=30)
