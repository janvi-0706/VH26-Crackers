"""deferral.py: the durable store, the drainer, and the conservation
guarantee the prompt asks for directly — nothing deferred is ever lost.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from triage import deferral
from triage.contracts import Event, EventType, Tier
from triage.deferral import DeferralStore


def make_event(
    seq: int, *, tier: Tier = Tier.P1, etype: EventType = EventType.INVENTORY,
    ingest_ts: float | None = None, sla_seconds: float = 5.0,
) -> Event:
    ingest_ts = time.time() if ingest_ts is None else ingest_ts
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=etype, tier=tier, payload_size=64, value=40.0, cost=2.0,
        ingest_ts=ingest_ts, deadline_ts=ingest_ts + sla_seconds,
    )


@pytest.fixture(autouse=True)
def clean_default_store():
    deferral.reset_default_store()
    yield
    deferral.reset_default_store()


# --------------------------------------------------------------------------
# DeferralStore: schema, storage, ordering
# --------------------------------------------------------------------------


def test_defer_rejects_p0():
    store = DeferralStore()
    p0_event = make_event(1, tier=Tier.P0, etype=EventType.PAYMENT, sla_seconds=0.2)
    with pytest.raises(ValueError):
        store.defer(p0_event, "should never happen")


def test_defer_persists_original_ingest_ts_and_reason():
    """The DDL's whole point: a deferred row remembers when the event
    really arrived and why it was parked, not just that it exists."""
    store = DeferralStore()
    original_ingest = time.time() - 3.0
    event = make_event(1, ingest_ts=original_ingest)
    store.defer(event, "pressure 0.81 >= 0.75 — deferring until pressure falls")

    row = store.connection.execute(
        "SELECT deferred_ts, defer_reason, event_json FROM deferred_buffer WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    assert row["defer_reason"] == "pressure 0.81 >= 0.75 — deferring until pressure falls"
    restored = Event.model_validate_json(row["event_json"])
    assert restored.ingest_ts == pytest.approx(original_ingest)


def test_pending_count_tracks_defer_and_drain():
    store = DeferralStore()
    assert store.pending_count() == 0
    store.defer(make_event(1), "r")
    store.defer(make_event(2), "r")
    assert store.pending_count() == 2

    drained = store._pop_ready_batch(10)
    assert len(drained) == 2
    assert store.pending_count() == 0


def test_pop_ready_batch_orders_p1_before_p2_then_by_deadline_then_seq():
    """Matches idx_deferred_ready_priority exactly:
    (ready_at, tier, deadline_ts, seq)."""
    store = DeferralStore()
    now = time.time()
    p2_first_deferred = make_event(1, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=now, sla_seconds=30.0)
    p1_urgent = make_event(2, tier=Tier.P1, ingest_ts=now, sla_seconds=1.0)
    p1_relaxed = make_event(3, tier=Tier.P1, ingest_ts=now, sla_seconds=10.0)

    # Deferred in an order that would be wrong if ready_at/insertion order
    # were what decided drain order.
    store.defer(p2_first_deferred, "r", now=now)
    store.defer(p1_relaxed, "r", now=now)
    store.defer(p1_urgent, "r", now=now)

    drained = store._pop_ready_batch(10)
    assert [e.event_id for e in drained] == [
        p1_urgent.event_id, p1_relaxed.event_id, p2_first_deferred.event_id,
    ]


def test_already_deferred_is_populated_and_persists():
    store = DeferralStore()
    event = make_event(1)
    assert event.event_id not in store.already_deferred
    store.defer(event, "r")
    assert event.event_id in store.already_deferred
    # Draining does not forget it — that would reopen the infinite-redefer
    # trap the moment the same event_id came back around.
    store._pop_ready_batch(10)
    assert event.event_id in store.already_deferred


def test_drain_rate_reflects_recent_drains_only():
    store = DeferralStore()
    now = time.time()
    for i in range(5):
        store.defer(make_event(i), "r", now=now)
    store._pop_ready_batch(5)
    store.total_drained = 5
    store._drain_timestamps.extend([now] * 5)
    assert store.drain_rate(now=now) == pytest.approx(5 / deferral._DRAIN_RATE_WINDOW_SECONDS)
    # Well outside the window: the rate must have decayed to zero.
    assert store.drain_rate(now=now + 3600) == 0.0


# --------------------------------------------------------------------------
# The drainer: pressure-gated, rate-limited
# --------------------------------------------------------------------------


async def test_drainer_does_nothing_while_pressure_stays_high():
    store = DeferralStore()
    store.defer(make_event(1), "r")
    replayed: list[Event] = []
    stop = asyncio.Event()

    async def run_briefly():
        await store.run_drainer(
            replay=replayed.append, current_pressure=lambda: 0.9, stop_event=stop,
        )

    task = asyncio.ensure_future(run_briefly())
    await asyncio.sleep(deferral.DRAIN_TICK_SECONDS * 3)
    stop.set()
    await task

    assert replayed == []
    assert store.pending_count() == 1


async def test_drainer_replays_once_pressure_falls_below_threshold():
    store = DeferralStore()
    for i in range(3):
        store.defer(make_event(i), "r")
    replayed: list[Event] = []
    stop = asyncio.Event()
    pressure_value = [0.9]

    async def run_briefly():
        await store.run_drainer(
            replay=replayed.append,
            current_pressure=lambda: pressure_value[0],
            stop_event=stop,
        )

    task = asyncio.ensure_future(run_briefly())
    await asyncio.sleep(deferral.DRAIN_TICK_SECONDS * 2)
    assert replayed == [], "must not drain while pressure is still high"

    pressure_value[0] = 0.1
    await asyncio.sleep(deferral.DRAIN_TICK_SECONDS * 3)
    stop.set()
    await task

    assert len(replayed) == 3
    assert store.pending_count() == 0


async def test_drainer_rate_limits_to_drain_batch_per_tick():
    """Rate-limited so replay cannot re-trigger pressure and oscillate —
    proven by actually exceeding one tick's worth of backlog and watching
    it drain over multiple ticks, not all at once."""
    store = DeferralStore()
    total = deferral.DRAIN_BATCH_PER_TICK * 3
    for i in range(total):
        store.defer(make_event(i), "r")
    replayed: list[Event] = []
    stop = asyncio.Event()

    async def run_briefly():
        await store.run_drainer(
            replay=replayed.append, current_pressure=lambda: 0.0, stop_event=stop,
        )

    task = asyncio.ensure_future(run_briefly())
    await asyncio.sleep(deferral.DRAIN_TICK_SECONDS * 1.5)
    first_wave = len(replayed)
    assert 0 < first_wave <= deferral.DRAIN_BATCH_PER_TICK, (
        "one tick must not drain more than DRAIN_BATCH_PER_TICK"
    )

    await asyncio.sleep(deferral.DRAIN_TICK_SECONDS * 10)
    stop.set()
    await task
    assert len(replayed) == total
