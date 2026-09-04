"""worker.py: what actually happens to a dequeued event once decision.decide()
has an opinion about it — the execution, not just the label. Complements
test_decision.py (pure formulas) and test_deferral.py (the store/drainer).

Pressure is forced via monkeypatch rather than driven live through real
queue depth/EWMAs — that computation is already covered in test_metrics.py
and test_decision.py; these tests are about what worker.py *does* once it
has a pressure value and a decision, which needs to be deterministic to
test at all.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from triage import codel, deferral, ladder, ledger, metrics
from triage.config import load_config
from triage.contracts import Event, EventType, Tier
from triage.queue import EventQueue
from triage.worker import WorkerPool


@pytest.fixture(autouse=True)
def clean_state():
    metrics.reset()  # also resets codel.py and ladder.py's ambient state
    ledger.reset()
    deferral.reset_default_store()
    yield
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()


def force_codel_sampling(monkeypatch: pytest.MonkeyPatch, value: bool) -> None:
    monkeypatch.setattr(codel, "is_sampling", lambda: value)


def make_event(
    seq: int, *, tier: Tier = Tier.P1, etype: EventType = EventType.INVENTORY,
    cost: float = 2.0, value: float = 40.0, ingest_ts: float | None = None,
    sla_seconds: float = 5.0,
) -> Event:
    ingest_ts = time.time() if ingest_ts is None else ingest_ts
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=etype, tier=tier, payload_size=64, value=value, cost=cost,
        ingest_ts=ingest_ts, deadline_ts=ingest_ts + sla_seconds,
    )


def force_pressure(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    monkeypatch.setattr(metrics, "current_pressure", lambda *a, **k: value)


# --------------------------------------------------------------------------
# STREAM_NOW / P0
# --------------------------------------------------------------------------


async def test_p0_always_streams_individually_even_at_pressure_1(monkeypatch):
    force_pressure(monkeypatch, 1.0)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    ev = make_event(
        1, tier=Tier.P0, etype=EventType.PAYMENT, cost=3.5, value=120.0, sla_seconds=0.2
    )
    q.put_nowait(ev)
    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()
    assert sunk == [ev]
    assert pool.batched_count == 0
    assert pool.deferred_count == 0


# --------------------------------------------------------------------------
# MICRO_BATCH
# --------------------------------------------------------------------------


async def test_micro_batch_gathers_multiple_events_and_serves_them_together(monkeypatch):
    force_pressure(monkeypatch, 0.5)  # inside [0.40, 0.75)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    events = [make_event(i, ingest_ts=now, sla_seconds=30.0) for i in range(4)]
    for e in events:
        q.put_nowait(e)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sorted(e.event_id for e in sunk) == sorted(e.event_id for e in events)
    assert pool.batched_count == 4
    assert pool.deferred_count == 0


async def test_micro_batch_is_actually_faster_wall_clock_not_just_relabelled(monkeypatch):
    """The prompt's own bar: genuinely cheaper, not relabelled. Checked by
    the clock, not just by calling decision.batch_cost() and trusting it."""
    force_pressure(monkeypatch, 0.5)
    cfg = load_config()
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg, sink_write=lambda e: None)
    now = time.time()
    events = [make_event(i, ingest_ts=now, sla_seconds=30.0, cost=2.0) for i in range(4)]
    for e in events:
        q.put_nowait(e)

    individually_would_take = sum(e.cost for e in events) / cfg.worker_capacity_ups

    pool.start()
    started = time.monotonic()
    await asyncio.wait_for(q.join(), timeout=2.0)
    elapsed = time.monotonic() - started
    await pool.stop()

    assert elapsed < individually_would_take * 0.8  # a real margin, not a rounding coincidence


async def test_micro_batch_task_done_accounting_is_exact(monkeypatch):
    """join() would hang forever (and this test would time out) if a
    single task_done() were missed anywhere in the gather-and-serve path —
    exactly the class of bug queue.py's own docstring documents from
    Stage C's /control/reset."""
    force_pressure(monkeypatch, 0.5)
    cfg = load_config()
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg, sink_write=lambda e: None)
    now = time.time()
    for i in range(4):
        q.put_nowait(make_event(i, ingest_ts=now, sla_seconds=30.0))
    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()
    assert q.qsize() == 0


async def test_micro_batch_leaves_events_that_deserve_stream_now_alone(monkeypatch):
    """Gathering is best-effort and re-checks each candidate: an event
    pulled while filling a batch that turns out to have negative slack (or
    would independently qualify for something else) must not be forced
    into the batch just because it was convenient to grab."""
    force_pressure(monkeypatch, 0.5)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    batchable = make_event(1, ingest_ts=now, sla_seconds=30.0)
    already_late = make_event(2, ingest_ts=now - 100.0, sla_seconds=1.0)  # slack < 0
    q.put_nowait(batchable)
    q.put_nowait(already_late)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    # already_late has negative slack -> decide() says DEFER regardless of
    # pressure, and it has never been deferred before, so no override
    # applies: it goes to the deferral store, not into the batch, and
    # batchable is served on its own without being held up waiting for it.
    assert [e.event_id for e in sunk] == [batchable.event_id]
    assert deferral.pending_count() == 1
    assert deferral.was_deferred(already_late.event_id)


# --------------------------------------------------------------------------
# DEFER
# --------------------------------------------------------------------------


async def test_defer_sends_the_event_to_the_store_instead_of_serving_it(monkeypatch):
    force_pressure(monkeypatch, 0.9)  # >= 0.75
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    ev = make_event(1, ingest_ts=now, sla_seconds=30.0)  # ample positive slack
    q.put_nowait(ev)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sunk == []
    assert pool.deferred_count == 1
    assert deferral.pending_count() == 1
    assert deferral.was_deferred(ev.event_id)


async def test_defer_is_recorded_to_the_ledger(monkeypatch):
    from triage.contracts import Decision

    force_pressure(monkeypatch, 0.9)
    cfg = load_config()
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg, sink_write=lambda e: None)
    now = time.time()
    ev = make_event(1, ingest_ts=now, sla_seconds=30.0)
    q.put_nowait(ev)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert ledger.total_recorded() >= 1
    rows = list(ledger.records())
    assert any(r["decision"] == Decision.DEFER.value for r in rows)


# --------------------------------------------------------------------------
# The already-deferred-once override — the fix for the infinite-redefer trap
# --------------------------------------------------------------------------


async def test_a_second_defer_verdict_is_overridden_to_stream_instead_of_looping_forever(
    monkeypatch,
):
    """The exact trap worker.py's own docstring documents: an event whose
    slack has already gone negative by the time it is replayed would,
    without this override, be deferred again — forever."""
    force_pressure(monkeypatch, 0.9)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    # Already past its effective deadline: decide() alone would say DEFER
    # unconditionally (slack < 0), regardless of pressure.
    ev = make_event(1, ingest_ts=now - 100.0, sla_seconds=1.0)
    deferral.defer(ev, "first defer, simulating a prior pass through this worker")
    assert deferral.pending_count() == 1
    # Simulate the drainer's own replay: it pops (removes) the row from the
    # store as part of handing the event back to the live queue — exactly
    # what deferral._pop_ready_batch()/run_drainer() do together.
    replayed = deferral._default_store._pop_ready_batch(1)
    assert [e.event_id for e in replayed] == [ev.event_id]
    assert deferral.pending_count() == 0

    q.put_nowait(ev)  # now genuinely back in the live queue, as the drainer would leave it

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sunk == [ev], "must be served on the second pass, not deferred again"
    assert deferral.pending_count() == 0, "must not have been re-inserted into the store"


# --------------------------------------------------------------------------
# SAMPLE_ROLLUP — Stage E, CoDel-driven, P2 only
# --------------------------------------------------------------------------


async def test_codel_sampling_routes_p2_to_sample_rollup_instead_of_streaming(monkeypatch):
    force_pressure(monkeypatch, 0.1)  # well below every pressure band
    force_codel_sampling(monkeypatch, True)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    ev = make_event(1, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=now, sla_seconds=30.0)
    q.put_nowait(ev)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sunk == [], "a sampled event must never be sunk individually"
    assert pool.sampled_count == 1


async def test_codel_sampling_never_touches_p0_or_p1(monkeypatch):
    force_pressure(monkeypatch, 0.1)
    force_codel_sampling(monkeypatch, True)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    p0 = make_event(1, tier=Tier.P0, etype=EventType.PAYMENT, cost=3.5, value=120.0,
                     ingest_ts=now, sla_seconds=0.2)
    p1 = make_event(2, tier=Tier.P1, ingest_ts=now, sla_seconds=30.0)
    q.put_nowait(p0)
    q.put_nowait(p1)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sorted(e.event_id for e in sunk) == sorted([p0.event_id, p1.event_id])
    assert pool.sampled_count == 0


async def test_a_finished_reservoir_window_is_persisted_and_counted(monkeypatch):
    """RESERVOIR_N (10) sampled P2 clicks must produce exactly one durable
    rollup row and move weighted_click_count by exactly RESERVOIR_N — the
    exact-reconstruction property ladder.py's own docstring describes."""
    force_pressure(monkeypatch, 0.1)
    force_codel_sampling(monkeypatch, True)
    cfg = load_config()
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg, sink_write=lambda e: None)
    now = time.time()
    events = [
        make_event(i, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=now, sla_seconds=30.0)
        for i in range(ladder.RESERVOIR_N)
    ]
    for e in events:
        q.put_nowait(e)

    from triage import sink

    rollups_before = sink.rollup_count()

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert pool.sampled_count == ladder.RESERVOIR_N
    assert sink.rollup_count() == rollups_before + 1
    assert metrics.snapshot().weighted_click_count == pytest.approx(float(ladder.RESERVOIR_N))


# --------------------------------------------------------------------------
# SHED — Stage E, hard shed above ladder.HARD_SHED_PRESSURE, P2 only
# --------------------------------------------------------------------------


async def test_hard_shed_drops_p2_above_the_pressure_threshold(monkeypatch):
    force_pressure(monkeypatch, ladder.HARD_SHED_PRESSURE)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    ev = make_event(1, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=now, sla_seconds=30.0)
    q.put_nowait(ev)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sunk == []
    assert pool.shed_count == 1
    assert deferral.pending_count() == 0


async def test_hard_shed_never_touches_p0_or_p1(monkeypatch):
    force_pressure(monkeypatch, 1.0)
    cfg = load_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)
    now = time.time()
    p0 = make_event(1, tier=Tier.P0, etype=EventType.PAYMENT, cost=3.5, value=120.0,
                     ingest_ts=now, sla_seconds=0.2)
    q.put_nowait(p0)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert sunk == [p0]
    assert pool.shed_count == 0


async def test_hard_shed_is_recorded_to_the_ledger(monkeypatch):
    from triage.contracts import Decision

    force_pressure(monkeypatch, ladder.HARD_SHED_PRESSURE)
    cfg = load_config()
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg, sink_write=lambda e: None)
    now = time.time()
    ev = make_event(1, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=now, sla_seconds=30.0)
    q.put_nowait(ev)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert ledger.total_recorded() >= 1
    rows = list(ledger.records())
    assert any(r["decision"] == Decision.SHED.value for r in rows)


async def test_in_flight_does_not_leak_on_shed_or_sample(monkeypatch):
    """The same in_flight-leak bug DEFER had in Stage D (observe_dequeue's
    +1 only ever balanced by observe_complete's -1) applies identically to
    SHED and SAMPLE_ROLLUP — neither ever completes an event."""
    force_pressure(monkeypatch, ladder.HARD_SHED_PRESSURE)
    cfg = load_config()
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg, sink_write=lambda e: None)
    now = time.time()
    ev = make_event(1, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=now, sla_seconds=30.0)
    q.put_nowait(ev)

    pool.start()
    await asyncio.wait_for(q.join(), timeout=2.0)
    await pool.stop()

    assert metrics.snapshot().in_flight == 0
