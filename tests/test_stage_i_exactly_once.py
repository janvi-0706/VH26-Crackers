"""Stage I's own claim, end to end, through a real WorkerPool: kill a
worker mid-batch and prove exactly-once side effects — not just that
checkpoint.py's own store behaves (test_checkpoint.py covers that in
isolation), but that worker.py's real cancellation-recovery-respawn wiring
actually produces "a batch of N with M unfinished retries M, not N" and
never double-processes anything along the way.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time

import pytest

from triage import deferral, ledger, metrics
from triage.config import load_config
from triage.contracts import Event, EventType, Tier
from triage.queue import EventQueue
from triage.worker import WorkerPool


def single_worker_config():
    """Config is a frozen dataclass — worker_count=1 via dataclasses.replace,
    not mutation. Forcing exactly one worker isolates the one thing these
    tests exist to prove (what happens when THE worker holding a batch
    dies) from an unrelated second worker racing to pick up leftovers,
    which would still be correct but would no longer isolate anything."""
    return dataclasses.replace(load_config(), worker_count=1)


@pytest.fixture(autouse=True)
def clean_state():
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()
    yield
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()


def make_event(seq: int, *, ingest_ts: float) -> Event:
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=EventType.INVENTORY, tier=Tier.P1, payload_size=64,
        value=40.0, cost=2.0, ingest_ts=ingest_ts, deadline_ts=ingest_ts + 30.0,
    )


def force_pressure(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    monkeypatch.setattr(metrics, "current_pressure", lambda *a, **k: value)


async def wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.01) -> None:
    """Poll rather than rely solely on `queue.join()` — recovery requeues
    a dead worker's orphaned events from an `add_done_callback`, which runs
    on its own turn of the event loop. There is a real, if brief, window
    where every ORIGINAL put() already has its matching task_done() (the
    cancelled task's own `finally` blocks already ran) but the recovery
    callback has not yet called `put_replayed()` for what it orphaned —
    `queue.join()` alone can wake up and return inside exactly that window,
    before the replayed events are even back in the queue, let alone
    reprocessed. Polling the actual final side effect (what landed in the
    sink) is what these tests care about, not the queue's own unfinished
    count at some arbitrary instant."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for condition")


async def test_killing_a_worker_mid_batch_retries_only_the_unfinished_members(monkeypatch):
    """7 events, one MICRO_BATCH, pressure forced to 0.74 (just inside the
    [0.40, 0.75) MICRO_BATCH band) so decision.batch_size(0.74) == 7 — the
    largest a genuine MICRO_BATCH decision can ever gather (B_MAX=8 is only
    reachable by a pressure value decide() itself would have routed to
    DEFER instead, see decision.batch_size's own docstring on why it still
    clamps there defensively). The worker is killed (task.cancel()) the
    instant the 4th member's sink write lands — the earliest point a real
    cancellation could plausibly be requested from outside — so members
    0-3 are already fully served and 4-6 are still checkpointed when the
    task actually dies at the next await (the per-member sleep(0)).

    Asserts the prompt's own claim directly: a batch of 7 with 3
    unfinished retries 3, not 7 — every event lands in the sink exactly
    once, exactly_once_violations stays 0, and retries reports exactly 3.
    """
    force_pressure(monkeypatch, 0.74)
    cfg = single_worker_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    task_holder: dict[str, asyncio.Task] = {}

    def fake_sink_write(e: Event) -> None:
        sunk.append(e)
        if len(sunk) == 4:
            task_holder["task"].cancel()

    pool = WorkerPool(q, config=cfg, sink_write=fake_sink_write)

    now = time.time()
    events = [make_event(i, ingest_ts=now) for i in range(7)]
    for e in events:
        q.put_nowait(e)

    tasks = pool.start()
    task_holder["task"] = tasks[0]

    await wait_until(lambda: len(sunk) == len(events))
    await asyncio.wait_for(q.join(), timeout=5.0)
    await pool.stop()

    # Exactly one sink write per event — no duplicates, no losses.
    assert sorted(e.event_id for e in sunk) == sorted(e.event_id for e in events)
    assert len(sunk) == 7

    frame = metrics.snapshot()
    assert frame.exactly_once_violations == 0
    assert frame.retries == 3  # members 4, 5, 6 — never the whole batch of 7
    assert pool.recovered_count == 3


async def test_a_recovered_event_completes_exactly_once_even_when_streamed_alone(monkeypatch):
    """The single-event (STREAM_NOW) path, not a batch: kill the worker
    while it is asleep serving the ONE event it holds, confirm recovery
    hands it back, and confirm it is only ever sunk once total (the dead
    attempt never got far enough to write it; the retry is the only
    write)."""
    force_pressure(monkeypatch, 0.0)  # STREAM_NOW band
    cfg = single_worker_config()
    q = EventQueue(config=cfg)
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)

    now = time.time()
    ev = make_event(1, ingest_ts=now)  # cost=2.0 -> ~80ms of simulated service
    q.put_nowait(ev)

    tasks = pool.start()
    worker_task = tasks[0]

    # A real (small) wall-clock pause, not a bare `await sleep(0)`: the
    # worker has to actually get scheduled, dequeue, decide, and reach its
    # own `await asyncio.sleep(service_seconds)` before cancelling means
    # anything — a single sleep(0) yield gives no guarantee it got that
    # far. 10ms is comfortably inside this event's own ~80ms simulated
    # service time, so cancellation reliably lands inside that sleep, not
    # before checkpoint.begin() or after the event has already completed.
    await asyncio.sleep(0.01)
    worker_task.cancel()

    await wait_until(lambda: len(sunk) == 1)
    await asyncio.wait_for(q.join(), timeout=5.0)
    await pool.stop()

    assert sunk == [ev]
    frame = metrics.snapshot()
    assert frame.exactly_once_violations == 0
    assert frame.retries == 1


async def test_a_dead_worker_is_replaced_so_pool_capacity_does_not_shrink(monkeypatch):
    """Fixed 6-worker capacity is the number every other claim in this
    project is measured against — a worker dying must not permanently
    shrink the pool, or that ceiling silently stops being true."""
    force_pressure(monkeypatch, 0.0)
    cfg = dataclasses.replace(load_config(), worker_count=2)
    q = EventQueue(config=cfg)
    pool = WorkerPool(q, config=cfg)

    tasks = pool.start()
    assert len(tasks) == 2
    victim = tasks[0]
    victim.cancel()
    # A real (small) pause, not a bare sleep(0): the cancellation has to
    # actually propagate through the task and its done-callback has to
    # run before pool._tasks reflects the replacement.
    await asyncio.sleep(0.05)

    assert len(pool._tasks) == 2
    assert all(not t.done() for t in pool._tasks)
    assert victim not in pool._tasks  # replaced, not merely still counted

    await pool.stop()
