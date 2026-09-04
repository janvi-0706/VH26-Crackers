"""queue.py + worker.py: the Stage B vertical slice, minus generator/classifier.

Stage B has no priority: a single FIFO, a fixed pool, and a cost-model sleep
that must sustain the documented 150 u/s ceiling within 5%. That number is
what every later stage's pressure signal is computed against, so it is worth
its own test rather than trusting the fake feed's calibration check.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from triage import ledger, metrics
from triage.config import load_config
from triage.contracts import Event, EventType, Tier
from triage.queue import EventQueue
from triage.worker import WorkerPool


@pytest.fixture(autouse=True)
def clean_registry():
    metrics.reset()
    ledger.reset()
    yield
    metrics.reset()
    ledger.reset()


def make_event(seq: int, cost: float = 1.0) -> Event:
    now = time.time()
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=EventType.LOG, tier=Tier.P2, payload_size=64,
        value=1.0, cost=cost, ingest_ts=now, deadline_ts=now + 60.0,
    )


# --------------------------------------------------------------------------
# EventQueue: put/get wire straight into metrics
# --------------------------------------------------------------------------


async def test_put_observes_ingest_and_get_observes_dequeue():
    q = EventQueue()
    ev = make_event(1)

    await q.put(ev)
    frame = metrics.snapshot()
    assert frame.ingested == 1
    assert frame.in_queue == 1
    assert frame.queue_depth["P2"] == 1

    got = await q.get()
    assert got.event_id == ev.event_id
    frame = metrics.snapshot()
    assert frame.in_queue == 0
    assert frame.in_flight == 1
    assert frame.queue_depth["P2"] == 0


def test_put_nowait_also_observes_ingest():
    q = EventQueue()
    q.put_nowait(make_event(1))
    assert metrics.snapshot().ingested == 1
    assert q.qsize() == 1


async def test_queue_is_first_in_first_out():
    q = EventQueue()
    for i in range(5):
        await q.put(make_event(i))
    order = [(await q.get()).seq for _ in range(5)]
    assert order == [0, 1, 2, 3, 4]


# --------------------------------------------------------------------------
# WorkerPool: the capacity ceiling
# --------------------------------------------------------------------------


async def test_worker_pool_sustains_150_units_per_second_within_5_percent():
    """6 workers x 25 u/s = 150 u/s. With cost=1.0 events, that ceiling
    translates directly into events/sec, so a 5% throughput check is also a
    5% check on the cost-model sleep math in worker.serve().
    """
    cfg = load_config()
    q = EventQueue()
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)

    # A backlog large enough that the queue never runs dry during the
    # measurement window — otherwise an idle worker would understate the
    # ceiling rather than reveal it.
    backlog = 1200
    for i in range(backlog):
        q.put_nowait(make_event(i))

    measure_seconds = 3.0
    pool.start()
    started = time.monotonic()
    await asyncio.sleep(measure_seconds)
    elapsed = time.monotonic() - started
    await pool.stop()

    assert q.qsize() > 0, "backlog ran dry mid-measurement; throughput is understated"

    throughput = pool.served_count / elapsed
    expected = cfg.total_capacity_ups  # 150.0 u/s, and cost=1.0 => events/s
    error = abs(throughput - expected) / expected
    assert error <= 0.05, (
        f"throughput {throughput:.1f} ev/s vs expected {expected:.1f} u/s, "
        f"error {error:.1%} exceeds 5%"
    )
    assert len(sunk) == pool.served_count


async def test_worker_serve_completes_and_writes_to_the_given_sink():
    cfg = load_config()
    q = EventQueue()
    sunk: list[Event] = []
    pool = WorkerPool(q, config=cfg, sink_write=sunk.append)

    ev = make_event(1, cost=0.5)
    await pool.serve(ev)

    assert sunk == [ev]
    frame = metrics.snapshot()
    assert frame.processed == 1
    assert frame.latency_p50["P2"] >= 0.0


async def test_worker_service_time_matches_the_cost_model():
    """cost / capacity_units_per_sec is the whole simulation — assert it,
    not just its downstream throughput average."""
    cfg = load_config()
    q = EventQueue()
    pool = WorkerPool(q, config=cfg, sink_write=lambda _e: None)

    ev = make_event(1, cost=2.5)
    started = time.monotonic()
    await pool.serve(ev)
    elapsed = time.monotonic() - started

    expected = 2.5 / cfg.worker_capacity_ups
    assert elapsed == pytest.approx(expected, abs=0.03)


async def test_stop_cancels_workers_and_start_cannot_be_called_twice():
    q = EventQueue()
    pool = WorkerPool(q, sink_write=lambda _e: None)
    tasks = pool.start()
    assert len(tasks) == pool.worker_count
    with pytest.raises(RuntimeError):
        pool.start()
    await pool.stop()
    assert all(t.done() for t in tasks)
