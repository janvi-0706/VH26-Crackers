"""checkpoint.py in isolation: the write-ahead store's own contract, before
any worker.py wiring gets involved. Complements test_worker.py-style
integration tests (a real WorkerPool, a real cancelled task) with the
direct, deterministic proof of the one claim that matters most: recovery
returns only what is still genuinely in flight, at per-event granularity,
never a whole batch for a few real stragglers.
"""

from __future__ import annotations

import time

from triage.checkpoint import CheckpointStore
from triage.contracts import Event, EventType, Tier


def make_event(seq: int, *, tier: Tier = Tier.P1, etype: EventType = EventType.INVENTORY) -> Event:
    now = time.time()
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=etype, tier=tier, payload_size=64, value=40.0, cost=2.0,
        ingest_ts=now, deadline_ts=now + 5.0,
    )


# --------------------------------------------------------------------------
# begin() / mark_done(): the normal, one-event lifecycle
# --------------------------------------------------------------------------


def test_begin_then_mark_done_is_the_normal_lifecycle_and_leaves_nothing_behind():
    store = CheckpointStore()
    ev = make_event(1)
    assert store.begin(ev, worker_id=0) is True
    assert store.is_in_flight(ev.event_id) is True
    assert store.in_flight_count() == 1

    assert store.mark_done(ev.event_id) is True
    assert store.is_in_flight(ev.event_id) is False
    assert store.in_flight_count() == 0


def test_mark_done_with_no_matching_row_reports_false_not_an_exception():
    """The signal worker.py wires to metrics.observe_exactly_once_violation
    — a near-miss must be reported, not silently swallowed or crashed on."""
    store = CheckpointStore()
    assert store.mark_done("never-begun") is False


def test_begin_twice_for_the_same_event_id_reports_the_collision():
    """Structurally this should never happen (an Event is dequeued, and
    therefore handed to exactly one serve() call, at most once at a time)
    — proving begin() actually detects it if it ever did, rather than
    silently overwriting the first row, is what makes that "should never
    happen" a checked claim instead of an assumption."""
    store = CheckpointStore()
    ev = make_event(1)
    assert store.begin(ev, worker_id=0) is True
    assert store.begin(ev, worker_id=1) is False  # collision, even under a different worker_id
    # The original row is untouched — still attributed to worker 0.
    assert store.recover_worker(1) == []
    recovered = store.recover_worker(0)
    assert [e.event_id for e in recovered] == [ev.event_id]


# --------------------------------------------------------------------------
# recover_worker(): per-event granularity — the prompt's own "3 of 50" claim
# --------------------------------------------------------------------------


def test_recover_worker_returns_only_the_events_still_in_flight_for_that_worker():
    """The core claim: a batch of 8 where a worker died after finishing 5
    retries the remaining 3, never all 8. Modelled directly here (begin all
    8, mark_done the 5 that "finished"), independent of worker.py's own
    async batch-serving loop, which test_stage_i_exactly_once.py exercises
    end-to-end separately."""
    store = CheckpointStore()
    batch = [make_event(i) for i in range(8)]
    for e in batch:
        store.begin(e, worker_id=0)
    assert store.in_flight_count() == 8

    finished, unfinished = batch[:5], batch[5:]
    for e in finished:
        assert store.mark_done(e.event_id) is True

    recovered = store.recover_worker(0)
    assert sorted(e.event_id for e in recovered) == sorted(e.event_id for e in unfinished)
    assert store.in_flight_count() == 0  # recovery removes what it returns


def test_recover_worker_never_touches_a_different_workers_own_in_flight_rows():
    """The other half of the same claim: recovery scoped to a dead worker
    must never sweep up a still-alive worker's own in-progress event — that
    would be recovery CAUSING the exact double-processing this whole
    mechanism exists to prevent."""
    store = CheckpointStore()
    dead_worker_events = [make_event(i) for i in range(3)]
    alive_worker_event = make_event(99)
    for e in dead_worker_events:
        store.begin(e, worker_id=0)
    store.begin(alive_worker_event, worker_id=1)

    recovered = store.recover_worker(0)
    assert sorted(e.event_id for e in recovered) == sorted(e.event_id for e in dead_worker_events)
    # Worker 1's own row survives untouched.
    assert store.is_in_flight(alive_worker_event.event_id) is True
    assert store.in_flight_count() == 1


def test_recover_worker_on_a_worker_with_nothing_in_flight_returns_empty():
    store = CheckpointStore()
    assert store.recover_worker(0) == []
