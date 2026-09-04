"""POST /chaos/kill-worker and POST /chaos/duplicate-flood, driven through a
real running Engine (fastapi TestClient, real mode) — not mocked, for the
same reason test_app.py's own real-mode tests are not mocked: a chaos
endpoint that only proves itself against a fake engine proves nothing
about the actual recovery/dedup wiring.
"""

from __future__ import annotations

import time as _time

import pytest
from fastapi.testclient import TestClient

from triage import metrics, sink
from triage.app import create_app


@pytest.fixture(autouse=True)
def clean_sink():
    """The sink is an ambient, process-wide singleton (matching
    deferral.py/ledger.py's own precedent) — a full-suite run creates many
    independent real Engines back to back, all sharing it. Without this,
    `sink.recent()`'s own "most recent N" is contaminated by whatever an
    unrelated, already-finished test's engine most recently committed,
    which can starve THIS test's own duplicate-flood replay of the exact
    dedup_keys it just admitted. Never reset by Engine.reset() itself (see
    sink.reset_default_store()'s own docstring) — this is a test-isolation
    concern only."""
    sink.reset_default_store()
    yield
    sink.reset_default_store()


def wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.02) -> None:
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if predicate():
            return
        _time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for condition")


# --------------------------------------------------------------------------
# POST /chaos/kill-worker
# --------------------------------------------------------------------------


def test_kill_worker_endpoint_kills_a_real_task_and_recovery_respawns_it():
    app = create_app(fake=False, seed=101)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_rate(200.0)  # fast enough that a worker is likely busy
        wait_until(lambda: metrics.snapshot().processed > 5)

        resp = client.post("/chaos/kill-worker")
        assert resp.status_code == 200
        worker_id = resp.json()["worker_id"]
        assert worker_id is not None
        assert 0 <= worker_id < engine.workers.worker_count

        # Recovery is asynchronous (the done-callback runs on the event
        # loop's own next turn) — poll the actual healed state, not just
        # the task list's length, which never changes (a dead task is
        # still one list entry until its done-callback replaces it).
        wait_until(lambda: all(not t.done() for t in engine.workers._tasks))
        assert len(engine.workers._tasks) == engine.workers.worker_count

        # The pipeline keeps making real progress after the kill — the
        # whole point of automatic recovery.
        processed_after_kill = metrics.snapshot().processed
        wait_until(lambda: metrics.snapshot().processed > processed_after_kill + 5)

        frame = metrics.snapshot()
        assert frame.exactly_once_violations == 0


def test_kill_worker_is_fake_mode_guarded():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/chaos/kill-worker")
    assert resp.status_code == 409


def test_kill_worker_before_start_reports_no_worker_killed_not_an_error():
    """Calling chaos_kill_worker() on a pool with no live tasks (e.g. right
    at shutdown) is a valid outcome, not a crash — WorkerPool.kill_worker's
    own contract."""
    from triage.config import load_config
    from triage.queue import EventQueue
    from triage.worker import WorkerPool

    cfg = load_config()
    pool = WorkerPool(EventQueue(config=cfg), config=cfg)
    assert pool.kill_worker() is None


# --------------------------------------------------------------------------
# POST /chaos/duplicate-flood
# --------------------------------------------------------------------------


def test_duplicate_flood_of_1000_leaves_the_sink_row_count_unchanged():
    """The prompt's own literal claim. Real events are admitted first (so
    there is something real to replay), then the generator is throttled to
    zero so organic arrivals cannot be mistaken for the flood's own effect
    on the count, then a flood of 1000 is fired and the sink row count is
    compared before/after."""
    app = create_app(fake=False, seed=102)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_rate(300.0)
        wait_until(lambda: sink.count() >= 50, timeout=8.0)

        engine.set_rate(0.0)  # stop organic arrivals before measuring
        _time.sleep(0.2)  # let anything already in flight settle

        count_before = sink.count()
        duplicates_before = metrics.snapshot().duplicates_caught

        resp = client.post("/chaos/duplicate-flood", json={"count": 1000})
        assert resp.status_code == 200
        body = resp.json()
        assert body["requested"] == 1000
        assert body["replayed"] > 0  # there was real sink history to replay

        # Give any admitted-but-not-suppressed stragglers time to flow
        # through queue -> worker -> sink; even those cannot create a new
        # row (same idempotency_key), so this is generous, not load-bearing
        # for the count assertion itself.
        _time.sleep(0.3)

        assert sink.count() == count_before, (
            f"sink grew from {count_before} to {sink.count()} after a "
            "duplicate flood — a duplicate delivery must never create a "
            "new row"
        )

        # The stronger claim: the NEW dedup-at-ingest layer is doing real
        # work, not just riding on the sink's own upsert safety net.
        frame = metrics.snapshot()
        assert frame.duplicates_caught > duplicates_before
        assert frame.duplicates_caught - duplicates_before == body["suppressed"]
        assert frame.exactly_once_violations == 0


def test_duplicate_flood_is_fake_mode_guarded():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/chaos/duplicate-flood", json={"count": 10})
    assert resp.status_code == 409


def test_duplicate_flood_rejects_a_non_positive_count():
    app = create_app(fake=False, seed=103)
    with TestClient(app) as client:
        resp = client.post("/chaos/duplicate-flood", json={"count": 0})
    assert resp.status_code == 422
