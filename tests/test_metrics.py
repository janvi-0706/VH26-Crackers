"""The five observation points, and the percentiles they feed.

Percentiles are the one thing in metrics.py that is real in Stage A, so they
are tested against hand-computed answers rather than against themselves.
"""

from __future__ import annotations

import time

import pytest

from triage import ledger, metrics
from triage.contracts import Decision, Event, EventType, Tier


@pytest.fixture(autouse=True)
def clean_registry():
    metrics.reset()
    ledger.reset()
    yield
    metrics.reset()
    ledger.reset()


def event(seq: int = 1, tier: Tier = Tier.P2, etype: EventType = EventType.CLICK,
          ingest_ts: float | None = None, sla: float = 30.0) -> Event:
    ingest_ts = time.time() if ingest_ts is None else ingest_ts
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="cust-1", idempotency_key=f"ik-{seq}",
        type=etype, tier=tier, payload_size=128, value=5.0, cost=0.5,
        ingest_ts=ingest_ts, deadline_ts=ingest_ts + sla,
    )


# --------------------------------------------------------------------------
# Percentiles
# --------------------------------------------------------------------------


def test_percentile_of_empty_window_is_zero():
    assert metrics.percentile([], 0.99) == 0.0


def test_percentile_of_single_sample_is_that_sample():
    assert metrics.percentile([42.0], 0.5) == 42.0
    assert metrics.percentile([42.0], 0.99) == 42.0


def test_percentile_interpolates_between_ranks():
    samples = [1.0, 2.0, 3.0, 4.0]
    assert metrics.percentile(samples, 0.0) == 1.0
    assert metrics.percentile(samples, 1.0) == 4.0
    assert metrics.percentile(samples, 0.5) == 2.5  # midway between 2 and 3
    assert metrics.percentile(samples, 0.25) == pytest.approx(1.75)


def test_percentile_ignores_input_order():
    ordered = list(range(1, 101))
    shuffled = ordered[50:] + ordered[:50]
    assert metrics.percentile(shuffled, 0.95) == metrics.percentile(ordered, 0.95)
    assert metrics.percentile(ordered, 0.95) == pytest.approx(95.05)


def test_latency_window_is_bounded():
    """A 30-hour run must not grow memory. Oldest samples fall out."""
    base = time.time()
    for i in range(metrics.WINDOW + 500):
        ev = event(seq=i, ingest_ts=base)
        metrics.observe_dequeue(ev, now=base)
        metrics.observe_complete(ev, now=base + 0.01)
    assert len(metrics._latency_ms["P2"]) == metrics.WINDOW


# --------------------------------------------------------------------------
# Observation points
# --------------------------------------------------------------------------


def test_ingest_dequeue_complete_moves_the_event_through_the_counters():
    base = time.time()
    ev = event(ingest_ts=base)

    metrics.observe_ingest(ev)
    frame = metrics.snapshot()
    assert (frame.ingested, frame.in_queue, frame.in_flight, frame.processed) == (1, 1, 0, 0)
    assert frame.queue_depth["P2"] == 1

    metrics.observe_dequeue(ev, now=base + 0.05)
    frame = metrics.snapshot()
    assert (frame.in_queue, frame.in_flight, frame.processed) == (0, 1, 0)
    assert frame.queue_depth["P2"] == 0

    metrics.observe_complete(ev, now=base + 0.2)
    frame = metrics.snapshot()
    assert (frame.in_queue, frame.in_flight, frame.processed) == (0, 0, 1)
    assert frame.latency_p50["P2"] == pytest.approx(200.0, abs=1.0)


def test_latency_is_measured_from_ingest_not_from_dequeue():
    """Measuring from dequeue would hide the queue wait — the exact number the
    demo is about."""
    base = time.time()
    ev = event(ingest_ts=base)
    metrics.observe_dequeue(ev, now=base + 4.0)
    metrics.observe_complete(ev, now=base + 4.5)
    frame = metrics.snapshot()
    assert frame.latency_p50["P2"] == pytest.approx(4500.0, abs=1.0)
    assert metrics.queue_wait_percentile(Tier.P2, 0.5) == pytest.approx(4000.0, abs=1.0)


def test_sla_attainment_splits_on_the_deadline():
    base = time.time()
    on_time = event(seq=1, ingest_ts=base, sla=1.0)
    late = event(seq=2, ingest_ts=base, sla=1.0)
    metrics.observe_complete(on_time, now=base + 0.5)
    metrics.observe_complete(late, now=base + 2.0)
    frame = metrics.snapshot()
    assert frame.sla_met["P2"] == 1
    assert frame.sla_missed["P2"] == 1
    assert frame.value_delivered == pytest.approx(5.0)


def test_percentiles_are_reported_per_tier_independently():
    base = time.time()
    fast = event(seq=1, tier=Tier.P0, etype=EventType.ORDER, ingest_ts=base)
    slow = event(seq=2, tier=Tier.P2, ingest_ts=base)
    metrics.observe_complete(fast, now=base + 0.01)
    metrics.observe_complete(slow, now=base + 5.0)
    frame = metrics.snapshot()
    assert frame.latency_p99["P0"] < 50.0
    assert frame.latency_p99["P2"] > 4000.0
    assert frame.latency_p99["P1"] == 0.0  # nothing observed, not a gap


def test_decision_counters_and_narrative():
    metrics.observe_decision(event(seq=1), Decision.SHED, "below shed line", 1.4)
    metrics.observe_decision(event(seq=2), Decision.SAMPLE_ROLLUP, "duplicate", 1.2)
    metrics.observe_decision(event(seq=3), Decision.DEFER, "deadline distant", 1.1)
    metrics.observe_decision(event(seq=4), Decision.STREAM_NOW, "headroom", 0.2)

    frame = metrics.snapshot()
    assert (frame.shed, frame.sampled_out, frame.deferred_pending) == (1, 1, 1)
    assert frame.value_shed == pytest.approx(5.0)
    assert len(frame.recent_decisions) == 4
    assert frame.recent_decisions[0].seq == 4, "newest decision comes first"
    assert len(frame.recent_sheds) == 1
    assert frame.recent_sheds[0].reason == "below shed line"


def test_every_decision_writes_exactly_one_ledger_row():
    """The point of routing decisions through one choke point: no path can
    reach a verdict without leaving an audit row."""
    for i, decision in enumerate(Decision, start=1):
        metrics.observe_decision(event(seq=i), decision, "reason", 0.9)
    assert ledger.total_recorded() == len(Decision)
    rows = list(ledger.records())
    assert [r["decision"] for r in rows] == [d.value for d in Decision]
    assert all(r["tier"] == "P2" for r in rows)


def test_recent_lists_are_bounded():
    for i in range(metrics.RECENT * 3):
        metrics.observe_decision(event(seq=i), Decision.SHED, "flood", 1.5)
    frame = metrics.snapshot()
    assert len(frame.recent_decisions) == metrics.RECENT
    assert len(frame.recent_sheds) == metrics.RECENT


def test_snapshot_is_always_a_valid_frame_even_when_nothing_happened():
    frame = metrics.snapshot()
    assert frame.ingested == 0
    assert frame.latency_p99["P0"] == 0.0
    assert frame.exactly_once_violations == 0
