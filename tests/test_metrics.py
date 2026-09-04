"""The five observation points, and the percentiles they feed.

Percentiles are the one thing in metrics.py that is real in Stage A, so they
are tested against hand-computed answers rather than against themselves.
"""

from __future__ import annotations

import time

import pytest

from triage import deferral, ledger, metrics
from triage.contracts import Decision, Event, EventType, Mode, Tier


@pytest.fixture(autouse=True)
def clean_registry():
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()
    yield
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()


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
    """deferred_pending is deliberately not asserted here: as of Stage D it
    is sourced live from deferral.pending_count() (see observe_replay's own
    docstring for why), not from observe_decision — a DEFER decision being
    *recorded* here is a separate concern from an event actually being
    *stored* in the deferred buffer, which nothing in this test does. See
    tests/test_deferral.py for deferred_pending's own coverage."""
    metrics.observe_decision(event(seq=1), Decision.SHED, "below shed line", 1.4)
    metrics.observe_decision(event(seq=2), Decision.SAMPLE_ROLLUP, "duplicate", 1.2)
    metrics.observe_decision(event(seq=3), Decision.DEFER, "deadline distant", 1.1)
    metrics.observe_decision(event(seq=4), Decision.STREAM_NOW, "headroom", 0.2)

    frame = metrics.snapshot()
    assert (frame.shed, frame.sampled_out) == (1, 1)
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


# --------------------------------------------------------------------------
# mode: mirrored from the queue, so the dashboard's label is never a lie
# --------------------------------------------------------------------------


def test_mode_defaults_to_adaptive():
    assert metrics.get_mode() is Mode.ADAPTIVE
    assert metrics.snapshot().mode is Mode.ADAPTIVE


def test_set_mode_is_reflected_in_the_next_snapshot():
    metrics.set_mode(Mode.NAIVE)
    assert metrics.snapshot().mode is Mode.NAIVE
    metrics.set_mode(Mode.ADAPTIVE)  # restore: reset() is what tests rely on


def test_reset_restores_mode_to_adaptive():
    """reset() is a full wipe, mode included — Engine.reset() (app.py) is
    the one place that needs mode preserved across a reset, and it does
    that itself by re-applying set_mode() right after."""
    metrics.set_mode(Mode.NAIVE)
    metrics.reset()
    assert metrics.get_mode() is Mode.ADAPTIVE


# --------------------------------------------------------------------------
# Stage C acceptance: a blown P0 latency must never be hidden by P2
# --------------------------------------------------------------------------


def test_a_blown_p0_latency_is_not_hidden_by_many_healthy_p2_samples():
    """The demo's whole premise. If latency were ever pooled across tiers —
    or if P0's own reading could be dragged down by a flood of good P2
    numbers — a jury watching the P0 scoreboard could see green while P0 is
    actually breaching its SLA. Prove the opposite: a hundred healthy P2
    completions cannot move P0's p99 by a millisecond, in either direction.
    """
    base = time.time()

    # A hundred fast, healthy P2 completions (well inside their 30s SLA).
    for i in range(100):
        metrics.observe_complete(
            event(seq=i, tier=Tier.P2, etype=EventType.CLICK, ingest_ts=base, sla=30.0),
            now=base + 0.010,
        )

    # One P0 event that has badly blown its 200ms-class SLA.
    blown = event(seq=999, tier=Tier.P0, etype=EventType.PAYMENT, ingest_ts=base, sla=0.2)
    metrics.observe_complete(blown, now=base + 5.0)

    frame = metrics.snapshot()

    # P0's own reading reflects the blown event, full stop — not "mostly
    # fine because most events were fine."
    assert frame.latency_p50["P0"] == pytest.approx(5000.0, abs=1.0)
    assert frame.latency_p99["P0"] == pytest.approx(5000.0, abs=1.0)

    # And the flood of P0 badness (if it existed) could not run the other
    # direction either: P2's reading stays healthy, untouched by P0.
    assert frame.latency_p99["P2"] < 50.0

    # The SLA miss is independently visible too, not just inferable from a
    # latency number a dashboard might round away.
    assert frame.sla_missed["P0"] == 1
    assert frame.sla_met["P2"] == 100


def test_p0_and_p2_percentiles_are_computed_from_disjoint_sample_sets():
    """A structural guarantee, not just a plausible-looking number: P0's
    percentile can only ever be computed from P0 samples."""
    base = time.time()
    for i in range(20):
        metrics.observe_complete(
            event(seq=i, tier=Tier.P2, ingest_ts=base), now=base + 0.005
        )
    # No P0 samples at all yet.
    assert metrics.snapshot().latency_p99["P0"] == 0.0

    metrics.observe_complete(
        event(seq=100, tier=Tier.P0, etype=EventType.ORDER, ingest_ts=base, sla=0.5),
        now=base + 2.0,
    )
    frame = metrics.snapshot()
    assert frame.latency_p99["P0"] == pytest.approx(2000.0, abs=1.0)
    assert frame.latency_p99["P2"] < 50.0  # unmoved by the single P0 sample


# --------------------------------------------------------------------------
# Stage D: real pressure, wired from live signals
# --------------------------------------------------------------------------


def test_pressure_is_zero_on_a_freshly_reset_registry():
    assert metrics.snapshot().pressure == 0.0


def test_pressure_rises_as_queue_depth_grows():
    base = time.time()
    quiet = metrics.current_pressure(now=base)
    for i in range(300):
        metrics.observe_ingest(event(seq=i, tier=Tier.P2, ingest_ts=base), now=base)
    # Force past the refresh throttle so the new depth is actually reflected.
    loaded = metrics.current_pressure(now=base + 0.1)
    assert loaded > quiet


def test_pressure_is_throttled_between_refreshes():
    """current_pressure() must not recompute (and re-sort/re-percentile)
    on every call at spike rate — see the throttle's own docstring for why
    that specific mistake already happened once in this project."""
    base = time.time()
    first = metrics.current_pressure(now=base)
    for i in range(500):
        metrics.observe_ingest(event(seq=i, tier=Tier.P1, ingest_ts=base), now=base)
    # Within the throttle window: the cached value, not a fresh recompute.
    still_cached = metrics.current_pressure(now=base + 0.001)
    assert still_cached == first


def test_worker_count_and_active_workers_are_real_in_the_frame():
    from triage.config import load_config

    cfg = load_config()
    base = time.time()
    ev = event(seq=1, tier=Tier.P1, ingest_ts=base)
    metrics.observe_dequeue(ev, now=base)

    frame = metrics.snapshot()
    assert frame.worker_count == cfg.worker_count
    assert frame.active_workers == 1


def test_reset_clears_the_rate_ewmas_and_pressure_cache():
    base = time.time()
    for i in range(50):
        metrics.observe_ingest(event(seq=i, tier=Tier.P2, ingest_ts=base), now=base)
    assert metrics.current_pressure(now=base + 0.1) >= 0.0  # populate the cache

    metrics.reset()

    assert metrics._arrival_rate_ewma.level == 0.0
    assert metrics._service_rate_ewma.level == 0.0
    assert metrics.snapshot().pressure == 0.0


def test_ewma_ignores_a_single_point_then_tracks_a_steady_rate():
    ewma = metrics._Ewma(half_life_seconds=1.0)
    t0 = time.time()
    ewma.observe_amount(10.0, t0)
    assert ewma.level == 0.0  # first point only anchors the clock

    # A steady 10 units/sec, fed once a second for a while, should converge
    # toward 10 - not explode, not stay at zero.
    for i in range(1, 12):
        ewma.observe_amount(10.0, t0 + i * 1.0)
    assert ewma.level == pytest.approx(10.0, rel=0.05)


def test_ewma_with_trend_reflects_a_rising_rate():
    ewma = metrics._Ewma(half_life_seconds=1.0)
    t0 = time.time()
    ewma.observe_amount(5.0, t0)
    for i in range(1, 6):
        ewma.observe_amount(5.0, t0 + i * 1.0)  # steady at 5/s
    steady_level = ewma.level

    ewma.observe_amount(50.0, t0 + 6.0)  # a sudden burst
    assert ewma.level > steady_level
    assert ewma.trend > 0
    assert ewma.with_trend > ewma.level  # leans into the ramp, not just behind it
