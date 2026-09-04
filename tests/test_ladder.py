"""ladder.py: rung caps, escalation, and the reservoir sampler."""

from __future__ import annotations

import pytest

from triage import ladder
from triage.contracts import Decision, Event, EventType, Tier
from triage.ladder import (
    HARD_SHED_PRESSURE,
    RESERVOIR_N,
    ReservoirSampler,
    Rung,
    cap,
    escalate,
)


def make_event(
    *, seq: int = 1, tier: Tier = Tier.P2, etype: EventType = EventType.CLICK,
    value: float = 5.0, cost: float = 0.5, ingest_ts: float = 1000.0, sla_seconds: float = 30.0,
) -> Event:
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=etype, tier=tier, payload_size=64, value=value, cost=cost,
        ingest_ts=ingest_ts, deadline_ts=ingest_ts + sla_seconds,
    )


@pytest.fixture(autouse=True)
def clean_state():
    ladder.reset_samplers()
    yield
    ladder.reset_samplers()


# --------------------------------------------------------------------------
# The rung ceiling — CLAUDE.md hard rule 3, enforced a second, independent
# way here regardless of what any escalation logic ever computes.
# --------------------------------------------------------------------------


def test_p0_caps_at_stream_no_matter_what_rung_is_requested():
    for rung in Rung:
        assert cap(Tier.P0, rung) == Rung.STREAM


def test_p1_caps_at_defer_never_sampled_or_shed():
    assert cap(Tier.P1, Rung.STREAM) == Rung.STREAM
    assert cap(Tier.P1, Rung.MICRO_BATCH) == Rung.MICRO_BATCH
    assert cap(Tier.P1, Rung.DEFER) == Rung.DEFER
    assert cap(Tier.P1, Rung.SAMPLE_ROLLUP) == Rung.DEFER
    assert cap(Tier.P1, Rung.SHED) == Rung.DEFER


def test_p2_is_uncapped_all_the_way_to_shed():
    for rung in Rung:
        assert cap(Tier.P2, rung) == rung


# --------------------------------------------------------------------------
# escalate() — P2 only, hard shed before sampling
# --------------------------------------------------------------------------


def test_escalate_does_nothing_below_shed_pressure_and_without_codel():
    decision, reason = escalate(Tier.P2, Decision.STREAM_NOW, 0.5, False)
    assert decision is Decision.STREAM_NOW
    assert reason is None


def test_escalate_hard_sheds_p2_above_the_pressure_threshold():
    decision, reason = escalate(Tier.P2, Decision.MICRO_BATCH, HARD_SHED_PRESSURE, False)
    assert decision is Decision.SHED
    assert reason is not None and "hard shed" in reason


def test_escalate_codel_sampling_wins_over_hard_shed():
    """"When CoDel signals, do NOT drop" is unconditional — a real
    sustained spike drives pressure to ~1.0 for most of its duration
    (Stage D's own 30s-spike test confirms this directly), so hard shed
    must not simply win by pressure being high: it is the fallback for
    when CoDel is NOT already sampling, not a competing priority."""
    decision, reason = escalate(Tier.P2, Decision.STREAM_NOW, HARD_SHED_PRESSURE, True)
    assert decision is Decision.SAMPLE_ROLLUP
    assert reason is not None and "CoDel" in reason


def test_escalate_samples_p2_when_codel_signals_below_shed_pressure():
    decision, reason = escalate(Tier.P2, Decision.STREAM_NOW, 0.5, True)
    assert decision is Decision.SAMPLE_ROLLUP
    assert reason is not None and "CoDel" in reason


def test_escalate_never_touches_p0_even_at_extreme_pressure_and_sampling():
    decision, reason = escalate(Tier.P0, Decision.STREAM_NOW, 1.0, True)
    assert decision is Decision.STREAM_NOW
    assert reason is None


def test_escalate_never_touches_p1_even_at_extreme_pressure_and_sampling():
    decision, reason = escalate(Tier.P1, Decision.DEFER, 1.0, True)
    assert decision is Decision.DEFER
    assert reason is None


def test_escalate_does_not_downgrade_an_already_sampled_or_shed_decision():
    """If a P2 event somehow already arrived as SAMPLE_ROLLUP (defensive —
    decide() itself never returns that), codel sampling being on must not
    matter (already at/above that rung) and hard shed still can escalate
    further."""
    decision, reason = escalate(Tier.P2, Decision.SAMPLE_ROLLUP, 0.5, True)
    assert decision is Decision.SAMPLE_ROLLUP
    assert reason is None  # already at that rung; nothing to escalate to


# --------------------------------------------------------------------------
# ReservoirSampler — 1 kept in N, exact reconstruction by construction
# --------------------------------------------------------------------------


def test_reservoir_returns_none_until_the_window_is_full():
    sampler = ReservoirSampler()
    for i in range(RESERVOIR_N - 1):
        assert sampler.add(make_event(seq=i), now=1000.0 + i) is None


def test_reservoir_emits_exactly_one_rollup_per_n_events():
    sampler = ReservoirSampler()
    rollup = None
    for i in range(RESERVOIR_N):
        rollup = sampler.add(make_event(seq=i + 1), now=1000.0 + i)
    assert rollup is not None
    assert rollup.observed_count == 1
    assert rollup.sample_weight == float(RESERVOIR_N)
    # Exact reconstruction: observed_count * sample_weight == N == the true
    # number of raw events this window actually covered.
    assert rollup.observed_count * rollup.sample_weight == RESERVOIR_N


def test_reservoir_rollup_seq_bounds_cover_every_event_in_the_window():
    sampler = ReservoirSampler()
    rollup = None
    seqs = list(range(100, 100 + RESERVOIR_N))
    for i, seq in enumerate(seqs):
        rollup = sampler.add(make_event(seq=seq), now=2000.0 + i)
    assert rollup.seq_low == min(seqs)
    assert rollup.seq_high == max(seqs)


def test_reservoir_window_resets_after_emitting():
    sampler = ReservoirSampler()
    for i in range(RESERVOIR_N):
        sampler.add(make_event(seq=i), now=1000.0 + i)
    # A fresh window: the next N-1 events produce nothing yet.
    for i in range(RESERVOIR_N - 1):
        assert sampler.add(make_event(seq=1000 + i), now=2000.0 + i) is None


def test_reservoir_subtype_counts_key_on_the_events_own_type():
    sampler = ReservoirSampler()
    rollup = None
    for i in range(RESERVOIR_N):
        rollup = sampler.add(make_event(seq=i, etype=EventType.CLICK), now=1000.0 + i)
    assert rollup.event_type == "click"
    assert rollup.subtype_counts == {"click": 1}


def test_reservoir_reset_discards_a_partial_window():
    sampler = ReservoirSampler()
    sampler.add(make_event(seq=1), now=1000.0)
    sampler.reset()
    for i in range(RESERVOIR_N - 1):
        assert sampler.add(make_event(seq=100 + i), now=2000.0 + i) is None


# --------------------------------------------------------------------------
# add_to_reservoir() — routes by event type, click and log independent
# --------------------------------------------------------------------------


def test_add_to_reservoir_keeps_click_and_log_independent():
    for i in range(RESERVOIR_N - 1):
        assert ladder.add_to_reservoir(make_event(seq=i, etype=EventType.CLICK), now=1000.0) is None
    # A log event in between must not push the click reservoir over the top.
    assert ladder.add_to_reservoir(make_event(seq=999, etype=EventType.LOG), now=1000.0) is None
    # The click reservoir still only needed one more click to complete.
    rollup = ladder.add_to_reservoir(make_event(seq=RESERVOIR_N, etype=EventType.CLICK), now=1000.0)
    assert rollup is not None
    assert rollup.event_type == "click"


def test_add_to_reservoir_rejects_a_non_p2_type():
    with pytest.raises(ValueError):
        ladder.add_to_reservoir(make_event(etype=EventType.PAYMENT), now=1000.0)


def test_reset_samplers_clears_both_click_and_log():
    for i in range(RESERVOIR_N - 1):
        ladder.add_to_reservoir(make_event(seq=i, etype=EventType.CLICK), now=1000.0)
    ladder.reset_samplers()
    for i in range(RESERVOIR_N - 1):
        assert ladder.add_to_reservoir(make_event(seq=100 + i, etype=EventType.CLICK), now=2000.0) is None
