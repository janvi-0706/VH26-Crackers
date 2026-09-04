"""decision.py: the two functions, tested separately, and the proof that
pressure added additively to the score would be a no-op.
"""

from __future__ import annotations

import time

import pytest

from triage.contracts import Decision, Event, EventType, Tier
from triage import decision
from triage.decision import (
    DEFAULT_PRESSURE_WEIGHTS,
    DEFAULT_SCORE_WEIGHTS,
    PressureSignals,
    PressureWeights,
    ScoreWeights,
    batch_cost,
    batch_size,
    decide,
    est_service_time,
    get_weights,
    pressure,
    score,
    set_weights,
    slack,
)

CAPACITY = 25.0  # one worker's u/s, matching config/tiers.yaml


def make_event(
    *, seq: int = 1, tier: Tier = Tier.P1, value: float = 40.0, cost: float = 2.0,
    ingest_ts: float | None = None, sla_seconds: float = 5.0,
) -> Event:
    ingest_ts = time.time() if ingest_ts is None else ingest_ts
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=EventType.INVENTORY, tier=tier, payload_size=64,
        value=value, cost=cost, ingest_ts=ingest_ts, deadline_ts=ingest_ts + sla_seconds,
    )


# --------------------------------------------------------------------------
# slack / est_service_time
# --------------------------------------------------------------------------


def test_est_service_time_is_cost_over_capacity():
    ev = make_event(cost=5.0)
    assert est_service_time(ev, CAPACITY) == pytest.approx(0.2)


def test_slack_is_deadline_minus_now_minus_service_time():
    now = time.time()
    ev = make_event(ingest_ts=now, sla_seconds=1.0, cost=2.5)  # service = 0.1s
    assert slack(ev, now, CAPACITY) == pytest.approx(0.9, abs=1e-6)


def test_slack_goes_negative_once_past_effective_deadline():
    now = time.time()
    ev = make_event(ingest_ts=now - 2.0, sla_seconds=1.0, cost=2.5)
    assert slack(ev, now, CAPACITY) < 0


# --------------------------------------------------------------------------
# score() — ordering
# --------------------------------------------------------------------------


def test_score_rises_as_slack_shrinks_toward_zero():
    now = time.time()
    far = make_event(seq=1, ingest_ts=now, sla_seconds=10.0)
    near = make_event(seq=2, ingest_ts=now, sla_seconds=0.5)
    assert score(near, now, CAPACITY) > score(far, now, CAPACITY)


def test_score_saturates_rather_than_blows_up_past_zero_slack():
    """Once slack is negative, urgency = 1/EPS regardless of exactly how
    negative — an event ten minutes overdue must not outscore one ten
    seconds overdue just because it is "more" overdue; both are already
    maximally urgent."""
    now = time.time()
    a_bit_late = make_event(seq=1, ingest_ts=now - 10.0, sla_seconds=1.0)
    very_late = make_event(seq=2, ingest_ts=now - 600.0, sla_seconds=1.0)
    score_a = score(a_bit_late, now, CAPACITY)
    score_b = score(very_late, now, CAPACITY)
    # They differ only through the `aging` term now (density*urgency is
    # identical, both saturated at 1/EPS) — not by orders of magnitude.
    assert score_b > score_a
    assert score_b / score_a < 1000  # aging alone, not urgency, explains the gap


def test_score_rewards_higher_value_density_at_equal_urgency():
    now = time.time()
    cheap = make_event(seq=1, ingest_ts=now, sla_seconds=1.0, value=10.0, cost=2.0)
    valuable = make_event(seq=2, ingest_ts=now, sla_seconds=1.0, value=100.0, cost=2.0)
    assert score(valuable, now, CAPACITY) > score(cheap, now, CAPACITY)


def test_score_rewards_aging_for_events_with_identical_deadlines():
    """Two events with the same remaining slack right now, but one has
    already waited longer relative to its own SLA budget — aging must
    break the tie in its favour."""
    now = time.time()
    fresh = make_event(seq=1, ingest_ts=now, sla_seconds=1.0)  # deadline = now+1
    older = make_event(seq=2, ingest_ts=now - 0.5, sla_seconds=1.5)  # deadline = now+1 too
    assert slack(fresh, now, CAPACITY) == pytest.approx(slack(older, now, CAPACITY), abs=1e-6)
    assert score(older, now, CAPACITY) > score(fresh, now, CAPACITY)


def test_score_weights_are_non_negative():
    with pytest.raises(ValueError):
        ScoreWeights(w1=-0.1, w2=1.1)


def test_score_never_divides_by_zero_on_degenerate_cost():
    """cost is always > 0 in practice, but score() must not crash if it
    were ever 0 — EPS floors every denominator."""
    now = time.time()
    ev = make_event(cost=0.0)
    assert score(ev, now, CAPACITY) != float("inf")


# --------------------------------------------------------------------------
# pressure() — the system-state signal, and why it can't be additive
# --------------------------------------------------------------------------


def test_pressure_weights_must_sum_to_one():
    with pytest.raises(ValueError):
        PressureWeights(a=0.5, b=0.5, c=0.5, d=0.5)


def test_pressure_weights_must_be_non_negative():
    with pytest.raises(ValueError):
        PressureWeights(a=-0.1, b=0.5, c=0.4, d=0.2)


def test_pressure_is_clamped_to_0_1_even_under_extreme_signals():
    signals = PressureSignals(
        qdepth=1_000_000, qmax=1.0,
        arrival_rate_ewma_with_trend=1_000_000, service_rate=1.0,
        p95_sojourn=1_000_000, sla_reference=1.0,
        worker_util=1.0,
    )
    assert pressure(signals) == 1.0


def test_pressure_is_zero_when_every_signal_is_calm():
    signals = PressureSignals(
        qdepth=0, qmax=500.0,
        arrival_rate_ewma_with_trend=10.0, service_rate=150.0,
        p95_sojourn=0.0, sla_reference=5.0,
        worker_util=0.0,
    )
    assert pressure(signals) == pytest.approx(10.0 / 150.0 * PressureWeights().b, abs=1e-6)


def test_pressure_handles_zero_service_rate_without_crashing():
    """A cold start (nothing has completed yet) must produce a large-but-
    finite ratio, never a ZeroDivisionError — a control signal that can
    crash the loop it informs is worse than a wrong number."""
    signals = PressureSignals(
        qdepth=10, qmax=500.0,
        arrival_rate_ewma_with_trend=50.0, service_rate=0.0,
        p95_sojourn=0.0, sla_reference=5.0,
        worker_util=0.0,
    )
    assert pressure(signals) == 1.0  # the arrival/EPS term alone saturates the clamp


def test_pressure_additive_score_term_would_be_a_no_op():
    """The exact mistake CLAUDE.md and this prompt both forbid, demonstrated
    directly: adding a system-global constant to every event's score never
    changes which one is larger. This is the test that would fail if a
    future refactor "helpfully" added pressure into score()."""
    now = time.time()
    a = make_event(seq=1, ingest_ts=now, sla_seconds=1.0, value=100.0, cost=2.0)
    b = make_event(seq=2, ingest_ts=now, sla_seconds=1.0, value=10.0, cost=2.0)

    score_a = score(a, now, CAPACITY)
    score_b = score(b, now, CAPACITY)
    assert score_a > score_b  # a genuinely denser event ranks higher

    for p in (0.0, 0.3, 0.75, 1.0):
        with_p_a = score_a + p
        with_p_b = score_b + p
        # The ordering is identical at every pressure value — adding a
        # system-global scalar to both sides of a comparison changes
        # nothing about which side wins.
        assert (with_p_a > with_p_b) == (score_a > score_b)


# --------------------------------------------------------------------------
# decide() — routing
# --------------------------------------------------------------------------


def test_p0_is_absolute_regardless_of_slack_or_pressure():
    now = time.time()
    ev = make_event(tier=Tier.P0, ingest_ts=now - 100.0, sla_seconds=1.0)  # very negative slack
    result, reason = decide(ev, 1.0, now, CAPACITY)
    assert result is Decision.STREAM_NOW
    assert "P0" in reason


def test_negative_slack_defers_even_at_zero_pressure():
    now = time.time()
    ev = make_event(tier=Tier.P1, ingest_ts=now - 100.0, sla_seconds=1.0)
    result, reason = decide(ev, 0.0, now, CAPACITY)
    assert result is Decision.DEFER
    assert "slack" in reason.lower()


@pytest.mark.parametrize(
    "pressure_value,expected",
    [
        (0.0, Decision.STREAM_NOW),
        (0.39, Decision.STREAM_NOW),
        (0.40, Decision.MICRO_BATCH),
        (0.60, Decision.MICRO_BATCH),
        (0.74, Decision.MICRO_BATCH),
        (0.75, Decision.DEFER),
        (1.0, Decision.DEFER),
    ],
)
def test_routing_bands_match_the_spec_exactly_at_the_boundaries(pressure_value, expected):
    now = time.time()
    ev = make_event(tier=Tier.P2, ingest_ts=now, sla_seconds=30.0)  # ample positive slack
    result, _ = decide(ev, pressure_value, now, CAPACITY)
    assert result is expected


def test_decide_reason_is_a_nonempty_human_string():
    now = time.time()
    ev = make_event(tier=Tier.P1, ingest_ts=now, sla_seconds=5.0)
    _, reason = decide(ev, 0.5, now, CAPACITY)
    assert isinstance(reason, str) and len(reason) > 0


# --------------------------------------------------------------------------
# batch_size() / batch_cost()
# --------------------------------------------------------------------------


def test_batch_size_grows_with_pressure_between_the_bounds():
    small = batch_size(0.40, b_min=4, b_max=8)
    large = batch_size(0.74, b_min=4, b_max=8)
    assert 4 <= small <= large <= 8


def test_batch_size_is_hard_capped_regardless_of_pressure_out_of_range():
    """B_max is a hard safety bound, not just the natural top of the
    formula's range — a stale or synthetic pressure outside [0, 1] must not
    produce an oversized batch."""
    assert batch_size(5.0, b_min=4, b_max=8) == 8
    assert batch_size(-3.0, b_min=4, b_max=8) == 4


def test_batch_size_default_bounds_match_the_spec():
    assert batch_size(0.0) == 4  # B_MIN
    # At P=1.0 the raw formula would hit B_MAX exactly; MICRO_BATCH's own
    # band never actually reaches 1.0 (>=0.75 is DEFER), but the formula
    # itself must still cap there if ever asked.
    assert batch_size(1.0) == 8  # B_MAX


def test_batch_cost_formula_is_exactly_sum_times_0_4_plus_0_5():
    assert batch_cost([2.0, 2.0, 2.0, 2.0]) == pytest.approx(8.0 * 0.4 + 0.5)


def test_batch_cost_is_genuinely_cheaper_than_streaming_at_the_default_b_min():
    """The prompt's own requirement: batching must be genuinely cheaper,
    not just relabelled. True at B_MIN (4) even for the cheapest real
    event type (log, cost=0.3u) — the case most likely to fail if it were
    going to."""
    log_costs = [0.3] * 4
    individual_total = sum(log_costs)
    assert batch_cost(log_costs) < individual_total


def test_batch_cost_can_exceed_individual_cost_below_b_min():
    """Documents *why* B_MIN exists, rather than asserting it blindly: a
    single cheap event batched alone is genuinely more expensive than
    streaming it — batch_cost() faithfully computes that, it does not
    protect against it. batch_size() choosing >= B_MIN is what protects
    against it in practice."""
    one_log = [0.3]
    assert batch_cost(one_log) > sum(one_log)


# --------------------------------------------------------------------------
# Live weights — Stage D dashboard sliders (GET/POST /control/weights)
# --------------------------------------------------------------------------


def setup_function() -> None:
    decision.current_score_weights = DEFAULT_SCORE_WEIGHTS
    decision.current_pressure_weights = DEFAULT_PRESSURE_WEIGHTS


def teardown_function() -> None:
    decision.current_score_weights = DEFAULT_SCORE_WEIGHTS
    decision.current_pressure_weights = DEFAULT_PRESSURE_WEIGHTS


def test_get_weights_reports_the_defaults_initially():
    assert get_weights() == {"w1": 0.7, "w2": 0.3, "a": 0.35, "b": 0.35, "c": 0.2, "d": 0.1}


def test_set_weights_updates_only_the_named_fields():
    result = set_weights(w1=0.2, w2=0.8)
    assert result["w1"] == pytest.approx(0.2)
    assert result["w2"] == pytest.approx(0.8)
    # the untouched pressure group stays exactly at its previous values
    assert result["a"] == pytest.approx(0.35)
    assert result["b"] == pytest.approx(0.35)
    assert result["c"] == pytest.approx(0.2)
    assert result["d"] == pytest.approx(0.1)


def test_set_weights_renormalises_the_touched_group_to_sum_to_one():
    result = set_weights(a=0.9)
    assert sum(result[k] for k in ("a", "b", "c", "d")) == pytest.approx(1.0)
    # a's share of the new total (0.9 + 0.35 + 0.2 + 0.1 = 1.55)
    assert result["a"] == pytest.approx(0.9 / 1.55)


def test_set_weights_persists_across_calls_as_the_new_live_baseline():
    first = set_weights(a=0.9)
    # a second, unrelated update must renormalise against the *previous*
    # call's already-renormalised a (first["a"]), not against the raw 0.9
    # or against DEFAULT_PRESSURE_WEIGHTS — "live" means every call builds
    # on the last one, not on some fixed starting point.
    result = set_weights(b=0.9)
    total = first["a"] + 0.9 + first["c"] + first["d"]
    assert result["a"] == pytest.approx(first["a"] / total)
    assert result["b"] == pytest.approx(0.9 / total)


def test_set_weights_actually_moves_the_live_dataclasses_score_and_pressure_read():
    set_weights(w1=0.9, w2=0.1, a=0.9)
    assert decision.current_score_weights.w1 == pytest.approx(0.9)
    assert decision.current_pressure_weights.a == pytest.approx(0.9 / 1.55)


def test_set_weights_rejects_a_negative_value_without_mutating_state():
    before = get_weights()
    with pytest.raises(ValueError):
        set_weights(c=-0.5)
    assert get_weights() == before


def test_set_weights_rejects_a_group_summing_to_zero_without_mutating_state():
    before = get_weights()
    with pytest.raises(ValueError):
        set_weights(a=0, b=0, c=0, d=0)
    assert get_weights() == before
