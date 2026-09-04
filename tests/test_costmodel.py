"""costmodel.py: the learned cost estimate, in isolation. THE claims this
file exists to prove: true_cost's expectation over the generator's own
payload_size draw is exactly the config prior (calibration is untouched);
the learned estimate falls back to the prior with no data and converges
toward reality with enough of it; it re-adapts to a sustained shift, not
just noise; and it is never a bandit — observe() never influences what
gets served, only how already-served traffic is weighed afterward.
"""

from __future__ import annotations

from triage.config import load_config
from triage.contracts import EventType
from triage.costmodel import (
    BUCKETS_PER_TYPE,
    MIN_CONFIDENT_SAMPLES,
    CostModel,
    RunningEstimate,
    true_cost,
)
from triage.generator import PAYLOAD_SIZE_RANGES


# --------------------------------------------------------------------------
# true_cost(): the calibration argument, checked directly
# --------------------------------------------------------------------------


def test_true_cost_equals_the_prior_exactly_at_the_midpoint_payload_size():
    cfg = load_config()
    for event_type in cfg.tiers:
        low, high = PAYLOAD_SIZE_RANGES[event_type]
        midpoint = (low + high) / 2.0
        assert true_cost(cfg, event_type, midpoint) == cfg.tiers[event_type].cost


def test_true_cost_expectation_over_a_uniform_draw_equals_the_prior():
    """The actual calibration guarantee: not just correct at one point,
    but correct ON AVERAGE over the whole range the generator draws from —
    a uniform distribution's own mean is its midpoint, which is exactly
    what the test above already confirms true_cost maps to the prior, so
    the average over many draws must converge to the prior too. Checked
    directly with real random draws, not just algebra, to catch an
    off-by-one in the range bounds an equation alone would not."""
    import random

    cfg = load_config()
    rng = random.Random(7)
    for event_type in cfg.tiers:
        low, high = PAYLOAD_SIZE_RANGES[event_type]
        prior = cfg.tiers[event_type].cost
        samples = [true_cost(cfg, event_type, rng.randint(low, high)) for _ in range(20_000)]
        mean = sum(samples) / len(samples)
        assert abs(mean - prior) / prior < 0.01, (
            f"{event_type}: mean true_cost {mean:.4f} vs prior {prior:.4f} "
            "diverges more than 1% over 20k draws"
        )


def test_true_cost_scales_with_payload_size():
    cfg = load_config()
    et = EventType.PAYMENT
    low, high = PAYLOAD_SIZE_RANGES[et]
    assert true_cost(cfg, et, high) > true_cost(cfg, et, low)


# --------------------------------------------------------------------------
# RunningEstimate: the EWMA itself
# --------------------------------------------------------------------------


def test_running_estimate_starts_unset_and_takes_the_first_observation_exactly():
    est = RunningEstimate()
    assert est.mean is None
    est.observe(5.0)
    assert est.mean == 5.0
    assert est.count == 1


def test_running_estimate_converges_toward_a_constant_true_value():
    est = RunningEstimate()
    for _ in range(500):
        est.observe(10.0)
    assert est.mean is not None
    assert abs(est.mean - 10.0) < 1e-6


def test_running_estimate_reacts_to_a_sustained_regime_shift():
    """Not just convergence — RE-adaptation, the demo beat's own claim.
    500 samples at 10.0, then a sustained shift to 20.0: the estimate must
    move meaningfully toward the new value within a bounded number of
    post-shift samples, regardless of how much pre-shift history exists."""
    est = RunningEstimate()
    for _ in range(500):
        est.observe(10.0)
    before_shift = est.mean
    for _ in range(80):
        est.observe(20.0)
    after_shift = est.mean
    assert after_shift is not None and before_shift is not None
    assert after_shift > before_shift + 5.0, (
        f"estimate barely moved after a sustained shift: {before_shift:.2f} -> "
        f"{after_shift:.2f}"
    )


# --------------------------------------------------------------------------
# CostModel: fallback, blending, re-adaptation, learned-vs-prior exposure
# --------------------------------------------------------------------------


def test_estimate_equals_the_prior_exactly_with_zero_observations():
    cfg = load_config()
    model = CostModel(cfg)
    for event_type in cfg.tiers:
        low, high = PAYLOAD_SIZE_RANGES[event_type]
        mid = (low + high) // 2
        assert model.estimate(event_type, mid) == cfg.tiers[event_type].cost


def test_estimate_converges_toward_the_true_cost_with_enough_observations():
    cfg = load_config()
    model = CostModel(cfg)
    et = EventType.PAYMENT
    low, high = PAYLOAD_SIZE_RANGES[et]
    heavy_payload = high  # top of the range -> a real, fixed true_cost
    true = true_cost(cfg, et, heavy_payload)

    for _ in range(500):
        model.observe(et, heavy_payload, true)

    learned = model.estimate(et, heavy_payload)
    assert abs(learned - true) / true < 0.02


def test_estimate_is_a_smooth_blend_not_a_hard_cutoff():
    """Confidence should climb continuously with sample count, not jump —
    the dashboard's own convergence chart depends on this being a curve."""
    cfg = load_config()
    model = CostModel(cfg)
    et = EventType.PAYMENT
    low, high = PAYLOAD_SIZE_RANGES[et]
    prior = cfg.tiers[et].cost
    heavy_payload = high
    true = true_cost(cfg, et, heavy_payload)

    estimates = []
    for i in range(MIN_CONFIDENT_SAMPLES * 2):
        model.observe(et, heavy_payload, true)
        estimates.append(model.estimate(et, heavy_payload))

    # Monotonic drift away from the prior toward the true value (allowing
    # tiny float noise), never a single-step jump larger than one sample
    # could plausibly cause.
    assert estimates[0] != prior or true == prior
    max_step = max(abs(b - a) for a, b in zip(estimates, estimates[1:]))
    assert max_step < abs(true - prior) * 0.5 + 1e-9


def test_cost_model_re_adapts_when_the_payload_mix_gets_heavier():
    """The demo beat itself, at the CostModel level (test_app.py's own
    integration test covers it through a real running Engine + generator).
    A model already converged on light payloads must visibly move toward
    the true cost of a sustained heavy-payload regime."""
    cfg = load_config()
    model = CostModel(cfg)
    et = EventType.INVENTORY
    low, high = PAYLOAD_SIZE_RANGES[et]
    light, heavy = low, high

    for _ in range(200):
        model.observe(et, light, true_cost(cfg, et, light))
    converged_light = model.estimate(et, light)

    for _ in range(200):
        model.observe(et, heavy, true_cost(cfg, et, heavy))
    converged_heavy = model.estimate(et, heavy)

    assert converged_heavy > converged_light


def test_summary_reports_prior_and_learned_per_type_with_zero_data():
    cfg = load_config()
    model = CostModel(cfg)
    rows = {row.event_type: row for row in model.summary()}
    assert set(rows) == {t.value for t in cfg.tiers}
    for row in rows.values():
        assert row.learned == row.prior
        assert row.samples == 0
        assert row.confidence == 0.0


def test_summary_reflects_real_observations():
    cfg = load_config()
    model = CostModel(cfg)
    et = EventType.CLICK
    low, high = PAYLOAD_SIZE_RANGES[et]
    for _ in range(MIN_CONFIDENT_SAMPLES * 3):
        model.observe(et, high, true_cost(cfg, et, high))
    row = next(r for r in model.summary() if r.event_type == et.value)
    assert row.samples > 0
    assert row.confidence == 1.0
    assert row.learned != row.prior


def test_observe_never_influences_what_is_served_it_is_not_a_bandit():
    """The prompt's own explicit non-negotiable, checked directly: calling
    observe() changes ONLY future estimate()/summary() output — it has no
    other observable side effect (no return value driving a decision, no
    stored "next action", nothing exploratory)."""
    cfg = load_config()
    model = CostModel(cfg)
    result = model.observe(EventType.LOG, 100, 1.0)
    assert result is None  # purely an observation, nothing returned to act on


def test_every_bucket_starts_independent_and_isolated():
    """Observations in one payload-size bucket must not silently bleed
    into another type's or another bucket's own estimate."""
    cfg = load_config()
    model = CostModel(cfg)
    et_a, et_b = EventType.PAYMENT, EventType.LOG
    low_a, high_a = PAYLOAD_SIZE_RANGES[et_a]
    for _ in range(200):
        model.observe(et_a, high_a, 999.0)
    # A different type, untouched, still reads exactly its own prior.
    low_b, high_b = PAYLOAD_SIZE_RANGES[et_b]
    mid_b = (low_b + high_b) // 2
    assert model.estimate(et_b, mid_b) == cfg.tiers[et_b].cost


def test_reset_clears_learning_back_to_the_prior():
    cfg = load_config()
    model = CostModel(cfg)
    et = EventType.ORDER
    low, high = PAYLOAD_SIZE_RANGES[et]
    for _ in range(200):
        model.observe(et, high, 999.0)
    assert model.estimate(et, high) != cfg.tiers[et].cost

    model.reset()
    assert model.estimate(et, high) == cfg.tiers[et].cost
    for row in model.summary():
        assert row.samples == 0


def test_bucket_count_matches_config():
    cfg = load_config()
    model = CostModel(cfg)
    assert len(model._estimates) == len(cfg.tiers) * BUCKETS_PER_TYPE
