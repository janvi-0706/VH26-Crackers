"""admission.py: the AIMD credit bucket, and AdmissionControl's per-tier
routing over it."""

from __future__ import annotations

import pytest

from triage.admission import (
    ADDITIVE_INCREASE_UPS,
    DECREASE_CHECK_INTERVAL_SECONDS,
    HIGH_PRESSURE,
    INCREASE_CHECK_INTERVAL_SECONDS,
    MIN_BULK_RATE_UPS,
    MULTIPLICATIVE_DECREASE,
    AdmissionControl,
    CreditBucket,
)
from triage.config import load_config
from triage.contracts import EventType, Tier


def make_bulk_bucket(rate: float = 100.0, capacity: float | None = None) -> CreditBucket:
    return CreditBucket(
        tier=Tier.P2,
        rate_ups=rate,
        capacity_units=capacity if capacity is not None else rate,
        max_rate_ups=rate,
        critical=False,
    )


# --------------------------------------------------------------------------
# CreditBucket — the token bucket itself
# --------------------------------------------------------------------------


def test_a_fresh_bucket_starts_full():
    bucket = make_bulk_bucket(rate=50.0)
    assert bucket.credits == pytest.approx(50.0)


def test_try_acquire_spends_exactly_its_cost():
    bucket = make_bulk_bucket(rate=50.0)
    assert bucket.try_acquire(10.0, now=0.0) is True
    assert bucket.credits == pytest.approx(40.0)


def test_try_acquire_denies_when_credits_are_insufficient():
    bucket = make_bulk_bucket(rate=5.0, capacity=5.0)
    assert bucket.try_acquire(3.0, now=0.0) is True
    assert bucket.try_acquire(3.0, now=0.0) is False  # only 2.0 left
    assert bucket.denied_count == 1


def test_credits_refill_over_elapsed_time_capped_at_capacity():
    bucket = make_bulk_bucket(rate=10.0, capacity=10.0)
    bucket.try_acquire(10.0, now=0.0)  # drain to 0
    assert bucket.try_acquire(1.0, now=0.05) is False  # not enough time to refill 1.0 at 10/s... 0.5 available
    assert bucket.try_acquire(0.4, now=0.05) is True
    # Refilling well past a second must cap at capacity, not overshoot.
    assert bucket.try_acquire(0.0, now=100.0) is True
    bucket._refill(100.0)
    assert bucket.credits == pytest.approx(10.0)


def test_critical_bucket_always_grants_regardless_of_credits():
    bucket = CreditBucket(
        tier=Tier.P0, rate_ups=0.0, capacity_units=0.0, max_rate_ups=0.0, critical=True
    )
    assert bucket.credits == 0.0
    for _ in range(1000):
        assert bucket.try_acquire(9999.0, now=0.0) is True
    assert bucket.denied_count == 0


# --------------------------------------------------------------------------
# AIMD — additive increase while calm, x0.8 above HIGH_PRESSURE
# --------------------------------------------------------------------------


def test_aimd_increases_additively_on_the_first_check():
    """The first check on a fresh bucket applies immediately — a real
    spike can start at any wall-clock instant, and nothing should require
    an interval's worth of real time to elapse before the very first
    adjustment can register."""
    bucket = make_bulk_bucket(rate=50.0)
    bucket.max_rate_ups = 1_000.0  # headroom so the ceiling isn't what stops growth here
    bucket.update_aimd(pressure=0.5, now=0.0)
    assert bucket.rate_ups == pytest.approx(50.0 + ADDITIVE_INCREASE_UPS)


def test_aimd_increase_is_rate_limited_to_once_per_check_interval():
    bucket = make_bulk_bucket(rate=50.0)
    bucket.max_rate_ups = 1_000.0
    bucket.update_aimd(pressure=0.5, now=0.0)
    after_first = bucket.rate_ups
    bucket.update_aimd(pressure=0.5, now=INCREASE_CHECK_INTERVAL_SECONDS / 2)
    assert bucket.rate_ups == pytest.approx(after_first), "must not increase again before a full interval passes"
    bucket.update_aimd(pressure=0.5, now=INCREASE_CHECK_INTERVAL_SECONDS)
    assert bucket.rate_ups == pytest.approx(after_first + ADDITIVE_INCREASE_UPS)


def test_aimd_increase_never_exceeds_max_rate_ups():
    bucket = make_bulk_bucket(rate=98.0)
    bucket.max_rate_ups = 100.0
    now = 0.0
    for _ in range(10):
        now += INCREASE_CHECK_INTERVAL_SECONDS
        bucket.update_aimd(pressure=0.0, now=now)
    assert bucket.rate_ups == pytest.approx(100.0)
    assert bucket.capacity_units == pytest.approx(100.0)


def test_aimd_decreases_multiplicatively_above_high_pressure():
    bucket = make_bulk_bucket(rate=100.0)
    bucket.update_aimd(pressure=HIGH_PRESSURE, now=0.0)
    assert bucket.rate_ups == pytest.approx(100.0 * MULTIPLICATIVE_DECREASE)
    assert bucket.capacity_units == pytest.approx(100.0 * MULTIPLICATIVE_DECREASE)


def test_aimd_decrease_claws_back_banked_credits_above_the_new_ceiling():
    bucket = make_bulk_bucket(rate=100.0)
    assert bucket.credits == pytest.approx(100.0)  # full at start
    bucket.update_aimd(pressure=HIGH_PRESSURE, now=0.0)
    assert bucket.credits == pytest.approx(80.0), "banked credits must shrink with the new capacity"


def test_aimd_decrease_is_checked_much_faster_than_increase():
    """The AIMD asymmetry itself: decrease can fire again almost
    immediately; increase cannot."""
    bucket = make_bulk_bucket(rate=100.0)
    bucket.update_aimd(pressure=HIGH_PRESSURE, now=0.0)
    first = bucket.rate_ups
    bucket.update_aimd(pressure=HIGH_PRESSURE, now=DECREASE_CHECK_INTERVAL_SECONDS)
    assert bucket.rate_ups == pytest.approx(first * MULTIPLICATIVE_DECREASE)


def test_aimd_never_decreases_rate_below_the_floor():
    bucket = make_bulk_bucket(rate=2.0, capacity=2.0)
    now = 0.0
    for _ in range(200):
        now += DECREASE_CHECK_INTERVAL_SECONDS
        bucket.update_aimd(pressure=1.0, now=now)
    assert bucket.rate_ups == pytest.approx(MIN_BULK_RATE_UPS)
    assert bucket.rate_ups > 0.0, "a throttled bulk source must never be fully starved"


def test_critical_bucket_is_never_adjusted_by_aimd():
    bucket = CreditBucket(
        tier=Tier.P0, rate_ups=10.0, capacity_units=10.0, max_rate_ups=10.0, critical=True
    )
    bucket.update_aimd(pressure=1.0, now=0.0)
    bucket.update_aimd(pressure=1.0, now=100.0)
    assert bucket.rate_ups == pytest.approx(10.0)
    assert bucket.capacity_units == pytest.approx(10.0)


def test_reset_restores_a_bucket_to_its_calibrated_ceiling_full():
    bucket = make_bulk_bucket(rate=100.0)
    bucket.update_aimd(pressure=1.0, now=0.0)
    bucket.try_acquire(5.0, now=0.0)
    bucket.reset()
    assert bucket.rate_ups == pytest.approx(100.0)
    assert bucket.capacity_units == pytest.approx(100.0)
    assert bucket.credits == pytest.approx(100.0)
    assert bucket.denied_count == 0


# --------------------------------------------------------------------------
# AdmissionControl — per-tier routing, seeded from real calibration
# --------------------------------------------------------------------------


def test_buckets_are_seeded_from_each_tiers_own_calibrated_spike_demand():
    config = load_config()
    control = AdmissionControl(config=config)
    spike_eps = config.spike_eps
    for tier in (Tier.P0, Tier.P1, Tier.P2):
        expected = config.demand_ups(spike_eps, tier)
        bucket = control.bucket(tier)
        assert bucket.rate_ups == pytest.approx(expected)
        assert bucket.capacity_units == pytest.approx(expected)


def test_p0_types_always_admit_regardless_of_pressure():
    control = AdmissionControl(config=load_config())
    for _ in range(500):
        assert control.try_acquire(EventType.PAYMENT, pressure=1.0, now=0.0) is True
        assert control.try_acquire(EventType.ORDER, pressure=1.0, now=0.0) is True
    assert control.bucket(Tier.P0).denied_count == 0


def test_bulk_types_can_be_denied_under_sustained_high_pressure():
    control = AdmissionControl(config=load_config())
    now = 0.0
    denied_at_least_once = False
    for _ in range(400):
        now += DECREASE_CHECK_INTERVAL_SECONDS
        if not control.try_acquire(EventType.CLICK, pressure=1.0, now=now):
            denied_at_least_once = True
    assert denied_at_least_once, "sustained pressure=1.0 must eventually deny bulk admission"


def test_tier_of_and_cost_of_match_the_frozen_config_table():
    config = load_config()
    control = AdmissionControl(config=config)
    assert control.tier_of(EventType.PAYMENT) is Tier.P0
    assert control.tier_of(EventType.CLICK) is Tier.P2
    assert control.cost_of(EventType.PAYMENT) == pytest.approx(config.tiers[EventType.PAYMENT].cost)


def test_p1_and_p2_buckets_are_independent():
    control = AdmissionControl(config=load_config())
    p2 = control.bucket(Tier.P2)
    p1_rate_before = control.bucket(Tier.P1).rate_ups
    # Drive P2 hard into the floor; P1 must not move at all.
    now = 0.0
    for _ in range(300):
        now += DECREASE_CHECK_INTERVAL_SECONDS
        control.try_acquire(EventType.CLICK, pressure=1.0, now=now)
    assert p2.rate_ups < control.bucket(Tier.P1).rate_ups
    assert control.bucket(Tier.P1).rate_ups == pytest.approx(p1_rate_before)


def test_reset_clears_every_bucket():
    control = AdmissionControl(config=load_config())
    now = 0.0
    for _ in range(300):
        now += DECREASE_CHECK_INTERVAL_SECONDS
        control.try_acquire(EventType.CLICK, pressure=1.0, now=now)
    assert control.bucket(Tier.P2).denied_count > 0

    control.reset()

    for tier in (Tier.P0, Tier.P1, Tier.P2):
        bucket = control.bucket(tier)
        assert bucket.denied_count == 0
        assert bucket.rate_ups == pytest.approx(bucket.max_rate_ups)
