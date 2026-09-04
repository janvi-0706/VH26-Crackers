"""codel.py: the sojourn-only CoDel controller.

No test here ever passes a queue length or a depth — asserting that is the
whole point of "no queue-length threshold anywhere in this file."
"""

from __future__ import annotations

from triage import codel
from triage.codel import CoDelController, INTERVAL_SECONDS, TARGET_SECONDS


def make() -> CoDelController:
    return CoDelController()


def test_starts_not_sampling():
    c = make()
    assert c.sampling is False


def test_a_single_slow_observation_does_not_trigger_sampling():
    """One slow item is not sustained congestion — entry requires the
    minimum to stay above target for a FULL interval, not one sample."""
    c = make()
    now = 1000.0
    assert c.update(TARGET_SECONDS + 1.0, now) is False
    assert c.sampling is False


def test_minimum_above_target_for_a_full_interval_enters_sampling():
    c = make()
    t0 = 1000.0
    # Two observations, both above target, spanning a full interval.
    c.update(TARGET_SECONDS + 0.2, t0)
    result = c.update(TARGET_SECONDS + 0.3, t0 + INTERVAL_SECONDS)
    assert result is True
    assert c.sampling is True


def test_minimum_at_or_below_target_does_not_enter_sampling():
    c = make()
    t0 = 1000.0
    c.update(TARGET_SECONDS - 0.05, t0)  # below target
    c.update(TARGET_SECONDS + 5.0, t0 + INTERVAL_SECONDS / 2)  # one high sample...
    result = c.update(TARGET_SECONDS + 5.0, t0 + INTERVAL_SECONDS)
    # ...but the interval's MINIMUM (the first, below-target sample) is what
    # matters, not any single high sample within it.
    assert result is False


def test_exit_is_immediate_not_interval_gated():
    """Once sampling, a single observation below target exits right away —
    no need to wait for a full interval, unlike entry."""
    c = make()
    t0 = 1000.0
    c.update(TARGET_SECONDS + 0.2, t0)
    c.update(TARGET_SECONDS + 0.3, t0 + INTERVAL_SECONDS)
    assert c.sampling is True

    result = c.update(TARGET_SECONDS - 0.01, t0 + INTERVAL_SECONDS + 0.001)
    assert result is False
    assert c.sampling is False


def test_sustained_congestion_across_many_intervals_stays_sampling():
    c = make()
    now = 1000.0
    for _ in range(20):
        now += INTERVAL_SECONDS
        c.update(TARGET_SECONDS + 1.0, now)
    assert c.sampling is True


def test_reset_returns_to_the_initial_not_sampling_state():
    c = make()
    t0 = 1000.0
    c.update(TARGET_SECONDS + 0.2, t0)
    c.update(TARGET_SECONDS + 0.3, t0 + INTERVAL_SECONDS)
    assert c.sampling is True

    c.reset()
    assert c.sampling is False
    # And re-entry still requires a full interval again, not one sample —
    # reset must not leave a warm interval-min lying around.
    assert c.update(TARGET_SECONDS + 1.0, t0 + 100.0) is False


# --------------------------------------------------------------------------
# The ambient default controller (what metrics.py/worker.py actually use)
# --------------------------------------------------------------------------


def test_module_level_observe_and_is_sampling_track_the_default_controller():
    codel.reset()
    assert codel.is_sampling() is False
    t0 = 2000.0
    codel.observe(TARGET_SECONDS + 0.2, t0)
    codel.observe(TARGET_SECONDS + 0.3, t0 + INTERVAL_SECONDS)
    assert codel.is_sampling() is True
    codel.reset()
    assert codel.is_sampling() is False
