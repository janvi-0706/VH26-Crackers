"""The tier table must stay calibrated, or the demo has no story.

CLAUDE.md: "If you change any cost or mix number, re-verify all three." This
file is that re-verification, run on every commit instead of remembered.
"""

from __future__ import annotations

import textwrap

import pytest

from triage.config import CalibrationError, load_config
from triage.contracts import EventType, Tier


def test_config_loads_and_all_three_invariants_hold():
    cfg = load_config()
    for check in cfg.calibration_report():
        assert check.ok, str(check)


def test_mix_sums_to_one():
    cfg = load_config()
    assert abs(sum(cfg.mix.values()) - 1.0) < 1e-9


def test_p0_fits_under_capacity_at_spike():
    """The guarantee is only keepable if the protected tier fits. 108 u/s of
    P0 against 150 u/s of capacity is the headroom that lets us promise
    payments and orders are never dropped."""
    cfg = load_config()
    p0 = cfg.demand_ups(cfg.spike_eps, Tier.P0)
    assert p0 < cfg.total_capacity_ups
    assert p0 / cfg.total_capacity_ups < 0.8, "P0 headroom is too thin"


def test_spike_actually_overloads_the_pool():
    """If total demand fit under capacity, triage would never fire and the
    whole project would be a queue with extra steps."""
    cfg = load_config()
    total = cfg.demand_ups(cfg.spike_eps)
    assert total > cfg.total_capacity_ups
    assert 1.7 < total / cfg.total_capacity_ups < 2.1


def test_baseline_is_comfortably_idle():
    cfg = load_config()
    assert cfg.demand_ups(cfg.baseline_eps) < 0.2 * cfg.total_capacity_ups


def test_tier_table_matches_claude_md():
    cfg = load_config()
    expected = {
        EventType.PAYMENT: (Tier.P0, 120, 200, 3.5),
        EventType.ORDER: (Tier.P0, 100, 500, 3.0),
        EventType.INVENTORY: (Tier.P1, 40, 5000, 2.0),
        EventType.CLICK: (Tier.P2, 5, 30000, 0.5),
        EventType.LOG: (Tier.P2, 1, 60000, 0.3),
    }
    for etype, (tier, value, sla_ms, cost) in expected.items():
        spec = cfg.tiers[etype]
        assert (spec.tier, spec.value, spec.sla_ms, spec.cost) == (
            tier, value, sla_ms, cost
        )


def test_a_miscalibrated_table_refuses_to_load(tmp_path):
    """Halving every cost would make the spike fit under capacity. The loader
    has to catch that here, not three hours later in a demo that looks fine
    and proves nothing."""
    bad = tmp_path / "tiers.yaml"
    bad.write_text(textwrap.dedent("""
        schema_version: 1
        tiers:
          payment:   {tier: P0, value: 120, sla_ms: 200,   cost: 0.35}
          order:     {tier: P0, value: 100, sla_ms: 500,   cost: 0.30}
          inventory: {tier: P1, value: 40,  sla_ms: 5000,  cost: 0.20}
          click:     {tier: P2, value: 5,   sla_ms: 30000, cost: 0.05}
          log:       {tier: P2, value: 1,   sla_ms: 60000, cost: 0.03}
        mix: {payment: 0.05, order: 0.05, inventory: 0.10, click: 0.50, log: 0.30}
        workers: {count: 6, capacity_units_per_sec: 25}
        load: {baseline_eps: 16.65, spike_multiplier: 20}
        calibration:
          p0_demand_at_spike_ups: 108.2
          total_demand_at_spike_ups: 288.0
          total_demand_at_baseline_ups: 14.4
          tolerance_ups: 1.0
    """), encoding="utf-8")

    with pytest.raises(CalibrationError) as err:
        load_config(bad)
    assert "nothing would force triage" in str(err.value)


def test_a_mix_that_does_not_sum_to_one_is_rejected(tmp_path):
    bad = tmp_path / "tiers.yaml"
    bad.write_text(textwrap.dedent("""
        schema_version: 1
        tiers:
          payment:   {tier: P0, value: 120, sla_ms: 200,   cost: 3.5}
          order:     {tier: P0, value: 100, sla_ms: 500,   cost: 3.0}
          inventory: {tier: P1, value: 40,  sla_ms: 5000,  cost: 2.0}
          click:     {tier: P2, value: 5,   sla_ms: 30000, cost: 0.5}
          log:       {tier: P2, value: 1,   sla_ms: 60000, cost: 0.3}
        mix: {payment: 0.05, order: 0.05, inventory: 0.10, click: 0.50, log: 0.50}
        workers: {count: 6, capacity_units_per_sec: 25}
        load: {baseline_eps: 16.65, spike_multiplier: 20}
        calibration:
          p0_demand_at_spike_ups: 108.2
          total_demand_at_spike_ups: 288.0
          total_demand_at_baseline_ups: 14.4
          tolerance_ups: 1.0
    """), encoding="utf-8")

    with pytest.raises(CalibrationError) as err:
        load_config(bad)
    assert "mix must sum to 1.0" in str(err.value)
