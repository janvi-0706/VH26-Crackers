"""config/servers.yaml must keep the three-process split calibrated the same
way tests/test_config.py already keeps the tier table calibrated.

Phase J1's own inspection (docs/PHASE-J-INSPECTION.md) named the shape;
these are the three specific numbers this phase's own prompt calls out as
load-bearing, plus the structural checks (worker derivation, tier coverage)
that make the YAML itself trustworthy.
"""

from __future__ import annotations

import pytest

from triage.config import load_config
from triage.contracts import Tier
from triage.servers_config import (
    ServersConfigError,
    default_reference_worker_rate_ups,
    derive_workers,
    load_servers_config,
)


def test_server1_capacity_is_135_at_roughly_80_percent_utilisation():
    """108 u/s of P0 demand at spike (config/tiers.yaml's own calibration
    constant) against 135 u/s of server1 capacity is ~80% utilisation —
    real headroom, not a number picked to look round."""
    cfg = load_servers_config()
    assert cfg.server1.capacity_us == 135

    tiers_cfg = load_config()
    p0_demand = tiers_cfg.demand_ups(tiers_cfg.spike_eps, Tier.P0)
    utilisation = p0_demand / cfg.server1.capacity_us
    assert 0.75 < utilisation <= 0.85, (
        f"P0 demand {p0_demand:.1f} u/s against {cfg.server1.capacity_us} u/s "
        f"capacity is {utilisation:.1%} utilisation, expected roughly 80%"
    )


def test_server1_scaling_is_fixed():
    """P0 must never autoscale — CLAUDE.md hard rule 3 protects it from
    ever being throttled, and a scheduler that can add or remove capacity
    on its own schedule is a second, uncontrolled way that guarantee could
    be undermined even if nothing in this codebase ever calls
    /control/rate against it."""
    cfg = load_servers_config()
    assert cfg.server1.scaling == "fixed"


def test_server2_max_pods_caps_total_capacity_so_the_spike_stays_oversubscribed():
    """This is the test that protects the entire experiment.

    System ceiling at max_pods: server1's fixed 135 u/s + server2's own
    ceiling (capacity_us_per_pod x max_pods). Total demand at a 20x spike
    is ~288 u/s (config/tiers.yaml's own calibration constant). If HPA
    could scale server2 past this ceiling far enough to close that gap,
    the pipeline would simply catch up to the spike by adding pods, and
    this project's entire decision engine (score-ordering, MICRO_BATCH,
    DEFER, the ladder, CoDel) would never have a reason to fire — there
    would be nothing left to triage.
    """
    cfg = load_servers_config()
    assert cfg.server2.max_pods == 3

    ceiling = cfg.total_capacity_at_max()
    assert ceiling == pytest.approx(180.0)

    tiers_cfg = load_config()
    total_demand = tiers_cfg.demand_ups(tiers_cfg.spike_eps)
    oversubscription = total_demand / ceiling
    assert 1.5 < oversubscription < 1.7, (
        f"total demand {total_demand:.1f} u/s against a {ceiling:.1f} u/s "
        f"ceiling is {oversubscription:.2f}x oversubscribed, expected ~1.6x"
    )


def test_server2_min_pods_is_never_above_max_pods():
    cfg = load_servers_config()
    assert cfg.server2.min_pods <= cfg.server2.max_pods


def test_server1_and_server2_partition_every_tier_exactly_once():
    cfg = load_servers_config()
    assert set(cfg.server1.tiers) == {Tier.P0}
    assert set(cfg.server2.tiers) == {Tier.P1, Tier.P2}
    assert set(cfg.server1.tiers) | set(cfg.server2.tiers) == set(Tier)
    assert not (set(cfg.server1.tiers) & set(cfg.server2.tiers))


def test_server_for_tier_routes_correctly():
    cfg = load_servers_config()
    assert cfg.server_for_tier(Tier.P0) is cfg.server1
    assert cfg.server_for_tier(Tier.P1) is cfg.server2
    assert cfg.server_for_tier(Tier.P2) is cfg.server2


def test_batching_flags_match_the_prompt():
    cfg = load_servers_config()
    assert cfg.server1.batching is False
    assert cfg.server2.batching is True


def test_transport_and_metrics_sections_load():
    cfg = load_servers_config()
    assert cfg.transport.batch_size == 20
    assert cfg.transport.batch_window_ms == 10
    assert cfg.transport.timeout_ms == 500
    assert cfg.transport.ack_timeout_ms == 5000
    assert cfg.metrics.push_interval_ms == 250
    assert cfg.metrics.fragment_ttl_ms == 1000


# --------------------------------------------------------------------------
# Worker derivation: "do not express the split as a count of equal
# workers — 135/15 does not divide into six."
# --------------------------------------------------------------------------


def test_derive_workers_gives_exact_totals_not_rounded_ones():
    """Whatever count comes out, count * rate must reconstruct the
    original capacity exactly — the derivation may not silently over- or
    under-provision to land on a round worker count."""
    for capacity in (135.0, 15.0, 1.0, 999.0):
        count, rate = derive_workers(capacity, reference_worker_rate_ups=25.0)
        assert count >= 1
        assert count * rate == pytest.approx(capacity)


def test_derive_workers_does_not_force_a_shared_worker_count():
    """server1 (135 u/s) and server2 (15 u/s/pod) must not derive the same
    worker count just because the single-process build once had 6 workers
    total — the whole point of deriving from capacity."""
    ref = 25.0
    server1_count, server1_rate = derive_workers(135.0, ref)
    server2_count, server2_rate = derive_workers(15.0, ref)
    assert server1_count == 6
    assert server1_rate == pytest.approx(22.5)
    assert server2_count == 1
    assert server2_rate == pytest.approx(15.0)
    assert server1_count != server2_count or server1_rate != server2_rate


def test_derive_workers_rejects_nonpositive_input():
    with pytest.raises(ValueError):
        derive_workers(0.0, 25.0)
    with pytest.raises(ValueError):
        derive_workers(10.0, 0.0)


def test_server_spec_workers_matches_derive_workers():
    cfg = load_servers_config()
    ref = default_reference_worker_rate_ups()
    assert cfg.server1.workers(reference_worker_rate_ups=ref) == derive_workers(
        cfg.server1.capacity_us, ref
    )
    assert cfg.server2.workers(reference_worker_rate_ups=ref) == derive_workers(
        cfg.server2.capacity_us_per_pod, ref
    )


def test_server2_workers_are_the_same_per_pod_regardless_of_pod_count():
    """Every hpa pod is an identical replica sized off capacity_us_per_pod
    alone — pod_count changes how many pods exist, not one pod's own
    internal worker layout."""
    cfg = load_servers_config()
    ref = default_reference_worker_rate_ups()
    at_one = cfg.server2.workers(reference_worker_rate_ups=ref, pod_count=1)
    at_three = cfg.server2.workers(reference_worker_rate_ups=ref, pod_count=3)
    assert at_one == at_three


def test_capacity_at_requires_pod_count_for_hpa_server():
    cfg = load_servers_config()
    with pytest.raises(ValueError):
        cfg.server2.capacity_at()
    assert cfg.server2.capacity_at(pod_count=1) == 15.0
    assert cfg.server2.capacity_at(pod_count=3) == 45.0


def test_capacity_at_rejects_pod_count_outside_declared_range():
    cfg = load_servers_config()
    with pytest.raises(ValueError):
        cfg.server2.capacity_at(pod_count=4)
    with pytest.raises(ValueError):
        cfg.server2.capacity_at(pod_count=0)


def test_capacity_at_ignores_pod_count_for_fixed_server():
    cfg = load_servers_config()
    assert cfg.server1.capacity_at() == 135.0
    assert cfg.server1.capacity_at(pod_count=1) == 135.0


# --------------------------------------------------------------------------
# Structural validation — a malformed servers.yaml must fail loudly, not
# silently start a mis-provisioned server.
# --------------------------------------------------------------------------


def test_fixed_server_rejects_capacity_us_per_pod():
    with pytest.raises(ServersConfigError):
        _parse_from_dict(
            server1_overrides={"capacity_us": None, "capacity_us_per_pod": 10}
        )


def test_hpa_server_requires_capacity_us_per_pod():
    with pytest.raises(ServersConfigError):
        _parse_from_dict(
            server2_overrides={"capacity_us_per_pod": None}
        )


def test_overlapping_tiers_rejected():
    with pytest.raises(ServersConfigError):
        _parse_from_dict(server2_overrides={"tiers": ["P0", "P1", "P2"]})


def test_uncovered_tier_rejected():
    with pytest.raises(ServersConfigError):
        _parse_from_dict(server1_overrides={"tiers": []}, server2_overrides={"tiers": ["P1"]})


def test_server1_must_be_fixed():
    with pytest.raises(ServersConfigError):
        _parse_from_dict(server1_overrides={"scaling": "hpa", "capacity_us": None,
                                             "capacity_us_per_pod": 20})


def _parse_from_dict(server1_overrides=None, server2_overrides=None):
    from triage.servers_config import _parse

    raw = {
        "ingress": {"port": 8000, "history_db": "data/history.db"},
        "server1": {
            "port": 8001, "tiers": ["P0"], "capacity_us": 135,
            "batching": False, "scaling": "fixed",
        },
        "server2": {
            "port": 8002, "tiers": ["P1", "P2"], "capacity_us_per_pod": 15,
            "batching": True, "scaling": "hpa", "min_pods": 1, "max_pods": 3,
        },
        "transport": {
            "batch_size": 20, "batch_window_ms": 10, "timeout_ms": 500,
            "ack_timeout_ms": 5000,
        },
        "metrics": {"push_interval_ms": 250, "fragment_ttl_ms": 1000},
    }
    if server1_overrides:
        raw["server1"].update(server1_overrides)
    if server2_overrides:
        raw["server2"].update(server2_overrides)
    return _parse(raw, source=None)
