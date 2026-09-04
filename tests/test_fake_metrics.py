"""The fake feed is a contract too.

Lane C builds every panel against these frames for the first few hours. If the
fake data violates an invariant the real engine keeps, the dashboard gets built
to display something that will never happen — and worse, a panel that is
correctly red on fake data looks like a bug in the panel.
"""

from __future__ import annotations

import pytest

from triage.contracts import Decision, MetricsFrame, Tier
from triage.fake_metrics import FakeSource


def run(seconds: float = 45.0, seed: int = 11) -> list[MetricsFrame]:
    """A full demo cycle: calm, 20x spike, recovery."""
    src = FakeSource(seed=seed)
    return [src.tick(src.started + i * 0.25) for i in range(int(seconds * 4))]


@pytest.fixture(scope="module")
def frames() -> list[MetricsFrame]:
    return run()


def test_every_frame_validates(frames):
    for frame in frames:
        assert MetricsFrame.model_validate_json(frame.model_dump_json()) == frame


def test_counters_conserve_exactly(frames):
    """ingested == processed + in_queue + in_flight + deferred + sampled + shed.

    The same equation the real ledger has to satisfy in Stage F.
    """
    for i, f in enumerate(frames):
        accounted = (f.processed + f.in_queue + f.in_flight
                     + f.deferred_pending + f.sampled_out + f.shed)
        assert f.ingested == accounted, f"frame {i}: {f.ingested} != {accounted}"


def test_counters_only_ever_move_forward(frames):
    for a, b in zip(frames, frames[1:]):
        assert b.ingested >= a.ingested
        assert b.processed >= a.processed
        assert b.shed >= a.shed


def test_p0_is_never_degraded(frames):
    """Hard rule 3, enforced in the fake data as well as the engine."""
    for f in frames:
        assert f.ladder_rung["P0"] == 0
        assert all(s.tier is not Tier.P0 for s in f.recent_sheds)
        assert all(d.decision is Decision.STREAM_NOW
                   for d in f.recent_decisions if d.tier is Tier.P0)


def test_in_flight_never_exceeds_the_worker_pool(frames):
    for f in frames:
        assert f.in_flight <= f.worker_count


def test_the_spike_actually_shows_up(frames):
    """A fake feed that never gets into trouble would let Lane C build a
    dashboard that has never rendered a loaded pipeline."""
    peak = max(frames, key=lambda f: f.pressure)
    assert peak.pressure > 1.5
    assert peak.spike_multiplier > 1.0
    assert peak.ladder_rung["P2"] >= 2
    assert peak.shed > 0


def test_tiers_diverge_under_load(frames):
    """The whole demo in one assertion: one number flat, one degrading."""
    peak = max(frames, key=lambda f: f.pressure)
    assert peak.latency_p99["P0"] < 100.0
    assert peak.latency_p99["P2"] > 10 * peak.latency_p99["P0"]


def test_adaptive_spends_less_capacity_than_naive(frames):
    last = frames[-1]
    assert last.cost_adaptive < last.cost_naive


def test_the_feed_recovers_when_the_spike_passes(frames):
    calm_after = [f for f in frames if f.spike_multiplier == 1.0][-8:]
    assert min(f.pressure for f in calm_after) < 0.8
