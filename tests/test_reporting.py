"""reporting.py: the metrics-fragment push/aggregate interface.

Push, not poll (this module's own docstring, and docs/PHASE-J-INSPECTION.md
section 4, both say why: a Service in front of server2's own 1-3 pods hands
a poll to one random pod, never all of them). These tests exercise the
receiving/aggregating side against a fake clock, independent of anything
that will eventually do the actual pushing over the wire (Phase J3).
"""

from __future__ import annotations

from triage import reporting
from triage.reporting import FragmentStore, MetricsFragment


class _FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_push_then_fragments_returns_it():
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.5)
    frag = MetricsFragment(
        server="server1", instance_id="server1-only",
        pushed_ts=1000.0, counters={"processed": 5, "in_flight": 2},
    )
    store.push(frag)
    assert store.fragments() == [frag]


def test_fragments_filters_by_server():
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.5)
    store.push(MetricsFragment("server1", "s1-only", 1000.0, {"processed": 1}))
    store.push(MetricsFragment("server2", "pod-a", 1000.0, {"processed": 2}))

    assert [f.server for f in store.fragments("server1")] == ["server1"]
    assert [f.server for f in store.fragments("server2")] == ["server2"]
    assert len(store.fragments()) == 2


def test_a_second_push_from_the_same_instance_replaces_the_first():
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.5)
    store.push(MetricsFragment("server2", "pod-a", 1000.0, {"processed": 1}))
    store.push(MetricsFragment("server2", "pod-a", 1000.25, {"processed": 2}))

    frags = store.fragments("server2")
    assert len(frags) == 1
    assert frags[0].counters["processed"] == 2


def test_an_out_of_order_older_push_does_not_overwrite_a_newer_one():
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.75)
    store.push(MetricsFragment("server2", "pod-a", 1000.5, {"processed": 5}))
    store.push(MetricsFragment("server2", "pod-a", 1000.25, {"processed": 1}))  # stale, arrives late

    frags = store.fragments("server2")
    assert frags[0].counters["processed"] == 5


def test_aggregate_sums_across_multiple_live_instances():
    """The multi-pod server2 case docs/PHASE-J-INSPECTION.md section 4
    names explicitly: three pods' own `processed` counts must sum, not
    overwrite."""
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.5)
    store.push(MetricsFragment("server2", "pod-a", 1000.0, {"processed": 10, "in_flight": 2}))
    store.push(MetricsFragment("server2", "pod-b", 1000.0, {"processed": 7, "in_flight": 1}))
    store.push(MetricsFragment("server2", "pod-c", 1000.0, {"processed": 3, "in_flight": 0}))

    totals = store.aggregate("server2")
    assert totals == {"processed": 20, "in_flight": 3}


def test_aggregate_with_no_server_filter_sums_everything():
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.5)
    store.push(MetricsFragment("server1", "s1", 1000.0, {"processed": 4}))
    store.push(MetricsFragment("server2", "pod-a", 1000.0, {"processed": 6}))

    assert store.aggregate()["processed"] == 10


def test_a_counter_only_some_instances_report_is_summed_over_the_ones_that_do():
    store = FragmentStore(fragment_ttl_ms=1000, now=lambda: 1000.5)
    store.push(MetricsFragment("server1", "s1", 1000.0, {"processed": 5}))  # no sampled_out — P0 is never sampled
    store.push(MetricsFragment("server2", "pod-a", 1000.0, {"processed": 1, "sampled_out": 3}))

    totals = store.aggregate()
    assert totals["processed"] == 6
    assert totals["sampled_out"] == 3


def test_a_stale_fragment_past_ttl_is_excluded_from_fragments_and_aggregate():
    clock = _FakeClock(start=1000.0)
    store = FragmentStore(fragment_ttl_ms=1000, now=clock)  # 1000ms TTL
    store.push(MetricsFragment("server2", "pod-a", pushed_ts=1000.0, counters={"processed": 5}))

    clock.advance(1.5)  # 1500ms later — past the 1000ms TTL
    assert store.fragments("server2") == []
    assert store.aggregate("server2") == {}


def test_fresh_only_false_still_returns_a_stale_fragment():
    clock = _FakeClock(start=1000.0)
    store = FragmentStore(fragment_ttl_ms=1000, now=clock)
    store.push(MetricsFragment("server2", "pod-a", pushed_ts=1000.0, counters={"processed": 5}))

    clock.advance(1.5)
    stale = store.fragments("server2", fresh_only=False)
    assert len(stale) == 1
    assert stale[0].counters["processed"] == 5


def test_instance_count_reflects_only_live_instances():
    clock = _FakeClock(start=1000.0)
    store = FragmentStore(fragment_ttl_ms=1000, now=clock)
    store.push(MetricsFragment("server2", "pod-a", 1000.0, {}))
    store.push(MetricsFragment("server2", "pod-b", 1000.0, {}))

    assert store.instance_count("server2") == 2

    clock.advance(1.5)
    assert store.instance_count("server2") == 0  # both aged out


def test_ttl_defaults_from_servers_config():
    store = FragmentStore()
    assert store._ttl_ms == 1000  # config/servers.yaml's own metrics.fragment_ttl_ms


def test_reset_clears_all_fragments():
    store = FragmentStore(fragment_ttl_ms=1000)
    store.push(MetricsFragment("server1", "s1", 1000.0, {"processed": 1}))
    store.reset()
    assert store.fragments() == []


# --------------------------------------------------------------------------
# The ambient module-level default
# --------------------------------------------------------------------------


def test_ambient_push_and_aggregate_round_trip():
    reporting.reset_default()
    try:
        reporting.push(MetricsFragment("server1", "s1", __import__("time").time(), {"processed": 3}))
        assert reporting.aggregate("server1") == {"processed": 3}
        assert reporting.instance_count("server1") == 1
    finally:
        reporting.reset_default()
