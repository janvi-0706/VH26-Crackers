"""Metrics fragment push — how server1/server2 tell ingress what their own
local counters are, once "the pipeline" is no longer one process that can
just read its own module-level state.

Owner: Lane D (Phase J2).

Servers PUSH; ingress never polls. docs/PHASE-J-INSPECTION.md (section 4)
already named why: server2 can be 1-3 pods behind a Kubernetes Service
(`config/servers.yaml`'s own `scaling: hpa`), and a poll against that
Service reaches ONE random pod, never all of them — there is no way to
poll "every server2 instance" through a Service by construction. A push
has no such problem: each instance independently sends its own fragment on
its own schedule (`config/servers.yaml`'s `metrics.push_interval_ms`), and
ingress simply keeps whatever it has most recently heard from each one.

This module is the receiving, aggregating side only — interface only, per
this phase's own instruction. It does not decide what a server puts in a
fragment (that is wherever `metrics.py`'s per-tier state ends up living
post-split, per Phase J1's own inspection) and it does not send anything
anywhere (there is nothing to send until J3 gives server1/server2 an
actual process boundary to push across) — it defines the fragment shape,
and what ingress does with a stream of them: keep the latest one per
(server, instance), aggregate the live ones into per-server or system-wide
totals, and age out an instance that has stopped reporting.

Why per-INSTANCE, not per-server: `docs/PHASE-J-INSPECTION.md` section 4
already worked out that aggregating `processed`/`in_queue`/`in_flight`/
`sampled_out`/`shed` across N live server2 pods means summing N separate
numbers, not overwriting one — a fragment keyed only by `"server2"` would
have each new pod's push silently clobber the last one's contribution
instead of adding to it. `instance_id` is this module's own answer to that:
whatever value uniquely identifies the reporting process (a pod name, once
one exists) is opaque to this module — it is never parsed, only used as a
dictionary key.

Why `fragment_ttl_ms`, not "trust every fragment forever": the same
section of the inspection doc named the risk directly — an instance that
died mid-processing (rescheduled by Kubernetes, or crashed outright) stops
pushing, and its LAST fragment must eventually stop being counted, or a
dead pod's stale `in_flight` count would sit in the aggregate forever,
silently and permanently unbalancing whatever conservation check reads
it. `aggregate()` below only ever sums fragments younger than
`fragment_ttl_ms` as of the moment it is called — an instance's
contribution simply disappears from the total `fragment_ttl_ms`
milliseconds after its last push, with no separate "instance died" signal
required. This is a real, known tradeoff, not a hidden one: a genuinely
live instance whose push happens to be delayed past the TTL (a slow
network tick, not a death) will ALSO be silently dropped for that one
aggregation, exactly the "reporting lag looks like loss" problem the
inspection document flagged and did not solve — `fragment_ttl_ms: 1000`
against a `push_interval_ms: 250` cadence (four pushes' worth of grace)
is `config/servers.yaml`'s own answer to keeping that false-drop rate low,
not a proof it is zero.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from .servers_config import load_servers_config

# The counters this project's own conservation equation and dashboard
# already track per tier (docs/PHASE-J-INSPECTION.md section 4's own
# table) — the known, expected keys of `MetricsFragment.counters`. Not
# enforced as a closed set (`counters` stays a plain dict, deliberately,
# so a server can report a counter this module was never told about
# without a schema change here) — named here only as documentation of what
# a real fragment is expected to carry, mirroring how `contracts.py`'s own
# `MetricsFrame` names its fields as documentation of the dashboard's
# contract even though nothing in THIS module reads them by name.
KNOWN_COUNTER_KEYS: tuple[str, ...] = (
    "processed", "in_queue", "in_flight", "sampled_out", "shed",
)


@dataclass(frozen=True)
class MetricsFragment:
    """One server instance's own local contribution, as of `pushed_ts`.

    `counters` is intentionally a flat `dict[str, float]`, not a fixed set
    of named fields: which counters a given server can even report depends
    on which tiers it owns (server1 never has a `sampled_out`/`shed`
    value to report — P0 is never sampled or shed, per CLAUDE.md hard rule
    3 — and this module has no reason to force it to send zeros for
    counters that are structurally meaningless for it).
    """

    server: str
    instance_id: str
    pushed_ts: float
    counters: dict[str, float] = field(default_factory=dict)


class FragmentStore:
    """Keeps the single most recent fragment per (server, instance_id),
    and aggregates whichever of those are still fresh as of `now()`."""

    def __init__(
        self,
        *,
        fragment_ttl_ms: float | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._ttl_ms = (
            fragment_ttl_ms
            if fragment_ttl_ms is not None
            else load_servers_config().metrics.fragment_ttl_ms
        )
        self._now = now
        # (server, instance_id) -> latest fragment from that instance.
        # Replaced wholesale on every push, never merged field-by-field —
        # a fragment is one instance's own complete self-report at
        # `pushed_ts`, not a delta to fold into a running total.
        self._latest: dict[tuple[str, str], MetricsFragment] = {}

    def push(self, fragment: MetricsFragment) -> None:
        """Record `fragment` as the newest one from its (server,
        instance_id). An older `pushed_ts` arriving after a newer one
        (a delayed retransmission, out-of-order delivery) is dropped
        rather than overwriting the fresher data — the store always
        reflects the most RECENT thing each instance is known to have
        reported, not the most recently ARRIVED message."""
        key = (fragment.server, fragment.instance_id)
        current = self._latest.get(key)
        if current is not None and current.pushed_ts > fragment.pushed_ts:
            return
        self._latest[key] = fragment

    def _is_fresh(self, fragment: MetricsFragment, now: float) -> bool:
        age_ms = (now - fragment.pushed_ts) * 1000.0
        return age_ms <= self._ttl_ms

    def fragments(self, server: str | None = None, *, fresh_only: bool = True) -> list[MetricsFragment]:
        """Every currently-known fragment, optionally filtered to one
        server and/or to only those still within `fragment_ttl_ms`."""
        now = self._now()
        result = [
            f for f in self._latest.values()
            if (server is None or f.server == server)
            and (not fresh_only or self._is_fresh(f, now))
        ]
        result.sort(key=lambda f: (f.server, f.instance_id))
        return result

    def instance_count(self, server: str) -> int:
        """How many instances of `server` have a live (unexpired)
        fragment right now — the number the dashboard's own worker-pool
        panel would want for an `hpa` server, since `config/servers.yaml`
        alone only ever says the STATIC [min_pods, max_pods] range, never
        how many pods actually happen to be up this second."""
        return len(self.fragments(server))

    def aggregate(self, server: str | None = None) -> dict[str, float]:
        """Sum every live fragment's `counters`, across however many
        instances currently have a fresh one — the mechanism
        docs/PHASE-J-INSPECTION.md section 4 describes needing for a
        multi-pod server2: this IS that sum. A counter key only some
        instances report is summed over the ones that do (missing means
        0 for that instance, not "exclude this key from the total")."""
        totals: dict[str, float] = {}
        for fragment in self.fragments(server=server):
            for key, value in fragment.counters.items():
                totals[key] = totals.get(key, 0.0) + value
        return totals

    def reset(self) -> None:
        """Tests only."""
        self._latest.clear()


# --------------------------------------------------------------------------
# Ambient default store, matching metrics.py/ledger.py's own precedent —
# there is exactly one ingress process receiving fragments, the same "one
# pipeline" reasoning those modules already document.
# --------------------------------------------------------------------------

_default = FragmentStore()


def push(fragment: MetricsFragment) -> None:
    _default.push(fragment)


def fragments(server: str | None = None, *, fresh_only: bool = True) -> list[MetricsFragment]:
    return _default.fragments(server, fresh_only=fresh_only)


def instance_count(server: str) -> int:
    return _default.instance_count(server)


def aggregate(server: str | None = None) -> dict[str, float]:
    return _default.aggregate(server)


def reset_default() -> None:
    """Tests only — mirrors metrics.reset()/ledger.reset()'s own
    per-test-isolation contract."""
    global _default
    _default = FragmentStore()
