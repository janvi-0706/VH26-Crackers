"""Credit-based upstream backpressure — AIMD, per tier.

Owner: Lane A.

Every stage before this one protected P0 *downstream*: decide() (Stage D)
never batches/defers it, ladder.py (Stage E) never samples/sheds it. But
CLAUDE.md's hard rule 3 has said, since Stage A, "under pressure we throttle
the source instead" — and nothing built so far actually did that. The
generator kept trying to emit at whatever rate `/control/rate` (or the
SPIKE button) told it to, unconditionally. This module is the missing half:
admission control at the SOURCE, before an event even exists.

    The generator must acquire a credit before emitting (this stage's own
    spec). Denied, an emission attempt simply does not happen this tick —
    no event is created, nothing is classified, nothing reaches the queue.
    That is upstream backpressure: friction applied before entry, not a
    downstream decision about what to do with something already admitted.

AIMD — Additive Increase, Multiplicative Decrease, the same control law TCP
congestion control uses — governs each bucket's own sustainable admission
*rate* (work-units/sec), not a fixed per-request allowance:

    pressure <  HIGH_PRESSURE   -> rate/capacity creep up by
                                    ADDITIVE_INCREASE_UPS, checked at most
                                    once every INCREASE_CHECK_INTERVAL_S.
    pressure >= HIGH_PRESSURE   -> rate/capacity *= MULTIPLICATIVE_DECREASE,
                                    checked at most once every
                                    DECREASE_CHECK_INTERVAL_S (deliberately
                                    much shorter than the increase interval
                                    — AIMD's whole point is a slow climb and
                                    a fast retreat, not a symmetric one).

Critical (P0) vs bulk (P1, P2), not a single global bucket: CLAUDE.md hard
rule 3 forbids ever throttling P0, so its bucket is exempt from the
multiplicative decrease entirely and `try_acquire()` for it is
unconditional — a second, independent enforcement of the same rule
`decision.decide()`'s own defensive assert and `deferral.defer()`'s
ValueError already give it, in the same "enforced twice, not once" spirit.
"Critical sources retain credits far longer than bulk sources" (this
stage's own spec) is realised concretely: P0's capacity is never clawed
back by a decrease, so whatever burst allowance it has banked simply stays
banked, while a bulk bucket's capacity — and therefore whatever credits it
had banked past the new, smaller ceiling — shrinks together with its rate
the moment pressure crosses the line.

Each bulk tier's own bucket is independent: P1 being throttled hard does
not touch P2's own rate or vice versa, the same way ladder.py's two
reservoir samplers (click, log) never share state either.

This module needs to know which tier an EventType belongs to, and what it
costs, before generator.py has classified anything — it reads
`config.tiers[event_type]` directly for that, the same frozen table
classifier.py itself reads. That is not classification (no value, no
deadline, no seq are assigned here): it is a read-only lookup admission
control needs to gate correctly, nothing more.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .config import Config, load_config
from .contracts import EventType, Tier

# AIMD control law constants — this stage's own spec fixes the shape
# (additive increase while calm, x0.8 above 0.85); the rest are this
# module's own, reasoned choices, documented at each use.
HIGH_PRESSURE = 0.85
MULTIPLICATIVE_DECREASE = 0.8

# Work-units/sec added to a bulk bucket's rate (and capacity) per increase
# check. At the fast decrease cadence below, a bucket knocked all the way
# down to its floor recovers meaningful headroom within a few real seconds
# (25 u/s/sec while calm) rather than needing tens of seconds — visible
# recovery on a demo timescale, not an asymptote nobody watching would ever
# see complete.
ADDITIVE_INCREASE_UPS = 5.0

# The increase side is deliberately slow (checked rarely) and the decrease
# side deliberately fast (checked often) — that asymmetry, not the specific
# numbers, is what makes this AIMD rather than a symmetric rate limiter.
INCREASE_CHECK_INTERVAL_SECONDS = 0.2
# Matches metrics.py's own pressure-cache refresh cadence
# (_PRESSURE_REFRESH_SECONDS): checking for a decrease more often than
# pressure itself can actually change would just be re-applying the same
# verdict against a stale value.
DECREASE_CHECK_INTERVAL_SECONDS = 0.05

# A throttled bulk bucket never reaches literal zero — a trickle still
# gets through, matching this project's standing refusal to let anything
# go silently to nothing (P0 is never silently dropped downstream; this is
# the same ethos applied upstream: even the least-favoured source is still
# observably alive, not indistinguishable from broken).
MIN_BULK_RATE_UPS = 1.0


@dataclass
class CreditBucket:
    """One tier's admission gate. A token bucket whose own ceiling (rate,
    capacity) is itself under AIMD control, not fixed."""

    tier: Tier
    rate_ups: float
    capacity_units: float
    max_rate_ups: float
    critical: bool = False
    credits: float = 0.0
    denied_count: int = 0

    _last_refill: float | None = field(default=None, repr=False)
    # None, not 0.0: a fresh bucket's very first AIMD check must be allowed
    # to apply immediately (a real spike can hit at any wall-clock instant,
    # and CLAUDE.md hard rule 3 aside, nothing here should require an
    # interval's worth of real time to elapse before the very first
    # decrease can register). "Checked at most once per interval" only
    # starts rate-limiting *after* the first check has actually happened.
    _last_increase_check: float | None = field(default=None, repr=False)
    _last_decrease_check: float | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        # Start full: a fresh bucket should not itself be the reason the
        # very first burst after startup gets throttled.
        self.credits = self.capacity_units

    def _refill(self, now: float) -> None:
        if self._last_refill is None:
            self._last_refill = now
            return
        dt = max(0.0, now - self._last_refill)
        self.credits = min(self.capacity_units, self.credits + self.rate_ups * dt)
        self._last_refill = now

    def update_aimd(self, pressure: float, now: float) -> None:
        """Adjust this bucket's own ceiling given the live pressure signal.
        No-op for a critical bucket — its ceiling is never touched, which
        is exactly what "retain credits far longer" means for P0: nothing
        here ever claws it back."""
        if self.critical:
            return
        if pressure >= HIGH_PRESSURE:
            due = (
                self._last_decrease_check is None
                or now - self._last_decrease_check >= DECREASE_CHECK_INTERVAL_SECONDS
            )
            if due:
                self.rate_ups = max(MIN_BULK_RATE_UPS, self.rate_ups * MULTIPLICATIVE_DECREASE)
                self.capacity_units = max(
                    MIN_BULK_RATE_UPS, self.capacity_units * MULTIPLICATIVE_DECREASE
                )
                # The claw-back: banked credits above the new, smaller
                # ceiling are lost with it. A bulk source cannot ride out a
                # decrease on a reserve it built up before pressure rose.
                self.credits = min(self.credits, self.capacity_units)
                self._last_decrease_check = now
        else:
            due = (
                self._last_increase_check is None
                or now - self._last_increase_check >= INCREASE_CHECK_INTERVAL_SECONDS
            )
            if due:
                self.rate_ups = min(self.max_rate_ups, self.rate_ups + ADDITIVE_INCREASE_UPS)
                self.capacity_units = min(
                    self.max_rate_ups, self.capacity_units + ADDITIVE_INCREASE_UPS
                )
                self._last_increase_check = now

    def try_acquire(self, cost: float, now: float) -> bool:
        """Attempt to spend `cost` work-units of credit. A critical bucket
        never actually gates — CLAUDE.md hard rule 3, enforced here as a
        second, independent layer on top of decide()'s own unconditional
        P0 return and ladder.py's per-tier ceiling."""
        if self.critical:
            return True
        self._refill(now)
        if self.credits >= cost:
            self.credits -= cost
            return True
        self.denied_count += 1
        return False

    def reset(self) -> None:
        self.rate_ups = self.max_rate_ups
        self.capacity_units = self.max_rate_ups
        self.credits = self.capacity_units
        self.denied_count = 0
        self._last_refill = None
        self._last_increase_check = None
        self._last_decrease_check = None


def _make_buckets(config: Config) -> dict[Tier, CreditBucket]:
    """Seed each tier's bucket at that tier's own calibrated spike demand
    (config.demand_ups(spike_eps, tier)) — admission starts fully open
    (nothing gated at t=0) and only clamps down reactively once pressure
    actually crosses HIGH_PRESSURE, rather than needing a synthetic warm-up
    before it behaves sensibly. The additive-increase ceiling
    (`max_rate_ups`) is the same number: a bulk bucket's rate should never
    need to exceed the most this tier's own real traffic could ever
    organically demand."""
    spike_eps = config.spike_eps
    buckets: dict[Tier, CreditBucket] = {}
    for tier in (Tier.P0, Tier.P1, Tier.P2):
        demand_ups = config.demand_ups(spike_eps, tier)
        buckets[tier] = CreditBucket(
            tier=tier,
            rate_ups=demand_ups,
            capacity_units=demand_ups,
            max_rate_ups=demand_ups,
            critical=(tier is Tier.P0),
        )
    return buckets


class AdmissionControl:
    """One instance per Engine — mirrors WorkerPool/EventQueue's own
    per-Engine lifecycle (not ambient like metrics/ledger/deferral/sink/
    codel/ladder), because it is constructed straight from a `Config` at
    `__init__` and a benchmark comparing two configs side by side (Stage F)
    needs two independent instances, not one shared global. Tests and
    `/control/reset` therefore call `reset()` on the specific instance in
    play rather than a module-level reset function."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or load_config()
        self._buckets = _make_buckets(self.config)

    def tier_of(self, event_type: EventType) -> Tier:
        return self.config.tiers[event_type].tier

    def cost_of(self, event_type: EventType) -> float:
        return self.config.tiers[event_type].cost

    def try_acquire(self, event_type: EventType, pressure: float, now: float | None = None) -> bool:
        """Update this event type's tier bucket against the live pressure
        signal, then attempt to spend its credit. The one function
        generator.py calls before creating an event."""
        now = time.time() if now is None else now
        bucket = self._buckets[self.tier_of(event_type)]
        bucket.update_aimd(pressure, now)
        return bucket.try_acquire(self.cost_of(event_type), now)

    def bucket(self, tier: Tier) -> CreditBucket:
        return self._buckets[tier]

    def reset(self) -> None:
        for bucket in self._buckets.values():
            bucket.reset()
