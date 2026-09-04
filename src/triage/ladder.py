"""The five-rung escalation ladder, its per-tier ceilings, and what actually
happens on the two rungs above DEFER: reservoir-sampled rollups and hard
shedding.

Owner: Lane A.

    STREAM -> MICRO_BATCH -> DEFER -> SAMPLE_ROLLUP -> SHED

decision.decide() (Stage D) already chooses among the first three, from
per-event slack and system pressure. This module adds the two lossy rungs
above DEFER, and — just as importantly — the ceiling that makes CLAUDE.md
hard rule 3 structural rather than a convention every call site has to
remember:

    P0   caps at STREAM   — decide() already guarantees this unconditionally
                             (it returns before pressure is even consulted);
                             the cap here is a second, independent
                             enforcement. CLAUDE.md: hard rules are "enforced
                             twice, not once" elsewhere in this codebase
                             (decision.decide()'s own defensive assert,
                             deferral.defer()'s ValueError) for exactly the
                             same reason — a single enforcement point is one
                             refactor away from silently disappearing.
    P1   caps at DEFER     — inventory can wait, but it must never be
                             represented lossily. DEFER preserves every
                             field for later replay; SAMPLE_ROLLUP and SHED
                             both discard information P1 is not allowed to
                             lose.
    P2   uncapped          — click/log are exactly the tiers this stage's
                             lossy machinery exists to protect capacity from.

Two escalation triggers push a P2 event past DEFER, checked in this order:

    1. Hard shed, pressure >= HARD_SHED_PRESSURE (0.95). No sampling
       machinery involved — genuinely dropped, audited via ledger.record
       (through metrics.observe_decision(), the single choke point every
       decision already passes through), never silent.
    2. CoDel-triggered sampling (codel.py): while codel.is_sampling() is
       true for the live P2 sojourn signal, P2 events are reservoir-sampled
       instead of dropped — see ReservoirSampler below.

Both are P2-only. Nothing in this module ever escalates a P1 event; MAX_RUNG
exists as the second enforcement layer regardless, exercised directly in
tests rather than only trusted by inspection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum

from .contracts import Decision, Event, EventType, Tier

# --------------------------------------------------------------------------
# The ladder itself
# --------------------------------------------------------------------------


class Rung(IntEnum):
    STREAM = 0
    MICRO_BATCH = 1
    DEFER = 2
    SAMPLE_ROLLUP = 3
    SHED = 4


RUNG_DECISION: dict[Rung, Decision] = {
    Rung.STREAM: Decision.STREAM_NOW,
    Rung.MICRO_BATCH: Decision.MICRO_BATCH,
    Rung.DEFER: Decision.DEFER,
    Rung.SAMPLE_ROLLUP: Decision.SAMPLE_ROLLUP,
    Rung.SHED: Decision.SHED,
}
DECISION_RUNG: dict[Decision, Rung] = {v: k for k, v in RUNG_DECISION.items()}

MAX_RUNG: dict[Tier, Rung] = {
    Tier.P0: Rung.STREAM,
    Tier.P1: Rung.DEFER,
    Tier.P2: Rung.SHED,
}

# Pressure at/above which a P2 event is hard-shed instead of sampled. P2
# only — CLAUDE.md hard rule 3 protects P0 absolutely; nothing protects P2
# from this, by design, once pressure is this extreme.
HARD_SHED_PRESSURE = 0.95


def cap(tier: Tier, rung: Rung) -> Rung:
    """Clamp `rung` to `tier`'s ceiling (MAX_RUNG). The one function every
    escalation path is expected to route through last, regardless of how
    the rung was arrived at — the second, independent enforcement layer
    MAX_RUNG's own docstring describes."""
    return min(rung, MAX_RUNG[tier])


def escalate(
    tier: Tier,
    base_decision: Decision,
    pressure_value: float,
    codel_sampling: bool,
) -> tuple[Decision, str | None]:
    """Given decide()'s own answer for an event that has already cleared
    decide() (P0 never reaches here — decide() returns for it before
    pressure is even consulted), decide whether the ladder pushes it
    further: reservoir-sampled rollup while codel.py's controller says P2's
    sojourn has been elevated for a sustained interval, or hard shed above
    HARD_SHED_PRESSURE.

    CoDel sampling is checked FIRST, unconditionally — "when CoDel signals,
    do NOT drop" (this stage's own spec) is not conditioned on pressure
    being below the hard-shed line. Hard shedding is the fallback for
    when pressure is already extreme and CoDel is NOT (yet, or no longer)
    actively sampling: a sharper spike than CoDel's own 100ms-interval
    detection has caught up with yet, or a case noisy enough that CoDel
    never latched on. Checking pressure first instead — shedding whenever
    pressure is extreme regardless of CoDel's own state — was the first
    implementation here, and it was wrong: a real sustained spike drives
    pressure to ~1.0 for most of its duration (confirmed directly, not
    assumed — Stage D's own 30s-spike acceptance test's docstring says so),
    so "hard shed above 0.95" would have fired on nearly everything and
    CoDel's sampling path would rarely execute at all — the reservoir
    would sit almost empty while shed (genuinely, unrecoverably lost)
    climbed, making `weighted_click_count` diverge from `true_click_count`
    rather than track it. Reservoir sampling first is what actually lets
    this stage's own acceptance line ("we lost resolution, not
    information") be true.

    Returns `(decision, reason)` when the ladder overrides decide()'s
    answer, or `(base_decision, None)` when it does not — the `None` tells
    the caller to keep decide()'s own reason string rather than manufacture
    one for a decision that was never actually changed.

    Defensively capped: even though only P2 is ever escalated here, the
    result is still run through `cap()` before returning, so a future bug
    in this function's own tier check fails closed (clamped to the tier's
    ceiling) rather than failing open (an uncapped, silently wrong rung).
    """
    if tier is Tier.P2 and codel_sampling and DECISION_RUNG[base_decision] < Rung.SAMPLE_ROLLUP:
        decision, reason = (
            Decision.SAMPLE_ROLLUP,
            "CoDel: P2 queue sojourn sustained above 500ms target "
            "— reservoir sampling instead of streaming",
        )
    elif tier is Tier.P2 and pressure_value >= HARD_SHED_PRESSURE:
        decision, reason = (
            Decision.SHED,
            f"pressure {pressure_value:.2f} >= {HARD_SHED_PRESSURE:.2f} "
            "and CoDel is not already sampling — hard shed "
            "(P2 only, CLAUDE.md hard rule 3 does not apply here)",
        )
    else:
        return base_decision, None

    capped_rung = cap(tier, DECISION_RUNG[decision])
    return RUNG_DECISION[capped_rung], reason


# --------------------------------------------------------------------------
# Reservoir sampling — what SAMPLE_ROLLUP actually does
#
# "1 in N": every Nth SAMPLE_ROLLUP-routed event of a given type is kept
# (counted into the rollup's own observed_count and subtype_counts); the
# other N-1 are represented ONLY by sample_weight — never individually
# examined beyond incrementing a counter, which is the entire efficiency
# story (skipping N-1 out of every N expensive individual sink writes, at
# the cost of a bounded, honest, small loss of resolution).
#
# Window size is chosen to be exactly N events, not a fixed time duration:
# with a count-based window, observed_count is always exactly 1 and
# sample_weight is always exactly N, so `observed_count * sample_weight ==
# N == the true number of raw events the rollup actually covers` — an exact
# reconstruction by construction, not a statistical estimate that happens to
# land close. The "we lost resolution, not information" the CLAUDE.md
# acceptance line asks to prove is about individual payload/timing detail
# (N-1 out of N events' own fields are gone forever), not about the count,
# which this design keeps exactly right. The one honest, bounded exception:
# a window left less than N events full when CoDel exits sampling stays
# open, uncounted, until sampling next resumes and finishes it — up to N-1
# events' worth of transient undercount, negligible at any real N against
# the volumes a sustained spike actually produces (see RESERVOIR_N's own
# comment for the worked numbers).
# --------------------------------------------------------------------------

# 1 kept in 10. At the calibrated spike, P2 (click+log) arrives at roughly
# 265 events/sec combined; even split unevenly between the two types, each
# type's own reservoir closes multiple windows per second. The worst-case
# transient undercount this can ever contribute (one unfinished window, up
# to N-1 = 9 events) is under 4% of even a single second's worth of one
# type's traffic at spike, and shrinks further the longer sampling stays
# active — comfortably inside the acceptance line's 5% bound without being
# so large that a rollup carries no per-event resolution at all.
RESERVOIR_N = 10


@dataclass
class Rollup:
    """Mirrors docs/DATA_MODEL.md's `rollups` table exactly (minus
    `rollup_id`/`created_ts`/`schema_version`, which sink.py stamps at
    persistence time — this dataclass is the in-memory result of one
    finished reservoir window, not the durable row)."""

    event_type: str
    window_start: float
    window_end: float
    sample_weight: float
    observed_count: int
    subtype_counts: dict[str, int]
    seq_low: int
    seq_high: int


@dataclass
class ReservoirSampler:
    """One reservoir per event type. CoDel's sampling signal is type-blind
    (it watches P2 sojourn as a whole), but a rollup row is documented as
    covering one `event_type` (docs/DATA_MODEL.md: "Aggregated type, such
    as click or log") — so click and log each get their own instance,
    dispatched by the caller on `event.type`.
    """

    sample_n: int = RESERVOIR_N
    _count: int = field(default=0, repr=False)
    _window_start: float | None = field(default=None, repr=False)
    _seq_low: int | None = field(default=None, repr=False)
    _seq_high: int | None = field(default=None, repr=False)
    # The previous window's own window_end (or None before the first window
    # has ever closed) — see add()'s own comment on why the next window's
    # start is measured against this, not against `now` directly.
    _last_window_end: float | None = field(default=None, repr=False)

    def add(self, event: Event, now: float) -> Rollup | None:
        """Fold one SAMPLE_ROLLUP-routed event into the open window. Returns
        the finished Rollup once the window reaches `sample_n` events, else
        None — the caller (worker.py) only persists/accounts for a window
        once it is actually complete."""
        if self._window_start is None:
            # A window's start must be strictly after the PREVIOUS window's
            # end (DATA_MODEL.md's own unique index is on
            # (event_type, window_start, window_end) — "prevents duplicate
            # window output", not "assumes real time always moves enough
            # between windows to tell them apart"). At spike rate, a
            # RESERVOIR_N-sized window can close within the same tick of
            # the system clock's own resolution the next one opens in —
            # found empirically by stress-testing this exact path with an
            # artificially frozen clock, not guessed. Anchoring to the
            # previous window's own end rather than to `now` keeps windows
            # strictly ordered and distinct regardless of how fast real
            # time is actually moving.
            self._window_start = (
                now if self._last_window_end is None
                else max(now, self._last_window_end + 1e-6)
            )
        self._count += 1
        self._seq_low = event.seq if self._seq_low is None else min(self._seq_low, event.seq)
        self._seq_high = event.seq if self._seq_high is None else max(self._seq_high, event.seq)

        if self._count < self.sample_n:
            return None

        window_end = max(now, self._window_start + 1e-6)

        rollup = Rollup(
            event_type=event.type.value,
            window_start=self._window_start,
            window_end=window_end,
            sample_weight=float(self.sample_n),
            observed_count=1,
            subtype_counts={event.type.value: 1},
            seq_low=self._seq_low,
            seq_high=self._seq_high,
        )
        self._last_window_end = window_end
        self._count = 0
        self._window_start = None
        self._seq_low = None
        self._seq_high = None
        return rollup

    def reset(self) -> None:
        self._count = 0
        self._window_start = None
        self._seq_low = None
        self._seq_high = None
        self._last_window_end = None


# --------------------------------------------------------------------------
# Ambient default samplers — one per P2 type, matching codel.py/metrics.py's
# own ambient-singleton precedent (one pipeline, one process).
# --------------------------------------------------------------------------

_samplers: dict[EventType, ReservoirSampler] = {
    EventType.CLICK: ReservoirSampler(),
    EventType.LOG: ReservoirSampler(),
}


def add_to_reservoir(event: Event, now: float) -> Rollup | None:
    """Route `event` into its type's reservoir. Only ever called for P2
    events already routed to SAMPLE_ROLLUP — worker.py's job, not this
    module's, to have made that decision first."""
    sampler = _samplers.get(event.type)
    if sampler is None:
        # Every P2 EventType (click, log) has a sampler; a type this module
        # was never told about reaching here is a wiring bug upstream, not
        # a case to silently swallow.
        raise ValueError(f"no reservoir sampler registered for event type {event.type!r}")
    return sampler.add(event, now)


def reset_samplers() -> None:
    """Tests, and /control/reset, call this — an open partial window from
    before a reset must not silently resume counting toward a rollup whose
    seq range would then span across the reset boundary."""
    for sampler in _samplers.values():
        sampler.reset()
