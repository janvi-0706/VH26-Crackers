"""CoDel (RFC 8289), applied to P2 queue sojourn time only.

Owner: Lane A.

CoDel's whole point is the one most queueing controllers get wrong: it does
not look at queue *length* at all. A deep queue that is draining fast is
fine; a shallow queue where every item still waits too long is not. The only
signal in this file is **sojourn time** — how long an item actually sat in
the queue before being dequeued — because that is the thing a caller of this
pipeline actually experiences. No queue-length threshold appears anywhere
below, on purpose.

The control law, exactly as specified for this stage (a deliberate
simplification of RFC 8289's full drop-scheduling machinery — see the
module-level note under ``CoDelController`` for what is and is not carried
over):

    Track the minimum observed sojourn within each 100ms interval.
    Enter the sampling state only once that minimum has stayed above the
    500ms target for a FULL interval — one slow item does not trigger it;
    sustained congestion does.
    Exit the instant any single observed sojourn drops back below target —
    congestion clearing is trusted immediately, because continuing to
    sample after it clears only costs fidelity for no remaining reason.

That asymmetry (slow, confident entry; instant exit) is RFC 8289's own
design, not something invented here for this codebase.

This module only decides *whether* the system is in a sampling state. What
sampling actually does to an event (reservoir-sample 1-in-N, emit a rollup
instead of dropping) is ladder.py's job — this file has no opinion about
decisions, only about one boolean.
"""

from __future__ import annotations

from dataclasses import dataclass

# RFC 8289's own defaults. Nothing in this project's demo scale calls for
# retuning them: 100ms is fast enough to react within a spike's own ramp-up,
# and 500ms sits comfortably inside P2's own SLA range (log: 60s, click:
# 30s) — CoDel is meant to catch sustained queueing well before an SLA
# breach, not to *be* the SLA.
INTERVAL_SECONDS = 0.1
TARGET_SECONDS = 0.5

# `now` is real wall-clock time (time.time(), ~1.7e9 in magnitude) subtracted
# from a value of the same magnitude — exactly the floating-point precision
# case where two numbers this large lose enough low-order bits that a gap
# genuinely equal to INTERVAL_SECONDS comes out short. Measured directly at
# real epoch magnitude, not guessed: `(time.time() + 0.1) - time.time()`
# landed ~1e-7 short of `0.1` — a *toy* timestamp like `1000.0` doesn't show
# this at all (tested clean down to 1e-9), which is exactly why this needs
# a real epoch-scale measurement to catch, not just a unit test with small
# round numbers. 1e-4 (0.1ms) sits three orders of magnitude above that
# measured error — safely absorbing it — while staying three orders of
# magnitude below the 100ms interval itself, so it cannot meaningfully
# change when a real interval boundary is recognised. Without it, "a full
# interval elapsed" could silently miss its own boundary and delay entering
# the sampling state by one extra interval — wrong for a reason that has
# nothing to do with the actual control law.
_ELAPSED_EPSILON_SECONDS = 1e-4


@dataclass
class CoDelController:
    """One controller per queue being watched. This project watches exactly
    one: the P2 tier's queue sojourn, fed by metrics.observe_dequeue() at
    the moment each P2 event is dequeued (see that function's own docstring
    — it has named this controller as its consumer since Stage D).

    What this class deliberately does NOT implement from RFC 8289: the
    adaptive drop-frequency schedule (each successive drop happening sooner
    than the last, via a ``count``/``sqrt`` interval shrink). That machinery
    exists in the RFC to pace *repeated packet drops* against a control
    loop's own convergence — but this stage does not drop; it substitutes
    reservoir sampling instead (ladder.py), which has its own separate,
    much simpler pacing (fixed 1-in-N). Carrying over a scheduling
    mechanism designed to pace a different action than the one this
    codebase actually takes would be complexity with no corresponding
    behaviour to justify it — exactly the kind of building-ahead CLAUDE.md
    says not to do.
    """

    interval_seconds: float = INTERVAL_SECONDS
    target_seconds: float = TARGET_SECONDS

    def __post_init__(self) -> None:
        self._interval_start: float | None = None
        self._interval_min: float = float("inf")
        self._sampling: bool = False

    def update(self, sojourn_seconds: float, now: float) -> bool:
        """Feed one observed sojourn (seconds). Returns whether the sampling
        state is active immediately after this observation.

        Called once per P2 dequeue, in dequeue order — CoDel's own model is
        a continuous stream of per-item observations, not a periodic poll.
        """
        if self._interval_start is None:
            self._interval_start = now
            self._interval_min = sojourn_seconds
        else:
            self._interval_min = min(self._interval_min, sojourn_seconds)

        # Exit is immediate and unconditional, independent of the interval
        # timer below — see the module docstring for why entry and exit are
        # deliberately asymmetric.
        if self._sampling and sojourn_seconds < self.target_seconds:
            self._sampling = False

        elapsed = now - self._interval_start
        if elapsed >= self.interval_seconds - _ELAPSED_EPSILON_SECONDS:
            if not self._sampling and self._interval_min > self.target_seconds:
                self._sampling = True
            # A completed interval's job is done regardless of the outcome
            # above; start the next one fresh from this same observation so
            # a long gap between events doesn't retroactively count as
            # "the interval min" for time nothing was observed at all.
            self._interval_start = now
            self._interval_min = sojourn_seconds

        return self._sampling

    @property
    def sampling(self) -> bool:
        return self._sampling

    def reset(self) -> None:
        self._interval_start = None
        self._interval_min = float("inf")
        self._sampling = False


# --------------------------------------------------------------------------
# Ambient default controller — matches metrics.py/ledger.py/deferral.py's own
# precedent: one pipeline, one process (CLAUDE.md hard rule 1), so a single
# module-level instance costs nothing and saves every call site an argument.
# --------------------------------------------------------------------------

_default = CoDelController()


def observe(sojourn_seconds: float, now: float) -> bool:
    return _default.update(sojourn_seconds, now)


def is_sampling() -> bool:
    return _default.sampling


def reset() -> None:
    """Tests, and /control/reset, call this — a fresh demo run should not
    inherit a stale sampling verdict from before the reset."""
    _default.reset()
