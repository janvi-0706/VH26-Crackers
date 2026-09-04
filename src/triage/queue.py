"""The event queue. Stage D: score-ordered dequeue within each tier.

Owner: Lane A.

The public shape from Stage B survives unchanged — async ``put``/``get``,
instrumented at exactly those two points — so worker.py still did not need
to change for this rewrite. What changed, again, is what lives behind that
shape.

Selection policy, in order (unchanged from Stage C — this stage does not
touch tier selection, only ordering *within* a tier):

1. **P0 is absolute.** If P0 is non-empty, it wins, full stop — no exception
   in this file ever reaches past it. CLAUDE.md hard rule 3.
2. **Aging guard exception, P1 vs P2 only.** With P0 empty, if the
   chronologically oldest P2 item's sojourn has crossed
   ``aging_guard_seconds``, serve *that* item ahead of P1 and stop. This
   still picks by ingest_ts, not by score — the guard's whole job is
   "the one that has waited longest gets unstuck," which is a different
   question from "which item is most valuable to serve right now."
3. **Otherwise, the highest-priority non-empty tier wins**: P0, then P1,
   then P2.

What Stage D actually changes is *which item comes out of a tier* once that
tier has been chosen. Stage C used EDF for P0 and arrival order for P1/P2.
Stage D replaces both with :func:`triage.decision.score` — one formula,
applied uniformly to all three tiers (P0 included: it doesn't change P0's
*routing*, since P0 is always STREAM_NOW regardless, but a concurrently
queued P0 backlog is now served value-density-and-urgency first rather than
purely by deadline; the concrete Stage C test case — an order close to
breach dequeuing ahead of a fresher payment — still holds, because urgency
dominates density once slack is small, and dominates completely once slack
goes negative).

Why "settled" and "pending" per tier, not one list or a heap:

``score()`` depends on ``now`` in a way that only ever *increases* for a
fixed event (urgency climbs as slack shrinks then saturates; aging climbs as
age grows) — there is no sort key computed once at insertion that stays
valid as real time passes without being recomputed. A plain ``heapq`` keyed
on such a value would silently go stale. The other extreme — a fresh
``score()`` for every item on every single dequeue — is correct but was
measured to cost real time at this project's own demo scale: a full scan of
a 10,000-deep backlog (the kind Stage C's own sustained-spike testing
produced) took ~7-8ms, called up to ~200 times/sec, which is enough to
visibly steal event-loop time from the workers themselves — precisely the
class of bug already found once in this project (the Stage C generator
pacing fix).

The middle ground: each tier keeps a ``_settled`` list, kept in ascending
score order as of the last resort (so the best item is at the end — O(1) to
pop), and a small ``_pending`` list of arrivals since that resort. A resort
(one O(n log n) sort of settled+pending merged) happens at most once every
``RESORT_INTERVAL_SECONDS``. Between resorts, a pop only has to compare
``_pending``'s own best (a live-recomputed, cheap linear scan — pending
stays small, bounded by roughly arrival_rate x RESORT_INTERVAL) against
``_settled[-1]`` (also freshly re-scored, one call) — O(len(pending)) per
pop, not O(len(tier)). The result is exact at the moment of every resort,
and staleness in between is bounded to at most one interval's worth of
score drift — negligible against SLAs measured in hundreds of milliseconds
to tens of seconds.

``set_mode("naive")`` switches the *selection policy only*. Naive picks the
globally smallest ``seq`` across every item in every tier (settled and
pending both) every time — plain arrival order, tier-blind, exactly the
single FIFO Stage B had, and completely unaffected by Stage D's scoring
machinery (naive never calls ``score()`` at all). This is the benchmark
control and it has to keep working for the rest of the project.
"""

from __future__ import annotations

import asyncio
import time
from typing import Literal

from . import decision, metrics
from .config import Config, load_config
from .contracts import Event, Tier

Mode = Literal["adaptive", "naive"]

# How long a P2 item may sit behind P0/P1 traffic before the aging guard
# pulls it forward regardless of what else is queued. Chosen to be visible
# inside a single demo spike (tens of seconds) without being so aggressive
# that it undercuts the priority story the dashboard is telling — this is a
# starvation *bound*, not a scheduling policy of its own.
DEFAULT_P2_AGING_GUARD_SECONDS = 2.0

# How often a tier's score order is fully rebuilt. Short enough that
# staleness is invisible against real SLAs; long enough that the O(n log n)
# resort cost stays a small fraction of a second even at a 10,000-deep
# backlog (measured ~7-8ms at that depth — under 1% of one second when paid
# once per interval, versus paying an O(n) scan on every single dequeue).
RESORT_INTERVAL_SECONDS = 0.05

_TIER_PRIORITY: tuple[Tier, ...] = (Tier.P0, Tier.P1, Tier.P2)


class EventQueue:
    """Three tiers, each a settled/pending pair scored live at dequeue
    time — behind the same instrumented put/get shape."""

    def __init__(
        self,
        *,
        config: Config | None = None,
        mode: Mode = "adaptive",
        aging_guard_seconds: float = DEFAULT_P2_AGING_GUARD_SECONDS,
    ) -> None:
        if mode not in ("adaptive", "naive"):
            raise ValueError(f"unknown mode: {mode!r}")
        self.config = config or load_config()
        self._mode: Mode = mode
        self.aging_guard_seconds = aging_guard_seconds

        # _settled[tier]: ascending score order as of the last resort — the
        # best item is always the *last* element, so popping it is O(1).
        # _pending[tier]: unsorted arrivals since that resort; kept small.
        self._settled: dict[Tier, list[Event]] = {t: [] for t in _TIER_PRIORITY}
        self._pending: dict[Tier, list[Event]] = {t: [] for t in _TIER_PRIORITY}
        self._resort_ts: dict[Tier, float] = {t: 0.0 for t in _TIER_PRIORITY}

        # Multi-consumer wakeup: set whenever anything might be takeable,
        # cleared by whichever waiter next finds every tier empty. See the
        # module-level note in get() for why this needs no extra lock.
        self._nonempty = asyncio.Event()

        # Mirrors asyncio.Queue's task_done()/join() contract exactly, so
        # worker.py's unconditional `finally: queue.task_done()` and any
        # future `await queue.join()` behave the same as they did in Stage B.
        self._unfinished = 0
        self._all_done = asyncio.Event()
        self._all_done.set()

    # -- mode ---------------------------------------------------------------

    @property
    def mode(self) -> Mode:
        return self._mode

    def set_mode(self, mode: Mode) -> None:
        """Switch the selection policy. Storage is untouched — nothing
        queued is lost or reordered on disk, only what the *next* get()
        prefers changes."""
        if mode not in ("adaptive", "naive"):
            raise ValueError(f"unknown mode: {mode!r}")
        self._mode = mode

    # -- put ------------------------------------------------------------------

    async def put(self, event: Event) -> None:
        """Admit one event. Counted as ingested the instant it is queued,
        not when a worker eventually picks it up."""
        metrics.observe_ingest(event)
        self._enqueue(event)

    def put_nowait(self, event: Event) -> None:
        """Synchronous admit, for tests and for pre-loading a backlog."""
        metrics.observe_ingest(event)
        self._enqueue(event)

    def put_replayed(self, event: Event) -> None:
        """Re-admit an event the deferral drainer is replaying. Deliberately
        NOT put()/put_nowait(): this event was already counted once in
        `ingested` when it first arrived, and metrics.observe_replay (not
        observe_ingest) is what keeps the conservation equation from
        double-counting it — see that function's own docstring. Used only
        by deferral.py's drainer."""
        metrics.observe_replay(event)
        self._enqueue(event)

    def _enqueue(self, event: Event) -> None:
        # Always into pending: settled's sortedness must never be disturbed
        # by an append, or popping its last element stops being O(1)-safe.
        self._pending[event.tier].append(event)
        self._unfinished += 1
        self._all_done.clear()
        self._nonempty.set()

    def _tier_events(self, tier: Tier) -> list[Event]:
        """Every item currently held by one tier, settled and pending both.
        O(n) — used only by the paths that already have to look at
        everything anyway (naive mode, the aging guard's oldest-item check,
        clear(), depth accounting), never by the score-ordered hot path."""
        return self._settled[tier] + self._pending[tier]

    # -- get ------------------------------------------------------------------

    async def get(self) -> Event:
        """Take the next event under the current mode's selection policy.
        Its queue wait is measured from here."""
        while True:
            event = self._try_take()
            if event is not None:
                metrics.observe_dequeue(event)
                return event
            # Nothing takeable right now. Clear-then-wait with no await in
            # between is safe on a single-threaded event loop: no other
            # coroutine can run (and therefore no put() can sneak in and
            # call .set()) between the two statements below, since only an
            # await point yields control.
            self._nonempty.clear()
            await self._nonempty.wait()

    def try_get(self) -> Event | None:
        """Non-blocking get: the current best event under the active
        policy, or None immediately if nothing is takeable right now —
        never waits. Used by worker.py while gathering a MICRO_BATCH: a
        batch worth waiting to fill would add latency exactly where
        batching is supposed to save it, so gathering only ever takes what
        is *already* available."""
        event = self._try_take()
        if event is not None:
            metrics.observe_dequeue(event)
        return event

    def _try_take(self) -> Event | None:
        if self._mode == "naive":
            return self._take_naive()
        return self._take_adaptive()

    def _take_adaptive(self) -> Event | None:
        # P0 is absolute: no exception below this line ever reaches it.
        if self._settled[Tier.P0] or self._pending[Tier.P0]:
            return self._pop_best_by_score(Tier.P0)

        # The aging guard only ever arbitrates P1 vs P2 — see the module
        # docstring for exactly why it must stop here and go no further.
        # It picks by ingest_ts (chronologically oldest), not by score:
        # "unstick whoever has waited longest" is a different question from
        # "who is most valuable to serve right now".
        p2_all = self._tier_events(Tier.P2)
        if p2_all:
            oldest = min(p2_all, key=lambda e: e.ingest_ts)
            if time.time() - oldest.ingest_ts >= self.aging_guard_seconds:
                self._remove(Tier.P2, oldest)
                return oldest

        if self._settled[Tier.P1] or self._pending[Tier.P1]:
            return self._pop_best_by_score(Tier.P1)
        if p2_all:
            return self._pop_best_by_score(Tier.P2)
        return None

    def _take_naive(self) -> Event | None:
        """Pure arrival order, tier-blind — the Stage B FIFO's behaviour.
        `seq` is globally unique and monotonic across every tier, so
        "smallest seq across every item in every tier" *is* "earliest
        arrival across the whole stream". Never calls score() — naive is
        the benchmark control and must be completely unaffected by
        Stage D's scoring machinery."""
        best_tier: Tier | None = None
        best_event: Event | None = None
        for tier in _TIER_PRIORITY:
            for candidate in self._tier_events(tier):
                if best_event is None or candidate.seq < best_event.seq:
                    best_tier, best_event = tier, candidate
        if best_tier is None or best_event is None:
            return None
        self._remove(best_tier, best_event)
        return best_event

    def _remove(self, tier: Tier, event: Event) -> None:
        """Remove one specific event from wherever it lives (settled or
        pending) for this tier."""
        try:
            self._pending[tier].remove(event)
        except ValueError:
            self._settled[tier].remove(event)

    def _maybe_resort(self, tier: Tier, now: float) -> None:
        if not self._pending[tier] and self._settled[tier]:
            # Nothing new since the last resort and there's still something
            # settled to serve from — no need to force a resort just because
            # the clock ticked; the next pop's live comparison is exact
            # either way once pending is empty (there is nothing to weigh
            # settled's tail against).
            return
        if now - self._resort_ts[tier] < RESORT_INTERVAL_SECONDS and self._settled[tier]:
            return
        capacity = self.config.worker_capacity_ups
        merged = self._settled[tier] + self._pending[tier]
        weights = decision.current_score_weights
        merged.sort(key=lambda e: (decision.score(e, now, capacity, weights), -e.seq))
        self._settled[tier] = merged
        self._pending[tier] = []
        self._resort_ts[tier] = now

    def _pop_best_by_score(self, tier: Tier) -> Event:
        """The highest-`decision.score()` item in this tier, right now.

        Resorts at most once per RESORT_INTERVAL_SECONDS (see
        _maybe_resort). Between resorts, only `_pending[tier]` — bounded by
        roughly arrival_rate x RESORT_INTERVAL_SECONDS, not the whole tier
        — is scanned live; it is compared against the settled list's own
        current tail, itself re-scored fresh for this one comparison.
        """
        now = time.time()
        self._maybe_resort(tier, now)
        capacity = self.config.worker_capacity_ups
        weights = decision.current_score_weights

        settled = self._settled[tier]
        pending = self._pending[tier]

        best_pending: Event | None = None
        best_pending_score = -1.0
        for candidate in pending:
            candidate_score = decision.score(candidate, now, capacity, weights)
            if best_pending is None or candidate_score > best_pending_score or (
                candidate_score == best_pending_score and candidate.seq < best_pending.seq
            ):
                best_pending, best_pending_score = candidate, candidate_score

        if best_pending is None:
            return settled.pop()
        if not settled:
            pending.remove(best_pending)
            return best_pending

        settled_score = decision.score(settled[-1], now, capacity, weights)
        if best_pending_score > settled_score or (
            best_pending_score == settled_score and best_pending.seq < settled[-1].seq
        ):
            pending.remove(best_pending)
            return best_pending
        return settled.pop()

    # -- asyncio.Queue-compatible bookkeeping --------------------------------

    def task_done(self) -> None:
        if self._unfinished <= 0:
            raise ValueError("task_done() called more times than there were items")
        self._unfinished -= 1
        if self._unfinished == 0:
            self._all_done.set()

    async def join(self) -> None:
        """Wait until every put() has a matching task_done()."""
        await self._all_done.wait()

    def qsize(self) -> int:
        return sum(len(self._settled[t]) + len(self._pending[t]) for t in _TIER_PRIORITY)

    def empty(self) -> bool:
        return self.qsize() == 0

    def clear(self) -> None:
        """Drop everything currently *queued* (not yet taken by a worker),
        across all three tiers.

        For ``/control/reset``: the demo needs to walk back to a clean
        baseline mid-presentation without restarting the process. This
        method only owns storage — the call site (``Engine.reset`` in
        app.py) is responsible for also resetting ``metrics``/``ledger``,
        so the dropped items don't linger as phantom queue depth.

        Deliberately does NOT touch items a worker has already ``get()``'d
        and is mid-``serve()`` on — those are not in these lists any more,
        so ``clear()`` cannot see them, and must not assume they don't
        exist. Zeroing ``_unfinished`` unconditionally here was an earlier
        version of this method's actual bug: a worker's later
        ``task_done()`` for that in-flight item would then find nothing
        outstanding and raise, which happens inside worker.py's `finally`
        block — outside the `except Exception` guard — silently killing
        that worker. Decrementing by exactly what was dropped keeps
        in-flight items correctly accounted for.
        """
        dropped = self.qsize()
        for tier in _TIER_PRIORITY:
            self._settled[tier].clear()
            self._pending[tier].clear()
        self._unfinished -= dropped
        if self._unfinished == 0:
            self._all_done.set()
        self._nonempty.clear()

    def tier_depth(self, tier: Tier) -> int:
        """Current depth of one tier. metrics.py tracks the same number
        independently (via observe_ingest/observe_dequeue) for the
        dashboard; this accessor exists for tests and debugging so nothing
        has to reach into the private lists directly."""
        return len(self._settled[tier]) + len(self._pending[tier])
