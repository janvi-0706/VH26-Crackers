"""The event queue. Stage C: three tiered heaps replace the Stage B FIFO.

Owner: Lane A.

The public shape from Stage B survives unchanged — async ``put``/``get``,
instrumented at exactly those two points — so worker.py did not need to
change at all for this rewrite. What changed is what lives behind that
shape.

Selection policy, in order:

1. **Aging guard exception.** If the oldest P2 item's sojourn (now minus its
   ingest_ts) has crossed ``aging_guard_seconds``, serve that one item and
   stop — a single ``get()`` call never pulls more than one item, from any
   tier, ever. The *next* call re-evaluates from scratch: if another P2
   item is, on its own merits, also past the guard, the exception fires
   again for it too. That is a deliberately stronger guarantee than
   "unstick the queue once" — every individual aged item gets its own
   bounded wait, not just whichever one happened to be found first.
2. **Otherwise, the highest-priority non-empty tier wins**: P0, then P1,
   then P2.

That ordering is deliberately NOT "P0 fully before P1 before P2" as an
absolute guarantee — it is priority *with a starvation bound*. Read it
literally: on any call where no P2 item has aged past the guard, tier
priority alone decides, full stop. The bound only ever pulls one P2 item
out of turn, never more, and never touches P0/P1's own ordering.

Within a tier, ordering is:

- **P0: earliest deadline_ts wins (EDF)**, not arrival order. A payment that
  just arrived does not automatically jump an order that is closer to
  breaching its own SLA — see ``tests/test_queue.py`` for the concrete case
  CLAUDE.md asks us to be able to defend under questioning.
- **P1 and P2: arrival order** (by ``seq``, the classifier's monotonic
  pipeline sequence number — already globally unique, so no separate
  arrival counter is needed).

``set_mode("naive")`` switches the *selection policy only* — the same three
heaps stay the underlying storage. Naive picks the globally smallest
``seq`` across all three heaps every time, i.e. plain arrival order,
tier-blind: exactly the single FIFO Stage B had. Because storage never
changes, switching modes mid-flight needs no migration and loses nothing
that is already queued — it just changes what the next ``get()`` prefers.
This is the benchmark control and it has to keep working for the rest of
the project (see ``test_naive_mode_is_locked_to_pure_arrival_order``).
"""

from __future__ import annotations

import asyncio
import heapq
import time
from typing import Literal

from . import metrics
from .contracts import Event, Tier

Mode = Literal["adaptive", "naive"]

# How long a P2 item may sit behind P0/P1 traffic before the aging guard
# pulls it forward regardless of what else is queued. Chosen to be visible
# inside a single demo spike (tens of seconds) without being so aggressive
# that it undercuts the priority story the dashboard is telling — this is a
# starvation *bound*, not a scheduling policy of its own.
DEFAULT_P2_AGING_GUARD_SECONDS = 2.0

_TIER_PRIORITY: tuple[Tier, ...] = (Tier.P0, Tier.P1, Tier.P2)

# Heap entries are tuples ending in the Event, so heapq never has to compare
# two Event objects directly (it short-circuits on the first differing
# element). P0 entries carry `seq` as a tie-breaker specifically so two
# events with an identical deadline_ts never fall through to comparing
# Events, which would raise (pydantic models are not ordered).
P0Entry = tuple[float, int, Event]  # (deadline_ts, seq, event)
TierEntry = tuple[int, Event]  # (seq, event) — P1 and P2


class EventQueue:
    """Three tiered heaps behind the same instrumented put/get shape."""

    def __init__(
        self,
        *,
        mode: Mode = "adaptive",
        aging_guard_seconds: float = DEFAULT_P2_AGING_GUARD_SECONDS,
    ) -> None:
        if mode not in ("adaptive", "naive"):
            raise ValueError(f"unknown mode: {mode!r}")
        self._mode: Mode = mode
        self.aging_guard_seconds = aging_guard_seconds

        self._p0: list[P0Entry] = []
        self._p1: list[TierEntry] = []
        self._p2: list[TierEntry] = []
        self._heaps: dict[Tier, list] = {
            Tier.P0: self._p0,
            Tier.P1: self._p1,
            Tier.P2: self._p2,
        }

        # Multi-consumer wakeup: set whenever anything might be takeable,
        # cleared by whichever waiter next finds all three heaps empty. See
        # the module-level note in get() for why this needs no extra lock.
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

    def _enqueue(self, event: Event) -> None:
        if event.tier is Tier.P0:
            heapq.heappush(self._p0, (event.deadline_ts, event.seq, event))
        elif event.tier is Tier.P1:
            heapq.heappush(self._p1, (event.seq, event))
        else:
            heapq.heappush(self._p2, (event.seq, event))
        self._unfinished += 1
        self._all_done.clear()
        self._nonempty.set()

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

    def _try_take(self) -> Event | None:
        if self._mode == "naive":
            return self._take_naive()
        return self._take_adaptive()

    def _take_adaptive(self) -> Event | None:
        if self._p2:
            oldest_seq, oldest_event = self._p2[0]
            if time.time() - oldest_event.ingest_ts >= self.aging_guard_seconds:
                return self._pop(Tier.P2)
        for tier in _TIER_PRIORITY:
            if self._heaps[tier]:
                return self._pop(tier)
        return None

    def _take_naive(self) -> Event | None:
        """Pure arrival order, tier-blind — the Stage B FIFO's behaviour,
        rebuilt as a merge over the same three heaps. `seq` is globally
        unique and monotonic across every tier, so "smallest seq across all
        three heap heads" *is* "earliest arrival across the whole stream"."""
        best_tier: Tier | None = None
        best_seq: int | None = None
        for tier in _TIER_PRIORITY:
            heap = self._heaps[tier]
            if not heap:
                continue
            seq = heap[0][1] if tier is Tier.P0 else heap[0][0]
            if best_seq is None or seq < best_seq:
                best_seq, best_tier = seq, tier
        if best_tier is None:
            return None
        return self._pop(best_tier)

    def _pop(self, tier: Tier) -> Event:
        entry = heapq.heappop(self._heaps[tier])
        return entry[-1]  # the Event is always the last tuple element

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
        return len(self._p0) + len(self._p1) + len(self._p2)

    def empty(self) -> bool:
        return self.qsize() == 0

    def tier_depth(self, tier: Tier) -> int:
        """Current depth of one tier's heap. metrics.py tracks the same
        number independently (via observe_ingest/observe_dequeue) for the
        dashboard; this accessor exists for tests and debugging so nothing
        has to reach into the private heaps directly."""
        return len(self._heaps[tier])
