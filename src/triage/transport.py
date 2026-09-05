"""Dispatch tracking between ingress and the two downstream servers.

Owner: Lane A (Phase J2).

Interface only, per this phase's own instruction — the four functions
below (`dispatch`, `ack`, `outstanding`, `redispatch_expired`) are the
complete public surface, and their SHAPE is what this phase actually
builds: ingress records every dispatch with a timestamp; anything
unacknowledged past `ack_timeout_ms` gets automatically re-dispatched.
Idempotency keys (docs/DATA_MODEL.md's own identity model, unchanged) are
what make that safe rather than a double-charge — a re-dispatched event is,
at every field but `event_id`/`seq`, indistinguishable from the delivery
`sink.py`'s own upsert-by-`idempotency_key` already treats a genuine retry
as (see that module's own docstring), and `dedup.py` gives an identical
defence on the ingest side of this same identity model.

What actually MOVES an event to a server is injected via `configure()`,
not hardcoded here — this module owns the tracking (timestamps, acks,
timeout, redispatch), never the transport itself. Today (Phase J2) that
injected function is a same-process call, matching exactly how
`worker.py` already takes `sink_write`/`defer` as constructor-injected
callables instead of importing `sink.write`/`deferral.defer` directly:
this file, `app.py`'s eventual Engine wiring, and every test in
`tests/test_transport.py` can all supply a plain Python function. Phase
J3 replaces that one function with a real batched-HTTP client — this
module's own four functions, their contracts, and every test that exercises
them do not change; only what happens inside the injected callable does.

Not durable. Every dispatch record here lives in this module's own
process memory, same as `checkpoint.py`'s in-flight table (that module's
own docstring already explains why a worker-death recovery mechanism does
not need SQLite-grade durability) — but see docs/PHASE-J-INSPECTION.md
section 5 for the harder case this module is the eventual, ingress-side
answer to: a downstream SERVER dying (not one task inside a
still-running process) with events dispatched to it but never acked. This
file's own `redispatch_expired()` is that answer's first piece — the part
that notices and retries — not the complete exactly-once story Phase J3's
own dispatch tracking still has to finish (a redispatch after a real
downstream crash and a redispatch after a merely-slow ack are
indistinguishable from ingress's side, by design: both are simply "no ack
within ack_timeout_ms", which is the whole reason idempotent redelivery,
not perfect failure detection, is the mechanism this project relies on).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from .contracts import Event
from .servers_config import load_servers_config

DeliverFn = Callable[[str, list[Event]], Awaitable[None]]


@dataclass(frozen=True)
class DispatchResult:
    """What `dispatch()` hands back — enough for the caller to later
    `ack()` exactly this batch, or to simply let it time out."""

    dispatch_id: str
    server: str
    event_ids: tuple[str, ...]
    dispatched_ts: float


@dataclass
class _DispatchRecord:
    server: str
    dispatched_ts: float
    # event_id -> Event, mutated as partial acks arrive. A dict, not a
    # list: ack() removes specific event_ids from a batch that may only be
    # PARTIALLY acknowledged (a real transport can plausibly ack the 18 of
    # 20 events in a batch that a downstream server actually finished
    # before the 19th and 20th's own acks are still in flight), and a dict
    # makes that an O(1) removal per id rather than a linear scan.
    events: dict[str, Event] = field(default_factory=dict)


class Transport:
    """One instance tracks dispatch state for every downstream server it
    is configured to reach. Constructor-injected `deliver`, matching
    `WorkerPool`'s own `sink_write`/`defer` precedent — see this module's
    docstring for why."""

    def __init__(
        self,
        deliver: DeliverFn,
        *,
        ack_timeout_ms: float | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._deliver = deliver
        self._ack_timeout_ms = (
            ack_timeout_ms
            if ack_timeout_ms is not None
            else load_servers_config().transport.ack_timeout_ms
        )
        self._now = now
        self._outstanding: dict[str, _DispatchRecord] = {}
        self._next_id = 0

    def _fresh_dispatch_id(self) -> str:
        self._next_id += 1
        return f"dispatch-{self._next_id}"

    async def dispatch(self, server: str, events: list[Event]) -> DispatchResult:
        """Hand `events` to `server` via the injected `deliver`, and
        record the attempt with a timestamp before returning. Recording
        happens regardless of whether `deliver` itself has actually
        finished sending anything anywhere by the time this returns
        (Phase J3's real HTTP client will batch/queue internally per
        `config/servers.yaml`'s own `batch_window_ms`) — what matters for
        `redispatch_expired()` is "when did ingress consider this
        attempted", not "when did the network call return"."""
        if not events:
            raise ValueError("dispatch() requires at least one event")
        dispatch_id = self._fresh_dispatch_id()
        now = self._now()
        self._outstanding[dispatch_id] = _DispatchRecord(
            server=server,
            dispatched_ts=now,
            events={e.event_id: e for e in events},
        )
        await self._deliver(server, events)
        return DispatchResult(
            dispatch_id=dispatch_id,
            server=server,
            event_ids=tuple(e.event_id for e in events),
            dispatched_ts=now,
        )

    async def ack(self, dispatch_id: str, event_ids: list[str]) -> None:
        """Confirm that `event_ids` (a subset of, or all of, one
        dispatch's own events) genuinely finished downstream. An unknown
        `dispatch_id` (already fully acked, already redispatched and
        therefore replaced by a new id, or simply never issued) is a
        no-op, not an error — by the time an ack for a since-redispatched
        batch arrives late, ingress has already moved on, and there is
        nothing left for a stale ack to correct."""
        record = self._outstanding.get(dispatch_id)
        if record is None:
            return
        for event_id in event_ids:
            record.events.pop(event_id, None)
        if not record.events:
            del self._outstanding[dispatch_id]

    def outstanding(self, server: str) -> list[Event]:
        """Every event currently dispatched to `server` with no ack yet,
        oldest dispatch first, in each dispatch's own original order."""
        result: list[Event] = []
        for record in sorted(
            (r for r in self._outstanding.values() if r.server == server),
            key=lambda r: r.dispatched_ts,
        ):
            result.extend(record.events.values())
        return result

    async def redispatch_expired(self) -> int:
        """Sweep every dispatch older than `ack_timeout_ms` with events
        still unacked, and re-dispatch exactly those remaining events as a
        BRAND NEW dispatch (a new `dispatch_id`, a new timestamp) — the
        expired record is discarded, not retried in place, so a late ack
        for the old `dispatch_id` correctly becomes the no-op `ack()`'s
        own docstring describes rather than silently re-clearing an event
        the new dispatch is now responsible for.

        Returns the number of EVENTS redispatched (not the number of
        dispatch records), matching the granularity every other counter
        in this project reports at (`metrics.observe_retry`'s own
        `retries` counter is likewise per-event, not per-batch).
        """
        now = self._now()
        timeout_seconds = self._ack_timeout_ms / 1000.0
        expired_ids = [
            dispatch_id
            for dispatch_id, record in self._outstanding.items()
            if now - record.dispatched_ts >= timeout_seconds
        ]
        redispatched_count = 0
        for dispatch_id in expired_ids:
            record = self._outstanding.pop(dispatch_id)
            events = list(record.events.values())
            if not events:
                continue
            await self.dispatch(record.server, events)
            redispatched_count += len(events)
        return redispatched_count

    def reset(self) -> None:
        """Tests only."""
        self._outstanding.clear()
        self._next_id = 0


# --------------------------------------------------------------------------
# Ambient default, matching metrics.py/ledger.py/sink.py/deferral.py's own
# precedent (one pipeline, one process, still true pre-split) — but unlike
# those, there is no universally correct default `deliver`: what "dispatch
# to server1" actually DOES is a real design decision the current
# single-process build has not made yet (there is one Engine, one queue,
# no separate per-tier destination to call into). `configure()` is the
# seam: until something calls it, the ambient default raises rather than
# silently pretending to deliver, so a caller that forgot to wire this up
# fails loudly instead of dropping events with no error.
# --------------------------------------------------------------------------


async def _unconfigured_deliver(server: str, events: list[Event]) -> None:
    raise RuntimeError(
        f"transport.dispatch({server!r}, ...) called before transport.configure() "
        "wired up a real delivery function — see this module's own docstring"
    )


_default: Transport = Transport(deliver=_unconfigured_deliver)


def configure(deliver: DeliverFn, *, ack_timeout_ms: float | None = None) -> None:
    """Wire the ambient default `Transport` to a real delivery function.
    Phase J2's own callers are `tests/test_transport.py` (a same-process
    stub proving the dispatch/ack/timeout contract) and, once `app.py`
    wires an Engine to it, a same-process call into that Engine's own
    per-tier queue; Phase J3 replaces the function passed here with an
    HTTP client, changing nothing else."""
    global _default
    _default = Transport(deliver=deliver, ack_timeout_ms=ack_timeout_ms)


async def dispatch(server: str, events: list[Event]) -> DispatchResult:
    return await _default.dispatch(server, events)


async def ack(dispatch_id: str, event_ids: list[str]) -> None:
    await _default.ack(dispatch_id, event_ids)


def outstanding(server: str) -> list[Event]:
    return _default.outstanding(server)


async def redispatch_expired() -> int:
    return await _default.redispatch_expired()


def reset_default() -> None:
    """Tests only — back to the unconfigured, loudly-failing default."""
    global _default
    _default = Transport(deliver=_unconfigured_deliver)
