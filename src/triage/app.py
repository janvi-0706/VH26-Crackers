"""FastAPI application: wires the vertical slice into one asyncio event loop.

Owner: Lane D.

Two modes, chosen once at process start, never mixed:

  real  (default)  generator -> classifier -> queue -> worker pool -> sink,
                    running as background tasks in the app's own event loop.
                    /ws streams metrics.snapshot() at 4 Hz.
  fake  (--fake)    no engine at all. /ws streams triage.fake_metrics
                    instead, so Lane C can build the whole dashboard before
                    Stage C's priority/tiers exist — the reason
                    fake_metrics.py was built in Stage A.

Stage D: `Engine` also owns the deferred-buffer drainer's lifecycle
(started alongside the worker pool, stopped alongside it) — the actual
decision-making (score, pressure, batch vs defer) lives in queue.py and
worker.py; this file just starts and stops the background tasks.

Phase J3: this process IS "ingress" in the three-process topology
docs/PHASE-J-INSPECTION.md names — `/ack` and `/metrics/report` below are
its receiving half of transport.py/reporting.py's own wire protocol, real
regardless of `--transport`. `--transport` itself only chooses which
`transport.py` configuration is active (`direct`: a same-process loopback
deliver, the demo fallback this phase's own prompt asks for; `http`: a
real pooled `httpx.AsyncClient`, the batcher, and the background
redispatch sweep) — it does NOT yet reroute `Engine._ingest()`'s own
generate -> classify -> queue -> worker pipeline through
`transport.submit()`. That is real, separate scope for a later prompt:
this phase's own instruction is "implement transport.py and reporting.py
over HTTP," not "make Engine dispatch through them," and CLAUDE.md's own
working-style rule ("if you think a later feature is needed now, say so
and wait") is why that wiring is named here rather than done silently.

Phase J6: ingress is the SINGLE WRITER for history.db. `--persist` (off
by default — see `history_db.py`'s own docstring for why this is opt-in,
not automatic) opens one real, WAL-mode SQLite file and points
sink.py/ledger.py/deferral.py's own ambient defaults at it. `/ack`'s own
wire shape grows optional fields (`events`, `decision`, `reason`,
`pressure`, `source`) so a real server1/server2 completion can durably
write `events_sink` + `audit_ledger` + `decision_traces` + `sla_outcomes`
here in the SAME request that already clears transport's own dispatch
bookkeeping — old callers that send only `event_ids` (existing tests,
`--transport=direct`'s own loopback ack) are unaffected; `/ack` still
just clears transport bookkeeping for them. `/defer` now also clears that
bookkeeping (a successfully-durable DEFER is a resolved dispatch, not an
outstanding one) and tags the row `origin='server2'`, so a SEPARATE
background drainer (started here, not inside `Engine`, since this is
ingress's own cross-process concern) can redispatch it back to a real
server2 — gated on server2's own reported pressure via
`reporting.fragments("server2")`, never on Engine's local
`metrics.current_pressure()`, and never averaged across server2
instances (Phase J5's own rule).
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import decision, deferral, history_db, ledger, metrics, pg_compat, reporting, sink, transport
from .classifier import Classifier
from .config import Config, load_config
from .contracts import Decision, DecisionTrace, Event, EventType, MetricsFrame, ShedRecord, Tier
from .costmodel import CostModel
from .dedup import Deduplicator
from .fake_metrics import FakeSource
from .generator import EventGenerator, GeneratedEvent
from .ladder import Rollup as LadderRollup
from .queue import EventQueue, Mode as QueueMode
from .servers_config import load_servers_config
from .worker import WorkerPool

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_DIST = REPO_ROOT / "dashboard" / "dist"

SNAPSHOT_HZ = 4.0
SNAPSHOT_PERIOD = 1.0 / SNAPSHOT_HZ

# The SPIKE button's target rate. A fixed demo constant, not derived from
# config/tiers.yaml's spike_multiplier — the button has to reproduce the
# exact same number every time regardless of later tuning, and 20000/min is
# the literal figure the spec names (it happens to land within 0.1% of
# config's own calibrated spike_eps of 333.0, which is reassuring, not load
# bearing). "Instant jump, no ramp": set_rate() takes effect on the very
# next emission, since the generator reads its rate fresh every loop.
SPIKE_EVENTS_PER_MINUTE = 20_000
SPIKE_RATE_EPS = SPIKE_EVENTS_PER_MINUTE / 60.0


def _server1_pressure() -> float:
    """Fails OPEN (0.0), not closed — unlike J6's own redispatch gate
    (`_server2_pressure_safe_to_drain`, which fails closed because the
    risky action there is "resume sending work into an unknown target").
    This function feeds admission's own AIMD control law instead: failing
    closed here would ratchet every bulk bucket's ceiling down hard for
    the first fragment_ttl_ms of every fresh start (before server1/
    server2 have pushed anything yet), an artificial cold-start throttle
    the monolith's own metrics.current_pressure() never had (it starts
    genuinely at 0.0, not defensively at 1.0) — found empirically while
    smoke-testing `make dev-split` fresh, not assumed."""
    fragments = reporting.fragments("server1")
    if not fragments:
        return 0.0
    return max(f.counters.get("pressure", 0.0) for f in fragments)


def _server2_pressure() -> float:
    """See `_server1_pressure()`'s own docstring for why this fails open."""
    fragments = reporting.fragments("server2")
    if not fragments:
        return 0.0
    return max(f.counters.get("pressure", 0.0) for f in fragments)


def _server_pressure_source(event_type: EventType, now: float) -> float:
    """Phase J7: P0 credits respond to server1's own pressure only; P1/P2
    respond to server2's — never averaged, and never each other's. P0's
    own bucket is `critical` (admission.py) so this value never actually
    gates it, but it is still the honest, correctly-attributed signal for
    the AIMD bookkeeping and for anything reading it (a dashboard gauge)."""
    del now
    tier = load_config().tiers[event_type].tier
    return _server1_pressure() if tier is Tier.P0 else _server2_pressure()


class Engine:
    """The real generator -> classifier -> queue -> workers pipeline.

    Everything here runs as background asyncio tasks inside the app's own
    event loop — there is no separate process or thread, per CLAUDE.md hard
    rule 1 (single Python process, asyncio, in-memory).
    """

    def __init__(
        self, *, config: Config | None = None, seed: int | None = None,
        dispatch_via_transport: bool = False,
    ) -> None:
        self.config = config or load_config()
        # Phase J7: when True, _ingest() dispatches every admitted event to
        # the real server1/server2 split via transport.submit() instead of
        # this process's own local queue/workers, and admission reads each
        # tier's pressure from that server's own reported fragment (never
        # averaged across P0/P1/P2, never averaged across server2
        # instances) instead of this process's own local pressure.
        self.dispatch_via_transport = dispatch_via_transport
        self.generator = EventGenerator(
            config=self.config, seed=seed,
            pressure_source=_server_pressure_source if dispatch_via_transport else None,
        )
        self.classifier = Classifier(config=self.config)
        # Per-Engine, not ambient — same reasoning as generator.admission
        # (AdmissionControl): a fresh Engine, or a /control/reset, must not
        # inherit another run's learned costs. The SAME instance is handed
        # to both the queue (ordering) and the worker pool (routing +
        # learning), so the two halves of decision-making never disagree
        # about what "the current estimate" is.
        self.cost_model = CostModel(config=self.config)
        self.queue = EventQueue(config=self.config, cost_model=self.cost_model)
        self.workers = WorkerPool(self.queue, config=self.config, cost_model=self.cost_model)
        # Per-Engine, not ambient — same reasoning as generator.admission
        # (AdmissionControl): a fresh Engine, or a /control/reset, must not
        # inherit another run's dedup state.
        self.dedup = Deduplicator()
        self._stop = asyncio.Event()
        self._ingest_task: asyncio.Task[None] | None = None
        self._drain_stop: asyncio.Event | None = None
        self._drain_task: asyncio.Task[None] | None = None

    def set_rate(self, rate: float) -> None:
        self.generator.set_rate(rate)

    def spike(self) -> float:
        """Instant jump to the SPIKE_RATE_EPS step function — no ramp. The
        very next emission after this call already uses the new rate."""
        self.set_rate(SPIKE_RATE_EPS)
        return SPIKE_RATE_EPS

    def set_mode(self, mode: QueueMode) -> None:
        """The one call site that changes the live queue's selection policy
        — routed through here (not the queue directly) so metrics.mode
        never drifts from what the queue is actually doing."""
        self.queue.set_mode(mode)
        metrics.set_mode(mode)

    async def reset(self) -> None:
        """Walk the whole pipeline back to a clean baseline, mid-process —
        for /control/reset, so a presenter can restart the demo without
        restarting the server. Deliberately leaves `mode` untouched: it is
        a separate, explicit control, not a statistic a reset should flip.
        metrics.reset() resets mode to adaptive as part of its own "clear
        everything" contract, so the current mode is captured first and
        reapplied right after.

        Restarts the worker pool rather than only clearing the queue. A
        worker can already be mid-serve() on an event from before the
        reset; left alone, it finishes normally and reports that event's
        real (huge, pre-reset) latency into a now-otherwise-empty window,
        where it can dominate that tier's p50/p99 for a very long time at
        low arrival rates — a "clean slate" that silently isn't one.
        Cancelling in-flight work here is safe: worker.py's `finally:
        queue.task_done()` still runs on cancellation, so the queue's
        unfinished-count stays correct either way.

        Stage I: `workers.stop()` cancels every worker cleanly
        (`WorkerPool._stopping` suppresses the death-recovery path — see
        its own docstring), so none of those workers' in-flight checkpoint
        rows get recovered-and-replayed here, on purpose: this reset
        already intends to discard whatever those events were mid-serving
        (the same intent `queue.clear()` already carries out for anything
        still queued), not resurrect pre-reset events into the clean
        post-reset queue. `workers.reset_checkpoint()` clears those
        now-orphaned rows explicitly, the same way `ledger.reset()` clears
        the ledger — leaving them would silently leak rows forever across
        every future reset, and would still be sitting there, tagged to
        worker_ids that now belong to brand-new post-reset tasks, the next
        time any of THOSE workers happened to die for real.
        """
        current_mode = self.queue.mode
        self.generator.set_rate(self.config.baseline_eps)
        # The generator's admission credits are per-Engine, not ambient
        # (see admission.py's own AdmissionControl docstring), so
        # metrics.reset() cannot reach them the way it reaches codel.py/
        # ladder.py's module-level state — reset explicitly, here, same as
        # every other piece of live control-loop state a clean demo
        # restart should not inherit.
        self.generator.admission.reset()
        self.generator.set_payload_multiplier(1.0)
        self.cost_model.reset()
        self.dedup = Deduplicator()
        await self.workers.stop()
        self.workers.reset_checkpoint()
        self.queue.clear()
        metrics.reset()
        metrics.set_mode(current_mode)
        ledger.reset()
        self.workers.start()

    async def inject_event(
        self, event_type: EventType, partition_key: str | None = None
    ) -> Event:
        """Drop one event of `event_type` into the running stream, outside
        the mix draw. Tier/value/cost/deadline still come from config via
        the same classifier every generated event goes through — this
        method accepts no economics of its own to override them with."""
        raw = self.generator.emit_single(event_type, partition_key)
        event = self.classifier.classify(raw)
        await self.queue.put(event)
        return event

    async def chaos_kill_worker(self) -> int | None:
        """`POST /chaos/kill-worker`'s own mechanism: real cancellation of
        one live worker task, not a simulated effect — see
        `WorkerPool.kill_worker`'s own docstring for why it prefers a
        currently-busy worker, and `_on_worker_done` for the recovery and
        respawn that follow automatically, exactly as they would for an
        unplanned death."""
        return self.workers.kill_worker()

    async def chaos_duplicate_flood(self, n: int) -> dict[str, int]:
        """`POST /chaos/duplicate-flood`'s own mechanism: replay up to `n`
        of the most recently sink-committed events (`sink.recent()` — a
        real durable read, not an in-memory guess at "recent"), each as a
        genuinely new physical delivery of the SAME business fact —
        `generator.retry()` mints a fresh `event_id` and `ingest_ts` for
        the same `dedup_key`/`partition_key`, the exact identity-model
        primitive this project has carried since Stage A (docs/DATA_MODEL.md,
        ADR 0003), not a chaos-specific shortcut. `classifier.classify()`
        then deterministically re-derives the SAME `idempotency_key` from
        that `dedup_key` (see classifier.py), so a replayed event is
        indistinguishable, at every field but `event_id` and `seq`, from a
        real duplicate delivery.

        Routed through `self.dedup.check()` — the identical gate
        `_ingest()` itself uses on every real event, not a second, fake
        check that only exists for this endpoint. A correctly-working
        Deduplicator suppresses effectively all of them (each `dedup_key`
        was, by construction, already inside the bounded exact-set window
        from its own original admission); any that a genuine Bloom false
        positive or an aged-out window entry lets through still land in
        the sink safely, unduplicated, via `sink.write()`'s own
        idempotency-key upsert — belt AND suspenders, not one covering for
        the other's absence.
        """
        sources = sink.recent(n)
        admitted = 0
        suppressed = 0
        for original in sources:
            raw = GeneratedEvent(
                event_id=original.event_id,
                dedup_key=original.dedup_key,
                partition_key=original.partition_key,
                type=original.type,
                payload_size=original.payload_size,
                ingest_ts=original.ingest_ts,
            )
            replayed = self.generator.retry(raw)
            event = self.classifier.classify(replayed)
            if self.dedup.check(event.dedup_key):
                metrics.observe_duplicate_caught(event)
                suppressed += 1
            else:
                await self.queue.put(event)
                admitted += 1
        return {
            "requested": n,
            "replayed": len(sources),
            "admitted": admitted,
            "suppressed": suppressed,
        }

    async def start(self) -> None:
        # Phase J7: in dispatch mode nothing is ever put into this
        # process's own queue, so its own local worker pool would just sit
        # idle — skipped, not merely harmless-but-wasteful, so a judge
        # inspecting a running ingress pod sees zero idle worker tasks
        # rather than a confusing six of them doing nothing.
        if not self.dispatch_via_transport:
            self.workers.start()
        self._ingest_task = asyncio.create_task(self._ingest(), name="pulse-ingest")
        self._drain_stop = asyncio.Event()
        self._drain_task = asyncio.create_task(
            deferral.run_drainer(
                replay=self.queue.put_replayed,
                current_pressure=lambda: metrics.current_pressure(self.config),
                stop_event=self._drain_stop,
                # Phase J6: without this, Engine's own local drainer (the
                # unfiltered origin=None default every pre-J6 call site
                # relies on) would happily scoop up 'server2'-origin rows
                # too and replay them into ITS OWN local queue — silently
                # processing a real server2 pod's own deferred work
                # through Engine's own separate decision engine instead of
                # sending it back over the wire, exactly the violation
                # deferral.py's own top docstring warns against. Found
                # while testing the second drainer this phase adds, not
                # assumed.
                origin=deferral.ORIGIN_LOCAL,
            ),
            name="pulse-drainer",
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._ingest_task is not None:
            self._ingest_task.cancel()
            await asyncio.gather(self._ingest_task, return_exceptions=True)
        if self._drain_stop is not None:
            self._drain_stop.set()
        if self._drain_task is not None:
            self._drain_task.cancel()
            await asyncio.gather(self._drain_task, return_exceptions=True)
        if not self.dispatch_via_transport:
            await self.workers.stop()

    async def _ingest(self) -> None:
        """generator -> classifier -> queue, one event at a time.

        No decision-making here. Stage D's first draft decided at admission
        (against pressure measured right then) — moved to worker.py instead,
        because MICRO_BATCH/DEFER are about what a worker actually does with
        an event it is about to serve, and by dequeue time a real backlog
        may have sat long enough that pressure measured at ingest is stale.
        Deciding as late as possible uses the freshest signal, and it is
        also the only point that can actually act on the answer — batch
        execution and deferral both live in worker.py now, not just their
        audit trail.

        Stage I adds exactly one more gate here, before queue.put(): a
        dedup check on `event.dedup_key`. This is deliberately NOT the
        same kind of thing as the routing decisions this docstring's own
        first paragraph says do not belong at ingest — those (batch vs.
        defer) are about WHAT TO DO with an event this pipeline has
        already agreed exists; dedup is about WHETHER this event is a
        second physical delivery of a business fact already admitted
        once. That question does not get better with a fresher signal the
        way pressure-based routing does — it is exactly as answerable at
        ingest as it will ever be — and answering it here, before
        queue.put(), is the entire point: a confirmed duplicate never
        occupies a queue slot or a worker's simulated service time.
        `self.dedup.check()` never suppresses tier-blind: see dedup.py's
        own docstring for why an unconfirmed Bloom hit is always admitted,
        for every tier, P0 included."""
        async for raw in self.generator.events(self._stop):
            event = self.classifier.classify(raw)
            if self.dedup.check(event.dedup_key):
                metrics.observe_duplicate_caught(event)
                continue
            if self.dispatch_via_transport:
                # Phase J7: the real split — hand off to server1/server2
                # over the wire instead of this process's own queue.
                # Deliberately does NOT call metrics.observe_ingest(): that
                # would bump this process's own LOCAL in_queue counter for
                # an event that is never locally dequeued/completed
                # (nothing here ever calls observe_dequeue/observe_complete
                # for it), silently breaking the LOCAL conservation
                # equation. "Ingested" for the cross-process view instead
                # comes from transport.dispatch_stats() — see
                # _dispatch_merged_frame(), the /ws handler's own merge
                # step for this mode.
                try:
                    server = load_servers_config().server_for_tier(event.tier).name
                    await transport.submit(server, event)
                except Exception:  # noqa: BLE001 - one bad dispatch must not
                    # kill the whole ingest loop (matches worker.py's own
                    # "one bad event must not kill the worker" precedent);
                    # a lost submit is recovered the same way a lost ack is
                    # — nothing here, so log loudly instead of pretending.
                    logger.exception("dispatch failed for event %s", event.event_id)
            else:
                await self.queue.put(event)


class RateBody(BaseModel):
    rate: float


class PayloadMultiplierBody(BaseModel):
    multiplier: float


class ModeBody(BaseModel):
    mode: str


class InjectBody(BaseModel):
    type: str
    partition_key: str | None = None


class DuplicateFloodBody(BaseModel):
    count: int = 1000


class WeightsBody(BaseModel):
    """All six fields optional — POST /control/weights is a partial update:
    a dashboard slider only ever reports the one value it moved. See
    decision.set_weights() for why the rest are then renormalised rather
    than left as-is."""

    w1: float | None = None
    w2: float | None = None
    a: float | None = None
    b: float | None = None
    c: float | None = None
    d: float | None = None


def _fake_mode_error(action: str) -> JSONResponse:
    return JSONResponse(
        {"error": f"{action} has no effect in --fake mode"}, status_code=409
    )


class AckBody(BaseModel):
    """`POST /ack`'s own wire shape — a server never knows ingress's own
    internal `dispatch_id` (see transport.py's own docstring on
    `ack_by_event_ids`), only the `event_id`s it actually finished.

    Phase J6: `events`/`decision`/`reason`/`pressure`/`source` are
    optional and additive. Old callers (existing tests, the
    `--transport=direct` loopback deliver, anything that only ever knew
    about transport bookkeeping) send bare `event_ids` and get exactly
    today's behaviour — this body's own defaults make that shape still
    valid. A real server1/server2 completion sends the richer shape too,
    in the SAME request, so ingress can durably record the completion
    (sink + ledger + decision trace + SLA outcome — see
    `_record_completions()`) without a second network round trip (a real
    concern for P0's own 200ms SLA — see transport.py's own docstring on
    the ~60ms queue budget that leaves). `events` is a full `Event`
    payload per completed id, in no particular correspondence to
    `event_ids`' own order — matched by `event_id` field, not position,
    so a partial ack (transport.py's own documented case) cannot
    silently pair the wrong event with the wrong id.
    """

    event_ids: list[str] = []
    events: list[dict] = []
    decision: str | None = None
    reason: str = ""
    pressure: float = 0.0
    source: str = ""


class MetricsReportBody(BaseModel):
    """`POST /metrics/report`'s own wire shape — one server instance's own
    fragment, per reporting.py's `MetricsFragment`."""

    server: str
    instance_id: str
    pushed_ts: float
    counters: dict[str, float] = {}


class DeferBody(BaseModel):
    """`POST /defer`'s own wire shape — Phase J5's server2 has no local
    deferral buffer (docs/PHASE-J-INSPECTION.md section 3: the deferred
    buffer is durable, ingress-owned state); this is how a deferred event
    actually reaches the store that owns it. `event` is the full event
    payload (whatever `Event.model_dump(mode="json")` produces), validated
    against the frozen contract here, at the one place it re-enters a
    process that holds the real `deferral.py` store."""

    event: dict
    reason: str


class ShedBody(BaseModel):
    """`POST /shed`'s own wire shape (Phase J8 live-demo fix) — a SHED
    decision has no durable store of its own anywhere in the split
    topology; this is purely a narration-panel feed (see `/shed`'s own
    handler)."""

    event: dict
    reason: str
    pressure: float = 0.0


class RollupBody(BaseModel):
    """`POST /rollup`'s own wire shape — one finished reservoir window
    (`ladder.Rollup`'s own fields), durably persisted here via
    `sink.write_rollup()`. Phase J5's server2 keeps the OPEN, in-progress
    window local (legitimate per-instance state — see `ladder.py`'s own
    docstring); only a finished window ever crosses this wire."""

    event_type: str
    window_start: float
    window_end: float
    sample_weight: float
    observed_count: int
    subtype_counts: dict[str, int]
    seq_low: int
    seq_high: int


def _dispatch_merged_frame(now: float | None = None) -> MetricsFrame:
    """Phase J7: the WS frame for dispatch mode — ingress's own local
    counters stay at 0 (nothing is ever locally queued/served in this
    mode — see Engine._ingest()'s own docstring), so the numbers a judge
    actually wants (processed, in_queue, in_flight, sampled_out, shed)
    have to come from server1's/server2's own pushed fragments instead.
    `ingested` comes from transport.dispatch_stats() — the one
    synchronous, non-stale cross-process signal ingress has (see
    Transport.dispatch_stats()'s own docstring) — rather than a local
    counter this mode never increments.

    `pressure` (the single legacy gauge every existing dashboard panel
    already reads) reports server2's own — the tier PULSE's whole
    decision engine actually triages under load; server1's own separate
    number is exposed through GET /control/topology for the two-gauge
    panel this phase's own prompt asks for, not overloaded onto this one
    frozen field.

    Deliberately does NOT go through `metrics.snapshot()`: that call has
    the side effect of running `_check_conservation()` against ingress's
    own PURELY LOCAL counters (all 0 in this mode — nothing is ever
    locally queued/served) versus `deferral.pending_count()`, which is
    NOT purely local (it counts BOTH origins). The instant any
    'server2'-origin row is deferred, that check would see `0 !=
    (nonzero) deferred_pending` and record a permanent
    "CRITICAL INVARIANT VIOLATION" — a real, tested false alarm found
    while smoke-testing `make dev-split` (the dashboard's own Conservation
    panel showed BROKEN, not clearing on reset), not a real invariant
    break; the monolith's own conservation identity was never designed to
    hold against a cross-process aggregate in the first place (`docs/
    PHASE-J-INSPECTION.md` section 4's own "reporting lag" finding — see
    GET /control/conservation for the honest, separately-reported
    cross-process view instead). Building the frame fresh, field by
    field, sidesteps that check entirely rather than triggering it and
    then papering over the result.
    """
    now = time.time() if now is None else now
    frame = MetricsFrame(ts=now, mode=metrics.get_mode())
    stats = transport.dispatch_stats()
    s1 = reporting.aggregate("server1")
    s2 = reporting.aggregate("server2")
    frame.ingested = stats["dispatched"]
    frame.processed = int(s1.get("processed", 0) + s2.get("processed", 0))
    frame.in_queue = int(s1.get("in_queue", 0) + s2.get("in_queue", 0))
    frame.in_flight = int(s1.get("in_flight", 0) + s2.get("in_flight", 0))
    frame.sampled_out = int(s2.get("sampled_out", 0))
    frame.shed = int(s2.get("shed", 0))
    frame.deferred_pending = deferral.pending_count()
    frame.pressure = round(s2.get("pressure", 0.0), 4)

    # Offered/admitted: generator.py calls metrics.observe_admission() on
    # every emission attempt regardless of dispatch mode (admission is an
    # ingress-side gate that runs BEFORE the dispatch-vs-local-queue
    # branch) — these two EWMAs are therefore already real and live in
    # this mode; reading metrics.py's own module-level instances directly
    # (not going through metrics.snapshot(), which this function's own
    # docstring already explains avoiding) is the same "reuse the private
    # object directly" pattern server2.py already uses for metrics._Ewma.
    frame.offered_rate = round(metrics._offered_rate_ewma.with_trend, 3)
    frame.admitted_rate = round(metrics._admitted_rate_ewma.with_trend, 3)

    # service_rate: no local metrics.observe_complete() ever runs in this
    # mode (nothing is served locally), so metrics.py's own service EWMA
    # stays at 0 here — approximated instead by feeding a dedicated EWMA
    # (metrics._Ewma, the same class metrics.py's own rates already use)
    # the count newly processed since the last frame. Using _Ewma rather
    # than a bare delta/dt division is deliberate, not just consistent
    # style: /ws can have more than one connected client (or a browser tab
    # reconnecting), each independently calling this function on its own
    # schedule against this SAME module-level state — a raw division would
    # see near-zero dt whenever two callers interleave and momentarily
    # report a bogus near-zero or wildly spiky rate; _Ewma.observe_amount()
    # already carries a sub-zero-dt observation forward instead of losing
    # or misreporting it (see that method's own docstring).
    prev_processed = _dispatch_rate_state["processed"]
    _dispatch_service_rate_ewma.observe_amount(max(0.0, frame.processed - prev_processed), now)
    frame.service_rate = round(_dispatch_service_rate_ewma.with_trend, 3)
    _dispatch_rate_state["processed"] = frame.processed

    # Per-tier p99 latency and queue depth: server1 only ever holds P0,
    # so its own numbers map there exactly; server2 pools P1+P2 into one
    # number each (its own `/metrics` doesn't split latency by tier, and
    # its own live queue is dominated by whichever tier pressure is
    # currently biting hardest) — reported under both P1 and P2 rather
    # than invented as two separate numbers neither server actually
    # computes.
    frame.latency_p99 = {
        "P0": round(s1.get("latency_p99", 0.0), 3),
        "P1": round(s2.get("latency_p99", 0.0), 3),
        "P2": round(s2.get("latency_p99", 0.0), 3),
    }
    frame.queue_depth = {
        "P0": int(s1.get("in_queue", 0)),
        "P1": 0,
        "P2": int(s2.get("in_queue", 0)),
    }

    # worker_count/active_workers: neither server pushes its own worker
    # count in its metrics fragment (it's a static, config-derived number,
    # not a live counter worth the wire cost every 250ms) — server1's own
    # is fixed (never scales, Phase J4); server2's is per-pod, multiplied
    # by however many live instances are actually reporting right now
    # (`reporting.instance_count`, real under HPA). `in_flight` (already
    # computed above) is the real cross-process analogue of "workers
    # currently busy".
    servers_cfg = load_servers_config()
    ref_rate = load_config().worker_capacity_ups
    server1_workers, _ = servers_cfg.server1.workers(reference_worker_rate_ups=ref_rate)
    server2_workers_per_pod, _ = servers_cfg.server2.workers(reference_worker_rate_ups=ref_rate)
    server2_instances = max(1, reporting.instance_count("server2"))
    frame.worker_count = server1_workers + server2_workers_per_pod * server2_instances
    # Phase J8 (live-demo fix, chaos wiring): each server's own /metrics
    # now reports `active_worker_count` — how many of ITS OWN worker tasks
    # are actually alive right now, real-cancellation-aware (see server1.py/
    # server2.py's own `_kill_one_worker`/`_on_worker_done` docstrings).
    # Summing the two is the real cross-process analogue of "how many
    # worker cells should be lit right now", correctly dipping by exactly
    # one for the brief window between a real POST /chaos/kill-worker and
    # that worker's own automatic respawn — `frame.in_flight` (busy-ness)
    # answers a different question and was never the right source for
    # this, even though the numbers often coincide at rest.
    frame.active_workers = int(
        s1.get("active_worker_count", server1_workers)
        + s2.get("active_worker_count", server2_workers_per_pod * server2_instances)
    )
    frame.recent_sheds = list(_recent_dispatch_sheds)

    return frame


# Phase J8 (live-demo fix): mutable rate-tracking state for
# _dispatch_merged_frame()'s own service_rate approximation — module-level
# because /ws's polling loop (possibly more than one connected client)
# calls this function repeatedly and needs the PREVIOUS frame's own
# processed count to compute a delta against. Not per-Engine state: this
# mode has exactly one Engine per process, matching every other ambient
# module in this codebase's own "one pipeline, one process" reasoning.
_dispatch_rate_state: dict[str, float] = {"processed": 0.0}
_dispatch_service_rate_ewma = metrics._Ewma(metrics._RATE_EWMA_HALF_LIFE_SECONDS)

# Phase J8 (live-demo fix): the split topology's own narration feed for
# the Shed Log panel — SHED has no durable record anywhere else in this
# mode (server2's own `/shed` POST is the only place an individual shed
# event is ever named), so this small ring buffer (matching metrics.py's
# own `_recent_sheds` bound for the monolith) is where `recent_sheds`
# below actually comes from.
_recent_dispatch_sheds: deque = deque(maxlen=50)


_VALID_COMPLETION_SOURCES = frozenset({"ingress", "server1", "server2"})


def _record_completions(
    events: list[Event], *, decision: str | None, reason: str, pressure: float, source: str,
) -> None:
    """Phase J6's own durable-write path, triggered by `/ack`'s richer
    optional shape (`AckBody`'s own docstring): sink (the processed
    event), ledger (an audit row + decision trace), and a durable
    `sla_outcomes` row, per completed event. This mirrors, for a REMOTE
    completion arriving over the wire, exactly what `worker.py`'s own
    `serve()`/`_serve_batch()` already do locally for Engine's own
    pipeline (`metrics.observe_complete` + `sink.write`;
    `metrics.observe_decision`, which itself calls
    `ledger.record`/`record_trace`) — the same durability contract,
    reached by a different path.

    Never raises: an unparseable `decision` string, like a failed audit
    write (`ledger.record`'s own guarantee), must not fail an otherwise-
    successful ack — the caller (`/ack`'s own handler) has already
    validated the events themselves before this is called.
    """
    now = time.time()
    try:
        decision_enum = Decision(decision) if decision else Decision.STREAM_NOW
    except ValueError:
        decision_enum = Decision.STREAM_NOW
    resolved_source = source if source in _VALID_COMPLETION_SOURCES else "ingress"

    for event in events:
        sink.write(event)
        ledger.record(
            seq=event.seq, decision=decision_enum, reason=reason, pressure=pressure, tier=event.tier,
            now=now,
        )
        ledger.record_trace(
            DecisionTrace(
                seq=event.seq, event_id=event.event_id, type=event.type, tier=event.tier,
                decision=decision_enum, reason=reason, pressure=pressure, value=event.value, ts=now,
            ),
            now=now,
        )
        met = (now <= event.deadline_ts) if event.deadline_ts else True
        latency_ms = max(0.0, (now - event.ingest_ts) * 1000.0)
        sink.write_outcome(event, met=met, latency_ms=latency_ms, source=resolved_source, now=now)


def _make_direct_deliver() -> transport.DeliverFn:
    """The `--transport=direct` demo fallback's own `deliver`: no HTTP, and
    no separate process that could die independently of ingress — CLAUDE.md
    hard rule 1's single failure domain already applies in this mode, so a
    "delivered" batch is unconditionally acked back immediately rather than
    waiting on a real downstream completion that, in this mode, is not a
    separate thing to wait for.

    This governs transport.py's own ambient state for whatever calls
    `transport.dispatch()`/`submit()` (today: tests, and an operator
    driving it directly) — it does NOT reroute `Engine._ingest()`'s own
    pipeline, which keeps processing every event locally regardless of
    `--transport`, exactly as it did before this phase. See this module's
    own top docstring for why that wiring is named as separate scope
    rather than done here.
    """

    async def _deliver(server: str, events: list[Event]) -> None:
        await transport.ack_by_event_ids([e.event_id for e in events])

    return _deliver


def _server2_pressure_safe_to_drain() -> float:
    """Phase J6's own redispatch gate for `origin='server2'` deferred
    rows: the MAX pressure among server2's own LIVE reported fragments —
    never the average (Phase J5's own rule: ingress reports every
    instance's pressure separately and never averages them), and
    conservative on purpose: a Kubernetes Service could route the very
    next dispatch to ANY live instance, so resuming the moment even one
    happens to read calm, while others are still saturated, would just
    relocate the overload rather than actually relieve it.

    No live fragment at all (server2 has not pushed one yet, or every one
    has aged out — `reporting.py`'s own TTL) returns 1.0, the maximally
    unsafe reading, on purpose: an unknown pressure must never be treated
    as "safe to send more work into," the same fail-closed reasoning
    `ladder.cap()` already applies to a routing decision it cannot
    verify.
    """
    fragments = reporting.fragments("server2")
    if not fragments:
        return 1.0
    return max(f.counters.get("pressure", 1.0) for f in fragments)


def _redispatch_to_server2(event: Event) -> None:
    """The `replay` callable Phase J6's own second drainer passes to
    `deferral.run_drainer(..., origin=deferral.ORIGIN_SERVER2)` — fires a
    background task rather than awaiting inline, because
    `DeferralStore.run_drainer()`'s own loop calls `replay(event)`
    synchronously (queue.put_replayed, its original, Stage D-era
    contract, is synchronous too) and changing that shared contract for
    one new caller is a bigger change than this phase asks for. The task
    itself does the real work: `transport.submit()` re-enters the exact
    same batching/dispatch/ack machinery a fresh arrival would, so a
    redispatched, once-deferred event is indistinguishable, on the wire,
    from a brand-new one — idempotency (docs/DATA_MODEL.md's own identity
    model) is what makes that safe rather than a double-charge, the same
    guarantee transport.py's own `redispatch_expired()` already rests on.
    """
    asyncio.create_task(
        transport.submit("server2", event), name=f"pulse-redispatch-server2-{event.event_id}"
    )


def create_app(
    *, fake: bool = False, seed: int | None = None, transport_mode: str = "direct",
    persist: bool = False,
) -> FastAPI:
    """Build one FastAPI app in either mode. Kept as a factory (rather than a
    single module-level app) so tests can construct independent instances.

    `transport_mode` ("direct" or "http") governs transport.py's own
    ambient configuration for this process acting as ingress — see this
    module's own top docstring and `_make_direct_deliver`'s own docstring
    for exactly what it does and does not affect.

    `persist` (Phase J6, default off) opens one real, WAL-mode
    `history.db` file (`config/servers.yaml`'s own `ingress.history_db`
    path) and points sink.py/ledger.py/deferral.py's own ambient defaults
    at it, instead of each module's own separate `:memory:` default —
    see `history_db.py`'s own docstring for why this is opt-in rather
    than automatic. Also starts the second, `origin='server2'` deferral
    drainer, which only means anything once real traffic is actually
    dispatched to a real server2 (today: direct test traffic against
    server2's own `/ingest`, or a future prompt's own real dispatch
    wiring — see this module's own top docstring)."""

    if transport_mode not in ("direct", "http"):
        raise ValueError(f"unknown transport_mode: {transport_mode!r}")

    fake_source = FakeSource(seed=seed) if fake else None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        history_connection = None
        server2_drain_stop: asyncio.Event | None = None
        server2_drain_task: asyncio.Task[None] | None = None
        if not fake:
            if persist:
                # DATABASE_URL (.env or the real environment — see
                # pg_compat.database_url()'s own precedence) wins when set:
                # the history database lives in Supabase for this run.
                # Otherwise fall back to config/servers.yaml's own local
                # SQLite path. This decision belongs HERE, not inside
                # history_db.open_history_db() itself — that function
                # must always open exactly what it is told, never
                # override an explicit caller argument based on ambient
                # environment (see its own docstring).
                history_target = pg_compat.database_url() or load_servers_config().ingress.history_db
                history_connection = history_db.open_history_db(history_target)
                history_db.wire_ambient_stores(history_connection)
            if transport_mode == "http":
                transport.configure_http()
                await transport.start_http()
            else:
                transport.configure(_make_direct_deliver())
            server2_drain_stop = asyncio.Event()
            server2_drain_task = asyncio.create_task(
                deferral.run_drainer(
                    replay=_redispatch_to_server2,
                    current_pressure=_server2_pressure_safe_to_drain,
                    stop_event=server2_drain_stop,
                    origin=deferral.ORIGIN_SERVER2,
                ),
                name="pulse-drainer-server2-origin",
            )
        if fake:
            yield
            return
        engine = Engine(seed=seed, dispatch_via_transport=(transport_mode == "http"))
        app.state.engine = engine
        await engine.start()
        try:
            yield
        finally:
            await engine.stop()
            if server2_drain_stop is not None:
                server2_drain_stop.set()
            if server2_drain_task is not None:
                server2_drain_task.cancel()
                await asyncio.gather(server2_drain_task, return_exceptions=True)
            if transport_mode == "http":
                await transport.stop_http()
            if history_connection is not None:
                history_connection.close()

    app = FastAPI(title="PULSE", lifespan=lifespan)
    app.state.mode = "fake" if fake else "real"
    app.state.transport_mode = transport_mode

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {
            "status": "ok",
            "mode": app.state.mode,
            "transport": app.state.transport_mode,
            "uptime_s": None if fake else round(metrics.uptime_seconds(), 1),
        }

    @app.post("/ack")
    async def ack_endpoint(body: AckBody) -> JSONResponse:
        """A server's own completion signal — see transport.py's own
        docstring, and AckBody's own docstring for the Phase J6 additions.
        Not fake-mode-gated: it only ever touches transport.py's ambient
        state (plus, now, sink/ledger/deferral's — all three already
        ambient regardless of mode), which is harmless (a no-op on
        unknown event_ids, an ordinary durable write otherwise) to call
        regardless of engine mode."""
        event_ids = list(body.event_ids)
        if body.events:
            try:
                events = [Event.model_validate(e) for e in body.events]
            except Exception as exc:  # noqa: BLE001 - a malformed payload is a
                # caller bug, not a pipeline fault; report it, don't 500.
                return JSONResponse({"error": f"invalid event: {exc}"}, status_code=422)
            if not event_ids:
                event_ids = [e.event_id for e in events]
            _record_completions(
                events, decision=body.decision, reason=body.reason,
                pressure=body.pressure, source=body.source,
            )
        await transport.ack_by_event_ids(event_ids)
        return JSONResponse({"status": "ok"})

    @app.post("/metrics/report")
    async def metrics_report_endpoint(body: MetricsReportBody) -> JSONResponse:
        """One server instance's own fragment — see reporting.py's own
        docstring on why this is push, not poll. Not fake-mode-gated for
        the same reason /ack is not."""
        reporting.push(
            reporting.MetricsFragment(
                server=body.server,
                instance_id=body.instance_id,
                pushed_ts=body.pushed_ts,
                counters=body.counters,
            )
        )
        return JSONResponse({"status": "ok"})

    @app.post("/defer")
    async def defer_endpoint(body: DeferBody) -> JSONResponse:
        """Phase J5's server2 POSTs a DEFER decision here instead of
        buffering it locally — see DeferBody's own docstring. Not
        fake-mode-gated: it only touches `deferral.py`'s own already-
        ambient store, the same store `/control/reset` already knows how
        to leave alone/clear regardless of which mode produced the row.

        Phase J6: tagged `origin='server2'` (`deferral.py`'s own
        docstring on why a 'server2'-origin row must redispatch back over
        the wire, never into Engine's local queue), and — since a
        successfully durable DEFER is a RESOLVED dispatch attempt, not an
        outstanding one — this also clears transport's own bookkeeping
        for it. Without this, `redispatch_expired()` would find this
        event still "outstanding" once `ack_timeout_ms` passed and
        redispatch it to server2 a second time even though it was
        already, correctly, durably deferred — a real, tested bug found
        while wiring this phase, not a hypothetical.
        """
        try:
            event = Event.model_validate(body.event)
        except Exception as exc:  # noqa: BLE001 - a malformed payload is a
            # caller bug, not a pipeline fault; report it, don't 500.
            return JSONResponse({"error": f"invalid event: {exc}"}, status_code=422)
        try:
            deferral.defer(event, body.reason, origin=deferral.ORIGIN_SERVER2)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
        await transport.ack_by_event_ids([event.event_id])
        return JSONResponse({"status": "ok"})

    @app.post("/rollup")
    async def rollup_endpoint(body: RollupBody) -> JSONResponse:
        """Phase J5's server2 POSTs a finished reservoir window here — see
        RollupBody's own docstring. Not fake-mode-gated, same reasoning as
        /defer above."""
        rollup = LadderRollup(
            event_type=body.event_type,
            window_start=body.window_start,
            window_end=body.window_end,
            sample_weight=body.sample_weight,
            observed_count=body.observed_count,
            subtype_counts=body.subtype_counts,
            seq_low=body.seq_low,
            seq_high=body.seq_high,
        )
        rollup_id = sink.write_rollup(rollup)
        return JSONResponse({"status": "ok", "rollup_id": rollup_id})

    @app.post("/shed")
    async def shed_endpoint(body: ShedBody) -> JSONResponse:
        """Phase J8 (live-demo fix): server2 POSTs a SHED decision here —
        see ShedBody's own docstring. Kept in a small, bounded, in-memory
        ring buffer (not durable — SHED already has no durable record
        anywhere else in the split topology; this is a narration-panel
        convenience, matching `metrics.py`'s own `_recent_sheds` deque
        for the monolith, not a new audit trail)."""
        try:
            event = Event.model_validate(body.event)
        except Exception as exc:  # noqa: BLE001 - a malformed payload is a
            return JSONResponse({"error": f"invalid event: {exc}"}, status_code=422)
        _recent_dispatch_sheds.appendleft(
            ShedRecord(
                seq=event.seq, event_id=event.event_id, type=event.type, tier=event.tier,
                reason=body.reason, pressure=body.pressure, value=event.value, ts=time.time(),
            )
        )
        return JSONResponse({"status": "ok"})

    @app.get("/control/conservation")
    async def get_conservation() -> JSONResponse:
        """Phase J6's own cross-process conservation view: "counters live
        in three processes; each pushes its own in its metrics fragment,
        ingress sums across all live fragments plus its own
        outstanding-dispatch table" (this phase's own instruction,
        verbatim) — a new, small, dedicated endpoint, matching
        `/control/transport-latency`'s own precedent for a real number
        `contracts.py`'s frozen `MetricsFrame` was never going to carry.

        `dispatch` is transport.py's own event-id-SET-based identity
        (`dispatched == resolved + outstanding`, robust to redispatch —
        see `Transport.dispatch_stats()`'s own docstring), which is the
        one exact, non-stale signal ingress has for cross-process
        traffic; `server1`/`server2` are each server's own summed live
        fragment counters (`reporting.aggregate()`), included for
        visibility, not folded into the `dispatch` identity itself
        (`docs/PHASE-J-INSPECTION.md` section 4's own "aggregating
        network-reported counters can leave the equation transiently
        wrong purely from reporting lag" finding — this endpoint reports
        both signals honestly rather than pretending a single perfect
        number exists). `shed_critical` sums `shed_critical` across every
        live server2 fragment — architecturally always 0 (`ladder.cap()`
        already forbids a P1 event from ever reaching SHED), reported as
        a live, continuously-checked invariant rather than merely assumed
        from code inspection, the same spirit as
        `metrics._check_p0_never_non_stream`'s own live check.
        """
        server1_counters = reporting.aggregate("server1")
        server2_counters = reporting.aggregate("server2")
        return JSONResponse(
            {
                "dispatch": transport.dispatch_stats(),
                "server1": server1_counters,
                "server2": server2_counters,
                "deferred_pending": deferral.pending_count(),
                "deferred_pending_server2_origin": deferral.pending_count_by_origin(
                    deferral.ORIGIN_SERVER2
                ),
                "shed_critical": server2_counters.get("shed_critical", 0.0),
            }
        )

    @app.get("/control/topology")
    async def get_topology() -> JSONResponse:
        """Phase J7's own dashboard data source: two separate pressure
        gauges (server1/server2 — never averaged, per this phase's own
        instruction), transport latency, a topology strip's worth of
        component health, server2's own live instance count (derived from
        live metrics fragments — meaningful under HPA, per this phase's
        own instruction), and the outstanding-dispatch/redispatch
        counters. A new, small, dedicated endpoint, matching
        `/control/conservation`'s own J6 precedent."""
        dispatch = transport.dispatch_stats()
        return JSONResponse(
            {
                "mode": "split" if app.state.transport_mode == "http" else "monolith",
                "server1": {
                    "pressure": round(_server1_pressure(), 4) if reporting.fragments("server1") else None,
                    "instance_count": reporting.instance_count("server1"),
                },
                "server2": {
                    "pressure": round(_server2_pressure(), 4) if reporting.fragments("server2") else None,
                    "instance_count": reporting.instance_count("server2"),
                },
                "transport_latency_ms": transport.latency_percentiles(),
                "outstanding_dispatch": dispatch["outstanding"],
                "redispatch_count": dispatch["redispatched"],
            }
        )

    @app.get("/control/transport-latency")
    async def get_transport_latency() -> JSONResponse:
        """Dispatch-to-ack latency, milliseconds — separate from
        metrics.py's own queue-wait number, per this phase's own
        instruction (a payment's 200ms SLA leaves only ~60ms of queue
        budget once transport and simulated service time are subtracted).
        A new, small, dedicated endpoint rather than a MetricsFrame field
        — contracts.py is frozen, and GET /control/costmodel already
        established this exact precedent (Stage I)."""
        return JSONResponse(transport.latency_percentiles())

    @app.post("/control/rate")
    async def control_rate(body: RateBody) -> JSONResponse:
        if fake:
            return _fake_mode_error("rate control")
        if body.rate < 0:
            return JSONResponse({"error": "rate must be non-negative"}, status_code=422)
        app.state.engine.set_rate(body.rate)
        return JSONResponse({"rate": body.rate})

    @app.post("/control/payload-multiplier")
    async def control_payload_multiplier(body: PayloadMultiplierBody) -> JSONResponse:
        """Stage I's own demo beat: scale every subsequent draw's payload
        size by `multiplier` (1.0 = the documented, calibration-preserving
        default). A sustained shift, not a one-off outlier — the point is
        watching costmodel.py's own estimate re-adapt to it, live."""
        if fake:
            return _fake_mode_error("payload multiplier control")
        try:
            app.state.engine.generator.set_payload_multiplier(body.multiplier)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
        return JSONResponse({"multiplier": body.multiplier})

    @app.get("/control/costmodel")
    async def get_costmodel() -> JSONResponse:
        """Learned vs. prior cost per type — costmodel.py's own
        `CostModel.summary()`. No fake-mode restriction (a read, like GET
        /control/weights) — in --fake mode there is no engine, so this
        simply 404s rather than 409ing like a fake-mode-guarded write
        would, since there is genuinely nothing to read, not an action
        being refused."""
        if fake:
            return JSONResponse({"error": "no cost model in --fake mode"}, status_code=404)
        return JSONResponse(
            [dataclasses.asdict(row) for row in app.state.engine.cost_model.summary()]
        )

    @app.post("/control/spike")
    async def control_spike() -> JSONResponse:
        """Instant jump to the SPIKE_RATE_EPS step function. No ramp — the
        spike is a step function by spec, not a gradual climb."""
        if fake:
            return _fake_mode_error("spike control")
        rate = app.state.engine.spike()
        return JSONResponse({"rate": rate, "events_per_minute": SPIKE_EVENTS_PER_MINUTE})

    @app.post("/control/mode")
    async def control_mode(body: ModeBody) -> JSONResponse:
        if fake:
            return _fake_mode_error("mode control")
        try:
            app.state.engine.set_mode(body.mode)  # type: ignore[arg-type]
        except ValueError:
            return JSONResponse(
                {"error": f"unknown mode: {body.mode!r}; use 'naive' or 'adaptive'"},
                status_code=422,
            )
        return JSONResponse({"mode": body.mode})

    @app.post("/control/reset")
    async def control_reset() -> JSONResponse:
        """Walk the pipeline back to a clean baseline without restarting
        the process — mode is left exactly as it was."""
        if fake:
            return _fake_mode_error("reset")
        await app.state.engine.reset()
        return JSONResponse({"status": "reset", "rate": app.state.engine.config.baseline_eps})

    @app.post("/control/inject")
    async def control_inject(body: InjectBody) -> JSONResponse:
        """Drop one event into the running stream. Only `type` and an
        optional `partition_key` are accepted — tier/value/cost/deadline
        always come from config via the classifier, never from the caller."""
        if fake:
            return _fake_mode_error("event injection")
        try:
            event_type = EventType(body.type)
        except ValueError:
            valid = ", ".join(t.value for t in EventType)
            return JSONResponse(
                {"error": f"unknown type: {body.type!r}; use one of: {valid}"},
                status_code=422,
            )
        event = await app.state.engine.inject_event(event_type, body.partition_key)
        return JSONResponse(
            {
                "event_id": event.event_id,
                "type": event.type.value,
                "tier": event.tier.value,
                "value": event.value,
                "cost": event.cost,
                "deadline_ts": event.deadline_ts,
            }
        )

    @app.post("/chaos/kill-worker")
    async def chaos_kill_worker() -> JSONResponse:
        """Cancel one live worker task for real. `worker_id: null` means
        the pool had no live worker to kill (called before start or after
        stop) rather than an error — killing nothing is a valid, if
        uninteresting, outcome.

        Split mode (`transport_mode == "http"`): real traffic is served by
        server1/server2, not by this process's own local Engine — the
        local pool sits idle in this mode (nothing is ever queued/served
        here), so killing a worker in it was a real bug, not a smaller
        version of the intended effect: the dashboard's worker-pool grid
        (fed by server1+server2's own real counts) would never show
        anything happen at all. Forwarded instead to server2's own real
        `/chaos/kill-worker` — server2 is the tier this project's whole
        pressure/ladder/CoDel story is actually about, and the one the
        dashboard's own Chaos tab is built to make interesting to watch
        recover."""
        if fake:
            return _fake_mode_error("chaos: kill-worker")
        if transport_mode == "http":
            import httpx

            cfg = load_servers_config()
            url = f"http://127.0.0.1:{cfg.server2.port}/chaos/kill-worker"
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    response = await client.post(url)
                response.raise_for_status()
                return JSONResponse(response.json())
            except Exception as exc:  # noqa: BLE001 - server2 unreachable is a
                # real, reportable outcome for a chaos button, not a 500
                # that looks like ingress itself is broken.
                return JSONResponse(
                    {"worker_id": None, "error": f"server2 unreachable: {exc}"},
                    status_code=502,
                )
        worker_id = await app.state.engine.chaos_kill_worker()
        return JSONResponse({"worker_id": worker_id})

    @app.post("/chaos/duplicate-flood")
    async def chaos_duplicate_flood(body: DuplicateFloodBody) -> JSONResponse:
        """Replay up to `count` of the most recently sink-committed events
        as genuine new duplicate deliveries (same dedup_key/idempotency_key,
        new event_id) — see Engine.chaos_duplicate_flood's own docstring."""
        if fake:
            return _fake_mode_error("chaos: duplicate-flood")
        if body.count <= 0:
            return JSONResponse({"error": "count must be positive"}, status_code=422)
        result = await app.state.engine.chaos_duplicate_flood(body.count)
        return JSONResponse(result)

    @app.get("/control/weights")
    async def get_control_weights() -> JSONResponse:
        """Read the six live decision weights — GET has no fake-mode
        restriction, unlike every POST /control/* endpoint: reading the
        current weights is harmless in either mode, and the dashboard's
        sliders need an initial value to render before the first drag."""
        return JSONResponse(decision.get_weights())

    @app.post("/control/weights")
    async def control_weights(body: WeightsBody) -> JSONResponse:
        """Live-tune score()'s w1/w2 and pressure()'s a/b/c/d. Any subset of
        the six may be sent; decision.set_weights() renormalises each group
        (w1+w2, a+b+c+d) back to summing to 1.0, so one slider can move on
        its own without the caller doing that arithmetic."""
        if fake:
            return _fake_mode_error("weight control")
        try:
            result = decision.set_weights(**body.model_dump())
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
        return JSONResponse(result)

    @app.get("/audit.csv")
    async def audit_csv() -> Response:
        """The whole durable, hash-chained audit_ledger table, exported as
        CSV. No fake-mode restriction (a read, like GET /control/weights)
        — in --fake mode the ledger is simply empty, since nothing in
        that mode ever calls metrics.observe_decision()."""
        return Response(
            content=ledger.export_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=audit_ledger.csv"},
        )

    @app.get("/audit/trace/{event_id}")
    async def audit_trace(event_id: str) -> JSONResponse:
        """One decision trace by event_id, from the 500-item ring buffer —
        the query surface this stage's own spec asks for. 404, not an
        empty 200, when the id is unknown or has aged out of the buffer:
        the two cases are indistinguishable from here, and either way
        there is nothing to return."""
        trace = ledger.get_trace(event_id)
        if trace is None:
            return JSONResponse(
                {"error": f"no decision trace for event_id {event_id!r}"}, status_code=404
            )
        return JSONResponse(trace.model_dump())

    @app.websocket("/ws")
    async def ws_metrics(websocket: WebSocket) -> None:
        """Push one MetricsFrame at SNAPSHOT_HZ until the client disconnects."""
        await websocket.accept()
        try:
            while True:
                if fake:
                    frame: MetricsFrame = fake_source.tick()
                elif app.state.engine.dispatch_via_transport:
                    frame = _dispatch_merged_frame()
                else:
                    frame = metrics.snapshot()
                await websocket.send_text(frame.model_dump_json())
                await asyncio.sleep(SNAPSHOT_PERIOD)
        except WebSocketDisconnect:
            logger.info("dashboard disconnected")

    if DASHBOARD_DIST.is_dir():
        app.mount("/", StaticFiles(directory=str(DASHBOARD_DIST), html=True), name="dashboard")
    else:
        # Stage B: dashboard/dist does not exist yet. Root must not 500 —
        # every stage ends runnable, per CLAUDE.md hard rule 4.
        @app.get("/")
        async def no_dashboard() -> dict[str, str]:
            return {"info": "dashboard/dist not built yet; try /health or /ws"}

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the PULSE FastAPI app.")
    parser.add_argument("--fake", action="store_true",
                        help="serve triage.fake_metrics instead of the real engine")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--transport", choices=["direct", "http"], default="direct",
        help=(
            "direct (default): same-process loopback, the demo fallback — "
            "make dev's own unchanged behaviour. http: real pooled HTTP "
            "dispatch/ack/redispatch against separately-run server1/server2 "
            "processes (triage.server_app) — see app.py's own top docstring "
            "for exactly what this does and does not wire up yet."
        ),
    )
    parser.add_argument(
        "--persist", action="store_true",
        help=(
            "Phase J6: open config/servers.yaml's own ingress.history_db as "
            "a real, WAL-mode SQLite file and point sink/ledger/deferral's "
            "ambient defaults at it, instead of each module's own separate "
            "in-memory default. Off by default — see history_db.py's own "
            "docstring for why this is opt-in."
        ),
    )
    args = parser.parse_args(argv)

    import uvicorn

    uvicorn.run(
        create_app(
            fake=args.fake, seed=args.seed, transport_mode=args.transport, persist=args.persist,
        ),
        host=args.host, port=args.port,
    )


if __name__ == "__main__":
    main()
