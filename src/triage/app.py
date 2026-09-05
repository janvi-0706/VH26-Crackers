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
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import decision, deferral, ledger, metrics, reporting, sink, transport
from .classifier import Classifier
from .config import Config, load_config
from .contracts import Event, EventType, MetricsFrame
from .costmodel import CostModel
from .dedup import Deduplicator
from .fake_metrics import FakeSource
from .generator import EventGenerator, GeneratedEvent
from .ladder import Rollup as LadderRollup
from .queue import EventQueue, Mode as QueueMode
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


class Engine:
    """The real generator -> classifier -> queue -> workers pipeline.

    Everything here runs as background asyncio tasks inside the app's own
    event loop — there is no separate process or thread, per CLAUDE.md hard
    rule 1 (single Python process, asyncio, in-memory).
    """

    def __init__(self, *, config: Config | None = None, seed: int | None = None) -> None:
        self.config = config or load_config()
        self.generator = EventGenerator(config=self.config, seed=seed)
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
        self.workers.start()
        self._ingest_task = asyncio.create_task(self._ingest(), name="pulse-ingest")
        self._drain_stop = asyncio.Event()
        self._drain_task = asyncio.create_task(
            deferral.run_drainer(
                replay=self.queue.put_replayed,
                current_pressure=lambda: metrics.current_pressure(self.config),
                stop_event=self._drain_stop,
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
    `ack_by_event_ids`), only the `event_id`s it actually finished."""

    event_ids: list[str]


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


def create_app(
    *, fake: bool = False, seed: int | None = None, transport_mode: str = "direct"
) -> FastAPI:
    """Build one FastAPI app in either mode. Kept as a factory (rather than a
    single module-level app) so tests can construct independent instances.

    `transport_mode` ("direct" or "http") governs transport.py's own
    ambient configuration for this process acting as ingress — see this
    module's own top docstring and `_make_direct_deliver`'s own docstring
    for exactly what it does and does not affect."""

    if transport_mode not in ("direct", "http"):
        raise ValueError(f"unknown transport_mode: {transport_mode!r}")

    fake_source = FakeSource(seed=seed) if fake else None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if not fake:
            if transport_mode == "http":
                transport.configure_http()
                await transport.start_http()
            else:
                transport.configure(_make_direct_deliver())
        if fake:
            yield
            return
        engine = Engine(seed=seed)
        app.state.engine = engine
        await engine.start()
        try:
            yield
        finally:
            await engine.stop()
            if transport_mode == "http":
                await transport.stop_http()

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
        docstring. Not fake-mode-gated: it only ever touches transport.py's
        ambient state, which is harmless (a no-op on unknown event_ids) to
        call regardless of engine mode."""
        await transport.ack_by_event_ids(body.event_ids)
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
        Re-dispatching a deferred event once server2's own reported
        pressure drops is real, separate scope this endpoint does not
        implement — see server2.py's own top docstring."""
        try:
            event = Event.model_validate(body.event)
        except Exception as exc:  # noqa: BLE001 - a malformed payload is a
            # caller bug, not a pipeline fault; report it, don't 500.
            return JSONResponse({"error": f"invalid event: {exc}"}, status_code=422)
        try:
            deferral.defer(event, body.reason)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=422)
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
        uninteresting, outcome."""
        if fake:
            return _fake_mode_error("chaos: kill-worker")
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
                frame: MetricsFrame = fake_source.tick() if fake else metrics.snapshot()
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
    args = parser.parse_args(argv)

    import uvicorn

    uvicorn.run(
        create_app(fake=args.fake, seed=args.seed, transport_mode=args.transport),
        host=args.host, port=args.port,
    )


if __name__ == "__main__":
    main()
