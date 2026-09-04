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
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import decision, deferral, ledger, metrics
from .classifier import Classifier
from .config import Config, load_config
from .contracts import Event, EventType, MetricsFrame
from .fake_metrics import FakeSource
from .generator import EventGenerator
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
        self.queue = EventQueue(config=self.config)
        self.workers = WorkerPool(self.queue, config=self.config)
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
        await self.workers.stop()
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
        audit trail."""
        async for raw in self.generator.events(self._stop):
            event = self.classifier.classify(raw)
            await self.queue.put(event)


class RateBody(BaseModel):
    rate: float


class ModeBody(BaseModel):
    mode: str


class InjectBody(BaseModel):
    type: str
    partition_key: str | None = None


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


def create_app(*, fake: bool = False, seed: int | None = None) -> FastAPI:
    """Build one FastAPI app in either mode. Kept as a factory (rather than a
    single module-level app) so tests can construct independent instances."""

    fake_source = FakeSource(seed=seed) if fake else None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
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

    app = FastAPI(title="PULSE", lifespan=lifespan)
    app.state.mode = "fake" if fake else "real"

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {
            "status": "ok",
            "mode": app.state.mode,
            "uptime_s": None if fake else round(metrics.uptime_seconds(), 1),
        }

    @app.post("/control/rate")
    async def control_rate(body: RateBody) -> JSONResponse:
        if fake:
            return _fake_mode_error("rate control")
        if body.rate < 0:
            return JSONResponse({"error": "rate must be non-negative"}, status_code=422)
        app.state.engine.set_rate(body.rate)
        return JSONResponse({"rate": body.rate})

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
    args = parser.parse_args(argv)

    import uvicorn

    uvicorn.run(create_app(fake=args.fake, seed=args.seed), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
