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

Stage C: the queue is now the three-heap priority structure from queue.py
(P0 by EDF, P1/P2 by arrival, a bounded aging exception for P2). This file
did not need to change for that — Engine just constructs an EventQueue and
never looks inside it.
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

from . import metrics
from .classifier import Classifier
from .config import Config, load_config
from .contracts import MetricsFrame
from .fake_metrics import FakeSource
from .generator import EventGenerator
from .queue import EventQueue
from .worker import WorkerPool

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_DIST = REPO_ROOT / "dashboard" / "dist"

SNAPSHOT_HZ = 4.0
SNAPSHOT_PERIOD = 1.0 / SNAPSHOT_HZ


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
        self.queue = EventQueue()
        self.workers = WorkerPool(self.queue, config=self.config)
        self._stop = asyncio.Event()
        self._ingest_task: asyncio.Task[None] | None = None

    def set_rate(self, rate: float) -> None:
        self.generator.set_rate(rate)

    async def start(self) -> None:
        self.workers.start()
        self._ingest_task = asyncio.create_task(self._ingest(), name="pulse-ingest")

    async def stop(self) -> None:
        self._stop.set()
        if self._ingest_task is not None:
            self._ingest_task.cancel()
            await asyncio.gather(self._ingest_task, return_exceptions=True)
        await self.workers.stop()

    async def _ingest(self) -> None:
        """generator -> classifier -> queue, one event at a time."""
        async for raw in self.generator.events(self._stop):
            event = self.classifier.classify(raw)
            await self.queue.put(event)


class RateBody(BaseModel):
    rate: float


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
            return JSONResponse(
                {"error": "rate control has no effect in --fake mode"},
                status_code=409,
            )
        if body.rate < 0:
            return JSONResponse({"error": "rate must be non-negative"}, status_code=422)
        app.state.engine.set_rate(body.rate)
        return JSONResponse({"rate": body.rate})

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
