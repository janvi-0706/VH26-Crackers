"""server1 / server2: the two downstream HTTP processes Phase J3's own
topology names — a real, runnable FastAPI app per server, not just an
interface.

Owner: Lane A (Phase J3).

Deliberately small — this is the minimum that makes `POST /ingest` mean
something real: simulate each event's own cost-model service time (the
SAME simulated-sleep mechanism `worker.py` already uses, so a demo run
against the split topology has the same deterministic capacity ceiling
CLAUDE.md hard rule 2 already establishes for the single-process build),
then POST an ack back to ingress per event.

Explicitly, and on purpose, NOT built here (a later prompt's job, named so
it is not mistaken for an oversight): the actual sink write below is a
direct, same-process call to `sink.write()`, not an HTTP call to ingress.
docs/PHASE-J-INSPECTION.md (section 3) already names `sink.py` as
ingress-owned once this genuinely runs as three separate OS processes —
moving that specific write over the wire is real, separate scope this
phase's own prompt does not ask for ("Implement transport.py and
reporting.py over HTTP" — sink forwarding is neither). Running this file
as a literally separate process today would therefore give it its own,
local SQLite file rather than ingress's one — a known gap, not a silent
one, until that later prompt closes it.

Also deliberately simple: this app has no queue, no decision engine, no
ladder, no CoDel — `worker.py`'s own routing logic (STREAM_NOW vs.
MICRO_BATCH vs. DEFER vs. SAMPLE_ROLLUP vs. SHED) stays exactly where
Phase J1's own inspection said it belongs (server2, once that split is
real); this file only proves the TRANSPORT half Phase J3 is actually
about — receive a batch, simulate service, ack. Wiring `worker.py`'s own
decision machinery into this app is real, separate, future scope.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Callable

import httpx
from fastapi import FastAPI

from . import reporting, sink
from .config import load_config
from .contracts import Event
from .servers_config import ServerSpec, load_servers_config

logger = logging.getLogger(__name__)

SinkWriter = Callable[[Event], object]


def create_server_app(
    spec: ServerSpec,
    *,
    ingress_url: str,
    ack_client: httpx.AsyncClient | None = None,
    report_client: httpx.AsyncClient | None = None,
    sink_write: SinkWriter = sink.write,
    reference_worker_rate_ups: float | None = None,
    push_interval_ms: float | None = None,
) -> FastAPI:
    """Build one server's FastAPI app. `spec` (from `servers_config.py`)
    says which tiers it serves and how much capacity it has; worker count
    and per-worker rate are DERIVED from that capacity
    (`ServerSpec.workers()` — see `servers_config.py`'s own docstring for
    why this must never be a hardcoded shared count).
    """
    app = FastAPI(title=f"PULSE-{spec.name}")
    app.state.spec = spec
    app.state.ingress_url = ingress_url.rstrip("/")
    app.state.ack_client = ack_client or httpx.AsyncClient(timeout=5.0)
    app.state.owns_ack_client = ack_client is None
    app.state.processed_count = 0

    cfg = load_config()
    reference_rate = reference_worker_rate_ups or cfg.worker_capacity_ups
    worker_count, per_worker_rate = spec.workers(reference_worker_rate_ups=reference_rate)
    app.state.worker_count = worker_count
    app.state.per_worker_rate = per_worker_rate
    # A crude capacity gate, not this server's real decision engine (see
    # this module's own top docstring on why that stays out of scope
    # here): at most `worker_count` events simulating service at once,
    # matching the capacity this server's own config actually derives —
    # enough to prove the topology's capacity numbers are real, not a
    # claim that this replaces `worker.py`'s own scoring/batching/ladder
    # logic.
    app.state.semaphore = asyncio.Semaphore(max(1, worker_count))

    async def _serve_one(event: Event) -> None:
        async with app.state.semaphore:
            await asyncio.sleep(event.cost / app.state.per_worker_rate)
            sink_write(event)
            app.state.processed_count += 1
        try:
            response = await app.state.ack_client.post(
                f"{app.state.ingress_url}/ack",
                json={"event_ids": [event.event_id]},
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001 - a lost ack is exactly what
            # ingress's own redispatch sweep (transport.py) exists to
            # recover from; this handler must not raise out of what is
            # otherwise a completed, sink-written event.
            logger.debug("ack post failed for %s", event.event_id, exc_info=True)

    @app.post("/ingest")
    async def ingest(body: dict) -> dict:
        events = [Event.model_validate(e) for e in body.get("events", [])]
        for event in events:
            await _serve_one(event)
        return {"accepted": len(events)}

    def _collect_metrics() -> dict[str, float]:
        return {"processed": float(app.state.processed_count)}

    app.state.reporting_client = reporting.ReportingClient(
        server=spec.name,
        ingress_url=ingress_url,
        collect=_collect_metrics,
        push_interval_ms=push_interval_ms,
        client=report_client,
    )

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.reporting_client.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await app.state.reporting_client.stop()
        if app.state.owns_ack_client:
            await app.state.ack_client.aclose()

    @app.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "server": spec.name,
            "worker_count": app.state.worker_count,
            "processed": app.state.processed_count,
        }

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run one PULSE downstream server (server1/server2).")
    parser.add_argument("--name", required=True, choices=["server1", "server2"])
    parser.add_argument("--ingress", default=None, help="ingress base URL, e.g. http://127.0.0.1:8000")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args(argv)

    cfg = load_servers_config()
    spec = cfg.server(args.name)
    ingress_url = args.ingress or f"http://127.0.0.1:{cfg.ingress.port}"
    port = args.port or spec.port

    import uvicorn

    uvicorn.run(create_server_app(spec, ingress_url=ingress_url), host=args.host, port=port)


if __name__ == "__main__":
    main()
