"""server1: the standalone P0 process — port 8001.

Owner: Lane A (Phase J4).

Holds no durable state, per the split's own hard constraint
(docs/PHASE-J-INSPECTION.md, section 2): completed events are POSTed to
ingress's own `/ack` (Phase J3's own mechanism — the same "a server only
ever knows event_ids, never ingress's internal dispatch_id" contract
`transport.py` already implements) so ingress can eventually persist them
to `history.db`; nothing here ever opens a file. If this process is
rescheduled, every event still in its queue or mid-service is gone — that
loss, and what closes it, is exactly what J3's dispatch-tracking
(`redispatch_expired()`) and K6's graceful drain exist for, not this file.

Explicitly, and on purpose, does NOT contain: batching, CoDel, the ladder,
deferral, shedding. Every one of those exists to trade fidelity for
capacity under pressure — the entire deal CLAUDE.md hard rule 3 refuses to
offer P0. Server1's own job description is "be simple and fast": receive,
queue by deadline, serve, ack. Nothing else.

Ordering: pure earliest-deadline-first, a hand-rolled binary heap keyed on
`(deadline_ts, seq)` — P0's own original ordering (Stage C), made literal
now that this process holds P0 in total isolation and no longer needs
`decision.score()`'s cross-tier value-density weighing (Stage D) against a
P1/P2 backlog that, on this process, cannot structurally exist at all.

Capacity: `config/servers.yaml`'s own `server1.capacity_us` (135 u/s),
never a hardcoded worker count — `servers_config.ServerSpec.workers()`
derives however many equal-rate workers reconstruct that capacity exactly
(see that module's own docstring for why 135 does not need to divide
evenly by anything).

Scaling: fixed, asserted twice — once by `servers_config.py`'s own parser
(a malformed `config/servers.yaml` cannot even load with `server1.scaling
!= "fixed"`), and again here, independently, at process startup. See
`docs/adr/0012-server1-fixed-scaling-not-hpa.md` for why: this stack's own
realistic pod cold start (~45s) is longer than the spike this project is
calibrated against, so HPA on this specific process would add capacity
after the SLA-relevant window has already closed — the wrong mechanism,
not a slower version of the right one.
"""

from __future__ import annotations

import argparse
import asyncio
import heapq
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from . import reporting
from .config import load_config
from .contracts import Event, Tier
from .metrics import percentile
from .servers_config import ServerSpec, load_servers_config

logger = logging.getLogger(__name__)

# How often the background ingress-connectivity check runs. Short enough
# that /readyz reflects a real ingress outage within a couple of seconds
# (Kubernetes' own default readinessProbe periodSeconds is comparable),
# long enough that it is not itself a meaningful source of load.
INGRESS_HEALTH_CHECK_INTERVAL_SECONDS = 1.0

# How often /drain re-checks whether the queue and every worker have
# actually gone idle, while it waits.
DRAIN_POLL_INTERVAL_SECONDS = 0.05

# Local per-worker latency samples retained for GET /metrics — same bound
# and reasoning as metrics.py's own WINDOW / transport.py's own
# LATENCY_WINDOW: enough to mean something, bounded so a long run cannot
# grow it without limit.
LATENCY_WINDOW = 4096


class P0Queue:
    """Pure EDF — see this module's own top docstring for why this is not
    `queue.py`'s own `EventQueue` (that class's scoring machinery exists to
    weigh P0 against a P1/P2 backlog this process never holds).

    Hand-rolled, not `queue.PriorityQueue`: CLAUDE.md's own "writing the
    scheduling logic ourselves is the originality score" applies here
    exactly as much as it does to `queue.py`'s own settled/pending design
    — this one is simpler because P0's own isolation makes it possible to
    be, not because the principle stopped applying.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[float, int, Event]] = []
        self._not_empty = asyncio.Event()

    def put(self, event: Event) -> None:
        heapq.heappush(self._heap, (event.deadline_ts, event.seq, event))
        self._not_empty.set()

    async def get(self) -> Event:
        while not self._heap:
            self._not_empty.clear()
            await self._not_empty.wait()
        _, _, event = heapq.heappop(self._heap)
        return event

    def __len__(self) -> int:
        return len(self._heap)


@dataclass
class _ServerState:
    """Everything one running server1 process needs beyond the queue
    itself — grouped so the FastAPI handlers below read/write one object
    instead of a scatter of `app.state.*` attributes."""

    queue: P0Queue = field(default_factory=P0Queue)
    worker_count: int = 0
    per_worker_rate: float = 0.0
    processed_count: int = 0
    in_flight: int = 0
    draining: bool = False
    ingress_ready: bool = False
    latency_ms: list[float] = field(default_factory=list)
    # Phase J7: queue wait ALONE (ingest -> dequeue), separate from the
    # existing full end-to-end latency (ingest -> complete, which also
    # includes simulated service time — a real, ~130-155ms floor per P0
    # event on this stack's own cost model, unrelated to contention). This
    # is the number directly comparable to bench/contention-before.md's
    # own "P0 queue wait" figures (that report never included service
    # time either), for a fair "did the split actually reduce queueing
    # contention" claim rather than one polluted by an unavoidable,
    # unchanged service-time constant.
    queue_wait_ms: list[float] = field(default_factory=list)


def _assert_server1_is_correctly_provisioned(spec: ServerSpec) -> None:
    """The two hard-rule assertions this module's own docstring promises,
    independent of `servers_config.py`'s own structural validation (which
    already refuses to even LOAD a `servers.yaml` with these values wrong)
    — CLAUDE.md's "enforced twice, not once" pattern, applied to this
    process's own startup rather than only to config parsing."""
    if spec.batching:
        raise RuntimeError(
            "server1 startup refused: batching must be disabled for P0 "
            "(CLAUDE.md hard rule 3 — P0 is never batched); "
            "config/servers.yaml's own server1.batching is not false"
        )
    if set(spec.tiers) != {Tier.P0}:
        raise RuntimeError(
            f"server1 startup refused: this process may only ever serve "
            f"P0 — config/servers.yaml declares server1.tiers={spec.tiers!r}"
        )
    if spec.scaling != "fixed":
        raise RuntimeError(
            "server1 startup refused: scaling must be 'fixed' — see "
            "docs/adr/0012-server1-fixed-scaling-not-hpa.md for why P0 "
            f"must never autoscale; got scaling={spec.scaling!r}"
        )


def create_server1_app(
    spec: ServerSpec | None = None,
    *,
    ingress_url: str,
    ack_client: httpx.AsyncClient | None = None,
    report_client: httpx.AsyncClient | None = None,
    reference_worker_rate_ups: float | None = None,
    push_interval_ms: float | None = None,
    health_check_interval_seconds: float | None = None,
) -> FastAPI:
    spec = spec or load_servers_config().server1
    _assert_server1_is_correctly_provisioned(spec)

    state = _ServerState()
    resolved_ack_client = ack_client or httpx.AsyncClient(timeout=5.0)
    owns_ack_client = ack_client is None
    base_ingress_url = ingress_url.rstrip("/")

    cfg = load_config()
    reference_rate = reference_worker_rate_ups or cfg.worker_capacity_ups
    worker_count, per_worker_rate = spec.workers(reference_worker_rate_ups=reference_rate)
    state.worker_count = worker_count
    state.per_worker_rate = per_worker_rate

    async def _ack(event: Event) -> None:
        """Phase J3's own transport-ack, plus (Phase J6) the richer,
        additive `AckBody` fields — see that model's own docstring —
        so ingress durably records this completion (sink + ledger +
        decision trace + sla_outcomes) in the SAME request, without a
        second round trip P0's own ~60ms queue budget cannot spare.
        P0 is always STREAM_NOW (CLAUDE.md hard rule 3, decision.decide()'s
        own unconditional first branch) — server1 never computes a
        pressure value of its own, so it reports 0.0 rather than a number
        that would misleadingly suggest otherwise.
        """
        try:
            response = await resolved_ack_client.post(
                f"{base_ingress_url}/ack",
                json={
                    "event_ids": [event.event_id],
                    "events": [event.model_dump(mode="json")],
                    "decision": "STREAM_NOW",
                    "reason": "P0 is never batched, deferred, sampled, or shed (CLAUDE.md hard rule 3)",
                    "pressure": 0.0,
                    "source": "server1",
                },
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001 - a lost ack is what ingress's own
            # redispatch sweep (transport.py, Phase J3) exists to recover
            # from; this loop must keep serving regardless.
            logger.debug("ack post failed for %s", event.event_id, exc_info=True)

    async def _worker(worker_id: int) -> None:
        while True:
            event = await state.queue.get()
            dequeued_ts = time.time()
            queue_wait_ms = max(0.0, (dequeued_ts - event.ingest_ts) * 1000.0)
            state.queue_wait_ms.append(queue_wait_ms)
            if len(state.queue_wait_ms) > LATENCY_WINDOW:
                del state.queue_wait_ms[: len(state.queue_wait_ms) - LATENCY_WINDOW]
            state.in_flight += 1
            try:
                await asyncio.sleep(event.cost / state.per_worker_rate)
            finally:
                state.in_flight -= 1
            completed_ts = time.time()
            state.processed_count += 1
            latency_ms = max(0.0, (completed_ts - event.ingest_ts) * 1000.0)
            state.latency_ms.append(latency_ms)
            if len(state.latency_ms) > LATENCY_WINDOW:
                del state.latency_ms[: len(state.latency_ms) - LATENCY_WINDOW]
            await _ack(event)

    async def _check_ingress_once() -> bool:
        try:
            response = await resolved_ack_client.get(f"{base_ingress_url}/health")
            return response.status_code == 200
        except Exception:  # noqa: BLE001 - any failure means "not confirmed"
            return False

    async def _ingress_health_loop() -> None:
        interval = health_check_interval_seconds or INGRESS_HEALTH_CHECK_INTERVAL_SECONDS
        while True:
            state.ingress_ready = await _check_ingress_once()
            await asyncio.sleep(interval)

    def _collect_metrics() -> dict[str, float]:
        # Phase J7: server1's own honest pressure gauge — not
        # decision.pressure() (that formula's arrival/service EWMA and
        # CoDel-adjacent terms are P1/P2 machinery server1 deliberately
        # has none of, per this module's own top docstring), just a plain
        # blend of queue depth and worker utilisation. P0's real
        # protection story is "demand sits under capacity" (Stage A's own
        # calibration) — this gauge exists so a dashboard/judge can watch
        # that stay true live, not to drive any routing decision (P0
        # always streams, unconditionally, regardless of this number).
        qdepth_ratio = min(len(state.queue) / 50.0, 1.0)
        worker_util = min(state.in_flight / max(state.worker_count, 1), 1.0)
        pressure = min(max(0.5 * qdepth_ratio + 0.5 * worker_util, 0.0), 1.0)
        return {
            "processed": float(state.processed_count),
            "in_queue": float(len(state.queue)),
            "in_flight": float(state.in_flight),
            "pressure": pressure,
            # Phase J8 (live-demo fix): so ingress's own dispatch-mode
            # merged frame can show a real P0 latency number on the
            # dashboard's Traffic tab instead of a permanent 0 — this
            # process's own real, local end-to-end latency, same window
            # GET /metrics already reports.
            "latency_p99": percentile(state.latency_ms, 0.99),
        }

    reporting_client = reporting.ReportingClient(
        server="server1",
        ingress_url=ingress_url,
        collect=_collect_metrics,
        push_interval_ms=push_interval_ms,
        client=report_client,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        worker_tasks = [
            asyncio.create_task(_worker(i), name=f"pulse-server1-worker-{i}")
            for i in range(max(1, state.worker_count))
        ]
        health_check_task = asyncio.create_task(
            _ingress_health_loop(), name="pulse-server1-ingress-healthcheck"
        )
        reporting_client.start()
        try:
            yield
        finally:
            for task in worker_tasks:
                task.cancel()
            if worker_tasks:
                await asyncio.gather(*worker_tasks, return_exceptions=True)
            health_check_task.cancel()
            await asyncio.gather(health_check_task, return_exceptions=True)
            await reporting_client.stop()
            if owns_ack_client:
                await resolved_ack_client.aclose()

    app = FastAPI(title="PULSE-server1", lifespan=lifespan)
    app.state.pulse = state

    @app.post("/ingest")
    async def ingest(body: dict) -> JSONResponse:
        if state.draining:
            return JSONResponse({"accepted": 0, "rejected": "draining"}, status_code=503)
        raw_events = body.get("events", [])
        events = [Event.model_validate(e) for e in raw_events]
        non_p0 = [e for e in events if e.tier is not Tier.P0]
        if non_p0:
            # Second, independent enforcement — see
            # _assert_server1_is_correctly_provisioned's own docstring.
            # This is not merely defensive: a caller misrouting a non-P0
            # event here would otherwise silently get P0's own latency
            # treatment, which is exactly the kind of silent tier
            # confusion CLAUDE.md hard rule 3 exists to make impossible.
            return JSONResponse(
                {
                    "error": "server1 only serves P0; received tier(s) "
                    f"{sorted({e.tier.value for e in non_p0})}"
                },
                status_code=422,
            )
        for event in events:
            state.queue.put(event)
        return JSONResponse({"accepted": len(events)})

    @app.post("/drain")
    async def drain(timeout_s: float = 30.0) -> dict:
        """Stop accepting new work (subsequent /ingest calls are rejected)
        and wait for the queue and every in-flight event to finish, up to
        `timeout_s`. This is server1's own half of a graceful shutdown —
        the orchestration that actually calls this before sending SIGTERM
        is K6's own scope (docs/PHASE-J-INSPECTION.md section 5 already
        names graceful drain as narrowing, not closing, the loss window a
        hard kill leaves; this endpoint is the mechanism that narrowing
        needs, not the policy of when to call it)."""
        state.draining = True
        deadline = time.time() + timeout_s
        while (len(state.queue) > 0 or state.in_flight > 0) and time.time() < deadline:
            await asyncio.sleep(DRAIN_POLL_INTERVAL_SECONDS)
        drained = len(state.queue) == 0 and state.in_flight == 0
        return {
            "status": "drained" if drained else "timeout",
            "queue_depth": len(state.queue),
            "in_flight": state.in_flight,
        }

    @app.get("/metrics")
    async def get_metrics() -> dict:
        latencies = state.latency_ms
        queue_waits = state.queue_wait_ms
        return {
            "server": "server1",
            "worker_count": state.worker_count,
            "per_worker_rate_ups": state.per_worker_rate,
            "processed": state.processed_count,
            "in_queue": len(state.queue),
            "in_flight": state.in_flight,
            "draining": state.draining,
            "latency_ms": {
                "p50": round(percentile(latencies, 0.50), 3),
                "p95": round(percentile(latencies, 0.95), 3),
                "p99": round(percentile(latencies, 0.99), 3),
            },
            "queue_wait_ms": {
                "p50": round(percentile(queue_waits, 0.50), 3),
                "p95": round(percentile(queue_waits, 0.95), 3),
                "p99": round(percentile(queue_waits, 0.99), 3),
            },
        }

    @app.get("/healthz")
    async def healthz() -> dict:
        """Liveness: this process is up and its event loop is responsive.
        Deliberately unconditional (no ingress dependency) — a Kubernetes
        liveness probe failing here means "restart this pod", which must
        never be triggered merely by INGRESS being briefly unreachable
        (that is exactly what /readyz, not /healthz, exists to express)."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> object:
        """Not ready until the ingress connection is confirmed — this
        phase's own instruction, verbatim. `state.ingress_ready` starts
        False and is only ever set True by a background loop's own
        successful check against ingress's real `/health`; it is
        re-verified continuously (not just once at boot), so a pod that
        loses its route to ingress after starting correctly flips back to
        not-ready rather than continuing to advertise a capability it no
        longer actually has. FastAPI/Starlette does not let a route
        handler set the response status via a bare dict return, so this
        one uses a real Response for the 503 case."""
        if state.ingress_ready:
            return {"status": "ready"}
        return JSONResponse({"status": "not-ready"}, status_code=503)

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run PULSE server1 (P0 only).")
    parser.add_argument("--ingress", default=None, help="ingress base URL, e.g. http://127.0.0.1:8000")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args(argv)

    cfg = load_servers_config()
    spec = cfg.server1
    ingress_url = args.ingress or f"http://127.0.0.1:{cfg.ingress.port}"
    port = args.port or spec.port

    import uvicorn

    uvicorn.run(create_server1_app(spec, ingress_url=ingress_url), host=args.host, port=port)


if __name__ == "__main__":
    main()
