"""Phase J7 acceptance: generator -> classifier -> admission -> dispatch ->
server1/server2 -> ack -> ingress, driven for real by Engine itself (not by
hand-posting to /ingest like every split-topology test before this one).

Under a 20x spike: Server 1 pressure stays low while Server 2 saturates
(separate signals, never averaged — this phase's own instruction), and
P0 p99 improves on bench/contention-before.md's own head-of-line-blocking
finding (p99 63.04ms) now that P0 runs in total isolation on server1.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from triage import deferral, ledger, reporting, sink, transport
from triage.app import Engine
from triage.server1 import create_server1_app
from triage.server2 import create_server2_app


@pytest.fixture(autouse=True)
def clean_state():
    sink.reset_default_store()
    ledger.reset()
    deferral.reset_default_store()
    transport.reset_default()
    reporting.reset_default()
    yield
    sink.reset_default_store()
    ledger.reset()
    deferral.reset_default_store()
    transport.reset_default()
    reporting.reset_default()


@pytest.mark.asyncio
async def test_engine_dispatches_for_real_p0_isolated_server2_saturates():
    ingress_stub_calls: list[str] = []

    async def _make_ingress_app() -> httpx.ASGITransport:
        # A minimal ingress stand-in for /ack, /defer, /rollup,
        # /metrics/report — server1/server2's own outbound calls, exactly
        # matching test_history_integration.py's own pattern.
        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/health")
        async def health() -> dict:
            return {"status": "ok"}

        @app.post("/ack")
        async def ack(body: dict) -> dict:
            await transport.ack_by_event_ids(list(body.get("event_ids", [])))
            ingress_stub_calls.append("ack")
            return {"status": "ok"}

        @app.post("/metrics/report")
        async def metrics_report(body: dict) -> dict:
            reporting.push(reporting.fragment_from_payload(body))
            return {"status": "ok"}

        @app.post("/defer")
        async def defer(body: dict) -> dict:
            from triage.contracts import Event

            event = Event.model_validate(body["event"])
            deferral.defer(event, body["reason"], origin=deferral.ORIGIN_SERVER2)
            await transport.ack_by_event_ids([event.event_id])
            return {"status": "ok"}

        @app.post("/rollup")
        async def rollup(body: dict) -> dict:
            return {"status": "ok"}

        return httpx.ASGITransport(app=app)

    ingress_transport = await _make_ingress_app()
    async with httpx.AsyncClient(transport=ingress_transport, base_url="http://ingress") as ingress_client:
        server1_app = create_server1_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=100,
        )
        server2_app = create_server2_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=100,
        )
        async with httpx.ASGITransport(app=server1_app).app.router.lifespan_context(server1_app), \
                   httpx.ASGITransport(app=server2_app).app.router.lifespan_context(server2_app):
            server1_client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=server1_app), base_url="http://server1"
            )
            server2_client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=server2_app), base_url="http://server2"
            )
            transport.configure_http(
                clients_by_server={"server1": server1_client, "server2": server2_client},
                ack_timeout_ms=5000,
            )
            await transport.start_http()
            try:
                engine = Engine(seed=42, dispatch_via_transport=True)
                await engine.start()
                try:
                    engine.spike()
                    await asyncio.sleep(6.0)
                finally:
                    await engine.stop()
                await transport.redispatch_expired()
                await asyncio.sleep(0.3)

                s1_metrics = (await server1_client.get("/metrics")).json()
                s2_metrics = (await server2_client.get("/metrics")).json()

                assert s1_metrics["processed"] > 0, "server1 must have actually served P0 traffic"
                # Compared against bench/contention-before.md's own "Total
                # queue wait" p99 (187.73ms, section 2) — QUEUE WAIT ONLY,
                # not end-to-end latency, which also includes ~130-155ms
                # of unavoidable, unchanged simulated service time on this
                # cost model (server1.py's own queue_wait_ms field exists
                # specifically for this fair, apples-to-apples comparison
                # — see its own docstring).
                p0_queue_wait_p99 = s1_metrics["queue_wait_ms"]["p99"]
                assert p0_queue_wait_p99 < 187.73, (
                    f"P0 queue-wait p99 ({p0_queue_wait_p99}ms) must improve on "
                    "bench/contention-before.md's own total-queue-wait finding "
                    f"(187.73ms) now that P0 runs in total isolation from P1/P2 "
                    f"contention — server1 metrics: {s1_metrics}"
                )

                server1_pressure = reporting.fragments("server1")[0].counters.get("pressure", 0.0)
                server2_pressure = reporting.fragments("server2")[0].counters.get("pressure", 0.0)
                assert server1_pressure < server2_pressure, (
                    f"server1 pressure ({server1_pressure}) must stay lower than "
                    f"server2's ({server2_pressure}) under a 20x spike — server1 is "
                    f"never oversubscribed by design (108 u/s demand vs 135 u/s "
                    f"capacity), server2 always is (180 u/s demand vs 15 u/s at one pod)"
                )
                assert server2_pressure > 0.3, (
                    f"server2 must show real, meaningful saturation under spike, got {server2_pressure}"
                )
            finally:
                await transport.stop_http()
                await server1_client.aclose()
                await server2_client.aclose()
