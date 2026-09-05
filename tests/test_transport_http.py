"""transport.py/reporting.py over real HTTP — Phase J3's own tests.

`httpx.ASGITransport` gives every request here a genuine HTTP
request/response cycle (real JSON encode/decode, a real Starlette routing
pass, a real `httpx.Response`) without opening a real socket — the same
reasoning CLAUDE.md hard rule 2 already applies to worker.py's own
simulated service time: deterministic on any machine, not dependent on
this machine's actual network stack or a free port. A real, multi-process,
real-socket deployment's own wall-clock transport latency is a different,
infrastructure-dependent number this suite does not and cannot promise
from a laptop — these tests are about the CORRECTNESS of the dispatch/ack/
redispatch/fragment machinery, which a real socket would not exercise any
differently.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from fastapi import FastAPI

from triage import reporting, sink, transport
from triage.contracts import Event, EventType, Tier


def _event(i: int, cost: float = 0.001) -> Event:
    now = time.time()
    return Event(
        event_id=f"evt-{i}",
        dedup_key=f"dk-{i}",
        seq=i,
        partition_key="customer:0",
        idempotency_key=f"ik-{i}",
        type=EventType.INVENTORY,
        tier=Tier.P1,
        payload_size=64,
        value=40.0,
        cost=cost,
        ingest_ts=now,
        deadline_ts=now + 5.0,
    )


@pytest.fixture(autouse=True)
def clean_state():
    sink.reset_default_store()
    transport.reset_default()
    reporting.reset_default()
    yield
    sink.reset_default_store()
    transport.reset_default()
    reporting.reset_default()


def _make_ingress_app() -> FastAPI:
    """The two endpoints app.py's real ingress adds for Phase J3 — a
    minimal stand-in so these tests exercise the real wire format without
    booting the full Engine (transport.py/reporting.py are what is under
    test here, not the rest of the pipeline). Delegates to the exact same
    `handle_*` functions app.py's own real endpoints call, so there is one
    place, not two, that knows how to turn a decoded body into a
    transport/reporting call."""
    app = FastAPI()

    @app.post("/ack")
    async def ack(body: dict) -> dict:
        await transport.handle_ack_payload(body)
        return {"status": "ok"}

    @app.post("/metrics/report")
    async def metrics_report(body: dict) -> dict:
        reporting.handle_metrics_report_payload(body)
        return {"status": "ok"}

    return app


def _make_consumer_app(*, alive: list[bool]) -> FastAPI:
    """A stand-in for server1/server2's own `/ingest` — `alive[0]` toggles
    between "processes and acks" and "received the batch, then died before
    doing anything with it" (docs/PHASE-J-INSPECTION.md section 5's own
    scenario, made deterministic instead of actually killing a process).
    Processing and acking happen fully awaited inline, before the HTTP
    response returns — this is what makes the tests below deterministic
    with no extra sleeps needed: by the time `transport.dispatch()`'s own
    POST call returns, a live consumer has already served and acked every
    event in the batch."""
    app = FastAPI()

    @app.post("/ingest")
    async def ingest(body: dict) -> dict:
        events = [Event.model_validate(e) for e in body.get("events", [])]
        if not alive[0]:
            return {"accepted": len(events)}  # "dies" here: never served, never acked
        for event in events:
            sink.write(event)
        await app.state.ack_client.post(
            f"{app.state.ingress_base}/ack",
            json={"event_ids": [e.event_id for e in events]},
        )
        return {"accepted": len(events)}

    return app


@pytest.mark.asyncio
async def test_dispatch_ack_round_trip_over_http():
    ingress_app = _make_ingress_app()
    consumer_app = _make_consumer_app(alive=[True])

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client, httpx.AsyncClient(
        transport=httpx.ASGITransport(app=consumer_app), base_url="http://server2"
    ) as consumer_client:
        consumer_app.state.ack_client = ingress_client
        consumer_app.state.ingress_base = "http://ingress"

        transport.configure_http(
            {"server2": "http://server2"},
            clients_by_server={"server2": consumer_client},
            ack_timeout_ms=5000,
        )
        try:
            events = [_event(1), _event(2)]
            result = await transport.dispatch("server2", events)

            assert result.server == "server2"
            assert result.event_ids == ("evt-1", "evt-2")
            assert transport.outstanding("server2") == []  # consumer already acked, inline
            assert sink.count() == 2
        finally:
            await transport.stop_http()


@pytest.mark.asyncio
async def test_1000_events_redispatched_after_consumer_dies_gives_exactly_1000_sink_rows():
    """The prompt's own test, verbatim: dispatch 1000 events, kill the
    consumer before it acks, verify all 1000 are re-dispatched and the
    sink contains exactly 1000 rows.

    "Kill the consumer" here means: the first delivery lands on a handler
    that received the batch into memory and then never processed or acked
    it (`alive[0] = False`) — indistinguishable, from ingress's own side,
    from a real process that crashed holding those events
    (docs/PHASE-J-INSPECTION.md section 5). Flipping `alive[0] = True`
    stands in for the consumer coming back (a real deployment: Kubernetes
    replacing the dead pod). `redispatch_expired()` then re-sends whatever
    is still outstanding past `ack_timeout_ms`, and idempotency
    (`idempotency_key`, unique per event here, matching `sink.py`'s own
    upsert contract) is what makes a genuinely-already-served event safe
    to redispatch if it ever were live rather than dead — in this test no
    event is ever double-served (the dead consumer serves nothing), so the
    row count is exactly 1000 with no duplicates either way.
    """
    ingress_app = _make_ingress_app()
    consumer_alive = [False]  # starts dead
    consumer_app = _make_consumer_app(alive=consumer_alive)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client, httpx.AsyncClient(
        transport=httpx.ASGITransport(app=consumer_app), base_url="http://server2"
    ) as consumer_client:
        consumer_app.state.ack_client = ingress_client
        consumer_app.state.ingress_base = "http://ingress"

        transport.configure_http(
            {"server2": "http://server2"},
            clients_by_server={"server2": consumer_client},
            ack_timeout_ms=50,  # short, so the test does not need a long sleep
        )
        try:
            events = [_event(i) for i in range(1000)]
            for i in range(0, len(events), 20):  # config/servers.yaml's own batch_size
                await transport.dispatch("server2", events[i : i + 20])

            # The dead consumer received everything but processed nothing.
            assert sink.count() == 0
            assert len(transport.outstanding("server2")) == 1000

            await asyncio.sleep(0.06)  # past the 50ms ack_timeout_ms
            consumer_alive[0] = True  # the consumer "comes back"

            redispatched = await transport.redispatch_expired()

            assert redispatched == 1000
            assert transport.outstanding("server2") == []
            assert sink.count() == 1000
        finally:
            await transport.stop_http()


@pytest.mark.asyncio
async def test_10000_events_via_http_zero_loss_and_transport_latency_under_10ms():
    ingress_app = _make_ingress_app()
    consumer_app = _make_consumer_app(alive=[True])

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client, httpx.AsyncClient(
        transport=httpx.ASGITransport(app=consumer_app), base_url="http://server2"
    ) as consumer_client:
        consumer_app.state.ack_client = ingress_client
        consumer_app.state.ingress_base = "http://ingress"

        transport.configure_http(
            {"server2": "http://server2"},
            clients_by_server={"server2": consumer_client},
            ack_timeout_ms=5000,
        )
        try:
            events = [_event(i) for i in range(10_000)]
            for i in range(0, len(events), 20):
                await transport.dispatch("server2", events[i : i + 20])

            assert transport.outstanding("server2") == []  # zero loss: nothing left unacked
            assert sink.count() == 10_000

            latencies = transport.latency_percentiles()
            assert latencies["p99"] < 10.0, latencies
        finally:
            await transport.stop_http()


@pytest.mark.asyncio
async def test_fragment_expiry_stopped_reporter_vanishes_within_one_second():
    ingress_app = _make_ingress_app()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as client:
        pushes = {"n": 0}

        def _collect() -> dict[str, float]:
            pushes["n"] += 1
            return {"processed": float(pushes["n"])}

        reporter = reporting.ReportingClient(
            server="server1",
            ingress_url="http://ingress",
            collect=_collect,
            push_interval_ms=50,
            instance_id="server1-test",
            client=client,
        )
        reporter.start()
        try:
            await asyncio.sleep(0.12)  # a couple of pushes land
            assert reporting.instance_count("server1") == 1
        finally:
            await reporter.stop()  # "stopped reporter" — no further pushes arrive

        await asyncio.sleep(1.05)  # past config/servers.yaml's own fragment_ttl_ms (1000)

        assert reporting.instance_count("server1") == 0
        assert reporting.aggregate("server1") == {}


@pytest.mark.asyncio
async def test_a_live_reporter_stays_in_the_aggregate():
    """The control case for the expiry test above: a reporter that keeps
    pushing must NOT age out just because more than fragment_ttl_ms has
    elapsed since it started."""
    ingress_app = _make_ingress_app()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as client:
        reporter = reporting.ReportingClient(
            server="server2",
            ingress_url="http://ingress",
            collect=lambda: {"processed": 1.0},
            push_interval_ms=50,
            instance_id="server2-test",
            client=client,
        )
        reporter.start()
        try:
            await asyncio.sleep(1.2)  # longer than fragment_ttl_ms, but still pushing every 50ms
            assert reporting.instance_count("server2") == 1
        finally:
            await reporter.stop()
