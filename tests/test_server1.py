"""server1.py: the standalone P0 process.

`httpx.ASGITransport` throughout — a genuine HTTP request/response cycle,
no real socket, matching Phase J3's own established testing pattern (and
CLAUDE.md hard rule 2's own "deterministic on any machine" reasoning,
applied here to transport rather than service-time simulation).
"""

from __future__ import annotations

import asyncio
import dataclasses
import time

import httpx
import pytest
from fastapi import FastAPI

from triage.config import load_config
from triage.contracts import Event, EventType, Tier
from triage.server1 import P0Queue, create_server1_app
from triage.servers_config import ServerSpec, load_servers_config


def _p0_event(event_id: str, seq: int, deadline_ts: float, cost: float = 3.25, now: float | None = None) -> Event:
    now = now if now is not None else time.time()
    return Event(
        event_id=event_id, dedup_key=f"dk-{event_id}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{event_id}",
        type=EventType.PAYMENT, tier=Tier.P0, payload_size=64,
        value=120.0, cost=cost, ingest_ts=now, deadline_ts=deadline_ts,
    )


def _make_ingress_app(ack_sink: list[list[str]]) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/ack")
    async def ack(body: dict) -> dict:
        ack_sink.append(list(body.get("event_ids", [])))
        return {"status": "ok"}

    @app.post("/metrics/report")
    async def metrics_report(body: dict) -> dict:
        return {"status": "ok"}

    return app


# --------------------------------------------------------------------------
# P0Queue: pure EDF
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p0_queue_serves_earliest_deadline_first_regardless_of_arrival_order():
    q = P0Queue()
    q.put(_p0_event("late", seq=1, deadline_ts=100.0))
    q.put(_p0_event("early", seq=2, deadline_ts=10.0))
    q.put(_p0_event("middle", seq=3, deadline_ts=50.0))

    order = [
        (await q.get()).event_id,
        (await q.get()).event_id,
        (await q.get()).event_id,
    ]
    assert order == ["early", "middle", "late"]


@pytest.mark.asyncio
async def test_p0_queue_breaks_deadline_ties_by_seq():
    q = P0Queue()
    q.put(_p0_event("second", seq=5, deadline_ts=10.0))
    q.put(_p0_event("first", seq=2, deadline_ts=10.0))

    order = [(await q.get()).event_id, (await q.get()).event_id]
    assert order == ["first", "second"]


@pytest.mark.asyncio
async def test_p0_queue_get_blocks_until_an_item_is_put():
    q = P0Queue()
    got: list[str] = []

    async def waiter() -> None:
        event = await q.get()
        got.append(event.event_id)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert got == []  # nothing put yet — still waiting

    q.put(_p0_event("e1", seq=1, deadline_ts=10.0))
    await asyncio.wait_for(task, timeout=1.0)
    assert got == ["e1"]


def test_p0_queue_len():
    q = P0Queue()
    assert len(q) == 0
    q.put(_p0_event("e1", seq=1, deadline_ts=10.0))
    assert len(q) == 1


# --------------------------------------------------------------------------
# Startup assertions — enforced twice, per this phase's own instruction.
# --------------------------------------------------------------------------


def _base_spec(**overrides) -> ServerSpec:
    defaults = dict(
        name="server1", port=8001, tiers=(Tier.P0,), batching=False,
        scaling="fixed", capacity_us=135.0,
    )
    defaults.update(overrides)
    return ServerSpec(**defaults)


def test_startup_refuses_if_batching_enabled():
    with pytest.raises(RuntimeError, match="batching"):
        create_server1_app(_base_spec(batching=True), ingress_url="http://ingress")


def test_startup_refuses_if_a_non_p0_tier_is_declared():
    with pytest.raises(RuntimeError, match="P0"):
        create_server1_app(
            _base_spec(tiers=(Tier.P0, Tier.P1)), ingress_url="http://ingress"
        )


def test_startup_refuses_if_scaling_is_not_fixed():
    with pytest.raises(RuntimeError, match="fixed"):
        create_server1_app(
            _base_spec(scaling="hpa", capacity_us=None, capacity_us_per_pod=135.0),
            ingress_url="http://ingress",
        )


def test_startup_succeeds_with_the_real_servers_config():
    app = create_server1_app(ingress_url="http://ingress")
    assert app.state.pulse.worker_count >= 1


def test_worker_count_and_rate_are_derived_from_capacity_not_hardcoded():
    """server1's own capacity (135 u/s) derives 6 workers x 22.5 u/s each
    — see servers_config.py's own docstring for the formula. This test
    exists so a future change to config/servers.yaml's own capacity_us
    cannot silently drift from what server1.py actually spins up."""
    cfg = load_servers_config()
    app = create_server1_app(ingress_url="http://ingress")
    ref = load_config().worker_capacity_ups
    expected_count, expected_rate = cfg.server1.workers(reference_worker_rate_ups=ref)
    assert app.state.pulse.worker_count == expected_count
    assert app.state.pulse.per_worker_rate == pytest.approx(expected_rate)


# --------------------------------------------------------------------------
# /ingest, /ack round trip, and the second tier-enforcement layer
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_serves_and_acks_p0_events():
    ack_sink: list[list[str]] = []
    ingress_app = _make_ingress_app(ack_sink)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client:
        app = create_server1_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=10_000,
        )
        async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://server1"
            ) as client:
                events = [_p0_event(f"e{i}", seq=i, deadline_ts=time.time() + 0.2, cost=0.01) for i in range(5)]
                r = await client.post(
                    "/ingest", json={"events": [e.model_dump(mode="json") for e in events]}
                )
                assert r.status_code == 200
                assert r.json() == {"accepted": 5}

                await asyncio.sleep(0.2)  # tiny simulated cost — plenty of margin

                acked = {eid for batch in ack_sink for eid in batch}
                assert acked == {f"e{i}" for i in range(5)}

                metrics = (await client.get("/metrics")).json()
                assert metrics["processed"] == 5
                assert metrics["in_queue"] == 0
                assert metrics["in_flight"] == 0


@pytest.mark.asyncio
async def test_ingest_rejects_non_p0_events():
    ingress_app = _make_ingress_app([])

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client:
        app = create_server1_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=10_000,
        )
        async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://server1"
            ) as client:
                bad = Event(
                    event_id="bad", dedup_key="dk", seq=1, partition_key="c",
                    idempotency_key="ik", type=EventType.INVENTORY, tier=Tier.P1,
                    value=40.0, cost=2.0, ingest_ts=time.time(), deadline_ts=time.time() + 5,
                )
                r = await client.post("/ingest", json={"events": [bad.model_dump(mode="json")]})
                assert r.status_code == 422
                metrics = (await client.get("/metrics")).json()
                assert metrics["in_queue"] == 0  # never queued


# --------------------------------------------------------------------------
# /drain
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_waits_for_the_queue_to_empty_and_rejects_new_ingest():
    ack_sink: list[list[str]] = []
    ingress_app = _make_ingress_app(ack_sink)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client:
        app = create_server1_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=10_000,
        )
        async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://server1"
            ) as client:
                events = [_p0_event(f"e{i}", seq=i, deadline_ts=time.time() + 1, cost=0.05) for i in range(3)]
                await client.post("/ingest", json={"events": [e.model_dump(mode="json") for e in events]})

                drain_result = (await client.post("/drain", params={"timeout_s": 5.0})).json()
                assert drain_result["status"] == "drained"
                assert drain_result["queue_depth"] == 0
                assert drain_result["in_flight"] == 0

                # draining: further ingest is rejected
                more = [_p0_event("late", seq=99, deadline_ts=time.time() + 1)]
                r = await client.post("/ingest", json={"events": [e.model_dump(mode="json") for e in more]})
                assert r.status_code == 503

                metrics = (await client.get("/metrics")).json()
                assert metrics["draining"] is True


# --------------------------------------------------------------------------
# /healthz, /readyz
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_healthz_is_always_ok():
    app = create_server1_app(ingress_url="http://ingress")
    async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://server1"
        ) as client:
            r = await client.get("/healthz")
            assert r.status_code == 200
            assert r.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_is_not_ready_until_ingress_confirmed_then_flips_ready():
    ingress_app = _make_ingress_app([])

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client:
        app = create_server1_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=10_000,
            health_check_interval_seconds=0.02,
        )
        async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://server1"
            ) as client:
                r = await client.get("/readyz")
                assert r.status_code == 503
                assert r.json() == {"status": "not-ready"}

                await asyncio.sleep(0.1)  # a couple of health-check cycles

                r2 = await client.get("/readyz")
                assert r2.status_code == 200
                assert r2.json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_readyz_flips_back_to_not_ready_if_ingress_disappears():
    class _FlakyIngressClient:
        """Answers /health successfully exactly once, then always fails —
        standing in for ingress becoming unreachable after server1 has
        already started."""

        def __init__(self, real: httpx.AsyncClient) -> None:
            self._real = real
            self._calls = 0

        async def get(self, url: str, *a, **kw):
            self._calls += 1
            if self._calls == 1:
                return await self._real.get(url, *a, **kw)
            raise httpx.ConnectError("ingress unreachable")

        async def post(self, url: str, *a, **kw):
            return await self._real.post(url, *a, **kw)

        async def aclose(self) -> None:
            await self._real.aclose()

    ingress_app = _make_ingress_app([])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as real_client:
        flaky = _FlakyIngressClient(real_client)
        # A wide interval relative to the sleeps below, so the two
        # observation points below land unambiguously inside "only the
        # first (successful) check has run" and "at least one more (failing)
        # check has run since" — real asyncio task scheduling gives no
        # guarantee finer than "eventually", so this needs real headroom,
        # not a tight race between the loop's own cadence and the test's.
        app = create_server1_app(
            ingress_url="http://ingress", ack_client=flaky,  # type: ignore[arg-type]
            report_client=real_client, push_interval_ms=10_000,
            health_check_interval_seconds=0.3,
        )
        async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://server1"
            ) as client:
                await asyncio.sleep(0.05)  # well before the first 0.3s interval elapses
                assert (await client.get("/readyz")).status_code == 200

                await asyncio.sleep(0.4)  # past one full interval: the second check has now failed
                assert (await client.get("/readyz")).status_code == 503


# --------------------------------------------------------------------------
# The load test this phase's own prompt names verbatim: server1 alone
# under 20x P0-only load holds p99 under 200ms.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server1_alone_holds_p99_under_200ms_at_20x_p0_only_spike():
    """config/tiers.yaml's own calibration: P0 demand at the documented
    20x spike is ~108.2 u/s (payment 0.05 + order 0.05 of a 333eps mix, at
    their own costs) against server1's own 135 u/s capacity — ~80%
    utilisation, real but not razor-thin headroom (see
    tests/test_servers_config.py's own calibration test for the same
    number checked from the config side).

    Paced to the real aggregate rate over several real seconds — an actual
    wall-clock test, not a burst dumped in all at once, so the queue never
    artificially empties or overflows in a way instantaneous delivery
    would not represent. Weighted 50/50 between payment (cost 3.5u, sla
    200ms) and order (cost 3.0u, sla 500ms), matching tiers.yaml's own
    5%/5% split of the full mix (i.e. an even split of P0 itself).
    """
    cfg = load_config()
    payment_spec = cfg.tiers[EventType.PAYMENT]
    order_spec = cfg.tiers[EventType.ORDER]
    avg_p0_cost = (payment_spec.cost + order_spec.cost) / 2.0

    p0_demand_ups = cfg.demand_ups(cfg.spike_eps, Tier.P0)  # ~108.2 u/s
    events_per_second = p0_demand_ups / avg_p0_cost
    duration_s = 6.0
    interval_s = 1.0 / events_per_second

    ack_sink: list[list[str]] = []
    ingress_app = _make_ingress_app(ack_sink)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
    ) as ingress_client:
        app = create_server1_app(
            ingress_url="http://ingress", ack_client=ingress_client,
            report_client=ingress_client, push_interval_ms=10_000,
        )
        async with httpx.ASGITransport(app=app).app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://server1"
            ) as client:
                start = time.time()
                seq = 0
                sent = 0
                while time.time() - start < duration_s:
                    seq += 1
                    sent += 1
                    now = time.time()
                    spec = payment_spec if seq % 2 == 0 else order_spec
                    event_type = EventType.PAYMENT if seq % 2 == 0 else EventType.ORDER
                    event = Event(
                        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
                        partition_key="customer:0", idempotency_key=f"ik-{seq}",
                        type=event_type, tier=Tier.P0, payload_size=64,
                        value=spec.value, cost=spec.cost, ingest_ts=now,
                        deadline_ts=now + spec.sla_seconds,
                    )
                    await client.post("/ingest", json={"events": [event.model_dump(mode="json")]})
                    await asyncio.sleep(interval_s)

                # Drain whatever is left before measuring.
                drain_result = (await client.post("/drain", params={"timeout_s": 10.0})).json()
                assert drain_result["status"] == "drained", drain_result

                metrics = (await client.get("/metrics")).json()
                assert metrics["processed"] == sent
                assert len(ack_sink) > 0

                p99 = metrics["latency_ms"]["p99"]
                assert p99 < 200.0, (
                    f"server1 p99 latency {p99}ms exceeded the 200ms P0 SLA "
                    f"under {events_per_second:.1f} eps of P0-only load "
                    f"({p0_demand_ups:.1f} u/s against 135 u/s capacity)"
                )
