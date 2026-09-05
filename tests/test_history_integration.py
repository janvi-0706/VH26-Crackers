"""Phase J6: ingress as the single writer for history.db.

`httpx.ASGITransport` throughout — a genuine HTTP request/response cycle for
every hop (ingress <-> server1, ingress <-> server2), no real socket,
matching Phase J3/J4/J5's own established testing pattern.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from triage import deferral, history_db, ledger, reporting, sink, transport
from triage.app import create_app
from triage.config import load_config
from triage.contracts import Event, EventType, Tier
from triage.server1 import create_server1_app
from triage.server2 import create_server2_app


@pytest.fixture(autouse=True)
def clean_ambient_state():
    """Every store this phase wires onto a shared connection is an ambient
    module-level default (sink.py/ledger.py/deferral.py's own long-
    standing precedent) — reset all of them, plus transport/reporting,
    before and after each test in this file so a real `history.db`
    connection from one test can never leak into the next."""
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


def _event(event_id: str, seq: int, *, tier: Tier, event_type: EventType, spec, now: float) -> Event:
    return Event(
        event_id=event_id, dedup_key=f"dk-{event_id}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{event_id}",
        type=event_type, tier=tier, payload_size=64,
        value=spec.value, cost=spec.cost, ingest_ts=now, deadline_ts=now + spec.sla_seconds,
    )


# --------------------------------------------------------------------------
# The literal acceptance line: after a spike across all three processes,
# the conservation-relevant durable records exist, verify_chain() passes,
# and shed_critical is zero.
#
# Scaled from a literal 60 real seconds to 8: at this test harness's own
# per-request ASGI overhead (no real socket, but still real Python/
# Starlette/pydantic work per call), an UNPACED 60-second run against
# server2 produces tens of thousands of individual /ack + /ingest calls,
# each triggering real SQLite commits (sink write + ledger row + decision
# trace + sla_outcome, four commits per completion) — the wall-clock cost
# of that volume dominates the whole test suite's own runtime without
# adding proportionally more evidence for what this test actually checks
# (the WRITE PATH is correct, not a specific throughput number — that is
# bench/run.py's own job). 8 real, continuously-arriving seconds already
# produces thousands of durable rows across all three processes, which is
# what every assertion below actually needs.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_db_survives_a_multi_process_spike_chain_verifies_shed_critical_zero(tmp_path):
    db_path = tmp_path / "history.db"
    connection = history_db.open_history_db(db_path)
    history_db.wire_ambient_stores(connection)
    try:
        mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal", "history_db.open_history_db must actually enable WAL mode"

        ingress_app = create_app(fake=False, transport_mode="direct", seed=7)
        async with httpx.ASGITransport(app=ingress_app).app.router.lifespan_context(ingress_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
            ) as ingress_client:
                server1_app = create_server1_app(
                    ingress_url="http://ingress", ack_client=ingress_client,
                    report_client=ingress_client, push_interval_ms=250,
                )
                server2_app = create_server2_app(
                    ingress_url="http://ingress", ack_client=ingress_client,
                    report_client=ingress_client, push_interval_ms=250,
                )
                # Cold-start guard — see tests/test_server2.py's own load
                # tests for why: decision.pressure()'s own b term would
                # otherwise explode to 1.0 before a single event has a
                # real chance to stream, purely from this test's own
                # back-to-back individual /ingest calls.
                server2_app.state.pulse.service_ewma.level = server2_app.state.pulse.per_worker_rate

                async with httpx.ASGITransport(app=server1_app).app.router.lifespan_context(server1_app), \
                           httpx.ASGITransport(app=server2_app).app.router.lifespan_context(server2_app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=server1_app), base_url="http://server1"
                    ) as c1, httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=server2_app), base_url="http://server2"
                    ) as c2:
                        cfg = load_config()
                        specs = {
                            EventType.PAYMENT: (cfg.tiers[EventType.PAYMENT], Tier.P0),
                            EventType.ORDER: (cfg.tiers[EventType.ORDER], Tier.P0),
                            EventType.INVENTORY: (cfg.tiers[EventType.INVENTORY], Tier.P1),
                            EventType.CLICK: (cfg.tiers[EventType.CLICK], Tier.P2),
                            EventType.LOG: (cfg.tiers[EventType.LOG], Tier.P2),
                        }
                        # tiers.yaml's own mix: 5% payment, 5% order,
                        # 10% inventory, 50% click, 30% log.
                        mix_cycle = (
                            [EventType.PAYMENT] + [EventType.ORDER]
                            + [EventType.INVENTORY] * 2
                            + [EventType.CLICK] * 10 + [EventType.LOG] * 6
                        )

                        duration_s = 8.0
                        start = time.time()
                        seq = 0
                        sent_p0 = 0
                        while time.time() - start < duration_s:
                            seq += 1
                            event_type = mix_cycle[seq % len(mix_cycle)]
                            spec, tier = specs[event_type]
                            now = time.time()
                            event = _event(f"e{seq}", seq, tier=tier, event_type=event_type, spec=spec, now=now)
                            client = c1 if tier is Tier.P0 else c2
                            await client.post("/ingest", json={"events": [event.model_dump(mode="json")]})
                            if tier is Tier.P0:
                                sent_p0 += 1
                            # Paced to the calibrated 20x-spike aggregate
                            # rate (~333 eps, tiers.yaml's own constant) —
                            # a single interleaved stream at ONE overall
                            # rate, split by tier downstream, is what a
                            # real generator would actually produce. An
                            # earlier, unpaced version of this test found,
                            # empirically, that server1 (which never
                            # sheds or defers — CLAUDE.md hard rule 3) backs
                            # up WITHOUT BOUND against an arrival rate with
                            # no ceiling at all, unlike server2 (whose
                            # DEFER/SAMPLE_ROLLUP/SHED machinery keeps its
                            # own queue bounded under real pressure) — P0
                            # demand at this pace (~108 u/s) sits under
                            # server1's own 135 u/s capacity, matching
                            # test_server1.py's own load test, while P1/P2
                            # demand at this SAME pace (~180 u/s) still
                            # massively oversubscribes server2's single-pod
                            # 15 u/s (config/servers.yaml's own ~12x
                            # comment) — real pressure on server2, real
                            # headroom on server1, in one realistic stream.
                            await asyncio.sleep(1.0 / cfg.spike_eps)

                        drain1 = (await c1.post("/drain", params={"timeout_s": 20.0})).json()
                        drain2 = (await c2.post("/drain", params={"timeout_s": 20.0})).json()
                        await asyncio.sleep(0.5)  # let the last metrics/report pushes land

                        assert sent_p0 > 0, "test setup: the mix must actually include P0 traffic"

                        # -- the equation balances: durable evidence exists
                        # for real completions from BOTH split-topology
                        # servers, not just Engine's own local pipeline.
                        assert sink.count() > 0
                        assert sink.sla_outcome_count(tier="P0") > 0, (
                            f"server1's own completions must be durably recorded — drain1={drain1}"
                        )
                        assert sink.sla_outcome_count(source="server1") > 0
                        assert ledger.decision_trace_count() > 0

                        # -- shed_critical is zero (server2's own live,
                        # continuously-checked invariant — ladder.cap()
                        # already forbids a P1 event from ever reaching
                        # SHED; this confirms it held for real, not just
                        # by code inspection).
                        server2_counters = reporting.aggregate("server2")
                        assert server2_counters.get("shed_critical", 0.0) == 0.0

                        # -- verify_chain() passes: the durable audit
                        # ledger, re-derived from its own stored columns,
                        # is internally consistent end to end.
                        verification = ledger.verify_chain()
                        assert verification.ok, verification.reason
    finally:
        connection.close()


# --------------------------------------------------------------------------
# Re-dispatch: a 'server2'-origin deferred event genuinely goes back OVER
# THE WIRE to a real server2 process once server2's own reported pressure
# drops below deferral.DRAIN_PRESSURE_THRESHOLD (0.35) — this phase's own
# instruction, verbatim.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server2_origin_deferred_event_redispatches_once_pressure_drops(tmp_path):
    db_path = tmp_path / "history.db"
    connection = history_db.open_history_db(db_path)
    history_db.wire_ambient_stores(connection)
    server2_client: httpx.AsyncClient | None = None
    try:
        # transport_mode="direct" so create_app()'s own lifespan does not
        # start ITS OWN real-socket batcher — we wire transport ourselves,
        # to ASGI-backed clients, right after the app's lifespan (and this
        # phase's own new server2-origin drain task inside it) has
        # started, matching test_transport_http.py's own established
        # pattern for exactly this kind of multi-app wiring.
        ingress_app = create_app(fake=False, transport_mode="direct", seed=11)
        async with httpx.ASGITransport(app=ingress_app).app.router.lifespan_context(ingress_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=ingress_app), base_url="http://ingress"
            ) as ingress_client:
                server2_app = create_server2_app(
                    ingress_url="http://ingress", ack_client=ingress_client,
                    report_client=ingress_client, push_interval_ms=50,
                )
                server2_app.state.pulse.service_ewma.level = server2_app.state.pulse.per_worker_rate

                async with httpx.ASGITransport(app=server2_app).app.router.lifespan_context(server2_app):
                    server2_client = httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=server2_app), base_url="http://server2"
                    )
                    transport.configure_http(
                        clients_by_server={"server2": server2_client}, ack_timeout_ms=5000,
                    )
                    await transport.start_http()

                    # Force server2 into DEFER territory (decide()'s own
                    # [0.75, 0.95) band) before the event ever arrives.
                    server2_app.state.pulse.pressure_cache = 0.8
                    server2_app.state.pulse.pressure_cache_ts = time.time() + 3600

                    cfg = load_config()
                    inv_spec = cfg.tiers[EventType.INVENTORY]
                    event = _event(
                        "inv-1", 1, tier=Tier.P1, event_type=EventType.INVENTORY,
                        spec=inv_spec, now=time.time(),
                    )
                    await transport.submit("server2", event)
                    await asyncio.sleep(0.3)

                    assert deferral.pending_count_by_origin(deferral.ORIGIN_SERVER2) == 1, (
                        "test setup: the event must actually be sitting in the "
                        "deferred buffer, tagged with its real origin, before pressure eases"
                    )
                    # /defer's own handler already cleared transport's
                    # outstanding-dispatch bookkeeping for it (this
                    # phase's own fix — see app.py's /defer docstring).
                    assert transport.dispatch_stats()["outstanding"] == 0

                    # Ease server2's own reported pressure below
                    # deferral.DRAIN_PRESSURE_THRESHOLD (0.35) and let its
                    # own ReportingClient push a fresh fragment reflecting it.
                    server2_app.state.pulse.pressure_cache = 0.1
                    server2_app.state.pulse.pressure_cache_ts = time.time() + 3600
                    await asyncio.sleep(0.3)  # a push_interval_ms=50 cycle

                    # Give the server2-origin drainer (DRAIN_TICK_SECONDS
                    # = 0.25) a few real ticks to notice and act.
                    await asyncio.sleep(1.5)

                    assert deferral.pending_count_by_origin(deferral.ORIGIN_SERVER2) == 0, (
                        "the deferred event must have been redispatched once "
                        "server2's own reported pressure dropped below 0.35"
                    )
                    metrics = (await server2_client.get("/metrics")).json()
                    assert metrics["processed"] >= 1, (
                        "the redispatched event must have actually been served "
                        "by server2 on its second pass, not merely removed from the buffer"
                    )

                    await transport.stop_http()
    finally:
        if server2_client is not None:
            await server2_client.aclose()
        connection.close()
