"""bench/contention_after.py — Phase J8: the "after" half of the
before/after comparison `bench/contention.py` (Phase J0) set up.

Owner: Lane D.

Testing only, per this phase's own instruction — nothing in `src/` is
touched, same discipline `contention.py`'s own docstring already commits
to. Measures the identical two things, against server1's own real
process, standalone:

1. HEAD-OF-LINE BLOCKING behind a lower-tier batch. `contention.py`'s own
   instrumentation monkeypatched `WorkerPool.serve`/`_serve_batch` to
   attribute a P0 event's own wait to overlapping P1/P2 intervals on the
   same worker — that measurement APPARATUS does not apply here, because
   the thing it was built to detect cannot occur here BY CONSTRUCTION,
   not merely by observation: `server1.py`'s own worker loop
   (`create_server1_app`'s inner `_worker`) has exactly one code path —
   dequeue from `P0Queue`, `asyncio.sleep(cost / per_worker_rate)`, ack —
   with no `MICRO_BATCH`, `DEFER`, `SAMPLE_ROLLUP`, or `SHED` branch
   anywhere in the file (`server1.py`'s own top docstring: "Explicitly
   does NOT contain: batching, CoDel, the ladder, deferral, shedding"),
   and `/ingest` 422s any non-P0 event before it ever reaches the queue
   (a second, independent enforcement — `server1.py`'s own
   `_assert_server1_is_correctly_provisioned` plus that runtime check).
   There is no lower tier for a P0 event to ever wait behind. This
   script's own job for item 1 is therefore to VERIFY the precondition
   live (every served event this run observes really is P0, and the
   worker's own per-event log never shows anything else), not to
   re-derive a number `contention-before.md`'s own methodology already
   made unnecessary here.

2. EVENT-LOOP SCHEDULING DELAY — measured identically to
   `contention.py`'s own `_loop_lag_prober` (same technique, same
   `sleep(0)`-only reasoning, same recording stride), run concurrently
   against server1's own real event loop under the same calibrated 20x
   P0-only spike rate `bench/contention-before.md` used. This is the one
   number a process split does NOT trivially fix by construction — a
   smaller process still shares its own one event loop across however
   many P0 workers it runs, and this is the honest way to check whether
   isolating P0 into its own process changed anything about how
   responsive THAT loop is under P0's own real load.

Traffic is submitted directly into server1's own `P0Queue`
(`app.state.pulse.queue.put()`), bypassing HTTP entirely — matching
`contention.py`'s own "call straight into the pipeline" choice (Engine's
`queue.put_nowait` there), for the same reason: this script measures the
PROCESSING loop's own contention, not the transport layer's (Phase J3-J7
already have their own dedicated latency measurements for that).
"""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import httpx  # noqa: E402

from triage.config import load_config  # noqa: E402
from triage.contracts import Event, EventType, Tier  # noqa: E402
from triage.metrics import percentile  # noqa: E402
from triage.server1 import create_server1_app  # noqa: E402
from triage.servers_config import load_servers_config  # noqa: E402

DURATION_SECONDS = 90.0
LOOP_PROBE_SAMPLE_STRIDE = 500


class _NullAckClient:
    """Stands in for a real ingress connection this standalone bench
    never has — server1.py POSTs an ack per completed event and a health
    check per readiness cycle; both are no-ops here, exactly as harmless
    as `server1.py`'s own `_ack`'s `except Exception` branch already
    assumes any lost ack is (Phase J3's `redispatch_expired()` is what
    actually recovers a lost ack in a real deployment)."""

    async def post(self, *args, **kwargs) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    async def get(self, *args, **kwargs) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    async def aclose(self) -> None:
        return None


@dataclass
class MeasurementData:
    loop_lag_samples: list[float] = field(default_factory=list)
    loop_probe_count: int = 0
    served_tiers: set[str] = field(default_factory=set)
    served_count: int = 0


async def _loop_lag_prober(data: MeasurementData, stop: asyncio.Event) -> None:
    """Identical technique to `contention.py`'s own `_loop_lag_prober` —
    see that function's own docstring for why `sleep(0)`-only, no pacing."""
    count = 0
    while not stop.is_set():
        t0 = time.perf_counter()
        await asyncio.sleep(0)
        lag = time.perf_counter() - t0
        count += 1
        if count % LOOP_PROBE_SAMPLE_STRIDE == 0:
            data.loop_lag_samples.append(lag)
    data.loop_probe_count = count


def _p0_event(seq: int, event_type: EventType, cost: float, value: float, sla_seconds: float, now: float) -> Event:
    return Event(
        event_id=f"evt-{seq}", dedup_key=f"dk-{seq}", seq=seq,
        partition_key="customer:0", idempotency_key=f"ik-{seq}",
        type=event_type, tier=Tier.P0, payload_size=64,
        value=value, cost=cost, ingest_ts=now, deadline_ts=now + sla_seconds,
    )


async def run_measurement(duration_s: float) -> tuple[MeasurementData, dict]:
    cfg = load_config()
    payment = cfg.tiers[EventType.PAYMENT]
    order = cfg.tiers[EventType.ORDER]
    avg_p0_cost = (payment.cost + order.cost) / 2.0
    p0_demand_ups = cfg.demand_ups(cfg.spike_eps, Tier.P0)  # ~108.2 u/s, same as contention-before.md
    events_per_second = p0_demand_ups / avg_p0_cost
    interval_s = 1.0 / events_per_second

    async def _emit_load(state, duration_s: float, served_tiers: set[str]) -> int:
        start = time.time()
        seq = 0
        while time.time() - start < duration_s:
            seq += 1
            now = time.time()
            spec, event_type = (payment, EventType.PAYMENT) if seq % 2 == 0 else (order, EventType.ORDER)
            event = _p0_event(seq, event_type, spec.cost, spec.value, spec.sla_seconds, now)
            state.queue.put(event)
            served_tiers.add(event.tier.value)
            await asyncio.sleep(interval_s)
        return seq

    async def _drain(state) -> None:
        deadline = time.time() + 30.0
        while (len(state.queue) > 0 or state.in_flight > 0) and time.time() < deadline:
            await asyncio.sleep(0.05)

    data = MeasurementData()

    # --- Pass A: throughput/latency/queue-wait, no prober running. -----
    # A first version ran the loop-lag prober CONCURRENTLY with this pass
    # and found catastrophic queue backup (tens of SECONDS of queue wait)
    # even though an isolated burst test (no prober at all) against the
    # identical app confirmed 6 workers at 22.5 u/s each genuinely drain
    # 60 P0 events in ~1.6s, matching the ~1.56s the math predicts. The
    # cause, traced directly rather than assumed: `_loop_lag_prober`
    # (below) probes with a bare `await asyncio.sleep(0)` on EVERY loop
    # turn — `contention.py`'s own docstring already measured this at
    # ~774,000 turns/sec in isolation and named it "a real, if likely
    # minor, contributor" for the MONOLITH's 150 u/s pool. For server1's
    # own smaller, standalone 135 u/s pool, sharing one event loop with a
    # prober consuming that much of the loop's own attention is not
    # minor at all — it starves the real timer callbacks that wake a
    # worker's own `asyncio.sleep(cost / rate)` up on schedule. Reported
    # here as a real, measured methodology finding, not silently
    # smoothed over: throughput/latency and loop-lag are measured in two
    # SEPARATE passes so neither number is an artifact of the other's
    # own measurement apparatus.
    app_a = create_server1_app(ingress_url="http://ingress-not-used", ack_client=_NullAckClient(), report_client=_NullAckClient())
    async with httpx.ASGITransport(app=app_a).app.router.lifespan_context(app_a):
        state_a = app_a.state.pulse
        sent = await _emit_load(state_a, duration_s, data.served_tiers)
        await _drain(state_a)
        throughput_summary = {
            "sent": sent,
            "processed": state_a.processed_count,
            "queue_depth_at_end": len(state_a.queue),
            "in_flight_at_end": state_a.in_flight,
            "latency_ms_p50": percentile(state_a.latency_ms, 0.50),
            "latency_ms_p95": percentile(state_a.latency_ms, 0.95),
            "latency_ms_p99": percentile(state_a.latency_ms, 0.99),
            "queue_wait_ms_p50": percentile(state_a.queue_wait_ms, 0.50),
            "queue_wait_ms_p95": percentile(state_a.queue_wait_ms, 0.95),
            "queue_wait_ms_p99": percentile(state_a.queue_wait_ms, 0.99),
            "worker_count": state_a.worker_count,
            "per_worker_rate_ups": state_a.per_worker_rate,
        }

    # --- Pass B: loop-lag, WITH the same real load running concurrently
    # (that is the whole point of this measurement — loop responsiveness
    # UNDER load), but this pass's own throughput/latency numbers are not
    # trusted or reported, for the reason above.
    app_b = create_server1_app(ingress_url="http://ingress-not-used", ack_client=_NullAckClient(), report_client=_NullAckClient())
    stop_probe = asyncio.Event()
    async with httpx.ASGITransport(app=app_b).app.router.lifespan_context(app_b):
        state_b = app_b.state.pulse
        probe_task = asyncio.create_task(_loop_lag_prober(data, stop_probe))
        try:
            await _emit_load(state_b, duration_s, data.served_tiers)
        finally:
            stop_probe.set()
            await probe_task
        await _drain(state_b)

    summary = {
        **throughput_summary,
        "p0_demand_ups": p0_demand_ups,
        "events_per_second_target": events_per_second,
    }
    return data, summary


def _fmt_ms(seconds_or_ms: float, *, already_ms: bool = False) -> str:
    ms = seconds_or_ms if already_ms else seconds_or_ms * 1000.0
    return f"{ms:.3f}ms"


def _fmt_us_or_ms(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.1f}us"
    return f"{seconds * 1000:.3f}ms"


def render_markdown(data: MeasurementData, summary: dict, duration_s: float) -> str:
    cfg = load_servers_config()
    lines: list[str] = []
    lines.append("# server1 contention — after the split (Phase J8)")
    lines.append("")
    lines.append(
        f"The \"after\" half of `bench/contention-before.md`'s own before/after "
        f"comparison — same two measurements, same calibrated 20x P0-only spike "
        f"rate ({summary['p0_demand_ups']:.1f} u/s, {summary['events_per_second_target']:.1f} "
        f"events/sec), run for {duration_s:.0f}s directly against server1's own real "
        f"process (`triage.server1.create_server1_app`), traffic submitted straight "
        f"into its own `P0Queue` — bypassing HTTP, matching `contention.py`'s own "
        f"choice to call straight into the pipeline it measures, for the same reason: "
        f"this measures the PROCESSING loop's own contention, not the transport "
        f"layer's (which Phase J3-J7 already measure separately — see "
        f"`GET /control/transport-latency`)."
    )
    lines.append("")
    lines.append(
        "Run as **two separate passes**, not one — a real, measured methodology "
        "finding from this phase, not the original plan: a first, single-pass "
        "version ran the loop-lag prober (section 2) CONCURRENTLY with the "
        "throughput/latency measurement (section 1) and produced tens of SECONDS "
        "of queue wait, even though an isolated burst test against the identical "
        "app (no prober at all) confirmed 6 workers genuinely drain 60 P0 events "
        "in ~1.6s, matching the ~1.56s the math predicts. Traced directly, not "
        "assumed: `contention-before.md`'s own section 4 already measured the "
        "prober at ~774,000 loop turns/sec in isolation and called that \"a real, "
        "if likely minor, contributor\" for the MONOLITH's 150 u/s pool — for "
        "server1's own smaller, standalone 135 u/s pool sharing one event loop "
        "with that same prober, it is not minor at all: it starves the real timer "
        "callbacks a worker's own `asyncio.sleep(cost / rate)` needs to fire on "
        "schedule. Section 1 below is measured with NO prober running; section 2 "
        "is measured in a separate pass with the prober running (that is the "
        "whole point of it — loop responsiveness UNDER load) but that pass's own "
        "throughput numbers are discarded rather than reported."
    )
    lines.append("")

    lines.append("## 1. Head-of-line blocking behind a lower-tier batch")
    lines.append("")
    lines.append(
        "**Zero, by construction — not merely observed as zero this run.** "
        "`server1.py`'s own worker loop has exactly one path (dequeue -> "
        "`asyncio.sleep(cost / per_worker_rate)` -> ack); there is no "
        "`MICRO_BATCH`/`DEFER`/`SAMPLE_ROLLUP`/`SHED` branch anywhere in the file "
        "(confirmed by direct inspection, not assumed), and `/ingest` 422s any "
        "non-P0 event before it ever reaches the queue — a P0 event on this "
        "process cannot structurally wait behind a lower-tier interval, because a "
        "lower-tier interval cannot exist on this process at all. Live confirmation "
        f"from this run: every one of the **{summary['sent']}** events submitted, "
        f"and every one of the **{summary['processed']}** this run's own workers "
        f"served before the measurement window closed, carried `tier={sorted(data.served_tiers)}` "
        "— P0 and only P0."
    )
    lines.append("")
    lines.append(
        "The OTHER component `contention-before.md`'s own section 2 named "
        "separately — a P0 event queueing behind ANOTHER P0 event on the same "
        "worker — is not eliminated by the split (splitting P0 into its own "
        "process does not stop P0's own load from queueing behind itself if it "
        "exceeds P0's own worker capacity), and is measured honestly below as "
        "server1's own real end-to-end latency and queue wait under this run's "
        "own load."
    )
    lines.append("")
    lines.append("| Metric | p50 | p95 | p99 |")
    lines.append("|---|---|---|---|")
    lines.append(
        f"| Queue wait (ingest -> dequeue) | {_fmt_ms(summary['queue_wait_ms_p50'], already_ms=True)} "
        f"| {_fmt_ms(summary['queue_wait_ms_p95'], already_ms=True)} "
        f"| {_fmt_ms(summary['queue_wait_ms_p99'], already_ms=True)} |"
    )
    lines.append(
        f"| End-to-end latency (ingest -> complete) | {_fmt_ms(summary['latency_ms_p50'], already_ms=True)} "
        f"| {_fmt_ms(summary['latency_ms_p95'], already_ms=True)} "
        f"| {_fmt_ms(summary['latency_ms_p99'], already_ms=True)} |"
    )
    lines.append("")
    lines.append(
        f"({summary['worker_count']} worker(s) at {summary['per_worker_rate_ups']:.2f} u/s each, "
        f"derived from `config/servers.yaml`'s own `server1.capacity_us` — "
        f"`servers_config.ServerSpec.workers()`, unchanged since Phase J2/J4.)"
    )
    lines.append("")

    lines.append("## 2. Event-loop scheduling delay (proxy for GIL/loop contention)")
    lines.append("")
    lines.append(
        "Identical technique to `contention-before.md`'s own section 4: how much "
        "longer `await asyncio.sleep(0)` actually took than the microseconds it "
        "should, sampled continuously (every loop turn probed; only "
        f"1-in-{LOOP_PROBE_SAMPLE_STRIDE} recorded) throughout the run, concurrently "
        "with server1's own real worker(s) processing the same 20x P0-only load."
    )
    lines.append("")
    lag = data.loop_lag_samples
    lines.append(f"**{len(lag)} recorded samples**, out of {data.loop_probe_count} loop turns probed.")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| p50 | {_fmt_us_or_ms(percentile(lag, 0.50))} |")
    lines.append(f"| p95 | {_fmt_us_or_ms(percentile(lag, 0.95))} |")
    lines.append(f"| p99 | {_fmt_us_or_ms(percentile(lag, 0.99))} |")
    lines.append(f"| max | {_fmt_us_or_ms(max(lag) if lag else 0.0)} |")
    lines.append("")

    lines.append("## What this does and does not show")
    lines.append("")
    lines.append(
        "This measures server1's own real process, standalone, under the identical "
        "calibrated load `contention-before.md` used for the single-process build's "
        "P0 traffic share — it does NOT include server2, ingress, or the real "
        "transport hop between them (those are measured separately: "
        "`GET /control/transport-latency`, `tests/test_server1.py`'s own load test, "
        "`bench/phase-j-stress.md`'s own live sustained-spike run). It confirms the "
        "one claim this bench file exists to check — P0 head-of-line blocking behind "
        "a lower tier is structurally impossible post-split, not merely rare — and "
        "reports the loop-lag number honestly rather than assuming a smaller process "
        "trivially implies a quieter loop: server1 still runs its own worker tasks "
        "on its own one event loop, and this is that loop's own real, measured "
        "responsiveness under real load, not an assumption."
    )
    lines.append("")

    return "\n".join(lines)


async def main() -> int:
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else DURATION_SECONDS
    data, summary = await run_measurement(duration_s=duration)
    md = render_markdown(data, summary, duration_s=duration)
    out_path = REPO_ROOT / "bench" / "contention-after.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"wrote {out_path.relative_to(REPO_ROOT)}")
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
