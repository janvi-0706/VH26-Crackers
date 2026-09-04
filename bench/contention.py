"""bench/contention.py — Phase J0: measurement, not a feature.

Owner: Lane D.

CLAUDE.md's own working style says stop and ask before building ahead of
what a prompt asks. This prompt asks for exactly one thing: measure the
specific contention a P0/P1-P2 process split (Phase J) is supposed to
eliminate, on the CURRENT single-process build, before that split
happens — so the split has a real number to justify it against, not a
hunch. Nothing in `src/` is touched. This file only imports from
`triage` and, for the run's own duration, temporarily REPLACES two
`WorkerPool` methods with instrumented wrappers that call straight
through to the real ones — a monkeypatch, restored before this script
exits, never a file edit. If this script is deleted, the pipeline it
measured is byte-for-byte what it was before it ran.

What "contention" means here, precisely, and why the definition matters:

Single-process asyncio (CLAUDE.md hard rule 1) means every worker
coroutine, whatever tier it is currently serving, shares the same one
event loop and the same one CPU core's worth of real interpreter time.
Two independent claims about the cost of that get investigated below,
each measured directly rather than inferred from a service-time formula:

1. HEAD-OF-LINE BLOCKING: when a P0 event is ready to run but the worker
   that ends up serving it is still finishing a P1/P2 MICRO_BATCH, the
   P0 event waits for that batch's own remaining service time — the
   thing an EDF/priority queue cannot prevent on its own, because the
   queue only controls what gets PICKED next, not what a worker already
   holding something lower-tier is currently doing. This is measured
   directly: every worker's own timeline of what it served and when is
   recorded, and for every P0 event, the portion of its own queue wait
   that overlaps a LOWER-tier interval on the specific worker that
   eventually serves it is attributed to head-of-line blocking. The
   portion overlapping ANOTHER P0 event on that same worker is
   attributed separately (item 2) — a real distinction: one is exactly
   what a process split removes, the other is not (splitting P0 into its
   own process does not stop two P0 events from genuinely queueing
   behind each other if P0's own load exceeds P0's own workers).

2. EVENT-LOOP SCHEDULING DELAY: separate from queueing, this measures
   how promptly the event loop itself resumes a coroutine once it is
   ready to run — the honest proxy for GIL/loop contention CLAUDE.md's
   own "single process" trade-off (ADR 0001) accepts, stated once and
   for all rather than reargued at the specific case of one P0 coroutine.
   A lightweight prober coroutine repeatedly does `await asyncio.sleep(0)`
   and measures how much longer that actually took than the ~0 it should
   — the standard technique for measuring loop lag, chosen because it
   measures the LOOP's own responsiveness under real load rather than
   any one event's own path, which is what "a proxy for," not "an exact
   substitute for," means here.

This is the "before" half of a before/after comparison Phase J's own
future prompt will need — `bench/contention-before.md` is dated evidence
for a decision, not a permanent claim about this codebase's own ceiling.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from triage import decision, deferral, ledger, metrics  # noqa: E402
from triage.app import Engine  # noqa: E402
from triage.config import load_config  # noqa: E402
from triage.contracts import Tier  # noqa: E402
from triage.metrics import percentile  # noqa: E402
from triage.worker import WorkerPool  # noqa: E402

# This phase's own spec: a 90-second run at the same 20x spike every other
# benchmark in this project is calibrated around — the contention this
# file measures has to be measured under the load Phase J is actually
# meant to survive, not at baseline where six workers are rarely all busy
# at once and head-of-line blocking would be too rare to characterise.
DURATION_SECONDS = 90.0
SPIKE_MULTIPLIER = 20.0

# The loop-lag prober probes on EVERY loop turn (`await asyncio.sleep(0)`,
# never a nonzero-duration sleep — see `_loop_lag_prober`'s own docstring
# for the real, measured reason a nonzero pacing sleep was tried and
# abandoned) but only RECORDS every Nth one, to keep memory bounded: an
# isolated 3-second measurement against the real engine produced ~774,000
# raw probes/sec, which would be tens of millions of floats over a real
# 90s run. Recording every 500th still yields tens of thousands of
# samples — plenty for real percentiles — while keeping the list a
# rounding error of memory next to everything else this run tracks.
LOOP_PROBE_SAMPLE_STRIDE = 500

# A wait this small is scheduling noise, not a real queueing event —
# below this, "did this P0 event experience any wait at all" reports No.
NOISE_FLOOR_SECONDS = 0.0005

# A wait this small is scheduling noise, not a real queueing event —
# below this, "did this P0 event experience any wait at all" reports No.
NOISE_FLOOR_SECONDS = 0.0005


# --------------------------------------------------------------------------
# Recorded data
# --------------------------------------------------------------------------


@dataclass
class Interval:
    """One worker's own record of "I was busy serving THIS from START to
    END" — `end` is set twice: once optimistically (the service time the
    real cost model computes, so a P0 event evaluated WHILE this interval
    is still running can already see it), and once for real when the
    underlying `await` actually returns, so the interval this file reports
    reflects any real scheduling delay in that sleep too, not just the
    theoretical duration."""

    worker_id: int
    tier_kind: str  # "P0" or "LOWER" (P1/P2, individually or MICRO_BATCH-mixed)
    tiers: tuple[str, ...]
    batch_size: int
    start: float
    end: float


@dataclass
class P0Observation:
    event_id: str
    ingest_ts: float
    service_start_ts: float
    worker_id: int
    total_wait_s: float
    blocked_by_lower_tier_s: float
    blocked_by_other_p0_s: float

    @property
    def unattributed_s(self) -> float:
        """Wait not explained by the specific worker's own recorded
        timeline — real scheduling noise, or (rarely) this event being
        served by a worker this instrumentation had no prior record for
        yet. Reported honestly rather than folded into either bucket."""
        return max(0.0, self.total_wait_s - self.blocked_by_lower_tier_s - self.blocked_by_other_p0_s)


@dataclass
class ContentionData:
    worker_timelines: dict[int, list[Interval]] = field(default_factory=lambda: defaultdict(list))
    p0_observations: list[P0Observation] = field(default_factory=list)
    loop_lag_samples: list[float] = field(default_factory=list)
    loop_probe_count: int = 0
    largest_block_duration_s: float = 0.0
    largest_block_tiers: tuple[str, ...] = ()
    largest_block_batch_size: int = 0
    largest_block_p0_event_id: str = ""


def _overlap_seconds(intervals: list[Interval], t0: float, t1: float, tier_kind: str) -> float:
    """Total time, within [t0, t1), that any interval of the given
    tier_kind was active. Worker timelines are internally non-overlapping
    (one coroutine per worker, strictly sequential — see this module's own
    docstring), so this is a plain sum, not an interval-merge problem."""
    total = 0.0
    for iv in intervals:
        if iv.tier_kind != tier_kind:
            continue
        lo = max(iv.start, t0)
        hi = min(iv.end, t1)
        if hi > lo:
            total += hi - lo
    return total


# --------------------------------------------------------------------------
# Instrumentation: monkeypatch WorkerPool for the run, then restore it
# --------------------------------------------------------------------------


class ContentionInstrumentation:
    """Wraps `WorkerPool.serve`/`_serve_batch` in memory, for the lifetime
    of one measurement run, then restores the originals byte-for-byte.
    Never touches `worker.py` itself — see this module's own top
    docstring for why that is the whole point of this file existing."""

    def __init__(self) -> None:
        self.data = ContentionData()
        self._orig_serve = None
        self._orig_serve_batch = None

    def install(self) -> None:
        self._orig_serve = WorkerPool.serve
        self._orig_serve_batch = WorkerPool._serve_batch
        data = self.data
        orig_serve = self._orig_serve
        orig_serve_batch = self._orig_serve_batch

        async def instrumented_serve(pool_self, event, worker_id: int = -1):
            start = time.time()
            # The exact formula worker.serve() itself uses — read here to
            # know how far ahead this interval will run, never to change
            # what actually happens (orig_serve, called below unchanged,
            # is what does the real work).
            expected_seconds = event.cost / pool_self.capacity_units_per_sec
            tier_kind = "P0" if event.tier is Tier.P0 else "LOWER"
            timeline = data.worker_timelines[worker_id]

            if event.tier is Tier.P0:
                t0 = event.ingest_ts
                prior = [iv for iv in timeline if iv.start < start and iv.end > t0]
                blocked_lower = _overlap_seconds(prior, t0, start, "LOWER")
                blocked_p0 = _overlap_seconds(prior, t0, start, "P0")
                data.p0_observations.append(
                    P0Observation(
                        event_id=event.event_id, ingest_ts=t0, service_start_ts=start,
                        worker_id=worker_id, total_wait_s=max(0.0, start - t0),
                        blocked_by_lower_tier_s=blocked_lower, blocked_by_other_p0_s=blocked_p0,
                    )
                )
                for iv in prior:
                    if iv.tier_kind != "LOWER":
                        continue
                    dur = min(iv.end, start) - max(iv.start, t0)
                    if dur > data.largest_block_duration_s:
                        data.largest_block_duration_s = dur
                        data.largest_block_tiers = iv.tiers
                        data.largest_block_batch_size = iv.batch_size
                        data.largest_block_p0_event_id = event.event_id

            interval = Interval(
                worker_id=worker_id, tier_kind=tier_kind, tiers=(event.tier.value,),
                batch_size=1, start=start, end=start + expected_seconds,
            )
            timeline.append(interval)
            try:
                return await orig_serve(pool_self, event, worker_id)
            finally:
                interval.end = time.time()

        async def instrumented_serve_batch(pool_self, batch, worker_id: int = -1):
            start = time.time()
            expected_seconds = decision.batch_cost([e.cost for e in batch]) / pool_self.capacity_units_per_sec
            tiers_present = tuple(sorted({e.tier.value for e in batch}))
            interval = Interval(
                worker_id=worker_id, tier_kind="LOWER", tiers=tiers_present,
                batch_size=len(batch), start=start, end=start + expected_seconds,
            )
            data.worker_timelines[worker_id].append(interval)
            try:
                return await orig_serve_batch(pool_self, batch, worker_id)
            finally:
                interval.end = time.time()

        WorkerPool.serve = instrumented_serve
        WorkerPool._serve_batch = instrumented_serve_batch

    def uninstall(self) -> None:
        """Restores the exact, unmodified originals. Idempotent-safe to
        call even if install() was never called (no-op)."""
        if self._orig_serve is not None:
            WorkerPool.serve = self._orig_serve
            self._orig_serve = None
        if self._orig_serve_batch is not None:
            WorkerPool._serve_batch = self._orig_serve_batch
            self._orig_serve_batch = None


async def _loop_lag_prober(data: ContentionData, stop: asyncio.Event) -> None:
    """The standard technique for measuring event-loop responsiveness:
    `asyncio.sleep(0)` yields to the loop and resumes on the loop's very
    next turn — under no contention that round trip is a few
    microseconds; how much LONGER it actually takes under real load is
    the loop's own scheduling delay, a proxy for GIL/loop contention
    (this module's own top docstring), not a per-event measurement.
    `time.perf_counter()` is the correct clock for this — monotonic, not
    subject to wall-clock adjustment.

    Deliberately probes with `sleep(0)` ONLY, never a nonzero-duration
    pacing sleep between measurements, after two real, measured problems
    with that approach: `asyncio.wait_for(stop.wait(), timeout=...)`
    turned out not to behave like a timer at all on this platform/asyncio
    combination (an isolated 2-second test produced ~118,000 samples,
    ~17us apart, instead of the ~400 a 5ms interval implies), and even
    after switching to a plain `asyncio.sleep(interval)`, a majority of
    the gaps BETWEEN consecutive wake-ups measured under the real engine's
    own load — never reproduced against a synthetic six-task
    `asyncio.sleep()`-only substitute — came back shorter than the
    requested interval, which a correct `asyncio.sleep()` should never
    allow. Both symptoms vanished once pacing was removed entirely and
    `sleep(0)`'s own round-trip became the only thing measured — isolated
    directly: a 3-second probe against the real engine this way produced
    ~774,000 samples with a sane, all-non-negative distribution (p50 ~4us,
    max ~1.6ms), the same call that misbehaved paced at a 5ms interval.
    Root cause not fully pinned down (asyncio.sleep with a nonzero
    duration behaving unreliably specifically under this engine's own
    real load, on this platform) — named honestly rather than silently
    worked around by trusting numbers a now-abandoned method produced.

    That much raw probing needs its own throttle so the sample list stays
    bounded over a real 90s run — see `LOOP_PROBE_SAMPLE_STRIDE`'s own
    docstring: still probes (yields) every turn, only RECORDS every Nth,
    so the loop gets exactly as many chances to reveal contention as
    before, just without keeping tens of millions of floats for it."""
    counter = 0
    while not stop.is_set():
        t0 = time.perf_counter()
        await asyncio.sleep(0)
        counter += 1
        if counter % LOOP_PROBE_SAMPLE_STRIDE == 0:
            data.loop_lag_samples.append(time.perf_counter() - t0)
    data.loop_probe_count = counter


# --------------------------------------------------------------------------
# The run
# --------------------------------------------------------------------------


def _full_reset() -> None:
    """A true zero — same reasoning as bench/run.py's own full_reset()."""
    metrics.reset()
    metrics.reset_critical_failures()
    ledger.reset()
    deferral.reset_default_store()


def _p0_loss_count() -> int:
    row = ledger._default_ledger.connection.execute(
        "SELECT COUNT(*) FROM audit_ledger WHERE tier = 'P0' "
        "AND decision IN ('SAMPLE_ROLLUP', 'SHED')"
    ).fetchone()
    return int(row[0])


async def run_measurement(duration_s: float = DURATION_SECONDS, *, seed: int = 4242) -> ContentionData:
    _full_reset()
    config = load_config()
    rate_eps = config.baseline_eps * SPIKE_MULTIPLIER

    instrumentation = ContentionInstrumentation()
    instrumentation.install()
    try:
        engine = Engine(config=config, seed=seed)
        engine.set_mode("adaptive")
        engine.set_rate(rate_eps)

        stop_probe = asyncio.Event()
        probe_task = asyncio.create_task(_loop_lag_prober(instrumentation.data, stop_probe))

        await engine.start()
        print(f"[contention] running adaptive/spike for {duration_s:.0f}s...", flush=True)
        await asyncio.sleep(duration_s)
        await engine.stop()

        stop_probe.set()
        await probe_task

        # Sanity check on the instrumentation itself, not a new claim:
        # wrapping serve()/_serve_batch() must not be the thing that
        # breaks CLAUDE.md hard rule 3. If this ever fires, the bug is in
        # THIS file, not in worker.py.
        loss = _p0_loss_count()
        if loss:
            print(
                f"[contention] WARNING: {loss} P0 loss events recorded during "
                "an INSTRUMENTED run — investigate this file before trusting "
                "the numbers below.",
                flush=True,
            )
        frame = metrics.snapshot()
        print(
            f"[contention]   ingested={frame.ingested} processed={frame.processed} "
            f"P0 events observed={len(instrumentation.data.p0_observations)}",
            flush=True,
        )
    finally:
        instrumentation.uninstall()

    return instrumentation.data


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def _pctl(samples: list[float], q: float) -> float:
    return percentile(samples, q) if samples else 0.0


def _fmt_ms(seconds: float) -> str:
    ms = seconds * 1000.0
    if ms >= 1000:
        return f"{ms / 1000:.2f}s"
    return f"{ms:.2f}ms"


def _fmt_us_or_ms(seconds: float) -> str:
    """Loop lag lives in the microseconds-to-low-milliseconds range —
    `_fmt_ms`'s own 2-decimal-place millisecond formatting would print
    "0.00ms" for most of it, which is a real loss of the exact resolution
    this measurement exists to show."""
    if seconds >= 0.001:
        return _fmt_ms(seconds)
    return f"{seconds * 1_000_000:.1f}us"


def render_markdown(data: ContentionData, duration_s: float = DURATION_SECONDS) -> str:
    lines: list[str] = []
    lines.append("# PULSE contention report — before Phase J (single process)")
    lines.append("")
    lines.append(
        f"Phase J0: measurement only. `bench/contention.py`, {duration_s:.0f}s at "
        f"{SPIKE_MULTIPLIER:.0f}x spike, adaptive mode, driven directly against `Engine` "
        "the same way `bench/run.py` is — no HTTP involved, `src/` untouched. This is the "
        "\"before\" evidence for the P0/P1-P2 process split Phase J proposes."
    )
    lines.append("")

    obs = data.p0_observations
    n = len(obs)
    lines.append(f"**{n} P0 events observed.**")
    lines.append("")

    # -- 1. Head-of-line blocking ------------------------------------------------
    lines.append("## 1. P0 head-of-line blocking (waited for a worker busy with P1/P2)")
    lines.append("")
    lines.append(
        "For each P0 event, the portion of its own queue wait that overlapped a "
        "LOWER-tier (P1/P2, individual or MICRO_BATCH) interval on the specific worker "
        "that ended up serving it — the exact cost a process split removes."
    )
    lines.append("")
    hol = [o.blocked_by_lower_tier_s for o in obs]
    experienced = sum(1 for s in hol if s > NOISE_FLOOR_SECONDS)
    pct_experienced = (experienced / n * 100.0) if n else 0.0
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| p50 | {_fmt_ms(_pctl(hol, 0.50))} |")
    lines.append(f"| p95 | {_fmt_ms(_pctl(hol, 0.95))} |")
    lines.append(f"| p99 | {_fmt_ms(_pctl(hol, 0.99))} |")
    lines.append(f"| max | {_fmt_ms(max(hol) if hol else 0.0)} |")
    lines.append(f"| P0 events with ANY such wait | {experienced} / {n} ({pct_experienced:.1f}%) |")
    lines.append("")

    # -- 2. Queue wait decomposed --------------------------------------------------
    lines.append("## 2. P0 queue wait, decomposed")
    lines.append("")
    lines.append(
        "Total P0 queue wait, split into: waited behind another P0 event on the same "
        "worker; waited behind P1/P2 work on the same worker (row 1's own numbers, "
        "repeated here for direct comparison); and unattributed (real scheduling noise, "
        "or a worker this run had no prior record for yet)."
    )
    lines.append("")
    total_wait = [o.total_wait_s for o in obs]
    behind_p0 = [o.blocked_by_other_p0_s for o in obs]
    unattributed = [o.unattributed_s for o in obs]
    lines.append("| Component | p50 | p95 | p99 | max |")
    lines.append("|---|---|---|---|---|")
    for name, series in (
        ("Total queue wait", total_wait),
        ("...behind other P0", behind_p0),
        ("...behind P1/P2 (head-of-line)", hol),
        ("...unattributed", unattributed),
    ):
        lines.append(
            f"| {name} | {_fmt_ms(_pctl(series, 0.50))} | {_fmt_ms(_pctl(series, 0.95))} "
            f"| {_fmt_ms(_pctl(series, 0.99))} | {_fmt_ms(max(series) if series else 0.0)} |"
        )
    lines.append("")

    # -- 3. Largest single blocking event ------------------------------------------
    lines.append("## 3. Largest single blocking event observed")
    lines.append("")
    if data.largest_block_duration_s > 0:
        lines.append(
            f"**{_fmt_ms(data.largest_block_duration_s)}** — P0 event "
            f"`{data.largest_block_p0_event_id}` waited behind a "
            f"{'batch of ' + str(data.largest_block_batch_size) if data.largest_block_batch_size > 1 else 'single event'} "
            f"on tier(s) `{', '.join(data.largest_block_tiers)}` "
            f"(batch_size={data.largest_block_batch_size})."
        )
    else:
        lines.append("No P0 event ever overlapped a P1/P2 interval — no blocking observed.")
    lines.append("")

    # -- 4. Event-loop scheduling delay --------------------------------------------
    lines.append("## 4. Event-loop scheduling delay (proxy for GIL/loop contention)")
    lines.append("")
    lines.append(
        "How much longer `await asyncio.sleep(0)` — yield to the loop, resume on its next "
        "turn — actually took than the microseconds it should, sampled continuously "
        "throughout the run (every loop turn probed; only 1-in-"
        f"{LOOP_PROBE_SAMPLE_STRIDE} recorded, to keep the sample list bounded — see "
        "`LOOP_PROBE_SAMPLE_STRIDE`'s own docstring). A property of the loop itself under "
        "real load, not of any one P0 event's own path — see `_loop_lag_prober`'s own "
        "docstring for two real, measured problems with pacing this any other way, and why "
        "probing every turn but recording only a stride of them is what survived."
    )
    lines.append("")
    lag = data.loop_lag_samples
    lines.append(f"**{len(lag)} recorded samples**, out of {data.loop_probe_count} loop turns probed.")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| p50 | {_fmt_us_or_ms(_pctl(lag, 0.50))} |")
    lines.append(f"| p95 | {_fmt_us_or_ms(_pctl(lag, 0.95))} |")
    lines.append(f"| p99 | {_fmt_us_or_ms(_pctl(lag, 0.99))} |")
    lines.append(f"| max | {_fmt_us_or_ms(max(lag) if lag else 0.0)} |")
    lines.append("")

    lines.append("## What this does and does not show")
    lines.append("")
    lines.append(
        "This measures the CURRENT single-process build under exactly the load Phase J "
        "is meant to survive better. It does not simulate the split itself — a real "
        "two-process build could still have its own new costs (IPC, serialization, a "
        "second audit-ledger-consistency problem) this report says nothing about. It is "
        "evidence for whether the specific contention Phase J targets is real and "
        "large enough to be worth that cost, not a promise of what Phase J will achieve."
    )
    lines.append("")
    lines.append(
        "Section 4's own prober is itself an observer effect worth naming: it yields to the "
        "loop on every single turn for the whole run (hundreds of thousands of times per "
        "second, per the isolated test in `_loop_lag_prober`'s own docstring), which is "
        "itself additional loop activity, not a free window into it. Its own per-call cost "
        "is small, but at that frequency it is a real, if likely minor, contributor to "
        "whatever contention section 4 reports — not purely a passive measurement."
    )
    lines.append("")

    return "\n".join(lines)


async def main() -> int:
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else DURATION_SECONDS
    data = await run_measurement(duration_s=duration)
    md = render_markdown(data, duration_s=duration)
    out_path = REPO_ROOT / "bench" / "contention-before.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"\nwrote {out_path.relative_to(REPO_ROOT)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
