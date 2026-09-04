"""Plausible MetricsFrames at 4 Hz, with no engine behind them.

Owner: Lane D.

This exists so Lane C can build the entire dashboard against the real frame
schema starting at hour one, before queue.py has a line in it. It is a shape
generator, not a model of the pipeline: the numbers are invented, but they are
invented in the right relationships, so every panel has something honest to
render and a spike looks like a spike.

What it gets right on purpose:

  * the counters conserve — ingested == processed + in_queue + in_flight
    + deferred_pending + sampled_out + shed, exactly, every frame. If the
    dashboard's conservation panel is red, the panel is wrong, not this.
  * P0 is never shed, never deferred, never sampled. Same hard rule as the
    real engine (CLAUDE.md rule 3), because a dashboard built against fake
    data that violates it will have no way to display the guarantee.
  * pressure leads the queue, and the ladder follows pressure, so the
    "it acted before it broke" story is visible in the fake data.
  * latency separates by tier under load: P0 flat, P2 degrading. That
    divergence is the whole demo.

Run it:  python -m triage.fake_metrics            (JSON lines, 4 Hz, forever)
         python -m triage.fake_metrics --seconds 5 --pretty
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
from typing import AsyncIterator

from .config import Config, load_config
from .contracts import (
    TIER_KEYS,
    Decision,
    DecisionTrace,
    EventType,
    MetricsFrame,
    Mode,
    ShedRecord,
    Tier,
    per_tier_int,
)

# The demo cycle the fake data walks through, in seconds.
CALM_SECONDS = 12.0
SPIKE_SECONDS = 16.0
CYCLE_SECONDS = CALM_SECONDS + SPIKE_SECONDS + 12.0  # calm, spike, recovery

RECENT = 50

_REASONS = {
    Decision.STREAM_NOW: "headroom available",
    Decision.MICRO_BATCH: "batching amortises per-event overhead",
    Decision.DEFER: "deadline is distant, park until pressure falls",
    Decision.SAMPLE_ROLLUP: "near-duplicate, represented by a rollup",
    Decision.SHED: "value/cost below the shed line at this pressure",
}


class FakeSource:
    """Random-walk state machine producing one frame per tick."""

    def __init__(self, cfg: Config | None = None, seed: int | None = None,
                 hz: float = 4.0) -> None:
        self.cfg = cfg or load_config()
        self.rng = random.Random(seed)
        self.hz = hz
        self.dt = 1.0 / hz
        self.started = time.time()

        self.pressure = 0.05
        self.spike = 1.0
        self.ladder = per_tier_int()

        # conserved counters
        self.ingested = 0
        self.processed = 0
        self.in_flight = 0
        self.deferred_pending = 0
        self.sampled_out = 0
        self.shed = 0
        self.queue: dict[str, int] = per_tier_int()
        # What the workers are physically holding this instant. A separate
        # bucket, not a view of the queue, so the conservation equation the
        # dashboard checks is the same one the real engine will satisfy.
        self.in_flight_by_tier: dict[str, int] = per_tier_int()

        self.true_clicks = 0
        self.sampled_clicks = 0
        self.value_delivered = 0.0
        self.value_shed = 0.0
        self.cost_adaptive = 0.0
        self.cost_naive = 0.0
        self.completed_recent: list[float] = []

        self.recent_decisions: list[DecisionTrace] = []
        self.recent_sheds: list[ShedRecord] = []
        self.seq = 0

        self._types = list(self.cfg.mix.keys())
        self._weights = [self.cfg.mix[t] for t in self._types]

    # -- helpers ---------------------------------------------------------

    def _tier_of(self, t: EventType) -> Tier:
        return self.cfg.tiers[t].tier

    def _in_queue(self) -> int:
        return sum(self.queue.values())

    def _phase(self, elapsed: float) -> float:
        """Spike multiplier for this instant. Ramps rather than steps, so the
        forecast/pressure line has something to lead."""
        pos = elapsed % CYCLE_SECONDS
        if pos < CALM_SECONDS:
            return 1.0
        if pos < CALM_SECONDS + 1.5:  # ramp up over 1.5s
            frac = (pos - CALM_SECONDS) / 1.5
            return 1.0 + frac * (self.cfg.spike_multiplier - 1.0)
        if pos < CALM_SECONDS + SPIKE_SECONDS:
            return self.cfg.spike_multiplier
        if pos < CALM_SECONDS + SPIKE_SECONDS + 2.0:  # ramp down over 2s
            frac = (pos - CALM_SECONDS - SPIKE_SECONDS) / 2.0
            return self.cfg.spike_multiplier - frac * (self.cfg.spike_multiplier - 1.0)
        return 1.0

    def _rung(self, pressure: float, tier: Tier) -> int:
        """Degradation rung 0..3. P0 never leaves 0 — that is the guarantee."""
        if tier is Tier.P0:
            return 0
        thresholds = (0.75, 1.0, 1.25) if tier is Tier.P2 else (0.95, 1.2, 1.45)
        return sum(1 for th in thresholds if pressure >= th)

    def _note(self, seq: int, etype: EventType, decision: Decision,
              reason: str, ts: float) -> None:
        spec = self.cfg.tiers[etype]
        trace = DecisionTrace(
            seq=seq, event_id=f"evt-{seq:08d}", type=etype, tier=spec.tier,
            decision=decision, reason=reason, pressure=round(self.pressure, 3),
            value=spec.value, ts=ts,
        )
        self.recent_decisions.insert(0, trace)
        del self.recent_decisions[RECENT:]
        if decision is Decision.SHED:
            self.recent_sheds.insert(0, ShedRecord(
                seq=seq, event_id=trace.event_id, type=etype, tier=spec.tier,
                reason=reason, pressure=trace.pressure, value=spec.value, ts=ts,
            ))
            del self.recent_sheds[RECENT:]

    # -- the tick --------------------------------------------------------

    def tick(self, now: float | None = None) -> MetricsFrame:
        now = time.time() if now is None else now
        elapsed = now - self.started
        cfg = self.cfg

        self.spike = self._phase(elapsed)
        offered_rate = cfg.baseline_eps * self.spike * self.rng.uniform(0.92, 1.08)

        # Pressure is offered work over capacity, smoothed. The engine will
        # compute this from real signals; the shape is what matters here.
        demand_ups = offered_rate * cfg.weighted_cost_per_event()
        backlog_push = min(0.45, self._in_queue() / 900.0)
        target = demand_ups / cfg.total_capacity_ups + backlog_push
        self.pressure += (target - self.pressure) * 0.35
        self.pressure = max(0.0, self.pressure + self.rng.gauss(0, 0.012))

        for key in TIER_KEYS:
            self.ladder[key] = self._rung(self.pressure, Tier(key))

        # --- arrivals, split by the mix -------------------------------------
        arrivals = int(offered_rate * self.dt + self.rng.random())
        admitted_units = 0.0
        naive_units = 0.0
        for etype in self.rng.choices(self._types, self._weights, k=arrivals):
            spec = cfg.tiers[etype]
            tier = spec.tier
            self.seq += 1
            self.ingested += 1
            naive_units += spec.cost
            if etype is EventType.CLICK:
                self.true_clicks += 1

            rung = self.ladder[tier.value]
            roll = self.rng.random()

            if tier is Tier.P0:
                decision = Decision.STREAM_NOW  # never anything else. ever.
            elif tier is Tier.P1:
                if rung >= 2 and roll < 0.35:
                    decision = Decision.DEFER
                elif rung >= 1 and roll < 0.55:
                    decision = Decision.MICRO_BATCH
                else:
                    decision = Decision.STREAM_NOW
            else:  # P2
                if rung >= 3 and roll < 0.55:
                    decision = Decision.SHED
                elif rung >= 2 and roll < 0.70:
                    decision = Decision.SAMPLE_ROLLUP
                elif rung >= 1 and roll < 0.60:
                    decision = Decision.MICRO_BATCH
                else:
                    decision = Decision.STREAM_NOW

            if decision is Decision.SHED:
                self.shed += 1
                self.value_shed += spec.value
            elif decision is Decision.SAMPLE_ROLLUP:
                self.sampled_out += 1
                if etype is EventType.CLICK:
                    self.sampled_clicks += 1
            elif decision is Decision.DEFER:
                self.deferred_pending += 1
            else:
                self.queue[tier.value] += 1
                admitted_units += spec.cost

            # Narrate roughly one decision in eight, weighted toward the
            # interesting ones — the panel is a story, not a log.
            if decision in (Decision.SHED, Decision.SAMPLE_ROLLUP) or self.rng.random() < 0.12:
                self._note(self.seq, etype, decision, _REASONS[decision], now)

        # --- service --------------------------------------------------------
        # 1. the workers finish whatever they were holding at the last frame
        served = 0
        for key in TIER_KEYS:
            held = self.in_flight_by_tier[key]
            if held:
                self.processed += held
                self.value_delivered += held * self._avg_value_for_tier(Tier(key))
                self.in_flight_by_tier[key] = 0
                served += held

        # 2. drain the queue under this tick's capacity budget, richest tier
        #    first. Cheap events are cheap: 37.5 u/tick buys a lot of clicks.
        budget = cfg.total_capacity_ups * self.dt
        served_units = 0.0
        for key in TIER_KEYS:
            spec_cost = self._avg_cost_for_tier(Tier(key))
            while self.queue[key] > 0 and budget >= spec_cost:
                self.queue[key] -= 1
                budget -= spec_cost
                served_units += spec_cost
                served += 1
                self.processed += 1
                self.value_delivered += self._avg_value_for_tier(Tier(key))

        # 3. hand each worker the one event it is holding as this frame is
        #    taken. At most worker_count events are ever in flight.
        grab = min(cfg.worker_count, self._in_queue())
        for key in TIER_KEYS:
            if grab <= 0:
                break
            take = min(grab, self.queue[key])
            self.queue[key] -= take
            self.in_flight_by_tier[key] += take
            grab -= take
        self.in_flight = sum(self.in_flight_by_tier.values())
        self.cost_adaptive += served_units
        self.cost_naive += naive_units

        self.completed_recent.append(served / self.dt)
        del self.completed_recent[:-8]  # last 2 seconds

        # --- drain the deferred buffer when the storm passes -----------------
        if self.pressure < 0.7 and self.deferred_pending:
            drained = min(self.deferred_pending, max(1, int(6 * self.dt * 4)))
            self.deferred_pending -= drained
            self.queue[Tier.P1.value] += drained

        return self._frame(now)

    def _avg_cost_for_tier(self, tier: Tier) -> float:
        specs = self.cfg.tiers_of(tier)
        return sum(s.cost for s in specs) / len(specs)

    def _avg_value_for_tier(self, tier: Tier) -> float:
        specs = self.cfg.tiers_of(tier)
        return sum(s.value for s in specs) / len(specs)

    # -- frame assembly --------------------------------------------------

    def _latency(self, tier: str, q: float) -> float:
        """Queue wait implied by depth, plus service time and noise. P0 stays
        flat because it is never allowed to build a backlog."""
        base = {"P0": 6.0, "P1": 24.0, "P2": 90.0}[tier]
        share = {"P0": 0.5, "P1": 1.0, "P2": 2.4}[tier]
        wait = self.queue[tier] * share * 3.2
        spread = {0.50: 1.0, 0.95: 1.9, 0.99: 2.7}[q]
        jitter = self.rng.uniform(0.9, 1.15)
        return round((base + wait) * spread * jitter, 2)

    def _frame(self, now: float) -> MetricsFrame:
        cfg = self.cfg
        throughput = (sum(self.completed_recent) / len(self.completed_recent)
                      if self.completed_recent else 0.0)
        offered = cfg.baseline_eps * self.spike

        p50 = {t: self._latency(t, 0.50) for t in TIER_KEYS}
        p95 = {t: self._latency(t, 0.95) for t in TIER_KEYS}
        p99 = {t: self._latency(t, 0.99) for t in TIER_KEYS}
        weight = {t: max(self.queue[t], 1) for t in TIER_KEYS}
        total_w = sum(weight.values())

        def pooled(d: dict[str, float]) -> float:
            return round(sum(d[t] * weight[t] for t in TIER_KEYS) / total_w, 2)

        # Rollup estimate of clicks: observed * sample_weight, with the small
        # estimation error a real sampler would have.
        weighted_clicks = (self.true_clicks - self.sampled_clicks) + \
            self.sampled_clicks * self.rng.uniform(0.97, 1.03)

        return MetricsFrame(
            ts=now,
            mode=Mode.ADAPTIVE,
            queue_depth=dict(self.queue),
            latency_p50=p50,
            latency_p95=p95,
            latency_p99=p99,
            latency_p50_all=pooled(p50),
            latency_p95_all=pooled(p95),
            latency_p99_all=pooled(p99),
            throughput=round(throughput, 2),
            offered_rate=round(offered, 2),
            admitted_rate=round(offered * (1.0 - min(0.6, max(0.0, self.pressure - 1.0))), 2),
            service_rate=round(min(cfg.total_capacity_ups,
                                   offered * cfg.weighted_cost_per_event()), 2),
            pressure=round(self.pressure, 4),
            ladder_rung=dict(self.ladder),
            spike_multiplier=round(self.spike, 2),
            worker_count=cfg.worker_count,
            active_workers=self.in_flight,
            ingested=self.ingested,
            processed=self.processed,
            in_queue=self._in_queue(),
            in_flight=self.in_flight,
            deferred_pending=self.deferred_pending,
            sampled_out=self.sampled_out,
            shed=self.shed,
            weighted_click_count=round(weighted_clicks, 1),
            true_click_count=self.true_clicks,
            cost_adaptive=round(self.cost_adaptive, 1),
            cost_naive=round(self.cost_naive, 1),
            value_delivered=round(self.value_delivered, 1),
            value_shed=round(self.value_shed, 1),
            sla_met=self._sla(met=True),
            sla_missed=self._sla(met=False),
            retries=0,
            duplicates_caught=0,
            exactly_once_violations=0,
            recent_decisions=list(self.recent_decisions),
            recent_sheds=list(self.recent_sheds),
        )

    def _sla(self, met: bool) -> dict[str, int]:
        """Split processed work into met/missed by tier, plausibly: P0 always
        makes its deadline, P2 stops making it under pressure."""
        out = per_tier_int()
        share = {"P0": 0.10, "P1": 0.10, "P2": 0.80}
        miss_rate = {"P0": 0.0, "P1": min(0.25, max(0.0, self.pressure - 1.0)),
                     "P2": min(0.75, max(0.0, self.pressure - 0.8))}
        for t in TIER_KEYS:
            total = int(self.processed * share[t])
            missed = int(total * miss_rate[t])
            out[t] = (total - missed) if met else missed
        return out


# --------------------------------------------------------------------------
# Feeds
# --------------------------------------------------------------------------


async def frames(hz: float = 4.0, seed: int | None = None,
                 seconds: float | None = None) -> AsyncIterator[MetricsFrame]:
    """Async feed of fake frames. app.py's WebSocket will consume either this
    or metrics.snapshot() behind the same interface."""
    src = FakeSource(seed=seed, hz=hz)
    period = 1.0 / hz
    deadline = None if seconds is None else time.time() + seconds
    next_at = time.time()
    while deadline is None or time.time() < deadline:
        yield src.tick()
        next_at += period
        await asyncio.sleep(max(0.0, next_at - time.time()))


async def _run(args: argparse.Namespace) -> int:
    count = 0
    async for frame in frames(hz=args.hz, seed=args.seed, seconds=args.seconds):
        count += 1
        if args.pretty:
            print(frame.model_dump_json(indent=2), flush=True)
        else:
            print(frame.model_dump_json(), flush=True)
    print(f"# {count} frames at {args.hz} Hz", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit plausible MetricsFrames.")
    ap.add_argument("--hz", type=float, default=4.0)
    ap.add_argument("--seconds", type=float, default=None,
                    help="stop after N seconds (default: run forever)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--pretty", action="store_true")
    args = ap.parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
