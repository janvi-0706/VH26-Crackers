# 0002 — Simulated service cost for deterministic capacity

## Context

Every claim PULSE makes ("P0 stays under 200ms," "we survive a 1.9x
overload") is a claim about capacity versus demand. It must hold identically
on a judge's laptop, a demo machine, and CI — none with comparable hardware.

## Options considered

1. **Real work per event** — actual CPU/IO proportional to declared cost.
   "Authentic," but throughput becomes a function of whatever machine runs it.
2. **A discrete-event simulation with a logical clock, no wall-clock at
   all.** Perfectly deterministic, but the live "watch it happen" dashboard
   moment disappears.
3. **Simulated service time via `asyncio.sleep(cost / capacity_per_worker)`,
   real wall clock, a fixed cost model from `config/tiers.yaml`.**

## Decision

Option 3. `cost` is a config-driven work-unit number, not measured CPU time;
a worker "does the work" by sleeping for `cost / 25 u/s`. Six workers × 25
u/s is a documented constant capacity ceiling, not a benchmark result.
Disclosed everywhere it matters (CLAUDE.md hard rule 2, `DATA_MODEL.md`, the
demo script), never presented as real processing.

## Consequences

- The three calibration invariants (P0 ~108 u/s, total ~288 u/s at spike,
  ~14 u/s baseline) are exact and reproducible — verified by `config.py`'s
  own load-time check, not asserted by hope.
- This model exposed a real bug: Windows' asyncio timer overhead made 333
  events/sec unreachable with naive per-event pacing (measured ~200 eps); a
  batching fix was required for the simulation to honor its own contract
  (PROGRESS.md, Stage C).
- Honesty cost: we say out loud, in the demo, that service time is
  simulated — explicitly rewarded per the problem statement, never glossed
  over.
- If a judge asks "is this real," the answer is one sentence, not a
  scramble.
