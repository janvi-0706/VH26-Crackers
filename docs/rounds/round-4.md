# PULSE — Round 4 (final)

## What we have built

PULSE survives a 20x traffic spike (16.65 → 333 events/sec) by triaging,
not scaling: one Python process, `asyncio`, in-memory heaps, a
hand-written three-tier priority scheduler in front of a fixed 6-worker
pool — no Kafka, no Redis, no Celery, by deliberate design
([ADR 0001](../adr/0001-in-process-asyncio-over-kafka.md)). Every event
carries a business value, a tier, and an SLA deadline, all sourced from
an externalised, calibration-checked config, never hardcoded. A split
ordering/pressure decision function ([ADR 0005](../adr/0005-split-ordering-and-pressure-functions.md))
drives a five-rung escalation ladder (MICRO_BATCH → DEFER →
SAMPLE_ROLLUP → SHED) gated by a sojourn-time AQM
([ADR 0006](../adr/0006-sojourn-aqm-over-queue-length.md)), backed by
AIMD credit-based admission control upstream and a hash-chained,
tamper-evident audit ledger ([ADR 0008](../adr/0008-hash-chained-audit-ledger.md))
downstream — every decision this system ever makes is in that ledger, in
order, exportable, independently verifiable. Since the `v1-jury` tag,
Stage I added exactly-once recovery across a real worker death
([ADR 0009](../adr/0009-write-ahead-checkpoint-over-full-transaction-log.md)),
ingest-time deduplication that never trusts an unconfirmed signal for any
tier ([ADR 0010](../adr/0010-bloom-lru-over-persistent-dedup-store.md)),
and a passively-learned, non-bandit cost model that replaces a flat
per-type constant with a real, re-adapting estimate
([ADR 0011](../adr/0011-online-cost-learning-over-static-or-bandit.md)).
This final round extends `bench/run.py` to prove the exactly-once claim
under real chaos headlessly (two more configs: a real worker killed
mid-spike, a real 1000-event duplicate flood mid-spike — both report
`exactly_once_violations`, and it reads 0 in every row of all six), and
closes out the documentation set: architecture, eleven ADRs, and this
round history.

## What we're showing you

1. **`exactly_once_violations: 0`, in a table, for a config where we
   ourselves killed a worker mid-spike.** Not a live demo claim taken on
   faith — a headless, reproducible benchmark row, generated the same way
   every other number in `bench/report.md` was.
2. **The same claim, for a config where we ourselves replayed 1000
   duplicate deliveries mid-spike.** Two independent chaos actions,
   two independent zero counts, in the one report a judge can open
   without needing the dashboard running at all.
3. **Eleven ADRs, not four.** `docs/adr/0001` through `0011` is the
   complete, dated record of every consequential design choice this
   project made and the alternative each one was chosen over — the
   literal answer to "why didn't you just—" for eleven different
   questions, without needing us to remember the reasoning live.
4. **`docs/SUBMISSION.md`** — one page, everything else linked from it:
   both architecture views, the data model, the benchmark report, the ADR
   index, and every round document including this one.

## Known, final scope — named as cuts, not gaps

Nothing below is an oversight; each was a deliberate choice, stated
plainly, the way `RUNBOOK.md`'s own cut list was written before this
project's first line of code:

- **No per-customer/partition ordering enforced.** `partition_key` exists
  on every event and in every relevant index; EDF reordering can still
  serve one customer's events out of arrival order. Unchanged since
  Round 2 — the honest answer to "why not just one id"'s sibling
  question, "why not preserve per-customer order."
- **No chaos beyond a killed worker and a duplicate flood.** A corrupted
  sink file, a hung (not killed) worker, a mid-write dropped WebSocket —
  real, different failure modes this project does not claim to survive.
- **`metrics.py`'s cross-thread test-polling hazard, found and flagged
  this session, is fixed in this session's own new tests but not
  patched at the source.** A lock around `metrics.py`'s counters (or a
  project-wide rule against polling `metrics.snapshot()` directly from a
  different thread than the engine's own) is real, scoped follow-up work
  named explicitly in Round 3, not silently left for someone to
  rediscover.
- **The learned cost model is validated against this project's own
  synthetic, uniform payload-size distribution, not a real production
  workload's.** The mechanism (passive observation, smooth confidence
  blending, sample-recency decay) does not depend on that distribution
  being uniform; the specific calibration numbers quoted in `costmodel.py`
  and `ADR 0011` do.
- **Four `MetricsFrame` fields remain stub**: `throughput`,
  `cost_adaptive`, `cost_naive`, `spike_multiplier` — designed into the
  frozen contract in Stage A specifically so no later stage would need to
  unfreeze it, still unclaimed because nothing built since has needed
  them to ship its own acceptance line.

## Evidence: `git log --oneline --decorate v1-jury..v2-final`

```
3b28f50 (HEAD -> main, tag: v2-final) Final prompt: bench chaos configs, ADRs 0009-0011, docs, submission page
f0ca5ac Stage I: the learned cost model
3053e2c Stage I: write-ahead checkpoint + chaos endpoints + ingest-time dedup
```
