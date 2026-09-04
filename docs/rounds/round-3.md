# PULSE — Round 3

## What we have built

Since Round 2 (`v1-jury` — the tagged end of the core build), Stage I
added the layer every real deployment eventually needs and every demo
benefits from proving live: the pipeline now survives failures it causes
on purpose, not just load it was asked to survive. A write-ahead
checkpoint (`checkpoint.py`) records, per worker, exactly which events it
currently holds — before the one `await` a worker's death can land
inside, cleared immediately after — so `WorkerPool` can detect a real
task death, recover exactly what that worker was holding (never a whole
batch for a few real stragglers — a batch of 50 with 3 unfinished
retries 3, not 50), and respawn a replacement under the same `worker_id`
so the fixed 6-worker capacity ceiling never silently shrinks
([ADR 0009](../adr/0009-write-ahead-checkpoint-over-full-transaction-log.md)).
An ingest-time deduplicator (`dedup.py`) — a hand-rolled Bloom filter as a
candidate check ONLY, backed by a bounded exact-set confirmation — stops
a confirmed duplicate before it ever reaches the queue, for every tier
including P0, while an unconfirmed Bloom hit is never trusted alone, for
any tier, P0 included ([ADR 0010](../adr/0010-bloom-lru-over-persistent-dedup-store.md)).
`POST /chaos/kill-worker` and `POST /chaos/duplicate-flood` trigger both
mechanisms for real, live, on demand — not a simulated stand-in. And the
per-type cost that drives every ordering decision is now a learned
estimate (`costmodel.py`), passively observed from real completions,
never a bandit, falling back to the config prior at low confidence, with
a live convergence chart and a demo-beat trigger (a heavier payload mix,
injected mid-run) that visibly re-adapts and reroutes
([ADR 0011](../adr/0011-online-cost-learning-over-static-or-bandit.md)).

## What we're showing you

1. **Kill a worker, live, and watch the pool heal itself with zero
   double-processing.** The Recovery panel's own four numbers (workers
   killed, events retried, duplicates suppressed, exactly-once
   violations) are the same claim the audit trail already made possible
   for triage decisions, now made for failure recovery too — and
   `exactly_once_violations` reads 0 whether we ask nicely or not.
2. **Flood 1000 duplicates and watch the sink not move.** Not because the
   sink happens to be idempotent (it always was) — because `dedup.py`
   catches nearly all of them BEFORE they ever cost a queue slot or a
   worker's simulated service time, which is the actual point: dedup at
   ingest is a capacity-protection mechanism, not just a correctness
   backstop.
3. **Inject a heavier payload mix and watch the system re-route around
   its own updated belief about cost**, live: the learned line crosses
   the dotted prior on the chart within seconds, and the SAME shift
   visibly moves pressure, P0 latency, and queue depth — a live
   demonstration that this project's own ordering math actually consumes
   the estimate it claims to, not a chart drawn from a number nothing
   downstream reads.
4. **A real bug, found this session, by directly reproducing it, not by
   guessing.** Building the cost-model's own end-to-end test surfaced a
   genuine (3-for-3 reproducible) conservation-equation false alarm — not
   a pipeline defect, but a cross-thread race in how a test polls
   `metrics.snapshot()` against `TestClient`'s own background-thread
   engine. Root-caused with a standalone reproduction script, fixed in
   this stage's own tests (poll the HTTP layer instead), and flagged —
   not silently patched — as a latent hazard in several already-committed
   tests that simply haven't hit it yet.

## What we know is incomplete

- **Four `MetricsFrame` fields remain stub**: `throughput`,
  `cost_adaptive`, `cost_naive`, `spike_multiplier` — down from seven at
  Round 2, since `retries`, `exactly_once_violations`, and
  `duplicates_caught` are all real as of this stage. Live cost comparison
  ships anyway (Stage H's `CostComparisonPanel`, computed client-side
  from real `worker_count`/`offered_rate` fields) — the stub fields were
  never actually blocking anything the dashboard needed to show.
- **`metrics.py`'s own thread-safety gap, found and flagged this
  session, not fixed.** `metrics.snapshot()`'s module-level counters
  assume single-threaded access (CLAUDE.md hard rule 1) — true for the
  real running pipeline, but not for a test polling it directly from a
  different thread than the one `TestClient` runs the engine on. This
  stage's own new tests avoid the pattern; several already-committed
  tests still use it and simply haven't hit the race. A lock around
  `metrics.py`'s counters, or a project-wide rule against direct
  cross-thread polling in tests, is a real, scoped follow-up — not done
  here because it is a different-sized change than any single stage this
  session covered.
- **No per-customer/partition ordering, no chaos injection beyond worker
  death and duplicate delivery** (a corrupted sink file, a hung — not
  killed — worker, a WebSocket dropped mid-write) — both already named
  in Round 2 as deliberate cuts, unchanged since.
- **The learned cost model has never been validated against a REAL
  workload's true cost distribution** — only against this project's own
  synthetic, uniform payload-size draw. The calibration argument
  (`true_cost`'s expectation equals the prior) is proven for that
  synthetic distribution specifically; a real deployment's own payload
  sizes might not be uniform, which would change the mean the learner
  converges toward, not break the mechanism itself.

## Evidence: `git log --oneline --decorate v1-jury..HEAD`

```
f0ca5ac (HEAD -> main) Stage I: the learned cost model
3053e2c Stage I: write-ahead checkpoint + chaos endpoints + ingest-time dedup
```
