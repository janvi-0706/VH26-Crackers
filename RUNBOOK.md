# PULSE — single-machine runbook

One laptop. One repo. One branch. 25 prompts in strict order.
Core build: ~15 hours. Freeze and rehearse: ~3 hours. Stretch: hours 18–24.

---

## How this works

**One prompt at a time.** Run it, check the acceptance criteria, commit, then move on. Never run two prompts before checking the first.

**Commit after every prompt** with the prompt number in the message: `git commit -m "P7: three tiers + EDF + naive toggle"`. That history is literally your "progress made" evidence at each jury round.

The canonical remote is the GitHub repository supplied by the user. After each
successful commit, push `main` to that remote. Never force-push. If the remote
already contains commits, fetch and merge them before the first push; do not
assume that `git init` can be pushed as an unrelated history.

**Tag at every stage boundary.** `git tag stage-c`. If something breaks catastrophically at hour 14, `git checkout stage-c` gives you a working demo in ten seconds.

**Start a fresh Claude Code session at each stage boundary** and open it with prompt **R1** (appendix). Long sessions accumulate context bloat and Claude Code starts forgetting `CLAUDE.md`. Fresh session + `R1` + `PROGRESS.md` is more reliable than one 18-hour conversation.

**If you have teammates**, they aren't idle. While one person drives Claude Code, the others write `DATA_MODEL.md` review notes, draft the round documents, rehearse the pitch, and read every diff before it's committed. Reading the code is not optional — "code originality" gets tested by a judge asking *you* to explain a function.

---

## What got cut, and why

| Cut | Reason |
|---|---|
| Dynamic worker scaling | Nice metric, no rubric points, ~2h |
| Causal dependency ordering | Interesting but demo-invisible, ~2h |
| Per-key partition ordering | Becomes a Q&A answer instead of code |
| Six-config benchmark | Four configs prove the same thing |
| Four architecture diagrams | Two views, auto-generated |
| Deployment | Local demo only |

Every one of those is a fine answer to *"what's next?"* in the jury round. Saying "we scoped it out deliberately, here's why" scores better than a half-built version of it.

---

## Stage map

| Stage | Hours | Prompts | Ships |
|---|---|---|---|
| **A** | 0–2 | S1, P1–P3 | Contract frozen, data model documented |
| **B** | 2–4.5 | P4–P6 | Vertical slice runs end to end |
| **C** | 4.5–7 | P7–P9 | **Demoable — tiers, EDF, naive toggle, spike** |
| **D** | 7–10 | P10–P12 | Adaptive engine, batching, deferral |
| **E** | 10–12 | P13–P14 | CoDel, weighted rollups, backpressure |
| **F** | 12–13.5 | P15–P16 | Conservation ledger, audit, traces |
| **G** | 13.5–15 | P17–P18 | Benchmark report, invariant tests |
| **H** | 15–18 | P19–P21 | Docs, freeze, rehearse |
| **I** | 18–24 | P22–P25 | Stretch: retry, chaos, dedup, cost learner |

Jury rounds land at roughly H8 (end of C), H16 (mid H), H23 (end of I), H30 (final).

---

# STAGE A — Contract and data model (H0–2)

## S1 — Create `CLAUDE.md` by hand

Not a prompt. Create this at the repo root and commit it before anything else. Claude Code re-reads it every session, which is what saves you when context compacts at hour 14.

````markdown
# PULSE — adaptive event pipeline (hackathon)

## What this is
An event pipeline that survives a 20x traffic spike by triaging, not scaling.
Every event carries a business value and an SLA deadline. Worker capacity is
scarce and fixed. The pipeline maximises value delivered per unit of capacity,
subject to a hard constraint: critical events are never silently dropped, and
we can prove it.

## Judged on (four jury rounds)
1. Progress made — a delta at each round, not an absolute
2. Code originality — hand-written logic, not glued-together libraries
3. Data design — schemas, identity model, indexes, growth bounds
4. Architecture — module boundaries, control loops, documented decisions
Demo polish is NOT scored. Do not spend time on it.

## Hard rules
1. NO Kafka, NO Redis, NO Docker Compose, NO Celery. Single Python process,
   asyncio, in-memory heaps. Spike load is only ~333 events/sec. Writing the
   scheduling logic ourselves IS the originality score.
2. Worker service time is SIMULATED via a cost model so the capacity ceiling
   is deterministic on any machine. Intentional. Must be documented.
3. P0 (orders, payments) is NEVER batched, deferred, sampled, or shed. Under
   pressure we throttle the source instead. Asserted in code and tests.
4. Every prompt must end in a RUNNABLE state. Never leave the repo broken.

## Working style — single machine, sequential
- ONE prompt at a time. Do exactly what it asks, nothing more.
- Never build ahead. If you think a later feature is needed now, SAY SO and
  wait for me. Do not implement it.
- After every prompt: run it, show me the output, update PROGRESS.md,
  then `git commit -m "PN: <what you did>"`.
- Keep commit messages legible. Our git history is progress evidence.
- If PROGRESS.md and my instructions disagree, PROGRESS.md is the truth about
  what exists; ask me about the difference.

## Stack
Python 3.11, asyncio, FastAPI, uvicorn, pydantic. SQLite (stdlib) for sink,
deferred buffer, audit ledger, rollups. Dashboard: React + Vite + Recharts +
Tailwind, served as static from FastAPI. One WebSocket at 4 Hz. pytest.

## Event taxonomy (config/tiers.yaml)
| type      | tier | value | sla   | cost |
|-----------|------|-------|-------|------|
| payment   | P0   | 120   | 200ms | 3.5u |
| order     | P0   | 100   | 500ms | 3.0u |
| inventory | P1   | 40    | 5s    | 2.0u |
| click     | P2   | 5     | 30s   | 0.5u |
| log       | P2   | 1     | 60s   | 0.3u |

Mix: 5% payment, 5% order, 10% inventory, 50% click, 30% log.
Worker capacity: 25 work-units/sec each, 6 workers = 150 u/s total.

Calibration that MUST hold — re-verify if you change any number:
- P0 demand at spike ~108 u/s, comfortably under 150 u/s capacity
- Total demand at spike ~288 u/s, ~1.9x capacity, so triage is forced
- Total demand at baseline ~14 u/s, so everything streams individually

## Data identity model — five distinct fields, never one
| field           | identifies             | assigned by | on retry |
|-----------------|------------------------|-------------|----------|
| event_id        | one emission           | generator   | NEW      |
| dedup_key       | business identity      | generator   | SAME     |
| seq             | pipeline order         | classifier  | NEW      |
| partition_key   | ordering domain (cust) | generator   | SAME     |
| idempotency_key | sink upsert target     | classifier  | SAME     |
Collapsing these into one `id` is why most pipelines' dedup breaks retry.

## Frozen after Stage A
contracts.py and config/tiers.yaml. If you need a field that isn't there,
STOP and tell me — do not add it silently.
````

## P1 — Scaffold

```
Read CLAUDE.md. No feature code yet.

Create the repo skeleton:
- pyproject.toml (python 3.11: fastapi, uvicorn, pydantic, pytest,
  pytest-asyncio, pyyaml)
- src/triage/ with empty modules: contracts.py, metrics.py, ledger.py,
  app.py, queue.py, decision.py, worker.py, codel.py, ladder.py,
  generator.py, classifier.py, admission.py, sink.py, deferral.py
- config/, bench/, docs/, docs/adr/, docs/rounds/, tests/, dashboard/
- .gitignore, Makefile with empty dev / fake / test / bench targets
- PLAN.md listing stages A-I with checkboxes
- PROGRESS.md, empty

git init, add everything, initial commit.

Show me the tree. Stop.
```

## P2 — Contracts (the most important prompt in the runbook)

Everything downstream depends on this being right. `MetricsFrame` carries every field you will ever need, defaulted to zero, so the schema never changes again — that single decision prevents the most common cause of hour-14 chaos.

```
Read CLAUDE.md. Stage A: contracts only, no engine logic.

1. src/triage/contracts.py — pydantic models.

   Event: event_id, dedup_key, seq, partition_key, idempotency_key,
   type, tier, payload_size, value, cost, ingest_ts, deadline_ts,
   schema_version. All five identity fields separate, per CLAUDE.md.

   Decision enum: STREAM_NOW | MICRO_BATCH | DEFER | SAMPLE_ROLLUP | SHED

   MetricsFrame — must ALREADY contain every field we will ever need, all
   defaulting to 0 or empty, so this schema never changes again:
   per-tier queue_depth, per-tier p50/p95/p99 latency, throughput,
   offered_rate, admitted_rate, service_rate, pressure, per-tier ladder_rung,
   worker_count, active_workers, ingested, processed, in_queue, in_flight,
   deferred_pending, sampled_out, shed, weighted_click_count,
   true_click_count, cost_adaptive, cost_naive, retries, duplicates_caught,
   exactly_once_violations, mode, recent_decisions[], recent_sheds[].

2. src/triage/metrics.py — module-level registry:
   observe_ingest(event), observe_dequeue(event), observe_complete(event),
   observe_decision(event, decision, reason, pressure), snapshot() -> MetricsFrame.
   Implement latency percentiles for real. Stub everything else to 0.

3. src/triage/ledger.py — record(seq, decision, reason, pressure, tier) as a
   no-op stub appending to an in-memory list. The real version lands in Stage F.
   Do not call it during ingestion: no routing decision exists yet. The first
   call site is the decision/routing stage, and every actual decision must be
   recorded there.

4. config/tiers.yaml — the tier table, mix percentages, worker capacity.
   Plus a small loader module.

5. src/triage/fake_metrics.py — emits plausible random-walk MetricsFrames at
   4 Hz, so the dashboard can be built before the engine exists.

6. tests/test_contracts.py — round-trip serialisation.

Acceptance: `python -m triage.fake_metrics` prints valid frames at 4 Hz.
Then print the full MetricsFrame field list so I can review it.
```

**Before P3: read that field list yourself.** Anything missing is free to add now and expensive at hour 12.

## P3 — Data model document

Data design is a quarter of your score and almost nobody will have a schema doc at round 1. Thirty minutes buys a criterion.

```
Read CLAUDE.md. Write docs/DATA_MODEL.md. No code changes.

1. Identity model — the five fields. For each: what it identifies, who
   assigns it, its lifecycle under retry, which component consumes it.
   Explain why five fields and not one, using the concrete failure case:
   a retry that dedup would wrongly suppress if event_id and dedup_key
   were the same field.

2. Event envelope — explain that the frozen MVP Event carries routing metadata
   and payload_size; the type-specific payload is opaque persistence data, not
   a field in the generic scheduler contract. Do not add a payload field after
   the freeze without an explicit contract review. Include schema_version and
   the forward-compatibility argument for MetricsFrame carrying every future
   dashboard field defaulted from day one.

3. Tier configuration as externalized data, not code constants.

4. Full SQLite DDL for: events_sink, deferred_buffer, audit_ledger, rollups,
   decision_traces. For every table: primary key, every index and the query
   it serves, and the bounded-growth strategy.

5. The rollups table in detail:
   rollup_id, event_type, window_start, window_end, sample_weight,
   observed_count, subtype_counts (JSON), seq_low, seq_high.
   Explain sample_weight semantics, why seq_low/seq_high are required for
   the conservation equation to balance, and how downstream count estimation
   works (observed_count * sample_weight).

6. The audit ledger hash chain — what is hashed, what tampering it detects,
   what it does not detect.

7. A mermaid erDiagram of the five tables.

Then verify every *wire-contract* field described actually exists in
contracts.py. SQL-only columns such as primary keys, timestamps, and hashes
are persistence schema, not Pydantic wire fields; label them as such. List
any missing wire fields — we may need to unfreeze the contract before Stage B.
```

**`git tag stage-a`. New Claude Code session.**

---

# STAGE B — Vertical slice (H2–4.5)

## P4 — Ingress and sink

```
Read CLAUDE.md and PROGRESS.md. Stage B. Build generator.py, classifier.py,
sink.py only. No queues or workers yet.

generator.py: async event generator, configurable rate, emitting the 5 types
per the mix in CLAUDE.md. Payload sizes vary within type. Assigns event_id,
dedup_key, and partition_key (customer_id drawn from a pool of 500).

classifier.py: assigns tier, value, cost, absolute deadline_ts, monotonic seq,
and idempotency_key from config/tiers.yaml. Do not call ledger.record here:
classification is not a routing decision; the decision stage records decisions
when it exists.

sink.py: SQLite writer, events_sink table exactly as specified in
docs/DATA_MODEL.md, including the indexes that document specifies.

Import contracts and config; do not modify them.

Tests: type mix within 2% of target over 10,000 events; seq strictly
monotonic with no gaps; sink round-trips an event correctly.
```

## P5 — Queue, workers, app

```
Read CLAUDE.md and PROGRESS.md. Stage B. Build queue.py, worker.py, app.py,
Makefile.

queue.py: a single FIFO asyncio queue for now. put() and get() call
metrics.observe_ingest and observe_dequeue.

worker.py: pool of 6. Each dequeues one event and awaits
asyncio.sleep(event.cost / 25.0) to simulate service time, then calls
metrics.observe_complete and sink.write. 25 work-units/sec per worker is our
documented capacity ceiling.

app.py: FastAPI. GET /health, POST /control/rate, WS /ws pushing
metrics.snapshot() at 4 Hz. A --fake flag serves fake_metrics instead of the
real engine. Wire generator -> classifier -> queue -> workers -> sink into one
asyncio event loop. Serve dashboard/dist as static.

Makefile: `make dev`, `make fake`, `make test`, `make bench` (stub).

No priority, no tiers, no batching yet.
Test: 6 workers sustain ~150 u/s within 5%.
```

## P6 — Dashboard scaffold

```
Read CLAUDE.md and PROGRESS.md. Stage B. Build dashboard/ only. Do not touch src/.

Vite + React + Tailwind + Recharts, dark theme. Connect to ws://localhost:8000/ws
and render MetricsFrame.

Build a reusable Panel component FIRST — we will add roughly 10 panels over the
next 12 hours, so the layout system matters more than these three charts.
Use a CSS grid that panels drop into without reflowing the others.

Panels this prompt:
- throughput line chart
- three-line per-tier latency chart
- large P0 p99 scoreboard against its 200ms target, green/red

Auto-reconnect on socket drop with a visible connection indicator.

Acceptance: `make dev` starts one process; charts move at 1000 events/min;
POST /control/rate 20000 makes latency visibly climb because 6 workers cannot
keep up. That degradation is the baseline we are about to beat.
```

**`git tag stage-b`. New session.**

---

# STAGE C — Demoable (H4.5–7)

After this stage you always have something to show a jury, whatever happens later. This is the stage you protect.

## P7 — Tiers, EDF, naive toggle

```
Read CLAUDE.md and PROGRESS.md. Stage C. Replace the FIFO with three heaps.

P0, P1, P2. Prefer the highest-priority non-empty tier. The aging guard is an
explicit bounded exception: once the oldest P2 sojourn exceeds its guard, serve
one eligible P2 item, then resume priority selection. This is priority with a
starvation bound, not an absolute "P0 fully before P1" guarantee.

Within P0, order by earliest deadline_ts (EDF), not arrival order.

Write a readable test proving an order at 400ms of its 500ms SLA is dequeued
ahead of a payment that arrived 2ms ago with a 200ms SLA. That behaviour is the
difference between a lookup table and a scheduler, and a judge will ask about it.

Expose set_mode("naive" | "adaptive"). Naive reverts to a single FIFO with no
priority. This is our benchmark control and must keep working for the rest of
the project — add a test that locks it.

Update the dashboard: per-tier stacked queue depth panel.
```

## P8 — Live controls

```
Read CLAUDE.md and PROGRESS.md. Stage C.

Backend: POST /control/mode, /control/spike (instant jump to 20000/min, no
ramp — the spike is a step function by spec), /control/reset. Add
inject_event(type, partition_key=None) so we can drop a single event into a
running stream. Its value, tier, cost, and SLA still come from config; callers
must not override classification economics.

Wire per-tier latency percentiles properly in metrics.py — p50/p95/p99 per
tier, never a blended aggregate. Add a test proving a blown P0 latency cannot
be hidden by healthy P2 numbers.

Dashboard: control bar with a rate slider, a large SPIKE button, RESET, and a
naive/adaptive toggle. Make the SPIKE button unmissable — it gets pressed on
stage and must be impossible to misclick.

Acceptance, verify both:
- naive + spike: all three tier latencies climb together
- adaptive + spike: P0 flat, P2 climbing
If that contrast is not visible on screen, STOP and fix it before Stage D.
It is the entire demo.
```

## P9 — Round 1 documents

```
Read CLAUDE.md and PROGRESS.md. Documentation only, no code.

1. docs/adr/ — one file per decision, max 300 words, format:
   Context / Options considered (at least two real alternatives) /
   Decision / Consequences.
   Write these now:
     0001 in-process asyncio over Kafka
     0002 simulated service cost for deterministic capacity
     0003 five-field identity model instead of one id
     0004 contract-first: freeze schemas before implementation

2. docs/rounds/round-1.md, one page, for judges:
   What we have built / What we're showing you (3 items max) / What we know
   is incomplete (be specific — this reads as maturity, not weakness) /
   Next 8 hours.
   Append `git log --oneline --decorate` as evidence.
```

**`git tag stage-c`. New session. You are now jury-ready — this tag is your fallback for the rest of the event.**

**Round 1 talking points:** lead with contract-first design and the five-field identity model. Hand them `DATA_MODEL.md`. Nobody else has a schema document at hour 8.

---

# STAGE D — Adaptive engine (H7–10)

The originality core. Do not rush this stage.

## P10 — The split decision function

```
Read CLAUDE.md and PROGRESS.md. Stage D. Build decision.py.

TWO functions, not one. Do NOT add pressure as an additive term to the score —
pressure is a system-global scalar, so it cancels out across every pair of
events and has literally zero effect on ordering. That is the standard version
of this design and we are deliberately not building it.

ORDERING (per-event properties only — decides what goes next):
  slack   = deadline_ts - now - est_service_time
  urgency = 1 / max(slack, EPS)
  density = value / cost
  aging   = age / sla
  score   = w1 * density * urgency + w2 * aging

PRESSURE (system state only — decides what mode we are in):
  P = clamp(a*(qdepth/qmax) + b*(arrival_ewma_with_trend/max(service_rate, EPS))
          + c*(p95_sojourn/sla) + d*worker_util, 0, 1)
  qmax and EPS are positive constants; the weights are non-negative and sum
  to 1.0. A zero-service-rate startup uses EPS, never a division by zero.

ROUTING:
  tier P0        -> STREAM_NOW always. Assert every other branch unreachable.
  slack < 0      -> DEFER
  P < 0.40       -> STREAM_NOW
  0.40 to 0.75   -> MICRO_BATCH
  P >= 0.75      -> DEFER

Replace the strict-priority dequeue with score-ordered dequeue within the
tier structure.

Call ledger.record on every non-STREAM_NOW decision.

tests/test_invariant.py: no P0 event ever receives a non-STREAM_NOW decision,
sweeping pressure from 0 to 1 in 0.01 steps.
```

## P11 — Batching and deferral

```
Read CLAUDE.md and PROGRESS.md. Stage D.

Micro-batching in worker.py: batch size = round(B_min + (B_max - B_min) * P),
with B_max capped at 8 to protect the 200ms payment SLA.
A batch costs sum(costs) * 0.4 + 0.5 overhead, so batching is genuinely
cheaper per event rather than just relabelled.

deferral.py: SQLite-backed store matching the deferred_buffer schema in
docs/DATA_MODEL.md. Deferred events persist with their original ingest_ts and
decision reason. A background drainer replays them when P < 0.35, rate-limited
so replay cannot re-trigger pressure and oscillate.

Expose deferred_pending and drain rate in metrics.

Test: after a 30s spike and reset, deferred count in equals count out and the
backlog reaches zero. Nothing deferred is ever lost.
```

## P12 — Engine panels

```
Read CLAUDE.md and PROGRESS.md. Stage D. Dashboard only.

Add:
- pressure gauge, 0 to 1
- current mode indicator per tier
- deferred backlog area chart
- live sliders bound to GET/POST /control/weights for w1, w2, a, b, c, d
  (add those endpoints in app.py)

The sliders are a demo centrepiece. Dragging one on stage and watching routing
change in real time is the most persuasive twenty seconds available to us.

Acceptance: spike -> pressure climbs -> mode steps to micro-batch -> P0 stays
flat -> backlog rises -> reset -> backlog drains to zero.
```

**`git tag stage-d`. New session.**

---

# STAGE E — The degradation ladder (H10–12)

## P13 — CoDel and weighted rollups

```
Read CLAUDE.md and PROGRESS.md. Stage E. Build codel.py and ladder.py.

codel.py — RFC 8289, applied to P2 queue sojourn time. Track the local minimum
sojourn over a 100ms interval. Enter the sampling state only when that minimum
stays above a 500ms target for a full interval. Exit when it drops below.
No queue-length threshold anywhere in this file — sojourn time is the signal.

When CoDel signals, do NOT drop. Reservoir-sample 1 in N and emit a rollup
carrying sample_weight = N, subtype counts, window bounds, and seq_low/seq_high
per docs/DATA_MODEL.md. Hard shed only above P = 0.95, P2 only, always through
ledger.record with a reason string.

sink.py: apply sample_weight when computing counts so downstream totals stay
statistically correct despite dropping individual records. Expose
weighted_click_count and true_click_count.

ladder.py — enum STREAM -> MICRO_BATCH -> DEFER -> SAMPLE_ROLLUP -> SHED with
a per-tier maximum rung: P0 caps at STREAM, P1 caps at DEFER, P2 uncapped.
Assert the caps in tests.

Dashboard: ladder widget showing each tier's current rung.

Acceptance: at sustained spike, weighted_click_count lands within 5% of
true_click_count. That number is the proof we lost resolution, not information.
```

## P14 — Upstream backpressure

```
Read CLAUDE.md and PROGRESS.md. Stage E. Build admission.py.

Credit-based upstream backpressure. The generator must acquire a credit before
emitting. AIMD: additive increase while pressure is low, multiplicative
decrease (x0.8) above P = 0.85. Critical sources retain credits far longer
than bulk sources.

Track offered_rate and admitted_rate separately.
Define offered_rate as the rate presented at the post-throttle source
boundary, and admitted_rate as the rate accepted by the pipeline. A critical
source acquires its credit before creating/offering the event, so P0 admitted
equals P0 offered. Pre-throttle demand is a separate diagnostic and must not
be compared with admitted P0 events as an invariant.

Dashboard: one chart with three lines, offered / admitted / service rate.
The gap between offered and admitted IS the backpressure, made visible.
```

**`git tag stage-e`. New session.**

---

# STAGE F — The proof layer (H12–13.5)

## P15 — Conservation ledger

```
Read CLAUDE.md and PROGRESS.md. Stage F. Make ledger.py real.

Live invariant, asserted continuously:
  ingested == processed + in_queue + in_flight + deferred_pending
              + sampled_out + shed
  no audit or decision-trace row for tier P0 has a non-STREAM_NOW decision

Append-only SQLite audit_ledger: seq, tier, decision, reason,
pressure at decision time, timestamp, prev_hash, row_hash. Hash-chain each row
with the previous row's hash so the log is tamper-evident.
GET /audit.csv exports it. Add verify_chain() and a test proving that mutating
any row breaks verification.

Ring buffer of the last 500 decision traces, queryable by event_id, using only
frozen DecisionTrace fields: seq, event_id, type, tier, decision, reason,
pressure, value, and timestamp. Add derived fields only after an explicit
contract review.

Test: after a 60s spike the equation balances exactly and the critical
assertions never fired.
```

## P16 — Proof panels

```
Read CLAUDE.md and PROGRESS.md. Stage F. Dashboard only.

1. Conservation panel — render the equation live, green tick when balanced,
   hard red if it ever is not. LARGE type. A judge must read this from three
   metres. This panel is what turns a claim into a demonstration.
2. Scrolling shed log with reasons.
3. Event inspector — paste an event_id, see the full decision trace.
4. Download button for /audit.csv.
```

**`git tag stage-f`. New session.**

---

# STAGE G — Benchmark (H13.5–15)

## P17 — Benchmark harness

```
Read CLAUDE.md and PROGRESS.md. Stage G. Build bench/run.py.

Four configs, 90s each, headless: naive/adaptive x baseline/spike.

Per config record: throughput, per-tier p50/p95/p99, per-tier SLA attainment,
counts deferred/batched/sampled/shed, estimated cost. Cost model:
worker-seconds at a stated rate, with a naive comparison that scales workers
linearly to hold everything in stream mode.

Also add a sensitivity row set at 5x, 10x, 20x, 40x showing per-tier SLA
attainment — we want to show where the system actually breaks, not only that
it survives the specified spike. Knowing your own breaking point is a stronger
claim than passing one test.

`make bench` writes bench/report.html (table + charts) and bench/report.md.

Target: naive-at-spike P0 p99 in the seconds, adaptive-at-spike P0 p99 under
200ms, zero critical events lost. If the numbers don't show that, tell me
immediately — it means a calibration problem, not a reporting problem.
```

## P18 — Invariant test suite

```
Read CLAUDE.md and PROGRESS.md. Stage G. No new features. Tests only.

Write tests that lock every invariant we will claim on stage. Name each test
so it reads like the claim it proves:

- P0 is never batched, deferred, sampled, or shed, at any pressure 0 to 1
- P0 admitted rate never falls below P0 offered rate
- ladder rung caps hold per tier under sustained load
- the conservation equation balances after a 60s spike
- deferred count in equals count out after a full drain
- weighted click count is within 5% of true count under sampling
- the audit hash chain detects any row mutation
- naive mode still works and produces the degraded baseline

These tests are what you cite when a judge asks "how do you know?"
Run the full suite and show me the output.
```

**`git tag stage-g`. New session.**

---

# STAGE H — Freeze (H15–18)

## P19 — Dashboard final pass

```
Read CLAUDE.md and PROGRESS.md. Stage H. Dashboard only, then stop.

Final layout: all panels visible without scrolling on a 1920x1080 projector.
Numbers larger than labels. No panel should require explanation to read.

Add the cost comparison panel (adaptive vs naive running total) and the worker
pool grid with cells lighting on activity.

Verify the WebSocket survives a 5-minute run without reconnecting.

Hard stop after this prompt. Demo polish is not a judged criterion and we have
better uses for the remaining hours.
```

## P20 — Architecture documentation

```
Read CLAUDE.md and PROGRESS.md. Stage H. Documentation, no code.

1. docs/ARCHITECTURE.md with two mermaid views:
   - component diagram: modules and dependency direction, showing that
     contracts.py is a leaf that everything imports and nothing imports from
   - control loop: pressure signal, admission credits, mode transitions and
     the ladder, drawn as a feedback system rather than a pipeline

   Then a short section explaining why the module boundaries are where they
   are, and why the contract was frozen before implementation.

2. ADRs 0005-0008:
   0005 split ordering and pressure functions instead of one additive score
   0006 sojourn-time AQM instead of queue-length thresholds
   0007 sample-with-weight instead of drop
   0008 hash-chained audit ledger

3. README.md with setup instructions and an explicit "what is real vs
   simulated" section.
```

## P21 — Demo script and freeze

```
Read CLAUDE.md and PROGRESS.md. Stage H. Final prompt of the core build.

1. docs/DEMO.md — the 5-minute script:
   0:00 baseline, all green, state the claim
   0:30 naive mode + spike, everything collapses together
   1:30 reset, adaptive + spike, P0 flat while P2 degrades down the ladder
   3:00 the conservation panel, shed log, one decision trace, audit.csv
   4:15 benchmark table and the sensitivity row
   4:45 closing line plus one honest sentence on what is simulated

2. docs/QA.md — under 100 words each:
   - why not Kafka
   - is the processing real
   - what if critical events alone exceed capacity
   - what about per-customer ordering when you reorder by priority
   - how did you pick the weights
   - how is your ordering function different from a priority queue
   - where does the system actually break

3. docs/rounds/round-2.md, same format as round-1, with
   `git log --oneline --decorate stage-c..HEAD` appended.

Then: run make test and make bench one final time, update PROGRESS.md with the
full stage history, and git tag v1-jury.
```

**H17–18, no prompts:** record a backup demo video, then rehearse three times. Whoever is presenting drives; the others field the `QA.md` questions cold.

---

# STAGE I — Stretch (H18–24, only if H18 arrived clean)

Progress is scored as a delta at each round, so rounds 3 and 4 need real content. But do not start this stage if anything in Stage H is unfinished.

## P22 — Idempotent retry

```
Read CLAUDE.md and PROGRESS.md. Stage I.

Write-ahead checkpoint: before a worker starts an event or batch, record an
in-flight entry keyed on idempotency_key. On completion, mark done. On worker
death, a recovery pass re-queues only entries still in flight.

Critically: retry re-queues the individual failed events, NOT the whole batch.
A batch of 50 where 3 failed retries 3, not 50.

The sink upsert is keyed on idempotency_key, so duplicate delivery is a no-op.
A payment can never be processed twice.

Expose retries and exactly_once_violations (must always read 0).
Test: kill a worker mid-batch, assert exactly-once side effects.
```

## P23 — Chaos and dedup

```
Read CLAUDE.md and PROGRESS.md. Stage I.

POST /chaos/kill-worker — kills a live worker mid-processing.
POST /chaos/duplicate-flood — replays N recent events with the SAME dedup_key
and idempotency_key but NEW event_ids, exercising the identity model from
docs/DATA_MODEL.md.

Build dedup: a Bloom filter may be used only as a candidate filter, followed by
an exact bounded-set confirmation. Never let a Bloom false positive suppress a
P0 event. Wire the exact decision into ingest.
Expose duplicates_caught.

Dashboard: chaos control panel with KILL WORKER and DUPLICATE FLOOD buttons,
plus a recovery panel showing workers killed, events retried, duplicates
suppressed, and exactly-once violations.

Test: after a duplicate flood of 1000, the sink row count is unchanged.

The kill button is the most memorable ten seconds in our demo. Rehearse it
three times before showing a judge.
```

## P24 — Online cost learner

```
Read CLAUDE.md and PROGRESS.md. Stage I. Our ML component.

Replace the hardcoded per-type cost with a learned estimate in costmodel.py.
Running mean per (type, payload_size bucket), or ridge regression on
payload_size and type, updated from observed service times. Falls back to the
config prior when confidence is low.

This is deliberately load-bearing: cost feeds the value-density term that
drives every ordering decision. It is deliberately NOT a bandit — an exploring
policy could misbehave live in front of judges, and this cannot.

Expose learned vs prior cost per type. Dashboard: a convergence chart with the
config prior as a dotted line.

Demo beat: inject a heavier payload mix mid-run and show the estimate
re-adapting and rerouting.
```

## P25 — Final

```
Read CLAUDE.md and PROGRESS.md. Final prompt.

1. Extend bench/run.py with two configs: adaptive+spike+worker-kill and
   adaptive+spike+duplicate-flood. Report exactly_once_violations as a column
   (0 in every row). Regenerate the report.

2. ADRs 0009-0011: write-ahead checkpoint over full transaction log;
   bloom filter plus LRU over a persistent dedup store; online cost learning
   over static constants and over a bandit.

3. Update docs/ARCHITECTURE.md with the Stage I components.

4. docs/rounds/round-3.md and round-4.md.

5. docs/SUBMISSION.md — one page linking the repo, both architecture views,
   DATA_MODEL.md, the benchmark report, the ADR index, and the round documents.

Then: make test, make bench, update PROGRESS.md, git tag v2-final.
```

---

# APPENDIX — reusable prompts

## R1 — Session resume (run at the start of every new session)

```
Read CLAUDE.md, PLAN.md, and PROGRESS.md.
Run `git log --oneline -15` and `git status`.
Tell me what stage we are in, what exists, and what the next prompt should
build. Do not write any code yet.
```

## R2 — Rollback (when something breaks badly)

```
Something in this stage broke the build and we are short on time.

`git checkout -b rollback/<last-good-tag> <last-good-tag>` onto a new branch.
Confirm `make dev` runs and
`make test` passes. Then tell me exactly what we lose by reverting.

We demo from a working older state, never from a broken newer one.
```

## R3 — Scope refusal (run this on yourself before adding anything)

```
I want to add [feature]. Before I do:
1. Which of the four judged criteria does it improve — progress, code
   originality, data design, or architecture?
2. Can it be done in under 45 minutes?
3. Does it risk breaking anything currently working?
If the answer to 1 is "none" or 2 is "no", tell me not to build it.
```

## R4 — Round document (one hour before each jury round)

```
Write docs/rounds/round-N.md, one page, for judges.
What's new since round N-1 (most significant first) / What we're showing you
today (3 max) / What we know is incomplete (specific — this reads as
engineering maturity) / What's next.
Append `git log --oneline --decorate <prev-tag>..HEAD` as evidence.
```

---

## Three rules for a sequential build

**Never run a prompt before the previous one's acceptance criteria pass.** Sequentially there is no parallel work to hide a broken step behind. One bad commit blocks everything downstream.

**Tag every stage, always.** Nine tags across eighteen hours. Each is a working demo you can fall back to in ten seconds.

**Stage C is the line.** Once `stage-c` is tagged you have a demonstrable project for the rest of the event. Anything that risks breaking it gets built forward from a tag, never on top of an unstable working tree at hour 16.
