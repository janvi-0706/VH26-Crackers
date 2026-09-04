# PULSE — Round 2

## What we have built

Since Round 1 (Stage C: a three-heap EDF queue with no decision logic),
every layer the pitch depends on now actually exists and is real,
measured, wall-clock-timed. `decision.py`'s split ordering/pressure
functions (Stage D) replaced pure priority with a scored routing decision
— STREAM_NOW / MICRO_BATCH / DEFER, chosen from a live pressure signal
computed from real EWMA rates, queue depth, p95 sojourn, and worker
utilisation, never from a per-event term (see
[ADR 0005](../adr/0005-split-ordering-and-pressure-functions.md) for why
that split matters). CoDel (RFC 8289, sojourn-time only, not queue length
— [ADR 0006](../adr/0006-sojourn-aqm-over-queue-length.md)) and a
five-rung escalation ladder (Stage E) added SAMPLE_ROLLUP and SHED for P2
only, with weighted reservoir sampling so sampled volume stays
reconstructable within a tested 5% error bound instead of silently lost
([ADR 0007](../adr/0007-sample-with-weight-instead-of-drop.md)). AIMD
credit-based admission control (Stage F) now throttles the *source*
under pressure — P0's own credit bucket is asymmetric so it is never the
one throttled, matching hard rule 3 exactly. The audit ledger (Stage F)
is real: append-only SQLite, hash-chained
([ADR 0008](../adr/0008-hash-chained-audit-ledger.md)), exportable via
`GET /audit.csv`, with `verify_chain()` proven to catch a changed row, a
deleted row, and a forged row+hash pair together. A headless benchmark
harness (Stage G) runs four 90-second configs plus a 5x/10x/20x/40x
sensitivity sweep and writes `bench/report.md`/`.html`. 948 automated
tests lock every invariant we cite on stage by name
(`tests/test_stage_g_claims.py`). The dashboard (Stage H) now fits a
1920x1080 projector with zero scroll by construction (an explicit
row/column grid, not fixed-pixel guessing), survives a real 5-minute run
with zero WebSocket reconnects, and adds a live cost-comparison panel and
a worker-pool activity grid.

## What we're showing you

1. **The pressure loop closing, live, on one screen.** Adaptive + SPIKE:
   the pressure gauge climbs past ~0.85 at the same moment the
   offered/admitted rate lines visibly separate (admission throttling the
   source) and the per-tier ladder panel shows P2 walking through
   MICRO-BATCH → DEFER → SAMPLE_ROLLUP while P0 never leaves STREAM —
   two independent feedback loops (upstream AIMD, in-flight CoDel/ladder)
   reading the same three sensed numbers, not a scripted animation.
2. **A tamper-evident proof, not a dashboard claim.** Pick any row in the
   live shed log, look it up in the Event Inspector for the full decision
   trace, then download `/audit.csv` — the same hash-chained record,
   independently checkable outside the UI.
3. **We found our own breaking point, not just the one we were asked to
   survive.** The sensitivity sweep shows near-100% SLA attainment at
   5x/10x/20x, then a sharp, arithmetic-explained collapse at 40x (P0's
   own unthrottled demand alone exceeds total capacity) —
   `docs/QA.md`'s "where does the system actually break" answer is a
   measured number, not a guess.
4. **A real bug, found live, fixed live, not smoothed over.**
   `active_workers` (metrics.py's own `in_flight` counter) read 30
   against a 6-worker pool under spike — correct backend behaviour, but a
   worker-pool panel rendering it verbatim as "30/6 busy" would need
   explaining on stage. Fixed to clamp lit cells to pool size and report
   the overflow honestly ("6/6 (+24 waiting)") — the fix and the reason
   for it are in `PROGRESS.md`, not hidden.

## What we know is incomplete

- **`throughput`, `cost_adaptive`, `cost_naive`, `retries`,
  `duplicates_caught`, `exactly_once_violations`, `spike_multiplier`
  remain stub fields, reporting 0** (`metrics.py`'s own module docstring
  names all seven). They were designed into the frozen contract in Stage
  A specifically so no later stage would need to unfreeze it, but no
  stage has implemented them yet — cost comparison is currently derived
  client-side in the dashboard from real `worker_count`/`offered_rate`
  fields instead, which is why `cost_adaptive`/`cost_naive` never needed
  to land to ship Stage H's cost panel.
- **No per-customer/partition ordering enforced.** `partition_key` exists
  on every event and in both the sink's and the deferred buffer's
  indexes, but EDF reordering can still serve one customer's events out
  of arrival order. Deliberately scoped out (`RUNBOOK.md`'s own cut
  list), not half-built — the honest answer to "what's next," per
  `docs/QA.md`.
- **No chaos injection or fault injection.** The pipeline has never been
  tested against a worker crash mid-event, a dropped WebSocket during a
  write, or a corrupted SQLite file — resilience claims stop at "the
  process itself doesn't crash under load," not "the process recovers
  from being killed."
- **No idempotent retry has actually been exercised end-to-end** — the
  five-field identity model and the sink's idempotency-key upsert exist
  and are unit-tested (`test_ingress.py`), but no test replays the same
  `dedup_key` through the *live*, running engine under load the way the
  spike and conservation tests do.

## Next 7 hours

Stage I (if time allows, per `RUNBOOK.md`'s own stretch window,
hours 18-24): implement one or two of the stub fields above rather than
all seven — `cost_adaptive`/`cost_naive` computed server-side would let
`bench/run.py`'s own cost model and the live dashboard share one source
of truth instead of two. Otherwise: freeze, rehearse the demo script
(`docs/DEMO.md`), and stop touching working code. `v1-jury` is tagged at
the end of this round — anything built after it is explicitly framed as
"since the jury tag," never silently folded into what was already
proven to work.

## Evidence: `git log --oneline --decorate stage-c..HEAD`

`stage-c` was never tagged at the time (only `stage-b` was) — retroactively
tagged for this command at `7cee815`, the commit `RUNBOOK.md` itself names
as the Stage C boundary and the exact commit Round 1's own evidence log
already showed as its HEAD.

```
303d623 (HEAD -> main) Stage H: architecture docs, ADRs 0005-0008, README
c96591e Stage H: dashboard final layout, cost + worker-pool panels
4350235 (origin/main, origin/HEAD) Chore: clean dashboard whitespace
85c57fa Stage G: benchmark and dashboard updates
03a82d3 Stage F (dashboard): conservation panel, shed log, event inspector, audit.csv download
e8a6a17 Stage F (ledger): make ledger.py real — hash chain, audit CSV, live invariants
0604791 Stage F: admission.py, credit-based upstream backpressure (AIMD)
b9895c0 Stage E: CoDel, the escalation ladder, and reservoir sampling
7a839d1 Stage D: dashboard pressure gauge, mode-by-tier, backlog chart, live weight sliders
126d576 P11: micro-batching, durable deferral buffer, and the drain-rate/pressure-feedback bugs it exposed
e39db60 UI: add pixel background to dashboard
c2b1557 P10: Stage D split decision function (score/pressure), score-ordered queue
fd3186e Docs: ADRs 0001-0004 and round-1 notes
4d055f7 Docs: add Stage A to C summary
```
