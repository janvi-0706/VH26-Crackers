# PULSE — Round 1

## What we have built

An event pipeline that survives a 20x traffic spike (16.65 → 333 events/sec)
by triaging, not scaling: a single asyncio process, no Kafka/Redis/Celery,
with a hand-written three-tier priority scheduler in front of a fixed
6-worker pool. Every event carries a business value, a tier, and an SLA
deadline, all assigned from an externalized, calibration-checked config
(`config/tiers.yaml`) — never hardcoded. The five-field identity model
(`event_id` / `dedup_key` / `seq` / `partition_key` / `idempotency_key`) is
frozen and documented (`docs/DATA_MODEL.md`), and the full `MetricsFrame`
contract was designed complete in Stage A so the dashboard was built and
demoable before the scheduler existed. A live control bar (rate slider,
naive/adaptive toggle, SPIKE, RESET) drives the same backend a judge can
also hit directly over HTTP. 111 automated tests pass, including invariant
tests for EDF ordering, the P2 aging guard, and P0's absolute priority under
sustained overload.

## What we're showing you

1. **P0 stays flat while P2 climbs, live, on one screen.** Hit SPIKE in
   adaptive mode: P0's p99 scoreboard holds near its 200ms target while the
   per-tier latency and queue-depth charts show P2 absorbing the entire
   backlog. Switch to naive and repeat: all three tiers climb together,
   identically — the control arm that proves the scheduler is doing
   something, not just labeled differently.
2. **EDF, not a lookup table.** A payment that arrived 2ms ago with a 200ms
   SLA is dequeued *behind* an order at 400ms of its 500ms SLA — ordering by
   actual deadline proximity, not type or arrival order (`test_queue.py`).
3. **A real bug, found and fixed by testing the demo itself, not just unit
   tests.** Verifying "P0 stays flat" live exposed a scheduling bug where a
   starvation guard for P2 could — under sustained load only — preempt P0
   entirely. Root-caused, fixed, and locked with two new invariant tests
   before this round. We can walk through exactly how and why it happened.

## What we know is incomplete

- **No adaptive decision function yet.** Routing is priority-order only;
  there is no scored decision (batch/defer/sample/shed) and no pressure
  signal — that is Stage D, not started.
- **No backpressure, no admission control, no durable ledger.** The audit
  ledger is an in-memory stub with call sites wired but no hash chain yet;
  upstream throttling and CoDel-style sojourn control land in Stage E.
- **P0's own latency floor is worker contention, not zero.** Under a
  *sustained* spike, P0 holds flat around its cost-model service time
  (~250-420ms) rather than the SLA target exactly — because 6 non-preemptive
  workers can be 100% busy when a P0 event arrives. The *queue* never
  misorders P0 (`queue_depth.P0` stays ≈0 throughout); the residual latency
  is the reason Stage D exists.
- **No idempotent retry, chaos injection, or dedup** — Stage H/I.

## Next 8 hours

Stage D: the split ordering/pressure decision function, wired into the
queue in place of pure priority; per-event decision reasons visible on the
dashboard; the pressure signal driving batching for P1 before it drives
shedding for P2.

## Evidence: `git log --oneline --decorate`

```
7cee815 (HEAD -> main, origin/main, origin/HEAD) P8: control endpoints, per-tier percentile proof, dashboard control bar
81b0cbe Docs: add jury Q and A guide
120125f Docs: summarize stages A through C
f59a2c7 P7: three-heap priority queue (EDF within P0, bounded P2 aging guard, naive lock)
de6177c (tag: stage-b) P6 follow-up: install Node, fix build config, verify acceptance line
4ab1fce P6: dashboard scaffold
6908c4a P5: queue, workers, app
40ec841 P4: ingress and sink
9322be9 P2: document data model and contract gaps
be8104b Merge remote history and preserve hackathon strategy
78641da Stage A: freeze contracts and metrics foundation
ef42f9f Scaffold: repo skeleton for PULSE adaptive event pipeline
aff55df Add files via upload
1e7b59f Add files via upload
```
