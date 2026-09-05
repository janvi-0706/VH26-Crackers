# Phase J1 — inspection: what breaks when one process becomes three

Inspection only, per this prompt's own instruction. No file under `src/` is
touched by this document — confirmed via `git status --short src/` before and
after writing it. This is the "what exists and what it assumes" survey J2+
implements against; it makes no design decision `docs/adr/` doesn't already
own, and where a real decision is still open, that is said explicitly rather
than picked here.

Target shape, as specified for this phase:

    Ingress  (8000): generator, classifier, admission, dashboard API, WS,
                     history.db writer, deferral store, dispatch tracker
    Server 1 (8001): P0 queue + workers, no batching, fixed capacity
    Server 2 (8002): P1/P2 queues, decision engine, ladder, CoDel, deferral
                     forwarding

Hard constraint this whole document is checked against: **Server 1 and
Server 2 hold no durable state.** Kubernetes reschedules and scales those
pods; anything only in their process memory is gone the instant that
happens. Every durable fact the pipeline depends on has to already live in
ingress (or be re-derivable from something that does) before that pod dies,
not sometime after.

---

## 1–2. Every module assuming single-process shared memory, and where it goes

Every module below currently either (a) holds module-level ("ambient")
mutable state with a comment invoking CLAUDE.md hard rule 1 ("one pipeline,
one process, no locks needed"), or (b) is constructed once per `Engine` and
handed by direct object reference to two or more of generator/queue/worker,
trusting that reference to stay a single shared Python object. Both patterns
break the instant "one process" becomes three: an ambient global becomes
three separate globals (one per process), and a shared object reference
becomes three independent copies with no way to see each other's writes
unless something ships the state across the wire.

| # | Module / structure | What it assumes | Kind | Destination after split |
|---|---|---|---|---|
| 1 | `metrics.py` — `_counters` (`ingested, processed, in_queue, in_flight, sampled_out, shed, true_click_count, retries, exactly_once_violations, duplicates_caught`) | One process increments/decrements these; `snapshot()` reads them synchronously, no lock | Module-level dict, in-memory only | **Split across all three, reassembled at ingress.** See §4 — this is the conservation equation's own home and needs its own analysis, not a single "move it" answer. |
| 2 | `metrics.py` — `_latency_ms`, `_queue_wait_ms` (per-tier `deque(maxlen=4096)`) | Populated by `observe_dequeue`/`observe_complete`, called wherever a worker actually dequeues/completes | Module-level, in-memory, bounded ring buffers | **Server 1 (P0 samples) and Server 2 (P1/P2 samples), each locally** — a percentile does not need to be computed over a globally-merged sample set to be honest; ingress can either (a) have each server report its own local percentiles and display them side-by-side per tier, or (b) have each server periodically ship its raw recent samples to ingress for a merged percentile. Neither is built yet — flagged as an open design question for J2, not decided here. |
| 3 | `metrics.py` — `_queue_depth` (per tier) | Incrementer (`observe_ingest`/`observe_replay`) and decrementer (`observe_dequeue`) run in the same process as whichever queue holds the events | Module-level dict | **Server 1 owns P0's depth, Server 2 owns P1/P2's.** Each server is the sole source of truth for the depth of the queue it physically holds — ingress cannot know this except by asking. |
| 4 | `metrics.py` — `_sla_met` / `_sla_missed` (per tier) | Same as latency — set only at `observe_complete`, i.e. wherever the worker that actually served the event runs | Module-level dict | Split by tier: **Server 1** for P0, **Server 2** for P1/P2. Aggregated at ingress for the dashboard exactly the way §2's percentiles are. |
| 5 | `metrics.py` — `_value_delivered`, `_value_shed` | Same as above | Module-level floats | Split by tier/decision-owner: **Server 1** (P0 delivered value — always delivered, never shed, per hard rule 3), **Server 2** (P1/P2 delivered and shed value). Summed at ingress. |
| 6 | `metrics.py` — `_weighted_click_count` | `observe_complete`/`observe_rollup`, both currently only ever called for a P2 (click) event, which only Server 2 will ever hold | Module-level float | **Server 2 only** — P0/P1 never touch this counter today, so it does not even need Server 1 to know about it. |
| 7 | `metrics.py` — `_ladder_rung` (per tier, most recent decision) | Written at `observe_decision`, wherever that tier's decision is actually made | Module-level dict | Split by tier: **Server 1** writes P0's own rung (always `STREAM`), **Server 2** writes P1/P2's. Ingress reads both to assemble one `MetricsFrame`. |
| 8 | `metrics.py` — `_recent_decisions` (50-item narration deque), `_recent_sheds` (50-item deque) | Populated at the same `observe_decision`/`observe_decision`-with-SHED call sites, in ingest-order within one process | Module-level, bounded, in-memory | **Server 1 and Server 2 each keep their own; ingress merges the two streams by timestamp for the dashboard's narration panel.** A P0 decision and a P1/P2 decision are now made in genuinely different processes, so there is no longer one single ingest-ordered stream to keep without doing that merge somewhere. |
| 9 | `metrics.py` — `_arrival_rate_ewma`, `_service_rate_ewma`, `_offered_rate_ewma`, `_admitted_rate_ewma`, `_pressure_cache` (`current_pressure()`) | Fed by `observe_admission` (ingress, at admission — before an Event exists), `observe_ingest` (wherever `queue.put()` runs), `observe_complete` (wherever the worker runs) | Module-level `_Ewma` instances + a cached float | **This is the single hardest item on this list.** `current_pressure()` is read by: `queue.py` (to order P1/P2 by score — Server 2), `worker.py`/`decision.decide()` (to route P1/P2 — Server 2), `admission.py` (to throttle the source — ingress), and `deferral.py`'s drainer (to decide whether to replay — Server 2, if deferral forwarding stays there, or ingress, if the deferral store itself is the one polling). Pressure's own formula needs `arrival_rate` (ingress-observed), `service_rate` (server-observed, both servers), `p95_sojourn` (server-observed), `worker_util` (server-observed), and `qdepth` (server-observed). **No single process after the split has all five inputs locally any more.** Two real options, neither built: (a) ingress computes ONE global pressure value from data all three processes ship it, and pushes it back down to Server 2/admission on every tick — extra round-trip latency on the hottest path in the system; (b) each of Server 1/Server 2 computes its own LOCAL pressure from what it can see, and admission.py (ingress) gates on whichever is worse, accepting that "pressure" stops being one number and becomes (at least) two. This is exactly the kind of consequence CLAUDE.md's hard rule 1 docstring already warned generically about ("a registry object passed through six constructors would buy nothing... single event loop, no locks") — the multi-process split spends exactly the cost that comment was avoiding. **Flagged, not decided.** |
| 10 | `metrics.py` — `_current_mode` (adaptive/naive) | `Engine.set_mode()` writes it in the same process `queue.py` reads `self.mode` from | Module-level enum | **Ingress is the control plane** (it owns `/control/mode`), but the queue whose selection policy this actually switches lives on Server 2 (naive/adaptive only affects tier-internal ordering, which is only ever P1/P2 today — P0 is always absolute regardless of mode). Ingress must forward the mode change to Server 2 over the batched-HTTP control channel; Server 2 must not durably remember it past its own restart (no durable state rule) — it re-fetches current mode from ingress on startup instead of assuming "adaptive" and waiting to be told otherwise. |
| 11 | `metrics.py` — `_critical_failure_count`, `_critical_failures` (the "P0 got a non-STREAM decision" / "conservation broke" log) | Written by `_record_critical_failure`, called from `_check_p0_never_non_stream` (inside `observe_decision`, wherever a decision is made) and `_check_conservation` (inside `snapshot()`, ingress-only per §4) | Module-level, in-memory, explicitly NOT reset by `/control/reset` (a demo must not be able to quietly erase evidence of a real invariant violation) | **Must move to ingress and become durable**, not stay in-memory anywhere. Today this is the one piece of state in the whole codebase whose entire reason for existing is "must survive a reset" — under the split, it must also survive a pod recycling a stateless Server 1/Server 2, which is a strictly harder version of the same requirement. If `_check_p0_never_non_stream` stays evaluated on Server 1 (the process that actually makes P0's decision), Server 1 must ship every violation to ingress as it happens rather than counting it locally — a locally-counted violation dies with the pod exactly when Kubernetes decides to reschedule it, which is precisely the failure mode this counter exists to catch never being silently invisible. |
| 12 | `metrics.py` — `_replay_admitted_at` (event_id -> re-admission timestamp, for queue-wait accounting on a replay) | Set in `observe_replay` (wherever `put_replayed` runs), read in `observe_dequeue` (same process, same tier's worker) | Module-level dict, self-cleaning (popped by the matching dequeue) | Stays co-located with whichever process actually replays and dequeues P1/P2 work — **Server 2**, entirely local, never needs to leave that process. Not durable by nature (it is explicitly a "one pass through the live queue" bookkeeping value), so the no-durable-state rule does not apply to it — it is fine for it to die with a rescheduled Server 2 pod, exactly as a mid-flight replay's queue-wait measurement for that one event would understandably be lost with it. |
| 13 | `ledger.py` — `_default_ledger` (`SQLiteLedger`: the hash-chained `audit_ledger` table + the 500-item decision-trace ring buffer) | `record()`/`record_trace()` called from `observe_decision()`, i.e. wherever ANY tier's decision is made — currently one process, one chain, one `prev_hash` pointer | Module-level singleton wrapping a SQLite connection, defaults to `:memory:` | **Ingress, unconditionally — this is the audit trail, and durable-state discipline says it cannot live anywhere else.** But the hash chain's own invariant (`prev_hash` = the immediately preceding row's `row_hash`) assumes one single writer appending in strict order. Once P0 decisions (Server 1) and P1/P2 decisions (Server 2) are made in two different processes, EITHER (a) both servers must synchronously call into ingress to append their own ledger row (making ingress a genuine bottleneck/single point of failure for every decision, everywhere), or (b) each server ships decision facts to ingress asynchronously and ingress is the only process that ever actually appends to the chain, accepting that the row order (and therefore the literal `seq` of ledger IDs) reflects arrival-at-ingress order, not true global decision order, under concurrent load from two servers. This is a real design decision K-phase has to make explicitly, not something this split does for free — flagged, not decided. |
| 14 | `deferral.py` — `_default_store` (`DeferralStore`: the `deferred_buffer` SQLite table, `already_deferred` set, drain-rate timestamps) | `defer()` called wherever P1/P2 events are dequeued and deferred (worker.py); `run_drainer()` background task reads `current_pressure()` and calls `replay` (`queue.put_replayed`) in the SAME process | Module-level singleton, `:memory:` by default, explicitly durable ACROSS `/control/reset` | **Ingress — named as such in the target architecture ("deferral store") and required by the no-durable-state rule regardless.** This is the single biggest behavioural change of the whole split: today, `defer()` and the drainer's `replay()` call are two ends of an in-process function call. After the split, Server 2 dequeues a P1/P2 event, decides DEFER, and must now make an HTTP call to ingress to actually store it — and the drainer (which needs `current_pressure()` to decide whether to release a batch, and needs to hand replayed events back into a live queue) either runs IN ingress and calls Server 2 over HTTP to re-inject replayed events, or the drain trigger stays a decision Server 2 makes but the actual buffer contents it is draining live one network hop away. Either shape is new plumbing that does not exist today ("deferral forwarding" in Server 2's own description in this phase's prompt is presumably naming exactly this). `already_deferred` (used by `worker.py`'s redefer-trap: "already deferred once, serve now instead of deferring forever") must be queryable from Server 2 without a local set to check against — another network round-trip on a path that today is a Python `set.__contains__`. |
| 15 | `sink.py` — `_default_sink` (`SQLiteSink`: `events_sink` table, `rollups` table) | `write()` called wherever an event actually finishes serving (worker.py, both P0's and P1/P2's workers, currently the same process); `recent()` read by `Engine.chaos_duplicate_flood` (ingress-owned control endpoint) | Module-level singleton, `:memory:` by default, durable across reset | **Ingress.** Both Server 1 and Server 2 finish events and must ship each completed event to ingress for the upsert-by-idempotency-key write — this is on the critical completion path for every single event in the system, P0 included, so its latency budget matters (P0's SLA is 200ms end-to-end; a synchronous write-then-acknowledge round trip to ingress on every P0 completion eats directly into that budget — batched/async delivery, not a blocking call per event, is presumably part of why this phase's prompt says "connected by batched HTTP"). `recent(n)` (chaos duplicate-flood's own read) already only makes sense against a single durable store, so it staying in ingress is not a new constraint — it already assumed "the" sink is one thing. |
| 16 | `checkpoint.py` — `CheckpointStore` (`in_flight_checkpoint` table) | **Explicitly, by its own docstring, one instance per `WorkerPool`, in-memory, deliberately NOT required to be durable** — "what it protects against is one `asyncio.Task` dying while the surrounding process keeps running... a whole-process crash is a different failure this project does not claim to survive" | Constructor-injected, one per pool (one per process, post-split) | **Stays local to whichever process runs the workers it checkpoints — Server 1 gets its own, Server 2 gets its own.** This is the one piece of state on this whole list that the module's own docstring already anticipated would need to be process-scoped rather than durable, and its documented threat model (a worker task dying, not the whole process) is now the ONLY threat model it protects against, because K6's own graceful-drain work is what has to cover "the whole process (pod) dies" — see §5. This module needs no code change for the split itself, but its safety guarantee measurably shrinks: today "the process" and "the pipeline" are the same thing, so surviving a worker death is surviving nearly every real failure this demo can hit. After the split, "the process" is a stateless, disposable Kubernetes pod that can die as a WHOLE unit far more plausibly than one Python task dying alone inside a long-lived process — and checkpoint.py's own in-memory table is gone the instant that happens, along with everything it was tracking. |
| 17 | `dedup.py` — `Deduplicator` (Bloom filter bit array + bounded exact-set `OrderedDict`) | One instance per `Engine`, checked once at ingest (`Engine._ingest()`, and `Engine.chaos_duplicate_flood()`) — both currently ingress-side operations in the target split too | Per-Engine (not ambient today; matches AdmissionControl's own reasoning: a fresh Engine/reset must not inherit stale dedup state) | **Ingress — already where the target architecture puts "classifier" and "admission", and dedup is logically the same kind of gate: it runs before an event is ever queued anywhere.** No process split is even needed here since dedup never touches Server 1/Server 2 in the current design; it stays exactly where it is architecturally, just inside a smaller process after the split. The Bloom filter's own bit array is in-memory and NOT durable today either — a `/control/reset` already throws it away on purpose. If ingress itself is meant to be restart-safe (the prompt doesn't say it isn't — only Server 1/2 are named as stateless-and-rescheduled), this is worth flagging: a real Kubernetes deployment would presumably still keep ingress as a single long-lived pod (it owns the one SQLite file), so an in-memory Bloom filter surviving only as long as ingress survives is consistent with that, not a new gap the split introduces. |
| 18 | `admission.py` — `AdmissionControl` (`CreditBucket` per tier, AIMD state) | One instance per `Engine`, `try_acquire()` called by `generator.py`, `update_aimd()` fed the live `current_pressure()` value | Per-Engine (already not ambient) | **Ingress**, per the target architecture naming it explicitly. Its live input, `current_pressure()`, is exactly item #9's open problem — admission cannot correctly throttle P1/P2 sources without a pressure signal that now has to travel from wherever it is actually computed (Server 2, mostly) back to ingress on every check. |
| 19 | `codel.py` — `_default` (`CoDelController`, P2 sojourn tracking) | Fed by `metrics.observe_dequeue()` for P2 events, which today happens wherever the P1/P2 worker dequeues — that will be **Server 2** post-split | Module-level ambient singleton | **Server 2, entirely local.** Nothing about CoDel's own control law (interval-min sojourn tracking, an in/out sampling boolean) needs any state ingress or Server 1 ever has to see — `codel.is_sampling()` is only ever consulted by `ladder.escalate()`, itself only ever called for a P2 event, which only exists on Server 2. This module needs no cross-process plumbing at all, as long as it moves wholesale to Server 2 rather than staying "ambient" against a smaller process that no longer has the P1/P2 traffic to feed it. |
| 20 | `ladder.py` — `_samplers` (two `ReservoirSampler`s: click, log) | Fed via `add_to_reservoir()`, called only from `worker.py`'s SAMPLE_ROLLUP path — P2 only | Module-level ambient dict | **Server 2, entirely local**, same reasoning as CoDel. A finished window is written durably via `sink.write_rollup()` (ingress, §15) and counted via `metrics.observe_rollup()` (the weighted-click-count counter, §6, also Server 2-local) — so the OPEN, in-progress window (partial count toward the next rollup) is the only in-memory state here, and losing an unfinished window (up to 9 events' worth, per `RESERVOIR_N`) if Server 2 dies mid-window is a bounded, already-accepted-as-honest loss under the CURRENT design (see item 5 in the DATA_MODEL doc's own accounting) — the split does not make this worse, it just changes WHICH kind of process death can trigger it (today: whole pipeline restart; tomorrow: a Server 2 pod reschedule, plausibly far more frequent). |
| 21 | `decision.py` — `current_score_weights`, `current_pressure_weights` (the six live-tunable dashboard-slider weights) | Read by `queue.py` (`_score()`, scoring P1/P2 — Server 2) and `metrics.py` (`_compute_pressure()`, computed wherever pressure is computed — see #9) and written by `/control/weights` (ingress) | Module-level, no lock, "single event loop" | **Ingress is the control plane** (owns the POST endpoint) but the values are consumed on Server 2 (score) and wherever pressure ends up being computed (#9). Same "ingress must push, or server must poll/pull-on-demand" shape as `_current_mode` (#10) — this is genuinely the same open plumbing problem appearing a third time (mode, weights, pressure), suggesting J2/J3 may want ONE mechanism (a small control-state broadcast from ingress to both servers) rather than three separate one-off ones. |
| 22 | `costmodel.py` — `CostModel` (per-`Engine` learned running estimates, keyed by `(EventType, payload bucket)`) | One instance, constructor-shared by `EventQueue` (ordering — reads `estimate()`) and `WorkerPool` (routing — reads `estimate()`; learning — writes via `observe()` at every completion) so the two "never disagree about the current estimate" (the module's own stated invariant) | Shared object reference within one `Engine`, in-memory, not durable (reset by `/control/reset`) | **Splits by concern, and the split-brain risk is real:** ordering (`EventQueue._score`) happens wherever the queue lives (P0's queue → Server 1; P1/P2's queue → Server 2), and observation (`WorkerPool.serve`/`_serve_batch`, fed by real completions) happens wherever the worker that actually finished the event runs — the SAME two processes. So each of Server 1 and Server 2 could plausibly keep its own local, type-scoped `CostModel` (P0's own cost distribution learned on Server 1, P1/P2's on Server 2) — this is likely FINE, not a bug, because the model is already keyed by `EventType`, and P0 event types (payment, order) never appear on Server 2, nor do P1/P2 types (inventory, click, log) ever appear on Server 1. **No cross-process synchronization is actually required here as long as each server only ever asks about the types it itself handles** — worth confirming explicitly in J2 rather than assumed, since the current single-`Engine` design's own docstring treats "one shared instance" as load-bearing, and that reasoning needs to be re-derived for the split, not just carried over by habit. Not durable either way — `/control/reset`'s own reset-to-unlearned behavior is unaffected. |
| 23 | `worker.py` — `WorkerPool` itself (`served_count`, `batched_count`, `deferred_count`, `sampled_count`, `shed_count`, `recovered_count`) | Per-pool counters, explicitly "observability for tests... not in MetricsFrame" | In-memory, per-`WorkerPool` | Not dashboard-visible today and not part of the conservation equation — **stays wherever its owning pool ends up** (Server 1 for the P0 pool's own counters, Server 2 for P1/P2's), with no cross-process requirement. Flagged only for completeness. |

**Summary shape of the answer to Q2:** everything durable (the audit
ledger, the deferred buffer, the terminal sink, the eventual `history.db`)
goes to **ingress**, no exceptions — that is the hard constraint, restated
as a rule for every row in the table above. Everything ephemeral and
tier-scoped (queue contents, in-flight checkpoints, per-tier percentile
samples, CoDel/ladder state) stays on **whichever server physically holds
that tier's traffic** — Server 1 for P0, Server 2 for P1/P2 — and is allowed
to vanish when that server's pod is rescheduled, because nothing downstream
depends on it surviving that. The one genuinely unresolved category is
**live control-loop signals that have to flow the "wrong" way** — pressure
computed from server-side facts but consumed by ingress-side admission
control, and control values (mode, weights) set at ingress but consumed
server-side — items #9, #10, #18, #21. Those four are the actual hard
part of this phase, not a "just move the code" exercise, and none of them
are decided in this document.

---

## 3. Tables staying in ingress's one SQLite file

Every table this codebase currently defines lives in exactly one of three
ambient SQLite-backed modules, every one of which already defaults to
`:memory:` and is described in its own docstring as "the single-process
durable edge":

| Table | Owning module | DDL location |
|---|---|---|
| `audit_ledger` | `ledger.py` | `ledger.py:60` |
| `deferred_buffer` | `deferral.py` | `deferral.py:40` |
| `events_sink` | `sink.py` | `sink.py:29` |
| `rollups` | `sink.py` | `sink.py:52` |
| `in_flight_checkpoint` | `checkpoint.py` | `checkpoint.py:104` |

The first four are unconditionally durable-by-design and unconditionally
belong in ingress under the no-durable-state rule (§2, items 13–15) — no
other process needs write access to any of them, and none of the three
currently takes a `path` argument that isn't already `":memory:"` by
default (a real deployment would point them at a file; that file lives
wherever ingress runs, nowhere else, since ingress is the only process the
target architecture names as durable).

`in_flight_checkpoint` is the one exception worth naming explicitly rather
than silently folding into "everything moves to ingress": per §2 item 16,
this table's whole design is deliberately NOT meant to be durable or
shared — one instance per `WorkerPool`, scoped to a single process's own
worker_ids, explicitly built to protect against a worker task dying while
"the surrounding process keeps running." Under the split this table
correctly becomes **two** tables, one inside Server 1's own process memory
and one inside Server 2's — neither should live in ingress's SQLite file,
and neither needs write access to any table that does. The reason this
matters for Q5 is that this table's own documented safety guarantee (survive
a task dying, not a process dying) is exactly the gap K6's graceful drain
has to close, since a rescheduled Server 1/2 pod IS the "whole process
dies" case this table's own docstring says it was never built to survive.

**Confirmed: no process other than ingress needs write access to any
durable table.** Server 1 and Server 2 need to WRITE completed events, audit
decisions, and deferred events — durably — but per the hard constraint they
do this by sending the fact to ingress over the batched-HTTP channel and
letting ingress perform the actual SQLite write; they hold no local
connection to ingress's database file. This is a real, load-bearing
consequence of the no-durable-state rule that is worth stating plainly: it
turns what is today an in-process function call (`sink.write(event)`,
`deferral.defer(event, reason)`, `ledger.record(...)`) into a network call
on the hot path for every single event, P0 included — see §2 items 13–15
for why that specifically matters for P0's 200ms SLA budget, and why
"batched HTTP" rather than "one HTTP call per event" is presumably this
phase's own answer to that cost, though the actual batching design is not
specified by this inspection and is J2/J3's to make.

---

## 4. The conservation equation across three processes

The equation, unchanged since Stage A (`metrics._check_conservation`,
`metrics.py:224`, and `docs/DATA_MODEL.md`'s own copy):

    ingested == processed + in_queue + in_flight + deferred_pending
                + sampled_out + shed

Seven counters, checked on every `snapshot()` call (today: 4Hz, ingress-side,
since `/ws` and `snapshot()` both already live in what becomes the ingress
process). Where each one lives after the split, and why:

| Counter | Current owner | Post-split owner | Why |
|---|---|---|---|
| `ingested` | `metrics._counters["ingested"]`, incremented in `observe_ingest` — called from `EventQueue.put()`/`put_nowait()`, i.e. wherever an event is FIRST admitted to a live queue | **Ingress** | The classifier and admission both run in ingress; an event becomes "ingested" the moment ingress decides to hand it to a queue, before that queue's own process (Server 1 or 2) has necessarily even received it over the wire. Ingress can and should count this the instant it dispatches the event, independent of whether the downstream server's own receipt-acknowledgement has come back yet — otherwise "ingested" would mean "acknowledged by a downstream process," a materially different (and network-latency-dependent) definition than it has today. |
| `processed` | `_counters["processed"]`, incremented in `observe_complete` — called from `WorkerPool.serve()`/`_serve_batch()`, wherever the worker that finishes the event actually runs | **Server 1 (P0) and Server 2 (P1/P2), locally, then reported to ingress** | This is inherently split by construction — P0 completions physically happen on Server 1, P1/P2 completions on Server 2. Neither server can be the sole "processed" counter for the whole system; ingress has to sum both. |
| `in_queue` | `_counters["in_queue"]`, +1 on `observe_ingest`/`observe_replay`, -1 on `observe_dequeue` — tracks live queue depth wherever the queue object itself lives | **Server 1 reports P0's; Server 2 reports P1/P2's; ingress sums** | Same reasoning as `_queue_depth` (§2 item 3) — this is the SAME live number `_queue_depth` already tracks per-tier, just pooled across tiers. Ingress cannot know queue depth except by asking the process that actually holds the queue. |
| `in_flight` | `_counters["in_flight"]`, +1 on `observe_dequeue`, -1 on `observe_complete`/`observe_defer`/`observe_retry` — tracks events a worker currently holds | **Server 1 and Server 2, locally, summed at ingress** | Same shape as `processed` — inherently split, since "in flight" means "a worker somewhere is holding it," and workers are now on two different servers. |
| `deferred_pending` | **Not a resettable in-memory counter today** — already sourced live from `deferral.pending_count()`, i.e. read straight off the `deferred_buffer` table's own row count, specifically because a resettable counter would go stale the moment `/control/reset` clears `_counters` while the durable buffer still holds real rows (`metrics.py`'s own docstring, quoted almost verbatim in `deferral.py`'s header) | **Ingress — unchanged in spirit, and actually the EASIEST of the seven post-split**, because the deferred buffer already lives in ingress (§2 item 14, §3) regardless of which process decided to defer the event | This is the one counter in the whole equation that the original single-process design already treats as "read live from the store of record" rather than "increment a local variable" — that design choice turns out to be exactly right for a multi-process split it was not written anticipating, because the store of record was always going to be ingress-resident. |
| `sampled_out` | `_counters["sampled_out"]`, incremented in `observe_decision` when the decision is `SAMPLE_ROLLUP` — only ever a P2 outcome | **Server 2 only, reported to ingress** | Never happens on Server 1 (P0 is never sampled — hard rule 3) or ingress itself (ingress never makes a routing decision, per the target architecture's own module list). Single-owner, simplest case in the table. |
| `shed` | `_counters["shed"]`, incremented in `observe_decision` when the decision is `SHED` — only ever a P2 outcome | **Server 2 only, reported to ingress** | Same reasoning as `sampled_out` — never P0, never ingress. |

**How the dashboard computes the invariant across three processes, with
Server 2 potentially having multiple instances:** the equation's left side
(`ingested`) and the `deferred_pending` term are already exclusively
ingress-resident and need no aggregation across server instances at all.
The remaining five terms (`processed`, `in_queue`, `in_flight`,
`sampled_out`, `shed`) are each **owned by exactly one tier's worth of
processing** — P0 always Server 1, P1/P2 always whichever Server 2
instance(s) currently exist — so the correct aggregation is a **sum over
every live Server 2 instance's own locally-reported value for that term**,
plus Server 1's own single value where applicable (`processed`/`in_queue`/
`in_flight` only — P0 is never sampled or shed).

Concretely, if Server 2 is scaled to N instances, ingress needs, per
snapshot:

    processed  = server1.processed + sum(server2[i].processed for i in 1..N)
    in_queue   = server1.in_queue  + sum(server2[i].in_queue  for i in 1..N)
    in_flight  = server1.in_flight + sum(server2[i].in_flight for i in 1..N)
    sampled_out = sum(server2[i].sampled_out for i in 1..N)
    shed        = sum(server2[i].shed        for i in 1..N)

This requires ingress to know, at snapshot time, the current live set of
Server 2 instances and to have each one's most recent self-report on hand
— which is new infrastructure the current single-process design has no
analogue for at all (there has only ever been one worker pool per tier to
ask). Two real shapes for that new infrastructure, not decided here: (a)
each Server 2 instance pushes its own delta/counters to ingress on a fixed
cadence (matching the "batched HTTP" theme already named for this phase),
and ingress keeps the last-received value per known instance, discarding
an instance's contribution if it hasn't reported within some staleness
window (needed so a rescheduled/dead instance's LAST number doesn't get
double-counted once its replacement starts reporting too); or (b) ingress
polls a registry of live instances synchronously before computing each
snapshot, which reintroduces exactly the kind of per-tick blocking-call
cost `metrics.py`'s own `_PRESSURE_REFRESH_SECONDS` comment already warned
against once for a much cheaper operation (sorting an in-memory deque) than
a network round-trip to N processes would be.

**A second, real risk this analysis surfaces**: `_check_conservation`
today runs synchronously inside `snapshot()`, reading `_counters` values
that are guaranteed self-consistent because nothing else can be mutating
them mid-read (single event loop). Once the seven terms are five separate
network-reported numbers from up to `N+2` processes, each with its own
report cadence and its own possible staleness, the equation can go
**transiently unbalanced for reasons that have nothing to do with an actual
bug** — Server 2 instance #3 reported 30ms ago, instance #1 reported 400ms
ago, and an event mid-flight between "dequeued on instance #1" and "counted
processed on instance #1's own next report" will, for that window, appear
to have vanished from the equation's right-hand side even though it is
correctly, safely sitting in that instance's own local `in_flight` counter
the whole time. Distinguishing "genuinely lost" from "reporting lag" is a
real design problem for whatever replaces `_check_conservation`'s current
single-process assertion, and is not solved by this document — flagged for
J2/J3, since it directly affects how much the split can compromise
CLAUDE.md's own "critical events are never silently dropped, and we can
prove it" claim if not handled deliberately.

---

## 5. What breaks if a Server 2 instance dies mid-processing

Trace the concrete sequence, mapped onto the module-level mechanisms that
already exist today (§2) and what they do and do not cover once "the
process" is a disposable pod rather than the whole pipeline:

1. **An event is dequeued from Server 2's live P1/P2 queue.** Today,
   `EventQueue.get()`/`try_get()` calls `metrics.observe_dequeue()`, which
   moves the event's accounting from `in_queue` to `in_flight` (§4). Under
   the split, this happens entirely within Server 2's own local counters —
   nothing has been reported to ingress about this specific event yet
   (ingress already counted it once, as `ingested`, back when it was first
   admitted).

2. **`worker.py` calls `checkpoint.begin(event, worker_id)`** — write-ahead,
   into Server 2's OWN in-memory `CheckpointStore` (§2 item 16), BEFORE the
   simulated service-time `await asyncio.sleep(...)` starts. This is the
   sole record, anywhere in the system, that this specific event was ever
   dequeued and is currently being served — it does not exist in ingress,
   in the durable `deferred_buffer`, or anywhere else. It only exists in
   this one Server 2 instance's process memory.

3. **The instance dies right here** — during the simulated sleep, which is
   the only `await` point a real cancellation/crash can land inside (per
   `checkpoint.py`'s own docstring, quoted in §2 item 16). This is exactly
   the scenario the prompt asks about.

**What is lost, precisely:**

- **The in-flight checkpoint row itself.** Today, `WorkerPool._on_worker_done`
  runs in the SAME process the moment the worker task ends, calls
  `_recover_worker()`, which reads `CheckpointStore.recover_worker(worker_id)`
  — a table that still exists, in the same process's memory, because only
  the one `asyncio.Task` died, not the process hosting it. **Under the
  split, the whole Kubernetes pod is what died — there is no surviving
  process left to run `_on_worker_done`, and no surviving `CheckpointStore`
  to query.** The checkpoint row simply ceases to exist along with
  everything else in that pod's memory. Nothing recovers it, because the
  only thing that ever knew this event was in flight is gone.
- **The event itself, as a business fact still needing service.** It was
  already removed from Server 2's live queue in step 1 (dequeue is
  destructive — `EventQueue._pop_best_by_score`/`_take_naive` both actually
  remove the item from `_settled`/`_pending`). It was never durably
  persisted anywhere in its "about to be served" state — `deferral.py`'s
  durable buffer only ever receives an event on an explicit DEFER decision,
  which this event did not receive (it was routed STREAM_NOW/MICRO_BATCH,
  which is why it reached `serve()`/`_serve_batch()` and `checkpoint.begin()`
  at all). **The result is a genuine, silent, permanent loss of one
  business event** — not an SLA miss recorded anywhere (nothing ever calls
  `observe_complete` or `sink.write()` for it, so it never reaches the
  terminal sink either), not a shed recorded in the ledger (SHED is a
  decision this event was never given), just an event that was ingested,
  counted, dequeued, and then vanished.
- **The conservation equation itself goes wrong, and wrong in a way nothing
  currently catches.** `ingested` already counted this event (ingress,
  at admission). But it will never become `processed` (worker never
  finished), it is no longer `in_queue` (already dequeued before the
  death), the `in_flight` counter that was tracking it lived only in the
  now-dead process's memory and dies with it (never decremented by an
  `observe_complete`, `observe_defer`, or `observe_retry` call, because
  none of those ever ran for this event) — so the ONE process that still
  claims to be counting it (ingress, aggregating Server 2 instance reports
  per §4) never receives a report that would remove it from `in_flight`
  either, meaning ingress's own aggregated `in_flight` figure could
  **overcount forever**, or, if ingress applies a staleness timeout to a
  dead instance's last-known numbers (one candidate design floated in §4),
  the count could simply be dropped along with the rest of that instance's
  contribution, at which point the equation silently rebalances around a
  hole that used to be one real event — the exact "silently drop, but the
  arithmetic still adds up" failure this project's own CLAUDE.md hard rule
  3 was written to make impossible for P0, and which nothing in the design
  described in this document currently makes impossible for P1/P2 either
  once a whole Server 2 pod, rather than one worker task inside a
  surviving process, is what can die.
- **Everything already in a batch with this event, if it died inside
  `_serve_batch()` rather than `serve()`.** Today, `_serve_batch()`'s own
  per-member `checkpoint.begin()` calls (one per batch member, before the
  ONE shared `await asyncio.sleep()`) mean a worker death recovers exactly
  the unfinished members, never the ones already fully served (§2 item 16,
  and `worker.py`'s own extensive docstring on this). That precision is
  entirely a property of the SAME process surviving to run the recovery
  logic after the task dies. With the whole process gone, this precision
  buys nothing — every member of that batch that had not yet reached its
  own `mark_done()` at the moment of death is lost identically, whether it
  was the first member checkpointed or the last.

**This is exactly the gap this phase's prompt names as the reason J3 and
K6 exist**, and this document treats their existence as the acknowledged
answer rather than proposing one of its own:

- **J3's dispatch tracking** is presumably the mechanism that lets ingress
  (not Server 2) durably know "event X was dispatched to server-2-instance-3
  at time T and has not yet been acknowledged as complete" — the write-ahead
  half of exactly-once, moved from a process-local SQLite table
  (`checkpoint.py`, today) to ingress's own durable state, which is the only
  state guaranteed to survive a Server 2 pod dying. Without it, the loss
  traced above is total and undetectable; with it, ingress at minimum knows
  such an event existed and was in flight when its dispatch target stopped
  reporting, which is the precondition for recovering it at all (by
  redispatching to a different, live Server 2 instance) rather than merely
  noticing after the fact that the books no longer balance.
- **K6's graceful drain** is presumably what keeps this scenario rare in
  the FIRST place — a pod given time to finish its own in-flight work and
  stop accepting new work before Kubernetes actually kills it, rather than
  being killed while `checkpoint.begin()` rows still exist for events mid-
  service. Graceful drain narrows the WINDOW during which the loss traced
  above can happen (a hard `.cancel()`/SIGKILL mid-sleep vs. a clean
  shutdown that waits out any in-flight `asyncio.sleep`s first); it does
  not, by itself, make the loss impossible for the genuinely ungraceful
  case (a node failure, an OOM kill, anything that does not go through a
  clean shutdown sequence at all) — which is presumably exactly why J3's
  own durable dispatch tracking still has to exist as the real backstop
  rather than graceful drain alone being treated as sufficient.

This document does not attempt to design either mechanism — that is
explicitly out of this inspection prompt's own scope, and is named here
only to state precisely, with the actual current code as evidence, why
each is necessary rather than optional.
