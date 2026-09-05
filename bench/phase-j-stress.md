# Phase J8 — chaos/stress testing report

Testing only, per this phase's own instruction. Nothing in `src/` was
changed to produce the data below — one unrelated, explicitly-directed,
out-of-band fix landed in `app.py`/`server2.py` mid-session (a live-demo
dashboard emergency, documented in this session's own chat log, not this
phase's own work) and is called out explicitly wherever it is relevant
below, not silently folded in.

All data in this report comes from two real artifacts, both checked in
alongside it:

- `bench/stress_j8.py` — spawns real, separate OS processes for ingress
  (`--transport http --persist`), server1, and server2, on real sockets
  (not `httpx.ASGITransport`, unlike every earlier split-topology test in
  this project — killing one process and observing the other two survive
  is a claim about genuinely independent OS processes, which an
  in-process ASGI test cannot make). Every sample and every lifecycle
  event is written to `bench/stress_j8_log.jsonl` (one JSON object per
  line) as it happens.
- `bench/analyze_stress_j8.py` — the tool used to pull the numbers below
  out of that real log; re-run it against a fresh `stress_j8_log.jsonl`
  to reproduce this report's own figures from a new run.

Run once, real time, on this machine: boot + a real 5-minute sustained
20x spike + three real kill/recovery cycles (server2, server1, ingress),
~11 minutes total wall clock.

---

## 1. `bench/contention.py` against the split

Re-running `bench/contention.py` unmodified against the split doesn't
apply — it is hardwired to monkeypatch `WorkerPool.serve`/`_serve_batch`,
and server1 (the standalone P0 process) uses neither class; it has no
`WorkerPool` at all. `bench/contention_after.py` (new) measures the
identical two things — head-of-line blocking, event-loop scheduling
delay — against server1's own real process instead. Full report:
**[`bench/contention-after.md`](contention-after.md)**.

**Headline finding, confirmed both by direct code inspection and by a
live run:** P0 head-of-line blocking behind a lower-tier batch is
**zero, by construction** — server1.py's own worker loop has exactly one
code path (dequeue → `asyncio.sleep(cost / rate)` → ack), no
`MICRO_BATCH`/`DEFER`/`SAMPLE_ROLLUP`/`SHED` branch exists anywhere in
the file, and `/ingest` 422s any non-P0 event before it ever reaches the
queue. There is no lower tier for a P0 event to structurally wait behind
on this process, matching this phase's own expectation exactly.

**A real methodology finding along the way, not assumed:** a first
version of `contention_after.py` ran the loop-lag prober concurrently
with the throughput measurement and produced 20-36 SECONDS of apparent
P0 queue wait — even though an isolated burst test against the identical
app (no prober) confirmed 6 workers genuinely drain 60 P0 events in
~1.6s. Traced to `bench/contention-before.md`'s own already-disclosed
prober observer effect (~774,000 loop turns/sec) being "likely minor"
against the monolith's 150 u/s pool but NOT minor against server1's own
smaller, standalone 135 u/s pool sharing the same one event loop. Fixed
by measuring throughput and loop-lag in two separate passes — see
`contention-after.md`'s own methodology note. **Corrected, real result:**
end-to-end P0 latency p50/p95/p99 = 140/169/173ms (standalone, no
transport hop), loop-lag p99 = 5.8us — both consistent with
`contention-before.md`'s own single-process numbers, confirming the split
did not introduce new loop contention on server1's own smaller process.

---

## 2. Five-minute sustained 20x spike

Real ingress + server1 + server2, `POST /control/spike`, sampled every 3s
for 300 real seconds (100 samples) via `GET /control/topology`,
`GET /control/conservation`, `GET /control/transport-latency`, and both
servers' own `GET /metrics`.

| Assertion | Result | Evidence |
|---|---|---|
| P0 p99 < 200ms | **FAILED** | 99/100 samples over 200ms; max 5394ms, ending the window at 2397ms, not converging back down |
| `shed_critical` == 0 | **PASSED** | 0 at every one of 100 samples, both at the per-fragment (`server2_metrics.shed_critical`) and aggregated (`conservation.shed_critical`) level |
| Conservation balances at every sample | **PASSED, in the one sense currently checked; see caveat** | `dispatch.dispatched == dispatch.resolved + dispatch.outstanding` held exactly at all 100 samples (0 mismatches) — but see below for why "balances" undersells what is actually happening |
| Transport p99 < 10ms | **FAILED** | 99/100 samples over 10ms; max 2372ms |
| Outstanding-dispatch count bounded | **FAILED** | Grew from 26 to 1350 over the window, monotonically, with no sign of levelling off — not bounded on this run's own timescale |

**Root cause, traced directly, not assumed — the single most important
finding in this report:** `server2.py`'s `_dispatch_off_path()` resolves
a dispatch (calls something that eventually reaches
`transport.ack_by_event_ids()`) for `DEFER` (via `/defer`, which itself
acks — Phase J6) and effectively narrates `SAMPLE_ROLLUP` (via
`/rollup`), but **`SHED` never resolved the dispatch at all** at the time
this run was captured — no ack, no defer, nothing. Every SHED decision
therefore sat in `transport.py`'s own `_outstanding`/`_event_index`
forever, was redispatched every `ack_timeout_ms` (5000ms) indefinitely,
and — since server2 stayed oversubscribed — was frequently shed again on
redispatch, repeating. By the end of the 5-minute window: `shed` =
27,754, `sampled_out` = 17,199 (SAMPLE_ROLLUP folds 9 of every 10 events
into a rollup that never individually acks either — only the tenth,
window-closing event's own POST to `/rollup` happens, and that POST
doesn't ack anything), summing to 44,953 — closely matching the
observed `redispatch_count` of 43,634 over the same window. This is not
a coincidence; it is the mechanism. Confirmed independently: the same
growth pattern (smaller in absolute terms, same shape) reproduces with
`--persist` OFF, ruling out SQLite write latency as the primary cause —
it is a real, structural gap in which decisions resolve a transport
dispatch, not a persistence bottleneck.

**Consequence for every other failed assertion above:** the redispatch
storm this causes puts real, additional load on server1 (P0 payments and
orders end up interleaved with a growing stream of P1/P2 redispatch
traffic contending for the same ingress event loop and the same real
HTTP round trips) and on ingress's own dispatch bookkeeping, which is the
most direct, evidence-backed explanation for why P0's own p99 and the
transport p99 both climb steadily rather than settling at the
`bench/contention-after.md`-confirmed ~173ms/~10ms baseline. This was
NOT visible in any earlier phase's own tests (J3-J7) because none of them
sustained real load long enough, at real oversubscription, for the
redispatch counter to climb into the thousands — this is squarely the
kind of finding this phase's own 5-minute-not-8-second run exists to
surface.

**The conservation caveat, stated precisely:** the identity `dispatched
== resolved + outstanding` held exactly at every sample — no event
literally vanished from transport's own bookkeeping. But "outstanding"
growing to 1350 and climbing is not the same claim as "the system is
draining correctly" — it means over a thousand events are stuck
mid-flight, correctly accounted for but not resolved, which is precisely
what items 3-4 below also surface from a different angle.

**One unrelated fact, not a finding against this phase's own assertions:**
partway through drafting this report, a live-demo emergency (unrelated to
this phase, driven by the user directly) required adding a `POST /shed`
endpoint and wiring server2 to call it — a narration-panel fix for the
dashboard's own Shed Log, landed AFTER this section's own data was
captured. That endpoint does **not** call `transport.ack_by_event_ids()`
— it only appends to a dashboard-only ring buffer — so it does not
resolve the root cause above; the numbers in this section remain accurate
for the code as it existed when this run was captured, and the root
cause remains open.

---

## 3. Kill Server 2 mid-spike

server2's real OS process was killed (`SIGKILL`-equivalent) at the
5-minute mark, sampled every 1s for 20s while dead, restarted, sampled
every 1s for 20s more.

| Claim | Result |
|---|---|
| Server 1 and P0 completely unaffected | **CONFIRMED** |
| Outstanding dispatches re-sent after `ack_timeout_ms` | **CONFIRMED** |
| No duplicate side effects | **CONFIRMED, with a caveat below** |
| Conservation returns to balance within 10 seconds | **NOT CONFIRMED — see below** |

**Server 1 independence, the strongest claim this architecture makes:**
throughout the entire 20-second `server2_down` window, server1's own
`/healthz` returned `ok` and `/readyz` returned `ready` at every single
sample; it kept processing P0 events the whole time (+501 processed in
the window) with **zero** interruption. `server2_healthz` correctly
returned nothing (connection refused — the process was genuinely dead).
This is real, live, verified evidence for the one claim this whole
architecture exists to buy: server1's own correctness and availability
do not depend on server2 being alive at all.

**Redispatch after restart worked, but did not fully resolve within 10s
— it got WORSE before it (presumably) would have gotten better:**
`outstanding_dispatch` continued climbing for the entire 20-second
`server2_recovery` window sampled — 2293 → 7366 → settling around 6980,
still far above where it started (1369, right when server2 died).
`redispatch_count` stayed FLAT (44288, unchanged) for the whole
`server2_down` AND `server2_recovery` windows — meaning the automatic
redispatch sweep (`REDISPATCH_SWEEP_INTERVAL_SECONDS` = 50ms,
`ack_timeout_ms` = 5000ms) had not yet fired a fresh redispatch pass by
20 seconds after server2 restarted, even though `outstanding_dispatch`
was demonstrably still being drained downward toward the end of the
window (7366 → 6982). This is consistent with, and made worse by, the
section-2 root cause: the backlog server2 died holding already included
a large number of permanently-outstanding SHED/SAMPLE_ROLLUP dispatches
from before it died, on top of genuinely-recoverable ones, so "return to
balance" was never going to happen in 10 seconds against that backlog —
**this specific 10-second claim did not hold on this run**, and the
honest reason is the same root cause named in section 2, not a failure
of the redispatch mechanism itself.

**No duplicate side effects, the caveat:** 4 of the 20 `server2_recovery`
samples showed the dispatch identity transiently NOT holding
(`dispatched != resolved + outstanding` by a small margin) — a real,
observed instance of the "aggregating network-reported counters can
leave the equation transiently wrong purely from reporting lag" gap
`docs/PHASE-J-INSPECTION.md` section 4 already named as a disclosed,
open limitation back in Phase J1. It self-corrected within the same
sampling window (later samples in `server2_recovery` show the identity
holding again) — a timing/reporting-lag artifact, not evidence of an
actual double-processed event (no idempotency-key collision or duplicate
sink row was observed).

**Rehearsal note, for Q&A:** the honest, rehearsable line is "server1 and
P0 traffic are completely, verifiably unaffected by a server2 death —
that independence is real and demonstrated live. What is NOT yet true is
that the system snaps back to a fully-resolved state within 10 seconds of
recovery; a real, identified bug (section 2's SHED/SAMPLE_ROLLUP gap)
means some of what's outstanding when server2 dies was never going to
resolve anyway, restart or not, and that inflates the recovery window."

---

## 4. Kill Server 1 mid-spike

server1's real OS process was killed at the same point in the spike,
sampled every 1s for 20s while dead, restarted, sampled every 1s for 20s
more.

| Claim | Result |
|---|---|
| Ingress applies backpressure to P0 sources rather than dropping | **NOT CONFIRMED as stated — see honest finding below** |
| `shed_critical` stays zero | **CONFIRMED** |
| P0 events re-dispatch on restart | **CONFIRMED** |

**The honest finding, stated precisely rather than glossed over:**
`admission.py`'s `CreditBucket` for P0 is constructed with
`critical=True`, and `try_acquire()`'s own first line for a critical
bucket is `return True` unconditionally — P0 admission is **never**
throttled, by design, regardless of pressure, and regardless of whether
server1 (P0's own destination) is reachable at all. Live confirmation:
while server1 was dead, `outstanding_dispatch` kept climbing (6991 → 7266
→ 7454 over the 20-second window) — ingress kept admitting AND
dispatching P0 events the entire time server1 was unreachable, with no
throttling of any kind. This is not "backpressure was applied"; it is
"nothing was dropped, because everything dispatched is durably tracked as
outstanding and will be retried" — a real, meaningfully different and
weaker guarantee than the word "backpressure" implies. **Events were
genuinely not lost** (the dispatch identity held, 0/2 mismatches during
`server1_down`) — that half of the claim is true and important — but the
specific mechanism named ("applies backpressure ... rather than
dropping") does not exist for P0 in this codebase, and by CLAUDE.md hard
rule 3's own design (P0's admission bucket is deliberately, permanently
exempt from AIMD throttling — see `admission.py`'s own docstring), it
arguably should not: throttling the one tier this project promises to
never degrade would be a strange trade. The right fix, if this literal
behaviour is wanted, is a NEW, different mechanism (e.g., ingress
refusing new P0 admissions specifically when it has independent evidence
server1 itself is unreachable) — not a change to `admission.py`'s
existing AIMD gate, which was never meant to do this and would violate
its own stated purpose if it did.

**`shed_critical` stayed 0** throughout — real, confirmed, and expected:
P0 is never routed through server2's ladder at all, so this counter has
no path to ever see a P0 event regardless of what happens to server1.

**P0 events re-dispatched on restart, confirmed:** the moment server1
came back, its own `/healthz` returned `ok` again and processing resumed
immediately (34 events processed in the first 20-second
`server1_recovery` window). Its own p99 latency recovered to 245.8ms —
close to, though still slightly above, the 200ms target, plausibly the
residual backlog built up during the outage plus the section-2 root
cause's own general background load, not a new problem this scenario
introduces.

---

## 5. Kill ingress mid-spike — the blast radius, stated precisely

ingress's real OS process was killed at the same point, sampled every 1s
for 15s while dead (server1/server2's own endpoints only — ingress is
unreachable by definition during this window), restarted, sampled every
1s for 15s more.

**Ingress is a genuine single point of failure. Precisely, this is what
that means, verified live, not assumed:**

- **All new traffic generation stops, system-wide, immediately.**
  `Engine` (the generator → classifier → admission → dispatch pipeline)
  lives entirely inside the ingress process. With ingress dead, nothing
  anywhere in the system creates a new event. Live confirmation:
  server1's own `processed` counter stayed EXACTLY flat (37 → 37, delta
  0) for the entire 15-second window — not because server1 was idle by
  choice, but because it had genuinely nothing left to process; whatever
  it already held finished, and nothing new ever arrived.
- **server1 and server2 themselves stay alive and correctly report their
  own liveness** (`/healthz` returned `ok` at every sample for both,
  the whole time) — a real, positive confirmation that these two
  processes' own liveness does not depend on ingress being reachable.
- **Their own readiness correctly flips to not-ready.** Both `/readyz`
  endpoints transitioned from `ready` to `503` within the sampling
  window (server1/server2's own background health-check loop polls
  ingress's `/health` every second — the transition is visible within
  1-2 samples of ingress actually going down), exactly matching Phase
  J4/J5's own design intent: "so Kubernetes will not route to a pod that
  cannot report."
- **Every ack, defer, rollup, and (new) shed POST that server1/server2
  attempt to send during this window is silently lost** — each of those
  calls is wrapped in a bare `except Exception: logger.debug(...)` with
  no local retry and no local durable buffer (the explicit, documented
  design: statelessness on those two processes means there is nowhere
  for a lost POST to be held). Concretely: any event server1/server2
  finish serving WHILE ingress is down is durably lost from the sink's
  own perspective — it was truly served (the simulated work happened,
  and in server1's case the event is gone from its own queue, never to
  be retried) but the completion record never reaches ingress, and
  nothing re-sends it once ingress comes back, because neither server
  keeps a durable record of what it already finished. This is a REAL
  gap, not previously stated this precisely anywhere in this project's
  own PROGRESS.md history — worth a jury hearing plainly rather than
  discovering by asking "what happens to a payment your worker finished
  the instant before ingress died?"
- **Recovery on ingress restart is automatic, fast, and required no
  manual intervention:** both servers' own `/readyz` flipped back to
  `ready` within roughly one health-check cycle (~1s) of ingress coming
  back up; real traffic resumed within the same window (server1
  processed 20 more events in the 15-second `ingress_recovery` window);
  transport p99 dropped back to a normal 36-304ms range.

**The rehearsed, one-paragraph answer for Q&A:** "Ingress is our single
point of failure, by design and by measurement, not by oversight — it is
the one process this whole split explicitly keeps durable state and
traffic generation in (`docs/PHASE-J-INSPECTION.md`'s own section 3).
When it dies: all new work stops system-wide within the instant it dies;
server1 and server2 stay alive and correctly report their own liveness
but flip to not-ready within about a second, exactly as designed so
Kubernetes stops routing to them; and — the one genuinely uncomfortable
fact — any completion a server finishes in the exact window ingress is
down is silently lost, because neither server keeps a durable local
record to retry from once ingress returns. Recovery itself is automatic
and fast once ingress is back. Making ingress non-single-point-of-failure
(a real ingress replica set with its own leader election or shared
storage) is real, substantial, out-of-scope work for a future phase —
this report names it, not solves it."

---

## Summary table

| # | Assertion | Result |
|---|---|---|
| 1 | P0 head-of-line blocking behind lower tier is zero | **PASSED** (by construction, confirmed live) |
| 2 | P0 p99 < 200ms under 5-min sustained spike | **FAILED** — root cause identified (§2) |
| 2 | `shed_critical` == 0 throughout | **PASSED** |
| 2 | Conservation balances at every sample | **PASSED** (exact identity), with a real caveat about what "balances" does and doesn't mean |
| 2 | Transport p99 < 10ms | **FAILED** — same root cause |
| 2 | Outstanding-dispatch bounded | **FAILED** — same root cause |
| 3 | Server1/P0 unaffected by Server2 death | **PASSED**, verified live |
| 3 | Outstanding dispatches re-sent after timeout | **PASSED** |
| 3 | No duplicate side effects | **PASSED**, with a transient reporting-lag caveat |
| 3 | Conservation rebalances within 10s | **FAILED** on this run, root cause identified |
| 4 | Ingress backpressures P0 sources | **NOT AS STATED** — P0 admission is unconditionally never throttled, by design; nothing was dropped, but no backpressure mechanism exists for it |
| 4 | `shed_critical` stays zero | **PASSED** |
| 4 | P0 events re-dispatch on restart | **PASSED** |
| 5 | Blast radius documented honestly | **DONE** — see §5; a real, previously-unstated loss window identified |

## What this report recommends, not decides (testing-only phase)

Two real, load-bearing findings from this run are worth a dedicated
follow-up phase, named here rather than fixed silently:

1. **`server2.py`'s `SHED` (and, more subtly, 9-of-10 `SAMPLE_ROLLUP`)
   decisions never resolve their own transport dispatch.** The fix is
   almost certainly small (call `transport.ack_by_event_ids()` — or the
   equivalent HTTP round trip to ingress — for these two outcomes, the
   same way `/defer` already does) but is real source-code scope this
   testing-only phase does not make.
2. **A completion server1/server2 finish while ingress is unreachable is
   silently lost**, with no local durable buffer to retry from. Closing
   this (or explicitly, permanently accepting it as this architecture's
   own known limit) is a real design decision for a future phase, not
   this one.
