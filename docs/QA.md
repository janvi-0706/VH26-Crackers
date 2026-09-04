# PULSE — jury Q&A

Answers under 100 words each. Every claim here is backed by a specific
file or test cited in parentheses — ask and we'll open it.

## Why not Kafka?

At ~333 events/sec, a broker adds operational surface — topics, offsets,
consumer lag, another process to keep alive — without adding any
scheduling intelligence; Kafka doesn't know what EDF or an aging guard
is, so we'd write that logic ourselves on top of it anyway. We chose to
spend our hours on the decision logic that's actually judged (originality
score), not on plumbing that looks production-grade but adds nothing. One
process also makes the capacity ceiling deterministic on any judge's
machine, zero setup. Full reasoning: [ADR 0001](adr/0001-in-process-asyncio-over-kafka.md).

## Is the processing real?

Everything is real except one number: how long a worker takes to finish
one event. Generation, classification, ordering, pressure, admission,
CoDel, the ladder, the audit ledger — all real, measured, wall-clock.
Service time is `asyncio.sleep(cost / capacity)`, a disclosed, config-
driven number, so the 150 u/s capacity ceiling is identical on any
machine rather than a benchmark result nobody can reproduce
([ADR 0002](adr/0002-simulated-service-cost.md), README's "what is real
vs. simulated" table).

## What if critical events alone exceed capacity?

We measured this rather than guessing. P0 admission is unconditional
(hard rule 3), so at 40x spike, P0's own organic demand alone (~216 u/s)
already exceeds the entire 150 u/s pool — no scheduler can serve more
than physically exists to serve. P0 SLA attainment collapses to 5.6% at
that point (`bench/report.md`'s sensitivity sweep). At 5x/10x/20x, P0/P1/P2
all sit near 100% — there's real headroom, then a sharp, well-understood
cliff, not a slow decline.

## What about per-customer ordering when you reorder by priority?

`partition_key` (normally a customer) is already in every event and in
both the sink's and deferred buffer's indexes — the field exists so
per-partition ordering can be added without a schema change. We do not
enforce it today: EDF reordering can serve one customer's events out of
arrival order. Deliberately scoped out (RUNBOOK's own cut list) rather
than half-built; the honest answer to "what's next," not a gap we're
hiding.

## How did you pick the weights?

Engineering judgment, not fitted from data — we have no production
traffic to fit against. `w1=0.7/w2=0.3` biases ordering toward
value-density-and-urgency over pure aging, so the queue isn't just FIFO
with extra steps, while `w2>0` guarantees no event waits forever. Pressure's
weights (`a=0.35,b=0.35,c=0.20,d=0.10`) favor the two leading indicators
(queue saturation, arrival/service ratio) over the two lagging ones
(sojourn, worker util). Live-tunable via dashboard sliders
(`/control/weights`) for exactly this question.

## How is your ordering function different from a priority queue?

A priority queue sorts on a key frozen at insertion. Our `score()` is
recomputed fresh at *every* dequeue from `now` — urgency and aging both
grow as real time passes, so an event's rank changes while it waits
without anyone touching it. A static heap key would freeze aging at zero
forever. The cost is real (O(n) rescan per dequeue, `queue.py`), accepted
because a stale priority is a correctness bug at this system's scale, not
just a performance nit.

## Where does the system actually break?

At 40x offered load (not the specified 20x), from pure arithmetic: P0's
own admission is never throttled, and P0's demand alone at 40x exceeds
total worker capacity. This is the honest, measured breaking point from
our own sensitivity sweep (`bench/report.md`, 5x/10x/20x/40x), not a
hand-wave — we know exactly where headroom ends (~20x) and where physics
takes over (40x), and can show the number live.
