# PULSE — 5-minute demo script

Run `make dev` (dashboard built, engine live) before walking on stage.
Reset (`RESET` button) so the ledger and metrics start clean at 0:00.

## 0:00 — Baseline, all green. State the claim.

Dashboard is up, rate slider near baseline (~16.6 eps), mode = adaptive.
Point at the Conservation panel (✓ BALANCED, large type) and the P0
scoreboard (comfortably under its 200ms target).

> "PULSE survives a 20x traffic spike by triaging, not scaling — one
> Python process, no Kafka, no extra machines. The claim: critical events
> — payments, orders — never get silently dropped, and we can prove it.
> Everything you're about to see is really running, right now, on this
> laptop."

## 0:30 — Naive mode + spike. Everything collapses together.

Click `NAIVE`, then `SPIKE`. Do not touch anything else — let it run
~30-45s.

> "This is the control arm — priority-blind, first-in-first-out. Watch
> P0, P1, P2 climb together, identically. There's no triage happening,
> just one queue getting deeper."

Point at the Latency-by-tier and Queue-depth panels: all three lines
rising together, no separation between tiers.

## 1:30 — Reset. Adaptive + spike. P0 flat, P2 degrades down the ladder.

Click `RESET`, switch to `ADAPTIVE`, click `SPIKE` again.

> "Same spike, same hardware, one difference: adaptive routing."

Narrate live as it plays out:

- P0 scoreboard stays pinned near its SLA target — point at the number,
  not the chart; it's the one that matters and it barely moves.
- Ladder-by-tier panel: P1 moves to MICRO-BATCH, P2 moves through
  MICRO-BATCH → DEFER → SAMPLE_ROLLUP as pressure climbs — the actual
  rung each tier is on, live, not a canned animation.
- Pressure gauge climbing toward and past ~0.85 is the same moment the
  Rates panel's offered/admitted lines separate — admission control
  throttling the source, visibly, not just in the code.

> "P0 never left STREAM. Everything else is being asked to wait, batch,
> or sample — in that order, worst tier first — so the capacity we do
> have goes to what's worth the most per unit of it."

## 3:00 — Conservation, shed log, one decision trace, audit.csv.

- Conservation panel: read the equation out loud once —
  `ingested = processed + in_queue + in_flight + deferred_pending +
  sampled_out + shed`. Still balanced, still green, under a real spike.
- Scroll the Shed log — pick one row, read its reason string out loud
  (it names the exact pressure and rule that fired).
- Copy that row's `event_id` into the Event Inspector, click Look up —
  show the full decision trace: every field, not a summary.
- Click "Download audit.csv" — this is the same proof, exportable,
  independently checkable outside the dashboard, hash-chained
  (`verify_chain()` — mention it, don't demo it live, it's a CLI/test
  concern not a stage one).

> "This isn't a claim on a slide. Every decision this system made is in
> that file, in order, tamper-evident."

## 4:15 — Benchmark table and the sensitivity row.

Switch to (or already have open) `bench/report.html`.

> "We didn't just survive the spike we were asked to handle — we found
> our own breaking point." Point at the 5x/10x/20x/40x sensitivity row:
> "20x, everything's still near 100% SLA attainment. 40x, P0 alone
> demands more than our whole worker pool has — that's arithmetic, not a
> bug, and we can show you the exact number."

## 4:45 — Closing line, plus one honest sentence on what's simulated.

> "Everything you watched — the pressure signal, the admission credits,
> the ladder, the ledger — is real, computed live, from real wall-clock
> timing. The one thing that's simulated is how long a worker takes to
> finish one event: a fixed, disclosed cost model, not measured CPU time,
> so our capacity ceiling is identical on this laptop, on a judge's
> laptop, or in CI. That's the only simulated number in the whole system,
> and now you know exactly which one it is."

Stop talking. Let the dashboard sit on the post-spike adaptive state
while questions start.
