# 0007 — Sample-with-weight instead of drop

## Context

Once CoDel signals sustained congestion on P2 (click, log), the pipeline
has to shed load somewhere below capacity. The easy answer is to drop
excess P2 events outright — cheapest possible implementation, and P2's
own tier definition already says it is allowed to lose fidelity. But
"allowed to lose fidelity" is not the same claim as "allowed to lose
count": a click-volume dashboard fed straight-dropped events silently
under-reports traffic with no way to tell by how much.

## Options considered

1. **Drop.** Excess P2 events are discarded, unrecorded, uncounted.
   Simplest possible mechanism; the true event count becomes permanently
   unrecoverable the instant an event is dropped.
2. **Sample a fixed fraction, unweighted** (e.g. keep 1-in-N, forward each
   kept event as itself). Preserves *some* individual events, but a
   consumer counting forwarded events still undercounts true volume by
   the same drop factor, with no field saying by how much.
3. **Reservoir-sample into weighted rollups**: fold every SAMPLE_ROLLUP-
   routed event of one `event_type` into an open window; once the window
   reaches `sample_n` events, emit one `Rollup` row carrying
   `sample_weight = sample_n` and `observed_count` — a single row that
   says "this many real events happened here," not one row standing in
   silently for `sample_n` uncounted ones.

## Decision

Option 3. `ladder.ReservoirSampler` (one instance per P2 `event_type`,
since a rollup row is documented as covering one type) accumulates events
into a window keyed by `(event_type, window_start, window_end)`; on
completion it emits a `Rollup` whose `sample_weight` records exactly how
much real volume that one row represents. `metrics.py`'s
`weighted_click_count` is reconstructed from these weights, not from a
raw forwarded-event count — the dashboard's own click counter is provably
within 5% of true volume under sampling
(`test_weighted_click_count_is_within_5_percent_of_true_click_count_under_sampling`),
which is a claim option 1 or 2 could never make.

Window boundaries are anchored to the *previous* window's own end (plus a
`1e-6`s nudge), not to `now` directly — found necessary by stress-testing
this exact path with a frozen clock: at spike rate, a `sample_n`-sized
window can close within the same tick of the system clock's own
resolution the next one opens in, and two windows sharing a start would
collide against the unique index on `(event_type, window_start,
window_end)` that `docs/DATA_MODEL.md` already specifies to prevent
duplicate window output.

## Consequences

- P2 volume is always reconstructable to within a known, tested error
  bound (5%), even at maximum sampling pressure — a specific, falsifiable
  claim rather than "we sample so it's approximately right."
- One extra field to carry (`sample_weight`) and one extra invariant to
  hold (`observed_count` × implicit multiplier reconciles against
  `sample_weight`) versus plain dropping — accepted because the
  alternative (drop) makes "how much did we actually lose" an
  unanswerable question on stage, which is a worse trade for a system
  whose entire pitch is "we can prove what we did with every event."
- CoDel (ADR 0006) only ever decides *whether* sampling is active; it has
  no opinion about windows, weights, or reservoirs — keeping the
  congestion signal and the sampling mechanism in separate files means
  either could be swapped (a different AQM, a different sampling scheme)
  without touching the other.
