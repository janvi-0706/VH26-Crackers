# 0006 — Sojourn-time AQM (CoDel) instead of queue-length thresholds

## Context

P2 (click, log) is the tier allowed to lose fidelity under pressure —
sampled into a rollup rather than shed outright, per
[ADR 0007](0007-sample-with-weight-instead-of-drop.md). Something has to
decide *when* P2 has been queued long enough to justify that, and the
conventional answer in most hand-rolled queue-management code is a
threshold on queue length ("if depth > N, start shedding").

## Options considered

1. **Queue-length threshold.** Simple, one comparison, but it answers the
   wrong question: a deep queue draining fast is fine for the caller
   waiting on it; a shallow queue where every item still sits too long is
   not. Length alone cannot tell those two states apart.
2. **A fixed latency SLA check per event** (defer/sample the instant one
   event's own sojourn exceeds P2's SLA). Reacts to individual events, not
   sustained congestion — a single slow outlier would trigger it, and a
   queue that is *always* a little slow but never breaches SLA never
   would, missing the actual congestion signal.
3. **CoDel (RFC 8289), applied to sojourn time only, exactly as specified
   for this stage**: track the minimum observed sojourn within each 100ms
   interval; enter a sampling state only once that minimum has stayed
   above a 500ms target for a full interval; exit the instant any single
   observed sojourn drops back below target.

## Decision

Option 3. `codel.py` has exactly one signal — sojourn time, how long an
item actually waited before being dequeued — and no queue-length
threshold appears anywhere in it. The entry/exit asymmetry is RFC 8289's
own design, not invented for this project: slow, confident entry (a full
interval's worth of evidence, so one slow item never triggers it) and
instant exit (congestion clearing is trusted immediately, since
continuing to sample after it clears only costs fidelity for no remaining
reason). `codel.py` decides only the boolean "is P2 currently in a
sampling state"; what sampling actually *does* to an event is
`ladder.py`'s job, kept deliberately separate.

The 100ms interval and 500ms target are RFC 8289's own published
defaults, kept rather than retuned: 100ms is fast enough to react within
this project's own spike ramp-up, and 500ms sits comfortably inside P2's
own SLA range (log: 60s, click: 30s) — CoDel is meant to catch sustained
queueing well before an SLA breach, not to *be* the SLA.

## Consequences

- Caught a real floating-point bug this design exposed that a
  queue-length threshold never would have: at real epoch-scale timestamps
  (`time.time()`, ~1.7×10⁹), `(now + 0.1) - now` measured ~1×10⁻⁷ short of
  `0.1` — invisible at toy-timestamp magnitudes, real at production ones.
  Fixed with a `1×10⁻⁴`s epsilon on the interval-boundary check (three
  orders of magnitude above the measured error, three below the interval
  itself) — documented in `codel.py` rather than silently patched.
- `codel.py` stays a two-function, zero-project-import module (see
  docs/ARCHITECTURE.md's "why the module boundaries are where they are")
  — a duration and a clock reading in, a boolean out — trivially testable
  with a frozen clock and reusable independent of this codebase's own
  `Event`/`Tier` types.
- Cost: this is a deliberate simplification of RFC 8289's full
  drop-scheduling machinery (no adaptive interval shrinking on repeated
  entry), named as such rather than presented as a complete
  implementation — the right amount of CoDel for a 30-hour build, not the
  whole spec.
