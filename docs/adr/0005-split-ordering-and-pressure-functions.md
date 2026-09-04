# 0005 — Split ordering and pressure functions instead of one additive score

## Context

Every event needs two different questions answered: *which event goes
next within its tier* (ordering) and *what should we do with this event
given how loaded the system is right now* (routing/mode). The obvious
one-function design folds both into a single score, e.g.
`total = w1*density*urgency + w2*aging + w3*pressure`, and picks the
highest-scoring event.

## Options considered

1. **One additive score, pressure included as a term.** Simplest surface
   area — one function, one number, one comparison.
2. **One score, pressure included multiplicatively instead of additively**
   (`total = base_score * (1 + pressure)`). Avoids the specific failure of
   option 1 below, but still couples two different questions into one
   return value, and still recomputes system-global pressure once per
   *event* instead of once per *tick*.
3. **Two pure functions: `score(event, now, capacity)` for ordering only,
   `pressure(signals)` for system state only. `decide()` consumes
   `pressure`'s output as a routing input, never as a scoring term.**

## Decision

Option 3. `score()` takes only per-event properties (density × urgency,
aging) and never looks at system load; `pressure()` takes only
system-global signals (queue utilization, arrival/service ratio, p95
sojourn ÷ SLA, worker utilization) and never looks at any individual
event. `decide()` is the one function allowed to consume both, and it
uses pressure to choose a **mode** (STREAM_NOW / MICRO_BATCH / DEFER),
never to adjust an event's rank against its own tier-mates.

The reason option 1 is actually broken, not just less clean: pressure is
one scalar, identical for every event compared against every other event
at the same instant. For any two events A and B in the same comparison,

```
(score_A + P) > (score_B + P)  ⟺  score_A > score_B
```

P cancels out of every pairwise comparison it is added into. The result
*looks* load-aware — the number on screen changes when pressure changes —
but it has literally zero effect on which event is chosen next. That is
the standard mistake this design exists to avoid, and it is why option 2
(multiplicative) doesn't fully save option 1 either: it stops the
constant from canceling, but it is still one function trying to answer
two independently-testable questions with one return value, which is the
deeper problem being split apart here.

## Consequences

- `score()` is pure and stateless with zero project imports beyond
  `contracts` — trivially unit-testable against fixed events with no
  system state to fake.
- `pressure()` is computed once per tick from real EWMA signals
  (`metrics.py`), not recomputed per-event — one number shared by every
  event `decide()` evaluates that tick, which is also what makes the
  cancellation argument above apply in the first place.
- A judge asking "does load actually change which event wins" gets a
  concrete, checkable answer: no, ordering is load-invariant by
  construction; only *mode* (stream vs. batch vs. defer) is load-aware.
  That is a deliberate design claim, not an oversight — see also why
  `ladder.escalate()` is a third, separate function (docs/ARCHITECTURE.md)
  rather than a third term bolted onto either of these two.
