# 0011 — Online cost learning over static constants, and over a bandit

## Context

Every ordering decision (`decision.score()`'s density term) and every
routing decision (`decide()`'s slack/EDF check) depends on knowing an
event's cost. Through Stage H, that number was a flat, hardcoded
per-type constant from `config/tiers.yaml`, blind to any real per-event
variance (payload size, in particular). Stage I asked for cost to be
learned instead — from real, observed service times — while remaining
something that cannot misbehave live in front of a jury.

## Options considered

1. **Keep the static per-type constant.** Simple, deterministic, zero
   learning risk — and blind to real variance forever; the same number
   for a 64-byte click and a 4096-byte one.
2. **A bandit** — an exploring policy that occasionally routes events
   differently specifically to gather better cost information faster
   (the classic explore/exploit trade-off). Rejected outright, not
   merely deprioritised: an exploring policy can, by its own design,
   deliberately make a locally worse scheduling decision in order to
   learn — indistinguishable, live, from the system simply misbehaving,
   and "cannot misbehave live in front of judges" is this stage's own
   explicit, non-negotiable constraint. A bandit's own upside (faster
   convergence) is not worth a live demo's downside (an unexplained bad
   decision at the worst possible moment).
3. **Passive online learning** — a running estimate updated ONLY from
   real traffic the system would have routed identically either way,
   never from a deliberately-perturbed action. `costmodel.py`'s
   `CostModel.observe()`/`estimate()`.

## Decision

Option 3. `CostModel` never chooses what gets served — `observe()` is
fed, after the fact, from whatever a worker actually just finished
(`worker.py`'s own completion point), and `estimate()` only changes how
ALREADY-admitted, already-scheduled-the-same-way traffic is weighed by
`decision.py`'s ordering math on its NEXT comparison. Checked directly,
not just asserted: `tests/test_costmodel.py::
test_observe_never_influences_what_is_served_it_is_not_a_bandit`.

Within option 3, a sample-recency EWMA (`RunningEstimate`) was chosen
over a heavier online model (e.g. online ridge regression) for the same
reason ADR 0006 picked RFC 8289 CoDel's own simplified control law over a
more elaborate AQM: this project has no numpy/scipy dependency to lean on
for the linear algebra, and a per-(type, payload-bucket) running estimate
answers the one thing a live demo actually needs — converging visibly,
re-adapting visibly to a sustained shift — without new machinery. Decaying
by SAMPLE count rather than wall-clock time is the one deliberate
refinement over a plain cumulative average: a flat average would converge
correctly but then respond to a real regime shift ever more slowly the
longer the process has already been running, which would make "inject a
heavier mix and watch it re-adapt" a demo beat that stops working after
enough uptime — the one failure mode this design specifically avoids.

## Consequences

- `true_cost()` (the ground truth `worker.py` still simulates) and
  `CostModel.estimate()` (what ordering math uses instead) are two
  different numbers, kept deliberately distinct — see `costmodel.py`'s
  own docstring. Confusing them was the one mistake this design had to
  not make: worker.py's actual simulated sleep must always use the TRUE
  cost, never the estimate, or CLAUDE.md hard rule 2's determinism (a
  simulated-but-deterministic capacity ceiling) would depend on how
  converged the learner happened to be at any given instant.
- Calibration is untouched: `true_cost`'s own expectation over the
  generator's real payload-size draw equals the config prior exactly
  (proved with 20,000 real samples per type, not just algebra) — the
  three calibration invariants `config.py`'s own load-time check enforces
  were never a function of a flat cost to begin with, only of its mean.
- Cost: slower convergence than a bandit could achieve, and less
  statistical sophistication than a full regression — accepted
  explicitly, because the property this stage actually asked for
  ("deliberately load-bearing... and this cannot [misbehave]") ranks
  above faster convergence or a tighter fit.
- The honest answer to "why not use a real bandit, they converge faster":
  a bandit's exploration is exactly the failure mode a live jury demo
  cannot absorb — a wrong scheduling decision made on purpose, for the
  system's own benefit rather than the traffic's, at a moment nobody can
  predict.
