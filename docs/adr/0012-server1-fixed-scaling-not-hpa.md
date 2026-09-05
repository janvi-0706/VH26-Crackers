# 0012 — server1 is fixed-scaling, never HPA

## Context

Phase J's own three-process split (`config/servers.yaml`, `docs/PHASE-J-
INSPECTION.md`) gives server2 (P1/P2) a Horizontal Pod Autoscaler between
`min_pods` and `max_pods` — real, useful elasticity for tiers this project
deliberately lets degrade (batch, defer, sample, shed) under pressure.
Server1 (P0) is provisioned differently: `capacity_us: 135`,
`scaling: fixed` — a single fixed number, never a range. This ADR is the
reasoning `servers_config.py` and `server1.py` both already enforce in
code (see "Decision" below); CLAUDE.md's own working style asks that this
kind of reasoning live in an ADR, not only in a code comment.

## Options considered

1. **HPA on server1, same as server2.** Consistent tooling, one scaling
   story for both processes, and in principle more capacity available
   under an even larger spike than the one this project is calibrated
   against.
2. **Fixed capacity, sized with real headroom against the calibrated
   spike, never autoscaled.** What `config/servers.yaml` actually
   declares: `capacity_us: 135` against ~108 u/s of P0 demand at the
   documented 20x spike — ~80% utilisation, a real but not razor-thin
   margin.

## Decision

Fixed (option 2), enforced twice: structurally, by `servers_config.py`'s
own parser (`_parse` raises `ServersConfigError` if `server1.scaling !=
"fixed"` — a malformed `config/servers.yaml` cannot even load), and again
at `server1.py`'s own process startup (an explicit assertion against the
loaded `ServerSpec`, independent of the config loader's own check) — the
same "enforced twice, not once, because a single enforcement point is one
refactor away from silently disappearing" reasoning CLAUDE.md hard rule 3
already established for P0's protection downstream of admission, and that
`admission.py`/`ladder.py`/`decision.py` each already apply their own
extra, independent copy of.

The reason is a real number, not a style preference: a Kubernetes pod
running this project's own dependency stack (Python interpreter start,
FastAPI/uvicorn import and route registration, an `httpx.AsyncClient`
warm-up, the readiness handshake against ingress `server1.py` itself now
requires — see its own `/readyz` docstring) does not become schedulable
work in zero time. A published, honest estimate for that kind of cold
start on this stack is on the order of **45 seconds** — the number this
ADR is named for. HPA's own reaction loop cannot outrun that: by the time
a new server1 pod is actually ready to serve a single request, the spike
that triggered the scale-up has typically already run most of its own
course (this project's own calibrated demo spike is measured in tens of
seconds, not minutes) — the new capacity arrives after the SLA-relevant
window it was meant to protect has already closed. Autoscaling a
LATENCY-bound tier on a COLD-START timescale longer than the event you are
scaling for is not a slower version of the right answer; it is the wrong
mechanism for the problem, regardless of how quickly HPA itself decides to
act (HPA's own decision latency is not the bottleneck here — pod
readiness is).

P0's actual protection against a real spike was never meant to be "add
more pods" in the first place — it is CLAUDE.md hard rule 3's own
standing answer, unchanged since Stage A: *"under pressure we throttle the
source instead."* `admission.py`'s AIMD gate already does exactly that for
every tier including, structurally, the option to do it for P0 (its
bucket is simply marked `critical` and exempted); server1's own fixed
135 u/s ceiling is the OTHER half of that same answer applied to this
specific process — a capacity number sized with real, calibrated headroom
against the load it is actually promised to see, not a number that
expects to be topped up reactively once the promise is already being
tested.

## Consequences

- Server1's own total P0 capacity is a hard ceiling, by design: `135 u/s`,
  full stop, regardless of how much worse a real spike gets than this
  project's own calibrated 20x. A spike larger than the one
  `config/tiers.yaml`'s own calibration constants describe would need a
  LARGER fixed number chosen up front (a config change, re-verified
  against the same three invariants `config.py.verify()` already checks),
  not a live scale-out this ADR has just ruled out as ineffective on this
  stack's own cold-start timescale.
- This is a real, named cut, not a hidden gap: a genuinely elastic
  P0 tier (one that scales on a timescale actually faster than the spikes
  it protects against) is a legitimate design a different stack
  (pre-warmed pods, a faster runtime, a standing pool of idle-but-ready
  replicas) could pursue — explicitly out of scope for this project,
  named here rather than silently assumed away.
- `server1.py`'s own two independent assertions (config-parse time, and
  process-startup time) mean a future change that accidentally re-enables
  HPA for server1 — a config typo, a copy-paste from server2's own YAML
  block — fails loudly at the next config load or the next server1
  restart, rather than silently letting P0 inherit server2's own,
  deliberately different risk profile.
