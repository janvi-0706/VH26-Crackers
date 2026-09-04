# 0004 — Contract-first: freeze schemas before implementation

## Context

This is a sequential, prompt-by-prompt build against a hard clock, with a
dashboard, an engine, and a benchmark harness that all need to agree on one
wire format (`MetricsFrame`) and one data model (`Event`). A field missing
at hour 20 costs far more than one missing at hour 1.

## Options considered

1. **Evolve the schema as each stage needs it.** Add fields only when the
   feature producing them lands. Minimal upfront work, but the dashboard
   needs rewriting each time, and a late field means revisiting every
   earlier consumer.
2. **A loose/dynamic payload** (a dict, not a typed model). Maximum
   flexibility, zero safety — a key typo fails silently, not at validation.
3. **Contract-first: design `Event`, `Decision`, `MetricsFrame` in Stage A
   with every field any later stage will need, all defaulted, then freeze
   them.** Later stages fill fields in; none add new ones without
   deliberately unfreezing the contract.

## Decision

Option 3. `contracts.py` and `config/tiers.yaml` were designed once in
Stage A — including fields with no implementation yet (`pressure`,
`ladder_rung`, `cost_adaptive`) — then frozen. CLAUDE.md's rule: if a later
stage needs a missing field, stop and ask; never add one silently.

## Consequences

- The dashboard (`metrics.ts`) was built and demoed against the real schema
  before the adaptive engine existed (Stage A/B), via `fake_metrics.py` —
  genuine parallel progress, not a frontend blocked on the backend.
- `tests/test_contracts.py` locks the frozen field set, so a field quietly
  disappearing is a test failure, not a demo-day surprise.
- Cost: Stage A spent time speculating about fields Stage D/E would need,
  some possibly unused — accepted over reworking a live dashboard mid-event.
- The answer to "you have a schema document at hour 8" — most teams don't,
  and it's a direct consequence of this decision.
