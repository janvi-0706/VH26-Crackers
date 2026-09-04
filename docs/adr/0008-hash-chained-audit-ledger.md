# 0008 — Hash-chained audit ledger

## Context

PULSE's central claim is "critical events are never silently dropped,
and we can prove it" (CLAUDE.md). A live dashboard panel can *assert*
that, but an assertion a presenter types on stage is not evidence a judge
can independently check after the fact — the pipeline needs a durable
record whose integrity does not depend on trusting the process that wrote
it.

## Options considered

1. **Plain append-only log (SQLite table, no hashing).** Durable and
   queryable, but a row edited or deleted directly in the database file
   afterward is indistinguishable from a row that was always correct —
   the log is durable, not tamper-evident.
2. **Sign each row individually** (e.g. HMAC per row with a fixed key).
   Detects a changed row, but not a *deleted* one — removing a row
   entirely leaves every other row's own signature still valid, so a
   gap in the record is invisible.
3. **Hash-chain each row to the previous row's hash**, RFC-free and
   specific to this project: `row_hash = SHA-256(ledger_id | recorded_ts
   | seq | decision | reason | pressure | tier | prev_hash)`, `prev_hash`
   = the previous row's own `row_hash` (a published genesis constant for
   row 1). A `verify_chain()` walk re-derives every row's hash from its
   stored columns and checks both the hash itself and the link to its
   predecessor.

## Decision

Option 3, exactly as `docs/DATA_MODEL.md`'s own section 6 already
specified — `ledger.py` is that design's first implementation, not a new
decision made in code. Canonicalization is deliberately strict (fixed
`|`-separators, a fixed decimal representation for both REAL columns) so
that re-deriving a hash from stored columns is reproducible: two
logically-equal SQLite REAL values that happen to round-trip through
Python float formatting differently would otherwise hash differently,
making a verifier disagree with itself on an untampered row.

## Consequences

- `verify_chain()` catches a changed historical row (its recomputed hash
  no longer matches what is stored), a deleted middle row (the chain
  breaks at the gap it leaves), and an inserted or reordered row (same) —
  proved directly by `test_the_audit_hash_chain_detects_any_row_mutation`,
  parametrized over six different columns plus a deleted-row case plus
  the "forge both the row and its own row_hash" case (still caught, at
  the *next* row's now-broken `prev_hash` link).
- Named honestly, not oversold: this does **not** catch an attacker who
  rewrites both the database and the trusted head hash together, a false
  value recorded faithfully and consistently at decision time, or
  corruption outside this one table entirely. `docs/DATA_MODEL.md` states
  this limitation explicitly rather than letting "tamper-evident" imply
  "tamper-proof."
- `GET /audit.csv` exports the same rows a verifier would check, so the
  proof is independently downloadable, not only inspectable through the
  live dashboard.
- A reset (`POST /control/reset`) swaps in a fresh ledger instance rather
  than preserving the durable table across it — a deliberate reversal
  from an earlier draft that tried to keep history across reset "for
  tamper-evidence," reverted once it broke the existing
  `test_every_decision_writes_exactly_one_ledger_row` exact-count
  assertion; the module's own docstring documents the reversal rather
  than hiding it. Tamper-evidence is a property of one continuous chain
  during the run it covers, not a claim that a fresh demo run must carry
  every previous run's history.
