# 0009 — Write-ahead checkpoint over a full transaction log

## Context

Stage I needed exactly-once processing across one specific failure: a
worker (an `asyncio.Task`) dying mid-`serve()`, mid-cancellation, or
mid-exception, while the surrounding process keeps running. Something has
to know which events were taken off the queue but never finished, so
recovery can re-queue exactly those and nothing else.

## Options considered

1. **A full transaction log** — every state transition an event passes
   through (dequeued, batched, completed, deferred, …) appended to a
   durable, replayable log, the way an event-sourced system reconstructs
   state by replaying its entire history from the beginning. Recovery
   after a crash means replaying the log forward to the last consistent
   point.
2. **A write-ahead checkpoint, per worker, per event**: `begin()` before
   the one `await` a worker's death can land inside, `mark_done()` after —
   an ephemeral, in-memory table of "what is this worker holding right
   now," not a history of everything that ever happened.

## Decision

Option 2. `checkpoint.py`'s `CheckpointStore` records only current state
(an event is either checkpointed under a worker or it isn't), scoped to
one `WorkerPool`, `:memory:` SQLite by design — see that module's own
docstring for why durability beyond a single worker's death was never the
requirement: this project already accepts CLAUDE.md hard rule 1's
one-process, no-durability-across-a-crash design (ADR 0001). A full
transaction log solves a strictly harder problem (reconstructing ALL
history after the whole process dies) that this project has already,
deliberately, chosen not to solve.

## Consequences

- Recovery is O(1) in the number of events one worker was holding (at
  most a handful — one event, or one in-flight batch), not O(the whole
  run's history) — a transaction log's replay cost grows with uptime; a
  write-ahead checkpoint's does not.
- Per-event granularity inside a batch (`checkpoint.py`'s own central
  claim: a batch of 50 with 3 unfinished retries 3, not 50) falls out of
  the design for free — a transaction log would need the same per-event
  granularity engineered into it separately, at more storage cost per
  event, to make the identical claim.
- Cost: no forensic value. A transaction log doubles as an audit trail
  ("what exactly happened, in order, ever") — this project already has
  that, deliberately separate and durable-on-purpose: `ledger.py`'s
  hash-chained audit ledger (ADR 0008). Checkpoint state is explicitly
  disposable (`Engine.reset()` discards it outright), which is the
  correct behaviour for "what is currently in flight," not a limitation
  of the design.
- The honest answer to "what if the whole process crashes, not just one
  worker": unchanged from ADR 0001 — out of scope, named as such, not
  silently implied to be covered by this mechanism.
