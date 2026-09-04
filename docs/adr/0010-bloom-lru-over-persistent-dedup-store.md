# 0010 — Bloom filter + LRU over a persistent dedup store

## Context

Stage I needed to catch duplicate deliveries (the same `dedup_key`
arriving twice — a chaos-flood replay, or a real retried webhook) at
ingest, before a duplicate ever occupies a queue slot or a worker's
simulated service time — not just rely on `sink.py`'s own
idempotency-key upsert, which only stops a duplicate from creating a
second row after it has already cost real capacity to process once more.

## Options considered

1. **A persistent dedup store** — every `dedup_key` ever admitted,
   written durably (SQLite, matching `sink.py`/`deferral.py`'s own
   default), checked by exact lookup on every ingest. Unbounded, exact,
   simple.
2. **A Bloom filter alone** — a fixed-size bit array, O(1) candidate
   check, no per-key storage growth. Rejected outright, not just
   deprioritised: a Bloom filter's false positives would then be the
   entire dedup decision, and CLAUDE.md hard rule 3's spirit ("a P0 event
   is never silently lost") extends naturally to "a P0 event is never
   silently suppressed by a coin-flip-shaped false positive" — unacceptable
   on its own.
3. **A Bloom filter as a candidate check, backed by an exact, BOUNDED
   (LRU) confirmation set** — a Bloom "maybe" is never trusted alone; only
   a hit the bounded exact set can actually confirm suppresses anything.

## Decision

Option 3. `dedup.py`'s `Deduplicator` never suppresses on an unconfirmed
Bloom hit, for any tier, P0 included (proved both directions in
`tests/test_dedup.py`) — this is what rules out option 2 as unsafe by
itself, and what makes option 3 strictly safer than option 1 was ever
required to be (option 1 has no false-positive risk at all, but see
below for why its own cost was the deciding factor against it).

The exact set is bounded (`DEFAULT_EXACT_SET_CAPACITY`), not unbounded
like option 1's own persistent store: a real duplicate-delivery SLA is
"the retry lands within N seconds/minutes," not "reject a repeat of
something admitted a week ago" — a bounded recent window is what that
retry SLA actually calls for, and it is what keeps memory bounded
regardless of how long the process has been running (CLAUDE.md hard
rule 1's own single-process, in-memory posture). An aged-out entry's
real repeat is deliberately treated exactly like a Bloom false positive
— admitted, not incorrectly suppressed forever — the same rule doing
double duty rather than two special cases.

## Consequences

- No SQLite write per ingest just to check for a duplicate — the Bloom
  filter is a fixed-size in-memory bit array, and the exact set is a
  plain `OrderedDict`; option 1 would pay a durable write (or at least a
  durable-store round trip) on the hot ingest path for every single
  event, including the ~95% that are never duplicates at all.
- Memory is bounded by construction (`DEFAULT_EXPECTED_ITEMS`/
  `DEFAULT_EXACT_SET_CAPACITY`), not by how long the demo has been
  running — option 1's own store would grow forever, exactly the kind of
  unbounded-growth question this project's own `deferral.py` docstring
  already flags as the honest limitation of its own unbounded
  `already_deferred` set (Stage E), not repeated here.
- Cost: a duplicate that arrives long after its own window has closed is
  not caught — named as a deliberate scope decision (a bounded recency
  window, not a permanent record), not hidden. `sink.py`'s own
  idempotency-key upsert is the second, independent safety net for
  exactly this case: a duplicate that slips past `dedup.py`'s window
  still cannot create a second row.
- The concrete answer to "why not just remember everything forever": a
  persistent, unbounded, exact-match store is a strictly heavier
  mechanism solving a stronger guarantee (unlimited-window dedup) than
  this project's own duplicate-delivery SLA actually needs — the same
  shape of trade-off ADR 0009 makes for exactly-once recovery.
