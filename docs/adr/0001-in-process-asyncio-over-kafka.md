# 0001 — In-process asyncio over Kafka

## Context

PULSE needs a pipeline that visibly triages ~333 events/sec under a 20x
spike, judged on progress, code originality, data design, and architecture —
not infrastructure breadth. Demo polish and deployment footprint are
explicitly not scored.

## Options considered

1. **Kafka (or Redis Streams) as the backbone.** Real topics, partitions,
   consumer groups. Familiar and "production-looking."
2. **Multi-process / Celery workers** pulling from a broker, closer to how a
   real deployment might scale horizontally.
3. **In-process asyncio, one Python process, in-memory heaps.** Generator,
   classifier, queue, workers, and metrics all run on one event loop.

## Decision

In-process asyncio (option 3).

At ~333 events/sec, a broker adds operational surface (topics, offsets,
consumer lag, another process to keep alive) without adding any scheduling
intelligence — Kafka doesn't know what EDF or an aging guard is; we'd still
write that ourselves on top of it. Choosing infrastructure that *looks*
production-grade would spend our limited hours on plumbing instead of the
decision logic that is actually the originality score. One process also
makes the capacity ceiling deterministic on any judge's machine, zero setup.

## Consequences

- No horizontal scaling story; a deliberate cut, named as such if asked
  "what's next," not an oversight.
- Every scheduling primitive (priority queue, aging guard, admission
  control) is hand-written and directly inspectable — the code *is* the
  answer to "how does this work."
- A crash of the single process loses in-memory queue state; acceptable for
  a 30-hour demo, documented rather than hidden.
- Sets up ADR 0002: without a broker's built-in backpressure, we need our
  own deterministic notion of capacity.
