"""The deferred buffer: durable storage for P1/P2 work parked under
pressure, and the background drainer that replays it once pressure falls.

Owner: Lane B.

Schema is exactly `deferred_buffer` from docs/DATA_MODEL.md — that document
is the design contract; this file is its first implementation, not a new
design. P0 is forbidden at the schema level (`CHECK (tier IN ('P1', 'P2'))`)
and at the API level (`defer()` raises if handed a P0 event) — CLAUDE.md
hard rule 3 enforced twice, not once, because a single enforcement point is
exactly one refactor away from silently disappearing.

This module deliberately does not import metrics.py or ledger.py, matching
sink.py's own precedent: a persistence module owns storage, not auditing.
The caller (worker.py) calls both `metrics.observe_decision(...)` and
`deferral_store.defer(...)` as two separate steps, exactly as it already
calls both `metrics.observe_complete(...)` and `sink.write(...)` separately
for a normal completion. This also avoids a real import cycle: metrics.py
needs to read this module's live pending count for `MetricsFrame.
deferred_pending` (see metrics.current_pressure's docstring for why that
count can no longer be a resettable in-memory counter), so the dependency
has to run one way only.

The genuinely hard part of "defer, then replay" — found by working through
what happens to an event whose slack has already gone negative by the time
it is replayed — is documented on `DeferralStore.already_deferred`.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from collections import deque
from pathlib import Path
from typing import Callable

from .contracts import Event, Tier
from .pg_compat import is_postgres

DEFERRED_BUFFER_DDL = """
CREATE TABLE IF NOT EXISTS deferred_buffer (
    defer_id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    dedup_key TEXT NOT NULL,
    seq INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    tier TEXT NOT NULL CHECK (tier IN ('P1', 'P2')),
    deadline_ts REAL NOT NULL,
    deferred_ts REAL NOT NULL,
    ready_at REAL NOT NULL,
    defer_reason TEXT NOT NULL,
    event_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    origin TEXT NOT NULL DEFAULT 'local' CHECK (origin IN ('local', 'server2'))
);
CREATE INDEX IF NOT EXISTS idx_deferred_ready_priority
    ON deferred_buffer (ready_at, tier, deadline_ts, seq);
CREATE INDEX IF NOT EXISTS idx_deferred_partition_seq
    ON deferred_buffer (partition_key, seq);
CREATE INDEX IF NOT EXISTS idx_deferred_deadline
    ON deferred_buffer (deadline_ts);
CREATE INDEX IF NOT EXISTS idx_deferred_origin_ready
    ON deferred_buffer (origin, ready_at);
"""

# Postgres (Supabase) mirror of DEFERRED_BUFFER_DDL — see pg_compat.py's
# own top docstring for the hand-written-sibling reasoning. `defer_id
# INTEGER PRIMARY KEY` relies on SQLite's own implicit rowid-alias
# autoincrement (`defer()` never supplies it) — `BIGSERIAL PRIMARY KEY`
# is the real Postgres equivalent.
DEFERRED_BUFFER_DDL_POSTGRES = """
CREATE TABLE IF NOT EXISTS deferred_buffer (
    defer_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    dedup_key TEXT NOT NULL,
    seq INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    tier TEXT NOT NULL CHECK (tier IN ('P1', 'P2')),
    deadline_ts DOUBLE PRECISION NOT NULL,
    deferred_ts DOUBLE PRECISION NOT NULL,
    ready_at DOUBLE PRECISION NOT NULL,
    defer_reason TEXT NOT NULL,
    event_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    origin TEXT NOT NULL DEFAULT 'local' CHECK (origin IN ('local', 'server2'))
);
CREATE INDEX IF NOT EXISTS idx_deferred_ready_priority
    ON deferred_buffer (ready_at, tier, deadline_ts, seq);
CREATE INDEX IF NOT EXISTS idx_deferred_partition_seq
    ON deferred_buffer (partition_key, seq);
CREATE INDEX IF NOT EXISTS idx_deferred_deadline
    ON deferred_buffer (deadline_ts);
CREATE INDEX IF NOT EXISTS idx_deferred_origin_ready
    ON deferred_buffer (origin, ready_at);
"""

# Phase J6: `origin` distinguishes a row deferred by THIS process's own
# in-process worker.py ('local' — the monolith/Engine's own pipeline,
# unchanged since Stage E) from one deferred by a real, separate server2
# instance over HTTP ('server2' — Phase J5's own /defer POST). The two
# need different replay destinations: a 'local' row re-enters Engine's own
# queue.put_replayed() exactly as before; a 'server2' row must go back
# OVER THE WIRE to server2 (transport.submit()), never into Engine's local
# queue — Engine's own decision engine is a different, independent
# instance from whichever real server2 pod actually deferred it, and
# CLAUDE.md's "P1/P2 -> Server 2" boundary would be silently violated by
# quietly processing it locally instead. Two independent drain loops, one
# per origin, run off the SAME store (see run_drainer's own `origin`
# filter) rather than one loop guessing a destination per row.
ORIGIN_LOCAL = "local"
ORIGIN_SERVER2 = "server2"

# The drainer's own pacing — separate from the pressure threshold it waits
# for. "Rate-limited so replay cannot re-trigger pressure and oscillate"
# means two things, both enforced here: never drain everything in one
# breath (DRAIN_BATCH_PER_TICK caps a single tick), and never poll so
# tightly that the drain loop itself becomes load (DRAIN_TICK_SECONDS).
#
# The actual numbers matter, not just their existence: DRAIN_BATCH_PER_TICK
# / DRAIN_TICK_SECONDS = 100 events/sec, chosen against real spare
# capacity, not picked arbitrarily. Post-reset, baseline demand is ~14 u/s
# against a 150 u/s pool — roughly 135 u/s spare. At P1/P2's mix-weighted
# average cost (~0.6u/event), that ceiling is ~225 events/sec; draining at
# 100/sec uses under half of it, leaving real headroom so replay traffic
# alone cannot re-saturate the pool it is trying to drain into. Verified
# empirically, not just computed: a first draft at 20 events/sec (5 per
# tick) could not clear a real 30-second spike's backlog (several thousand
# events) within any reasonable wait — draining slower than a backlog can
# plausibly grow is not "rate-limited", it is "never finishes".
DRAIN_PRESSURE_THRESHOLD = 0.35
DRAIN_TICK_SECONDS = 0.25
DRAIN_BATCH_PER_TICK = 25

# How wide a window drain_rate() averages over. Wide enough that a single
# tick's burst of DRAIN_BATCH_PER_TICK doesn't make the rate look spiky;
# narrow enough that it reflects "is the backlog actually going down right
# now", not a stale number from minutes ago.
_DRAIN_RATE_WINDOW_SECONDS = 5.0

# ready_at is used purely as a query-ordering column here (matching
# docs/DATA_MODEL.md's own framing: "(ready_at, tier, deadline_ts, seq):
# eligible work in urgency order") — every deferred row becomes ready the
# instant it is stored, and readiness to actually drain is gated globally
# by pressure, not per-row. A per-row backoff schedule is a real,
# legitimate design (and *would* use ready_at for its literal purpose) but
# is not what this prompt asks for — not built silently.


class DeferralStore:
    """SQLite-backed. Defaults to `:memory:`, matching sink.py's own
    demo-scale default.

    Phase J6: `connection`, when given, is used AS IS instead of opening a
    new one — this is how `history_db.py` wires this store onto ingress's
    one shared, WAL-mode `history.db` connection alongside `sink.py` and
    `ledger.py`, rather than each module opening its own separate file
    (`path` is then informational only, for `__repr__`/logging; the
    connection already IS whatever file it was opened against). `path` on
    its own, unchanged, still means what it always did — open a fresh
    connection there — for every existing caller that never heard of the
    shared-connection wiring.
    """

    def __init__(
        self, path: str | Path = ":memory:", *, connection: sqlite3.Connection | None = None
    ) -> None:
        self.path = str(path)
        if connection is not None:
            self.connection = connection
        else:
            if self.path != ":memory:":
                Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            DEFERRED_BUFFER_DDL_POSTGRES if is_postgres(self.connection) else DEFERRED_BUFFER_DDL
        )
        self.connection.commit()

        self._pending_count = self._count_rows()
        self.total_deferred = 0
        self.total_drained = 0
        self._drain_timestamps: deque[float] = deque()

        # Every event_id that has ever been deferred at least once, for the
        # life of this store. See the note below on why this exists and
        # why it is a plain, unbounded-for-now set.
        #
        # The trap this closes: decide()'s own rule is "slack < 0 -> DEFER",
        # checked before pressure, and slack can only ever get *more*
        # negative as time passes (deadline_ts is fixed). P1's SLA is only
        # 5 seconds — under a spike that keeps pressure elevated for
        # anywhere near that long, a deferred inventory event's slack is
        # essentially guaranteed to have gone negative by the time the
        # drainer replays it. Re-running decide() unchanged on replay would
        # then defer it AGAIN, forever: dequeue -> DEFER -> re-buffer ->
        # replay -> dequeue -> DEFER -> ... — the backlog would never reach
        # zero, and "nothing deferred is ever lost" would be true only in
        # the narrow, useless sense that it is never lost from the
        # database while also never actually being served.
        #
        # The fix lives here, not in decision.py: decide()'s formula is
        # correct and untouched — the SECOND time an event that has already
        # been given one chance to wait comes back DEFER, worker.py treats
        # that as "serve it now" instead of asking decide() to relitigate a
        # question whose answer cannot change. It will correctly show up as
        # an SLA miss (metrics.observe_complete already does that), not
        # loop forever and not silently vanish.
        #
        # Unbounded for the life of one process: event_ids are short
        # strings, and this project's actual runtime (a demo, not a real
        # 30-hour production deployment) never grows this past a few
        # hundred thousand entries. A real long-running system would want
        # an LRU or a TTL here.
        self.already_deferred: set[str] = set()

    # -- storage --------------------------------------------------------

    def _count_rows(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM deferred_buffer").fetchone()
        return int(row[0])

    def defer(
        self, event: Event, reason: str, *, now: float | None = None, origin: str = ORIGIN_LOCAL,
    ) -> None:
        """Park one event. Synchronous, like sink.write() — a SQLite insert
        at this scale is not worth an await.

        UPSERT, not a bare INSERT, on `event_id` — Phase J6's own
        cross-process re-dispatch (this store's `origin='server2'` rows
        replay by POSTing the event back to a real server2 over the wire,
        not into a local queue this process controls) makes a genuine
        re-defer of the SAME event_id a real, reachable case for the first
        time: pressure can easily still be high (or have gone high again)
        by the time server2 finishes deciding what to do with a
        redispatched event, and DEFERring it AGAIN is the correct,
        expected outcome, not a bug. A bare INSERT would raise
        `sqlite3.IntegrityError` against this table's own `event_id
        UNIQUE` constraint the second time — found while designing this
        exact redispatch path, not observed as a live incident (the
        Stage D-era, single-process redefer trap `already_deferred`/
        `was_deferred` below already prevented worker.py from ever
        double-deferring the SAME event within one process; a real
        network round trip out to server2 and back is what makes it
        reachable now). The UPSERT keeps `defer_id` and the original
        `dedup_key`/`idempotency_key`/`schema_version` stable while
        refreshing everything else to the new attempt's own values.
        """
        if event.tier is Tier.P0:
            raise ValueError("P0 must never be deferred (CLAUDE.md hard rule 3)")
        now = time.time() if now is None else now
        self.connection.execute(
            """
            INSERT INTO deferred_buffer (
                event_id, dedup_key, seq, partition_key, idempotency_key,
                event_type, tier, deadline_ts, deferred_ts, ready_at,
                defer_reason, event_json, schema_version, origin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                seq = excluded.seq,
                deadline_ts = excluded.deadline_ts,
                deferred_ts = excluded.deferred_ts,
                ready_at = excluded.ready_at,
                defer_reason = excluded.defer_reason,
                event_json = excluded.event_json,
                origin = excluded.origin
            """,
            (
                event.event_id, event.dedup_key, event.seq, event.partition_key,
                event.idempotency_key, event.type.value, event.tier.value,
                event.deadline_ts, now, now, reason,
                event.model_dump_json(), event.schema_version, origin,
            ),
        )
        self.connection.commit()
        # A genuine re-defer (the ON CONFLICT branch) does not grow the
        # table or the live backlog — it was already counted pending from
        # its first defer, still is, and cursor.rowcount is 1 either way
        # for an upsert, so re-deriving "was this actually new" from the
        # row count would silently double count. already_deferred (a set)
        # is the one source that already answers "have we seen this
        # event_id before" for free.
        is_new = event.event_id not in self.already_deferred
        if is_new:
            self._pending_count += 1
        self.total_deferred += 1
        self.already_deferred.add(event.event_id)

    def pending_count(self) -> int:
        """The live count — sourced from this store, not a resettable
        in-memory counter, so it stays true across /control/reset (see
        metrics.py: reset() clears the live queue and dashboard counters,
        but this buffer is durable and untouched by it)."""
        return self._pending_count

    def pending_count_by_origin(self, origin: str) -> int:
        """A real query, not the cached `_pending_count` total — used by
        the two independent drainers (and tests) to see their own share
        of the backlog without the other origin's rows in the way."""
        row = self.connection.execute(
            "SELECT COUNT(*) FROM deferred_buffer WHERE origin = ?", (origin,)
        ).fetchone()
        return int(row[0])

    def drain_rate(self, now: float | None = None) -> float:
        """Events/sec actually drained, averaged over the last
        _DRAIN_RATE_WINDOW_SECONDS. A real, queryable number — not wired
        into MetricsFrame, which is frozen after Stage A; surfacing it on
        the dashboard itself would need an explicit contract change, which
        this prompt does not ask for and this file does not make
        silently."""
        now = time.time() if now is None else now
        cutoff = now - _DRAIN_RATE_WINDOW_SECONDS
        while self._drain_timestamps and self._drain_timestamps[0] < cutoff:
            self._drain_timestamps.popleft()
        return len(self._drain_timestamps) / _DRAIN_RATE_WINDOW_SECONDS

    def _pop_ready_batch(self, limit: int, *, origin: str | None = None) -> list[Event]:
        """Up to `limit` events, oldest-deferred first, P1 before P2, then
        by deadline urgency then arrival — exactly the
        idx_deferred_ready_priority index's own ordering. Removes them from
        the table as part of the same operation: once popped, an event is
        the caller's responsibility, not this store's.

        `origin=None` (every existing caller before Phase J6) pops across
        BOTH origins, unchanged. Phase J6's two independent drainers each
        pass their own origin so a 'local' drain tick can never pop a
        'server2' row (which must go back over the wire, not into
        Engine's own queue) or vice versa — seeing the wrong origin's row
        here would be silently doing the wrong thing with it, not a
        merely suboptimal ordering choice.
        """
        query = "SELECT defer_id, event_json FROM deferred_buffer"
        params: list[object] = []
        if origin is not None:
            query += " WHERE origin = ?"
            params.append(origin)
        query += " ORDER BY ready_at ASC, tier ASC, deadline_ts ASC, seq ASC LIMIT ?"
        params.append(limit)
        rows = self.connection.execute(query, params).fetchall()
        if not rows:
            return []
        ids = [row["defer_id"] for row in rows]
        events = [Event.model_validate_json(row["event_json"]) for row in rows]
        placeholders = ",".join("?" * len(ids))
        self.connection.execute(
            f"DELETE FROM deferred_buffer WHERE defer_id IN ({placeholders})", ids
        )
        self.connection.commit()
        self._pending_count -= len(events)
        return events

    def close(self) -> None:
        self.connection.close()

    # -- the drainer ------------------------------------------------------

    async def run_drainer(
        self,
        replay: Callable[[Event], None],
        current_pressure: Callable[[], float],
        stop_event: asyncio.Event,
        *,
        origin: str | None = None,
    ) -> None:
        """Background loop: every DRAIN_TICK_SECONDS, if pressure has
        fallen below DRAIN_PRESSURE_THRESHOLD, hand up to
        DRAIN_BATCH_PER_TICK events back to `replay` (queue.put_replayed
        for a 'local' origin; a POST back to server2 for a 'server2'
        origin — see this module's own top docstring) and record the
        drain. Otherwise does nothing and waits for the next tick — the
        whole rate limit is exactly these two constants, no separate
        backoff logic needed.

        `current_pressure` is deliberately a caller-supplied callable, not
        this store reading some ambient signal itself: a 'local' drainer
        gates on Engine's own `metrics.current_pressure()`, while a
        'server2' drainer gates on that real process's own reported
        pressure (`reporting.fragments("server2")`, never averaged across
        instances — Phase J5's own rule) — two different signals, and this
        store has no business knowing which one applies to which origin.
        """
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=DRAIN_TICK_SECONDS)
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                return
            if current_pressure() >= DRAIN_PRESSURE_THRESHOLD:
                continue
            batch = self._pop_ready_batch(DRAIN_BATCH_PER_TICK, origin=origin)
            if not batch:
                continue
            now = time.time()
            for event in batch:
                replay(event)
            self.total_drained += len(batch)
            self._drain_timestamps.extend([now] * len(batch))


# --------------------------------------------------------------------------
# Module-level default store, matching sink.py's own precedent: metrics.py
# is already ambient/global ("one pipeline, one process"), and this store's
# whole reason for existing is to stay in sync with metrics regardless of
# /control/reset, so it is ambient too rather than owned per-Engine like
# queue.py/worker.py are. Tests that need isolation reset it explicitly
# (reset_default_store()), the same way tests reset metrics/ledger.
# --------------------------------------------------------------------------

_default_store = DeferralStore()


def defer(
    event: Event, reason: str, *, now: float | None = None, origin: str = ORIGIN_LOCAL,
) -> None:
    _default_store.defer(event, reason, now=now, origin=origin)


def pending_count() -> int:
    return _default_store.pending_count()


def pending_count_by_origin(origin: str) -> int:
    return _default_store.pending_count_by_origin(origin)


def drain_rate(now: float | None = None) -> float:
    return _default_store.drain_rate(now)


def was_deferred(event_id: str) -> bool:
    return event_id in _default_store.already_deferred


async def run_drainer(
    replay: Callable[[Event], None],
    current_pressure: Callable[[], float],
    stop_event: asyncio.Event,
    *,
    origin: str | None = None,
) -> None:
    await _default_store.run_drainer(replay, current_pressure, stop_event, origin=origin)


def reset_default_store() -> None:
    """Tests only — a fresh store, mirroring metrics.reset()/ledger.reset().
    Never called by Engine.reset(): the whole point of this buffer is to
    survive that reset."""
    global _default_store
    _default_store.close()
    _default_store = DeferralStore()


def configure_default(store: DeferralStore) -> None:
    """Phase J6: swap the ambient default store for one already wired onto
    ingress's own shared, WAL-mode `history.db` connection
    (`history_db.py`'s own job) — real-mode startup only, called once,
    before anything else touches this module. Distinct from
    `reset_default_store()` (tests only, always a fresh `:memory:` store):
    this one takes the store to use, and is how a real deployment's
    `data/history.db` actually gets adopted rather than the demo-scale
    `:memory:` default this module would otherwise keep forever."""
    global _default_store
    _default_store = store
