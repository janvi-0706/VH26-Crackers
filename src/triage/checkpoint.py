"""Write-ahead in-flight checkpoint — exactly-once processing across a
worker death.

Owner: Lane A.

Same shape as an off-path decision (DEFER, then replay), just triggered by
a different cause. `worker.py` already has a durable pattern for "this
event left the live queue but has not finished yet, and might need to come
back": `deferral.py`'s store plus `queue.put_replayed()`. A worker dying
mid-service is the same situation — an event has been taken off the queue
(`metrics.observe_dequeue` already ran, `in_flight` is already +1 for it)
and has not yet completed — so recovery reuses exactly that same shape
(release the `in_flight` slot, then `put_replayed()`) rather than inventing
a second, parallel notion of "waiting to be served again."

The mechanism, in order:

    begin(event, worker_id)    write-ahead, BEFORE the worker's simulated
                                service time starts (the one `await` point
                                a real cancellation, or worker death, can
                                land inside). One row per EVENT, not one
                                row per batch — see the module docstring's
                                own note on why that granularity is the
                                whole point.
    mark_done(event_id)        AFTER the event has been fully served
                                (metrics.observe_complete + the sink write
                                both already happened) — deletes the row.
    recover_worker(worker_id)  called only when worker.py's own supervisor
                                has confirmed a specific worker's task
                                actually ended (cancelled, or an
                                unexpected exception escaped `_run`'s own
                                per-event try/except) — returns every event
                                still checkpointed under that worker_id
                                (there should be at most a handful: however
                                many events that one worker had begun but
                                not yet finished) and deletes those rows.
                                Scoped to ONE worker_id, never "everything
                                still in flight" — a still-alive worker's
                                own in-progress events must never be
                                touched, or recovery itself would create
                                the exact double-processing this whole
                                mechanism exists to prevent.

Why per-event, not per-batch: `worker._serve_batch()` still issues one
combined `asyncio.sleep()` for cost-model reasons (ADR 0002 — batching is
what makes MICRO_BATCH genuinely cheaper), but every member of that batch
gets its own `begin()`/`mark_done()` pair, called individually in the same
per-member loop that already calls `metrics.observe_complete`/the sink
write for each one — with a deliberate `asyncio.sleep(0)` at the top of
each iteration, the only point a real cancellation can land, and
deliberately BEFORE that member's own observe/write/mark_done rather than
after (see `_serve_batch`'s own docstring): a worker dying always leaves
the loop cleanly between two members, never mid-member. If a worker dies
after successfully finishing 47 of a 50-event batch, exactly the 3
unfinished members are still checkpointed; `recover_worker()` returns
exactly those 3, never the 47 already-`mark_done()`-cleared ones. A
batch-level checkpoint (one row per batch) could only ever answer "did
this whole batch finish", which would force retrying all 50 for 3 real
failures — the specific mistake this
design exists to avoid.

Why this does not need to be a durable (on-disk) store the way
`deferral.py`/`sink.py` default to being: what it protects against is one
`asyncio.Task` (a worker) dying while the surrounding Python process keeps
running — the same process, same event loop, same in-memory state that
recovery reads from. A whole-process crash is a different failure this
project does not claim to survive (see CLAUDE.md hard rule 1: one process,
by design) — `:memory:` SQLite is exactly as durable as this mechanism
needs to be, matching `sink.py`/`deferral.py`'s own default.

Ownership: one `CheckpointStore` per `WorkerPool`, constructor-injected
exactly like `sink_write`/`defer` already are — NOT a module-level ambient
singleton the way `sink.py`/`deferral.py` default to. Those two are
genuinely global concepts for this project (one audit trail, one deferred
backlog, for the one pipeline CLAUDE.md hard rule 1 says exists per
process). An in-flight table keyed by `worker_id` is scoped to one specific
pool's own tasks instead — worker_id 0..N-1 only means something within
one `WorkerPool` — and giving each pool (each test's own `WorkerPool(...)`
included) its own store is what keeps two unrelated tests that happen to
reuse the same `event_id` (this codebase's tests routinely do — "evt-1"
appears in dozens of files) from ever colliding in a shared table and
tripping a false exactly-once-violation neither test caused.

Exactly-once, made checkable rather than merely intended: `begin()` and
`mark_done()` both report whether the row they touched was in the expected
state (no existing row to begin over; an existing row to delete). A caller
getting the *unexpected* answer either way is live evidence of a double
process attempt — `metrics.observe_exactly_once_violation()` is wired to
exactly that signal (worker.py), not to a value nobody ever increments.
`MetricsFrame.exactly_once_violations` "must always read 0" is therefore a
claim this file can actually falsify if it were ever wrong, the same way
`metrics.critical_failure_count()` is a real, continuously-asserted check
and not a comment promising a property nothing verifies.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from .contracts import Event

IN_FLIGHT_DDL = """
CREATE TABLE IF NOT EXISTS in_flight_checkpoint (
    event_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    worker_id INTEGER NOT NULL,
    started_ts REAL NOT NULL,
    event_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_in_flight_worker
    ON in_flight_checkpoint (worker_id);
"""


class CheckpointStore:
    """SQLite-backed, `:memory:` by default — see the module docstring for
    why that default is the right one here, not a shortcut."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(IN_FLIGHT_DDL)
        self.connection.commit()

    def begin(self, event: Event, worker_id: int, *, now: float | None = None) -> bool:
        """Write-ahead: this event is about to occupy a worker. Returns
        True on the normal path (no row existed yet). Returns False if a
        row for this `event_id` was already there — structurally this
        should be impossible (an `Event` is dequeued, and therefore handed
        to exactly one `serve()`/`_serve_batch()` call, at most once at a
        time), so False is live evidence of a double-begin, not a state
        this function tries to merge or overwrite; the existing row is
        left untouched either way."""
        now = time.time() if now is None else now
        try:
            self.connection.execute(
                """
                INSERT INTO in_flight_checkpoint (
                    event_id, idempotency_key, worker_id, started_ts, event_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (event.event_id, event.idempotency_key, worker_id, now, event.model_dump_json()),
            )
            self.connection.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def mark_done(self, event_id: str) -> bool:
        """This event finished (observe_complete + the sink write both
        already happened). Returns True on the normal path (a row existed
        and was removed). Returns False if no row existed — evidence this
        event_id was already marked done once, or was never begun; either
        way, a second, correctly-guarded completion attempt did not get to
        actually re-run the side effects (the caller checks this BEFORE
        doing anything observable), so this signals the near-miss rather
        than the violation actually landing."""
        cur = self.connection.execute(
            "DELETE FROM in_flight_checkpoint WHERE event_id = ?", (event_id,)
        )
        self.connection.commit()
        return cur.rowcount > 0

    def recover_worker(self, worker_id: int) -> list[Event]:
        """Everything still checkpointed under this one worker_id — called
        only once worker.py's own supervisor has confirmed that specific
        worker's task actually ended. Deletes the recovered rows as part of
        the same operation: once handed back, they are the caller's
        responsibility (a fresh `put_replayed()`), not this store's,
        exactly like `deferral.DeferralStore._pop_ready_batch()`'s own
        pop-removes-it contract."""
        rows = self.connection.execute(
            "SELECT event_id, event_json FROM in_flight_checkpoint WHERE worker_id = ?",
            (worker_id,),
        ).fetchall()
        if not rows:
            return []
        events = [Event.model_validate_json(row["event_json"]) for row in rows]
        self.connection.execute(
            "DELETE FROM in_flight_checkpoint WHERE worker_id = ?", (worker_id,)
        )
        self.connection.commit()
        return events

    def in_flight_count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM in_flight_checkpoint").fetchone()
        return int(row[0])

    def is_in_flight(self, event_id: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM in_flight_checkpoint WHERE event_id = ?", (event_id,)
        ).fetchone()
        return row is not None

    def busy_worker_ids(self) -> list[int]:
        """Every worker_id with at least one row checkpointed right now —
        Stage I's `/chaos/kill-worker` uses this to prefer killing a
        worker that is actually doing something. Killing an idle worker
        (blocked on `queue.get()`) is a real, valid kill too, but recovers
        nothing and would make the demo's own most memorable ten seconds
        occasionally show nothing happening, for no reason a judge could
        see."""
        rows = self.connection.execute(
            "SELECT DISTINCT worker_id FROM in_flight_checkpoint"
        ).fetchall()
        return [int(row["worker_id"]) for row in rows]

    def close(self) -> None:
        self.connection.close()
