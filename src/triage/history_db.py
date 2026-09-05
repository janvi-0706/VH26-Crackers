"""history.db — the one shared SQLite connection ingress writes through.

Owner: Lane B/D (Phase J6).

Ingress is the SINGLE WRITER for `events_sink`, `sla_outcomes`, `rollups`
(sink.py), `audit_ledger` + `decision_traces` (ledger.py), and
`deferred_buffer` (deferral.py) — `docs/PHASE-J-INSPECTION.md` section 3
already named all five tables (`decision_traces`/`sla_outcomes` are this
phase's own additions to that list) as staying in exactly one process. This
module is what makes that literal rather than aspirational: one
`sqlite3.Connection`, opened once, handed to `SQLiteSink`, `SQLiteLedger`,
and `DeferralStore` (each already accepts a `connection=` — see their own
docstrings) instead of each opening its own separate `:memory:` connection
the way every stage before this one did.

WAL mode, not the default rollback journal: with a single writer this is
about concurrent READERS (the dashboard, `GET /audit.csv`,
`GET /audit/trace/{event_id}`) never blocking behind a write transaction,
not about avoiding writer-writer contention — there is only ever one
writer here, by design (`docs/PHASE-J-INSPECTION.md`'s own "only one pod
needs a volume" reasoning, Phase K's own future PVC). `busy_timeout` is
defensive insurance on top of that: WAL still has a brief exclusive window
during a checkpoint, and a reader or writer that lands in it should wait a
bounded time rather than raise `sqlite3.OperationalError: database is
locked` immediately.

Deliberately opt-in, not the automatic default for every `create_app()`
call: hundreds of existing tests construct `create_app(fake=False)` (often
several per test file) expecting the SAME ambient, isolated, in-memory
sink/ledger/deferral behaviour every stage before this one already relied
on. Silently defaulting real mode to a real, persistent
`config/servers.yaml`-configured file would have every one of those tests
sharing ONE real file across an entire test run (and across runs, since
the file outlives the process) — exactly the cross-test contamination
`reset_default_store()`/`reset()` exist to prevent. `triage.app`'s own
`--persist` flag (this phase's own addition) is the explicit opt-in;
omitting it keeps `make dev`, `make test`, and every existing test file's
own behaviour byte-for-byte unchanged.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from . import deferral, ledger, sink
from .pg_compat import is_postgres, open_connection

logger = logging.getLogger(__name__)

# 5 seconds — the same order of magnitude as transport.py's own
# ack_timeout_ms (5000), chosen for the same reason: long enough that a
# genuinely transient lock (a WAL checkpoint, a concurrent reader mid-scan
# of a large table) resolves within it on this project's own demo scale,
# short enough that a real deadlock or a stuck connection surfaces as a
# real error within a few seconds rather than hanging the request that hit
# it indefinitely.
BUSY_TIMEOUT_MS = 5000


def open_history_db(path: str | Path) -> Any:
    """Open (or create) the one shared connection every durable table in
    this process writes through, from whatever `path` actually names.

    `path` may be a local filesystem path (SQLite, this module's own
    original behaviour) OR a `postgres://`/`postgresql://` URL (Supabase,
    or any Postgres) — `pg_compat.open_connection()`'s own dispatch by
    scheme. This function does NOT read `DATABASE_URL`/`.env` itself and
    never silently overrides an explicit `path` with one — a test (or any
    caller) that explicitly asks for a specific local SQLite file must
    get exactly that file, regardless of what happens to be sitting in
    the ambient environment. `app.py`'s own `--persist` startup code is
    where `pg_compat.database_url()` is actually consulted, to decide
    WHICH string to pass in here in the first place — see that call site
    for the real opt-in logic.

    `check_same_thread=False` (SQLite path only) matches every other
    SQLite connection already in this codebase (sink.py, ledger.py,
    deferral.py) — this project's own single-event-loop, single-process
    model (CLAUDE.md hard rule 1) means "same thread" was never the real
    safety property anyway; what actually matters is that every write
    happens on the one asyncio event loop, which none of these modules'
    own callers ever violate. WAL mode and `busy_timeout` are SQLite-only
    concepts (this module's own top docstring covers why they exist) —
    skipped entirely for a Postgres connection, which has no equivalent
    PRAGMA and needs none (Supabase's own server handles concurrent
    readers without this process asking it to).
    """
    connection = open_connection(str(path))
    if not is_postgres(connection):
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    return connection


def wire_ambient_stores(connection: Any) -> None:
    """Point sink.py/ledger.py/deferral.py's own ambient defaults at ONE
    shared connection instead of each module's own separate `:memory:`
    default. Called once, at real-mode ingress startup with `--persist` —
    see this module's own top docstring for why it is not the automatic
    default for every real-mode `create_app()` call.

    Order does not matter between the three (each store's own
    `executescript()` uses `CREATE TABLE IF NOT EXISTS`, so all three can
    initialise their own tables against the same connection in any order
    without racing each other — there is no cross-table foreign key here
    for ordering to matter to).
    """
    sink.configure_default(sink.SQLiteSink(connection=connection))
    ledger.configure_default(ledger.SQLiteLedger(connection=connection))
    deferral.configure_default(deferral.DeferralStore(connection=connection))
    logger.info("history.db wired: sink, ledger, and deferral now share one connection")
