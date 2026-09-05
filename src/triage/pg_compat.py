"""Postgres/Supabase support for the one shared "history" connection.

Owner: Lane B/D.

`history_db.py`'s own job (Phase J6) was "one shared connection, not a
separate `:memory:` per store" — this file is the same idea one step
further: that ONE connection can now be a real Supabase Postgres database
instead of a local SQLite file, without `sink.py`/`ledger.py`/
`deferral.py` needing to know or care which. Those three modules already
only ever call five things on whatever connection they are handed:
`.execute(sql, params)`, `.executescript(sql)`, `.commit()`, `.close()`,
and (once, at construction) `.row_factory = sqlite3.Row` — sqlite3's own
`Connection` class provides all five directly; `psycopg2`'s does not.
`PGConnection` below is a small adapter that provides the SAME five, so
every existing call site in this codebase keeps working completely
unchanged against either backend.

Two real, load-bearing differences this adapter papers over, deliberately
and explicitly, not silently:

  placeholders    SQLite's driver accepts `?`; psycopg2 requires `%s`.
                  Every hand-written query in this codebase already uses
                  `?` (sqlite3's own convention, unchanged since Stage A).
                  Translating `?` -> `%s` on every `.execute()` call,
                  once, here, is far lower-risk than hand-editing every
                  query string in three files and keeping two divergent
                  copies in sync forever after.
  row access      `sqlite3.Row` supports `row["column"]`; a plain
                  psycopg2 cursor returns tuples. `psycopg2.extras.
                  RealDictCursor` gives the same `row["column"]` access
                  this codebase already relies on everywhere (`sink.py`'s
                  own `row["payload_json"]`, etc.) — set as this
                  connection's own default cursor factory, so no call
                  site needs to know it changed.

What this adapter deliberately does NOT paper over: DDL. `INTEGER PRIMARY
KEY`'s own SQLite meaning (an implicit autoincrementing alias for the
table's own rowid) has no Postgres equivalent via simple text
substitution — a table that relies on it (`deferred_buffer.defer_id`,
`decision_traces.trace_id`, `sla_outcomes.outcome_id`; `audit_ledger.
ledger_id` does NOT — `ledger.py`'s own `_next_id` already supplies it
explicitly on every insert, matching real hash-chain requirements) needs
a real, hand-written Postgres DDL string (`BIGSERIAL PRIMARY KEY`), not
an automatic translation. `sink.py`/`ledger.py`/`deferral.py` each own
their own `..._DDL_POSTGRES` constant, right next to the SQLite one they
mirror, and pick between the two with `is_postgres(connection)` below —
explicit, inspectable, the same "hand-written, not auto-generated" spirit
CLAUDE.md's own originality rule already asks for everywhere else in this
project.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

_PLACEHOLDER_RE = re.compile(r"\?")


def _load_dotenv(path: str | Path = ".env") -> None:
    """The smallest possible `.env` loader — this project has no
    `python-dotenv` dependency, and a KEY=VALUE-per-line file does not
    need one. Only sets a variable that is not ALREADY in the real
    environment (a real deployment's own env vars must win over a
    checked-out `.env` file that happens to still be sitting around).
    Silently does nothing if the file does not exist — `.env` is a local
    developer convenience, never a requirement to boot."""
    import os

    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def database_url() -> str | None:
    """The one place `DATABASE_URL` is read from — `.env` first (local
    convenience, see `_load_dotenv`'s own docstring), then whatever the
    real process environment already has, which always wins."""
    import os

    _load_dotenv()
    return os.environ.get("DATABASE_URL") or None


def is_postgres_url(url: str) -> bool:
    return url.startswith("postgres://") or url.startswith("postgresql://")


class _HybridRow:
    """`sqlite3.Row` supports BOTH `row["column"]` and `row[0]` (positional)
    on the SAME row — this codebase's own call sites use both styles
    (e.g. `ledger.py`'s own `_count_rows()`: `row[0]`; `_load_last_hash()`:
    `row["row_hash"]`). `psycopg2.extras.RealDictRow` only supports the
    first (it is a plain dict subclass); a `NamedTupleCursor` row only
    supports the second. Rather than hunt down and rewrite every existing
    positional-access call site across three files (a real, avoidable
    risk under time pressure, and this codebase already leans on
    `sqlite3.Row`'s own dual nature on purpose), this small wrapper
    supports both, over one real `RealDictRow` (which already preserves
    column order, being an `OrderedDict` subclass)."""

    __slots__ = ("_dict",)

    def __init__(self, dict_row: Any) -> None:
        self._dict = dict_row

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return list(self._dict.values())[key]
        return self._dict[key]

    def keys(self) -> Any:
        return self._dict.keys()

    def __repr__(self) -> str:  # pragma: no cover - debugging convenience
        return f"_HybridRow({dict(self._dict)!r})"


class _PGCursorAdapter:
    """Wraps a real psycopg2 cursor just enough that `fetchone()`/
    `fetchall()`/`rowcount` behave the way this codebase's own sqlite3-
    based call sites already expect — dict-AND-positional row access
    (`_HybridRow`, see its own docstring for why both), `rowcount` after
    a DELETE/UPDATE."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def fetchone(self) -> Any:
        row = self._cursor.fetchone()
        return None if row is None else _HybridRow(row)

    def fetchall(self) -> list[Any]:
        return [_HybridRow(row) for row in self._cursor.fetchall()]

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int:
        """`sqlite3`'s own convenience for "the rowid this INSERT just
        generated" — `sink.py`'s own `write_rollup()`/`write_outcome()`
        read it after inserting into a `..._id BIGSERIAL PRIMARY KEY`
        column. Postgres has no `lastrowid` at all; `LASTVAL()` is its own
        real equivalent — "the value most recently returned by
        `nextval()` in THIS SESSION" — which the `BIGSERIAL` column's own
        implicit sequence already called during the `INSERT` this cursor
        just ran, on this exact connection."""
        cur = self._cursor.connection.cursor()
        cur.execute("SELECT LASTVAL()")
        # This connection's own cursor_factory is RealDictCursor (set once,
        # in PGConnection.__init__) — every cursor from it, including this
        # one, returns dict-like rows, never a plain tuple; `lastval` is
        # `LASTVAL()`'s own default column alias.
        return int(cur.fetchone()["lastval"])


class PGConnection:
    """Drop-in stand-in for `sqlite3.Connection`, backed by a real
    psycopg2 connection to Supabase (or any Postgres). See this module's
    own top docstring for exactly which five methods this provides and
    why that is enough for every existing call site in this codebase."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._connect()
        self.row_factory: Any = None

    def _connect(self) -> None:
        import psycopg2
        import psycopg2.extras

        self._conn = psycopg2.connect(self._url, connect_timeout=10)
        self._conn.cursor_factory = psycopg2.extras.RealDictCursor
        # A real, found-not-assumed bug: Postgres's own default
        # `extra_float_digits` setting does not guarantee a `double
        # precision` value comes back out of a TEXT-format round trip
        # bit-for-bit identical to what went in — found directly, not
        # theorised: `ledger.verify_chain()` failed against a real
        # Supabase table with "row_hash does not match this row's own
        # stored columns" on the very first row ever written, in the same
        # process, milliseconds after writing it. `_canonical_bytes()`'s
        # own `.6f`-formatted `recorded_ts` (an epoch float — ~10 digits
        # before the point, right at the edge of float64's own ~15-17
        # significant digits once 6 more are added after it) is exactly
        # the kind of value this Postgres default can silently perturb in
        # its own least-significant digit. `extra_float_digits = 3`
        # (Postgres's own documented fix for exactly this class of
        # round-trip loss) restores bit-for-bit fidelity — set once, on
        # this connection, before this codebase's own hash-chain logic
        # (frozen, untouched) ever reads a float back from a row it wrote.
        with self._conn.cursor() as _cur:
            _cur.execute("SET extra_float_digits = 3")
        self._conn.commit()

    def _reconnect_if_dead(self) -> None:
        """Real bug, found live: Supabase's own pooler (`aws-0-...-pooler.
        supabase.com:6543`, its `transaction`-mode pgbouncer) closes an
        idle server-side connection out from under this process without
        warning — every subsequent `.execute()` then raised
        `psycopg2.InterfaceError: connection already closed`, forever,
        because nothing here ever checked. Confirmed live: 12,700+
        consecutive `/ack` calls 500'd this way over one demo run, which
        in turn meant `transport.py` never resolved a single dispatch,
        which is the entire reason `outstanding_dispatch` climbed
        unboundedly and pressure looked permanently pinned — a Postgres
        connectivity blip masquerading as a triage-logic bug. `closed`
        (an int, 0 while open) is psycopg2's own liveness flag — checked
        before every statement, not caught reactively after a failure, so
        a dead connection is replaced before it ever gets a chance to
        raise."""
        if self._conn.closed:
            logger.warning("history.db Postgres connection was closed; reconnecting")
            self._connect()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> _PGCursorAdapter:
        self._reconnect_if_dead()
        cur = self._conn.cursor()
        cur.execute(_PLACEHOLDER_RE.sub("%s", sql), tuple(params))
        return _PGCursorAdapter(cur)

    def executescript(self, sql: str) -> None:
        """psycopg2 has no `executescript()` — but a plain cursor's own
        `.execute()` already runs multiple `;`-separated simple
        statements (no PL/pgSQL `$$` blocks appear anywhere in this
        codebase's own DDL) in one call, which is all any `..._DDL`
        constant here ever needs."""
        self._reconnect_if_dead()
        cur = self._conn.cursor()
        cur.execute(sql)

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def is_postgres(connection: Any) -> bool:
    """The one predicate `sink.py`/`ledger.py`/`deferral.py` each use to
    pick between their own SQLite and Postgres DDL constants — see this
    module's own top docstring for why DDL specifically is never
    auto-translated."""
    return isinstance(connection, PGConnection)


def open_connection(path_or_url: str) -> Any:
    """`history_db.py`'s own single entry point: a `postgresql://`/
    `postgres://` URL opens a real Supabase connection; anything else
    (a local filesystem path, or `:memory:`) opens SQLite exactly as
    before. Returns a `sqlite3.Connection` or a `PGConnection` — either
    one satisfies every call site in this codebase, per this module's own
    top docstring."""
    if is_postgres_url(path_or_url):
        logger.info("history.db: connecting to Postgres (Supabase)")
        return PGConnection(path_or_url)
    import sqlite3

    p = Path(path_or_url)
    if str(p) != ":memory:":
        p.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(p), check_same_thread=False)
