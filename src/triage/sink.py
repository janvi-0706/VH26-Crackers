"""SQLite terminal sink, upserted by the stable idempotency key.

Stage E adds the `rollups` table: the durable audit trail for every
reservoir-sampled window (ladder.Rollup) — schema exactly per
docs/DATA_MODEL.md's own `rollups` DDL, persisted here because sink.py is
already this project's "SQLite is the single-process durable edge" module
(that document's own framing).

Deliberately NOT the source the live dashboard number reads from, though:
`weighted_click_count` lives in metrics.py instead (see that module's own
note on why), because it has to reset in lockstep with `true_click_count`
on every /control/reset for the two to stay comparable, and this sink is
durable across a reset by design — same as `events_sink` itself already is.
This table is the reconciliation record docs/DATA_MODEL.md describes
("compares rollup coverage ... with sampled-out counters"), not the
dashboard's data source.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from .contracts import SCHEMA_VERSION, Event
from .ladder import Rollup

EVENTS_SINK_DDL = """
CREATE TABLE IF NOT EXISTS events_sink (
    idempotency_key TEXT PRIMARY KEY,
    dedup_key TEXT NOT NULL,
    latest_event_id TEXT NOT NULL,
    latest_seq INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    tier TEXT NOT NULL CHECK (tier IN ('P0', 'P1', 'P2')),
    payload_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    first_ingest_ts REAL NOT NULL,
    committed_ts REAL NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1 CHECK (attempt_count >= 1)
);
CREATE INDEX IF NOT EXISTS idx_events_sink_dedup_key
    ON events_sink (dedup_key);
CREATE INDEX IF NOT EXISTS idx_events_sink_partition_seq
    ON events_sink (partition_key, latest_seq);
CREATE INDEX IF NOT EXISTS idx_events_sink_committed_ts
    ON events_sink (committed_ts);
"""

ROLLUPS_DDL = """
CREATE TABLE IF NOT EXISTS rollups (
    rollup_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    window_start REAL NOT NULL,
    window_end REAL NOT NULL CHECK (window_end > window_start),
    sample_weight REAL NOT NULL CHECK (sample_weight >= 1.0),
    observed_count INTEGER NOT NULL CHECK (observed_count >= 0),
    subtype_counts TEXT NOT NULL,
    seq_low INTEGER NOT NULL,
    seq_high INTEGER NOT NULL CHECK (seq_high >= seq_low),
    created_ts REAL NOT NULL,
    schema_version INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rollups_type_window
    ON rollups (event_type, window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_rollups_seq_coverage
    ON rollups (seq_low, seq_high);
CREATE INDEX IF NOT EXISTS idx_rollups_window
    ON rollups (window_start DESC, window_end DESC);
"""

# Phase J6: "historical SLA outcomes" — docs/DATA_MODEL.md's own table list
# never named this one explicitly before this phase (see that document's
# own new section added alongside this DDL); the need only became concrete
# once completions could arrive from three different processes and ingress
# had to durably remember, per event, whether its SLA was actually met —
# metrics.py's own sla_met/sla_missed counters are in-memory, per-tier
# aggregates that reset on every /control/reset (correct for a live demo
# gauge, wrong for a historical record). One row per terminal completion,
# `source` naming whichever process actually served it — this is what lets
# a post-spike query ask "how did P0 do, specifically on server1" rather
# than only "how did P0 do, in aggregate".
SLA_OUTCOMES_DDL = """
CREATE TABLE IF NOT EXISTS sla_outcomes (
    outcome_id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    tier TEXT NOT NULL CHECK (tier IN ('P0', 'P1', 'P2')),
    event_type TEXT NOT NULL,
    value REAL NOT NULL,
    met INTEGER NOT NULL CHECK (met IN (0, 1)),
    latency_ms REAL NOT NULL,
    recorded_ts REAL NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ingress', 'server1', 'server2'))
);
CREATE INDEX IF NOT EXISTS idx_sla_outcomes_tier_met_ts
    ON sla_outcomes (tier, met, recorded_ts DESC);
CREATE INDEX IF NOT EXISTS idx_sla_outcomes_event_id
    ON sla_outcomes (event_id);
CREATE INDEX IF NOT EXISTS idx_sla_outcomes_source_ts
    ON sla_outcomes (source, recorded_ts DESC);
"""


class SQLiteSink:
    """Persist the latest successful delivery for each business operation.

    Phase J6: `connection`, when given, is used as is instead of opening a
    new one — see `deferral.DeferralStore`'s own docstring for why (the
    same `history_db.py`-owned, WAL-mode, shared connection this module,
    `ledger.py`, and `deferral.py` all now write through)."""

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
        self._rollup_seq = 0
        self.initialize()

    def initialize(self) -> None:
        self.connection.executescript(EVENTS_SINK_DDL)
        self.connection.executescript(ROLLUPS_DDL)
        self.connection.executescript(SLA_OUTCOMES_DDL)
        self.connection.commit()

    def write(self, event: Event) -> bool:
        """Upsert one event and return whether the write succeeded."""
        committed_ts = time.time()
        self.connection.execute(
            """
            INSERT INTO events_sink (
                idempotency_key, dedup_key, latest_event_id, latest_seq,
                partition_key, event_type, tier, payload_json, schema_version,
                first_ingest_ts, committed_ts, attempt_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                dedup_key = excluded.dedup_key,
                latest_event_id = excluded.latest_event_id,
                latest_seq = excluded.latest_seq,
                partition_key = excluded.partition_key,
                event_type = excluded.event_type,
                tier = excluded.tier,
                payload_json = excluded.payload_json,
                schema_version = excluded.schema_version,
                committed_ts = excluded.committed_ts,
                attempt_count = events_sink.attempt_count + 1
            """,
            (
                event.idempotency_key,
                event.dedup_key,
                event.event_id,
                event.seq,
                event.partition_key,
                event.type.value,
                event.tier.value,
                event.model_dump_json(),
                event.schema_version,
                event.ingest_ts,
                committed_ts,
            ),
        )
        self.connection.commit()
        return True

    def read(self, idempotency_key: str) -> Event | None:
        row = self.connection.execute(
            "SELECT payload_json FROM events_sink WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        return Event.model_validate_json(row["payload_json"]) if row else None

    get = read

    def recent(self, n: int) -> list[Event]:
        """The `n` most recently committed rows, newest first. Stage I's
        chaos duplicate-flood reads from here rather than from any
        in-memory ring buffer: this table already durably holds a full
        `Event` payload per business fact, keyed by `idempotency_key`, so
        "N recent events to replay" is a real read of what the pipeline
        actually, durably processed — not a second, parallel notion of
        "recent" this file would have to keep in sync with the first."""
        rows = self.connection.execute(
            "SELECT payload_json FROM events_sink ORDER BY committed_ts DESC LIMIT ?",
            (n,),
        ).fetchall()
        return [Event.model_validate_json(row["payload_json"]) for row in rows]

    def count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM events_sink").fetchone()
        return int(row[0])

    def write_rollup(self, rollup: Rollup, *, now: float | None = None) -> str:
        """Persist one finished reservoir window (a ladder.Rollup) as the
        durable audit trail docs/DATA_MODEL.md describes. `rollup_id` is
        generated here, not carried on the dataclass — ladder.py's job is
        the sampling arithmetic, not durable-row identity.

        Returns the generated rollup_id, mostly so tests can look the row
        back up without guessing it.
        """
        now = time.time() if now is None else now
        self._rollup_seq += 1
        rollup_id = f"rollup-{rollup.event_type}-{self._rollup_seq}"
        self.connection.execute(
            """
            INSERT INTO rollups (
                rollup_id, event_type, window_start, window_end,
                sample_weight, observed_count, subtype_counts,
                seq_low, seq_high, created_ts, schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rollup_id,
                rollup.event_type,
                rollup.window_start,
                rollup.window_end,
                rollup.sample_weight,
                rollup.observed_count,
                json.dumps(rollup.subtype_counts),
                rollup.seq_low,
                rollup.seq_high,
                now,
                SCHEMA_VERSION,
            ),
        )
        self.connection.commit()
        return rollup_id

    def rollup_count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM rollups").fetchone()
        return int(row[0])

    def write_outcome(
        self,
        event: Event,
        *,
        met: bool,
        latency_ms: float,
        source: str,
        now: float | None = None,
    ) -> int:
        """One durable row per terminal completion — Phase J6's own
        `sla_outcomes` table (this module's own top docstring has the
        full reasoning for why this exists alongside metrics.py's
        in-memory sla_met/sla_missed counters rather than instead of
        them). `source` is whichever process actually served the event
        ('ingress' for Engine's own local pipeline, 'server1'/'server2'
        for a real split-topology completion) — carried explicitly
        rather than inferred from `event.tier`, since inferring it would
        quietly break the day ingress's own Engine ever serves a P1/P2
        event too (nothing today prevents that; Engine's local pipeline
        is tier-blind).

        Returns the generated `outcome_id`, the same convenience
        `write_rollup()` already offers for `rollup_id`.
        """
        now = time.time() if now is None else now
        cursor = self.connection.execute(
            """
            INSERT INTO sla_outcomes (
                event_id, seq, tier, event_type, value, met,
                latency_ms, recorded_ts, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id, event.seq, event.tier.value, event.type.value,
                event.value, 1 if met else 0, latency_ms, now, source,
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def sla_outcome_count(
        self, *, tier: str | None = None, met: bool | None = None, source: str | None = None,
    ) -> int:
        query = "SELECT COUNT(*) FROM sla_outcomes WHERE 1=1"
        params: list[object] = []
        if tier is not None:
            query += " AND tier = ?"
            params.append(tier)
        if met is not None:
            query += " AND met = ?"
            params.append(1 if met else 0)
        if source is not None:
            query += " AND source = ?"
            params.append(source)
        row = self.connection.execute(query, params).fetchone()
        return int(row[0])

    def attempts(self, idempotency_key: str) -> int:
        row = self.connection.execute(
            "SELECT attempt_count FROM events_sink WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "SQLiteSink":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


_default_sink = SQLiteSink()


def write(event: Event) -> bool:
    return _default_sink.write(event)


def read(idempotency_key: str) -> Event | None:
    return _default_sink.read(idempotency_key)


def recent(n: int) -> list[Event]:
    return _default_sink.recent(n)


def count() -> int:
    return _default_sink.count()


def write_rollup(rollup: Rollup, *, now: float | None = None) -> str:
    return _default_sink.write_rollup(rollup, now=now)


def rollup_count() -> int:
    return _default_sink.rollup_count()


def write_outcome(
    event: Event, *, met: bool, latency_ms: float, source: str, now: float | None = None,
) -> int:
    return _default_sink.write_outcome(event, met=met, latency_ms=latency_ms, source=source, now=now)


def sla_outcome_count(
    *, tier: str | None = None, met: bool | None = None, source: str | None = None,
) -> int:
    return _default_sink.sla_outcome_count(tier=tier, met=met, source=source)


def reset_default_store() -> None:
    """Tests only, mirroring deferral.reset_default_store()/ledger.reset().
    Never called by Engine.reset(): `events_sink` is durable across a demo
    reset by the same design choice `deferral.py`'s own buffer already
    rests on (a completed business fact should not vanish because a
    presenter clicked Reset) — this exists only so a test suite running
    many independent real Engines back to back in one process (each
    sharing this one ambient sink, same as they share metrics/ledger) can
    give one test a clean, unambiguous view of "recent", the same reason
    every other ambient store's own reset exists."""
    global _default_sink
    _default_sink.close()
    _default_sink = SQLiteSink()


def configure_default(sink: SQLiteSink) -> None:
    """Phase J6: swap the ambient default sink for one already wired onto
    ingress's own shared, WAL-mode `history.db` connection — see
    `deferral.configure_default()`'s own docstring for the full reasoning;
    this is the same mechanism for this module."""
    global _default_sink
    _default_sink = sink
