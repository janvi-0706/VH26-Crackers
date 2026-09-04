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


class SQLiteSink:
    """Persist the latest successful delivery for each business operation."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._rollup_seq = 0
        self.initialize()

    def initialize(self) -> None:
        self.connection.executescript(EVENTS_SINK_DDL)
        self.connection.executescript(ROLLUPS_DDL)
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


def count() -> int:
    return _default_sink.count()


def write_rollup(rollup: Rollup, *, now: float | None = None) -> str:
    return _default_sink.write_rollup(rollup, now=now)


def rollup_count() -> int:
    return _default_sink.rollup_count()
