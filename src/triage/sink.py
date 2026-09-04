"""SQLite terminal sink, upserted by the stable idempotency key."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from .contracts import Event


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


class SQLiteSink:
    """Persist the latest successful delivery for each business operation."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.initialize()

    def initialize(self) -> None:
        self.connection.executescript(EVENTS_SINK_DDL)
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
