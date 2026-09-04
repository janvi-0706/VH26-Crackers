"""Audit ledger — append-only, hash-chained, real as of this stage.

Owner: Lane D.

Schema and hash-chain rule are exactly docs/DATA_MODEL.md section 6's own
design contract — this file is its first implementation, not a new design
decided here. For ledger row n:

    canonical = "ledger_id|recorded_ts|seq|decision|reason|pressure|tier|prev_hash"
    row_hash  = SHA-256(canonical, UTF-8)
    prev_hash = row (n-1)'s own row_hash; the genesis row uses GENESIS_HASH
                (64 zero hex characters, a published constant).

Fixed separators, a fixed decimal representation for the two REAL columns
(recorded_ts, pressure), and integer decimal form for the two INTEGER
columns are what make this reproducible: two logically-equal SQLite REAL
values that happen to round-trip through Python float formatting
differently would otherwise hash differently, and a verifier re-deriving
the chain from the stored columns would then disagree with itself.

verify_chain() re-derives every row's hash from its own stored columns and
checks it against the stored row_hash, and checks every row's prev_hash
against its predecessor's actual row_hash — a verifier walking from the
known genesis hash to a trusted current head. What this catches: a changed
historical row (its recomputed hash no longer matches what's stored), a
deleted middle row (the chain breaks at the gap), an inserted or reordered
row (same). What it does NOT catch (docs/DATA_MODEL.md's own honest list):
an attacker who rewrites both the database and the trusted head hash, a
false value recorded faithfully at decision time, or corruption outside
this table entirely.

This module also owns the ring buffer of decision traces (this stage's own
addition, not in the original Stage A stub) — 500 most recent, queryable
by event_id, built only from DecisionTrace's own frozen fields. No table:
"ring buffer" is the literal spec, and nothing here needs SQL's
durability-across-restart guarantee the way the audit ledger genuinely
does — a dashboard/API convenience index, not evidence.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import sqlite3
import time
from collections import deque
from pathlib import Path
from typing import Any, Iterable

from .contracts import Decision, DecisionTrace, Tier

logger = logging.getLogger(__name__)

# Published constant, per docs/DATA_MODEL.md section 6 — the genesis row's
# prev_hash, standing in for "row -1" that does not exist.
GENESIS_HASH = "0" * 64

AUDIT_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS audit_ledger (
    ledger_id INTEGER PRIMARY KEY,
    recorded_ts REAL NOT NULL,
    seq INTEGER NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN
        ('STREAM_NOW', 'MICRO_BATCH', 'DEFER', 'SAMPLE_ROLLUP', 'SHED')),
    reason TEXT NOT NULL,
    pressure REAL NOT NULL,
    tier TEXT NOT NULL CHECK (tier IN ('P0', 'P1', 'P2')),
    prev_hash TEXT NOT NULL,
    row_hash TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_audit_ledger_seq
    ON audit_ledger (seq);
CREATE INDEX IF NOT EXISTS idx_audit_ledger_tier_decision_ts
    ON audit_ledger (tier, decision, recorded_ts DESC);
"""

CSV_COLUMNS = (
    "ledger_id", "recorded_ts", "seq", "decision", "reason",
    "pressure", "tier", "prev_hash", "row_hash",
)

# Ring buffer size for decision traces — this stage's own spec, literally.
TRACE_BUFFER_SIZE = 500

# A 30-hour run at spike rate is tens of millions of rows in the durable
# ledger — deliberately NOT capped here (docs/DATA_MODEL.md's own retention
# note: "keep the 30-hour run", archive beyond that). A demo session never
# approaches that; nothing in this stage adds a growth bound the doc itself
# doesn't already call for.


def _canonical_bytes(
    ledger_id: int, recorded_ts: float, seq: int, decision: str,
    reason: str, pressure: float, tier: str, prev_hash: str,
) -> bytes:
    """The exact pipe-separated sequence docs/DATA_MODEL.md section 6
    specifies. Fixed formatting on every field a naive str() would render
    ambiguously: recorded_ts and pressure both get a fixed-precision
    decimal (`%.6f` — six places comfortably exceeds this project's own
    rounding elsewhere, e.g. pressure is already rounded to 4dp before it
    gets here, so 6dp never loses information re-deriving the hash later),
    and the two integers use plain decimal form. `reason` is free text and
    is not escaped further — it cannot contain the `|` separator in
    practice (every reason string in this codebase is a hand-written
    sentence), and this function's only job is byte-for-byte
    reproducibility of what was actually stored, not defending against a
    reason string that was never going to occur.
    """
    fields = (
        str(int(ledger_id)),
        f"{float(recorded_ts):.6f}",
        str(int(seq)),
        str(decision),
        str(reason),
        f"{float(pressure):.6f}",
        str(tier),
        str(prev_hash),
    )
    return "|".join(fields).encode("utf-8")


def _hash(canonical: bytes) -> str:
    return hashlib.sha256(canonical).hexdigest()


class ChainVerification:
    """The result of verify_chain(): ok, or the first row where the chain
    breaks and why — enough for an operator to know both *that* the log
    was tampered with and *where* to start looking."""

    __slots__ = ("ok", "broken_at", "reason")

    def __init__(self, ok: bool, broken_at: int | None = None, reason: str = ""):
        self.ok = ok
        self.broken_at = broken_at
        self.reason = reason

    def __bool__(self) -> bool:
        return self.ok

    def __repr__(self) -> str:  # pragma: no cover - debugging convenience
        if self.ok:
            return "ChainVerification(ok=True)"
        return f"ChainVerification(ok=False, broken_at={self.broken_at}, reason={self.reason!r})"


class SQLiteLedger:
    """Durable, hash-chained, append-only. Defaults to `:memory:`, matching
    sink.py/deferral.py's own demo-scale default."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(AUDIT_LEDGER_DDL)
        self.connection.commit()

        self._last_hash = self._load_last_hash()
        self._next_id = self._load_next_id()
        self._total_recorded = self._count_rows()

        # The ring buffer of decision traces — separate from the durable
        # SQL table above entirely (see this module's own docstring).
        self._trace_buffer: deque[DecisionTrace] = deque(maxlen=TRACE_BUFFER_SIZE)
        self._trace_by_event_id: dict[str, DecisionTrace] = {}

    # -- durable, hash-chained rows ---------------------------------------

    def _count_rows(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM audit_ledger").fetchone()
        return int(row[0])

    def _load_last_hash(self) -> str:
        row = self.connection.execute(
            "SELECT row_hash FROM audit_ledger ORDER BY ledger_id DESC LIMIT 1"
        ).fetchone()
        return row["row_hash"] if row is not None else GENESIS_HASH

    def _load_next_id(self) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(ledger_id), 0) + 1 FROM audit_ledger"
        ).fetchone()
        return int(row[0])

    def record(
        self,
        seq: int,
        decision: Decision,
        reason: str,
        pressure: float,
        tier: Tier,
        now: float | None = None,
    ) -> None:
        """Append one hash-chained row. Never raises: losing an audit row
        must not take down the pipeline that produced it — the same
        guarantee the Stage A stub already made, kept here even though the
        body underneath it is now real I/O that could, in principle, fail.
        """
        now = time.time() if now is None else now
        decision = Decision(decision)
        tier = Tier(tier)
        pressure_r = round(float(pressure), 4)

        try:
            ledger_id = self._next_id
            prev_hash = self._last_hash
            canonical = _canonical_bytes(
                ledger_id, now, seq, decision.value, reason, pressure_r, tier.value, prev_hash
            )
            row_hash = _hash(canonical)
            self.connection.execute(
                """
                INSERT INTO audit_ledger (
                    ledger_id, recorded_ts, seq, decision, reason,
                    pressure, tier, prev_hash, row_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ledger_id, now, seq, decision.value, reason, pressure_r, tier.value, prev_hash, row_hash),
            )
            self.connection.commit()
            self._last_hash = row_hash
            self._next_id += 1
            self._total_recorded += 1
        except Exception:  # noqa: BLE001 - an audit-write failure must not crash the pipeline
            logger.exception("failed to append audit ledger row (seq=%s)", seq)

    def rows(self) -> list[sqlite3.Row]:
        """Every row, oldest first — the chain's own order."""
        return self.connection.execute(
            "SELECT * FROM audit_ledger ORDER BY ledger_id ASC"
        ).fetchall()

    def total_recorded(self) -> int:
        return self._total_recorded

    def retained(self) -> int:
        return self._count_rows()

    def verify_chain(self) -> ChainVerification:
        """Walk the whole chain from the genesis hash, re-deriving each
        row's hash from its own stored columns and checking both that hash
        and the prev_hash link to the row before it. The first break wins
        — a tampered row further down the chain than the true first break
        is not itself evidence of anything once an earlier link is
        already broken.
        """
        expected_prev = GENESIS_HASH
        for row in self.rows():
            if row["prev_hash"] != expected_prev:
                return ChainVerification(
                    False, row["ledger_id"],
                    f"prev_hash does not match row {row['ledger_id'] - 1}'s row_hash "
                    "— a row was deleted, inserted, or reordered",
                )
            recomputed = _hash(
                _canonical_bytes(
                    row["ledger_id"], row["recorded_ts"], row["seq"], row["decision"],
                    row["reason"], row["pressure"], row["tier"], row["prev_hash"],
                )
            )
            if recomputed != row["row_hash"]:
                return ChainVerification(
                    False, row["ledger_id"],
                    f"row_hash does not match this row's own stored columns "
                    "— the row was tampered with after being written",
                )
            expected_prev = row["row_hash"]
        return ChainVerification(True)

    def export_csv(self) -> str:
        """The whole durable ledger, CSV, header first — GET /audit.csv's
        own body. `csv.writer` handles quoting a `reason` string that
        happens to contain a comma or a newline; hand-joining columns with
        `,` would not."""
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(CSV_COLUMNS)
        for row in self.rows():
            writer.writerow(row[col] for col in CSV_COLUMNS)
        return buffer.getvalue()

    def close(self) -> None:
        self.connection.close()

    # -- the decision-trace ring buffer ------------------------------------

    def record_trace(self, trace: DecisionTrace) -> None:
        """Fold one DecisionTrace into the 500-item ring buffer, indexed by
        event_id. No fields beyond DecisionTrace's own frozen ones are
        read or stored here — this stage's own instruction: "add derived
        fields only after an explicit contract review."""
        buf = self._trace_buffer
        if len(buf) == (buf.maxlen or 0):
            evicted = buf[-1]
            # Only drop the index entry if it still points at the very
            # object being evicted — a duplicate event_id recorded again
            # more recently would already have overwritten the index, and
            # evicting the old *tail* copy must not blow away that newer
            # entry.
            if self._trace_by_event_id.get(evicted.event_id) is evicted:
                del self._trace_by_event_id[evicted.event_id]
        buf.appendleft(trace)
        self._trace_by_event_id[trace.event_id] = trace

    def get_trace(self, event_id: str) -> DecisionTrace | None:
        return self._trace_by_event_id.get(event_id)

    def recent_traces(self) -> tuple[DecisionTrace, ...]:
        """Newest first, matching _recent_decisions' own convention."""
        return tuple(self._trace_buffer)



# --------------------------------------------------------------------------
# Module-level default store — matches sink.py/deferral.py's own ambient
# precedent (one pipeline, one process, CLAUDE.md hard rule 1).
# --------------------------------------------------------------------------

_default_ledger = SQLiteLedger()


def record(
    seq: int,
    decision: Decision,
    reason: str,
    pressure: float,
    tier: Tier,
    now: float | None = None,
) -> None:
    _default_ledger.record(seq, decision, reason, pressure, tier, now=now)


def record_trace(trace: DecisionTrace) -> None:
    _default_ledger.record_trace(trace)


def get_trace(event_id: str) -> DecisionTrace | None:
    return _default_ledger.get_trace(event_id)


def recent_traces() -> tuple[DecisionTrace, ...]:
    return _default_ledger.recent_traces()


def records() -> Iterable[dict[str, Any]]:
    """Every durable row, oldest first, as plain dicts — kept for the
    call sites (tests, earlier stages) that used the Stage A stub's own
    dict shape rather than a sqlite3.Row."""
    return tuple(dict(row) for row in _default_ledger.rows())


def rows() -> list[sqlite3.Row]:
    return _default_ledger.rows()


def total_recorded() -> int:
    return _default_ledger.total_recorded()


def retained() -> int:
    return _default_ledger.retained()


def verify_chain() -> ChainVerification:
    return _default_ledger.verify_chain()


def export_csv() -> str:
    return _default_ledger.export_csv()


def reset() -> None:
    """Tests only — same contract the Stage A stub already had: a fully
    fresh ledger, exactly mirroring deferral.reset_default_store()'s own
    "swap in a new store" mechanic. `Engine.reset()` (app.py) already
    calls this today, same as every stage before this one — a behaviour
    change to make the live /control/reset endpoint stop touching the
    audit trail is a real, separate design decision this prompt does not
    ask for, so it is flagged here rather than made silently."""
    global _default_ledger
    _default_ledger.close()
    _default_ledger = SQLiteLedger()
