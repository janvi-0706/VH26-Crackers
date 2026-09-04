"""Audit ledger — append-only record of every decision the pipeline made.

Owner: Lane D.

STAGE A STATUS: deliberate stub. record() appends to a bounded in-memory
deque and nothing else. The real implementation lands in Stage E/F: a
hash-chained, SQLite-backed ledger where each row carries the hash of the row
before it, so a shed event cannot be quietly removed from the record after the
fact.

The stub exists now so that the CALL SITES exist now. Instrumentation that is
retrofitted into code somebody else wrote gets added where it is easy, not
where it is correct, and it always misses a branch. Every decision path is
wired to this function from the first line of engine code that Lane A writes.

The public signature is frozen even though the body is not:

    record(seq, decision, reason, pressure, tier) -> None
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Iterable

from .contracts import Decision, Tier

# A 30-hour run at spike rate would be tens of millions of rows. The stub is
# in memory, so it is bounded; the real ledger is on disk and is not, but it
# gets a documented retention strategy instead (docs/DATA_MODEL.md).
MAX_RECORDS = 100_000

_records: deque[dict[str, Any]] = deque(maxlen=MAX_RECORDS)
_total_recorded = 0


def record(
    seq: int,
    decision: Decision,
    reason: str,
    pressure: float,
    tier: Tier,
) -> None:
    """Append one decision to the ledger.

    Called from metrics.observe_decision(), which is the single choke point
    every decision passes through. Never raises: losing an audit row must not
    take down the pipeline that produced it.
    """
    global _total_recorded

    _total_recorded += 1
    _records.append(
        {
            "seq": seq,
            "decision": Decision(decision).value,
            "reason": reason,
            "pressure": round(float(pressure), 4),
            "tier": Tier(tier).value,
            "ts": time.time(),
            # Stage E fills these in: prev_hash chains the row to its
            # predecessor, row_hash is the hash of this row's contents.
            "prev_hash": None,
            "row_hash": None,
        }
    )


def records() -> Iterable[dict[str, Any]]:
    """The retained window, oldest first."""
    return tuple(_records)


def total_recorded() -> int:
    """Everything ever recorded, including rows aged out of the window."""
    return _total_recorded


def retained() -> int:
    return len(_records)


def reset() -> None:
    """Tests only."""
    global _total_recorded
    _records.clear()
    _total_recorded = 0
