"""sink.py: the terminal events_sink table (Stage B) plus the Stage E
rollups table — the durable audit trail for reservoir-sampled windows.
"""

from __future__ import annotations

import json

import pytest

from triage.ladder import Rollup
from triage.sink import SQLiteSink


def make_rollup(
    *, event_type: str = "click", window_start: float = 100.0, window_end: float = 101.0,
    sample_weight: float = 10.0, observed_count: int = 1, seq_low: int = 1, seq_high: int = 10,
) -> Rollup:
    return Rollup(
        event_type=event_type, window_start=window_start, window_end=window_end,
        sample_weight=sample_weight, observed_count=observed_count,
        subtype_counts={event_type: observed_count}, seq_low=seq_low, seq_high=seq_high,
    )


def test_write_rollup_persists_a_row_and_returns_its_id():
    sink = SQLiteSink()
    rollup_id = sink.write_rollup(make_rollup())
    assert sink.rollup_count() == 1

    row = sink.connection.execute(
        "SELECT * FROM rollups WHERE rollup_id = ?", (rollup_id,)
    ).fetchone()
    assert row is not None
    assert row["event_type"] == "click"
    assert row["sample_weight"] == 10.0
    assert row["observed_count"] == 1
    assert row["seq_low"] == 1
    assert row["seq_high"] == 10
    assert json.loads(row["subtype_counts"]) == {"click": 1}


def test_write_rollup_gives_each_window_a_distinct_id():
    sink = SQLiteSink()
    first = sink.write_rollup(make_rollup(window_start=0.0, window_end=1.0))
    second = sink.write_rollup(make_rollup(window_start=1.0, window_end=2.0))
    assert first != second
    assert sink.rollup_count() == 2


def test_rollup_count_starts_at_zero():
    sink = SQLiteSink()
    assert sink.rollup_count() == 0


def test_seq_high_must_be_at_least_seq_low():
    """The DDL's own CHECK constraint — a rollup that claims to cover a
    backwards sequence range is a bug, not a valid row."""
    sink = SQLiteSink()
    with pytest.raises(Exception):
        sink.write_rollup(make_rollup(seq_low=10, seq_high=1))
