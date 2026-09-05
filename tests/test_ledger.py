"""ledger.py: the hash-chained audit ledger, its verification, CSV export,
and the decision-trace ring buffer."""

from __future__ import annotations

import csv
import io

import pytest

from triage import ledger
from triage.contracts import Decision, DecisionTrace, EventType, Tier
from triage.ledger import GENESIS_HASH, SQLiteLedger


def make_trace(seq: int = 1, event_id: str | None = None, tier: Tier = Tier.P2) -> DecisionTrace:
    return DecisionTrace(
        seq=seq, event_id=event_id or f"evt-{seq}", type=EventType.CLICK, tier=tier,
        decision=Decision.SAMPLE_ROLLUP, reason="test", pressure=0.5, value=5.0, ts=1000.0 + seq,
    )


# --------------------------------------------------------------------------
# The hash chain itself
# --------------------------------------------------------------------------


def test_first_row_chains_to_the_genesis_hash():
    store = SQLiteLedger()
    store.record(1, Decision.DEFER, "first", 0.5, Tier.P1, now=1000.0)
    row = store.rows()[0]
    assert row["prev_hash"] == GENESIS_HASH


def test_each_row_chains_to_the_previous_rows_own_hash():
    store = SQLiteLedger()
    store.record(1, Decision.DEFER, "first", 0.5, Tier.P1, now=1000.0)
    store.record(2, Decision.SHED, "second", 0.96, Tier.P2, now=1001.0)
    rows = store.rows()
    assert rows[1]["prev_hash"] == rows[0]["row_hash"]


def test_row_hash_is_deterministic_given_the_same_inputs():
    a = SQLiteLedger()
    b = SQLiteLedger()
    a.record(1, Decision.DEFER, "same reason", 0.6, Tier.P1, now=2000.0)
    b.record(1, Decision.DEFER, "same reason", 0.6, Tier.P1, now=2000.0)
    assert a.rows()[0]["row_hash"] == b.rows()[0]["row_hash"]


def test_a_different_reason_produces_a_different_hash():
    a = SQLiteLedger()
    b = SQLiteLedger()
    a.record(1, Decision.DEFER, "reason A", 0.6, Tier.P1, now=2000.0)
    b.record(1, Decision.DEFER, "reason B", 0.6, Tier.P1, now=2000.0)
    assert a.rows()[0]["row_hash"] != b.rows()[0]["row_hash"]


def test_verify_chain_is_true_for_an_untouched_ledger():
    store = SQLiteLedger()
    for i in range(20):
        store.record(i, Decision.MICRO_BATCH, f"reason {i}", 0.5, Tier.P1, now=1000.0 + i)
    result = store.verify_chain()
    assert result.ok is True
    assert bool(result) is True


def test_verify_chain_is_true_for_an_empty_ledger():
    store = SQLiteLedger()
    assert store.verify_chain().ok is True


# --------------------------------------------------------------------------
# Tamper detection — the explicit acceptance line for this stage
# --------------------------------------------------------------------------


@pytest.mark.parametrize("column,new_value", [
    ("reason", "tampered reason"),
    ("pressure", 0.01),
    ("decision", "SHED"),
    ("tier", "P0"),
    ("seq", 9999),
])
def test_mutating_any_row_column_breaks_verification(column, new_value):
    store = SQLiteLedger()
    for i in range(5):
        store.record(i, Decision.DEFER, f"reason {i}", 0.6, Tier.P1, now=1000.0 + i)
    assert store.verify_chain().ok is True

    store.connection.execute(
        f"UPDATE audit_ledger SET {column} = ? WHERE ledger_id = ?", (new_value, 3)
    )
    store.connection.commit()

    result = store.verify_chain()
    assert result.ok is False
    assert result.broken_at is not None
    assert bool(result) is False


def test_deleting_a_middle_row_breaks_verification():
    store = SQLiteLedger()
    for i in range(5):
        store.record(i, Decision.DEFER, f"reason {i}", 0.6, Tier.P1, now=1000.0 + i)
    store.connection.execute("DELETE FROM audit_ledger WHERE ledger_id = 3")
    store.connection.commit()

    result = store.verify_chain()
    assert result.ok is False


def test_tampering_with_the_row_hash_itself_is_also_caught():
    """A tamperer might try to patch row_hash to match a forged row's own
    fields — but row_hash is a UNIQUE column and the *next* row's
    prev_hash still points at the ORIGINAL hash, so the chain still
    breaks at the following link even if this row's own recomputed hash
    happens to line up."""
    store = SQLiteLedger()
    for i in range(5):
        store.record(i, Decision.DEFER, f"reason {i}", 0.6, Tier.P1, now=1000.0 + i)
    store.connection.execute(
        "UPDATE audit_ledger SET reason = 'forged', row_hash = 'deadbeef' || row_hash "
        "WHERE ledger_id = 3"
    )
    store.connection.commit()
    assert store.verify_chain().ok is False


def test_verify_chain_reports_the_first_break_not_a_later_one():
    store = SQLiteLedger()
    for i in range(5):
        store.record(i, Decision.DEFER, f"reason {i}", 0.6, Tier.P1, now=1000.0 + i)
    store.connection.execute("UPDATE audit_ledger SET reason = 'x' WHERE ledger_id = 2")
    store.connection.commit()
    result = store.verify_chain()
    assert result.broken_at == 2


# --------------------------------------------------------------------------
# CSV export
# --------------------------------------------------------------------------


def test_export_csv_has_a_header_and_one_row_per_record():
    """Header text is the friendly `CSV_HEADER_LABELS` (Phase J8's own
    live-demo readability fix — a raw Unix `recorded_ts` float meant
    nothing to a person opening this in a spreadsheet), not the raw,
    DB-facing `CSV_COLUMNS` names `verify_chain()` still reads by."""
    store = SQLiteLedger()
    for i in range(3):
        store.record(i, Decision.DEFER, f"reason {i}", 0.6, Tier.P1, now=1000.0 + i)
    text = store.export_csv()
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    assert rows[0] == [
        "Entry ID", "Recorded At (UTC)", "Event Sequence", "Decision", "Reason",
        "System Pressure", "Priority Tier", "Previous Row Hash", "Row Hash",
    ]
    assert len(rows) == 4  # header + 3 records
    # recorded_ts is reformatted to a readable UTC string, not the raw
    # Unix float — same column, same position, just a human-readable value.
    assert rows[1][1] == "1970-01-01 00:16:40.000000 UTC"  # 1000.0s since epoch


def test_export_csv_of_an_empty_ledger_is_just_the_header():
    store = SQLiteLedger()
    text = store.export_csv()
    rows = list(csv.reader(io.StringIO(text)))
    assert len(rows) == 1


def test_export_csv_survives_a_reason_containing_a_comma():
    store = SQLiteLedger()
    store.record(1, Decision.SHED, "pressure 0.96, hard shed", 0.96, Tier.P2, now=1000.0)
    text = store.export_csv()
    rows = list(csv.reader(io.StringIO(text)))
    assert rows[1][4] == "pressure 0.96, hard shed"


# --------------------------------------------------------------------------
# The decision-trace ring buffer
# --------------------------------------------------------------------------


def test_get_trace_finds_a_recorded_trace_by_event_id():
    store = SQLiteLedger()
    trace = make_trace(seq=1, event_id="evt-abc")
    store.record_trace(trace)
    assert store.get_trace("evt-abc") is trace


def test_get_trace_returns_none_for_an_unknown_event_id():
    store = SQLiteLedger()
    assert store.get_trace("never-recorded") is None


def test_recent_traces_are_newest_first():
    store = SQLiteLedger()
    store.record_trace(make_trace(seq=1, event_id="a"))
    store.record_trace(make_trace(seq=2, event_id="b"))
    traces = store.recent_traces()
    assert [t.event_id for t in traces] == ["b", "a"]


def test_ring_buffer_evicts_the_oldest_trace_past_500():
    store = SQLiteLedger()
    for i in range(501):
        store.record_trace(make_trace(seq=i, event_id=f"evt-{i}"))
    assert len(store.recent_traces()) == 500
    assert store.get_trace("evt-0") is None, "the oldest trace must have been evicted"
    assert store.get_trace("evt-500") is not None


def test_ring_buffer_eviction_does_not_clobber_a_newer_duplicate_event_id():
    """A later re-recording of the same event_id (e.g. a retry) already
    overwrote the index; evicting the much-older, still-physically-present
    tail copy of that same id must not delete the newer entry the index
    actually points at.

    Constructed so the two copies age out at different times: the ORIGINAL
    "dup" is recorded first (so it stays the oldest item, always at the
    tail, however much else gets added after it) and stays well short of
    the buffer's capacity when "dup" is re-recorded — so both copies are
    briefly, physically present in the deque at once, with the index
    already repointed at the newer one. Only enough further insertions to
    evict the stale original follow; `newer` itself must not have aged out
    yet by the time that happens.
    """
    store = SQLiteLedger()
    store.record_trace(make_trace(seq=0, event_id="dup"))
    for i in range(1, 10):
        store.record_trace(make_trace(seq=i, event_id=f"evt-{i}"))
    newer = make_trace(seq=999, event_id="dup")
    store.record_trace(newer)
    # Exactly enough further insertions (10 + 1 + 490 = 501) that the one
    # eviction this triggers is the original "dup" — still the oldest item
    # in the deque this whole time — not `newer`.
    for i in range(10, 500):
        store.record_trace(make_trace(seq=i, event_id=f"evt-{i}"))
    assert store.get_trace("dup") is newer


def test_only_frozen_decisiontrace_fields_are_used():
    """This stage's own instruction: "add derived fields only after an
    explicit contract review." The ring buffer stores DecisionTrace
    objects verbatim -- there is no wrapper type here that could carry
    anything DecisionTrace itself doesn't already have."""
    trace = make_trace()
    assert set(type(trace).model_fields) == {
        "seq", "event_id", "type", "tier", "decision", "reason",
        "pressure", "value", "ts",
    }


# --------------------------------------------------------------------------
# Module-level default store: record()/reset() wiring
# --------------------------------------------------------------------------


def test_module_level_record_and_reset():
    ledger.reset()
    ledger.record(1, Decision.DEFER, "reason", 0.6, Tier.P1)
    assert ledger.total_recorded() == 1
    assert ledger.verify_chain().ok is True

    ledger.reset()
    assert ledger.total_recorded() == 0
    assert ledger.retained() == 0


def test_reset_also_clears_the_trace_ring_buffer():
    ledger.reset()
    ledger.record_trace(make_trace(event_id="before-reset"))
    assert ledger.get_trace("before-reset") is not None

    ledger.reset()
    assert ledger.get_trace("before-reset") is None
    assert ledger.recent_traces() == ()
