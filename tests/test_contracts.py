"""Contract tests: round-trip serialisation, and a guard on the frozen schema.

The round-trip tests matter because the frame crosses a WebSocket into
JavaScript and comes back in nobody's control but the browser's. The schema
guard matters more: contracts.py is frozen after Stage A, and the field list
below is the thing four people agreed to. If a field disappears, this test
fails loudly rather than the dashboard rendering a blank panel at hour 20.
"""

from __future__ import annotations

import time

import pytest
from pydantic import ValidationError

from triage.contracts import (
    SCHEMA_VERSION,
    TIER_KEYS,
    Decision,
    DecisionTrace,
    Event,
    EventType,
    MetricsFrame,
    Mode,
    ShedRecord,
    Tier,
)


def make_event(**overrides) -> Event:
    now = time.time()
    base = dict(
        event_id="evt-0001",
        dedup_key="order:cust-42:2026-09-04T10:00:00",
        seq=17,
        partition_key="cust-42",
        idempotency_key="sink:order:cust-42:1",
        type=EventType.ORDER,
        tier=Tier.P0,
        payload_size=512,
        value=100.0,
        cost=3.0,
        ingest_ts=now,
        deadline_ts=now + 0.5,
    )
    base.update(overrides)
    return Event(**base)


# --------------------------------------------------------------------------
# Event
# --------------------------------------------------------------------------


def test_event_round_trips_through_json():
    event = make_event()
    restored = Event.model_validate_json(event.model_dump_json())
    assert restored == event
    assert restored.schema_version == SCHEMA_VERSION


def test_event_round_trips_through_dict():
    event = make_event()
    assert Event.model_validate(event.model_dump()) == event


def test_event_keeps_five_identity_fields_separate():
    """The retry case, which is the whole reason these are five fields.

    A retry is a NEW emission (new event_id, new seq) of the SAME business
    fact (same dedup_key, same partition_key) that must land on the SAME sink
    row (same idempotency_key). Collapse any pair and one of dedup, ordering
    or upsert breaks.
    """
    first = make_event()
    retry = first.model_copy(update={"event_id": "evt-0002", "seq": 98})

    assert retry.event_id != first.event_id  # new emission
    assert retry.seq != first.seq  # new position in the pipeline
    assert retry.dedup_key == first.dedup_key  # same business fact
    assert retry.partition_key == first.partition_key  # same ordering domain
    assert retry.idempotency_key == first.idempotency_key  # same sink row

    identity = {
        first.event_id, first.dedup_key, str(first.seq),
        first.partition_key, first.idempotency_key,
    }
    assert len(identity) == 5, "identity fields must be independently valued"


def test_event_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        make_event(priority="high")


def test_event_enums_deserialise_from_plain_strings():
    """The dashboard and the SQLite sink both hand us strings, not enums."""
    payload = make_event().model_dump()
    payload["type"] = "payment"
    payload["tier"] = "P0"
    restored = Event.model_validate(payload)
    assert restored.type is EventType.PAYMENT
    assert restored.tier is Tier.P0


# --------------------------------------------------------------------------
# Decision
# --------------------------------------------------------------------------


def test_decision_enum_is_exactly_the_five_outcomes():
    assert [d.value for d in Decision] == [
        "STREAM_NOW", "MICRO_BATCH", "DEFER", "SAMPLE_ROLLUP", "SHED",
    ]


def test_decision_serialises_as_its_name():
    trace = DecisionTrace(decision=Decision.SHED)
    assert '"decision":"SHED"' in trace.model_dump_json()


# --------------------------------------------------------------------------
# MetricsFrame
# --------------------------------------------------------------------------


def test_empty_frame_is_valid_and_round_trips():
    """A frame with nothing filled in must still be a legal frame — that is
    what lets the dashboard render before the engine exists."""
    frame = MetricsFrame()
    assert MetricsFrame.model_validate_json(frame.model_dump_json()) == frame


def test_populated_frame_round_trips_with_nested_records():
    frame = MetricsFrame(
        ts=time.time(),
        queue_depth={"P0": 1, "P1": 20, "P2": 900},
        latency_p99={"P0": 12.5, "P1": 300.0, "P2": 9000.0},
        pressure=1.42,
        recent_decisions=[DecisionTrace(seq=1, decision=Decision.MICRO_BATCH,
                                        reason="rung 1", tier=Tier.P2)],
        recent_sheds=[ShedRecord(seq=2, reason="below shed line", tier=Tier.P2)],
    )
    restored = MetricsFrame.model_validate_json(frame.model_dump_json())
    assert restored == frame
    assert restored.recent_decisions[0].decision is Decision.MICRO_BATCH
    assert restored.recent_sheds[0].tier is Tier.P2


def test_every_per_tier_field_is_keyed_by_all_three_tiers():
    frame = MetricsFrame()
    per_tier = [
        frame.queue_depth, frame.latency_p50, frame.latency_p95,
        frame.latency_p99, frame.ladder_rung, frame.sla_met, frame.sla_missed,
    ]
    for mapping in per_tier:
        assert tuple(mapping) == TIER_KEYS


def test_frame_carries_every_agreed_field():
    """The frozen field list. Adding to it is cheap; removing from it breaks
    Lane C's dashboard, so removal has to be a conscious act, not a rebase."""
    agreed = {
        "schema_version", "ts", "mode",
        "queue_depth",
        "latency_p50", "latency_p95", "latency_p99",
        "latency_p50_all", "latency_p95_all", "latency_p99_all",
        "throughput", "offered_rate", "admitted_rate", "service_rate",
        "pressure", "ladder_rung", "spike_multiplier",
        "worker_count", "active_workers",
        "ingested", "processed", "in_queue", "in_flight",
        "deferred_pending", "sampled_out", "shed",
        "weighted_click_count", "true_click_count",
        "cost_adaptive", "cost_naive", "value_delivered", "value_shed",
        "sla_met", "sla_missed",
        "retries", "duplicates_caught", "exactly_once_violations",
        "recent_decisions", "recent_sheds",
    }
    assert agreed <= set(MetricsFrame.model_fields), (
        "fields removed from the frozen frame: "
        f"{sorted(agreed - set(MetricsFrame.model_fields))}"
    )


def test_every_frame_field_has_a_default():
    """No required fields: a partial frame is always constructible."""
    missing = [
        name for name, f in MetricsFrame.model_fields.items() if f.is_required()
    ]
    assert missing == []


def test_frame_defaults_to_zero_or_empty():
    frame = MetricsFrame()
    assert frame.mode is Mode.ADAPTIVE
    assert frame.ingested == 0 and frame.shed == 0
    assert frame.recent_decisions == [] and frame.recent_sheds == []
    assert frame.exactly_once_violations == 0
    assert all(v == 0 for v in frame.queue_depth.values())
