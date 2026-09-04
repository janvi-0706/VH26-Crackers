"""Frozen data contracts: Event, Decision, MetricsFrame.

Owner: Lane D. FROZEN at the end of Stage A — changing anything here needs all
four of us to agree (see CLAUDE.md). If you need a field that isn't here, stop
and ask.

Two design rules this module exists to enforce:

1. Five identity fields, never one. Collapsing event_id / dedup_key / seq /
   partition_key / idempotency_key into a single id is exactly why most
   pipelines break dedup and retry against each other. See docs/DATA_MODEL.md.
2. MetricsFrame carries every field the dashboard will EVER need, all
   defaulted, from day one. Lane C builds against this schema before the
   engine exists; a field that appears later is a field the dashboard has to
   be rewritten for.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


# --------------------------------------------------------------------------
# Enumerations
# --------------------------------------------------------------------------


class Tier(str, Enum):
    """Priority class. P0 is the protected tier — never batched, deferred,
    sampled or shed. Under pressure we throttle the source instead."""

    P0 = "P0"
    P1 = "P1"
    P2 = "P2"


class EventType(str, Enum):
    PAYMENT = "payment"
    ORDER = "order"
    INVENTORY = "inventory"
    CLICK = "click"
    LOG = "log"


class Decision(str, Enum):
    """What the decision function chose to do with one event.

    STREAM_NOW    — individual processing, full fidelity, immediately
    MICRO_BATCH   — grouped with siblings to amortise per-event overhead
    DEFER         — parked in the deferred buffer, drained when pressure falls
    SAMPLE_ROLLUP — represented by a rollup row carrying a sample_weight
    SHED          — dropped, recorded in the ledger, never silent
    """

    STREAM_NOW = "STREAM_NOW"
    MICRO_BATCH = "MICRO_BATCH"
    DEFER = "DEFER"
    SAMPLE_ROLLUP = "SAMPLE_ROLLUP"
    SHED = "SHED"


class Mode(str, Enum):
    """Which policy the pipeline is running under. naive is the control arm
    for the benchmark: FIFO, no triage."""

    ADAPTIVE = "adaptive"
    NAIVE = "naive"


TIER_KEYS: tuple[str, ...] = (Tier.P0.value, Tier.P1.value, Tier.P2.value)


def per_tier_int() -> dict[str, int]:
    return {k: 0 for k in TIER_KEYS}


def per_tier_float() -> dict[str, float]:
    return {k: 0.0 for k in TIER_KEYS}


# --------------------------------------------------------------------------
# Event
# --------------------------------------------------------------------------


class Event(BaseModel):
    """One emission travelling through the pipeline.

    Identity fields and their lifecycle under retry:

      event_id         one emission            generator    NEW on retry
      dedup_key        business identity       generator    SAME on retry
      seq              pipeline order          classifier   NEW on retry
      partition_key    ordering domain (cust)  generator    SAME on retry
      idempotency_key  sink upsert target      classifier   SAME on retry
    """

    model_config = ConfigDict(extra="forbid")

    # --- identity (five separate fields, per CLAUDE.md) ---
    event_id: str
    dedup_key: str
    seq: int = 0  # 0 until the classifier stamps it
    partition_key: str
    idempotency_key: str = ""  # empty until the classifier derives it

    # --- classification ---
    type: EventType
    tier: Tier

    # --- economics: what it is worth, and what it costs to serve ---
    payload_size: int = 0  # bytes
    value: float = 0.0  # business value delivered if processed in time
    cost: float = 0.0  # work-units of simulated service time

    # --- time ---
    ingest_ts: float = 0.0  # wall clock, seconds, stamped at ingress
    deadline_ts: float = 0.0  # ingest_ts + the tier SLA

    schema_version: int = SCHEMA_VERSION


# --------------------------------------------------------------------------
# Decision traces — what the dashboard shows event by event
# --------------------------------------------------------------------------


class DecisionTrace(BaseModel):
    """One decision, made legible. reason is a short human string because a
    judge has to be able to read why an event was treated the way it was."""

    model_config = ConfigDict(extra="forbid")

    seq: int = 0
    event_id: str = ""
    type: EventType | None = None
    tier: Tier | None = None
    decision: Decision | None = None
    reason: str = ""
    pressure: float = 0.0
    value: float = 0.0
    ts: float = 0.0


class ShedRecord(BaseModel):
    """A shed event. Kept separate from DecisionTrace so the what-did-we-drop
    panel never has to filter a mixed stream."""

    model_config = ConfigDict(extra="forbid")

    seq: int = 0
    event_id: str = ""
    type: EventType | None = None
    tier: Tier | None = None
    reason: str = ""
    pressure: float = 0.0
    value: float = 0.0
    ts: float = 0.0


# --------------------------------------------------------------------------
# MetricsFrame — the ONLY thing the dashboard ever reads
# --------------------------------------------------------------------------


class MetricsFrame(BaseModel):
    """A complete picture of the pipeline at one instant, emitted at 4 Hz.

    Every field defaults, so a frame is always valid and the schema never has
    to change again. Fields not yet computed report their default rather than
    being absent.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    ts: float = 0.0
    mode: Mode = Mode.ADAPTIVE

    # --- queue ---
    queue_depth: dict[str, int] = Field(default_factory=per_tier_int)

    # --- latency, milliseconds, end to end (ingest -> complete) ---
    latency_p50: dict[str, float] = Field(default_factory=per_tier_float)
    latency_p95: dict[str, float] = Field(default_factory=per_tier_float)
    latency_p99: dict[str, float] = Field(default_factory=per_tier_float)

    # all tiers pooled — the headline number
    latency_p50_all: float = 0.0
    latency_p95_all: float = 0.0
    latency_p99_all: float = 0.0

    # --- rates, per second ---
    throughput: float = 0.0  # events completed
    offered_rate: float = 0.0  # events arriving at the door
    admitted_rate: float = 0.0  # events let past admission control
    service_rate: float = 0.0  # work-units actually served

    # --- control loop ---
    pressure: float = 0.0  # 0.0 calm .. 1.0+ saturated
    ladder_rung: dict[str, int] = Field(default_factory=per_tier_int)
    spike_multiplier: float = 1.0

    # --- workers ---
    worker_count: int = 0
    active_workers: int = 0

    # --- ledger counters (the conservation equation) ---
    #   ingested == processed + in_queue + in_flight
    #               + deferred_pending + sampled_out + shed
    ingested: int = 0
    processed: int = 0
    in_queue: int = 0
    in_flight: int = 0
    deferred_pending: int = 0
    sampled_out: int = 0
    shed: int = 0

    # --- sampling fidelity: what the rollups claim vs what really happened ---
    weighted_click_count: float = 0.0  # estimate = observed * sample_weight
    true_click_count: int = 0  # ground truth the simulator knows

    # --- benchmark: adaptive arm vs naive control arm ---
    cost_adaptive: float = 0.0
    cost_naive: float = 0.0
    value_delivered: float = 0.0
    value_shed: float = 0.0

    # --- SLA attainment ---
    sla_met: dict[str, int] = Field(default_factory=per_tier_int)
    sla_missed: dict[str, int] = Field(default_factory=per_tier_int)

    # --- correctness ---
    retries: int = 0
    duplicates_caught: int = 0
    exactly_once_violations: int = 0  # an invariant, not a statistic: stays 0

    # --- narrative: the last N of each, newest first ---
    recent_decisions: list[DecisionTrace] = Field(default_factory=list)
    recent_sheds: list[ShedRecord] = Field(default_factory=list)
