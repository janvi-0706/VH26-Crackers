"""server2: the standalone P1/P2 process — port 8002.

Owner: Lane A (Phase J5).

MUST be stateless and horizontally scalable: Kubernetes runs one to three of
these (`config/servers.yaml`'s own `server2.scaling: hpa`, `min_pods: 1`,
`max_pods: 3`) and can kill any of them without warning. Concretely, that
means:

  - No instance holds state another instance needs. Every piece of live
    control-loop state below (the queue, the pressure EWMAs, the CoDel
    controller, the reservoir samplers) is a plain instance attribute,
    constructed fresh per process, never shared or coordinated across pods.
    `docs/PHASE-J-INSPECTION.md` section 4 named `current_pressure()` as
    having "no single owner post-split" — this file's answer for server2 is
    "each instance owns its own, computed from its own local queue state,
    and ingress never averages them" (this phase's own instruction,
    verbatim; see `reporting.FragmentStore.aggregate()` for the SUM it does
    take across instances, which is a different, counter-level operation).
  - A DEFER decision is POSTed to ingress's `/defer` (built ahead of this
    file, in Phase J3/J4's own app.py, specifically for this phase) rather
    than buffered locally — this process holds no `deferral.DeferralStore`
    of its own. The corollary, named rather than silently accepted: once
    ingress's own drainer eventually re-admits a deferred event, it comes
    back through some future dispatch to server2 as an ordinary new
    `/ingest` arrival, indistinguishable from a first attempt — server2 has
    no memory of "this was already deferred once" the way the monolith's
    `worker.py._resolve()` does via `deferral.was_deferred()`. A P1 event
    whose slack has already gone negative by the time it is redispatched
    will therefore DEFER again under `decide()`'s own unchanged rule,
    potentially forever. `docs/PHASE-J-INSPECTION.md` already flagged
    "deferral-forwarding's actual shape" as unresolved; closing the redefer
    trap in a stateless topology needs ingress itself to mark a
    already-deferred-once event before redispatch (or serve it directly
    past the slack check) — real, separate scope this phase's own prompt
    (build server2.py) does not ask for, not solved here.
  - A finished reservoir window is POSTed to ingress's `/rollup` for durable
    persistence (`sink.write_rollup`, ingress-owned per
    `docs/PHASE-J-INSPECTION.md` section 3) — this process keeps only the
    OPEN, in-progress window per type in memory, which is legitimate
    per-instance state (`ladder.py`'s own docstring: a window is this
    instance's own accounting of events it personally sampled; it is not
    shared or replayable). A pod killed mid-window loses that partial
    count — the same bounded, honest undercount `ladder.RESERVOIR_N`'s own
    comment already accepts for a mid-window CoDel exit, just now also
    triggerable by a pod death, not only by sampling ending.
  - A completed event (STREAM_NOW or MICRO_BATCH) is POSTed to ingress's
    existing `/ack` (Phase J3's own mechanism, unchanged) — exactly
    server1.py's own precedent. Nothing here ever opens a file or writes to
    `sink.py` directly; `docs/PHASE-J-INSPECTION.md` section 3 already
    named `events_sink` as staying in ingress's one SQLite file.

Explicitly, and on purpose, does NOT contain: EDF-only ordering (that is
P0's own isolated simplicity, server1.py's job) or worker-death checkpoint
recovery (`checkpoint.py`) — the same acknowledged gap server1.py already
carries: a pod killed mid-`serve()` loses whatever it was mid-serving,
narrowed (not closed) by K6's own graceful `/drain`, not solved here. Nor
does it write a decision-trace to `ledger.py` — that table is ALSO
ingress-owned per the same inspection, and no forwarding endpoint for a
decision trace exists yet (unlike `/defer` and `/rollup`, which this
phase's own prompt names); auditing decisions made in the split topology is
real, separate, un-asked-for scope, named here rather than silently
dropped.

Ordering and routing, unlike server1: this tier pair genuinely needs
`decision.py`'s full split score/pressure/route machinery (P0's isolation
is exactly what let server1 skip it). `P1P2Queue` below mirrors
`queue.py`'s own settled/pending score-cached design (same reasoning: a
fresh `decision.score()` scan on every dequeue was measured too expensive
at spike scale) restricted to two tiers, with the same P2-aging-guard
exception — but does NOT import `metrics.py`: that module's ambient,
module-level state is the monolith/ingress process's own dashboard
registry, not a thing a standalone P1/P2 process should be touching or
mutating. Pressure, CoDel, and the reservoir samplers are therefore each
reimplemented here as plain per-instance state, fed from this queue alone,
per this phase's own instruction that pressure must never be averaged or
shared across instances.

Capacity: `config/servers.yaml`'s own `server2.capacity_us_per_pod` (15
u/s), never a hardcoded worker count —
`servers_config.ServerSpec.workers()` derives it: `derive_workers(15, 25)`
= 1 worker at 15 u/s per pod (see that module's own docstring). Twelve
times oversubscribed against the ~180 u/s of P1/P2 spike demand at one pod,
four times at three (`config/servers.yaml`'s own `max_pods` comment has the
full arithmetic) — intentional: the ladder is meant to be fully exercised,
P2 reaching SAMPLE_ROLLUP and P1 reaching DEFER and no further.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from . import codel, decision, ladder, reporting
from .config import load_config
from .contracts import Decision, Event, EventType, Tier
from .metrics import percentile
from .servers_config import ServerSpec, load_servers_config

logger = logging.getLogger(__name__)

# Mirrors server1.py's own INGRESS_HEALTH_CHECK_INTERVAL_SECONDS /
# DRAIN_POLL_INTERVAL_SECONDS constants — same reasoning, same numbers.
INGRESS_HEALTH_CHECK_INTERVAL_SECONDS = 1.0
DRAIN_POLL_INTERVAL_SECONDS = 0.05

# Bounded sample windows — same bound and reasoning as metrics.py's own
# WINDOW / server1.py's own LATENCY_WINDOW: enough to mean something at
# spike rate, bounded so a long run cannot grow them without limit.
LATENCY_WINDOW = 4096
QUEUE_WAIT_WINDOW = 4096

# --------------------------------------------------------------------------
# P1P2Queue — queue.py's own settled/pending score-cached design (see that
# module's own docstring for the full O(n)-per-dequeue-vs-resort-caching
# argument), restricted to the two tiers this process ever holds, and with
# no metrics.py side effects: pure scheduling, nothing else.
# --------------------------------------------------------------------------

_TIER_PRIORITY: tuple[Tier, ...] = (Tier.P1, Tier.P2)

# Same numbers as queue.py's own DEFAULT_P2_AGING_GUARD_SECONDS /
# RESORT_INTERVAL_SECONDS — this is the same starvation bound and the same
# resort-cost tradeoff, on a smaller queue.
DEFAULT_P2_AGING_GUARD_SECONDS = 2.0
RESORT_INTERVAL_SECONDS = 0.05


class P1P2Queue:
    """Two tiers, each a settled/pending pair scored live at dequeue time —
    P0 never reaches this class at all (rejected at `/ingest`, before an
    event is ever queued), so there is no absolute-priority branch to carry
    here the way `queue.py`'s own three-tier version needs one.

    Selection policy (unchanged from `queue.py`'s own P1-vs-P2 half):
    the aging guard first (the chronologically oldest P2 item jumps the
    queue once its sojourn crosses `aging_guard_seconds`, picked by
    `ingest_ts` not by score — "unstick whoever has waited longest" is a
    different question from "who is most valuable right now"), then P1,
    then P2, each scored by `decision.score()`.
    """

    def __init__(
        self,
        *,
        capacity_units_per_sec: float,
        aging_guard_seconds: float = DEFAULT_P2_AGING_GUARD_SECONDS,
    ) -> None:
        self.capacity_units_per_sec = capacity_units_per_sec
        self.aging_guard_seconds = aging_guard_seconds
        self._settled: dict[Tier, list[Event]] = {t: [] for t in _TIER_PRIORITY}
        self._pending: dict[Tier, list[Event]] = {t: [] for t in _TIER_PRIORITY}
        self._resort_ts: dict[Tier, float] = {t: 0.0 for t in _TIER_PRIORITY}
        self._nonempty = asyncio.Event()

    def put(self, event: Event) -> None:
        self._pending[event.tier].append(event)
        self._nonempty.set()

    async def get(self) -> Event:
        while True:
            event = self.try_get()
            if event is not None:
                return event
            self._nonempty.clear()
            await self._nonempty.wait()

    def try_get(self) -> Event | None:
        """Non-blocking: the current best event, or None immediately if
        nothing is takeable — used both for the initial dequeue and for
        greedily gathering a MICRO_BATCH (a batch worth waiting to fill
        would add latency exactly where batching is supposed to save it)."""
        p2_all = self._tier_events(Tier.P2)
        if p2_all:
            oldest = min(p2_all, key=lambda e: e.ingest_ts)
            if time.time() - oldest.ingest_ts >= self.aging_guard_seconds:
                self._remove(Tier.P2, oldest)
                return oldest
        if self._settled[Tier.P1] or self._pending[Tier.P1]:
            return self._pop_best_by_score(Tier.P1)
        if p2_all:
            return self._pop_best_by_score(Tier.P2)
        return None

    def _tier_events(self, tier: Tier) -> list[Event]:
        return self._settled[tier] + self._pending[tier]

    def _remove(self, tier: Tier, event: Event) -> None:
        try:
            self._pending[tier].remove(event)
        except ValueError:
            self._settled[tier].remove(event)

    def _score(self, event: Event, now: float, weights) -> float:
        return decision.score(event, now, self.capacity_units_per_sec, weights)

    def _maybe_resort(self, tier: Tier, now: float) -> None:
        if not self._pending[tier] and self._settled[tier]:
            return
        if now - self._resort_ts[tier] < RESORT_INTERVAL_SECONDS and self._settled[tier]:
            return
        weights = decision.current_score_weights
        merged = self._settled[tier] + self._pending[tier]
        merged.sort(key=lambda e: (self._score(e, now, weights), -e.seq))
        self._settled[tier] = merged
        self._pending[tier] = []
        self._resort_ts[tier] = now

    def _pop_best_by_score(self, tier: Tier) -> Event:
        now = time.time()
        self._maybe_resort(tier, now)
        weights = decision.current_score_weights

        settled = self._settled[tier]
        pending = self._pending[tier]

        best_pending: Event | None = None
        best_pending_score = -1.0
        for candidate in pending:
            candidate_score = self._score(candidate, now, weights)
            if best_pending is None or candidate_score > best_pending_score or (
                candidate_score == best_pending_score and candidate.seq < best_pending.seq
            ):
                best_pending, best_pending_score = candidate, candidate_score

        if best_pending is None:
            return settled.pop()
        if not settled:
            pending.remove(best_pending)
            return best_pending

        settled_score = self._score(settled[-1], now, weights)
        if best_pending_score > settled_score or (
            best_pending_score == settled_score and best_pending.seq < settled[-1].seq
        ):
            pending.remove(best_pending)
            return best_pending
        return settled.pop()

    def __len__(self) -> int:
        return sum(len(self._settled[t]) + len(self._pending[t]) for t in _TIER_PRIORITY)

    def tier_depth(self, tier: Tier) -> int:
        return len(self._settled[tier]) + len(self._pending[tier])


# --------------------------------------------------------------------------
# Local pressure — decision.pressure()'s own inputs, fed by THIS instance's
# own queue/EWMAs alone. Duplicated from metrics.py's own _Ewma, not
# imported: that class is private ambient state belonging to the
# monolith/ingress process (one registry, one process, per that module's
# own docstring); this process needs its own independent copy per instance,
# not a shared one — exactly this phase's own "instances do not coordinate"
# instruction, applied to the rate signal itself, not only to the pressure
# number it feeds.
# --------------------------------------------------------------------------

QDEPTH_SATURATION = 500.0  # same number and reasoning as metrics.py's own
# constant — a property of how we observe saturation, not of worker count.
_RATE_EWMA_HALF_LIFE_SECONDS = 2.0
_PRESSURE_REFRESH_SECONDS = 0.05


class _Ewma:
    """Same formula as metrics.py's own `_Ewma` — see that class's own
    docstring for the full reasoning (amount-at-a-timestamp, the dt<=0
    carry-forward case for same-instant batch completions)."""

    def __init__(self, half_life_seconds: float) -> None:
        self.half_life = half_life_seconds
        self.level = 0.0
        self.trend = 0.0
        self._last_update: float | None = None
        self._pending_amount = 0.0

    def observe_amount(self, amount: float, now: float) -> None:
        if self._last_update is None:
            self._last_update = now
            self._pending_amount += amount
            return
        dt = now - self._last_update
        if dt <= 0.0:
            self._pending_amount += amount
            return
        raw_rate = (amount + self._pending_amount) / dt
        self._pending_amount = 0.0
        alpha = 1.0 - 0.5 ** (dt / self.half_life)
        new_level = alpha * raw_rate + (1.0 - alpha) * self.level
        self.trend = new_level - self.level
        self.level = new_level
        self._last_update = now

    @property
    def with_trend(self) -> float:
        return max(self.level + self.trend, 0.0)


@dataclass
class _ServerState:
    """Everything one running server2 process needs — grouped so the
    FastAPI handlers below read/write one object instead of a scatter of
    `app.state.*` attributes. Every field here is per-instance, plain,
    unshared state — no module-level ambient singleton anywhere in this
    file, per this phase's own statelessness requirement."""

    queue: P1P2Queue
    worker_count: int = 0
    per_worker_rate: float = 0.0
    sla_reference: float = 5.0
    processed_count: int = 0
    batched_count: int = 0
    in_flight: int = 0
    draining: bool = False
    ingress_ready: bool = False
    latency_ms: list[float] = field(default_factory=list)
    queue_wait_ms: list[float] = field(default_factory=list)
    arrival_ewma: _Ewma = field(default_factory=lambda: _Ewma(_RATE_EWMA_HALF_LIFE_SECONDS))
    service_ewma: _Ewma = field(default_factory=lambda: _Ewma(_RATE_EWMA_HALF_LIFE_SECONDS))
    codel: codel.CoDelController = field(default_factory=codel.CoDelController)
    reservoirs: dict[EventType, ladder.ReservoirSampler] = field(
        default_factory=lambda: {
            EventType.CLICK: ladder.ReservoirSampler(),
            EventType.LOG: ladder.ReservoirSampler(),
        }
    )
    deferred_count: int = 0
    sampled_count: int = 0
    shed_count: int = 0
    rollups_persisted_count: int = 0
    true_click_count: int = 0
    weighted_click_count: float = 0.0
    ladder_rung: dict[str, int] = field(
        default_factory=lambda: {Tier.P1.value: 0, Tier.P2.value: 0}
    )
    pressure_cache: float = 0.0
    pressure_cache_ts: float = 0.0


def _compute_pressure(state: _ServerState) -> float:
    """decision.pressure(), fed entirely from this instance's own local
    signals — no cross-instance input anywhere, per this phase's own
    instruction that ingress reports every instance's pressure separately
    and never averages them."""
    worker_util = min(state.in_flight / max(state.worker_count, 1), 1.0)
    signals = decision.PressureSignals(
        qdepth=float(len(state.queue)),
        qmax=QDEPTH_SATURATION,
        arrival_rate_ewma_with_trend=state.arrival_ewma.with_trend,
        service_rate=state.service_ewma.with_trend,
        p95_sojourn=percentile(state.queue_wait_ms, 0.95) / 1000.0,
        sla_reference=state.sla_reference,
        worker_util=worker_util,
    )
    return decision.pressure(signals)


def _pressure_value(state: _ServerState, now: float) -> float:
    """Cached — see metrics.py's own _PRESSURE_REFRESH_SECONDS for why:
    this calls percentile() over up to QUEUE_WAIT_WINDOW samples, too
    expensive to pay on every single dequeue at spike rate."""
    if now - state.pressure_cache_ts >= _PRESSURE_REFRESH_SECONDS:
        state.pressure_cache = _compute_pressure(state)
        state.pressure_cache_ts = now
    return state.pressure_cache


def _assert_server2_is_correctly_provisioned(spec: ServerSpec) -> None:
    """The startup half of "assert ladder caps hold and no P0 event can be
    routed here" — independent of `servers_config.py`'s own structural
    validation (which already refuses to even LOAD a `servers.yaml` with
    these wrong), matching server1.py's own "enforced twice, not once"
    precedent for exactly the same reason."""
    if set(spec.tiers) != {Tier.P1, Tier.P2}:
        raise RuntimeError(
            f"server2 startup refused: this process may only ever serve "
            f"P1 and P2 (never P0) — config/servers.yaml declares "
            f"server2.tiers={spec.tiers!r}"
        )
    if not spec.batching:
        raise RuntimeError(
            "server2 startup refused: batching must be enabled for P1/P2 "
            "— MICRO_BATCH is real amortised savings here (decision.py's "
            "own batch_cost worked numbers), unlike P0; "
            "config/servers.yaml's own server2.batching is not true"
        )
    if spec.scaling != "hpa":
        raise RuntimeError(
            "server2 startup refused: scaling must be 'hpa' — server2 is "
            "the one process this project's capacity story lets "
            f"Kubernetes scale; got scaling={spec.scaling!r}"
        )


def create_server2_app(
    spec: ServerSpec | None = None,
    *,
    ingress_url: str,
    ack_client: httpx.AsyncClient | None = None,
    report_client: httpx.AsyncClient | None = None,
    reference_worker_rate_ups: float | None = None,
    push_interval_ms: float | None = None,
    health_check_interval_seconds: float | None = None,
    aging_guard_seconds: float = DEFAULT_P2_AGING_GUARD_SECONDS,
) -> FastAPI:
    spec = spec or load_servers_config().server2
    _assert_server2_is_correctly_provisioned(spec)

    resolved_ack_client = ack_client or httpx.AsyncClient(timeout=5.0)
    owns_ack_client = ack_client is None
    base_ingress_url = ingress_url.rstrip("/")

    cfg = load_config()
    reference_rate = reference_worker_rate_ups or cfg.worker_capacity_ups
    worker_count, per_worker_rate = spec.workers(reference_worker_rate_ups=reference_rate)
    # P1's own SLA is the natural pressure reference (the tighter of the
    # two gated tiers) — same reasoning as metrics.py's own
    # _compute_pressure, applied locally.
    sla_reference = min(s.sla_seconds for s in cfg.tiers_of(Tier.P1))

    state = _ServerState(
        queue=P1P2Queue(
            capacity_units_per_sec=per_worker_rate,
            aging_guard_seconds=aging_guard_seconds,
        ),
        worker_count=worker_count,
        per_worker_rate=per_worker_rate,
        sla_reference=sla_reference,
    )

    def _record_latency(latency_ms: float) -> None:
        state.latency_ms.append(latency_ms)
        if len(state.latency_ms) > LATENCY_WINDOW:
            del state.latency_ms[: len(state.latency_ms) - LATENCY_WINDOW]

    def _note_dequeue(event: Event, now: float) -> None:
        """The one call site that plays `metrics.observe_dequeue()`'s own
        role here: records this event's queue-wait for the pressure
        signal and, for P2 only, feeds it to this instance's own CoDel
        controller — the exact consumer codel.py's own docstring names,
        just fed locally instead of through the ambient default."""
        wait_ms = max(0.0, (now - event.ingest_ts) * 1000.0)
        state.queue_wait_ms.append(wait_ms)
        if len(state.queue_wait_ms) > QUEUE_WAIT_WINDOW:
            del state.queue_wait_ms[: len(state.queue_wait_ms) - QUEUE_WAIT_WINDOW]
        if event.tier is Tier.P2:
            state.codel.update(wait_ms / 1000.0, now)
        state.in_flight += 1

    async def _ack_many(event_ids: list[str]) -> None:
        try:
            response = await resolved_ack_client.post(
                f"{base_ingress_url}/ack", json={"event_ids": event_ids},
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001 - a lost ack is what ingress's own
            # redispatch sweep (transport.py) exists to recover from.
            logger.debug("ack post failed for %s", event_ids, exc_info=True)

    async def _post_defer(event: Event, reason: str) -> None:
        """See this module's own top docstring for the accepted risk: a
        failed POST here has no local buffer to fall back to (that is the
        whole point of statelessness) — the event is genuinely lost if
        ingress cannot be reached at the moment this fires, exactly the
        same class of gap server1.py's `_ack` already accepts for a lost
        ack, just with no redispatch sweep on this specific wire to
        recover it. Named, not silently risked."""
        try:
            response = await resolved_ack_client.post(
                f"{base_ingress_url}/defer",
                json={"event": event.model_dump(mode="json"), "reason": reason},
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001 - see docstring above
            logger.debug("defer post failed for %s", event.event_id, exc_info=True)

    async def _post_rollup(rollup: "ladder.Rollup") -> None:
        try:
            response = await resolved_ack_client.post(
                f"{base_ingress_url}/rollup", json=dataclasses.asdict(rollup),
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001 - see _post_defer's own docstring
            logger.debug("rollup post failed for %s window", rollup.event_type, exc_info=True)

    def _resolve(event: Event, pressure_value: float, now: float) -> tuple[Decision, str]:
        """decision.decide() plus the P2-only ladder escalation, plus the
        one ladder.cap() pass every result — escalated or not — is run
        through: the second, independent enforcement layer this phase's
        own instruction ("assert ladder caps hold") asks for, matching
        ladder.escalate()'s own defensive final cap() call.

        Deliberately does NOT implement worker.py's own redefer trap
        (`deferral.was_deferred()`) — see this module's own top docstring
        for why a stateless server2 structurally cannot know that here."""
        assert event.tier is not Tier.P0, "unreachable: /ingest already rejects P0"

        result, reason = decision.decide(event, pressure_value, now, state.per_worker_rate)
        if event.tier is Tier.P2:
            escalated, escalated_reason = ladder.escalate(
                event.tier, result, pressure_value, state.codel.sampling
            )
            if escalated_reason is not None:
                result, reason = escalated, escalated_reason

        capped_rung = ladder.cap(event.tier, ladder.DECISION_RUNG[result])
        return ladder.RUNG_DECISION[capped_rung], reason

    async def _serve(event: Event) -> None:
        await asyncio.sleep(event.cost / state.per_worker_rate)
        now = time.time()
        state.in_flight -= 1
        state.processed_count += 1
        _record_latency(max(0.0, (now - event.ingest_ts) * 1000.0))
        state.service_ewma.observe_amount(event.cost, now)
        if event.type is EventType.CLICK:
            state.weighted_click_count += 1.0
        await _ack_many([event.event_id])

    async def _serve_batch(batch: list[Event]) -> None:
        """One combined sleep for the whole batch — decision.batch_cost()
        is what makes this genuinely cheaper, not merely labelled
        differently. Matches worker.py's own _serve_batch reasoning."""
        total_cost = decision.batch_cost([e.cost for e in batch])
        await asyncio.sleep(total_cost / state.per_worker_rate)
        now = time.time()
        for e in batch:
            state.in_flight -= 1
            state.processed_count += 1
            _record_latency(max(0.0, (now - e.ingest_ts) * 1000.0))
            state.service_ewma.observe_amount(e.cost, now)
            if e.type is EventType.CLICK:
                state.weighted_click_count += 1.0
        state.batched_count += len(batch)
        await _ack_many([e.event_id for e in batch])

    async def _dispatch_off_path(
        event: Event, result: Decision, reason: str, now: float
    ) -> None:
        state.in_flight -= 1
        state.ladder_rung[event.tier.value] = int(ladder.DECISION_RUNG[result])

        if result is Decision.DEFER:
            state.deferred_count += 1
            await _post_defer(event, reason)
        elif result is Decision.SAMPLE_ROLLUP:
            state.sampled_count += 1
            rollup = state.reservoirs[event.type].add(event, now)
            if rollup is not None:
                state.rollups_persisted_count += 1
                if event.type is EventType.CLICK:
                    state.weighted_click_count += rollup.observed_count * rollup.sample_weight
                await _post_rollup(rollup)
        elif result is Decision.SHED:
            state.shed_count += 1

    _OFF_PATH: frozenset[Decision] = frozenset(
        {Decision.DEFER, Decision.SAMPLE_ROLLUP, Decision.SHED}
    )

    async def _handle(event: Event) -> None:
        now = time.time()
        pressure_value = _pressure_value(state, now)
        result, reason = _resolve(event, pressure_value, now)

        if result is Decision.STREAM_NOW:
            await _serve(event)
            return
        if result in _OFF_PATH:
            await _dispatch_off_path(event, result, reason, now)
            return

        # MICRO_BATCH: gather more, best-effort, non-blocking — mirrors
        # worker.py's own _handle exactly, minus checkpoint/sink coupling.
        batch: list[Event] = [event]
        target_size = decision.batch_size(pressure_value)
        while len(batch) < target_size:
            extra = state.queue.try_get()
            if extra is None:
                break
            extra_now = time.time()
            _note_dequeue(extra, extra_now)
            extra_result, extra_reason = _resolve(extra, pressure_value, extra_now)
            if extra_result is Decision.MICRO_BATCH:
                batch.append(extra)
                continue
            if extra_result is Decision.STREAM_NOW:
                await _serve(extra)
            else:
                await _dispatch_off_path(extra, extra_result, extra_reason, extra_now)
        await _serve_batch(batch)

    async def _worker(worker_id: int) -> None:
        while True:
            event = await state.queue.get()
            _note_dequeue(event, time.time())
            try:
                await _handle(event)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one bad event must not kill the worker
                logger.exception("worker-%d failed on %s", worker_id, event.event_id)

    async def _check_ingress_once() -> bool:
        try:
            response = await resolved_ack_client.get(f"{base_ingress_url}/health")
            return response.status_code == 200
        except Exception:  # noqa: BLE001 - any failure means "not confirmed"
            return False

    async def _ingress_health_loop() -> None:
        interval = health_check_interval_seconds or INGRESS_HEALTH_CHECK_INTERVAL_SECONDS
        while True:
            state.ingress_ready = await _check_ingress_once()
            await asyncio.sleep(interval)

    def _collect_metrics() -> dict[str, float]:
        return {
            "processed": float(state.processed_count),
            "in_queue": float(len(state.queue)),
            "in_flight": float(state.in_flight),
            "sampled_out": float(state.sampled_count),
            "shed": float(state.shed_count),
        }

    reporting_client = reporting.ReportingClient(
        server="server2",
        ingress_url=ingress_url,
        collect=_collect_metrics,
        push_interval_ms=push_interval_ms,
        client=report_client,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        worker_tasks = [
            asyncio.create_task(_worker(i), name=f"pulse-server2-worker-{i}")
            for i in range(max(1, state.worker_count))
        ]
        health_check_task = asyncio.create_task(
            _ingress_health_loop(), name="pulse-server2-ingress-healthcheck"
        )
        reporting_client.start()
        try:
            yield
        finally:
            for task in worker_tasks:
                task.cancel()
            if worker_tasks:
                await asyncio.gather(*worker_tasks, return_exceptions=True)
            health_check_task.cancel()
            await asyncio.gather(health_check_task, return_exceptions=True)
            await reporting_client.stop()
            if owns_ack_client:
                await resolved_ack_client.aclose()

    app = FastAPI(title="PULSE-server2", lifespan=lifespan)
    app.state.pulse = state

    @app.post("/ingest")
    async def ingest(body: dict) -> JSONResponse:
        if state.draining:
            return JSONResponse({"accepted": 0, "rejected": "draining"}, status_code=503)
        raw_events = body.get("events", [])
        events = [Event.model_validate(e) for e in raw_events]
        p0 = [e for e in events if e.tier is Tier.P0]
        if p0:
            # Second, independent enforcement of "no P0 event can be
            # routed here" — see _assert_server2_is_correctly_provisioned's
            # own docstring for the startup half of the same assertion.
            return JSONResponse(
                {"error": "server2 never serves P0; received P0 event(s)"},
                status_code=422,
            )
        now = time.time()
        for event in events:
            state.arrival_ewma.observe_amount(event.cost, now)
            if event.type is EventType.CLICK:
                state.true_click_count += 1
            state.queue.put(event)
        return JSONResponse({"accepted": len(events)})

    @app.post("/drain")
    async def drain(timeout_s: float = 30.0) -> dict:
        """Matches server1.py's own /drain exactly — this endpoint's own
        mechanism, not the policy of when to call it (K6's own scope)."""
        state.draining = True
        deadline = time.time() + timeout_s
        while (len(state.queue) > 0 or state.in_flight > 0) and time.time() < deadline:
            await asyncio.sleep(DRAIN_POLL_INTERVAL_SECONDS)
        drained = len(state.queue) == 0 and state.in_flight == 0
        return {
            "status": "drained" if drained else "timeout",
            "queue_depth": len(state.queue),
            "in_flight": state.in_flight,
        }

    @app.get("/metrics")
    async def get_metrics() -> dict:
        latencies = state.latency_ms
        return {
            "server": "server2",
            "worker_count": state.worker_count,
            "per_worker_rate_ups": state.per_worker_rate,
            "processed": state.processed_count,
            "batched": state.batched_count,
            "in_queue": len(state.queue),
            "queue_depth": {
                Tier.P1.value: state.queue.tier_depth(Tier.P1),
                Tier.P2.value: state.queue.tier_depth(Tier.P2),
            },
            "in_flight": state.in_flight,
            "draining": state.draining,
            "pressure": round(_pressure_value(state, time.time()), 4),
            "ladder_rung": dict(state.ladder_rung),
            "deferred": state.deferred_count,
            "sampled_out": state.sampled_count,
            "shed": state.shed_count,
            "rollups_persisted": state.rollups_persisted_count,
            "true_click_count": state.true_click_count,
            "weighted_click_count": round(state.weighted_click_count, 3),
            "latency_ms": {
                "p50": round(percentile(latencies, 0.50), 3),
                "p95": round(percentile(latencies, 0.95), 3),
                "p99": round(percentile(latencies, 0.99), 3),
            },
        }

    @app.get("/healthz")
    async def healthz() -> dict:
        """Liveness, unconditional — see server1.py's own /healthz
        docstring for why this must never depend on ingress."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> object:
        """Not ready until ingress is confirmed reachable, re-verified
        continuously — matches server1.py's own /readyz exactly."""
        if state.ingress_ready:
            return {"status": "ready"}
        return JSONResponse({"status": "not-ready"}, status_code=503)

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run PULSE server2 (P1/P2 only).")
    parser.add_argument("--ingress", default=None, help="ingress base URL, e.g. http://127.0.0.1:8000")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args(argv)

    cfg = load_servers_config()
    spec = cfg.server2
    ingress_url = args.ingress or f"http://127.0.0.1:{cfg.ingress.port}"
    port = args.port or spec.port

    import uvicorn

    uvicorn.run(create_server2_app(spec, ingress_url=ingress_url), host=args.host, port=port)


if __name__ == "__main__":
    main()
