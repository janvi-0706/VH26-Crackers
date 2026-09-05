"""Dispatch tracking between ingress and the two downstream servers.

Owner: Lane A (Phase J2 interface, Phase J3 real HTTP).

The four functions below (`dispatch`, `ack`, `outstanding`,
`redispatch_expired`) are unchanged, in signature and contract, from Phase
J2 — this phase's own instruction is "same interfaces from J2." What
changes is what actually moves an event: Phase J2's `deliver` was a
same-process function call; Phase J3 adds `HttpDeliverer`, a real HTTP
client, as one more implementation of that exact same `DeliverFn` seam.
Nothing in `Transport.dispatch/ack/outstanding/redispatch_expired` or
`tests/test_transport.py` changed to make that swap possible — that was
the entire point of injecting `deliver` in the first place.

New in this phase, layered on top of the unchanged core:

  HttpDeliverer          one pooled `httpx.AsyncClient` (per this phase's
                         own instruction: "one pooled client, not a
                         connection per batch") POSTing a batch to
                         `{base_url}/ingest`.
  Batcher                accumulates individual `submit()`ed events per
                         server and flushes them as one `dispatch()` call
                         at `batch_size` events or `batch_window_ms`,
                         whichever comes first — config/servers.yaml's own
                         `transport.batch_size`/`batch_window_ms`.
  a background sweep     periodically calls `redispatch_expired()` so a
                         downstream server dying with events already
                         dispatched to it (docs/PHASE-J-INSPECTION.md
                         section 5's own scenario) is noticed and retried
                         without ingress having to poll for it manually.
  transport latency      `ack()` already knows both the dispatch timestamp
                         and the ack timestamp for every event; this phase
                         records that gap and exposes its own p50/p95/p99
                         — separately from queue-wait, which is a
                         different number metrics.py already owns —
                         because a payment's SLA budget (200ms) leaves
                         only ~60ms for the network hop itself once queue
                         wait and simulated service time are subtracted,
                         and that number needs to be visible on its own,
                         not folded into either of the others.

Idempotency (docs/DATA_MODEL.md's own identity model, unchanged) is what
makes automatic redispatch safe rather than a double-charge — a
redispatched event is, at every field but nothing at all (this module
never mints a new `event_id`; it resends the exact same `Event`), what
`sink.py`'s own upsert-by-`idempotency_key` already treats as an idempotent
retry. At-least-once delivery (this module's own guarantee: redispatch
whatever isn't acked in time) plus an idempotent sink write is what
together give exactly-once EFFECTS without needing exactly-once DELIVERY,
which is the actual, standard way this problem is solved rather than an
invented shortcut.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import httpx

from .contracts import Event
from .metrics import percentile
from .servers_config import ServersConfig, load_servers_config

logger = logging.getLogger(__name__)

DeliverFn = Callable[[str, list[Event]], Awaitable[None]]

# How many transport-latency samples to keep — same bound and same
# reasoning as metrics.py's own WINDOW: enough to mean something at spike
# rate, bounded so a long-running demo cannot grow this without limit.
LATENCY_WINDOW = 4096

# How often the background sweep checks for expired dispatches. Separate
# from ack_timeout_ms itself (a much longer number, seconds): checking
# every 50ms means a timeout is noticed within, at worst, one sweep
# interval of it actually elapsing, without the sweep loop itself becoming
# a meaningful source of load.
REDISPATCH_SWEEP_INTERVAL_SECONDS = 0.05


@dataclass(frozen=True)
class DispatchResult:
    """What `dispatch()` hands back — enough for the caller to later
    `ack()` exactly this batch, or to simply let it time out."""

    dispatch_id: str
    server: str
    event_ids: tuple[str, ...]
    dispatched_ts: float


@dataclass
class _DispatchRecord:
    server: str
    dispatched_ts: float
    # event_id -> Event, mutated as partial acks arrive. A dict, not a
    # list: ack() removes specific event_ids from a batch that may only be
    # PARTIALLY acknowledged (a real transport can plausibly ack the 18 of
    # 20 events in a batch that a downstream server actually finished
    # before the 19th and 20th's own acks are still in flight), and a dict
    # makes that an O(1) removal per id rather than a linear scan.
    events: dict[str, Event] = field(default_factory=dict)


class Transport:
    """One instance tracks dispatch state for every downstream server it
    is configured to reach. Constructor-injected `deliver`, matching
    `WorkerPool`'s own `sink_write`/`defer` precedent — see this module's
    docstring for why."""

    def __init__(
        self,
        deliver: DeliverFn,
        *,
        ack_timeout_ms: float | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._deliver = deliver
        self._ack_timeout_ms = (
            ack_timeout_ms
            if ack_timeout_ms is not None
            else load_servers_config().transport.ack_timeout_ms
        )
        self._now = now
        self._outstanding: dict[str, _DispatchRecord] = {}
        # event_id -> the dispatch_id CURRENTLY responsible for it. Lets a
        # server ack by event_id alone (the only thing it genuinely knows
        # about — a real downstream process has no reason to track
        # ingress's own internal dispatch_id) without ingress needing a
        # linear scan of every outstanding record to find it.
        self._event_index: dict[str, str] = {}
        self._next_id = 0
        # Dispatch -> ack latency, per acked event, milliseconds. This is
        # the "transport latency" this phase's own prompt asks to expose
        # separately from queue wait (metrics.py's own, different number).
        self._latency_ms: list[float] = []
        # Phase J6: lifetime event_id SETS, not counts — what the
        # cross-process conservation check needs. A raw dispatched-events
        # counter would double-count a redispatch (the SAME event_id
        # dispatched again after `ack_timeout_ms`, still tracked under a
        # brand-new dispatch_id — see `redispatch_expired()`'s own
        # docstring), which would break the identity `dispatched ==
        # resolved + outstanding` the moment even one redispatch happened.
        # Sets are naturally idempotent under a repeat dispatch of the same
        # event_id, so the identity holds regardless of how many times
        # anything was retried.
        self._all_dispatched_event_ids: set[str] = set()
        self._all_resolved_event_ids: set[str] = set()

    def _fresh_dispatch_id(self) -> str:
        self._next_id += 1
        return f"dispatch-{self._next_id}"

    async def dispatch(self, server: str, events: list[Event]) -> DispatchResult:
        """Hand `events` to `server` via the injected `deliver`, and
        record the attempt with a timestamp before returning. Recording
        happens regardless of whether `deliver` itself has actually
        finished sending anything anywhere by the time this returns
        (a real HTTP client may internally queue/pipeline) — what matters
        for `redispatch_expired()` is "when did ingress consider this
        attempted", not "when did the network call return"."""
        if not events:
            raise ValueError("dispatch() requires at least one event")
        dispatch_id = self._fresh_dispatch_id()
        now = self._now()
        self._outstanding[dispatch_id] = _DispatchRecord(
            server=server,
            dispatched_ts=now,
            events={e.event_id: e for e in events},
        )
        for e in events:
            self._event_index[e.event_id] = dispatch_id
        self._all_dispatched_event_ids.update(e.event_id for e in events)
        await self._deliver(server, events)
        return DispatchResult(
            dispatch_id=dispatch_id,
            server=server,
            event_ids=tuple(e.event_id for e in events),
            dispatched_ts=now,
        )

    async def ack(self, dispatch_id: str, event_ids: list[str]) -> None:
        """Confirm that `event_ids` (a subset of, or all of, one
        dispatch's own events) genuinely finished downstream. An unknown
        `dispatch_id` (already fully acked, already redispatched and
        therefore replaced by a new id, or simply never issued) is a
        no-op, not an error — by the time an ack for a since-redispatched
        batch arrives late, ingress has already moved on, and there is
        nothing left for a stale ack to correct."""
        record = self._outstanding.get(dispatch_id)
        if record is None:
            return
        now = self._now()
        for event_id in event_ids:
            if record.events.pop(event_id, None) is not None:
                self._latency_ms.append((now - record.dispatched_ts) * 1000.0)
                if len(self._latency_ms) > LATENCY_WINDOW:
                    del self._latency_ms[: len(self._latency_ms) - LATENCY_WINDOW]
                self._event_index.pop(event_id, None)
                self._all_resolved_event_ids.add(event_id)
        if not record.events:
            del self._outstanding[dispatch_id]

    async def ack_by_event_ids(self, event_ids: list[str]) -> None:
        """What a real server actually calls: it only ever knows the
        `event_id`s it finished, never ingress's own `dispatch_id` — this
        resolves each one via `_event_index` and forwards to `ack()`
        grouped by whichever dispatch it currently belongs to. An
        `event_id` this transport has no record of (already acked, or a
        stray/duplicate ack) is silently skipped, same spirit as `ack()`'s
        own unknown-`dispatch_id` no-op."""
        by_dispatch: dict[str, list[str]] = {}
        for event_id in event_ids:
            dispatch_id = self._event_index.get(event_id)
            if dispatch_id is None:
                continue
            by_dispatch.setdefault(dispatch_id, []).append(event_id)
        for dispatch_id, ids in by_dispatch.items():
            await self.ack(dispatch_id, ids)

    def outstanding(self, server: str) -> list[Event]:
        """Every event currently dispatched to `server` with no ack yet,
        oldest dispatch first, in each dispatch's own original order."""
        result: list[Event] = []
        for record in sorted(
            (r for r in self._outstanding.values() if r.server == server),
            key=lambda r: r.dispatched_ts,
        ):
            result.extend(record.events.values())
        return result

    async def redispatch_expired(self) -> int:
        """Sweep every dispatch older than `ack_timeout_ms` with events
        still unacked, and re-dispatch exactly those remaining events as a
        BRAND NEW dispatch (a new `dispatch_id`, a new timestamp) — the
        expired record is discarded, not retried in place, so a late ack
        for the old `dispatch_id` correctly becomes the no-op `ack()`'s
        own docstring describes rather than silently re-clearing an event
        the new dispatch is now responsible for.

        This is ingress's own answer to a downstream server dying with
        events already in its process memory
        (docs/PHASE-J-INSPECTION.md section 5): it does not know or care
        WHY an ack never arrived — a crashed pod and a merely-slow ack
        look identical from here, by design — it just tries again, and
        idempotency (this module's own docstring) is what makes trying
        again safe.

        Returns the number of EVENTS redispatched (not the number of
        dispatch records), matching the granularity every other counter
        in this project reports at (`metrics.observe_retry`'s own
        `retries` counter is likewise per-event, not per-batch).
        """
        now = self._now()
        timeout_seconds = self._ack_timeout_ms / 1000.0
        expired_ids = [
            dispatch_id
            for dispatch_id, record in self._outstanding.items()
            if now - record.dispatched_ts >= timeout_seconds
        ]
        redispatched_count = 0
        for dispatch_id in expired_ids:
            record = self._outstanding.pop(dispatch_id)
            events = list(record.events.values())
            for event_id in record.events:
                self._event_index.pop(event_id, None)
            if not events:
                continue
            await self.dispatch(record.server, events)
            redispatched_count += len(events)
        return redispatched_count

    def latency_percentiles(self) -> dict[str, float]:
        """Dispatch-to-ack latency, milliseconds — the actual cost of the
        network hop, separate from `metrics.py`'s own queue-wait number.
        Named explicitly in this phase's own prompt: a payment has only
        ~60ms of queue budget left against its 200ms SLA once transport
        and simulated service time are accounted for, so this has to be
        visible on its own, not folded into either of the other two."""
        return {
            "p50": round(percentile(self._latency_ms, 0.50), 3),
            "p95": round(percentile(self._latency_ms, 0.95), 3),
            "p99": round(percentile(self._latency_ms, 0.99), 3),
        }

    def dispatch_stats(self) -> dict[str, int]:
        """Phase J6's own cross-process conservation identity:
        `dispatched == resolved + outstanding`, always — regardless of how
        many redispatches happened along the way (see the set-based
        counters' own docstring). `outstanding` here is the TOTAL count
        across every server this transport reaches, not per-server (see
        `outstanding(server)` for that)."""
        dispatched = len(self._all_dispatched_event_ids)
        resolved = len(self._all_resolved_event_ids)
        outstanding = len(self._event_index)
        return {"dispatched": dispatched, "resolved": resolved, "outstanding": outstanding}

    def reset(self) -> None:
        """Tests only."""
        self._outstanding.clear()
        self._event_index.clear()
        self._latency_ms.clear()
        self._next_id = 0
        self._all_dispatched_event_ids.clear()
        self._all_resolved_event_ids.clear()


# --------------------------------------------------------------------------
# HTTP delivery — one pooled client, batching, and the redispatch sweep.
# --------------------------------------------------------------------------


class HttpDeliverer:
    """The real, Phase J3 `DeliverFn`: POSTs a batch, JSON-encoded, to
    `{base_url}/ingest`.

    One shared `httpx.AsyncClient` for every server this deliverer reaches
    — "one pooled client, not a connection per batch" (this phase's own
    instruction). `httpx.AsyncClient` already pools/reuses connections
    per-host internally, so one client posting to both server1's and
    server2's own base URL costs nothing extra over one client per host.

    `clients_by_server`, when given, overrides that pooling with one
    client PER server instead. This exists only for tests: an ASGI-
    transport-backed client is bound to exactly one in-process app, and a
    test that runs server1's and server2's own `/ingest` as two separate
    in-process ASGI apps therefore genuinely needs two separate clients —
    a real deployment (real hosts, real sockets) never has this
    restriction and should never pass this argument.
    """

    def __init__(
        self,
        base_urls: dict[str, str],
        *,
        timeout_ms: float,
        client: httpx.AsyncClient | None = None,
        clients_by_server: dict[str, httpx.AsyncClient] | None = None,
    ) -> None:
        self._base_urls = dict(base_urls)
        if clients_by_server is not None:
            self._clients_by_server = dict(clients_by_server)
            self._shared_client: httpx.AsyncClient | None = None
            self._owns_shared_client = False
        else:
            self._clients_by_server = None
            self._shared_client = client or httpx.AsyncClient(timeout=timeout_ms / 1000.0)
            self._owns_shared_client = client is None

    def _client_for(self, server: str) -> httpx.AsyncClient:
        if self._clients_by_server is not None:
            return self._clients_by_server[server]
        assert self._shared_client is not None
        return self._shared_client

    async def __call__(self, server: str, events: list[Event]) -> None:
        url = f"{self._base_urls[server]}/ingest"
        payload = {"events": [e.model_dump(mode="json") for e in events]}
        response = await self._client_for(server).post(url, json=payload)
        response.raise_for_status()

    async def close(self) -> None:
        if self._owns_shared_client and self._shared_client is not None:
            await self._shared_client.aclose()


class Batcher:
    """Accumulates individual `submit()`ed events per server and flushes
    them as one `dispatch()` call once `batch_size` is reached or
    `batch_window_ms` has elapsed since the oldest still-buffered item for
    that server, whichever comes first — this phase's own spec.

    One background task per server (`_flush_loop`), so server1's own
    batching cadence never waits on server2's queue or vice versa. A
    clean `stop()` best-effort flushes whatever is still buffered rather
    than silently dropping it — the same "don't strand in-flight work on
    shutdown" instinct `WorkerPool.stop()`/`queue.clear()` already apply
    elsewhere in this codebase, even though a dropped buffered event here
    would eventually be re-admitted by nothing (unlike a queued Event,
    which `EventQueue.clear()` at least accounts for) — best-effort really
    does mean best-effort: a flush that itself fails on the way out is
    swallowed rather than raised out of `stop()`, since a demo shutting
    down is not the moment to surface a new exception.
    """

    def __init__(
        self,
        dispatch_fn: Callable[[str, list[Event]], Awaitable[DispatchResult]],
        *,
        batch_size: int,
        batch_window_ms: float,
        servers: list[str],
    ) -> None:
        self._dispatch_fn = dispatch_fn
        self._batch_size = batch_size
        self._batch_window_seconds = batch_window_ms / 1000.0
        self._queues: dict[str, asyncio.Queue[Event]] = {s: asyncio.Queue() for s in servers}
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def start(self) -> None:
        for server, queue in self._queues.items():
            self._tasks[server] = asyncio.create_task(
                self._flush_loop(server, queue), name=f"pulse-batcher-{server}"
            )

    async def stop(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def submit(self, server: str, event: Event) -> None:
        await self._queues[server].put(event)

    async def _flush_loop(self, server: str, queue: asyncio.Queue[Event]) -> None:
        buffer: list[Event] = []
        deadline: float | None = None
        loop = asyncio.get_running_loop()
        try:
            while True:
                timeout = None if deadline is None else max(0.0, deadline - loop.time())
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    if buffer:
                        await self._dispatch_fn(server, buffer)
                        buffer = []
                        deadline = None
                    continue
                buffer.append(event)
                if deadline is None:
                    deadline = loop.time() + self._batch_window_seconds
                if len(buffer) >= self._batch_size:
                    await self._dispatch_fn(server, buffer)
                    buffer = []
                    deadline = None
        except asyncio.CancelledError:
            if buffer:
                try:
                    await self._dispatch_fn(server, buffer)
                except Exception:  # noqa: BLE001 - best-effort flush on shutdown only
                    logger.debug("batcher flush-on-stop failed for %s", server, exc_info=True)
            raise


async def _redispatch_sweep_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=REDISPATCH_SWEEP_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            return
        await redispatch_expired()


# --------------------------------------------------------------------------
# Ambient default, matching metrics.py/ledger.py/sink.py/deferral.py's own
# precedent (one pipeline, one process — still true for ingress, the one
# process this module's own ambient state belongs to). Unconfigured by
# default: there is no universally correct `deliver` (Phase J2's own
# reasoning, unchanged) — `configure()` (direct/in-process) and
# `configure_http()` (real HTTP) are the two seams; whichever one a
# caller uses last is the one in effect.
# --------------------------------------------------------------------------


async def _unconfigured_deliver(server: str, events: list[Event]) -> None:
    raise RuntimeError(
        f"transport.dispatch({server!r}, ...) called before transport.configure() "
        "or transport.configure_http() wired up a real delivery function — "
        "see this module's own docstring"
    )


_default: Transport = Transport(deliver=_unconfigured_deliver)
_batcher: Batcher | None = None
_http_deliverer: HttpDeliverer | None = None
_redispatch_task: asyncio.Task[None] | None = None
_redispatch_stop: asyncio.Event | None = None


def configure(deliver: DeliverFn, *, ack_timeout_ms: float | None = None) -> None:
    """Wire the ambient default `Transport` to a same-process delivery
    function — the `--transport=direct` demo fallback's own mechanism, and
    Phase J2's original seam, unchanged. Mutually exclusive with
    `configure_http()`; whichever is called last wins."""
    global _default
    _default = Transport(deliver=deliver, ack_timeout_ms=ack_timeout_ms)


def configure_http(
    base_urls: dict[str, str] | None = None,
    *,
    servers_cfg: ServersConfig | None = None,
    client: httpx.AsyncClient | None = None,
    clients_by_server: dict[str, httpx.AsyncClient] | None = None,
    ack_timeout_ms: float | None = None,
) -> None:
    """Wire the ambient default `Transport` to real HTTP delivery.
    `base_urls` defaults to `http://127.0.0.1:{port}` for server1/server2,
    read from `config/servers.yaml`. Does not start the batcher or the
    redispatch sweep — call `start_http()` for that, once, after this."""
    global _default, _http_deliverer
    cfg = servers_cfg or load_servers_config()
    urls = base_urls or {
        "server1": f"http://127.0.0.1:{cfg.server1.port}",
        "server2": f"http://127.0.0.1:{cfg.server2.port}",
    }
    _http_deliverer = HttpDeliverer(
        urls,
        timeout_ms=cfg.transport.timeout_ms,
        client=client,
        clients_by_server=clients_by_server,
    )
    _default = Transport(
        deliver=_http_deliverer,
        ack_timeout_ms=ack_timeout_ms if ack_timeout_ms is not None else cfg.transport.ack_timeout_ms,
    )


async def start_http(servers_cfg: ServersConfig | None = None) -> None:
    """Start the batcher and the background redispatch sweep. Call once,
    after `configure_http()`. Idempotent-unsafe by design (calling twice
    without `stop_http()` leaks tasks) — matching `WorkerPool.start()`'s
    own "already started" contract elsewhere in this codebase, just
    without the explicit raise, since nothing here needs one for the
    tests this phase adds."""
    global _batcher, _redispatch_task, _redispatch_stop
    cfg = servers_cfg or load_servers_config()
    _batcher = Batcher(
        dispatch,
        batch_size=cfg.transport.batch_size,
        batch_window_ms=cfg.transport.batch_window_ms,
        servers=["server1", "server2"],
    )
    _batcher.start()
    _redispatch_stop = asyncio.Event()
    _redispatch_task = asyncio.create_task(
        _redispatch_sweep_loop(_redispatch_stop), name="pulse-transport-redispatch"
    )


async def stop_http() -> None:
    """Undo `configure_http()` + `start_http()`: stop the sweep, stop the
    batcher (best-effort flushing whatever it still held), and close the
    pooled HTTP client."""
    global _batcher, _redispatch_task, _redispatch_stop, _http_deliverer
    if _redispatch_stop is not None:
        _redispatch_stop.set()
    if _redispatch_task is not None:
        _redispatch_task.cancel()
        await asyncio.gather(_redispatch_task, return_exceptions=True)
        _redispatch_task = None
    _redispatch_stop = None
    if _batcher is not None:
        await _batcher.stop()
        _batcher = None
    if _http_deliverer is not None:
        await _http_deliverer.close()
        _http_deliverer = None


async def submit(server: str, event: Event) -> None:
    """Hand one event to the batcher (`config/servers.yaml`'s own
    `batch_size`/`batch_window_ms` govern when it actually goes out) if
    one is running, or dispatch it immediately as a batch of one
    otherwise — the fallback keeps this usable in tests/direct mode that
    never called `start_http()`."""
    if _batcher is not None:
        await _batcher.submit(server, event)
    else:
        await dispatch(server, [event])


async def dispatch(server: str, events: list[Event]) -> DispatchResult:
    return await _default.dispatch(server, events)


async def ack(dispatch_id: str, event_ids: list[str]) -> None:
    await _default.ack(dispatch_id, event_ids)


async def ack_by_event_ids(event_ids: list[str]) -> None:
    await _default.ack_by_event_ids(event_ids)


async def handle_ack_payload(payload: dict) -> None:
    """What `POST /ack`'s own handler actually calls — the wire shape a
    server sends: `{"event_ids": [...]}`. A plain dict in, not a Pydantic
    model, so this module carries no FastAPI dependency of its own; the
    HTTP layer (app.py, or a test's own minimal stand-in for ingress) owns
    request parsing/validation and hands this the already-decoded body."""
    await ack_by_event_ids(list(payload.get("event_ids", [])))


def outstanding(server: str) -> list[Event]:
    return _default.outstanding(server)


def dispatch_stats() -> dict[str, int]:
    return _default.dispatch_stats()


async def redispatch_expired() -> int:
    return await _default.redispatch_expired()


def latency_percentiles() -> dict[str, float]:
    return _default.latency_percentiles()


def reset_default() -> None:
    """Tests only — back to the unconfigured, loudly-failing default."""
    global _default
    _default = Transport(deliver=_unconfigured_deliver)
