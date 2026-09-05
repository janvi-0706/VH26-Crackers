"""Metrics fragment push — how server1/server2 tell ingress what their own
local counters are, once "the pipeline" is no longer one process that can
just read its own module-level state.

Owner: Lane D (Phase J2 interface, Phase J3 real HTTP).

Servers PUSH; ingress never polls. docs/PHASE-J-INSPECTION.md (section 4)
already named why: server2 can be 1-3 pods behind a Kubernetes Service
(`config/servers.yaml`'s own `scaling: hpa`), and a poll against that
Service reaches ONE random pod, never all of them — there is no way to
poll "every server2 instance" through a Service by construction.

The receiving/aggregating side (`FragmentStore`, `push`/`fragments`/
`aggregate`/`instance_count`) is unchanged from Phase J2 — same class, same
functions, same tests. Phase J3 adds the other end of the wire:

  ReportingClient        a background loop a server runs, calling a
                         caller-supplied `collect()` on its own
                         `push_interval_ms` cadence (`config/servers.yaml`)
                         and POSTing the result to `{ingress_url}/metrics/
                         report`.
  default_instance_id()  `POD_NAME` (a real Kubernetes pod identity, once
                         one exists) if set, else a fresh UUID — this
                         phase's own instruction, and the reason
                         `FragmentStore` keys by `(server, instance_id)`
                         rather than `server` alone in the first place
                         (Phase J2's own docstring already worked out why
                         a multi-pod server2 needs that).
  fragment_from_payload  turns a decoded `POST /metrics/report` JSON body
                         into a `MetricsFragment` — the one place that
                         mapping is written, so app.py's real endpoint and
                         every test's own minimal stand-in for ingress
                         agree on it by construction rather than by
                         convention.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

import httpx

from .servers_config import load_servers_config

logger = logging.getLogger(__name__)

# The counters this project's own conservation equation and dashboard
# already track per tier (docs/PHASE-J-INSPECTION.md section 4's own
# table) — the known, expected keys of `MetricsFragment.counters`. Not
# enforced as a closed set (`counters` stays a plain dict, deliberately,
# so a server can report a counter this module was never told about
# without a schema change here) — named here only as documentation of what
# a real fragment is expected to carry, mirroring how `contracts.py`'s own
# `MetricsFrame` names its fields as documentation of the dashboard's
# contract even though nothing in THIS module reads them by name.
KNOWN_COUNTER_KEYS: tuple[str, ...] = (
    "processed", "in_queue", "in_flight", "sampled_out", "shed",
)


def default_instance_id() -> str:
    """`POD_NAME` if this is actually running as a Kubernetes pod (that
    env var is the standard way a pod's own downward API exposes its
    name to the container); a fresh UUID otherwise, for local/demo runs
    where there is no pod at all. This phase's own instruction, verbatim."""
    return os.environ.get("POD_NAME") or str(uuid.uuid4())


@dataclass(frozen=True)
class MetricsFragment:
    """One server instance's own local contribution, as of `pushed_ts`.

    `counters` is intentionally a flat `dict[str, float]`, not a fixed set
    of named fields: which counters a given server can even report depends
    on which tiers it owns (server1 never has a `sampled_out`/`shed`
    value to report — P0 is never sampled or shed, per CLAUDE.md hard rule
    3 — and this module has no reason to force it to send zeros for
    counters that are structurally meaningless for it).
    """

    server: str
    instance_id: str
    pushed_ts: float
    counters: dict[str, float] = field(default_factory=dict)


def fragment_from_payload(payload: dict) -> MetricsFragment:
    """The wire shape a `POST /metrics/report` body arrives in, decoded
    into a `MetricsFragment`. A plain dict in, not a Pydantic model — see
    this module's own docstring on why."""
    return MetricsFragment(
        server=str(payload["server"]),
        instance_id=str(payload["instance_id"]),
        pushed_ts=float(payload["pushed_ts"]),
        counters={k: float(v) for k, v in payload.get("counters", {}).items()},
    )


def handle_metrics_report_payload(payload: dict) -> None:
    """What `POST /metrics/report`'s own handler actually calls."""
    push(fragment_from_payload(payload))


class FragmentStore:
    """Keeps the single most recent fragment per (server, instance_id),
    and aggregates whichever of those are still fresh as of `now()`."""

    def __init__(
        self,
        *,
        fragment_ttl_ms: float | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._ttl_ms = (
            fragment_ttl_ms
            if fragment_ttl_ms is not None
            else load_servers_config().metrics.fragment_ttl_ms
        )
        self._now = now
        # (server, instance_id) -> latest fragment from that instance.
        # Replaced wholesale on every push, never merged field-by-field —
        # a fragment is one instance's own complete self-report at
        # `pushed_ts`, not a delta to fold into a running total.
        self._latest: dict[tuple[str, str], MetricsFragment] = {}

    def push(self, fragment: MetricsFragment) -> None:
        """Record `fragment` as the newest one from its (server,
        instance_id). An older `pushed_ts` arriving after a newer one
        (a delayed retransmission, out-of-order delivery) is dropped
        rather than overwriting the fresher data — the store always
        reflects the most RECENT thing each instance is known to have
        reported, not the most recently ARRIVED message."""
        key = (fragment.server, fragment.instance_id)
        current = self._latest.get(key)
        if current is not None and current.pushed_ts > fragment.pushed_ts:
            return
        self._latest[key] = fragment

    def _is_fresh(self, fragment: MetricsFragment, now: float) -> bool:
        age_ms = (now - fragment.pushed_ts) * 1000.0
        return age_ms <= self._ttl_ms

    def fragments(self, server: str | None = None, *, fresh_only: bool = True) -> list[MetricsFragment]:
        """Every currently-known fragment, optionally filtered to one
        server and/or to only those still within `fragment_ttl_ms`."""
        now = self._now()
        result = [
            f for f in self._latest.values()
            if (server is None or f.server == server)
            and (not fresh_only or self._is_fresh(f, now))
        ]
        result.sort(key=lambda f: (f.server, f.instance_id))
        return result

    def instance_count(self, server: str) -> int:
        """How many instances of `server` have a live (unexpired)
        fragment right now — the number the dashboard's own worker-pool
        panel would want for an `hpa` server, since `config/servers.yaml`
        alone only ever says the STATIC [min_pods, max_pods] range, never
        how many pods actually happen to be up this second."""
        return len(self.fragments(server))

    def aggregate(self, server: str | None = None) -> dict[str, float]:
        """Sum every live fragment's `counters`, across however many
        instances currently have a fresh one — the mechanism
        docs/PHASE-J-INSPECTION.md section 4 describes needing for a
        multi-pod server2: this IS that sum. A counter key only some
        instances report is summed over the ones that do (missing means
        0 for that instance, not "exclude this key from the total")."""
        totals: dict[str, float] = {}
        for fragment in self.fragments(server=server):
            for key, value in fragment.counters.items():
                totals[key] = totals.get(key, 0.0) + value
        return totals

    def reset(self) -> None:
        """Tests only."""
        self._latest.clear()


# --------------------------------------------------------------------------
# The push side — a server's own background loop.
# --------------------------------------------------------------------------


class ReportingClient:
    """A background loop one server instance runs: every
    `push_interval_ms`, call `collect()` for this instance's own current
    counters and POST them to `{ingress_url}/metrics/report`.

    A push failure (ingress briefly unreachable, a transient network
    error) is logged and otherwise swallowed, never raised out of the
    loop — a missed push simply means this instance's own fragment ages
    out `fragment_ttl_ms` after its LAST successful push rather than
    immediately; that is the intended, documented behaviour (this
    module's own top docstring), not a bug this class needs to work
    around by retrying individual pushes. The next scheduled push tries
    again on its own.
    """

    def __init__(
        self,
        *,
        server: str,
        ingress_url: str,
        collect: Callable[[], dict[str, float]],
        push_interval_ms: float | None = None,
        instance_id: str | None = None,
        client: httpx.AsyncClient | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._server = server
        self._instance_id = instance_id or default_instance_id()
        self._ingress_url = ingress_url.rstrip("/")
        self._collect = collect
        self._interval_seconds = (
            push_interval_ms
            if push_interval_ms is not None
            else load_servers_config().metrics.push_interval_ms
        ) / 1000.0
        self._now = now
        self._client = client or httpx.AsyncClient(timeout=5.0)
        self._owns_client = client is None
        self._task: asyncio.Task[None] | None = None
        self._stop: asyncio.Event | None = None

    @property
    def instance_id(self) -> str:
        return self._instance_id

    def start(self) -> None:
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name=f"pulse-report-{self._server}")

    async def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self._owns_client:
            await self._client.aclose()

    async def _loop(self) -> None:
        assert self._stop is not None
        while not self._stop.is_set():
            await self.push_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def push_once(self) -> None:
        """One push, callable directly (tests use this to avoid waiting
        out a real `push_interval_ms` in the loop)."""
        payload = {
            "server": self._server,
            "instance_id": self._instance_id,
            "pushed_ts": self._now(),
            "counters": self._collect(),
        }
        try:
            response = await self._client.post(
                f"{self._ingress_url}/metrics/report", json=payload
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001 - a missed push just ages out at ingress
            logger.debug("metrics fragment push failed", exc_info=True)


# --------------------------------------------------------------------------
# Ambient default store, matching metrics.py/ledger.py's own precedent —
# there is exactly one ingress process receiving fragments, the same "one
# pipeline" reasoning those modules already document.
# --------------------------------------------------------------------------

_default = FragmentStore()


def push(fragment: MetricsFragment) -> None:
    _default.push(fragment)


def fragments(server: str | None = None, *, fresh_only: bool = True) -> list[MetricsFragment]:
    return _default.fragments(server, fresh_only=fresh_only)


def instance_count(server: str) -> int:
    return _default.instance_count(server)


def aggregate(server: str | None = None) -> dict[str, float]:
    return _default.aggregate(server)


def reset_default() -> None:
    """Tests only — mirrors metrics.reset()/ledger.reset()'s own
    per-test-isolation contract."""
    global _default
    _default = FragmentStore()
