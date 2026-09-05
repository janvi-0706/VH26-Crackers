"""Loader for config/servers.yaml — the three-process topology.

Owner: Lane A/D (Phase J).

Phase J1's own inspection (docs/PHASE-J-INSPECTION.md) named the target
shape; this file is the first executable artifact of it. Nothing here
starts a server, opens a socket, or touches `src/triage/app.py` — Phase J2
is explicitly "smallest interfaces only," and the single-process build
(`make dev`) is unchanged and still reads `config/tiers.yaml` alone.

Capacity, not worker count, is the number this file's own YAML expresses
(`config/servers.yaml`'s own header comment has the full reasoning): 135
u/s for server1, 15 u/s per pod for server2. Neither divides evenly by the
single-process build's 6 workers x 25 u/s/worker template, and this module
does not try to make them — `derive_workers()` below computes each
server's own worker count and per-worker rate FROM its capacity,
independently, using `config/tiers.yaml`'s own `worker_capacity_ups` (25
u/s) only as a REFERENCE for how fast one worker should aim to run, not as
a divisor that has to come out even:

    server1:  capacity 135 u/s / reference 25 u/s -> ceil(5.4) = 6 workers,
              135 / 6 = 22.5 u/s each (slower than the reference, so 6
              equal workers exactly reconstruct 135 u/s with none left over)
    server2:  capacity  15 u/s / reference 25 u/s -> ceil(0.6) = 1 worker
              per pod, 15 / 1 = 15 u/s (one worker per pod is enough; a
              second would have nothing to do)

Both are real, whole worker counts whose combined rate is EXACTLY their
server's own declared capacity — never more (would silently over-provision
past what capacity_us actually promises) and never fewer (would silently
under-serve it) — computed generically, not hand-picked per server.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

from .config import load_config
from .contracts import Tier

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SERVERS_CONFIG_PATH = REPO_ROOT / "config" / "servers.yaml"

Scaling = Literal["fixed", "hpa"]


class ServersConfigError(ValueError):
    """config/servers.yaml failed a structural or calibration check."""


def derive_workers(capacity_ups: float, reference_worker_rate_ups: float) -> tuple[int, float]:
    """The smallest whole number of equal-rate workers whose combined rate
    is EXACTLY `capacity_ups`, each sized as close to
    `reference_worker_rate_ups` as possible without exceeding it.

    Rounds the worker count UP (`ceil`), then divides capacity evenly
    across that count — never rounds capacity itself, and never leaves a
    remainder unaccounted for the way a naive `capacity //
    reference_rate` (floor) would (that could under-provision: e.g.
    15 // 25 == 0 workers for server2, silently serving nothing). A
    fractional per-worker rate (22.5 u/s for server1) is not a real-world
    oddity to apologise for — CLAUDE.md hard rule 2 already establishes
    that worker service time is a SIMULATED cost-model number, not a
    physical CPU/network resource with a natural integer granularity; a
    worker "running at 22.5 u/s" is exactly as meaningful a number as one
    running at 25.
    """
    if capacity_ups <= 0:
        raise ValueError(f"capacity_ups must be positive, got {capacity_ups!r}")
    if reference_worker_rate_ups <= 0:
        raise ValueError(
            f"reference_worker_rate_ups must be positive, got {reference_worker_rate_ups!r}"
        )
    count = max(1, math.ceil(capacity_ups / reference_worker_rate_ups))
    return count, capacity_ups / count


@dataclass(frozen=True)
class IngressSpec:
    port: int
    history_db: str


@dataclass(frozen=True)
class ServerSpec:
    """One of server1/server2. Exactly one of `capacity_us` (fixed) /
    `capacity_us_per_pod` (hpa) is set, matching which `scaling` value this
    server declares — enforced in `_parse_server`, not here, so this class
    itself stays a plain data holder."""

    name: str
    port: int
    tiers: tuple[Tier, ...]
    batching: bool
    scaling: Scaling
    capacity_us: float | None = None
    capacity_us_per_pod: float | None = None
    min_pods: int = 1
    max_pods: int = 1

    def capacity_at(self, pod_count: int | None = None) -> float:
        """Total work-units/sec this server provides right now.

        `pod_count` is required for an `hpa` server (there is no single
        answer without knowing how many pods are currently live — that
        count lives wherever K6-era orchestration state lives, not in this
        static config file) and ignored for a `fixed` server (there is
        only ever one meaningful count: the whole declared `capacity_us`).
        """
        if self.scaling == "fixed":
            assert self.capacity_us is not None  # guaranteed by _parse_server
            return self.capacity_us
        if pod_count is None:
            raise ValueError(
                f"server {self.name!r} scales via hpa; pod_count is required "
                "to know its current total capacity"
            )
        if not (self.min_pods <= pod_count <= self.max_pods):
            raise ValueError(
                f"pod_count {pod_count} outside server {self.name!r}'s own "
                f"declared range [{self.min_pods}, {self.max_pods}]"
            )
        assert self.capacity_us_per_pod is not None  # guaranteed by _parse_server
        return self.capacity_us_per_pod * pod_count

    def max_capacity_ups(self) -> float:
        """The most this server can ever provide — `capacity_us` for a
        fixed server (its only value), or `capacity_us_per_pod *
        max_pods` for an hpa server (its declared ceiling). This is the
        number `tests/test_servers_config.py`'s own oversubscription test
        checks system-wide: HPA scaling up to `max_pods` must never be
        enough to close the gap a real spike opens."""
        if self.scaling == "fixed":
            assert self.capacity_us is not None
            return self.capacity_us
        assert self.capacity_us_per_pod is not None
        return self.capacity_us_per_pod * self.max_pods

    def workers(
        self, *, reference_worker_rate_ups: float, pod_count: int | None = None
    ) -> tuple[int, float]:
        """`(worker_count, per_worker_rate_ups)` for ONE pod of this
        server — see `derive_workers()`'s own docstring for the formula.

        For an `hpa` server this is deliberately the SAME regardless of
        `pod_count`: every pod is an identical replica running its own
        worker pool sized off `capacity_us_per_pod` alone (that is the
        entire point of "per pod" capacity) — `pod_count` changes how many
        such pods exist, never what one pod's own internal layout looks
        like. Accepted as a keyword for symmetry with `capacity_at()` and
        because a future caller may want to assert it matches what
        `capacity_at(pod_count)` implies, not because this method's answer
        actually varies with it.
        """
        del pod_count  # see docstring: intentionally unused for hpa too
        base = self.capacity_us if self.scaling == "fixed" else self.capacity_us_per_pod
        assert base is not None
        return derive_workers(base, reference_worker_rate_ups)


@dataclass(frozen=True)
class TransportSpec:
    batch_size: int
    batch_window_ms: int
    timeout_ms: int
    ack_timeout_ms: int


@dataclass(frozen=True)
class MetricsPushSpec:
    push_interval_ms: int
    fragment_ttl_ms: int


@dataclass(frozen=True)
class ServersConfig:
    ingress: IngressSpec
    server1: ServerSpec
    server2: ServerSpec
    transport: TransportSpec
    metrics: MetricsPushSpec
    source_path: Path | None = None

    def server(self, name: str) -> ServerSpec:
        if name == "server1":
            return self.server1
        if name == "server2":
            return self.server2
        raise KeyError(f"no such server: {name!r}")

    def server_for_tier(self, tier: Tier) -> ServerSpec:
        for server in (self.server1, self.server2):
            if tier in server.tiers:
                return server
        raise KeyError(f"no server declares tier {tier!r}")

    def total_capacity_at_max(self) -> float:
        """System-wide capacity ceiling — server1's fixed capacity plus
        server2's own max-pods ceiling. The number
        `tests/test_servers_config.py` checks stays well under total
        demand at spike, and the number `config/servers.yaml`'s own
        `max_pods` comment derives 3 from."""
        return self.server1.max_capacity_ups() + self.server2.max_capacity_ups()


def _parse_server(name: str, row: dict[str, Any]) -> ServerSpec:
    scaling = row["scaling"]
    if scaling not in ("fixed", "hpa"):
        raise ServersConfigError(
            f"server {name!r}: scaling must be 'fixed' or 'hpa', got {scaling!r}"
        )
    tiers = tuple(Tier(t) for t in row["tiers"])

    capacity_us = row.get("capacity_us")
    capacity_us_per_pod = row.get("capacity_us_per_pod")
    if scaling == "fixed":
        if capacity_us is None:
            raise ServersConfigError(
                f"server {name!r}: scaling is 'fixed' but capacity_us is missing"
            )
        if capacity_us_per_pod is not None:
            raise ServersConfigError(
                f"server {name!r}: scaling is 'fixed'; capacity_us_per_pod "
                "does not apply (a fixed server has no pod count to vary)"
            )
    else:  # hpa
        if capacity_us_per_pod is None:
            raise ServersConfigError(
                f"server {name!r}: scaling is 'hpa' but capacity_us_per_pod is missing"
            )
        if capacity_us is not None:
            raise ServersConfigError(
                f"server {name!r}: scaling is 'hpa'; capacity_us does not "
                "apply (use capacity_us_per_pod - total capacity varies "
                "with live pod count)"
            )

    min_pods = int(row.get("min_pods", 1))
    max_pods = int(row.get("max_pods", 1))
    if scaling == "hpa" and min_pods > max_pods:
        raise ServersConfigError(
            f"server {name!r}: min_pods ({min_pods}) exceeds max_pods ({max_pods})"
        )

    return ServerSpec(
        name=name,
        port=int(row["port"]),
        tiers=tiers,
        batching=bool(row["batching"]),
        scaling=scaling,
        capacity_us=float(capacity_us) if capacity_us is not None else None,
        capacity_us_per_pod=(
            float(capacity_us_per_pod) if capacity_us_per_pod is not None else None
        ),
        min_pods=min_pods,
        max_pods=max_pods,
    )


def _parse(raw: dict[str, Any], source: Path | None) -> ServersConfig:
    ingress_row = raw["ingress"]
    ingress = IngressSpec(
        port=int(ingress_row["port"]),
        history_db=str(ingress_row["history_db"]),
    )

    server1 = _parse_server("server1", raw["server1"])
    server2 = _parse_server("server2", raw["server2"])

    if server1.scaling != "fixed":
        raise ServersConfigError(
            "server1 must never autoscale (CLAUDE.md hard rule 3: P0 is "
            f"never throttled or capacity-starved by a scheduler), got "
            f"scaling={server1.scaling!r}"
        )
    covered = set(server1.tiers) | set(server2.tiers)
    if covered != set(Tier):
        raise ServersConfigError(
            f"server1/server2 together must cover every tier; covered={covered}, "
            f"all tiers={set(Tier)}"
        )
    overlap = set(server1.tiers) & set(server2.tiers)
    if overlap:
        raise ServersConfigError(f"server1 and server2 both claim tier(s): {overlap}")

    transport_row = raw["transport"]
    transport = TransportSpec(
        batch_size=int(transport_row["batch_size"]),
        batch_window_ms=int(transport_row["batch_window_ms"]),
        timeout_ms=int(transport_row["timeout_ms"]),
        ack_timeout_ms=int(transport_row["ack_timeout_ms"]),
    )

    metrics_row = raw["metrics"]
    metrics_spec = MetricsPushSpec(
        push_interval_ms=int(metrics_row["push_interval_ms"]),
        fragment_ttl_ms=int(metrics_row["fragment_ttl_ms"]),
    )

    return ServersConfig(
        ingress=ingress,
        server1=server1,
        server2=server2,
        transport=transport,
        metrics=metrics_spec,
        source_path=source,
    )


_cached: ServersConfig | None = None


def load_servers_config(path: str | Path | None = None) -> ServersConfig:
    """Load config/servers.yaml. Cached for the default path, same pattern
    as `triage.config.load_config`; `PULSE_SERVERS_CONFIG` overrides it the
    same way `PULSE_CONFIG` overrides the tier table."""
    global _cached

    if path is None:
        env = os.environ.get("PULSE_SERVERS_CONFIG")
        path = Path(env) if env else DEFAULT_SERVERS_CONFIG_PATH
        if _cached is not None and _cached.source_path == Path(path):
            return _cached
        cache = True
    else:
        path = Path(path)
        cache = False

    with open(path, "r", encoding="utf-8") as fh:
        cfg = _parse(yaml.safe_load(fh), Path(path))

    if cache:
        _cached = cfg
    return cfg


def reset_cache() -> None:
    """Tests only."""
    global _cached
    _cached = None


def default_reference_worker_rate_ups() -> float:
    """`config/tiers.yaml`'s own `worker_capacity_ups` (25 u/s today) — the
    one number this module borrows from the tier table rather than
    re-declaring, so "how fast is one worker" stays defined in exactly one
    place regardless of how many processes now derive a worker count from
    it. Not a claim that this number is itself part of the frozen tier
    table's own economics (it is a cost-model property, not a value/SLA
    one) — just the existing, already-calibrated reference this module has
    no reason to duplicate."""
    return load_config().worker_capacity_ups


if __name__ == "__main__":  # pragma: no cover - operator convenience
    cfg = load_servers_config()
    ref = default_reference_worker_rate_ups()
    for spec in (cfg.server1, cfg.server2):
        count, rate = spec.workers(reference_worker_rate_ups=ref)
        cap = spec.capacity_us if spec.scaling == "fixed" else spec.capacity_us_per_pod
        unit = "u/s" if spec.scaling == "fixed" else "u/s/pod"
        print(
            f"{spec.name}: {cap:.1f} {unit} -> {count} worker(s) x {rate:.2f} u/s "
            f"[{spec.scaling}]"
        )
    print(f"system ceiling at max_pods: {cfg.total_capacity_at_max():.1f} u/s")
