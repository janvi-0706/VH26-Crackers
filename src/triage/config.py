"""Loader for config/tiers.yaml, plus the calibration guard.

Owner: Lane D. FROZEN alongside contracts.py at the end of Stage A.

The tier table is externalised data, not Python constants, for three reasons:
an operator should be able to retune it without touching the engine, the
benchmark loads two different tables inside one process, and a config file is
reviewable evidence of the data design in a way scattered literals are not.

Because the whole demo depends on three numeric relationships holding, this
module re-derives them from the table on every load and refuses to start if
they have drifted. A silently miscalibrated table would mean either P0 cannot
be protected or triage never triggers — both of which look like a bug in the
engine, hours away from where the real change was made.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .contracts import EventType, Tier

# Code/src/triage/config.py -> Code/
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "tiers.yaml"


class CalibrationError(RuntimeError):
    """The tier table no longer satisfies the three demo invariants."""


@dataclass(frozen=True)
class TierSpec:
    """One row of the tier table."""

    name: EventType
    tier: Tier
    value: float
    sla_ms: int
    cost: float

    @property
    def sla_seconds(self) -> float:
        return self.sla_ms / 1000.0


@dataclass(frozen=True)
class CalibrationCheck:
    name: str
    expected: float
    actual: float
    tolerance: float

    @property
    def ok(self) -> bool:
        return abs(self.actual - self.expected) <= self.tolerance

    def __str__(self) -> str:
        mark = "ok " if self.ok else "OFF"
        return (
            f"[{mark}] {self.name}: expected {self.expected:.2f} u/s, "
            f"actual {self.actual:.2f} u/s (tolerance +/-{self.tolerance:.2f})"
        )


@dataclass(frozen=True)
class Config:
    schema_version: int
    tiers: dict[EventType, TierSpec]
    mix: dict[EventType, float]
    worker_count: int
    worker_capacity_ups: float
    baseline_eps: float
    spike_multiplier: float
    calibration: dict[str, float] = field(default_factory=dict)
    source_path: Path | None = None

    # --- derived quantities -------------------------------------------------

    @property
    def total_capacity_ups(self) -> float:
        """Work-units per second the whole worker pool can serve."""
        return self.worker_count * self.worker_capacity_ups

    @property
    def spike_eps(self) -> float:
        return self.baseline_eps * self.spike_multiplier

    def weighted_cost_per_event(self, tier: Tier | None = None) -> float:
        """Expected service cost of one event drawn from the mix. Restricted to
        a single tier when `tier` is given, which is how we ask "what does the
        protected tier alone cost us?"."""
        return sum(
            self.mix[t] * spec.cost
            for t, spec in self.tiers.items()
            if tier is None or spec.tier is tier
        )

    def weighted_value_per_event(self, tier: Tier | None = None) -> float:
        return sum(
            self.mix[t] * spec.value
            for t, spec in self.tiers.items()
            if tier is None or spec.tier is tier
        )

    def demand_ups(self, eps: float, tier: Tier | None = None) -> float:
        """Offered load in work-units/sec at an arrival rate of `eps`."""
        return eps * self.weighted_cost_per_event(tier)

    def tiers_of(self, tier: Tier) -> list[TierSpec]:
        return [s for s in self.tiers.values() if s.tier is tier]

    # --- the guard ----------------------------------------------------------

    def calibration_report(self) -> list[CalibrationCheck]:
        tol = float(self.calibration.get("tolerance_ups", 1.0))
        return [
            CalibrationCheck(
                "P0 demand at spike (must sit under capacity)",
                float(self.calibration["p0_demand_at_spike_ups"]),
                self.demand_ups(self.spike_eps, Tier.P0),
                tol,
            ),
            CalibrationCheck(
                "total demand at spike (must exceed capacity)",
                float(self.calibration["total_demand_at_spike_ups"]),
                self.demand_ups(self.spike_eps),
                tol,
            ),
            CalibrationCheck(
                "total demand at baseline (must be far under capacity)",
                float(self.calibration["total_demand_at_baseline_ups"]),
                self.demand_ups(self.baseline_eps),
                tol,
            ),
        ]

    def verify(self) -> list[CalibrationCheck]:
        """Re-derive the three invariants. Raises if the table has drifted."""
        checks = self.calibration_report()
        broken = [c for c in checks if not c.ok]

        # Structural invariants, independent of the declared expectations.
        problems = [str(c) for c in broken]
        mix_total = sum(self.mix.values())
        if abs(mix_total - 1.0) > 1e-6:
            problems.append(f"[OFF] mix must sum to 1.0, got {mix_total:.6f}")
        p0_spike = self.demand_ups(self.spike_eps, Tier.P0)
        if p0_spike >= self.total_capacity_ups:
            problems.append(
                f"[OFF] P0 demand at spike {p0_spike:.2f} u/s does not fit under "
                f"capacity {self.total_capacity_ups:.2f} u/s — P0 could not be "
                f"protected, which breaks hard rule 3"
            )
        total_spike = self.demand_ups(self.spike_eps)
        if total_spike <= self.total_capacity_ups:
            problems.append(
                f"[OFF] total demand at spike {total_spike:.2f} u/s fits under "
                f"capacity {self.total_capacity_ups:.2f} u/s — nothing would "
                f"force triage and the demo has no story"
            )
        if problems:
            raise CalibrationError(
                "config/tiers.yaml is no longer calibrated:\n  "
                + "\n  ".join(problems)
            )
        return checks


def _parse(raw: dict[str, Any], source: Path | None) -> Config:
    tiers: dict[EventType, TierSpec] = {}
    for name, row in raw["tiers"].items():
        et = EventType(name)
        tiers[et] = TierSpec(
            name=et,
            tier=Tier(row["tier"]),
            value=float(row["value"]),
            sla_ms=int(row["sla_ms"]),
            cost=float(row["cost"]),
        )

    mix = {EventType(k): float(v) for k, v in raw["mix"].items()}
    missing = set(tiers) - set(mix)
    if missing:
        raise ValueError(f"mix is missing entries for: {sorted(m.value for m in missing)}")

    return Config(
        schema_version=int(raw.get("schema_version", 1)),
        tiers=tiers,
        mix=mix,
        worker_count=int(raw["workers"]["count"]),
        worker_capacity_ups=float(raw["workers"]["capacity_units_per_sec"]),
        baseline_eps=float(raw["load"]["baseline_eps"]),
        spike_multiplier=float(raw["load"]["spike_multiplier"]),
        calibration={k: float(v) for k, v in raw.get("calibration", {}).items()},
        source_path=source,
    )


_cached: Config | None = None


def load_config(path: str | Path | None = None, *, verify: bool = True) -> Config:
    """Load the tier table. Cached for the default path; PULSE_CONFIG overrides."""
    global _cached

    if path is None:
        env = os.environ.get("PULSE_CONFIG")
        path = Path(env) if env else DEFAULT_CONFIG_PATH
        if _cached is not None and _cached.source_path == Path(path):
            return _cached
        cache = True
    else:
        path = Path(path)
        cache = False

    with open(path, "r", encoding="utf-8") as fh:
        cfg = _parse(yaml.safe_load(fh), Path(path))

    if verify:
        cfg.verify()
    if cache:
        _cached = cfg
    return cfg


def reset_cache() -> None:
    """Tests only."""
    global _cached
    _cached = None


if __name__ == "__main__":  # pragma: no cover - operator convenience
    c = load_config()
    print(f"config: {c.source_path}")
    print(f"capacity: {c.total_capacity_ups:.1f} u/s "
          f"({c.worker_count} workers x {c.worker_capacity_ups:.0f} u/s)")
    print(f"baseline: {c.baseline_eps:.2f} eps, spike: {c.spike_eps:.2f} eps "
          f"({c.spike_multiplier:.0f}x)")
    print(f"weighted cost/event: {c.weighted_cost_per_event():.3f} u")
    for check in c.calibration_report():
        print(check)
