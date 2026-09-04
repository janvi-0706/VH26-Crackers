"""A learned per-event cost estimate, replacing the flat per-type constant
`config/tiers.yaml` used to be the only source of.

Owner: Lane A.

Two different numbers now share the name "cost", on purpose, and confusing
them would quietly break the calibration invariants this whole project is
built on:

    true_cost(config, type, payload_size)   GROUND TRUTH. What
                                             `worker.py` actually simulates
                                             — the real, deterministic
                                             number of work-units this one
                                             event genuinely needs. Still a
                                             pure function of config +
                                             payload_size (CLAUDE.md hard
                                             rule 2: service time stays a
                                             simulated, deterministic
                                             cost model), just no longer
                                             blind to payload_size the way
                                             the flat per-type constant was.

    CostModel.estimate(type, payload_size)  LEARNED PREDICTION. What
                                             decision.py's ordering math
                                             (score's density term,
                                             slack's est_service_time) uses
                                             INSTEAD of the true cost — the
                                             honest position a real
                                             scheduler is actually in: it
                                             does not know how expensive an
                                             event truly is until a worker
                                             has already served one enough
                                             like it, so it estimates.

Why payload_size can vary true cost at all without breaking calibration:
`true_cost` is `config_prior * (payload_size / reference_payload_size)`,
where `reference_payload_size` is the exact midpoint of that type's own
`generator.PAYLOAD_SIZE_RANGES`. Since `payload_size` is drawn uniformly
from that same range, its expectation IS that midpoint — so
`E[true_cost] == config_prior` exactly, in the long run, under the
generator's own normal mix. The three calibration invariants
(`config.py`'s own load-time check) are stated in terms of that same
expectation and are therefore untouched by this file. What DOES change is
real, event-to-event variance the learner has something honest to learn
from, and — the whole point of the demo beat this stage asks for — a
sustained SHIFT in the payload-size distribution (a heavier mix injected
mid-run) shows up as a real, sustained shift in true cost, which the
learner then has to visibly re-adapt to.

Why a running estimate (EWMA over samples), not ridge regression: the
prompt names both as acceptable. A closed-form or iteratively-fit ridge
regression is the richer model, but it is also the one CLAUDE.md's own
hard rule 2 spirit ("deterministic, inspectable, no more machinery than
this stage needs") argues against here, and this project has no numpy/
scipy dependency to lean on for the linear algebra — hand-rolling matrix
inversion would spend hours on plumbing a per-(type, bucket) running
estimate already answers just as honestly, for the one thing that
actually matters for a live demo: converging visibly, and re-adapting
visibly when the input distribution shifts. `RunningEstimate` decays by
SAMPLE count, not wall-clock time — matching metrics.py's own `_Ewma`
class in spirit (recency-weighted, not a flat cumulative average) but
deliberately NOT time-based like that one: a cumulative average would
converge steadily but then respond to a regime shift ever more slowly the
longer the process has run, which is the one failure mode that would make
"inject a heavier mix and watch it re-adapt" stop working after the demo
has been running a while. A sample-recency EWMA keeps a constant
responsiveness regardless of how much history has already accumulated.

Why this is NOT a bandit, stated once, precisely: nothing here ever
chooses what to serve in order to learn faster. `CostModel.observe()` is
fed passively, once per real completion, from whatever the pipeline's own
existing traffic already does — there is no exploration term, no
deliberate perturbation, no action selection at all. An exploring policy
could misbehave live in front of a jury; a purely-observational estimator
cannot, because it never decides what happens, only how the (unchanged)
ordering math should weigh what already did.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .config import Config
from .contracts import EventType
from .generator import PAYLOAD_SIZE_RANGES

# Below this many real observations for a (type, bucket) pair, the config
# prior is trusted more than the running estimate — smoothly, not a hard
# cliff (see CostModel.estimate's own blend). 30 is enough for an EWMA at
# the smoothing factor below to have mostly forgotten its own zero-sample
# start, small enough that even a single type's own share of a baseline
# mix (the rarest tier, payment, at 5% of baseline ~16.6eps) reaches it
# within a few real seconds, not minutes.
MIN_CONFIDENT_SAMPLES = 30

# How many observations an EWMA "remembers" — not a half-life in time,
# a half-life in SAMPLE COUNT, matching this module's own design note on
# why sample-recency (not wall-clock-recency) is the right basis here.
# alpha = 1 - 0.5^(1/N) for an N-sample half-life; N=40 keeps the estimate
# responsive within a few dozen events of a real distribution shift
# without being so twitchy that ordinary per-event cost variance (real,
# even at a fixed payload size range) makes the dashboard's own
# convergence line visibly jitter.
_EWMA_HALF_LIFE_SAMPLES = 40.0
_EWMA_ALPHA = 1.0 - 0.5 ** (1.0 / _EWMA_HALF_LIFE_SAMPLES)

# How many payload-size buckets per type. Coarse on purpose: with a
# five-type mix at demo-scale traffic, a handful of buckets reach
# MIN_CONFIDENT_SAMPLES in seconds; sizing this to look "precise" (dozens
# of buckets) would mean production doesn't actually validate this the
# same as a real deployment.
BUCKETS_PER_TYPE = 4


def _bucket(event_type: EventType, payload_size: int) -> int:
    """Which of BUCKETS_PER_TYPE equal-width buckets `payload_size` falls
    into, within this type's own known range (generator.PAYLOAD_SIZE_RANGES
    — the same table the generator itself draws from, imported rather than
    duplicated so the two can never drift apart)."""
    low, high = PAYLOAD_SIZE_RANGES[event_type]
    width = max(high - low, 1)
    fraction = (payload_size - low) / width
    bucket = int(fraction * BUCKETS_PER_TYPE)
    return max(0, min(BUCKETS_PER_TYPE - 1, bucket))


def _reference_payload_size(event_type: EventType) -> float:
    low, high = PAYLOAD_SIZE_RANGES[event_type]
    return (low + high) / 2.0


def true_cost(config: Config, event_type: EventType, payload_size: int) -> float:
    """The ground-truth simulated cost for one event — what worker.py
    actually spends simulated service time on. See this module's own
    docstring for the calibration argument: this equals `config`'s own
    flat prior exactly when `payload_size` is at its type's own midpoint,
    and its EXPECTATION over the generator's own uniform draw is exactly
    that prior, so the three calibration invariants are untouched."""
    prior = config.tiers[event_type].cost
    reference = _reference_payload_size(event_type)
    return prior * (payload_size / reference)


@dataclass
class RunningEstimate:
    """EWMA over discrete observations — see module docstring for why
    sample-count decay, not wall-clock decay."""

    alpha: float = _EWMA_ALPHA
    mean: float | None = None
    count: int = 0

    def observe(self, value: float) -> None:
        self.count += 1
        self.mean = value if self.mean is None else self.mean + self.alpha * (value - self.mean)


@dataclass
class TypeCostSummary:
    """One type's own learned-vs-prior picture — what `GET
    /control/costmodel` and the dashboard's convergence chart both read."""

    event_type: str
    prior: float
    learned: float
    samples: int
    confidence: float  # 0..1 — how much `learned` actually reflects real data


class CostModel:
    """One instance per Engine (per-Engine, not ambient — matching
    `admission.AdmissionControl`'s own precedent: a fresh Engine or a
    `/control/reset` must not inherit another run's learning state)."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self._estimates: dict[tuple[EventType, int], RunningEstimate] = {
            (event_type, bucket): RunningEstimate()
            for event_type in config.tiers
            for bucket in range(BUCKETS_PER_TYPE)
        }

    def observe(self, event_type: EventType, payload_size: int, observed_cost: float) -> None:
        """Fed once per real completion (worker.py, at the same point
        `metrics.observe_complete` already runs) with the event's own
        TRUE cost — "updated from observed service times" means exactly
        this: what a worker actually just spent, not a guess."""
        self._estimates[(event_type, _bucket(event_type, payload_size))].observe(observed_cost)

    def estimate(self, event_type: EventType, payload_size: int) -> float:
        """The blended learned/prior estimate `decision.py`'s ordering
        math should use INSTEAD OF the true cost. Blends smoothly by
        confidence (`count / MIN_CONFIDENT_SAMPLES`, capped at 1) rather
        than switching on a hard cliff at the threshold — a smooth blend
        is also what makes the dashboard's own convergence chart a curve
        converging toward the learned value, not a line that jumps."""
        prior = self.config.tiers[event_type].cost
        running = self._estimates[(event_type, _bucket(event_type, payload_size))]
        if running.mean is None:
            return prior
        confidence = min(1.0, running.count / MIN_CONFIDENT_SAMPLES)
        return confidence * running.mean + (1.0 - confidence) * prior

    def reset(self) -> None:
        """Back to a fresh, unlearned state — in place, not a new object,
        because `EventQueue`/`WorkerPool` are handed this exact instance
        once at construction and are never rebuilt on `/control/reset`
        (only stopped/started); replacing `self._estimates` here is what
        makes those two references stay correct without Engine.reset()
        also having to know about and re-thread a second, new instance
        through them."""
        for key in self._estimates:
            self._estimates[key] = RunningEstimate()

    def summary(self) -> list[TypeCostSummary]:
        """One row per EventType — buckets pooled (sample-weighted mean of
        each bucket's own learned value) into a single learned-vs-prior
        number per type, which is the granularity both the API response
        and the convergence chart actually want; per-bucket detail stays
        an implementation detail of how the estimate is learned, not
        something either consumer needs to know about."""
        rows: list[TypeCostSummary] = []
        for event_type in self.config.tiers:
            prior = self.config.tiers[event_type].cost
            total_samples = 0
            weighted_sum = 0.0
            for bucket in range(BUCKETS_PER_TYPE):
                running = self._estimates[(event_type, bucket)]
                if running.mean is not None:
                    weighted_sum += running.mean * running.count
                    total_samples += running.count
            learned = (weighted_sum / total_samples) if total_samples else prior
            confidence = min(1.0, total_samples / MIN_CONFIDENT_SAMPLES)
            blended = confidence * learned + (1.0 - confidence) * prior
            rows.append(
                TypeCostSummary(
                    event_type=event_type.value,
                    prior=round(prior, 4),
                    learned=round(blended, 4),
                    samples=total_samples,
                    confidence=round(confidence, 3),
                )
            )
        return rows
