"""Deterministic, adjustable source for the PULSE event stream.

The generator owns only emission identity and partition identity.  Classification
is deliberately a separate stage: tier, value, cost, deadline and sequence are
assigned by :mod:`triage.classifier`.

Stage F adds one more thing the generator owns: asking `admission.py` for a
credit before it emits, on the *real* async stream (`events()`) only —
`emit()`/`emit_single()` stay the raw, ungated, synchronous path they always
were (benchmark setup, `Engine.inject_event()`'s one-off drops), since
neither of those represents the live pipeline's own organic arrival stream
that upstream backpressure exists to shape.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import AsyncIterator

from . import metrics
from .admission import AdmissionControl
from .config import Config, load_config
from .contracts import EventType


# The simulator varies payload size without carrying type-specific business
# payloads through the generic scheduler contract.
PAYLOAD_SIZE_RANGES: dict[EventType, tuple[int, int]] = {
    EventType.PAYMENT: (256, 1024),
    EventType.ORDER: (384, 2048),
    EventType.INVENTORY: (128, 768),
    EventType.CLICK: (64, 512),
    EventType.LOG: (256, 4096),
}


@dataclass(frozen=True, slots=True)
class GeneratedEvent:
    """The ingress envelope before classifier enrichment."""

    event_id: str
    dedup_key: str
    partition_key: str
    type: EventType
    payload_size: int
    ingest_ts: float


class EventGenerator:
    """Emit the configured mix at a runtime-adjustable rate.

    ``emit`` is synchronous for fast benchmark setup.  ``events`` is the
    asynchronous source used by the eventual application loop.
    """

    # A single catch-up burst in events() stops here even if the schedule
    # says we're further behind than that. At the spec spike rate (333
    # eps) this is well under a second of backlog to work through in one
    # go; it exists so a stall or a deliberately absurd rate can't hog the
    # event loop indefinitely instead of yielding back to workers/`/ws`.
    _MAX_BURST = 500

    def __init__(
        self,
        rate: float | None = None,
        *,
        config: Config | None = None,
        seed: int | None = None,
        customer_count: int = 500,
        admission: AdmissionControl | None = None,
    ) -> None:
        if customer_count <= 0:
            raise ValueError("customer_count must be positive")
        self.config = config or load_config()
        self._rate = self.config.baseline_eps if rate is None else float(rate)
        if self._rate < 0:
            raise ValueError("rate must be non-negative")
        self.customer_count = customer_count
        self._rng = random.Random(seed)
        self._emission_number = 0
        self._business_numbers: defaultdict[tuple[EventType, str], int] = defaultdict(int)
        # Per-Engine, not ambient (see admission.py's own AdmissionControl
        # docstring on why) — constructed from the same Config this
        # generator already has, unless the caller (Engine, or a test)
        # supplies one explicitly.
        self.admission = admission or AdmissionControl(config=self.config)

    @property
    def rate(self) -> float:
        return self._rate

    def set_rate(self, rate: float) -> None:
        """Change the source rate; the next async interval uses it."""
        rate = float(rate)
        if rate < 0:
            raise ValueError("rate must be non-negative")
        self._rate = rate

    def _choose_type(self) -> EventType:
        return self._rng.choices(
            list(self.config.mix),
            weights=[self.config.mix[event_type] for event_type in self.config.mix],
            k=1,
        )[0]

    def emit(self) -> GeneratedEvent:
        """Create one event using the configured mix and customer pool.
        The raw, ungated path — no admission check. `events()` is where
        the real pipeline's stream goes through `admission.py`; this stays
        available unchanged for synchronous benchmark setup that wants
        events without a live pressure signal to gate against."""
        return self.emit_single(self._choose_type())

    def emit_single(
        self, event_type: EventType, partition_key: str | None = None
    ) -> GeneratedEvent:
        """Create one event of a caller-specified type, outside the mix draw.

        This is the identity half of ``inject_event`` (see app.py's
        ``Engine``): dropping a single event into a running stream — e.g.
        "watch one huge order jump the queue" — still has to go through the
        same event_id/dedup_key/payload-size machinery as the mix does, or
        it would be a structurally different kind of event pretending to be
        a normal one. What it must NOT do is touch tier/value/cost/deadline
        — those are classification economics, assigned only by
        :mod:`triage.classifier`, from config, never by a caller.

        ``partition_key`` defaults to a random customer from the same pool
        ``emit()`` draws from, so an injected event without an explicit
        customer looks exactly like an organic one.
        """
        if partition_key is None:
            partition_key = f"customer:{self._rng.randrange(self.customer_count)}"
        business_key = (event_type, partition_key)
        self._business_numbers[business_key] += 1
        self._emission_number += 1
        low, high = PAYLOAD_SIZE_RANGES[event_type]
        return GeneratedEvent(
            event_id=f"evt-{self._emission_number:08d}",
            dedup_key=(
                f"{event_type.value}:{partition_key}:"
                f"{self._business_numbers[business_key]}"
            ),
            partition_key=partition_key,
            type=event_type,
            payload_size=self._rng.randint(low, high),
            ingest_ts=time.time(),
        )

    def retry(self, event: GeneratedEvent) -> GeneratedEvent:
        """Make a new physical emission for the same business fact.

        The retry keeps ``dedup_key`` and ``partition_key`` but receives a new
        ``event_id``.  This is the concrete reason the identity model has five
        fields rather than one.
        """
        self._emission_number += 1
        return GeneratedEvent(
            event_id=f"evt-{self._emission_number:08d}",
            dedup_key=event.dedup_key,
            partition_key=event.partition_key,
            type=event.type,
            payload_size=event.payload_size,
            ingest_ts=time.time(),
        )

    async def events(self, stop_event: asyncio.Event | None = None) -> AsyncIterator[GeneratedEvent]:
        """Yield events until ``stop_event`` is set, at (close to) ``rate``.

        Paced against a running schedule (``next_emit_time``), not "sleep
        the nominal interval after every single emission". That distinction
        matters more than it looks: ``asyncio.sleep()`` has a real fixed
        overhead per call — even with worker.py's Windows timer-resolution
        fix, on the order of ~1ms — which is negligible against a 60ms
        interval (16.65 eps, our baseline) but dominates a 3ms interval
        (333 eps, our spec spike rate). Sleeping once per event at spike
        rate was measured to sustain only ~200 eps, not 333 — a generator
        that silently can't hit its own documented calibration.

        The fix: catch up in a tight, no-sleep burst whenever the schedule
        says we're behind, and only sleep for whatever time is genuinely
        left before the next scheduled emission. One sleep call's overhead
        is then amortised across a whole burst instead of paid per event.
        ``_MAX_BURST`` caps a single catch-up so an absurd rate (or a long
        stall) can't monopolise the event loop indefinitely — it degrades
        by falling further behind rather than starving workers/`/ws`.

        A zero rate remains responsive to control and shutdown without
        busy-spinning.

        Stage F: every scheduled slot in the burst below still fires on
        schedule (the *pacing* is untouched — that is "offered", the rate
        presented at this post-rate-throttle boundary) but now asks
        `admission.py` for a credit first. Denied, the slot is skipped —
        no event is created, nothing is yielded, nothing reaches the
        classifier or the queue. That gap between how many slots fired and
        how many actually admitted an event *is* offered_rate vs
        admitted_rate, made real here rather than only defined in
        metrics.py.
        """
        next_emit_time = time.monotonic()
        while stop_event is None or not stop_event.is_set():
            if self._rate <= 0:
                await asyncio.sleep(0.05)
                next_emit_time = time.monotonic()
                continue

            interval = 1.0 / self._rate
            now = time.monotonic()
            burst = 0
            while next_emit_time <= now and burst < self._MAX_BURST:
                event_type = self._choose_type()
                wall_now = time.time()
                pressure = metrics.current_pressure(self.config, now=wall_now)
                cost = self.admission.cost_of(event_type)
                admitted = self.admission.try_acquire(event_type, pressure, now=wall_now)
                metrics.observe_admission(cost, admitted, now=wall_now)
                if admitted:
                    yield self.emit_single(event_type)
                next_emit_time += interval
                burst += 1
                now = time.monotonic()

            remaining = next_emit_time - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(remaining)
            else:
                # Behind schedule even after a full burst (rate change,
                # long stall, or an absurd target): yield control once so
                # the loop stays responsive, then let the next iteration's
                # burst continue catching up rather than blocking here.
                await asyncio.sleep(0)

    stream = events
    generate = events


async def generate(
    rate: float | None = None,
    *,
    seed: int | None = None,
    stop_event: asyncio.Event | None = None,
) -> AsyncIterator[GeneratedEvent]:
    """Convenience async generator for callers that do not need an object."""
    source = EventGenerator(rate=rate, seed=seed)
    async for event in source.events(stop_event):
        yield event


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    async def _main() -> None:
        source = EventGenerator(rate=4, seed=7)
        stop = asyncio.Event()
        async for item in source.events(stop):
            print(item)
            stop.set()

    asyncio.run(_main())
