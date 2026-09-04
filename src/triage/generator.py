"""Deterministic, adjustable source for the PULSE event stream.

The generator owns only emission identity and partition identity.  Classification
is deliberately a separate stage: tier, value, cost, deadline and sequence are
assigned by :mod:`triage.classifier`.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import AsyncIterator

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

    def __init__(
        self,
        rate: float | None = None,
        *,
        config: Config | None = None,
        seed: int | None = None,
        customer_count: int = 500,
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

    @property
    def rate(self) -> float:
        return self._rate

    def set_rate(self, rate: float) -> None:
        """Change the source rate; the next async interval uses it."""
        rate = float(rate)
        if rate < 0:
            raise ValueError("rate must be non-negative")
        self._rate = rate

    def emit(self) -> GeneratedEvent:
        """Create one event using the configured mix and customer pool."""
        event_type = self._rng.choices(
            list(self.config.mix),
            weights=[self.config.mix[event_type] for event_type in self.config.mix],
            k=1,
        )[0]
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
        """Yield events until ``stop_event`` is set.

        The interval is measured around the emission itself so a slow consumer
        does not accidentally create a faster source.  A zero rate remains
        responsive to control and shutdown without busy-spinning.
        """
        while stop_event is None or not stop_event.is_set():
            if self._rate <= 0:
                await asyncio.sleep(0.05)
                continue
            started = time.monotonic()
            yield self.emit()
            remaining = (1.0 / self._rate) - (time.monotonic() - started)
            if remaining > 0:
                await asyncio.sleep(remaining)

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
