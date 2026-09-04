"""The event queue. Stage B: a single FIFO. Stage C replaces the inside of
this file with three tiered heaps.

Owner: Lane A.

The FIFO is deliberately dumb — no priority, no tiers, no batching, per the
Stage B scope. What has to survive the Stage C rewrite is the shape callers
see: an async ``put``/``get`` pair that instruments every crossing. worker.py
is written against that shape, not against ``asyncio.Queue`` directly, so
swapping the FIFO for the tiered structure later touches this file only.

put() and get() are the only two places an event legitimately enters or
leaves queued state, so they are exactly where metrics.observe_ingest and
metrics.observe_dequeue belong — one call site each, matching CLAUDE.md's
instrumentation-from-day-one rule from Stage A.
"""

from __future__ import annotations

import asyncio

from . import metrics
from .contracts import Event


class EventQueue:
    """Instrumented wrapper around one ``asyncio.Queue``."""

    def __init__(self, maxsize: int = 0) -> None:
        """``maxsize`` is 0 (unbounded) by default. Stage E's admission
        control is where backpressure belongs, not a queue that silently
        blocks producers."""
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)

    async def put(self, event: Event) -> None:
        """Admit one event. Counted as ingested the instant it is queued,
        not when a worker eventually picks it up."""
        metrics.observe_ingest(event)
        await self._queue.put(event)

    def put_nowait(self, event: Event) -> None:
        """Synchronous admit, for tests and for pre-loading a backlog."""
        metrics.observe_ingest(event)
        self._queue.put_nowait(event)

    async def get(self) -> Event:
        """Take the next event. Its queue wait is measured from here."""
        event = await self._queue.get()
        metrics.observe_dequeue(event)
        return event

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        """Wait until every put() has a matching task_done()."""
        await self._queue.join()

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()
