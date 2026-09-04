from __future__ import annotations

import asyncio
from collections import Counter

from triage import worker as _worker  # noqa: F401 - import side effect only
from triage.classifier import Classifier
from triage.config import load_config
from triage.contracts import EventType, Tier
from triage.generator import EventGenerator
from triage.sink import SQLiteSink

# worker.py raises the Windows timer resolution on import (see its own
# docstring). The rate-pacing test below needs that regardless of which
# other test modules pytest happens to have already imported this run.


def test_generator_mix_is_within_two_percentage_points() -> None:
    config = load_config()
    source = EventGenerator(config=config, seed=2026)
    events = [source.emit() for _ in range(10_000)]
    counts = Counter(event.type for event in events)

    for event_type, target in config.mix.items():
        assert abs(counts[event_type] / len(events) - target) <= 0.02
    assert len({event.partition_key for event in events}) <= 500
    assert len({event.partition_key for event in events}) > 450


def test_retry_has_new_emission_identity_but_same_business_identity() -> None:
    source = EventGenerator(seed=7)
    original = source.emit()
    retry = source.retry(original)

    assert retry.event_id != original.event_id
    assert retry.dedup_key == original.dedup_key
    assert retry.partition_key == original.partition_key


def test_emit_single_bypasses_the_mix_draw_but_shares_identity_machinery() -> None:
    """inject_event's identity half: a caller-chosen type, not a mix draw,
    but still a real event_id/dedup_key/payload-size assignment — otherwise
    an injected event would be structurally distinguishable from an organic
    one, which defeats the point of dropping it "into" the stream."""
    source = EventGenerator(seed=42)
    injected = source.emit_single(EventType.PAYMENT)
    assert injected.type is EventType.PAYMENT
    assert injected.event_id.startswith("evt-")
    assert injected.dedup_key.startswith("payment:")
    assert injected.partition_key.startswith("customer:")


def test_emit_single_accepts_an_explicit_partition_key() -> None:
    source = EventGenerator(seed=43)
    injected = source.emit_single(EventType.ORDER, partition_key="customer:7")
    assert injected.partition_key == "customer:7"
    assert injected.dedup_key == "order:customer:7:1"


def test_emit_single_advances_the_same_event_id_counter_as_emit() -> None:
    """Injected events must not collide with, or be distinguishable from,
    organically generated ones — they share one monotonic counter."""
    source = EventGenerator(seed=44)
    first = source.emit()
    injected = source.emit_single(EventType.LOG)
    second = source.emit()
    assert [first.event_id, injected.event_id, second.event_id] == [
        "evt-00000001", "evt-00000002", "evt-00000003",
    ]


def test_async_generator_sustains_the_spec_spike_rate() -> None:
    """Found live while verifying the P8 acceptance criteria: pacing with
    one asyncio.sleep() per emitted event cannot actually sustain 333
    eps — Windows' per-call sleep overhead (even after worker.py's timer
    fix) dominates a ~3ms target interval, and the old implementation
    measured only ~200 eps in practice, silently breaking the demo's own
    288 u/s spike calibration. events() now paces against a running
    schedule and catches up in a no-sleep burst rather than sleeping once
    per event, precisely so this holds at spike rate, not just baseline.
    """
    async def measure(target_rate: float, seconds: float) -> float:
        source = EventGenerator(rate=target_rate, seed=5)
        stop = asyncio.Event()

        async def stopper() -> None:
            await asyncio.sleep(seconds)
            stop.set()

        asyncio.ensure_future(stopper())
        n = 0
        started = asyncio.get_event_loop().time()
        async for _ in source.events(stop):
            n += 1
        elapsed = asyncio.get_event_loop().time() - started
        return n / elapsed

    spike_eps = 20_000 / 60  # the SPIKE button's own target, per app.py
    actual = asyncio.run(measure(spike_eps, seconds=2.0))
    error = abs(actual - spike_eps) / spike_eps
    assert error <= 0.10, (
        f"actual {actual:.1f} eps vs target {spike_eps:.1f} eps, "
        f"error {error:.1%} exceeds 10%"
    )


def test_async_generator_honors_stop_event() -> None:
    async def collect_one() -> list[object]:
        stop = asyncio.Event()
        source = EventGenerator(rate=10_000, seed=3)
        collected = []
        async for event in source.events(stop):
            collected.append(event)
            stop.set()
        return collected

    collected = asyncio.run(collect_one())
    assert len(collected) == 1


def test_classifier_sequence_is_monotonic_without_gaps() -> None:
    source = EventGenerator(config=load_config(), seed=12)
    classifier = Classifier(config=load_config())
    classified = [classifier.classify(source.emit()) for _ in range(10_000)]

    assert [event.seq for event in classified] == list(range(1, 10_001))
    assert all(event.idempotency_key == f"sink:{event.dedup_key}" for event in classified)
    assert all(event.deadline_ts > event.ingest_ts for event in classified)
    assert all(
        (event.tier is Tier.P0)
        == (event.type in {EventType.PAYMENT, EventType.ORDER})
        for event in classified
    )


def test_sink_round_trips_event_and_upserts_retry() -> None:
    source = EventGenerator(seed=19)
    classifier = Classifier(config=load_config())
    raw = source.emit()
    original = classifier.classify(raw)
    retry = classifier.classify(source.retry(raw))

    with SQLiteSink() as sink:
        assert sink.write(original)
        assert sink.read(original.idempotency_key) == original
        assert sink.write(retry)
        assert sink.count() == 1
        assert sink.attempts(original.idempotency_key) == 2
        assert sink.read(original.idempotency_key) == retry
