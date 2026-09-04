from __future__ import annotations

import asyncio
from collections import Counter

from triage.classifier import Classifier
from triage.config import load_config
from triage.contracts import EventType, Tier
from triage.generator import EventGenerator
from triage.sink import SQLiteSink


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
