"""Enrich ingress envelopes with the frozen :class:`triage.contracts.Event`."""

from __future__ import annotations

import time
from typing import Any

from .config import Config, load_config
from .contracts import Event, EventType, SCHEMA_VERSION


def _get(raw: Any, name: str, default: Any = None) -> Any:
    if isinstance(raw, dict):
        return raw.get(name, default)
    return getattr(raw, name, default)


class Classifier:
    """Assign pipeline sequence and configuration-derived event metadata."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or load_config()
        self._seq = 0

    @property
    def sequence(self) -> int:
        return self._seq

    def reset_sequence(self) -> None:
        """Reset sequence numbering for a fresh simulation or test run."""
        self._seq = 0

    def classify(self, raw: Any, *, now: float | None = None) -> Event:
        event_type = EventType(_get(raw, "type"))
        spec = self.config.tiers[event_type]
        ingest_ts = float(_get(raw, "ingest_ts", None) or now or time.time())
        self._seq += 1
        dedup_key = str(_get(raw, "dedup_key"))
        return Event(
            event_id=str(_get(raw, "event_id")),
            dedup_key=dedup_key,
            seq=self._seq,
            partition_key=str(_get(raw, "partition_key")),
            idempotency_key=f"sink:{dedup_key}",
            type=event_type,
            tier=spec.tier,
            payload_size=int(_get(raw, "payload_size", 0)),
            value=spec.value,
            cost=spec.cost,
            ingest_ts=ingest_ts,
            deadline_ts=ingest_ts + spec.sla_seconds,
            schema_version=SCHEMA_VERSION,
        )


_default_classifier: Classifier | None = None


def classify(raw: Any, *, config: Config | None = None, now: float | None = None) -> Event:
    """Module-level convenience API used by the eventual ingress loop."""
    global _default_classifier
    if _default_classifier is None or (
        config is not None and _default_classifier.config is not config
    ):
        _default_classifier = Classifier(config)
    return _default_classifier.classify(raw, now=now)


def reset_sequence() -> None:
    """Reset the module-level classifier, primarily for isolated runs/tests."""
    global _default_classifier
    _default_classifier = None
