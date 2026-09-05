"""transport.py: dispatch/ack/outstanding/redispatch_expired.

Phase J2 is interface-only — every test here exercises the tracking logic
(timestamps, acks, timeout-driven redispatch) against a same-process stub
`deliver`, exactly the shape the module's own docstring says Phase J3
replaces with real HTTP without touching this contract.
"""

from __future__ import annotations

import pytest

from triage import transport
from triage.contracts import Event, EventType, Tier
from triage.transport import Transport


def _event(event_id: str, tier: Tier = Tier.P1) -> Event:
    return Event(
        event_id=event_id,
        dedup_key=f"dedup-{event_id}",
        partition_key="cust-1",
        type=EventType.INVENTORY if tier is Tier.P1 else EventType.PAYMENT,
        tier=tier,
    )


class _RecordingDeliverer:
    """A stub `deliver` that just remembers what it was asked to send —
    the same-process stand-in this phase's own build uses."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[Event]]] = []

    async def __call__(self, server: str, events: list[Event]) -> None:
        self.calls.append((server, list(events)))


class _FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.mark.asyncio
async def test_dispatch_calls_deliver_and_returns_a_result():
    deliverer = _RecordingDeliverer()
    t = Transport(deliver=deliverer, ack_timeout_ms=5000, now=_FakeClock())
    events = [_event("e1"), _event("e2")]

    result = await t.dispatch("server2", events)

    assert result.server == "server2"
    assert result.event_ids == ("e1", "e2")
    assert deliverer.calls == [("server2", events)]


@pytest.mark.asyncio
async def test_dispatch_requires_at_least_one_event():
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000)
    with pytest.raises(ValueError):
        await t.dispatch("server1", [])


@pytest.mark.asyncio
async def test_outstanding_reports_dispatched_but_unacked_events():
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000)
    e1, e2 = _event("e1"), _event("e2")

    assert t.outstanding("server2") == []
    await t.dispatch("server2", [e1, e2])
    assert {e.event_id for e in t.outstanding("server2")} == {"e1", "e2"}
    assert t.outstanding("server1") == []  # a different server sees nothing


@pytest.mark.asyncio
async def test_full_ack_clears_outstanding():
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000)
    result = await t.dispatch("server2", [_event("e1"), _event("e2")])

    await t.ack(result.dispatch_id, ["e1", "e2"])

    assert t.outstanding("server2") == []


@pytest.mark.asyncio
async def test_partial_ack_leaves_the_rest_outstanding():
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000)
    result = await t.dispatch("server2", [_event("e1"), _event("e2"), _event("e3")])

    await t.ack(result.dispatch_id, ["e1"])

    remaining = {e.event_id for e in t.outstanding("server2")}
    assert remaining == {"e2", "e3"}


@pytest.mark.asyncio
async def test_ack_on_unknown_dispatch_id_is_a_silent_no_op():
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000)
    await t.dispatch("server2", [_event("e1")])

    await t.ack("dispatch-does-not-exist", ["e1"])  # must not raise

    assert {e.event_id for e in t.outstanding("server2")} == {"e1"}


@pytest.mark.asyncio
async def test_ack_of_unknown_event_id_within_a_real_dispatch_is_ignored():
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000)
    result = await t.dispatch("server2", [_event("e1")])

    await t.ack(result.dispatch_id, ["not-e1"])

    assert {e.event_id for e in t.outstanding("server2")} == {"e1"}


@pytest.mark.asyncio
async def test_redispatch_expired_leaves_fresh_dispatches_alone():
    clock = _FakeClock()
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000, now=clock)
    await t.dispatch("server2", [_event("e1")])

    clock.advance(1.0)  # well under the 5s timeout
    redispatched = await t.redispatch_expired()

    assert redispatched == 0
    assert {e.event_id for e in t.outstanding("server2")} == {"e1"}


@pytest.mark.asyncio
async def test_redispatch_expired_resends_unacked_events_past_the_timeout():
    deliverer = _RecordingDeliverer()
    clock = _FakeClock()
    t = Transport(deliver=deliverer, ack_timeout_ms=5000, now=clock)
    await t.dispatch("server2", [_event("e1"), _event("e2")])

    clock.advance(5.001)
    redispatched = await t.redispatch_expired()

    assert redispatched == 2
    # Delivered twice total: once on the original dispatch, once on redispatch.
    assert len(deliverer.calls) == 2
    assert {e.event_id for e in t.outstanding("server2")} == {"e1", "e2"}


@pytest.mark.asyncio
async def test_redispatch_expired_only_resends_the_still_unacked_remainder():
    deliverer = _RecordingDeliverer()
    clock = _FakeClock()
    t = Transport(deliver=deliverer, ack_timeout_ms=5000, now=clock)
    result = await t.dispatch("server2", [_event("e1"), _event("e2")])
    await t.ack(result.dispatch_id, ["e1"])  # e2 alone is still outstanding

    clock.advance(5.001)
    redispatched = await t.redispatch_expired()

    assert redispatched == 1
    assert deliverer.calls[-1] == ("server2", [_event("e2")])


@pytest.mark.asyncio
async def test_a_late_ack_for_a_since_redispatched_batch_is_a_no_op():
    """An event's original dispatch_id must not be able to clear the NEW
    dispatch a timeout already superseded it with."""
    clock = _FakeClock()
    t = Transport(deliver=_RecordingDeliverer(), ack_timeout_ms=5000, now=clock)
    original = await t.dispatch("server2", [_event("e1")])

    clock.advance(5.001)
    await t.redispatch_expired()

    await t.ack(original.dispatch_id, ["e1"])  # the stale id — must be a no-op

    assert {e.event_id for e in t.outstanding("server2")} == {"e1"}


@pytest.mark.asyncio
async def test_ack_timeout_defaults_from_servers_config():
    t = Transport(deliver=_RecordingDeliverer())
    assert t._ack_timeout_ms == 5000  # config/servers.yaml's own transport.ack_timeout_ms


# --------------------------------------------------------------------------
# The ambient module-level default and configure()/reset_default()
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unconfigured_default_raises_loudly_instead_of_dropping_events():
    transport.reset_default()
    with pytest.raises(RuntimeError):
        await transport.dispatch("server2", [_event("e1")])


@pytest.mark.asyncio
async def test_configure_wires_the_ambient_default():
    deliverer = _RecordingDeliverer()
    transport.configure(deliverer, ack_timeout_ms=5000)
    try:
        result = await transport.dispatch("server2", [_event("e1")])
        await transport.ack(result.dispatch_id, ["e1"])
        assert transport.outstanding("server2") == []
        assert deliverer.calls == [("server2", [_event("e1")])]
    finally:
        transport.reset_default()
