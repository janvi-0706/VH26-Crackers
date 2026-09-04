"""costmodel.py wired through a real running Engine — not mocked, for the
same reason test_app.py's own real-mode tests are not mocked: the demo
beat ("inject a heavier payload mix mid-run and show the estimate
re-adapting and rerouting") is a claim about the WHOLE wired pipeline, not
about costmodel.py in isolation (test_costmodel.py already covers that).

Progress is polled through `GET /control/costmodel` (routed through
TestClient's own portal onto the app's real event-loop thread), never by
calling `metrics.snapshot()` directly from this test's own thread. That
distinction matters: `metrics.py`'s module-level counters are documented
as safe under CLAUDE.md hard rule 1's single-thread assumption, but
`TestClient` runs the app on a background thread — polling `metrics.
snapshot()` straight from the test thread in a tight loop is a real,
pre-existing cross-thread race (confirmed by direct reproduction while
building this stage: identical polling produced a consistent, real
`ingested != processed + ... ` conservation mismatch purely from a torn
read, which vanished completely once every read was routed through the
HTTP layer instead). Not this stage's bug to fix — flagged to the user —
but this file does not rely on the unsafe pattern either.
"""

from __future__ import annotations

import time as _time

from fastapi.testclient import TestClient

from triage.app import create_app
from triage.contracts import EventType


def wait_until(predicate, *, timeout: float = 15.0, interval: float = 0.05) -> None:
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if predicate():
            return
        _time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for condition")


def costmodel_rows(client: TestClient) -> dict[str, dict]:
    resp = client.get("/control/costmodel")
    assert resp.status_code == 200
    return {row["event_type"]: row for row in resp.json()}


def total_samples(client: TestClient) -> int:
    return sum(row["samples"] for row in costmodel_rows(client).values())


def test_costmodel_endpoint_starts_at_the_prior_with_no_traffic():
    app = create_app(fake=False, seed=201)
    with TestClient(app) as client:
        rows = costmodel_rows(client)
        engine = app.state.engine
        for event_type, spec in engine.config.tiers.items():
            row = rows[event_type.value]
            assert row["learned"] == row["prior"] == spec.cost
            assert row["samples"] == 0


def test_costmodel_endpoint_is_404_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.get("/control/costmodel")
    assert resp.status_code == 404


def test_payload_multiplier_endpoint_rejects_non_positive_values():
    app = create_app(fake=False, seed=202)
    with TestClient(app) as client:
        resp = client.post("/control/payload-multiplier", json={"multiplier": 0})
    assert resp.status_code == 422


def test_payload_multiplier_is_fake_mode_guarded():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/payload-multiplier", json={"multiplier": 2.0})
    assert resp.status_code == 409


def test_the_demo_beat_heavier_payload_mix_makes_the_estimate_re_adapt():
    """The literal demo beat, end to end: real traffic at a real rate,
    real workers actually completing events and feeding costmodel.py real
    observed costs, a live payload-multiplier injection mid-run, and the
    /control/costmodel endpoint showing the learned value move away from
    the (unchanged) prior in response."""
    app = create_app(fake=False, seed=203)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_rate(250.0)

        # Let it run long enough at the NORMAL payload distribution for
        # every type to accumulate real confidence.
        wait_until(lambda: total_samples(client) > 400)

        payment_before = costmodel_rows(client)[EventType.PAYMENT.value]
        assert payment_before["confidence"] > 0.5, (
            "not enough real traffic accumulated before the injection to "
            "make the re-adaptation claim meaningful"
        )
        # Converged near the prior under the normal (unscaled) mix — not
        # exact (real per-event variance), but close.
        assert abs(payment_before["learned"] - payment_before["prior"]) < payment_before["prior"] * 0.15

        # The demo beat's own action.
        resp = client.post("/control/payload-multiplier", json={"multiplier": 3.0})
        assert resp.status_code == 200

        base = total_samples(client)
        wait_until(lambda: total_samples(client) > base + 200)

        payment_after = costmodel_rows(client)[EventType.PAYMENT.value]

        # Re-adapting: the learned estimate has moved meaningfully above
        # the (still unchanged) config prior.
        assert payment_after["prior"] == payment_before["prior"]
        assert payment_after["learned"] > payment_before["learned"]
        assert payment_after["learned"] > payment_after["prior"] * 1.2, (
            f"learned cost {payment_after['learned']:.3f} did not rise "
            f"meaningfully above the prior {payment_after['prior']:.3f} "
            "after a sustained 3x heavier payload mix"
        )

        # Rerouting: a P1/P2 event whose type just got structurally more
        # expensive should now be scored lower (density = value/cost) than
        # before, all else equal — checked directly against decision.score,
        # not inferred from a chart, using the SAME live cost_model the
        # engine's own queue is actually scoring with.
        from triage import decision
        from triage.contracts import Event, Tier

        now = _time.time()
        sample_event = Event(
            event_id="evt-test", dedup_key="dk-test", seq=999_999,
            partition_key="customer:0", idempotency_key="ik-test",
            type=EventType.INVENTORY, tier=Tier.P1, payload_size=200,
            value=engine.config.tiers[EventType.INVENTORY].value, cost=1.0,
            ingest_ts=now, deadline_ts=now + 5.0,
        )
        cost_now = engine.cost_model.estimate(EventType.INVENTORY, 200)
        prior_cost = engine.config.tiers[EventType.INVENTORY].cost
        score_with_learned_cost = decision.score(
            sample_event, now, engine.config.worker_capacity_ups, cost=cost_now
        )
        score_with_stale_prior = decision.score(
            sample_event, now, engine.config.worker_capacity_ups, cost=prior_cost
        )
        # A higher learned cost must never score HIGHER than the stale,
        # cheaper prior would have for the identical event — density is
        # value/cost, monotonically decreasing in cost.
        if cost_now > prior_cost:
            assert score_with_learned_cost <= score_with_stale_prior
