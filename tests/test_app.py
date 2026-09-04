"""app.py: the FastAPI wiring, in both modes.

Real mode is tested with the full generator -> classifier -> queue -> workers
pipeline actually running under the app's own lifespan, not mocked out --
this is the "one asyncio event loop" wiring the Stage B prompt asked for, and
a mocked engine would not prove it is actually wired together.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from triage import decision, deferral, ledger, metrics
from triage.app import SPIKE_EVENTS_PER_MINUTE, SPIKE_RATE_EPS, create_app
from triage.contracts import Decision, MetricsFrame, Tier


def _reset_live_weights() -> None:
    # decision.set_weights() mutates module-level globals (see decision.py's
    # own docstring on why: single event loop, no lock needed) -- a test
    # that drags a weight and doesn't put it back would otherwise leak into
    # every test that runs after it in the same process.
    decision.current_score_weights = decision.DEFAULT_SCORE_WEIGHTS
    decision.current_pressure_weights = decision.DEFAULT_PRESSURE_WEIGHTS


def setup_function() -> None:
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()
    _reset_live_weights()


def teardown_function() -> None:
    metrics.reset()
    ledger.reset()
    deferral.reset_default_store()
    _reset_live_weights()


# --------------------------------------------------------------------------
# /health
# --------------------------------------------------------------------------


def test_health_reports_real_mode():
    app = create_app(fake=False, seed=1)
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["mode"] == "real"
    assert body["uptime_s"] is not None


def test_health_reports_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["mode"] == "fake"


# --------------------------------------------------------------------------
# root: must not 500 before dashboard/dist exists (Stage B)
# --------------------------------------------------------------------------


def test_root_does_not_500_without_a_built_dashboard():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.get("/")
    assert resp.status_code == 200


# --------------------------------------------------------------------------
# /control/rate
# --------------------------------------------------------------------------


def test_control_rate_updates_the_running_generator():
    app = create_app(fake=False, seed=2)
    with TestClient(app) as client:
        resp = client.post("/control/rate", json={"rate": 42.0})
        assert resp.status_code == 200
        assert resp.json()["rate"] == 42.0
        assert app.state.engine.generator.rate == 42.0


def test_control_rate_rejects_negative_rate():
    app = create_app(fake=False, seed=3)
    with TestClient(app) as client:
        resp = client.post("/control/rate", json={"rate": -5.0})
    assert resp.status_code == 422


def test_control_rate_has_no_effect_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/rate", json={"rate": 10.0})
    assert resp.status_code == 409


# --------------------------------------------------------------------------
# /ws
# --------------------------------------------------------------------------


def test_websocket_streams_valid_frames_in_fake_mode():
    app = create_app(fake=True, seed=7)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            raw = ws.receive_text()
    frame = MetricsFrame.model_validate_json(raw)
    assert frame.schema_version == 1


def test_websocket_streams_the_real_snapshot_in_real_mode():
    app = create_app(fake=False, seed=8)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            raw = ws.receive_text()
    frame = MetricsFrame.model_validate_json(raw)
    assert frame.mode.value == "adaptive"


def test_real_engine_actually_moves_events_through_the_pipeline():
    """The wiring claim: generator -> classifier -> queue -> workers is one
    live loop, not four modules that merely import without error."""
    import time as _time

    app = create_app(fake=False, seed=9)
    with TestClient(app):
        client_engine = app.state.engine
        client_engine.set_rate(200.0)  # fast enough to see progress quickly
        deadline = _time.monotonic() + 3.0
        while _time.monotonic() < deadline and metrics.snapshot().ingested == 0:
            _time.sleep(0.05)
    frame = metrics.snapshot()
    assert frame.ingested > 0


# --------------------------------------------------------------------------
# /control/spike — an instant step function, not a ramp
# --------------------------------------------------------------------------


def test_spike_jumps_the_rate_to_the_spec_value_instantly():
    app = create_app(fake=False, seed=10)
    with TestClient(app) as client:
        resp = client.post("/control/spike")
        assert resp.status_code == 200
        body = resp.json()
        assert body["events_per_minute"] == SPIKE_EVENTS_PER_MINUTE
        assert body["rate"] == SPIKE_RATE_EPS
        # No ramp: the generator's rate is already the spike value the
        # instant the call returns, not eventually.
        assert app.state.engine.generator.rate == SPIKE_RATE_EPS


def test_spike_rate_is_twenty_thousand_per_minute():
    assert SPIKE_RATE_EPS == pytest.approx(20_000 / 60)


def test_spike_has_no_effect_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/spike")
    assert resp.status_code == 409


# --------------------------------------------------------------------------
# /control/mode
# --------------------------------------------------------------------------


def test_control_mode_switches_the_live_queue_and_the_reported_mode():
    app = create_app(fake=False, seed=11)
    with TestClient(app) as client:
        resp = client.post("/control/mode", json={"mode": "naive"})
        assert resp.status_code == 200
        assert resp.json()["mode"] == "naive"
        assert app.state.engine.queue.mode == "naive"
        assert metrics.get_mode().value == "naive"

        resp = client.post("/control/mode", json={"mode": "adaptive"})
        assert resp.status_code == 200
        assert app.state.engine.queue.mode == "adaptive"
        assert metrics.get_mode().value == "adaptive"


def test_control_mode_rejects_unknown_mode():
    app = create_app(fake=False, seed=12)
    with TestClient(app) as client:
        resp = client.post("/control/mode", json={"mode": "fifo"})
    assert resp.status_code == 422


def test_control_mode_has_no_effect_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/mode", json={"mode": "naive"})
    assert resp.status_code == 409


# --------------------------------------------------------------------------
# /control/weights — the dashboard slider endpoints (Stage D)
# --------------------------------------------------------------------------


def test_get_weights_reports_the_defaults_before_any_change():
    app = create_app(fake=False, seed=14)
    with TestClient(app) as client:
        resp = client.get("/control/weights")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"w1": 0.7, "w2": 0.3, "a": 0.35, "b": 0.35, "c": 0.2, "d": 0.1}


def test_post_weights_is_a_partial_update_that_renormalises_its_group():
    app = create_app(fake=False, seed=15)
    with TestClient(app) as client:
        resp = client.post("/control/weights", json={"a": 0.9})
    assert resp.status_code == 200
    body = resp.json()
    # a=0.9, b=0.35, c=0.2, d=0.1 renormalised: divide each by their sum (1.55)
    assert body["a"] == pytest.approx(0.9 / 1.55)
    assert body["b"] == pytest.approx(0.35 / 1.55)
    assert body["c"] == pytest.approx(0.2 / 1.55)
    assert body["d"] == pytest.approx(0.1 / 1.55)
    assert sum(body[k] for k in ("a", "b", "c", "d")) == pytest.approx(1.0)
    # the untouched score group (w1/w2) is not disturbed by a pressure update
    assert body["w1"] == 0.7
    assert body["w2"] == 0.3
    # and it actually took effect on the live weights the queue/pressure
    # code reads, not just on the response body
    assert decision.current_pressure_weights.a == pytest.approx(0.9 / 1.55)


def test_post_weights_actually_changes_what_the_running_queue_reads():
    app = create_app(fake=False, seed=16)
    with TestClient(app) as client:
        client.post("/control/weights", json={"w1": 0.1, "w2": 0.9})
    assert decision.current_score_weights.w1 == pytest.approx(0.1)
    assert decision.current_score_weights.w2 == pytest.approx(0.9)


def test_post_weights_rejects_a_negative_value():
    app = create_app(fake=False, seed=17)
    with TestClient(app) as client:
        resp = client.post("/control/weights", json={"a": -0.1})
    assert resp.status_code == 422


def test_post_weights_rejects_a_group_that_would_sum_to_zero():
    app = create_app(fake=False, seed=18)
    with TestClient(app) as client:
        resp = client.post(
            "/control/weights", json={"a": 0, "b": 0, "c": 0, "d": 0}
        )
    assert resp.status_code == 422


def test_control_weights_post_has_no_effect_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/weights", json={"a": 0.9})
    assert resp.status_code == 409


def test_control_weights_get_works_even_in_fake_mode():
    # Reading the live weights is harmless in either mode -- unlike every
    # POST /control/* endpoint, GET /control/weights has no fake-mode guard.
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.get("/control/weights")
    assert resp.status_code == 200


# --------------------------------------------------------------------------
# /control/reset
# --------------------------------------------------------------------------


def test_reset_restores_baseline_rate_and_clears_the_queue_and_metrics():
    app = create_app(fake=False, seed=13)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_rate(9999.0)
        # Push a synthetic backlog directly so reset has something to clear
        # without depending on generator timing.
        import time as _time

        from triage.contracts import Event, EventType, Tier

        backlog_event = Event(
            event_id="evt-reset-1", dedup_key="dk-1", seq=1,
            partition_key="customer:0", idempotency_key="ik-1",
            type=EventType.LOG, tier=Tier.P2, payload_size=1, value=1.0,
            cost=1.0, ingest_ts=_time.time(), deadline_ts=_time.time() + 60,
        )
        engine.queue.put_nowait(backlog_event)
        assert not engine.queue.empty()
        assert metrics.snapshot().ingested >= 1

        resp = client.post("/control/reset")
        assert resp.status_code == 200
        assert resp.json()["rate"] == engine.config.baseline_eps

    assert engine.generator.rate == engine.config.baseline_eps
    assert engine.queue.empty()
    assert metrics.snapshot().ingested == 0


def test_reset_does_not_touch_mode():
    app = create_app(fake=False, seed=14)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_mode("naive")
        client.post("/control/reset")
        assert engine.queue.mode == "naive"


def test_workers_keep_processing_after_a_reset_under_load():
    """Regression for a real bug found while verifying this prompt's
    acceptance criteria live: resetting while workers had events in flight
    used to kill every worker silently (see queue.py's clear() docstring).
    Drive real load, reset mid-flight, then prove the pipeline is still
    alive by observing `processed` actually increase afterward."""
    import time as _time

    app = create_app(fake=False, seed=18)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_rate(400.0)  # fast enough to guarantee workers are busy

        deadline = _time.monotonic() + 2.0
        while _time.monotonic() < deadline and metrics.snapshot().in_flight == 0:
            _time.sleep(0.02)
        assert metrics.snapshot().in_flight > 0, "test setup: workers must be busy"

        client.post("/control/reset")

        processed_at_reset = metrics.snapshot().processed
        deadline = _time.monotonic() + 2.0
        while (
            _time.monotonic() < deadline
            and metrics.snapshot().processed <= processed_at_reset
        ):
            _time.sleep(0.02)

    assert metrics.snapshot().processed > processed_at_reset, (
        "workers must still be alive and completing events after a reset"
    )


def test_reset_discards_stragglers_instead_of_letting_them_pollute_latency():
    """Found live while verifying this prompt's acceptance criteria: an
    event a worker had already dequeued before reset keeps its true,
    pre-reset ingest_ts. Left to finish normally, it reports its real (and
    after a heavy spike, enormous) latency into an otherwise-fresh window,
    where — especially for a lightly-loaded tier — it can dominate p50/p99
    for a very long time. `reset()` must restart the worker pool so no
    straggler survives to do that."""
    import time as _time

    from triage.contracts import Event, EventType, Tier

    app = create_app(fake=False, seed=19)
    with TestClient(app) as client:
        engine = app.state.engine

        # Put one worker in flight on a deliberately ancient event: its
        # true latency, if it were allowed to finish and report, would be
        # enormous (minutes), which is unmistakable in a percentile check.
        ancient = Event(
            event_id="evt-straggler", dedup_key="dk-straggler", seq=1,
            partition_key="customer:0", idempotency_key="ik-straggler",
            type=EventType.INVENTORY, tier=Tier.P1, payload_size=1,
            value=40.0, cost=2.0, ingest_ts=_time.time() - 120.0,
            deadline_ts=_time.time() + 60.0,
        )
        engine.queue.put_nowait(ancient)

        deadline = _time.monotonic() + 1.0
        while _time.monotonic() < deadline and metrics.snapshot().in_flight == 0:
            _time.sleep(0.01)
        assert metrics.snapshot().in_flight > 0, "test setup: worker must have taken it"

        client.post("/control/reset")

        # Give the (restarted) pool a moment; if the straggler survived the
        # reset it would complete within ~80ms of being taken (2.0u / 25u/s)
        # and report ~120s of latency.
        _time.sleep(0.3)

    assert metrics.snapshot().latency_p99["P1"] < 1000.0, (
        "a pre-reset straggler's latency leaked into the post-reset window"
    )


def test_reset_has_no_effect_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/reset")
    assert resp.status_code == 409


# --------------------------------------------------------------------------
# /control/inject
# --------------------------------------------------------------------------


def test_inject_drops_one_correctly_classified_event_into_the_stream():
    app = create_app(fake=False, seed=15)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.set_rate(0.0)  # isolate: only the injected event exists
        before = metrics.snapshot().ingested

        resp = client.post("/control/inject", json={"type": "payment"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["type"] == "payment"
        assert body["tier"] == "P0"
        # Economics come from config, not the caller: payment is worth 120,
        # costs 3.5u, per CLAUDE.md's tier table.
        assert body["value"] == 120
        assert body["cost"] == 3.5

        assert metrics.snapshot().ingested == before + 1


def test_inject_accepts_an_explicit_partition_key():
    app = create_app(fake=False, seed=16)
    with TestClient(app) as client:
        app.state.engine.set_rate(0.0)
        resp = client.post(
            "/control/inject", json={"type": "order", "partition_key": "customer:999"}
        )
        assert resp.status_code == 200


def test_inject_rejects_unknown_type():
    app = create_app(fake=False, seed=17)
    with TestClient(app) as client:
        resp = client.post("/control/inject", json={"type": "refund"})
    assert resp.status_code == 422


def test_inject_has_no_effect_in_fake_mode():
    app = create_app(fake=True)
    with TestClient(app) as client:
        resp = client.post("/control/inject", json={"type": "payment"})
    assert resp.status_code == 409


# --------------------------------------------------------------------------
# Stage D — the live decision wiring, exercised for real, not just
# synthetically in test_invariant.py / test_decision.py
# --------------------------------------------------------------------------


def test_live_pipeline_records_non_stream_decisions_under_a_real_spike():
    """Drives the real, calibrated 20x spike until pressure genuinely
    crosses into MICRO_BATCH/DEFER territory, then confirms the decision
    trail (metrics.observe_decision -> recent_decisions, and the ledger
    behind it) actually shows it. This is the live counterpart to
    test_invariant.py's synthetic sweep: real pressure, real events, same
    guarantee — and it doubles as proof the wiring in Engine._ingest is
    real, not inert."""
    import time as _time

    app = create_app(fake=False, seed=25)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.spike()  # the real, calibrated overload — not an arbitrary rate

        deadline = _time.monotonic() + 6.0
        seen_non_stream = False
        while _time.monotonic() < deadline and not seen_non_stream:
            frame = metrics.snapshot()
            seen_non_stream = any(
                d.decision is not Decision.STREAM_NOW for d in frame.recent_decisions
            )
            if not seen_non_stream:
                _time.sleep(0.05)

    assert seen_non_stream, "expected at least one non-STREAM_NOW decision under a real spike"
    assert ledger.total_recorded() > 0, "non-STREAM_NOW decisions must reach the ledger"


def test_live_pipeline_never_defers_p0_even_under_a_real_spike():
    """The other half of the same run: whatever else happens under real
    overload, no P0 event ever gets a non-STREAM_NOW decision — checked
    against the ledger rather than recent_decisions, because only
    non-STREAM_NOW decisions ever reach either one (STREAM_NOW is the
    common case and is deliberately never recorded, so recent_decisions
    can never hold a STREAM_NOW entry to inspect either way). Since the
    ledger only ever contains non-STREAM_NOW rows by construction, a P0 row
    appearing there at all — regardless of what it says — is exactly the
    leak this test exists to catch, proven against events that actually
    flowed through the real generator -> classifier -> decision -> queue
    path, not just constructed synthetically."""
    import time as _time

    from triage import ledger as ledger_module

    app = create_app(fake=False, seed=26)
    with TestClient(app) as client:
        engine = app.state.engine
        engine.spike()
        _time.sleep(4.0)  # let real pressure build and real decisions land

    p0_rows = [row for row in ledger_module.records() if row["tier"] == Tier.P0.value]
    assert p0_rows == [], f"P0 event(s) reached the ledger under a real spike: {p0_rows}"


def test_deferred_events_are_never_lost_across_a_real_spike_and_reset():
    """The literal acceptance line: after a 30s spike and a reset, the
    number of events ever deferred equals the number ever drained, and the
    backlog reaches exactly zero. Uses deltas from a captured baseline
    (not deferral's raw totals) because the default store is process-wide
    and shared across tests — see deferral.py's own module docstring on
    why it is ambient rather than per-Engine.

    30 real seconds of overload is what actually exercises this
    end-to-end: it has to build a genuine backlog (some of it inventory,
    whose 5s SLA all but guarantees slack goes negative before pressure
    ever falls — the exact case worker.py's already-deferred override
    exists for), survive a real reset, and then actually finish draining —
    which, at the drainer's deliberately rate-limited pace, takes its own
    real time afterwards. Not just be asserted to in principle.
    """
    import time as _time

    from triage import deferral as deferral_module

    app = create_app(fake=False, seed=27)
    with TestClient(app) as client:
        engine = app.state.engine
        baseline_deferred = deferral_module.pending_count()  # usually 0, robust either way
        baseline_total_deferred = deferral_module._default_store.total_deferred
        baseline_total_drained = deferral_module._default_store.total_drained

        engine.spike()
        _time.sleep(30.0)

        deferred_by_end_of_spike = deferral_module._default_store.total_deferred - baseline_total_deferred
        assert deferred_by_end_of_spike > 0, "test setup: the spike must actually defer something"

        client.post("/control/reset")  # rate back to baseline; deferral is untouched by this

        # A real 30s spike at ~333 events/sec, ~90% of it P1/P2 and pressure
        # sustained near 1.0 for most of it, parks thousands of events (this
        # backlog has been observed in the 3000-5000 range) — at the
        # drainer's deliberately rate-limited ~100 events/sec (see
        # deferral.py's DRAIN_BATCH_PER_TICK comment), draining ~5000 events
        # alone takes ~50s, plus another 15-20s for post-reset pressure to
        # first fall under DRAIN_PRESSURE_THRESHOLD at all. 150s gives that
        # real math genuine margin rather than being tuned to just clear it.
        deadline = _time.monotonic() + 150.0
        while (
            _time.monotonic() < deadline
            and deferral_module.pending_count() > baseline_deferred
        ):
            _time.sleep(0.25)

        # Pressure does not drop below DRAIN_PRESSURE_THRESHOLD the instant
        # /control/reset returns — the rate takes a moment to actually fall
        # and a couple more real events can still land with negative slack
        # (or still-high pressure) in that window and get deferred too.
        # Those are just as real and just as owed a drain as anything from
        # the spike itself, so "everything ever deferred" is read fresh
        # here, after the wait loop, not from the pre-reset snapshot above.
        total_ever_deferred = deferral_module._default_store.total_deferred - baseline_total_deferred

    store = deferral_module._default_store
    assert deferral_module.pending_count() == baseline_deferred, "backlog must reach zero"
    assert store.total_drained - baseline_total_drained == total_ever_deferred, (
        "everything ever deferred must have been drained — nothing lost"
    )
