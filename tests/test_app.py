"""app.py: the FastAPI wiring, in both modes.

Real mode is tested with the full generator -> classifier -> queue -> workers
pipeline actually running under the app's own lifespan, not mocked out --
this is the "one asyncio event loop" wiring the Stage B prompt asked for, and
a mocked engine would not prove it is actually wired together.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from triage import ledger, metrics
from triage.app import create_app
from triage.contracts import MetricsFrame


def setup_function() -> None:
    metrics.reset()
    ledger.reset()


def teardown_function() -> None:
    metrics.reset()
    ledger.reset()


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
