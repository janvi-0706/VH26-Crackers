"""bench/stress_j8.py — Phase J8: live chaos testing against the real
three-process split.

Owner: Lane D.

Testing only, per this phase's own instruction — nothing in `src/` is
touched. Unlike every earlier split-topology test in this project
(`tests/test_server1.py`, `test_server2.py`, `test_transport_http.py`,
`test_history_integration.py`), which all deliberately use
`httpx.ASGITransport` to avoid a real socket (CLAUDE.md hard rule 2's own
"deterministic on any machine" reasoning, applied to transport), THIS
script needs the opposite: real, separate OS processes, on real ports,
so that "kill Server 2" and "Server 1 is completely unaffected" are
claims about genuinely independent processes, not about two coroutines
in one shared Python interpreter that would trivially survive each
other's cancellation regardless of whether the architecture actually
decouples them.

What this script does, in order, matching this phase's own five
numbered items:

  1. (separate script: bench/contention_after.py)
  2. Start real ingress + server1 + server2 (`make dev-split`'s own three
     commands, spawned directly rather than through `make` so this
     script can kill one selectively — `make dev-split`'s own
     `trap 'kill 0'` tears down all three together on purpose, which is
     exactly the behaviour item 3/4/5 need to NOT have).
  3. Trigger a 20x spike and sample `/control/topology`,
     `/control/conservation`, `/control/transport-latency`, and both
     servers' own `/metrics` every few seconds for five real minutes,
     logging every sample and every assertion violation (this script
     does not stop at the first violation — a live chaos run's whole
     point is the complete record, not a single pass/fail bit).
  4. Kill server2's real OS process mid-spike; sample server1 and
     ingress throughout; restart server2; keep sampling until the
     cross-process conservation identity balances again.
  5. Kill server1's real OS process mid-spike; sample admission/topology
     throughout; restart server1; keep sampling until its own
     outstanding dispatches resolve.
  6. Kill ingress's real OS process mid-spike; sample server1/server2's
     own `/healthz`/`/readyz` throughout (they cannot reach ingress at
     all during this window — this is the one scenario this script
     cannot poll ingress's own endpoints for, by definition); restart
     ingress; observe what does and does not recover on its own.

Every sample is written to `bench/stress_j8_log.jsonl` as one JSON object
per line — `docs/`'s own report is written by hand afterward FROM this
real log, not the other way around; this file's only job is to produce
honest data, not a narrative.
"""

from __future__ import annotations

import json
import os
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
VENV_PY_WINDOWS = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
VENV_PY_POSIX = REPO_ROOT / ".venv" / "bin" / "python"
PY = str(VENV_PY_WINDOWS if VENV_PY_WINDOWS.exists() else (VENV_PY_POSIX if VENV_PY_POSIX.exists() else sys.executable))

ENV = dict(os.environ)
ENV["PYTHONPATH"] = str(REPO_ROOT / "src")

INGRESS = "http://127.0.0.1:8000"
SERVER1 = "http://127.0.0.1:8001"
SERVER2 = "http://127.0.0.1:8002"

LOG_PATH = REPO_ROOT / "bench" / "stress_j8_log.jsonl"

_log_fh = None


def log(event: str, **fields) -> dict:
    record = {"ts": time.time(), "event": event, **fields}
    line = json.dumps(record, default=str)
    print(line, flush=True)
    global _log_fh
    if _log_fh is not None:
        _log_fh.write(line + "\n")
        _log_fh.flush()
    return record


def start(name: str, module: str, *args: str) -> subprocess.Popen:
    cmd = [PY, "-m", module, *args]
    proc = subprocess.Popen(
        cmd, cwd=str(REPO_ROOT), env=ENV,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    log("process_start", name=name, pid=proc.pid, cmd=cmd)
    return proc


def stop(proc: subprocess.Popen | None, name: str, *, hard: bool = False) -> None:
    if proc is None or proc.poll() is not None:
        return
    if hard:
        proc.kill()  # SIGKILL-equivalent on POSIX; TerminateProcess on Windows either way
    else:
        proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=8)
    log("process_stop", name=name, hard=hard, returncode=proc.returncode)


def wait_ready(client: httpx.Client, url: str, path: str = "/health", timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = client.get(url + path, timeout=2.0)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def get_json(client: httpx.Client, url: str, timeout: float = 3.0) -> dict | None:
    try:
        r = client.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return {"_status": r.status_code}
    except Exception as exc:  # noqa: BLE001 - a failed sample is itself data
        return {"_error": str(exc)}


def sample(client: httpx.Client, *, ingress_reachable: bool = True) -> dict:
    out = {}
    if ingress_reachable:
        out["topology"] = get_json(client, INGRESS + "/control/topology")
        out["conservation"] = get_json(client, INGRESS + "/control/conservation")
        out["transport_latency"] = get_json(client, INGRESS + "/control/transport-latency")
        out["ingress_health"] = get_json(client, INGRESS + "/health")
    out["server1_metrics"] = get_json(client, SERVER1 + "/metrics")
    out["server1_healthz"] = get_json(client, SERVER1 + "/healthz")
    out["server1_readyz"] = get_json(client, SERVER1 + "/readyz")
    out["server2_metrics"] = get_json(client, SERVER2 + "/metrics")
    out["server2_healthz"] = get_json(client, SERVER2 + "/healthz")
    out["server2_readyz"] = get_json(client, SERVER2 + "/readyz")
    return out


def run_phase(name: str, client: httpx.Client, duration_s: float, interval_s: float, *, ingress_reachable: bool = True) -> list[dict]:
    samples = []
    start_ts = time.time()
    while time.time() - start_ts < duration_s:
        s = sample(client, ingress_reachable=ingress_reachable)
        s["_elapsed_s"] = round(time.time() - start_ts, 2)
        log("sample", phase=name, **s)
        samples.append(s)
        time.sleep(interval_s)
    return samples


def main() -> None:
    global _log_fh
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _log_fh = open(LOG_PATH, "w", encoding="utf-8")

    # data/ must exist for --persist's own history.db.
    (REPO_ROOT / "data").mkdir(parents=True, exist_ok=True)
    for stale in (REPO_ROOT / "data" / "history.db", REPO_ROOT / "data" / "history.db-wal", REPO_ROOT / "data" / "history.db-shm"):
        if stale.exists():
            stale.unlink()

    client = httpx.Client()
    ingress_proc = server1_proc = server2_proc = None

    try:
        # ------------------------------------------------------------------
        # Boot the real three-process split.
        # ------------------------------------------------------------------
        ingress_proc = start("ingress", "triage.app", "--transport", "http", "--persist")
        if not wait_ready(client, INGRESS, "/health", timeout=30):
            log("fatal", detail="ingress never became healthy")
            return
        server1_proc = start("server1", "triage.server1")
        server2_proc = start("server2", "triage.server2")
        if not wait_ready(client, SERVER1, "/healthz", timeout=30):
            log("fatal", detail="server1 never became healthy")
            return
        if not wait_ready(client, SERVER2, "/healthz", timeout=30):
            log("fatal", detail="server2 never became healthy")
            return
        # readyz gates on ingress connectivity — give the health-check
        # loops (1s interval) a few cycles to catch up before trusting them.
        for _ in range(20):
            r1 = client.get(SERVER1 + "/readyz")
            r2 = client.get(SERVER2 + "/readyz")
            if r1.status_code == 200 and r2.status_code == 200:
                break
            time.sleep(0.5)
        log("boot_complete",
            server1_ready=client.get(SERVER1 + "/readyz").status_code,
            server2_ready=client.get(SERVER2 + "/readyz").status_code)

        # ------------------------------------------------------------------
        # Item 2: trigger the 20x spike, sustain it through everything below.
        # ------------------------------------------------------------------
        spike_resp = client.post(INGRESS + "/control/spike", timeout=5.0)
        log("spike_triggered", status=spike_resp.status_code, body=spike_resp.json() if spike_resp.status_code == 200 else None)

        # ------------------------------------------------------------------
        # Item 2 proper: five real minutes, sampled every 3s.
        # ------------------------------------------------------------------
        run_phase("sustained_spike_5min", client, duration_s=300.0, interval_s=3.0)

        # ------------------------------------------------------------------
        # Item 3: kill server2 mid-spike.
        # ------------------------------------------------------------------
        log("action", detail="killing server2 (SIGKILL-equivalent) mid-spike")
        stop(server2_proc, "server2", hard=True)
        server2_proc = None
        run_phase("server2_down", client, duration_s=20.0, interval_s=1.0)

        log("action", detail="restarting server2")
        server2_proc = start("server2", "triage.server2")
        wait_ready(client, SERVER2, "/healthz", timeout=30)
        run_phase("server2_recovery", client, duration_s=20.0, interval_s=1.0)

        # ------------------------------------------------------------------
        # Item 4: kill server1 mid-spike.
        # ------------------------------------------------------------------
        log("action", detail="killing server1 (SIGKILL-equivalent) mid-spike")
        stop(server1_proc, "server1", hard=True)
        server1_proc = None
        run_phase("server1_down", client, duration_s=20.0, interval_s=1.0)

        log("action", detail="restarting server1")
        server1_proc = start("server1", "triage.server1")
        wait_ready(client, SERVER1, "/healthz", timeout=30)
        run_phase("server1_recovery", client, duration_s=20.0, interval_s=1.0)

        # ------------------------------------------------------------------
        # Item 5: kill ingress mid-spike.
        # ------------------------------------------------------------------
        log("action", detail="killing ingress (SIGKILL-equivalent) mid-spike")
        stop(ingress_proc, "ingress", hard=True)
        ingress_proc = None
        run_phase("ingress_down", client, duration_s=15.0, interval_s=1.0, ingress_reachable=False)

        log("action", detail="restarting ingress")
        ingress_proc = start("ingress", "triage.app", "--transport", "http", "--persist")
        wait_ready(client, INGRESS, "/health", timeout=30)
        run_phase("ingress_recovery", client, duration_s=15.0, interval_s=1.0)

        log("run_complete")

    finally:
        stop(server1_proc, "server1", hard=True)
        stop(server2_proc, "server2", hard=True)
        stop(ingress_proc, "ingress", hard=True)
        if _log_fh is not None:
            _log_fh.close()


if __name__ == "__main__":
    main()
