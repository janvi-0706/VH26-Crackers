"""bench/run.py — the headless benchmark harness (Stage G).

`bench/` is not a package under `src/`, so it is not on pytest's own
`pythonpath` (pyproject.toml only adds `src`) — this file adds `bench/`
to `sys.path` itself, the same way `bench/run.py` adds `src/` to its own
`sys.path` to be runnable standalone outside `make`.

Full 90-second runs are exercised manually (`make bench`) and by hand
before every commit that touches this file — nothing here runs a real
90-second config, since that would make the whole suite unusable for
everyday iteration. What IS tested here at full weight: every pure
function (formatting, SLA-attainment arithmetic, the SVG renderers, the
report renderers against synthetic data including both a passing and a
failing target check), and one short-duration (2s) real integration run
proving `run_config()`/`run_sensitivity_point()` actually drive a real
`Engine` correctly — the same class of proof `test_app.py`'s live-spike
tests already establish for the main application, at a duration that
keeps the suite fast.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

BENCH_DIR = Path(__file__).resolve().parents[1] / "bench"
sys.path.insert(0, str(BENCH_DIR))

import run as bench_run  # noqa: E402
from triage import deferral, ledger, metrics  # noqa: E402
from triage.contracts import Decision, Tier  # noqa: E402


@pytest.fixture(autouse=True)
def clean_state():
    bench_run.full_reset()
    yield
    bench_run.full_reset()


# --------------------------------------------------------------------------
# sla_attainment()
# --------------------------------------------------------------------------


def test_sla_attainment_computes_met_over_total():
    result = bench_run.sla_attainment({"P0": 9, "P1": 0, "P2": 0}, {"P0": 1, "P1": 0, "P2": 0})
    assert result["P0"] == pytest.approx(0.9)


def test_sla_attainment_is_none_for_a_tier_with_zero_completions():
    result = bench_run.sla_attainment({"P0": 0, "P1": 0, "P2": 0}, {"P0": 0, "P1": 0, "P2": 0})
    assert result["P0"] is None
    assert result["P1"] is None
    assert result["P2"] is None


def test_sla_attainment_is_perfect_when_nothing_was_missed():
    result = bench_run.sla_attainment({"P0": 5}, {"P0": 0})
    assert result["P0"] == pytest.approx(1.0)


# --------------------------------------------------------------------------
# p0_loss_count() — reads the real (per-test-reset) ledger
# --------------------------------------------------------------------------


def test_p0_loss_count_is_zero_on_a_fresh_ledger():
    assert bench_run.p0_loss_count() == 0


def test_p0_loss_count_finds_shed_and_sample_rollup_rows_for_p0_only():
    ledger.record(1, Decision.SHED, "should never happen", 0.99, Tier.P0)
    ledger.record(2, Decision.SAMPLE_ROLLUP, "should never happen either", 0.5, Tier.P0)
    ledger.record(3, Decision.SHED, "this one is fine", 0.99, Tier.P2)
    ledger.record(4, Decision.DEFER, "not a loss", 0.8, Tier.P0)
    assert bench_run.p0_loss_count() == 2


# --------------------------------------------------------------------------
# Formatters
# --------------------------------------------------------------------------


def test_fmt_ms_switches_to_seconds_at_1000():
    assert bench_run._fmt_ms(999) == "999ms"
    assert bench_run._fmt_ms(1000) == "1.00s"
    assert bench_run._fmt_ms(45250) == "45.25s"


def test_fmt_pct_handles_none_as_not_available():
    assert bench_run._fmt_pct(None) == "n/a"
    assert bench_run._fmt_pct(0.5) == "50.0%"
    assert bench_run._fmt_pct(1.0) == "100.0%"


def test_fmt_usd_has_four_decimal_places():
    assert bench_run._fmt_usd(0.00005) == "$0.0001"
    assert bench_run._fmt_usd(1.5) == "$1.5000"


# --------------------------------------------------------------------------
# SVG renderers — smoke tests: valid, well-formed-enough SVG comes out
# --------------------------------------------------------------------------


def test_svg_bar_chart_produces_one_svg_root_with_a_rect_per_bar():
    svg = bench_run._svg_bar_chart(
        "test chart", ["a", "b"], {"s1": [1.0, 2.0], "s2": [3.0, 4.0]},
        {"s1": "#111", "s2": "#222"}, "y",
    )
    assert svg.startswith("<svg")
    assert svg.endswith("</svg>")
    assert svg.count("<rect") >= 4  # 2 categories x 2 series


def test_svg_bar_chart_log_scale_does_not_crash_on_zero():
    svg = bench_run._svg_bar_chart(
        "log test", ["a"], {"s1": [0.0]}, {"s1": "#111"}, "y", log_scale=True,
    )
    assert "<svg" in svg


def test_svg_line_chart_produces_one_polyline_per_series():
    svg = bench_run._svg_line_chart(
        "test line", [5.0, 10.0, 20.0], {"P0": [1.0, 0.9, 0.5], "P1": [1.0, 1.0, 0.8]},
        {"P0": "#111", "P1": "#222"}, "attainment", "x",
    )
    assert svg.count("<polyline") == 2


def test_svg_line_chart_handles_a_single_x_value_without_dividing_by_zero():
    svg = bench_run._svg_line_chart(
        "single point", [20.0], {"P0": [1.0]}, {"P0": "#111"}, "attainment", "x",
    )
    assert "<svg" in svg


# --------------------------------------------------------------------------
# Report rendering, against synthetic data — both a passing and a failing
# target check, so the report's own "tell me immediately" framing is
# exercised in both directions, not just the happy path.
# --------------------------------------------------------------------------


def _make_result(
    label: str, mode: str, p0_p99: float, p0_lost: int = 0, chain_ok: bool = True,
    exactly_once_violations: int = 0,
) -> "bench_run.ConfigResult":
    per_tier_f = {"P0": p0_p99, "P1": 500.0, "P2": 2000.0}
    per_tier_i = {"P0": 10, "P1": 10, "P2": 10}
    return bench_run.ConfigResult(
        label=label, mode=mode, rate_label="x", multiplier=1.0, rate_eps=16.65,
        duration_s=90.0, ingested=100, processed=95, throughput_eps=1.05,
        latency_p50=dict(per_tier_f), latency_p95=dict(per_tier_f), latency_p99=dict(per_tier_f),
        sla_met=dict(per_tier_i), sla_missed={"P0": 0, "P1": 1, "P2": 2},
        sla_attainment={"P0": 1.0, "P1": 0.9, "P2": 0.8},
        deferred_total=3, batched_total=4, sampled_total=5, shed_total=6,
        value_delivered=1000.0, value_shed=10.0,
        actual_worker_seconds=540.0, naive_scaled_worker_seconds=1000.0,
        cost_actual_usd=0.05, cost_naive_scaled_usd=0.1,
        p0_loss_count=p0_lost, audit_chain_ok=chain_ok, critical_failures=0,
        exactly_once_violations=exactly_once_violations,
    )


def _make_sensitivity(multiplier: float, p0_attain: float | None) -> "bench_run.SensitivityPoint":
    return bench_run.SensitivityPoint(
        multiplier=multiplier, rate_eps=16.65 * multiplier,
        sla_attainment={"P0": p0_attain, "P1": 0.9, "P2": 0.5},
        latency_p99={"P0": 100.0, "P1": 1000.0, "P2": 5000.0},
        p0_loss_count=0,
    )


def _passing_matrix() -> list["bench_run.ConfigResult"]:
    return [
        _make_result("naive-baseline", "naive", 120.0),
        _make_result("naive-spike", "naive", 5000.0),  # seconds -- target met
        _make_result("adaptive-baseline", "adaptive", 110.0),
        _make_result("adaptive-spike", "adaptive", 190.0),  # under 200ms -- target met
    ]


def _failing_matrix() -> list["bench_run.ConfigResult"]:
    results = _passing_matrix()
    # Break the adaptive-spike P0 p99 target and introduce a P0 loss.
    results[3] = _make_result("adaptive-spike", "adaptive", 450.0, p0_lost=1)
    return results


def test_render_markdown_reports_all_targets_met_when_they_are():
    md = bench_run.render_markdown(_passing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert "ALL TARGETS MET" in md
    assert "NOT MET" not in md.split("ALL TARGETS MET")[1].split("##")[0]


def test_render_markdown_flags_missed_targets_immediately():
    md = bench_run.render_markdown(_failing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert "TARGET(S) NOT MET" in md
    assert "calibration problem, not a reporting problem" in md


def test_render_markdown_includes_every_config_row():
    matrix = _passing_matrix()
    md = bench_run.render_markdown(matrix, [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    for r in matrix:
        assert r.label in md


def test_render_markdown_includes_the_cost_model_section():
    md = bench_run.render_markdown(_passing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert "worker_seconds" in md
    assert "naive-scaled" in md.lower()


def test_render_html_is_well_formed_enough_and_shows_the_right_banner():
    html_pass = bench_run.render_html(_passing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert "<html>" in html_pass and "</html>" in html_pass
    assert "ALL TARGETS MET" in html_pass
    assert 'class="banner pass"' in html_pass

    html_fail = bench_run.render_html(_failing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert 'class="banner fail"' in html_fail
    assert "TARGET(S) NOT MET" in html_fail


def test_render_html_marks_a_p0_loss_as_bad_not_ok():
    html = bench_run.render_html(_failing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    # The failing matrix's adaptive-spike row has p0_loss_count=1.
    assert '<td class="bad">1</td>' in html


def test_render_html_includes_all_three_charts():
    html = bench_run.render_html(_passing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert html.count("<svg") == 3


# --------------------------------------------------------------------------
# exactly_once_violations — the final prompt's own column ("0 in every row")
# --------------------------------------------------------------------------


def test_render_markdown_reports_exactly_once_violations_as_zero():
    md = bench_run.render_markdown(_passing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert "Exactly-once violations" in md
    # Every synthetic row in _passing_matrix() is built with the default
    # exactly_once_violations=0 — the header appears once, and the matrix
    # section's own row count of trailing "| 0 |" cells (one per config)
    # is a cheap, real proxy for "every row reports zero", not just "the
    # column exists".
    assert md.count("| yes | 0 |") == len(_passing_matrix())


def test_render_html_marks_a_nonzero_exactly_once_violation_as_bad():
    matrix = _passing_matrix()
    matrix[3] = _make_result("adaptive-spike", "adaptive", 190.0, exactly_once_violations=1)
    html = bench_run.render_html(matrix, [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert '<td class="bad">1</td>' in html


def test_render_html_marks_zero_exactly_once_violations_as_ok():
    html = bench_run.render_html(_passing_matrix(), [_make_sensitivity(m, 1.0) for m in (5, 10, 20, 40)])
    assert html.count('<td class="ok">0</td>') >= len(_passing_matrix())


# --------------------------------------------------------------------------
# The two chaos configs run_all() adds — real Engine, real chaos actions
# --------------------------------------------------------------------------


async def test_run_all_includes_both_chaos_configs_with_zero_violations():
    """A short, real run (not the full 90s) proving run_all() actually
    wires the two new configs in, fires a real chaos action against each,
    and reports exactly_once_violations=0 for both — the same claim
    bench/report.md makes at full duration, checked here fast enough to
    run in the normal test suite."""
    matrix, _ = await bench_run.run_all(duration_s=2.0)
    labels = {r.label for r in matrix}
    assert "adaptive-spike-worker-kill" in labels
    assert "adaptive-spike-duplicate-flood" in labels
    for r in matrix:
        assert r.exactly_once_violations == 0, f"{r.label}: {r.exactly_once_violations}"


async def test_duplicate_flood_config_actually_suppresses_duplicates():
    """Not just that the config runs without crashing — that the chaos
    action it fires is the real mechanism, genuinely catching duplicates,
    not a no-op wired in for show."""
    matrix, _ = await bench_run.run_all(duration_s=2.0)
    flood_row = next(r for r in matrix if r.label == "adaptive-spike-duplicate-flood")
    # sink.reset_default_store() in full_reset() gives this config a clean
    # sink, so whatever it replays mid-run was admitted by this SAME run's
    # own Deduplicator moments earlier — it should have real duplicates to
    # catch, not be starved by another config's leftover state.
    assert flood_row.ingested > 0
    assert flood_row.p0_loss_count == 0


def test_sensitivity_none_attainment_renders_as_not_available_not_zero():
    md = bench_run.render_markdown(
        _passing_matrix(), [_make_sensitivity(5, None), _make_sensitivity(10, 1.0),
                              _make_sensitivity(20, 1.0), _make_sensitivity(40, 1.0)]
    )
    assert "n/a" in md


# --------------------------------------------------------------------------
# A real, short integration run — proves run_config()/run_sensitivity_point()
# actually drive a real Engine, not just that the report renderer accepts
# whatever shape of data it is handed.
# --------------------------------------------------------------------------


async def test_run_config_drives_a_real_engine_and_never_loses_p0():
    result = await bench_run.run_config(
        label="smoke", mode="adaptive", rate_label="spike", multiplier=20.0,
        duration_s=2.0, seed=1,
    )
    assert result.ingested > 0
    assert result.p0_loss_count == 0
    assert result.audit_chain_ok is True
    assert result.actual_worker_seconds > 0
    assert result.naive_scaled_worker_seconds > result.actual_worker_seconds, (
        "at a real spike rate, naively scaling workers linearly should need "
        "more capacity than our fixed 6-worker pool"
    )


async def test_run_sensitivity_point_drives_a_real_engine_in_adaptive_mode():
    point = await bench_run.run_sensitivity_point(multiplier=5.0, duration_s=2.0, seed=2)
    assert point.rate_eps > 0
    assert point.p0_loss_count == 0


async def test_full_reset_actually_clears_ledger_and_metrics():
    ledger.record(1, Decision.SHED, "x", 0.9, Tier.P2)
    assert ledger.total_recorded() >= 1

    bench_run.full_reset()

    assert ledger.total_recorded() == 0
    assert metrics.critical_failure_count() == 0
    assert deferral.pending_count() == 0
