"""Regression tests for PB9 Optimize parity fixes: Sweep, Explorer, presets and source checks."""

import copy
from datetime import datetime, timezone
import json
import math
from pathlib import Path

from fastapi import HTTPException
import pytest

from api import optimize_v9
from api import pareto_explorer
from ParetoDataLoader import _flatten_bot_params
import pareto_preset_generator as generator
from sweep_cycles import PB9_CAGR_METRIC, build_sweep_plan, evaluate_sweep_cycles


def _sweep_config() -> dict:
    """Return one valid two-window sweep config with 181/182-day windows."""
    scenarios = [
        {"label": "train_01", "start_date": "2024-01-01", "end_date": "2024-06-30"},
        {"label": "train_02", "start_date": "2024-07-08", "end_date": "2025-01-06"},
    ]
    return {
        "backtest": {"suite_enabled": True, "scenarios": scenarios},
        "pbgui": {"scenario_template": {
            "template": "sweep_cycles", "template_version": 1,
            "parameters": {"window_days": 181, "stride_days": 189, "sweep_policy": {
                "starting_balance": 1000, "balance_multiplier": 2, "refill_cost": 0, "cooldown_days": 7}},
        }},
    }


def _ms(day: str) -> int:
    """Return one UTC midnight as epoch milliseconds."""
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _source(start: str, end: str, exchange: str = "binance") -> dict:
    """Return one native per-source metric metadata record."""
    return {exchange: {"start_timestamp_ms": _ms(start), "actual_end_timestamp_ms": _ms(end),
                       "intended_end_timestamp_ms": _ms(end), "year_days": 365.25}}


def _native_cagr(gain: float, start: str, end: str) -> float:
    """PB9's published forward definition: expm1(ln(E_end/E_start) / years)."""
    years = (_ms(end) - _ms(start)) / 86_400_000 / 365.25
    return math.expm1(math.log(gain) / years)


def _summary(result: dict) -> dict:
    return {key: pytest.approx(value) for key, value in result["summary"].items()}


def test_pb9_sweep_derives_window_gain_from_native_cagr_and_clock() -> None:
    """Window multipliers 2.5 and 0.8 give one 1500 sweep and one 200 refill."""
    plan = build_sweep_plan(_sweep_config())
    sources = {"train_01": _source("2024-01-01", "2024-06-30"), "train_02": _source("2024-07-08", "2025-01-06")}
    cagr = {"train_01": _native_cagr(2.5, "2024-01-01", "2024-06-30"),
            "train_02": _native_cagr(0.8, "2024-07-08", "2025-01-06")}
    result = evaluate_sweep_cycles(plan, {PB9_CAGR_METRIC: {"scenarios": cagr}}, sources)
    assert result["available"] is True
    assert result["gain_metric"] == PB9_CAGR_METRIC
    assert [window["gain"] for window in result["windows"]] == [pytest.approx(2.5), pytest.approx(0.8)]
    assert [window["action"] for window in result["windows"]] == ["sweep_reset", "refill_reset"]
    assert result["summary"] == _summary({"summary": {
        "sweep_net_cashflow": 1300.0, "sweep_total_swept": 1500.0, "sweep_external_capital": 200.0,
        "sweep_cycles_completed": 1.0, "sweep_refill_count": 1.0, "sweep_final_balance": 1000.0,
        "sweep_target_hit_rate": 0.5}})


def test_pb9_sweep_uses_shortened_actual_native_runtime_not_planned_window() -> None:
    """A run that ended after 90 of 181 days still grew 1.5x; plan dates would claim about 2.27x."""
    plan = build_sweep_plan(_sweep_config())
    sources = {"train_01": _source("2024-01-01", "2024-03-31"),
               "train_02": _source("2024-08-01", "2025-01-06")}  # later native start
    cagr = {"train_01": _native_cagr(1.5, "2024-01-01", "2024-03-31"),
            "train_02": _native_cagr(1.6, "2024-08-01", "2025-01-06")}
    result = evaluate_sweep_cycles(plan, {PB9_CAGR_METRIC: {"scenarios": cagr}}, sources)
    assert result["available"] is True
    assert [window["gain"] for window in result["windows"]] == [pytest.approx(1.5), pytest.approx(1.6)]
    # 1000 -> 1500 carries; 1500 * 1.6 = 2400 reaches the 2000 target and sweeps 1400.
    assert [window["action"] for window in result["windows"]] == ["carry", "sweep_reset"]
    assert result["summary"]["sweep_total_swept"] == pytest.approx(1400.0)
    assert result["summary"]["sweep_final_balance"] == pytest.approx(1000.0)


@pytest.mark.parametrize("sources, reason", [
    (None, "No native evaluation clock for train_01"),
    ({"train_01": {}}, "No native evaluation clock for train_01"),
    ({"train_01": {**_source("2024-01-01", "2024-06-30"), **_source("2024-01-01", "2024-06-30", "bybit")}},
     "averages 2 native sources"),
    ({"train_01": {"binance": {"start_timestamp_ms": 5, "actual_end_timestamp_ms": 5, "year_days": 365.25}}},
     "No valid native evaluation clock"),
    ({"train_01": {"binance": {"start_timestamp_ms": 0, "actual_end_timestamp_ms": 86_400_000}}},
     "No valid native evaluation clock"),
])
def test_pb9_sweep_is_unavailable_without_one_exact_native_clock(sources, reason) -> None:
    """Planned dates are no substitute for the native clock, and averaged sources cannot be inverted."""
    plan = build_sweep_plan(_sweep_config())
    result = evaluate_sweep_cycles(plan, {PB9_CAGR_METRIC: {"scenarios": {"train_01": 0.5, "train_02": 0.5}}}, sources)
    assert result["available"] is False
    assert reason in result["reason"]


@pytest.mark.parametrize("cagr, reason", [
    (-1.0, "does not determine the window gain"),
    (-1.5, "does not determine the window gain"),
    (-1.0 + 1e-12, "too close to -1"),
])
def test_pb9_sweep_rejects_cagr_values_that_cannot_be_inverted(cagr, reason) -> None:
    """-100% CAGR is a rounded floor, not proof of a zero multiplier."""
    plan = build_sweep_plan(_sweep_config())
    sources = {"train_01": _source("2024-01-01", "2024-06-30"), "train_02": _source("2024-07-08", "2025-01-06")}
    result = evaluate_sweep_cycles(plan, {PB9_CAGR_METRIC: {"scenarios": {"train_01": cagr, "train_02": 0.0}}}, sources)
    assert result["available"] is False
    assert reason in result["reason"]


def test_pb9_sweep_reports_numeric_overflow_without_raising() -> None:
    """A multiplier or balance beyond float range is reported, never returned as inf."""
    plan = build_sweep_plan(_sweep_config())
    decade = {"train_01": _source("2014-01-01", "2024-01-01"), "train_02": _source("2024-07-08", "2025-01-06")}
    result = evaluate_sweep_cycles(plan, {PB9_CAGR_METRIC: {"scenarios": {"train_01": 1e300, "train_02": 0.0}}}, decade)
    assert result == {"available": False, "reason": "Window gain for train_01 overflows"}
    balance = evaluate_sweep_cycles(plan, {"gain_strategy_eq": {"scenarios": {"train_01": 1e306, "train_02": 1.0}}})
    assert balance == {"available": False, "reason": "Sweep balances overflow at train_01"}
    json.dumps([result, balance], allow_nan=False)


def test_pb9_sweep_reports_target_balance_overflow_as_json_safe_unavailable() -> None:
    """1e308 * 2 exceeds float range; the result must stay strict-JSON serializable."""
    config = _sweep_config()
    config["pbgui"]["scenario_template"]["parameters"]["sweep_policy"].update(
        starting_balance=1e308, balance_multiplier=2)
    plan = build_sweep_plan(config)
    assert plan is not None and plan["policy"]["starting_balance"] == 1e308
    sources = {"train_01": _source("2024-01-01", "2024-06-30"), "train_02": _source("2024-07-08", "2025-01-06")}
    result = evaluate_sweep_cycles(plan, {PB9_CAGR_METRIC: {"scenarios": {"train_01": 0.0, "train_02": 0.0}}}, sources)
    assert result == {"available": False, "reason": "Sweep target balance overflows"}
    json.dumps(result, allow_nan=False)


def test_pb9_sweep_prefers_legacy_gain_values() -> None:
    """Existing PB8 gain values win over CAGR and need no native clock."""
    plan = build_sweep_plan(_sweep_config())
    both = {"gain_strategy_eq": {"scenarios": {"train_01": 1.1, "train_02": 1.2}},
            PB9_CAGR_METRIC: {"scenarios": {"train_01": 5.0, "train_02": 5.0}}}
    assert evaluate_sweep_cycles(plan, both)["gain_metric"] == "gain_strategy_eq"
    assert evaluate_sweep_cycles(plan, {}) == {"available": False, "reason": "No per-scenario gain metric for train_01"}


def test_pb9_compact_pareto_keeps_native_clocks_for_sweep() -> None:
    """The cached Pareto list projection carries suite_metrics.sources into Sweep."""
    cagr = {"train_01": _native_cagr(2.5, "2024-01-01", "2024-06-30"),
            "train_02": _native_cagr(0.8, "2024-07-08", "2025-01-06")}
    data = {"suite_metrics": {
        "definition_version": 2, "scenario_labels": ["train_01", "train_02"],
        "metrics": {PB9_CAGR_METRIC: {"stats": {"mean": 0.0}, "aggregated": 0.0, "scenarios": cagr}},
        "sources": {"train_01": _source("2024-01-01", "2024-06-30"), "train_02": _source("2024-07-08", "2025-01-06")}}}
    compact = optimize_v9.engine._compact_pareto_data(data, (PB9_CAGR_METRIC,))
    assert compact["suite_sources"]["train_01"]["binance"] == {
        "start_timestamp_ms": _ms("2024-01-01"), "actual_end_timestamp_ms": _ms("2024-06-30"), "year_days": 365.25}
    result = evaluate_sweep_cycles(build_sweep_plan(_sweep_config()), compact["suite_values"], compact["suite_sources"])
    assert result["available"] is True
    assert result["summary"]["sweep_net_cashflow"] == pytest.approx(1300.0)


def test_pb9_engine_requests_native_cagr_for_sweep_results() -> None:
    """Only PB9 adds CAGR because PB8 gain metrics are already projected."""
    from api.optimize_engine import OptimizeEngine
    assert optimize_v9.engine._SWEEP_GAIN_SOURCE_METRICS == (PB9_CAGR_METRIC,)
    assert OptimizeEngine()._SWEEP_GAIN_SOURCE_METRICS == ()


def test_loader_flattens_pb9_global_and_portfolio_params_like_bounds() -> None:
    """PB9 shared and account-wide genes use the dotted keys of PB9 bounds."""
    params = _flatten_bot_params({
        "global": {"risk": {"max_positions": 3}, "forager": {"score_weights": {"volume": 0.4}}},
        "portfolio": {"unstuck": {"memory_ema_span_minutes": 10080}, "strategy_kind": "trailing_martingale"},
        "long": {"enabled": True, "risk": {"total_wallet_exposure_limit_ratio": 1.2}},
        "unknown": {"ignored": 1},
    })
    assert params == {
        "global.risk.max_positions": 3,
        "global.forager.score_weights.volume": 0.4,
        "portfolio.unstuck.memory_ema_span_minutes": 10080,
        "portfolio_strategy_kind": "trailing_martingale",
        "long_enabled": True,
        "long.risk.total_wallet_exposure_limit_ratio": 1.2,
    }


@pytest.mark.parametrize("direction", generator.OPTIMIZE_PRESET_DIRECTIONS)
def test_pb9_preset_uses_native_metrics_nested_bounds_and_keeps_limits(monkeypatch, direction) -> None:
    """Every refinement direction writes PB9 metrics and leaves native limits untouched."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", lambda: {"currency_metrics": []})
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals",
                        lambda *_args: pytest.fail("PB9 must not use PB7/PB8 default goals"))
    limits = [{"metric": "backtest_completion_fraction", "penalize_if": "less_than", "value": 0.99}]
    full_config = {
        "config_version": "v9.0.0",
        "bot": {"global": {"risk": {"max_positions": 4, "total_wallet_exposure_limit_ratio": 1.5}}},
        "optimize": {
            "bounds": {"global": {"risk": {"max_positions": [1, 10, 1], "total_wallet_exposure_limit_ratio": [0.5, 3.0]}}},
            "scoring": [{"metric": "cagr_fraction_strategy_equity_ew", "goal": "max", "reducer": "mean"}],
            "limits": limits,
        },
    }
    payload = generator.build_optimize_preset(
        config_context={"optimize_version": "v9", "bot_params": {
            "global.risk.max_positions": 4, "global.risk.total_wallet_exposure_limit_ratio": 1.5},
            "suite_metrics": {"cagr_fraction_strategy_equity_ew": 0.4}, "scoring_goals": {}, "bounds": {}},
        full_config_data=full_config,
        params={"direction": direction, "bounds_window_pct": 20, "risk_adjust": 25},
    )
    preset = payload["preset_config"]
    metrics = [entry["metric"] for entry in preset["optimize"]["scoring"]]
    retired = ("_strategy_eq", "adg", "gain", "drawdown_worst", "_usd")
    assert metrics and not any(name.endswith(retired) or name.startswith(("adg", "gain", "mdg")) for name in metrics)
    assert all(entry["goal"] in {"min", "max"} for entry in preset["optimize"]["scoring"])
    assert preset["optimize"]["limits"] == limits
    risk = preset["optimize"]["bounds"]["global"]["risk"]
    assert set(risk) == {"max_positions", "total_wallet_exposure_limit_ratio"}
    assert risk["total_wallet_exposure_limit_ratio"][0] <= 1.5 <= risk["total_wallet_exposure_limit_ratio"][1]
    assert preset["pbgui"]["optimize_runtime"]["source"] == "__self__"


def test_explorer_registers_pb9_root_and_normalizes_generation(monkeypatch, tmp_path: Path) -> None:
    """PB9 results resolve as v9; unknown requested generations stay PB7."""
    import api.v9_instances as v9_instances
    monkeypatch.setattr(pareto_explorer, "pb7dir", lambda: str(tmp_path / "pb7"))
    monkeypatch.setattr(pareto_explorer, "pb8_runtime_status", lambda: {"pb8dir": str(tmp_path / "pb8")})
    monkeypatch.setattr(v9_instances, "_paths", lambda: {"pb9dir": str(tmp_path / "pb9")})
    result = tmp_path / "pb9" / "optimize_results" / "run"
    (result / "pareto").mkdir(parents=True)
    assert pareto_explorer._resolve_result_dir(str(result)) == result.resolve()
    assert pareto_explorer._result_optimize_version(result) == "v9"
    assert [pareto_explorer._normalize_optimize_version(value) for value in ("V9", "v8", "v10", "")] == ["v9", "v8", "v7", "v7"]


class _NativeLoader:
    """Minimal PB9 loader for config-detail serialization."""

    optimize_version = "v9"
    scoring_metrics = ["cagr_fraction_strategy_equity_ew"]
    scoring_goals = {"cagr_fraction_strategy_equity_ew": "max"}
    scenario_labels = []

    def __init__(self, path: Path) -> None:
        self.results_path = str(path)
        config = type("Config", (), {"config_index": 3, "is_pareto": True, "scenario_metrics": {},
                                      "suite_metrics": {"cagr_fraction_strategy_equity_ew": 0.2}})()
        self.configs = [config]

    def get_config_by_index(self, _index):
        return self.configs[0]

    def get_pareto_configs(self):
        return list(self.configs)

    def ensure_bot_params(self, _config):
        return None

    def ensure_details(self, _config):
        return None

    def compute_risk_profile_score(self, _config):
        return {"overall": 1.0}

    def get_full_config(self, _index):
        return {"config_version": "v9.0.0", "backtest": {"coins": {"long": ["HYPE"]}}, "bot": {},
                "metrics": {"stats": {}}, "optimizer_evaluation_contract": {}}

    def compute_trading_style(self, _config):
        return "Balanced"

    def compute_overall_robustness(self, _config):
        return 1.0


def test_explorer_detail_uses_native_pb9_result_reader_and_bounded_cache(monkeypatch, tmp_path: Path) -> None:
    """Handoff configs come from the native result reader, cached per clean source commit."""
    calls = []

    def convert(record):
        calls.append(copy.deepcopy(record))
        return {"config_version": record["config_version"], "backtest": {}, "bot": {}}

    runtime = {"commit": "a" * 40, "source_clean": True}
    monkeypatch.setattr(optimize_v9.engine.native, "result_config", convert)
    monkeypatch.setattr(optimize_v9.engine.native, "runtime", lambda **_kwargs: dict(runtime))
    monkeypatch.setattr(optimize_v9.engine, "_load_sweep_plan", lambda _path: None)
    monkeypatch.setattr(pareto_explorer, "_resolve_result_dir", lambda _path: None)
    pareto_explorer._NATIVE_RESULT_CONFIG_CACHE.clear()
    loader = _NativeLoader(tmp_path)
    first = pareto_explorer._serialize_config_detail(loader, 3)
    second = pareto_explorer._serialize_config_detail(loader, 3)
    assert first["full_config"] == second["full_config"] == {"config_version": "v9.0.0", "backtest": {}, "bot": {}}
    assert first["native_config_error"] == ""
    assert len(calls) == 1
    runtime["source_clean"] = False
    pareto_explorer._serialize_config_detail(loader, 3)
    pareto_explorer._serialize_config_detail(loader, 3)
    assert len(calls) == 3

    def reject(_record):
        raise HTTPException(422, "result is not a PB9 record")

    monkeypatch.setattr(optimize_v9.engine.native, "result_config", reject)
    failed = pareto_explorer._serialize_config_detail(loader, 3)
    assert failed["native_config_error"] == "result is not a PB9 record"


def test_native_result_listing_reads_strategy_from_portfolio(tmp_path: Path, monkeypatch) -> None:
    """The Results Strategy column uses the generation-specific strategy hook."""
    engine = optimize_v9.NativeOptimizeEngine()
    assert engine._listed_strategy_kind({"bot": {"portfolio": {"strategy_kind": "trailing_martingale"}}}) == "trailing_martingale"
    assert engine._listed_strategy_kind({"live": {"strategy_kind": "legacy"}}) == ""
    assert engine._listed_strategy_kind({"bot": {"portfolio": "malformed"}}) == ""
    assert engine._listed_strategy_kind(None) == ""


def test_dirty_pb9_source_keeps_results_readable_but_blocks_launches(tmp_path: Path, monkeypatch) -> None:
    """Local source changes only fail strict queue/launch provenance checks."""
    import pb9_optimize_adapter as adapter
    engine = optimize_v9.NativeOptimizeEngine()
    paths = {"pb9dir": str(tmp_path / "pb9"), "pb9venv": str(tmp_path / "env/bin/python")}
    monkeypatch.setattr(engine.native, "paths", lambda: paths)
    monkeypatch.setattr(adapter, "inspect_pb9_runtime", lambda *_args, **_kwargs: {**paths, "detected": True})

    def dirty(_repo):
        raise RuntimeError("PB9 source tree has local changes; commit provenance is not exact")

    monkeypatch.setattr(adapter, "source_identity", dirty)
    monkeypatch.setattr(adapter, "classify_passivbot_source",
                        lambda _repo: {"generation": "pb9", "commit": "b" * 40, "config_schema": "v9.0.0"})
    status = engine.native.runtime()
    assert status["ready"] is True
    assert status["commit"] == "b" * 40 and status["source_clean"] is False
    assert engine._results_root() == Path(paths["pb9dir"]) / "optimize_results"
    with pytest.raises(RuntimeError):
        engine.native.runtime(strict=True)
    with pytest.raises(HTTPException) as queued:
        engine._strict_runtime()
    assert queued.value.status_code == 409
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text("{}")
    with pytest.raises(HTTPException) as launch:
        engine._verify_queue_identity({"runtime": "pb9", "commit": "b" * 40}, status, snapshot)
    assert launch.value.status_code == 409 and "local changes" in launch.value.detail


@pytest.fixture
def optimize_migration(tmp_path, monkeypatch):
    """Use real PB8 Optimize path/lock rules with a synthetic official-converter result."""
    from api import backtest_v9, pb9_migration
    from api.optimize_v8 import engine as pb8_optimize
    monkeypatch.setattr(pb8_optimize, "PBGDIR", tmp_path)
    monkeypatch.setattr(optimize_v9.engine, "PBGDIR", tmp_path / "pbgui9")
    results = tmp_path / "pb8" / "optimize_results"
    monkeypatch.setattr(pb8_optimize, "_results_root", lambda: results)
    config = {"config_version": "v8.6.0", "pbgui": {"note": "keep", "optimize_runtime": {"mode": "fresh"}},
              "backtest": {"start_date": "2024-01-01"}, "optimize": {"scoring": ["adg"]}}
    saved = pb8_optimize._config_file("source_opt")
    saved.parent.mkdir(parents=True)
    saved.write_text(json.dumps(config))
    pareto = results / "run_1" / "pareto" / "abc.json"
    pareto.parent.mkdir(parents=True)
    pareto.write_text(json.dumps({**config, "metrics": {"stats": {}}, "suite_metrics": {}, "optimizer_evaluation_contract": {}}))
    native = {"config_version": "v9.0.0", "backtest": {}, "bot": {}, "optimize": {"scoring": []}}
    status = {"detected": True, "commit": "a" * 40, "config_schema": "v9.0.0", "package_version": "9.0.0"}
    calls = []

    def convert(bundle, *args, **kwargs):
        calls.append(copy.deepcopy(bundle))
        return {"source": status, "migration": {"status": "ready", "config": copy.deepcopy(native),
                                                "report": {"mode": "best_effort", "changes": [], "warnings": []}}}

    monkeypatch.setattr(pb9_migration, "migrate_pb8_config_to_pb9", convert)
    monkeypatch.setattr(backtest_v9, "_paths", lambda: {"pb9dir": str(tmp_path / "pb9"), "pb9venv": str(tmp_path / "env")})
    monkeypatch.setattr(backtest_v9, "_runtime", lambda cancel=None: (dict(status), {}))
    prepared = []
    monkeypatch.setattr(optimize_v9.engine.native, "prepare", lambda value, **_kwargs: prepared.append(value) or copy.deepcopy(value))
    pb9_migration.shutdown()
    backtest_v9._drafts.clear()
    yield saved, pareto, calls, prepared
    pb9_migration.shutdown()
    backtest_v9._drafts.clear()


def _migrate(name, body, owner="one"):
    """Run one Optimize migration operation in its request-owner envelope."""
    from api.v9_instances import _encode_payload
    from pb9_config import PB9CancelSignal
    return optimize_v9._operation("opt9_" + name, PB9CancelSignal(), _encode_payload({"owner": owner}, json.dumps(body).encode()))


@pytest.mark.parametrize("kind", ["config", "pareto"])
def test_pb8_optimize_migration_reviews_then_opens_unsaved_pb9_draft(optimize_migration, kind) -> None:
    """Saved PB8 Optimize/Pareto configs use the official converter and leave sources unchanged."""
    from api import backtest_v9
    saved, pareto, calls, prepared = optimize_migration
    source = "source_opt" if kind == "config" else str(pareto)
    raw = (saved if kind == "config" else pareto).read_bytes()
    review = _migrate("migrate_preview", {"source_type": kind, "source": source})
    assert review["status"] == "ready"
    assert any("optimize run settings" in warning for warning in review["report"]["warnings"])
    assert set(calls[0]["config"]) == {"config_version", "backtest", "optimize"}
    with pytest.raises(HTTPException) as other_owner:
        _migrate("migrate_accept", {"review_id": review["review_id"]}, owner="two")
    assert other_owner.value.status_code == 404
    accepted = _migrate("migrate_accept", {"review_id": review["review_id"]})
    draft = backtest_v9._drafts[accepted["draft_id"]]
    assert draft["owner"] == "one" and draft["config"]["config_version"] == "v9.0.0"
    assert draft["migration_report"] == review["report"]
    assert prepared and prepared[0]["pbgui"] == {"note": "keep"}
    assert (saved if kind == "config" else pareto).read_bytes() == raw
    assert not optimize_v9.engine._configs_dir().exists()


def test_pb8_optimize_migration_reviews_are_bound_to_their_target(optimize_migration) -> None:
    """A Backtest review cannot be adopted through Optimize and vice versa."""
    from api import pb9_migration
    review = _migrate("migrate_preview", {"source_type": "config", "source": "source_opt"})
    with pytest.raises(HTTPException) as wrong_target:
        pb9_migration.reviewed({"review_id": review["review_id"]}, "one", {}, target="backtest")
    assert wrong_target.value.status_code == 404
    with pytest.raises(HTTPException) as invalid:
        _migrate("migrate_preview", {"source_type": "result", "source": "x"})
    assert invalid.value.status_code == 422


def test_pb9_rejects_inherited_pb8_hsl_migration() -> None:
    """The PB8 HSL converter route is not applicable to native PB9 configs."""
    with pytest.raises(HTTPException) as rejected:
        optimize_v9.engine.preview_hsl_migration("name", {}, None)
    assert rejected.value.status_code == 422
