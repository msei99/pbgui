"""Contract tests for PB7 bridge metadata and PB7 runtime preflights."""

from __future__ import annotations

from types import SimpleNamespace

from api import pb7_bridge
from pbgui_purefunc import pb7_suite_preflight_errors


def build_hsl_preflight_config() -> dict:
    """Build a minimal optimize config for HSL preflight tests."""
    return {
        "backtest": {},
        "bot": {
            "long": {
                "hsl_enabled": True,
                "hsl_red_threshold": 0.2,
            },
            "short": {
                "hsl_enabled": False,
                "hsl_red_threshold": 0.2,
            },
        },
        "optimize": {
            "bounds": {},
            "fixed_params": [],
            "fixed_runtime_overrides": {},
        },
    }


def test_optimize_metric_sets_come_from_pb7_metrics(monkeypatch) -> None:
    """Metric set helper should centralize PB7 config.metrics access."""
    metrics_module = SimpleNamespace(
        CURRENCY_METRICS={"gain", "adg_w"},
        SHARED_METRICS={"positions_held_per_day"},
        ANALYSIS_SHARED_KEYS={"total_wallet_exposure_mean"},
    )

    def fake_import(module_name: str):
        """Return the fake PB7 module requested by the bridge."""
        assert module_name == "config.metrics"
        return metrics_module

    monkeypatch.setattr(pb7_bridge, "_import_pb7_module", fake_import)

    payload = pb7_bridge.get_optimize_metric_sets()

    assert payload["currency_metrics"] == ["adg_w", "gain"]
    assert "positions_held_per_day" in payload["shared_metrics"]
    assert "total_wallet_exposure_mean" in payload["shared_metrics"]
    assert "backtest_completion_ratio" in payload["shared_metrics"]


def test_optimize_limits_meta_uses_metric_sets(monkeypatch) -> None:
    """Limits metadata should be buildable from PB7 metrics/limits/scoring modules."""
    modules = {
        "config.metrics": SimpleNamespace(
            CURRENCY_METRICS={"gain"},
            SHARED_METRICS={"loss_profit_ratio"},
            ANALYSIS_SHARED_KEYS={"total_wallet_exposure_mean"},
        ),
        "config.limits": SimpleNamespace(SUPPORTED_LIMIT_STATS={"min", "max"}),
        "config.scoring": SimpleNamespace(
            OBJECTIVE_GOALS=("min", "max"),
            default_objective_goal=lambda metric: "max" if metric != "loss_profit_ratio" else "min",
        ),
    }

    def fake_import(module_name: str):
        """Return a fake PB7 module by import name."""
        return modules[module_name]

    monkeypatch.setattr(pb7_bridge, "_import_pb7_module", fake_import)

    payload = pb7_bridge.get_optimize_limits_meta_payload()

    assert "gain_usd" in payload["all_valid_metrics"]
    assert "total_wallet_exposure_mean" in payload["all_valid_metrics"]
    assert payload["default_goal_map"]["gain_usd"] == "max"
    assert "max" in payload["stat_options"]


def test_optimize_scoring_default_goals_use_pb7_scoring(monkeypatch) -> None:
    """Scoring goal helper should centralize PB7 default goal lookups."""
    scoring_module = SimpleNamespace(default_objective_goal=lambda metric: "min" if "drawdown" in metric else "max")

    def fake_import(module_name: str):
        """Return the fake PB7 scoring module requested by the bridge."""
        assert module_name == "config.scoring"
        return scoring_module

    monkeypatch.setattr(pb7_bridge, "_import_pb7_module", fake_import)

    payload = pb7_bridge.get_optimize_scoring_default_goals(["gain_usd", "drawdown_worst_usd"])

    assert payload == {"gain_usd": "max", "drawdown_worst_usd": "min"}


def test_pb7_preflight_rejects_active_hsl_red_threshold_bound_at_zero() -> None:
    """Active HSL must not allow optimizer candidates with red_threshold=0."""
    cfg = build_hsl_preflight_config()
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    errors = pb7_suite_preflight_errors(cfg)

    assert "optimize.bounds.long_hsl_red_threshold lower bound must be > 0.0" in errors[0]


def test_pb7_preflight_allows_positive_active_hsl_red_threshold_bound() -> None:
    """Positive HSL red-threshold bounds are valid for active HSL optimization."""
    cfg = build_hsl_preflight_config()
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0.001, 1, 0.001]

    assert pb7_suite_preflight_errors(cfg) == []


def test_pb7_preflight_allows_zero_hsl_bound_when_hsl_disabled() -> None:
    """Disabled HSL may keep neutral red-threshold bounds at zero."""
    cfg = build_hsl_preflight_config()
    cfg["bot"]["long"]["hsl_enabled"] = False
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    assert pb7_suite_preflight_errors(cfg) == []


def test_pb7_preflight_uses_runtime_hsl_enabled_override() -> None:
    """A runtime override enabling HSL makes the red-threshold bound safety check active."""
    cfg = build_hsl_preflight_config()
    cfg["bot"]["long"]["hsl_enabled"] = False
    cfg["optimize"]["fixed_runtime_overrides"] = {"bot.long.hsl_enabled": True}
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    errors = pb7_suite_preflight_errors(cfg)

    assert "optimize.bounds.long_hsl_red_threshold lower bound must be > 0.0" in errors[0]


def test_pb7_preflight_allows_fixed_zero_bound_with_positive_active_hsl_config() -> None:
    """A fixed red-threshold bound is safe when PB7 will use the positive current config value."""
    cfg = build_hsl_preflight_config()
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]
    cfg["optimize"]["fixed_params"] = ["long_hsl_red_threshold"]

    assert pb7_suite_preflight_errors(cfg) == []


def test_pb7_preflight_rejects_nonpositive_active_hsl_base_threshold_even_if_fixed() -> None:
    """A fixed bound cannot rescue an active HSL config whose base threshold is zero."""
    cfg = build_hsl_preflight_config()
    cfg["bot"]["long"]["hsl_red_threshold"] = 0
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]
    cfg["optimize"]["fixed_params"] = ["long_hsl_red_threshold"]

    errors = pb7_suite_preflight_errors(cfg)

    assert "Long HSL red threshold must be > 0.0" in errors[0]


def test_pb7_preflight_keeps_hsl_errors_when_suite_includes_base_scenario() -> None:
    """Suite include-base configs still need optimize HSL bound validation."""
    cfg = build_hsl_preflight_config()
    cfg["backtest"]["suite"] = {
        "enabled": True,
        "include_base_scenario": True,
        "scenarios": [{"name": "base"}],
    }
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    errors = pb7_suite_preflight_errors(cfg)

    assert "optimize.bounds.long_hsl_red_threshold lower bound must be > 0.0" in errors[0]
