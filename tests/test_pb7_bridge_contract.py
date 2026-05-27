"""Contract tests for PB7 bridge metadata access used by FastAPI routers."""

from __future__ import annotations

from types import SimpleNamespace

from api import pb7_bridge


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
