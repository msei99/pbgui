"""Tests for streamlit-free Pareto optimize preset generation."""

from __future__ import annotations

import copy

import pareto_preset_generator as generator


def _metric_sets() -> dict[str, list[str]]:
    """Return a compact PB7 metric set for preset generator unit tests."""
    return {
        "currency_metrics": [
            "adg_w",
            "adg_w_per_exposure_long",
            "adg_w_per_exposure_short",
            "drawdown_worst",
            "drawdown_worst_mean_1pct",
            "expected_shortfall_1pct",
            "gain",
            "mdg_w",
            "sharpe_ratio",
        ],
        "shared_metrics": [
            "adg_strategy_eq",
            "equity_choppiness_w",
            "equity_jerkiness_w",
            "exponential_fit_error_w",
            "loss_profit_ratio",
            "mdg_strategy_eq_w",
            "adg_strategy_eq_w",
            "drawdown_worst_strategy_eq",
            "drawdown_worst_mean_1pct_strategy_eq",
            "peak_recovery_days_strategy_eq",
            "position_held_days_max",
            "position_held_days_mean",
            "position_held_hours_max",
            "position_held_hours_mean",
            "positions_held_per_day",
            "sortino_ratio_strategy_eq",
            "total_wallet_exposure_mean",
        ],
    }


def _default_goals(metric_names: list[str]) -> dict[str, str]:
    """Return PB7-style default goals for the compact test metrics."""
    minimum = {
        "drawdown_worst_usd",
        "drawdown_worst_mean_1pct_usd",
        "drawdown_worst_strategy_eq",
        "drawdown_worst_mean_1pct_strategy_eq",
        "expected_shortfall_1pct_usd",
        "loss_profit_ratio",
        "peak_recovery_days_strategy_eq",
        "position_held_days_max",
        "position_held_days_mean",
        "position_held_hours_max",
        "position_held_hours_mean",
        "positions_held_per_day",
        "total_wallet_exposure_mean",
    }
    return {metric: ("min" if metric in minimum else "max") for metric in metric_names}


def _base_context() -> tuple[dict, dict]:
    """Build a minimal selected-config context and source config."""
    full_config = {
        "backtest": {"base_dir": "backtests/pbgui/source"},
        "bot": {},
        "optimize": {
            "bounds": {
                "long_entry_grid_spacing_pct": [0.001, 0.1, 0.001],
                "long_n_positions": [1, 5, 1],
                "long_total_wallet_exposure_limit": [0.0, 10.0, 0.1],
            },
            "scoring": ["adg_w_usd", "loss_profit_ratio", "sharpe_ratio_usd"],
            "limits": [],
        },
    }
    context = {
        "config_index": 42,
        "bounds": copy.deepcopy(full_config["optimize"]["bounds"]),
        "bot_params": {
            "long_entry_grid_spacing_pct": 0.02,
            "long_n_positions": 3,
            "long_total_wallet_exposure_limit": 4.0,
        },
        "suite_metrics": {
            "drawdown_worst_usd": 0.20,
            "drawdown_worst_mean_1pct_usd": 0.15,
            "drawdown_worst_strategy_eq": 0.22,
            "drawdown_worst_mean_1pct_strategy_eq": 0.16,
            "peak_recovery_days_strategy_eq": 35.0,
            "adg_strategy_eq_w": 0.0018,
            "mdg_strategy_eq_w": 0.0011,
            "adg_w_per_exposure_long_usd": 0.0014,
            "adg_w_per_exposure_short_usd": 0.0010,
            "total_wallet_exposure_mean": 0.82,
            "expected_shortfall_1pct_usd": 0.18,
            "sortino_ratio_strategy_eq": 1.35,
            "sharpe_ratio_usd": 1.2,
        },
        "optimize_settings": {},
    }
    return context, full_config


def test_build_optimize_preset_uses_direction_and_existing_bounds(monkeypatch) -> None:
    """Safer presets should adjust scoring, limits, and bounds without Streamlit."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 5,
            "direction": "Safer (lower drawdowns)",
            "risk_adjust": -25,
            "show_near_bounds": False,
        },
    )

    preset = payload["preset_config"]
    assert payload["window_pct"] == 5
    assert preset["optimize"]["scoring"][:2] == [
        {"metric": "drawdown_worst_strategy_eq", "goal": "min"},
        {"metric": "drawdown_worst_mean_1pct_strategy_eq", "goal": "min"},
    ]
    assert any(item.get("metric") == "drawdown_worst_usd" for item in preset["optimize"]["limits"])
    assert preset["optimize"]["bounds"]["long_entry_grid_spacing_pct"] != full_config["optimize"]["bounds"]["long_entry_grid_spacing_pct"]
    assert payload["bounds_preview_rows"]


def test_build_optimize_preset_reports_near_bounds_preview(monkeypatch) -> None:
    """Near-bound rows and expansion notes should be returned for the frontend preview."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": True,
            "expand_near_bounds": True,
            "near_bounds_expand_pct": 25,
        },
        near_bounds={
            "at_lower": {
                "long_total_wallet_exposure_limit": {"value": 0.0, "bound": 0.0},
            },
            "at_upper": {},
        },
    )

    assert payload["near_rows"] == [
        {"param": "long_total_wallet_exposure_limit", "edge": "lower", "value": 0.0, "bound": 0.0}
    ]
    assert payload["bounds_preview_rows"]


def test_balanced_preserves_modern_scoring_objects(monkeypatch) -> None:
    """Balanced presets should keep modern PB7 scoring objects and goals."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()
    full_config["optimize"]["scoring"] = [
        {"metric": "adg_w_usd", "goal": "max"},
        {"metric": "loss_profit_ratio", "goal": "min"},
    ]

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    assert payload["preset_config"]["optimize"]["scoring"] == [
        {"metric": "adg_w_usd", "goal": "max"},
        {"metric": "loss_profit_ratio", "goal": "min"},
    ]


def test_balanced_preserves_up_to_eight_scoring_objectives(monkeypatch) -> None:
    """Balanced presets should keep current PB7's eight-objective scoring shape."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()
    full_config["optimize"]["scoring"] = [
        {"metric": metric, "goal": _default_goals([metric])[metric]}
        for metric in generator.DEFAULT_PRESET_SCORING
    ]

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert len(scoring) == 8
    assert [entry["metric"] for entry in scoring] == generator.DEFAULT_PRESET_SCORING


def test_missing_run_scoring_uses_modern_pb7_defaults(monkeypatch) -> None:
    """Missing run scoring should fall back to current PB7 strategy-equity defaults."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()
    full_config["optimize"].pop("scoring", None)
    context["optimize_settings"] = {}

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert [entry["metric"] for entry in scoring] == generator.DEFAULT_PRESET_SCORING
    assert all("goal" in entry for entry in scoring)


def test_missing_pb7_default_goals_get_explicit_preset_fallbacks(monkeypatch) -> None:
    """Generated presets should stay valid when PB7 has no default goal for a metric."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", lambda _metrics: {})
    context, full_config = _base_context()
    full_config["optimize"].pop("scoring", None)
    context["optimize_settings"] = {}

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert all(isinstance(entry, dict) and entry.get("goal") in {"min", "max"} for entry in scoring)
    goals = {entry["metric"]: entry["goal"] for entry in scoring}
    assert goals["peak_recovery_days_strategy_eq"] == "min"
    assert goals["position_held_days_max"] == "min"


def test_lower_exposure_prefers_exposure_efficiency_metrics(monkeypatch) -> None:
    """Lower-exposure presets should use PB7 exposure-aware metrics when available."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Lower exposure (safer sizing)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert scoring[:3] == [
        {"metric": "adg_w_per_exposure_long_usd", "goal": "max"},
        {"metric": "adg_w_per_exposure_short_usd", "goal": "max"},
        {"metric": "total_wallet_exposure_mean", "goal": "min"},
    ]


def test_fewer_shorter_holds_prefers_duration_exposure_and_quality(monkeypatch) -> None:
    """Less-time-in-market presets should prefer PB7 duration and exposure metrics."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Fewer/shorter holds (less time in market)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert scoring[:5] == [
        {"metric": "position_held_days_max", "goal": "min"},
        {"metric": "position_held_days_mean", "goal": "min"},
        {"metric": "total_wallet_exposure_mean", "goal": "min"},
        {"metric": "mdg_strategy_eq_w", "goal": "max"},
        {"metric": "sortino_ratio_strategy_eq", "goal": "max"},
    ]


def test_only_adjust_near_bounds_limits_window_changes(monkeypatch) -> None:
    """Near-bound scope should keep non-near parameters unchanged."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "only_adjust_near_bounds": True,
            "bounds_window_pct": 10,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
        near_bounds={
            "at_lower": {},
            "at_upper": {
                "long_total_wallet_exposure_limit": {"value": 10.0, "bound": 10.0},
            },
        },
    )

    bounds = payload["preset_config"]["optimize"]["bounds"]
    assert payload["only_adjust_near_bounds"] is True
    assert payload["near_bounds_count"] == 1
    assert bounds["long_entry_grid_spacing_pct"] == full_config["optimize"]["bounds"]["long_entry_grid_spacing_pct"]
    assert bounds["long_total_wallet_exposure_limit"] != full_config["optimize"]["bounds"]["long_total_wallet_exposure_limit"]
