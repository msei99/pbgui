"""Tests for streamlit-free Pareto optimize preset generation."""

from __future__ import annotations

import copy

import pareto_preset_generator as generator


def _metric_sets() -> dict[str, list[str]]:
    """Return a compact PB7 metric set for preset generator unit tests."""
    return {
        "currency_metrics": [
            "adg_w",
            "drawdown_worst",
            "drawdown_worst_mean_1pct",
            "expected_shortfall_1pct",
            "gain",
            "mdg_w",
            "sharpe_ratio",
        ],
        "shared_metrics": [
            "equity_choppiness_w",
            "equity_jerkiness_w",
            "exponential_fit_error_w",
            "loss_profit_ratio",
            "position_held_hours_max",
            "position_held_hours_mean",
            "positions_held_per_day",
        ],
    }


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
            "expected_shortfall_1pct_usd": 0.18,
            "sharpe_ratio_usd": 1.2,
        },
        "optimize_settings": {},
    }
    return context, full_config


def test_build_optimize_preset_uses_direction_and_existing_bounds(monkeypatch) -> None:
    """Safer presets should adjust scoring, limits, and bounds without Streamlit."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_adjust": -5,
            "direction": "Safer (lower drawdowns)",
            "risk_adjust": -25,
            "show_near_bounds": False,
        },
    )

    preset = payload["preset_config"]
    assert preset["optimize"]["scoring"][:2] == ["drawdown_worst_usd", "drawdown_worst_mean_1pct_usd"]
    assert any(item.get("metric") == "drawdown_worst_usd" for item in preset["optimize"]["limits"])
    assert preset["optimize"]["bounds"]["long_entry_grid_spacing_pct"] != full_config["optimize"]["bounds"]["long_entry_grid_spacing_pct"]
    assert payload["bounds_preview_rows"]


def test_build_optimize_preset_reports_near_bounds_preview(monkeypatch) -> None:
    """Near-bound rows and expansion notes should be returned for the frontend preview."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_adjust": 0,
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
