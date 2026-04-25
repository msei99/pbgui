"""Shared PB7 bridge helpers for FastAPI routers.

This module centralizes PB7 src import bootstrapping and schema/config lookups
used by the FastAPI routers, so upstream PB7 changes only need one adapter.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pbgui_help
from pbgui_purefunc import pb7dir

_OPTIMIZE_METRIC_GROUP_ORDER = (
    "Returns & Growth",
    "Risk Metrics",
    "Ratios & Efficiency",
    "Position & Execution Metrics",
    "Equity Curve Quality",
    "Other",
)

_OPTIMIZE_METRIC_GROUP_DESCRIPTIONS = {
    "Returns & Growth": "Return and growth metrics such as gain, ADG/MDG, exposure-normalized variants, and strategy-PnL-rebased growth.",
    "Risk Metrics": "Drawdown, shortfall, divergence, trade-loss, and HSL risk metrics.",
    "Ratios & Efficiency": "Risk-adjusted and efficiency ratios such as Sharpe, Sortino, Omega, Calmar, Sterling, exposure and win-rate metrics.",
    "Position & Execution Metrics": "Holding-time, activity, recovery, high-exposure, hard-stop timeline, and completion metrics.",
    "Equity Curve Quality": "Equity smoothness and fit-quality metrics.",
    "Other": "Miscellaneous metrics.",
}

_LIMIT_ONLY_SHARED_METRICS = {"backtest_completion_ratio"}


def ensure_pb7_src_importable() -> None:
    src = str(Path(pb7dir()) / "src")
    if src not in sys.path:
        sys.path.insert(0, src)


def _import_pb7_module(module_name: str):
    ensure_pb7_src_importable()
    return importlib.import_module(module_name)


def get_template_config() -> dict[str, Any]:
    return _import_pb7_module("config.schema").get_template_config()


def get_bot_param_keys() -> list[str]:
    template = get_template_config()
    bot_long = template.get("bot", {}).get("long", {})
    return [key for key, value in sorted(bot_long.items()) if not isinstance(value, dict)]


def get_allowed_override_params() -> dict[str, Any]:
    return _import_pb7_module("config.overrides").get_allowed_modifications()


def get_hsl_signal_modes() -> list[str]:
    coerce_mod = _import_pb7_module("config.coerce")
    return [str(value) for value in getattr(coerce_mod, "HSL_SIGNAL_MODES", ())]


def get_optimize_backend_options() -> list[str]:
    backends_dir = Path(pb7dir()) / "src" / "optimization" / "backends"
    if not backends_dir.exists():
        return []
    options: list[str] = []
    for path in sorted(backends_dir.glob("*_backend.py")):
        name = path.stem.removesuffix("_backend").strip().lower()
        if name and name != "__init__":
            options.append(name)
    return options


def get_pymoo_algorithm_options() -> list[str]:
    coerce_mod = _import_pb7_module("config.coerce")
    return [str(value) for value in getattr(coerce_mod, "PYMOO_ALGORITHMS", ())]


def get_pymoo_ref_dir_method_options() -> list[str]:
    coerce_mod = _import_pb7_module("config.coerce")
    return [str(value) for value in getattr(coerce_mod, "PYMOO_REF_DIR_METHODS", ())]


def prepare_override_config(config: dict[str, Any], *, verbose: bool = False) -> dict[str, Any]:
    prepared_input = dict(config or {})
    if "live" not in prepared_input:
        prepared_input["live"] = {}
    prepare_config = _import_pb7_module("config.load").prepare_config
    strip_config_metadata = _import_pb7_module("config_utils").strip_config_metadata
    prepared = prepare_config(prepared_input, verbose=verbose)
    return strip_config_metadata(prepared)


def _metric_base_name(metric: str) -> str:
    metric_name = str(metric or "").strip()
    if metric_name.endswith(("_usd", "_btc")):
        return metric_name[:-4]
    return metric_name


def _group_optimize_metric(metric: str) -> str:
    base = _metric_base_name(metric)
    if not base:
        return "Other"

    if (
        base in {"gain", "gain_strategy_pnl_rebased"}
        or base.startswith(("adg", "mdg"))
        or base.startswith(("gain_per_exposure_", "adg_per_exposure_", "mdg_per_exposure_"))
    ):
        return "Returns & Growth"

    if (
        base.startswith(("drawdown_", "expected_shortfall_", "equity_balance_diff_", "trade_loss_"))
        or base in {
            "hard_stop_halt_to_restart_equity_loss_pct",
            "hard_stop_trigger_drawdown_mean",
            "hard_stop_panic_close_loss_sum",
            "hard_stop_panic_close_loss_max",
        }
    ):
        return "Risk Metrics"

    if base.startswith(
        (
            "sharpe_ratio",
            "sortino_ratio",
            "omega_ratio",
            "sterling_ratio",
            "calmar_ratio",
            "loss_profit_ratio",
            "paper_loss_ratio",
            "paper_loss_mean_ratio",
            "exposure_ratio",
            "exposure_mean_ratio",
            "win_rate",
        )
    ):
        return "Ratios & Efficiency"

    if base.startswith(
        (
            "positions_held_per_day",
            "position_held_hours_",
            "position_unchanged_hours_",
            "volume_pct_per_day_avg",
            "peak_recovery_hours_",
            "high_exposure_hours_",
            "hard_stop_triggers_per_year",
            "hard_stop_restarts_per_year",
            "hard_stop_time_in_",
            "hard_stop_duration_minutes_",
            "hard_stop_flatten_time_minutes_",
            "hard_stop_post_restart_retrigger_pct",
            "backtest_completion_ratio",
        )
    ):
        return "Position & Execution Metrics"

    if base.startswith(("equity_choppiness", "equity_jerkiness", "exponential_fit_error")):
        return "Equity Curve Quality"

    return "Other"


def _grouped_metric_options(base_metrics: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {group: [] for group in _OPTIMIZE_METRIC_GROUP_ORDER}
    for metric in sorted(set(base_metrics)):
        grouped.setdefault(_group_optimize_metric(metric), []).append(metric)
    return {group: metrics for group, metrics in grouped.items() if metrics}


def _metric_group_help_text(group: str, metrics: list[str]) -> str:
    if not metrics:
        return ""
    lines = [f"Available metrics ({group}) ({len(metrics)}):", ""]
    lines.extend(f"- `{metric}`" for metric in metrics)
    return "\n".join(lines)


def _metric_type_help_text(groups: list[str]) -> str:
    lines = [
        "Type filters the Metric list.",
        "all: show all available metrics",
        "",
        "Groups:",
    ]
    for group in groups:
        description = _OPTIMIZE_METRIC_GROUP_DESCRIPTIONS.get(group, "")
        lines.append(f"- {group}: {description}" if description else f"- {group}")
    return "\n".join(lines)


def get_optimize_limits_meta_payload() -> dict[str, Any]:
    limits_mod = _import_pb7_module("config.limits")
    metrics_mod = _import_pb7_module("config.metrics")
    scoring_mod = _import_pb7_module("config.scoring")

    currency_metrics = sorted(set(metrics_mod.CURRENCY_METRICS))
    shared_metrics = sorted(set(metrics_mod.SHARED_METRICS).union(_LIMIT_ONLY_SHARED_METRICS))
    all_base_metrics = sorted(set(currency_metrics).union(shared_metrics))
    grouped_metrics = _grouped_metric_options(all_base_metrics)
    groups = [group for group in _OPTIMIZE_METRIC_GROUP_ORDER if group in grouped_metrics]

    metrics_by_group = {"all": all_base_metrics}
    metric_help_by_group = {"all": _metric_group_help_text("all", all_base_metrics)}
    for group in groups:
        metrics_by_group[group] = grouped_metrics[group]
        metric_help_by_group[group] = _metric_group_help_text(group, grouped_metrics[group])

    all_valid_metrics = sorted(
        set(shared_metrics)
        .union(f"{metric}_usd" for metric in currency_metrics)
        .union(f"{metric}_btc" for metric in currency_metrics)
    )
    default_goal_map = {}
    for metric in all_valid_metrics:
        goal = scoring_mod.default_objective_goal(metric)
        if goal is not None:
            default_goal_map[metric] = goal

    return {
        "type_options": ["all", *groups],
        "type_help": _metric_type_help_text(groups),
        "metrics_by_group": metrics_by_group,
        "metric_help_by_group": metric_help_by_group,
        "currency_metrics": currency_metrics,
        "shared_metrics": shared_metrics,
        "all_valid_metrics": all_valid_metrics,
        "currency_options": ["usd", "btc"],
        "penalize_if_options": [
            "greater_than",
            "greater_than_or_equal",
            "less_than",
            "less_than_or_equal",
            "equal_to",
            "not_equal",
            "outside_range",
            "inside_range",
            "auto",
        ],
        "stat_options": [""] + sorted(limits_mod.SUPPORTED_LIMIT_STATS),
        "goal_options": list(scoring_mod.OBJECTIVE_GOALS),
        "default_goal_map": default_goal_map,
        "currency_help": pbgui_help.limit_currency,
        "penalize_help": pbgui_help.limits_penalize_if,
        "stat_help": pbgui_help.limits_stat,
        "goal_help": "PB7 stores optimize.scoring as explicit {metric, goal} objects. Known metrics prefill Passivbot's default min/max goal; metrics without a PB7 default should be checked explicitly.",
        "value_help": pbgui_help.limit_value,
        "range_low_help": pbgui_help.limit_range_low,
        "range_high_help": pbgui_help.limit_range_high,
        "add_button_help": pbgui_help.add_limit_button,
    }