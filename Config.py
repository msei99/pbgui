import streamlit as st
from pathlib import Path
import json
from pbgui_func import validateJSON, error_popup
from pbgui_purefunc import config_pretty_str, pb7_suite_preflight_errors
import pbgui_help
import traceback
import multiprocessing
import datetime
from Exchange import Exchange, V7
from PBCoinData import CoinData, normalize_symbol
from time import sleep
import math
from dataclasses import dataclass

# ============================================================================
# Metrics Definitions (Docs-aligned Registry)
# ============================================================================

# Group labels aligned with Passivbot docs:
# - https://github.com/enarjord/passivbot/blob/master/docs/optimizing.md (Performance Metrics)
# - https://github.com/enarjord/passivbot/blob/master/docs/metrics.md
METRIC_GROUP_ORDER = [
    "Returns & Growth",
    "Risk Metrics",
    "Ratios & Efficiency",
    "Position & Execution Metrics",
    "Equity Curve Quality",
    "Other",
]


# Short descriptions for metric groups (used as UI help/tooltip text).
METRIC_GROUP_DESCRIPTIONS: dict[str, str] = {
    "Returns & Growth": "Return/growth metrics (gain, ADG/MDG, exposure-normalized variants).",
    "Risk Metrics": "Risk and downside metrics (drawdowns, expected shortfall, equity/balance divergence).",
    "Ratios & Efficiency": "Risk-adjusted and efficiency ratios (Sharpe/Sortino/Omega/Calmar/Sterling, loss-profit).",
    "Position & Execution Metrics": "Trading activity and holding/execution characteristics (positions/day, hold times, volume, recovery).",
    "Equity Curve Quality": "Equity curve smoothness/fit quality (choppiness, jerkiness, exponential fit error).",
    "Other": "Miscellaneous metrics.",
}


@dataclass(frozen=True)
class MetricDef:
    group: str
    has_currency: bool
    weighted_variant: str | None = None
    aliases: tuple[str, ...] = ()
    description: str = ""


# Registry keys are the canonical (denomination-less) metric names used in configs.
# Weighted variants are represented via `weighted_variant` (so the UI can derive them
# without duplicating string logic).
METRIC_REGISTRY: dict[str, MetricDef] = {
    # Returns & Growth
    "gain": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        description="Terminal equity divided by starting equity.",
    ),
    "adg": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        weighted_variant="adg_w",
        description="Average daily gain (smoothed) and its recency-weighted variant.",
    ),
    "mdg": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        weighted_variant="mdg_w",
        description="Median daily gain and its recency-weighted variant.",
    ),
    "adg_pnl": MetricDef(
        group="Returns & Growth",
        has_currency=False,
        weighted_variant="adg_pnl_w",
        description="Daily realized PnL ratio (collateral-agnostic) and weighted variant.",
    ),
    "mdg_pnl": MetricDef(
        group="Returns & Growth",
        has_currency=False,
        weighted_variant="mdg_pnl_w",
        description="Median of daily realized PnL ratios (collateral-agnostic) and weighted variant.",
    ),
    "adg_per_exposure_long": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        weighted_variant="adg_w_per_exposure_long",
        description="ADG normalized by long exposure limit (plus weighted variant).",
    ),
    "adg_per_exposure_short": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        weighted_variant="adg_w_per_exposure_short",
        description="ADG normalized by short exposure limit (plus weighted variant).",
    ),
    "mdg_per_exposure_long": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        weighted_variant="mdg_w_per_exposure_long",
        description="MDG normalized by long exposure limit (plus weighted variant).",
    ),
    "mdg_per_exposure_short": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        weighted_variant="mdg_w_per_exposure_short",
        description="MDG normalized by short exposure limit (plus weighted variant).",
    ),
    "gain_per_exposure_long": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        description="Gain normalized by long exposure limit.",
    ),
    "gain_per_exposure_short": MetricDef(
        group="Returns & Growth",
        has_currency=True,
        description="Gain normalized by short exposure limit.",
    ),

    # Risk Metrics
    "drawdown_worst": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Maximum peak-to-trough drawdown.",
    ),
    "drawdown_worst_mean_1pct": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Mean of worst 1% daily drawdowns.",
    ),
    "expected_shortfall_1pct": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Average of worst 1% daily losses (CVaR).",
    ),
    "equity_balance_diff_neg_max": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Largest negative (equity-balance)/balance divergence.",
    ),
    "equity_balance_diff_neg_mean": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Average negative (equity-balance)/balance divergence.",
    ),
    "equity_balance_diff_pos_max": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Largest positive (equity-balance)/balance divergence.",
    ),
    "equity_balance_diff_pos_mean": MetricDef(
        group="Risk Metrics",
        has_currency=True,
        description="Average positive (equity-balance)/balance divergence.",
    ),

    # Ratios & Efficiency
    "sharpe_ratio": MetricDef(
        group="Ratios & Efficiency",
        has_currency=True,
        weighted_variant="sharpe_ratio_w",
        description="Return-to-volatility ratio and its recency-weighted variant.",
    ),
    "sortino_ratio": MetricDef(
        group="Ratios & Efficiency",
        has_currency=True,
        weighted_variant="sortino_ratio_w",
        description="Return-to-downside-volatility ratio and its recency-weighted variant.",
    ),
    "omega_ratio": MetricDef(
        group="Ratios & Efficiency",
        has_currency=True,
        weighted_variant="omega_ratio_w",
        description="Sum of positive returns divided by absolute sum of negative returns (plus weighted variant).",
    ),
    "sterling_ratio": MetricDef(
        group="Ratios & Efficiency",
        has_currency=True,
        weighted_variant="sterling_ratio_w",
        description="Return divided by average of worst 1% drawdowns (plus weighted variant).",
    ),
    "calmar_ratio": MetricDef(
        group="Ratios & Efficiency",
        has_currency=True,
        weighted_variant="calmar_ratio_w",
        description="Return divided by maximum drawdown (plus weighted variant).",
    ),
    "loss_profit_ratio": MetricDef(
        group="Ratios & Efficiency",
        has_currency=False,
        weighted_variant="loss_profit_ratio_w",
        description="Loss-to-profit efficiency ratio (plus weighted variant).",
    ),
    "sharpe_ratio_pnl": MetricDef(
        group="Ratios & Efficiency",
        has_currency=False,
        weighted_variant="sharpe_ratio_pnl_w",
        description="Sharpe ratio computed on realized daily PnL ratios (plus weighted variant).",
    ),
    "sortino_ratio_pnl": MetricDef(
        group="Ratios & Efficiency",
        has_currency=False,
        weighted_variant="sortino_ratio_pnl_w",
        description="Sortino ratio computed on realized daily PnL ratios (plus weighted variant).",
    ),

    # Position & Execution Metrics
    "positions_held_per_day": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        weighted_variant="positions_held_per_day_w",
        description="Average number of positions opened per day (plus weighted variant).",
    ),
    "position_held_hours_mean": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        description="Mean holding time (hours).",
    ),
    "position_held_hours_median": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        description="Median holding time (hours).",
    ),
    "position_held_hours_max": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        description="Maximum holding time (hours).",
    ),
    "position_unchanged_hours_max": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        description="Longest span without modifying an open position (hours).",
    ),
    "volume_pct_per_day_avg": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        weighted_variant="volume_pct_per_day_avg_w",
        description="Average daily traded volume as % of balance (plus weighted variant).",
    ),
    "peak_recovery_hours_equity": MetricDef(
        group="Position & Execution Metrics",
        has_currency=True,
        description="Longest time until equity makes a new peak (hours).",
    ),
    "peak_recovery_hours_pnl": MetricDef(
        group="Position & Execution Metrics",
        has_currency=False,
        description="Longest time until cumulative realized PnL makes a new peak (hours).",
    ),

    # Equity Curve Quality
    "equity_choppiness": MetricDef(
        group="Equity Curve Quality",
        has_currency=True,
        weighted_variant="equity_choppiness_w",
        description="Equity curve total variation (lower is smoother) and weighted variant.",
    ),
    "equity_jerkiness": MetricDef(
        group="Equity Curve Quality",
        has_currency=True,
        weighted_variant="equity_jerkiness_w",
        description="Mean absolute second derivative of equity (lower is smoother) and weighted variant.",
    ),
    "exponential_fit_error": MetricDef(
        group="Equity Curve Quality",
        has_currency=True,
        weighted_variant="exponential_fit_error_w",
        description="Log-linear equity fit error (lower is better) and weighted variant.",
    ),

    # Exposure Metrics
    "high_exposure_hours_mean_long": MetricDef(
        group="Exposure Metrics",
        has_currency=False,
        description="Mean number of hours with long position above high exposure threshold.",
    ),
    "high_exposure_hours_max_long": MetricDef(
        group="Exposure Metrics",
        has_currency=False,
        description="Maximum consecutive hours with long position above high exposure threshold.",
    ),
    "high_exposure_hours_mean_short": MetricDef(
        group="Exposure Metrics",
        has_currency=False,
        description="Mean number of hours with short position above high exposure threshold.",
    ),
    "high_exposure_hours_max_short": MetricDef(
        group="Exposure Metrics",
        has_currency=False,
        description="Maximum consecutive hours with short position above high exposure threshold.",
    ),
}


def _build_metric_lookup() -> dict[str, MetricDef]:
    lookup: dict[str, MetricDef] = {}
    for name, spec in METRIC_REGISTRY.items():
        lookup[name] = spec
        if spec.weighted_variant:
            lookup[spec.weighted_variant] = spec

        # Optional legacy aliases (PB7 canonicalize_metric_name supports usd_*/btc_* prefixes)
        aliases = list(spec.aliases)
        aliases.extend([f"usd_{name}", f"btc_{name}"])
        if spec.weighted_variant:
            aliases.extend([f"usd_{spec.weighted_variant}", f"btc_{spec.weighted_variant}"])
        for alias in aliases:
            lookup.setdefault(alias, spec)
    return lookup


_METRIC_LOOKUP = _build_metric_lookup()


def get_metric_def(metric: str) -> MetricDef | None:
    """Return metric definition for a base metric or its weighted/legacy alias."""
    if not isinstance(metric, str):
        return None
    return _METRIC_LOOKUP.get(metric)


def get_metric_group(metric: str) -> str | None:
    spec = get_metric_def(metric)
    return spec.group if spec else None


def get_metric_groups() -> list[str]:
    """Return docs-aligned metric groups in stable order (excluding 'Other' if unused)."""
    groups_present = {spec.group for spec in METRIC_REGISTRY.values()}
    ordered = [g for g in METRIC_GROUP_ORDER if g in groups_present]
    # If we ever have groups outside the known order, append them deterministically.
    extras = sorted(groups_present.difference(ordered))
    return ordered + extras


def get_metric_group_description(group: str) -> str:
    if not isinstance(group, str) or not group:
        return ""
    return METRIC_GROUP_DESCRIPTIONS.get(group, "")


def get_metric_description(metric: str) -> str:
    """Return description for a metric name (handles weighted variants and currency suffixes)."""
    if not isinstance(metric, str) or not metric:
        return ""

    # Allow passing full metric names like `gain_usd`/`gain_btc`.
    base = metric
    if base.endswith("_usd") or base.endswith("_btc"):
        base = base[:-4]

    spec = get_metric_def(base)
    return spec.description if spec and spec.description else ""


def get_metric_help_text(metric: str) -> str:
    """Help text for a metric selectbox: include group + description when available."""
    if not isinstance(metric, str) or not metric:
        return ""

    # Use canonicalized name to tolerate legacy prefixes when called from other contexts.
    canon = canonicalize_metric_name(metric)
    base = canon
    if base.endswith("_usd") or base.endswith("_btc"):
        base = base[:-4]

    spec = get_metric_def(base)
    if not spec:
        return ""
    desc = spec.description or ""
    return f"{spec.group}: {desc}" if desc else spec.group


def get_limits_type_help_text() -> str:
    """Help text for the Limits 'Type' selectbox."""
    lines: list[str] = [
        "Type filters the Metric list.",
        "all: show all metrics",
        "",
        "Groups:",
    ]
    for group in get_metric_groups():
        desc = get_metric_group_description(group)
        lines.append(f"- {group}: {desc}" if desc else f"- {group}")
    return "\n".join(lines)


def get_limits_metric_list_help_text(selected_type: str, *, include_weighted: bool = True) -> str:
    """Help text for the Limits 'Metric' selectbox showing available metrics for current Type.

    Format is `metric = description` with aligned `=`.
    """

    def _clean_desc(desc: str) -> str:
        # Keep it compact: no embedded newlines, no repeated weighted-variant explanations.
        if not desc:
            return ""
        desc = " ".join(str(desc).split())
        for phrase in (
            " and its recency-weighted variant.",
            " (recency-weighted variant).",
            " (plus weighted variant).",
            " plus weighted variant.",
            " and weighted variant.",
            " (weighted variant).",
        ):
            desc = desc.replace(phrase, "")
        desc = " ".join(desc.split()).strip()
        # Restore a trailing period if we removed the end.
        if desc and desc[-1] not in ".!?":
            desc += "."
        return desc

    def _describe_metric(name: str) -> str:
        spec = get_metric_def(name)
        if not spec or not spec.description:
            return ""
        # For both base + weighted metrics, show the same base description.
        return _clean_desc(spec.description)

    group = selected_type or "all"
    metrics = get_metrics_by_group(group, include_weighted=include_weighted)

    if group == "all":
        header_lines = [f"Available metrics (all) ({len(metrics)}):"]
    else:
        header_lines = [f"Available metrics ({group}) ({len(metrics)}):"]

    if not metrics:
        return "\n".join(header_lines)

    # Use a markdown table so the '=' column aligns visually even in proportional fonts.
    table_lines: list[str] = [
        "| Metric | | Description |",
        "|---|:--:|---|",
    ]
    for m in metrics:
        desc = _describe_metric(m)
        table_lines.append(f"| `{m}` | = | {desc} |")

    footer = "Legend: metrics containing `_w` are recency-weighted variants."
    return "\n".join(header_lines + ["", *table_lines, "", footer])


def get_metrics_by_group(group: str, *, include_weighted: bool = True) -> list[str]:
    """Return canonical metric names belonging to a group.

    If include_weighted is True, include the weighted variants as separate selectable metrics.
    """
    if not group:
        return []
    if group == "all":
        return get_all_metrics_list()
    out: list[str] = []
    for name, spec in METRIC_REGISTRY.items():
        if spec.group != group:
            continue
        out.append(name)
        if include_weighted and spec.weighted_variant:
            out.append(spec.weighted_variant)
    return sorted(set(out))


# Derived sets used throughout PBGui (kept for backward compatibility)
CURRENCY_METRICS: set[str] = set()
SHARED_METRICS: set[str] = set()
for name, spec in METRIC_REGISTRY.items():
    target = CURRENCY_METRICS if spec.has_currency else SHARED_METRICS
    target.add(name)
    if spec.weighted_variant:
        target.add(spec.weighted_variant)

def get_all_metrics_with_currency():
    """Get all valid metrics including currency suffixes (for limits)."""
    all_metrics = SHARED_METRICS.copy()
    for m in CURRENCY_METRICS:
        all_metrics.add(f"{m}_usd")
        all_metrics.add(f"{m}_btc")
    return all_metrics

def get_all_metrics_list():
    """Get sorted list of base metrics for UI selection (includes weighted variants)."""
    return sorted(list(SHARED_METRICS) + list(CURRENCY_METRICS))

def get_aggregate_metrics():
    """Get list of metrics commonly used for suite aggregation (with currency suffixes)."""
    metrics = []
    # Add currency metrics with both suffixes
    for m in ["adg", "adg_w", "mdg", "mdg_w", "drawdown_worst", "drawdown_worst_mean_1pct", "gain"]:
        if m in CURRENCY_METRICS:
            metrics.extend([f"{m}_usd", f"{m}_btc"])
    # Add shared metrics that don't need suffixes
    for m in ["sharpe_ratio", "sharpe_ratio_w", "sortino_ratio", "sortino_ratio_w", 
              "loss_profit_ratio", "loss_profit_ratio_w", "positions_held_per_day",
              "position_held_hours_max", "position_held_hours_mean", "position_held_hours_median",
              "position_unchanged_hours_max", "peak_recovery_hours_pnl"]:
        if m in SHARED_METRICS:
            metrics.append(m)
    # Add ratio metrics with currency suffixes
    for m in ["calmar_ratio", "calmar_ratio_w", "omega_ratio", "omega_ratio_w", 
              "sterling_ratio", "sterling_ratio_w", "sharpe_ratio", "sharpe_ratio_w",
              "sortino_ratio", "sortino_ratio_w"]:
        if m in CURRENCY_METRICS:
            metrics.extend([f"{m}_usd", f"{m}_btc"])
    # Add additional useful metrics
    for m in ["equity_balance_diff_neg_max", "equity_balance_diff_neg_mean",
              "equity_balance_diff_pos_max", "equity_balance_diff_pos_mean",
              "expected_shortfall_1pct", "peak_recovery_hours_equity"]:
        if m in CURRENCY_METRICS:
            metrics.extend([f"{m}_usd", f"{m}_btc"])
        elif m in SHARED_METRICS:
            metrics.append(m)
    return sorted(list(set(metrics)))

def is_currency_metric(metric_base):
    """Check if a metric requires currency suffix (_usd or _btc)."""
    return metric_base in CURRENCY_METRICS


def canonicalize_metric_name(metric: str) -> str:
    """Canonicalize metric names like PB7 `canonicalize_metric_name`.

    - Keep explicit `_usd`/`_btc` suffixes.
    - Convert legacy `usd_foo`/`btc_foo` prefixes.
    - Default bare currency metrics to `_usd`.
    """
    if not isinstance(metric, str):
        return metric
    if metric.endswith("_usd") or metric.endswith("_btc"):
        return metric

    for prefix, suffix in (("usd_", "usd"), ("btc_", "btc")):
        if metric.startswith(prefix):
            core = metric[len(prefix):]
            if core in SHARED_METRICS:
                return core
            return f"{core}_{suffix}"

    if metric in SHARED_METRICS:
        return metric

    if metric in CURRENCY_METRICS:
        return f"{metric}_usd"

    return metric

# ============================================================================
# Bot Parameter Overrides
# ============================================================================

# Bot parameters that can be overridden in coin_overrides and scenario overrides
# Source: pb7/src/config_utils.py get_allowed_modifications()
ALLOWED_OVERRIDES = [
    "close_grid_markup_end",
    "close_grid_markup_start",
    "close_grid_qty_pct",
    "close_trailing_grid_ratio",
    "close_trailing_qty_pct",
    "close_trailing_retracement_pct",
    "close_trailing_threshold_pct",
    "ema_span_0",
    "ema_span_1",
    "entry_grid_double_down_factor",
    "entry_grid_spacing_pct",
    "entry_grid_spacing_we_weight",
    "entry_grid_spacing_volatility_weight",
    "entry_volatility_ema_span_hours",
    "entry_initial_ema_dist",
    "entry_initial_qty_pct",
    "entry_trailing_double_down_factor",
    "entry_trailing_grid_ratio",
    "entry_trailing_retracement_pct",
    "entry_trailing_retracement_we_weight",
    "entry_trailing_retracement_volatility_weight",
    "entry_trailing_threshold_pct",
    "entry_trailing_threshold_we_weight",
    "entry_trailing_threshold_volatility_weight",
    "risk_we_excess_allowance_pct",
    "risk_wel_enforcer_threshold",
    "unstuck_close_pct",
    "unstuck_ema_dist",
    "unstuck_threshold",
    "wallet_exposure_limit",  # Note: pbgui uses total_wallet_exposure_limit internally
]

class Config:
    def __init__(self, file_name = None, config = None):
        self._config_file = file_name
        self._long_we = 1.0
        self._short_we = 1.0
        self._long_enabled = True
        self._short_enabled = False
        self._type = None
        self._preview_grid = False
        self._config_v7 = ConfigV7()
        self._config_v7.bot.long.n_positions = 1.0
        self._config_v7.bot.short.n_positions = 1.0
        if config:
            self.config = config
        else:
            self._config = None

    @property
    def type(self): return self._type

    @property
    def config_file(self): return self._config_file

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file
        
    @property
    def config(self): return self._config

    @property
    def config_v7(self):
        if self._config:
            # Check if config is a recursive grid config
            config = json.loads(self._config)
            if "long" in config:
                if not "ddown_factor" in config["long"]:
                    return None
            # long settings
            self._config_v7.bot.long.close_grid_markup_start = json.loads(self._config)["long"]["min_markup"] + json.loads(self._config)["long"]["markup_range"]
            self._config_v7.bot.long.close_grid_markup_end = json.loads(self._config)["long"]["min_markup"]
            self._config_v7.bot.long.close_grid_qty_pct = 1.0 / float(json.loads(self._config)["long"]["n_close_orders"])
            self._config_v7.bot.long.close_trailing_grid_ratio = 0
            self._config_v7.bot.long.close_trailing_qty_pct = 1
            self._config_v7.bot.long.close_trailing_retracement_pct = 0
            self._config_v7.bot.long.close_trailing_threshold_pct = 0
            self._config_v7.bot.long.ema_span_0 = json.loads(self._config)["long"]["ema_span_0"]
            self._config_v7.bot.long.ema_span_1 = json.loads(self._config)["long"]["ema_span_1"]
            self._config_v7.bot.long.entry_grid_double_down_factor = json.loads(self._config)["long"]["ddown_factor"]
            self._config_v7.bot.long.entry_grid_spacing_pct = json.loads(self._config)["long"]["rentry_pprice_dist"]
            self._config_v7.bot.long.entry_grid_spacing_weight = json.loads(self._config)["long"]["rentry_pprice_dist_wallet_exposure_weighting"]
            self._config_v7.bot.long.entry_initial_ema_dist = json.loads(self._config)["long"]["initial_eprice_ema_dist"]
            self._config_v7.bot.long.entry_initial_qty_pct = json.loads(self._config)["long"]["initial_qty_pct"]
            self._config_v7.bot.long.entry_trailing_grid_ratio = 0
            self._config_v7.bot.long.entry_trailing_retracement_pct = 0
            self._config_v7.bot.long.entry_trailing_threshold_pct = 0
            self._config_v7.bot.long.entry_trailing_double_down_factor = 0
            # self._config_v7.bot.long.total_wallet_exposure_limit = json.loads(self._config)["long"]["wallet_exposure_limit"]
            try:
                self._config_v7.bot.long.unstuck_close_pct = json.loads(self._config)["long"]["auto_unstuck_qty_pct"]
            except:
                self._config_v7.bot.long.unstuck_close_pct = 0.025
            self._config_v7.bot.long.unstuck_ema_dist = json.loads(self._config)["long"]["auto_unstuck_ema_dist"]
            # short settings
            self._config_v7.bot.short.close_grid_markup_start = json.loads(self._config)["short"]["min_markup"] + json.loads(self._config)["short"]["markup_range"]
            self._config_v7.bot.short.close_grid_markup_end = json.loads(self._config)["short"]["min_markup"]
            self._config_v7.bot.short.close_grid_qty_pct = 1.0 / float(json.loads(self._config)["short"]["n_close_orders"])
            self._config_v7.bot.short.close_trailing_grid_ratio = 0
            self._config_v7.bot.short.close_trailing_qty_pct = 1
            self._config_v7.bot.short.close_trailing_retracement_pct = 0
            self._config_v7.bot.short.close_trailing_threshold_pct = 0
            self._config_v7.bot.short.ema_span_0 = json.loads(self._config)["short"]["ema_span_0"]
            self._config_v7.bot.short.ema_span_1 = json.loads(self._config)["short"]["ema_span_1"]
            self._config_v7.bot.short.entry_grid_double_down_factor = json.loads(self._config)["short"]["ddown_factor"]
            self._config_v7.bot.short.entry_grid_spacing_pct = json.loads(self._config)["short"]["rentry_pprice_dist"]
            self._config_v7.bot.short.entry_grid_spacing_weight = json.loads(self._config)["short"]["rentry_pprice_dist_wallet_exposure_weighting"]
            self._config_v7.bot.short.entry_initial_ema_dist = json.loads(self._config)["short"]["initial_eprice_ema_dist"]
            self._config_v7.bot.short.entry_initial_qty_pct = json.loads(self._config)["short"]["initial_qty_pct"]
            self._config_v7.bot.short.entry_trailing_grid_ratio = 0
            self._config_v7.bot.short.entry_trailing_retracement_pct = 0
            self._config_v7.bot.short.entry_trailing_threshold_pct = 0
            # self._config_v7.bot.short.total_wallet_exposure_limit = json.loads(self._config)["short"]["wallet_exposure_limit"]
            try:
                self._config_v7.bot.short.unstuck_close_pct = json.loads(self._config)["short"]["auto_unstuck_qty_pct"]
            except:
                self._config_v7.bot.short.unstuck_close_pct = 0.025
            self._config_v7.bot.short.unstuck_ema_dist = json.loads(self._config)["short"]["auto_unstuck_ema_dist"]
            return json.dumps(self._config_v7.config, indent=4)
        return None

    @config.setter
    def config(self, new_config):
        if new_config != "None":
            if validateJSON(new_config):
                self._config = new_config
                self.update_config()
                if "error_config" in st.session_state:
                    del st.session_state.error_config
            else:
                st.session_state.error_config = "Config is invalid"

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file

    @property
    def long_we(self): return self._long_we

    @long_we.setter
    def long_we(self, new_long_we):
        self._long_we = round(new_long_we,2)
        if self._config:
            t = json.loads(self._config)
            t["long"]["wallet_exposure_limit"] = self._long_we
            self._config = config_pretty_str(t)
    
    @property
    def long_enabled(self): return self._long_enabled

    @long_enabled.setter
    def long_enabled(self, new_long_enabled):
        self._long_enabled = new_long_enabled
        if self._config:
            t = json.loads(self._config)
            t["long"]["enabled"] = self._long_enabled
            self._config = config_pretty_str(t)
            self._config_v7.bot.long.total_wallet_exposure_limit = self.long_we
            if self.long_enabled:
                self._config_v7.bot.long.n_positions = 1.0
            else:
                self._config_v7.bot.long.n_positions = 0.0

    @property
    def short_enabled(self): return self._short_enabled

    @short_enabled.setter
    def short_enabled(self, new_short_enabled):
        self._short_enabled = new_short_enabled
        if self._config:
            t = json.loads(self._config)
            t["short"]["enabled"] = self._short_enabled
            self._config = config_pretty_str(t)
            self._config_v7.bot.short.total_wallet_exposure_limit = self.short_we
            if self.short_enabled:
                self._config_v7.bot.short.n_positions = 1.0
            else:
                self._config_v7.bot.short.n_positions = 0.0

    @property
    def short_we(self): return self._short_we

    @short_we.setter
    def short_we(self, new_short_we):
        self._short_we = round(new_short_we,2)
        if self._config:
            t = json.loads(self._config)
            t["short"]["wallet_exposure_limit"] = self._short_we
            self._config = config_pretty_str(t)

    @property
    def preview_grid(self): return self._preview_grid
    @preview_grid.setter
    def preview_grid(self, new_preview_grid):
        self._preview_grid = new_preview_grid

    def update_config(self):
        self.long_we = json.loads(self._config)["long"]["wallet_exposure_limit"]
        self.short_we = json.loads(self._config)["short"]["wallet_exposure_limit"]
        self._config_v7.bot.long.total_wallet_exposure_limit = self.long_we
        self._config_v7.bot.short.total_wallet_exposure_limit = self.short_we
        self.long_enabled = json.loads(self._config)["long"]["enabled"]
        self.short_enabled = json.loads(self._config)["short"]["enabled"]
        if not self.long_enabled:
            self._config_v7.bot.long.n_positions = 0.0
        if not self.short_enabled:
            self._config_v7.bot.short.n_positions = 0.0
        long = json.loads(self._config)["long"]
        if "ddown_factor" in long:
            self._type = "recursive_grid"
        elif "qty_pct_entry" in long:
            self._type = "clock"
        elif "grid_span" in long:
            self._type = "neat_grid"

    def load_config(self):
        file =  Path(f'{self._config_file}')
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                self._config = f.read()
                self.update_config()

    def save_config(self):
        if self._config != None and self._config_file != None:
            file = Path(f'{self._config_file}')
            with open(file, "w", encoding='utf-8') as f:
                f.write(self._config)

    def edit_config(self):
        # Init session_state for keys
        if "config_long_enabled" in st.session_state:
            if st.session_state.config_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.config_long_enabled
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_short_enabled" in st.session_state:
            if st.session_state.config_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.config_short_enabled
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_long_we" in st.session_state:
            if st.session_state.config_long_we != self.long_we:
                self.long_we = st.session_state.config_long_we
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_short_we" in st.session_state:
            if st.session_state.config_short_we != self.short_we:
                self.short_we = st.session_state.config_short_we
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_preview_grid" in st.session_state:
            if st.session_state.config_preview_grid != self.preview_grid:
                self.preview_grid = st.session_state.config_preview_grid
        if "config_instance_config" in st.session_state:
            if st.session_state.config_instance_config != self.config:
                self.config = st.session_state.config_instance_config
                st.session_state.config_long_enabled = self.long_enabled
                st.session_state.config_short_enabled = self.short_enabled
                st.session_state.config_long_we = self.long_we
                st.session_state.config_short_we = self.short_we
            else:
                if validateJSON(st.session_state.config_instance_config):
                    if "error_config" in st.session_state:
                        del st.session_state.error_config
        # if self.config:
        #     self.config = st.session_state.config_instance_config
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            st.toggle("Long enabled", value=self.long_enabled, key="config_long_enabled", help=None)
            st.number_input("LONG_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=100.0, value=float(round(self.long_we,2)), step=0.05, format="%.2f", key="config_long_we", help=pbgui_help.exposure)
        with col2:
            st.toggle("Short enabled", value=self.short_enabled, key="config_short_enabled", help=None)
            st.number_input("SHORT_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=100.0, value=float(round(self.short_we,2)), step=0.05, format="%.2f", key="config_short_we", help=pbgui_help.exposure)
        with col3:
            st.toggle("Preview Grid", value=self.preview_grid, key="config_preview_grid", help=None)
            st.selectbox("Config Type", [self.type], index=0, key="config_type", help=None, disabled=True)
        # Init height and color with defaults
        height = 600
        color = None
        # Display Error
        if "error_config" in st.session_state:
            st.error(st.session_state.error_config, icon="🚨")
            color = "red"
        if not self.config is None:
            height = len(self.config.splitlines()) *23
        if height < 600:
            height = 600
        if not self.config:
            color = "red"
        col1, col2 = st.columns([1,1])
        with col1:
            if color:
                st.text_area(f':{color}[config]', self.config, key="config_instance_config", height=height)
            else:
                st.text_area(f'config', self.config, key="config_instance_config", height=height)
        with col2:
            st.text_area(f'config converted to v7', self.config_v7, key="config_instance_config_v7", height=height, disabled=True)

# config template
# {
#     "backtest": {
#         "balance_sample_divider": 60,
#         "base_dir": "backtests",
#         "btc_collateral_cap": 0.7,
#         "btc_collateral_ltv_cap": null,
#         "combine_ohlcvs": true,
#         "compress_cache": true,
#         "end_date": "now",
#         "exchanges": ["binance","bybit"],
#         "filter_by_min_effective_cost": false,
#         "gap_tolerance_ohlcvs_minutes": 120,
#         "max_warmup_minutes": 0,
#         "start_date": "2021-03-01",
#         "starting_balance": 100000,
#         "suite": {
#             "aggregate": {"default":"mean"},
#             "base_label": "base",
#             "enabled": false,
#             "include_base_scenario": true,
#             "scenarios": [
#                 {
#                     "coins": ["ADA","BTC","ETH","SOL","XRP"],
#                     "label": "trump_troupe"
#                 },
#                 {
#                     "coins": [
#                         "ADA",
#                         "BCH",
#                         "BNB",
#                         "BTC",
#                         "DOGE",
#                         "ETH",
#                         "HYPE",
#                         "LINK",
#                         "SOL",
#                         "TRX",
#                         "XRP"
#                     ],
#                     "label": "subset1"
#                 },
#                 {
#                     "coins": [
#                         "AVAX",
#                         "DOT",
#                         "HBAR",
#                         "LTC",
#                         "SHIB",
#                         "SUI",
#                         "TON",
#                         "UNI",
#                         "XLM",
#                         "XMR",
#                         "ZEC"
#                     ],
#                     "label": "subset2"
#                 },
#                 {
#                     "coins": [
#                         "AVAX",
#                         "BTC",
#                         "DOGE",
#                         "HYPE",
#                         "LINK",
#                         "SHIB",
#                         "SOL",
#                         "UNI",
#                         "XMR",
#                         "XRP",
#                         "ZEC"
#                     ],
#                     "label": "subset3"
#                 },
#                 {
#                     "coins": [
#                         "ADA",
#                         "BCH",
#                         "BNB",
#                         "DOT",
#                         "ETH",
#                         "HBAR",
#                         "LTC",
#                         "SUI",
#                         "TON",
#                         "TRX",
#                         "XLM"
#                     ],
#                     "label": "subset4"
#                 },
#                 {
#                     "coins": ["ADA","BTC","ETH","SOL","XRP"],
#                     "label": "pure_trailing",
#                     "overrides": {"bot.long.entry_trailing_grid_ratio":1, "bot.long.close_trailing_grid_ratio":1},
#                     "start_date": "2024-07"
#                 },
#                 {
#                     "coins": ["ADA","BTC","ETH","SOL","XRP"],
#                     "label": "pure_grid",
#                     "overrides": {"bot.long.entry_trailing_grid_ratio":0, "bot.long.close_trailing_grid_ratio":0},
#                     "start_date": "2024-07"
#                 },
#                 {
#                     "label": "n_positions=3",
#                     "overrides": {"bot.long.n_positions":3},
#                     "start_date": "2024-07"
#                 }
#             ]
#         }
#     },
#     "bot": {
#         "long": {
#             "close_grid_markup_end": 0.00294,
#             "close_grid_markup_start": 0.00401,
#             "close_grid_qty_pct": 0.328,
#             "close_trailing_grid_ratio": -0.541,
#             "close_trailing_qty_pct": 0.133,
#             "close_trailing_retracement_pct": 0.00214,
#             "close_trailing_threshold_pct": 0.0344,
#             "ema_span_0": 1130.0,
#             "ema_span_1": 1370.0,
#             "entry_grid_double_down_factor": 2.28,
#             "entry_grid_spacing_pct": 0.0147,
#             "entry_grid_spacing_volatility_weight": 93.7,
#             "entry_grid_spacing_we_weight": 4.35,
#             "entry_initial_ema_dist": 0.00245,
#             "entry_initial_qty_pct": 0.0159,
#             "entry_trailing_double_down_factor": 0.443,
#             "entry_trailing_grid_ratio": -0.519,
#             "entry_trailing_retracement_pct": 0.00724,
#             "entry_trailing_retracement_volatility_weight": 140.0,
#             "entry_trailing_retracement_we_weight": 18.6,
#             "entry_trailing_threshold_pct": 0.00911,
#             "entry_trailing_threshold_volatility_weight": 199.0,
#             "entry_trailing_threshold_we_weight": 2.11,
#             "entry_volatility_ema_span_hours": 2540.0,
#             "filter_volatility_drop_pct": 0,
#             "filter_volatility_ema_span": 103.0,
#             "filter_volume_drop_pct": 0.5,
#             "filter_volume_ema_span": 2160,
#             "n_positions": 7,
#             "risk_twel_enforcer_threshold": 0.99,
#             "risk_we_excess_allowance_pct": 0.907,
#             "risk_wel_enforcer_threshold": 0.985,
#             "total_wallet_exposure_limit": 1,
#             "unstuck_close_pct": 0.0182,
#             "unstuck_ema_dist": -0.0906,
#             "unstuck_loss_allowance_pct": 0.00143,
#             "unstuck_threshold": 0.447
#         },
#         "short": {
#             "close_grid_markup_end": 0.001,
#             "close_grid_markup_start": 0.001,
#             "close_grid_qty_pct": 0.05,
#             "close_trailing_grid_ratio": -1,
#             "close_trailing_qty_pct": 0.05,
#             "close_trailing_retracement_pct": 0.0001,
#             "close_trailing_threshold_pct": 0.0001,
#             "ema_span_0": 200,
#             "ema_span_1": 200,
#             "entry_grid_double_down_factor": 0.2,
#             "entry_grid_spacing_pct": 0.001,
#             "entry_grid_spacing_volatility_weight": 1,
#             "entry_grid_spacing_we_weight": 0.1,
#             "entry_initial_ema_dist": -0.1,
#             "entry_initial_qty_pct": 0.004,
#             "entry_trailing_double_down_factor": 0.2,
#             "entry_trailing_grid_ratio": -1,
#             "entry_trailing_retracement_pct": 0.0001,
#             "entry_trailing_retracement_volatility_weight": 1,
#             "entry_trailing_retracement_we_weight": 0,
#             "entry_trailing_threshold_pct": 0.0001,
#             "entry_trailing_threshold_volatility_weight": 1,
#             "entry_trailing_threshold_we_weight": 0,
#             "entry_volatility_ema_span_hours": 672,
#             "filter_volatility_drop_pct": 0,
#             "filter_volatility_ema_span": 10,
#             "filter_volume_drop_pct": 0.5,
#             "filter_volume_ema_span": 360,
#             "n_positions": 7,
#             "risk_twel_enforcer_threshold": 0.95,
#             "risk_we_excess_allowance_pct": 0,
#             "risk_wel_enforcer_threshold": 0.95,
#             "total_wallet_exposure_limit": 0,
#             "unstuck_close_pct": 0.001,
#             "unstuck_ema_dist": -0.1,
#             "unstuck_loss_allowance_pct": 0.001,
#             "unstuck_threshold": 0.4
#         }
#     },
#     "coin_overrides": {},
#     "live": {
#         "approved_coins": "configs/approved_coins.json",
#         "auto_gs": true,
#         "empty_means_all_approved": true,
#         "execution_delay_seconds": 2,
#         "filter_by_min_effective_cost": true,
#         "forced_mode_long": "",
#         "forced_mode_short": "",
#         "ignored_coins": {"long":[],"short":[]},
#         "inactive_coin_candle_ttl_minutes": 10,
#         "leverage": 10,
#         "market_orders_allowed": false,
#         "max_disk_candles_per_symbol_per_tf": 2000000,
#         "max_memory_candles_per_symbol": 200000,
#         "max_n_cancellations_per_batch": 5,
#         "max_n_creations_per_batch": 3,
#         "max_n_restarts_per_day": 10,
#         "max_warmup_minutes": 0,
#         "minimum_coin_age_days": 180,
#         "order_match_tolerance_pct": 0.0002,
#         "pnls_max_lookback_days": 30,
#         "price_distance_threshold": 0.002,
#         "recv_window_ms": 5000,
#         "time_in_force": "good_till_cancelled",
#         "balance_override": null,
#         "balance_hysteresis_snap_pct": 0.02,
#         "user": "bybit_01",
#         "warmup_ratio": 0.3
#     },
#     "logging": {
#         "level": 1,
#         "memory_snapshot_interval_minutes": 30,
#         "volume_refresh_info_threshold_seconds": 30
#     },
#     "optimize": {
#         "bounds": {
#             "long_close_grid_markup_end": [0.001,0.025],
#             "long_close_grid_markup_start": [0.001,0.025],
#             "long_close_grid_qty_pct": [0.05,1],
#             "long_close_trailing_grid_ratio": [-1,1],
#             "long_close_trailing_qty_pct": [0.05,1],
#             "long_close_trailing_retracement_pct": [0.0001,0.025],
#             "long_close_trailing_threshold_pct": [0.0001,0.035],
#             "long_ema_span_0": [200,1440],
#             "long_ema_span_1": [200,1440],
#             "long_entry_grid_double_down_factor": [0.2,3],
#             "long_entry_grid_spacing_pct": [0.001,0.045],
#             "long_entry_grid_spacing_volatility_weight": [1,300],
#             "long_entry_grid_spacing_we_weight": [0.1,20],
#             "long_entry_initial_ema_dist": [-0.1,0.003],
#             "long_entry_initial_qty_pct": [0.004,0.05],
#             "long_entry_trailing_double_down_factor": [0.2,2],
#             "long_entry_trailing_grid_ratio": [-1,1],
#             "long_entry_trailing_retracement_pct": [0.0001,0.03],
#             "long_entry_trailing_retracement_volatility_weight": [1,300],
#             "long_entry_trailing_retracement_we_weight": [0,20],
#             "long_entry_trailing_threshold_pct": [0.0001,0.03],
#             "long_entry_trailing_threshold_volatility_weight": [1,300],
#             "long_entry_trailing_threshold_we_weight": [0,20],
#             "long_entry_volatility_ema_span_hours": [672,2688],
#             "long_filter_volatility_drop_pct": [0,0],
#             "long_filter_volatility_ema_span": [10,720],
#             "long_filter_volume_drop_pct": [0.5,1],
#             "long_filter_volume_ema_span": [360,2880],
#             "long_n_positions": [7,20],
#             "long_risk_twel_enforcer_threshold": [0.95,0.99],
#             "long_risk_we_excess_allowance_pct": [0,3],
#             "long_risk_wel_enforcer_threshold": [0.95,0.99],
#             "long_total_wallet_exposure_limit": [1,1],
#             "long_unstuck_close_pct": [0.001,0.05],
#             "long_unstuck_ema_dist": [-0.1,0.01],
#             "long_unstuck_loss_allowance_pct": [0.001,0.05],
#             "long_unstuck_threshold": [0.4,0.99],
#             "short_close_grid_markup_end": [0.001,0.025],
#             "short_close_grid_markup_start": [0.001,0.025],
#             "short_close_grid_qty_pct": [0.05,1],
#             "short_close_trailing_grid_ratio": [-1,1],
#             "short_close_trailing_qty_pct": [0.05,1],
#             "short_close_trailing_retracement_pct": [0.0001,0.025],
#             "short_close_trailing_threshold_pct": [0.0001,0.035],
#             "short_ema_span_0": [200,1440],
#             "short_ema_span_1": [200,1440],
#             "short_entry_grid_double_down_factor": [0.2,3],
#             "short_entry_grid_spacing_pct": [0.001,0.045],
#             "short_entry_grid_spacing_volatility_weight": [1,300],
#             "short_entry_grid_spacing_we_weight": [0.1,20],
#             "short_entry_initial_ema_dist": [-0.1,0.003],
#             "short_entry_initial_qty_pct": [0.004,0.05],
#             "short_entry_trailing_double_down_factor": [0.2,2],
#             "short_entry_trailing_grid_ratio": [-1,1],
#             "short_entry_trailing_retracement_pct": [0.0001,0.03],
#             "short_entry_trailing_retracement_volatility_weight": [1,300],
#             "short_entry_trailing_retracement_we_weight": [0,20],
#             "short_entry_trailing_threshold_pct": [0.0001,0.03],
#             "short_entry_trailing_threshold_volatility_weight": [1,300],
#             "short_entry_trailing_threshold_we_weight": [0,20],
#             "short_entry_volatility_ema_span_hours": [672,2688],
#             "short_filter_volatility_drop_pct": [0,0],
#             "short_filter_volatility_ema_span": [10,720],
#             "short_filter_volume_drop_pct": [0.5,1],
#             "short_filter_volume_ema_span": [360,2880],
#             "short_n_positions": [7,20],
#             "short_risk_twel_enforcer_threshold": [0.95,0.99],
#             "short_risk_we_excess_allowance_pct": [0,3],
#             "short_risk_wel_enforcer_threshold": [0.95,0.99],
#             "short_total_wallet_exposure_limit": [0,0],
#             "short_unstuck_close_pct": [0.001,0.05],
#             "short_unstuck_ema_dist": [-0.1,0.01],
#             "short_unstuck_loss_allowance_pct": [0.001,0.05],
#             "short_unstuck_threshold": [0.4,0.99]
#         },
#         "compress_results_file": true,
#         "crossover_eta": 20,
#         "crossover_probability": 0.64,
#         "enable_overrides": [],
#         "iters": 500000,
#         "limits": [
#             {"metric":"drawdown_worst_btc","penalize_if":"greater_than","value":0.9},
#             {"metric":"drawdown_worst_usd","penalize_if":"greater_than","value":0.9},
#             {"metric":"loss_profit_ratio","penalize_if":"greater_than","value":0.6},
#             {
#                 "metric": "adg_pnl_w",
#                 "penalize_if": "less_than",
#                 "stat": "mean",
#                 "value": 0.0007
#             },
#             {
#                 "metric": "peak_recovery_hours_pnl",
#                 "penalize_if": "greater_than",
#                 "value": 1344
#             },
#             {
#                 "metric": "position_held_hours_max",
#                 "penalize_if": "greater_than",
#                 "value": 1344
#             },
#             {
#                 "metric": "position_unchanged_hours_max",
#                 "penalize_if": "greater_than",
#                 "value": 840
#             }
#         ],
#         "mutation_eta": 20,
#         "mutation_indpb": 0.05,
#         "mutation_probability": 0.34,
#         "n_cpus": 8,
#         "offspring_multiplier": 1,
#         "pareto_max_size": 250,
#         "population_size": 250,
#         "round_to_n_significant_digits": 3,
#         "scoring": [
#             "adg_pnl_w",
#             "mdg_pnl_w",
#             "loss_profit_ratio",
#             "peak_recovery_hours_pnl",
#             "position_held_hours_max",
#             "position_unchanged_hours_max",
#             "volume_pct_per_day_avg_w",
#             "entry_initial_balance_pct_long"
#         ],
#         "write_all_results": true
#     }
# }



class Logging:

    LEVEL = {
        0: "warnings",
        1: "info",
        2: "debug",
        3: "trace"}

    def __init__(self):
        self._level = 1
        self._memory_snapshot_interval_minutes = 30
        self._volume_refresh_info_threshold_seconds = 30
        self._logging = {
            "level": self._level,
            "memory_snapshot_interval_minutes": self._memory_snapshot_interval_minutes,
            "volume_refresh_info_threshold_seconds": self._volume_refresh_info_threshold_seconds
        }
    
    def __repr__(self):
        return str(self._logging)

    @property
    def logging(self): return self._logging
    @logging.setter
    def logging(self, new_logging):
        if "level" in new_logging:
            self.level = new_logging["level"]
        if "memory_snapshot_interval_minutes" in new_logging:
            self.memory_snapshot_interval_minutes = new_logging["memory_snapshot_interval_minutes"]
        if "volume_refresh_info_threshold_seconds" in new_logging:
            self.volume_refresh_info_threshold_seconds = new_logging["volume_refresh_info_threshold_seconds"]
    
    @property
    def level(self): return self._level
    @property
    def memory_snapshot_interval_minutes(self): return self._memory_snapshot_interval_minutes
    @property
    def volume_refresh_info_threshold_seconds(self): return self._volume_refresh_info_threshold_seconds
    @level.setter
    def level(self, new_level):
        self._level = new_level
        self._logging["level"] = self._level
    @memory_snapshot_interval_minutes.setter
    def memory_snapshot_interval_minutes(self, new_memory_snapshot_interval_minutes):
        self._memory_snapshot_interval_minutes = new_memory_snapshot_interval_minutes
        self._logging["memory_snapshot_interval_minutes"] = self._memory_snapshot_interval_minutes
    @volume_refresh_info_threshold_seconds.setter
    def volume_refresh_info_threshold_seconds(self, new_volume_refresh_info_threshold_seconds):
        self._volume_refresh_info_threshold_seconds = new_volume_refresh_info_threshold_seconds
        self._logging["volume_refresh_info_threshold_seconds"] = self._volume_refresh_info_threshold_seconds


class Scenario:
    """
    A single scenario within a Suite configuration.
    Each scenario can override coins, dates, exchanges, and bot parameters.
    """
    def __init__(self):
        self._label = ""
        self._coins = []              # Optional: override approved_coins for this scenario
        self._ignored_coins = []      # Optional: coins to exclude
        self._start_date = None       # Optional: override backtest.start_date
        self._end_date = None         # Optional: override backtest.end_date
        self._exchanges = None        # Optional: ["binance"] or ["bybit"] or both
        self._coin_sources = {}       # Optional: {"BTC": "binance", "SOL": "bybit"}
        self._overrides = {}          # Optional: {"bot.long.n_positions": 3}
    
    def __repr__(self):
        return str(self.scenario)
    
    @property
    def scenario(self):
        """Returns the scenario as a dict for JSON serialization."""
        result = {"label": self._label}
        if self._coins:
            result["coins"] = self._coins
        if self._ignored_coins:
            result["ignored_coins"] = self._ignored_coins
        if self._start_date is not None:
            result["start_date"] = self._start_date
        if self._end_date is not None:
            result["end_date"] = self._end_date
        if self._exchanges is not None:
            result["exchanges"] = self._exchanges
        if self._coin_sources:
            result["coin_sources"] = self._coin_sources
        if self._overrides:
            result["overrides"] = self._overrides
        return result
    
    @scenario.setter
    def scenario(self, new_scenario):
        if isinstance(new_scenario, dict):
            self._label = new_scenario.get("label", "")
            self._coins = new_scenario.get("coins", [])
            self._ignored_coins = new_scenario.get("ignored_coins", [])
            self._start_date = new_scenario.get("start_date", None)
            self._end_date = new_scenario.get("end_date", None)
            self._exchanges = new_scenario.get("exchanges", None)
            self._coin_sources = new_scenario.get("coin_sources", {})
            self._overrides = new_scenario.get("overrides", {})
    
    @property
    def label(self): return self._label
    @label.setter
    def label(self, new_label):
        self._label = new_label
    
    @property
    def coins(self): return self._coins
    @coins.setter
    def coins(self, new_coins):
        """Set coins list, automatically normalizing symbols."""
        if new_coins:
            # Normalize all coin symbols (remove USDT/USDC suffixes and exchange prefixes)
            self._coins = [normalize_symbol(coin) for coin in new_coins]
        else:
            self._coins = []
    
    @property
    def ignored_coins(self): return self._ignored_coins
    @ignored_coins.setter
    def ignored_coins(self, new_ignored_coins):
        self._ignored_coins = [normalize_symbol(coin) for coin in new_ignored_coins] if new_ignored_coins else []
    
    @property
    def start_date(self): return self._start_date
    @start_date.setter
    def start_date(self, new_start_date):
        self._start_date = new_start_date if new_start_date else None
    
    @property
    def end_date(self): return self._end_date
    @end_date.setter
    def end_date(self, new_end_date):
        self._end_date = new_end_date if new_end_date else None
    
    @property
    def exchanges(self): return self._exchanges
    @exchanges.setter
    def exchanges(self, new_exchanges):
        self._exchanges = new_exchanges if new_exchanges else None
    
    @property
    def coin_sources(self): return self._coin_sources
    @coin_sources.setter
    def coin_sources(self, new_coin_sources):
        self._coin_sources = new_coin_sources if new_coin_sources else {}
    
    @property
    def overrides(self): return self._overrides
    @overrides.setter
    def overrides(self, new_overrides):
        self._overrides = new_overrides if new_overrides else {}


class Suite:
    """
    Suite configuration for multi-scenario backtesting/optimization.
    Allows evaluating a config across multiple coin sets, date ranges, and parameter variations.
    """
    def __init__(self):
        self._enabled = False
        self._include_base_scenario = True
        self._base_label = "base"
        self._aggregate = {"default": "mean"}
        self._scenarios = []  # List of Scenario objects
    
    def __repr__(self):
        return str(self.suite)
    
    @property
    def suite(self):
        """Returns the suite as a dict for JSON serialization."""
        return {
            "enabled": self._enabled,
            "include_base_scenario": self._include_base_scenario,
            "base_label": self._base_label,
            "aggregate": self._aggregate,
            "scenarios": [s.scenario for s in self._scenarios]
        }
    
    @suite.setter
    def suite(self, new_suite):
        if isinstance(new_suite, dict):
            self._enabled = new_suite.get("enabled", False)
            self._include_base_scenario = new_suite.get("include_base_scenario", True)
            self._base_label = new_suite.get("base_label", "base")
            self._aggregate = new_suite.get("aggregate", {"default": "mean"})
            # Parse scenarios list
            self._scenarios = []
            for scenario_dict in new_suite.get("scenarios", []):
                scenario = Scenario()
                scenario.scenario = scenario_dict
                self._scenarios.append(scenario)
    
    @property
    def enabled(self): return self._enabled
    @enabled.setter
    def enabled(self, new_enabled):
        self._enabled = bool(new_enabled)
    
    @property
    def include_base_scenario(self): return self._include_base_scenario
    @include_base_scenario.setter
    def include_base_scenario(self, new_include_base_scenario):
        self._include_base_scenario = bool(new_include_base_scenario)
    
    @property
    def base_label(self): return self._base_label
    @base_label.setter
    def base_label(self, new_base_label):
        self._base_label = new_base_label if new_base_label else "base"
    
    @property
    def aggregate(self): return self._aggregate
    @aggregate.setter
    def aggregate(self, new_aggregate):
        self._aggregate = new_aggregate if new_aggregate else {"default": "mean"}
    
    @property
    def scenarios(self): return self._scenarios
    @scenarios.setter
    def scenarios(self, new_scenarios):
        """Set scenarios from a list of Scenario objects or dicts."""
        self._scenarios = []
        if new_scenarios:
            for item in new_scenarios:
                if isinstance(item, Scenario):
                    self._scenarios.append(item)
                elif isinstance(item, dict):
                    scenario = Scenario()
                    scenario.scenario = item
                    self._scenarios.append(scenario)
    
    def add_scenario(self, scenario):
        """Add a scenario (Scenario object or dict)."""
        if isinstance(scenario, Scenario):
            self._scenarios.append(scenario)
        elif isinstance(scenario, dict):
            s = Scenario()
            s.scenario = scenario
            self._scenarios.append(s)
    
    def remove_scenario(self, index):
        """Remove a scenario by index."""
        if 0 <= index < len(self._scenarios):
            self._scenarios.pop(index)
    
    def get_scenario(self, index):
        """Get a scenario by index."""
        if 0 <= index < len(self._scenarios):
            return self._scenarios[index]
        return None
    
    def update_scenario(self, index, scenario):
        """Update a scenario at the given index."""
        if 0 <= index < len(self._scenarios):
            if isinstance(scenario, Scenario):
                self._scenarios[index] = scenario
            elif isinstance(scenario, dict):
                self._scenarios[index].scenario = scenario


class Backtest:
    def __init__(self):
        self._balance_sample_divider = 60
        self._base_dir = "backtests"
        self._compress_cache = True
        self._end_date = "now"
        self._exchanges = ["binance", "bybit"]
        self._filter_by_min_effective_cost = False
        self._gap_tolerance_ohlcvs_minutes = 120.0
        # PB7 backtest defaults (see pb7/src/config_utils.py)
        self._maker_fee_override = None
        self._start_date = "2020-01-01"
        self._starting_balance = 1000.0
        self._btc_collateral_cap = 0.0
        self._btc_collateral_ltv_cap = None
        self._max_warmup_minutes = 0.0
        self._candle_interval_minutes = 1
        self._market_settings_sources = {}
        self._volume_normalization = True
        self._coin_sources = {}
        self._ohlcv_source_dir = None
        self._suite = Suite()
        self._suite_enabled = bool(self._suite.enabled)
        self._backtest = {
            "balance_sample_divider": self._balance_sample_divider,
            "base_dir": self._base_dir,
            "compress_cache": self._compress_cache,
            "end_date": self._end_date,
            "exchanges": self._exchanges,
            "filter_by_min_effective_cost": self._filter_by_min_effective_cost,
            "gap_tolerance_ohlcvs_minutes": self._gap_tolerance_ohlcvs_minutes,
            "maker_fee_override": self._maker_fee_override,
            "start_date": self._start_date,
            "starting_balance": self._starting_balance,
            "btc_collateral_cap": self._btc_collateral_cap,
            "btc_collateral_ltv_cap": self._btc_collateral_ltv_cap,
            "max_warmup_minutes": self._max_warmup_minutes,
            "candle_interval_minutes": self._candle_interval_minutes,
            "market_settings_sources": self._market_settings_sources,
            "volume_normalization": self._volume_normalization,
            "coin_sources": self._coin_sources,
            "ohlcv_source_dir": self._ohlcv_source_dir,
            "suite_enabled": self._suite_enabled,
            "suite": self._suite.suite
        }
    
    def __repr__(self):
        return str(self.backtest)
    
    @property
    def backtest(self):
        # Dynamically update suite to ensure scenarios are current
        self._backtest["suite"] = self._suite.suite
        self._backtest["suite_enabled"] = self._suite_enabled
        self._backtest["coin_sources"] = self._coin_sources
        return self._backtest
    @backtest.setter
    def backtest(self, new_backtest):
        if "balance_sample_divider" in new_backtest:
            self.balance_sample_divider = new_backtest["balance_sample_divider"]
        if "base_dir" in new_backtest:
            self.base_dir = new_backtest["base_dir"]
        if "compress_cache" in new_backtest:
            self.compress_cache = new_backtest["compress_cache"]
        if "end_date" in new_backtest:
            self.end_date = new_backtest["end_date"]
        if "exchanges" in new_backtest:
            self.exchanges = new_backtest["exchanges"]
        if "filter_by_min_effective_cost" in new_backtest:
            self.filter_by_min_effective_cost = new_backtest["filter_by_min_effective_cost"]
        if "gap_tolerance_ohlcvs_minutes" in new_backtest:
            self.gap_tolerance_ohlcvs_minutes = new_backtest["gap_tolerance_ohlcvs_minutes"]
        if "maker_fee_override" in new_backtest:
            self.maker_fee_override = new_backtest["maker_fee_override"]
        if "start_date" in new_backtest:
            self.start_date = new_backtest["start_date"]
        if "starting_balance" in new_backtest:
            self.starting_balance = new_backtest["starting_balance"]
        if "btc_collateral_cap" in new_backtest:
            self.btc_collateral_cap = new_backtest["btc_collateral_cap"]
        if "btc_collateral_ltv_cap" in new_backtest:
            self.btc_collateral_ltv_cap = new_backtest["btc_collateral_ltv_cap"]
        # Legacy support: convert use_btc_collateral to btc_collateral_cap
        if "use_btc_collateral" in new_backtest:
            if new_backtest["use_btc_collateral"]:
                self.btc_collateral_cap = 1.0
            else:
                self.btc_collateral_cap = 0.0
        if "max_warmup_minutes" in new_backtest:
            self.max_warmup_minutes = new_backtest["max_warmup_minutes"]
        if "candle_interval_minutes" in new_backtest:
            self.candle_interval_minutes = new_backtest["candle_interval_minutes"]
        if "market_settings_sources" in new_backtest:
            self.market_settings_sources = new_backtest["market_settings_sources"]
        if "volume_normalization" in new_backtest:
            self.volume_normalization = new_backtest["volume_normalization"]
        if "coin_sources" in new_backtest:
            self.coin_sources = new_backtest["coin_sources"]
        if "ohlcv_source_dir" in new_backtest:
            self.ohlcv_source_dir = new_backtest["ohlcv_source_dir"]
        if "suite" in new_backtest:
            self.suite = new_backtest["suite"]
        if "suite_enabled" in new_backtest:
            self.suite_enabled = new_backtest["suite_enabled"]
    
    @property
    def balance_sample_divider(self): return self._balance_sample_divider
    @property
    def base_dir(self): return self._base_dir
    @property
    def compress_cache(self): return self._compress_cache
    @property
    def end_date(self):
        if self._end_date == "now":
            return (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        return self._end_date
    @property
    def exchanges(self): return self._exchanges
    @property
    def filter_by_min_effective_cost(self): return self._filter_by_min_effective_cost
    @property
    def gap_tolerance_ohlcvs_minutes(self): return self._gap_tolerance_ohlcvs_minutes
    @property
    def maker_fee_override(self): return self._maker_fee_override
    @property
    def start_date(self): return self._start_date
    @property
    def starting_balance(self): return self._starting_balance
    @property
    def btc_collateral_cap(self): return self._btc_collateral_cap
    @property
    def btc_collateral_ltv_cap(self): return self._btc_collateral_ltv_cap
    @property
    def max_warmup_minutes(self): return self._max_warmup_minutes
    @property
    def candle_interval_minutes(self): return self._candle_interval_minutes
    @property
    def market_settings_sources(self): return self._market_settings_sources
    @property
    def volume_normalization(self): return self._volume_normalization
    @property
    def coin_sources(self): return self._coin_sources
    @property
    def ohlcv_source_dir(self): return self._ohlcv_source_dir
    @property
    def suite_enabled(self): return self._suite_enabled
    @property
    def suite(self): return self._suite

    @balance_sample_divider.setter
    def balance_sample_divider(self, new_balance_sample_divider):
        self._balance_sample_divider = new_balance_sample_divider
        self._backtest["balance_sample_divider"] = self._balance_sample_divider
    @base_dir.setter
    def base_dir(self, new_base_dir):
        self._base_dir = new_base_dir
        self._backtest["base_dir"] = self._base_dir
    @compress_cache.setter
    def compress_cache(self, new_compress_cache):
        self._compress_cache = new_compress_cache
        self._backtest["compress_cache"] = self._compress_cache
    @end_date.setter
    def end_date(self, new_end_date):
        self._end_date = new_end_date
        self._backtest["end_date"] = self._end_date
    @exchanges.setter
    def exchanges(self, new_exchanges):
        self._exchanges = new_exchanges
        self._backtest["exchanges"] = self._exchanges
    @filter_by_min_effective_cost.setter
    def filter_by_min_effective_cost(self, new_filter_by_min_effective_cost):
        self._filter_by_min_effective_cost = new_filter_by_min_effective_cost
        self._backtest["filter_by_min_effective_cost"] = self._filter_by_min_effective_cost
    @gap_tolerance_ohlcvs_minutes.setter
    def gap_tolerance_ohlcvs_minutes(self, new_gap_tolerance_ohlcvs_minutes):
        self._gap_tolerance_ohlcvs_minutes = new_gap_tolerance_ohlcvs_minutes
        self._backtest["gap_tolerance_ohlcvs_minutes"] = self._gap_tolerance_ohlcvs_minutes
    @maker_fee_override.setter
    def maker_fee_override(self, new_maker_fee_override):
        if new_maker_fee_override in (None, ""):
            self._maker_fee_override = None
        else:
            self._maker_fee_override = float(new_maker_fee_override)
        self._backtest["maker_fee_override"] = self._maker_fee_override
    @start_date.setter
    def start_date(self, new_start_date):
        self._start_date = new_start_date
        self._backtest["start_date"] = self._start_date
    @starting_balance.setter
    def starting_balance(self, new_starting_balance):
        self._starting_balance = new_starting_balance
        self._backtest["starting_balance"] = self._starting_balance
    @btc_collateral_cap.setter
    def btc_collateral_cap(self, new_btc_collateral_cap):
        self._btc_collateral_cap = new_btc_collateral_cap
        self._backtest["btc_collateral_cap"] = self._btc_collateral_cap
    @btc_collateral_ltv_cap.setter
    def btc_collateral_ltv_cap(self, new_btc_collateral_ltv_cap):
        self._btc_collateral_ltv_cap = new_btc_collateral_ltv_cap
        self._backtest["btc_collateral_ltv_cap"] = self._btc_collateral_ltv_cap
    @max_warmup_minutes.setter
    def max_warmup_minutes(self, new_max_warmup_minutes):
        self._max_warmup_minutes = new_max_warmup_minutes
        self._backtest["max_warmup_minutes"] = self._max_warmup_minutes
    @candle_interval_minutes.setter
    def candle_interval_minutes(self, new_candle_interval_minutes):
        try:
            value = int(new_candle_interval_minutes)
        except (TypeError, ValueError):
            value = 1
        self._candle_interval_minutes = max(1, value)
        self._backtest["candle_interval_minutes"] = self._candle_interval_minutes
    @market_settings_sources.setter
    def market_settings_sources(self, new_market_settings_sources):
        self._market_settings_sources = new_market_settings_sources if new_market_settings_sources else {}
        self._backtest["market_settings_sources"] = self._market_settings_sources
    @volume_normalization.setter
    def volume_normalization(self, new_volume_normalization):
        self._volume_normalization = bool(new_volume_normalization)
        self._backtest["volume_normalization"] = self._volume_normalization
    @coin_sources.setter
    def coin_sources(self, new_coin_sources):
        self._coin_sources = new_coin_sources if new_coin_sources else {}
        self._backtest["coin_sources"] = self._coin_sources
    @ohlcv_source_dir.setter
    def ohlcv_source_dir(self, new_ohlcv_source_dir):
        if new_ohlcv_source_dir in (None, ""):
            self._ohlcv_source_dir = None
        else:
            self._ohlcv_source_dir = str(new_ohlcv_source_dir)
        self._backtest["ohlcv_source_dir"] = self._ohlcv_source_dir
    @suite_enabled.setter
    def suite_enabled(self, new_suite_enabled):
        self._suite_enabled = bool(new_suite_enabled)
        if self._suite:
            self._suite.enabled = self._suite_enabled
        self._backtest["suite_enabled"] = self._suite_enabled
    @suite.setter
    def suite(self, new_suite):
        if isinstance(new_suite, Suite):
            self._suite = new_suite
        elif isinstance(new_suite, dict):
            self._suite.suite = new_suite
        self._suite_enabled = bool(self._suite.enabled)
        self._backtest["suite_enabled"] = self._suite_enabled
        self._backtest["suite"] = self._suite.suite

class Bot:
    def __init__(self):
        self._long = Long()
        self._short = Short()
        self._bot = {
            "long": self._long._long,
            "short": self._short._short
        }    

    def __repr__(self):
        return str(self._bot)
    
    @property
    def bot(self): return self._bot
    @bot.setter
    def bot(self, new_bot):
        if "long" in new_bot:
            self.long = new_bot["long"]
        if "short" in new_bot:
            self.short = new_bot["short"]
    
    @property
    def long(self): return self._long
    @property
    def short(self): return self._short

    @long.setter
    def long(self, new_long):
        # Create a fresh Long so missing keys fall back to defaults
        # instead of inheriting stale values from a previously loaded config.
        self._long = Long()
        self._long.long = new_long
        self._bot["long"] = self._long.long
    @short.setter
    def short(self, new_short):
        # Create a fresh Short so missing keys fall back to defaults
        # instead of inheriting stale values from a previously loaded config.
        self._short = Short()
        self._short.short = new_short
        self._bot["short"] = self._short.short
    
    @st.fragment
    def edit(self):
        # Init session_state for keys
        if "edit_configv7_long_twe" in st.session_state:
            if st.session_state.edit_configv7_long_twe != self.long.total_wallet_exposure_limit:
                self.long.total_wallet_exposure_limit = round(st.session_state.edit_configv7_long_twe,2)
                st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
            if "edit_configv7_long" in st.session_state:
                try:
                    long = json.loads(st.session_state.edit_configv7_long)
                    if st.session_state.edit_configv7_long_twe != float(long["total_wallet_exposure_limit"]):
                        st.session_state.edit_configv7_long_twe = float(long["total_wallet_exposure_limit"])
                except:
                    st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
                    error_popup("Invalid JSON long | RESET")
        else:
            st.session_state.edit_configv7_long_twe = float(self.long.total_wallet_exposure_limit)

        if "edit_configv7_long_positions" in st.session_state:
            if st.session_state.edit_configv7_long_positions != self.long.n_positions:
                self.long.n_positions = round(st.session_state.edit_configv7_long_positions,0)
                st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
            if "edit_configv7_long" in st.session_state:
                try:
                    long = json.loads(st.session_state.edit_configv7_long)
                    if st.session_state.edit_configv7_long_positions != float(long["n_positions"]):
                        st.session_state.edit_configv7_long_positions = float(long["n_positions"])
                except:
                    st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
                    error_popup("Invalid JSON long | RESET")
        else:
            st.session_state.edit_configv7_long_positions = float(self.long.n_positions)

        if "edit_configv7_short_twe" in st.session_state:
            if st.session_state.edit_configv7_short_twe != self.short.total_wallet_exposure_limit:
                self.short.total_wallet_exposure_limit = round(st.session_state.edit_configv7_short_twe,2)
                st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
            if "edit_configv7_short" in st.session_state:
                try:
                    short = json.loads(st.session_state.edit_configv7_short)
                    if st.session_state.edit_configv7_short_twe != float(short["total_wallet_exposure_limit"]):
                        st.session_state.edit_configv7_short_twe = float(short["total_wallet_exposure_limit"])
                except:
                    st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
                    error_popup("Invalid JSON short | RESET")
        else:
            st.session_state.edit_configv7_short_twe = float(self.short.total_wallet_exposure_limit)

        if "edit_configv7_short_positions" in st.session_state:
            if st.session_state.edit_configv7_short_positions != self.short.n_positions:
                self.short.n_positions = round(st.session_state.edit_configv7_short_positions,0)
                st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
            if "edit_configv7_short" in st.session_state:
                try:
                    short = json.loads(st.session_state.edit_configv7_short)
                    if st.session_state.edit_configv7_short_positions != float(short["n_positions"]):
                        st.session_state.edit_configv7_short_positions = float(short["n_positions"])
                except:
                    st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
                    error_popup("Invalid JSON short | RESET")   
        else:
            st.session_state.edit_configv7_short_positions = float(self.short.n_positions)

        if "edit_configv7_long" in st.session_state:
            if st.session_state.edit_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_configv7_long)
                except:
                    st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
                    error_popup("Invalid JSON long | RESET")
        else:
            st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)

        if "edit_configv7_short" in st.session_state:
            if st.session_state.edit_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_configv7_short)
                except:
                    st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
                    error_popup("Invalid JSON short | RESET")
        else:
            st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
        # Display config
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("long twe", min_value=0.0, max_value=100.0, step=0.05, format="%.2f", key="edit_configv7_long_twe", help=pbgui_help.total_wallet_exposure_limit)
        with col2:
            st.number_input("long positions", min_value=0.0, max_value=100.0, step=1.0, format="%.2f", key="edit_configv7_long_positions", help=pbgui_help.n_positions)
        with col3:
            st.number_input("short twe", min_value=0.0, max_value=100.0, step=0.05, format="%.2f", key="edit_configv7_short_twe", help=pbgui_help.total_wallet_exposure_limit)
        with col4:
            st.number_input("short positions", min_value=0.0, max_value=100.0, step=1.0, format="%.2f", key="edit_configv7_short_positions", help=pbgui_help.n_positions)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', key="edit_configv7_long", height=600)
        with col2:
            st.text_area(f'short', key="edit_configv7_short", height=600)

    def edit_cf(self):
        # Init session_state for keys
        if "edit_cf_configv7_long" in st.session_state:
            if st.session_state.edit_cf_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_cf_configv7_long)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_cf_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_cf_configv7_short" in st.session_state:
            if st.session_state.edit_cf_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_cf_configv7_short)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_cf_configv7_short = json.dumps(self.bot["short"], indent=4)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', key="edit_cf_configv7_long", height=640)
        with col2:
            st.text_area(f'short', key="edit_cf_configv7_short", height=640)
    
    def edit_co(self):
        # Init session_state for keys
        if "edit_co_configv7_long" in st.session_state:
            if st.session_state.edit_co_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_co_configv7_long)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_co_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_co_configv7_short" in st.session_state:
            if st.session_state.edit_co_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_co_configv7_short)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_co_configv7_short = json.dumps(self.bot["short"], indent=4)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', key="edit_co_configv7_long", height=640)
        with col2:
            st.text_area(f'short', key="edit_co_configv7_short", height=640)

class Long:
    def __init__(self):
        self._close_grid_markup_end = 0.0089
        self._close_grid_markup_start = 0.0344
        self._close_grid_qty_pct = 0.125
        self._close_trailing_grid_ratio = 0.5
        self._close_trailing_qty_pct = 0.125
        self._close_trailing_retracement_pct = 0.002
        self._close_trailing_threshold_pct = 0.008
        self._ema_span_0 = 1318.0
        self._ema_span_1 = 1435.0
        self._entry_grid_double_down_factor = 0.894
        self._entry_grid_spacing_volatility_weight = 72.0
        self._entry_grid_spacing_pct = 0.04
        self._entry_grid_spacing_we_weight = 0.697
        self._entry_initial_ema_dist = -0.00738
        self._entry_initial_qty_pct = 0.00592
        self._entry_trailing_double_down_factor = 0.894
        self._entry_trailing_grid_ratio = 0.5
        self._entry_trailing_retracement_pct = 0.01
        self._entry_trailing_threshold_pct = 0.05
        self._entry_trailing_threshold_we_weight = 0.0
        self._entry_trailing_threshold_volatility_weight = 0.0
        self._entry_trailing_retracement_we_weight = 0.0
        self._entry_trailing_retracement_volatility_weight = 0.0
        self._entry_volatility_ema_span_hours = 72
        self._filter_volatility_ema_span = 60.0
        self._filter_volatility_drop_pct = 0.0
        self._filter_volume_drop_pct = 0.95
        self._filter_volume_ema_span = 60.0
        self._n_positions = 10.0
        self._total_wallet_exposure_limit = 1.7
        self._unstuck_close_pct = 0.001
        self._unstuck_ema_dist = 0.0
        self._unstuck_loss_allowance_pct = 0.03
        self._unstuck_threshold = 0.916
        self._risk_wel_enforcer_threshold = 1.0
        self._risk_we_excess_allowance_pct = 0.0
        self._risk_twel_enforcer_threshold = 1.0
        self._long = {
            "close_grid_markup_end": self._close_grid_markup_end,
            "close_grid_markup_start": self._close_grid_markup_start,
            "close_grid_qty_pct": self._close_grid_qty_pct,
            "close_trailing_grid_ratio": self._close_trailing_grid_ratio,
            "close_trailing_qty_pct": self._close_trailing_qty_pct,
            "close_trailing_retracement_pct": self._close_trailing_retracement_pct,
            "close_trailing_threshold_pct": self._close_trailing_threshold_pct,
            "ema_span_0": self._ema_span_0,
            "ema_span_1": self._ema_span_1,
            "entry_grid_double_down_factor": self._entry_grid_double_down_factor,
            "entry_grid_spacing_volatility_weight": self._entry_grid_spacing_volatility_weight,
            "entry_grid_spacing_pct": self._entry_grid_spacing_pct,
            "entry_grid_spacing_we_weight": self._entry_grid_spacing_we_weight,
            "entry_initial_ema_dist": self._entry_initial_ema_dist,
            "entry_initial_qty_pct": self._entry_initial_qty_pct,
            "entry_trailing_double_down_factor": self._entry_trailing_double_down_factor,
            "entry_trailing_grid_ratio": self._entry_trailing_grid_ratio,
            "entry_trailing_retracement_pct": self._entry_trailing_retracement_pct,
            "entry_trailing_threshold_pct": self._entry_trailing_threshold_pct,
            "entry_trailing_threshold_we_weight": self._entry_trailing_threshold_we_weight,
            "entry_trailing_threshold_volatility_weight": self._entry_trailing_threshold_volatility_weight,
            "entry_trailing_retracement_we_weight": self._entry_trailing_retracement_we_weight,
            "entry_trailing_retracement_volatility_weight": self._entry_trailing_retracement_volatility_weight,
            "entry_volatility_ema_span_hours": self._entry_volatility_ema_span_hours,
            "filter_volatility_ema_span": self._filter_volatility_ema_span,
            "filter_volatility_drop_pct": self._filter_volatility_drop_pct,
            "filter_volume_drop_pct": self._filter_volume_drop_pct,
            "filter_volume_ema_span": self._filter_volume_ema_span,
            "n_positions": self._n_positions,
            "total_wallet_exposure_limit": self._total_wallet_exposure_limit,
            "unstuck_close_pct": self._unstuck_close_pct,
            "unstuck_ema_dist": self._unstuck_ema_dist,
            "unstuck_loss_allowance_pct": self._unstuck_loss_allowance_pct,
            "unstuck_threshold": self._unstuck_threshold,
            "risk_wel_enforcer_threshold": self._risk_wel_enforcer_threshold,
            "risk_we_excess_allowance_pct": self._risk_we_excess_allowance_pct,
            "risk_twel_enforcer_threshold": self._risk_twel_enforcer_threshold
        }

    def __repr__(self):
        return str(self._long)
    
    @property
    def long(self): return self._long
    @long.setter
    def long(self, new_long):
        #Fix for old markup parameters
        if "close_grid_markup_range" in new_long and "close_grid_min_markup" in new_long:
            self.close_grid_markup_start = new_long["close_grid_min_markup"] + new_long["close_grid_markup_range"]
            self.close_grid_markup_end = new_long["close_grid_min_markup"]
        if "close_grid_markup_end" in new_long:
            self.close_grid_markup_end = new_long["close_grid_markup_end"]
        if "close_grid_markup_start" in new_long:
            self.close_grid_markup_start = new_long["close_grid_markup_start"]
        if "close_grid_qty_pct" in new_long:
            self.close_grid_qty_pct = new_long["close_grid_qty_pct"]
        if "close_trailing_grid_ratio" in new_long:
            self.close_trailing_grid_ratio = new_long["close_trailing_grid_ratio"]
        if "close_trailing_qty_pct" in new_long:
            self.close_trailing_qty_pct = new_long["close_trailing_qty_pct"]
        if "close_trailing_retracement_pct" in new_long:
            self.close_trailing_retracement_pct = new_long["close_trailing_retracement_pct"]
        if "close_trailing_threshold_pct" in new_long:
            self.close_trailing_threshold_pct = new_long["close_trailing_threshold_pct"]
        if "ema_span_0" in new_long:
            self.ema_span_0 = new_long["ema_span_0"]
        if "ema_span_1" in new_long:
            self.ema_span_1 = new_long["ema_span_1"]
        if "entry_grid_double_down_factor" in new_long:
            self.entry_grid_double_down_factor = new_long["entry_grid_double_down_factor"]
        if "entry_grid_spacing_volatility_weight" in new_long:
            self.entry_grid_spacing_volatility_weight = new_long["entry_grid_spacing_volatility_weight"]
        # Fix for old configs
        elif "entry_grid_spacing_log_weight" in new_long:
            self.entry_grid_spacing_volatility_weight = new_long["entry_grid_spacing_log_weight"]
        if "entry_grid_spacing_pct" in new_long:
            self.entry_grid_spacing_pct = new_long["entry_grid_spacing_pct"]
        if "entry_grid_spacing_we_weight" in new_long:
            self.entry_grid_spacing_we_weight = new_long["entry_grid_spacing_we_weight"]
        # Fix for old configs
        elif "entry_grid_spacing_weight" in new_long:
            self.entry_grid_spacing_we_weight = new_long["entry_grid_spacing_weight"]
        if "entry_initial_ema_dist" in new_long:
            self.entry_initial_ema_dist = new_long["entry_initial_ema_dist"]
        if "entry_initial_qty_pct" in new_long:
            self.entry_initial_qty_pct = new_long["entry_initial_qty_pct"]
        if "entry_trailing_double_down_factor" in new_long:
            self.entry_trailing_double_down_factor = new_long["entry_trailing_double_down_factor"]
        else:
            self.entry_trailing_double_down_factor = self.entry_grid_double_down_factor
        if "entry_trailing_grid_ratio" in new_long:
            self.entry_trailing_grid_ratio = new_long["entry_trailing_grid_ratio"]
        if "entry_trailing_retracement_pct" in new_long:
            self.entry_trailing_retracement_pct = new_long["entry_trailing_retracement_pct"]
        if "entry_trailing_threshold_pct" in new_long:
            self.entry_trailing_threshold_pct = new_long["entry_trailing_threshold_pct"]
        if "entry_trailing_threshold_we_weight" in new_long:
            self.entry_trailing_threshold_we_weight = new_long["entry_trailing_threshold_we_weight"]
        if "entry_trailing_threshold_volatility_weight" in new_long:
            self.entry_trailing_threshold_volatility_weight = new_long["entry_trailing_threshold_volatility_weight"]
        if "entry_trailing_retracement_we_weight" in new_long:
            self.entry_trailing_retracement_we_weight = new_long["entry_trailing_retracement_we_weight"]
        if "entry_trailing_retracement_volatility_weight" in new_long:
            self.entry_trailing_retracement_volatility_weight = new_long["entry_trailing_retracement_volatility_weight"]
        if "entry_volatility_ema_span_hours" in new_long:
            self.entry_volatility_ema_span_hours = new_long["entry_volatility_ema_span_hours"]
        # Fix for old configs
        elif "entry_grid_spacing_log_span_hours" in new_long:
            self.entry_volatility_ema_span_hours = new_long["entry_grid_spacing_log_span_hours"]
        if "filter_volatility_ema_span" in new_long:
            self.filter_volatility_ema_span = new_long["filter_volatility_ema_span"]
        # Fix for old configs
        elif "filter_log_range_ema_span" in new_long:
            self.filter_volatility_ema_span = new_long["filter_log_range_ema_span"]
        # Fix for old configs
        elif "filter_noisiness_rolling_window" in new_long:
            self.filter_log_range_ema_span = new_long["filter_noisiness_rolling_window"]
        elif "filter_rolling_window" in new_long:
            self.filter_log_range_ema_span = new_long["filter_rolling_window"]
        if "filter_volatility_drop_pct" in new_long:
            self.filter_volatility_drop_pct = new_long["filter_volatility_drop_pct"]
        if "filter_volume_drop_pct" in new_long:
            self.filter_volume_drop_pct = new_long["filter_volume_drop_pct"]
        # Fix for old configs
        elif "filter_relative_volume_clip_pct" in new_long:
            self.filter_volume_drop_pct = new_long["filter_relative_volume_clip_pct"]
        if "filter_volume_ema_span" in new_long:
            self.filter_volume_ema_span = new_long["filter_volume_ema_span"]
        # Fix for old configs
        elif "filter_rolling_window" in new_long:
            self.filter_volume_ema_span = new_long["filter_rolling_window"]
        if "n_positions" in new_long:
            self.n_positions = new_long["n_positions"]
        if "total_wallet_exposure_limit" in new_long:
            self.total_wallet_exposure_limit = new_long["total_wallet_exposure_limit"]
        if "unstuck_close_pct" in new_long:
            self.unstuck_close_pct = new_long["unstuck_close_pct"]
        if "unstuck_ema_dist" in new_long:
            self.unstuck_ema_dist = new_long["unstuck_ema_dist"]
        if "unstuck_loss_allowance_pct" in new_long:
            self.unstuck_loss_allowance_pct = new_long["unstuck_loss_allowance_pct"]
        if "unstuck_threshold" in new_long:
            self.unstuck_threshold = new_long["unstuck_threshold"]
        if "risk_wel_enforcer_threshold" in new_long:
            self.risk_wel_enforcer_threshold = new_long["risk_wel_enforcer_threshold"]
        if "risk_we_excess_allowance_pct" in new_long:
            self.risk_we_excess_allowance_pct = new_long["risk_we_excess_allowance_pct"]
        if "risk_twel_enforcer_threshold" in new_long:
            self.risk_twel_enforcer_threshold = new_long["risk_twel_enforcer_threshold"]

    @property
    def close_grid_markup_end(self): return self._close_grid_markup_end
    @property
    def close_grid_markup_start(self): return self._close_grid_markup_start
    @property
    def close_grid_qty_pct(self): return self._close_grid_qty_pct
    @property
    def close_trailing_grid_ratio(self): return self._close_trailing_grid_ratio
    @property
    def close_trailing_qty_pct(self): return self._close_trailing_qty_pct
    @property
    def close_trailing_retracement_pct(self): return self._close_trailing_retracement_pct
    @property
    def close_trailing_threshold_pct(self): return self._close_trailing_threshold_pct
    @property
    def ema_span_0(self): return self._ema_span_0
    @property
    def ema_span_1(self): return self._ema_span_1
    @property
    def entry_grid_double_down_factor(self): return self._entry_grid_double_down_factor
    @property
    def entry_grid_spacing_volatility_weight(self): return self._entry_grid_spacing_volatility_weight
    @property
    def entry_grid_spacing_pct(self): return self._entry_grid_spacing_pct
    @property
    def entry_grid_spacing_we_weight(self): return self._entry_grid_spacing_we_weight
    @property
    def entry_initial_ema_dist(self): return self._entry_initial_ema_dist
    @property
    def entry_initial_qty_pct(self): return self._entry_initial_qty_pct
    @property
    def entry_trailing_double_down_factor(self): return self._entry_trailing_double_down_factor
    @property
    def entry_trailing_grid_ratio(self): return self._entry_trailing_grid_ratio
    @property
    def entry_trailing_retracement_pct(self): return self._entry_trailing_retracement_pct
    @property
    def entry_trailing_threshold_pct(self): return self._entry_trailing_threshold_pct
    @property
    def entry_trailing_threshold_we_weight(self): return self._entry_trailing_threshold_we_weight
    @property
    def entry_trailing_threshold_volatility_weight(self): return self._entry_trailing_threshold_volatility_weight
    @property
    def entry_trailing_retracement_we_weight(self): return self._entry_trailing_retracement_we_weight
    @property
    def entry_trailing_retracement_volatility_weight(self): return self._entry_trailing_retracement_volatility_weight
    @property
    def entry_volatility_ema_span_hours(self): return self._entry_volatility_ema_span_hours
    @property
    def filter_volatility_ema_span(self): return self._filter_volatility_ema_span
    @property
    def filter_volatility_drop_pct(self): return self._filter_volatility_drop_pct
    @property
    def filter_volume_drop_pct(self): return self._filter_volume_drop_pct
    @property
    def filter_volume_ema_span(self): return self._filter_volume_ema_span
    @property
    def n_positions(self): return self._n_positions
    @property
    def total_wallet_exposure_limit(self): return self._total_wallet_exposure_limit
    @property
    def unstuck_close_pct(self): return self._unstuck_close_pct
    @property
    def unstuck_ema_dist(self): return self._unstuck_ema_dist
    @property
    def unstuck_loss_allowance_pct(self): return self._unstuck_loss_allowance_pct
    @property
    def unstuck_threshold(self): return self._unstuck_threshold
    @property
    def risk_wel_enforcer_threshold(self): return self._risk_wel_enforcer_threshold
    @property
    def risk_we_excess_allowance_pct(self): return self._risk_we_excess_allowance_pct
    @property
    def risk_twel_enforcer_threshold(self): return self._risk_twel_enforcer_threshold

    @close_grid_markup_end.setter
    def close_grid_markup_end(self, new_close_grid_markup_end):
        self._close_grid_markup_end = new_close_grid_markup_end
        self._long["close_grid_markup_end"] = self._close_grid_markup_end
    @close_grid_markup_start.setter
    def close_grid_markup_start(self, new_close_grid_markup_start):
        self._close_grid_markup_start = new_close_grid_markup_start
        self._long["close_grid_markup_start"] = self._close_grid_markup_start
    @close_grid_qty_pct.setter
    def close_grid_qty_pct(self, new_close_grid_qty_pct):
        self._close_grid_qty_pct = new_close_grid_qty_pct
        self._long["close_grid_qty_pct"] = self._close_grid_qty_pct
    @close_trailing_grid_ratio.setter
    def close_trailing_grid_ratio(self, new_close_trailing_grid_ratio):
        self._close_trailing_grid_ratio = new_close_trailing_grid_ratio
        self._long["close_trailing_grid_ratio"] = self._close_trailing_grid_ratio
    @close_trailing_qty_pct.setter
    def close_trailing_qty_pct(self, new_close_trailing_qty_pct):
        self._close_trailing_qty_pct = new_close_trailing_qty_pct
        self._long["close_trailing_qty_pct"] = self._close_trailing_qty_pct
    @close_trailing_retracement_pct.setter
    def close_trailing_retracement_pct(self, new_close_trailing_retracement_pct):
        self._close_trailing_retracement_pct = new_close_trailing_retracement_pct
        self._long["close_trailing_retracement_pct"] = self._close_trailing_retracement_pct
    @close_trailing_threshold_pct.setter
    def close_trailing_threshold_pct(self, new_close_trailing_threshold_pct):
        self._close_trailing_threshold_pct = new_close_trailing_threshold_pct
        self._long["close_trailing_threshold_pct"] = self._close_trailing_threshold_pct
    @ema_span_0.setter
    def ema_span_0(self, new_ema_span_0):
        self._ema_span_0 = new_ema_span_0
        self._long["ema_span_0"] = self._ema_span_0
    @ema_span_1.setter
    def ema_span_1(self, new_ema_span_1):
        self._ema_span_1 = new_ema_span_1
        self._long["ema_span_1"] = self._ema_span_1
    @entry_grid_double_down_factor.setter
    def entry_grid_double_down_factor(self, new_entry_grid_double_down_factor):
        self._entry_grid_double_down_factor = new_entry_grid_double_down_factor
        self._long["entry_grid_double_down_factor"] = self._entry_grid_double_down_factor
    @entry_grid_spacing_volatility_weight.setter
    def entry_grid_spacing_volatility_weight(self, new_entry_grid_spacing_volatility_weight):
        self._entry_grid_spacing_volatility_weight = new_entry_grid_spacing_volatility_weight
        self._long["entry_grid_spacing_volatility_weight"] = self._entry_grid_spacing_volatility_weight
    @entry_grid_spacing_pct.setter
    def entry_grid_spacing_pct(self, new_entry_grid_spacing_pct):
        self._entry_grid_spacing_pct = new_entry_grid_spacing_pct
        self._long["entry_grid_spacing_pct"] = self._entry_grid_spacing_pct
    @entry_grid_spacing_we_weight.setter
    def entry_grid_spacing_we_weight(self, new_entry_grid_spacing_we_weight):
        self._entry_grid_spacing_we_weight = new_entry_grid_spacing_we_weight
        self._long["entry_grid_spacing_we_weight"] = self._entry_grid_spacing_we_weight
    @entry_initial_ema_dist.setter
    def entry_initial_ema_dist(self, new_entry_initial_ema_dist):
        self._entry_initial_ema_dist = new_entry_initial_ema_dist
        self._long["entry_initial_ema_dist"] = self._entry_initial_ema_dist
    @entry_initial_qty_pct.setter
    def entry_initial_qty_pct(self, new_entry_initial_qty_pct):
        self._entry_initial_qty_pct = new_entry_initial_qty_pct
        self._long["entry_initial_qty_pct"] = self._entry_initial_qty_pct
    @entry_trailing_double_down_factor.setter
    def entry_trailing_double_down_factor(self, new_entry_trailing_double_down_factor):
        self._entry_trailing_double_down_factor = new_entry_trailing_double_down_factor
        self._long["entry_trailing_double_down_factor"] = self._entry_trailing_double_down_factor
    @entry_trailing_grid_ratio.setter
    def entry_trailing_grid_ratio(self, new_entry_trailing_grid_ratio):
        self._entry_trailing_grid_ratio = new_entry_trailing_grid_ratio
        self._long["entry_trailing_grid_ratio"] = self._entry_trailing_grid_ratio
    @entry_trailing_retracement_pct.setter
    def entry_trailing_retracement_pct(self, new_entry_trailing_retracement_pct):
        self._entry_trailing_retracement_pct = new_entry_trailing_retracement_pct
        self._long["entry_trailing_retracement_pct"] = self._entry_trailing_retracement_pct
    @entry_trailing_threshold_pct.setter
    def entry_trailing_threshold_pct(self, new_entry_trailing_threshold_pct):
        self._entry_trailing_threshold_pct = new_entry_trailing_threshold_pct
        self._long["entry_trailing_threshold_pct"] = self._entry_trailing_threshold_pct
    @entry_trailing_threshold_we_weight.setter
    def entry_trailing_threshold_we_weight(self, new_entry_trailing_threshold_we_weight):
        self._entry_trailing_threshold_we_weight = new_entry_trailing_threshold_we_weight
        self._long["entry_trailing_threshold_we_weight"] = self._entry_trailing_threshold_we_weight
    @entry_trailing_threshold_volatility_weight.setter
    def entry_trailing_threshold_volatility_weight(self, new_entry_trailing_threshold_volatility_weight):
        self._entry_trailing_threshold_volatility_weight = new_entry_trailing_threshold_volatility_weight
        self._long["entry_trailing_threshold_volatility_weight"] = self._entry_trailing_threshold_volatility_weight
    @entry_trailing_retracement_we_weight.setter
    def entry_trailing_retracement_we_weight(self, new_entry_trailing_retracement_we_weight):
        self._entry_trailing_retracement_we_weight = new_entry_trailing_retracement_we_weight
        self._long["entry_trailing_retracement_we_weight"] = self._entry_trailing_retracement_we_weight
    @entry_trailing_retracement_volatility_weight.setter
    def entry_trailing_retracement_volatility_weight(self, new_entry_trailing_retracement_volatility_weight):
        self._entry_trailing_retracement_volatility_weight = new_entry_trailing_retracement_volatility_weight
        self._long["entry_trailing_retracement_volatility_weight"] = self._entry_trailing_retracement_volatility_weight
    @entry_volatility_ema_span_hours.setter
    def entry_volatility_ema_span_hours(self, new_entry_volatility_ema_span_hours):
        self._entry_volatility_ema_span_hours = new_entry_volatility_ema_span_hours
        self._long["entry_volatility_ema_span_hours"] = self._entry_volatility_ema_span_hours
    @filter_volatility_ema_span.setter
    def filter_volatility_ema_span(self, new_filter_volatility_ema_span):
        self._filter_volatility_ema_span = new_filter_volatility_ema_span
        self._long["filter_volatility_ema_span"] = self._filter_volatility_ema_span
    @filter_volatility_drop_pct.setter
    def filter_volatility_drop_pct(self, new_filter_volatility_drop_pct):
        self._filter_volatility_drop_pct = new_filter_volatility_drop_pct
        self._long["filter_volatility_drop_pct"] = self._filter_volatility_drop_pct
    @filter_volume_drop_pct.setter
    def filter_volume_drop_pct(self, new_filter_volume_drop_pct):
        self._filter_volume_drop_pct = new_filter_volume_drop_pct
        self._long["filter_volume_drop_pct"] = self._filter_volume_drop_pct
    @filter_volume_ema_span.setter
    def filter_volume_ema_span(self, new_filter_volume_ema_span):
        self._filter_volume_ema_span = new_filter_volume_ema_span
        self._long["filter_volume_ema_span"] = self._filter_volume_ema_span
    @n_positions.setter
    def n_positions(self, new_n_positions):
        self._n_positions = new_n_positions
        self._long["n_positions"] = self._n_positions
    @total_wallet_exposure_limit.setter
    def total_wallet_exposure_limit(self, new_total_wallet_exposure_limit):
        self._total_wallet_exposure_limit = new_total_wallet_exposure_limit
        self._long["total_wallet_exposure_limit"] = self._total_wallet_exposure_limit
    @unstuck_close_pct.setter
    def unstuck_close_pct(self, new_unstuck_close_pct):
        self._unstuck_close_pct = new_unstuck_close_pct
        self._long["unstuck_close_pct"] = self._unstuck_close_pct
    @unstuck_ema_dist.setter
    def unstuck_ema_dist(self, new_unstuck_ema_dist):
        self._unstuck_ema_dist = new_unstuck_ema_dist
        self._long["unstuck_ema_dist"] = self._unstuck_ema_dist
    @unstuck_loss_allowance_pct.setter
    def unstuck_loss_allowance_pct(self, new_unstuck_loss_allowance_pct):
        self._unstuck_loss_allowance_pct = new_unstuck_loss_allowance_pct
        self._long["unstuck_loss_allowance_pct"] = self._unstuck_loss_allowance_pct
    @unstuck_threshold.setter
    def unstuck_threshold(self, new_unstuck_threshold):
        self._unstuck_threshold = new_unstuck_threshold
        self._long["unstuck_threshold"] = self._unstuck_threshold

    @risk_wel_enforcer_threshold.setter
    def risk_wel_enforcer_threshold(self, new_risk_wel_enforcer_threshold):
        self._risk_wel_enforcer_threshold = new_risk_wel_enforcer_threshold
        self._long["risk_wel_enforcer_threshold"] = self._risk_wel_enforcer_threshold

    @risk_we_excess_allowance_pct.setter
    def risk_we_excess_allowance_pct(self, new_risk_we_excess_allowance_pct):
        self._risk_we_excess_allowance_pct = new_risk_we_excess_allowance_pct
        self._long["risk_we_excess_allowance_pct"] = self._risk_we_excess_allowance_pct

    @risk_twel_enforcer_threshold.setter
    def risk_twel_enforcer_threshold(self, new_risk_twel_enforcer_threshold):
        self._risk_twel_enforcer_threshold = new_risk_twel_enforcer_threshold
        self._long["risk_twel_enforcer_threshold"] = self._risk_twel_enforcer_threshold

class Short:
    def __init__(self):
        self._close_grid_markup_end = 0.0089
        self._close_grid_markup_start = 0.0344
        self._close_grid_qty_pct = 0.125
        self._close_trailing_grid_ratio = 0.5
        self._close_trailing_qty_pct = 0.125
        self._close_trailing_retracement_pct = 0.002
        self._close_trailing_threshold_pct = 0.008
        self._ema_span_0 = 1318.0
        self._ema_span_1 = 1435.0
        self._entry_grid_double_down_factor = 0.894
        self._entry_grid_spacing_volatility_weight = 0.0
        self._entry_grid_spacing_pct = 0.04
        self._entry_grid_spacing_we_weight = 0.697
        self._entry_initial_ema_dist = -0.00738
        self._entry_initial_qty_pct = 0.00592
        self._entry_trailing_double_down_factor = 0.894
        self._entry_trailing_grid_ratio = 0.5
        self._entry_trailing_retracement_pct = 0.01
        self._entry_trailing_threshold_pct = 0.05
        self._entry_trailing_threshold_we_weight = 0.0
        self._entry_trailing_threshold_volatility_weight = 0.0
        self._entry_trailing_retracement_we_weight = 0.0
        self._entry_trailing_retracement_volatility_weight = 0.0
        self._entry_volatility_ema_span_hours = 72
        self._filter_volatility_ema_span = 60.0
        self._filter_volatility_drop_pct = 0.0
        self._filter_volume_drop_pct = 0.95
        self._filter_volume_ema_span = 60.0
        self._n_positions = 0.0
        self._total_wallet_exposure_limit = 0.0
        self._unstuck_close_pct = 0.001
        self._unstuck_ema_dist = 0.0
        self._unstuck_loss_allowance_pct = 0.03
        self._unstuck_threshold = 0.916
        self._risk_wel_enforcer_threshold = 1.0
        self._risk_we_excess_allowance_pct = 0.0
        self._risk_twel_enforcer_threshold = 1.0
        self._short = {
            "close_grid_markup_end": self._close_grid_markup_end,
            "close_grid_markup_start": self._close_grid_markup_start,
            "close_grid_qty_pct": self._close_grid_qty_pct,
            "close_trailing_grid_ratio": self._close_trailing_grid_ratio,
            "close_trailing_qty_pct": self._close_trailing_qty_pct,
            "close_trailing_retracement_pct": self._close_trailing_retracement_pct,
            "close_trailing_threshold_pct": self._close_trailing_threshold_pct,
            "ema_span_0": self._ema_span_0,
            "ema_span_1": self._ema_span_1,
            "entry_grid_double_down_factor": self._entry_grid_double_down_factor,
            "entry_grid_spacing_volatility_weight": self._entry_grid_spacing_volatility_weight,
            "entry_grid_spacing_pct": self._entry_grid_spacing_pct,
            "entry_grid_spacing_we_weight": self._entry_grid_spacing_we_weight,
            "entry_initial_ema_dist": self._entry_initial_ema_dist,
            "entry_initial_qty_pct": self._entry_initial_qty_pct,
            "entry_trailing_double_down_factor": self._entry_trailing_double_down_factor,
            "entry_trailing_grid_ratio": self._entry_trailing_grid_ratio,
            "entry_trailing_retracement_pct": self._entry_trailing_retracement_pct,
            "entry_trailing_threshold_pct": self._entry_trailing_threshold_pct,
            "entry_trailing_threshold_we_weight": self._entry_trailing_threshold_we_weight,
            "entry_trailing_threshold_volatility_weight": self._entry_trailing_threshold_volatility_weight,
            "entry_trailing_retracement_we_weight": self._entry_trailing_retracement_we_weight,
            "entry_trailing_retracement_volatility_weight": self._entry_trailing_retracement_volatility_weight,
            "entry_volatility_ema_span_hours": self._entry_volatility_ema_span_hours,
            "filter_volatility_ema_span": self._filter_volatility_ema_span,
            "filter_volatility_drop_pct": self._filter_volatility_drop_pct,
            "filter_volume_drop_pct": self._filter_volume_drop_pct,
            "filter_volume_ema_span": self._filter_volume_ema_span,
            "n_positions": self._n_positions,
            "total_wallet_exposure_limit": self._total_wallet_exposure_limit,
            "unstuck_close_pct": self._unstuck_close_pct,
            "unstuck_ema_dist": self._unstuck_ema_dist,
            "unstuck_loss_allowance_pct": self._unstuck_loss_allowance_pct,
            "unstuck_threshold": self._unstuck_threshold,
            "risk_wel_enforcer_threshold": self._risk_wel_enforcer_threshold,
            "risk_we_excess_allowance_pct": self._risk_we_excess_allowance_pct,
            "risk_twel_enforcer_threshold": self._risk_twel_enforcer_threshold
        }

    def __repr__(self):
        return str(self._short)

    @property
    def short(self): return self._short
    @short.setter
    def short(self, new_short):
        #Fix for old markup parameters
        if "close_grid_markup_range" in new_short and "close_grid_min_markup" in new_short:
            self.close_grid_markup_start = new_short["close_grid_min_markup"] + new_short["close_grid_markup_range"]
            self.close_grid_markup_end = new_short["close_grid_min_markup"]
        if "close_grid_markup_end" in new_short:
            self.close_grid_markup_end = new_short["close_grid_markup_end"]
        if "close_grid_markup_start" in new_short:
            self.close_grid_markup_start = new_short["close_grid_markup_start"]
        if "close_grid_qty_pct" in new_short:
            self.close_grid_qty_pct = new_short["close_grid_qty_pct"]
        if "close_trailing_grid_ratio" in new_short:
            self.close_trailing_grid_ratio = new_short["close_trailing_grid_ratio"]
        if "close_trailing_qty_pct" in new_short:
            self.close_trailing_qty_pct = new_short["close_trailing_qty_pct"]
        if "close_trailing_retracement_pct" in new_short:
            self.close_trailing_retracement_pct = new_short["close_trailing_retracement_pct"]
        if "close_trailing_threshold_pct" in new_short:
            self.close_trailing_threshold_pct = new_short["close_trailing_threshold_pct"]
        if "ema_span_0" in new_short:
            self.ema_span_0 = new_short["ema_span_0"]
        if "ema_span_1" in new_short:
            self.ema_span_1 = new_short["ema_span_1"]
        if "entry_grid_double_down_factor" in new_short:
            self.entry_grid_double_down_factor = new_short["entry_grid_double_down_factor"]
        if "entry_grid_spacing_volatility_weight" in new_short:
            self.entry_grid_spacing_volatility_weight = new_short["entry_grid_spacing_volatility_weight"]
        # Fix for old configs
        elif "entry_grid_spacing_log_weight" in new_short:
            self.entry_grid_spacing_volatility_weight = new_short["entry_grid_spacing_log_weight"]
        if "entry_grid_spacing_pct" in new_short:
            self.entry_grid_spacing_pct = new_short["entry_grid_spacing_pct"]
        if "entry_grid_spacing_we_weight" in new_short:
            self.entry_grid_spacing_we_weight = new_short["entry_grid_spacing_we_weight"]
        # Fix for old configs
        elif "entry_grid_spacing_weight" in new_short:
            self.entry_grid_spacing_we_weight = new_short["entry_grid_spacing_weight"]
        if "entry_initial_ema_dist" in new_short:
            self.entry_initial_ema_dist = new_short["entry_initial_ema_dist"]
        if "entry_initial_qty_pct" in new_short:
            self.entry_initial_qty_pct = new_short["entry_initial_qty_pct"]
        if "entry_trailing_double_down_factor" in new_short:
            self.entry_trailing_double_down_factor = new_short["entry_trailing_double_down_factor"]
        else:
            self.entry_trailing_double_down_factor = self.entry_grid_double_down_factor
        if "entry_trailing_grid_ratio" in new_short:
            self.entry_trailing_grid_ratio = new_short["entry_trailing_grid_ratio"]
        if "entry_trailing_retracement_pct" in new_short:
            self.entry_trailing_retracement_pct = new_short["entry_trailing_retracement_pct"]
        if "entry_trailing_threshold_pct" in new_short:
            self.entry_trailing_threshold_pct = new_short["entry_trailing_threshold_pct"]
        if "entry_trailing_threshold_we_weight" in new_short:
            self.entry_trailing_threshold_we_weight = new_short["entry_trailing_threshold_we_weight"]
        if "entry_trailing_threshold_volatility_weight" in new_short:
            self.entry_trailing_threshold_volatility_weight = new_short["entry_trailing_threshold_volatility_weight"]
        if "entry_trailing_retracement_we_weight" in new_short:
            self.entry_trailing_retracement_we_weight = new_short["entry_trailing_retracement_we_weight"]
        if "entry_trailing_retracement_volatility_weight" in new_short:
            self.entry_trailing_retracement_volatility_weight = new_short["entry_trailing_retracement_volatility_weight"]
        if "entry_volatility_ema_span_hours" in new_short:
            self.entry_volatility_ema_span_hours = new_short["entry_volatility_ema_span_hours"]
        # Fix for old configs
        elif "entry_grid_spacing_log_span_hours" in new_short:
            self.entry_volatility_ema_span_hours = new_short["entry_grid_spacing_log_span_hours"]
        if "filter_volatility_ema_span" in new_short:
            self.filter_volatility_ema_span = new_short["filter_volatility_ema_span"]
        # Fix for old configs
        elif "filter_log_range_ema_span" in new_short:
            self.filter_volatility_ema_span = new_short["filter_log_range_ema_span"]
        # Fix for old configs
        elif "filter_noisiness_rolling_window" in new_short:
            self.filter_log_range_ema_span = new_short["filter_noisiness_rolling_window"]
        elif "filter_rolling_window" in new_short:
            self.filter_log_range_ema_span = new_short["filter_rolling_window"]
        if "filter_volatility_drop_pct" in new_short:
            self.filter_volatility_drop_pct = new_short["filter_volatility_drop_pct"]
        if "filter_volume_drop_pct" in new_short:
            self.filter_volume_drop_pct = new_short["filter_volume_drop_pct"]
        # Fix for old configs
        elif "filter_relative_volume_clip_pct" in new_short:
            self.filter_volume_drop_pct = new_short["filter_relative_volume_clip_pct"]
        if "filter_volume_ema_span" in new_short:
            self.filter_volume_ema_span = new_short["filter_volume_ema_span"]
        # Fix for old configs
        elif "filter_rolling_window" in new_short:
            self.filter_volume_ema_span = new_short["filter_rolling_window"]
        if "n_positions" in new_short:
            self.n_positions = new_short["n_positions"]
        if "total_wallet_exposure_limit" in new_short:
            self.total_wallet_exposure_limit = new_short["total_wallet_exposure_limit"]
        if "unstuck_close_pct" in new_short:
            self.unstuck_close_pct = new_short["unstuck_close_pct"]
        if "unstuck_ema_dist" in new_short:
            self.unstuck_ema_dist = new_short["unstuck_ema_dist"]
        if "unstuck_loss_allowance_pct" in new_short:
            self.unstuck_loss_allowance_pct = new_short["unstuck_loss_allowance_pct"]
        if "unstuck_threshold" in new_short:
            self.unstuck_threshold = new_short["unstuck_threshold"]
        if "risk_wel_enforcer_threshold" in new_short:
            self.risk_wel_enforcer_threshold = new_short["risk_wel_enforcer_threshold"]
        if "risk_we_excess_allowance_pct" in new_short:
            self.risk_we_excess_allowance_pct = new_short["risk_we_excess_allowance_pct"]
        if "risk_twel_enforcer_threshold" in new_short:
            self.risk_twel_enforcer_threshold = new_short["risk_twel_enforcer_threshold"]

    @property
    def close_grid_markup_end(self): return self._close_grid_markup_end
    @property
    def close_grid_markup_start(self): return self._close_grid_markup_start
    @property
    def close_grid_qty_pct(self): return self._close_grid_qty_pct
    @property
    def close_trailing_grid_ratio(self): return self._close_trailing_grid_ratio
    @property
    def close_trailing_qty_pct(self): return self._close_trailing_qty_pct
    @property
    def close_trailing_retracement_pct(self): return self._close_trailing_retracement_pct
    @property
    def close_trailing_threshold_pct(self): return self._close_trailing_threshold_pct
    @property
    def ema_span_0(self): return self._ema_span_0
    @property
    def ema_span_1(self): return self._ema_span_1
    @property
    def entry_grid_double_down_factor(self): return self._entry_grid_double_down_factor
    @property
    def entry_grid_spacing_volatility_weight(self): return self._entry_grid_spacing_volatility_weight
    @property
    def entry_grid_spacing_pct(self): return self._entry_grid_spacing_pct
    @property
    def entry_grid_spacing_we_weight(self): return self._entry_grid_spacing_we_weight
    @property
    def entry_initial_ema_dist(self): return self._entry_initial_ema_dist
    @property
    def entry_initial_qty_pct(self): return self._entry_initial_qty_pct
    @property
    def entry_trailing_double_down_factor(self): return self._entry_trailing_double_down_factor
    @property
    def entry_trailing_grid_ratio(self): return self._entry_trailing_grid_ratio
    @property
    def entry_trailing_retracement_pct(self): return self._entry_trailing_retracement_pct
    @property
    def entry_trailing_threshold_pct(self): return self._entry_trailing_threshold_pct
    @property
    def entry_trailing_threshold_we_weight(self): return self._entry_trailing_threshold_we_weight
    @property
    def entry_trailing_threshold_volatility_weight(self): return self._entry_trailing_threshold_volatility_weight
    @property
    def entry_trailing_retracement_we_weight(self): return self._entry_trailing_retracement_we_weight
    @property
    def entry_trailing_retracement_volatility_weight(self): return self._entry_trailing_retracement_volatility_weight
    @property
    def entry_volatility_ema_span_hours(self): return self._entry_volatility_ema_span_hours
    @property
    def filter_volatility_ema_span(self): return self._filter_volatility_ema_span
    @property
    def filter_volatility_drop_pct(self): return self._filter_volatility_drop_pct
    @property
    def filter_volume_drop_pct(self): return self._filter_volume_drop_pct
    @property
    def filter_volume_ema_span(self): return self._filter_volume_ema_span
    @property
    def n_positions(self): return self._n_positions
    @property
    def total_wallet_exposure_limit(self): return self._total_wallet_exposure_limit
    @property
    def unstuck_close_pct(self): return self._unstuck_close_pct
    @property
    def unstuck_ema_dist(self): return self._unstuck_ema_dist
    @property
    def unstuck_loss_allowance_pct(self): return self._unstuck_loss_allowance_pct
    @property
    def unstuck_threshold(self): return self._unstuck_threshold
    @property
    def risk_wel_enforcer_threshold(self): return self._risk_wel_enforcer_threshold
    @property
    def risk_we_excess_allowance_pct(self): return self._risk_we_excess_allowance_pct
    @property
    def risk_twel_enforcer_threshold(self): return self._risk_twel_enforcer_threshold

    @close_grid_markup_end.setter
    def close_grid_markup_end(self, new_close_grid_markup_end):
        self._close_grid_markup_end = new_close_grid_markup_end
        self._short["close_grid_markup_end"] = self._close_grid_markup_end
    @close_grid_markup_start.setter
    def close_grid_markup_start(self, new_close_grid_markup_start):
        self._close_grid_markup_start = new_close_grid_markup_start
        self._short["close_grid_markup_start"] = self._close_grid_markup_start
    @close_grid_qty_pct.setter
    def close_grid_qty_pct(self, new_close_grid_qty_pct):
        self._close_grid_qty_pct = new_close_grid_qty_pct
        self._short["close_grid_qty_pct"] = self._close_grid_qty_pct
    @close_trailing_grid_ratio.setter
    def close_trailing_grid_ratio(self, new_close_trailing_grid_ratio):
        self._close_trailing_grid_ratio = new_close_trailing_grid_ratio
        self._short["close_trailing_grid_ratio"] = self._close_trailing_grid_ratio
    @close_trailing_qty_pct.setter
    def close_trailing_qty_pct(self, new_close_trailing_qty_pct):
        self._close_trailing_qty_pct = new_close_trailing_qty_pct
        self._short["close_trailing_qty_pct"] = self._close_trailing_qty_pct
    @close_trailing_retracement_pct.setter
    def close_trailing_retracement_pct(self, new_close_trailing_retracement_pct):
        self._close_trailing_retracement_pct = new_close_trailing_retracement_pct
        self._short["close_trailing_retracement_pct"] = self._close_trailing_retracement_pct
    @close_trailing_threshold_pct.setter
    def close_trailing_threshold_pct(self, new_close_trailing_threshold_pct):
        self._close_trailing_threshold_pct = new_close_trailing_threshold_pct
        self._short["close_trailing_threshold_pct"] = self._close_trailing_threshold_pct
    @ema_span_0.setter
    def ema_span_0(self, new_ema_span_0):
        self._ema_span_0 = new_ema_span_0
        self._short["ema_span_0"] = self._ema_span_0
    @ema_span_1.setter
    def ema_span_1(self, new_ema_span_1):
        self._ema_span_1 = new_ema_span_1
        self._short["ema_span_1"] = self._ema_span_1
    @entry_grid_double_down_factor.setter
    def entry_grid_double_down_factor(self, new_entry_grid_double_down_factor):
        self._entry_grid_double_down_factor = new_entry_grid_double_down_factor
        self._short["entry_grid_double_down_factor"] = self._entry_grid_double_down_factor
    @entry_grid_spacing_volatility_weight.setter
    def entry_grid_spacing_volatility_weight(self, new_entry_grid_spacing_volatility_weight):
        self._entry_grid_spacing_volatility_weight = new_entry_grid_spacing_volatility_weight
        self._short["entry_grid_spacing_volatility_weight"] = self._entry_grid_spacing_volatility_weight
    @entry_grid_spacing_pct.setter
    def entry_grid_spacing_pct(self, new_entry_grid_spacing_pct):
        self._entry_grid_spacing_pct = new_entry_grid_spacing_pct
        self._short["entry_grid_spacing_pct"] = self._entry_grid_spacing_pct
    @entry_grid_spacing_we_weight.setter
    def entry_grid_spacing_we_weight(self, new_entry_grid_spacing_we_weight):
        self._entry_grid_spacing_we_weight = new_entry_grid_spacing_we_weight
        self._short["entry_grid_spacing_we_weight"] = self._entry_grid_spacing_we_weight
    @entry_initial_ema_dist.setter
    def entry_initial_ema_dist(self, new_entry_initial_ema_dist):
        self._entry_initial_ema_dist = new_entry_initial_ema_dist
        self._short["entry_initial_ema_dist"] = self._entry_initial_ema_dist
    @entry_initial_qty_pct.setter
    def entry_initial_qty_pct(self, new_entry_initial_qty_pct):
        self._entry_initial_qty_pct = new_entry_initial_qty_pct
        self._short["entry_initial_qty_pct"] = self._entry_initial_qty_pct
    @entry_trailing_double_down_factor.setter
    def entry_trailing_double_down_factor(self, new_entry_trailing_double_down_factor):
        self._entry_trailing_double_down_factor = new_entry_trailing_double_down_factor
        self._short["entry_trailing_double_down_factor"] = self._entry_trailing_double_down_factor
    @entry_trailing_grid_ratio.setter
    def entry_trailing_grid_ratio(self, new_entry_trailing_grid_ratio):
        self._entry_trailing_grid_ratio = new_entry_trailing_grid_ratio
        self._short["entry_trailing_grid_ratio"] = self._entry_trailing_grid_ratio
    @entry_trailing_retracement_pct.setter
    def entry_trailing_retracement_pct(self, new_entry_trailing_retracement_pct):
        self._entry_trailing_retracement_pct = new_entry_trailing_retracement_pct
        self._short["entry_trailing_retracement_pct"] = self._entry_trailing_retracement_pct
    @entry_trailing_threshold_pct.setter
    def entry_trailing_threshold_pct(self, new_entry_trailing_threshold_pct):
        self._entry_trailing_threshold_pct = new_entry_trailing_threshold_pct
        self._short["entry_trailing_threshold_pct"] = self._entry_trailing_threshold_pct
    @entry_trailing_threshold_we_weight.setter
    def entry_trailing_threshold_we_weight(self, new_entry_trailing_threshold_we_weight):
        self._entry_trailing_threshold_we_weight = new_entry_trailing_threshold_we_weight
        self._short["entry_trailing_threshold_we_weight"] = self._entry_trailing_threshold_we_weight
    @entry_trailing_threshold_volatility_weight.setter
    def entry_trailing_threshold_volatility_weight(self, new_entry_trailing_threshold_volatility_weight):
        self._entry_trailing_threshold_volatility_weight = new_entry_trailing_threshold_volatility_weight
        self._short["entry_trailing_threshold_volatility_weight"] = self._entry_trailing_threshold_volatility_weight
    @entry_trailing_retracement_we_weight.setter
    def entry_trailing_retracement_we_weight(self, new_entry_trailing_retracement_we_weight):
        self._entry_trailing_retracement_we_weight = new_entry_trailing_retracement_we_weight
        self._short["entry_trailing_retracement_we_weight"] = self._entry_trailing_retracement_we_weight
    @entry_trailing_retracement_volatility_weight.setter
    def entry_trailing_retracement_volatility_weight(self, new_entry_trailing_retracement_volatility_weight):
        self._entry_trailing_retracement_volatility_weight = new_entry_trailing_retracement_volatility_weight
        self._short["entry_trailing_retracement_volatility_weight"] = self._entry_trailing_retracement_volatility_weight
    @entry_volatility_ema_span_hours.setter
    def entry_volatility_ema_span_hours(self, new_entry_volatility_ema_span_hours):
        self._entry_volatility_ema_span_hours = new_entry_volatility_ema_span_hours
        self._short["entry_volatility_ema_span_hours"] = self._entry_volatility_ema_span_hours
    @filter_volatility_ema_span.setter
    def filter_volatility_ema_span(self, new_filter_volatility_ema_span):
        self._filter_volatility_ema_span = new_filter_volatility_ema_span
        self._short["filter_volatility_ema_span"] = self._filter_volatility_ema_span
    @filter_volatility_drop_pct.setter
    def filter_volatility_drop_pct(self, new_filter_volatility_drop_pct):
        self._filter_volatility_drop_pct = new_filter_volatility_drop_pct
        self._short["filter_volatility_drop_pct"] = self._filter_volatility_drop_pct
    @filter_volume_drop_pct.setter
    def filter_volume_drop_pct(self, new_filter_volume_drop_pct):
        self._filter_volume_drop_pct = new_filter_volume_drop_pct
        self._short["filter_volume_drop_pct"] = self._filter_volume_drop_pct
    @filter_volume_ema_span.setter
    def filter_volume_ema_span(self, new_filter_volume_ema_span):
        self._filter_volume_ema_span = new_filter_volume_ema_span
        self._short["filter_volume_ema_span"] = self._filter_volume_ema_span
    @n_positions.setter
    def n_positions(self, new_n_positions):
        self._n_positions = new_n_positions
        self._short["n_positions"] = self._n_positions
    @total_wallet_exposure_limit.setter
    def total_wallet_exposure_limit(self, new_total_wallet_exposure_limit):
        self._total_wallet_exposure_limit = new_total_wallet_exposure_limit
        self._short["total_wallet_exposure_limit"] = self._total_wallet_exposure_limit
    @unstuck_close_pct.setter
    def unstuck_close_pct(self, new_unstuck_close_pct):
        self._unstuck_close_pct = new_unstuck_close_pct
        self._short["unstuck_close_pct"] = self._unstuck_close_pct
    @unstuck_ema_dist.setter
    def unstuck_ema_dist(self, new_unstuck_ema_dist):
        self._unstuck_ema_dist = new_unstuck_ema_dist
        self._short["unstuck_ema_dist"] = self._unstuck_ema_dist
    @unstuck_loss_allowance_pct.setter
    def unstuck_loss_allowance_pct(self, new_unstuck_loss_allowance_pct):
        self._unstuck_loss_allowance_pct = new_unstuck_loss_allowance_pct
        self._short["unstuck_loss_allowance_pct"] = self._unstuck_loss_allowance_pct
    @unstuck_threshold.setter
    def unstuck_threshold(self, new_unstuck_threshold):
        self._unstuck_threshold = new_unstuck_threshold
        self._short["unstuck_threshold"] = self._unstuck_threshold

    @risk_wel_enforcer_threshold.setter
    def risk_wel_enforcer_threshold(self, new_risk_wel_enforcer_threshold):
        self._risk_wel_enforcer_threshold = new_risk_wel_enforcer_threshold
        self._short["risk_wel_enforcer_threshold"] = self._risk_wel_enforcer_threshold

    @risk_we_excess_allowance_pct.setter
    def risk_we_excess_allowance_pct(self, new_risk_we_excess_allowance_pct):
        self._risk_we_excess_allowance_pct = new_risk_we_excess_allowance_pct
        self._short["risk_we_excess_allowance_pct"] = self._risk_we_excess_allowance_pct

    @risk_twel_enforcer_threshold.setter
    def risk_twel_enforcer_threshold(self, new_risk_twel_enforcer_threshold):
        self._risk_twel_enforcer_threshold = new_risk_twel_enforcer_threshold
        self._short["risk_twel_enforcer_threshold"] = self._risk_twel_enforcer_threshold

class ApprovedCoins:
    def __init__(self):
        self._long = []
        self._short = []
        self._approved_coins = {
            "long": self._long,
            "short": self._short
        }

    def __repr__(self):
        return str(self._approved_coins)
    
    @property
    def approved_coins(self): return self._approved_coins
    @approved_coins.setter
    def approved_coins(self, new_approved_coins):
        if "long" in new_approved_coins:
            self.long = new_approved_coins["long"]
        else:
            self.long = new_approved_coins
        if "short" in new_approved_coins:
            self.short = new_approved_coins["short"]
        else:
            self.short = new_approved_coins
    
    @property
    def long(self): return self._long
    @property
    def short(self): return self._short
    @long.setter
    def long(self, new_long):
        self._long = [str(coin).strip() for coin in new_long if str(coin).strip()]
        self._approved_coins["long"] = self._long
    @short.setter
    def short(self, new_short):
        self._short = [str(coin).strip() for coin in new_short if str(coin).strip()]
        self._approved_coins["short"] = self._short

class IgnoredCoins:
    def __init__(self):
        self._long = []
        self._short = []
        self._ignored_coins = {
            "long": self._long,
            "short": self._short
        }
    
    def __repr__(self):
        return str(self._ignored_coins)

    @property
    def ignored_coins(self): return self._ignored_coins
    @ignored_coins.setter
    def ignored_coins(self, new_ignored_coins):
        if "long" in new_ignored_coins:
            self.long = new_ignored_coins["long"]
        else:
            self.long = new_ignored_coins
        if "short" in new_ignored_coins:
            self.short = new_ignored_coins["short"]
        else:
            self.short = new_ignored_coins
    
    @property
    def long(self): return self._long
    @property
    def short(self): return self._short
    @long.setter
    def long(self, new_long):
        self._long = new_long
        self._ignored_coins["long"] = self._long
    @short.setter
    def short(self, new_short):
        self._short = new_short
        self._ignored_coins["short"] = self._short

class Live:
    def __init__(self):
        self._approved_coins = ApprovedCoins()
        self._auto_gs = True
        self._inactive_coin_candle_ttl_minutes = 10.0
        self._empty_means_all_approved = False
        self._execution_delay_seconds = 2.0
        self._filter_by_min_effective_cost = True
        self._forced_mode_long = ""
        self._forced_mode_short = ""
        # PB7 live defaults (see pb7/src/config_utils.py)
        self._hedge_mode = True
        self._ignored_coins = IgnoredCoins()
        self._leverage = 10.0
        self._market_orders_allowed = True
        self._max_disk_candles_per_symbol_per_tf = 2000000
        self._max_memory_candles_per_symbol = 20000
        self._max_n_cancellations_per_batch = 5
        self._max_n_creations_per_batch = 3
        self._max_n_restarts_per_day = 10
        self._minimum_coin_age_days = 30.0
        self._order_match_tolerance_pct = 0.0002
        self._pnls_max_lookback_days = 30.0
        self._price_distance_threshold = 0.002
        self._recv_window_ms = 5000
        self._time_in_force = "good_till_cancelled"
        self._warmup_jitter_seconds = 30.0
        self._warmup_ratio = 0.2
        self._warmup_concurrency = 0
        # PBGui uses 0 to mean unlimited (PB7 treats 0 like None)
        self._max_concurrent_api_requests = 0
        self._max_warmup_minutes = 0
        self._candle_lock_timeout_seconds = 10
        self._balance_override = None
        self._balance_hysteresis_snap_pct = 0.02
        self._enable_archive_candle_fetch = False
        self._max_ohlcv_fetches_per_minute = 30
        self._user = "bybit_01"

        self._live = {
            "approved_coins": self._approved_coins._approved_coins,
            "auto_gs": self._auto_gs,
            "inactive_coin_candle_ttl_minutes": self._inactive_coin_candle_ttl_minutes,
            "empty_means_all_approved": self._empty_means_all_approved,
            "execution_delay_seconds": self._execution_delay_seconds,
            "filter_by_min_effective_cost": self._filter_by_min_effective_cost,
            "forced_mode_long": self._forced_mode_long,
            "forced_mode_short": self._forced_mode_short,
            "hedge_mode": self._hedge_mode,
            "ignored_coins": self._ignored_coins._ignored_coins,
            "leverage": self._leverage,
            "market_orders_allowed": self._market_orders_allowed,
            "max_disk_candles_per_symbol_per_tf": self._max_disk_candles_per_symbol_per_tf,
            "max_memory_candles_per_symbol": self._max_memory_candles_per_symbol,
            "max_n_cancellations_per_batch": self._max_n_cancellations_per_batch,
            "max_n_creations_per_batch": self._max_n_creations_per_batch,
            "max_n_restarts_per_day": self._max_n_restarts_per_day,
            "minimum_coin_age_days": self._minimum_coin_age_days,
            "order_match_tolerance_pct": self._order_match_tolerance_pct,
            "pnls_max_lookback_days": self._pnls_max_lookback_days,
            "price_distance_threshold": self._price_distance_threshold,
            "recv_window_ms": self._recv_window_ms,
            "time_in_force": self._time_in_force,
            "warmup_jitter_seconds": self._warmup_jitter_seconds,
            "warmup_ratio": self._warmup_ratio,
            "warmup_concurrency": self._warmup_concurrency,
            "max_concurrent_api_requests": self._max_concurrent_api_requests,
            "max_warmup_minutes": self._max_warmup_minutes,
            "candle_lock_timeout_seconds": self._candle_lock_timeout_seconds,
            "balance_override": self._balance_override,
            "balance_hysteresis_snap_pct": self._balance_hysteresis_snap_pct,
            "enable_archive_candle_fetch": self._enable_archive_candle_fetch,
            "max_ohlcv_fetches_per_minute": self._max_ohlcv_fetches_per_minute,
            "user": self._user
        }
    
    def __repr__(self):
        return str(self._live)

    @property
    def live(self): return self._live
    @live.setter
    def live(self, new_live):
        if "approved_coins" in new_live:
            self.approved_coins = new_live["approved_coins"]
        if "auto_gs" in new_live:
            self.auto_gs = new_live["auto_gs"]
        if "inactive_coin_candle_ttl_minutes" in new_live:
            self.inactive_coin_candle_ttl_minutes = new_live["inactive_coin_candle_ttl_minutes"]
        if "empty_means_all_approved" in new_live:
            self.empty_means_all_approved = new_live["empty_means_all_approved"]
        if "execution_delay_seconds" in new_live:
            self.execution_delay_seconds = new_live["execution_delay_seconds"]
        if "filter_by_min_effective_cost" in new_live:
            self.filter_by_min_effective_cost = new_live["filter_by_min_effective_cost"]
        if "forced_mode_long" in new_live:
            self.forced_mode_long = new_live["forced_mode_long"]
        if "forced_mode_short" in new_live:
            self.forced_mode_short = new_live["forced_mode_short"]
        if "hedge_mode" in new_live:
            self.hedge_mode = new_live["hedge_mode"]
        if "ignored_coins" in new_live:
            self.ignored_coins = new_live["ignored_coins"]
        if "leverage" in new_live:
            self.leverage = new_live["leverage"]
        if "market_orders_allowed" in new_live:
            self.market_orders_allowed = new_live["market_orders_allowed"]
        if "max_disk_candles_per_symbol_per_tf" in new_live:
            self.max_disk_candles_per_symbol_per_tf = new_live["max_disk_candles_per_symbol_per_tf"]
        if "max_memory_candles_per_symbol" in new_live:
            self.max_memory_candles_per_symbol = new_live["max_memory_candles_per_symbol"]
        if "max_n_cancellations_per_batch" in new_live:
            self.max_n_cancellations_per_batch = new_live["max_n_cancellations_per_batch"]
        if "max_n_creations_per_batch" in new_live:
            self.max_n_creations_per_batch = new_live["max_n_creations_per_batch"]
        if "max_n_restarts_per_day" in new_live:
            self.max_n_restarts_per_day = new_live["max_n_restarts_per_day"]
        if "minimum_coin_age_days" in new_live:
            self.minimum_coin_age_days = new_live["minimum_coin_age_days"]
        if "order_match_tolerance_pct" in new_live:
            self.order_match_tolerance_pct = new_live["order_match_tolerance_pct"]
        if "pnls_max_lookback_days" in new_live:
            self.pnls_max_lookback_days = new_live["pnls_max_lookback_days"]
        if "price_distance_threshold" in new_live:
            self.price_distance_threshold = new_live["price_distance_threshold"]
        if "recv_window_ms" in new_live:
            self.recv_window_ms = new_live["recv_window_ms"]
        if "time_in_force" in new_live:
            self.time_in_force = new_live["time_in_force"]
        if "warmup_jitter_seconds" in new_live:
            self.warmup_jitter_seconds = new_live["warmup_jitter_seconds"]
        if "warmup_ratio" in new_live:
            self.warmup_ratio = new_live["warmup_ratio"]
        if "warmup_concurrency" in new_live:
            self.warmup_concurrency = new_live["warmup_concurrency"]
        if "max_concurrent_api_requests" in new_live:
            self.max_concurrent_api_requests = new_live["max_concurrent_api_requests"]
        if "max_warmup_minutes" in new_live:
            self.max_warmup_minutes = new_live["max_warmup_minutes"]
        if "candle_lock_timeout_seconds" in new_live:
            self.candle_lock_timeout_seconds = new_live["candle_lock_timeout_seconds"]
        if "balance_override" in new_live:
            self.balance_override = new_live["balance_override"]
        if "balance_hysteresis_snap_pct" in new_live:
            self.balance_hysteresis_snap_pct = new_live["balance_hysteresis_snap_pct"]
        if "enable_archive_candle_fetch" in new_live:
            self.enable_archive_candle_fetch = new_live["enable_archive_candle_fetch"]
        if "max_ohlcv_fetches_per_minute" in new_live:
            self.max_ohlcv_fetches_per_minute = new_live["max_ohlcv_fetches_per_minute"]
        if "user" in new_live:
            self.user = new_live["user"]
    
    @property
    def approved_coins(self): return self._approved_coins
    @property
    def auto_gs(self): return self._auto_gs
    @property
    def inactive_coin_candle_ttl_minutes(self): return self._inactive_coin_candle_ttl_minutes
    @property
    def empty_means_all_approved(self): return self._empty_means_all_approved
    @property
    def execution_delay_seconds(self): return self._execution_delay_seconds
    @property
    def filter_by_min_effective_cost(self): return self._filter_by_min_effective_cost
    @property
    def forced_mode_long(self): return self._forced_mode_long
    @property
    def forced_mode_short(self): return self._forced_mode_short
    @property
    def hedge_mode(self): return self._hedge_mode
    @property
    def ignored_coins(self): return self._ignored_coins
    @property
    def leverage(self): return self._leverage
    @property
    def market_orders_allowed(self): return self._market_orders_allowed
    @property
    def max_disk_candles_per_symbol_per_tf(self): return self._max_disk_candles_per_symbol_per_tf
    @property
    def max_memory_candles_per_symbol(self): return self._max_memory_candles_per_symbol
    @property
    def max_n_cancellations_per_batch(self): return self._max_n_cancellations_per_batch
    @property
    def max_n_creations_per_batch(self): return self._max_n_creations_per_batch
    @property
    def max_n_restarts_per_day(self): return self._max_n_restarts_per_day
    @property
    def minimum_coin_age_days(self): return self._minimum_coin_age_days
    @property
    def order_match_tolerance_pct(self): return self._order_match_tolerance_pct
    @property
    def pnls_max_lookback_days(self): return self._pnls_max_lookback_days
    @property
    def price_distance_threshold(self): return self._price_distance_threshold
    @property
    def recv_window_ms(self): return self._recv_window_ms
    @property
    def time_in_force(self): return self._time_in_force
    @property
    def warmup_jitter_seconds(self): return self._warmup_jitter_seconds
    @property
    def warmup_ratio(self): return self._warmup_ratio
    @property
    def warmup_concurrency(self): return self._warmup_concurrency
    @property
    def max_concurrent_api_requests(self): return self._max_concurrent_api_requests
    @property
    def max_warmup_minutes(self): return self._max_warmup_minutes
    @property
    def candle_lock_timeout_seconds(self): return self._candle_lock_timeout_seconds
    @property
    def balance_override(self): return self._balance_override
    @property
    def balance_hysteresis_snap_pct(self): return self._balance_hysteresis_snap_pct
    @property
    def enable_archive_candle_fetch(self): return self._enable_archive_candle_fetch
    @property
    def max_ohlcv_fetches_per_minute(self): return self._max_ohlcv_fetches_per_minute
    @property
    def user(self): return self._user

    @approved_coins.setter
    def approved_coins(self, new_approved_coins):
        self._approved_coins.approved_coins = new_approved_coins
        self._live["approved_coins"] = self._approved_coins.approved_coins
    @auto_gs.setter
    def auto_gs(self, new_auto_gs):
        self._auto_gs = new_auto_gs
        self._live["auto_gs"] = self._auto_gs
    @inactive_coin_candle_ttl_minutes.setter
    def inactive_coin_candle_ttl_minutes(self, new_inactive_coin_candle_ttl_minutes):
        self._inactive_coin_candle_ttl_minutes = new_inactive_coin_candle_ttl_minutes
        self._live["inactive_coin_candle_ttl_minutes"] = self._inactive_coin_candle_ttl_minutes
    @empty_means_all_approved.setter
    def empty_means_all_approved(self, new_empty_means_all_approved):
        self._empty_means_all_approved = new_empty_means_all_approved
        self._live["empty_means_all_approved"] = self._empty_means_all_approved
    @execution_delay_seconds.setter
    def execution_delay_seconds(self, new_execution_delay_seconds):
        self._execution_delay_seconds = new_execution_delay_seconds
        self._live["execution_delay_seconds"] = self._execution_delay_seconds
    @filter_by_min_effective_cost.setter
    def filter_by_min_effective_cost(self, new_filter_by_min_effective_cost):
        self._filter_by_min_effective_cost = new_filter_by_min_effective_cost
        self._live["filter_by_min_effective_cost"] = self._filter_by_min_effective_cost
    @forced_mode_long.setter
    def forced_mode_long(self, new_forced_mode_long):
        self._forced_mode_long = new_forced_mode_long
        self._live["forced_mode_long"] = self._forced_mode_long
    @forced_mode_short.setter
    def forced_mode_short(self, new_forced_mode_short):
        self._forced_mode_short = new_forced_mode_short
        self._live["forced_mode_short"] = self._forced_mode_short
    @hedge_mode.setter
    def hedge_mode(self, new_hedge_mode):
        self._hedge_mode = bool(new_hedge_mode)
        self._live["hedge_mode"] = self._hedge_mode
    @ignored_coins.setter
    def ignored_coins(self, new_ignored_coins):
        self._ignored_coins.ignored_coins = new_ignored_coins
        self._live["ignored_coins"] = self._ignored_coins.ignored_coins
    @leverage.setter
    def leverage(self, new_leverage):
        self._leverage = new_leverage
        self._live["leverage"] = self._leverage
    @market_orders_allowed.setter
    def market_orders_allowed(self, new_market_orders_allowed):
        self._market_orders_allowed = new_market_orders_allowed
        self._live["market_orders_allowed"] = self._market_orders_allowed
    @max_disk_candles_per_symbol_per_tf.setter
    def max_disk_candles_per_symbol_per_tf(self, new_max_disk_candles_per_symbol_per_tf):
        self._max_disk_candles_per_symbol_per_tf = new_max_disk_candles_per_symbol_per_tf
        self._live["max_disk_candles_per_symbol_per_tf"] = self._max_disk_candles_per_symbol_per_tf
    @max_memory_candles_per_symbol.setter
    def max_memory_candles_per_symbol(self, new_max_memory_candles_per_symbol):
        self._max_memory_candles_per_symbol = new_max_memory_candles_per_symbol
        self._live["max_memory_candles_per_symbol"] = self._max_memory_candles_per_symbol
    @max_n_cancellations_per_batch.setter
    def max_n_cancellations_per_batch(self, new_max_n_cancellations_per_batch):
        self._max_n_cancellations_per_batch = new_max_n_cancellations_per_batch
        self._live["max_n_cancellations_per_batch"] = self._max_n_cancellations_per_batch
    @max_n_creations_per_batch.setter
    def max_n_creations_per_batch(self, new_max_n_creations_per_batch):
        self._max_n_creations_per_batch = new_max_n_creations_per_batch
        self._live["max_n_creations_per_batch"] = self._max_n_creations_per_batch
    @max_n_restarts_per_day.setter
    def max_n_restarts_per_day(self, new_max_n_restarts_per_day):
        self._max_n_restarts_per_day = new_max_n_restarts_per_day
        self._live["max_n_restarts_per_day"] = self._max_n_restarts_per_day
    @minimum_coin_age_days.setter
    def minimum_coin_age_days(self, new_minimum_coin_age_days):
        self._minimum_coin_age_days = new_minimum_coin_age_days
        self._live["minimum_coin_age_days"] = self._minimum_coin_age_days
    @order_match_tolerance_pct.setter
    def order_match_tolerance_pct(self, new_order_match_tolerance_pct):
        self._order_match_tolerance_pct = new_order_match_tolerance_pct
        self._live["order_match_tolerance_pct"] = self._order_match_tolerance_pct
    @pnls_max_lookback_days.setter
    def pnls_max_lookback_days(self, new_pnls_max_lookback_days):
        self._pnls_max_lookback_days = new_pnls_max_lookback_days
        self._live["pnls_max_lookback_days"] = self._pnls_max_lookback_days
    @price_distance_threshold.setter
    def price_distance_threshold(self, new_price_distance_threshold):
        self._price_distance_threshold = new_price_distance_threshold
        self._live["price_distance_threshold"] = self._price_distance_threshold
    @recv_window_ms.setter
    def recv_window_ms(self, new_recv_window_ms):
        self._recv_window_ms = new_recv_window_ms
        self._live["recv_window_ms"] = self._recv_window_ms
    @time_in_force.setter
    def time_in_force(self, new_time_in_force):
        self._time_in_force = new_time_in_force
        self._live["time_in_force"] = self._time_in_force
    @warmup_jitter_seconds.setter
    def warmup_jitter_seconds(self, new_warmup_jitter_seconds):
        self._warmup_jitter_seconds = new_warmup_jitter_seconds
        self._live["warmup_jitter_seconds"] = self._warmup_jitter_seconds
    @warmup_ratio.setter
    def warmup_ratio(self, new_warmup_ratio):
        self._warmup_ratio = new_warmup_ratio
        self._live["warmup_ratio"] = self._warmup_ratio
    @warmup_concurrency.setter
    def warmup_concurrency(self, new_warmup_concurrency):
        if new_warmup_concurrency in (None, ""):
            self._warmup_concurrency = 0
        else:
            self._warmup_concurrency = max(0, int(new_warmup_concurrency))
        self._live["warmup_concurrency"] = self._warmup_concurrency
    @max_concurrent_api_requests.setter
    def max_concurrent_api_requests(self, new_max_concurrent_api_requests):
        if new_max_concurrent_api_requests in (None, ""):
            self._max_concurrent_api_requests = 0
        else:
            self._max_concurrent_api_requests = max(0, int(new_max_concurrent_api_requests))
        self._live["max_concurrent_api_requests"] = self._max_concurrent_api_requests
    @max_warmup_minutes.setter
    def max_warmup_minutes(self, new_max_warmup_minutes):
        self._max_warmup_minutes = new_max_warmup_minutes
        self._live["max_warmup_minutes"] = self._max_warmup_minutes
    @candle_lock_timeout_seconds.setter
    def candle_lock_timeout_seconds(self, new_candle_lock_timeout_seconds):
        self._candle_lock_timeout_seconds = new_candle_lock_timeout_seconds
        self._live["candle_lock_timeout_seconds"] = self._candle_lock_timeout_seconds
    @balance_override.setter
    def balance_override(self, new_balance_override):
        self._balance_override = new_balance_override
        self._live["balance_override"] = self._balance_override
    @balance_hysteresis_snap_pct.setter
    def balance_hysteresis_snap_pct(self, new_balance_hysteresis_snap_pct):
        self._balance_hysteresis_snap_pct = new_balance_hysteresis_snap_pct
        self._live["balance_hysteresis_snap_pct"] = self._balance_hysteresis_snap_pct
    @enable_archive_candle_fetch.setter
    def enable_archive_candle_fetch(self, new_enable_archive_candle_fetch):
        self._enable_archive_candle_fetch = bool(new_enable_archive_candle_fetch)
        self._live["enable_archive_candle_fetch"] = self._enable_archive_candle_fetch
    @max_ohlcv_fetches_per_minute.setter
    def max_ohlcv_fetches_per_minute(self, new_max_ohlcv_fetches_per_minute):
        self._max_ohlcv_fetches_per_minute = int(new_max_ohlcv_fetches_per_minute)
        self._live["max_ohlcv_fetches_per_minute"] = self._max_ohlcv_fetches_per_minute
    @user.setter
    def user(self, new_user):
        self._user = new_user
        self._live["user"] = self._user

class Optimize:
    def __init__(self):
        self._bounds = Bounds()
        self._limits = []  # New list format: [{"metric": "x", "penalize_if": "greater_than", "value": 0.5}, ...]
        # optimize
        self._compress_results_file = True
        self._crossover_probability = 0.7
        self._crossover_eta = 20.0
        self._enable_overrides = []
        self._iters = 100000
        self._mutation_probability = 0.45
        self._mutation_eta = 20.0
        self._mutation_indpb = 0.0
        self._n_cpus = 5
        self._offspring_multiplier = 1.0
        self._pareto_max_size = 250
        self._population_size = 1000
        self._round_to_n_significant_digits = 5
        # scoring
        self._scoring = ["loss_profit_ratio", "mdg_w", "sharpe_ratio"]
        self._write_all_results = True

        self._optimize = {
            "bounds": self._bounds.bounds,
            "compress_results_file": self._compress_results_file,
            "crossover_probability": self._crossover_probability,
            "crossover_eta": self._crossover_eta,
            "enable_overrides": self._enable_overrides,
            "iters": self._iters,
            "limits": self._limits,
            "mutation_probability": self._mutation_probability,
            "mutation_eta": self._mutation_eta,
            "mutation_indpb": self._mutation_indpb,
            "n_cpus": self._n_cpus,
            "offspring_multiplier": self._offspring_multiplier,
            "pareto_max_size": self._pareto_max_size,
            "population_size": self._population_size,
            "round_to_n_significant_digits": self._round_to_n_significant_digits,
            "scoring": self._scoring,
            "write_all_results": self._write_all_results
        }
    
    def __repr__(self):
        return str(self._optimize)

    @property
    def optimize(self):
        # Ensure bounds are always exported in PB7-compatible form.
        # This avoids stale optimize["bounds"] when bounds were loaded as [lo, hi]
        # and later edited in the UI to include a positive step.
        self._optimize["bounds"] = self._bounds.bounds
        return self._optimize
    @optimize.setter
    def optimize(self, new_optimize):
        if "bounds" in new_optimize:
            self.bounds = new_optimize["bounds"]
        if "compress_results_file" in new_optimize:
            self.compress_results_file = new_optimize["compress_results_file"]
        if "crossover_probability" in new_optimize:
            self.crossover_probability = new_optimize["crossover_probability"]
        if "crossover_eta" in new_optimize:
            self.crossover_eta = new_optimize["crossover_eta"]
        if "enable_overrides" in new_optimize:
            self.enable_overrides = new_optimize["enable_overrides"]
        if "iters" in new_optimize:
            self.iters = new_optimize["iters"]
        if "limits" in new_optimize:
            self.limits = new_optimize["limits"]
        if "mutation_probability" in new_optimize:
            self.mutation_probability = new_optimize["mutation_probability"]
        if "mutation_eta" in new_optimize:
            self.mutation_eta = new_optimize["mutation_eta"]
        if "mutation_indpb" in new_optimize:
            self.mutation_indpb = new_optimize["mutation_indpb"]
        if "n_cpus" in new_optimize:
            self.n_cpus = new_optimize["n_cpus"]
        if "offspring_multiplier" in new_optimize:
            self.offspring_multiplier = new_optimize["offspring_multiplier"]
        if "pareto_max_size" in new_optimize:
            self.pareto_max_size = new_optimize["pareto_max_size"]
        if "population_size" in new_optimize:
            self.population_size = new_optimize["population_size"]
        if "round_to_n_significant_digits" in new_optimize:
            self.round_to_n_significant_digits = new_optimize["round_to_n_significant_digits"]
        if "scoring" in new_optimize:
            self.scoring = new_optimize["scoring"]
        if "write_all_results" in new_optimize:
            self.write_all_results = new_optimize["write_all_results"]

    @property
    def bounds(self): return self._bounds
    @property
    def compress_results_file(self): return self._compress_results_file
    @property
    def limits(self): return self._limits
    @property
    def crossover_probability(self): return self._crossover_probability
    @property
    def crossover_eta(self): return self._crossover_eta
    @property
    def enable_overrides(self): return self._enable_overrides
    @property
    def iters(self): return self._iters
    @property
    def mutation_probability(self): return self._mutation_probability
    @property
    def mutation_eta(self): return self._mutation_eta
    @property
    def mutation_indpb(self): return self._mutation_indpb
    @property
    def n_cpus(self):
        if self._n_cpus > multiprocessing.cpu_count():
            self.n_cpus = multiprocessing.cpu_count()
        return self._n_cpus
    @property
    def offspring_multiplier(self): return self._offspring_multiplier
    @property
    def pareto_max_size(self): return self._pareto_max_size
    @property
    def population_size(self): return self._population_size
    @property
    def round_to_n_significant_digits(self): return self._round_to_n_significant_digits
    @property
    def scoring(self): return self._scoring
    @property
    def write_all_results(self): return self._write_all_results

    @bounds.setter
    def bounds(self, new_bounds):
        self._bounds.bounds = new_bounds
        self._optimize["bounds"] = self._bounds.bounds
    @compress_results_file.setter
    def compress_results_file(self, new_compress_results_file):
        self._compress_results_file = new_compress_results_file
        self._optimize["compress_results_file"] = self._compress_results_file
    @limits.setter
    def limits(self, new_limits):
        # Convert legacy dict format to new list format
        if isinstance(new_limits, dict):
            self._limits = self._convert_legacy_limits(new_limits)
        elif isinstance(new_limits, list):
            normalized = []
            for entry in new_limits:
                if isinstance(entry, dict) and isinstance(entry.get("metric"), str):
                    canon = canonicalize_metric_name(entry["metric"])
                    if canon != entry["metric"]:
                        patched = entry.copy()
                        patched["metric"] = canon
                        entry = patched
                normalized.append(entry)
            self._limits = normalized
        else:
            self._limits = []
        self._optimize["limits"] = self._limits

    def _convert_legacy_limits(self, limits_dict: dict) -> list:
        """Convert legacy dict format to new list format.
        
        Legacy formats:
        - penalize_if_greater_than_X: value -> {metric: X, penalize_if: greater_than, value: ...}
        - penalize_if_lower_than_X: value -> {metric: X, penalize_if: less_than, value: ...}
        - penalize_if_greater_than_btc_X: value -> {metric: X_btc, penalize_if: greater_than, value: ...}
        - penalize_if_lower_than_btc_X: value -> {metric: X_btc, penalize_if: less_than, value: ...}
        - lower_bound_X: value -> {metric: X, penalize_if: greater_than, value: ...}
        - upper_bound_X: value -> {metric: X, penalize_if: less_than, value: ...}
        
        Currency metrics without suffix get _usd appended (like PassivBot canonicalize_metric_name).
        Invalid/unknown metrics are skipped with a warning.
        """
        # Use centralized metrics definitions
        ALL_VALID_METRICS = get_all_metrics_with_currency()
        
        entries = []
        for key, value in limits_dict.items():
            metric = None
            penalize_if = None
            
            # Handle lower_bound_X format (older format)
            if key.startswith("lower_bound_btc_"):
                metric = key[len("lower_bound_btc_"):] + "_btc"
                penalize_if = "greater_than"
            elif key.startswith("lower_bound_"):
                metric = key[len("lower_bound_"):]
                penalize_if = "greater_than"
            # Handle upper_bound_X format (older format)
            elif key.startswith("upper_bound_btc_"):
                metric = key[len("upper_bound_btc_"):] + "_btc"
                penalize_if = "less_than"
            elif key.startswith("upper_bound_"):
                metric = key[len("upper_bound_"):]
                penalize_if = "less_than"
            # Handle penalize_if_greater_than_X format
            elif key.startswith("penalize_if_greater_than_btc_"):
                metric = key[len("penalize_if_greater_than_btc_"):] + "_btc"
                penalize_if = "greater_than"
            elif key.startswith("penalize_if_lower_than_btc_"):
                metric = key[len("penalize_if_lower_than_btc_"):] + "_btc"
                penalize_if = "less_than"
            elif key.startswith("penalize_if_greater_than_"):
                metric = key[len("penalize_if_greater_than_"):]
                penalize_if = "greater_than"
            elif key.startswith("penalize_if_lower_than_"):
                metric = key[len("penalize_if_lower_than_"):]
                penalize_if = "less_than"
            else:
                # Unknown format, skip
                continue
            
            # Canonicalize metric name (like PassivBot does)
            # If it's a currency metric without suffix, append _usd
            if metric and not metric.endswith("_usd") and not metric.endswith("_btc"):
                if is_currency_metric(metric):
                    metric = f"{metric}_usd"
            
            # Validate metric - skip if not in valid metrics list
            if metric not in ALL_VALID_METRICS:
                print(f"Warning: Skipping invalid/obsolete limit metric '{key}' -> '{metric}'")
                continue
            
            try:
                numeric_value = float(value)
                entries.append({
                    "metric": metric,
                    "penalize_if": penalize_if,
                    "value": numeric_value
                })
            except (TypeError, ValueError):
                continue
        
        return entries
    @crossover_probability.setter
    def crossover_probability(self, new_crossover_probability):
        self._crossover_probability = new_crossover_probability
        self._optimize["crossover_probability"] = self._crossover_probability
    @crossover_eta.setter
    def crossover_eta(self, new_crossover_eta):
        self._crossover_eta = new_crossover_eta
        self._optimize["crossover_eta"] = self._crossover_eta
    @enable_overrides.setter
    def enable_overrides(self, new_enable_overrides):
        self._enable_overrides = new_enable_overrides
        self._optimize["enable_overrides"] = self._enable_overrides
    @iters.setter
    def iters(self, new_iters):
        self._iters = new_iters
        self._optimize["iters"] = self._iters
    @mutation_probability.setter
    def mutation_probability(self, new_mutation_probability):
        self._mutation_probability = new_mutation_probability
        self._optimize["mutation_probability"] = self._mutation_probability
    @mutation_eta.setter
    def mutation_eta(self, new_mutation_eta):
        self._mutation_eta = new_mutation_eta
        self._optimize["mutation_eta"] = self._mutation_eta
    @mutation_indpb.setter
    def mutation_indpb(self, new_mutation_indpb):
        self._mutation_indpb = new_mutation_indpb
        self._optimize["mutation_indpb"] = self._mutation_indpb
    @n_cpus.setter
    def n_cpus(self, new_n_cpus):
        self._n_cpus = new_n_cpus
        self._optimize["n_cpus"] = self._n_cpus
        if self._n_cpus > multiprocessing.cpu_count():
            self.n_cpus = multiprocessing.cpu_count()
    @offspring_multiplier.setter
    def offspring_multiplier(self, new_offspring_multiplier):
        self._offspring_multiplier = new_offspring_multiplier
        self._optimize["offspring_multiplier"] = self._offspring_multiplier
    @pareto_max_size.setter
    def pareto_max_size(self, new_pareto_max_size):
        self._pareto_max_size = new_pareto_max_size
        self._optimize["pareto_max_size"] = self._pareto_max_size
    @population_size.setter
    def population_size(self, new_population_size):
        self._population_size = new_population_size
        self._optimize["population_size"] = self._population_size
    @round_to_n_significant_digits.setter
    def round_to_n_significant_digits(self, new_round_to_n_significant_digits):
        self._round_to_n_significant_digits = new_round_to_n_significant_digits
        self._optimize["round_to_n_significant_digits"] = self._round_to_n_significant_digits
    @scoring.setter
    def scoring(self, new_scoring):
        self._scoring = new_scoring
        self._optimize["scoring"] = self._scoring
    @write_all_results.setter
    def write_all_results(self, new_write_all_results):
        self._write_all_results = new_write_all_results
        self._optimize["write_all_results"] = self._write_all_results

class Bounds:

    CLOSE_GRID_MARKUP_END_MIN = 0.0
    CLOSE_GRID_MARKUP_END_MAX = 1.0
    CLOSE_GRID_MARKUP_END_STEP = 0.001
    CLOSE_GRID_MARKUP_END_ROUND = 3
    CLOSE_GRID_MARKUP_END_FORMAT = f'%.{CLOSE_GRID_MARKUP_END_ROUND}f'
    CLOSE_GRID_MARKUP_END_WIDGET_STEP = 0.00001

    CLOSE_GRID_MARKUP_START_MIN = 0.0
    CLOSE_GRID_MARKUP_START_MAX = 1.0
    CLOSE_GRID_MARKUP_START_STEP = 0.001
    CLOSE_GRID_MARKUP_START_ROUND = 3
    CLOSE_GRID_MARKUP_START_FORMAT = f'%.{CLOSE_GRID_MARKUP_START_ROUND}f'
    CLOSE_GRID_MARKUP_START_WIDGET_STEP = 0.00001
    
    CLOSE_GRID_QTY_PCT_MIN = 0.0
    CLOSE_GRID_QTY_PCT_MAX = 1.0
    CLOSE_GRID_QTY_PCT_STEP = 0.05
    CLOSE_GRID_QTY_PCT_ROUND = 2
    CLOSE_GRID_QTY_PCT_FORMAT = f'%.{CLOSE_GRID_QTY_PCT_ROUND}f'
    CLOSE_GRID_QTY_PCT_WIDGET_STEP = 0.00001

    CLOSE_TRAILING_GRID_RATIO_MIN = -1.0
    CLOSE_TRAILING_GRID_RATIO_MAX = 1.0
    CLOSE_TRAILING_GRID_RATIO_STEP = 0.01
    CLOSE_TRAILING_GRID_RATIO_ROUND = 2
    CLOSE_TRAILING_GRID_RATIO_FORMAT = f'%.{CLOSE_TRAILING_GRID_RATIO_ROUND}f'
    CLOSE_TRAILING_GRID_RATIO_WIDGET_STEP = 0.00001

    CLOSE_TRAILING_QTY_PCT_MIN = 0.0
    CLOSE_TRAILING_QTY_PCT_MAX = 1.0
    CLOSE_TRAILING_QTY_PCT_STEP = 0.001
    CLOSE_TRAILING_QTY_PCT_ROUND = 3
    CLOSE_TRAILING_QTY_PCT_FORMAT = f'%.{CLOSE_TRAILING_QTY_PCT_ROUND}f'
    CLOSE_TRAILING_QTY_PCT_WIDGET_STEP = 0.00001

    CLOSE_TRAILING_RETRACEMENT_PCT_MIN = 0.0
    CLOSE_TRAILING_RETRACEMENT_PCT_MAX = 1.0
    CLOSE_TRAILING_RETRACEMENT_PCT_STEP = 0.001
    CLOSE_TRAILING_RETRACEMENT_PCT_ROUND = 3
    CLOSE_TRAILING_RETRACEMENT_PCT_FORMAT = f'%.{CLOSE_TRAILING_RETRACEMENT_PCT_ROUND}f'
    CLOSE_TRAILING_RETRACEMENT_PCT_WIDGET_STEP = 0.00001

    CLOSE_TRAILING_THRESHOLD_PCT_MIN = -1.0
    CLOSE_TRAILING_THRESHOLD_PCT_MAX = 1.0
    CLOSE_TRAILING_THRESHOLD_PCT_STEP = 0.0001
    CLOSE_TRAILING_THRESHOLD_PCT_ROUND = 4
    CLOSE_TRAILING_THRESHOLD_PCT_FORMAT = f'%.{CLOSE_TRAILING_THRESHOLD_PCT_ROUND}f'
    CLOSE_TRAILING_THRESHOLD_PCT_WIDGET_STEP = 0.00001

    EMA_SPAN_0_MIN = 1.0
    EMA_SPAN_0_MAX = 10000.0
    EMA_SPAN_0_STEP = 1.0
    EMA_SPAN_0_ROUND = 1
    EMA_SPAN_0_FORMAT = f'%.{EMA_SPAN_0_ROUND}f'
    EMA_SPAN_0_WIDGET_STEP = 1.0

    EMA_SPAN_1_MIN = 1.0
    EMA_SPAN_1_MAX = 10000.0
    EMA_SPAN_1_STEP = 1.0
    EMA_SPAN_1_ROUND = 1
    EMA_SPAN_1_FORMAT = f'%.{EMA_SPAN_1_ROUND}f'
    EMA_SPAN_1_WIDGET_STEP = 1.0

    ENTRY_GRID_DOUBLE_DOWN_FACTOR_MIN = 0.0
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_MAX = 10.0
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_STEP = 0.05
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_ROUND = 2
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_FORMAT = f'%.{ENTRY_GRID_DOUBLE_DOWN_FACTOR_ROUND}f'
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_WIDGET_STEP = 0.00001

    ENTRY_VOLATILITY_EMA_SPAN_HOURS_MIN = 1.0
    ENTRY_VOLATILITY_EMA_SPAN_HOURS_MAX = 10000.0
    ENTRY_VOLATILITY_EMA_SPAN_HOURS_STEP = 1.0
    ENTRY_VOLATILITY_EMA_SPAN_HOURS_ROUND = 1
    ENTRY_VOLATILITY_EMA_SPAN_HOURS_FORMAT = f'%.{ENTRY_VOLATILITY_EMA_SPAN_HOURS_ROUND}f'
    ENTRY_VOLATILITY_EMA_SPAN_HOURS_WIDGET_STEP = 1.0

    ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_MIN = 0.0
    ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_MAX = 10000.0
    ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_STEP = 1.0
    ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_ROUND = 1
    ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_FORMAT = f'%.{ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_ROUND}f'
    ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_WIDGET_STEP = 1.0

    ENTRY_GRID_SPACING_PCT_MIN = 0.0
    ENTRY_GRID_SPACING_PCT_MAX = 1.0
    ENTRY_GRID_SPACING_PCT_STEP = 0.001
    ENTRY_GRID_SPACING_PCT_ROUND = 3
    ENTRY_GRID_SPACING_PCT_FORMAT = f'%.{ENTRY_GRID_SPACING_PCT_ROUND}f'
    ENTRY_GRID_SPACING_PCT_WIDGET_STEP = 0.00001

    ENTRY_GRID_SPACING_WE_WEIGHT_MIN = 0.0
    ENTRY_GRID_SPACING_WE_WEIGHT_MAX = 100.0
    ENTRY_GRID_SPACING_WE_WEIGHT_STEP = 0.01
    ENTRY_GRID_SPACING_WE_WEIGHT_ROUND = 2
    ENTRY_GRID_SPACING_WE_WEIGHT_FORMAT = f'%.{ENTRY_GRID_SPACING_WE_WEIGHT_ROUND}f'
    ENTRY_GRID_SPACING_WE_WEIGHT_WIDGET_STEP = 0.00001

    ENTRY_INITIAL_EMA_DIST_MIN = -1.0
    ENTRY_INITIAL_EMA_DIST_MAX = 1.0
    ENTRY_INITIAL_EMA_DIST_STEP = 0.0001
    ENTRY_INITIAL_EMA_DIST_ROUND = 4
    ENTRY_INITIAL_EMA_DIST_FORMAT = f'%.{ENTRY_INITIAL_EMA_DIST_ROUND}f'
    ENTRY_INITIAL_EMA_DIST_WIDGET_STEP = 0.00001

    ENTRY_INITIAL_QTY_PCT_MIN = 0.0
    ENTRY_INITIAL_QTY_PCT_MAX = 1.0
    ENTRY_INITIAL_QTY_PCT_STEP = 0.001
    ENTRY_INITIAL_QTY_PCT_ROUND = 3
    ENTRY_INITIAL_QTY_PCT_FORMAT = f'%.{ENTRY_INITIAL_QTY_PCT_ROUND}f'
    ENTRY_INITIAL_QTY_PCT_WIDGET_STEP = 0.00001

    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MIN = 0.0
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MAX = 10.0
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_STEP = 0.05
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_ROUND = 2
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_FORMAT = f'%.{ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_ROUND}f'
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_WIDGET_STEP = 0.00001
    
    ENTRY_TRAILING_GRID_RATIO_MIN = -1.0
    ENTRY_TRAILING_GRID_RATIO_MAX = 1.0
    ENTRY_TRAILING_GRID_RATIO_STEP = 0.01
    ENTRY_TRAILING_GRID_RATIO_ROUND = 2
    ENTRY_TRAILING_GRID_RATIO_FORMAT = f'%.{ENTRY_TRAILING_GRID_RATIO_ROUND}f'
    ENTRY_TRAILING_GRID_RATIO_WIDGET_STEP = 0.00001

    ENTRY_TRAILING_RETRACEMENT_PCT_MIN = 0.0
    ENTRY_TRAILING_RETRACEMENT_PCT_MAX = 1.0
    ENTRY_TRAILING_RETRACEMENT_PCT_STEP = 0.001
    ENTRY_TRAILING_RETRACEMENT_PCT_ROUND = 3
    ENTRY_TRAILING_RETRACEMENT_PCT_FORMAT = f'%.{ENTRY_TRAILING_RETRACEMENT_PCT_ROUND}f'
    ENTRY_TRAILING_RETRACEMENT_PCT_WIDGET_STEP = 0.00001

    ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_MIN = 0.0
    ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_MAX = 100.0
    ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_STEP = 0.1
    ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_ROUND = 1
    ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_FORMAT = f'%.{ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_ROUND}f'
    ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_WIDGET_STEP = 1.0

    ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_MIN = 0.0
    ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_MAX = 1000.0
    ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_STEP = 1.0
    ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_ROUND = 1
    ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_FORMAT = f'%.{ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_ROUND}f'
    ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_WIDGET_STEP = 1.0

    ENTRY_TRAILING_THRESHOLD_PCT_MIN = -1.0
    ENTRY_TRAILING_THRESHOLD_PCT_MAX = 1.0
    ENTRY_TRAILING_THRESHOLD_PCT_STEP = 0.0001
    ENTRY_TRAILING_THRESHOLD_PCT_ROUND = 4
    ENTRY_TRAILING_THRESHOLD_PCT_FORMAT = f'%.{ENTRY_TRAILING_THRESHOLD_PCT_ROUND}f'
    ENTRY_TRAILING_THRESHOLD_PCT_WIDGET_STEP = 0.00001

    ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_MIN = 0.0
    ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_MAX = 100.0
    ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_STEP = 0.1
    ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_ROUND = 1
    ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_FORMAT = f'%.{ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_ROUND}f'
    ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_WIDGET_STEP = 1.0

    ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_MIN = 0.0
    ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_MAX = 1000.0
    ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_STEP = 1.0
    ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_ROUND = 1
    ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_FORMAT = f'%.{ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_ROUND}f'
    ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_WIDGET_STEP = 1.0

    FILTER_VOLATILITY_EMA_SPAN_MIN = 0.0
    FILTER_VOLATILITY_EMA_SPAN_MAX = 10000.0
    FILTER_VOLATILITY_EMA_SPAN_STEP = 1.0
    FILTER_VOLATILITY_EMA_SPAN_ROUND = 0
    FILTER_VOLATILITY_EMA_SPAN_FORMAT = f'%.{FILTER_VOLATILITY_EMA_SPAN_ROUND}f'
    FILTER_VOLATILITY_EMA_SPAN_WIDGET_STEP = 0.1

    FILTER_VOLUME_DROP_PCT_MIN = 0.0
    FILTER_VOLUME_DROP_PCT_MAX = 1.0
    FILTER_VOLUME_DROP_PCT_STEP = 0.01
    FILTER_VOLUME_DROP_PCT_ROUND = 2
    FILTER_VOLUME_DROP_PCT_FORMAT = f'%.{FILTER_VOLUME_DROP_PCT_ROUND}f'
    FILTER_VOLUME_DROP_PCT_WIDGET_STEP = 0.00001

    FILTER_VOLATILITY_DROP_PCT_MIN = 0.0
    FILTER_VOLATILITY_DROP_PCT_MAX = 1.0
    FILTER_VOLATILITY_DROP_PCT_STEP = 0.01
    FILTER_VOLATILITY_DROP_PCT_ROUND = 2
    FILTER_VOLATILITY_DROP_PCT_FORMAT = f'%.{FILTER_VOLATILITY_DROP_PCT_ROUND}f'
    FILTER_VOLATILITY_DROP_PCT_WIDGET_STEP = 0.00001

    FILTER_VOLUME_EMA_SPAN_MIN = 0.0
    FILTER_VOLUME_EMA_SPAN_MAX = 10000.0
    FILTER_VOLUME_EMA_SPAN_STEP = 1.0
    FILTER_VOLUME_EMA_SPAN_ROUND = 0
    FILTER_VOLUME_EMA_SPAN_FORMAT = f'%.{FILTER_VOLUME_EMA_SPAN_ROUND}f'
    FILTER_VOLUME_EMA_SPAN_WIDGET_STEP = 0.1

    N_POSITIONS_MIN = 0.0
    N_POSITIONS_MAX = 100.0
    N_POSITIONS_STEP = 1.0
    N_POSITIONS_ROUND = 0
    N_POSITIONS_FORMAT = f'%.{N_POSITIONS_ROUND}f'
    N_POSITIONS_WIDGET_STEP = 1.0

    TOTAL_WALLET_EXPOSURE_LIMIT_MIN = 0.0
    TOTAL_WALLET_EXPOSURE_LIMIT_MAX = 100.0
    TOTAL_WALLET_EXPOSURE_LIMIT_STEP = 0.1
    TOTAL_WALLET_EXPOSURE_LIMIT_ROUND = 1
    TOTAL_WALLET_EXPOSURE_LIMIT_FORMAT = f'%.{TOTAL_WALLET_EXPOSURE_LIMIT_ROUND}f'
    TOTAL_WALLET_EXPOSURE_LIMIT_WIDGET_STEP = 0.01

    UNSTUCK_CLOSE_PCT_MIN = 0.0
    UNSTUCK_CLOSE_PCT_MAX = 1.0
    UNSTUCK_CLOSE_PCT_STEP = 0.001
    UNSTUCK_CLOSE_PCT_ROUND = 3
    UNSTUCK_CLOSE_PCT_FORMAT = f'%.{UNSTUCK_CLOSE_PCT_ROUND}f'
    UNSTUCK_CLOSE_PCT_WIDGET_STEP = 0.00001

    UNSTUCK_EMA_DIST_MIN = -1.0
    UNSTUCK_EMA_DIST_MAX = 1.0
    UNSTUCK_EMA_DIST_STEP = 0.001
    UNSTUCK_EMA_DIST_ROUND = 3
    UNSTUCK_EMA_DIST_FORMAT = f'%.{UNSTUCK_EMA_DIST_ROUND}f'
    UNSTUCK_EMA_DIST_WIDGET_STEP = 0.00001

    UNSTUCK_LOSS_ALLOWANCE_PCT_MIN = 0.0
    UNSTUCK_LOSS_ALLOWANCE_PCT_MAX = 1.0
    UNSTUCK_LOSS_ALLOWANCE_PCT_STEP = 0.001
    UNSTUCK_LOSS_ALLOWANCE_PCT_ROUND = 3
    UNSTUCK_LOSS_ALLOWANCE_PCT_FORMAT = f'%.{UNSTUCK_LOSS_ALLOWANCE_PCT_ROUND}f'
    UNSTUCK_LOSS_ALLOWANCE_PCT_WIDGET_STEP = 0.00001

    UNSTUCK_THRESHOLD_MIN = 0.0
    UNSTUCK_THRESHOLD_MAX = 1.0
    UNSTUCK_THRESHOLD_STEP = 0.01
    UNSTUCK_THRESHOLD_ROUND = 2
    UNSTUCK_THRESHOLD_FORMAT = f'%.{UNSTUCK_THRESHOLD_ROUND}f'
    UNSTUCK_THRESHOLD_WIDGET_STEP = 0.00001

    RISK_WEL_ENFORCER_THRESHOLD_MIN = 0.0
    RISK_WEL_ENFORCER_THRESHOLD_MAX = 2.0
    RISK_WEL_ENFORCER_THRESHOLD_STEP = 0.01
    RISK_WEL_ENFORCER_THRESHOLD_ROUND = 2
    RISK_WEL_ENFORCER_THRESHOLD_FORMAT = f'%.{RISK_WEL_ENFORCER_THRESHOLD_ROUND}f'
    RISK_WEL_ENFORCER_THRESHOLD_WIDGET_STEP = 0.00001

    RISK_WE_EXCESS_ALLOWANCE_PCT_MIN = 0.0
    RISK_WE_EXCESS_ALLOWANCE_PCT_MAX = 1.0
    RISK_WE_EXCESS_ALLOWANCE_PCT_STEP = 0.01
    RISK_WE_EXCESS_ALLOWANCE_PCT_ROUND = 2
    RISK_WE_EXCESS_ALLOWANCE_PCT_FORMAT = f'%.{RISK_WE_EXCESS_ALLOWANCE_PCT_ROUND}f'
    RISK_WE_EXCESS_ALLOWANCE_PCT_WIDGET_STEP = 0.00001

    RISK_TWEL_ENFORCER_THRESHOLD_MIN = 0.0
    RISK_TWEL_ENFORCER_THRESHOLD_MAX = 2.0
    RISK_TWEL_ENFORCER_THRESHOLD_STEP = 0.01
    RISK_TWEL_ENFORCER_THRESHOLD_ROUND = 2
    RISK_TWEL_ENFORCER_THRESHOLD_FORMAT = f'%.{RISK_TWEL_ENFORCER_THRESHOLD_ROUND}f'
    RISK_TWEL_ENFORCER_THRESHOLD_WIDGET_STEP = 0.00001

    def __init__(self):
        # bounds long
        self._long_close_grid_markup_end_0 = 0.001
        self._long_close_grid_markup_end_1 = 0.03
        self._long_close_grid_markup_end_step = 0.0
        self._long_close_grid_markup_start_0 = 0.001
        self._long_close_grid_markup_start_1 = 0.03
        self._long_close_grid_markup_start_step = 0.0
        self._long_close_grid_qty_pct_0 = 0.05
        self._long_close_grid_qty_pct_1 = 1.0
        self._long_close_grid_qty_pct_step = 0.0
        self._long_close_trailing_grid_ratio_0 = 0.0
        self._long_close_trailing_grid_ratio_1 = 1.0
        self._long_close_trailing_grid_ratio_step = 0.0
        self._long_close_trailing_qty_pct_0 = 0.05
        self._long_close_trailing_qty_pct_1 = 1.0
        self._long_close_trailing_qty_pct_step = 0.0
        self._long_close_trailing_retracement_pct_0 = 0.0
        self._long_close_trailing_retracement_pct_1 = 0.1
        self._long_close_trailing_retracement_pct_step = 0.0
        self._long_close_trailing_threshold_pct_0 = -0.1
        self._long_close_trailing_threshold_pct_1 = 0.1
        self._long_close_trailing_threshold_pct_step = 0.0
        self._long_ema_span_0_0 = 200.0
        self._long_ema_span_0_1 = 1440.0
        self._long_ema_span_1_0 = 200.0
        self._long_ema_span_1_1 = 1440.0
        self._long_ema_span_0_step = 0.0
        self._long_ema_span_1_step = 0.0
        self._long_entry_grid_double_down_factor_0 = 0.1
        self._long_entry_grid_double_down_factor_1 = 3.0
        self._long_entry_grid_double_down_factor_step = 0.0
        self._long_entry_volatility_ema_span_hours_0 = 24.0
        self._long_entry_volatility_ema_span_hours_1 = 336.0
        self._long_entry_volatility_ema_span_hours_step = 0.0
        self._long_entry_grid_spacing_volatility_weight_0 = 0.0
        self._long_entry_grid_spacing_volatility_weight_1 = 400.0
        self._long_entry_grid_spacing_volatility_weight_step = 0.0
        self._long_entry_grid_spacing_pct_0 = 0.001
        self._long_entry_grid_spacing_pct_1 = 0.12
        self._long_entry_grid_spacing_pct_step = 0.0
        self._long_entry_grid_spacing_we_weight_0 = 0.0
        self._long_entry_grid_spacing_we_weight_1 = 10.0
        self._long_entry_grid_spacing_we_weight_step = 0.0
        self._long_entry_initial_ema_dist_0 = -0.1
        self._long_entry_initial_ema_dist_1 = 0.003
        self._long_entry_initial_ema_dist_step = 0.0
        self._long_entry_initial_qty_pct_0 = 0.005
        self._long_entry_initial_qty_pct_1 = 0.1
        self._long_entry_initial_qty_pct_step = 0.0
        self._long_entry_trailing_double_down_factor_0 = 0.1
        self._long_entry_trailing_double_down_factor_1 = 3.0
        self._long_entry_trailing_double_down_factor_step = 0.0
        self._long_entry_trailing_grid_ratio_0 = -1.0
        self._long_entry_trailing_grid_ratio_1 = 1.0
        self._long_entry_trailing_grid_ratio_step = 0.0
        self._long_entry_trailing_retracement_pct_0 = 0.0
        self._long_entry_trailing_retracement_pct_1 = 0.1
        self._long_entry_trailing_retracement_pct_step = 0.0
        self._long_entry_trailing_retracement_we_weight_0 = 0.0
        self._long_entry_trailing_retracement_we_weight_1 = 20.0
        self._long_entry_trailing_retracement_we_weight_step = 0.0
        self._long_entry_trailing_retracement_volatility_weight_0 = 0.0
        self._long_entry_trailing_retracement_volatility_weight_1 = 300.0
        self._long_entry_trailing_retracement_volatility_weight_step = 0.0
        self._long_entry_trailing_threshold_pct_0 = -0.1
        self._long_entry_trailing_threshold_pct_1 = 0.1
        self._long_entry_trailing_threshold_pct_step = 0.0
        self._long_entry_trailing_threshold_we_weight_0 = 0.0
        self._long_entry_trailing_threshold_we_weight_1 = 20.0
        self._long_entry_trailing_threshold_we_weight_step = 0.0
        self._long_entry_trailing_threshold_volatility_weight_0 = 0.0
        self._long_entry_trailing_threshold_volatility_weight_1 = 300.0
        self._long_entry_trailing_threshold_volatility_weight_step = 0.0
        self._long_filter_volatility_ema_span_0 = 10.0
        self._long_filter_volatility_ema_span_1 = 360.0
        self._long_filter_volatility_ema_span_step = 0.0
        self._long_filter_volume_drop_pct_0 = 0.5
        self._long_filter_volume_drop_pct_1 = 1.0
        self._long_filter_volume_drop_pct_step = 0.0
        self._long_filter_volatility_drop_pct_0 = 0.0
        self._long_filter_volatility_drop_pct_1 = 0.0
        self._long_filter_volatility_drop_pct_step = 0.0
        self._long_filter_volume_ema_span_0 = 10.0
        self._long_filter_volume_ema_span_1 = 360.0
        self._long_filter_volume_ema_span_step = 0.0
        self._long_n_positions_0 = 1.0
        self._long_n_positions_1 = 20.0
        self._long_n_positions_step = 0.0
        self._long_total_wallet_exposure_limit_0 = 0.0
        self._long_total_wallet_exposure_limit_1 = 5.0
        self._long_total_wallet_exposure_limit_step = 0.0
        self._long_unstuck_close_pct_0 = 0.001
        self._long_unstuck_close_pct_1 = 0.1
        self._long_unstuck_close_pct_step = 0.0
        self._long_unstuck_ema_dist_0 = -0.1
        self._long_unstuck_ema_dist_1 = 0.01
        self._long_unstuck_ema_dist_step = 0.0
        self._long_unstuck_loss_allowance_pct_0 = 0.0
        self._long_unstuck_loss_allowance_pct_1 = 0.05
        self._long_unstuck_loss_allowance_pct_step = 0.0
        self._long_unstuck_threshold_0 = 0.4
        self._long_unstuck_threshold_1 = 0.95
        self._long_unstuck_threshold_step = 0.0
        self._long_risk_wel_enforcer_threshold_0 = 0.8
        self._long_risk_wel_enforcer_threshold_1 = 1.2
        self._long_risk_wel_enforcer_threshold_step = 0.0
        self._long_risk_we_excess_allowance_pct_0 = 0.0
        self._long_risk_we_excess_allowance_pct_1 = 0.5
        self._long_risk_we_excess_allowance_pct_step = 0.0
        self._long_risk_twel_enforcer_threshold_0 = 0.8
        self._long_risk_twel_enforcer_threshold_1 = 1.2
        self._long_risk_twel_enforcer_threshold_step = 0.0
        # bounds short
        self._short_close_grid_markup_end_0 = 0.001
        self._short_close_grid_markup_end_1 = 0.03
        self._short_close_grid_markup_end_step = 0.0
        self._short_close_grid_markup_start_0 = 0.001
        self._short_close_grid_markup_start_1 = 0.03
        self._short_close_grid_markup_start_step = 0.0
        self._short_close_grid_qty_pct_0 = 0.05
        self._short_close_grid_qty_pct_1 = 1.0
        self._short_close_grid_qty_pct_step = 0.0
        self._short_close_trailing_grid_ratio_0 = -1.0
        self._short_close_trailing_grid_ratio_1 = 1.0
        self._short_close_trailing_grid_ratio_step = 0.0
        self._short_close_trailing_qty_pct_0 = 0.05
        self._short_close_trailing_qty_pct_1 = 1.0
        self._short_close_trailing_qty_pct_step = 0.0
        self._short_close_trailing_retracement_pct_0 = 0.0
        self._short_close_trailing_retracement_pct_1 = 0.1
        self._short_close_trailing_retracement_pct_step = 0.0
        self._short_close_trailing_threshold_pct_0 = -0.1
        self._short_close_trailing_threshold_pct_1 = 0.1
        self._short_close_trailing_threshold_pct_step = 0.0
        self._short_ema_span_0_0 = 200.0
        self._short_ema_span_0_1 = 1440.0
        self._short_ema_span_0_step = 0.0
        self._short_ema_span_1_0 = 200.0
        self._short_ema_span_1_1 = 1440.0
        self._short_ema_span_1_step = 0.0
        self._short_entry_grid_double_down_factor_0 = 0.1
        self._short_entry_grid_double_down_factor_1 = 3.0
        self._short_entry_grid_double_down_factor_step = 0.0
        self._short_entry_volatility_ema_span_hours_0 = 24.0
        self._short_entry_volatility_ema_span_hours_1 = 336.0
        self._short_entry_volatility_ema_span_hours_step = 0.0
        self._short_entry_grid_spacing_volatility_weight_0 = 0.0
        self._short_entry_grid_spacing_volatility_weight_1 = 400.0
        self._short_entry_grid_spacing_volatility_weight_step = 0.0
        self._short_entry_grid_spacing_pct_0 = 0.001
        self._short_entry_grid_spacing_pct_1 = 0.12
        self._short_entry_grid_spacing_pct_step = 0.0
        self._short_entry_grid_spacing_we_weight_0 = 0.0
        self._short_entry_grid_spacing_we_weight_1 = 10.0
        self._short_entry_grid_spacing_we_weight_step = 0.0
        self._short_entry_initial_ema_dist_0 = -0.1
        self._short_entry_initial_ema_dist_1 = 0.003
        self._short_entry_initial_ema_dist_step = 0.0
        self._short_entry_initial_qty_pct_0 = 0.005
        self._short_entry_initial_qty_pct_1 = 0.1
        self._short_entry_initial_qty_pct_step = 0.0
        self._short_entry_trailing_double_down_factor_0 = 0.1
        self._short_entry_trailing_double_down_factor_1 = 3.0
        self._short_entry_trailing_double_down_factor_step = 0.0
        self._short_entry_trailing_grid_ratio_0 = -1.0
        self._short_entry_trailing_grid_ratio_1 = 1.0
        self._short_entry_trailing_grid_ratio_step = 0.0
        self._short_entry_trailing_retracement_pct_0 = 0.0
        self._short_entry_trailing_retracement_pct_1 = 0.1
        self._short_entry_trailing_retracement_pct_step = 0.0
        self._short_entry_trailing_retracement_we_weight_0 = 0.0
        self._short_entry_trailing_retracement_we_weight_1 = 20.0
        self._short_entry_trailing_retracement_we_weight_step = 0.0
        self._short_entry_trailing_retracement_volatility_weight_0 = 0.0
        self._short_entry_trailing_retracement_volatility_weight_1 = 300.0
        self._short_entry_trailing_retracement_volatility_weight_step = 0.0
        self._short_entry_trailing_threshold_pct_0 = -0.1
        self._short_entry_trailing_threshold_pct_1 = 0.1
        self._short_entry_trailing_threshold_pct_step = 0.0
        self._short_entry_trailing_threshold_we_weight_0 = 0.0
        self._short_entry_trailing_threshold_we_weight_1 = 20.0
        self._short_entry_trailing_threshold_we_weight_step = 0.0
        self._short_entry_trailing_threshold_volatility_weight_0 = 0.0
        self._short_entry_trailing_threshold_volatility_weight_1 = 300.0
        self._short_entry_trailing_threshold_volatility_weight_step = 0.0
        self._short_filter_volatility_ema_span_0 = 10.0
        self._short_filter_volatility_ema_span_1 = 360.0
        self._short_filter_volatility_ema_span_step = 0.0
        self._short_filter_volume_drop_pct_0 = 0.5
        self._short_filter_volume_drop_pct_1 = 1.0
        self._short_filter_volume_drop_pct_step = 0.0
        self._short_filter_volatility_drop_pct_0 = 0.0
        self._short_filter_volatility_drop_pct_1 = 0.0
        self._short_filter_volatility_drop_pct_step = 0.0
        self._short_filter_volume_ema_span_0 = 10.0
        self._short_filter_volume_ema_span_1 = 360.0
        self._short_filter_volume_ema_span_step = 0.0
        self._short_n_positions_0 = 1.0
        self._short_n_positions_1 = 20.0
        self._short_n_positions_step = 0.0
        self._short_total_wallet_exposure_limit_0 = 0.0
        self._short_total_wallet_exposure_limit_1 = 5.0
        self._short_total_wallet_exposure_limit_step = 0.0
        self._short_unstuck_close_pct_0 = 0.001
        self._short_unstuck_close_pct_1 = 0.1
        self._short_unstuck_close_pct_step = 0.0
        self._short_unstuck_ema_dist_0 = -0.1
        self._short_unstuck_ema_dist_1 = 0.01
        self._short_unstuck_ema_dist_step = 0.0
        self._short_unstuck_loss_allowance_pct_0 = 0.0
        self._short_unstuck_loss_allowance_pct_1 = 0.05
        self._short_unstuck_loss_allowance_pct_step = 0.0
        self._short_unstuck_threshold_0 = 0.4
        self._short_unstuck_threshold_1 = 0.95
        self._short_unstuck_threshold_step = 0.0
        self._short_risk_wel_enforcer_threshold_0 = 0.8
        self._short_risk_wel_enforcer_threshold_1 = 1.2
        self._short_risk_wel_enforcer_threshold_step = 0.0
        self._short_risk_we_excess_allowance_pct_0 = 0.0
        self._short_risk_we_excess_allowance_pct_1 = 0.5
        self._short_risk_we_excess_allowance_pct_step = 0.0
        self._short_risk_twel_enforcer_threshold_0 = 0.8
        self._short_risk_twel_enforcer_threshold_1 = 1.2
        self._short_risk_twel_enforcer_threshold_step = 0.0
        self._bounds = {
                # "long_close_grid_markup_range": [self._long_close_grid_markup_range_0, self._long_close_grid_markup_range_1],
                # "long_close_grid_min_markup": [self._long_close_grid_min_markup_0, self._long_close_grid_min_markup_1],
            "long_close_grid_markup_end": [self._long_close_grid_markup_end_0, self._long_close_grid_markup_end_1, self._long_close_grid_markup_end_step],
                "long_close_grid_markup_start": [self._long_close_grid_markup_start_0, self._long_close_grid_markup_start_1, self._long_close_grid_markup_start_step],
                "long_close_grid_qty_pct": [self._long_close_grid_qty_pct_0, self._long_close_grid_qty_pct_1, self._long_close_grid_qty_pct_step],
                "long_close_trailing_grid_ratio": [self._long_close_trailing_grid_ratio_0, self._long_close_trailing_grid_ratio_1, self._long_close_trailing_grid_ratio_step],
                "long_close_trailing_qty_pct": [self._long_close_trailing_qty_pct_0, self._long_close_trailing_qty_pct_1, self._long_close_trailing_qty_pct_step],
                "long_close_trailing_retracement_pct": [self._long_close_trailing_retracement_pct_0, self._long_close_trailing_retracement_pct_1, self._long_close_trailing_retracement_pct_step],
                "long_close_trailing_threshold_pct": [self._long_close_trailing_threshold_pct_0, self._long_close_trailing_threshold_pct_1, self._long_close_trailing_threshold_pct_step],
                "long_ema_span_0": [self._long_ema_span_0_0, self._long_ema_span_0_1, self._long_ema_span_0_step],
                "long_ema_span_1": [self._long_ema_span_1_0, self._long_ema_span_1_1, self._long_ema_span_1_step],
                "long_entry_grid_double_down_factor": [self._long_entry_grid_double_down_factor_0, self._long_entry_grid_double_down_factor_1, self._long_entry_grid_double_down_factor_step],
                "long_entry_volatility_ema_span_hours": [self._long_entry_volatility_ema_span_hours_0, self._long_entry_volatility_ema_span_hours_1, self._long_entry_volatility_ema_span_hours_step],
                "long_entry_grid_spacing_volatility_weight": [self._long_entry_grid_spacing_volatility_weight_0, self._long_entry_grid_spacing_volatility_weight_1, self._long_entry_grid_spacing_volatility_weight_step],
                "long_entry_grid_spacing_pct": [self._long_entry_grid_spacing_pct_0, self._long_entry_grid_spacing_pct_1, self._long_entry_grid_spacing_pct_step],
                "long_entry_grid_spacing_we_weight": [self._long_entry_grid_spacing_we_weight_0, self._long_entry_grid_spacing_we_weight_1, self._long_entry_grid_spacing_we_weight_step],
                "long_entry_initial_ema_dist": [self._long_entry_initial_ema_dist_0, self._long_entry_initial_ema_dist_1, self._long_entry_initial_ema_dist_step],
                "long_entry_initial_qty_pct": [self._long_entry_initial_qty_pct_0, self._long_entry_initial_qty_pct_1, self._long_entry_initial_qty_pct_step],
                "long_entry_trailing_double_down_factor": [self._long_entry_trailing_double_down_factor_0, self._long_entry_trailing_double_down_factor_1, self._long_entry_trailing_double_down_factor_step],
                "long_entry_trailing_grid_ratio": [self._long_entry_trailing_grid_ratio_0, self._long_entry_trailing_grid_ratio_1, self._long_entry_trailing_grid_ratio_step],
                "long_entry_trailing_retracement_pct": [self._long_entry_trailing_retracement_pct_0, self._long_entry_trailing_retracement_pct_1, self._long_entry_trailing_retracement_pct_step],
                "long_entry_trailing_retracement_we_weight": [self._long_entry_trailing_retracement_we_weight_0, self._long_entry_trailing_retracement_we_weight_1, self._long_entry_trailing_retracement_we_weight_step],
                "long_entry_trailing_retracement_volatility_weight": [self._long_entry_trailing_retracement_volatility_weight_0, self._long_entry_trailing_retracement_volatility_weight_1, self._long_entry_trailing_retracement_volatility_weight_step],
                "long_entry_trailing_threshold_pct": [self._long_entry_trailing_threshold_pct_0, self._long_entry_trailing_threshold_pct_1, self._long_entry_trailing_threshold_pct_step],
                "long_entry_trailing_threshold_we_weight": [self._long_entry_trailing_threshold_we_weight_0, self._long_entry_trailing_threshold_we_weight_1, self._long_entry_trailing_threshold_we_weight_step],
                "long_entry_trailing_threshold_volatility_weight": [self._long_entry_trailing_threshold_volatility_weight_0, self._long_entry_trailing_threshold_volatility_weight_1, self._long_entry_trailing_threshold_volatility_weight_step],
                "long_filter_volatility_ema_span": [self._long_filter_volatility_ema_span_0, self._long_filter_volatility_ema_span_1, self._long_filter_volatility_ema_span_step],
                "long_filter_volume_drop_pct": [self._long_filter_volume_drop_pct_0, self._long_filter_volume_drop_pct_1, self._long_filter_volume_drop_pct_step],
                "long_filter_volatility_drop_pct": [self._long_filter_volatility_drop_pct_0, self._long_filter_volatility_drop_pct_1, self._long_filter_volatility_drop_pct_step],
                "long_filter_volume_ema_span": [self._long_filter_volume_ema_span_0, self._long_filter_volume_ema_span_1, self._long_filter_volume_ema_span_step],
                "long_n_positions": [self._long_n_positions_0, self._long_n_positions_1, self._long_n_positions_step],
                "long_total_wallet_exposure_limit": [self._long_total_wallet_exposure_limit_0, self._long_total_wallet_exposure_limit_1, self._long_total_wallet_exposure_limit_step],
                "long_unstuck_close_pct": [self._long_unstuck_close_pct_0, self._long_unstuck_close_pct_1, self._long_unstuck_close_pct_step],
                "long_unstuck_ema_dist": [self._long_unstuck_ema_dist_0, self._long_unstuck_ema_dist_1, self._long_unstuck_ema_dist_step],
                "long_unstuck_loss_allowance_pct": [self._long_unstuck_loss_allowance_pct_0, self._long_unstuck_loss_allowance_pct_1, self._long_unstuck_loss_allowance_pct_step],
                "long_unstuck_threshold": [self._long_unstuck_threshold_0, self._long_unstuck_threshold_1, self._long_unstuck_threshold_step],
                "long_risk_wel_enforcer_threshold": [self._long_risk_wel_enforcer_threshold_0, self._long_risk_wel_enforcer_threshold_1, self._long_risk_wel_enforcer_threshold_step],
                "long_risk_we_excess_allowance_pct": [self._long_risk_we_excess_allowance_pct_0, self._long_risk_we_excess_allowance_pct_1, self._long_risk_we_excess_allowance_pct_step],
                "long_risk_twel_enforcer_threshold": [self._long_risk_twel_enforcer_threshold_0, self._long_risk_twel_enforcer_threshold_1, self._long_risk_twel_enforcer_threshold_step],
                # "short_close_grid_markup_range": [self._short_close_grid_markup_range_0, self._short_close_grid_markup_range_1, self._short_close_grid_markup_range_step],
                # "short_close_grid_min_markup": [self._short_close_grid_min_markup_0, self._short_close_grid_min_markup_1, self._short_close_grid_min_markup_step],
                "short_close_grid_markup_end": [self._short_close_grid_markup_end_0, self._short_close_grid_markup_end_1, self._short_close_grid_markup_end_step],
                "short_close_grid_markup_start": [self._short_close_grid_markup_start_0, self._short_close_grid_markup_start_1, self._short_close_grid_markup_start_step],
                "short_close_grid_qty_pct": [self._short_close_grid_qty_pct_0, self._short_close_grid_qty_pct_1, self._short_close_grid_qty_pct_step],
                "short_close_trailing_grid_ratio": [self._short_close_trailing_grid_ratio_0, self._short_close_trailing_grid_ratio_1, self._short_close_trailing_grid_ratio_step],
                "short_close_trailing_qty_pct": [self._short_close_trailing_qty_pct_0, self._short_close_trailing_qty_pct_1, self._short_close_trailing_qty_pct_step],
                "short_close_trailing_retracement_pct": [self._short_close_trailing_retracement_pct_0, self._short_close_trailing_retracement_pct_1, self._short_close_trailing_retracement_pct_step],
                "short_close_trailing_threshold_pct": [self._short_close_trailing_threshold_pct_0, self._short_close_trailing_threshold_pct_1, self._short_close_trailing_threshold_pct_step],
                "short_ema_span_0": [self._short_ema_span_0_0, self._short_ema_span_0_1, self._short_ema_span_0_step],
                "short_ema_span_1": [self._short_ema_span_1_0, self._short_ema_span_1_1, self._short_ema_span_1_step],
                "short_entry_grid_double_down_factor": [self._short_entry_grid_double_down_factor_0, self._short_entry_grid_double_down_factor_1, self._short_entry_grid_double_down_factor_step],
                "short_entry_volatility_ema_span_hours": [self._short_entry_volatility_ema_span_hours_0, self._short_entry_volatility_ema_span_hours_1, self._short_entry_volatility_ema_span_hours_step],
                "short_entry_grid_spacing_volatility_weight": [self._short_entry_grid_spacing_volatility_weight_0, self._short_entry_grid_spacing_volatility_weight_1, self._short_entry_grid_spacing_volatility_weight_step],
                "short_entry_grid_spacing_pct": [self._short_entry_grid_spacing_pct_0, self._short_entry_grid_spacing_pct_1, self._short_entry_grid_spacing_pct_step],
                "short_entry_grid_spacing_we_weight": [self._short_entry_grid_spacing_we_weight_0, self._short_entry_grid_spacing_we_weight_1, self._short_entry_grid_spacing_we_weight_step],
                "short_entry_initial_ema_dist": [self._short_entry_initial_ema_dist_0, self._short_entry_initial_ema_dist_1, self._short_entry_initial_ema_dist_step],
                "short_entry_initial_qty_pct": [self._short_entry_initial_qty_pct_0, self._short_entry_initial_qty_pct_1, self._short_entry_initial_qty_pct_step],
                "short_entry_trailing_double_down_factor": [self._short_entry_trailing_double_down_factor_0, self._short_entry_trailing_double_down_factor_1, self._short_entry_trailing_double_down_factor_step],
                "short_entry_trailing_grid_ratio": [self._short_entry_trailing_grid_ratio_0, self._short_entry_trailing_grid_ratio_1, self._short_entry_trailing_grid_ratio_step],
                "short_entry_trailing_retracement_pct": [self._short_entry_trailing_retracement_pct_0, self._short_entry_trailing_retracement_pct_1, self._short_entry_trailing_retracement_pct_step],
                "short_entry_trailing_retracement_we_weight": [self._short_entry_trailing_retracement_we_weight_0, self._short_entry_trailing_retracement_we_weight_1, self._short_entry_trailing_retracement_we_weight_step],
                "short_entry_trailing_retracement_volatility_weight": [self._short_entry_trailing_retracement_volatility_weight_0, self._short_entry_trailing_retracement_volatility_weight_1, self._short_entry_trailing_retracement_volatility_weight_step],
                "short_entry_trailing_threshold_pct": [self._short_entry_trailing_threshold_pct_0, self._short_entry_trailing_threshold_pct_1, self._short_entry_trailing_threshold_pct_step],
                "short_entry_trailing_threshold_we_weight": [self._short_entry_trailing_threshold_we_weight_0, self._short_entry_trailing_threshold_we_weight_1, self._short_entry_trailing_threshold_we_weight_step],
                "short_entry_trailing_threshold_volatility_weight": [self._short_entry_trailing_threshold_volatility_weight_0, self._short_entry_trailing_threshold_volatility_weight_1, self._short_entry_trailing_threshold_volatility_weight_step],
                "short_filter_volatility_ema_span": [self._short_filter_volatility_ema_span_0, self._short_filter_volatility_ema_span_1, self._short_filter_volatility_ema_span_step],
                "short_filter_volume_drop_pct": [self._short_filter_volume_drop_pct_0, self._short_filter_volume_drop_pct_1, self._short_filter_volume_drop_pct_step],
                "short_filter_volatility_drop_pct": [self._short_filter_volatility_drop_pct_0, self._short_filter_volatility_drop_pct_1, self._short_filter_volatility_drop_pct_step],
                "short_filter_volume_ema_span": [self._short_filter_volume_ema_span_0, self._short_filter_volume_ema_span_1, self._short_filter_volume_ema_span_step],
                "short_n_positions": [self._short_n_positions_0, self._short_n_positions_1, self._short_n_positions_step],
                "short_total_wallet_exposure_limit": [self._short_total_wallet_exposure_limit_0, self._short_total_wallet_exposure_limit_1, self._short_total_wallet_exposure_limit_step],
                "short_unstuck_close_pct": [self._short_unstuck_close_pct_0, self._short_unstuck_close_pct_1, self._short_unstuck_close_pct_step],
                "short_unstuck_ema_dist": [self._short_unstuck_ema_dist_0, self._short_unstuck_ema_dist_1, self._short_unstuck_ema_dist_step],
                "short_unstuck_loss_allowance_pct": [self._short_unstuck_loss_allowance_pct_0, self._short_unstuck_loss_allowance_pct_1, self._short_unstuck_loss_allowance_pct_step],
                "short_unstuck_threshold": [self._short_unstuck_threshold_0, self._short_unstuck_threshold_1, self._short_unstuck_threshold_step],
                "short_risk_wel_enforcer_threshold": [self._short_risk_wel_enforcer_threshold_0, self._short_risk_wel_enforcer_threshold_1, self._short_risk_wel_enforcer_threshold_step],
                "short_risk_we_excess_allowance_pct": [self._short_risk_we_excess_allowance_pct_0, self._short_risk_we_excess_allowance_pct_1, self._short_risk_we_excess_allowance_pct_step],
                "short_risk_twel_enforcer_threshold": [self._short_risk_twel_enforcer_threshold_0, self._short_risk_twel_enforcer_threshold_1, self._short_risk_twel_enforcer_threshold_step]
            }
    
    def __repr__(self):
        return str(self._bounds)

    @property
    def bounds(self):
        """Return bounds for export.

        PB7 supports bounds in the forms:
        - [low, high] (continuous)
        - [low, high, step] (grid) where step must be > 0

        PBGui historically stores step=0.0 to mean "disabled".
        When exporting, omit the step element unless it is strictly > 0.
        """

        exported = {}
        for key, val in self._bounds.items():
            if isinstance(val, (list, tuple)) and len(val) >= 2:
                lo, hi = val[0], val[1]
                if len(val) >= 3:
                    step = val[2]
                    try:
                        step_f = float(step)
                    except Exception:
                        step_f = 0.0
                    if step_f > 0.0:
                        exported[key] = [lo, hi, step]
                    else:
                        exported[key] = [lo, hi]
                else:
                    exported[key] = [lo, hi]
            else:
                exported[key] = val
        return exported
    
    @bounds.setter
    def bounds(self, new_bounds):
        # Preserve optional step sizes for grid-based optimization.
        # Passivbot v7.6.0 allows bounds lists like [low, high, step].
        if isinstance(new_bounds, dict):
            for k, v in new_bounds.items():
                if k in self._bounds and isinstance(v, (list, tuple)) and len(v) >= 2:
                    self._bounds[k] = list(v)
        # if "long_close_grid_markup_range" in new_bounds:
        #     self.long_close_grid_markup_range_0 = new_bounds["long_close_grid_markup_range"][0]
        #     self.long_close_grid_markup_range_1 = new_bounds["long_close_grid_markup_range"][1]
        # if "long_close_grid_min_markup" in new_bounds:
        #     self.long_close_grid_min_markup_0 = new_bounds["long_close_grid_min_markup"][0]
        #     self.long_close_grid_min_markup_1 = new_bounds["long_close_grid_min_markup"][1]
        if "long_close_grid_markup_end" in new_bounds:
            self.long_close_grid_markup_end_0 = new_bounds["long_close_grid_markup_end"][0]
            self.long_close_grid_markup_end_1 = new_bounds["long_close_grid_markup_end"][1]
            if isinstance(new_bounds["long_close_grid_markup_end"], (list, tuple)) and len(new_bounds["long_close_grid_markup_end"]) >= 3:
                self.long_close_grid_markup_end_step = new_bounds["long_close_grid_markup_end"][2]
            else:
                self.long_close_grid_markup_end_step = 0.0
        if "long_close_grid_markup_start" in new_bounds:
            self.long_close_grid_markup_start_0 = new_bounds["long_close_grid_markup_start"][0]
            self.long_close_grid_markup_start_1 = new_bounds["long_close_grid_markup_start"][1]
            if isinstance(new_bounds["long_close_grid_markup_start"], (list, tuple)) and len(new_bounds["long_close_grid_markup_start"]) >= 3:
                self.long_close_grid_markup_start_step = new_bounds["long_close_grid_markup_start"][2]
            else:
                self.long_close_grid_markup_start_step = 0.0
        if "long_close_grid_qty_pct" in new_bounds:
            self.long_close_grid_qty_pct_0 = new_bounds["long_close_grid_qty_pct"][0]
            self.long_close_grid_qty_pct_1 = new_bounds["long_close_grid_qty_pct"][1]
            if isinstance(new_bounds["long_close_grid_qty_pct"], (list, tuple)) and len(new_bounds["long_close_grid_qty_pct"]) >= 3:
                self.long_close_grid_qty_pct_step = new_bounds["long_close_grid_qty_pct"][2]
            else:
                self.long_close_grid_qty_pct_step = 0.0
        if "long_close_trailing_grid_ratio" in new_bounds:
            self.long_close_trailing_grid_ratio_0 = new_bounds["long_close_trailing_grid_ratio"][0]
            self.long_close_trailing_grid_ratio_1 = new_bounds["long_close_trailing_grid_ratio"][1]
            if isinstance(new_bounds["long_close_trailing_grid_ratio"], (list, tuple)) and len(new_bounds["long_close_trailing_grid_ratio"]) >= 3:
                self.long_close_trailing_grid_ratio_step = new_bounds["long_close_trailing_grid_ratio"][2]
            else:
                self.long_close_trailing_grid_ratio_step = 0.0
        if "long_close_trailing_qty_pct" in new_bounds:
            self.long_close_trailing_qty_pct_0 = new_bounds["long_close_trailing_qty_pct"][0]
            self.long_close_trailing_qty_pct_1 = new_bounds["long_close_trailing_qty_pct"][1]
            if isinstance(new_bounds["long_close_trailing_qty_pct"], (list, tuple)) and len(new_bounds["long_close_trailing_qty_pct"]) >= 3:
                self.long_close_trailing_qty_pct_step = new_bounds["long_close_trailing_qty_pct"][2]
            else:
                self.long_close_trailing_qty_pct_step = 0.0
        if "long_close_trailing_retracement_pct" in new_bounds:
            self.long_close_trailing_retracement_pct_0 = new_bounds["long_close_trailing_retracement_pct"][0]
            self.long_close_trailing_retracement_pct_1 = new_bounds["long_close_trailing_retracement_pct"][1]
            if isinstance(new_bounds["long_close_trailing_retracement_pct"], (list, tuple)) and len(new_bounds["long_close_trailing_retracement_pct"]) >= 3:
                self.long_close_trailing_retracement_pct_step = new_bounds["long_close_trailing_retracement_pct"][2]
            else:
                self.long_close_trailing_retracement_pct_step = 0.0
        if "long_close_trailing_threshold_pct" in new_bounds:
            self.long_close_trailing_threshold_pct_0 = new_bounds["long_close_trailing_threshold_pct"][0]
            self.long_close_trailing_threshold_pct_1 = new_bounds["long_close_trailing_threshold_pct"][1]
            if isinstance(new_bounds["long_close_trailing_threshold_pct"], (list, tuple)) and len(new_bounds["long_close_trailing_threshold_pct"]) >= 3:
                self.long_close_trailing_threshold_pct_step = new_bounds["long_close_trailing_threshold_pct"][2]
            else:
                self.long_close_trailing_threshold_pct_step = 0.0
        if "long_ema_span_0" in new_bounds:
            self.long_ema_span_0_0 = new_bounds["long_ema_span_0"][0]
            self.long_ema_span_0_1 = new_bounds["long_ema_span_0"][1]
            if isinstance(new_bounds["long_ema_span_0"], (list, tuple)) and len(new_bounds["long_ema_span_0"]) >= 3:
                self.long_ema_span_0_step = new_bounds["long_ema_span_0"][2]
            else:
                self.long_ema_span_0_step = 0.0
        if "long_ema_span_1" in new_bounds:
            self.long_ema_span_1_0 = new_bounds["long_ema_span_1"][0]
            self.long_ema_span_1_1 = new_bounds["long_ema_span_1"][1]
            if isinstance(new_bounds["long_ema_span_1"], (list, tuple)) and len(new_bounds["long_ema_span_1"]) >= 3:
                self.long_ema_span_1_step = new_bounds["long_ema_span_1"][2]
            else:
                self.long_ema_span_1_step = 0.0
        if "long_entry_grid_double_down_factor" in new_bounds:
            self.long_entry_grid_double_down_factor_0 = new_bounds["long_entry_grid_double_down_factor"][0]
            self.long_entry_grid_double_down_factor_1 = new_bounds["long_entry_grid_double_down_factor"][1]
            if isinstance(new_bounds["long_entry_grid_double_down_factor"], (list, tuple)) and len(new_bounds["long_entry_grid_double_down_factor"]) >= 3:
                self.long_entry_grid_double_down_factor_step = new_bounds["long_entry_grid_double_down_factor"][2]
            else:
                self.long_entry_grid_double_down_factor_step = 0.0
        if "long_entry_volatility_ema_span_hours" in new_bounds:
            self.long_entry_volatility_ema_span_hours_0 = new_bounds["long_entry_volatility_ema_span_hours"][0]
            self.long_entry_volatility_ema_span_hours_1 = new_bounds["long_entry_volatility_ema_span_hours"][1]
            if isinstance(new_bounds["long_entry_volatility_ema_span_hours"], (list, tuple)) and len(new_bounds["long_entry_volatility_ema_span_hours"]) >= 3:
                self.long_entry_volatility_ema_span_hours_step = new_bounds["long_entry_volatility_ema_span_hours"][2]
            else:
                self.long_entry_volatility_ema_span_hours_step = 0.0
        # Fix for old configs
        elif "long_entry_grid_spacing_log_span_hours" in new_bounds:
            self.long_entry_volatility_ema_span_hours_0 = new_bounds["long_entry_grid_spacing_log_span_hours"][0]
            self.long_entry_volatility_ema_span_hours_1 = new_bounds["long_entry_grid_spacing_log_span_hours"][1]
        if "long_entry_grid_spacing_volatility_weight" in new_bounds:
            self.long_entry_grid_spacing_volatility_weight_0 = new_bounds["long_entry_grid_spacing_volatility_weight"][0]
            self.long_entry_grid_spacing_volatility_weight_1 = new_bounds["long_entry_grid_spacing_volatility_weight"][1]
            if isinstance(new_bounds["long_entry_grid_spacing_volatility_weight"], (list, tuple)) and len(new_bounds["long_entry_grid_spacing_volatility_weight"]) >= 3:
                self.long_entry_grid_spacing_volatility_weight_step = new_bounds["long_entry_grid_spacing_volatility_weight"][2]
            else:
                self.long_entry_grid_spacing_volatility_weight_step = 0.0
        # Fix for old configs
        elif "long_entry_grid_spacing_log_weight" in new_bounds:
            self.long_entry_grid_spacing_volatility_weight_0 = new_bounds["long_entry_grid_spacing_log_weight"][0]
            self.long_entry_grid_spacing_volatility_weight_1 = new_bounds["long_entry_grid_spacing_log_weight"][1]
        if "long_entry_grid_spacing_pct" in new_bounds:
            self.long_entry_grid_spacing_pct_0 = new_bounds["long_entry_grid_spacing_pct"][0]
            self.long_entry_grid_spacing_pct_1 = new_bounds["long_entry_grid_spacing_pct"][1]
            if isinstance(new_bounds["long_entry_grid_spacing_pct"], (list, tuple)) and len(new_bounds["long_entry_grid_spacing_pct"]) >= 3:
                self.long_entry_grid_spacing_pct_step = new_bounds["long_entry_grid_spacing_pct"][2]
            else:
                self.long_entry_grid_spacing_pct_step = 0.0
        if "long_entry_grid_spacing_we_weight" in new_bounds:
            self.long_entry_grid_spacing_we_weight_0 = new_bounds["long_entry_grid_spacing_we_weight"][0]
            self.long_entry_grid_spacing_we_weight_1 = new_bounds["long_entry_grid_spacing_we_weight"][1]
            if isinstance(new_bounds["long_entry_grid_spacing_we_weight"], (list, tuple)) and len(new_bounds["long_entry_grid_spacing_we_weight"]) >= 3:
                self.long_entry_grid_spacing_we_weight_step = new_bounds["long_entry_grid_spacing_we_weight"][2]
            else:
                self.long_entry_grid_spacing_we_weight_step = 0.0
        if "long_entry_initial_ema_dist" in new_bounds:
            self.long_entry_initial_ema_dist_0 = new_bounds["long_entry_initial_ema_dist"][0]
            self.long_entry_initial_ema_dist_1 = new_bounds["long_entry_initial_ema_dist"][1]
            if isinstance(new_bounds["long_entry_initial_ema_dist"], (list, tuple)) and len(new_bounds["long_entry_initial_ema_dist"]) >= 3:
                self.long_entry_initial_ema_dist_step = new_bounds["long_entry_initial_ema_dist"][2]
            else:
                self.long_entry_initial_ema_dist_step = 0.0
        if "long_entry_initial_qty_pct" in new_bounds:
            self.long_entry_initial_qty_pct_0 = new_bounds["long_entry_initial_qty_pct"][0]
            self.long_entry_initial_qty_pct_1 = new_bounds["long_entry_initial_qty_pct"][1]
            if isinstance(new_bounds["long_entry_initial_qty_pct"], (list, tuple)) and len(new_bounds["long_entry_initial_qty_pct"]) >= 3:
                self.long_entry_initial_qty_pct_step = new_bounds["long_entry_initial_qty_pct"][2]
            else:
                self.long_entry_initial_qty_pct_step = 0.0
        if "long_entry_trailing_double_down_factor" in new_bounds:
            self.long_entry_trailing_double_down_factor_0 = new_bounds["long_entry_trailing_double_down_factor"][0]
            self.long_entry_trailing_double_down_factor_1 = new_bounds["long_entry_trailing_double_down_factor"][1]
            if isinstance(new_bounds["long_entry_trailing_double_down_factor"], (list, tuple)) and len(new_bounds["long_entry_trailing_double_down_factor"]) >= 3:
                self.long_entry_trailing_double_down_factor_step = new_bounds["long_entry_trailing_double_down_factor"][2]
            else:
                self.long_entry_trailing_double_down_factor_step = 0.0
        if "long_entry_trailing_grid_ratio" in new_bounds:
            self.long_entry_trailing_grid_ratio_0 = new_bounds["long_entry_trailing_grid_ratio"][0]
            self.long_entry_trailing_grid_ratio_1 = new_bounds["long_entry_trailing_grid_ratio"][1]
            if isinstance(new_bounds["long_entry_trailing_grid_ratio"], (list, tuple)) and len(new_bounds["long_entry_trailing_grid_ratio"]) >= 3:
                self.long_entry_trailing_grid_ratio_step = new_bounds["long_entry_trailing_grid_ratio"][2]
            else:
                self.long_entry_trailing_grid_ratio_step = 0.0
        if "long_entry_trailing_retracement_pct" in new_bounds:
            self.long_entry_trailing_retracement_pct_0 = new_bounds["long_entry_trailing_retracement_pct"][0]
            self.long_entry_trailing_retracement_pct_1 = new_bounds["long_entry_trailing_retracement_pct"][1]
            if isinstance(new_bounds["long_entry_trailing_retracement_pct"], (list, tuple)) and len(new_bounds["long_entry_trailing_retracement_pct"]) >= 3:
                self.long_entry_trailing_retracement_pct_step = new_bounds["long_entry_trailing_retracement_pct"][2]
            else:
                self.long_entry_trailing_retracement_pct_step = 0.0
        if "long_entry_trailing_threshold_pct" in new_bounds:
            self.long_entry_trailing_threshold_pct_0 = new_bounds["long_entry_trailing_threshold_pct"][0]
            self.long_entry_trailing_threshold_pct_1 = new_bounds["long_entry_trailing_threshold_pct"][1]
            if isinstance(new_bounds["long_entry_trailing_threshold_pct"], (list, tuple)) and len(new_bounds["long_entry_trailing_threshold_pct"]) >= 3:
                self.long_entry_trailing_threshold_pct_step = new_bounds["long_entry_trailing_threshold_pct"][2]
            else:
                self.long_entry_trailing_threshold_pct_step = 0.0
        if "long_entry_trailing_threshold_we_weight" in new_bounds:
            self.long_entry_trailing_threshold_we_weight_0 = new_bounds["long_entry_trailing_threshold_we_weight"][0]
            self.long_entry_trailing_threshold_we_weight_1 = new_bounds["long_entry_trailing_threshold_we_weight"][1]
            if isinstance(new_bounds["long_entry_trailing_threshold_we_weight"], (list, tuple)) and len(new_bounds["long_entry_trailing_threshold_we_weight"]) >= 3:
                self.long_entry_trailing_threshold_we_weight_step = new_bounds["long_entry_trailing_threshold_we_weight"][2]
            else:
                self.long_entry_trailing_threshold_we_weight_step = 0.0
        if "long_entry_trailing_threshold_volatility_weight" in new_bounds:
            self.long_entry_trailing_threshold_volatility_weight_0 = new_bounds["long_entry_trailing_threshold_volatility_weight"][0]
            self.long_entry_trailing_threshold_volatility_weight_1 = new_bounds["long_entry_trailing_threshold_volatility_weight"][1]
            if isinstance(new_bounds["long_entry_trailing_threshold_volatility_weight"], (list, tuple)) and len(new_bounds["long_entry_trailing_threshold_volatility_weight"]) >= 3:
                self.long_entry_trailing_threshold_volatility_weight_step = new_bounds["long_entry_trailing_threshold_volatility_weight"][2]
            else:
                self.long_entry_trailing_threshold_volatility_weight_step = 0.0
        if "long_entry_trailing_retracement_we_weight" in new_bounds:
            self.long_entry_trailing_retracement_we_weight_0 = new_bounds["long_entry_trailing_retracement_we_weight"][0]
            self.long_entry_trailing_retracement_we_weight_1 = new_bounds["long_entry_trailing_retracement_we_weight"][1]
            if isinstance(new_bounds["long_entry_trailing_retracement_we_weight"], (list, tuple)) and len(new_bounds["long_entry_trailing_retracement_we_weight"]) >= 3:
                self.long_entry_trailing_retracement_we_weight_step = new_bounds["long_entry_trailing_retracement_we_weight"][2]
            else:
                self.long_entry_trailing_retracement_we_weight_step = 0.0
        if "long_entry_trailing_retracement_volatility_weight" in new_bounds:
            self.long_entry_trailing_retracement_volatility_weight_0 = new_bounds["long_entry_trailing_retracement_volatility_weight"][0]
            self.long_entry_trailing_retracement_volatility_weight_1 = new_bounds["long_entry_trailing_retracement_volatility_weight"][1]
            if isinstance(new_bounds["long_entry_trailing_retracement_volatility_weight"], (list, tuple)) and len(new_bounds["long_entry_trailing_retracement_volatility_weight"]) >= 3:
                self.long_entry_trailing_retracement_volatility_weight_step = new_bounds["long_entry_trailing_retracement_volatility_weight"][2]
            else:
                self.long_entry_trailing_retracement_volatility_weight_step = 0.0
        if "long_filter_volatility_ema_span" in new_bounds:
            self.long_filter_volatility_ema_span_0 = new_bounds["long_filter_volatility_ema_span"][0]
            self.long_filter_volatility_ema_span_1 = new_bounds["long_filter_volatility_ema_span"][1]
            if isinstance(new_bounds["long_filter_volatility_ema_span"], (list, tuple)) and len(new_bounds["long_filter_volatility_ema_span"]) >= 3:
                self.long_filter_volatility_ema_span_step = new_bounds["long_filter_volatility_ema_span"][2]
            else:
                self.long_filter_volatility_ema_span_step = 0.0
        # Fix for old configs
        elif "long_filter_log_range_ema_span" in new_bounds:
            self.long_filter_volatility_ema_span_0 = new_bounds["long_filter_log_range_ema_span"][0]
            self.long_filter_volatility_ema_span_1 = new_bounds["long_filter_log_range_ema_span"][1]
        # Fix for old configs
        elif "long_filter_noisiness_rolling_window" in new_bounds:
            self.long_filter_log_range_ema_span_0 = new_bounds["long_filter_noisiness_rolling_window"][0]
            self.long_filter_log_range_ema_span_1 = new_bounds["long_filter_noisiness_rolling_window"][1]
        elif "long_filter_rolling_window" in new_bounds:
            self.long_filter_log_range_ema_span_0 = new_bounds["long_filter_rolling_window"][0]
            self.long_filter_log_range_ema_span_1 = new_bounds["long_filter_rolling_window"][1]
        if "long_filter_volume_drop_pct" in new_bounds:
            self.long_filter_volume_drop_pct_0 = new_bounds["long_filter_volume_drop_pct"][0]
            self.long_filter_volume_drop_pct_1 = new_bounds["long_filter_volume_drop_pct"][1]
            if isinstance(new_bounds["long_filter_volume_drop_pct"], (list, tuple)) and len(new_bounds["long_filter_volume_drop_pct"]) >= 3:
                self.long_filter_volume_drop_pct_step = new_bounds["long_filter_volume_drop_pct"][2]
            else:
                self.long_filter_volume_drop_pct_step = 0.0
        # Fix for old configs
        elif "long_filter_relative_volume_clip_pct" in new_bounds:
            self.long_filter_volume_drop_pct_0 = new_bounds["long_filter_relative_volume_clip_pct"][0]
            self.long_filter_volume_drop_pct_1 = new_bounds["long_filter_relative_volume_clip_pct"][1]
        if "long_filter_volatility_drop_pct" in new_bounds:
            self.long_filter_volatility_drop_pct_0 = new_bounds["long_filter_volatility_drop_pct"][0]
            self.long_filter_volatility_drop_pct_1 = new_bounds["long_filter_volatility_drop_pct"][1]
            if isinstance(new_bounds["long_filter_volatility_drop_pct"], (list, tuple)) and len(new_bounds["long_filter_volatility_drop_pct"]) >= 3:
                self.long_filter_volatility_drop_pct_step = new_bounds["long_filter_volatility_drop_pct"][2]
            else:
                self.long_filter_volatility_drop_pct_step = 0.0
        if "long_filter_volume_ema_span" in new_bounds:
            self.long_filter_volume_ema_span_0 = new_bounds["long_filter_volume_ema_span"][0]
            self.long_filter_volume_ema_span_1 = new_bounds["long_filter_volume_ema_span"][1]
            if isinstance(new_bounds["long_filter_volume_ema_span"], (list, tuple)) and len(new_bounds["long_filter_volume_ema_span"]) >= 3:
                self.long_filter_volume_ema_span_step = new_bounds["long_filter_volume_ema_span"][2]
            else:
                self.long_filter_volume_ema_span_step = 0.0
        # Fix for old configs
        elif "long_filter_rolling_window" in new_bounds:
            self.long_filter_volume_ema_span_0 = new_bounds["long_filter_rolling_window"][0]
            self.long_filter_volume_ema_span_1 = new_bounds["long_filter_rolling_window"][1]
        if "long_n_positions" in new_bounds:
            self.long_n_positions_0 = new_bounds["long_n_positions"][0]
            self.long_n_positions_1 = new_bounds["long_n_positions"][1]
            if isinstance(new_bounds["long_n_positions"], (list, tuple)) and len(new_bounds["long_n_positions"]) >= 3:
                self.long_n_positions_step = new_bounds["long_n_positions"][2]
            else:
                self.long_n_positions_step = 0.0
        if "long_total_wallet_exposure_limit" in new_bounds:
            self.long_total_wallet_exposure_limit_0 = new_bounds["long_total_wallet_exposure_limit"][0]
            self.long_total_wallet_exposure_limit_1 = new_bounds["long_total_wallet_exposure_limit"][1]
            if isinstance(new_bounds["long_total_wallet_exposure_limit"], (list, tuple)) and len(new_bounds["long_total_wallet_exposure_limit"]) >= 3:
                self.long_total_wallet_exposure_limit_step = new_bounds["long_total_wallet_exposure_limit"][2]
            else:
                self.long_total_wallet_exposure_limit_step = 0.0
        if "long_unstuck_close_pct" in new_bounds:
            self.long_unstuck_close_pct_0 = new_bounds["long_unstuck_close_pct"][0]
            self.long_unstuck_close_pct_1 = new_bounds["long_unstuck_close_pct"][1]
            if isinstance(new_bounds["long_unstuck_close_pct"], (list, tuple)) and len(new_bounds["long_unstuck_close_pct"]) >= 3:
                self.long_unstuck_close_pct_step = new_bounds["long_unstuck_close_pct"][2]
            else:
                self.long_unstuck_close_pct_step = 0.0
        if "long_unstuck_ema_dist" in new_bounds:
            self.long_unstuck_ema_dist_0 = new_bounds["long_unstuck_ema_dist"][0]
            self.long_unstuck_ema_dist_1 = new_bounds["long_unstuck_ema_dist"][1]
            if isinstance(new_bounds["long_unstuck_ema_dist"], (list, tuple)) and len(new_bounds["long_unstuck_ema_dist"]) >= 3:
                self.long_unstuck_ema_dist_step = new_bounds["long_unstuck_ema_dist"][2]
            else:
                self.long_unstuck_ema_dist_step = 0.0
        if "long_unstuck_loss_allowance_pct" in new_bounds:
            self.long_unstuck_loss_allowance_pct_0 = new_bounds["long_unstuck_loss_allowance_pct"][0]
            self.long_unstuck_loss_allowance_pct_1 = new_bounds["long_unstuck_loss_allowance_pct"][1]
            if isinstance(new_bounds["long_unstuck_loss_allowance_pct"], (list, tuple)) and len(new_bounds["long_unstuck_loss_allowance_pct"]) >= 3:
                self.long_unstuck_loss_allowance_pct_step = new_bounds["long_unstuck_loss_allowance_pct"][2]
            else:
                self.long_unstuck_loss_allowance_pct_step = 0.0
        if "long_unstuck_threshold" in new_bounds:
            self.long_unstuck_threshold_0 = new_bounds["long_unstuck_threshold"][0]
            self.long_unstuck_threshold_1 = new_bounds["long_unstuck_threshold"][1]
            if isinstance(new_bounds["long_unstuck_threshold"], (list, tuple)) and len(new_bounds["long_unstuck_threshold"]) >= 3:
                self.long_unstuck_threshold_step = new_bounds["long_unstuck_threshold"][2]
            else:
                self.long_unstuck_threshold_step = 0.0
    
        # Short parameters
        # if "short_close_grid_markup_range" in new_bounds:
        #     self.short_close_grid_markup_range_0 = new_bounds["short_close_grid_markup_range"][0]
        #     self.short_close_grid_markup_range_1 = new_bounds["short_close_grid_markup_range"][1]
        # if "short_close_grid_min_markup" in new_bounds:
        #     self.short_close_grid_min_markup_0 = new_bounds["short_close_grid_min_markup"][0]
        #     self.short_close_grid_min_markup_1 = new_bounds["short_close_grid_min_markup"][1]
        if "short_close_grid_markup_end" in new_bounds:
            self.short_close_grid_markup_end_0 = new_bounds["short_close_grid_markup_end"][0]
            self.short_close_grid_markup_end_1 = new_bounds["short_close_grid_markup_end"][1]
            if isinstance(new_bounds["short_close_grid_markup_end"], (list, tuple)) and len(new_bounds["short_close_grid_markup_end"]) >= 3:
                self.short_close_grid_markup_end_step = new_bounds["short_close_grid_markup_end"][2]
            else:
                self.short_close_grid_markup_end_step = 0.0
        if "short_close_grid_markup_start" in new_bounds:
            self.short_close_grid_markup_start_0 = new_bounds["short_close_grid_markup_start"][0]
            self.short_close_grid_markup_start_1 = new_bounds["short_close_grid_markup_start"][1]
            if isinstance(new_bounds["short_close_grid_markup_start"], (list, tuple)) and len(new_bounds["short_close_grid_markup_start"]) >= 3:
                self.short_close_grid_markup_start_step = new_bounds["short_close_grid_markup_start"][2]
            else:
                self.short_close_grid_markup_start_step = 0.0
        if "short_close_grid_qty_pct" in new_bounds:
            self.short_close_grid_qty_pct_0 = new_bounds["short_close_grid_qty_pct"][0]
            self.short_close_grid_qty_pct_1 = new_bounds["short_close_grid_qty_pct"][1]
            if isinstance(new_bounds["short_close_grid_qty_pct"], (list, tuple)) and len(new_bounds["short_close_grid_qty_pct"]) >= 3:
                self.short_close_grid_qty_pct_step = new_bounds["short_close_grid_qty_pct"][2]
            else:
                self.short_close_grid_qty_pct_step = 0.0
        if "short_close_trailing_grid_ratio" in new_bounds:
            self.short_close_trailing_grid_ratio_0 = new_bounds["short_close_trailing_grid_ratio"][0]
            self.short_close_trailing_grid_ratio_1 = new_bounds["short_close_trailing_grid_ratio"][1]
            if isinstance(new_bounds["short_close_trailing_grid_ratio"], (list, tuple)) and len(new_bounds["short_close_trailing_grid_ratio"]) >= 3:
                self.short_close_trailing_grid_ratio_step = new_bounds["short_close_trailing_grid_ratio"][2]
            else:
                self.short_close_trailing_grid_ratio_step = 0.0
        if "short_close_trailing_qty_pct" in new_bounds:
            self.short_close_trailing_qty_pct_0 = new_bounds["short_close_trailing_qty_pct"][0]
            self.short_close_trailing_qty_pct_1 = new_bounds["short_close_trailing_qty_pct"][1]
            if isinstance(new_bounds["short_close_trailing_qty_pct"], (list, tuple)) and len(new_bounds["short_close_trailing_qty_pct"]) >= 3:
                self.short_close_trailing_qty_pct_step = new_bounds["short_close_trailing_qty_pct"][2]
            else:
                self.short_close_trailing_qty_pct_step = 0.0
        if "short_close_trailing_retracement_pct" in new_bounds:
            self.short_close_trailing_retracement_pct_0 = new_bounds["short_close_trailing_retracement_pct"][0]
            self.short_close_trailing_retracement_pct_1 = new_bounds["short_close_trailing_retracement_pct"][1]
            if isinstance(new_bounds["short_close_trailing_retracement_pct"], (list, tuple)) and len(new_bounds["short_close_trailing_retracement_pct"]) >= 3:
                self.short_close_trailing_retracement_pct_step = new_bounds["short_close_trailing_retracement_pct"][2]
            else:
                self.short_close_trailing_retracement_pct_step = 0.0
        if "short_close_trailing_threshold_pct" in new_bounds:
            self.short_close_trailing_threshold_pct_0 = new_bounds["short_close_trailing_threshold_pct"][0]
            self.short_close_trailing_threshold_pct_1 = new_bounds["short_close_trailing_threshold_pct"][1]
            if isinstance(new_bounds["short_close_trailing_threshold_pct"], (list, tuple)) and len(new_bounds["short_close_trailing_threshold_pct"]) >= 3:
                self.short_close_trailing_threshold_pct_step = new_bounds["short_close_trailing_threshold_pct"][2]
            else:
                self.short_close_trailing_threshold_pct_step = 0.0
        if "short_ema_span_0" in new_bounds:
            self.short_ema_span_0_0 = new_bounds["short_ema_span_0"][0]
            self.short_ema_span_0_1 = new_bounds["short_ema_span_0"][1]
            if isinstance(new_bounds["short_ema_span_0"], (list, tuple)) and len(new_bounds["short_ema_span_0"]) >= 3:
                self.short_ema_span_0_step = new_bounds["short_ema_span_0"][2]
            else:
                self.short_ema_span_0_step = 0.0
        if "short_ema_span_1" in new_bounds:
            self.short_ema_span_1_0 = new_bounds["short_ema_span_1"][0]
            self.short_ema_span_1_1 = new_bounds["short_ema_span_1"][1]
            if isinstance(new_bounds["short_ema_span_1"], (list, tuple)) and len(new_bounds["short_ema_span_1"]) >= 3:
                self.short_ema_span_1_step = new_bounds["short_ema_span_1"][2]
            else:
                self.short_ema_span_1_step = 0.0
        if "short_entry_grid_double_down_factor" in new_bounds:
            self.short_entry_grid_double_down_factor_0 = new_bounds["short_entry_grid_double_down_factor"][0]
            self.short_entry_grid_double_down_factor_1 = new_bounds["short_entry_grid_double_down_factor"][1]
            if isinstance(new_bounds["short_entry_grid_double_down_factor"], (list, tuple)) and len(new_bounds["short_entry_grid_double_down_factor"]) >= 3:
                self.short_entry_grid_double_down_factor_step = new_bounds["short_entry_grid_double_down_factor"][2]
            else:
                self.short_entry_grid_double_down_factor_step = 0.0
        if "short_entry_volatility_ema_span_hours" in new_bounds:
            self.short_entry_volatility_ema_span_hours_0 = new_bounds["short_entry_volatility_ema_span_hours"][0]
            self.short_entry_volatility_ema_span_hours_1 = new_bounds["short_entry_volatility_ema_span_hours"][1]
            if isinstance(new_bounds["short_entry_volatility_ema_span_hours"], (list, tuple)) and len(new_bounds["short_entry_volatility_ema_span_hours"]) >= 3:
                self.short_entry_volatility_ema_span_hours_step = new_bounds["short_entry_volatility_ema_span_hours"][2]
            else:
                self.short_entry_volatility_ema_span_hours_step = 0.0
        # Fix for old configs
        elif "short_entry_grid_spacing_log_span_hours" in new_bounds:
            self.short_entry_volatility_ema_span_hours_0 = new_bounds["short_entry_grid_spacing_log_span_hours"][0]
            self.short_entry_volatility_ema_span_hours_1 = new_bounds["short_entry_grid_spacing_log_span_hours"][1]
        if "short_entry_grid_spacing_volatility_weight" in new_bounds:
            self.short_entry_grid_spacing_volatility_weight_0 = new_bounds["short_entry_grid_spacing_volatility_weight"][0]
            self.short_entry_grid_spacing_volatility_weight_1 = new_bounds["short_entry_grid_spacing_volatility_weight"][1]
            if isinstance(new_bounds["short_entry_grid_spacing_volatility_weight"], (list, tuple)) and len(new_bounds["short_entry_grid_spacing_volatility_weight"]) >= 3:
                self.short_entry_grid_spacing_volatility_weight_step = new_bounds["short_entry_grid_spacing_volatility_weight"][2]
            else:
                self.short_entry_grid_spacing_volatility_weight_step = 0.0
        # Fix for old configs
        elif "short_entry_grid_spacing_log_weight" in new_bounds:
            self.short_entry_grid_spacing_volatility_weight_0 = new_bounds["short_entry_grid_spacing_log_weight"][0]
            self.short_entry_grid_spacing_volatility_weight_1 = new_bounds["short_entry_grid_spacing_log_weight"][1]
        if "short_entry_grid_spacing_pct" in new_bounds:
            self.short_entry_grid_spacing_pct_0 = new_bounds["short_entry_grid_spacing_pct"][0]
            self.short_entry_grid_spacing_pct_1 = new_bounds["short_entry_grid_spacing_pct"][1]
            if isinstance(new_bounds["short_entry_grid_spacing_pct"], (list, tuple)) and len(new_bounds["short_entry_grid_spacing_pct"]) >= 3:
                self.short_entry_grid_spacing_pct_step = new_bounds["short_entry_grid_spacing_pct"][2]
            else:
                self.short_entry_grid_spacing_pct_step = 0.0
        if "short_entry_grid_spacing_we_weight" in new_bounds:
            self.short_entry_grid_spacing_we_weight_0 = new_bounds["short_entry_grid_spacing_we_weight"][0]
            self.short_entry_grid_spacing_we_weight_1 = new_bounds["short_entry_grid_spacing_we_weight"][1]
            if isinstance(new_bounds["short_entry_grid_spacing_we_weight"], (list, tuple)) and len(new_bounds["short_entry_grid_spacing_we_weight"]) >= 3:
                self.short_entry_grid_spacing_we_weight_step = new_bounds["short_entry_grid_spacing_we_weight"][2]
            else:
                self.short_entry_grid_spacing_we_weight_step = 0.0
        if "short_entry_initial_ema_dist" in new_bounds:
            self.short_entry_initial_ema_dist_0 = new_bounds["short_entry_initial_ema_dist"][0]
            self.short_entry_initial_ema_dist_1 = new_bounds["short_entry_initial_ema_dist"][1]
            if isinstance(new_bounds["short_entry_initial_ema_dist"], (list, tuple)) and len(new_bounds["short_entry_initial_ema_dist"]) >= 3:
                self.short_entry_initial_ema_dist_step = new_bounds["short_entry_initial_ema_dist"][2]
            else:
                self.short_entry_initial_ema_dist_step = 0.0
        if "short_entry_initial_qty_pct" in new_bounds:
            self.short_entry_initial_qty_pct_0 = new_bounds["short_entry_initial_qty_pct"][0]
            self.short_entry_initial_qty_pct_1 = new_bounds["short_entry_initial_qty_pct"][1]
            if isinstance(new_bounds["short_entry_initial_qty_pct"], (list, tuple)) and len(new_bounds["short_entry_initial_qty_pct"]) >= 3:
                self.short_entry_initial_qty_pct_step = new_bounds["short_entry_initial_qty_pct"][2]
            else:
                self.short_entry_initial_qty_pct_step = 0.0
        if "short_entry_trailing_double_down_factor" in new_bounds:
            self.short_entry_trailing_double_down_factor_0 = new_bounds["short_entry_trailing_double_down_factor"][0]
            self.short_entry_trailing_double_down_factor_1 = new_bounds["short_entry_trailing_double_down_factor"][1]
            if isinstance(new_bounds["short_entry_trailing_double_down_factor"], (list, tuple)) and len(new_bounds["short_entry_trailing_double_down_factor"]) >= 3:
                self.short_entry_trailing_double_down_factor_step = new_bounds["short_entry_trailing_double_down_factor"][2]
            else:
                self.short_entry_trailing_double_down_factor_step = 0.0
        if "short_entry_trailing_grid_ratio" in new_bounds:
            self.short_entry_trailing_grid_ratio_0 = new_bounds["short_entry_trailing_grid_ratio"][0]
            self.short_entry_trailing_grid_ratio_1 = new_bounds["short_entry_trailing_grid_ratio"][1]
            if isinstance(new_bounds["short_entry_trailing_grid_ratio"], (list, tuple)) and len(new_bounds["short_entry_trailing_grid_ratio"]) >= 3:
                self.short_entry_trailing_grid_ratio_step = new_bounds["short_entry_trailing_grid_ratio"][2]
            else:
                self.short_entry_trailing_grid_ratio_step = 0.0
        if "short_entry_trailing_retracement_pct" in new_bounds:
            self.short_entry_trailing_retracement_pct_0 = new_bounds["short_entry_trailing_retracement_pct"][0]
            self.short_entry_trailing_retracement_pct_1 = new_bounds["short_entry_trailing_retracement_pct"][1]
            if isinstance(new_bounds["short_entry_trailing_retracement_pct"], (list, tuple)) and len(new_bounds["short_entry_trailing_retracement_pct"]) >= 3:
                self.short_entry_trailing_retracement_pct_step = new_bounds["short_entry_trailing_retracement_pct"][2]
            else:
                self.short_entry_trailing_retracement_pct_step = 0.0
        if "short_entry_trailing_threshold_pct" in new_bounds:
            self.short_entry_trailing_threshold_pct_0 = new_bounds["short_entry_trailing_threshold_pct"][0]
            self.short_entry_trailing_threshold_pct_1 = new_bounds["short_entry_trailing_threshold_pct"][1]
            if isinstance(new_bounds["short_entry_trailing_threshold_pct"], (list, tuple)) and len(new_bounds["short_entry_trailing_threshold_pct"]) >= 3:
                self.short_entry_trailing_threshold_pct_step = new_bounds["short_entry_trailing_threshold_pct"][2]
            else:
                self.short_entry_trailing_threshold_pct_step = 0.0
        if "short_entry_trailing_threshold_we_weight" in new_bounds:
            self.short_entry_trailing_threshold_we_weight_0 = new_bounds["short_entry_trailing_threshold_we_weight"][0]
            self.short_entry_trailing_threshold_we_weight_1 = new_bounds["short_entry_trailing_threshold_we_weight"][1]
            if isinstance(new_bounds["short_entry_trailing_threshold_we_weight"], (list, tuple)) and len(new_bounds["short_entry_trailing_threshold_we_weight"]) >= 3:
                self.short_entry_trailing_threshold_we_weight_step = new_bounds["short_entry_trailing_threshold_we_weight"][2]
            else:
                self.short_entry_trailing_threshold_we_weight_step = 0.0
        if "short_entry_trailing_threshold_volatility_weight" in new_bounds:
            self.short_entry_trailing_threshold_volatility_weight_0 = new_bounds["short_entry_trailing_threshold_volatility_weight"][0]
            self.short_entry_trailing_threshold_volatility_weight_1 = new_bounds["short_entry_trailing_threshold_volatility_weight"][1]
            if isinstance(new_bounds["short_entry_trailing_threshold_volatility_weight"], (list, tuple)) and len(new_bounds["short_entry_trailing_threshold_volatility_weight"]) >= 3:
                self.short_entry_trailing_threshold_volatility_weight_step = new_bounds["short_entry_trailing_threshold_volatility_weight"][2]
            else:
                self.short_entry_trailing_threshold_volatility_weight_step = 0.0
        if "short_entry_trailing_retracement_we_weight" in new_bounds:
            self.short_entry_trailing_retracement_we_weight_0 = new_bounds["short_entry_trailing_retracement_we_weight"][0]
            self.short_entry_trailing_retracement_we_weight_1 = new_bounds["short_entry_trailing_retracement_we_weight"][1]
            if isinstance(new_bounds["short_entry_trailing_retracement_we_weight"], (list, tuple)) and len(new_bounds["short_entry_trailing_retracement_we_weight"]) >= 3:
                self.short_entry_trailing_retracement_we_weight_step = new_bounds["short_entry_trailing_retracement_we_weight"][2]
            else:
                self.short_entry_trailing_retracement_we_weight_step = 0.0
        if "short_entry_trailing_retracement_volatility_weight" in new_bounds:
            self.short_entry_trailing_retracement_volatility_weight_0 = new_bounds["short_entry_trailing_retracement_volatility_weight"][0]
            self.short_entry_trailing_retracement_volatility_weight_1 = new_bounds["short_entry_trailing_retracement_volatility_weight"][1]
            if isinstance(new_bounds["short_entry_trailing_retracement_volatility_weight"], (list, tuple)) and len(new_bounds["short_entry_trailing_retracement_volatility_weight"]) >= 3:
                self.short_entry_trailing_retracement_volatility_weight_step = new_bounds["short_entry_trailing_retracement_volatility_weight"][2]
            else:
                self.short_entry_trailing_retracement_volatility_weight_step = 0.0
        if "short_filter_volatility_ema_span" in new_bounds:
            self.short_filter_volatility_ema_span_0 = new_bounds["short_filter_volatility_ema_span"][0]
            self.short_filter_volatility_ema_span_1 = new_bounds["short_filter_volatility_ema_span"][1]
            if isinstance(new_bounds["short_filter_volatility_ema_span"], (list, tuple)) and len(new_bounds["short_filter_volatility_ema_span"]) >= 3:
                self.short_filter_volatility_ema_span_step = new_bounds["short_filter_volatility_ema_span"][2]
            else:
                self.short_filter_volatility_ema_span_step = 0.0
        # Fix for old configs
        elif "short_filter_log_range_ema_span" in new_bounds:
            self.short_filter_volatility_ema_span_0 = new_bounds["short_filter_log_range_ema_span"][0]
            self.short_filter_volatility_ema_span_1 = new_bounds["short_filter_log_range_ema_span"][1]
        elif "short_filter_noisiness_rolling_window" in new_bounds:
            self.short_filter_volatility_ema_span_0 = new_bounds["short_filter_noisiness_rolling_window"][0]
            self.short_filter_volatility_ema_span_1 = new_bounds["short_filter_noisiness_rolling_window"][1]
        elif "short_filter_rolling_window" in new_bounds:
            self.short_filter_volatility_ema_span_0 = new_bounds["short_filter_rolling_window"][0]
            self.short_filter_volatility_ema_span_1 = new_bounds["short_filter_rolling_window"][1]
        if "short_filter_volume_drop_pct" in new_bounds:
            self.short_filter_volume_drop_pct_0 = new_bounds["short_filter_volume_drop_pct"][0]
            self.short_filter_volume_drop_pct_1 = new_bounds["short_filter_volume_drop_pct"][1]
            if isinstance(new_bounds["short_filter_volume_drop_pct"], (list, tuple)) and len(new_bounds["short_filter_volume_drop_pct"]) >= 3:
                self.short_filter_volume_drop_pct_step = new_bounds["short_filter_volume_drop_pct"][2]
            else:
                self.short_filter_volume_drop_pct_step = 0.0
        # Fix for old configs
        elif "short_filter_relative_volume_clip_pct" in new_bounds:
            self.short_filter_volume_drop_pct_0 = new_bounds["short_filter_relative_volume_clip_pct"][0]
            self.short_filter_volume_drop_pct_1 = new_bounds["short_filter_relative_volume_clip_pct"][1]
        if "short_filter_volatility_drop_pct" in new_bounds:
            self.short_filter_volatility_drop_pct_0 = new_bounds["short_filter_volatility_drop_pct"][0]
            self.short_filter_volatility_drop_pct_1 = new_bounds["short_filter_volatility_drop_pct"][1]
            if isinstance(new_bounds["short_filter_volatility_drop_pct"], (list, tuple)) and len(new_bounds["short_filter_volatility_drop_pct"]) >= 3:
                self.short_filter_volatility_drop_pct_step = new_bounds["short_filter_volatility_drop_pct"][2]
            else:
                self.short_filter_volatility_drop_pct_step = 0.0
        if "short_filter_volume_ema_span" in new_bounds:
            self.short_filter_volume_ema_span_0 = new_bounds["short_filter_volume_ema_span"][0]
            self.short_filter_volume_ema_span_1 = new_bounds["short_filter_volume_ema_span"][1]
            if isinstance(new_bounds["short_filter_volume_ema_span"], (list, tuple)) and len(new_bounds["short_filter_volume_ema_span"]) >= 3:
                self.short_filter_volume_ema_span_step = new_bounds["short_filter_volume_ema_span"][2]
            else:
                self.short_filter_volume_ema_span_step = 0.0
        # Fix for old configs
        elif "short_filter_rolling_window" in new_bounds:
            self.short_filter_volume_ema_span_0 = new_bounds["short_filter_rolling_window"][0]
            self.short_filter_volume_ema_span_1 = new_bounds["short_filter_rolling_window"][1]
        if "short_n_positions" in new_bounds:
            self.short_n_positions_0 = new_bounds["short_n_positions"][0]
            self.short_n_positions_1 = new_bounds["short_n_positions"][1]
            if isinstance(new_bounds["short_n_positions"], (list, tuple)) and len(new_bounds["short_n_positions"]) >= 3:
                self.short_n_positions_step = new_bounds["short_n_positions"][2]
            else:
                self.short_n_positions_step = 0.0
        if "short_total_wallet_exposure_limit" in new_bounds:
            self.short_total_wallet_exposure_limit_0 = new_bounds["short_total_wallet_exposure_limit"][0]
            self.short_total_wallet_exposure_limit_1 = new_bounds["short_total_wallet_exposure_limit"][1]
            if isinstance(new_bounds["short_total_wallet_exposure_limit"], (list, tuple)) and len(new_bounds["short_total_wallet_exposure_limit"]) >= 3:
                self.short_total_wallet_exposure_limit_step = new_bounds["short_total_wallet_exposure_limit"][2]
            else:
                self.short_total_wallet_exposure_limit_step = 0.0
        if "short_unstuck_close_pct" in new_bounds:
            self.short_unstuck_close_pct_0 = new_bounds["short_unstuck_close_pct"][0]
            self.short_unstuck_close_pct_1 = new_bounds["short_unstuck_close_pct"][1]
            if isinstance(new_bounds["short_unstuck_close_pct"], (list, tuple)) and len(new_bounds["short_unstuck_close_pct"]) >= 3:
                self.short_unstuck_close_pct_step = new_bounds["short_unstuck_close_pct"][2]
            else:
                self.short_unstuck_close_pct_step = 0.0
        if "short_unstuck_ema_dist" in new_bounds:
            self.short_unstuck_ema_dist_0 = new_bounds["short_unstuck_ema_dist"][0]
            self.short_unstuck_ema_dist_1 = new_bounds["short_unstuck_ema_dist"][1]
            if isinstance(new_bounds["short_unstuck_ema_dist"], (list, tuple)) and len(new_bounds["short_unstuck_ema_dist"]) >= 3:
                self.short_unstuck_ema_dist_step = new_bounds["short_unstuck_ema_dist"][2]
            else:
                self.short_unstuck_ema_dist_step = 0.0
        if "short_unstuck_loss_allowance_pct" in new_bounds:
            self.short_unstuck_loss_allowance_pct_0 = new_bounds["short_unstuck_loss_allowance_pct"][0]
            self.short_unstuck_loss_allowance_pct_1 = new_bounds["short_unstuck_loss_allowance_pct"][1]
            if isinstance(new_bounds["short_unstuck_loss_allowance_pct"], (list, tuple)) and len(new_bounds["short_unstuck_loss_allowance_pct"]) >= 3:
                self.short_unstuck_loss_allowance_pct_step = new_bounds["short_unstuck_loss_allowance_pct"][2]
            else:
                self.short_unstuck_loss_allowance_pct_step = 0.0
        if "short_unstuck_threshold" in new_bounds:
            self.short_unstuck_threshold_0 = new_bounds["short_unstuck_threshold"][0]
            self.short_unstuck_threshold_1 = new_bounds["short_unstuck_threshold"][1]
            if isinstance(new_bounds["short_unstuck_threshold"], (list, tuple)) and len(new_bounds["short_unstuck_threshold"]) >= 3:
                self.short_unstuck_threshold_step = new_bounds["short_unstuck_threshold"][2]
            else:
                self.short_unstuck_threshold_step = 0.0
        if "long_risk_wel_enforcer_threshold" in new_bounds:
            self.long_risk_wel_enforcer_threshold_0 = new_bounds["long_risk_wel_enforcer_threshold"][0]
            self.long_risk_wel_enforcer_threshold_1 = new_bounds["long_risk_wel_enforcer_threshold"][1]
            if isinstance(new_bounds["long_risk_wel_enforcer_threshold"], (list, tuple)) and len(new_bounds["long_risk_wel_enforcer_threshold"]) >= 3:
                self.long_risk_wel_enforcer_threshold_step = new_bounds["long_risk_wel_enforcer_threshold"][2]
            else:
                self.long_risk_wel_enforcer_threshold_step = 0.0
        if "long_risk_we_excess_allowance_pct" in new_bounds:
            self.long_risk_we_excess_allowance_pct_0 = new_bounds["long_risk_we_excess_allowance_pct"][0]
            self.long_risk_we_excess_allowance_pct_1 = new_bounds["long_risk_we_excess_allowance_pct"][1]
            if isinstance(new_bounds["long_risk_we_excess_allowance_pct"], (list, tuple)) and len(new_bounds["long_risk_we_excess_allowance_pct"]) >= 3:
                self.long_risk_we_excess_allowance_pct_step = new_bounds["long_risk_we_excess_allowance_pct"][2]
            else:
                self.long_risk_we_excess_allowance_pct_step = 0.0
        if "long_risk_twel_enforcer_threshold" in new_bounds:
            self.long_risk_twel_enforcer_threshold_0 = new_bounds["long_risk_twel_enforcer_threshold"][0]
            self.long_risk_twel_enforcer_threshold_1 = new_bounds["long_risk_twel_enforcer_threshold"][1]
            if isinstance(new_bounds["long_risk_twel_enforcer_threshold"], (list, tuple)) and len(new_bounds["long_risk_twel_enforcer_threshold"]) >= 3:
                self.long_risk_twel_enforcer_threshold_step = new_bounds["long_risk_twel_enforcer_threshold"][2]
            else:
                self.long_risk_twel_enforcer_threshold_step = 0.0
        if "short_risk_wel_enforcer_threshold" in new_bounds:
            self.short_risk_wel_enforcer_threshold_0 = new_bounds["short_risk_wel_enforcer_threshold"][0]
            self.short_risk_wel_enforcer_threshold_1 = new_bounds["short_risk_wel_enforcer_threshold"][1]
            if isinstance(new_bounds["short_risk_wel_enforcer_threshold"], (list, tuple)) and len(new_bounds["short_risk_wel_enforcer_threshold"]) >= 3:
                self.short_risk_wel_enforcer_threshold_step = new_bounds["short_risk_wel_enforcer_threshold"][2]
            else:
                self.short_risk_wel_enforcer_threshold_step = 0.0
        if "short_risk_we_excess_allowance_pct" in new_bounds:
            self.short_risk_we_excess_allowance_pct_0 = new_bounds["short_risk_we_excess_allowance_pct"][0]
            self.short_risk_we_excess_allowance_pct_1 = new_bounds["short_risk_we_excess_allowance_pct"][1]
            if isinstance(new_bounds["short_risk_we_excess_allowance_pct"], (list, tuple)) and len(new_bounds["short_risk_we_excess_allowance_pct"]) >= 3:
                self.short_risk_we_excess_allowance_pct_step = new_bounds["short_risk_we_excess_allowance_pct"][2]
            else:
                self.short_risk_we_excess_allowance_pct_step = 0.0
        if "short_risk_twel_enforcer_threshold" in new_bounds:
            self.short_risk_twel_enforcer_threshold_0 = new_bounds["short_risk_twel_enforcer_threshold"][0]
            self.short_risk_twel_enforcer_threshold_1 = new_bounds["short_risk_twel_enforcer_threshold"][1]
            if isinstance(new_bounds["short_risk_twel_enforcer_threshold"], (list, tuple)) and len(new_bounds["short_risk_twel_enforcer_threshold"]) >= 3:
                self.short_risk_twel_enforcer_threshold_step = new_bounds["short_risk_twel_enforcer_threshold"][2]
            else:
                self.short_risk_twel_enforcer_threshold_step = 0.0
        
    # Long parameters
    @property
    def long_close_grid_markup_end_0(self): return self._long_close_grid_markup_end_0
    @property
    def long_close_grid_markup_end_1(self): return self._long_close_grid_markup_end_1
    @property
    def long_close_grid_markup_end_step(self): return self._long_close_grid_markup_end_step
    @property
    def long_close_grid_markup_start_0(self): return self._long_close_grid_markup_start_0
    @property
    def long_close_grid_markup_start_1(self): return self._long_close_grid_markup_start_1
    @property
    def long_close_grid_markup_start_step(self): return self._long_close_grid_markup_start_step
    @property
    def long_close_grid_qty_pct_0(self): return self._long_close_grid_qty_pct_0
    @property
    def long_close_grid_qty_pct_1(self): return self._long_close_grid_qty_pct_1
    @property
    def long_close_grid_qty_pct_step(self): return self._long_close_grid_qty_pct_step
    @property
    def long_close_trailing_grid_ratio_0(self): return self._long_close_trailing_grid_ratio_0
    @property
    def long_close_trailing_grid_ratio_1(self): return self._long_close_trailing_grid_ratio_1
    @property
    def long_close_trailing_grid_ratio_step(self): return self._long_close_trailing_grid_ratio_step
    @property
    def long_close_trailing_qty_pct_0(self): return self._long_close_trailing_qty_pct_0
    @property
    def long_close_trailing_qty_pct_1(self): return self._long_close_trailing_qty_pct_1
    @property
    def long_close_trailing_qty_pct_step(self): return self._long_close_trailing_qty_pct_step
    @property
    def long_close_trailing_retracement_pct_0(self): return self._long_close_trailing_retracement_pct_0
    @property
    def long_close_trailing_retracement_pct_1(self): return self._long_close_trailing_retracement_pct_1
    @property
    def long_close_trailing_retracement_pct_step(self): return self._long_close_trailing_retracement_pct_step
    @property
    def long_close_trailing_threshold_pct_0(self): return self._long_close_trailing_threshold_pct_0
    @property
    def long_close_trailing_threshold_pct_1(self): return self._long_close_trailing_threshold_pct_1
    @property
    def long_close_trailing_threshold_pct_step(self): return self._long_close_trailing_threshold_pct_step
    @property
    def long_ema_span_0_0(self): return self._long_ema_span_0_0
    @property
    def long_ema_span_0_1(self): return self._long_ema_span_0_1
    @property
    def long_ema_span_0_step(self): return self._long_ema_span_0_step
    @property
    def long_ema_span_1_0(self): return self._long_ema_span_1_0
    @property
    def long_ema_span_1_1(self): return self._long_ema_span_1_1
    @property
    def long_ema_span_1_step(self): return self._long_ema_span_1_step
    @property
    def long_entry_grid_double_down_factor_0(self): return self._long_entry_grid_double_down_factor_0
    @property
    def long_entry_grid_double_down_factor_1(self): return self._long_entry_grid_double_down_factor_1
    @property
    def long_entry_grid_double_down_factor_step(self): return self._long_entry_grid_double_down_factor_step
    @property
    def long_entry_volatility_ema_span_hours_0(self): return self._long_entry_volatility_ema_span_hours_0
    @property
    def long_entry_volatility_ema_span_hours_1(self): return self._long_entry_volatility_ema_span_hours_1
    @property
    def long_entry_volatility_ema_span_hours_step(self): return self._long_entry_volatility_ema_span_hours_step
    @property
    def long_entry_grid_spacing_volatility_weight_0(self): return self._long_entry_grid_spacing_volatility_weight_0
    @property
    def long_entry_grid_spacing_volatility_weight_1(self): return self._long_entry_grid_spacing_volatility_weight_1
    @property
    def long_entry_grid_spacing_volatility_weight_step(self): return self._long_entry_grid_spacing_volatility_weight_step
    @property
    def long_entry_grid_spacing_pct_0(self): return self._long_entry_grid_spacing_pct_0
    @property
    def long_entry_grid_spacing_pct_1(self): return self._long_entry_grid_spacing_pct_1
    @property
    def long_entry_grid_spacing_pct_step(self): return self._long_entry_grid_spacing_pct_step
    @property
    def long_entry_grid_spacing_we_weight_0(self): return self._long_entry_grid_spacing_we_weight_0
    @property
    def long_entry_grid_spacing_we_weight_1(self): return self._long_entry_grid_spacing_we_weight_1
    @property
    def long_entry_grid_spacing_we_weight_step(self): return self._long_entry_grid_spacing_we_weight_step
    @property
    def long_entry_initial_ema_dist_0(self): return self._long_entry_initial_ema_dist_0
    @property
    def long_entry_initial_ema_dist_1(self): return self._long_entry_initial_ema_dist_1
    @property
    def long_entry_initial_ema_dist_step(self): return self._long_entry_initial_ema_dist_step
    @property
    def long_entry_initial_qty_pct_0(self): return self._long_entry_initial_qty_pct_0
    @property
    def long_entry_initial_qty_pct_1(self): return self._long_entry_initial_qty_pct_1
    @property
    def long_entry_initial_qty_pct_step(self): return self._long_entry_initial_qty_pct_step
    @property
    def long_entry_trailing_double_down_factor_0(self): return self._long_entry_trailing_double_down_factor_0
    @property
    def long_entry_trailing_double_down_factor_1(self): return self._long_entry_trailing_double_down_factor_1
    @property
    def long_entry_trailing_double_down_factor_step(self): return self._long_entry_trailing_double_down_factor_step
    @property
    def long_entry_trailing_grid_ratio_0(self): return self._long_entry_trailing_grid_ratio_0
    @property
    def long_entry_trailing_grid_ratio_1(self): return self._long_entry_trailing_grid_ratio_1
    @property
    def long_entry_trailing_grid_ratio_step(self): return self._long_entry_trailing_grid_ratio_step
    @property
    def long_entry_trailing_retracement_pct_0(self): return self._long_entry_trailing_retracement_pct_0
    @property
    def long_entry_trailing_retracement_pct_1(self): return self._long_entry_trailing_retracement_pct_1
    @property
    def long_entry_trailing_retracement_pct_step(self): return self._long_entry_trailing_retracement_pct_step
    @property
    def long_entry_trailing_retracement_we_weight_0(self): return self._long_entry_trailing_retracement_we_weight_0
    @property
    def long_entry_trailing_retracement_we_weight_1(self): return self._long_entry_trailing_retracement_we_weight_1
    @property
    def long_entry_trailing_retracement_we_weight_step(self): return self._long_entry_trailing_retracement_we_weight_step
    @property
    def long_entry_trailing_retracement_volatility_weight_0(self): return self._long_entry_trailing_retracement_volatility_weight_0
    @property
    def long_entry_trailing_retracement_volatility_weight_1(self): return self._long_entry_trailing_retracement_volatility_weight_1
    @property
    def long_entry_trailing_retracement_volatility_weight_step(self): return self._long_entry_trailing_retracement_volatility_weight_step
    @property
    def long_entry_trailing_threshold_pct_0(self): return self._long_entry_trailing_threshold_pct_0
    @property
    def long_entry_trailing_threshold_pct_1(self): return self._long_entry_trailing_threshold_pct_1
    @property
    def long_entry_trailing_threshold_pct_step(self): return self._long_entry_trailing_threshold_pct_step
    @property
    def long_entry_trailing_threshold_we_weight_0(self): return self._long_entry_trailing_threshold_we_weight_0
    @property
    def long_entry_trailing_threshold_we_weight_1(self): return self._long_entry_trailing_threshold_we_weight_1
    @property
    def long_entry_trailing_threshold_we_weight_step(self): return self._long_entry_trailing_threshold_we_weight_step
    @property
    def long_entry_trailing_threshold_volatility_weight_0(self): return self._long_entry_trailing_threshold_volatility_weight_0
    @property
    def long_entry_trailing_threshold_volatility_weight_1(self): return self._long_entry_trailing_threshold_volatility_weight_1
    @property
    def long_entry_trailing_threshold_volatility_weight_step(self): return self._long_entry_trailing_threshold_volatility_weight_step
    @property
    def long_filter_volatility_ema_span_0(self): return self._long_filter_volatility_ema_span_0
    @property
    def long_filter_volatility_ema_span_1(self): return self._long_filter_volatility_ema_span_1
    @property
    def long_filter_volatility_ema_span_step(self): return self._long_filter_volatility_ema_span_step
    @property
    def long_filter_volume_drop_pct_0(self): return self._long_filter_volume_drop_pct_0
    @property
    def long_filter_volume_drop_pct_1(self): return self._long_filter_volume_drop_pct_1
    @property
    def long_filter_volume_drop_pct_step(self): return self._long_filter_volume_drop_pct_step
    @property
    def long_filter_volatility_drop_pct_0(self): return self._long_filter_volatility_drop_pct_0
    @property
    def long_filter_volatility_drop_pct_1(self): return self._long_filter_volatility_drop_pct_1
    @property
    def long_filter_volatility_drop_pct_step(self): return self._long_filter_volatility_drop_pct_step
    @property
    def long_filter_volume_ema_span_0(self): return self._long_filter_volume_ema_span_0
    @property
    def long_filter_volume_ema_span_1(self): return self._long_filter_volume_ema_span_1
    @property
    def long_filter_volume_ema_span_step(self): return self._long_filter_volume_ema_span_step
    @property
    def long_n_positions_0(self): return self._long_n_positions_0
    @property
    def long_n_positions_1(self): return self._long_n_positions_1
    @property
    def long_n_positions_step(self): return self._long_n_positions_step
    @property
    def long_total_wallet_exposure_limit_0(self): return self._long_total_wallet_exposure_limit_0
    @property
    def long_total_wallet_exposure_limit_1(self): return self._long_total_wallet_exposure_limit_1
    @property
    def long_total_wallet_exposure_limit_step(self): return self._long_total_wallet_exposure_limit_step
    @property
    def long_unstuck_close_pct_0(self): return self._long_unstuck_close_pct_0
    @property
    def long_unstuck_close_pct_1(self): return self._long_unstuck_close_pct_1
    @property
    def long_unstuck_close_pct_step(self): return self._long_unstuck_close_pct_step
    @property
    def long_unstuck_ema_dist_0(self): return self._long_unstuck_ema_dist_0
    @property
    def long_unstuck_ema_dist_1(self): return self._long_unstuck_ema_dist_1
    @property
    def long_unstuck_ema_dist_step(self): return self._long_unstuck_ema_dist_step
    @property
    def long_unstuck_loss_allowance_pct_0(self): return self._long_unstuck_loss_allowance_pct_0
    @property
    def long_unstuck_loss_allowance_pct_1(self): return self._long_unstuck_loss_allowance_pct_1
    @property
    def long_unstuck_loss_allowance_pct_step(self): return self._long_unstuck_loss_allowance_pct_step
    @property
    def long_unstuck_threshold_0(self): return self._long_unstuck_threshold_0
    @property
    def long_unstuck_threshold_1(self): return self._long_unstuck_threshold_1
    @property
    def long_unstuck_threshold_step(self): return self._long_unstuck_threshold_step
    @property
    def long_risk_wel_enforcer_threshold_0(self): return self._long_risk_wel_enforcer_threshold_0
    @property
    def long_risk_wel_enforcer_threshold_1(self): return self._long_risk_wel_enforcer_threshold_1
    @property
    def long_risk_wel_enforcer_threshold_step(self): return self._long_risk_wel_enforcer_threshold_step
    @property
    def long_risk_we_excess_allowance_pct_0(self): return self._long_risk_we_excess_allowance_pct_0
    @property
    def long_risk_we_excess_allowance_pct_1(self): return self._long_risk_we_excess_allowance_pct_1
    @property
    def long_risk_we_excess_allowance_pct_step(self): return self._long_risk_we_excess_allowance_pct_step
    @property
    def long_risk_twel_enforcer_threshold_0(self): return self._long_risk_twel_enforcer_threshold_0
    @property
    def long_risk_twel_enforcer_threshold_1(self): return self._long_risk_twel_enforcer_threshold_1
    @property
    def long_risk_twel_enforcer_threshold_step(self): return self._long_risk_twel_enforcer_threshold_step
    
    # Short parameters
    @property
    def short_close_grid_markup_end_0(self): return self._short_close_grid_markup_end_0
    @property
    def short_close_grid_markup_end_1(self): return self._short_close_grid_markup_end_1
    @property
    def short_close_grid_markup_end_step(self): return self._short_close_grid_markup_end_step
    @property
    def short_close_grid_markup_start_0(self): return self._short_close_grid_markup_start_0
    @property
    def short_close_grid_markup_start_1(self): return self._short_close_grid_markup_start_1
    @property
    def short_close_grid_markup_start_step(self): return self._short_close_grid_markup_start_step
    @property
    def short_close_grid_qty_pct_0(self): return self._short_close_grid_qty_pct_0
    @property
    def short_close_grid_qty_pct_1(self): return self._short_close_grid_qty_pct_1
    @property
    def short_close_grid_qty_pct_step(self): return self._short_close_grid_qty_pct_step
    @property
    def short_close_trailing_grid_ratio_0(self): return self._short_close_trailing_grid_ratio_0
    @property
    def short_close_trailing_grid_ratio_1(self): return self._short_close_trailing_grid_ratio_1
    @property
    def short_close_trailing_grid_ratio_step(self): return self._short_close_trailing_grid_ratio_step
    @property
    def short_close_trailing_qty_pct_0(self): return self._short_close_trailing_qty_pct_0
    @property
    def short_close_trailing_qty_pct_1(self): return self._short_close_trailing_qty_pct_1
    @property
    def short_close_trailing_qty_pct_step(self): return self._short_close_trailing_qty_pct_step
    @property
    def short_close_trailing_retracement_pct_0(self): return self._short_close_trailing_retracement_pct_0
    @property
    def short_close_trailing_retracement_pct_1(self): return self._short_close_trailing_retracement_pct_1
    @property
    def short_close_trailing_retracement_pct_step(self): return self._short_close_trailing_retracement_pct_step
    @property
    def short_close_trailing_threshold_pct_0(self): return self._short_close_trailing_threshold_pct_0
    @property
    def short_close_trailing_threshold_pct_1(self): return self._short_close_trailing_threshold_pct_1
    @property
    def short_close_trailing_threshold_pct_step(self): return self._short_close_trailing_threshold_pct_step
    @property
    def short_ema_span_0_0(self): return self._short_ema_span_0_0
    @property
    def short_ema_span_0_1(self): return self._short_ema_span_0_1
    @property
    def short_ema_span_0_step(self): return self._short_ema_span_0_step
    @property
    def short_ema_span_1_0(self): return self._short_ema_span_1_0
    @property
    def short_ema_span_1_1(self): return self._short_ema_span_1_1
    @property
    def short_ema_span_1_step(self): return self._short_ema_span_1_step
    @property
    def short_entry_grid_double_down_factor_0(self): return self._short_entry_grid_double_down_factor_0
    @property
    def short_entry_grid_double_down_factor_1(self): return self._short_entry_grid_double_down_factor_1
    @property
    def short_entry_grid_double_down_factor_step(self): return self._short_entry_grid_double_down_factor_step
    @property
    def short_entry_volatility_ema_span_hours_0(self): return self._short_entry_volatility_ema_span_hours_0
    @property
    def short_entry_volatility_ema_span_hours_1(self): return self._short_entry_volatility_ema_span_hours_1
    @property
    def short_entry_volatility_ema_span_hours_step(self): return self._short_entry_volatility_ema_span_hours_step
    @property
    def short_entry_grid_spacing_volatility_weight_0(self): return self._short_entry_grid_spacing_volatility_weight_0
    @property
    def short_entry_grid_spacing_volatility_weight_1(self): return self._short_entry_grid_spacing_volatility_weight_1
    @property
    def short_entry_grid_spacing_volatility_weight_step(self): return self._short_entry_grid_spacing_volatility_weight_step
    @property
    def short_entry_grid_spacing_pct_0(self): return self._short_entry_grid_spacing_pct_0
    @property
    def short_entry_grid_spacing_pct_1(self): return self._short_entry_grid_spacing_pct_1
    @property
    def short_entry_grid_spacing_pct_step(self): return self._short_entry_grid_spacing_pct_step
    @property
    def short_entry_grid_spacing_we_weight_0(self): return self._short_entry_grid_spacing_we_weight_0
    @property
    def short_entry_grid_spacing_we_weight_1(self): return self._short_entry_grid_spacing_we_weight_1
    @property
    def short_entry_grid_spacing_we_weight_step(self): return self._short_entry_grid_spacing_we_weight_step
    @property
    def short_entry_initial_ema_dist_0(self): return self._short_entry_initial_ema_dist_0
    @property
    def short_entry_initial_ema_dist_1(self): return self._short_entry_initial_ema_dist_1
    @property
    def short_entry_initial_ema_dist_step(self): return self._short_entry_initial_ema_dist_step
    @property
    def short_entry_initial_qty_pct_0(self): return self._short_entry_initial_qty_pct_0
    @property
    def short_entry_initial_qty_pct_1(self): return self._short_entry_initial_qty_pct_1
    @property
    def short_entry_initial_qty_pct_step(self): return self._short_entry_initial_qty_pct_step
    @property
    def short_entry_trailing_double_down_factor_0(self): return self._short_entry_trailing_double_down_factor_0
    @property
    def short_entry_trailing_double_down_factor_1(self): return self._short_entry_trailing_double_down_factor_1
    @property
    def short_entry_trailing_double_down_factor_step(self): return self._short_entry_trailing_double_down_factor_step
    @property
    def short_entry_trailing_grid_ratio_0(self): return self._short_entry_trailing_grid_ratio_0
    @property
    def short_entry_trailing_grid_ratio_1(self): return self._short_entry_trailing_grid_ratio_1
    @property
    def short_entry_trailing_grid_ratio_step(self): return self._short_entry_trailing_grid_ratio_step
    @property
    def short_entry_trailing_retracement_pct_0(self): return self._short_entry_trailing_retracement_pct_0
    @property
    def short_entry_trailing_retracement_pct_1(self): return self._short_entry_trailing_retracement_pct_1
    @property
    def short_entry_trailing_retracement_pct_step(self): return self._short_entry_trailing_retracement_pct_step
    @property
    def short_entry_trailing_retracement_we_weight_0(self): return self._short_entry_trailing_retracement_we_weight_0
    @property
    def short_entry_trailing_retracement_we_weight_1(self): return self._short_entry_trailing_retracement_we_weight_1
    @property
    def short_entry_trailing_retracement_we_weight_step(self): return self._short_entry_trailing_retracement_we_weight_step
    @property
    def short_entry_trailing_retracement_volatility_weight_0(self): return self._short_entry_trailing_retracement_volatility_weight_0
    @property
    def short_entry_trailing_retracement_volatility_weight_1(self): return self._short_entry_trailing_retracement_volatility_weight_1
    @property
    def short_entry_trailing_retracement_volatility_weight_step(self): return self._short_entry_trailing_retracement_volatility_weight_step
    @property
    def short_entry_trailing_threshold_pct_0(self): return self._short_entry_trailing_threshold_pct_0
    @property
    def short_entry_trailing_threshold_pct_1(self): return self._short_entry_trailing_threshold_pct_1
    @property
    def short_entry_trailing_threshold_pct_step(self): return self._short_entry_trailing_threshold_pct_step
    @property
    def short_entry_trailing_threshold_we_weight_0(self): return self._short_entry_trailing_threshold_we_weight_0
    @property
    def short_entry_trailing_threshold_we_weight_1(self): return self._short_entry_trailing_threshold_we_weight_1
    @property
    def short_entry_trailing_threshold_we_weight_step(self): return self._short_entry_trailing_threshold_we_weight_step
    @property
    def short_entry_trailing_threshold_volatility_weight_0(self): return self._short_entry_trailing_threshold_volatility_weight_0
    @property
    def short_entry_trailing_threshold_volatility_weight_1(self): return self._short_entry_trailing_threshold_volatility_weight_1
    @property
    def short_entry_trailing_threshold_volatility_weight_step(self): return self._short_entry_trailing_threshold_volatility_weight_step
    @property
    def short_filter_volatility_ema_span_0(self): return self._short_filter_volatility_ema_span_0
    @property
    def short_filter_volatility_ema_span_1(self): return self._short_filter_volatility_ema_span_1
    @property
    def short_filter_volatility_ema_span_step(self): return self._short_filter_volatility_ema_span_step
    @property
    def short_filter_volume_drop_pct_0(self): return self._short_filter_volume_drop_pct_0
    @property
    def short_filter_volume_drop_pct_1(self): return self._short_filter_volume_drop_pct_1
    @property
    def short_filter_volume_drop_pct_step(self): return self._short_filter_volume_drop_pct_step
    @property
    def short_filter_volatility_drop_pct_0(self): return self._short_filter_volatility_drop_pct_0
    @property
    def short_filter_volatility_drop_pct_1(self): return self._short_filter_volatility_drop_pct_1
    @property
    def short_filter_volatility_drop_pct_step(self): return self._short_filter_volatility_drop_pct_step
    @property
    def short_filter_volume_ema_span_0(self): return self._short_filter_volume_ema_span_0
    @property
    def short_filter_volume_ema_span_1(self): return self._short_filter_volume_ema_span_1
    @property
    def short_filter_volume_ema_span_step(self): return self._short_filter_volume_ema_span_step
    @property
    def short_n_positions_0(self): return self._short_n_positions_0
    @property
    def short_n_positions_1(self): return self._short_n_positions_1
    @property
    def short_n_positions_step(self): return self._short_n_positions_step
    @property
    def short_total_wallet_exposure_limit_0(self): return self._short_total_wallet_exposure_limit_0
    @property
    def short_total_wallet_exposure_limit_1(self): return self._short_total_wallet_exposure_limit_1
    @property
    def short_total_wallet_exposure_limit_step(self): return self._short_total_wallet_exposure_limit_step
    @property
    def short_unstuck_close_pct_0(self): return self._short_unstuck_close_pct_0
    @property
    def short_unstuck_close_pct_1(self): return self._short_unstuck_close_pct_1
    @property
    def short_unstuck_close_pct_step(self): return self._short_unstuck_close_pct_step
    @property
    def short_unstuck_ema_dist_0(self): return self._short_unstuck_ema_dist_0
    @property
    def short_unstuck_ema_dist_1(self): return self._short_unstuck_ema_dist_1
    @property
    def short_unstuck_ema_dist_step(self): return self._short_unstuck_ema_dist_step
    @property
    def short_unstuck_loss_allowance_pct_0(self): return self._short_unstuck_loss_allowance_pct_0
    @property
    def short_unstuck_loss_allowance_pct_1(self): return self._short_unstuck_loss_allowance_pct_1
    @property
    def short_unstuck_loss_allowance_pct_step(self): return self._short_unstuck_loss_allowance_pct_step
    @property
    def short_unstuck_threshold_0(self): return self._short_unstuck_threshold_0
    @property
    def short_unstuck_threshold_1(self): return self._short_unstuck_threshold_1
    @property
    def short_unstuck_threshold_step(self): return self._short_unstuck_threshold_step
    @property
    def short_risk_wel_enforcer_threshold_0(self): return self._short_risk_wel_enforcer_threshold_0
    @property
    def short_risk_wel_enforcer_threshold_1(self): return self._short_risk_wel_enforcer_threshold_1
    @property
    def short_risk_wel_enforcer_threshold_step(self): return self._short_risk_wel_enforcer_threshold_step
    @property
    def short_risk_we_excess_allowance_pct_0(self): return self._short_risk_we_excess_allowance_pct_0
    @property
    def short_risk_we_excess_allowance_pct_1(self): return self._short_risk_we_excess_allowance_pct_1
    @property
    def short_risk_we_excess_allowance_pct_step(self): return self._short_risk_we_excess_allowance_pct_step
    @property
    def short_risk_twel_enforcer_threshold_0(self): return self._short_risk_twel_enforcer_threshold_0
    @property
    def short_risk_twel_enforcer_threshold_1(self): return self._short_risk_twel_enforcer_threshold_1
    @property
    def short_risk_twel_enforcer_threshold_step(self): return self._short_risk_twel_enforcer_threshold_step

    # Long setters
    @long_close_grid_markup_end_0.setter
    def long_close_grid_markup_end_0(self, new_value):
        self._long_close_grid_markup_end_0 = new_value
        self._bounds["long_close_grid_markup_end"][0] = new_value
    @long_close_grid_markup_end_1.setter
    def long_close_grid_markup_end_1(self, new_value):
        self._long_close_grid_markup_end_1 = new_value
        self._bounds["long_close_grid_markup_end"][1] = new_value
    @long_close_grid_markup_end_step.setter
    def long_close_grid_markup_end_step(self, new_value):
        self._long_close_grid_markup_end_step = new_value
        if len(self._bounds["long_close_grid_markup_end"]) < 3:
            self._bounds["long_close_grid_markup_end"].append(new_value)
        else:
            self._bounds["long_close_grid_markup_end"][2] = new_value
    @long_close_grid_markup_start_0.setter
    def long_close_grid_markup_start_0(self, new_value):
        self._long_close_grid_markup_start_0 = new_value
        self._bounds["long_close_grid_markup_start"][0] = new_value
    @long_close_grid_markup_start_1.setter
    def long_close_grid_markup_start_1(self, new_value):
        self._long_close_grid_markup_start_1 = new_value
        self._bounds["long_close_grid_markup_start"][1] = new_value
    @long_close_grid_markup_start_step.setter
    def long_close_grid_markup_start_step(self, new_value):
        self._long_close_grid_markup_start_step = new_value
        if len(self._bounds["long_close_grid_markup_start"]) < 3:
            self._bounds["long_close_grid_markup_start"].append(new_value)
        else:
            self._bounds["long_close_grid_markup_start"][2] = new_value
    @long_close_grid_qty_pct_0.setter
    def long_close_grid_qty_pct_0(self, new_value):
        self._long_close_grid_qty_pct_0 = new_value
        self._bounds["long_close_grid_qty_pct"][0] = new_value
    @long_close_grid_qty_pct_1.setter
    def long_close_grid_qty_pct_1(self, new_value):
        self._long_close_grid_qty_pct_1 = new_value
        self._bounds["long_close_grid_qty_pct"][1] = new_value
    @long_close_grid_qty_pct_step.setter
    def long_close_grid_qty_pct_step(self, new_value):
        self._long_close_grid_qty_pct_step = new_value
        if len(self._bounds["long_close_grid_qty_pct"]) < 3:
            self._bounds["long_close_grid_qty_pct"].append(new_value)
        else:
            self._bounds["long_close_grid_qty_pct"][2] = new_value
    @long_close_trailing_grid_ratio_0.setter
    def long_close_trailing_grid_ratio_0(self, new_value):
        self._long_close_trailing_grid_ratio_0 = new_value
        self._bounds["long_close_trailing_grid_ratio"][0] = new_value
    @long_close_trailing_grid_ratio_1.setter
    def long_close_trailing_grid_ratio_1(self, new_value):
        self._long_close_trailing_grid_ratio_1 = new_value
        self._bounds["long_close_trailing_grid_ratio"][1] = new_value
    @long_close_trailing_grid_ratio_step.setter
    def long_close_trailing_grid_ratio_step(self, new_value):
        self._long_close_trailing_grid_ratio_step = new_value
        if len(self._bounds["long_close_trailing_grid_ratio"]) < 3:
            self._bounds["long_close_trailing_grid_ratio"].append(new_value)
        else:
            self._bounds["long_close_trailing_grid_ratio"][2] = new_value
    @long_close_trailing_qty_pct_0.setter
    def long_close_trailing_qty_pct_0(self, new_value):
        self._long_close_trailing_qty_pct_0 = new_value
        self._bounds["long_close_trailing_qty_pct"][0] = new_value
    @long_close_trailing_qty_pct_1.setter
    def long_close_trailing_qty_pct_1(self, new_value):
        self._long_close_trailing_qty_pct_1 = new_value
        self._bounds["long_close_trailing_qty_pct"][1] = new_value
    @long_close_trailing_qty_pct_step.setter
    def long_close_trailing_qty_pct_step(self, new_value):
        self._long_close_trailing_qty_pct_step = new_value
        if len(self._bounds["long_close_trailing_qty_pct"]) < 3:
            self._bounds["long_close_trailing_qty_pct"].append(new_value)
        else:
            self._bounds["long_close_trailing_qty_pct"][2] = new_value
    @long_close_trailing_retracement_pct_0.setter
    def long_close_trailing_retracement_pct_0(self, new_value):
        self._long_close_trailing_retracement_pct_0 = new_value
        self._bounds["long_close_trailing_retracement_pct"][0] = new_value
    @long_close_trailing_retracement_pct_1.setter
    def long_close_trailing_retracement_pct_1(self, new_value):
        self._long_close_trailing_retracement_pct_1 = new_value
        self._bounds["long_close_trailing_retracement_pct"][1] = new_value
    @long_close_trailing_retracement_pct_step.setter
    def long_close_trailing_retracement_pct_step(self, new_value):
        self._long_close_trailing_retracement_pct_step = new_value
        if len(self._bounds["long_close_trailing_retracement_pct"]) < 3:
            self._bounds["long_close_trailing_retracement_pct"].append(new_value)
        else:
            self._bounds["long_close_trailing_retracement_pct"][2] = new_value
    @long_close_trailing_threshold_pct_0.setter
    def long_close_trailing_threshold_pct_0(self, new_value):
        self._long_close_trailing_threshold_pct_0 = new_value
        self._bounds["long_close_trailing_threshold_pct"][0] = new_value
    @long_close_trailing_threshold_pct_1.setter
    def long_close_trailing_threshold_pct_1(self, new_value):
        self._long_close_trailing_threshold_pct_1 = new_value
        self._bounds["long_close_trailing_threshold_pct"][1] = new_value
    @long_close_trailing_threshold_pct_step.setter
    def long_close_trailing_threshold_pct_step(self, new_value):
        self._long_close_trailing_threshold_pct_step = new_value
        if len(self._bounds["long_close_trailing_threshold_pct"]) < 3:
            self._bounds["long_close_trailing_threshold_pct"].append(new_value)
        else:
            self._bounds["long_close_trailing_threshold_pct"][2] = new_value
    @long_ema_span_0_0.setter
    def long_ema_span_0_0(self, new_value):
        self._long_ema_span_0_0 = new_value
        self._bounds["long_ema_span_0"][0] = new_value
    @long_ema_span_0_1.setter
    def long_ema_span_0_1(self, new_value):
        self._long_ema_span_0_1 = new_value
        self._bounds["long_ema_span_0"][1] = new_value
    @long_ema_span_0_step.setter
    def long_ema_span_0_step(self, new_value):
        self._long_ema_span_0_step = new_value
        if len(self._bounds["long_ema_span_0"]) < 3:
            self._bounds["long_ema_span_0"].append(new_value)
        else:
            self._bounds["long_ema_span_0"][2] = new_value
    @long_ema_span_1_0.setter
    def long_ema_span_1_0(self, new_value):
        self._long_ema_span_1_0 = new_value
        self._bounds["long_ema_span_1"][0] = new_value
    @long_ema_span_1_1.setter
    def long_ema_span_1_1(self, new_value):
        self._long_ema_span_1_1 = new_value
        self._bounds["long_ema_span_1"][1] = new_value
    @long_ema_span_1_step.setter
    def long_ema_span_1_step(self, new_value):
        self._long_ema_span_1_step = new_value
        if len(self._bounds["long_ema_span_1"]) < 3:
            self._bounds["long_ema_span_1"].append(new_value)
        else:
            self._bounds["long_ema_span_1"][2] = new_value
    @long_entry_grid_double_down_factor_0.setter
    def long_entry_grid_double_down_factor_0(self, new_value):
        self._long_entry_grid_double_down_factor_0 = new_value
        self._bounds["long_entry_grid_double_down_factor"][0] = new_value
    @long_entry_grid_double_down_factor_1.setter
    def long_entry_grid_double_down_factor_1(self, new_value):
        self._long_entry_grid_double_down_factor_1 = new_value
        self._bounds["long_entry_grid_double_down_factor"][1] = new_value
    @long_entry_grid_double_down_factor_step.setter
    def long_entry_grid_double_down_factor_step(self, new_value):
        self._long_entry_grid_double_down_factor_step = new_value
        if len(self._bounds["long_entry_grid_double_down_factor"]) < 3:
            self._bounds["long_entry_grid_double_down_factor"].append(new_value)
        else:
            self._bounds["long_entry_grid_double_down_factor"][2] = new_value
    @long_entry_volatility_ema_span_hours_0.setter
    def long_entry_volatility_ema_span_hours_0(self, new_value):
        self._long_entry_volatility_ema_span_hours_0 = new_value
        self._bounds["long_entry_volatility_ema_span_hours"][0] = new_value
    @long_entry_volatility_ema_span_hours_1.setter
    def long_entry_volatility_ema_span_hours_1(self, new_value):
        self._long_entry_volatility_ema_span_hours_1 = new_value
        self._bounds["long_entry_volatility_ema_span_hours"][1] = new_value
    @long_entry_volatility_ema_span_hours_step.setter
    def long_entry_volatility_ema_span_hours_step(self, new_value):
        self._long_entry_volatility_ema_span_hours_step = new_value
        if len(self._bounds["long_entry_volatility_ema_span_hours"]) < 3:
            self._bounds["long_entry_volatility_ema_span_hours"].append(new_value)
        else:
            self._bounds["long_entry_volatility_ema_span_hours"][2] = new_value
    @long_entry_grid_spacing_volatility_weight_0.setter
    def long_entry_grid_spacing_volatility_weight_0(self, new_value):
        self._long_entry_grid_spacing_volatility_weight_0 = new_value
        self._bounds["long_entry_grid_spacing_volatility_weight"][0] = new_value
    @long_entry_grid_spacing_volatility_weight_1.setter
    def long_entry_grid_spacing_volatility_weight_1(self, new_value):
        self._long_entry_grid_spacing_volatility_weight_1 = new_value
        self._bounds["long_entry_grid_spacing_volatility_weight"][1] = new_value
    @long_entry_grid_spacing_volatility_weight_step.setter
    def long_entry_grid_spacing_volatility_weight_step(self, new_value):
        self._long_entry_grid_spacing_volatility_weight_step = new_value
        if len(self._bounds["long_entry_grid_spacing_volatility_weight"]) < 3:
            self._bounds["long_entry_grid_spacing_volatility_weight"].append(new_value)
        else:
            self._bounds["long_entry_grid_spacing_volatility_weight"][2] = new_value
    @long_entry_grid_spacing_pct_0.setter
    def long_entry_grid_spacing_pct_0(self, new_value):
        self._long_entry_grid_spacing_pct_0 = new_value
        self._bounds["long_entry_grid_spacing_pct"][0] = new_value
    @long_entry_grid_spacing_pct_1.setter
    def long_entry_grid_spacing_pct_1(self, new_value):
        self._long_entry_grid_spacing_pct_1 = new_value
        self._bounds["long_entry_grid_spacing_pct"][1] = new_value
    @long_entry_grid_spacing_pct_step.setter
    def long_entry_grid_spacing_pct_step(self, new_value):
        self._long_entry_grid_spacing_pct_step = new_value
        if len(self._bounds["long_entry_grid_spacing_pct"]) < 3:
            self._bounds["long_entry_grid_spacing_pct"].append(new_value)
        else:
            self._bounds["long_entry_grid_spacing_pct"][2] = new_value
    @long_entry_grid_spacing_we_weight_0.setter
    def long_entry_grid_spacing_we_weight_0(self, new_value):
        self._long_entry_grid_spacing_we_weight_0 = new_value
        self._bounds["long_entry_grid_spacing_we_weight"][0] = new_value
    @long_entry_grid_spacing_we_weight_1.setter
    def long_entry_grid_spacing_we_weight_1(self, new_value):
        self._long_entry_grid_spacing_we_weight_1 = new_value
        self._bounds["long_entry_grid_spacing_we_weight"][1] = new_value
    @long_entry_grid_spacing_we_weight_step.setter
    def long_entry_grid_spacing_we_weight_step(self, new_value):
        self._long_entry_grid_spacing_we_weight_step = new_value
        if len(self._bounds["long_entry_grid_spacing_we_weight"]) < 3:
            self._bounds["long_entry_grid_spacing_we_weight"].append(new_value)
        else:
            self._bounds["long_entry_grid_spacing_we_weight"][2] = new_value
    @long_entry_initial_ema_dist_0.setter
    def long_entry_initial_ema_dist_0(self, new_value):
        self._long_entry_initial_ema_dist_0 = new_value
        self._bounds["long_entry_initial_ema_dist"][0] = new_value
    @long_entry_initial_ema_dist_1.setter
    def long_entry_initial_ema_dist_1(self, new_value):
        self._long_entry_initial_ema_dist_1 = new_value
        self._bounds["long_entry_initial_ema_dist"][1] = new_value
    @long_entry_initial_ema_dist_step.setter
    def long_entry_initial_ema_dist_step(self, new_value):
        self._long_entry_initial_ema_dist_step = new_value
        if len(self._bounds["long_entry_initial_ema_dist"]) < 3:
            self._bounds["long_entry_initial_ema_dist"].append(new_value)
        else:
            self._bounds["long_entry_initial_ema_dist"][2] = new_value
    @long_entry_initial_qty_pct_0.setter
    def long_entry_initial_qty_pct_0(self, new_value):
        self._long_entry_initial_qty_pct_0 = new_value
        self._bounds["long_entry_initial_qty_pct"][0] = new_value
    @long_entry_initial_qty_pct_1.setter
    def long_entry_initial_qty_pct_1(self, new_value):
        self._long_entry_initial_qty_pct_1 = new_value
        self._bounds["long_entry_initial_qty_pct"][1] = new_value
    @long_entry_initial_qty_pct_step.setter
    def long_entry_initial_qty_pct_step(self, new_value):
        self._long_entry_initial_qty_pct_step = new_value
        if len(self._bounds["long_entry_initial_qty_pct"]) < 3:
            self._bounds["long_entry_initial_qty_pct"].append(new_value)
        else:
            self._bounds["long_entry_initial_qty_pct"][2] = new_value
    @long_entry_trailing_double_down_factor_0.setter
    def long_entry_trailing_double_down_factor_0(self, new_value):
        self._long_entry_trailing_double_down_factor_0 = new_value
        self._bounds["long_entry_trailing_double_down_factor"][0] = new_value
    @long_entry_trailing_double_down_factor_1.setter
    def long_entry_trailing_double_down_factor_1(self, new_value):
        self._long_entry_trailing_double_down_factor_1 = new_value
        self._bounds["long_entry_trailing_double_down_factor"][1] = new_value
    @long_entry_trailing_double_down_factor_step.setter
    def long_entry_trailing_double_down_factor_step(self, new_value):
        self._long_entry_trailing_double_down_factor_step = new_value
        if len(self._bounds["long_entry_trailing_double_down_factor"]) < 3:
            self._bounds["long_entry_trailing_double_down_factor"].append(new_value)
        else:
            self._bounds["long_entry_trailing_double_down_factor"][2] = new_value
    @long_entry_trailing_grid_ratio_0.setter
    def long_entry_trailing_grid_ratio_0(self, new_value):
        self._long_entry_trailing_grid_ratio_0 = new_value
        self._bounds["long_entry_trailing_grid_ratio"][0] = new_value
    @long_entry_trailing_grid_ratio_1.setter
    def long_entry_trailing_grid_ratio_1(self, new_value):
        self._long_entry_trailing_grid_ratio_1 = new_value
        self._bounds["long_entry_trailing_grid_ratio"][1] = new_value
    @long_entry_trailing_grid_ratio_step.setter
    def long_entry_trailing_grid_ratio_step(self, new_value):
        self._long_entry_trailing_grid_ratio_step = new_value
        if len(self._bounds["long_entry_trailing_grid_ratio"]) < 3:
            self._bounds["long_entry_trailing_grid_ratio"].append(new_value)
        else:
            self._bounds["long_entry_trailing_grid_ratio"][2] = new_value
    @long_entry_trailing_retracement_pct_0.setter
    def long_entry_trailing_retracement_pct_0(self, new_value):
        self._long_entry_trailing_retracement_pct_0 = new_value
        self._bounds["long_entry_trailing_retracement_pct"][0] = new_value
    @long_entry_trailing_retracement_pct_1.setter
    def long_entry_trailing_retracement_pct_1(self, new_value):
        self._long_entry_trailing_retracement_pct_1 = new_value
        self._bounds["long_entry_trailing_retracement_pct"][1] = new_value
    @long_entry_trailing_retracement_pct_step.setter
    def long_entry_trailing_retracement_pct_step(self, new_value):
        self._long_entry_trailing_retracement_pct_step = new_value
        if len(self._bounds["long_entry_trailing_retracement_pct"]) < 3:
            self._bounds["long_entry_trailing_retracement_pct"].append(new_value)
        else:
            self._bounds["long_entry_trailing_retracement_pct"][2] = new_value
    @long_entry_trailing_retracement_we_weight_0.setter
    def long_entry_trailing_retracement_we_weight_0(self, new_value):
        self._long_entry_trailing_retracement_we_weight_0 = new_value
        self._bounds["long_entry_trailing_retracement_we_weight"][0] = new_value
    @long_entry_trailing_retracement_we_weight_1.setter
    def long_entry_trailing_retracement_we_weight_1(self, new_value):
        self._long_entry_trailing_retracement_we_weight_1 = new_value
        self._bounds["long_entry_trailing_retracement_we_weight"][1] = new_value
    @long_entry_trailing_retracement_we_weight_step.setter
    def long_entry_trailing_retracement_we_weight_step(self, new_value):
        self._long_entry_trailing_retracement_we_weight_step = new_value
        if len(self._bounds["long_entry_trailing_retracement_we_weight"]) < 3:
            self._bounds["long_entry_trailing_retracement_we_weight"].append(new_value)
        else:
            self._bounds["long_entry_trailing_retracement_we_weight"][2] = new_value
    @long_entry_trailing_retracement_volatility_weight_0.setter
    def long_entry_trailing_retracement_volatility_weight_0(self, new_value):
        self._long_entry_trailing_retracement_volatility_weight_0 = new_value
        self._bounds["long_entry_trailing_retracement_volatility_weight"][0] = new_value
    @long_entry_trailing_retracement_volatility_weight_1.setter
    def long_entry_trailing_retracement_volatility_weight_1(self, new_value):
        self._long_entry_trailing_retracement_volatility_weight_1 = new_value
        self._bounds["long_entry_trailing_retracement_volatility_weight"][1] = new_value
    @long_entry_trailing_retracement_volatility_weight_step.setter
    def long_entry_trailing_retracement_volatility_weight_step(self, new_value):
        self._long_entry_trailing_retracement_volatility_weight_step = new_value
        if len(self._bounds["long_entry_trailing_retracement_volatility_weight"]) < 3:
            self._bounds["long_entry_trailing_retracement_volatility_weight"].append(new_value)
        else:
            self._bounds["long_entry_trailing_retracement_volatility_weight"][2] = new_value
    @long_entry_trailing_threshold_pct_0.setter
    def long_entry_trailing_threshold_pct_0(self, new_value):
        self._long_entry_trailing_threshold_pct_0 = new_value
        self._bounds["long_entry_trailing_threshold_pct"][0] = new_value
    @long_entry_trailing_threshold_pct_1.setter
    def long_entry_trailing_threshold_pct_1(self, new_value):
        self._long_entry_trailing_threshold_pct_1 = new_value
        self._bounds["long_entry_trailing_threshold_pct"][1] = new_value
    @long_entry_trailing_threshold_pct_step.setter
    def long_entry_trailing_threshold_pct_step(self, new_value):
        self._long_entry_trailing_threshold_pct_step = new_value
        if len(self._bounds["long_entry_trailing_threshold_pct"]) < 3:
            self._bounds["long_entry_trailing_threshold_pct"].append(new_value)
        else:
            self._bounds["long_entry_trailing_threshold_pct"][2] = new_value
    @long_entry_trailing_threshold_we_weight_0.setter
    def long_entry_trailing_threshold_we_weight_0(self, new_value):
        self._long_entry_trailing_threshold_we_weight_0 = new_value
        self._bounds["long_entry_trailing_threshold_we_weight"][0] = new_value
    @long_entry_trailing_threshold_we_weight_1.setter
    def long_entry_trailing_threshold_we_weight_1(self, new_value):
        self._long_entry_trailing_threshold_we_weight_1 = new_value
        self._bounds["long_entry_trailing_threshold_we_weight"][1] = new_value
    @long_entry_trailing_threshold_we_weight_step.setter
    def long_entry_trailing_threshold_we_weight_step(self, new_value):
        self._long_entry_trailing_threshold_we_weight_step = new_value
        if len(self._bounds["long_entry_trailing_threshold_we_weight"]) < 3:
            self._bounds["long_entry_trailing_threshold_we_weight"].append(new_value)
        else:
            self._bounds["long_entry_trailing_threshold_we_weight"][2] = new_value
    @long_entry_trailing_threshold_volatility_weight_0.setter
    def long_entry_trailing_threshold_volatility_weight_0(self, new_value):
        self._long_entry_trailing_threshold_volatility_weight_0 = new_value
        self._bounds["long_entry_trailing_threshold_volatility_weight"][0] = new_value
    @long_entry_trailing_threshold_volatility_weight_1.setter
    def long_entry_trailing_threshold_volatility_weight_1(self, new_value):
        self._long_entry_trailing_threshold_volatility_weight_1 = new_value
        self._bounds["long_entry_trailing_threshold_volatility_weight"][1] = new_value
    @long_entry_trailing_threshold_volatility_weight_step.setter
    def long_entry_trailing_threshold_volatility_weight_step(self, new_value):
        self._long_entry_trailing_threshold_volatility_weight_step = new_value
        if len(self._bounds["long_entry_trailing_threshold_volatility_weight"]) < 3:
            self._bounds["long_entry_trailing_threshold_volatility_weight"].append(new_value)
        else:
            self._bounds["long_entry_trailing_threshold_volatility_weight"][2] = new_value
    @long_filter_volatility_ema_span_0.setter
    def long_filter_volatility_ema_span_0(self, new_value):
        self._long_filter_volatility_ema_span_0 = new_value
        self._bounds["long_filter_volatility_ema_span"][0] = new_value
    @long_filter_volatility_ema_span_1.setter
    def long_filter_volatility_ema_span_1(self, new_value):
        self._long_filter_volatility_ema_span_1 = new_value
        self._bounds["long_filter_volatility_ema_span"][1] = new_value
    @long_filter_volatility_ema_span_step.setter
    def long_filter_volatility_ema_span_step(self, new_value):
        self._long_filter_volatility_ema_span_step = new_value
        if len(self._bounds["long_filter_volatility_ema_span"]) < 3:
            self._bounds["long_filter_volatility_ema_span"].append(new_value)
        else:
            self._bounds["long_filter_volatility_ema_span"][2] = new_value
    @long_filter_volume_drop_pct_0.setter
    def long_filter_volume_drop_pct_0(self, new_value):
        self._long_filter_volume_drop_pct_0 = new_value
        self._bounds["long_filter_volume_drop_pct"][0] = new_value
    @long_filter_volume_drop_pct_1.setter
    def long_filter_volume_drop_pct_1(self, new_value):
        self._long_filter_volume_drop_pct_1 = new_value
        self._bounds["long_filter_volume_drop_pct"][1] = new_value
    @long_filter_volume_drop_pct_step.setter
    def long_filter_volume_drop_pct_step(self, new_value):
        self._long_filter_volume_drop_pct_step = new_value
        if len(self._bounds["long_filter_volume_drop_pct"]) < 3:
            self._bounds["long_filter_volume_drop_pct"].append(new_value)
        else:
            self._bounds["long_filter_volume_drop_pct"][2] = new_value
    @long_filter_volatility_drop_pct_0.setter
    def long_filter_volatility_drop_pct_0(self, new_value):
        self._long_filter_volatility_drop_pct_0 = new_value
        self._bounds["long_filter_volatility_drop_pct"][0] = new_value
    @long_filter_volatility_drop_pct_1.setter
    def long_filter_volatility_drop_pct_1(self, new_value):
        self._long_filter_volatility_drop_pct_1 = new_value
        self._bounds["long_filter_volatility_drop_pct"][1] = new_value
    @long_filter_volatility_drop_pct_step.setter
    def long_filter_volatility_drop_pct_step(self, new_value):
        self._long_filter_volatility_drop_pct_step = new_value
        if len(self._bounds["long_filter_volatility_drop_pct"]) < 3:
            self._bounds["long_filter_volatility_drop_pct"].append(new_value)
        else:
            self._bounds["long_filter_volatility_drop_pct"][2] = new_value
    @long_filter_volume_ema_span_0.setter
    def long_filter_volume_ema_span_0(self, new_value):
        self._long_filter_volume_ema_span_0 = new_value
        self._bounds["long_filter_volume_ema_span"][0] = new_value
    @long_filter_volume_ema_span_1.setter
    def long_filter_volume_ema_span_1(self, new_value):
        self._long_filter_volume_ema_span_1 = new_value
        self._bounds["long_filter_volume_ema_span"][1] = new_value
    @long_filter_volume_ema_span_step.setter
    def long_filter_volume_ema_span_step(self, new_value):
        self._long_filter_volume_ema_span_step = new_value
        if len(self._bounds["long_filter_volume_ema_span"]) < 3:
            self._bounds["long_filter_volume_ema_span"].append(new_value)
        else:
            self._bounds["long_filter_volume_ema_span"][2] = new_value
    @long_n_positions_0.setter
    def long_n_positions_0(self, new_value):
        self._long_n_positions_0 = new_value
        self._bounds["long_n_positions"][0] = new_value
    @long_n_positions_1.setter
    def long_n_positions_1(self, new_value):
        self._long_n_positions_1 = new_value
        self._bounds["long_n_positions"][1] = new_value
    @long_n_positions_step.setter
    def long_n_positions_step(self, new_value):
        self._long_n_positions_step = new_value
        if len(self._bounds["long_n_positions"]) < 3:
            self._bounds["long_n_positions"].append(new_value)
        else:
            self._bounds["long_n_positions"][2] = new_value
    @long_total_wallet_exposure_limit_0.setter
    def long_total_wallet_exposure_limit_0(self, new_value):
        self._long_total_wallet_exposure_limit_0 = new_value
        self._bounds["long_total_wallet_exposure_limit"][0] = new_value
    @long_total_wallet_exposure_limit_1.setter
    def long_total_wallet_exposure_limit_1(self, new_value):
        self._long_total_wallet_exposure_limit_1 = new_value
        self._bounds["long_total_wallet_exposure_limit"][1] = new_value
    @long_total_wallet_exposure_limit_step.setter
    def long_total_wallet_exposure_limit_step(self, new_value):
        self._long_total_wallet_exposure_limit_step = new_value
        if len(self._bounds["long_total_wallet_exposure_limit"]) < 3:
            self._bounds["long_total_wallet_exposure_limit"].append(new_value)
        else:
            self._bounds["long_total_wallet_exposure_limit"][2] = new_value
    @long_unstuck_close_pct_0.setter
    def long_unstuck_close_pct_0(self, new_value):
        self._long_unstuck_close_pct_0 = new_value
        self._bounds["long_unstuck_close_pct"][0] = new_value
    @long_unstuck_close_pct_1.setter
    def long_unstuck_close_pct_1(self, new_value):
        self._long_unstuck_close_pct_1 = new_value
        self._bounds["long_unstuck_close_pct"][1] = new_value
    @long_unstuck_close_pct_step.setter
    def long_unstuck_close_pct_step(self, new_value):
        self._long_unstuck_close_pct_step = new_value
        if len(self._bounds["long_unstuck_close_pct"]) < 3:
            self._bounds["long_unstuck_close_pct"].append(new_value)
        else:
            self._bounds["long_unstuck_close_pct"][2] = new_value
    @long_unstuck_ema_dist_0.setter
    def long_unstuck_ema_dist_0(self, new_value):
        self._long_unstuck_ema_dist_0 = new_value
        self._bounds["long_unstuck_ema_dist"][0] = new_value
    @long_unstuck_ema_dist_1.setter
    def long_unstuck_ema_dist_1(self, new_value):
        self._long_unstuck_ema_dist_1 = new_value
        self._bounds["long_unstuck_ema_dist"][1] = new_value
    @long_unstuck_ema_dist_step.setter
    def long_unstuck_ema_dist_step(self, new_value):
        self._long_unstuck_ema_dist_step = new_value
        if len(self._bounds["long_unstuck_ema_dist"]) < 3:
            self._bounds["long_unstuck_ema_dist"].append(new_value)
        else:
            self._bounds["long_unstuck_ema_dist"][2] = new_value
    @long_unstuck_loss_allowance_pct_0.setter
    def long_unstuck_loss_allowance_pct_0(self, new_value):
        self._long_unstuck_loss_allowance_pct_0 = new_value
        self._bounds["long_unstuck_loss_allowance_pct"][0] = new_value
    @long_unstuck_loss_allowance_pct_1.setter
    def long_unstuck_loss_allowance_pct_1(self, new_value):
        self._long_unstuck_loss_allowance_pct_1 = new_value
        self._bounds["long_unstuck_loss_allowance_pct"][1] = new_value
    @long_unstuck_loss_allowance_pct_step.setter
    def long_unstuck_loss_allowance_pct_step(self, new_value):
        self._long_unstuck_loss_allowance_pct_step = new_value
        if len(self._bounds["long_unstuck_loss_allowance_pct"]) < 3:
            self._bounds["long_unstuck_loss_allowance_pct"].append(new_value)
        else:
            self._bounds["long_unstuck_loss_allowance_pct"][2] = new_value
    @long_unstuck_threshold_0.setter
    def long_unstuck_threshold_0(self, new_value):
        self._long_unstuck_threshold_0 = new_value
        self._bounds["long_unstuck_threshold"][0] = new_value
    @long_unstuck_threshold_1.setter
    def long_unstuck_threshold_1(self, new_value):
        self._long_unstuck_threshold_1 = new_value
        self._bounds["long_unstuck_threshold"][1] = new_value
    @long_unstuck_threshold_step.setter
    def long_unstuck_threshold_step(self, new_value):
        self._long_unstuck_threshold_step = new_value
        if len(self._bounds["long_unstuck_threshold"]) < 3:
            self._bounds["long_unstuck_threshold"].append(new_value)
        else:
            self._bounds["long_unstuck_threshold"][2] = new_value
    @long_risk_wel_enforcer_threshold_0.setter
    def long_risk_wel_enforcer_threshold_0(self, new_value):
        self._long_risk_wel_enforcer_threshold_0 = new_value
        self._bounds["long_risk_wel_enforcer_threshold"][0] = new_value
    @long_risk_wel_enforcer_threshold_1.setter
    def long_risk_wel_enforcer_threshold_1(self, new_value):
        self._long_risk_wel_enforcer_threshold_1 = new_value
        self._bounds["long_risk_wel_enforcer_threshold"][1] = new_value
    @long_risk_wel_enforcer_threshold_step.setter
    def long_risk_wel_enforcer_threshold_step(self, new_value):
        self._long_risk_wel_enforcer_threshold_step = new_value
        if len(self._bounds["long_risk_wel_enforcer_threshold"]) < 3:
            self._bounds["long_risk_wel_enforcer_threshold"].append(new_value)
        else:
            self._bounds["long_risk_wel_enforcer_threshold"][2] = new_value
    @long_risk_we_excess_allowance_pct_0.setter
    def long_risk_we_excess_allowance_pct_0(self, new_value):
        self._long_risk_we_excess_allowance_pct_0 = new_value
        self._bounds["long_risk_we_excess_allowance_pct"][0] = new_value
    @long_risk_we_excess_allowance_pct_1.setter
    def long_risk_we_excess_allowance_pct_1(self, new_value):
        self._long_risk_we_excess_allowance_pct_1 = new_value
        self._bounds["long_risk_we_excess_allowance_pct"][1] = new_value
    @long_risk_we_excess_allowance_pct_step.setter
    def long_risk_we_excess_allowance_pct_step(self, new_value):
        self._long_risk_we_excess_allowance_pct_step = new_value
        if len(self._bounds["long_risk_we_excess_allowance_pct"]) < 3:
            self._bounds["long_risk_we_excess_allowance_pct"].append(new_value)
        else:
            self._bounds["long_risk_we_excess_allowance_pct"][2] = new_value
    @long_risk_twel_enforcer_threshold_0.setter
    def long_risk_twel_enforcer_threshold_0(self, new_value):
        self._long_risk_twel_enforcer_threshold_0 = new_value
        self._bounds["long_risk_twel_enforcer_threshold"][0] = new_value
    @long_risk_twel_enforcer_threshold_1.setter
    def long_risk_twel_enforcer_threshold_1(self, new_value):
        self._long_risk_twel_enforcer_threshold_1 = new_value
        self._bounds["long_risk_twel_enforcer_threshold"][1] = new_value
    @long_risk_twel_enforcer_threshold_step.setter
    def long_risk_twel_enforcer_threshold_step(self, new_value):
        self._long_risk_twel_enforcer_threshold_step = new_value
        if len(self._bounds["long_risk_twel_enforcer_threshold"]) < 3:
            self._bounds["long_risk_twel_enforcer_threshold"].append(new_value)
        else:
            self._bounds["long_risk_twel_enforcer_threshold"][2] = new_value

    # Short setters
    @short_close_grid_markup_end_0.setter
    def short_close_grid_markup_end_0(self, new_value):
        self._short_close_grid_markup_end_0 = new_value
        self._bounds["short_close_grid_markup_end"][0] = new_value
    @short_close_grid_markup_end_1.setter
    def short_close_grid_markup_end_1(self, new_value):
        self._short_close_grid_markup_end_1 = new_value
        self._bounds["short_close_grid_markup_end"][1] = new_value
    @short_close_grid_markup_end_step.setter
    def short_close_grid_markup_end_step(self, new_value):
        self._short_close_grid_markup_end_step = new_value
        if len(self._bounds["short_close_grid_markup_end"]) < 3:
            self._bounds["short_close_grid_markup_end"].append(new_value)
        else:
            self._bounds["short_close_grid_markup_end"][2] = new_value
    @short_close_grid_markup_start_0.setter
    def short_close_grid_markup_start_0(self, new_value):
        self._short_close_grid_markup_start_0 = new_value
        self._bounds["short_close_grid_markup_start"][0] = new_value
    @short_close_grid_markup_start_1.setter
    def short_close_grid_markup_start_1(self, new_value):
        self._short_close_grid_markup_start_1 = new_value
        self._bounds["short_close_grid_markup_start"][1] = new_value
    @short_close_grid_markup_start_step.setter
    def short_close_grid_markup_start_step(self, new_value):
        self._short_close_grid_markup_start_step = new_value
        if len(self._bounds["short_close_grid_markup_start"]) < 3:
            self._bounds["short_close_grid_markup_start"].append(new_value)
        else:
            self._bounds["short_close_grid_markup_start"][2] = new_value
    @short_close_grid_qty_pct_0.setter
    def short_close_grid_qty_pct_0(self, new_value):
        self._short_close_grid_qty_pct_0 = new_value
        self._bounds["short_close_grid_qty_pct"][0] = new_value
    @short_close_grid_qty_pct_1.setter
    def short_close_grid_qty_pct_1(self, new_value):
        self._short_close_grid_qty_pct_1 = new_value
        self._bounds["short_close_grid_qty_pct"][1] = new_value
    @short_close_grid_qty_pct_step.setter
    def short_close_grid_qty_pct_step(self, new_value):
        self._short_close_grid_qty_pct_step = new_value
        if len(self._bounds["short_close_grid_qty_pct"]) < 3:
            self._bounds["short_close_grid_qty_pct"].append(new_value)
        else:
            self._bounds["short_close_grid_qty_pct"][2] = new_value
    @short_close_trailing_grid_ratio_0.setter
    def short_close_trailing_grid_ratio_0(self, new_value):
        self._short_close_trailing_grid_ratio_0 = new_value
        self._bounds["short_close_trailing_grid_ratio"][0] = new_value
    @short_close_trailing_grid_ratio_1.setter
    def short_close_trailing_grid_ratio_1(self, new_value):
        self._short_close_trailing_grid_ratio_1 = new_value
        self._bounds["short_close_trailing_grid_ratio"][1] = new_value
    @short_close_trailing_grid_ratio_step.setter
    def short_close_trailing_grid_ratio_step(self, new_value):
        self._short_close_trailing_grid_ratio_step = new_value
        if len(self._bounds["short_close_trailing_grid_ratio"]) < 3:
            self._bounds["short_close_trailing_grid_ratio"].append(new_value)
        else:
            self._bounds["short_close_trailing_grid_ratio"][2] = new_value
    @short_close_trailing_qty_pct_0.setter
    def short_close_trailing_qty_pct_0(self, new_value):
        self._short_close_trailing_qty_pct_0 = new_value
        self._bounds["short_close_trailing_qty_pct"][0] = new_value
    @short_close_trailing_qty_pct_1.setter
    def short_close_trailing_qty_pct_1(self, new_value):
        self._short_close_trailing_qty_pct_1 = new_value
        self._bounds["short_close_trailing_qty_pct"][1] = new_value
    @short_close_trailing_qty_pct_step.setter
    def short_close_trailing_qty_pct_step(self, new_value):
        self._short_close_trailing_qty_pct_step = new_value
        if len(self._bounds["short_close_trailing_qty_pct"]) < 3:
            self._bounds["short_close_trailing_qty_pct"].append(new_value)
        else:
            self._bounds["short_close_trailing_qty_pct"][2] = new_value
    @short_close_trailing_retracement_pct_0.setter
    def short_close_trailing_retracement_pct_0(self, new_value):
        self._short_close_trailing_retracement_pct_0 = new_value
        self._bounds["short_close_trailing_retracement_pct"][0] = new_value
    @short_close_trailing_retracement_pct_1.setter
    def short_close_trailing_retracement_pct_1(self, new_value):
        self._short_close_trailing_retracement_pct_1 = new_value
        self._bounds["short_close_trailing_retracement_pct"][1] = new_value
    @short_close_trailing_retracement_pct_step.setter
    def short_close_trailing_retracement_pct_step(self, new_value):
        self._short_close_trailing_retracement_pct_step = new_value
        if len(self._bounds["short_close_trailing_retracement_pct"]) < 3:
            self._bounds["short_close_trailing_retracement_pct"].append(new_value)
        else:
            self._bounds["short_close_trailing_retracement_pct"][2] = new_value
    @short_close_trailing_threshold_pct_0.setter
    def short_close_trailing_threshold_pct_0(self, new_value):
        self._short_close_trailing_threshold_pct_0 = new_value
        self._bounds["short_close_trailing_threshold_pct"][0] = new_value
    @short_close_trailing_threshold_pct_1.setter
    def short_close_trailing_threshold_pct_1(self, new_value):
        self._short_close_trailing_threshold_pct_1 = new_value
        self._bounds["short_close_trailing_threshold_pct"][1] = new_value
    @short_close_trailing_threshold_pct_step.setter
    def short_close_trailing_threshold_pct_step(self, new_value):
        self._short_close_trailing_threshold_pct_step = new_value
        if len(self._bounds["short_close_trailing_threshold_pct"]) < 3:
            self._bounds["short_close_trailing_threshold_pct"].append(new_value)
        else:
            self._bounds["short_close_trailing_threshold_pct"][2] = new_value
    @short_ema_span_0_0.setter
    def short_ema_span_0_0(self, new_value):
        self._short_ema_span_0_0 = new_value
        self._bounds["short_ema_span_0"][0] = new_value
    @short_ema_span_0_1.setter
    def short_ema_span_0_1(self, new_value):
        self._short_ema_span_0_1 = new_value
        self._bounds["short_ema_span_0"][1] = new_value
    @short_ema_span_0_step.setter
    def short_ema_span_0_step(self, new_value):
        self._short_ema_span_0_step = new_value
        if len(self._bounds["short_ema_span_0"]) < 3:
            self._bounds["short_ema_span_0"].append(new_value)
        else:
            self._bounds["short_ema_span_0"][2] = new_value
    @short_ema_span_1_0.setter
    def short_ema_span_1_0(self, new_value):
        self._short_ema_span_1_0 = new_value
        self._bounds["short_ema_span_1"][0] = new_value
    @short_ema_span_1_1.setter
    def short_ema_span_1_1(self, new_value):
        self._short_ema_span_1_1 = new_value
        self._bounds["short_ema_span_1"][1] = new_value
    @short_ema_span_1_step.setter
    def short_ema_span_1_step(self, new_value):
        self._short_ema_span_1_step = new_value
        if len(self._bounds["short_ema_span_1"]) < 3:
            self._bounds["short_ema_span_1"].append(new_value)
        else:
            self._bounds["short_ema_span_1"][2] = new_value
    @short_entry_grid_double_down_factor_0.setter
    def short_entry_grid_double_down_factor_0(self, new_value):
        self._short_entry_grid_double_down_factor_0 = new_value
        self._bounds["short_entry_grid_double_down_factor"][0] = new_value
    @short_entry_grid_double_down_factor_1.setter
    def short_entry_grid_double_down_factor_1(self, new_value):
        self._short_entry_grid_double_down_factor_1 = new_value
        self._bounds["short_entry_grid_double_down_factor"][1] = new_value
    @short_entry_grid_double_down_factor_step.setter
    def short_entry_grid_double_down_factor_step(self, new_value):
        self._short_entry_grid_double_down_factor_step = new_value
        if len(self._bounds["short_entry_grid_double_down_factor"]) < 3:
            self._bounds["short_entry_grid_double_down_factor"].append(new_value)
        else:
            self._bounds["short_entry_grid_double_down_factor"][2] = new_value
    @short_entry_volatility_ema_span_hours_0.setter
    def short_entry_volatility_ema_span_hours_0(self, new_value):
        self._short_entry_volatility_ema_span_hours_0 = new_value
        self._bounds["short_entry_volatility_ema_span_hours"][0] = new_value
    @short_entry_volatility_ema_span_hours_1.setter
    def short_entry_volatility_ema_span_hours_1(self, new_value):
        self._short_entry_volatility_ema_span_hours_1 = new_value
        self._bounds["short_entry_volatility_ema_span_hours"][1] = new_value
    @short_entry_volatility_ema_span_hours_step.setter
    def short_entry_volatility_ema_span_hours_step(self, new_value):
        self._short_entry_volatility_ema_span_hours_step = new_value
        if len(self._bounds["short_entry_volatility_ema_span_hours"]) < 3:
            self._bounds["short_entry_volatility_ema_span_hours"].append(new_value)
        else:
            self._bounds["short_entry_volatility_ema_span_hours"][2] = new_value
    @short_entry_grid_spacing_volatility_weight_0.setter
    def short_entry_grid_spacing_volatility_weight_0(self, new_value):
        self._short_entry_grid_spacing_volatility_weight_0 = new_value
        self._bounds["short_entry_grid_spacing_volatility_weight"][0] = new_value
    @short_entry_grid_spacing_volatility_weight_1.setter
    def short_entry_grid_spacing_volatility_weight_1(self, new_value):
        self._short_entry_grid_spacing_volatility_weight_1 = new_value
        self._bounds["short_entry_grid_spacing_volatility_weight"][1] = new_value
    @short_entry_grid_spacing_volatility_weight_step.setter
    def short_entry_grid_spacing_volatility_weight_step(self, new_value):
        self._short_entry_grid_spacing_volatility_weight_step = new_value
        if len(self._bounds["short_entry_grid_spacing_volatility_weight"]) < 3:
            self._bounds["short_entry_grid_spacing_volatility_weight"].append(new_value)
        else:
            self._bounds["short_entry_grid_spacing_volatility_weight"][2] = new_value
    @short_entry_grid_spacing_pct_0.setter
    def short_entry_grid_spacing_pct_0(self, new_value):
        self._short_entry_grid_spacing_pct_0 = new_value
        self._bounds["short_entry_grid_spacing_pct"][0] = new_value
    @short_entry_grid_spacing_pct_1.setter
    def short_entry_grid_spacing_pct_1(self, new_value):
        self._short_entry_grid_spacing_pct_1 = new_value
        self._bounds["short_entry_grid_spacing_pct"][1] = new_value
    @short_entry_grid_spacing_pct_step.setter
    def short_entry_grid_spacing_pct_step(self, new_value):
        self._short_entry_grid_spacing_pct_step = new_value
        if len(self._bounds["short_entry_grid_spacing_pct"]) < 3:
            self._bounds["short_entry_grid_spacing_pct"].append(new_value)
        else:
            self._bounds["short_entry_grid_spacing_pct"][2] = new_value
    @short_entry_grid_spacing_we_weight_0.setter
    def short_entry_grid_spacing_we_weight_0(self, new_value):
        self._short_entry_grid_spacing_we_weight_0 = new_value
        self._bounds["short_entry_grid_spacing_we_weight"][0] = new_value
    @short_entry_grid_spacing_we_weight_1.setter
    def short_entry_grid_spacing_we_weight_1(self, new_value):
        self._short_entry_grid_spacing_we_weight_1 = new_value
        self._bounds["short_entry_grid_spacing_we_weight"][1] = new_value
    @short_entry_grid_spacing_we_weight_step.setter
    def short_entry_grid_spacing_we_weight_step(self, new_value):
        self._short_entry_grid_spacing_we_weight_step = new_value
        if len(self._bounds["short_entry_grid_spacing_we_weight"]) < 3:
            self._bounds["short_entry_grid_spacing_we_weight"].append(new_value)
        else:
            self._bounds["short_entry_grid_spacing_we_weight"][2] = new_value
    @short_entry_initial_ema_dist_0.setter
    def short_entry_initial_ema_dist_0(self, new_value):
        self._short_entry_initial_ema_dist_0 = new_value
        self._bounds["short_entry_initial_ema_dist"][0] = new_value
    @short_entry_initial_ema_dist_1.setter
    def short_entry_initial_ema_dist_1(self, new_value):
        self._short_entry_initial_ema_dist_1 = new_value
        self._bounds["short_entry_initial_ema_dist"][1] = new_value
    @short_entry_initial_ema_dist_step.setter
    def short_entry_initial_ema_dist_step(self, new_value):
        self._short_entry_initial_ema_dist_step = new_value
        if len(self._bounds["short_entry_initial_ema_dist"]) < 3:
            self._bounds["short_entry_initial_ema_dist"].append(new_value)
        else:
            self._bounds["short_entry_initial_ema_dist"][2] = new_value
    @short_entry_initial_qty_pct_0.setter
    def short_entry_initial_qty_pct_0(self, new_value):
        self._short_entry_initial_qty_pct_0 = new_value
        self._bounds["short_entry_initial_qty_pct"][0] = new_value
    @short_entry_initial_qty_pct_1.setter
    def short_entry_initial_qty_pct_1(self, new_value):
        self._short_entry_initial_qty_pct_1 = new_value
        self._bounds["short_entry_initial_qty_pct"][1] = new_value
    @short_entry_initial_qty_pct_step.setter
    def short_entry_initial_qty_pct_step(self, new_value):
        self._short_entry_initial_qty_pct_step = new_value
        if len(self._bounds["short_entry_initial_qty_pct"]) < 3:
            self._bounds["short_entry_initial_qty_pct"].append(new_value)
        else:
            self._bounds["short_entry_initial_qty_pct"][2] = new_value
    @short_entry_trailing_double_down_factor_0.setter
    def short_entry_trailing_double_down_factor_0(self, new_value):
        self._short_entry_trailing_double_down_factor_0 = new_value
        self._bounds["short_entry_trailing_double_down_factor"][0] = new_value
    @short_entry_trailing_double_down_factor_1.setter
    def short_entry_trailing_double_down_factor_1(self, new_value):
        self._short_entry_trailing_double_down_factor_1 = new_value
        self._bounds["short_entry_trailing_double_down_factor"][1] = new_value
    @short_entry_trailing_double_down_factor_step.setter
    def short_entry_trailing_double_down_factor_step(self, new_value):
        self._short_entry_trailing_double_down_factor_step = new_value
        if len(self._bounds["short_entry_trailing_double_down_factor"]) < 3:
            self._bounds["short_entry_trailing_double_down_factor"].append(new_value)
        else:
            self._bounds["short_entry_trailing_double_down_factor"][2] = new_value
    @short_entry_trailing_grid_ratio_0.setter
    def short_entry_trailing_grid_ratio_0(self, new_value):
        self._short_entry_trailing_grid_ratio_0 = new_value
        self._bounds["short_entry_trailing_grid_ratio"][0] = new_value
    @short_entry_trailing_grid_ratio_1.setter
    def short_entry_trailing_grid_ratio_1(self, new_value):
        self._short_entry_trailing_grid_ratio_1 = new_value
        self._bounds["short_entry_trailing_grid_ratio"][1] = new_value
    @short_entry_trailing_grid_ratio_step.setter
    def short_entry_trailing_grid_ratio_step(self, new_value):
        self._short_entry_trailing_grid_ratio_step = new_value
        if len(self._bounds["short_entry_trailing_grid_ratio"]) < 3:
            self._bounds["short_entry_trailing_grid_ratio"].append(new_value)
        else:
            self._bounds["short_entry_trailing_grid_ratio"][2] = new_value
    @short_entry_trailing_retracement_pct_0.setter
    def short_entry_trailing_retracement_pct_0(self, new_value):
        self._short_entry_trailing_retracement_pct_0 = new_value
        self._bounds["short_entry_trailing_retracement_pct"][0] = new_value
    @short_entry_trailing_retracement_pct_1.setter
    def short_entry_trailing_retracement_pct_1(self, new_value):
        self._short_entry_trailing_retracement_pct_1 = new_value
        self._bounds["short_entry_trailing_retracement_pct"][1] = new_value
    @short_entry_trailing_retracement_pct_step.setter
    def short_entry_trailing_retracement_pct_step(self, new_value):
        self._short_entry_trailing_retracement_pct_step = new_value
        if len(self._bounds["short_entry_trailing_retracement_pct"]) < 3:
            self._bounds["short_entry_trailing_retracement_pct"].append(new_value)
        else:
            self._bounds["short_entry_trailing_retracement_pct"][2] = new_value
    @short_entry_trailing_retracement_we_weight_0.setter
    def short_entry_trailing_retracement_we_weight_0(self, new_value):
        self._short_entry_trailing_retracement_we_weight_0 = new_value
        self._bounds["short_entry_trailing_retracement_we_weight"][0] = new_value
    @short_entry_trailing_retracement_we_weight_1.setter
    def short_entry_trailing_retracement_we_weight_1(self, new_value):
        self._short_entry_trailing_retracement_we_weight_1 = new_value
        self._bounds["short_entry_trailing_retracement_we_weight"][1] = new_value
    @short_entry_trailing_retracement_we_weight_step.setter
    def short_entry_trailing_retracement_we_weight_step(self, new_value):
        self._short_entry_trailing_retracement_we_weight_step = new_value
        if len(self._bounds["short_entry_trailing_retracement_we_weight"]) < 3:
            self._bounds["short_entry_trailing_retracement_we_weight"].append(new_value)
        else:
            self._bounds["short_entry_trailing_retracement_we_weight"][2] = new_value
    @short_entry_trailing_retracement_volatility_weight_0.setter
    def short_entry_trailing_retracement_volatility_weight_0(self, new_value):
        self._short_entry_trailing_retracement_volatility_weight_0 = new_value
        self._bounds["short_entry_trailing_retracement_volatility_weight"][0] = new_value
    @short_entry_trailing_retracement_volatility_weight_1.setter
    def short_entry_trailing_retracement_volatility_weight_1(self, new_value):
        self._short_entry_trailing_retracement_volatility_weight_1 = new_value
        self._bounds["short_entry_trailing_retracement_volatility_weight"][1] = new_value
    @short_entry_trailing_retracement_volatility_weight_step.setter
    def short_entry_trailing_retracement_volatility_weight_step(self, new_value):
        self._short_entry_trailing_retracement_volatility_weight_step = new_value
        if len(self._bounds["short_entry_trailing_retracement_volatility_weight"]) < 3:
            self._bounds["short_entry_trailing_retracement_volatility_weight"].append(new_value)
        else:
            self._bounds["short_entry_trailing_retracement_volatility_weight"][2] = new_value
    @short_entry_trailing_threshold_pct_0.setter
    def short_entry_trailing_threshold_pct_0(self, new_value):
        self._short_entry_trailing_threshold_pct_0 = new_value
        self._bounds["short_entry_trailing_threshold_pct"][0] = new_value
    @short_entry_trailing_threshold_pct_1.setter
    def short_entry_trailing_threshold_pct_1(self, new_value):
        self._short_entry_trailing_threshold_pct_1 = new_value
        self._bounds["short_entry_trailing_threshold_pct"][1] = new_value
    @short_entry_trailing_threshold_pct_step.setter
    def short_entry_trailing_threshold_pct_step(self, new_value):
        self._short_entry_trailing_threshold_pct_step = new_value
        if len(self._bounds["short_entry_trailing_threshold_pct"]) < 3:
            self._bounds["short_entry_trailing_threshold_pct"].append(new_value)
        else:
            self._bounds["short_entry_trailing_threshold_pct"][2] = new_value
    @short_entry_trailing_threshold_we_weight_0.setter
    def short_entry_trailing_threshold_we_weight_0(self, new_value):
        self._short_entry_trailing_threshold_we_weight_0 = new_value
        self._bounds["short_entry_trailing_threshold_we_weight"][0] = new_value
    @short_entry_trailing_threshold_we_weight_1.setter
    def short_entry_trailing_threshold_we_weight_1(self, new_value):
        self._short_entry_trailing_threshold_we_weight_1 = new_value
        self._bounds["short_entry_trailing_threshold_we_weight"][1] = new_value
    @short_entry_trailing_threshold_we_weight_step.setter
    def short_entry_trailing_threshold_we_weight_step(self, new_value):
        self._short_entry_trailing_threshold_we_weight_step = new_value
        if len(self._bounds["short_entry_trailing_threshold_we_weight"]) < 3:
            self._bounds["short_entry_trailing_threshold_we_weight"].append(new_value)
        else:
            self._bounds["short_entry_trailing_threshold_we_weight"][2] = new_value
    @short_entry_trailing_threshold_volatility_weight_0.setter
    def short_entry_trailing_threshold_volatility_weight_0(self, new_value):
        self._short_entry_trailing_threshold_volatility_weight_0 = new_value
        self._bounds["short_entry_trailing_threshold_volatility_weight"][0] = new_value
    @short_entry_trailing_threshold_volatility_weight_1.setter
    def short_entry_trailing_threshold_volatility_weight_1(self, new_value):
        self._short_entry_trailing_threshold_volatility_weight_1 = new_value
        self._bounds["short_entry_trailing_threshold_volatility_weight"][1] = new_value
    @short_entry_trailing_threshold_volatility_weight_step.setter
    def short_entry_trailing_threshold_volatility_weight_step(self, new_value):
        self._short_entry_trailing_threshold_volatility_weight_step = new_value
        if len(self._bounds["short_entry_trailing_threshold_volatility_weight"]) < 3:
            self._bounds["short_entry_trailing_threshold_volatility_weight"].append(new_value)
        else:
            self._bounds["short_entry_trailing_threshold_volatility_weight"][2] = new_value
    @short_filter_volatility_ema_span_0.setter
    def short_filter_volatility_ema_span_0(self, new_value):
        self._short_filter_volatility_ema_span_0 = new_value
        self._bounds["short_filter_volatility_ema_span"][0] = new_value
    @short_filter_volatility_ema_span_1.setter
    def short_filter_volatility_ema_span_1(self, new_value):
        self._short_filter_volatility_ema_span_1 = new_value
        self._bounds["short_filter_volatility_ema_span"][1] = new_value
    @short_filter_volatility_ema_span_step.setter
    def short_filter_volatility_ema_span_step(self, new_value):
        self._short_filter_volatility_ema_span_step = new_value
        if len(self._bounds["short_filter_volatility_ema_span"]) < 3:
            self._bounds["short_filter_volatility_ema_span"].append(new_value)
        else:
            self._bounds["short_filter_volatility_ema_span"][2] = new_value
    @short_filter_volume_drop_pct_0.setter
    def short_filter_volume_drop_pct_0(self, new_value):
        self._short_filter_volume_drop_pct_0 = new_value
        self._bounds["short_filter_volume_drop_pct"][0] = new_value
    @short_filter_volume_drop_pct_1.setter
    def short_filter_volume_drop_pct_1(self, new_value):
        self._short_filter_volume_drop_pct_1 = new_value
        self._bounds["short_filter_volume_drop_pct"][1] = new_value
    @short_filter_volume_drop_pct_step.setter
    def short_filter_volume_drop_pct_step(self, new_value):
        self._short_filter_volume_drop_pct_step = new_value
        if len(self._bounds["short_filter_volume_drop_pct"]) < 3:
            self._bounds["short_filter_volume_drop_pct"].append(new_value)
        else:
            self._bounds["short_filter_volume_drop_pct"][2] = new_value
    @short_filter_volatility_drop_pct_0.setter
    def short_filter_volatility_drop_pct_0(self, new_value):
        self._short_filter_volatility_drop_pct_0 = new_value
        self._bounds["short_filter_volatility_drop_pct"][0] = new_value
    @short_filter_volatility_drop_pct_1.setter
    def short_filter_volatility_drop_pct_1(self, new_value):
        self._short_filter_volatility_drop_pct_1 = new_value
        self._bounds["short_filter_volatility_drop_pct"][1] = new_value
    @short_filter_volatility_drop_pct_step.setter
    def short_filter_volatility_drop_pct_step(self, new_value):
        self._short_filter_volatility_drop_pct_step = new_value
        if len(self._bounds["short_filter_volatility_drop_pct"]) < 3:
            self._bounds["short_filter_volatility_drop_pct"].append(new_value)
        else:
            self._bounds["short_filter_volatility_drop_pct"][2] = new_value
    @short_filter_volume_ema_span_0.setter
    def short_filter_volume_ema_span_0(self, new_value):
        self._short_filter_volume_ema_span_0 = new_value
        self._bounds["short_filter_volume_ema_span"][0] = new_value
    @short_filter_volume_ema_span_1.setter
    def short_filter_volume_ema_span_1(self, new_value):
        self._short_filter_volume_ema_span_1 = new_value
        self._bounds["short_filter_volume_ema_span"][1] = new_value
    @short_filter_volume_ema_span_step.setter
    def short_filter_volume_ema_span_step(self, new_value):
        self._short_filter_volume_ema_span_step = new_value
        if len(self._bounds["short_filter_volume_ema_span"]) < 3:
            self._bounds["short_filter_volume_ema_span"].append(new_value)
        else:
            self._bounds["short_filter_volume_ema_span"][2] = new_value
    @short_n_positions_0.setter
    def short_n_positions_0(self, new_value):
        self._short_n_positions_0 = new_value
        self._bounds["short_n_positions"][0] = new_value
    @short_n_positions_1.setter
    def short_n_positions_1(self, new_value):
        self._short_n_positions_1 = new_value
        self._bounds["short_n_positions"][1] = new_value
    @short_n_positions_step.setter
    def short_n_positions_step(self, new_value):
        self._short_n_positions_step = new_value
        if len(self._bounds["short_n_positions"]) < 3:
            self._bounds["short_n_positions"].append(new_value)
        else:
            self._bounds["short_n_positions"][2] = new_value
    @short_total_wallet_exposure_limit_0.setter
    def short_total_wallet_exposure_limit_0(self, new_value):
        self._short_total_wallet_exposure_limit_0 = new_value
        self._bounds["short_total_wallet_exposure_limit"][0] = new_value
    @short_total_wallet_exposure_limit_1.setter
    def short_total_wallet_exposure_limit_1(self, new_value):
        self._short_total_wallet_exposure_limit_1 = new_value
        self._bounds["short_total_wallet_exposure_limit"][1] = new_value
    @short_total_wallet_exposure_limit_step.setter
    def short_total_wallet_exposure_limit_step(self, new_value):
        self._short_total_wallet_exposure_limit_step = new_value
        if len(self._bounds["short_total_wallet_exposure_limit"]) < 3:
            self._bounds["short_total_wallet_exposure_limit"].append(new_value)
        else:
            self._bounds["short_total_wallet_exposure_limit"][2] = new_value
    @short_unstuck_close_pct_0.setter
    def short_unstuck_close_pct_0(self, new_value):
        self._short_unstuck_close_pct_0 = new_value
        self._bounds["short_unstuck_close_pct"][0] = new_value
    @short_unstuck_close_pct_1.setter
    def short_unstuck_close_pct_1(self, new_value):
        self._short_unstuck_close_pct_1 = new_value
        self._bounds["short_unstuck_close_pct"][1] = new_value
    @short_unstuck_close_pct_step.setter
    def short_unstuck_close_pct_step(self, new_value):
        self._short_unstuck_close_pct_step = new_value
        if len(self._bounds["short_unstuck_close_pct"]) < 3:
            self._bounds["short_unstuck_close_pct"].append(new_value)
        else:
            self._bounds["short_unstuck_close_pct"][2] = new_value
    @short_unstuck_ema_dist_0.setter
    def short_unstuck_ema_dist_0(self, new_value):
        self._short_unstuck_ema_dist_0 = new_value
        self._bounds["short_unstuck_ema_dist"][0] = new_value
    @short_unstuck_ema_dist_1.setter
    def short_unstuck_ema_dist_1(self, new_value):
        self._short_unstuck_ema_dist_1 = new_value
        self._bounds["short_unstuck_ema_dist"][1] = new_value
    @short_unstuck_ema_dist_step.setter
    def short_unstuck_ema_dist_step(self, new_value):
        self._short_unstuck_ema_dist_step = new_value
        if len(self._bounds["short_unstuck_ema_dist"]) < 3:
            self._bounds["short_unstuck_ema_dist"].append(new_value)
        else:
            self._bounds["short_unstuck_ema_dist"][2] = new_value
    @short_unstuck_loss_allowance_pct_0.setter
    def short_unstuck_loss_allowance_pct_0(self, new_value):
        self._short_unstuck_loss_allowance_pct_0 = new_value
        self._bounds["short_unstuck_loss_allowance_pct"][0] = new_value
    @short_unstuck_loss_allowance_pct_1.setter
    def short_unstuck_loss_allowance_pct_1(self, new_value):
        self._short_unstuck_loss_allowance_pct_1 = new_value
        self._bounds["short_unstuck_loss_allowance_pct"][1] = new_value
    @short_unstuck_loss_allowance_pct_step.setter
    def short_unstuck_loss_allowance_pct_step(self, new_value):
        self._short_unstuck_loss_allowance_pct_step = new_value
        if len(self._bounds["short_unstuck_loss_allowance_pct"]) < 3:
            self._bounds["short_unstuck_loss_allowance_pct"].append(new_value)
        else:
            self._bounds["short_unstuck_loss_allowance_pct"][2] = new_value
    @short_unstuck_threshold_0.setter
    def short_unstuck_threshold_0(self, new_value):
        self._short_unstuck_threshold_0 = new_value
        self._bounds["short_unstuck_threshold"][0] = new_value
    @short_unstuck_threshold_1.setter
    def short_unstuck_threshold_1(self, new_value):
        self._short_unstuck_threshold_1 = new_value
        self._bounds["short_unstuck_threshold"][1] = new_value
    @short_unstuck_threshold_step.setter
    def short_unstuck_threshold_step(self, new_value):
        self._short_unstuck_threshold_step = new_value
        if len(self._bounds["short_unstuck_threshold"]) < 3:
            self._bounds["short_unstuck_threshold"].append(new_value)
        else:
            self._bounds["short_unstuck_threshold"][2] = new_value
    @short_risk_wel_enforcer_threshold_0.setter
    def short_risk_wel_enforcer_threshold_0(self, new_value):
        self._short_risk_wel_enforcer_threshold_0 = new_value
        self._bounds["short_risk_wel_enforcer_threshold"][0] = new_value
    @short_risk_wel_enforcer_threshold_1.setter
    def short_risk_wel_enforcer_threshold_1(self, new_value):
        self._short_risk_wel_enforcer_threshold_1 = new_value
        self._bounds["short_risk_wel_enforcer_threshold"][1] = new_value
    @short_risk_wel_enforcer_threshold_step.setter
    def short_risk_wel_enforcer_threshold_step(self, new_value):
        self._short_risk_wel_enforcer_threshold_step = new_value
        if len(self._bounds["short_risk_wel_enforcer_threshold"]) < 3:
            self._bounds["short_risk_wel_enforcer_threshold"].append(new_value)
        else:
            self._bounds["short_risk_wel_enforcer_threshold"][2] = new_value
    @short_risk_we_excess_allowance_pct_0.setter
    def short_risk_we_excess_allowance_pct_0(self, new_value):
        self._short_risk_we_excess_allowance_pct_0 = new_value
        self._bounds["short_risk_we_excess_allowance_pct"][0] = new_value
    @short_risk_we_excess_allowance_pct_1.setter
    def short_risk_we_excess_allowance_pct_1(self, new_value):
        self._short_risk_we_excess_allowance_pct_1 = new_value
        self._bounds["short_risk_we_excess_allowance_pct"][1] = new_value
    @short_risk_we_excess_allowance_pct_step.setter
    def short_risk_we_excess_allowance_pct_step(self, new_value):
        self._short_risk_we_excess_allowance_pct_step = new_value
        if len(self._bounds["short_risk_we_excess_allowance_pct"]) < 3:
            self._bounds["short_risk_we_excess_allowance_pct"].append(new_value)
        else:
            self._bounds["short_risk_we_excess_allowance_pct"][2] = new_value
    @short_risk_twel_enforcer_threshold_0.setter
    def short_risk_twel_enforcer_threshold_0(self, new_value):
        self._short_risk_twel_enforcer_threshold_0 = new_value
        self._bounds["short_risk_twel_enforcer_threshold"][0] = new_value
    @short_risk_twel_enforcer_threshold_1.setter
    def short_risk_twel_enforcer_threshold_1(self, new_value):
        self._short_risk_twel_enforcer_threshold_1 = new_value
        self._bounds["short_risk_twel_enforcer_threshold"][1] = new_value
    @short_risk_twel_enforcer_threshold_step.setter
    def short_risk_twel_enforcer_threshold_step(self, new_value):
        self._short_risk_twel_enforcer_threshold_step = new_value
        if len(self._bounds["short_risk_twel_enforcer_threshold"]) < 3:
            self._bounds["short_risk_twel_enforcer_threshold"].append(new_value)
        else:
            self._bounds["short_risk_twel_enforcer_threshold"][2] = new_value

class PBGui:
    def __init__(self):
        self._version = 0
        self._enabled_on = "disabled"
        self._only_cpt = False
        self._starting_config = False
        self._market_cap = 0
        self._vol_mcap = 10.0
        self._tags = []
        self._dynamic_ignore = False
        self._notices_ignore = False
        self._note = ''
        self._pbgui = {
            "version": self._version,
            "enabled_on": self._enabled_on,
            "only_cpt": self._only_cpt,
            "starting_config": self._starting_config,
            "market_cap": self._market_cap,
            "vol_mcap": self._vol_mcap,
            "tags": self._tags,
            "dynamic_ignore": self._dynamic_ignore,
            "notices_ignore": self._notices_ignore,
            "note": self._note,
        }
    
    def __repr__(self):
        return str(self._pbgui)
    
    @property
    def pbgui(self): return self._pbgui
    @pbgui.setter
    def pbgui(self, new_pbgui):
        if "version" in new_pbgui:
            self.version = new_pbgui["version"]
        if "enabled_on" in new_pbgui:
            self.enabled_on = new_pbgui["enabled_on"]
        if "only_cpt" in new_pbgui:
            self.only_cpt = new_pbgui["only_cpt"]
        if "starting_config" in new_pbgui:
            self.starting_config = new_pbgui["starting_config"]
        if "market_cap" in new_pbgui:
            self.market_cap = new_pbgui["market_cap"]
        if "vol_mcap" in new_pbgui:
            self.vol_mcap = new_pbgui["vol_mcap"]
        if "tags" in new_pbgui:
            self.tags = new_pbgui["tags"]
        if "dynamic_ignore" in new_pbgui:
            self.dynamic_ignore = new_pbgui["dynamic_ignore"]
        if "notices_ignore" in new_pbgui:
            self.notices_ignore = new_pbgui["notices_ignore"]
        if "note" in new_pbgui:
            self.note = new_pbgui["note"]

    @property
    def version(self): return self._version
    @property
    def enabled_on(self): return self._enabled_on
    @property
    def only_cpt(self): return self._only_cpt
    @property
    def starting_config(self): return self._starting_config
    @property
    def market_cap(self): return self._market_cap
    @property
    def vol_mcap(self): return self._vol_mcap
    @property
    def tags(self): return self._tags
    @property
    def dynamic_ignore(self): return self._dynamic_ignore
    @property
    def notices_ignore(self): return self._notices_ignore
    @property
    def note(self): return self._note

    @version.setter
    def version(self, new_version):
        self._version = new_version
        self._pbgui["version"] = self._version
    @enabled_on.setter
    def enabled_on(self, new_enabled_on):
        self._enabled_on = new_enabled_on
        self._pbgui["enabled_on"] = self._enabled_on
    @only_cpt.setter
    def only_cpt(self, new_only_cpt):
        self._only_cpt = new_only_cpt
        self._pbgui["only_cpt"] = self._only_cpt
    @starting_config.setter
    def starting_config(self, new_starting_config):
        self._starting_config = new_starting_config
        self._pbgui["starting_config"] = self._starting_config
    @market_cap.setter
    def market_cap(self, new_market_cap):
        self._market_cap = new_market_cap
        self._pbgui["market_cap"] = self._market_cap
    @vol_mcap.setter
    def vol_mcap(self, new_vol_mcap):
        self._vol_mcap = new_vol_mcap
        self._pbgui["vol_mcap"] = self._vol_mcap
    @tags.setter
    def tags(self, new_tags):
        self._tags = new_tags
        self._pbgui["tags"] = self._tags
    @dynamic_ignore.setter
    def dynamic_ignore(self, new_dynamic_ignore):
        self._dynamic_ignore = new_dynamic_ignore
        self._pbgui["dynamic_ignore"] = self._dynamic_ignore
    @notices_ignore.setter
    def notices_ignore(self, new_notices_ignore):
        self._notices_ignore = new_notices_ignore
        self._pbgui["notices_ignore"] = self._notices_ignore
    @note.setter
    def note(self, new_note):
        self._note = new_note
        self._pbgui["note"] = self._note

# ============================================================================
# ConfigV7Editor - Base class for Suite and coin_sources editing
# ============================================================================

class ConfigV7Editor:
    """
    Base class providing Suite and coin_sources UI editing functionality.
    Used by BacktestV7Item and OptimizeV7Item for consistent config editing.
    
    Requirements for subclasses:
    - self.config must be a ConfigV7 instance
    - self._get_available_symbols(exchanges=None) must be implemented
    - self._get_key_prefix() must be implemented (returns "bt_" or "opt_")
    """
    
    # ============ ABSTRACT METHODS ============
    
    def _get_key_prefix(self):
        """
        Return key prefix for streamlit widgets.
        Must be implemented by subclass.
        
        Returns:
            str: "bt_" for BacktestV7, "opt_" for OptimizeV7
        """
        raise NotImplementedError("Subclass must implement _get_key_prefix()")
    
    def _get_available_symbols(self, exchanges=None):
        """
        Get available symbols from coindata for specified exchanges.
        Must be implemented by subclass.
        
        Args:
            exchanges: List of exchange names. If None, uses config.backtest.exchanges
            
        Returns:
            List of normalized coin names (e.g., ["BTC", "ETH", "SOL"])
        """
        raise NotImplementedError("Subclass must implement _get_available_symbols()")
    
    # ============ COIN SOURCES ============
    
    def _get_exchanges_for_coin(self, coin: str, available_exchanges: list) -> list:
        """Get list of exchanges that have the specified coin.
        
        Args:
            coin: Coin symbol (e.g., "BTC")
            available_exchanges: List of exchanges to check
            
        Returns:
            List of exchanges that have this coin
        """
        exchanges_with_coin = []
        for exchange in available_exchanges:
            if f"coindata_{exchange}" in st.session_state:
                symbols = st.session_state[f"coindata_{exchange}"].symbols
                # Normalize and check if coin exists
                normalized = [normalize_symbol(s) for s in symbols]
                if coin in normalized:
                    exchanges_with_coin.append(exchange)
        return exchanges_with_coin
    
    def _edit_coin_sources_ui(self, coin_sources_dict: dict, available_exchanges: list, key_prefix: str = "", save_callback=None, current_exchanges: list = None, all_suite_coin_sources: dict = None):
        """
        UI for editing coin_sources with read-only data_editor and add section.
        
        Args:
            coin_sources_dict: {"BTC": "binance", "SOL": "bybit"}
            available_exchanges: ["binance", "bybit", ...]
            key_prefix: Unique prefix for widget keys (e.g., "bt_", "scenario_")
            save_callback: Function to call when changes are made (receives updated dict)
            current_exchanges: List of currently selected exchanges (for context)
            all_suite_coin_sources: Merged coin_sources from all scenarios (to prevent conflicts)
        
        Returns:
            Updated coin_sources_dict
        """
        if current_exchanges is None:
            current_exchanges = available_exchanges
        if all_suite_coin_sources is None:
            all_suite_coin_sources = {}
        import pandas as pd
        
        # Expander always visible, expanded only if coin_sources configured
        has_sources = bool(coin_sources_dict)
        expander_title = f"**Coin Sources** ({len(coin_sources_dict)} configured)" if has_sources else "**Coin Sources**"
        
        with st.expander(expander_title, expanded=has_sources):
            st.caption("Override automatic exchange selection for specific coins")
            
            # Display existing mappings in read-only data_editor with delete checkbox
            if coin_sources_dict:
                # Build DataFrame from current dict
                rows = []
                for coin, exchange in sorted(coin_sources_dict.items()):
                    rows.append({
                        "Delete": False,
                        "Coin": coin,
                        "Exchange": exchange
                    })
                df = pd.DataFrame(rows)
                
                # Display as read-only table with only Delete column editable
                edited_df = st.data_editor(
                    df,
                    width="stretch",
                    num_rows="fixed",
                    hide_index=True,
                    column_config={
                        "Delete": st.column_config.CheckboxColumn(
                            "Delete",
                            help=pbgui_help.coin_sources_delete,
                            default=False
                        ),
                        "Coin": st.column_config.TextColumn(
                            "Coin",
                            disabled=True,
                            help=pbgui_help.coin_sources_coin
                        ),
                        "Exchange": st.column_config.TextColumn(
                            "Exchange",
                            disabled=True,
                            help=pbgui_help.coin_sources_exchange
                        )
                    },
                    key=f"{key_prefix}coin_sources_table"
                )
                
                # Process deletions
                coins_to_delete = []
                for _, row in edited_df.iterrows():
                    if row["Delete"]:
                        coins_to_delete.append(row["Coin"])
                
                if coins_to_delete:
                    for coin in coins_to_delete:
                        if coin in coin_sources_dict:
                            del coin_sources_dict[coin]
                    if save_callback:
                        save_callback(coin_sources_dict)
                    st.rerun()
                
            # Add new mapping section
            st.caption("Add new coin source mapping:")
            
            col1, col2, col3 = st.columns([1, 1, 2], vertical_alignment="bottom")
            
            with col1:
                # Step 1: Select exchange - show ALL available exchanges
                all_exchanges = V7.list()  # Always show all exchanges
                selected_exchange = st.selectbox(
                    "Exchange",
                    options=all_exchanges,
                    key=f"{key_prefix}new_coin_source_exchange",
                    help=pbgui_help.coin_sources_select_exchange
                )
            
            with col2:
                # Step 2: Get coins for selected exchange (filtered)
                if selected_exchange:
                    available_coins = self._get_available_symbols([selected_exchange])
                    # Filter: coins already configured in THIS coin_sources (no duplicates within same context)
                    available_coins = [c for c in available_coins if c not in coin_sources_dict]
                    
                    # CRITICAL: Filter coins that exist in suite with DIFFERENT exchange
                    # Passivbot merges all coin_sources and rejects conflicts
                    # Allow coin if: not in suite, OR same exchange
                    # Conflicted coins (None value) are excluded automatically
                    if all_suite_coin_sources:
                        available_coins = [c for c in available_coins 
                                         if c not in all_suite_coin_sources 
                                         or all_suite_coin_sources[c] == selected_exchange]
                    
                    if available_coins:
                        selected_coin = st.selectbox(
                            "Coin",
                            options=available_coins,
                            key=f"{key_prefix}new_coin_source_coin",
                            help=pbgui_help.coin_sources_select_coin
                        )
                    else:
                        st.info(f"All coins from {selected_exchange} are already configured or would conflict with other scenarios")
                        selected_coin = None
                else:
                    st.info("Select exchange first")
                    selected_coin = None
            
            with col3:
                if st.button("➕", key=f"{key_prefix}add_new_coin_source", 
                            disabled=not (selected_exchange and selected_coin),
                            help=pbgui_help.add_coin_source_button):
                    if selected_coin not in coin_sources_dict:
                        coin_sources_dict[selected_coin] = selected_exchange
                        if save_callback:
                            save_callback(coin_sources_dict)
                        st.rerun()
                    else:
                        st.warning(f"{selected_coin} already mapped")
        
        return coin_sources_dict
    
    @st.fragment
    def fragment_coin_sources(self):
        """Fragment for coin_sources with suite conflict detection."""
        # Collect all scenario coin_sources to prevent conflicts
        all_suite_sources = {}
        if self.config.backtest.suite:
            for scenario in self.config.backtest.suite.scenarios:
                if scenario.coin_sources:
                    for coin, exchange in scenario.coin_sources.items():
                        if coin in all_suite_sources and all_suite_sources[coin] != exchange:
                            all_suite_sources[coin] = None
                        elif coin not in all_suite_sources:
                            all_suite_sources[coin] = exchange
        
        self._edit_coin_sources_ui(
            self.config.backtest.coin_sources,
            self.config.backtest.exchanges if self.config.backtest.exchanges else V7.list(),
            key_prefix=self._get_key_prefix(),
            save_callback=lambda cs: setattr(self.config.backtest, 'coin_sources', cs),
            current_exchanges=self.config.backtest.exchanges if self.config.backtest.exchanges else V7.list(),
            all_suite_coin_sources=all_suite_sources
        )
    
    # ============ SUITE CONFIGURATION ============
    
    def _get_override_parameters(self):
        """Get list of bot parameters that can be overridden in scenarios."""
        return ALLOWED_OVERRIDES
    
    def _get_aggregate_metrics(self):
        """Get list of metrics that can have custom aggregation."""
        return get_aggregate_metrics()
    
    def _edit_aggregate_ui(self, suite):
        """UI for editing metric-specific aggregation rules (limits-style)."""
        key_prefix = self._get_key_prefix()
        suite_key_ver = int(st.session_state.get(f"{key_prefix}suite_key_ver", 0) or 0)

        aggregate_options = ["mean", "min", "max", "std", "median"]
        agg_currency_options = ["both", "usd", "btc"]
        type_options = ["all"] + list(get_metric_groups())

        def split_agg_metric_key(metric_key: str) -> tuple[str, str]:
            """Return (base_metric, currency_sel) where currency_sel in {both, usd, btc}.

            For currency metrics:
            - base key (no suffix) => currency_sel == both
            - suffix key (e.g. *_usd) => currency_sel == usd/btc
            """
            if not isinstance(metric_key, str):
                return str(metric_key), "both"

            for prefix, currency in (("usd_", "usd"), ("btc_", "btc")):
                if metric_key.startswith(prefix):
                    core = metric_key[len(prefix):]
                    if core in SHARED_METRICS:
                        return core, "both"
                    return core, currency

            for suffix, currency in (("_usd", "usd"), ("_btc", "btc")):
                if metric_key.endswith(suffix):
                    core = metric_key[:-len(suffix)]
                    return core, currency

            base = metric_key
            if is_currency_metric(base):
                return base, "both"
            return base, "both"

        def build_agg_metric_key(base_metric: str, currency_sel: str) -> str:
            if currency_sel in ("usd", "btc") and is_currency_metric(base_metric):
                return f"{base_metric}_{currency_sel}"
            return base_metric

        # Keys for this suite editor version
        agg_ed_key_name = f"{key_prefix}suite_agg_ed_key_{suite_key_ver}"
        edit_key_name = f"{key_prefix}edit_suite_agg_metric_{suite_key_ver}"
        if agg_ed_key_name not in st.session_state:
            st.session_state[agg_ed_key_name] = 0

        current_aggregates = {
            k: v
            for k, v in getattr(suite, "aggregate", {}).items()
            if isinstance(k, str) and k != "default"
        }

        st.caption("Metric-specific aggregation rules:")

        edit_mode = st.session_state.get(edit_key_name) is not None

        if not edit_mode:
            # Display table (edit/delete)
            d_aggregates: list[dict[str, object]] = []
            for metric_key in sorted(current_aggregates.keys()):
                d_aggregates.append(
                    {
                        "metric": metric_key,
                        "aggregation": str(current_aggregates.get(metric_key, "")),
                        "edit": False,
                        "delete": False,
                    }
                )

            ed_key = st.session_state[agg_ed_key_name]
            table_key = f"{key_prefix}suite_agg_table_{suite_key_ver}_{ed_key}"
            if table_key in st.session_state:
                ed = st.session_state[table_key]
                for row in ed.get("edited_rows", {}):
                    if d_aggregates and 0 <= row < len(d_aggregates):
                        if ed["edited_rows"][row].get("delete"):
                            metric_to_delete = str(d_aggregates[row]["metric"])
                            updated = dict(suite.aggregate)
                            if metric_to_delete in updated:
                                del updated[metric_to_delete]
                                suite.aggregate = updated
                                self.config.backtest.suite = suite
                                st.session_state[agg_ed_key_name] += 1
                                st.rerun()
                        if ed["edited_rows"][row].get("edit"):
                            st.session_state[edit_key_name] = str(d_aggregates[row]["metric"])
                            # Refresh the table key so data_editor state can't keep retriggering.
                            st.session_state[agg_ed_key_name] += 1
                            st.rerun()

            if d_aggregates:
                column_config = {
                    "metric": st.column_config.TextColumn("Metric", width="medium"),
                    "aggregation": st.column_config.TextColumn("Aggregation", width="small"),
                    "edit": st.column_config.CheckboxColumn("Edit", width="small"),
                    "delete": st.column_config.CheckboxColumn("Del", width="small"),
                }
                st.data_editor(
                    data=d_aggregates,
                    height=36 + (len(d_aggregates)) * 35,
                    key=table_key,
                    disabled=["metric", "aggregation"],
                    column_config=column_config,
                    hide_index=True,
                    width="stretch",
                )
            else:
                st.info("No metric-specific aggregation rules. Add one below if needed.")

        # Edit existing rule
        if st.session_state.get(edit_key_name):
            old_metric_key = str(st.session_state.get(edit_key_name))
            if old_metric_key not in suite.aggregate:
                st.session_state[edit_key_name] = None
                st.rerun()

            st.subheader("Edit Aggregate")

            old_base, old_currency_sel = split_agg_metric_key(old_metric_key)
            old_method = str(suite.aggregate.get(old_metric_key, "mean") or "mean").lower()
            if old_method not in aggregate_options:
                old_method = "mean"

            default_type = get_metric_group(old_base) or "all"
            edit_type_key = f"{key_prefix}suite_agg_edit_type_{suite_key_ver}"
            if edit_type_key not in st.session_state:
                st.session_state[edit_type_key] = default_type

            selected_type = st.selectbox(
                "Type",
                type_options,
                index=type_options.index(st.session_state.get(edit_type_key, "all"))
                if st.session_state.get(edit_type_key, "all") in type_options
                else 0,
                key=edit_type_key,
                help=get_limits_type_help_text(),
            )

            if selected_type == "all":
                filtered_base_metrics = get_all_metrics_list()
            else:
                filtered_base_metrics = get_metrics_by_group(selected_type, include_weighted=True)
                if not filtered_base_metrics:
                    filtered_base_metrics = get_all_metrics_list()

            edit_metric_key = f"{key_prefix}suite_agg_edit_metric_{suite_key_ver}"
            if edit_metric_key not in st.session_state:
                st.session_state[edit_metric_key] = old_base
            if st.session_state.get(edit_metric_key) not in filtered_base_metrics:
                st.session_state[edit_metric_key] = filtered_base_metrics[0]

            new_base = st.selectbox(
                "Metric",
                filtered_base_metrics,
                index=filtered_base_metrics.index(st.session_state[edit_metric_key]),
                key=edit_metric_key,
                help=get_limits_metric_list_help_text(selected_type, include_weighted=True),
            )

            new_is_currency = is_currency_metric(new_base)
            edit_currency_key = f"{key_prefix}suite_agg_edit_currency_{suite_key_ver}"
            if edit_currency_key not in st.session_state:
                st.session_state[edit_currency_key] = old_currency_sel

            if new_is_currency:
                new_currency_sel = st.selectbox(
                    "Currency",
                    agg_currency_options,
                    index=agg_currency_options.index(st.session_state.get(edit_currency_key, "both"))
                    if st.session_state.get(edit_currency_key, "both") in agg_currency_options
                    else 0,
                    key=edit_currency_key,
                    help="both stores the base key (applies to _usd and _btc); usd/btc store suffixed keys.",
                )
            else:
                st.write("")
                new_currency_sel = "both"

            edit_method_key = f"{key_prefix}suite_agg_edit_method_{suite_key_ver}"
            if edit_method_key not in st.session_state:
                st.session_state[edit_method_key] = old_method
            new_method = st.selectbox(
                "Aggregation",
                aggregate_options,
                index=aggregate_options.index(st.session_state.get(edit_method_key, "mean"))
                if st.session_state.get(edit_method_key, "mean") in aggregate_options
                else 0,
                key=edit_method_key,
                help=pbgui_help.suite_add_aggregation,
            )

            new_metric_key = build_agg_metric_key(new_base, new_currency_sel)

            col_ok, col_cancel, col_del = st.columns([1, 1, 1])
            with col_ok:
                if st.button("OK", key=f"{key_prefix}suite_agg_edit_ok_{suite_key_ver}"):
                    updated = dict(suite.aggregate)
                    if old_metric_key != new_metric_key and old_metric_key in updated:
                        del updated[old_metric_key]
                    updated[new_metric_key] = new_method
                    suite.aggregate = updated
                    self.config.backtest.suite = suite
                    st.session_state[edit_key_name] = None
                    st.session_state[agg_ed_key_name] += 1
                    st.rerun()
            with col_cancel:
                if st.button("Cancel", key=f"{key_prefix}suite_agg_edit_cancel_{suite_key_ver}"):
                    st.session_state[edit_key_name] = None
                    st.rerun()
            with col_del:
                if st.button("Delete", key=f"{key_prefix}suite_agg_edit_delete_{suite_key_ver}"):
                    updated = dict(suite.aggregate)
                    if old_metric_key in updated:
                        del updated[old_metric_key]
                    suite.aggregate = updated
                    self.config.backtest.suite = suite
                    st.session_state[edit_key_name] = None
                    st.session_state[agg_ed_key_name] += 1
                    st.rerun()

            return

        # Add new rule
        st.subheader("Add Metric")
        add_type_key = f"{key_prefix}suite_agg_add_type_{suite_key_ver}"
        if add_type_key not in st.session_state:
            st.session_state[add_type_key] = "all"

        col_type, col_metric, col_curr, col_method, col_btn = st.columns(
            [0.9, 1.5, 0.7, 1.0, 0.6],
            vertical_alignment="bottom",
        )
        with col_type:
            selected_type = st.selectbox(
                "Type",
                type_options,
                index=type_options.index(st.session_state.get(add_type_key, "all"))
                if st.session_state.get(add_type_key, "all") in type_options
                else 0,
                key=add_type_key,
                help=get_limits_type_help_text(),
            )

        if selected_type == "all":
            filtered_base_metrics = get_all_metrics_list()
        else:
            filtered_base_metrics = get_metrics_by_group(selected_type, include_weighted=True)
            if not filtered_base_metrics:
                filtered_base_metrics = get_all_metrics_list()

        add_metric_key = f"{key_prefix}suite_agg_add_metric_{suite_key_ver}"
        if add_metric_key not in st.session_state or st.session_state.get(add_metric_key) not in filtered_base_metrics:
            st.session_state[add_metric_key] = filtered_base_metrics[0]

        with col_metric:
            new_base = st.selectbox(
                "Metric",
                filtered_base_metrics,
                index=filtered_base_metrics.index(st.session_state[add_metric_key]),
                key=add_metric_key,
                help=get_limits_metric_list_help_text(selected_type, include_weighted=True),
            )

        is_curr = is_currency_metric(new_base)
        add_currency_key = f"{key_prefix}suite_agg_add_currency_{suite_key_ver}"
        if add_currency_key not in st.session_state:
            st.session_state[add_currency_key] = "both"

        with col_curr:
            if is_curr:
                currency_sel = st.selectbox(
                    "Currency",
                    agg_currency_options,
                    index=agg_currency_options.index(st.session_state.get(add_currency_key, "both"))
                    if st.session_state.get(add_currency_key, "both") in agg_currency_options
                    else 0,
                    key=add_currency_key,
                    help="both stores the base key (applies to _usd and _btc); usd/btc store suffixed keys.",
                )
            else:
                st.write("")
                currency_sel = "both"

        add_method_key = f"{key_prefix}suite_agg_add_method_{suite_key_ver}"
        if add_method_key not in st.session_state:
            st.session_state[add_method_key] = "mean"
        with col_method:
            method = st.selectbox(
                "Aggregation",
                aggregate_options,
                index=aggregate_options.index(st.session_state.get(add_method_key, "mean"))
                if st.session_state.get(add_method_key, "mean") in aggregate_options
                else 0,
                key=add_method_key,
                help=pbgui_help.suite_add_aggregation,
            )

        with col_btn:
            if st.button("➕", key=f"{key_prefix}suite_agg_add_btn_{suite_key_ver}", help=pbgui_help.suite_add_button):
                metric_key = build_agg_metric_key(new_base, currency_sel)
                updated = dict(suite.aggregate)
                updated[metric_key] = method
                suite.aggregate = updated
                self.config.backtest.suite = suite
                st.session_state[agg_ed_key_name] += 1
                st.rerun()
    
    @st.fragment
    def _edit_scenario_start_date(self, scenario, suite, idx):
        """Fragment for editing scenario start date."""
        # Get fresh scenario reference to ensure we have latest data
        scenario = suite.get_scenario(idx)
        if not scenario:
            return None
        
        # Initialize counter for forcing widget refresh
        if "start_date_counter" not in st.session_state:
            st.session_state.start_date_counter = 0
            
        # Prepare default value
        if scenario.start_date:
            try:
                default_start = datetime.datetime.strptime(scenario.start_date[:10], '%Y-%m-%d').date()
            except:
                default_start = None
        else:
            default_start = None
        
        subcol1, subcol2 = st.columns([4, 1], vertical_alignment="bottom")
        with subcol1:
            new_start_date = st.date_input(
                "Start Date (empty = base config)", 
                value=default_start, 
                format="YYYY-MM-DD", 
                key=f"edit_scenario_start_{st.session_state.start_date_counter}",
                help=pbgui_help.scenario_start_date
            )
        with subcol2:
            # Always show button, but check the actual scenario data
            if st.button("🗑️", key="clear_start_date", help=pbgui_help.scenario_clear_date, disabled=(scenario.start_date is None)):
                scenario.start_date = None
                suite.update_scenario(idx, scenario)
                self.config.backtest.suite = suite
                # Increment counter to force widget recreation
                st.session_state.start_date_counter += 1
                st.rerun()
        
        return new_start_date

    @st.fragment
    def _edit_scenario_end_date(self, scenario, suite, idx):
        """Fragment for editing scenario end date."""
        # Get fresh scenario reference to ensure we have latest data
        scenario = suite.get_scenario(idx)
        if not scenario:
            return None
        
        # Initialize counter for forcing widget refresh
        if "end_date_counter" not in st.session_state:
            st.session_state.end_date_counter = 0
            
        # Prepare default value
        if scenario.end_date:
            try:
                default_end = datetime.datetime.strptime(scenario.end_date[:10], '%Y-%m-%d').date()
            except:
                default_end = None
        else:
            default_end = None
        
        subcol1, subcol2 = st.columns([4, 1], vertical_alignment="bottom")
        with subcol1:
            new_end_date = st.date_input(
                "End Date (empty = base config)", 
                value=default_end, 
                format="YYYY-MM-DD", 
                key=f"edit_scenario_end_{st.session_state.end_date_counter}",
                help=pbgui_help.scenario_end_date
            )
        with subcol2:
            # Always show button, but check the actual scenario data
            if st.button("🗑️", key="clear_end_date", help=pbgui_help.scenario_clear_date, disabled=(scenario.end_date is None)):
                scenario.end_date = None
                suite.update_scenario(idx, scenario)
                self.config.backtest.suite = suite
                # Increment counter to force widget recreation
                st.session_state.end_date_counter += 1
                st.rerun()
        
        return new_end_date

    def _edit_scenario_ui(self, suite):
        """UI for editing an existing scenario."""
        key_prefix = self._get_key_prefix()
        suite_key_ver = int(st.session_state.get(f"{key_prefix}suite_key_ver", 0) or 0)
        idx_key = f"{key_prefix}edit_scenario_idx_{suite_key_ver}"
        suite_ed_key_name = f"{key_prefix}suite_ed_key_{suite_key_ver}"
        idx = st.session_state.get(idx_key)
        if idx is None:
            return
        scenario = suite.get_scenario(idx)
        if not scenario:
            st.session_state[idx_key] = None
            st.rerun()
            return
        
        st.subheader(f"Edit Scenario: {scenario.label}")
        
        # Label, Exchanges, and Date range on one line
        col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
        with col1:
            new_label = st.text_input("Label", value=scenario.label, key="edit_scenario_label", help=pbgui_help.scenario_label)
        with col2:
            available_exchanges = V7.list()
            current_exchanges = scenario.exchanges if scenario.exchanges else []
            new_exchanges = st.multiselect("Exchanges (leave empty for base)", available_exchanges, default=current_exchanges, key="edit_scenario_exchanges", help=pbgui_help.scenario_exchanges)
        with col3:
            new_start_date = self._edit_scenario_start_date(scenario, suite, idx)
        with col4:
            new_end_date = self._edit_scenario_end_date(scenario, suite, idx)
        
        # Get symbols based on selected exchanges (or base config exchanges if none selected)
        exchanges_for_symbols = new_exchanges if new_exchanges else None
        symbols = self._get_available_symbols(exchanges_for_symbols)
        
        # Coins - Multi-Select
        current_coins = scenario.coins if scenario.coins else []
        # Filter out coins not in symbols list
        valid_coins = [c for c in current_coins if c in symbols]
        col1, col2 = st.columns(2)
        with col1:
            new_coins = st.multiselect("Coins (leave empty for base)", symbols, default=valid_coins, key="edit_scenario_coins", help=pbgui_help.scenario_coins)
        with col2:
            current_ignored = scenario.ignored_coins if scenario.ignored_coins else []
            valid_ignored = [c for c in current_ignored if c in symbols]
            new_ignored = st.multiselect("Ignored Coins", symbols, default=valid_ignored, key="edit_scenario_ignored", help=pbgui_help.scenario_ignored_coins)
        
        # Coin Sources
        exchanges_for_sources = new_exchanges if new_exchanges else available_exchanges
        
        # Collect all suite coin_sources EXCEPT this scenario to check for conflicts
        # Like Passivbot's collect_suite_coin_sources(), detect conflicts
        all_suite_sources = {}
        # Add base coin_sources
        if self.config.backtest.coin_sources:
            all_suite_sources.update(self.config.backtest.coin_sources)
        # Add all other scenarios' coin_sources (exclude current)
        for i, s in enumerate(suite.scenarios):
            if i != idx and s.coin_sources:  # Exclude current scenario
                for coin, exchange in s.coin_sources.items():
                    if coin in all_suite_sources and all_suite_sources[coin] != exchange:
                        # Conflict detected - mark as conflicted
                        all_suite_sources[coin] = None
                    elif coin not in all_suite_sources:
                        all_suite_sources[coin] = exchange
        
        self._edit_coin_sources_ui(
            scenario.coin_sources if scenario.coin_sources else {},
            exchanges_for_sources,
            key_prefix="scenario_",
            save_callback=lambda cs: setattr(scenario, 'coin_sources', cs),
            current_exchanges=new_exchanges if new_exchanges else available_exchanges,
            all_suite_coin_sources=all_suite_sources
        )
        
        # Overrides - GUI-based
        st.write("**Parameter Overrides**")
        override_params = self._get_override_parameters()
        sides = ["long", "short"]
        
        # Initialize overrides editor key
        if "edit_scenario_overrides_ed_key" not in st.session_state:
            st.session_state.edit_scenario_overrides_ed_key = 0
        
        # Build display data from scenario overrides
        d_overrides = []
        if scenario.overrides:
            for key, value in scenario.overrides.items():
                parts = key.split(".")
                if len(parts) == 3 and parts[0] == "bot" and parts[1] in sides:
                    d_overrides.append({
                        "side": parts[1],
                        "parameter": parts[2],
                        "value": float(value) if isinstance(value, (int, float)) else 0.0,
                        "delete": False
                    })
        
        ed_key = st.session_state.edit_scenario_overrides_ed_key
        
        if d_overrides:
            # Handle data_editor events
            if f'select_overrides_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_overrides_{ed_key}']
                changes_made = False
                new_overrides = {}
                
                for row_idx, override_data in enumerate(d_overrides):
                    # Check if this row was deleted
                    if row_idx in ed.get("edited_rows", {}) and ed["edited_rows"][row_idx].get("delete"):
                        changes_made = True
                        continue
                    
                    # Get edited or original values
                    side = ed.get("edited_rows", {}).get(row_idx, {}).get("side", override_data["side"])
                    param = ed.get("edited_rows", {}).get(row_idx, {}).get("parameter", override_data["parameter"])
                    value = ed.get("edited_rows", {}).get(row_idx, {}).get("value", override_data["value"])
                    
                    # Check if anything changed
                    if (side != override_data["side"] or param != override_data["parameter"] or value != override_data["value"]):
                        changes_made = True
                    
                    # Convert to int if whole number
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    key = f"bot.{side}.{param}"
                    new_overrides[key] = value
                
                if changes_made:
                    scenario.overrides = new_overrides
                    st.session_state.edit_scenario_overrides_ed_key += 1
                    st.rerun()
            
            # Display overrides table
            column_config = {
                "side": st.column_config.SelectboxColumn("Side", options=sides, required=True),
                "parameter": st.column_config.SelectboxColumn("Parameter", options=override_params, required=True, width="large"),
                "value": st.column_config.NumberColumn("Value", format="%.6f"),
                "delete": st.column_config.CheckboxColumn("Del", width="small"),
            }
            st.data_editor(d_overrides, column_config=column_config, hide_index=True, key=f'select_overrides_{ed_key}', width="stretch")
        
        # Add new override with selection
        col1, col2, col3, col4 = st.columns([2, 4, 1, 1], vertical_alignment="bottom")
        with col1:
            new_side = st.selectbox("Side", sides, key="edit_scenario_add_override_side", help=pbgui_help.scenario_override_side)
        with col2:
            new_param = st.selectbox("Parameter", override_params, key="edit_scenario_add_override_param", help=pbgui_help.scenario_override_param)
        with col3:
            new_value = st.number_input("Value", value=0.0, format="%.6f", key="edit_scenario_add_override_value", help=pbgui_help.scenario_override_value)
        with col4:
            if st.button("➕", key="edit_scenario_add_override", help=pbgui_help.add_scenario_override_button):
                new_overrides = dict(scenario.overrides) if scenario.overrides else {}
                new_key = f"bot.{new_side}.{new_param}"
                # Convert to int if whole number
                if isinstance(new_value, float) and new_value.is_integer():
                    new_value = int(new_value)
                new_overrides[new_key] = new_value
                scenario.overrides = new_overrides
                st.session_state.edit_scenario_overrides_ed_key += 1
                st.rerun()
        
        # Buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("OK", key="edit_scenario_ok"):
                # Save
                scenario.label = new_label.strip()
                scenario.coins = new_coins if new_coins else []
                scenario.ignored_coins = new_ignored if new_ignored else []
                scenario.start_date = new_start_date.strftime("%Y-%m-%d") if new_start_date else None
                scenario.end_date = new_end_date.strftime("%Y-%m-%d") if new_end_date else None
                scenario.exchanges = new_exchanges if new_exchanges else None
                # Overrides are already saved in the table editor
                # Just make sure the suite is updated
                
                suite.update_scenario(idx, scenario)
                # Trigger setter to update _backtest dict (like limits pattern)
                self.config.backtest.suite = suite
                # Clean up session state
                st.session_state[idx_key] = None
                if suite_ed_key_name in st.session_state:
                    st.session_state[suite_ed_key_name] += 1
                st.rerun()
        with col2:
            if st.button("Cancel", key="edit_scenario_cancel"):
                st.session_state[idx_key] = None
                st.rerun()
        with col3:
            if st.button("Delete", key="edit_scenario_delete"):
                suite.remove_scenario(idx)
                # Trigger setter to update _backtest dict (like limits pattern)
                self.config.backtest.suite = suite
                st.session_state[idx_key] = None
                if suite_ed_key_name in st.session_state:
                    st.session_state[suite_ed_key_name] += 1
                st.rerun()

    def _add_scenario_ui(self, suite, suite_ed_key_name):
        """UI for adding a new scenario."""
        st.subheader("Add Scenario")
        
        col1, col2, col3 = st.columns([1, 2, 1], vertical_alignment="bottom")
        with col1:
            new_label = st.text_input("Label", key="add_scenario_label", help=pbgui_help.scenario_label, placeholder="e.g., bull_market_2024")
        with col2:
            # Get symbols based on base config exchanges (since no exchanges selected yet in new scenario)
            symbols = self._get_available_symbols()
            new_coins = st.multiselect("Coins (optional)", symbols, key="add_scenario_coins", help=pbgui_help.scenario_coins)
        with col3:
            if st.button("➕", key="add_scenario_button", help=pbgui_help.add_scenario_button):
                if new_label.strip():
                    new_scenario = {
                        "label": new_label.strip(),
                        "coins": new_coins if new_coins else []
                    }
                    suite.add_scenario(new_scenario)
                    # Trigger setter to update _backtest dict (like limits pattern)
                    self.config.backtest.suite = suite
                    if suite_ed_key_name not in st.session_state:
                        st.session_state[suite_ed_key_name] = 0
                    st.session_state[suite_ed_key_name] += 1
                    # Clear add fields
                    if "add_scenario_label" in st.session_state:
                        del st.session_state["add_scenario_label"]
                    if "add_scenario_coins" in st.session_state:
                        del st.session_state["add_scenario_coins"]
                    st.rerun()
                else:
                    error_popup("Label is required")

    @st.fragment
    def fragment_suite(self):
        """UI for configuring multi-scenario suite for backtesting/optimization."""
        suite = self.config.backtest.suite
        has_scenarios = bool(suite.scenarios)
        key_prefix = self._get_key_prefix()
        suite_key_ver_name = f"{key_prefix}suite_key_ver"
        if suite_key_ver_name not in st.session_state:
            st.session_state[suite_key_ver_name] = 0
        suite_key_ver = int(st.session_state.get(suite_key_ver_name, 0) or 0)
        
        with st.expander("Suite Configuration", expanded=suite.enabled or has_scenarios):
            # Init session state
            suite_ed_key_name = f"{key_prefix}suite_ed_key_{suite_key_ver}"
            edit_scenario_idx_name = f"{key_prefix}edit_scenario_idx_{suite_key_ver}"
            if suite_ed_key_name not in st.session_state:
                st.session_state[suite_ed_key_name] = 0
            
            # Main suite controls
            col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
            with col1:
                new_enabled = st.checkbox("Enable Suite", value=suite.enabled, key=f"{key_prefix}suite_enabled_{suite_key_ver}", help=pbgui_help.suite_enabled)
                if new_enabled != suite.enabled:
                    suite.enabled = new_enabled
                    self.config.backtest.suite = suite
            with col2:
                new_include_base = st.checkbox("Include Base Scenario", value=suite.include_base_scenario, key=f"{key_prefix}suite_include_base_{suite_key_ver}", help=pbgui_help.suite_include_base_scenario)
                if new_include_base != suite.include_base_scenario:
                    suite.include_base_scenario = new_include_base
                    self.config.backtest.suite = suite
            with col3:
                new_base_label = st.text_input("Base Label", value=suite.base_label, key=f"{key_prefix}suite_base_label_{suite_key_ver}", help=pbgui_help.suite_base_label)
                if new_base_label != suite.base_label:
                    suite.base_label = new_base_label
                    self.config.backtest.suite = suite
            with col4:
                aggregate_options = ["mean", "min", "max", "std", "median"]
                current_default = suite.aggregate.get("default", "mean")
                new_default = st.selectbox("Default Aggregate", aggregate_options, index=aggregate_options.index(current_default) if current_default in aggregate_options else 0, key=f"{key_prefix}suite_aggregate_default_{suite_key_ver}", help=pbgui_help.suite_aggregate)
                if new_default != current_default:
                    new_agg = dict(suite.aggregate)
                    new_agg["default"] = new_default
                    suite.aggregate = new_agg
                    self.config.backtest.suite = suite
            
            # Metric-specific aggregation rules
            self._edit_aggregate_ui(suite)
            
            # Scenario editing mode check
            if st.session_state.get(edit_scenario_idx_name) is not None:
                self._edit_scenario_ui(suite)
                return
            
            # Scenarios table
            st.subheader("Scenarios")
            if has_scenarios:
                # Build display data
                d_scenarios = []
                for i, scenario in enumerate(suite.scenarios):
                    coins_display = f"{len(scenario.coins)} coins" if scenario.coins else "base"
                    dates_display = f"{scenario.start_date or 'base'} → {scenario.end_date or 'base'}"
                    if scenario.start_date is None and scenario.end_date is None:
                        dates_display = "base"
                    exchanges_display = ", ".join(scenario.exchanges) if scenario.exchanges else "base"
                    coin_sources_display = f"{len(scenario.coin_sources)} sources" if scenario.coin_sources else "-"
                    overrides_display = f"{len(scenario.overrides)} overrides" if scenario.overrides else "-"
                    d_scenarios.append({
                        "label": scenario.label,
                        "coins": coins_display,
                        "dates": dates_display,
                        "exchanges": exchanges_display,
                        "coin_sources": coin_sources_display,
                        "overrides": overrides_display,
                        "edit": False,
                        "delete": False
                    })
                
                ed_key = st.session_state[suite_ed_key_name]
                
                # Handle data_editor events
                scenarios_table_key = f"{key_prefix}select_scenarios_{suite_key_ver}_{ed_key}"
                if scenarios_table_key in st.session_state:
                    ed = st.session_state[scenarios_table_key]
                    for row in ed.get("edited_rows", {}):
                        if ed["edited_rows"][row].get("delete"):
                            suite.remove_scenario(row)
                            # Trigger setter to update _backtest dict (like limits pattern)
                            self.config.backtest.suite = suite
                            st.session_state[suite_ed_key_name] += 1
                            st.rerun()
                        if ed["edited_rows"][row].get("edit"):
                            st.session_state[edit_scenario_idx_name] = row
                            st.rerun()
                
                # Display scenarios table
                column_config = {
                    "label": st.column_config.TextColumn("Label", width="medium"),
                    "coins": st.column_config.TextColumn("Coins", width="small"),
                    "dates": st.column_config.TextColumn("Date Range", width="medium"),
                    "exchanges": st.column_config.TextColumn("Exchanges", width="small"),
                    "coin_sources": st.column_config.TextColumn("Coin Sources", width="small"),
                    "overrides": st.column_config.TextColumn("Overrides", width="small"),
                    "edit": st.column_config.CheckboxColumn("Edit", width="small"),
                    "delete": st.column_config.CheckboxColumn("Del", width="small"),
                }
                st.data_editor(d_scenarios, column_config=column_config, hide_index=True, key=f"{key_prefix}select_scenarios_{suite_key_ver}_{ed_key}", width="stretch")
            else:
                st.info("No scenarios configured. Add a scenario below to test your config across different coin sets, date ranges, or parameter variations.")

            # Add new scenario UI
            self._add_scenario_ui(suite, suite_ed_key_name)

            # Display suite preflight warnings
            preflight_errors = pb7_suite_preflight_errors(self.config.config)
            if preflight_errors:
                st.error("\n\n".join(preflight_errors))

# ============================================================================
# ConfigV7 - Main configuration class
# ============================================================================

class ConfigV7():
    def __init__(self, file_name = None):
        self._config_file = file_name
        self._logging = Logging()
        self._backtest = Backtest()
        self._bot = Bot()
        self._coin_overrides = {}
        self._live = Live()
        self._optimize = Optimize()
        self._pbgui = PBGui()

        self._config = {
            "logging": self._logging._logging,
            "backtest": self._backtest._backtest,
            "bot": self._bot._bot,
            "coin_overrides": self._coin_overrides,
            "live": self._live._live,
            "optimize": self._optimize._optimize,
            "pbgui": self._pbgui._pbgui
        }

    @property
    def config_file(self): return self._config_file
    @config_file.setter
    def config_file(self, new_value):
        self._config_file = new_value

    @property
    def logging(self): return self._logging
    @logging.setter
    def logging(self, new_value):
        self._logging.logging = new_value
        self._config["logging"] = self._logging.logging

    @property
    def backtest(self): return self._backtest
    @backtest.setter
    def backtest(self, new_value):
        self._backtest.backtest = new_value
        self._config["backtest"] = self._backtest.backtest

    @property
    def bot(self): return self._bot
    @bot.setter
    def bot(self, new_value):
        self._bot.bot = new_value
        self._config["bot"] = self._bot.bot

    @property
    def coin_overrides(self): return self._coin_overrides
    @coin_overrides.setter
    def coin_overrides(self, new_value):
        self._coin_overrides = new_value
        self._config["coin_overrides"] = self._coin_overrides

    @property
    def live(self): return self._live
    @live.setter
    def live(self, new_value):
        self._live.live = new_value
        self._config["live"] = self._live.live

    @property
    def optimize(self): return self._optimize
    @optimize.setter
    def optimize(self, new_value):
        self._optimize.optimize = new_value
        self._config["optimize"] = self._optimize.optimize

    @property
    def pbgui(self): return self._pbgui
    @pbgui.setter
    def pbgui(self, new_value):
        self._pbgui.pbgui = new_value
        self._config["pbgui"] = self._pbgui.pbgui

    @property
    def config(self):
        # Dynamically update backtest to ensure suite/scenarios are current
        self._config["backtest"] = self._backtest.backtest
        # Ensure optimize export is current (bounds filtering + step persistence).
        self._config["optimize"] = self._optimize.optimize
        return self._config
    @config.setter
    def config(self, new_value):
        if not isinstance(new_value, dict):
            return
        if "logging" in new_value:
            self.logging = new_value["logging"]
        if "backtest" in new_value:
            self.backtest = new_value["backtest"]
        if "bot" in new_value:
            self.bot = new_value["bot"]
        if "coin_overrides" in new_value:
            self.coin_overrides = new_value["coin_overrides"]
        if "live" in new_value:
            self.live = new_value["live"]
        if "optimize" in new_value:
            self.optimize = new_value["optimize"]
        if "pbgui" in new_value:
            self.pbgui = new_value["pbgui"]
        # Convert coin_flags to coin_overrides
        live = new_value.get("live")
        if isinstance(live, dict) and live.get("coin_flags"):
            for symbol, flags in live["coin_flags"].items():
                    # remove USDT and USDC from symbol
                    # if symbol.endswith("USDT"):
                    #     symbol = symbol[:-4]
                    # elif symbol.endswith("USDC"):
                    #     symbol = symbol[:-4]
                    # print(symbol, flags)
                    if symbol not in self.coin_overrides:
                        self.coin_overrides[symbol] = {}
                    lm = {
                        "n": "normal",
                        "normal": "normal",
                        "m": "manual",
                        "manual": "manual",
                        "gs": "graceful_stop",
                        "graceful-stop": "graceful_stop",
                        "graceful_stop": "graceful_stop",
                        "p": "panic",
                        "panic": "panic",
                        "t": "tp_only",
                        "tp": "tp_only",
                        "tp-only": "tp_only",
                        "tp_only": "tp_only"
                    }.get(flags.split("-lm")[1].split()[0], "") if "-lm" in flags else ""
                    if lm:
                        if "live" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["live"] = {}
                        self.coin_overrides[symbol]["live"]["forced_mode_long"] = lm

                    lw = flags.split("-lw")[1].split()[0] if "-lw" in flags else ""
                    if lw:
                        if "bot" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["bot"] = {}
                        if "long" not in self.coin_overrides[symbol]["bot"]:
                            self.coin_overrides[symbol]["bot"]["long"] = {}
                        self.coin_overrides[symbol]["bot"]["long"]["wallet_exposure_limit"] = float(lw)

                    sm = {
                        "n": "normal",
                        "normal": "normal",
                        "m": "manual",
                        "manual": "manual",
                        "gs": "graceful_stop",
                        "graceful-stop": "graceful_stop",
                        "graceful_stop": "graceful_stop",
                        "p": "panic",
                        "panic": "panic",
                        "t": "tp_only",
                        "tp": "tp_only",
                        "tp-only": "tp_only",
                        "tp_only": "tp_only"
                    }.get(flags.split("-sm")[1].split()[0], "") if "-sm" in flags else ""
                    if sm:
                        if "live" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["live"] = {}
                        self.coin_overrides[symbol]["live"]["forced_mode_short"] = sm

                    sw = flags.split("-sw")[1].split()[0] if "-sw" in flags else ""
                    if sw:
                        if "bot" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["bot"] = {}
                        if "short" not in self.coin_overrides[symbol]["bot"]:
                            self.coin_overrides[symbol]["bot"]["short"] = {}
                        self.coin_overrides[symbol]["bot"]["short"]["wallet_exposure_limit"] = float(sw)

                    lev = flags.split("-lev")[1].split()[0] if "-lev" in flags else ""
                    if lev:
                        if "live" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["live"] = {}
                        self.coin_overrides[symbol]["live"]["leverage"] = float(lev)

                    config = flags.split("-lc")[1].split()[0] if "-lc" in flags else ""
                    if config:
                        self.coin_overrides[symbol]["override_config_path"] = config

    def load_config(self):
        file =  Path(f'{self._config_file}')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    config = json.load(f)
                self.config = config
            except Exception as e:
                print(f'Error loding v7 config: {file} {e}')
                traceback.print_exc()


    def save_config(self):
        if self._config != None and self._config_file != None:
            file = Path(f'{self._config_file}')
            file.parent.mkdir(parents=True, exist_ok=True)
            with open(file, "w", encoding='utf-8') as f:
                json.dump(self.config, f, indent=4)

    def view_coin_overrides(self):
        if self.config["coin_overrides"]:
            overrides = True
        else:
            overrides = False
        with st.expander("Coin Overrides", expanded=overrides):
            # Init
            if not "ed_key" in st.session_state:
                st.session_state.ed_key = 0
            ed_key = st.session_state.ed_key
            if f'select_coins_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_coins_{ed_key}']
                for row in ed["edited_rows"]:
                    if "edit" in ed["edited_rows"][row]:
                        if ed["edited_rows"][row]["edit"]:
                            st.session_state.edit_coin_override = st.session_state.co_data[row]["coin"]
            # if not "co_data" in st.session_state:
            co_data = []
            if self.config["coin_overrides"]:
                for coin in self.config["coin_overrides"]:
                    co_data.append({
                        'edit': False,
                        'coin': coin,
                        'override_config_path': self.config["coin_overrides"][coin].get('override_config_path', False),
                        'config.bot.long parameters': self.config["coin_overrides"][coin].get('bot', {}).get('long', {}),
                        'config.bot.short parameters': self.config["coin_overrides"][coin].get('bot', {}).get('short', {}),
                        'config.live parameters': self.config["coin_overrides"][coin].get('live', {}),
                    })
            st.session_state.co_data = co_data
            # Display coin_overrides
            if st.session_state.co_data and not "edit_coin_override" in st.session_state:
                d = st.session_state.co_data
                st.data_editor(data=d, height=36+(len(d))*35, key=f'select_coins_{ed_key}', disabled=['coin', 'override_config_path', 'config.bot.long parameters', 'config.bot.short parameters', 'config.live parameters'])
            if "edit_run_v7_add_coin_override_button" in st.session_state:
                if st.session_state.edit_run_v7_add_coin_override_button:
                    if self.config_file is None:
                        error_popup("Please save config, before editing coin overrides.")
                    else:
                        st.session_state.edit_coin_override = st.session_state.edit_run_v7_add_coin_override
                        st.rerun()
            if "edit_coin_override" in st.session_state:
                self.edit_coin_override(st.session_state.edit_coin_override)
            else:
                col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
                with col1:
                    st.selectbox('Symbol', st.session_state.pbcoindata.symbols, key="edit_run_v7_add_coin_override")
                with col2:
                    st.button("Add Coin Override", key="edit_run_v7_add_coin_override_button")

    def edit_coin_override(self, symbol):
        # reove USDT or USDC from symbol
        # if symbol.endswith("USDT"):
        #     symbol = symbol[:-4]
        # elif symbol.endswith("USDC"):
        #     symbol = symbol[:-4]
        
        # Live parameters that can be overridden per coin
        OVERRIDES_LIVE = [
            "forced_mode_long",
            "forced_mode_short",
            "leverage"
        ]
        
        MODE = [
            "normal",
            "manual",
            "graceful_stop",
            "panic",
            "tp_only"
        ]
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'edit_run_v7_co_parameters_{ed_key}' in st.session_state:
            ed = st.session_state[f'edit_run_v7_co_parameters_{ed_key}']
            for row in ed["edited_rows"]:
                if "delete" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["delete"]:
                        if st.session_state.co_parameters[row]["section"] == "bot":
                            self.config["coin_overrides"][symbol]["bot"][st.session_state.co_parameters[row]["side"]].pop(st.session_state.co_parameters[row]["parameter"])
                            # cleanup empty sections
                            if self.config["coin_overrides"][symbol]["bot"][st.session_state.co_parameters[row]["side"]] == {}:
                                del self.config["coin_overrides"][symbol]["bot"][st.session_state.co_parameters[row]["side"]]
                            if self.config["coin_overrides"][symbol]["bot"] == {}:
                                del self.config["coin_overrides"][symbol]["bot"]
                        elif st.session_state.co_parameters[row]["section"] == "live":
                            self.config["coin_overrides"][symbol]["live"].pop(st.session_state.co_parameters[row]["parameter"])
                            # cleanup empty sections
                            if self.config["coin_overrides"][symbol]["live"] == {}:
                                del self.config["coin_overrides"][symbol]["live"]
                        # clear co_parameters
                        if "co_parameters" in st.session_state:
                            del st.session_state.co_parameters
                        st.rerun()

        config = False
        # Init from config
        if self.config["coin_overrides"] and "edit_run_v7_co_config" not in st.session_state:
            if symbol in self.config["coin_overrides"]:
                if "override_config_path" in self.config["coin_overrides"][symbol]:
                    config = True
                    if "co_config" not in st.session_state:
                        st.session_state.co_config = ConfigV7(file_name=Path(Path(self.config_file).parent, f'{symbol}.json'))
                        st.session_state.co_config.load_config()
                        if "edit_co_configv7_long" in st.session_state:
                            del st.session_state.edit_co_configv7_long
                        if "edit_co_configv7_short" in st.session_state:
                            del st.session_state.edit_co_configv7_short
        # Init session_state for keys
        if "edit_run_v7_co_config" in st.session_state:
            if st.session_state.edit_run_v7_co_config != config:
                config = st.session_state.edit_run_v7_co_config
        if "edit_run_v7_co_parameter" in st.session_state:
            if st.session_state.edit_run_v7_co_add_parameter and st.session_state.edit_run_v7_co_side and st.session_state.edit_run_v7_co_value:
                # Ensure nested dicts exist
                if symbol not in self.config["coin_overrides"]:
                    self.config["coin_overrides"][symbol] = {}
                if "bot" not in self.config["coin_overrides"][symbol]:
                    self.config["coin_overrides"][symbol]["bot"] = {}
                if st.session_state.edit_run_v7_co_side not in self.config["coin_overrides"][symbol]["bot"]:
                    self.config["coin_overrides"][symbol]["bot"][st.session_state.edit_run_v7_co_side] = {}
                self.config["coin_overrides"][symbol]["bot"][st.session_state.edit_run_v7_co_side][st.session_state.edit_run_v7_co_parameter] = st.session_state.edit_run_v7_co_value
                if "co_parameters" in st.session_state:
                    del st.session_state.co_parameters
        if "edit_run_v7_co_parameter_live" in st.session_state:
            if st.session_state.edit_run_v7_co_add_parameter_live and st.session_state.edit_run_v7_co_value_live:
                # Ensure nested dicts exist
                if symbol not in self.config["coin_overrides"]:
                    self.config["coin_overrides"][symbol] = {}
                if "live" not in self.config["coin_overrides"][symbol]:
                    self.config["coin_overrides"][symbol]["live"] = {}
                self.config["coin_overrides"][symbol]["live"][st.session_state.edit_run_v7_co_parameter_live] = st.session_state.edit_run_v7_co_value_live
                if "co_parameters" in st.session_state:
                    del st.session_state.co_parameters
        if not "co_parameters" in st.session_state:
            co_parameters = []
            for parameter in self.config["coin_overrides"].get(symbol, {}).get('bot', {}).get('long', {}):
                co_parameters.append({
                    'section': 'bot',
                    'parameter': parameter,
                    'side': 'long',
                    'value': self.config["coin_overrides"][symbol]['bot']['long'][parameter],
                    'delete': False,
                })
            for parameter in self.config["coin_overrides"].get(symbol, {}).get('bot', {}).get('short', {}):
                co_parameters.append({
                    'section': 'bot',
                    'parameter': parameter,
                    'side': 'short',
                    'value': self.config["coin_overrides"][symbol]['bot']['short'][parameter],
                    'delete': False,
                })
            for parameter in self.config["coin_overrides"].get(symbol, {}).get('live', {}):
                co_parameters.append({
                    'section': 'live',
                    'parameter': parameter,
                    'side': 'live',
                    'value': self.config["coin_overrides"][symbol]['live'][parameter],
                    'delete': False,
                })
            st.session_state.co_parameters = co_parameters
        # Display coin_overrides
        st.write(f"{symbol}")
        if st.session_state.co_parameters:
            d = st.session_state.co_parameters
            st.data_editor(data=d, height=36+(len(d))*35, key=f'edit_run_v7_co_parameters_{ed_key}', disabled=['parameter', 'side', 'value'])
        # config.live parameters
        col1, col2, col3, col4 = st.columns([1,1,1,3], vertical_alignment="bottom")
        with col1:
            st.selectbox('config.live override parameter', OVERRIDES_LIVE, key="edit_run_v7_co_parameter_live")
        with col2:
            if st.session_state.edit_run_v7_co_parameter_live == "leverage":
                st.number_input("value", min_value=0.0, max_value=100.0, step=1.0, format="%.1f", key="edit_run_v7_co_value_live")
            else:
                st.selectbox("mode", MODE, key="edit_run_v7_co_value_live")
        with col3:
            st.button("Add", key="edit_run_v7_co_add_parameter_live")

        # config.bot parameters
        col1, col2, col3, col4 = st.columns([1,1,1,3], vertical_alignment="bottom")
        with col1:
            st.selectbox('config.bot override parameter', ALLOWED_OVERRIDES, key="edit_run_v7_co_parameter")
        with col2:
            st.selectbox("side", ["long", "short"], key="edit_run_v7_co_side")
        with col3:
            st.number_input("value", format="%.8f", key="edit_run_v7_co_value")
        with col4:
            st.button("Add", key="edit_run_v7_co_add_parameter")

        st.checkbox("Config", value=config, key="edit_run_v7_co_config", help=pbgui_help.coin_overrides_config)
        if config:
            if "co_config" not in st.session_state:
                st.session_state.co_config = ConfigV7()
            st.session_state.co_config.bot.edit_co()
        # print(self.config.coin_overrides)
        col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
        with col1:
            if st.button("OK"):
                # {"COIN1": {"override_config_path": "path/to/override_config.json"}}
                # {"COIN2": {"override_config_path": "path/to/other_override_config.json", {"bot": {"long": {"close_grid_markup_start": 0.005}}}}}
                # {"COIN3": {"bot": {"short": {"entry_initial_qty_pct": 0.01}}, "live": {"forced_mode_long": "panic"}}}
                if st.session_state.edit_run_v7_co_config:
                    st.session_state.co_config.config_file = Path(Path(self.config_file).parent, f'{symbol}.json')
                    st.session_state.co_config.save_config()
                    if symbol not in self.config["coin_overrides"]:
                        self.config["coin_overrides"][symbol] = {}
                    self.config["coin_overrides"][symbol]["override_config_path"] = f'{symbol}.json'
                else:
                    Path(Path(self.config_file).parent, f'{symbol}.json').unlink(missing_ok=True)
                    if symbol in self.config["coin_overrides"]:
                        if "override_config_path" in self.config["coin_overrides"][symbol]:
                            del self.config["coin_overrides"][symbol]["override_config_path"]
                # Remove symbol from coin_overrides if it has no parameters
                if symbol in self.config["coin_overrides"] and self.config["coin_overrides"][symbol] == {}:
                    del self.config["coin_overrides"][symbol]
                # self.save()
                self.clean_co_session_state()
                st.rerun()
        with col2:
            if st.button("Cancel"):
                self.clean_co_session_state()
                st.rerun()
        with col3:
            if st.button("Remove"):
                if self.config["coin_overrides"]:
                    if symbol in self.config["coin_overrides"]:
                        del self.config["coin_overrides"][symbol]
                Path(Path(self.config_file).parent, f'{symbol}.json').unlink(missing_ok=True)
                # self.save()
                self.clean_co_session_state()
                st.rerun()

    def clean_co_session_state(self):
        if "co_config" in st.session_state:
            del st.session_state.co_config
        if "edit_run_v7_co_config" in st.session_state:
            del st.session_state.edit_run_v7_co_config
        if "edit_coin_override" in st.session_state:
            del st.session_state.edit_coin_override
        if "co_data" in st.session_state:
            del st.session_state.co_data
        if "ed_key" in st.session_state:
            st.session_state.ed_key += 1
        if "co_parameters" in st.session_state:
            del st.session_state.co_parameters
        if "edit_run_v7_co_parameter" in st.session_state:
            del st.session_state.edit_run_v7_co_parameter
        if "edit_run_v7_co_parameter_live" in st.session_state:
            del st.session_state.edit_run_v7_co_parameter_live
        if "edit_run_v7_co_side" in st.session_state:
            del st.session_state.edit_run_v7_co_side
        if "edit_run_v7_co_value" in st.session_state:
            del st.session_state.edit_run_v7_co_value
        if "edit_run_v7_co_value_live" in st.session_state:
            del st.session_state.edit_run_v7_co_value_live

class BalanceCalculator:
    def __init__(self, config_file: str = None):
        self.config = ConfigV7()
        self.exchange = Exchange("binance", None)
        if config_file:
            self.config.config_file = config_file
            self.config.load_config()
            if "edit_bc_config" in st.session_state:
                del st.session_state.edit_bc_config
        self.coin_infos = []
        self.balance_long = []
        self.balance_short = []

    @st.dialog("Select Exchange")
    def _dialog_select_backtest_exchange(self, exchanges: list[str]):
        st.write("This backtest config has multiple exchanges. Please choose one for Balance Calculator.")
        current = st.session_state.get("bc_exchange_id")
        default_index = exchanges.index(current) if current in exchanges else 0
        selected = st.selectbox("Exchange", exchanges, index=default_index, key="bc_backtest_exchange_choice")
        if st.button("Use exchange"):
            st.session_state.bc_exchange_id = selected
            self.exchange = Exchange(selected, None)
            if "bc_require_exchange_choice" in st.session_state:
                del st.session_state.bc_require_exchange_choice
            st.rerun()
    
    @property
    def balance(self):
        return self.balance_long + self.balance_short

    def init_coindata(self):
        if "pbcoindata" not in st.session_state:
            st.session_state.pbcoindata = CoinData()
        coindata = st.session_state.pbcoindata
        coindata.exchange = self.exchange.id
        if self.config.pbgui.dynamic_ignore:
            approved_coins, _ = coindata.filter_mapping(
                exchange=self.exchange.id,
                market_cap_min_m=self.config.pbgui.market_cap,
                vol_mcap_max=self.config.pbgui.vol_mcap,
                only_cpt=self.config.pbgui.only_cpt,
                notices_ignore=self.config.pbgui.notices_ignore,
                tags=self.config.pbgui.tags,
                quote_filter=None,
                use_cache=True,
            )
            self.config.live.approved_coins = approved_coins

    def view(self):
        context_exchanges = st.session_state.get("bc_context_exchanges")
        if isinstance(context_exchanges, list):
            exchanges = [e for e in context_exchanges if isinstance(e, str) and e in V7.list()]
        else:
            exchanges = []

        if len(exchanges) == 1:
            self.exchange = Exchange(exchanges[0], None)
            st.session_state.bc_exchange_id = exchanges[0]
            if "bc_missing_exchange_context" in st.session_state:
                del st.session_state.bc_missing_exchange_context
            if "bc_require_exchange_choice" in st.session_state:
                del st.session_state.bc_require_exchange_choice
        elif len(exchanges) > 1:
            if "bc_missing_exchange_context" in st.session_state:
                del st.session_state.bc_missing_exchange_context
            current = st.session_state.get("bc_exchange_id")
            if current in exchanges:
                self.exchange = Exchange(current, None)
                if "bc_require_exchange_choice" in st.session_state:
                    del st.session_state.bc_require_exchange_choice
            else:
                st.session_state.bc_require_exchange_choice = exchanges
        else:
            st.session_state.bc_missing_exchange_context = True

        if st.session_state.get("bc_missing_exchange_context"):
            st.error("Missing exchange context. Please open Balance Calculator from RunV7 or BacktestV7 again.")
            return

        required_exchanges = st.session_state.get("bc_require_exchange_choice")
        if isinstance(required_exchanges, list) and required_exchanges:
            self._dialog_select_backtest_exchange(required_exchanges)
            st.warning("Please choose an exchange to continue.")
            return

        if "bc_exchange_id" in st.session_state:
            if st.session_state.bc_exchange_id != self.exchange.id:
                self.exchange = Exchange(st.session_state.bc_exchange_id, None)
                # st.session_state.bc_exchange = bc_exchange
        else:
            st.session_state.bc_exchange_id = self.exchange.id

        # Init coindata
        self.init_coindata()
        if "edit_bc_config" in st.session_state:
            if st.session_state.edit_bc_config != json.dumps(self.config.config, indent=4):
                try:
                    self.config.config = json.loads(st.session_state.edit_bc_config)
                    self.init_coindata()
                except:
                    error_popup("Invalid JSON")
                    st.session_state.edit_bc_config = json.dumps(self.config.config, indent=4)
        else:
            st.session_state.edit_bc_config = json.dumps(self.config.config, indent=4)
        col1, col2 = st.columns([1, 1])
        with col1:
            st.text_area(f'config', key="edit_bc_config", height=500)
        with col2:
            st.markdown("### Balance Calculator")
            st.markdown("This tool allows you to calculate the balance for a given configuration.")
            st.markdown("You can edit the configuration in the left text area and click on 'Calculate' to see the results.")
            st.selectbox("Exchange", V7.list(), key="bc_exchange_id")
            if st.button("Calculate"):
                coins = set(self.config.live.approved_coins.long + self.config.live.approved_coins.short)
                self.coin_infos = []
                self.balance_long = []
                self.balance_short = []
                coindata = st.session_state.pbcoindata
                mapping = coindata.load_mapping(exchange=self.exchange.id, use_cache=True)
                preferred_quote = "USDC" if self.exchange.id == "hyperliquid" else "USDT"
                best_rows_by_coin = {}

                for record in mapping:
                    coin = (record.get("coin") or "").upper()
                    if not coin or coin not in coins:
                        continue

                    quote = (record.get("quote") or "").upper()
                    price = float(record.get("price_last") or 0.0)
                    contract_size = float(record.get("contract_size") or 1.0)
                    min_amount = float(record.get("min_amount") or record.get("precision_amount") or 0.0)
                    min_cost = float(record.get("min_cost") or 0.0)
                    min_order_price = float(record.get("min_order_price") or 0.0)
                    if min_order_price <= 0 and price > 0:
                        min_order_price = max(min_cost, min_amount * contract_size * price)

                    score = (
                        0 if quote == preferred_quote else 1,
                        0 if bool(record.get("active", True)) else 1,
                        0 if bool(record.get("linear", True)) else 1,
                        0 if min_order_price > 0 else 1,
                        -price,
                    )

                    prev = best_rows_by_coin.get(coin)
                    if prev is None or score < prev[0]:
                        best_rows_by_coin[coin] = (score, record, min_order_price, price, contract_size, min_amount, min_cost)

                with st.spinner(text='loading coin infos from mapping...'):
                    with st.empty():
                        for counter, coin in enumerate(sorted(coins)):
                            st.text(f'{counter + 1}/{len(coins)}: {coin}')
                            best = best_rows_by_coin.get(coin)
                            if best is None:
                                continue
                            _, record, min_order_price, price, contract_size, min_amount, min_cost = best
                            lev = record.get("max_leverage")
                            self.coin_infos.append({
                                "coin": coin,
                                "currentPrice": price,
                                "contractSize": contract_size,
                                "min_amount": min_amount,
                                "min_cost": min_cost,
                                "min_order_price": min_order_price,
                                "max lev": lev
                            })
                            if coin in self.config.live.approved_coins.long:
                                if self.config.bot.long.n_positions > 0 and self.config.bot.long.total_wallet_exposure_limit > 0:
                                    we = self.config.bot.long.total_wallet_exposure_limit / self.config.bot.long.n_positions
                                    balance = min_order_price / (we * self.config.bot.long.entry_initial_qty_pct)
                                    self.balance_long.append({
                                        "coin": coin,
                                        "balance": balance
                                    })
                            if coin in self.config.live.approved_coins.short:
                                if self.config.bot.short.n_positions > 0 and self.config.bot.short.total_wallet_exposure_limit > 0:
                                    we = self.config.bot.short.total_wallet_exposure_limit / self.config.bot.short.n_positions
                                    balance = min_order_price / (we * self.config.bot.short.entry_initial_qty_pct)
                                    self.balance_short.append({
                                        "coin": coin,
                                        "balance": balance
                                    })

        # sort coin_infos by min_order_price
        self.coin_infos = sorted(self.coin_infos, key=lambda x: x['min_order_price'], reverse=True)
        if self.coin_infos:
            st.write("### Coin Information")
            st.dataframe(self.coin_infos, hide_index=True)

        # find highest balance in short and long
        self.balance_long = sorted(self.balance_long, key=lambda x: x['balance'], reverse=True)
        self.balance_short = sorted(self.balance_short, key=lambda x: x['balance'], reverse=True)
        side = None
        if self.balance_long:
            if self.balance_short:
                if self.balance_long[0]['balance'] > self.balance_short[0]['balance']:
                    side = "long"
                else:
                    side = "short"
            else:
                side = "long"
        else:
            if self.balance_short:
                side = "short"
        if side in ["long", "short"]:
            # Select the correct attributes based on side
            balance_list = self.balance_long if side == "long" else self.balance_short
            bot_side = self.config.bot.long if side == "long" else self.config.bot.short
            # Get symbol name with highest balance
            symbol = balance_list[0]['coin']
            # get min order price for symbol from coin_infos
            min_order_price = next((coin['min_order_price'] for coin in self.coin_infos if coin['coin'] == symbol), 0)
            # Display calculated balance with formula
            st.write(f"### Balance needed for {symbol} ({side.capitalize()} Side)")
            st.write(f"**Minimum Order Price:** `{min_order_price:.2f}`")
            st.write(f"**Total Wallet Exposure Limit:** `{bot_side.total_wallet_exposure_limit:.2f}`")
            st.write(f"**Number of Positions:** `{bot_side.n_positions}`")
            st.write(f"**Entry Initial Quantity Percentage:** `{bot_side.entry_initial_qty_pct:.2f}`")
            st.write(f"To calculate the balance needed for {symbol} on the {side} side, use the formula:")
            st.write(f"**Formula:** `min_order_price / ((total_wallet_exposure_limit / n_positions) * entry_initial_qty_pct)`")
            result = min_order_price / ((bot_side.total_wallet_exposure_limit / bot_side.n_positions) * bot_side.entry_initial_qty_pct)
            st.write(f"**Calculation:** `{min_order_price} / (({bot_side.total_wallet_exposure_limit} / {bot_side.n_positions}) * {bot_side.entry_initial_qty_pct}) = {result:.2f}`")
            recommended_balance = math.ceil(result * 1.1 / 10) * 10
            st.write(f"### Recommended Balance (10% more): :green[{int(recommended_balance)} USDT]")

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
