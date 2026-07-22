"""Pure PB7 optimize preset generation helpers for Pareto Explorer.

This module intentionally has no UI dependency. It mirrors the legacy
ParetoExplorer preset generator calculations and returns plain dictionaries.
"""

from __future__ import annotations

import copy
import json
from decimal import Decimal, InvalidOperation
from typing import Any

from api.pb7_bridge import get_optimize_metric_sets, get_optimize_scoring_default_goals
from ParetoDataLoader import _extract_scoring_metric_names


OPTIMIZE_PRESET_DIRECTIONS = [
    "Balanced (keep run scoring)",
    "More profit (risk can be higher)",
    "Safer (lower drawdowns)",
    "Smoother equity curve",
    "Fewer/shorter holds (less time in market)",
    "Lower exposure (safer sizing)",
]

MAX_PRESET_OBJECTIVES = 8

DEFAULT_PRESET_SCORING = [
    "adg_strategy_eq",
    "adg_strategy_eq_w",
    "mdg_strategy_eq_w",
    "sortino_ratio_strategy_eq",
    "peak_recovery_days_strategy_eq",
    "position_held_days_max",
    "drawdown_worst_strategy_eq",
    "drawdown_worst_mean_1pct_strategy_eq",
]

_PRESET_GOAL_FALLBACK_PREFIXES = (
    "drawdown_",
    "expected_shortfall_",
    "equity_balance_diff_",
    "trade_loss_",
    "peak_recovery_",
    "position_held_",
    "position_unchanged_",
    "positions_held_",
    "high_exposure_",
    "hard_stop_",
    "equity_choppiness",
    "equity_jerkiness",
    "exponential_fit_error",
)
_PRESET_GOAL_FALLBACK_MIN = {
    "loss_profit_ratio",
    "paper_loss_ratio",
    "paper_loss_mean_ratio",
    "total_wallet_exposure_mean",
    "position_held_days_max",
    "peak_recovery_days_strategy_eq",
}


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _clamp_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(low, min(high, parsed))


def _detect_metric_scheme(metrics: list[str]) -> str:
    for metric in metrics:
        if isinstance(metric, str) and metric.startswith("btc_"):
            return "btc_prefix"
    for metric in metrics:
        if isinstance(metric, str) and (metric.endswith("_btc") or metric.endswith("_usd")):
            return "suffix"
    return "base"


def _metric_name(name_base: str, scheme: str, currency_metrics: set[str]) -> str:
    if scheme == "btc_prefix":
        return f"btc_{name_base}"
    if scheme == "suffix":
        if name_base in currency_metrics:
            return f"{name_base}_usd"
        return name_base
    return name_base


def _upsert_limit(limits: Any, entry: dict[str, Any]) -> Any:
    if not isinstance(limits, list):
        return limits
    metric = entry.get("metric")
    if not metric:
        return limits
    out = []
    replaced = False
    for item in limits:
        if isinstance(item, dict) and item.get("metric") == metric:
            out.append(entry)
            replaced = True
        else:
            out.append(item)
    if not replaced:
        out.append(entry)
    return out


def _find_limit_entry(limits: Any, metric_name: str) -> dict[str, Any] | None:
    if not isinstance(limits, list):
        return None
    for item in limits:
        if isinstance(item, dict) and item.get("metric") == metric_name:
            return item
    return None


def _unique_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _cap_objectives(items: list[str], max_n: int = MAX_PRESET_OBJECTIVES) -> list[str]:
    return items[:max_n]


def _scoring_metric_name(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry.strip() or None
    if isinstance(entry, bytes):
        return entry.decode("utf-8", errors="replace").strip() or None
    if isinstance(entry, dict):
        metric = entry.get("metric") or entry.get(b"metric")
        if isinstance(metric, bytes):
            metric = metric.decode("utf-8", errors="replace")
        metric_name = str(metric or "").strip()
        return metric_name or None
    return None


def _scoring_dict_entry(entry: dict[str, Any], default_goal_map: dict[str, str]) -> dict[str, Any] | None:
    metric_name = _scoring_metric_name(entry)
    if not metric_name:
        return None

    out: dict[str, Any] = {}
    for key, value in entry.items():
        key_name = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
        out[key_name] = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    out["metric"] = metric_name
    if out.get("goal") is not None:
        out["goal"] = str(out["goal"])
    elif metric_name in default_goal_map:
        out["goal"] = default_goal_map[metric_name]
    else:
        out["goal"] = _fallback_scoring_goal(metric_name)
    return out


def _dedupe_scoring_entries(entries: list[Any], max_n: int = MAX_PRESET_OBJECTIVES) -> list[Any]:
    seen = set()
    out = []
    for entry in entries:
        metric_name = _scoring_metric_name(entry)
        if not metric_name or metric_name in seen:
            continue
        seen.add(metric_name)
        out.append(entry)
        if len(out) >= max_n:
            break
    return out


def _scoring_entry(metric_name: str, default_goal_map: dict[str, str]) -> Any:
    goal = default_goal_map.get(metric_name) or _fallback_scoring_goal(metric_name)
    if goal:
        return {"metric": metric_name, "goal": goal}
    return metric_name


def _fallback_scoring_goal(metric_name: str) -> str:
    base = str(metric_name or "").strip()
    if base.endswith(("_usd", "_btc")):
        base = base[:-4]
    if base in _PRESET_GOAL_FALLBACK_MIN or base.startswith(_PRESET_GOAL_FALLBACK_PREFIXES):
        return "min"
    return "max"


def _scoring_entries(metric_names: list[str], default_goal_map: dict[str, str]) -> list[Any]:
    return [_scoring_entry(metric, default_goal_map) for metric in _cap_objectives(_unique_keep_order(metric_names))]


def _first_available_metric(candidates: list[str], suite_metrics: dict[str, Any]) -> str:
    for metric_name in candidates:
        if metric_name in suite_metrics:
            return metric_name
    return candidates[0]


def _available_metrics(candidates: list[str], suite_metrics: dict[str, Any]) -> list[str]:
    available = [metric_name for metric_name in candidates if metric_name in suite_metrics]
    return available or candidates[:1]


def _balanced_scoring_entries(base_scoring: list[Any], base_metric_names: list[str], default_goal_map: dict[str, str]) -> list[Any]:
    if any(isinstance(entry, dict) for entry in base_scoring):
        preserved = []
        for entry in base_scoring:
            if isinstance(entry, dict):
                normalized = _scoring_dict_entry(entry, default_goal_map)
                if normalized is not None:
                    preserved.append(normalized)
            else:
                metric_name = _scoring_metric_name(entry)
                if metric_name:
                    preserved.append(_scoring_entry(metric_name, default_goal_map))
        return _dedupe_scoring_entries(preserved)
    return _scoring_entries(_unique_keep_order(list(base_metric_names)), default_goal_map)


def _as_bound_tuple(bound_value: Any) -> tuple[float | None, float | None, float | None]:
    if isinstance(bound_value, (list, tuple)):
        if len(bound_value) >= 2:
            low = _to_float(bound_value[0])
            high = _to_float(bound_value[1])
            step = _to_float(bound_value[2]) if len(bound_value) >= 3 else None
            if step == 0:
                step = None
            return low, high, step
        if len(bound_value) == 1:
            value = _to_float(bound_value[0])
            return value, value, None
    value = _to_float(bound_value)
    if value is not None:
        return value, value, None
    return None, None, None


def _step_decimal_places(step: float | None) -> int | None:
    if step is None or step == 0:
        return None
    try:
        value = Decimal(str(abs(float(step)))).normalize()
    except (InvalidOperation, ValueError, TypeError):
        return None
    exponent = value.as_tuple().exponent
    if exponent >= 0:
        return 0
    return min(abs(exponent), 12)


def _clean_generated_float(value: float | None, step: float | None = None) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if step and abs(step - 1.0) < 1e-12:
        return float(int(round(numeric)))
    step_decimals = _step_decimal_places(step)
    if step_decimals is not None:
        return float(round(numeric, step_decimals))
    return float(f"{numeric:.12g}")


def _clean_generated_bounds(low: float, high: float, step: float | None) -> tuple[float, float]:
    cleaned_low = _clean_generated_float(low, step)
    cleaned_high = _clean_generated_float(high, step)
    return float(cleaned_low), float(cleaned_high)


def _tighten_bounds_around_value(low: float | None, high: float | None, value: float, pct: float, step: float | None) -> Any:
    if pct <= 0:
        return [low, high, step] if step else [low, high]
    if low is None or high is None:
        return [value, value]
    if high < low:
        low, high = high, low

    delta = abs(value) * (pct / 100.0)
    if delta == 0:
        delta = (high - low) * (pct / 100.0)
    new_low = max(low, value - delta)
    new_high = min(high, value + delta)

    if new_high <= new_low:
        if step:
            new_low = max(low, new_low - step)
            new_high = min(high, new_high + step)
        else:
            expand = max((high - low) * 0.01, 1e-12)
            new_low = max(low, new_low - expand)
            new_high = min(high, new_high + expand)

    if step:
        new_low, new_high = _clean_generated_bounds(new_low, new_high, step)
        if new_high <= new_low:
            new_high = min(high, new_low + step)
        return [new_low, new_high, step]
    new_low, new_high = _clean_generated_bounds(new_low, new_high, None)
    return [new_low, new_high]


def _tighten_bounds_around_value_asymmetric(
    low: float | None,
    high: float | None,
    value: float,
    pct_down: float,
    pct_up: float,
    step: float | None,
) -> Any:
    if pct_down <= 0 and pct_up <= 0:
        return [low, high, step] if step else [low, high]
    if low is None or high is None:
        return [value, value]
    if high < low:
        low, high = high, low

    down = abs(value) * (pct_down / 100.0)
    up = abs(value) * (pct_up / 100.0)
    if down == 0:
        down = (high - low) * (pct_down / 100.0)
    if up == 0:
        up = (high - low) * (pct_up / 100.0)

    new_low = max(low, value - down)
    new_high = min(high, value + up)

    if new_high <= new_low:
        if step:
            new_low = max(low, new_low - step)
            new_high = min(high, new_high + step)
        else:
            expand = max((high - low) * 0.01, 1e-12)
            new_low = max(low, new_low - expand)
            new_high = min(high, new_high + expand)

    if step:
        new_low, new_high = _clean_generated_bounds(new_low, new_high, step)
        if new_high <= new_low:
            new_high = min(high, new_low + step)
        return [new_low, new_high, step]
    new_low, new_high = _clean_generated_bounds(new_low, new_high, None)
    return [new_low, new_high]


def _normalize_bound_for_compare(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return tuple(round(item, 12) if isinstance(item, float) else item for item in value)
    if isinstance(value, float):
        return round(value, 12)
    return value


def _bound_pretty(value: Any) -> str:
    try:
        return json.dumps(value)
    except Exception:
        return str(value)


def _is_side_enabled(bot_params: dict[str, Any], side: str) -> bool | None:
    if side not in {"long", "short"}:
        return None
    has_any = False
    disabled = False
    for key in (
        f"{side}_n_positions",
        f"{side}_total_wallet_exposure_limit",
        f"{side}.risk.n_positions",
        f"{side}.risk.total_wallet_exposure_limit",
    ):
        value = _to_float(bot_params.get(key))
        if value is not None:
            has_any = True
            if value <= 0:
                disabled = True
    if not has_any:
        return None
    return not disabled


def _risk_multipliers(direction: str, signed: float, *, invert: bool = False) -> tuple[float, float]:
    if signed == 0:
        return 1.0, 1.0
    aggressive = signed > 0
    if invert:
        aggressive = not aggressive
    if direction == "More profit (risk can be higher)" and not invert:
        return (0.3, 2.0) if aggressive else (2.0, 0.3)
    return (0.5, 1.5) if aggressive else (1.5, 0.5)


def _risk_limit_from_tolerance(
    *,
    metric_base: str,
    base_entry: dict[str, Any] | None,
    suite_metrics: dict[str, Any],
    scheme: str,
    currency_metrics: set[str],
    risk_adjust: int,
    margin_min: float,
    margin_max: float,
    use_abs_seed: bool = False,
    default_value: float | None = None,
) -> dict[str, Any] | None:
    metric_name = _metric_name(metric_base, scheme, currency_metrics)
    penalize_if_raw = (base_entry or {}).get("penalize_if", ">")
    penalize_if = {"greater_than": ">", "less_than": "<"}.get(str(penalize_if_raw), str(penalize_if_raw))
    stat = (base_entry or {}).get("stat")

    seed_value = _to_float(suite_metrics.get(metric_name))
    if seed_value is not None and use_abs_seed:
        seed_value = abs(seed_value)

    signed = float(risk_adjust) / 50.0
    magnitude = min(1.0, abs(signed))
    margin = margin_min + magnitude * (margin_max - margin_min)

    if seed_value is not None:
        op = str(penalize_if).strip()
        is_less = op.startswith("<") or op == "less_than"
        if signed > 0:
            factor = (1.0 - margin) if is_less else (1.0 + margin)
        else:
            factor = (1.0 + margin) if is_less else (1.0 - margin)
        value = seed_value * max(0.0, factor)
    else:
        base_value = _to_float((base_entry or {}).get("value"))
        if base_value is not None:
            value = base_value
        elif default_value is not None:
            value = float(default_value)
        else:
            return None

    entry: dict[str, Any] = {"metric": metric_name, "penalize_if": penalize_if, "value": round(float(value), 6)}
    if stat is not None:
        entry["stat"] = stat
    return entry


def _risk_limits_pack_from_tolerance(
    limits: Any,
    *,
    suite_metrics: dict[str, Any],
    scheme: str,
    currency_metrics: set[str],
    risk_adjust: int,
) -> Any:
    if not isinstance(limits, list):
        return limits
    specs = [
        ("drawdown_worst", 0.05, 0.50, False, 0.30),
        ("expected_shortfall_1pct", 0.05, 0.60, False, 0.30),
        ("equity_choppiness_w", 0.10, 1.00, False, None),
        ("peak_recovery_hours_equity", 0.05, 0.80, False, None),
        ("position_held_hours_max", 0.05, 0.80, False, None),
        ("equity_balance_diff_neg_max", 0.05, 0.80, True, None),
    ]
    out = list(limits)
    for metric_base, margin_min, margin_max, use_abs, fallback in specs:
        metric_name = _metric_name(metric_base, scheme, currency_metrics)
        entry = _risk_limit_from_tolerance(
            metric_base=metric_base,
            base_entry=_find_limit_entry(out, metric_name),
            suite_metrics=suite_metrics,
            scheme=scheme,
            currency_metrics=currency_metrics,
            risk_adjust=risk_adjust,
            margin_min=margin_min,
            margin_max=margin_max,
            use_abs_seed=use_abs,
            default_value=fallback,
        )
        if entry is not None:
            out = _upsert_limit(out, entry)
    return out


def _normalize_near_map(near_bounds: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(near_bounds, dict):
        return out
    for side_key, edge in (("at_lower", "lower"), ("at_upper", "upper")):
        for key, info in (near_bounds.get(side_key) or {}).items():
            item = dict(info or {}) if isinstance(info, dict) else {}
            item["edge"] = edge
            out[str(key)] = item
    return out


def _flatten_dotted_bounds(bounds: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten canonical PB8 bounds while preserving each bound value exactly."""
    if not isinstance(bounds, dict):
        return {}
    out: dict[str, Any] = {}
    for raw_key, value in bounds.items():
        key = str(raw_key)
        path = f"{prefix}.{key}" if prefix else key
        is_bound_object = isinstance(value, dict) and any(
            low in value and high in value
            for low, high in (("lower", "upper"), ("min", "max"), ("lo", "hi"))
        )
        if isinstance(value, dict) and not is_bound_object and "bounds" not in value:
            out.update(_flatten_dotted_bounds(value, path))
        else:
            out[path] = copy.deepcopy(value)
    return out


def _restore_dotted_bounds(bounds: dict[str, Any]) -> dict[str, Any]:
    """Restore dotted PB8 bound paths to the canonical nested object shape."""
    restored: dict[str, Any] = {}
    for dotted_key, value in bounds.items():
        parts = [part for part in str(dotted_key).split(".") if part]
        if not parts:
            continue
        target = restored
        for part in parts[:-1]:
            child = target.get(part)
            if not isinstance(child, dict):
                child = {}
                target[part] = child
            target = child
        target[parts[-1]] = copy.deepcopy(value)
    return restored


def _build_near_rows(near_map: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for key, info in sorted(near_map.items(), key=lambda item: item[0]):
        rows.append({
            "param": str(key),
            "edge": info.get("edge"),
            "value": info.get("value"),
            "bound": info.get("bound"),
        })
    return rows


def _build_scoring_and_limits(
    *,
    full_config_data: dict[str, Any],
    optimize_settings: dict[str, Any],
    suite_metrics: dict[str, Any],
    direction: str,
    risk_adjust: int,
    currency_metrics: set[str],
    default_goal_map_override: dict[str, str] | None = None,
) -> tuple[list[Any], Any, str]:
    optimize_block = full_config_data.get("optimize", {}) if isinstance(full_config_data, dict) else {}
    base_scoring = optimize_block.get("scoring") if isinstance(optimize_block, dict) else None
    base_limits = optimize_block.get("limits") if isinstance(optimize_block, dict) else None
    if not base_scoring and isinstance(optimize_settings, dict):
        base_scoring = optimize_settings.get("scoring")
    if base_limits is None and isinstance(optimize_settings, dict):
        base_limits = optimize_settings.get("limits")

    base_scoring_entries = list(base_scoring) if isinstance(base_scoring, list) and base_scoring else []
    base_metric_names = _extract_scoring_metric_names(base_scoring_entries) if base_scoring_entries else []
    if not base_metric_names:
        base_metric_names = list(DEFAULT_PRESET_SCORING)
        base_scoring_entries = list(base_metric_names)
    if base_limits is None:
        base_limits = []

    scheme = _detect_metric_scheme(base_metric_names)
    metric = lambda name: _metric_name(name, scheme, currency_metrics)
    profit_set = [
        _first_available_metric(["mdg_strategy_eq_w", "mdg_strategy_eq", metric("mdg_w"), metric("mdg"), metric("gain")], suite_metrics),
        _first_available_metric(["adg_strategy_eq_w", "adg_strategy_eq", metric("adg_w"), metric("adg"), metric("gain")], suite_metrics),
        _first_available_metric(["gain_strategy_eq", metric("gain")], suite_metrics),
    ]
    ratio_set = [
        _first_available_metric(["loss_profit_ratio", "loss_profit_ratio_w", metric("loss_profit_ratio")], suite_metrics),
        _first_available_metric(["sortino_ratio_strategy_eq", "sharpe_ratio_strategy_eq", metric("sortino_ratio"), metric("sharpe_ratio")], suite_metrics),
        _first_available_metric(["sharpe_ratio_strategy_eq", "sortino_ratio_strategy_eq", metric("sharpe_ratio"), metric("sortino_ratio")], suite_metrics),
    ]
    risk_set = [
        _first_available_metric(["drawdown_worst_strategy_eq", metric("drawdown_worst")], suite_metrics),
        _first_available_metric(["drawdown_worst_mean_1pct_strategy_eq", "expected_shortfall_1pct_strategy_eq", metric("drawdown_worst_mean_1pct"), metric("expected_shortfall_1pct")], suite_metrics),
        _first_available_metric(["peak_recovery_days_strategy_eq", "peak_recovery_hours_strategy_eq", "peak_recovery_hours_equity"], suite_metrics),
    ]
    smooth_set = [
        _first_available_metric([metric("equity_choppiness_w"), metric("equity_choppiness")], suite_metrics),
        _first_available_metric([metric("equity_jerkiness_w"), metric("equity_jerkiness")], suite_metrics),
        _first_available_metric([metric("exponential_fit_error_w"), metric("exponential_fit_error")], suite_metrics),
    ]
    turnover_set = [
        _first_available_metric(["position_held_days_max", "position_held_hours_max", "position_unchanged_days_max", "position_unchanged_hours_max"], suite_metrics),
        _first_available_metric(["position_held_days_mean", "position_held_days_median", "position_held_hours_mean", "position_held_hours_median"], suite_metrics),
        _first_available_metric(["total_wallet_exposure_mean", "high_exposure_hours_mean_long", "high_exposure_hours_mean_short", "positions_held_per_day_w", "positions_held_per_day"], suite_metrics),
    ]
    exposure_set = _available_metrics([
        metric("adg_w_per_exposure_long"),
        metric("adg_w_per_exposure_short"),
        "total_wallet_exposure_mean",
        "high_exposure_hours_mean_long",
        "high_exposure_hours_mean_short",
    ], suite_metrics)
    if default_goal_map_override:
        default_goal_map = {
            str(metric): str(goal).lower()
            for metric, goal in default_goal_map_override.items()
            if str(goal).lower() in {"min", "max"}
        }
    else:
        default_goal_map = get_optimize_scoring_default_goals(
            _unique_keep_order(base_metric_names + profit_set + ratio_set + risk_set + smooth_set + turnover_set + exposure_set)
        )

    if direction not in OPTIMIZE_PRESET_DIRECTIONS:
        direction = OPTIMIZE_PRESET_DIRECTIONS[0]

    limits_out: Any = list(base_limits) if isinstance(base_limits, list) else base_limits
    if direction == "Balanced (keep run scoring)":
        scoring_out = _balanced_scoring_entries(base_scoring_entries, base_metric_names, default_goal_map)
        limits_out = base_limits
    elif direction == "More profit (risk can be higher)":
        scoring_out = _scoring_entries([profit_set[0], profit_set[1], ratio_set[1], ratio_set[0]], default_goal_map)
        limits_out = base_limits
    elif direction == "Safer (lower drawdowns)":
        scoring_out = _scoring_entries([risk_set[0], risk_set[1], ratio_set[1], ratio_set[0]], default_goal_map)
        limits_out = _risk_limits_pack_from_tolerance(limits_out, suite_metrics=suite_metrics, scheme=scheme, currency_metrics=currency_metrics, risk_adjust=risk_adjust) if risk_adjust != 0 else base_limits
    elif direction == "Smoother equity curve":
        scoring_out = _scoring_entries([smooth_set[0], smooth_set[1], ratio_set[1], profit_set[0]], default_goal_map)
        limits_out = base_limits
    elif direction == "Fewer/shorter holds (less time in market)":
        scoring_out = _scoring_entries([turnover_set[0], turnover_set[1], turnover_set[2], profit_set[0], ratio_set[1]], default_goal_map)
        limits_out = base_limits
    else:
        scoring_out = _scoring_entries(exposure_set + [risk_set[0], ratio_set[1], profit_set[0]], default_goal_map)
        limits_out = _risk_limits_pack_from_tolerance(limits_out, suite_metrics=suite_metrics, scheme=scheme, currency_metrics=currency_metrics, risk_adjust=risk_adjust) if risk_adjust != 0 else base_limits

    if isinstance(limits_out, list) and risk_adjust != 0:
        metric_name = metric("drawdown_worst")
        entry = _risk_limit_from_tolerance(
            metric_base="drawdown_worst",
            base_entry=_find_limit_entry(limits_out, metric_name),
            suite_metrics=suite_metrics,
            scheme=scheme,
            currency_metrics=currency_metrics,
            risk_adjust=risk_adjust,
            margin_min=0.05,
            margin_max=0.60,
            use_abs_seed=False,
            default_value=0.30,
        )
        if entry is not None:
            limits_out = _upsert_limit(limits_out, entry)

    return _dedupe_scoring_entries(scoring_out), limits_out, scheme


def _build_new_bounds(
    *,
    bounds: dict[str, Any],
    bot_params: dict[str, Any],
    direction: str,
    risk_adjust: int,
    window_pct: float,
    near_map: dict[str, dict[str, Any]],
    expand_near_bounds: bool,
    near_bounds_expand_pct: float,
    only_near_bounds: bool = False,
    apply_risk_adjustments: bool = True,
    apply_window_adjustments: bool = True,
    apply_near_expansion: bool = True,
    expand_notes_out: dict[str, str] | None = None,
) -> dict[str, Any]:
    base_bounds = copy.deepcopy(bounds or {})
    new_bounds: dict[str, Any] = {}
    expand_enabled = bool(apply_near_expansion and expand_near_bounds and near_bounds_expand_pct > 0 and near_map)

    if expand_enabled:
        for param_name, info in near_map.items():
            if param_name not in base_bounds:
                continue
            edge = (info or {}).get("edge")
            if edge not in {"lower", "upper"}:
                continue
            low, high, step = _as_bound_tuple(base_bounds.get(param_name))
            if low is None or high is None or abs(low) < 1e-15 and abs(high) < 1e-15:
                continue
            if high < low:
                low, high = high, low
            span = high - low
            if span < 1e-12:
                continue
            expand = span * (near_bounds_expand_pct / 100.0)
            new_low = low
            new_high = high
            if edge == "lower":
                requested_low = low - expand
                new_low = requested_low
                if low >= 0 and new_low < 0:
                    if expand_notes_out is not None:
                        expand_notes_out[param_name] = f"lower expansion clamped to 0 (requested {requested_low:g})"
                    new_low = 0.0
            else:
                new_high = high + expand
            if step and abs(step - 1.0) < 1e-12:
                rounded_low = _clean_generated_float(new_low, step)
                rounded_high = _clean_generated_float(new_high, step)
                if (abs(rounded_low - new_low) > 1e-12 or abs(rounded_high - new_high) > 1e-12) and expand_notes_out is not None:
                    prev = expand_notes_out.get(param_name)
                    expand_notes_out[param_name] = f"{prev}; rounded to integer step" if prev else "rounded to integer step"
                new_low = rounded_low
                new_high = rounded_high
                if new_high <= new_low:
                    new_high = new_low + 1.0
            else:
                new_low, new_high = _clean_generated_bounds(new_low, new_high, step)
            base_bounds[param_name] = [new_low, new_high, step] if step else [new_low, new_high]

    risk_enabled = bool(apply_risk_adjustments and risk_adjust != 0)
    window_enabled = bool(apply_window_adjustments and window_pct > 0)
    if not window_enabled and not risk_enabled and not expand_enabled:
        return copy.deepcopy(base_bounds)

    long_enabled = _is_side_enabled(bot_params, "long")
    short_enabled = _is_side_enabled(bot_params, "short")
    signed = float(risk_adjust) / 50.0
    strength = min(1.0, abs(signed))
    risk_window_pct = max(2.0, min(25.0, 5.0 + 20.0 * strength))

    for param_name, bound_value in base_bounds.items():
        name = str(param_name)
        if name.startswith(("long_", "long.")) and long_enabled is False:
            new_bounds[param_name] = bound_value
            continue
        if name.startswith(("short_", "short.")) and short_enabled is False:
            new_bounds[param_name] = bound_value
            continue
        if only_near_bounds and str(param_name) not in near_map:
            new_bounds[param_name] = bound_value
            continue

        is_risk_param = (
            "total_wallet_exposure_limit" in name
            or name.endswith("n_positions")
            or "_n_positions" in name
            or "risk_we_excess_allowance_pct" in name
            or "risk_wel_enforcer_threshold" in name
            or "risk_twel_enforcer_threshold" in name
            or "unstuck_loss_allowance_pct" in name
        )
        if not window_enabled and not (risk_enabled and is_risk_param):
            new_bounds[param_name] = bound_value
            continue

        low, high, step = _as_bound_tuple(bound_value)
        if low is None or high is None or abs(low) < 1e-15 and abs(high) < 1e-15:
            new_bounds[param_name] = bound_value
            continue

        value = _to_float(bot_params.get(param_name))
        if value is None:
            new_bounds[param_name] = [low, high, step] if step else [low, high]
            continue

        if risk_enabled and is_risk_param:
            base = float(window_pct) if window_enabled else float(risk_window_pct)
            invert = name.endswith("n_positions") or "_n_positions" in name
            down_mult, up_mult = _risk_multipliers(direction, signed, invert=invert)
            new_bounds[param_name] = _tighten_bounds_around_value_asymmetric(
                low,
                high,
                value,
                pct_down=max(0.0, min(100.0, base * down_mult)),
                pct_up=max(0.0, min(100.0, base * up_mult)),
                step=step,
            )
            continue

        if window_enabled:
            new_bounds[param_name] = _tighten_bounds_around_value(low, high, value, window_pct, step)
        else:
            new_bounds[param_name] = bound_value

    return new_bounds


def build_optimize_preset(
    *,
    config_context: dict[str, Any],
    full_config_data: dict[str, Any],
    params: dict[str, Any],
    near_bounds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a version-aware optimize preset from a selected Pareto config."""
    metric_sets = get_optimize_metric_sets()
    currency_metrics = set(metric_sets.get("currency_metrics") or [])

    optimize_version = str(config_context.get("optimize_version") or "v7").lower()
    source_optimize = full_config_data.get("optimize", {}) if isinstance(full_config_data, dict) else {}
    if optimize_version == "v8" and isinstance(source_optimize, dict):
        bounds = _flatten_dotted_bounds(source_optimize.get("bounds") or {})
    else:
        bounds = copy.deepcopy(config_context.get("bounds") or {})
    bot_params = copy.deepcopy(config_context.get("bot_params") or {})
    suite_metrics = copy.deepcopy(config_context.get("suite_metrics") or {})
    optimize_settings = copy.deepcopy(config_context.get("optimize_settings") or {})

    direction = str(params.get("direction") or OPTIMIZE_PRESET_DIRECTIONS[0]).strip() or OPTIMIZE_PRESET_DIRECTIONS[0]
    if direction not in OPTIMIZE_PRESET_DIRECTIONS:
        direction = OPTIMIZE_PRESET_DIRECTIONS[0]
    if params.get("bounds_window_pct") is not None:
        bounds_window_pct = _clamp_int(params.get("bounds_window_pct"), 0, 0, 100)
    else:
        legacy_bounds_adjust = _clamp_int(params.get("bounds_adjust"), 0, -50, 50)
        # Legacy slider was centered around a 10% window; keep old API callers mapped to the direct percent control.
        bounds_window_pct = 0 if legacy_bounds_adjust == 0 else max(0, min(100, 10 + legacy_bounds_adjust))
    risk_adjust = _clamp_int(params.get("risk_adjust"), 0, -50, 50)
    window_pct = float(bounds_window_pct)
    show_near_bounds = bool(params.get("show_near_bounds", False))
    only_adjust_near_bounds = bool(params.get("only_adjust_near_bounds", False))
    expand_near_bounds = bool(params.get("expand_near_bounds", False))
    near_bounds_expand_pct = float(_clamp_int(params.get("near_bounds_expand_pct"), 25, 0, 100))
    hide_hard_limited_near = bool(params.get("hide_hard_limited_near", False))

    near_map = _normalize_near_map(near_bounds) if show_near_bounds or only_adjust_near_bounds else {}
    hidden_near_params: set[str] = set()
    if show_near_bounds and hide_hard_limited_near and near_map:
        if expand_near_bounds and near_bounds_expand_pct > 0:
            hide_notes: dict[str, str] = {}
            _build_new_bounds(
                bounds=bounds,
                bot_params=bot_params,
                direction=direction,
                risk_adjust=risk_adjust,
                window_pct=window_pct,
                near_map=near_map,
                expand_near_bounds=expand_near_bounds,
                near_bounds_expand_pct=near_bounds_expand_pct,
                only_near_bounds=False,
                apply_risk_adjustments=False,
                apply_window_adjustments=False,
                apply_near_expansion=True,
                expand_notes_out=hide_notes,
            )
            hidden_near_params = {param for param, note in hide_notes.items() if "clamped to 0" in str(note)}
        else:
            for param_name, info in near_map.items():
                low, _high, _step = _as_bound_tuple(bounds.get(param_name))
                if (info or {}).get("edge") == "lower" and low is not None and abs(low) <= 1e-12:
                    hidden_near_params.add(param_name)
        if hidden_near_params:
            near_map = {key: value for key, value in near_map.items() if key not in hidden_near_params}

    scoring_out, limits_out, _scheme = _build_scoring_and_limits(
        full_config_data=full_config_data,
        optimize_settings=optimize_settings,
        suite_metrics=suite_metrics,
        direction=direction,
        risk_adjust=risk_adjust,
        currency_metrics=currency_metrics,
        default_goal_map_override=(config_context.get("scoring_goals") or {}) if optimize_version == "v8" else None,
    )

    expand_notes: dict[str, str] = {}
    result_bounds = _build_new_bounds(
        bounds=bounds,
        bot_params=bot_params,
        direction=direction,
        risk_adjust=risk_adjust,
        window_pct=window_pct,
        near_map=near_map,
        expand_near_bounds=expand_near_bounds,
        near_bounds_expand_pct=near_bounds_expand_pct,
        only_near_bounds=only_adjust_near_bounds,
        expand_notes_out=expand_notes,
    )
    expand_bounds = _build_new_bounds(
        bounds=bounds,
        bot_params=bot_params,
        direction=direction,
        risk_adjust=risk_adjust,
        window_pct=window_pct,
        near_map=near_map,
        expand_near_bounds=expand_near_bounds,
        near_bounds_expand_pct=near_bounds_expand_pct,
        only_near_bounds=only_adjust_near_bounds,
        apply_risk_adjustments=False,
        apply_window_adjustments=False,
        apply_near_expansion=True,
        expand_notes_out=expand_notes,
    )
    window_bounds = _build_new_bounds(
        bounds=bounds,
        bot_params=bot_params,
        direction=direction,
        risk_adjust=risk_adjust,
        window_pct=window_pct,
        near_map=near_map,
        expand_near_bounds=expand_near_bounds,
        near_bounds_expand_pct=near_bounds_expand_pct,
        only_near_bounds=only_adjust_near_bounds,
        apply_risk_adjustments=False,
        apply_window_adjustments=True,
        apply_near_expansion=False,
    )
    risk_bounds = _build_new_bounds(
        bounds=bounds,
        bot_params=bot_params,
        direction=direction,
        risk_adjust=risk_adjust,
        window_pct=window_pct,
        near_map=near_map,
        expand_near_bounds=expand_near_bounds,
        near_bounds_expand_pct=near_bounds_expand_pct,
        only_near_bounds=only_adjust_near_bounds,
        apply_risk_adjustments=True,
        apply_window_adjustments=False,
        apply_near_expansion=False,
    )

    rows = []
    for key in sorted(set(bounds.keys()) | set(result_bounds.keys())):
        if show_near_bounds and hide_hard_limited_near and str(key) in hidden_near_params:
            continue
        before = bounds.get(key)
        result = result_bounds.get(key)
        if key not in bounds:
            change = "added"
        elif key not in result_bounds:
            change = "removed"
        else:
            change = "changed" if _normalize_bound_for_compare(before) != _normalize_bound_for_compare(result) else ""
        if not change and show_near_bounds and expand_near_bounds and near_bounds_expand_pct > 0 and str(key) in expand_notes:
            change = "limited"
        if not change:
            continue
        row = {
            "param": str(key),
            "change": change,
            "before": _bound_pretty(before),
            "expand": _bound_pretty(expand_bounds.get(key)),
            "window": _bound_pretty(window_bounds.get(key)),
            "risk": _bound_pretty(risk_bounds.get(key)),
            "result": _bound_pretty(result),
        }
        if show_near_bounds and expand_near_bounds and near_bounds_expand_pct > 0:
            row["expand_note"] = expand_notes.get(str(key), "")
        if show_near_bounds:
            info = near_map.get(str(key)) or {}
            row["near_edge"] = info.get("edge", "")
            row["near_value"] = info.get("value", "")
            row["near_bound"] = info.get("bound", "")
        rows.append(row)

    preset_config = copy.deepcopy(full_config_data or {})
    optimize = dict(preset_config.get("optimize") or {})
    optimize["bounds"] = _restore_dotted_bounds(result_bounds) if optimize_version == "v8" else result_bounds
    optimize["scoring"] = scoring_out
    optimize["limits"] = limits_out
    preset_config["optimize"] = optimize
    if optimize_version == "v8":
        pbgui = preset_config.get("pbgui")
        runtime = pbgui.get("optimize_runtime") if isinstance(pbgui, dict) else None
        if isinstance(runtime, dict) and runtime.get("mode") == "checkpoint_resume":
            pbgui.pop("optimize_runtime", None)

    return {
        "preset_config": preset_config,
        "scoring": scoring_out,
        "limits": limits_out,
        "bounds": result_bounds,
        "bounds_preview_rows": rows,
        "near_rows": _build_near_rows(near_map) if show_near_bounds else [],
        "hidden_near_params": sorted(hidden_near_params),
        "near_bounds_count": len(near_map),
        "only_adjust_near_bounds": only_adjust_near_bounds,
        "expand_notes": expand_notes,
        "window_pct": window_pct,
        "bounds_window_pct": bounds_window_pct,
        "risk_adjust": risk_adjust,
        "direction": direction,
    }
