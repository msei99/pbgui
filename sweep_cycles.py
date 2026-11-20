"""Deterministic PB8/PB9 Suite sweep-cycle evaluation."""

from __future__ import annotations

import copy
from datetime import date, timedelta
import math
from typing import Any


SWEEP_PLAN_FILENAME = ".pbgui_sweep_cycles.json"
SWEEP_METRIC_NAMES = (
    "sweep_net_cashflow",
    "sweep_total_swept",
    "sweep_external_capital",
    "sweep_cycles_completed",
    "sweep_refill_count",
    "sweep_final_balance",
    "sweep_target_hit_rate",
)
_GAIN_ALIASES = ("gain_strategy_eq", "gain_usd", "gain")
# PB9 has no gain metric. Its native CAGR is expm1(ln(E_end / E_start) / Y)
# with Y = (actual_end_timestamp_ms - start_timestamp_ms) / DAY_MS / year_days,
# taken from the scenario's own metric metadata. The window multiplier is only
# reconstructed when exactly one native source defines that clock.
PB9_CAGR_METRIC = "cagr_fraction_strategy_equity"
_DAY_MS = 86_400_000
# Maximum relative error of a reconstructed multiplier caused by the stored
# CAGR rounding alone; larger errors (CAGR close to -1) are not trustworthy.
_MAX_GAIN_RELATIVE_ERROR = 1e-9


def build_sweep_plan(config: dict[str, Any]) -> dict[str, Any] | None:
    """Extract one immutable validated sweep plan from a PBGui config snapshot."""
    if not isinstance(config, dict):
        return None
    pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
    template = pbgui.get("scenario_template") if isinstance(pbgui.get("scenario_template"), dict) else {}
    if template.get("template") != "sweep_cycles":
        return None
    parameters = template.get("parameters") if isinstance(template.get("parameters"), dict) else {}
    policy = parameters.get("sweep_policy") if isinstance(parameters.get("sweep_policy"), dict) else {}
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    if not bool(backtest.get("suite_enabled")):
        return None
    scenarios = backtest.get("scenarios") if isinstance(backtest.get("scenarios"), list) else []
    labels = []
    training_scenarios = []
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            continue
        label = str(scenario.get("label") or "").strip()
        if not label or label in labels:
            return None
        labels.append(label)
        training_scenarios.append(
            {
                key: copy.deepcopy(scenario[key])
                for key in ("label", "start_date", "end_date", "exchanges")
                if key in scenario
            }
        )
    if not training_scenarios:
        return None
    try:
        starting_balance = float(policy["starting_balance"])
        balance_multiplier = float(policy["balance_multiplier"])
        refill_cost = float(policy.get("refill_cost") or 0.0)
        cooldown_days = int(policy.get("cooldown_days") or 0)
        window_days = int(parameters["window_days"])
        stride_days = int(parameters["stride_days"])
    except (KeyError, TypeError, ValueError):
        return None
    numeric = (starting_balance, balance_multiplier, refill_cost)
    if not all(math.isfinite(value) for value in numeric):
        return None
    if starting_balance <= 0.0 or balance_multiplier <= 1.0 or refill_cost < 0.0 or cooldown_days < 0:
        return None
    if window_days <= 0 or stride_days < window_days + cooldown_days:
        return None
    holdouts = template.get("holdout_scenarios") if isinstance(template.get("holdout_scenarios"), list) else []
    holdout_scenarios = []
    for scenario in holdouts:
        if not isinstance(scenario, dict):
            continue
        holdout_scenarios.append(
            {
                key: copy.deepcopy(scenario[key])
                for key in ("label", "start_date", "end_date", "exchanges")
                if key in scenario
            }
        )
    return {
        "contract_version": 1,
        "template_version": int(template.get("template_version") or 1),
        "policy": {
            "starting_balance": starting_balance,
            "balance_multiplier": balance_multiplier,
            "refill_cost": refill_cost,
            "cooldown_days": cooldown_days,
        },
        "window_days": window_days,
        "stride_days": stride_days,
        "training_scenarios": training_scenarios,
        "holdout_count": len(holdout_scenarios),
        "holdout_scenarios": holdout_scenarios,
    }


def validate_sweep_plan(plan: object) -> dict[str, Any] | None:
    """Validate a persisted plan through the same extraction contract."""
    if not isinstance(plan, dict):
        return None
    raw_holdouts = plan.get("holdout_scenarios")
    if not isinstance(raw_holdouts, list):
        raw_holdouts = [{}] * max(0, int(plan.get("holdout_count") or 0))
    wrapper = {
        "pbgui": {
            "scenario_template": {
                "template": "sweep_cycles",
                "template_version": plan.get("template_version"),
                "parameters": {
                    "window_days": plan.get("window_days"),
                    "stride_days": plan.get("stride_days"),
                    "sweep_policy": plan.get("policy"),
                },
                "holdout_scenarios": raw_holdouts,
            }
        },
        "backtest": {"suite_enabled": True, "scenarios": plan.get("training_scenarios")},
    }
    return build_sweep_plan(wrapper)


def sweep_holdout_scenarios(plan: object) -> list[dict[str, Any]]:
    """Return explicit holdouts or derive them after the final training window."""
    normalized = validate_sweep_plan(plan)
    if normalized is None or not normalized["holdout_count"]:
        return []
    explicit = normalized.get("holdout_scenarios") or []
    if len(explicit) == normalized["holdout_count"] and all(
        isinstance(item.get("start_date"), str) and isinstance(item.get("end_date"), str)
        for item in explicit
    ):
        return copy.deepcopy(explicit)
    training = normalized["training_scenarios"]
    try:
        last_end = date.fromisoformat(str(training[-1]["end_date"]))
    except (KeyError, TypeError, ValueError):
        return []
    window_days = int(normalized["window_days"])
    stride_days = int(normalized["stride_days"])
    result = []
    for index in range(normalized["holdout_count"]):
        window_end = last_end + timedelta(days=(index + 1) * stride_days)
        window_start = window_end - timedelta(days=window_days - 1)
        source = explicit[index] if index < len(explicit) else {}
        entry = {
            "label": str(source.get("label") or f"holdout_{index + 1:02d}"),
            "start_date": window_start.isoformat(),
            "end_date": window_end.isoformat(),
        }
        if isinstance(source.get("exchanges"), list):
            entry["exchanges"] = copy.deepcopy(source["exchanges"])
        result.append(entry)
    return result


def _scenario_metric(suite_values: dict[str, Any], metric: str, label: str) -> float | None:
    """Return one finite per-scenario value."""
    payload = suite_values.get(metric) if isinstance(suite_values, dict) else None
    scenarios = payload.get("scenarios") if isinstance(payload, dict) else None
    value = scenarios.get(label) if isinstance(scenarios, dict) else None
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
        return float(value)
    return None


def _native_source_years(sources: object, label: str) -> tuple[float | None, str | None]:
    """Return the exact native evaluation years of one single-source scenario."""
    per_label = sources.get(label) if isinstance(sources, dict) else None
    if not isinstance(per_label, dict) or not per_label:
        return None, f"No native evaluation clock for {label}"
    if len(per_label) != 1:
        return None, f"{label} averages {len(per_label)} native sources; its window gain cannot be reconstructed from CAGR"
    metadata = next(iter(per_label.values()))
    if not isinstance(metadata, dict):
        return None, f"No native evaluation clock for {label}"
    start = metadata.get("start_timestamp_ms")
    end = metadata.get("actual_end_timestamp_ms")
    year_days = metadata.get("year_days")
    if (
        any(isinstance(value, bool) or not isinstance(value, int) for value in (start, end))
        or isinstance(year_days, bool)
        or not isinstance(year_days, (int, float))
        or not math.isfinite(float(year_days))
        or float(year_days) <= 0.0
        or start < 0
        or end <= start
    ):
        return None, f"No valid native evaluation clock for {label}"
    return (end - start) / _DAY_MS / float(year_days), None


def _cagr_gain(cagr: float, years: float, label: str) -> tuple[float | None, str | None]:
    """Invert the native CAGR when the stored value still determines the multiplier."""
    if cagr <= -1.0:
        return None, f"{PB9_CAGR_METRIC}={cagr!r} for {label} does not determine the window gain"
    relative_error = years * (math.ulp(cagr) / 2.0) / (1.0 + cagr)
    if not math.isfinite(relative_error) or relative_error > _MAX_GAIN_RELATIVE_ERROR:
        return None, f"{PB9_CAGR_METRIC}={cagr!r} for {label} is too close to -1 to reconstruct the window gain"
    try:
        gain = math.exp(math.log1p(cagr) * years)
    except OverflowError:
        gain = math.inf
    if not math.isfinite(gain):
        return None, f"Window gain for {label} overflows"
    return gain, None


def _scenario_gain(
    suite_values: dict[str, Any], label: str, sources: object = None
) -> tuple[str | None, float | None, str | None]:
    """Return (metric, finite window multiplier, unavailable reason)."""
    for metric in _GAIN_ALIASES:
        value = _scenario_metric(suite_values, metric, label)
        if value is not None:
            return metric, value, None
    cagr = _scenario_metric(suite_values, PB9_CAGR_METRIC, label)
    if cagr is None:
        return None, None, f"No per-scenario gain metric for {label}"
    years, reason = _native_source_years(sources, label)
    if years is None:
        return PB9_CAGR_METRIC, None, reason
    gain, reason = _cagr_gain(cagr, years, label)
    return PB9_CAGR_METRIC, gain, reason


def evaluate_sweep_cycles(
    plan: object, suite_values: dict[str, Any], scenario_sources: object = None
) -> dict[str, Any]:
    """Evaluate sequential window gains using reset, sweep, and refill rules.

    ``scenario_sources`` is the native ``suite_metrics.sources`` mapping
    (label -> source -> metric metadata) required for PB9 CAGR inversion.
    """
    normalized = validate_sweep_plan(plan)
    if normalized is None:
        return {"available": False, "reason": "Sweep plan is invalid"}
    policy = normalized["policy"]
    starting_balance = float(policy["starting_balance"])
    target_balance = starting_balance * float(policy["balance_multiplier"])
    if not math.isfinite(target_balance):
        return {"available": False, "reason": "Sweep target balance overflows"}
    refill_cost = float(policy["refill_cost"])
    balance = starting_balance
    total_swept = 0.0
    external_capital = 0.0
    completed_cycles = 0
    refill_count = 0
    windows = []
    metric_used = None
    for scenario in normalized["training_scenarios"]:
        label = str(scenario["label"])
        metric, gain, reason = _scenario_gain(suite_values, label, scenario_sources)
        if gain is None:
            return {"available": False, "reason": reason}
        metric_used = metric_used or metric
        opening_balance = balance
        ending_balance = max(0.0, opening_balance * gain)
        swept = 0.0
        refill = 0.0
        cost = 0.0
        action = "carry"
        if ending_balance >= target_balance:
            swept = max(0.0, ending_balance - starting_balance)
            total_swept += swept
            completed_cycles += 1
            balance = starting_balance
            action = "sweep_reset"
        elif ending_balance < starting_balance:
            refill = starting_balance - ending_balance
            cost = refill_cost
            external_capital += refill + cost
            refill_count += 1
            balance = starting_balance
            action = "refill_reset"
        else:
            balance = ending_balance
        if not all(math.isfinite(value) for value in (ending_balance, total_swept, external_capital, balance)):
            return {"available": False, "reason": f"Sweep balances overflow at {label}"}
        windows.append(
            {
                "label": label,
                "start_date": scenario.get("start_date"),
                "end_date": scenario.get("end_date"),
                "gain": gain,
                "opening_balance": opening_balance,
                "ending_balance": ending_balance,
                "action": action,
                "swept": swept,
                "refill": refill,
                "refill_cost": cost,
                "next_balance": balance,
            }
        )
    window_count = len(windows)
    summary = {
        "sweep_net_cashflow": total_swept - external_capital,
        "sweep_total_swept": total_swept,
        "sweep_external_capital": external_capital,
        "sweep_cycles_completed": float(completed_cycles),
        "sweep_refill_count": float(refill_count),
        "sweep_final_balance": balance,
        "sweep_target_hit_rate": completed_cycles / window_count if window_count else 0.0,
    }
    return {
        "available": True,
        "gain_metric": metric_used,
        "target_balance": target_balance,
        "policy": copy.deepcopy(policy),
        "summary": summary,
        "windows": windows,
        "holdout_count": normalized["holdout_count"],
        "holdout_scenarios": sweep_holdout_scenarios(normalized),
        "holdout_status": "pending_separate_validation" if normalized["holdout_count"] else "not_configured",
    }
