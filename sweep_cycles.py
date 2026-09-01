"""Deterministic PB8 Suite sweep-cycle evaluation."""

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


def _scenario_gain(suite_values: dict[str, Any], label: str) -> tuple[str | None, float | None]:
    """Return the first canonical finite per-scenario gain value."""
    for metric in _GAIN_ALIASES:
        payload = suite_values.get(metric) if isinstance(suite_values, dict) else None
        scenarios = payload.get("scenarios") if isinstance(payload, dict) else None
        value = scenarios.get(label) if isinstance(scenarios, dict) else None
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
            return metric, float(value)
    return None, None


def evaluate_sweep_cycles(plan: object, suite_values: dict[str, Any]) -> dict[str, Any]:
    """Evaluate sequential window gains using reset, sweep, and refill rules."""
    normalized = validate_sweep_plan(plan)
    if normalized is None:
        return {"available": False, "reason": "Sweep plan is invalid"}
    policy = normalized["policy"]
    starting_balance = float(policy["starting_balance"])
    target_balance = starting_balance * float(policy["balance_multiplier"])
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
        metric, gain = _scenario_gain(suite_values, label)
        if gain is None:
            return {"available": False, "reason": f"No per-scenario gain metric for {label}"}
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
