"""Context-safe preprocessing for official PB7 to PB8 config migration."""

from __future__ import annotations

import copy
from typing import Any


_LEGACY_CHURN_DISTANCE_KEYS = (
    "price_distance_threshold",
    "initial_entry_exec_max_market_dist_pct",
)
_RETIRED_CHURN_TRACKING_KEY = "order_replacement_churn_gate_tracking_tolerance_pct"
_CANONICAL_CHURN_KEYS = (
    "order_replacement_churn_gate_activation_count",
    "order_replacement_churn_gate_market_dist_pct",
    "order_replacement_churn_gate_stability_minutes",
    "order_replacement_churn_gate_window_minutes",
)


def extract_legacy_churn_gate(config: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None, list[str]]:
    """Remove legacy churn inputs so PB8 can migrate them after V7 shape conversion."""
    candidate = copy.deepcopy(config)
    live = candidate.get("live")
    if not isinstance(live, dict):
        return candidate, None, []

    present = [(key, live[key]) for key in _LEGACY_CHURN_DISTANCE_KEYS if key in live]
    if len(present) == 2 and present[0][1] != present[1][1]:
        raise ValueError(
            "live.price_distance_threshold conflicts with "
            "live.initial_entry_exec_max_market_dist_pct; remove one or make both values equal"
        )

    adjustments = []
    plan = None
    if present:
        source_key, value = present[-1]
        plan = {
            "source_field": f"live.{source_key}",
            "value": copy.deepcopy(value),
            "explicit_churn_keys": [key for key in _CANONICAL_CHURN_KEYS if key in live],
        }
        for key, _value in present:
            live.pop(key, None)
            adjustments.append(f"live.{key} -> PB8 canonical churn-gate migration")

    if _RETIRED_CHURN_TRACKING_KEY in live:
        live.pop(_RETIRED_CHURN_TRACKING_KEY, None)
        adjustments.append(
            f"live.{_RETIRED_CHURN_TRACKING_KEY} (retired; churn history uses order_match_tolerance_pct)"
        )

    return candidate, plan, adjustments


def apply_legacy_churn_gate(config: dict[str, Any], plan: dict[str, Any] | None) -> dict[str, Any]:
    """Inject one legacy value into canonical V8 shape for PB8's own migration pass."""
    candidate = copy.deepcopy(config)
    if plan is None:
        return candidate
    live = candidate.setdefault("live", {})
    if not isinstance(live, dict):
        raise ValueError("Migrated V8 config live section must be an object")
    explicit_keys = set(plan.get("explicit_churn_keys") or [])
    for key in _CANONICAL_CHURN_KEYS:
        if key not in explicit_keys:
            live.pop(key, None)
    live["initial_entry_exec_max_market_dist_pct"] = copy.deepcopy(plan.get("value"))
    return candidate


def sanitize_optimize_migration_source(config: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Remove PBGui metadata and redundant V7 Optimize defaults before PB8 migration."""
    candidate = copy.deepcopy(config)
    adjustments = []
    if "pbgui" in candidate:
        candidate.pop("pbgui", None)
        adjustments.append("pbgui")
    optimize = candidate.get("optimize")
    if isinstance(optimize, dict) and "max_pending_starting_evals_per_cpu" in optimize:
        value = optimize.get("max_pending_starting_evals_per_cpu")
        if value is None or (
            isinstance(value, (int, float)) and not isinstance(value, bool) and value == 1
        ):
            optimize.pop("max_pending_starting_evals_per_cpu", None)
            adjustments.append("optimize.max_pending_starting_evals_per_cpu (retired default 1)")
    return candidate, adjustments
