"""Context-safe preprocessing for official PB7 to PB8 config migration."""

from __future__ import annotations

import copy
import math
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
_OPTIMIZER_OVERRIDE_STRATEGIES = {
    "lossless_close_trailing": "trailing_martingale",
    "forward_tp_grid": "trailing_grid_v7",
    "backward_tp_grid": "trailing_grid_v7",
}
_V7_FIXED_RUNTIME_GROUP_KEYS = {
    "risk": {
        "risk_entry_cooldown_minutes": "entry_cooldown_minutes",
        "n_positions": "n_positions",
        "total_wallet_exposure_limit": "total_wallet_exposure_limit",
        "risk_twel_entry_gate_enabled": "total_exposure_entry_gate_enabled",
        "risk_twel_enforcer_enabled": "total_exposure_enforcer_enabled",
        "risk_twel_enforcer_policy": "total_exposure_enforcer_policy",
        "risk_twel_enforcer_threshold": "total_exposure_enforcer_threshold",
        "risk_we_excess_allowance_pct": "we_excess_allowance_pct",
        "risk_we_excess_allowance_mode": "we_excess_allowance_mode",
        "risk_wel_enforcer_enabled": "position_exposure_enforcer_enabled",
        "risk_wel_enforcer_threshold": "position_exposure_enforcer_threshold",
    },
    "forager": {
        "forager_score_weights": "score_weights",
        "forager_volatility_ema_span_1m": "volatility_ema_span_1m",
        "forager_volume_drop_pct": "volume_drop_pct",
        "forager_volume_ema_span_1m": "volume_ema_span_1m",
    },
    "hsl": {
        "hsl_cooldown_minutes_after_red": "cooldown_minutes_after_red",
        "hsl_ema_span_minutes": "ema_span_minutes",
        "hsl_enabled": "enabled",
        "hsl_no_restart_drawdown_threshold": "no_restart_drawdown_threshold",
        "hsl_orange_tier_mode": "orange_tier_mode",
        "hsl_panic_close_order_type": "panic_close_order_type",
        "hsl_red_threshold": "red_threshold",
        "hsl_restart_after_red_policy": "restart_after_red_policy",
        "hsl_tier_ratios": "tier_ratios",
    },
    "unstuck": {
        "unstuck_close_pct": "close_pct",
        "unstuck_ema_dist": "ema_dist",
        "unstuck_ema_gating_enabled": "ema_gating_enabled",
        "unstuck_enabled": "enabled",
        "unstuck_loss_allowance_pct": "loss_allowance_pct",
        "unstuck_threshold": "threshold",
    },
}
_REVIEW_INSERTED_DEFAULT_SUFFIXES = (
    ".risk.entry_cooldown_minutes",
    ".risk.we_excess_allowance_mode",
    ".hsl.restart_after_red_policy",
    ".unstuck.enabled",
    ".risk.position_exposure_enforcer_enabled",
    ".risk.total_exposure_enforcer_enabled",
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


def _append_unique(report: dict[str, Any], key: str, value: Any) -> None:
    items = report.setdefault(key, [])
    if value not in items:
        items.append(value)


def _path_exists(config: dict[str, Any], dotted_path: str) -> bool:
    current: Any = config
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def _canonical_fixed_runtime_path(config: dict[str, Any], raw_path: str) -> str | None:
    path = str(raw_path or "").strip()
    if _path_exists(config, path):
        return path
    parts = path.split(".")
    if parts and parts[0] in {"long", "short"}:
        canonical = ".".join(("bot", *parts))
        if _path_exists(config, canonical):
            return canonical
    if (
        len(parts) >= 4
        and parts[0] == "bot"
        and parts[1] in {"long", "short"}
        and parts[2] == "strategy"
    ):
        live = config.get("live")
        strategy_kind = str(live.get("strategy_kind") or "").strip() if isinstance(live, dict) else ""
        canonical = ".".join((*parts[:3], strategy_kind, *parts[3:]))
        if strategy_kind and _path_exists(config, canonical):
            return canonical
    if len(parts) != 3 or parts[0] != "bot" or parts[1] not in {"long", "short"}:
        return None
    flat_key = parts[2]
    for group_name, field_map in _V7_FIXED_RUNTIME_GROUP_KEYS.items():
        local_key = field_map.get(flat_key)
        if local_key is None:
            continue
        canonical = f"bot.{parts[1]}.{group_name}.{local_key}"
        return canonical if _path_exists(config, canonical) else None
    return None


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _source_side_value(source_side: dict[str, Any], flat_key: str, nested_key: str) -> tuple[bool, Any]:
    if flat_key in source_side:
        return True, source_side[flat_key]
    risk = source_side.get("risk")
    if isinstance(risk, dict) and nested_key in risk:
        return True, risk[nested_key]
    return False, None


def _record_adjustment(report: dict[str, Any], code: str, path: str, detail: str) -> None:
    _append_unique(
        report,
        "pbgui_post_migration_adjustments",
        {"code": code, "path": path, "detail": detail},
    )


def _filter_strategy_overrides(config: dict[str, Any], report: dict[str, Any]) -> None:
    optimize = config.get("optimize")
    live = config.get("live")
    if not isinstance(optimize, dict) or not isinstance(optimize.get("enable_overrides"), list):
        return
    strategy_kind = str(live.get("strategy_kind") or "").strip() if isinstance(live, dict) else ""
    filtered = []
    seen = set()
    for raw_value in optimize["enable_overrides"]:
        if not isinstance(raw_value, str):
            filtered.append(raw_value)
            continue
        value = raw_value.strip()
        if value in seen:
            continue
        seen.add(value)
        required = _OPTIMIZER_OVERRIDE_STRATEGIES.get(value)
        if required and required != strategy_kind:
            _record_adjustment(
                report,
                "drop_incompatible_optimizer_override",
                "optimize.enable_overrides",
                f"removed {value!r}; requires live.strategy_kind={required!r}, got {strategy_kind!r}",
            )
            continue
        filtered.append(value)
    optimize["enable_overrides"] = filtered


def _canonicalize_fixed_runtime_overrides(config: dict[str, Any], report: dict[str, Any]) -> None:
    optimize = config.get("optimize")
    overrides = optimize.get("fixed_runtime_overrides") if isinstance(optimize, dict) else None
    if not isinstance(overrides, dict):
        return
    canonicalized = {}
    source_by_canonical = {}
    for raw_path, value in overrides.items():
        path = str(raw_path or "").strip()
        canonical = _canonical_fixed_runtime_path(config, path)
        if canonical is None:
            _append_unique(report, "manual_review_fields", f"optimize.fixed_runtime_overrides.{path}")
            _append_unique(
                report,
                "pbgui_post_migration_review_fields",
                f"optimize.fixed_runtime_overrides.{path}",
            )
            _append_unique(
                report,
                "warnings",
                f"optimize.fixed_runtime_overrides path {path!r} could not be resolved to a canonical V8 config path.",
            )
            continue
        if canonical in canonicalized and canonicalized[canonical] != value:
            previous = source_by_canonical[canonical]
            _append_unique(report, "manual_review_fields", f"optimize.fixed_runtime_overrides.{path}")
            _append_unique(
                report,
                "pbgui_post_migration_review_fields",
                f"optimize.fixed_runtime_overrides.{path}",
            )
            _append_unique(
                report,
                "warnings",
                f"optimize.fixed_runtime_overrides paths {previous!r} and {path!r} conflict for {canonical!r}.",
            )
            continue
        canonicalized[canonical] = copy.deepcopy(value)
        source_by_canonical[canonical] = path
        if canonical != path:
            _record_adjustment(
                report,
                "canonicalize_fixed_runtime_override",
                f"optimize.fixed_runtime_overrides.{path}",
                f"rewrote to {canonical}",
            )
    optimize["fixed_runtime_overrides"] = canonicalized


def _fix_disabled_side_search(config: dict[str, Any], report: dict[str, Any]) -> None:
    bot = config.get("bot")
    optimize = config.get("optimize")
    fixed_params = optimize.get("fixed_params") if isinstance(optimize, dict) else None
    if not isinstance(bot, dict) or not isinstance(fixed_params, list):
        return
    for side in ("long", "short"):
        side_config = bot.get(side)
        risk = side_config.get("risk") if isinstance(side_config, dict) else None
        if not isinstance(risk, dict):
            continue
        n_positions = _finite_number(risk.get("n_positions"))
        exposure = _finite_number(risk.get("total_wallet_exposure_limit"))
        selector = f"bot.{side}"
        if n_positions is None or exposure is None or n_positions > 0.0 or exposure > 0.0 or selector in fixed_params:
            continue
        fixed_params.append(selector)
        _record_adjustment(
            report,
            "freeze_disabled_side",
            "optimize.fixed_params",
            f"added {selector!r} because n_positions and total_wallet_exposure_limit are both disabled",
        )


def _enable_implicit_v7_enforcers(
    source: dict[str, Any],
    config: dict[str, Any],
    report: dict[str, Any],
) -> None:
    source_bot = source.get("bot")
    target_bot = config.get("bot")
    if not isinstance(source_bot, dict) or not isinstance(target_bot, dict):
        return
    pairs = (
        (
            "risk_wel_enforcer_threshold",
            "position_exposure_enforcer_threshold",
            "risk_wel_enforcer_enabled",
            "position_exposure_enforcer_enabled",
        ),
        (
            "risk_twel_enforcer_threshold",
            "total_exposure_enforcer_threshold",
            "risk_twel_enforcer_enabled",
            "total_exposure_enforcer_enabled",
        ),
    )
    for side in ("long", "short"):
        source_side = source_bot.get(side)
        target_side = target_bot.get(side)
        target_risk = target_side.get("risk") if isinstance(target_side, dict) else None
        if not isinstance(source_side, dict) or not isinstance(target_risk, dict):
            continue
        for flat_threshold, target_threshold, flat_enabled, target_enabled in pairs:
            enabled_present, _enabled_value = _source_side_value(source_side, flat_enabled, target_enabled)
            threshold_present, threshold_value = _source_side_value(source_side, flat_threshold, target_threshold)
            threshold = _finite_number(threshold_value)
            if enabled_present or not threshold_present or threshold is None or threshold <= 0.0:
                continue
            if target_risk.get(target_enabled) is True:
                continue
            target_risk[target_enabled] = True
            path = f"bot.{side}.risk.{target_enabled}"
            _record_adjustment(
                report,
                "enable_implicit_v7_enforcer",
                path,
                f"set true because V7 {flat_threshold}={threshold_value!r} was implicitly active",
            )
            _append_unique(
                report,
                "behavior_change_warnings",
                f"{path} was enabled to preserve V7's implicit positive-threshold enforcer behavior.",
            )


def _append_v7_excess_allowance_decisions(
    source: dict[str, Any],
    config: dict[str, Any],
    report: dict[str, Any],
) -> None:
    source_bot = source.get("bot")
    target_bot = config.get("bot")
    if not isinstance(source_bot, dict) or not isinstance(target_bot, dict):
        return

    def clamp_details(risk: dict[str, Any]) -> dict[str, float] | None:
        excess = _finite_number(risk.get("we_excess_allowance_pct"))
        twel = _finite_number(risk.get("total_wallet_exposure_limit"))
        n_positions = _finite_number(risk.get("n_positions"))
        explicit_wel = _finite_number(risk.get("wallet_exposure_limit"))
        if excess is None or twel is None or n_positions is None or excess <= 0.0 or twel <= 0.0 or n_positions <= 0.0:
            return None
        base_wel = explicit_wel if explicit_wel is not None and explicit_wel > 0.0 else twel / n_positions
        raw_allowed_wel = base_wel * (1.0 + excess)
        if raw_allowed_wel <= twel:
            return None
        return {
            "excess_allowance_pct": excess,
            "base_wel": base_wel,
            "raw_allowed_wel": raw_allowed_wel,
            "side_twel": twel,
        }

    coin_overrides = config.get("coin_overrides")
    for side in ("long", "short"):
        source_side = source_bot.get(side)
        target_side = target_bot.get(side)
        target_risk = target_side.get("risk") if isinstance(target_side, dict) else None
        if not isinstance(source_side, dict) or not isinstance(target_risk, dict):
            continue
        mode_present, _source_mode = _source_side_value(
            source_side,
            "risk_we_excess_allowance_mode",
            "we_excess_allowance_mode",
        )
        if mode_present or target_risk.get("we_excess_allowance_mode") != "bounded":
            continue
        affected = []
        base_details = clamp_details(target_risk)
        if base_details is not None:
            affected.append({"context": f"bot.{side}.risk", **base_details})
        if isinstance(coin_overrides, dict):
            for coin, override in coin_overrides.items():
                override_bot = override.get("bot") if isinstance(override, dict) else None
                override_side = override_bot.get(side) if isinstance(override_bot, dict) else None
                override_risk = override_side.get("risk") if isinstance(override_side, dict) else None
                if not isinstance(override_risk, dict):
                    continue
                merged_risk = copy.deepcopy(target_risk)
                merged_risk.update(override_risk)
                if "wallet_exposure_limit" in override_side:
                    merged_risk["wallet_exposure_limit"] = override_side["wallet_exposure_limit"]
                details = clamp_details(merged_risk)
                if details is not None:
                    affected.append(
                        {"context": f"coin_overrides.{coin}.bot.{side}.risk", **details}
                    )
        if not affected:
            continue
        worst = max(affected, key=lambda item: item["raw_allowed_wel"] - item["side_twel"])
        path = f"bot.{side}.risk.we_excess_allowance_mode"
        _append_unique(report, "manual_review_fields", path)
        _append_unique(report, "pbgui_post_migration_review_fields", path)
        _append_unique(
            report,
            "pbgui_review_decisions",
            {
                "code": "we_excess_allowance_mode",
                "path": path,
                "side": side,
                "current_value": "bounded",
                "v7_parity_value": "legacy_raw",
                "excess_allowance_pct": worst["excess_allowance_pct"],
                "base_wel": worst["base_wel"],
                "raw_allowed_wel": worst["raw_allowed_wel"],
                "side_twel": worst["side_twel"],
                "affected_contexts": [item["context"] for item in affected],
            },
        )


def _append_minimum_coin_age_decision(
    source: dict[str, Any],
    config: dict[str, Any],
    report: dict[str, Any],
) -> None:
    source_live = source.get("live")
    target_live = config.get("live")
    if not isinstance(source_live, dict) or not isinstance(target_live, dict):
        return
    if "minimum_coin_age_days" not in source_live:
        return
    source_days = _finite_number(source_live.get("minimum_coin_age_days"))
    target_days = _finite_number(target_live.get("minimum_coin_age_days"))
    if source_days is None or target_days is None or source_days <= 0.0 or target_days <= 0.0:
        return
    path = "live.minimum_coin_age_days"
    _append_unique(report, "manual_review_fields", path)
    _append_unique(report, "pbgui_post_migration_review_fields", path)
    _append_unique(
        report,
        "pbgui_review_decisions",
        {
            "code": "minimum_coin_age_days",
            "path": path,
            "current_value": target_days,
            "v7_parity_value": 0.0,
            "pb8_age_gate_value": target_days,
        },
    )


def _metric_names(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    names = []
    for item in items:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and isinstance(item.get("metric"), str):
            names.append(item["metric"])
    return names


def _append_optimize_review_warnings(config: dict[str, Any], report: dict[str, Any]) -> None:
    optimize = config.get("optimize")
    if not isinstance(optimize, dict):
        return
    warnings = []
    scoring = _metric_names(optimize.get("scoring"))
    weighted = [name for name in scoring if name.endswith("_w") or "_w_" in name]
    if weighted and "adg_strategy_eq" not in scoring:
        warnings.append(
            "Optimize scoring contains weighted objectives but no unweighted adg_strategy_eq objective; review whether that matches the intended V8 search policy."
        )
    limits = optimize.get("limits")
    if isinstance(limits, list):
        floor_metrics = sorted(
            {
                str(item.get("metric"))
                for item in limits
                if isinstance(item, dict)
                and str(item.get("metric") or "").startswith(("adg", "mdg"))
                and str(item.get("penalize_if") or "") == "less_than"
            }
        )
        if floor_metrics:
            warnings.append(
                "Optimize limits contain ADG/MDG floors that may be unreachable penalties after migration: "
                + ", ".join(floor_metrics)
                + "."
            )
    inserted = [
        str(path)
        for path in report.get("inserted_v8_defaults") or []
        if str(path).endswith(_REVIEW_INSERTED_DEFAULT_SUFFIXES)
    ]
    if inserted:
        warnings.append(
            "Review V8 defaults inserted for behavior-affecting fields: " + ", ".join(inserted) + "."
        )
    bounds = optimize.get("bounds")
    tight_cooldowns = []
    if isinstance(bounds, dict):
        for side in ("long", "short"):
            risk = bounds.get(side, {}).get("risk") if isinstance(bounds.get(side), dict) else None
            value = risk.get("entry_cooldown_minutes") if isinstance(risk, dict) else None
            if isinstance(value, (list, tuple)) and len(value) >= 2:
                low = _finite_number(value[0])
                high = _finite_number(value[1])
                if low is not None and high is not None and abs(high - low) <= 1e-12:
                    tight_cooldowns.append(f"optimize.bounds.{side}.risk.entry_cooldown_minutes={list(value)!r}")
    if tight_cooldowns:
        warnings.append(
            "Review fixed V8 entry cooldown bounds: " + ", ".join(tight_cooldowns) + "."
        )
    for warning in warnings:
        _append_unique(report, "warnings", warning)


def postprocess_v7_migration(
    source_v7: dict[str, Any],
    migrated_v8: dict[str, Any],
    report: dict[str, Any],
    *,
    require_v7_excess_review: bool = False,
    require_minimum_coin_age_review: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply deterministic PBGui safety fixes after PB8's official V7 migration."""
    config = copy.deepcopy(migrated_v8)
    updated_report = copy.deepcopy(report)
    _filter_strategy_overrides(config, updated_report)
    _canonicalize_fixed_runtime_overrides(config, updated_report)
    _fix_disabled_side_search(config, updated_report)
    _enable_implicit_v7_enforcers(source_v7, config, updated_report)
    if require_v7_excess_review:
        _append_v7_excess_allowance_decisions(source_v7, config, updated_report)
    if require_minimum_coin_age_review:
        _append_minimum_coin_age_decision(source_v7, config, updated_report)
    _append_optimize_review_warnings(config, updated_report)
    unresolved = bool(
        updated_report.get("manual_review_fields")
        or updated_report.get("dropped_unsupported_fields")
    )
    updated_report["manual_review_required"] = unresolved
    if unresolved:
        updated_report["status"] = "manual_review_required"
    elif updated_report.get("pbgui_post_migration_adjustments"):
        updated_report["status"] = "ok_with_adjustments"
    return config, updated_report
