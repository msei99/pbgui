"""Deterministic PB8 optimization scenario template generation."""

from __future__ import annotations

from datetime import date, timedelta
import re
from typing import Any


SERVICE = "ScenarioTemplates"

TEMPLATE_VERSION = 1
MAX_SCENARIOS = 64
MAX_WINDOWS = 48
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_EXCHANGE_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")

TEMPLATE_DESCRIPTORS = (
    {
        "id": "rolling_windows",
        "label": "Rolling Windows",
        "description": "Overlapping or stepped training windows across a fixed date range.",
        "supports_holdout": False,
        "supports_sweep_policy": False,
    },
    {
        "id": "walk_forward",
        "label": "Walk-Forward",
        "description": "Chronological training windows followed by untouched holdout windows.",
        "supports_holdout": True,
        "supports_sweep_policy": False,
    },
    {
        "id": "sweep_cycles",
        "label": "Sweep Cycles",
        "description": "Sequential non-overlapping windows with deterministic sweep, reset, and refill result evaluation.",
        "supports_holdout": True,
        "supports_sweep_policy": True,
    },
)
_TEMPLATE_IDS = {item["id"] for item in TEMPLATE_DESCRIPTORS}


class ScenarioTemplateError(ValueError):
    """Raised when a scenario template request is invalid."""


def list_scenario_templates() -> list[dict[str, Any]]:
    """Return the supported deterministic scenario template descriptors."""
    return [dict(item) for item in TEMPLATE_DESCRIPTORS]


def _parse_date(value: Any, field: str) -> date:
    """Parse one strict ISO date field."""
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ScenarioTemplateError(f"{field} must be an ISO date (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ScenarioTemplateError(f"{field} must be a valid calendar date") from exc


def _bounded_int(payload: dict[str, Any], field: str, default: int, minimum: int, maximum: int) -> int:
    """Read an integer parameter with explicit bounds."""
    value = payload.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScenarioTemplateError(f"{field} must be an integer")
    if value < minimum or value > maximum:
        raise ScenarioTemplateError(f"{field} must be between {minimum} and {maximum}")
    return value


def _bounded_number(
    payload: dict[str, Any], field: str, default: float, minimum: float, maximum: float
) -> float:
    """Read a finite numeric parameter with explicit bounds."""
    value = payload.get(field, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScenarioTemplateError(f"{field} must be a number")
    value = float(value)
    if value != value or value in (float("inf"), float("-inf")) or value < minimum or value > maximum:
        raise ScenarioTemplateError(f"{field} must be between {minimum:g} and {maximum:g}")
    return value


def _normalize_exchanges(payload: dict[str, Any], exchange_mode: str) -> list[str]:
    """Validate and normalize the optional per-exchange expansion list."""
    raw = payload.get("exchanges", [])
    if not isinstance(raw, list):
        raise ScenarioTemplateError("exchanges must be a list")
    exchanges: list[str] = []
    for value in raw:
        if not isinstance(value, str) or not _EXCHANGE_RE.fullmatch(value):
            raise ScenarioTemplateError("each exchange must contain only letters, numbers, '_' or '-'")
        normalized = value.lower()
        if normalized not in exchanges:
            exchanges.append(normalized)
    if len(exchanges) > 8:
        raise ScenarioTemplateError("at most 8 exchanges may be selected")
    if exchange_mode == "per_exchange" and not exchanges:
        raise ScenarioTemplateError("per_exchange mode requires at least one exchange")
    return exchanges


def _window_scenario(window_start: date, window_end: date, label: str, exchange: str | None) -> dict[str, Any]:
    """Build one PB8 backtest scenario object."""
    scenario: dict[str, Any] = {
        "label": label,
        "start_date": window_start.isoformat(),
        "end_date": window_end.isoformat(),
    }
    if exchange is not None:
        scenario["exchanges"] = [exchange]
    return scenario


def generate_scenario_template(payload: dict[str, Any]) -> dict[str, Any]:
    """Generate a deterministic PB8 training/holdout scenario preview."""
    if not isinstance(payload, dict):
        raise ScenarioTemplateError("request body must be an object")

    template = payload.get("template", "rolling_windows")
    if template not in _TEMPLATE_IDS:
        raise ScenarioTemplateError(f"template must be one of {', '.join(sorted(_TEMPLATE_IDS))}")

    start_date = _parse_date(payload.get("start_date"), "start_date")
    end_date = _parse_date(payload.get("end_date"), "end_date")
    if start_date > end_date:
        raise ScenarioTemplateError("start_date must not be after end_date")

    window_days = _bounded_int(payload, "window_days", 90, 1, 3650)
    auto_windows = template == "sweep_cycles" and payload.get("auto_windows") is True
    stride_days = window_days if auto_windows else _bounded_int(payload, "stride_days", 30, 1, 3650)
    training_windows = 1 if auto_windows else _bounded_int(payload, "training_windows", 4, 1, MAX_WINDOWS)
    holdout_default = 0 if template == "rolling_windows" else 1
    holdout_windows = _bounded_int(payload, "holdout_windows", holdout_default, 0, 16)
    if template == "rolling_windows" and holdout_windows:
        raise ScenarioTemplateError("rolling_windows does not support holdout windows")

    exchange_mode = payload.get("exchange_mode", "inherit")
    if exchange_mode not in {"inherit", "per_exchange"}:
        raise ScenarioTemplateError("exchange_mode must be 'inherit' or 'per_exchange'")
    exchanges = _normalize_exchanges(payload, exchange_mode)
    sweep_policy = None
    if template == "sweep_cycles":
        if exchange_mode != "inherit":
            raise ScenarioTemplateError("sweep_cycles requires inherited combined exchanges")
        sweep_policy = {
            "balance_multiplier": _bounded_number(payload, "balance_multiplier", 2.0, 1.01, 100.0),
            "starting_balance": _bounded_number(payload, "starting_balance", 1000.0, 1.0, 1_000_000_000.0),
            "refill_cost": _bounded_number(payload, "refill_cost", 0.0, 0.0, 1_000_000_000.0),
            "cooldown_days": _bounded_int(payload, "cooldown_days", 0, 0, 3650),
        }
        minimum_stride = window_days + sweep_policy["cooldown_days"]
        if auto_windows:
            stride_days = minimum_stride
            available_days = (end_date - start_date).days + 1
            total_windows = 0 if available_days < window_days else 1 + (available_days - window_days) // stride_days
            training_windows = total_windows - holdout_windows
            if training_windows < 1:
                raise ScenarioTemplateError(
                    "date range does not fit at least one training window plus the requested holdout windows"
                )
            if training_windows > MAX_WINDOWS:
                raise ScenarioTemplateError(
                    f"automatic sweep plan requires {training_windows} training windows; maximum is {MAX_WINDOWS}. "
                    "Increase window_days or cooldown_days."
                )
        elif stride_days < minimum_stride:
            raise ScenarioTemplateError(
                f"sweep_cycles stride_days must be at least window_days + cooldown_days ({minimum_stride})"
            )

    window_count = training_windows + holdout_windows
    first_window_end = end_date - timedelta(days=(window_count - 1) * stride_days)
    first_window_start = first_window_end - timedelta(days=window_days - 1)
    if first_window_start < start_date:
        available_days = (end_date - start_date).days + 1
        required_days = window_days + (window_count - 1) * stride_days
        raise ScenarioTemplateError(
            f"date range provides {available_days} days but this template requires {required_days} days"
        )

    expansion = exchanges if exchange_mode == "per_exchange" else [None]
    scenario_count = window_count * len(expansion)
    if scenario_count > MAX_SCENARIOS:
        raise ScenarioTemplateError(f"template expands to {scenario_count} scenarios; maximum is {MAX_SCENARIOS}")

    windows: list[tuple[date, date]] = []
    for index in range(window_count):
        window_end = first_window_end + timedelta(days=index * stride_days)
        windows.append((window_end - timedelta(days=window_days - 1), window_end))

    training_scenarios: list[dict[str, Any]] = []
    holdout_scenarios: list[dict[str, Any]] = []
    for index, (window_start, window_end) in enumerate(windows):
        is_holdout = index >= training_windows
        ordinal = index - training_windows + 1 if is_holdout else index + 1
        prefix = "holdout" if is_holdout else "train"
        target = holdout_scenarios if is_holdout else training_scenarios
        for exchange in expansion:
            exchange_suffix = f"_{exchange}" if exchange else ""
            label = f"{prefix}_{ordinal:02d}_{window_start:%Y%m%d}_{window_days}d{exchange_suffix}"
            target.append(_window_scenario(window_start, window_end, label, exchange))

    reducer = {
        "default": "median" if template in {"walk_forward", "sweep_cycles"} else "mean",
        "drawdown_worst_strategy_eq": "max",
        "backtest_completion_ratio": "min",
    }
    normalized_parameters: dict[str, Any] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "window_days": window_days,
        "stride_days": stride_days,
        "training_windows": training_windows,
        "holdout_windows": holdout_windows,
        "exchange_mode": exchange_mode,
        "exchanges": exchanges,
        "auto_windows": auto_windows,
    }
    if sweep_policy is not None:
        normalized_parameters["sweep_policy"] = sweep_policy

    warnings: list[str] = []
    if stride_days < window_days:
        warnings.append("Windows overlap; adjacent scenarios reuse part of the same market period.")
    if holdout_scenarios:
        warnings.append("Holdout scenarios are preview-only and are not applied to backtest.scenarios.")
    if exchange_mode == "inherit":
        warnings.append("Scenarios inherit the optimizer's selected exchanges.")
    if template == "sweep_cycles":
        warnings.append("Sweep cash-flow metrics are calculated for Pareto candidates after PB8 produces per-scenario gains.")
        if auto_windows:
            warnings.append("Training windows and stride were calculated automatically from the available date range.")

    provenance = {
        "contract_version": 1,
        "template": template,
        "template_version": TEMPLATE_VERSION,
        "parameters": normalized_parameters,
        "holdout_scenarios": holdout_scenarios,
    }
    unused_leading_days = max(0, (first_window_start - start_date).days)
    if auto_windows and unused_leading_days:
        warnings.append(
            f"{unused_leading_days} leading day(s) remain unused because they do not form another complete window."
        )
    return {
        "contract_version": 1,
        "template": template,
        "template_version": TEMPLATE_VERSION,
        "parameters": normalized_parameters,
        "training_scenarios": training_scenarios,
        "holdout_scenarios": holdout_scenarios,
        "reducer": reducer,
        "coverage": {
            "status": "date_bounds_only",
            "first_window_start": first_window_start.isoformat(),
            "last_window_end": end_date.isoformat(),
            "window_count": window_count,
            "scenario_count": scenario_count,
            "unused_leading_days": unused_leading_days,
        },
        "warnings": warnings,
        "provenance": provenance,
    }
