"""Run PB8 config operations inside the isolated PB8 virtual environment."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path


def _load_pb8_modules(pb8_dir: Path):
    """Import PB8 modules only after its source directory is selected."""
    src_dir = pb8_dir / "src"
    if not src_dir.is_dir():
        raise RuntimeError(f"PB8 source directory not found: {src_dir}")
    sys.path.insert(0, str(src_dir))

    from config.load import load_prepared_config, prepare_config
    from config.metrics import ANALYSIS_SHARED_KEYS, CURRENCY_METRICS
    from config.limits import SUPPORTED_LIMIT_STATS
    from config.scoring import DEFAULT_OBJECTIVE_GOALS, OBJECTIVE_GOALS
    from config.schema import CONFIG_SCHEMA_VERSION, get_template_config
    from config.optimize_bounds import get_optimize_bounds_defaults
    from config.strategy_spec import get_all_strategy_defaults, get_supported_strategy_kinds, get_strategy_spec
    from config.migrations.trailing_grid_v7 import migrate_v7_trailing_grid_file
    from config_utils import sanitize_prepared_config_for_dump
    from optimization.backends import BACKEND_RUNNERS
    from optimization.backends.pymoo_backend import SUPPORTED_PYMOO_ALGORITHMS, SUPPORTED_REF_DIR_METHODS
    from optimizer_overrides import KNOWN_OPTIMIZER_OVERRIDES
    from passivbot_version import __version__

    return {
        "load_prepared_config": load_prepared_config,
        "prepare_config": prepare_config,
        "schema_version": CONFIG_SCHEMA_VERSION,
        "get_template_config": get_template_config,
        "get_supported_strategy_kinds": get_supported_strategy_kinds,
        "get_strategy_spec": get_strategy_spec,
        "get_all_strategy_defaults": get_all_strategy_defaults,
        "get_optimize_bounds_defaults": get_optimize_bounds_defaults,
        "backends": sorted(BACKEND_RUNNERS),
        "pymoo_algorithms": sorted(SUPPORTED_PYMOO_ALGORITHMS),
        "pymoo_ref_dir_methods": sorted(SUPPORTED_REF_DIR_METHODS),
        "objective_goals": list(OBJECTIVE_GOALS),
        "default_objective_goals": dict(DEFAULT_OBJECTIVE_GOALS),
        "limit_statistics": sorted(SUPPORTED_LIMIT_STATS),
        "optimizer_overrides": sorted(KNOWN_OPTIMIZER_OVERRIDES),
        "migrate_v7": migrate_v7_trailing_grid_file,
        "result_metrics": sorted(
            set(ANALYSIS_SHARED_KEYS)
            | set(CURRENCY_METRICS)
            | {
                f"{metric}_{currency}"
                for metric in CURRENCY_METRICS
                for currency in ("usd", "btc")
            }
        ),
        "sanitize": sanitize_prepared_config_for_dump,
        "version": __version__,
    }


def _leaf_metadata(value, prefix: str = "") -> list[dict]:
    """Describe every runtime-provided leaf without imposing a PB7 schema."""
    result = []
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            result.extend(_leaf_metadata(item, path))
        return result
    if isinstance(value, bool):
        value_type = "boolean"
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        value_type = "number"
    elif isinstance(value, str):
        value_type = "string"
    elif value is None:
        value_type = "null"
    elif isinstance(value, list):
        value_type = "array"
    else:
        value_type = "json"
    result.append({"path": prefix, "type": value_type, "default": copy.deepcopy(value)})
    return result


def _optimize_metadata(modules: dict) -> dict:
    """Build one coherent metadata model from the installed PB8 runtime."""
    template = _prepare(modules, modules["get_template_config"]())
    optimize = copy.deepcopy(template.get("optimize") or {})
    strategies = list(modules["get_supported_strategy_kinds"]())
    bounds = optimize.get("bounds") if isinstance(optimize.get("bounds"), dict) else {}
    all_bounds = modules["get_optimize_bounds_defaults"]()
    active_bounds = {}
    strategy_specs = {}
    for strategy in strategies:
        strategy_specs[strategy] = copy.deepcopy(modules["get_strategy_spec"](strategy))
        selected = copy.deepcopy(all_bounds)
        for side in ("long", "short"):
            side_bounds = selected.get(side) if isinstance(selected.get(side), dict) else {}
            strategy_bounds = side_bounds.get("strategy") if isinstance(side_bounds.get("strategy"), dict) else {}
            side_bounds["strategy"] = {
                strategy: copy.deepcopy(strategy_bounds.get(strategy) or {})
            }
        active_bounds[strategy] = selected
    metrics = sorted(set(modules["result_metrics"]) | set(modules["default_objective_goals"]))
    return {
        "template": template,
        "strategies": strategies,
        "strategy_specs": strategy_specs,
        "strategy_defaults": modules["get_all_strategy_defaults"](),
        "bounds": copy.deepcopy(bounds),
        "all_bounds": all_bounds,
        "active_bounds": active_bounds,
        "optimize_defaults": optimize,
        "optimize_parameters": _leaf_metadata(optimize, "optimize"),
        "bot_parameter_paths": [entry["path"] for entry in _leaf_metadata(template.get("bot") or {}, "bot")],
        "backends": modules["backends"],
        "pymoo": {
            "algorithms": modules["pymoo_algorithms"],
            "ref_dir_methods": modules["pymoo_ref_dir_methods"],
            "defaults": copy.deepcopy(optimize.get("pymoo") or {}),
        },
        "scoring": {
            "metrics": metrics,
            "goals": modules["objective_goals"],
            "default_goals": modules["default_objective_goals"],
            "defaults": copy.deepcopy(optimize.get("scoring") or []),
        },
        "limits": {
            "metrics": metrics,
            "statistics": modules["limit_statistics"],
            "operators": [
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
            "defaults": copy.deepcopy(optimize.get("limits") or []),
        },
        "optimizer_overrides": modules["optimizer_overrides"],
        "fixed_runtime_overrides": copy.deepcopy(optimize.get("fixed_runtime_overrides") or {}),
        "runtime_options": {
            "mode": {"choices": ["fresh", "pareto_seed", "checkpoint_resume"], "default": "fresh"},
            "fine_tune_params": {"type": "array", "default": []},
            "polish_percentage": {"type": "number_or_null", "default": None, "minimum": 0},
            "polish_bounds_mode": {
                "choices": ["clamp", "override-tunable", "override-all"],
                "default": "clamp",
            },
        },
    }


def _prepare(modules: dict, config: dict, base_config_path: str = "") -> dict:
    """Return a clean canonical PB8 config suitable for persistence."""
    candidate = copy.deepcopy(config)
    pbgui_metadata = candidate.pop("pbgui", None)
    if pbgui_metadata is not None and not isinstance(pbgui_metadata, dict):
        raise TypeError("pbgui must be an object")
    prepared = modules["prepare_config"](
        candidate,
        base_config_path=base_config_path,
        verbose=False,
        target="canonical",
        runtime=None,
        raw_snapshot=candidate,
        effective_snapshot=candidate,
    )
    sanitized = modules["sanitize"](prepared)
    if pbgui_metadata is not None:
        sanitized["pbgui"] = copy.deepcopy(pbgui_metadata)
    return sanitized


def handle(payload: dict) -> dict:
    """Dispatch one JSON request and return a JSON-compatible result."""
    pb8_dir = Path(str(payload.get("pb8_dir") or "")).resolve()
    modules = _load_pb8_modules(pb8_dir)
    operation = str(payload.get("operation") or "")

    if operation == "status":
        return {
            "version": modules["version"],
            "config_schema": modules["schema_version"],
        }
    if operation == "default":
        return {"config": _prepare(modules, modules["get_template_config"]())}
    if operation == "result_metrics":
        return {"metrics": modules["result_metrics"]}
    if operation == "optimize_metadata":
        return _optimize_metadata(modules)
    if operation == "prepare":
        config = payload.get("config")
        if not isinstance(config, dict):
            raise TypeError("config must be an object")
        return {
            "config": _prepare(
                modules,
                config,
                str(payload.get("base_config_path") or ""),
            )
        }
    if operation == "load":
        config_path = Path(str(payload.get("config_path") or "")).resolve()
        raw_config = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(raw_config, dict):
            raise TypeError("config must be an object")
        pbgui_metadata = raw_config.get("pbgui")
        if pbgui_metadata is not None and not isinstance(pbgui_metadata, dict):
            raise TypeError("pbgui must be an object")
        prepared = modules["load_prepared_config"](
            str(config_path),
            verbose=False,
            target="canonical",
            runtime=None,
            log_info=False,
        )
        sanitized = modules["sanitize"](prepared)
        if pbgui_metadata is not None:
            sanitized["pbgui"] = copy.deepcopy(pbgui_metadata)
        return {"config": sanitized}
    if operation == "migrate_v7":
        source_path = Path(str(payload.get("source_path") or "")).resolve()
        output_path = Path(str(payload.get("output_path") or "")).resolve()
        allow_manual_review = bool(payload.get("allow_manual_review_output", False))
        migrated, report = modules["migrate_v7"](
            source_path,
            output_path,
            allow_manual_review_output=allow_manual_review,
        )
        result = {"report": report}
        if report.get("output_written") and isinstance(migrated, dict):
            result["config"] = _prepare(modules, migrated, str(output_path))
        return result
    raise ValueError(f"Unsupported operation: {operation}")


def main() -> int:
    """Read one request from stdin and write one response to stdout."""
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise TypeError("request must be an object")
        response = {"ok": True, "result": handle(payload)}
    except Exception as exc:
        response = {
            "ok": False,
            "error": type(exc).__name__,
            "detail": str(exc),
        }
    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
