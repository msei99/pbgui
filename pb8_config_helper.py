"""Run PB8 config operations inside the isolated PB8 virtual environment."""

from __future__ import annotations

import asyncio
import copy
import importlib
import importlib.util
import json
import platform
import sys
from contextlib import redirect_stdout
from pathlib import Path

from config_dates import normalize_backtest_dates
from passivbot_market import (
    _load_pb8_market_modules,
    _validated_market_strings,
    _market_display_label,
    _resolve_market_identifier,
    _market_identifiers,
)

_OPTIMIZE_METADATA_CACHE: dict[str, dict] = {}
SERVICE = "PB8ConfigHelper"


def _gpu_runtime_contract(backends: list[str]) -> dict:
    """Share hardware probing without importing another generation's config modules."""
    spec = importlib.util.spec_from_file_location("_pbgui_gpu_runtime", Path(__file__).with_name("optimizer_gpu_runtime.py"))
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    return probe.gpu_runtime_contract(backends)


def _gpu_effective_defaults() -> dict:
    """Read PB8's effective GPU defaults without importing optional Torch."""
    try:
        from optimization.backends.gpu_backend import GPU_DEFAULTS

        return copy.deepcopy(GPU_DEFAULTS)
    except (ImportError, ModuleNotFoundError):
        return {}


def _optimizer_backend_contract(backends: list[str], metrics: list[str]) -> dict:
    """Return an additive versioned backend capability and metric contract."""
    runtime = _gpu_runtime_contract(backends)
    gpu_supported = None
    gpu_exact_only: list[str] = []
    try:
        from optimization.gpu.metric_registry import GPU_EXACT_ONLY_METRICS

        gpu_exact_only = sorted(str(value) for value in GPU_EXACT_ONLY_METRICS)
    except (ImportError, ModuleNotFoundError):
        pass
    if runtime["device_available"]:
        try:
            from optimization.gpu.metrics import SUPPORTED_METRICS

            gpu_supported = set(str(value) for value in SUPPORTED_METRICS)
            # PB8 validates aliases after its exact-only exclusion. Use that same
            # policy for editor choices instead of guessing aliases in JavaScript.
            from optimization.gpu import metrics as gpu_metrics

            validate = getattr(gpu_metrics, "validate_gpu_metric_names", None)
            if callable(validate):
                for metric in metrics:
                    try:
                        validate([metric])
                    except ValueError:
                        continue
                    gpu_supported.add(metric)
            gpu_supported = sorted(gpu_supported)
        except (ImportError, ModuleNotFoundError):
            gpu_supported = None
    items = {
        backend: {
            "recognized": True,
            "available": True,
            "metric_set": "cpu",
            "reason_code": "available",
            "reason": "",
        }
        for backend in backends
    }
    if "gpu" in items:
        items["gpu"].update(
            available=bool(runtime["device_available"]),
            metric_set="gpu_proxy",
            metric_eligibility_known=gpu_supported is not None,
            reason_code=runtime["reason_code"],
            reason=runtime["reason"],
            runtime=runtime,
            exact_only_metrics=gpu_exact_only,
            effective_defaults=_gpu_effective_defaults(),
        )
    return {
        "contract_version": 1,
        "metric_sets": {"cpu": list(metrics), "gpu_proxy": gpu_supported},
        "items": items,
    }


def _configured_optimize_metrics(config: dict) -> list[str]:
    """Collect enabled objective and limit metric names from a prepared config."""
    optimize = config.get("optimize") if isinstance(config.get("optimize"), dict) else {}
    result = []
    for item in optimize.get("scoring") or []:
        if isinstance(item, dict) and item.get("metric"):
            result.append(str(item["metric"]))
    for item in optimize.get("limits") or []:
        if isinstance(item, dict) and item.get("enabled", True) is not False and item.get("metric"):
            result.append(str(item["metric"]))
    return result


def _optimize_preflight(modules: dict, config: dict, base_config_path: str = "") -> dict:
    """Run PB8-native static validation before a GPU queue item can launch."""
    prepared = copy.deepcopy(config)
    optimize = prepared.get("optimize") if isinstance(prepared.get("optimize"), dict) else {}
    backend = str(optimize.get("backend") or "pymoo").strip().lower()
    if backend != "gpu":
        return {
            "contract_version": 1,
            "backend": backend,
            "valid": True,
            "stage": "not_required",
            "native_contract": None,
        }

    runtime = _gpu_runtime_contract(modules["backends"])
    if not runtime["device_available"]:
        raise RuntimeError(runtime["reason"])

    from optimization.backends import gpu_backend
    from optimization.gpu import metrics as gpu_metrics
    from suite_runner import extract_suite_config

    materialize = getattr(gpu_backend, "materialize_gpu_preparation_config", None)
    native_contract = "validate_gpu_preparation_scope"
    if callable(materialize):
        effective = materialize(prepared)
    else:
        materialize = getattr(gpu_backend, "_materialize_gpu_override_template")
        effective = materialize(prepared, optimize.get("enable_overrides") or [])
        native_contract = "legacy_static_fallback"
    metric_names = _configured_optimize_metrics(effective)
    validate_metrics = getattr(gpu_metrics, "validate_gpu_metric_names", None)
    if callable(validate_metrics):
        validate_metrics(metric_names)
    else:
        supported = set(getattr(gpu_metrics, "SUPPORTED_METRICS", ()))
        unsupported = sorted(set(metric_names) - supported)
        if unsupported:
            raise ValueError(f"GPU optimizer does not support metrics {unsupported}")
    validate_scope = getattr(gpu_backend, "validate_gpu_preparation_scope", None)
    if callable(validate_scope):
        validate_scope(effective, extract_suite_config(effective, None))
    else:
        strategy = str((effective.get("live") or {}).get("strategy_kind") or "").strip().lower()
        supported_strategies = set(getattr(gpu_backend, "GPU_STRATEGY_BOUND_MAPS", {}))
        if strategy not in supported_strategies:
            raise ValueError(f"GPU optimizer does not support strategy_kind={strategy!r}")
        getattr(gpu_backend, "_validate_gpu_optimizer_overrides")(
            optimize.get("enable_overrides") or [], strategy
        )
    return {
        "contract_version": 1,
        "backend": backend,
        "valid": True,
        "stage": "complete",
        "native_contract": native_contract,
        "runtime": runtime,
    }


def _optimize_basis_contract(limits_module, scoring_fields, reducers_module=None) -> dict:
    """Describe the installed PB8 optimizer's canonical suite-reduction fields."""
    reducers = getattr(reducers_module, "SUPPORTED_REDUCERS", None)
    if reducers is not None:
        limit_basis_field = "reducer"
    else:
        reducers = getattr(limits_module, "SUPPORTED_LIMIT_STATS", None)
        if reducers is None:
            reducers = getattr(limits_module, "SUPPORTED_AGGREGATE_MODES", None)
        limit_basis_field = "stat"
    if not reducers:
        raise RuntimeError("PB8 exposes no supported optimizer suite reducers")
    scoring_basis_field = "reducer" if "reducer" in scoring_fields else "aggregate"
    return {
        "statistics": sorted(reducers),
        "limit_basis_field": limit_basis_field,
        "scoring_basis_field": scoring_basis_field,
    }


def _cached_optimize_metadata(modules: dict, pb8_dir: Path) -> dict:
    """Reuse immutable optimizer metadata inside the persistent helper process."""
    key = str(pb8_dir)
    if key not in _OPTIMIZE_METADATA_CACHE:
        _OPTIMIZE_METADATA_CACHE[key] = _optimize_metadata(modules, include_backend=False)
    return copy.deepcopy(_OPTIMIZE_METADATA_CACHE[key])


def _complete_optimize_metadata(modules: dict, pb8_dir: Path) -> dict:
    """Keep migration and legacy helper replies complete, refreshing host state."""
    metadata = _cached_optimize_metadata(modules, pb8_dir)
    metadata["backend_contract"] = _optimizer_backend_contract(
        modules["backends"], metadata["scoring"]["metrics"])
    return metadata


def _load_pb8_modules(pb8_dir: Path):
    """Import PB8 modules only after its source directory is selected."""
    src_dir = pb8_dir / "src"
    if not src_dir.is_dir():
        raise RuntimeError(f"PB8 source directory not found: {src_dir}")
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    from config.load import load_prepared_config, prepare_config
    from config.coerce import normalize_hsl_signal_mode
    from config.metrics import ANALYSIS_SHARED_KEYS, CURRENCY_METRICS
    from config import limits as limits_module
    from config.scoring import DEFAULT_OBJECTIVE_GOALS, OBJECTIVE_GOALS, SCORING_ENTRY_FIELDS
    from config.schema import CONFIG_SCHEMA_VERSION, get_template_config
    from config.optimize_bounds import get_optimize_bounds_defaults
    from config.strategy_spec import get_all_strategy_defaults, get_supported_strategy_kinds, get_strategy_spec
    from config.strategy_spec import normalize_strategy_kind
    from config.overrides import get_allowed_modifications, parse_overrides
    from live.order_churn_gate import ORDER_CHURN_GATE_SUPPORTED_EXCHANGES
    from utils import to_ccxt_client_id
    import ccxt.async_support as ccxt_async
    from config.migrations.trailing_grid_v7 import migrate_v7_trailing_grid_file
    from config_utils import sanitize_prepared_config_for_dump
    from optimization.backends import BACKEND_RUNNERS
    from optimization.backends.pymoo_backend import SUPPORTED_PYMOO_ALGORITHMS, SUPPORTED_REF_DIR_METHODS
    from optimizer_overrides import KNOWN_OPTIMIZER_OVERRIDES, optimizer_overrides
    from passivbot_version import __version__

    try:
        from config import reducers as reducers_module
    except ImportError:
        reducers_module = None
    optimize_basis = _optimize_basis_contract(
        limits_module,
        SCORING_ENTRY_FIELDS,
        reducers_module,
    )

    return {
        "load_prepared_config": load_prepared_config,
        "prepare_config": prepare_config,
        "normalize_hsl_signal_mode": normalize_hsl_signal_mode,
        "normalize_strategy_kind": normalize_strategy_kind,
        "get_allowed_modifications": get_allowed_modifications,
        "parse_overrides": parse_overrides,
        "live_exchanges": ORDER_CHURN_GATE_SUPPORTED_EXCHANGES,
        "to_ccxt_client_id": to_ccxt_client_id,
        "ccxt_exchanges": frozenset(ccxt_async.exchanges),
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
        "limit_statistics": optimize_basis["statistics"],
        "limit_basis_field": optimize_basis["limit_basis_field"],
        "scoring_basis_field": optimize_basis["scoring_basis_field"],
        "optimizer_overrides": sorted(KNOWN_OPTIMIZER_OVERRIDES),
        "apply_optimizer_overrides": optimizer_overrides,
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


def _override_leaf_metadata(value) -> dict:
    """Describe a PB8 scalar, including the nullable cooldown ceiling."""
    if isinstance(value, bool):
        value_type = "boolean"
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        value_type = "number"
    elif isinstance(value, str):
        value_type = "string"
    elif value is None:
        value_type = "number_or_null"
    else:
        raise TypeError(f"PB8 override leaf has unsupported default type {type(value).__name__}")
    return {"type": value_type, "default": copy.deepcopy(value)}


def _coin_override_metadata(modules: dict, payload: dict) -> dict:
    """Build typed canonical metadata from PB8's official override policy."""
    hsl_signal_mode = modules["normalize_hsl_signal_mode"](
        payload.get("hsl_signal_mode", "coin")
    )
    strategy_kind = modules["normalize_strategy_kind"](payload.get("strategy_kind"))
    policy = modules["get_allowed_modifications"](hsl_signal_mode=hsl_signal_mode)
    template = _prepare(modules, modules["get_template_config"]())
    strategy_defaults = modules["get_all_strategy_defaults"]()
    params = {"bot": {"long": {}, "short": {}}, "live": {}}
    for side in ("long", "short"):
        side_policy = policy["bot"][side]
        canonical = {}
        for group in ("risk", "unstuck", "hsl", "entry_cooldown", "forager"):
            if isinstance(side_policy.get(group), dict):
                canonical[group] = side_policy[group]
        canonical["strategy"] = {strategy_kind: side_policy["strategy"][strategy_kind]}
        if side_policy.get("wallet_exposure_limit") is True:
            params["bot"][side]["wallet_exposure_limit"] = {"type": "number"}
        for path, allowed in _iter_policy_leaves(canonical):
            if allowed is not True:
                continue
            if len(path) >= 2 and path[:2] == ("strategy", strategy_kind):
                default = _nested_value(strategy_defaults[side][strategy_kind], path[2:])
            else:
                default = _nested_value(template["bot"][side], path)
            params["bot"][side][".".join(path)] = _override_leaf_metadata(default)
    for key, allowed in policy["live"].items():
        if allowed is True:
            params["live"][key] = _override_leaf_metadata(template["live"][key])
    return {
        "contract_version": 1,
        "hsl_signal_mode": hsl_signal_mode,
        "strategy_kind": strategy_kind,
        "params": params,
    }


def _exchange_metadata(modules: dict) -> dict:
    """Report PB8.1 connector and historical-data capabilities."""
    live = sorted(set(modules["live_exchanges"]) - {"fake"})
    historical = sorted(
        exchange
        for exchange in live
        if modules["to_ccxt_client_id"](exchange) in modules["ccxt_exchanges"]
    )
    return {
        "contract_version": 1,
        "live": live,
        "backtest": historical,
        "optimize": historical,
        "suite": historical,
    }


def _iter_policy_leaves(value, path=()):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _iter_policy_leaves(child, path + (key,))
        return
    yield path, value


def _nested_value(value, path):
    current = value
    for key in path:
        if not isinstance(current, dict) or key not in current:
            raise KeyError(".".join(path))
        current = current[key]
    return current


def _optimize_metadata(modules: dict, *, include_backend: bool = True) -> dict:
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
        **({"backend_contract": _optimizer_backend_contract(modules["backends"], metrics)}
           if include_backend else {}),
        "pymoo": {
            "algorithms": modules["pymoo_algorithms"],
            "ref_dir_methods": modules["pymoo_ref_dir_methods"],
            "defaults": copy.deepcopy(optimize.get("pymoo") or {}),
        },
        "scoring": {
            "metrics": metrics,
            "goals": modules["objective_goals"],
            "default_goals": modules["default_objective_goals"],
            "basis_field": modules["scoring_basis_field"],
            "defaults": copy.deepcopy(optimize.get("scoring") or []),
        },
        "limits": {
            "metrics": metrics,
            "statistics": modules["limit_statistics"],
            "basis_field": modules["limit_basis_field"],
            "scoring_basis_field": modules["scoring_basis_field"],
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


def _sanitize_for_editor(modules: dict, prepared: dict, source: dict) -> dict:
    """Keep validated explicit GPU CPU auto values through PB8's dump cleaner."""
    source = source.get("config", source)
    source_optimize = source.get("optimize") or {}
    prepared_optimize = prepared.get("optimize") or {}
    preserve_auto = (isinstance(source_optimize, dict)
            and "n_cpus" in source_optimize and source_optimize["n_cpus"] is None
            and prepared_optimize.get("backend") == "gpu"
            and prepared_optimize.get("n_cpus") is None)
    sanitized = modules["sanitize"](prepared)
    if preserve_auto:
        sanitized["optimize"]["n_cpus"] = None
    return normalize_backtest_dates(sanitized)


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
    sanitized = _sanitize_for_editor(modules, prepared, candidate)
    if pbgui_metadata is not None:
        sanitized["pbgui"] = copy.deepcopy(pbgui_metadata)
    return sanitized


def _validate_optimizer_overrides(modules: dict, config: dict, base_config_path: str = "") -> None:
    """Exercise PB8's native override application for every optimizer side."""
    prepared = _prepare(modules, config, base_config_path)
    overrides = prepared.get("optimize", {}).get("enable_overrides", [])
    candidate = modules["apply_optimizer_overrides"](overrides, copy.deepcopy(prepared), None)
    for pside in (side for side in ("long", "short") if side in candidate.get("bot", {})):
        candidate = modules["apply_optimizer_overrides"](overrides, candidate, pside)


def _optimizer_warmup(configs: list) -> dict:
    """Use native bounds, strategy spans, fixed overrides, ratio and cap rules."""
    from optimization.warmup import compute_optimizer_backtest_warmup_minutes
    from warmup_utils import compute_backtest_warmup_minutes

    if not isinstance(configs, list) or not configs or any(not isinstance(c, dict) for c in configs):
        raise TypeError('configs must be a nonempty array of objects')
    return {'minutes': [max(compute_backtest_warmup_minutes(config),
                            compute_optimizer_backtest_warmup_minutes(config))
                        for config in configs]}


def _validate_migration_references(source, source_path):
    """Constrain native migration file reads to the managed config bundle."""
    if source.get("live", {}).get("coin_flags") and not source.get("coin_overrides"):
        from config.overrides import parse_old_coin_flags
        references = parse_old_coin_flags(source)
    else:
        references = source.get("coin_overrides", {})

    def visit(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "override_config_path" and child is not None:
                    if (not isinstance(child, str) or not child.endswith(".json")
                            or child.startswith(".") or child in ("config.json", "migration_report.json") or any(c in child for c in ("/", "\\"))
                            or any(ord(c) < 32 or ord(c) == 127 for c in child)):
                        raise ValueError("Invalid migration override filename")
                    path = source_path.parent / child
                    if path.is_symlink() or not path.is_file():
                        raise ValueError("Migration override must be a regular file in the config directory")
                else:
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
    visit(references)
    visit(source)


def _canonicalize_editor_fixed_params(source):
    """Repair PBGui's former flattened forager selectors using native paths."""
    optimize = source.get("optimize", {})
    selectors = optimize.get("fixed_params") if isinstance(optimize, dict) else None
    if not isinstance(selectors, list):
        return
    result = []
    for selector in selectors:
        parts = selector.split(".") if isinstance(selector, str) else []
        if parts and parts[0] in ("long", "short"):
            parts.insert(0, "bot")
        if (len(parts) == 4 and parts[0] == "bot" and parts[1] in ("long", "short")
                and parts[2] == "forager" and parts[3].startswith("score_weights_")):
            from config.param_paths import resolve_optimizer_key_path
            path = resolve_optimizer_key_path(source, f"{parts[1]}_forager_{parts[3]}")
            if path and len(path) == 5 and path[:4] == ("bot", parts[1], "forager", "score_weights"):
                selector = ".".join(path)
        result.append(selector)
    optimize["fixed_params"] = result


def _migrate_editor_hsl(source, source_path, modules):
    """Use native migration for an unsaved draft, retaining unresolved choices."""
    from tools.migrate_hsl_config import migrate

    original = copy.deepcopy(source)
    source = copy.deepcopy(source)
    pbgui = source.pop("pbgui", None)
    if pbgui is not None and not isinstance(pbgui, dict):
        raise TypeError("pbgui must be an object")
    _canonicalize_editor_fixed_params(source)
    bot = source.get("bot", {})
    mode = str(source.get("live", {}).get("hsl_signal_mode", "coin")).strip().lower()
    scopes = ["portfolio"] if mode == "unified" else ["long", "short"]
    choices, unresolved = {}, []
    portfolio = None
    if mode == "unified" and "hsl" not in bot:
        # A temporary disabled native template permits structural normalization.
        # None of its portfolio values are returned as a user's policy.
        portfolio = copy.deepcopy(modules["get_template_config"]()["bot"]["long"]["hsl"])
        portfolio["enabled"] = False
        portfolio["restart_after_red_policy"] = None
        unresolved.extend("bot.hsl." + key for key in portfolio)
    for scope in scopes:
        block = bot.get("hsl", {}) if scope == "portfolio" else bot.get(scope, {}).get("hsl", {})
        policy = block.get("restart_after_red_policy")
        normalized = policy.strip().lower() if isinstance(policy, str) else policy
        if block.get("enabled") is True and normalized in (None, "threshold"):
            # The tool requires a choice to normalize an enabled scope. Its
            # placeholder is removed before the draft leaves this helper.
            choices[scope] = "always"
            unresolved.append("bot.hsl.restart_after_red_policy" if scope == "portfolio"
                              else f"bot.{scope}.hsl.restart_after_red_policy")
    migrated = migrate(source, restart_policies=choices, portfolio=portfolio,
                       base_config_path=str(source_path))
    for path in unresolved:
        parts = path.split(".")
        node = migrated
        for key in parts[:-1]:
            node = node[key]
        node[parts[-1]] = None
        fixed = migrated.get("optimize", {}).get("fixed_runtime_overrides", {})
        if path in fixed:
            authored = original.get("optimize", {}).get("fixed_runtime_overrides", {}).get(path)
            if authored in ("always", "never"):
                fixed[path] = authored
            else:
                del fixed[path]  # Inherit the policy the user selects in the draft.
    if pbgui is not None:
        migrated["pbgui"] = pbgui

    def flatten(value, prefix=""):
        result = {}
        if isinstance(value, dict) and value:
            for key, child in value.items():
                result.update(flatten(child, f"{prefix}.{key}" if prefix else key))
        else:
            result[prefix] = value
        return result

    before, after = flatten(original), flatten(migrated)
    changes = []
    config_changes = {}
    status = {"long": {}, "short": {}}
    for path in sorted(before.keys() | after.keys()):
        if path in before and path in after and before[path] == after[path]:
            continue
        change = {"path": path, "removed": path not in after, "added": path not in before,
                  "before": before.get(path), "after": after.get(path), "requires_choice": path in unresolved}
        config_changes[path] = change
        if not path.startswith(("bot.long.hsl.", "bot.short.hsl.", "bot.hsl.", "live.hsl_")):
            continue
        changes.append(change)
        parts = path.split(".")
        if len(parts) >= 4 and parts[0] == "bot" and parts[1] in status and parts[2] == "hsl":
            key = ".".join(parts[2:])
            # Keep deleted leaves and their authored values available for the
            # read-only diff beside the editor; they must not reenter config.
            status[parts[1]][key] = ({"status": "removed", "before": before[path]}
                                      if path not in after else "review_line")
    status["config_changes"] = config_changes
    return {"config": migrated, "param_status": status, "migration_changes": changes,
            "migration_unresolved": unresolved, "hsl_migration_required": False}


def handle(payload: dict) -> dict:
    """Dispatch one JSON request and return a JSON-compatible result."""
    pb8_dir = Path(str(payload.get("pb8_dir") or "")).resolve()
    modules = _load_pb8_modules(pb8_dir)
    operation = str(payload.get("operation") or "")

    if operation in {"load_hsl_editor", "migrate_hsl"}:
        source_path = Path(str(payload.get("config_path") or "")).resolve()
        source = json.loads(source_path.read_text(encoding="utf-8"))
        if not isinstance(source, dict):
            raise TypeError("config must be an object")
        _validate_migration_references(source, source_path)
        if operation == "load_hsl_editor":
            try:
                modules["load_prepared_config"](
                    str(source_path), verbose=False, target="canonical", runtime=None, log_info=False,
                )
            except ValueError as exc:
                if "migrate-hsl" not in str(exc):
                    raise
                return _migrate_editor_hsl(source, source_path, modules)
            raise ValueError("This config does not require HSL migration")
        from tools.migrate_hsl_config import migrate

        pbgui = source.pop("pbgui", None)
        if pbgui is not None and not isinstance(pbgui, dict):
            raise TypeError("pbgui must be an object")
        _canonicalize_editor_fixed_params(source)
        migrated = migrate(
            source, restart_policies=payload.get("restart_policies"),
            portfolio=payload.get("portfolio"), base_config_path=str(source_path),
        )
        if pbgui is not None:
            migrated["pbgui"] = pbgui
        return {"config": migrated, "hsl_migration_required": False,
                "migration_notice": "HSL behavior changed. Re-backtest before live use. Save explicitly to apply."}

    if operation == "optimizer_warmup":
        return _optimizer_warmup(payload.get('configs'))

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
        return _complete_optimize_metadata(modules, pb8_dir)
    if operation == "optimize_metadata_static":
        return _cached_optimize_metadata(modules, pb8_dir)
    if operation == "optimizer_backend_contract":
        metadata = _cached_optimize_metadata(modules, pb8_dir)
        return _optimizer_backend_contract(modules["backends"], metadata["scoring"]["metrics"])
    if operation == "optimize_preflight":
        config = payload.get("config")
        if not isinstance(config, dict):
            raise TypeError("config must be an object")
        return _optimize_preflight(
            modules,
            config,
            str(payload.get("base_config_path") or ""),
        )
    if operation == "coin_override_metadata":
        return _coin_override_metadata(modules, payload)
    if operation == "exchange_metadata":
        return _exchange_metadata(modules)
    if operation == "validate_overrides":
        config_path = Path(str(payload.get("config_path") or "")).resolve()
        raw_config = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(raw_config, dict):
            raise TypeError("config must be an object")
        prepared = modules["load_prepared_config"](
            str(config_path),
            verbose=False,
            target="canonical",
            runtime=None,
            log_info=False,
        )
        modules["parse_overrides"](prepared, verbose=False)
        return {"valid": True}
    if operation == "validate_optimizer_overrides":
        config = payload.get("config")
        if not isinstance(config, dict):
            raise TypeError("config must be an object")
        _validate_optimizer_overrides(
            modules,
            config,
            str(payload.get("base_config_path") or ""),
        )
        return {"valid": True}
    if operation == "market_identifiers":
        return asyncio.run(_market_identifiers(_load_pb8_market_modules(), payload))
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
        sanitized = _sanitize_for_editor(modules, prepared, raw_config)
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
            result["config"] = migrated
            result["optimize_metadata"] = _complete_optimize_metadata(modules, pb8_dir)
        return result
    raise ValueError(f"Unsupported operation: {operation}")


def main() -> int:
    """Read one request from stdin and write one response to stdout."""
    try:
        payload = json.load(sys.stdin)
    except Exception as exc:
        response = {"ok": False, "error": type(exc).__name__, "detail": str(exc)}
    else:
        response = _response(payload)
    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0 if response["ok"] else 1


def _response(payload) -> dict:
    """Return one protocol response without terminating a persistent helper."""
    try:
        if not isinstance(payload, dict):
            raise TypeError("request must be an object")
        # Native imports (notably optimize/backtest's Rust checks) may print.
        # stdout is exclusively the JSON protocol in both main and serve.
        with redirect_stdout(sys.stderr):
            result = handle(payload)
        return {"ok": True, "result": result}
    except Exception as exc:
        return {
            "ok": False,
            "error": type(exc).__name__,
            "detail": str(exc),
        }


def serve() -> int:
    """Serve newline-delimited requests while retaining imported PB8 modules."""
    for line in sys.stdin:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            response = {"ok": False, "error": type(exc).__name__, "detail": str(exc)}
        else:
            response = _response(payload)
        sys.stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(serve() if "--serve" in sys.argv[1:] else main())
