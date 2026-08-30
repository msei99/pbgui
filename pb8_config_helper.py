"""Run PB8 config operations inside the isolated PB8 virtual environment."""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import platform
import sys
from pathlib import Path

_OPTIMIZE_METADATA_CACHE: dict[str, dict] = {}


def _gpu_runtime_contract(backends: list[str]) -> dict:
    """Describe whether PB8's registered Apple MPS backend can run here."""
    registered = "gpu" in backends
    runtime = {
        "accelerator": "apple_mps",
        "platform": platform.system(),
        "machine": platform.machine(),
        "dependency": "torch",
        "dependency_installed": False,
        "backend_built": False,
        "device_available": False,
        "reason_code": "backend_not_registered",
        "reason": "The installed PB8 runtime does not register the GPU backend.",
    }
    if not registered:
        return runtime
    if runtime["platform"] != "Darwin" or runtime["machine"] != "arm64":
        runtime.update(
            reason_code="unsupported_platform",
            reason="PB8 GPU optimization requires Apple Silicon and Apple MPS.",
        )
        return runtime
    if importlib.util.find_spec("torch") is None:
        runtime.update(
            reason_code="torch_not_installed",
            reason="PB8 GPU optimization requires the optional gpu-mps dependencies.",
        )
        return runtime
    runtime["dependency_installed"] = True
    try:
        import torch

        mps = getattr(getattr(torch, "backends", None), "mps", None)
        runtime["backend_built"] = bool(mps and (not hasattr(mps, "is_built") or mps.is_built()))
        runtime["device_available"] = bool(mps and mps.is_available())
    except Exception as exc:
        runtime.update(
            reason_code="torch_import_failed",
            reason=f"The PB8 PyTorch runtime could not be loaded: {type(exc).__name__}",
        )
        return runtime
    if not runtime["backend_built"]:
        runtime.update(reason_code="mps_not_built", reason="The installed PyTorch build has no Apple MPS backend.")
    elif not runtime["device_available"]:
        runtime.update(reason_code="mps_unavailable", reason="Apple MPS is unavailable in the PB8 process.")
    else:
        runtime.update(reason_code="available", reason="Apple MPS is available.")
    return runtime


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

            gpu_supported = sorted(str(value) for value in SUPPORTED_METRICS)
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
        _OPTIMIZE_METADATA_CACHE[key] = _optimize_metadata(modules)
    return copy.deepcopy(_OPTIMIZE_METADATA_CACHE[key])


def _load_pb8_modules(pb8_dir: Path):
    """Import PB8 modules only after its source directory is selected."""
    src_dir = pb8_dir / "src"
    if not src_dir.is_dir():
        raise RuntimeError(f"PB8 source directory not found: {src_dir}")
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


def _load_pb8_market_modules():
    """Import PB8.1's collision-safe market resolver only for market requests."""
    try:
        from utils import (
            AmbiguousMarketIdentifier,
            MarketIdentifierExchangeMismatch,
            UnknownMarketIdentifier,
            _approved_all_market_identifiers,
            coin_to_symbol,
            filter_markets,
            get_quote,
            load_markets,
            looks_like_exact_market_identifier,
            reject_cross_exchange_market_identifier_collisions,
            split_exchange_qualified_market_identifier,
            symbol_to_coin,
            to_standard_exchange_name,
        )
    except ImportError as exc:
        raise RuntimeError(
            "PB8's collision-safe market resolver is unavailable; update PB8 to v8.1.0 or newer"
        ) from exc
    return {
        "AmbiguousMarketIdentifier": AmbiguousMarketIdentifier,
        "MarketIdentifierExchangeMismatch": MarketIdentifierExchangeMismatch,
        "UnknownMarketIdentifier": UnknownMarketIdentifier,
        "approved_all_market_identifiers": _approved_all_market_identifiers,
        "coin_to_symbol": coin_to_symbol,
        "filter_markets": filter_markets,
        "get_quote": get_quote,
        "load_markets": load_markets,
        "looks_like_exact_market_identifier": looks_like_exact_market_identifier,
        "reject_cross_exchange_market_identifier_collisions": reject_cross_exchange_market_identifier_collisions,
        "split_exchange_qualified_market_identifier": split_exchange_qualified_market_identifier,
        "symbol_to_coin": symbol_to_coin,
        "to_standard_exchange_name": to_standard_exchange_name,
    }


def _validated_market_strings(value, field: str, *, maximum: int, max_bytes: int) -> list[str]:
    """Validate one bounded list without changing exact market identifier text."""
    if not isinstance(value, list):
        raise TypeError(f"{field} must be an array")
    if len(value) > maximum:
        raise ValueError(f"{field} exceeds the maximum of {maximum} items")
    result = []
    for item in value:
        if not isinstance(item, str):
            raise TypeError(f"{field} entries must be strings")
        normalized = item.strip()
        if not normalized:
            raise ValueError(f"{field} entries cannot be empty")
        if len(normalized.encode("utf-8")) > max_bytes:
            raise ValueError(f"{field} entries cannot exceed {max_bytes} bytes")
        if any(ord(char) < 32 or ord(char) == 127 for char in normalized):
            raise ValueError(f"{field} entries cannot contain control characters")
        result.append(normalized)
    return list(dict.fromkeys(result))


def _market_display_label(config_id: str, resolutions: list[dict], symbol_to_coin, looks_exact) -> str:
    """Return a compact label while preserving scaled bases for collisions."""
    if "::" not in config_id and not looks_exact(config_id):
        return config_id
    if not resolutions:
        return config_id
    symbol = str(resolutions[0].get("symbol") or "")
    base = symbol.split("/", 1)[0].strip()
    if base.startswith("k") and base[1:].isupper():
        return f"1000{base[1:]}"
    if base and base[0].isdigit():
        return base
    return str(symbol_to_coin(symbol, verbose=False) or base or config_id)


def _resolve_market_identifier(modules: dict, identifier: str, exchanges: list[str], quote: str | None) -> dict:
    """Resolve one identifier through PB8 and retain the submitted config value."""
    qualified_exchange, _unqualified = modules["split_exchange_qualified_market_identifier"](identifier)
    if qualified_exchange is not None and qualified_exchange not in exchanges:
        return {
            "input": identifier,
            "normalized": identifier,
            "status": "invalid",
            "reason": "exchange_mismatch",
            "detail": f"market identifier targets {qualified_exchange}, which is not selected",
            "resolutions": [],
            "display": identifier,
        }

    resolutions = []
    failures = []
    for exchange in exchanges:
        try:
            symbol = modules["coin_to_symbol"](identifier, exchange, quote=quote, verbose=False)
            resolutions.append({"exchange": exchange, "symbol": symbol})
        except modules["AmbiguousMarketIdentifier"] as exc:
            failures.append(("ambiguous", str(exc)))
        except modules["MarketIdentifierExchangeMismatch"] as exc:
            failures.append(("exchange_mismatch", str(exc)))
        except modules["UnknownMarketIdentifier"] as exc:
            failures.append(("unknown", str(exc)))

    ambiguous = next((detail for reason, detail in failures if reason == "ambiguous"), "")
    if ambiguous:
        status = "invalid"
        reason = "ambiguous"
        detail = ambiguous
        resolutions = []
    elif resolutions:
        status = "valid"
        reason = "resolved"
        detail = ""
    else:
        status = "invalid"
        reason = failures[0][0] if failures else "unknown"
        detail = failures[0][1] if failures else f"market identifier {identifier!r} is unavailable"
    return {
        "input": identifier,
        "normalized": identifier,
        "status": status,
        "reason": reason,
        "detail": detail,
        "resolutions": resolutions,
        "display": _market_display_label(
            identifier,
            resolutions,
            modules["symbol_to_coin"],
            modules["looks_like_exact_market_identifier"],
        ),
    }


async def _market_identifiers(modules: dict, payload: dict) -> dict:
    """Return PB8's official collision-aware catalog and submitted-ID statuses."""
    exchanges = _validated_market_strings(payload.get("exchanges"), "exchanges", maximum=16, max_bytes=64)
    standard_exchanges = []
    for exchange in exchanges:
        if exchange.lower() == "fake":
            raise ValueError("fake exchange market resolution requires a PB8 scenario context")
        normalized = str(modules["to_standard_exchange_name"](exchange))
        if not normalized or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for char in normalized):
            raise ValueError(f"unsupported exchange name: {exchange}")
        if normalized not in standard_exchanges:
            standard_exchanges.append(normalized)
    if not standard_exchanges:
        raise ValueError("select at least one exchange")

    identifiers = _validated_market_strings(
        payload.get("identifiers", []), "identifiers", maximum=1000, max_bytes=256
    )
    quote_value = payload.get("quote")
    if quote_value is not None and not isinstance(quote_value, str):
        raise TypeError("quote must be a string or null")
    quote = str(quote_value or "").strip().upper() or None
    if quote is not None and (len(quote) > 16 or not quote.isalnum()):
        raise ValueError("quote must contain at most 16 letters or digits")

    marketss = await asyncio.gather(
        *[modules["load_markets"](exchange, verbose=False, quote=quote) for exchange in standard_exchanges]
    )
    eligible_by_exchange = {}
    exchange_markets_quotes = []
    for exchange, markets in zip(standard_exchanges, marketss):
        eligible = modules["filter_markets"](markets, exchange, quote=quote)[0]
        eligible_by_exchange[exchange] = set(eligible)
        exchange_markets_quotes.append((exchange, eligible, modules["get_quote"](exchange, quote)))
    symbols = sorted(modules["approved_all_market_identifiers"](exchange_markets_quotes))
    if len(standard_exchanges) == 1:
        prefix = f"{standard_exchanges[0]}::"
        symbols = [symbol[len(prefix):] if symbol.startswith(prefix) else symbol for symbol in symbols]
        symbols = sorted(set(symbols))
    catalog = []
    for config_id in symbols:
        resolved = _resolve_market_identifier(modules, config_id, standard_exchanges, quote)
        eligible_resolutions = [
            item
            for item in resolved["resolutions"]
            if item["symbol"] in eligible_by_exchange.get(item["exchange"], set())
        ]
        if not eligible_resolutions:
            continue
        resolved["resolutions"] = eligible_resolutions
        catalog.append(
            {
                "config_id": config_id,
                "coin": resolved["display"],
                "display": resolved["display"],
                "resolutions": eligible_resolutions,
            }
        )
    symbols = sorted(entry["config_id"] for entry in catalog)
    label_counts = {}
    for entry in catalog:
        label_counts[entry["display"]] = label_counts.get(entry["display"], 0) + 1
    for entry in catalog:
        if label_counts[entry["display"]] > 1 and "::" in entry["config_id"]:
            exchange, native_id = entry["config_id"].split("::", 1)
            entry["display"] = f"{entry['display']} ({exchange}: {native_id})"

    statuses = {}
    ambiguous_cross_exchange = {}
    collision_check_failed = False
    try:
        await modules["reject_cross_exchange_market_identifier_collisions"](
            identifiers, standard_exchanges, quote=quote, verbose=False
        )
    except modules["AmbiguousMarketIdentifier"]:
        # PB8 reports one conflicting identifier at a time; classify only on failure.
        collision_check_failed = True
    for identifier in identifiers if collision_check_failed else []:
        try:
            await modules["reject_cross_exchange_market_identifier_collisions"](
                [identifier], standard_exchanges, quote=quote, verbose=False
            )
        except modules["AmbiguousMarketIdentifier"] as exc:
            ambiguous_cross_exchange[identifier] = str(exc)
    for identifier in identifiers:
        if identifier in ambiguous_cross_exchange:
            statuses[identifier] = {
                "input": identifier,
                "normalized": identifier,
                "status": "invalid",
                "reason": "ambiguous",
                "detail": ambiguous_cross_exchange[identifier],
                "resolutions": [],
                "display": identifier,
            }
            continue
        status = _resolve_market_identifier(modules, identifier, standard_exchanges, quote)
        if status["status"] == "valid":
            eligible_resolutions = [
                item
                for item in status["resolutions"]
                if item["symbol"] in eligible_by_exchange.get(item["exchange"], set())
            ]
            if eligible_resolutions:
                status["resolutions"] = eligible_resolutions
            else:
                status.update(
                    status="invalid",
                    reason="inactive_or_ineligible",
                    detail=f"market identifier {identifier!r} does not select an active linear swap",
                    resolutions=[],
                )
        statuses[identifier] = status

    return {
        "contract_version": 1,
        "exchanges": standard_exchanges,
        "symbols": symbols,
        "catalog": catalog,
        "statuses": statuses,
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
    """Describe one PB8 scalar override leaf without accepting null."""
    if isinstance(value, bool):
        value_type = "boolean"
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        value_type = "number"
    elif isinstance(value, str):
        value_type = "string"
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
        for group in ("risk", "unstuck", "hsl"):
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
        "backend_contract": _optimizer_backend_contract(modules["backends"], metrics),
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


def _validate_optimizer_overrides(modules: dict, config: dict, base_config_path: str = "") -> None:
    """Exercise PB8's native override application for every optimizer side."""
    prepared = _prepare(modules, config, base_config_path)
    overrides = prepared.get("optimize", {}).get("enable_overrides", [])
    candidate = modules["apply_optimizer_overrides"](overrides, copy.deepcopy(prepared), None)
    for pside in sorted(candidate.get("bot", {})):
        candidate = modules["apply_optimizer_overrides"](overrides, candidate, pside)


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
        return _cached_optimize_metadata(modules, pb8_dir)
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
            result["config"] = migrated
            result["optimize_metadata"] = _cached_optimize_metadata(modules, pb8_dir)
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
        return {"ok": True, "result": handle(payload)}
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
