"""Isolated PB8-venv helper for Strategy Explorer native calculations."""

from __future__ import annotations

import asyncio
import bisect
import copy
import csv
import datetime as dt
import gzip
import json
import math
import sys
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Iterable

MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_SIM_CANDLES = 20_000
MAX_SIM_ORDERS = 2_000
MAX_MOVIE_FRAMES = 2_000
MAX_RESULT_FILLS_BYTES = 64 * 1024 * 1024
MAX_RESULT_FILL_ROWS = 1_000_000
SIDES = ("long", "short")
FILL_COLUMNS = (
    "index",
    "timestamp",
    "coin",
    "pnl",
    "fee_paid",
    "usd_total_balance",
    "btc_cash_wallet",
    "usd_cash_wallet",
    "btc_price",
    "qty",
    "price",
    "psize",
    "pprice",
    "type",
    "liquidity",
    "wallet_exposure",
    "twe_long",
    "twe_short",
    "twe_net",
)


def _load_pb8(pb8_dir: str) -> dict[str, Any]:
    """Insert the validated PB8 source directory and import runtime modules lazily."""
    root = Path(str(pb8_dir or "")).expanduser().resolve()
    if root != Path.cwd().resolve():
        raise ValueError("PB8 helper directory does not match its working directory")
    src = root / "src"
    if not src.is_dir() or src.is_symlink():
        raise ValueError("PB8 src directory is unavailable")
    src_text = str(src)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)

    import backtest  # type: ignore
    import passivbot_rust  # type: ignore
    from config.load import prepare_config  # type: ignore
    from config.schema import get_template_config  # type: ignore
    from config.shared_bot import (  # type: ignore
        BOT_GROUP_FIELD_MAP,
        BOT_SHARED_GROUPS,
        flatten_shared_bot_side,
        get_grouped_bot_value,
    )
    from config.strategy_spec import get_strategy_spec, get_supported_strategy_kinds  # type: ignore
    from config_utils import sanitize_prepared_config_for_dump  # type: ignore
    from materialized_cache import release_materialized_payload  # type: ignore

    return {
        "root": root,
        "backtest": backtest,
        "pbr": passivbot_rust,
        "prepare_config": prepare_config,
        "template_config": get_template_config,
        "sanitize": sanitize_prepared_config_for_dump,
        "shared_group_map": BOT_GROUP_FIELD_MAP,
        "shared_groups": BOT_SHARED_GROUPS,
        "flatten_shared": flatten_shared_bot_side,
        "grouped_value": get_grouped_bot_value,
        "get_spec": get_strategy_spec,
        "get_kinds": get_supported_strategy_kinds,
        "release": release_materialized_payload,
    }


def _json_safe(value: Any) -> Any:
    """Convert numpy-like values and non-finite floats to strict JSON values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "tolist"):
        return _json_safe(value.tolist())
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    return str(value)


def _canonicalize(config: Any, modules: dict[str, Any]) -> dict[str, Any]:
    """Prepare and sanitize one canonical PB8 configuration."""
    if not isinstance(config, dict):
        raise ValueError("config must be an object")
    candidate = copy.deepcopy(config)
    pbgui_metadata = candidate.pop("pbgui", None)
    if pbgui_metadata is not None and not isinstance(pbgui_metadata, dict):
        raise ValueError("pbgui must be an object")
    prepared = modules["prepare_config"](
        candidate,
        verbose=False,
        log_config_transforms=False,
        target="canonical",
        runtime=None,
        raw_snapshot=candidate,
        effective_snapshot=candidate,
    )
    sanitized = modules["sanitize"](prepared)
    if not isinstance(sanitized, dict):
        raise RuntimeError("PB8 config sanitizer returned no object")
    if pbgui_metadata is not None:
        sanitized["pbgui"] = copy.deepcopy(pbgui_metadata)
    return sanitized


def _identifier(value: Any, label: str) -> str:
    """Validate a market identifier before using it in cache or config lookups."""
    text = str(value or "").strip()
    if (
        not text
        or len(text.encode("utf-8")) > 128
        or text in {".", ".."}
        or any(char in text for char in ("/", "\\", "\x00"))
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
    ):
        raise ValueError(f"invalid {label}")
    return text


def _configured_exchanges(config: dict[str, Any]) -> list[str]:
    """Return safe configured exchange names in deterministic order."""
    raw = (config.get("backtest") or {}).get("exchanges") or []
    if isinstance(raw, str):
        raw = [raw]
    result = []
    for item in raw if isinstance(raw, list) else []:
        try:
            value = _identifier(item, "exchange").lower()
        except ValueError:
            continue
        if value not in result and value != "combined":
            result.append(value)
    return result


def _approved_by_side(config: dict[str, Any]) -> dict[str, list[str]]:
    """Return canonical approved coin lists without performing discovery."""
    approved = (config.get("live") or {}).get("approved_coins") or {}
    result: dict[str, list[str]] = {side: [] for side in SIDES}
    if isinstance(approved, list):
        approved = {side: approved for side in SIDES}
    if not isinstance(approved, dict):
        return result
    for side in SIDES:
        values = approved.get(side) or []
        if not isinstance(values, list):
            continue
        for item in values:
            try:
                coin = _identifier(item, "coin").upper()
            except ValueError:
                continue
            if coin not in result[side]:
                result[side].append(coin)
    return result


def _market_choice(config: dict[str, Any], options: dict[str, Any]) -> tuple[str, str]:
    """Resolve one selected exchange and coin from bounded config-approved values."""
    exchanges = _configured_exchanges(config)
    if not exchanges:
        raise ValueError("configuration has no approved backtest exchange")
    exchange = str(options.get("exchange") or (exchanges[0] if exchanges else "")).strip().lower()
    exchange = _identifier(exchange, "exchange")
    if exchange not in exchanges:
        raise ValueError("selected exchange is not approved by the configuration")
    approved = _approved_by_side(config)
    approved_coins = list(dict.fromkeys([*approved["long"], *approved["short"]]))
    if not approved_coins:
        raise ValueError("configuration has no approved coin")
    coin = str(options.get("coin") or (approved_coins[0] if approved_coins else "")).strip().upper()
    coin = _identifier(coin, "coin")
    if approved_coins and coin not in approved_coins:
        raise ValueError("selected coin is not approved by the configuration")
    return exchange, coin


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    """Return an integer clamped to one public operation bound."""
    try:
        parsed = int(float(value))
    except (TypeError, ValueError, OverflowError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _date(value: Any, fallback: dt.date) -> dt.date:
    """Parse a YYYY-MM-DD value, including PB8's ``now`` marker."""
    text = str(value or "").strip().lower()
    if not text or text == "now":
        return fallback
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"invalid date {text!r}; expected YYYY-MM-DD") from exc


def _restrict_config(
    config: dict[str, Any], options: dict[str, Any], modules: dict[str, Any]
) -> tuple[dict[str, Any], str, str, int]:
    """Restrict a canonical config to one market and bounded native replay window."""
    canonical = _canonicalize(config, modules)
    exchange, coin = _market_choice(canonical, options)
    backtest = canonical.setdefault("backtest", {})
    live = canonical.setdefault("live", {})
    approved = _approved_by_side(canonical)
    backtest["exchanges"] = [exchange]
    backtest["suite_enabled"] = False
    backtest["scenarios"] = []
    backtest["coins"] = {exchange: [coin]}
    backtest["cache_dir"] = {}
    max_candles = _bounded_int(
        options.get("sim_max_candles", options.get("compare_max_candles", MAX_SIM_CANDLES)),
        2_000,
        10,
        MAX_SIM_CANDLES,
    )
    today = dt.datetime.now(dt.timezone.utc).date()
    configured_start = _date(options.get("start_date") or backtest.get("start_date"), today - dt.timedelta(days=1))
    configured_end = _date(backtest.get("end_date"), today)
    max_end = configured_start + dt.timedelta(days=max(1, math.ceil(max_candles / 1440)) + 1)
    end_date = min(configured_end, max_end, today)
    if end_date <= configured_start:
        end_date = min(today, configured_start + dt.timedelta(days=1))
    backtest["start_date"] = configured_start.isoformat()
    backtest["end_date"] = end_date.isoformat()
    live["approved_coins"] = {
        side: [coin] if coin in approved[side] else [] for side in SIDES
    }
    overrides = canonical.get("coin_overrides")
    canonical["coin_overrides"] = (
        {coin: copy.deepcopy(overrides[coin])}
        if isinstance(overrides, dict) and isinstance(overrides.get(coin), dict)
        else {}
    )
    return _canonicalize(canonical, modules), exchange, coin, max_candles


def _iter_leaves(value: Any, prefix: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], Any]]:
    """Yield nested config leaves with stable dotted paths."""
    if isinstance(value, dict):
        for key in sorted(value):
            yield from _iter_leaves(value[key], (*prefix, str(key)))
    else:
        yield prefix, value


def _field_type(value: Any, spec: dict[str, Any]) -> str:
    """Map dynamic PB8 metadata to the frontend's simple field types."""
    if isinstance(value, bool):
        return "bool"
    options = spec.get("allowed_values") or spec.get("choices") or spec.get("options")
    if isinstance(options, (list, tuple)):
        return "select"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    return "string"


def _strategy_metadata(config: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Build UI metadata from Rust strategy specs and PB8 shared bot groups."""
    kinds = list(modules["get_kinds"]())
    active_kind = str((config.get("live") or {}).get("strategy_kind") or (kinds[0] if kinds else ""))
    specs = {kind: _json_safe(modules["get_spec"](kind)) for kind in kinds}
    active_spec = specs.get(active_kind) or {}
    param_groups: dict[str, list[str]] = {str(group): [] for group in modules["shared_groups"]}
    field_meta: dict[str, dict[str, Any]] = {}
    for group, mapping in modules["shared_group_map"].items():
        for local_name, flat_name in mapping.items():
            sample = ((config.get("bot") or {}).get("long") or {}).get(group, {}).get(local_name)
            leaves = list(_iter_leaves(sample)) if isinstance(sample, dict) else [((), sample)]
            for nested_path, nested_value in leaves:
                suffix = ".".join(nested_path)
                field_name = f"{flat_name}.{suffix}" if suffix else str(flat_name)
                config_path = f"{group}.{local_name}.{suffix}" if suffix else f"{group}.{local_name}"
                param_groups.setdefault(str(group), []).append(field_name)
                field_meta[field_name] = {
                    "label": field_name.replace("_", " ").replace(".", " / ").title(),
                    "type": _field_type(nested_value, {}),
                    "path": config_path,
                    "group": str(group),
                }
    for raw in [*(active_spec.get("parameters") or []), *(active_spec.get("fixed_parameters") or [])]:
        if not isinstance(raw, dict):
            continue
        path = [str(part) for part in raw.get("config_path") or []]
        leaf = ".".join(path[2:]) if len(path) >= 3 else str(raw.get("name") or "")
        if not leaf:
            continue
        group = path[2] if len(path) >= 4 else "strategy"
        param_groups.setdefault(group, []).append(leaf)
        meta = {
            "label": str(raw.get("label") or raw.get("name") or leaf).replace("_", " ").title(),
            "type": _field_type(raw.get("default"), raw),
            "path": f"bot.long.strategy.{active_kind}.{leaf}",
            "group": group,
            "strategy_kind": active_kind,
        }
        bounds = raw.get("bounds")
        if isinstance(bounds, (list, tuple)) and len(bounds) >= 2:
            meta.update({"min": bounds[0], "max": bounds[1]})
            if len(bounds) >= 3:
                meta["step"] = bounds[2]
        choices = raw.get("allowed_values") or raw.get("choices") or raw.get("options")
        if isinstance(choices, (list, tuple)):
            meta["options"] = list(choices)
        field_meta[leaf] = meta
    for group in param_groups:
        param_groups[group] = list(dict.fromkeys(param_groups[group]))
    group_order = ("entry", "close", "strategy", "risk", "forager", "unstuck", "hsl")
    ordered_groups = [
        {
            "key": group,
            "label": group.replace("_", " ").title(),
            "fields": param_groups[group],
        }
        for group in group_order
        if param_groups.get(group)
    ]
    ordered_groups.extend(
        {
            "key": group,
            "label": group.replace("_", " ").title(),
            "fields": fields,
        }
        for group, fields in param_groups.items()
        if fields and group not in group_order
    )
    return {
        "active_kind": active_kind,
        "supported_kinds": kinds,
        "strategy_specs": specs,
        "param_groups": ordered_groups,
        "param_field_meta": field_meta,
    }


def _capabilities(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Return the installed PB8 Strategy Explorer feature contract."""
    config = request.get("config")
    canonical = _canonicalize(
        config if isinstance(config, dict) else modules["template_config"](), modules
    )
    metadata = _strategy_metadata(canonical, modules)
    return {
        "ok": True,
        "engine": "pb8_engine",
        "operations": ["capabilities", "markets", "snapshot", "replay", "compare", "movie"],
        "limits": {
            "request_bytes": MAX_REQUEST_BYTES,
            "sim_candles": MAX_SIM_CANDLES,
            "sim_orders": MAX_SIM_ORDERS,
            "movie_frames": MAX_MOVIE_FRAMES,
        },
        "simulation_modes": [{"key": "pb8_engine", "label": "PB8 Native Replay"}],
        "strategy": metadata,
        "canonical_config": canonical,
    }


def _markets(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """List config-approved markets without network discovery."""
    canonical = _canonicalize(request.get("config"), modules)
    approved = _approved_by_side(canonical)
    configured_coins = list(dict.fromkeys([*approved["long"], *approved["short"]]))
    exchanges = _configured_exchanges(canonical)
    return {
        "ok": True,
        "exchanges": exchanges,
        "coins_by_exchange": {exchange: list(configured_coins) for exchange in exchanges},
        "source": "PB8 config-approved markets",
        "network_used": False,
    }


def _native_data(
    config: dict[str, Any],
    exchange: str,
    coin: str,
    max_candles: int,
    modules: dict[str, Any],
    *,
    start_time: str = "00:00",
) -> dict[str, Any]:
    """Prepare native HLCVs while retaining warmup and bounding the tradable window."""
    backtest = modules["backtest"]
    result = asyncio.run(backtest.prepare_hlcvs_mss(config, exchange))
    coins, hlcvs, mss, _results_path, _cache_dir, btc_prices, timestamps = result
    coins = list(coins)
    if coin not in coins:
        raise ValueError(f"PB8 HLCV preparation did not return selected coin {coin}")
    config.setdefault("backtest", {}).setdefault("coins", {})[exchange] = [coin]
    coin_idx = coins.index(coin)
    import numpy as np  # type: ignore

    timestamps_arr = np.asarray(timestamps, dtype=np.int64)
    time_text = str(start_time or "00:00").strip()
    try:
        requested_dt = dt.datetime.fromisoformat(
            f"{config['backtest']['start_date']}T{time_text[:5]}:00"
        ).replace(tzinfo=dt.timezone.utc)
    except ValueError as exc:
        raise ValueError("invalid start_time; expected HH:MM") from exc
    requested_ms = int(requested_dt.timestamp() * 1000)
    trade_idx = int(np.searchsorted(timestamps_arr, requested_ms, side="left"))
    if trade_idx >= len(timestamps_arr):
        modules["release"](hlcvs)
        raise ValueError("selected replay start is after the available PB8 candle range")
    end_idx = min(len(timestamps_arr), trade_idx + max_candles)
    if end_idx <= 0:
        raise ValueError("PB8 returned no candles in the selected replay window")
    selected_hlcvs = np.asarray(hlcvs)[:end_idx, coin_idx : coin_idx + 1, :]
    selected_timestamps = timestamps_arr[:end_idx]
    selected_btc = np.asarray(btc_prices)[:end_idx]
    selected_mss = {coin: copy.deepcopy(mss[coin]), "__meta__": copy.deepcopy(mss.get("__meta__", {}))}
    selected_mss[coin]["first_valid_index"] = min(int(selected_mss[coin].get("first_valid_index", 0)), end_idx)
    selected_mss[coin]["last_valid_index"] = min(int(selected_mss[coin].get("last_valid_index", end_idx - 1)), end_idx - 1)
    return {
        "coins": [coin],
        "hlcvs": selected_hlcvs,
        "mss": selected_mss,
        "btc_prices": selected_btc,
        "timestamps": selected_timestamps,
        "trade_idx": min(trade_idx, max(0, end_idx - 1)),
        "source_hlcvs": hlcvs,
    }


def _candles(data: dict[str, Any], *, start: int = 0, limit: int | None = None) -> list[dict[str, Any]]:
    """Convert native high/low/close/volume rows to PB7-compatible candle objects."""
    rows = data["hlcvs"][:, 0, :]
    timestamps = data["timestamps"]
    end = len(rows) if limit is None else min(len(rows), start + max(0, limit))
    output = []
    previous_close = float(rows[max(0, start - 1), 2]) if len(rows) else 0.0
    for idx in range(start, end):
        high, low, close = (float(rows[idx, col]) for col in range(3))
        volume = float(rows[idx, 3]) if rows.shape[1] > 3 else 0.0
        open_price = previous_close if previous_close > 0 else close
        timestamp_ms = int(timestamps[idx])
        output.append(
            {
                "timestamp": dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "timestamp_ms": timestamp_ms,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
        previous_close = close
    return output


def _ema(values: list[float], span: float) -> float:
    """Compute a conventional EMA value for one positive dynamic span."""
    if not values:
        return 0.0
    alpha = 2.0 / (max(1.0, float(span)) + 1.0)
    current = float(values[0])
    for value in values[1:]:
        current += alpha * (float(value) - current)
    return current


def _ema_spans(*mappings: dict[str, Any]) -> dict[str, dict[str, set[float]]]:
    """Infer required EMA bundles from version-neutral leaf-name heuristics."""
    result = {interval: {kind: set() for kind in ("close", "volume", "log_range")} for interval in ("m1", "h1")}
    for mapping in mappings:
        for path, value in _iter_leaves(mapping):
            name = ".".join(path).lower()
            if "ema_span" not in name:
                continue
            try:
                span = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(span) or span <= 0:
                continue
            interval = "h1" if "1h" in name or "h1" in name else "m1"
            kind = "volume" if "volume" in name else "log_range" if ("volatility" in name or "log_range" in name) else "close"
            result[interval][kind].add(span)
    return result


def _ema_bundle(
    data: dict[str, Any],
    bot_params: dict[str, Any],
    strategy_params: dict[str, Any],
    *,
    end_index: int | None = None,
) -> dict[str, Any]:
    """Compute all inferred m1/h1 close, volume, and log-range EMA values."""
    rows = data["hlcvs"][:, 0, :]
    if end_index is not None:
        rows = rows[: max(1, min(len(rows), int(end_index) + 1))]
    spans = _ema_spans(bot_params, strategy_params)
    for side in SIDES:
        side_params = strategy_params.get(side) if isinstance(strategy_params.get(side), dict) else {}
        try:
            ema_span_0 = float(side_params.get("ema_span_0"))
            ema_span_1 = float(side_params.get("ema_span_1"))
        except (TypeError, ValueError):
            continue
        if all(math.isfinite(value) and value > 0.0 for value in (ema_span_0, ema_span_1)):
            spans["m1"]["close"].add(math.sqrt(ema_span_0 * ema_span_1))

    def series(source: Any) -> dict[str, list[float]]:
        values = source.tolist() if hasattr(source, "tolist") else list(source)
        return {
            "close": [float(row[2]) for row in values],
            "volume": [float(row[3]) if len(row) > 3 else 0.0 for row in values],
            "log_range": [math.log(max(float(row[0]), 1e-300) / max(float(row[1]), 1e-300)) for row in values],
        }

    m1 = series(rows)
    h1_rows = []
    for offset in range(0, len(rows), 60):
        chunk = rows[offset : offset + 60]
        if len(chunk):
            h1_rows.append([max(chunk[:, 0]), min(chunk[:, 1]), chunk[-1, 2], sum(chunk[:, 3])])
    h1 = series(h1_rows)
    bundle = {}
    for interval, source in (("m1", m1), ("h1", h1)):
        bundle[interval] = {
            kind: [[span, _ema(source[kind], span)] for span in sorted(spans[interval][kind])]
            for kind in ("close", "volume", "log_range")
        }
    return bundle


def _orchestrator_input(
    *,
    balance: float,
    price: float,
    timestamp_ms: int,
    exchange_params: dict[str, Any],
    bot_params: dict[str, Any],
    strategy_params: dict[str, Any],
    strategy_kind: str,
    emas: dict[str, Any],
    positions: dict[str, dict[str, float]],
    backtest_params: dict[str, Any],
) -> dict[str, Any]:
    """Build one complete strict Rust orchestrator JSON input."""
    trailing = {"min_since_open": price, "max_since_min": price, "max_since_open": price, "min_since_max": price}
    symbol = {
        "symbol_idx": 0,
        "order_book": {"bid": price, "ask": price},
        "exchange": exchange_params,
        "tradable": True,
        "next_candle": None,
        "effective_min_cost": max(float(exchange_params.get("min_cost") or 0.0), 0.0),
        "emas": emas,
    }
    for side in SIDES:
        symbol[side] = {
            "mode": None,
            "position": positions[side],
            "trailing": trailing,
            "last_increase_fill_timestamp_ms": None,
            "bot_params": bot_params[side],
            "strategy_params": strategy_params[side],
        }
    return {
        "timestamp_ms": timestamp_ms,
        "balance": balance,
        "balance_raw": balance,
        "global": {
            "filter_by_min_effective_cost": bool(backtest_params.get("filter_by_min_effective_cost", False)),
            "market_orders_allowed": bool(backtest_params.get("market_orders_allowed", False)),
            "market_order_near_touch_threshold": float(backtest_params.get("market_order_near_touch_threshold", 0.0)),
            "panic_close_market": False,
            "auto_unstuck_allowed": False,
            "unstuck_allowance_long": 0.0,
            "unstuck_allowance_short": 0.0,
            "max_realized_loss_pct": float(backtest_params.get("max_realized_loss_pct", 1.0)),
            "realized_pnl_cumsum_max": 0.0,
            "realized_pnl_cumsum_last": 0.0,
            "sort_global": True,
            "global_bot_params": bot_params,
            "hedge_mode": bool(backtest_params.get("hedge_mode", True)),
            "strategy_kind": strategy_kind,
        },
        "symbols": [symbol],
        "peek_hints": None,
    }


def _orchestrator_bot_params(bot_params: dict[str, Any]) -> dict[str, Any]:
    """Remove Python-only aliases and flatten HSL tiers for Rust's strict BotParams JSON."""
    result = copy.deepcopy(bot_params)
    result.pop("forager_volatility_ema_span_1m", None)
    result.pop("forager_volume_ema_span_1m", None)
    result.pop("filter_volume_drop_pct", None)
    tier_ratios = result.pop("hsl_tier_ratios", None)
    if isinstance(tier_ratios, dict):
        result["hsl_tier_ratio_yellow"] = float(tier_ratios.get("yellow") or 0.0)
        result["hsl_tier_ratio_orange"] = float(tier_ratios.get("orange") or 0.0)
    return result


def _normalize_orders(orders: Any, balance: float, c_mult: float) -> list[dict[str, Any]]:
    """Normalize native Rust order objects to the PB7 Strategy Explorer shape."""
    result = []
    exposure = 0.0
    for idx, raw in enumerate(orders if isinstance(orders, list) else []):
        if not isinstance(raw, dict):
            continue
        qty = float(raw.get("qty") or 0.0)
        price = float(raw.get("price") or 0.0)
        exposure += abs(qty * price * c_mult) / max(balance, 1e-12)
        result.append(
            {
                "index": idx + 1,
                "qty": qty,
                "price": price,
                "order_type": str(raw.get("order_type") or ""),
                "max_twe_pct_after": round(exposure * 100.0, 8),
            }
        )
    return result


def _snapshot(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Build a supplied-state native ideal-order snapshot for one selected market."""
    options = request.get("options") if isinstance(request.get("options"), dict) else {}
    canonical_config = _canonicalize(request.get("config"), modules)
    config, exchange, coin, max_candles = _restrict_config(canonical_config, options, modules)
    approved = _approved_by_side(canonical_config)
    data = _native_data(
        config,
        exchange,
        coin,
        max_candles,
        modules,
        start_time=str(options.get("start_time") or "00:00"),
    )
    try:
        backtest = modules["backtest"]
        bot_list, strategy_list, exchange_list, backtest_params = backtest.prep_backtest_args(config, data["mss"], exchange)
        bot_params = {
            side: _orchestrator_bot_params(bot_list[0][side]) for side in SIDES
        }
        strategy_params = strategy_list[0]
        exchange_params = exchange_list[0]
        for side in SIDES:
            local = bot_params[side]
            if float(local.get("wallet_exposure_limit") or 0.0) < 0.0:
                total_limit = max(0.0, float(local.get("total_wallet_exposure_limit") or 0.0))
                n_positions = max(1, int(local.get("n_positions") or 1))
                local["wallet_exposure_limit"] = total_limit / n_positions
        requested_exchange_params = options.get("exchange_params")
        if isinstance(requested_exchange_params, dict):
            for key, value in requested_exchange_params.items():
                if key not in exchange_params:
                    continue
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(numeric) and numeric >= 0.0:
                    exchange_params[key] = numeric
        selected_idx = data["trade_idx"]
        try:
            context_days = float(options.get("context_days", 5.0))
        except (TypeError, ValueError, OverflowError):
            context_days = 5.0
        context_days = max(0.5, min(60.0, context_days))
        context_candles = max(1, int(math.ceil(context_days * 1440.0)))
        row_idx = min(len(data["timestamps"]) - 1, selected_idx + context_candles - 1)
        price = float(data["hlcvs"][row_idx, 0, 2])
        balance = float(options.get("balance") or (config.get("backtest") or {}).get("starting_balance") or 1000.0)
        emas = _ema_bundle(data, bot_params, strategy_params, end_index=row_idx)
        strategy_kind = str(backtest_params.get("strategy_kind") or (config.get("live") or {}).get("strategy_kind") or "")
        flat_positions = {side: {"size": 0.0, "price": 0.0} for side in SIDES}
        flat_input = _orchestrator_input(
            balance=balance,
            price=price,
            timestamp_ms=int(data["timestamps"][row_idx]),
            exchange_params=exchange_params,
            bot_params=bot_params,
            strategy_params=strategy_params,
            strategy_kind=strategy_kind,
            emas=emas,
            positions=flat_positions,
            backtest_params=backtest_params,
        )
        flat_output = json.loads(modules["pbr"].compute_ideal_orders_json(json.dumps(flat_input, allow_nan=False)))
        c_mult = max(float(exchange_params.get("c_mult") or 1.0), 1e-12)
        positioned_outputs = {}
        synthetic_positions = {}
        for side, sign in (("long", 1.0), ("short", -1.0)):
            limit = float(bot_params[side].get("wallet_exposure_limit") or bot_params[side].get("total_wallet_exposure_limit") or 0.1)
            size = max(balance * max(0.01, min(abs(limit), 1.0)) / max(price * c_mult, 1e-12), float(exchange_params.get("min_qty") or 0.0))
            synthetic_positions[side] = {"size": sign * size, "price": price}
            close_positions = copy.deepcopy(flat_positions)
            close_positions[side] = synthetic_positions[side]
            positioned_input = _orchestrator_input(
                balance=balance,
                price=price,
                timestamp_ms=int(data["timestamps"][row_idx]),
                exchange_params=exchange_params,
                bot_params=bot_params,
                strategy_params=strategy_params,
                strategy_kind=strategy_kind,
                emas=emas,
                positions=close_positions,
                backtest_params=backtest_params,
            )
            positioned_outputs[side] = {
                "input": positioned_input,
                "output": json.loads(
                    modules["pbr"].compute_ideal_orders_json(
                        json.dumps(positioned_input, allow_nan=False)
                    )
                ),
            }
        metadata = _strategy_metadata(config, modules)
        flat_orders = flat_output.get("orders") if isinstance(flat_output, dict) else []
        sides = {}
        for side in SIDES:
            positioned_output = positioned_outputs[side]["output"]
            positioned_orders = positioned_output.get("orders") if isinstance(positioned_output, dict) else []
            entries_raw = [order for order in flat_orders or [] if str(order.get("pside") or "").lower() == side and "entry" in str(order.get("order_type") or "").lower()]
            closes_raw = [order for order in positioned_orders or [] if str(order.get("pside") or "").lower() == side and "close" in str(order.get("order_type") or "").lower()]
            entries = _normalize_orders(entries_raw, balance, c_mult)
            closes = _normalize_orders(closes_raw, balance, c_mult)
            canonical_side = copy.deepcopy((canonical_config.get("bot") or {}).get(side) or {})
            visual = modules["flatten_shared"](canonical_side)
            active_strategy = ((canonical_side.get("strategy") or {}).get(strategy_kind) or {})
            visual.update({"strategy": copy.deepcopy(active_strategy)})
            for path, value in _iter_leaves(active_strategy):
                dotted = ".".join(path)
                visual["_".join(path)] = copy.deepcopy(value)
                alias = {
                    "entry.threshold_base_pct": "entry_trailing_threshold_pct",
                    "entry.retracement_base_pct": "entry_trailing_retracement_pct",
                    "close.threshold_base_pct": "close_trailing_threshold_pct",
                    "close.retracement_base_pct": "close_trailing_retracement_pct",
                    "close.qty_pct": "close_trailing_qty_pct",
                }.get(dotted)
                if alias:
                    visual[alias] = copy.deepcopy(value)
            active = (
                coin in approved[side]
                and bool(modules["grouped_value"](canonical_side, "n_positions", 0))
                and float(modules["grouped_value"](canonical_side, "total_wallet_exposure_limit", 0.0) or 0.0) > 0.0
            )
            if not active:
                entries = []
                closes = []
            sides[side] = {
                "active": active,
                "params": canonical_side,
                "visual_params": visual,
                "modes": {"entry": "Native", "close": "Native"},
                "orders": {
                    "entries": entries,
                    "closes": closes,
                    "normal_entries": entries,
                    "gridonly_entries": entries,
                    "gridonly_closes": closes,
                },
                "summary": {
                    "entry_orders": len(entries),
                    "close_orders": len(closes),
                    "entry_avg_price": sum(item["price"] for item in entries) / len(entries) if entries else 0.0,
                    "entry_grid_pct": 0.0,
                    "wallet_exposure_limit_per_position": float(bot_params[side].get("wallet_exposure_limit") or 0.0),
                },
                "debug": {
                    "exchange_params": exchange_params,
                    "state_params": {
                        "balance": balance,
                        "entry_volatility_logrange_ema_1h": next((value for _span, value in emas["h1"]["log_range"]), 0.0),
                        "supplied_state": flat_positions[side],
                        "hypothetical_close_state": synthetic_positions[side],
                    },
                    "entry_input": flat_input,
                    "entry_output_decoded": entries_raw,
                    "close_input": positioned_outputs[side]["input"],
                    "close_output_decoded": closes_raw,
                },
            }
        candles = _candles(data, start=selected_idx, limit=context_candles)
        selected_start_ms = int(data["timestamps"][selected_idx])
        grid_time_ms = int(data["timestamps"][row_idx])
        return {
            "ok": True,
            "source": "posted",
            "title": f"PB8 {strategy_kind}",
            "market": {
                "exchange": exchange,
                "coin": coin,
                "reference_price": price,
                "engine_status": "PB8 native ideal orders",
                "ohlcv_status": "PB8 local/native HLCV",
                "metadata": {
                    "state_model": "supplied flat state for entries and representative hypothetical positions for closes",
                    "ohlcv": {
                        "rows": len(data["timestamps"]),
                        "selected_start": dt.datetime.fromtimestamp(selected_start_ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                        "grid_time": dt.datetime.fromtimestamp(grid_time_ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                        "context_truncated": len(candles) < context_candles,
                    },
                    "market_settings": data["mss"][coin],
                },
            },
            "labels": {"engine": "PB8 Backtest Engine", "snapshot": "Native ideal orders"},
            "param_groups": metadata["param_groups"],
            "param_field_meta": metadata["param_field_meta"],
            "sides": sides,
            "candles": candles,
            "config": canonical_config,
            "options": options,
            "messages": [
                {
                    "level": "info",
                    "text": "Entry orders use a supplied flat state; close orders use a representative hypothetical position and are not a live account forecast.",
                }
            ],
        }
    finally:
        modules["release"](data["source_hlcvs"])


def _side_for_fill(order_type: str, qty: float, psize: float) -> str:
    """Classify a native fill side using type first and signed state as fallback."""
    lowered = order_type.lower()
    if "long" in lowered:
        return "long"
    if "short" in lowered:
        return "short"
    if psize < 0 or (psize == 0 and qty < 0 and "entry" in lowered):
        return "short"
    return "long"


def _normalize_native_fills(fills: Any, max_orders: int) -> dict[str, list[dict[str, Any]]]:
    """Normalize PB8's native 19-column fills into the PB7 GUI event contract."""
    output = {side: [] for side in SIDES}
    rows = fills.tolist() if hasattr(fills, "tolist") else list(fills or [])
    displayed = 0
    for row in rows:
        if displayed >= max_orders:
            break
        if not isinstance(row, (list, tuple)) or len(row) < len(FILL_COLUMNS):
            continue
        item = dict(zip(FILL_COLUMNS, row))
        timestamp_ms = int(float(item["timestamp"]))
        order_type = str(item["type"] or "")
        event = {
            "timestamp": dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "timestamp_ms": timestamp_ms,
            "event": order_type,
            "order_type": order_type,
            "coin": str(item["coin"] or ""),
            "qty": float(item["qty"] or 0.0),
            "price": float(item["price"] or 0.0),
            "pos_size": float(item["psize"] or 0.0),
            "pos_price": float(item["pprice"] or 0.0),
            "wallet_balance": float(item["usd_total_balance"] or 0.0),
            "wallet_exposure": float(item["wallet_exposure"] or 0.0),
            "pnl": float(item["pnl"] or 0.0),
            "fee_paid": float(item["fee_paid"] or 0.0),
        }
        side = _side_for_fill(order_type, event["qty"], event["pos_size"])
        output[side].append(event)
        displayed += 1
    return output


def _replay(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Run one bounded synchronous PB8 native backtest without writing result artifacts."""
    options = request.get("options") if isinstance(request.get("options"), dict) else {}
    if str(options.get("sim_start_state") or "flat").strip().lower() == "manual":
        raise ValueError("PB8 Native Replay does not support manual starting positions")
    canonical_config = _canonicalize(request.get("config"), modules)
    config, exchange, coin, max_candles = _restrict_config(canonical_config, options, modules)
    try:
        requested_balance = float(options.get("balance"))
    except (TypeError, ValueError, OverflowError):
        requested_balance = 0.0
    if math.isfinite(requested_balance) and requested_balance > 0.0:
        config.setdefault("backtest", {})["starting_balance"] = requested_balance
    data = _native_data(
        config,
        exchange,
        coin,
        max_candles,
        modules,
        start_time=str(options.get("start_time") or "00:00"),
    )
    try:
        payload = modules["backtest"].build_backtest_payload(
            data["hlcvs"], data["mss"], config, exchange, data["btc_prices"], data["timestamps"]
        )
        payload.backtest_params["requested_start_timestamp_ms"] = int(
            data["timestamps"][data["trade_idx"]]
        )
        aggregate_timestamps = payload.bundle.timestamps
        requested_start_index = bisect.bisect_left(
            [int(timestamp) for timestamp in aggregate_timestamps],
            payload.backtest_params["requested_start_timestamp_ms"],
        )
        payload.backtest_params["trade_start_indices"] = [
            max(int(index), requested_start_index)
            for index in payload.backtest_params.get("trade_start_indices", [])
        ]
        fills, equities, analysis = modules["backtest"].execute_backtest(payload, config)
        max_orders = _bounded_int(options.get("sim_max_orders", options.get("compare_max_orders", MAX_SIM_ORDERS)), 200, 1, MAX_SIM_ORDERS)
        native_fill_count = len(fills) if hasattr(fills, "__len__") else 0
        events = _normalize_native_fills(fills, max_orders)
        displayed_fill_count = sum(len(values) for values in events.values())
        displayed_fill_end_timestamp_ms = max(
            (
                int(event.get("timestamp_ms") or 0)
                for side_events in events.values()
                for event in side_events
            ),
            default=0,
        )
        market_settings = data["mss"].get(coin) if isinstance(data.get("mss"), dict) else {}
        market_settings = market_settings if isinstance(market_settings, dict) else {}
        equities_rows = equities.tolist() if hasattr(equities, "tolist") else list(equities or [])
        normalized_equities = [
            {
                "timestamp": dt.datetime.fromtimestamp(float(row[0]) / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "wallet_equity": float(row[1]),
                "btc_equity": float(row[2]) if len(row) > 2 else None,
                "strategy_equity": float(row[3]) if len(row) > 3 else float(row[1]),
            }
            for row in equities_rows
            if isinstance(row, (list, tuple)) and len(row) >= 2
        ]
        processed_end_idx = max(data["trade_idx"], len(data["timestamps"]) - 2)
        return {
            "ok": True,
            "mode": "pb8_engine",
            "labels": {"engine": "PB8 Backtest Engine", "source": "native replay"},
            "events": events,
            "equities": normalized_equities,
            "analysis": _json_safe(analysis),
            "metadata": {
                "exchange": exchange,
                "coin": coin,
                "engine": "pb8_engine",
                "candle_count": len(data["timestamps"]),
                "warmup_candles": data["trade_idx"],
                "fill_count": displayed_fill_count,
                "displayed_fill_count": displayed_fill_count,
                "total_fill_count": native_fill_count,
                "fills_truncated": native_fill_count > displayed_fill_count,
                "displayed_fill_end_timestamp_ms": displayed_fill_end_timestamp_ms,
                "price_step": float(market_settings.get("price_step") or 0.0),
                "qty_step": float(market_settings.get("qty_step") or 0.0),
                "start_timestamp_ms": int(data["timestamps"][data["trade_idx"]]),
                "end_timestamp_ms": int(data["timestamps"][processed_end_idx]),
            },
            "candles": _candles(
                data,
                start=data["trade_idx"],
                limit=max(1, processed_end_idx - data["trade_idx"] + 1),
            ),
            "config": canonical_config,
            "message": f"PB8 native replay finished with {displayed_fill_count} displayed fills.",
        }
    finally:
        modules["release"](data["source_hlcvs"])


def _safe_result_dir(raw_path: Any, modules: dict[str, Any]) -> Path:
    """Resolve a result below PB8 backtests or PBGui archives and reject symlinks."""
    result = Path(str(raw_path or "")).expanduser().resolve()
    roots = [
        (modules["root"] / "backtests" / "pbgui").resolve(),
        (Path(__file__).resolve().parent / "data" / "archives").resolve(),
    ]
    if not any(_is_relative_to(result, root) and result != root for root in roots):
        raise ValueError("result path is outside managed PB8 result roots")
    if not result.is_dir() or result.is_symlink():
        raise ValueError("result path is not a safe directory")
    for required in ("analysis.json",):
        path = result / required
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"result path is missing safe {required}")
    return result


def _is_relative_to(path: Path, root: Path) -> bool:
    """Return whether ``path`` is below ``root`` on supported Python versions."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _stored_events(
    result_dir: Path,
    max_orders: int,
    coin: str,
    *,
    start_timestamp_ms: int | None = None,
    end_timestamp_ms: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Read a bounded stored fills.csv(.gz) from one already validated result directory."""
    path = result_dir / "fills.csv"
    if not path.is_file():
        path = result_dir / "fills.csv.gz"
    if not path.is_file() or path.is_symlink():
        raise ValueError("validated result has no safe fills.csv or fills.csv.gz")
    if path.stat().st_size > MAX_RESULT_FILLS_BYTES:
        raise ValueError("stored fills file exceeds the 64 MiB Strategy Explorer limit")
    opener = gzip.open if path.suffix == ".gz" else open
    output = {side: [] for side in SIDES}
    displayed = 0
    csv.field_size_limit(256 * 1024)
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        for scanned, row in enumerate(csv.DictReader(handle), start=1):
            if displayed >= max_orders:
                break
            if scanned > MAX_RESULT_FILL_ROWS:
                raise ValueError("stored fills file exceeds the 1,000,000 row Strategy Explorer limit")
            if str(row.get("coin") or "").strip().upper() != coin.upper():
                continue
            order_type = str(row.get("type") or row.get("order_type") or "")
            try:
                timestamp_raw = str(row.get("timestamp") or "").strip()
                if timestamp_raw.replace(".", "", 1).isdigit():
                    numeric_timestamp = float(timestamp_raw)
                    divisor = 1000.0 if numeric_timestamp > 10_000_000_000 else 1.0
                    timestamp = dt.datetime.fromtimestamp(numeric_timestamp / divisor, tz=dt.timezone.utc)
                else:
                    timestamp = dt.datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
                timestamp_ms = int(timestamp.timestamp() * 1000)
                if start_timestamp_ms is not None and timestamp_ms < start_timestamp_ms:
                    continue
                if end_timestamp_ms is not None and timestamp_ms > end_timestamp_ms:
                    continue
                qty = float(row.get("qty") or 0.0)
                psize = float(row.get("psize") or row.get("pos_size") or 0.0)
                event = {
                    "timestamp": timestamp.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                    "timestamp_ms": timestamp_ms,
                    "event": order_type,
                    "order_type": order_type,
                    "coin": str(row.get("coin") or ""),
                    "qty": qty,
                    "price": float(row.get("price") or 0.0),
                    "pos_size": psize,
                    "pos_price": float(row.get("pprice") or row.get("pos_price") or 0.0),
                    "wallet_balance": float(row.get("usd_total_balance") or row.get("wallet_balance") or 0.0),
                    "wallet_exposure": float(row.get("wallet_exposure") or 0.0),
                    "pnl": float(row.get("pnl") or 0.0),
                    "fee_paid": float(row.get("fee_paid") or 0.0),
                }
            except (TypeError, ValueError, OverflowError):
                continue
            side = _side_for_fill(order_type, qty, psize)
            output[side].append(event)
            displayed += 1
    return output


def _events_match(left: dict[str, Any], right: dict[str, Any], options: dict[str, Any]) -> bool:
    """Compare event identity with bounded timestamps and exchange-step quantization."""
    ts_tol = _bounded_int(options.get("timestamp_tolerance_ms"), 1_000, 0, 60_000)
    try:
        qty_tol = max(0.0, min(float(options.get("qty_tolerance", 1e-8)), 1.0))
        price_tol = max(0.0, min(float(options.get("price_tolerance", 1e-6)), 1.0))
        qty_step = max(0.0, float(options.get("qty_step") or 0.0))
        price_step = max(0.0, float(options.get("price_step") or 0.0))
    except (TypeError, ValueError):
        qty_tol, price_tol, qty_step, price_step = 1e-8, 1e-6, 0.0, 0.0

    def numeric_match(left_value: Any, right_value: Any, step: float, tolerance: float) -> bool:
        lhs = float(left_value or 0.0)
        rhs = float(right_value or 0.0)
        if step > 0.0:
            return int(round(lhs / step)) == int(round(rhs / step))
        return math.isclose(lhs, rhs, rel_tol=0.0, abs_tol=tolerance)

    return (
        abs(int(left.get("timestamp_ms") or 0) - int(right.get("timestamp_ms") or 0)) <= ts_tol
        and numeric_match(left.get("qty"), right.get("qty"), qty_step, qty_tol)
        and numeric_match(left.get("price"), right.get("price"), price_step, price_tol)
        and str(left.get("order_type") or "") == str(right.get("order_type") or "")
    )


def _compare_rows(
    left: dict[str, list[dict[str, Any]]],
    right: dict[str, list[dict[str, Any]]],
    options: dict[str, Any],
    left_key: str,
    right_key: str,
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Build PB7-compatible compare status counts and flattened rows."""
    summary: dict[str, Any] = {}
    rows: dict[str, list[dict[str, Any]]] = {side: [] for side in SIDES}
    timestamp_tolerance_ms = _bounded_int(
        options.get("timestamp_tolerance_ms"), 1_000, 0, 60_000
    )
    for side in SIDES:
        counts = {key: 0 for key in ("match", "pb7_only", "b_only", "c_only", "pb7_and_b", "pb7_and_c", "b_and_c", "mismatch")}
        pairs: list[tuple[dict[str, Any] | None, dict[str, Any] | None, str]] = []
        used_right: set[int] = set()
        for lhs in left[side]:
            candidates = [
                index
                for index, rhs in enumerate(right[side])
                if index not in used_right
                and abs(int(lhs.get("timestamp_ms") or 0) - int(rhs.get("timestamp_ms") or 0))
                <= timestamp_tolerance_ms
                and _events_match(lhs, rhs, options)
            ]
            if candidates:
                right_index = min(
                    candidates,
                    key=lambda index: abs(
                        int(lhs.get("timestamp_ms") or 0)
                        - int(right[side][index].get("timestamp_ms") or 0)
                    ),
                )
                used_right.add(right_index)
                pairs.append((lhs, right[side][right_index], "match"))
            else:
                pairs.append((lhs, None, f"{left_key}_only"))
        for right_index, rhs in enumerate(right[side]):
            if right_index not in used_right:
                pairs.append((None, rhs, f"{right_key}_only"))
        pairs.sort(
            key=lambda pair: (
                int((pair[0] or pair[1] or {}).get("timestamp_ms") or 0),
                0 if pair[2] == "match" else 1,
            )
        )
        compare_index = 0
        for lhs, rhs, status in pairs:
            compare_index += 1
            counts[status] = counts.get(status, 0) + 1
            representative = lhs or rhs or {}
            row = {
                "compare_index": compare_index,
                "timestamp": representative.get("timestamp"),
                "order_type": representative.get("order_type"),
                "qty": representative.get("qty"),
                "price": representative.get("price"),
                "status": status,
                "in_pb7": left_key == "pb7" and lhs is not None,
                "in_b": (left_key == "b" and lhs is not None) or (right_key == "b" and rhs is not None),
                "in_c": (left_key == "c" and lhs is not None) or (right_key == "c" and rhs is not None),
            }
            for key, event in ((left_key, lhs), (right_key, rhs)):
                if event:
                    for field in ("timestamp", "order_type", "qty", "price", "pos_size", "pos_price", "wallet_balance", "pnl", "fee_paid", "wallet_exposure"):
                        row[f"{key}_{field}"] = event.get(field)
            if not options.get("mismatches_only") or status != "match":
                rows[side].append(row)
        summary[side] = counts
    summary["events"] = {
        left_key: {"long": len(left["long"]), "short": len(left["short"]), "total": len(left["long"]) + len(left["short"])},
        right_key: {"long": len(right["long"]), "short": len(right["short"]), "total": len(right["long"]) + len(right["short"])},
    }
    return summary, rows


def _comparison_signature(config: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Return canonical fields that can affect a native replay comparison."""
    canonical = _canonicalize(config, modules)
    for key in ("config_version", "logging", "monitor", "optimize", "pbgui"):
        canonical.pop(key, None)
    return canonical


def _stored_fill_bounds(result_dir: Path, coin: str) -> tuple[int | None, int | None, int]:
    """Read only selected-coin timestamp bounds from a validated stored fills file."""
    path = result_dir / "fills.csv"
    if not path.is_file():
        path = result_dir / "fills.csv.gz"
    if not path.is_file() or path.is_symlink():
        raise ValueError("validated result has no safe fills.csv or fills.csv.gz")
    if path.stat().st_size > MAX_RESULT_FILLS_BYTES:
        raise ValueError("stored fills file exceeds the 64 MiB Strategy Explorer limit")
    opener = gzip.open if path.suffix == ".gz" else open
    first_timestamp_ms = None
    last_timestamp_ms = None
    matched = 0
    csv.field_size_limit(256 * 1024)
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        for scanned, row in enumerate(csv.DictReader(handle), start=1):
            if scanned > MAX_RESULT_FILL_ROWS:
                raise ValueError("stored fills file exceeds the 1,000,000 row Strategy Explorer limit")
            if str(row.get("coin") or "").strip().upper() != coin.upper():
                continue
            try:
                timestamp_raw = str(row.get("timestamp") or "").strip()
                if timestamp_raw.replace(".", "", 1).isdigit():
                    numeric_timestamp = float(timestamp_raw)
                    divisor = 1000.0 if numeric_timestamp > 10_000_000_000 else 1.0
                    timestamp_ms = int(numeric_timestamp / divisor * 1000.0)
                else:
                    timestamp = dt.datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
                    timestamp_ms = int(timestamp.timestamp() * 1000)
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            first_timestamp_ms = timestamp_ms if first_timestamp_ms is None else min(first_timestamp_ms, timestamp_ms)
            last_timestamp_ms = timestamp_ms if last_timestamp_ms is None else max(last_timestamp_ms, timestamp_ms)
            matched += 1
    return first_timestamp_ms, last_timestamp_ms, matched


def _compare(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Compare native PB8 fills with safe stored fills or a second native replay."""
    options = request.get("options") if isinstance(request.get("options"), dict) else {}
    compare_config = request.get("compare_config") if isinstance(request.get("compare_config"), dict) else None
    if not request.get("result_path") and compare_config is None:
        raise ValueError("Compare requires validated stored fills or a distinct second config")
    if compare_config is not None and _comparison_signature(compare_config, modules) == _comparison_signature(
        request.get("config") if isinstance(request.get("config"), dict) else {}, modules
    ):
        raise ValueError("Compare requires two runtime-distinct PB8 configs")
    replay_options = dict(options)
    result_dir = None
    primary_config = _canonicalize(request.get("config"), modules)
    _selected_exchange, selected_coin = _market_choice(primary_config, options)
    stored_bounds: tuple[int | None, int | None, int] = (None, None, 0)
    if request.get("result_path"):
        result_dir = _safe_result_dir(request["result_path"], modules)
        if options.get("use_fills_range"):
            stored_bounds = _stored_fill_bounds(result_dir, selected_coin)
            if stored_bounds[0] is not None:
                first_timestamp = dt.datetime.fromtimestamp(
                    int(stored_bounds[0]) / 1000,
                    tz=dt.timezone.utc,
                )
                config = request.get("config") if isinstance(request.get("config"), dict) else {}
                backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
                candle_interval_minutes = _bounded_int(
                    backtest.get("candle_interval_minutes"), 1, 1, 1_440
                )
                replay_start = first_timestamp - dt.timedelta(minutes=candle_interval_minutes)
                replay_options["start_date"] = replay_start.date().isoformat()
                replay_options["start_time"] = replay_start.strftime("%H:%M")
    replay_request = {"config": request.get("config"), "options": replay_options}
    fresh = _replay(replay_request, modules)
    fresh_events = fresh["events"]
    max_orders = _bounded_int(options.get("compare_max_orders"), MAX_SIM_ORDERS, 1, MAX_SIM_ORDERS)
    if result_dir is not None:
        metadata = fresh.get("metadata") if isinstance(fresh.get("metadata"), dict) else {}
        comparison_options = dict(options)
        comparison_options.setdefault("price_step", metadata.get("price_step"))
        comparison_options.setdefault("qty_step", metadata.get("qty_step"))
        start_timestamp_ms = int(metadata.get("start_timestamp_ms") or 0)
        end_timestamp_ms = int(metadata.get("end_timestamp_ms") or 0)
        stored = _stored_events(
            result_dir,
            max_orders,
            str((fresh.get("metadata") or {}).get("coin") or ""),
            start_timestamp_ms=start_timestamp_ms,
            end_timestamp_ms=end_timestamp_ms,
        )
        summary, rows = _compare_rows(stored, fresh_events, comparison_options, "pb7", "c")
        summary["window"] = {
            "start_timestamp_ms": start_timestamp_ms,
            "end_timestamp_ms": end_timestamp_ms,
        }
        if options.get("use_fills_range"):
            stored_start, stored_end, stored_count = stored_bounds
            partial = bool(
                stored_count
                and (
                    stored_start is None
                    or stored_end is None
                    or stored_start < start_timestamp_ms
                    or stored_end > end_timestamp_ms
                )
            )
            summary["coverage"] = {
                "partial": partial,
                "stored_start_timestamp_ms": stored_start,
                "stored_end_timestamp_ms": stored_end,
                "stored_fill_count": stored_count,
                "replay_start_timestamp_ms": start_timestamp_ms,
                "replay_end_timestamp_ms": end_timestamp_ms,
            }
        sources = {"pb7": "validated stored PB8 fills", "c": "fresh PB8 native replay"}
        message = (
            "Compared validated stored fills with a fresh PB8 native replay "
            f"from {dt.datetime.fromtimestamp(start_timestamp_ms / 1000, tz=dt.timezone.utc).isoformat()} "
            f"to {dt.datetime.fromtimestamp(end_timestamp_ms / 1000, tz=dt.timezone.utc).isoformat()}."
        )
    elif compare_config is not None:
        second = _replay({"config": compare_config, "options": options}, modules)
        comparison_options = dict(options)
        comparison_options.setdefault("price_step", (fresh.get("metadata") or {}).get("price_step"))
        comparison_options.setdefault("qty_step", (fresh.get("metadata") or {}).get("qty_step"))
        summary, rows = _compare_rows(fresh_events, second["events"], comparison_options, "b", "c")
        sources = {"b": "PB8 native replay A", "c": "PB8 native replay B"}
        message = "Compared two PB8 native replay configurations."
    else:
        raise ValueError("Compare source disappeared before comparison")
    return {"ok": True, "summary": summary, "rows": rows, "sources": sources, "message": message}


def _movie_frames_with_native_orders(
    config: dict[str, Any],
    replay_options: dict[str, Any],
    replay: dict[str, Any],
    selected: list[dict[str, Any]],
    modules: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build native replay frames without inventing unavailable historical order state."""
    del config, replay_options, replay, modules
    frames = []
    for index, candle in enumerate(selected):
        frame = {
            "index": index + 1,
            "timestamp": candle.get("timestamp"),
            "candle": candle,
        }
        for side in SIDES:
            frame[side] = {
                "orders": {"entries": [], "closes": []},
                "summary": {"entry_orders": 0, "close_orders": 0},
                "debug": {
                    "engine": "pb8_engine",
                    "orders_available": False,
                    "state_params": {},
                },
            }
        frames.append(frame)
    return frames


def _aggregate_movie_candles(
    candles: list[dict[str, Any]], step: int
) -> list[dict[str, Any]]:
    """Aggregate minute candles into complete step-sized OHLCV movie bars."""
    aggregated = []
    for start in range(0, len(candles), step):
        chunk = candles[start : start + step]
        if len(chunk) < step:
            break
        aggregated.append(
            {
                "timestamp": chunk[0].get("timestamp"),
                "timestamp_ms": chunk[0].get("timestamp_ms"),
                "open": float(chunk[0].get("open") or 0.0),
                "high": max(float(item.get("high") or 0.0) for item in chunk),
                "low": min(float(item.get("low") or 0.0) for item in chunk),
                "close": float(chunk[-1].get("close") or 0.0),
                "volume": sum(float(item.get("volume") or 0.0) for item in chunk),
            }
        )
    return aggregated


def _movie(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Build bounded PB7-compatible movie frames from real native replay data."""
    options = request.get("options") if isinstance(request.get("options"), dict) else {}
    frame_count = _bounded_int(options.get("frames"), 200, 1, MAX_MOVIE_FRAMES)
    step = _bounded_int(options.get("step_mins"), 240, 1, 1440)
    requested_candles = frame_count * step + 1
    if requested_candles > MAX_SIM_CANDLES:
        raise ValueError(
            f"PB8 Movie Builder coverage requires {requested_candles} one-minute candles; "
            f"the safe limit is {MAX_SIM_CANDLES}. Reduce Duration or Frame Step."
        )
    replay_options = dict(options)
    replay_options["sim_max_candles"] = max(10, requested_candles)
    replay_options["sim_max_orders"] = MAX_SIM_ORDERS
    replay = _replay({"config": request.get("config"), "options": replay_options}, modules)
    candles = replay.get("candles") or []
    selected = _aggregate_movie_candles(candles, step)[:frame_count]
    frames = _movie_frames_with_native_orders(
        request.get("config"), replay_options, replay, selected, modules
    )
    metadata = dict(replay.get("metadata") or {})
    metadata.update(
        {
            "start_time": selected[0]["timestamp"] if selected else "",
            "step_mins": step,
            "requested_frames": frame_count,
            "actual_frames": len(selected),
            "requested_candles": requested_candles,
            "orders_available": False,
            "coverage_complete": len(selected) == frame_count,
        }
    )
    return {
        "ok": True,
        "engine": "pb8_engine",
        "metadata": metadata,
        "events": replay["events"],
        "frames": frames,
        "message": (
            f"Built {len(frames)} PB8 native replay frames from real candles and fills; "
            "historical native order state is unavailable."
            + (
                " Fill-derived position annotations stop when the displayed fill limit is reached."
                if metadata.get("fills_truncated")
                else ""
            )
        ),
    }


def dispatch(request: dict[str, Any], modules: dict[str, Any]) -> dict[str, Any]:
    """Dispatch one validated helper operation."""
    handlers = {
        "capabilities": _capabilities,
        "markets": _markets,
        "snapshot": _snapshot,
        "replay": _replay,
        "compare": _compare,
        "movie": _movie,
    }
    operation = str(request.get("operation") or "")
    if operation not in handlers:
        raise ValueError(f"unsupported Strategy Explorer operation: {operation}")
    return handlers[operation](request, modules)


def main() -> int:
    """Read one bounded stdin request and write one strict JSON response."""
    raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        response = {"ok": False, "detail": "Strategy Explorer request exceeds the 2 MiB limit"}
        sys.stdout.write(json.dumps(response, separators=(",", ":"), allow_nan=False))
        return 1
    try:
        request = json.loads(raw.decode("utf-8"))
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        with redirect_stdout(sys.stderr):
            modules = _load_pb8(str(request.get("pb8_dir") or ""))
            result = dispatch(request, modules)
        response = {"ok": True, "result": _json_safe(result)}
        return_code = 0
    except Exception as exc:
        response = {"ok": False, "detail": str(exc)[-2000:]}
        return_code = 1
    sys.stdout.write(json.dumps(response, separators=(",", ":"), allow_nan=False))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
