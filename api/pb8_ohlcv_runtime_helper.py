"""Run PB8's read-only OHLCV planner inside the configured PB8 runtime."""

from __future__ import annotations

import asyncio
import copy
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any


STATUS_LABELS = {
    "store_complete": "Local v2 ready",
    "legacy_importable": "Local source import",
    "missing_local": "Will fetch on start",
    "blocked_by_persistent_gap": "Blocked by persistent gap",
    "missing_market": "Coin not on exchange",
    "coin_too_young": "Too young for window",
}
STATUS_RANK = {
    "store_complete": 0,
    "legacy_importable": 1,
    "missing_local": 2,
    "blocked_by_persistent_gap": 3,
    "coin_too_young": 4,
    "missing_market": 5,
}


def _load_modules(pb8_dir: Path) -> dict[str, Any]:
    """Import planner modules only from the selected PB8 source tree."""
    src_dir = pb8_dir / "src"
    if not src_dir.is_dir():
        raise RuntimeError(f"PB8 source directory not found: {src_dir}")
    sys.path.insert(0, str(src_dir))

    from config.access import require_config_value, require_live_value
    from config.load import prepare_config
    from hlcv_preparation import HLCVManager
    from ohlcv_catalog import OhlcvCatalog
    from ohlcv_legacy_import import inspect_legacy_range
    from ohlcv_planner import plan_local_symbol_range
    from procedures import date_to_ts, ts_to_date
    from utils import (
        format_approved_ignored_coins,
        format_end_date,
        to_ccxt_exchange_id,
        to_standard_exchange_name,
    )
    from warmup_utils import compute_backtest_warmup_minutes

    return locals()


def _counts() -> dict[str, int]:
    """Return an empty shared-editor readiness count map."""
    return {key: 0 for key in STATUS_LABELS}


def _uses_all_coins(source: Any) -> bool:
    """Detect PB8's dynamic all-coins selector in either side."""
    if isinstance(source, str):
        return source.strip().lower() == "all"
    if isinstance(source, (list, tuple)):
        return len(source) == 1 and str(source[0]).strip().lower() == "all"
    if isinstance(source, dict):
        return any(_uses_all_coins(source.get(side)) for side in ("long", "short"))
    return False


def _collect_coin_sides(approved: Any) -> dict[str, list[str]]:
    """Map each resolved approved coin to its enabled sides."""
    result: dict[str, list[str]] = {}
    if not isinstance(approved, dict):
        return result
    for side in ("long", "short"):
        values = approved.get(side, [])
        if isinstance(values, str):
            values = [values]
        for coin in values or []:
            name = str(coin or "").strip()
            if not name or name == "all":
                continue
            sides = result.setdefault(name, [])
            if side not in sides:
                sides.append(side)
    return result


def _gpu_suite_exchange_requirements(config: dict, base_exchanges: list[str]) -> dict[str, list[str]]:
    """Map every strict GPU suite exchange dataset to its scenario labels."""
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    optimize = config.get("optimize") if isinstance(config.get("optimize"), dict) else {}
    scenarios = backtest.get("scenarios") if isinstance(backtest.get("scenarios"), list) else []
    if (
        str(optimize.get("backend") or "").strip().lower() != "gpu"
        or not bool(backtest.get("suite_enabled"))
        or not scenarios
    ):
        return {}
    requirements: dict[str, list[str]] = {}
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            continue
        label = str(scenario.get("label") or f"scenario_{index + 1:02d}")
        scenario_exchanges = scenario.get("exchanges")
        selected = scenario_exchanges if isinstance(scenario_exchanges, list) and scenario_exchanges else base_exchanges
        for exchange in selected:
            value = str(exchange or "").strip()
            if value:
                requirements.setdefault(value, []).append(label)
    return requirements


def _gpu_suite_dataset_requirements(
    config: dict,
    base_exchanges: list[str],
    default_coins: list[str],
) -> dict[str, dict[str, Any]]:
    """Return strict exchange datasets with their effective scenario coin unions."""
    labels = _gpu_suite_exchange_requirements(config, base_exchanges)
    if not labels:
        return {}
    scenarios = ((config.get("backtest") or {}).get("scenarios") or [])
    requirements = {
        exchange: {"labels": list(dict.fromkeys(exchange_labels)), "coins": set()}
        for exchange, exchange_labels in labels.items()
    }
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            continue
        label = str(scenario.get("label") or f"scenario_{index + 1:02d}")
        scenario_exchanges = scenario.get("exchanges")
        selected_exchanges = scenario_exchanges if isinstance(scenario_exchanges, list) and scenario_exchanges else base_exchanges
        scenario_coins = scenario.get("coins")
        selected_coins = (
            [str(coin).strip() for coin in scenario_coins if str(coin).strip()]
            if isinstance(scenario_coins, list) and scenario_coins
            else list(default_coins)
        )
        ignored = scenario.get("ignored_coins")
        ignored_set = {str(coin).strip() for coin in ignored if str(coin).strip()} if isinstance(ignored, list) else set()
        selected_coins = [coin for coin in selected_coins if coin not in ignored_set]
        for raw_exchange in selected_exchanges:
            exchange = str(raw_exchange or "").strip()
            if exchange in requirements and label in requirements[exchange]["labels"]:
                requirements[exchange]["coins"].update(selected_coins)
    for requirement in requirements.values():
        if not requirement["coins"]:
            requirement["coins"].update(default_coins)
        requirement["coins"] = sorted(requirement["coins"])
    return requirements


def _market_start_ts(market: dict[str, Any] | None) -> int | None:
    """Extract the earliest known market inception timestamp."""
    if not isinstance(market, dict):
        return None
    candidates = [market.get("created")]
    info = market.get("info")
    if isinstance(info, dict):
        candidates.extend(
            info.get(key)
            for key in ("launchTime", "onboardDate", "launch_time", "listingTime", "createTime")
        )
    values = []
    for candidate in candidates:
        try:
            parsed = int(float(str(candidate).strip()))
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            values.append(parsed)
    return min(values) if values else None


def _compact(entry: dict[str, Any]) -> dict[str, Any]:
    """Keep only fields rendered by the shared preflight panel."""
    keys = (
        "coin",
        "exchange",
        "status",
        "status_label",
        "note",
        "sides",
        "symbol",
        "effective_start_date",
        "catalog_bounds",
        "persistent_gap",
    )
    return {key: copy.deepcopy(entry[key]) for key in keys if entry.get(key) is not None}


def _group_samples(entries: list[dict[str, Any]], limit: int = 6) -> dict[str, list[dict[str, Any]]]:
    """Return bounded representative entries grouped by status."""
    grouped: dict[str, list[dict[str, Any]]] = {key: [] for key in STATUS_LABELS}
    for entry in sorted(entries, key=lambda item: (item.get("coin") or "", item.get("exchange") or "")):
        status = str(entry.get("status") or "")
        if status in grouped and len(grouped[status]) < limit:
            grouped[status].append(_compact(entry))
    return {key: value for key, value in grouped.items() if value}


def _summary(counts: dict[str, int], coin_count: int, *, explicit_source: bool) -> dict[str, Any]:
    """Build the shared editor summary with PB8 source semantics."""
    ready = int(counts.get("store_complete") or 0)
    legacy = int(counts.get("legacy_importable") or 0)
    fetch = int(counts.get("missing_local") or 0)
    blocked = int(counts.get("blocked_by_persistent_gap") or 0)
    missing_market = int(counts.get("missing_market") or 0)
    too_young = int(counts.get("coin_too_young") or 0)
    parts = []
    for value, label in (
        (ready, "ready locally"),
        (legacy, "available from the configured source"),
        (fetch, "missing locally" if explicit_source else "would fetch on start"),
        (blocked, "blocked by persistent gaps"),
        (missing_market, "not available on the selected exchanges"),
        (too_young, "too young for the requested window"),
    ):
        if value:
            parts.append(f"{value} {label}")

    if coin_count <= 0:
        status, headline = "empty", "No approved coins resolved"
    elif ready == coin_count:
        status, headline = "ready", "Local PB8 v2 data is ready"
    elif too_young == coin_count:
        status, headline = "too_young", "Selected coins start after the requested window"
    elif missing_market == coin_count:
        status, headline = "missing_market", "Approved coins are not on the selected exchanges"
    elif fetch > 0 and explicit_source:
        status, headline = "blocked", "Configured OHLCV source is incomplete"
    elif fetch > 0:
        status, headline = "preload", "Some coins need PB8 OHLCV data"
    elif blocked > 0 and ready == 0 and legacy == 0:
        status, headline = "blocked", "Persistent gaps block local readiness"
    elif legacy > 0 and fetch == 0 and blocked == 0 and missing_market == 0 and too_young == 0:
        status, headline = "legacy", "Configured source can satisfy the request"
    else:
        status, headline = "mixed", "OHLCV readiness is mixed"

    preload_supported = fetch > 0 and not explicit_source
    if explicit_source and fetch > 0:
        preload_detail = (
            "PB8 treats an explicit ohlcv_source_dir as read-only. Populate that source first, "
            "or clear it to let PB8 download into its own cache."
        )
        preload_label = "PB8 source not ready"
    elif preload_supported:
        preload_detail = "Run PB8's native passivbot download command for this config."
        preload_label = "Preload missing OHLCV data"
    else:
        preload_detail = "Nothing in the current best-per-coin view needs a remote preload."
        preload_label = "No preload needed"
    return {
        "overall_status": status,
        "headline": headline,
        "detail": ", ".join(parts) if parts else "No readiness data available.",
        "counts": dict(counts),
        "preload_supported": preload_supported,
        "preload_label": preload_label,
        "preload_detail": preload_detail,
    }


async def _build(payload: dict[str, Any]) -> dict[str, Any]:
    """Prepare a config and evaluate its local ranges with PB8 modules."""
    pb8_dir = Path(str(payload.get("pb8_dir") or "")).resolve()
    modules = _load_modules(pb8_dir)
    raw_config = payload.get("config")
    if not isinstance(raw_config, dict):
        raise TypeError("config must be an object")
    candidate = copy.deepcopy(raw_config)
    candidate.pop("pbgui", None)
    config = modules["prepare_config"](
        candidate,
        base_config_path=str(pb8_dir / ".pbgui-ohlcv-preflight.json"),
        verbose=False,
        target="canonical",
        runtime=None,
        raw_snapshot=candidate,
        effective_snapshot=candidate,
    )
    backtest = config.get("backtest", {})
    live = config.get("live", {})
    require_config_value = modules["require_config_value"]
    require_live_value = modules["require_live_value"]
    exchanges = list(require_config_value(config, "backtest.exchanges") or [])
    if not exchanges:
        raise ValueError("backtest.exchanges is empty")
    base_exchanges = list(exchanges)
    required_exchange_labels = _gpu_suite_exchange_requirements(config, base_exchanges)
    gpu_suite = bool(required_exchange_labels)
    for exchange in required_exchange_labels:
        if exchange not in exchanges:
            exchanges.append(exchange)
    requested_start_date = str(require_config_value(config, "backtest.start_date"))
    end_date = modules["format_end_date"](require_config_value(config, "backtest.end_date"))
    requested_start_ts = int(modules["date_to_ts"](requested_start_date))
    end_ts = int(modules["date_to_ts"](end_date))
    warmup_minutes = max(0, int(modules["compute_backtest_warmup_minutes"](config)))
    effective_start_ts = max(0, requested_start_ts - warmup_minutes * 60_000)
    effective_start_ts = (effective_start_ts // 60_000) * 60_000
    minimum_coin_age_days = float(require_live_value(config, "minimum_coin_age_days"))
    min_coin_age_ms = int(max(0.0, minimum_coin_age_days) * 86_400_000)
    source_value = str(backtest.get("ohlcv_source_dir") or "").strip()
    source_dir = Path(source_value) if source_value else None
    legacy_root = source_dir or pb8_dir / "caches" / "ohlcv"
    if not legacy_root.exists():
        legacy_root = None
    catalog_path = pb8_dir / "caches" / "ohlcvs" / "catalog.sqlite"
    # PB8 deliberately bypasses its v2 store when an explicit caller-managed
    # source is selected, so the readiness result must do the same.
    catalog = (
        modules["OhlcvCatalog"](catalog_path)
        if catalog_path.exists() and source_dir is None
        else None
    )

    await modules["format_approved_ignored_coins"](config, exchanges)
    approved = require_live_value(config, "approved_coins")
    coin_sides = _collect_coin_sides(approved)
    coins = sorted(coin_sides)
    dataset_requirements = _gpu_suite_dataset_requirements(config, base_exchanges, coins)
    request = {
        "requested_start_date": requested_start_date,
        "effective_start_date": modules["ts_to_date"](effective_start_ts),
        "end_date": end_date,
        "warmup_minutes": warmup_minutes,
        "minimum_coin_age_days": minimum_coin_age_days,
        "source_dir": str(source_dir) if source_dir else None,
        "catalog_path": str(catalog_path),
        "catalog_present": catalog is not None,
    }
    universe = {
        "coin_count": len(coins),
        "coins_mode": "all" if _uses_all_coins(live.get("approved_coins")) else "explicit",
        "exchange_count": len(exchanges),
    }
    if not coins:
        return {
            "summary": _summary(_counts(), 0, explicit_source=bool(source_dir)),
            "request": request,
            "universe": universe,
            "best_samples": {},
            "exchanges": [],
            "notes": ["Preflight uses PB8's current approved-coin resolution."],
        }

    entries_by_coin: dict[str, list[dict[str, Any]]] = {coin: [] for coin in coins}
    exchange_payloads = []
    for raw_exchange in exchanges:
        ccxt_exchange = modules["to_ccxt_exchange_id"](raw_exchange)
        store_exchange = modules["to_standard_exchange_name"](ccxt_exchange)
        manager = modules["HLCVManager"](
            ccxt_exchange,
            modules["ts_to_date"](effective_start_ts),
            end_date,
            gap_tolerance_ohlcvs_minutes=require_config_value(
                config, "backtest.gap_tolerance_ohlcvs_minutes"
            ),
            cm_debug_level=int(backtest.get("cm_debug_level", 0) or 0),
            cm_progress_log_interval_seconds=float(
                backtest.get("cm_progress_log_interval_seconds", 10.0) or 10.0
            ),
            force_refetch_gaps=False,
            ohlcv_source_dir=source_value or None,
        )
        exchange_entries = []
        counts = _counts()
        try:
            await manager.load_markets()
            for coin in coins:
                entry: dict[str, Any] = {
                    "coin": coin,
                    "exchange": store_exchange,
                    "requested_exchange": str(raw_exchange),
                    "sides": list(coin_sides.get(coin, [])),
                    "effective_start_date": modules["ts_to_date"](effective_start_ts),
                }
                if not manager.has_coin(coin):
                    entry.update(
                        status="missing_market",
                        status_label=STATUS_LABELS["missing_market"],
                        note="Coin is not listed on this exchange.",
                    )
                else:
                    symbol = manager.get_symbol(coin)
                    market = (manager.markets or {}).get(symbol) if isinstance(manager.markets, dict) else None
                    market_start_ts = _market_start_ts(market)
                    first_ts_guess = manager.load_first_timestamp(coin)
                    age_anchor = market_start_ts or (int(float(first_ts_guess)) if first_ts_guess else None)
                    adjusted_start_ts = max(
                        effective_start_ts,
                        (age_anchor + min_coin_age_ms) if age_anchor is not None else effective_start_ts,
                    )
                    entry["symbol"] = symbol
                    entry["effective_start_date"] = modules["ts_to_date"](adjusted_start_ts)
                    if adjusted_start_ts > end_ts:
                        entry.update(
                            status="coin_too_young",
                            status_label=STATUS_LABELS["coin_too_young"],
                            note="Minimum coin age pushes the usable start beyond the end date.",
                        )
                    elif catalog is None:
                        inspection = None
                        if legacy_root is not None:
                            inspection = modules["inspect_legacy_range"](
                                legacy_root=legacy_root,
                                exchange=store_exchange,
                                timeframe="1m",
                                symbol=symbol,
                                start_ts=adjusted_start_ts,
                                end_ts=end_ts,
                            )
                        status = "legacy_importable" if inspection and inspection.all_days_present else "missing_local"
                        entry.update(
                            status=status,
                            status_label=(
                                "Missing from configured source"
                                if status == "missing_local" and source_dir
                                else STATUS_LABELS[status]
                            ),
                            note=(
                                "The configured source covers the requested range."
                                if status == "legacy_importable"
                                else "No PB8 v2 catalog or complete configured source covers this range."
                            ),
                        )
                    else:
                        plan = modules["plan_local_symbol_range"](
                            catalog=catalog,
                            legacy_root=legacy_root,
                            exchange=store_exchange,
                            timeframe="1m",
                            symbol=symbol,
                            start_ts=adjusted_start_ts,
                            end_ts=end_ts,
                        )
                        entry.update(status=plan.status, status_label=STATUS_LABELS.get(plan.status, plan.status))
                        if plan.bounds[0] is not None or plan.bounds[1] is not None:
                            entry["catalog_bounds"] = {
                                "first": modules["ts_to_date"](plan.bounds[0]) if plan.bounds[0] is not None else None,
                                "last": modules["ts_to_date"](plan.bounds[1]) if plan.bounds[1] is not None else None,
                            }
                        if plan.persistent_gaps:
                            gap = plan.persistent_gaps[0]
                            entry["persistent_gap"] = {
                                "start": modules["ts_to_date"](gap.start_ts),
                                "end": modules["ts_to_date"](gap.end_ts),
                                "reason": gap.reason,
                                "retry_count": int(gap.retry_count or 0),
                            }
                        entry["note"] = {
                            "store_complete": "PB8 can use its local v2 store without fetching.",
                            "legacy_importable": "PB8 can prepare this range from the configured source.",
                            "missing_local": "PB8 needs local source coverage." if source_dir else "PB8 would fetch this range.",
                            "blocked_by_persistent_gap": "Persistent gaps block the local range.",
                        }.get(plan.status, plan.status)
                counts[entry["status"]] += 1
                exchange_entries.append(entry)
                entries_by_coin[coin].append(entry)
        finally:
            await manager.aclose()
            if manager.cc:
                await manager.cc.close()
        exchange_payloads.append(
            {
                "exchange": store_exchange,
                "input_exchange": str(raw_exchange),
                "counts": counts,
                "samples": _group_samples(exchange_entries),
            }
        )

    best_entries = []
    best_counts = _counts()
    requirements = []
    if gpu_suite:
        entries_by_pair = {
            (str(entry.get("coin") or ""), str(entry.get("requested_exchange") or "")): entry
            for entries in entries_by_coin.values()
            for entry in entries
        }
        for exchange, requirement in dataset_requirements.items():
            labels = requirement["labels"]
            required_coins = requirement["coins"]
            requirements.append({
                "dataset": exchange,
                "exchange": exchange,
                "scenarios": list(dict.fromkeys(labels)),
                "coin_count": len(required_coins),
            })
            for coin in required_coins:
                entry = entries_by_pair.get((coin, exchange))
                if entry is None:
                    entry = {
                        "coin": coin,
                        "exchange": exchange,
                        "requested_exchange": exchange,
                        "sides": list(coin_sides.get(coin, [])),
                        "status": "missing_market",
                        "status_label": STATUS_LABELS["missing_market"],
                        "note": "Required GPU suite dataset was not prepared by the readiness check.",
                    }
                best_entries.append(entry)
                best_counts[entry["status"]] += 1
    else:
        for coin in coins:
            entries = entries_by_coin.get(coin, [])
            if not entries:
                continue
            best = min(entries, key=lambda item: (STATUS_RANK.get(str(item.get("status")), 999), item.get("exchange") or ""))
            best_entries.append(best)
            best_counts[best["status"]] += 1
    notes = [
        "Preflight is read-only and runs PB8's installed planner modules in the configured PB8 virtualenv.",
        "Approved coins are resolved from both long and short lists.",
    ]
    if source_dir:
        notes.append(f"PB8 explicit source checked read-only: {source_dir}")
    summary = _summary(best_counts, len(best_entries), explicit_source=bool(source_dir))
    scenario_only_exchanges = set(required_exchange_labels) - set(base_exchanges)
    if gpu_suite and scenario_only_exchanges and summary.get("preload_supported"):
        summary.update(
            preload_supported=False,
            preload_label="Suite preload requires separate datasets",
            preload_detail="The current single PB8 download command does not include scenario-only exchanges; preload each required dataset explicitly.",
        )
    return {
        "summary": summary,
        "request": request,
        "universe": universe,
        "best_samples": _group_samples(best_entries),
        "exchanges": exchange_payloads,
        "aggregation_mode": "required_datasets" if gpu_suite else "best_per_coin",
        "requirements": requirements,
        "notes": notes,
    }


def main() -> int:
    """Read one JSON request from stdin and write one JSON response."""
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise TypeError("request must be an object")
        with redirect_stdout(sys.stderr):
            result = asyncio.run(_build(payload))
        response = {"ok": True, "result": result}
    except Exception as exc:
        response = {"ok": False, "error": type(exc).__name__, "detail": str(exc)}
    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
