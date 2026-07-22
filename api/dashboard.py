"""
FastAPI router: Dashboard data endpoints.

Provides data for Vanilla JS dashboard components (balance, positions, …).
All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import asyncio as _asyncio
import json
import math
import threading
import time as _time
import urllib.request
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, List

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR

SERVICE = "Dashboard"

router = APIRouter()

# In-memory storage for JS→Python selection sync (keyed by position string)
_balance_user_selections: dict[str, list[str]] = {}

# In-memory storage for JS grid editor → save() sync
# Stores the latest pending grid config from the grid editor component.
_grid_pending_config: dict[str, Any] = {}

# Full pending dashboard config per named dashboard (for the full JS grid editor)
# Key = original dashboard name (or "" for a new dashboard)
_pending_full_configs: dict[str, dict[str, Any]] = {}

# Cache connected Exchange instances per user so CCXT init only runs once per user.
_exchange_cache: dict[str, Any] = {}
_exchange_cache_last_used: dict[str, float] = {}
_exchange_cache_lock = threading.RLock()
_EXCHANGE_CACHE_IDLE_SECONDS = 15 * 60
_EXCHANGE_CACHE_MAX_SIZE = 64

_PANIC_GLOBAL_MODE = "p"
_PANIC_OVERRIDE_MODE = "panic"
_GRACEFUL_STOP_MODE = "graceful_stop"
_TP_ONLY_MODE = "tp_only"


def _classify_position_orders(orders: list, side: str) -> tuple[int, float, float]:
    """Return DCA count, next DCA price, and next TP price for a position side."""
    side = str(side).lower()
    dca_side = "buy"
    tp_side = "sell"
    prefer_higher_dca = True
    prefer_lower_tp = True
    if side == "short":
        dca_side = "sell"
        tp_side = "buy"
        prefer_higher_dca = False
        prefer_lower_tp = False

    dca = 0
    next_dca = 0.0
    next_tp = 0.0
    for order in orders:
        order_side = str(order[5]).lower()
        order_price = order[4]
        if order_side == dca_side:
            dca += 1
            if next_dca == 0 or (prefer_higher_dca and next_dca < order_price) or (not prefer_higher_dca and next_dca > order_price):
                next_dca = order_price
        elif order_side == tp_side:
            if next_tp == 0 or (prefer_lower_tp and next_tp > order_price) or (not prefer_lower_tp and next_tp < order_price):
                next_tp = order_price
    return dca, next_dca, next_tp


def _normalize_position_side(value: Any) -> str:
    """Normalize a raw order/position side value to long/short/both."""
    if value is None:
        return ""
    normalized = str(value).strip().lower()
    if normalized in {"long", "short", "both"}:
        return normalized
    if normalized in {"1", "1.0", "buy"}:
        return "long"
    if normalized in {"2", "2.0", "sell"}:
        return "short"
    if normalized in {"net", "net_mode"}:
        return "both"
    return ""


def _extract_order_position_side(order: dict) -> str:
    """Best-effort leg detection from live exchange order payloads."""
    if not isinstance(order, dict):
        return ""

    sources = [order]
    info = order.get("info", {})
    if isinstance(info, dict):
        sources.append(info)

    for source in sources:
        for key in ("position_side", "positionSide", "posSide"):
            value = _normalize_position_side(source.get(key))
            if value:
                return value
        if "positionIdx" in source:
            value = _normalize_position_side(source.get("positionIdx"))
            if value:
                return value

    for source in sources:
        for key in (
            "clientOrderId",
            "client_order_id",
            "orderLinkId",
            "order_link_id",
            "clientOid",
            "client_oid",
            "clOrdId",
        ):
            raw_value = source.get(key)
            if not raw_value:
                continue
            value = str(raw_value).lower()
            if value.endswith("_long") or "long" in value or "lng" in value:
                return "long"
            if value.endswith("_short") or "short" in value or "shrt" in value:
                return "short"

    reduce_only = None
    for source in sources:
        for key in ("reduceOnly", "reduce_only"):
            if key not in source:
                continue
            raw_value = source.get(key)
            if isinstance(raw_value, bool):
                reduce_only = raw_value
            elif isinstance(raw_value, str):
                reduce_only = raw_value.strip().lower() in {"true", "1", "yes", "y"}
            else:
                reduce_only = bool(raw_value)
            break
        if reduce_only is not None:
            break

    side = str(order.get("side", "")).strip().lower()
    if reduce_only is not None and side in {"buy", "sell"}:
        if side == "buy":
            return "short" if reduce_only else "long"
        return "long" if reduce_only else "short"

    return ""


def _has_hedged_symbol_positions(positions: list, symbol: str) -> bool:
    """Return True when both long and short legs are open for a symbol."""
    sides = set()
    for pos in positions:
        if pos[1] != symbol:
            continue
        try:
            size = float(pos[3])
        except (TypeError, ValueError):
            size = 0.0
        if size == 0.0:
            continue
        sides.add(str(pos[7] if len(pos) > 7 else "long").lower())
    return "long" in sides and "short" in sides


def _build_order_line(order: dict) -> dict[str, Any]:
    """Normalize a live order into the lightweight chart payload shape."""
    return {
        "price": order.get("price", 0),
        "amount": order.get("amount", order.get("qty", 0)),
        "side": order.get("side", "buy"),
    }


def _filter_live_orders_for_side(orders: list[dict], side: str) -> tuple[list[dict], bool]:
    """Return live orders for a leg plus whether any order stayed ambiguous."""
    normalized_side = str(side or "").lower()
    filtered = []
    has_ambiguous = False
    for order in orders or []:
        order_side = _extract_order_position_side(order)
        if order_side == normalized_side:
            filtered.append(_build_order_line(order))
        elif not order_side or order_side == "both":
            has_ambiguous = True
    return filtered, has_ambiguous


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert exchange payload values to finite floats."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _dashboard_symbol_from_ccxt(raw_symbol: Any) -> str:
    """Normalize CCXT symbols like DOGE/USDC:USDC to dashboard DOGEUSDC."""
    symbol = str(raw_symbol or "").strip().upper()
    if not symbol:
        return ""
    if "/" in symbol:
        base = symbol.split("/", 1)[0]
        quote = symbol.split("/", 1)[1].split(":", 1)[0]
        return (base + quote).replace("-", "")
    if ":" in symbol:
        symbol = symbol.split(":", 1)[0]
    return symbol.replace("/", "").replace("-", "")


def _live_position_side(position: dict) -> str:
    """Return a dashboard long/short side from a CCXT position payload."""
    for source in (position, position.get("info", {}) if isinstance(position.get("info"), dict) else {}):
        for key in ("side", "positionSide", "position_side", "posSide"):
            side = _normalize_position_side(source.get(key))
            if side in {"long", "short"}:
                return side
    signed_size = _safe_float(position.get("contracts") or position.get("size") or position.get("positionAmt"))
    return "short" if signed_size < 0 else "long"


def _live_position_size(position: dict) -> float:
    """Return dashboard position size from CCXT contracts and contractSize."""
    contracts = _safe_float(position.get("contracts"), 0.0)
    if contracts == 0.0:
        contracts = _safe_float(position.get("size") or position.get("positionAmt"), 0.0)
    contract_size = _safe_float(position.get("contractSize"), 1.0) or 1.0
    return abs(contracts * contract_size)


def _live_position_price(position: dict, exchange: Any, symbol_ccxt: str) -> float:
    """Return a fresh-ish position price, preferring exchange position fields."""
    info = position.get("info", {}) if isinstance(position.get("info"), dict) else {}
    for source in (position, info):
        for key in ("markPrice", "mark_price", "lastPrice", "last_price", "indexPrice", "oraclePrice"):
            price = _safe_float(source.get(key), 0.0)
            if price > 0.0:
                return price
    try:
        ticker = exchange.instance.fetch_ticker(symbol_ccxt)
        for key in ("last", "mark", "bid", "ask"):
            price = _safe_float(ticker.get(key), 0.0)
            if price > 0.0:
                return price
    except Exception:
        pass
    return 0.0


def _hyperliquid_user_state(user_obj: Any) -> dict[str, Any]:
    """Fetch Hyperliquid clearinghouse state for the configured wallet/vault address."""
    wallet = str(getattr(user_obj, "wallet_address", "") or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail=f"Hyperliquid user '{user_obj.name}' has no wallet_address configured")
    payload = json.dumps({"type": "clearinghouseState", "user": wallet}).encode()
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode() or "{}")


def _hyperliquid_open_orders(user_obj: Any, symbol: str | None = None) -> list[dict[str, Any]]:
    """Fetch Hyperliquid open orders for the configured wallet/vault address."""
    wallet = str(getattr(user_obj, "wallet_address", "") or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail=f"Hyperliquid user '{user_obj.name}' has no wallet_address configured")
    payload = json.dumps({"type": "openOrders", "user": wallet}).encode()
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw_orders = json.loads(resp.read().decode() or "[]")
    wanted_symbol = str(symbol or "").upper()
    result = []
    for order in raw_orders or []:
        if not isinstance(order, dict):
            continue
        coin = str(order.get("coin") or "").strip().upper()
        candidates = {coin, f"{coin}USDC", f"{coin}USDT"}
        if wanted_symbol and wanted_symbol not in candidates:
            continue
        raw_side = str(order.get("side") or "").strip().upper()
        result.append({
            "price": _safe_float(order.get("limitPx") or order.get("price"), 0.0),
            "amount": _safe_float(order.get("sz") or order.get("origSz") or order.get("amount"), 0.0),
            "side": "buy" if raw_side in {"B", "BUY"} else "sell",
            "info": order,
        })
    return result


def _live_open_orders_for_symbol(user_obj: Any, symbol: str) -> list[dict[str, Any]]:
    """Fetch normalized live open orders for a dashboard symbol."""
    if str(getattr(user_obj, "exchange", "")).lower() == "hyperliquid":
        return _hyperliquid_open_orders(user_obj, symbol)
    exchange = _get_exchange(user_obj)
    symbol_ccxt = _symbol_to_ccxt(symbol)
    raw_orders = exchange.instance.fetch_open_orders(symbol=symbol_ccxt)
    result = []
    for order in raw_orders or []:
        if str(order.get("status") or "open").lower() not in {"open", "new"}:
            continue
        normalized_order = dict(order)
        normalized_order.update({
            "price": _safe_float(order.get("price"), 0.0),
            "amount": _safe_float(order.get("amount") or order.get("qty"), 0.0),
            "side": str(order.get("side") or "buy").lower(),
            "info": order.get("info", order),
        })
        result.append(normalized_order)
    return result


def _order_rows_from_live_orders(orders: list[dict[str, Any]]) -> list[list[Any]]:
    """Convert normalized live orders to the DB row shape used by DCA/TP helpers."""
    rows = []
    for order in orders or []:
        rows.append([0, 0, 0, _safe_float(order.get("amount"), 0.0), _safe_float(order.get("price"), 0.0), str(order.get("side") or "").lower()])
    return rows


def _dashboard_orders_for_position(user_obj: Any, db: Any, symbol: str, side: str, live: bool = True) -> tuple[list[dict[str, Any]], bool, str]:
    """Return open orders for a position side, live-first with DB fallback."""
    if live:
        try:
            live_orders = _live_open_orders_for_symbol(user_obj, symbol)
            filtered, unknown = _filter_live_orders_for_side(live_orders, side)
            if not filtered and unknown:
                filtered = [_build_order_line(order) for order in live_orders]
            return filtered, unknown, "live"
        except Exception as exc:
            _log(SERVICE, f"Live orders fetch failed for '{user_obj.name}/{symbol}', falling back to DB: {exc}", level="WARNING", user=user_obj.name)
    db_orders = db.fetch_orders_by_symbol(user_obj.name, symbol) or []
    orders = []
    for o in sorted(db_orders, key=lambda x: x[4], reverse=True):
        orders.append({"price": o[4], "amount": o[3], "side": o[5]})
    return orders, False, "db"


def _classify_orders_for_position(user_obj: Any, db: Any, symbol: str, side: str, live: bool = True) -> tuple[int, float, float]:
    """Classify DCA/TP from live open orders with DB fallback."""
    if live:
        try:
            live_orders = _live_open_orders_for_symbol(user_obj, symbol)
            filtered, _unknown = _filter_live_orders_for_side(live_orders, side)
            if not filtered:
                filtered = [_build_order_line(order) for order in live_orders]
            return _classify_position_orders(_order_rows_from_live_orders(filtered), side)
        except Exception as exc:
            _log(SERVICE, f"Live order classification failed for '{user_obj.name}/{symbol}', falling back to DB: {exc}", level="WARNING", user=user_obj.name)
    orders = db.fetch_orders_by_symbol(user_obj.name, symbol) or []
    return _classify_position_orders(orders, side)


def _hyperliquid_live_positions_for_user(user_obj: Any, db: Any) -> list[dict[str, Any]]:
    """Build dashboard positions from Hyperliquid's authoritative account state."""
    state = _hyperliquid_user_state(user_obj)
    result: list[dict[str, Any]] = []
    for item in state.get("assetPositions") or []:
        position = item.get("position", {}) if isinstance(item, dict) else {}
        if not isinstance(position, dict):
            continue
        raw_size = _safe_float(position.get("szi"), 0.0)
        if raw_size == 0.0:
            continue
        coin = str(position.get("coin") or "").strip().upper()
        if not coin:
            continue
        symbol = coin if coin.endswith(("USDT", "USDC")) else f"{coin}USDC"
        side = "long" if raw_size > 0.0 else "short"
        size = abs(raw_size)
        entry = _safe_float(position.get("entryPx"), 0.0)
        upnl = _safe_float(position.get("unrealizedPnl"), 0.0)
        position_value = _safe_float(position.get("positionValue"), 0.0)
        price = position_value / size if size > 0.0 and position_value > 0.0 else entry
        dca = 0
        next_dca = 0.0
        next_tp = 0.0
        try:
            dca, next_dca, next_tp = _classify_orders_for_position(user_obj, db, symbol, side, live=True)
        except Exception:
            pass
        result.append({
            "user":     user_obj.name,
            "exchange": user_obj.exchange,
            "symbol":   symbol,
            "side":     side,
            "size":     size,
            "upnl":     round(upnl, 8),
            "entry":    entry,
            "price":    price,
            "dca":      dca,
            "next_dca": next_dca,
            "next_tp":  next_tp,
            "pos_value": round(position_value if position_value > 0.0 else size * price, 2),
        })
    return result


def _hyperliquid_live_balance_for_user(user_obj: Any) -> tuple[float, float]:
    """Return Hyperliquid wallet balance and uPnL from authoritative account state."""
    state = _hyperliquid_user_state(user_obj)
    account_value = _safe_float((state.get("marginSummary") or {}).get("accountValue"), 0.0)
    upnl = 0.0
    for item in state.get("assetPositions") or []:
        position = item.get("position", {}) if isinstance(item, dict) else {}
        if isinstance(position, dict):
            upnl += _safe_float(position.get("unrealizedPnl"), 0.0)
    return account_value - upnl, upnl


def _live_balance_for_user(user_obj: Any, db: Any) -> tuple[float, float, float]:
    """Return live balance, uPnL and position entry exposure for a dashboard user."""
    if str(getattr(user_obj, "exchange", "")).lower() == "hyperliquid":
        balance, upnl = _hyperliquid_live_balance_for_user(user_obj)
        positions = _hyperliquid_live_positions_for_user(user_obj, db)
    else:
        exchange = _get_exchange(user_obj)
        balance = _safe_float(exchange.fetch_balance("swap"), 0.0)
        positions = _live_positions_for_user(user_obj, db)
        upnl = sum(_safe_float(pos.get("upnl"), 0.0) for pos in positions)
    pprices = sum(_safe_float(pos.get("size"), 0.0) * _safe_float(pos.get("entry"), 0.0) for pos in positions)
    return balance, upnl, pprices


def _live_positions_for_user(user_obj: Any, db: Any) -> list[dict[str, Any]]:
    """Fetch open positions directly from the user's exchange for dashboard display."""
    if str(getattr(user_obj, "exchange", "")).lower() == "hyperliquid":
        return _hyperliquid_live_positions_for_user(user_obj, db)
    exchange = _get_exchange(user_obj)
    raw_positions = exchange.fetch_positions() or []
    result: list[dict[str, Any]] = []
    for position in raw_positions:
        if not isinstance(position, dict):
            continue
        size = _live_position_size(position)
        if size <= 0.0:
            continue
        symbol_ccxt = str(position.get("symbol") or "")
        symbol = _dashboard_symbol_from_ccxt(symbol_ccxt)
        if not symbol:
            continue
        side = _live_position_side(position)
        entry = _safe_float(position.get("entryPrice") or position.get("entry_price"), 0.0)
        upnl = _safe_float(position.get("unrealizedPnl") or position.get("unrealisedPnl"), 0.0)
        price = _live_position_price(position, exchange, symbol_ccxt or _symbol_to_ccxt(symbol))
        dca = 0
        next_dca = 0.0
        next_tp = 0.0
        try:
            dca, next_dca, next_tp = _classify_orders_for_position(user_obj, db, symbol, side, live=True)
        except Exception:
            pass
        result.append({
            "user":     user_obj.name,
            "exchange": user_obj.exchange,
            "symbol":   symbol,
            "side":     side,
            "size":     size,
            "upnl":     round(upnl, 8),
            "entry":    entry,
            "price":    price,
            "dca":      dca,
            "next_dca": next_dca,
            "next_tp":  next_tp,
            "pos_value": round(size * price, 2) if price > 0.0 else 0.0,
        })
    return result


def _position_close_order_side(side: str) -> str:
    """Return the market order side needed to reduce a position side."""
    normalized = str(side or "long").strip().lower()
    if normalized == "short":
        return "buy"
    if normalized == "long":
        return "sell"
    raise HTTPException(status_code=400, detail="side must be long or short")


def _dashboard_coin_key(symbol: str) -> str:
    """Normalize a position symbol to the coin_overrides key used by PBGui."""
    key = str(symbol or "").strip().upper()
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if len(key) > len(quote) and key.endswith(quote):
            key = key[:-len(quote)]
            break
    # Match the coin override editor: 1000BONKUSDT -> BONK, kSHIB -> SHIB.
    for prefix in ("1000", "100", "10"):
        if key.startswith(prefix) and len(key) > len(prefix):
            key = key[len(prefix):]
            break
    if len(key) > 1 and key[0] == "K" and key[1] != "K" and key[1:].isalpha():
        key = key[1:]
    if not key:
        raise HTTPException(status_code=400, detail="symbol required")
    return key


def _resolve_close_amount(position_size: float, amount: float | None, percent: float | None) -> float:
    """Resolve requested close amount and reject unsafe partial/full close values."""
    size = abs(float(position_size or 0.0))
    if not math.isfinite(size) or size <= 0.0:
        raise HTTPException(status_code=400, detail="Position size is zero")
    requested = None
    if amount is not None:
        requested = float(amount)
    elif percent is not None:
        pct = float(percent)
        if not math.isfinite(pct) or pct <= 0.0 or pct > 100.0:
            raise HTTPException(status_code=400, detail="percent must be between 0 and 100")
        requested = size * pct / 100.0
    if requested is None:
        raise HTTPException(status_code=400, detail="amount or percent required")
    if not math.isfinite(requested) or requested <= 0.0:
        raise HTTPException(status_code=400, detail="amount must be greater than zero")
    if requested > size:
        raise HTTPException(status_code=400, detail="amount exceeds position size")
    return requested


def _apply_symbol_forced_mode(cfg: dict, symbol: str, side: str, mode: str) -> str:
    """Set a per-symbol long/short forced_mode override."""
    normalized_side = str(side or "long").strip().lower()
    if normalized_side not in {"long", "short"}:
        raise HTTPException(status_code=400, detail="side must be long or short")
    coin = _dashboard_coin_key(symbol)
    forced_key = "forced_mode_long" if normalized_side == "long" else "forced_mode_short"
    coin_cfg = cfg.setdefault("coin_overrides", {}).setdefault(coin, {})
    live_cfg = coin_cfg.setdefault("live", {})
    live_cfg[forced_key] = mode
    return coin


def _apply_all_forced_mode(cfg: dict, mode: str) -> None:
    """Set global PB7 forced modes for all long and short positions."""
    live = cfg.setdefault("live", {})
    live["forced_mode_long"] = mode
    live["forced_mode_short"] = mode


def _apply_panic_symbol(cfg: dict, symbol: str, side: str) -> str:
    """Set a per-symbol long/short forced_mode override to PB7 panic."""
    return _apply_symbol_forced_mode(cfg, symbol, side, _PANIC_OVERRIDE_MODE)


def _apply_panic_all(cfg: dict) -> None:
    """Set global PB7 forced modes to panic for all long and short positions."""
    _apply_all_forced_mode(cfg, _PANIC_GLOBAL_MODE)


def _apply_graceful_stop_symbol(cfg: dict, symbol: str, side: str) -> str:
    """Set a per-symbol long/short forced_mode override to graceful stop."""
    return _apply_symbol_forced_mode(cfg, symbol, side, _GRACEFUL_STOP_MODE)


def _apply_graceful_stop_all(cfg: dict) -> None:
    """Set global PB7 forced modes to graceful stop for all long and short positions."""
    _apply_all_forced_mode(cfg, _GRACEFUL_STOP_MODE)


def _apply_tp_only_symbol(cfg: dict, symbol: str, side: str) -> str:
    """Set a per-symbol long/short forced_mode override to take profit only."""
    return _apply_symbol_forced_mode(cfg, symbol, side, _TP_ONLY_MODE)


def _apply_tp_only_all(cfg: dict) -> None:
    """Set global PB7 forced modes to take profit only for all long and short positions."""
    _apply_all_forced_mode(cfg, _TP_ONLY_MODE)


def _find_dashboard_position(positions: list, symbol: str, side: str) -> list | None:
    """Find a DB position row by symbol and normalized side."""
    normalized_side = str(side or "long").strip().lower()
    for pos in positions or []:
        pos_side = str(pos[7] if len(pos) > 7 else "long").strip().lower()
        if pos[1] == symbol and pos_side == normalized_side:
            return pos
    return None


def _dashboard_row_from_live_position(pos: dict[str, Any]) -> list:
    """Convert a live dashboard position dict to the DB row shape used by helpers."""
    return [
        0,
        pos.get("symbol", ""),
        0,
        _safe_float(pos.get("size"), 0.0),
        _safe_float(pos.get("upnl"), 0.0),
        _safe_float(pos.get("entry"), 0.0),
        pos.get("user", ""),
        str(pos.get("side") or "long").lower(),
    ]


def _run_v7_dir() -> Path:
    """Return the local V7 run instances directory."""
    return Path(PBGDIR) / "data" / "run_v7"


def _load_dashboard_instance_config(config_path: Path) -> dict:
    """Load an instance config through the PB7 config pipeline."""
    from pb7_config import load_pb7_config
    return load_pb7_config(config_path, neutralize_added=False)


def _find_instance_config_for_user(user_name: str) -> tuple[str, Path, dict]:
    """Find exactly one local V7 instance config for a live.user value."""
    user = str(user_name or "").strip()
    if not user:
        raise HTTPException(status_code=400, detail="user required")
    matches: list[tuple[str, Path, dict]] = []
    base_dir = _run_v7_dir()
    if not base_dir.is_dir():
        raise HTTPException(status_code=404, detail="No V7 instances directory found")
    for config_path in sorted(base_dir.glob("*/config.json")):
        try:
            cfg = _load_dashboard_instance_config(config_path)
        except Exception as exc:
            _log(SERVICE, f"Skipping unreadable V7 config {config_path}: {exc}", level="WARNING")
            continue
        if str(cfg.get("live", {}).get("user", "")).strip() == user:
            matches.append((config_path.parent.name, config_path, cfg))
    if not matches:
        raise HTTPException(status_code=404, detail=f"No V7 instance found for user '{user}'")
    if len(matches) > 1:
        names = ", ".join(name for name, _, _ in matches)
        raise HTTPException(status_code=409, detail=f"Multiple V7 instances found for user '{user}': {names}")
    return matches[0]


def _backup_dashboard_instance_config(instance_dir: Path, name: str, cfg: dict) -> None:
    """Create the same versioned JSON backup shape used by the V7 editor."""
    try:
        old_version = cfg.get("pbgui", {}).get("version", 0)
        backup_dir = instance_dir.parent.parent / "backup" / "v7" / name / str(old_version)
        if backup_dir.exists():
            return
        backup_dir.mkdir(parents=True, exist_ok=True)
        for item in instance_dir.iterdir():
            if item.suffix == ".json" and item.name not in (
                "config.json.tmp", "ignored_coins.json", "approved_coins.json", "config_run.json"
            ):
                import shutil
                shutil.copy2(str(item), str(backup_dir / item.name))
    except Exception as exc:
        _log(SERVICE, f"Failed to create dashboard panic backup for '{name}': {exc}", level="WARNING")


async def _save_dashboard_panic_config(name: str, config_path: Path, cfg: dict) -> dict:
    """Save a forced-mode mutation and materialize it immediately on the bot host."""
    from api.cluster import sync_and_materialize_v7_instance
    from api.v7_instances import _ensure_target_runtime_compatible, _record_cluster_config_upsert
    from pb7_config import save_pb7_config

    await _ensure_target_runtime_compatible(name, cfg)
    _backup_dashboard_instance_config(config_path.parent, name, cfg)
    previous_version = cfg["pbgui"].get("version", 0)
    cfg.setdefault("pbgui", {})["version"] = cfg["pbgui"].get("version", 0) + 1
    save_pb7_config(cfg, config_path)
    _record_cluster_config_upsert(name, config_path.parent, cfg, parent_version=previous_version)
    version = cfg["pbgui"]["version"]
    sync_result = await sync_and_materialize_v7_instance(name, expected_version=version)
    return {"name": name, "version": version, "sync": sync_result}


def _prune_exchange_cache(now: float, keep_key: str | None = None) -> None:
    """Close idle or excess REST exchange clients without evicting the active key."""
    stale_keys = [
        key for key, last_used in _exchange_cache_last_used.items()
        if key != keep_key and now - last_used >= _EXCHANGE_CACHE_IDLE_SECONDS
    ]
    remaining = len(_exchange_cache) - len(stale_keys)
    if remaining >= _EXCHANGE_CACHE_MAX_SIZE:
        candidates = sorted(
            (last_used, key) for key, last_used in _exchange_cache_last_used.items()
            if key != keep_key and key not in stale_keys
        )
        stale_keys.extend(key for _, key in candidates[:remaining - _EXCHANGE_CACHE_MAX_SIZE + 1])
    for key in stale_keys:
        exchange = _exchange_cache.pop(key, None)
        _exchange_cache_last_used.pop(key, None)
        if exchange:
            exchange.close()


def _get_exchange(user_obj):
    """Return a cached, connected Exchange instance for the given user object."""
    from Exchange import Exchange
    key = f"{user_obj.name}:{user_obj.exchange}"
    now = _time.time()
    with _exchange_cache_lock:
        _prune_exchange_cache(now, keep_key=key)
        if key not in _exchange_cache:
            ex = Exchange(user_obj.exchange, user_obj)
            ex.connect()
            _exchange_cache[key] = ex
        _exchange_cache_last_used[key] = now
        return _exchange_cache[key]


def _market_close_params(exchange_id: str, side: str, hedged_symbol: bool = False) -> dict[str, Any]:
    """Build conservative reduce-only params for one-way and hedge-mode swaps."""
    normalized_side = str(side or "long").strip().lower()
    ex_id = str(exchange_id or "").strip().lower()
    params: dict[str, Any] = {"reduceOnly": True}
    if ex_id == "bitget":
        params["holdSide"] = "short" if normalized_side == "short" else "long"
        params["oneWayMode"] = False
        return params
    if ex_id == "gateio":
        params["reduce_only"] = True
        return params
    if ex_id == "okx":
        params["hedged"] = True
        params["posSide"] = "short" if normalized_side == "short" else "long"
        return params
    if not hedged_symbol:
        return params
    if ex_id == "binance":
        params["positionSide"] = "SHORT" if normalized_side == "short" else "LONG"
    elif ex_id == "bybit":
        params["positionIdx"] = 2 if normalized_side == "short" else 1
    return params


def _market_close_param_candidates(exchange_id: str, side: str, hedged_symbol: bool = False) -> list[dict[str, Any]]:
    """Return reduce-only order param candidates, including hedge/one-way fallbacks."""
    ex_id = str(exchange_id or "").strip().lower()
    normalized_side = str(side or "long").strip().lower()
    if ex_id == "binance":
        hedge_params = {"positionSide": "SHORT" if normalized_side == "short" else "LONG"}
        one_way_params = {"reduceOnly": True}
        return [hedge_params, one_way_params]
    if ex_id != "bybit":
        return [_market_close_params(exchange_id, side, hedged_symbol)]
    hedge_params = {"reduceOnly": True, "positionIdx": 2 if normalized_side == "short" else 1}
    one_way_params = {"reduceOnly": True}
    return [hedge_params, one_way_params]


def _is_position_mode_mismatch(exc: Exception) -> bool:
    """Detect exchange errors caused by hedge/one-way position mode mismatch."""
    text = str(exc).lower()
    return (
        "position idx not match position mode" in text
        or "positionidx" in text and "position mode" in text
        or "position side does not match" in text
    )


def _is_amount_precision_error(exc: Exception) -> bool:
    """Detect user-correctable exchange amount precision/minimum errors."""
    text = str(exc).lower()
    return "minimum amount precision" in text or "amount" in text and "precision" in text and "minimum" in text


def _dashboard_price_from_rows(prices: list, symbol: str) -> float:
    """Return the latest dashboard price for a symbol from DB price rows."""
    for row in prices or []:
        if len(row) > 3 and row[1] == symbol:
            try:
                price = float(row[3])
            except (TypeError, ValueError):
                price = 0.0
            if math.isfinite(price) and price > 0.0:
                return price
    return 0.0


def _market_close_price_arg(exchange_instance: Any, exchange_id: str, symbol_ccxt: str, side: str, db_price: float) -> float | None:
    """Return the market-order price argument required by Hyperliquid CCXT."""
    if str(exchange_id or "").strip().lower() != "hyperliquid":
        return None
    normalized_side = str(side or "").strip().lower()
    preferred_keys = ("ask", "last", "mark", "index", "bid") if normalized_side == "buy" else ("bid", "last", "mark", "index", "ask")
    try:
        ticker = exchange_instance.fetch_ticker(symbol_ccxt)
        for key in preferred_keys:
            price = float(ticker.get(key) or 0.0)
            if math.isfinite(price) and price > 0.0:
                return price
    except Exception as exc:
        _log(SERVICE, f"Failed to fetch Hyperliquid ticker for market close {symbol_ccxt}: {exc}", level="WARNING")
    if math.isfinite(float(db_price or 0.0)) and float(db_price or 0.0) > 0.0:
        return float(db_price)
    raise HTTPException(status_code=400, detail="Market price unavailable for Hyperliquid market close")


def _apply_market_close_user_params(params: dict[str, Any], exchange_id: str, user_obj: Any) -> dict[str, Any]:
    """Add account-specific order params needed by selected exchanges."""
    ex_id = str(exchange_id or "").strip().lower()
    if ex_id == "hyperliquid" and bool(getattr(user_obj, "is_vault", False)):
        wallet_address = str(getattr(user_obj, "wallet_address", "") or "").strip()
        if wallet_address:
            params = dict(params)
            params["vaultAddress"] = wallet_address
    return params


def _market_close_min_cost(exchange_instance: Any, exchange_id: str, symbol_ccxt: str) -> float:
    """Return exchange minimum market-close value when known."""
    if str(exchange_id or "").strip().lower() != "hyperliquid":
        return 0.0
    try:
        market = exchange_instance.market(symbol_ccxt)
        min_cost = float(((market.get("limits") or {}).get("cost") or {}).get("min") or 0.0)
        if math.isfinite(min_cost) and min_cost > 0.0:
            return min_cost
    except Exception:
        pass
    return 10.0


def _validate_market_close_min_cost(exchange_id: str, amount: float, price: float | None, min_cost: float) -> None:
    """Reject Hyperliquid closes below the minimum order value before submission."""
    if str(exchange_id or "").strip().lower() != "hyperliquid" or min_cost <= 0.0:
        return
    value = abs(float(amount or 0.0)) * abs(float(price or 0.0))
    if not math.isfinite(value) or value >= min_cost:
        return
    min_amount = min_cost / abs(float(price or 1.0)) if price else 0.0
    raise HTTPException(
        status_code=400,
        detail=(
            f"Hyperliquid minimum order value is ${min_cost:g}. "
            f"Selected close value is ${value:g}; use at least {min_amount:.8g} amount."
        ),
    )


def _market_close_price_snapshot(user_obj: Any, symbol: str, side: str) -> dict[str, Any]:
    """Return the current market close reference price and minimum value."""
    exchange = _get_exchange(user_obj)
    if not getattr(exchange, "instance", None):
        raise HTTPException(status_code=500, detail="Exchange is not connected")
    symbol_ccxt = _symbol_to_ccxt(symbol)
    db_price = _dashboard_price_from_rows(_get_db().fetch_prices(user_obj) or [], symbol)
    close_side = _position_close_order_side(side)
    price = _market_close_price_arg(exchange.instance, user_obj.exchange, symbol_ccxt, close_side, db_price)
    min_cost = _market_close_min_cost(exchange.instance, user_obj.exchange, symbol_ccxt)
    return {"price": price, "min_cost": min_cost}


def _precision_amount(exchange_instance: Any, symbol: str, amount: float) -> float:
    """Apply exchange amount precision when CCXT exposes it."""
    try:
        precise = exchange_instance.amount_to_precision(symbol, amount)
        value = float(precise)
        if value > 0.0:
            return value
    except Exception:
        pass
    return amount


class PositionManagePayload(BaseModel):
    """Dashboard position action payload."""

    user: str
    symbol: str = ""
    side: str = "long"
    action: str = "market_close"
    amount: float | None = None
    percent: float | None = None
    dry_run: bool = False


# ── OHLCV in-memory cache ──────────────────────────────────────────────────
# Key: (user_name, symbol, timeframe)
# Value: {"candles": [...], "ts": last_update_epoch, "exchange_id": str}
_ohlcv_cache: dict[tuple[str, str, str], dict] = {}
_ohlcv_cache_lock = threading.Lock()

# Active subscriptions that the background poller refreshes.
# Key: same tuple, Value: epoch when last requested by a client
_ohlcv_active: dict[tuple[str, str, str], float] = {}

# How many seconds a subscription stays alive without client requests
_OHLCV_TTL = 300  # 5 min
# Poll interval for the background thread
_OHLCV_POLL_INTERVAL = 5  # seconds

_ohlcv_poller_started = False
_ohlcv_poller_thread: threading.Thread | None = None
_ohlcv_poller_stop = threading.Event()


def _symbol_to_ccxt(symbol: str) -> str:
    """Convert exchange symbol (e.g. BTCUSDT) to CCXT format."""
    if symbol.endswith("USDT"):
        return f'{symbol[:-4]}/USDT:USDT'
    elif symbol.endswith("USDC"):
        return f'{symbol[:-4]}/USDC:USDC'
    return symbol


def _ohlcv_cache_get(user_name: str, symbol: str, tf: str, limit: int = 500):
    """Return cached candles or None.  Also touches the active subscription."""
    key = (user_name, symbol, tf)
    _ohlcv_active[key] = _time.time()
    with _ohlcv_cache_lock:
        entry = _ohlcv_cache.get(key)
        if entry and entry["candles"]:
            candles = entry["candles"][-limit:]
            return candles
    return None


def _ohlcv_cache_put(user_name: str, symbol: str, tf: str, candles: list):
    """Store / merge candles into cache."""
    key = (user_name, symbol, tf)
    with _ohlcv_cache_lock:
        existing = _ohlcv_cache.get(key)
        if existing and existing["candles"]:
            # Merge: keep old candles, update/append new ones by timestamp
            by_ts = {c[0]: c for c in existing["candles"]}
            for c in candles:
                by_ts[c[0]] = c
            merged = sorted(by_ts.values(), key=lambda x: x[0])
            _ohlcv_cache[key] = {"candles": merged, "ts": _time.time()}
        else:
            _ohlcv_cache[key] = {"candles": sorted(candles, key=lambda x: x[0]),
                                 "ts": _time.time()}


def _ohlcv_poll_loop():
    """Background thread: polls the last candle for active subscriptions.

    Skips keys that have an active ccxt.pro watchOHLCV stream (Phase 2).
    """
    from logging_helpers import human_log as _log
    _log("Dashboard", "OHLCV poller thread started")
    while not _ohlcv_poller_stop.wait(_OHLCV_POLL_INTERVAL):
        now = _time.time()
        # Copy keys to avoid mutation during iteration
        keys = list(_ohlcv_active.keys())
        for key in keys:
            last_req = _ohlcv_active.get(key, 0)
            if now - last_req > _OHLCV_TTL:
                _ohlcv_active.pop(key, None)
                with _ohlcv_cache_lock:
                    _ohlcv_cache.pop(key, None)
                continue
            # Skip if ccxt.pro stream is active for this key
            ws_task = _ws_ohlcv_tasks.get(key)
            if ws_task and not ws_task.done():
                continue
            user_name, symbol, tf = key
            try:
                all_users = _get_users()
                user_obj = all_users.find_user(user_name)
                if not user_obj:
                    continue
                exchange = _get_exchange(user_obj)
                symbol_ccxt = _symbol_to_ccxt(symbol)
                # Fetch only last 2 candles for the update
                ohlcv = exchange.fetch_ohlcv(symbol_ccxt, "futures",
                                             timeframe=tf, limit=2)
                if ohlcv:
                    _ohlcv_cache_put(user_name, symbol, tf, ohlcv)
                    # Push update to WS clients
                    _notify_candle_update(user_name, symbol, tf, ohlcv[-1],
                                          from_thread=True)
            except Exception as e:
                _log("Dashboard", f"OHLCV poll error {key}: {e}", level="WARNING")
    _log("Dashboard", "OHLCV poller thread stopped")


def _start_ohlcv_poller():
    """Start the background poller thread (once)."""
    global _ohlcv_poller_started, _ohlcv_poller_thread
    if _ohlcv_poller_thread is not None and _ohlcv_poller_thread.is_alive():
        return
    _ohlcv_poller_stop.clear()
    _ohlcv_poller_started = True
    _ohlcv_poller_thread = threading.Thread(
        target=_ohlcv_poll_loop,
        daemon=True,
        name="dashboard-ohlcv-poller",
    )
    _ohlcv_poller_thread.start()


# ── Event loop reference for cross-thread queue access (Phase 2) ──────────
_event_loop: _asyncio.AbstractEventLoop | None = None


def _set_event_loop(loop: _asyncio.AbstractEventLoop):
    """Store FastAPI's event loop for polling-thread → asyncio.Queue bridging."""
    global _event_loop
    _event_loop = loop


# ── Candle WS push ─────────────────────────────────────────────────────────
# Subscribers use asyncio.Queue.  Messages are pre-formatted dicts:
#   {"type": "candle", "candle": [t,o,h,l,c,v]}
#   {"type": "position", "position": {...}}
#   {"type": "orders", "orders": [...]}
_candle_subscribers: dict[tuple[str, str, str], set] = {}
_candle_sub_lock = threading.Lock()

# Position subscribers: key = (user_name, symbol, side)
_position_subscribers: dict[tuple[str, str, str], set] = {}
_position_sub_lock = threading.Lock()

# Order subscribers: key = (user_name, symbol, side)
_order_subscribers: dict[tuple[str, str, str], set] = {}
_order_sub_lock = threading.Lock()


def _push_to_subs(subs: set, msg: dict, from_thread: bool = False):
    """Push a message to a set of asyncio.Queue subscribers.

    from_thread=True uses loop.call_soon_threadsafe (for the polling thread).
    from_thread=False uses put_nowait directly (for async ccxt.pro streams).
    """
    dead = set()
    for q in subs:
        try:
            if from_thread and _event_loop and not _event_loop.is_closed():
                _event_loop.call_soon_threadsafe(q.put_nowait, msg)
            else:
                q.put_nowait(msg)
        except Exception:
            dead.add(q)
    if dead:
        subs -= dead


def _notify_candle_update(user_name: str, symbol: str, tf: str, candle: list,
                          from_thread: bool = True):
    """Push a candle update to all subscribed WS queues."""
    msg = {"type": "candle", "candle": candle}
    key = (user_name, symbol, tf)
    with _candle_sub_lock:
        subs = _candle_subscribers.get(key)
        if not subs:
            return
        _push_to_subs(subs, msg, from_thread=from_thread)


def _notify_position_update(user_name: str, symbol: str, side: str, position_data: dict | None,
                            from_thread: bool = False):
    """Push a position update to subscribers watching (user, symbol)."""
    msg = {"type": "position", "position": position_data}
    key = (user_name, symbol, str(side or "long").lower())
    with _position_sub_lock:
        subs = _position_subscribers.get(key)
        if not subs:
            return
        _push_to_subs(subs, msg, from_thread=from_thread)


def _notify_order_update(user_name: str, symbol: str, side: str, orders_data: list,
                         unknown: bool = False,
                         from_thread: bool = False):
    """Push an orders update to subscribers watching (user, symbol)."""
    msg = {"type": "orders", "orders": orders_data, "orders_unknown": bool(unknown)}
    key = (user_name, symbol, str(side or "long").lower())
    with _order_sub_lock:
        subs = _order_subscribers.get(key)
        if not subs:
            return
        _push_to_subs(subs, msg, from_thread=from_thread)


def refresh_positions_for_user(user_name: str):
    """Read current positions from DB for user_name and push to all chart subscribers.

    Called from the internal /notify/positions endpoint after PBData writes updated
    positions.  Ensures the Orders widget entry line is corrected even when the
    ccxt.pro watchPositions() stream missed the original 'position closed' event.
    Runs synchronously (called via run_in_executor from the async endpoint).
    """
    try:
        all_users = _get_users()
        user_obj = all_users.find_user(user_name)
        if not user_obj:
            return
        db = _get_db()
        positions = db.fetch_positions(user_obj) or []
        # Build a dict: (symbol, side) -> position_data (only open positions)
        open_pos: dict[tuple[str, str], dict] = {}
        for pos in positions:
            sym = pos[1]
            if pos[3]:  # size != 0 means open
                side = str(pos[7] if len(pos) > 7 else "long").lower()
                open_pos[(sym, side)] = {
                    "entry": pos[5],
                    "size":  pos[3],
                    "upnl":  pos[4],
                    "side":  side,
                }
        # Notify all chart subscribers for this user
        with _position_sub_lock:
            sub_keys = [k for k in _position_subscribers if k[0] == user_name]
        for key in sub_keys:
            _, sym, side = key
            pos_data = open_pos.get((sym, side), None)  # None = position closed
            _notify_position_update(user_name, sym, side, pos_data, from_thread=True)
    except Exception:
        pass


# ── ccxt.pro live stream tasks (Phase 2) ───────────────────────────────────
_ws_ohlcv_tasks: dict[tuple[str, str, str], _asyncio.Task] = {}
_ws_position_tasks: dict[str, _asyncio.Task] = {}   # per user
_ws_order_tasks: dict[str, _asyncio.Task] = {}       # per user
_stream_task_lock: _asyncio.Lock | None = None


def _get_stream_task_lock() -> _asyncio.Lock:
    global _stream_task_lock
    if _stream_task_lock is None:
        _stream_task_lock = _asyncio.Lock()
    return _stream_task_lock


async def _watch_ohlcv_stream(user_name: str, symbol: str, tf: str):
    """Async task: watches OHLCV via ccxt.pro and pushes to candle subscribers."""
    from Exchange import Exchange
    from logging_helpers import human_log as _log
    key = (user_name, symbol, tf)
    user_obj = _get_users().find_user(user_name)
    if not user_obj:
        return
    ex_id = "kucoinfutures" if user_obj.exchange == "kucoin" else user_obj.exchange
    caller = f"dashboard_ohlcv:{user_name}:{symbol}:{tf}"
    try:
        ws_client = await Exchange.get_shared_ws_client(ex_id, user_obj, caller=caller)
    except Exception as e:
        _log("Dashboard", f"ccxt.pro unavailable for {ex_id}: {e}", level="WARNING")
        return
    if not ws_client:
        _log("Dashboard", f"ccxt.pro client unavailable for {ex_id}")
        return
    symbol_ccxt = _symbol_to_ccxt(symbol)
    _log("Dashboard", f"watchOHLCV started: {user_name} {symbol} {tf}")
    try:
        while True:
            try:
                ohlcv = await ws_client.watchOHLCV(symbol_ccxt, tf)
                if ohlcv:
                    _ohlcv_cache_put(user_name, symbol, tf, ohlcv)
                    _notify_candle_update(user_name, symbol, tf, ohlcv[-1],
                                          from_thread=False)
            except _asyncio.CancelledError:
                raise
            except Exception as e:
                _log("Dashboard", f"watchOHLCV error {key}: {e}", level="WARNING")
                await _asyncio.sleep(5)
    except _asyncio.CancelledError:
        _log("Dashboard", f"watchOHLCV stopped: {user_name} {symbol} {tf}")
    finally:
        await Exchange.release_shared_ws_client(ex_id, caller=caller)


async def _watch_positions_stream(user_name: str):
    """Async task: watches positions via ccxt.pro private stream."""
    from Exchange import Exchange
    from logging_helpers import human_log as _log
    user_obj = _get_users().find_user(user_name)
    if not user_obj:
        return
    ex_id = "kucoinfutures" if user_obj.exchange == "kucoin" else user_obj.exchange
    caller = "dashboard_positions"
    try:
        ws_client = await Exchange.get_private_ws_client(ex_id, user_obj,
                                                         caller=caller)
    except Exception as e:
        _log("Dashboard", f"ccxt.pro private unavailable for {ex_id}/{user_name}: {e}",
             level="WARNING")
        return
    if not ws_client:
        _log("Dashboard", f"ccxt.pro private client unavailable for {ex_id}/{user_name}")
        return
    _log("Dashboard", f"watchPositions started: {user_name}")
    try:
        while True:
            try:
                positions = await ws_client.watchPositions()
                if not positions:
                    continue
                # Push matching position to each (user, symbol, side) subscriber
                with _position_sub_lock:
                    sub_keys = [k for k in _position_subscribers if k[0] == user_name]
                for key in sub_keys:
                    _, sym, wanted_side = key
                    sym_ccxt = _symbol_to_ccxt(sym)
                    pos_data = None
                    for p in positions:
                        if p.get("symbol") == sym_ccxt and str(p.get("side", "long")).lower() == wanted_side:
                            pos_data = {
                                "entry": p.get("entryPrice", 0),
                                "size": p.get("contracts", 0),
                                "upnl": p.get("unrealizedPnl", 0),
                                "side": p.get("side", "long"),
                            }
                            break
                    _notify_position_update(user_name, sym, wanted_side, pos_data)
            except _asyncio.CancelledError:
                raise
            except Exception as e:
                _log("Dashboard", f"watchPositions error {user_name}: {e}",
                     level="WARNING")
                await _asyncio.sleep(5)
    except _asyncio.CancelledError:
        _log("Dashboard", f"watchPositions stopped: {user_name}")
    finally:
        await Exchange.release_private_ws_client(ex_id, user_obj, caller=caller)


async def _watch_orders_stream(user_name: str):
    """Async task: watches open orders via ccxt.pro private stream."""
    from Exchange import Exchange
    from logging_helpers import human_log as _log
    user_obj = _get_users().find_user(user_name)
    if not user_obj:
        return
    ex_id = "kucoinfutures" if user_obj.exchange == "kucoin" else user_obj.exchange
    caller = "dashboard_orders"
    try:
        ws_client = await Exchange.get_private_ws_client(ex_id, user_obj,
                                                         caller=caller)
    except Exception as e:
        _log("Dashboard", f"ccxt.pro private unavailable for {ex_id}/{user_name}: {e}",
             level="WARNING")
        return
    if not ws_client:
        _log("Dashboard", f"ccxt.pro private client unavailable for {ex_id}/{user_name}")
        return
    _log("Dashboard", f"watchOrders started: {user_name}")
    try:
        while True:
            try:
                orders = await ws_client.watchOrders()
                if not orders:
                    continue
                # Group orders by symbol and push to matching subscribers
                by_symbol: dict[str, list] = {}
                for o in orders:
                    if o.get("status") != "open":
                        continue
                    sym_ccxt = o.get("symbol", "")
                    by_symbol.setdefault(sym_ccxt, []).append(o)
                with _order_sub_lock:
                    sub_keys = [k for k in _order_subscribers if k[0] == user_name]
                for key in sub_keys:
                    _, sym, wanted_side = key
                    sym_ccxt = _symbol_to_ccxt(sym)
                    sym_orders, orders_unknown = _filter_live_orders_for_side(
                        by_symbol.get(sym_ccxt, []), wanted_side
                    )
                    _notify_order_update(user_name, sym, wanted_side, sym_orders, unknown=orders_unknown)
            except _asyncio.CancelledError:
                raise
            except Exception as e:
                _log("Dashboard", f"watchOrders error {user_name}: {e}",
                     level="WARNING")
                await _asyncio.sleep(5)
    except _asyncio.CancelledError:
        _log("Dashboard", f"watchOrders stopped: {user_name}")
    finally:
        await Exchange.release_private_ws_client(ex_id, user_obj, caller=caller)


async def register_chart_client(user_name: str, symbol: str, tf: str, side: str,
                                q: _asyncio.Queue):
    """Register a WS client queue for candle/position/order updates.

    Starts ccxt.pro streams if not already running for this key.
    Falls back to polling if ccxt.pro is unavailable.
    """
    from logging_helpers import human_log as _log
    candle_key = (user_name, symbol, tf)
    normalized_side = str(side or "long").lower()
    pos_key = (user_name, symbol, normalized_side)

    # Register in subscriber dicts
    with _candle_sub_lock:
        _candle_subscribers.setdefault(candle_key, set()).add(q)
    with _position_sub_lock:
        _position_subscribers.setdefault(pos_key, set()).add(q)
    with _order_sub_lock:
        _order_subscribers.setdefault(pos_key, set()).add(q)

    # Touch active subscription for polling fallback
    _ohlcv_active[candle_key] = _time.time()

    # Immediately push current DB position state to the new subscriber.
    # This reconciles any stale entry line: if PBData already wrote 'position closed'
    # to the DB (but the ccxt.pro watchPositions stream hasn't fired yet for this
    # new subscriber), the client gets the correct state right away.
    try:
        user_obj = _get_users().find_user(user_name)
        if user_obj:
            positions = _get_db().fetch_positions(user_obj) or []
            pos_data = None
            for pos in positions:
                if pos[1] == symbol and pos[3] and str(pos[7] if len(pos) > 7 else "long").lower() == normalized_side:
                    pos_data = {
                        "entry": pos[5], "size": pos[3],
                        "upnl":  pos[4], "side": pos[7] if len(pos) > 7 else "long",
                    }
                    break
            q.put_nowait({"type": "position", "position": pos_data})
    except Exception:
        pass

    async with _get_stream_task_lock():
        # Try to start ccxt.pro OHLCV stream
        if candle_key not in _ws_ohlcv_tasks or _ws_ohlcv_tasks[candle_key].done():
            try:
                task = _asyncio.create_task(
                    _watch_ohlcv_stream(user_name, symbol, tf),
                    name=f"watchOHLCV-{user_name}-{symbol}-{tf}"
                )
                _ws_ohlcv_tasks[candle_key] = task
            except Exception as e:
                _log("Dashboard", f"Failed to start watchOHLCV: {e}", level="WARNING")

        # Try to start ccxt.pro position stream (one per user)
        if user_name not in _ws_position_tasks or _ws_position_tasks[user_name].done():
            try:
                task = _asyncio.create_task(
                    _watch_positions_stream(user_name),
                    name=f"watchPositions-{user_name}"
                )
                _ws_position_tasks[user_name] = task
            except Exception as e:
                _log("Dashboard", f"Failed to start watchPositions: {e}", level="WARNING")

        # Try to start ccxt.pro order stream (one per user)
        if user_name not in _ws_order_tasks or _ws_order_tasks[user_name].done():
            try:
                task = _asyncio.create_task(
                    _watch_orders_stream(user_name),
                    name=f"watchOrders-{user_name}"
                )
                _ws_order_tasks[user_name] = task
            except Exception as e:
                _log("Dashboard", f"Failed to start watchOrders: {e}", level="WARNING")

    # Ensure polling fallback is running
    _start_ohlcv_poller()


async def unregister_chart_client(user_name: str, symbol: str, tf: str, side: str,
                                  q: _asyncio.Queue):
    """Remove a WS client queue and stop streams if no subscribers left."""
    candle_key = (user_name, symbol, tf)
    pos_key = (user_name, symbol, str(side or "long").lower())

    with _candle_sub_lock:
        subs = _candle_subscribers.get(candle_key)
        if subs:
            subs.discard(q)
            if not subs:
                _candle_subscribers.pop(candle_key, None)
    with _position_sub_lock:
        subs = _position_subscribers.get(pos_key)
        if subs:
            subs.discard(q)
            if not subs:
                _position_subscribers.pop(pos_key, None)
    with _order_sub_lock:
        subs = _order_subscribers.get(pos_key)
        if subs:
            subs.discard(q)
            if not subs:
                _order_subscribers.pop(pos_key, None)

    tasks_to_stop: list[_asyncio.Task] = []
    async with _get_stream_task_lock():
        # Stop OHLCV stream if no more candle subscribers for this key
        with _candle_sub_lock:
            has_candle_subs = bool(_candle_subscribers.get(candle_key))
        if not has_candle_subs:
            task = _ws_ohlcv_tasks.get(candle_key)
            if task is not None:
                tasks_to_stop.append(task)

        # Stop position stream if no more position subscribers for this user
        with _position_sub_lock:
            has_pos_subs = any(k[0] == user_name for k in _position_subscribers)
        if not has_pos_subs:
            task = _ws_position_tasks.get(user_name)
            if task is not None:
                tasks_to_stop.append(task)

        # Stop order stream if no more order subscribers for this user
        with _order_sub_lock:
            has_ord_subs = any(k[0] == user_name for k in _order_subscribers)
        if not has_ord_subs:
            task = _ws_order_tasks.get(user_name)
            if task is not None:
                tasks_to_stop.append(task)

        for task in tasks_to_stop:
            if not task.done():
                task.cancel()
        if tasks_to_stop:
            await _asyncio.gather(*tasks_to_stop, return_exceptions=True)
        if not has_candle_subs:
            _ws_ohlcv_tasks.pop(candle_key, None)
        if not has_pos_subs:
            _ws_position_tasks.pop(user_name, None)
        if not has_ord_subs:
            _ws_order_tasks.pop(user_name, None)


async def shutdown() -> None:
    """Stop dashboard polling and shared streams owned by the API process."""
    global _event_loop, _ohlcv_poller_started, _ohlcv_poller_thread, _stream_task_lock
    _ohlcv_poller_stop.set()
    poller_thread = _ohlcv_poller_thread
    if poller_thread is not None and poller_thread.is_alive():
        await _asyncio.to_thread(poller_thread.join)
    _ohlcv_poller_thread = None
    _ohlcv_poller_started = False

    lock = _get_stream_task_lock()
    async with lock:
        tasks = list({*list(_ws_ohlcv_tasks.values()), *list(_ws_position_tasks.values()), *list(_ws_order_tasks.values())})
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await _asyncio.gather(*tasks, return_exceptions=True)
        _ws_ohlcv_tasks.clear()
        _ws_position_tasks.clear()
        _ws_order_tasks.clear()
    with _candle_sub_lock:
        _candle_subscribers.clear()
    with _position_sub_lock:
        _position_subscribers.clear()
    with _order_sub_lock:
        _order_subscribers.clear()
    with _exchange_cache_lock:
        exchanges = list(_exchange_cache.values())
        _exchange_cache.clear()
        _exchange_cache_last_used.clear()
    if exchanges:
        await _asyncio.gather(
            *(_asyncio.to_thread(exchange.close) for exchange in exchanges),
            return_exceptions=True,
        )
    _event_loop = None
    _stream_task_lock = None


# --------------------------------------------------------------------------- helpers

def _get_db():
    from Database import Database
    return Database()


def _get_users():
    from User import Users
    u = Users()
    u.load()
    return u


# --------------------------------------------------------------------------- /balance

@router.get("/balance")
def get_balance(
    users: str = Query(default="ALL", description="Comma-separated user names, or ALL"),
    live: bool = Query(default=False),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Return balance data for the given users, including TWE and uPnl per user,
    plus overall totals. Used by the dashboard_balance.html component.
    """
    db = _get_db()
    all_users = _get_users()

    # Resolve user list
    if not users or users.strip().upper() == "ALL":
        users_selected = all_users.list()
    else:
        requested = [u.strip() for u in users.split(",") if u.strip()]
        users_selected = [u for u in requested if u in all_users.list()]

    if not users_selected:
        return {"rows": [], "totals": {"balance": 0, "upnl": 0, "we": 0}, "source": "db"}

    if live:
        rows = []
        total_balance = 0.0
        total_upnl = 0.0
        all_pprices = 0.0
        used_live = False
        used_db = False

        for user_name in users_selected:
            user_obj = all_users.find_user(user_name)
            if not user_obj:
                continue
            try:
                balance, upnl, pprices = _live_balance_for_user(user_obj, db)
                used_live = True
                date_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            except Exception as exc:
                used_db = True
                _log(SERVICE, f"Live balance fetch failed for '{user_name}', falling back to DB: {exc}", level="WARNING", user=user_name)
                db_rows = db.fetch_balances([user_name]) or []
                if not db_rows:
                    continue
                db_row = db_rows[-1]
                balance = _safe_float(db_row[2], 0.0)
                try:
                    date_str = datetime.utcfromtimestamp(db_row[1] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    date_str = str(db_row[1])
                positions = db.fetch_positions(user_obj) or []
                upnl = sum(_safe_float(pos[4], 0.0) for pos in positions)
                pprices = sum(_safe_float(pos[3], 0.0) * _safe_float(pos[5], 0.0) for pos in positions)
            total_balance += balance
            total_upnl += upnl
            all_pprices += pprices
            twe = (100 / balance * pprices) if balance and pprices else 0.0
            rows.append({
                "id": 0,
                "user": user_name,
                "date": date_str,
                "balance": round(balance, 2),
                "upnl": round(upnl, 2),
                "we": round(twe, 2),
            })

        total_twe = (100 / total_balance * all_pprices) if total_balance and all_pprices else 0.0
        source = "mixed" if used_live and used_db else ("live" if used_live else "db")
        return {
            "rows": rows,
            "totals": {
                "balance": round(total_balance, 2),
                "upnl": round(total_upnl, 2),
                "we": round(total_twe, 2),
            },
            "source": source,
        }

    balances = db.fetch_balances(users_selected)
    if not balances:
        return {"rows": [], "totals": {"balance": 0, "upnl": 0, "we": 0}, "source": "db"}

    rows = []
    total_balance = 0.0
    total_upnl = 0.0
    all_pprices = 0.0

    for row in balances:
        # row: (id, date_ms, balance, user)
        row_id, date_ms, balance, user_name = row[0], row[1], row[2], row[3]

        # format date
        try:
            dt = datetime.utcfromtimestamp(date_ms / 1000)
            date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            date_str = str(date_ms)

        # fetch positions for TWE / uPnl
        user_obj = all_users.find_user(user_name)
        positions = db.fetch_positions(user_obj) if user_obj else []

        upnl = 0.0
        pprices = 0.0
        for pos in (positions or []):
            pprices += pos[3] * pos[5]
            upnl += pos[4]

        all_pprices += pprices
        twe = (100 / balance * pprices) if balance and pprices else 0.0

        total_balance += balance
        total_upnl += upnl

        rows.append({
            "id": row_id,
            "user": user_name,
            "date": date_str,
            "balance": round(balance, 2),
            "upnl": round(upnl, 2),
            "we": round(twe, 2),
        })

    total_twe = (100 / total_balance * all_pprices) if total_balance and all_pprices else 0.0

    return {
        "rows": rows,
        "totals": {
            "balance": round(total_balance, 2),
            "upnl": round(total_upnl, 2),
            "we": round(total_twe, 2),
        },
        "source": "db",
    }


# ---------------------------------------------------------------- /balance/selection

@router.post("/balance/selection")
def set_balance_selection(
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Store user selection from JS dropdown so save() can read it."""
    position = payload.get("position", "")
    users = payload.get("users", ["ALL"])
    if position:
        _balance_user_selections[position] = users
    return {"status": "ok"}


@router.get("/balance/selection")
def get_balance_selection_endpoint(
    position: str = Query(description="Dashboard position key, e.g. 1_1"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return the stored user selection for a dashboard position."""
    users = _balance_user_selections.get(position)
    if users is None:
        return {"found": False, "users": []}
    return {"found": True, "users": users}


# ---------------------------------------------------------------- /grid/pending

@router.post("/grid/pending")
def set_grid_pending(
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """
    Store the pending grid config from the JS grid editor.
    Called on every change (debounced by the JS side).
    Payload: { name, rows, cols, dashboard_type_1_1, ... }
    """
    _grid_pending_config.clear()
    _grid_pending_config.update(payload)
    return {"status": "ok"}


@router.get("/grid/pending")
def get_grid_pending(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return the latest pending grid config from the JS editor."""
    if not _grid_pending_config:
        return {"found": False}
    return {"found": True, **_grid_pending_config}


# ---------------------------------------------------------------- /users

@router.get("/users")
def get_dashboard_users(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return list of available user names for cell config dropdowns."""
    try:
        users = _get_users()
        return {"users": users.list()}
    except Exception:
        return {"users": []}


# ---------------------------------------------------------------- /pending_full

@router.get("/pending_full")
def get_pending_full(
    name: str = Query(default="", description="Dashboard name (original)"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full pending dashboard config for a named dashboard."""
    cfg = _pending_full_configs.get(name)
    if cfg is None:
        return {"found": False, "config": {}}
    return {"found": True, "config": cfg}


@router.post("/pending_full")
def set_pending_full(
    payload: dict[str, Any],
    name: str = Query(default="", description="Dashboard name (original)"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Store the full pending dashboard config for a named dashboard."""
    _pending_full_configs[name] = payload
    return {"status": "ok"}


# ---------------------------------------------------------------- /editor_page

@router.get("/editor_page", response_class=HTMLResponse)
def get_editor_page(
    name: str = Query(default="", description="Dashboard name"),
    api_base: str = Query(default="", description="API base URL"),
    view_only: bool = Query(default=False, description="View-only mode (no editing controls)"),
    standalone: bool = Query(default=False, description="Standalone mode (save/cancel post to parent)"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the full dashboard grid editor HTML page."""
    import json as _json
    from pathlib import Path as _P
    html_path = _P(__file__).parent.parent / "frontend" / "dashboard_editor.html"
    html = html_path.read_text(encoding="utf-8")
    html = html.replace("%%TOKEN%%", session.token)
    html = html.replace("%%API_BASE%%", api_base)
    html = html.replace("%%DASHBOARD_NAME%%", _json.dumps(name))
    html = html.replace("%%VIEW_ONLY%%", "1" if view_only else "0")
    html = html.replace("%%STANDALONE%%", "1" if standalone else "0")
    html = html.replace("%%EDIT_ONLY_STYLE%%",
                         "display:none!important" if view_only else "")
    body_classes = []
    if view_only:
        body_classes.append("view-mode")
    if standalone:
        body_classes.append("standalone-mode")
    html = html.replace("%%BODY_CLASS%%", " ".join(body_classes))
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------------------- /main_page

@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    current: str = Query(default="", description="Currently selected dashboard name"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone dashboard main page (logo + sidebar + content area)."""
    import json as _json
    from pathlib import Path as _P

    html_path = _P(__file__).parent.parent / "frontend" / "dashboard_main.html"
    html = html_path.read_text(encoding="utf-8")

    # Derive API base from the actual request URL so iframes use the correct host/port
    scheme = request.url.scheme
    host   = request.url.hostname or "127.0.0.1"
    port   = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api"
    ws_base  = api_base.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"',         _json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"',      _json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"',       _json.dumps(ws_base))
    html = html.replace('"%%CURRENT%%"',       _json.dumps(current))

    from pbgui_purefunc import PBGDIR, PBGUI_SERIAL, PBGUI_VERSION

    try:
        dashboards_dir = _P(PBGDIR) / "data" / "dashboards"
        dashboards = sorted(path.stem for path in dashboards_dir.glob("*.json") if path.is_file())
    except Exception:
        dashboards = []
    html = html.replace("%%DASHBOARDS_JSON%%", _json.dumps(dashboards))

    html = html.replace('"%%VERSION%%"', _json.dumps(PBGUI_VERSION))
    html = html.replace('%%VERSION%%', PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', _json.dumps(PBGUI_SERIAL))
    html = html.replace('%%SERIAL%%', PBGUI_SERIAL)

    # Cache-bust pbgui_nav.js with file mtime so browser always loads latest
    nav_js = _P(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace('%%NAV_HASH%%', nav_hash)

    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------------------- /templates_page

@router.get("/templates_page", response_class=HTMLResponse)
def get_templates_page(
    current: str = Query(default="", description="Currently open dashboard name"),
    api_base: str = Query(default="", description="API base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the template manager popup page."""
    import json as _json
    from pathlib import Path as _P
    html_path = _P(__file__).parent.parent / "frontend" / "dashboard_templates.html"
    html = html_path.read_text(encoding="utf-8")
    html = html.replace('"%%TOKEN%%"', f'"{session.token}"')
    html = html.replace('"%%API_BASE%%"', f'"{api_base}"')
    html = html.replace('%%CURRENT%%', _json.dumps(current))
    return HTMLResponse(content=html)


# ---------------------------------------------------------------- period helper

def _period_to_range(period: str) -> tuple:
    """Return (start_ms, end_ms, from_date_str, to_date_str) for a period key."""
    from datetime import date, timedelta
    from dateutil.relativedelta import relativedelta, MO
    import time

    today = date.today()
    now_ms = int(time.time()) * 1000

    def ts(d: date) -> int:
        return int(time.mktime(d.timetuple())) * 1000

    p = period.upper()
    if p == 'TODAY':
        start, end = ts(today), now_ms
    elif p == 'YESTERDAY':
        y = today - timedelta(days=1)
        start, end = ts(y), ts(today)
    elif p == 'THIS_WEEK':
        mon = today + relativedelta(weekday=MO(-1))
        start, end = ts(mon), now_ms
    elif p == 'LAST_WEEK':
        mon = today + relativedelta(weekday=MO(-1))
        start, end = ts(mon - timedelta(days=7)), ts(mon)
    elif p == 'LAST_WEEK_NOW':
        mon = today + relativedelta(weekday=MO(-1))
        start, end = ts(mon - timedelta(days=7)), now_ms
    elif p == 'THIS_MONTH':
        m = today + relativedelta(day=1)
        start, end = ts(m), now_ms
    elif p == 'LAST_MONTH':
        m = today + relativedelta(day=1)
        start, end = ts(m - relativedelta(months=1)), ts(m)
    elif p == 'LAST_MONTH_NOW':
        m = today + relativedelta(day=1)
        start, end = ts(m - relativedelta(months=1)), now_ms
    elif p == 'THIS_QUARTER':
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        start, end = ts(q_start), now_ms
    elif p == 'LAST_QUARTER':
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        lq_month = q_month - 3
        if lq_month < 1:
            lq_start = date(today.year - 1, lq_month + 12, 1)
        else:
            lq_start = date(today.year, lq_month, 1)
        start, end = ts(lq_start), ts(q_start)
    elif p == 'LAST_QUARTER_NOW':
        q_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_month, 1)
        lq_month = q_month - 3
        if lq_month < 1:
            lq_start = date(today.year - 1, lq_month + 12, 1)
        else:
            lq_start = date(today.year, lq_month, 1)
        start, end = ts(lq_start), now_ms
    elif p == 'THIS_YEAR':
        y = date(today.year, 1, 1)
        start, end = ts(y), now_ms
    elif p == 'LAST_YEAR':
        start, end = ts(date(today.year - 1, 1, 1)), ts(date(today.year, 1, 1))
    elif p == 'LAST_YEAR_NOW':
        start, end = ts(date(today.year - 1, 1, 1)), now_ms
    elif p == 'LAST_7_DAYS':
        start, end = ts(today - timedelta(days=7)), now_ms
    elif p == 'LAST_30_DAYS':
        start, end = ts(today - timedelta(days=30)), now_ms
    elif p == 'LAST_90_DAYS':
        start, end = ts(today - timedelta(days=90)), now_ms
    elif p == 'LAST_180_DAYS':
        start, end = ts(today - timedelta(days=180)), now_ms
    elif p == 'LAST_365_DAYS':
        start, end = ts(today - timedelta(days=365)), now_ms
    elif p == 'ALL_TIME':
        start, end = 0, now_ms
    elif p.startswith('CUSTOM:'):
        parts = p.split(':')
        try:
            from datetime import datetime as _dt
            from_d = _dt.strptime(parts[1], '%Y-%m-%d').date()
            if len(parts) > 2 and parts[2] not in ('', 'NOW'):
                to_d = _dt.strptime(parts[2], '%Y-%m-%d').date() + timedelta(days=1)
                end  = ts(to_d)
            else:
                end  = now_ms  # 'NOW' or empty → always today
            start = ts(from_d)
        except Exception:
            m = today + relativedelta(day=1)
            start, end = ts(m), now_ms
    else:  # default THIS_MONTH
        m = today + relativedelta(day=1)
        start, end = ts(m), now_ms

    from datetime import datetime
    from_str = datetime.fromtimestamp(start / 1000).strftime('%Y-%m-%d') if start > 0 else ''
    to_str   = datetime.fromtimestamp(end   / 1000).strftime('%Y-%m-%d')
    return start, end, from_str, to_str


# ---------------------------------------------------------------- /top_data

@router.get("/top_data")
def get_top_data(
    users:  str = Query(default="ALL"),
    period: str = Query(default="THIS_MONTH"),
    top:    int = Query(default=10, ge=1, le=500),
    session: SessionToken = Depends(require_auth),
):
    """Return top-symbols income data as JSON for the dashboard_top.html component."""
    start_ms, end_ms, from_date, to_date = _period_to_range(period)
    user_list = [u.strip() for u in users.split(",") if u.strip()] or ["ALL"]
    db = _get_db()
    rows = db.select_top(user_list, start_ms, end_ms, top) or []
    return {
        "rows":      [list(r) for r in rows],
        "from_date": from_date,
        "to_date":   to_date,
    }


# ---------------------------------------------------------------- /income_data

@router.get("/income_data")
def get_income_data(
    users:     str   = Query(default="ALL"),
    period:    str   = Query(default="THIS_MONTH"),
    last_n:    int   = Query(default=0, ge=0),
    filter_val: float = Query(default=0.0, ge=0.0, alias="filter"),
    session: SessionToken = Depends(require_auth),
):
    """Return income data for the INCOME dashboard widget.

    When *last_n* > 0  → table mode  (latest N rows, sorted desc, with ids).
    When *last_n* == 0 → chart mode  (cumsum, per-symbol traces).
    """
    start_ms, end_ms, from_date, to_date = _period_to_range(period)
    user_list = [u.strip() for u in users.split(",") if u.strip()] or ["ALL"]
    db = _get_db()

    raw = db.select_income_by_symbol_with_id(user_list, start_ms, end_ms) or []
    import pandas as pd

    df = pd.DataFrame(raw, columns=["id", "date_ms", "symbol", "income", "user"])
    if df.empty:
        return {
            "mode":      "table" if last_n > 0 else "chart",
            "rows":      [],
            "traces":    [],
            "from_date": from_date,
            "to_date":   to_date,
        }

    if last_n > 0:
        # ----- table mode -----
        if filter_val > 0:
            df = df[(df["income"] >= filter_val) | (df["income"] <= -filter_val)]
        df = df.tail(last_n)
        df = df.sort_values("date_ms", ascending=False)
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "id":      int(r["id"]),
                "date_ms": int(r["date_ms"]),
                "date":    datetime.utcfromtimestamp(int(r["date_ms"]) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "symbol":  r["symbol"],
                "income":  round(float(r["income"]), 2),
                "user":    r["user"],
            })
        return {
            "mode":      "table",
            "rows":      rows,
            "from_date": from_date,
            "to_date":   to_date,
        }
    else:
        # ----- chart mode -----
        df = df.sort_values("date_ms")
        # Total cumsum trace
        total = df[["date_ms", "income"]].copy()
        total["cum"] = total["income"].cumsum()
        total_trace = {
            "name": "Total Income",
            "x": [datetime.utcfromtimestamp(t / 1000).strftime("%Y-%m-%d %H:%M:%S") for t in total["date_ms"]],
            "y": [round(float(v), 2) for v in total["cum"]],
        }
        # Per-symbol traces
        symbols = list(df["symbol"].unique())
        sym_traces = []
        for sym in sorted(symbols):
            sdf = df[df["symbol"] == sym].copy()
            sdf["cum"] = sdf["income"].cumsum()
            sym_traces.append({
                "name": sym,
                "x": [datetime.utcfromtimestamp(t / 1000).strftime("%Y-%m-%d %H:%M:%S") for t in sdf["date_ms"]],
                "y": [round(float(v), 2) for v in sdf["cum"]],
            })
        return {
            "mode":      "chart",
            "rows":      [],
            "traces":    [total_trace] + sym_traces,
            "from_date": from_date,
            "to_date":   to_date,
        }


# ---------------------------------------------------------------- /pnl_data

@router.get("/adg_data")
def get_adg_data(
    users:  str = Query(default="ALL"),
    period: str = Query(default="THIS_MONTH"),
    mode:   str = Query(default="bar"),
    session: SessionToken = Depends(require_auth),
):
    """Return ADG (Average Daily Growth %) data.

    Computes daily PnL, then converts each day's income to a percentage
    of the running balance (working backwards from current balance).
    Missing dates are filled with 0%.
    """
    start_ms, end_ms, from_date, to_date = _period_to_range(period)
    user_list = [u.strip() for u in users.split(",") if u.strip()] or ["ALL"]
    db = _get_db()
    all_users = _get_users()

    rows = db.select_pnl(user_list, start_ms, end_ms) or []

    # Resolve users for balance lookup
    if "ALL" in user_list:
        users_selected = all_users.list()
    else:
        users_selected = [u for u in user_list if u in all_users.list()]

    balances = db.fetch_balances(users_selected) if users_selected else []
    if not balances:
        return {
            "mode": mode, "bars": [],
            "from_date": from_date, "to_date": to_date,
            "starting_balance": 0, "total_pnl": 0, "current_balance": 0,
        }

    current_balance = sum(b[2] for b in balances)
    total_pnl = sum(float(r[1]) for r in rows if r[1] is not None)
    starting_balance = current_balance - total_pnl

    # Convert daily income to ADG% (working backwards from current balance)
    adg_rows = []
    for r in rows:
        adg_rows.append([r[0], float(r[1])])

    running = current_balance
    for i in reversed(range(len(adg_rows))):
        income = adg_rows[i][1]
        running -= income
        adg_pct = 100.0 * (income / running) if running != 0 else 0.0
        adg_rows[i][1] = round(adg_pct, 4)

    # Fill missing dates with 0
    from datetime import datetime as _dt, timedelta as _td
    if adg_rows:
        filled = []
        date_map = {r[0]: r[1] for r in adg_rows}
        d_start = _dt.strptime(adg_rows[0][0], "%Y-%m-%d").date()
        d_end = _dt.strptime(adg_rows[-1][0], "%Y-%m-%d").date()
        d = d_start
        while d <= d_end:
            ds = d.strftime("%Y-%m-%d")
            filled.append({"date": ds, "adg": date_map.get(ds, 0.0)})
            d += _td(days=1)
    else:
        filled = []

    return {
        "mode":             mode,
        "bars":             filled,
        "from_date":        from_date,
        "to_date":          to_date,
        "starting_balance": round(starting_balance, 2),
        "total_pnl":        round(total_pnl, 2),
        "current_balance":  round(current_balance, 2),
    }


# ---------------------------------------------------------------- /pnl_data

@router.get("/pnl_data")
def get_pnl_data(
    users:  str = Query(default="ALL"),
    period: str = Query(default="THIS_MONTH"),
    mode:   str = Query(default="Daily"),
    session: SessionToken = Depends(require_auth),
):
    """Return daily PNL data for the PNL dashboard widget.

    mode='Daily'      → bar chart with daily sums.
    mode='Cumulative' → line chart with running cumulative sum.
    """
    start_ms, end_ms, from_date, to_date = _period_to_range(period)
    user_list = [u.strip() for u in users.split(",") if u.strip()] or ["ALL"]
    db = _get_db()
    rows = db.select_pnl(user_list, start_ms, end_ms) or []

    bars = []
    cum = 0.0
    for r in rows:
        date_str = r[0]
        income = round(float(r[1]), 2)
        cum += income
        bars.append({
            "date":   date_str,
            "income": income,
            "cum":    round(cum, 2),
        })

    return {
        "mode":      mode,
        "bars":      bars,
        "from_date": from_date,
        "to_date":   to_date,
    }


# ---------------------------------------------------------------- /ppl_data

@router.get("/ppl_data")
def get_ppl_data(
    users:      str = Query(default="ALL"),
    period:     str = Query(default="THIS_MONTH"),
    sum_period: str = Query(default="MONTH"),
    session: SessionToken = Depends(require_auth),
):
    """Return Profits and Losses data grouped by sum_period."""
    start_ms, end_ms, from_date, to_date = _period_to_range(period)
    user_list = [u.strip() for u in users.split(",") if u.strip()] or ["ALL"]
    db = _get_db()
    rows = db.select_ppl(user_list, start_ms, end_ms, sum_period) or []

    bars = []
    for r in rows:
        period_label = r[0]
        profits = round(float(r[1]), 2) if r[1] else 0.0
        losses  = round(float(r[2]), 2) if r[2] else 0.0
        # ensure losses are negative
        if losses > 0:
            losses = -losses
        bars.append({
            "period":  period_label,
            "profits": profits,
            "losses":  losses,
        })

    return {
        "bars":       bars,
        "sum_period": sum_period,
        "from_date":  from_date,
        "to_date":    to_date,
    }


# ---------------------------------------------------------------- /income/delete_ids

class IncomeDeleteIds(BaseModel):
    ids: List[int]

@router.post("/income/delete_ids")
def delete_income_by_ids(
    payload: IncomeDeleteIds,
    session: SessionToken = Depends(require_auth),
):
    """Delete specific income rows by primary key, with automatic DB backup."""
    if not payload.ids:
        return {"deleted": 0}
    db = _get_db()
    # Pause PBData, backup, delete, restart
    was_running = _pbdata_stop()
    backup = db.backup_full_db()
    deleted = db.delete_income_by_ids(payload.ids)
    if was_running:
        _pbdata_start()
    return {"deleted": deleted, "backup": backup or ""}


# ---------------------------------------------------------------- /income/delete_older

class IncomeDeleteOlder(BaseModel):
    users: List[str]
    cutoff_ms: int

@router.post("/income/delete_older")
def delete_income_older(
    payload: IncomeDeleteOlder,
    session: SessionToken = Depends(require_auth),
):
    """Delete income entries older-than-or-equal to cutoff for given users."""
    db = _get_db()
    was_running = _pbdata_stop()
    backup = db.backup_full_db()
    deleted = db.delete_income_older_than(payload.users, payload.cutoff_ms)
    if was_running:
        _pbdata_start()
    return {"deleted": deleted, "backup": backup or ""}


# ---------------------------------------------------------------- /income/backups

@router.get("/income/backups")
def get_income_backups(
    session: SessionToken = Depends(require_auth),
):
    """Return the latest 10 DB backups for the restore picker."""
    from pathlib import Path as _P
    from pbgui_purefunc import PBGDIR
    backups_dir = _P(f"{PBGDIR}/data/backup/db")
    result = []
    if backups_dir.exists():
        files = sorted(
            [p for p in backups_dir.glob("pbgui-*.db") if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:10]
        for p in files:
            ts = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            result.append({"name": p.name, "path": str(p), "date": ts})
    return {"backups": result}


# ---------------------------------------------------------------- /income/restore

class IncomeRestore(BaseModel):
    path: str

@router.post("/income/restore")
def restore_income_backup(
    payload: IncomeRestore,
    session: SessionToken = Depends(require_auth),
):
    """Restore the DB from a backup file."""
    from pbgui_purefunc import PBGDIR
    backup_dir = Path(f"{PBGDIR}/data/backup/db").resolve()
    try:
        restore_path = Path(payload.path).resolve()
    except (ValueError, OSError):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not restore_path.is_relative_to(backup_dir):
        raise HTTPException(status_code=400, detail="Path outside backup directory")
    db = _get_db()
    was_running = _pbdata_stop()
    ok = db.restore_db_from(str(restore_path))
    if was_running:
        _pbdata_start()
    return {"ok": ok}


# ---------------------------------------------------------------- PBData helpers

def _pbdata_stop() -> bool:
    """Stop PBData if running. Returns True if it was running."""
    try:
        from PBData import PBData
        pb = PBData()
        if pb.is_running():
            pb.stop()
            return True
    except Exception:
        pass
    return False


def _pbdata_start():
    """Start PBData."""
    try:
        from PBData import PBData
        pb = PBData()
        pb.run()
    except Exception:
        pass


# ---------------------------------------------------------------- /positions_data

@router.get("/positions_data")
def get_positions_data(
    users:   str = Query(default="ALL"),
    live:    bool = Query(default=False),
    session: SessionToken = Depends(require_auth),
):
    """Return enriched positions data (with prices, DCA count, next DCA/TP)."""
    db = _get_db()
    all_users = _get_users()

    if not users or users.strip().upper() == "ALL":
        users_selected = all_users.list()
    else:
        requested = [u.strip() for u in users.split(",") if u.strip()]
        users_selected = [u for u in requested if u in all_users.list()]

    if not users_selected:
        return {"positions": []}

    all_positions = []
    used_live = False
    used_db = False
    for user_name in users_selected:
        user_obj = all_users.find_user(user_name)
        if not user_obj:
            continue
        if live:
            try:
                live_positions = _live_positions_for_user(user_obj, db)
                all_positions.extend(live_positions)
                used_live = True
                continue
            except Exception as exc:
                used_db = True
                _log(SERVICE, f"Live positions fetch failed for '{user_name}', falling back to DB: {exc}", level="WARNING", user=user_name)
        positions = db.fetch_positions(user_obj) or []
        prices = db.fetch_prices(user_obj) or []
        used_db = True
        for pos in positions:
            symbol = pos[1]
            uname = pos[6]
            side = pos[7] if len(pos) > 7 else "long"
            orders = db.fetch_orders_by_symbol(uname, symbol) or []
            dca, next_dca, next_tp = _classify_position_orders(orders, side)
            price = 0.0
            for p in prices:
                if p[1] == symbol:
                    price = p[3]
            pos_value = pos[3] * price
            all_positions.append({
                "user":     uname,
                "exchange": user_obj.exchange,
                "symbol":   symbol,
                "side":     side,
                "size":     pos[3],
                "upnl":     round(pos[4], 8),
                "entry":    pos[5],
                "price":    price,
                "dca":      dca,
                "next_dca": next_dca,
                "next_tp":  next_tp,
                "pos_value": round(pos_value, 2),
            })

    all_positions.sort(key=lambda x: (x["user"], x["symbol"]))
    if used_live and used_db:
        source = "mixed"
    elif used_live:
        source = "live"
    else:
        source = "db"
    return {"positions": all_positions, "source": source}


def _execute_market_close(payload: PositionManagePayload) -> dict[str, Any]:
    """Execute a synchronous reduce-only market close order."""
    all_users = _get_users()
    user_obj = all_users.find_user(payload.user)
    if not user_obj:
        raise HTTPException(status_code=404, detail=f"User '{payload.user}' not found")
    db = _get_db()
    positions = []
    live_position_price = 0.0
    live_lookup_failed = False
    try:
        live_positions = _live_positions_for_user(user_obj, db)
        positions = [_dashboard_row_from_live_position(row) for row in live_positions]
        for row in live_positions:
            if row.get("symbol") == payload.symbol and str(row.get("side") or "long").lower() == str(payload.side or "long").lower():
                live_position_price = _safe_float(row.get("price"), 0.0)
                break
    except Exception as exc:
        live_lookup_failed = True
        _log(SERVICE, f"Live position lookup failed for market close '{payload.user}', falling back to DB: {exc}", level="WARNING", user=payload.user)
    if live_lookup_failed:
        positions = db.fetch_positions(user_obj) or []
    pos = _find_dashboard_position(positions, payload.symbol, payload.side)
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")
    close_amount = _resolve_close_amount(float(pos[3]), payload.amount, payload.percent)
    close_side = _position_close_order_side(payload.side)
    exchange = _get_exchange(user_obj)
    if not getattr(exchange, "instance", None):
        raise HTTPException(status_code=500, detail="Exchange is not connected")
    symbol_ccxt = _symbol_to_ccxt(payload.symbol)
    order_amount = _precision_amount(exchange.instance, symbol_ccxt, close_amount)
    if order_amount <= 0.0:
        raise HTTPException(status_code=400, detail="Amount is below exchange precision")
    order_price = _market_close_price_arg(
        exchange.instance,
        user_obj.exchange,
        symbol_ccxt,
        close_side,
        live_position_price or _dashboard_price_from_rows(db.fetch_prices(user_obj) or [], payload.symbol),
    )
    _validate_market_close_min_cost(
        user_obj.exchange,
        order_amount,
        order_price,
        _market_close_min_cost(exchange.instance, user_obj.exchange, symbol_ccxt),
    )
    hedged_symbol = _has_hedged_symbol_positions(positions, payload.symbol)
    param_candidates = _market_close_param_candidates(user_obj.exchange, payload.side, hedged_symbol)
    order = None
    last_exc: Exception | None = None
    for idx, params in enumerate(param_candidates):
        params = _apply_market_close_user_params(params, user_obj.exchange, user_obj)
        try:
            order = exchange.instance.create_order(
                symbol_ccxt,
                "market",
                close_side,
                order_amount,
                order_price,
                params,
            )
            break
        except HTTPException:
            raise
        except Exception as exc:
            last_exc = exc
            if idx + 1 < len(param_candidates) and _is_position_mode_mismatch(exc):
                _log(
                    SERVICE,
                    f"Market close retry with alternate position mode params for {payload.user}/{payload.symbol}/{payload.side}: {exc}",
                    level="WARNING",
                )
                continue
            _log(SERVICE, f"Market close failed for {payload.user}/{payload.symbol}/{payload.side}: {exc}", level="ERROR")
            if _is_amount_precision_error(exc):
                raise HTTPException(status_code=400, detail=f"Market close failed: {exc}") from exc
            raise HTTPException(status_code=502, detail=f"Market close failed: {exc}") from exc
    if order is None:
        detail = f"Market close failed: {last_exc}" if last_exc else "Market close failed"
        raise HTTPException(status_code=502, detail=detail)
    _log(
        SERVICE,
        f"Market close sent for {payload.user}/{payload.symbol}/{payload.side}: {order_amount} {close_side}",
        level="WARNING",
    )
    return {
        "ok": True,
        "action": "market_close",
        "user": payload.user,
        "symbol": payload.symbol,
        "side": payload.side,
        "amount": order_amount,
        "order": order,
    }


@router.get("/positions/close_price")
def get_position_close_price(
    user: str = Query(...),
    symbol: str = Query(...),
    side: str = Query(default="long"),
    session: SessionToken = Depends(require_auth),
):
    """Return fresh price metadata used by direct dashboard market closes."""
    user_obj = _get_users().find_user(user)
    if not user_obj:
        raise HTTPException(status_code=404, detail=f"User '{user}' not found")
    snapshot = _market_close_price_snapshot(user_obj, symbol, side)
    return {"ok": True, "user": user, "symbol": symbol, "side": side, **snapshot}


@router.post("/positions/manage")
async def manage_position(
    payload: PositionManagePayload,
    session: SessionToken = Depends(require_auth),
):
    """Manage a dashboard position with market close or PB7 forced-mode config sync."""
    action = str(payload.action or "").strip().lower()
    if action == "market_close":
        return await _asyncio.to_thread(_execute_market_close, payload)

    if action not in {"panic_symbol", "panic_all", "graceful_stop_symbol", "graceful_stop_all", "tp_only_symbol", "tp_only_all"}:
        raise HTTPException(status_code=400, detail="Unsupported action")

    name, config_path, cfg = await _asyncio.to_thread(_find_instance_config_for_user, payload.user)
    working_cfg = deepcopy(cfg) if payload.dry_run else cfg
    if action in {"panic_symbol", "graceful_stop_symbol", "tp_only_symbol"}:
        # Require the selected row to still exist before changing a per-symbol mode.
        all_users = _get_users()
        user_obj = all_users.find_user(payload.user)
        if not user_obj:
            raise HTTPException(status_code=404, detail=f"User '{payload.user}' not found")
        pos = _find_dashboard_position(_get_db().fetch_positions(user_obj) or [], payload.symbol, payload.side)
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found")
        if action == "panic_symbol":
            coin = _apply_panic_symbol(working_cfg, payload.symbol, payload.side)
        elif action == "graceful_stop_symbol":
            coin = _apply_graceful_stop_symbol(working_cfg, payload.symbol, payload.side)
        else:
            coin = _apply_tp_only_symbol(working_cfg, payload.symbol, payload.side)
    else:
        if action == "panic_all":
            _apply_panic_all(working_cfg)
        elif action == "graceful_stop_all":
            _apply_graceful_stop_all(working_cfg)
        else:
            _apply_tp_only_all(working_cfg)
        coin = ""

    if payload.dry_run:
        preview_cfg = working_cfg
        preview_cfg.setdefault("pbgui", {})["version"] = preview_cfg["pbgui"].get("version", 0) + 1
        return {
            "ok": True,
            "dry_run": True,
            "action": action,
            "user": payload.user,
            "symbol": payload.symbol,
            "side": payload.side,
            "coin": coin,
            "name": name,
            "version": preview_cfg["pbgui"]["version"],
            "config": preview_cfg,
        }

    result = await _save_dashboard_panic_config(name, config_path, working_cfg)
    _log(
        SERVICE,
        f"Dashboard {action} saved for user={payload.user} symbol={payload.symbol} side={payload.side} instance={name}",
        level="WARNING",
    )
    return {
        "ok": True,
        "action": action,
        "user": payload.user,
        "symbol": payload.symbol,
        "side": payload.side,
        "coin": coin,
        **result,
    }


# ---------------------------------------------------------------- /orders_data

@router.get("/orders_data")
def get_orders_data(
    user:      str = Query(...),
    symbol:    str = Query(...),
    side:      str = Query(default="long"),
    timeframe: str = Query(default="4h"),
    since:     int = Query(default=None),
    limit:     int = Query(default=500),
    live:      bool = Query(default=True),
    session:   SessionToken = Depends(require_auth),
):
    """Return OHLCV candles + orders + position for a symbol/user.

    Uses the OHLCV cache for fast responses.  If cache is empty, fetches
    from the exchange (first call per symbol/tf).  Subsequent calls served
    from cache while the background poller keeps it fresh.
    """
    _start_ohlcv_poller()
    db = _get_db()
    all_users = _get_users()
    user_obj = all_users.find_user(user)

    if not user_obj:
        return {"candles": [], "orders": [], "position": None,
                "current_price": 0, "user": user, "symbol": symbol}

    # Try cache first (no `since` = latest candles)
    if since is None:
        cached = _ohlcv_cache_get(user, symbol, timeframe, limit)
        if cached:
            candles = [{"t": c[0], "o": c[1], "h": c[2],
                        "l": c[3], "c": c[4], "v": c[5]} for c in cached]
        else:
            # Cache miss — fetch from exchange and populate cache
            exchange = _get_exchange(user_obj)
            symbol_ccxt = _symbol_to_ccxt(symbol)
            try:
                ohlcv = exchange.fetch_ohlcv(
                    symbol_ccxt, "futures",
                    timeframe=timeframe, limit=limit
                )
            except Exception:
                ohlcv = []
            if ohlcv:
                _ohlcv_cache_put(user, symbol, timeframe, ohlcv)
            candles = [{"t": c[0], "o": c[1], "h": c[2],
                        "l": c[3], "c": c[4], "v": c[5]} for c in (ohlcv or [])]
    else:
        # Historical navigation — always fetch from exchange
        exchange = _get_exchange(user_obj)
        symbol_ccxt = _symbol_to_ccxt(symbol)
        try:
            ohlcv = exchange.fetch_ohlcv(
                symbol_ccxt, "futures",
                timeframe=timeframe, limit=limit, since=since
            )
        except Exception:
            ohlcv = []
        candles = [{"t": c[0], "o": c[1], "h": c[2],
                    "l": c[3], "c": c[4], "v": c[5]} for c in (ohlcv or [])]

    normalized_side = str(side or "long").lower()
    positions = []
    live_positions = []
    if live:
        try:
            live_positions = _live_positions_for_user(user_obj, db)
            positions = [_dashboard_row_from_live_position(row) for row in live_positions]
        except Exception as exc:
            _log(SERVICE, f"Live position lookup failed for orders chart '{user}/{symbol}', falling back to DB: {exc}", level="WARNING", user=user)
    if not positions:
        positions = db.fetch_positions(user_obj) or []
    hedged_symbol = _has_hedged_symbol_positions(positions, symbol)

    orders_list, orders_unknown, orders_source = _dashboard_orders_for_position(
        user_obj, db, symbol, normalized_side, live=live and not hedged_symbol
    )

    prices = db.fetch_prices(user_obj) or []
    current_price = 0.0
    for row in live_positions:
        if row.get("symbol") == symbol and str(row.get("side") or "long").lower() == normalized_side:
            current_price = _safe_float(row.get("price"), 0.0)
            break
    for p in prices:
        if current_price <= 0.0 and p[1] == symbol:
            current_price = p[3]

    position_data = None
    for live_pos in live_positions:
        if live_pos.get("symbol") == symbol and str(live_pos.get("side") or "long").lower() == normalized_side:
            position_data = {
                "entry": live_pos.get("entry", 0),
                "size":  live_pos.get("size", 0),
                "upnl":  live_pos.get("upnl", 0),
                "side":  live_pos.get("side", "long"),
            }
            break
    for pos in positions:
        if position_data is None and pos[1] == symbol and str(pos[7] if len(pos) > 7 else "long").lower() == normalized_side:
            position_data = {
                "entry": pos[5], "size": pos[3],
                "upnl":  pos[4], "side": pos[7] if len(pos) > 7 else "long",
            }
            break

    return {
        "candles":       candles,
        "orders":        orders_list,
        "position":      position_data,
        "current_price": current_price,
        "user":          user,
        "symbol":        symbol,
        "orders_unknown": bool(hedged_symbol or orders_unknown),
        "orders_source":  orders_source,
        "timeframe":     timeframe,
    }
