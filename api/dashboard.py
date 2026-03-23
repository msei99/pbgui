"""
FastAPI router: Dashboard data endpoints.

Provides data for Vanilla JS dashboard components (balance, positions, …).
All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import asyncio as _asyncio
import threading
import time as _time
from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.auth import SessionToken, require_auth

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


def _get_exchange(user_obj):
    """Return a cached, connected Exchange instance for the given user object."""
    from Exchange import Exchange
    key = f"{user_obj.name}:{user_obj.exchange}"
    if key not in _exchange_cache:
        ex = Exchange(user_obj.exchange, user_obj)
        ex.connect()
        _exchange_cache[key] = ex
    return _exchange_cache[key]


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
    while True:
        _time.sleep(_OHLCV_POLL_INTERVAL)
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


def _start_ohlcv_poller():
    """Start the background poller thread (once)."""
    global _ohlcv_poller_started
    if _ohlcv_poller_started:
        return
    _ohlcv_poller_started = True
    t = threading.Thread(target=_ohlcv_poll_loop, daemon=True)
    t.start()


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

# Position subscribers: key = (user_name, symbol)
_position_subscribers: dict[tuple[str, str], set] = {}
_position_sub_lock = threading.Lock()

# Order subscribers: key = (user_name, symbol)
_order_subscribers: dict[tuple[str, str], set] = {}
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


def _notify_position_update(user_name: str, symbol: str, position_data: dict | None,
                            from_thread: bool = False):
    """Push a position update to subscribers watching (user, symbol)."""
    msg = {"type": "position", "position": position_data}
    key = (user_name, symbol)
    with _position_sub_lock:
        subs = _position_subscribers.get(key)
        if not subs:
            return
        _push_to_subs(subs, msg, from_thread=from_thread)


def _notify_order_update(user_name: str, symbol: str, orders_data: list,
                         from_thread: bool = False):
    """Push an orders update to subscribers watching (user, symbol)."""
    msg = {"type": "orders", "orders": orders_data}
    key = (user_name, symbol)
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
        # Build a dict: symbol -> position_data (only open positions)
        open_pos: dict[str, dict] = {}
        for pos in positions:
            sym = pos[1]
            if pos[3]:  # size != 0 means open
                open_pos[sym] = {
                    "entry": pos[5],
                    "size":  pos[3],
                    "upnl":  pos[4],
                    "side":  pos[7] if len(pos) > 7 else "long",
                }
        # Notify all chart subscribers for this user
        with _position_sub_lock:
            sub_keys = [k for k in _position_subscribers if k[0] == user_name]
        for key in sub_keys:
            _, sym = key
            pos_data = open_pos.get(sym, None)  # None = position closed
            _notify_position_update(user_name, sym, pos_data, from_thread=True)
    except Exception:
        pass


# ── ccxt.pro live stream tasks (Phase 2) ───────────────────────────────────
_ws_ohlcv_tasks: dict[tuple[str, str, str], _asyncio.Task] = {}
_ws_position_tasks: dict[str, _asyncio.Task] = {}   # per user
_ws_order_tasks: dict[str, _asyncio.Task] = {}       # per user


async def _watch_ohlcv_stream(user_name: str, symbol: str, tf: str):
    """Async task: watches OHLCV via ccxt.pro and pushes to candle subscribers."""
    from Exchange import Exchange
    from logging_helpers import human_log as _log
    key = (user_name, symbol, tf)
    user_obj = _get_users().find_user(user_name)
    if not user_obj:
        return
    ex_id = "kucoinfutures" if user_obj.exchange == "kucoin" else user_obj.exchange
    try:
        ws_client = await Exchange.get_shared_ws_client(ex_id, user_obj)
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


async def _watch_positions_stream(user_name: str):
    """Async task: watches positions via ccxt.pro private stream."""
    from Exchange import Exchange
    from logging_helpers import human_log as _log
    user_obj = _get_users().find_user(user_name)
    if not user_obj:
        return
    ex_id = "kucoinfutures" if user_obj.exchange == "kucoin" else user_obj.exchange
    try:
        ws_client = await Exchange.get_private_ws_client(ex_id, user_obj,
                                                         caller="dashboard_positions")
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
                # Push matching position to each (user, symbol) subscriber
                with _position_sub_lock:
                    sub_keys = [k for k in _position_subscribers if k[0] == user_name]
                for key in sub_keys:
                    _, sym = key
                    sym_ccxt = _symbol_to_ccxt(sym)
                    pos_data = None
                    for p in positions:
                        if p.get("symbol") == sym_ccxt:
                            pos_data = {
                                "entry": p.get("entryPrice", 0),
                                "size": p.get("contracts", 0),
                                "upnl": p.get("unrealizedPnl", 0),
                                "side": p.get("side", "long"),
                            }
                            break
                    _notify_position_update(user_name, sym, pos_data)
            except _asyncio.CancelledError:
                raise
            except Exception as e:
                _log("Dashboard", f"watchPositions error {user_name}: {e}",
                     level="WARNING")
                await _asyncio.sleep(5)
    except _asyncio.CancelledError:
        _log("Dashboard", f"watchPositions stopped: {user_name}")


async def _watch_orders_stream(user_name: str):
    """Async task: watches open orders via ccxt.pro private stream."""
    from Exchange import Exchange
    from logging_helpers import human_log as _log
    user_obj = _get_users().find_user(user_name)
    if not user_obj:
        return
    ex_id = "kucoinfutures" if user_obj.exchange == "kucoin" else user_obj.exchange
    try:
        ws_client = await Exchange.get_private_ws_client(ex_id, user_obj,
                                                         caller="dashboard_orders")
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
                    by_symbol.setdefault(sym_ccxt, []).append({
                        "price": o.get("price", 0),
                        "amount": o.get("amount", 0),
                        "side": o.get("side", "buy"),
                    })
                with _order_sub_lock:
                    sub_keys = [k for k in _order_subscribers if k[0] == user_name]
                for key in sub_keys:
                    _, sym = key
                    sym_ccxt = _symbol_to_ccxt(sym)
                    sym_orders = by_symbol.get(sym_ccxt, [])
                    _notify_order_update(user_name, sym, sym_orders)
            except _asyncio.CancelledError:
                raise
            except Exception as e:
                _log("Dashboard", f"watchOrders error {user_name}: {e}",
                     level="WARNING")
                await _asyncio.sleep(5)
    except _asyncio.CancelledError:
        _log("Dashboard", f"watchOrders stopped: {user_name}")


async def register_chart_client(user_name: str, symbol: str, tf: str,
                                q: _asyncio.Queue):
    """Register a WS client queue for candle/position/order updates.

    Starts ccxt.pro streams if not already running for this key.
    Falls back to polling if ccxt.pro is unavailable.
    """
    from logging_helpers import human_log as _log
    candle_key = (user_name, symbol, tf)
    pos_key = (user_name, symbol)

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
                if pos[1] == symbol and pos[3]:  # size != 0
                    pos_data = {
                        "entry": pos[5], "size": pos[3],
                        "upnl":  pos[4], "side": pos[7] if len(pos) > 7 else "long",
                    }
                    break
            q.put_nowait({"type": "position", "position": pos_data})
    except Exception:
        pass

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


async def unregister_chart_client(user_name: str, symbol: str, tf: str,
                                  q: _asyncio.Queue):
    """Remove a WS client queue and stop streams if no subscribers left."""
    candle_key = (user_name, symbol, tf)
    pos_key = (user_name, symbol)

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

    # Stop OHLCV stream if no more candle subscribers for this key
    with _candle_sub_lock:
        has_candle_subs = bool(_candle_subscribers.get(candle_key))
    if not has_candle_subs:
        task = _ws_ohlcv_tasks.pop(candle_key, None)
        if task and not task.done():
            task.cancel()

    # Stop position stream if no more position subscribers for this user
    with _position_sub_lock:
        has_pos_subs = any(k[0] == user_name for k in _position_subscribers)
    if not has_pos_subs:
        task = _ws_position_tasks.pop(user_name, None)
        if task and not task.done():
            task.cancel()

    # Stop order stream if no more order subscribers for this user
    with _order_sub_lock:
        has_ord_subs = any(k[0] == user_name for k in _order_subscribers)
    if not has_ord_subs:
        task = _ws_order_tasks.pop(user_name, None)
        if task and not task.done():
            task.cancel()


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
        return {"rows": [], "totals": {"balance": 0, "upnl": 0, "we": 0}}

    balances = db.fetch_balances(users_selected)
    if not balances:
        return {"rows": [], "totals": {"balance": 0, "upnl": 0, "we": 0}}

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
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
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
    html = html.replace('"%%ST_BASE%%"',       _json.dumps(st_base))
    html = html.replace('"%%CURRENT%%"',       _json.dumps(current))

    from Dashboard import Dashboard as _Dashboard
    try:
        dashboards = sorted(_Dashboard().list_dashboards())
    except Exception:
        dashboards = []
    html = html.replace("%%DASHBOARDS_JSON%%", _json.dumps(dashboards))

    from pbgui_func import PBGUI_VERSION
    html = html.replace('"%%VERSION%%"', _json.dumps(PBGUI_VERSION))
    html = html.replace('%%VERSION%%', PBGUI_VERSION)

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
    from pbgui_func import PBGDIR
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
    from pbgui_func import PBGDIR
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
    for user_name in users_selected:
        user_obj = all_users.find_user(user_name)
        if not user_obj:
            continue
        positions = db.fetch_positions(user_obj) or []
        prices = db.fetch_prices(user_obj) or []
        for pos in positions:
            symbol = pos[1]
            uname = pos[6]
            orders = db.fetch_orders_by_symbol(uname, symbol) or []
            dca = 0
            next_tp = 0.0
            next_dca = 0.0
            for order in orders:
                if order[5] == "buy":
                    dca += 1
                    if next_dca < order[4]:
                        next_dca = order[4]
                elif order[5] == "sell":
                    if next_tp == 0 or next_tp > order[4]:
                        next_tp = order[4]
            price = 0.0
            for p in prices:
                if p[1] == symbol:
                    price = p[3]
            pos_value = pos[3] * price
            all_positions.append({
                "user":     uname,
                "symbol":   symbol,
                "side":     pos[7] if len(pos) > 7 else "long",
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
    return {"positions": all_positions}


# ---------------------------------------------------------------- /orders_data

@router.get("/orders_data")
def get_orders_data(
    user:      str = Query(...),
    symbol:    str = Query(...),
    timeframe: str = Query(default="4h"),
    since:     int = Query(default=None),
    limit:     int = Query(default=500),
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

    db_orders = db.fetch_orders_by_symbol(user, symbol) or []
    orders_list = []
    for o in sorted(db_orders, key=lambda x: x[4], reverse=True):
        orders_list.append({"price": o[4], "amount": o[3], "side": o[5]})

    prices = db.fetch_prices(user_obj) or []
    current_price = 0.0
    for p in prices:
        if p[1] == symbol:
            current_price = p[3]

    positions = db.fetch_positions(user_obj) or []
    position_data = None
    for pos in positions:
        if pos[1] == symbol:
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
        "timeframe":     timeframe,
    }
