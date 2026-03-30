"""Live-Session API — Layer 2 of PBGui's three-tier data architecture.

Layer 1 (background): REST poller writes positions/balances/history to SQLite every 60–300 s.
Layer 2 (on-demand):  This module.  When a browser opens the dashboard it subscribes to
                       /api/live/stream; FastAPI opens private ccxtpro WS connections per
                       user and streams live position/balance updates via SSE.  WS tasks
                       run only as long as at least one SSE subscriber is active.
Layer 3 (browser):    Vanilla JS applies SSE deltas on top of the DB snapshot.

Live session constraints:
  - MAX_LIVE_USERS (10) users per SSE stream.
  - WS tasks are shared and ref-counted: one watcher per (user, kind) across all sessions.
  - On SSE disconnect → unsubscribe → if last subscriber, cancel WS task immediately.
"""
from __future__ import annotations

import asyncio
import json
import traceback
from typing import Any

try:
    from ccxt.base.errors import NotSupported as _CcxtNotSupported
    _WS_UNSUPPORTED = (NotImplementedError, _CcxtNotSupported)
except ImportError:
    _WS_UNSUPPORTED = (NotImplementedError,)

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from api.auth import require_auth, SessionToken
from logging_helpers import human_log as _log

router = APIRouter()

MAX_LIVE_USERS = 10
_SERVICE = "LiveSession"

# ── Shared watcher state (process-wide, one entry per (kind, user_name)) ──────
# kind is 'positions' or 'balance'.
_watcher_tasks: dict[str, asyncio.Task] = {}   # "kind:user" → task
_watcher_subs:  dict[str, set]          = {}   # "kind:user" → set of asyncio.Queue
_watcher_lock:  asyncio.Lock | None     = None  # lazily created inside event loop


def _get_lock() -> asyncio.Lock:
    global _watcher_lock
    if _watcher_lock is None:
        _watcher_lock = asyncio.Lock()
    return _watcher_lock


def _wkey(user_name: str, kind: str) -> str:
    return f"{kind}:{user_name}"


def _broadcast(key: str, msg: str) -> None:
    """Push *msg* to every SSE subscriber watching *key* (non-blocking)."""
    for q in list(_watcher_subs.get(key, set())):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


# ── Watcher lifecycle ──────────────────────────────────────────────────────────

async def _ensure_watcher(user_name: str, kind: str, queue: asyncio.Queue) -> None:
    """Add *queue* as a subscriber for *kind* updates for *user_name*.

    Starts a background WS watcher task if none is running yet.
    """
    from User import Users  # lazy import to avoid circular deps at module load

    key = _wkey(user_name, kind)
    async with _get_lock():
        _watcher_subs.setdefault(key, set()).add(queue)
        task = _watcher_tasks.get(key)
        if task is None or task.done():
            users = Users()
            users.load()
            user = users.find_user(user_name)
            if user is None:
                _log(_SERVICE, f"[live] User {user_name!r} not found; no watcher started", level="WARNING")
                return
            _watcher_tasks[key] = asyncio.create_task(
                _watcher_loop(user, kind),
                name=f"live_{kind}_{user_name}",
            )


async def _unsubscribe(user_name: str, kind: str, queue: asyncio.Queue) -> None:
    """Remove *queue* from subscribers; stop watcher task if no subscribers remain."""
    key = _wkey(user_name, kind)
    async with _get_lock():
        subs = _watcher_subs.get(key)
        if subs:
            subs.discard(queue)
            if not subs:
                task = _watcher_tasks.pop(key, None)
                if task and not task.done():
                    task.cancel()
                _watcher_subs.pop(key, None)
                _log(_SERVICE, f"[live] Stopped {kind} watcher for {user_name!r} (no more subscribers)", level="INFO")


# ── DB polling fallback ───────────────────────────────────────────────────────

async def _db_poll_loop(user: Any, kind: str, db: Any) -> None:
    """Fallback for exchanges where ccxtpro watch*() is not supported.

    Polls the DB every 30 s and broadcasts the result so the SSE badge stays
    fresh (e.g. Hyperliquid — ccxtpro watchBalance/watchPositions not supported).
    """
    key = _wkey(user.name, kind)
    _log(_SERVICE, f"[live] {kind} DB-poll fallback active for {user.name} ({user.exchange})", level="INFO")
    while True:
        try:
            if kind == "balance":
                rows = await asyncio.to_thread(db.fetch_balances, [user.name]) or []
                if rows:
                    balance_val = rows[0][2]
                    snap = _normalize_balance(user.name, {
                        "USDT": {"total": balance_val},
                        "total": {"USDT": balance_val},
                    })
                    _broadcast(key, json.dumps({
                        "type": "balance_update",
                        "user": user.name,
                        "data": snap,
                    }))
            else:  # positions
                positions_rows = await asyncio.to_thread(db.fetch_positions, user) or []
                prices_rows    = await asyncio.to_thread(db.fetch_prices, user) or []
                price_map = {p[1]: p[3] for p in prices_rows}
                result = []
                for pos in positions_rows:
                    symbol = pos[1]
                    price  = price_map.get(symbol, 0.0)
                    orders = await asyncio.to_thread(db.fetch_orders_by_symbol, user.name, symbol) or []
                    dca = 0; next_tp = 0.0; next_dca = 0.0
                    for order in orders:
                        if order[5] == "buy":
                            dca += 1
                            if next_dca < order[4]: next_dca = order[4]
                        elif order[5] == "sell":
                            if next_tp == 0 or next_tp > order[4]: next_tp = order[4]
                    result.append({
                        "user": user.name, "symbol": symbol,
                        "side": pos[7] if len(pos) > 7 else "long",
                        "size": pos[3], "upnl": round(pos[4], 8),
                        "entry": pos[5], "price": price,
                        "dca": dca, "next_dca": next_dca, "next_tp": next_tp,
                        "pos_value": round(pos[3] * price, 2),
                    })
                result.sort(key=lambda x: x["symbol"])
                _broadcast(key, json.dumps({
                    "type": "position_update",
                    "user": user.name,
                    "data": {"positions": result},
                }))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log(_SERVICE, f"[live] DB poll error {kind} {user.name}: {exc}", level="WARNING")
        await asyncio.sleep(30)


# ── WS watcher loop ───────────────────────────────────────────────────────────

async def _watcher_loop(user: Any, kind: str) -> None:
    """Watch positions or balance for *user* via ccxtpro private WebSocket.

    Broadcasts normalized data to all SSE subscribers whenever new data arrives.
    Restarts automatically after transient errors; stops when cancelled.
    """
    from Exchange import Exchange
    from Database import Database

    key = _wkey(user.name, kind)
    _log(_SERVICE, f"[live] Starting {kind} watcher for {user.name} ({user.exchange})", level="INFO")
    db = Database()

    try:
        client = await Exchange.get_private_ws_client(
            user.exchange, user, caller=f"live_session.{kind}"
        )
        if client is None:
            _log(
                _SERVICE,
                f"[live] No WS client for {user.name} ({user.exchange}), kind={kind}. "
                "Exchange WS cap reached or exchange unsupported; live updates unavailable.",
                level="WARNING",
            )
            return

        # For balance: send DB snapshot immediately so the badge shows data
        # right away — watchBalance() only pushes on CHANGE, not on connect.
        if kind == "balance":
            try:
                rows = await asyncio.to_thread(db.fetch_balances, [user.name]) or []
                if rows:
                    _id, date_ms, balance_val, _user = rows[0][0], rows[0][1], rows[0][2], rows[0][3]
                    snap = _normalize_balance(user.name, {
                        "USDT": {"total": balance_val},
                        "total": {"USDT": balance_val},
                    })
                    _broadcast(key, json.dumps({
                        "type": "balance_update",
                        "user": user.name,
                        "data": snap,
                    }))
            except Exception:
                pass

        while True:
            try:
                if kind == "positions":
                    raw = await client.watchPositions()
                    positions = await asyncio.to_thread(_normalize_positions, user, raw, db)
                    msg = json.dumps({
                        "type": "position_update",
                        "user": user.name,
                        "data": {"positions": positions},
                    })
                else:  # balance
                    raw = await client.watchBalance()
                    row = _normalize_balance(user.name, raw)
                    msg = json.dumps({
                        "type": "balance_update",
                        "user": user.name,
                        "data": row,
                    })
                _broadcast(key, msg)

            except asyncio.CancelledError:
                raise
            except _WS_UNSUPPORTED:
                # ccxtpro does not support watch*() for this exchange;
                # switch to a DB polling loop so the badge stays fresh.
                _log(_SERVICE, f"[live] watch{kind.capitalize()} not supported for {user.exchange}; switching to 30s DB polling", level="INFO")
                await _db_poll_loop(user, kind, db)
                return
            except Exception as exc:
                _log(
                    _SERVICE,
                    f"[live] {kind} watch error for {user.name}: {exc}",
                    level="ERROR",
                    meta={"traceback": traceback.format_exc()},
                )
                await asyncio.sleep(5)

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        _log(
            _SERVICE,
            f"[live] {kind} watcher crashed for {user.name}: {exc}",
            level="ERROR",
            meta={"traceback": traceback.format_exc()},
        )
    finally:
        _log(_SERVICE, f"[live] {kind} watcher stopped for {user.name}", level="DEBUG")


# ── Data normalization ────────────────────────────────────────────────────────

def _normalize_positions(user: Any, raw: list, db: Any) -> list:
    """Map ccxtpro watchPositions() output to positions_data REST format."""
    if not raw:
        return []

    result = []

    # Enrich with prices + orders from DB (fast SQLite reads, ≤60 s stale — acceptable)
    try:
        prices_raw = db.fetch_prices(user) or []
        price_map = {p[1]: p[3] for p in prices_raw}
    except Exception:
        price_map = {}

    for pos in raw:
        try:
            contracts = float(pos.get("contracts") or pos.get("size") or 0)
            if contracts == 0:
                continue

            # Normalise symbol: "BTC/USDT:USDT" → "BTCUSDT"
            raw_sym = pos.get("symbol", "")
            if "/" in raw_sym:
                base  = raw_sym.split("/")[0]
                quote = raw_sym.split("/")[-1].split(":")[0]
                symbol = base + quote
            else:
                symbol = raw_sym

            side  = pos.get("side", "long")
            entry = float(pos.get("entryPrice") or 0)
            upnl  = float(pos.get("unrealizedPnl") or pos.get("unrealisedPnl") or 0)
            price = price_map.get(symbol, 0.0)
            pos_value = abs(contracts) * price if price else 0.0

            # Orders (dca/tp) — still from DB; refreshed every 60 s by background poller
            dca = 0
            next_tp  = 0.0
            next_dca = 0.0
            try:
                orders = db.fetch_orders_by_symbol(user.name, symbol) or []
                for order in orders:
                    if order[5] == "buy":
                        dca += 1
                        if next_dca < order[4]:
                            next_dca = order[4]
                    elif order[5] == "sell":
                        if next_tp == 0 or next_tp > order[4]:
                            next_tp = order[4]
            except Exception:
                pass

            result.append({
                "user":      user.name,
                "symbol":    symbol,
                "side":      side,
                "size":      contracts,
                "upnl":      round(upnl, 8),
                "entry":     entry,
                "price":     price,
                "dca":       dca,
                "next_dca":  next_dca,
                "next_tp":   next_tp,
                "pos_value": round(pos_value, 2),
            })
        except Exception:
            pass

    result.sort(key=lambda x: x["symbol"])
    return result


def _normalize_balance(user_name: str, raw: dict) -> dict:
    """Map ccxtpro watchBalance() dict to a balance row dict."""
    try:
        balance = 0.0
        # Try top-level currency keys first
        for currency in ("USDT", "USDC"):
            curr = raw.get(currency)
            if isinstance(curr, dict):
                total = float(curr.get("total") or 0)
                if total > 0:
                    balance = total
                    break
            elif isinstance(curr, (int, float)) and curr > 0:
                balance = float(curr)
                break
        # Fallback: nested 'total' dict
        if balance == 0:
            total_dict = raw.get("total") or {}
            if isinstance(total_dict, dict):
                for currency in ("USDT", "USDC"):
                    v = float(total_dict.get(currency) or 0)
                    if v > 0:
                        balance = v
                        break
    except Exception:
        balance = 0.0

    return {"user": user_name, "balance": round(balance, 2), "upnl": 0.0, "we": 0.0}


# ── SSE endpoint ──────────────────────────────────────────────────────────────

@router.get("/stream")
async def live_stream(
    users:   str = Query(..., description="Comma-separated user names (max 10)"),
    session: SessionToken = Depends(require_auth),
):
    """SSE stream: live positions + balance updates for up to 10 selected users.

    Events emitted:
      data: {"type": "connected",       "users": [...]}
      data: {"type": "position_update", "user": "...", "data": {"positions": [...]}}
      data: {"type": "balance_update",  "user": "...", "data": {balance row}}
      : keepalive  (every 20 s)

    WS watcher tasks start on first subscriber and stop when the last subscriber
    disconnects — so connecting/disconnecting the dashboard is zero-cost at rest.
    """
    requested = [u.strip() for u in users.split(",") if u.strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="No users specified")
    if len(requested) > MAX_LIVE_USERS:
        raise HTTPException(
            status_code=400,
            detail=f"Live session limited to {MAX_LIVE_USERS} users; {len(requested)} requested",
        )

    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    subscribed: list[tuple[str, str]] = []

    async def generate():
        nonlocal subscribed
        try:
            yield f"data: {json.dumps({'type': 'connected', 'users': requested})}\n\n"
            for user_name in requested:
                for kind in ("positions", "balance"):
                    await _ensure_watcher(user_name, kind, queue)
                    subscribed.append((user_name, kind))
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=20)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except (GeneratorExit, asyncio.CancelledError):
            pass
        finally:
            for user_name, kind in subscribed:
                try:
                    await _unsubscribe(user_name, kind, queue)
                except Exception:
                    pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
