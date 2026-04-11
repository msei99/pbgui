"""
FastAPI router for the Balance Calculator page.

Endpoints:
    GET  /main_page         → serve the standalone HTML page
    GET  /instances          → list v7 instance names
    POST /calculate          → run balance calculation
    POST /draft              → store config temporarily, returns draft_id
    GET  /draft/{draft_id}   → retrieve stored draft config
"""

from __future__ import annotations

import json
import math
import secrets as _secrets
import time
import traceback
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from User import Users

SERVICE = "BalanceCalc"
router = APIRouter()

# ── Draft store ───────────────────────────────────────────────
_draft_store: dict[str, tuple[float, dict]] = {}
_DRAFT_TTL = 600  # 10 minutes


def _clean_drafts() -> None:
    now = time.time()
    expired = [k for k, (ts, _) in _draft_store.items() if now - ts > _DRAFT_TTL]
    for k in expired:
        _draft_store.pop(k, None)


# ── Helpers ───────────────────────────────────────────────────

PBGDIR = Path(__file__).resolve().parent.parent
RUN_V7_DIR = PBGDIR / "data" / "run_v7"
COINDATA_DIR = PBGDIR / "data" / "coindata"

EXCHANGES = ["binance", "bybit", "bitget", "gateio", "hyperliquid", "okx"]


def _read_json(path: Path) -> dict | list | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _norm_coin(c: str) -> str:
    """Normalize coin name to match mapping 'coin' field.

    Config uses symbol names like 'DOGEUSDT', mapping uses base coin 'DOGE'.
    PB7 stock perps use 'xyz:AAPL', mapping uses 'XYZ-AAPL'.
    """
    u = c.strip().upper()
    if u.startswith("XYZ:") and len(u) > 4:
        return "XYZ-" + u[4:]
    # Strip common quote suffixes to get the base coin name
    for suffix in ("USDT", "USDC", "BUSD", "USD"):
        if u.endswith(suffix) and len(u) > len(suffix):
            return u[:-len(suffix)]
    return u


def _load_mapping(exchange: str) -> list[dict]:
    """Load mapping.json for an exchange."""
    path = COINDATA_DIR / exchange / "mapping.json"
    if not path.exists():
        return []
    data = _read_json(path)
    return data if isinstance(data, list) else []


def _extract_coins(config: dict) -> tuple[set[str], set[str], set[str]]:
    """Extract (all_coins, coins_long, coins_short) from config dict."""
    live = config.get("live", {})
    ac = live.get("approved_coins", {})
    if isinstance(ac, list):
        long_list = ac
        short_list = ac
    else:
        long_list = ac.get("long", [])
        short_list = ac.get("short", [])
    coins_long = set(_norm_coin(c) for c in long_list if c)
    coins_short = set(_norm_coin(c) for c in short_list if c)
    coins = coins_long | coins_short
    return coins, coins_long, coins_short


def _extract_bot_params(config: dict) -> dict:
    """Extract bot long/short parameters needed for balance calculation."""
    bot = config.get("bot", {})
    result = {}
    for side in ("long", "short"):
        s = bot.get(side, {})
        result[side] = {
            "n_positions": float(s.get("n_positions", 0)),
            "total_wallet_exposure_limit": float(s.get("total_wallet_exposure_limit", 0)),
            "entry_initial_qty_pct": float(s.get("entry_initial_qty_pct", 0)),
        }
    return result


def _apply_dynamic_ignore(config: dict, exchange: str) -> tuple[set[str], set[str], set[str]]:
    """If dynamic_ignore is enabled, filter mapping and override approved_coins."""
    from PBCoinData import CoinData

    pbgui = config.get("pbgui", {})
    if not pbgui.get("dynamic_ignore", False):
        return _extract_coins(config)

    coindata = CoinData()
    approved, _ = coindata.filter_mapping(
        exchange=exchange,
        market_cap_min_m=pbgui.get("market_cap", 0),
        vol_mcap_max=pbgui.get("vol_mcap", 10.0),
        only_cpt=pbgui.get("only_cpt", False),
        notices_ignore=pbgui.get("notices_ignore", False),
        tags=pbgui.get("tags", []),
        quote_filter=None,
        use_cache=True,
    )
    coins_long = set(_norm_coin(c) for c in approved if c)
    coins_short = set(_norm_coin(c) for c in approved if c)
    coins = coins_long | coins_short
    return coins, coins_long, coins_short


def _calculate(config: dict, exchange: str) -> dict:
    """Run the balance calculation and return results."""
    # Determine coins
    coins, coins_long, coins_short = _apply_dynamic_ignore(config, exchange)
    if not coins:
        return {"error": "No approved coins found in config."}

    bot_params = _extract_bot_params(config)
    mapping = _load_mapping(exchange)
    if not mapping:
        return {"error": f"No mapping data for exchange '{exchange}'. Check Coin Data configuration."}

    preferred_quote = "USDC" if exchange == "hyperliquid" else "USDT"

    # Find best mapping row per coin
    best_rows_by_coin = {}
    for record in mapping:
        coin = (record.get("coin") or "").upper()
        if not coin or coin not in coins:
            continue
        quote = (record.get("quote") or "").upper()
        price = float(record.get("price_last") or 0.0)
        contract_size = float(record.get("contract_size") or 1.0)
        min_amount = float(record.get("min_amount") or record.get("precision_amount") or 0.0)
        min_cost = float(record.get("min_cost") or 0.0)
        min_order_price = float(record.get("min_order_price") or 0.0)
        if min_order_price <= 0 and price > 0:
            min_order_price = max(min_cost, min_amount * contract_size * price)

        score = (
            0 if quote == preferred_quote else 1,
            0 if bool(record.get("active", True)) else 1,
            0 if bool(record.get("linear", True)) else 1,
            0 if min_order_price > 0 else 1,
            -price,
        )

        prev = best_rows_by_coin.get(coin)
        if prev is None or score < prev[0]:
            best_rows_by_coin[coin] = (score, record, min_order_price, price, contract_size, min_amount, min_cost)

    coin_infos = []
    balance_long = []
    balance_short = []

    for coin in sorted(coins):
        best = best_rows_by_coin.get(coin)
        if best is None:
            continue
        _, record, min_order_price, price, contract_size, min_amount, min_cost = best
        lev = record.get("max_leverage")
        coin_infos.append({
            "coin": coin,
            "currentPrice": price,
            "contractSize": contract_size,
            "min_amount": min_amount,
            "min_cost": min_cost,
            "min_order_price": round(min_order_price, 6),
            "max_lev": lev,
        })
        lp = bot_params["long"]
        if coin in coins_long and lp["n_positions"] > 0 and lp["total_wallet_exposure_limit"] > 0 and lp["entry_initial_qty_pct"] > 0:
            we = lp["total_wallet_exposure_limit"] / lp["n_positions"]
            balance = min_order_price / (we * lp["entry_initial_qty_pct"])
            balance_long.append({"coin": coin, "balance": round(balance, 2)})

        sp = bot_params["short"]
        if coin in coins_short and sp["n_positions"] > 0 and sp["total_wallet_exposure_limit"] > 0 and sp["entry_initial_qty_pct"] > 0:
            we = sp["total_wallet_exposure_limit"] / sp["n_positions"]
            balance = min_order_price / (we * sp["entry_initial_qty_pct"])
            balance_short.append({"coin": coin, "balance": round(balance, 2)})

    # Sort
    coin_infos.sort(key=lambda x: x["min_order_price"], reverse=True)
    balance_long.sort(key=lambda x: x["balance"], reverse=True)
    balance_short.sort(key=lambda x: x["balance"], reverse=True)

    # Determine which side dominates
    result = {
        "exchange": exchange,
        "coin_infos": coin_infos,
        "balance_long": balance_long,
        "balance_short": balance_short,
        "bot_params": bot_params,
        "recommendation": None,
    }

    side = None
    if balance_long and balance_short:
        side = "long" if balance_long[0]["balance"] > balance_short[0]["balance"] else "short"
    elif balance_long:
        side = "long"
    elif balance_short:
        side = "short"

    if side:
        bl = balance_long if side == "long" else balance_short
        bp = bot_params[side]
        symbol = bl[0]["coin"]
        min_op = next((c["min_order_price"] for c in coin_infos if c["coin"] == symbol), 0)
        calculated = min_op / ((bp["total_wallet_exposure_limit"] / bp["n_positions"]) * bp["entry_initial_qty_pct"])
        recommended = math.ceil(calculated * 1.1 / 10) * 10
        result["recommendation"] = {
            "side": side,
            "symbol": symbol,
            "min_order_price": round(min_op, 6),
            "total_wallet_exposure_limit": bp["total_wallet_exposure_limit"],
            "n_positions": bp["n_positions"],
            "entry_initial_qty_pct": bp["entry_initial_qty_pct"],
            "calculated_balance": round(calculated, 2),
            "recommended_balance": recommended,
        }

    return result


# ── Endpoints ────────────────────────────────────────────────

@router.post("/load-config")
def load_config(
    request_body: dict,
    session: SessionToken = Depends(require_auth),
):
    """Load a config JSON file and return its contents.

    Body: { "config_file": "/path/to/config.json" }
    """
    config_file = request_body.get("config_file", "")
    if not config_file:
        return {"error": "No config_file specified"}
    p = Path(config_file)
    try:
        p.resolve().relative_to(RUN_V7_DIR.resolve())
    except ValueError:
        return {"error": "Config file must be under data/run_v7/"}
    config = _read_json(p)
    if config is None:
        return {"error": f"Failed to read: {config_file}"}
    return {"config": config}


@router.get("/instances")
def get_instances(session: SessionToken = Depends(require_auth)):
    """List v7 instance names with their exchange."""
    # Load users once for exchange lookup
    try:
        users = Users()
    except Exception:
        users = None

    instances = []
    if RUN_V7_DIR.is_dir():
        for d in sorted(RUN_V7_DIR.iterdir()):
            if not d.is_dir():
                continue
            cfg_file = d / "config.json"
            if not cfg_file.exists():
                continue
            name = d.name
            exchange = ""
            # Primary: derive exchange from live.user via api-keys
            cfg = _read_json(cfg_file)
            if cfg and isinstance(cfg, dict):
                user = cfg.get("live", {}).get("user", "")
                if user and users:
                    ex = users.find_exchange(user)
                    if ex:
                        exchange = ex.lower()
            # Fallback: directory name prefix
            if not exchange:
                for ex in EXCHANGES:
                    if name.lower().startswith(ex + "_"):
                        exchange = ex
                        break
            instances.append({"name": name, "exchange": exchange, "config_file": str(cfg_file)})
    return instances


@router.post("/calculate")
def calculate_balance(
    request_body: dict,
    session: SessionToken = Depends(require_auth),
):
    """Run balance calculation.

    Body: { "config": <dict>, "exchange": "bybit" }
    or    { "config_file": "/path/to/config.json", "exchange": "bybit" }
    """
    exchange = request_body.get("exchange", "").strip().lower()
    if exchange not in EXCHANGES:
        return {"error": f"Invalid exchange: '{exchange}'. Must be one of {EXCHANGES}"}

    config = request_body.get("config")
    config_file = request_body.get("config_file", "")

    if not config and config_file:
        p = Path(config_file)
        # Security: only allow files under data/run_v7
        try:
            p.resolve().relative_to(RUN_V7_DIR.resolve())
        except ValueError:
            return {"error": "Config file must be under data/run_v7/"}
        config = _read_json(p)
        if config is None:
            return {"error": f"Failed to read config file: {config_file}"}

    if not isinstance(config, dict):
        return {"error": "Invalid config — must be a JSON object"}

    try:
        return _calculate(config, exchange)
    except Exception as e:
        _log(SERVICE, f"Calculation error: {e}", level="ERROR",
             meta={"traceback": traceback.format_exc()})
        return {"error": f"Calculation failed: {e}"}


@router.post("/draft")
def create_draft(body: dict, session: SessionToken = Depends(require_auth)):
    """Store a config dict temporarily and return a draft_id (TTL 10 min)."""
    config = body.get("config")
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be a JSON object")
    _clean_drafts()
    draft_id = _secrets.token_urlsafe(16)
    _draft_store[draft_id] = (time.time(), config)
    return {"draft_id": draft_id}


@router.get("/draft/{draft_id}")
def get_draft(draft_id: str, session: SessionToken = Depends(require_auth)):
    """Retrieve a stored draft config."""
    entry = _draft_store.get(draft_id)
    if not entry:
        raise HTTPException(404, "Draft not found or expired")
    return {"config": entry[1]}


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    instance: str = Query(default="", description="Pre-select instance name"),
    draft_id: str = Query(default="", description="Draft config id to pre-load"),
    exchange: str = Query(default="", description="Pre-select exchange"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Balance Calculator page."""
    html_path = Path(__file__).parent.parent / "frontend" / "balance_calc.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/balance-calc"

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%INSTANCE%%"', json.dumps(instance))
    html = html.replace('"%%DRAFT_ID%%"', json.dumps(draft_id))
    html = html.replace('"%%INIT_EXCHANGE%%"', json.dumps(exchange))
    html = html.replace('"%%EXCHANGES%%"', json.dumps(EXCHANGES))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_func import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
