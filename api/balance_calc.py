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
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from threading import RLock

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth
from api.page_templates import render_page_urls, script_json
from logging_helpers import human_log as _log
from pb7_config import load_pb7_config
from pb8_config import load_pb8_config
from User import Users

SERVICE = "BalanceCalc"
router = APIRouter()

# ── Draft store ───────────────────────────────────────────────
_draft_store: dict[str, tuple[float, dict]] = {}
_DRAFT_TTL = 600  # 10 minutes
_draft_lock = RLock()


def _clean_drafts() -> None:
    """Remove expired process-local drafts under the shared thread lock."""
    with _draft_lock:
        now = time.monotonic()
        expired = [k for k, (ts, _) in _draft_store.items() if now - ts >= _DRAFT_TTL]
        for k in expired:
            _draft_store.pop(k, None)


# ── Helpers ───────────────────────────────────────────────────

PBGDIR = Path(__file__).resolve().parent.parent
RUN_V7_DIR = PBGDIR / "data" / "run_v7"
RUN_V8_DIR = PBGDIR / "data" / "run_v8"
COINDATA_DIR = PBGDIR / "data" / "coindata"

EXCHANGES = ["binance", "bybit", "bitget", "gateio", "hyperliquid", "kucoin", "okx"]


def _validate_instance_name(name: object) -> str:
    """Validate one instance-directory component."""
    value = str(name or "")
    if value != value.strip() or not value or value.startswith(".") or value in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid instance name")
    if any(char in value for char in ("/", "\\", "\x00")) or any(ord(char) < 32 for char in value):
        raise HTTPException(status_code=400, detail="Invalid instance name")
    if len(value.encode("utf-8")) > 128:
        raise HTTPException(status_code=400, detail="Instance name is too long")
    return value


def _instance_config_path(version: object, name: object) -> Path:
    """Resolve one validated PB7 or PB8 config without following symlinks."""
    clean_version = str(version or "").strip().lower()
    roots = {"v7": RUN_V7_DIR, "v8": RUN_V8_DIR}
    root = roots.get(clean_version)
    if root is None:
        raise HTTPException(status_code=400, detail="Version must be v7 or v8")
    clean_name = _validate_instance_name(name)
    instance_dir = root / clean_name
    config_path = instance_dir / "config.json"
    if root.is_symlink() or instance_dir.is_symlink() or config_path.is_symlink() or not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"{clean_version.upper()} instance '{clean_name}' not found")
    try:
        config_path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid instance path") from exc
    return config_path


def _read_json(path: Path) -> dict | list | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _norm_coin(c: str) -> str:
    """Normalize spelling only; market aliases must come from the mapping."""
    u = c.strip().upper()
    if u.startswith("XYZ:") and len(u) > 4:
        return "XYZ-" + u[4:]
    return u


def _load_mapping(exchange: str) -> list[dict]:
    """Load mapping.json for an exchange."""
    path = COINDATA_DIR / exchange / "mapping.json"
    if not path.exists():
        return []
    data = _read_json(path)
    return data if isinstance(data, list) else []


def _extract_coins(config: dict, available_coins: set[str] | None = None,
                   aliases: dict[str, str] | None = None) -> tuple[set[str], set[str], set[str]]:
    """Extract (all_coins, coins_long, coins_short) from config dict."""
    available = available_coins or set()
    live = config.get("live", {}) if isinstance(config.get("live"), dict) else {}
    ac = live.get("approved_coins", {})
    if isinstance(ac, (str, list, tuple)):
        long_list = ac
        short_list = ac
    elif isinstance(ac, dict):
        long_list = ac.get("long", [])
        short_list = ac.get("short", [])
    else:
        long_list = []
        short_list = []

    def resolve(value: object) -> set[str]:
        """Resolve canonical names first, then unambiguous mapping aliases."""
        values = [value] if isinstance(value, str) else list(value) if isinstance(value, (list, tuple, set)) else []
        if len(values) == 1 and str(values[0]).strip().lower() == "all":
            return set(available)
        names = {_norm_coin(str(coin)) for coin in values if str(coin).strip()}
        return {name if name in available else (aliases or {}).get(name, name) for name in names}

    coins_long = resolve(long_list)
    coins_short = resolve(short_list)
    ignored = live.get("ignored_coins", {})
    if isinstance(ignored, (str, list, tuple)):
        ignored_long = resolve(ignored)
        ignored_short = resolve(ignored)
    elif isinstance(ignored, dict):
        ignored_long = resolve(ignored.get("long", []))
        ignored_short = resolve(ignored.get("short", []))
    else:
        ignored_long = set()
        ignored_short = set()
    coins_long -= ignored_long
    coins_short -= ignored_short
    if available_coins is not None:
        coins_long &= available
        coins_short &= available
    coins = coins_long | coins_short
    return coins, coins_long, coins_short


def _extract_bot_params(config: dict) -> dict:
    """Extract V7 or V8 bot-side parameters needed for balance calculation."""
    bot = config.get("bot", {})
    live = config.get("live", {})
    if not isinstance(bot, dict) or not isinstance(live, dict):
        raise ValueError("bot and live must be JSON objects")
    strategy_kind = str(live.get("strategy_kind") or "").strip()
    result = {}
    for side in ("long", "short"):
        s = bot.get(side, {})
        if not isinstance(s, dict):
            raise ValueError(f"bot.{side} must be a JSON object")
        risk = s.get("risk", {}) if isinstance(s.get("risk"), dict) else {}
        strategies = s.get("strategy", {}) if isinstance(s.get("strategy"), dict) else {}
        strategy = strategies.get(strategy_kind) if strategy_kind else None
        if not isinstance(strategy, dict) and len(strategies) == 1:
            strategy = next(iter(strategies.values()))
        strategy = strategy if isinstance(strategy, dict) else {}
        entry = strategy.get("entry", {}) if isinstance(strategy.get("entry"), dict) else {}
        initial_qty_pct = entry.get("initial_qty_pct")
        if initial_qty_pct is None:
            initial_qty_pct = strategy.get("base_qty_pct")
        if initial_qty_pct is None:
            initial_qty_pct = s.get("entry_initial_qty_pct", 0)
        values = {
            "n_positions": risk.get("n_positions", s.get("n_positions", 0)),
            "total_wallet_exposure_limit": risk.get("total_wallet_exposure_limit", s.get("total_wallet_exposure_limit", 0)),
            "entry_initial_qty_pct": initial_qty_pct,
        }
        result[side] = {}
        for name, raw in values.items():
            try:
                original = Decimal(0 if raw is None else raw)
            except (TypeError, ValueError, OverflowError, InvalidOperation) as exc:
                raise ValueError(f"bot.{side}.{name} must be a finite non-negative number") from exc
            if isinstance(raw, bool) or not original.is_finite() or original < 0:
                raise ValueError(f"bot.{side}.{name} must be a finite non-negative number")
            # Nonzero decimal strings must not underflow into a disabled side.
            value = float(original)
            if not math.isfinite(value) or (value == 0 and not original.is_zero()):
                raise ValueError(f"bot.{side}.{name} is outside the supported numeric range")
            result[side][name] = value
    return result


def _apply_dynamic_ignore(config: dict, exchange: str, available_coins: set[str],
                          aliases: dict[str, str] | None = None) -> tuple[set[str], set[str], set[str]]:
    """If dynamic_ignore is enabled, filter mapping and override approved_coins."""
    from PBCoinData import CoinData

    pbgui = config.get("pbgui", {})
    if not pbgui.get("dynamic_ignore", False):
        return _extract_coins(config, available_coins, aliases)

    coindata = CoinData()
    approved, _ = coindata.filter_mapping(
        exchange=exchange,
        market_cap_min_m=pbgui.get("market_cap", 0),
        vol_mcap_max=pbgui.get("vol_mcap", 10.0),
        only_cpt=pbgui.get("only_cpt", False),
        notices_ignore=pbgui.get("notices_ignore", False),
        tags=pbgui.get("tags", []),
        active_only=True,
        quote_filter=["USDC" if exchange == "hyperliquid" else "USDT"],
        use_cache=True,
    )
    return _extract_coins({"live": {"approved_coins": approved}}, available_coins, aliases)


def _calculate(config: dict, exchange: str) -> dict:
    """Run the balance calculation and return results."""
    from PBCoinData import compute_coin_name

    bot_params = _extract_bot_params(config)
    mapping = _load_mapping(exchange)
    if not mapping:
        return {"error": f"No mapping data for exchange '{exchange}'. Check Coin Data configuration."}

    preferred_quote = "USDC" if exchange == "hyperliquid" else "USDT"

    def eligible(record: dict) -> bool:
        if not bool(record.get("active", True)) or not bool(record.get("swap", False)) or not bool(record.get("linear", True)):
            return False
        if str(record.get("quote") or "").upper() != preferred_quote:
            return False
        if exchange == "hyperliquid":
            if bool(record.get("is_hip3", False)) and str(record.get("dex") or "").strip().lower() != "xyz":
                return False
            try:
                open_interest = float(record.get("open_interest")) if record.get("open_interest") is not None else None
            except (TypeError, ValueError):
                open_interest = None
            if open_interest is not None and open_interest <= 0:
                return False
        return True

    # Find best mapping row per coin
    best_rows_by_coin = {}
    alias_targets: dict[str, set[str]] = {}
    for record in mapping:
        if not isinstance(record, dict) or not eligible(record):
            continue
        quote = str(record.get("quote") or "").upper()
        raw_coin = str(record.get("coin") or compute_coin_name(str(record.get("symbol") or ""), quote))
        coin = _norm_coin(raw_coin)
        if not coin:
            continue
        try:
            price = float(record.get("price_last") or 0.0)
            contract_size = float(record.get("contract_size") or 1.0)
            min_amount = float(record.get("min_amount") or record.get("precision_amount") or 0.0)
            min_cost = float(record.get("min_cost") or 0.0)
            min_order_price = float(record.get("min_order_price") or 0.0)
        except (TypeError, ValueError, OverflowError):
            continue
        if not all(math.isfinite(value) and value >= 0 for value in (price, contract_size, min_amount, min_cost, min_order_price)):
            continue
        if min_order_price <= 0 and price > 0:
            min_order_price = max(min_cost, min_amount * contract_size * price)
        if not math.isfinite(min_order_price) or min_order_price <= 0:
            continue

        for alias in (coin + quote, record.get("symbol"), record.get("base"), record.get("ccxt_symbol")):
            if isinstance(alias, str) and alias.strip():
                alias_targets.setdefault(_norm_coin(alias), set()).add(coin)

        score = (0 if min_order_price > 0 else 1, -price, str(record.get("symbol") or ""))

        prev = best_rows_by_coin.get(coin)
        if prev is None or score < prev[0]:
            best_rows_by_coin[coin] = (score, record, min_order_price, price, contract_size, min_amount, min_cost)

    available_coins = set(best_rows_by_coin)
    aliases = {alias: next(iter(targets)) for alias, targets in alias_targets.items() if len(targets) == 1}
    coins, coins_long, coins_short = _apply_dynamic_ignore(config, exchange, available_coins, aliases)
    if not coins:
        return {"error": "No eligible approved coins with usable minimum-order data were found."}

    coin_infos = []
    balance_long = []
    balance_short = []

    for coin in sorted(coins):
        best = best_rows_by_coin.get(coin)
        if best is None:
            continue
        _, record, min_order_price, price, contract_size, min_amount, min_cost = best
        lev = record.get("max_leverage")
        try:
            lev = float(lev) if lev is not None else None
        except (TypeError, ValueError, OverflowError):
            lev = None
        if lev is not None and (not math.isfinite(lev) or lev < 0):
            lev = None
        coin_infos.append({
            "coin": coin,
            "currentPrice": price,
            "contractSize": contract_size,
            "min_amount": min_amount,
            "min_cost": min_cost,
            "min_order_price": min_order_price,
            "max_lev": lev,
        })
        for side, side_coins, balances in (("long", coins_long, balance_long), ("short", coins_short, balance_short)):
            bp = bot_params[side]
            if coin not in side_coins or any(value == 0 for value in bp.values()):
                continue
            denominator = (bp["total_wallet_exposure_limit"] / bp["n_positions"]) * bp["entry_initial_qty_pct"]
            if not math.isfinite(denominator) or denominator <= 0:
                raise ValueError(f"{side} sizing is outside the supported numeric range")
            balance = min_order_price / denominator
            if not math.isfinite(balance) or balance <= 0:
                raise ValueError(f"{side} required balance is outside the supported numeric range")
            balances.append({"coin": coin, "balance": balance})

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
        min_op = best_rows_by_coin[symbol][2]
        calculated = bl[0]["balance"]
        if not math.isfinite(calculated * 1.1):
            raise ValueError("Recommended balance is outside the supported numeric range")
        # Decimal avoids a spurious extra step for binary floats such as 800 * 1.1.
        recommended = int((Decimal(str(calculated)) * Decimal("1.1") / 10).to_integral_value(rounding=ROUND_CEILING)) * 10
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

    for item in coin_infos:
        item["min_order_price"] = round(item["min_order_price"], 6)
    for item in balance_long + balance_short:
        item["balance"] = round(item["balance"], 2)
    return result


# ── Endpoints ────────────────────────────────────────────────

@router.post("/load-config")
def load_config(
    request_body: dict,
    session: SessionToken = Depends(require_auth),
):
    """Load one named PB7 or PB8 instance config."""
    version = str(request_body.get("version") or "").strip().lower()
    name = request_body.get("name")
    path = _instance_config_path(version, name)
    try:
        config = load_pb7_config(path, neutralize_added=False) if version == "v7" else load_pb8_config(path)
    except Exception as exc:
        _log(
            SERVICE,
            f"Failed to load {version.upper()} instance '{name}': {exc}",
            level="ERROR",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=f"Failed to load {version.upper()} instance '{name}'") from exc
    exchange = ""
    user = config.get("live", {}).get("user", "") if isinstance(config.get("live"), dict) else ""
    if user:
        try:
            exchange = str(Users().find_exchange(user) or "").lower()
        except Exception as exc:
            _log(SERVICE, f"Failed to resolve exchange for user '{user}': {exc}", level="WARNING")
    return {"config": config, "exchange": exchange}


@router.get("/instances")
def get_instances(session: SessionToken = Depends(require_auth)):
    """List named PB7 and PB8 instances without exposing filesystem paths."""
    instances = []
    for version, root in (("v7", RUN_V7_DIR), ("v8", RUN_V8_DIR)):
        if not root.is_dir() or root.is_symlink():
            continue
        for instance_dir in root.iterdir():
            if not instance_dir.is_dir() or instance_dir.is_symlink():
                continue
            config_path = instance_dir / "config.json"
            if not config_path.is_file() or config_path.is_symlink():
                continue
            instances.append({"name": instance_dir.name, "version": version})
    return sorted(instances, key=lambda item: (item["name"].lower(), item["version"]))


@router.post("/calculate")
def calculate_balance(
    request_body: dict,
    session: SessionToken = Depends(require_auth),
):
    """Run balance calculation.

    Body: { "config": <dict>, "exchange": "bybit" }
    or    { "config_file": "/path/to/config.json", "exchange": "bybit" }
    """
    exchange = request_body.get("exchange", "")
    if not isinstance(exchange, str):
        raise HTTPException(status_code=422, detail="exchange must be a string")
    exchange = exchange.strip().lower()
    if exchange not in EXCHANGES:
        return {"error": f"Invalid exchange: '{exchange}'. Must be one of {EXCHANGES}"}

    config = request_body.get("config")
    if not config and "config_file" in request_body:
        config_file = request_body["config_file"]
        if (not isinstance(config_file, str) or not config_file.strip()
                or "\\" in config_file or any(ord(char) < 32 or ord(char) == 127 for char in config_file)
                or any(part in {".", ".."} for part in config_file.split("/"))):
            raise HTTPException(status_code=422, detail="config_file must be a valid non-empty path")
        path = Path(config_file).absolute()
        loader = None
        for root, candidate_loader in ((RUN_V7_DIR, load_pb7_config), (RUN_V8_DIR, load_pb8_config)):
            try:
                relative = path.relative_to(root.absolute())
            except ValueError:
                continue
            if root.is_symlink() or any((root / Path(*relative.parts[:index])).is_symlink()
                                        for index in range(1, len(relative.parts) + 1)):
                raise HTTPException(status_code=422, detail="Config file symlinks are not allowed")
            if not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
                raise HTTPException(status_code=422, detail="Config file not found under the allowed run directory")
            loader = candidate_loader
            break
        if loader is None:
            raise HTTPException(status_code=422, detail="Config file must be under data/run_v7/ or data/run_v8/")
        try:
            config = loader(path, neutralize_added=False) if loader is load_pb7_config else loader(path)
        except HTTPException:
            raise
        except Exception as exc:
            _log(SERVICE, "Failed to load calculator config", level="ERROR", meta={"error_type": type(exc).__name__})
            raise HTTPException(status_code=422, detail="Failed to load config file") from exc

    if not isinstance(config, dict):
        raise HTTPException(status_code=422, detail="Invalid config: must be a JSON object")

    try:
        return _calculate(config, exchange)
    except HTTPException:
        raise
    except ValueError as exc:
        _log(SERVICE, "Invalid balance calculation parameters", level="WARNING")
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _log(SERVICE, "Balance calculation failed", level="ERROR", meta={"error_type": type(exc).__name__})
        raise HTTPException(status_code=500, detail="Balance calculation failed") from exc


@router.post("/draft")
def create_draft(body: dict, session: SessionToken = Depends(require_auth)):
    """Store a config dict temporarily and return a draft_id (TTL 10 min)."""
    config = body.get("config")
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be a JSON object")
    with _draft_lock:
        _clean_drafts()
        draft_id = _secrets.token_urlsafe(16)
        _draft_store[draft_id] = (time.monotonic(), config)
    return {"draft_id": draft_id}


@router.get("/draft/{draft_id}")
def get_draft(draft_id: str, session: SessionToken = Depends(require_auth)):
    """Retrieve a stored draft config."""
    with _draft_lock:
        entry = _draft_store.get(draft_id)
        if not entry or time.monotonic() - entry[0] >= _DRAFT_TTL:
            _draft_store.pop(draft_id, None)
            raise HTTPException(404, "Draft not found or expired")
    return {"config": entry[1]}


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    instance: str = Query(default="", description="Pre-select instance name"),
    instance_version: str = Query(default="", description="Pre-select instance version"),
    draft_id: str = Query(default="", description="Draft config id to pre-load"),
    exchange: str = Query(default="", description="Pre-select exchange"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Balance Calculator page."""
    html_path = Path(__file__).parent.parent / "frontend" / "balance_calc.html"
    html = html_path.read_text(encoding="utf-8")

    html = render_page_urls(request, html, "/api/balance-calc")
    html = html.replace('"%%INSTANCE%%"', script_json(instance))
    html = html.replace('"%%INSTANCE_VERSION%%"', script_json(instance_version))
    html = html.replace('"%%DRAFT_ID%%"', script_json(draft_id))
    html = html.replace('"%%INIT_EXCHANGE%%"', script_json(exchange))
    html = html.replace('"%%EXCHANGES%%"', script_json(EXCHANGES))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', script_json(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', script_json(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
