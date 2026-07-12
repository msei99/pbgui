"""FastAPI endpoints for market data status monitoring and standalone page shell."""

from datetime import date as _date, datetime as _datetime
import shutil
import shlex

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pathlib import Path, PurePosixPath
from typing import Any
import json
from urllib.parse import urlencode

from hyperliquid_aws import HYPERLIQUID_AWS_REGION
from hyperliquid_best_1m import (
    _load_tradfi_profiles_from_ini,
    get_tiingo_runtime_usage,
    probe_tiingo_iex_1m,
    resolve_tradfi_symbol,
)
from market_data import (
    _get_pb7_root_dir,
    get_effective_enabled_coins,
    get_exchange_raw_root_dir,
    get_market_data_coin_options,
    get_minute_presence_for_dataset,
    load_aws_profile_credentials,
    load_aws_profile_region,
    load_market_data_config,
    normalize_market_data_coin_dir,
    save_aws_profile_credentials,
    save_aws_profile_region,
    set_auto_enable_new_coins,
    set_enabled_coins,
)
from market_data_tradfi import (
    TRADFI_CANONICAL_TYPES,
    TRADFI_STATUSES,
    TRADFI_STATUSES_SELECTABLE,
    build_effective_tradfi_status_map,
    build_merged_tradfi_table,
    build_tiingo_meta_cache_info,
    build_tiingo_search_price_map,
    build_tradfi_quote_cache_info,
    build_xyz_spec_cache_info,
    build_xyz_spec_rows,
    find_tradfi_row,
    load_tradfi_map,
    refresh_tradfi_quote_cache,
    tradfi_canonical_type_for_coin,
    tradfi_expected_indices_for_type,
    tiingo_search,
    update_tiingo_start_date_for_selected,
    update_tiingo_start_dates_for_all,
    upsert_tradfi_map_entry,
)
from market_data_sources import SOURCE_CODE_API, remove_days_from_index, update_source_index_for_day
from pbgui_purefunc import coin_from_symbol_code
from pbgui_purefunc import load_ini, save_ini
from tradfi_sync import auto_map_tradfi, fetch_tiingo_meta, fetch_xyz_spec
from logging_helpers import human_log as _log

from .auth import require_auth, SessionToken
from .heatmap import _get_missing_lag_minutes

router = APIRouter(prefix="/market-data", tags=["market-data"])
_market_data_status_snapshot: dict[str, Any] = {}
SERVICE = "MarketDataAPI"

PBGDIR = Path(__file__).resolve().parent.parent
SETTINGS_EXCHANGES: dict[str, dict[str, Any]] = {
    "hyperliquid": {
        "label": "Hyperliquid",
        "ini_section": "pbdata",
        "defaults": {
            "interval_seconds": 1800,
            "coin_pause_seconds": 0.5,
            "api_timeout_seconds": 30.0,
            "min_lookback_days": 2,
            "max_lookback_days": 4,
        },
        "save_message": "Settings saved. Refresh queued; cycle will start within seconds.",
    },
    "binance": {
        "label": "Binance USDM",
        "ini_section": "binance_data",
        "defaults": {
            "interval_seconds": 3600,
            "coin_pause_seconds": 0.5,
            "api_timeout_seconds": 30.0,
            "min_lookback_days": 2,
            "max_lookback_days": 7,
        },
        "save_message": "Settings saved. Binance refresh queued; cycle will start within seconds.",
    },
    "bybit": {
        "label": "Bybit",
        "ini_section": "bybit_data",
        "defaults": {
            "interval_seconds": 3600,
            "coin_pause_seconds": 0.5,
            "api_timeout_seconds": 30.0,
            "min_lookback_days": 2,
            "max_lookback_days": 7,
        },
        "save_message": "Settings saved. Bybit refresh queued; cycle will start within seconds.",
    },
    "okx": {
        "label": "OKX",
        "ini_section": "okx_data",
        "defaults": {
            "interval_seconds": 3600,
            "coin_pause_seconds": 0.5,
            "api_timeout_seconds": 30.0,
            "min_lookback_days": 2,
            "max_lookback_days": 7,
        },
        "save_message": "Settings saved. OKX refresh queued; cycle will start within seconds.",
    },
    "bitget": {
        "label": "Bitget",
        "ini_section": "bitget_data",
        "defaults": {
            "interval_seconds": 3600,
            "coin_pause_seconds": 0.5,
            "api_timeout_seconds": 30.0,
            "min_lookback_days": 2,
            "max_lookback_days": 7,
        },
        "save_message": "Settings saved. Bitget refresh queued; cycle will start within seconds.",
    },
}


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = PBGDIR / "frontend" / "market_data_main.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/market-data"

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))

    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = PBGDIR / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


def _get_request_origin(request: Request) -> str:
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    return f"{scheme}://{host}" + (f":{port}" if port else "")


def _normalize_settings_exchange(exchange: str) -> str:
    ex = str(exchange or "").strip().lower()
    if ex in ("binanceusdm", "binance-usdm"):
        return "binance"
    return ex


def _get_exchange_settings_meta(exchange: str) -> tuple[str, dict[str, Any]]:
    ex = _normalize_settings_exchange(exchange)
    meta = SETTINGS_EXCHANGES.get(ex)
    if not meta:
        raise ValueError("Unknown exchange")
    return ex, meta


def _canonical_market_coin(exchange_name: str, coin_value: str) -> str:
    ex_name = str(exchange_name or "").strip().lower()
    value = str(coin_value or "").strip()
    if not value:
        return ""
    if ex_name == "hyperliquid":
        lower = value.lower()
        if lower.startswith("xyz:") or lower.startswith("xyz-"):
            tail = value[4:].strip().upper()
            return f"xyz:{tail}" if tail else ""
    return value.upper()


def _coin_options_for_exchange(exchange: str) -> list[str]:
    return get_market_data_coin_options(_normalize_settings_exchange(exchange))


def _read_int_ini(section: str, key: str, default: int) -> int:
    try:
        value = load_ini(section, key)
        value_s = str(value).strip() if value is not None else ""
        if value_s == "":
            return default
        return int(float(value_s))
    except Exception:
        return default


def _read_float_ini(section: str, key: str, default: float) -> float:
    try:
        value = load_ini(section, key)
        value_s = str(value).strip() if value is not None else ""
        if value_s == "":
            return default
        return float(value_s)
    except Exception:
        return default


def _read_bool_ini(section: str, key: str, default: bool) -> bool:
    value = str(load_ini(section, key) or "").strip().lower()
    if value == "":
        return default
    return value in ("true", "1", "yes", "on")


def _build_market_data_settings_payload(exchange: str) -> dict[str, Any]:
    ex, meta = _get_exchange_settings_meta(exchange)
    defaults = dict(meta.get("defaults") or {})

    cfg = load_market_data_config()
    coin_options = _coin_options_for_exchange(ex)
    enabled_coins, missing_saved_coins, auto_enable_new_coins = get_effective_enabled_coins(
        ex,
        cfg=cfg,
        coin_options=coin_options,
    )

    settings: dict[str, Any] = {
        "interval_seconds": _read_int_ini(meta["ini_section"], "latest_1m_interval_seconds", int(defaults["interval_seconds"])),
        "coin_pause_seconds": _read_float_ini(meta["ini_section"], "latest_1m_coin_pause_seconds", float(defaults["coin_pause_seconds"])),
        "api_timeout_seconds": _read_float_ini(meta["ini_section"], "latest_1m_api_timeout_seconds", float(defaults["api_timeout_seconds"])),
        "min_lookback_days": _read_int_ini(meta["ini_section"], "latest_1m_min_lookback_days", int(defaults["min_lookback_days"])),
        "max_lookback_days": _read_int_ini(meta["ini_section"], "latest_1m_max_lookback_days", int(defaults["max_lookback_days"])),
    }

    if ex == "hyperliquid":
        profile_for_settings = str(load_ini("market_data", "hl_aws_profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
        try:
            creds_settings = load_aws_profile_credentials(profile_for_settings)
        except Exception:
            creds_settings = {}

        region_default_settings = load_aws_profile_region(profile_for_settings) or HYPERLIQUID_AWS_REGION
        tradfi_profiles = _load_tradfi_profiles_from_ini()
        tiingo_cfg = tradfi_profiles.get("tiingo") if isinstance(tradfi_profiles, dict) else {}
        tiingo_api_key = str((tiingo_cfg or {}).get("api_key") or "")
        try:
            tiingo_usage = get_tiingo_runtime_usage(api_key=tiingo_api_key) if tiingo_api_key else {}
        except Exception:
            tiingo_usage = {}

        settings.update(
            {
                "aws_profile": profile_for_settings,
                "aws_access_key_id": str(creds_settings.get("aws_access_key_id") or ""),
                "aws_secret_access_key": str(creds_settings.get("aws_secret_access_key") or ""),
                "aws_region": region_default_settings,
                "l2book_scan_timeout_s": _read_float_ini("market_data", "hl_l2book_scan_timeout_s", 5.0),
                "l2book_scan_workers": _read_int_ini("market_data", "hl_l2book_scan_workers", 8),
                "l2book_archive_enabled": _read_bool_ini("market_data", "l2book_archive_enabled", False),
                "l2book_archive_dir": str(load_ini("market_data", "l2book_archive_dir") or "").strip(),
                "tiingo_api_key": tiingo_api_key,
                "tiingo_usage": tiingo_usage,
            }
        )

    return {
        "exchange": ex,
        "exchange_label": meta["label"],
        "auto_enable_new_coins": auto_enable_new_coins,
        "enabled_coins": enabled_coins,
        "coin_options": coin_options,
        "missing_saved_coins": missing_saved_coins,
        "settings": settings,
    }


def _save_market_data_settings(exchange: str, request: dict[str, Any]) -> dict[str, Any]:
    ex, meta = _get_exchange_settings_meta(exchange)
    body = request if isinstance(request, dict) else {}
    enabled_coins = body.get("enabled_coins", [])
    auto_enable_new_coins = body.get("auto_enable_new_coins")
    settings = body.get("settings", {})

    if not isinstance(enabled_coins, list):
        raise ValueError("enabled_coins must be a list")
    if not isinstance(settings, dict):
        raise ValueError("settings must be an object")
    if auto_enable_new_coins is None:
        auto_enable_new_coins = bool(load_market_data_config().auto_enable_new_coins.get(ex, False))
    elif not isinstance(auto_enable_new_coins, bool):
        raise ValueError("auto_enable_new_coins must be a boolean")

    if auto_enable_new_coins and not enabled_coins:
        enabled_coins = _coin_options_for_exchange(ex)

    set_enabled_coins(ex, [str(coin) for coin in enabled_coins])
    set_auto_enable_new_coins(ex, bool(auto_enable_new_coins))

    save_ini(meta["ini_section"], "latest_1m_interval_seconds", str(int(settings.get("interval_seconds", meta["defaults"]["interval_seconds"]))))
    save_ini(meta["ini_section"], "latest_1m_coin_pause_seconds", str(float(settings.get("coin_pause_seconds", meta["defaults"]["coin_pause_seconds"]))))
    save_ini(meta["ini_section"], "latest_1m_api_timeout_seconds", str(float(settings.get("api_timeout_seconds", meta["defaults"]["api_timeout_seconds"]))))
    save_ini(meta["ini_section"], "latest_1m_min_lookback_days", str(int(settings.get("min_lookback_days", meta["defaults"]["min_lookback_days"]))))
    save_ini(meta["ini_section"], "latest_1m_max_lookback_days", str(int(settings.get("max_lookback_days", meta["defaults"]["max_lookback_days"]))))

    if ex == "hyperliquid":
        profile = str(settings.get("aws_profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
        aws_access_key_id = str(settings.get("aws_access_key_id") or "").strip()
        aws_secret_access_key = str(settings.get("aws_secret_access_key") or "").strip()
        aws_region = str(settings.get("aws_region") or "").strip()

        save_ini("market_data", "hl_aws_profile", profile)
        save_aws_profile_credentials(
            profile=profile,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        save_aws_profile_region(profile=profile, region=aws_region)

        save_ini("market_data", "hl_l2book_scan_timeout_s", str(float(settings.get("l2book_scan_timeout_s", 5.0))))
        save_ini("market_data", "hl_l2book_scan_workers", str(int(settings.get("l2book_scan_workers", 8))))
        save_ini("market_data", "l2book_archive_enabled", "true" if bool(settings.get("l2book_archive_enabled")) else "false")
        save_ini("market_data", "l2book_archive_dir", str(settings.get("l2book_archive_dir") or "").strip())
        save_ini("tradfi_profiles", "tiingo_api_key", str(settings.get("tiingo_api_key") or "").strip())

    try:
        _touch_exchange_refresh_flag(ex)
    except Exception as exc:
        _log(SERVICE, f"Failed to queue {ex} latest 1m refresh after settings save: {exc}", level="WARNING")

    return _build_market_data_settings_payload(ex)


def _build_tradfi_symbol_map_payload() -> dict[str, Any]:
    rows = build_merged_tradfi_table()
    type_values = sorted(
        {
            str(row.get("canonical_type") or "").strip()
            for row in rows
            if str(row.get("canonical_type") or "").strip()
        }
    )
    return {
        "rows": rows,
        "type_values": type_values,
        "status_values": list(TRADFI_STATUSES_SELECTABLE),
        "canonical_types": list(TRADFI_CANONICAL_TYPES),
        "statuses": list(TRADFI_STATUSES),
        "meta_cache_info": build_tiingo_meta_cache_info(),
        "quote_cache_info": build_tradfi_quote_cache_info(),
        "spec_cache_info": build_xyz_spec_cache_info(),
    }


def _require_tiingo_api_key(body: dict[str, Any]) -> str:
    api_key = str((body or {}).get("api_key") or "").strip()
    if not api_key:
        raise ValueError("Tiingo API key is empty.")
    return api_key


@router.get("/settings/{exchange}")
def get_market_data_settings(exchange: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        return _build_market_data_settings_payload(exchange)
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "exchange": _normalize_settings_exchange(exchange),
        }


@router.post("/settings/{exchange}")
def save_market_data_settings(exchange: str, request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        _, meta = _get_exchange_settings_meta(exchange)
        payload = _save_market_data_settings(exchange, request)
        return {
            "success": True,
            "message": meta["save_message"],
            "settings": payload,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/settings/hyperliquid/tradfi-map")
def get_market_data_tradfi_map(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        return {
            "success": True,
            "payload": _build_tradfi_symbol_map_payload(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map")
def save_market_data_tradfi_map(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        entry = body.get("entry") if isinstance(body.get("entry"), dict) else body
        saved_entry = upsert_tradfi_map_entry(entry)
        payload = _build_tradfi_symbol_map_payload()
        payload["selected_xyz_coin"] = str(saved_entry.get("xyz_coin") or "")
        return {
            "success": True,
            "message": "TradFi symbol mapping saved.",
            "payload": payload,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/search-ticker")
def search_market_data_tradfi_ticker(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        api_key = _require_tiingo_api_key(body)
        xyz_coin = str(body.get("xyz_coin") or "").strip().upper()
        query = str(body.get("query") or xyz_coin).strip()
        if not query:
            raise ValueError("Search query is empty.")
        results = tiingo_search(query=query, api_key=api_key)
        tickers = [
            str(item.get("ticker") or "").strip().upper()
            for item in results[:10]
            if isinstance(item, dict) and str(item.get("ticker") or "").strip()
        ]
        price_map = build_tiingo_search_price_map(api_key=api_key, tickers=tickers)
        normalized_results = [
            {
                "ticker": ticker,
                "name": str(item.get("name") or "").strip(),
                "asset_type": str(item.get("assetType") or "").strip(),
                "is_active": bool(item.get("isActive", False)),
                "tiingo_price": price_info.get("price"),
                "tiingo_price_timestamp": str(price_info.get("quote_timestamp") or "").strip(),
                "tiingo_price_source": str(price_info.get("source") or "").strip(),
            }
            for item in results[:10]
            for ticker in [str(item.get("ticker") or "").strip().upper()]
            if ticker
            for price_info in [price_map.get(ticker) if isinstance(price_map.get(ticker), dict) else {}]
        ]
        price_count = sum(1 for item in normalized_results if item.get("tiingo_price") not in (None, ""))
        return {
            "success": True,
            "message": (
                f"Found {len(normalized_results)} Tiingo ticker matches."
                + (f" Tiingo prices available for {price_count}." if price_count else "")
            ),
            "results": normalized_results,
            "xyz_coin": xyz_coin,
            "query": query,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/test-resolve")
def test_market_data_tradfi_resolve(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        xyz_coin = str(body.get("xyz_coin") or "").strip().upper()
        if not xyz_coin:
            raise ValueError("xyz_coin is empty.")
        tiingo_ticker, tiingo_fx_ticker, tiingo_fx_invert, tiingo_start_date = resolve_tradfi_symbol(xyz_coin)
        row = find_tradfi_row(xyz_coin)
        return {
            "success": True,
            "result": {
                "xyz_coin": xyz_coin,
                "tiingo_ticker": tiingo_ticker,
                "tiingo_fx_ticker": tiingo_fx_ticker,
                "tiingo_fx_invert": tiingo_fx_invert,
                "tiingo_start_date": str(tiingo_start_date) if tiingo_start_date else None,
                "entry_status": str((row or {}).get("status") or ""),
            },
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/fetch-start-date")
def fetch_market_data_tradfi_start_date(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        api_key = _require_tiingo_api_key(body)
        xyz_coin = str(body.get("xyz_coin") or "").strip().upper()
        if not xyz_coin:
            raise ValueError("xyz_coin is empty.")
        row = find_tradfi_row(xyz_coin)
        result = update_tiingo_start_date_for_selected(selected_entry=row, api_key=api_key)
        payload = _build_tradfi_symbol_map_payload()
        payload["selected_xyz_coin"] = xyz_coin
        return {
            "success": True,
            "message": "TradFi start date processed.",
            "result": result,
            "payload": payload,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/fetch-all-start-dates")
def fetch_market_data_tradfi_all_start_dates(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        api_key = _require_tiingo_api_key(body)
        result = update_tiingo_start_dates_for_all(api_key=api_key, rows=build_merged_tradfi_table())
        return {
            "success": True,
            "message": "TradFi start dates processed.",
            "result": result,
            "payload": _build_tradfi_symbol_map_payload(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/spec-refresh")
def refresh_market_data_tradfi_specs(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        instruments = fetch_xyz_spec()
        return {
            "success": True,
            "message": f"XYZ specification cache refreshed: {len(instruments):,} instruments.",
            "payload": _build_tradfi_symbol_map_payload(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/auto-map")
def auto_map_market_data_tradfi(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        api_key = _require_tiingo_api_key(body)
        result = auto_map_tradfi(api_key=api_key, force_meta_refresh=False)
        return {
            "success": True,
            "message": "TradFi Auto-Map completed.",
            "result": result,
            "payload": _build_tradfi_symbol_map_payload(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/refresh-metadata")
def refresh_market_data_tradfi_metadata(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        api_key = _require_tiingo_api_key(body)
        meta = fetch_tiingo_meta(api_key=api_key, force_refresh=True)
        return {
            "success": True,
            "message": f"Tiingo metadata refreshed: {len(meta):,} tickers loaded.",
            "result": {"count": len(meta)},
            "payload": _build_tradfi_symbol_map_payload(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tradfi-map/refresh-prices")
def refresh_market_data_tradfi_prices(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        api_key = _require_tiingo_api_key(body)
        result = refresh_tradfi_quote_cache(api_key=api_key, records=build_merged_tradfi_table())
        return {
            "success": True,
            "message": "TradFi quote cache refreshed.",
            "result": result,
            "payload": _build_tradfi_symbol_map_payload(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/settings/hyperliquid/tradfi-map/specs")
def get_market_data_tradfi_specs(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    try:
        return {
            "success": True,
            "payload": build_xyz_spec_rows(),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/settings/hyperliquid/tiingo-probe")
def test_market_data_tiingo(request: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    api_key = str((request or {}).get("api_key") or "").strip()
    ticker = str((request or {}).get("ticker") or "AAPL").strip().upper() or "AAPL"
    if not api_key:
        return {"success": False, "error": "Tiingo API key is empty."}

    try:
        probe = probe_tiingo_iex_1m(api_key=api_key, ticker=ticker, timeout_s=20.0)
        usage = get_tiingo_runtime_usage(api_key=api_key)
        return {
            "success": True,
            "message": f"Tiingo connection OK: status={probe.get('status', 200)} message={probe.get('message', '')}",
            "probe": probe,
            "usage": usage,
        }
    except Exception as e:
        return {"success": False, "error": f"Tiingo test failed: {e}"}


def _load_market_data_status() -> dict:
    """Return the latest market data status snapshot kept in API memory."""
    return dict(_market_data_status_snapshot)


@router.post("/internal/status")
async def update_market_data_status_snapshot(request: Request) -> dict[str, Any]:
    """Accept PBData market-data status from localhost and keep it in memory."""
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(status_code=403, detail="Internal endpoint")
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid market data status payload")
    global _market_data_status_snapshot
    _market_data_status_snapshot = dict(body)
    return {"ok": True}


def _filter_status_coins_to_enabled(exchange: str, exchange_status: dict[str, Any]) -> dict[str, Any]:
    status = dict(exchange_status) if isinstance(exchange_status, dict) else {}
    coins = status.get("coins")
    if not isinstance(coins, dict):
        status["coins"] = {}
        return status

    try:
        cfg = load_market_data_config()
        enabled_coins, _, _ = get_effective_enabled_coins(exchange, cfg=cfg)
        enabled = {
            str(coin).strip().upper()
            for coin in (enabled_coins or [])
            if str(coin).strip()
        }
    except Exception:
        return status

    status["coins"] = {
        str(coin).strip().upper(): coin_status
        for coin, coin_status in coins.items()
        if str(coin).strip().upper() in enabled and isinstance(coin_status, dict)
    }
    status["coins_total"] = len(enabled)

    current_coin = str(status.get("current_coin") or "").strip().upper()
    if current_coin and current_coin not in enabled:
        status["current_coin"] = ""

    try:
        status["coins_done"] = min(int(status.get("coins_done", 0)), len(enabled))
    except Exception:
        status["coins_done"] = 0

    return status


def _get_exchange_status_key(exchange: str) -> str:
    """Map exchange name to status key."""
    exchange = exchange.lower().strip()
    if exchange in ("binance", "binanceusdm"):
        return "binance_latest_1m"
    elif exchange == "bybit":
        return "bybit_latest_1m"
    elif exchange == "okx":
        return "okx_latest_1m"
    elif exchange == "bitget":
        return "bitget_latest_1m"
    elif exchange == "hyperliquid":
        return "latest_1m"
    return ""


def _get_exchange_flag_prefix(exchange: str) -> str:
    """Map exchange name to flag file prefix."""
    exchange = exchange.lower().strip()
    if exchange in ("binance", "binanceusdm"):
        return "binance_latest_1m"
    elif exchange == "bybit":
        return "bybit_latest_1m"
    elif exchange == "okx":
        return "okx_latest_1m"
    elif exchange == "bitget":
        return "bitget_latest_1m"
    elif exchange == "hyperliquid":
        return "hyperliquid_latest_1m"
    return ""


def _touch_exchange_refresh_flag(exchange: str) -> None:
    """Wake the PBData latest-1m loop for an exchange."""
    flag_prefix = _get_exchange_flag_prefix(exchange)
    if not flag_prefix:
        return
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    flag_path.parent.mkdir(parents=True, exist_ok=True)
    flag_path.touch()


def _render_market_data_status_html(request: Request, token: str, exchange: str) -> str:
    exchange_param = str(exchange or "").strip().lower()
    html_path = PBGDIR / "frontend" / "market_data_status.html"
    html_content = html_path.read_text(encoding="utf-8")

    browser_origin = _get_request_origin(request)
    api_host_str = request.url.netloc or request.headers.get("host", "127.0.0.1")
    api_base_str = browser_origin + "/api"

    instance_id = f"mds_fastapi_{exchange_param}".replace("-", "_")
    html_content = html_content.replace("__MDS_ROOT_ID__", instance_id)
    html_content = html_content.replace("__MDS_ID__", f"{instance_id}_")

    html_content = html_content.replace(
        'data-token=""', f'data-token="{token}"'
    ).replace(
        'data-exchange=""', f'data-exchange="{exchange_param}"'
    ).replace(
        'data-api-host=""', f'data-api-host="{api_host_str}"'
    ).replace(
        'data-api-base=""', f'data-api-base="{api_base_str}"'
    )
    return html_content


def _render_hl_data_actions_html(request: Request, token: str, initial_section: str = "") -> str:
    html_path = PBGDIR / "frontend" / "hl_data_actions.html"
    html_content = html_path.read_text(encoding="utf-8")

    browser_origin = _get_request_origin(request)
    api_host_str = request.url.netloc or request.headers.get("host", "127.0.0.1")
    api_base_str = browser_origin + "/api"

    instance_id = "hlda_fastapi_market_data"
    html_content = html_content.replace("__HLDA_ROOT__", instance_id)
    html_content = html_content.replace("__HLDA__", f"{instance_id}_")

    html_content = html_content.replace(
        'data-token=""', f'data-token="{token}"'
    ).replace(
        'data-api-base=""', f'data-api-base="{api_base_str}"'
    ).replace(
        'data-api-host=""', f'data-api-host="{api_host_str}"'
    ).replace(
        'data-initial-section=""', f'data-initial-section="{initial_section}"'
    )
    return html_content


BEST_1M_EXCHANGES: dict[str, dict[str, str]] = {
    "binance": {
        "label": "Binance USDM",
        "job_type": "binance_best_1m",
        "queue_exchange": "binanceusdm",
        "description": (
            "Downloads full 1m OHLCV history from the Binance archive "
            "(monthly and daily ZIPs) and backfills gaps via CCXT."
        ),
        "hint": (
            "Leave the coin list empty to queue all available Binance USDM coins. "
            "Use refetch only when you need to rebuild corrupted days from scratch."
        ),
        "refetch_label": "Refetch all days from scratch",
    },
    "bybit": {
        "label": "Bybit",
        "job_type": "bybit_best_1m",
        "queue_exchange": "bybit",
        "description": (
            "Downloads raw trade history from public.bybit.com and aggregates it to "
            "1m OHLCV, then tops up the last days via CCXT REST."
        ),
        "hint": (
            "Leave the coin list empty to queue all available Bybit coins. "
            "Use refetch only when you need to overwrite broken local days."
        ),
        "refetch_label": "Refetch all days from scratch",
    },
    "okx": {
        "label": "OKX",
        "job_type": "okx_best_1m",
        "queue_exchange": "okx",
        "description": (
            "Downloads OKX USDT-SWAP 1m OHLCV history from OKX public daily "
            "archives, enriches missing archive volume from REST, and repairs gaps via REST."
        ),
        "hint": (
            "Leave the coin list empty to queue all available OKX coins. "
            "Only OKX USDT-SWAP perpetual markets are used."
        ),
        "refetch_label": "Refetch all days from scratch",
    },
    "bitget": {
        "label": "Bitget",
        "job_type": "bitget_best_1m",
        "queue_exchange": "bitget",
        "description": (
            "Downloads Bitget USDT-FUTURES 1m OHLCV history via public REST "
            "and repairs missing days from the same REST source."
        ),
        "hint": (
            "Leave the coin list empty to queue all available Bitget USDT-FUTURES coins. "
            "Use refetch only when you need to overwrite broken local days."
        ),
        "refetch_label": "Refetch all days from scratch",
    },
}

COPY_DATA_EXCHANGES: dict[str, dict[str, str]] = {
    "binance": {"label": "Binance USDM", "storage": "binanceusdm"},
    "bybit": {"label": "Bybit", "storage": "bybit"},
    "bitget": {"label": "Bitget", "storage": "bitget"},
    "okx": {"label": "OKX", "storage": "okx"},
    "hyperliquid": {"label": "Hyperliquid", "storage": "hyperliquid"},
}

COPY_DATA_MODE = "update"
COPY_DATA_TARGET_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-@")
COPY_DATA_REMOTE_PATH_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/._-")


def _load_bitget_distributed_hosts() -> list[dict[str, str]]:
    """Load known VPS hosts that can run distributed Bitget backfill jobs."""

    hosts_root = PBGDIR / "data" / "vpsmanager" / "hosts"
    out: list[dict[str, str]] = [
        {
            "hostname": "master",
            "label": "Master (local downloader)",
            "target": "master",
            "ssh_command": "",
            "mode": "master",
        }
    ]
    if not hosts_root.is_dir():
        return out

    for path in sorted(hosts_root.glob("*/*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        hostname = str(raw.get("_hostname") or path.stem).strip()
        user = str(raw.get("user") or "").strip()
        ssh_host = str(raw.get("ip") or hostname).strip()
        if not hostname or not ssh_host:
            continue
        try:
            # Use the known VPS hostname/SSH alias as the command target. The raw
            # IP can have a different host-key entry and fail strict SSH checks.
            target = _normalize_copy_data_target(f"{user}@{hostname}" if user else hostname)
            port = int(raw.get("firewall_ssh_port") or 22)
            if port < 1 or port > 65535:
                port = 22
            ssh_args = ["ssh"] if port == 22 else ["ssh", "-p", str(port)]
            out.append(
                {
                    "hostname": hostname,
                    "label": f"{hostname} ({target}, ip={ssh_host})",
                    "target": target,
                    "ssh_command": shlex.join(ssh_args),
                    "mode": "ssh",
                }
            )
        except Exception:
            continue
    return out


def _select_bitget_distributed_hosts(raw_hosts: Any) -> list[dict[str, str]]:
    """Resolve requested distributed hostnames against known VPS host config."""

    known = {str(item.get("hostname") or "").strip(): item for item in _load_bitget_distributed_hosts()}
    if not isinstance(raw_hosts, list):
        raw_hosts = []
    selected: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_item in raw_hosts:
        if isinstance(raw_item, dict):
            hostname = str(raw_item.get("hostname") or "").strip()
        else:
            hostname = str(raw_item or "").strip()
        if not hostname or hostname in seen:
            continue
        host = known.get(hostname)
        if not host:
            raise ValueError(f"Unknown or unsupported Bitget downloader: {hostname}")
        selected.append(dict(host))
        seen.add(hostname)
    if not selected:
        raise ValueError("Select at least one VPS host for distributed Bitget backfill.")
    return selected


def _best_1m_exchange_meta(exchange: str) -> dict[str, str] | None:
    return BEST_1M_EXCHANGES.get(_normalize_settings_exchange(exchange))


def _get_best_1m_available_coins(exchange: str) -> list[str]:
    """Return all available coins for manual Best 1m builds."""

    return [str(coin).strip().upper() for coin in get_market_data_coin_options(exchange) if str(coin).strip()]


def _normalize_best_1m_request_coin(coin: str, *, available_coins: list[str] | None = None) -> str:
    raw = str(coin or "").strip().upper()
    if not raw:
        return ""
    available = {str(item or "").strip().upper() for item in (available_coins or []) if str(item or "").strip()}
    if raw == "ALL" or raw in available:
        return raw
    normalized = str(coin_from_symbol_code(raw) or "").strip().upper()
    if available and normalized in available:
        return normalized
    return normalized or raw


def _normalize_copy_data_exchange(exchange: str) -> str:
    """Return the supported Copy Data exchange key for a request value."""

    ex = _normalize_settings_exchange(exchange)
    if ex == "binanceusdm":
        ex = "binance"
    return ex


def _normalize_copy_data_exchanges(raw_exchanges: Any) -> list[str]:
    """Validate and de-duplicate Copy Data exchange selections."""

    if not isinstance(raw_exchanges, list):
        raw_exchanges = []
    exchanges: list[str] = []
    for value in raw_exchanges:
        ex = _normalize_copy_data_exchange(str(value or ""))
        if not ex:
            continue
        if ex not in COPY_DATA_EXCHANGES:
            raise ValueError(f"Unsupported exchange for Copy Data: {value}")
        if ex not in exchanges:
            exchanges.append(ex)
    if not exchanges:
        raise ValueError("Select at least one exchange to copy.")
    return exchanges


def _normalize_copy_data_target(target: Any) -> str:
    """Validate the remote rsync target host."""

    text = str(target or "").strip()
    if not text:
        raise ValueError("Remote target is required.")
    if any(ch.isspace() for ch in text) or any(ch in text for ch in ("/", "\\", "\x00", ":")):
        raise ValueError("Remote target must be a host or user@host without spaces, slashes, or a path.")
    if any(ch not in COPY_DATA_TARGET_CHARS for ch in text):
        raise ValueError("Remote target contains unsupported characters.")
    if text in (".", ".."):
        raise ValueError("Remote target is invalid.")
    return text


def _normalize_copy_data_destination_root(destination_root: Any) -> str:
    """Validate the target-side absolute data/ohlcv root path."""

    text = str(destination_root or "").strip()
    if not text:
        text = str((PBGDIR / "data" / "ohlcv").resolve())
    if "\x00" in text or "\n" in text or "\r" in text or any(ch.isspace() for ch in text):
        raise ValueError("Destination root must not contain whitespace or control characters.")
    if not text.startswith("/"):
        raise ValueError("Destination root must be an absolute path on the target host.")
    if any(ch not in COPY_DATA_REMOTE_PATH_CHARS for ch in text):
        raise ValueError("Destination root contains unsupported characters.")
    return text.rstrip("/") or "/"


def _parse_copy_data_ssh_command(ssh_command: Any) -> list[str]:
    """Parse and validate the SSH command used as rsync's remote shell."""

    text = str(ssh_command or "").strip() or "ssh"
    try:
        parts = shlex.split(text)
    except ValueError as exc:
        raise ValueError(f"Invalid SSH command: {exc}") from exc
    if not parts:
        parts = ["ssh"]
    if Path(parts[0]).name != "ssh":
        raise ValueError("SSH command must start with ssh.")
    return parts


def _build_copy_data_queue_payload(request: dict[str, Any]) -> dict[str, Any]:
    """Build the sanitized task-queue payload for an OHLCV copy job."""

    payload = _build_copy_data_test_payload(request)
    exchanges = _normalize_copy_data_exchanges(request.get("exchanges"))
    payload.update(
        {
            "exchanges": exchanges,
            "exchange_storage": {ex: COPY_DATA_EXCHANGES[ex]["storage"] for ex in exchanges},
            "mode": COPY_DATA_MODE,
        }
    )
    return payload


def _queue_copy_data_job_response(request: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    """Queue a Copy Data job and start the worker when needed."""

    import subprocess
    import sys

    from market_data import append_exchange_download_log
    from task_queue import enqueue_running_job, move_job_file, update_job_file

    try:
        payload = _build_copy_data_queue_payload(request)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    dry_run = bool(dry_run)
    if dry_run:
        payload["dry_run"] = True

    exchanges = list(payload.get("exchanges") or [])
    labels = [COPY_DATA_EXCHANGES[ex]["label"] for ex in exchanges if ex in COPY_DATA_EXCHANGES]
    job_type = "ohlcv_copy_dry_run" if dry_run else "ohlcv_copy"
    action_label = "dry run" if dry_run else "copy"
    try:
        job = enqueue_running_job(
            job_type=job_type,
            exchange="ohlcv",
            payload=payload,
            manual_parallel=True,
        )
    except Exception as exc:
        return {"success": False, "error": f"Failed to enqueue OHLCV {action_label} job: {exc}"}

    append_exchange_download_log(
        "ohlcv",
        f"[{job_type}] queued job_id={job.job_id} target={payload['target']} exchanges={','.join(exchanges)} mode={payload['mode']}",
    )

    worker_started = False
    runner_started = False
    job_path = Path(job.path)
    try:
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py"), "--run-job", str(job_path)],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        runner_started = True
    except Exception as exc:
        try:
            update_job_file(job_path, mutate=lambda o: o.update({"status": "failed", "error": f"Failed to launch {action_label} worker: {exc}"}))
            move_job_file(job_path, "failed")
        except Exception:
            pass
        return {"success": False, "error": f"Failed to launch {action_label} worker: {exc}"}

    return {
        "success": True,
        "job_id": job.job_id,
        "job_type": job_type,
        "dry_run": dry_run,
        "target": payload["target"],
        "destination_root": payload["destination_root"],
        "exchanges": exchanges,
        "mode": payload["mode"],
        "worker_started": worker_started,
        "runner_started": runner_started,
        "message": (
            f"Queued OHLCV copy {'dry run ' if dry_run else ''}job {job.job_id} for {', '.join(labels) or len(exchanges)} "
            f"to {payload['target']}:{payload['destination_root']}."
        ),
    }


def _build_copy_data_test_payload(request: dict[str, Any]) -> dict[str, Any]:
    """Build the sanitized payload for a read-only Copy Data connection test."""

    if not isinstance(request, dict):
        raise ValueError("Invalid request payload.")

    target = _normalize_copy_data_target(request.get("target"))
    destination_root = _normalize_copy_data_destination_root(request.get("destination_root"))
    ssh_args = _parse_copy_data_ssh_command(request.get("ssh_command"))
    if len(ssh_args) > 1 and ssh_args[-1] == target:
        raise ValueError("SSH command must not include the target host. Put it only in Remote target.")

    return {
        "target": target,
        "destination_root": destination_root,
        "ssh_command": shlex.join(ssh_args),
    }


def _build_copy_data_ssh_test_command(payload: dict[str, Any], remote_args: list[str]) -> list[str]:
    """Build one read-only SSH probe command from a sanitized Copy Data payload."""

    ssh_args = _parse_copy_data_ssh_command(payload.get("ssh_command"))
    target = _normalize_copy_data_target(payload.get("target"))
    return list(ssh_args) + [target] + [str(part) for part in remote_args]


def _copy_data_remote_parent(path: str) -> str:
    """Return the POSIX parent path for a remote destination root."""

    parent = PurePosixPath(str(path or "/")).parent
    return str(parent) or "/"


def _run_copy_data_ssh_probe(cmd: list[str], *, timeout_s: float = 12.0) -> dict[str, Any]:
    """Run one read-only SSH probe command with a short timeout."""

    import subprocess

    try:
        proc = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=float(timeout_s),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "returncode": 124, "error": "SSH probe timed out."}
    except Exception as exc:
        return {"ok": False, "returncode": 1, "error": str(exc)}

    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    return {
        "ok": proc.returncode == 0,
        "returncode": int(proc.returncode),
        "stdout": stdout,
        "stderr": stderr,
        "error": stderr or stdout or f"SSH probe failed with exit code {proc.returncode}.",
    }


def _test_copy_data_connection_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Run read-only SSH checks for Copy Data target and destination path."""

    target = str(payload.get("target") or "").strip()
    destination_root = _normalize_copy_data_destination_root(payload.get("destination_root"))
    parent_root = _copy_data_remote_parent(destination_root)

    ping = _run_copy_data_ssh_probe(_build_copy_data_ssh_test_command(payload, ["printf", "PBGUI_COPY_TEST_OK"]))
    if not ping.get("ok"):
        return {
            "success": False,
            "target": target,
            "destination_root": destination_root,
            "message": "SSH connection test failed.",
            "detail": str(ping.get("error") or "SSH probe failed."),
            "checks": {"ssh": ping},
        }

    root_exists = _run_copy_data_ssh_probe(_build_copy_data_ssh_test_command(payload, ["test", "-d", destination_root]))
    root_writable = {"ok": False, "returncode": 1, "error": "Destination root does not exist."}
    parent_exists = {"ok": False, "returncode": 1, "error": "Not checked."}
    parent_writable = {"ok": False, "returncode": 1, "error": "Not checked."}

    if root_exists.get("ok"):
        root_writable = _run_copy_data_ssh_probe(_build_copy_data_ssh_test_command(payload, ["test", "-w", destination_root]))
        success = bool(root_writable.get("ok"))
        message = "SSH OK. Destination root exists and is writable." if success else "SSH OK, but destination root is not writable."
    else:
        parent_exists = _run_copy_data_ssh_probe(_build_copy_data_ssh_test_command(payload, ["test", "-d", parent_root]))
        if parent_exists.get("ok"):
            parent_writable = _run_copy_data_ssh_probe(_build_copy_data_ssh_test_command(payload, ["test", "-w", parent_root]))
        success = bool(parent_writable.get("ok"))
        message = (
            "SSH OK. Destination root does not exist yet, but its parent is writable so the copy job can create it."
            if success
            else "SSH OK, but destination root is missing and its parent is not writable."
        )

    return {
        "success": success,
        "target": target,
        "destination_root": destination_root,
        "destination_parent": parent_root,
        "message": message,
        "checks": {
            "ssh": ping,
            "root_exists": root_exists,
            "root_writable": root_writable,
            "parent_exists": parent_exists,
            "parent_writable": parent_writable,
        },
    }


INVENTORY_VIEW_META: dict[str, dict[str, Any]] = {
    "1m": {
        "label": "1m candles",
        "dataset": "1m",
        "read_only": False,
        "empty_message": "No 1m data found yet.",
    },
    "1m_api": {
        "label": "1m API",
        "dataset": "1m_api",
        "read_only": False,
        "empty_message": "No 1m_api data found yet.",
    },
    "l2Book": {
        "label": "l2Book",
        "dataset": "l2Book",
        "read_only": False,
        "empty_message": "No l2Book data found yet.",
    },
    "pb7_cache": {
        "label": "PB7 cache",
        "dataset": "pb7_cache",
        "read_only": True,
        "empty_message": "No PB7 cache files found for this exchange (expected path: pb7/caches/ohlcv/<exchange>/...).",
    },
}


def _inventory_views_for_exchange(exchange: str) -> list[dict[str, str]]:
    ex = _normalize_settings_exchange(exchange)
    view_keys = ["1m", "pb7_cache"]
    if ex == "hyperliquid":
        view_keys = ["1m", "1m_api", "l2Book", "pb7_cache"]
    return [
        {"key": key, "label": str(INVENTORY_VIEW_META[key]["label"])}
        for key in view_keys
    ]


def _normalize_inventory_view(view: str) -> str:
    value = str(view or "").strip()
    lower = value.lower()
    if lower in ("1m", "candles_1m"):
        return "1m"
    if lower in ("1m_api", "candles_1m_api"):
        return "1m_api"
    if lower in ("l2book", "l2book_mid"):
        return "l2Book"
    if lower in ("pb7 cache", "pb7_cache"):
        return "pb7_cache"
    return ""


def _require_inventory_view(exchange: str, view: str) -> str:
    normalized = _normalize_inventory_view(view)
    available = {item["key"] for item in _inventory_views_for_exchange(exchange)}
    if normalized not in available:
        raise ValueError("Unsupported inventory view for the selected exchange.")
    return normalized


def _inventory_storage_exchange(exchange: str) -> str:
    ex = _normalize_settings_exchange(exchange)
    return "binanceusdm" if ex == "binance" else ex


def _fmt_bytes(value: int | float | None) -> str:
    try:
        size = float(value or 0)
    except Exception:
        size = 0.0
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024.0 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"


def _inventory_extract_xyz_coin(coin: str) -> str:
    coin_value = str(coin or "").strip().upper()
    if coin_value.startswith("XYZ:") or coin_value.startswith("XYZ-"):
        coin_value = coin_value[4:].strip()
    if "_" in coin_value:
        coin_value = coin_value.split("_", 1)[0]
    if ":" in coin_value:
        coin_value = coin_value.split(":", 1)[0]
    return coin_value


def _inventory_tradfi_mapping_statuses() -> dict[str, str]:
    statuses: dict[str, str] = {}
    for key, raw_status in build_effective_tradfi_status_map().items():
        norm_key = _inventory_extract_xyz_coin(key)
        status = str(raw_status or "").strip().lower()
        if not norm_key:
            continue
        if status in ("ok", "alias"):
            statuses[norm_key] = "mapped"
        elif status:
            statuses[norm_key] = status
        else:
            statuses[norm_key] = "missing"
    return statuses


def _annotate_inventory_row(exchange: str, row: dict[str, Any], tradfi_mapping_statuses: dict[str, str] | None = None) -> dict[str, Any]:
    annotated = dict(row)
    coin = str(annotated.get("coin") or "").strip()
    coin_upper = coin.upper()
    is_stock = coin_upper.startswith("XYZ:") or coin_upper.startswith("XYZ-")
    xyz_coin = _inventory_extract_xyz_coin(coin) if is_stock else ""
    mapping_status = ""
    if is_stock and _normalize_settings_exchange(exchange) == "hyperliquid":
        mapping_status = str((tradfi_mapping_statuses or {}).get(xyz_coin) or "missing")
    has_mapping = mapping_status == "mapped"
    annotated["is_xyz"] = is_stock
    annotated["xyz_coin"] = xyz_coin
    annotated["has_mapping"] = has_mapping
    annotated["mapping_status"] = mapping_status
    return annotated


def _inventory_kind_matches(row: dict[str, Any], kind_filter: str) -> bool:
    mode = str(kind_filter or "all").strip().lower()
    coin_upper = str(row.get("coin") or "").strip().upper()
    is_stock = bool(row.get("is_xyz")) or coin_upper.startswith("XYZ:") or coin_upper.startswith("XYZ-")
    mapping_status = str(row.get("mapping_status") or "").strip().lower()
    if mode in ("stocks (xyz)", "xyz only"):
        return is_stock
    if mode == "xyz mapped":
        return is_stock and mapping_status == "mapped"
    if mode in ("xyz missing", "xyz not mapped"):
        return is_stock and mapping_status != "mapped"
    if mode == "crypto":
        return not is_stock
    return True


def _get_pb7_inventory_via_cache(exchange: str) -> list[dict[str, Any]]:
    import os
    from inventory_cache import get_inventory as _get_inventory

    ex = str(exchange or "").strip().lower()
    root = _get_pb7_root_dir()
    if root is None:
        return []
    base = root / "caches" / "ohlcv" / ex
    if not base.is_dir():
        return []
    try:
        timeframes = sorted(entry.name for entry in os.scandir(str(base)) if entry.is_dir())
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for timeframe in timeframes:
        for record in _get_inventory(ex, f"pb7_cache:{timeframe}"):
            rows.append({**record, "timeframe": timeframe})
    return rows


def _collect_inventory_rows(exchange: str, view: str) -> list[dict[str, Any]]:
    from inventory_cache import get_inventory as _get_inventory

    view_key = _require_inventory_view(exchange, view)
    storage_ex = _inventory_storage_exchange(exchange)
    tradfi_mapping_statuses = _inventory_tradfi_mapping_statuses() if _normalize_settings_exchange(exchange) == "hyperliquid" else {}

    if view_key == "pb7_cache":
        rows: list[dict[str, Any]] = []
        for record in _get_pb7_inventory_via_cache(_normalize_settings_exchange(exchange)):
            total_bytes = int(record.get("total_bytes", 0) or 0)
            timeframe = str(record.get("timeframe") or "").strip() or "1m"
            coin = str(record.get("coin") or "").strip()
            dataset = f"pb7_cache:{timeframe}"
            rows.append(
                _annotate_inventory_row(exchange, {
                    "row_id": f"{dataset}|{coin}",
                    "exchange": str(record.get("exchange") or "").strip(),
                    "dataset": dataset,
                    "coin": coin,
                    "timeframe": timeframe,
                    "n_files": int(record.get("n_files", 0) or 0),
                    "total_bytes": total_bytes,
                    "size": round(float(total_bytes) / (1024.0 * 1024.0), 2),
                    "oldest_day": str(record.get("oldest_day") or "").strip(),
                    "newest_day": str(record.get("newest_day") or "").strip(),
                    "n_days": int(record.get("n_days", 0) or 0),
                    "expected_hours": record.get("expected_hours", 0),
                    "coverage_pct": record.get("coverage_pct", 0),
                    "missing_days_count": int(record.get("missing_days_count", 0) or 0),
                    "missing_days_sample": str(record.get("missing_days_sample") or "").strip(),
                }, tradfi_mapping_statuses)
            )
        return rows

    dataset_name = str(INVENTORY_VIEW_META[view_key]["dataset"])
    is_hyperliquid = storage_ex == "hyperliquid"
    raw_rows = _get_inventory(
        storage_ex,
        dataset_name,
        lag_minutes=_get_missing_lag_minutes(storage_ex),
        tradfi_type_fn=tradfi_canonical_type_for_coin if is_hyperliquid else None,
        expected_minutes_fn=(
            (lambda tradfi_type, day: tradfi_expected_indices_for_type(day, tradfi_type))
            if is_hyperliquid else None
        ),
    )

    rows = []
    for record in raw_rows:
        total_bytes = int(record.get("total_bytes", 0) or 0)
        coin = str(record.get("coin") or "").strip()
        dataset = str(record.get("dataset") or dataset_name).strip() or dataset_name
        rows.append(
            _annotate_inventory_row(storage_ex, {
                "row_id": f"{dataset}|{coin}",
                "exchange": str(record.get("exchange") or storage_ex).strip(),
                "dataset": dataset,
                "coin": coin,
                "n_files": int(record.get("n_files", 0) or 0),
                "total_bytes": total_bytes,
                "size": round(float(total_bytes) / (1024.0 * 1024.0), 2),
                "oldest_day": str(record.get("oldest_day") or "").strip(),
                "newest_day": str(record.get("newest_day") or "").strip(),
                "n_days": int(record.get("n_days", 0) or 0),
                "expected_hours": record.get("expected_hours", 0),
                "coverage_pct": record.get("coverage_pct", 0),
                "missing_days_count": int(record.get("missing_days_count", 0) or 0),
                "missing_days_sample": str(record.get("missing_days_sample") or "").strip(),
                "hl_minutes": int(record.get("hl_minutes", 0) or 0) if is_hyperliquid else 0,
                "other_minutes": int(record.get("other_minutes", 0) or 0) if is_hyperliquid else 0,
                "missing_minutes": int(record.get("missing_minutes", 0) or 0) if is_hyperliquid else 0,
            }, tradfi_mapping_statuses)
        )
    return rows


def _inventory_include_missing_supported(exchange: str, view: str) -> bool:
    ex = _normalize_settings_exchange(exchange)
    view_key = _normalize_inventory_view(view)
    return ex == "hyperliquid" and view_key == "l2Book"


def _build_missing_inventory_rows(exchange: str, view: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not _inventory_include_missing_supported(exchange, view):
        return rows

    ex = _normalize_settings_exchange(exchange)
    view_key = _require_inventory_view(ex, view)
    dataset_name = str(INVENTORY_VIEW_META[view_key]["dataset"])
    storage_ex = _inventory_storage_exchange(ex)
    tradfi_mapping_statuses = _inventory_tradfi_mapping_statuses() if ex == "hyperliquid" else {}
    cfg = load_market_data_config()
    enabled_coins, _, _ = get_effective_enabled_coins(ex, cfg=cfg)

    existing_coins = {
        normalize_market_data_coin_dir(storage_ex, str(row.get("coin") or "").strip())
        for row in rows
        if str(row.get("coin") or "").strip()
    }

    augmented = list(rows)
    for coin in enabled_coins:
        coin_value = str(coin or "").strip()
        if not coin_value:
            continue
        coin_upper = coin_value.upper()
        if coin_upper.startswith("XYZ:") or coin_upper.startswith("XYZ-"):
            continue
        normalized_coin = normalize_market_data_coin_dir(storage_ex, coin_value)
        if not normalized_coin:
            continue
        if normalized_coin in existing_coins:
            continue
        augmented.append(
            _annotate_inventory_row(ex, {
                "row_id": f"{dataset_name}|{normalized_coin}",
                "exchange": storage_ex,
                "dataset": dataset_name,
                "coin": normalized_coin,
                "n_files": 0,
                "total_bytes": 0,
                "size": 0.0,
                "oldest_day": "",
                "newest_day": "",
                "n_days": 0,
                "expected_hours": 0,
                "coverage_pct": 0,
                "missing_days_count": 0,
                "missing_days_sample": "",
                "hl_minutes": 0,
                "other_minutes": 0,
                "missing_minutes": 0,
            }, tradfi_mapping_statuses)
        )
        existing_coins.add(normalized_coin)

    return augmented


def _filter_inventory_rows(rows: list[dict[str, Any]], coin_filter: str, kind_filter: str) -> list[dict[str, Any]]:
    coin_query = str(coin_filter or "").strip().upper()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        coin = str(row.get("coin") or "").strip()
        coin_upper = coin.upper()
        if coin_query and coin_query not in coin_upper:
            continue
        if not _inventory_kind_matches(row, kind_filter):
            continue
        filtered.append(row)
    return filtered


def _build_inventory_dataset_payload(
    exchange: str,
    view: str,
    coin_filter: str = "",
    kind_filter: str = "all",
    include_missing: bool = False,
) -> dict[str, Any]:
    ex = _normalize_settings_exchange(exchange)
    view_key = _require_inventory_view(ex, view)
    meta = INVENTORY_VIEW_META[view_key]
    all_rows = _collect_inventory_rows(ex, view_key)
    include_missing_supported = _inventory_include_missing_supported(ex, view_key)
    include_missing_current = bool(include_missing_supported and include_missing)
    if include_missing_current:
        all_rows = _build_missing_inventory_rows(ex, view_key, all_rows)
    filtered_rows = _filter_inventory_rows(all_rows, coin_filter=coin_filter, kind_filter=kind_filter)

    available_coins = sorted(
        {
            str(row.get("coin") or "").strip().upper()
            for row in all_rows
            if str(row.get("coin") or "").strip()
        }
    )

    metrics: list[dict[str, str]] = []
    if view_key == "pb7_cache":
        total_files = sum(int(row.get("n_files", 0) or 0) for row in all_rows)
        total_bytes = sum(int(row.get("total_bytes", 0) or 0) for row in all_rows)
        n_coins = len({str(row.get("coin") or "").strip() for row in all_rows if str(row.get("coin") or "").strip()})
        n_timeframes = len({str(row.get("timeframe") or "").strip() for row in all_rows if str(row.get("timeframe") or "").strip()})
        metrics = [
            {"label": "timeframes", "value": str(n_timeframes)},
            {"label": "coins", "value": str(n_coins)},
            {"label": "files", "value": str(total_files)},
            {"label": "size", "value": _fmt_bytes(total_bytes)},
        ]
    else:
        total_files = sum(int(row.get("n_files", 0) or 0) for row in all_rows)
        total_bytes = sum(int(row.get("total_bytes", 0) or 0) for row in all_rows)
        n_coins = len({str(row.get("coin") or "").strip() for row in all_rows if str(row.get("coin") or "").strip()})
        metrics = [
            {"label": "coins", "value": str(n_coins)},
            {"label": "files", "value": str(total_files)},
            {"label": "size", "value": _fmt_bytes(total_bytes)},
        ]

    return {
        "success": True,
        "exchange": ex,
        "view": view_key,
        "view_label": str(meta["label"]),
        "read_only": bool(meta["read_only"]),
        "empty_message": str(meta["empty_message"]),
        "rows": filtered_rows,
        "all_rows_count": len(all_rows),
        "available_coins": available_coins,
        "coin_filter": str(coin_filter or ""),
        "kind_filter": str(kind_filter or "all"),
        "include_missing_supported": include_missing_supported,
        "include_missing_current": include_missing_current,
        "metrics": metrics,
        "available_views": _inventory_views_for_exchange(ex),
        "helper_note": (
            "Read-only view of PB7 cache inventory from pb7/caches/ohlcv."
            if view_key == "pb7_cache"
            else "Click a row to display the heatmap."
        ),
    }


def _inventory_row_map(exchange: str, view: str) -> dict[str, dict[str, Any]]:
    rows = _collect_inventory_rows(exchange, view)
    by_coin: dict[str, dict[str, Any]] = {}
    for row in rows:
        coin = str(row.get("coin") or "").strip().upper()
        if coin and coin not in by_coin:
            by_coin[coin] = row
    return by_coin


def _remove_source_index_dirs_for_coin(storage_ex: str, actual_coin: str) -> int:
    src_dir = get_exchange_raw_root_dir(storage_ex) / "1m_src" / str(actual_coin).strip()
    if src_dir.exists():
        shutil.rmtree(src_dir)
        return 1
    return 0


def _rebuild_source_index_from_api_for_coin(storage_ex: str, actual_coin: str) -> tuple[int, int]:
    presence = get_minute_presence_for_dataset(storage_ex, "1m_api", str(actual_coin).strip())
    days = presence.get("days") if isinstance(presence, dict) else {}
    if not isinstance(days, dict) or not days:
        return (0, 0)

    days_written = 0
    minutes_written = 0
    for day_s, hours_map in days.items():
        if not isinstance(hours_map, dict):
            continue
        minute_indices: set[int] = set()
        for hour_s, mins_map in hours_map.items():
            try:
                hour_i = int(hour_s)
            except Exception:
                continue
            if hour_i < 0 or hour_i > 23 or not isinstance(mins_map, dict):
                continue
            for minute_k in mins_map.keys():
                try:
                    minute_i = int(minute_k)
                except Exception:
                    continue
                if 0 <= minute_i <= 59:
                    minute_indices.add((hour_i * 60) + minute_i)

        if minute_indices:
            update_source_index_for_day(
                exchange=storage_ex,
                coin=str(actual_coin).strip(),
                day=str(day_s),
                minute_indices=sorted(minute_indices),
                code=SOURCE_CODE_API,
            )
            days_written += 1
            minutes_written += len(minute_indices)

    return (days_written, minutes_written)


def _extract_inventory_file_day(file_name: str) -> str:
    stem = Path(str(file_name or "")).stem
    if len(stem) == 8 and stem.isdigit():
        return stem
    if len(stem) >= 8 and stem[:8].isdigit():
        return stem[:8]
    if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
        return stem.replace("-", "")
    return ""


def _resolve_inventory_rows_for_coins(exchange: str, view: str, coins: list[str]) -> list[dict[str, Any]]:
    row_map = _inventory_row_map(exchange, view)
    resolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for coin in coins:
        key = str(coin or "").strip().upper()
        if not key or key in seen or key not in row_map:
            continue
        seen.add(key)
        resolved.append(row_map[key])
    return resolved


def _build_inventory_delete_older_preview(exchange: str, view: str, coins: list[str], cutoff_day: str) -> dict[str, Any]:
    if not cutoff_day or len(cutoff_day) != 8 or not cutoff_day.isdigit():
        raise ValueError("Invalid cutoff date format (expected YYYYMMDD).")

    rows = _resolve_inventory_rows_for_coins(exchange, view, coins)
    if not rows:
        return {
            "success": True,
            "scope_label": "no coins selected",
            "would_delete_files": 0,
            "would_delete_size": 0,
            "affected_coins": [],
        }

    storage_ex = _inventory_storage_exchange(exchange)
    would_delete_files = 0
    would_delete_size = 0
    affected_coins: list[dict[str, Any]] = []

    for row in rows:
        actual_coin = str(row.get("coin") or "").strip()
        actual_dataset = str(row.get("dataset") or "").strip()
        coin_dir = get_exchange_raw_root_dir(storage_ex) / actual_dataset / actual_coin
        if not coin_dir.exists():
            continue
        coin_files = 0
        coin_size = 0
        try:
            for file_path in coin_dir.iterdir():
                if not file_path.is_file():
                    continue
                file_day = _extract_inventory_file_day(file_path.name)
                if file_day and file_day < cutoff_day:
                    coin_files += 1
                    coin_size += int(file_path.stat().st_size)
        except Exception:
            continue
        if coin_files > 0:
            would_delete_files += coin_files
            would_delete_size += coin_size
            affected_coins.append(
                {
                    "coin": actual_coin.upper(),
                    "files": coin_files,
                    "size": coin_size,
                    "size_label": _fmt_bytes(coin_size),
                }
            )

    affected_coins.sort(key=lambda item: int(item.get("size", 0) or 0), reverse=True)
    scope_label = str(rows[0].get("coin") or "").upper() if len(rows) == 1 else f"{len(rows)} selected coins"
    return {
        "success": True,
        "scope_label": scope_label,
        "would_delete_files": would_delete_files,
        "would_delete_size": would_delete_size,
        "would_delete_size_label": _fmt_bytes(would_delete_size),
        "affected_coins": affected_coins,
    }


def _load_ohlcv_from_npz_range(*, exchange: str, dataset: str, coin: str, start_day: str, end_day: str):
    import numpy as np
    import pandas as pd
    from market_data import _parse_day_hour_from_filename

    base = get_exchange_raw_root_dir(exchange) / str(dataset) / str(coin)
    if not base.is_dir():
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    frames: list[Any] = []
    for path in sorted(base.glob("*.npz")):
        parsed = _parse_day_hour_from_filename(path.name)
        day_s = parsed[0] if isinstance(parsed, tuple) else parsed
        if not day_s:
            continue
        if start_day and day_s < start_day:
            continue
        if end_day and day_s > end_day:
            continue
        try:
            with np.load(path) as data:
                arr = data["candles"] if "candles" in data else (data[data.files[0]] if data.files else None)
            if arr is None or len(arr) == 0:
                continue
            names = list(getattr(arr, "dtype", object()).names or [])
            ts_key = "ts" if "ts" in names else ("t" if "t" in names else None)
            o_key = "o" if "o" in names else ("open" if "open" in names else None)
            h_key = "h" if "h" in names else ("high" if "high" in names else None)
            l_key = "l" if "l" in names else ("low" if "low" in names else None)
            c_key = "c" if "c" in names else ("close" if "close" in names else None)
            v_key = "v" if "v" in names else ("bv" if "bv" in names else ("volume" if "volume" in names else None))
            if ts_key and o_key and h_key and l_key and c_key:
                frame = pd.DataFrame(
                    {
                        "ts": arr[ts_key].astype("int64", copy=False),
                        "o": arr[o_key].astype("float64", copy=False),
                        "h": arr[h_key].astype("float64", copy=False),
                        "l": arr[l_key].astype("float64", copy=False),
                        "c": arr[c_key].astype("float64", copy=False),
                        "v": arr[v_key].astype("float64", copy=False) if v_key else 0.0,
                    }
                )
            else:
                arr2 = np.asarray(arr)
                if arr2.ndim != 2 or arr2.shape[1] < 5:
                    continue
                frame = pd.DataFrame(
                    {
                        "ts": arr2[:, 0].astype("int64", copy=False),
                        "o": arr2[:, 1].astype("float64", copy=False),
                        "h": arr2[:, 2].astype("float64", copy=False),
                        "l": arr2[:, 3].astype("float64", copy=False),
                        "c": arr2[:, 4].astype("float64", copy=False),
                        "v": arr2[:, 5].astype("float64", copy=False) if arr2.shape[1] > 5 else 0.0,
                    }
                )
            frames.append(frame)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["ts", "o", "h", "l", "c"])
    out["ts"] = out["ts"].astype("int64", copy=False)
    return out.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)


def _load_ohlcv_from_pb7_cache(*, exchange: str, timeframe: str, coin: str, start_day: str, end_day: str):
    import numpy as np
    import pandas as pd
    from market_data import _parse_pb7_cache_day_from_name

    root = _get_pb7_root_dir()
    if root is None:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])
    tf = str(timeframe or "1m").strip() or "1m"
    base = root / "caches" / "ohlcv" / str(exchange) / tf / str(coin)
    if not base.is_dir():
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    frames: list[Any] = []
    for path in sorted(base.glob("*.npy")):
        day_s = _parse_pb7_cache_day_from_name(path.name)
        if not day_s:
            continue
        if start_day and day_s < start_day:
            continue
        if end_day and day_s > end_day:
            continue
        try:
            arr = np.load(path)
            if len(arr) == 0:
                continue
            names = list(getattr(arr, "dtype", object()).names or [])
            ts_key = "ts" if "ts" in names else None
            o_key = "o" if "o" in names else None
            h_key = "h" if "h" in names else None
            l_key = "l" if "l" in names else None
            c_key = "c" if "c" in names else None
            v_key = "bv" if "bv" in names else ("v" if "v" in names else None)
            if not (ts_key and o_key and h_key and l_key and c_key):
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "ts": arr[ts_key].astype("int64", copy=False),
                        "o": arr[o_key].astype("float64", copy=False),
                        "h": arr[h_key].astype("float64", copy=False),
                        "l": arr[l_key].astype("float64", copy=False),
                        "c": arr[c_key].astype("float64", copy=False),
                        "v": arr[v_key].astype("float64", copy=False) if v_key else 0.0,
                    }
                )
            )
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["ts", "o", "h", "l", "c"])
    out["ts"] = out["ts"].astype("int64", copy=False)
    return out.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)


def _resample_ohlcv(df: Any, rule: str):
    import pandas as pd

    if df is None or df.empty or not rule or str(rule).strip() == "1min":
        return df
    frame = df.copy()
    frame["dt"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
    frame = frame.set_index("dt").sort_index()
    agg = frame.resample(str(rule)).agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna(subset=["o", "h", "l", "c"])
    if agg.empty:
        return df
    agg = agg.reset_index()
    agg["ts"] = (agg["dt"].astype("int64") // 1_000_000)
    return agg[["ts", "o", "h", "l", "c", "v"]]


def _df_to_columnar(df: Any) -> dict[str, list[Any]]:
    if df is None or df.empty:
        return {"ts": [], "o": [], "h": [], "l": [], "c": [], "v": []}
    return {
        "ts": [int(value) for value in df["ts"]],
        "o": [round(float(value), 8) for value in df["o"]],
        "h": [round(float(value), 8) for value in df["h"]],
        "l": [round(float(value), 8) for value in df["l"]],
        "c": [round(float(value), 8) for value in df["c"]],
        "v": [round(float(value), 4) for value in df["v"]],
    }


_OHLCV_ZOOM_TFS = ["1d", "1h", "15m", "5m", "1m"]
_OHLCV_ZOOM_TF_RANK = {tf: idx for idx, tf in enumerate(_OHLCV_ZOOM_TFS)}
_OHLCV_ZOOM_WINDOW_LIMITS_MS: dict[str, tuple[int, int]] = {
    "15m": (7 * 86400_000, 30 * 86400_000),
    "5m": (2 * 86400_000, 10 * 86400_000),
    "1m": (6 * 3600_000, 3 * 86400_000),
}


def _get_ohlcv_source_timeframe(dataset: str) -> str:
    ds_l = str(dataset or "").strip().lower()
    if ds_l.startswith("pb7_cache:"):
        return str(ds_l.split(":", 1)[1] if ":" in ds_l else "1m").strip() or "1m"
    if ds_l in {"1m", "1m_api"}:
        return "1m"
    return "1h"


def _get_ohlcv_supported_zoom_tfs(dataset: str) -> list[str]:
    source_tf = _get_ohlcv_source_timeframe(dataset)
    if source_tf == "1m":
        return ["15m", "5m", "1m"]
    if source_tf == "5m":
        return ["15m", "5m"]
    if source_tf == "15m":
        return ["15m"]
    return []


def _build_ohlcv_initial_layers(ohlcv_df: Any) -> dict[str, dict[str, list[Any]]]:
    return {
        "1d": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1D")),
        "1h": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1h")),
    }


def _build_ohlcv_zoom_layers(ohlcv_df: Any, dataset: str, need_tf: str) -> dict[str, dict[str, list[Any]]]:
    target_tf = str(need_tf or "").strip().lower()
    supported_tfs = _get_ohlcv_supported_zoom_tfs(dataset)
    if target_tf not in supported_tfs:
        return {}
    if target_tf == "1m":
        layer_df = ohlcv_df
    elif target_tf == "5m":
        layer_df = _resample_ohlcv(ohlcv_df, "5min")
    elif target_tf == "15m":
        layer_df = _resample_ohlcv(ohlcv_df, "15min")
    else:
        return {}
    layer = _df_to_columnar(layer_df)
    return {target_tf: layer} if layer.get("ts") else {}


def _get_ohlcv_chart_coin_label(coin: str) -> str:
    label = str(coin or "")
    label_upper = label.upper()
    if label_upper.startswith("XYZ:") or label_upper.startswith("XYZ-"):
        label = label[4:]
    for suffix in ("_USDC:USDC", "_USDT:USDT", "_USDC_USDC", "_USDT_USDT", "/USDC:USDC", "/USDT:USDT"):
        if label.upper().endswith(suffix):
            return label[: -len(suffix)]
    return label


def _get_ohlcv_chart_split_dates(coin: str, layers: dict[str, dict[str, list[Any]]]) -> list[dict[str, Any]]:
    coin_upper = str(coin or "").upper()
    if not (coin_upper.startswith("XYZ:") or coin_upper.startswith("XYZ-")):
        return []
    try:
        from hyperliquid_best_1m import _load_split_factors_from_cache
    except Exception:
        return []

    tail = coin_upper[4:].strip()
    for suffix in ("_USDC:USDC", "_USDT:USDT", "_USDC_USDC", "_USDT_USDT", "/USDC:USDC", "/USDT:USDT"):
        if tail.endswith(suffix):
            tail = tail[: -len(suffix)]
            break
    ticker = tail.strip(" _:-")
    if not ticker:
        return []
    try:
        splits = _load_split_factors_from_cache(ticker)
    except Exception:
        return []
    if not splits:
        return []

    earliest_date = ""
    one_day = layers.get("1d") or {}
    if one_day.get("ts"):
        earliest_ts = min(int(value) for value in one_day["ts"])
        earliest_date = _datetime.utcfromtimestamp(earliest_ts / 1000.0).strftime("%Y-%m-%d")
    return [
        {"date": str(day), "factor": factor}
        for day, factor in splits
        if not earliest_date or str(day) >= earliest_date
    ]


def _get_ohlcv_zoom_window_ms(range_start: str, range_end: str, need_tf: str) -> tuple[int, int]:
    import pandas as pd

    start_ts = pd.Timestamp(str(range_start or ""))
    end_ts = pd.Timestamp(str(range_end or ""))
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    start_ms = int(start_ts.value // 1_000_000)
    end_ms = int(end_ts.value // 1_000_000)
    lo_ms = min(start_ms, end_ms)
    hi_ms = max(start_ms, end_ms)
    span_ms = max(1, hi_ms - lo_ms)
    center_ms = lo_ms + (span_ms / 2.0)
    tf_key = str(need_tf or "").strip().lower()
    min_window_ms, max_window_ms = _OHLCV_ZOOM_WINDOW_LIMITS_MS.get(tf_key, (2 * 86400_000, 14 * 86400_000))
    full_window_ms = max(span_ms * 3.0, float(min_window_ms))
    full_window_ms = min(full_window_ms, float(max_window_ms))
    half_window_ms = full_window_ms / 2.0
    return int(center_ms - half_window_ms), int(center_ms + half_window_ms)


def _ms_to_ohlcv_day(ms: int) -> str:
    return _datetime.utcfromtimestamp(int(ms) / 1000.0).strftime("%Y%m%d")


def _build_ohlcv_pyramid(ohlcv_df: Any, total_span_minutes: float) -> dict[str, dict[str, list[Any]]]:
    pyramid: dict[str, dict[str, list[Any]]] = {
        "1d": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1D")),
        "1h": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1h")),
        "15m": _df_to_columnar(_resample_ohlcv(ohlcv_df, "15min")),
        "5m": _df_to_columnar(_resample_ohlcv(ohlcv_df, "5min")),
    }
    if total_span_minutes <= 90 * 24 * 60:
        pyramid["1m"] = _df_to_columnar(ohlcv_df)
    return pyramid


_OHLCV_CHART_TEMPLATE = r"""<!DOCTYPE html>
<html><head><meta charset=\"utf-8\">
<script src=\"/app/plotly.min.js?v=1\"></script>
<style>
html,body{margin:0;padding:0;background:transparent;overflow:hidden;}
#wrap{position:relative;width:100%;}
#chart{width:100%;}
#tf-ind{position:absolute;top:6px;left:50%;transform:translateX(-50%);background:rgba(40,40,50,0.85);
  color:#aaa;padding:3px 10px;border-radius:4px;font:13px/1.4 sans-serif;
  z-index:10;pointer-events:none;}
#coin-label{position:absolute;top:6px;left:8px;background:rgba(40,40,50,0.85);
    color:#e0e0e0;padding:3px 10px;border-radius:4px;font:bold 13px/1.4 sans-serif;
    z-index:10;pointer-events:none;}
#loading{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
    background:rgba(14,17,23,0.92);color:#ddd;padding:14px 28px;
    border-radius:8px;font:14px sans-serif;z-index:20;display:none;}
</style></head>
<body>
<div id=\"wrap\">
    <div id=\"coin-label\"></div>
  <div id=\"tf-ind\"></div>
    <div id=\"loading\">Loading chart&#8230;</div>
  <div id=\"chart\"></div>
</div>
<script>
(function(){
  \"use strict\";
  var L=/*__DATA__*/null;
  var SVOL=/*__SHOW_VOL__*/false;
    var COIN=/*__COIN__*/\"\";
    var SPLITS=/*__SPLITS__*/[];
    var LAZY_URL=/*__LAZY_URL__*/\"\";
  var HV=/*__HEIGHT_VOL__*/620;
  var HN=/*__HEIGHT_NO_VOL__*/460;
  var TFO=[\"1d\",\"1h\",\"15m\",\"5m\",\"1m\"];
  var TFT={\"1d\":365*864e5,\"1h\":45*864e5,\"15m\":10*864e5,\"5m\":2*864e5,\"1m\":0};
    var cur=null,guard=false,inited=false,pendingRequest=null,lastXKey=\"\";
    var unavailable={};
    var debounceTimer=null;

    function showError(msg){
        var loadDiv=document.getElementById(\"loading\");
        if(loadDiv){loadDiv.textContent=String(msg||\"Failed to load chart.\");loadDiv.style.display=\"block\";}
    }

    function hasLayer(tf){
        return !!(L&&L[tf]&&L[tf].ts&&L[tf].ts.length);
    }

    function registerLayerCoverage(tf,layer){
        if(!tf||!layer||!layer.ts||!layer.ts.length)return;
        var startMs=layer.ts[0];
        var endMs=layer.ts[layer.ts.length-1];
        if(!isFinite(startMs)||!isFinite(endMs)||endMs<startMs)return;
        var ranges=Array.isArray(loadedRanges[tf])?loadedRanges[tf].slice():[];
        ranges.push([startMs,endMs]);
        ranges.sort(function(a,b){return a[0]-b[0];});
        var mergedRanges=[];
        for(var idx=0;idx<ranges.length;idx++){
            var current=ranges[idx];
            if(!mergedRanges.length||current[0]>mergedRanges[mergedRanges.length-1][1]){
                mergedRanges.push([current[0],current[1]]);
            }else{
                mergedRanges[mergedRanges.length-1][1]=Math.max(mergedRanges[mergedRanges.length-1][1],current[1]);
            }
        }
        loadedRanges[tf]=mergedRanges;
    }

    function mergeLayerData(existingLayer,incomingLayer){
        if(!existingLayer||!existingLayer.ts||!existingLayer.ts.length)return incomingLayer;
        if(!incomingLayer||!incomingLayer.ts||!incomingLayer.ts.length)return existingLayer;
        var keys=["ts","o","h","l","c","v"];
        var merged={ts:[],o:[],h:[],l:[],c:[],v:[]};
        function pushPoint(layer,idx){
            for(var keyIdx=0;keyIdx<keys.length;keyIdx++){
                var key=keys[keyIdx];
                merged[key].push(layer[key][idx]);
            }
        }
        var leftIdx=0,rightIdx=0;
        while(leftIdx<existingLayer.ts.length&&rightIdx<incomingLayer.ts.length){
            var leftTs=existingLayer.ts[leftIdx];
            var rightTs=incomingLayer.ts[rightIdx];
            if(leftTs<rightTs){
                pushPoint(existingLayer,leftIdx);
                leftIdx+=1;
            }else if(rightTs<leftTs){
                pushPoint(incomingLayer,rightIdx);
                rightIdx+=1;
            }else{
                pushPoint(incomingLayer,rightIdx);
                leftIdx+=1;
                rightIdx+=1;
            }
        }
        while(leftIdx<existingLayer.ts.length){
            pushPoint(existingLayer,leftIdx);
            leftIdx+=1;
        }
        while(rightIdx<incomingLayer.ts.length){
            pushPoint(incomingLayer,rightIdx);
            rightIdx+=1;
        }
        return merged;
    }

    function mergeLayers(nextLayers){
        if(!nextLayers)return;
        for(var tf in nextLayers){
            if(Object.prototype.hasOwnProperty.call(nextLayers,tf)&&nextLayers[tf]&&nextLayers[tf].ts&&nextLayers[tf].ts.length){
                L[tf]=mergeLayerData(L[tf],nextLayers[tf]);
                registerLayerCoverage(tf,nextLayers[tf]);
            }
        }
    }

    function hasCov(tf,startMs,endMs){
        return coverageRatio(tf,startMs,endMs)>=0.995;
    }

    function coverageRatio(tf,startMs,endMs){
        if(!hasLayer(tf)||endMs<=startMs)return 0;
        var ranges=loadedRanges[tf];
        if(!Array.isArray(ranges)||!ranges.length){
            registerLayerCoverage(tf,L[tf]);
            ranges=loadedRanges[tf] || [];
        }
        var spanMs=Math.max(1,endMs-startMs);
        var overlapMs=0;
        for(var idx=0;idx<ranges.length;idx++){
            var range=ranges[idx];
            if(range[1] <= startMs)continue;
            if(range[0] >= endMs)break;
            overlapMs += Math.max(0,Math.min(range[1],endMs)-Math.max(range[0],startMs));
        }
        return overlapMs/spanMs;
    }

    function countPointsInRange(tf,startMs,endMs){
        if(!hasLayer(tf)||endMs<=startMs)return 0;
        var ts=L[tf].ts;
        if(!ts||!ts.length)return 0;
        var left=0,right=ts.length;
        while(left<right){
            var mid=(left+right)>>1;
            if(ts[mid] < startMs)left=mid+1;
            else right=mid;
        }
        var startIdx=left;
        left=0;right=ts.length;
        while(left<right){
            var mid2=(left+right)>>1;
            if(ts[mid2] <= endMs)left=mid2+1;
            else right=mid2;
        }
        return Math.max(0,left-startIdx);
    }

    function pick(ms,startMs,endMs){
        var want=wantTF(ms);
        var wantIdx=TFO.indexOf(want);
        if(wantIdx<0)wantIdx=TFO.length-1;
        for(var i=wantIdx;i>=0;i--)if(hasCov(TFO[i],startMs,endMs))return TFO[i];
        for(var k=0;k<TFO.length;k++)if(hasLayer(TFO[k]))return TFO[k];
    return \"1d\";
  }
    function wantTF(ms){
        for(var i=0;i<TFO.length;i++)if(ms>=TFT[TFO[i]])return TFO[i];
        return \"1m\";
    }
  function iso(t){return new Date(t).toISOString();}
    function ms(t){
                if(t===null||t===undefined)return NaN;
                if(typeof t==="number")return t;
                if(t instanceof Date)return t.getTime();
                var s=String(t).trim();
                if(!s)return NaN;
                var naive=s.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2})(?:\.(\d+))?)?$/);
                if(naive&&!/(Z|[+-]\d{2}:?\d{2})$/.test(s)){
                    var secs=naive[3]||"00";
                    var frac=naive[4]?("."+naive[4].slice(0,3).padEnd(3,"0")):"";
                    s=naive[1]+"T"+naive[2]+":"+secs+frac+"Z";
                }
                return new Date(s).getTime();
        }

    function lowerBound(values,target){
        var left=0,right=values.length;
        while(left<right){
            var mid=(left+right)>>1;
            if(values[mid]<target)left=mid+1;
            else right=mid;
        }
        return left;
    }

    function upperBound(values,target){
        var left=0,right=values.length;
        while(left<right){
            var mid=(left+right)>>1;
            if(values[mid]<=target)left=mid+1;
            else right=mid;
        }
        return left;
    }

    function yRange(tf,startMs,endMs){
        var d=L[tf];if(!d||!d.ts.length)return null;
        var mn=Infinity,mx=-Infinity,vmx=0;
        var first=lowerBound(d.ts,startMs);
        var last=upperBound(d.ts,endMs);
        for(var i=first;i<last;i++){
            if(d.l[i]<mn)mn=d.l[i];
            if(d.h[i]>mx)mx=d.h[i];
            if(SVOL&&d.v[i]>vmx)vmx=d.v[i];
        }
        if(!isFinite(mn)||!isFinite(mx))return null;
        var pad=(mx-mn)*0.10||mx*0.02;
        return {price:[mn-pad,mx+pad],vol:[0,vmx*1.10]};
    }

  function mkTraces(tf){
    var d=L[tf],x=d.ts.map(iso);
    var tr=[{type:\"candlestick\",x:x,open:d.o,high:d.h,low:d.l,close:d.c,
            name:\"OHLC\",showlegend:false,
      increasing:{line:{color:\"#26a69a\"}},decreasing:{line:{color:\"#ef5350\"}}}];
    if(SVOL){
      var vc=[];for(var i=0;i<d.o.length;i++)vc.push(d.c[i]>=d.o[i]?\"#26a69a\":\"#ef5350\");
      tr.push({type:\"bar\",x:x,y:d.v,name:\"Vol\",
        marker:{color:vc},opacity:0.6,showlegend:false,yaxis:\"y2\"});
    }
    return tr;
  }

    function splitShapes(){
        if(!SPLITS||!SPLITS.length)return {shapes:[],annotations:[]};
        var shapes=[],annotations=[];
        for(var i=0;i<SPLITS.length;i++){
            var split=SPLITS[i];
            var xVal=split.date+\"T00:00:00\";
            shapes.push({type:\"line\",x0:xVal,x1:xVal,y0:0,y1:1,yref:\"paper\",
                line:{color:\"#ff9800\",width:1.5,dash:\"dash\"},layer:\"below\"});
            var factor=split.factor;
            var label=factor>=1?(factor+\":1\"):(\"1:\"+Math.round(1/factor));
            annotations.push({x:xVal,y:1,yref:\"paper\",yanchor:\"bottom\",
                text:\"Split \"+label,font:{size:10,color:\"#ff9800\"},
                showarrow:false,xshift:4,yshift:2,textangle:0,
                bgcolor:\"rgba(14,17,23,0.7)\",borderpad:2});
        }
        return {shapes:shapes,annotations:annotations};
    }

    function mkLayout(xr,yr){
        var splitDecor=splitShapes();
        var dragMode=(el&&el.layout&&el.layout.dragmode)?el.layout.dragmode:"pan";
    var o={paper_bgcolor:\"rgba(0,0,0,0)\",plot_bgcolor:\"#0e1117\",
            font:{color:\"#ccc\",size:11},margin:{l:55,r:10,t:40,b:30},
      xaxis:{rangeslider:{visible:false},gridcolor:\"#222\",type:\"date\"},
            yaxis:{title:\"Price\",gridcolor:\"#222\",fixedrange:false},
            showlegend:false,
            dragmode:dragMode,
            shapes:splitDecor.shapes,annotations:splitDecor.annotations};
        if(xr)o.xaxis.range=[xr[0],xr[1]];
        if(yr&&yr.price){o.yaxis.range=yr.price;o.yaxis.autorange=false;}else{o.yaxis.autorange=true;}
    if(SVOL){o.height=HV;o.yaxis.domain=[0.25,1.0];
      o.yaxis2={title:\"Volume\",gridcolor:\"#222\",domain:[0,0.2],
                rangemode:\"tozero\",fixedrange:false};o.bargap=0;
            if(yr&&yr.vol)o.yaxis2.range=yr.vol;
    }else{o.height=HN;}
    return o;
  }

  var el=document.getElementById(\"chart\");
    var tfInd=document.getElementById(\"tf-ind\");
    var coinLabel=document.getElementById(\"coin-label\");
    var loadDiv=document.getElementById(\"loading\");
    var CFG={scrollZoom:false,displayModeBar:true,responsive:true};
    var activeFetchController=null;
    var activeFetchToken=0;
    var lastSpanMs=null;
    var loadedRanges={};

    function setLastSpan(startMs,endMs){
        lastSpanMs=Math.max(1,endMs-startMs);
    }

    function abortPendingFetch(hideLoading){
        if(activeFetchController){
            try{activeFetchController.abort();}catch(_err){}
            activeFetchController=null;
        }
        activeFetchToken+=1;
        pendingRequest=null;
        if(hideLoading)loadDiv.style.display=\"none\";
    }

    function clampRangeToLayer(xr,tf,preserveSpan){
        if(!xr||xr.length<2)return xr;
        // Clamp to the full source range, not the currently loaded fine layer.
        // Otherwise lazy-loaded panning snaps back to the last loaded window.
        var base=(L["1d"]&&L["1d"].ts&&L["1d"].ts.length)?L["1d"]:((L["1h"]&&L["1h"].ts&&L["1h"].ts.length)?L["1h"]:null);
        var dataTs=base?base.ts:(hasLayer(tf)?L[tf].ts:null);
        if(!dataTs||!dataTs.length)return xr;
        var minMs=dataTs[0],maxMs=dataTs[dataTs.length-1];
        var a=ms(xr[0]),b=ms(xr[1]);
        if(!isFinite(a)||!isFinite(b))return xr;
        var startMs=Math.min(a,b),endMs=Math.max(a,b);
        var spanMs=Math.max(1,endMs-startMs);
        var dataSpanMs=Math.max(1,maxMs-minMs);
        if(spanMs>=dataSpanMs){
            return [iso(minMs),iso(maxMs)];
        }
        if(preserveSpan){
            if(startMs<minMs){
                startMs=minMs;
                endMs=Math.min(maxMs,startMs+spanMs);
                startMs=Math.max(minMs,endMs-spanMs);
                return [iso(startMs),iso(endMs)];
            }
            if(endMs>maxMs){
                endMs=maxMs;
                startMs=Math.max(minMs,endMs-spanMs);
                endMs=Math.min(maxMs,startMs+spanMs);
                return [iso(startMs),iso(endMs)];
            }
            return [iso(startMs),iso(endMs)];
        }
        if(endMs<=minMs||startMs>=maxMs){
            return [iso(minMs),iso(maxMs)];
        }
        startMs=Math.max(minMs,startMs);
        endMs=Math.min(maxMs,endMs);
        if(endMs<=startMs){
            return [iso(minMs),iso(maxMs)];
        }
        return [iso(startMs),iso(endMs)];
    }

    function renderRange(xr,preserveSpan){
        if(!xr||xr.length<2)return Promise.resolve();
        var a=ms(xr[0]),b=ms(xr[1]);
        if(!isFinite(a)||!isFinite(b))return Promise.resolve();
        var startMs=Math.min(a,b),endMs=Math.max(a,b),span=endMs-startMs;
        var best=pick(span,startMs,endMs);
        xr=clampRangeToLayer(xr,best,!!preserveSpan);
        a=ms(xr[0]);
        b=ms(xr[1]);
        startMs=Math.min(a,b);
        endMs=Math.max(a,b);
        cur=best;
        tfInd.textContent=best;
        guard=true;
        var yr=yRange(best,startMs,endMs);
        var plotPromise=inited
            ? Plotly.react(el,mkTraces(best),mkLayout(xr,yr),CFG)
            : Plotly.newPlot(el,mkTraces(best),mkLayout(xr,yr),CFG);
        return plotPromise.then(function(){
            inited=true;
            setLastSpan(startMs,endMs);
            setTimeout(function(){guard=false;},80);
        }).catch(function(err){
            guard=false;
            throw err;
        });
    }

    function fetchNeeded(want,xr){
        if(!LAZY_URL||unavailable[want])return Promise.resolve(false);
        abortPendingFetch(false);
        activeFetchToken+=1;
        var requestToken=activeFetchToken;
        var controller=new AbortController();
        activeFetchController=controller;
        pendingRequest=want;
        loadDiv.textContent=\"Loading \"+want+\" candles...\";
        loadDiv.style.display=\"block\";
        var reqStartMs=ms(xr[0]),reqEndMs=ms(xr[1]);
        var reqStart=isFinite(reqStartMs)?iso(reqStartMs):xr[0];
        var reqEnd=isFinite(reqEndMs)?iso(reqEndMs):xr[1];
        var url=LAZY_URL+\"&need_tf=\"+encodeURIComponent(want)
            +"&range_start="+encodeURIComponent(reqStart)
            +"&range_end="+encodeURIComponent(reqEnd);
        return fetch(url,{credentials:\"same-origin\",signal:controller.signal})
            .then(function(response){
                if(!response.ok)throw new Error(\"HTTP \"+response.status);
                return response.json();
            })
            .then(function(payload){
                if(requestToken!==activeFetchToken)return false;
                activeFetchController=null;
                pendingRequest=null;
                loadDiv.style.display=\"none\";
                if(payload&&Array.isArray(payload.unavailable)){
                    for(var i=0;i<payload.unavailable.length;i++)unavailable[payload.unavailable[i]]=true;
                }
                mergeLayers(payload&&payload.layers?payload.layers:null);
                return !!(payload&&payload.layers&&payload.layers[want]&&payload.layers[want].ts&&payload.layers[want].ts.length);
            })
            .catch(function(err){
                if(requestToken!==activeFetchToken||(err&&err.name===\"AbortError\"))return false;
                activeFetchController=null;
                pendingRequest=null;
                showError(\"Failed to load finer candles: \"+(err&&err.message?err.message:String(err||\"unknown error\")));
                return false;
            });
    }

    function updateAxesForRange(xr,startMs,endMs,rangeChanged){
        var upd={};
        if(rangeChanged)upd[\"xaxis.range\"]=xr;
        var yr=yRange(cur,startMs,endMs);
        if(yr){
            upd[\"yaxis.range\"]=yr.price;
            upd[\"yaxis.autorange\"]=false;
            if(SVOL)upd[\"yaxis2.range\"]=yr.vol;
        }
        if(!Object.keys(upd).length){
            setLastSpan(startMs,endMs);
            return Promise.resolve();
        }
        return Plotly.relayout(el,upd).then(function(){
            setLastSpan(startMs,endMs);
        });
    }

    function onZoom(){
        if(guard)return;
        clearTimeout(debounceTimer);
        debounceTimer=setTimeout(function(){
            var xr=el.layout.xaxis.range;if(!xr||xr.length<2)return;
            var a=ms(xr[0]),b=ms(xr[1]);
            if(!isFinite(a)||!isFinite(b))return;
            var originalXr=[xr[0],xr[1]];
            var startMs=Math.min(a,b),endMs=Math.max(a,b),span=endMs-startMs;
            var requestedSpanMs=Math.max(1,span);
            var preserveSpan=lastSpanMs!==null&&Math.abs(requestedSpanMs-lastSpanMs)<=Math.max(60_000,lastSpanMs*0.05);
            xr=clampRangeToLayer(xr,cur,preserveSpan);
            var rangeChanged=xr[0]!==originalXr[0]||xr[1]!==originalXr[1];
            a=ms(xr[0]);
            b=ms(xr[1]);
            if(!isFinite(a)||!isFinite(b))return;
            startMs=Math.min(a,b);
            endMs=Math.max(a,b);
            span=endMs-startMs;
            var xKey=Math.round(startMs/1000)+\":\"+Math.round(endMs/1000);
            if(xKey===lastXKey&&!rangeChanged)return;
            lastXKey=xKey;
            abortPendingFetch(true);
            var want=wantTF(span),best=pick(span,startMs,endMs);
            var sameTfVisiblePoints=countPointsInRange(want,startMs,endMs);
            var needFineFetch=LAZY_URL&&want!==best&&!hasCov(want,startMs,endMs)&&!unavailable[want];
            var needSameTfFetch=LAZY_URL&&want===best&&want===cur&&(want==="15m"||want==="5m"||want==="1m")&&(coverageRatio(want,startMs,endMs)<0.995||sameTfVisiblePoints===0);
            var renderPromise=Promise.resolve();
            if(best!==cur){
                renderPromise=renderRange(xr,preserveSpan).catch(function(err){showError(err&&err.message?err.message:String(err));});
            }else{
                renderPromise=updateAxesForRange(xr,startMs,endMs,rangeChanged).catch(function(err){showError(err&&err.message?err.message:String(err));});
            }
            if(needFineFetch||needSameTfFetch){
                renderPromise.then(function(){
                    return fetchNeeded(want,xr);
                }).then(function(loaded){
                    if(!loaded)return;
                    renderRange(xr,preserveSpan).catch(function(err){showError(err&&err.message?err.message:String(err));});
                });
            }
        },35);
    }

    function onWheelZoom(event){
        if(!inited||!el.layout||!el.layout.xaxis||!el.layout.xaxis.range)return;
        var xr=el.layout.xaxis.range;
        if(!xr||xr.length<2)return;
        var a=ms(xr[0]),b=ms(xr[1]);
        if(!isFinite(a)||!isFinite(b))return;
        event.preventDefault();
        event.stopPropagation();
        var startMs=Math.min(a,b),endMs=Math.max(a,b),span=Math.max(1,endMs-startMs);
        var delta=event.deltaY||0;
        if(event.deltaMode===1)delta*=16;
        else if(event.deltaMode===2)delta*=300;
        var steps=Math.max(-6,Math.min(6,delta/120));
        if(!steps)return;
        var factor=Math.pow(1.28,steps);
        var rect=el.getBoundingClientRect();
        var anchorRatio=rect.width>0?(event.clientX-rect.left)/rect.width:0.5;
        anchorRatio=Math.max(0.02,Math.min(0.98,anchorRatio));
        var minSpanMs=60_000;
        var newSpan=Math.max(minSpanMs,span*factor);
        factor=newSpan/span;
        var anchorMs=startMs+span*anchorRatio;
        var nextStart=anchorMs-(anchorMs-startMs)*factor;
        var nextEnd=anchorMs+(endMs-anchorMs)*factor;
        var nextRange=clampRangeToLayer([iso(nextStart),iso(nextEnd)],cur,true);
        Plotly.relayout(el,{\"xaxis.range\":nextRange}).catch(function(err){showError(err&&err.message?err.message:String(err));});
    }

    try{
        el.addEventListener("wheel",onWheelZoom,{passive:false});
        if(typeof Plotly===\"undefined\")throw new Error(\"Plotly failed to load\");
        coinLabel.textContent=COIN||\"\";
        for(var tf in L){
            if(Object.prototype.hasOwnProperty.call(L,tf))registerLayerCoverage(tf,L[tf]);
        }
        var base=L[\"1d\"]&&L[\"1d\"].ts&&L[\"1d\"].ts.length?L[\"1d\"]:L[\"1h\"];
        if(!base||!base.ts||base.ts.length<2)throw new Error(\"No OHLCV chart data available\");
        var initialRange=[iso(base.ts[0]),iso(base.ts[base.ts.length-1])];
        loadDiv.style.display=\"block\";
        renderRange(initialRange).then(function(){
            loadDiv.style.display=\"none\";
            el.on(\"plotly_relayout\",onZoom);
        }).catch(function(err){
            showError(err&&err.message?err.message:String(err));
        });
    }catch(err){
        showError(err&&err.message?err.message:String(err));
    }
})();
</script></body></html>"""


def _build_ohlcv_chart_html(
        pyramid: dict[str, Any],
        show_volume: bool,
        coin_name: str,
        split_dates: list[dict[str, Any]],
        lazy_url: str,
        height_vol: int = 620,
        height_no_vol: int = 460,
) -> str:
    data_json = json.dumps(pyramid, separators=(",", ":"))
    html = _OHLCV_CHART_TEMPLATE.replace('\\"', '"')
    html = html.replace("/*__DATA__*/null", data_json)
    html = html.replace("/*__SHOW_VOL__*/false", "true" if show_volume else "false")
    html = html.replace("/*__COIN__*/\"\"", json.dumps(str(coin_name or "")))
    html = html.replace("/*__SPLITS__*/[]", json.dumps(split_dates or [], separators=(",", ":")))
    html = html.replace("/*__LAZY_URL__*/\"\"", json.dumps(str(lazy_url or "")))
    html = html.replace("/*__HEIGHT_VOL__*/620", str(height_vol))
    html = html.replace("/*__HEIGHT_NO_VOL__*/460", str(height_no_vol))
    return html


@router.get("/status-monitor/{exchange}", response_class=HTMLResponse)
def get_market_data_status_monitor(
    exchange: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    exchange_clean = str(exchange or "").strip().lower()
    if not _get_exchange_status_key(exchange_clean) or not _get_exchange_flag_prefix(exchange_clean):
        return HTMLResponse("<div>Unknown exchange</div>", status_code=404)

    html = _render_market_data_status_html(request=request, token=session.token, exchange=exchange_clean)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/data-actions/hyperliquid", response_class=HTMLResponse)
def get_hyperliquid_data_actions(
    request: Request,
    section: str = Query(default="", description="Optional initial section: build or download"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    section_clean = str(section or "").strip().lower()
    if section_clean not in ("build", "download"):
        section_clean = ""

    html = _render_hl_data_actions_html(
        request=request,
        token=session.token,
        initial_section=section_clean,
    )
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/best-1m/info/{exchange}")
def get_best_1m_info(exchange: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    meta = _best_1m_exchange_meta(exchange)
    if not meta:
        return {"success": False, "error": "Unsupported exchange for Best 1m."}

    exchange_clean = _normalize_settings_exchange(exchange)
    return {
        "success": True,
        "exchange": exchange_clean,
        "label": meta["label"],
        "coins": _get_best_1m_available_coins(exchange_clean),
        "description": meta["description"],
        "hint": meta["hint"],
        "refetch_label": meta["refetch_label"],
        "distributed_supported": exchange_clean == "bitget",
        "distributed_hosts": _load_bitget_distributed_hosts() if exchange_clean == "bitget" else [],
        "empty_message": "No available coins found for this exchange. Refresh CoinData first.",
    }


@router.post("/best-1m/queue/{exchange}")
def queue_best_1m_job(
    exchange: str,
    request: dict,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    import re as _re
    import subprocess
    import sys
    from datetime import date as _date

    from market_data import append_exchange_download_log
    from task_queue import enqueue_job, enqueue_running_job, is_pid_running, move_job_file, read_worker_pid, update_job_file

    exchange_clean = _normalize_settings_exchange(exchange)
    meta = _best_1m_exchange_meta(exchange_clean)
    if not meta:
        return {"success": False, "error": "Unsupported exchange for Best 1m."}

    available_coins = _get_best_1m_available_coins(exchange_clean)
    raw_coins = request.get("coins", [])
    if not isinstance(raw_coins, list):
        raw_coins = []
    raw_coins = [
        _normalize_best_1m_request_coin(coin, available_coins=available_coins)
        for coin in raw_coins
        if str(coin).strip()
    ]
    raw_coins = list(dict.fromkeys(raw_coins))
    selected_only = bool(request.get("selected_only", False))

    if raw_coins and "ALL" not in raw_coins:
        invalid_coins = [coin for coin in raw_coins if coin not in available_coins]
        if invalid_coins:
            preview = ", ".join(invalid_coins[:10])
            suffix = "..." if len(invalid_coins) > 10 else ""
            return {"success": False, "error": f"Unsupported coin(s) for {meta['label']}: {preview}{suffix}. Refresh CoinData and select coins from the list."}
        build_coins = list(raw_coins)
    else:
        if selected_only:
            return {"success": False, "error": "No explicitly selected coins were provided."}
        if not available_coins:
            return {"success": False, "error": "No available coins found for this exchange. Refresh CoinData first."}
        build_coins = list(available_coins)

    if not build_coins:
        return {"success": False, "error": "No coins selected."}

    start_day = str(request.get("start_day") or "").strip()
    end_day = str(request.get("end_day") or "").strip()
    if start_day and not _re.fullmatch(r"\d{8}", start_day):
        return {"success": False, "error": "Invalid start_day format (expected YYYYMMDD)."}
    if end_day and not _re.fullmatch(r"\d{8}", end_day):
        return {"success": False, "error": "Invalid end_day format (expected YYYYMMDD)."}
    if start_day and end_day and start_day > end_day:
        return {"success": False, "error": "Start date must be on or before End date."}

    effective_end = end_day or _date.today().strftime("%Y%m%d")
    refetch = bool(request.get("refetch", False))
    distributed = bool(request.get("distributed", False))
    distributed_hosts: list[dict[str, str]] = []
    if distributed:
        if exchange_clean != "bitget":
            return {"success": False, "error": "Distributed Best 1m backfill is only supported for Bitget."}
        try:
            distributed_hosts = _select_bitget_distributed_hosts(request.get("distributed_hosts"))
        except ValueError as exc:
            return {"success": False, "error": str(exc)}

    job_type = "bitget_best_1m_distributed" if distributed else meta["job_type"]
    run_immediately = job_type in {"bitget_best_1m", "bitget_best_1m_distributed"}
    payload: dict[str, Any] = {
        "coins": list(build_coins),
        "end_day": effective_end,
        "start_day": start_day,
        "refetch": refetch,
    }
    if distributed:
        payload["distributed_hosts"] = distributed_hosts
    try:
        enqueue_fn = enqueue_running_job if run_immediately else enqueue_job
        job = enqueue_fn(
            job_type=job_type,
            exchange=meta["queue_exchange"],
            payload=payload,
        )
    except Exception as exc:
        return {"success": False, "error": f"Failed to enqueue job: {exc}"}

    append_exchange_download_log(
        meta["queue_exchange"],
        f"[{job_type}] queued job_id={job.job_id} coins={len(build_coins)} range={start_day or '?'}-{effective_end} downloaders={len(distributed_hosts)}",
    )

    runner_started = False
    if run_immediately:
        job_path = Path(job.path)
        try:
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py"), "--run-job", str(job_path)],
                cwd=str(Path(__file__).resolve().parents[1]),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                start_new_session=True,
            )
            runner_started = True
        except Exception as exc:
            try:
                update_job_file(job_path, mutate=lambda o: o.update({"status": "failed", "error": f"Failed to launch {job_type} runner: {exc}"}))
                move_job_file(job_path, "failed")
            except Exception:
                pass
            return {"success": False, "error": f"Failed to launch {meta['label']} worker: {exc}"}

    try:
        pid = read_worker_pid()
        if not run_immediately and not (pid and is_pid_running(int(pid))):
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
    except Exception:
        pass

    return {
        "success": True,
        "job_id": job.job_id,
        "job_type": job_type,
        "exchange": exchange_clean,
        "coins_count": len(build_coins),
        "distributed": distributed,
        "distributed_hosts_count": len(distributed_hosts),
        "runner_started": runner_started,
        "start_day": start_day,
        "end_day": effective_end,
        "refetch": refetch,
        "message": (
            f"Queued {meta['label']} {'distributed ' if distributed else ''}Best 1m job {job.job_id} for {len(build_coins)} coin(s)"
            + (f" across {len(distributed_hosts)} downloader(s). " if distributed else ". ")
            + "Use Activity Log or Jobs to watch progress."
        ),
    }


@router.post("/copy-data/queue")
def queue_copy_data_job(
    request: dict,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Queue an rsync-based local OHLCV copy job for the task worker."""

    return _queue_copy_data_job_response(request, dry_run=False)


@router.post("/copy-data/dry-run/queue")
def queue_copy_data_dry_run_job(
    request: dict,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Queue a dry-run rsync OHLCV copy job that writes nothing remotely."""

    return _queue_copy_data_job_response(request, dry_run=True)


@router.post("/copy-data/test")
def test_copy_data_connection(
    request: dict,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Run a read-only SSH and destination-path check for Copy Data."""

    try:
        payload = _build_copy_data_test_payload(request)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    try:
        result = _test_copy_data_connection_payload(payload)
    except Exception as exc:
        return {"success": False, "error": f"Connection test failed: {exc}"}
    return result


@router.get("/inventory/chart/ohlcv", response_class=HTMLResponse)
def get_inventory_ohlcv_chart(
    request: Request,
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    start_day: str = Query(default=""),
    end_day: str = Query(default=""),
    show_volume: bool = Query(True),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    ex = _normalize_settings_exchange(exchange)
    ds = str(dataset or "").strip()
    ds_l = ds.lower()
    actual_coin = str(coin or "").strip()

    if not ds or not actual_coin:
        return HTMLResponse("<div style='padding:24px;color:#a0a4ab'>No OHLCV chart available.</div>", status_code=400)
    if ds_l in ("l2book", "l2book_mid"):
        return HTMLResponse("<div style='padding:24px;color:#a0a4ab'>l2Book inventory does not have an OHLCV chart.</div>")

    try:
        if ds_l.startswith("pb7_cache:"):
            timeframe = str(ds.split(":", 1)[1] if ":" in ds else "1m").strip() or "1m"
            frame = _load_ohlcv_from_pb7_cache(
                exchange=ex,
                timeframe=timeframe,
                coin=actual_coin,
                start_day=str(start_day or "").strip(),
                end_day=str(end_day or "").strip(),
            )
        else:
            frame = _load_ohlcv_from_npz_range(
                exchange=_inventory_storage_exchange(ex),
                dataset=ds,
                coin=actual_coin,
                start_day=str(start_day or "").strip(),
                end_day=str(end_day or "").strip(),
            )
    except Exception as exc:
        return HTMLResponse(f"<div style='padding:24px;color:#ef5350'>Failed to load OHLCV chart: {exc}</div>", status_code=500)

    if frame is None or frame.empty:
        return HTMLResponse("<div style='padding:24px;color:#a0a4ab'>No OHLCV candles found for the selected range.</div>")

    try:
        layers = _build_ohlcv_initial_layers(frame)
        split_dates = _get_ohlcv_chart_split_dates(actual_coin, layers)
        lazy_query = urlencode(
            {
                "exchange": ex,
                "dataset": ds,
                "coin": actual_coin,
                "start_day": str(start_day or "").strip(),
                "end_day": str(end_day or "").strip(),
            }
        )
        lazy_url = f"{request.url_for('get_inventory_ohlcv_chart_layers')}?{lazy_query}"
        html = _build_ohlcv_chart_html(
            layers,
            show_volume=bool(show_volume),
            coin_name=_get_ohlcv_chart_coin_label(actual_coin),
            split_dates=split_dates,
            lazy_url=lazy_url,
        )
        return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
    except Exception as exc:
        return HTMLResponse(f"<div style='padding:24px;color:#ef5350'>Failed to build OHLCV chart: {exc}</div>", status_code=500)


@router.get("/inventory/chart/ohlcv/layers")
def get_inventory_ohlcv_chart_layers(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    need_tf: str = Query(...),
    range_start: str = Query(...),
    range_end: str = Query(...),
    start_day: str = Query(default=""),
    end_day: str = Query(default=""),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    ex = _normalize_settings_exchange(exchange)
    ds = str(dataset or "").strip()
    ds_l = ds.lower()
    actual_coin = str(coin or "").strip()
    target_tf = str(need_tf or "").strip().lower()

    if not ds or not actual_coin or not target_tf:
        return {"layers": {}, "unavailable": []}
    if ds_l in ("l2book", "l2book_mid"):
        return {"layers": {}, "unavailable": [target_tf]}
    supported_tfs = _get_ohlcv_supported_zoom_tfs(ds)
    if target_tf not in supported_tfs:
        return {"layers": {}, "unavailable": [target_tf]}

    try:
        window_start_ms, window_end_ms = _get_ohlcv_zoom_window_ms(range_start, range_end, target_tf)
    except Exception:
        return {"layers": {}, "unavailable": []}

    start_key = max(str(start_day or "").strip(), _ms_to_ohlcv_day(window_start_ms)) if start_day else _ms_to_ohlcv_day(window_start_ms)
    end_key = min(str(end_day or "").strip(), _ms_to_ohlcv_day(window_end_ms)) if end_day else _ms_to_ohlcv_day(window_end_ms)

    try:
        if ds_l.startswith("pb7_cache:"):
            timeframe = str(ds.split(":", 1)[1] if ":" in ds else "1m").strip() or "1m"
            frame = _load_ohlcv_from_pb7_cache(
                exchange=ex,
                timeframe=timeframe,
                coin=actual_coin,
                start_day=start_key,
                end_day=end_key,
            )
        else:
            frame = _load_ohlcv_from_npz_range(
                exchange=_inventory_storage_exchange(ex),
                dataset=ds,
                coin=actual_coin,
                start_day=start_key,
                end_day=end_key,
            )
    except Exception:
        return {"layers": {}, "unavailable": []}

    if frame is None or frame.empty:
        return {"layers": {}, "unavailable": []}
    frame = frame[(frame["ts"] >= int(window_start_ms)) & (frame["ts"] <= int(window_end_ms))]
    if frame.empty:
        return {"layers": {}, "unavailable": []}

    layers = _build_ohlcv_zoom_layers(frame, ds, target_tf)
    unavailable = [] if target_tf in layers else [target_tf]
    return {"layers": layers, "unavailable": unavailable}


@router.get("/inventory/{exchange}")
def get_inventory_dataset(
    exchange: str,
    view: str = Query(default="1m"),
    coin_filter: str = Query(default=""),
    kind_filter: str = Query(default="all"),
    include_missing: bool = Query(default=False),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    try:
        return _build_inventory_dataset_payload(
            exchange=exchange,
            view=view,
            coin_filter=coin_filter,
            kind_filter=kind_filter,
            include_missing=include_missing,
        )
    except Exception as exc:
        return {
            "success": False,
            "exchange": _normalize_settings_exchange(exchange),
            "view": _normalize_inventory_view(view) or "1m",
            "error": str(exc),
            "available_views": _inventory_views_for_exchange(exchange),
        }


@router.post("/inventory/{exchange}/delete-selected")
def delete_inventory_selected(
    exchange: str,
    request: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        view_key = _require_inventory_view(exchange, body.get("view") or "1m")
        if view_key == "pb7_cache":
            raise ValueError("PB7 cache is read-only.")

        selected_rows = _resolve_inventory_rows_for_coins(exchange, view_key, body.get("coins") or [])
        if not selected_rows:
            raise ValueError("No matching coins selected.")

        storage_ex = _inventory_storage_exchange(exchange)
        deleted_count = 0
        rebuilt_days_total = 0
        rebuilt_minutes_total = 0
        deleted_size = 0

        for row in selected_rows:
            actual_dataset = str(row.get("dataset") or "").strip()
            actual_dataset_lower = actual_dataset.lower()
            actual_coin = str(row.get("coin") or "").strip()
            coin_dir = get_exchange_raw_root_dir(storage_ex) / actual_dataset / actual_coin
            if coin_dir.exists():
                shutil.rmtree(coin_dir)
                deleted_count += 1
                deleted_size += int(row.get("total_bytes", 0) or 0)

            if actual_dataset_lower in ("1m", "candles_1m"):
                _remove_source_index_dirs_for_coin(storage_ex, actual_coin)
                rebuilt_days, rebuilt_minutes = _rebuild_source_index_from_api_for_coin(storage_ex, actual_coin)
                rebuilt_days_total += int(rebuilt_days)
                rebuilt_minutes_total += int(rebuilt_minutes)

        rebuild_msg = ""
        if rebuilt_days_total > 0:
            rebuild_msg = (
                f" Rebuilt API-only source index ({rebuilt_days_total} days, {rebuilt_minutes_total} minutes)."
            )

        return {
            "success": True,
            "exchange": _normalize_settings_exchange(exchange),
            "view": view_key,
            "deleted_count": deleted_count,
            "deleted_size": deleted_size,
            "deleted_size_label": _fmt_bytes(deleted_size),
            "message": f"Deleted {deleted_count} coin directories ({_fmt_bytes(deleted_size)}).{rebuild_msg}",
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/inventory/{exchange}/preview-delete-older")
def preview_inventory_delete_older(
    exchange: str,
    request: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        view_key = _require_inventory_view(exchange, body.get("view") or "1m")
        if view_key == "pb7_cache":
            raise ValueError("PB7 cache is read-only.")
        coins = body.get("coins") if isinstance(body.get("coins"), list) else []
        cutoff_day = str(body.get("cutoff_day") or "").strip()
        return _build_inventory_delete_older_preview(exchange, view_key, coins, cutoff_day)
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/inventory/{exchange}/delete-older")
def delete_inventory_older_than(
    exchange: str,
    request: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        view_key = _require_inventory_view(exchange, body.get("view") or "1m")
        if view_key == "pb7_cache":
            raise ValueError("PB7 cache is read-only.")

        coins = body.get("coins") if isinstance(body.get("coins"), list) else []
        cutoff_day = str(body.get("cutoff_day") or "").strip()
        if not cutoff_day or len(cutoff_day) != 8 or not cutoff_day.isdigit():
            raise ValueError("Invalid cutoff date format (expected YYYYMMDD).")

        rows = _resolve_inventory_rows_for_coins(exchange, view_key, coins)
        if not rows:
            raise ValueError("No matching coins selected.")

        storage_ex = _inventory_storage_exchange(exchange)
        deleted_count = 0
        deleted_size = 0
        coins_deleted_days: dict[str, set[str]] = {}

        for row in rows:
            actual_coin = str(row.get("coin") or "").strip()
            actual_dataset = str(row.get("dataset") or "").strip()
            actual_dataset_lower = actual_dataset.lower()
            coin_dir = get_exchange_raw_root_dir(storage_ex) / actual_dataset / actual_coin
            if not coin_dir.exists():
                continue

            for file_path in coin_dir.iterdir():
                if not file_path.is_file():
                    continue
                file_day = _extract_inventory_file_day(file_path.name)
                if not file_day or file_day >= cutoff_day:
                    continue
                file_size = int(file_path.stat().st_size)
                file_path.unlink()
                deleted_count += 1
                deleted_size += file_size
                if actual_dataset_lower in ("1m", "candles_1m"):
                    coins_deleted_days.setdefault(actual_coin, set()).add(file_day)

        updated_count = 0
        for coin_name, deleted_days in coins_deleted_days.items():
            removed = remove_days_from_index(
                exchange=storage_ex,
                coin=coin_name,
                days_to_remove=deleted_days,
            )
            if removed > 0:
                updated_count += 1

        index_msg = f" Updated {updated_count} source indexes." if updated_count > 0 else ""
        return {
            "success": True,
            "exchange": _normalize_settings_exchange(exchange),
            "view": view_key,
            "deleted_count": deleted_count,
            "deleted_size": deleted_size,
            "deleted_size_label": _fmt_bytes(deleted_size),
            "updated_indexes": updated_count,
            "message": f"Deleted {deleted_count} files ({_fmt_bytes(deleted_size)}).{index_msg}",
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/inventory/{exchange}/clear-dataset")
def clear_inventory_dataset(
    exchange: str,
    request: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    try:
        body = request if isinstance(request, dict) else {}
        view_key = _require_inventory_view(exchange, body.get("view") or "1m")
        if view_key == "pb7_cache":
            raise ValueError("PB7 cache is read-only.")

        dataset_name = str(INVENTORY_VIEW_META[view_key]["dataset"])
        storage_ex = _inventory_storage_exchange(exchange)
        dataset_dir = get_exchange_raw_root_dir(storage_ex) / dataset_name
        cleaned_indexes = 0

        if dataset_name.lower() in ("1m", "candles_1m") and dataset_dir.exists():
            for coin_dir in dataset_dir.iterdir():
                if not coin_dir.is_dir():
                    continue
                cleaned_indexes += _remove_source_index_dirs_for_coin(storage_ex, coin_dir.name)

        if dataset_dir.exists():
            shutil.rmtree(dataset_dir)

        index_msg = f" Cleaned {cleaned_indexes} source indexes." if cleaned_indexes > 0 else ""
        return {
            "success": True,
            "exchange": _normalize_settings_exchange(exchange),
            "view": view_key,
            "message": f"{INVENTORY_VIEW_META[view_key]['label']} dataset cleared.{index_msg}",
            "cleaned_indexes": cleaned_indexes,
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.get("/status/{exchange}")
def get_market_data_status(exchange: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Get market data daemon status for a specific exchange.
    
    Returns:
        {
            "exchange": "binanceusdm",
            "running": bool,
            "queued": bool,
            "status_key": "binance_latest_1m",
            "status": {...},  # Full status from status file
            "coins_done": int,
            "coins_total": int,
            "current_coin": str,
            "coins": {...}  # Per-coin details
        }
    """
    exchange_clean = exchange.lower().strip()
    status_key = _get_exchange_status_key(exchange_clean)
    flag_prefix = _get_exchange_flag_prefix(exchange_clean)
    
    if not status_key or not flag_prefix:
        return {
            "exchange": exchange_clean,
            "error": "Unknown exchange",
            "running": False,
            "queued": False,
        }
    
    # Load status
    all_status = _load_market_data_status()
    exchange_status = _filter_status_coins_to_enabled(exchange_clean, all_status.get(status_key, {}))
    
    # Check flags
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    queued = flag_path.exists()
    running = bool(exchange_status.get("running", False))
    
    return {
        "exchange": exchange_clean,
        "status_key": status_key,
        "running": running,
        "queued": queued,
        "coins_done": int(exchange_status.get("coins_done", 0)),
        "coins_total": int(exchange_status.get("coins_total", 0)),
        "current_coin": exchange_status.get("current_coin", ""),
        "interval_seconds": int(exchange_status.get("interval_seconds", 0)),
        "coins": exchange_status.get("coins", {}),
        "status": exchange_status,
    }


@router.post("/refresh-now")
def trigger_refresh_now(request: dict, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Trigger immediate refresh by creating flag file.
    
    Request body:
        {
            "exchange": "binanceusdm"|"bybit"|"hyperliquid"|"okx"
        }
    """
    exchange = request.get("exchange", "").lower().strip()
    flag_prefix = _get_exchange_flag_prefix(exchange)
    
    if not flag_prefix:
        return {"success": False, "error": "Unknown exchange"}
    
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    
    try:
        flag_path.touch()
        return {
            "success": True,
            "message": "Refresh triggered — cycle will start within seconds.",
            "exchange": exchange,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/cancel-refresh")
def cancel_refresh(request: dict, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Cancel queued refresh by removing flag file.
    
    Request body:
        {
            "exchange": "binanceusdm"|"bybit"|"hyperliquid"|"okx"
        }
    """
    exchange = request.get("exchange", "").lower().strip()
    flag_prefix = _get_exchange_flag_prefix(exchange)
    
    if not flag_prefix:
        return {"success": False, "error": "Unknown exchange"}
    
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    
    try:
        flag_path.unlink(missing_ok=True)
        return {
            "success": True,
            "message": "Queued refresh cancelled.",
            "exchange": exchange,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/stop-run")
def stop_current_run(request: dict, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Stop current run by creating stop flag file.
    
    Request body:
        {
            "exchange": "binanceusdm"|"bybit"|"hyperliquid"|"okx"
        }
    """
    exchange = request.get("exchange", "").lower().strip()
    flag_prefix = _get_exchange_flag_prefix(exchange)
    
    if not flag_prefix:
        return {"success": False, "error": "Unknown exchange"}
    
    stop_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_stop.flag"
    
    try:
        stop_path.touch()
        return {
            "success": True,
            "message": "Stop signal sent — run will abort after current coin.",
            "exchange": exchange,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
