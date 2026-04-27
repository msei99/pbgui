"""FastAPI router for the Coin Data page.

Endpoints:
    GET  /main_page           -> serve the standalone HTML page
    GET  /state               -> return filtered Coin Data page state
    POST /refresh/exchange    -> refresh selected exchange data
    POST /refresh/all         -> refresh all exchanges
    POST /refresh/cmc         -> refresh CoinMarketCap data and selected exchange
    POST /refresh/cmc_all     -> refresh CoinMarketCap data and all exchanges
"""

from __future__ import annotations

import json
import math
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.auth import SessionToken, require_auth
from Exchange import V7
from PBCoinData import CoinData, compute_coin_name
from logging_helpers import human_log as _log

SERVICE = "CoinDataUI"
router = APIRouter()

PBGDIR = Path(__file__).resolve().parent.parent
COINDATA_DIR = PBGDIR / "data" / "coindata"
SUPPORTED_EXCHANGES = V7.list()
_CMC_METADATA_CACHE_SIG: tuple[int, int] | None = None
_CMC_LINK_BY_ID_CACHE: dict[str, str] = {}
_REFRESH_JOBS_LOCK = threading.Lock()
_REFRESH_JOBS: dict[str, dict[str, Any]] = {}
_REFRESH_JOB_TTL_SECONDS = 900.0
_REFRESH_JOB_LIMIT = 64


class CoinDataRefreshRequest(BaseModel):
    exchange: str | None = None
    market_cap: float | None = None
    vol_mcap: float | None = None
    tags: list[str] = Field(default_factory=list)
    only_cpt: bool = False
    hide_notices: bool = False


def _normalize_tags(tags: list[str] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for tag in tags or []:
        value = str(tag or "").strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _ceil_to_significant(value: float, digits: int = 1) -> float:
    if value <= 0:
        return 0.0
    magnitude = math.floor(math.log10(value))
    factor = 10 ** (magnitude - digits + 1)
    rounded = math.ceil(value / factor) * factor
    decimals = max(0, digits - 1 - magnitude)
    return round(rounded, decimals)


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _prune_refresh_jobs_locked(now: float | None = None) -> None:
    current = time.time() if now is None else now
    stale_ids = [
        job_id
        for job_id, job in _REFRESH_JOBS.items()
        if current - float(job.get("updated_at") or current) > _REFRESH_JOB_TTL_SECONDS
    ]
    for job_id in stale_ids:
        _REFRESH_JOBS.pop(job_id, None)
    if len(_REFRESH_JOBS) <= _REFRESH_JOB_LIMIT:
        return
    removable = sorted(
        [
            (job_id, float(job.get("updated_at") or current))
            for job_id, job in _REFRESH_JOBS.items()
            if str(job.get("status") or "") in {"completed", "error"}
        ],
        key=lambda item: item[1],
    )
    while len(_REFRESH_JOBS) > _REFRESH_JOB_LIMIT and removable:
        job_id, _ = removable.pop(0)
        _REFRESH_JOBS.pop(job_id, None)


def _create_refresh_job(title: str, message: str, total_steps: int) -> str:
    now = time.time()
    job_id = uuid.uuid4().hex
    with _REFRESH_JOBS_LOCK:
        _prune_refresh_jobs_locked(now)
        _REFRESH_JOBS[job_id] = {
            "id": job_id,
            "title": title,
            "message": message,
            "status": "running",
            "percent": 0.0,
            "step": 0,
            "total": max(1, int(total_steps or 1)),
            "result_message": "",
            "error": "",
            "state": None,
            "created_at": now,
            "updated_at": now,
        }
    return job_id


def _set_refresh_job_progress(job_id: str, step: int, total: int, message: str) -> None:
    safe_total = max(1, int(total or 1))
    safe_step = max(0, min(int(step or 0), safe_total))
    percent = round((safe_step / safe_total) * 100.0, 1)
    with _REFRESH_JOBS_LOCK:
        job = _REFRESH_JOBS.get(job_id)
        if not job:
            return
        job["step"] = safe_step
        job["total"] = safe_total
        job["percent"] = percent
        job["message"] = message
        job["updated_at"] = time.time()


def _complete_refresh_job(job_id: str, message: str, state: dict[str, Any]) -> None:
    with _REFRESH_JOBS_LOCK:
        job = _REFRESH_JOBS.get(job_id)
        if not job:
            return
        total = max(1, int(job.get("total") or 1))
        job["status"] = "completed"
        job["step"] = total
        job["percent"] = 100.0
        job["message"] = message
        job["result_message"] = message
        job["state"] = state
        job["updated_at"] = time.time()


def _fail_refresh_job(job_id: str, message: str) -> None:
    with _REFRESH_JOBS_LOCK:
        job = _REFRESH_JOBS.get(job_id)
        if not job:
            return
        job["status"] = "error"
        job["message"] = message
        job["error"] = message
        job["updated_at"] = time.time()


def _get_refresh_job(job_id: str) -> dict[str, Any] | None:
    with _REFRESH_JOBS_LOCK:
        _prune_refresh_jobs_locked()
        job = _REFRESH_JOBS.get(job_id)
        return dict(job) if job else None


def _start_refresh_job(title: str, message: str, total_steps: int, runner: Any) -> str:
    job_id = _create_refresh_job(title, message, total_steps)

    def _worker() -> None:
        try:
            result_message, state = runner(job_id, total_steps)
            _complete_refresh_job(job_id, result_message, state)
        except Exception as exc:
            _fail_refresh_job(job_id, str(exc))

    thread = threading.Thread(target=_worker, name=f"coin-data-refresh-{job_id[:8]}", daemon=True)
    thread.start()
    return job_id


def _make_refresh_progress_cb(job_id: str) -> Any:
    def _progress(step: int, total: int, message: str) -> None:
        _set_refresh_job_progress(job_id, step, total, message)

    return _progress


def _refresh_cmc_data(coindata: CoinData, job_id: str, total_steps: int) -> None:
    _set_refresh_job_progress(job_id, 0, total_steps, "Fetching CoinMarketCap listings...")
    coindata.fetch_data()
    _set_refresh_job_progress(job_id, 1, total_steps, "Saving CoinMarketCap listings...")
    coindata.save_data()
    _set_refresh_job_progress(job_id, 2, total_steps, "Loading CoinMarketCap listings...")
    coindata.load_data()
    _set_refresh_job_progress(job_id, 3, total_steps, "Fetching CoinMarketCap metadata...")
    coindata.fetch_metadata()
    _set_refresh_job_progress(job_id, 4, total_steps, "Saving CoinMarketCap metadata...")
    coindata.save_metadata()
    _set_refresh_job_progress(job_id, 5, total_steps, "Loading CoinMarketCap metadata...")
    coindata.load_metadata()


def _file_mtime(path: Path) -> float | None:
    if path.exists():
        return path.stat().st_mtime
    return None


def _format_age(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def _format_ts(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    now_ts = datetime.now().timestamp()
    dt = datetime.fromtimestamp(ts)
    age = max(0.0, now_ts - ts)
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} ({_format_age(age)})"


def _load_cmc_link_map() -> dict[str, str]:
    global _CMC_METADATA_CACHE_SIG, _CMC_LINK_BY_ID_CACHE

    metadata_path = COINDATA_DIR / "metadata.json"
    if not metadata_path.exists():
        _CMC_METADATA_CACHE_SIG = None
        _CMC_LINK_BY_ID_CACHE = {}
        return _CMC_LINK_BY_ID_CACHE

    stat = metadata_path.stat()
    file_sig = (stat.st_mtime_ns, stat.st_size)
    if _CMC_METADATA_CACHE_SIG == file_sig:
        return _CMC_LINK_BY_ID_CACHE

    link_map: dict[str, str] = {}
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        data = payload.get("data") if isinstance(payload, dict) else {}
        if isinstance(data, dict):
            for key, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                slug = str(entry.get("slug") or "").strip()
                if not slug:
                    continue
                cmc_id = entry.get("id") or key
                link_map[str(cmc_id)] = f"https://coinmarketcap.com/currencies/{slug}"
    except Exception:
        link_map = {}

    _CMC_METADATA_CACHE_SIG = file_sig
    _CMC_LINK_BY_ID_CACHE = link_map
    return _CMC_LINK_BY_ID_CACHE


def _cmc_link_for_row(row: dict[str, Any], cmc_links: dict[str, str]) -> str:
    direct_link = str(row.get("link") or "").strip()
    if direct_link:
        return direct_link

    slug = str(row.get("slug") or "").strip()
    if slug:
        return f"https://coinmarketcap.com/currencies/{slug}"

    cmc_id = row.get("cmc_id")
    if cmc_id is None or cmc_id == "":
        return ""
    return str(cmc_links.get(str(cmc_id)) or "")


def _max_price_ts(mapping_rows: list[dict[str, Any]]) -> float | None:
    max_ts = None
    for row in mapping_rows:
        ts = row.get("price_ts")
        if ts is None:
            continue
        try:
            ts_f = float(ts)
        except Exception:
            continue
        if ts_f > 1_000_000_000_000:
            ts_f = ts_f / 1000.0
        if max_ts is None or ts_f > max_ts:
            max_ts = ts_f
    return max_ts


def _latest_timestamp(*timestamps: float | None) -> float | None:
    available = [ts for ts in timestamps if ts is not None]
    if not available:
        return None
    return max(available)


def _refresh_single_exchange(
    coindata: CoinData,
    exchange_id: str,
    *,
    progress_cb: Any | None = None,
    step_offset: int = 0,
    total_steps: int = 5,
) -> None:
    if progress_cb:
        progress_cb(step_offset, total_steps, f"{exchange_id}: fetching markets...")
    coindata.fetch_ccxt_markets(exchange_id)
    if progress_cb:
        progress_cb(step_offset + 1, total_steps, f"{exchange_id}: loading markets...")
    markets = coindata.load_ccxt_markets(exchange_id)
    if progress_cb:
        progress_cb(step_offset + 2, total_steps, f"{exchange_id}: updating copy-trading cache...")
    coindata.fetch_copy_trading_symbols(exchange_id, markets)
    if progress_cb:
        progress_cb(step_offset + 3, total_steps, f"{exchange_id}: rebuilding mapping...")
    coindata.build_mapping(exchange_id)
    if progress_cb:
        progress_cb(step_offset + 4, total_steps, f"{exchange_id}: updating prices...")
    coindata.update_prices(exchange_id)
    if progress_cb:
        progress_cb(step_offset + 5, total_steps, f"{exchange_id}: refreshed.")


def _new_coindata(
    exchange: str | None = None,
    market_cap: float | None = None,
    vol_mcap: float | None = None,
    tags: list[str] | None = None,
    only_cpt: bool = False,
    hide_notices: bool = False,
) -> CoinData:
    coindata = CoinData()
    supported_exchanges = [exchange_id for exchange_id in coindata.exchanges if exchange_id in SUPPORTED_EXCHANGES]
    if supported_exchanges and coindata.exchange not in supported_exchanges:
        coindata.exchange = supported_exchanges[0]
    if exchange:
        exchange_key = str(exchange).strip().lower()
        if exchange_key in supported_exchanges:
            coindata.exchange = exchange_key
            try:
                coindata.exchange_index = coindata.exchanges.index(exchange_key)
            except Exception:
                pass
    coindata.market_cap = max(0.0, _coerce_float(market_cap, coindata.market_cap))
    coindata.vol_mcap = max(0.0, _coerce_float(vol_mcap, coindata.vol_mcap))
    coindata.tags = _normalize_tags(tags)
    coindata.only_cpt = bool(only_cpt)
    coindata.notices_ignore = bool(hide_notices)
    return coindata


def _serialize_main_row(row: dict[str, Any], cmc_links: dict[str, str]) -> dict[str, Any]:
    return {
        "coin": str(row.get("coin") or ""),
        "ccxt_symbol": str(row.get("ccxt_symbol") or ""),
        "base": str(row.get("base") or ""),
        "quote": str(row.get("quote") or ""),
        "copy_trading": bool(row.get("copy_trading", False)),
        "cmc_id": row.get("cmc_id"),
        "cmc_rank": row.get("cmc_rank"),
        "cmc_link": _cmc_link_for_row(row, cmc_links),
        "price": _as_float(row.get("price")),
        "market_cap": _as_float(row.get("market_cap")),
        "volume_24h": _as_float(row.get("volume_24h")),
        "vol_mcap": _as_float(row.get("vol/mcap")),
        "tags": [str(tag) for tag in (row.get("tags") or []) if tag],
        "notice": str(row.get("notice") or ""),
        "contract_size": row.get("contract_size"),
        "min_amount": row.get("min_amount"),
        "min_cost": row.get("min_cost"),
        "precision_amount": row.get("precision_amount"),
        "max_leverage": row.get("max_leverage"),
        "min_order_price": row.get("min_order_price"),
    }


def _serialize_hip3_row(row: dict[str, Any], cmc_links: dict[str, str]) -> dict[str, Any]:
    return {
        "dex": str(row.get("dex") or ""),
        "coin": str(row.get("coin") or ""),
        "ccxt_symbol": str(row.get("ccxt_symbol") or ""),
        "quote": str(row.get("quote") or ""),
        "cmc_link": _cmc_link_for_row(row, cmc_links),
        "price": _as_float(row.get("price_last") or row.get("price")),
        "volume_24h": _as_float(row.get("volume_24h")),
        "copy_trading": bool(row.get("copy_trading", False)),
        "notice": str(row.get("notice") or ""),
        "contract_size": row.get("contract_size"),
        "min_amount": row.get("min_amount"),
        "min_cost": row.get("min_cost"),
        "precision_amount": row.get("precision_amount"),
        "max_leverage": row.get("max_leverage"),
        "min_order_price": row.get("min_order_price"),
    }


def _build_state(
    exchange: str | None = None,
    market_cap: float | None = None,
    vol_mcap: float | None = None,
    tags: list[str] | None = None,
    only_cpt: bool = False,
    hide_notices: bool = False,
) -> dict[str, Any]:
    coindata = _new_coindata(
        exchange=exchange,
        market_cap=market_cap,
        vol_mcap=vol_mcap,
        tags=tags,
        only_cpt=only_cpt,
        hide_notices=hide_notices,
    )
    warnings: list[str] = []
    supported_exchanges = [exchange_id for exchange_id in coindata.exchanges if exchange_id in SUPPORTED_EXCHANGES]
    if supported_exchanges and coindata.exchange not in supported_exchanges:
        coindata.exchange = supported_exchanges[0]

    mapping_rows = coindata.load_exchange_mapping(coindata.exchange)
    if not mapping_rows:
        try:
            coindata.build_mapping(coindata.exchange)
            coindata.update_prices(coindata.exchange)
        except Exception as exc:
            warnings.append(f"Failed to build mapping for {coindata.exchange}: {exc}")
        mapping_rows = coindata.load_exchange_mapping(coindata.exchange)

    if not mapping_rows:
        warnings.append(
            f"No mapping data available for {coindata.exchange}. Refresh market data and try again."
        )

    if coindata.exchange == "hyperliquid" and mapping_rows and not any(
        row.get("is_hip3", False) for row in mapping_rows
    ):
        try:
            _refresh_single_exchange(coindata, "hyperliquid")
            mapping_rows = coindata.load_exchange_mapping("hyperliquid")
        except Exception as exc:
            warnings.append(f"Hyperliquid HIP-3 rebuild failed: {exc}")

    exchange_dir = COINDATA_DIR / coindata.exchange
    cmc_data_ts = _file_mtime(COINDATA_DIR / "coindata.json")
    cmc_metadata_ts = _file_mtime(COINDATA_DIR / "metadata.json")
    cmc_refresh_ts = _latest_timestamp(cmc_data_ts, cmc_metadata_ts)
    ccxt_markets_ts = _file_mtime(exchange_dir / "ccxt_markets.json")
    mapping_ts = _file_mtime(exchange_dir / "mapping.json")
    cpt_cache_ts = _file_mtime(exchange_dir / "copy_trading.json")
    prices_ts = _max_price_ts(mapping_rows)
    exchange_refresh_ts = _latest_timestamp(ccxt_markets_ts, mapping_ts, prices_ts, cpt_cache_ts)

    available_quotes = sorted(
        {
            (row.get("quote") or "").upper()
            for row in mapping_rows
            if row.get("quote")
        }
    )
    preferred_quotes = ["USDT"]
    if coindata.exchange == "hyperliquid":
        preferred_quotes = ["USDC", "USDT0"]
    quote_filter = [quote for quote in preferred_quotes if quote in available_quotes]
    if not quote_filter:
        quote_filter = list(available_quotes)

    mapping_tags = coindata.get_mapping_tags(coindata.exchange, quote_filter=quote_filter)
    selected_tags = [tag for tag in _normalize_tags(tags) if tag in mapping_tags]
    coindata.tags = selected_tags

    unmatched_all = [
        row for row in mapping_rows if row.get("cmc_id") is None and not row.get("is_hip3", False)
    ]
    unmatched_visible = [
        row for row in unmatched_all if (row.get("quote") or "").upper() in quote_filter
    ]
    unmatched_display: list[dict[str, Any]] = []
    for row in unmatched_visible:
        quote = (row.get("quote") or "").upper()
        symbol = row.get("symbol") or ""
        unmatched_display.append(
            {
                "coin": compute_coin_name(symbol, quote),
                "symbol": symbol,
                "base": row.get("base"),
                "quote": row.get("quote"),
                "ccxt_symbol": row.get("ccxt_symbol"),
            }
        )
    unmatched_display = sorted(
        {entry["symbol"]: entry for entry in unmatched_display}.values(),
        key=lambda item: (str(item.get("coin") or ""), str(item.get("symbol") or "")),
    )

    filtered_rows_all = coindata.filter_mapping_rows(
        exchange=coindata.exchange,
        market_cap_min_m=coindata.market_cap,
        vol_mcap_max=coindata.vol_mcap,
        only_cpt=coindata.only_cpt,
        notices_ignore=coindata.notices_ignore,
        tags=coindata.tags,
        quote_filter=quote_filter,
    )
    hip3_rows = [
        row
        for row in mapping_rows
        if row.get("is_hip3", False)
        and bool(row.get("active", True))
        and bool(row.get("linear", True))
    ]
    if coindata.exchange == "hyperliquid":
        hip3_rows.sort(key=lambda row: (str(row.get("coin") or ""), str(row.get("symbol") or "")))

    vol_mcap_candidate_rows = coindata.filter_mapping_rows(
        exchange=coindata.exchange,
        market_cap_min_m=coindata.market_cap,
        vol_mcap_max=float("inf"),
        only_cpt=coindata.only_cpt,
        notices_ignore=coindata.notices_ignore,
        tags=coindata.tags,
        quote_filter=quote_filter,
    )
    vol_mcap_values = sorted(
        {
            _ceil_to_significant(value, digits=1)
            for row in vol_mcap_candidate_rows
            if not row.get("is_hip3", False)
            for value in [_as_float(row.get("vol/mcap"))]
            if value is not None and value > 0.0
        }
    )

    filtered_rows = [row for row in filtered_rows_all if not row.get("is_hip3", False)]
    cmc_links = _load_cmc_link_map()

    return {
        "filters": {
            "exchange": coindata.exchange,
            "market_cap": coindata.market_cap,
            "vol_mcap": coindata.vol_mcap,
            "tags": selected_tags,
            "only_cpt": bool(coindata.only_cpt),
            "hide_notices": bool(coindata.notices_ignore),
        },
        "options": {
            "exchanges": supported_exchanges,
            "tags": mapping_tags,
            "quote_filter": quote_filter,
            "vol_mcap_values": vol_mcap_values,
        },
        "meta": {
            "cmc_line": (
                f"CMC refreshed {_format_age(max(0.0, datetime.now().timestamp() - cmc_refresh_ts))}"
                if cmc_refresh_ts is not None
                else "CMC refresh status unavailable"
            ),
            "cmc_line_detail": f"CMC - Listings: {_format_ts(cmc_data_ts)} - Metadata: {_format_ts(cmc_metadata_ts)}",
            "exchange_line": (
                f"{coindata.exchange} refreshed {_format_age(max(0.0, datetime.now().timestamp() - exchange_refresh_ts))}"
                if exchange_refresh_ts is not None
                else f"{coindata.exchange} refresh status unavailable"
            ),
            "exchange_line_detail": (
                f"{coindata.exchange} - Markets: {_format_ts(ccxt_markets_ts)} - "
                f"Mapping: {_format_ts(mapping_ts)} - Prices: {_format_ts(prices_ts)} - "
                f"CPT cache: {_format_ts(cpt_cache_ts)}"
            ),
            "timestamps": {
                "cmc_data": cmc_data_ts,
                "cmc_metadata": cmc_metadata_ts,
                "ccxt_markets": ccxt_markets_ts,
                "mapping": mapping_ts,
                "prices": prices_ts,
                "copy_trading": cpt_cache_ts,
            },
        },
        "counts": {
            "main": len(filtered_rows),
            "unmatched_visible": len(unmatched_display),
            "unmatched_all": len(unmatched_all),
            "hip3": len(hip3_rows),
        },
        "sections": {
            "unmatched_title": (
                f"CMC unmatched ({coindata.exchange}) - "
                f"{', '.join(quote_filter) if quote_filter else 'all'}: {len(unmatched_display)}, "
                f"all quotes: {len(unmatched_all)}"
            ),
            "main_title": f"Filtered symbols ({len(filtered_rows)})",
            "hip3_title": f"HIP-3 symbols ({len(hip3_rows)})",
        },
        "warnings": warnings,
        "rows": [_serialize_main_row(row, cmc_links) for row in filtered_rows],
        "unmatched_rows": unmatched_display,
        "hip3_rows": [_serialize_hip3_row(row, cmc_links) for row in hip3_rows],
    }


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "coin_data.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/coin-data"

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/state")
def get_state(
    exchange: str | None = Query(default=None),
    market_cap: float | None = Query(default=None),
    vol_mcap: float | None = Query(default=None),
    tags: list[str] | None = Query(default=None),
    only_cpt: bool = Query(default=False),
    hide_notices: bool = Query(default=False),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    return _build_state(
        exchange=exchange,
        market_cap=market_cap,
        vol_mcap=vol_mcap,
        tags=tags,
        only_cpt=only_cpt,
        hide_notices=hide_notices,
    )


@router.get("/refresh/jobs/{job_id}")
def get_refresh_job(
    job_id: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    job = _get_refresh_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Refresh job not found")
    return {"job": job}


@router.post("/refresh/exchange")
def refresh_exchange(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    total_steps = 6

    def _runner(job_id: str, _: int) -> tuple[str, dict[str, Any]]:
        try:
            _refresh_single_exchange(
                coindata,
                coindata.exchange,
                progress_cb=_make_refresh_progress_cb(job_id),
                step_offset=0,
                total_steps=total_steps,
            )
            _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
            state = _build_state(
                exchange=coindata.exchange,
                market_cap=coindata.market_cap,
                vol_mcap=coindata.vol_mcap,
                tags=coindata.tags,
                only_cpt=coindata.only_cpt,
                hide_notices=coindata.notices_ignore,
            )
            return f"Refreshed {coindata.exchange}", state
        except Exception as exc:
            _log(SERVICE, f"Refresh selected exchange failed for {coindata.exchange}: {exc}", level="ERROR")
            raise RuntimeError(f"Failed to refresh {coindata.exchange}: {exc}")

    job_id = _start_refresh_job(
        f"Refreshing {coindata.exchange}...",
        f"{coindata.exchange}: fetching markets...",
        total_steps,
        _runner,
    )
    return {"ok": True, "job_id": job_id}


@router.post("/refresh/all")
def refresh_all(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    exchanges = V7.list()
    total_steps = len(exchanges) * 5 + 1

    def _runner(job_id: str, _: int) -> tuple[str, dict[str, Any]]:
        errors: list[str] = []
        try:
            for index, exchange_id in enumerate(exchanges):
                step_offset = index * 5
                try:
                    _refresh_single_exchange(
                        coindata,
                        exchange_id,
                        progress_cb=_make_refresh_progress_cb(job_id),
                        step_offset=step_offset,
                        total_steps=total_steps,
                    )
                except Exception as exc:
                    errors.append(f"{exchange_id}: {exc}")
            if errors:
                raise RuntimeError("Some exchanges failed to refresh: " + "; ".join(errors))
            _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
            state = _build_state(
                exchange=coindata.exchange,
                market_cap=coindata.market_cap,
                vol_mcap=coindata.vol_mcap,
                tags=coindata.tags,
                only_cpt=coindata.only_cpt,
                hide_notices=coindata.notices_ignore,
            )
            return "All exchanges refreshed", state
        except Exception as exc:
            _log(SERVICE, f"Refresh all exchanges failed: {exc}", level="ERROR")
            raise

    first_exchange = exchanges[0] if exchanges else "exchange"
    job_id = _start_refresh_job(
        "Refreshing all exchanges...",
        f"{first_exchange}: fetching markets...",
        total_steps,
        _runner,
    )
    return {"ok": True, "job_id": job_id}


@router.post("/refresh/cmc")
def refresh_cmc(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    total_steps = 12

    def _runner(job_id: str, _: int) -> tuple[str, dict[str, Any]]:
        try:
            _refresh_cmc_data(coindata, job_id, total_steps)
            _refresh_single_exchange(
                coindata,
                coindata.exchange,
                progress_cb=_make_refresh_progress_cb(job_id),
                step_offset=6,
                total_steps=total_steps,
            )
            _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
            state = _build_state(
                exchange=coindata.exchange,
                market_cap=coindata.market_cap,
                vol_mcap=coindata.vol_mcap,
                tags=coindata.tags,
                only_cpt=coindata.only_cpt,
                hide_notices=coindata.notices_ignore,
            )
            return "CoinMarketCap data refreshed", state
        except Exception as exc:
            _log(SERVICE, f"Refresh CoinMarketCap data failed for {coindata.exchange}: {exc}", level="ERROR")
            raise RuntimeError(f"Failed to refresh CoinMarketCap data: {exc}")

    job_id = _start_refresh_job(
        "Refreshing CMC + selected exchange...",
        "Fetching CoinMarketCap listings...",
        total_steps,
        _runner,
    )
    return {"ok": True, "job_id": job_id}


@router.post("/refresh/cmc_all")
def refresh_cmc_all(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    exchanges = V7.list()
    total_steps = 6 + (len(exchanges) * 5) + 1

    def _runner(job_id: str, _: int) -> tuple[str, dict[str, Any]]:
        errors: list[str] = []
        try:
            _refresh_cmc_data(coindata, job_id, total_steps)
            for index, exchange_id in enumerate(exchanges):
                step_offset = 6 + (index * 5)
                try:
                    _refresh_single_exchange(
                        coindata,
                        exchange_id,
                        progress_cb=_make_refresh_progress_cb(job_id),
                        step_offset=step_offset,
                        total_steps=total_steps,
                    )
                except Exception as exc:
                    errors.append(f"{exchange_id}: {exc}")
            if errors:
                raise RuntimeError("Some exchanges failed to refresh after CoinMarketCap update: " + "; ".join(errors))
            _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
            state = _build_state(
                exchange=coindata.exchange,
                market_cap=coindata.market_cap,
                vol_mcap=coindata.vol_mcap,
                tags=coindata.tags,
                only_cpt=coindata.only_cpt,
                hide_notices=coindata.notices_ignore,
            )
            return "CoinMarketCap data and all exchanges refreshed", state
        except Exception as exc:
            _log(SERVICE, f"Refresh CoinMarketCap data and all exchanges failed: {exc}", level="ERROR")
            raise RuntimeError(f"Failed to refresh CoinMarketCap data and all exchanges: {exc}")

    first_exchange = exchanges[0] if exchanges else "exchange"
    job_id = _start_refresh_job(
        "Refreshing CMC + all exchanges...",
        f"Fetching CoinMarketCap listings before {first_exchange}...",
        total_steps,
        _runner,
    )
    return {"ok": True, "job_id": job_id}