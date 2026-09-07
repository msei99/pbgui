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

import asyncio
import json
import math
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.auth import SessionToken, require_auth
from api.page_templates import render_page_urls, script_json
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
_REFRESH_JOBS_LOCK = threading.RLock()
_REFRESH_JOBS: dict[str, dict[str, Any]] = {}
_REFRESH_THREADS: dict[str, threading.Thread] = {}
_REFRESH_ACCEPTING = True
_REFRESH_JOB_TTL_SECONDS = 900.0
_REFRESH_JOB_LIMIT = 64
_REFRESH_ACTIVE_JOB_LIMIT = 4


class CoinDataRefreshRequest(BaseModel):
    exchange: str | None = None
    market_cap: float | None = None
    vol_mcap: float | None = None
    tags: list[str] = Field(default_factory=list)
    only_cpt: bool = False
    hide_notices: bool = False
    quotes: list[str] | None = None


def _validate_nonempty_quote_selection(quotes: list[str] | None) -> None:
    """Reject an explicitly empty quote selection while preserving omitted defaults."""
    if quotes is not None and not quotes:
        raise HTTPException(status_code=422, detail="Select at least one quote")


@dataclass(frozen=True)
class _RefreshJobOutcome:
    """Terminal refresh state retained for authenticated job polling."""

    status: str
    message: str
    state: dict[str, Any] | None
    exchange_results: list[dict[str, Any]]
    warnings: list[str]


def _refresh_job_key(
    action: str,
    coindata: CoinData,
    payload: CoinDataRefreshRequest,
) -> str:
    """Return a stable identity for one exact refresh request."""
    values = {
        "action": action,
        "exchange": coindata.exchange,
        "market_cap": coindata.market_cap,
        "vol_mcap": coindata.vol_mcap,
        "tags": sorted(coindata.tags),
        "only_cpt": bool(coindata.only_cpt),
        "hide_notices": bool(coindata.notices_ignore),
        "quotes": (
            None
            if payload.quotes is None
            else sorted(str(quote).strip().upper() for quote in payload.quotes)
        ),
    }
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


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


def _select_quotes(
    exchange: str,
    available_quotes: list[str],
    requested_quotes: list[str] | None,
) -> list[str]:
    """Validate an explicit quote selection or apply the exchange default."""
    _validate_nonempty_quote_selection(requested_quotes)
    if requested_quotes is not None:
        normalized: list[str] = []
        for raw_quote in requested_quotes:
            quote = str(raw_quote or "").strip().upper()
            if not re.fullmatch(r"[A-Z0-9]{2,16}", quote):
                raise HTTPException(status_code=422, detail=f"Invalid quote: {raw_quote}")
            if quote not in normalized:
                normalized.append(quote)
        unavailable = [quote for quote in normalized if quote not in available_quotes]
        if unavailable:
            raise HTTPException(
                status_code=422,
                detail=f"Quote not available for {exchange}: {', '.join(unavailable)}",
            )
        return [quote for quote in available_quotes if quote in normalized]

    preferred_quotes = ["USDC", "USDT0"] if exchange == "hyperliquid" else ["USDT"]
    selected_quotes = [quote for quote in preferred_quotes if quote in available_quotes]
    return selected_quotes or list(available_quotes)


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
        if str(job.get("status") or "") in {"completed", "partial", "error"}
        and current - float(job.get("updated_at") or current) > _REFRESH_JOB_TTL_SECONDS
    ]
    for job_id in stale_ids:
        _REFRESH_JOBS.pop(job_id, None)
    if len(_REFRESH_JOBS) <= _REFRESH_JOB_LIMIT:
        return
    removable = sorted(
        [
            (job_id, float(job.get("updated_at") or current))
            for job_id, job in _REFRESH_JOBS.items()
            if str(job.get("status") or "") in {"completed", "partial", "error"}
        ],
        key=lambda item: item[1],
    )
    while len(_REFRESH_JOBS) > _REFRESH_JOB_LIMIT and removable:
        job_id, _ = removable.pop(0)
        _REFRESH_JOBS.pop(job_id, None)


def _create_refresh_job(
    title: str,
    message: str,
    total_steps: int,
    *,
    coalesce_key: str | None = None,
) -> str:
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
            "exchange_results": [],
            "warnings": [],
            "_coalesce_key": coalesce_key,
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


def _complete_refresh_job(
    job_id: str,
    message: str,
    state: dict[str, Any] | None,
    *,
    status: str = "completed",
    exchange_results: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> None:
    with _REFRESH_JOBS_LOCK:
        job = _REFRESH_JOBS.get(job_id)
        if not job:
            return
        total = max(1, int(job.get("total") or 1))
        job["status"] = status
        job["step"] = total
        job["percent"] = 100.0
        job["message"] = message
        job["result_message"] = message
        job["state"] = state
        job["exchange_results"] = list(exchange_results or [])
        job["warnings"] = list(warnings or [])
        job["updated_at"] = time.time()


def _fail_refresh_job(
    job_id: str,
    message: str,
    *,
    state: dict[str, Any] | None = None,
    exchange_results: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> None:
    with _REFRESH_JOBS_LOCK:
        job = _REFRESH_JOBS.get(job_id)
        if not job:
            return
        job["status"] = "error"
        job["message"] = message
        job["error"] = message
        job["state"] = state
        job["exchange_results"] = list(exchange_results or [])
        job["warnings"] = list(warnings or [])
        job["updated_at"] = time.time()


def _get_refresh_job(job_id: str) -> dict[str, Any] | None:
    with _REFRESH_JOBS_LOCK:
        _prune_refresh_jobs_locked()
        job = _REFRESH_JOBS.get(job_id)
        if not job:
            return None
        result = dict(job)
        result.pop("_coalesce_key", None)
        return result


def _start_refresh_job(
    title: str,
    message: str,
    total_steps: int,
    runner: Any,
    *,
    coalesce_key: str,
) -> str:
    with _REFRESH_JOBS_LOCK:
        if not _REFRESH_ACCEPTING:
            raise HTTPException(status_code=503, detail="Coin Data refresh is shutting down")
        _prune_refresh_jobs_locked()
        for existing_id, job in _REFRESH_JOBS.items():
            if job.get("status") == "running" and job.get("_coalesce_key") == coalesce_key:
                return existing_id
        active_jobs = sum(1 for job in _REFRESH_JOBS.values() if job.get("status") == "running")
        if active_jobs >= _REFRESH_ACTIVE_JOB_LIMIT:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Coin Data already has {active_jobs} active refresh jobs; "
                    "wait for one to finish and retry"
                ),
                headers={"Retry-After": "2"},
            )
        job_id = _create_refresh_job(
            title,
            message,
            total_steps,
            coalesce_key=coalesce_key,
        )

    def _worker() -> None:
        try:
            outcome = runner(job_id, total_steps)
            if outcome.status == "error":
                _fail_refresh_job(
                    job_id,
                    outcome.message,
                    state=outcome.state,
                    exchange_results=outcome.exchange_results,
                    warnings=outcome.warnings,
                )
            else:
                _complete_refresh_job(
                    job_id,
                    outcome.message,
                    outcome.state,
                    status=outcome.status,
                    exchange_results=outcome.exchange_results,
                    warnings=outcome.warnings,
                )
        except Exception as exc:
            _fail_refresh_job(job_id, str(exc))
        finally:
            with _REFRESH_JOBS_LOCK:
                _REFRESH_THREADS.pop(job_id, None)

    thread = threading.Thread(target=_worker, name=f"coin-data-refresh-{job_id[:8]}")
    with _REFRESH_JOBS_LOCK:
        _REFRESH_THREADS[job_id] = thread
        try:
            thread.start()
        except Exception as exc:
            _REFRESH_THREADS.pop(job_id, None)
            _fail_refresh_job(job_id, str(exc))
            raise
    return job_id


def startup() -> None:
    """Allow Coin Data refresh jobs for a newly started API lifespan."""
    global _REFRESH_ACCEPTING
    with _REFRESH_JOBS_LOCK:
        _REFRESH_ACCEPTING = True


def restart_block_reason() -> str:
    """Return a reason while Coin Data is writing refreshed files."""
    with _REFRESH_JOBS_LOCK:
        active = sum(1 for thread in _REFRESH_THREADS.values() if thread.is_alive())
    if active:
        return f"Coin Data has {active} active refresh job(s)"
    return ""


async def shutdown() -> None:
    """Stop accepting refreshes and join active file-mutating workers."""
    global _REFRESH_ACCEPTING
    with _REFRESH_JOBS_LOCK:
        _REFRESH_ACCEPTING = False
        threads = [thread for thread in _REFRESH_THREADS.values() if thread.ident is not None]
    if threads:
        await asyncio.gather(
            *(asyncio.to_thread(thread.join) for thread in threads),
            return_exceptions=True,
        )
    with _REFRESH_JOBS_LOCK:
        _REFRESH_THREADS.clear()


def _make_refresh_progress_cb(job_id: str) -> Any:
    def _progress(step: int, total: int, message: str) -> None:
        _set_refresh_job_progress(job_id, step, total, message)

    return _progress


def _refresh_cmc_data(coindata: CoinData, job_id: str, total_steps: int) -> None:
    _set_refresh_job_progress(job_id, 0, total_steps, "Fetching CoinMarketCap listings...")
    if coindata.fetch_data() is False:
        raise RuntimeError("CoinMarketCap listings fetch returned False")
    _set_refresh_job_progress(job_id, 1, total_steps, "CoinMarketCap listings published.")
    _set_refresh_job_progress(job_id, 2, total_steps, "Loading CoinMarketCap listings...")
    coindata.load_data()
    _set_refresh_job_progress(job_id, 3, total_steps, "Fetching CoinMarketCap metadata...")
    if coindata.fetch_metadata() is False:
        raise RuntimeError("CoinMarketCap metadata fetch returned False")
    _set_refresh_job_progress(job_id, 4, total_steps, "CoinMarketCap metadata published.")
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
) -> dict[str, Any]:
    """Return one normalized result, including False returns and exceptions."""
    try:
        raw_result = coindata.refresh_exchange_mapping(
            exchange_id,
            progress_cb=progress_cb,
            step_offset=step_offset,
            total_steps=total_steps,
        )
    except Exception as exc:
        _log(SERVICE, f"Refresh failed for {exchange_id}: {exc}", level="ERROR")
        return {
            "exchange": exchange_id,
            "markets_ok": False,
            "mapping_ok": False,
            "prices_ok": False,
            "ok": False,
            "error": str(exc),
        }

    if not isinstance(raw_result, dict):
        return {
            "exchange": exchange_id,
            "markets_ok": False,
            "mapping_ok": False,
            "prices_ok": False,
            "ok": False,
            "error": f"refresh_exchange_mapping returned {raw_result!r}",
        }

    result = dict(raw_result)
    result["exchange"] = exchange_id
    result["ok"] = bool(result.get("ok"))
    price_update = result.get("price_update")
    result["partial"] = bool(
        result.get("partial")
        or (isinstance(price_update, dict) and price_update.get("partial"))
    )
    if not result["ok"] and not result.get("error"):
        if result["partial"] and isinstance(price_update, dict):
            result["error"] = (
                f"partial price coverage: {int(price_update.get('priced') or 0)}/"
                f"{int(price_update.get('requested') or 0)} priced, "
                f"{int(price_update.get('missing') or 0)} missing"
            )
        else:
            result["error"] = (
                f"stages markets={bool(result.get('markets_ok'))}, "
                f"mapping={bool(result.get('mapping_ok'))}, prices={bool(result.get('prices_ok'))}"
            )
    return result


def _exchange_result_warning(result: dict[str, Any]) -> str:
    exchange = str(result.get("exchange") or "exchange")
    return f"{exchange}: {result.get('error') or 'refresh failed'}"


def _batch_refresh_outcome(
    exchange_results: list[dict[str, Any]],
    state: dict[str, Any] | None,
    success_message: str,
) -> _RefreshJobOutcome:
    """Classify all-success, partial, and zero-success exchange batches."""
    successful = [result for result in exchange_results if bool(result.get("ok"))]
    partial = [
        result
        for result in exchange_results
        if not bool(result.get("ok")) and bool(result.get("partial"))
    ]
    failed = [
        result
        for result in exchange_results
        if not bool(result.get("ok")) and not bool(result.get("partial"))
    ]
    incomplete = partial + failed
    warnings = [_exchange_result_warning(result) for result in incomplete]
    total = len(exchange_results)
    if total and len(successful) == total:
        return _RefreshJobOutcome("completed", success_message, state, exchange_results, [])
    if successful or partial:
        usable = len(successful) + len(partial)
        message = (
            f"{usable}/{total} exchanges returned usable data with incomplete results: "
            + ", ".join(str(result.get("exchange") or "exchange") for result in incomplete)
        )
        return _RefreshJobOutcome("partial", message, state, exchange_results, warnings)
    message = "No exchanges refreshed successfully"
    if failed:
        message += "; failed: " + ", ".join(
            str(result.get("exchange") or "exchange") for result in failed
        )
    return _RefreshJobOutcome("error", message, state, exchange_results, warnings)


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


def _require_cmc_pool_ready(coindata: CoinData) -> None:
    """Reject CMC jobs only when this host has no active local pool key."""
    status = coindata.cmc_pool_status()
    if status.get("error"):
        raise HTTPException(status_code=503, detail="CoinMarketCap pool status is unavailable")
    if int(status.get("active_credentials") or 0) < 1:
        raise HTTPException(
            status_code=409,
            detail="No active local CoinMarketCap pool key is available",
        )


def _serialize_main_row(row: dict[str, Any], cmc_links: dict[str, str]) -> dict[str, Any]:
    raw_cmc_rank = row.get("cmc_rank")
    cmc_rank = (
        int(raw_cmc_rank)
        if isinstance(raw_cmc_rank, (int, float))
        and not isinstance(raw_cmc_rank, bool)
        and math.isfinite(float(raw_cmc_rank))
        and float(raw_cmc_rank).is_integer()
        and raw_cmc_rank > 0
        else None
    )
    return {
        "coin": str(row.get("coin") or ""),
        "symbol": str(row.get("symbol") or ""),
        "ccxt_symbol": str(row.get("ccxt_symbol") or ""),
        "base": str(row.get("base") or ""),
        "quote": str(row.get("quote") or ""),
        "copy_trading": bool(row.get("copy_trading", False)),
        "cmc_id": row.get("cmc_id"),
        "cmc_rank": cmc_rank,
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
        "symbol": str(row.get("symbol") or ""),
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
    quotes: list[str] | None = None,
    allow_refresh: bool = True,
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
    if not mapping_rows and allow_refresh:
        result = _refresh_single_exchange(coindata, coindata.exchange)
        if not result.get("ok"):
            warnings.append(f"Failed to build mapping: {_exchange_result_warning(result)}")
        mapping_rows = coindata.load_exchange_mapping(coindata.exchange)

    if not mapping_rows:
        warnings.append(
            f"No mapping data available for {coindata.exchange}. Refresh market data and try again."
        )

    if allow_refresh and coindata.exchange == "hyperliquid" and mapping_rows and not any(
        row.get("is_hip3", False) for row in mapping_rows
    ):
        result = _refresh_single_exchange(coindata, "hyperliquid")
        if result.get("ok"):
            mapping_rows = coindata.load_exchange_mapping("hyperliquid")
        else:
            warnings.append(f"Hyperliquid HIP-3 rebuild failed: {_exchange_result_warning(result)}")

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
    quote_filter = _select_quotes(coindata.exchange, available_quotes, quotes)

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
        and (row.get("quote") or "").upper() in quote_filter
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
    cmc_pool = coindata.cmc_pool_status()

    return {
        "cmc_pool": cmc_pool,
        "available_quotes": available_quotes,
        "selected_quotes": quote_filter,
        "filters": {
            "exchange": coindata.exchange,
            "market_cap": coindata.market_cap,
            "vol_mcap": coindata.vol_mcap,
            "tags": selected_tags,
            "only_cpt": bool(coindata.only_cpt),
            "hide_notices": bool(coindata.notices_ignore),
            "quotes": quote_filter,
        },
        "options": {
            "exchanges": supported_exchanges,
            "tags": mapping_tags,
            "available_quotes": available_quotes,
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


def _build_refresh_state(coindata: CoinData, payload: CoinDataRefreshRequest) -> dict[str, Any]:
    """Read the post-refresh state without triggering another exchange refresh."""
    return _build_state(
        exchange=coindata.exchange,
        market_cap=coindata.market_cap,
        vol_mcap=coindata.vol_mcap,
        tags=coindata.tags,
        only_cpt=coindata.only_cpt,
        hide_notices=coindata.notices_ignore,
        quotes=payload.quotes,
        allow_refresh=False,
    )


def _attach_optional_refresh_state(
    outcome: _RefreshJobOutcome,
    coindata: CoinData,
    payload: CoinDataRefreshRequest,
) -> _RefreshJobOutcome:
    """Attach current page state without replacing primary refresh diagnostics."""
    try:
        state = _build_refresh_state(coindata, payload)
    except Exception as exc:
        warning = f"Post-refresh state reconstruction failed: {exc}"
        _log(SERVICE, f"{warning} ({coindata.exchange})", level="ERROR")
        return _RefreshJobOutcome(
            "partial" if outcome.status == "completed" else outcome.status,
            f"{outcome.message}; {warning}",
            None,
            outcome.exchange_results,
            [*outcome.warnings, warning],
        )
    return _RefreshJobOutcome(
        outcome.status,
        outcome.message,
        state,
        outcome.exchange_results,
        outcome.warnings,
    )


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "coin_data.html"
    html = html_path.read_text(encoding="utf-8")

    html = render_page_urls(request, html, "/api/coin-data")

    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', script_json(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', script_json(PBGUI_SERIAL))
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
    quotes: list[str] | None = Query(default=None),
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
        quotes=quotes,
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
    _validate_nonempty_quote_selection(payload.quotes)
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    total_steps = 6

    def _runner(job_id: str, _: int) -> _RefreshJobOutcome:
        result = _refresh_single_exchange(
            coindata,
            coindata.exchange,
            progress_cb=_make_refresh_progress_cb(job_id),
            step_offset=0,
            total_steps=total_steps,
        )
        _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
        if result.get("ok"):
            outcome = _RefreshJobOutcome(
                "completed", f"Refreshed {coindata.exchange}", None, [result], []
            )
        else:
            warning = _exchange_result_warning(result)
            status = "partial" if result.get("partial") else "error"
            _log(SERVICE, f"Refresh selected exchange {status}: {warning}", level="WARNING" if status == "partial" else "ERROR")
            outcome = _RefreshJobOutcome(status, warning, None, [result], [warning])
        return _attach_optional_refresh_state(outcome, coindata, payload)

    job_id = _start_refresh_job(
        f"Refreshing {coindata.exchange}...",
        f"{coindata.exchange}: fetching markets...",
        total_steps,
        _runner,
        coalesce_key=_refresh_job_key("exchange", coindata, payload),
    )
    return {"ok": True, "job_id": job_id}


@router.post("/refresh/all")
def refresh_all(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    _validate_nonempty_quote_selection(payload.quotes)
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

    def _runner(job_id: str, _: int) -> _RefreshJobOutcome:
        exchange_results = []
        for index, exchange_id in enumerate(exchanges):
            exchange_results.append(
                _refresh_single_exchange(
                    coindata,
                    exchange_id,
                    progress_cb=_make_refresh_progress_cb(job_id),
                    step_offset=index * 5,
                    total_steps=total_steps,
                )
        )
        _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
        outcome = _batch_refresh_outcome(exchange_results, None, "All exchanges refreshed")
        outcome = _attach_optional_refresh_state(outcome, coindata, payload)
        if outcome.status != "completed":
            _log(SERVICE, f"Refresh all exchanges {outcome.status}: {outcome.message}", level="ERROR")
        return outcome

    first_exchange = exchanges[0] if exchanges else "exchange"
    job_id = _start_refresh_job(
        "Refreshing all exchanges...",
        f"{first_exchange}: fetching markets...",
        total_steps,
        _runner,
        coalesce_key=_refresh_job_key("all", coindata, payload),
    )
    return {"ok": True, "job_id": job_id}


@router.post("/refresh/cmc")
def refresh_cmc(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    _validate_nonempty_quote_selection(payload.quotes)
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    _require_cmc_pool_ready(coindata)
    total_steps = 12

    def _runner(job_id: str, _: int) -> _RefreshJobOutcome:
        try:
            _refresh_cmc_data(coindata, job_id, total_steps)
        except Exception as exc:
            _log(SERVICE, f"Refresh CoinMarketCap data failed for {coindata.exchange}: {exc}", level="ERROR")
            outcome = _RefreshJobOutcome(
                "error", f"Failed to refresh CoinMarketCap data: {exc}", None, [], [str(exc)]
            )
            return _attach_optional_refresh_state(outcome, coindata, payload)

        result = _refresh_single_exchange(
            coindata,
            coindata.exchange,
            progress_cb=_make_refresh_progress_cb(job_id),
            step_offset=6,
            total_steps=total_steps,
        )
        _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
        if result.get("ok"):
            outcome = _RefreshJobOutcome(
                "completed", "CoinMarketCap data refreshed", None, [result], []
            )
        else:
            warning = _exchange_result_warning(result)
            status = "partial" if result.get("partial") else "error"
            outcome = _RefreshJobOutcome(status, warning, None, [result], [warning])
        return _attach_optional_refresh_state(outcome, coindata, payload)

    job_id = _start_refresh_job(
        "Refreshing CMC + selected exchange...",
        "Fetching CoinMarketCap listings...",
        total_steps,
        _runner,
        coalesce_key=_refresh_job_key("cmc", coindata, payload),
    )
    return {"ok": True, "job_id": job_id}


@router.post("/refresh/cmc_all")
def refresh_cmc_all(
    payload: CoinDataRefreshRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    del session
    _validate_nonempty_quote_selection(payload.quotes)
    coindata = _new_coindata(
        exchange=payload.exchange,
        market_cap=payload.market_cap,
        vol_mcap=payload.vol_mcap,
        tags=payload.tags,
        only_cpt=payload.only_cpt,
        hide_notices=payload.hide_notices,
    )
    _require_cmc_pool_ready(coindata)
    exchanges = V7.list()
    total_steps = 6 + (len(exchanges) * 5) + 1

    def _runner(job_id: str, _: int) -> _RefreshJobOutcome:
        try:
            _refresh_cmc_data(coindata, job_id, total_steps)
        except Exception as exc:
            _log(SERVICE, f"Refresh CoinMarketCap data and all exchanges failed: {exc}", level="ERROR")
            outcome = _RefreshJobOutcome(
                "error",
                f"Failed to refresh CoinMarketCap data and all exchanges: {exc}",
                None,
                [],
                [str(exc)],
            )
            return _attach_optional_refresh_state(outcome, coindata, payload)

        exchange_results = []
        for index, exchange_id in enumerate(exchanges):
            exchange_results.append(
                _refresh_single_exchange(
                    coindata,
                    exchange_id,
                    progress_cb=_make_refresh_progress_cb(job_id),
                    step_offset=6 + (index * 5),
                    total_steps=total_steps,
                )
            )
        _set_refresh_job_progress(job_id, total_steps - 1, total_steps, "Refreshing page state...")
        outcome = _batch_refresh_outcome(
            exchange_results,
            None,
            "CoinMarketCap data and all exchanges refreshed",
        )
        outcome = _attach_optional_refresh_state(outcome, coindata, payload)
        if outcome.status != "completed":
            _log(SERVICE, f"CMC-all refresh {outcome.status}: {outcome.message}", level="ERROR")
        return outcome

    first_exchange = exchanges[0] if exchanges else "exchange"
    job_id = _start_refresh_job(
        "Refreshing CMC + all exchanges...",
        f"Fetching CoinMarketCap listings before {first_exchange}...",
        total_steps,
        _runner,
        coalesce_key=_refresh_job_key("cmc_all", coindata, payload),
    )
    return {"ok": True, "job_id": job_id}
