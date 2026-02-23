from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Callable
from contextlib import contextmanager
import configparser
import hashlib
import json
import os
import time
import numpy as np
import requests

if os.name == "posix":
    import fcntl
else:
    fcntl = None

from hyperliquid_api import (
    download_hyperliquid_candles_api,
    fetch_candle_snapshot,
    normalize_hyperliquid_coin,
    resolve_hyperliquid_coin_name,
)
from hyperliquid_l2book_candles import generate_1m_candles_from_l2book_range, iter_hyperliquid_l2book_mid_prices
from market_data import (
    append_exchange_download_log,
    get_exchange_raw_root_dir,
    get_minute_presence_for_dataset,
    normalize_market_data_coin_dir,
)
from market_data_sources import (
    SOURCE_CODE_API,
    SOURCE_CODE_L2BOOK,
    SOURCE_CODE_OTHER,
    get_oldest_day_with_source_code,
    get_source_codes_for_day,
    update_source_index_for_day,
)
from Exchange import Exchange

# Enable detailed timing logs for performance analysis
# Set environment variable PBGUI_TIMING_LOGS=1 to enable, or change this constant
ENABLE_TIMING_LOGS = os.getenv("PBGUI_TIMING_LOGS", "0") == "1"

_ET_TZ = ZoneInfo("America/New_York")
TRADFI_IMPROVE_DEFAULT_LOOKBACK_DAYS = 365 * 5
TRADFI_DISCOVERY_MAX_LOOKBACK_DAYS = 365 * 30
TRADFI_FX_EMPTY_CHUNKS_STOP = 8
TRADFI_EMPTY_PERIODS_STOP = 2
TRADFI_ERROR_LOG_THROTTLE_SECONDS = 300
TIINGO_RETRY_ATTEMPTS = 4
TIINGO_RETRY_BACKOFF_SECONDS = 1.5
# Earliest date Tiingo IEX 1m data is available (IEX exchange launch date)
_IEX_FLOOR_DATE = date(2016, 12, 12)
TIINGO_MAX_REQ_PER_HOUR = 50
TIINGO_MAX_REQ_PER_DAY = 1000
TIINGO_MAX_BANDWIDTH_PER_MONTH_BYTES = 2 * 1024 * 1024 * 1024
TIINGO_LIMIT_WAIT_CHUNK_SECONDS = 60
_TRADFI_ERROR_LAST_TS: dict[str, float] = {}
_TIINGO_USAGE_STATE: dict[str, dict[str, Any]] = {}
_TIINGO_USAGE_LOADED = False
_TIINGO_MONTH_BAR_CACHE: dict[tuple[str, str, str], tuple[str, dict[str, list[dict[str, Any]]]]] = {}
_TIINGO_DAY_BAR_CACHE: dict[tuple[str, str, str], tuple[str, dict[str, list[dict[str, Any]]]]] = {}
_TRADFI_SYMBOL_MAP_CACHE: dict[str, Any] = {}  # {"records": list, "sig": (mtime_ns, size)}


def _tiingo_usage_state_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "logs" / "tiingo_usage_state.json"


@contextmanager
def _tiingo_usage_file_lock():
    lock_path = _tiingo_usage_state_path().with_suffix(".json.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as lock_fh:
        if fcntl is not None:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


def _tiingo_usage_key_id(api_key: str) -> str:
    token = str(api_key or "").strip().encode("utf-8")
    return hashlib.sha256(token).hexdigest()[:24]


def _tiingo_default_state() -> dict[str, Any]:
    return {
        "hour_key": "",
        "day_key": "",
        "month_key": "",
        "hour_requests": 0,
        "day_requests": 0,
        "month_bytes": 0,
    }


def _tiingo_load_usage_state_once() -> None:
    global _TIINGO_USAGE_LOADED
    if _TIINGO_USAGE_LOADED:
        return
    _TIINGO_USAGE_LOADED = True
    path = _tiingo_usage_state_path()
    if not path.exists():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    entries = raw.get("entries") if isinstance(raw, dict) else None
    if not isinstance(entries, dict):
        return
    for key_id, rec in entries.items():
        if not isinstance(key_id, str) or not isinstance(rec, dict):
            continue
        _TIINGO_USAGE_STATE[key_id] = {
            "hour_key": str(rec.get("hour_key") or ""),
            "day_key": str(rec.get("day_key") or ""),
            "month_key": str(rec.get("month_key") or ""),
            "hour_requests": max(0, int(rec.get("hour_requests") or 0)),
            "day_requests": max(0, int(rec.get("day_requests") or 0)),
            "month_bytes": max(0, int(rec.get("month_bytes") or 0)),
        }


def _tiingo_persist_usage_state() -> None:
    path = _tiingo_usage_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable_entries: dict[str, dict[str, Any]] = {}
    for key_id, state in _TIINGO_USAGE_STATE.items():
        if not isinstance(key_id, str) or not isinstance(state, dict):
            continue
        serializable_entries[key_id] = {
            "hour_key": str(state.get("hour_key") or ""),
            "day_key": str(state.get("day_key") or ""),
            "month_key": str(state.get("month_key") or ""),
            "hour_requests": max(0, int(state.get("hour_requests") or 0)),
            "day_requests": max(0, int(state.get("day_requests") or 0)),
            "month_bytes": max(0, int(state.get("month_bytes") or 0)),
        }
    payload = {
        "version": 1,
        "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "entries": serializable_entries,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _log_tradfi_error_throttled(*, provider: str, ticker: str, err: Exception, status: int | None = None) -> None:
    key = f"{str(provider).lower()}:{str(ticker).upper()}:{int(status) if status is not None else 'na'}:{type(err).__name__}"
    now = float(time.time())
    last = float(_TRADFI_ERROR_LAST_TS.get(key) or 0.0)
    if now - last < float(TRADFI_ERROR_LOG_THROTTLE_SECONDS):
        return
    _TRADFI_ERROR_LAST_TS[key] = now
    append_exchange_download_log(
        "hyperliquid",
        f"[hl_tradfi] fetch_error provider={provider} ticker={ticker} status={status} err={type(err).__name__}: {err}",
        level="WARNING",
    )


def _log_tradfi_warning_throttled(key: str, msg: str) -> None:
    """Throttled warning log for resolver/mapping issues (one per TRADFI_ERROR_LOG_THROTTLE_SECONDS)."""
    now = float(time.time())
    last = float(_TRADFI_ERROR_LAST_TS.get(key) or 0.0)
    if now - last < float(TRADFI_ERROR_LOG_THROTTLE_SECONDS):
        return
    _TRADFI_ERROR_LAST_TS[key] = now
    append_exchange_download_log("hyperliquid", msg, level="WARNING")


def _load_tradfi_symbol_map_cached() -> list[dict]:
    """Load data/coindata/hyperliquid/tradfi_symbol_map.json with module-level mtime cache."""
    path = Path.cwd() / "data" / "coindata" / "hyperliquid" / "tradfi_symbol_map.json"
    if not path.exists():
        return []
    try:
        stat = path.stat()
        sig = (stat.st_mtime_ns, stat.st_size)
        if _TRADFI_SYMBOL_MAP_CACHE.get("sig") == sig and "records" in _TRADFI_SYMBOL_MAP_CACHE:
            return _TRADFI_SYMBOL_MAP_CACHE["records"]
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            _TRADFI_SYMBOL_MAP_CACHE["sig"] = sig
            _TRADFI_SYMBOL_MAP_CACHE["records"] = data
            return data
    except Exception:
        pass
    return []


def resolve_tradfi_symbol(xyz_coin: str) -> tuple[str | None, str | None, bool, date | None]:
    """Resolve an XYZ coin name to Tiingo ticker info via tradfi_symbol_map.json.

    Returns (tiingo_ticker, tiingo_fx_ticker, tiingo_fx_invert, tiingo_start_date).
    Returns all-None/False when coin has no provider or map entry is missing.

    Tier 1: map entry with tiingo_ticker or tiingo_fx_ticker → use it
    Tier 2: map entry with status=no_provider/pending/delisted → None (silent)
    Tier 3: no map entry → None + throttled WARNING in log
    """
    key = str(xyz_coin or "").strip().upper()
    if not key:
        return None, None, False, None

    records = _load_tradfi_symbol_map_cached()
    entry = next((r for r in records if str(r.get("xyz_coin") or "").upper() == key), None)

    if entry is None:
        _log_tradfi_warning_throttled(
            f"resolver_missing:{key}",
            f"[hl_best_1m] {key} not in tradfi_symbol_map.json — TradFi fetch skipped (add mapping entry)",
        )
        return None, None, False, None

    status = str(entry.get("status") or "").lower()
    if status in ("no_provider", "pending", "delisted"):
        return None, None, False, None

    tiingo_ticker: str | None = str(entry.get("tiingo_ticker") or "").strip() or None
    tiingo_fx_ticker: str | None = str(entry.get("tiingo_fx_ticker") or "").strip() or None
    tiingo_fx_invert: bool = bool(entry.get("tiingo_fx_invert") or False)
    tiingo_start_date: date | None = None
    sd = str(entry.get("tiingo_start_date") or "").strip()
    if sd:
        try:
            tiingo_start_date = date.fromisoformat(sd)
        except ValueError:
            pass

    return tiingo_ticker, tiingo_fx_ticker, tiingo_fx_invert, tiingo_start_date


def _load_tradfi_profiles_from_ini() -> dict[str, dict[str, str]]:
    cfg = configparser.ConfigParser()
    ini_path = Path(__file__).resolve().parent / "pbgui.ini"
    cfg.read(ini_path)
    sec = "tradfi_profiles"
    out: dict[str, dict[str, str]] = {
        "tiingo": {"api_key": "", "api_secret": "", "enabled": "0"},
    }
    if not cfg.has_section(sec):
        if cfg.has_section("market_data"):
            out["tiingo"]["enabled"] = str(cfg.get("market_data", "tiingo_enabled", fallback="0") or "").strip()
        return out
    out["tiingo"]["api_key"] = str(cfg.get(sec, "tiingo_api_key", fallback="") or "").strip()
    out["tiingo"]["enabled"] = str(cfg.get(sec, "tiingo_enabled", fallback="") or "").strip()
    if not out["tiingo"]["enabled"] and cfg.has_section("market_data"):
        out["tiingo"]["enabled"] = str(cfg.get("market_data", "tiingo_enabled", fallback="0") or "").strip()
    return out


def _is_stock_perp_coin(coin: str) -> bool:
    s = str(coin or "")
    if not s:
        return False
    base = s.split("/")[0]
    u = base.upper()
    if u.startswith("XYZ:") or u.startswith("XYZ-"):
        return True
    # Some call-sites pass bare XYZ coin names without prefix (e.g. "EUR").
    # If the coin exists in tradfi_symbol_map, treat it as stock-perp.
    key = _tradfi_ticker_from_hyperliquid_coin(u)
    if not key:
        return False
    records = _load_tradfi_symbol_map_cached()
    return any(str(r.get("xyz_coin") or "").strip().upper() == key for r in records)


def _tradfi_ticker_from_hyperliquid_coin(coin: str) -> str:
    s = str(coin or "").strip()
    if not s:
        return ""
    base = s.split("/")[0].strip()
    if not base:
        return ""
    if base.lower().startswith("xyz:"):
        return base[4:].strip().upper()
    if base.upper().startswith("XYZ-"):
        return base[4:].strip().upper()
    return base.strip().upper()


def _minute_index(ts_ms: int, day_start_ms: int) -> int:
    return int((int(ts_ms) - int(day_start_ms)) // 60_000)


def _retry_after_or_backoff_seconds(*, resp: Any | None, attempt: int, base_backoff_s: float) -> float:
    retry_after = 0.0
    try:
        if resp is not None:
            retry_after = float(str(getattr(resp, "headers", {}).get("Retry-After") or "0").strip() or 0)
    except Exception:
        retry_after = 0.0
    if retry_after > 0.0:
        return float(retry_after)
    return float(base_backoff_s) * float(2 ** max(0, int(attempt)))


def _tiingo_state_for_key_unlocked(api_key: str) -> dict[str, Any]:
    key = str(api_key or "").strip()
    if not key:
        return _tiingo_default_state()

    _tiingo_load_usage_state_once()
    key_id = _tiingo_usage_key_id(key)
    state = _TIINGO_USAGE_STATE.get(key_id)
    if not isinstance(state, dict):
        state = _tiingo_default_state()
        _TIINGO_USAGE_STATE[key_id] = state
        _tiingo_persist_usage_state()

    now = datetime.now(timezone.utc)
    hour_key = now.strftime("%Y%m%d%H")
    day_key = now.strftime("%Y%m%d")
    month_key = now.strftime("%Y%m")

    if str(state.get("hour_key") or "") != hour_key:
        state["hour_key"] = hour_key
        state["hour_requests"] = 0
        _tiingo_persist_usage_state()
    if str(state.get("day_key") or "") != day_key:
        state["day_key"] = day_key
        state["day_requests"] = 0
        _tiingo_persist_usage_state()
    if str(state.get("month_key") or "") != month_key:
        state["month_key"] = month_key
        state["month_bytes"] = 0
        _tiingo_persist_usage_state()
    return state


def _tiingo_state_for_key(api_key: str) -> dict[str, Any]:
    with _tiingo_usage_file_lock():
        return _tiingo_state_for_key_unlocked(api_key)


def get_tiingo_runtime_usage(*, api_key: str) -> dict[str, int]:
    state = _tiingo_state_for_key(api_key)
    hour_requests = int(state.get("hour_requests") or 0)
    day_requests = int(state.get("day_requests") or 0)
    month_bytes = int(state.get("month_bytes") or 0)
    return {
        "hour_requests": hour_requests,
        "hour_remaining": max(0, int(TIINGO_MAX_REQ_PER_HOUR) - hour_requests),
        "day_requests": day_requests,
        "day_remaining": max(0, int(TIINGO_MAX_REQ_PER_DAY) - day_requests),
        "month_bytes": month_bytes,
        "month_bytes_remaining": max(0, int(TIINGO_MAX_BANDWIDTH_PER_MONTH_BYTES) - month_bytes),
        "hour_limit": int(TIINGO_MAX_REQ_PER_HOUR),
        "day_limit": int(TIINGO_MAX_REQ_PER_DAY),
        "month_bytes_limit": int(TIINGO_MAX_BANDWIDTH_PER_MONTH_BYTES),
    }


def _tiingo_register_call(*, api_key: str, response_bytes: int) -> None:
    with _tiingo_usage_file_lock():
        state = _tiingo_state_for_key_unlocked(api_key)
        state["hour_requests"] = int(state.get("hour_requests") or 0) + 1
        state["day_requests"] = int(state.get("day_requests") or 0) + 1
        state["month_bytes"] = int(state.get("month_bytes") or 0) + max(0, int(response_bytes))
        _tiingo_persist_usage_state()


def _tiingo_limit_allows_call(*, api_key: str) -> bool:
    usage = get_tiingo_runtime_usage(api_key=api_key)
    return (
        int(usage.get("hour_remaining") or 0) > 0
        and int(usage.get("day_remaining") or 0) > 0
        and int(usage.get("month_bytes_remaining") or 0) > 0
    )


def _seconds_until_next_hour_utc(now: datetime) -> int:
    nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return max(1, int((nxt - now).total_seconds()) + 1)


def _seconds_until_next_day_utc(now: datetime) -> int:
    nxt = (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
    return max(1, int((nxt - now).total_seconds()) + 1)


def _seconds_until_next_month_utc(now: datetime) -> int:
    if int(now.month) == 12:
        nxt = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        nxt = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((nxt - now).total_seconds()) + 1)


def _tiingo_wait_until_quota_allows_call(
    *,
    api_key: str,
    ticker: str = "",
    status_cb: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    while True:
        usage = get_tiingo_runtime_usage(api_key=api_key)
        hour_remaining = int(usage.get("hour_remaining") or 0)
        day_remaining = int(usage.get("day_remaining") or 0)
        month_remaining = int(usage.get("month_bytes_remaining") or 0)
        if hour_remaining > 0 and day_remaining > 0 and month_remaining > 0:
            return

        now = datetime.now(timezone.utc)
        waits: list[int] = []
        reasons: list[str] = []
        if hour_remaining <= 0:
            waits.append(_seconds_until_next_hour_utc(now))
            reasons.append("hour")
        if day_remaining <= 0:
            waits.append(_seconds_until_next_day_utc(now))
            reasons.append("day")
        if month_remaining <= 0:
            waits.append(_seconds_until_next_month_utc(now))
            reasons.append("month")

        wait_s = max(1, min(waits) if waits else 1)
        reason_txt = ",".join(reasons)

        def _emit_wait_status(remaining_s: int) -> None:
            if status_cb is None:
                return
            try:
                status_cb(
                    {
                        "stage": "tiingo_wait",
                        "ticker": str(ticker or "").upper(),
                        "tiingo_wait_s": int(max(0, remaining_s)),
                        "tiingo_wait_reason": reason_txt,
                        "tiingo_wait_kind": "quota",
                    }
                )
            except Exception:
                pass

        _emit_wait_status(wait_s)
        append_exchange_download_log(
            "hyperliquid",
            f"[hl_tradfi] tiingo_wait_for_quota reasons={reason_txt} wait_s={wait_s}",
            level="WARNING",
        )

        remaining = int(wait_s)
        while remaining > 0:
            sleep_chunk = int(min(remaining, 1))
            time.sleep(float(sleep_chunk))
            remaining -= sleep_chunk
            if remaining > 0:
                _emit_wait_status(remaining)


def _tiingo_fetch_1m_iex(
    *,
    ticker: str,
    start_ms: int,
    end_ms: int,
    api_key: str,
    timeout_s: float,
    allow_rate_limit_hour_retry: bool = True,
    status_cb: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    token = str(api_key or "").strip()
    if not token:
        return []
    _tiingo_wait_until_quota_allows_call(api_key=token, ticker=ticker, status_cb=status_cb)

    url = f"https://api.tiingo.com/iex/{ticker}/prices"
    params: dict[str, Any] = {
        "startDate": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        "endDate": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        "resampleFreq": "1min",
        "columns": "open,high,low,close,volume",
        "token": token,
    }

    attempts = int(max(1, TIINGO_RETRY_ATTEMPTS))
    payload: Any = None
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, timeout=float(timeout_s))
            status = int(resp.status_code)
            if status in (401, 403, 422):
                return []
            if status in (429, 500, 502, 503, 504):
                if attempt < attempts - 1:
                    time.sleep(_retry_after_or_backoff_seconds(resp=resp, attempt=attempt, base_backoff_s=float(TIINGO_RETRY_BACKOFF_SECONDS)))
                    continue
            resp.raise_for_status()
            payload = resp.json()
            content_len = 0
            try:
                content_len = int(resp.headers.get("Content-Length") or 0)
            except Exception:
                content_len = 0
            if content_len <= 0:
                try:
                    content_len = len(resp.content or b"")
                except Exception:
                    content_len = 0
            _tiingo_register_call(api_key=token, response_bytes=int(content_len))
            last_err = None
            break
        except Exception as e:
            last_err = e
            status = None
            try:
                status = int(getattr(getattr(e, "response", None), "status_code", None))
            except Exception:
                status = None
            is_transient = (
                status in (429, 500, 502, 503, 504)
                or isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            )
            if is_transient and attempt < attempts - 1:
                time.sleep(_retry_after_or_backoff_seconds(resp=getattr(e, "response", None), attempt=attempt, base_backoff_s=float(TIINGO_RETRY_BACKOFF_SECONDS)))
                continue
            break

    if last_err is not None:
        status = None
        try:
            status = int(getattr(getattr(last_err, "response", None), "status_code", None))
        except Exception:
            status = None
        if status == 429 and allow_rate_limit_hour_retry:
            wait_s = _seconds_until_next_hour_utc(datetime.now(timezone.utc))
            if status_cb is not None:
                try:
                    status_cb(
                        {
                            "stage": "tiingo_wait",
                            "ticker": str(ticker or "").upper(),
                            "tiingo_wait_s": int(wait_s),
                            "tiingo_wait_reason": "server_429",
                            "tiingo_wait_kind": "server_429",
                        }
                    )
                except Exception:
                    pass
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_tradfi] tiingo_server_429_wait ticker={ticker} wait_s={wait_s}",
                level="WARNING",
            )
            time.sleep(float(wait_s))
            return _tiingo_fetch_1m_iex(
                ticker=ticker,
                start_ms=int(start_ms),
                end_ms=int(end_ms),
                api_key=token,
                timeout_s=float(timeout_s),
                allow_rate_limit_hour_retry=False,
                status_cb=status_cb,
            )
        _log_tradfi_error_throttled(provider="tiingo", ticker=ticker, err=last_err, status=status)
        return []

    if not isinstance(payload, list):
        return []

    out: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        try:
            ts_raw = str(row.get("date") or "").replace("Z", "+00:00")
            ts_ms = int(datetime.fromisoformat(ts_raw).astimezone(timezone.utc).timestamp() * 1000)
            if ts_ms < int(start_ms) or ts_ms > int(end_ms):
                continue
            out.append(
                {
                    "t": ts_ms,
                    "o": float(row.get("open")),
                    "h": float(row.get("high")),
                    "l": float(row.get("low")),
                    "c": float(row.get("close")),
                    "v": float(row.get("volume")) if row.get("volume") is not None else 0.0,
                }
            )
        except Exception:
            continue
    return out


def _tiingo_fetch_1m_fx(
    *,
    ticker: str,
    start_ms: int,
    end_ms: int,
    api_key: str,
    timeout_s: float,
    allow_rate_limit_hour_retry: bool = True,
    status_cb: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    token = str(api_key or "").strip()
    if not token:
        return []
    t_u = str(ticker or "").strip().lower()
    if not t_u:
        return []

    _tiingo_wait_until_quota_allows_call(api_key=token, ticker=t_u, status_cb=status_cb)

    url = f"https://api.tiingo.com/tiingo/fx/{t_u}/prices"
    params: dict[str, Any] = {
        "startDate": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        "endDate": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        "resampleFreq": "1min",
        "token": token,
    }

    attempts = int(max(1, TIINGO_RETRY_ATTEMPTS))
    payload: Any = None
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, timeout=float(timeout_s))
            status = int(resp.status_code)
            if status in (401, 403, 422):
                return []
            if status in (429, 500, 502, 503, 504):
                if attempt < attempts - 1:
                    time.sleep(_retry_after_or_backoff_seconds(resp=resp, attempt=attempt, base_backoff_s=float(TIINGO_RETRY_BACKOFF_SECONDS)))
                    continue
            resp.raise_for_status()
            payload = resp.json()
            content_len = 0
            try:
                content_len = int(resp.headers.get("Content-Length") or 0)
            except Exception:
                content_len = 0
            if content_len <= 0:
                try:
                    content_len = len(resp.content or b"")
                except Exception:
                    content_len = 0
            _tiingo_register_call(api_key=token, response_bytes=int(content_len))
            last_err = None
            break
        except Exception as e:
            last_err = e
            status = None
            try:
                status = int(getattr(getattr(e, "response", None), "status_code", None))
            except Exception:
                status = None
            is_transient = (
                status in (429, 500, 502, 503, 504)
                or isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            )
            if is_transient and attempt < attempts - 1:
                time.sleep(_retry_after_or_backoff_seconds(resp=getattr(e, "response", None), attempt=attempt, base_backoff_s=float(TIINGO_RETRY_BACKOFF_SECONDS)))
                continue
            break

    if last_err is not None:
        status = None
        try:
            status = int(getattr(getattr(last_err, "response", None), "status_code", None))
        except Exception:
            status = None
        if status == 429 and allow_rate_limit_hour_retry:
            wait_s = _seconds_until_next_hour_utc(datetime.now(timezone.utc))
            if status_cb is not None:
                try:
                    status_cb(
                        {
                            "stage": "tiingo_wait",
                            "ticker": str(t_u or "").upper(),
                            "tiingo_wait_s": int(wait_s),
                            "tiingo_wait_reason": "server_429",
                            "tiingo_wait_kind": "server_429",
                        }
                    )
                except Exception:
                    pass
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_tradfi] tiingo_fx_server_429_wait ticker={t_u} wait_s={wait_s}",
                level="WARNING",
            )
            time.sleep(float(wait_s))
            return _tiingo_fetch_1m_fx(
                ticker=t_u,
                start_ms=int(start_ms),
                end_ms=int(end_ms),
                api_key=token,
                timeout_s=float(timeout_s),
                allow_rate_limit_hour_retry=False,
                status_cb=status_cb,
            )
        _log_tradfi_error_throttled(provider="tiingo_fx", ticker=t_u, err=last_err, status=status)
        return []

    if not isinstance(payload, list):
        return []

    out: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        try:
            ts_raw = str(row.get("date") or "").replace("Z", "+00:00")
            ts_ms = int(datetime.fromisoformat(ts_raw).astimezone(timezone.utc).timestamp() * 1000)
            if ts_ms < int(start_ms) or ts_ms > int(end_ms):
                continue
            out.append(
                {
                    "t": ts_ms,
                    "o": float(row.get("open")),
                    "h": float(row.get("high")),
                    "l": float(row.get("low")),
                    "c": float(row.get("close")),
                    "v": float(row.get("volume")) if row.get("volume") is not None else 0.0,
                }
            )
        except Exception:
            continue
    return out


def _invert_ohlc_bar(bar: dict[str, Any]) -> dict[str, Any] | None:
    try:
        o = float(bar.get("o"))
        h = float(bar.get("h"))
        l = float(bar.get("l"))
        c = float(bar.get("c"))
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            return None
        return {
            "t": int(bar.get("t")),
            "o": float(1.0 / o),
            "h": float(1.0 / l),
            "l": float(1.0 / h),
            "c": float(1.0 / c),
            "v": float(bar.get("v", 0.0) or 0.0),
        }
    except Exception:
        return None


def _month_start_end_dates_utc(day: date) -> tuple[date, date]:
    month_start = date(day.year, day.month, 1)
    if day.month == 12:
        month_end = date(day.year, 12, 31)
    else:
        next_month_start = date(day.year + (1 if day.month == 12 else 0), 1 if day.month == 12 else day.month + 1, 1)
        month_end = next_month_start - timedelta(days=1)
    return month_start, month_end


def _tiingo_fetch_1m_iex_day_from_month_cache(
    *,
    ticker: str,
    day: date,
    session_start_ms: int,
    session_end_ms: int,
    api_key: str,
    timeout_s: float,
    status_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], bool, bool]:
    token = str(api_key or "").strip()
    if not token:
        return ([], False, False)

    cache_key = (token, "iex", str(ticker or "").upper())
    month_key = day.strftime("%Y%m")
    cached = _TIINGO_MONTH_BAR_CACHE.get(cache_key)

    day_map: dict[str, list[dict[str, Any]]]
    did_fetch_month = False
    if cached and str(cached[0]) == month_key and isinstance(cached[1], dict):
        day_map = cached[1]
    else:
        did_fetch_month = True
        month_start, month_end = _month_start_end_dates_utc(day)
        month_start_ms = int(datetime(month_start.year, month_start.month, month_start.day, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        month_end_ms = int(datetime(month_end.year, month_end.month, month_end.day, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)
        month_bars = _tiingo_fetch_1m_iex(
            ticker=ticker,
            start_ms=month_start_ms,
            end_ms=month_end_ms,
            api_key=token,
            timeout_s=float(timeout_s),
            status_cb=status_cb,
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for bar in month_bars:
            if not isinstance(bar, dict):
                continue
            try:
                ts_ms = int(bar.get("t"))
                ds = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y%m%d")
            except Exception:
                continue
            grouped.setdefault(ds, []).append(bar)
        _TIINGO_MONTH_BAR_CACHE[cache_key] = (month_key, grouped)
        day_map = grouped

    month_has_any_bars = any(bool(v) for v in day_map.values())
    bars = day_map.get(day.strftime("%Y%m%d")) or []
    if not bars:
        return ([], did_fetch_month, month_has_any_bars)
    filtered = [
        bar
        for bar in bars
        if isinstance(bar, dict)
        and int(bar.get("t", 0)) >= int(session_start_ms)
        and int(bar.get("t", 0)) <= int(session_end_ms)
    ]
    return (filtered, did_fetch_month, month_has_any_bars)


def _tiingo_fetch_1m_fx_day_from_month_cache(
    *,
    ticker: str,
    day: date,
    session_start_ms: int,
    session_end_ms: int,
    api_key: str,
    timeout_s: float,
    invert: bool = False,
    status_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], bool, bool]:
    token = str(api_key or "").strip()
    if not token:
        return ([], False, False)

    t_u = str(ticker or "").strip().lower()
    if not t_u:
        return ([], False, False)

    cache_key = (token, "fx", t_u)
    # 7-day chunk aligned to Monday..Sunday to reduce request count while staying
    # below Tiingo's datapoint limits.
    chunk_start = day - timedelta(days=int(day.weekday()))
    chunk_end = chunk_start + timedelta(days=6)
    chunk_key = f"{chunk_start.strftime('%Y%m%d')}-{chunk_end.strftime('%Y%m%d')}"
    cached = _TIINGO_DAY_BAR_CACHE.get(cache_key)

    did_fetch_day = False
    if cached and str(cached[0]) == chunk_key and isinstance(cached[1], dict):
        day_map = cached[1]
    else:
        did_fetch_day = True
        start_ms = int(datetime(chunk_start.year, chunk_start.month, chunk_start.day, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime(chunk_end.year, chunk_end.month, chunk_end.day, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)
        chunk_bars = _tiingo_fetch_1m_fx(
            ticker=t_u,
            start_ms=start_ms,
            end_ms=end_ms,
            api_key=token,
            timeout_s=float(timeout_s),
            status_cb=status_cb,
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for bar in chunk_bars:
            if not isinstance(bar, dict):
                continue
            try:
                ts_ms = int(bar.get("t"))
                ds = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y%m%d")
            except Exception:
                continue
            grouped.setdefault(ds, []).append(bar)
        _TIINGO_DAY_BAR_CACHE[cache_key] = (chunk_key, grouped)
        day_map = grouped

    chunk_has_any_bars = any(bool(v) for v in day_map.values())
    bars = day_map.get(day.strftime("%Y%m%d")) or []

    if not bars:
        return ([], did_fetch_day, chunk_has_any_bars)

    filtered = [
        bar
        for bar in bars
        if isinstance(bar, dict)
        and int(bar.get("t", 0)) >= int(session_start_ms)
        and int(bar.get("t", 0)) <= int(session_end_ms)
    ]
    if not invert:
        return (filtered, did_fetch_day, chunk_has_any_bars)

    inverted: list[dict[str, Any]] = []
    for bar in filtered:
        b = _invert_ohlc_bar(bar)
        if b is not None:
            inverted.append(b)
    return (inverted, did_fetch_day, chunk_has_any_bars)


def probe_tiingo_iex_1m(*, api_key: str, ticker: str = "AAPL", timeout_s: float = 20.0) -> dict[str, Any]:
    token = str(api_key or "").strip()
    if not token:
        raise ValueError("Tiingo API key is empty.")

    url = "https://api.tiingo.com/api/test/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {token}",
    }

    attempts = int(max(1, TIINGO_RETRY_ATTEMPTS))
    payload: Any = None
    status_code: int | None = None
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, headers=headers, timeout=float(timeout_s))
            status = int(resp.status_code)
            status_code = status
            if status in (429, 500, 502, 503, 504):
                if attempt < attempts - 1:
                    time.sleep(_retry_after_or_backoff_seconds(resp=resp, attempt=attempt, base_backoff_s=float(TIINGO_RETRY_BACKOFF_SECONDS)))
                    continue
            resp.raise_for_status()
            payload = resp.json()
            last_err = None
            break
        except Exception as e:
            last_err = e
            status = None
            try:
                status = int(getattr(getattr(e, "response", None), "status_code", None))
                status_code = status
            except Exception:
                status = None
            is_transient = (
                status in (429, 500, 502, 503, 504)
                or isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            )
            if is_transient and attempt < attempts - 1:
                time.sleep(_retry_after_or_backoff_seconds(resp=getattr(e, "response", None), attempt=attempt, base_backoff_s=float(TIINGO_RETRY_BACKOFF_SECONDS)))
                continue
            break

    if last_err is not None:
        status = None
        try:
            status = int(getattr(getattr(last_err, "response", None), "status_code", None))
        except Exception:
            status = status_code
        if status in (401, 403):
            raise ValueError("Tiingo authentication failed (invalid API key).")
        raise last_err

    message = ""
    if isinstance(payload, dict):
        message = str(payload.get("message") or "").strip()

    return {
        "ok": True,
        "status": int(status_code or 200),
        "message": message or "You successfully sent a request",
    }


def _default_us_equity_session_utc(day: date) -> tuple[int, int] | None:
    # Fallback RTH session with US market holiday/early-close handling.
    if int(day.weekday()) >= 5:
        return None
    if _is_us_market_holiday(day):
        return None
    close_hour = 16
    close_minute = 0
    if _is_us_market_early_close(day):
        close_hour = 13
        close_minute = 0
    open_dt = datetime(day.year, day.month, day.day, 9, 30, tzinfo=_ET_TZ).astimezone(timezone.utc)
    close_dt = datetime(day.year, day.month, day.day, close_hour, close_minute, tzinfo=_ET_TZ).astimezone(timezone.utc)
    start_ms = int(open_dt.timestamp() * 1000)
    end_ms = int(close_dt.timestamp() * 1000) - 60_000
    if end_ms < start_ms:
        return None
    return (start_ms, end_ms)


def _is_fx_market_holiday(day: date) -> bool:
    # FX weekend closure is handled by session windows.
    return False


def _default_fx_session_utc(day: date) -> tuple[int, int] | None:
    # FX weekend boundary from observed Tiingo EUR 2025 behavior:
    # - Friday close follows 17:00 New York local time (DST-aware)
    # - Sunday reopen is effectively fixed around 22:00 UTC year-round
    cutover_dt_utc = datetime(day.year, day.month, day.day, 17, 0, tzinfo=_ET_TZ).astimezone(timezone.utc)
    cutover_minute_utc = (int(cutover_dt_utc.hour) * 60) + int(cutover_dt_utc.minute)

    # Observed reduced sessions on major FX holidays (UTC minute of day).
    # Keep this explicit so we can preserve known late reopens/early closes
    # instead of treating those dates as full-day closures.
    special_open_minute_utc: int | None = None
    special_close_minute_utc: int | None = None
    md = (int(day.month), int(day.day))
    if md == (1, 1):
        special_open_minute_utc = 23 * 60
    elif md == (12, 25):
        special_open_minute_utc = 23 * 60
    elif md in ((12, 24), (12, 31)):
        special_close_minute_utc = 22 * 60

    wd = int(day.weekday())
    if wd == 5:
        return None

    if special_open_minute_utc is not None and wd < 5:
        normal_end_minute_utc = int(cutover_minute_utc - 1) if wd == 4 else 1439
        if int(special_open_minute_utc) > int(normal_end_minute_utc):
            return None
        start_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=int(special_open_minute_utc))
        end_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=int(normal_end_minute_utc))
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        if end_ms < start_ms:
            return None
        return (start_ms, end_ms)

    if special_close_minute_utc is not None and wd < 5:
        end_minute_utc = min(1439, max(0, int(special_close_minute_utc) - 1))
        start_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc)
        end_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=int(end_minute_utc))
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        if end_ms < start_ms:
            return None
        return (start_ms, end_ms)

    if wd == 4:
        start_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc)
        if cutover_minute_utc <= 0:
            return None
        end_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=int(cutover_minute_utc - 1))
    elif wd == 6:
        start_dt = datetime(day.year, day.month, day.day, 22, 0, tzinfo=timezone.utc)
        end_dt = datetime(day.year, day.month, day.day, 23, 59, tzinfo=timezone.utc)
    else:
        start_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc)
        end_dt = start_dt + timedelta(days=1) - timedelta(minutes=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    if end_ms < start_ms:
        return None
    return (start_ms, end_ms)


def _full_day_session_utc(day: date) -> tuple[int, int]:
    start_dt = datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1) - timedelta(minutes=1)
    return (int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000))


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    shift = (int(weekday) - int(d.weekday())) % 7
    d = d + timedelta(days=shift)
    d = d + timedelta(days=(int(n) - 1) * 7)
    return d


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if int(month) == 12:
        next_month = date(int(year) + 1, 1, 1)
    else:
        next_month = date(int(year), int(month) + 1, 1)
    d = next_month - timedelta(days=1)
    while int(d.weekday()) != int(weekday):
        d = d - timedelta(days=1)
    return d


def _easter_sunday(year: int) -> date:
    # Anonymous Gregorian algorithm
    a = int(year) % 19
    b = int(year) // 100
    c = int(year) % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(int(year), int(month), int(day))


def _is_us_market_holiday(day: date) -> bool:
    y = int(day.year)
    fixed: set[date] = set()
    for m, d in ((1, 1), (6, 19), (7, 4), (12, 25)):
        if m == 6 and y < 2021:
            continue
        h = date(y, m, d)
        if h.weekday() == 5:
            h = h - timedelta(days=1)
        elif h.weekday() == 6:
            h = h + timedelta(days=1)
        fixed.add(h)

    floating: set[date] = {
        _nth_weekday_of_month(y, 1, 0, 3),
        _nth_weekday_of_month(y, 2, 0, 3),
        _last_weekday_of_month(y, 5, 0),
        _nth_weekday_of_month(y, 9, 0, 1),
        _nth_weekday_of_month(y, 11, 3, 4),
    }
    floating.add(_easter_sunday(y) - timedelta(days=2))

    return day in fixed or day in floating


def _is_us_market_early_close(day: date) -> bool:
    if int(day.weekday()) >= 5:
        return False
    if _is_us_market_holiday(day):
        return False

    thanksgiving = _nth_weekday_of_month(int(day.year), 11, 3, 4)
    if day == (thanksgiving + timedelta(days=1)):
        return True
    if int(day.month) == 7 and int(day.day) == 3:
        return True
    if int(day.month) == 12 and int(day.day) == 24:
        return True
    return False


def _determine_stock_perp_improve_start(
    *,
    coin_u: str,
    coin_dir: str,
    d_end: date,
    earliest_candidates: list[date],
    tiingo_start_date: date | None = None,
    refetch: bool = False,
    timeout_s: float = 30.0,
) -> date:
    if tiingo_start_date is not None:
        return max(_IEX_FLOOR_DATE, tiingo_start_date)
    return _IEX_FLOOR_DATE


def _parse_day_from_filename(name: str) -> str | None:
    # expected: YYYYMMDD-HH.<ext>
    s = str(name)
    if len(s) < 11:
        return None
    day = s[:8]
    if not (len(day) == 8 and day.isdigit()):
        return None
    if s[8] != "-":
        return None
    hh = s[9:11]
    if not (len(hh) == 2 and hh.isdigit()):
        return None
    return day


def _scan_day_range(folder: Path, *, suffix: str) -> tuple[date, date] | None:
    if not folder.exists() or not folder.is_dir():
        return None
    oldest: date | None = None
    newest: date | None = None

    for p in folder.glob(f"*{suffix}"):
        if not p.is_file():
            continue
        day_s = _parse_day_from_filename(p.name)
        if not day_s:
            continue
        try:
            d = datetime.strptime(day_s, "%Y%m%d").date()
        except Exception:
            continue
        oldest = d if oldest is None else min(oldest, d)
        newest = d if newest is None else max(newest, d)

    if oldest is None or newest is None:
        return None
    return (oldest, newest)


def get_local_l2book_day_range(*, coin: str) -> tuple[date, date] | None:
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        return None
    base = get_exchange_raw_root_dir("hyperliquid") / "l2Book" / coin_dir
    return _scan_day_range(base, suffix=".lz4")


def _api_farthest_lookback_start(end_date: date) -> date:
    # Based on Hyperliquid docs: candleSnapshot returns the most recent ~5000 candles.
    # For 1m candles this is ~3.5 days.
    return end_date - timedelta(days=4)


def _fmt_num(v: Any) -> str:
    try:
        s = format(float(v), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s
    except Exception:
        return str(v)


def _binance_perp_symbol(coin: str) -> str:
    return f"{coin}/USDT:USDT"


def _bybit_perp_symbol(coin: str) -> str:
    """Bybit USDT perpetual symbol format."""
    return f"{coin}/USDT:USDT"


_PERP_SYMBOL_CACHE: dict[tuple[str, str], str | None] = {}


def _canonical_perp_base(s: str) -> str:
    base = str(s or "").strip().upper()
    if not base:
        return ""
    while base and base[0].isdigit():
        base = base[1:]
    if base.startswith("K") and len(base) > 1 and base[1:].isalpha():
        base = base[1:]
    return base


def _resolve_perp_symbol(exchange_id: str, coin: str) -> str | None:
    coin_u = normalize_hyperliquid_coin(coin)
    cache_key = (str(exchange_id), str(coin_u))
    if cache_key in _PERP_SYMBOL_CACHE:
        return _PERP_SYMBOL_CACHE.get(cache_key)

    direct = f"{coin_u}/USDT:USDT"
    ex = Exchange(exchange_id)
    chosen: str | None = None
    try:
        markets = ex.load_market() or {}
    except Exception:
        markets = {}

    if isinstance(markets, dict) and direct in markets:
        _PERP_SYMBOL_CACHE[cache_key] = direct
        return direct

    canonical_coin = _canonical_perp_base(coin_u)
    exact: list[str] = []
    suffix: list[str] = []
    canonical: list[str] = []

    for market in (markets.values() if isinstance(markets, dict) else []):
        if not isinstance(market, dict):
            continue
        if not bool(market.get("swap")):
            continue
        quote = str(market.get("quote") or "").upper()
        if quote != "USDT":
            continue
        symbol = str(market.get("symbol") or "").strip()
        if not symbol:
            continue
        base = str(market.get("base") or "").upper()
        if base == coin_u:
            exact.append(symbol)
            continue
        if base.endswith(coin_u):
            suffix.append(symbol)
            continue
        if _canonical_perp_base(base) == canonical_coin:
            canonical.append(symbol)

    for bucket in (exact, suffix, canonical):
        if bucket:
            chosen = sorted(bucket, key=lambda x: (len(x), x))[0]
            break

    _PERP_SYMBOL_CACHE[cache_key] = chosen
    return chosen


def _day_start_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _day_tag(day: str) -> str:
    s = str(day or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"invalid day: {day}")


def _api_day_path(*, coin: str, day: str) -> Path:
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    base = get_exchange_raw_root_dir("hyperliquid")
    return base / "1m_api" / coin_u / f"{_day_tag(day)}.npz"


def _best_day_path(*, coin: str, day: str) -> Path:
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    base = get_exchange_raw_root_dir("hyperliquid")
    return base / "1m" / coin_u / f"{_day_tag(day)}.npz"


def _list_best_days(*, coin: str) -> list[date]:
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    base = get_exchange_raw_root_dir("hyperliquid") / "1m" / coin_u
    if not base.exists():
        return []
    out: list[date] = []
    for p in sorted(base.glob("*.npz")):
        try:
            d = datetime.strptime(p.stem, "%Y-%m-%d").date()
        except Exception:
            continue
        out.append(d)
    return out


def _list_api_days(*, coin: str) -> list[date]:
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    base = get_exchange_raw_root_dir("hyperliquid") / "1m_api" / coin_u
    if not base.exists():
        return []
    out: list[date] = []
    for p in sorted(base.glob("*.npz")):
        try:
            d = datetime.strptime(p.stem, "%Y-%m-%d").date()
        except Exception:
            continue
        out.append(d)
    return out


def _determine_best_sync_start(
    *,
    d_start: date,
    d_end: date,
    has_best_data: bool,
    full_gap_days: int,
    api_days: list[date],
) -> tuple[date, str]:
    """Pick sync window start for copying 1m_api -> 1m.

    Modes:
    - bootstrap: no local best data yet -> copy the full available API window
    - catchup: full-gap days detected -> copy the full available API window
    - incremental: fast path, only keep recent 2 days in sync
    """

    if (not has_best_data) or int(full_gap_days) > 0:
        if api_days:
            return min(api_days), ("bootstrap" if not has_best_data else "catchup")
        return d_start, ("bootstrap" if not has_best_data else "catchup")

    return max(d_start, d_end - timedelta(days=1)), "incremental"


def _l2book_minutes_for_day(
    *,
    coin: str,
    day: str,
    hours_filter: set[int] | None = None,
) -> dict[int, dict[str, Any]]:
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    base = get_exchange_raw_root_dir("hyperliquid") / "l2Book" / coin_u
    if not base.exists():
        return {}

    day_start = datetime.strptime(str(day), "%Y%m%d").replace(tzinfo=timezone.utc)
    day_start_ms = int(day_start.timestamp() * 1000)
    minute_ms = 60_000
    out: dict[int, dict[str, Any]] = {}

    for hour in range(24):
        if hours_filter is not None and hour not in hours_filter:
            continue
        in_path = base / f"{day}-{hour:02d}.lz4"
        if not in_path.exists():
            continue
        hour_start_ms = day_start_ms + (hour * 60 * minute_ms)
        hour_end_ms = hour_start_ms + (60 * minute_ms)
        bars: list[dict[str, Decimal] | None] = [None] * 60

        for ts_ms, mid in iter_hyperliquid_l2book_mid_prices(in_path):
            if ts_ms < hour_start_ms or ts_ms >= hour_end_ms:
                continue
            idx = int((ts_ms - hour_start_ms) // minute_ms)
            if idx < 0 or idx >= 60:
                continue
            b = bars[idx]
            if b is None:
                bars[idx] = {"o": mid, "h": mid, "l": mid, "c": mid}
            else:
                b["h"] = max(Decimal(b["h"]), mid)
                b["l"] = min(Decimal(b["l"]), mid)
                b["c"] = mid

        for i in range(60):
            b = bars[i]
            if b is None:
                continue
            t = hour_start_ms + i * minute_ms
            idx = int((t - day_start_ms) // minute_ms)
            if idx < 0 or idx >= 1440:
                continue
            out[idx] = {
                "t": int(t),
                "o": float(Decimal(b["o"])),
                "h": float(Decimal(b["h"])),
                "l": float(Decimal(b["l"])),
                "c": float(Decimal(b["c"])),
                "v": 0.0,
            }

    return out


def _read_day_npz(path: Path, *, day: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    if not path.exists():
        return out
    try:
        with np.load(path) as data:
            arr = data["candles"] if "candles" in data else None
    except Exception as e:
        try:
            ts = int(time.time())
            bad_path = path.with_name(path.name + f".corrupt.{ts}")
            os.replace(path, bad_path)
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_best_1m] corrupt_npz moved={bad_path.name} error={type(e).__name__}",
            )
        except Exception:
            pass
        return out
    if arr is None:
        return out
    day_start = _day_start_ms(datetime.strptime(_day_tag(day), "%Y-%m-%d").date())
    try:
        for row in arr:
            ts_ms = int(row["ts"])
            idx = int((ts_ms - day_start) // 60_000)
            if idx < 0 or idx >= 1440:
                continue
            out[idx] = {
                "t": ts_ms,
                "o": float(row["o"]),
                "h": float(row["h"]),
                "l": float(row["l"]),
                "c": float(row["c"]),
                "v": float(row["bv"]),
            }
    except Exception:
        return {}
    return out


def _write_day_npz(path: Path, candles_by_minute: dict[int, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for minute_idx in sorted(candles_by_minute.keys()):
        c = candles_by_minute[minute_idx]
        try:
            rows.append(
                (
                    int(c["t"]),
                    float(c["o"]),
                    float(c["h"]),
                    float(c["l"]),
                    float(c["c"]),
                    float(c["v"]),
                )
            )
        except Exception:
            continue
    dtype = np.dtype([
        ("ts", "i8"),
        ("o", "f4"),
        ("h", "f4"),
        ("l", "f4"),
        ("c", "f4"),
        ("bv", "f4"),
    ])
    arr = np.array(rows, dtype=dtype)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        np.savez_compressed(f, candles=arr)
    os.replace(tmp, path)


def _merge_api_candles_into_day_file(
    *,
    coin: str,
    day: str,
    candles: list[dict[str, Any]],
) -> int:
    out_path = _api_day_path(coin=coin, day=day)
    existing = _read_day_npz(out_path, day=day)
    added = 0
    day_start = _day_start_ms(datetime.strptime(_day_tag(day), "%Y-%m-%d").date())
    for c in candles:
        if not isinstance(c, dict):
            continue
        t = c.get("t")
        if t is None:
            continue
        try:
            ts_ms = int(t)
        except Exception:
            continue
        idx = int((ts_ms - day_start) // 60_000)
        if idx < 0 or idx >= 1440:
            continue
        if idx in existing:
            continue
        try:
            existing[idx] = {
                "t": ts_ms,
                "o": float(c.get("o")),
                "h": float(c.get("h")),
                "l": float(c.get("l")),
                "c": float(c.get("c")),
                "v": float(c.get("v")),
            }
        except Exception:
            continue
        added += 1
    if added:
        _write_day_npz(out_path, existing)
    return int(added)


def _read_hour_jsonl(path: Path) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    if not path.exists():
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                t = obj.get("t")
                if t is None:
                    continue
                try:
                    ts_ms = int(t)
                except Exception:
                    continue
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                out[int(dt.minute)] = obj
    except Exception:
        return out
    return out


def _write_hour_jsonl(path: Path, candles_by_minute: dict[int, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for minute in sorted(candles_by_minute.keys()):
        lines.append(json.dumps(candles_by_minute[minute], separators=(",", ":"), ensure_ascii=False))
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _fill_missing_from_exchange_perp_1m(
    *,
    exchange_id: str,
    symbol: str,
    coin: str,
    start_date: date,
    end_date: date,
    sleep_s: float = 0.2,
) -> int:
    """Fill missing minutes from an exchange's USDT perpetual.
    
    Args:
        exchange_id: Exchange identifier (e.g., 'binanceusdm', 'bybit')
        symbol: Symbol in exchange format (e.g., 'BTC/USDT:USDT')
        coin: Hyperliquid coin name for path resolution
        start_date: First date to check
        end_date: Last date to check
        sleep_s: Sleep between days (rate limiting)
    
    Returns:
        Number of minutes filled
    """
    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        raise ValueError("coin is empty")

    to_fill: list[str] = []
    for d in _iter_dates_inclusive(start_date, end_date):
        day_s = d.strftime("%Y%m%d")
        day_path = _best_day_path(coin=coin_u, day=day_s)
        if day_path.exists():
            existing = _read_day_npz(day_path, day=day_s)
            if len(existing) >= 1440:
                continue
        to_fill.append(day_s)
    if not to_fill:
        return 0

    ex = Exchange(exchange_id)

    minutes_filled = 0
    for day_s in to_fill:
        try:
            day_dt = datetime.strptime(day_s, "%Y%m%d").date()
        except Exception:
            continue

        day_start = _day_start_ms(day_dt)
        day_end = day_start + 86_400_000 - 1
        since = int(day_start)
        candles: list[list[Any]] = []

        try:
            while since < day_end:
                chunk = ex.fetch_ohlcv(symbol, "swap", "1m", limit=1500, since=since)
                if not chunk:
                    break
                candles.extend(chunk)
                last_ts = int(chunk[-1][0])
                if last_ts <= since:
                    break
                since = last_ts + 60_000
                if len(chunk) < 1000:
                    break
        except Exception as e:
            append_exchange_download_log("hyperliquid", f"[hl_best_1m] {coin_u} {exchange_id}_1m ERROR {day_s} {e}")
            continue

        day_path = _best_day_path(coin=coin_u, day=day_s)
        existing = _read_day_npz(day_path, day=day_s) if day_path.exists() else {}
        if len(existing) >= 1440:
            continue

        source_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)

        by_hour: dict[int, dict[int, dict[str, Any]]] = {}
        for c in candles:
            if not isinstance(c, (list, tuple)) or len(c) < 6:
                continue
            ts_ms = int(c[0])
            if ts_ms < day_start or ts_ms > day_end:
                continue
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            hh = int(dt.hour)
            mm = int(dt.minute)
            by_hour.setdefault(hh, {})[mm] = {
                "o": float(c[1]),
                "h": float(c[2]),
                "l": float(c[3]),
                "c": float(c[4]),
                "v": float(c[5]),
            }

        added = 0
        added_indices: list[int] = []
        missing = [m for m in range(1440) if m not in existing]
        gaps: list[tuple[int, int]] = []
        if missing:
            start = missing[0]
            prev = missing[0]
            for m in missing[1:]:
                if m == prev + 1:
                    prev = m
                    continue
                gaps.append((start, prev))
                start = m
                prev = m
            gaps.append((start, prev))

        for g_start, g_end in gaps:
            prev_idx = g_start - 1
            next_idx = g_end + 1
            prev_is_l2 = (
                source_codes is not None
                and 0 <= prev_idx < 1440
                and source_codes[prev_idx] == SOURCE_CODE_L2BOOK
            )
            next_is_l2 = (
                source_codes is not None
                and 0 <= next_idx < 1440
                and source_codes[next_idx] == SOURCE_CODE_L2BOOK
            )
            prev_c = existing.get(prev_idx) if prev_is_l2 else None
            next_o = existing.get(next_idx) if next_is_l2 else None

            for idx in range(g_start, g_end + 1):
                hh = idx // 60
                mm = idx % 60
                bn = by_hour.get(hh, {}).get(mm)
                if not bn:
                    continue
                o_val = bn["o"]
                c_val = bn["c"]
                if idx == g_start and prev_c is not None:
                    o_val = float(prev_c.get("c", o_val))
                if idx == g_end and next_o is not None:
                    c_val = float(next_o.get("o", c_val))
                try:
                    existing[idx] = {
                        "t": int(day_start + idx * 60_000),
                        "o": float(o_val),
                        "h": float(bn["h"]),
                        "l": float(bn["l"]),
                        "c": float(c_val),
                        "v": float(bn["v"]),
                    }
                except Exception:
                    continue
                added += 1
                added_indices.append(idx)
        if added:
            _write_day_npz(day_path, existing)
            minutes_filled += added
            update_source_index_for_day(
                exchange="hyperliquid",
                coin=coin_dir,
                day=day_s,
                minute_indices=added_indices,
                code=SOURCE_CODE_OTHER,
            )

        if sleep_s:
            time.sleep(float(sleep_s))

    return int(minutes_filled)


def _fill_missing_from_binance_perp_1m(
    *,
    coin: str,
    start_date: date,
    end_date: date,
    sleep_s: float = 0.2,
) -> int:
    """Fill missing minutes from Binance USDT perpetual (legacy wrapper)."""
    coin_u = normalize_hyperliquid_coin(coin)
    symbol = _resolve_perp_symbol("binanceusdm", coin_u)
    if not symbol:
        append_exchange_download_log("hyperliquid", f"[hl_best_1m] {coin_u} binanceusdm_1m SKIP no matching USDT perp symbol")
        return 0
    return _fill_missing_from_exchange_perp_1m(
        exchange_id="binanceusdm",
        symbol=symbol,
        coin=coin,
        start_date=start_date,
        end_date=end_date,
        sleep_s=sleep_s,
    )


def _fill_missing_from_bybit_perp_1m(
    *,
    coin: str,
    start_date: date,
    end_date: date,
    sleep_s: float = 0.2,
) -> int:
    """Fill missing minutes from Bybit USDT perpetual."""
    coin_u = normalize_hyperliquid_coin(coin)
    symbol = _resolve_perp_symbol("bybit", coin_u)
    if not symbol:
        append_exchange_download_log("hyperliquid", f"[hl_best_1m] {coin_u} bybit_1m SKIP no matching USDT perp symbol")
        return 0
    return _fill_missing_from_exchange_perp_1m(
        exchange_id="bybit",
        symbol=symbol,
        coin=coin,
        start_date=start_date,
        end_date=end_date,
        sleep_s=sleep_s,
    )


def _fill_missing_from_tradfi_1m(
    *,
    coin: str,
    start_date: date,
    end_date: date,
    timeout_s: float = 30.0,
    sleep_s: float = 0.0,
    stats_out: dict[str, int] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> int:
    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        raise ValueError("coin is empty")

    # Resolve ticker via tradfi_symbol_map.json (3-tier: map → no_provider → missing+WARNING)
    xyz_coin_name = _tradfi_ticker_from_hyperliquid_coin(coin_u)
    tiingo_ticker, tiingo_fx_ticker, tiingo_fx_invert, tiingo_start_date = resolve_tradfi_symbol(xyz_coin_name)
    ticker = None
    source_kind = None
    if tiingo_ticker:
        ticker = str(tiingo_ticker).upper()
        source_kind = "iex"
    elif tiingo_fx_ticker:
        ticker = str(tiingo_fx_ticker).lower()
        source_kind = "fx"
    else:
        return 0

    profiles = _load_tradfi_profiles_from_ini()
    tiingo_key = str((profiles.get("tiingo") or {}).get("api_key") or "").strip()
    has_tiingo = bool(tiingo_key)

    if not has_tiingo:
        append_exchange_download_log(
            "hyperliquid",
            f"[hl_best_1m] {coin_u} tradfi_1m SKIP no tiingo api_key in pbgui.ini [tradfi_profiles]",
        )
        return 0

    tiingo_minutes_filled = 0
    tiingo_month_requests_used = 0
    last_tiingo_day_bars = 0
    last_fx_chunk_fetched = False
    last_fx_chunk_has_any_bars = False
    last_iex_month_fetched = False
    last_iex_month_has_any_bars = False

    # IEX floor applies only to equity. FX runs without guessed listing floor.
    if source_kind == "fx":
        fx_floor = tiingo_start_date if tiingo_start_date is not None else start_date
        effective_start = max(start_date, fx_floor)
    else:
        iex_floor = _IEX_FLOOR_DATE
        if tiingo_start_date:
            iex_floor = max(iex_floor, tiingo_start_date)
        effective_start = max(start_date, iex_floor)

    for d in _iter_dates_inclusive(effective_start, end_date):
        day_s = d.strftime("%Y%m%d")
        day_tag = _day_tag(day_s)

        if source_kind == "fx":
            session = _default_fx_session_utc(d)
        else:
            session = _full_day_session_utc(d)
        if not session:
            continue

        session_start_ms, session_end_ms = int(session[0]), int(session[1])
        day_start_ms = _day_start_ms(d)
        start_idx = max(0, _minute_index(session_start_ms, day_start_ms))
        end_idx = min(1439, _minute_index(session_end_ms, day_start_ms))
        if end_idx < start_idx:
            continue

        day_path = _best_day_path(coin=coin_u, day=day_tag)
        existing = _read_day_npz(day_path, day=day_tag) if day_path.exists() else {}
        source_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
        existing_before = set(int(k) for k in existing.keys())

        def _allow_write(minute_idx: int) -> bool:
            if source_codes is None:
                return True
            if minute_idx >= len(source_codes):
                return True
            return int(source_codes[minute_idx]) in (0, int(SOURCE_CODE_OTHER))

        def _merge_bars(bars: list[dict[str, Any]]) -> int:
            added = 0
            for bar in bars:
                if not isinstance(bar, dict):
                    continue
                try:
                    idx = _minute_index(int(bar.get("t")), day_start_ms)
                    if idx < start_idx or idx > end_idx:
                        continue
                    if idx in existing:
                        continue
                    if not _allow_write(idx):
                        continue
                    existing[idx] = {
                        "t": int(bar.get("t")),
                        "o": float(bar.get("o")),
                        "h": float(bar.get("h")),
                        "l": float(bar.get("l")),
                        "c": float(bar.get("c")),
                        "v": float(bar.get("v", 0.0)),
                    }
                    added += 1
                except Exception:
                    continue
            return added

        initial_missing = [
            mi for mi in range(start_idx, end_idx + 1)
            if mi not in existing and _allow_write(mi)
        ]
        if not initial_missing:
            if sleep_s:
                time.sleep(float(sleep_s))
            continue

        def _tiingo_status_cb(evt: dict[str, Any]) -> None:
            if progress_cb is None or not isinstance(evt, dict):
                return
            try:
                payload = {
                    "stage": "tiingo_wait",
                    "day": day_s,
                    "month_key": d.strftime("%Y-%m"),
                    "ticker": str(evt.get("ticker") or ticker),
                    "tiingo_wait_s": int(evt.get("tiingo_wait_s") or 0),
                    "tiingo_wait_reason": str(evt.get("tiingo_wait_reason") or ""),
                    "tiingo_wait_kind": str(evt.get("tiingo_wait_kind") or ""),
                }
                progress_cb(payload)
            except Exception:
                pass

        fx_chunk_has_any_bars = False
        fx_chunk_fetched = False
        if source_kind == "fx":
            tiingo_bars, tiingo_month_fetch, fx_chunk_has_any_bars = _tiingo_fetch_1m_fx_day_from_month_cache(
                ticker=ticker,
                day=d,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                api_key=tiingo_key,
                timeout_s=float(timeout_s),
                invert=bool(tiingo_fx_invert),
                status_cb=_tiingo_status_cb,
            )
            fx_chunk_fetched = bool(tiingo_month_fetch)
        else:
            tiingo_bars, tiingo_month_fetch, iex_month_has_any_bars = _tiingo_fetch_1m_iex_day_from_month_cache(
                ticker=ticker,
                day=d,
                session_start_ms=session_start_ms,
                session_end_ms=session_end_ms,
                api_key=tiingo_key,
                timeout_s=float(timeout_s),
                status_cb=_tiingo_status_cb,
            )
            last_iex_month_fetched = bool(tiingo_month_fetch)
            last_iex_month_has_any_bars = bool(iex_month_has_any_bars)
        last_tiingo_day_bars = int(len(tiingo_bars)) if isinstance(tiingo_bars, list) else 0
        last_fx_chunk_fetched = bool(fx_chunk_fetched)
        last_fx_chunk_has_any_bars = bool(fx_chunk_has_any_bars)
        if tiingo_month_fetch:
            tiingo_month_requests_used += 1
        tiingo_minutes_filled += int(_merge_bars(tiingo_bars))

        new_indices = sorted(set(int(k) for k in existing.keys()) - existing_before)
        if new_indices:
            _write_day_npz(day_path, existing)
            update_source_index_for_day(
                exchange="hyperliquid",
                coin=coin_dir,
                day=day_s,
                minute_indices=new_indices,
                code=SOURCE_CODE_OTHER,
            )

        if sleep_s:
            time.sleep(float(sleep_s))

    if isinstance(stats_out, dict):
        stats_out["tiingo_minutes_filled"] = int(tiingo_minutes_filled)
        stats_out["tiingo_month_requests_used"] = int(tiingo_month_requests_used)
        stats_out["tiingo_day_bars"] = int(last_tiingo_day_bars)
        stats_out["tiingo_fx_chunk_fetched"] = 1 if bool(last_fx_chunk_fetched) else 0
        stats_out["tiingo_fx_chunk_has_data"] = 1 if bool(last_fx_chunk_has_any_bars) else 0
        stats_out["tiingo_iex_month_fetched"] = 1 if bool(last_iex_month_fetched) else 0
        stats_out["tiingo_iex_month_has_data"] = 1 if bool(last_iex_month_has_any_bars) else 0

    return int(tiingo_minutes_filled)




def _iter_dates_inclusive(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _first_json_obj_from_jsonl(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                obj = json.loads(s)
                return obj if isinstance(obj, dict) else None
    except Exception:
        return None
    return None


def _copy_api_1m_into_best(
    *,
    coin: str,
    start_date: date,
    end_date: date,
    dry_run: bool,
    overwrite: bool,
    only_missing_days: bool = False,
) -> tuple[int, int, int, int, int]:
    """Copy raw API 1m day files into the best archive dir.

    Source: data/ohlcv/hyperliquid/1m_api/<COIN>/YYYY-MM-DD.npz
    Dest:   data/ohlcv/hyperliquid/1m/<COIN>/YYYY-MM-DD.npz

    We prefer API data when available; API minutes overwrite existing minutes.

    Returns (n_copied_days, n_skipped_days, n_minutes_new, n_minutes_overwritten, n_minutes_written_total).
    """

    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    n_copied = 0
    n_skipped = 0
    n_minutes_new = 0
    n_minutes_overwritten = 0

    for d in _iter_dates_inclusive(start_date, end_date):
        day = d.strftime("%Y-%m-%d")
        src = _api_day_path(coin=coin_u, day=day)
        if not src.exists():
            n_skipped += 1
            continue
        dst = _best_day_path(coin=coin_u, day=day)

        if only_missing_days and dst.exists() and not overwrite:
            dst_data = _read_day_npz(dst, day=day)
            if len(dst_data) >= 1440:
                n_skipped += 1
                continue

        if dry_run:
            n_copied += 1
            continue

        src_data = _read_day_npz(src, day=day)
        if not src_data:
            n_skipped += 1
            continue
        dst_data = _read_day_npz(dst, day=day) if dst.exists() else {}

        for minute_idx, candle in src_data.items():
            if minute_idx in dst_data:
                n_minutes_overwritten += 1
            else:
                n_minutes_new += 1
            dst_data[minute_idx] = candle

        _write_day_npz(dst, dst_data)
        update_source_index_for_day(
            exchange="hyperliquid",
            coin=coin_dir,
            day=day,
            minute_indices=src_data.keys(),
            code=SOURCE_CODE_API,
        )
        n_copied += 1

    n_minutes_total = int(n_minutes_new + n_minutes_overwritten)
    return (
        int(n_copied),
        int(n_skipped),
        int(n_minutes_new),
        int(n_minutes_overwritten),
        n_minutes_total,
    )


@dataclass
class BuildBest1mResult:
    coin: str
    end_date: str
    l2book_oldest: str | None
    l2book_newest: str | None
    l2book_hours_written: int
    api_1m_files_written: int
    api_1m_files_skipped: int
    binance_1m_minutes_filled: int
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "l2book_oldest": self.l2book_oldest,
            "l2book_newest": self.l2book_newest,
            "l2book_hours_written": int(self.l2book_hours_written),
            "api_1m_files_written": int(self.api_1m_files_written),
            "api_1m_files_skipped": int(self.api_1m_files_skipped),
            "binance_1m_minutes_filled": int(self.binance_1m_minutes_filled),
            "notes": list(self.notes),
        }


@dataclass
class ImproveBest1mResult:
    coin: str
    end_date: str
    days_checked: int
    l2book_minutes_added: int
    binance_minutes_filled: int
    bybit_minutes_filled: int
    tiingo_minutes_filled: int = 0
    tiingo_month_requests_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "l2book_minutes_added": int(self.l2book_minutes_added),
            "binance_minutes_filled": int(self.binance_minutes_filled),
            "bybit_minutes_filled": int(self.bybit_minutes_filled),
            "tiingo_minutes_filled": int(self.tiingo_minutes_filled),
            "tiingo_month_requests_used": int(self.tiingo_month_requests_used),
        }


def build_best_hyperliquid_1m_archive_for_coin(
    *,
    coin: str,
    end_date: date | str | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
    timeout_s: float = 30.0,
    only_missing_days: bool = False,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> BuildBest1mResult:
    """Build the best-available continuous 1m archive.

        Strategy (best -> worst), without overwhelming the user with choices:
            1) If local l2Book exists: generate 1m from l2Book (exact-ish mid price, v=0)
            2) Download API 1m candles where available (stored in 1m_api)
            3) Copy API 1m into the best archive (1m), preferring API over synthetic

                Outputs:
                        - Raw API 1m downloads: data/ohlcv/hyperliquid/1m_api/<COIN>/YYYY-MM-DD.npz
                        - Best/computed archive: data/ohlcv/hyperliquid/1m/<COIN>/YYYY-MM-DD.npz
                            (with Binance USDT-perp gap fill for missing minutes)
    """

    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    d_end = end_date
    if d_end is None:
        d_end = date.today()
    if isinstance(d_end, str):
        d_end = datetime.strptime(d_end.strip(), "%Y-%m-%d").date() if "-" in d_end else datetime.strptime(d_end.strip(), "%Y%m%d").date()

    api_min_start = _api_farthest_lookback_start(d_end)

    notes: list[str] = []
    l2book_oldest_s: str | None = None
    l2book_newest_s: str | None = None
    l2book_hours_written = 0

    rng = get_local_l2book_day_range(coin=coin_u)
    if rng is not None:
        d0, d1 = rng
        l2book_oldest_s = d0.strftime("%Y%m%d")
        l2book_newest_s = d1.strftime("%Y%m%d")

        append_exchange_download_log(
            "hyperliquid",
            f"[hl_best_1m] {coin_u} l2book_range {l2book_oldest_s}->{l2book_newest_s}",
        )
        def _on_l2book_day_done(day_s: str) -> None:
            try:
                d = datetime.strptime(str(day_s), "%Y%m%d").date()
            except Exception:
                return
            if progress_cb is not None:
                try:
                    progress_cb({"stage": "binance_fill", "day": str(day_s)})
                except Exception:
                    pass
            _fill_missing_from_binance_perp_1m(
                coin=coin_u,
                start_date=d,
                end_date=d,
                sleep_s=0.0,
            )

        l2_res = generate_1m_candles_from_l2book_range(
            coin=coin_u,
            start_date=d0,
            end_date=d1,
            overwrite=bool(overwrite),
            dry_run=bool(dry_run),
            fill_missing=False,
            only_missing_days=bool(only_missing_days),
            progress_cb=progress_cb,
            day_done_cb=_on_l2book_day_done,
        )
        l2book_hours_written = int(l2_res.n_hours_written)
        api_start = max(d1 + timedelta(days=1), api_min_start)
    else:
        notes.append("no_local_l2book")
        api_start = api_min_start

    if api_start > d_end:
        api_start = d_end

    api_1m_files_written = 0
    api_1m_files_skipped = 0
    binance_1m_minutes_filled = 0

    # Prefer true API 1m where it exists.
    try:
        api_res = download_hyperliquid_candles_api(
            coin=coin_u,
            interval="1m",
            start_date=api_start,
            end_date=d_end,
            overwrite=False if not overwrite else True,
            dry_run=bool(dry_run),
            timeout_s=float(timeout_s),
            sleep_s=0.05,
        )
        api_1m_files_written = int(api_res.n_files_written)
        api_1m_files_skipped = int(api_res.n_files_skipped)
    except Exception as e:
        notes.append(f"api_1m_error:{type(e).__name__}")
        append_exchange_download_log("hyperliquid", f"[hl_best_1m] {coin_u} api_1m ERROR {e}")

    try:
        copied, skipped, min_new, min_overwritten, min_total = _copy_api_1m_into_best(
            coin=coin_u,
            start_date=api_start,
            end_date=d_end,
            dry_run=bool(dry_run),
            overwrite=bool(overwrite),
            only_missing_days=bool(only_missing_days),
        )
        notes.append(f"api_1m_copied_to_best:{copied}")
        if skipped:
            notes.append(f"api_1m_skipped_to_best:{skipped}")
        notes.append(f"api_1m_minutes_written_to_best:{min_total}")
        if min_new:
            notes.append(f"api_1m_minutes_new_to_best:{min_new}")
        if min_overwritten:
            notes.append(f"api_1m_minutes_overwritten_to_best:{min_overwritten}")
    except Exception as e:
        notes.append(f"api_1m_copy_error:{type(e).__name__}")
        append_exchange_download_log("hyperliquid", f"[hl_best_1m] {coin_u} api_1m COPY ERROR {e}")

    # Fill remaining gaps with Binance USDT-perp 1m candles.
    try:
        fill_start = api_start if rng is not None else api_start
        binance_1m_minutes_filled = _fill_missing_from_binance_perp_1m(
            coin=coin_u,
            start_date=fill_start,
            end_date=d_end,
        )
        if binance_1m_minutes_filled:
            notes.append(f"binance_1m_minutes_filled:{binance_1m_minutes_filled}")
    except Exception as e:
        notes.append(f"binance_1m_error:{type(e).__name__}")
        append_exchange_download_log("hyperliquid", f"[hl_best_1m] {coin_u} binance_1m ERROR {e}")

    # Upsampling disabled: keep output strictly API 1m and l2Book-derived 1m.

    res = BuildBest1mResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y%m%d"),
        l2book_oldest=l2book_oldest_s,
        l2book_newest=l2book_newest_s,
        l2book_hours_written=int(l2book_hours_written),
        api_1m_files_written=int(api_1m_files_written),
        api_1m_files_skipped=int(api_1m_files_skipped),
        binance_1m_minutes_filled=int(binance_1m_minutes_filled),
        notes=notes,
    )
    append_exchange_download_log("hyperliquid", f"[INFO] [hl_best_1m] done {res.to_dict()}")
    return res


def improve_best_hyperliquid_1m_archive_for_coin(
    *,
    coin: str,
    end_date: date | str | None = None,
    start_date_override: date | str | None = None,
    dry_run: bool = False,
    refetch: bool = False,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> ImproveBest1mResult:
    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin_u)
    d_end = end_date
    if d_end is None:
        d_end = date.today()
    if isinstance(d_end, str):
        d_end = datetime.strptime(d_end.strip(), "%Y-%m-%d").date() if "-" in d_end else datetime.strptime(d_end.strip(), "%Y%m%d").date()

    d_start_override: date | None = None
    if start_date_override is not None and str(start_date_override).strip() != "":
        if isinstance(start_date_override, date):
            d_start_override = start_date_override
        elif isinstance(start_date_override, str):
            s = str(start_date_override).strip()
            try:
                d_start_override = datetime.strptime(s, "%Y-%m-%d").date() if "-" in s else datetime.strptime(s, "%Y%m%d").date()
            except Exception:
                d_start_override = None
    if d_start_override is not None and d_start_override > d_end:
        d_start_override = d_end

    is_stock_perp = _is_stock_perp_coin(coin_u)
    tradfi_source_kind: str | None = None
    tradfi_tiingo_start_date: date | None = None
    if is_stock_perp:
        xyz_coin_name = _tradfi_ticker_from_hyperliquid_coin(coin_u)
        tiingo_ticker, tiingo_fx_ticker, _, tradfi_tiingo_start_date = resolve_tradfi_symbol(xyz_coin_name)
        if tiingo_ticker:
            tradfi_source_kind = "iex"
        elif tiingo_fx_ticker:
            tradfi_source_kind = "fx"
    tradfi_backfill_mode = bool(is_stock_perp and tradfi_source_kind in ("fx", "iex"))

    earliest_candidates: list[date] = []
    l2_rng = get_local_l2book_day_range(coin=coin_u)
    if l2_rng is not None:
        earliest_candidates.append(l2_rng[0])
    api_days = _list_api_days(coin=coin_u)
    if api_days:
        earliest_candidates.append(min(api_days))
    best_days = _list_best_days(coin=coin_u)
    if best_days:
        earliest_candidates.append(min(best_days))

    if is_stock_perp and tradfi_backfill_mode:
        oldest_other = None
        if not refetch:
            oldest_other = get_oldest_day_with_source_code(
                exchange="hyperliquid",
                coin=coin_dir,
                code=SOURCE_CODE_OTHER,
            )

        oldest_other_date: date | None = None
        if oldest_other:
            try:
                oldest_other_date = datetime.strptime(oldest_other, "%Y%m%d").date()
            except Exception:
                oldest_other_date = None

        if refetch or oldest_other_date is None:
            cursor_start = d_end
        else:
            cursor_start = min(d_end, oldest_other_date - timedelta(days=1))

        if tradfi_source_kind == "iex":
            lower_bound = _determine_stock_perp_improve_start(
                coin_u=coin_u,
                coin_dir=coin_dir,
                d_end=d_end,
                earliest_candidates=earliest_candidates,
                tiingo_start_date=tradfi_tiingo_start_date,
                refetch=refetch,
                timeout_s=30.0,
            )
            if d_start_override is not None:
                lower_bound = max(lower_bound, d_start_override)
        else:
            lower_bound = d_end - timedelta(days=int(TRADFI_DISCOVERY_MAX_LOOKBACK_DAYS))
            if d_start_override is not None:
                lower_bound = max(lower_bound, d_start_override)

        days = []
        cur = cursor_start
        while cur >= lower_bound:
            days.append(cur)
            cur -= timedelta(days=1)
    elif is_stock_perp:
        days = []
    else:
        if d_start_override is not None:
            start_day = d_start_override
            days = [d for d in _iter_dates_inclusive(start_day, d_end)]
        elif not earliest_candidates:
            days = []
        else:
            start_day = min(earliest_candidates)
            days = [d for d in _iter_dates_inclusive(start_day, d_end)]
    days_checked = 0
    l2book_minutes_added = 0
    binance_minutes_filled = 0
    bybit_minutes_filled = 0
    tiingo_minutes_filled = 0
    tiingo_month_requests_used = 0
    month_totals: dict[str, int] = {}
    month_seen: dict[str, int] = {}
    month_progress_by_day: dict[str, tuple[str, int, int]] = {}
    for d in days:
        mk = d.strftime("%Y-%m")
        month_totals[mk] = int(month_totals.get(mk, 0)) + 1
    for d in days:
        day_s_local = d.strftime("%Y%m%d")
        mk = d.strftime("%Y-%m")
        seen = int(month_seen.get(mk, 0)) + 1
        month_seen[mk] = seen
        month_progress_by_day[day_s_local] = (mk, seen, int(month_totals.get(mk, seen)))

    if progress_cb is not None:
        try:
            progress_cb({"stage": "improve", "planned": len(days), "done": 0})
        except Exception:
            pass

    tradfi_empty_period_streak = 0

    for i, d in enumerate(days, start=1):
        day_start_time = time.time()
        day_s = d.strftime("%Y%m%d")
        day_tag = d.strftime("%Y-%m-%d")
        day_path = _best_day_path(coin=coin_u, day=day_tag)
        
        t0 = time.time()
        existing = _read_day_npz(day_path, day=day_tag) if day_path.exists() else {}
        t_read = time.time() - t0
        
        # OPTIMIZATION 1: Skip days that are already complete
        if len(existing) >= 1440:
            if ENABLE_TIMING_LOGS:
                day_total_time = time.time() - day_start_time
                append_exchange_download_log(
                    "hyperliquid",
                    f"[TIMING] {coin_u} {day_s} total={day_total_time:.3f}s "
                    f"read={t_read:.3f}s SKIPPED (complete, existing=1440)"
                )
            days_checked += 1
            if progress_cb is not None:
                try:
                    month_key, month_day_index, month_day_total = month_progress_by_day.get(day_s, ("", 0, 0))
                    progress_cb({
                        "stage": "improve",
                        "planned": len(days),
                        "done": i,
                        "day": day_s,
                        "days_checked": int(days_checked),
                        "l2book_minutes_added": int(l2book_minutes_added),
                        "binance_minutes_filled": int(binance_minutes_filled),
                        "bybit_minutes_filled": int(bybit_minutes_filled),
                        "month_key": str(month_key),
                        "month_day_index": int(month_day_index),
                        "month_day_total": int(month_day_total),
                        "tiingo_wait_s": 0,
                        "tiingo_wait_reason": "",
                        "tiingo_wait_kind": "",
                        "tradfi_source_kind": str(tradfi_source_kind or ""),
                        "fx_backfill_mode": bool(tradfi_backfill_mode),
                        "fx_backfill_direction": "newest_to_oldest" if tradfi_backfill_mode else "",
                        "fx_empty_chunk_streak": int(tradfi_empty_period_streak),
                    })
                except Exception:
                    pass
            continue
        
        t0 = time.time()
        source_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
        t_src_index = time.time() - t0

        api_path = _api_day_path(coin=coin_u, day=day_tag)
        t_api = 0.0
        if api_path.exists() and not dry_run:
            t0 = time.time()
            api_data = _read_day_npz(api_path, day=day_tag)
            api_added: list[int] = []
            for idx, candle in api_data.items():
                if source_codes is not None:
                    if source_codes[idx] not in (SOURCE_CODE_OTHER, 0):
                        continue
                existing[idx] = candle
                api_added.append(idx)
            if api_added:
                _write_day_npz(day_path, existing)
                update_source_index_for_day(
                    exchange="hyperliquid",
                    coin=coin_dir,
                    day=day_s,
                    minute_indices=api_added,
                    code=SOURCE_CODE_API,
                )
                source_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
            t_api = time.time() - t0

        # Use 1m source index to avoid rebuilding an hour once l2Book migration already
        # touched that hour at least once. From then on, remaining gaps are filled by
        # Binance/Bybit only.
        t0 = time.time()
        hours_to_rebuild: set[int] = set()
        if source_codes is None:
            hours_to_rebuild = set(range(24))
        else:
            for hour in range(24):
                h0 = hour * 60
                h1 = h0 + 60
                any_src_l2book = False
                for mi in range(h0, h1):
                    if mi < len(source_codes) and int(source_codes[mi]) == int(SOURCE_CODE_L2BOOK):
                        any_src_l2book = True
                        break
                if not any_src_l2book:
                    hours_to_rebuild.add(hour)
        t_hours_check = time.time() - t0

        t_l2book = 0.0
        t_l2book_write = 0.0
        added_indices: list[int] = []
        if not is_stock_perp:
            t0 = time.time()
            l2_map = _l2book_minutes_for_day(
                coin=coin_u,
                day=day_s,
                hours_filter=hours_to_rebuild,
            )
            t_l2book = time.time() - t0

            t0 = time.time()
            for idx, candle in l2_map.items():
                if idx in existing:
                    if source_codes is None:
                        continue
                    if source_codes[idx] != SOURCE_CODE_OTHER:
                        continue
                existing[idx] = candle
                added_indices.append(idx)

            if added_indices and not dry_run:
                _write_day_npz(day_path, existing)
                update_source_index_for_day(
                    exchange="hyperliquid",
                    coin=coin_dir,
                    day=day_s,
                    minute_indices=added_indices,
                    code=SOURCE_CODE_L2BOOK,
                )
                l2book_minutes_added += len(added_indices)
            t_l2book_write = time.time() - t0

        # OPTIMIZATION 2: Only call Binance/Bybit if there are actual gaps
        t_binance = 0.0
        t_bybit = 0.0
        if not dry_run and len(existing) < 1440:
            if is_stock_perp:
                t0 = time.time()
                tradfi_fill_stats: dict[str, int] = {}

                # Explicit guard: skip TradFi fetch when target session minutes are already covered
                # by best/API data for this day.
                needs_tradfi_fetch = False
                if tradfi_source_kind == "fx":
                    session = _default_fx_session_utc(d)
                else:
                    session = _full_day_session_utc(d)
                if session:
                    session_start_ms, session_end_ms = int(session[0]), int(session[1])
                    day_start_ms = _day_start_ms(d)
                    start_idx = max(0, _minute_index(session_start_ms, day_start_ms))
                    end_idx = min(1439, _minute_index(session_end_ms, day_start_ms))
                    if end_idx >= start_idx:
                        for minute_idx in range(start_idx, end_idx + 1):
                            if minute_idx in existing:
                                continue
                            if source_codes is not None and minute_idx < len(source_codes):
                                if int(source_codes[minute_idx]) not in (0, int(SOURCE_CODE_OTHER)):
                                    continue
                            needs_tradfi_fetch = True
                            break

                if needs_tradfi_fetch:
                    tiingo_added = _fill_missing_from_tradfi_1m(
                        coin=coin_u,
                        start_date=d,
                        end_date=d,
                        timeout_s=30.0,
                        sleep_s=0.0,
                        stats_out=tradfi_fill_stats,
                        progress_cb=progress_cb,
                    )
                else:
                    tiingo_added = 0

                tiingo_minutes_filled += int(tiingo_added)
                tiingo_month_requests_used += int(tradfi_fill_stats.get("tiingo_month_requests_used") or 0)

                if tradfi_source_kind == "fx":
                    if int(tradfi_fill_stats.get("tiingo_fx_chunk_fetched") or 0) > 0:
                        if int(tradfi_fill_stats.get("tiingo_fx_chunk_has_data") or 0) > 0:
                            tradfi_empty_period_streak = 0
                        else:
                            tradfi_empty_period_streak += 1
                elif tradfi_source_kind == "iex":
                    if int(tradfi_fill_stats.get("tiingo_iex_month_fetched") or 0) > 0:
                        if int(tradfi_fill_stats.get("tiingo_iex_month_has_data") or 0) > 0:
                            tradfi_empty_period_streak = 0
                        else:
                            tradfi_empty_period_streak += 1
                t_binance = time.time() - t0
                t_bybit = 0.0
            else:
                t0 = time.time()
                before_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
                _fill_missing_from_binance_perp_1m(
                    coin=coin_u,
                    start_date=d,
                    end_date=d,
                    sleep_s=0.0,
                )
                after_binance_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
                if after_binance_codes:
                    if before_codes is None:
                        binance_minutes_filled += sum(1 for c in after_binance_codes if c == SOURCE_CODE_OTHER)
                    else:
                        binance_minutes_filled += sum(
                            1 for b, a in zip(before_codes, after_binance_codes) if a == SOURCE_CODE_OTHER and b != SOURCE_CODE_OTHER
                        )
                else:
                    before_keys = set(existing.keys())
                    after = _read_day_npz(day_path, day=day_tag) if day_path.exists() else {}
                    new_keys = sorted(set(after.keys()) - before_keys)
                    if new_keys:
                        binance_minutes_filled += len(new_keys)
                t_binance = time.time() - t0

                t0 = time.time()
                before_bybit_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
                _fill_missing_from_bybit_perp_1m(
                    coin=coin_u,
                    start_date=d,
                    end_date=d,
                    sleep_s=0.0,
                )
                after_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day_s)
                if after_codes:
                    if before_bybit_codes is None:
                        bybit_minutes_filled += sum(1 for c in after_codes if c == SOURCE_CODE_OTHER)
                    else:
                        bybit_minutes_filled += sum(
                            1 for b, a in zip(before_bybit_codes, after_codes) if a == SOURCE_CODE_OTHER and b != SOURCE_CODE_OTHER
                        )
                else:
                    before_keys = set(existing.keys())
                    after = _read_day_npz(day_path, day=day_tag) if day_path.exists() else {}
                    new_keys = sorted(set(after.keys()) - before_keys)
                    if new_keys:
                        bybit_minutes_filled += len(new_keys)
                t_bybit = time.time() - t0

        if not dry_run and day_path.exists():
            existing = _read_day_npz(day_path, day=day_tag)

        day_total_time = time.time() - day_start_time
        
        # Log detailed timing for this day (if enabled)
        if ENABLE_TIMING_LOGS:
            append_exchange_download_log(
                "hyperliquid",
                f"[TIMING] {coin_u} {day_s} total={day_total_time:.3f}s "
                f"read={t_read:.3f}s src_idx={t_src_index:.3f}s api={t_api:.3f}s "
                f"l2book={t_l2book:.3f}s l2write={t_l2book_write:.3f}s "
                f"binance={t_binance:.3f}s bybit={t_bybit:.3f}s "
                f"existing={len(existing)} l2added={len(added_indices)}"
            )

        days_checked += 1
        if progress_cb is not None:
            try:
                month_key, month_day_index, month_day_total = month_progress_by_day.get(day_s, ("", 0, 0))
                progress_cb(
                    {
                        "stage": "improve",
                        "planned": len(days),
                        "done": i,
                        "day": day_s,
                        "days_checked": int(days_checked),
                        "l2book_minutes_added": int(l2book_minutes_added),
                        "binance_minutes_filled": int(binance_minutes_filled),
                        "bybit_minutes_filled": int(bybit_minutes_filled),
                        "tiingo_minutes_filled": int(tiingo_minutes_filled),
                        "tiingo_month_requests_used": int(tiingo_month_requests_used),
                        "month_key": str(month_key),
                        "month_day_index": int(month_day_index),
                        "month_day_total": int(month_day_total),
                        "tiingo_wait_s": 0,
                        "tiingo_wait_reason": "",
                        "tiingo_wait_kind": "",
                        "tradfi_source_kind": str(tradfi_source_kind or ""),
                        "fx_backfill_mode": bool(tradfi_backfill_mode),
                        "fx_backfill_direction": "newest_to_oldest" if tradfi_backfill_mode else "",
                        "fx_empty_chunk_streak": int(tradfi_empty_period_streak),
                    }
                )
            except Exception:
                pass

        if (
            is_stock_perp
            and tradfi_source_kind in ("fx", "iex")
            and int(tradfi_empty_period_streak) >= int(TRADFI_EMPTY_PERIODS_STOP)
        ):
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_best_1m] {coin_u} tradfi_1m stop: {tradfi_empty_period_streak} consecutive empty {'weeks' if tradfi_source_kind == 'fx' else 'months'} (newest→oldest)",
                level="INFO",
            )
            break

    return ImproveBest1mResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y%m%d"),
        days_checked=int(days_checked),
        l2book_minutes_added=int(l2book_minutes_added),
        binance_minutes_filled=int(binance_minutes_filled),
        bybit_minutes_filled=int(bybit_minutes_filled),
        tiingo_minutes_filled=int(tiingo_minutes_filled),
        tiingo_month_requests_used=int(tiingo_month_requests_used),
    )


def update_latest_hyperliquid_1m_api_for_coin(
    *,
    coin: str,
    lookback_days: int = 7,
    overwrite: bool = False,
    dry_run: bool = False,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """Refresh the most recent 1m window from the API (overwrite).

    Hyperliquid only provides the most recent ~5000 1m candles (~3.5 days),
    so overwriting the last N days is the simplest "keep it updated" workflow.
    """

    coin_u = normalize_hyperliquid_coin(coin)
    # Hyperliquid uses k-prefixed symbols for some coins.
    k_prefix_coins = {"BONK", "FLOKI", "LUNC", "PEPE", "SHIB", "DOGS", "NEIRO"}
    if coin_u in k_prefix_coins:
        coin_u = f"k{coin_u}"

    # Resolve once upfront so unavailable coins are handled gracefully and we
    # avoid repeated per-day resolve errors in logs.
    try:
        coin_u = resolve_hyperliquid_coin_name(coin=coin_u, timeout_s=float(timeout_s))
    except ValueError as e:
        msg = str(e)
        if "does not contain coin" in msg:
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_latest_1m] skip coin={coin_u} reason=not_in_live_meta",
                level="WARNING",
            )
            return {
                "coin": coin_u,
                "lookback_days": int(max(1, int(lookback_days))),
                "overwrite": False,
                "hours_requested": 0,
                "minutes_filled": 0,
                "best_sync_days_copied": 0,
                "best_sync_days_skipped": 0,
                "skipped": True,
                "skip_reason": "not_in_live_meta",
            }
        raise
    lb = int(lookback_days)
    if lb < 1:
        lb = 1

    # Use UTC to avoid requesting future hours when local time is ahead of UTC.
    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    d_end = now_utc.date()
    d_start = d_end - timedelta(days=lb)

    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin_u)
    if not coin_dir:
        return {
            "coin": coin_u,
            "lookback_days": lb,
            "overwrite": bool(overwrite),
            "result": {
                "result": "error",
                "error": f"Unknown coin '{coin_u}' for hyperliquid",
            },
        }

    if overwrite:
        r = download_hyperliquid_candles_api(
            coin=coin_u,
            interval="1m",
            start_date=d_start,
            end_date=d_end,
            overwrite=True,
            dry_run=bool(dry_run),
            timeout_s=float(timeout_s),
            sleep_s=0.05,
        )
        return {"coin": coin_u, "lookback_days": lb, "overwrite": True, "result": r.to_dict()}

    # Selective fetch: only request missing minutes/hours and merge into existing files.
    start_day = d_start.strftime("%Y%m%d")
    end_day = d_end.strftime("%Y%m%d")
    presence = get_minute_presence_for_dataset(
        "hyperliquid",
        "1m_api",
        coin_dir,
        start_day=start_day,
        end_day=end_day,
    )

    days_present = presence.get("days") if isinstance(presence, dict) else {}
    hours_requested = 0
    minutes_filled = 0
    days_requested = 0
    full_gap_days = 0
    partial_gap_days = 0

    # Always scan the full lookback range; missing days should still be requested.
    if not isinstance(days_present, dict):
        days_present = {}
    days: dict[str, dict[str, dict[int, str]]] = {}
    cur = d_start
    while cur <= d_end:
        day_s = cur.strftime("%Y%m%d")
        day_map = days_present.get(day_s)
        days[day_s] = day_map if isinstance(day_map, dict) else {}
        cur = cur + timedelta(days=1)

    now_ms = int(now_utc.timestamp() * 1000)

    def _fetch_day_with_retry(*, day_start_ms: int, day_end_ms: int) -> list[dict[str, Any]]:
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                return fetch_candle_snapshot(
                    coin=coin_u,
                    interval="1m",
                    start_ms=day_start_ms,
                    end_ms=day_end_ms,
                    timeout_s=float(timeout_s),
                )
            except requests.exceptions.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 429 and attempt < max_attempts:
                    sleep_s = 0.5 * (2 ** (attempt - 1))
                    append_exchange_download_log(
                        "hyperliquid",
                        f"[hl_latest_1m] rate_limited coin={coin_u} day={day_s} attempt={attempt} sleep_s={sleep_s}",
                    )
                    time.sleep(float(sleep_s))
                    continue
                body = ""
                try:
                    body = str(getattr(e.response, "text", "") or "")[:200]
                except Exception:
                    body = ""
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_latest_1m] http_error coin={coin_u} day={day_s} status={status} body={body}",
                )
                raise
            except Exception as e:
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_latest_1m] error coin={coin_u} day={day_s} err={type(e).__name__}: {e}",
                )
                raise
        return []

    for day_s, hours_map in sorted(days.items()):
        if not isinstance(hours_map, dict):
            hours_map = {}

        try:
            d = datetime.strptime(day_s, "%Y%m%d").date()
        except Exception:
            continue

        start_ms = _day_start_ms(d)
        if start_ms > now_ms:
            continue

        # Only consider minutes that can already exist (avoid counting/requesting future minutes).
        day_end_ms = start_ms + 86_400_000 - 1
        effective_end_ms = min(day_end_ms, now_ms)
        if effective_end_ms < start_ms:
            continue

        valid_minutes = int((effective_end_ms - start_ms) // 60_000) + 1
        if valid_minutes < 1:
            continue
        if valid_minutes > 1440:
            valid_minutes = 1440

        missing_total = 0
        for minute_idx in range(valid_minutes):
            hour = minute_idx // 60
            minute = minute_idx % 60
            hh = f"{hour:02d}"
            mins_map = hours_map.get(hh) or {}
            mins_map = mins_map if isinstance(mins_map, dict) else {}
            if (minute not in mins_map) and (str(minute) not in mins_map):
                missing_total += 1

        if missing_total <= 0:
            continue
        days_requested += 1
        if missing_total >= valid_minutes:
            full_gap_days += 1
        else:
            partial_gap_days += 1
        if dry_run:
            hours_requested += 1
            continue

        candles = _fetch_day_with_retry(day_start_ms=int(start_ms), day_end_ms=int(effective_end_ms))
        hours_requested += 1
        minutes_filled += _merge_api_candles_into_day_file(
            coin=coin_u,
            day=day_s,
            candles=candles,
        )
        append_exchange_download_log(
            "hyperliquid",
            (
                f"[hl_latest_1m] api_request coin={coin_u} day={day_s} "
                f"missing_min_before_fetch={missing_total} valid_min={valid_minutes} "
                f"present_min_before_fetch={max(0, valid_minutes - missing_total)}"
            ),
        )
        time.sleep(0.1)

    sync_mode = "catchup" if full_gap_days > 0 else "incremental"

    # Keep best 1m archive in sync when:
    # - bootstrap is needed (no local best data yet, but API days exist), or
    # - this run merged new API data.
    copied_best_days = 0
    skipped_best_days = 0
    copied_best_minutes_total = 0
    copied_best_minutes_new = 0
    copied_best_minutes_overwritten = 0
    best_sync_mode = "skipped"
    api_days = _list_api_days(coin=coin_u)
    has_best_data = bool(_list_best_days(coin=coin_u))
    should_sync_best = (not has_best_data and bool(api_days)) or int(minutes_filled) > 0

    if should_sync_best:
        # - bootstrap/catchup: sync full available API window
        # - incremental: fast path (today + previous day)
        sync_start, best_sync_mode = _determine_best_sync_start(
            d_start=d_start,
            d_end=d_end,
            has_best_data=has_best_data,
            full_gap_days=full_gap_days,
            api_days=api_days,
        )
        (
            copied_best_days,
            skipped_best_days,
            copied_best_minutes_new,
            copied_best_minutes_overwritten,
            copied_best_minutes_total,
        ) = _copy_api_1m_into_best(
            coin=coin_u,
            start_date=sync_start,
            end_date=d_end,
            dry_run=bool(dry_run),
            overwrite=False,
            only_missing_days=False,
        )
        append_exchange_download_log(
            "hyperliquid",
            (
                f"[hl_latest_1m] best_sync coin={coin_u} mode={best_sync_mode} "
                f"start={sync_start.strftime('%Y%m%d')} end={d_end.strftime('%Y%m%d')} "
                f"copied_days={copied_best_days} skipped_days={skipped_best_days} "
                f"copied_minutes={copied_best_minutes_total} "
                f"new_minutes={copied_best_minutes_new} overwritten_minutes={copied_best_minutes_overwritten}"
            ),
        )
    else:
        skip_reason = "no_new_data"
        if not has_best_data and not api_days:
            skip_reason = "no_api_data"
        append_exchange_download_log(
            "hyperliquid",
            (
                f"[hl_latest_1m] best_sync coin={coin_u} mode=skipped "
                f"reason={skip_reason} copied_days=0 skipped_days=0"
            ),
        )

    result = {
        "coin": coin_u,
        "lookback_days": lb,
        "overwrite": False,
        "hours_requested": int(hours_requested),
        "minutes_filled": int(minutes_filled),
        "days_requested": int(days_requested),
        "full_gap_days": int(full_gap_days),
        "partial_gap_days": int(partial_gap_days),
        "mode": sync_mode,
        "best_sync_days_copied": int(copied_best_days),
        "best_sync_days_skipped": int(skipped_best_days),
        "best_sync_minutes_copied": int(copied_best_minutes_total),
        "best_sync_minutes_new": int(copied_best_minutes_new),
        "best_sync_minutes_overwritten": int(copied_best_minutes_overwritten),
    }
    return result
