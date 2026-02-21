from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Callable
import configparser
import json
import os
import time
import numpy as np
import requests

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
TRADFI_DISCOVERY_WINDOW_DAYS = 14
TRADFI_ERROR_LOG_THROTTLE_SECONDS = 300
POLYGON_RETRY_ATTEMPTS = 4
POLYGON_RETRY_BACKOFF_SECONDS = 1.5
ALPACA_RETRY_ATTEMPTS = 4
ALPACA_RETRY_BACKOFF_SECONDS = 1.5
POLYGON_FREE_MAX_LOOKBACK_DAYS = 730
POLYGON_MAX_RESULTS_PER_CALL = 50_000
POLYGON_EST_SESSION_MINUTES_PER_DAY = 390
# Derived from Polygon aggregate-bars docs (limit max 50,000 results).
# For stock session data (~390 1m bars/day), this is the largest minute window
# that should stay within one call's result cap.
POLYGON_MAX_RANGE_MINUTES_PER_CALL = int((POLYGON_MAX_RESULTS_PER_CALL * 1440) // POLYGON_EST_SESSION_MINUTES_PER_DAY)
_TRADFI_ERROR_LAST_TS: dict[str, float] = {}
_POLYGON_ACCESS_MODE_CACHE: dict[str, str] = {}


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


def _load_tradfi_profiles_from_ini() -> dict[str, dict[str, str]]:
    cfg = configparser.ConfigParser()
    ini_path = Path(__file__).resolve().parent / "pbgui.ini"
    cfg.read(ini_path)
    sec = "tradfi_profiles"
    out: dict[str, dict[str, str]] = {
        "alpaca": {"api_key": "", "api_secret": ""},
        "polygon": {"api_key": "", "api_secret": ""},
    }
    if not cfg.has_section(sec):
        return out
    out["alpaca"]["api_key"] = str(cfg.get(sec, "alpaca_api_key", fallback="") or "").strip()
    out["alpaca"]["api_secret"] = str(cfg.get(sec, "alpaca_api_secret", fallback="") or "").strip()
    out["polygon"]["api_key"] = str(cfg.get(sec, "polygon_api_key", fallback="") or "").strip()
    out["polygon"]["api_secret"] = str(cfg.get(sec, "polygon_api_secret", fallback="") or "").strip()
    return out


def _is_stock_perp_coin(coin: str) -> bool:
    s = str(coin or "")
    if not s:
        return False
    base = s.split("/")[0]
    u = base.upper()
    return u.startswith("XYZ:") or u.startswith("XYZ-")


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


def _alpaca_fetch_trading_sessions(
    *,
    start_date: date,
    end_date: date,
    api_key: str,
    api_secret: str,
    timeout_s: float,
) -> dict[str, tuple[int, int]]:
    if not api_key or not api_secret:
        return {}
    url = "https://paper-api.alpaca.markets/v2/calendar"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }
    params = {
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
    }
    out: dict[str, tuple[int, int]] = {}
    attempts = int(max(1, ALPACA_RETRY_ATTEMPTS))
    rows: Any = None
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=float(timeout_s))
            status = int(resp.status_code)
            if status in (401, 403, 422):
                return {}
            if status in (429, 500, 502, 503, 504):
                if attempt < attempts - 1:
                    time.sleep(_retry_after_or_backoff_seconds(resp=resp, attempt=attempt, base_backoff_s=float(ALPACA_RETRY_BACKOFF_SECONDS)))
                    continue
            resp.raise_for_status()
            rows = resp.json()
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
                time.sleep(_retry_after_or_backoff_seconds(resp=getattr(e, "response", None), attempt=attempt, base_backoff_s=float(ALPACA_RETRY_BACKOFF_SECONDS)))
                continue
            break

    if last_err is not None:
        status = None
        try:
            status = int(getattr(getattr(last_err, "response", None), "status_code", None))
        except Exception:
            status = None
        _log_tradfi_error_throttled(provider="alpaca", ticker="CALENDAR", err=last_err, status=status)
        return {}

    if not isinstance(rows, list):
        return {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        day_s = str(row.get("date") or "").strip()
        open_s = str(row.get("open") or "").strip()
        close_s = str(row.get("close") or "").strip()
        if not day_s or not open_s or not close_s:
            continue
        try:
            y, m, d = [int(x) for x in day_s.split("-")]
            oh, om = [int(x) for x in open_s.split(":")[:2]]
            ch, cm = [int(x) for x in close_s.split(":")[:2]]
            open_dt = datetime(y, m, d, oh, om, tzinfo=_ET_TZ).astimezone(timezone.utc)
            close_dt = datetime(y, m, d, ch, cm, tzinfo=_ET_TZ).astimezone(timezone.utc)
            start_ms = int(open_dt.timestamp() * 1000)
            end_ms = int(close_dt.timestamp() * 1000) - 60_000
            if end_ms >= start_ms:
                out[day_s.replace("-", "")] = (start_ms, end_ms)
        except Exception:
            continue
    return out


def _alpaca_fetch_1m_iex(
    *,
    ticker: str,
    start_ms: int,
    end_ms: int,
    api_key: str,
    api_secret: str,
    timeout_s: float,
) -> list[dict[str, Any]]:
    if not api_key or not api_secret:
        return []
    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }
    params: dict[str, Any] = {
        "timeframe": "1Min",
        "start": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": 10000,
        "adjustment": "split",
        "feed": "iex",
    }
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    for _ in range(20):
        if page_token:
            params["page_token"] = page_token
        elif "page_token" in params:
            params.pop("page_token", None)
        attempts = int(max(1, ALPACA_RETRY_ATTEMPTS))
        data: Any = None
        last_err: Exception | None = None
        for attempt in range(attempts):
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=float(timeout_s))
                status = int(resp.status_code)
                if status in (403, 422):
                    return []
                if status in (429, 500, 502, 503, 504):
                    if attempt < attempts - 1:
                        time.sleep(_retry_after_or_backoff_seconds(resp=resp, attempt=attempt, base_backoff_s=float(ALPACA_RETRY_BACKOFF_SECONDS)))
                        continue
                resp.raise_for_status()
                data = resp.json()
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
                    time.sleep(_retry_after_or_backoff_seconds(resp=getattr(e, "response", None), attempt=attempt, base_backoff_s=float(ALPACA_RETRY_BACKOFF_SECONDS)))
                    continue
                break

        if last_err is not None:
            status = None
            try:
                status = int(getattr(getattr(last_err, "response", None), "status_code", None))
            except Exception:
                status = None
            _log_tradfi_error_throttled(provider="alpaca", ticker=ticker, err=last_err, status=status)
            return out
        bars = data.get("bars") if isinstance(data, dict) else None
        if not isinstance(bars, list) or not bars:
            break
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            try:
                ts_str = str(bar.get("t") or "")
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                out.append(
                    {
                        "t": ts_ms,
                        "o": float(bar.get("o")),
                        "h": float(bar.get("h")),
                        "l": float(bar.get("l")),
                        "c": float(bar.get("c")),
                        "v": float(bar.get("v", 0.0)),
                    }
                )
            except Exception:
                continue
        nxt = data.get("next_page_token") if isinstance(data, dict) else None
        page_token = str(nxt) if nxt else None
        if not page_token:
            break
    return out


def _default_us_equity_session_utc(day: date) -> tuple[int, int] | None:
    # Fallback RTH session: weekdays only, 09:30-16:00 America/New_York.
    if int(day.weekday()) >= 5:
        return None
    open_dt = datetime(day.year, day.month, day.day, 9, 30, tzinfo=_ET_TZ).astimezone(timezone.utc)
    close_dt = datetime(day.year, day.month, day.day, 16, 0, tzinfo=_ET_TZ).astimezone(timezone.utc)
    start_ms = int(open_dt.timestamp() * 1000)
    end_ms = int(close_dt.timestamp() * 1000) - 60_000
    if end_ms < start_ms:
        return None
    return (start_ms, end_ms)


def _tradfi_earliest_marker_path(*, coin_dir: str) -> Path:
    return get_exchange_raw_root_dir("hyperliquid") / "1m_src" / str(coin_dir) / "tradfi_earliest.txt"


def _load_tradfi_earliest_marker(*, coin_dir: str) -> date | None:
    p = _tradfi_earliest_marker_path(coin_dir=coin_dir)
    if not p.exists():
        return None
    try:
        s = str(p.read_text(encoding="utf-8") or "").strip()
        if len(s) != 8 or not s.isdigit():
            return None
        return datetime.strptime(s, "%Y%m%d").date()
    except Exception:
        return None


def _save_tradfi_earliest_marker(*, coin_dir: str, day: date) -> None:
    p = _tradfi_earliest_marker_path(coin_dir=coin_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(day.strftime("%Y%m%d") + "\n", encoding="utf-8")
    os.replace(tmp, p)


def _alpaca_has_data_in_window(
    *,
    ticker: str,
    start_date: date,
    end_date: date,
    api_key: str,
    api_secret: str,
    timeout_s: float,
) -> bool | None:
    if not api_key or not api_secret:
        return None
    if end_date < start_date:
        return False

    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }
    params: dict[str, Any] = {
        "timeframe": "1Min",
        "start": datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": datetime(end_date.year, end_date.month, end_date.day, 23, 59, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": 1,
        "adjustment": "split",
        "feed": "iex",
    }
    attempts = int(max(1, ALPACA_RETRY_ATTEMPTS))
    last_err: Exception | None = None
    payload: Any = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=float(timeout_s))
            status = int(resp.status_code)
            if status in (401, 403, 422):
                _log_tradfi_error_throttled(
                    provider="alpaca",
                    ticker=ticker,
                    err=requests.exceptions.HTTPError(f"status={resp.status_code}"),
                    status=int(resp.status_code),
                )
                return None
            if status in (429, 500, 502, 503, 504):
                if attempt < attempts - 1:
                    time.sleep(_retry_after_or_backoff_seconds(resp=resp, attempt=attempt, base_backoff_s=float(ALPACA_RETRY_BACKOFF_SECONDS)))
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
            except Exception:
                status = None
            is_transient = (
                status in (429, 500, 502, 503, 504)
                or isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            )
            if is_transient and attempt < attempts - 1:
                time.sleep(_retry_after_or_backoff_seconds(resp=getattr(e, "response", None), attempt=attempt, base_backoff_s=float(ALPACA_RETRY_BACKOFF_SECONDS)))
                continue
            break

    if last_err is not None:
        status = None
        try:
            status = int(getattr(getattr(last_err, "response", None), "status_code", None))
        except Exception:
            status = None
        _log_tradfi_error_throttled(provider="alpaca", ticker=ticker, err=last_err, status=status)
        return None

    bars = payload.get("bars") if isinstance(payload, dict) else None
    if not isinstance(bars, list):
        return False
    return bool(bars)


def _discover_alpaca_earliest_day(
    *,
    ticker: str,
    end_date: date,
    api_key: str,
    api_secret: str,
    timeout_s: float,
) -> date | None:
    if not api_key or not api_secret:
        return None

    window = int(max(3, TRADFI_DISCOVERY_WINDOW_DAYS))
    initial_probe = _alpaca_has_data_in_window(
        ticker=ticker,
        start_date=max(date(1970, 1, 1), end_date - timedelta(days=window)),
        end_date=end_date,
        api_key=api_key,
        api_secret=api_secret,
        timeout_s=float(timeout_s),
    )
    if initial_probe is None:
        return None
    if not initial_probe:
        return None

    max_lookback = int(max(365, TRADFI_DISCOVERY_MAX_LOOKBACK_DAYS))
    has_bound = end_date
    no_bound: date | None = None

    step = 365
    while step <= max_lookback:
        probe_start = end_date - timedelta(days=step)
        probe_end = min(end_date, probe_start + timedelta(days=window - 1))
        has_data = _alpaca_has_data_in_window(
            ticker=ticker,
            start_date=probe_start,
            end_date=probe_end,
            api_key=api_key,
            api_secret=api_secret,
            timeout_s=float(timeout_s),
        )
        if has_data is None:
            break
        if has_data:
            has_bound = probe_start
            step *= 2
            continue
        no_bound = probe_start
        break

    if no_bound is None:
        return has_bound

    low = no_bound
    high = has_bound
    while (high - low).days > int(window):
        mid = low + timedelta(days=((high - low).days // 2))
        mid_end = min(end_date, mid + timedelta(days=window - 1))
        has_data = _alpaca_has_data_in_window(
            ticker=ticker,
            start_date=mid,
            end_date=mid_end,
            api_key=api_key,
            api_secret=api_secret,
            timeout_s=float(timeout_s),
        )
        if has_data is None:
            break
        if has_data:
            high = mid
        else:
            low = mid

    return high


def _determine_stock_perp_improve_start(
    *,
    coin_u: str,
    coin_dir: str,
    d_end: date,
    earliest_candidates: list[date],
    timeout_s: float = 30.0,
) -> date:
    if earliest_candidates:
        fallback_start = min(earliest_candidates)
    else:
        fallback_start = d_end - timedelta(days=int(TRADFI_IMPROVE_DEFAULT_LOOKBACK_DAYS))

    persisted_earliest = _load_tradfi_earliest_marker(coin_dir=coin_dir)

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

    profiles = _load_tradfi_profiles_from_ini()
    alpaca_key = str((profiles.get("alpaca") or {}).get("api_key") or "").strip()
    alpaca_secret = str((profiles.get("alpaca") or {}).get("api_secret") or "").strip()
    ticker = _tradfi_ticker_from_hyperliquid_coin(coin_u)

    # Migration rule:
    # - If SOURCE_CODE_OTHER already exists: continue from that marker and skip rediscovery.
    # - Still validate persisted marker (if present) and correct obviously stale values.
    if oldest_other_date is not None:
        if persisted_earliest is not None and ticker and alpaca_key and alpaca_secret:
            marker_end = persisted_earliest + timedelta(days=max(1, int(TRADFI_DISCOVERY_WINDOW_DAYS) - 1))
            marker_ok = _alpaca_has_data_in_window(
                ticker=ticker,
                start_date=persisted_earliest,
                end_date=min(d_end, marker_end),
                api_key=alpaca_key,
                api_secret=alpaca_secret,
                timeout_s=float(timeout_s),
            )
            if marker_ok is False:
                try:
                    _save_tradfi_earliest_marker(coin_dir=coin_dir, day=oldest_other_date)
                    append_exchange_download_log(
                        "hyperliquid",
                        f"[hl_best_1m] {coin_u} tradfi_earliest_marker_corrected={oldest_other_date.strftime('%Y%m%d')} (from stale {persisted_earliest.strftime('%Y%m%d')})",
                    )
                except Exception:
                    pass
        return oldest_other_date

    # Guard against stale/wrong persisted markers: keep only if it still returns data.
    if persisted_earliest is not None and ticker and alpaca_key and alpaca_secret:
        marker_end = persisted_earliest + timedelta(days=max(1, int(TRADFI_DISCOVERY_WINDOW_DAYS) - 1))
        marker_ok = _alpaca_has_data_in_window(
            ticker=ticker,
            start_date=persisted_earliest,
            end_date=min(d_end, marker_end),
            api_key=alpaca_key,
            api_secret=alpaca_secret,
            timeout_s=float(timeout_s),
        )
        if marker_ok is False:
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_best_1m] {coin_u} tradfi_earliest_marker_invalid={persisted_earliest.strftime('%Y%m%d')} -> rediscover",
            )
            persisted_earliest = None

    discovered_date: date | None = None
    if ticker and alpaca_key and alpaca_secret:
        discovered_date = _discover_alpaca_earliest_day(
            ticker=ticker,
            end_date=d_end,
            api_key=alpaca_key,
            api_secret=alpaca_secret,
            timeout_s=float(timeout_s),
        )
    effective_earliest: date | None = persisted_earliest
    if discovered_date is not None:
        effective_earliest = discovered_date if effective_earliest is None else min(effective_earliest, discovered_date)

    if effective_earliest is not None:
        if persisted_earliest is None or effective_earliest < persisted_earliest:
            try:
                _save_tradfi_earliest_marker(coin_dir=coin_dir, day=effective_earliest)
            except Exception:
                pass
        append_exchange_download_log(
            "hyperliquid",
            f"[hl_best_1m] {coin_u} tradfi_earliest_discovered={effective_earliest.strftime('%Y%m%d')}",
        )
        return effective_earliest

    return fallback_start


def _polygon_fetch_1m(
    *,
    ticker: str,
    start_ms: int,
    end_ms: int,
    api_key: str,
    timeout_s: float,
) -> list[dict[str, Any]]:
    if not api_key:
        return []
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{int(start_ms)}/{int(end_ms)}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }
    attempts = int(max(1, POLYGON_RETRY_ATTEMPTS))
    last_err: Exception | None = None
    data: dict[str, Any] | None = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, timeout=float(timeout_s))
            status = int(resp.status_code)
            if status in (403, 422):
                return []
            if status in (429, 500, 502, 503, 504):
                if attempt < attempts - 1:
                    retry_after = 0.0
                    try:
                        retry_after = float(str(resp.headers.get("Retry-After") or "0").strip() or 0)
                    except Exception:
                        retry_after = 0.0
                    if retry_after <= 0.0:
                        retry_after = float(POLYGON_RETRY_BACKOFF_SECONDS) * float(2 ** attempt)
                    time.sleep(max(0.0, retry_after))
                    continue
            resp.raise_for_status()
            payload = resp.json()
            data = payload if isinstance(payload, dict) else {}
            last_err = None
            break
        except Exception as e:
            last_err = e
            transient = False
            status = None
            try:
                status = int(getattr(getattr(e, "response", None), "status_code", None))
            except Exception:
                status = None
            if status in (429, 500, 502, 503, 504):
                transient = True
            if transient and attempt < attempts - 1:
                wait_s = float(POLYGON_RETRY_BACKOFF_SECONDS) * float(2 ** attempt)
                time.sleep(max(0.0, wait_s))
                continue
            break

    if last_err is not None:
        status = None
        try:
            status = int(getattr(getattr(last_err, "response", None), "status_code", None))
        except Exception:
            status = None
        _log_tradfi_error_throttled(provider="polygon", ticker=ticker, err=last_err, status=status)
        return []

    rows = data.get("results") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            ts_ms = int(row.get("t"))
            if ts_ms < start_ms or ts_ms > end_ms:
                continue
            out.append(
                {
                    "t": ts_ms,
                    "o": float(row.get("o")),
                    "h": float(row.get("h")),
                    "l": float(row.get("l")),
                    "c": float(row.get("c")),
                    "v": float(row.get("v", 0.0)),
                }
            )
        except Exception:
            continue
    return out


def _polygon_detect_access_mode(*, ticker: str, api_key: str, timeout_s: float) -> str:
    # Returns one of: "extended", "free", "unknown"
    key_id = f"{str(api_key)[:12]}:{str(ticker).upper()}"
    cached = str(_POLYGON_ACCESS_MODE_CACHE.get(key_id) or "").strip().lower()
    if cached in ("extended", "free", "unknown"):
        return cached

    probe_day = (date.today() - timedelta(days=int(POLYGON_FREE_MAX_LOOKBACK_DAYS + 30)))
    while int(probe_day.weekday()) >= 5:
        probe_day += timedelta(days=1)

    utc_start = datetime(probe_day.year, probe_day.month, probe_day.day, 14, 30, tzinfo=timezone.utc)
    utc_end = datetime(probe_day.year, probe_day.month, probe_day.day, 20, 59, tzinfo=timezone.utc)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{int(utc_start.timestamp() * 1000)}/{int(utc_end.timestamp() * 1000)}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 1,
        "apiKey": api_key,
    }
    try:
        resp = requests.get(url, params=params, timeout=float(timeout_s))
        status = int(resp.status_code)
        if status == 200:
            mode = "extended"
        elif status in (401, 403, 422):
            mode = "free"
        else:
            mode = "unknown"
    except Exception:
        mode = "unknown"

    _POLYGON_ACCESS_MODE_CACHE[key_id] = mode
    return mode


def _polygon_can_fill_day(*, day: date, ticker: str, api_key: str, timeout_s: float) -> bool:
    age_days = int((date.today() - day).days)
    if age_days <= int(POLYGON_FREE_MAX_LOOKBACK_DAYS):
        return True
    mode = _polygon_detect_access_mode(ticker=ticker, api_key=api_key, timeout_s=float(timeout_s))
    return mode == "extended"


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
) -> tuple[int, int]:
    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        raise ValueError("coin is empty")

    ticker = _tradfi_ticker_from_hyperliquid_coin(coin_u)
    if not ticker:
        return (0, 0)

    profiles = _load_tradfi_profiles_from_ini()
    alpaca_key = str((profiles.get("alpaca") or {}).get("api_key") or "").strip()
    alpaca_secret = str((profiles.get("alpaca") or {}).get("api_secret") or "").strip()
    polygon_key = str((profiles.get("polygon") or {}).get("api_key") or "").strip()

    if not (alpaca_key and alpaca_secret) and not polygon_key:
        append_exchange_download_log(
            "hyperliquid",
            f"[hl_best_1m] {coin_u} tradfi_1m SKIP no tradfi credentials in pbgui.ini [tradfi_profiles]",
        )
        return (0, 0)

    sessions = _alpaca_fetch_trading_sessions(
        start_date=start_date,
        end_date=end_date,
        api_key=alpaca_key,
        api_secret=alpaca_secret,
        timeout_s=float(timeout_s),
    ) if (alpaca_key and alpaca_secret) else {}

    alpaca_minutes_filled = 0
    polygon_minutes_filled = 0
    polygon_days: dict[str, dict[str, Any]] = {}

    for d in _iter_dates_inclusive(start_date, end_date):
        day_s = d.strftime("%Y%m%d")
        day_tag = _day_tag(day_s)

        session = sessions.get(day_s)
        if not session:
            session = _default_us_equity_session_utc(d)
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

        def _allow_write(minute_idx: int) -> bool:
            if source_codes is None:
                return True
            if minute_idx >= len(source_codes):
                return True
            return int(source_codes[minute_idx]) in (0, int(SOURCE_CODE_OTHER))

        alpaca_bars = _alpaca_fetch_1m_iex(
            ticker=ticker,
            start_ms=session_start_ms,
            end_ms=session_end_ms,
            api_key=alpaca_key,
            api_secret=alpaca_secret,
            timeout_s=float(timeout_s),
        ) if (alpaca_key and alpaca_secret) else []

        alpaca_added_idx: list[int] = []
        for bar in alpaca_bars:
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
                alpaca_added_idx.append(idx)
            except Exception:
                continue

        if alpaca_added_idx:
            _write_day_npz(day_path, existing)
            update_source_index_for_day(
                exchange="hyperliquid",
                coin=coin_dir,
                day=day_s,
                minute_indices=alpaca_added_idx,
                code=SOURCE_CODE_OTHER,
            )
            alpaca_minutes_filled += len(alpaca_added_idx)

        # Policy:
        # - Priority source is Alpaca.
        # - Polygon is used for remaining missing minutes inside the same
        #   trading session window (never outside session).
        remaining_missing = [
            mi for mi in range(start_idx, end_idx + 1)
            if mi not in existing and _allow_write(mi)
        ]
        if remaining_missing and polygon_key:
            if not _polygon_can_fill_day(day=d, ticker=ticker, api_key=polygon_key, timeout_s=float(timeout_s)):
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_best_1m] {coin_u} polygon_skip_old_day_no_extended_access day={day_s}",
                )
                if isinstance(stats_out, dict):
                    stats_out["polygon_old_days_skipped"] = int(stats_out.get("polygon_old_days_skipped") or 0) + 1
                if sleep_s:
                    time.sleep(float(sleep_s))
                continue
            polygon_days[day_s] = {
                "day": d,
                "day_tag": day_tag,
                "day_start_ms": int(day_start_ms),
                "session_start_ms": int(session_start_ms),
                "session_end_ms": int(session_end_ms),
                "missing_set": set(int(mi) for mi in remaining_missing),
                "existing": existing,
                "added_idx": [],
            }

        if sleep_s:
            time.sleep(float(sleep_s))

    if polygon_key and polygon_days:
        sessions = sorted(
            [
                (int(meta["session_start_ms"]), int(meta["session_end_ms"]), str(day_s))
                for day_s, meta in polygon_days.items()
            ],
            key=lambda x: x[0],
        )

        max_span_ms = int(max(1, int(POLYGON_MAX_RANGE_MINUTES_PER_CALL)) * 60_000)
        polygon_ranges: list[tuple[int, int]] = []
        cur_start: int | None = None
        cur_end: int | None = None
        for s_ms, e_ms, _day_s in sessions:
            if cur_start is None:
                cur_start, cur_end = int(s_ms), int(e_ms)
                continue
            candidate_end = max(int(cur_end), int(e_ms))
            if int(candidate_end) - int(cur_start) <= int(max_span_ms):
                cur_end = int(candidate_end)
            else:
                polygon_ranges.append((int(cur_start), int(cur_end)))
                cur_start, cur_end = int(s_ms), int(e_ms)
        if cur_start is not None and cur_end is not None:
            polygon_ranges.append((int(cur_start), int(cur_end)))

        for range_start_ms, range_end_ms in polygon_ranges:
            polygon_bars = _polygon_fetch_1m(
                ticker=ticker,
                start_ms=int(range_start_ms),
                end_ms=int(range_end_ms),
                api_key=polygon_key,
                timeout_s=float(timeout_s),
            )
            for bar in polygon_bars:
                if not isinstance(bar, dict):
                    continue
                try:
                    ts_ms = int(bar.get("t"))
                    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                    day_s = dt.strftime("%Y%m%d")
                    meta = polygon_days.get(day_s)
                    if not meta:
                        continue
                    day_start_ms = int(meta["day_start_ms"])
                    idx = _minute_index(ts_ms, day_start_ms)
                    missing_set = meta.get("missing_set")
                    if not isinstance(missing_set, set) or idx not in missing_set:
                        continue
                    existing = meta.get("existing")
                    if not isinstance(existing, dict):
                        continue
                    if idx in existing:
                        continue
                    existing[idx] = {
                        "t": ts_ms,
                        "o": float(bar.get("o")),
                        "h": float(bar.get("h")),
                        "l": float(bar.get("l")),
                        "c": float(bar.get("c")),
                        "v": float(bar.get("v", 0.0)),
                    }
                    added_idx = meta.get("added_idx")
                    if isinstance(added_idx, list):
                        added_idx.append(int(idx))
                except Exception:
                    continue

            if sleep_s:
                time.sleep(float(sleep_s))

        for day_s, meta in polygon_days.items():
            added_idx = meta.get("added_idx")
            if not isinstance(added_idx, list) or not added_idx:
                continue
            day_tag = str(meta.get("day_tag") or _day_tag(day_s))
            day_path = _best_day_path(coin=coin_u, day=day_tag)
            existing = meta.get("existing")
            if not isinstance(existing, dict):
                continue
            _write_day_npz(day_path, existing)
            update_source_index_for_day(
                exchange="hyperliquid",
                coin=coin_dir,
                day=day_s,
                minute_indices=sorted(set(int(x) for x in added_idx)),
                code=SOURCE_CODE_OTHER,
            )
            polygon_minutes_filled += len(set(int(x) for x in added_idx))

    return (int(alpaca_minutes_filled), int(polygon_minutes_filled))


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
    alpaca_minutes_filled: int = 0
    polygon_minutes_filled: int = 0
    polygon_old_days_skipped: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "l2book_minutes_added": int(self.l2book_minutes_added),
            "binance_minutes_filled": int(self.binance_minutes_filled),
            "bybit_minutes_filled": int(self.bybit_minutes_filled),
            "alpaca_minutes_filled": int(self.alpaca_minutes_filled),
            "polygon_minutes_filled": int(self.polygon_minutes_filled),
            "polygon_old_days_skipped": int(self.polygon_old_days_skipped),
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
    dry_run: bool = False,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> ImproveBest1mResult:
    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin_u)
    d_end = end_date
    if d_end is None:
        d_end = date.today()
    if isinstance(d_end, str):
        d_end = datetime.strptime(d_end.strip(), "%Y-%m-%d").date() if "-" in d_end else datetime.strptime(d_end.strip(), "%Y%m%d").date()

    is_stock_perp = _is_stock_perp_coin(coin_u)
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

    if is_stock_perp:
        start_day = _determine_stock_perp_improve_start(
            coin_u=coin_u,
            coin_dir=coin_dir,
            d_end=d_end,
            earliest_candidates=earliest_candidates,
            timeout_s=30.0,
        )
        days = [d for d in _iter_dates_inclusive(start_day, d_end)]
    else:
        if not earliest_candidates:
            days = []
        else:
            start_day = min(earliest_candidates)
            days = [d for d in _iter_dates_inclusive(start_day, d_end)]
    days_checked = 0
    l2book_minutes_added = 0
    binance_minutes_filled = 0
    bybit_minutes_filled = 0
    alpaca_minutes_filled = 0
    polygon_minutes_filled = 0
    polygon_old_days_skipped = 0

    if progress_cb is not None:
        try:
            progress_cb({"stage": "improve", "planned": len(days), "done": 0})
        except Exception:
            pass

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
                    progress_cb({
                        "stage": "improve",
                        "planned": len(days),
                        "done": i,
                        "day": day_s,
                        "days_checked": int(days_checked),
                        "l2book_minutes_added": int(l2book_minutes_added),
                        "binance_minutes_filled": int(binance_minutes_filled),
                        "bybit_minutes_filled": int(bybit_minutes_filled),
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
                tradfi_fill_stats = {"polygon_old_days_skipped": int(polygon_old_days_skipped)}
                alpaca_added, polygon_added = _fill_missing_from_tradfi_1m(
                    coin=coin_u,
                    start_date=d,
                    end_date=d,
                    timeout_s=30.0,
                    sleep_s=0.0,
                    stats_out=tradfi_fill_stats,
                )
                alpaca_minutes_filled += int(alpaca_added)
                polygon_minutes_filled += int(polygon_added)
                polygon_old_days_skipped = int(tradfi_fill_stats.get("polygon_old_days_skipped") or polygon_old_days_skipped)
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
                        "alpaca_minutes_filled": int(alpaca_minutes_filled),
                        "polygon_minutes_filled": int(polygon_minutes_filled),
                        "polygon_old_days_skipped": int(polygon_old_days_skipped),
                    }
                )
            except Exception:
                pass

    return ImproveBest1mResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y%m%d"),
        days_checked=int(days_checked),
        l2book_minutes_added=int(l2book_minutes_added),
        binance_minutes_filled=int(binance_minutes_filled),
        bybit_minutes_filled=int(bybit_minutes_filled),
        alpaca_minutes_filled=int(alpaca_minutes_filled),
        polygon_minutes_filled=int(polygon_minutes_filled),
        polygon_old_days_skipped=int(polygon_old_days_skipped),
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
