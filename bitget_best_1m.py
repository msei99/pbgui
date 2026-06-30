"""
bitget_best_1m.py - Bitget USDT-FUTURES 1m OHLCV downloader for PBGui.

Bitget has no public historical archive, so this module uses the public v2 REST
candle endpoints for both historical backfills and latest-day refreshes.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import numpy as np
import requests
from requests.adapters import HTTPAdapter

from market_data import append_exchange_download_log, get_exchange_raw_root_dir
from market_data_sources import SOURCE_CODE_API, update_source_index_for_day


SERVICE = "BitgetBest1m"
EXCHANGE = "bitget"
STORAGE_EXCHANGE = "bitget"

BITGET_BASE = "https://api.bitget.com"
RECENT_ENDPOINT = "/api/v2/mix/market/candles"
HISTORY_ENDPOINT = "/api/v2/mix/market/history-candles"
PRODUCT_TYPE = "USDT-FUTURES"
GRANULARITY = "1m"

REST_LIMIT = 200
REST_RATE_PER_SECOND = 18.0
REST_WORKERS = 16
REST_POOL_CONNECTIONS = 2
REST_POOL_MAXSIZE = 8
MAX_RETRIES = 8
RETRY_WAIT_BASE_S = 0.5
RETRY_WAIT_MULT = 2.0
RETRY_WAIT_MAX_S = 20.0
RATE_LIMIT_PENALTY_S = 3.0
DEFAULT_LATEST_LOOKBACK_DAYS = 3
MIN_DAY_CANDLES = 1440

MS_PER_MINUTE = 60_000
DAY_MS = 86_400_000
INCEPTION_PROBE_LOW = datetime(2018, 1, 1, tzinfo=timezone.utc)
INCEPTION_DEFAULT = date(2019, 7, 10)

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "PBGui/bitget-best-1m",
}

_NPZ_DTYPE = np.dtype([
    ("ts", "i8"),
    ("o", "f4"),
    ("h", "f4"),
    ("l", "f4"),
    ("c", "f4"),
    ("bv", "f4"),
])

_BITGET_USDT_MAP: dict[str, dict[str, str]] = {}
_BITGET_USDT_MAP_SIG: tuple[int, int] | None = None
_THREAD_LOCAL = threading.local()


@dataclass
class ImproveBest1mBitgetResult:
    """Summary returned by improve_best_bitget_1m_for_coin."""

    coin: str
    end_date: str
    days_checked: int
    rest_minutes_fetched: int
    repair_minutes_fetched: int
    minutes_written: int
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "rest_minutes_fetched": int(self.rest_minutes_fetched),
            "repair_minutes_fetched": int(self.repair_minutes_fetched),
            "minutes_written": int(self.minutes_written),
            "notes": list(self.notes),
        }


class RateLimiter:
    """Small process-local rate limiter shared by worker threads."""

    def __init__(self, rate_per_second: float) -> None:
        self.interval = 1.0 / max(0.001, float(rate_per_second))
        self.lock = Lock()
        self.next_time = 0.0

    def wait(self) -> None:
        """Block until the next request slot is available."""

        with self.lock:
            now = time.monotonic()
            if self.next_time > now:
                time.sleep(self.next_time - now)
                now = time.monotonic()
            self.next_time = max(now, self.next_time) + self.interval

    def penalize(self, seconds: float) -> None:
        """Push the shared schedule forward after a rate-limit response."""

        penalty = max(0.0, float(seconds or 0.0))
        if penalty <= 0.0:
            return
        with self.lock:
            self.next_time = max(self.next_time, time.monotonic() + penalty)


class BitgetUnavailableSymbolError(RuntimeError):
    """Raised when Bitget reports that a requested market symbol is unavailable."""


class _RetryableBitgetError(RuntimeError):
    """Internal marker for retryable Bitget HTTP/API failures."""


def _is_unavailable_symbol_error(status: int, code: str, message: str) -> bool:
    """Return whether a Bitget response means the requested symbol is unavailable."""

    text = f"{code} {message}".lower()
    return int(status) == 400 and (code == "40034" or "symbol not exists" in text or "symbol does not exist" in text)


def _day_tag(day: str | date) -> str:
    if isinstance(day, date):
        return day.strftime("%Y-%m-%d")
    s = str(day or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"invalid day: {day!r}")


def _parse_date_input(value: date | str | None, default: date) -> date:
    if value is None:
        return default
    if isinstance(value, date):
        return value
    s = str(value or "").strip()
    if not s:
        return default
    return datetime.strptime(s, "%Y-%m-%d").date() if "-" in s else datetime.strptime(s, "%Y%m%d").date()


def _day_start_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _ms_to_date(ts_ms: int) -> date:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).date()


def _safe_float(value: Any) -> float | None:
    text = str(value if value is not None else "").strip()
    if not text or text.lower() in ("none", "nan"):
        return None
    try:
        return float(text)
    except Exception:
        return None


def _get_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=REST_POOL_CONNECTIONS,
            pool_maxsize=REST_POOL_MAXSIZE,
            max_retries=0,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.session = session
    return session


def _bitget_get_json(
    path: str,
    params: dict[str, Any],
    *,
    timeout_s: float = 30.0,
    retries: int = MAX_RETRIES,
    limiter: RateLimiter | None = None,
) -> dict[str, Any]:
    """GET Bitget JSON with retry handling for public candle endpoints."""

    url = BITGET_BASE + path
    delay = RETRY_WAIT_BASE_S
    last_error: Exception | None = None
    for attempt in range(1, int(retries) + 1):
        if limiter is not None:
            limiter.wait()
        try:
            response = _get_session().get(url, params=params, headers=_HEADERS, timeout=float(timeout_s))
            status = int(response.status_code)
            if status == 429:
                if limiter is not None:
                    limiter.penalize(RATE_LIMIT_PENALTY_S)
                raise _RetryableBitgetError(f"HTTP {status}: {response.text[:300]}")
            if status >= 500:
                raise _RetryableBitgetError(f"HTTP {status}: {response.text[:300]}")
            if status == 400:
                try:
                    error_payload = response.json()
                except Exception:
                    error_payload = {}
                code = str(error_payload.get("code") or "") if isinstance(error_payload, dict) else ""
                msg = str(error_payload.get("msg") or error_payload.get("message") or response.text[:300]) if isinstance(error_payload, dict) else response.text[:300]
                if _is_unavailable_symbol_error(status, code, msg):
                    raise BitgetUnavailableSymbolError(f"Bitget code={code} msg={msg}")
                raise RuntimeError(f"HTTP {status}: {response.text[:300]}")
            response.raise_for_status()
            payload = response.json()
            code = str(payload.get("code") or "")
            if code != "00000":
                msg = str(payload.get("msg") or payload.get("message") or "")
                if _is_unavailable_symbol_error(status, code, msg):
                    raise BitgetUnavailableSymbolError(f"Bitget code={code} msg={msg}")
                if code.startswith("429") or code in ("30014", "30015"):
                    if limiter is not None:
                        limiter.penalize(RATE_LIMIT_PENALTY_S)
                    raise _RetryableBitgetError(f"Bitget code={code} msg={msg}")
                raise RuntimeError(f"Bitget code={code} msg={msg}")
            return payload
        except Exception as exc:
            last_error = exc
            retryable = isinstance(exc, _RetryableBitgetError)
            if isinstance(exc, RuntimeError) and not retryable:
                raise
            if not retryable:
                retryable = not isinstance(exc, requests.HTTPError)
            if attempt >= int(retries) or not retryable:
                break
            is_rate_limit = "429" in str(exc) or "Too Many Requests" in str(exc)
            floor = RATE_LIMIT_PENALTY_S if is_rate_limit else 0.0
            sleep_s = min(max(delay, floor), RETRY_WAIT_MAX_S)
            time.sleep(sleep_s + random.random() * min(sleep_s, 1.0))
            delay = min(delay * RETRY_WAIT_MULT, RETRY_WAIT_MAX_S)
    raise RuntimeError(f"Bitget GET failed path={path} params={params}: {last_error}")


def _load_bitget_usdt_map() -> dict[str, dict[str, str]]:
    """Return lookup aliases for active Bitget USDT-FUTURES swap records."""

    global _BITGET_USDT_MAP, _BITGET_USDT_MAP_SIG
    mapping_path = Path(__file__).resolve().parent / "data" / "coindata" / "bitget" / "mapping.json"
    sig: tuple[int, int] | None = None
    if mapping_path.exists():
        st = mapping_path.stat()
        sig = (st.st_mtime_ns, st.st_size)
    if sig == _BITGET_USDT_MAP_SIG:
        return dict(_BITGET_USDT_MAP)

    out: dict[str, dict[str, str]] = {}
    try:
        raw = json.loads(mapping_path.read_text(encoding="utf-8"))
        for rec in raw if isinstance(raw, list) else []:
            if not bool(rec.get("swap")) or not bool(rec.get("active", True)):
                continue
            if str(rec.get("quote") or "").strip().upper() != "USDT":
                continue
            symbol = str(rec.get("symbol") or "").strip().upper()
            ccxt_symbol = str(rec.get("ccxt_symbol") or "").strip().upper()
            coin = str(rec.get("coin") or "").strip().upper()
            base = str(rec.get("base") or "").strip().upper()
            if not symbol:
                continue
            if not base:
                base = symbol[:-4] if symbol.endswith("USDT") else symbol
            if not ccxt_symbol:
                ccxt_symbol = f"{base}/USDT:USDT"
            info = {"symbol": symbol, "coin": coin or base, "base": base, "ccxt_symbol": ccxt_symbol}
            for key in {coin, base, symbol, ccxt_symbol, ccxt_symbol.replace("/", "_"), f"{base}_USDT:USDT"}:
                key_s = str(key or "").strip().upper()
                if key_s:
                    out.setdefault(key_s, info)
    except Exception:
        out = {}
    _BITGET_USDT_MAP = out
    _BITGET_USDT_MAP_SIG = sig
    return dict(_BITGET_USDT_MAP)


def _raw_base_from_coin(coin: str) -> str:
    value = str(coin or "").strip().upper()
    if not value:
        return ""
    if "/" in value:
        return value.split("/", 1)[0].strip()
    if value.endswith("_USDT:USDT"):
        return value[: -len("_USDT:USDT")]
    if value.endswith("USDT") and not value.endswith("_USDT"):
        return value[:-4]
    return value.strip(" _:-")


def _resolve_info(coin: str) -> dict[str, str]:
    value = str(coin or "").strip().upper()
    mapping = _load_bitget_usdt_map()
    info = mapping.get(value)
    if info:
        return info
    base = _raw_base_from_coin(value)
    info = mapping.get(base)
    if info:
        return info
    symbol = value if value.endswith("USDT") else f"{base}USDT"
    base = symbol[:-4] if symbol.endswith("USDT") else base
    return {"symbol": symbol, "coin": base, "base": base, "ccxt_symbol": f"{base}/USDT:USDT"}


def _coin_to_bitget_symbol(coin: str) -> str:
    """Return Bitget native USDT-FUTURES symbol, e.g. BTCUSDT."""

    return _resolve_info(coin)["symbol"]


def _coin_dir(coin: str) -> str:
    """Return PB7-compatible storage directory, e.g. BTC_USDT:USDT."""

    info = _resolve_info(coin)
    ccxt_symbol = str(info.get("ccxt_symbol") or "").strip().upper()
    if ccxt_symbol:
        return ccxt_symbol.replace("/", "_")
    base = str(info.get("base") or _raw_base_from_coin(coin)).strip().upper()
    return f"{base}_USDT:USDT"


def get_storage_coin_dir(coin: str) -> str:
    """Public helper for callers that need the on-disk Bitget coin directory."""

    return _coin_dir(coin)


def _bitget_day_path(coin: str, day: str | date) -> Path:
    return get_exchange_raw_root_dir(STORAGE_EXCHANGE) / "1m" / _coin_dir(coin) / f"{_day_tag(day)}.npz"


def _list_existing_days(coin: str) -> list[date]:
    base = get_exchange_raw_root_dir(STORAGE_EXCHANGE) / "1m" / _coin_dir(coin)
    if not base.exists():
        return []
    out: list[date] = []
    for path in base.glob("*.npz"):
        try:
            out.append(datetime.strptime(path.stem, "%Y-%m-%d").date())
        except Exception:
            continue
    return sorted(out)


def _read_day_npz(path: Path, *, day: str | date) -> dict[int, dict[str, Any]]:
    """Return {minute_index: candle_dict} for an existing NPZ file."""

    out: dict[int, dict[str, Any]] = {}
    if not path.exists():
        return out
    try:
        with np.load(path) as data:
            arr = data["candles"] if "candles" in data else None
    except Exception as exc:
        try:
            bad = path.with_name(path.name + f".corrupt.{int(time.time())}")
            os.replace(path, bad)
            append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_best_1m] corrupt_npz moved={bad.name} error={type(exc).__name__}")
        except Exception:
            pass
        return out
    if arr is None or len(arr) == 0:
        return out
    day_start = _day_start_ms(datetime.strptime(_day_tag(day), "%Y-%m-%d").date())
    for row in arr:
        try:
            ts_ms = int(row["ts"])
            idx = int((ts_ms - day_start) // MS_PER_MINUTE)
            if 0 <= idx < MIN_DAY_CANDLES:
                out[idx] = {
                    "t": ts_ms,
                    "o": float(row["o"]),
                    "h": float(row["h"]),
                    "l": float(row["l"]),
                    "c": float(row["c"]),
                    "v": float(row["bv"]),
                }
        except Exception:
            continue
    return out


def _write_day_npz(path: Path, candles_by_minute: dict[int, dict[str, Any]]) -> None:
    """Write {minute_index: candle_dict} to a compressed NPZ file atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for idx in sorted(candles_by_minute.keys()):
        candle = candles_by_minute[idx]
        try:
            rows.append((
                int(candle["t"]),
                float(candle["o"]),
                float(candle["h"]),
                float(candle["l"]),
                float(candle["c"]),
                float(candle["v"]),
            ))
        except Exception:
            continue
    arr = np.array(rows, dtype=_NPZ_DTYPE)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as handle:
        np.savez_compressed(handle, candles=arr)
    os.replace(tmp, path)


def _write_candles_for_day(
    coin: str,
    day: str | date,
    candles: dict[int, dict[str, Any]],
    *,
    overwrite: bool = False,
    source_code: int = SOURCE_CODE_API,
) -> int:
    """Merge candles into one daily NPZ and update the source index."""

    day_s = _day_tag(day)
    path = _bitget_day_path(coin, day_s)
    existing: dict[int, dict[str, Any]] = {}
    if not overwrite and path.exists():
        existing = _read_day_npz(path, day=day_s)
    before_keys: set[int] = set() if overwrite else set(existing.keys())
    written_indices: list[int] = []
    for idx, candle in candles.items():
        if overwrite or idx not in before_keys:
            existing[int(idx)] = candle
            written_indices.append(int(idx))
    added = len(existing) - len(before_keys)
    if added > 0 or overwrite:
        _write_day_npz(path, existing)
        try:
            update_source_index_for_day(
                exchange=STORAGE_EXCHANGE,
                coin=_coin_dir(coin),
                day=day_s,
                minute_indices=list(existing.keys()) if overwrite else written_indices,
                code=int(source_code),
            )
        except Exception:
            pass
    return max(0, added)


def _parse_rest_row(row: list[Any]) -> dict[str, Any] | None:
    if not isinstance(row, list) or len(row) < 6:
        return None
    try:
        ts_ms = int(row[0])
    except Exception:
        return None
    return {
        "t": ts_ms,
        "o": _safe_float(row[1]) or 0.0,
        "h": _safe_float(row[2]) or 0.0,
        "l": _safe_float(row[3]) or 0.0,
        "c": _safe_float(row[4]) or 0.0,
        "v": _safe_float(row[5]) or 0.0,
    }


def _bucket_rows(
    rows: list[list[Any]],
    *,
    since_ms: int | None = None,
    end_ms: int | None = None,
) -> dict[str, dict[int, dict[str, Any]]]:
    buckets: dict[str, dict[int, dict[str, Any]]] = {}
    for row in rows:
        candle = _parse_rest_row(row)
        if not candle:
            continue
        ts_ms = int(candle["t"])
        if since_ms is not None and ts_ms < int(since_ms):
            continue
        if end_ms is not None and ts_ms >= int(end_ms):
            continue
        day_dt = _ms_to_date(ts_ms)
        day_s = day_dt.strftime("%Y-%m-%d")
        idx = int((ts_ms - _day_start_ms(day_dt)) // MS_PER_MINUTE)
        if 0 <= idx < MIN_DAY_CANDLES:
            buckets.setdefault(day_s, {})[idx] = candle
    return buckets


def _build_end_time_cursors(since_ms: int, end_ms: int, *, limit: int = REST_LIMIT) -> list[int]:
    """Build descending Bitget history-candles endTime cursors."""

    cursors: list[int] = []
    cursor = int(end_ms)
    step_ms = int(limit) * MS_PER_MINUTE
    while cursor > int(since_ms):
        cursors.append(cursor)
        cursor -= step_ms
    return cursors


def _history_params(symbol: str, *, end_ms: int, start_ms: int | None = None, limit: int = REST_LIMIT) -> dict[str, Any]:
    params: dict[str, Any] = {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "granularity": GRANULARITY,
        "limit": int(limit),
        "endTime": int(end_ms),
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    return params


def _fetch_history_page(
    symbol: str,
    end_ms: int,
    *,
    start_ms: int | None = None,
    limit: int = REST_LIMIT,
    timeout_s: float = 30.0,
    limiter: RateLimiter | None = None,
) -> list[list[Any]]:
    payload = _bitget_get_json(
        HISTORY_ENDPOINT,
        _history_params(symbol, end_ms=end_ms, start_ms=start_ms, limit=limit),
        timeout_s=timeout_s,
        limiter=limiter,
    )
    data = payload.get("data")
    return data if isinstance(data, list) else []


def _rest_fetch_range(
    coin: str,
    since_ms: int,
    end_ms: int,
    *,
    timeout_s: float = 30.0,
    limiter: RateLimiter | None = None,
    workers: int = REST_WORKERS,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> tuple[dict[str, dict[int, dict[str, Any]]], int]:
    """Fetch Bitget history candles for [since_ms, end_ms) and bucket by day."""

    if end_ms <= since_ms:
        return {}, 0
    symbol = _coin_to_bitget_symbol(coin)
    cursors = _build_end_time_cursors(since_ms, end_ms)
    buckets: dict[str, dict[int, dict[str, Any]]] = {}
    fetched_minutes = 0
    completed = 0
    total = len(cursors)
    rate_limiter = limiter or RateLimiter(REST_RATE_PER_SECOND)

    def fetch_cursor(cursor_end: int) -> list[list[Any]]:
        if stop_check and stop_check():
            return []
        return _fetch_history_page(symbol, cursor_end, timeout_s=timeout_s, limiter=rate_limiter)

    with ThreadPoolExecutor(max_workers=max(1, min(int(workers), max(1, total)))) as executor:
        futures = {executor.submit(fetch_cursor, cursor): cursor for cursor in cursors}
        for future in as_completed(futures):
            if stop_check and stop_check():
                break
            cursor = futures[future]
            rows = future.result()
            sub = _bucket_rows(rows, since_ms=since_ms, end_ms=end_ms)
            for day_s, candles in sub.items():
                day_bucket = buckets.setdefault(day_s, {})
                before = len(day_bucket)
                day_bucket.update(candles)
                fetched_minutes += max(0, len(day_bucket) - before)
            completed += 1
            if progress_cb:
                try:
                    progress_cb({"stage": "rest_fetch", "done": completed, "total_days": total, "cursor": cursor})
                except Exception:
                    pass
    return buckets, fetched_minutes


def _find_inception_ms(coin: str, *, timeout_s: float = 30.0, limiter: RateLimiter | None = None) -> int:
    """Find earliest available Bitget 1m candle timestamp for a coin."""

    symbol = _coin_to_bitget_symbol(coin)
    low_ms = int(INCEPTION_PROBE_LOW.timestamp() * 1000)
    high_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    try:
        while high_ms - low_ms > DAY_MS:
            mid_ms = ((low_ms + high_ms) // (2 * DAY_MS)) * DAY_MS
            rows = _fetch_history_page(symbol, mid_ms, limit=1, timeout_s=timeout_s, limiter=limiter)
            if rows:
                high_ms = mid_ms
            else:
                low_ms = mid_ms + DAY_MS
        buckets, _ = _rest_fetch_range(
            coin,
            max(0, low_ms - DAY_MS),
            high_ms + DAY_MS,
            timeout_s=timeout_s,
            limiter=limiter,
            workers=4,
        )
        times = [int(candle["t"]) for candles in buckets.values() for candle in candles.values()]
        if times:
            return min(times)
    except BitgetUnavailableSymbolError as exc:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_best_1m] inception_probe_unavailable coin={coin} sym={symbol} err={exc}", level="WARNING")
        raise
    except Exception as exc:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_best_1m] inception_probe_error coin={coin} sym={symbol} err={exc}", level="WARNING")
    return _day_start_ms(INCEPTION_DEFAULT)


def _validate_day_minutes(candles: dict[int, dict[str, Any]]) -> list[int]:
    return [idx for idx in range(MIN_DAY_CANDLES) if idx not in candles]


def _repair_missing_minutes_for_day(
    coin: str,
    day_s: str,
    *,
    timeout_s: float = 30.0,
    limiter: RateLimiter | None = None,
) -> tuple[dict[int, dict[str, Any]], int]:
    day_dt = datetime.strptime(_day_tag(day_s), "%Y-%m-%d").date()
    buckets, fetched = _rest_fetch_range(
        coin,
        _day_start_ms(day_dt),
        _day_start_ms(day_dt) + DAY_MS,
        timeout_s=timeout_s,
        limiter=limiter,
        workers=min(8, REST_WORKERS),
    )
    return buckets.get(_day_tag(day_s), {}), fetched


def improve_best_bitget_1m_for_coin(
    *,
    coin: str,
    end_date: date | str | None = None,
    start_date_override: date | str | None = None,
    refetch: bool = False,
    timeout_s: float = 30.0,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
    rest_limiter: RateLimiter | None = None,
) -> ImproveBest1mBitgetResult:
    """Full backfill of Bitget USDT-FUTURES 1m candles from inception."""

    coin_u = str(coin or "").strip().upper()
    d_end = _parse_date_input(end_date, date.today())
    d_start_override = _parse_date_input(start_date_override, date.min) if start_date_override else None
    limiter = rest_limiter or RateLimiter(REST_RATE_PER_SECOND)
    notes: list[str] = []
    minutes_written = 0
    rest_minutes_fetched = 0
    repair_minutes_fetched = 0

    def _emit(snap: dict[str, Any]) -> None:
        if progress_cb:
            try:
                progress_cb(snap)
            except Exception:
                pass

    def _stop() -> bool:
        return bool(stop_check and stop_check())

    _emit({"stage": "starting", "coin": coin_u})
    if _stop():
        return ImproveBest1mBitgetResult(coin_u, d_end.strftime("%Y-%m-%d"), 0, 0, 0, 0, ["stopped"])

    _emit({"stage": "finding_inception", "coin": coin_u})
    inception_ms = _find_inception_ms(coin_u, timeout_s=timeout_s, limiter=limiter)
    inception_day = _ms_to_date(inception_ms)
    d_start = max(inception_day, d_start_override) if d_start_override else inception_day
    notes.append(f"inception={inception_day.strftime('%Y-%m-%d')}")
    _emit({"stage": "inception_found", "day": inception_day.strftime("%Y-%m-%d")})

    total_planned_days = max(1, (d_end - d_start).days + 1)
    today = datetime.now(tz=timezone.utc).date()
    days_to_fetch: list[date] = []
    cur = d_start
    while cur <= d_end:
        day_s = cur.strftime("%Y-%m-%d")
        if refetch:
            days_to_fetch.append(cur)
        else:
            existing = _read_day_npz(_bitget_day_path(coin_u, day_s), day=day_s)
            if cur in (inception_day, today):
                if not existing:
                    days_to_fetch.append(cur)
            elif len(existing) < MIN_DAY_CANDLES:
                days_to_fetch.append(cur)
        cur += timedelta(days=1)

    _emit({"stage": "days_planned", "days_to_fetch": len(days_to_fetch), "total_days": total_planned_days, "coin": coin_u})
    if not days_to_fetch or _stop():
        return ImproveBest1mBitgetResult(
            coin=coin_u,
            end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=total_planned_days,
            rest_minutes_fetched=0,
            repair_minutes_fetched=0,
            minutes_written=0,
            notes=notes + (["stopped"] if _stop() else ["all_complete"]),
        )

    ranges: list[tuple[date, date]] = []
    run_start = run_end = days_to_fetch[0]
    for day in days_to_fetch[1:]:
        if day == run_end + timedelta(days=1):
            run_end = day
        else:
            ranges.append((run_start, run_end))
            run_start = run_end = day
    ranges.append((run_start, run_end))

    results_by_day: dict[str, dict[int, dict[str, Any]]] = {}
    for range_start, range_end in ranges:
        if _stop():
            notes.append("stopped")
            break
        since_ms = max(_day_start_ms(range_start), inception_ms)
        end_ms = _day_start_ms(range_end) + DAY_MS
        _emit({"stage": "rest_fetch", "day": range_start.strftime("%Y-%m-%d"), "done": 0, "total_days": total_planned_days})
        buckets, fetched = _rest_fetch_range(
            coin_u,
            since_ms,
            end_ms,
            timeout_s=timeout_s,
            limiter=limiter,
            progress_cb=progress_cb,
            stop_check=stop_check,
        )
        rest_minutes_fetched += fetched
        for day_s, candles in buckets.items():
            results_by_day.setdefault(day_s, {}).update(candles)

    days_written = 0
    for day_s in sorted(results_by_day.keys()):
        candles = results_by_day[day_s]
        if not candles:
            continue
        day_dt = datetime.strptime(day_s, "%Y-%m-%d").date()
        overwrite_day = bool(refetch or day_dt in days_to_fetch)
        w = _write_candles_for_day(coin_u, day_s, candles, overwrite=overwrite_day)
        minutes_written += w
        if w > 0:
            days_written += 1
        _emit({"stage": "writing", "day": day_s, "done": days_written, "total_days": total_planned_days, "minutes_written": minutes_written})

    cur = d_start
    while cur <= d_end:
        if _stop():
            break
        if cur in (inception_day, today):
            cur += timedelta(days=1)
            continue
        day_s = cur.strftime("%Y-%m-%d")
        existing = _read_day_npz(_bitget_day_path(coin_u, day_s), day=day_s)
        missing = _validate_day_minutes(existing)
        if missing:
            repaired, fetched = _repair_missing_minutes_for_day(coin_u, day_s, timeout_s=timeout_s, limiter=limiter)
            repair_minutes_fetched += fetched
            if repaired:
                _write_candles_for_day(coin_u, day_s, repaired, overwrite=False)
                existing = _read_day_npz(_bitget_day_path(coin_u, day_s), day=day_s)
                missing = _validate_day_minutes(existing)
            if missing:
                notes.append(f"unrepaired_minutes={day_s}:{len(missing)}")
        cur += timedelta(days=1)

    result = ImproveBest1mBitgetResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y-%m-%d"),
        days_checked=total_planned_days,
        rest_minutes_fetched=rest_minutes_fetched,
        repair_minutes_fetched=repair_minutes_fetched,
        minutes_written=minutes_written,
        notes=notes,
    )
    append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_best_1m] {coin_u} done {result.to_dict()}")
    return result


def update_latest_bitget_1m_for_coin(
    *,
    coin: str,
    lookback_days: int = DEFAULT_LATEST_LOOKBACK_DAYS,
    overwrite: bool = True,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """Refresh the latest Bitget 1m candles for one coin."""

    coin_u = str(coin or "").strip().upper()
    lb = max(1, int(lookback_days))
    now_utc = datetime.now(tz=timezone.utc)
    d_end = now_utc.date()
    d_start = d_end - timedelta(days=lb)
    since_ms = _day_start_ms(d_start)
    end_ms = int(now_utc.timestamp() * 1000) + (2 * MS_PER_MINUTE)
    symbol = _coin_to_bitget_symbol(coin_u)
    limiter = RateLimiter(REST_RATE_PER_SECOND)

    append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_latest_1m] start coin={coin_u} sym={symbol} lookback={lb}d ({d_start} -> {d_end})")
    pages = 0
    minutes_written = 0
    repair_minutes_fetched = 0
    candles_by_day: dict[str, dict[int, dict[str, Any]]] = {}

    try:
        recent_payload = _bitget_get_json(
            RECENT_ENDPOINT,
            {
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
                "granularity": GRANULARITY,
                "limit": REST_LIMIT,
            },
            timeout_s=timeout_s,
            limiter=limiter,
        )
        pages += 1
        recent_rows = recent_payload.get("data") if isinstance(recent_payload, dict) else []
        for day_s, candles in _bucket_rows(recent_rows if isinstance(recent_rows, list) else [], since_ms=since_ms, end_ms=end_ms).items():
            candles_by_day.setdefault(day_s, {}).update(candles)

        buckets, fetched = _rest_fetch_range(
            coin_u,
            since_ms,
            end_ms,
            timeout_s=timeout_s,
            limiter=limiter,
            workers=min(8, REST_WORKERS),
        )
        pages += len(_build_end_time_cursors(since_ms, end_ms))
        repair_minutes_fetched += fetched
        for day_s, candles in buckets.items():
            candles_by_day.setdefault(day_s, {}).update(candles)

        for day_s, candles in sorted(candles_by_day.items()):
            minutes_written += _write_candles_for_day(coin_u, day_s, candles, overwrite=overwrite)

        result = {
            "coin": coin_u,
            "lookback_days": lb,
            "pages": pages,
            "days_fetched": len(candles_by_day),
            "minutes_written": minutes_written,
            "repair_minutes_fetched": repair_minutes_fetched,
            "result": "ok",
        }
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_latest_1m] done coin={coin_u} {result}")
        return result
    except Exception as exc:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bitget_latest_1m] error coin={coin_u} err={exc}", level="WARNING")
        return {
            "coin": coin_u,
            "lookback_days": lb,
            "pages": pages,
            "days_fetched": len(candles_by_day),
            "minutes_written": 0,
            "repair_minutes_fetched": repair_minutes_fetched,
            "result": "error",
            "error": str(exc),
        }


def get_newest_day(coin: str) -> str | None:
    """Return newest day (YYYYMMDD) with data, or None."""

    days = _list_existing_days(coin)
    return days[-1].strftime("%Y%m%d") if days else None


def get_oldest_day(coin: str) -> str | None:
    """Return oldest day (YYYYMMDD) with data, or None."""

    days = _list_existing_days(coin)
    return days[0].strftime("%Y%m%d") if days else None
