"""
binance_best_1m.py — Binance USDM perpetuals 1m OHLCV downloader for PBGui.

Download strategy per coin:
 1. Find inception (first CCXT candle) using REST API
 2. Find first available archive month on data.binance.vision
 3. CCXT pagination for the pre-archive gap (inception → first archive month)
 4. Monthly ZIPs for complete historical months
 5. Daily ZIPs for the current/most-recent incomplete month
 6. CCXT for the last `lookback_days` days (update_latest path)

Storage layout:
  data/ohlcv/binanceusdm/1m/<COIN_DIR>/YYYY-MM-DD.npz         ← OHLCV candles
  data/ohlcv/binanceusdm/1m_src/<COIN_DIR>/sources.idx        ← minute coverage index
  COIN_DIR format: BTC_USDT:USDT, 1000SHIB_USDT:USDT  (matches PB7 cache layout in caches/binanceusdm/; 1000x-prefix coins use prefixed dirname)
  (same NPZ format as hyperliquid_best_1m: structured array ts/o/h/l/c/bv)

Main public API:
  update_latest_binance_1m_for_coin(coin, lookback_days, ...)
      → keeps the last N days up-to-date; called by PBData._binance_latest_1m_loop
  improve_best_binance_1m_for_coin(coin, end_date, ...)
      → full backfill from inception; called by task_worker for job type "binance_best_1m"
"""

from __future__ import annotations

import asyncio
import calendar
import io
import os
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import requests

from logging_helpers import human_log as _human_log
from market_data import (
    append_exchange_download_log,
    get_exchange_raw_root_dir,
    normalize_market_data_coin_dir,
)
from market_data_sources import SOURCE_CODE_API, update_source_index_for_day
from PBCoinData import get_symbol_for_coin as _get_binance_symbol

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXCHANGE = "binance"  # config/UI key (enabled_coins, exchange selectbox)
STORAGE_EXCHANGE = "binanceusdm"  # disk storage path (data/ohlcv/binanceusdm/) — matches PB7 cache layout
ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
# Binance USDM 1m CCXT limit per request
CCXT_LIMIT = 1000
# Minimum gap (days) before we switch from CCXT to archive.  Archive is only
# available for data older than ~2 days anyway.
ARCHIVE_MIN_AGE_DAYS = 2
# Parallel ZIP downloads from data.binance.vision (no server-side throttling observed)
MAX_CONCURRENT_DOWNLOADS = 8
# Timeout for archive HTTP requests
ARCHIVE_TIMEOUT_S = 60
# Connect timeout for HEAD probes
PROBE_TIMEOUT_S = 10
# Default lookback for latest refresh
DEFAULT_LATEST_LOOKBACK_DAYS = 3


# ---------------------------------------------------------------------------
# Coin / symbol helpers
# ---------------------------------------------------------------------------

def _coin_to_archive_symbol(coin: str) -> str:
    """BTC → BTCUSDT, SHIB → 1000SHIBUSDT (via data/coindata/binance/mapping.json)."""
    c = str(coin or "").strip().upper()
    if c.endswith("USDT"):
        return c
    return _get_binance_symbol(c, "binance.swap")


def _coin_to_ccxt_symbol(coin: str) -> str:
    """BTC → BTC/USDT:USDT, SHIB → 1000SHIB/USDT:USDT (via data/coindata/binance/mapping.json)."""
    archive = _coin_to_archive_symbol(coin)
    # Strip trailing USDT → base, then format as CCXT perp symbol
    base = archive[:-4] if archive.endswith("USDT") else archive
    return f"{base}/USDT:USDT"


def _coin_dir(coin: str) -> str:
    """Return the directory name matching PB7 cache layout: BTC → BTC_USDT:USDT, SHIB → 1000SHIB_USDT:USDT."""
    c = str(coin or "").strip().upper()
    # Already in target format (e.g. 1000SHIB_USDT:USDT)
    if c.endswith("_USDT:USDT"):
        return c
    # CCXT format 1000SHIB/USDT:USDT → 1000SHIB
    if "/" in c:
        c = c.split("/")[0].strip()
    # Archive format 1000SHIBUSDT → strip USDT suffix
    if c.endswith("USDT") and not c.endswith("_USDT"):
        c = c[:-4]
    # At this point c is the raw coin name (e.g. SHIB, BONK) — apply prefix via mapping
    base = _get_binance_symbol(c, "binance.swap")  # → 1000SHIBUSDT
    if base.endswith("USDT"):
        base = base[:-4]  # → 1000SHIB
    return f"{base}_USDT:USDT"


# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------

def _day_tag(day: str) -> str:
    s = str(day or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"invalid day: {day!r}")


def _day_start_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _binance_day_path(coin: str, day: str) -> Path:
    base = get_exchange_raw_root_dir(STORAGE_EXCHANGE)
    cdir = _coin_dir(coin)
    return base / "1m" / cdir / f"{_day_tag(day)}.npz"


def _list_existing_days(coin: str) -> list[date]:
    base = get_exchange_raw_root_dir(STORAGE_EXCHANGE) / "1m" / _coin_dir(coin)
    if not base.exists():
        return []
    out: list[date] = []
    for p in base.glob("*.npz"):
        try:
            out.append(datetime.strptime(p.stem, "%Y-%m-%d").date())
        except Exception:
            pass
    return sorted(out)


# ---------------------------------------------------------------------------
# NPZ read / write  (same dtype as hyperliquid_best_1m for compatibility)
# ---------------------------------------------------------------------------

_NPZ_DTYPE = np.dtype([
    ("ts", "i8"),
    ("o",  "f4"),
    ("h",  "f4"),
    ("l",  "f4"),
    ("c",  "f4"),
    ("bv", "f4"),
])


def _read_day_npz(path: Path, *, day: str) -> dict[int, dict[str, Any]]:
    """Return {minute_index: candle_dict} for an existing NPZ file."""
    out: dict[int, dict[str, Any]] = {}
    if not path.exists():
        return out
    try:
        with np.load(path) as data:
            arr = data["candles"] if "candles" in data else None
    except Exception as e:
        try:
            bad = path.with_name(path.name + f".corrupt.{int(time.time())}")
            os.replace(path, bad)
            append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] corrupt_npz moved={bad.name} error={type(e).__name__}")
        except Exception:
            pass
        return out
    if arr is None or len(arr) == 0:
        return out
    day_start = _day_start_ms(datetime.strptime(_day_tag(day), "%Y-%m-%d").date())
    for row in arr:
        try:
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
            continue
    return out


def _write_day_npz(path: Path, candles_by_minute: dict[int, dict[str, Any]]) -> None:
    """Write {minute_index: candle_dict} to a compressed NPZ file atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for idx in sorted(candles_by_minute.keys()):
        c = candles_by_minute[idx]
        try:
            rows.append((
                int(c["t"]),
                float(c["o"]),
                float(c["h"]),
                float(c["l"]),
                float(c["c"]),
                float(c["v"]),
            ))
        except Exception:
            continue
    arr = np.array(rows, dtype=_NPZ_DTYPE)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        np.savez_compressed(f, candles=arr)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# CCXT helpers (synchronous — uses ccxt, not ccxt.pro)
# ---------------------------------------------------------------------------

def _get_ccxt_exchange(timeout_s: float = 30.0):
    """Create and return a sync ccxt binanceusdm (USDM perps) instance."""
    import ccxt  # type: ignore
    ex = ccxt.binanceusdm({
        "enableRateLimit": True,
        "timeout": int(timeout_s * 1000),
    })
    return ex


def _ccxt_find_inception(coin: str, timeout_s: float = 30.0) -> int | None:
    """Return the timestamp (ms) of the very first 1m candle for this coin."""
    ex = _get_ccxt_exchange(timeout_s)
    sym = _coin_to_ccxt_symbol(coin)
    since_ms = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    try:
        page = ex.fetch_ohlcv(sym, timeframe="1m", since=since_ms, limit=3)
        if page:
            return int(page[0][0])
    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] ccxt_inception_error coin={coin} err={e}", level="WARNING")
    return None


def _ccxt_fetch_range(
    coin: str,
    since_ms: int,
    end_ms: int,
    *,
    timeout_s: float = 30.0,
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, dict[int, dict[str, Any]]]:
    """Fetch 1m candles in [since_ms, end_ms) via CCXT, return {day_tag: {minute_idx: candle}}."""
    ex = _get_ccxt_exchange(timeout_s)
    sym = _coin_to_ccxt_symbol(coin)
    result: dict[str, dict[int, dict[str, Any]]] = {}
    cursor = since_ms
    pages = 0

    while cursor < end_ms:
        if stop_check and stop_check():
            break
        try:
            page = ex.fetch_ohlcv(sym, timeframe="1m", since=cursor, limit=CCXT_LIMIT)
        except Exception as e:
            append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] ccxt_fetch_error coin={coin} since={cursor} err={e}", level="WARNING")
            time.sleep(2.0)
            continue
        if not page:
            break
        added = 0
        for row in page:
            ts_ms = int(row[0])
            if ts_ms >= end_ms:
                break
            day_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
            day_s = day_dt.strftime("%Y-%m-%d")
            day_start = _day_start_ms(day_dt)
            idx = int((ts_ms - day_start) // 60_000)
            if idx < 0 or idx >= 1440:
                continue
            result.setdefault(day_s, {})[idx] = {
                "t": ts_ms,
                "o": float(row[1]),
                "h": float(row[2]),
                "l": float(row[3]),
                "c": float(row[4]),
                "v": float(row[5]),
            }
            cursor = ts_ms + 60_000
            added += 1
        pages += 1
        if added == 0:
            break
        if progress_cb:
            try:
                progress_cb({"stage": "ccxt", "day": list(result.keys())[-1] if result else "", "pages": pages})
            except Exception:
                pass

    return result


# ---------------------------------------------------------------------------
# Archive helpers (data.binance.vision)
# ---------------------------------------------------------------------------

def _archive_url_monthly(symbol_code: str, year: int, month: int) -> str:
    m = f"{year}-{month:02d}"
    return f"{ARCHIVE_BASE}/monthly/klines/{symbol_code}/1m/{symbol_code}-1m-{m}.zip"


def _archive_url_daily(symbol_code: str, day: str) -> str:
    return f"{ARCHIVE_BASE}/daily/klines/{symbol_code}/1m/{symbol_code}-1m-{day}.zip"


def _probe_archive_month(symbol_code: str, year: int, month: int) -> bool:
    url = _archive_url_monthly(symbol_code, year, month)
    try:
        r = requests.head(url, timeout=PROBE_TIMEOUT_S)
        return r.status_code == 200
    except Exception:
        return False


def _find_first_archive_month(
    symbol_code: str,
    *,
    start_year: int = 2019,
    start_month: int = 1,
) -> tuple[int, int] | None:
    """Linear search for earliest available monthly archive, starting from start_year/start_month."""
    today = date.today()
    for year in range(start_year, today.year + 1):
        m0 = start_month if year == start_year else 1
        for month in range(m0, 13):
            if year == today.year and month >= today.month:
                break
            if _probe_archive_month(symbol_code, year, month):
                return (year, month)
        else:
            continue
        break
    return None


def _parse_zip_csv(data: bytes) -> dict[int, dict[str, Any]]:
    """Parse a Binance OHLCV CSV from a ZIP and return {minute_index: candle}."""
    zf = zipfile.ZipFile(io.BytesIO(data))
    csv_bytes = zf.read(zf.namelist()[0]).decode()
    result: dict[int, dict[str, Any]] = {}
    for line in csv_bytes.strip().split("\n"):
        if not line or line.startswith("open_time"):
            continue
        parts = line.split(",")
        if len(parts) < 6:
            continue
        try:
            ts_ms = int(parts[0])
        except Exception:
            continue
        day_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
        day_start = _day_start_ms(day_dt)
        idx = int((ts_ms - day_start) // 60_000)
        if idx < 0 or idx >= 1440:
            continue
        try:
            result[idx] = {
                "t": ts_ms,
                "o": float(parts[1]),
                "h": float(parts[2]),
                "l": float(parts[3]),
                "c": float(parts[4]),
                "v": float(parts[5]),
            }
        except Exception:
            continue
    return result


def _stream_download_bytes(
    url: str,
    *,
    stop_check: Callable[[], bool] | None = None,
    chunk_size: int = 65536,
) -> bytes | None:
    """Download URL with streaming so stop_check is honoured between chunks.

    Returns the full content as bytes, or None on HTTP error / stop / exception.
    """
    try:
        with requests.get(url, timeout=ARCHIVE_TIMEOUT_S, stream=True) as r:
            if r.status_code != 200:
                return None
            buf: list[bytes] = []
            for chunk in r.iter_content(chunk_size=chunk_size):
                if stop_check and stop_check():
                    return None  # cancelled mid-download
                if chunk:
                    buf.append(chunk)
            return b"".join(buf)
    except Exception as e:
        append_exchange_download_log(
            STORAGE_EXCHANGE,
            f"[binance_best_1m] stream_download_error url={url} err={e}",
            level="WARNING",
        )
        return None


def _parse_archive_monthly_bytes(
    raw_data: bytes,
    symbol_code: str,
    year: int,
    month: int,
) -> dict[str, dict[int, dict[str, Any]]] | None:
    """Parse raw monthly ZIP bytes → {day_tag: {minute_idx: candle}}."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw_data))
        csv_bytes = zf.read(zf.namelist()[0]).decode()
        result: dict[str, dict[int, dict[str, Any]]] = {}
        for line in csv_bytes.strip().split("\n"):
            if not line or line.startswith("open_time"):
                continue
            parts = line.split(",")
            if len(parts) < 6:
                continue
            try:
                ts_ms = int(parts[0])
            except Exception:
                continue
            day_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
            day_s = day_dt.strftime("%Y-%m-%d")
            day_start = _day_start_ms(day_dt)
            idx = int((ts_ms - day_start) // 60_000)
            if idx < 0 or idx >= 1440:
                continue
            try:
                result.setdefault(day_s, {})[idx] = {
                    "t": ts_ms,
                    "o": float(parts[1]),
                    "h": float(parts[2]),
                    "l": float(parts[3]),
                    "c": float(parts[4]),
                    "v": float(parts[5]),
                }
            except Exception:
                continue
        return result
    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] archive_monthly_parse_error {symbol_code} {year}-{month:02d} err={e}", level="WARNING")
        return None


def _download_archive_monthly(
    symbol_code: str,
    year: int,
    month: int,
    *,
    stop_check: Callable[[], bool] | None = None,
) -> dict[str, dict[int, dict[str, Any]]] | None:
    """Download a monthly ZIP and return {day_tag: {minute_idx: candle}}."""
    url = _archive_url_monthly(symbol_code, year, month)
    raw_data = _stream_download_bytes(url, stop_check=stop_check)
    if raw_data is None:
        return None
    return _parse_archive_monthly_bytes(raw_data, symbol_code, year, month)


def _download_archive_daily(symbol_code: str, day: str, *, stop_check: Callable[[], bool] | None = None) -> dict[int, dict[str, Any]] | None:
    """Download a daily ZIP and return {minute_idx: candle}."""
    url = _archive_url_daily(symbol_code, day)
    raw_data = _stream_download_bytes(url, stop_check=stop_check)
    if raw_data is None:
        return None
    try:
        return _parse_zip_csv(raw_data)
    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] archive_daily_parse_error {symbol_code} {day} err={e}", level="WARNING")
        return None


async def _async_download_bytes_bulk(
    urls: list[str],
    *,
    concurrency: int = MAX_CONCURRENT_DOWNLOADS,
) -> dict[str, bytes | None]:
    """Download multiple URLs in parallel using aiohttp. Returns {url: bytes_or_None}.
    Falls back to sequential requests if aiohttp is unavailable.
    """
    try:
        import aiohttp  # type: ignore
    except ImportError:
        return {url: _stream_download_bytes(url) for url in urls}

    results: dict[str, bytes | None] = {}
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=120)

    async def fetch(session: aiohttp.ClientSession, url: str) -> None:
        async with sem:
            try:
                async with session.get(url, timeout=timeout) as r:
                    if r.status != 200:
                        results[url] = None
                    else:
                        results[url] = await r.read()
            except Exception as e:
                append_exchange_download_log(
                    STORAGE_EXCHANGE,
                    f"[binance_best_1m] async_download_error url={url} err={e}",
                    level="WARNING",
                )
                results[url] = None

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*(fetch(session, u) for u in urls))
    return results


# ---------------------------------------------------------------------------
# Core: write candles for one day (merge with existing)
# ---------------------------------------------------------------------------

def _write_candles_for_day(
    coin: str,
    day: str,
    candles: dict[int, dict[str, Any]],
    *,
    overwrite: bool = False,
) -> int:
    """Merge candles into existing NPZ (or create new). Returns number written."""
    path = _binance_day_path(coin, day)
    existing: dict[int, dict[str, Any]] = {}
    if not overwrite and path.exists():
        existing = _read_day_npz(path, day=day)
    before_keys: set[int] = set() if overwrite else set(existing.keys())
    for idx, c in candles.items():
        if overwrite or idx not in before_keys:
            existing[idx] = c
    added = len(existing) - len(before_keys)
    if added > 0 or overwrite:
        _write_day_npz(path, existing)
        try:
            written = list(existing.keys()) if overwrite else [i for i in candles if i not in before_keys]
            update_source_index_for_day(
                exchange=STORAGE_EXCHANGE,
                coin=_coin_dir(coin),
                day=day,
                minute_indices=written,
                code=SOURCE_CODE_API,
            )
        except Exception:
            pass
    return max(0, added)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ImproveBest1mBinanceResult:
    coin: str
    end_date: str
    days_checked: int
    archive_monthly_downloaded: int
    archive_daily_downloaded: int
    ccxt_minutes_fetched: int
    minutes_written: int
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "archive_monthly_downloaded": int(self.archive_monthly_downloaded),
            "archive_daily_downloaded": int(self.archive_daily_downloaded),
            "ccxt_minutes_fetched": int(self.ccxt_minutes_fetched),
            "minutes_written": int(self.minutes_written),
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# Main backfill: improve_best_binance_1m_for_coin
# ---------------------------------------------------------------------------

def improve_best_binance_1m_for_coin(
    *,
    coin: str,
    end_date: date | str | None = None,
    start_date_override: date | str | None = None,
    refetch: bool = False,
    timeout_s: float = 30.0,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> ImproveBest1mBinanceResult:
    """
    Full backfill of Binance USDM 1m data from inception to end_date.

    Strategy:
      1. Find inception via CCXT (first candle timestamp)
      2. Find first available archive month on data.binance.vision
      3. CCXT pagination for pre-archive gap (inception → first archive month)
      4. Monthly ZIPs for complete past months
      5. Daily ZIPs for the current/most-recent incomplete month
      6. CCXT for the last 2 days (no archive yet)
    """
    coin_u = str(coin or "").strip().upper()
    symbol_code = _coin_to_archive_symbol(coin_u)

    # Resolve end_date
    if end_date is None:
        d_end = date.today()
    elif isinstance(end_date, str):
        s = end_date.strip()
        d_end = datetime.strptime(s, "%Y-%m-%d").date() if "-" in s else datetime.strptime(s, "%Y%m%d").date()
    else:
        d_end = end_date

    # Resolve start_date_override
    d_start_override: date | None = None
    if start_date_override:
        s = str(start_date_override).strip()
        try:
            d_start_override = datetime.strptime(s, "%Y-%m-%d").date() if "-" in s else datetime.strptime(s, "%Y%m%d").date()
        except Exception:
            pass

    notes: list[str] = []
    archive_monthly_downloaded = 0
    archive_daily_downloaded = 0
    ccxt_minutes_fetched = 0
    minutes_written = 0
    days_checked = 0
    total_planned_days = 0

    def _emit(snap: dict[str, Any]) -> None:
        if progress_cb:
            try:
                if total_planned_days > 0:
                    snap["total_days"] = total_planned_days
                progress_cb(snap)
            except Exception:
                pass

    def _stop() -> bool:
        return bool(stop_check and stop_check())

    _emit({"stage": "starting", "coin": coin_u})

    # --- Step 1: Find inception ---
    _emit({"stage": "finding_inception", "coin": coin_u})
    inception_ms = _ccxt_find_inception(coin_u, timeout_s=timeout_s)
    if inception_ms is None:
        notes.append("inception_not_found")
        append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] {coin_u} inception not found, skipping")
        return ImproveBest1mBinanceResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=0, archive_monthly_downloaded=0, archive_daily_downloaded=0,
            ccxt_minutes_fetched=0, minutes_written=0, notes=notes,
        )
    inception_dt = datetime.fromtimestamp(inception_ms / 1000, tz=timezone.utc).date()
    _emit({"stage": "inception_found", "day": inception_dt.strftime("%Y-%m-%d")})

    # Apply start_date_override: don't go before inception
    d_start = max(inception_dt, d_start_override) if d_start_override else inception_dt
    total_planned_days = max(1, (d_end - d_start).days + 1)

    if _stop():
        return ImproveBest1mBinanceResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=days_checked, archive_monthly_downloaded=archive_monthly_downloaded,
            archive_daily_downloaded=archive_daily_downloaded,
            ccxt_minutes_fetched=ccxt_minutes_fetched, minutes_written=minutes_written, notes=["stopped"],
        )

    # --- Step 2: Find first archive month — start from inception month to avoid probing years of missing months ---
    _emit({"stage": "probing_archive", "coin": coin_u})
    first_archive = _find_first_archive_month(
        symbol_code,
        start_year=inception_dt.year,
        start_month=inception_dt.month,
    )
    if first_archive:
        first_archive_date = date(first_archive[0], first_archive[1], 1)
        notes.append(f"first_archive={first_archive[0]}-{first_archive[1]:02d}")
    else:
        first_archive_date = None
        notes.append("no_archive_found")

    _emit({"stage": "archive_probed", "first_archive": f"{first_archive[0]}-{first_archive[1]:02d}" if first_archive else "none"})

    # --- Step 3: CCXT gap fill (inception → first archive month) ---
    if first_archive_date and d_start < first_archive_date and not _stop():
        gap_end = first_archive_date
        since_ms = int(datetime(d_start.year, d_start.month, d_start.day, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime(gap_end.year, gap_end.month, gap_end.day, tzinfo=timezone.utc).timestamp() * 1000)

        _emit({"stage": "ccxt_gap", "day": d_start.strftime("%Y-%m-%d"),
               "planned": int((end_ms - since_ms) // 60_000)})

        ccxt_days = _ccxt_fetch_range(
            coin_u, since_ms, end_ms,
            timeout_s=timeout_s, stop_check=stop_check,
            progress_cb=progress_cb,
        )
        for day_s, candles in ccxt_days.items():
            # Skip days already on disk unless refetch
            path = _binance_day_path(coin_u, day_s)
            if not refetch and path.exists() and len(_read_day_npz(path, day=day_s)) >= len(candles):
                days_checked += 1
                continue
            w = _write_candles_for_day(coin_u, day_s, candles, overwrite=refetch)
            minutes_written += w
            ccxt_minutes_fetched += len(candles)
            days_checked += 1
            _emit({"stage": "ccxt_gap", "day": day_s, "done": days_checked, "minutes_written": minutes_written})

    if _stop():
        return ImproveBest1mBinanceResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=days_checked, archive_monthly_downloaded=archive_monthly_downloaded,
            archive_daily_downloaded=archive_daily_downloaded,
            ccxt_minutes_fetched=ccxt_minutes_fetched, minutes_written=minutes_written, notes=notes + ["stopped"],
        )

    # Determine archive range: first_archive up to (but not including) current month
    today = date.today()
    archive_end_month = (today.year, today.month)  # exclusive

    # --- Step 4: Monthly ZIPs for complete past months (parallel download) ---
    if first_archive_date and not _stop():
        # Phase 4a: Pre-scan — decide which months to skip vs download
        _scan_year = max(d_start.year, first_archive_date.year)
        _scan_month = first_archive_date.month if _scan_year == first_archive_date.year else 1
        if _scan_year == d_start.year:
            _scan_month = max(_scan_month, d_start.month)

        months_order: list[tuple[int, int]] = []
        months_skip: set[tuple[int, int]] = set()
        months_skip_counts: dict[tuple[int, int], int] = {}
        ex_days = set(_list_existing_days(coin_u)) if not refetch else set()

        while (_scan_year, _scan_month) < archive_end_month:
            ym = (_scan_year, _scan_month)
            months_order.append(ym)
            if not refetch:
                ms = date(_scan_year, _scan_month, 1)
                nd = calendar.monthrange(_scan_year, _scan_month)[1]
                needed = {ms + timedelta(days=i) for i in range(nd) if ms + timedelta(days=i) <= d_end}
                if needed and needed.issubset(ex_days):
                    all_full = all(
                        (p := _binance_day_path(coin_u, d_.strftime("%Y-%m-%d"))).exists()
                        and len(_read_day_npz(p, day=d_.strftime("%Y-%m-%d"))) >= 1200
                        for d_ in needed
                    )
                    if all_full:
                        months_skip.add(ym)
                        months_skip_counts[ym] = len(needed)
            _scan_month += 1
            if _scan_month > 12:
                _scan_month = 1
                _scan_year += 1

        # Phase 4b: Parallel download all non-skipped months
        months_to_fetch = [ym for ym in months_order if ym not in months_skip]
        month_urls: dict[tuple[int, int], str] = {
            ym: _archive_url_monthly(symbol_code, ym[0], ym[1]) for ym in months_to_fetch
        }
        if months_to_fetch and not _stop():
            _emit({"stage": "monthly_downloading", "total": len(months_to_fetch), "coin": coin_u})
            url_to_bytes: dict[str, bytes | None] = asyncio.run(
                _async_download_bytes_bulk([month_urls[ym] for ym in months_to_fetch])
            )
        else:
            url_to_bytes = {}

        # Phase 4c: Process in month order
        for (year, month) in months_order:
            if _stop():
                break
            mk = f"{year}-{month:02d}"
            ym = (year, month)

            if ym in months_skip:
                days_checked += months_skip_counts.get(ym, 0)
                _emit({"stage": "monthly_skip", "month_key": mk, "done": days_checked})
                continue

            _emit({"stage": "monthly_download", "month_key": mk, "done": days_checked})
            raw = url_to_bytes.get(month_urls.get(ym))
            month_data = _parse_archive_monthly_bytes(raw, symbol_code, year, month) if raw else None

            if month_data is None:
                notes.append(f"monthly_download_failed={mk}")
                append_exchange_download_log(
                    STORAGE_EXCHANGE,
                    f"[binance_best_1m] monthly_zip_unavailable={mk} coin={coin_u}, falling back to daily ZIPs",
                    level="WARNING",
                )
                # Fallback: parallel daily ZIPs for each day in the failed month
                num_days_fb = calendar.monthrange(year, month)[1]
                monthly_cutoff = date.today() - timedelta(days=ARCHIVE_MIN_AGE_DAYS)
                fb_days_needed: list[str] = []
                fb_days_skipped = 0
                for fb_i in range(num_days_fb):
                    fb_day = date(year, month, fb_i + 1)
                    if fb_day > d_end or fb_day > monthly_cutoff:
                        continue
                    fb_day_s = fb_day.strftime("%Y-%m-%d")
                    fb_path = _binance_day_path(coin_u, fb_day_s)
                    if not refetch and fb_path.exists():
                        existing_fb = _read_day_npz(fb_path, day=fb_day_s)
                        if len(existing_fb) >= 1200:
                            fb_days_skipped += 1
                            continue
                    fb_days_needed.append(fb_day_s)
                days_checked += fb_days_skipped

                fb_urls: dict[str, str] = {d: _archive_url_daily(symbol_code, d) for d in fb_days_needed}
                if fb_days_needed and not _stop():
                    fb_bytes: dict[str, bytes | None] = asyncio.run(
                        _async_download_bytes_bulk(list(fb_urls.values()))
                    )
                else:
                    fb_bytes = {}

                for fb_day_s in fb_days_needed:
                    if _stop():
                        break
                    _emit({"stage": "daily_fallback", "day": fb_day_s, "done": days_checked})
                    fb_raw = fb_bytes.get(fb_urls.get(fb_day_s))
                    if fb_raw is not None:
                        try:
                            fb_candles: dict[int, dict[str, Any]] | None = _parse_zip_csv(fb_raw)
                        except Exception as _e:
                            append_exchange_download_log(
                                STORAGE_EXCHANGE,
                                f"[binance_best_1m] daily_fallback_parse_error {symbol_code} {fb_day_s} err={_e}",
                                level="WARNING",
                            )
                            fb_candles = None
                    else:
                        fb_candles = None
                    if fb_candles:
                        w = _write_candles_for_day(coin_u, fb_day_s, fb_candles, overwrite=refetch)
                        minutes_written += w
                        archive_daily_downloaded += 1
                    days_checked += 1
                    _emit({"stage": "daily_fallback", "day": fb_day_s, "done": days_checked, "minutes_written": minutes_written})
            else:
                archive_monthly_downloaded += 1
                month_days = sorted(month_data.keys())
                for i, day_s in enumerate(month_days):
                    if _stop():
                        break
                    d_day = datetime.strptime(day_s, "%Y-%m-%d").date()
                    if d_day > d_end:
                        continue
                    w = _write_candles_for_day(coin_u, day_s, month_data[day_s], overwrite=refetch)
                    minutes_written += w
                    days_checked += 1
                    _emit({
                        "stage": "monthly_download",
                        "month_key": mk,
                        "day": day_s,
                        "month_day_index": i + 1,
                        "month_day_total": len(month_days),
                        "done": days_checked,
                        "minutes_written": minutes_written,
                    })

                # Binance monthly ZIPs sometimes omit the last few days of the month
                # (archive created before month-end data was available).
                # Fill any expected days missing from month_data via daily ZIPs.
                if not _stop():
                    num_days_in_month = calendar.monthrange(year, month)[1]
                    month_start_d = date(year, month, 1)
                    partial_missing: list[str] = []
                    for _mi in range(num_days_in_month):
                        _md = month_start_d + timedelta(days=_mi)
                        if _md < d_start or _md > d_end:
                            continue
                        _md_s = _md.strftime("%Y-%m-%d")
                        if _md_s not in month_data:
                            partial_missing.append(_md_s)
                    if partial_missing:
                        append_exchange_download_log(
                            STORAGE_EXCHANGE,
                            f"[binance_best_1m] monthly_archive_incomplete={mk} coin={coin_u}"
                            f" missing={len(partial_missing)} days; fetching via daily ZIPs",
                            level="INFO",
                        )
                        pm_urls = {d: _archive_url_daily(symbol_code, d) for d in partial_missing}
                        pm_bytes: dict[str, bytes | None] = asyncio.run(
                            _async_download_bytes_bulk(list(pm_urls.values()))
                        )
                        for pm_day_s in partial_missing:
                            if _stop():
                                break
                            _emit({"stage": "monthly_partial_fill", "month_key": mk, "day": pm_day_s, "done": days_checked})
                            pm_raw = pm_bytes.get(pm_urls[pm_day_s])
                            pm_candles: dict[int, dict[str, Any]] | None = None
                            if pm_raw is not None:
                                try:
                                    pm_candles = _parse_zip_csv(pm_raw)
                                except Exception as _e:
                                    append_exchange_download_log(
                                        STORAGE_EXCHANGE,
                                        f"[binance_best_1m] monthly_partial_fill_parse_error"
                                        f" {symbol_code} {pm_day_s} err={_e}",
                                        level="WARNING",
                                    )
                            if pm_candles:
                                w = _write_candles_for_day(coin_u, pm_day_s, pm_candles, overwrite=refetch)
                                minutes_written += w
                                archive_daily_downloaded += 1
                            days_checked += 1
                            _emit({
                                "stage": "monthly_partial_fill",
                                "month_key": mk,
                                "day": pm_day_s,
                                "done": days_checked,
                                "minutes_written": minutes_written,
                            })

    if _stop():
        return ImproveBest1mBinanceResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=days_checked, archive_monthly_downloaded=archive_monthly_downloaded,
            archive_daily_downloaded=archive_daily_downloaded,
            ccxt_minutes_fetched=ccxt_minutes_fetched, minutes_written=minutes_written, notes=notes + ["stopped"],
        )

    # --- Step 5: Daily ZIPs for current month (up to 2 days ago, parallel download) ---
    archive_cutoff = today - timedelta(days=ARCHIVE_MIN_AGE_DAYS)
    cur_month_start = date(today.year, today.month, 1)

    # Pre-scan: collect days that need download vs can be skipped
    step5_days_needed: list[str] = []
    step5_days_skip: set[str] = set()
    _d5 = cur_month_start
    while _d5 <= min(d_end, archive_cutoff):
        _day_s5 = _d5.strftime("%Y-%m-%d")
        _path5 = _binance_day_path(coin_u, _day_s5)
        if not refetch and _path5.exists() and len(_read_day_npz(_path5, day=_day_s5)) >= 1200:
            step5_days_skip.add(_day_s5)
        else:
            step5_days_needed.append(_day_s5)
        _d5 = _d5 + timedelta(days=1)

    # Parallel bulk download
    step5_urls: dict[str, str] = {d: _archive_url_daily(symbol_code, d) for d in step5_days_needed}
    if step5_days_needed and not _stop():
        step5_bytes: dict[str, bytes | None] = asyncio.run(
            _async_download_bytes_bulk(list(step5_urls.values()))
        )
    else:
        step5_bytes = {}

    # Process in order
    _d5 = cur_month_start
    while _d5 <= min(d_end, archive_cutoff) and not _stop():
        day_s = _d5.strftime("%Y-%m-%d")
        if day_s in step5_days_skip:
            days_checked += 1
            _d5 = _d5 + timedelta(days=1)
            continue
        _emit({"stage": "daily_download", "day": day_s})
        raw5 = step5_bytes.get(step5_urls.get(day_s))
        if raw5 is not None:
            try:
                candles: dict[int, dict[str, Any]] | None = _parse_zip_csv(raw5)
            except Exception as _e:
                append_exchange_download_log(
                    STORAGE_EXCHANGE,
                    f"[binance_best_1m] daily_parse_error {symbol_code} {day_s} err={_e}",
                    level="WARNING",
                )
                candles = None
        else:
            candles = None
        if candles:
            w = _write_candles_for_day(coin_u, day_s, candles, overwrite=refetch)
            minutes_written += w
            archive_daily_downloaded += 1
        days_checked += 1
        _emit({"stage": "daily_download", "day": day_s, "done": days_checked, "minutes_written": minutes_written})
        _d5 = _d5 + timedelta(days=1)

    # --- Step 6: CCXT for last 2 days (no archive yet) ---
    ccxt_start = archive_cutoff + timedelta(days=1)
    if ccxt_start <= d_end and not _stop():
        since_ms = int(datetime(ccxt_start.year, ccxt_start.month, ccxt_start.day, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime(d_end.year, d_end.month, d_end.day, tzinfo=timezone.utc).timestamp() * 1000) + 86_400_000

        _emit({"stage": "ccxt_recent", "day": ccxt_start.strftime("%Y-%m-%d")})
        recent_days = _ccxt_fetch_range(
            coin_u, since_ms, end_ms,
            timeout_s=timeout_s, stop_check=stop_check,
            progress_cb=progress_cb,
        )
        for day_s, candles in recent_days.items():
            w = _write_candles_for_day(coin_u, day_s, candles, overwrite=True)
            minutes_written += w
            ccxt_minutes_fetched += len(candles)
            days_checked += 1
            _emit({"stage": "ccxt_recent", "day": day_s, "done": days_checked, "minutes_written": minutes_written})

    result = ImproveBest1mBinanceResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y-%m-%d"),
        days_checked=days_checked,
        archive_monthly_downloaded=archive_monthly_downloaded,
        archive_daily_downloaded=archive_daily_downloaded,
        ccxt_minutes_fetched=ccxt_minutes_fetched,
        minutes_written=minutes_written,
        notes=notes,
    )
    append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_best_1m] {coin_u} done {result.to_dict()}")
    return result


# ---------------------------------------------------------------------------
# Latest refresh: update_latest_binance_1m_for_coin
# Called by PBData._binance_latest_1m_loop via asyncio.to_thread
# ---------------------------------------------------------------------------

def update_latest_binance_1m_for_coin(
    *,
    coin: str,
    lookback_days: int = DEFAULT_LATEST_LOOKBACK_DAYS,
    overwrite: bool = True,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """
    Refresh the last `lookback_days` days for a Binance USDM coin via CCXT.
    Fast and simple: no archive, just CCXT REST fetch.
    """
    coin_u = str(coin or "").strip().upper()
    lb = max(1, int(lookback_days))

    now_utc = datetime.now(tz=timezone.utc)
    d_end = now_utc.date()
    d_start = d_end - timedelta(days=lb)

    since_ms = int(datetime(d_start.year, d_start.month, d_start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(now_utc.timestamp() * 1000) + 120_000  # +2 min buffer

    ex = _get_ccxt_exchange(timeout_s)
    sym = _coin_to_ccxt_symbol(coin_u)
    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[binance_latest_1m] start coin={coin_u} sym={sym} lookback={lb}d ({d_start} \u2192 {d_end})",
    )
    cursor = since_ms
    minutes_written = 0
    pages = 0
    candles_by_day: dict[str, dict[int, dict[str, Any]]] = {}

    try:
        while cursor < end_ms:
            try:
                page = ex.fetch_ohlcv(sym, timeframe="1m", since=cursor, limit=CCXT_LIMIT)
            except Exception as e:
                append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_latest_1m] fetch_error coin={coin_u} err={e}", level="WARNING")
                break
            if not page:
                break
            added = 0
            for row in page:
                ts_ms = int(row[0])
                if ts_ms >= end_ms:
                    break
                day_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
                day_s = day_dt.strftime("%Y-%m-%d")
                day_start = _day_start_ms(day_dt)
                idx = int((ts_ms - day_start) // 60_000)
                if idx < 0 or idx >= 1440:
                    continue
                candles_by_day.setdefault(day_s, {})[idx] = {
                    "t": ts_ms,
                    "o": float(row[1]),
                    "h": float(row[2]),
                    "l": float(row[3]),
                    "c": float(row[4]),
                    "v": float(row[5]),
                }
                cursor = ts_ms + 60_000
                added += 1
            pages += 1
            if added == 0:
                break

        # Write all days (overwrite for latest, merge for older)
        for day_s, candles in candles_by_day.items():
            d_ = datetime.strptime(day_s, "%Y-%m-%d").date()
            do_overwrite = overwrite and (d_end - d_).days <= lb
            w = _write_candles_for_day(coin_u, day_s, candles, overwrite=do_overwrite)
            minutes_written += w

    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[binance_latest_1m] error coin={coin_u} err={e}", level="WARNING")
        return {
            "coin": coin_u,
            "lookback_days": lb,
            "result": "error",
            "error": str(e),
            "minutes_written": 0,
        }

    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[binance_latest_1m] done coin={coin_u} pages={pages} days={len(candles_by_day)} min_written={minutes_written}",
    )
    return {
        "coin": coin_u,
        "lookback_days": lb,
        "pages": pages,
        "days_fetched": len(candles_by_day),
        "minutes_written": minutes_written,
        "result": "ok",
    }


# ---------------------------------------------------------------------------
# Convenience: get newest available day for a coin
# ---------------------------------------------------------------------------

def get_newest_day(coin: str) -> str | None:
    """Return newest day (YYYYMMDD) with data, or None."""
    days = _list_existing_days(coin)
    if not days:
        return None
    return days[-1].strftime("%Y%m%d")


def get_oldest_day(coin: str) -> str | None:
    """Return oldest day (YYYYMMDD) with data, or None."""
    days = _list_existing_days(coin)
    if not days:
        return None
    return days[0].strftime("%Y%m%d")
