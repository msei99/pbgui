"""
bybit_best_1m.py — Bybit linear perpetuals 1m OHLCV downloader for PBGui.

Download strategy per coin:
 1. Find inception (first date) by scraping public.bybit.com/trading/{SYMBOL}/
 2. Daily CSV.GZ files from public.bybit.com/trading/ for the full historical
    range — raw trades are aggregated on-the-fly to 1m OHLCV
 3. CCXT for the last `lookback_days` days (update_latest path)

Archive availability: typically all days up to ~1 day ago.
The archive contains raw trade data (timestamp_sec, price, size, side, ...),
NOT pre-built OHLCV candles — so aggregation is done locally.

Storage layout (identical scheme to binance_best_1m / PB7 cache):
  data/ohlcv/bybit/1m/<COIN_DIR>/YYYY-MM-DD.npz         ← OHLCV candles
  data/ohlcv/bybit/1m_src/<COIN_DIR>/sources.idx         ← minute coverage index
  COIN_DIR format: BTC_USDT:USDT, 1000SHIB_USDT:USDT    (matches PB7 cache layout)
  (same NPZ format: structured array ts/o/h/l/c/bv)

Main public API:
  update_latest_bybit_1m_for_coin(coin, lookback_days, ...)
      → keeps the last N days up-to-date via CCXT; fast daily refresh
  improve_best_bybit_1m_for_coin(coin, end_date, ...)
      → full backfill from inception via public archive; called by task_worker
"""

from __future__ import annotations

import gzip
import io
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import requests

from logging_helpers import human_log as _human_log
from market_data import (
    append_exchange_download_log,
    get_exchange_raw_root_dir,
    normalize_market_data_coin_dir,
)
from market_data_sources import SOURCE_CODE_API, update_source_index_for_day
# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXCHANGE = "bybit"          # config/UI key (enabled_coins, exchange selectbox)
STORAGE_EXCHANGE = "bybit"  # disk storage path (data/ohlcv/bybit/)
ARCHIVE_BASE = "https://public.bybit.com/trading"
# CCXT limit per request (Bybit supports up to 1000)
CCXT_LIMIT = 1000
# Days before today that the archive already has (archive lags ~1 day)
ARCHIVE_MIN_AGE_DAYS = 2
# HTTP timeouts
ARCHIVE_TIMEOUT_S = 120   # large files (BTC ~100 MB)
PROBE_TIMEOUT_S = 15
# Default lookback for latest refresh
DEFAULT_LATEST_LOOKBACK_DAYS = 3
# Directory listing cache TTL (seconds) — avoid re-scraping per-coin
_DIR_CACHE: dict[str, tuple[float, list[str]]] = {}  # symbol → (ts, dates)
_DIR_CACHE_TTL = 300.0


# ---------------------------------------------------------------------------
# Coin / symbol helpers
# ---------------------------------------------------------------------------

def _coin_base(coin: str) -> str:
    """
    Normalise a PBGui coin name to its base form (without USDT suffix).

    PBGui coins already carry the exchange-specific prefix when needed
    (e.g. the coin is stored as '1000SHIB', not 'SHIB').  So we only need
    to strip formatting artefacts.
    """
    c = str(coin or "").strip().upper()
    # Dir format BTC_USDT:USDT → BTC
    if c.endswith("_USDT:USDT"):
        return c[:-len("_USDT:USDT")]
    # CCXT format BTC/USDT:USDT → BTC
    if "/" in c:
        c = c.split("/")[0].strip()
    # Archive / plain USDT suffix → strip
    if c.endswith("USDT") and not c.endswith("_USDT"):
        return c[:-4]
    return c


def _coin_to_archive_symbol(coin: str) -> str:
    """BTC → BTCUSDT, 1000SHIB → 1000SHIBUSDT  (Bybit archive / REST symbol)."""
    return _coin_base(coin) + "USDT"


def _coin_to_ccxt_symbol(coin: str) -> str:
    """BTC → BTC/USDT:USDT  (CCXT linear perp format)."""
    return f"{_coin_base(coin)}/USDT:USDT"


def _coin_dir(coin: str) -> str:
    """Return PB7-compatible dir name: BTC → BTC_USDT:USDT, 1000SHIB → 1000SHIB_USDT:USDT."""
    return f"{_coin_base(coin)}_USDT:USDT"


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


def _bybit_day_path(coin: str, day: str) -> Path:
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
# NPZ read / write  (same dtype as binance_best_1m / hyperliquid_best_1m)
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
            append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] corrupt_npz moved={bad.name} error={type(e).__name__}")
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
    """Write {minute_index: candle_dict} to compressed NPZ atomically."""
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
# Archive helpers: public.bybit.com/trading/
# ---------------------------------------------------------------------------

def _list_archive_days(symbol: str, *, force_refresh: bool = False) -> list[str]:
    """
    Scrape available dates from public.bybit.com/trading/{SYMBOL}/.
    Returns sorted list of date strings 'YYYY-MM-DD'.
    Results are in-process cached for _DIR_CACHE_TTL seconds.
    """
    now = time.monotonic()
    if not force_refresh and symbol in _DIR_CACHE:
        ts, cached = _DIR_CACHE[symbol]
        if now - ts < _DIR_CACHE_TTL:
            return cached

    url = f"{ARCHIVE_BASE}/{symbol}/"
    try:
        r = requests.get(url, timeout=PROBE_TIMEOUT_S)
        r.raise_for_status()
    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] dir_list_error symbol={symbol} err={e}", level="WARNING")
        return []

    # Extract dates: pattern SYMBOLYYYY-MM-DD.csv.gz
    pattern = rf'{re.escape(symbol)}(\d{{4}}-\d{{2}}-\d{{2}})\.csv\.gz'
    dates = sorted(set(re.findall(pattern, r.text)))
    _DIR_CACHE[symbol] = (now, dates)
    return dates


def _stream_download_bytes(
    url: str,
    *,
    stop_check: Callable[[], bool] | None = None,
    chunk_size: int = 131_072,  # 128 KB chunks
) -> bytes | None:
    """Stream-download URL. Returns bytes or None on error / stop."""
    try:
        with requests.get(url, timeout=ARCHIVE_TIMEOUT_S, stream=True) as r:
            if r.status_code != 200:
                return None
            buf: list[bytes] = []
            for chunk in r.iter_content(chunk_size=chunk_size):
                if stop_check and stop_check():
                    return None
                if chunk:
                    buf.append(chunk)
            return b"".join(buf)
    except Exception as e:
        append_exchange_download_log(
            STORAGE_EXCHANGE,
            f"[bybit_best_1m] stream_download_error url={url} err={e}",
            level="WARNING",
        )
        return None


def _aggregate_trades_to_1m(
    raw_bytes: bytes,
    *,
    day: str,
    symbol: str,
) -> dict[int, dict[str, Any]]:
    """
    Parse a Bybit daily trade CSV.GZ and aggregate to 1m OHLCV.

    CSV columns (modern format):
      timestamp, symbol, side, size, price, tickDirection,
      trdMatchID, grossValue, homeNotional, foreignNotional, [rpi]

    timestamp is Unix seconds (float).
    Returns {minute_index: candle_dict} for the given day.
    """
    day_dt = datetime.strptime(_day_tag(day), "%Y-%m-%d").date()
    day_start_ms = _day_start_ms(day_dt)
    day_end_ms = day_start_ms + 1440 * 60_000

    try:
        with gzip.open(io.BytesIO(raw_bytes)) as f:
            df = pd.read_csv(f, usecols=["timestamp", "price", "size"])
    except Exception:
        # Fallback: read all columns, then select
        try:
            with gzip.open(io.BytesIO(raw_bytes)) as f:
                df = pd.read_csv(f)
            df.columns = [c.lower().strip() for c in df.columns]
            # Some older files may use different column names
            ts_col = next((c for c in df.columns if "timestamp" in c or c == "time"), None)
            pr_col = next((c for c in df.columns if c == "price"), None)
            sz_col = next((c for c in df.columns if c in ("size", "qty", "quantity")), None)
            if not (ts_col and pr_col and sz_col):
                append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] unknown_columns day={day} symbol={symbol} cols={list(df.columns)}", level="WARNING")
                return {}
            df = df.rename(columns={ts_col: "timestamp", pr_col: "price", sz_col: "size"})
            df = df[["timestamp", "price", "size"]]
        except Exception as e:
            append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] parse_error day={day} symbol={symbol} err={e}", level="WARNING")
            return {}

    if df.empty:
        return {}

    # Convert timestamp: seconds (float) → ms (int)
    ts_vals = df["timestamp"].values.astype("float64")
    if ts_vals.max() < 2e12:  # seconds, not ms
        ts_ms = (ts_vals * 1000).astype("int64")
    else:
        ts_ms = ts_vals.astype("int64")

    # Filter to this day only (trades at midnight boundary may leak)
    mask = (ts_ms >= day_start_ms) & (ts_ms < day_end_ms)
    ts_ms = ts_ms[mask]
    price = df["price"].values.astype("float64")[mask]
    size = df["size"].values.astype("float64")[mask]

    if len(ts_ms) == 0:
        return {}

    # Minute bucket (index 0..1439)
    bucket_idx = (ts_ms - day_start_ms) // 60_000

    out: dict[int, dict[str, Any]] = {}
    for idx in np.unique(bucket_idx):
        m = bucket_idx == idx
        p = price[m]
        s = size[m]
        bucket_ms = int(day_start_ms + idx * 60_000)
        out[int(idx)] = {
            "t": bucket_ms,
            "o": float(p[0]),
            "h": float(p.max()),
            "l": float(p.min()),
            "c": float(p[-1]),
            "v": float(s.sum()),
        }
    return out


def _download_archive_day(
    symbol: str,
    day: str,
    *,
    stop_check: Callable[[], bool] | None = None,
) -> dict[int, dict[str, Any]] | None:
    """Download one daily trade file and return {minute_idx: candle}, or None on error."""
    url = f"{ARCHIVE_BASE}/{symbol}/{symbol}{day}.csv.gz"
    raw = _stream_download_bytes(url, stop_check=stop_check)
    if raw is None:
        return None
    try:
        return _aggregate_trades_to_1m(raw, day=day, symbol=symbol)
    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] aggregate_error day={day} symbol={symbol} err={e}", level="WARNING")
        return None


# ---------------------------------------------------------------------------
# CCXT helpers (synchronous)
# ---------------------------------------------------------------------------

def _get_ccxt_exchange(timeout_s: float = 30.0):
    """Create and return a sync ccxt bybit (linear perps) instance."""
    import ccxt  # type: ignore
    ex = ccxt.bybit({
        "enableRateLimit": True,
        "timeout": int(timeout_s * 1000),
    })
    return ex


def _ccxt_fetch_range(
    coin: str,
    since_ms: int,
    end_ms: int,
    *,
    timeout_s: float = 30.0,
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, dict[int, dict[str, Any]]]:
    """Fetch 1m candles in [since_ms, end_ms) via CCXT. Returns {day_tag: {minute_idx: candle}}."""
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
            append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] ccxt_fetch_error coin={coin} since={cursor} err={e}", level="WARNING")
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
# Core: write candles for one day (merge with existing)
# ---------------------------------------------------------------------------

def _write_candles_for_day(
    coin: str,
    day: str,
    candles: dict[int, dict[str, Any]],
    *,
    overwrite: bool = False,
) -> int:
    """Merge candles into existing NPZ (or create new). Returns number of new candles written."""
    path = _bybit_day_path(coin, day)
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
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ImproveBest1mBybitResult:
    coin: str
    end_date: str
    days_checked: int
    archive_days_downloaded: int
    ccxt_minutes_fetched: int
    minutes_written: int
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "archive_days_downloaded": int(self.archive_days_downloaded),
            "ccxt_minutes_fetched": int(self.ccxt_minutes_fetched),
            "minutes_written": int(self.minutes_written),
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# Main backfill: improve_best_bybit_1m_for_coin
# ---------------------------------------------------------------------------

def improve_best_bybit_1m_for_coin(
    *,
    coin: str,
    end_date: date | str | None = None,
    start_date_override: date | str | None = None,
    refetch: bool = False,
    timeout_s: float = 30.0,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> ImproveBest1mBybitResult:
    """
    Full backfill of Bybit linear perp 1m data from inception to end_date.

    Strategy:
      1. Scrape public.bybit.com/trading/{SYMBOL}/ for available dates
      2. For each missing day in range: download daily CSV.GZ → aggregate → save NPZ
      3. CCXT for the last 2 days (archive lag)

    Archive coverage: BTCUSDT from 2020-03-25, ETHUSDT from 2020-10-21, etc.
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
    archive_days_downloaded = 0
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

    # --- Step 1: Get available dates from archive directory listing ---
    _emit({"stage": "listing_archive", "coin": coin_u, "symbol": symbol_code})
    archive_dates = _list_archive_days(symbol_code)

    if not archive_dates:
        notes.append("archive_unavailable")
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] {coin_u} archive not found for symbol={symbol_code}")
        # Fall back to CCXT only
        archive_dates = []

    if archive_dates:
        inception_dt = datetime.strptime(archive_dates[0], "%Y-%m-%d").date()
        archive_set = set(archive_dates)
        notes.append(f"inception={archive_dates[0]}")
        _emit({"stage": "inception_found", "day": archive_dates[0], "archive_days": len(archive_dates)})
    else:
        # No archive → estimate inception date via CCXT
        inception_dt = date(2020, 1, 1)
        archive_set = set()

    # Apply start_date_override (must not go before inception)
    d_start = max(inception_dt, d_start_override) if d_start_override else inception_dt
    total_planned_days = max(1, (d_end - d_start).days + 1)

    if _stop():
        return ImproveBest1mBybitResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=days_checked, archive_days_downloaded=archive_days_downloaded,
            ccxt_minutes_fetched=ccxt_minutes_fetched, minutes_written=minutes_written,
            notes=["stopped"],
        )

    # Cutoff: archive is available up to ~ARCHIVE_MIN_AGE_DAYS ago
    today = date.today()
    archive_cutoff = today - timedelta(days=ARCHIVE_MIN_AGE_DAYS)

    # --- Step 2: Download missing days from archive ---
    cur = d_start
    while cur <= min(d_end, archive_cutoff) and not _stop():
        day_s = cur.strftime("%Y-%m-%d")

        # Skip if day not available in archive yet
        if archive_set and day_s not in archive_set:
            cur = cur + timedelta(days=1)
            days_checked += 1
            continue

        path = _bybit_day_path(coin_u, day_s)
        if not refetch and path.exists():
            existing = _read_day_npz(path, day=day_s)
            # Accept day as complete if ≥ 1380 candles (95% of 1440) — some low-volume
            # coins may genuinely have gaps
            if len(existing) >= 1380:
                days_checked += 1
                cur = cur + timedelta(days=1)
                continue

        _emit({"stage": "archive_download", "day": day_s, "done": days_checked, "total_days": total_planned_days})
        candles = _download_archive_day(symbol_code, day_s, stop_check=_stop)

        if candles is None:
            # Error already logged by _download_archive_day
            notes.append(f"download_failed={day_s}")
        elif len(candles) == 0:
            notes.append(f"empty_day={day_s}")
            append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] empty day={day_s} symbol={symbol_code}", level="WARNING")
        else:
            w = _write_candles_for_day(coin_u, day_s, candles, overwrite=refetch)
            minutes_written += w
            archive_days_downloaded += 1

        days_checked += 1
        _emit({
            "stage": "archive_download",
            "day": day_s,
            "done": days_checked,
            "total_days": total_planned_days,
            "minutes_written": minutes_written,
        })
        cur = cur + timedelta(days=1)

    if _stop():
        return ImproveBest1mBybitResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=days_checked, archive_days_downloaded=archive_days_downloaded,
            ccxt_minutes_fetched=ccxt_minutes_fetched, minutes_written=minutes_written,
            notes=notes + ["stopped"],
        )

    # --- Step 3: CCXT for recent days not yet in archive ---
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
            _emit({
                "stage": "ccxt_recent",
                "day": day_s,
                "done": days_checked,
                "total_days": total_planned_days,
                "minutes_written": minutes_written,
            })

    result = ImproveBest1mBybitResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y-%m-%d"),
        days_checked=days_checked,
        archive_days_downloaded=archive_days_downloaded,
        ccxt_minutes_fetched=ccxt_minutes_fetched,
        minutes_written=minutes_written,
        notes=notes,
    )
    append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_best_1m] {coin_u} done {result.to_dict()}")
    return result


# ---------------------------------------------------------------------------
# Latest refresh: update_latest_bybit_1m_for_coin
# Called by PBData background loop (or directly by task_worker latest path)
# ---------------------------------------------------------------------------

def update_latest_bybit_1m_for_coin(
    *,
    coin: str,
    lookback_days: int = DEFAULT_LATEST_LOOKBACK_DAYS,
    overwrite: bool = True,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """
    Refresh the last `lookback_days` days for a Bybit coin via CCXT.
    Fast daily refresh — no archive download needed.
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
        f"[bybit_latest_1m] start coin={coin_u} sym={sym} lookback={lb}d ({d_start} → {d_end})",
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
                append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_latest_1m] fetch_error coin={coin_u} err={e}", level="WARNING")
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

        for day_s, candles in candles_by_day.items():
            d_ = datetime.strptime(day_s, "%Y-%m-%d").date()
            do_overwrite = overwrite and (d_end - d_).days <= lb
            w = _write_candles_for_day(coin_u, day_s, candles, overwrite=do_overwrite)
            minutes_written += w

    except Exception as e:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[bybit_latest_1m] error coin={coin_u} err={e}", level="WARNING")
        return {
            "coin": coin_u,
            "lookback_days": lb,
            "result": "error",
            "error": str(e),
            "minutes_written": 0,
        }

    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[bybit_latest_1m] done coin={coin_u} pages={pages} days={len(candles_by_day)} min_written={minutes_written}",
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
# Convenience helpers
# ---------------------------------------------------------------------------

def get_newest_day(coin: str) -> str | None:
    """Return newest day (YYYYMMDD) with data, or None."""
    days = _list_existing_days(coin)
    return days[-1].strftime("%Y%m%d") if days else None


def get_oldest_day(coin: str) -> str | None:
    """Return oldest day (YYYYMMDD) with data, or None."""
    days = _list_existing_days(coin)
    return days[0].strftime("%Y%m%d") if days else None
