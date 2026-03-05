"""
bybit_best_1m.py — Bybit linear perpetuals 1m OHLCV downloader for PBGui.

Download strategy per coin:
 1. Find inception (first available date) via a single CCXT probe request
 2. Parallel async CCXT download of all missing days — the entire history is
    split into 1000-candle chunks and fetched concurrently (up to
    MAX_CONCURRENT requests in flight).  Bybit's public kline endpoint has
    no meaningful rate-limit for unauthenticated bulk-history requests —
    tested to >37 req/s without errors.
 3. Rate-limit / network errors are retried automatically with exponential
    back-off.

Compared to the old archive-based approach:
  Old: download raw trade CSV.GZ per day (~100 MB/day for BTC) → aggregate locally → gaps
  New: fetch pre-built OHLCV via CCXT → 1440/1440 minutes, zero-volume fill → ~13× faster

Storage layout (identical to binance_best_1m / PB7 cache):
  data/ohlcv/bybit/1m/<COIN_DIR>/YYYY-MM-DD.npz         <- OHLCV candles
  data/ohlcv/bybit/1m_src/<COIN_DIR>/sources.idx         <- minute coverage index
  COIN_DIR format: BTC_USDT:USDT, 1000SHIB_USDT:USDT    (matches PB7 cache layout)
  (same NPZ format: structured array ts/o/h/l/c/bv)

Main public API:
  update_latest_bybit_1m_for_coin(coin, lookback_days, ...)
      -> keeps the last N days up-to-date via CCXT; fast daily refresh
  improve_best_bybit_1m_for_coin(coin, end_date, ...)
      -> full backfill from inception; parallel async download
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np

from market_data import (
    append_exchange_download_log,
    get_exchange_raw_root_dir,
)
from market_data_sources import SOURCE_CODE_API, update_source_index_for_day

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXCHANGE = "bybit"
STORAGE_EXCHANGE = "bybit"

# Bybit supports up to 1000 candles per kline request
CCXT_LIMIT = 1000

# Max parallel CCXT requests in flight (sweet spot: ~60 req/s throughput)
MAX_CONCURRENT = 20

# Retry config for rate-limit / transient errors
MAX_RETRIES = 5
RETRY_WAIT_BASE_S = 5.0

# A day is considered "complete" if it has at least this many candles
MIN_DAY_CANDLES = 1380  # 95% of 1440

# Default lookback for latest refresh
DEFAULT_LATEST_LOOKBACK_DAYS = 3


# ---------------------------------------------------------------------------
# Coin / symbol helpers
# ---------------------------------------------------------------------------

_BYBIT_USDT_MAP: dict[str, str] = {}
_BYBIT_USDT_MAP_SIG: tuple[int, int] | None = None


def _get_bybit_usdt_symbol(coin: str) -> str:
    """Look up the Bybit USDT-linear perpetual symbol for *coin* from mapping.json.

    Filters entries to ``quote == "USDT"`` and ``swap == True`` so that
    USDC-quoted perps are excluded.  Falls back to ``{coin}USDT`` if not found.

    Examples: BTC -> BTCUSDT, BONK -> 1000BONKUSDT, BABYDOGE -> 1000000BABYDOGEUSDT
    """
    global _BYBIT_USDT_MAP, _BYBIT_USDT_MAP_SIG
    coin_key = str(coin or "").strip().upper()

    mapping_path = Path.cwd() / "data" / "coindata" / "bybit" / "mapping.json"
    sig: tuple[int, int] | None = None
    if mapping_path.exists():
        st = mapping_path.stat()
        sig = (st.st_mtime_ns, st.st_size)

    if sig != _BYBIT_USDT_MAP_SIG:
        new_map: dict[str, str] = {}
        try:
            import json as _json
            with open(mapping_path, encoding="utf-8") as fh:
                raw = _json.load(fh)
            for rec in raw if isinstance(raw, list) else []:
                if not bool(rec.get("swap")):
                    continue
                if str(rec.get("quote") or "").strip().upper() != "USDT":
                    continue
                sym = str(rec.get("symbol") or "").strip().upper()
                c = str(rec.get("coin") or "").strip().upper()
                if sym and c and c not in new_map:
                    new_map[c] = sym
        except Exception:
            pass
        _BYBIT_USDT_MAP = new_map
        _BYBIT_USDT_MAP_SIG = sig

    if coin_key in _BYBIT_USDT_MAP:
        return _BYBIT_USDT_MAP[coin_key]
    return f"{coin_key}USDT"


def _coin_to_ccxt_symbol(coin: str) -> str:
    """BTC -> BTC/USDT:USDT, BONK -> 1000BONK/USDT:USDT (via mapping.json)."""
    sym = _get_bybit_usdt_symbol(coin)       # -> 1000BONKUSDT
    base = sym[:-4] if sym.endswith("USDT") else sym
    return f"{base}/USDT:USDT"


def _coin_dir(coin: str) -> str:
    """Return the directory name matching PB7 cache layout.

    BTC -> BTC_USDT:USDT, BONK -> 1000BONK_USDT:USDT
    """
    c = str(coin or "").strip().upper()
    if c.endswith("_USDT:USDT"):
        return c
    if "/" in c:
        c = c.split("/")[0].strip()
    if c.endswith("USDT") and not c.endswith("_USDT"):
        c = c[:-4]
    base = _get_bybit_usdt_symbol(c)   # -> 1000BONKUSDT
    if base.endswith("USDT"):
        base = base[:-4]               # -> 1000BONK
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
            append_exchange_download_log(
                STORAGE_EXCHANGE,
                f"[bybit_best_1m] corrupt_npz moved={bad.name} error={type(e).__name__}",
            )
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
# Inception detection
# ---------------------------------------------------------------------------

def _find_inception_date(coin: str, *, timeout_s: float = 30.0) -> date:
    """Return the earliest available 1m candle date for *coin* via CCXT.

    Sends a single request with ``since`` = 2019-01-01 and reads the first
    returned timestamp.  Falls back to 2020-01-01 on error.
    """
    import ccxt  # type: ignore
    ex = ccxt.bybit({"enableRateLimit": False, "timeout": int(timeout_s * 1000)})
    sym = _coin_to_ccxt_symbol(coin)
    probe_ms = int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    try:
        page = ex.fetch_ohlcv(sym, "1m", since=probe_ms, limit=1)
        if page:
            inception_ms = int(page[0][0])
            return datetime.fromtimestamp(inception_ms / 1000, tz=timezone.utc).date()
    except Exception as e:
        append_exchange_download_log(
            STORAGE_EXCHANGE,
            f"[bybit_best_1m] inception_probe_error coin={coin} sym={sym} err={e}",
            level="WARNING",
        )
    return date(2020, 1, 1)


# ---------------------------------------------------------------------------
# Async CCXT helpers (parallel download engine)
# ---------------------------------------------------------------------------

def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if the exception looks like a Bybit rate-limit response."""
    err = str(exc)
    return any(x in err for x in ("10006", "Too many visits", "429", "rate limit", "access too frequent"))


async def _fetch_chunk_async(
    ex: Any,
    sem: asyncio.Semaphore,
    sym: str,
    chunk_start_ms: int,
    *,
    coin: str = "",
) -> list[list]:
    """Fetch up to CCXT_LIMIT 1m candles starting at chunk_start_ms.

    Retries up to MAX_RETRIES times on rate-limit errors with exponential
    back-off.  Returns raw OHLCV rows or [] on unrecoverable error.
    """
    async with sem:
        for attempt in range(MAX_RETRIES):
            try:
                page = await ex.fetch_ohlcv(sym, "1m", since=chunk_start_ms, limit=CCXT_LIMIT)
                return page or []
            except Exception as e:
                if _is_rate_limit_error(e) and attempt < MAX_RETRIES - 1:
                    wait = RETRY_WAIT_BASE_S * (attempt + 1)
                    append_exchange_download_log(
                        STORAGE_EXCHANGE,
                        f"[bybit_best_1m] rate_limit coin={coin} attempt={attempt + 1}/{MAX_RETRIES}"
                        f" wait={wait:.0f}s err={e}",
                        level="WARNING",
                    )
                    await asyncio.sleep(wait)
                    continue
                # Non-rate-limit error or exhausted retries
                append_exchange_download_log(
                    STORAGE_EXCHANGE,
                    f"[bybit_best_1m] fetch_error coin={coin} since={chunk_start_ms} err={e}",
                    level="WARNING",
                )
                return []
        return []


async def _async_backfill(
    coin: str,
    chunks: list[int],
    end_ms: int,
    *,
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, dict[int, dict[str, Any]]]:
    """Fetch all chunks in parallel and return assembled {day_s: {idx: candle}}.

    ``chunks`` is a list of chunk_start_ms values.  Each chunk fetches up to
    CCXT_LIMIT candles starting from that timestamp.  Results past ``end_ms``
    are discarded.
    """
    import ccxt.async_support as ccxt_async  # type: ignore

    ex = ccxt_async.bybit({"enableRateLimit": False, "timeout": 30_000})
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    sym = _coin_to_ccxt_symbol(coin)

    results_by_day: dict[str, dict[int, dict[str, Any]]] = {}
    completed_count = [0]
    total = len(chunks)

    async def fetch_and_store(chunk_start: int) -> None:
        if stop_check and stop_check():
            return
        rows = await _fetch_chunk_async(ex, sem, sym, chunk_start, coin=coin)
        for row in rows:
            ts_ms = int(row[0])
            if ts_ms >= end_ms:
                continue
            day_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
            day_s = day_dt.strftime("%Y-%m-%d")
            day_start = _day_start_ms(day_dt)
            idx = int((ts_ms - day_start) // 60_000)
            if 0 <= idx < 1440:
                results_by_day.setdefault(day_s, {})[idx] = {
                    "t": ts_ms,
                    "o": float(row[1]),
                    "h": float(row[2]),
                    "l": float(row[3]),
                    "c": float(row[4]),
                    "v": float(row[5]),
                }
        completed_count[0] += 1
        if progress_cb:
            last_day = ""
            if rows:
                try:
                    last_day = datetime.fromtimestamp(
                        int(rows[-1][0]) / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                except Exception:
                    pass
            try:
                progress_cb({
                    "stage": "ccxt_download",
                    "done": completed_count[0],
                    "total_days": total,
                    "day": last_day,
                })
            except Exception:
                pass

    await asyncio.gather(*[fetch_and_store(cs) for cs in chunks])
    await ex.close()
    return results_by_day


def _build_chunk_list(days: list[date]) -> list[int]:
    """Convert a sorted list of dates into CCXT chunk start timestamps.

    Consecutive days are merged into runs so that chunk boundaries span
    day boundaries naturally (chunks are 1000 minutes = ~16.7 h).
    """
    if not days:
        return []

    # Group into consecutive runs
    runs: list[tuple[int, int]] = []
    run_start = run_end = days[0]
    for d in days[1:]:
        if d == run_end + timedelta(days=1):
            run_end = d
        else:
            runs.append((_day_start_ms(run_start), _day_start_ms(run_end) + 86_400_000))
            run_start = run_end = d
    runs.append((_day_start_ms(run_start), _day_start_ms(run_end) + 86_400_000))

    chunks: list[int] = []
    for s_ms, e_ms in runs:
        c = s_ms
        while c < e_ms:
            chunks.append(c)
            c += CCXT_LIMIT * 60_000
    return chunks


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
    ccxt_days_fetched: int
    minutes_written: int
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "ccxt_days_fetched": int(self.ccxt_days_fetched),
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
    """Full backfill of Bybit linear perp 1m data from inception to end_date.

    Strategy:
      1. Probe CCXT for inception date (one request)
      2. Scan existing NPZ files; collect incomplete/missing days
      3. Build chunk list from those days and fetch all in parallel (async)
      4. Write results to disk day by day

    Bybit CCXT returns 1440/1440 minutes per day (zero-volume for quiet
    intervals) -- no gap-filling needed.
    """
    coin_u = str(coin or "").strip().upper()

    # --- Resolve end_date ---
    if end_date is None:
        d_end = date.today()
    elif isinstance(end_date, str):
        s = end_date.strip()
        d_end = datetime.strptime(s, "%Y-%m-%d").date() if "-" in s else datetime.strptime(s, "%Y%m%d").date()
    else:
        d_end = end_date

    # --- Resolve start_date_override ---
    d_start_override: date | None = None
    if start_date_override:
        s = str(start_date_override).strip()
        try:
            d_start_override = (
                datetime.strptime(s, "%Y-%m-%d").date() if "-" in s
                else datetime.strptime(s, "%Y%m%d").date()
            )
        except Exception:
            pass

    notes: list[str] = []
    minutes_written = 0

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
        return ImproveBest1mBybitResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=0, ccxt_days_fetched=0, minutes_written=0,
            notes=["stopped"],
        )

    # --- Step 1: Inception ---
    _emit({"stage": "finding_inception", "coin": coin_u})
    inception_dt = _find_inception_date(coin_u, timeout_s=timeout_s)
    d_start = max(inception_dt, d_start_override) if d_start_override else inception_dt
    notes.append(f"inception={inception_dt.strftime('%Y-%m-%d')}")
    _emit({"stage": "inception_found", "day": inception_dt.strftime("%Y-%m-%d")})

    total_planned_days = max(1, (d_end - d_start).days + 1)

    # --- Step 2: Collect incomplete / missing days ---
    days_to_fetch: list[date] = []
    cur = d_start
    while cur <= d_end:
        day_s = cur.strftime("%Y-%m-%d")
        if refetch:
            days_to_fetch.append(cur)
        else:
            path = _bybit_day_path(coin_u, day_s)
            if not path.exists():
                days_to_fetch.append(cur)
            else:
                existing = _read_day_npz(path, day=day_s)
                if len(existing) < MIN_DAY_CANDLES:
                    days_to_fetch.append(cur)
        cur += timedelta(days=1)

    _emit({
        "stage": "days_planned",
        "days_to_fetch": len(days_to_fetch),
        "total_days": total_planned_days,
        "coin": coin_u,
    })

    if not days_to_fetch or _stop():
        return ImproveBest1mBybitResult(
            coin=coin_u, end_date=d_end.strftime("%Y-%m-%d"),
            days_checked=total_planned_days, ccxt_days_fetched=0, minutes_written=0,
            notes=notes + (["stopped"] if _stop() else ["all_complete"]),
        )

    # --- Step 3: Build chunks and fetch in parallel ---
    chunks = _build_chunk_list(days_to_fetch)
    end_ms = _day_start_ms(d_end) + 86_400_000 + 60_000  # end of d_end + 1 min buffer

    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[bybit_best_1m] {coin_u} starting parallel fetch:"
        f" days_to_fetch={len(days_to_fetch)} chunks={len(chunks)} concurrency={MAX_CONCURRENT}",
    )

    results_by_day = asyncio.run(_async_backfill(
        coin_u, chunks, end_ms,
        stop_check=stop_check,
        progress_cb=progress_cb,
    ))

    if _stop():
        notes.append("stopped")

    # --- Step 4: Write to disk ---
    days_written = 0
    for day_s in sorted(results_by_day.keys()):
        candles = results_by_day[day_s]
        if not candles:
            continue
        w = _write_candles_for_day(coin_u, day_s, candles, overwrite=refetch)
        minutes_written += w
        if w > 0:
            days_written += 1
        _emit({
            "stage": "writing",
            "day": day_s,
            "done": days_written,
            "total_days": total_planned_days,
            "minutes_written": minutes_written,
        })

    result = ImproveBest1mBybitResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y-%m-%d"),
        days_checked=total_planned_days,
        ccxt_days_fetched=len(results_by_day),
        minutes_written=minutes_written,
        notes=notes,
    )
    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[bybit_best_1m] {coin_u} done {result.to_dict()}",
    )
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
    """Refresh the last ``lookback_days`` days for a Bybit coin via CCXT.

    Sequential fetch -- no parallelism needed for short lookback windows.
    """
    import ccxt  # type: ignore

    coin_u = str(coin or "").strip().upper()
    lb = max(1, int(lookback_days))

    now_utc = datetime.now(tz=timezone.utc)
    d_end = now_utc.date()
    d_start = d_end - timedelta(days=lb)

    since_ms = int(datetime(d_start.year, d_start.month, d_start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(now_utc.timestamp() * 1000) + 120_000  # +2 min buffer

    sym = _coin_to_ccxt_symbol(coin_u)
    ex = ccxt.bybit({"enableRateLimit": False, "timeout": int(timeout_s * 1000)})

    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[bybit_latest_1m] start coin={coin_u} sym={sym} lookback={lb}d ({d_start} -> {d_end})",
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
                append_exchange_download_log(
                    STORAGE_EXCHANGE,
                    f"[bybit_latest_1m] fetch_error coin={coin_u} err={e}",
                    level="WARNING",
                )
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
        append_exchange_download_log(
            STORAGE_EXCHANGE,
            f"[bybit_latest_1m] error coin={coin_u} err={e}",
            level="WARNING",
        )
        return {
            "coin": coin_u,
            "lookback_days": lb,
            "result": "error",
            "error": str(e),
            "minutes_written": 0,
        }

    append_exchange_download_log(
        STORAGE_EXCHANGE,
        f"[bybit_latest_1m] done coin={coin_u} pages={pages} days={len(candles_by_day)}"
        f" min_written={minutes_written}",
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
