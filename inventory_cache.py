"""
Persistent SQLite inventory cache with mtime-based per-coin invalidation.

Location: data/logs/inventory_cache.db

For each (exchange, dataset, coin):
  - Stores all table display columns (n_files, total_bytes, oldest_day, ...,
    hl_minutes, other_minutes, missing_minutes, expected_hours, coverage_pct, ...)
  - Stores dir_mtime + optionally src_mtime (for source-index changes)
  - On load: reads all coin-dir mtimes (< 1ms for 227 coins), recomputes only
    changed coins, writes back — typical page load < 50ms regardless of coin count

Supported datasets:
  - "1m" / "candles_1m"      → data/ohlcv/{exchange}/1m/{coin}/*.npz + source index
  - "1m_api" / "candles_1m_api" → data/ohlcv/{exchange}/1m_api/{coin}/*.npz
  - "l2Book"                 → data/ohlcv/{exchange}/l2Book/{coin}/*.lz4
  - "pb7_cache:{tf}"         → pb7/caches/ohlcv/{exchange}/{tf}/{coin}/*.npy

Public API:
  get_inventory(exchange, dataset, *, lag_minutes, tradfi_type_fn, force_refresh)
      → list[dict]  (sorted by coin)
  invalidate_exchange(exchange)   → remove all rows for this exchange (full recompute)
  invalidate_coin(exchange, dataset, coin) → remove specific coin row
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from market_data import (
    _parse_day_hour_from_filename,
    _parse_pb7_cache_day_from_name,
    get_exchange_raw_root_dir,
    _get_pb7_root_dir,
)
from market_data_sources import (
    get_daily_source_counts_for_range,
    get_source_codes_for_day,
    get_source_index_path,
)

# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

_DB_FILENAME = "inventory_cache.db"


def _db_path() -> Path:
    p = Path(__file__).resolve().parent / "data" / "logs" / _DB_FILENAME
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@contextmanager
def _get_conn():
    conn = sqlite3.connect(str(_db_path()), timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _init_db(conn)
    try:
        yield conn
    finally:
        conn.close()


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory (
            exchange         TEXT NOT NULL,
            dataset          TEXT NOT NULL,
            coin             TEXT NOT NULL,
            dir_mtime        REAL NOT NULL DEFAULT 0,
            src_mtime        REAL NOT NULL DEFAULT 0,
            n_files          INTEGER NOT NULL DEFAULT 0,
            total_bytes      INTEGER NOT NULL DEFAULT 0,
            oldest_day       TEXT NOT NULL DEFAULT '',
            newest_day       TEXT NOT NULL DEFAULT '',
            n_days           INTEGER NOT NULL DEFAULT 0,
            expected_hours   REAL NOT NULL DEFAULT 0,
            coverage_pct     REAL NOT NULL DEFAULT 0,
            missing_days_count INTEGER NOT NULL DEFAULT 0,
            missing_days_sample TEXT NOT NULL DEFAULT '',
            hl_minutes       INTEGER NOT NULL DEFAULT 0,
            other_minutes    INTEGER NOT NULL DEFAULT 0,
            missing_minutes  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (exchange, dataset, coin)
        )
        """
    )
    conn.commit()


# ---------------------------------------------------------------------------
# mtime helpers
# ---------------------------------------------------------------------------

def _dir_mtime(path: Path) -> float:
    try:
        return float(path.stat().st_mtime)
    except Exception:
        return 0.0


def _src_mtime(exchange: str, coin: str) -> float:
    """mtime of the source index file (1m_src/.../sources.idx)."""
    try:
        return float(get_source_index_path(exchange, coin).stat().st_mtime)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Per-coin computation
# ---------------------------------------------------------------------------

def _scan_npz_coin_dir(coin_path: Path) -> dict[str, Any]:
    """Fast scan of a coin dir containing .npz files. Returns base fields."""
    n_files = 0
    total_bytes = 0
    oldest_day = ""
    newest_day = ""

    try:
        with os.scandir(str(coin_path)) as it:
            for fe in it:
                if not fe.name.endswith(".npz"):
                    continue
                day = _parse_day_hour_from_filename(fe.name)
                if not day:
                    continue
                day_s = day[0] if isinstance(day, tuple) else day
                if not day_s:
                    continue
                n_files += 1
                try:
                    total_bytes += fe.stat().st_size
                except Exception:
                    pass
                if not oldest_day or day_s < oldest_day:
                    oldest_day = day_s
                if not newest_day or day_s > newest_day:
                    newest_day = day_s
    except Exception:
        pass

    n_days = 0
    try:
        if oldest_day and newest_day:
            dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
            dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
            today = date.today()
            if today > dt1:
                dt1 = today
            if dt1 >= dt0:
                n_days = (dt1 - dt0).days + 1
    except Exception:
        pass

    return {
        "n_files": n_files,
        "total_bytes": total_bytes,
        "oldest_day": oldest_day,
        "newest_day": newest_day,
        "n_days": n_days,
    }


def _compute_1m_coin(
    exchange: str,
    coin: str,
    coin_path: Path,
    lag_minutes: int,
    tradfi_type_fn: Callable[[str], str] | None,
    expected_minutes_fn: "Callable[[str, Any], set[int]] | None" = None,
) -> dict[str, Any]:
    base = _scan_npz_coin_dir(coin_path)

    hl_minutes = 0
    other_minutes = 0
    missing_minutes = 0
    expected_hours = int(base["n_days"]) * 24
    coverage_pct = 0.0
    missing_days_count = 0
    missing_days_sample = ""

    oldest_day = base["oldest_day"]
    newest_day = base["newest_day"]

    if oldest_day and newest_day:
        end_day = date.today().strftime("%Y%m%d")
        try:
            effective_now = datetime.utcnow()
            if lag_minutes > 0:
                effective_now = effective_now - timedelta(minutes=lag_minutes)

            counts = get_daily_source_counts_for_range(
                exchange=exchange,
                coin=coin,
                start_day=oldest_day,
                end_day=end_day,
                lag_minutes=lag_minutes,
                cutoff_ts_ms=None,
            )

            if counts:
                for day_data in counts.values():
                    hl_minutes += int(day_data.get("api") or 0) + int(day_data.get("l2Book_mid") or 0)
                    other_minutes += int(day_data.get("other_exchange") or 0)

                tradfi_type = tradfi_type_fn(coin) if tradfi_type_fn else ""

                if tradfi_type and expected_minutes_fn is not None:
                    # TradFi: use session-aware expected minutes via caller-provided callback
                    expected_minutes_total = 0
                    covered_minutes_total = 0
                    missing_days: list[str] = []
                    try:
                        dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
                        dt1 = datetime.strptime(end_day, "%Y%m%d").date()
                        effective_day_utc = effective_now.date()
                        effective_minute_idx = effective_now.hour * 60 + effective_now.minute
                        cur = dt0
                        while cur <= dt1:
                            expected_indices: set[int] = expected_minutes_fn(tradfi_type, cur)
                            if expected_indices:
                                if cur > effective_day_utc:
                                    expected_indices = set()
                                elif cur == effective_day_utc:
                                    expected_indices = {mi for mi in expected_indices if mi <= effective_minute_idx}
                            expected_cnt = len(expected_indices)
                            if expected_cnt > 0:
                                expected_minutes_total += expected_cnt
                                day_s = cur.strftime("%Y%m%d")
                                day_codes = get_source_codes_for_day(exchange=exchange, coin=coin, day=day_s)
                                covered_cnt = 0
                                if isinstance(day_codes, list) and day_codes:
                                    for minute_idx in expected_indices:
                                        if minute_idx < len(day_codes) and int(day_codes[minute_idx]) != 0:
                                            covered_cnt += 1
                                covered_minutes_total += covered_cnt
                                if covered_cnt < expected_cnt:
                                    missing_days.append(day_s)
                            cur = cur + timedelta(days=1)

                        missing_missing = max(0, expected_minutes_total - covered_minutes_total)
                        missing_minutes = missing_missing
                        expected_hours = round(float(expected_minutes_total) / 60.0, 2)
                        coverage_pct = (
                            round((float(covered_minutes_total) / float(expected_minutes_total)) * 100.0, 2)
                            if expected_minutes_total > 0
                            else 0.0
                        )
                        missing_days_count = len(missing_days)
                        if missing_days:
                            sample = ",".join(missing_days[:10])
                            if len(missing_days) > 10:
                                sample += ",..."
                            missing_days_sample = sample
                    except Exception:
                        pass
                else:
                    # Non-TradFi: missing = total missing from source index
                    total_missing = sum(int(d.get("missing") or 0) for d in counts.values())
                    missing_minutes = total_missing
                    total_present = hl_minutes + other_minutes
                    total_counted = total_present + total_missing
                    if total_counted > 0:
                        coverage_pct = round((total_present / total_counted) * 100.0, 2)
                    # Missing days (< 1440 total minutes)
                    m_days = []
                    for day_s, day_data in sorted(counts.items()):
                        day_total = sum(day_data.values())
                        if day_total < 1440:
                            m_days.append(day_s)
                    missing_days_count = len(m_days)
                    if m_days:
                        sample = ",".join(m_days[:10])
                        if len(m_days) > 10:
                            sample += ",..."
                        missing_days_sample = sample
        except Exception:
            pass

    return {
        **base,
        "expected_hours": expected_hours,
        "coverage_pct": coverage_pct,
        "missing_days_count": missing_days_count,
        "missing_days_sample": missing_days_sample,
        "hl_minutes": hl_minutes,
        "other_minutes": other_minutes,
        "missing_minutes": missing_minutes,
    }


def _compute_1m_api_coin(coin_path: Path) -> dict[str, Any]:
    base = _scan_npz_coin_dir(coin_path)
    n_days = base["n_days"]
    n_files = base["n_files"]
    coverage_pct = 0.0
    if n_days > 0:
        coverage_pct = round(min(100.0, (n_files / float(n_days)) * 100.0), 2)
    expected_hours = int(n_days) * 24
    return {
        **base,
        "expected_hours": expected_hours,
        "coverage_pct": coverage_pct,
        "missing_days_count": 0,
        "missing_days_sample": "",
        "hl_minutes": 0,
        "other_minutes": 0,
        "missing_minutes": 0,
    }


def _compute_l2book_coin(coin_path: Path) -> dict[str, Any]:
    n_files = 0
    total_bytes = 0
    oldest_day = ""
    newest_day = ""
    hours_present: set[tuple[str, str]] = set()

    try:
        with os.scandir(str(coin_path)) as it:
            for fe in it:
                nm = fe.name.lower()
                if not (nm.endswith(".lz4") or nm.endswith(".jsonl")):
                    continue
                parsed = _parse_day_hour_from_filename(fe.name)
                if not parsed:
                    continue
                day_s, hour = parsed
                if not day_s:
                    continue
                n_files += 1
                try:
                    total_bytes += fe.stat().st_size
                except Exception:
                    pass
                if hour is not None:
                    hours_present.add((day_s, str(hour)))
                if not oldest_day or day_s < oldest_day:
                    oldest_day = day_s
                if not newest_day or day_s > newest_day:
                    newest_day = day_s
    except Exception:
        pass

    n_days = 0
    expected_hours = 0
    coverage_pct = 0.0
    missing_days_count = 0
    missing_days_sample = ""

    try:
        if oldest_day and newest_day:
            dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
            dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
            n_days = (dt1 - dt0).days + 1
            expected_hours = n_days * 24
            if expected_hours > 0:
                coverage_pct = round((len(hours_present) / float(expected_hours)) * 100.0, 2)

            hours_by_day: dict[str, int] = {}
            for day_s, _ in hours_present:
                hours_by_day[day_s] = hours_by_day.get(day_s, 0) + 1

            m_days: list[str] = []
            cur = dt0
            while cur <= dt1:
                day_s = cur.strftime("%Y%m%d")
                if hours_by_day.get(day_s, 0) < 24:
                    m_days.append(day_s)
                cur = cur + timedelta(days=1)
            missing_days_count = len(m_days)
            if m_days:
                sample = ",".join(m_days[:10])
                if len(m_days) > 10:
                    sample += ",..."
                missing_days_sample = sample
    except Exception:
        pass

    return {
        "n_files": n_files,
        "total_bytes": total_bytes,
        "oldest_day": oldest_day,
        "newest_day": newest_day,
        "n_days": n_days,
        "expected_hours": expected_hours,
        "coverage_pct": coverage_pct,
        "missing_days_count": missing_days_count,
        "missing_days_sample": missing_days_sample,
        "hl_minutes": 0,
        "other_minutes": 0,
        "missing_minutes": 0,
    }


def _compute_pb7_coin(coin_path: Path) -> dict[str, Any]:
    n_files = 0
    total_bytes = 0
    oldest_day = ""
    newest_day = ""

    try:
        with os.scandir(str(coin_path)) as it:
            for fe in it:
                if not fe.name.endswith(".npy"):
                    continue
                day_s = _parse_pb7_cache_day_from_name(fe.name)
                if not day_s:
                    continue
                n_files += 1
                try:
                    total_bytes += fe.stat().st_size
                except Exception:
                    pass
                if not oldest_day or day_s < oldest_day:
                    oldest_day = day_s
                if not newest_day or day_s > newest_day:
                    newest_day = day_s
    except Exception:
        pass

    n_days = 0
    try:
        if oldest_day and newest_day:
            dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
            dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
            if dt1 >= dt0:
                n_days = (dt1 - dt0).days + 1
    except Exception:
        pass

    return {
        "n_files": n_files,
        "total_bytes": total_bytes,
        "oldest_day": oldest_day,
        "newest_day": newest_day,
        "n_days": n_days,
        "expected_hours": 0,
        "coverage_pct": 0.0,
        "missing_days_count": 0,
        "missing_days_sample": "",
        "hl_minutes": 0,
        "other_minutes": 0,
        "missing_minutes": 0,
    }


def _compute_coin(
    exchange: str,
    dataset: str,
    coin: str,
    coin_path: Path,
    lag_minutes: int,
    tradfi_type_fn: Callable[[str], str] | None,
    expected_minutes_fn: "Callable[[str, Any], set[int]] | None" = None,
) -> dict[str, Any]:
    """Dispatch per-coin computation to the right function."""
    ds_l = str(dataset).strip().lower()
    if ds_l in ("1m", "candles_1m"):
        return _compute_1m_coin(exchange, coin, coin_path, lag_minutes, tradfi_type_fn, expected_minutes_fn)
    elif ds_l in ("1m_api", "candles_1m_api"):
        return _compute_1m_api_coin(coin_path)
    elif ds_l in ("l2book",):
        return _compute_l2book_coin(coin_path)
    elif ds_l.startswith("pb7_cache:"):
        return _compute_pb7_coin(coin_path)
    else:
        # Generic NPZ fallback
        return _compute_1m_api_coin(coin_path)


# ---------------------------------------------------------------------------
# Directory discovery
# ---------------------------------------------------------------------------

def _coin_dirs_for_dataset(exchange: str, dataset: str) -> dict[str, Path]:
    """Return {coin_name: coin_path} for all coin dirs of this exchange+dataset."""
    ds_l = str(dataset).strip().lower()

    if ds_l.startswith("pb7_cache:"):
        tf = ds_l[len("pb7_cache:"):]
        root = _get_pb7_root_dir()
        if root is None:
            return {}
        base = root / "caches" / "ohlcv" / str(exchange) / tf
        if not base.is_dir():
            return {}
        result: dict[str, Path] = {}
        with os.scandir(str(base)) as it:
            for e in it:
                if e.is_dir(follow_symlinks=False):
                    result[e.name] = Path(e.path)
        return result

    # Normalise dataset name for disk lookup
    ds_disk = ds_l
    if ds_l == "l2book":
        # On disk it might be l2Book (capital B) — check both
        base_lower = get_exchange_raw_root_dir(exchange) / "l2book"
        base_upper = get_exchange_raw_root_dir(exchange) / "l2Book"
        base = base_upper if base_upper.is_dir() else base_lower
    else:
        base = get_exchange_raw_root_dir(exchange) / ds_disk

    if not base.is_dir():
        # Try without "candles_" prefix mapping
        for alt in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
            alt_base = get_exchange_raw_root_dir(exchange) / alt
            if alt_base.is_dir() and alt == ds_disk:
                base = alt_base
                break

    if not base.is_dir():
        return {}

    result = {}
    with os.scandir(str(base)) as it:
        for e in it:
            if e.is_dir(follow_symlinks=False) and not e.name.endswith("_src"):
                result[e.name] = Path(e.path)
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_inventory(
    exchange: str,
    dataset: str,
    *,
    lag_minutes: int = 0,
    tradfi_type_fn: Callable[[str], str] | None = None,
    expected_minutes_fn: "Callable[[str, Any], set[int]] | None" = None,
    force_refresh: bool = False,
) -> list[dict]:
    """
    Return inventory rows for (exchange, dataset).

    On first call (cold DB): computes all coins — same runtime as before.
    On subsequent calls: only recomputes coins whose dir mtime changed.
    force_refresh=True: recompute all coins regardless of mtime.
    """
    ex = str(exchange or "").strip().lower()
    ds = str(dataset or "").strip().lower()

    coin_dirs = _coin_dirs_for_dataset(ex, ds)
    if not coin_dirs:
        return []

    # Fast mtime read for all coin dirs (< 1ms for 227 dirs)
    live_mtimes: dict[str, float] = {}
    for coin, cpath in coin_dirs.items():
        live_mtimes[coin] = _dir_mtime(cpath)

    # For 1m datasets: also check source-index mtime
    is_1m = ds in ("1m", "candles_1m")
    live_src_mtimes: dict[str, float] = {}
    if is_1m:
        for coin in coin_dirs:
            live_src_mtimes[coin] = _src_mtime(ex, coin)

    with _get_conn() as conn:
        # Load all cached rows for this exchange+dataset
        cur = conn.execute(
            "SELECT coin, dir_mtime, src_mtime, n_files, total_bytes, oldest_day, newest_day, "
            "n_days, expected_hours, coverage_pct, missing_days_count, missing_days_sample, "
            "hl_minutes, other_minutes, missing_minutes "
            "FROM inventory WHERE exchange=? AND dataset=?",
            (ex, ds),
        )
        cached: dict[str, dict] = {}
        for row in cur.fetchall():
            (coin, dir_mt, src_mt, n_files, total_bytes, oldest_day, newest_day,
             n_days, expected_hours, coverage_pct, missing_days_count, missing_days_sample,
             hl_minutes, other_minutes, missing_minutes) = row
            cached[coin] = {
                "dir_mtime": float(dir_mt),
                "src_mtime": float(src_mt),
                "n_files": int(n_files),
                "total_bytes": int(total_bytes),
                "oldest_day": oldest_day,
                "newest_day": newest_day,
                "n_days": int(n_days),
                "expected_hours": float(expected_hours),
                "coverage_pct": float(coverage_pct),
                "missing_days_count": int(missing_days_count),
                "missing_days_sample": missing_days_sample,
                "hl_minutes": int(hl_minutes),
                "other_minutes": int(other_minutes),
                "missing_minutes": int(missing_minutes),
            }

        # Find coins that need recomputation
        coins_to_recompute: list[str] = []
        coins_to_delete: list[str] = []

        # Coins on disk not in cache, or mtime changed
        for coin, cpath in coin_dirs.items():
            lmt = live_mtimes[coin]
            lsmt = live_src_mtimes.get(coin, 0.0)
            if force_refresh or coin not in cached:
                coins_to_recompute.append(coin)
            else:
                c = cached[coin]
                if abs(c["dir_mtime"] - lmt) > 0.001 or (is_1m and abs(c["src_mtime"] - lsmt) > 0.001):
                    coins_to_recompute.append(coin)

        # Coins in cache but no longer on disk → remove
        for coin in list(cached.keys()):
            if coin not in coin_dirs:
                coins_to_delete.append(coin)

        # Recompute changed coins
        upsert_params: list[tuple] = []
        for coin in coins_to_recompute:
            cpath = coin_dirs[coin]
            try:
                computed = _compute_coin(ex, ds, coin, cpath, lag_minutes, tradfi_type_fn, expected_minutes_fn)
            except Exception:
                computed = {
                    "n_files": 0, "total_bytes": 0, "oldest_day": "", "newest_day": "",
                    "n_days": 0, "expected_hours": 0, "coverage_pct": 0.0,
                    "missing_days_count": 0, "missing_days_sample": "",
                    "hl_minutes": 0, "other_minutes": 0, "missing_minutes": 0,
                }
            lmt = live_mtimes[coin]
            lsmt = live_src_mtimes.get(coin, 0.0)
            upsert_params.append((
                ex, ds, coin,
                lmt, lsmt,
                int(computed["n_files"]),
                int(computed["total_bytes"]),
                str(computed["oldest_day"]),
                str(computed["newest_day"]),
                int(computed["n_days"]),
                float(computed["expected_hours"]),
                float(computed["coverage_pct"]),
                int(computed["missing_days_count"]),
                str(computed["missing_days_sample"]),
                int(computed["hl_minutes"]),
                int(computed["other_minutes"]),
                int(computed["missing_minutes"]),
            ))
            # Update local cache dict so we return fresh data
            cached[coin] = {
                "dir_mtime": lmt,
                "src_mtime": lsmt,
                **computed,
            }

        if upsert_params:
            conn.executemany(
                "INSERT OR REPLACE INTO inventory VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                upsert_params,
            )
        if coins_to_delete:
            conn.executemany(
                "DELETE FROM inventory WHERE exchange=? AND dataset=? AND coin=?",
                [(ex, ds, c) for c in coins_to_delete],
            )
            for c in coins_to_delete:
                cached.pop(c, None)
        if upsert_params or coins_to_delete:
            conn.commit()

    # Build result list — same schema as summarize_raw_inventory rows
    # + hl_minutes, other_minutes, missing_minutes columns
    rows: list[dict] = []
    for coin, data in sorted(cached.items()):
        if coin not in coin_dirs:
            continue  # was deleted
        rows.append(
            {
                "exchange": ex,
                "dataset": ds,
                "coin": coin,
                "n_files": data["n_files"],
                "total_bytes": data["total_bytes"],
                "oldest_day": data["oldest_day"],
                "newest_day": data["newest_day"],
                "n_days": data["n_days"],
                "expected_hours": data["expected_hours"],
                "coverage_pct": data["coverage_pct"],
                "missing_days_count": data["missing_days_count"],
                "missing_days_sample": data["missing_days_sample"],
                "hl_minutes": data["hl_minutes"],
                "other_minutes": data["other_minutes"],
                "missing_minutes": data["missing_minutes"],
            }
        )
    return rows


def invalidate_exchange(exchange: str) -> None:
    """Remove all cached rows for an exchange — triggers full recompute on next get_inventory."""
    ex = str(exchange or "").strip().lower()
    with _get_conn() as conn:
        conn.execute("DELETE FROM inventory WHERE exchange=?", (ex,))
        conn.commit()


def invalidate_coin(exchange: str, dataset: str, coin: str) -> None:
    """Remove a single cached row — triggers recompute for that coin on next get_inventory."""
    ex = str(exchange or "").strip().lower()
    ds = str(dataset or "").strip().lower()
    cn = str(coin or "").strip()
    with _get_conn() as conn:
        conn.execute(
            "DELETE FROM inventory WHERE exchange=? AND dataset=? AND coin=?",
            (ex, ds, cn),
        )
        conn.commit()
