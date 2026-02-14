from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
import json
import os
import time
import numpy as np
import requests

from hyperliquid_api import download_hyperliquid_candles_api, fetch_candle_snapshot, normalize_hyperliquid_coin
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
    get_source_codes_for_day,
    update_source_index_for_day,
)
from Exchange import Exchange

# Enable detailed timing logs for performance analysis
# Set environment variable PBGUI_TIMING_LOGS=1 to enable, or change this constant
ENABLE_TIMING_LOGS = os.getenv("PBGUI_TIMING_LOGS", "0") == "1"


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
) -> tuple[int, int]:
    """Copy raw API 1m day files into the best archive dir.

    Source: data/ohlcv/hyperliquid/1m_api/<COIN>/YYYY-MM-DD.npz
    Dest:   data/ohlcv/hyperliquid/1m/<COIN>/YYYY-MM-DD.npz

    We prefer API data when available; API minutes overwrite existing minutes.

    Returns (n_copied, n_skipped).
    """

    coin_u = normalize_hyperliquid_coin(coin)
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    n_copied = 0
    n_skipped = 0

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

    return (int(n_copied), int(n_skipped))


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "l2book_minutes_added": int(self.l2book_minutes_added),
            "binance_minutes_filled": int(self.binance_minutes_filled),
            "bybit_minutes_filled": int(self.bybit_minutes_filled),
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
        copied, skipped = _copy_api_1m_into_best(
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

    if not earliest_candidates:
        days = []
    else:
        start_day = min(earliest_candidates)
        days = [d for d in _iter_dates_inclusive(start_day, d_end)]
    days_checked = 0
    l2book_minutes_added = 0
    binance_minutes_filled = 0
    bybit_minutes_filled = 0

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

        t0 = time.time()
        l2_map = _l2book_minutes_for_day(
            coin=coin_u,
            day=day_s,
            hours_filter=hours_to_rebuild,
        )
        t_l2book = time.time() - t0
        
        t0 = time.time()
        added_indices: list[int] = []
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
            # Fill with Binance USDT-Perp first
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

            # Fill remaining gaps with Bybit USDT-Perp
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
        if dry_run:
            hours_requested += 1
            continue

        candles = _fetch_day_with_retry(day_start_ms=start_ms, day_end_ms=effective_end_ms)
        hours_requested += 1
        minutes_filled += _merge_api_candles_into_day_file(
            coin=coin_u,
            day=day_s,
            candles=candles,
        )
        append_exchange_download_log(
            "hyperliquid",
            f"[hl_latest_1m] api_request coin={coin_u} day={day_s} missing_min={missing_total}",
        )
        time.sleep(0.1)

    append_exchange_download_log(
        "hyperliquid",
        f"[hl_latest_1m] api_summary coin={coin_u} lookback_days={lb} hours_requested={hours_requested} minutes_filled={minutes_filled}",
    )

    # Keep best 1m archive in sync with freshly updated API data.
    # Fast path: only sync recent window (today + previous day).
    sync_start = max(d_start, d_end - timedelta(days=1))
    copied_best_days, skipped_best_days = _copy_api_1m_into_best(
        coin=coin_u,
        start_date=sync_start,
        end_date=d_end,
        dry_run=bool(dry_run),
        overwrite=False,
        only_missing_days=False,
    )
    append_exchange_download_log(
        "hyperliquid",
        f"[hl_latest_1m] best_sync coin={coin_u} start={sync_start.strftime('%Y%m%d')} end={d_end.strftime('%Y%m%d')} copied_days={copied_best_days} skipped_days={skipped_best_days}",
    )

    result = {
        "coin": coin_u,
        "lookback_days": lb,
        "overwrite": False,
        "hours_requested": int(hours_requested),
        "minutes_filled": int(minutes_filled),
        "best_sync_days_copied": int(copied_best_days),
        "best_sync_days_skipped": int(skipped_best_days),
    }
    return result
