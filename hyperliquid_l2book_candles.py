from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import lz4.frame
import numpy as np
import orjson

from market_data import append_exchange_download_log, get_exchange_raw_root_dir, normalize_market_data_coin_dir
from market_data_sources import SOURCE_CODE_L2BOOK, update_source_index_for_day


def _ensure_date(v: date | str) -> date:
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    raise ValueError(f"Invalid date: {v!r}")


def _iter_dates_inclusive(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError("end_date must be >= start_date")
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _l2book_hour_path(*, coin: str, day: str, hour: int) -> Path:
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        raise ValueError("coin is empty")
    if len(day) != 8 or not day.isdigit():
        raise ValueError("day must be YYYYMMDD")
    if hour < 0 or hour > 23:
        raise ValueError("hour must be 0..23")
    base = get_exchange_raw_root_dir("hyperliquid")
    return base / "l2Book" / coin_dir / f"{day}-{hour:02d}.lz4"


def _day_tag(day: str) -> str:
    s = str(day or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"invalid day: {day}")


def _candles_day_out_path(*, coin: str, interval: str, day: str) -> Path:
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    interval_norm = str(interval or "").strip()
    base = get_exchange_raw_root_dir("hyperliquid")
    return base / f"candles_{interval_norm}" / coin_u / f"{_day_tag(day)}.npz"


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
                f"[hl_l2book_1m] corrupt_npz moved={bad_path.name} error={type(e).__name__}",
            )
        except Exception:
            pass
        return out
    if arr is None:
        return out
    day_start = datetime.strptime(_day_tag(day), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_start_ms = int(day_start.timestamp() * 1000)
    try:
        for row in arr:
            ts_ms = int(row["ts"])
            idx = int((ts_ms - day_start_ms) // 60_000)
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


def _hours_from_npz(path: Path) -> set[int]:
    hours: set[int] = set()
    try:
        with np.load(path) as data:
            arr = data["candles"] if "candles" in data else None
        if arr is None or len(arr) == 0:
            return hours
        ts = arr["ts"].astype("int64", copy=False)
        hour_vals = ((ts // 3_600_000) % 24).astype(int)
        for h in np.unique(hour_vals):
            hours.add(int(h))
    except Exception:
        return hours
    return hours


def _safe_decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _fmt_decimal(v: Decimal) -> str:
    # Avoid scientific notation; strip trailing zeros.
    s = format(v, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _extract_bid_ask_from_l2book_obj(obj: dict[str, Any]) -> tuple[Decimal | None, Decimal | None, int | None]:
    """Return (best_bid_px, best_ask_px, ts_ms) from one l2Book JSON line."""

    raw = obj.get("raw")
    if not isinstance(raw, dict):
        return (None, None, None)
    data = raw.get("data")
    if not isinstance(data, dict):
        return (None, None, None)

    ts_ms = data.get("time")
    try:
        ts_ms_i = int(ts_ms)
    except Exception:
        ts_ms_i = None

    levels = data.get("levels")
    if not isinstance(levels, list) or len(levels) < 2:
        return (None, None, ts_ms_i)

    bids = levels[0]
    asks = levels[1]

    best_bid = None
    best_ask = None

    if isinstance(bids, list) and bids:
        first = bids[0]
        if isinstance(first, dict):
            best_bid = _safe_decimal(first.get("px"))
        elif isinstance(first, (list, tuple)) and first:
            best_bid = _safe_decimal(first[0])

    if isinstance(asks, list) and asks:
        first = asks[0]
        if isinstance(first, dict):
            best_ask = _safe_decimal(first.get("px"))
        elif isinstance(first, (list, tuple)) and first:
            best_ask = _safe_decimal(first[0])

    return (best_bid, best_ask, ts_ms_i)


def iter_hyperliquid_l2book_mid_prices(path: Path) -> Iterator[tuple[int, Decimal]]:
    """Yield (ts_ms, mid_price) for snapshots in a .lz4 hour file."""
    try:
        with lz4.frame.open(str(path), mode="rb") as f:
            for raw_line in f:
                if not raw_line:
                    continue
                try:
                    line = raw_line.strip()  # orjson parses bytes directly
                except Exception:
                    continue
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)  # ~37% faster than json.loads
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                bid, ask, ts_ms = _extract_bid_ask_from_l2book_obj(obj)
                if ts_ms is None or bid is None or ask is None:
                    continue
                if bid <= 0 or ask <= 0:
                    continue
                # Use float for mid calculation (~5% faster), convert to Decimal for output
                mid = Decimal(str((float(bid) + float(ask)) / 2.0))
                yield (int(ts_ms), mid)
    except Exception as e:
        try:
            ts = int(time.time())
            bad_path = path.with_name(path.name + f".corrupt.{ts}")
            os.replace(path, bad_path)
            append_exchange_download_log(
                "hyperliquid",
                f"[hl_l2book_1m] corrupt_l2book moved={bad_path.name} error={type(e).__name__}",
            )
        except Exception:
            pass
        return


@dataclass
class GeneratedHourResult:
    coin: str
    day: str
    hour: int
    in_path: str
    out_path: str
    n_minutes_written: int
    n_lines_in: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "day": self.day,
            "hour": int(self.hour),
            "in_path": self.in_path,
            "out_path": self.out_path,
            "n_minutes_written": int(self.n_minutes_written),
            "n_lines_in": int(self.n_lines_in),
        }


def generate_1m_candles_from_l2book_hour(
    *,
    coin: str,
    day: str,
    hour: int,
    overwrite: bool = False,
    dry_run: bool = False,
    fill_missing: bool = True,
) -> GeneratedHourResult | None:
    """Generate synthetic 1m candles for one hour based on l2Book mid price.

    Produces 60 candles (or fewer if no snapshots). Volume is set to 0.
    """

    in_path = _l2book_hour_path(coin=coin, day=day, hour=int(hour))
    if not in_path.exists():
        return None

    out_path = _candles_day_out_path(coin=coin, interval="1m", day=day)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    minute_ms = 60_000
    hour_start = datetime.strptime(f"{day} {int(hour):02d}", "%Y%m%d %H").replace(tzinfo=timezone.utc)
    hour_start_ms = int(hour_start.timestamp() * 1000)
    hour_end_ms = hour_start_ms + 60 * minute_ms

    # Build minute OHLC from mid prices.
    # We keep only 60 minutes worth of bars.
    bars: list[dict[str, Any] | None] = [None] * 60
    counts: list[int] = [0] * 60

    n_lines_in = 0
    for ts_ms, mid in iter_hyperliquid_l2book_mid_prices(in_path):
        n_lines_in += 1
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
        counts[idx] += 1

    # Optionally fill missing minutes with last_close.
    if fill_missing:
        last: Decimal | None = None
        for i in range(60):
            b = bars[i]
            if b is None:
                if last is None:
                    continue
                bars[i] = {"o": last, "h": last, "l": last, "c": last}
                counts[i] = 0
            else:
                last = Decimal(b["c"])  # update carry

    out_map: dict[int, dict[str, Any]] = {}
    coin_u = normalize_market_data_coin_dir("hyperliquid", coin)
    day_start = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    day_start_ms = int(day_start.timestamp() * 1000)
    for i in range(60):
        b = bars[i]
        if b is None:
            continue
        t = hour_start_ms + i * minute_ms
        idx = int((t - day_start_ms) // minute_ms)
        if idx < 0 or idx >= 1440:
            continue
        candle = {
            "t": int(t),
            "o": float(Decimal(b["o"])),
            "h": float(Decimal(b["h"])),
            "l": float(Decimal(b["l"])),
            "c": float(Decimal(b["c"])),
            "v": 0.0,
        }
        out_map[idx] = candle

    if not out_map:
        return None

    added = len(out_map)
    added_indices: list[int] = list(out_map.keys())
    if not dry_run:
        existing = _read_day_npz(out_path, day=day) if out_path.exists() else {}
        added = 0
        added_indices = []
        for idx, candle in out_map.items():
            if not overwrite and idx in existing:
                continue
            existing[idx] = candle
            added += 1
            added_indices.append(idx)
        if added:
            _write_day_npz(out_path, existing)
            update_source_index_for_day(
                exchange="hyperliquid",
                coin=coin_u,
                day=day,
                minute_indices=added_indices,
                code=SOURCE_CODE_L2BOOK,
            )
        else:
            return GeneratedHourResult(
                coin=coin_u,
                day=day,
                hour=int(hour),
                in_path=str(in_path),
                out_path=str(out_path),
                n_minutes_written=0,
                n_lines_in=int(n_lines_in),
            )

    return GeneratedHourResult(
        coin=coin_u,
        day=day,
        hour=int(hour),
        in_path=str(in_path),
        out_path=str(out_path),
        n_minutes_written=int(added),
        n_lines_in=int(n_lines_in),
    )


@dataclass
class GenerateRangeResult:
    coin: str
    start_date: str
    end_date: str
    n_hours_found: int
    n_hours_written: int
    n_hours_skipped_missing: int
    n_hours_skipped_existing: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "n_hours_found": int(self.n_hours_found),
            "n_hours_written": int(self.n_hours_written),
            "n_hours_skipped_missing": int(self.n_hours_skipped_missing),
            "n_hours_skipped_existing": int(self.n_hours_skipped_existing),
        }


def generate_1m_candles_from_l2book_range(
    *,
    coin: str,
    start_date: date | str,
    end_date: date | str,
    overwrite: bool = False,
    dry_run: bool = False,
    fill_missing: bool = True,
    only_missing_days: bool = False,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    day_done_cb: Callable[[str], None] | None = None,
) -> GenerateRangeResult:
    """Generate synthetic 1m candles for all locally available l2Book hours in a date range."""

    d0 = _ensure_date(start_date)
    d1 = _ensure_date(end_date)
    coin_u = str(coin).strip().upper()

    append_exchange_download_log(
        "hyperliquid",
        f"[hl_l2book_1m] start coin={coin_u} {d0.strftime('%Y%m%d')}->{d1.strftime('%Y%m%d')} overwrite={bool(overwrite)} dry_run={bool(dry_run)} fill_missing={bool(fill_missing)}",
    )

    n_hours_found = 0
    n_hours_written = 0
    n_hours_skipped_missing = 0
    n_hours_skipped_existing = 0

    hours_plan: list[tuple[str, int]] = []
    day_plan_counts: dict[str, int] = {}
    for d in _iter_dates_inclusive(d0, d1):
        day = d.strftime("%Y%m%d")
        day_out_path = _candles_day_out_path(coin=coin_u, interval="1m", day=day)
        hours_present = _hours_from_npz(day_out_path) if day_out_path.exists() else set()
        if only_missing_days and day_out_path.exists() and len(hours_present) >= 24:
            continue
        for hour in range(24):
            in_path = _l2book_hour_path(coin=coin_u, day=day, hour=hour)
            if not in_path.exists():
                n_hours_skipped_missing += 1
                continue
            n_hours_found += 1
            if day_out_path.exists() and not overwrite and hour in hours_present:
                n_hours_skipped_existing += 1
                continue
            hours_plan.append((day, int(hour)))
            day_plan_counts[day] = int(day_plan_counts.get(day, 0)) + 1

    planned_total = len(hours_plan)
    done_total = 0
    if progress_cb is not None:
        try:
            progress_cb({"stage": "l2book", "planned": int(planned_total), "done": int(done_total)})
        except Exception:
            pass

    day_done_counts: dict[str, int] = {}
    for day, hour in hours_plan:
        res = generate_1m_candles_from_l2book_hour(
            coin=coin_u,
            day=day,
            hour=hour,
            overwrite=overwrite,
            dry_run=dry_run,
            fill_missing=fill_missing,
        )
        if res is not None:
            n_hours_written += 1
        done_total += 1
        day_done_counts[day] = int(day_done_counts.get(day, 0)) + 1
        if progress_cb is not None:
            try:
                progress_cb({
                    "stage": "l2book",
                    "planned": int(planned_total),
                    "done": int(done_total),
                    "day": str(day),
                    "hour": int(hour),
                })
            except Exception:
                pass
        if day_done_cb is not None:
            if int(day_done_counts.get(day, 0)) >= int(day_plan_counts.get(day, 0) or 0):
                try:
                    day_done_cb(str(day))
                except Exception:
                    pass

    out = GenerateRangeResult(
        coin=coin_u,
        start_date=d0.strftime("%Y%m%d"),
        end_date=d1.strftime("%Y%m%d"),
        n_hours_found=int(n_hours_found),
        n_hours_written=int(n_hours_written),
        n_hours_skipped_missing=int(n_hours_skipped_missing),
        n_hours_skipped_existing=int(n_hours_skipped_existing),
    )
    append_exchange_download_log("hyperliquid", f"[INFO] [hl_l2book_1m] done {out.to_dict()}")
    return out
