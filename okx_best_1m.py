"""
okx_best_1m.py - OKX USDT-SWAP 1m OHLCV downloader for PBGui.

Download strategy per coin:
  1. Find inception via OKX REST using a binary probe.
  2. Fetch the REST-only gap before the public archive starts.
  3. Fetch OKX archive ZIPs for complete historical UTC days.
  4. Fill archive rows that lack vol_ccy from OKX contract metadata, with REST fallback.
  5. Repair missing archive minutes via REST.
  6. Refresh recent/non-archive days via REST.

Storage layout:
  data/ohlcv/okx/1m/<COIN_DIR>/YYYY-MM-DD.npz
  data/ohlcv/okx/1m_src/<COIN_DIR>/sources.idx

COIN_DIR format: BTC_USDT:USDT, 1000PEPE_USDT:USDT.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import numpy as np
import requests

from market_data import append_exchange_download_log, get_exchange_raw_root_dir
from market_data_sources import SOURCE_CODE_API, update_source_index_for_day


SERVICE = "OKXBest1m"
EXCHANGE = "okx"
STORAGE_EXCHANGE = "okx"

OKX_BASE = "https://www.okx.com"
ARCHIVE_ENDPOINT = "/api/v5/public/market-data-history"
HISTORY_ENDPOINT = "/api/v5/market/history-candles"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"

BAR = "1m"
INST_TYPE = "SWAP"
REST_LIMIT = 300
MS_PER_MINUTE = 60_000
DAY_MS = 86_400_000

ARCHIVE_START = date(2021, 9, 1)
ARCHIVE_MIN_AGE_DAYS = 2
ARCHIVE_INDEX_WINDOW_DAYS = 20
ARCHIVE_MONTHLY_WINDOW_MONTHS = 20
ARCHIVE_DOWNLOAD_WORKERS = 24
REST_WORKERS = 16
REST_RATE_PER_SECOND = 9.0
ARCHIVE_INDEX_RATE_PER_SECOND = 2.2
VOLUME_ENRICH_WINDOW_DAYS = 60
MAX_RETRIES = 5
RETRY_WAIT_BASE_S = 1.0
DEFAULT_LATEST_LOOKBACK_DAYS = 3
MIN_DAY_CANDLES = 1440

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "PBGui/okx-best-1m",
}

_NPZ_DTYPE = np.dtype([
    ("ts", "i8"),
    ("o", "f4"),
    ("h", "f4"),
    ("l", "f4"),
    ("c", "f4"),
    ("bv", "f4"),
])

_OKX_USDT_MAP: dict[str, str] = {}
_OKX_USDT_MAP_SIG: tuple[int, int] | None = None
_CONTRACT_CACHE: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class ArchiveFile:
    """OKX archive file metadata returned by /public/market-data-history."""

    filename: str
    url: str
    size_mb: float = 0.0


@dataclass
class ImproveBest1mOkxResult:
    """Summary returned by improve_best_okx_1m_for_coin."""

    coin: str
    end_date: str
    days_checked: int
    archive_daily_downloaded: int
    rest_minutes_fetched: int
    repair_minutes_fetched: int
    minutes_written: int
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "end_date": self.end_date,
            "days_checked": int(self.days_checked),
            "archive_daily_downloaded": int(self.archive_daily_downloaded),
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


class _RetryableOkxError(RuntimeError):
    """Internal marker for retryable OKX HTTP/API failures."""


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


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _add_months(d: date, months: int) -> date:
    month0 = (int(d.year) * 12) + int(d.month) - 1 + int(months)
    return date(month0 // 12, (month0 % 12) + 1, 1)


def _month_end(d: date) -> date:
    return _add_months(_month_start(d), 1) - timedelta(days=1)


def _safe_float(value: Any) -> float | None:
    text = str(value if value is not None else "").strip()
    if not text or text.lower() == "none" or text.lower() == "nan":
        return None
    try:
        return float(text)
    except Exception:
        return None


def _okx_get_json(
    path: str,
    params: dict[str, Any],
    *,
    timeout_s: float = 30.0,
    retries: int = MAX_RETRIES,
    limiter: RateLimiter | None = None,
) -> dict[str, Any]:
    """GET OKX JSON with retry handling for public endpoints."""

    url = OKX_BASE + path
    delay = RETRY_WAIT_BASE_S
    last_error: Exception | None = None
    for attempt in range(1, int(retries) + 1):
        if limiter is not None:
            limiter.wait()
        try:
            response = requests.get(url, params=params, headers=_HEADERS, timeout=float(timeout_s))
            status = int(response.status_code)
            if status == 429 or status >= 500:
                raise _RetryableOkxError(f"HTTP {status}: {response.text[:300]}")
            response.raise_for_status()
            payload = response.json()
            if str(payload.get("code")) != "0":
                raise _RetryableOkxError(f"OKX code={payload.get('code')} msg={payload.get('msg')}")
            return payload
        except Exception as exc:
            last_error = exc
            if attempt >= int(retries):
                break
            sleep_s = delay + random.random() * min(delay, 1.0)
            time.sleep(sleep_s)
            delay *= 1.7
    raise RuntimeError(f"OKX GET failed path={path} params={params}: {last_error}")


def _download_bytes(url: str, *, timeout_s: float = 60.0, retries: int = MAX_RETRIES) -> bytes:
    """Download one static archive object with retries."""

    delay = RETRY_WAIT_BASE_S
    last_error: Exception | None = None
    for attempt in range(1, int(retries) + 1):
        try:
            response = requests.get(url, headers=_HEADERS, timeout=float(timeout_s))
            status = int(response.status_code)
            if status == 429 or status >= 500:
                raise _RetryableOkxError(f"HTTP {status}: {response.text[:300]}")
            response.raise_for_status()
            return bytes(response.content)
        except Exception as exc:
            last_error = exc
            if attempt >= int(retries):
                break
            time.sleep(delay + random.random() * min(delay, 1.0))
            delay *= 1.7
    raise RuntimeError(f"download failed url={url}: {last_error}")


def _load_okx_usdt_map() -> dict[str, str]:
    """Return coin -> OKX USDT-SWAP instrument map from data/coindata/okx/mapping.json."""

    global _OKX_USDT_MAP, _OKX_USDT_MAP_SIG
    mapping_path = Path(__file__).resolve().parent / "data" / "coindata" / "okx" / "mapping.json"
    sig: tuple[int, int] | None = None
    if mapping_path.exists():
        st = mapping_path.stat()
        sig = (st.st_mtime_ns, st.st_size)
    if sig == _OKX_USDT_MAP_SIG:
        return dict(_OKX_USDT_MAP)

    out: dict[str, str] = {}
    try:
        raw = json.loads(mapping_path.read_text(encoding="utf-8"))
        for rec in raw if isinstance(raw, list) else []:
            if not bool(rec.get("swap")):
                continue
            if not bool(rec.get("active", True)):
                continue
            if str(rec.get("quote") or "").strip().upper() != "USDT":
                continue
            symbol = str(rec.get("symbol") or "").strip().upper()
            coin = str(rec.get("coin") or "").strip().upper()
            if symbol and coin and symbol.endswith("-USDT-SWAP"):
                out.setdefault(coin, symbol)
    except Exception:
        out = {}
    _OKX_USDT_MAP = out
    _OKX_USDT_MAP_SIG = sig
    return dict(_OKX_USDT_MAP)


def _raw_base_from_coin(coin: str) -> str:
    value = str(coin or "").strip().upper()
    if not value:
        return ""
    if value.endswith("-USDT-SWAP"):
        return value[: -len("-USDT-SWAP")]
    if "/" in value:
        return value.split("/", 1)[0].strip()
    if value.endswith("_USDT:USDT"):
        return value[: -len("_USDT:USDT")]
    if value.endswith("USDT") and not value.endswith("_USDT"):
        return value[:-4]
    return value.strip(" _:-")


def _coin_to_okx_inst_id(coin: str) -> str:
    """BTC -> BTC-USDT-SWAP, already formatted OKX symbols pass through."""

    value = str(coin or "").strip().upper()
    if value.endswith("-USDT-SWAP"):
        return value
    base = _raw_base_from_coin(value)
    mapping = _load_okx_usdt_map()
    return mapping.get(base) or f"{base}-USDT-SWAP"


def _coin_to_inst_family(coin: str) -> str:
    inst_id = _coin_to_okx_inst_id(coin)
    parts = inst_id.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return f"{_raw_base_from_coin(coin)}-USDT"


def _coin_dir(coin: str) -> str:
    """Return PB7-compatible OKX USDT directory name for *coin*."""

    inst_id = _coin_to_okx_inst_id(coin)
    base = inst_id[: -len("-USDT-SWAP")] if inst_id.endswith("-USDT-SWAP") else _raw_base_from_coin(inst_id)
    return f"{base}_USDT:USDT"


def get_storage_coin_dir(coin: str) -> str:
    """Public helper for callers that need the on-disk OKX coin directory."""

    return _coin_dir(coin)


def _okx_day_path(coin: str, day: str | date) -> Path:
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
            append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] corrupt_npz moved={bad.name} error={type(exc).__name__}")
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
        if candle.get("v") is None:
            continue
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
    path = _okx_day_path(coin, day_s)
    existing: dict[int, dict[str, Any]] = {}
    if not overwrite and path.exists():
        existing = _read_day_npz(path, day=day_s)
    before_keys: set[int] = set() if overwrite else set(existing.keys())
    written_indices: list[int] = []
    for idx, candle in candles.items():
        if candle.get("v") is None:
            continue
        if overwrite or idx not in before_keys:
            existing[idx] = candle
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
    if not isinstance(row, list) or len(row) < 7:
        return None
    try:
        ts_ms = int(row[0])
    except Exception:
        return None
    vol_ccy = _safe_float(row[6] if len(row) > 6 else None)
    return {
        "t": ts_ms,
        "o": _safe_float(row[1]) or 0.0,
        "h": _safe_float(row[2]) or 0.0,
        "l": _safe_float(row[3]) or 0.0,
        "c": _safe_float(row[4]) or 0.0,
        "v": vol_ccy,
        "raw_vol": _safe_float(row[5] if len(row) > 5 else None),
    }


def _add_candle_to_bucket(buckets: dict[str, dict[int, dict[str, Any]]], candle: dict[str, Any]) -> bool:
    try:
        ts_ms = int(candle["t"])
    except Exception:
        return False
    day_dt = _ms_to_date(ts_ms)
    day_s = day_dt.strftime("%Y-%m-%d")
    idx = int((ts_ms - _day_start_ms(day_dt)) // MS_PER_MINUTE)
    if idx < 0 or idx >= MIN_DAY_CANDLES:
        return False
    buckets.setdefault(day_s, {})[idx] = candle
    return True


def _fetch_rest_chunk(
    inst_id: str,
    chunk_start_ms: int,
    chunk_end_ms: int,
    limiter: RateLimiter,
    timeout_s: float,
) -> tuple[int, int, list[list[Any]], str]:
    try:
        payload = _okx_get_json(
            HISTORY_ENDPOINT,
            {
                "instId": inst_id,
                "bar": BAR,
                "limit": str(REST_LIMIT),
                "after": str(int(chunk_end_ms)),
            },
            timeout_s=timeout_s,
            limiter=limiter,
        )
        return chunk_start_ms, chunk_end_ms, list(payload.get("data") or []), ""
    except Exception as exc:
        return chunk_start_ms, chunk_end_ms, [], str(exc)


def _build_rest_chunks(since_ms: int, end_ms: int) -> list[tuple[int, int]]:
    chunks: list[tuple[int, int]] = []
    cursor = int(since_ms)
    step = REST_LIMIT * MS_PER_MINUTE
    while cursor < int(end_ms):
        chunk_start = cursor
        cursor = min(cursor + step, int(end_ms))
        chunks.append((chunk_start, cursor))
    return chunks


def _rest_fetch_range(
    coin: str,
    since_ms: int,
    end_ms: int,
    *,
    timeout_s: float = 30.0,
    workers: int = REST_WORKERS,
    limiter: RateLimiter | None = None,
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stage: str = "rest",
) -> tuple[dict[str, dict[int, dict[str, Any]]], int, list[str]]:
    """Fetch OKX REST candles in [since_ms, end_ms)."""

    inst_id = _coin_to_okx_inst_id(coin)
    chunks = _build_rest_chunks(int(since_ms), int(end_ms))
    if not chunks:
        return {}, 0, []
    own_limiter = limiter or RateLimiter(REST_RATE_PER_SECOND)
    errors: list[str] = []
    buckets: dict[str, dict[int, dict[str, Any]]] = {}
    pages = 0

    def emit(done: int) -> None:
        if not progress_cb:
            return
        try:
            progress_cb({"stage": stage, "done": int(done), "planned": len(chunks)})
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = [
            pool.submit(_fetch_rest_chunk, inst_id, chunk_start, chunk_end, own_limiter, timeout_s)
            for chunk_start, chunk_end in chunks
        ]
        for done, future in enumerate(as_completed(futures), start=1):
            if stop_check and stop_check():
                break
            chunk_start, chunk_end, rows, error = future.result()
            pages += 1
            if error:
                errors.append(error)
            for row in rows:
                candle = _parse_rest_row(row)
                if not candle:
                    continue
                ts_ms = int(candle["t"])
                if chunk_start <= ts_ms < chunk_end:
                    _add_candle_to_bucket(buckets, candle)
            if done == 1 or done % 100 == 0 or done == len(futures):
                emit(done)
    return buckets, pages, errors


def _has_rest_data_before(inst_id: str, ts_ms: int, limiter: RateLimiter, timeout_s: float) -> bool:
    payload = _okx_get_json(
        HISTORY_ENDPOINT,
        {"instId": inst_id, "bar": BAR, "limit": "1", "after": str(int(ts_ms))},
        timeout_s=timeout_s,
        limiter=limiter,
    )
    return bool(payload.get("data"))


def _find_inception_ms(coin: str, *, timeout_s: float = 30.0, limiter: RateLimiter | None = None) -> int | None:
    """Return earliest available OKX 1m candle timestamp for *coin*."""

    inst_id = _coin_to_okx_inst_id(coin)
    rest_limiter = limiter or RateLimiter(REST_RATE_PER_SECOND)
    low = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    high = int((datetime.now(tz=timezone.utc) + timedelta(days=1)).timestamp() * 1000)
    try:
        if not _has_rest_data_before(inst_id, high, rest_limiter, timeout_s):
            return None
        while high - low > MS_PER_MINUTE:
            mid = ((low + high) // (2 * MS_PER_MINUTE)) * MS_PER_MINUTE
            if mid <= low:
                mid = low + MS_PER_MINUTE
            if _has_rest_data_before(inst_id, mid, rest_limiter, timeout_s):
                high = mid
            else:
                low = mid
        payload = _okx_get_json(
            HISTORY_ENDPOINT,
            {"instId": inst_id, "bar": BAR, "limit": "3", "after": str(int(high + MS_PER_MINUTE))},
            timeout_s=timeout_s,
            limiter=rest_limiter,
        )
        rows = payload.get("data") or []
        ts_values = []
        for row in rows:
            try:
                ts_values.append(int(row[0]))
            except Exception:
                continue
        return min(ts_values) if ts_values else high - MS_PER_MINUTE
    except Exception as exc:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] inception_error coin={coin} inst={inst_id} err={exc}", level="WARNING")
        return None


def _archive_file_date(item: ArchiveFile) -> date | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", str(item.filename or ""))
    if not match:
        month_match = re.search(r"(\d{4}-\d{2})(?:\.zip|$)", str(item.filename or ""))
        if not month_match:
            return None
        try:
            return datetime.strptime(month_match.group(1), "%Y-%m").date()
        except Exception:
            return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def _archive_file_month(item: ArchiveFile) -> date | None:
    match = re.search(r"(\d{4}-\d{2})(?:\.zip|$)", str(item.filename or ""))
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m").date()
    except Exception:
        return None


def _collect_archive_files(
    inst_family: str,
    start_day: date,
    end_day: date,
    *,
    date_aggr_type: str = "daily",
    timeout_s: float = 30.0,
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> list[ArchiveFile]:
    """Collect OKX archive ZIP URLs in bounded daily or monthly windows."""

    if end_day < start_day:
        return []
    limiter = RateLimiter(ARCHIVE_INDEX_RATE_PER_SECOND)
    files: list[ArchiveFile] = []
    aggr = str(date_aggr_type or "daily").strip().lower()
    cursor = _month_start(start_day) if aggr == "monthly" else start_day
    calls = 0
    while cursor <= end_day:
        if stop_check and stop_check():
            break
        if aggr == "monthly":
            window_end = min(_add_months(cursor, ARCHIVE_MONTHLY_WINDOW_MONTHS - 1), _month_start(end_day))
        else:
            window_end = min(cursor + timedelta(days=ARCHIVE_INDEX_WINDOW_DAYS - 1), end_day)
        payload = _okx_get_json(
            ARCHIVE_ENDPOINT,
            {
                "module": "2",
                "instType": INST_TYPE,
                "instFamilyList": inst_family,
                "dateAggrType": aggr,
                "begin": str(_day_start_ms(cursor)),
                "end": str(_day_start_ms(window_end)),
            },
            timeout_s=timeout_s,
            limiter=limiter,
        )
        calls += 1
        for block in payload.get("data") or []:
            for detail in block.get("details") or []:
                for item in detail.get("groupDetails") or []:
                    filename = str(item.get("filename") or "")
                    url = str(item.get("url") or "")
                    if filename and url:
                        files.append(ArchiveFile(filename=filename, url=url, size_mb=_safe_float(item.get("sizeMB")) or 0.0))
        if progress_cb:
            try:
                progress_cb({"stage": "archive_index", "day": window_end.strftime("%Y-%m-%d"), "done": calls, "files": len(files)})
            except Exception:
                pass
        cursor = _add_months(window_end, 1) if aggr == "monthly" else window_end + timedelta(days=1)
    files.sort(key=lambda item: (_archive_file_date(item) or date.min, item.filename))
    return files


def _parse_archive_zip(raw_data: bytes, inst_id: str) -> dict[str, dict[int, dict[str, Any]]]:
    """Parse one OKX archive ZIP into UTC day buckets."""

    buckets: dict[str, dict[int, dict[str, Any]]] = {}
    with zipfile.ZipFile(io.BytesIO(raw_data)) as archive:
        names = archive.namelist()
        if not names:
            raise RuntimeError("empty zip")
        csv_text = archive.read(names[0]).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        if str(row.get("instrument_name") or "").strip().upper() != inst_id:
            continue
        try:
            ts_ms = int(str(row.get("open_time") or "0"))
        except Exception:
            continue
        if ts_ms <= 0:
            continue
        vol_ccy = _safe_float(row.get("vol_ccy"))
        candle = {
            "t": ts_ms,
            "o": _safe_float(row.get("open")) or 0.0,
            "h": _safe_float(row.get("high")) or 0.0,
            "l": _safe_float(row.get("low")) or 0.0,
            "c": _safe_float(row.get("close")) or 0.0,
            "v": vol_ccy,
            "raw_vol": _safe_float(row.get("vol")),
        }
        _add_candle_to_bucket(buckets, candle)
    if not buckets:
        raise RuntimeError("no matching candles in zip")
    return buckets


def _archive_candidate_days(item: ArchiveFile) -> list[date]:
    archive_month = _archive_file_month(item)
    if archive_month is not None and re.search(r"\d{4}-\d{2}(?:\.zip|$)", str(item.filename or "")):
        first = archive_month - timedelta(days=1)
        last = _month_end(archive_month)
        return list(_iter_day_range(first, last))
    archive_day = _archive_file_date(item)
    if archive_day is None:
        return []
    return [archive_day - timedelta(days=1), archive_day]


def _is_day_complete_on_disk(coin: str, day_d: date) -> bool:
    day_s = day_d.strftime("%Y-%m-%d")
    path = _okx_day_path(coin, day_s)
    return path.exists() and len(_read_day_npz(path, day=day_s)) >= MIN_DAY_CANDLES


def _download_one_archive_file(
    item: ArchiveFile,
    inst_id: str,
    coin: str,
    *,
    skip_existing: bool,
    timeout_s: float,
) -> tuple[str, dict[str, dict[int, dict[str, Any]]], bool, str]:
    if skip_existing:
        candidate_days = _archive_candidate_days(item)
        if candidate_days and all(_is_day_complete_on_disk(coin, day_d) for day_d in candidate_days):
            return item.filename, {}, True, ""
    try:
        raw = _download_bytes(item.url, timeout_s=timeout_s)
        return item.filename, _parse_archive_zip(raw, inst_id), False, ""
    except Exception as exc:
        return item.filename, {}, False, str(exc)


def _download_archive_files_bulk(
    files: list[ArchiveFile],
    inst_id: str,
    coin: str,
    *,
    skip_existing: bool,
    timeout_s: float,
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[tuple[str, dict[str, dict[int, dict[str, Any]]]]], int, list[str]]:
    """Download and parse archive files concurrently."""

    parsed: list[tuple[str, dict[str, dict[int, dict[str, Any]]]]] = []
    skipped = 0
    errors: list[str] = []
    if not files:
        return parsed, skipped, errors
    with ThreadPoolExecutor(max_workers=max(1, int(ARCHIVE_DOWNLOAD_WORKERS))) as pool:
        futures = [
            pool.submit(
                _download_one_archive_file,
                item,
                inst_id,
                coin,
                skip_existing=skip_existing,
                timeout_s=timeout_s,
            )
            for item in files
        ]
        for done, future in enumerate(as_completed(futures), start=1):
            if stop_check and stop_check():
                break
            filename, buckets, was_skipped, error = future.result()
            if was_skipped:
                skipped += 1
            elif error:
                errors.append(f"{filename}: {error}")
            else:
                parsed.append((filename, buckets))
            if progress_cb and (done == 1 or done % 50 == 0 or done == len(futures)):
                try:
                    progress_cb({"stage": "archive_download", "done": done, "planned": len(futures), "errors": len(errors), "skipped": skipped})
                except Exception:
                    pass
    parsed.sort(key=lambda item: item[0])
    return parsed, skipped, errors


def _get_contract_meta(inst_id: str, *, timeout_s: float = 30.0) -> dict[str, Any]:
    cached = _CONTRACT_CACHE.get(inst_id)
    if cached:
        return dict(cached)
    payload = _okx_get_json(INSTRUMENTS_ENDPOINT, {"instType": INST_TYPE, "instId": inst_id}, timeout_s=timeout_s)
    row = (payload.get("data") or [{}])[0]
    base = inst_id.split("-", 1)[0].upper()
    quote = "USDT"
    meta = {
        "ct_val": _safe_float(row.get("ctVal")) or 0.0,
        "ct_val_ccy": str(row.get("ctValCcy") or "").strip().upper(),
        "base": base,
        "quote": quote,
    }
    _CONTRACT_CACHE[inst_id] = meta
    return dict(meta)


def _contract_volume_to_base(candle: dict[str, Any], contract_meta: dict[str, Any]) -> float | None:
    raw_vol = _safe_float(candle.get("raw_vol"))
    ct_val = _safe_float(contract_meta.get("ct_val"))
    if raw_vol is None or ct_val is None or raw_vol < 0 or ct_val <= 0:
        return None
    ccy = str(contract_meta.get("ct_val_ccy") or "").upper()
    base = str(contract_meta.get("base") or "").upper()
    quote = str(contract_meta.get("quote") or "USDT").upper()
    if ccy == quote:
        close = _safe_float(candle.get("c"))
        if close and close > 0:
            return (raw_vol * ct_val) / close
        return None
    if not ccy or ccy == base:
        return raw_vol * ct_val
    return raw_vol * ct_val


def _derive_missing_archive_volumes_from_contract(
    coin: str,
    day_buckets: dict[str, dict[int, dict[str, Any]]],
    *,
    timeout_s: float,
) -> tuple[int, int]:
    """Fill missing archive vol_ccy values using OKX contract volume metadata."""

    if not any(any(candle.get("v") is None for candle in candles.values()) for candles in day_buckets.values()):
        return 0, 0
    try:
        meta = _get_contract_meta(_coin_to_okx_inst_id(coin), timeout_s=timeout_s)
    except Exception:
        return 0, 0

    filled = 0
    days_filled = 0
    for _day_s, candles in sorted(day_buckets.items()):
        day_filled = 0
        for _idx, candle in list(candles.items()):
            if candle.get("v") is not None:
                continue
            value = _contract_volume_to_base(candle, meta)
            if value is None:
                continue
            candle["v"] = float(value)
            filled += 1
            day_filled += 1
        if day_filled:
            days_filled += 1
    return filled, days_filled


def _enrich_missing_archive_volumes(
    coin: str,
    day_s: str,
    candles: dict[int, dict[str, Any]],
    *,
    timeout_s: float,
    notes: list[str],
    rest_limiter: RateLimiter | None = None,
) -> tuple[int, int]:
    """Fill archive candles lacking vol_ccy from contract metadata, then REST."""

    contract_filled, _contract_days = _derive_missing_archive_volumes_from_contract(
        coin,
        {day_s: candles},
        timeout_s=timeout_s,
    )
    if contract_filled:
        notes.append(f"volume_contract_derived={day_s}:{contract_filled}")

    missing_indices = [idx for idx, candle in candles.items() if candle.get("v") is None]
    if not missing_indices:
        return contract_filled, 0
    day_d = datetime.strptime(_day_tag(day_s), "%Y-%m-%d").date()
    start_ms = _day_start_ms(day_d)
    rest_days, pages, errors = _rest_fetch_range(
        coin,
        start_ms,
        start_ms + DAY_MS,
        timeout_s=timeout_s,
        workers=REST_WORKERS,
        limiter=rest_limiter,
        stage="volume_enrich",
    )
    if errors:
        notes.append(f"volume_enrich_errors={day_s}:{len(errors)}")
    rest_candles = rest_days.get(day_s, {})
    enriched = contract_filled
    for idx in list(missing_indices):
        rest_candle = rest_candles.get(idx)
        if rest_candle and rest_candle.get("v") is not None:
            candles[idx]["v"] = float(rest_candle["v"])
            enriched += 1

    still_missing = [idx for idx, candle in candles.items() if candle.get("v") is None]
    if still_missing:
        fallback = 0
        try:
            meta = _get_contract_meta(_coin_to_okx_inst_id(coin), timeout_s=timeout_s)
        except Exception:
            meta = {}
        for idx in list(still_missing):
            value = _contract_volume_to_base(candles[idx], meta)
            if value is not None:
                candles[idx]["v"] = float(value)
                fallback += 1
        if fallback:
            notes.append(f"volume_contract_fallback={day_s}:{fallback}")
        remaining = sum(1 for candle in candles.values() if candle.get("v") is None)
        if remaining:
            notes.append(f"volume_missing_unresolved={day_s}:{remaining}")
    return enriched, pages


def _day_windows(start_day: date, end_day: date, max_days: int) -> list[tuple[date, date]]:
    """Split a day range into bounded consecutive windows."""

    if end_day < start_day:
        return []
    out: list[tuple[date, date]] = []
    cur = start_day
    step_days = max(1, int(max_days))
    while cur <= end_day:
        win_end = min(end_day, cur + timedelta(days=step_days - 1))
        out.append((cur, win_end))
        cur = win_end + timedelta(days=1)
    return out


def _enrich_missing_archive_volumes_bulk(
    coin: str,
    day_buckets: dict[str, dict[int, dict[str, Any]]],
    *,
    timeout_s: float,
    notes: list[str],
    stop_check: Callable[[], bool] | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    rest_limiter: RateLimiter | None = None,
) -> tuple[int, int]:
    """Fill missing archive vol_ccy for many UTC days."""

    contract_filled, contract_days = _derive_missing_archive_volumes_from_contract(
        coin,
        day_buckets,
        timeout_s=timeout_s,
    )
    if contract_filled:
        notes.append(f"volume_contract_derived={contract_days}:{contract_filled}")

    missing_days: list[date] = []
    for day_s, candles in day_buckets.items():
        if any(candle.get("v") is None for candle in candles.values()):
            try:
                missing_days.append(datetime.strptime(_day_tag(day_s), "%Y-%m-%d").date())
            except Exception:
                continue
    if not missing_days:
        return contract_filled, 0

    windows: list[tuple[date, date]] = []
    for run_start, run_end in _runs_from_days(sorted(set(missing_days))):
        windows.extend(_day_windows(run_start, run_end, VOLUME_ENRICH_WINDOW_DAYS))

    enriched = contract_filled
    pages_total = 0
    limiter = rest_limiter or RateLimiter(REST_RATE_PER_SECOND)
    for window_i, (window_start, window_end) in enumerate(windows, start=1):
        if stop_check and stop_check():
            break
        _emit(progress_cb, {
            "stage": "volume_enrich",
            "day": window_start.strftime("%Y-%m-%d"),
            "done": window_i - 1,
            "planned": len(windows),
        })
        rest_days, pages, errors = _rest_fetch_range(
            coin,
            _day_start_ms(window_start),
            _day_start_ms(window_end) + DAY_MS,
            timeout_s=timeout_s,
            workers=REST_WORKERS,
            limiter=limiter,
            stop_check=stop_check,
            stage="volume_enrich",
        )
        pages_total += pages
        if errors:
            notes.append(f"volume_enrich_errors={window_start.strftime('%Y-%m-%d')}..{window_end.strftime('%Y-%m-%d')}:{len(errors)}")

        for day_d in _iter_day_range(window_start, window_end):
            day_s = day_d.strftime("%Y-%m-%d")
            candles = day_buckets.get(day_s)
            if not candles:
                continue
            rest_candles = rest_days.get(day_s, {})
            for idx, candle in list(candles.items()):
                if candle.get("v") is not None:
                    continue
                rest_candle = rest_candles.get(idx)
                if rest_candle and rest_candle.get("v") is not None:
                    candle["v"] = float(rest_candle["v"])
                    enriched += 1
        _emit(progress_cb, {
            "stage": "volume_enrich",
            "day": window_end.strftime("%Y-%m-%d"),
            "done": window_i,
            "planned": len(windows),
        })

    remaining_days = [
        day_s
        for day_s, candles in sorted(day_buckets.items())
        if any(candle.get("v") is None for candle in candles.values())
    ]
    if remaining_days:
        try:
            meta = _get_contract_meta(_coin_to_okx_inst_id(coin), timeout_s=timeout_s)
        except Exception:
            meta = {}
        for day_s in remaining_days:
            candles = day_buckets.get(day_s) or {}
            fallback = 0
            for idx, candle in list(candles.items()):
                if candle.get("v") is not None:
                    continue
                value = _contract_volume_to_base(candle, meta)
                if value is not None:
                    candle["v"] = float(value)
                    fallback += 1
            if fallback:
                notes.append(f"volume_contract_fallback={day_s}:{fallback}")
            remaining = sum(1 for candle in candles.values() if candle.get("v") is None)
            if remaining:
                notes.append(f"volume_missing_unresolved={day_s}:{remaining}")

    return enriched, pages_total


def _missing_minute_indices(candles: dict[int, dict[str, Any]]) -> list[int]:
    present = {int(idx) for idx in candles.keys() if 0 <= int(idx) < MIN_DAY_CANDLES}
    return [idx for idx in range(MIN_DAY_CANDLES) if idx not in present]


def _repair_missing_minutes_for_day(
    coin: str,
    day_s: str,
    *,
    timeout_s: float,
    stop_check: Callable[[], bool] | None = None,
    rest_limiter: RateLimiter | None = None,
) -> tuple[int, int, int]:
    """Repair a UTC day missing minute candles via REST. Returns (written, fetched, remaining)."""

    path = _okx_day_path(coin, day_s)
    existing = _read_day_npz(path, day=day_s)
    missing = _missing_minute_indices(existing)
    if not missing:
        return 0, 0, 0
    if stop_check and stop_check():
        return 0, 0, len(missing)
    day_d = datetime.strptime(_day_tag(day_s), "%Y-%m-%d").date()
    start_ms = _day_start_ms(day_d)
    rest_days, _pages, errors = _rest_fetch_range(
        coin,
        start_ms,
        start_ms + DAY_MS,
        timeout_s=timeout_s,
        workers=REST_WORKERS,
        limiter=rest_limiter,
        stop_check=stop_check,
        stage="repair",
    )
    if errors:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] repair_errors coin={coin} day={day_s} count={len(errors)}", level="WARNING")
    rest_candles = rest_days.get(day_s, {})
    repair = {idx: rest_candles[idx] for idx in missing if idx in rest_candles and rest_candles[idx].get("v") is not None}
    written = _write_candles_for_day(coin, day_s, repair, overwrite=False, source_code=SOURCE_CODE_API) if repair else 0
    after = _read_day_npz(path, day=day_s)
    remaining = len(_missing_minute_indices(after))
    return written, len(repair), remaining


def _iter_day_range(start_day: date, end_day: date) -> list[date]:
    if end_day < start_day:
        return []
    out: list[date] = []
    cur = start_day
    while cur <= end_day:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _runs_from_days(days: list[date]) -> list[tuple[date, date]]:
    if not days:
        return []
    sorted_days = sorted(days)
    runs: list[tuple[date, date]] = []
    start = prev = sorted_days[0]
    for day_d in sorted_days[1:]:
        if day_d == prev + timedelta(days=1):
            prev = day_d
            continue
        runs.append((start, prev))
        start = prev = day_d
    runs.append((start, prev))
    return runs


def _days_needing_fetch(coin: str, start_day: date, end_day: date, *, refetch: bool) -> list[date]:
    days: list[date] = []
    for day_d in _iter_day_range(start_day, end_day):
        if refetch:
            days.append(day_d)
            continue
        if not _is_day_complete_on_disk(coin, day_d):
            days.append(day_d)
    return days


def _emit(progress_cb: Callable[[dict[str, Any]], None] | None, snap: dict[str, Any]) -> None:
    if not progress_cb:
        return
    try:
        progress_cb(snap)
    except Exception:
        pass


def improve_best_okx_1m_for_coin(
    *,
    coin: str,
    end_date: date | str | None = None,
    start_date_override: date | str | None = None,
    refetch: bool = False,
    timeout_s: float = 30.0,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
    rest_limiter: RateLimiter | None = None,
) -> ImproveBest1mOkxResult:
    """Full backfill of OKX USDT-SWAP 1m data from inception to end_date."""

    coin_u = _raw_base_from_coin(coin)
    d_end = _parse_date_input(end_date, date.today())
    d_start_override = _parse_date_input(start_date_override, date.min) if start_date_override else None
    notes: list[str] = []
    archive_daily_downloaded = 0
    rest_minutes_fetched = 0
    repair_minutes_fetched = 0
    minutes_written = 0

    def stopped() -> bool:
        return bool(stop_check and stop_check())

    _emit(progress_cb, {"stage": "starting", "coin": coin_u})
    _emit(progress_cb, {"stage": "finding_inception", "coin": coin_u})
    inception_ms = _find_inception_ms(coin_u, timeout_s=timeout_s, limiter=rest_limiter)
    if inception_ms is None:
        notes.append("inception_not_found")
        append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] {coin_u} inception not found")
        return ImproveBest1mOkxResult(coin_u, d_end.strftime("%Y-%m-%d"), 0, 0, 0, 0, 0, notes)
    inception_day = _ms_to_date(inception_ms)
    d_start = max(inception_day, d_start_override) if d_start_override else inception_day
    if d_start > d_end:
        notes.append("empty_range")
        return ImproveBest1mOkxResult(coin_u, d_end.strftime("%Y-%m-%d"), 0, 0, 0, 0, 0, notes)
    total_planned_days = max(1, (d_end - d_start).days + 1)
    notes.append(f"inception={inception_day.strftime('%Y-%m-%d')}")
    _emit(progress_cb, {"stage": "inception_found", "day": inception_day.strftime("%Y-%m-%d"), "total_days": total_planned_days})

    if stopped():
        return ImproveBest1mOkxResult(coin_u, d_end.strftime("%Y-%m-%d"), total_planned_days, 0, 0, 0, 0, notes + ["stopped"])

    # REST-only gap before archive starts.
    rest_gap_end = min(d_end, ARCHIVE_START - timedelta(days=1))
    if d_start <= rest_gap_end and not stopped():
        gap_days = _days_needing_fetch(coin_u, d_start, rest_gap_end, refetch=refetch)
        for run_start, run_end in _runs_from_days(gap_days):
            _emit(progress_cb, {"stage": "rest_gap", "day": run_start.strftime("%Y-%m-%d"), "total_days": total_planned_days})
            days_data, _pages, errors = _rest_fetch_range(
                coin_u,
                _day_start_ms(run_start),
                _day_start_ms(run_end) + DAY_MS,
                timeout_s=timeout_s,
                limiter=rest_limiter,
                stop_check=stop_check,
                progress_cb=progress_cb,
                stage="rest_gap",
            )
            if errors:
                notes.append(f"rest_gap_errors={len(errors)}")
            for day_s, candles in sorted(days_data.items()):
                written = _write_candles_for_day(coin_u, day_s, candles, overwrite=refetch, source_code=SOURCE_CODE_API)
                minutes_written += written
                rest_minutes_fetched += len(candles)
                _emit(progress_cb, {"stage": "rest_gap", "day": day_s, "minutes_written": minutes_written, "total_days": total_planned_days})

    if stopped():
        return ImproveBest1mOkxResult(coin_u, d_end.strftime("%Y-%m-%d"), total_planned_days, archive_daily_downloaded, rest_minutes_fetched, repair_minutes_fetched, minutes_written, notes + ["stopped"])

    today = date.today()
    # OKX archive files are UTC+8 days. A complete UTC day D needs archive file D+1,
    # so only days through today-(ARCHIVE_MIN_AGE_DAYS+1) are archive-complete.
    archive_complete_cutoff = today - timedelta(days=ARCHIVE_MIN_AGE_DAYS + 1)
    archive_start = max(d_start, ARCHIVE_START)
    archive_end = min(d_end, archive_complete_cutoff)

    if archive_start <= archive_end and not stopped():
        inst_id = _coin_to_okx_inst_id(coin_u)
        inst_family = _coin_to_inst_family(coin_u)
        index_start = archive_start
        index_end = archive_end + timedelta(days=1)
        _emit(progress_cb, {"stage": "archive_index", "day": index_start.strftime("%Y-%m-%d"), "total_days": total_planned_days})
        try:
            archive_files: list[ArchiveFile] = []
            try:
                archive_files = _collect_archive_files(
                    inst_family,
                    index_start,
                    index_end,
                    date_aggr_type="monthly",
                    timeout_s=timeout_s,
                    stop_check=stop_check,
                    progress_cb=progress_cb,
                )
            except Exception as exc:
                notes.append(f"archive_monthly_index_failed={type(exc).__name__}")
                append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] archive_monthly_index_failed coin={coin_u} err={exc}", level="WARNING")
            if archive_files:
                notes.append("archive_aggr=monthly")
                tail_start = _month_start(archive_end)
                try:
                    tail_daily_files = _collect_archive_files(
                        inst_family,
                        tail_start,
                        index_end,
                        date_aggr_type="daily",
                        timeout_s=timeout_s,
                        stop_check=stop_check,
                        progress_cb=progress_cb,
                    )
                    if tail_daily_files:
                        archive_files.extend(tail_daily_files)
                        archive_files.sort(key=lambda item: (_archive_file_date(item) or date.min, item.filename))
                        notes.append(f"archive_tail_daily={len(tail_daily_files)}")
                except Exception as exc:
                    notes.append(f"archive_tail_daily_failed={type(exc).__name__}")
                    append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] archive_tail_daily_failed coin={coin_u} err={exc}", level="WARNING")
            elif not stopped():
                archive_files = _collect_archive_files(
                    inst_family,
                    index_start,
                    index_end,
                    date_aggr_type="daily",
                    timeout_s=timeout_s,
                    stop_check=stop_check,
                    progress_cb=progress_cb,
                )
                if archive_files:
                    notes.append("archive_aggr=daily")
        except Exception as exc:
            archive_files = []
            notes.append(f"archive_index_failed={type(exc).__name__}")
            append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] archive_index_failed coin={coin_u} err={exc}", level="WARNING")

        parsed_archive, _skipped, archive_errors = _download_archive_files_bulk(
            archive_files,
            inst_id,
            coin_u,
            skip_existing=not refetch,
            timeout_s=max(60.0, float(timeout_s)),
            stop_check=stop_check,
            progress_cb=progress_cb,
        )
        if archive_errors:
            notes.append(f"archive_download_errors={len(archive_errors)}")
            append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] archive_download_errors coin={coin_u} count={len(archive_errors)}", level="WARNING")

        archive_day_buckets: dict[str, dict[int, dict[str, Any]]] = {}
        for filename, buckets in parsed_archive:
            if stopped():
                break
            _emit(progress_cb, {"stage": "archive_bucket", "day": filename, "total_days": total_planned_days})
            for day_s, candles in sorted(buckets.items()):
                day_d = datetime.strptime(day_s, "%Y-%m-%d").date()
                if day_d < archive_start or day_d > archive_end:
                    continue

                day_bucket = archive_day_buckets.setdefault(day_s, {})
                day_bucket.update(candles)
        parsed_archive.clear()

        if archive_day_buckets and not stopped():
            _enrich_missing_archive_volumes_bulk(
                coin_u,
                archive_day_buckets,
                timeout_s=timeout_s,
                notes=notes,
                stop_check=stop_check,
                progress_cb=progress_cb,
                rest_limiter=rest_limiter,
            )

        archive_days = sorted(archive_day_buckets.items())
        for day_i, (day_s, candles) in enumerate(archive_days, start=1):
            if stopped():
                break
            written = _write_candles_for_day(coin_u, day_s, candles, overwrite=refetch, source_code=SOURCE_CODE_API)
            minutes_written += written
            archive_daily_downloaded += 1
            _emit(progress_cb, {
                "stage": "archive_write",
                "day": day_s,
                "done": day_i,
                "planned": len(archive_days),
                "minutes_written": minutes_written,
                "total_days": total_planned_days,
            })

        # Validate and repair archive-backed complete UTC days, including days whose ZIP failed.
        for day_d in _iter_day_range(archive_start, archive_end):
            if stopped():
                break
            day_s = day_d.strftime("%Y-%m-%d")
            path = _okx_day_path(coin_u, day_s)
            current = _read_day_npz(path, day=day_s)
            if len(current) >= MIN_DAY_CANDLES:
                continue
            _emit(progress_cb, {"stage": "repair", "day": day_s, "total_days": total_planned_days})
            written, fetched, remaining = _repair_missing_minutes_for_day(coin_u, day_s, timeout_s=timeout_s, stop_check=stop_check, rest_limiter=rest_limiter)
            minutes_written += written
            repair_minutes_fetched += fetched
            if remaining:
                notes.append(f"unrepaired_minutes={day_s}:{remaining}")

    if stopped():
        return ImproveBest1mOkxResult(coin_u, d_end.strftime("%Y-%m-%d"), total_planned_days, archive_daily_downloaded, rest_minutes_fetched, repair_minutes_fetched, minutes_written, notes + ["stopped"])

    # Recent days and archive-ineligible days are REST-backed.
    recent_start = max(d_start, min(d_end, archive_complete_cutoff) + timedelta(days=1))
    if recent_start <= d_end and not stopped():
        recent_days = _days_needing_fetch(coin_u, recent_start, d_end, refetch=True)
        for run_start, run_end in _runs_from_days(recent_days):
            _emit(progress_cb, {"stage": "rest_recent", "day": run_start.strftime("%Y-%m-%d"), "total_days": total_planned_days})
            days_data, _pages, errors = _rest_fetch_range(
                coin_u,
                _day_start_ms(run_start),
                _day_start_ms(run_end) + DAY_MS,
                timeout_s=timeout_s,
                limiter=rest_limiter,
                stop_check=stop_check,
                progress_cb=progress_cb,
                stage="rest_recent",
            )
            if errors:
                notes.append(f"rest_recent_errors={len(errors)}")
            for day_s, candles in sorted(days_data.items()):
                written = _write_candles_for_day(coin_u, day_s, candles, overwrite=True, source_code=SOURCE_CODE_API)
                minutes_written += written
                rest_minutes_fetched += len(candles)
                _emit(progress_cb, {"stage": "rest_recent", "day": day_s, "minutes_written": minutes_written, "total_days": total_planned_days})

    result = ImproveBest1mOkxResult(
        coin=coin_u,
        end_date=d_end.strftime("%Y-%m-%d"),
        days_checked=total_planned_days,
        archive_daily_downloaded=archive_daily_downloaded,
        rest_minutes_fetched=rest_minutes_fetched,
        repair_minutes_fetched=repair_minutes_fetched,
        minutes_written=minutes_written,
        notes=notes,
    )
    append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_best_1m] {coin_u} done {result.to_dict()}")
    return result


def update_latest_okx_1m_for_coin(
    *,
    coin: str,
    lookback_days: int = DEFAULT_LATEST_LOOKBACK_DAYS,
    overwrite: bool = True,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """Refresh the last lookback_days for an OKX USDT-SWAP coin via REST."""

    coin_u = _raw_base_from_coin(coin)
    lb = max(1, int(lookback_days))
    now_utc = datetime.now(tz=timezone.utc)
    d_end = now_utc.date()
    d_start = d_end - timedelta(days=lb)
    since_ms = _day_start_ms(d_start)
    end_ms = int(now_utc.timestamp() * 1000) + 120_000
    append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_latest_1m] start coin={coin_u} lookback={lb}d ({d_start} -> {d_end})")

    try:
        days_data, pages, errors = _rest_fetch_range(
            coin_u,
            since_ms,
            end_ms,
            timeout_s=timeout_s,
            workers=max(1, min(REST_WORKERS, 4)),
            stage="latest",
        )
        minutes_written = 0
        for day_s, candles in sorted(days_data.items()):
            d_day = datetime.strptime(day_s, "%Y-%m-%d").date()
            do_overwrite = bool(overwrite and (d_end - d_day).days <= lb)
            minutes_written += _write_candles_for_day(coin_u, day_s, candles, overwrite=do_overwrite, source_code=SOURCE_CODE_API)
        result = {
            "coin": coin_u,
            "lookback_days": lb,
            "pages": int(pages),
            "days_fetched": len(days_data),
            "minutes_written": int(minutes_written),
            "errors": len(errors),
            "result": "ok" if not errors else "partial",
        }
        append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_latest_1m] done coin={coin_u} {result}", level="INFO")
        return result
    except Exception as exc:
        append_exchange_download_log(STORAGE_EXCHANGE, f"[okx_latest_1m] error coin={coin_u} err={exc}", level="WARNING")
        return {
            "coin": coin_u,
            "lookback_days": lb,
            "result": "error",
            "error": str(exc),
            "minutes_written": 0,
        }


def get_newest_day(coin: str) -> str | None:
    """Return newest day (YYYYMMDD) with data, or None."""

    days = _list_existing_days(coin)
    return days[-1].strftime("%Y%m%d") if days else None


def get_oldest_day(coin: str) -> str | None:
    """Return oldest day (YYYYMMDD) with data, or None."""

    days = _list_existing_days(coin)
    return days[0].strftime("%Y%m%d") if days else None
