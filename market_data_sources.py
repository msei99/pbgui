from __future__ import annotations

import os
import struct
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

if os.name == "posix":
    import fcntl
else:
    fcntl = None


SOURCE_CODE_MISSING = 0
SOURCE_CODE_API = 1
SOURCE_CODE_L2BOOK = 2
SOURCE_CODE_OTHER = 3

SOURCE_LABELS = {
    SOURCE_CODE_API: "api",
    SOURCE_CODE_L2BOOK: "l2Book_mid",
    SOURCE_CODE_OTHER: "other_exchange",
}

MAGIC = b"PBGS"
VERSION = 1
BITS_PER_MIN = 2
DAY_MINUTES = 1440
DAY_BYTES = 360
HEADER_FMT = "<4sBBHII"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


def _day_to_int(day_str: str) -> int:
    s = str(day_str or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        s = s.replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"invalid day: {day_str}")
    return int(s)


def _int_to_date(day_int: int) -> date:
    return datetime.strptime(str(int(day_int)), "%Y%m%d").date()


def _date_to_int(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def get_source_index_path(exchange: str, coin: str) -> Path:
    ex = str(exchange or "").strip().lower()
    if not ex:
        raise ValueError("exchange is empty")
    cn = str(coin or "").strip().upper()
    if not cn:
        raise ValueError("coin is empty")
    base = Path(__file__).resolve().parent / "data" / "ohlcv" / ex
    return base / "1m_src" / cn / "sources.idx"


def _read_index(path: Path) -> tuple[int, int, bytearray] | None:
    if not path.exists():
        return None
    raw = path.read_bytes()
    if len(raw) < HEADER_SIZE:
        return None
    magic, ver, bits, _reserved, base_day, day_count = struct.unpack_from(HEADER_FMT, raw, 0)
    if magic != MAGIC or ver != VERSION or bits != BITS_PER_MIN:
        return None
    expected = HEADER_SIZE + (int(day_count) * DAY_BYTES)
    if len(raw) < expected:
        return None
    data = bytearray(raw[HEADER_SIZE:expected])
    return (int(base_day), int(day_count), data)


def _write_index(path: Path, base_day: int, day_count: int, data: bytearray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = struct.pack(HEADER_FMT, MAGIC, VERSION, BITS_PER_MIN, 0, int(base_day), int(day_count))
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(header)
        f.write(data)
    os.replace(tmp, path)


@contextmanager
def _index_write_lock(path: Path):
    lock_path = path.with_name(path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as lock_fh:
        if fcntl is not None:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


def _ensure_range(
    base_day: int,
    day_count: int,
    data: bytearray,
    target_day: int,
) -> tuple[int, int, bytearray, int]:
    base_date = _int_to_date(base_day)
    target_date = _int_to_date(target_day)
    day_index = (target_date - base_date).days
    if day_index < 0:
        prepend_days = -day_index
        data = bytearray(b"\x00" * (prepend_days * DAY_BYTES)) + data
        base_day = target_day
        day_count = int(day_count) + prepend_days
        day_index = 0
    elif day_index >= day_count:
        append_days = (day_index - day_count) + 1
        data.extend(b"\x00" * (append_days * DAY_BYTES))
        day_count = int(day_count) + append_days
    return (base_day, day_count, data, int(day_index))


def update_source_index_for_day(
    *,
    exchange: str,
    coin: str,
    day: str,
    minute_indices: Iterable[int],
    code: int,
) -> None:
    if int(code) < 0 or int(code) > 3:
        return
    path = get_source_index_path(exchange, coin)
    target_day = _day_to_int(day)

    with _index_write_lock(path):
        existing = _read_index(path)
        if existing is None:
            base_day = target_day
            day_count = 1
            data = bytearray(b"\x00" * DAY_BYTES)
            day_index = 0
        else:
            base_day, day_count, data = existing
            base_day, day_count, data, day_index = _ensure_range(base_day, day_count, data, target_day)

        day_offset = day_index * DAY_BYTES
        for minute in minute_indices:
            try:
                m = int(minute)
            except Exception:
                continue
            if m < 0 or m >= DAY_MINUTES:
                continue
            byte_index = day_offset + (m // 4)
            shift = (m % 4) * 2
            cur = data[byte_index]
            data[byte_index] = (cur & ~(0x03 << shift)) | ((int(code) & 0x03) << shift)

        _write_index(path, base_day, day_count, data)


def get_source_minutes_for_range(
    *,
    exchange: str,
    coin: str,
    start_day: str | None = None,
    end_day: str | None = None,
) -> dict[str, dict[str, dict[int, str]]]:
    path = get_source_index_path(exchange, coin)
    existing = _read_index(path)
    if existing is None:
        return {}

    base_day, day_count, data = existing
    base_date = _int_to_date(base_day)
    last_date = base_date + timedelta(days=day_count - 1)

    try:
        s0 = _day_to_int(start_day) if start_day else base_day
    except Exception:
        s0 = base_day
    try:
        s1 = _day_to_int(end_day) if end_day else _date_to_int(last_date)
    except Exception:
        s1 = _date_to_int(last_date)

    start_date = _int_to_date(s0)
    end_date = _int_to_date(s1)
    if end_date < start_date:
        return {}

    out: dict[str, dict[str, dict[int, str]]] = {}
    cur = start_date
    while cur <= end_date:
        day_index = (cur - base_date).days
        if 0 <= day_index < day_count:
            day_offset = day_index * DAY_BYTES
            day_key = cur.strftime("%Y%m%d")
            for minute in range(DAY_MINUTES):
                byte_index = day_offset + (minute // 4)
                shift = (minute % 4) * 2
                code = (data[byte_index] >> shift) & 0x03
                if code == SOURCE_CODE_MISSING:
                    continue
                label = SOURCE_LABELS.get(int(code))
                if not label:
                    continue
                hour = minute // 60
                mm = minute % 60
                hour_s = f"{hour:02d}"
                out.setdefault(day_key, {}).setdefault(hour_s, {})[mm] = label
        cur = cur + timedelta(days=1)

    return out


def get_source_index_day_range(*, exchange: str, coin: str) -> tuple[str, str] | None:
    """Return (oldest_day, newest_day) from source index as YYYYMMDD."""

    path = get_source_index_path(exchange, coin)
    existing = _read_index(path)
    if existing is None:
        return None

    base_day, day_count, _data = existing
    if int(day_count) <= 0:
        return None

    try:
        d0 = _int_to_date(base_day)
        d1 = d0 + timedelta(days=int(day_count) - 1)
        return (d0.strftime("%Y%m%d"), d1.strftime("%Y%m%d"))
    except Exception:
        return None


def get_oldest_day_with_source_code(*, exchange: str, coin: str, code: int) -> str | None:
    """Return oldest YYYYMMDD day containing at least one minute with given source code."""

    try:
        code_i = int(code)
    except Exception:
        return None
    if code_i < 0 or code_i > 3:
        return None

    path = get_source_index_path(exchange, coin)
    existing = _read_index(path)
    if existing is None:
        return None

    base_day, day_count, data = existing
    if int(day_count) <= 0:
        return None

    base_date = _int_to_date(base_day)
    for day_index in range(int(day_count)):
        day_offset = day_index * DAY_BYTES
        found = False
        for minute in range(DAY_MINUTES):
            byte_index = day_offset + (minute // 4)
            shift = (minute % 4) * 2
            minute_code = (data[byte_index] >> shift) & 0x03
            if int(minute_code) == code_i:
                found = True
                break
        if found:
            day = base_date + timedelta(days=day_index)
            return day.strftime("%Y%m%d")

    return None


def get_daily_source_counts_for_range(
    *,
    exchange: str,
    coin: str,
    start_day: str | None = None,
    end_day: str | None = None,
    lag_minutes: int = 0,
    cutoff_ts_ms: int | None = None,
) -> dict[str, dict[str, int]]:
    path = get_source_index_path(exchange, coin)
    existing = _read_index(path)
    if existing is None:
        return {}

    base_day, day_count, data = existing
    base_date = _int_to_date(base_day)
    last_date = base_date + timedelta(days=day_count - 1)

    try:
        s0 = _day_to_int(start_day) if start_day else base_day
    except Exception:
        s0 = base_day
    try:
        s1 = _day_to_int(end_day) if end_day else _date_to_int(last_date)
    except Exception:
        s1 = _date_to_int(last_date)

    start_date = _int_to_date(s0)
    end_date = _int_to_date(s1)
    if end_date < start_date:
        return {}

    out: dict[str, dict[str, int]] = {}
    try:
        lag_min = max(0, int(lag_minutes))
    except Exception:
        lag_min = 0
    effective_now_utc: datetime
    if cutoff_ts_ms is not None:
        try:
            effective_now_utc = datetime.utcfromtimestamp(int(cutoff_ts_ms) / 1000.0)
        except Exception:
            effective_now_utc = datetime.utcnow()
    else:
        effective_now_utc = datetime.utcnow()
    if lag_min > 0:
        effective_now_utc = effective_now_utc - timedelta(minutes=lag_min)
    effective_day_utc = effective_now_utc.date()
    effective_minute_idx = (int(effective_now_utc.hour) * 60) + int(effective_now_utc.minute)
    cur = start_date
    while cur <= end_date:
        day_index = (cur - base_date).days
        if 0 <= day_index < day_count:
            day_offset = day_index * DAY_BYTES
            counts = {0: 0, 1: 0, 2: 0, 3: 0}
            block = data[day_offset:day_offset + DAY_BYTES]
            for b in block:
                counts[b & 0x03] += 1
                counts[(b >> 2) & 0x03] += 1
                counts[(b >> 4) & 0x03] += 1
                counts[(b >> 6) & 0x03] += 1

            if cur > effective_day_utc:
                counts[0] = 0
            elif cur == effective_day_utc and effective_minute_idx < (DAY_MINUTES - 1):
                future_missing = 0
                for minute in range(effective_minute_idx + 1, DAY_MINUTES):
                    byte_index = minute // 4
                    shift = (minute % 4) * 2
                    code = (block[byte_index] >> shift) & 0x03
                    if code == SOURCE_CODE_MISSING:
                        future_missing += 1
                if future_missing > 0:
                    counts[0] = max(0, int(counts[0]) - int(future_missing))

            day_key = cur.strftime("%Y%m%d")
            out[day_key] = {
                "missing": int(counts[0]),
                "api": int(counts[1]),
                "l2Book_mid": int(counts[2]),
                "other_exchange": int(counts[3]),
            }
        cur = cur + timedelta(days=1)

    return out


def get_source_codes_for_day(
    *,
    exchange: str,
    coin: str,
    day: str,
) -> list[int] | None:
    path = get_source_index_path(exchange, coin)
    existing = _read_index(path)
    if existing is None:
        return None

    base_day, day_count, data = existing
    base_date = _int_to_date(base_day)
    target_day = _day_to_int(day)
    target_date = _int_to_date(target_day)
    day_index = (target_date - base_date).days
    if day_index < 0 or day_index >= day_count:
        return None

    day_offset = day_index * DAY_BYTES
    block = data[day_offset:day_offset + DAY_BYTES]
    codes: list[int] = []
    for b in block:
        codes.append(b & 0x03)
        codes.append((b >> 2) & 0x03)
        codes.append((b >> 4) & 0x03)
        codes.append((b >> 6) & 0x03)
    return codes[:DAY_MINUTES]


def remove_days_from_index(
    *,
    exchange: str,
    coin: str,
    days_to_remove: set[str],
) -> int:
    """
    Remove specific days from an existing source index by zeroing out their data.
    Much faster than rebuilding from scratch.
    Returns the number of days removed.
    """
    if not days_to_remove:
        return 0

    index_path = get_source_index_path(exchange, coin)

    with _index_write_lock(index_path):
        existing = _read_index(index_path)
        if existing is None:
            return 0

        base_day, day_count, data = existing
        base_date = _int_to_date(base_day)

        removed_count = 0
        for day_str in days_to_remove:
            try:
                target_day = _day_to_int(day_str)
                target_date = _int_to_date(target_day)
                day_index = (target_date - base_date).days

                if 0 <= day_index < day_count:
                    day_offset = day_index * DAY_BYTES
                    for i in range(DAY_BYTES):
                        data[day_offset + i] = 0x00
                    removed_count += 1
            except Exception:
                continue

        if removed_count > 0:
            _write_index(index_path, base_day, day_count, data)

        return removed_count
