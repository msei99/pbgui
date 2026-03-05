"""
Pure tradfi (TradFi / stock-perp) helper functions.

No Streamlit dependency — safe to import from FastAPI endpoints.
"""
from __future__ import annotations

import calendar
import json
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta, timezone as _timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    _ZoneInfo = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def tradfi_map_path() -> Path:
    """Return path to the TradFi symbol map JSON."""
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "tradfi_symbol_map.json"


def load_tradfi_map() -> list:
    """Load the TradFi symbol map, returns empty list on missing/broken file."""
    path = tradfi_map_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Stock-perp detection
# ---------------------------------------------------------------------------

def is_hyperliquid_stock_perp_1m(*, exchange: str, dataset: str, coin: str) -> bool:
    """Return True if the coin is a Hyperliquid stock-perp 1m dataset."""
    ex_l = str(exchange or "").strip().lower()
    ds_l = str(dataset or "").strip().lower()
    coin_u = str(coin or "").strip().upper()
    if ex_l != "hyperliquid":
        return False
    if ds_l not in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
        return False
    return coin_u.startswith("XYZ:") or coin_u.startswith("XYZ-")


# ---------------------------------------------------------------------------
# US equity session helpers
# ---------------------------------------------------------------------------

def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> _date:
    first = _date(year, month, 1)
    delta = (int(weekday) - int(first.weekday())) % 7
    return first + _timedelta(days=delta + (max(1, int(n)) - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> _date:
    last_dom = calendar.monthrange(year, month)[1]
    d = _date(year, month, last_dom)
    while int(d.weekday()) != int(weekday):
        d = d - _timedelta(days=1)
    return d


def _easter_sunday(year: int) -> _date:
    """Anonymous Gregorian algorithm for Easter Sunday."""
    a = int(year) % 19
    b = int(year) // 100
    c = int(year) % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return _date(int(year), int(month), int(day))


def is_us_market_holiday(day: _date) -> bool:
    """Return True if *day* is a US equity market holiday."""
    y = int(day.year)
    fixed: set[_date] = set()
    for m, d in ((1, 1), (6, 19), (7, 4), (12, 25)):
        if m == 6 and y < 2021:
            continue
        h = _date(y, m, d)
        if h.weekday() == 5:
            h = h - _timedelta(days=1)
        elif h.weekday() == 6:
            h = h + _timedelta(days=1)
        fixed.add(h)
    floating: set[_date] = {
        _nth_weekday_of_month(y, 1, 0, 3),   # MLK Day
        _nth_weekday_of_month(y, 2, 0, 3),   # Presidents' Day
        _last_weekday_of_month(y, 5, 0),     # Memorial Day
        _nth_weekday_of_month(y, 9, 0, 1),   # Labor Day
        _nth_weekday_of_month(y, 11, 3, 4),  # Thanksgiving
    }
    floating.add(_easter_sunday(y) - _timedelta(days=2))  # Good Friday
    return day in fixed or day in floating


def is_us_market_early_close(day: _date) -> bool:
    """Return True if the US market closes early on *day*."""
    if int(day.weekday()) >= 5:
        return False
    if is_us_market_holiday(day):
        return False
    thanksgiving = _nth_weekday_of_month(int(day.year), 11, 3, 4)
    if day == (thanksgiving + _timedelta(days=1)):
        return True
    if int(day.month) == 7 and int(day.day) == 3:
        return True
    if int(day.month) == 12 and int(day.day) == 24:
        return True
    return False


def tradfi_expected_minute_indices(day: _date) -> set[int]:
    """US equities regular session (09:30-16:00 ET), converted to UTC."""
    if int(day.weekday()) >= 5:
        return set()
    if _ZoneInfo is None:
        return set(range((14 * 60) + 30, (21 * 60)))
    try:
        et = _ZoneInfo("America/New_York")
        utc = _ZoneInfo("UTC")
        open_dt = _datetime(day.year, day.month, day.day, 9, 30, tzinfo=et).astimezone(utc)
        close_dt = _datetime(day.year, day.month, day.day, 16, 0, tzinfo=et).astimezone(utc)
        start_i = int(open_dt.hour) * 60 + int(open_dt.minute)
        end_excl = int(close_dt.hour) * 60 + int(close_dt.minute)
        if end_excl <= start_i:
            return set()
        return set(range(start_i, end_excl))
    except Exception:
        return set(range((14 * 60) + 30, (21 * 60)))


def tradfi_expected_minute_indices_custom_close(day: _date, *, close_hour: int, close_minute: int = 0) -> set[int]:
    """US equities session with custom close time (DST-aware)."""
    if int(day.weekday()) >= 5:
        return set()
    if _ZoneInfo is None:
        start_i = (14 * 60) + 30
        end_excl = int(close_hour) * 60 + int(close_minute)
        return set(range(start_i, end_excl)) if end_excl > start_i else set()
    try:
        et = _ZoneInfo("America/New_York")
        utc = _ZoneInfo("UTC")
        open_dt = _datetime(day.year, day.month, day.day, 9, 30, tzinfo=et).astimezone(utc)
        close_dt = _datetime(day.year, day.month, day.day, int(close_hour), int(close_minute), tzinfo=et).astimezone(utc)
        start_i = int(open_dt.hour) * 60 + int(open_dt.minute)
        end_excl = int(close_dt.hour) * 60 + int(close_dt.minute)
        if end_excl <= start_i:
            return set()
        return set(range(start_i, end_excl))
    except Exception:
        start_i = (14 * 60) + 30
        end_excl = int(close_hour) * 60 + int(close_minute)
        return set(range(start_i, end_excl)) if end_excl > start_i else set()


def tradfi_canonical_type_for_coin(coin: str) -> str:
    """Return the canonical TradFi type string for a coin (e.g. 'equity_us', 'fx')."""
    key = str(coin or "").strip().upper()
    if not key:
        return ""
    if key.startswith("XYZ:") or key.startswith("XYZ-"):
        key = key[4:].strip()
    for suffix in (
        "/USDC:USDC", "_USDC:USDC", "_USDC_USDC", "USDC",
        "/USDT:USDT", "_USDT:USDT", "_USDT_USDT", "USDT",
    ):
        if key.endswith(suffix):
            key = key[: -len(suffix)]
            break
    key = key.strip(" _:-")
    rows = load_tradfi_map()
    for row in rows:
        if str(row.get("xyz_coin") or "").strip().upper() == key:
            return str(row.get("canonical_type") or "").strip().lower()
    return ""


def uses_us_holiday_calendar(canonical_type: str) -> bool:
    return str(canonical_type or "").strip().lower() in {"equity_us", "etf", "commodity_etf", "index_etf", "commodity"}


def is_tradfi_market_holiday(day: _date, canonical_type: str) -> bool:
    ctype = str(canonical_type or "").strip().lower()
    if ctype == "fx":
        return False
    if uses_us_holiday_calendar(ctype):
        return is_us_market_holiday(day)
    return int(day.weekday()) >= 5


def fx_expected_minute_indices(day: _date) -> set[int]:
    """FX session indices (weekend boundary DST-aware)."""
    cutover_minute_utc = 22 * 60
    if _ZoneInfo is not None:
        try:
            et = _ZoneInfo("America/New_York")
            utc = _ZoneInfo("UTC")
            cutover_dt = _datetime(day.year, day.month, day.day, 17, 0, tzinfo=et).astimezone(utc)
            cutover_minute_utc = (int(cutover_dt.hour) * 60) + int(cutover_dt.minute)
        except Exception:
            cutover_minute_utc = 22 * 60

    wd = int(day.weekday())
    special_open_minute_utc: int | None = None
    special_close_minute_utc: int | None = None
    md = (int(day.month), int(day.day))
    if md == (1, 1):
        special_open_minute_utc = 23 * 60
    elif md == (12, 25):
        special_open_minute_utc = 23 * 60
    elif md in ((12, 24), (12, 31)):
        special_close_minute_utc = 22 * 60

    if wd == 5:  # Saturday
        return set()
    if special_open_minute_utc is not None and wd < 5:
        normal_end_minute_utc = int(cutover_minute_utc - 1) if wd == 4 else 1439
        if int(special_open_minute_utc) > int(normal_end_minute_utc):
            return set()
        return set(range(int(special_open_minute_utc), int(normal_end_minute_utc) + 1))
    if special_close_minute_utc is not None and wd < 5:
        return set(range(0, min(1440, max(0, int(special_close_minute_utc)))))
    if wd == 4:  # Friday 00:00-cutover
        return set(range(0, max(0, int(cutover_minute_utc))))
    if wd == 6:  # Sunday 22:00-23:59
        return set(range(22 * 60, 1440))
    return set(range(1440))


def tradfi_expected_indices_for_type(day: _date, canonical_type: str) -> set[int]:
    """Return expected trading minute indices for a canonical TradFi type."""
    ctype = str(canonical_type or "").strip().lower()
    if ctype == "fx":
        return fx_expected_minute_indices(day)
    if is_tradfi_market_holiday(day, ctype):
        return set()
    if uses_us_holiday_calendar(ctype):
        if is_us_market_early_close(day):
            return tradfi_expected_minute_indices_custom_close(day, close_hour=13, close_minute=0)
    return tradfi_expected_minute_indices(day)


def tradfi_expected_minute_indices_from_session(
    *, day: _date, session_start_ms: int, session_end_ms: int
) -> set[int]:
    """Convert session start/end millisecond timestamps to minute indices for *day*."""
    if int(session_end_ms) < int(session_start_ms):
        return set()
    utc = _ZoneInfo("UTC") if _ZoneInfo is not None else _timezone.utc
    day_start = _datetime(day.year, day.month, day.day, tzinfo=utc)
    day_start_ms = int(day_start.timestamp() * 1000)
    start_idx = max(0, (int(session_start_ms) - day_start_ms) // 60_000)
    end_idx = min(1439, (int(session_end_ms) - day_start_ms) // 60_000)
    if end_idx < start_idx:
        return set()
    return set(range(int(start_idx), int(end_idx) + 1))
