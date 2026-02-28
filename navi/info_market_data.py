import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import calendar

from datetime import date as _date, datetime as _datetime, timedelta as _timedelta, timezone as _timezone
from pathlib import Path
import subprocess
import sys
import os
import signal
import time
import inspect
import json
from typing import Any

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except Exception:
    _ZoneInfo = None

from Exchange import Exchanges
from PBCoinData import CoinData, compute_coin_name
from pbgui_purefunc import load_ini, save_ini

from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    info_popup,
    error_popup,
    is_authenticted,
    get_navi_paths,
    load_symbols_from_ini,
    render_header_with_guide,
    PBGDIR,
)
from logging_view import view_log_filtered

from market_data import (
    load_market_data_config,
    set_enabled_coins,
    summarize_raw_inventory,
    summarize_pb7_cache_inventory,
    get_daily_presence_for_pb7_cache,
    get_daily_hour_coverage_for_dataset,
    get_minute_presence_for_dataset,
    get_exchange_download_log_path,
    append_exchange_download_log,
    get_exchange_raw_root_dir,
    load_aws_profile_credentials,
    save_aws_profile_credentials,
    load_aws_profile_region,
    save_aws_profile_region,
)
from market_data_sources import (
    SOURCE_CODE_API,
    get_daily_source_counts_for_range,
    get_source_codes_for_day,
    remove_days_from_index,
    update_source_index_for_day,
)

from hyperliquid_aws import (
    HYPERLIQUID_AWS_REGION,
    download_hyperliquid_l2book_aws,
    check_hyperliquid_l2book_coin_exists_aws,
    get_hyperliquid_archive_day_range_aws,
    list_hyperliquid_archive_hours_aws,
)

from hyperliquid_best_1m import (
    update_latest_hyperliquid_1m_api_for_coin,
    _load_tradfi_profiles_from_ini,
    probe_tiingo_iex_1m,
    get_tiingo_runtime_usage,
    resolve_tradfi_symbol,
)
from hyperliquid_api import resolve_hyperliquid_coin_name

from task_queue import (
    enqueue_job,
    list_jobs,
    read_worker_pid,
    is_pid_running,
    request_cancel_job,
    retry_failed_job,
    delete_jobs_by_ids,
    clear_worker_pid,
)
from tradfi_sync import load_xyz_spec, fetch_xyz_spec, auto_map_tradfi, fetch_tiingo_meta


def _is_hyperliquid_stock_perp_1m(*, exchange: str, dataset: str, coin: str) -> bool:
    ex_l = str(exchange or "").strip().lower()
    ds_l = str(dataset or "").strip().lower()
    coin_u = str(coin or "").strip().upper()
    if ex_l != "hyperliquid":
        return False
    if ds_l not in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
        return False
    return coin_u.startswith("XYZ:") or coin_u.startswith("XYZ-")


def _tradfi_expected_minute_indices(day: _date) -> set[int]:
    # US equities regular session (09:30-16:00 ET), converted to UTC per day (DST-aware).
    if int(day.weekday()) >= 5:
        return set()
    if _ZoneInfo is None:
        # Conservative fallback when zoneinfo is unavailable: assume standard UTC window.
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


def _tradfi_expected_minute_indices_custom_close(day: _date, *, close_hour: int, close_minute: int = 0) -> set[int]:
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
    # Anonymous Gregorian algorithm
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


def _is_us_market_holiday(day: _date) -> bool:
    y = int(day.year)
    # Fixed-date holidays (observed)
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
        _nth_weekday_of_month(y, 1, 0, 3),    # MLK Day
        _nth_weekday_of_month(y, 2, 0, 3),    # Presidents' Day
        _last_weekday_of_month(y, 5, 0),      # Memorial Day
        _nth_weekday_of_month(y, 9, 0, 1),    # Labor Day
        _nth_weekday_of_month(y, 11, 3, 4),   # Thanksgiving
    }
    floating.add(_easter_sunday(y) - _timedelta(days=2))  # Good Friday

    return day in fixed or day in floating


def _is_us_market_early_close(day: _date) -> bool:
    if int(day.weekday()) >= 5:
        return False
    if _is_us_market_holiday(day):
        return False
    # Day after Thanksgiving (Friday)
    thanksgiving = _nth_weekday_of_month(int(day.year), 11, 3, 4)
    if day == (thanksgiving + _timedelta(days=1)):
        return True
    # Common NYSE early-close dates
    if int(day.month) == 7 and int(day.day) == 3:
        return True
    if int(day.month) == 12 and int(day.day) == 24:
        return True
    return False


def _tradfi_canonical_type_for_coin(coin: str) -> str:
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
    rows = _load_tradfi_map_for_ui()
    for row in rows:
        if str(row.get("xyz_coin") or "").strip().upper() == key:
            return str(row.get("canonical_type") or "").strip().lower()
    return ""


def _uses_us_holiday_calendar(canonical_type: str) -> bool:
    return str(canonical_type or "").strip().lower() in {"equity_us", "etf", "commodity_etf", "index_etf"}


def _is_fx_market_holiday(day: _date) -> bool:
    # FX weekend handling is modeled via expected-session windows, not holiday labels.
    return False


def _is_tradfi_market_holiday(day: _date, canonical_type: str) -> bool:
    ctype = str(canonical_type or "").strip().lower()
    if ctype == "fx":
        return False
    if _uses_us_holiday_calendar(ctype):
        return _is_us_market_holiday(day)
    return int(day.weekday()) >= 5


def _fx_expected_minute_indices(day: _date) -> set[int]:
    # FX weekend boundary from observed Tiingo EUR behavior:
    # - Friday close follows 17:00 New York local time (DST-aware)
    # - Sunday reopen is effectively fixed around 22:00 UTC year-round
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

    # Observed reduced sessions on major FX holidays (UTC minute of day).
    special_open_minute_utc: int | None = None
    special_close_minute_utc: int | None = None
    md = (int(day.month), int(day.day))
    if md == (1, 1):
        special_open_minute_utc = 23 * 60
    elif md == (12, 25):
        special_open_minute_utc = 23 * 60
    elif md in ((12, 24), (12, 31)):
        special_close_minute_utc = 22 * 60

    # UTC session model:
    # => closed window: Fri cutover -> Sun 22:00 UTC
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


def _tradfi_expected_indices_for_type(day: _date, canonical_type: str) -> set[int]:
    ctype = str(canonical_type or "").strip().lower()
    if ctype == "fx":
        return _fx_expected_minute_indices(day)
    if _is_tradfi_market_holiday(day, ctype):
        return set()
    if _uses_us_holiday_calendar(ctype):
        if _is_us_market_early_close(day):
            # 09:30-13:00 ET
            return _tradfi_expected_minute_indices_custom_close(day, close_hour=13, close_minute=0)
    return _tradfi_expected_minute_indices(day)


def _tradfi_expected_minute_indices_from_session(*, day: _date, session_start_ms: int, session_end_ms: int) -> set[int]:
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


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            label = p.name
        out.append((label, str(p)))
    return out


def _format_unix_ts(ts: object) -> str:
    try:
        ts_i = int(float(ts))
    except Exception:
        return str(ts or "")
    if ts_i > 10_000_000_000:
        ts_i = ts_i // 1000
    try:
        return _datetime.fromtimestamp(ts_i).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts or "")

def _parse_day_from_npz_filename(name: str) -> str:
    s = str(name or "").strip()
    if s.lower().endswith(".npz"):
        s = s[:-4]
    if len(s) == 8 and s.isdigit():
        return s
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        raw = f"{s[0:4]}{s[5:7]}{s[8:10]}"
        if len(raw) == 8 and raw.isdigit():
            return raw
    return ""

def _load_ohlcv_from_npz_range(
    *,
    exchange: str,
    dataset: str,
    coin: str,
    start_day: str,
    end_day: str,
):
    import numpy as np
    import pandas as pd

    base = get_exchange_raw_root_dir(exchange) / str(dataset) / str(coin)
    if not base.is_dir():
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    frames: list[pd.DataFrame] = []
    for p in sorted(base.glob("*.npz")):
        day_s = _parse_day_from_npz_filename(p.name)
        if not day_s:
            continue
        if start_day and day_s < start_day:
            continue
        if end_day and day_s > end_day:
            continue
        try:
            with np.load(p) as data:
                arr = data["candles"] if "candles" in data else (data[data.files[0]] if data.files else None)
            if arr is None or len(arr) == 0:
                continue
            names = list(getattr(arr, "dtype", object()).names or [])

            ts_key = "ts" if "ts" in names else ("t" if "t" in names else None)
            o_key = "o" if "o" in names else ("open" if "open" in names else None)
            h_key = "h" if "h" in names else ("high" if "high" in names else None)
            l_key = "l" if "l" in names else ("low" if "low" in names else None)
            c_key = "c" if "c" in names else ("close" if "close" in names else None)
            v_key = (
                "v"
                if "v" in names
                else ("bv" if "bv" in names else ("volume" if "volume" in names else None))
            )

            if ts_key and o_key and h_key and l_key and c_key:
                df = pd.DataFrame(
                    {
                        "ts": arr[ts_key].astype("int64", copy=False),
                        "o": arr[o_key].astype("float64", copy=False),
                        "h": arr[h_key].astype("float64", copy=False),
                        "l": arr[l_key].astype("float64", copy=False),
                        "c": arr[c_key].astype("float64", copy=False),
                        "v": arr[v_key].astype("float64", copy=False) if v_key else 0.0,
                    }
                )
            else:
                arr2 = np.asarray(arr)
                if arr2.ndim != 2 or arr2.shape[1] < 5:
                    continue
                df = pd.DataFrame(
                    {
                        "ts": arr2[:, 0].astype("int64", copy=False),
                        "o": arr2[:, 1].astype("float64", copy=False),
                        "h": arr2[:, 2].astype("float64", copy=False),
                        "l": arr2[:, 3].astype("float64", copy=False),
                        "c": arr2[:, 4].astype("float64", copy=False),
                        "v": arr2[:, 5].astype("float64", copy=False) if arr2.shape[1] > 5 else 0.0,
                    }
                )
            frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["ts", "o", "h", "l", "c"])
    out["ts"] = out["ts"].astype("int64", copy=False)
    out = out.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)
    return out

def _load_ohlcv_from_pb7_cache(
    *,
    exchange: str,
    timeframe: str,
    coin: str,
    start_day: str,
    end_day: str,
):
    """Load OHLCV data from PB7 cache .npy files into a DataFrame."""
    import numpy as np
    import pandas as pd
    from market_data import _get_pb7_root_dir, _parse_pb7_cache_day_from_name

    root = _get_pb7_root_dir()
    if root is None:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    tf = str(timeframe or "1m").strip() or "1m"
    base = root / "caches" / "ohlcv" / str(exchange) / tf / str(coin)
    if not base.is_dir():
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    frames: list[pd.DataFrame] = []
    for p in sorted(base.glob("*.npy")):
        day_s = _parse_pb7_cache_day_from_name(p.name)
        if not day_s:
            continue
        if start_day and day_s < start_day:
            continue
        if end_day and day_s > end_day:
            continue
        try:
            arr = np.load(p)
            if len(arr) == 0:
                continue
            names = list(getattr(arr, "dtype", object()).names or [])
            ts_key = "ts" if "ts" in names else None
            o_key = "o" if "o" in names else None
            h_key = "h" if "h" in names else None
            l_key = "l" if "l" in names else None
            c_key = "c" if "c" in names else None
            v_key = "bv" if "bv" in names else ("v" if "v" in names else None)
            if not (ts_key and o_key and h_key and l_key and c_key):
                continue
            df = pd.DataFrame(
                {
                    "ts": arr[ts_key].astype("int64", copy=False),
                    "o": arr[o_key].astype("float64", copy=False),
                    "h": arr[h_key].astype("float64", copy=False),
                    "l": arr[l_key].astype("float64", copy=False),
                    "c": arr[c_key].astype("float64", copy=False),
                    "v": arr[v_key].astype("float64", copy=False) if v_key else 0.0,
                }
            )
            frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["ts", "o", "h", "l", "c"])
    out["ts"] = out["ts"].astype("int64", copy=False)
    out = out.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)
    return out


def _resample_ohlcv(df, rule: str):
    import pandas as pd

    if df is None or df.empty or not rule or str(rule).strip() == "1min":
        return df
    x = df.copy()
    x["dt"] = pd.to_datetime(x["ts"], unit="ms", utc=True)
    x = x.set_index("dt").sort_index()
    agg = x.resample(str(rule)).agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna(subset=["o", "h", "l", "c"])
    if agg.empty:
        return df
    agg = agg.reset_index()
    agg["ts"] = (agg["dt"].view("int64") // 1_000_000).astype("int64")
    return agg[["ts", "o", "h", "l", "c", "v"]]


def _df_to_columnar(df) -> dict:
    """Convert OHLCV DataFrame to columnar dict of plain Python lists."""
    if df is None or df.empty:
        return {"ts": [], "o": [], "h": [], "l": [], "c": [], "v": []}
    return {
        "ts": [int(x) for x in df["ts"]],
        "o": [round(float(x), 8) for x in df["o"]],
        "h": [round(float(x), 8) for x in df["h"]],
        "l": [round(float(x), 8) for x in df["l"]],
        "c": [round(float(x), 8) for x in df["c"]],
        "v": [round(float(x), 4) for x in df["v"]],
    }


def _build_ohlcv_pyramid(ohlcv_df, total_span_min: float) -> dict:
    """Build multi-resolution candle pyramid.  Only layers appropriate for
    the data span are computed so the resulting JSON stays small enough to
    embed in the browser.

    Full range (years): 1d + 1h  (~2 MB JSON, <1s to compute)
    Selected month:     + 15m, 5m, 1m  (~3 MB total)
    """
    pyramid: dict[str, dict] = {}
    pyramid["1d"] = _df_to_columnar(_resample_ohlcv(ohlcv_df, "1D"))
    pyramid["1h"] = _df_to_columnar(_resample_ohlcv(ohlcv_df, "1h"))
    # 15m and 5m are always included – even for years of data these are
    # compact (e.g. 3 years ≈ 105 K / 315 K rows → ~3 / 10 MB JSON).
    pyramid["15m"] = _df_to_columnar(_resample_ohlcv(ohlcv_df, "15min"))
    pyramid["5m"] = _df_to_columnar(_resample_ohlcv(ohlcv_df, "5min"))
    # 1m raw data can be huge for long ranges (1.5 M rows / 3 years),
    # so only include it for spans ≤ 90 days.
    if total_span_min <= 90 * 24 * 60:
        pyramid["1m"] = _df_to_columnar(ohlcv_df)
    return pyramid


_OHLCV_CHART_TEMPLATE = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
html,body{margin:0;padding:0;background:transparent;overflow:hidden;}
#wrap{position:relative;width:100%;}
#chart{width:100%;}
#tf-ind{position:absolute;top:6px;right:12px;background:rgba(40,40,50,0.85);
  color:#aaa;padding:3px 10px;border-radius:4px;font:13px/1.4 sans-serif;
  z-index:10;pointer-events:none;}
#loading{text-align:center;padding:60px;color:#888;font:14px sans-serif;}
</style></head>
<body>
<div id="loading">Loading chart&#8230;</div>
<div id="wrap" style="display:none;">
  <div id="tf-ind"></div>
  <div id="chart"></div>
</div>
<script>
(function(){
  "use strict";
  var L=/*__DATA__*/null;
  var SVOL=/*__SHOW_VOL__*/false;
  var HV=/*__HEIGHT_VOL__*/620;
  var HN=/*__HEIGHT_NO_VOL__*/460;
  var TFO=["1d","1h","15m","5m","1m"];
  var TFT={"1d":365*864e5,"1h":45*864e5,"15m":10*864e5,"5m":2*864e5,"1m":0};
  var cur=null,sw=false;

  function pick(ms){
    for(var i=0;i<TFO.length;i++){var t=TFO[i];if(L[t]&&ms>=TFT[t])return t;}
    for(var i=TFO.length-1;i>=0;i--)if(L[TFO[i]])return TFO[i];
    return"1d";
  }
  function iso(t){return new Date(t).toISOString();}

  function mkTraces(tf){
    var d=L[tf],x=d.ts.map(iso);
    var tr=[{type:"candlestick",x:x,open:d.o,high:d.h,low:d.l,close:d.c,
      name:"OHLC "+tf,
      increasing:{line:{color:"#26a69a"}},decreasing:{line:{color:"#ef5350"}}}];
    if(SVOL){
      var vc=[];for(var i=0;i<d.o.length;i++)vc.push(d.c[i]>=d.o[i]?"#26a69a":"#ef5350");
      tr.push({type:"bar",x:x,y:d.v,name:"Vol",
        marker:{color:vc},opacity:0.6,showlegend:false,yaxis:"y2"});
    }
    return tr;
  }

  function mkLayout(xr){
    var o={paper_bgcolor:"rgba(0,0,0,0)",plot_bgcolor:"#0e1117",
      font:{color:"#ccc",size:11},margin:{l:55,r:10,t:10,b:30},
      xaxis:{rangeslider:{visible:false},gridcolor:"#222",type:"date"},
      yaxis:{title:"Price",gridcolor:"#222",fixedrange:false},
      legend:{orientation:"h",x:0,y:-0.05,font:{size:11}}};
    if(xr)o.xaxis.range=xr;
    if(SVOL){o.height=HV;o.yaxis.domain=[0.25,1.0];
      o.yaxis2={title:"Volume",gridcolor:"#222",domain:[0,0.2],
        rangemode:"tozero",fixedrange:false};o.bargap=0;
    }else{o.height=HN;}
    return o;
  }

  /* initial render with coarsest TF */
  var itf=pick(Infinity);cur=itf;
  document.getElementById("loading").style.display="none";
  document.getElementById("wrap").style.display="block";
  document.getElementById("tf-ind").textContent=itf;
  var el=document.getElementById("chart");
  Plotly.newPlot(el,mkTraces(itf),mkLayout(null),
    {scrollZoom:true,displayModeBar:true,responsive:true});

  /* on zoom: auto-switch TF */
  var dbt=null;
  el.on("plotly_relayout",function(){
    if(sw)return;clearTimeout(dbt);
    dbt=setTimeout(function(){
      var xr=el.layout.xaxis.range;if(!xr||xr.length<2)return;
      var a=new Date(xr[0]).getTime(),b=new Date(xr[1]).getTime();
      if(!isFinite(a)||!isFinite(b))return;
      var s=Math.abs(b-a),nf=pick(s);
      if(nf!==cur){cur=nf;
        document.getElementById("tf-ind").textContent=nf;
        sw=true;
        Plotly.react(el,mkTraces(nf),mkLayout(xr),
          {scrollZoom:true,displayModeBar:true,responsive:true}
        ).then(function(){sw=false;});
      }
    },150);
  });
})();
</script></body></html>"""


def _build_ohlcv_chart_html(pyramid: dict, show_volume: bool,
                            height_vol: int = 620, height_no_vol: int = 460) -> str:
    """Build self-contained HTML with Plotly.js multi-resolution OHLCV chart."""
    import json as _json
    data_json = _json.dumps(pyramid, separators=(',', ':'))
    html = _OHLCV_CHART_TEMPLATE
    html = html.replace('/*__DATA__*/null', data_json)
    html = html.replace('/*__SHOW_VOL__*/false', 'true' if show_volume else 'false')
    html = html.replace('/*__HEIGHT_VOL__*/620', str(height_vol))
    html = html.replace('/*__HEIGHT_NO_VOL__*/460', str(height_no_vol))
    return html


def _load_market_data_status() -> dict:
    path = Path(__file__).resolve().parents[1] / "data" / "logs" / "market_data_status.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


_HL_STATUS_SYMBOL_TO_COIN_CACHE: dict[str, str] | None = None


def _load_hyperliquid_status_symbol_map() -> dict[str, str]:
    global _HL_STATUS_SYMBOL_TO_COIN_CACHE
    if isinstance(_HL_STATUS_SYMBOL_TO_COIN_CACHE, dict):
        return _HL_STATUS_SYMBOL_TO_COIN_CACHE

    out: dict[str, str] = {}
    try:
        mapping_path = Path(__file__).resolve().parents[1] / "data" / "coindata" / "hyperliquid" / "mapping.json"
        if mapping_path.exists():
            raw = json.loads(mapping_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                for rec in raw:
                    if not isinstance(rec, dict):
                        continue
                    symbol = str(rec.get("symbol") or "").strip().upper()
                    coin = str(rec.get("coin") or "").strip().upper()
                    if symbol and coin and symbol not in out:
                        out[symbol] = coin
    except Exception:
        out = {}

    _HL_STATUS_SYMBOL_TO_COIN_CACHE = out
    return out


def _display_market_data_status_coin(*, exchange: str, coin: object) -> str:
    c = str(coin or "").strip()
    if not c:
        return ""

    ex = str(exchange or "").strip().lower()
    if ex != "hyperliquid":
        return c

    c_u = c.upper()

    # Legacy / low-level Hyperliquid market ids (e.g. "0", "50") -> coin name.
    if c_u.isdigit():
        mapped = _load_hyperliquid_status_symbol_map().get(c_u)
        if mapped:
            return mapped

    # Normalize stock-perp display variants to xyz:TICKER.
    if c_u.startswith("XYZ:") or c_u.startswith("XYZ-"):
        tail = c_u[4:].strip()
        for suffix in ("/USDC:USDC", "_USDC:USDC", "_USDC_USDC", "USDC", "/USDT:USDT", "_USDT:USDT", "_USDT_USDT", "USDT"):
            if tail.endswith(suffix):
                tail = tail[: -len(suffix)]
                break
        tail = tail.strip(" _:-")
        return f"xyz:{tail}" if tail else c

    return c


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


def _supports_fragment_run_every() -> bool:
    try:
        return "run_every" in inspect.signature(st.fragment).parameters
    except Exception:
        return False


def _is_background_refresh_paused() -> bool:
    return False


def _fmt_bytes(n: int | float | None) -> str:
    if n is None:
        return "0 B"
    try:
        val = float(n)
    except Exception:
        return "0 B"
    if val < 1:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while val >= 1024.0 and idx < len(units) - 1:
        val /= 1024.0
        idx += 1
    return f"{val:.2f} {units[idx]}"


def _filter_jobs_by_type(jobs: list[dict], job_types: list[str] | None) -> list[dict]:
    if not job_types:
        return list(jobs)
    allowed = {str(t) for t in job_types}
    return [j for j in jobs if str(j.get("type") or "") in allowed]


def _has_active_jobs(job_types: list[str] | None) -> bool:
    try:
        jobs = list_jobs(states=["pending", "running"], limit=20)
        return bool(_filter_jobs_by_type(jobs, job_types))
    except Exception:
        return False


def _render_jobs_panel(
    *,
    job_types: list[str] | None,
    details_key: str,
    panel_key: str,
    show_worker_controls: bool = False,
    auto_refresh_key: str | None = None,
) -> None:
    if show_worker_controls:
        pass

    try:
        jobs = list_jobs(states=["pending", "running"], limit=20)
        jobs = _filter_jobs_by_type(jobs, job_types)
        pending_jobs = list_jobs(states=["pending"], limit=200)
        pending_jobs = _filter_jobs_by_type(pending_jobs, job_types)
        done_jobs = list_jobs(states=["done"], limit=200)
        done_jobs = _filter_jobs_by_type(done_jobs, job_types)
        failed_jobs = list_jobs(states=["failed"], limit=200)
        failed_jobs = _filter_jobs_by_type(failed_jobs, job_types)
        # --- Summary line (always visible) ---
        st.caption(
            f"Queued/running jobs: {len(jobs)} · Pending: {len(pending_jobs)} · Done: {len(done_jobs)} · Failed: {len(failed_jobs)}"
        )
        if not jobs:
            st.write("No active jobs.")

        # Auto-restart: if there are active jobs but the worker process is dead,
        # restart it automatically so jobs don't get stuck forever.
        if jobs:
            _wp = read_worker_pid()
            if not (_wp and is_pid_running(int(_wp))):
                try:
                    clear_worker_pid()
                    subprocess.Popen(
                        [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        close_fds=True,
                    )
                    append_exchange_download_log(
                        "hyperliquid",
                        "[worker] auto-restart: worker dead but active jobs found",
                        level="WARNING",
                    )
                except Exception:
                    pass

        # --- Progress rows (always visible, above Running expander) ---
        if jobs:
            h1, h2, h3, h4, h5, h6, h7, h9 = st.columns([0.16, 0.13, 0.09, 0.06, 0.17, 0.16, 0.08, 0.05])
            h1.write("id")
            h2.write("type")
            h3.write("status")
            h4.write("coin")
            h5.write("chunk")
            h6.write("progress")
            h7.write("updated_ts")
            h9.write("stop")

        for j in jobs:
            jid = str(j.get("id") or "").strip()
            pr = j.get("progress") if isinstance(j.get("progress"), dict) else {}
            coin = str((pr or {}).get("coin") or "")
            chunk = f"{(pr or {}).get('chunk_start')}→{(pr or {}).get('chunk_end')}" if (pr or {}).get("chunk_start") else ""

            step_i = (pr or {}).get("step")
            total_i = (pr or {}).get("total")
            chunk_done = (pr or {}).get("chunk_done")
            chunk_total = (pr or {}).get("chunk_total")
            pct = 0.0
            try:
                if total_i:
                    if chunk_total and step_i:
                        frac = float(chunk_done or 0) / float(chunk_total)
                        pct = float(max(0, int(step_i) - 1) + frac) / float(total_i)
                    else:
                        pct = float(step_i or 0) / float(total_i)
            except Exception:
                pct = 0.0
            pct = max(0.0, min(1.0, pct))

            dl_t = (pr or {}).get("downloaded_total")
            sk_t = (pr or {}).get("skipped_existing_total")
            fl_t = (pr or {}).get("failed_total")

            dl_b = (pr or {}).get("downloaded_bytes_total")
            sk_b = (pr or {}).get("skipped_existing_bytes_total")
            fl_b = (pr or {}).get("failed_bytes_total")

            c1, c2, c3, c4, c5, c6, c7, c9 = st.columns([0.16, 0.13, 0.09, 0.06, 0.17, 0.16, 0.08, 0.05])
            c1.write(jid)
            c2.write(str(j.get("type") or ""))
            c3.write(str(j.get("status") or ""))
            c4.write(coin)
            c5.write(chunk)
            c6.progress(pct)
            c7.write(str(j.get("updated_ts") or ""))
            with c9:
                if st.button("Stop", key=f"{panel_key}_stop_{jid}"):
                    try:
                        ok2 = request_cancel_job(jid, reason="user stop")
                        if not ok2:
                            st.warning("Job not found.")
                    except Exception as e:
                        st.error(str(e))

        # --- Running expander: detailed job info (download stats, substatus etc.) ---
        def _render_job_details(match: dict) -> None:
            pr = match.get("progress") if isinstance(match.get("progress"), dict) else {}
            coin_local = str((pr or {}).get("coin") or "").strip().upper()
            is_stock_perp = coin_local.startswith("XYZ:") or coin_local.startswith("XYZ-")
            step_i = (pr or {}).get("step")
            total_i = (pr or {}).get("total")
            step_txt = f"{step_i}/{total_i}" if total_i else ""

            dl_t = (pr or {}).get("downloaded_total")
            sk_t = (pr or {}).get("skipped_existing_total")
            fl_t = (pr or {}).get("failed_total")
            totals_txt = ""
            if dl_t is not None or sk_t is not None or fl_t is not None:
                totals_txt = f"d={dl_t or 0} s={sk_t or 0} f={fl_t or 0}"

            dl_b = (pr or {}).get("downloaded_bytes_total")
            sk_b = (pr or {}).get("skipped_existing_bytes_total")
            fl_b = (pr or {}).get("failed_bytes_total")
            bytes_txt = ""
            if dl_b is not None or sk_b is not None or fl_b is not None:
                bytes_txt = f"bytes: d={_fmt_bytes(dl_b)} s={_fmt_bytes(sk_b)} f={_fmt_bytes(fl_b)}"

            line = (step_txt + ("  " if step_txt and totals_txt else "") + totals_txt).strip()
            if bytes_txt:
                line = (line + (" | " if line else "") + bytes_txt).strip()
            stage = str((pr or {}).get("stage") or "")
            mode = str((pr or {}).get("mode") or "")
            chunk_done = (pr or {}).get("chunk_done")
            chunk_total = (pr or {}).get("chunk_total")
            extra_parts: list[str] = []
            if stage:
                extra_parts.append(f"stage={stage}")
            if mode:
                extra_parts.append(f"mode={mode}")
            if chunk_total:
                extra_parts.append(f"chunk={chunk_done or 0}/{chunk_total}")
            merged_parts = []
            if line:
                merged_parts.append(line)
            if extra_parts:
                merged_parts.append(" ".join(extra_parts))
            if merged_parts:
                st.caption(" | ".join(merged_parts))
            sub_day = (pr or {}).get("day")
            sub_hour = (pr or {}).get("hour")
            last_binance_day = (pr or {}).get("last_binance_fill_day")
            if mode == "binance_best_1m":
                if sub_day:
                    st.caption(f"substatus: {stage} day={sub_day}")
            elif stage == "binance_fill" and sub_day:
                st.caption(f"substatus: binance_fill day={sub_day}")
            elif last_binance_day and not is_stock_perp:
                st.caption(f"substatus: binance_fill day={last_binance_day}")
            if (sub_day or sub_hour is not None) and stage != "binance_fill" and mode != "binance_best_1m":
                try:
                    hour_txt = f"{int(sub_hour):02d}" if sub_hour is not None else ""
                except Exception:
                    hour_txt = str(sub_hour)
                if sub_day and hour_txt:
                    if is_stock_perp:
                        st.caption(f"substatus: tradfi {sub_day} {hour_txt}:00")
                    else:
                        st.caption(f"substatus: l2book {sub_day} {hour_txt}:00")
                elif sub_day:
                    if is_stock_perp:
                        st.caption(f"substatus: tradfi {sub_day}")
                    else:
                        st.caption(f"substatus: l2book {sub_day}")
                elif hour_txt:
                    if is_stock_perp:
                        st.caption(f"substatus: tradfi hour {hour_txt}")
                    else:
                        st.caption(f"substatus: l2book hour {hour_txt}")
            if is_stock_perp:
                month_key = str((pr or {}).get("month_key") or "").strip()
                month_day_index = int((pr or {}).get("month_day_index") or 0)
                month_day_total = int((pr or {}).get("month_day_total") or 0)
                tiingo_wait_s = int((pr or {}).get("tiingo_wait_s") or 0)
                tiingo_wait_reason = str((pr or {}).get("tiingo_wait_reason") or "").strip()
                tiingo_ticker = str((pr or {}).get("ticker") or "").strip().upper()
                last_result = (pr or {}).get("last_result") if isinstance((pr or {}).get("last_result"), dict) else {}
                tiingo_month_requests_used = int((last_result or {}).get("tiingo_month_requests_used") or 0)
                if month_key and month_day_total > 0:
                    st.caption(f"substatus: tradfi month {month_key} day {month_day_index}/{month_day_total} (Tiingo loaded monthly, written day-by-day)")
                st.caption(f"substatus: tiingo_month_requests_used={tiingo_month_requests_used}")
                if tiingo_wait_s > 0:
                    reason_txt = tiingo_wait_reason or "rate_limit"
                    ticker_txt = f" ticker={tiingo_ticker}" if tiingo_ticker else ""
                    st.warning(f"⚠️ Tiingo rate-limit{ticker_txt}: waiting {tiingo_wait_s}s (reason={reason_txt})")
            corrupt = (pr or {}).get("corrupt_files")
            if isinstance(corrupt, list) and corrupt:
                st.caption(f"corrupt_files: {len(corrupt)}")
            recent_failed = (pr or {}).get("recent_failed")
            if isinstance(recent_failed, list) and recent_failed:
                st.caption("recent_failed:")
                st.code("\n".join(str(x) for x in recent_failed[:12]))
            recent_keys = (pr or {}).get("recent_keys")
            if isinstance(recent_keys, list) and recent_keys:
                st.caption("recent_keys:")
                st.code("\n".join(str(x) for x in recent_keys[:12]))

        if jobs:
            with st.expander("Running", expanded=False):
                for j in jobs:
                    jid_r = str(j.get("id") or "").strip()
                    status_r = str(j.get("status") or "")
                    upd_ts_r = _format_unix_ts(j.get("updated_ts"))
                    err_r = str(j.get("error") or "")
                    payload_r = j.get("payload") if isinstance(j.get("payload"), dict) else {}
                    coins_r = payload_r.get("coins") if isinstance(payload_r, dict) else None
                    coins_r = coins_r if isinstance(coins_r, list) else []
                    coins_preview = ", ".join(str(c) for c in coins_r[:12])
                    if coins_preview and len(coins_r) > 12:
                        coins_preview += " …"
                    pr_r = j.get("progress") if isinstance(j.get("progress"), dict) else {}
                    lr_r = (pr_r or {}).get("last_result") if isinstance(pr_r, dict) else {}

                    # Status line
                    st.write(
                        f"**{jid_r}** status={status_r}"
                        + (f" updated={upd_ts_r}" if upd_ts_r else "")
                        + (f" error={err_r}" if err_r else "")
                    )
                    if coins_preview:
                        st.caption(f"coins: {coins_preview}")

                    # FX backfill info
                    if isinstance(pr_r, dict) and bool(pr_r.get("fx_backfill_mode")):
                        st.caption(
                            "fx_backfill: "
                            f"direction={pr_r.get('fx_backfill_direction') or 'newest_to_oldest'} "
                            f"empty_chunk_streak={int(pr_r.get('fx_empty_chunk_streak') or 0)} "
                            f"source={pr_r.get('tradfi_source_kind') or 'fx'}"
                        )

                    # Detailed live progress
                    _render_job_details(j)

                    # Last result summary (improve stats + duration)
                    if isinstance(lr_r, dict) and lr_r:
                        dur_s = None
                        try:
                            if lr_r.get("duration_s") is not None:
                                dur_s = int(float(lr_r.get("duration_s") or 0))
                        except Exception:
                            dur_s = None
                        dur_txt = ""
                        if dur_s is not None:
                            h = dur_s // 3600
                            m = (dur_s % 3600) // 60
                            s = dur_s % 60
                            if h > 0:
                                dur_txt = f" duration={h}h {m:02d}m {s:02d}s"
                            elif m > 0:
                                dur_txt = f" duration={m}m {s:02d}s"
                            else:
                                dur_txt = f" duration={s}s"
                        if all(k in lr_r for k in ("days_checked", "l2book_minutes_added", "binance_minutes_filled")):
                            st.caption(
                                "improve: "
                                f"days={lr_r.get('days_checked')} "
                                f"l2book_added={lr_r.get('l2book_minutes_added')} "
                                f"binance_filled={lr_r.get('binance_minutes_filled')} "
                                f"bybit_filled={lr_r.get('bybit_minutes_filled', 0)}"
                                f"{dur_txt}"
                            )
                        elif all(k in lr_r for k in ("days_checked", "tiingo_minutes_filled")):
                            st.caption(
                                "improve: "
                                f"days={lr_r.get('days_checked')} "
                                f"tiingo_filled={lr_r.get('tiingo_minutes_filled', 0)} "
                                f"tiingo_month_requests={lr_r.get('tiingo_month_requests_used', 0)}"
                                f"{dur_txt}"
                            )
                        else:
                            st.caption(f"result: {lr_r}")

                    # Details button – show raw JSON
                    if st.button("Details", key=f"{panel_key}_running_details_{jid_r}"):
                        _job_details_dialog(j)

                    if len(jobs) > 1:
                        st.divider()

        active_expander_key = f"{panel_key}_active_state_expander"

        def _render_state_expander(title: str, state_key: str, state_jobs: list[dict], allow_retry: bool = False) -> None:
            with st.expander(
                f"{title} ({len(state_jobs)})",
                expanded=str(st.session_state.get(active_expander_key) or "") == state_key,
            ):
                if not state_jobs:
                    st.caption(f"No {title.lower()}.")
                    return

                confirm_key = f"{panel_key}_{state_key}_confirm_delete_all"
                if st.session_state.get(confirm_key, False):
                    st.warning(f"Delete all {title.lower()} on this page filter? Click again to confirm.")

                table_nonce_key = f"{panel_key}_{state_key}_table_nonce"
                if table_nonce_key not in st.session_state:
                    st.session_state[table_nonce_key] = 0
                page_jobs = state_jobs
                rows: list[dict] = []
                jobs_by_id: dict[str, dict] = {}
                for j in page_jobs:
                    jid = str(j.get("id") or "").strip()
                    jobs_by_id[jid] = j
                    payload = j.get("payload") if isinstance(j.get("payload"), dict) else {}
                    payload_coins = payload.get("coins") if isinstance(payload, dict) else None
                    payload_coins = payload_coins if isinstance(payload_coins, list) else []
                    payload_coins_clean = [str(c).strip().upper() for c in payload_coins if str(c).strip()]

                    pr = j.get("progress") if isinstance(j.get("progress"), dict) else {}
                    single_coin = str((pr or {}).get("coin") or "").strip().upper()
                    if payload_coins_clean:
                        coins_txt = ", ".join(payload_coins_clean)
                    else:
                        coins_txt = single_coin

                    err = str(j.get("error") or "")
                    err_short = (err[:157] + "...") if len(err) > 160 else err

                    rows.append(
                        {
                            "id": jid,
                            "type": str(j.get("type") or ""),
                            "coins": coins_txt,
                            "updated": _format_unix_ts(j.get("updated_ts")),
                            "error": err_short,
                        }
                    )

                table_height = min(560, max(220, 36 * (len(rows) + 2)))
                import pandas as pd

                df_rows = pd.DataFrame(rows)
                event = st.dataframe(
                    df_rows,
                    use_container_width=True,
                    hide_index=True,
                    height=int(table_height),
                    on_select="rerun",
                    selection_mode="multi-row",
                    key=f"{panel_key}_{state_key}_table_{int(st.session_state.get(table_nonce_key, 0))}",
                )

                sel_indices = event.selection.rows if event and event.selection else []
                selected_ids: list[str] = []
                if sel_indices:
                    st.session_state[active_expander_key] = state_key
                    for idx_raw in sel_indices:
                        try:
                            idx = int(idx_raw)
                        except Exception:
                            continue
                        if 0 <= idx < len(rows):
                            jid = str(rows[idx].get("id") or "")
                            if jid:
                                selected_ids.append(jid)
                selected_jobs = [jobs_by_id[sid] for sid in selected_ids if sid in jobs_by_id]

                if not selected_ids:
                    st.caption("Select one or more rows in the table to use actions.")
                else:
                    st.caption(f"Selected: {len(selected_ids)}")

                if allow_retry:
                    a1, a2, a3, a4 = st.columns([1, 1, 1, 1])
                    if a1.button("Retry", key=f"{panel_key}_retry_selected_{state_key}", disabled=not bool(selected_ids)):
                        try:
                            retried_n = 0
                            for sid in selected_ids:
                                if retry_failed_job(str(sid)):
                                    retried_n += 1
                            if retried_n > 0:
                                pid2 = read_worker_pid()
                                if not (pid2 and is_pid_running(int(pid2))):
                                    clear_worker_pid()
                                    subprocess.Popen(
                                        [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                                        stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL,
                                        close_fds=True,
                                    )
                                st.session_state[active_expander_key] = state_key
                                st.session_state[table_nonce_key] = int(st.session_state.get(table_nonce_key, 0)) + 1
                                st.success(f"Retried {retried_n}/{len(selected_ids)} jobs")
                                st.rerun()
                            else:
                                st.warning("Retry failed: job not found.")
                        except Exception as e:
                            st.error(str(e))
                    if a2.button("Details", key=f"{panel_key}_details_selected_{state_key}", disabled=not bool(selected_ids)):
                        _job_details_dialog(list(selected_jobs))
                    if a3.button("Delete", key=f"{panel_key}_delete_selected_{state_key}", disabled=not bool(selected_ids)):
                        try:
                            deleted_n = delete_jobs_by_ids(selected_ids, states=["failed"])
                            if deleted_n > 0:
                                st.session_state[active_expander_key] = state_key
                                st.session_state[table_nonce_key] = int(st.session_state.get(table_nonce_key, 0)) + 1
                                st.success(f"Deleted {deleted_n}/{len(selected_ids)} jobs")
                                st.rerun()
                            else:
                                st.warning("Delete failed: job not found.")
                        except Exception as e:
                            st.error(str(e))
                    if a4.button("Delete all", key=f"{panel_key}_{state_key}_delete_all"):
                        if not st.session_state.get(confirm_key, False):
                            st.session_state[confirm_key] = True
                            st.session_state[active_expander_key] = state_key
                            st.rerun()
                        else:
                            try:
                                ids = [str(j.get("id") or "").strip() for j in state_jobs if str(j.get("id") or "").strip()]
                                deleted_n = delete_jobs_by_ids(ids, states=[state_key])
                                st.session_state[confirm_key] = False
                                st.session_state[active_expander_key] = state_key
                                st.session_state[table_nonce_key] = int(st.session_state.get(table_nonce_key, 0)) + 1
                                st.success(f"Deleted {deleted_n}/{len(ids)} jobs")
                                st.rerun()
                            except Exception as e:
                                st.session_state[confirm_key] = False
                                st.error(str(e))
                else:
                    b1, b2, b3 = st.columns([1, 1, 1])
                    if b1.button("Details", key=f"{panel_key}_details_selected_{state_key}", disabled=not bool(selected_ids)):
                        _job_details_dialog(list(selected_jobs))
                    if b2.button("Delete", key=f"{panel_key}_delete_selected_{state_key}", disabled=not bool(selected_ids)):
                        try:
                            deleted_n = delete_jobs_by_ids(selected_ids, states=[state_key])
                            if deleted_n > 0:
                                st.session_state[active_expander_key] = state_key
                                st.session_state[table_nonce_key] = int(st.session_state.get(table_nonce_key, 0)) + 1
                                st.success(f"Deleted {deleted_n}/{len(selected_ids)} jobs")
                                st.rerun()
                            else:
                                st.warning("Delete failed: job not found.")
                        except Exception as e:
                            st.error(str(e))
                    if b3.button("Delete all", key=f"{panel_key}_{state_key}_delete_all"):
                        if not st.session_state.get(confirm_key, False):
                            st.session_state[confirm_key] = True
                            st.session_state[active_expander_key] = state_key
                            st.rerun()
                        else:
                            try:
                                ids = [str(j.get("id") or "").strip() for j in state_jobs if str(j.get("id") or "").strip()]
                                deleted_n = delete_jobs_by_ids(ids, states=[state_key])
                                st.session_state[confirm_key] = False
                                st.session_state[active_expander_key] = state_key
                                st.session_state[table_nonce_key] = int(st.session_state.get(table_nonce_key, 0)) + 1
                                st.success(f"Deleted {deleted_n}/{len(ids)} jobs")
                                st.rerun()
                            except Exception as e:
                                st.session_state[confirm_key] = False
                                st.error(str(e))

        _render_state_expander("Pending jobs", "pending", pending_jobs, allow_retry=False)
        _render_state_expander("Failed jobs", "failed", failed_jobs, allow_retry=True)
        _render_state_expander("Done jobs", "done", done_jobs, allow_retry=False)

        if auto_refresh_key and not jobs:
            st.session_state[auto_refresh_key] = False
            st.rerun()

    except Exception as e:
        st.error(str(e))


@st.dialog("Job details", width="large")
def _job_details_dialog(job: dict | list[dict]) -> None:
    if isinstance(job, list):
        st.caption(f"Selected jobs: {len(job)}")
        for idx, item in enumerate(job, start=1):
            jid = str((item or {}).get("id") or "").strip()
            jtype = str((item or {}).get("type") or "").strip()
            status = str((item or {}).get("status") or "").strip()
            st.markdown(f"**{idx}.** id={jid} · type={jtype} · status={status}")
            st.json(item)
            if idx < len(job):
                st.divider()
        return

    jid = str((job or {}).get("id") or "").strip()
    jtype = str((job or {}).get("type") or "").strip()
    status = str((job or {}).get("status") or "").strip()
    st.caption(f"id={jid} · type={jtype} · status={status}")
    st.json(job)


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "Market Data"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="market_data_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]
    default_index = 0
    try:
        target = str(default_topic or "").strip().lower()
        if target:
            for i, lbl in enumerate(labels):
                if target in str(lbl).lower():
                    default_index = i
                    break
    except Exception:
        default_index = 0

    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="market_data_help_sel",
    )
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)
    try:
        base = str(st.get_option("server.baseUrlPath") or "").strip("/")
        prefix = f"/{base}" if base else ""
        st.markdown(
            f"<a href='{prefix}/help' target='_blank'>Open full Help page in new tab</a>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


@st.dialog("📋 XYZ Specs", width="large")
def _tradfi_spec_view_dialog():
    spec_path = Path.cwd() / "data" / "coindata" / "hyperliquid" / "xyz_spec.json"
    if not spec_path.exists():
        st.info("No local spec cache found. Click 'Spec' first to fetch the latest XYZ specs.")
        return

    try:
        raw = json.loads(spec_path.read_text(encoding="utf-8"))
    except Exception as exc:
        st.error(f"Failed to read spec cache: {exc}")
        return

    fetched_at = str((raw or {}).get("fetched_at") or "").strip()
    instruments_raw = (raw or {}).get("instruments") or []
    instruments = [r for r in instruments_raw if isinstance(r, dict)]

    c_spec_meta, c_spec_link = st.columns([0.78, 0.22], vertical_alignment="center")
    with c_spec_meta:
        st.caption(
            f"Source: {spec_path.name} · Fetched at: {fetched_at or 'unknown'} · "
            f"Rows: {len(instruments):,}"
        )
    with c_spec_link:
        st.markdown(
            "<div style='text-align:right;'>"
            "<a href='https://docs.trade.xyz/consolidated-resources/specification-index' target='_blank'>"
            "Original XYZ page"
            "</a></div>",
            unsafe_allow_html=True,
        )

    if not instruments:
        st.info("Spec cache is empty.")
        return

    view_rows: list[dict[str, str]] = []
    for row in instruments:
        coin = str(row.get("xyz_coin") or "").strip().upper()
        ctype = str(row.get("canonical_type") or "").strip()
        instrument = str(row.get("instrument_label") or "").strip()
        desc = str(row.get("description") or "").strip()
        underlying = str(row.get("underlying") or "").strip()
        underlying_href = str(row.get("underlying_href") or "").strip()
        pyth_symbol = str(row.get("pyth_symbol") or "").strip()
        max_leverage = str(row.get("max_leverage") or "").strip()

        if underlying_href.startswith("//"):
            underlying_href = "https:" + underlying_href
        elif underlying_href and "://" in underlying_href and not underlying_href.startswith(("http://", "https://")):
            underlying_href = "https://" + underlying_href.split("://", 1)[1]

        hl_link = f"https://app.hyperliquid.xyz/trade/xyz:{coin}" if coin else ""

        view_rows.append(
            {
                "XYZ": coin,
                "Type": ctype,
                "Instrument": instrument,
                "Description": desc,
                "Underlying": underlying,
                "Max Leverage": max_leverage,
                "Pyth": pyth_symbol,
                "Pyth Link": underlying_href,
                "HL Link": hl_link,
            }
        )

    if not view_rows:
        st.info("No spec rows available.")
        return

    table_height = min(1100, max(540, 34 * (len(view_rows) + 2)))
    st.dataframe(
        view_rows,
        use_container_width=True,
        hide_index=True,
        height=int(table_height),
        column_config={
            "Pyth Link": st.column_config.LinkColumn(
                "Pyth Link",
                display_text="🔗",
                help="Open Pyth insights page",
                width="small",
            ),
            "HL Link": st.column_config.LinkColumn(
                "HL Link",
                display_text=r".+xyz:(.+)",
                help="Open on Hyperliquid",
                width="small",
            ),
        },
    )


# ---- TradFi symbol map helpers ----

def _tradfi_map_path() -> Path:
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "tradfi_symbol_map.json"


def _load_tradfi_map_for_ui() -> list:
    path = _tradfi_map_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_tradfi_map_for_ui(records: list) -> None:
    path = _tradfi_map_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    normalized_records: list = []
    for rec in records or []:
        if not isinstance(rec, dict):
            normalized_records.append(rec)
            continue
        item = dict(rec)
        fx = str(item.get("tiingo_fx_ticker") or "").strip()
        if fx:
            item["tiingo_fx_ticker"] = fx.upper()
        normalized_records.append(item)

    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(normalized_records, indent=4, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _load_xyz_spec_cache_info() -> dict | None:
    """Kept for backwards compat — returns None (cache file no longer used)."""
    return None


_TRADFI_CANONICAL_TYPES = [
    "equity_us", "equity_kr", "equity_jp", "fx", "commodity",
    "commodity_etf", "index_etf", "etf",
]
_TRADFI_STATUSES = ["ok", "alias", "pending", "no_provider", "delisted"]

_TRADFI_STATUSES_SELECTABLE = ["ok", "alias", "pending", "no_provider"]

# Local descriptions for known XYZ symbols — used as fallback for coins not yet in tradfi_symbol_map.json.
_TRADFI_KNOWN_DESCRIPTIONS: dict[str, str] = {
    # Commodities
    "GOLD": "Gold (XAU/USD spot)",
    "SILVER": "Silver (XAG/USD spot)",
    "PLATINUM": "Platinum (XPT/USD spot)",
    "PALLADIUM": "Palladium (XPD/USD spot)",
    "CL": "WTI Crude Oil (WTIJ6 front-month futures)",
    "NATGAS": "Natural Gas (NGJ26 front-month futures)",
    "COPPER": "Copper (HGK6 front-month futures)",
    "ALUMINIUM": "Aluminium (LME spot commodity)",
    "URANIUM": "Uranium (UX spot price)",
    # FX
    "EUR": "Euro / US Dollar FX (EUR/USD)",
    "JPY": "Japanese Yen (USD/JPY rate)",
    "GBP": "British Pound / US Dollar FX (GBP/USD)",
    "DXY": "US Dollar Index (DXY basket)",
    # Indices
    "XYZ100": "XYZ100 index (XYZ stock-perps basket, NMH6/USD oracle)",
    "JP225": "Nikkei 225 index (Japan)",
    "KR200": "KOSPI 200 index (South Korea)",
    # ETF
    "URNM": "Sprott Uranium Miners ETF (NASDAQ: URNM)",
    # Korean equities
    "HYUN": "Hyundai Motor Company (KRX: 005380.KS)",
    "SKHX": "SK Hynix Inc. (KRX: 000660.KS)",
    "SMSN": "Samsung Electronics Co. Ltd. (KRX: 005930.KS)",
    # Japanese equities
    "SOFTBANK": "SoftBank Group Corp. (TYO: 9984)",
    # US equities — for any not yet saved to tradfi_symbol_map.json
    "TSLA": "Tesla Inc. (NASDAQ: TSLA)",
    "NVDA": "NVIDIA Corp. (NASDAQ: NVDA)",
    "AAPL": "Apple Inc. (NASDAQ: AAPL)",
    "MSFT": "Microsoft Corp. (NASDAQ: MSFT)",
    "AMZN": "Amazon.com Inc. (NASDAQ: AMZN)",
    "GOOGL": "Alphabet Inc. Class A (NASDAQ: GOOGL)",
    "META": "Meta Platforms Inc. (NASDAQ: META)",
    "INTC": "Intel Corp. (NASDAQ: INTC)",
    "AMD": "Advanced Micro Devices Inc. (NASDAQ: AMD)",
    "MU": "Micron Technology Inc. (NASDAQ: MU)",
    "PLTR": "Palantir Technologies Inc. (NYSE: PLTR)",
    "ORCL": "Oracle Corp. (NYSE: ORCL)",
    "MSTR": "Strategy Inc. / MicroStrategy (NASDAQ: MSTR)",
    "COIN": "Coinbase Global Inc. (NASDAQ: COIN)",
    "HOOD": "Robinhood Markets Inc. (NASDAQ: HOOD)",
    "NFLX": "Netflix Inc. (NASDAQ: NFLX)",
    "CRCL": "Circle Internet Group Inc. (NYSE: CRCL)",
    "SNDK": "SanDisk Corp. (NASDAQ: SNDK)",
    "RIVN": "Rivian Automotive Inc. (NASDAQ: RIVN)",
    "TSM": "Taiwan Semiconductor Mfg. Co. Ltd. (NYSE: TSM)",
    "BABA": "Alibaba Group Holding Ltd. (NYSE: BABA)",
    "CRWV": "CoreWeave Inc. (NASDAQ: CRWV)",
    "USAR": "USAR / USD (synthetic XYZ instrument)",
}


def _guess_tradfi_canonical_type(xyz_coin: str) -> str:
    u = xyz_coin.upper()
    if u in {"GOLD", "SILVER", "PLATINUM", "PALLADIUM", "CL", "NATGAS", "COPPER", "ALUMINIUM", "URANIUM"}:
        return "commodity"
    if u in {"EUR", "JPY", "GBP", "DXY"}:
        return "fx"
    if u in {"JP225", "KR200", "XYZ100"}:
        return "index"
    if u in {"URNM"}:
        return "commodity_etf"
    if u in {"HYUN", "SKHX", "SMSN"}:
        return "equity_kr"
    if u in {"SOFTBANK"}:
        return "equity_jp"
    return "equity_us"


def _build_merged_tradfi_table() -> list[dict]:
    """Merge mapping.json XYZ coins with tradfi_symbol_map.json.

    Returns one row per XYZ coin (plus any manually-added entries not in mapping.json).
    Rows not yet saved to tradfi_symbol_map.json have ``_in_map=False``.
    canonical_type is taken from the live XYZ spec if available (cached for 24 h),
    falling back to the local heuristic.
    """
    # Load live spec from cache (fast — no network call if cache is fresh)
    spec_list = load_xyz_spec()
    spec_by_coin: dict[str, dict] = {}
    if spec_list:
        for s in spec_list:
            coin = str(s.get("xyz_coin") or "").upper()
            if coin:
                spec_by_coin[coin] = s

    # Load XYZ coins from mapping.json
    mapping_path = Path.cwd() / "data" / "coindata" / "hyperliquid" / "mapping.json"
    xyz_coins: dict[str, bool] = {}  # normalized coin name → is_active
    if mapping_path.exists():
        try:
            raw = json.loads(mapping_path.read_text(encoding="utf-8"))
            entries = raw if isinstance(raw, list) else list(raw.values())
            for e in entries:
                if not (e.get("is_hip3") and str(e.get("dex") or "").lower() == "xyz"):
                    continue
                coin_field = str(e.get("coin") or e.get("base") or "").strip()
                if coin_field.upper().startswith("XYZ-"):
                    coin_name = coin_field[4:].upper()
                elif coin_field.upper().startswith("XYZ:"):
                    coin_name = coin_field[4:].upper()
                else:
                    coin_name = coin_field.upper()
                if coin_name:
                    xyz_coins[coin_name] = bool(e.get("active", True))
        except Exception:
            pass

    # Load saved symbol map
    saved_map: dict[str, dict] = {}
    for r in _load_tradfi_map_for_ui():
        key = str(r.get("xyz_coin") or "").upper()
        if key:
            saved_map[key] = r

    rows: list[dict] = []

    # Coins from mapping.json
    for coin_name, is_active in xyz_coins.items():
        if coin_name in saved_map:
            row = dict(saved_map[coin_name])
            row["_in_map"] = True
        else:
            row = {
                "xyz_coin": coin_name,
                "description": _TRADFI_KNOWN_DESCRIPTIONS.get(coin_name, ""),
                "canonical_type": (
                    spec_by_coin[coin_name]["canonical_type"]
                    if coin_name in spec_by_coin
                    else _guess_tradfi_canonical_type(coin_name)
                ),
                "tiingo_ticker": None,
                "tiingo_fx_ticker": None,
                "tiingo_fx_invert": False,
                "tiingo_start_date": None,
                "status": "pending" if is_active else "delisted",
                "note": "",
                "last_verified": None,
                "spec_source": "mapping.json",
                "_in_map": False,
            }
        # Attach Pyth link and Hyperliquid link from spec cache / coin name
        _href = str((spec_by_coin.get(coin_name) or {}).get("underlying_href") or "")
        if _href.startswith("//"):
            _href = "https:" + _href
        elif _href and "://" in _href and not _href.startswith(("http://", "https://")):
            _href = "https://" + _href.split("://", 1)[1]
        row["pyth_link"] = _href
        row["hl_link"] = f"https://app.hyperliquid.xyz/trade/xyz:{coin_name}"
        rows.append(row)

    # Manually-added entries not in mapping.json
    for coin_name, entry in saved_map.items():
        if coin_name not in xyz_coins:
            row = dict(entry)
            row["_in_map"] = True
            _href = str((spec_by_coin.get(coin_name) or {}).get("underlying_href") or "")
            if _href.startswith("//"):
                _href = "https:" + _href
            elif _href and "://" in _href and not _href.startswith(("http://", "https://")):
                _href = "https://" + _href.split("://", 1)[1]
            row["pyth_link"] = _href
            row["hl_link"] = f"https://app.hyperliquid.xyz/trade/xyz:{coin_name}"
            rows.append(row)

    rows.sort(key=lambda r: str(r.get("xyz_coin") or ""))
    # Delisted coins are not actionable — exclude from the table
    rows = [r for r in rows if str(r.get("status") or "").lower() != "delisted"]
    return rows


@st.dialog("Add / Edit TradFi Symbol", width="large")
def _tradfi_map_edit_dialog(mode: str, existing: dict | None = None):
    """Add or edit a tradfi_symbol_map entry. mode: 'add' | 'edit'"""
    ex = existing or {}
    is_edit = mode == "edit"

    xyz_coin_def = str(ex.get("xyz_coin") or "").strip().upper()
    if is_edit:
        st.markdown(f"**Symbol:** `{xyz_coin_def}`")
        xyz_coin = xyz_coin_def
    else:
        xyz_coin = st.text_input(
            "XYZ coin name (without XYZ- prefix)",
            value=xyz_coin_def,
            key="tradfi_edit_xyz_coin",
            help="e.g. TSLA, HYUNDAI, GOLD",
        ).strip().upper()

    can_types = _TRADFI_CANONICAL_TYPES
    can_def = str(ex.get("canonical_type") or "equity_us")
    can_idx = can_types.index(can_def) if can_def in can_types else 0
    canonical_type = st.selectbox("Canonical type", options=can_types, index=can_idx, key="tradfi_edit_ctype")

    description = st.text_input(
        "Description",
        value=str(ex.get("description") or ""),
        key="tradfi_edit_description",
        help="Human-readable name auto-populated by sync (e.g. 'Tesla Inc.'). Helps identify the correct Tiingo ticker.",
    )

    st.divider()
    st.markdown("**Tiingo Equity (IEX)**")
    tiingo_ticker = (
        st.text_input(
            "tiingo_ticker",
            value=str(ex.get("tiingo_ticker") or ""),
            key="tradfi_edit_ticker",
            help="Ticker for Tiingo IEX 1m endpoint. Leave empty if using FX endpoint.",
        ).strip().upper() or None
    )

    st.markdown("**Tiingo FX (Spot)**")
    tiingo_fx_ticker = (
        st.text_input(
            "tiingo_fx_ticker",
            value=str(ex.get("tiingo_fx_ticker") or ""),
            key="tradfi_edit_fx_ticker",
            help="Ticker for Tiingo FX endpoint, e.g. XAUUSD, EURUSD.",
        ).strip().upper() or None
    )
    tiingo_fx_invert = st.checkbox(
        "tiingo_fx_invert (e.g. usdjpy → invert to get price in USD)",
        value=bool(ex.get("tiingo_fx_invert") or False),
        key="tradfi_edit_fx_invert",
    )

    tiingo_start_date = (
        st.text_input(
            "tiingo_start_date (ISO, e.g. 2025-03-28)",
            value=str(ex.get("tiingo_start_date") or ""),
            key="tradfi_edit_start_date",
            help="Symbol listing date from /tiingo/daily/{ticker}. Leave empty if unknown.",
        ).strip() or None
    )

    st.divider()
    statuses = _TRADFI_STATUSES
    stat_def = str(ex.get("status") or "pending")
    stat_idx = statuses.index(stat_def) if stat_def in statuses else 2
    status = st.selectbox("Status", options=statuses, index=stat_idx, key="tradfi_edit_status")

    note = st.text_input(
        "Note",
        value=str(ex.get("note") or ""),
        key="tradfi_edit_note",
        help="Free-text, e.g. 'Hyundai OTC traded as HYMTF'",
    )

    col_save, col_cancel = st.columns(2)
    with col_cancel:
        if st.button("Cancel", key="tradfi_edit_cancel"):
            st.rerun()
    with col_save:
        if st.button("💾 Save", key="tradfi_edit_save", type="primary"):
            key_u = xyz_coin.strip().upper()
            if not key_u:
                st.error("xyz_coin cannot be empty")
                return
            entry = {
                "xyz_coin": key_u,
                "description": str(description),
                "canonical_type": str(canonical_type),
                "tiingo_ticker": tiingo_ticker,
                "tiingo_fx_ticker": tiingo_fx_ticker,
                "tiingo_fx_invert": bool(tiingo_fx_invert),
                "tiingo_start_date": tiingo_start_date,
                "status": str(status),
                "note": str(note),
                "last_verified": _date.today().isoformat(),
                "spec_source": "manual",
            }
            try:
                all_records = _load_tradfi_map_for_ui()
                idx = next(
                    (i for i, r in enumerate(all_records) if str(r.get("xyz_coin") or "").upper() == key_u),
                    None,
                )
                if idx is not None:
                    all_records[idx] = entry
                else:
                    all_records.append(entry)
                _save_tradfi_map_for_ui(all_records)
                st.rerun()
            except Exception as e:
                st.error(f"Save failed: {e}")


# ── TradFi price check + Tiingo search ───────────────────────────────────────

def _tradfi_quote_cache_path() -> Path:
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "tradfi_quote_cache.json"


def _load_tradfi_quote_cache() -> dict:
    path = _tradfi_quote_cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_tradfi_quote_cache(cache: dict) -> None:
    path = _tradfi_quote_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cache, indent=4, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _tiingo_pick_iex_price(quote: dict) -> tuple[float | None, str | None]:
    for field in ("tngoLast", "last", "mid", "prevClose"):
        raw = quote.get(field)
        if raw not in (None, ""):
            try:
                return float(raw), field
            except Exception:
                continue
    return None, None


def _tiingo_pick_fx_price(quote: dict) -> tuple[float | None, str | None]:
    raw_mid = quote.get("midPrice")
    if raw_mid not in (None, ""):
        try:
            return float(raw_mid), "midPrice"
        except Exception:
            pass
    bid = quote.get("bidPrice")
    ask = quote.get("askPrice")
    try:
        if bid not in (None, "") and ask not in (None, ""):
            return (float(bid) + float(ask)) / 2.0, "bidAskMid"
    except Exception:
        pass
    return None, None


def refresh_tradfi_quote_cache(api_key: str, records: list[dict] | None = None) -> dict[str, int]:
    """Refresh quote cache from Tiingo in bulk (IEX all + FX top).

    Returns counts with fetched/saved/used metrics.
    """
    import urllib.request
    import urllib.parse

    rows = records if isinstance(records, list) else _load_tradfi_map_for_ui()
    equity_tickers = {
        str(r.get("tiingo_ticker") or "").upper()
        for r in rows
        if str(r.get("tiingo_ticker") or "").strip()
    }
    fx_tickers = {
        str(r.get("tiingo_fx_ticker") or "").lower()
        for r in rows
        if str(r.get("tiingo_fx_ticker") or "").strip()
    }

    out_quotes: dict[str, dict] = {}
    iex_payload: list[dict] = []
    fx_payload: list[dict] = []

    # 1) IEX bulk snapshot (all tickers in one request)
    try:
        iex_url = f"https://api.tiingo.com/iex?token={urllib.parse.quote(api_key)}"
        iex_req = urllib.request.Request(iex_url, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(iex_req, timeout=30) as resp:
            data = json.loads(resp.read())
        if isinstance(data, list):
            iex_payload = [q for q in data if isinstance(q, dict)]
    except Exception:
        iex_payload = []

    # Keep only mapped equity tickers
    for q in iex_payload:
        ticker = str(q.get("ticker") or "").upper()
        if not ticker or ticker not in equity_tickers:
            continue
        price, field = _tiingo_pick_iex_price(q)
        if price is None:
            continue
        out_quotes[ticker] = {
            "price": price,
            "source": "iex_all",
            "field": field,
            "quote_timestamp": str(q.get("timestamp") or q.get("lastSaleTimestamp") or ""),
        }

    # 2) FX bulk snapshot (only mapped FX tickers)
    if fx_tickers:
        try:
            tickers_csv = ",".join(sorted(fx_tickers))
            fx_url = (
                "https://api.tiingo.com/tiingo/fx/top?"
                f"tickers={urllib.parse.quote(tickers_csv)}&token={urllib.parse.quote(api_key)}"
            )
            fx_req = urllib.request.Request(fx_url, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(fx_req, timeout=20) as resp:
                data = json.loads(resp.read())
            if isinstance(data, list):
                fx_payload = [q for q in data if isinstance(q, dict)]
        except Exception:
            fx_payload = []

    for q in fx_payload:
        ticker = str(q.get("ticker") or "").lower()
        if not ticker or ticker not in fx_tickers:
            continue
        price, field = _tiingo_pick_fx_price(q)
        if price is None:
            continue
        out_quotes[ticker] = {
            "price": price,
            "source": "fx_top",
            "field": field,
            "quote_timestamp": str(q.get("quoteTimestamp") or ""),
        }

    cache = {
        "fetched_at": _datetime.now(_timezone.utc).isoformat(),
        "quotes": out_quotes,
    }
    _save_tradfi_quote_cache(cache)
    return {
        "mapped_equity_tickers": len(equity_tickers),
        "mapped_fx_tickers": len(fx_tickers),
        "iex_rows": len(iex_payload),
        "fx_rows": len(fx_payload),
        "quotes_saved": len(out_quotes),
    }


def _hl_fetch_cached_price_for_xyz(xyz_coin: str, pbgui_dir: Path | None = None) -> float | None:
    """Return latest HL close from local 1m cache (no network)."""
    import numpy as np

    base_dir = (pbgui_dir or Path.cwd()) / "data" / "ohlcv" / "hyperliquid" / "1m"
    coin_upper = xyz_coin.upper()
    coin_dir = base_dir / f"XYZ-{coin_upper}_USDC:USDC"
    if not coin_dir.is_dir():
        return None
    npz_files = sorted(coin_dir.glob("*.npz"))
    if not npz_files:
        return None
    try:
        f = np.load(str(npz_files[-1]))
        data = f[f.files[0]]
        if len(data) > 0:
            return float(data[-1]["c"])
    except Exception:
        return None
    return None


def tiingo_search(query: str, api_key: str, timeout_s: float = 10.0) -> list[dict]:
    """Search Tiingo database. Early Beta — 1 request per call.
    Free-Tier: ~50 requests/day.
    """
    import urllib.request, urllib.parse
    q = urllib.parse.quote(query.strip())
    url = f"https://api.tiingo.com/tiingo/utilities/search/{q}?token={api_key}"
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read())


def _tiingo_fetch_daily_start_date(ticker: str, api_key: str, timeout_s: float = 15.0) -> str | None:
    """Fetch `startDate` for one Tiingo daily ticker."""
    import urllib.request
    import urllib.parse

    t = str(ticker or "").strip().upper()
    token = str(api_key or "").strip()
    if not t or not token:
        return None

    url = f"https://api.tiingo.com/tiingo/daily/{urllib.parse.quote(t)}?token={urllib.parse.quote(token)}"
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            payload = json.loads(resp.read())
    except Exception:
        return None

    if isinstance(payload, dict):
        sd = str(payload.get("startDate") or "").strip()
        return sd[:10] if sd else None
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        sd = str(payload[0].get("startDate") or "").strip()
        return sd[:10] if sd else None
    return None


def _update_tiingo_start_date_for_selected(*, selected_entry: dict | None, api_key: str) -> dict[str, Any]:
    if not selected_entry:
        return {"updated": 0, "reason": "no selection"}

    xyz = str((selected_entry or {}).get("xyz_coin") or "").strip().upper()
    ticker = str((selected_entry or {}).get("tiingo_ticker") or "").strip().upper()
    if not ticker:
        return {"updated": 0, "reason": "selected symbol has no Tiingo equity ticker"}

    start_date = _tiingo_fetch_daily_start_date(ticker=ticker, api_key=api_key)

    if not start_date:
        return {"updated": 0, "reason": f"no startDate for {ticker}"}

    records = _load_tradfi_map_for_ui()
    idx = next((i for i, r in enumerate(records) if str(r.get("xyz_coin") or "").upper() == xyz), None)

    if idx is None:
        row = dict(selected_entry)
        row.pop("_in_map", None)
        row.pop("hl_link", None)
        row.pop("pyth_link", None)
        row["tiingo_start_date"] = start_date
        row["last_verified"] = _datetime.now(_timezone.utc).isoformat()
        records.append(row)
    else:
        records[idx]["tiingo_start_date"] = start_date
        records[idx]["last_verified"] = _datetime.now(_timezone.utc).isoformat()

    _save_tradfi_map_for_ui(records)
    return {
        "updated": 1,
        "xyz_coin": xyz,
        "ticker": ticker,
        "start_date": start_date,
    }


def _update_tiingo_start_dates_for_all(*, api_key: str, rows: list[dict]) -> dict[str, Any]:
    by_xyz: dict[str, dict] = {
        str(r.get("xyz_coin") or "").strip().upper(): dict(r)
        for r in _load_tradfi_map_for_ui()
        if str(r.get("xyz_coin") or "").strip()
    }

    updated = 0
    skipped = 0
    errors = 0

    for row in rows:
        xyz = str(row.get("xyz_coin") or "").strip().upper()
        ticker = str(row.get("tiingo_ticker") or "").strip().upper()
        if not xyz or not ticker:
            skipped += 1
            continue

        existing_start = str(row.get("tiingo_start_date") or "").strip()
        if existing_start:
            skipped += 1
            continue

        start_date = _tiingo_fetch_daily_start_date(ticker=ticker, api_key=api_key)
        if not start_date:
            errors += 1
            continue

        if xyz in by_xyz:
            by_xyz[xyz]["tiingo_start_date"] = start_date
            by_xyz[xyz]["last_verified"] = _datetime.now(_timezone.utc).isoformat()
        else:
            new_row = dict(row)
            new_row.pop("_in_map", None)
            new_row.pop("hl_link", None)
            new_row.pop("pyth_link", None)
            new_row["tiingo_start_date"] = start_date
            new_row["last_verified"] = _datetime.now(_timezone.utc).isoformat()
            by_xyz[xyz] = new_row
        updated += 1

    _save_tradfi_map_for_ui(list(by_xyz.values()))
    return {"updated": updated, "skipped": skipped, "errors": errors}


@st.dialog("🔍 Search Tiingo Ticker", width="large")
def _tiingo_search_dialog(xyz_coin: str, api_key: str):
    """Search Tiingo for a ticker and optionally write it to the map entry."""
    st.markdown(f"Search a Tiingo ticker for **XYZ-{xyz_coin}**")
    st.caption("⚠️ Tiingo Search API (Early Beta) — 1 request per search, free tier ~50/day")
    query = st.text_input("Query", value=xyz_coin, key="_tiingo_search_query")
    if st.button("Search", key="_tiingo_search_go"):
        if not api_key:
            st.error("No Tiingo API key configured. Please set it under 'TradFi / Tiingo'.")
            return
        try:
            results = tiingo_search(query, api_key)
            if not results:
                st.info("No results.")
            else:
                st.markdown(f"**{len(results)} results** (max 5):")
                for r in results[:5]:
                    ticker = str(r.get("ticker") or "")
                    name = str(r.get("name") or "")
                    asset_type = str(r.get("assetType") or "")
                    is_active = r.get("isActive", False)
                    active_lbl = "✅ active" if is_active else "⛔ inactive"
                    col_info, col_use = st.columns([4, 1])
                    with col_info:
                        st.markdown(f"**`{ticker}`** — {name}  \n`{asset_type}` · {active_lbl}")
                    with col_use:
                        if st.button("Apply", key=f"_tiingo_use_{ticker}"):
                            # Write ticker into the map entry
                            all_records = _load_tradfi_map_for_ui()
                            key_u = xyz_coin.upper()
                            idx = next(
                                (i for i, rec in enumerate(all_records)
                                 if str(rec.get("xyz_coin") or "").upper() == key_u),
                                None,
                            )
                            if idx is not None:
                                all_records[idx]["tiingo_ticker"] = ticker
                                all_records[idx]["status"] = "alias"
                                all_records[idx]["note"] = (
                                    str(all_records[idx].get("note") or "") +
                                    f" [Tiingo search: {name}]"
                                ).strip()
                            else:
                                all_records.append({
                                    "xyz_coin": key_u,
                                    "description": name,
                                    "canonical_type": "equity_us",
                                    "tiingo_ticker": ticker,
                                    "tiingo_fx_ticker": None,
                                    "tiingo_fx_invert": False,
                                    "tiingo_start_date": None,
                                    "status": "alias",
                                    "note": f"Tiingo search: {name}",
                                    "last_verified": _date.today().isoformat(),
                                    "spec_source": "manual",
                                })
                            _save_tradfi_map_for_ui(all_records)
                            st.success(f"Ticker `{ticker}` saved.")
                            st.rerun()
        except Exception as exc:
            st.error(f"Tiingo search failed: {exc}")


def _normalize_archive_range(v: object) -> dict[str, str]:
    if isinstance(v, dict):
        oldest = str(v.get("oldest_day") or "").strip()
        newest = str(v.get("newest_day") or "").strip()
        return {"oldest_day": oldest, "newest_day": newest}
    if isinstance(v, (tuple, list)) and len(v) == 2:
        oldest = str(v[0] or "").strip()
        newest = str(v[1] or "").strip()
        return {"oldest_day": oldest, "newest_day": newest}
    return {"oldest_day": "", "newest_day": ""}


def _coin_options_for_exchange(exchange: str) -> list[str]:
    def _filter_hyperliquid_live_meta(coins: list[str]) -> list[str]:
        ex_l = str(exchange or "").strip().lower()
        if ex_l != "hyperliquid":
            return sorted(set(str(c).strip() for c in coins if str(c).strip()))
        out: list[str] = []
        for c in sorted(set(str(v).strip() for v in coins if str(v).strip())):
            cl = c.lower()
            if not (cl.startswith("xyz:") or cl.startswith("xyz-")):
                out.append(c)
                continue
            try:
                resolve_hyperliquid_coin_name(coin=c, timeout_s=5.0)
                out.append(c)
            except Exception:
                continue
        return out

    def _canonical_market_coin(exchange_name: str, coin_value: str) -> str:
        ex_name = str(exchange_name or "").strip().lower()
        value = str(coin_value or "").strip()
        if not value:
            return ""
        if ex_name == "hyperliquid":
            lower = value.lower()
            if lower.startswith("xyz:") or lower.startswith("xyz-"):
                tail = value[4:].strip().upper()
                return f"xyz:{tail}" if tail else ""
        return value.upper()

    try:
        coindata = CoinData()
        approved_coins, _ = coindata.filter_mapping(
            exchange=str(exchange).lower(),
            market_cap_min_m=0,
            vol_mcap_max=float("inf"),
            only_cpt=False,
            notices_ignore=False,
            tags=[],
            quote_filter=None,
            use_cache=True,
            active_only=True,
        )
        approved = {
            _canonical_market_coin(exchange, c)
            for c in approved_coins
            if _canonical_market_coin(exchange, c)
        }
        if approved:
            return _filter_hyperliquid_live_meta(sorted(approved))

        mapping_path = Path(__file__).resolve().parents[1] / "data" / "coindata" / str(exchange).lower() / "mapping.json"
        if mapping_path.exists():
            mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
            mapped_coins = set()
            for row in mapping if isinstance(mapping, list) else []:
                if not bool(row.get("swap", False)) or not bool(row.get("active", True)) or not bool(row.get("linear", True)):
                    continue
                coin = str(row.get("coin") or "").strip()
                if not coin:
                    symbol = str(row.get("ccxt_symbol") or row.get("symbol") or "").strip()
                    quote = str(row.get("quote") or "").strip().upper()
                    if not symbol:
                        continue
                    coin = compute_coin_name(symbol, quote)
                coin = _canonical_market_coin(exchange, coin)
                if coin:
                    mapped_coins.add(coin)
            return _filter_hyperliquid_live_meta(sorted(mapped_coins))

        symbols = load_symbols_from_ini(exchange, "swap")
        fallback_coins = {
            _canonical_market_coin(exchange, s)
            for s in symbols
            if _canonical_market_coin(exchange, s)
        }
        if fallback_coins:
            return _filter_hyperliquid_live_meta(sorted(fallback_coins))
        return []
    except Exception:
        return []


def view_market_data():
    preserve_selection_once = bool(st.session_state.pop("market_data_preserve_selection_once", False))
    # (pause_background_refresh removed – bidirectional component preserves chart state across reruns)

    def _canonical_market_coin(exchange_name: str, coin_value: str) -> str:
        ex_name = str(exchange_name or "").strip().lower()
        value = str(coin_value or "").strip()
        if not value:
            return ""
        if ex_name == "hyperliquid":
            lower = value.lower()
            if lower.startswith("xyz:") or lower.startswith("xyz-"):
                tail = value[4:].strip().upper()
                return f"xyz:{tail}" if tail else ""
        return value.upper()

    exchanges = list(Exchanges.list())
    if "hyperliquid" in exchanges:
        default_exchange = "hyperliquid"
    else:
        default_exchange = exchanges[0] if exchanges else "hyperliquid"

    exchange = st.selectbox(
        "Exchange",
        options=exchanges or [default_exchange],
        index=(exchanges.index(default_exchange) if default_exchange in exchanges else 0),
        key="market_data_exchange",
    )

    cfg = load_market_data_config()
    enabled_default_raw = [
        _canonical_market_coin(exchange, c)
        for c in (cfg.enabled_coins.get(str(exchange).lower(), []) or [])
        if _canonical_market_coin(exchange, c)
    ]

    coin_options = _coin_options_for_exchange(str(exchange))
    option_set = set(coin_options)
    enabled_default = [c for c in enabled_default_raw if c in option_set]
    dropped_defaults = sorted(set(enabled_default_raw) - option_set)
    enabled_key = f"market_data_enabled_{str(exchange).lower()}"

    enabled_preview = enabled_default
    try:
        if enabled_key in st.session_state and isinstance(st.session_state.get(enabled_key), list):
            enabled_preview = [
                _canonical_market_coin(exchange, c)
                for c in (st.session_state.get(enabled_key) or [])
                if _canonical_market_coin(exchange, c) in option_set
            ]
    except Exception:
        enabled_preview = enabled_default

    if str(exchange).lower() == "hyperliquid":
        with st.expander("Settings (Latest 1m Auto-Refresh)", expanded=False):
            st.caption("Configure automatic 1m candle refresh settings. Changes are saved to pbgui.ini and applied automatically in the next cycle (no restart needed).")

            # Apply select/clear action BEFORE multiselect is instantiated
            _hl_action_key = f"hl_coins_action_{enabled_key}"
            _hl_action = st.session_state.pop(_hl_action_key, None)
            if _hl_action == "all":
                st.session_state.pop(enabled_key, None)
                enabled_default = list(coin_options)
            elif _hl_action == "clear":
                st.session_state.pop(enabled_key, None)
                enabled_default = []

            enabled_in_settings = st.multiselect(
                "Enabled coins",
                options=coin_options,
                default=enabled_default,
                key=enabled_key,
            )
            if dropped_defaults:
                st.warning(
                    "Ignored missing saved coins (not in current options): " + ", ".join(dropped_defaults),
                    icon="⚠️",
                )
            _c_sel, _c_clr, _c_cap = st.columns([1, 1, 8])
            with _c_sel:
                if st.button("Select all", key=f"hl_sel_all_{enabled_key}"):
                    st.session_state[_hl_action_key] = "all"
                    st.rerun()
            with _c_clr:
                if st.button("Clear all", key=f"hl_clr_all_{enabled_key}"):
                    st.session_state[_hl_action_key] = "clear"
                    st.rerun()
            with _c_cap:
                st.caption(f"Enabled: {len(enabled_in_settings)} / {len(coin_options)}")
            with _c_cap:
                st.caption(f"Enabled: {len(enabled_in_settings)} / {len(coin_options)}")

            def _read_int_ini(section: str, key: str, default: int) -> int:
                try:
                    v = load_ini(section, key)
                    s = str(v).strip() if v is not None else ''
                    if s == '':
                        return default
                    return int(float(s))
                except Exception:
                    return default

            def _read_float_ini(section: str, key: str, default: float) -> float:
                try:
                    v = load_ini(section, key)
                    s = str(v).strip() if v is not None else ''
                    if s == '':
                        return default
                    return float(s)
                except Exception:
                    return default

            interval_val = _read_int_ini('pbdata', 'latest_1m_interval_seconds', 1800)
            coin_pause_val = _read_float_ini('pbdata', 'latest_1m_coin_pause_seconds', 0.5)
            timeout_val = _read_float_ini('pbdata', 'latest_1m_api_timeout_seconds', 30.0)
            min_lb_val = _read_int_ini('pbdata', 'latest_1m_min_lookback_days', 2)
            max_lb_val = _read_int_ini('pbdata', 'latest_1m_max_lookback_days', 4)

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                st.number_input(
                    'Cycle interval (s)',
                    min_value=60,
                    max_value=3600,
                    value=int(interval_val),
                    step=30,
                    key='md_setting_interval',
                    help='How often to refresh all enabled coins (default: 120s). Increase for many symbols (e.g. 300-600s for all Hyperliquid coins).'
                )
            with c2:
                st.number_input(
                    'Pause between coins (s)',
                    min_value=0.0,
                    max_value=10.0,
                    value=float(coin_pause_val),
                    step=0.1,
                    key='md_setting_coin_pause',
                    help='Pause after each coin to avoid rate limits (default: 0.5s). Increase to 1-2s if seeing 429 errors.'
                )
            with c3:
                st.number_input(
                    'API timeout per coin (s)',
                    min_value=10.0,
                    max_value=120.0,
                    value=float(timeout_val),
                    step=5.0,
                    key='md_setting_timeout',
                    help='Timeout for API request per coin (default: 30s). Increase for slow connections or larger lookback windows.'
                )
            with c4:
                st.number_input(
                    'Min lookback days',
                    min_value=1,
                    max_value=10,
                    value=int(min_lb_val),
                    step=1,
                    key='md_setting_min_lb',
                    help='Minimum lookback window for API fetch (default: 2 days).'
                )
            with c5:
                st.number_input(
                    'Max lookback days',
                    min_value=1,
                    max_value=10,
                    value=int(max_lb_val),
                    step=1,
                    key='md_setting_max_lb',
                    help='Maximum lookback window for API fetch (default: 4 days).'
                )

            st.markdown("**AWS Settings (l2Book)**")
            profile_for_settings = str(st.session_state.get("market_data_hl_aws_profile") or "pbgui-hyperliquid")
            tradfi_profiles = _load_tradfi_profiles_from_ini()
            tiingo_cfg = tradfi_profiles.get("tiingo") if isinstance(tradfi_profiles, dict) else {}
            tiingo_api_key_default = str((tiingo_cfg or {}).get("api_key") or "")

            creds_settings = {}
            try:
                creds_settings = load_aws_profile_credentials(profile_for_settings)
            except Exception:
                creds_settings = {}

            region_default_settings = load_aws_profile_region(profile_for_settings) or HYPERLIQUID_AWS_REGION

            c_p, c_ak, c_sk, c_rg, c_to, c_wk = st.columns([1.3, 1.2, 1.2, 1.0, 0.9, 0.9], vertical_alignment="bottom")
            with c_p:
                st.text_input(
                    "AWS profile name",
                    value=profile_for_settings,
                    key="market_data_hl_aws_profile",
                    help="Named local AWS profile used to store/read credentials for this page.",
                )
            with c_ak:
                st.text_input(
                    "aws_access_key_id",
                    value=str(creds_settings.get("aws_access_key_id") or ""),
                    key="market_data_hl_aws_access_key_id",
                    type="password",
                    help="AWS Access Key ID for Requester-Pays S3 access.",
                )
            with c_sk:
                st.text_input(
                    "aws_secret_access_key",
                    value=str(creds_settings.get("aws_secret_access_key") or ""),
                    key="market_data_hl_aws_secret_access_key",
                    type="password",
                    help="AWS Secret Access Key for the selected profile.",
                )
            with c_rg:
                st.text_input(
                    "AWS region",
                    value=str(st.session_state.get("market_data_hl_aws_region") or region_default_settings),
                    key="market_data_hl_aws_region",
                    help="AWS region for the Hyperliquid archive bucket (default: us-east-2).",
                )
            with c_to:
                st.number_input(
                    "Scan timeout (s)",
                    min_value=0.1,
                    max_value=60.0,
                    value=float(st.session_state.get("market_data_hl_l2book_scan_timeout_s") or _read_float_ini("market_data", "hl_l2book_scan_timeout_s", 5.0)),
                    step=0.5,
                    key="market_data_hl_l2book_scan_timeout_s",
                    help="Timeout per S3 list operation while scanning archive availability.",
                )
            with c_wk:
                st.number_input(
                    "Workers",
                    min_value=1,
                    max_value=64,
                    value=int(st.session_state.get("market_data_hl_l2book_scan_workers") or _read_int_ini("market_data", "hl_l2book_scan_workers", 8)),
                    step=1,
                    key="market_data_hl_l2book_scan_workers",
                    help="Parallel workers used for archive scan checks.",
                )

            st.markdown("**Tiingo Settings (stock-perp)**")
            c_tk, c_tt, c_tl = st.columns([1.05, 0.38, 1.45], vertical_alignment="bottom")
            with c_tk:
                st.text_input(
                    "tiingo_api_key",
                    value=str(st.session_state.get("market_data_tiingo_api_key", tiingo_api_key_default) or ""),
                    key="market_data_tiingo_api_key",
                    type="password",
                    help="Tiingo API token for stock-perp history fetch.",
                )
            tiingo_api_key_current = str(st.session_state.get("market_data_tiingo_api_key") or "").strip()
            with c_tt:
                st.caption("")
                do_test_tiingo = st.button("Test Tiingo", key="market_data_test_tiingo_btn")
            with c_tl:
                st.caption("")
                tiingo_limits = "Limits: 50 req/hour, 1000 req/day, 2 GB/month"
                st.markdown(f"[{tiingo_limits}](https://www.tiingo.com/account/api/usage)")

            st.markdown("[Get free Tiingo API key](https://www.tiingo.com/)")

            if tiingo_api_key_current:
                try:
                    usage = get_tiingo_runtime_usage(api_key=tiingo_api_key_current)
                except Exception:
                    usage = {}

                hour_used = int(usage.get("hour_requests") or 0)
                hour_limit = int(usage.get("hour_limit") or 0)
                hour_remaining = int(usage.get("hour_remaining") or 0)
                day_used = int(usage.get("day_requests") or 0)
                day_limit = int(usage.get("day_limit") or 0)
                day_remaining = int(usage.get("day_remaining") or 0)
                month_used_bytes = int(usage.get("month_bytes") or 0)
                month_limit_bytes = int(usage.get("month_bytes_limit") or 0)
                month_remaining_bytes = int(usage.get("month_bytes_remaining") or 0)

                u1, u2, u3 = st.columns(3)
                hour_ratio = 0.0 if hour_limit <= 0 else min(1.0, max(0.0, float(hour_used) / float(hour_limit)))
                day_ratio = 0.0 if day_limit <= 0 else min(1.0, max(0.0, float(day_used) / float(day_limit)))
                month_ratio = 0.0 if month_limit_bytes <= 0 else min(1.0, max(0.0, float(month_used_bytes) / float(month_limit_bytes)))
                with u1:
                    st.caption(f"Hour: {hour_used}/{hour_limit} used, {hour_remaining} remaining")
                    st.progress(hour_ratio)
                with u2:
                    st.caption(f"Day: {day_used}/{day_limit} used, {day_remaining} remaining")
                    st.progress(day_ratio)
                with u3:
                    st.caption(
                        f"Month bandwidth: {_fmt_bytes(month_used_bytes)}/{_fmt_bytes(month_limit_bytes)} used, {_fmt_bytes(month_remaining_bytes)} remaining"
                    )
                    st.progress(month_ratio)
            else:
                st.caption("Tiingo current usage: set tiingo_api_key to view tracked counters.")

            tiingo_api_key_preview = tiingo_api_key_current

            if do_test_tiingo:
                if not tiingo_api_key_preview:
                    st.error("Tiingo API key is empty.")
                else:
                    try:
                        probe = probe_tiingo_iex_1m(api_key=tiingo_api_key_preview, ticker="AAPL", timeout_s=20.0)
                        st.success(
                            f"Tiingo connection OK: status={probe.get('status', 200)} message={probe.get('message', '')}"
                        )
                    except Exception as e:
                        st.error(f"Tiingo test failed: {e}")

            if st.button('Save', key='md_save_settings_btn'):
                try:
                    set_enabled_coins(exchange, enabled_in_settings)
                    save_ini('pbdata', 'latest_1m_interval_seconds', str(int(st.session_state.get('md_setting_interval', 1800))))
                    save_ini('pbdata', 'latest_1m_coin_pause_seconds', str(float(st.session_state.get('md_setting_coin_pause', 0.5))))
                    save_ini('pbdata', 'latest_1m_api_timeout_seconds', str(float(st.session_state.get('md_setting_timeout', 30.0))))
                    save_ini('pbdata', 'latest_1m_min_lookback_days', str(int(st.session_state.get('md_setting_min_lb', 2))))
                    save_ini('pbdata', 'latest_1m_max_lookback_days', str(int(st.session_state.get('md_setting_max_lb', 4))))
                    ak = str(st.session_state.get("market_data_hl_aws_access_key_id") or "").strip()
                    sk = str(st.session_state.get("market_data_hl_aws_secret_access_key") or "").strip()
                    profile = str(st.session_state.get("market_data_hl_aws_profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
                    region = str(st.session_state.get("market_data_hl_aws_region") or "").strip()
                    save_aws_profile_credentials(profile=profile, aws_access_key_id=ak, aws_secret_access_key=sk)
                    save_aws_profile_region(profile=profile, region=region)
                    timeout_s = float(st.session_state.get("market_data_hl_l2book_scan_timeout_s", 5.0))
                    workers = int(st.session_state.get("market_data_hl_l2book_scan_workers", 8))
                    save_ini("market_data", "hl_l2book_scan_timeout_s", str(timeout_s))
                    save_ini("market_data", "hl_l2book_scan_workers", str(workers))
                    tiingo_key = str(st.session_state.get("market_data_tiingo_api_key") or "").strip()
                    save_ini("tradfi_profiles", "tiingo_api_key", tiingo_key)
                    st.success('✅ Settings saved. Enabled coins and auto-refresh settings are applied automatically in the next refresh cycle.')
                except Exception as e:
                    st.error(f'Failed to save settings: {e}')

    elif str(exchange).lower() == "binance":
        with st.expander("Settings (Binance USDM Latest 1m Auto-Refresh)", expanded=False):
            st.caption(
                "Configure automatic 1m candle refresh settings for Binance USDM. "
                "Changes are saved to pbgui.ini and applied automatically in the next cycle (no restart needed)."
            )

            # Apply select/clear action BEFORE multiselect is instantiated
            _bnc_action_key = f"bnc_coins_action_{enabled_key}"
            _bnc_action = st.session_state.pop(_bnc_action_key, None)
            if _bnc_action == "all":
                st.session_state.pop(enabled_key, None)
                enabled_default = list(coin_options)
            elif _bnc_action == "clear":
                st.session_state.pop(enabled_key, None)
                enabled_default = []

            enabled_in_settings_bnc = st.multiselect(
                "Enabled coins",
                options=coin_options,
                default=enabled_default,
                key=enabled_key,
            )
            if dropped_defaults:
                st.warning(
                    "Ignored missing saved coins (not in current options): " + ", ".join(dropped_defaults),
                    icon="⚠️",
                )
            _bnc_c_sel, _bnc_c_clr, _bnc_c_cap = st.columns([1, 1, 8])
            with _bnc_c_sel:
                if st.button("Select all", key=f"bnc_sel_all_{enabled_key}"):
                    st.session_state[_bnc_action_key] = "all"
                    st.rerun()
            with _bnc_c_clr:
                if st.button("Clear all", key=f"bnc_clr_all_{enabled_key}"):
                    st.session_state[_bnc_action_key] = "clear"
                    st.rerun()
            with _bnc_c_cap:
                st.caption(f"Enabled: {len(enabled_in_settings_bnc)} / {len(coin_options)}")

            def _bnc_read_int_ini(section: str, key: str, default: int) -> int:
                try:
                    v = load_ini(section, key)
                    s = str(v).strip() if v is not None else ''
                    return default if s == '' else int(float(s))
                except Exception:
                    return default

            def _bnc_read_float_ini(section: str, key: str, default: float) -> float:
                try:
                    v = load_ini(section, key)
                    s = str(v).strip() if v is not None else ''
                    return default if s == '' else float(s)
                except Exception:
                    return default

            bnc_interval_val = _bnc_read_int_ini('binance_data', 'latest_1m_interval_seconds', 3600)
            bnc_coin_pause_val = _bnc_read_float_ini('binance_data', 'latest_1m_coin_pause_seconds', 0.5)
            bnc_timeout_val = _bnc_read_float_ini('binance_data', 'latest_1m_api_timeout_seconds', 30.0)
            bnc_min_lb_val = _bnc_read_int_ini('binance_data', 'latest_1m_min_lookback_days', 2)
            bnc_max_lb_val = _bnc_read_int_ini('binance_data', 'latest_1m_max_lookback_days', 7)

            bc1, bc2, bc3, bc4, bc5 = st.columns(5)
            with bc1:
                st.number_input(
                    'Cycle interval (s)',
                    min_value=60, max_value=3600, value=int(bnc_interval_val),
                    step=30, key='bnc_md_setting_interval',
                    help='How often to refresh all enabled coins (default: 120s).',
                )
            with bc2:
                st.number_input(
                    'Pause between coins (s)',
                    min_value=0.0, max_value=10.0, value=float(bnc_coin_pause_val),
                    step=0.1, key='bnc_md_setting_coin_pause',
                    help='Pause after each coin to avoid rate limits (default: 0.5s).',
                )
            with bc3:
                st.number_input(
                    'API timeout per coin (s)',
                    min_value=10.0, max_value=120.0, value=float(bnc_timeout_val),
                    step=5.0, key='bnc_md_setting_timeout',
                    help='Timeout for CCXT request per coin (default: 30s).',
                )
            with bc4:
                st.number_input(
                    'Min lookback days',
                    min_value=1, max_value=10, value=int(bnc_min_lb_val),
                    step=1, key='bnc_md_setting_min_lb',
                    help='Minimum lookback window for API fetch (default: 2 days).',
                )
            with bc5:
                st.number_input(
                    'Max lookback days',
                    min_value=1, max_value=30, value=int(bnc_max_lb_val),
                    step=1, key='bnc_md_setting_max_lb',
                    help='Maximum lookback window for API fetch (default: 7 days).',
                )

            if st.button('Save', key='bnc_save_settings_btn'):
                try:
                    set_enabled_coins(exchange, enabled_in_settings_bnc)
                    save_ini('binance_data', 'latest_1m_interval_seconds', str(int(st.session_state.get('bnc_md_setting_interval', 3600))))
                    save_ini('binance_data', 'latest_1m_coin_pause_seconds', str(float(st.session_state.get('bnc_md_setting_coin_pause', 0.5))))
                    save_ini('binance_data', 'latest_1m_api_timeout_seconds', str(float(st.session_state.get('bnc_md_setting_timeout', 30.0))))
                    save_ini('binance_data', 'latest_1m_min_lookback_days', str(int(st.session_state.get('bnc_md_setting_min_lb', 2))))
                    save_ini('binance_data', 'latest_1m_max_lookback_days', str(int(st.session_state.get('bnc_md_setting_max_lb', 7))))
                    st.success('✅ Settings saved. Applied automatically in the next Binance refresh cycle.')
                except Exception as e:
                    st.error(f'Failed to save settings: {e}')

    main_view = st.segmented_control(
        "",
        options=["Actions", "Already have", "Activity log"],
        default="Actions",
        key="market_data_main_view",
    )

    if main_view == "Actions":
        if str(exchange).lower() == "binance":
            bnc_coin_list = [str(c).strip().upper() for c in enabled_preview if str(c).strip()]

            @st.fragment(run_every=5)
            def _bnc_status_fragment():
                _bnc_flag_path = Path(f"{PBGDIR}/data/logs/binance_latest_1m_run_now.flag")
                _bnc_stop_path = Path(f"{PBGDIR}/data/logs/binance_latest_1m_stop.flag")
                bnc_status_all = _load_market_data_status()
                bnc_status = bnc_status_all.get("binance_latest_1m") if isinstance(bnc_status_all, dict) else {}
                _bnc_queued = _bnc_flag_path.exists()
                _bnc_running = bool(bnc_status.get("running")) if bnc_status else False
                with st.expander("Market Data status (Binance USDM Latest 1m)", expanded=False):
                    _c1, _c2 = st.columns([1, 1])
                    with _c1:
                        if not _bnc_queued:
                            if st.button("⏩ Refresh now", key="bnc_run_now_btn", help="Skip wait and trigger next Binance refresh cycle immediately", use_container_width=True):
                                try:
                                    _bnc_flag_path.touch()
                                    st.toast("Refresh triggered — cycle will start within seconds.")
                                except Exception as _e:
                                    st.error(f"Failed: {_e}")
                        else:
                            if st.button("⏹ Cancel queued refresh", key="bnc_cancel_btn", type="primary", help="Cancel the queued refresh — loop will do the normal wait instead", use_container_width=True):
                                try:
                                    _bnc_flag_path.unlink(missing_ok=True)
                                    st.toast("Queued refresh cancelled.")
                                except Exception as _e:
                                    st.error(f"Failed: {_e}")
                    with _c2:
                        if _bnc_running:
                            if st.button("⏹ Stop current run", key="bnc_stop_btn", type="primary", help="Stop after the current coin finishes", use_container_width=True):
                                try:
                                    _bnc_stop_path.touch()
                                    st.toast("Stop signal sent — run will abort after current coin.")
                                except Exception as _e:
                                    st.error(f"Failed: {_e}")
                    if not bnc_status:
                        st.info("No Binance status yet. Start PBData with Binance enabled to populate status.")
                    else:
                        if bnc_status.get("running"):
                            _done = int(bnc_status.get("coins_done") or 0)
                            _total = int(bnc_status.get("coins_total") or 0)
                            _cur = bnc_status.get("current_coin") or "..."
                            if _total > 0:
                                st.progress(_done / _total, text=f"Running: {_done} / {_total} — current: {_cur}")
                            else:
                                st.info("Running...")
                        bnc_coins_st = bnc_status.get("coins") if isinstance(bnc_status, dict) else {}
                        bnc_interval_s = int(bnc_status.get("interval_seconds") or 0) if isinstance(bnc_status, dict) else 0
                        if isinstance(bnc_coins_st, dict) and bnc_coins_st:
                            bnc_status_rows = []
                            now_bnc = _datetime.now()
                            for coin, cst in sorted(bnc_coins_st.items()):
                                last_fetch = str(cst.get("last_fetch") or "") if isinstance(cst, dict) else ""
                                next_run = ""
                                if bnc_interval_s and last_fetch:
                                    try:
                                        last_dt = _datetime.fromisoformat(last_fetch)
                                        next_run = max(0, int(bnc_interval_s - (now_bnc - last_dt).total_seconds()))
                                    except Exception:
                                        pass
                                api_res = cst.get("api_result") if isinstance(cst, dict) else {}
                                bnc_status_rows.append({
                                    "coin": coin,
                                    "last_fetch": last_fetch,
                                    "result": (cst.get("result") if isinstance(cst, dict) else ""),
                                    "lookback_days": (cst.get("lookback_days") if isinstance(cst, dict) else ""),
                                    "minutes_written": (api_res.get("minutes_written") if isinstance(api_res, dict) else ""),
                                    "next_run_in_s": next_run,
                                    "note": (cst.get("note") or cst.get("error") or "") if isinstance(cst, dict) else "",
                                })
                            st.dataframe(bnc_status_rows, use_container_width=True)
                        else:
                            st.info("No Binance latest 1m status available yet.")
            _bnc_status_fragment()

            with st.expander("Build best 1m OHLCV (Binance USDM)", expanded=False):
                st.caption(
                    "Downloads full 1m OHLCV history from the Binance archive (monthly/daily ZIPs) "
                    "and backfills any gap via CCXT. Runs as a background job."
                )

                bnc_eligible_coins = bnc_coin_list[:]
                if not bnc_eligible_coins:
                    st.warning("No enabled coins. Add coins in Settings above first.")

                bnc_build_options = ["All"] + bnc_eligible_coins if bnc_eligible_coins else []
                bnc_build_sel = st.multiselect(
                    "Coins for build",
                    options=bnc_build_options,
                    default=["All"] if bnc_eligible_coins else [],
                    key="market_data_bnc_best_1m_coins",
                )
                if "All" in bnc_build_sel or not bnc_build_sel:
                    bnc_build_coins = list(bnc_eligible_coins)
                else:
                    bnc_build_coins = [c for c in bnc_build_sel if c in bnc_eligible_coins]

                bnc_c_build, bnc_c_start, bnc_c_end, bnc_c_refetch = st.columns(
                    [0.18, 0.22, 0.22, 0.38], vertical_alignment="bottom"
                )
                with bnc_c_build:
                    bnc_run = st.button("Build best 1m", key="market_data_bnc_best_1m_run", use_container_width=True)
                with bnc_c_start:
                    bnc_start_date = st.date_input(
                        "Start date (optional)", value=None,
                        key="market_data_bnc_best_1m_start_date",
                        help="Optional lower bound. If empty: earliest available data (CCXT inception or 2019-01-01).",
                    )
                with bnc_c_end:
                    bnc_end_date = st.date_input(
                        "End date (optional)", value=None,
                        key="market_data_bnc_best_1m_end_date",
                        help="Optional upper bound. If empty: today is used.",
                    )
                with bnc_c_refetch:
                    bnc_refetch = st.checkbox(
                        "Refetch all days from scratch",
                        value=False,
                        key="market_data_bnc_best_1m_refetch",
                        help="Re-downloads all archive ZIPs and overwrites existing data. Use to fix corrupted days.",
                    )

                if bnc_run:
                    try:
                        if not bnc_build_coins:
                            raise ValueError("No coins selected")
                        if bnc_start_date and bnc_end_date and bnc_start_date > bnc_end_date:
                            raise ValueError("Start date must be on or before End date")
                        bnc_eff_end = bnc_end_date.strftime("%Y%m%d") if bnc_end_date else _date.today().strftime("%Y%m%d")
                        job = enqueue_job(
                            job_type="binance_best_1m",
                            payload={
                                "coins": list(bnc_build_coins),
                                "end_day": bnc_eff_end,
                                "start_day": bnc_start_date.strftime("%Y%m%d") if bnc_start_date else "",
                                "refetch": bool(bnc_refetch),
                            },
                        )
                        append_exchange_download_log("binanceusdm", f"[binance_best_1m] queued job_id={job.job_id}")
                        pid = read_worker_pid()
                        if not (pid and is_pid_running(int(pid))):
                            subprocess.Popen(
                                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                close_fds=True,
                            )
                        st.success(f"Queued background build job: {job.job_id}")
                        st.rerun()
                    except Exception as e:
                        append_exchange_download_log("binanceusdm", f"[binance_best_1m] ERROR {e}")
                        st.error(str(e))

                bnc_jobs_active = _has_active_jobs(["binance_best_1m"])
                if bnc_jobs_active and _supports_fragment_run_every() and not _is_background_refresh_paused():
                    @st.fragment(run_every=2)
                    def _bnc_best_jobs_fragment():
                        _render_jobs_panel(
                            job_types=["binance_best_1m"],
                            details_key="market_data_bnc_best_job_details",
                            panel_key="market_data_bnc_best_jobs",
                            show_worker_controls=True,
                            auto_refresh_key="market_data_bnc_best_auto_refresh",
                        )
                else:
                    @st.fragment
                    def _bnc_best_jobs_fragment():
                        _render_jobs_panel(
                            job_types=["binance_best_1m"],
                            details_key="market_data_bnc_best_job_details",
                            panel_key="market_data_bnc_best_jobs",
                            show_worker_controls=True,
                        )
                _bnc_best_jobs_fragment()

        elif str(exchange).lower() != "hyperliquid":
            st.info("Market Data actions are currently implemented for Hyperliquid and Binance (USDM).")
        else:
            coin_list = [str(c).strip().upper() for c in enabled_preview if str(c).strip()]

            @st.fragment(run_every=5)
            def _hl_status_fragment():
                _hl_flag_path = Path(f"{PBGDIR}/data/logs/hyperliquid_latest_1m_run_now.flag")
                _hl_stop_path = Path(f"{PBGDIR}/data/logs/hyperliquid_latest_1m_stop.flag")
                status = _load_market_data_status()
                _hl_queued = _hl_flag_path.exists()
                _hl_running = bool((status.get("latest_1m") or {}).get("running")) if status else False
                with st.expander("Market Data status", expanded=False):
                    _c1, _c2 = st.columns([1, 1])
                    with _c1:
                        if not _hl_queued:
                            if st.button("⏩ Refresh now", key="hl_run_now_btn", help="Skip wait and trigger next Hyperliquid refresh cycle immediately", use_container_width=True):
                                try:
                                    _hl_flag_path.touch()
                                    st.toast("Refresh triggered — cycle will start within seconds.")
                                except Exception as _e:
                                    st.error(f"Failed: {_e}")
                        else:
                            if st.button("⏹ Cancel queued refresh", key="hl_cancel_btn", type="primary", help="Cancel the queued refresh — loop will do the normal wait instead", use_container_width=True):
                                try:
                                    _hl_flag_path.unlink(missing_ok=True)
                                    st.toast("Queued refresh cancelled.")
                                except Exception as _e:
                                    st.error(f"Failed: {_e}")
                    with _c2:
                        if _hl_running:
                            if st.button("⏹ Stop current run", key="hl_stop_btn", type="primary", help="Stop after the current coin finishes", use_container_width=True):
                                try:
                                    _hl_stop_path.touch()
                                    st.toast("Stop signal sent — run will abort after current coin.")
                                except Exception as _e:
                                    st.error(f"Failed: {_e}")
                    if not status:
                        st.info("No status yet. Start PBData to populate market data status.")
                    else:
                        latest = status.get("latest_1m") if isinstance(status, dict) else {}
                        if latest and latest.get("running"):
                            _done = int(latest.get("coins_done") or 0)
                            _total = int(latest.get("coins_total") or 0)
                            _cur = latest.get("current_coin") or "..."
                            if _total > 0:
                                st.progress(_done / _total, text=f"Running: {_done} / {_total} — current: {_cur}")
                            else:
                                st.info("Running...")
                        latest_coins = latest.get("coins") if isinstance(latest, dict) else {}
                        interval_s = int(latest.get("interval_seconds") or 0) if isinstance(latest, dict) else 0
                        if isinstance(latest_coins, dict) and latest_coins:
                            rows = []
                            now = _datetime.now()
                            for coin, cst in sorted(latest_coins.items()):
                                last_fetch = str(cst.get("last_fetch") or "") if isinstance(cst, dict) else ""
                                next_run = ""
                                if interval_s and last_fetch:
                                    try:
                                        last_dt = _datetime.fromisoformat(last_fetch)
                                        next_run = max(0, int(interval_s - (now - last_dt).total_seconds()))
                                    except Exception:
                                        next_run = ""
                                coin_display = _display_market_data_status_coin(
                                    exchange=str(exchange),
                                    coin=coin,
                                )
                                rows.append(
                                    {
                                        "coin": coin_display,
                                        "last_fetch": last_fetch,
                                        "result": (cst.get("result") if isinstance(cst, dict) else ""),
                                        "lookback_days": (cst.get("lookback_days") if isinstance(cst, dict) else ""),
                                        "newest_day": (cst.get("newest_day") if isinstance(cst, dict) else ""),
                                        "next_run_in_s": next_run,
                                    }
                                )
                            st.dataframe(rows, use_container_width=True)
                        else:
                            st.info("No latest 1m status available yet.")
            _hl_status_fragment()

            tradfi_anchor = st.container()
            download_anchor = st.container()
            build_anchor = st.container()

            if False:
                st.caption(
                    "Automatic refresh for newest data. Merges missing minutes in a small lookback window per coin. "
                    "(Hyperliquid provides only the most recent ~5000 1m candles.)"
                )

                if not coin_list:
                    st.warning("No enabled coins selected.")
                else:
                    st.caption(f"Coins: {', '.join(coin_list[:12])}{' …' if len(coin_list) > 12 else ''}")

                latest_coin_options = ["All"] + coin_list if coin_list else []
                latest_coin_sel = st.multiselect(
                    "Coins for latest update",
                    options=latest_coin_options,
                    default=["All"] if coin_list else [],
                    key="market_data_hl_latest_1m_coins",
                )
                if "All" in latest_coin_sel or not latest_coin_sel:
                    latest_coins = list(coin_list)
                else:
                    latest_coins = [c for c in latest_coin_sel if c in coin_list]

                if st.button("Update latest 1m", key="market_data_hl_latest_1m_run"):
                    try:
                        if not latest_coins:
                            raise ValueError("No enabled coins selected")

                        lookback_days = 7
                        p = st.progress(0)
                        total = max(1, len(latest_coins))

                        append_exchange_download_log(
                            "hyperliquid",
                            f"[hl_latest_1m] bulk start coins={len(latest_coins)} lookback_days={lookback_days}",
                        )

                        for i, coin in enumerate(latest_coins):
                            with st.spinner(f"{coin}: updating latest 1m"):
                                r = update_latest_hyperliquid_1m_api_for_coin(
                                    coin=coin,
                                    lookback_days=int(lookback_days),
                                    overwrite=False,
                                    dry_run=False,
                                )
                            append_exchange_download_log("hyperliquid", f"[INFO] [hl_latest_1m] result {r}")
                            p.progress(min(1.0, (i + 1) / float(total)))

                        st.success("Latest 1m update finished.")
                    except Exception as e:
                        append_exchange_download_log("hyperliquid", f"[hl_latest_1m] ERROR {e}")
                        st.error(str(e))

            with build_anchor.expander("Build best 1m OHLCV", expanded=False):
                def _extract_xyz_coin_name(coin: str) -> str | None:
                    c_u = str(coin or "").strip().upper()
                    if not c_u:
                        return None
                    if c_u.startswith("XYZ:") or c_u.startswith("XYZ-"):
                        tail = c_u[4:].strip()
                    else:
                        # Some enabled coin lists store stock-perp tickers without XYZ prefix
                        # (e.g. "EUR"). Treat as candidate XYZ coin name.
                        tail = c_u
                    for suffix in (
                        "/USDC:USDC", "_USDC:USDC", "_USDC_USDC", "USDC",
                        "/USDT:USDT", "_USDT:USDT", "_USDT_USDT", "USDT",
                    ):
                        if tail.endswith(suffix):
                            tail = tail[: -len(suffix)]
                            break
                    tail = tail.strip(" _:-")
                    return tail or None

                tradfi_by_xyz: dict[str, dict] = {
                    str(r.get("xyz_coin") or "").strip().upper(): dict(r)
                    for r in _load_tradfi_map_for_ui()
                    if str(r.get("xyz_coin") or "").strip()
                }

                eligible_build_coins: list[str] = []
                for coin in coin_list:
                    xyz_name = _extract_xyz_coin_name(coin)
                    if not xyz_name:
                        continue
                    entry = tradfi_by_xyz.get(xyz_name)
                    if isinstance(entry, dict):
                        # TradFi/XYZ coin: enforce Tiingo mapping + status=ok
                        status = str(entry.get("status") or "").strip().lower()
                        has_tiingo = bool(
                            str(entry.get("tiingo_ticker") or "").strip()
                            or str(entry.get("tiingo_fx_ticker") or "").strip()
                        )
                        if status == "ok" and has_tiingo:
                            eligible_build_coins.append(coin)
                    else:
                        # Non-XYZ (crypto): always allow build selection
                        eligible_build_coins.append(coin)

                if not eligible_build_coins:
                    st.warning("No eligible coins found. XYZ symbols require Tiingo mapping with status 'ok'.")

                build_coin_options = ["All"] + eligible_build_coins if eligible_build_coins else []
                build_coin_sel = st.multiselect(
                    "Coins for build",
                    options=build_coin_options,
                    default=["All"] if eligible_build_coins else [],
                    key="market_data_hl_best_1m_coins",
                )
                if "All" in build_coin_sel or not build_coin_sel:
                    build_coins = list(eligible_build_coins)
                else:
                    build_coins = [c for c in build_coin_sel if c in eligible_build_coins]

                c_build, c_start, c_end, c_refetch = st.columns([0.18, 0.22, 0.22, 0.38], vertical_alignment="bottom")
                with c_build:
                    run_improve = st.button("Build best 1m", key="market_data_hl_best_1m_run_improve", use_container_width=True)
                with c_start:
                    build_start_date = st.date_input(
                        "Start date (optional)",
                        value=None,
                        key="market_data_hl_best_1m_start_date",
                        help=(
                            "Optional lower bound for Build best 1m. "
                            "If set: FX backfill runs newest→oldest only down to this date; "
                            "other symbols use this as start date even if older data exists."
                        ),
                    )
                with c_end:
                    build_end_date = st.date_input(
                        "End date (optional)",
                        value=None,
                        key="market_data_hl_best_1m_end_date",
                        help=(
                            "Optional upper bound for Build best 1m. "
                            "If empty: today is used."
                        ),
                    )
                with c_refetch:
                    refetch_tradfi = st.checkbox(
                        "Refetch TradFi data from scratch (stock-perps)",
                        value=False,
                        key="market_data_hl_best_1m_refetch",
                        help="Ignores existing TradFi 1m data and re-fetches from 2016-12-12. "
                             "Use after symbol mapping corrections. Applies only to XYZ-* coins.",
                    )

                if run_improve:
                    try:
                        if not build_coins:
                            raise ValueError("No eligible coins selected")

                        if build_start_date and build_end_date and build_start_date > build_end_date:
                            raise ValueError("Start date must be on or before End date")

                        effective_end_day = build_end_date.strftime("%Y%m%d") if build_end_date else _date.today().strftime("%Y%m%d")

                        job = enqueue_job(
                            job_type="hl_best_1m",
                            payload={
                                "coins": list(build_coins),
                                "end_day": effective_end_day,
                                "start_day": build_start_date.strftime("%Y%m%d") if build_start_date else "",
                                "refetch": bool(refetch_tradfi),
                            },
                        )
                        st.session_state["market_data_hl_last_job_id"] = job.job_id
                        append_exchange_download_log(
                            "hyperliquid",
                            f"[hl_best_1m] queued job_id={job.job_id}",
                        )

                        pid = read_worker_pid()
                        if not (pid and is_pid_running(int(pid))):
                            subprocess.Popen(
                                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                close_fds=True,
                            )

                        st.success(f"Queued background build job: {job.job_id}")
                        st.rerun()
                    except Exception as e:
                        append_exchange_download_log("hyperliquid", f"[hl_best_1m] ERROR {e}")
                        st.error(str(e))

                jobs_active_best = _has_active_jobs(["hl_best_1m"])
                if jobs_active_best and _supports_fragment_run_every() and not _is_background_refresh_paused():
                    @st.fragment(run_every=2)
                    def _best_jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_best_1m"],
                            details_key="market_data_hl_best_job_details",
                            panel_key="market_data_hl_best_jobs",
                            show_worker_controls=True,
                            auto_refresh_key="market_data_best_auto_refresh",
                        )
                else:
                    @st.fragment
                    def _best_jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_best_1m"],
                            details_key="market_data_hl_best_job_details",
                            panel_key="market_data_hl_best_jobs",
                            show_worker_controls=True,
                        )

                _best_jobs_fragment()

            with tradfi_anchor.expander("TradFi Symbol Mappings", expanded=False):
                # Table is built live from mapping.json merged with tradfi_symbol_map.json.
                # No sync step needed — new coins from mapping.json appear automatically.
                st.markdown("##### 🗂 Symbol Map")

                merged = _build_merged_tradfi_table()
                type_values = sorted(
                    {
                        str(r.get("canonical_type") or "").strip()
                        for r in merged
                        if str(r.get("canonical_type") or "").strip()
                    }
                )

                c_symbol, c_type, c_status = st.columns([1, 1, 1])
                with c_symbol:
                    symbol_filter = str(
                        st.text_input(
                            "Filter by symbol",
                            value=str(st.session_state.get("tradfi_map_symbol_filter", "") or ""),
                            key="tradfi_map_symbol_filter",
                            placeholder="e.g. GOOGL or EURUSD",
                        )
                        or ""
                    ).strip().upper()
                with c_type:
                    type_opts = ["all"] + list(type_values)
                    type_filter = st.selectbox(
                        "Filter by type",
                        options=type_opts,
                        index=0,
                        key="tradfi_map_type_filter",
                    )
                with c_status:
                    status_opts = ["all"] + _TRADFI_STATUSES_SELECTABLE
                    status_filter = st.selectbox(
                        "Filter by status",
                        options=status_opts,
                        index=0,
                        key="tradfi_map_status_filter",
                    )

                def _symbol_match(row: dict) -> bool:
                    if not symbol_filter:
                        return True
                    xyz = str(row.get("xyz_coin") or "").strip().upper()
                    t_tick = str(row.get("tiingo_ticker") or "").strip().upper()
                    t_fx = str(row.get("tiingo_fx_ticker") or "").strip().upper()
                    tiingo_sym = (f"IEX:{t_tick}" if t_tick else (f"FX:{t_fx}" if t_fx else ""))
                    return (
                        symbol_filter in xyz
                        or symbol_filter in t_tick
                        or symbol_filter in t_fx
                        or symbol_filter in tiingo_sym
                    )

                filtered = [
                    r for r in merged
                    if (status_filter == "all" or str(r.get("status") or "") == status_filter)
                    and (type_filter == "all" or str(r.get("canonical_type") or "") == type_filter)
                    and _symbol_match(r)
                ]

                selected_entry = None
                selected_xyz_key = "tradfi_map_selected_xyz"
                if filtered:
                    import pandas as pd
                    quote_cache = (_load_tradfi_quote_cache().get("quotes") or {})

                    def _row_prices(row: dict) -> tuple[float | None, float | None]:
                        xyz = str(row.get("xyz_coin") or "").upper()
                        hl_p = _hl_fetch_cached_price_for_xyz(xyz)

                        ti_p = None
                        t_tick = str(row.get("tiingo_ticker") or "").upper()
                        t_fx = str(row.get("tiingo_fx_ticker") or "").lower()
                        t_inv = bool(row.get("tiingo_fx_invert", False))
                        canonical_type = str(row.get("canonical_type") or "").lower()
                        status = str(row.get("status") or "").lower()

                        # Optional manual scaling for alias tickers with different share units
                        # (e.g. OTC ADR/ORD ratios). Keep None by default to avoid misleading values.
                        multiplier = None
                        try:
                            raw_mult = row.get("tiingo_price_multiplier")
                            if raw_mult not in (None, ""):
                                mv = float(raw_mult)
                                if mv > 0:
                                    multiplier = mv
                        except Exception:
                            multiplier = None

                        # Guard: alias KR/JP equities often have unit mismatch vs HL index constituents.
                        if (
                            t_tick
                            and status == "alias"
                            and canonical_type in {"equity_kr", "equity_jp"}
                            and multiplier is None
                        ):
                            return hl_p, None

                        if t_tick:
                            q = quote_cache.get(t_tick)
                            if isinstance(q, dict):
                                try:
                                    ti_p = float(q.get("price"))
                                    if multiplier is not None:
                                        ti_p = ti_p * multiplier
                                except Exception:
                                    ti_p = None
                        elif t_fx:
                            q = quote_cache.get(t_fx)
                            if isinstance(q, dict):
                                try:
                                    raw = float(q.get("price"))
                                    ti_p = (1.0 / raw) if (raw and t_inv) else raw
                                except Exception:
                                    ti_p = None

                        return hl_p, ti_p

                    def _row_tiingo_symbol(row: dict) -> str:
                        t_tick = str(row.get("tiingo_ticker") or "").strip().upper()
                        if t_tick:
                            return f"IEX:{t_tick}"
                        t_fx = str(row.get("tiingo_fx_ticker") or "").strip().upper()
                        if t_fx:
                            inv = bool(row.get("tiingo_fx_invert", False))
                            return f"FX:{t_fx}" + (" (inv)" if inv else "")
                        return ""

                    def _row_fetch_start_date(row: dict) -> str:
                        raw = str(row.get("tiingo_start_date") or "").strip()
                        t_tick = str(row.get("tiingo_ticker") or "").strip().upper()

                        parsed: _date | None = None
                        if raw:
                            try:
                                parsed = _date.fromisoformat(raw[:10])
                            except Exception:
                                parsed = None

                        # Only show fetch start if we know the real provider start date.
                        if parsed is None:
                            return ""

                        if t_tick:
                            floor = _date(2016, 12, 12)
                            return max(parsed, floor).isoformat()

                        return parsed.isoformat()

                    display_cols = [
                        ("hl_link", "Symbol"),
                        ("hl_price", "HL Price"),
                        ("tiingo_price", "Tiingo Price"),
                        ("description", "Description"),
                        ("pyth_link", "Pyth"),
                        ("canonical_type", "Type"),
                        ("tiingo_symbol", "Tiingo Symbol"),
                        ("status", "Status"),
                        ("tiingo_start_date", "Start Date"),
                        ("tiingo_fetch_start", "Fetch Start"),
                        ("last_verified", "Verified"),
                        ("note", "Note"),
                    ]

                    df_rows = []
                    for r in filtered:
                        hl_p, ti_p = _row_prices(r)
                        item = dict(r)
                        item["hl_price"] = hl_p
                        item["tiingo_price"] = ti_p
                        item["tiingo_symbol"] = _row_tiingo_symbol(r)
                        item["tiingo_fetch_start"] = _row_fetch_start_date(r)
                        df_rows.append(item)

                    df = pd.DataFrame(
                        [
                            {
                                col: (
                                    rr.get(col)
                                    if col in {"hl_price", "tiingo_price"}
                                    else str(rr.get(col) or "")
                                )
                                for col, _ in display_cols
                            }
                            for rr in df_rows
                        ]
                    )
                    df.columns = [label for _, label in display_cols]
                    sel = st.dataframe(
                        df,
                        use_container_width=True,
                        hide_index=True,
                        selection_mode="single-row",
                        on_select="rerun",
                        key="tradfi_map_df",
                        column_config={
                            "Symbol": st.column_config.LinkColumn(
                                "Symbol",
                                display_text=r"xyz:(.+)",
                                help="Open on Hyperliquid",
                                width="small",
                            ),
                            "Pyth": st.column_config.LinkColumn(
                                "Pyth",
                                display_text="🔗",
                                help="Pyth Insights — price feed details",
                                width="small",
                            ),
                            "HL Price": st.column_config.NumberColumn(
                                "HL Price",
                                format="%.4f",
                                width="small",
                            ),
                            "Tiingo Price": st.column_config.NumberColumn(
                                "Tiingo Price",
                                format="%.4f",
                                width="small",
                            ),
                            "Start Date": st.column_config.TextColumn(
                                "Start Date",
                                help="Provider-reported listing/start date from Tiingo metadata.",
                            ),
                            "Fetch Start": st.column_config.TextColumn(
                                "Fetch Start",
                                help=(
                                    "Effective earliest fetch date based on known Start Date. "
                                    "For IEX symbols it is max(Start Date, 2016-12-12). "
                                    "Empty if Start Date is unknown."
                                ),
                            ),
                        },
                    )
                    st.caption(
                        "Start Date = provider metadata. "
                        "Fetch Start = effective earliest fetch date (IEX floor 2016-12-12, "
                        "only shown when Start Date is known)."
                    )
                    sel_rows = (sel.get("selection") or {}).get("rows") or []
                    sel_idx = int(sel_rows[0]) if sel_rows else None
                    if sel_idx is not None and 0 <= sel_idx < len(filtered):
                        selected_entry = filtered[sel_idx]
                        st.session_state[selected_xyz_key] = str(
                            selected_entry.get("xyz_coin") or ""
                        ).strip().upper()
                    else:
                        selected_xyz = str(st.session_state.get(selected_xyz_key) or "").strip().upper()
                        if selected_xyz:
                            selected_entry = next(
                                (r for r in filtered if str(r.get("xyz_coin") or "").strip().upper() == selected_xyz),
                                None,
                            )
                        if selected_entry is None:
                            st.session_state.pop(selected_xyz_key, None)
                else:
                    st.session_state.pop(selected_xyz_key, None)
                    st.caption("No entries match this filter." if merged else
                               "mapping.json not found. Run a Hyperliquid market data sync first.")

                # Row 1: selected-row workflow (find -> edit -> validate -> fetch start)
                tiingo_key_for_actions = str(
                    st.session_state.get("market_data_tiingo_api_key") or ""
                ).strip()
                selected_has_equity_ticker = bool(
                    str((selected_entry or {}).get("tiingo_ticker") or "").strip()
                )
                col_search, col_edit, col_test, col_start_one, col_spec = st.columns([2, 2, 2, 2, 2])
                with col_search:
                    if st.button(
                        "🔍 Search ticker",
                        key="tradfi_ticker_search_btn",
                        disabled=selected_entry is None,
                        help="Search Tiingo ticker database (beta). Cost: 1 API request per search.",
                    ):
                        xyz = str((selected_entry or {}).get("xyz_coin") or "")
                        _tiingo_search_dialog(xyz_coin=xyz, api_key=tiingo_key_for_actions)
                with col_edit:
                    if st.button("✏️ Edit", key="tradfi_map_edit_btn", disabled=selected_entry is None):
                        _tradfi_map_edit_dialog(mode="edit", existing=selected_entry)
                with col_test:
                    if st.button("🔍 Test Resolve", key="tradfi_map_test_btn", disabled=selected_entry is None):
                        xyz = str((selected_entry or {}).get("xyz_coin") or "")
                        t_ticker, t_fx, t_inv, t_start = resolve_tradfi_symbol(xyz)
                        st.session_state["tradfi_map_test_result"] = {
                            "xyz_coin": xyz,
                            "tiingo_ticker": t_ticker,
                            "tiingo_fx_ticker": t_fx,
                            "tiingo_fx_invert": t_inv,
                            "tiingo_start_date": str(t_start) if t_start else None,
                        }
                with col_start_one:
                    if st.button(
                        "📅 Fetch start date",
                        key="tradfi_startdate_selected_btn",
                        disabled=(selected_entry is None or not tiingo_key_for_actions or not selected_has_equity_ticker),
                        help="Fetch startDate for selected equity symbol via /tiingo/daily/{ticker}. Cost: 1 API request.",
                    ):
                        with st.spinner("Fetching start date for selected symbol…"):
                            try:
                                _sd = _update_tiingo_start_date_for_selected(
                                    selected_entry=selected_entry,
                                    api_key=tiingo_key_for_actions,
                                )
                                st.session_state["tradfi_startdate_selected_result"] = _sd
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Start-date fetch failed: {_exc}")
                with col_spec:
                    if st.button("🔄 Spec", key="tradfi_spec_refresh_btn",
                                 help="Re-fetch the XYZ Specification Index from docs.trade.xyz"):
                        try:
                            instruments = fetch_xyz_spec()
                            st.success(f"Spec updated: {len(instruments)} instruments")
                            st.rerun()
                        except Exception as _exc:
                            st.error(f"Spec fetch failed: {_exc}")

                # Row 2: global/batch workflow (sync -> map -> batch dates -> refresh caches)
                col_automap, col_start_all, col_metarefresh, col_pricerefresh, col_spec_view = st.columns([2, 2, 2, 2, 2])
                with col_automap:
                    if st.button(
                        "🤖 Auto-Map",
                        key="tradfi_automap_btn",
                        disabled=not tiingo_key_for_actions,
                        help="Auto-map all 'pending' entries to Tiingo tickers."
                             " Cost: usually 0 requests (uses meta cache), up to 1 request if meta cache is missing.",
                    ):
                        with st.spinner("Auto-Map running…"):
                            try:
                                _am_result = auto_map_tradfi(
                                    api_key=tiingo_key_for_actions,
                                    force_meta_refresh=False,
                                )
                                st.session_state["tradfi_automap_result"] = _am_result
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Auto-Map failed: {_exc}")

                with col_start_all:
                    if st.button(
                        "📅 Fetch all start dates",
                        key="tradfi_startdate_all_btn",
                        disabled=not tiingo_key_for_actions,
                        help="Fetch startDate for all symbols missing it and having Tiingo equity tickers. Cost: 1 API request per symbol fetched.",
                    ):
                        with st.spinner("Fetching start dates for all symbols…"):
                            try:
                                _sd_all = _update_tiingo_start_dates_for_all(
                                    api_key=tiingo_key_for_actions,
                                    rows=merged,
                                )
                                st.session_state["tradfi_startdate_all_result"] = _sd_all
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Bulk start-date fetch failed: {_exc}")

                # Cache info for global refresh actions
                _meta_cache = (
                    Path.cwd() / "data" / "coindata" / "tiingo_meta.json"
                )
                _meta_info = ""
                if _meta_cache.exists():
                    try:
                        _md = json.loads(_meta_cache.read_text(encoding="utf-8"))
                        _meta_ts = (_md.get("fetched_at") or "")[:10]
                        _meta_n = len(_md.get("meta") or {})
                        _meta_info = f"Cache: {_meta_n:,} tickers from {_meta_ts}"
                    except Exception:
                        _meta_info = "Cache available"
                else:
                    _meta_info = "No cache — fetched on first Auto-Map"

                _quote_cache = _tradfi_quote_cache_path()
                _quote_info = ""
                if _quote_cache.exists():
                    try:
                        _qd = json.loads(_quote_cache.read_text(encoding="utf-8"))
                        _qt = str(_qd.get("fetched_at") or "")[:19].replace("T", " ")
                        _qn = len((_qd.get("quotes") or {}))
                        _quote_info = f"Price cache: {_qn:,} quotes · {_qt} UTC"
                    except Exception:
                        _quote_info = "Price cache available"
                else:
                    _quote_info = "No price cache — load via 'Refresh prices'"
                with col_metarefresh:
                    if st.button(
                        "🔄 Refresh metadata",
                        key="tradfi_meta_refresh_btn",
                        disabled=not tiingo_key_for_actions,
                        help=f"Refresh Tiingo fundamentals/meta cache. Cost: 1 API request.\n{_meta_info}",
                    ):
                        with st.spinner("Loading Tiingo metadata…"):
                            try:
                                _meta = fetch_tiingo_meta(
                                    api_key=tiingo_key_for_actions,
                                    force_refresh=True,
                                )
                                st.success(f"Metadata refreshed: {len(_meta):,} tickers loaded")
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Metadata refresh failed: {_exc}")

                with col_pricerefresh:
                    if st.button(
                        "💲 Refresh prices",
                        key="tradfi_price_refresh_btn",
                        disabled=not tiingo_key_for_actions,
                        help=f"Refresh Tiingo quotes (IEX all + FX top). Cost: 1 IEX request + up to 1 FX request.\n{_quote_info}",
                    ):
                        with st.spinner("Loading Tiingo prices…"):
                            try:
                                _pr = refresh_tradfi_quote_cache(
                                    api_key=tiingo_key_for_actions,
                                    records=_load_tradfi_map_for_ui(),
                                )
                                st.session_state["tradfi_price_refresh_result"] = _pr
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Price refresh failed: {_exc}")

                with col_spec_view:
                    if st.button(
                        "📋 View specs",
                        key="tradfi_spec_view_btn",
                        help="Open the cached XYZ specification list in a popup.",
                    ):
                        _tradfi_spec_view_dialog()

                if not tiingo_key_for_actions:
                    st.caption("🤖 Auto-Map and metadata refresh require a configured Tiingo API key.")
                else:
                    st.caption(f"{_meta_info} · {_quote_info}")

                # Auto-Map Ergebnis anzeigen
                _am_res = st.session_state.pop("tradfi_automap_result", None)
                if _am_res:
                    eq = _am_res.get("mapped_equity", 0)
                    fx = _am_res.get("mapped_fx", 0)
                    np_ = _am_res.get("no_provider", 0)
                    nf = _am_res.get("not_found", 0)
                    sk = _am_res.get("skipped", 0)
                    st.success(
                        f"✅ Auto-Map completed: "
                        f"**{eq}** equities/ETFs mapped · "
                        f"**{fx}** FX/commodities · "
                        f"**{np_}** no provider · "
                        f"**{nf}** not found · "
                        f"**{sk}** skipped (already set)"
                    )

                _pr_res = st.session_state.pop("tradfi_price_refresh_result", None)
                if _pr_res:
                    st.success(
                        "✅ Prices refreshed: "
                        f"**{_pr_res.get('quotes_saved', 0)}** quotes saved · "
                        f"IEX Rows: **{_pr_res.get('iex_rows', 0)}** · "
                        f"FX Rows: **{_pr_res.get('fx_rows', 0)}**"
                    )

                _sd_one_res = st.session_state.pop("tradfi_startdate_selected_result", None)
                if _sd_one_res:
                    if int(_sd_one_res.get("updated", 0)) > 0:
                        st.success(
                            f"✅ Start date updated: XYZ-{_sd_one_res.get('xyz_coin')} → "
                            f"{_sd_one_res.get('ticker')} · {_sd_one_res.get('start_date')}"
                        )
                    else:
                        st.info(f"Start-date skipped: {_sd_one_res.get('reason')}")

                _sd_all_res = st.session_state.pop("tradfi_startdate_all_result", None)
                if _sd_all_res:
                    st.success(
                        "✅ Start dates fetched: "
                        f"**{_sd_all_res.get('updated', 0)}** updated · "
                        f"**{_sd_all_res.get('skipped', 0)}** skipped · "
                        f"**{_sd_all_res.get('errors', 0)}** no-data/errors"
                    )

                # Test resolve result
                test_res = st.session_state.get("tradfi_map_test_result")
                if test_res:
                    t_tick = test_res.get("tiingo_ticker")
                    t_fx = test_res.get("tiingo_fx_ticker")
                    xyz_label = str(test_res.get("xyz_coin") or "")
                    if t_tick or t_fx:
                        parts = []
                        if t_tick:
                            parts.append(f"Tiingo IEX: `{t_tick}`")
                        if t_fx:
                            inv_note = " (inverted)" if test_res.get("tiingo_fx_invert") else ""
                            parts.append(f"Tiingo FX: `{t_fx}`{inv_note}")
                        if test_res.get("tiingo_start_date"):
                            parts.append(f"Start: `{test_res['tiingo_start_date']}`")
                        st.success(f"**XYZ-{xyz_label}** → " + "  ·  ".join(parts))
                    else:
                        entry_status = next(
                            (str(r.get("status") or "") for r in merged
                             if str(r.get("xyz_coin") or "").upper() == xyz_label.upper()),
                            None,
                        )
                        if entry_status in ("no_provider", "delisted", "pending"):
                            st.info(f"XYZ-{xyz_label}: status={entry_status} → fetch skipped silently")
                        else:
                            st.warning(
                                f"XYZ-{xyz_label}: no map entry or no ticker configured "
                                f"→ fetch skipped with WARNING in log"
                            )
                    if st.button("✖ Clear result", key="tradfi_map_test_clear"):
                        st.session_state.pop("tradfi_map_test_result", None)
                        st.rerun()

            with download_anchor.expander("Download l2Book from AWS", expanded=False):
                profile = str(st.session_state.get("market_data_hl_aws_profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
                region_default = load_aws_profile_region(profile) or HYPERLIQUID_AWS_REGION
                region = str(st.session_state.get("market_data_hl_aws_region") or region_default).strip()

                jobs_active = _has_active_jobs(["hl_aws_l2book_auto"])
                if jobs_active and _supports_fragment_run_every() and not _is_background_refresh_paused():
                    @st.fragment(run_every=2)
                    def _jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_aws_l2book_auto"],
                            details_key="market_data_hl_job_details",
                            panel_key="market_data_hl_jobs",
                            show_worker_controls=True,
                            auto_refresh_key="market_data_aws_auto_refresh",
                        )
                else:
                    @st.fragment
                    def _jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_aws_l2book_auto"],
                            details_key="market_data_hl_job_details",
                            panel_key="market_data_hl_jobs",
                            show_worker_controls=True,
                        )

                if coin_list:
                    aws_coin_options = ["All"] + coin_list if coin_list else []
                    st.multiselect(
                        "Coins",
                        options=aws_coin_options,
                        default=[],
                        key="market_data_hl_aws_coins",
                    )
                else:
                    st.warning("No enabled coins selected.")

                rr = st.session_state.get("market_data_hl_archive_range") or {}
                default_oldest = str(rr.get("oldest_day") or "20230415")
                default_newest = str(rr.get("newest_day") or "20251202")

                # Parse defaults to date objects
                try:
                    from datetime import datetime as _dt
                    default_start_date = _dt.strptime(default_oldest, "%Y%m%d").date()
                except Exception:
                    default_start_date = _dt(2023, 4, 15).date()

                try:
                    default_end_date = _dt.strptime(default_newest, "%Y%m%d").date()
                except Exception:
                    default_end_date = _dt.now().date()

                col1, col2 = st.columns(2)
                with col1:
                    dl_start_date = st.date_input(
                        "Start date",
                        value=default_start_date,
                        key="market_data_hl_dl_start_date",
                        help="First day to download (archive oldest: " + default_oldest + ")",
                    )
                with col2:
                    dl_end_date = st.date_input(
                        "End date",
                        value=default_end_date,
                        key="market_data_hl_dl_end_date",
                        help="Last day to download (archive newest: " + default_newest + ")",
                    )

                c_dl_btn, c_dl_opt = st.columns([0.28, 0.72], vertical_alignment="center")
                with c_dl_opt:
                    st.checkbox(
                        "Only missing 1m_src hours",
                        value=bool(st.session_state.get("market_data_hl_dl_only_missing_1m_src_hours", True)),
                        key="market_data_hl_dl_only_missing_1m_src_hours",
                        help="If enabled, downloads only l2Book hours that have no minute coverage in 1m_src yet. "
                        "Also skips days older than your local oldest l2Book day for that coin. "
                        "Useful to keep disk usage low after you delete processed l2Book files.",
                    )
                with c_dl_btn:
                    do_download = st.button("Download", key="market_data_hl_dl_auto")

                if do_download:
                    try:
                        # Resolve selected coins for auto-download. If 'All' is mixed with
                        # specific coins, prefer the explicit selection.
                        if not coin_list:
                            raise ValueError("No enabled coins selected")
                        _aws_sel = st.session_state.get("market_data_hl_aws_coins") or []
                        _aws_sel = [str(x).strip().upper() for x in _aws_sel if str(x).strip()]
                        if "ALL" in _aws_sel and len(_aws_sel) > 1:
                            _aws_sel = [c for c in _aws_sel if c != "ALL"]
                        if not _aws_sel or "ALL" in _aws_sel:
                            _payload_coins = list(coin_list)
                        else:
                            _payload_coins = [c for c in _aws_sel if c in coin_list]
                        if not _payload_coins:
                            raise ValueError("No enabled coins selected")

                        # Keep canonical coin names for l2Book archive checks/download.
                        # Hyperliquid mapping symbols may be numeric IDs (e.g. BABY -> 189),
                        # while AWS l2Book objects are keyed by coin names.
                        _payload_coins = [str(c).strip() for c in _payload_coins if str(c).strip()]

                        ak = str(st.session_state.get("market_data_hl_aws_access_key_id") or "").strip()
                        sk = str(st.session_state.get("market_data_hl_aws_secret_access_key") or "").strip()
                        if not ak or not sk:
                            raise ValueError("Missing AWS credentials")

                        # Persist creds+region so the background worker can use them
                        try:
                            save_aws_profile_credentials(profile=profile, aws_access_key_id=ak, aws_secret_access_key=sk)
                        except Exception:
                            pass
                        try:
                            save_aws_profile_region(profile=profile, region=str(region).strip())
                        except Exception:
                            pass

                        # Preflight: verify coin exists in archive for a probe day (fast).
                        rr = _normalize_archive_range(st.session_state.get("market_data_hl_archive_range") or {})
                        probe_day = str(rr.get("newest_day") or rr.get("oldest_day") or "").strip()
                        if not probe_day:
                            with st.spinner("Detecting available archive day range..."):
                                rr = _normalize_archive_range(
                                    get_hyperliquid_archive_day_range_aws(
                                        aws_access_key_id=ak,
                                        aws_secret_access_key=sk,
                                        region_name=str(region).strip(),
                                    )
                                )
                            st.session_state["market_data_hl_archive_range"] = rr
                            probe_day = str(rr.get("newest_day") or rr.get("oldest_day") or "").strip()
                        if not probe_day:
                            raise RuntimeError("Failed to detect archive range for preflight")

                        probe_hours = list_hyperliquid_archive_hours_aws(
                            day=probe_day,
                            aws_access_key_id=ak,
                            aws_secret_access_key=sk,
                            region_name=str(region).strip(),
                        )
                        if not probe_hours:
                            raise RuntimeError(f"No archive hours found for {probe_day}")

                        missing_coins = []
                        for coin in list(_payload_coins):
                            ok = check_hyperliquid_l2book_coin_exists_aws(
                                coin=coin,
                                day=probe_day,
                                aws_access_key_id=ak,
                                aws_secret_access_key=sk,
                                region_name=str(region).strip(),
                                hours=probe_hours,
                            )
                            if not ok:
                                missing_coins.append(coin)

                        if missing_coins:
                            st.warning(
                                f"No l2Book objects found for: {', '.join(missing_coins)} (probe day {probe_day}). Skipping."
                            )
                            _payload_coins = [c for c in _payload_coins if c not in missing_coins]
                        if not _payload_coins:
                            raise ValueError("No selected coins exist in the archive")

                        # Convert selected dates to YYYYMMDD format
                        start_day_str = dl_start_date.strftime("%Y%m%d") if dl_start_date else ""
                        end_day_str = dl_end_date.strftime("%Y%m%d") if dl_end_date else ""

                        if not start_day_str or not end_day_str:
                            raise ValueError("Start and end dates are required")

                        # Enqueue background job
                        job = enqueue_job(
                            job_type="hl_aws_l2book_auto",
                            payload={
                                "profile": str(profile).strip() or "pbgui-hyperliquid",
                                "region": str(region).strip(),
                                "coins": list(_payload_coins),
                                "chunk_days": 7,
                                "start_day": start_day_str,
                                "end_day": end_day_str,
                                "only_missing_1m_src_hours": bool(
                                    st.session_state.get("market_data_hl_dl_only_missing_1m_src_hours", True)
                                ),
                            },
                        )
                        # Log which coins are enqueued for easier debugging
                        append_exchange_download_log("hyperliquid", f"[hl_aws_l2book_auto] queued job_id={job.job_id} coins={_payload_coins} range={start_day_str}-{end_day_str}")
                        st.success(f"Queued background download job: {job.job_id} (coins={len(_payload_coins)})")
                        st.session_state["market_data_hl_last_job_id"] = job.job_id

                        # Start worker if not running
                        pid = read_worker_pid()
                        if not (pid and is_pid_running(int(pid))):
                            subprocess.Popen(
                                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                close_fds=True,
                            )

                        st.rerun()
                    except Exception as e:
                        append_exchange_download_log("hyperliquid", f"[hl_aws_l2book_auto] ERROR {e}")
                        st.error(str(e))

                # Job queue (shown below download controls)
                _jobs_fragment()

                # Last download job summary (auto-refresh while jobs are active)
                try:
                    def _render_last_download_job() -> None:
                        jobs_any = list_jobs(states=["running", "done", "failed"], limit=50)
                        jobs_any = [j for j in jobs_any if str(j.get("type") or "") == "hl_aws_l2book_auto"]
                        try:
                            jobs_any = sorted(
                                jobs_any,
                                key=lambda j: int(float(j.get("updated_ts") or 0)),
                                reverse=True,
                            )
                        except Exception:
                            pass

                        if jobs_any:
                            last = jobs_any[0]
                            status = str(last.get("status") or "")
                            upd_ts = _format_unix_ts(last.get("updated_ts"))
                            err = str(last.get("error") or "")
                            payload = last.get("payload") if isinstance(last.get("payload"), dict) else {}
                            pr = last.get("progress") if isinstance(last.get("progress"), dict) else {}
                            coins = payload.get("coins") if isinstance(payload, dict) else None
                            coins = coins if isinstance(coins, list) else []
                            coins_preview = ", ".join(str(c) for c in coins[:12])
                            if coins_preview and len(coins) > 12:
                                coins_preview += " …"
                            start_day = str(payload.get("start_day") or "") if isinstance(payload, dict) else ""
                            end_day = str(payload.get("end_day") or "") if isinstance(payload, dict) else ""
                            st.write(
                                f"status={status}"
                                + (f" updated={upd_ts}" if upd_ts else "")
                                + (f" error={err}" if err else "")
                            )
                            if coins_preview:
                                st.caption(f"coins: {coins_preview}")
                            if start_day or end_day:
                                st.caption(f"range: {start_day or '?'} → {end_day or '?'}")

                            lr = pr.get("last_result") if isinstance(pr, dict) and isinstance(pr.get("last_result"), dict) else {}
                            downloaded = int(pr.get("downloaded_total", lr.get("downloaded", 0)) or 0)
                            skipped = int(pr.get("skipped_existing_total", lr.get("skipped_existing", 0)) or 0)
                            failed = int(pr.get("failed_total", lr.get("failed", 0)) or 0)
                            planned = int(lr.get("planned", 0) or 0)
                            if planned <= 0:
                                planned = int(pr.get("chunk_total", 0) or 0)
                            done = downloaded + skipped + failed

                            progress_pct = ""
                            if planned > 0:
                                try:
                                    pct = max(0.0, min(100.0, (float(done) / float(planned)) * 100.0))
                                    progress_pct = f" ({pct:.1f}%)"
                                except Exception:
                                    progress_pct = ""

                            duration_txt = ""
                            try:
                                created_ts_raw = last.get("created_ts")
                                updated_ts_raw = last.get("updated_ts")
                                created_ts = float(created_ts_raw) if created_ts_raw is not None else 0.0
                                updated_ts = float(updated_ts_raw) if updated_ts_raw is not None else 0.0
                                if created_ts > 0 and updated_ts >= created_ts:
                                    dur_s = int(updated_ts - created_ts)
                                    h = dur_s // 3600
                                    m = (dur_s % 3600) // 60
                                    s = dur_s % 60
                                    if h > 0:
                                        duration_txt = f" duration={h}h {m:02d}m {s:02d}s"
                                    elif m > 0:
                                        duration_txt = f" duration={m}m {s:02d}s"
                                    else:
                                        duration_txt = f" duration={s}s"
                            except Exception:
                                duration_txt = ""

                            dl_b = int(pr.get("downloaded_bytes_total", lr.get("downloaded_bytes", 0)) or 0)
                            sk_b = int(pr.get("skipped_existing_bytes_total", lr.get("skipped_existing_bytes", 0)) or 0)
                            fl_b = int(pr.get("failed_bytes_total", lr.get("failed_bytes", 0)) or 0)
                            total_b = dl_b + sk_b + fl_b

                            st.caption(
                                f"stats: downloaded={downloaded} skipped={skipped} failed={failed}"
                                + (f" done={done}/{planned}{progress_pct}" if planned > 0 else "")
                                + duration_txt
                            )
                            st.caption(
                                f"size: downloaded={_fmt_bytes(dl_b)} skipped={_fmt_bytes(sk_b)} "
                                f"failed={_fmt_bytes(fl_b)} total={_fmt_bytes(total_b)}"
                            )
                        else:
                            st.write("No download jobs yet.")

                    with st.expander("Last download job", expanded=False):
                        jobs_active_last = _has_active_jobs(["hl_aws_l2book_auto"])
                        if jobs_active_last and _supports_fragment_run_every() and not _is_background_refresh_paused():
                            @st.fragment(run_every=2)
                            def _last_download_fragment():
                                try:
                                    if not bool(list_jobs(states=["pending", "running"], limit=1)):
                                        st.rerun()
                                except Exception:
                                    pass
                                _render_last_download_job()
                        else:
                            @st.fragment
                            def _last_download_fragment():
                                _render_last_download_job()
                        _last_download_fragment()
                except Exception:
                    pass


    if main_view == "Already have":
        _have_options = (
            ["1m", "PB7 cache"]
            if str(exchange).lower() == "binance"
            else ["1m", "1m_api", "l2Book", "PB7 cache"]
        )
        have_view = st.segmented_control(
            "",
            options=_have_options,
            default="1m",
            key="market_data_have_view",
        )

        # Map UI exchange name → storage directory name (e.g. "binance" → "binanceusdm")
        _storage_exchange_map = {"binance": "binanceusdm"}
        _storage_ex = _storage_exchange_map.get(str(exchange).lower(), str(exchange).lower())

        import pandas as pd

        try:
            _latest_interval_s = int(str(load_ini("pbdata", "latest_1m_interval_seconds") or "1800").strip())
        except Exception:
            _latest_interval_s = 120
        _missing_lag_minutes = max(0, int((_latest_interval_s + 59) // 60))

        from inventory_cache import get_inventory as _get_inventory

        # Helper function to render table for a specific dataset
        def _render_dataset_table(dataset_ds: str, tab_key: str) -> tuple:
            _is_hl = str(_storage_ex) == "hyperliquid"
            _raw = _get_inventory(
                _storage_ex,
                dataset_ds,
                lag_minutes=_missing_lag_minutes,
                tradfi_type_fn=_tradfi_canonical_type_for_coin if _is_hl else None,
                expected_minutes_fn=(
                    (lambda _tt, _d: _tradfi_expected_indices_for_type(_d, _tt))
                    if _is_hl else None
                ),
            )
            if not _raw:
                st.info(f"No {dataset_ds} data found yet.")
                return None, []
            _rows = []
            for r in _raw:
                _tb = r.get("total_bytes", 0) or 0
                _rows.append(
                    {
                        "exchange": r.get("exchange", ""),
                        "dataset": r.get("dataset", ""),
                        "coin": r.get("coin", ""),
                        "n_files": r.get("n_files", 0),
                        "total_bytes": int(_tb),
                        "size": float(_tb) / (1024.0 * 1024.0),
                        "oldest_day": r.get("oldest_day", ""),
                        "newest_day": r.get("newest_day", ""),
                        "n_days": r.get("n_days", 0),
                        "expected_hours": r.get("expected_hours", 0),
                        "coverage_pct": r.get("coverage_pct", 0),
                        "missing_days_count": r.get("missing_days_count", 0),
                        "missing_days_sample": r.get("missing_days_sample", ""),
                        "hl_minutes": r.get("hl_minutes", 0) if _is_hl else "",
                        "other_minutes": r.get("other_minutes", 0) if _is_hl else "",
                        "missing_minutes": r.get("missing_minutes", 0) if _is_hl else "",
                    }
                )
            table_rows = _rows

            df_cached = pd.DataFrame(table_rows)
            if not df_cached.empty:
                total_files = int(df_cached["n_files"].sum()) if "n_files" in df_cached.columns else 0
                total_bytes = int(df_cached["total_bytes"].sum()) if "total_bytes" in df_cached.columns else 0
                n_coins = int(df_cached["coin"].nunique()) if "coin" in df_cached.columns else 0

                c1, c2, c3 = st.columns(3)
                c1.metric("coins", n_coins)
                c2.metric("files", total_files)
                c3.metric("size", _fmt_bytes(total_bytes))

            df_cached["_src_idx"] = list(range(len(df_cached)))
            df_view = df_cached.copy()
            if str(dataset_ds).lower() in ("1m_api", "candles_1m_api", "l2book"):
                drop_cols = [c for c in ("hl_minutes", "other_minutes", "missing_minutes") if c in df_cached.columns]
                if drop_cols:
                    df_view = df_cached.drop(columns=drop_cols)

            c_coin, c_kind = st.columns([0.7, 0.3])
            with c_coin:
                coin_filter = str(
                    st.text_input(
                        "Filter by coin",
                        value=str(st.session_state.get(f"market_data_have_coin_filter_{tab_key}", "") or ""),
                        key=f"market_data_have_coin_filter_{tab_key}",
                        placeholder="e.g. GOOGL or BTC",
                    )
                    or ""
                ).strip().upper()
            with c_kind:
                kind_filter = st.selectbox(
                    "Filter by type",
                    options=["all", "stocks (xyz)", "crypto"],
                    index=0,
                    key=f"market_data_have_kind_filter_{tab_key}",
                )

            if not df_view.empty:
                if coin_filter:
                    df_view = df_view[
                        df_view["coin"].astype(str).str.upper().str.contains(coin_filter, na=False)
                    ]

                if kind_filter != "all":
                    coin_upper = df_view["coin"].astype(str).str.upper()
                    is_stock = coin_upper.str.startswith("XYZ:") | coin_upper.str.startswith("XYZ-")
                    if kind_filter == "stocks (xyz)":
                        df_view = df_view[is_stock]
                    else:
                        df_view = df_view[~is_stock]

            drop_display_cols = [c for c in ("exchange", "dataset", "_src_idx", "total_bytes") if c in df_view.columns]
            if drop_display_cols:
                df_display = df_view.drop(columns=drop_display_cols)
            else:
                df_display = df_view

            # ---- stable st.dataframe with on_select ----
            column_config = {
                "size": st.column_config.NumberColumn(
                    "size",
                    format="%.2f MB",
                )
            }
            event = st.dataframe(
                df_display,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key=f"market_data_have_table_{tab_key}",
                column_config=column_config,
            )

            # Track selection per tab so we can distinguish
            # "empty because rerun" from "user explicitly deselected".
            _prev_sel_key = f"market_data_prev_sel_{tab_key}"
            sel_indices = event.selection.rows if event and event.selection else []
            prev_sel = st.session_state.get(_prev_sel_key, [])
            if sel_indices:
                # User selected a row
                idx = sel_indices[0]
                if 0 <= idx < len(df_view):
                    try:
                        src_idx = int(df_view.iloc[idx]["_src_idx"])
                    except Exception:
                        src_idx = idx
                else:
                    src_idx = -1
                if 0 <= src_idx < len(_rows):
                    clicked = (
                        str(_rows[src_idx].get("dataset") or ""),
                        str(_rows[src_idx].get("coin") or ""),
                    )
                    st.session_state["market_data_heatmap_sel"] = clicked
                    st.session_state["market_data_heatmap_tab"] = tab_key
            elif prev_sel and not preserve_selection_once:
                # Had a selection before, now empty → user deselected
                st.session_state.pop("market_data_heatmap_sel", None)
                st.session_state.pop("market_data_heatmap_tab", None)
            st.session_state[_prev_sel_key] = list(sel_indices)

            # Only show selection if it belongs to this tab
            sel_row = None
            hm = st.session_state.get("market_data_heatmap_sel")
            hm_tab = st.session_state.get("market_data_heatmap_tab")
            if isinstance(hm, (tuple, list)) and len(hm) == 2 and hm_tab == tab_key:
                for r in _rows:
                    if (str(r.get("dataset") or ""), str(r.get("coin") or "")) == tuple(hm):
                        sel_row = r
                        break

            if sel_row:
                st.caption(f"Heatmap: {sel_row.get('dataset')} / {sel_row.get('coin')}")
            else:
                st.info("Click a row to display the heatmap.")

            return sel_row, _rows

        # Helper for deletion operations
        def _render_deletion_tools(dataset_rows: list, dataset_key: str, dataset_label: str, sel_row: dict | None = None):
            """Render deletion tools for a specific dataset."""
            import shutil

            def _remove_source_index_dirs_for_coin(actual_coin: str) -> int:
                base = get_exchange_raw_root_dir(str(exchange).lower())
                removed_count = 0
                src_dir = base / "1m_src" / str(actual_coin).strip()
                if src_dir.exists():
                    shutil.rmtree(src_dir)
                    removed_count += 1
                return removed_count

            def _rebuild_source_index_from_api_for_coin(actual_coin: str) -> tuple[int, int]:
                presence = get_minute_presence_for_dataset(
                    str(exchange).lower(),
                    "1m_api",
                    str(actual_coin).strip(),
                )
                days = presence.get("days") if isinstance(presence, dict) else {}
                if not isinstance(days, dict) or not days:
                    return (0, 0)

                days_written = 0
                minutes_written = 0
                for day_s, hours_map in days.items():
                    if not isinstance(hours_map, dict):
                        continue
                    minute_indices: set[int] = set()
                    for hour_s, mins_map in hours_map.items():
                        try:
                            hour_i = int(hour_s)
                        except Exception:
                            continue
                        if hour_i < 0 or hour_i > 23:
                            continue
                        if not isinstance(mins_map, dict):
                            continue
                        for minute_k in mins_map.keys():
                            try:
                                minute_i = int(minute_k)
                            except Exception:
                                continue
                            if minute_i < 0 or minute_i > 59:
                                continue
                            minute_indices.add((hour_i * 60) + minute_i)

                    if minute_indices:
                        update_source_index_for_day(
                            exchange=str(exchange).lower(),
                            coin=str(actual_coin).strip(),
                            day=str(day_s),
                            minute_indices=sorted(minute_indices),
                            code=SOURCE_CODE_API,
                        )
                        days_written += 1
                        minutes_written += len(minute_indices)

                return (days_written, minutes_written)

            if not dataset_rows:
                return

            # Get all coins in this dataset
            available_coins = sorted({str(r.get("coin", "")).strip().upper() for r in dataset_rows if str(r.get("coin", "")).strip()})
            
            if not available_coins:
                return

            with st.expander("🗑️ Deletion Tools", expanded=False):
                # Get selected coin from table if available
                selected_coin_from_table = None
                if sel_row and isinstance(sel_row, dict):
                    selected_coin_from_table = str(sel_row.get("coin", "")).strip().upper()

                st.info(f"**{len(available_coins)} coins** in {dataset_label}")

                # 0. Quick delete selected row if available
                if selected_coin_from_table and selected_coin_from_table in available_coins:
                    st.subheader("Quick delete: Selected row", divider="red")
                    st.caption(f"Delete currently selected coin: **{selected_coin_from_table}**")
                    if st.button(f"🗑️ Delete {selected_coin_from_table}", key=f"market_data_delete_selected_row_{dataset_key}", type="secondary"):
                        try:
                            # Use dataset name and coin name EXACTLY as they appear in the row
                            actual_dataset = str(sel_row.get("dataset", "")).strip()
                            actual_dataset_lower = actual_dataset.lower()
                            actual_coin = str(sel_row.get("coin", "")).strip()
                            
                            coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                            if coin_dir.exists():
                                shutil.rmtree(coin_dir)
                                st.success(f"✅ Deleted {actual_coin}")
                            else:
                                st.error(f"❌ Directory not found: {coin_dir}")
                            
                            # Also reset + rebuild source index from existing 1m_api if deleting 1m dataset
                            if actual_dataset_lower in ("1m", "candles_1m"):
                                _remove_source_index_dirs_for_coin(actual_coin)
                                rebuilt_days, rebuilt_minutes = _rebuild_source_index_from_api_for_coin(actual_coin)
                                if rebuilt_days > 0:
                                    st.caption(
                                        f"Rebuilt source index from 1m_api: {rebuilt_days} days, {rebuilt_minutes} minutes"
                                    )
                            
                            # Clear selection cache
                            st.session_state.pop("market_data_heatmap_sel", None)
                            
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error deleting: {e}")
                    st.divider()

                # 1. Delete selected coins
                st.subheader("1️⃣ Delete selected coins", divider="red")
                sel_coins_key = f"market_data_delete_sel_coins_{dataset_key}"
                
                # Create options with "ALL" at the top
                coin_options = ["🔴 DELETE ALL COINS"] + available_coins
                selected_for_delete = st.multiselect(
                    f"Select coins to delete:",
                    options=coin_options,
                    key=sel_coins_key,
                    help="Multi-select coins to delete all their data from this dataset. Use '🔴 DELETE ALL COINS' to delete entire dataset."
                )
                
                # Handle "DELETE ALL" option
                if "🔴 DELETE ALL COINS" in selected_for_delete:
                    # If "DELETE ALL" is selected, that's the only thing we delete
                    selected_for_delete = available_coins
                else:
                    # Filter out the ALL option if it was there
                    selected_for_delete = [c for c in selected_for_delete if c != "🔴 DELETE ALL COINS"]

                if selected_for_delete:
                    # Calculate size preview
                    total_size = 0
                    total_files = 0
                    for r in dataset_rows:
                        if str(r.get("coin", "")).strip().upper() in selected_for_delete:
                            total_size += int(r.get("total_bytes", 0) or 0)
                            total_files += int(r.get("n_files", 0) or 0)

                    size_str = _fmt_bytes(total_size) if total_size else "0 B"
                    st.caption(f"📊 Preview: {len(selected_for_delete)} coins, {total_files} files, {size_str}")

                    if st.button("🗑️ Delete selected coins", key=f"market_data_delete_selected_btn_{dataset_key}", type="secondary"):
                        try:
                            deleted_count = 0
                            rebuilt_days_total = 0
                            rebuilt_minutes_total = 0
                            # Use actual dataset name from first row
                            actual_dataset = str(dataset_rows[0].get("dataset", "")).strip() if dataset_rows else dataset_key
                            actual_dataset_lower = actual_dataset.lower()
                            
                            for r in dataset_rows:
                                coin = str(r.get("coin", "")).strip().upper()
                                if coin not in selected_for_delete:
                                    continue
                                
                                # Use actual coin name (original case) from row
                                actual_coin = str(r.get("coin", "")).strip()
                                coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                                if coin_dir.exists():
                                    shutil.rmtree(coin_dir)
                                    deleted_count += 1
                                
                                # Also reset + rebuild source index from existing 1m_api if deleting 1m dataset
                                if actual_dataset_lower in ("1m", "candles_1m"):
                                    _remove_source_index_dirs_for_coin(actual_coin)
                                    rebuilt_days, rebuilt_minutes = _rebuild_source_index_from_api_for_coin(actual_coin)
                                    rebuilt_days_total += int(rebuilt_days)
                                    rebuilt_minutes_total += int(rebuilt_minutes)

                            rebuild_msg = ""
                            if rebuilt_days_total > 0:
                                rebuild_msg = (
                                    f" · rebuilt API-only source index ({rebuilt_days_total} days, "
                                    f"{rebuilt_minutes_total} minutes)"
                                )
                            st.success(f"✅ Deleted {deleted_count} coin directories ({size_str}){rebuild_msg}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error deleting: {e}")

                st.divider()

                # 2. Delete older than date
                st.subheader("2️⃣ Delete data older than date", divider="orange")
                st.caption("⚠️ Deletes individual files (days) older than the cutoff date, keeps coin directories")
                
                cutoff_date = st.date_input(
                    "Select cutoff date:",
                    value=None,
                    key=f"market_data_delete_older_date_{dataset_key}",
                    help=f"Deletes files dated before this date (e.g., 20241205.npz < 2025-01-01)"
                )

                if cutoff_date:
                    cutoff_str = cutoff_date.strftime("%Y%m%d")
                    
                    # Determine which coins to check based on selections (combine from both sources!)
                    coins_to_check = set()
                    
                    if selected_coin_from_table:
                        coins_to_check.add(selected_coin_from_table)
                    
                    # Also include selected coins from section 1
                    if selected_for_delete and "🔴 DELETE ALL COINS" not in selected_for_delete:
                        coins_to_check.update(selected_for_delete)
                    
                    # Initialize variables OUTSIDE the if block
                    from pathlib import Path
                    would_delete_files = 0
                    would_delete_size = 0
                    affected_coins_info = []
                    debug_info = []
                    scope_label = "no coins selected"
                    
                    if coins_to_check:
                        scope_label = f"{len(coins_to_check)} selected coins" if len(coins_to_check) > 1 else f"{list(coins_to_check)[0]}"
                        
                        for r in dataset_rows:
                            coin = str(r.get("coin", "")).strip().upper()
                            if coin not in coins_to_check:
                                continue
                            
                            actual_coin = str(r.get("coin", "")).strip()
                            actual_dataset = str(r.get("dataset", "")).strip()
                            coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                            
                            debug_info.append(f"Checking: {coin_dir} (exists={coin_dir.exists()})")
                            
                            if not coin_dir.exists():
                                continue
                            
                            coin_old_files = 0
                            coin_old_size = 0
                            all_files = []
                            
                            # Scan ALL files in coin directory
                            try:
                                for file_path in coin_dir.iterdir():
                                    if file_path.is_file():
                                        all_files.append(file_path.name)
                                        # Extract date from filename (try multiple patterns)
                                        fname = file_path.stem  # Without extension
                                        
                                        file_date = None
                                        
                                        # Pattern 1: 20241205.npz (8 digits)
                                        if len(fname) == 8 and fname.isdigit():
                                            file_date = fname
                                        # Pattern 2: 20241205-16.lz4 (8 digits + hyphen + hour)
                                        elif len(fname) >= 8 and fname[:8].isdigit():
                                            file_date = fname[:8]
                                        # Pattern 3: 2026-02-05.npz (ISO format YYYY-MM-DD)
                                        elif len(fname) == 10 and fname[4] == '-' and fname[7] == '-':
                                            # Convert YYYY-MM-DD to YYYYMMDD
                                            file_date = fname.replace('-', '')
                                        
                                        if file_date and file_date < cutoff_str:
                                            coin_old_files += 1
                                            coin_old_size += file_path.stat().st_size
                            except Exception as e:
                                debug_info.append(f"Error scanning {coin_dir}: {e}")
                            
                            debug_info.append(f"  Files: {len(all_files)}, Old: {coin_old_files}, Sample: {all_files[:3]}")
                            
                            if coin_old_files > 0:
                                would_delete_files += coin_old_files
                                would_delete_size += coin_old_size
                                affected_coins_info.append((coin, coin_old_files, coin_old_size))
                    
                    # Always show preview
                    size_str_old = _fmt_bytes(would_delete_size) if would_delete_size else "0 B"
                    
                    preview_container = st.container()
                    with preview_container:
                        st.info(f"📊 Cutoff: **{cutoff_date.strftime('%Y-%m-%d')}** | Scope: {scope_label}")
                        st.metric(
                            label="Files to delete",
                            value=f"{would_delete_files} files",
                            delta=f"{size_str_old}"
                        )
                    
                    if would_delete_files > 0:
                        # Show affected coins directly (no expander)
                        st.subheader(f"📋 Affected coins ({len(affected_coins_info)})", divider="gray")
                        for coin, nfiles, size in sorted(affected_coins_info, key=lambda x: -x[2]):
                            st.caption(f"• {coin}: {nfiles} files, {_fmt_bytes(size)}")

                        if st.button("🗑️ Delete old files", key=f"market_data_delete_older_btn_{dataset_key}", type="secondary"):
                            try:
                                deleted_count = 0
                                deleted_size = 0
                                coins_deleted_days: dict[str, set[str]] = {}  # Track deleted days per coin
                                
                                for r in dataset_rows:
                                    coin = str(r.get("coin", "")).strip().upper()
                                    if coin not in coins_to_check:
                                        continue
                                    
                                    actual_coin = str(r.get("coin", "")).strip()
                                    actual_dataset = str(r.get("dataset", "")).strip()
                                    actual_dataset_lower = actual_dataset.lower()
                                    coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                                    
                                    if not coin_dir.exists():
                                        continue
                                    
                                    # Delete old files (all file types)
                                    for file_path in coin_dir.iterdir():
                                        if not file_path.is_file():
                                            continue
                                        fname = file_path.stem
                                        
                                        file_date = None
                                        
                                        # Pattern 1: 20241205 (8 digits)
                                        if len(fname) == 8 and fname.isdigit():
                                            file_date = fname
                                        # Pattern 2: 20241205-16 (8 digits + hyphen + hour)
                                        elif len(fname) >= 8 and fname[:8].isdigit():
                                            file_date = fname[:8]
                                        # Pattern 3: 2026-02-05 (ISO format YYYY-MM-DD)
                                        elif len(fname) == 10 and fname[4] == '-' and fname[7] == '-':
                                            file_date = fname.replace('-', '')
                                        
                                        if file_date and file_date < cutoff_str:
                                            file_size = file_path.stat().st_size
                                            file_path.unlink()
                                            deleted_count += 1
                                            deleted_size += file_size
                                            
                                            # Track deleted day for index update
                                            if actual_dataset_lower in ("1m", "candles_1m"):
                                                if actual_coin not in coins_deleted_days:
                                                    coins_deleted_days[actual_coin] = set()
                                                coins_deleted_days[actual_coin].add(file_date)
                                
                                # Remove deleted days from 1m_src indexes
                                updated_count = 0
                                if coins_deleted_days:
                                    for coin, deleted_days in coins_deleted_days.items():
                                        removed = remove_days_from_index(
                                            exchange=str(exchange).lower(),
                                            coin=coin,
                                            days_to_remove=deleted_days
                                        )
                                        if removed > 0:
                                            updated_count += 1

                                index_msg = f" (updated {updated_count} source indexes)" if updated_count > 0 else ""
                                st.success(f"✅ Deleted {deleted_count} files ({_fmt_bytes(deleted_size)}){index_msg}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error deleting: {e}")
                                import traceback
                                st.code(traceback.format_exc())
                    else:
                        st.info(f"✅ No files older than {cutoff_str} in {scope_label}")

                else:
                    st.warning("⚠️ Select a coin or use section 1️⃣ to select coins before using date-based deletion")

                st.divider()

                # 3. Clear entire dataset (for this specific tab)
                st.subheader("3️⃣ Clear entire dataset", divider="red")
                st.warning(f"⚠️ This will delete ALL {dataset_label} data. This action cannot be undone!")

                if st.button(f"🗑️ Clear all {dataset_label}", key=f"market_data_clear_dataset_{dataset_key}", type="secondary"):
                    try:
                        dataset_dir = get_exchange_raw_root_dir(str(exchange).lower()) / dataset_key
                        
                        # Also clear 1m_src indexes for each coin if clearing 1m dataset
                        dataset_key_lower = dataset_key.lower()
                        cleaned_indexes = 0
                        if dataset_key_lower in ("1m", "candles_1m") and dataset_dir.exists():
                            # Get list of coins before deleting
                            coins_in_dataset = [d.name for d in dataset_dir.iterdir() if d.is_dir()]
                            for coin in coins_in_dataset:
                                src_dir = get_exchange_raw_root_dir(str(exchange).lower()) / "1m_src" / coin
                                if src_dir.exists():
                                    shutil.rmtree(src_dir)
                                    cleaned_indexes += 1
                        
                        # Now delete the dataset
                        if dataset_dir.exists():
                            shutil.rmtree(dataset_dir)
                        
                        index_msg = f" (cleaned {cleaned_indexes} source indexes)" if cleaned_indexes > 0 else ""
                        st.success(f"✅ {dataset_label} dataset cleared{index_msg}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

        sel_row_1m = None
        sel_row_1m_api = None
        sel_row_l2book = None
        sel_row_pb7 = None

        # Render selected dataset only (lazy)
        _ex_key = str(exchange).lower()
        if have_view == "1m":
            sel_row_1m, _del_rows = _render_dataset_table("1m", f"{_ex_key}_1m")
            _render_deletion_tools(_del_rows, f"{_ex_key}_1m", "1m candles", sel_row_1m)

        elif have_view == "1m_api":
            sel_row_1m_api, _del_rows = _render_dataset_table("1m_api", f"{_ex_key}_1m_api")
            _render_deletion_tools(_del_rows, f"{_ex_key}_1m_api", "1m API", sel_row_1m_api)

        elif have_view == "l2Book":
            sel_row_l2book, _del_rows = _render_dataset_table("l2Book", f"{_ex_key}_l2book")
            _render_deletion_tools(_del_rows, f"{_ex_key}_l2book", "l2Book", sel_row_l2book)

        elif have_view == "PB7 cache":
            pb7_rows = summarize_pb7_cache_inventory(str(exchange).lower(), limit=2000)
            if not pb7_rows:
                st.info("No PB7 cache files found for this exchange (expected path: pb7/caches/ohlcv/<exchange>/...).")
            else:
                import pandas as pd
                df_pb7 = pd.DataFrame(pb7_rows)
                df_pb7["_src_idx"] = list(range(len(df_pb7)))
                if not df_pb7.empty:
                    total_files = int(df_pb7["n_files"].sum()) if "n_files" in df_pb7.columns else 0
                    total_bytes = int(df_pb7["total_bytes"].sum()) if "total_bytes" in df_pb7.columns else 0
                    n_coins = int(df_pb7["coin"].nunique()) if "coin" in df_pb7.columns else 0
                    n_tf = int(df_pb7["timeframe"].nunique()) if "timeframe" in df_pb7.columns else 0

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("timeframes", n_tf)
                    c2.metric("coins", n_coins)
                    c3.metric("files", total_files)
                    c4.metric("size", _fmt_bytes(total_bytes))

                if "total_bytes" in df_pb7.columns:
                    df_pb7["size_mb"] = (df_pb7["total_bytes"].astype(float) / (1024.0 * 1024.0)).round(2)
                    df_pb7 = df_pb7.drop(columns=["total_bytes"])

                c_coin, c_kind = st.columns([0.7, 0.3])
                with c_coin:
                    pb7_coin_filter = str(
                        st.text_input(
                            "Filter by coin",
                            value=str(st.session_state.get("market_data_pb7_coin_filter", "") or ""),
                            key="market_data_pb7_coin_filter",
                            placeholder="e.g. GOOGL or BTC",
                        )
                        or ""
                    ).strip().upper()
                with c_kind:
                    pb7_kind_filter = st.selectbox(
                        "Filter by type",
                        options=["all", "stocks (xyz)", "crypto"],
                        index=0,
                        key="market_data_pb7_kind_filter",
                    )

                df_pb7_view = df_pb7.copy()
                if pb7_coin_filter:
                    df_pb7_view = df_pb7_view[
                        df_pb7_view["coin"].astype(str).str.upper().str.contains(pb7_coin_filter, na=False)
                    ]

                if pb7_kind_filter != "all":
                    coin_upper = df_pb7_view["coin"].astype(str).str.upper()
                    is_stock = coin_upper.str.startswith("XYZ:") | coin_upper.str.startswith("XYZ-")
                    if pb7_kind_filter == "stocks (xyz)":
                        df_pb7_view = df_pb7_view[is_stock]
                    else:
                        df_pb7_view = df_pb7_view[~is_stock]

                drop_display_cols = [c for c in ("exchange", "_src_idx") if c in df_pb7_view.columns]
                if drop_display_cols:
                    df_pb7_display = df_pb7_view.drop(columns=drop_display_cols)
                else:
                    df_pb7_display = df_pb7_view

                col_cfg = {
                    "size_mb": st.column_config.NumberColumn("size", format="%.2f MB")
                }
                event_pb7 = st.dataframe(
                    df_pb7_display,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    column_config=col_cfg,
                    key="market_data_pb7_cache_table",
                )

                _prev_sel_key = "market_data_prev_sel_pb7_cache"
                sel_indices = event_pb7.selection.rows if event_pb7 and event_pb7.selection else []
                prev_sel = st.session_state.get(_prev_sel_key, [])
                if sel_indices:
                    idx = sel_indices[0]
                    if 0 <= idx < len(df_pb7_view):
                        try:
                            src_idx = int(df_pb7_view.iloc[idx]["_src_idx"])
                        except Exception:
                            src_idx = idx
                    else:
                        src_idx = -1
                    if 0 <= src_idx < len(pb7_rows):
                        row = pb7_rows[src_idx]
                        tf = str(row.get("timeframe") or "").strip()
                        coin = str(row.get("coin") or "").strip()
                        if tf and coin:
                            st.session_state["market_data_heatmap_sel"] = (f"pb7_cache:{tf}", coin)
                            st.session_state["market_data_heatmap_tab"] = "pb7_cache"
                            sel_row_pb7 = {
                                "dataset": f"pb7_cache:{tf}",
                                "coin": coin,
                                "timeframe": tf,
                            }
                elif prev_sel and not preserve_selection_once:
                    st.session_state.pop("market_data_heatmap_sel", None)
                    st.session_state.pop("market_data_heatmap_tab", None)
                st.session_state[_prev_sel_key] = list(sel_indices)

                hm = st.session_state.get("market_data_heatmap_sel")
                hm_tab = st.session_state.get("market_data_heatmap_tab")
                if isinstance(hm, (tuple, list)) and len(hm) == 2 and hm_tab == "pb7_cache":
                    hm_ds = str(hm[0] or "").strip().lower()
                    hm_coin = str(hm[1] or "").strip()
                    hm_tf = hm_ds.split(":", 1)[1] if hm_ds.startswith("pb7_cache:") and ":" in hm_ds else ""
                    if hm_tf and hm_coin:
                        for row in pb7_rows:
                            row_tf = str(row.get("timeframe") or "").strip()
                            row_coin = str(row.get("coin") or "").strip()
                            if row_tf == hm_tf and row_coin == hm_coin:
                                sel_row_pb7 = {
                                    "dataset": f"pb7_cache:{row_tf}",
                                    "coin": row_coin,
                                    "timeframe": row_tf,
                                }
                                break

                if sel_row_pb7:
                    st.caption(f"Heatmap: PB7 cache {sel_row_pb7.get('timeframe')} / {sel_row_pb7.get('coin')}")
                else:
                    st.info("Click a row to display the heatmap. Use the sidebar refresh button to reload inventory.")

                st.caption("Read-only view of PB7 cache inventory from pb7/caches/ohlcv.")

        # Get the selected row from any of the tabs
        sel_row = sel_row_1m or sel_row_1m_api or sel_row_l2book or sel_row_pb7

        def _render_gap_heatmap() -> None:
            if sel_row:
                r = sel_row
                ex = str(exchange).lower()
                # Map UI exchange → storage directory (e.g. "binance" → "binanceusdm")
                _storage_ex_map = {"binance": "binanceusdm"}
                _storage_ex = _storage_ex_map.get(ex, ex)
                ds = str(r.get("dataset") or "")
                cn = str(r.get("coin") or "")
                ds_l = ds.strip().lower()
                is_stock_perp_1m = _is_hyperliquid_stock_perp_1m(exchange=ex, dataset=ds, coin=cn)
                tradfi_type = _tradfi_canonical_type_for_coin(cn) if is_stock_perp_1m else ""

                # For candles datasets: show only the gap from l2Book->today.
                start_day = None
                end_day = _date.today().strftime("%Y%m%d")
                if ds_l not in ("l2book", "1m", "candles_1m", "1m_api", "candles_1m_api"):
                    l2 = get_daily_hour_coverage_for_dataset(ex, "l2Book", cn)
                    l2_newest = str(l2.get("newest_day") or "") if isinstance(l2, dict) else ""
                    if l2_newest:
                        start_day = l2_newest

                if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                    # Candles view:
                    # - 1m: 1 row per day, 24 hour cells

                    # Choose presence resolution
                    if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                        day_counts = {}
                        if ds_l in ("1m", "candles_1m") and ex in ("hyperliquid", "binance"):
                            day_counts = get_daily_source_counts_for_range(
                                exchange=_storage_ex,
                                coin=cn,
                                start_day=start_day,
                                end_day=end_day,
                                lag_minutes=_missing_lag_minutes,
                                cutoff_ts_ms=None,
                            )

                        if isinstance(day_counts, dict) and day_counts:
                            try:
                                oldest_s = min(day_counts.keys())
                                newest_s = max(day_counts.keys())
                                dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
                                dt1 = _date.today()
                                if dt1 < _datetime.strptime(newest_s, "%Y%m%d").date():
                                    dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
                            except Exception:
                                dt0 = None
                                dt1 = None

                            if dt0 and dt1:
                                tradfi_sessions: dict[str, tuple[int, int]] = {}
                                tradfi_calendar_present = False
                                years: list[int] = []
                                cur = dt0
                                while cur <= dt1:
                                    y = int(cur.strftime("%Y"))
                                    if y not in years:
                                        years.append(y)
                                    cur = cur + _timedelta(days=1)
                                years = sorted(years)
                                max_days = 366
                                z = []
                                text = []
                                for y in years:
                                    days_in_year = 366 if calendar.isleap(int(y)) else 365
                                    row: list[float | None] = [None] * max_days
                                    row_text = [""] * max_days
                                    cur_day = dt0
                                    while cur_day <= dt1:
                                        day_s = cur_day.strftime("%Y%m%d")
                                        if not day_s.startswith(str(y)):
                                            cur_day = cur_day + _timedelta(days=1)
                                            continue
                                        try:
                                            doy = cur_day.timetuple().tm_yday
                                        except Exception:
                                            cur_day = cur_day + _timedelta(days=1)
                                            continue
                                        idx = doy - 1
                                        if 0 <= idx < days_in_year:
                                            counts = day_counts.get(day_s) or {}
                                            api = int(counts.get("api") or 0)
                                            l2b = int(counts.get("l2Book_mid") or 0)
                                            oth = int(counts.get("other_exchange") or 0)
                                            miss = int(counts.get("missing") or 0)
                                            if is_stock_perp_1m:
                                                is_holiday = _is_tradfi_market_holiday(cur_day, tradfi_type)
                                                sess = tradfi_sessions.get(day_s)
                                                if sess is not None:
                                                    expected_minutes = len(
                                                        _tradfi_expected_minute_indices_from_session(
                                                            day=cur_day,
                                                            session_start_ms=int(sess[0]),
                                                            session_end_ms=int(sess[1]),
                                                        )
                                                    )
                                                elif tradfi_calendar_present:
                                                    expected_minutes = 0
                                                else:
                                                    expected_minutes = len(_tradfi_expected_indices_for_type(cur_day, tradfi_type))
                                                if is_holiday:
                                                    expected_minutes = 0
                                                covered_minutes = api + l2b + oth
                                                miss = max(0, int(expected_minutes) - int(covered_minutes))
                                                if expected_minutes == 0 and covered_minutes == 0:
                                                    if is_holiday:
                                                        row[idx] = 0.25
                                                        row_text[idx] = f"{day_s} | market holiday"
                                                    else:
                                                        row[idx] = None
                                                        row_text[idx] = f"{day_s} | non-trading session"
                                                    cur_day = cur_day + _timedelta(days=1)
                                                    continue
                                            elif not counts:
                                                miss = 1440
                                            if miss > 0:
                                                row[idx] = 0.0
                                            elif oth > 0:
                                                row[idx] = 0.75 if oth < 5 else 0.5
                                            else:
                                                row[idx] = 1.0
                                            row_text[idx] = (
                                                f"{day_s} | api={api} l2Book={l2b} other={oth} missing={miss}"
                                            )
                                        cur_day = cur_day + _timedelta(days=1)
                                    z.append(row)
                                    text.append(row_text)

                                fig = go.Figure(
                                    data=go.Heatmap(
                                        z=z,
                                        x=list(range(1, max_days + 1)),
                                        y=[str(y) for y in years],
                                        text=text,
                                        hovertemplate="%{text}<extra></extra>",
                                        colorscale=[
                                            [0.0, "#b23b3b"],
                                            [0.24, "#b23b3b"],
                                            [0.25, "#7e57c2"],
                                            [0.49, "#7e57c2"],
                                            [0.5, "#ef6c00"],
                                            [0.74, "#ef6c00"],
                                            [0.75, "#7cb342"],
                                            [0.99, "#7cb342"],
                                            [1.0, "#2e7d32"],
                                        ],
                                        zmin=0,
                                        zmax=1,
                                        showscale=False,
                                        xgap=1,
                                        ygap=1,
                                    )
                                )
                                fig.update_layout(
                                    height=120 + (len(years) * 26),
                                    margin=dict(l=10, r=10, t=20, b=20),
                                    xaxis=dict(tickangle=-45, automargin=True, showgrid=False),
                                    yaxis=dict(
                                        autorange="reversed",
                                        showgrid=False,
                                        type="category",
                                        categoryorder="array",
                                        categoryarray=[str(y) for y in years],
                                        tickmode="array",
                                        tickvals=[str(y) for y in years],
                                        ticktext=[str(y) for y in years],
                                    ),
                                )
                                st.markdown(
                                    "<span style='display:inline-block;padding:6px;border-radius:4px;background:#2e7d32;color:#fff;margin-right:8px;'>HL only</span>"
                                    "<span style='display:inline-block;padding:6px;border-radius:4px;background:#7cb342;color:#fff;margin-right:8px;'>other_exchange &lt; 5 min</span>"
                                    "<span style='display:inline-block;padding:6px;border-radius:4px;background:#ef6c00;color:#fff;margin-right:8px;'>other_exchange ≥ 5 min</span>"
                                    "<span style='display:inline-block;padding:6px;border-radius:4px;background:#7e57c2;color:#fff;margin-right:8px;'>market holiday</span>"
                                    "<span style='display:inline-block;padding:6px;border-radius:4px;background:#b23b3b;color:#fff;margin-right:8px;'>missing minutes</span>",
                                    unsafe_allow_html=True,
                                )
                                st.caption("Overview (days). Select a month below to inspect minutes.")
                                st.plotly_chart(fig, use_container_width=True)

                                # Build month list from date range
                                chart_full_start_day = str(start_day or "")
                                chart_full_end_day = str(end_day or "")
                                month_list: list[str] = []
                                if dt0 and dt1:
                                    cur_m = dt0.replace(day=1)
                                    end_m = dt1.replace(day=1)
                                    while cur_m <= end_m:
                                        month_list.append(cur_m.strftime("%Y-%m"))
                                        # advance to next month
                                        if cur_m.month == 12:
                                            cur_m = cur_m.replace(year=cur_m.year + 1, month=1)
                                        else:
                                            cur_m = cur_m.replace(month=cur_m.month + 1)
                                if not month_list:
                                    month_list = [dt0.strftime("%Y-%m")] if dt0 else []
                                chart_full_start_day = dt0.strftime("%Y%m%d") if dt0 else str(start_day or "")
                                chart_full_end_day = dt1.strftime("%Y%m%d") if dt1 else str(end_day or "")
                                sel_key = f"market_data_1m_month_{cn}"
                                if month_list and sel_key not in st.session_state:
                                    st.session_state[sel_key] = month_list[-1]
                                cur_month = st.session_state.get(sel_key) or (month_list[-1] if month_list else "")
                                cur_idx = month_list.index(cur_month) if cur_month in month_list else len(month_list) - 1

                                def _go_prev(_key=sel_key, _ml=month_list, _ci=cur_idx):
                                    if _ci > 0:
                                        st.session_state[_key] = _ml[_ci - 1]

                                def _go_next(_key=sel_key, _ml=month_list, _ci=cur_idx):
                                    if _ci < len(_ml) - 1:
                                        st.session_state[_key] = _ml[_ci + 1]

                                # Small chevron buttons like Strategy Explorer
                                c_prev, c_sel, c_next = st.columns([0.04, 0.92, 0.04], vertical_alignment="bottom")
                                with c_prev:
                                    st.button(":material/chevron_left:", key=f"{sel_key}_prev", disabled=cur_idx <= 0, on_click=_go_prev)
                                with c_sel:
                                    sel_month = st.selectbox(
                                        "Select month for minute view",
                                        options=month_list,
                                        index=cur_idx,
                                        key=sel_key,
                                    )
                                with c_next:
                                    st.button(":material/chevron_right:", key=f"{sel_key}_next", disabled=cur_idx >= len(month_list) - 1, on_click=_go_next)
                                if sel_month:
                                    import calendar as _cal
                                    _sm_year = int(sel_month[:4])
                                    _sm_mon = int(sel_month[5:7])
                                    _last_day = _cal.monthrange(_sm_year, _sm_mon)[1]
                                    start_day = f"{_sm_year:04d}{_sm_mon:02d}01"
                                    end_day = f"{_sm_year:04d}{_sm_mon:02d}{_last_day:02d}"

                        show_market_holiday_overlay = True
                        show_out_of_session_overlay = True
                        if is_stock_perp_1m:
                            c_holiday, c_oos, _ = st.columns([0.28, 0.34, 0.38], vertical_alignment="bottom")
                            with c_holiday:
                                show_market_holiday_overlay = bool(
                                    st.checkbox(
                                        "Highlight market holidays",
                                        value=bool(st.session_state.get("market_data_show_market_holiday_overlay", True)),
                                        key="market_data_show_market_holiday_overlay",
                                    )
                                )
                            with c_oos:
                                show_out_of_session_overlay = bool(
                                    st.checkbox(
                                        "Highlight expected out-of-session gaps",
                                        value=bool(st.session_state.get("market_data_show_out_of_session_overlay", True)),
                                        key="market_data_show_out_of_session_overlay",
                                    )
                                )

                        hp = get_minute_presence_for_dataset(
                            _storage_ex,
                            ds,
                            cn,
                            start_day=start_day,
                            end_day=end_day,
                        )
                        present = hp.get("days") if isinstance(hp, dict) else {}
                        if not isinstance(present, dict) or not present:
                            st.info("No minute candles found for this selection.")
                            return

                    oldest_s = str(hp.get("oldest_day") or "")
                    newest_s = str(hp.get("newest_day") or "")
                    try:
                        dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
                        dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
                    except Exception:
                        st.info("No date range found for this selection.")
                        return

                    colorscale = [
                        [0.0, "#b23b3b"],
                        [1.0, "#2e7d32"],
                    ]

                    if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                        # Days split into two 12-hour rows (00-11, 12-23)
                        days_list: list[_date] = []
                        cur = dt0
                        while cur <= dt1:
                            days_list.append(cur)
                            cur = cur + _timedelta(days=1)

                        z = []
                        text = []
                        y_labels = []

                        # src -> code mapping (discrete)
                        src_code = {
                            None: 0,
                            "missing": 0,
                            "api": 2,
                            "best": 3,
                            "other_exchange": 4,
                            "binance_perp_usdt": 4,
                            "l2Book_mid": 5,
                        }

                        # colorscale: 0 missing (red), 1 filled (purple), 2 api (green), 3 best (teal), 4 other_exchange (orange), 5 l2book (blue)
                        if is_stock_perp_1m:
                            # -2 market holiday, -1 expected out-of-session gap (neutral gray)
                            colorscale = [
                                [0.0, "#7e57c2"],
                                [1 / 7, "#4e4e4e"],
                                [2 / 7, "#b23b3b"],
                                [3 / 7, "#6a1b9a"],
                                [4 / 7, "#2e7d32"],
                                [5 / 7, "#00897b"],
                                [6 / 7, "#ef6c00"],
                                [1.0, "#1e88e5"],
                            ]
                        else:
                            colorscale = [
                                [0.0, "#b23b3b"],
                                [0.2, "#6a1b9a"],
                                [0.4, "#2e7d32"],
                                [0.6, "#00897b"],
                                [0.8, "#ef6c00"],
                                [1.0, "#1e88e5"],
                            ]

                        for d in days_list:
                            day_s = d.strftime("%Y%m%d")
                            # present[day_s] is {HH: {MM: src}}
                            hours_map = present.get(day_s) if isinstance(present.get(day_s), dict) else {}
                            hours_map = hours_map if isinstance(hours_map, dict) else {}
                            expected_indices = None
                            if is_stock_perp_1m:
                                if str(tradfi_type or "").strip().lower() == "fx":
                                    holiday_session_indices = set(range(1440))
                                else:
                                    holiday_session_indices = _tradfi_expected_minute_indices(d)
                                is_market_holiday = _is_tradfi_market_holiday(d, tradfi_type)
                                sess_map, sess_present = ({}, False)
                                sess = (sess_map or {}).get(day_s)
                                if sess is not None:
                                    expected_indices = _tradfi_expected_minute_indices_from_session(
                                        day=d,
                                        session_start_ms=int(sess[0]),
                                        session_end_ms=int(sess[1]),
                                    )
                                elif sess_present:
                                    expected_indices = set()
                                else:
                                    expected_indices = _tradfi_expected_indices_for_type(d, tradfi_type)
                                if is_market_holiday:
                                    expected_indices = set()
                            else:
                                is_market_holiday = False
                                holiday_session_indices = set()

                            for block_start in (0, 12):
                                row = []
                                row_text = []
                                for h in range(block_start, block_start + 12):
                                    hh = f"{h:02d}"
                                    mins_map = hours_map.get(hh) or {}
                                    mins_map = mins_map if isinstance(mins_map, dict) else {}
                                    for minute in range(60):
                                        minute_idx = (h * 60) + int(minute)
                                        src = mins_map.get(minute)
                                        code = int(src_code.get(str(src), src_code.get(src, 0)))
                                        # Preserve real source data colors even outside expected session.
                                        # Out-of-session markers are only for truly missing minutes.
                                        if code == 0:
                                            if (
                                                show_market_holiday_overlay
                                                and is_market_holiday
                                                and minute_idx in holiday_session_indices
                                            ):
                                                row.append(-2)
                                                hhmm = f"{h:02d}:{minute:02d}"
                                                row_text.append(f"{day_s} {hhmm} (market holiday)")
                                                continue
                                            if (
                                                show_out_of_session_overlay
                                                and is_stock_perp_1m
                                                and expected_indices is not None
                                                and minute_idx not in expected_indices
                                            ):
                                                row.append(-1)
                                                hhmm = f"{h:02d}:{minute:02d}"
                                                row_text.append(f"{day_s} {hhmm} (expected out-of-session gap)")
                                                continue
                                        row.append(code)
                                        # hover text per minute
                                        hhmm = f"{h:02d}:{minute:02d}"
                                        src_label = str(src) if src is not None else "missing"
                                        row_text.append(f"{day_s} {hhmm} ({src_label})")
                                z.append(row)
                                text.append(row_text)
                                y_labels.append(f"{day_s} {block_start:02d}-{block_start+11:02d}")

                        if not z:
                            st.info("No minute presence found for this selection.")
                            return

                        fig = go.Figure(
                            data=go.Heatmap(
                                z=z,
                                x=list(range(720)),
                                y=[str(y) for y in y_labels],
                                text=text,
                                hovertemplate="%{text}<extra></extra>",
                                colorscale=colorscale,
                                zmin=-2 if is_stock_perp_1m else 0,
                                zmax=5,
                                showscale=False,
                                xgap=1,
                                ygap=1,
                            )
                        )
                        fig.update_layout(
                            height=max(300, 80 + (len(y_labels) * 18)),
                            margin=dict(l=10, r=10, t=20, b=20),
                            xaxis=dict(tickmode="array", tickvals=list(range(0, 720, 60)), ticktext=[f"{x:02d}h" for x in range(0, 12)]),
                            yaxis=dict(autorange="reversed", showgrid=False),
                        )
                        if is_stock_perp_1m:
                            holiday_legend = (
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#7e57c2;color:#fff;margin-right:8px;'>market holiday</span>"
                                if show_market_holiday_overlay
                                else ""
                            )
                            oos_legend = (
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#4e4e4e;color:#fff;margin-right:8px;'>expected out-of-session gap</span>"
                                if show_out_of_session_overlay
                                else ""
                            )
                            st.markdown(
                                holiday_legend
                                + oos_legend
                                +
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#b23b3b;color:#fff;margin-right:8px;'>missing</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#2e7d32;color:#fff;margin-right:8px;'>api</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#00897b;color:#fff;margin-right:8px;'>best (NPZ fallback)</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#ef6c00;color:#fff;margin-right:8px;'>other_exchange</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#1e88e5;color:#fff;margin-right:8px;'>l2Book_mid</span>",
                                unsafe_allow_html=True,
                            )
                        else:
                            st.markdown(
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#b23b3b;color:#fff;margin-right:8px;'>missing</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#2e7d32;color:#fff;margin-right:8px;'>api</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#00897b;color:#fff;margin-right:8px;'>best (NPZ fallback)</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#ef6c00;color:#fff;margin-right:8px;'>other_exchange</span>"
                                "<span style='display:inline-block;padding:6px;border-radius:4px;background:#1e88e5;color:#fff;margin-right:8px;'>l2Book_mid</span>",
                                unsafe_allow_html=True,
                            )
                        st.plotly_chart(fig, use_container_width=True)

                        with st.expander("OHLCV chart", expanded=False):
                            show_volume = True
                            chart_start_day = str(chart_full_start_day or "")
                            chart_end_day = str(chart_full_end_day or "")
                            ohlcv_df = _load_ohlcv_from_npz_range(
                                exchange=_storage_ex,
                                dataset=ds,
                                coin=cn,
                                start_day=chart_start_day,
                                end_day=chart_end_day,
                            )
                            if ohlcv_df.empty:
                                st.info("No OHLCV candles found for selected range.")
                            else:
                                import pandas as pd

                                full_min_ts = int(ohlcv_df["ts"].iloc[0])
                                full_max_ts = int(ohlcv_df["ts"].iloc[-1])

                                # ── Lazy multi-resolution: bidirectional component ──
                                from ohlcv_component import ohlcv_chart as _ohlcv_chart

                                # Cache keys (per symbol) – prefix with _c_ to avoid collision
                                # with the component widget key (ohlcv_zoom_{ex}_{cn})
                                _pyr_key = f"_c_ohlcv_pyr_{ex}_{cn}"
                                _zoom_key = f"_c_ohlcv_zr_{ex}_{cn}"
                                _fp_key = f"_c_ohlcv_fp_{ex}_{cn}"

                                # Invalidate cache when data changes
                                _fp = f"{full_min_ts}_{full_max_ts}_{len(ohlcv_df)}"
                                if st.session_state.get(_fp_key) != _fp:
                                    st.session_state[_fp_key] = _fp
                                    st.session_state.pop(_pyr_key, None)
                                    st.session_state.pop(_zoom_key, None)

                                # Base layers: only 1d + 1h (always fast)
                                pyramid = {
                                    "1d": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1D")),
                                    "1h": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1h")),
                                }

                                # Merge cached fine layers from previous zoom
                                _cached_fine = st.session_state.get(_pyr_key, {})
                                pyramid.update(_cached_fine)

                                chart_h = 630
                                _cached_zoom = st.session_state.get(_zoom_key)

                                # Derive display name for coin
                                _coin_display = str(cn or "")
                                if _coin_display.upper().startswith("XYZ-"):
                                    _coin_display = _coin_display[4:]
                                for _sfx in ("_USDC:USDC", "_USDT:USDT", "_USDC_USDC", "_USDT_USDT", "/USDC:USDC", "/USDT:USDT"):
                                    if _coin_display.upper().endswith(_sfx):
                                        _coin_display = _coin_display[: -len(_sfx)]
                                        break

                                # Load stock-split dates for TradFi coins
                                _split_dates_for_chart: list[dict] = []
                                _cn_upper = str(cn or "").upper()
                                if _cn_upper.startswith("XYZ:") or _cn_upper.startswith("XYZ-"):
                                    try:
                                        from hyperliquid_best_1m import (
                                            _load_split_factors_from_cache as _lsfc,
                                        )
                                        # Extract bare ticker: XYZ-GOOGL_USDC:USDC → GOOGL
                                        _tail = _cn_upper[4:].strip()
                                        for _sfx in ("_USDC:USDC", "_USDT:USDT", "_USDC_USDC", "_USDT_USDT", "/USDC:USDC", "/USDT:USDT"):
                                            if _tail.endswith(_sfx):
                                                _tail = _tail[: -len(_sfx)]
                                                break
                                        _ticker = _tail.strip(" _:-")
                                        if _ticker:
                                            _splits = _lsfc(_ticker)
                                            if _splits:
                                                # Only include splits within actual OHLCV data range
                                                _earliest_date = ""
                                                _1d = pyramid.get("1d")
                                                if _1d and _1d.get("ts"):
                                                    _earliest_ts = min(_1d["ts"])
                                                    from datetime import datetime as _dt, timezone as _tz
                                                    _earliest_date = _dt.fromtimestamp(_earliest_ts / 1000, tz=_tz.utc).strftime("%Y-%m-%d")
                                                _split_dates_for_chart = [
                                                    {"date": str(d), "factor": f}
                                                    for d, f in _splits
                                                    if not _earliest_date or str(d) >= _earliest_date
                                                ]
                                    except Exception:
                                        pass

                                _zoom_result = _ohlcv_chart(
                                    layers=pyramid,
                                    zoom_range=_cached_zoom,
                                    show_volume=show_volume,
                                    height=chart_h,
                                    split_dates=_split_dates_for_chart or None,
                                    coin_name=_coin_display,
                                    key=f"ohlcv_zoom_{ex}_{cn}",
                                )

                                # Handle zoom request from JS
                                if _zoom_result and isinstance(_zoom_result, dict) and _zoom_result.get("need_tf"):
                                    _needed = _zoom_result["need_tf"]
                                    if _needed not in _cached_fine:
                                        import pandas as _pd
                                        # Parse requested window and add 3× padding
                                        _rs = _pd.Timestamp(_zoom_result["range_start"])
                                        _re = _pd.Timestamp(_zoom_result["range_end"])
                                        _span_ms = (_re - _rs).total_seconds() * 1000
                                        _center_ms = (_rs.timestamp() + _re.timestamp()) / 2 * 1000
                                        _half_win = max(_span_ms * 1.5, 30 * 86400_000)  # min 30 days
                                        _half_win = min(_half_win, 90 * 86400_000)        # max 90 days
                                        _ws = _center_ms - _half_win
                                        _we = _center_ms + _half_win
                                        _win_df = ohlcv_df[(ohlcv_df["ts"] >= _ws) & (ohlcv_df["ts"] <= _we)]
                                        if _win_df.empty:
                                            _win_df = ohlcv_df  # fallback to full data

                                        _fine: dict = {}
                                        for _ftf, _frule in [("15m", "15min"), ("5m", "5min"), ("1m", None)]:
                                            if _ftf not in pyramid:
                                                if _frule is None:
                                                    _fine[_ftf] = _df_to_columnar(_win_df)
                                                else:
                                                    _fine[_ftf] = _df_to_columnar(_resample_ohlcv(_win_df, _frule))

                                        if _fine:
                                            st.session_state[_pyr_key] = {**_cached_fine, **_fine}
                                            st.session_state[_zoom_key] = [
                                                _zoom_result["range_start"],
                                                _zoom_result["range_end"],
                                            ]
                                            st.rerun()
                        return


                # Default view (incl. l2Book): render year rows with day-of-year columns.
                if ds_l.startswith("pb7_cache:"):
                    tf = str(ds.split(":", 1)[1] if ":" in ds else (r.get("timeframe") or "1m")).strip() or "1m"
                    cov = get_daily_presence_for_pb7_cache(
                        ex,
                        tf,
                        cn,
                        start_day=start_day,
                        end_day=end_day,
                    )
                else:
                    cov = get_daily_hour_coverage_for_dataset(
                        ex,
                        ds,
                        cn,
                        start_day=start_day,
                        end_day=end_day,
                    )
                days = cov.get("days") if isinstance(cov, dict) else []
                if isinstance(days, list) and days:
                    years: list[int] = []
                    for d in days:
                        try:
                            y = int(str(d.get("day") or "")[0:4])
                            if y not in years:
                                years.append(y)
                        except Exception:
                            continue
                    years = sorted(years)
                    max_days = 366
                    z = []
                    text = []
                    for y in years:
                        row = [None] * max_days
                        row_text = [""] * max_days
                        for d in days:
                            day_s = str(d.get("day") or "")
                            if not day_s.startswith(str(y)):
                                continue
                            try:
                                dt = _datetime.strptime(day_s, "%Y%m%d").date()
                                doy = dt.timetuple().tm_yday
                            except Exception:
                                continue
                            status = int(d.get("status") or 0)
                            hrs = int(d.get("hours") or 0)
                            idx = doy - 1
                            if 0 <= idx < max_days:
                                row[idx] = status
                                row_text[idx] = f"{day_s} | hours={hrs}/24"
                        z.append(row)
                        text.append(row_text)

                    fig = go.Figure(
                        data=go.Heatmap(
                            z=z,
                            x=list(range(1, max_days + 1)),
                            y=[str(y) for y in years],
                            text=text,
                            hovertemplate="%{text}<extra></extra>",
                            colorscale=[
                                [0.0, "#b23b3b"],
                                [0.5, "#c9a227"],
                                [1.0, "#2e7d32"],
                            ],
                            zmin=0,
                            zmax=2,
                            showscale=False,
                            xgap=1,
                            ygap=1,
                        )
                    )
                    fig.update_layout(
                        height=120 + (len(years) * 26),
                        margin=dict(l=10, r=10, t=20, b=20),
                        xaxis=dict(tickangle=-45, automargin=True, showgrid=False),
                        yaxis=dict(autorange="reversed", showgrid=False),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    if ds_l.startswith("pb7_cache:"):
                        with st.expander("OHLCV chart", expanded=False):
                            _pb7_tf = str(ds.split(":", 1)[1] if ":" in ds else (r.get("timeframe") or "1m")).strip() or "1m"
                            ohlcv_df = _load_ohlcv_from_pb7_cache(
                                exchange=ex,
                                timeframe=_pb7_tf,
                                coin=cn,
                                start_day=str(start_day or ""),
                                end_day=str(end_day or ""),
                            )
                            if ohlcv_df.empty:
                                st.info("No OHLCV candles found for selected range.")
                            else:
                                import pandas as pd

                                full_min_ts = int(ohlcv_df["ts"].iloc[0])
                                full_max_ts = int(ohlcv_df["ts"].iloc[-1])

                                from ohlcv_component import ohlcv_chart as _ohlcv_chart

                                _pyr_key = f"_c_ohlcv_pyr_pb7_{_pb7_tf}_{ex}_{cn}"
                                _zoom_key = f"_c_ohlcv_zr_pb7_{_pb7_tf}_{ex}_{cn}"
                                _fp_key = f"_c_ohlcv_fp_pb7_{_pb7_tf}_{ex}_{cn}"

                                _fp = f"{full_min_ts}_{full_max_ts}_{len(ohlcv_df)}"
                                if st.session_state.get(_fp_key) != _fp:
                                    st.session_state[_fp_key] = _fp
                                    st.session_state.pop(_pyr_key, None)
                                    st.session_state.pop(_zoom_key, None)

                                # Build base pyramid layers
                                pyramid: dict = {
                                    "1d": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1D")),
                                    "1h": _df_to_columnar(_resample_ohlcv(ohlcv_df, "1h")),
                                }
                                # Include native timeframe if finer than 1h
                                _tf_resample_map = {"1m": None, "5m": "5min", "15m": "15min"}
                                _native_rule = _tf_resample_map.get(_pb7_tf)
                                if _pb7_tf in _tf_resample_map:
                                    if _native_rule is None:
                                        pyramid[_pb7_tf] = _df_to_columnar(ohlcv_df)
                                    else:
                                        pyramid[_pb7_tf] = _df_to_columnar(_resample_ohlcv(ohlcv_df, _native_rule))

                                _cached_fine = st.session_state.get(_pyr_key, {})
                                pyramid.update(_cached_fine)

                                _cached_zoom = st.session_state.get(_zoom_key)
                                _coin_display = str(cn or "")
                                for _sfx in ("_USDC:USDC", "_USDT:USDT", "_USDC_USDC", "_USDT_USDT", "/USDC:USDC", "/USDT:USDT"):
                                    if _coin_display.upper().endswith(_sfx):
                                        _coin_display = _coin_display[: -len(_sfx)]
                                        break

                                _zoom_result = _ohlcv_chart(
                                    layers=pyramid,
                                    zoom_range=_cached_zoom,
                                    show_volume=True,
                                    height=630,
                                    coin_name=_coin_display,
                                    key=f"ohlcv_zoom_pb7_{_pb7_tf}_{ex}_{cn}",
                                )

                                if _zoom_result and isinstance(_zoom_result, dict) and _zoom_result.get("need_tf"):
                                    _needed = _zoom_result["need_tf"]
                                    if _needed not in _cached_fine:
                                        import pandas as _pd
                                        _rs = _pd.Timestamp(_zoom_result["range_start"])
                                        _re = _pd.Timestamp(_zoom_result["range_end"])
                                        _span_ms = (_re - _rs).total_seconds() * 1000
                                        _center_ms = (_rs.timestamp() + _re.timestamp()) / 2 * 1000
                                        _half_win = max(_span_ms * 1.5, 30 * 86400_000)
                                        _half_win = min(_half_win, 90 * 86400_000)
                                        _ws = _center_ms - _half_win
                                        _we = _center_ms + _half_win
                                        _win_df = ohlcv_df[(ohlcv_df["ts"] >= _ws) & (ohlcv_df["ts"] <= _we)]
                                        if _win_df.empty:
                                            _win_df = ohlcv_df
                                        _fine: dict = {}
                                        for _ftf, _frule in [("15m", "15min"), ("5m", "5min"), ("1m", None)]:
                                            if _ftf not in pyramid:
                                                if _frule is None:
                                                    _fine[_ftf] = _df_to_columnar(_win_df)
                                                else:
                                                    _fine[_ftf] = _df_to_columnar(_resample_ohlcv(_win_df, _frule))
                                        if _fine:
                                            st.session_state[_pyr_key] = {**_cached_fine, **_fine}
                                            st.session_state[_zoom_key] = [
                                                _zoom_result["range_start"],
                                                _zoom_result["range_end"],
                                            ]
                                            st.rerun()

                else:
                    st.info("No day range found for this selection.")

        if sel_row:
            gaps_active = False
            try:
                gaps_active = bool(list_jobs(states=["pending", "running"], limit=1))
            except Exception:
                gaps_active = False

            if gaps_active and _supports_fragment_run_every() and not _is_background_refresh_paused():
                @st.fragment(run_every=5)
                def _gap_fragment():
                    # Stop auto-refresh when no jobs remain
                    try:
                        still_active = bool(list_jobs(states=["pending", "running"], limit=1))
                    except Exception:
                        still_active = False
                    if not still_active:
                        st.rerun()
                    _render_gap_heatmap()
            else:
                @st.fragment
                def _gap_fragment():
                    _render_gap_heatmap()

            _gap_fragment()

    if main_view == "Activity log":
        view_log_filtered("MarketData")


# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

set_page_config("Market Data")
render_header_with_guide(
    "Market Data",
    guide_callback=lambda: _help_modal(default_topic="Market Data"),
    guide_key="market_data_guide_btn",
)

with st.sidebar:
    if st.button(":material/refresh:", help="Reload page"):
        # Preserve active table selection/overview across this refresh rerun.
        st.session_state["market_data_preserve_selection_once"] = True
        # Button interaction already triggers a rerun; avoid forcing an extra rerun
        # here so current segmented-control state is preserved.

    _pid = read_worker_pid()
    _running = bool(_pid and is_pid_running(int(_pid)))
    _worker_icon = ":material/stop_circle:" if _running else ":material/play_disabled:"
    _worker_help = "Worker running — click to stop" if _running else "Worker stopped"
    if st.button(_worker_icon, key="market_data_sidebar_worker_toggle", help=_worker_help, disabled=not _running):
        try:
            if _pid and is_pid_running(int(_pid)):
                os.kill(int(_pid), signal.SIGTERM)
            clear_worker_pid()
            st.rerun()
        except Exception as e:
            error_popup(str(e))

view_market_data()
