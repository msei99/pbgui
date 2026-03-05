"""
FastAPI router: Gap / Coverage Heatmap charts.

Endpoints return Plotly figure JSON so the frontend (Vanilla JS + Plotly.js)
can render them without any Streamlit involvement.

All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import calendar
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Query
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from api.auth import SessionToken, require_auth

router = APIRouter()

# --------------------------------------------------------------------------- helpers

_STORAGE_EXCHANGE_MAP = {"binance": "binanceusdm"}

def _storage_ex(exchange: str) -> str:
    return _STORAGE_EXCHANGE_MAP.get(str(exchange).lower().strip(), str(exchange).lower().strip())


def _get_missing_lag_minutes(exchange: str) -> int:
    try:
        from pbgui_purefunc import load_ini
        ex = str(exchange).lower().strip()
        if ex in ("binanceusdm", "binance"):
            sec = int(str(load_ini("binance_data", "latest_1m_interval_seconds") or "3600").strip())
        elif ex == "bybit":
            sec = int(str(load_ini("bybit_data", "latest_1m_interval_seconds") or "3600").strip())
        else:
            sec = int(str(load_ini("pbdata", "latest_1m_interval_seconds") or "1800").strip())
        return max(0, int((sec + 59) // 60))
    except Exception:
        return 30


def _data_dir_for(exchange: str, dataset: str, coin: str) -> Path | None:
    """Return the primary data directory for a coin/dataset (for mtime checks)."""
    from market_data import get_market_data_root_dir
    try:
        root = get_market_data_root_dir()
        ds_l = str(dataset).lower().strip()
        if ds_l.startswith("pb7_cache:"):
            # PB7 cache lives under pb7/ dir — just watch the generic ohlcv dir
            return root / str(exchange).lower() / "1m" / str(coin)
        return root / _storage_ex(exchange) / dataset / str(coin)
    except Exception:
        return None


def _latest_mtime(exchange: str, dataset: str, coin: str) -> float:
    """Return the newest mtime across data files for this coin/dataset."""
    d = _data_dir_for(exchange, dataset, coin)
    if not d or not d.exists():
        return 0.0
    try:
        mtimes = [p.stat().st_mtime for p in d.iterdir() if p.is_file()]
        # also check source index
        from market_data_sources import get_source_index_path
        idx = get_source_index_path(_storage_ex(exchange), coin)
        if idx.exists():
            mtimes.append(idx.stat().st_mtime)
        return float(max(mtimes)) if mtimes else 0.0
    except Exception:
        return 0.0


# --------------------------------------------------------------------------- BAR COLORS

_BAR_COLORS: dict[str, str] = {
    "l2Book_mid":     "#1e88e5",
    "api":            "#7e57c2",
    "other_exchange": "#ef6c00",
    "missing":        "#b23b3b",
    "out-of-session": "#2c3e50",
    "market holiday": "#546e7a",
}

_LEGEND_SPAN = (
    lambda label, color:
    f"<span style='display:inline-block;padding:6px;border-radius:4px;"
    f"background:{color};color:#fff;margin-right:8px;'>{label}</span>"
)


# --------------------------------------------------------------------------- /info

@router.get("/info")
def get_heatmap_info(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Return metadata about the coin/dataset combination:
    - is_stock_perp:  bool
    - is_candles:     bool
    - tradfi_type:    str
    - months:         list[str] YYYY-MM (only for candles datasets)
    - mtime:          float  (newest data file mtime, Unix seconds)
    """
    from market_data_tradfi import (
        is_hyperliquid_stock_perp_1m,
        tradfi_canonical_type_for_coin,
    )
    from market_data_sources import get_daily_source_counts_for_range

    ex = str(exchange).lower().strip()
    ds = str(dataset).strip()
    ds_l = ds.lower()
    cn = str(coin).strip()

    is_sp = is_hyperliquid_stock_perp_1m(exchange=ex, dataset=ds, coin=cn)
    tradfi_type = tradfi_canonical_type_for_coin(cn) if is_sp else ""
    is_candles = ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api")

    months: list[str] = []
    if is_candles:
        try:
            lag = _get_missing_lag_minutes(ex)
            day_counts = get_daily_source_counts_for_range(
                exchange=_storage_ex(ex),
                coin=cn,
                start_day=None,
                end_day=None,
                lag_minutes=lag,
                cutoff_ts_ms=None,
            )
            if isinstance(day_counts, dict) and day_counts:
                oldest_s = min(day_counts.keys())
                newest_s = max(day_counts.keys())
                dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
                dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
                cur_m = dt0.replace(day=1)
                end_m = dt1.replace(day=1)
                while cur_m <= end_m:
                    months.append(cur_m.strftime("%Y-%m"))
                    if cur_m.month == 12:
                        cur_m = cur_m.replace(year=cur_m.year + 1, month=1)
                    else:
                        cur_m = cur_m.replace(month=cur_m.month + 1)
        except Exception:
            pass

    return {
        "exchange": ex,
        "dataset": ds,
        "coin": cn,
        "is_stock_perp": is_sp,
        "tradfi_type": tradfi_type,
        "is_candles": is_candles,
        "months": months,
        "mtime": _latest_mtime(ex, ds, cn),
    }


# --------------------------------------------------------------------------- /mtime

@router.get("/mtime")
def get_heatmap_mtime(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    session: SessionToken = Depends(require_auth),
) -> dict[str, float]:
    """Return newest data file mtime (Unix seconds) for WebSocket change detection."""
    return {"mtime": _latest_mtime(exchange, dataset, coin)}


# --------------------------------------------------------------------------- /overview

@router.get("/overview")
def get_heatmap_overview(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Build the day-overview stacked-bar chart (one row per year).
    Returns Plotly figure JSON and an HTML legend string.
    """
    from market_data_tradfi import (
        is_hyperliquid_stock_perp_1m,
        tradfi_canonical_type_for_coin,
        is_tradfi_market_holiday,
        tradfi_expected_indices_for_type,
        tradfi_expected_minute_indices_from_session,
    )
    from market_data_sources import get_daily_source_counts_for_range
    from market_data import get_daily_hour_coverage_for_dataset

    ex = str(exchange).lower().strip()
    sx = _storage_ex(ex)
    ds = str(dataset).strip()
    ds_l = ds.lower()
    cn = str(coin).strip()

    is_sp = is_hyperliquid_stock_perp_1m(exchange=ex, dataset=ds, coin=cn)
    tradfi_type = tradfi_canonical_type_for_coin(cn) if is_sp else ""
    is_candles = ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api")

    if not is_candles:
        # l2Book / pb7 / other — use daily hour coverage heatmap (year × doy)
        return _build_coverage_heatmap(ex, ds, cn)

    # Candles: stacked-bar overview
    lag = _get_missing_lag_minutes(ex)
    day_counts: dict = {}
    if ex in ("hyperliquid", "binance", "bybit", "binanceusdm"):
        try:
            day_counts = get_daily_source_counts_for_range(
                exchange=sx,
                coin=cn,
                start_day=None,
                end_day=None,
                lag_minutes=lag,
                cutoff_ts_ms=None,
            ) or {}
        except Exception:
            day_counts = {}

    if not (isinstance(day_counts, dict) and day_counts):
        return {"figure": None, "legend_html": "", "error": "No data"}

    try:
        oldest_s = min(day_counts.keys())
        newest_s = max(day_counts.keys())
        dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
        dt1 = _date.today()
        if dt1 < _datetime.strptime(newest_s, "%Y%m%d").date():
            dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
    except Exception:
        return {"figure": None, "legend_html": "", "error": "Date parse error"}

    years: list[int] = []
    cur_iter = dt0
    while cur_iter <= dt1:
        y = int(cur_iter.strftime("%Y"))
        if y not in years:
            years.append(y)
        cur_iter = cur_iter + _timedelta(days=1)
    years = sorted(years)
    n_years = len(years)

    fig = make_subplots(rows=n_years, cols=1, shared_xaxes=True, vertical_spacing=0.02)
    _shown: set[str] = set()

    for row_i, y in enumerate(years, start=1):
        max_days = 366 if calendar.isleap(int(y)) else 365
        doy_vals = list(range(1, max_days + 1))
        l2b_row    = [0] * max_days
        api_row    = [0] * max_days
        oth_row    = [0] * max_days
        miss_row   = [0] * max_days
        outsess_row = [0] * max_days
        holiday_row = [0] * max_days
        hover_row  = [""] * max_days

        cur_day = dt0
        while cur_day <= dt1:
            day_s = cur_day.strftime("%Y%m%d")
            if not day_s.startswith(str(y)):
                cur_day = cur_day + _timedelta(days=1)
                continue
            try:
                doy_idx = cur_day.timetuple().tm_yday - 1
            except Exception:
                cur_day = cur_day + _timedelta(days=1)
                continue

            if 0 <= doy_idx < max_days:
                counts = day_counts.get(day_s) or {}
                api_v = int(counts.get("api") or 0)
                l2b_v = int(counts.get("l2Book_mid") or 0)
                oth_v = int(counts.get("other_exchange") or 0)

                if is_sp:
                    is_hday = is_tradfi_market_holiday(cur_day, tradfi_type)
                    exp = len(tradfi_expected_indices_for_type(cur_day, tradfi_type))
                    if is_hday:
                        exp = 0
                    if exp == 0 and api_v == 0 and l2b_v == 0 and oth_v == 0:
                        if is_hday:
                            holiday_row[doy_idx] = 1440
                            hover_row[doy_idx] = f"{day_s} | market holiday"
                        else:
                            outsess_row[doy_idx] = 1440
                            hover_row[doy_idx] = f"{day_s} | non-trading session"
                        cur_day = cur_day + _timedelta(days=1)
                        continue
                    miss_v = max(0, exp - api_v - l2b_v - oth_v)
                    outsess_v = max(0, 1440 - api_v - l2b_v - oth_v - miss_v)
                elif not counts:
                    miss_v = 1440
                    outsess_v = 0
                else:
                    miss_v = max(0, 1440 - api_v - l2b_v - oth_v)
                    outsess_v = 0

                l2b_row[doy_idx]    = l2b_v
                api_row[doy_idx]    = api_v
                oth_row[doy_idx]    = oth_v
                miss_row[doy_idx]   = miss_v
                outsess_row[doy_idx] = outsess_v
                hover_row[doy_idx] = (
                    f"{day_s} | api={api_v} l2Book={l2b_v} other={oth_v} missing={miss_v}"
                )
            cur_day = cur_day + _timedelta(days=1)

        base_segs = (
            [("l2Book_mid", l2b_row)] if ex == "hyperliquid" else []
        ) + [("api", api_row), ("other_exchange", oth_row), ("missing", miss_row)]
        tradfi_segs = (
            [("out-of-session", outsess_row), ("market holiday", holiday_row)]
            if is_sp else []
        )
        for seg_name, seg_vals in base_segs + tradfi_segs:
            show_leg = seg_name not in _shown
            if show_leg:
                _shown.add(seg_name)
            fig.add_trace(
                go.Bar(
                    name=seg_name,
                    x=doy_vals,
                    y=seg_vals,
                    marker_color=_BAR_COLORS[seg_name],
                    marker_line_width=0,
                    showlegend=False,
                    customdata=hover_row,
                    hovertemplate="%{customdata}<extra></extra>",
                ),
                row=row_i, col=1,
            )
        fig.update_yaxes(
            range=[0, 1440], showgrid=False, tickvals=[], ticktext=[],
            title_text=str(y), title_standoff=4,
            showline=False, zeroline=False, mirror=False,
            row=row_i, col=1,
        )
        fig.update_xaxes(
            showline=False, zeroline=False, mirror=False, row=row_i, col=1,
        )

    fig.update_layout(
        barmode="stack",
        height=80 + n_years * 90,
        margin=dict(l=10, r=10, t=10, b=20),
        xaxis=dict(range=[0.5, 366.5], showgrid=False, tickangle=-45, showline=False, zeroline=False),
        showlegend=False,
        bargap=0,
        bargroupgap=0,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )

    # Legend HTML
    parts: list[str] = []
    if is_sp:
        parts.append(_LEGEND_SPAN("market holiday", _BAR_COLORS["market holiday"]))
        parts.append(_LEGEND_SPAN("out-of-session", _BAR_COLORS["out-of-session"]))
    parts.append(_LEGEND_SPAN("missing", _BAR_COLORS["missing"]))
    parts.append(_LEGEND_SPAN("api", _BAR_COLORS["api"]))
    parts.append(_LEGEND_SPAN("other_exchange", _BAR_COLORS["other_exchange"]))
    if ex == "hyperliquid":
        parts.append(_LEGEND_SPAN("l2Book_mid", _BAR_COLORS["l2Book_mid"]))

    return {
        "figure": fig.to_json(),
        "legend_html": "".join(parts),
        "error": None,
    }


def _build_coverage_heatmap(exchange: str, dataset: str, coin: str) -> dict[str, Any]:
    """Year×DayOfYear heatmap for l2Book / pb7-cache / other non-candle datasets."""
    from market_data import get_daily_hour_coverage_for_dataset, get_daily_presence_for_pb7_cache
    ds = str(dataset).strip()
    ds_l = ds.lower()
    ex = str(exchange).lower().strip()

    if ds_l.startswith("pb7_cache:"):
        tf = str(ds.split(":", 1)[1] if ":" in ds else "1m").strip() or "1m"
        cov = get_daily_presence_for_pb7_cache(ex, tf, coin)
    else:
        cov = get_daily_hour_coverage_for_dataset(ex, ds, coin)

    days = cov.get("days") if isinstance(cov, dict) else []
    if not (isinstance(days, list) and days):
        return {"figure": None, "legend_html": "", "error": "No data"}

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

    z, text = [], []
    for y in years:
        row = [None] * max_days
        row_text = [""] * max_days
        for d in days:
            day_s = str(d.get("day") or "")
            if not day_s.startswith(str(y)):
                continue
            try:
                dt = _datetime.strptime(day_s, "%Y%m%d").date()
                doy = dt.timetuple().tm_yday - 1
            except Exception:
                continue
            status = int(d.get("status") or 0)
            hrs = int(d.get("hours") or 0)
            if 0 <= doy < max_days:
                row[doy] = status
                row_text[doy] = f"{day_s} | hours={hrs}/24"
        z.append(row)
        text.append(row_text)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=list(range(1, max_days + 1)),
            y=[str(y) for y in years],
            text=text,
            hovertemplate="%{text}<extra></extra>",
            colorscale=[[0.0, "#b23b3b"], [0.5, "#c9a227"], [1.0, "#2e7d32"]],
            zmin=0, zmax=2,
            showscale=False,
            xgap=1, ygap=1,
        )
    )
    fig.update_layout(
        height=120 + (len(years) * 26),
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis=dict(tickangle=-45, automargin=True, showgrid=False),
        yaxis=dict(autorange="reversed", showgrid=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return {"figure": fig.to_json(), "legend_html": "", "error": None}


# --------------------------------------------------------------------------- /minutes

@router.get("/minutes")
def get_heatmap_minutes(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    month: str = Query(..., description="YYYY-MM"),
    show_holiday: bool = Query(True),
    show_oos: bool = Query(True),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Build the per-minute heatmap for a specific month.
    Returns Plotly figure JSON and an HTML legend string.
    """
    from market_data_tradfi import (
        is_hyperliquid_stock_perp_1m,
        tradfi_canonical_type_for_coin,
        is_tradfi_market_holiday,
        tradfi_expected_indices_for_type,
        tradfi_expected_minute_indices,
        tradfi_expected_minute_indices_from_session,
    )
    from market_data import get_minute_presence_for_dataset

    ex = str(exchange).lower().strip()
    sx = _storage_ex(ex)
    ds = str(dataset).strip()
    ds_l = ds.lower()
    cn = str(coin).strip()

    is_sp = is_hyperliquid_stock_perp_1m(exchange=ex, dataset=ds, coin=cn)
    tradfi_type = tradfi_canonical_type_for_coin(cn) if is_sp else ""

    # parse month → start/end day
    try:
        sm_year = int(month[:4])
        sm_mon = int(month[5:7])
        last_day = calendar.monthrange(sm_year, sm_mon)[1]
        start_day = f"{sm_year:04d}{sm_mon:02d}01"
        end_day = f"{sm_year:04d}{sm_mon:02d}{last_day:02d}"
    except Exception:
        return {"figure": None, "legend_html": "", "error": "Invalid month"}

    try:
        hp = get_minute_presence_for_dataset(sx, ds, cn, start_day=start_day, end_day=end_day)
    except Exception as e:
        return {"figure": None, "legend_html": "", "error": str(e)}

    present = hp.get("days") if isinstance(hp, dict) else {}
    if not isinstance(present, dict) or not present:
        return {"figure": None, "legend_html": "", "error": "No minute data for this month"}

    oldest_s = str(hp.get("oldest_day") or "")
    newest_s = str(hp.get("newest_day") or "")
    try:
        dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
        dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
    except Exception:
        return {"figure": None, "legend_html": "", "error": "No date range"}

    src_code = {
        None: 0, "missing": 0, "api": 2, "best": 3,
        "other_exchange": 4, "binance_perp_usdt": 4, "l2Book_mid": 5,
    }

    if is_sp:
        colorscale = [
            [0.0, "#7e57c2"], [1/7, "#4e4e4e"], [2/7, "#b23b3b"],
            [3/7, "#6a1b9a"], [4/7, "#2e7d32"], [5/7, "#00897b"],
            [6/7, "#ef6c00"], [1.0, "#1e88e5"],
        ]
    else:
        colorscale = [
            [0.0, "#b23b3b"], [0.2, "#6a1b9a"], [0.4, "#7e57c2"],
            [0.6, "#00897b"], [0.8, "#ef6c00"], [1.0, "#1e88e5"],
        ]

    z, text, y_labels = [], [], []

    days_list: list[_date] = []
    cur = dt0
    while cur <= dt1:
        days_list.append(cur)
        cur = cur + _timedelta(days=1)

    for d in days_list:
        day_s = d.strftime("%Y%m%d")
        hours_map = present.get(day_s) if isinstance(present.get(day_s), dict) else {}

        is_hday = False
        holiday_session_indices: set[int] = set()
        expected_indices: set[int] | None = None

        if is_sp:
            from market_data_tradfi import tradfi_expected_minute_indices as _tmi
            holiday_session_indices = _tmi(d) if str(tradfi_type or "").strip().lower() != "fx" else set(range(1440))
            is_hday = is_tradfi_market_holiday(d, tradfi_type)
            expected_indices = tradfi_expected_indices_for_type(d, tradfi_type)
            if is_hday:
                expected_indices = set()

        for block_start in (0, 12):
            row, row_text = [], []
            for h in range(block_start, block_start + 12):
                hh = f"{h:02d}"
                mins_map = hours_map.get(hh) or {}
                for minute in range(60):
                    minute_idx = (h * 60) + minute
                    src = mins_map.get(minute)
                    code = int(src_code.get(str(src), src_code.get(src, 0)))
                    if code == 0:
                        if (
                            show_holiday
                            and is_hday
                            and minute_idx in holiday_session_indices
                        ):
                            row.append(-2)
                            row_text.append(f"{day_s} {h:02d}:{minute:02d} (market holiday)")
                            continue
                        if (
                            show_oos
                            and is_sp
                            and expected_indices is not None
                            and minute_idx not in expected_indices
                        ):
                            row.append(-1)
                            row_text.append(f"{day_s} {h:02d}:{minute:02d} (expected out-of-session gap)")
                            continue
                    row.append(code)
                    src_label = str(src) if src is not None else "missing"
                    row_text.append(f"{day_s} {h:02d}:{minute:02d} ({src_label})")
            z.append(row)
            text.append(row_text)
            y_labels.append(f"{day_s} {block_start:02d}-{block_start+11:02d}")

    if not z:
        return {"figure": None, "legend_html": "", "error": "No minute data"}

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=list(range(720)),
            y=[str(y) for y in y_labels],
            text=text,
            hovertemplate="%{text}<extra></extra>",
            colorscale=colorscale,
            zmin=-2 if is_sp else 0,
            zmax=5,
            showscale=False,
            xgap=0, ygap=0,
        )
    )
    fig.update_layout(
        height=max(300, 80 + (len(y_labels) * 18)),
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(0, 720, 60)),
            ticktext=[f"{x:02d}h" for x in range(0, 12)],
        ),
        yaxis=dict(autorange="reversed", showgrid=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )

    _has_l2 = ex == "hyperliquid"
    _l2_span = _LEGEND_SPAN("l2Book_mid", "#1e88e5") if _has_l2 else ""
    if is_sp:
        legend_html = (
            (_LEGEND_SPAN("market holiday", "#7e57c2") if show_holiday else "")
            + (_LEGEND_SPAN("expected out-of-session gap", "#4e4e4e") if show_oos else "")
            + _LEGEND_SPAN("missing", "#b23b3b")
            + _LEGEND_SPAN("api", "#7e57c2")
            + _LEGEND_SPAN("other_exchange", "#ef6c00")
            + _l2_span
        )
    else:
        legend_html = (
            _LEGEND_SPAN("missing", "#b23b3b")
            + _LEGEND_SPAN("api", "#7e57c2")
            + _LEGEND_SPAN("other_exchange", "#ef6c00")
            + _l2_span
        )

    return {
        "figure": fig.to_json(),
        "legend_html": legend_html,
        "error": None,
    }
