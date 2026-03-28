"""
FastAPI router: Gap / Coverage Heatmap charts.

Endpoints return Plotly figure JSON so the frontend (Vanilla JS + Plotly.js)
can render them without any Streamlit involvement.

All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import asyncio
import calendar
import json as _json
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
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
        base = root / _storage_ex(exchange) / dataset / str(coin)
        if base.exists():
            return base
        # l2book / l2Book casing fallback
        if ds_l == "l2book":
            for alt in ("l2Book", "l2book"):
                alt_base = root / _storage_ex(exchange) / alt / str(coin)
                if alt_base.exists():
                    return alt_base
        return base
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
    from market_data import get_daily_hour_coverage_for_dataset

    ex = str(exchange).lower().strip()
    ds = str(dataset).strip()
    ds_l = ds.lower()
    cn = str(coin).strip()

    is_sp = is_hyperliquid_stock_perp_1m(exchange=ex, dataset=ds, coin=cn)
    tradfi_type = tradfi_canonical_type_for_coin(cn) if is_sp else ""
    is_candles = ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api")

    months: list[str] = []
    if is_candles:
        if ds_l in ("1m_api", "candles_1m_api"):
            # 1m_api has no source index — derive months from coverage data
            try:
                cov = get_daily_hour_coverage_for_dataset(
                    _storage_ex(ex), ds, cn,
                )
                cov_days = cov.get("days") if isinstance(cov, dict) else []
                if isinstance(cov_days, list) and cov_days:
                    day_strs = [str(d.get("day") or "") for d in cov_days if d.get("day")]
                    if day_strs:
                        oldest_s = min(day_strs)
                        newest_s = max(day_strs)
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
        else:
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
    else:
        # Non-candle datasets (l2book, pb7_cache, etc.) — derive months from coverage
        # Extend to today so months without any data appear in the dropdown
        try:
            cov = get_daily_hour_coverage_for_dataset(
                _storage_ex(ex), ds, cn,
            )
            cov_days = cov.get("days") if isinstance(cov, dict) else []
            if isinstance(cov_days, list) and cov_days:
                day_strs = [str(d.get("day") or "") for d in cov_days if d.get("day")]
                if day_strs:
                    oldest_s = min(day_strs)
                    dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
                    dt1 = _date.today()  # extend to today
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

    if ds_l in ("1m_api", "candles_1m_api"):
        # 1m_api has no source index — show simple coverage heatmap from NPZ files
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
        margin=dict(l=45, r=10, t=10, b=20),
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


def _build_coverage_heatmap(
    exchange: str, dataset: str, coin: str,
    progress_cb: Any | None = None,
) -> dict[str, Any]:
    """Year×DayOfYear heatmap for l2Book / pb7-cache / other non-candle datasets."""
    from market_data import get_daily_hour_coverage_for_dataset, get_daily_presence_for_pb7_cache
    ds = str(dataset).strip()
    ds_l = ds.lower()
    ex = str(exchange).lower().strip()

    if ds_l.startswith("pb7_cache:"):
        tf = str(ds.split(":", 1)[1] if ":" in ds else "1m").strip() or "1m"
        cov = get_daily_presence_for_pb7_cache(ex, tf, coin)
    else:
        cov = get_daily_hour_coverage_for_dataset(ex, ds, coin, progress_cb=progress_cb)

    days = cov.get("days") if isinstance(cov, dict) else []
    if not (isinstance(days, list) and days):
        return {"figure": None, "legend_html": "", "error": "No data"}

    # --- extend coverage to today so months without data show as missing ---
    today = _date.today()
    newest_in_data = ""
    for d in days:
        ds_ = str(d.get("day") or "")
        if ds_ > newest_in_data:
            newest_in_data = ds_
    if newest_in_data:
        try:
            newest_dt = _datetime.strptime(newest_in_data, "%Y%m%d").date()
            if newest_dt < today:
                fill_cur = newest_dt + _timedelta(days=1)
                while fill_cur <= today:
                    days.append({"day": fill_cur.strftime("%Y%m%d"), "hours": 0, "status": 0})
                    fill_cur = fill_cur + _timedelta(days=1)
        except Exception:
            pass

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
        margin=dict(l=45, r=10, t=20, b=20),
        xaxis=dict(tickangle=-45, automargin=True, showgrid=False),
        yaxis=dict(autorange="reversed", showgrid=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    if progress_cb:
        progress_cb("Building chart", 0, 0)

    # Legend HTML for coverage heatmap
    legend_parts: list[str] = [
        _LEGEND_SPAN("missing", "#b23b3b"),
        _LEGEND_SPAN("partial", "#c9a227"),
    ]
    # Use dataset-specific label for the "full" color
    full_label = "l2Book" if ds_l in ("l2book",) else "complete"
    legend_parts.append(_LEGEND_SPAN(full_label, "#2e7d32"))

    return {
        "figure": fig.to_json(),
        "legend_html": "".join(legend_parts),
        "error": None,
        "newest_data_day": newest_in_data or None,
    }


# --------------------------------------------------------------------------- /overview-stream (SSE)

@router.get("/overview-stream")
async def get_heatmap_overview_stream(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    session: SessionToken = Depends(require_auth),
):
    """
    SSE (Server-Sent Events) version of /overview that streams progress
    updates while scanning directories (useful for slow NAS mounts).

    Events:
      - ``event: progress`` with ``{"msg": "...", "current": N, "total": M}``
      - ``event: result``   with the same payload as GET /overview
    """
    ex = str(exchange).lower().strip()
    sx = _storage_ex(ex)
    ds = str(dataset).strip()
    ds_l = ds.lower()
    cn = str(coin).strip()

    is_candles = ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api")

    queue: asyncio.Queue = asyncio.Queue()

    def _make_progress_cb(loop: asyncio.AbstractEventLoop):
        """Return a thread-safe progress callback that posts into the asyncio queue."""
        def progress_cb(msg: str, current: int, total: int) -> None:
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"type": "progress", "msg": msg, "current": current, "total": total},
            )
        return progress_cb

    def _run_sync(loop: asyncio.AbstractEventLoop) -> dict[str, Any]:
        cb = _make_progress_cb(loop)
        if not is_candles:
            return _build_coverage_heatmap(ex, ds, cn, progress_cb=cb)
        if ds_l in ("1m_api", "candles_1m_api"):
            return _build_coverage_heatmap(ex, ds, cn, progress_cb=cb)
        # Regular candle datasets: source-index based, no progress needed
        return get_heatmap_overview(
            exchange=exchange, dataset=dataset, coin=coin, session=session,
        )

    async def generate():
        loop = asyncio.get_event_loop()

        async def _worker():
            result = await loop.run_in_executor(None, _run_sync, loop)
            await queue.put({"type": "result", "payload": result})

        task = asyncio.create_task(_worker())
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if msg["type"] == "result":
                    yield f"event: result\ndata: {_json.dumps(msg['payload'])}\n\n"
                    break
                else:
                    yield f"event: progress\ndata: {_json.dumps(msg)}\n\n"
            await task
        except Exception:
            await task

    return StreamingResponse(generate(), media_type="text/event-stream")


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

    # l2Book datasets use SSE streaming endpoint for minute detail
    if ds_l in ("l2book", "l2book_mid"):
        return {
            "figure": None,
            "legend_html": "",
            "error": "l2Book minute detail uses streaming — please use /minutes-stream endpoint",
        }

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
        margin=dict(l=40, r=10, t=20, b=20),
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


# --------------------------------------------------------------------------- /minutes-stream (SSE)

def _parse_l2book_minutes(
    exchange: str,
    dataset: str,
    coin: str,
    start_day: str,
    end_day: str,
    progress_cb: Any | None = None,
    cancel_event: Any | None = None,
) -> dict[str, Any]:
    """Parse l2Book .lz4 files and extract per-minute presence for a date range.

    Returns dict compatible with get_minute_presence_for_dataset output:
      {oldest_day, newest_day, days: {YYYYMMDD: {HH: {mm: "l2Book"}}}}
    """
    import lz4.frame
    import re as _re
    from market_data import _resolve_dataset_coin_dirs

    # Pre-compile regex for fast timestamp extraction from l2Book JSON lines
    # Matches "time":1704067204224 (epoch ms) inside "data":{...}
    _TS_RE = _re.compile(r'"data":\{[^}]*"time"\s*:\s*(\d{13})')

    sx = _storage_ex(exchange)
    scan_dirs = _resolve_dataset_coin_dirs(sx, dataset, coin)
    if not scan_dirs:
        return {}

    # Collect all .lz4 files in the date range
    lz4_files: list[tuple[str, Path]] = []  # (YYYYMMDD-HH, path)
    for d in scan_dirs:
        for fp in sorted(d.glob("*.lz4")):
            fname = fp.stem  # e.g. "20240101-00"
            if len(fname) >= 8:
                day_part = fname[:8]
                if start_day <= day_part <= end_day:
                    lz4_files.append((fname, fp))

    if not lz4_files:
        return {}

    # De-duplicate by filename (prefer first occurrence = local over NAS)
    seen: set[str] = set()
    unique_files: list[tuple[str, Path]] = []
    for fname, fp in lz4_files:
        if fname not in seen:
            seen.add(fname)
            unique_files.append((fname, fp))

    total = len(unique_files)
    days: dict[str, dict[str, dict[int, str]]] = {}
    all_days: list[str] = []

    for idx, (fname, fp) in enumerate(unique_files):
        # Check cancellation
        if cancel_event is not None and cancel_event.is_set():
            break

        if progress_cb:
            progress_cb(f"Parsing l2Book files", idx, total)

        day_s = fname[:8]      # YYYYMMDD
        hour_s = fname[9:11]   # HH

        try:
            with open(fp, "rb") as f:
                raw = lz4.frame.decompress(f.read())
            text = raw.decode("utf-8", errors="replace")
            minutes_found: set[int] = set()
            # Use regex for ~10x faster extraction than full JSON parse
            for m in _TS_RE.finditer(text):
                ts_ms = int(m.group(1))
                minutes_found.add((ts_ms // 60000) % 60)

            if day_s not in days:
                days[day_s] = {}
                all_days.append(day_s)
            if hour_s not in days[day_s]:
                days[day_s][hour_s] = {}
            for m in minutes_found:
                days[day_s][hour_s][m] = "l2Book"

        except Exception:
            # If we can't parse a file, mark all 60 minutes based on filename
            if day_s not in days:
                days[day_s] = {}
                all_days.append(day_s)
            if hour_s not in days[day_s]:
                days[day_s][hour_s] = {}
            for m in range(60):
                days[day_s][hour_s][m] = "l2Book"

    if progress_cb:
        progress_cb("Building chart", total, total)

    if not all_days:
        return {}

    all_days_sorted = sorted(set(all_days))
    return {
        "oldest_day": all_days_sorted[0],
        "newest_day": all_days_sorted[-1],
        "days": days,
    }


def _build_minutes_chart(
    exchange: str,
    dataset: str,
    coin: str,
    month: str,
    show_holiday: bool,
    show_oos: bool,
    hp: dict[str, Any],
) -> dict[str, Any]:
    """Build the per-minute heatmap Plotly figure from parsed minute data.

    Shared by both /minutes and /minutes-stream.
    """
    from market_data_tradfi import (
        is_hyperliquid_stock_perp_1m,
        tradfi_canonical_type_for_coin,
        is_tradfi_market_holiday,
        tradfi_expected_indices_for_type,
        tradfi_expected_minute_indices,
    )

    ex = str(exchange).lower().strip()
    ds = str(dataset).strip()
    cn = str(coin).strip()

    is_sp = is_hyperliquid_stock_perp_1m(exchange=ex, dataset=ds, coin=cn)
    tradfi_type = tradfi_canonical_type_for_coin(cn) if is_sp else ""

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
        "l2Book": 5,
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
            holiday_session_indices = tradfi_expected_minute_indices(d) if str(tradfi_type or "").strip().lower() != "fx" else set(range(1440))
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
        margin=dict(l=40, r=10, t=20, b=20),
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
    _l2_span = _LEGEND_SPAN("l2Book", "#1e88e5") if _has_l2 else ""
    legend_html = (
        _LEGEND_SPAN("missing", "#b23b3b")
        + _l2_span
    )

    return {
        "figure": fig.to_json(),
        "legend_html": legend_html,
        "error": None,
    }


@router.get("/minutes-stream")
async def get_heatmap_minutes_stream(
    exchange: str = Query(...),
    dataset: str = Query(...),
    coin: str = Query(...),
    month: str = Query(..., description="YYYY-MM"),
    show_holiday: bool = Query(True),
    show_oos: bool = Query(True),
    session: SessionToken = Depends(require_auth),
):
    """
    SSE (Server-Sent Events) version of /minutes.

    For l2Book datasets this decompresses and parses .lz4 files with progress
    updates. For other datasets it delegates to the sync path immediately.

    Events:
      - ``event: progress`` — ``{"msg": "...", "current": N, "total": M}``
      - ``event: result``   — same payload as GET /minutes
    """
    import threading

    ex = str(exchange).lower().strip()
    sx = _storage_ex(ex)
    ds = str(dataset).strip()
    ds_l = ds.lower()
    cn = str(coin).strip()

    # parse month
    try:
        sm_year = int(month[:4])
        sm_mon = int(month[5:7])
        last_day = calendar.monthrange(sm_year, sm_mon)[1]
        start_day = f"{sm_year:04d}{sm_mon:02d}01"
        end_day = f"{sm_year:04d}{sm_mon:02d}{last_day:02d}"
    except Exception:
        async def _err():
            yield f"event: result\ndata: {_json.dumps({'figure': None, 'legend_html': '', 'error': 'Invalid month'})}\n\n"
        return StreamingResponse(_err(), media_type="text/event-stream")

    is_l2book = ds_l in ("l2book", "l2book_mid")

    if not is_l2book:
        # Non-l2Book: run sync and return immediately (no streaming needed)
        from market_data import get_minute_presence_for_dataset
        try:
            hp = get_minute_presence_for_dataset(sx, ds, cn, start_day=start_day, end_day=end_day)
        except Exception as e:
            hp = {}

        result = _build_minutes_chart(ex, ds, cn, month, show_holiday, show_oos, hp)

        async def _immediate():
            yield f"event: result\ndata: {_json.dumps(result)}\n\n"
        return StreamingResponse(_immediate(), media_type="text/event-stream")

    # l2Book: stream with progress
    queue: asyncio.Queue = asyncio.Queue()
    cancel_event = threading.Event()

    def _make_progress_cb(loop: asyncio.AbstractEventLoop):
        def progress_cb(msg: str, current: int, total: int) -> None:
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"type": "progress", "msg": msg, "current": current, "total": total},
            )
        return progress_cb

    def _run_sync(loop: asyncio.AbstractEventLoop) -> dict[str, Any]:
        cb = _make_progress_cb(loop)
        hp = _parse_l2book_minutes(
            ex, ds, cn, start_day, end_day,
            progress_cb=cb, cancel_event=cancel_event,
        )
        if cancel_event.is_set():
            return {"figure": None, "legend_html": "", "error": "Cancelled"}
        return _build_minutes_chart(ex, ds, cn, month, show_holiday, show_oos, hp)

    async def generate():
        loop = asyncio.get_event_loop()

        async def _worker():
            result = await loop.run_in_executor(None, _run_sync, loop)
            await queue.put({"type": "result", "payload": result})

        task = asyncio.create_task(_worker())
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if msg["type"] == "result":
                    yield f"event: result\ndata: {_json.dumps(msg['payload'])}\n\n"
                    break
                else:
                    yield f"event: progress\ndata: {_json.dumps(msg)}\n\n"
            await task
        except (asyncio.CancelledError, GeneratorExit):
            cancel_event.set()
            await task
        except Exception:
            cancel_event.set()
            await task

    return StreamingResponse(generate(), media_type="text/event-stream")


# --------------------------------------------------------------------------- /queue-l2book-download

@router.post("/queue-l2book-download")
def queue_l2book_download(
    exchange: str = Query(...),
    coin: str = Query(...),
    month: str = Query("", description="YYYY-MM (optional if start_day/end_day given)"),
    start_day: str = Query("", description="YYYYMMDD — explicit start (overrides month)"),
    end_day: str = Query("", description="YYYYMMDD — explicit end (overrides month)"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Queue a background job to download l2Book data for a specific coin
    from AWS S3.  Provide either *month* (YYYY-MM) **or** explicit
    *start_day*/*end_day* (YYYYMMDD).

    Returns ``{"job_id": "...", "start_day": "...", "end_day": "..."}``
    or ``{"error": "..."}`` on failure.
    """
    import subprocess
    import sys
    from task_queue import enqueue_job, read_worker_pid, is_pid_running
    from market_data import load_aws_profile_region
    import re as _re

    cn = str(coin).strip()
    ex = str(exchange).lower().strip()

    if ex != "hyperliquid":
        return {"error": "l2Book download is only available for Hyperliquid"}

    sd = str(start_day).strip()
    ed = str(end_day).strip()
    mo = str(month).strip()

    if sd and ed:
        # Explicit date range
        if not (_re.fullmatch(r"\d{8}", sd) and _re.fullmatch(r"\d{8}", ed)):
            return {"error": "Invalid date format (expected YYYYMMDD)"}
        if ed < sd:
            return {"error": "end_day must be >= start_day"}
        start_day_val = sd
        end_day_val = ed
    elif mo:
        # Parse month
        try:
            sm_year = int(mo[:4])
            sm_mon = int(mo[5:7])
            last_day = calendar.monthrange(sm_year, sm_mon)[1]
            start_day_val = f"{sm_year:04d}{sm_mon:02d}01"
            end_day_val = f"{sm_year:04d}{sm_mon:02d}{last_day:02d}"
        except Exception:
            return {"error": "Invalid month format (expected YYYY-MM)"}
    else:
        return {"error": "Provide either month (YYYY-MM) or start_day + end_day (YYYYMMDD)"}

    # AWS profile settings
    profile = "pbgui-hyperliquid"
    region = load_aws_profile_region(profile) or "us-east-2"

    try:
        job = enqueue_job(
            job_type="hl_aws_l2book_auto",
            exchange="hyperliquid",
            payload={
                "profile": profile,
                "region": region,
                "coins": [cn],
                "chunk_days": 7,
                "start_day": start_day_val,
                "end_day": end_day_val,
                "only_missing_1m_src_hours": False,
            },
        )
    except Exception as e:
        return {"error": f"Failed to enqueue job: {e}"}

    # Start worker if not running
    try:
        pid = read_worker_pid()
        if not (pid and is_pid_running(int(pid))):
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
    except Exception:
        pass

    return {
        "job_id": job.job_id,
        "start_day": start_day_val,
        "end_day": end_day_val,
        "coin": cn,
    }


# --------------------------------------------------------------------------- /l2book-download-info

@router.get("/l2book-download-info")
def l2book_download_info(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Return info needed by the l2Book Download form:
      - coins: enabled Hyperliquid coins
      - has_aws_creds: bool
      - archive_range: {oldest_day, newest_day} (best-effort, may be empty)
    """
    from market_data import load_market_data_config, load_aws_profile_credentials, load_aws_profile_region

    profile = "pbgui-hyperliquid"
    cfg = load_market_data_config()
    coins = sorted(cfg.enabled_coins.get("hyperliquid", []))

    creds = {}
    try:
        creds = load_aws_profile_credentials(profile)
    except Exception:
        pass

    has_creds = bool(
        str(creds.get("aws_access_key_id") or "").strip()
        and str(creds.get("aws_secret_access_key") or "").strip()
    )

    region = ""
    try:
        region = load_aws_profile_region(profile) or "us-east-2"
    except Exception:
        region = "us-east-2"

    # Try to detect archive range (fast S3 list)
    oldest_day = ""
    newest_day = ""
    if has_creds:
        try:
            from hyperliquid_aws import get_hyperliquid_archive_day_range_aws
            rng = get_hyperliquid_archive_day_range_aws(
                aws_access_key_id=str(creds.get("aws_access_key_id") or "").strip(),
                aws_secret_access_key=str(creds.get("aws_secret_access_key") or "").strip(),
                region_name=region,
            )
            if isinstance(rng, (tuple, list)) and len(rng) >= 2:
                oldest_day = str(rng[0] or "")
                newest_day = str(rng[1] or "")
            elif isinstance(rng, dict):
                oldest_day = str(rng.get("oldest_day") or "")
                newest_day = str(rng.get("newest_day") or "")
        except Exception:
            pass

    return {
        "coins": coins,
        "has_aws_creds": has_creds,
        "archive_range": {"oldest_day": oldest_day, "newest_day": newest_day},
        "region": region,
    }


# --------------------------------------------------------------------------- /queue-l2book-download-bulk

@router.post("/queue-l2book-download-bulk")
def queue_l2book_download_bulk(
    request: dict,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Queue a bulk l2Book download job for multiple coins.

    Request body:
        {
            "coins": ["BTC", "ETH"] or ["All"],
            "start_day": "20230415",
            "end_day": "20251201",
            "only_missing_1m_src_hours": true
        }
    """
    import subprocess
    import sys
    import re as _re
    from task_queue import enqueue_job, read_worker_pid, is_pid_running
    from market_data import (
        load_market_data_config,
        load_aws_profile_credentials,
        load_aws_profile_region,
        save_aws_profile_credentials,
        save_aws_profile_region,
    )
    from hyperliquid_aws import (
        get_hyperliquid_archive_day_range_aws,
        list_hyperliquid_archive_hours_aws,
        check_hyperliquid_l2book_coin_exists_aws,
    )
    from market_data import append_exchange_download_log

    profile = "pbgui-hyperliquid"

    # --- resolve coins ---
    raw_coins = request.get("coins", [])
    if not isinstance(raw_coins, list):
        raw_coins = []
    raw_coins = [str(c).strip() for c in raw_coins if str(c).strip()]
    raw_coins_upper = [c.upper() for c in raw_coins]

    cfg = load_market_data_config()
    all_coins = sorted(cfg.enabled_coins.get("hyperliquid", []))

    if not all_coins:
        return {"error": "No enabled Hyperliquid coins configured"}

    if "ALL" in raw_coins_upper:
        payload_coins = list(all_coins)
    else:
        payload_coins = [c for c in raw_coins if c in all_coins]

    if not payload_coins:
        return {"error": "No matching enabled coins selected"}

    # --- validate dates ---
    sd = str(request.get("start_day") or "").strip()
    ed = str(request.get("end_day") or "").strip()
    if not sd or not ed:
        return {"error": "start_day and end_day are required (YYYYMMDD)"}
    if not (_re.fullmatch(r"\d{8}", sd) and _re.fullmatch(r"\d{8}", ed)):
        return {"error": "Invalid date format (expected YYYYMMDD)"}
    if ed < sd:
        return {"error": "end_day must be >= start_day"}

    only_missing = bool(request.get("only_missing_1m_src_hours", True))

    # --- load AWS creds ---
    creds = {}
    try:
        creds = load_aws_profile_credentials(profile)
    except Exception:
        pass

    ak = str(creds.get("aws_access_key_id") or "").strip()
    sk = str(creds.get("aws_secret_access_key") or "").strip()
    if not ak or not sk:
        return {"error": "Missing AWS credentials — configure in Settings (l2Book)"}

    region = load_aws_profile_region(profile) or "us-east-2"

    # --- preflight: probe archive for coin existence ---
    missing_coins: list[str] = []
    try:
        rng = get_hyperliquid_archive_day_range_aws(
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=region,
        )
        probe_day = ""
        if isinstance(rng, (tuple, list)) and len(rng) >= 2:
            probe_day = str(rng[1] or rng[0] or "").strip()
        elif isinstance(rng, dict):
            probe_day = str(rng.get("newest_day") or rng.get("oldest_day") or "").strip()
        if not probe_day:
            return {"error": "Failed to detect archive range for preflight"}

        probe_hours = list_hyperliquid_archive_hours_aws(
            day=probe_day,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=region,
        )
        if not probe_hours:
            return {"error": f"No archive hours found for probe day {probe_day}"}

        missing_coins = []
        for coin in list(payload_coins):
            ok = check_hyperliquid_l2book_coin_exists_aws(
                coin=coin,
                day=probe_day,
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                region_name=region,
                hours=probe_hours,
            )
            if not ok:
                missing_coins.append(coin)

        if missing_coins:
            payload_coins = [c for c in payload_coins if c not in missing_coins]

        if not payload_coins:
            return {"error": f"No selected coins exist in the archive (missing: {', '.join(missing_coins)})"}
    except Exception as e:
        if "Failed to detect" in str(e) or "No archive hours" in str(e):
            return {"error": str(e)}
        # non-critical preflight failure — proceed anyway
        pass

    # --- enqueue job ---
    try:
        job = enqueue_job(
            job_type="hl_aws_l2book_auto",
            exchange="hyperliquid",
            payload={
                "profile": profile,
                "region": region,
                "coins": list(payload_coins),
                "chunk_days": 7,
                "start_day": sd,
                "end_day": ed,
                "only_missing_1m_src_hours": only_missing,
            },
        )
    except Exception as e:
        return {"error": f"Failed to enqueue job: {e}"}

    append_exchange_download_log(
        "hyperliquid",
        f"[hl_aws_l2book_auto] queued job_id={job.job_id} coins={payload_coins} range={sd}-{ed}",
    )

    # Start worker if not running
    try:
        pid = read_worker_pid()
        if not (pid and is_pid_running(int(pid))):
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
    except Exception:
        pass

    return {
        "job_id": job.job_id,
        "start_day": sd,
        "end_day": ed,
        "coins": payload_coins,
        "coins_count": len(payload_coins),
        "missing_coins": missing_coins,
    }


# --------------------------------------------------------------------------- /build-ohlcv-info

@router.get("/build-ohlcv-info")
def build_ohlcv_info(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Return info needed by the Build best 1m OHLCV form:
      - eligible_coins: list[str] — coins eligible for building
      - all_coins: list[str] — all enabled Hyperliquid coins
    """
    from market_data import load_market_data_config
    import json as _j

    cfg = load_market_data_config()
    all_coins = sorted(cfg.enabled_coins.get("hyperliquid", []))

    # Load TradFi map for eligibility check
    tradfi_map_path = Path(__file__).resolve().parents[1] / "data" / "tradfi_symbol_map.json"
    tradfi_by_xyz: dict[str, dict] = {}
    try:
        if tradfi_map_path.exists():
            raw = _j.loads(tradfi_map_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                for r in raw:
                    xyz = str(r.get("xyz_coin") or "").strip().upper()
                    if xyz:
                        tradfi_by_xyz[xyz] = dict(r)
    except Exception:
        pass

    def _extract_xyz_coin_name(coin: str) -> str | None:
        c_u = str(coin or "").strip().upper()
        if not c_u:
            return None
        if c_u.startswith("XYZ:") or c_u.startswith("XYZ-"):
            tail = c_u[4:].strip()
        else:
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

    eligible: list[str] = []
    for coin in all_coins:
        xyz_name = _extract_xyz_coin_name(coin)
        if not xyz_name:
            continue
        entry = tradfi_by_xyz.get(xyz_name)
        if isinstance(entry, dict):
            status = str(entry.get("status") or "").strip().lower()
            has_tiingo = bool(
                str(entry.get("tiingo_ticker") or "").strip()
                or str(entry.get("tiingo_fx_ticker") or "").strip()
            )
            if status == "ok" and has_tiingo:
                eligible.append(coin)
        else:
            # Non-XYZ (crypto): always allow
            eligible.append(coin)

    return {
        "eligible_coins": eligible,
        "all_coins": all_coins,
    }


# --------------------------------------------------------------------------- /queue-build-ohlcv

@router.post("/queue-build-ohlcv")
def queue_build_ohlcv(
    request: dict,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Queue a Build best 1m OHLCV job.

    Request body:
        {
            "coins": ["BTC", "ETH"] or ["All"],
            "start_day": "20230415" or "",
            "end_day": "20251201" or "",
            "refetch": false
        }
    """
    import subprocess
    import sys
    import re as _re
    from task_queue import enqueue_job, read_worker_pid, is_pid_running
    from market_data import append_exchange_download_log

    # Resolve eligible coins (reuse same logic)
    info = build_ohlcv_info(session=session)
    eligible = info["eligible_coins"]

    if not eligible:
        return {"error": "No eligible coins found"}

    raw_coins = request.get("coins", [])
    if not isinstance(raw_coins, list):
        raw_coins = []
    raw_coins = [str(c).strip() for c in raw_coins if str(c).strip()]
    raw_upper = [c.upper() for c in raw_coins]

    if "ALL" in raw_upper or not raw_coins:
        build_coins = list(eligible)
    else:
        build_coins = [c for c in raw_coins if c in eligible]

    if not build_coins:
        return {"error": "No matching eligible coins selected"}

    sd = str(request.get("start_day") or "").strip()
    ed = str(request.get("end_day") or "").strip()

    if sd and not _re.fullmatch(r"\d{8}", sd):
        return {"error": "Invalid start_day format (expected YYYYMMDD)"}
    if ed and not _re.fullmatch(r"\d{8}", ed):
        return {"error": "Invalid end_day format (expected YYYYMMDD)"}
    if sd and ed and ed < sd:
        return {"error": "end_day must be >= start_day"}

    from datetime import date as _d
    effective_end = ed if ed else _d.today().strftime("%Y%m%d")
    refetch = bool(request.get("refetch", False))

    try:
        job = enqueue_job(
            job_type="hl_best_1m",
            exchange="hyperliquid",
            payload={
                "coins": list(build_coins),
                "end_day": effective_end,
                "start_day": sd,
                "refetch": refetch,
            },
        )
    except Exception as e:
        return {"error": f"Failed to enqueue job: {e}"}

    append_exchange_download_log(
        "hyperliquid",
        f"[hl_best_1m] queued job_id={job.job_id} coins={len(build_coins)} range={sd or '?'}-{effective_end}",
    )

    # Start worker if not running
    try:
        pid = read_worker_pid()
        if not (pid and is_pid_running(int(pid))):
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
    except Exception:
        pass

    return {
        "job_id": job.job_id,
        "start_day": sd,
        "end_day": effective_end,
        "coins_count": len(build_coins),
        "refetch": refetch,
    }