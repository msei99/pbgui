"""Calculation helpers for the FastAPI Strategy Explorer migration."""

from __future__ import annotations

import copy
import math
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from pbgui_purefunc import PBGDIR, pb7dir
from pb7_config import prepare_pb7_config_dict
from strategy_explorer_types import BotParams, Order, Position, Side

import api.strategy_explorer_core as strategy_explorer_core
from api.strategy_explorer_core import (
    GVData,
    _calc_closes_rust,
    _calc_entries_rust,
    _calc_next_entry_rust,
    _calc_potential_trailing_entry_prices_from_fullgrid,
    _compare_fills_b_c,
    _compare_fills_pb7_b_c,
    _compute_warmup_minutes_for_mode_c,
    _compute_warmup_minutes_for_mode_c_from_config,
    _filter_pb7_events_by_coin,
    _find_1m_gaps,
    _get_passivbot_rust,
    _infer_maker_taker_fees,
    _market_metadata_source_debug,
    _ohlcv_source_debug,
    _order_type_to_str,
    _pb7_src_dir,
    _load_pb7_fills_csv_to_events,
    _resolve_safe_backtest_dir,
    _resolve_safe_ohlcv_source_dir,
    _run_compare_from_pb7_backtest_dir,
    _run_pb7_engine_backtest_for_visualizer,
    _safe_market_segment,
    _simulate_gridfilled_position_for_trailing,
    _simulate_backtest_over_historical_candles_pair,
    _simulate_trailing_sequence_forced,
    _standardize_ohlcv_1m_gaps,
    _try_autofill_exchange_params,
    adjust_order_quantities,
    calculate_v7_indicators,
    get_GridTrailing_mode,
    get_available_coins_v7,
    get_available_exchanges_v7,
    load_historical_ohlcv_v7,
)


def _safe_pb7_fills_to_events(fills_array: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize PB7 fills without assuming every numeric column is numeric."""
    if fills_array is None:
        return [], []
    cols_base = [
        "index",
        "timestamp_ms",
        "coin",
        "pnl",
        "fee_paid",
        "usd_total_balance",
        "btc_cash_wallet",
        "usd_cash_wallet",
        "btc_price",
        "fill_qty",
        "fill_price",
        "position_size",
        "position_price",
        "order_type",
        "wallet_exposure",
        "twe_long",
        "twe_short",
        "twe_net",
    ]
    cols_extended = cols_base + ["minute", "btc_total_balance"]
    try:
        arr = np.asarray(fills_array, dtype=object)
        if arr.size == 0 or arr.ndim != 2 or arr.shape[1] < len(cols_base):
            return [], []
        columns = cols_extended if arr.shape[1] >= len(cols_extended) else cols_base
        df = pd.DataFrame(arr[:, : len(columns)], columns=columns)
    except Exception:
        return [], []

    try:
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp_ms"], errors="coerce"), unit="ms", utc=True).dt.tz_localize(None)
    except Exception:
        df["timestamp"] = pd.to_datetime(df.get("timestamp_ms"), unit="ms", errors="coerce")

    def _num(value: Any, default: float = 0.0) -> float:
        try:
            result = float(value) if value is not None else float(default)
        except (TypeError, ValueError):
            result = float(default)
        return result if math.isfinite(result) else float(default)

    events_long: list[dict[str, Any]] = []
    events_short: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        order_type = str(row.get("order_type") or "")
        event_type = "entry" if order_type.startswith("entry") else "close" if order_type.startswith("close") else "fill"
        event = {
            "timestamp": row.get("timestamp"),
            "event": event_type,
            "qty": _num(row.get("fill_qty")),
            "price": _num(row.get("fill_price")),
            "order_type": order_type,
            "coin": str(row.get("coin") or ""),
            "wallet_balance": _num(row.get("usd_total_balance")),
            "pos_size": _num(row.get("position_size")),
            "pos_price": _num(row.get("position_price")),
            "pnl": _num(row.get("pnl")),
            "fee_paid": _num(row.get("fee_paid")),
            "wallet_exposure": _num(row.get("wallet_exposure")),
        }
        if "_short" in order_type or _num(row.get("position_size")) < 0.0:
            events_short.append(event)
        else:
            events_long.append(event)
    return events_long, events_short


def _run_pb7_engine_safe(**kwargs: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run the PB7 engine helper with robust FastAPI fill normalization."""
    strategy_explorer_core._pb7_fills_to_events = _safe_pb7_fills_to_events
    return strategy_explorer_core._run_pb7_engine_backtest_for_visualizer(**kwargs)

PARAM_GROUPS: list[dict[str, Any]] = [
    {
        "key": "exposure",
        "label": "Exposure",
        "fields": ["total_wallet_exposure_limit", "n_positions"],
    },
    {
        "key": "entry_grid",
        "label": "Entry Grid",
        "fields": [
            "entry_initial_qty_pct",
            "entry_initial_ema_dist",
            "entry_grid_spacing_pct",
            "entry_grid_spacing_we_weight",
            "entry_grid_spacing_volatility_weight",
            "entry_grid_double_down_factor",
        ],
    },
    {
        "key": "entry_trailing",
        "label": "Entry Trailing",
        "fields": [
            "entry_trailing_threshold_pct",
            "entry_trailing_threshold_we_weight",
            "entry_trailing_threshold_volatility_weight",
            "entry_trailing_retracement_pct",
            "entry_trailing_retracement_we_weight",
            "entry_trailing_retracement_volatility_weight",
            "entry_trailing_double_down_factor",
            "entry_trailing_grid_ratio",
        ],
    },
    {
        "key": "indicators_forager",
        "label": "Indicators + Forager",
        "fields": [
            "ema_span_0",
            "ema_span_1",
            "entry_volatility_ema_span_hours",
            "forager_volatility_ema_span",
            "forager_volume_ema_span",
            "forager_volume_drop_pct",
            "forager_score_weights.volume",
            "forager_score_weights.volatility",
            "forager_score_weights.ema_readiness",
        ],
    },
    {
        "key": "close_grid",
        "label": "Close Grid",
        "fields": ["close_grid_markup_start", "close_grid_markup_end", "close_grid_qty_pct"],
    },
    {
        "key": "close_trailing",
        "label": "Close Trailing",
        "fields": [
            "close_trailing_threshold_pct",
            "close_trailing_retracement_pct",
            "close_trailing_qty_pct",
            "close_trailing_grid_ratio",
        ],
    },
    {
        "key": "risk_unstuck",
        "label": "Risk + Unstuck",
        "fields": [
            "unstuck_close_pct",
            "unstuck_ema_dist",
            "unstuck_loss_allowance_pct",
            "unstuck_threshold",
            "risk_we_excess_allowance_pct",
            "risk_wel_enforcer_threshold",
            "risk_twel_enforcer_threshold",
        ],
    },
    {
        "key": "hsl",
        "label": "HSL",
        "fields": [
            "hsl_enabled",
            "hsl_red_threshold",
            "hsl_ema_span_minutes",
            "hsl_cooldown_minutes_after_red",
            "hsl_no_restart_drawdown_threshold",
            "hsl_tier_ratios.yellow",
            "hsl_tier_ratios.orange",
            "hsl_orange_tier_mode",
            "hsl_panic_close_order_type",
            "live.hsl_signal_mode",
        ],
    },
]


def default_strategy_config() -> dict[str, Any]:
    """Return a PB7-style default config for initial page bootstrap."""
    data = GVData()
    return {
        "backtest": {
            "exchanges": ["binance"],
            "start_date": "2020-01-01",
            "end_date": "now",
            "starting_balance": 1000,
        },
        "bot": {
            "long": asdict(data.normal_bot_params_long),
            "short": asdict(data.normal_bot_params_short),
        },
        "live": {"approved_coins": {"long": ["BTC"], "short": ["BTC"]}, "user": "strategy_explorer", "hsl_signal_mode": "unified"},
        "pbgui": {"note": "Strategy Explorer"},
    }


def normalize_strategy_config(config: dict[str, Any] | None, *, neutralize_added: bool = True) -> dict[str, Any]:
    """Normalize Strategy Explorer configs through PB7's canonical config pipeline."""
    source = copy.deepcopy(config) if isinstance(config, dict) else default_strategy_config()
    existing_status = copy.deepcopy(source.get("_pbgui_param_status")) if isinstance(source.get("_pbgui_param_status"), dict) else None
    prepared = prepare_pb7_config_dict(source, neutralize_added=neutralize_added)
    if existing_status and "_pbgui_param_status" not in prepared:
        prepared["_pbgui_param_status"] = existing_status
    return prepared


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
        return f if math.isfinite(f) else float(default)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _nested_dict(root: dict[str, Any], *keys: str) -> dict[str, Any]:
    cur: Any = root
    for key in keys:
        if not isinstance(cur, dict):
            return {}
        cur = cur.get(key)
    return cur if isinstance(cur, dict) else {}


def _first_exchange(config: dict[str, Any]) -> str:
    exchanges = _nested_dict(config, "backtest").get("exchanges")
    if isinstance(exchanges, list) and exchanges:
        return str(exchanges[0] or "").strip() or "binance"
    return "binance"


def _approved_coins(config: dict[str, Any], side: str) -> list[str]:
    approved = _nested_dict(config, "live").get("approved_coins")
    values: Any = []
    if isinstance(approved, dict):
        values = approved.get(side) or []
    elif isinstance(approved, list):
        values = approved
    if not isinstance(values, list):
        return []
    return [str(item).strip() for item in values if str(item).strip()]


def _first_coin(config: dict[str, Any]) -> str:
    for side in ("long", "short"):
        coins = _approved_coins(config, side)
        if coins:
            return coins[0]
    return "BTC"


def _source_settings(config: dict[str, Any], options: dict[str, Any]) -> tuple[str | None, bool]:
    """Return the Strategy Explorer OHLCV source routing."""
    config_source_dir = str(_nested_dict(config, "backtest").get("ohlcv_source_dir") or "").strip()
    requested = str(options.get("ohlcv_source") or "").strip()
    mode = requested or ("Backtest ohlcv_source_dir" if config_source_dir else "PB7 cache/historical")
    if mode == "PBGui market_data":
        path = PBGDIR / "data" / "ohlcv"
        return (str(path) if path.is_dir() else None), True
    if mode == "Backtest ohlcv_source_dir":
        source_dir = str(options.get("ohlcv_source_dir") or config_source_dir or "").strip()
        return (_resolve_safe_ohlcv_source_dir(source_dir), True)
    return None, False


def _side_params(config: dict[str, Any], side: str) -> BotParams:
    default_data = GVData()
    defaults = asdict(default_data.normal_bot_params_long if side == "long" else default_data.normal_bot_params_short)
    params = _nested_dict(config, "bot", side)
    merged = dict(defaults)
    merged.update(params)
    return BotParams.from_dict(merged)


def _order_payload(order: Order, *, index: int, balance: float, wallet_exposure_limit: float, cumulative_cost: float) -> tuple[dict[str, Any], float]:
    qty = abs(_safe_float(getattr(order, "qty", 0.0)))
    price = _safe_float(getattr(order, "price", 0.0))
    cost = qty * price
    cumulative = cumulative_cost + cost
    budget = max(0.0, balance * wallet_exposure_limit)
    twe_pct = int(min(100, max(0, (cumulative / budget * 100.0) if budget > 0.0 else 0.0)))
    order_type = str(getattr(order, "order_type_str", "") or getattr(getattr(order, "order_type", None), "name", ""))
    return {
        "index": index,
        "qty": qty,
        "price": price,
        "cost": cost,
        "max_twe_pct_after": twe_pct,
        "order_type": order_type or "unknown",
    }, cumulative


def _orders_payload(orders: list[Order], *, balance: float, wallet_exposure_limit: float) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cumulative = 0.0
    for idx, order in enumerate(orders, start=1):
        payload, cumulative = _order_payload(
            order,
            index=idx,
            balance=balance,
            wallet_exposure_limit=wallet_exposure_limit,
            cumulative_cost=cumulative,
        )
        out.append(payload)
    return out


def _decoded_orders_payload(orders: list[Order]) -> list[dict[str, Any]]:
    return [
        {
            "qty": _safe_float(getattr(order, "qty", 0.0)),
            "price": _safe_float(getattr(order, "price", 0.0)),
            "order_type": str(getattr(order, "order_type_str", "") or getattr(getattr(order, "order_type", None), "name", "")),
        }
        for order in orders
    ]


def _grid_pct(prices: list[float]) -> float:
    vals = [float(p) for p in prices if _safe_float(p) > 0.0]
    if len(vals) < 2:
        return 0.0
    mn = min(vals)
    mx = max(vals)
    ref = (mn + mx) / 2.0 or mx
    return ((mx - mn) / ref * 100.0) if ref > 0.0 else 0.0


def _avg_price(orders: list[Order]) -> float | None:
    total_qty = sum(abs(_safe_float(o.qty)) for o in orders)
    if total_qty <= 0.0:
        return None
    return sum(abs(_safe_float(o.qty)) * _safe_float(o.price) for o in orders) / total_qty


def _serialize_debug(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _serialize_debug(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_debug(v) for v in value]
    try:
        return asdict(value)
    except Exception:
        return str(value)


def _state_row_payload(row: pd.Series, close_price: float) -> dict[str, float]:
    """Return indicator-derived state fields for one grid-state row."""
    ema0 = _safe_float(row.get("ema_0"), close_price) or close_price
    ema1 = _safe_float(row.get("ema_1"), close_price) or close_price
    ema2 = _safe_float(row.get("ema_2"), close_price) or close_price
    return {
        "close": close_price,
        "ema_lower": min(ema0, ema1, ema2),
        "ema_upper": max(ema0, ema1, ema2),
        "volatility": _safe_float(row.get("volatility"), 0.0),
    }


def _load_candles(
    exchange: str,
    coin: str,
    options: dict[str, Any],
    long_bp: BotParams,
    short_bp: BotParams | None = None,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    if not bool(options.get("load_candles")) or not exchange or not coin:
        return None, {"loaded": False, "reason": "disabled"}
    try:
        source_dir = options.get("_source_dir")
        prefer_source_only = bool(options.get("_prefer_source_only"))
        df = load_historical_ohlcv_v7(exchange, coin, source_dir=source_dir, prefer_source_only=prefer_source_only)
        if df is None or getattr(df, "empty", True):
            return None, {"loaded": False, "reason": "no candles found"}
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index(kind="stable")
        full_rows = int(len(df))
        days = max(0.5, min(3650.0, _safe_float(options.get("context_days"), 5.0)))
        selected_ts = None
        start_date = str(options.get("start_date") or "").strip()
        start_time = str(options.get("start_time") or "00:00").strip() or "00:00"
        if start_date:
            try:
                selected_ts = pd.Timestamp(f"{start_date} {start_time}").floor("min")
            except Exception:
                selected_ts = None
        grid_idx = int(len(df) - 1)
        if isinstance(df.index, pd.DatetimeIndex):
            if selected_ts is not None:
                try:
                    idx = int(df.index.get_indexer([selected_ts], method="nearest")[0])
                except Exception:
                    idx = int(df.index.searchsorted(selected_ts, side="left"))
                idx = max(0, min(idx, int(len(df) - 1)))
                context_candles = max(1, int(float(days) * 1440.0))
                end_slice = min(int(len(df)), idx + context_candles + 1)
                grid_idx = max(idx, end_slice - 1)
                window = df.iloc[idx:end_slice].copy()
            else:
                start_ts = df.index.min()
                end_ts = start_ts + pd.Timedelta(days=days)
                window = df.loc[start_ts:end_ts].copy()
                if len(window):
                    grid_idx = int(df.index.get_indexer([window.index[-1]], method="nearest")[0])
        else:
            window = df.copy()
        state_rows: dict[str, dict[str, float]] = {}
        if len(df):
            close_price = _safe_float(df.iloc[grid_idx].get("close"), 100.0)
            indicator_df = df.iloc[: grid_idx + 1].copy()
            ind_long = calculate_v7_indicators(
                indicator_df,
                long_bp.ema_span_0,
                long_bp.ema_span_1,
                long_bp.entry_volatility_ema_span_hours,
            )
            state_rows["long"] = _state_row_payload(ind_long.iloc[-1], close_price)
            if short_bp is not None:
                same_indicators = (
                    _safe_float(short_bp.ema_span_0) == _safe_float(long_bp.ema_span_0)
                    and _safe_float(short_bp.ema_span_1) == _safe_float(long_bp.ema_span_1)
                    and _safe_float(short_bp.entry_volatility_ema_span_hours) == _safe_float(long_bp.entry_volatility_ema_span_hours)
                )
                if same_indicators:
                    state_rows["short"] = dict(state_rows["long"])
                else:
                    ind_short = calculate_v7_indicators(
                        indicator_df,
                        short_bp.ema_span_0,
                        short_bp.ema_span_1,
                        short_bp.entry_volatility_ema_span_hours,
                    )
                    state_rows["short"] = _state_row_payload(ind_short.iloc[-1], close_price)
        return window, {
            "loaded": True,
            "rows": full_rows,
            "display_rows": int(len(window)),
            "selected_start": pd.to_datetime(window.index[0]).isoformat() if isinstance(window.index, pd.DatetimeIndex) and len(window) else None,
            "grid_time": pd.to_datetime(df.index[grid_idx]).isoformat() if isinstance(df.index, pd.DatetimeIndex) and len(df) else None,
            "state_rows": state_rows,
            "source": _ohlcv_source_debug(exchange, coin),
        }
    except Exception as exc:
        return None, {"loaded": False, "error": str(exc)}


def _candles_payload(df: pd.DataFrame | None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    work = df.copy()
    if isinstance(work.index, pd.DatetimeIndex):
        work["timestamp"] = work.index
    rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        try:
            ts = pd.to_datetime(row.get("timestamp"))
            if pd.isna(ts):
                continue
            rows.append({
                "timestamp": ts.isoformat(),
                "open": _safe_float(row.get("open")),
                "high": _safe_float(row.get("high")),
                "low": _safe_float(row.get("low")),
                "close": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("volume")),
            })
        except Exception:
            continue
    return rows


def _inject_state_from_candles(data: GVData, df: pd.DataFrame | None, fallback_price: float) -> float:
    price = fallback_price if fallback_price > 0.0 else 100.0
    if df is not None and not df.empty:
        last = df.iloc[-1]
        price = _safe_float(last.get("close"), price) or price
        ema0 = _safe_float(last.get("ema_0"), price) or price
        ema1 = _safe_float(last.get("ema_1"), price) or price
        ema2 = _safe_float(last.get("ema_2"), price) or price
        ema_lower = min(ema0, ema1, ema2)
        ema_upper = max(ema0, ema1, ema2)
        volatility = _safe_float(last.get("volatility"), 0.0)
    else:
        ema_lower = price
        ema_upper = price
        volatility = 0.0
    data.state_params.order_book.bid = price
    data.state_params.order_book.ask = price
    data.state_params.ema_bands.lower = ema_lower
    data.state_params.ema_bands.upper = ema_upper
    data.state_params.entry_volatility_logrange_ema_1h = volatility
    data.trailing_price_bundle.max_since_open = price
    data.trailing_price_bundle.min_since_open = price
    data.trailing_price_bundle.max_since_min = price
    data.trailing_price_bundle.min_since_max = price
    return price


def _state_from_candle_payload(base_state: Any, payload: dict[str, Any] | None) -> Any | None:
    """Build a side-specific StateParams clone from candle-derived state metadata."""
    if not isinstance(payload, dict):
        return None
    price = _safe_float(payload.get("close"), 0.0)
    if price <= 0.0:
        return None
    state = copy.deepcopy(base_state)
    state.order_book.bid = price
    state.order_book.ask = price
    state.ema_bands.lower = _safe_float(payload.get("ema_lower"), price) or price
    state.ema_bands.upper = _safe_float(payload.get("ema_upper"), price) or price
    state.entry_volatility_logrange_ema_1h = _safe_float(payload.get("volatility"), 0.0)
    return state


def _build_data(config: dict[str, Any], options: dict[str, Any]) -> tuple[GVData, dict[str, Any], pd.DataFrame | None]:
    source_dir, prefer_source_only = _source_settings(config, options)
    options = dict(options)
    options["_source_dir"] = source_dir
    options["_prefer_source_only"] = prefer_source_only
    exchange = _safe_market_segment(str(options.get("exchange") or _first_exchange(config) or "binance"), "binance")
    coin = _safe_market_segment(str(options.get("coin") or _first_coin(config) or "BTC"), "BTC")
    data = GVData(
        normal_bot_params_long=_side_params(config, "long"),
        normal_bot_params_short=_side_params(config, "short"),
    )
    data.state_params.balance = _safe_float(
        options.get("balance"),
        _safe_float(_nested_dict(config, "backtest").get("starting_balance"), 1000.0),
    )
    data.prepare_data()
    requested_source = str(options.get("ohlcv_source") or "").strip()
    config_source_dir = str(_nested_dict(config, "backtest").get("ohlcv_source_dir") or "").strip()
    metadata = {
        "exchange": exchange,
        "coin": coin,
        "ohlcv_source": requested_source or ("Backtest ohlcv_source_dir" if config_source_dir else "PB7 cache/historical"),
        "ohlcv_source_dir": source_dir or "",
        "market_metadata": {},
        "ohlcv": {},
    }
    try:
        if exchange and coin and bool(options.get("auto_fill_exchange_params", True)):
            _try_autofill_exchange_params(exchange, coin, data)
        if exchange and coin:
            metadata["market_metadata"] = _market_metadata_source_debug(exchange, coin)
    except Exception as exc:
        metadata["market_metadata"] = {"error": str(exc)}

    exchange_overrides = options.get("exchange_params")
    if isinstance(exchange_overrides, dict):
        for field in ("min_cost", "price_step", "min_qty", "qty_step", "c_mult"):
            if field in exchange_overrides:
                try:
                    setattr(data.exchange_params, field, _safe_float(exchange_overrides.get(field), getattr(data.exchange_params, field)))
                except Exception:
                    pass

    hist_df, candle_meta = _load_candles(exchange, coin, options, data.normal_bot_params_long, data.normal_bot_params_short)
    metadata["ohlcv"] = candle_meta
    ref_price = _inject_state_from_candles(data, hist_df, _safe_float(options.get("reference_price"), 100.0))

    state_overrides = options.get("state_params")
    if isinstance(state_overrides, dict):
        if "balance" in state_overrides:
            data.state_params.balance = _safe_float(state_overrides.get("balance"), data.state_params.balance)
        order_book = state_overrides.get("order_book")
        if isinstance(order_book, dict):
            if "bid" in order_book:
                data.state_params.order_book.bid = _safe_float(order_book.get("bid"), data.state_params.order_book.bid)
            if "ask" in order_book:
                data.state_params.order_book.ask = _safe_float(order_book.get("ask"), data.state_params.order_book.ask)
        ema_bands = state_overrides.get("ema_bands")
        if isinstance(ema_bands, dict):
            if "lower" in ema_bands:
                data.state_params.ema_bands.lower = _safe_float(ema_bands.get("lower"), data.state_params.ema_bands.lower)
            if "upper" in ema_bands:
                data.state_params.ema_bands.upper = _safe_float(ema_bands.get("upper"), data.state_params.ema_bands.upper)
        if "entry_volatility_logrange_ema_1h" in state_overrides:
            data.state_params.entry_volatility_logrange_ema_1h = _safe_float(
                state_overrides.get("entry_volatility_logrange_ema_1h"),
                data.state_params.entry_volatility_logrange_ema_1h,
            )
    state_rows = candle_meta.get("state_rows") if isinstance(candle_meta, dict) else None
    if isinstance(state_rows, dict):
        data.state_params_long = _state_from_candle_payload(data.state_params, state_rows.get("long"))
        data.state_params_short = _state_from_candle_payload(data.state_params, state_rows.get("short"))
        if data.state_params_long is not None:
            data.state_params = copy.deepcopy(data.state_params_long)
            ref_price = _safe_float(data.state_params.order_book.bid, ref_price) or ref_price
    data.prepare_data()
    metadata["reference_price"] = ref_price
    return data, metadata, hist_df


def _selected_trade_start(options: dict[str, Any], fallback_df: pd.DataFrame | None) -> pd.Timestamp | None:
    """Return selected Strategy Explorer trade start timestamp."""
    start_date = str(options.get("start_date") or "").strip()
    start_time = str(options.get("start_time") or "00:00").strip() or "00:00"
    if start_date:
        try:
            return pd.Timestamp(f"{start_date} {start_time}").floor("min")
        except Exception:
            pass
    if fallback_df is not None and not fallback_df.empty and isinstance(fallback_df.index, pd.DatetimeIndex):
        try:
            return pd.Timestamp(fallback_df.index[0]).floor("min")
        except Exception:
            return None
    return None


def _simulation_candles_with_warmup(
    config: dict[str, Any],
    options: dict[str, Any],
    *,
    exchange: str,
    coin: str,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
    fallback_df: pd.DataFrame | None,
    forward_candles: int,
) -> tuple[pd.DataFrame | None, pd.Timestamp | None, dict[str, Any]]:
    """Load simulation candles with pre-start warmup but keep trade start unchanged."""
    trade_start = _selected_trade_start(options, fallback_df)
    if trade_start is None:
        return fallback_df, None, {"warmup_minutes": 0, "used_warmup": False}
    try:
        warmup_minutes = int(max(0, _compute_warmup_minutes_for_mode_c_from_config(config, bot_params_long, bot_params_short)))
    except Exception:
        warmup_minutes = 0
    source_dir, prefer_source_only = _source_settings(config, options)
    try:
        full_df = load_historical_ohlcv_v7(exchange, coin, source_dir=source_dir, prefer_source_only=prefer_source_only)
    except Exception:
        full_df = None
    if full_df is None or getattr(full_df, "empty", True):
        return fallback_df, trade_start, {"warmup_minutes": warmup_minutes, "used_warmup": False}
    if isinstance(full_df.index, pd.DatetimeIndex):
        full_df = full_df.sort_index(kind="stable")
    start_ts = trade_start - pd.Timedelta(minutes=warmup_minutes)
    end_ts = trade_start + pd.Timedelta(minutes=max(1, int(forward_candles)) - 1)
    try:
        window = full_df.loc[start_ts:end_ts].copy()
    except Exception:
        window = full_df.copy()
    try:
        window = _standardize_ohlcv_1m_gaps(window, start_ts=start_ts, end_ts=end_ts)
    except Exception:
        pass
    meta = {
        "warmup_minutes": warmup_minutes,
        "used_warmup": True,
        "trade_start": trade_start.isoformat(),
        "warmup_start": start_ts.isoformat(),
        "window_end": end_ts.isoformat(),
        "rows": int(len(window)) if window is not None else 0,
    }
    return window, trade_start, meta


def _calc_side(data: GVData, side: Side, pbr: Any) -> dict[str, Any]:
    is_long = side == Side.Long
    bp = data.normal_bot_params_long if is_long else data.normal_bot_params_short
    grid_bp = data.gridonly_bot_params_long if is_long else data.gridonly_bot_params_short
    entry_pos = data.position_long_enty if is_long else data.position_short_entry
    close_pos = data.position_long_close if is_long else data.position_short_close
    state_params = (data.state_params_long if is_long else data.state_params_short) or data.state_params
    calc_data = copy.copy(data)
    calc_data.state_params = state_params
    balance = _safe_float(state_params.balance)
    npos = max(0, _safe_int(bp.n_positions))
    total_wel = _safe_float(bp.total_wallet_exposure_limit)
    wallet_exposure_limit = (total_wel / float(npos)) if npos else total_wel
    ep_payload = asdict(data.exchange_params)
    sp_payload = asdict(state_params)
    tb_payload = asdict(data.trailing_price_bundle)

    entries = adjust_order_quantities(_calc_entries_rust(pbr, side, calc_data, bp, entry_pos))
    grid_entries = adjust_order_quantities(_calc_entries_rust(pbr, side, calc_data, grid_bp, entry_pos))
    closes = adjust_order_quantities(_calc_closes_rust(pbr, side, calc_data, bp, close_pos))
    grid_closes = adjust_order_quantities(_calc_closes_rust(pbr, side, calc_data, grid_bp, close_pos))
    if is_long:
        data.normal_entries_long = entries
        data.gridonly_entries_long = grid_entries
        data.normal_closes_long = closes
        data.gridonly_closes_long = grid_closes
    else:
        data.normal_entries_short = entries
        data.gridonly_entries_short = grid_entries
        data.normal_closes_short = closes
        data.gridonly_closes_short = grid_closes

    potential, potential_debug = _calc_potential_trailing_entry_prices_from_fullgrid(
        side=side,
        bot_params=bp,
        exchange_params=data.exchange_params,
        balance=balance,
        fullgrid_orders=grid_entries,
    )
    sim_pos, sim_debug = _simulate_gridfilled_position_for_trailing(
        side=side,
        raw_gridonly=[(o.qty, o.price, 0) for o in grid_entries],
        exchange_params=data.exchange_params,
        state_params=state_params,
        bot_params=bp,
    )
    trailing_chain = {}
    trailing_entries: list[Order] = []
    next_entry_current: dict[str, Any] = {}
    next_entry_error = ""
    try:
        next_entry_current = _calc_next_entry_rust(
            pbr=pbr,
            side=side,
            exchange_params=data.exchange_params,
            state_params=state_params,
            trailing_bundle=data.trailing_price_bundle,
            bot_params=bp,
            position=entry_pos,
        )
    except Exception as exc:
        next_entry_error = str(exc)
    if sim_pos is not None:
        trailing_chain = _simulate_trailing_sequence_forced(
            pbr=pbr,
            side=side,
            exchange_params=data.exchange_params,
            state_params=state_params,
            bot_params=bp,
            start_position=sim_pos,
            n_steps=25,
        )
        mode = get_GridTrailing_mode(_safe_float(bp.entry_trailing_grid_ratio))
        if getattr(mode, "name", "") != "GridOnly":
            for step in trailing_chain.get("steps", []) or []:
                nxt = step.get("next") or {}
                typ = str(nxt.get("type", ""))
                qty = _safe_float(nxt.get("qty"), 0.0)
                price = _safe_float(nxt.get("price"), 0.0)
                if qty == 0.0 or price <= 0.0:
                    break
                if getattr(mode, "name", "") == "TrailingFirst" and "trailing" not in typ:
                    break
                trailing_entries.append(Order(qty=qty, price=price, order_type_str=typ))
            trailing_entries = adjust_order_quantities(trailing_entries)
    entry_prices = [_safe_float(o.price) for o in entries]
    close_prices = [_safe_float(o.price) for o in closes]
    entry_grid = _grid_pct(entry_prices)
    close_grid = _grid_pct(close_prices)
    if is_long:
        data.long_entry_grid = entry_grid
        data.long_close_grid = close_grid
        data.potential_entry_trailing_prices_long = list(potential or [])
        data.simulated_entry_trailing_orders_long = trailing_entries
    else:
        data.short_entry_grid = entry_grid
        data.short_close_grid = close_grid
        data.potential_entry_trailing_prices_short = list(potential or [])
        data.simulated_entry_trailing_orders_short = trailing_entries

    entries_for_table = list(entries) + list(trailing_entries)

    return {
        "side": "long" if is_long else "short",
        "active": data.isActive(side),
        "params": asdict(bp),
        "modes": {
            "entry": get_GridTrailing_mode(_safe_float(bp.entry_trailing_grid_ratio)).name,
            "close": get_GridTrailing_mode(_safe_float(bp.close_trailing_grid_ratio)).name,
        },
        "summary": {
            "total_wallet_exposure_limit": total_wel,
            "wallet_exposure_limit_per_position": wallet_exposure_limit,
            "n_positions": npos,
            "entry_orders": len(entries),
            "close_orders": len(closes),
            "entry_avg_price": _avg_price(entries),
            "close_avg_price": _avg_price(closes),
            "entry_grid_pct": entry_grid,
            "close_grid_pct": close_grid,
        },
        "orders": {
            "entries": _orders_payload(entries_for_table, balance=balance, wallet_exposure_limit=wallet_exposure_limit),
            "closes": _orders_payload(closes, balance=balance, wallet_exposure_limit=wallet_exposure_limit),
            "gridonly_entries": _orders_payload(grid_entries, balance=balance, wallet_exposure_limit=wallet_exposure_limit),
            "gridonly_closes": _orders_payload(grid_closes, balance=balance, wallet_exposure_limit=wallet_exposure_limit),
            "normal_entries": _orders_payload(entries, balance=balance, wallet_exposure_limit=wallet_exposure_limit),
            "simulated_entry_trailing": _orders_payload(trailing_entries, balance=balance, wallet_exposure_limit=wallet_exposure_limit),
            "potential_entry_trailing_prices": [_safe_float(price) for price in (potential or []) if _safe_float(price) > 0.0],
        },
        "debug": {
            "entry_input": {
                "bp": asdict(bp),
                "sp": sp_payload,
                "tb": tb_payload,
                "ep": ep_payload,
                "pos": {"size": _safe_float(entry_pos.size), "price": _safe_float(entry_pos.price)},
            },
            "entry_output_decoded": _decoded_orders_payload(entries),
            "entry_gridonly_output_decoded": _decoded_orders_payload(grid_entries),
            "exchange_params": ep_payload,
            "state_params": sp_payload,
            "position_entry": asdict(entry_pos),
            "position_close": asdict(close_pos),
            "potential_trailing": _serialize_debug(potential_debug),
            "gridfilled_position": _serialize_debug(sim_debug),
            "next_entry_current": _serialize_debug(next_entry_current),
            "next_entry_error": next_entry_error,
            "forced_trailing_chain": _serialize_debug(trailing_chain),
        },
    }


def build_strategy_snapshot(
    config: dict[str, Any] | None,
    *,
    source: str = "default",
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a FastAPI Strategy Explorer snapshot using the legacy Rust calculations."""
    cfg = normalize_strategy_config(config)
    opts = dict(options or {})
    messages: list[dict[str, str]] = []
    data, metadata, hist_df = _build_data(cfg, opts)
    live = _nested_dict(cfg, "live")
    pbgui = _nested_dict(cfg, "pbgui")
    title_parts = [str(live.get("user") or "").strip(), str(pbgui.get("note") or "").strip()]
    title = " | ".join([part for part in title_parts if part]) or "Strategy Explorer"

    try:
        pbr = _get_passivbot_rust(pb7dir())
        if pbr is None:
            raise RuntimeError("passivbot_rust is not available")
        long_payload = _calc_side(data, Side.Long, pbr)
        short_payload = _calc_side(data, Side.Short, pbr)
        engine_status = "PB7 Rust calculations active"
    except Exception as exc:
        long_payload = {"side": "long", "active": False, "params": asdict(data.normal_bot_params_long), "orders": {"entries": [], "closes": []}, "debug": {"error": str(exc)}}
        short_payload = {"side": "short", "active": False, "params": asdict(data.normal_bot_params_short), "orders": {"entries": [], "closes": []}, "debug": {"error": str(exc)}}
        engine_status = "PB7 Rust calculations failed"
        messages.append({"level": "error", "text": f"PB7 Rust calculation failed: {exc}"})

    if metadata.get("ohlcv", {}).get("loaded"):
        messages.append({"level": "info", "text": f"Loaded {metadata['ohlcv'].get('rows', 0)} OHLCV candles for chart context."})
    elif opts.get("load_candles"):
        messages.append({"level": "warning", "text": f"No OHLCV candles loaded: {metadata.get('ohlcv', {}).get('reason') or metadata.get('ohlcv', {}).get('error') or 'unknown'}"})

    return {
        "ok": True,
        "source": source,
        "title": title,
        "market": {
            "exchange": metadata.get("exchange"),
            "coin": metadata.get("coin"),
            "reference_price": metadata.get("reference_price"),
            "engine_status": engine_status,
            "ohlcv_status": "Loaded" if metadata.get("ohlcv", {}).get("loaded") else "Candles disabled",
            "metadata": metadata,
        },
        "labels": {
            "local_simulation": "PBGui Simulation",
            "pb7_engine": "PB7 Backtest Engine",
        },
        "param_groups": PARAM_GROUPS,
        "sides": {"long": long_payload, "short": short_payload},
        "candles": _candles_payload(hist_df),
        "config": cfg,
        "options": opts,
        "messages": messages,
    }


def market_options(config: dict[str, Any] | None = None, options: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return Strategy Explorer market selectors."""
    cfg = normalize_strategy_config(config)
    opts = dict(options or {})
    source_dir, prefer_source_only = _source_settings(cfg, opts)
    exchanges = get_available_exchanges_v7(source_dir=source_dir, include_pb7=not prefer_source_only)
    cfg_exc = strategy_explorer_core._canonical_strategy_exchange(_first_exchange(cfg)) or "binance"
    cfg_coins = []
    for side in ("long", "short"):
        for coin in _approved_coins(cfg, side):
            coin = _safe_market_segment(coin)
            if coin and coin not in cfg_coins:
                cfg_coins.append(coin)
    if (
        cfg_exc
        and cfg_exc in strategy_explorer_core._supported_strategy_exchanges()
        and cfg_exc not in exchanges
        and not prefer_source_only
    ):
        exchanges = [cfg_exc] + exchanges
    coins_by_exchange: dict[str, list[str]] = {}
    for exchange in exchanges[:20]:
        try:
            coins = get_available_coins_v7(exchange, source_dir=source_dir, include_pb7=not prefer_source_only)
            if cfg_exc and exchange == cfg_exc and cfg_coins:
                for coin in reversed(cfg_coins):
                    if coin and coin not in coins:
                        coins.insert(0, coin)
            coins_by_exchange[exchange] = coins[:2000]
        except Exception:
            coins_by_exchange[exchange] = []
    return {"ok": True, "exchanges": exchanges, "coins_by_exchange": coins_by_exchange, "source_dir": source_dir, "prefer_source_only": prefer_source_only}


def backtest_result_handoff_options(config: dict[str, Any] | None, result_path: str) -> dict[str, Any]:
    """Return initial Strategy Explorer options derived from a PB7 backtest result folder."""
    cfg = normalize_strategy_config(config)
    path_text = str(result_path or "").strip()
    options: dict[str, Any] = {"load_candles": True}
    messages: list[dict[str, str]] = []
    if not path_text:
        return {"options": options, "messages": messages, "meta": {}}
    safe_path = _resolve_safe_backtest_dir(path_text)
    if not safe_path:
        messages.append({"level": "warning", "text": f"Strategy Explorer backtest result folder not found: {path_text}"})
        return {"options": options, "messages": messages, "meta": {"result_path": path_text}}
    path = Path(safe_path)
    if not path.is_dir():
        messages.append({"level": "warning", "text": f"Strategy Explorer backtest result folder not found: {path_text}"})
        return {"options": options, "messages": messages, "meta": {"result_path": str(path)}}
    if not ((path / "fills.csv").is_file() or (path / "fills.csv.gz").is_file()):
        messages.append({"level": "warning", "text": f"Strategy Explorer backtest result folder has no fills.csv: {path}"})
        return {"options": options, "messages": messages, "meta": {"result_path": str(path)}}

    exchange = _first_exchange(cfg)
    coin = _first_coin(cfg)
    options.update({"exchange": exchange, "coin": coin})
    meta: dict[str, Any] = {"result_path": str(path), "exchange": exchange, "coin": coin}
    try:
        pb7_long, pb7_short = _load_pb7_fills_csv_to_events(str(path))
        events = _filter_pb7_events_by_coin(list(pb7_long or []), coin) + _filter_pb7_events_by_coin(list(pb7_short or []), coin)
        event_times = []
        for event in events:
            ts = event.get("timestamp") if isinstance(event, dict) else None
            if ts is None:
                continue
            try:
                event_times.append(pd.to_datetime(ts).floor("min"))
            except Exception:
                continue
        if event_times:
            first_fill = pd.to_datetime(min(event_times)).floor("min")
            last_fill = pd.to_datetime(max(event_times)).floor("min")
            # The handoff is day-oriented: the result selects the fill date,
            # while the Analysis clock starts at the beginning of that day.
            analysis_start = first_fill.floor("D")
            options["start_date"] = analysis_start.date().isoformat()
            options["start_time"] = "00:00"
            meta.update({
                "fill_start": first_fill.isoformat(),
                "fill_end": last_fill.isoformat(),
                "analysis_start": analysis_start.isoformat(),
                "fills": len(events),
            })
    except Exception as exc:
        messages.append({"level": "warning", "text": f"Failed to inspect Strategy Explorer backtest fills: {exc}"})
    return {"options": options, "messages": messages, "meta": meta}


def _fallback_grid_fill_simulation(
    side_payload: dict[str, Any],
    candles: pd.DataFrame,
    *,
    side: str,
    max_orders: int,
    starting_position: Position | None = None,
) -> list[dict[str, Any]]:
    """Deterministic fill pass used when the PBGui orchestrator path cannot run."""
    events: list[dict[str, Any]] = []
    entries = list(_nested_dict(side_payload, "orders").get("entries") or [])
    closes = list(_nested_dict(side_payload, "orders").get("closes") or [])
    start_size = _safe_float(getattr(starting_position, "size", 0.0)) if starting_position is not None else 0.0
    pos_size = max(0.0, start_size) if side == "long" else min(0.0, start_size)
    entry_i = 0
    close_i = 0
    for ts, row in candles.iterrows():
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        while entry_i < len(entries) and len(events) < max_orders:
            order = entries[entry_i]
            price = _safe_float(order.get("price"))
            hit = low <= price if side == "long" else high >= price
            if not hit:
                break
            qty = _safe_float(order.get("qty"))
            pos_size += qty if side == "long" else -qty
            events.append({
                "timestamp": pd.to_datetime(ts).isoformat(),
                "event": "entry",
                "qty": qty if side == "long" else -qty,
                "price": price,
                "order_type": order.get("order_type"),
                "pos_size": pos_size,
            })
            entry_i += 1
        while abs(pos_size) > 0.0 and close_i < len(closes) and len(events) < max_orders:
            order = closes[close_i]
            price = _safe_float(order.get("price"))
            hit = high >= price if side == "long" else low <= price
            if not hit:
                break
            qty = min(abs(pos_size), _safe_float(order.get("qty")))
            pos_size -= qty if side == "long" else -qty
            events.append({
                "timestamp": pd.to_datetime(ts).isoformat(),
                "event": "close",
                "qty": -qty if side == "long" else qty,
                "price": price,
                "order_type": order.get("order_type"),
                "pos_size": pos_size,
            })
            close_i += 1
        if len(events) >= max_orders:
            break
    return events


def _simulation_start_state(data: Any, opts: dict[str, Any]) -> tuple[Position, Position, float, dict[str, Any]]:
    """Return local simulation start positions and balance from UI options."""
    default_balance = _safe_float(getattr(getattr(data, "state_params", None), "balance", 0.0), 0.0)
    mode = str(opts.get("sim_start_state") or "flat").lower()
    if mode != "manual":
        return Position(size=0.0, price=0.0), Position(size=0.0, price=0.0), default_balance, {
            "mode": "flat",
            "balance": default_balance,
            "long": {"size": 0.0, "price": 0.0},
            "short": {"size": 0.0, "price": 0.0},
        }

    try:
        order_book = getattr(getattr(data, "state_params", None), "order_book", None)
        bid0 = _safe_float(getattr(order_book, "bid", 0.0), 0.0)
        ask0 = _safe_float(getattr(order_book, "ask", 0.0), 0.0)
        px0 = (bid0 + ask0) / 2.0 if bid0 > 0.0 and ask0 > 0.0 else float(ask0 or bid0 or 0.0)
    except Exception:
        px0 = 0.0

    balance = max(0.0, _safe_float(opts.get("sim_start_balance"), default_balance))
    long_size = max(0.0, _safe_float(opts.get("sim_start_long_size"), 0.0))
    long_price = _safe_float(opts.get("sim_start_long_price"), 0.0)
    if long_size != 0.0 and long_price <= 0.0 and px0 > 0.0:
        long_price = px0
    short_size = _safe_float(opts.get("sim_start_short_size"), 0.0)
    short_size = -abs(short_size) if short_size != 0.0 else 0.0
    short_price = _safe_float(opts.get("sim_start_short_price"), 0.0)
    if short_size != 0.0 and short_price <= 0.0 and px0 > 0.0:
        short_price = px0

    long_pos = Position(size=float(long_size), price=float(long_price))
    short_pos = Position(size=float(short_size), price=float(short_price))
    return long_pos, short_pos, float(balance), {
        "mode": "manual",
        "balance": float(balance),
        "long": {"size": float(long_pos.size), "price": float(long_pos.price)},
        "short": {"size": float(short_pos.size), "price": float(short_pos.price)},
    }


def build_strategy_simulation(
    config: dict[str, Any] | None,
    *,
    mode: str,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the migrated Strategy Explorer simulation path."""
    cfg = normalize_strategy_config(config)
    opts = dict(options or {})
    progress_cb = opts.get("_progress_cb")

    def _report(progress: float, message: str) -> None:
        if not callable(progress_cb):
            return
        try:
            progress_cb(max(0.0, min(1.0, float(progress))), str(message))
        except RuntimeError:
            raise
        except Exception:
            pass

    _report(0.02, "Preparing Simulation...")
    opts["load_candles"] = True
    movie_builder = bool(opts.get("_movie_builder"))
    requested_forward_candles = _safe_int(opts.get("sim_max_candles"), 2000)
    if movie_builder:
        sim_forward_candles = max(10, requested_forward_candles)
    else:
        sim_forward_candles = max(10, min(20000, requested_forward_candles))
    opts["max_candles"] = max(_safe_int(opts.get("max_candles"), 0), sim_forward_candles + 2000)
    _report(0.08, "Loading Simulation candles and market state...")
    data, metadata, hist_df = _build_data(cfg, opts)
    if hist_df is None or hist_df.empty:
        return {
            "ok": False,
            "mode": mode,
            "events": {"long": [], "short": []},
            "message": "No OHLCV candles available for simulation.",
            "metadata": metadata,
        }
    pbr = _get_passivbot_rust(pb7dir())
    if pbr is None:
        raise RuntimeError("passivbot_rust is not available")
    exchange = str(metadata.get("exchange") or "")
    coin = str(metadata.get("coin") or "")
    max_candles = sim_forward_candles
    requested_max_orders = _safe_int(opts.get("sim_max_orders"), 200)
    if movie_builder and requested_max_orders <= 0:
        max_orders = 0
    elif movie_builder:
        max_orders = max(1, requested_max_orders)
    else:
        max_orders = max(1, min(2000, requested_max_orders))
    start_pos_long, start_pos_short, start_balance, start_state_meta = _simulation_start_state(data, opts)
    metadata["simulation_start_state"] = start_state_meta if mode == "local_simulation" else {**start_state_meta, "ignored_for": mode}
    _report(0.16, "Preparing Simulation warmup window...")
    sim_df, trade_start_time, warmup_meta = _simulation_candles_with_warmup(
        cfg,
        opts,
        exchange=exchange,
        coin=coin,
        bot_params_long=data.normal_bot_params_long,
        bot_params_short=data.normal_bot_params_short,
        fallback_df=hist_df,
        forward_candles=max_candles,
    )
    metadata["simulation_window"] = warmup_meta
    if sim_df is None or sim_df.empty:
        return {
            "ok": False,
            "mode": mode,
            "events": {"long": [], "short": []},
            "message": "No OHLCV candles available for simulation.",
            "metadata": metadata,
        }
    if trade_start_time is None and len(sim_df.index):
        trade_start_time = pd.to_datetime(sim_df.index[0])
    if mode == "pb7_engine":
        try:
            def _pb7_progress(progress: float, message: str) -> None:
                _report(0.24 + 0.68 * max(0.0, min(1.0, float(progress))), message)

            events_long, events_short = _run_pb7_engine_safe(
                pbr=pbr,
                exchange=exchange,
                coin=coin,
                analysis_time=trade_start_time.to_pydatetime() if hasattr(trade_start_time, "to_pydatetime") else pd.to_datetime(trade_start_time).to_pydatetime(),
                hist_df=sim_df,
                exchange_params=data.exchange_params,
                bot_params_long=data.normal_bot_params_long,
                bot_params_short=data.normal_bot_params_short,
                starting_balance=float(data.state_params.balance),
                max_candles_forward=max_candles,
                config=cfg,
                progress_cb=_pb7_progress,
            )
        except RuntimeError:
            raise
        except Exception as exc:
            return {
                "ok": False,
                "mode": mode,
                "events": {"long": [], "short": []},
                "message": f"PB7 Backtest Engine simulation failed: {exc}",
                "metadata": metadata,
            }
    else:
        try:
            try:
                maker_fee, taker_fee = _infer_maker_taker_fees(exchange, coin)
            except Exception:
                maker_fee, taker_fee = 0.0, 0.0
            live_cfg = cfg.get("live") if isinstance(cfg, dict) else {}
            bt_cfg = cfg.get("backtest") if isinstance(cfg, dict) else {}
            live_cfg = live_cfg if isinstance(live_cfg, dict) else {}
            bt_cfg = bt_cfg if isinstance(bt_cfg, dict) else {}

            def _local_progress(progress: float, message: str) -> None:
                _report(0.24 + 0.68 * max(0.0, min(1.0, float(progress))), f"PBGui Simulation: {message}")

            events_long, events_short = _simulate_backtest_over_historical_candles_pair(
                pbr=pbr,
                pb7_src=_pb7_src_dir(),
                candles=sim_df,
                exchange_params=data.exchange_params,
                bot_params_long=data.normal_bot_params_long,
                bot_params_short=data.normal_bot_params_short,
                starting_position_long=start_pos_long,
                starting_position_short=start_pos_short,
                balance=float(start_balance),
                maker_fee=float(maker_fee or 0.0),
                taker_fee=float(taker_fee or maker_fee or 0.0),
                market_orders_allowed=bool(live_cfg.get("market_orders_allowed", bt_cfg.get("market_orders_allowed", False))),
                market_order_near_touch_threshold=_safe_float(
                    bt_cfg.get("market_order_near_touch_threshold", live_cfg.get("market_order_near_touch_threshold", 0.001)),
                    0.001,
                ),
                market_order_slippage_pct=_safe_float(
                    bt_cfg.get("market_order_slippage_pct", live_cfg.get("market_order_slippage_pct", 0.0005)),
                    0.0005,
                ),
                hsl_signal_mode=str(live_cfg.get("hsl_signal_mode", "unified") or "unified"),
                pnls_max_lookback_days=live_cfg.get("pnls_max_lookback_days", bt_cfg.get("pnls_max_lookback_days", 30.0)),
                trade_start_time=trade_start_time,
                max_orders=max_orders,
                max_candles=len(sim_df),
                progress_cb=_local_progress,
            )
        except RuntimeError:
            raise
        except Exception as exc:
            _report(0.92, "Running PBGui Simulation fallback fill pass...")
            long_payload = _calc_side(data, Side.Long, pbr)
            short_payload = _calc_side(data, Side.Short, pbr)
            return {
                "ok": True,
                "mode": mode,
                "events": {
                    "long": _fallback_grid_fill_simulation(long_payload, sim_df, side="long", max_orders=max_orders, starting_position=start_pos_long),
                    "short": _fallback_grid_fill_simulation(short_payload, sim_df, side="short", max_orders=max_orders, starting_position=start_pos_short),
                },
                "message": f"PBGui Simulation used grid-fill fallback after orchestrator error: {exc}",
                "metadata": metadata,
            }
    _report(0.96, "Finalizing Simulation results...")
    return {
        "ok": True,
        "mode": mode,
        "labels": {"local_simulation": "PBGui Simulation", "pb7_engine": "PB7 Backtest Engine"},
        "events": {"long": list(events_long or []), "short": list(events_short or [])},
        "metadata": metadata,
    }


def _events_summary(events: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    """Return fill counts by side and total."""
    long_count = len(events.get("long") or [])
    short_count = len(events.get("short") or [])
    return {"long": long_count, "short": short_count, "total": long_count + short_count}


def _compare_rows(df: pd.DataFrame, *, mismatches_only: bool, limit: int = 500) -> list[dict[str, Any]]:
    """Serialize compare dataframe rows for the frontend."""
    if df is None or df.empty:
        return []
    work = df.copy()
    if mismatches_only and "status" in work.columns:
        work = work[work["status"] != "match"].copy()
    rows: list[dict[str, Any]] = []
    limited = work.head(max(1, int(limit))).copy()
    for idx, row in zip(limited.index, limited.to_dict(orient="records")):
        out: dict[str, Any] = {}
        try:
            out["compare_index"] = int(idx) + 1
        except Exception:
            out["compare_index"] = idx
        for key, value in row.items():
            if pd.isna(value):
                out[str(key)] = None
            elif hasattr(value, "isoformat"):
                out[str(key)] = value.isoformat()
            else:
                out[str(key)] = value
        rows.append(out)
    return rows


def _status_counts(df: pd.DataFrame) -> dict[str, int]:
    """Count compare status values."""
    if df is None or df.empty or "status" not in df.columns:
        return {}
    return {str(key): int(value) for key, value in df["status"].value_counts(dropna=False).to_dict().items()}


def _first_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return a compact first-event diagnostic payload."""
    if not events:
        return None
    event = events[0]
    return {
        "timestamp": _jsonable(event.get("timestamp")),
        "order_type": event.get("order_type"),
        "qty": event.get("qty"),
        "price": event.get("price"),
    }


def _compare_diagnostics(events: dict[str, dict[str, list[dict[str, Any]]]], meta: dict[str, Any]) -> dict[str, Any]:
    """Build human-readable diagnostics for compare differences."""
    labels = {"pb7": "PB7 Backtest Result", "b": "PBGui Simulation", "c": "PB7 Backtest Engine"}
    first_events = {
        source: {side: _first_event((by_side or {}).get(side) or []) for side in ("long", "short")}
        for source, by_side in (events or {}).items()
    }
    notes: list[str] = []
    b_first = (first_events.get("b") or {}).get("long")
    c_first = (first_events.get("c") or {}).get("long")
    if b_first and c_first:
        try:
            b_ts = pd.to_datetime(b_first.get("timestamp"))
            c_ts = pd.to_datetime(c_first.get("timestamp"))
            delta_mins = abs((c_ts - b_ts).total_seconds()) / 60.0
        except Exception:
            delta_mins = 0.0
        if delta_mins >= 5.0:
            notes.append(
                "First LONG fill differs: PBGui Simulation starts at "
                f"{b_first.get('timestamp')} ({b_first.get('order_type')} @ {b_first.get('price')}), "
                "while PB7 Backtest Engine starts at "
                f"{c_first.get('timestamp')} ({c_first.get('order_type')} @ {c_first.get('price')})."
            )
    window = meta.get("simulation_window") if isinstance(meta, dict) else None
    if isinstance(window, dict) and window.get("used_warmup"):
        notes.append(
            "Compare now loads pre-start warmup candles: "
            f"{window.get('warmup_minutes')} minutes from {window.get('warmup_start')} to trade start {window.get('trade_start')}."
        )
    if isinstance(meta, dict) and meta.get("auto_shifted_from"):
        notes.append(
            "Compare start was auto-shifted from "
            f"{meta.get('auto_shifted_from')} to {meta.get('trade_start_ts')} because earlier candles did not provide enough warmup coverage."
        )
    if b_first and c_first:
        notes.append(
            "Remaining differences are model differences between PBGui Simulation and PB7 Backtest Engine, not table rendering: "
            "PBGui Simulation uses the PBGui/orchestrator candle-walk path, while PB7 Backtest Engine uses PB7's Rust backtest engine state machine."
        )
    return {"labels": labels, "first_events": first_events, "notes": notes}


def _load_pb7_reference_pair(backtest_dir: str, coin: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Load selected-coin PB7 result fills for compare/movie scoring."""
    backtest_dir = _resolve_safe_backtest_dir(str(backtest_dir or "").strip()) or ""
    if not backtest_dir:
        return None
    path = Path(backtest_dir)
    if not path.is_dir():
        return None
    if not (path / "fills.csv").is_file() and not (path / "fills.csv.gz").is_file():
        return None
    try:
        pb7_long, pb7_short = _load_pb7_fills_csv_to_events(str(path))
    except Exception:
        return None
    selected_coin = str(coin or "").strip()
    pair = (
        _filter_pb7_events_by_coin(list(pb7_long or []), selected_coin),
        _filter_pb7_events_by_coin(list(pb7_short or []), selected_coin),
    )
    return pair if pair[0] or pair[1] else None


def _run_strategy_compare_b_c(
    cfg: dict[str, Any],
    opts: dict[str, Any],
    *,
    pbr: Any,
    max_candles: int,
    max_orders: int,
    pb7_reference: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None,
    run_pb7_engine: bool = True,
    progress_cb: Callable[[float, str], None] | None = None,
) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], dict[str, Any]]:
    """Run the PBGui Simulation vs PB7 Backtest Engine compare path."""

    def _report(progress: float, message: str) -> None:
        if not callable(progress_cb):
            return
        try:
            progress_cb(max(0.0, min(1.0, float(progress))), str(message))
        except RuntimeError:
            raise
        except Exception:
            pass

    _report(0.02, "Preparing compare data...")
    data, metadata, display_df = _build_data(cfg, dict(opts, load_candles=True))
    exchange = str(metadata.get("exchange") or opts.get("exchange") or _first_exchange(cfg) or "")
    coin = str(metadata.get("coin") or opts.get("coin") or _first_coin(cfg) or "")
    trade_start = _selected_trade_start(opts, display_df)
    if not exchange or not coin or trade_start is None:
        return {"b": {"long": [], "short": []}, "c": {"long": [], "short": []}}, {"error": "Compare requires exchange, coin, and analysis start time."}
    compare_start = pd.Timestamp(trade_start).floor("min")
    try:
        raw_compare_start = opts.get("_compare_filter_start")
        if raw_compare_start is not None:
            compare_start = pd.Timestamp(pd.to_datetime(raw_compare_start)).floor("min")
    except Exception:
        compare_start = pd.Timestamp(trade_start).floor("min")

    source_dir, prefer_source_only = _source_settings(cfg, opts)
    _report(0.06, "Loading compare candles...")
    hist_df_full = load_historical_ohlcv_v7(exchange, coin, source_dir=source_dir, prefer_source_only=prefer_source_only)
    if hist_df_full is None or getattr(hist_df_full, "empty", True):
        return {"b": {"long": [], "short": []}, "c": {"long": [], "short": []}}, {"error": "No OHLCV candles available for compare.", "exchange": exchange, "coin": coin}
    if isinstance(hist_df_full.index, pd.DatetimeIndex):
        hist_df_full = hist_df_full.sort_index(kind="stable")
    _report(0.10, "Preparing simulation window...")

    try:
        warmup_minutes = int(_compute_warmup_minutes_for_mode_c(data.normal_bot_params_long, data.normal_bot_params_short) or 0)
    except Exception:
        warmup_minutes = 0
    trade_start = pd.Timestamp(trade_start).floor("min")
    compare_start = max(pd.Timestamp(compare_start).floor("min"), pd.Timestamp(trade_start).floor("min"))
    warm_start = pd.Timestamp(trade_start) - pd.Timedelta(minutes=max(0, warmup_minutes))
    sim_end_target = pd.Timestamp(compare_start) + pd.Timedelta(minutes=max(0, int(max_candles) - 1))
    sim_end_run = sim_end_target + pd.Timedelta(minutes=5)
    auto_shifted_from: pd.Timestamp | None = None

    gaps = _find_1m_gaps(hist_df_full, start_ts=pd.Timestamp(warm_start), end_ts=pd.Timestamp(sim_end_target), warmup_minutes=int(warmup_minutes))
    try:
        if bool((gaps or {}).get("insufficient_coverage")) and not bool((gaps or {}).get("has_gaps")):
            recommended = gaps.get("recommended_trade_start") if isinstance(gaps, dict) else None
            if recommended is None and isinstance(gaps, dict) and gaps.get("available_start") is not None:
                recommended = pd.Timestamp(pd.to_datetime(gaps.get("available_start"))).floor("min") + pd.Timedelta(minutes=max(0, int(warmup_minutes)))
            if recommended is not None:
                recommended_ts = pd.Timestamp(pd.to_datetime(recommended)).floor("min")
                if recommended_ts > pd.Timestamp(trade_start).floor("min"):
                    auto_shifted_from = pd.Timestamp(trade_start).floor("min")
                    shift_delta = recommended_ts - pd.Timestamp(trade_start).floor("min")
                    trade_start = recommended_ts
                    try:
                        compare_start = pd.Timestamp(compare_start).floor("min") + shift_delta
                    except Exception:
                        compare_start = recommended_ts
                    warm_start = pd.Timestamp(trade_start) - pd.Timedelta(minutes=max(0, warmup_minutes))
                    sim_end_target = pd.Timestamp(compare_start) + pd.Timedelta(minutes=max(0, int(max_candles) - 1))
                    sim_end_run = sim_end_target + pd.Timedelta(minutes=5)
                    gaps = _find_1m_gaps(hist_df_full, start_ts=pd.Timestamp(warm_start), end_ts=pd.Timestamp(sim_end_target), warmup_minutes=int(warmup_minutes))
    except Exception:
        pass
    if bool((gaps or {}).get("has_gaps")) or bool((gaps or {}).get("insufficient_coverage")):
        return {"b": {"long": [], "short": []}, "c": {"long": [], "short": []}}, {
            "error": "Compare not possible: incomplete 1m candle coverage for warmup/window.",
            "exchange": exchange,
            "coin": coin,
            "trade_start_ts": trade_start,
            "start_ts": trade_start,
            "end_ts": sim_end_target,
            "warmup_minutes": int(warmup_minutes),
            "gaps": _jsonable(gaps),
        }

    try:
        sim_df = hist_df_full.loc[warm_start:sim_end_run].copy()
    except Exception:
        sim_df = hist_df_full.copy()
    if sim_df is None:
        sim_df = pd.DataFrame()
    try:
        sim_df = _standardize_ohlcv_1m_gaps(sim_df, start_ts=warm_start, end_ts=sim_end_run)
    except Exception:
        pass

    def _filter_window(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for event in events or []:
            try:
                ts = pd.to_datetime(event.get("timestamp"))
            except Exception:
                continue
            if ts < compare_start or ts > sim_end_target:
                continue
            out.append(event)
        return out

    try:
        maker_fee, _taker_fee = _infer_maker_taker_fees(exchange, coin)
    except Exception:
        maker_fee = 0.0
        _taker_fee = 0.0
    live_cfg = cfg.get("live") if isinstance(cfg, dict) else {}
    bt_cfg = cfg.get("backtest") if isinstance(cfg, dict) else {}
    live_cfg = live_cfg if isinstance(live_cfg, dict) else {}
    bt_cfg = bt_cfg if isinstance(bt_cfg, dict) else {}
    market_orders_allowed = bool(live_cfg.get("market_orders_allowed", bt_cfg.get("market_orders_allowed", False)))
    market_order_near_touch_threshold = _safe_float(
        bt_cfg.get("market_order_near_touch_threshold", live_cfg.get("market_order_near_touch_threshold", 0.001)),
        0.001,
    )
    market_order_slippage_pct = _safe_float(
        bt_cfg.get("market_order_slippage_pct", live_cfg.get("market_order_slippage_pct", 0.0005)),
        0.0005,
    )

    b_long: list[dict[str, Any]] = []
    b_short: list[dict[str, Any]] = []
    if sim_df is not None and not sim_df.empty:
        try:
            _report(0.14, f"Simulating PBGui candles 0/{max(1, int(len(sim_df)) - 1)}...")

            def _b_progress(progress: float, message: str) -> None:
                _report(0.14 + 0.58 * max(0.0, min(1.0, float(progress))), f"PBGui Simulation: {message}")

            b_long, b_short = _simulate_backtest_over_historical_candles_pair(
                pbr=pbr,
                pb7_src=_pb7_src_dir(),
                candles=sim_df,
                exchange_params=data.exchange_params,
                bot_params_long=data.normal_bot_params_long,
                bot_params_short=data.normal_bot_params_short,
                starting_position_long=Position(size=0.0, price=0.0),
                starting_position_short=Position(size=0.0, price=0.0),
                balance=float(data.state_params.balance),
                maker_fee=float(maker_fee or 0.0),
                taker_fee=float(_taker_fee or maker_fee or 0.0),
                market_orders_allowed=bool(market_orders_allowed),
                market_order_near_touch_threshold=float(market_order_near_touch_threshold),
                market_order_slippage_pct=float(market_order_slippage_pct),
                hsl_signal_mode=str(live_cfg.get("hsl_signal_mode", "unified") or "unified"),
                pnls_max_lookback_days=live_cfg.get("pnls_max_lookback_days", bt_cfg.get("pnls_max_lookback_days", 30.0)),
                trade_start_time=trade_start,
                max_orders=int(max_orders),
                max_candles=int(len(sim_df)),
                progress_cb=_b_progress,
            )
        except RuntimeError:
            raise
        except Exception:
            b_long, b_short = [], []
    b_long = _filter_window(b_long)
    b_short = _filter_window(b_short)

    ref_long: list[dict[str, Any]] = []
    ref_short: list[dict[str, Any]] = []
    if pb7_reference is not None:
        try:
            ref_long = _filter_window(list(pb7_reference[0] or []))
            ref_short = _filter_window(list(pb7_reference[1] or []))
        except Exception:
            ref_long, ref_short = [], []
    score_against_pb7 = bool(ref_long or ref_short)

    price_step = _safe_float(getattr(data.exchange_params, "price_step", 0.0))
    qty_step = _safe_float(getattr(data.exchange_params, "qty_step", 0.0))
    best = {"mismatch_count": None, "warmup_used": None, "c_long": [], "c_short": [], "per_attempt": []}
    if run_pb7_engine:
        warmup_extras = (0, 1000, 2000, 4000, 8000, 12000, 16000)
        total_attempts = max(1, len(warmup_extras))
        for attempt_index, extra in enumerate(warmup_extras, start=1):
            warmup_try = int(max(0, int(warmup_minutes) + int(extra)))
            _report(
                0.72 + 0.23 * (float(attempt_index - 1) / float(total_attempts)),
                f"Running PB7 Backtest Engine warmup attempt {attempt_index}/{total_attempts}...",
            )
            try:
                c_l_try, c_s_try = _run_pb7_engine_safe(
                    pbr=pbr,
                    exchange=exchange,
                    coin=coin,
                    analysis_time=pd.to_datetime(trade_start).to_pydatetime(),
                    hist_df=hist_df_full,
                    exchange_params=data.exchange_params,
                    bot_params_long=data.normal_bot_params_long,
                    bot_params_short=data.normal_bot_params_short,
                    starting_balance=float(data.state_params.balance),
                    max_candles_forward=max(10, int((pd.Timestamp(sim_end_target) - pd.Timestamp(trade_start)).total_seconds() // 60) + 5),
                    config=cfg,
                    warmup_minutes_override=int(warmup_try),
                )
            except RuntimeError:
                raise
            except Exception:
                c_l_try, c_s_try = [], []
            c_l_try = _filter_window(c_l_try)
            c_s_try = _filter_window(c_s_try)
            mismatch_count = 0
            if score_against_pb7:
                try:
                    if ref_long or c_l_try:
                        df_l = _compare_fills_b_c(b_events=ref_long, c_events=c_l_try, price_step=price_step, qty_step=qty_step)
                        mismatch_count += int((df_l["status"] != "match").sum()) if not df_l.empty else 0
                except Exception:
                    pass
                try:
                    if ref_short or c_s_try:
                        df_s = _compare_fills_b_c(b_events=ref_short, c_events=c_s_try, price_step=price_step, qty_step=qty_step)
                        mismatch_count += int((df_s["status"] != "match").sum()) if not df_s.empty else 0
                except Exception:
                    pass
            else:
                try:
                    if b_long or c_l_try:
                        df_l = _compare_fills_b_c(b_events=b_long, c_events=c_l_try, price_step=price_step, qty_step=qty_step)
                        mismatch_count += int((df_l["status"] != "match").sum()) if not df_l.empty else 0
                except Exception:
                    pass
                try:
                    if b_short or c_s_try:
                        df_s = _compare_fills_b_c(b_events=b_short, c_events=c_s_try, price_step=price_step, qty_step=qty_step)
                        mismatch_count += int((df_s["status"] != "match").sum()) if not df_s.empty else 0
                except Exception:
                    pass
            best["per_attempt"].append({"warmup": int(warmup_try), "mismatches": int(mismatch_count)})
            _report(
                0.72 + 0.23 * (float(attempt_index) / float(total_attempts)),
                f"Finished PB7 Backtest Engine warmup attempt {attempt_index}/{total_attempts}: {int(mismatch_count)} mismatches.",
            )
            if best["mismatch_count"] is None or int(mismatch_count) < int(best["mismatch_count"]):
                best["mismatch_count"] = int(mismatch_count)
                best["warmup_used"] = int(warmup_try)
                best["c_long"] = c_l_try
                best["c_short"] = c_s_try
            if int(mismatch_count) == 0:
                break
    else:
        _report(0.95, "PBGui Simulation compare path completed.")

    meta = {
        "exchange": exchange,
        "coin": coin,
        "trade_start_ts": trade_start,
        "start_ts": compare_start,
        "end_ts": sim_end_target,
        "compare_start_ts": compare_start,
        "pre_roll_minutes": max(0, int((pd.Timestamp(compare_start) - pd.Timestamp(trade_start)).total_seconds() // 60)),
        "warmup_minutes": int(warmup_minutes),
        "price_step": float(price_step),
        "qty_step": float(qty_step),
        "compare_mode": "PBGui Simulation vs PB7 Backtest Engine",
        "pb7_engine_run": bool(run_pb7_engine),
        "mode_c_warmup_used": best.get("warmup_used"),
        "mode_c_mismatch_reference": "PB7 Backtest Result" if score_against_pb7 else "PBGui Simulation",
        "mode_c_mismatches": best.get("mismatch_count"),
        "mode_c_mismatches_vs_pbgui": None if score_against_pb7 else best.get("mismatch_count"),
        "mode_c_mismatches_vs_pb7": best.get("mismatch_count") if score_against_pb7 else None,
        "mode_c_attempts": list(best.get("per_attempt") or []),
        "pb7_reference_counts": {"long": len(ref_long), "short": len(ref_short)} if score_against_pb7 else None,
        "auto_shifted_from": auto_shifted_from,
        "market_orders_allowed": bool(market_orders_allowed),
        "simulation_window": {
            "warmup_minutes": int(warmup_minutes),
            "used_warmup": True,
            "trade_start": pd.Timestamp(trade_start).isoformat(),
            "warmup_start": pd.Timestamp(warm_start).isoformat(),
            "window_end": pd.Timestamp(sim_end_target).isoformat(),
            "run_end": pd.Timestamp(sim_end_run).isoformat(),
            "rows": int(len(sim_df)) if sim_df is not None else 0,
        },
    }
    _report(1.0, "Compare path completed.")
    return {
        "b": {"long": list(b_long or []), "short": list(b_short or [])},
        "c": {"long": list(best.get("c_long") or []), "short": list(best.get("c_short") or [])},
    }, meta


def build_strategy_compare(config: dict[str, Any] | None, *, options: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run Strategy Explorer PB7/B/C or B/C-only fill comparison."""
    cfg = normalize_strategy_config(config)
    opts = dict(options or {})
    progress_cb = opts.get("_progress_cb")

    def _report(progress: float, message: str) -> None:
        if not callable(progress_cb):
            return
        try:
            progress_cb(max(0.0, min(1.0, float(progress))), str(message))
        except RuntimeError:
            raise
        except Exception:
            pass

    mode = str(opts.get("compare_mode") or "pb7_b_c")
    max_candles = max(10, min(20000, _safe_int(opts.get("compare_max_candles"), 2000)))
    max_orders = max(1, min(20000, _safe_int(opts.get("compare_max_orders"), 2000)))
    mismatches_only = bool(opts.get("mismatches_only", True))

    message = "Compare finished."
    _report(0.01, "Preparing Compare...")

    if mode == "pb7_b_c":
        backtest_dir_raw = str(opts.get("pb7_backtest_dir") or "").strip()
        backtest_dir = _resolve_safe_backtest_dir(backtest_dir_raw) if backtest_dir_raw else ""
        if not backtest_dir:
            return {"ok": False, "message": "PB7 backtest folder is required for PB7 vs B vs C compare.", "mode": mode}
        backtest_path = Path(backtest_dir)
        if not backtest_path.is_dir():
            return {"ok": False, "message": "PB7 backtest folder was not found. Select a result folder containing fills.csv.", "mode": mode}
        if not (backtest_path / "fills.csv").is_file() and not (backtest_path / "fills.csv.gz").is_file():
            return {"ok": False, "message": "PB7 backtest folder does not contain fills.csv or fills.csv.gz.", "mode": mode}
        _report(0.04, "Loading PB7 fills.csv...")
        pb7_pair = _load_pb7_fills_csv_to_events(backtest_dir)
        selected_coin = str(opts.get("coin") or _first_coin(cfg) or "").strip()
        _report(0.07, "Filtering PB7 fills for selected coin...")
        pb7_pair = (
            _filter_pb7_events_by_coin(list(pb7_pair[0] or []), selected_coin),
            _filter_pb7_events_by_coin(list(pb7_pair[1] or []), selected_coin),
        )
        if not (pb7_pair[0] or pb7_pair[1]):
            return {"ok": False, "message": "No PB7 fills were found in the selected fills.csv for compare.", "mode": mode, "meta": {"pb7_backtest_dir": backtest_dir}}
        all_pb7 = list(pb7_pair[0] or []) + list(pb7_pair[1] or [])
        use_fills_range = bool(opts.get("use_fills_range", True))
        auto_bounded_reason = ""
        auto_bounded_start: pd.Timestamp | None = None
        auto_bounded_source = ""
        auto_bounded_fill_start: pd.Timestamp | None = None
        _report(0.10, "Checking PB7 fills.csv time range...")
        if use_fills_range:
            try:
                fill_times = pd.to_datetime([event.get("timestamp") for event in all_pb7 if event.get("timestamp") is not None], errors="coerce").dropna()
                if len(fill_times):
                    fill_start = pd.to_datetime(fill_times.min())
                    fill_end = pd.to_datetime(fill_times.max())
                    fills_range_minutes = int((fill_end - fill_start).total_seconds() // 60) + 1
                    if fills_range_minutes > max_candles:
                        use_fills_range = False
                        auto_bounded_fill_start = pd.Timestamp(fill_start).floor("min")
                        selected_start = _selected_trade_start(opts, None)
                        if selected_start is not None:
                            auto_bounded_start = pd.Timestamp(selected_start).floor("min")
                            auto_bounded_source = "selected Analysis start"
                        else:
                            auto_bounded_start = auto_bounded_fill_start
                            auto_bounded_source = "first selected-coin fill"
                        auto_bounded_reason = (
                            f"fills.csv range spans {fills_range_minutes:,} one-minute candles, so Compare used a bounded "
                            f"{max_candles:,}-candle window starting at the {auto_bounded_source} instead of the full fills.csv range."
                        )
            except Exception:
                pass
        pbr = _get_passivbot_rust(pb7dir())
        if pbr is None:
            raise RuntimeError("passivbot_rust is not available")
        if use_fills_range:
            def _fills_range_progress(progress: float, progress_message: str) -> None:
                _report(0.14 + 0.76 * max(0.0, min(1.0, float(progress))), progress_message)

            pb7_pair, b_pair, c_pair, meta = _run_compare_from_pb7_backtest_dir(
                pbr=pbr,
                pb7_src=_pb7_src_dir(),
                backtest_dir=backtest_dir,
                max_orders=max_orders,
                progress_cb=_fills_range_progress,
            )
            pb7_pair = (
                _filter_pb7_events_by_coin(list(pb7_pair[0] or []), selected_coin),
                _filter_pb7_events_by_coin(list(pb7_pair[1] or []), selected_coin),
            )
        else:
            if auto_bounded_start is not None:
                simulation_start = pd.Timestamp(auto_bounded_start).floor("min") - pd.Timedelta(minutes=1)
                opts["start_date"] = simulation_start.date().isoformat()
                opts["start_time"] = simulation_start.strftime("%H:%M")
                opts["_compare_filter_start"] = pd.Timestamp(auto_bounded_start).isoformat()

            def _bounded_progress(progress: float, progress_message: str) -> None:
                _report(0.14 + 0.76 * max(0.0, min(1.0, float(progress))), progress_message)

            bc_events, meta = _run_strategy_compare_b_c(
                cfg,
                opts,
                pbr=pbr,
                max_candles=max_candles,
                max_orders=max_orders,
                pb7_reference=pb7_pair,
                progress_cb=_bounded_progress,
            )
            if isinstance(meta, dict) and meta.get("error"):
                return {"ok": False, "message": str(meta.get("error") or "Compare failed."), "mode": mode, "meta": _jsonable(meta)}
            b_pair = (bc_events["b"]["long"], bc_events["b"]["short"])
            c_pair = (bc_events["c"]["long"], bc_events["c"]["short"])
            meta = dict(meta or {})
            meta["pb7_backtest_dir"] = backtest_dir
            meta["use_fills_range"] = bool(opts.get("use_fills_range", True))
            if auto_bounded_reason:
                meta["auto_bounded_fills_range"] = True
                meta["auto_bounded_reason"] = auto_bounded_reason
                meta["auto_bounded_source"] = auto_bounded_source or None
                if auto_bounded_fill_start is not None:
                    meta["auto_bounded_fill_start"] = pd.Timestamp(auto_bounded_fill_start).isoformat()
                if auto_bounded_start is not None:
                    meta["auto_bounded_start"] = pd.Timestamp(auto_bounded_start).isoformat()
                message = auto_bounded_reason

            def _filter_window(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
                out: list[dict[str, Any]] = []
                try:
                    start_ts = pd.to_datetime(meta.get("start_ts") or meta.get("trade_start_ts"))
                    end_ts = pd.to_datetime(meta.get("end_ts") or meta.get("window_end"))
                except Exception:
                    return list(events or [])
                for event in events or []:
                    try:
                        ts = pd.to_datetime(event.get("timestamp"))
                    except Exception:
                        continue
                    if ts < start_ts or ts > end_ts:
                        continue
                    out.append(event)
                return out

            pb7_pair = (_filter_window(list(pb7_pair[0] or [])), _filter_window(list(pb7_pair[1] or [])))
        price_step = _safe_float((meta or {}).get("price_step"))
        qty_step = _safe_float((meta or {}).get("qty_step"))
        _report(0.93, "Building Compare tables...")
        long_df = _compare_fills_pb7_b_c(pb7_events=pb7_pair[0], b_events=b_pair[0], c_events=c_pair[0], price_step=price_step, qty_step=qty_step)
        short_df = _compare_fills_pb7_b_c(pb7_events=pb7_pair[1], b_events=b_pair[1], c_events=c_pair[1], price_step=price_step, qty_step=qty_step)
        events = {
            "pb7": {"long": list(pb7_pair[0] or []), "short": list(pb7_pair[1] or [])},
            "b": {"long": list(b_pair[0] or []), "short": list(b_pair[1] or [])},
            "c": {"long": list(c_pair[0] or []), "short": list(c_pair[1] or [])},
        }
    else:
        pbr = _get_passivbot_rust(pb7dir())
        if pbr is None:
            raise RuntimeError("passivbot_rust is not available")

        def _bc_progress(progress: float, progress_message: str) -> None:
            _report(0.08 + 0.82 * max(0.0, min(1.0, float(progress))), progress_message)

        events, meta = _run_strategy_compare_b_c(cfg, opts, pbr=pbr, max_candles=max_candles, max_orders=max_orders, progress_cb=_bc_progress)
        if isinstance(meta, dict) and meta.get("error"):
            return {"ok": False, "message": str(meta.get("error") or "Compare failed."), "mode": mode, "meta": _jsonable(meta)}
        price_step = _safe_float((meta or {}).get("price_step"))
        qty_step = _safe_float((meta or {}).get("qty_step"))
        _report(0.93, "Building Compare tables...")
        long_df = _compare_fills_b_c(b_events=events["b"]["long"], c_events=events["c"]["long"], price_step=price_step, qty_step=qty_step)
        short_df = _compare_fills_b_c(b_events=events["b"]["short"], c_events=events["c"]["short"], price_step=price_step, qty_step=qty_step)

    _report(0.98, "Finalizing Compare results...")
    result = {
        "ok": True,
        "mode": mode,
        "message": message,
        "meta": _jsonable(meta),
        "summary": {
            "long": _status_counts(long_df),
            "short": _status_counts(short_df),
            "events": {key: _events_summary(value) for key, value in events.items()},
        },
        "diagnostics": _compare_diagnostics(events, _jsonable(meta) if isinstance(meta, dict) else {}),
        "rows": {
            "long": _compare_rows(long_df, mismatches_only=mismatches_only),
            "short": _compare_rows(short_df, mismatches_only=mismatches_only),
        },
    }
    _report(1.0, "Compare finished.")
    return result


def deepcopy_events(result: dict[str, Any], side: str) -> list[dict[str, Any]]:
    """Return a copied event list from a simulation result."""
    return list(((result.get("events") or {}).get(side) or []))


def _jsonable(value: Any) -> Any:
    """Convert pandas/datetime values into JSON-safe values."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def build_movie_frames(config: dict[str, Any] | None, *, options: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Strategy Explorer replay frames for the Movie Builder."""
    cfg = normalize_strategy_config(config)
    opts = dict(options or {})
    progress_cb = opts.get("_progress_cb")
    cancel_cb = opts.get("_cancel_cb")

    def _progress(progress: float, message: str) -> None:
        if callable(progress_cb):
            try:
                progress_cb(float(progress), str(message))
            except Exception:
                pass

    def _check_cancelled() -> None:
        if callable(cancel_cb):
            try:
                if bool(cancel_cb()):
                    raise RuntimeError("Movie Builder stopped.")
            except RuntimeError:
                raise
            except Exception:
                pass

    opts["load_candles"] = True
    frame_count = max(1, min(20000, _safe_int(opts.get("frames"), 60)))
    step_mins = max(1, min(10080, _safe_int(opts.get("step_mins"), 60)))
    visible_candles = max(10, min(500, _safe_int(opts.get("visible_candles"), 60)))
    engine = str(opts.get("movie_engine") or "local_simulation")
    side_key = str(opts.get("movie_side") or "long").lower()
    side_key = "short" if side_key == "short" else "long"
    window_mins = int(visible_candles * step_mins)
    opts["max_candles"] = max(_safe_int(opts.get("max_candles"), 0), frame_count * step_mins + 500)
    opts["context_days"] = max(_safe_float(opts.get("context_days"), 5.0), float(window_mins) / 1440.0 + 1.0)
    _progress(0.03, "Loading candles...")
    data, metadata, hist_df = _build_data(cfg, opts)
    if hist_df is None or hist_df.empty:
        return {"ok": False, "frames": [], "message": "No OHLCV candles available for Movie Builder.", "metadata": metadata}
    _check_cancelled()
    exchange = str(metadata.get("exchange") or opts.get("exchange") or _first_exchange(cfg) or "")
    coin = str(metadata.get("coin") or opts.get("coin") or _first_coin(cfg) or "")
    start_time = _selected_trade_start(opts, hist_df)
    if start_time is None:
        try:
            start_time = pd.Timestamp(hist_df.index[0]).floor("min")
        except Exception:
            start_time = None
    if start_time is None:
        return {"ok": False, "frames": [], "message": "Movie Builder requires a valid start time.", "metadata": metadata}
    start_time = pd.Timestamp(start_time).floor("min")
    end_time = start_time + pd.Timedelta(minutes=int(frame_count) * int(step_mins))

    source_dir, prefer_source_only = _source_settings(cfg, opts)
    try:
        full_df = load_historical_ohlcv_v7(exchange, coin, source_dir=source_dir, prefer_source_only=prefer_source_only)
    except Exception:
        full_df = hist_df
    if full_df is None or getattr(full_df, "empty", True):
        full_df = hist_df
    if full_df is None or full_df.empty:
        return {"ok": False, "frames": [], "message": "No OHLCV candles available for Movie Builder.", "metadata": metadata}
    if not isinstance(full_df.index, pd.DatetimeIndex):
        try:
            full_df.index = pd.to_datetime(full_df.index)
        except Exception:
            pass
    if isinstance(full_df.index, pd.DatetimeIndex):
        full_df = full_df.sort_index(kind="stable")

    _progress(0.12, "Preparing movie window...")
    warm_start = start_time - pd.Timedelta(days=float(opts["context_days"]))
    try:
        work = full_df.loc[(full_df.index >= warm_start) & (full_df.index <= end_time)].copy()
    except Exception:
        work = full_df.copy()
    if work is None or work.empty:
        return {"ok": False, "frames": [], "message": "No candles for the selected Movie Builder time window.", "metadata": metadata}
    bp_plot = data.normal_bot_params_short if side_key == "short" else data.normal_bot_params_long
    try:
        work = calculate_v7_indicators(
            work,
            float(bp_plot.ema_span_0),
            float(bp_plot.ema_span_1),
            float(bp_plot.entry_volatility_ema_span_hours),
        )
    except Exception:
        pass
    stride = max(1, int(step_mins))
    if stride > 1 and isinstance(work.index, pd.DatetimeIndex):
        agg: dict[str, str] = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        for col in work.columns:
            if col not in agg:
                agg[str(col)] = "last"
        try:
            frame_source = work.resample(f"{stride}min", origin="start_day", label="right", closed="right").agg(agg).dropna(subset=["open", "high", "low", "close"])
        except Exception:
            frame_source = work.iloc[::stride].copy()
    else:
        frame_source = work
    try:
        frame_source = frame_source.dropna(subset=["open", "high", "low", "close"])
    except Exception:
        pass
    try:
        frame_rows = frame_source.loc[(frame_source.index >= start_time) & (frame_source.index <= end_time)].head(frame_count)
    except Exception:
        frame_rows = frame_source.head(frame_count)
    if frame_rows is None or frame_rows.empty:
        return {"ok": False, "frames": [], "message": "No frames generated for the selected Movie Builder time window.", "metadata": metadata}
    events = {"long": [], "short": []}
    frame_progress_base = 0.25
    frame_progress_span = 0.72
    if engine == "local_simulation":
        _progress(0.18, "Running PBGui Simulation compare path...")
        pbr = _get_passivbot_rust(pb7dir())
        if pbr is None:
            raise RuntimeError("passivbot_rust is not available")
        compare_opts = dict(opts)
        compare_opts["start_date"] = start_time.date().isoformat()
        compare_opts["start_time"] = start_time.strftime("%H:%M")

        def _compare_progress(progress: float, message: str) -> None:
            _check_cancelled()
            _progress(0.18 + 0.57 * max(0.0, min(1.0, float(progress))), message)

        bc_events, compare_meta = _run_strategy_compare_b_c(
            cfg,
            compare_opts,
            pbr=pbr,
            max_candles=max(10, int(frame_count) * int(step_mins) + 10),
            max_orders=0,
            run_pb7_engine=False,
            progress_cb=_compare_progress,
        )
        frame_progress_base = 0.76
        frame_progress_span = 0.21
        _check_cancelled()
        if isinstance(compare_meta, dict):
            metadata["compare_path"] = _jsonable(compare_meta)
        events = (bc_events or {}).get("b") if isinstance(bc_events, dict) else events
        if not isinstance(events, dict):
            events = {"long": [], "short": []}
    elif engine == "pb7_engine":
        _progress(0.18, "Running PB7 Backtest Engine compare path...")
        pbr = _get_passivbot_rust(pb7dir())
        if pbr is None:
            raise RuntimeError("passivbot_rust is not available")
        compare_opts = dict(opts)
        compare_opts["start_date"] = start_time.date().isoformat()
        compare_opts["start_time"] = start_time.strftime("%H:%M")
        pb7_reference = _load_pb7_reference_pair(str(compare_opts.get("pb7_backtest_dir") or ""), coin)

        def _compare_progress(progress: float, message: str) -> None:
            _check_cancelled()
            _progress(0.18 + 0.57 * max(0.0, min(1.0, float(progress))), message)

        bc_events, compare_meta = _run_strategy_compare_b_c(
            cfg,
            compare_opts,
            pbr=pbr,
            max_candles=max(10, int(frame_count) * int(step_mins) + 10),
            max_orders=0,
            pb7_reference=pb7_reference,
            progress_cb=_compare_progress,
        )
        frame_progress_base = 0.76
        frame_progress_span = 0.21
        _check_cancelled()
        if isinstance(compare_meta, dict):
            metadata["compare_path"] = _jsonable(compare_meta)
        events = (bc_events or {}).get("c") if isinstance(bc_events, dict) else events
        if not isinstance(events, dict):
            events = {"long": [], "short": []}
    elif engine == "pb7_fills":
        _progress(0.18, "Loading PB7 fills.csv...")
        backtest_dir = _resolve_safe_backtest_dir(str(opts.get("pb7_backtest_dir") or "").strip()) or ""
        if backtest_dir:
            pb7_long, pb7_short = _load_pb7_fills_csv_to_events(backtest_dir)
            _check_cancelled()
            coin = str(metadata.get("coin") or opts.get("coin") or _first_coin(cfg) or "")
            events = {
                "long": _filter_pb7_events_by_coin(list(pb7_long or []), coin),
                "short": _filter_pb7_events_by_coin(list(pb7_short or []), coin),
            }
    try:
        window_start = pd.to_datetime(start_time)
        window_end = pd.to_datetime(end_time)
    except Exception:
        window_start = None
        window_end = None
    if window_start is not None and window_end is not None:
        def _event_window(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for event in items or []:
                try:
                    ts = pd.to_datetime(event.get("timestamp"))
                except Exception:
                    continue
                if ts < window_start or ts > window_end:
                    continue
                out.append(event)
            return out
        events = {"long": _event_window(list((events or {}).get("long") or [])), "short": _event_window(list((events or {}).get("short") or []))}
    frames: list[dict[str, Any]] = []
    total_frames = max(1, int(len(frame_rows)))
    progress_every = max(1, int(total_frames // 100))
    for idx, (ts, row) in enumerate(frame_rows.iterrows(), start=1):
        if idx == 1 or idx == total_frames or idx % progress_every == 0:
            _check_cancelled()
            _progress(frame_progress_base + frame_progress_span * (float(idx) / float(total_frames)), f"Building frames {idx}/{total_frames}...")
        frame_opts = dict(opts)
        frame_opts["load_candles"] = False
        close_price = _safe_float(row.get("close"), _safe_float(metadata.get("reference_price"), 100.0))
        ema_values = [
            _safe_float(row.get("ema_0"), close_price),
            _safe_float(row.get("ema_1"), close_price),
            _safe_float(row.get("ema_2"), close_price),
        ]
        frame_opts["reference_price"] = close_price
        frame_opts["balance"] = _safe_float(data.state_params.balance, 1000.0)
        frame_opts["state_params"] = {
            "balance": _safe_float(data.state_params.balance, 1000.0),
            "order_book": {"bid": close_price, "ask": close_price},
            "ema_bands": {"lower": min(ema_values), "upper": max(ema_values)},
            "entry_volatility_logrange_ema_1h": _safe_float(row.get("volatility"), 0.0),
        }
        snap = build_strategy_snapshot(cfg, source="movie", options=frame_opts)
        frames.append({
            "index": idx,
            "timestamp": pd.to_datetime(ts).isoformat(),
            "candle": {
                "open": _safe_float(row.get("open")),
                "high": _safe_float(row.get("high")),
                "low": _safe_float(row.get("low")),
                "close": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("volume")),
            },
            "long": snap.get("sides", {}).get("long", {}),
            "short": snap.get("sides", {}).get("short", {}),
        })
    _progress(0.98, "Finalizing movie...")
    return {
        "ok": True,
        "frames": frames,
        "events": _jsonable(events),
        "metadata": metadata,
        "engine": engine,
        "visible_candles": visible_candles,
        "message": f"Built {len(frames)} Strategy Explorer movie frames.",
    }
