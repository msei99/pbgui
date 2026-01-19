try:
    import streamlit as st  # Streamlit library for creating web apps
    _STREAMLIT_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover
    _STREAMLIT_AVAILABLE = False

    class _NoStreamlitCacheDecorator:
        def __call__(self, func=None, **_kwargs):
            if func is None:
                def _deco(f):
                    return f
                return _deco
            return func

    class _StreamlitStub:
        cache_resource = _NoStreamlitCacheDecorator()
        cache_data = _NoStreamlitCacheDecorator()

        def __getattr__(self, name: str):
            raise RuntimeError(
                f"streamlit is required for '{name}'. "
                "Install streamlit or run via the Streamlit UI."
            )

    st = _StreamlitStub()  # type: ignore
import time
import math
import datetime
try:
    from pbgui_func import (
        set_page_config,
        is_session_state_not_initialized,
        error_popup,
        is_pb7_installed,
        is_authenticted,
        get_navi_paths,
        pb7dir,
    )
except ModuleNotFoundError:  # pragma: no cover
    # Allow importing this module for headless tooling (e.g. compare scripts)
    # without requiring Streamlit/UI helpers.
    def _requires_streamlit(*_args, **_kwargs):
        raise RuntimeError("This function requires streamlit UI context")

    set_page_config = _requires_streamlit
    is_session_state_not_initialized = _requires_streamlit
    error_popup = _requires_streamlit
    is_pb7_installed = _requires_streamlit
    is_authenticted = _requires_streamlit
    get_navi_paths = _requires_streamlit

    def pb7dir() -> str:
        # Used by headless helpers as well; default to workspace-relative pb7.
        import os as _os

        return _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..", "pb7"))
import numpy as np  # NumPy for numerical operations
import pandas as pd  # Pandas for data manipulation and analysis
try:
    import plotly.graph_objs as go  # Plotly for interactive data visualization
except ModuleNotFoundError:  # pragma: no cover
    go = None
from dataclasses import dataclass, field, asdict, replace
import json
from typing import List, Any, Optional, Callable
import copy
import os
import sys
try:
    from Config import ConfigV7
except ModuleNotFoundError:  # pragma: no cover
    class ConfigV7:  # type: ignore
        pass
import json
from dataclasses import dataclass, field, asdict
from typing import List
from GridVisualizerV7 import (
    ExchangeParams,
    StateParams,
    BotParams,
    EmaBands,
    OrderBook,
    Position,
    TrailingPriceBundle,
    Order,
    OrderType,
    Side,
    GridTrailingMode
)

from dataclasses import dataclass, asdict
import json

def get_GridTrailing_mode(trailing_grid_ratio: float) -> GridTrailingMode:
    if trailing_grid_ratio == 0.0:
        return GridTrailingMode.GridOnly
    elif trailing_grid_ratio == -1.0:
        return  GridTrailingMode.TrailingOnly
    elif trailing_grid_ratio == 1.0:
        return  GridTrailingMode.TrailingOnly
    elif trailing_grid_ratio < 0.0:
        return  GridTrailingMode.GridFirst
    elif trailing_grid_ratio > 0.0:
        return  GridTrailingMode.TrailingFirst
    return GridTrailingMode.Unknown 


def _import_passivbot_rust(pb7_src_dir: str):
    if pb7_src_dir and pb7_src_dir not in sys.path:
        sys.path.insert(0, pb7_src_dir)
    import passivbot_rust as pbr  # type: ignore
    return pbr


@st.cache_resource
def _get_passivbot_rust(pb7_src_dir: str):
    return _import_passivbot_rust(pb7_src_dir)

def get_available_exchanges_v7() -> List[str]:
    exchanges: set[str] = set()

    # 1) Historical data layout: pb7/historical_data/ohlcvs_<exchange>/...
    hist_dir = os.path.join(pb7dir(), "historical_data")
    if os.path.isdir(hist_dir):
        for d in os.listdir(hist_dir):
            if not d.startswith("ohlcvs_"):
                continue
            path = os.path.join(hist_dir, d)
            if os.path.isdir(path):
                exchanges.add(d[len("ohlcvs_") :])

    # 2) CandlestickManager cache layout: pb7/caches/ohlcv/<exchange>/1m/...
    cm_root = os.path.join(pb7dir(), "caches", "ohlcv")
    if os.path.isdir(cm_root):
        for d in os.listdir(cm_root):
            path = os.path.join(cm_root, d, "1m")
            if os.path.isdir(path):
                exchanges.add(d)

    return sorted(exchanges)

def get_available_symbols_v7(exchange: str) -> List[str]:
    if not exchange:
        return []

    symbols: set[str] = set()

    # 1) Historical data layout: pb7/historical_data/ohlcvs_<exchange>/<symbol>/YYYY-MM-DD.npy
    hist_sym_dir = os.path.join(pb7dir(), "historical_data", f"ohlcvs_{exchange}")
    if os.path.isdir(hist_sym_dir):
        for d in os.listdir(hist_sym_dir):
            p = os.path.join(hist_sym_dir, d)
            # folders are symbols; ignore day shards living directly in the exchange folder
            if os.path.isdir(p) and not d.startswith("."):
                symbols.add(d)

    # 2) CandlestickManager cache layout: pb7/caches/ohlcv/<exchange>/1m/<symbol_code>/YYYY-MM-DD.npy
    cm_sym_dir = os.path.join(pb7dir(), "caches", "ohlcv", exchange, "1m")
    if os.path.isdir(cm_sym_dir):
        for d in os.listdir(cm_sym_dir):
            p = os.path.join(cm_sym_dir, d)
            if os.path.isdir(p) and not d.startswith("."):
                symbols.add(d)

    return sorted(symbols)


def _coin_from_symbol_code(symbol_code: str) -> str:
    s = (symbol_code or "").strip()
    if not s:
        return ""

    # Common PB7 symbol codes:
    # - "DOGE_USDT:USDT" -> "DOGE"
    # - "BTC/USDT:USDT"  -> "BTC"
    for sep in ("_", "/", ":", "-"):
        if sep in s:
            return s.split(sep, 1)[0]

    # Common spot-ish symbol codes:
    # - "DOGEUSDT" -> "DOGE"
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4]

    return s


def _exchange_has_local_ohlcv(exchange: str, symbol: str) -> bool:
    """Return True if there are any local 1m OHLCV shards for (exchange, symbol/coin)."""
    exc = (exchange or "").strip()
    sym_raw = (symbol or "").strip()
    if not exc or not sym_raw:
        return False

    coin_base = _coin_from_symbol_code(sym_raw)
    coin_candidates = [c for c in [sym_raw, coin_base] if c]

    def _dir_matches_coin(d: str, coin: str) -> bool:
        if not d or not coin:
            return False
        if d == coin:
            return True
        if d.startswith(f"{coin}_"):
            return True
        if d.startswith(f"{coin}:"):
            return True
        if d.startswith(f"{coin}/"):
            return True
        if d.startswith(f"{coin}-"):
            return True
        if d.startswith(coin):
            return True
        return False

    # historical_data layout
    hist_root = os.path.join(pb7dir(), "historical_data", f"ohlcvs_{exc}")
    if os.path.isdir(hist_root):
        try:
            for d in os.listdir(hist_root):
                p = os.path.join(hist_root, d)
                if not os.path.isdir(p) or d.startswith("."):
                    continue
                if any(_dir_matches_coin(d, c) for c in coin_candidates):
                    try:
                        if any(fn.endswith(".npy") and not fn.startswith(".") for fn in os.listdir(p)):
                            return True
                    except Exception:
                        continue
        except Exception:
            pass

    # CandlestickManager cache layout
    cm_base = os.path.join(pb7dir(), "caches", "ohlcv", exc, "1m")
    if os.path.isdir(cm_base):
        try:
            for d in os.listdir(cm_base):
                p = os.path.join(cm_base, d)
                if not os.path.isdir(p) or d.startswith("."):
                    continue
                if any(_dir_matches_coin(d, c) for c in coin_candidates):
                    try:
                        if any(fn.endswith(".npy") and not fn.startswith(".") for fn in os.listdir(p)):
                            return True
                    except Exception:
                        continue
        except Exception:
            pass

    return False


def _resolve_exchange_for_history(exchange: str, symbol: str) -> str:
    """Resolve config exchange name to the local OHLCV exchange folder name.

    Common case: configs use `binance` but PB7 caches are stored under `binanceusdm`.
    """
    exc = (exchange or "").strip()
    if not exc:
        return exc

    # Project convention: Binance OHLCV for futures/perps lives under `binanceusdm`.
    # Do not attempt any other aliasing/mapping.
    if exc == "binance":
        return "binanceusdm"

    if _exchange_has_local_ohlcv(exc, symbol):
        return exc
    return exc


def get_available_coins_v7(exchange: str) -> List[str]:
    if not exchange:
        return []

    coins: set[str] = set()

    # 1) Historical data layout: pb7/historical_data/ohlcvs_<exchange>/<coin>/YYYY-MM-DD.npy
    hist_sym_dir = os.path.join(pb7dir(), "historical_data", f"ohlcvs_{exchange}")
    if os.path.isdir(hist_sym_dir):
        for d in os.listdir(hist_sym_dir):
            p = os.path.join(hist_sym_dir, d)
            if os.path.isdir(p) and not d.startswith("."):
                c = _coin_from_symbol_code(d)
                if c:
                    coins.add(c)

    # 2) CandlestickManager cache layout: pb7/caches/ohlcv/<exchange>/1m/<symbol_code>/YYYY-MM-DD.npy
    cm_sym_dir = os.path.join(pb7dir(), "caches", "ohlcv", exchange, "1m")
    if os.path.isdir(cm_sym_dir):
        for d in os.listdir(cm_sym_dir):
            p = os.path.join(cm_sym_dir, d)
            if os.path.isdir(p) and not d.startswith("."):
                c = _coin_from_symbol_code(d)
                if c:
                    coins.add(c)

    return sorted(coins)


@st.cache_data(show_spinner=False)
def _load_binance_markets_index() -> dict:
    """Build a lookup index for pbgui's local ccxt-style binance markets dump."""
    markets_path = os.path.join(os.path.dirname(__file__), "..", "binance_markets.json")
    markets_path = os.path.abspath(markets_path)
    if not os.path.exists(markets_path):
        return {}
    with open(markets_path, "r", encoding="utf-8") as f:
        markets = json.load(f)

    index: dict[str, dict] = {}
    for sym_key, m in markets.items():
        if not isinstance(m, dict):
            continue
        # direct key (e.g. "DOGE/USDT")
        index[str(sym_key)] = m
        # common identifiers
        for k in ("id", "lowercaseId", "symbol"):
            v = m.get(k)
            if v:
                index[str(v)] = m
    return index


@st.cache_data(show_spinner=False)
def _load_pb7_markets_index(exchange: str) -> dict:
    """Build a lookup index for PB7's ccxt-style markets dump for a given exchange."""
    if not exchange:
        return {}
    markets_path = os.path.join(pb7dir(), "caches", exchange, "markets.json")
    if not os.path.exists(markets_path):
        return {}
    try:
        with open(markets_path, "r", encoding="utf-8") as f:
            markets = json.load(f)
    except Exception:
        return {}
    if not isinstance(markets, dict):
        return {}

    index: dict[str, dict] = {}
    for sym_key, m in markets.items():
        if not isinstance(m, dict):
            continue
        index[str(sym_key)] = m
        for k in ("id", "lowercaseId", "symbol"):
            v = m.get(k)
            if v:
                index[str(v)] = m
    return index
def _match_market_from_index(idx: dict, symbol: str) -> tuple[dict | None, str | None]:
    """Return (market, matched_key) from a markets index for a symbol.

    Mirrors the symbol normalization logic used throughout the visualizer.
    """
    if not idx or not symbol:
        return None, None

    attempted_keys: list[str] = []
    attempted_keys.append(symbol)
    attempted_keys.append(symbol.replace("/", ""))
    attempted_keys.append(symbol.lower())
    if symbol.endswith("USDT") and "/" not in symbol:
        attempted_keys.append(f"{symbol[:-4]}/USDT")
        attempted_keys.append(f"{symbol[:-4]}/USDT:USDT")
    # If symbol looks like a base coin (e.g. "DOGE"), assume USDT perp/quote
    if "/" not in symbol and "_" not in symbol and not symbol.endswith("USDT"):
        attempted_keys.append(f"{symbol}/USDT:USDT")
        attempted_keys.append(f"{symbol}_USDT:USDT")

    seen: set[str] = set()
    for k in attempted_keys:
        if not k or k in seen:
            continue
        seen.add(k)
        m = idx.get(k)
        if isinstance(m, dict):
            return m, k
    return None, None


def _derive_exchange_params_from_market(exchange: str, symbol: str) -> dict:
    """Derive ExchangeParams fields from cached market metadata.

    Returns a dict with keys:
      - price_step, qty_step, min_qty, min_cost, c_mult
    Values are floats or None.
    """
    if not exchange or not symbol:
        return {"price_step": None, "qty_step": None, "min_qty": None, "min_cost": None, "c_mult": None}

    # Prefer PB7 exchange markets.json if available (futures/perps live here, e.g. binanceusdm)
    idx = _load_pb7_markets_index(exchange)
    if not idx and exchange == "binance":
        # spot fallback via pbgui local dump
        idx = _load_binance_markets_index()
    if not idx:
        return {"price_step": None, "qty_step": None, "min_qty": None, "min_cost": None, "c_mult": None}

    m, _ = _match_market_from_index(idx, symbol)
    if not isinstance(m, dict):
        return {"price_step": None, "qty_step": None, "min_qty": None, "min_cost": None, "c_mult": None}

    precision = m.get("precision") or {}
    limits = m.get("limits") or {}
    lim_amount = (limits.get("amount") or {})
    lim_cost = (limits.get("cost") or {})

    price_step = precision.get("price")
    qty_step = precision.get("amount")
    min_qty = lim_amount.get("min")
    min_cost = lim_cost.get("min")
    c_mult = m.get("contractSize")

    return {
        "price_step": float(price_step) if price_step is not None else None,
        "qty_step": float(qty_step) if qty_step is not None else None,
        "min_qty": float(min_qty) if min_qty is not None else None,
        "min_cost": float(min_cost) if min_cost is not None else None,
        "c_mult": float(c_mult) if c_mult is not None else None,
    }


def _derive_exchange_fees_from_market(exchange: str, symbol: str) -> dict:
    """Derive maker/taker fees from cached market metadata.

    Returns a dict with keys:
      - maker_fee, taker_fee
    Values are floats.

    Note: fees are intentionally *not* part of ExchangeParams passed into Rust.
    """
    if not exchange or not symbol:
        return {"maker_fee": 0.0, "taker_fee": 0.0}

    idx = _load_pb7_markets_index(exchange)
    if not idx and exchange == "binance":
        idx = _load_binance_markets_index()
    if not idx:
        return {"maker_fee": 0.0, "taker_fee": 0.0}

    m, _ = _match_market_from_index(idx, symbol)
    if not isinstance(m, dict):
        return {"maker_fee": 0.0, "taker_fee": 0.0}

    maker = m.get("maker_fee")
    if maker is None:
        maker = m.get("maker")
    taker = m.get("taker_fee")
    if taker is None:
        taker = m.get("taker")

    try:
        maker_f = float(maker) if maker is not None else 0.0
    except Exception:
        maker_f = 0.0
    try:
        taker_f = float(taker) if taker is not None else 0.0
    except Exception:
        taker_f = 0.0

    if not math.isfinite(maker_f) or maker_f < 0.0:
        maker_f = 0.0
    if not math.isfinite(taker_f) or taker_f < 0.0:
        taker_f = 0.0

    # PB7 fee overrides (mirrors pb7/src/hlcv_preparation.py)
    if exchange == "bybit":
        maker_f = 0.0002
        taker_f = 0.00055
    elif exchange in ("kucoin", "kucoinfutures"):
        maker_f = 0.0002
        taker_f = 0.0006
    elif exchange == "gateio":
        maker_f = 0.0002
        taker_f = 0.0005

    return {"maker_fee": maker_f, "taker_fee": taker_f}


def _try_autofill_exchange_params(exchange: str, symbol: str, data: "GVData") -> bool:
    """Best-effort auto-fill of ExchangeParams. Returns True if any field updated."""
    if not exchange or not symbol:
        return False

    derived = _derive_exchange_params_from_market(exchange, symbol)
    updated = False

    if derived.get("price_step") is not None:
        data.exchange_params.price_step = float(derived["price_step"])
        updated = True
    if derived.get("qty_step") is not None:
        data.exchange_params.qty_step = float(derived["qty_step"])
        updated = True
    if derived.get("min_qty") is not None:
        data.exchange_params.min_qty = float(derived["min_qty"])
        updated = True
    if derived.get("min_cost") is not None:
        data.exchange_params.min_cost = float(derived["min_cost"])
        updated = True
    if derived.get("c_mult") is not None:
        data.exchange_params.c_mult = float(derived["c_mult"])
        updated = True
    else:
        # spot fallback
        data.exchange_params.c_mult = float(data.exchange_params.c_mult or 1.0)

    return updated

    derived = _derive_exchange_params_from_market(exchange, symbol)
    updated = False

    if derived.get("price_step") is not None:
        data.exchange_params.price_step = float(derived["price_step"])
        updated = True
    if derived.get("qty_step") is not None:
        data.exchange_params.qty_step = float(derived["qty_step"])
        updated = True
    if derived.get("min_qty") is not None:
        data.exchange_params.min_qty = float(derived["min_qty"])
        updated = True
    if derived.get("min_cost") is not None:
        data.exchange_params.min_cost = float(derived["min_cost"])
        updated = True
    if derived.get("c_mult") is not None:
        data.exchange_params.c_mult = float(derived["c_mult"])
        updated = True
    else:
        # spot fallback
        data.exchange_params.c_mult = float(data.exchange_params.c_mult or 1.0)

    return updated


def _market_metadata_source_debug(exchange: str, symbol: str) -> dict:
    """Debug helper to show where market metadata is loaded from and how it is matched.

    Intended to answer: which file/index did we load, which key matched, and which
    fields are used to derive ExchangeParams.
    """
    exchange = (exchange or "").strip()
    symbol = (symbol or "").strip()

    pb7_markets_path = os.path.abspath(os.path.join(pb7dir(), "caches", exchange, "markets.json"))
    binance_markets_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "binance_markets.json"))

    def _file_meta(path: str) -> dict:
        try:
            stt = os.stat(path)
            return {
                "path": path,
                "exists": True,
                "size_bytes": int(stt.st_size),
                "mtime": datetime.datetime.fromtimestamp(stt.st_mtime).isoformat(timespec="seconds"),
            }
        except FileNotFoundError:
            return {"path": path, "exists": False}
        except Exception as e:
            return {"path": path, "exists": bool(os.path.exists(path)), "error": str(e)}

    debug: dict = {
        "exchange": exchange,
        "input_symbol": symbol,
        "pb7_markets": _file_meta(pb7_markets_path),
        "pbgui_binance_markets": _file_meta(binance_markets_path),
        "index_source": None,
        "index_size": 0,
        "attempted_keys": [],
        "matched_key": None,
        "market_snapshot": None,
        "derived": None,
    }

    if not exchange or not symbol:
        return debug

    idx = _load_pb7_markets_index(exchange)
    if idx:
        debug["index_source"] = "pb7"
        debug["index_size"] = int(len(idx))
    elif exchange == "binance":
        idx = _load_binance_markets_index()
        if idx:
            debug["index_source"] = "pbgui-binance"
            debug["index_size"] = int(len(idx))

    if not idx:
        debug["index_source"] = "none"
        return debug

    attempted_keys: list[str] = []
    attempted_keys.append(symbol)
    attempted_keys.append(symbol.replace("/", ""))
    attempted_keys.append(symbol.lower())
    if symbol.endswith("USDT") and "/" not in symbol:
        attempted_keys.append(f"{symbol[:-4]}/USDT")
        attempted_keys.append(f"{symbol[:-4]}/USDT:USDT")
    if "/" not in symbol and "_" not in symbol and not symbol.endswith("USDT"):
        attempted_keys.append(f"{symbol}/USDT:USDT")
        attempted_keys.append(f"{symbol}_USDT:USDT")

    # De-dup while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for k in attempted_keys:
        if k and k not in seen:
            seen.add(k)
            deduped.append(k)
    debug["attempted_keys"] = deduped

    m = None
    matched_key = None
    for k in deduped:
        mm = idx.get(k)
        if isinstance(mm, dict):
            m = mm
            matched_key = k
            break
    debug["matched_key"] = matched_key
    if not isinstance(m, dict):
        return debug

    precision = m.get("precision") or {}
    limits = m.get("limits") or {}
    info = m.get("info") or {}
    price_filter = info.get("priceFilter") or {}
    lot_size_filter = info.get("lotSizeFilter") or {}

    # Keep this snapshot small & JSON-safe.
    debug["market_snapshot"] = {
        "symbol": m.get("symbol"),
        "id": m.get("id"),
        "type": m.get("type"),
        "spot": bool(m.get("spot")),
        "swap": bool(m.get("swap")),
        "future": bool(m.get("future")),
        "linear": bool(m.get("linear")),
        "contractSize": m.get("contractSize"),
        "precision": {
            "price": precision.get("price"),
            "amount": precision.get("amount"),
        },
        "limits": {
            "amount": {"min": (limits.get("amount") or {}).get("min")},
            "cost": {"min": (limits.get("cost") or {}).get("min")},
        },
        "info": {
            "priceFilter.tickSize": price_filter.get("tickSize"),
            "lotSizeFilter.qtyStep": lot_size_filter.get("qtyStep"),
        },
    }

    debug["derived"] = {
        "price_step_from_precision.price": precision.get("price"),
        "qty_step_from_precision.amount": precision.get("amount"),
        "min_qty_from_limits.amount.min": (limits.get("amount") or {}).get("min"),
        "min_cost_from_limits.cost.min": (limits.get("cost") or {}).get("min"),
        "c_mult_from_contractSize": m.get("contractSize"),
    }

    return debug


def _ohlcv_source_debug(exchange: str, coin: str) -> dict:
    """Debug helper to show where OHLCV candles are coming from on disk."""
    exchange = (exchange or "").strip()
    coin = (coin or "").strip()
    root = pb7dir()

    hist_root = os.path.join(root, "historical_data", f"ohlcvs_{exchange}")
    cm_root = os.path.join(root, "caches", "ohlcv", exchange, "1m")

    def _count_npy_files(path: str, limit: int = 5000) -> int:
        # Safety: bound work, we just want a rough indicator.
        if not os.path.isdir(path):
            return 0
        n = 0
        for _, _, files in os.walk(path):
            for fn in files:
                if fn.endswith(".npy"):
                    n += 1
                    if n >= limit:
                        return n
        return n

    def _candidate_dirs(root_dir: str) -> list[str]:
        if not os.path.isdir(root_dir) or not coin:
            return []
        out: list[str] = []
        try:
            for d in os.listdir(root_dir):
                if d.startswith("."):
                    continue
                if _coin_from_symbol_code(d) == coin:
                    out.append(d)
        except Exception:
            return []
        return sorted(out)

    hist_candidates = _candidate_dirs(hist_root)
    cm_candidates = _candidate_dirs(cm_root)

    hist_counts = {d: _count_npy_files(os.path.join(hist_root, d)) for d in hist_candidates[:10]}
    cm_counts = {d: _count_npy_files(os.path.join(cm_root, d)) for d in cm_candidates[:10]}

    return {
        "exchange": exchange,
        "coin": coin,
        "pb7dir": os.path.abspath(root),
        "historical_root": os.path.abspath(hist_root),
        "cm_cache_root": os.path.abspath(cm_root),
        "historical_candidates": hist_candidates,
        "cm_cache_candidates": cm_candidates,
        "historical_npy_counts": hist_counts,
        "cm_cache_npy_counts": cm_counts,
        "merge_priority": "PB7-style: legacy historical_data canonical; cm cache fills gaps",
    }

@st.cache_data
def load_historical_ohlcv_v7(exchange: str, symbol: str) -> pd.DataFrame:
    """Load 1m candles for a coin.

    Supports both PB7 formats:
    - historical_data shards: 2D arrays (N, 6): [ts, o, h, l, c, v]
    - CandlestickManager cache shards: structured arrays with fields (ts,o,h,l,c,bv)

    Merge semantics (PB7 CandlestickManager-style):
    - Legacy downloader shards (`historical_data/`) are canonical where present.
    - Primary CandlestickManager cache (`caches/ohlcv/`) is used to fill legacy gaps.
    - Conflicts are resolved deterministically (stable sort, keep last).
    """

    def _dedupe_sort(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.sort_index(kind="stable")
        return df[~df.index.duplicated(keep="last")]

    def _archive_symbol_code(symbol_like: str) -> str:
        """Best-effort PB7-style archive symbol code (typically BASEQUOTE)."""
        s = str(symbol_like or "").strip()
        if not s:
            return ""

        # ccxt-style: BASE/QUOTE or BASE/QUOTE:SETTLE
        if "/" in s:
            base, rest = s.split("/", 1)
            quote = rest.split(":", 1)[0] if ":" in rest else rest
            base = (base or "").replace("/", "").replace(":", "")
            quote = (quote or "").replace("/", "").replace(":", "")
            return f"{base}{quote}" if quote else base

        # Some PB7 cache dirs use BASE_QUOTE:SETTLE (e.g. HYPE_USDT:USDT)
        if ":" in s or "_" in s:
            left = s.split(":", 1)[0]
            if "_" in left:
                base, quote = left.rsplit("_", 1)
                if base and quote:
                    return f"{base}{quote}"

        # Fallback: already code-like
        return s.replace("/", "").replace(":", "")

    def _df_from_npy(arr: np.ndarray) -> pd.DataFrame | None:
        try:
            if isinstance(arr, np.ndarray) and arr.dtype.names:
                names = set(arr.dtype.names or [])
                ts_k = "ts" if "ts" in names else ("timestamp" if "timestamp" in names else None)
                o_k = "o" if "o" in names else ("open" if "open" in names else None)
                h_k = "h" if "h" in names else ("high" if "high" in names else None)
                l_k = "l" if "l" in names else ("low" if "low" in names else None)
                c_k = "c" if "c" in names else ("close" if "close" in names else None)
                v_k = "bv" if "bv" in names else ("volume" if "volume" in names else None)

                if not all((ts_k, o_k, h_k, l_k, c_k, v_k)):
                    return None

                df = pd.DataFrame(
                    {
                        "timestamp": arr[ts_k].astype("int64"),
                        "open": arr[o_k].astype("float64"),
                        "high": arr[h_k].astype("float64"),
                        "low": arr[l_k].astype("float64"),
                        "close": arr[c_k].astype("float64"),
                        "volume": arr[v_k].astype("float64"),
                    }
                )
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.set_index("timestamp", inplace=True)
                return df

            if isinstance(arr, np.ndarray) and arr.ndim == 2 and arr.shape[1] >= 6:
                # PB7 CandlestickManager converts legacy 2D arrays to float32 fields.
                # Preserve that semantics to avoid drift from float64-quantized candles.
                raw = np.asarray(arr[:, :6], dtype=np.float64)
                ts = raw[:, 0].astype(np.int64)
                ohlcv = raw[:, 1:6].astype(np.float32).astype(np.float64)
                df = pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(ts, unit="ms"),
                        "open": ohlcv[:, 0],
                        "high": ohlcv[:, 1],
                        "low": ohlcv[:, 2],
                        "close": ohlcv[:, 3],
                        "volume": ohlcv[:, 4],
                    }
                )
                df.set_index("timestamp", inplace=True)
                return df
        except Exception:
            return None

        return None

    # Normalize symbol codes coming from configs/UI.
    # Example: config may contain "DOGEUSDT" while PB7 caches use "DOGE_USDT:USDT".
    sym_raw = str(symbol or "").strip()
    coin_base = _coin_from_symbol_code(sym_raw)
    coin_candidates = [c for c in [sym_raw, coin_base] if c]

    # Best-effort hint for deriving legacy sym_code dirs (e.g. HYPEUSDT).
    # We may only get base coin from callers, so we also try to infer from the
    # selected CandlestickManager cache dir name later.
    sym_code_hint = sym_raw

    # Only allow loose prefix matching (e.g., DOGE -> DOGEUSDT) when the user
    # provided a non-base symbol (sym_raw != coin_base). If sym_raw already is a
    # base coin (e.g. "HYPE"), do NOT match unrelated prefixes like "HYPEUSDT".
    allow_prefix_match = bool(sym_raw and coin_base and sym_raw != coin_base)

    def _dir_matches_coin(d: str, coin: str) -> bool:
        if not d or not coin:
            return False
        if d == coin:
            return True
        # common PB7 symbol folder styles
        if d.startswith(f"{coin}_"):
            return True
        if d.startswith(f"{coin}:"):
            return True
        if d.startswith(f"{coin}/"):
            return True
        if d.startswith(f"{coin}-"):
            return True
        # allow direct prefix match (e.g. DOGE matches DOGEUSDT) only when requested
        # symbol includes quote/suffix (sym_raw != coin_base)
        if allow_prefix_match and d.startswith(coin):
            return True
        return False

    dfs_cm: list[pd.DataFrame] = []
    dfs_hist: list[pd.DataFrame] = []

    # 1) CandlestickManager cache: pb7/caches/ohlcv/<exchange>/1m/<symbol_code>/YYYY-MM-DD.npy
    cm_base = os.path.join(pb7dir(), "caches", "ohlcv", exchange, "1m")
    if os.path.isdir(cm_base):
        candidates: list[str] = []
        try:
            for d in os.listdir(cm_base):
                for c in coin_candidates:
                    if _dir_matches_coin(d, c):
                        candidates.append(d)
                        break
        except Exception:
            candidates = []

        # pick the candidate dir with the most .npy shards (proxy for most history)
        target_dir = None
        if candidates:
            best = None
            best_count = -1
            for cdir in candidates:
                pdir = os.path.join(cm_base, cdir)
                if not os.path.isdir(pdir):
                    continue
                try:
                    n_shards = sum(1 for f in os.listdir(pdir) if f.endswith(".npy") and not f.startswith("."))
                except Exception:
                    n_shards = 0
                if n_shards > best_count:
                    best = cdir
                    best_count = n_shards
                elif n_shards == best_count and best is not None:
                    # tie-break: prefer USDT-ish dirs
                    if ("USDT" in cdir) and ("USDT" not in str(best)):
                        best = cdir

            if best is None:
                best = candidates[0]
            sym_code_hint = str(best)
            target_dir = os.path.join(cm_base, best)

        if target_dir and os.path.isdir(target_dir):
            try:
                shard_files = sorted([f for f in os.listdir(target_dir) if f.endswith(".npy") and not f.startswith(".")])
            except Exception:
                shard_files = []
            for f in shard_files:
                p = os.path.join(target_dir, f)
                try:
                    arr = np.load(p)
                except Exception:
                    continue
                df_shard = _df_from_npy(arr)
                if df_shard is not None and not df_shard.empty:
                    dfs_cm.append(df_shard)

    # 2) Historical data: legacy downloader shards (PB7 CandlestickManager-style)
    # - Primary legacy dir: pb7/historical_data/ohlcvs_<exchange>/<coin>/YYYY-MM-DD.npy
    # - Extra legacy dirs for some exchanges (e.g. bybit sym_code, binanceusdm futures sym_code)
    legacy_dir_candidates: list[str] = []
    try:
        coin_dir = os.path.join(pb7dir(), "historical_data", f"ohlcvs_{exchange}", coin_base)
        if coin_base and os.path.isdir(coin_dir):
            legacy_dir_candidates.append(coin_dir)
    except Exception:
        pass

    try:
        sym_code = _archive_symbol_code(sym_code_hint)
    except Exception:
        sym_code = ""

    if str(exchange).lower() == "bybit" and sym_code and sym_code != coin_base:
        try:
            bybit_sym_dir = os.path.join(pb7dir(), "historical_data", "ohlcvs_bybit", sym_code)
            if os.path.isdir(bybit_sym_dir):
                legacy_dir_candidates.append(bybit_sym_dir)
        except Exception:
            pass

    if str(exchange).lower() == "binanceusdm" and sym_code:
        try:
            futures_sym_dir = os.path.join(pb7dir(), "historical_data", "ohlcvs_futures", sym_code)
            if os.path.isdir(futures_sym_dir):
                legacy_dir_candidates.append(futures_sym_dir)
        except Exception:
            pass

    # Build date_key -> shard path mapping, preferring earlier dirs (PB7 uses setdefault).
    legacy_shards: dict[str, str] = {}
    for d in legacy_dir_candidates:
        try:
            for p in sorted([f for f in os.listdir(d) if f.endswith(".npy") and not f.startswith(".")]):
                date_key = os.path.splitext(p)[0]
                if len(date_key) == 10 and date_key[4] == "-" and date_key[7] == "-":
                    legacy_shards.setdefault(date_key, os.path.join(d, p))
        except Exception:
            continue

    for date_key, path in sorted(legacy_shards.items()):
        try:
            arr = np.load(path)
        except Exception:
            continue
        df_shard = _df_from_npy(arr)
        if df_shard is not None and not df_shard.empty:
            dfs_hist.append(df_shard)

    primary_df = _dedupe_sort(pd.concat(dfs_cm)) if dfs_cm else pd.DataFrame()
    legacy_df = _dedupe_sort(pd.concat(dfs_hist)) if dfs_hist else pd.DataFrame()

    if legacy_df.empty and primary_df.empty:
        return pd.DataFrame()
    if legacy_df.empty:
        return primary_df
    if primary_df.empty:
        return legacy_df

    # Legacy canonical; primary fills gaps.
    full_df = legacy_df.combine_first(primary_df)
    return _dedupe_sort(full_df)

@st.cache_data(show_spinner=False)
def calculate_v7_indicators(df: pd.DataFrame, ema0: float, ema1: float, vol_span_hours: float):
    # Volatility (PB7 semantics): log-range on 1h candles, EWM span in hours.
    # State param name is `entry_volatility_logrange_ema_1h`.
    df = df.copy()  # Avoid modifying cached df

    try:
        # Use fully completed 1h candles ending on the hour.
        ohlc_1h = df[["open", "high", "low", "close"]].resample("1h", label="right", closed="right").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        )
        ohlc_1h.dropna(subset=["open", "high", "low", "close"], inplace=True)
        log_ratio_1h = np.log(ohlc_1h["high"] / ohlc_1h["low"])
        vol_span_h = max(1.0, float(vol_span_hours) if vol_span_hours is not None else 1.0)
        # PB7 uses bias-adjusted EMA (see `update_adjusted_ema` in rust), matching pandas `adjust=True`.
        vol_1h = log_ratio_1h.ewm(span=vol_span_h, adjust=True).mean()
        # Forward-fill the last completed 1h value to 1m timestamps.
        df["volatility"] = vol_1h.reindex(df.index, method="ffill").fillna(0.0)
    except Exception:
        # Fallback: compute on 1m candles (older behavior)
        df["log_ratio"] = np.log(df["high"] / df["low"])
        vol_span_min = max(1.0, float(vol_span_hours or 0.0) * 60.0)
        df["volatility"] = df["log_ratio"].ewm(span=vol_span_min, adjust=True).mean()
    
    # EMAs
    # PB7 uses three EMA spans: ema_span_0, ema_span_1, and sqrt(ema_span_0*ema_span_1),
    # and a bias-adjusted EMA update (equivalent to pandas `adjust=True`).
    # Enforce minimum span of 1.0 to avoid ValueError.
    e0 = max(1.0, ema0)
    e1 = max(1.0, ema1)
    e2 = max(1.0, float(e0 * e1) ** 0.5)
    df["ema_0"] = df["close"].ewm(span=e0, adjust=True).mean()
    df["ema_1"] = df["close"].ewm(span=e1, adjust=True).mean()
    df["ema_2"] = df["close"].ewm(span=e2, adjust=True).mean()
    
    return df


def _any_trailing_enabled_for_backtest(bp: BotParams) -> bool:
    """Match PB7 backtest `TrailingEnabled` logic.

    PB7 updates trailing bundle only if either trailing_grid_ratio is non-zero.
    """
    try:
        return float(getattr(bp, "close_trailing_grid_ratio", 0.0) or 0.0) != 0.0 or float(
            getattr(bp, "entry_trailing_grid_ratio", 0.0) or 0.0
        ) != 0.0
    except Exception:
        return False


def _bot_params_dict_for_orchestrator_single_symbol(bp: BotParams, *, enabled: bool) -> dict:
    """Build a PB7-compatible BotParams dict for orchestrator JSON API.

    Ensures `wallet_exposure_limit` is populated and resolved for the 1-symbol case.
    """
    d = asdict(bp)
    d.setdefault("wallet_exposure_limit", -1.0)
    if not enabled:
        d["n_positions"] = 0
        d["total_wallet_exposure_limit"] = 0.0
        d["wallet_exposure_limit"] = 0.0
        return d

    try:
        wel = float(d.get("wallet_exposure_limit") if d.get("wallet_exposure_limit") is not None else -1.0)
    except Exception:
        wel = -1.0

    if wel < 0.0:
        try:
            npos = int(d.get("n_positions") or 0)
        except Exception:
            npos = 0
        try:
            total = float(d.get("total_wallet_exposure_limit") or 0.0)
        except Exception:
            total = 0.0
        # For a single tradable symbol, PB7 dynamic WEL resolves to total_wel / effective_n_positions.
        enp = min(max(0, npos), 1) if npos else 0
        d["wallet_exposure_limit"] = float(total / float(enp)) if enp else 0.0
    return d


def _prepare_orchestrator_ema_df(
    candles: pd.DataFrame,
    *,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
) -> pd.DataFrame:
    """Compute EMA series needed by PB7 orchestrator input.

    Matches PB7 backtest EMA semantics in `passivbot-rust/src/backtest.rs`:
    - price/volume/log-range EMAs: bias-adjusted EMA update (`update_adjusted_ema`), equivalent to pandas `ewm(adjust=True)`.
      PB7 maintains numerator/denominator state and returns `num/den`.
    - 1h entry-volatility log-range EMA: bias-adjusted EMA updated on hour boundaries using the *previous hour* bucket.

    Returns a DataFrame aligned to `candles.index`.
    """
    if candles is None or candles.empty:
        return pd.DataFrame()
    d = candles.copy()
    for col in ("open", "high", "low", "close"):
        if col not in d.columns:
            return pd.DataFrame()

    def _alpha_from_span(span: float) -> float:
        try:
            s = float(span)
        except Exception:
            s = 1.0
        if not np.isfinite(s) or s <= 0.0:
            s = 1.0
        return float(2.0 / (s + 1.0))

    def _adjusted_ema_skip_nan(values: np.ndarray, *, alpha: float, init_num: float, init_den: float) -> np.ndarray:
        """PB7 `update_adjusted_ema` semantics, skipping non-finite values (keep previous num/den)."""
        out_arr = np.empty(values.shape[0], dtype=np.float64)
        a = float(alpha)
        num = float(init_num)
        den = float(init_den)
        one_minus = float(1.0 - a) if (math.isfinite(a) and a > 0.0) else 1.0
        den_min_pos = float(getattr(sys, "float_info", None).min) if getattr(sys, "float_info", None) else 2.2250738585072014e-308

        for i, v in enumerate(values):
            if not (math.isfinite(a) and a > 0.0):
                out_arr[i] = (num / den) if (den > 0.0 and math.isfinite(den) and math.isfinite(num)) else float(v)
                continue

            if not np.isfinite(v):
                out_arr[i] = (num / den) if (den > 0.0 and math.isfinite(den) and math.isfinite(num)) else float(v)
                continue

            new_num = a * float(v) + one_minus * num
            new_den = a + one_minus * den
            if (not math.isfinite(new_den)) or new_den <= den_min_pos:
                num = a * float(v)
                den = a
                out_arr[i] = float(v)
                continue
            num = float(new_num)
            den = float(new_den)
            out_arr[i] = num / den
        return out_arr

    close = d["close"].astype("float64").to_numpy(copy=False)
    high = d["high"].astype("float64").to_numpy(copy=False)
    low = d["low"].astype("float64").to_numpy(copy=False)
    vol_raw = (
        d["volume"].astype("float64").to_numpy(copy=False)
        if "volume" in d.columns
        else np.zeros_like(close, dtype=np.float64)
    )

    # PB7 stores 1m OHLCV as float32 in numpy and reads as f64 in Rust.
    # Quantize inputs here to reduce strict-threshold divergences.
    try:
        close = close.astype(np.float32).astype(np.float64)
        high = high.astype(np.float32).astype(np.float64)
        low = low.astype(np.float32).astype(np.float64)
        vol_raw = vol_raw.astype(np.float32).astype(np.float64)
    except Exception:
        pass

    # PB7 update_emas() skips all EMA updates if close/high/low are not finite.
    update_ok = np.isfinite(close) & np.isfinite(high) & np.isfinite(low)

    # Init index = first valid candle (matches PB7 first_valid_idx semantics best-effort).
    if update_ok.any():
        first_ok = int(np.argmax(update_ok))
    else:
        first_ok = 0
    base_close = float(close[first_ok]) if np.isfinite(close[first_ok]) else 0.0
    base_vol = float(max(0.0, vol_raw[first_ok])) if np.isfinite(vol_raw[first_ok]) else 0.0
    hi0 = float(high[first_ok]) if np.isfinite(high[first_ok]) else float("nan")
    lo0 = float(low[first_ok]) if np.isfinite(low[first_ok]) else float("nan")
    if np.isfinite(hi0) and np.isfinite(lo0) and base_close > 0.0:
        typical0 = (hi0 + lo0 + base_close) / 3.0
    else:
        typical0 = max(base_close, 1.0)
    base_quote_vol = float(base_vol * typical0)

    # Prepare update streams (NaN => skip update, keep previous)
    close_upd = np.where(update_ok, close, np.nan)
    vol_base = np.where(np.isfinite(vol_raw), np.maximum(0.0, vol_raw), 0.0)
    typical = (high + low + close) / 3.0
    # If typical is non-finite, fall back to close (PB7 falls back more aggressively only for init)
    typical = np.where(np.isfinite(typical), typical, np.where(np.isfinite(close), close, 0.0))
    quote_vol = vol_base * typical
    quote_vol_upd = np.where(update_ok, quote_vol, np.nan)
    log_range_vals = np.where(
        (high > 0.0) & (low > 0.0) & np.isfinite(high) & np.isfinite(low),
        np.log(high / low),
        0.0,
    )
    log_range_upd = np.where(update_ok, log_range_vals, np.nan)

    # 1m close EMAs (3 spans) per pside
    e0l = float(getattr(bot_params_long, "ema_span_0", 1.0) or 1.0)
    e1l = float(getattr(bot_params_long, "ema_span_1", 1.0) or 1.0)
    e2l = float(max(1.0, float(e0l * e1l) ** 0.5))
    e0s = float(getattr(bot_params_short, "ema_span_0", 1.0) or 1.0)
    e1s = float(getattr(bot_params_short, "ema_span_1", 1.0) or 1.0)
    e2s = float(max(1.0, float(e0s * e1s) ** 0.5))

    out = pd.DataFrame(index=d.index)
    # PB7 initializes adjusted EMA state as: numerator=base_value, denominator=1.0.
    out["ema_l0"] = _adjusted_ema_skip_nan(close_upd, alpha=_alpha_from_span(e0l), init_num=base_close, init_den=1.0)
    out["ema_l1"] = _adjusted_ema_skip_nan(close_upd, alpha=_alpha_from_span(e1l), init_num=base_close, init_den=1.0)
    out["ema_l2"] = _adjusted_ema_skip_nan(close_upd, alpha=_alpha_from_span(e2l), init_num=base_close, init_den=1.0)
    out["ema_s0"] = _adjusted_ema_skip_nan(close_upd, alpha=_alpha_from_span(e0s), init_num=base_close, init_den=1.0)
    out["ema_s1"] = _adjusted_ema_skip_nan(close_upd, alpha=_alpha_from_span(e1s), init_num=base_close, init_den=1.0)
    out["ema_s2"] = _adjusted_ema_skip_nan(close_upd, alpha=_alpha_from_span(e2s), init_num=base_close, init_den=1.0)

    # 1m volume/log-range EMAs (forager)
    span_vol_l = float(getattr(bot_params_long, "filter_volume_ema_span", 1.0) or 1.0)
    span_vol_s = float(getattr(bot_params_short, "filter_volume_ema_span", 1.0) or 1.0)
    out["vol_ema_l"] = _adjusted_ema_skip_nan(quote_vol_upd, alpha=_alpha_from_span(span_vol_l), init_num=base_quote_vol, init_den=1.0)
    out["vol_ema_s"] = _adjusted_ema_skip_nan(quote_vol_upd, alpha=_alpha_from_span(span_vol_s), init_num=base_quote_vol, init_den=1.0)

    span_lr_l = float(getattr(bot_params_long, "filter_volatility_ema_span", 1.0) or 1.0)
    span_lr_s = float(getattr(bot_params_short, "filter_volatility_ema_span", 1.0) or 1.0)
    out["lr_ema_l"] = _adjusted_ema_skip_nan(log_range_upd, alpha=_alpha_from_span(span_lr_l), init_num=0.0, init_den=1.0)
    out["lr_ema_s"] = _adjusted_ema_skip_nan(log_range_upd, alpha=_alpha_from_span(span_lr_s), init_num=0.0, init_den=1.0)

    # 1h log-range EMA for entry volatility (updated on hour boundary using previous-hour bucket)
    def _entry_volatility_h1_series(span_hours: float) -> np.ndarray:
        try:
            span_h = float(span_hours or 0.0)
        except Exception:
            span_h = 0.0
        if span_h <= 0.0 or not np.isfinite(span_h):
            return np.zeros_like(close, dtype=np.float64)

        a = float(2.0 / (span_h + 1.0))
        one_minus = 1.0 - a
        out_arr = np.zeros_like(close, dtype=np.float64)
        num = 0.0
        den = 1.0
        den_min_pos = float(getattr(sys, "float_info", None).min) if getattr(sys, "float_info", None) else 2.2250738585072014e-308

        # timestamps aligned to epoch hours (PB7 uses absolute ms)
        try:
            ts_ms = (d.index.view("int64") // 1_000_000).astype(np.int64)
        except Exception:
            return out_arr
        hour_ms = (ts_ms // 3_600_000) * 3_600_000

        last_boundary = int(hour_ms[0]) if hour_ms.size else 0
        window_start = 0

        for i in range(ts_ms.size):
            hb = int(hour_ms[i])
            if hb > last_boundary:
                # update using previous window [window_start, i-1]
                end = i - 1
                if end >= window_start:
                    hh = high[window_start : end + 1]
                    ll = low[window_start : end + 1]
                    mask = np.isfinite(hh) & np.isfinite(ll)
                    if mask.any():
                        hmax = float(np.max(hh[mask]))
                        lmin = float(np.min(ll[mask]))
                        if hmax > 0.0 and lmin > 0.0 and np.isfinite(hmax) and np.isfinite(lmin):
                            lr = float(np.log(hmax / lmin))
                            new_num = a * lr + one_minus * num
                            new_den = a + one_minus * den
                            if (not math.isfinite(new_den)) or new_den <= den_min_pos:
                                num = a * lr
                                den = a
                            else:
                                num = float(new_num)
                                den = float(new_den)
                last_boundary = hb
                window_start = i
            out_arr[i] = (num / den) if (den > 0.0 and math.isfinite(den) and math.isfinite(num)) else 0.0
        return out_arr

    out["h1_lr_ema_l"] = _entry_volatility_h1_series(
        float(getattr(bot_params_long, "entry_volatility_ema_span_hours", 0.0) or 0.0)
    )
    out["h1_lr_ema_s"] = _entry_volatility_h1_series(
        float(getattr(bot_params_short, "entry_volatility_ema_span_hours", 0.0) or 0.0)
    )

    return out


def _historical_dict_to_df(hist: Any) -> pd.DataFrame:
    """Convert `data.historical_candles` to a DataFrame indexed by timestamp."""
    if hist is None:
        return pd.DataFrame()
    if isinstance(hist, pd.DataFrame):
        df = hist.copy()
    elif isinstance(hist, dict):
        try:
            df = pd.DataFrame(hist)
        except Exception:
            return pd.DataFrame()
    else:
        return pd.DataFrame()

    if df.empty:
        return df

    if "timestamp" in df.columns:
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df.dropna(subset=["timestamp"], inplace=True)
            df.set_index("timestamp", inplace=True)
        except Exception:
            pass
    return df


def _standardize_ohlcv_1m_gaps(
    df: pd.DataFrame,
    *,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """PB7 CandlestickManager-style gap standardization for 1m candles.

    - Synthesizes missing minutes as flat candles: o=h=l=c=prev_close, volume=0.
    - Does NOT fill leading gaps before first real candle.
    - Operates on the provided [start_ts, end_ts] window (inclusive), floored to minutes.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    for col in ("open", "high", "low", "close"):
        if col not in df.columns:
            return df

    d = df.sort_index(kind="stable")
    d = d[~d.index.duplicated(keep="last")]

    first_real = d.index.min()
    if pd.isna(first_real):
        return d

    if start_ts is None:
        start_ts = first_real
    if end_ts is None:
        end_ts = d.index.max()
    if pd.isna(end_ts):
        return d

    start_ts = pd.Timestamp(start_ts).floor("min")
    end_ts = pd.Timestamp(end_ts).floor("min")
    if end_ts < start_ts:
        return d

    # Do not create synthetic candles before first real data point.
    effective_start = max(start_ts, pd.Timestamp(first_real).floor("min"))
    if end_ts < effective_start:
        # Requested window ends before first data.
        return d.loc[effective_start:effective_start].iloc[:0]

    full_index = pd.date_range(effective_start, end_ts, freq="1min")

    # Seed close from the last candle strictly before effective_start if the first minute is missing.
    prev_close = None
    try:
        prev_rows = d.loc[: effective_start - pd.Timedelta(minutes=1)]
        if not prev_rows.empty:
            prev_close = float(prev_rows["close"].iloc[-1])
    except Exception:
        prev_close = None

    window = d.loc[:end_ts].reindex(full_index)
    close = window["close"].astype("float64")
    if prev_close is not None and (close.empty or pd.isna(close.iloc[0])):
        close.iloc[0] = float(prev_close)
    close = close.ffill()

    # Where the original row was missing, synthesize OHLC from prev_close.
    missing = window["close"].isna()
    out = window.copy()
    out.loc[missing, "open"] = close.loc[missing]
    out.loc[missing, "high"] = close.loc[missing]
    out.loc[missing, "low"] = close.loc[missing]
    out.loc[missing, "close"] = close.loc[missing]
    if "volume" in out.columns:
        out.loc[out["volume"].isna(), "volume"] = 0.0
    return out


def _find_1m_gaps(
    df: pd.DataFrame,
    *,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> dict:
    """Detect missing 1-minute candles in [start_ts, end_ts] (inclusive).

    Returns dict with:
      - has_gaps: bool
      - missing_count: int
      - first_missing: pd.Timestamp|None
      - last_missing: pd.Timestamp|None
      - sample_missing: list[pd.Timestamp] (up to 20)
    """
    try:
        if df is None or df.empty:
            return {
                "has_gaps": True,
                "missing_count": -1,
                "first_missing": None,
                "last_missing": None,
                "sample_missing": [],
            }

        idx = pd.to_datetime(df.index)
        if idx.tz is not None:
            idx = idx.tz_localize(None)

        s = pd.Timestamp(start_ts).floor("min")
        e = pd.Timestamp(end_ts).floor("min")
        if e < s:
            return {"has_gaps": False, "missing_count": 0, "first_missing": None, "last_missing": None, "sample_missing": []}

        # Restrict to window.
        present = pd.DatetimeIndex(idx[(idx >= s) & (idx <= e)]).floor("min")
        present = present.drop_duplicates().sort_values()
        full = pd.date_range(s, e, freq="1min")
        missing = full.difference(present)
        missing_count = int(len(missing))
        return {
            "has_gaps": missing_count > 0,
            "missing_count": missing_count,
            "first_missing": (missing[0] if missing_count else None),
            "last_missing": (missing[-1] if missing_count else None),
            "sample_missing": list(missing[:20]),
        }
    except Exception:
        return {
            "has_gaps": True,
            "missing_count": -1,
            "first_missing": None,
            "last_missing": None,
            "sample_missing": [],
        }


def _reset_trailing_bundle(tb: TrailingPriceBundle, price: float) -> TrailingPriceBundle:
    p = float(price)
    tb.min_since_open = p
    tb.max_since_min = p
    tb.max_since_open = p
    tb.min_since_max = p
    return tb


def _update_trailing_bundle_from_price_long(tb: TrailingPriceBundle, price: float) -> TrailingPriceBundle:
    p = float(price)
    if p < float(tb.min_since_open):
        tb.min_since_open = p
        tb.max_since_min = p
    else:
        tb.max_since_min = float(max(float(tb.max_since_min), p))

    if p > float(tb.max_since_open):
        tb.max_since_open = p
        tb.min_since_max = p
    else:
        tb.min_since_max = float(min(float(tb.min_since_max), p))
    return tb


def _infer_maker_taker_fees(exchange: str, coin: str) -> tuple[float, float]:
    """Best-effort fee inference for Mode C (PB7 backtest engine).

    PB7 backtest uses `backtest_params.maker_fee` as the actual fee rate; we still
    populate both maker/taker in bundle metadata for completeness.
    """
    # Mirror PB7's `MarketManager.get_market_specific_settings` overrides.
    if str(exchange) == "bybit":
        return 0.0002, 0.00055
    if str(exchange) in ("kucoin", "kucoinfutures"):
        return 0.0002, 0.0006
    if str(exchange) == "gateio":
        return 0.0002, 0.0005

    try:
        idx = _load_pb7_markets_index(exchange)
        m, _ = _match_market_from_index(idx, coin)
        if isinstance(m, dict):
            maker = m.get("maker")
            if maker is None:
                maker = m.get("maker_fee")
            taker = m.get("taker")
            if taker is None:
                taker = m.get("taker_fee")
            maker_f = float(maker) if maker is not None else 0.0
            taker_f = float(taker) if taker is not None else maker_f
            return maker_f, taker_f
    except Exception:
        pass
    return 0.0, 0.0


def _compute_warmup_minutes_for_mode_c(
    bp_long: BotParams,
    bp_short: BotParams,
    *,
    warmup_ratio: float = 0.2,
    max_warmup_minutes: float = 0.0,
) -> int:
    """Compute warmup minutes consistent with PB7's `compute_backtest_warmup_minutes` logic.

    We keep this local to avoid importing PB7 modules just for warmup.
    """

    def _as_minutes(bp: BotParams) -> float:
        candidates = [
            float(getattr(bp, "ema_span_0", 0.0) or 0.0),
            float(getattr(bp, "ema_span_1", 0.0) or 0.0),
            float(getattr(bp, "filter_volume_ema_span", 0.0) or 0.0),
            float(getattr(bp, "filter_volatility_ema_span", 0.0) or 0.0),
            float(getattr(bp, "entry_volatility_ema_span_hours", 0.0) or 0.0) * 60.0,
        ]
        out = 0.0
        for v in candidates:
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                out = max(out, float(v))
        return out

    max_minutes = max(_as_minutes(bp_long), _as_minutes(bp_short))
    if not math.isfinite(max_minutes) or max_minutes <= 0.0:
        return 0
    ratio = float(warmup_ratio) if math.isfinite(float(warmup_ratio)) else 1.0
    ratio = max(0.0, ratio)
    warm = max_minutes * ratio
    if max_warmup_minutes and float(max_warmup_minutes) > 0.0 and math.isfinite(float(max_warmup_minutes)):
        warm = min(warm, float(max_warmup_minutes))
    return int(math.ceil(warm)) if warm > 0.0 else 0


def _compute_warmup_minutes_for_mode_c_from_config(
    config: dict,
    bp_long: BotParams,
    bp_short: BotParams,
) -> int:
    """Compute warmup minutes matching PB7 `compute_backtest_warmup_minutes`.

    Key detail: PB7 also considers `optimize.bounds` maxima if present.
    """

    def _to_float(v) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    def _extract_bound_max(bounds: dict, key: str) -> float:
        if key not in bounds:
            return 0.0
        entry = bounds[key]
        candidates = [entry] if isinstance(entry, (list, tuple)) else [[entry]]
        max_val = 0.0
        for candidate in candidates:
            for val in candidate:
                max_val = max(max_val, _to_float(val))
        return max_val

    max_minutes = 0.0
    for bp in (bp_long, bp_short):
        max_minutes = max(max_minutes, _to_float(getattr(bp, "ema_span_0", 0.0)))
        max_minutes = max(max_minutes, _to_float(getattr(bp, "ema_span_1", 0.0)))
        max_minutes = max(max_minutes, _to_float(getattr(bp, "filter_volume_ema_span", 0.0)))
        max_minutes = max(max_minutes, _to_float(getattr(bp, "filter_volatility_ema_span", 0.0)))
        max_minutes = max(
            max_minutes,
            _to_float(getattr(bp, "entry_volatility_ema_span_hours", 0.0)) * 60.0,
        )

    bounds = (config.get("optimize") or {}).get("bounds") or {}
    bound_keys_minutes = [
        "long_ema_span_0",
        "long_ema_span_1",
        "long_filter_volume_ema_span",
        "long_filter_volatility_ema_span",
        "short_ema_span_0",
        "short_ema_span_1",
        "short_filter_volume_ema_span",
        "short_filter_volatility_ema_span",
    ]
    bound_keys_hours = [
        "long_entry_volatility_ema_span_hours",
        "short_entry_volatility_ema_span_hours",
    ]
    for key in bound_keys_minutes:
        max_minutes = max(max_minutes, _extract_bound_max(bounds, key))
    for key in bound_keys_hours:
        max_minutes = max(max_minutes, _extract_bound_max(bounds, key) * 60.0)

    # PB7 backtests define warmup settings under `backtest`.
    # Keep a fallback to `live` for older configs.
    backtest_cfg = config.get("backtest") or {}
    live_cfg = config.get("live") or {}

    warmup_ratio = _to_float(backtest_cfg.get("warmup_ratio"))
    if warmup_ratio <= 0.0:
        warmup_ratio = _to_float(live_cfg.get("warmup_ratio"))
    if warmup_ratio <= 0.0:
        warmup_ratio = 0.2

    limit = _to_float(backtest_cfg.get("max_warmup_minutes"))
    if limit <= 0.0:
        limit = _to_float(live_cfg.get("max_warmup_minutes"))

    if not math.isfinite(max_minutes):
        return 0
    warmup_minutes = max_minutes * max(0.0, warmup_ratio)
    if limit > 0.0:
        warmup_minutes = min(warmup_minutes, limit)
    return int(math.ceil(warmup_minutes)) if warmup_minutes > 0.0 else 0


def _pb7_fills_to_events(fills_array: Any) -> tuple[list[dict], list[dict]]:
    """Normalize PB7 backtest fills array into visualizer historical event rows.

    Returns: (events_long, events_short)
    """
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
    # Some PB7 versions append extra columns (e.g. minute, btc_total_balance).
    cols_extended = cols_base + [
        "minute",
        "btc_total_balance",
    ]
    try:
        arr = np.asarray(fills_array, dtype=object)
        if arr.size == 0:
            return [], []
        if arr.ndim != 2 or arr.shape[1] < len(cols_base):
            return [], []
        # Accept extra trailing columns by slicing/mapping.
        if arr.shape[1] >= len(cols_extended):
            df = pd.DataFrame(arr[:, : len(cols_extended)], columns=cols_extended)
        else:
            df = pd.DataFrame(arr[:, : len(cols_base)], columns=cols_base)
    except Exception:
        return [], []

    try:
        # Keep wall-time equal to UTC and drop timezone info.
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"].astype("int64"), unit="ms", utc=True).dt.tz_localize(None)
    except Exception:
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", errors="coerce")

    events_long: list[dict] = []
    events_short: list[dict] = []

    for _, r in df.iterrows():
        ot = str(r.get("order_type") or "")
        ev_type = "fill"
        if ot.startswith("entry"):
            ev_type = "entry"
        elif ot.startswith("close"):
            ev_type = "close"

        event = {
            "timestamp": r.get("timestamp"),
            "event": ev_type,
            "qty": float(r.get("fill_qty") or 0.0),
            "price": float(r.get("fill_price") or 0.0),
            "order_type": ot,
            "wallet_balance": float(r.get("usd_total_balance") or 0.0),
            "pos_size": float(r.get("position_size") or 0.0),
            "pos_price": float(r.get("position_price") or 0.0),
            "pnl": float(r.get("pnl") or 0.0),
            "fee_paid": float(r.get("fee_paid") or 0.0),
            "wallet_exposure": float(r.get("wallet_exposure") or 0.0),
        }

        if "_short" in ot:
            events_short.append(event)
        elif "_long" in ot:
            events_long.append(event)
        else:
            # If unknown, infer from position sign
            try:
                if float(r.get("position_size") or 0.0) < 0.0:
                    events_short.append(event)
                else:
                    events_long.append(event)
            except Exception:
                events_long.append(event)

    return events_long, events_short


def _load_pb7_fills_csv_to_events(backtest_dir: str) -> tuple[list[dict], list[dict]]:
    """Load PB7 backtest `fills.csv` (or `.csv.gz`) from a backtest directory and normalize to event dicts."""

    if not backtest_dir:
        return [], []

    try:
        backtest_dir = os.path.expanduser(str(backtest_dir))
    except Exception:
        backtest_dir = str(backtest_dir)

    candidates = [
        os.path.join(backtest_dir, "fills.csv"),
        os.path.join(backtest_dir, "fills.csv.gz"),
    ]
    fills_path = next((p for p in candidates if os.path.isfile(p)), "")
    if not fills_path:
        return [], []

    try:
        df = pd.read_csv(fills_path)
    except Exception:
        return [], []

    # Drop index columns (PB7 commonly writes an unnamed index column)
    try:
        df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].copy()
    except Exception:
        pass

    # Expected columns: timestamp, qty, price, psize, pprice, type, usd_total_balance, pnl, fee_paid, wallet_exposure
    if "timestamp" not in df.columns or "type" not in df.columns:
        return [], []

    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    except Exception:
        df["timestamp"] = pd.NaT

    events_long: list[dict] = []
    events_short: list[dict] = []

    for _, r in df.iterrows():
        ot = str(r.get("type") or "")
        if not ot:
            continue

        ts = r.get("timestamp")
        if pd.isna(ts):
            continue

        ev_type = "fill"
        if ot.startswith("entry"):
            ev_type = "entry"
        elif ot.startswith("close"):
            ev_type = "close"

        event = {
            "timestamp": ts,
            "event": ev_type,
            "qty": float(r.get("qty") or 0.0),
            "price": float(r.get("price") or 0.0),
            "order_type": ot,
            "wallet_balance": float(r.get("usd_total_balance") or 0.0),
            "pos_size": float(r.get("psize") or 0.0),
            "pos_price": float(r.get("pprice") or 0.0),
            "pnl": float(r.get("pnl") or 0.0),
            "fee_paid": float(r.get("fee_paid") or 0.0),
            "wallet_exposure": float(r.get("wallet_exposure") or 0.0),
        }

        if "_short" in ot:
            events_short.append(event)
        elif "_long" in ot:
            events_long.append(event)
        else:
            # If unknown, infer from position sign
            try:
                if float(r.get("psize") or 0.0) < 0.0:
                    events_short.append(event)
                else:
                    events_long.append(event)
            except Exception:
                events_long.append(event)

    return events_long, events_short


def _compare_fills_pb7_b_c(
    *,
    pb7_events: list[dict],
    b_events: list[dict],
    c_events: list[dict],
    price_step: float,
    qty_step: float,
) -> pd.DataFrame:
    def _to_ms(x: Any) -> int:
        try:
            return int(pd.Timestamp(x).value // 1_000_000)
        except Exception:
            return 0

    def _tick(v: float, step: float) -> int:
        if not step or not math.isfinite(step) or step <= 0.0:
            return int(round(float(v) * 1e12))
        try:
            return int(round(float(v) / float(step)))
        except Exception:
            return 0

    def _df(events: list[dict], prefix: str) -> pd.DataFrame:
        if not events:
            return pd.DataFrame(columns=["k_ts", "k_ot", "k_p", "k_q"])
        d = pd.DataFrame(events)
        if d.empty:
            return pd.DataFrame(columns=["k_ts", "k_ot", "k_p", "k_q"])
        d["k_ts"] = d["timestamp"].map(_to_ms)
        d["k_ot"] = d["order_type"].astype(str)
        d["k_p"] = d["price"].astype(float).map(lambda x: _tick(x, price_step))
        d["k_q"] = d["qty"].astype(float).map(lambda x: _tick(x, qty_step))
        keep = [
            "timestamp",
            "order_type",
            "qty",
            "price",
            "pos_size",
            "pos_price",
            "wallet_balance",
            "pnl",
            "fee_paid",
            "wallet_exposure",
        ]
        keep = [c for c in keep if c in d.columns]
        d = d[["k_ts", "k_ot", "k_p", "k_q"] + keep].copy()
        rename = {c: f"{prefix}_{c}" for c in keep}
        d = d.rename(columns=rename)
        return d

    dp = _df(pb7_events, "pb7")
    db = _df(b_events, "b")
    dc = _df(c_events, "c")

    # Outer-join on strict key
    m = dp.merge(db, how="outer", on=["k_ts", "k_ot", "k_p", "k_q"]).merge(
        dc, how="outer", on=["k_ts", "k_ot", "k_p", "k_q"]
    )

    # Prefer PB7 timestamp/type/price/qty for display, else B, else C (row-wise).
    # NOTE: must be per-row, otherwise b_only/c_only rows appear blank.
    def _s(col: str) -> pd.Series:
        return m[col] if col in m.columns else pd.Series(pd.NA, index=m.index)

    m["timestamp"] = _s("pb7_timestamp").combine_first(_s("b_timestamp")).combine_first(_s("c_timestamp"))
    m["order_type"] = _s("pb7_order_type").combine_first(_s("b_order_type")).combine_first(_s("c_order_type"))
    m["price"] = _s("pb7_price").combine_first(_s("b_price")).combine_first(_s("c_price"))
    m["qty"] = _s("pb7_qty").combine_first(_s("b_qty")).combine_first(_s("c_qty"))

    m["in_pb7"] = m.get("pb7_order_type").notna() if "pb7_order_type" in m.columns else False
    m["in_b"] = m.get("b_order_type").notna() if "b_order_type" in m.columns else False
    m["in_c"] = m.get("c_order_type").notna() if "c_order_type" in m.columns else False
    m["status"] = np.where(
        m["in_pb7"] & m["in_b"] & m["in_c"],
        "match",
        np.where(
            m["in_pb7"] & (~m["in_b"]) & (~m["in_c"]),
            "pb7_only",
            np.where(
                (~m["in_pb7"]) & m["in_b"] & (~m["in_c"]),
                "b_only",
                np.where(
                    (~m["in_pb7"]) & (~m["in_b"]) & m["in_c"],
                    "c_only",
                    np.where(
                        m["in_pb7"] & m["in_b"] & (~m["in_c"]),
                        "pb7_and_b",
                        np.where(
                            m["in_pb7"] & (~m["in_b"]) & m["in_c"],
                            "pb7_and_c",
                            np.where((~m["in_pb7"]) & m["in_b"] & m["in_c"], "b_and_c", "mismatch"),
                        ),
                    ),
                ),
            ),
        ),
    )

    cols_front = ["timestamp", "order_type", "qty", "price", "status"]
    cols_rest = [c for c in m.columns if c not in cols_front and not c.startswith("k_")]
    out = m[cols_front + cols_rest].sort_values(by=["timestamp", "order_type"], kind="mergesort")
    return out


def _compare_fills_b_c(
    *,
    b_events: list[dict],
    c_events: list[dict],
    price_step: float,
    qty_step: float,
) -> pd.DataFrame:
    """Compare Mode B vs Mode C fills without PB7 fills.csv."""

    def _to_ms(x: Any) -> int:
        try:
            return int(pd.Timestamp(x).value // 1_000_000)
        except Exception:
            return 0

    def _tick(v: float, step: float) -> int:
        if not step or not math.isfinite(step) or step <= 0.0:
            return int(round(float(v) * 1e12))
        try:
            return int(round(float(v) / float(step)))
        except Exception:
            return 0

    def _df(events: list[dict], prefix: str) -> pd.DataFrame:
        if not events:
            return pd.DataFrame(columns=["k_ts", "k_ot", "k_p", "k_q"])
        d = pd.DataFrame(events)
        if d.empty:
            return pd.DataFrame(columns=["k_ts", "k_ot", "k_p", "k_q"])
        d["k_ts"] = d["timestamp"].map(_to_ms)
        d["k_ot"] = d["order_type"].astype(str)
        d["k_p"] = d["price"].astype(float).map(lambda x: _tick(x, price_step))
        d["k_q"] = d["qty"].astype(float).map(lambda x: _tick(x, qty_step))
        keep = [
            "timestamp",
            "order_type",
            "qty",
            "price",
            "pos_size",
            "pos_price",
            "wallet_balance",
            "pnl",
            "fee_paid",
            "wallet_exposure",
        ]
        keep = [c for c in keep if c in d.columns]
        d = d[["k_ts", "k_ot", "k_p", "k_q"] + keep].copy()
        rename = {c: f"{prefix}_{c}" for c in keep}
        d = d.rename(columns=rename)
        return d

    db = _df(b_events, "b")
    dc = _df(c_events, "c")

    m = db.merge(dc, how="outer", on=["k_ts", "k_ot", "k_p", "k_q"])

    def _s(col: str) -> pd.Series:
        return m[col] if col in m.columns else pd.Series(pd.NA, index=m.index)

    m["timestamp"] = _s("b_timestamp").combine_first(_s("c_timestamp"))
    m["order_type"] = _s("b_order_type").combine_first(_s("c_order_type"))
    m["price"] = _s("b_price").combine_first(_s("c_price"))
    m["qty"] = _s("b_qty").combine_first(_s("c_qty"))

    m["in_b"] = m.get("b_order_type").notna() if "b_order_type" in m.columns else False
    m["in_c"] = m.get("c_order_type").notna() if "c_order_type" in m.columns else False
    m["status"] = np.where(m["in_b"] & m["in_c"], "match", np.where(m["in_b"], "b_only", "c_only"))

    cols_front = ["timestamp", "order_type", "qty", "price", "status"]
    cols_rest = [c for c in m.columns if c not in cols_front and not c.startswith("k_")]
    out = m[cols_front + cols_rest].sort_values(by=["timestamp", "order_type"], kind="mergesort")
    return out


def _run_compare_from_pb7_backtest_dir(
    *,
    pbr: Any,
    pb7_src: str,
    backtest_dir: str,
    max_orders: int = 20000,
) -> tuple[
    tuple[list[dict], list[dict]],
    tuple[list[dict], list[dict]],
    tuple[list[dict], list[dict]],
    dict,
]:
    """Run PB7(Benchmark) vs Mode B vs Mode C for the fills.csv time range.

    Returns: ((pb7_long, pb7_short), (b_long, b_short), (c_long, c_short), meta)
    meta includes: exchange, coin, start_ts, end_ts
    """
    backtest_dir = os.path.expanduser(str(backtest_dir or ""))
    cfg_path = os.path.join(backtest_dir, "config.json")
    cfg: dict = {}
    if os.path.isfile(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}

    pb7_long, pb7_short = _load_pb7_fills_csv_to_events(backtest_dir)
    all_pb7 = (pb7_long or []) + (pb7_short or [])
    if not all_pb7:
        return ([], []), ([], []), ([], []), {"exchange": "", "coin": "", "start_ts": None, "end_ts": None}

    try:
        ts_series = pd.to_datetime([e.get("timestamp") for e in all_pb7 if e.get("timestamp") is not None])
        start_ts = pd.to_datetime(ts_series.min())
        end_ts = pd.to_datetime(ts_series.max())
    except Exception:
        start_ts = pd.to_datetime(all_pb7[0].get("timestamp"))
        end_ts = pd.to_datetime(all_pb7[-1].get("timestamp"))

    # PB7 backtests begin at backtest.start_date 00:00; fills.csv may start later.
    # To reproduce PB7 engine state, run B/C from backtest.start_date and only filter to fills.csv time window.
    trade_start_ts = start_ts
    try:
        bt_start = ((cfg.get("backtest") or {}).get("start_date"))
        if bt_start:
            trade_start_ts = pd.to_datetime(str(bt_start))
    except Exception:
        trade_start_ts = start_ts

    exchange = ""
    coin = ""
    try:
        exchanges = (cfg.get("backtest") or {}).get("exchanges")
        if isinstance(exchanges, (list, tuple)) and exchanges:
            exchange = str(exchanges[0] or "")
        elif exchanges is not None:
            exchange = str(exchanges)
    except Exception:
        exchange = ""
    try:
        approved = (cfg.get("live") or {}).get("approved_coins") or {}
        coins_long = approved.get("long") if isinstance(approved, dict) else None
        if isinstance(coins_long, (list, tuple)) and coins_long:
            coin = str(coins_long[0] or "")
        elif coins_long is not None:
            coin = str(coins_long)
    except Exception:
        coin = ""
    if not coin:
        # fallback: infer from first pb7 event order_type suffix
        try:
            # order_type contains _long/_short; PB7 fills have separate `coin` column but our normalized events don't keep it.
            # Use config fallback only.
            pass
        except Exception:
            pass

    # Bot params from config.json (if available)
    try:
        bot_cfg = cfg.get("bot") or {}
        bp_long = BotParams(**(bot_cfg.get("long") or {}))
    except Exception:
        bp_long = BotParams()
    try:
        bot_cfg = cfg.get("bot") or {}
        bp_short = BotParams(**(bot_cfg.get("short") or {}))
    except Exception:
        bp_short = BotParams()

    # Starting balance
    try:
        starting_balance = float((cfg.get("backtest") or {}).get("starting_balance") or 0.0)
    except Exception:
        starting_balance = 0.0

    # Exchange params derived from market metadata
    market_ep = _derive_exchange_params_from_market(exchange, coin)
    exchange_params = ExchangeParams(
        qty_step=float(market_ep.get("qty_step") or 0.0),
        price_step=float(market_ep.get("price_step") or 0.0),
        min_qty=float(market_ep.get("min_qty") or 0.0),
        min_cost=float(market_ep.get("min_cost") or 0.0),
        c_mult=float(market_ep.get("c_mult") or 1.0),
    )

    # Warmup minutes: use PB7 config if possible (for parity)
    try:
        warmup_minutes = int(_compute_warmup_minutes_for_mode_c_from_config(cfg, bp_long, bp_short))
    except Exception:
        warmup_minutes = int(_compute_warmup_minutes_for_mode_c(bp_long, bp_short))

    # Candle range needed
    hist_df_full = load_historical_ohlcv_v7(exchange, coin)
    if hist_df_full is None or hist_df_full.empty:
        return (pb7_long, pb7_short), ([], []), ([], []), {"exchange": exchange, "coin": coin, "start_ts": start_ts, "end_ts": end_ts}

    warm_start = trade_start_ts - pd.Timedelta(minutes=max(0, warmup_minutes))
    try:
        candles = hist_df_full.loc[warm_start:end_ts].copy()
    except Exception:
        candles = hist_df_full.copy()

    # PB7 CandlestickManager standardizes gaps for 1m by synthesizing flat zero-volume candles.
    # Apply the same semantics here for parity.
    try:
        candles = _standardize_ohlcv_1m_gaps(candles, start_ts=warm_start, end_ts=end_ts)
    except Exception:
        pass

    b_long: list[dict] = []
    b_short: list[dict] = []

    # Run Mode C starting from PB7 backtest start_date, but only compare within fills.csv window.
    try:
        # Add a small safety buffer to ensure the final candle (at end_ts) is included.
        # Without this, the window may end up exclusive of the last minute and miss the final close.
        max_candles_forward = int(max(10, int((end_ts - trade_start_ts).total_seconds() // 60) + 5))
    except Exception:
        max_candles_forward = 2000
    c_long, c_short = [], []
    c_warmup_used: Optional[int] = None
    c_attempts = 0
    try:
        warmup_base = int(_compute_warmup_minutes_for_mode_c_from_config(cfg, bp_long, bp_short))
    except Exception:
        warmup_base = int(_compute_warmup_minutes_for_mode_c(bp_long, bp_short))

    # If Mode C starts trading too late (missing early fills), increase warmup window.
    for extra in (0, 1000, 2000, 4000, 8000, 12000, 16000):
        c_attempts += 1
        try:
            c_warmup_used = int(max(0, warmup_base + int(extra)))
        except Exception:
            c_warmup_used = int(max(0, warmup_base))

        try:
            c_long, c_short = _run_pb7_engine_backtest_for_visualizer(
                pbr=pbr,
                exchange=exchange,
                coin=coin,
                analysis_time=pd.to_datetime(trade_start_ts).to_pydatetime(),
                hist_df=hist_df_full,
                exchange_params=exchange_params,
                bot_params_long=bp_long,
                bot_params_short=bp_short,
                starting_balance=float(starting_balance),
                max_candles_forward=max_candles_forward,
                config=cfg,
                warmup_minutes_override=c_warmup_used,
            )
        except Exception:
            c_long, c_short = [], []

        try:
            c_all = (c_long or []) + (c_short or [])
            c_min_ts = min(pd.to_datetime(e.get("timestamp")) for e in c_all if e.get("timestamp") is not None)
        except Exception:
            c_min_ts = None

        # Stop once Mode C has fills at/earlier than PB7 first fill in the compare window.
        if c_min_ts is not None and pd.to_datetime(c_min_ts) <= pd.to_datetime(start_ts):
            break

    # Mode B: orchestrator-driven candle-walk simulation (should match PB7 fills.csv semantics)
    fees = {}
    try:
        fees = _derive_exchange_fees_from_market(exchange, coin)
    except Exception:
        fees = {}
    try:
        maker_fee = float((cfg.get("backtest") or {}).get("maker_fee") or 0.0)
    except Exception:
        maker_fee = 0.0
    if not math.isfinite(maker_fee) or maker_fee <= 0.0:
        try:
            maker_fee = float(fees.get("maker_fee", 0.0) or 0.0)
        except Exception:
            maker_fee = 0.0

    try:
        b_long, b_short = _simulate_backtest_over_historical_candles_pair(
            pbr=pbr,
            pb7_src=pb7_src,
            candles=candles,
            exchange_params=exchange_params,
            bot_params_long=bp_long,
            bot_params_short=bp_short,
            starting_position_long=Position(size=0.0, price=0.0),
            starting_position_short=Position(size=0.0, price=0.0),
            balance=float(starting_balance),
            maker_fee=float(maker_fee),
            trade_start_time=pd.to_datetime(trade_start_ts),
            max_orders=int(max_orders),
            max_candles=int(len(candles) if candles is not None else 0),
        )
    except Exception:
        b_long, b_short = [], []

    # Filter B/C/PB7 events to the fills.csv time range
    def _filt(events: list[dict]) -> list[dict]:
        out: list[dict] = []
        for e in events or []:
            try:
                t = pd.to_datetime(e.get("timestamp"))
            except Exception:
                continue
            if t < start_ts or t > end_ts:
                continue
            out.append(e)
        return out

    pb7_long = _filt(pb7_long)
    pb7_short = _filt(pb7_short)
    b_long = _filt(b_long)
    b_short = _filt(b_short)
    c_long = _filt(c_long)
    c_short = _filt(c_short)

    meta = {
        "exchange": exchange,
        "coin": coin,
        "trade_start_ts": trade_start_ts,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "price_step": float(getattr(exchange_params, "price_step", 0.0) or 0.0),
        "qty_step": float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
        "mode_c_warmup_used": c_warmup_used,
        "mode_c_attempts": c_attempts,
    }
    return (pb7_long, pb7_short), (b_long, b_short), (c_long, c_short), meta


def _run_pb7_engine_backtest_for_visualizer(
    *,
    pbr: Any,
    exchange: str,
    coin: str,
    analysis_time: datetime.datetime,
    hist_df: pd.DataFrame,
    exchange_params: ExchangeParams,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
    starting_balance: float,
    max_candles_forward: int,
    config: Optional[dict] = None,
    warmup_minutes_override: Optional[int] = None,
) -> tuple[list[dict], list[dict]]:
    """Mode C: run PB7 Rust backtest engine and return (events_long, events_short)."""

    if hist_df is None or hist_df.empty:
        return [], []

    maker_fee, taker_fee = _infer_maker_taker_fees(exchange, coin)
    if warmup_minutes_override is not None:
        try:
            warmup_minutes_req = max(0, int(warmup_minutes_override))
        except Exception:
            warmup_minutes_req = 0
    elif isinstance(config, dict) and config:
        warmup_minutes_req = _compute_warmup_minutes_for_mode_c_from_config(config, bot_params_long, bot_params_short)
    else:
        warmup_minutes_req = _compute_warmup_minutes_for_mode_c(bot_params_long, bot_params_short)

    # Root-cause parity: slice by timestamps, then standardize gaps, then compute trade_start_index
    # from the standardized index. If we computed trade_start_index before gap-filling, inserting
    # missing minutes would shift trade start by 1+ candles and cascade into mismatched fills.
    try:
        analysis_ts = pd.Timestamp(analysis_time).floor("min")
    except Exception:
        return [], []

    start_ts = analysis_ts - pd.Timedelta(minutes=max(0, int(warmup_minutes_req)))
    end_ts = analysis_ts + pd.Timedelta(minutes=max(0, int(max_candles_forward) - 1))
    if end_ts < start_ts + pd.Timedelta(minutes=5):
        end_ts = start_ts + pd.Timedelta(minutes=5)

    try:
        window = hist_df.loc[start_ts:end_ts].copy()
    except Exception:
        window = hist_df.copy()

    if window is None or window.empty:
        return [], []

    try:
        window = _standardize_ohlcv_1m_gaps(window, start_ts=start_ts, end_ts=end_ts)
    except Exception:
        pass

    if window is None or window.empty:
        return [], []

    # Trade starts at analysis_ts inside the standardized window.
    try:
        trade_start_index = int(window.index.get_indexer([analysis_ts], method="nearest")[0])
    except Exception:
        trade_start_index = 0
    trade_start_index = max(0, min(int(trade_start_index), int(len(window) - 1)))
    warmup_minutes_prov = int(trade_start_index)

    # Build tensors expected by PB7 backtest: (T, N, 4) with [high, low, close, volume]
    # Critical parity detail: PB7 backtest operates on f32-backed candle data.
    # If we feed float64 candles here, strict inequality checks (e.g. high > price) may flip
    # at the boundary and shift fills by 1 candle compared to Mode B / real PB7.
    highs = window["high"].astype(np.float32).astype(np.float64).to_numpy()
    lows = window["low"].astype(np.float32).astype(np.float64).to_numpy()
    closes = window["close"].astype(np.float32).astype(np.float64).to_numpy()
    vols = window["volume"].astype(np.float32).astype(np.float64).to_numpy()

    # Keep strict candle boundaries (do not inflate highs/deflate lows).

    hlcvs = np.stack([highs, lows, closes, vols], axis=1).reshape((-1, 1, 4))

    # BTC/USD series is required by the API.
    # PB7 provides real BTC prices; use best-effort lookup and align to this window.
    btc_usd = np.ones((hlcvs.shape[0],), dtype=np.float64)
    try:
        btc_df = load_historical_ohlcv_v7(exchange, "BTC")
        if btc_df is not None and not btc_df.empty and "close" in btc_df.columns:
            btc_close = (
                btc_df[["close"]]
                .copy()
                .reindex(window.index, method="ffill")
                .fillna(method="bfill")
            )
            btc_usd = btc_close["close"].astype("float64").to_numpy()
    except Exception:
        pass

    # timestamps array (ms)
    try:
        ts_ms = (window.index.view("int64") // 1_000_000).astype(np.int64)
    except Exception:
        ts_ms = np.arange(hlcvs.shape[0], dtype=np.int64)

    # Bundle metadata
    requested_start_ts_ms = int(pd.Timestamp(analysis_ts).value // 1_000_000)
    effective_start_ts_ms = int(ts_ms[0]) if len(ts_ms) else requested_start_ts_ms

    coin_meta = {
        "index": 0,
        "symbol": str(coin),
        "coin": str(coin),
        "exchange": str(exchange),
        "quote": "USDT",
        "base": str(coin),
        "qty_step": float(exchange_params.qty_step),
        "price_step": float(exchange_params.price_step),
        "min_qty": float(exchange_params.min_qty),
        "min_cost": float(exchange_params.min_cost),
        "c_mult": float(exchange_params.c_mult),
        "maker_fee": float(maker_fee),
        "taker_fee": float(taker_fee),
        "first_valid_index": 0,
        "last_valid_index": max(0, int(hlcvs.shape[0] - 1)),
        "warmup_minutes": int(warmup_minutes_prov),
        "trade_start_index": int(trade_start_index),
    }

    bundle_meta = {
        "requested_start_timestamp_ms": int(requested_start_ts_ms),
        "effective_start_timestamp_ms": int(effective_start_ts_ms),
        "warmup_minutes_requested": int(warmup_minutes_req),
        "warmup_minutes_provided": int(warmup_minutes_prov),
        "coins": [coin_meta],
    }

    bundle = pbr.HlcvsBundle(
        np.ascontiguousarray(hlcvs, dtype=np.float64),
        np.ascontiguousarray(btc_usd, dtype=np.float64),
        np.ascontiguousarray(ts_ms, dtype=np.int64),
        bundle_meta,
    )

    def _bp_dict(bp: BotParams, *, enabled: bool) -> dict:
        d = asdict(bp)
        # Backtest expects this key; PB7 defaults to -1.0 if not explicitly overridden.
        # (-1.0 has special meaning in PB7 config flow; keep for parity.)
        d.setdefault("wallet_exposure_limit", -1.0)
        # Disable side by forcing no exposure.
        if not enabled:
            d["total_wallet_exposure_limit"] = 0.0
        return d

    # Only enable sides that the visualizer considers active.
    long_enabled = float(bot_params_long.total_wallet_exposure_limit) > 0.0 and float(bot_params_long.n_positions) > 0.0
    short_enabled = float(bot_params_short.total_wallet_exposure_limit) > 0.0 and float(bot_params_short.n_positions) > 0.0

    bot_params_list = [
        {
            "long": _bp_dict(bot_params_long, enabled=long_enabled),
            "short": _bp_dict(bot_params_short, enabled=short_enabled),
        }
    ]
    exchange_params_list = [asdict(exchange_params)]

    backtest_params = {
        "starting_balance": float(starting_balance),
        "maker_fee": float(maker_fee),
        "coins": [str(coin)],
        "first_timestamp_ms": int(effective_start_ts_ms),
        "requested_start_timestamp_ms": int(requested_start_ts_ms),
        "first_valid_indices": [0],
        "last_valid_indices": [max(0, int(hlcvs.shape[0] - 1))],
        "warmup_minutes": [int(warmup_minutes_req)],
        "trade_start_indices": [int(trade_start_index)],
        "global_warmup_bars": int(warmup_minutes_req),
        "btc_collateral_cap": 0.0,
        "btc_collateral_ltv_cap": None,
        "metrics_only": False,
        "filter_by_min_effective_cost": False,
    }

    # Align a few backtest-level knobs with PB7 config if provided.
    # (No network, no PB7 backtest pipeline; just read values.)
    try:
        if isinstance(config, dict) and config:
            bt = config.get("backtest") or {}
            try:
                backtest_params["btc_collateral_cap"] = float(bt.get("btc_collateral_cap") or 0.0)
            except Exception:
                pass
            try:
                backtest_params["btc_collateral_ltv_cap"] = (
                    None
                    if bt.get("btc_collateral_ltv_cap") is None
                    else float(bt.get("btc_collateral_ltv_cap"))
                )
            except Exception:
                pass
            try:
                if "filter_by_min_effective_cost" in bt:
                    backtest_params["filter_by_min_effective_cost"] = bool(bt.get("filter_by_min_effective_cost"))
                else:
                    live = config.get("live") or {}
                    if "filter_by_min_effective_cost" in live:
                        backtest_params["filter_by_min_effective_cost"] = bool(live.get("filter_by_min_effective_cost"))
            except Exception:
                pass
    except Exception:
        pass

    fills, _equities, _analysis_usd, _analysis_btc = pbr.run_backtest_bundle(
        bundle,
        bot_params_list,
        exchange_params_list,
        backtest_params,
    )

    return _pb7_fills_to_events(fills)


def _update_trailing_bundle_from_price_short(tb: TrailingPriceBundle, price: float) -> TrailingPriceBundle:
    p = float(price)
    if p > float(tb.max_since_open):
        tb.max_since_open = p
        tb.min_since_max = p
    else:
        tb.min_since_max = float(min(float(tb.min_since_max), p))

    if p < float(tb.min_since_open):
        tb.min_since_open = p
        tb.max_since_min = p
    else:
        tb.max_since_min = float(max(float(tb.max_since_min), p))
    return tb


def _infer_hist_base_tf_minutes(hist_df: pd.DataFrame) -> int:
    """Infer base candle timeframe from a datetime index."""
    if hist_df is None or hist_df.empty:
        return 1
    try:
        idx = pd.to_datetime(hist_df.index)
        if len(idx) < 2:
            return 1
        deltas = (idx[1: min(len(idx), 500)] - idx[: min(len(idx) - 1, 499)]).total_seconds()
        deltas = [d for d in deltas if d and d > 0]
        if not deltas:
            return 1
        tf = int(round(float(np.median(deltas)) / 60.0))
        return max(1, tf)
    except Exception:
        return 1


def _slice_hist_df_for_modeb(
    hist_df: pd.DataFrame,
    *,
    trade_start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    include_prev_candle: bool = True,
) -> pd.DataFrame:
    """Slice historical candles for Mode B.

    We include one candle before `trade_start_time` (when possible) so that the
    first active candle can compute orders from a previous-minute state, which
    matches the PB7 backtest semantics used in the Mode B simulator.
    """
    if hist_df is None or hist_df.empty:
        return pd.DataFrame()
    try:
        df = hist_df.sort_index()
    except Exception:
        df = hist_df

    start = pd.to_datetime(trade_start_time)
    end = pd.to_datetime(end_time)
    if include_prev_candle:
        tf_mins = _infer_hist_base_tf_minutes(df)
        start = start - pd.Timedelta(minutes=int(tf_mins))

    try:
        return df.loc[start:end].copy()
    except Exception:
        # Fallback: return full df if slicing fails
        return df.copy()


def _get_modeb_starting_state(data, side: Side) -> tuple[Position, float]:
    """Return (starting_position, starting_balance) for Mode B simulation."""
    try:
        if side == Side.Long:
            pos = getattr(data, "position_long_enty", None)
            sp = getattr(data, "state_params_long", None) or getattr(data, "state_params", None)
        else:
            pos = getattr(data, "position_short_entry", None)
            sp = getattr(data, "state_params_short", None) or getattr(data, "state_params", None)
        if pos is None:
            pos = Position(size=0.0, price=0.0)
        bal = float(getattr(sp, "balance", 0.0) or 0.0)
        return (Position(size=float(pos.size), price=float(pos.price)), bal)
    except Exception:
        return (Position(size=0.0, price=0.0), 0.0)


def _simulate_backtest_over_historical_candles(
    *,
    pbr,
    pb7_src: str,
    side: Side,
    candles: pd.DataFrame,
    exchange_params: ExchangeParams,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
    starting_position: Position,
    balance: float,
    maker_fee: float = 0.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
) -> List[dict]:
    """Candle-by-candle mini backtest: simulates filled entry AND close orders.

    - Entries: Rust `calc_next_entry_*` per step.
    - Closes: Rust `calc_closes_*` per step (take all close orders and fill those crossed).
    - Uses PB7-like 1m semantics:
        - Orders are computed from previous-minute state and kept fixed during the candle.
        - Fills are determined using candle extremes only (buys: low < price, sells: high > price).
        - Closes are processed before entries.
        - Trailing bundle is updated with (high, low, close) each candle, and reset to default if any fill happened.
    """
    if candles is None or candles.empty:
        return []

    if max_candles > 0 and len(candles) > max_candles:
        candles = candles.iloc[:max_candles].copy()
    else:
        candles = candles.copy()

    # Parity note (PB7 vs Mode B): PB7 backtest uses f32-backed candle data.
    # With strict fill checks (low < price, high > price), float64 vs float32 edge cases can
    # flip a fill decision and then cascade into many downstream mismatches.
    # Therefore we quantize OHLCV to float32 in Mode B.
    for col in ("open", "high", "low", "close", "volume"):
        if col in candles.columns:
            try:
                candles[col] = candles[col].astype(np.float32)
            except Exception:
                pass

    for col in ("open", "high", "low", "close"):
        if col not in candles.columns:
            return []

    # Build EMA bundle components needed by PB7 orchestrator.
    # Note: Mode B uses prev-minute state; we will read EMAs from (i-1).
    ema_df = _prepare_orchestrator_ema_df(
        candles,
        bot_params_long=bot_params_long,
        bot_params_short=bot_params_short,
    )
    if ema_df is None or ema_df.empty:
        return []

    events: List[dict] = []
    pos = Position(size=float(starting_position.size), price=float(starting_position.price))
    sim_balance = float(balance)
    maker_fee = float(maker_fee or 0.0)
    if not math.isfinite(maker_fee) or maker_fee < 0.0:
        maker_fee = 0.0

    c_mult = float(getattr(exchange_params, "c_mult", 1.0) or 1.0)
    if c_mult <= 0.0:
        c_mult = 1.0

    qty_step = float(getattr(exchange_params, "qty_step", 0.0) or 0.0)
    if not math.isfinite(qty_step) or qty_step < 0.0:
        qty_step = 0.0

    price_step = float(getattr(exchange_params, "price_step", 0.0) or 0.0)
    if not math.isfinite(price_step) or price_step < 0.0:
        price_step = 0.0

    # PB7 `TrailingPriceBundle::default()` uses `f64::MAX` for the two "min" fields.
    # Use the same value (and keep it JSON-safe).
    TRAILING_INF = float(getattr(sys, "float_info", None).max) if getattr(sys, "float_info", None) else 1.7976931348623157e308

    # PB7 TrailingPriceBundle::default(): min_since_open=inf, max_since_min=0, max_since_open=0, min_since_max=inf
    tb = TrailingPriceBundle(float(TRAILING_INF), 0.0, 0.0, float(TRAILING_INF))

    # Trailing bundle update only if grid_ratio != 0 (PB7 backtest behavior).
    trailing_enabled = _any_trailing_enabled_for_backtest(bot_params_long) or _any_trailing_enabled_for_backtest(bot_params_short)

    def _pos_has_side_position(p: Position) -> bool:
        try:
            if side == Side.Long:
                return float(p.size) > 0.0
            return float(p.size) < 0.0
        except Exception:
            return False

    trade_start_time_pd = pd.to_datetime(trade_start_time) if trade_start_time is not None else None

    if len(candles.index) < 2:
        return events

    idx_list = list(candles.index)

    pnl_cumsum_running = 0.0
    pnl_cumsum_max = 0.0

    def _pb7_update_trailing_bundle_with_candle(bundle: TrailingPriceBundle, high: float, low: float, close: float) -> TrailingPriceBundle:
        # Mirrors pb7/passivbot-rust/src/trailing.rs::update_trailing_bundle_with_candle
        if not (math.isfinite(high) and math.isfinite(low) and math.isfinite(close)):
            return bundle
        if float(low) < float(bundle.min_since_open):
            bundle.min_since_open = float(low)
            bundle.max_since_min = float(close)
        else:
            bundle.max_since_min = float(max(float(bundle.max_since_min), float(high)))
        if float(high) > float(bundle.max_since_open):
            bundle.max_since_open = float(high)
            bundle.min_since_max = float(close)
        else:
            bundle.min_since_max = float(min(float(bundle.min_since_max), float(low)))
        return bundle

    def _pb7_reset_trailing_bundle(bundle: TrailingPriceBundle) -> TrailingPriceBundle:
        bundle.min_since_open = float(TRAILING_INF)
        bundle.max_since_min = 0.0
        bundle.max_since_open = 0.0
        bundle.min_since_max = float(TRAILING_INF)
        return bundle

    def _order_filled(low: float, high: float, qty: float, price: float, order_type: Optional[str] = None) -> bool:
        # Parity note (PB7 vs Mode B):
        # - PB7 candles are f32-backed.
        # - PB7 compares candle bounds (as f64 converted from f32) against order prices (f64).
        # - Fill checks are strict (< and >).
        try:
            low = float(np.float32(float(low)))
            high = float(np.float32(float(high)))
        except Exception:
            pass

        if qty > 0.0:
            return float(low) < float(price)
        if qty < 0.0:
            return float(high) > float(price)
        return False

    def _effective_min_cost(close_price: float) -> float:
        try:
            q = float(getattr(exchange_params, "min_qty", 0.0) or 0.0)
            mc = float(getattr(exchange_params, "min_cost", 0.0) or 0.0)
        except Exception:
            q, mc = 0.0, 0.0
        try:
            return float(max(float(pbr.qty_to_cost(float(q), float(close_price), float(c_mult))), float(mc)))
        except Exception:
            return float(max(abs(q) * float(close_price) * float(c_mult), mc))

    long_enabled = side == Side.Long
    short_enabled = side == Side.Short

    # PB7 orchestrator input includes:
    # - per-symbol bot_params (after overrides)
    # - global_bot_params from a *master* pair where n_positions is clamped to n_coins.
    bp_long_symbol_dict = _bot_params_dict_for_orchestrator_single_symbol(bot_params_long, enabled=True)
    bp_short_symbol_dict = _bot_params_dict_for_orchestrator_single_symbol(bot_params_short, enabled=True)
    bp_long_master_dict = dict(bp_long_symbol_dict)
    bp_short_master_dict = dict(bp_short_symbol_dict)
    try:
        bp_long_master_dict["n_positions"] = int(min(int(bp_long_master_dict.get("n_positions") or 0), 1))
    except Exception:
        pass
    try:
        bp_short_master_dict["n_positions"] = int(min(int(bp_short_master_dict.get("n_positions") or 0), 1))
    except Exception:
        pass

    def _unstuck_allowance() -> tuple[float, float]:
        # Mirrors PB7 backtest build_orchestrator_input: compute both long+short allowances from balance.
        def _one(bp: BotParams) -> float:
            try:
                pct = float(getattr(bp, "unstuck_loss_allowance_pct", 0.0) or 0.0)
                total_wel = float(getattr(bp, "total_wallet_exposure_limit", 0.0) or 0.0)
            except Exception:
                pct, total_wel = 0.0, 0.0
            if pct <= 0.0 or total_wel <= 0.0:
                return 0.0
            try:
                return float(
                    pbr.calc_auto_unstuck_allowance(
                        float(sim_balance),
                        float(pct) * float(total_wel),
                        float(pnl_cumsum_max),
                        float(pnl_cumsum_running),
                    )
                )
            except Exception:
                return 0.0

        return (_one(bot_params_long), _one(bot_params_short))

    def _compute_orch_orders(
        *,
        ob_price: float,
        ema_row: pd.Series,
        position: Position,
        trailing: TrailingPriceBundle,
        next_low: float,
        next_high: float,
        tradable_now: bool,
        tradable_next: bool,
    ) -> tuple[list[dict], list[dict]]:
        # Build minimal OrchestratorInput for 1 symbol.
        ul, us = _unstuck_allowance()

        # EMA bundle
        m1_close: list[list[float]] = []
        h1_log_range: list[list[float]] = []
        m1_volume: list[list[float]] = []
        m1_log_range: list[list[float]] = []

        # PB7 passes both long+short EMA spans/values in the same timeframe bundle.
        def _append_ema_close(bp: BotParams, prefix: str):
            span0 = float(getattr(bp, "ema_span_0", 1.0) or 1.0)
            span1 = float(getattr(bp, "ema_span_1", 1.0) or 1.0)
            span2 = float(max(1.0, float(span0 * span1) ** 0.5))
            span0 = max(1.0, float(span0))
            span1 = max(1.0, float(span1))
            span2 = max(1.0, float(span2))
            if prefix == "l":
                v0 = float(ema_row.get("ema_l0", ob_price) or ob_price)
                v1 = float(ema_row.get("ema_l1", ob_price) or ob_price)
                v2 = float(ema_row.get("ema_l2", ob_price) or ob_price)
            else:
                v0 = float(ema_row.get("ema_s0", ob_price) or ob_price)
                v1 = float(ema_row.get("ema_s1", ob_price) or ob_price)
                v2 = float(ema_row.get("ema_s2", ob_price) or ob_price)
            pairs: list[tuple[float, float]] = [(span0, v0), (span1, v1), (span2, v2)]
            pairs.sort(key=lambda x: x[0])
            for s, v in pairs:
                m1_close.append([float(s), float(v)])

        _append_ema_close(bot_params_long, "l")
        _append_ema_close(bot_params_short, "s")

        # m1 volume/log-range (two entries: long + short)
        m1_volume.append([
            float(getattr(bot_params_long, "filter_volume_ema_span", 1.0) or 1.0),
            float(ema_row.get("vol_ema_l", 0.0) or 0.0),
        ])
        m1_volume.append([
            float(getattr(bot_params_short, "filter_volume_ema_span", 1.0) or 1.0),
            float(ema_row.get("vol_ema_s", 0.0) or 0.0),
        ])
        m1_log_range.append([
            float(getattr(bot_params_long, "filter_volatility_ema_span", 1.0) or 1.0),
            float(ema_row.get("lr_ema_l", 0.0) or 0.0),
        ])
        m1_log_range.append([
            float(getattr(bot_params_short, "filter_volatility_ema_span", 1.0) or 1.0),
            float(ema_row.get("lr_ema_s", 0.0) or 0.0),
        ])

        # 1h log-range EMA (optional per-side span)
        span_h_l = float(getattr(bot_params_long, "entry_volatility_ema_span_hours", 0.0) or 0.0)
        if span_h_l > 0.0:
            h1_log_range.append([float(span_h_l), float(ema_row.get("h1_lr_ema_l", 0.0) or 0.0)])
        span_h_s = float(getattr(bot_params_short, "entry_volatility_ema_span_hours", 0.0) or 0.0)
        if span_h_s > 0.0:
            h1_log_range.append([float(span_h_s), float(ema_row.get("h1_lr_ema_s", 0.0) or 0.0)])

        default_trailing = TrailingPriceBundle(float(TRAILING_INF), 0.0, 0.0, float(TRAILING_INF))
        long_trailing = asdict(trailing) if (long_enabled and trailing is not None) else asdict(default_trailing)
        short_trailing = asdict(trailing) if (short_enabled and trailing is not None) else asdict(default_trailing)

        if not bool(tradable_next):
            next_low = 0.0
            next_high = 0.0

        ob_p = float(ob_price)
        if not math.isfinite(ob_p) or ob_p <= 0.0:
            ob_p = float(np.finfo("float64").eps)

        inp = {
            "balance": float(sim_balance),
            "global": {
                "filter_by_min_effective_cost": False,
                "unstuck_allowance_long": float(ul),
                "unstuck_allowance_short": float(us),
                "sort_global": False,
                "global_bot_params": {"long": bp_long_master_dict, "short": bp_short_master_dict},
            },
            "symbols": [
                {
                    "symbol_idx": 0,
                    "order_book": {"bid": float(ob_p), "ask": float(ob_p)},
                    "exchange": {
                        "qty_step": float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
                        "price_step": float(getattr(exchange_params, "price_step", 0.0) or 0.0),
                        "min_qty": float(getattr(exchange_params, "min_qty", 0.0) or 0.0),
                        "min_cost": float(getattr(exchange_params, "min_cost", 0.0) or 0.0),
                        "c_mult": float(c_mult),
                    },
                    "tradable": bool(tradable_now),
                    "next_candle": {"low": float(next_low), "high": float(next_high), "tradable": bool(tradable_next)},
                    "effective_min_cost": float(_effective_min_cost(float(ob_p))),
                    "emas": {
                        "m1": {
                            "close": m1_close,
                            "volume": m1_volume,
                            "log_range": m1_log_range,
                        },
                        "h1": {
                            "close": [],
                            "volume": [],
                            "log_range": h1_log_range,
                        },
                    },
                    "long": {
                        "mode": None,
                        "position": {"size": float(position.size if long_enabled else 0.0), "price": float(position.price if long_enabled else 0.0)},
                        "trailing": long_trailing,
                        "bot_params": bp_long_symbol_dict,
                    },
                    "short": {
                        "mode": None,
                        "position": {"size": float(position.size if short_enabled else 0.0), "price": float(position.price if short_enabled else 0.0)},
                        "trailing": short_trailing,
                        "bot_params": bp_short_symbol_dict,
                    },
                }
            ],
            "peek_hints": None,
        }

        out_json = pbr.compute_ideal_orders_json(json.dumps(inp))
        out = json.loads(out_json)
        orders = out.get("orders") or []
        entries: list[dict] = []
        closes: list[dict] = []
        want_pside = "long" if side == Side.Long else "short"
        for o in orders:
            try:
                if int(o.get("symbol_idx", -1)) != 0:
                    continue
                if str(o.get("pside")) != want_pside:
                    continue
                ot = str(o.get("order_type") or "")
                rec = {
                    "qty": float(o.get("qty") or 0.0),
                    "price": float(o.get("price") or 0.0),
                    "order_type": ot,
                }
                if ot.startswith("close_"):
                    closes.append(rec)
                else:
                    entries.append(rec)
            except Exception:
                continue
        return entries, closes

    for i in range(1, len(idx_list)):
        ts = idx_list[i]
        row = candles.loc[ts]
        prev_ts = idx_list[i - 1]
        prev_row = candles.loc[prev_ts]
        ema_prev = ema_df.loc[prev_ts] if prev_ts in ema_df.index else None
        if len(events) >= int(max_orders):
            break

        trading_active = True
        prev_trading_active = True
        if trade_start_time_pd is not None:
            try:
                trading_active = pd.to_datetime(ts) >= trade_start_time_pd
            except Exception:
                trading_active = True
            try:
                prev_trading_active = pd.to_datetime(prev_ts) >= trade_start_time_pd
            except Exception:
                prev_trading_active = True

        # PB7 uses float32-quantized candles stored in numpy and then read as f64.
        # Mirror that here to avoid strict-inequality edge cases.
        def _f32(x: float) -> float:
            try:
                return float(np.float32(float(x)))
            except Exception:
                return float(x)

        open_px = _f32(row["open"])
        close_px = _f32(row["close"])
        low_px = _f32(row["low"])
        high_px = _f32(row["high"])

        prev_close_px = _f32(prev_row.get("close", open_px) or open_px)

        # next candle hint for orchestrator expansion = current candle range
        next_low = float(low_px)
        next_high = float(high_px)

        # Orders are computed from prev-minute state.
        tb_local = copy.deepcopy(tb)
        filled_this_candle = False

        if trading_active and ema_prev is not None:
            pending_entries, pending_closes = _compute_orch_orders(
                ob_price=float(prev_close_px),
                ema_row=ema_prev,
                position=pos,
                trailing=tb_local,
                next_low=next_low,
                next_high=next_high,
                tradable_now=bool(prev_trading_active),
                tradable_next=bool(trading_active),
            )
        else:
            pending_entries, pending_closes = ([], [])

        # Process closes first (PB7 behavior)
        if trading_active and pending_closes and _pos_has_side_position(pos):
            for o in pending_closes:
                if len(events) >= int(max_orders):
                    break
                # PB7: if a close fully removes the position, remaining closes in the same candle
                # must not be processed (Backtest removes the position from the map).
                if not _pos_has_side_position(pos):
                    break
                q = float(o.get("qty") or 0.0)
                p = float(o.get("price") or 0.0)
                ot = str(o.get("order_type") or "")
                if q == 0.0 or p <= 0.0:
                    continue
                if not _order_filled(low_px, high_px, q, p):
                    continue

                # PB7 close fill adjustment if order closes beyond remaining psize.
                adj_qty = float(q)
                try:
                    if side == Side.Long:
                        new_psize = float(pbr.round_(float(pos.size) + float(adj_qty), float(qty_step))) if qty_step > 0.0 else float(pos.size) + float(adj_qty)
                        if new_psize < 0.0:
                            new_psize = 0.0
                            adj_qty = -float(pos.size)
                    else:
                        new_psize = float(pbr.round_(float(pos.size) + float(adj_qty), float(qty_step))) if qty_step > 0.0 else float(pos.size) + float(adj_qty)
                        if new_psize > 0.0:
                            new_psize = 0.0
                            adj_qty = abs(float(pos.size))
                except Exception:
                    new_psize = float(pos.size) + float(adj_qty)

                fee_paid = -float(pbr.qty_to_cost(float(adj_qty), float(p), float(c_mult))) * float(maker_fee)
                pnl = float(pbr.calc_pnl_long(float(pos.price), float(p), float(adj_qty), float(c_mult))) if side == Side.Long else float(
                    pbr.calc_pnl_short(float(pos.price), float(p), float(adj_qty), float(c_mult))
                )
                pnl_cumsum_running += float(pnl)
                pnl_cumsum_max = max(float(pnl_cumsum_max), float(pnl_cumsum_running))
                sim_balance += float(pnl) + float(fee_paid)

                # PB7 removes the position when size hits 0 (Position::default => price=0).
                if float(new_psize) == 0.0:
                    pos = Position(size=0.0, price=0.0)
                else:
                    pos = Position(size=float(new_psize), price=float(pos.price))

                events.append(
                    {
                        "timestamp": pd.to_datetime(ts),
                        "event": "close",
                        "qty": float(adj_qty),
                        "price": float(p),
                        "order_type": ot,
                        "fee_paid": float(fee_paid),
                        "wallet_balance": float(sim_balance),
                        "pos_size": float(pos.size),
                        "pos_price": float(pos.price),
                        "pnl": float(pnl),
                    }
                )
                filled_this_candle = True

                if not _pos_has_side_position(pos):
                    break

        # Process entries after closes.
        if trading_active and pending_entries:
            for o in pending_entries:
                if len(events) >= int(max_orders):
                    break
                q = float(o.get("qty") or 0.0)
                p = float(o.get("price") or 0.0)
                ot = str(o.get("order_type") or "")
                if q == 0.0 or p <= 0.0:
                    continue
                if not _order_filled(low_px, high_px, q, p):
                    continue

                fee_paid = -float(pbr.qty_to_cost(float(q), float(p), float(c_mult))) * float(maker_fee)
                sim_balance += float(fee_paid)
                try:
                    new_psize, new_pprice = pbr.calc_new_psize_pprice(
                        float(pos.size),
                        float(pos.price),
                        float(q),
                        float(p),
                        float(qty_step) if qty_step > 0.0 else 0.0,
                    )
                    pos = Position(size=float(new_psize), price=float(new_pprice))
                except Exception:
                    pos = _apply_fill_to_position(position=pos, fill_qty=float(q), fill_price=float(p))

                events.append(
                    {
                        "timestamp": pd.to_datetime(ts),
                        "event": "entry",
                        "qty": float(q),
                        "price": float(p),
                        "order_type": ot,
                        "fee_paid": float(fee_paid),
                        "wallet_balance": float(sim_balance),
                        "pos_size": float(pos.size),
                        "pos_price": float(pos.price),
                        "pnl": 0.0,
                    }
                )
                filled_this_candle = True

        # PB7 backtest: update trailing bundle only for symbols with an active position.
        # Reset to default if any fill happened for the symbol during the candle; otherwise update with (high, low, close).
        if trailing_enabled and _pos_has_side_position(pos):
            if filled_this_candle:
                tb = _pb7_reset_trailing_bundle(tb_local)
            else:
                tb = _pb7_update_trailing_bundle_with_candle(tb_local, float(high_px), float(low_px), float(close_px))
        else:
            tb = tb_local

    return events


def _simulate_backtest_over_historical_candles_pair(
    *,
    pbr,
    pb7_src: str,
    candles: pd.DataFrame,
    exchange_params: ExchangeParams,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
    starting_position_long: Position,
    starting_position_short: Position,
    balance: float,
    maker_fee: float = 0.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
) -> tuple[list[dict], list[dict]]:
    """Mode B candle-walk simulation with shared balance across long+short.

    Wrapper around `_simulate_backtest_over_historical_candles_pair_core` used by both compare and movie builder.
    Returns (long_events, short_events).
    """
    ev_l, ev_s, _frames = _simulate_backtest_over_historical_candles_pair_core(
        pbr=pbr,
        pb7_src=pb7_src,
        side_for_frames=Side.Long,
        candles=candles,
        exchange_params=exchange_params,
        bot_params_long=bot_params_long,
        bot_params_short=bot_params_short,
        starting_position_long=starting_position_long,
        starting_position_short=starting_position_short,
        balance=balance,
        maker_fee=maker_fee,
        trade_start_time=trade_start_time,
        max_orders=max_orders,
        max_candles=max_candles,
        capture_frames=False,
        include_viz_grids=False,
    )
    return ev_l, ev_s


def _simulate_backtest_over_historical_candles_replay(
    *,
    pbr,
    pb7_src: str,
    side: Side,
    candles: pd.DataFrame,
    exchange_params: ExchangeParams,
    bot_params: BotParams,
    starting_position: Position,
    balance: float,
    maker_fee: float = 0.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
    frame_every_n_candles: int = 1,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> tuple[list[dict], list[dict]]:
    """Mode B candle-walk simulation + replay capture.

    Returns (events, frames), where each frame contains:
      - candle OHLC
      - pending entry/close grids for that candle
      - fills that happened on that candle
      - trailing bundle + position + balance (before/after)
    """
    if candles is None or candles.empty:
        return ([], [])

    if max_candles > 0 and len(candles) > max_candles:
        candles = candles.iloc[:max_candles].copy()

    for col in ("open", "high", "low", "close"):
        if col not in candles.columns:
            return ([], [])

    ind = calculate_v7_indicators(
        candles,
        float(bot_params.ema_span_0),
        float(bot_params.ema_span_1),
        float(bot_params.entry_volatility_ema_span_hours),
    )

    events: list[dict] = []
    frames: list[dict] = []

    pos = Position(size=float(starting_position.size), price=float(starting_position.price))
    sim_balance = float(balance)
    maker_fee = float(maker_fee or 0.0)
    if not math.isfinite(maker_fee) or maker_fee < 0.0:
        maker_fee = 0.0

    c_mult = float(getattr(exchange_params, "c_mult", 1.0) or 1.0)
    if c_mult <= 0.0:
        c_mult = 1.0

    qty_step = float(getattr(exchange_params, "qty_step", 0.0) or 0.0)
    if not math.isfinite(qty_step) or qty_step < 0.0:
        qty_step = 0.0

    price_step = float(getattr(exchange_params, "price_step", 0.0) or 0.0)
    if not math.isfinite(price_step) or price_step < 0.0:
        price_step = 0.0
    price_eps = 0.0

    def _snap_pos_size(psize: float) -> float:
        psize = float(psize)
        if not math.isfinite(psize):
            return 0.0
        if qty_step <= 0.0:
            return 0.0 if abs(psize) <= 1e-12 else psize
        try:
            snapped = float(pbr.round_(float(psize), float(qty_step)))
        except Exception:
            ticks = int(round(psize / qty_step))
            snapped = float(ticks) * float(qty_step)
        if abs(snapped) < float(qty_step) * 0.5:
            return 0.0
        return snapped

    tb = TrailingPriceBundle(float("inf"), 0.0, 0.0, float("inf"))

    ep_json = json.dumps(asdict(exchange_params), sort_keys=True)
    bp_json = json.dumps(asdict(bot_params), sort_keys=True)

    total_wel = float(getattr(bot_params, "total_wallet_exposure_limit", 0.0) or 0.0)
    n_positions = int(getattr(bot_params, "n_positions", 0) or 0)
    wel_per_pos = (total_wel / float(n_positions)) if n_positions else total_wel

    def _pos_has_side_position(p: Position) -> bool:
        s = _snap_pos_size(float(p.size))
        if side == Side.Long:
            return s > 0.0
        return s < 0.0

    trade_start_time_pd = pd.to_datetime(trade_start_time) if trade_start_time is not None else None

    if len(ind) < 2:
        return (events, frames)

    ind_index = list(ind.index)
    fe = max(1, int(frame_every_n_candles or 1))
    total_steps = max(1, len(ind_index) - 1)
    progress_every = max(1, int(total_steps // 200))
    for i in range(1, len(ind_index)):
        if progress_cb is not None and (i == 1 or i == total_steps or (i % progress_every == 0)):
            try:
                progress_cb(min(1.0, float(i) / float(total_steps)), f"Simulating candles {i}/{total_steps}")
            except Exception:
                pass
        ts = ind_index[i]
        row = ind.loc[ts]
        prev_ts = ind_index[i - 1]
        prev_row = ind.loc[prev_ts]
        if len(events) >= int(max_orders):
            break

        trading_active = True
        if trade_start_time_pd is not None:
            try:
                trading_active = pd.to_datetime(ts) >= trade_start_time_pd
            except Exception:
                trading_active = True

        open_px = float(row["open"])
        close_px = float(row["close"])
        low_px = float(row["low"])
        high_px = float(row["high"])

        prev_close_px = float(prev_row.get("close", open_px) or open_px)
        vol = float(prev_row.get("volatility", 0.0) or 0.0)
        ema0 = float(prev_row.get("ema_0", prev_close_px) or prev_close_px)
        ema1 = float(prev_row.get("ema_1", prev_close_px) or prev_close_px)
        ema2 = float(prev_row.get("ema_2", prev_close_px) or prev_close_px)

        ema_lower = float(min(ema0, ema1, ema2))
        ema_upper = float(max(ema0, ema1, ema2))

        tb_local = copy.deepcopy(tb)
        filled_this_candle = False
        candle_fills: list[dict] = []

        pos_before = {"size": float(pos.size), "price": float(pos.price)}
        bal_before = float(sim_balance)
        tb_before = asdict(tb)

        def _pb7_update_trailing_bundle_with_candle(bundle: TrailingPriceBundle, high: float, low: float, close: float) -> TrailingPriceBundle:
            if not (math.isfinite(high) and math.isfinite(low) and math.isfinite(close)):
                return bundle
            if float(low) < float(bundle.min_since_open):
                bundle.min_since_open = float(low)
                bundle.max_since_min = float(close)
            else:
                bundle.max_since_min = float(max(float(bundle.max_since_min), float(high)))
            if float(high) > float(bundle.max_since_open):
                bundle.max_since_open = float(high)
                bundle.min_since_max = float(close)
            else:
                bundle.min_since_max = float(min(float(bundle.min_since_max), float(low)))
            return bundle

        def _pb7_reset_trailing_bundle(bundle: TrailingPriceBundle) -> TrailingPriceBundle:
            bundle.min_since_open = float("inf")
            bundle.max_since_min = 0.0
            bundle.max_since_open = 0.0
            bundle.min_since_max = float("inf")
            return bundle

        def _order_would_fill_is_buy(is_buy: bool, price: float, low: float, high: float) -> bool:
            if is_buy:
                return (float(low) < float(price)) or (abs(float(low) - float(price)) <= float(price_eps))
            return (float(high) > float(price)) or (abs(float(high) - float(price)) <= float(price_eps))

        def _entry_is_buy() -> bool:
            return side == Side.Long

        def _close_is_buy() -> bool:
            return side == Side.Short

        def _make_state_params(px: float) -> StateParams:
            return StateParams(
                balance=float(sim_balance),
                order_book=OrderBook(bid=float(px), ask=float(px)),
                ema_bands=EmaBands(lower=float(ema_lower), upper=float(ema_upper)),
                entry_volatility_logrange_ema_1h=float(vol),
            )

        def _recalc_entries(sp_for_calc: StateParams):
            sp_json = json.dumps(asdict(sp_for_calc), sort_keys=True)
            tb_json = json.dumps(asdict(tb_local), sort_keys=True)
            raw_entries = _calc_entries_rust_cached(
                pb7_src,
                int(side.value),
                ep_json,
                sp_json,
                tb_json,
                bp_json,
                float(pos.size),
                float(pos.price),
            )
            entries: list[tuple[float, float, int]] = []
            for q, p, t in (raw_entries or []):
                try:
                    entries.append((float(q), float(p), int(t)))
                except Exception:
                    continue
            return entries

        def _recalc_closes(sp_for_calc: StateParams):
            if not _pos_has_side_position(pos) or float(pos.price) <= 0.0:
                return []
            sp_json = json.dumps(asdict(sp_for_calc), sort_keys=True)
            tb_json = json.dumps(asdict(tb_local), sort_keys=True)
            raw_closes = _calc_closes_rust_cached(
                pb7_src,
                int(side.value),
                ep_json,
                sp_json,
                tb_json,
                bp_json,
                float(pos.size),
                float(pos.price),
            )
            closes: list[tuple[float, float, int]] = []
            for q, p, t in (raw_closes or []):
                try:
                    closes.append((float(q), float(p), int(t)))
                except Exception:
                    continue

            # Add unstucking close order (PB7 v7) if triggered.
            try:
                cur_px = float(sp_for_calc.order_book.ask if side == Side.Long else sp_for_calc.order_book.bid)
                allow_long = float(sim_balance) * float(getattr(bot_params, "unstuck_loss_allowance_pct", 0.0) or 0.0)
                allow_short = 0.0
                if side == Side.Short:
                    allow_short, allow_long = allow_long, 0.0

                pos_dict = {
                    "idx": 0,
                    "side": "long" if side == Side.Long else "short",
                    "position_size": float(pos.size),
                    "position_price": float(pos.price),
                    "wallet_exposure_limit": float(wel_per_pos),
                    "risk_we_excess_allowance_pct": float(getattr(bot_params, "risk_we_excess_allowance_pct", 0.0) or 0.0),
                    "unstuck_threshold": float(getattr(bot_params, "unstuck_threshold", 0.0) or 0.0),
                    "unstuck_close_pct": float(getattr(bot_params, "unstuck_close_pct", 0.0) or 0.0),
                    "unstuck_ema_dist": float(getattr(bot_params, "unstuck_ema_dist", 0.0) or 0.0),
                    "unstuck_loss_allowance_pct": float(getattr(bot_params, "unstuck_loss_allowance_pct", 0.0) or 0.0),
                    "ema_band_upper": float(sp_for_calc.ema_bands.upper),
                    "ema_band_lower": float(sp_for_calc.ema_bands.lower),
                    "current_price": float(cur_px),
                    "price_step": float(getattr(exchange_params, "price_step", 0.0) or 0.0),
                    "qty_step": float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
                    "min_qty": float(getattr(exchange_params, "min_qty", 0.0) or 0.0),
                    "min_cost": float(getattr(exchange_params, "min_cost", 0.0) or 0.0),
                    "c_mult": float(getattr(exchange_params, "c_mult", 1.0) or 1.0),
                }
                unstuck = pbr.calc_unstucking_close_py(float(sim_balance), float(allow_long), float(allow_short), [pos_dict])
                if unstuck is not None:
                    q_u = float(unstuck[2])
                    p_u = float(unstuck[3])
                    t_u = int(unstuck[4])
                    if math.isfinite(q_u) and math.isfinite(p_u) and q_u != 0.0 and p_u > 0.0:
                        closes.append((q_u, p_u, t_u))
            except Exception:
                pass
            return closes

        sp_calc = _make_state_params(prev_close_px)
        pending_entries = _recalc_entries(sp_calc) if trading_active else []
        pending_closes = _recalc_closes(sp_calc) if trading_active else []

        # Process closes first.
        if trading_active and pending_closes and _pos_has_side_position(pos):
            closes_to_process = [c for c in pending_closes if _order_would_fill_is_buy(_close_is_buy(), c[1], low_px, high_px)]
            for qc, pc, tc in closes_to_process:
                if len(events) >= int(max_orders):
                    break
                remaining = abs(float(pos.size))
                fill_amt = min(abs(float(qc)), remaining)
                if fill_amt <= 0.0:
                    continue

                if side == Side.Long:
                    q_eff = -fill_amt
                else:
                    q_eff = +fill_amt

                entry_price = float(pos.price)
                if side == Side.Long:
                    pnl = float(fill_amt) * (float(pc) - entry_price) * c_mult
                else:
                    pnl = float(fill_amt) * (entry_price - float(pc)) * c_mult
                sim_balance += float(pnl)

                fee_paid = float(fill_amt) * float(pc) * c_mult * float(maker_fee)
                if math.isfinite(fee_paid) and fee_paid > 0.0:
                    sim_balance -= float(fee_paid)

                typ_str = _order_type_to_str(pbr, int(tc))
                pos = _apply_fill_to_position(position=pos, fill_qty=float(q_eff), fill_price=float(pc))
                pos = Position(size=_snap_pos_size(float(pos.size)), price=float(pos.price))
                ev = {
                    "timestamp": pd.to_datetime(ts),
                    "event": "close",
                    "qty": float(q_eff),
                    "price": float(pc),
                    "order_type": str(typ_str),
                    "fee_paid": float(fee_paid),
                    "wallet_balance": float(sim_balance),
                    "pos_size": float(pos.size),
                }
                events.append(ev)
                candle_fills.append(ev)
                filled_this_candle = True

        # Process entries after closes.
        if trading_active and pending_entries:
            entries_to_process = [e for e in pending_entries if _order_would_fill_is_buy(_entry_is_buy(), e[1], low_px, high_px)]
            for qe, pe, te_id in entries_to_process:
                if len(events) >= int(max_orders):
                    break
                if qe == 0.0 or pe <= 0.0:
                    continue

                fee_paid = abs(float(qe)) * float(pe) * c_mult * float(maker_fee)
                if math.isfinite(fee_paid) and fee_paid > 0.0:
                    sim_balance -= float(fee_paid)
                pos = _apply_fill_to_position(position=pos, fill_qty=float(qe), fill_price=float(pe))
                pos = Position(size=_snap_pos_size(float(pos.size)), price=float(pos.price))
                ev = {
                    "timestamp": pd.to_datetime(ts),
                    "event": "entry",
                    "qty": float(qe),
                    "price": float(pe),
                    "order_type": str(_order_type_to_str(pbr, int(te_id))),
                    "fee_paid": float(fee_paid),
                    "wallet_balance": float(sim_balance),
                    "pos_size": float(pos.size),
                }
                events.append(ev)
                candle_fills.append(ev)
                filled_this_candle = True

        if filled_this_candle:
            tb = _pb7_reset_trailing_bundle(tb_local)
        else:
            tb = _pb7_update_trailing_bundle_with_candle(tb_local, float(high_px), float(low_px), float(close_px))

        if (i % fe) == 0:
            # Also compute POST-candle pending orders (state used for the *next* candle).
            pending_entries_post: list[dict] = []
            pending_closes_post: list[dict] = []
            if trading_active:
                try:
                    vol_next = float(row.get("volatility", vol) or 0.0)
                except Exception:
                    vol_next = float(vol)

                try:
                    ema0n = float(row.get("ema_0", close_px) or close_px)
                    ema1n = float(row.get("ema_1", close_px) or close_px)
                    ema2n = float(row.get("ema_2", close_px) or close_px)
                except Exception:
                    ema0n = float(close_px)
                    ema1n = float(close_px)
                    ema2n = float(close_px)

                ema_lower_next = float(min(ema0n, ema1n, ema2n))
                ema_upper_next = float(max(ema0n, ema1n, ema2n))

                sp_post = StateParams(
                    balance=float(sim_balance),
                    order_book=OrderBook(bid=float(close_px), ask=float(close_px)),
                    ema_bands=EmaBands(lower=float(ema_lower_next), upper=float(ema_upper_next)),
                    entry_volatility_logrange_ema_1h=float(vol_next),
                )

                try:
                    sp_post_json = json.dumps(asdict(sp_post), sort_keys=True)
                    tb_post_json = json.dumps(asdict(tb), sort_keys=True)

                    raw_entries_post = _calc_entries_rust_cached(
                        pb7_src,
                        int(side.value),
                        ep_json,
                        sp_post_json,
                        tb_post_json,
                        bp_json,
                        float(pos.size),
                        float(pos.price),
                    )
                    entries_post: list[tuple[float, float, int]] = []
                    for q, p, t in (raw_entries_post or []):
                        try:
                            entries_post.append((float(q), float(p), int(t)))
                        except Exception:
                            continue
                    pending_entries_post = _decode_rust_orders_for_debug(pbr, entries_post)

                    closes_post: list[tuple[float, float, int]] = []
                    if _pos_has_side_position(pos) and float(pos.price) > 0.0:
                        raw_closes_post = _calc_closes_rust_cached(
                            pb7_src,
                            int(side.value),
                            ep_json,
                            sp_post_json,
                            tb_post_json,
                            bp_json,
                            float(pos.size),
                            float(pos.price),
                        )
                        for q, p, t in (raw_closes_post or []):
                            try:
                                closes_post.append((float(q), float(p), int(t)))
                            except Exception:
                                continue
                    pending_closes_post = _decode_rust_orders_for_debug(pbr, closes_post)
                except Exception:
                    pending_entries_post = []
                    pending_closes_post = []

            frames.append(
                {
                    "timestamp": pd.to_datetime(ts),
                    "trading_active": bool(trading_active),
                    "candle": {"open": open_px, "high": high_px, "low": low_px, "close": close_px},
                    "pending_entries": _decode_rust_orders_for_debug(pbr, pending_entries),
                    "pending_closes": _decode_rust_orders_for_debug(pbr, pending_closes),
                    "pending_entries_post": list(pending_entries_post),
                    "pending_closes_post": list(pending_closes_post),
                    "fills": list(candle_fills),
                    "tb_before": tb_before,
                    "tb_after": asdict(tb),
                    "pos_before": pos_before,
                    "pos_after": {"size": float(pos.size), "price": float(pos.price)},
                    "balance_before": bal_before,
                    "balance_after": float(sim_balance),
                }
            )

    return (events, frames)


def _simulate_backtest_over_historical_candles_pair_core(
    *,
    pbr,
    pb7_src: str,
    side_for_frames: Side,
    candles: pd.DataFrame,
    exchange_params: ExchangeParams,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
    starting_position_long: Position,
    starting_position_short: Position,
    balance: float,
    maker_fee: float = 0.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
    capture_frames: bool = False,
    frame_every_n_candles: int = 1,
    capture_frames_from_time: Optional[pd.Timestamp] = None,
    include_viz_grids: bool = False,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Mode B candle-walk core using PB7 orchestrator (JSON API).

    This is the "movie builder" variant of Mode B which must match PB7 backtest semantics:
    - float32-backed candles (strict inequality fills)
    - orchestrator-driven open orders (compute_ideal_orders_json)
    - closes before entries
    - shared balance (long+short)

    Returns: (events_long, events_short, frames_for_selected_side)
    """
    events_long: list[dict] = []
    events_short: list[dict] = []
    frames: list[dict] = []

    if candles is None or candles.empty:
        return events_long, events_short, frames

    try:
        max_orders_i = int(max_orders)
    except Exception:
        max_orders_i = 0
    if max_orders_i < 0:
        max_orders_i = 0

    try:
        max_candles_i = int(max_candles)
    except Exception:
        max_candles_i = 0
    if max_candles_i < 0:
        max_candles_i = 0

    if max_candles_i > 0 and len(candles) > max_candles_i:
        candles = candles.iloc[:max_candles_i].copy()
    else:
        candles = candles.copy()

    # Parity note (PB7 vs Mode B): PB7 backtest uses f32-backed candle data.
    # See `_simulate_backtest_over_historical_candles_pair` for details.
    for col in ("open", "high", "low", "close", "volume"):
        if col in candles.columns:
            try:
                candles[col] = candles[col].astype(np.float32)
            except Exception:
                pass

    ema_df = _prepare_orchestrator_ema_df(candles, bot_params_long=bot_params_long, bot_params_short=bot_params_short)

    if include_viz_grids:
        try:
            ep_json = json.dumps(asdict(exchange_params), sort_keys=True)
        except Exception:
            ep_json = "{}"
        try:
            bp_json_long = json.dumps(asdict(bot_params_long), sort_keys=True)
        except Exception:
            bp_json_long = "{}"
        try:
            bp_json_short = json.dumps(asdict(bot_params_short), sort_keys=True)
        except Exception:
            bp_json_short = "{}"

        def _calc_full_grids_for_viz(*, side: Side, px: float, ema_row: pd.Series, pos: Position, tb: TrailingPriceBundle, bal: float):
            try:
                if side == Side.Long:
                    ema0 = float(ema_row.get("ema_l0", px) or px)
                    ema1 = float(ema_row.get("ema_l1", px) or px)
                    ema2 = float(ema_row.get("ema_l2", px) or px)
                    vol = float(ema_row.get("h1_lr_ema_l", 0.0) or 0.0)
                    bp_json = bp_json_long
                else:
                    ema0 = float(ema_row.get("ema_s0", px) or px)
                    ema1 = float(ema_row.get("ema_s1", px) or px)
                    ema2 = float(ema_row.get("ema_s2", px) or px)
                    vol = float(ema_row.get("h1_lr_ema_s", 0.0) or 0.0)
                    bp_json = bp_json_short

                ema_lower = float(min(ema0, ema1, ema2))
                ema_upper = float(max(ema0, ema1, ema2))
                sp = StateParams(
                    balance=float(bal),
                    order_book=OrderBook(bid=float(px), ask=float(px)),
                    ema_bands=EmaBands(lower=float(ema_lower), upper=float(ema_upper)),
                    entry_volatility_logrange_ema_1h=float(vol),
                )
                sp_json = json.dumps(asdict(sp), sort_keys=True)
                tb_json = json.dumps(asdict(tb), sort_keys=True)

                raw_entries = _calc_entries_rust_cached(
                    pb7_src,
                    int(side.value),
                    ep_json,
                    sp_json,
                    tb_json,
                    bp_json,
                    float(pos.size),
                    float(pos.price),
                )
                entries: list[tuple[float, float, int]] = []
                for q, p, t in (raw_entries or []):
                    try:
                        entries.append((float(q), float(p), int(t)))
                    except Exception:
                        continue
                decoded_entries = _decode_rust_orders_for_debug(pbr, entries)

                closes: list[tuple[float, float, int]] = []
                if float(pos.size) != 0.0 and float(pos.price) > 0.0:
                    raw_closes = _calc_closes_rust_cached(
                        pb7_src,
                        int(side.value),
                        ep_json,
                        sp_json,
                        tb_json,
                        bp_json,
                        float(pos.size),
                        float(pos.price),
                    )
                    for q, p, t in (raw_closes or []):
                        try:
                            closes.append((float(q), float(p), int(t)))
                        except Exception:
                            continue
                decoded_closes = _decode_rust_orders_for_debug(pbr, closes)
                return decoded_entries, decoded_closes
            except Exception:
                return [], []
    else:
        def _calc_full_grids_for_viz(*, side: Side, px: float, ema_row: pd.Series, pos: Position, tb: TrailingPriceBundle, bal: float):
            return [], []

    sim_balance = float(balance)
    pos_long = Position(size=float(starting_position_long.size), price=float(starting_position_long.price))
    pos_short = Position(size=float(starting_position_short.size), price=float(starting_position_short.price))

    maker_fee = float(maker_fee or 0.0)
    if not math.isfinite(maker_fee) or maker_fee < 0.0:
        maker_fee = 0.0

    qty_step = float(getattr(exchange_params, "qty_step", 0.0) or 0.0)
    price_step = float(getattr(exchange_params, "price_step", 0.0) or 0.0)
    c_mult = float(getattr(exchange_params, "c_mult", 1.0) or 1.0)
    if not math.isfinite(qty_step) or qty_step < 0.0:
        qty_step = 0.0
    if not math.isfinite(price_step) or price_step < 0.0:
        price_step = 0.0
    if not math.isfinite(c_mult) or c_mult <= 0.0:
        c_mult = 1.0

    # PB7 `TrailingPriceBundle::default()` uses f64::MAX sentinels.
    TRAILING_INF = float(getattr(sys, "float_info", None).max) if getattr(sys, "float_info", None) else 1.7976931348623157e308
    tb_long = TrailingPriceBundle(float(TRAILING_INF), 0.0, 0.0, float(TRAILING_INF))
    tb_short = TrailingPriceBundle(float(TRAILING_INF), 0.0, 0.0, float(TRAILING_INF))
    trailing_enabled_long = _any_trailing_enabled_for_backtest(bot_params_long)
    trailing_enabled_short = _any_trailing_enabled_for_backtest(bot_params_short)

    idx_list = list(candles.index)
    if len(idx_list) < 2:
        return events_long, events_short, frames

    trade_start_time_pd = pd.to_datetime(trade_start_time) if trade_start_time is not None else None
    capture_from_pd = pd.to_datetime(capture_frames_from_time) if capture_frames_from_time is not None else None

    pnl_cumsum_running = 0.0
    pnl_cumsum_max = 0.0

    def _pb7_update_trailing_bundle_with_candle(bundle: TrailingPriceBundle, high: float, low: float, close: float) -> TrailingPriceBundle:
        if not (math.isfinite(high) and math.isfinite(low) and math.isfinite(close)):
            return bundle
        if float(low) < float(bundle.min_since_open):
            bundle.min_since_open = float(low)
            bundle.max_since_min = float(close)
        else:
            bundle.max_since_min = float(max(float(bundle.max_since_min), float(high)))
        if float(high) > float(bundle.max_since_open):
            bundle.max_since_open = float(high)
            bundle.min_since_max = float(close)
        else:
            bundle.min_since_max = float(min(float(bundle.min_since_max), float(low)))
        return bundle

    def _pb7_reset_trailing_bundle(bundle: TrailingPriceBundle) -> TrailingPriceBundle:
        bundle.min_since_open = float(TRAILING_INF)
        bundle.max_since_min = 0.0
        bundle.max_since_open = 0.0
        bundle.min_since_max = float(TRAILING_INF)
        return bundle

    def _order_filled(low: float, high: float, qty: float, price: float, order_type: Optional[str] = None) -> bool:
        # Parity note (PB7 vs Mode B):
        # - PB7 candles are f32-backed.
        # - PB7 compares candle bounds (as f64 converted from f32) against order prices (f64).
        # - Fill checks are strict (< and >).
        try:
            low = float(np.float32(float(low)))
            high = float(np.float32(float(high)))
        except Exception:
            pass

        if qty > 0.0:
            return float(low) < float(price)
        if qty < 0.0:
            return float(high) > float(price)
        return False

    def _effective_min_cost(close_price: float) -> float:
        try:
            q = float(getattr(exchange_params, "min_qty", 0.0) or 0.0)
            mc = float(getattr(exchange_params, "min_cost", 0.0) or 0.0)
        except Exception:
            q, mc = 0.0, 0.0
        try:
            return float(max(float(pbr.qty_to_cost(float(q), float(close_price), float(c_mult))), float(mc)))
        except Exception:
            return float(max(abs(q) * float(close_price) * float(c_mult), mc))

    bp_long_symbol_dict = _bot_params_dict_for_orchestrator_single_symbol(bot_params_long, enabled=True)
    bp_short_symbol_dict = _bot_params_dict_for_orchestrator_single_symbol(bot_params_short, enabled=True)
    bp_long_master_dict = dict(bp_long_symbol_dict)
    bp_short_master_dict = dict(bp_short_symbol_dict)
    try:
        bp_long_master_dict["n_positions"] = int(min(int(bp_long_master_dict.get("n_positions") or 0), 1))
    except Exception:
        pass
    try:
        bp_short_master_dict["n_positions"] = int(min(int(bp_short_master_dict.get("n_positions") or 0), 1))
    except Exception:
        pass

    def _unstuck_allowance() -> tuple[float, float]:
        def _one(bp: BotParams) -> float:
            try:
                pct = float(getattr(bp, "unstuck_loss_allowance_pct", 0.0) or 0.0)
                total_wel = float(getattr(bp, "total_wallet_exposure_limit", 0.0) or 0.0)
            except Exception:
                pct, total_wel = 0.0, 0.0
            if pct <= 0.0 or total_wel <= 0.0:
                return 0.0
            try:
                return float(
                    pbr.calc_auto_unstuck_allowance(
                        float(sim_balance),
                        float(pct) * float(total_wel),
                        float(pnl_cumsum_max),
                        float(pnl_cumsum_running),
                    )
                )
            except Exception:
                return 0.0

        return (_one(bot_params_long), _one(bot_params_short))

    def _compute_orch_orders_pair(
        *,
        ob_price: float,
        ema_row: pd.Series,
        pos_l: Position,
        pos_s: Position,
        tb_l: TrailingPriceBundle,
        tb_s: TrailingPriceBundle,
        next_low: float,
        next_high: float,
        tradable_now: bool,
        tradable_next: bool,
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        ul, us = _unstuck_allowance()

        m1_close: list[list[float]] = []
        h1_log_range: list[list[float]] = []
        m1_volume: list[list[float]] = []
        m1_log_range: list[list[float]] = []

        def _append_ema_close(bp: BotParams, prefix: str):
            span0 = float(getattr(bp, "ema_span_0", 1.0) or 1.0)
            span1 = float(getattr(bp, "ema_span_1", 1.0) or 1.0)
            span2 = float(max(1.0, float(span0 * span1) ** 0.5))
            span0 = max(1.0, float(span0))
            span1 = max(1.0, float(span1))
            span2 = max(1.0, float(span2))
            if prefix == "l":
                v0 = float(ema_row.get("ema_l0", ob_price) or ob_price)
                v1 = float(ema_row.get("ema_l1", ob_price) or ob_price)
                v2 = float(ema_row.get("ema_l2", ob_price) or ob_price)
            else:
                v0 = float(ema_row.get("ema_s0", ob_price) or ob_price)
                v1 = float(ema_row.get("ema_s1", ob_price) or ob_price)
                v2 = float(ema_row.get("ema_s2", ob_price) or ob_price)
            pairs: list[tuple[float, float]] = [(span0, v0), (span1, v1), (span2, v2)]
            pairs.sort(key=lambda x: x[0])
            for s, v in pairs:
                m1_close.append([float(s), float(v)])

        _append_ema_close(bot_params_long, "l")
        _append_ema_close(bot_params_short, "s")

        m1_volume.append([
            float(getattr(bot_params_long, "filter_volume_ema_span", 1.0) or 1.0),
            float(ema_row.get("vol_ema_l", 0.0) or 0.0),
        ])
        m1_volume.append([
            float(getattr(bot_params_short, "filter_volume_ema_span", 1.0) or 1.0),
            float(ema_row.get("vol_ema_s", 0.0) or 0.0),
        ])
        m1_log_range.append([
            float(getattr(bot_params_long, "filter_volatility_ema_span", 1.0) or 1.0),
            float(ema_row.get("lr_ema_l", 0.0) or 0.0),
        ])
        m1_log_range.append([
            float(getattr(bot_params_short, "filter_volatility_ema_span", 1.0) or 1.0),
            float(ema_row.get("lr_ema_s", 0.0) or 0.0),
        ])

        span_h_l = float(getattr(bot_params_long, "entry_volatility_ema_span_hours", 0.0) or 0.0)
        if span_h_l > 0.0:
            h1_log_range.append([float(span_h_l), float(ema_row.get("h1_lr_ema_l", 0.0) or 0.0)])
        span_h_s = float(getattr(bot_params_short, "entry_volatility_ema_span_hours", 0.0) or 0.0)
        if span_h_s > 0.0:
            h1_log_range.append([float(span_h_s), float(ema_row.get("h1_lr_ema_s", 0.0) or 0.0)])

        if not bool(tradable_next):
            next_low = 0.0
            next_high = 0.0

        ob_p = float(ob_price)
        if not math.isfinite(ob_p) or ob_p <= 0.0:
            ob_p = float(np.finfo("float64").eps)

        inp = {
            "balance": float(sim_balance),
            "global": {
                "filter_by_min_effective_cost": False,
                "unstuck_allowance_long": float(ul),
                "unstuck_allowance_short": float(us),
                "sort_global": False,
                "global_bot_params": {"long": bp_long_master_dict, "short": bp_short_master_dict},
            },
            "symbols": [
                {
                    "symbol_idx": 0,
                    "order_book": {"bid": float(ob_p), "ask": float(ob_p)},
                    "exchange": {
                        "qty_step": float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
                        "price_step": float(getattr(exchange_params, "price_step", 0.0) or 0.0),
                        "min_qty": float(getattr(exchange_params, "min_qty", 0.0) or 0.0),
                        "min_cost": float(getattr(exchange_params, "min_cost", 0.0) or 0.0),
                        "c_mult": float(c_mult),
                    },
                    "tradable": bool(tradable_now),
                    "next_candle": {"low": float(next_low), "high": float(next_high), "tradable": bool(tradable_next)},
                    "effective_min_cost": float(_effective_min_cost(float(ob_p))),
                    "emas": {
                        "m1": {"close": m1_close, "volume": m1_volume, "log_range": m1_log_range},
                        "h1": {"close": [], "volume": [], "log_range": h1_log_range},
                    },
                    "long": {
                        "mode": None,
                        "position": {"size": float(pos_l.size), "price": float(pos_l.price)},
                        "trailing": asdict(tb_l),
                        "bot_params": bp_long_symbol_dict,
                    },
                    "short": {
                        "mode": None,
                        "position": {"size": float(pos_s.size), "price": float(pos_s.price)},
                        "trailing": asdict(tb_s),
                        "bot_params": bp_short_symbol_dict,
                    },
                }
            ],
            "peek_hints": None,
        }

        out_json = pbr.compute_ideal_orders_json(json.dumps(inp))
        out = json.loads(out_json)
        orders = out.get("orders") or []

        l_entries: list[dict] = []
        l_closes: list[dict] = []
        s_entries: list[dict] = []
        s_closes: list[dict] = []
        for o in orders:
            try:
                if int(o.get("symbol_idx", -1)) != 0:
                    continue
                pside = str(o.get("pside"))
                ot = str(o.get("order_type") or "")
                rec = {"qty": float(o.get("qty") or 0.0), "price": float(o.get("price") or 0.0), "order_type": ot}
                if pside == "long":
                    (l_closes if ot.startswith("close_") else l_entries).append(rec)
                elif pside == "short":
                    (s_closes if ot.startswith("close_") else s_entries).append(rec)
            except Exception:
                continue

        return l_entries, l_closes, s_entries, s_closes

    fe = max(1, int(frame_every_n_candles or 1))
    candle_cap = int(max_candles_i) if int(max_candles_i) > 0 else int(len(idx_list))
    total_steps = max(1, min(len(idx_list), int(candle_cap)) - 1)
    progress_every = max(1, int(total_steps // 200))

    for i in range(1, min(len(idx_list), int(candle_cap))):
        if progress_cb is not None and (i == 1 or i == total_steps or (i % progress_every == 0)):
            try:
                progress_cb(min(1.0, float(i) / float(total_steps)), f"Simulating candles {i}/{total_steps}")
            except Exception:
                pass

        ts = idx_list[i]
        row = candles.loc[ts]
        prev_ts = idx_list[i - 1]
        prev_row = candles.loc[prev_ts]
        ema_prev = ema_df.loc[prev_ts] if prev_ts in ema_df.index else None

        if max_orders_i > 0 and (len(events_long) + len(events_short) >= int(max_orders_i)):
            break

        trading_active = True
        prev_trading_active = True
        if trade_start_time_pd is not None:
            try:
                trading_active = pd.to_datetime(ts) >= trade_start_time_pd
            except Exception:
                trading_active = True
            try:
                prev_trading_active = pd.to_datetime(prev_ts) >= trade_start_time_pd
            except Exception:
                prev_trading_active = True

        def _f32(x: float) -> float:
            try:
                return float(np.float32(float(x)))
            except Exception:
                return float(x)

        open_px = _f32(row.get("open", row["close"]))
        close_px = _f32(row["close"])
        low_px = _f32(row["low"])
        high_px = _f32(row["high"])
        prev_close_px = _f32(prev_row.get("close", close_px) or close_px)

        # next candle hint (for current step relative to prev_ts) = current candle range
        next_low = float(low_px)
        next_high = float(high_px)

        tb_l_local = copy.deepcopy(tb_long)
        tb_s_local = copy.deepcopy(tb_short)
        filled_long = False
        filled_short = False

        if trading_active and ema_prev is not None:
            l_entries, l_closes, s_entries, s_closes = _compute_orch_orders_pair(
                ob_price=float(prev_close_px),
                ema_row=ema_prev,
                pos_l=pos_long,
                pos_s=pos_short,
                tb_l=tb_l_local,
                tb_s=tb_s_local,
                next_low=next_low,
                next_high=next_high,
                tradable_now=bool(prev_trading_active),
                tradable_next=bool(trading_active),
            )
        else:
            l_entries, l_closes, s_entries, s_closes = ([], [], [], [])

        # Select pending orders for frame (pre-candle)
        if side_for_frames == Side.Long:
            pending_entries_pre = list(l_entries or [])
            pending_closes_pre = list(l_closes or [])
            pos_before = {"size": float(pos_long.size), "price": float(pos_long.price)}
        else:
            pending_entries_pre = list(s_entries or [])
            pending_closes_pre = list(s_closes or [])
            pos_before = {"size": float(pos_short.size), "price": float(pos_short.price)}
        bal_before = float(sim_balance)
        tb_before = asdict(tb_long if side_for_frames == Side.Long else tb_short)
        candle_fills: list[dict] = []

        # Full-grid ladders for visualization (pre-candle state)
        viz_entries_pre: list[dict] = []
        viz_closes_pre: list[dict] = []
        if capture_frames and include_viz_grids and trading_active and ema_prev is not None:
            if side_for_frames == Side.Long:
                viz_entries_pre, viz_closes_pre = _calc_full_grids_for_viz(
                    side=Side.Long,
                    px=float(prev_close_px),
                    ema_row=ema_prev,
                    pos=pos_long,
                    tb=copy.deepcopy(tb_long),
                    bal=float(sim_balance),
                )
            else:
                viz_entries_pre, viz_closes_pre = _calc_full_grids_for_viz(
                    side=Side.Short,
                    px=float(prev_close_px),
                    ema_row=ema_prev,
                    pos=pos_short,
                    tb=copy.deepcopy(tb_short),
                    bal=float(sim_balance),
                )

        # --- closes first (PB7) ---
        if trading_active and l_closes and float(pos_long.size) > 0.0:
            for o in l_closes:
                if max_orders_i > 0 and (len(events_long) + len(events_short) >= int(max_orders_i)):
                    break
                # PB7: if a close removes the position, skip remaining closes this candle.
                if float(pos_long.size) <= 0.0:
                    break
                q = float(o.get("qty") or 0.0)
                p = float(o.get("price") or 0.0)
                ot = str(o.get("order_type") or "")
                if q == 0.0 or p <= 0.0:
                    continue
                if not _order_filled(low_px, high_px, q, p, ot):
                    continue

                adj_qty = float(q)
                try:
                    new_psize = float(pbr.round_(float(pos_long.size) + float(adj_qty), float(qty_step))) if qty_step > 0.0 else float(pos_long.size) + float(adj_qty)
                    if new_psize < 0.0:
                        new_psize = 0.0
                        adj_qty = -float(pos_long.size)
                except Exception:
                    new_psize = float(pos_long.size) + float(adj_qty)

                fee_paid = -float(pbr.qty_to_cost(float(adj_qty), float(p), float(c_mult))) * float(maker_fee)
                pnl = float(pbr.calc_pnl_long(float(pos_long.price), float(p), float(adj_qty), float(c_mult)))
                pnl_cumsum_running += float(pnl)
                pnl_cumsum_max = max(float(pnl_cumsum_max), float(pnl_cumsum_running))
                sim_balance += float(pnl) + float(fee_paid)

                if float(new_psize) == 0.0:
                    pos_long = Position(size=0.0, price=0.0)
                else:
                    pos_long = Position(size=float(new_psize), price=float(pos_long.price))

                ev = {
                    "timestamp": pd.to_datetime(ts),
                    "event": "close",
                    "qty": float(adj_qty),
                    "price": float(p),
                    "order_type": ot,
                    "fee_paid": float(fee_paid),
                    "wallet_balance": float(sim_balance),
                    "pos_size": float(pos_long.size),
                    "pos_price": float(pos_long.price),
                    "pnl": float(pnl),
                }
                events_long.append(ev)
                if side_for_frames == Side.Long:
                    candle_fills.append(ev)
                filled_long = True

                if float(pos_long.size) == 0.0:
                    break

        if trading_active and s_closes and float(pos_short.size) < 0.0:
            for o in s_closes:
                if max_orders_i > 0 and (len(events_long) + len(events_short) >= int(max_orders_i)):
                    break
                # PB7: if a close removes the position, skip remaining closes this candle.
                if float(pos_short.size) >= 0.0:
                    break
                q = float(o.get("qty") or 0.0)
                p = float(o.get("price") or 0.0)
                ot = str(o.get("order_type") or "")
                if q == 0.0 or p <= 0.0:
                    continue
                if not _order_filled(low_px, high_px, q, p, ot):
                    continue

                adj_qty = float(q)
                try:
                    new_psize = float(pbr.round_(float(pos_short.size) + float(adj_qty), float(qty_step))) if qty_step > 0.0 else float(pos_short.size) + float(adj_qty)
                    if new_psize > 0.0:
                        new_psize = 0.0
                        adj_qty = abs(float(pos_short.size))
                except Exception:
                    new_psize = float(pos_short.size) + float(adj_qty)

                fee_paid = -float(pbr.qty_to_cost(float(adj_qty), float(p), float(c_mult))) * float(maker_fee)
                pnl = float(pbr.calc_pnl_short(float(pos_short.price), float(p), float(adj_qty), float(c_mult)))
                pnl_cumsum_running += float(pnl)
                pnl_cumsum_max = max(float(pnl_cumsum_max), float(pnl_cumsum_running))
                sim_balance += float(pnl) + float(fee_paid)

                if float(new_psize) == 0.0:
                    pos_short = Position(size=0.0, price=0.0)
                else:
                    pos_short = Position(size=float(new_psize), price=float(pos_short.price))

                ev = {
                    "timestamp": pd.to_datetime(ts),
                    "event": "close",
                    "qty": float(adj_qty),
                    "price": float(p),
                    "order_type": ot,
                    "fee_paid": float(fee_paid),
                    "wallet_balance": float(sim_balance),
                    "pos_size": float(pos_short.size),
                    "pos_price": float(pos_short.price),
                    "pnl": float(pnl),
                }
                events_short.append(ev)
                if side_for_frames == Side.Short:
                    candle_fills.append(ev)
                filled_short = True

                if float(pos_short.size) == 0.0:
                    break

        # --- entries after closes ---
        if trading_active and l_entries:
            for o in l_entries:
                if max_orders_i > 0 and (len(events_long) + len(events_short) >= int(max_orders_i)):
                    break
                q = float(o.get("qty") or 0.0)
                p = float(o.get("price") or 0.0)
                ot = str(o.get("order_type") or "")
                if q == 0.0 or p <= 0.0:
                    continue
                if not _order_filled(low_px, high_px, q, p, ot):
                    continue

                fee_paid = -float(pbr.qty_to_cost(float(q), float(p), float(c_mult))) * float(maker_fee)
                sim_balance += float(fee_paid)
                try:
                    new_psize, new_pprice = pbr.calc_new_psize_pprice(
                        float(pos_long.size),
                        float(pos_long.price),
                        float(q),
                        float(p),
                        float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
                    )
                    pos_long = Position(size=float(new_psize), price=float(new_pprice))
                except Exception:
                    pos_long = _apply_fill_to_position(position=pos_long, fill_qty=float(q), fill_price=float(p))

                ev = {
                    "timestamp": pd.to_datetime(ts),
                    "event": "entry",
                    "qty": float(q),
                    "price": float(p),
                    "order_type": ot,
                    "fee_paid": float(fee_paid),
                    "wallet_balance": float(sim_balance),
                    "pos_size": float(pos_long.size),
                    "pos_price": float(pos_long.price),
                    "pnl": 0.0,
                }
                events_long.append(ev)
                if side_for_frames == Side.Long:
                    candle_fills.append(ev)
                filled_long = True

        if trading_active and s_entries:
            for o in s_entries:
                if max_orders_i > 0 and (len(events_long) + len(events_short) >= int(max_orders_i)):
                    break
                q = float(o.get("qty") or 0.0)
                p = float(o.get("price") or 0.0)
                ot = str(o.get("order_type") or "")
                if q == 0.0 or p <= 0.0:
                    continue
                if not _order_filled(low_px, high_px, q, p, ot):
                    continue

                fee_paid = -float(pbr.qty_to_cost(float(q), float(p), float(c_mult))) * float(maker_fee)
                sim_balance += float(fee_paid)
                try:
                    new_psize, new_pprice = pbr.calc_new_psize_pprice(
                        float(pos_short.size),
                        float(pos_short.price),
                        float(q),
                        float(p),
                        float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
                    )
                    pos_short = Position(size=float(new_psize), price=float(new_pprice))
                except Exception:
                    pos_short = _apply_fill_to_position(position=pos_short, fill_qty=float(q), fill_price=float(p))

                ev = {
                    "timestamp": pd.to_datetime(ts),
                    "event": "entry",
                    "qty": float(q),
                    "price": float(p),
                    "order_type": ot,
                    "fee_paid": float(fee_paid),
                    "wallet_balance": float(sim_balance),
                    "pos_size": float(pos_short.size),
                    "pos_price": float(pos_short.price),
                    "pnl": 0.0,
                }
                events_short.append(ev)
                if side_for_frames == Side.Short:
                    candle_fills.append(ev)
                filled_short = True

        # trailing updates are per-pside
        if trailing_enabled_long and float(pos_long.size) > 0.0:
            if filled_long:
                tb_long = _pb7_reset_trailing_bundle(tb_l_local)
            else:
                tb_long = _pb7_update_trailing_bundle_with_candle(tb_l_local, float(high_px), float(low_px), float(close_px))
        else:
            tb_long = tb_l_local

        if trailing_enabled_short and float(pos_short.size) < 0.0:
            if filled_short:
                tb_short = _pb7_reset_trailing_bundle(tb_s_local)
            else:
                tb_short = _pb7_update_trailing_bundle_with_candle(tb_s_local, float(high_px), float(low_px), float(close_px))
        else:
            tb_short = tb_s_local

        # Capture frame (optionally skip early warmup frames)
        if capture_frames and (i % fe) == 0:
            if capture_from_pd is None or pd.to_datetime(ts) >= capture_from_pd:
                # Compute POST-candle pending orders for immediate rendering.
                pending_entries_post: list[dict] = []
                pending_closes_post: list[dict] = []

                # Use inputs matching the next candle step (i+1) if available.
                if i + 1 < len(idx_list):
                    next_ts = idx_list[i + 1]
                    next_row = candles.loc[next_ts]
                    next_low2 = float(_f32(next_row["low"]))
                    next_high2 = float(_f32(next_row["high"]))
                    next_trading_active = True
                    if trade_start_time_pd is not None:
                        try:
                            next_trading_active = pd.to_datetime(next_ts) >= trade_start_time_pd
                        except Exception:
                            next_trading_active = True
                else:
                    next_low2 = float(low_px)
                    next_high2 = float(high_px)
                    next_trading_active = bool(trading_active)

                ema_now = ema_df.loc[ts] if ts in ema_df.index else None
                if bool(next_trading_active) and ema_now is not None:
                    try:
                        l_e2, l_c2, s_e2, s_c2 = _compute_orch_orders_pair(
                            ob_price=float(close_px),
                            ema_row=ema_now,
                            pos_l=pos_long,
                            pos_s=pos_short,
                            tb_l=copy.deepcopy(tb_long),
                            tb_s=copy.deepcopy(tb_short),
                            next_low=float(next_low2),
                            next_high=float(next_high2),
                            tradable_now=bool(trading_active),
                            tradable_next=bool(next_trading_active),
                        )
                        if side_for_frames == Side.Long:
                            pending_entries_post = list(l_e2 or [])
                            pending_closes_post = list(l_c2 or [])
                        else:
                            pending_entries_post = list(s_e2 or [])
                            pending_closes_post = list(s_c2 or [])
                    except Exception:
                        pending_entries_post = []
                        pending_closes_post = []

                # Full-grid ladders for visualization (post-candle state)
                viz_entries_post: list[dict] = []
                viz_closes_post: list[dict] = []
                if include_viz_grids and bool(next_trading_active) and ema_now is not None:
                    if side_for_frames == Side.Long:
                        viz_entries_post, viz_closes_post = _calc_full_grids_for_viz(
                            side=Side.Long,
                            px=float(close_px),
                            ema_row=ema_now,
                            pos=pos_long,
                            tb=copy.deepcopy(tb_long),
                            bal=float(sim_balance),
                        )
                    else:
                        viz_entries_post, viz_closes_post = _calc_full_grids_for_viz(
                            side=Side.Short,
                            px=float(close_px),
                            ema_row=ema_now,
                            pos=pos_short,
                            tb=copy.deepcopy(tb_short),
                            bal=float(sim_balance),
                        )

                if side_for_frames == Side.Long:
                    tb_after = asdict(tb_long)
                    pos_after = {"size": float(pos_long.size), "price": float(pos_long.price)}
                else:
                    tb_after = asdict(tb_short)
                    pos_after = {"size": float(pos_short.size), "price": float(pos_short.price)}

                frames.append(
                    {
                        "timestamp": pd.to_datetime(ts),
                        "trading_active": bool(trading_active),
                        "candle": {"open": float(open_px), "high": float(high_px), "low": float(low_px), "close": float(close_px)},
                        "pending_entries": list(pending_entries_pre),
                        "pending_closes": list(pending_closes_pre),
                        "pending_entries_post": list(pending_entries_post),
                        "pending_closes_post": list(pending_closes_post),
                        "viz_entries": list(viz_entries_pre),
                        "viz_closes": list(viz_closes_pre),
                        "viz_entries_post": list(viz_entries_post),
                        "viz_closes_post": list(viz_closes_post),
                        "fills": list(candle_fills),
                        "tb_before": tb_before,
                        "tb_after": tb_after,
                        "pos_before": pos_before,
                        "pos_after": pos_after,
                        "balance_before": float(bal_before),
                        "balance_after": float(sim_balance),
                    }
                )

    return events_long, events_short, frames


def _simulate_backtest_over_historical_candles_replay_orchestrator_pair(
    *,
    pbr,
    pb7_src: str,
    side_for_frames: Side,
    candles: pd.DataFrame,
    exchange_params: ExchangeParams,
    bot_params_long: BotParams,
    bot_params_short: BotParams,
    starting_position_long: Position,
    starting_position_short: Position,
    balance: float,
    maker_fee: float = 0.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
    frame_every_n_candles: int = 1,
    capture_frames_from_time: Optional[pd.Timestamp] = None,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Movie-builder wrapper around the shared Mode B core.

    Returns: (events_long, events_short, frames_for_selected_side).
    """
    return _simulate_backtest_over_historical_candles_pair_core(
        pbr=pbr,
        pb7_src=pb7_src,
        side_for_frames=side_for_frames,
        candles=candles,
        exchange_params=exchange_params,
        bot_params_long=bot_params_long,
        bot_params_short=bot_params_short,
        starting_position_long=starting_position_long,
        starting_position_short=starting_position_short,
        balance=balance,
        maker_fee=maker_fee,
        trade_start_time=trade_start_time,
        max_orders=max_orders,
        max_candles=max_candles,
        capture_frames=True,
        frame_every_n_candles=frame_every_n_candles,
        capture_frames_from_time=capture_frames_from_time,
        include_viz_grids=True,
        progress_cb=progress_cb,
    )


def render_replay_backtest_v7(*, replay_frames: list[dict], hist_df: pd.DataFrame, symbol: str, context_days: float = 5.0) -> None:
    if not replay_frames:
        st.write("No replay frames.")
        return

    idx = st.slider("Replay frame", min_value=0, max_value=len(replay_frames) - 1, value=0, step=1)
    fr = replay_frames[int(idx)]
    ts = pd.to_datetime(fr.get("timestamp"))

    ctx_start = ts - pd.Timedelta(days=float(context_days))
    try:
        df_ctx = hist_df.loc[(hist_df.index >= ctx_start) & (hist_df.index <= ts)].copy()
    except Exception:
        df_ctx = hist_df.copy()

    if df_ctx is None or df_ctx.empty:
        st.write("No candle data for replay window.")
        return

    # Keep payload small: resample plot source to ~300 candles in the context window.
    try:
        total_ctx_mins = float(context_days) * 1440.0
        opt_res_mins = int(total_ctx_mins / 300.0)
        if opt_res_mins < 1:
            opt_res_mins = 1
    except Exception:
        opt_res_mins = 1

    df_plot = df_ctx
    if opt_res_mins > 1:
        agg_dict = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        for c in df_plot.columns:
            if c not in agg_dict:
                agg_dict[c] = "last"
        try:
            df_plot = df_plot.resample(f"{opt_res_mins}min").agg(agg_dict).dropna()
        except Exception:
            df_plot = df_ctx

    def make_grid_trace(prices: list[float], color: str, name: str, x0, x1):
        if not prices:
            return go.Scatter(x=[], y=[])
        x_vals, y_vals = [], []
        for p in prices:
            x_vals.extend([x0, x1, None])
            y_vals.extend([p, p, None])
        return go.Scatter(x=x_vals, y=y_vals, mode="lines", line=dict(color=color, width=1, dash="dot"), name=name)

    # Prefer visualization grids (full ladders), fallback to active pending orders.
    pend_entries = list(
        fr.get("viz_entries_post")
        or fr.get("viz_entries")
        or fr.get("pending_entries_post")
        or fr.get("pending_entries")
        or []
    )
    pend_closes = list(
        fr.get("viz_closes_post")
        or fr.get("viz_closes")
        or fr.get("pending_closes_post")
        or fr.get("pending_closes")
        or []
    )
    entry_prices = [float(o.get("price")) for o in pend_entries if float(o.get("price", 0.0) or 0.0) > 0.0]
    close_prices = [float(o.get("price")) for o in pend_closes if float(o.get("price", 0.0) or 0.0) > 0.0]

    fill_rows = list(fr.get("fills") or [])
    fill_x = [ts for _ in fill_rows]
    fill_y = [float(r.get("price", 0.0) or 0.0) for r in fill_rows]
    fill_color = ["red" if str(r.get("event")) == "entry" else "green" for r in fill_rows]

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df_plot.index,
                open=df_plot["open"],
                high=df_plot["high"],
                low=df_plot["low"],
                close=df_plot["close"],
                name="Price",
            ),
            make_grid_trace(entry_prices, "rgba(255, 0, 0, 0.6)", "Entry Grid", df_plot.index[0], df_plot.index[-1]),
            make_grid_trace(close_prices, "rgba(0, 255, 0, 0.6)", "Close Grid", df_plot.index[0], df_plot.index[-1]),
            go.Scatter(x=fill_x, y=fill_y, mode="markers", marker=dict(size=10, color=fill_color), name="Fills"),
        ]
    )
    fig.update_layout(
        title=f"Replay: {symbol} @ {ts}",
        xaxis=dict(type="date", rangeslider=dict(visible=False)),
        height=800,
        margin=dict(l=50, r=50, t=50, b=50),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Minimal per-frame details
    st.caption(
        f"pos: {fr.get('pos_after', {})} | balance: {fr.get('balance_after', 0.0):.6f} | fills this candle: {len(fill_rows)}"
    )
    if fill_rows:
        sdf = pd.DataFrame(fill_rows)
        cols = [c for c in ["timestamp", "event", "qty", "price", "order_type", "fee_paid", "wallet_balance", "pos_size"] if c in sdf.columns]
        if cols:
            sdf = sdf[cols]
        st.dataframe(sdf, width="stretch")



def _try_get_pbr():
    if not is_pb7_installed():
        return None
    pb7_src_dir = os.path.join(pb7dir(), "src")
    try:
        return _get_passivbot_rust(pb7_src_dir)
    except Exception:
        return None


def _order_type_to_str(pbr, type_id: int) -> str:
    try:
        return str(pbr.order_type_id_to_snake(int(type_id)))
    except Exception:
        return str(type_id)


def _decode_rust_orders_for_debug(pbr, raw_orders):
    decoded = []
    if raw_orders is None:
        return decoded
    for qty, price, type_id in raw_orders:
        decoded.append(
            {
                "qty": float(qty),
                "price": float(price),
                "type_id": int(type_id),
                "type": _order_type_to_str(pbr, type_id),
            }
        )
    return decoded


def _render_debug_json(value) -> None:
    """Render debug values safely.

    Streamlit's `st.json()` may throw if passed `None` or other non-JSON types.
    """
    if value is None:
        st.write("n/a")
        return
    if isinstance(value, (dict, list)):
        st.json(value)
        return
    if isinstance(value, str):
        # Try parsing JSON strings for nicer display, else show as code.
        try:
            st.json(json.loads(value))
        except Exception:
            st.code(value)
        return
    st.write(value)


def _calc_potential_trailing_entry_prices_from_fullgrid(
    *,
    side: Side,
    bot_params: BotParams,
    exchange_params: ExchangeParams,
    balance: float,
    fullgrid_orders: list[Order],
) -> tuple[list[float], dict]:
    """Estimate which full-grid entry levels would belong to the *trailing* portion in PB7 semantics.

    PB7 (see `pb7/passivbot-rust/src/entries.rs::calc_next_entry_*`) uses `entry_trailing_grid_ratio` to
    partition *wallet exposure*, not price range.

    For GridFirst (`ratio < 0`):
      - grid orders are used while `wallet_exposure_ratio < 1.0 + ratio`
      - trailing takes over afterwards.

    We approximate this by walking the Rust-derived `gridonly` orders (full grid) and selecting those orders
    whose cumulative filled position would be at/above the grid-part exposure threshold.
    """
    ratio = float(bot_params.entry_trailing_grid_ratio)
    debug: dict = {
        "ratio": ratio,
        "fullgrid_count": len(fullgrid_orders or []),
        "qty_step": float(getattr(exchange_params, "qty_step", 0.0) or 0.0),
        "price_step": float(getattr(exchange_params, "price_step", 0.0) or 0.0),
        "c_mult": float(getattr(exchange_params, "c_mult", 1.0) or 1.0),
    }

    if ratio >= 0.0:
        return ([], debug)

    n_positions = int(getattr(bot_params, "n_positions", 1) or 1)
    total_wel = float(getattr(bot_params, "total_wallet_exposure_limit", 0.0) or 0.0)
    if n_positions <= 0 or total_wel <= 0.0:
        return ([], debug)

    # Rust uses per-position wallet_exposure_limit and applies risk allowance
    base_wel = total_wel / float(n_positions)
    allowance = max(0.0, float(getattr(bot_params, "risk_we_excess_allowance_pct", 0.0) or 0.0))
    base_wel_allow = base_wel * (1.0 + allowance) if base_wel > 0.0 else base_wel
    threshold_ratio = 1.0 + ratio  # e.g. -0.5 -> 0.5
    threshold_ratio = min(1.0, max(0.0, threshold_ratio))
    debug.update(
        {
            "total_wallet_exposure_limit": total_wel,
            "n_positions": n_positions,
            "base_wallet_exposure_limit": base_wel,
            "base_wallet_exposure_limit_with_allowance": base_wel_allow,
            "grid_part_wallet_exposure_ratio_threshold": threshold_ratio,
        }
    )

    bal = float(balance or 0.0)
    debug["balance"] = bal
    if bal <= 0.0:
        return ([], debug)
    c_mult = float(getattr(exchange_params, "c_mult", 1.0) or 1.0)
    if c_mult <= 0.0:
        c_mult = 1.0

    # Walk orders in the given sequence (gridonly output is already sequential from Rust).
    psize = 0.0
    pprice = 0.0
    cutoff_price: float | None = None
    cutoff_idx: int | None = None
    last_ratio: float | None = None

    for idx, o in enumerate(fullgrid_orders or []):
        try:
            qty = float(o.qty)
            price = float(o.price)
        except Exception:
            continue
        if qty == 0.0 or price <= 0.0:
            continue

        new_psize = psize + qty
        if new_psize == 0.0:
            new_pprice = 0.0
        elif psize == 0.0:
            new_pprice = price
        else:
            new_pprice = (psize * pprice + qty * price) / new_psize

        psize, pprice = new_psize, new_pprice

        # wallet_exposure = abs(psize) * pprice * c_mult / balance
        # ratio = wallet_exposure / (base_wel * (1 + allowance))
        wallet_exposure = (abs(psize) * pprice * c_mult) / bal
        we_ratio = (wallet_exposure / base_wel_allow) if base_wel_allow > 0.0 else 0.0
        last_ratio = float(we_ratio)

        # GridFirst semantics: grid is used while wallet_exposure_ratio < threshold_ratio.
        # The *last grid fill* is the one that first reaches/exceeds the threshold.
        if we_ratio >= threshold_ratio - 1e-12:
            cutoff_idx = idx
            cutoff_price = float(price)
            break

    debug.update(
        {
            "end_wallet_exposure_ratio": last_ratio,
            "gridfirst_cutoff_index": cutoff_idx,
            "gridfirst_cutoff_price": cutoff_price,
        }
    )
    return (([cutoff_price] if cutoff_price is not None else []), debug)


def _calc_next_entry_rust(
    *,
    pbr,
    side: Side,
    exchange_params: ExchangeParams,
    state_params: StateParams,
    trailing_bundle: TrailingPriceBundle,
    bot_params: BotParams,
    position: Position,
) -> dict:
    ep = exchange_params
    sp = state_params
    tb = trailing_bundle
    bp = bot_params

    if side == Side.Long:
        qty, price, typ = pbr.calc_next_entry_long_py(
            ep.qty_step,
            ep.price_step,
            ep.min_qty,
            ep.min_cost,
            ep.c_mult,
            bp.entry_grid_double_down_factor,
            bp.entry_grid_spacing_volatility_weight,
            bp.entry_grid_spacing_we_weight,
            bp.entry_grid_spacing_pct,
            bp.entry_initial_ema_dist,
            bp.entry_initial_qty_pct,
            (bp.entry_trailing_double_down_factor or bp.entry_grid_double_down_factor),
            bp.entry_trailing_grid_ratio,
            bp.entry_trailing_retracement_pct,
            bp.entry_trailing_retracement_we_weight,
            bp.entry_trailing_retracement_volatility_weight,
            bp.entry_trailing_threshold_pct,
            bp.entry_trailing_threshold_we_weight,
            bp.entry_trailing_threshold_volatility_weight,
            # NOTE: Rust pyfunction expects per-position wallet_exposure_limit.
            (bp.total_wallet_exposure_limit / float(bp.n_positions) if bp.n_positions else bp.total_wallet_exposure_limit),
            bp.risk_we_excess_allowance_pct,
            sp.balance,
            position.size,
            position.price,
            tb.min_since_open,
            tb.max_since_min,
            tb.max_since_open,
            tb.min_since_max,
            sp.ema_bands.lower,
            sp.entry_volatility_logrange_ema_1h,
            sp.order_book.bid,
        )
    else:
        qty, price, typ = pbr.calc_next_entry_short_py(
            ep.qty_step,
            ep.price_step,
            ep.min_qty,
            ep.min_cost,
            ep.c_mult,
            bp.entry_grid_double_down_factor,
            bp.entry_grid_spacing_volatility_weight,
            bp.entry_grid_spacing_we_weight,
            bp.entry_grid_spacing_pct,
            bp.entry_initial_ema_dist,
            bp.entry_initial_qty_pct,
            (bp.entry_trailing_double_down_factor or bp.entry_grid_double_down_factor),
            bp.entry_trailing_grid_ratio,
            bp.entry_trailing_retracement_pct,
            bp.entry_trailing_retracement_we_weight,
            bp.entry_trailing_retracement_volatility_weight,
            bp.entry_trailing_threshold_pct,
            bp.entry_trailing_threshold_we_weight,
            bp.entry_trailing_threshold_volatility_weight,
            (bp.total_wallet_exposure_limit / float(bp.n_positions) if bp.n_positions else bp.total_wallet_exposure_limit),
            bp.risk_we_excess_allowance_pct,
            sp.balance,
            position.size,
            position.price,
            tb.min_since_open,
            tb.max_since_min,
            tb.max_since_open,
            tb.min_since_max,
            sp.ema_bands.upper,
            sp.entry_volatility_logrange_ema_1h,
            sp.order_book.ask,
        )

    # `typ` may be an integer order-type ID; decode to the same string used elsewhere.
    typ_str: str
    try:
        typ_int = int(typ)
        typ_str = _order_type_to_str(pbr, typ_int)
    except Exception:
        typ_str = str(typ)

    return {
        "qty": float(qty),
        "price": float(price),
        "type": str(typ_str),
        "pos": {"size": float(position.size), "price": float(position.price)},
        "tb": {
            "min_since_open": float(trailing_bundle.min_since_open),
            "max_since_min": float(trailing_bundle.max_since_min),
            "max_since_open": float(trailing_bundle.max_since_open),
            "min_since_max": float(trailing_bundle.min_since_max),
        },
    }


def _simulate_gridfilled_position_for_trailing(
    *,
    side: Side,
    raw_gridonly: list,
    exchange_params: ExchangeParams,
    state_params: StateParams,
    bot_params: BotParams,
) -> tuple[Position | None, dict]:
    """Simulate filling grid orders until PB7 GridFirst would switch to trailing.

    PB7 condition (grid first): grid while `wallet_exposure_ratio < 1.0 + ratio`, else trailing.
    Here `wallet_exposure_ratio = wallet_exposure / wallet_exposure_limit_with_allowance`.

    We use Rust `gridonly` orders as the fill sequence.
    """
    dbg: dict = {
        "ratio": float(bot_params.entry_trailing_grid_ratio),
        "raw_count": len(raw_gridonly or []),
    }
    ratio = float(bot_params.entry_trailing_grid_ratio)
    if ratio >= 0.0:
        return (None, dbg)
    if not raw_gridonly:
        return (None, dbg)
    if state_params.balance <= 0.0:
        dbg["error"] = "balance<=0"
        return (None, dbg)
    if bot_params.total_wallet_exposure_limit <= 0.0 or bot_params.n_positions <= 0:
        dbg["error"] = "invalid total_wallet_exposure_limit/n_positions"
        return (None, dbg)

    per_pos_wel = float(bot_params.total_wallet_exposure_limit) / float(bot_params.n_positions)
    allowance = float(getattr(bot_params, "risk_we_excess_allowance_pct", 0.0) or 0.0)
    base_wel_allow = per_pos_wel * (1.0 + max(0.0, allowance)) if per_pos_wel > 0.0 else per_pos_wel
    threshold_ratio = min(1.0, max(0.0, 1.0 + ratio))

    dbg.update(
        {
            "per_pos_wallet_exposure_limit": per_pos_wel,
            "base_wallet_exposure_limit_with_allowance": base_wel_allow,
            "gridfirst_threshold_ratio": threshold_ratio,
        }
    )
    if base_wel_allow <= 0.0:
        dbg["error"] = "base_wel_allow<=0"
        return (None, dbg)

    psize = 0.0
    pprice = 0.0
    crossed_at: int | None = None
    last_we_ratio: float = 0.0

    c_mult = float(getattr(exchange_params, "c_mult", 1.0) or 1.0)
    if c_mult <= 0.0:
        c_mult = 1.0

    for idx, tup in enumerate(raw_gridonly):
        try:
            qty = float(tup[0])
            price = float(tup[1])
        except Exception:
            continue
        if qty == 0.0 or price <= 0.0:
            continue

        new_psize = psize + qty
        if new_psize == 0.0:
            new_pprice = 0.0
        elif psize == 0.0:
            new_pprice = price
        else:
            new_pprice = (psize * pprice + qty * price) / new_psize
        psize, pprice = new_psize, new_pprice

        wallet_exposure = (abs(psize) * pprice * c_mult) / float(state_params.balance)
        last_we_ratio = wallet_exposure / base_wel_allow
        if crossed_at is None and last_we_ratio >= threshold_ratio - 1e-12:
            crossed_at = idx
            break

    dbg.update(
        {
            "crossed_at_index": crossed_at,
            "sim_psize": float(psize),
            "sim_pprice": float(pprice),
            "sim_wallet_exposure_ratio": float(last_we_ratio),
        }
    )

    if psize == 0.0 or pprice <= 0.0:
        dbg["error"] = "sim position is empty"
        return (None, dbg)

    # Ensure correct sign for side
    if side == Side.Short and psize > 0.0:
        psize = -psize
    if side == Side.Long and psize < 0.0:
        psize = abs(psize)

    return (Position(size=float(psize), price=float(pprice)), dbg)


def _apply_fill_to_position(*, position: Position, fill_qty: float, fill_price: float) -> Position:
    psize = float(position.size)
    pprice = float(position.price)
    q = float(fill_qty)
    p = float(fill_price)
    new_psize = psize + q
    if new_psize == 0.0:
        return Position(size=0.0, price=0.0)
    if psize == 0.0:
        return Position(size=new_psize, price=p)

    # If position direction stays the same and size is reduced, avg price should remain unchanged.
    same_dir = (psize > 0.0 and new_psize > 0.0) or (psize < 0.0 and new_psize < 0.0)
    if same_dir and abs(new_psize) < abs(psize):
        return Position(size=new_psize, price=pprice)

    # If position flips direction, new avg price becomes the fill price.
    if (psize > 0.0 and new_psize < 0.0) or (psize < 0.0 and new_psize > 0.0):
        return Position(size=new_psize, price=p)

    # Otherwise position is increased in same direction -> weighted average.
    new_pprice = (psize * pprice + q * p) / new_psize
    return Position(size=new_psize, price=float(new_pprice))


def _simulate_trailing_sequence_forced(
    *,
    pbr,
    side: Side,
    exchange_params: ExchangeParams,
    state_params: StateParams,
    bot_params: BotParams,
    start_position: Position,
    n_steps: int = 5,
) -> dict:
    """Debug helper: simulate multiple trailing entries by forcing the trigger each step.

    PB7 resets the trailing bundle on fills (see `pb7/passivbot-rust/src/backtest.rs::update_trailing_prices`).
    Without future candles we cannot know real `tb`; here we assume after each fill price quickly moves so that
    threshold + retracement conditions are met again.
    """
    steps: list[dict] = []
    if n_steps <= 0:
        return {"steps": steps}
    if float(start_position.size) == 0.0:
        return {"steps": steps, "note": "start_position.size==0; trailing entries only exist after an initial fill"}

    pos = Position(size=float(start_position.size), price=float(start_position.price))
    sp_base = state_params
    ep = exchange_params
    bp = bot_params

    per_pos_wel = float(bp.total_wallet_exposure_limit) / float(bp.n_positions) if bp.n_positions else float(bp.total_wallet_exposure_limit)
    allowance = max(0.0, float(getattr(bp, "risk_we_excess_allowance_pct", 0.0) or 0.0))
    allowed_wel = per_pos_wel * (1.0 + allowance) if per_pos_wel > 0.0 else per_pos_wel
    c_mult = float(ep.c_mult or 1.0) or 1.0

    for i in range(int(n_steps)):
        # compute WE and threshold/retracement pct exactly like Rust (effective_wel = allowed_wel)
        wallet_exposure = (abs(float(pos.size)) * float(pos.price) * c_mult) / float(sp_base.balance)
        we_over = (wallet_exposure / allowed_wel) if allowed_wel > 0.0 else 0.0
        th_mult = we_over * float(bp.entry_trailing_threshold_we_weight)
        th_log_mult = float(sp_base.entry_volatility_logrange_ema_1h) * float(bp.entry_trailing_threshold_volatility_weight)
        threshold_pct = float(bp.entry_trailing_threshold_pct) * max(0.0, 1.0 + th_mult + th_log_mult)
        re_mult = we_over * float(bp.entry_trailing_retracement_we_weight)
        re_log_mult = float(sp_base.entry_volatility_logrange_ema_1h) * float(bp.entry_trailing_retracement_volatility_weight)
        retracement_pct = float(bp.entry_trailing_retracement_pct) * max(0.0, 1.0 + re_mult + re_log_mult)

        if retracement_pct <= 0.0:
            steps.append(
                {
                    "i": i,
                    "pos": {"size": float(pos.size), "price": float(pos.price)},
                    "wallet_exposure": float(wallet_exposure),
                    "wallet_exposure_limit_per_pos": float(per_pos_wel),
                    "wallet_exposure_limit_effective": float(allowed_wel),
                    "wallet_exposure_ratio": float(we_over),
                    "threshold_pct": float(threshold_pct),
                    "retracement_pct": float(retracement_pct),
                    "note": "forced-sequence requires retracement_pct>0",
                }
            )
            break

        if side == Side.Long:
            if threshold_pct > 0.0:
                threshold_price = float(pos.price) * (1.0 - threshold_pct)
                trigger_price = float(pos.price) * (1.0 - threshold_pct + retracement_pct)
                min_since_open = threshold_price * 0.99
                max_since_min = min_since_open * (1.0 + retracement_pct) * 1.01
                note = "synthetic trigger (threshold+retracement)"
            else:
                # Immediate trailing mode in PB7: trigger depends only on bundle retracement.
                min_since_open = float(pos.price) * 0.99
                max_since_min = min_since_open * (1.0 + retracement_pct) * 1.01
                threshold_price = float(min_since_open)
                trigger_price = float(min_since_open) * (1.0 + retracement_pct)
                note = "synthetic trigger (immediate trailing)"
            tb = TrailingPriceBundle(
                min_since_open=float(min_since_open),
                max_since_min=float(max_since_min),
                max_since_open=float(pos.price),
                min_since_max=float(min_since_open),
            )
            # Force the trigger by placing bid at the trigger price (and keep bid<=ask).
            bid = float(trigger_price)
            ask = float(trigger_price)
            sp = StateParams(
                balance=sp_base.balance,
                order_book=OrderBook(bid=float(bid), ask=float(ask)),
                ema_bands=sp_base.ema_bands,
                entry_volatility_logrange_ema_1h=sp_base.entry_volatility_logrange_ema_1h,
            )
        else:
            if threshold_pct > 0.0:
                threshold_price = float(pos.price) * (1.0 + threshold_pct)
                trigger_price = float(pos.price) * (1.0 + threshold_pct - retracement_pct)
                max_since_open = threshold_price * 1.01
                min_since_max = max_since_open * (1.0 - retracement_pct) * 0.99
                note = "synthetic trigger (threshold+retracement)"
            else:
                max_since_open = float(pos.price) * 1.01
                min_since_max = max_since_open * (1.0 - retracement_pct) * 0.99
                threshold_price = float(max_since_open)
                trigger_price = float(max_since_open) * (1.0 - retracement_pct)
                note = "synthetic trigger (immediate trailing)"
            tb = TrailingPriceBundle(
                min_since_open=float(min_since_max),
                max_since_min=float(max_since_open),
                max_since_open=float(max_since_open),
                min_since_max=float(min_since_max),
            )
            # Force the trigger by placing ask at the trigger price (and keep bid<=ask).
            ask = float(trigger_price)
            bid = float(trigger_price)
            sp = StateParams(
                balance=sp_base.balance,
                order_book=OrderBook(bid=float(bid), ask=float(ask)),
                ema_bands=sp_base.ema_bands,
                entry_volatility_logrange_ema_1h=sp_base.entry_volatility_logrange_ema_1h,
            )

        nxt = _calc_next_entry_rust(
            pbr=pbr,
            side=side,
            exchange_params=ep,
            state_params=sp,
            trailing_bundle=tb,
            bot_params=bp,
            position=pos,
        )
        steps.append(
            {
                "i": i,
                "pos_before": {"size": float(pos.size), "price": float(pos.price)},
                "wallet_exposure": float(wallet_exposure),
                "wallet_exposure_limit_per_pos": float(per_pos_wel),
                "wallet_exposure_limit_effective": float(allowed_wel),
                "wallet_exposure_ratio": float(we_over),
                "threshold_pct": float(threshold_pct),
                "retracement_pct": float(retracement_pct),
                "threshold_price": float(threshold_price),
                "trigger_price": float(trigger_price),
                "trigger_note": note,
                "next": nxt,
            }
        )
        if float(nxt.get("qty", 0.0) or 0.0) == 0.0:
            break
        pos = _apply_fill_to_position(position=pos, fill_qty=float(nxt["qty"]), fill_price=float(nxt["price"]))
        if float(pos.size) == 0.0:
            break

    return {
        "note": "synthetic: forces trailing trigger each step; real sequence depends on future candles + tb reset on fills",
        "steps": steps,
    }


def _calc_entries_rust(pbr, side: Side, data: "GVData", bot_params: BotParams, position: Position) -> List[Order]:
    ep = data.exchange_params
    sp = data.state_params
    tb = data.trailing_price_bundle
    wel_per_pos = (
        (float(bot_params.total_wallet_exposure_limit) / float(bot_params.n_positions))
        if getattr(bot_params, "n_positions", 0)
        else float(bot_params.total_wallet_exposure_limit)
    )
    if side == Side.Long:
        raw = pbr.calc_entries_long_py(
            ep.qty_step,
            ep.price_step,
            ep.min_qty,
            ep.min_cost,
            ep.c_mult,
            bot_params.entry_grid_double_down_factor,
            bot_params.entry_grid_spacing_volatility_weight,
            bot_params.entry_grid_spacing_we_weight,
            bot_params.entry_grid_spacing_pct,
            bot_params.entry_initial_ema_dist,
            bot_params.entry_initial_qty_pct,
            (bot_params.entry_trailing_double_down_factor or bot_params.entry_grid_double_down_factor),
            bot_params.entry_trailing_grid_ratio,
            bot_params.entry_trailing_retracement_pct,
            bot_params.entry_trailing_retracement_we_weight,
            bot_params.entry_trailing_retracement_volatility_weight,
            bot_params.entry_trailing_threshold_pct,
            bot_params.entry_trailing_threshold_we_weight,
            bot_params.entry_trailing_threshold_volatility_weight,
            wel_per_pos,
            bot_params.risk_we_excess_allowance_pct,
            sp.balance,
            position.size,
            position.price,
            tb.min_since_open,
            tb.max_since_min,
            tb.max_since_open,
            tb.min_since_max,
            sp.ema_bands.lower,
            sp.entry_volatility_logrange_ema_1h,
            sp.order_book.bid,
        )
    else:
        raw = pbr.calc_entries_short_py(
            ep.qty_step,
            ep.price_step,
            ep.min_qty,
            ep.min_cost,
            ep.c_mult,
            bot_params.entry_grid_double_down_factor,
            bot_params.entry_grid_spacing_volatility_weight,
            bot_params.entry_grid_spacing_we_weight,
            bot_params.entry_grid_spacing_pct,
            bot_params.entry_initial_ema_dist,
            bot_params.entry_initial_qty_pct,
            (bot_params.entry_trailing_double_down_factor or bot_params.entry_grid_double_down_factor),
            bot_params.entry_trailing_grid_ratio,
            bot_params.entry_trailing_retracement_pct,
            bot_params.entry_trailing_retracement_we_weight,
            bot_params.entry_trailing_retracement_volatility_weight,
            bot_params.entry_trailing_threshold_pct,
            bot_params.entry_trailing_threshold_we_weight,
            bot_params.entry_trailing_threshold_volatility_weight,
            wel_per_pos,
            bot_params.risk_we_excess_allowance_pct,
            sp.balance,
            position.size,
            position.price,
            tb.min_since_open,
            tb.max_since_min,
            tb.max_since_open,
            tb.min_since_max,
            sp.ema_bands.upper,
            sp.entry_volatility_logrange_ema_1h,
            sp.order_book.ask,
        )
    orders: List[Order] = []
    for qty, price, type_id in raw:
        orders.append(Order(qty=float(qty), price=float(price), order_type_str=_order_type_to_str(pbr, type_id)))
    return orders


def _calc_closes_rust(pbr, side: Side, data: "GVData", bot_params: BotParams, position: Position) -> List[Order]:
    ep = data.exchange_params
    sp = data.state_params
    tb = data.trailing_price_bundle
    close_grid_markup_end = bot_params.close_grid_markup_end
    close_grid_markup_start = bot_params.close_grid_markup_start
    wel_per_pos = (
        (float(bot_params.total_wallet_exposure_limit) / float(bot_params.n_positions))
        if getattr(bot_params, "n_positions", 0)
        else float(bot_params.total_wallet_exposure_limit)
    )
    if side == Side.Long:
        raw = pbr.calc_closes_long_py(
            ep.qty_step,
            ep.price_step,
            ep.min_qty,
            ep.min_cost,
            ep.c_mult,
            close_grid_markup_end,
            close_grid_markup_start,
            bot_params.close_grid_qty_pct,
            bot_params.close_trailing_grid_ratio,
            bot_params.close_trailing_qty_pct,
            bot_params.close_trailing_retracement_pct,
            bot_params.close_trailing_threshold_pct,
            wel_per_pos,
            bot_params.risk_we_excess_allowance_pct,
            bot_params.risk_wel_enforcer_threshold,
            sp.balance,
            position.size,
            position.price,
            tb.min_since_open,
            tb.max_since_min,
            tb.max_since_open,
            tb.min_since_max,
            sp.order_book.ask,
        )
    else:
        raw = pbr.calc_closes_short_py(
            ep.qty_step,
            ep.price_step,
            ep.min_qty,
            ep.min_cost,
            ep.c_mult,
            close_grid_markup_end,
            close_grid_markup_start,
            bot_params.close_grid_qty_pct,
            bot_params.close_trailing_grid_ratio,
            bot_params.close_trailing_qty_pct,
            bot_params.close_trailing_retracement_pct,
            bot_params.close_trailing_threshold_pct,
            wel_per_pos,
            bot_params.risk_we_excess_allowance_pct,
            bot_params.risk_wel_enforcer_threshold,
            sp.balance,
            position.size,
            position.price,
            tb.min_since_open,
            tb.max_since_min,
            tb.max_since_open,
            tb.min_since_max,
            sp.order_book.bid,
        )
    orders: List[Order] = []
    for qty, price, type_id in raw:
        orders.append(Order(qty=float(qty), price=float(price), order_type_str=_order_type_to_str(pbr, type_id)))
    return orders


def _pb7_src_dir() -> str:
    # Primary: configured PB7 dir from ini.
    try:
        cfg_root = pb7dir() or ""
    except Exception:
        cfg_root = ""
    if cfg_root:
        candidate = os.path.join(cfg_root, "src")
        if os.path.exists(os.path.join(candidate, "passivbot.py")):
            return candidate

    # Fallback: sibling checkout next to pbgui (useful for pytest/bare runs).
    try:
        here = os.path.abspath(__file__)
        # `.../pbgui/navi/v7_grid_visualizer.py` -> `.../pbgui/navi` -> `.../pbgui` -> `.../software`
        sibling = os.path.abspath(os.path.join(os.path.dirname(here), "..", "..", "pb7", "src"))
        if os.path.exists(os.path.join(sibling, "passivbot.py")):
            return sibling
    except Exception:
        pass

    return ""


@st.cache_data(show_spinner=False)
def _calc_entries_rust_cached(
    pb7_src_dir: str,
    side_value: int,
    exchange_params_json: str,
    state_params_json: str,
    trailing_bundle_json: str,
    bot_params_json: str,
    position_size: float,
    position_price: float,
):
    pbr = _get_passivbot_rust(pb7_src_dir)
    ep = json.loads(exchange_params_json)
    sp = json.loads(state_params_json)
    tb = json.loads(trailing_bundle_json)
    bp = json.loads(bot_params_json)

    total_wel = float(bp.get("total_wallet_exposure_limit", 0.0) or 0.0)
    n_positions = int(bp.get("n_positions", 0) or 0)
    wel_per_pos = (total_wel / float(n_positions)) if n_positions else total_wel

    if int(side_value) == int(Side.Long.value):
        return pbr.calc_entries_long_py(
            ep["qty_step"],
            ep["price_step"],
            ep["min_qty"],
            ep["min_cost"],
            ep["c_mult"],
            bp["entry_grid_double_down_factor"],
            bp.get("entry_grid_spacing_volatility_weight", 0.0),
            bp["entry_grid_spacing_we_weight"],
            bp["entry_grid_spacing_pct"],
            bp["entry_initial_ema_dist"],
            bp["entry_initial_qty_pct"],
            (bp.get("entry_trailing_double_down_factor") or bp["entry_grid_double_down_factor"]),
            bp["entry_trailing_grid_ratio"],
            bp["entry_trailing_retracement_pct"],
            bp.get("entry_trailing_retracement_we_weight", 0.0),
            bp.get("entry_trailing_retracement_volatility_weight", 0.0),
            bp["entry_trailing_threshold_pct"],
            bp.get("entry_trailing_threshold_we_weight", 0.0),
            bp.get("entry_trailing_threshold_volatility_weight", 0.0),
            wel_per_pos,
            bp.get("risk_we_excess_allowance_pct", 0.0),
            sp["balance"],
            float(position_size),
            float(position_price),
            tb["min_since_open"],
            tb["max_since_min"],
            tb["max_since_open"],
            tb["min_since_max"],
            sp["ema_bands"]["lower"],
            sp.get("entry_volatility_logrange_ema_1h", 0.0),
            sp["order_book"]["bid"],
        )
    else:
        return pbr.calc_entries_short_py(
            ep["qty_step"],
            ep["price_step"],
            ep["min_qty"],
            ep["min_cost"],
            ep["c_mult"],
            bp["entry_grid_double_down_factor"],
            bp.get("entry_grid_spacing_volatility_weight", 0.0),
            bp["entry_grid_spacing_we_weight"],
            bp["entry_grid_spacing_pct"],
            bp["entry_initial_ema_dist"],
            bp["entry_initial_qty_pct"],
            (bp.get("entry_trailing_double_down_factor") or bp["entry_grid_double_down_factor"]),
            bp["entry_trailing_grid_ratio"],
            bp["entry_trailing_retracement_pct"],
            bp.get("entry_trailing_retracement_we_weight", 0.0),
            bp.get("entry_trailing_retracement_volatility_weight", 0.0),
            bp["entry_trailing_threshold_pct"],
            bp.get("entry_trailing_threshold_we_weight", 0.0),
            bp.get("entry_trailing_threshold_volatility_weight", 0.0),
            wel_per_pos,
            bp.get("risk_we_excess_allowance_pct", 0.0),
            sp["balance"],
            float(position_size),
            float(position_price),
            tb["min_since_open"],
            tb["max_since_min"],
            tb["max_since_open"],
            tb["min_since_max"],
            sp["ema_bands"]["upper"],
            sp.get("entry_volatility_logrange_ema_1h", 0.0),
            sp["order_book"]["ask"],
        )


@st.cache_data(show_spinner=False)
def _calc_closes_rust_cached(
    pb7_src_dir: str,
    side_value: int,
    exchange_params_json: str,
    state_params_json: str,
    trailing_bundle_json: str,
    bot_params_json: str,
    position_size: float,
    position_price: float,
):
    pbr = _get_passivbot_rust(pb7_src_dir)
    ep = json.loads(exchange_params_json)
    sp = json.loads(state_params_json)
    tb = json.loads(trailing_bundle_json)
    bp = json.loads(bot_params_json)

    total_wel = float(bp.get("total_wallet_exposure_limit", 0.0) or 0.0)
    n_positions = int(bp.get("n_positions", 0) or 0)
    wel_per_pos = (total_wel / float(n_positions)) if n_positions else total_wel

    close_grid_markup_end = bp["close_grid_markup_end"]
    close_grid_markup_start = bp["close_grid_markup_start"]

    if int(side_value) == int(Side.Long.value):
        return pbr.calc_closes_long_py(
            ep["qty_step"],
            ep["price_step"],
            ep["min_qty"],
            ep["min_cost"],
            ep["c_mult"],
            close_grid_markup_end,
            close_grid_markup_start,
            bp["close_grid_qty_pct"],
            bp["close_trailing_grid_ratio"],
            bp["close_trailing_qty_pct"],
            bp["close_trailing_retracement_pct"],
            bp["close_trailing_threshold_pct"],
            wel_per_pos,
            bp.get("risk_we_excess_allowance_pct", 0.0),
            bp.get("risk_wel_enforcer_threshold", 1.0),
            sp["balance"],
            float(position_size),
            float(position_price),
            tb["min_since_open"],
            tb["max_since_min"],
            tb["max_since_open"],
            tb["min_since_max"],
            sp["order_book"]["ask"],
        )
    else:
        return pbr.calc_closes_short_py(
            ep["qty_step"],
            ep["price_step"],
            ep["min_qty"],
            ep["min_cost"],
            ep["c_mult"],
            close_grid_markup_end,
            close_grid_markup_start,
            bp["close_grid_qty_pct"],
            bp["close_trailing_grid_ratio"],
            bp["close_trailing_qty_pct"],
            bp["close_trailing_retracement_pct"],
            bp["close_trailing_threshold_pct"],
            wel_per_pos,
            bp.get("risk_we_excess_allowance_pct", 0.0),
            bp.get("risk_wel_enforcer_threshold", 1.0),
            sp["balance"],
            float(position_size),
            float(position_price),
            tb["min_since_open"],
            tb["max_since_min"],
            tb["max_since_open"],
            tb["min_since_max"],
            sp["order_book"]["bid"],
        )

@dataclass
class GVData:
    exchange_params: ExchangeParams = field(
        default_factory=lambda: ExchangeParams(
            min_qty=0.001,
            min_cost=1.0,
            qty_step=0.001,
            price_step=0.01,
            c_mult=1.0,
        )
    )
    state_params: StateParams = field(
        default_factory=lambda: StateParams(
            balance=1000.0,
            order_book=OrderBook(bid=100.0, ask=100),
            ema_bands=EmaBands(lower=100.0, upper=100.0),
        )
    )

    # Optional per-side derived states (e.g. indicator-derived EMA bands/volatility).
    # When present, these are used for charting + Rust calls per side.
    state_params_long: Optional[StateParams] = None
    state_params_short: Optional[StateParams] = None
        
    normal_bot_params_long: BotParams = field(
        default_factory=lambda: BotParams(
            total_wallet_exposure_limit=1.5,
            n_positions=1,
            entry_initial_qty_pct=0.03,
            entry_initial_ema_dist=0.03,
            entry_grid_spacing_pct=0.04,
            entry_grid_spacing_we_weight=1.2,
            entry_grid_double_down_factor=1.2,
            entry_trailing_threshold_pct=0.05,
            entry_trailing_retracement_pct=0.03,
            entry_trailing_grid_ratio=-0.7,
            
            ema_span_0=5.0,
            ema_span_1=1440.0,
            entry_volatility_ema_span_hours=120.0,

            close_grid_markup_end=0.03,
            close_grid_markup_start=0.02,
            close_grid_qty_pct=0.3,
            close_trailing_threshold_pct=0.05,
            close_trailing_retracement_pct=0.03,
            close_trailing_qty_pct=0.3,
            close_trailing_grid_ratio=0.0,
            risk_twel_enforcer_threshold=1.0,
        )
    )
    gridonly_bot_params_long: BotParams = field(init=False)
    
    normal_bot_params_short: BotParams = field(
        default_factory=lambda: BotParams(
            total_wallet_exposure_limit=1.5,
            n_positions=1,
            entry_initial_qty_pct=0.03,
            entry_initial_ema_dist=0.03,
            entry_grid_spacing_pct=0.04,
            entry_grid_spacing_we_weight=1.2,
            entry_grid_double_down_factor=1.2,
            entry_trailing_threshold_pct=0.05,
            entry_trailing_retracement_pct=0.03,
            entry_trailing_grid_ratio=-0.8,

            ema_span_0=5.0,
            ema_span_1=1440.0,
            entry_volatility_ema_span_hours=120.0,
            
            close_grid_markup_end=0.03,
            close_grid_markup_start=0.02,
            close_grid_qty_pct=0.3,
            close_trailing_threshold_pct=0.05,
            close_trailing_retracement_pct=0.03,
            close_trailing_qty_pct=0.3,
            close_trailing_grid_ratio=0.0,
            risk_twel_enforcer_threshold=1.0,
        )
    )
    gridonly_bot_params_short: BotParams = field(init=False)
    
    position_long_enty: Position = field(default_factory=lambda: Position(size=0.00, price=100.0))
    position_long_close: Position = field(default_factory=lambda: Position(size=10.00, price=100.0))
    position_short_entry: Position = field(default_factory=lambda: Position(size=0.00, price=100.0))
    position_short_close: Position = field(default_factory=lambda: Position(size=-10.00, price=100.0))
    
    trailing_price_bundle: TrailingPriceBundle = field(
        default_factory=lambda: TrailingPriceBundle(
            max_since_open=100.0,
            min_since_open=100.0,
            max_since_min=100.0,
            min_since_max=100.0,
        )
    )
    
    long_entry_mode = GridTrailingMode.Unknown
    long_close_mode = GridTrailingMode.Unknown
    short_entry_mode = GridTrailingMode.Unknown
    short_close_mode = GridTrailingMode.Unknown

    # Everything else
    is_external_config: bool = False
    title: str = ""
    
    # Historical Data for Plotting
    historical_candles: Any = None # pandas DataFrame or equivalent

    # Visualization-only: selected analysis timestamp (e.g. from the UI slider).
    analysis_time: Any = None  # datetime-like
    
    # Results
    normal_entries_long: List[Order] = field(default_factory=list)
    normal_closes_long: List[Order] = field(default_factory=list)
    normal_entries_short: List[Order] = field(default_factory=list)
    normal_closes_short: List[Order] = field(default_factory=list)
    gridonly_entries_long: List[Order] = field(default_factory=list)
    gridonly_closes_long: List[Order] = field(default_factory=list)
    gridonly_entries_short: List[Order] = field(default_factory=list)
    gridonly_closes_short: List[Order] = field(default_factory=list)

    long_entry_grid: int = 0
    long_close_grid: int = 0
    short_entry_grid: int = 0
    short_close_grid: int = 0

    # Visualization-only: potential trailing entry levels ("what if trailing is active")
    potential_entry_trailing_prices_long: List[float] = field(default_factory=list)
    potential_entry_trailing_prices_short: List[float] = field(default_factory=list)

    # Visualization-only: simulated trailing entry prices (uses Rust next-entry + synthetic trigger).
    # Intended for TrailingOnly mode where users expect steps to react to trailing DD factor.
    simulated_entry_trailing_prices_long: List[float] = field(default_factory=list)
    simulated_entry_trailing_prices_short: List[float] = field(default_factory=list)

    # Visualization-only: simulated trailing entry orders (qty+price+type) corresponding to the above.
    simulated_entry_trailing_orders_long: List[Order] = field(default_factory=list)
    simulated_entry_trailing_orders_short: List[Order] = field(default_factory=list)

    # Visualization-only: reference price used for simulated trailing (position average at trailing start).
    simulated_entry_trailing_ref_price_long: float = 0.0
    simulated_entry_trailing_ref_price_short: float = 0.0

    # Optional: historical candle-walk simulation results (filled entry orders)
    historical_sim_entries_long: List[Order] = field(default_factory=list)
    historical_sim_entries_short: List[Order] = field(default_factory=list)

    # Optional: historical candle-walk simulation results (filled close orders)
    historical_sim_closes_long: List[Order] = field(default_factory=list)
    historical_sim_closes_short: List[Order] = field(default_factory=list)

    # Optional: historical candle-walk simulation results (chronological fills: entries+closes)
    # Stored as list of dicts for easy display in Streamlit tables.
    historical_sim_fills_long: List[dict] = field(default_factory=list)
    historical_sim_fills_short: List[dict] = field(default_factory=list)

    def __post_init__(self):
        # Derived params; must be per-instance (not shared class attributes)
        self.gridonly_bot_params_long = self.normal_bot_params_long.clone()
        self.gridonly_bot_params_short = self.normal_bot_params_short.clone()
    
    def to_json(self) -> str:
        # Only serialize bot_params_long and bot_params_short
        data_dict = {
            "bot": {
                "long": asdict(self.normal_bot_params_long),
                "short": asdict(self.normal_bot_params_short)
            }
        }
        return json.dumps(data_dict, indent=4)

    def prepare_data(self):
        # Use current market price from state as the reference for hypothetical positions
        # Default is 100.0, but if injected, it will be the real price.
        current_price = self.state_params.order_book.ask if self.state_params.order_book.ask > 0 else 100.0

        # Apply TWE to position sizes
        self.position_long_enty = Position(size=0.00, price=current_price)
        self.position_long_close = Position(size=10.00 * self.normal_bot_params_long.total_wallet_exposure_limit, price=current_price)
        self.position_short_entry = Position(size=0.00, price=current_price)
        self.position_short_close = Position(size=-10.00 * self.normal_bot_params_short.total_wallet_exposure_limit, price=current_price)
        
        # Prepare gridonly bot params
        self.gridonly_bot_params_long = self.normal_bot_params_long.clone()
        self.gridonly_bot_params_short = self.normal_bot_params_short.clone()
        self.gridonly_bot_params_long.entry_trailing_grid_ratio = 0.0
        self.gridonly_bot_params_long.close_trailing_grid_ratio = 0.0
        self.gridonly_bot_params_short.entry_trailing_grid_ratio = 0.0
        self.gridonly_bot_params_short.close_trailing_grid_ratio = 0.0
        
        # Set modes
        self.long_entry_mode = get_GridTrailing_mode(self.normal_bot_params_long.entry_trailing_grid_ratio)
        self.long_close_mode = get_GridTrailing_mode(self.normal_bot_params_long.close_trailing_grid_ratio)
        self.short_entry_mode = get_GridTrailing_mode(self.normal_bot_params_short.entry_trailing_grid_ratio)
        self.short_close_mode = get_GridTrailing_mode(self.normal_bot_params_short.close_trailing_grid_ratio)
    
    def isActive(self, side: OrderType) -> bool:
        if side == Side.Long:
            return self.normal_bot_params_long.total_wallet_exposure_limit > 0.0 and self.normal_bot_params_long.n_positions > 0
        else:
            return self.normal_bot_params_short.total_wallet_exposure_limit > 0.0 and self.normal_bot_params_short.n_positions > 0
        
    @classmethod
    def from_json(cls, json_str: str) -> 'GVData':
        # Expect the "bot" structure with "long" and "short" keys
        data = json.loads(json_str)
        bot_data = data.get("bot", {})
        
        long_data = bot_data.get("long", {})
        short_data = bot_data.get("short", {})

        return cls(
            normal_bot_params_long=BotParams(**long_data),
            normal_bot_params_short=BotParams(**short_data)
        )

def clear_v7_tuning_keys():
    for key in list(st.session_state.keys()):
        if key.startswith("long_") or key.startswith("short_") or key.startswith("state_"):
            del st.session_state[key]

def prepare_config() -> GVData:
    # If there's no ConfigV7 in the session, load (probably passed from another page)
    if "v7_grid_visualizer_config" in st.session_state:
        # Build GVData from v7 config
        config_v7 = st.session_state.v7_grid_visualizer_config

        # Pre-fill Historical Data Injection from the config when coming from PBv7 Run.
        # We persist the config-derived exchange/coins in session_state so the UI can
        # offer a dropdown for the configured coins even after we delete the config.
        try:
            cfg_id = str(getattr(config_v7, "config_file", "") or getattr(getattr(config_v7, "pbgui", None), "note", "") or "")
        except Exception:
            cfg_id = ""

        try:
            cfg_exchanges = list(getattr(getattr(config_v7, "backtest", None), "exchanges", []) or [])
        except Exception:
            cfg_exchanges = []
        cfg_exchange = str(cfg_exchanges[0]) if cfg_exchanges else ""

        cfg_coins: list[str] = []
        try:
            ac = getattr(getattr(config_v7, "live", None), "approved_coins", None)
            long_coins = getattr(ac, "long", []) if ac is not None else []
            short_coins = getattr(ac, "short", []) if ac is not None else []
        except Exception:
            long_coins, short_coins = [], []

        # approved_coins may be a path string in some contexts; only accept sequences.
        seq_long = list(long_coins) if isinstance(long_coins, (list, tuple)) else []
        seq_short = list(short_coins) if isinstance(short_coins, (list, tuple)) else []
        for c in seq_long + seq_short:
            cs = str(c).strip()
            if cs and cs not in cfg_coins:
                cfg_coins.append(cs)

        resolved_cfg_exchange = _resolve_exchange_for_history(cfg_exchange, cfg_coins[0] if cfg_coins else "") if cfg_exchange else ""
        if resolved_cfg_exchange:
            st.session_state.gv_hist_config_exchange = resolved_cfg_exchange
        if cfg_coins:
            st.session_state.gv_hist_config_coins = cfg_coins

        # Apply the prefill only once per imported config to avoid overwriting user changes.
        last_applied = str(st.session_state.get("gv_hist_config_applied", "") or "")
        if cfg_id and cfg_id != last_applied:
            if resolved_cfg_exchange:
                st.session_state.gv_hist_exchange = resolved_cfg_exchange
            if cfg_coins:
                st.session_state.gv_hist_coin = cfg_coins[0]
            st.session_state.gv_hist_config_applied = cfg_id
        
        data = GVData()
        # Build Title identifying the config (show user + exchange + version)
        try:
            title_user = str(getattr(getattr(config_v7, "live", None), "user", "") or "").strip()
        except Exception:
            title_user = ""
        try:
            title_note = str(getattr(getattr(config_v7, "pbgui", None), "note", "") or "").strip()
        except Exception:
            title_note = ""
        try:
            title_ver = str(getattr(getattr(config_v7, "pbgui", None), "version", "") or "").strip()
        except Exception:
            title_ver = ""
        try:
            exs = list(getattr(getattr(config_v7, "backtest", None), "exchanges", []) or [])
            title_exc = str(exs[0]) if exs else ""
        except Exception:
            title_exc = ""

        left_parts = [p for p in [title_user, title_note] if p]
        left = " | ".join(left_parts) if left_parts else "(unnamed)"

        right_parts: list[str] = []
        if title_exc:
            right_parts.append(title_exc)
        if title_ver:
            right_parts.append(f"v{title_ver}")
        right = " | ".join(right_parts)

        data.title = f"Loaded Configuration: {left}{(' | ' + right) if right else ''}"
        data.is_external_config = True
        
        data.normal_bot_params_long = BotParams(
            total_wallet_exposure_limit=          config_v7.bot.long.total_wallet_exposure_limit,
            n_positions=                    config_v7.bot.long.n_positions,
            entry_initial_qty_pct=          config_v7.bot.long.entry_initial_qty_pct,
            entry_initial_ema_dist=         config_v7.bot.long.entry_initial_ema_dist,
            entry_grid_spacing_pct=         config_v7.bot.long.entry_grid_spacing_pct,
            entry_grid_spacing_we_weight=      config_v7.bot.long.entry_grid_spacing_we_weight,
            entry_grid_spacing_volatility_weight=getattr(config_v7.bot.long, "entry_grid_spacing_volatility_weight", 0.0),
            entry_grid_double_down_factor=  config_v7.bot.long.entry_grid_double_down_factor,
            entry_trailing_threshold_pct=   config_v7.bot.long.entry_trailing_threshold_pct,
            entry_trailing_threshold_we_weight=getattr(config_v7.bot.long, "entry_trailing_threshold_we_weight", 0.0),
            entry_trailing_threshold_volatility_weight=getattr(config_v7.bot.long, "entry_trailing_threshold_volatility_weight", 0.0),
            entry_trailing_retracement_pct= config_v7.bot.long.entry_trailing_retracement_pct,
            entry_trailing_retracement_we_weight=getattr(config_v7.bot.long, "entry_trailing_retracement_we_weight", 0.0),
            entry_trailing_retracement_volatility_weight=getattr(config_v7.bot.long, "entry_trailing_retracement_volatility_weight", 0.0),
            entry_trailing_double_down_factor=getattr(config_v7.bot.long, "entry_trailing_double_down_factor", config_v7.bot.long.entry_grid_double_down_factor),
            entry_trailing_grid_ratio=      config_v7.bot.long.entry_trailing_grid_ratio,

            ema_span_0=getattr(config_v7.bot.long, "ema_span_0", 1000.0),
            ema_span_1=getattr(config_v7.bot.long, "ema_span_1", 1000.0),
            entry_volatility_ema_span_hours=getattr(config_v7.bot.long, "entry_volatility_ema_span_hours", 240.0),
            filter_volatility_ema_span=getattr(config_v7.bot.long, "filter_volatility_ema_span", 120.0),
            filter_volatility_drop_pct=getattr(config_v7.bot.long, "filter_volatility_drop_pct", 0.0),
            filter_volume_drop_pct=getattr(config_v7.bot.long, "filter_volume_drop_pct", 0.0),
            filter_volume_ema_span=getattr(config_v7.bot.long, "filter_volume_ema_span", 1440.0),
            unstuck_close_pct=getattr(config_v7.bot.long, "unstuck_close_pct", 0.01),
            unstuck_ema_dist=getattr(config_v7.bot.long, "unstuck_ema_dist", 0.0),
            unstuck_loss_allowance_pct=getattr(config_v7.bot.long, "unstuck_loss_allowance_pct", 0.01),
            unstuck_threshold=getattr(config_v7.bot.long, "unstuck_threshold", 0.6),

            risk_we_excess_allowance_pct=getattr(config_v7.bot.long, "risk_we_excess_allowance_pct", 0.0),
            
            close_grid_markup_end=          config_v7.bot.long.close_grid_markup_end,
            close_grid_markup_start=        config_v7.bot.long.close_grid_markup_start,
            close_grid_qty_pct=             config_v7.bot.long.close_grid_qty_pct,
            close_trailing_threshold_pct=   config_v7.bot.long.close_trailing_threshold_pct,
            close_trailing_retracement_pct= config_v7.bot.long.close_trailing_retracement_pct,
            close_trailing_qty_pct=         config_v7.bot.long.close_trailing_qty_pct,
            close_trailing_grid_ratio=      config_v7.bot.long.close_trailing_grid_ratio,

            risk_wel_enforcer_threshold=getattr(config_v7.bot.long, "risk_wel_enforcer_threshold", 1.0),
            risk_twel_enforcer_threshold=getattr(config_v7.bot.long, "risk_twel_enforcer_threshold", 1.0),
        )
        
        data.normal_bot_params_short = BotParams(
            total_wallet_exposure_limit=          config_v7.bot.short.total_wallet_exposure_limit,
            n_positions=                    config_v7.bot.short.n_positions,
            entry_initial_qty_pct=          config_v7.bot.short.entry_initial_qty_pct,
            entry_initial_ema_dist=         config_v7.bot.short.entry_initial_ema_dist,
            entry_grid_spacing_pct=         config_v7.bot.short.entry_grid_spacing_pct,
            entry_grid_spacing_we_weight=      config_v7.bot.short.entry_grid_spacing_we_weight,
            entry_grid_spacing_volatility_weight=getattr(config_v7.bot.short, "entry_grid_spacing_volatility_weight", 0.0),
            entry_grid_double_down_factor=  config_v7.bot.short.entry_grid_double_down_factor,
            entry_trailing_threshold_pct=   config_v7.bot.short.entry_trailing_threshold_pct,
            entry_trailing_threshold_we_weight=getattr(config_v7.bot.short, "entry_trailing_threshold_we_weight", 0.0),
            entry_trailing_threshold_volatility_weight=getattr(config_v7.bot.short, "entry_trailing_threshold_volatility_weight", 0.0),
            entry_trailing_retracement_pct= config_v7.bot.short.entry_trailing_retracement_pct,
            entry_trailing_retracement_we_weight=getattr(config_v7.bot.short, "entry_trailing_retracement_we_weight", 0.0),
            entry_trailing_retracement_volatility_weight=getattr(config_v7.bot.short, "entry_trailing_retracement_volatility_weight", 0.0),
            entry_trailing_double_down_factor=getattr(config_v7.bot.short, "entry_trailing_double_down_factor", config_v7.bot.short.entry_grid_double_down_factor),
            entry_trailing_grid_ratio=      config_v7.bot.short.entry_trailing_grid_ratio,

            ema_span_0=getattr(config_v7.bot.short, "ema_span_0", 1000.0),
            ema_span_1=getattr(config_v7.bot.short, "ema_span_1", 1000.0),
            entry_volatility_ema_span_hours=getattr(config_v7.bot.short, "entry_volatility_ema_span_hours", 240.0),
            filter_volatility_ema_span=getattr(config_v7.bot.short, "filter_volatility_ema_span", 120.0),
            filter_volatility_drop_pct=getattr(config_v7.bot.short, "filter_volatility_drop_pct", 0.0),
            filter_volume_drop_pct=getattr(config_v7.bot.short, "filter_volume_drop_pct", 0.0),
            filter_volume_ema_span=getattr(config_v7.bot.short, "filter_volume_ema_span", 1440.0),
            unstuck_close_pct=getattr(config_v7.bot.short, "unstuck_close_pct", 0.01),
            unstuck_ema_dist=getattr(config_v7.bot.short, "unstuck_ema_dist", 0.0),
            unstuck_loss_allowance_pct=getattr(config_v7.bot.short, "unstuck_loss_allowance_pct", 0.01),
            unstuck_threshold=getattr(config_v7.bot.short, "unstuck_threshold", 0.6),

            risk_we_excess_allowance_pct=getattr(config_v7.bot.short, "risk_we_excess_allowance_pct", 0.0),
            
            close_grid_markup_end=          config_v7.bot.short.close_grid_markup_end,
            close_grid_markup_start=        config_v7.bot.short.close_grid_markup_start,
            close_grid_qty_pct=             config_v7.bot.short.close_grid_qty_pct,
            close_trailing_threshold_pct=   config_v7.bot.short.close_trailing_threshold_pct,
            close_trailing_retracement_pct= config_v7.bot.short.close_trailing_retracement_pct,
            close_trailing_qty_pct=         config_v7.bot.short.close_trailing_qty_pct,
            close_trailing_grid_ratio=      config_v7.bot.short.close_trailing_grid_ratio,

            risk_wel_enforcer_threshold=getattr(config_v7.bot.short, "risk_wel_enforcer_threshold", 1.0),
            risk_twel_enforcer_threshold=getattr(config_v7.bot.short, "risk_twel_enforcer_threshold", 1.0),
        )
        
        data.prepare_data()
        st.session_state.v7_grid_visualizer_data = data
        del st.session_state.v7_grid_visualizer_config
        clear_v7_tuning_keys()
        return data
    
    # If there's a data object in the session, use it (e.g. from editor)
    if "v7_grid_visualizer_data" in st.session_state:
        data = st.session_state.v7_grid_visualizer_data
        data.prepare_data()
        return data
    
    data = GVData()
    data.prepare_data()
    return data



def create_plotly_graph(side: OrderType, data: GVData):
    
    if not data.isActive(side):
        return None
    
    normal_entry_orders = []
    normal_entry_prices = []
    normal_enty_grid_min = 0
    normal_enty_grid_max = 0
    
    normal_close_orders = []
    normap_close_prices = []
    normal_close_grid_min = 0
    normal_close_grid_max = 0
    
    fullgrid_entry_orders = []
    fullgrid_entry_prices = []
    fullgrid_entry_grid_min = 0
    fullgrid_entry_grid_max = 0
    fullgrid_close_orders = []
    fullgrid_close_prices = []
    fullgrid_close_grid_min = 0
    fullgrid_close_grid_max = 0
    
    trailing_entry_orders = []
    trailing_entry_prices = []
    trailing_entry_grid_min = 0
    trailing_entry_grid_max = 0
    trailing_close_orders = []
    trailing_close_prices = []
    trailing_close_grid_min = 0
    trailing_close_grid_max = 0
    
    bot_params = None
    position_price = None
    state_params = None
    entry_mode = GridTrailingMode.Unknown
    close_mode = GridTrailingMode.Unknown
    
    # Determine which bot params to use depending on side
    if side == Side.Long:
        bot_params = data.normal_bot_params_long
        entry_pos_price = data.position_long_enty.price
        close_pos_price = data.position_long_close.price
        normal_entry_orders = data.normal_entries_long
        normal_close_orders = data.normal_closes_long
        fullgrid_entry_orders = data.gridonly_entries_long
        fullgrid_close_orders = data.gridonly_closes_long
        entry_mode = data.long_entry_mode
        close_mode = data.long_close_mode
        state_params = data.state_params_long or data.state_params
        title_side = "LONG"
    else:
        bot_params = data.normal_bot_params_short
        entry_pos_price = data.position_short_entry.price
        close_pos_price = data.position_short_close.price
        normal_entry_orders = data.normal_entries_short
        normal_close_orders = data.normal_closes_short
        fullgrid_entry_orders = data.gridonly_entries_short
        fullgrid_close_orders = data.gridonly_closes_short
        entry_mode = data.short_entry_mode
        close_mode = data.short_close_mode
        state_params = data.state_params_short or data.state_params
        title_side = "SHORT"

    # Price reference line ("EMA Band" in this plot): prefer EMA band, fallback to order book mid, then 100.
    start_price = 100.0
    if state_params is not None:
        if side == Side.Long and getattr(state_params.ema_bands, "lower", 0.0):
            start_price = float(state_params.ema_bands.lower)
        elif side == Side.Short and getattr(state_params.ema_bands, "upper", 0.0):
            start_price = float(state_params.ema_bands.upper)
        else:
            bid = float(getattr(state_params.order_book, "bid", 0.0) or 0.0)
            ask = float(getattr(state_params.order_book, "ask", 0.0) or 0.0)
            if bid > 0.0 and ask > 0.0:
                start_price = (bid + ask) / 2.0

    enable_plotly_entry_spacing_slider = False

    # Extract entry and close prices (server-side computed baseline)
    normal_entry_prices = [o.price for o in normal_entry_orders]
    if len(normal_entry_prices) > 0:
        normal_enty_grid_min = min(normal_entry_prices)
        normal_enty_grid_max = max(normal_entry_prices)
        
        if side == Side.Long:
            data.long_entry_grid = normal_enty_grid_max - normal_enty_grid_min
        else:
            data.short_entry_grid = normal_enty_grid_max - normal_enty_grid_min
    else:
        normal_enty_grid_min = 100
        normal_enty_grid_max = 100
    
    
    normal_close_prices = [o.price for o in normal_close_orders]
    if len(normal_close_prices) > 0:
        normal_close_grid_min = min(normal_close_prices)
        normal_close_grid_max = max(normal_close_prices)
        
        if side == Side.Long:
            data.long_close_grid = normal_close_grid_max - normal_close_grid_min
        else:
            data.short_close_grid = normal_close_grid_max - normal_close_grid_min
    else:
        normal_close_grid_min = 100
        normal_close_grid_max = 100
    
    fullgrid_entry_prices = [o.price for o in fullgrid_entry_orders]
    if len(fullgrid_entry_prices) > 0:
        fullgrid_entry_grid_min = min(fullgrid_entry_prices)
        fullgrid_entry_grid_max = max(fullgrid_entry_prices)
  
    fullgrid_close_prices = [o.price for o in fullgrid_close_orders]
    if len(fullgrid_close_prices) > 0:
        fullgrid_close_grid_min = min(fullgrid_close_prices)
        fullgrid_close_grid_max = max(fullgrid_close_prices)
    
    
    # Handle Trailing Grids - Robust Calculation
    warnings = []

    # Safe bound calculations
    fg_entry_min = min(fullgrid_entry_prices) if fullgrid_entry_prices else (min(normal_entry_prices) if normal_entry_prices else 0)
    fg_entry_max = max(fullgrid_entry_prices) if fullgrid_entry_prices else (max(normal_entry_prices) if normal_entry_prices else 0)
    fg_close_min = min(fullgrid_close_prices) if fullgrid_close_prices else (min(normal_close_prices) if normal_close_prices else 0)
    fg_close_max = max(fullgrid_close_prices) if fullgrid_close_prices else (max(normal_close_prices) if normal_close_prices else 0)

    n_entry_min = min(normal_entry_prices) if normal_entry_prices else fg_entry_min
    n_entry_max = max(normal_entry_prices) if normal_entry_prices else fg_entry_max
    n_close_min = min(normal_close_prices) if normal_close_prices else fg_close_min
    n_close_max = max(normal_close_prices) if normal_close_prices else fg_close_max
    
    ############
    #  LONG   #
    ############
    # Helper to calculate Grid/Trailing boundary price for GridFirst mode
    def _get_grid_trailing_boundary(side, data, bot_params):
        if bot_params.entry_trailing_grid_ratio >= 0:
            return None # Not GridFirst

        boundary_ratio = 1.0 - abs(bot_params.entry_trailing_grid_ratio)
        sp = (data.state_params_long or data.state_params) if side == Side.Long else (data.state_params_short or data.state_params)
        boundary_we = bot_params.total_wallet_exposure_limit * sp.balance * boundary_ratio
        
        relevant_entries = data.normal_entries_long if side == Side.Long else data.normal_entries_short
        curr_size = data.position_long_enty.size if side == Side.Long else data.position_short_entry.size
        curr_price = data.position_long_enty.price if side == Side.Long else data.position_short_entry.price
        
        cum_cost = curr_size * curr_price
        
        # Fallback if no entries
        if not relevant_entries:
            # Should be Initial Dist?
             if side == Side.Long:
                 return sp.order_book.bid * (1.0 - bot_params.entry_initial_ema_dist)
             else:
                 return sp.order_book.ask * (1.0 + bot_params.entry_initial_ema_dist)

        last_price = relevant_entries[0].price
        
        for e in relevant_entries:
            cost = e.qty * e.price
            cum_cost += cost
            last_price = e.price
            if cum_cost >= boundary_we:
                return e.price
        
        return last_price # Reached end of grid without hitting boundary (small balance?)

    if side == Side.Long:
        ############
        #  ENTRY   #
        ############
        if entry_mode == GridTrailingMode.GridFirst:
            # Trailing area is below Static Grid part.
            # 1. Find where Static Grid ends (Boundary)
            boundary_price = _get_grid_trailing_boundary(Side.Long, data, bot_params)
            
            # If we couldn't find boundary (e.g. no orders), fallback to n_entry_min
            start_ref = boundary_price if boundary_price is not None else n_entry_min
            
            # 2. Area extends from Boundary down to Threshold Target (Gap) or FullGrid bottom
            gap_target = start_ref * (1.0 - bot_params.entry_trailing_threshold_pct)
            
            # We want the area to START at start_ref.
            # And END at the deeper of (FullGrid Min, Gap Target).
            # But wait, start_ref is the TOP of the area (Price High).
            
            trailing_entry_grid_max = start_ref
            trailing_entry_grid_min = min(fg_entry_min, gap_target) 
            
        elif entry_mode == GridTrailingMode.TrailingFirst:
            # Trailing area logic:
            # Trailing Zone starts from "Start Price" (or first grid) down to Threshold.
            # But "Full Grid" (fg) already simulates grid from top to bottom.
            # If Threshold is large (e.g. 18%), the first ACTUAL order might be deeper than fg_max.
            # We must extend the UPPER bound of the trailing area to encompass the gap.
            
            # 1. Theoretical Start (EMA Band adjusted)
            theo_start = start_price
            
            # 2. Threshold Target (Where first order CAN happen)
            threshold_target = theo_start * (1.0 - bot_params.entry_trailing_threshold_pct)
            
            # The Trailing Area covers the space where orders are HELD BACK.
            # So from Start down to Threshold Target.
            # AND it covers the space where trailing is active.
            
            # Let's define Trailing Erea = [Highest Potential Start, Lowest Grid Limit]
            # Highest Potential Start = EMA Band (Start Price)
            # Lowest Limit = Full Grid Min
            
            # But specifically, the question was: Area should grow with Threshold.
            # That implies the Area involves the Threshold gap.
            
            trailing_entry_grid_max = max(fg_entry_max, theo_start)
            trailing_entry_grid_min = min(fg_entry_min, threshold_target)
            
        elif entry_mode == GridTrailingMode.TrailingOnly:
            # Standard Full Grid range
            start_ref = start_price
            gap_target = start_ref * (1.0 - bot_params.entry_trailing_threshold_pct)
            trailing_entry_grid_min = min(fg_entry_min, gap_target)
            trailing_entry_grid_max = max(fg_entry_max, start_ref)
        ############
        #  CLOSE   #
        ############
        if close_mode == GridTrailingMode.GridFirst:
            trailing_close_grid_min = n_close_max
            trailing_close_grid_max = fg_close_max
        elif close_mode == GridTrailingMode.TrailingFirst:
            trailing_close_grid_min = fg_close_min
            trailing_close_grid_max = n_close_min
        elif close_mode == GridTrailingMode.TrailingOnly:
            trailing_close_grid_min = fg_close_min
            trailing_close_grid_max = fg_close_max
    ############
    #  SHORT   #
    ############
    elif side == Side.Short:
        ############
        #  ENTRY   #
        ############
        if entry_mode == GridTrailingMode.GridFirst:
            # Trailing is ABOVE Normal Grid
            boundary_price = _get_grid_trailing_boundary(Side.Short, data, bot_params)
            start_ref = boundary_price if boundary_price is not None else n_entry_max
            
            gap_target = start_ref * (1.0 + bot_params.entry_trailing_threshold_pct)
            
            trailing_entry_grid_min = start_ref # Min Price (Start of area is at bottom, goes up)
            trailing_entry_grid_max = max(fg_entry_max, gap_target) # Extend up
            
        elif entry_mode == GridTrailingMode.TrailingFirst:
            # See Long logic above
            theo_start = start_price
            threshold_target = theo_start * (1.0 + bot_params.entry_trailing_threshold_pct)
            
            trailing_entry_grid_min = min(fg_entry_min, theo_start)
            trailing_entry_grid_max = max(fg_entry_max, threshold_target)
            
        elif entry_mode == GridTrailingMode.TrailingOnly:
            start_ref = start_price
            gap_target = start_ref * (1.0 + bot_params.entry_trailing_threshold_pct)
            trailing_entry_grid_min = min(fg_entry_min, start_ref)
            trailing_entry_grid_max = max(fg_entry_max, gap_target)
        ############
        #  CLOSE   #
        ############
        if close_mode == GridTrailingMode.GridFirst:
            trailing_close_grid_min = fg_close_min
            trailing_close_grid_max = n_close_min
        elif close_mode == GridTrailingMode.TrailingFirst:
            trailing_close_grid_min = n_close_max
            trailing_close_grid_max = fg_close_max
        elif close_mode == GridTrailingMode.TrailingOnly:
            trailing_close_grid_min = fg_close_min
            trailing_close_grid_max = fg_close_max
            
    # Create Plotly Figure
    fig = go.Figure()

    # Add candlesticks if historical data is present
    if data.historical_candles is not None:
        try:
            # Assume it's a dict with orient='list' or dataframe
            if isinstance(data.historical_candles, dict):
                hc = pd.DataFrame(data.historical_candles)
            else:
                hc = data.historical_candles
            
            # Ensure index is datetime for plotting
            if 'timestamp' in hc.columns:
                ts = hc['timestamp']
                # Heuristic: many sources store epoch timestamps as seconds/ms/us/ns.
                # If we don't pass `unit=...`, pandas treats integers as nanoseconds, leading to 1970 dates.
                try:
                    if np.issubdtype(getattr(ts, "dtype", object), np.datetime64):
                        hc['date'] = pd.to_datetime(ts)
                    elif np.issubdtype(getattr(ts, "dtype", object), np.number):
                        med = float(np.nanmedian(ts.to_numpy(dtype=float, copy=False)))
                        if med >= 1e17:
                            unit = 'ns'
                        elif med >= 1e14:
                            unit = 'us'
                        elif med >= 1e11:
                            unit = 'ms'
                        else:
                            unit = 's'
                        hc['date'] = pd.to_datetime(ts, unit=unit)
                    else:
                        hc['date'] = pd.to_datetime(ts)
                except Exception:
                    hc['date'] = pd.to_datetime(ts)
            elif isinstance(hc.index, pd.DatetimeIndex):
                hc['date'] = hc.index
            
            fig.add_trace(go.Candlestick(
                x=hc['date'],
                open=hc['open'],
                high=hc['high'],
                low=hc['low'],
                close=hc['close'],
                name='Price'
            ))
            
            # Since we are adding x-axis dates, we need to map the grid (which was purely y-based)
            # The original grid was plotted at x=plot_x.
            # Now we should probably extend the lines across the visible time range + strict projection
            # Or just plot them as horizontal lines across the whole chart.
            
            # Get x-axis range
            min_date = hc['date'].min()
            max_date = hc['date'].max()

            # If there's only one candle, Plotly's auto-range can collapse the x-axis,
            # making the chart look empty until the slider moves. Add a small padding.
            if pd.notna(min_date) and pd.notna(max_date) and min_date == max_date:
                try:
                    if len(hc['date']) >= 2:
                        dt = pd.to_datetime(hc['date'].iloc[-1]) - pd.to_datetime(hc['date'].iloc[-2])
                        if pd.isna(dt) or dt == pd.Timedelta(0):
                            dt = pd.Timedelta(minutes=1)
                    else:
                        dt = pd.Timedelta(minutes=1)
                except Exception:
                    dt = pd.Timedelta(minutes=1)

                pad = dt * 2
                fig.update_xaxes(range=[min_date - pad, max_date + pad])
            
            # We can use shapes to draw infinite lines, or use traces with manual x
            plot_x = [min_date, max_date]
            
        except Exception as e:
            st.warning(f"Failed to plot candles: {e}")
            plot_x = [0, 1] # Fallback
    else:
        plot_x = [0, 1]

    # Build x-samples for horizontal line hovers.
    # Plotly hover for `mode='lines'` triggers on vertices.
    # Note: Plotly may also simplify straight lines down to endpoints unless `line.simplify=False`.
    n_hover_samples_grid = 120
    x_samples_grid: list = []
    try:
        x0 = plot_x[0]
        x1 = plot_x[-1]
        is_datetime_like = isinstance(x0, (pd.Timestamp, datetime.datetime, np.datetime64)) or isinstance(
            x1, (pd.Timestamp, datetime.datetime, np.datetime64)
        )
        if is_datetime_like:
            x_samples_grid = list(
                pd.date_range(
                    start=pd.to_datetime(x0),
                    end=pd.to_datetime(x1),
                    periods=n_hover_samples_grid,
                )
            )
        else:
            x_samples_grid = [float(v) for v in np.linspace(float(x0), float(x1), n_hover_samples_grid)]
    except Exception:
        x_samples_grid = list(plot_x)

    # EMA band: draw as full-width shape so it always spans the entire plot.
    # Keep legend + hover via dummy trace + invisible markers (shapes don't support hover).
    fig.add_shape(
        type='line',
        xref='paper',
        x0=0,
        x1=1,
        yref='y',
        y0=float(start_price),
        y1=float(start_price),
        line=dict(color='purple', dash='solid', width=4),
        layer='above',
    )
    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            mode='lines',
            name='EMA Band',
            line=dict(color='purple', dash='solid', width=4),
            showlegend=True,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x_samples_grid,
            y=[start_price] * len(x_samples_grid),
            mode='markers',
            name='EMA Band (hover)',
            showlegend=False,
            marker=dict(size=8, color='rgba(0,0,0,0)'),
            hovertemplate='EMA Band: %{y:.6f}<extra></extra>',
        )
    )

    # Add entry grid range as a shaded area (skip if Plotly slider is enabled; slider will manage shapes)
    if not enable_plotly_entry_spacing_slider:
        fig.add_shape(
            type='rect',
            xref='paper',
            x0=0,
            x1=1,
            yref='y',
            y0=normal_enty_grid_min,
            y1=normal_enty_grid_max,
            fillcolor='red',
            opacity=0.10,
            layer='below',
            line_width=0
        )
    # Add a dummy trace to represent Entry Grid in the legend
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='lines',
        line=dict(color='rgba(255,0,0,0.12)', width=10),
        name='Entry Grid (Range)',
        showlegend=True
    ))
    
    # Entry grid lines: draw as full-width shapes (xref='paper') so they span the entire plot.
    # Hover: add invisible marker track; Plotly doesn't hover on shapes.
    if (not enable_plotly_entry_spacing_slider) and len(normal_entry_prices) > 0:
        # Legend entry (lines)
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode='lines',
                name='Entry Grid (Lines)',
                line=dict(color='rgba(255, 0, 0, 0.6)', dash='dash', width=1),
                showlegend=True,
                legendgroup='entry',
            )
        )

        for entry_price in normal_entry_prices:
            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=float(entry_price),
                y1=float(entry_price),
                line=dict(color='rgba(255, 0, 0, 0.6)', dash='dash', width=1),
                layer='above',
            )

        hover_xs: list = []
        hover_ys: list[float] = []
        hover_text: list[str] = []
        for entry_price in normal_entry_prices:
            y = float(entry_price)
            for x in x_samples_grid:
                hover_xs.append(x)
                hover_ys.append(y)
                hover_text.append(f"Entry Grid: {y:.6f}")
            hover_xs.append(None)
            hover_ys.append(None)
            hover_text.append("")

        if hover_ys:
            fig.add_trace(
                go.Scatter(
                    x=hover_xs,
                    y=hover_ys,
                    mode='markers',
                    name='Entry Grid (hover)',
                    showlegend=False,
                    legendgroup='entry',
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate="%{text}<extra></extra>",
                    text=hover_text,
                )
            )

    # Trailing entry levels (visualization-only)
    # In TrailingOnly mode, show simulated trailing steps (reacts to trailing DD factor).
    # Otherwise, show the existing "potential" view derived from grid-only partition.
    pot_prices = data.potential_entry_trailing_prices_long if side == Side.Long else data.potential_entry_trailing_prices_short
    sim_prices = data.simulated_entry_trailing_prices_long if side == Side.Long else data.simulated_entry_trailing_prices_short
    use_sim = bool(entry_mode != GridTrailingMode.GridOnly and sim_prices)
    shown_prices = sim_prices if use_sim else pot_prices
    shown_name = 'Entry Trailing (Simulated)' if use_sim else 'Entry Trailing (Potential)'
    shown_group = 'entry_trailing_simulated' if use_sim else 'entry_trailing_potential'

    if (not enable_plotly_entry_spacing_slider) and shown_prices:
        # Visible lines: keep as layout.shapes (original look).
        for pp in shown_prices:
            try:
                y = float(pp)
            except Exception:
                continue
            if y <= 0.0:
                continue
            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=y,
                y1=y,
                line=dict(color='orange', dash='dash', width=1),
                layer='above',
            )

        # Legend entry (as before)
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode='lines',
                line=dict(color='orange', dash='dash', width=2),
                name=shown_name,
                showlegend=True,
                legendgroup=shown_group,
            )
        )

        # Hover helper: add invisible markers along each line.
        # Plotly does not hover on layout.shapes; markers provide hover anywhere without changing visuals.
        x_samples = list(x_samples_grid)

        hover_xs: list = []
        hover_ys: list[float] = []
        hover_text: list[str] = []
        for pp in shown_prices:
            try:
                y = float(pp)
            except Exception:
                continue
            if y <= 0.0:
                continue
            for x in x_samples:
                hover_xs.append(x)
                hover_ys.append(y)
                hover_text.append(f"{shown_name}: {y:.6f}")
            # Separator (optional)
            hover_xs.append(None)
            hover_ys.append(None)
            hover_text.append("")

        if hover_ys:
            fig.add_trace(
                go.Scatter(
                    x=hover_xs,
                    y=hover_ys,
                    mode='markers',
                    name=f"{shown_name} (hover)",
                    showlegend=False,
                    legendgroup=shown_group,
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate="%{text}<extra></extra>",
                    text=hover_text,
                )
            )


    # Store dynamic entry trailing area bounds so we can expand y-axis to show them
    entry_trailing_area_bounds: tuple[float, float] | None = None

    # Trailing area is computed dynamically based on bot_params (which reflects current slider values)
    # This ensures the area updates when user changes entry_trailing_threshold_pct
    if (not enable_plotly_entry_spacing_slider):
        # Calculate current trailing area using bot_params
        
        if entry_mode == GridTrailingMode.GridFirst and side == Side.Long:
            boundary_price = _get_grid_trailing_boundary(Side.Long, data, bot_params)
            start_ref = boundary_price if boundary_price is not None else n_entry_min
            gap_target = start_ref * (1.0 - bot_params.entry_trailing_threshold_pct)
            dynamic_trailing_min = min(fg_entry_min, gap_target)
            dynamic_trailing_max = start_ref
            
            if dynamic_trailing_min > 0 and dynamic_trailing_max > 0 and abs(dynamic_trailing_max - dynamic_trailing_min) > 0.000001:
                entry_trailing_area_bounds = (float(dynamic_trailing_min), float(dynamic_trailing_max))
                fig.add_shape(
                    type='rect',
                    xref='paper',
                    x0=0,
                    x1=1,
                    yref='y',
                    y0=dynamic_trailing_min,
                    y1=dynamic_trailing_max,
                    fillcolor='rgba(200, 100, 0, 0.22)',
                    layer='below',
                    line_width=0
                )
                
        elif entry_mode == GridTrailingMode.GridFirst and side == Side.Short:
            boundary_price = _get_grid_trailing_boundary(Side.Short, data, bot_params)
            start_ref = boundary_price if boundary_price is not None else n_entry_max
            gap_target = start_ref * (1.0 + bot_params.entry_trailing_threshold_pct)
            dynamic_trailing_min = start_ref
            dynamic_trailing_max = max(fg_entry_max, gap_target)
            
            if dynamic_trailing_min > 0 and dynamic_trailing_max > 0 and abs(dynamic_trailing_max - dynamic_trailing_min) > 0.000001:
                entry_trailing_area_bounds = (float(dynamic_trailing_min), float(dynamic_trailing_max))
                fig.add_shape(
                    type='rect',
                    xref='paper',
                    x0=0,
                    x1=1,
                    yref='y',
                    y0=dynamic_trailing_min,
                    y1=dynamic_trailing_max,
                    fillcolor='rgba(200, 100, 0, 0.22)',
                    layer='below',
                    line_width=0
                )
                
        elif entry_mode == GridTrailingMode.TrailingFirst and side == Side.Long:
            theo_start = start_price
            threshold_target = theo_start * (1.0 - bot_params.entry_trailing_threshold_pct)
            dynamic_trailing_max = max(fg_entry_max, theo_start)
            dynamic_trailing_min = min(fg_entry_min, threshold_target)
            
            if dynamic_trailing_min > 0 and dynamic_trailing_max > 0 and abs(dynamic_trailing_max - dynamic_trailing_min) > 0.000001:
                entry_trailing_area_bounds = (float(dynamic_trailing_min), float(dynamic_trailing_max))
                fig.add_shape(
                    type='rect',
                    xref='paper',
                    x0=0,
                    x1=1,
                    yref='y',
                    y0=dynamic_trailing_min,
                    y1=dynamic_trailing_max,
                    fillcolor='rgba(200, 100, 0, 0.22)',
                    layer='below',
                    line_width=0
                )
                
        elif entry_mode == GridTrailingMode.TrailingFirst and side == Side.Short:
            theo_start = start_price
            threshold_target = theo_start * (1.0 + bot_params.entry_trailing_threshold_pct)
            dynamic_trailing_min = min(fg_entry_min, theo_start)
            dynamic_trailing_max = max(fg_entry_max, threshold_target)
            
            if dynamic_trailing_min > 0 and dynamic_trailing_max > 0 and abs(dynamic_trailing_max - dynamic_trailing_min) > 0.000001:
                entry_trailing_area_bounds = (float(dynamic_trailing_min), float(dynamic_trailing_max))
                fig.add_shape(
                    type='rect',
                    xref='paper',
                    x0=0,
                    x1=1,
                    yref='y',
                    y0=dynamic_trailing_min,
                    y1=dynamic_trailing_max,
                    fillcolor='rgba(200, 100, 0, 0.22)',
                    layer='below',
                    line_width=0
                )
                
        elif entry_mode == GridTrailingMode.TrailingOnly and side == Side.Long:
            start_ref = start_price
            gap_target = start_ref * (1.0 - bot_params.entry_trailing_threshold_pct)
            dynamic_trailing_min = min(fg_entry_min, gap_target)
            dynamic_trailing_max = max(fg_entry_max, start_ref)
            
            if dynamic_trailing_min > 0 and dynamic_trailing_max > 0 and abs(dynamic_trailing_max - dynamic_trailing_min) > 0.000001:
                entry_trailing_area_bounds = (float(dynamic_trailing_min), float(dynamic_trailing_max))
                fig.add_shape(
                    type='rect',
                    xref='paper',
                    x0=0,
                    x1=1,
                    yref='y',
                    y0=dynamic_trailing_min,
                    y1=dynamic_trailing_max,
                    fillcolor='rgba(200, 100, 0, 0.22)',
                    layer='below',
                    line_width=0
                )
                
        elif entry_mode == GridTrailingMode.TrailingOnly and side == Side.Short:
            start_ref = start_price
            gap_target = start_ref * (1.0 + bot_params.entry_trailing_threshold_pct)
            dynamic_trailing_min = min(fg_entry_min, start_ref)
            dynamic_trailing_max = max(fg_entry_max, gap_target)
            
            if dynamic_trailing_min > 0 and dynamic_trailing_max > 0 and abs(dynamic_trailing_max - dynamic_trailing_min) > 0.000001:
                entry_trailing_area_bounds = (float(dynamic_trailing_min), float(dynamic_trailing_max))
                fig.add_shape(
                    type='rect',
                    xref='paper',
                    x0=0,
                    x1=1,
                    yref='y',
                    y0=dynamic_trailing_min,
                    y1=dynamic_trailing_max,
                    fillcolor='rgba(200, 100, 0, 0.22)',
                    layer='below',
                    line_width=0
                )
        
        # Add legend entry only if an entry trailing area was actually drawn.
        if entry_trailing_area_bounds is not None:
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode='lines',
                    line=dict(color='rgba(200, 100, 0, 0.22)', width=10),
                    name='Entry Trailing (Band)',
                    showlegend=True,
                )
            )
        
    
    # Add closing grid range as a shaded area (skip if Plotly slider is enabled; slider will manage shapes)
    if not enable_plotly_entry_spacing_slider:
        fig.add_shape(
            type='rect',
            xref='paper',
            x0=0,
            x1=1,
            yref='y',
            y0=normal_close_grid_min,
            y1=normal_close_grid_max,
            fillcolor='lightgreen',
            opacity=0.2,
            layer='below',
            line_width=0
        )
    # Add a dummy trace to represent Close Grid in the legend
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='lines',
        line=dict(color='rgba(0,255,0,0.2)', width=10),
        name='Close Grid (Area)',
        showlegend=True
    ))

    # Close grid lines: full-width shapes + hover markers.
    if (not enable_plotly_entry_spacing_slider) and len(normal_close_prices) > 0:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode='lines',
                name='Close Grid (Lines)',
                line=dict(color='rgba(0, 255, 0, 0.6)', dash='dot', width=1),
                showlegend=True,
                legendgroup='close',
            )
        )

        for close_price in normal_close_prices:
            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=float(close_price),
                y1=float(close_price),
                line=dict(color='rgba(0, 255, 0, 0.6)', dash='dot', width=1),
                layer='above',
            )

        hover_xs: list = []
        hover_ys: list[float] = []
        hover_text: list[str] = []
        for close_price in normal_close_prices:
            y = float(close_price)
            for x in x_samples_grid:
                hover_xs.append(x)
                hover_ys.append(y)
                hover_text.append(f"Close Grid: {y:.6f}")
            hover_xs.append(None)
            hover_ys.append(None)
            hover_text.append("")

        if hover_ys:
            fig.add_trace(
                go.Scatter(
                    x=hover_xs,
                    y=hover_ys,
                    mode='markers',
                    name='Close Grid (hover)',
                    showlegend=False,
                    legendgroup='close',
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate="%{text}<extra></extra>",
                    text=hover_text,
                )
            )
        
    if (not enable_plotly_entry_spacing_slider) and trailing_close_grid_min != 0 and trailing_close_grid_max != 0:
        fig.add_shape(
            type='rect',
            xref='paper',
            x0=0,
            x1=1,
            yref='y',
            y0=trailing_close_grid_min,
            y1=trailing_close_grid_max,
            fillcolor='blue',
            opacity=0.2,
            layer='below',
            line_width=0
        )
        # Add a close trace to represent Entry Grid in the legend
        fig.add_trace(go.Scatter(
            x=[None],
            y=[None],
            mode='lines',
            line=dict(color='rgba(0,0,255,0.2)', width=10),
            name='Close Trailing (Area)',
            showlegend=True
        ))

    # --- Trailing trigger lines (threshold + retracement)
    tb = data.trailing_price_bundle
    if entry_mode != GridTrailingMode.GridOnly:
        # Calculate threshold/trigger reference.
        # PB7 trailing entry logic is based on `position.price` (average entry price) and the trailing bundle.
        # For visualization we prefer the simulated trailing reference (avg price at trailing start) when available.
        
        relevant_entries = data.normal_entries_long if side == Side.Long else data.normal_entries_short
        ref_price = 0.0
        sim_ref = float(data.simulated_entry_trailing_ref_price_long) if side == Side.Long else float(data.simulated_entry_trailing_ref_price_short)
        if sim_ref > 0.0:
            ref_price = sim_ref
        
        # Determine Ref Price (fallback heuristics) based on Grid Ratio
        if ref_price <= 0.0 and bot_params.entry_trailing_grid_ratio < 0:
            # GRID FIRST: Ref is the price of the order that pushes us into trailing zone
            boundary_ratio = 1.0 - abs(bot_params.entry_trailing_grid_ratio)
            boundary_we = bot_params.total_wallet_exposure_limit * state_params.balance * boundary_ratio
            
            curr_size = data.position_long_enty.size if side == Side.Long else data.position_short_entry.size
            curr_price = data.position_long_enty.price if side == Side.Long else data.position_short_entry.price
            
            cum_cost = curr_size * curr_price
            
            # Default ref if no entries found (fallback to start price adjusted by initial dist)
            if side == Side.Long:
                ref_price = state_params.order_book.bid * (1.0 - bot_params.entry_initial_ema_dist)
            else:
                ref_price = state_params.order_book.ask * (1.0 + bot_params.entry_initial_ema_dist)

            if relevant_entries:
                for e in relevant_entries:
                    cost = e.qty * e.price
                    cum_cost += cost
                    ref_price = e.price
                    if cum_cost >= boundary_we:
                        break
        elif ref_price <= 0.0:
            # TRAILING FIRST: Ref is the First Entry Price
            if relevant_entries and len(relevant_entries) > 0:
                ref_price = relevant_entries[0].price
            else:
                # Fallback estimate
                if side == Side.Long:
                    ref_price = state_params.order_book.bid * (1.0 - bot_params.entry_initial_ema_dist)
                else:
                    ref_price = state_params.order_book.ask * (1.0 + bot_params.entry_initial_ema_dist)

           # Calculate Threshold from Ref Price
        eth_price = 0.0
        if side == Side.Long:
             eth_price = ref_price * (1.0 - bot_params.entry_trailing_threshold_pct)
        else:
             eth_price = ref_price * (1.0 + bot_params.entry_trailing_threshold_pct)

        if bot_params.entry_trailing_threshold_pct > 0.0:
            show_thr = True
            thr_name = 'Trailing Start (Threshold)'
            thr_dash = 'dash'
            thr_color = 'rgba(255, 255, 0, 0.6)'
            
            # Helper to detect if Threshold is eclipsed by Grid Phase (GridFirst mode)
            if bot_params.entry_trailing_grid_ratio < 0:
                boundary_ratio = 1.0 - abs(bot_params.entry_trailing_grid_ratio)
                boundary_we = bot_params.total_wallet_exposure_limit * state_params.balance * boundary_ratio
                
                # Use current position + entries to find boundary price
                relevant_entries = data.normal_entries_long if side == Side.Long else data.normal_entries_short
                
                # Careful: adjust for Side. 
                # Long entries: Price decreases. Short entries: Price increases.
                # Threshold Long: Price * (1 - pct). Short: Price * (1 + pct).
                
                curr_size = data.position_long_enty.size if side == Side.Long else data.position_short_entry.size
                curr_price = data.position_long_enty.price if side == Side.Long else data.position_short_entry.price
                
                cum_cost = curr_size * curr_price
                boundary_price = None

                if relevant_entries:
                    # Initial boundary is start price if entries exist? No, deep in grid.
                    # Scan entries
                    for e in relevant_entries:
                        cost = e.qty * e.price
                        cum_cost += cost
                        if cum_cost >= boundary_we:
                            # This order crosses boundary
                            boundary_price = e.price
                            break
                        boundary_price = e.price
                
                if boundary_price is not None:
                    # Check overlap
                    if side == Side.Long:
                        # If Threshold is HIGHER than Grid End (Boundary), it is "inside" grid
                        if eth_price > boundary_price:
                            thr_name = 'Trailing Threshold (Met during Grid)'
                            thr_dash = 'dot' 
                            thr_color = 'rgba(255, 255, 0, 0.2)' # Dimmed
                    else:
                        # Short: Threshold < Boundary => Inside
                        if eth_price < boundary_price:
                            thr_name = 'Trailing Threshold (Met during Grid)'
                            thr_dash = 'dot'
                            thr_color = 'rgba(255, 255, 0, 0.2)'

            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=float(eth_price),
                y1=float(eth_price),
                line=dict(color=thr_color, dash=thr_dash, width=1),
                layer='above',
            )
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode='lines',
                    name=thr_name,
                    line=dict(color=thr_color, dash=thr_dash, width=1),
                    showlegend=True,
                    legendgroup='entry_trailing',
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=x_samples_grid,
                    y=[eth_price] * len(x_samples_grid),
                    mode='markers',
                    name=f"{thr_name} (hover)",
                    showlegend=False,
                    legendgroup='entry_trailing',
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate=f"{thr_name}: %{{y:.6f}}<extra></extra>",
                )
            )
        retr_price: float | None = None
        if bot_params.entry_trailing_retracement_pct > 0.0:
            # PB7 behavior:
            # - If entry_trailing_threshold_pct <= 0: trailing is active immediately; trigger uses bundle min/max retracement.
            # - Else: trigger uses ref*(1 - threshold + retracement) (long) / ref*(1 + threshold - retracement) (short).
            if float(bot_params.entry_trailing_threshold_pct) <= 0.0:
                retr = (
                    tb.min_since_open * (1.0 + bot_params.entry_trailing_retracement_pct)
                    if side == Side.Long
                    else tb.max_since_open * (1.0 - bot_params.entry_trailing_retracement_pct)
                )
            elif sim_ref > 0.0:
                retr = (
                    ref_price * (1.0 - bot_params.entry_trailing_threshold_pct + bot_params.entry_trailing_retracement_pct)
                    if side == Side.Long
                    else ref_price * (1.0 + bot_params.entry_trailing_threshold_pct - bot_params.entry_trailing_retracement_pct)
                )
            else:
                retr = (
                    tb.min_since_open * (1.0 + bot_params.entry_trailing_retracement_pct)
                    if side == Side.Long
                    else tb.max_since_open * (1.0 - bot_params.entry_trailing_retracement_pct)
                )
            retr_price = float(retr)
            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=float(retr_price),
                y1=float(retr_price),
                line=dict(color='rgba(255, 255, 0, 0.35)', dash='dot', width=1),
                layer='above',
            )
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode='lines',
                    name='Trailing Trigger (Retracement)',
                    line=dict(color='rgba(255, 255, 0, 0.35)', dash='dot', width=1),
                    showlegend=True,
                    legendgroup='entry_trailing',
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=x_samples_grid,
                    y=[retr_price] * len(x_samples_grid),
                    mode='markers',
                    name='Trailing Trigger (Retracement) (hover)',
                    showlegend=False,
                    legendgroup='entry_trailing',
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate="Trailing Trigger (Retracement): %{y:.6f}<extra></extra>",
                )
            )

        # Visual helper: show the trigger zone (Threshold -> Retracement) as a separate band.
        # This reduces confusion when trailing levels fall within the overall grid range.
        if (
            (not enable_plotly_entry_spacing_slider)
            and bot_params.entry_trailing_threshold_pct > 0.0
            and bot_params.entry_trailing_retracement_pct > 0.0
            and eth_price > 0.0
            and retr_price is not None
            and abs(float(retr_price) - float(eth_price)) > 0.000001
        ):
            zone_y0 = min(float(eth_price), float(retr_price))
            zone_y1 = max(float(eth_price), float(retr_price))
            fig.add_shape(
                type='rect',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=zone_y0,
                y1=zone_y1,
                fillcolor='rgba(255, 255, 0, 0.08)',
                layer='below',
                line_width=0,
            )
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode='lines',
                    line=dict(color='rgba(255, 255, 0, 0.08)', width=10),
                    name='Entry Trailing (Trigger Zone)',
                    showlegend=True,
                    legendgroup='entry_trailing',
                )
            )

    if close_mode != GridTrailingMode.GridOnly:
        if bot_params.close_trailing_threshold_pct > 0.0:
            thr = (
                close_pos_price * (1.0 + bot_params.close_trailing_threshold_pct)
                if side == Side.Long
                else close_pos_price * (1.0 - bot_params.close_trailing_threshold_pct)
            )
            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=float(thr),
                y1=float(thr),
                line=dict(color='rgba(0, 0, 255, 0.6)', dash='dash', width=1),
                layer='above',
            )
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode='lines',
                    name='Trailing Start (Threshold)',
                    line=dict(color='rgba(0, 0, 255, 0.6)', dash='dash', width=1),
                    showlegend=True,
                    legendgroup='close_trailing',
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=x_samples_grid,
                    y=[thr] * len(x_samples_grid),
                    mode='markers',
                    name='Trailing Start (Threshold) (hover)',
                    showlegend=False,
                    legendgroup='close_trailing',
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate="Trailing Start (Threshold): %{y:.6f}<extra></extra>",
                )
            )
        if bot_params.close_trailing_retracement_pct > 0.0:
            retr = (
                tb.max_since_open * (1.0 - bot_params.close_trailing_retracement_pct)
                if side == Side.Long
                else tb.min_since_open * (1.0 + bot_params.close_trailing_retracement_pct)
            )
            fig.add_shape(
                type='line',
                xref='paper',
                x0=0,
                x1=1,
                yref='y',
                y0=float(retr),
                y1=float(retr),
                line=dict(color='rgba(0, 0, 255, 0.35)', dash='dot', width=1),
                layer='above',
            )
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode='lines',
                    name='Trailing Trigger (Retracement)',
                    line=dict(color='rgba(0, 0, 255, 0.35)', dash='dot', width=1),
                    showlegend=True,
                    legendgroup='close_trailing',
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=x_samples_grid,
                    y=[retr] * len(x_samples_grid),
                    mode='markers',
                    name='Trailing Trigger (Retracement) (hover)',
                    showlegend=False,
                    legendgroup='close_trailing',
                    marker=dict(size=8, color='rgba(0,0,0,0)'),
                    hovertemplate="Trailing Trigger (Retracement): %{y:.6f}<extra></extra>",
                )
            )

    # Determine Y-axis range
    all_prices_for_min = normal_entry_prices + normal_close_prices

    # Include trailing entry levels (visualization-only)
    pot_prices = data.potential_entry_trailing_prices_long if side == Side.Long else data.potential_entry_trailing_prices_short
    sim_prices = data.simulated_entry_trailing_prices_long if side == Side.Long else data.simulated_entry_trailing_prices_short
    extra_prices = sim_prices if (entry_mode != GridTrailingMode.GridOnly and sim_prices) else pot_prices
    if extra_prices:
        all_prices_for_min.extend([float(p) for p in extra_prices])

    # Ensure entry trailing area is included in the visible y-range
    if entry_trailing_area_bounds is not None:
        all_prices_for_min.extend([entry_trailing_area_bounds[0], entry_trailing_area_bounds[1]])
    
    # Include candles in Y-range calculation if they exist
    if data.historical_candles is not None:
        try:
             # Already processed into 'hc' df above if data.historical_candles exists
             # But 'hc' is local to that block. Let's re-access safe/stateless or better: extract earlier.
             # access directly as we know the structure
             if isinstance(data.historical_candles, dict):
                 # orient='list'
                 if "low" in data.historical_candles:
                     all_prices_for_min.extend(data.historical_candles["low"])
                 if "high" in data.historical_candles:
                     all_prices_for_min.extend(data.historical_candles["high"])
        except Exception:
            pass

    if not all_prices_for_min:
        y_min = 80
        y_max = 120
    else:
        y_min = min(all_prices_for_min)
        y_max = max(all_prices_for_min)
        # Add some padding (e.g. 5%)
        y_padding = (y_max - y_min) * 0.05
        if y_padding == 0:
            y_padding = 10
        y_min -= y_padding
        y_max += y_padding

    # Adjust Layout for Dark Mode
    fig.update_layout(
        template='plotly_dark',
        title=f'📈 {title_side} Entry and Close Grids Visualization',
        xaxis=dict(
            showticklabels=True,
            title="Date"
        ),
        yaxis=dict(
            range=[y_min, y_max],
        ),
        hovermode='closest',
        hoverdistance=30,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=1400
    )

    # Add vertical line for "Analysis Time" (the moment analysis is performed)
    # Prefer the UI-selected analysis time; fall back to the last candle if missing.
    try:
        analysis_ts = None
        if getattr(data, "analysis_time", None) is not None:
            analysis_ts = pd.to_datetime(data.analysis_time)
        elif data.historical_candles is not None:
            if isinstance(data.historical_candles, dict) and "timestamp" in data.historical_candles:
                ts_list = data.historical_candles["timestamp"]
                if len(ts_list) > 0:
                    analysis_ts = pd.to_datetime(ts_list[-1])

        if analysis_ts is not None:
            fig.add_vline(
                x=analysis_ts,
                line_width=2,
                line_dash="dot",
                line_color="cyan",
                annotation_text="Analysis Time",
                annotation_position="top left",
            )
    except Exception:
        pass

    # Historical simulation fill markers (B/S) on the chart
    try:
        sim_enabled = bool(st.session_state.get("gv_hist_sim_enabled", False))
        is_datetime_axis = isinstance(plot_x[0], (pd.Timestamp, datetime.datetime, np.datetime64))
        if sim_enabled and is_datetime_axis:
            fills = list(
                getattr(data, "historical_sim_fills_long" if side == Side.Long else "historical_sim_fills_short", []) or []
            )

            buy_x: list = []
            buy_y: list[float] = []
            buy_custom: list[list] = []
            sell_x: list = []
            sell_y: list[float] = []
            sell_custom: list[list] = []

            for ord_idx, f in enumerate(fills, start=0):
                if not isinstance(f, dict):
                    continue
                ts = f.get("timestamp")
                px = f.get("price")
                qty = f.get("qty")
                if ts is None or px is None or qty is None:
                    continue
                try:
                    ts_dt = pd.to_datetime(ts)
                    px_f = float(px)
                    qty_f = float(qty)
                except Exception:
                    continue
                if not math.isfinite(px_f) or not math.isfinite(qty_f) or qty_f == 0.0:
                    continue

                event = str(f.get("event", "") or "")
                order_type = str(f.get("order_type", "") or "")
                abs_qty = abs(qty_f)

                is_buy = qty_f > 0.0
                if is_buy:
                    buy_x.append(ts_dt)
                    buy_y.append(px_f)
                    buy_custom.append([int(ord_idx), abs_qty, event, order_type])
                else:
                    sell_x.append(ts_dt)
                    sell_y.append(px_f)
                    sell_custom.append([int(ord_idx), abs_qty, event, order_type])

            if buy_x:
                fig.add_trace(
                    go.Scatter(
                        x=buy_x,
                        y=buy_y,
                        mode="markers+text",
                        name="Fills (B)",
                        text=["B"] * len(buy_x),
                        textposition="middle center",
                        textfont=dict(color="white", size=10),
                        marker=dict(
                            symbol="circle",
                            size=16,
                            color="rgba(0, 200, 0, 1.0)",
                            line=dict(color="rgba(0, 0, 0, 0.7)", width=1),
                        ),
                        customdata=buy_custom,
                        hovertemplate=(
                            "Buy #%{customdata[0]} (%{customdata[2]})<br>"
                            "qty=%{customdata[1]:.6f}<br>"
                            "price=%{y:.6f}<br>"
                            "type=%{customdata[3]}<br>"
                            "%{x|%Y-%m-%d %H:%M}<extra></extra>"
                        ),
                    )
                )

            if sell_x:
                fig.add_trace(
                    go.Scatter(
                        x=sell_x,
                        y=sell_y,
                        mode="markers+text",
                        name="Fills (S)",
                        text=["S"] * len(sell_x),
                        textposition="middle center",
                        textfont=dict(color="white", size=10),
                        marker=dict(
                            symbol="circle",
                            size=16,
                            color="rgba(220, 0, 0, 1.0)",
                            line=dict(color="rgba(0, 0, 0, 0.7)", width=1),
                        ),
                        customdata=sell_custom,
                        hovertemplate=(
                            "Sell #%{customdata[0]} (%{customdata[2]})<br>"
                            "qty=%{customdata[1]:.6f}<br>"
                            "price=%{y:.6f}<br>"
                            "type=%{customdata[3]}<br>"
                            "%{x|%Y-%m-%d %H:%M}<extra></extra>"
                        ),
                    )
                )
    except Exception:
        pass

    # Render the figure using Streamlit
    st.plotly_chart(fig, width="stretch")

    # Fills table under the chart
    try:
        sim_enabled = bool(st.session_state.get("gv_hist_sim_enabled", False))
        if sim_enabled:
            fills = list(getattr(data, "historical_sim_fills_long" if side == Side.Long else "historical_sim_fills_short", []) or [])
            if fills:
                df_fills = pd.DataFrame(fills)
                if not df_fills.empty:
                    if "timestamp" in df_fills.columns:
                        df_fills["timestamp"] = pd.to_datetime(df_fills["timestamp"])
                        df_fills = df_fills.sort_values("timestamp").reset_index(drop=True)
                    df_fills.insert(0, "ord_idx", np.arange(0, len(df_fills), dtype=int))
                    cols = [
                        c
                        for c in [
                            "ord_idx",
                            "timestamp",
                            "event",
                            "qty",
                            "price",
                            "order_type",
                            "fee_paid",
                            "wallet_balance",
                            "pos_size",
                        ]
                        if c in df_fills.columns
                    ]
                    if cols:
                        df_fills = df_fills[cols]
                    if len(df_fills) > 5000:
                        df_fills = df_fills.iloc[-5000:]
                    st.dataframe(df_fills, use_container_width=True, height=250)
    except Exception:
        pass

    # Trailing context (bundle + derived trigger lines)
    if bot_params is not None:
        tb = data.trailing_price_bundle
        st.caption(
            f"Trailing bundle: min_since_open={tb.min_since_open:.6f} | max_since_min={tb.max_since_min:.6f} | "
            f"max_since_open={tb.max_since_open:.6f} | min_since_max={tb.min_since_max:.6f}"
        )

    if (not enable_plotly_entry_spacing_slider) and (pot_prices or sim_prices):
        if entry_mode != GridTrailingMode.GridOnly and sim_prices:
            st.caption(f"Trailing steps (simulated; reacts to trailing DD): {len(sim_prices)}")
        elif pot_prices:
            st.caption(f"Potential trailing (grid-only partition; independent of trailing DD): {len(pot_prices)}")

    # Display warnings
    if warnings:
        st.info("ℹ️ **Please note:**")
        for warning in warnings:
            st.write(f"⚠️ {warning}")
    
    st.markdown("---")
    return fig


def create_statistics(side: OrderType, data: GVData):
    
    if not data.isActive(side):
        return None
    
    entries = []
    closes = []
    wallet_exposure_limit = 0.0
    # Determine if we're dealing with LONG or SHORT side
    if side == OrderType.Default or side == Side.Long:
        title_side = "LONG"
        entries = data.normal_entries_long
        closes = data.normal_closes_long
        bp_for_limit = data.normal_bot_params_long
    else:
        title_side = "SHORT"
        entries = data.normal_entries_short
        closes = data.normal_closes_short
        bp_for_limit = data.normal_bot_params_short

    # Rust expects per-position `wallet_exposure_limit`; PB7 config stores `total_wallet_exposure_limit`.
    try:
        npos = int(getattr(bp_for_limit, "n_positions", 0) or 0)
        total_wel = float(getattr(bp_for_limit, "total_wallet_exposure_limit", 0.0) or 0.0)
        wallet_exposure_limit = (total_wel / float(npos)) if npos else total_wel
    except Exception:
        wallet_exposure_limit = float(getattr(bp_for_limit, "total_wallet_exposure_limit", 0.0) or 0.0)

    # Table-only: optionally append simulated trailing entries.
    trailing_entries: list[Order] = []
    if side == Side.Long:
        trailing_entries = list(getattr(data, "simulated_entry_trailing_orders_long", []) or [])
    else:
        trailing_entries = list(getattr(data, "simulated_entry_trailing_orders_short", []) or [])
    entries_for_table: list[Order] = list(entries) + list(trailing_entries)

    # If trailing is configured but Rust returns no trailing entries, call it out explicitly.
    try:
        bp = data.normal_bot_params_long if side == Side.Long else data.normal_bot_params_short
        mode = get_GridTrailing_mode(float(getattr(bp, "entry_trailing_grid_ratio", 0.0) or 0.0))
        if mode != GridTrailingMode.GridOnly and not trailing_entries:
            bal = float(getattr((data.state_params_long or data.state_params) if side == Side.Long else (data.state_params_short or data.state_params), "balance", 0.0) or 0.0)
            budget = float(bal) * float(wallet_exposure_limit)
            used = float(sum(float(o.qty) * float(o.price) for o in entries) if entries else 0.0)
            ratio = (used / budget) if budget > 0.0 else 0.0
            st.info(
                "No trailing entry orders (Rust returned `empty`). This commonly happens if the GridFirst cutoff is at the last grid order "
                "or the wallet exposure limit is already reached, so no further entries are allowed. "
                f"(entry exposure ratio ≈ {ratio:.6f} of limit)",
                icon="ℹ️",
            )
    except Exception:
        pass

    # Calculate statistics for entries
    total_entry_qty = sum(o.qty for o in entries)
    total_close_qty = sum(o.qty for o in closes)

    # Weighted average price for entries
    if total_entry_qty > 0:
        avg_entry_price = sum(o.qty * o.price for o in entries) / total_entry_qty
    else:
        avg_entry_price = None

    # Weighted average price for closes
    if total_close_qty > 0:
        avg_close_price = sum(o.qty * o.price for o in closes) / total_close_qty
    else:
        avg_close_price = None

    # Count of orders
    entry_count = len(entries)
    close_count = len(closes)

    # Display the main statistics as a table
    st.write(f"**{title_side} Statistics**")

    stats_data = {
        "Metric": [
            "Entry: Mode",
            "Entry: Orders",
            "Entry: Average Price",
            "Entry: Grid Size",
            "Close: Mode",
            "Close: Orders",
            "Close: Average Price",
            "Close: Grid Size",
        ],
        "Value": [
            str(data.long_entry_mode.name if title_side == "LONG" else data.short_entry_mode.name),
            str(entry_count),
            str(avg_entry_price) if avg_entry_price is not None else "N/A", 
            f"{int(data.long_entry_grid)}%" if title_side == "LONG" else f"{int(data.short_entry_grid)}%",
            str(data.long_close_mode.name if title_side == "LONG" else data.short_close_mode.name),
            str(close_count),
            str(avg_close_price) if avg_close_price is not None else "N/A",
            f"{int(data.long_close_grid)}%" if title_side == "LONG" else f"{int(data.short_close_grid)}%",
        ]
    }

    stats_df = pd.DataFrame(stats_data)
    st.table(stats_df)
    
    # Calulate Total Wallet Exposure
    entry_wallet_expore_sum = 0
    entry_twe_budegt = data.state_params.balance * wallet_exposure_limit
    entry_twe_pct = []
    
    for entry in entries_for_table:
        entry_wallet_expore_sum += entry.qty * entry.price
        entry_pct = int(entry_wallet_expore_sum / entry_twe_budegt * 100)
        if entry_pct < 0:
            entry_pct = 0
        if entry_pct > 100:
            entry_pct = 100
        entry_twe_pct.append(entry_pct)    
    
    # Detailed tables of entries and closes
    # For entries
    if entries_for_table:
        entry_details = {
            "Qty": [o.qty for o in entries_for_table],
            "Price": [o.price for o in entries_for_table],
            "Max-TWE% After": entry_twe_pct,
            "Order Type": [o.order_type_str or o.order_type.name for o in entries_for_table],
        }
        entry_df = pd.DataFrame(entry_details)
        st.write(f"**{title_side} Entry Orders**")
        st.table(entry_df)
    else:
        st.write(f"**{title_side} Entry Orders:** None")

    # Historical candle-walk simulation (chronological fills)
    if bool(st.session_state.get("gv_hist_sim_enabled", False)):
        sim_fills = list(
            getattr(data, "historical_sim_fills_long" if side == Side.Long else "historical_sim_fills_short", []) or []
        )
        if sim_fills:
            sdf = pd.DataFrame(sim_fills)
            # Ensure consistent column order if present
            cols = [c for c in ["timestamp", "event", "qty", "price", "order_type", "wallet_balance", "pos_size"] if c in sdf.columns]
            if cols:
                sdf = sdf[cols]
            st.write(f"**{title_side} Fills (Historical Simulation)**")
            st.dataframe(sdf, width="stretch")
        else:
            st.write(f"**{title_side} Fills (Historical Simulation):** None")

    # Calulate Total Wallet Exposure
    close_wallet_expore_sum = data.state_params.balance * wallet_exposure_limit
    close_twe_budegt = data.state_params.balance * wallet_exposure_limit
    close_twe_pct = []
    
    for close in closes:
        close_wallet_expore_sum -= close.qty * close.price
        close_pct = int(close_wallet_expore_sum / close_twe_budegt * 100)
        if close_pct < 0:
            close_pct = 0
        if close_pct > 100:
            close_pct = 100
            
        close_twe_pct.append(close_pct)  
        
    # For closes
    if closes:
        close_details = {
            "Qty": [o.qty for o in closes],
            "Price": [o.price for o in closes],
            "Max-TWE% After": close_twe_pct,
            "Order Type": [o.order_type_str or o.order_type.name for o in closes]
        }
        close_df = pd.DataFrame(close_details)
        st.write(f"**{title_side} Close Orders**")
        st.table(close_df)
    else:
        st.write(f"**{title_side} Close Orders:** None")

def adjust_order_quantities(orders: List[Order]) -> List[Order]:
    for order in orders:
        order.qty = abs(order.qty)
    return orders
    
def show_visualizer():
    # Load the config
    data = prepare_config()

    # Session-state migration: old key `gv_hist_symbol` -> new key `gv_hist_coin`
    if "gv_hist_symbol" in st.session_state and "gv_hist_coin" not in st.session_state:
        st.session_state.gv_hist_coin = st.session_state.gv_hist_symbol
    
    # Title
    if not data.title == "":
        st.subheader(data.title)

    # Create columns for organizing parameters
    col1, col2, col3 = st.columns(3)

    # Output placeholder for PB7 vs B vs C compare; filled later (after `data.prepare_data()`).
    compare_out = None
    
    with col1:
        sel_exc = ""
        sel_sym = ""
        hist_df = None
        min_day = None
        max_day = None

        with st.expander("Data (Exchange/Coin)", expanded=True):
            exchanges = get_available_exchanges_v7()
            cfg_exc = str(st.session_state.get("gv_hist_config_exchange", "") or "")
            if cfg_exc and cfg_exc not in exchanges:
                exchanges = [cfg_exc] + exchanges

            sel_exc = st.selectbox("Exchange", [""] + exchanges, key="gv_hist_exchange")

            # When exchange changes, reset coin selection (or set to first config coin for that exchange).
            last_exc = str(st.session_state.get("gv_hist_exchange_last", "") or "")
            if str(sel_exc or "") != last_exc:
                st.session_state.gv_hist_exchange_last = str(sel_exc or "")
                cfg_coins = list(st.session_state.get("gv_hist_config_coins", []) or [])
                if cfg_exc and str(sel_exc or "") == cfg_exc and cfg_coins:
                    st.session_state.gv_hist_coin = str(cfg_coins[0])
                else:
                    st.session_state.gv_hist_coin = ""

            if sel_exc:
                cfg_coins = list(st.session_state.get("gv_hist_config_coins", []) or [])
                if cfg_exc and str(sel_exc or "") == cfg_exc and cfg_coins:
                    coins = cfg_coins
                else:
                    coins = get_available_coins_v7(sel_exc)

                # Ensure the current selection is always in the option list to avoid Streamlit widget errors.
                cur_coin = str(st.session_state.get("gv_hist_coin", "") or "")
                if cur_coin and cur_coin not in coins:
                    coins = [cur_coin] + coins

                sel_sym = st.selectbox("Coin", [""] + coins, key="gv_hist_coin")
            else:
                st.selectbox("Coin", [], disabled=True, key="gv_hist_coin")

            if sel_exc and sel_sym:
                hist_df = load_historical_ohlcv_v7(sel_exc, sel_sym)

            if hist_df is not None and not hist_df.empty:
                st.info(f"Loaded {len(hist_df)} candles for {sel_sym}")

                # Auto-fill exchange params (best-effort) based on selected market
                auto_ep = bool(st.session_state.get("gv_auto_exchange_params", True))
                if auto_ep:
                    key = f"{sel_exc}:{sel_sym}"
                    if st.session_state.get("gv_auto_exchange_params_last") != key:
                        if _try_autofill_exchange_params(sel_exc, sel_sym, data):
                            st.session_state.gv_auto_exchange_params_last = key
                            # keep UI stable; no rerun needed

                # Force day-based selection (00:00) for easier backtest-style stepping
                min_time = hist_df.index.min().to_pydatetime()
                max_time = hist_df.index.max().to_pydatetime()
                try:
                    min_day = pd.to_datetime(min_time).normalize().to_pydatetime()
                    max_day = pd.to_datetime(max_time).normalize().to_pydatetime()
                except Exception:
                    min_day = min_time
                    max_day = max_time
            else:
                if sel_exc and sel_sym:
                    st.warning(f"No candles found for {sel_exc} / {sel_sym}.")

        with st.expander("Exchange / State", expanded=False):
            st.caption("Exchange Parameters")
            st.checkbox(
                "Auto-fill exchange params (best-effort)",
                value=bool(st.session_state.get("gv_auto_exchange_params", True)),
                key="gv_auto_exchange_params",
            )

            # IMPORTANT: widget keys must be per-market.
            # Otherwise Streamlit may keep old widget values and overwrite newly auto-filled params.
            _sel_exc = str(st.session_state.get("gv_hist_exchange", "") or "")
            _sel_coin = str(st.session_state.get("gv_hist_coin", "") or "")
            _ep_suffix = f"{_sel_exc}__{_sel_coin}" if (_sel_exc and _sel_coin) else "default"
            _ep_key_min_cost = f"ep_min_cost__{_ep_suffix}"
            _ep_key_min_qty = f"ep_min_qty__{_ep_suffix}"
            _ep_key_price_step = f"ep_price_step__{_ep_suffix}"
            _ep_key_qty_step = f"ep_qty_step__{_ep_suffix}"

            _ep_widget_keys = {
                "min_cost": _ep_key_min_cost,
                "min_qty": _ep_key_min_qty,
                "price_step": _ep_key_price_step,
                "qty_step": _ep_key_qty_step,
            }

            auto_ep_enabled = bool(st.session_state.get("gv_auto_exchange_params", True))
            market_ep = (
                _derive_exchange_params_from_market(_sel_exc, _sel_coin)
                if (_sel_exc and _sel_coin)
                else {"price_step": None, "qty_step": None, "min_qty": None, "min_cost": None, "c_mult": None}
            )
            st.session_state.setdefault("gv_ep_market_values", {})[_ep_suffix] = market_ep

            def _ep_set_override(field: str, widget_key: str, suffix: str) -> None:
                overrides = st.session_state.setdefault("gv_ep_overrides", {})
                d = overrides.setdefault(suffix, {})
                try:
                    d[field] = float(st.session_state.get(widget_key))
                except Exception:
                    d[field] = st.session_state.get(widget_key)
                st.session_state["gv_ep_overrides"] = overrides

            overrides_for_market = (st.session_state.get("gv_ep_overrides") or {}).get(_ep_suffix, {})

            # Keep widget state aligned with the effective value.
            def _effective_ep(field: str, current: float) -> tuple[float, str]:
                if field in overrides_for_market:
                    return float(overrides_for_market[field]), "manual"
                mv = market_ep.get(field)
                if auto_ep_enabled and mv is not None:
                    return float(mv), "market"
                return float(current), "state"

            _min_cost_eff, _min_cost_src = _effective_ep("min_cost", float(data.exchange_params.min_cost))
            _min_qty_eff, _min_qty_src = _effective_ep("min_qty", float(data.exchange_params.min_qty))
            _price_step_eff, _price_step_src = _effective_ep("price_step", float(data.exchange_params.price_step))
            _qty_step_eff, _qty_step_src = _effective_ep("qty_step", float(data.exchange_params.qty_step))

            def _sync_widget(key: str, desired: float, src: str) -> None:
                # Never overwrite a manual override.
                if src == "manual":
                    return
                cur = st.session_state.get(key)
                try:
                    if cur is None or not math.isclose(float(cur), float(desired), rel_tol=0.0, abs_tol=1e-18):
                        st.session_state[key] = float(desired)
                except Exception:
                    st.session_state[key] = float(desired)

            _sync_widget(_ep_key_min_cost, _min_cost_eff, _min_cost_src)
            _sync_widget(_ep_key_min_qty, _min_qty_eff, _min_qty_src)
            _sync_widget(_ep_key_price_step, _price_step_eff, _price_step_src)
            _sync_widget(_ep_key_qty_step, _qty_step_eff, _qty_step_src)

            if st.button(
                "Reset ExchangeParams to market (clear overrides)",
                key=f"ep_reset__{_ep_suffix}",
                disabled=not bool(overrides_for_market),
                help="Clears manual overrides for this market and re-applies market-derived values (if available).",
            ):
                overrides = st.session_state.setdefault("gv_ep_overrides", {})
                overrides.pop(_ep_suffix, None)
                st.session_state["gv_ep_overrides"] = overrides
                for field, wkey in _ep_widget_keys.items():
                    mv = market_ep.get(field)
                    if mv is not None:
                        st.session_state[wkey] = float(mv)
                st.rerun()

            with st.expander("Debug Data Sources (markets.json / OHLCV)", expanded=False):
                sel_exc = str(st.session_state.get("gv_hist_exchange", "") or "")
                sel_coin = str(st.session_state.get("gv_hist_coin", "") or "")
                if not sel_exc or not sel_coin:
                    st.write("Select Exchange + Coin in 'Historical Data Injection' to populate this debug view.")
                else:
                    st.caption("Market metadata (used for price_step/qty_step/min_* autofill)")
                    _render_debug_json(_market_metadata_source_debug(sel_exc, sel_coin))
                    st.caption("Derived ExchangeParams from market (what auto-fill can use)")
                    _render_debug_json(
                        {
                            "suffix": _ep_suffix,
                            "market_ep": (st.session_state.get("gv_ep_market_values") or {}).get(_ep_suffix, {}),
                            "manual_overrides": (st.session_state.get("gv_ep_overrides") or {}).get(_ep_suffix, {}),
                        }
                    )
                    st.caption("Active ExchangeParams widget keys (to detect stale state)")
                    _render_debug_json(
                        {
                            "suffix": _ep_suffix,
                            "keys": {
                                "min_cost": _ep_key_min_cost,
                                "min_qty": _ep_key_min_qty,
                                "price_step": _ep_key_price_step,
                                "qty_step": _ep_key_qty_step,
                            },
                            "session_state_values": {
                                "min_cost": st.session_state.get(_ep_key_min_cost),
                                "min_qty": st.session_state.get(_ep_key_min_qty),
                                "price_step": st.session_state.get(_ep_key_price_step),
                                "qty_step": st.session_state.get(_ep_key_qty_step),
                            },
                        }
                    )
                    st.caption("OHLCV disk sources (historical_data vs CandlestickManager cache)")
                    _render_debug_json(_ohlcv_source_debug(sel_exc, sel_coin))

            col_ep1, col_ep2 = st.columns(2)
            with col_ep1:
                data.exchange_params.min_cost = st.number_input(
                    "min_cost",
                    value=float(_min_cost_eff),
                    step=0.1,
                    key=_ep_key_min_cost,
                    on_change=_ep_set_override,
                    args=("min_cost", _ep_key_min_cost, _ep_suffix),
                )
                if _min_cost_src == "market":
                    st.caption(f"source: market ({market_ep.get('min_cost')})")
                elif _min_cost_src == "manual":
                    st.caption(f"source: manual override (market: {market_ep.get('min_cost')})")
                else:
                    if market_ep.get("min_cost") is None:
                        st.caption("source: manual/state (market: None — not provided in markets.json)")
                    else:
                        st.caption(f"source: manual/state (market: {market_ep.get('min_cost')})")

                data.exchange_params.min_qty = st.number_input(
                    "min_qty",
                    value=float(_min_qty_eff),
                    step=0.001,
                    format="%.6f",
                    key=_ep_key_min_qty,
                    on_change=_ep_set_override,
                    args=("min_qty", _ep_key_min_qty, _ep_suffix),
                )
                if _min_qty_src == "market":
                    st.caption(f"source: market ({market_ep.get('min_qty')})")
                elif _min_qty_src == "manual":
                    st.caption(f"source: manual override (market: {market_ep.get('min_qty')})")
                else:
                    st.caption(f"source: manual/state (market: {market_ep.get('min_qty')})")
            with col_ep2:
                data.exchange_params.price_step = st.number_input(
                    "price_step",
                    value=float(_price_step_eff),
                    step=0.000001,
                    format="%.6f",
                    key=_ep_key_price_step,
                    on_change=_ep_set_override,
                    args=("price_step", _ep_key_price_step, _ep_suffix),
                )
                if _price_step_src == "market":
                    st.caption(f"source: market ({market_ep.get('price_step')})")
                elif _price_step_src == "manual":
                    st.caption(f"source: manual override (market: {market_ep.get('price_step')})")
                else:
                    st.caption(f"source: manual/state (market: {market_ep.get('price_step')})")
                data.exchange_params.qty_step = st.number_input(
                    "qty_step",
                    value=float(_qty_step_eff),
                    step=0.001,
                    format="%.6f",
                    key=_ep_key_qty_step,
                    on_change=_ep_set_override,
                    args=("qty_step", _ep_key_qty_step, _ep_suffix),
                )
                if _qty_step_src == "market":
                    st.caption(f"source: market ({market_ep.get('qty_step')})")
                elif _qty_step_src == "manual":
                    st.caption(f"source: manual override (market: {market_ep.get('qty_step')})")
                else:
                    st.caption(f"source: manual/state (market: {market_ep.get('qty_step')})")

            # Heuristic warning: coarse tick size collapses grid prices into duplicates
            try:
                ref_px = float(data.state_params.order_book.bid or data.state_params.order_book.ask or 0.0)
                if ref_px > 0 and float(data.exchange_params.price_step) >= ref_px * 0.01:
                    st.warning(
                        f"price_step={data.exchange_params.price_step} is very coarse vs price~{ref_px:.6f}. "
                        "This can collapse many grid levels into the same rounded price, so Rust returns only a few orders."
                    )
            except Exception:
                pass

            st.divider()

            # Additional State Params
            sp_col1, sp_col2 = st.columns(2)
            with sp_col1:
                data.state_params.balance = float(
                    st.number_input(
                        "Wallet Balance (State)",
                        value=float(data.state_params.balance),
                        step=10.0,
                        key="state_balance",
                    )
                )
            with sp_col2:
                data.state_params.entry_volatility_logrange_ema_1h = float(
                    st.number_input(
                        "Vol (LogRange EMA)",
                        value=float(data.state_params.entry_volatility_logrange_ema_1h),
                        step=0.01,
                        format="%.4f",
                        key="state_entry_volatility_logrange_ema_1h",
                    )
                )

        sel_time = None
        context_days = 5.0
        if hist_df is not None and not hist_df.empty and min_day is not None and max_day is not None:
            with st.expander("Time & View", expanded=True):
                # --- Playback Logic ---
                # When exchange/coin changes, reset viz time so the chart updates immediately.
                # Important: Streamlit widgets with a fixed `key` can keep their prior value and ignore
                # programmatic session_state changes. Use a per-market slider key.
                sel_key = f"{sel_exc}:{sel_sym}"
                viz_slider_key = f"gv_viz_time__{sel_exc}__{sel_sym}"
                viz_date_key = f"gv_start_date__{sel_exc}__{sel_sym}"
                viz_sync_key = f"gv_time_sync__{sel_exc}__{sel_sym}"

                if st.session_state.get("gv_viz_time_for") != sel_key:
                    st.session_state.gv_viz_time = min_day
                    st.session_state.gv_viz_time_for = sel_key

                # Clamp + initialize the per-market slider state
                try:
                    cur_viz_time = st.session_state.get("gv_viz_time")
                    if cur_viz_time is None or cur_viz_time < min_day or cur_viz_time > max_day:
                        st.session_state.gv_viz_time = min_day
                except Exception:
                    st.session_state.gv_viz_time = min_day

                try:
                    cur_slider_time = st.session_state.get(viz_slider_key)
                    if cur_slider_time is None or cur_slider_time < min_day or cur_slider_time > max_day:
                        st.session_state[viz_slider_key] = st.session_state.gv_viz_time
                except Exception:
                    st.session_state[viz_slider_key] = st.session_state.gv_viz_time

                # --- Bidirectional sync between Start Date and day-slider ---
                # We only push changes from the control that changed since last run,
                # otherwise Streamlit reruns would constantly overwrite the user's input.
                try:
                    if viz_date_key not in st.session_state or st.session_state.get(viz_date_key) is None:
                        st.session_state[viz_date_key] = pd.to_datetime(st.session_state[viz_slider_key]).date()
                except Exception:
                    st.session_state[viz_date_key] = pd.to_datetime(min_day).date()

                # Clamp date into available range
                try:
                    _min_d = pd.to_datetime(min_day).date()
                    _max_d = pd.to_datetime(max_day).date()
                    cur_date = st.session_state.get(viz_date_key)
                    if cur_date < _min_d:
                        cur_date = _min_d
                    if cur_date > _max_d:
                        cur_date = _max_d
                    st.session_state[viz_date_key] = cur_date
                except Exception:
                    pass

                try:
                    cur_slider_date = pd.to_datetime(st.session_state.get(viz_slider_key)).date()
                except Exception:
                    cur_slider_date = pd.to_datetime(min_day).date()
                    st.session_state[viz_slider_key] = min_day

                sync = st.session_state.get(viz_sync_key) or {}
                last_slider_date = sync.get("slider_date")
                last_date_input = sync.get("date_input")

                slider_changed = (last_slider_date is not None) and (cur_slider_date != last_slider_date)
                date_changed = (last_date_input is not None) and (st.session_state.get(viz_date_key) != last_date_input)

                if slider_changed and not date_changed:
                    # User moved slider -> update date input
                    st.session_state[viz_date_key] = cur_slider_date
                elif date_changed and not slider_changed:
                    # User changed date input -> update slider
                    try:
                        st.session_state[viz_slider_key] = datetime.datetime.combine(
                            st.session_state[viz_date_key], datetime.time(0, 0)
                        )
                    except Exception:
                        st.session_state[viz_slider_key] = min_day
                else:
                    # First run or ambiguous: just reconcile mismatch by snapping slider to date input
                    try:
                        if cur_slider_date != st.session_state[viz_date_key]:
                            st.session_state[viz_slider_key] = datetime.datetime.combine(
                                st.session_state[viz_date_key], datetime.time(0, 0)
                            )
                    except Exception:
                        pass

                start_date = st.date_input(
                    "Start Date",
                    value=st.session_state[viz_date_key],
                    min_value=pd.to_datetime(min_day).date(),
                    max_value=pd.to_datetime(max_day).date(),
                    key=viz_date_key,
                    help="Select the analysis/simulation start date. Time is fixed to 00:00.",
                )

                sel_time = st.slider(
                    "Select Time",
                    min_value=min_day,
                    max_value=max_day,
                    value=st.session_state[viz_slider_key],
                    step=datetime.timedelta(days=1),
                    format="YYYY-MM-DD 00:00",
                    key=viz_slider_key,
                )

                # Record last values for next rerun
                try:
                    st.session_state[viz_sync_key] = {
                        "slider_date": pd.to_datetime(sel_time).date(),
                        "date_input": start_date,
                    }
                except Exception:
                    pass

                # Keep canonical state in sync
                st.session_state.gv_viz_time = sel_time

                # Make the selected analysis time available to plotting.
                data.analysis_time = sel_time

                context_days = st.slider(
                    "Chart Context (Days)", min_value=0.5, max_value=60.0, value=5.0, step=0.5
                )
        else:
            with st.expander("Time & View", expanded=True):
                st.info("Select an Exchange/Coin with available local history.", icon="ℹ️")

        with st.expander("Config (Raw JSON)", expanded=False):
            json_str = st.text_area("hidden", data.to_json(), height=1000, label_visibility="collapsed")
            if st.button("Apply"):
                data = GVData.from_json(json_str)
                st.session_state.v7_grid_visualizer_data = data
                clear_v7_tuning_keys()
                st.rerun()

        if hist_df is not None and not hist_df.empty and sel_time is not None:
            with st.expander("Simulation (Mode B/C)", expanded=False):
                st.caption("Historical simulation (candle-walk)")
                st.checkbox(
                    "Simulate entry fills over historical candles",
                    value=bool(st.session_state.get("gv_hist_sim_enabled", False)),
                    key="gv_hist_sim_enabled",
                    help="Walk forward candle-by-candle from the selected time, using Rust next-entry each step; records filled entry orders.",
                )

                st.radio(
                    "Simulation mode",
                    options=["Local (B)", "PB7 backtest engine (C)"],
                    index=0 if str(st.session_state.get("gv_hist_sim_mode", "Local (B)")) == "Local (B)" else 1,
                    key="gv_hist_sim_mode",
                    horizontal=True,
                    help=(
                        "Local (B): PBGui simuliert Candle-für-Candle (Orchestrator-Orders + lokale Fill-Regeln) und kann deshalb Grids/Trailing pro Candle anzeigen. "
                        "PB7 (C): verwendet die echte PB7 Rust Backtest-Engine (Ground truth) für Vergleich – aber ohne per-Candle offene Grid-Ladder-Details."
                    ),
                )
                sim_col1, sim_col2 = st.columns(2)
                with sim_col1:
                    st.number_input(
                        "Sim max candles",
                        min_value=10,
                        max_value=20000,
                        value=int(st.session_state.get("gv_hist_sim_max_candles", 2000)),
                        step=10,
                        key="gv_hist_sim_max_candles",
                    )
                with sim_col2:
                    st.number_input(
                        "Sim max entry fills",
                        min_value=1,
                        max_value=2000,
                        value=int(st.session_state.get("gv_hist_sim_max_orders", 200)),
                        step=1,
                        key="gv_hist_sim_max_orders",
                    )

            with st.expander("Compare (PB7/B/C)", expanded=False):
                st.checkbox(
                    "Show compare table (PB7 vs B vs C)",
                    value=bool(st.session_state.get("gv_hist_compare_enabled", False)),
                    key="gv_hist_compare_enabled",
                    help="Loads PB7 backtest folder `fills.csv` and aligns it with Mode B and Mode C fills.",
                )

                st.radio(
                    "Compare mode",
                    options=["PB7 vs B vs C", "B vs C only (no PB7)"],
                    index=0 if str(st.session_state.get("gv_hist_compare_mode", "PB7 vs B vs C")) == "PB7 vs B vs C" else 1,
                    key="gv_hist_compare_mode",
                    horizontal=True,
                )

                st.number_input(
                    "Compare max candles (1m)",
                    min_value=100,
                    max_value=20000,
                    value=int(st.session_state.get("gv_hist_compare_max_candles", 2000)),
                    step=100,
                    key="gv_hist_compare_max_candles",
                    help="Used for 'B vs C only' (and as fallback if no PB7 fills range is used).",
                )

                compare_mode = str(st.session_state.get("gv_hist_compare_mode", "PB7 vs B vs C") or "PB7 vs B vs C")
                show_pb7_inputs = compare_mode == "PB7 vs B vs C"

                if show_pb7_inputs:
                    st.text_input(
                        "PB7 backtest folder (contains fills.csv)",
                        value=str(st.session_state.get("gv_hist_compare_pb7_dir", "") or ""),
                        key="gv_hist_compare_pb7_dir",
                        help="Example: /home/mani/software/pb7/backtests/pbgui/<exchange>_<coin>USDT/<exchange>/YYYY-MM-DDTHH_MM_SS",
                    )
                    st.checkbox(
                        "Use fills.csv time range for B/C",
                        value=bool(st.session_state.get("gv_hist_compare_use_pb7_range", True)),
                        key="gv_hist_compare_use_pb7_range",
                        help="Runs Mode B and Mode C across the same start/end timestamps found in fills.csv (with warmup).",
                    )
                else:
                    st.caption("B vs C compares fills from the selected analysis time for the chosen number of 1m candles.")
                st.checkbox(
                    "Mismatches only",
                    value=bool(st.session_state.get("gv_hist_compare_mismatches_only", True)),
                    key="gv_hist_compare_mismatches_only",
                )

                # Placeholder: rendered later in the same expander (after compute step further down).
                compare_out = st.container()

            long_active = data.isActive(Side.Long)
            short_active = data.isActive(Side.Short)

            def _ind_key(bp: BotParams):
                # Indicators used here depend on EMA spans and the volatility EMA span.
                ema_pair = tuple(sorted((float(bp.ema_span_0), float(bp.ema_span_1))))
                vol_span = float(bp.entry_volatility_ema_span_hours)
                return (ema_pair, vol_span)

            both_active = long_active and short_active
            same_indicators = both_active and (_ind_key(data.normal_bot_params_long) == _ind_key(data.normal_bot_params_short))

            with st.expander("Movie Builder", expanded=False):
                ani_col1, ani_col2, ani_col3 = st.columns(3)
                with ani_col1:
                    ani_frames = st.number_input("Frames", min_value=10, max_value=5000, value=200)
                with ani_col2:
                    ani_step_name = st.selectbox("Step Size", ["1m", "5m", "15m", "1h", "4h", "1d"], index=4)
                    ani_steps_mins = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
                    ani_step_min = ani_steps_mins[ani_step_name]
                with ani_col3:
                    gen_movie = st.button("Generate Movie")

                st.radio(
                    "Movie engine",
                    options=["Local (B) – full grids", "PB7 backtest engine (C) – upcoming fills"],
                    index=0 if str(st.session_state.get("gv_movie_engine", "Local (B) – full grids")) == "Local (B) – full grids" else 1,
                    key="gv_movie_engine",
                    horizontal=True,
                    help=(
                        "Local (B): rendert die sich entwickelnden Entry/Close-Grids + Trailing (volle Grid-Ladders). "
                        "PB7 (C): zeigt Fills aus der PB7 Backtest-Engine (Ground truth) und previewt kommende Fills; offene Grids pro Candle werden von der Engine nicht geliefert."
                    ),
                )

                movie_out = st.container()
                if gen_movie:
                    side_val = 1 if long_active else (2 if short_active else 1)
                    if str(st.session_state.get("gv_movie_engine")) == "PB7 backtest engine (C) – upcoming fills":
                        generate_animation_v7_modec(
                            start_time=pd.to_datetime(sel_time),
                            frames=int(ani_frames),
                            step_mins=int(ani_step_min),
                            hist_df=hist_df,
                            exchange=str(sel_exc),
                            symbol=str(sel_sym),
                            context_days=float(context_days),
                            side_val=int(side_val),
                            data_template=data,
                            output_container=movie_out,
                        )
                    else:
                        generate_animation_v7_modeb(
                            start_time=pd.to_datetime(sel_time),
                            frames=int(ani_frames),
                            step_mins=int(ani_step_min),
                            hist_df=hist_df,
                            exchange=str(sel_exc),
                            symbol=str(sel_sym),
                            context_days=float(context_days),
                            side_val=int(side_val),
                            data_template=data,
                            output_container=movie_out,
                        )
                else:
                    # Keep rendering the last generated movie on reruns until a new one is generated.
                    with movie_out:
                        if str(st.session_state.get("gv_movie_engine")) == "PB7 backtest engine (C) – upcoming fills":
                            fig = st.session_state.get("gv_movie_fig_modec")
                            df_fills = st.session_state.get("gv_movie_fills_modec")
                        else:
                            fig = st.session_state.get("gv_movie_fig_modeb")
                            df_fills = st.session_state.get("gv_movie_fills_modeb")
                        if fig is not None:
                            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})
                        try:
                            if df_fills is not None and hasattr(df_fills, "empty") and not df_fills.empty:
                                st.dataframe(df_fills, use_container_width=True, height=250)
                        except Exception:
                            pass

            # Automatically inject state based on slider
            idx = hist_df.index.get_indexer([sel_time], method='nearest')[0]
            row_time = hist_df.index[idx]

            # Compute indicator-derived state(s) at this point.
            # Rules:
            # 1) Only compute for active sides.
            # 2) If both active and indicator params are identical, compute once and reuse.
            primary_bp = data.normal_bot_params_long if (long_active or not short_active) else data.normal_bot_params_short
            other_bp = data.normal_bot_params_short if primary_bp is data.normal_bot_params_long else data.normal_bot_params_long

            close_px = float(hist_df.iloc[idx]["close"])

            df_primary = calculate_v7_indicators(
                hist_df,
                primary_bp.ema_span_0,
                primary_bp.ema_span_1,
                primary_bp.entry_volatility_ema_span_hours,
            )
            row_primary = df_primary.iloc[idx]

            row_other = None
            if both_active and (not same_indicators):
                df_other = calculate_v7_indicators(
                    hist_df,
                    other_bp.ema_span_0,
                    other_bp.ema_span_1,
                    other_bp.entry_volatility_ema_span_hours,
                )
                row_other = df_other.iloc[idx]

            def _apply_derived(sp: StateParams, row_any) -> StateParams:
                sp.entry_volatility_logrange_ema_1h = float(row_any["volatility"])
                e0 = float(row_any.get("ema_0", close_px))
                e1 = float(row_any.get("ema_1", close_px))
                e2 = float(row_any.get("ema_2", close_px))
                sp.ema_bands.lower = float(min(e0, e1, e2))
                sp.ema_bands.upper = float(max(e0, e1, e2))
                sp.order_book.bid = close_px
                sp.order_book.ask = close_px
                return sp

            # Per-side derived states
            data.state_params_long = None
            data.state_params_short = None

            if long_active:
                sp_long = copy.deepcopy(data.state_params)
                row_long = row_primary if primary_bp is data.normal_bot_params_long else (row_primary if same_indicators else row_other)
                data.state_params_long = _apply_derived(sp_long, row_long)

            if short_active:
                sp_short = copy.deepcopy(data.state_params)
                row_short = row_primary if primary_bp is data.normal_bot_params_short else (row_primary if same_indicators else row_other)
                data.state_params_short = _apply_derived(sp_short, row_short)

            # Keep legacy/shared state in sync for any code paths still reading `data.state_params`.
            _apply_derived(data.state_params, row_primary)

            # Also update trailing bundle to current price to avoid trailing logic using 100.0 vs 60k
            data.trailing_price_bundle.max_since_open = close_px
            data.trailing_price_bundle.min_since_open = close_px
            data.trailing_price_bundle.max_since_min = close_px
            data.trailing_price_bundle.min_since_max = close_px

            # Store historical slice for plotting.
            # Intention: show candles FROM the selected time FOR the chosen context window.
            # (So users can see whether subsequent candles hit the computed grid.)
            # Assumes 1m candles (1440 per day).
            context_candles = int(context_days * 1440)
            # If historical simulation is enabled, ensure we plot enough forward candles to cover the sim horizon.
            try:
                if bool(st.session_state.get("gv_hist_sim_enabled", False)):
                    sim_max_candles = int(st.session_state.get("gv_hist_sim_max_candles", 2000) or 2000)
                    if sim_max_candles > 0:
                        context_candles = max(int(context_candles), int(sim_max_candles))
            except Exception:
                pass
            available_candles = len(hist_df)

            remaining_candles = max(0, available_candles - idx)
            if context_candles > remaining_candles:
                st.warning(
                    f"⚠️ Requested {context_days} days ({context_candles} candles) from selected time, "
                    f"but only {remaining_candles} candles (~{remaining_candles/1440:.1f} days) are available. "
                    "Displaying all available candles from selected time."
                )

            end_slice = min(available_candles, idx + context_candles + 1)
            slice_df = hist_df.iloc[idx:end_slice].copy()

            # Store as dict for serialization
            slice_df.reset_index(inplace=True)
            if "timestamp" not in slice_df.columns and "index" in slice_df.columns:
                slice_df.rename(columns={"index": "timestamp"}, inplace=True)
            data.historical_candles = slice_df.to_dict(orient='list')

            with st.expander("Debug", expanded=False):
                st.write(f"Ref Time: {row_time}")
                st.write(f"Ref Close: {close_px}")
                st.write(f"Active: long={long_active} short={short_active} | same_indicators={same_indicators}")
                st.write(f"Ref Vol (primary): {float(row_primary['volatility'])}")
                st.write(f"Slice Rows: {len(slice_df)}")
                st.write("Head:", slice_df.head(3))
                st.write("Tail:", slice_df.tail(3))
        else:
            with st.expander("Simulation (Mode B/C)", expanded=False):
                st.info("Requires loaded OHLCV candles.", icon="ℹ️")
            with st.expander("Compare PB7 vs B vs C", expanded=False):
                st.info("Requires loaded OHLCV candles.", icon="ℹ️")
            with st.expander("Movie Builder", expanded=False):
                st.info("Requires loaded OHLCV candles.", icon="ℹ️")
            with st.expander("Debug", expanded=False):
                st.info("Requires loaded OHLCV candles.", icon="ℹ️")
    
    with col2:
        if data.isActive(Side.Long):
            st.subheader("LONG")
            panel_long = st.segmented_control(
                "Segment",
                options=["Entry grid", "Entry trailing", "Close grid", "Close trailing", "Risk/State", "Filters/Unstuck"],
                default=str(st.session_state.get("gv_tuning_segment_long", "Entry grid")),
                key="gv_tuning_segment_long",
            )
            l = data.normal_bot_params_long
            if panel_long == "Entry grid":
                st.caption(f"Bot Limit: Wallet Exposure {l.total_wallet_exposure_limit:.2f}x")
                l.total_wallet_exposure_limit = float(st.slider("total_wallet_exposure_limit", 0.1, 50.0, float(l.total_wallet_exposure_limit), 0.1, key="long_twe_override_entry"))
                
                l.entry_initial_qty_pct = float(st.slider("entry_initial_qty_pct", 0.0, 1.0, float(l.entry_initial_qty_pct), 0.001, key="long_entry_initial_qty_pct"))
                l.entry_initial_ema_dist = float(st.slider("entry_initial_ema_dist", -1.0, 1.0, float(l.entry_initial_ema_dist), 0.001, key="long_entry_initial_ema_dist"))
                
                if l.entry_trailing_grid_ratio > 0 and l.entry_trailing_threshold_pct > 0 and l.entry_initial_ema_dist < l.entry_trailing_threshold_pct:
                    st.warning(
                        f"⚠️ **Logic Warning**: Your Grid starts at **{l.entry_initial_ema_dist:.1%}** distance, "
                        f"but Trailing only tries to activate at **{l.entry_trailing_threshold_pct:.1%}**.\n\n"
                        "Since **Trailing First** is active (ratio > 0), the bot may skip initial levels or wait until the threshold is met."
                    )

                l.entry_grid_spacing_pct = float(st.slider("entry_grid_spacing_pct", 0.0, 0.5, float(l.entry_grid_spacing_pct), 0.0005, key="long_entry_grid_spacing_pct", format="%.4f"))
                if l.entry_grid_spacing_pct > 0.05:
                    st.warning(f"Spacing {l.entry_grid_spacing_pct*100:.1f}% is very high! This will result in few grid levels.")

                l.entry_grid_spacing_we_weight = float(st.slider("entry_grid_spacing_we_weight", 0.0, 10.0, float(l.entry_grid_spacing_we_weight), 0.01, key="long_entry_grid_spacing_we_weight"))
                
                # --- CALC TEOR GRID COUNT ---
                try:
                    twe_budget = l.total_wallet_exposure_limit * data.state_params.balance
                    # In PBv7 sizing, entry_initial_qty_pct is applied to the TWE budget (balance * total_wallet_exposure_limit)
                    curr_cost = l.entry_initial_qty_pct * twe_budget
                    
                    # For approximation, assume price = 1.0 so Qty = Cost
                    # In reality, min_qty check depends on Price.
                    # Here we check against min_cost
                    cost_limit = data.exchange_params.min_cost
                    
                    count = 1
                    ddf = l.entry_grid_double_down_factor
                    
                    break_reason = "TWE"
                    
                    total_spent = curr_cost
                    
                    while total_spent < twe_budget:
                        next_order_cost = curr_cost * (ddf ** count)

                        if next_order_cost < cost_limit:
                            break_reason = f"Min Cost ({next_order_cost:.4f} < {cost_limit})"
                            break

                        if total_spent + next_order_cost > twe_budget:
                            break_reason = "Wallet Limit"
                            break

                        total_spent += next_order_cost
                        count += 1
                        if count > 100:
                            break
                    
                    st.info(f"Theoretical Max Grid Orders: **{count}** (Reason: {break_reason}) | Cost: {total_spent:.2f} / {twe_budget:.2f}")
                except Exception as e:
                    st.error(f"Error approximating grid count: {e}")
                # -----------------------------
                l.entry_grid_double_down_factor = float(st.slider("entry_grid_double_down_factor", 0.01, 10.0, float(l.entry_grid_double_down_factor), 0.01, key="long_entry_grid_double_down_factor"))
                l.entry_grid_spacing_volatility_weight = float(
                    st.slider(
                        "entry_grid_spacing_volatility_weight",
                        0.0,
                        400.0,
                        float(l.entry_grid_spacing_volatility_weight),
                        1.0,
                        key="long_entry_grid_spacing_volatility_weight"
                    )
                )
                l.entry_volatility_ema_span_hours = float(st.slider("entry_volatility_ema_span_hours", 1.0, 4000.0, float(l.entry_volatility_ema_span_hours), 1.0, key="long_entry_volatility_ema_span_hours"))
                l.ema_span_0 = float(st.slider("ema_span_0", 1.0, 10000.0, float(l.ema_span_0), 1.0, key="long_ema_span_0"))
                l.ema_span_1 = float(st.slider("ema_span_1", 1.0, 10000.0, float(l.ema_span_1), 1.0, key="long_ema_span_1"))
            elif panel_long == "Entry trailing":
                l.entry_trailing_threshold_pct = float(st.slider("entry_trailing_threshold_pct", 0.0, 1.0, float(l.entry_trailing_threshold_pct), 0.001, key="long_entry_trailing_threshold_pct"))
                l.entry_trailing_retracement_pct = float(st.slider("entry_trailing_retracement_pct", 0.0, 1.0, float(l.entry_trailing_retracement_pct), 0.001, key="long_entry_trailing_retracement_pct"))
                l.entry_trailing_grid_ratio = float(st.slider("entry_trailing_grid_ratio", -1.0, 1.0, float(l.entry_trailing_grid_ratio), 0.01, key="long_entry_trailing_grid_ratio"))
                l.entry_trailing_double_down_factor = float(
                    st.slider(
                        "entry_trailing_double_down_factor",
                        0.0,
                        10.0,
                        float(l.entry_trailing_double_down_factor or l.entry_grid_double_down_factor),
                        0.01,
                        key="long_entry_trailing_double_down_factor"
                    )
                )
                l.entry_trailing_threshold_we_weight = float(st.slider("entry_trailing_threshold_we_weight", 0.0, 20.0, float(l.entry_trailing_threshold_we_weight), 0.1, key="long_entry_trailing_threshold_we_weight"))
                l.entry_trailing_threshold_volatility_weight = float(
                    st.slider(
                        "entry_trailing_threshold_volatility_weight",
                        0.0,
                        400.0,
                        float(l.entry_trailing_threshold_volatility_weight),
                        1.0,
                        key="long_entry_trailing_threshold_volatility_weight"
                    )
                )
                l.entry_trailing_retracement_we_weight = float(st.slider("entry_trailing_retracement_we_weight", 0.0, 20.0, float(l.entry_trailing_retracement_we_weight), 0.1, key="long_entry_trailing_retracement_we_weight"))
                l.entry_trailing_retracement_volatility_weight = float(
                    st.slider(
                        "entry_trailing_retracement_volatility_weight",
                        0.0,
                        400.0,
                        float(l.entry_trailing_retracement_volatility_weight),
                        1.0,
                        key="long_entry_trailing_retracement_volatility_weight"
                    )
                )
            elif panel_long == "Close grid":
                l.close_grid_markup_end = float(st.slider("close_grid_markup_end", 0.0, 1.0, float(l.close_grid_markup_end), 0.001, key="long_close_grid_markup_end"))
                l.close_grid_markup_start = float(st.slider("close_grid_markup_start", 0.0, 1.0, float(l.close_grid_markup_start), 0.001, key="long_close_grid_markup_start"))
                l.close_grid_qty_pct = float(st.slider("close_grid_qty_pct", 0.0, 1.0, float(l.close_grid_qty_pct), 0.01, key="long_close_grid_qty_pct"))
                l.close_trailing_grid_ratio = float(st.slider("close_trailing_grid_ratio", -1.0, 1.0, float(l.close_trailing_grid_ratio), 0.01, key="long_close_trailing_grid_ratio"))
            elif panel_long == "Close trailing":
                l.close_trailing_threshold_pct = float(st.slider("close_trailing_threshold_pct", 0.0, 1.0, float(l.close_trailing_threshold_pct), 0.001, key="long_close_trailing_threshold_pct"))
                l.close_trailing_retracement_pct = float(st.slider("close_trailing_retracement_pct", 0.0, 1.0, float(l.close_trailing_retracement_pct), 0.001, key="long_close_trailing_retracement_pct"))
                l.close_trailing_qty_pct = float(st.slider("close_trailing_qty_pct", 0.0, 1.0, float(l.close_trailing_qty_pct), 0.01, key="long_close_trailing_qty_pct"))
                l.close_trailing_grid_ratio = float(st.slider("close_trailing_grid_ratio", -1.0, 1.0, float(l.close_trailing_grid_ratio), 0.01, key="long_close_trailing_grid_ratio"))
            elif panel_long == "Filters/Unstuck":
                l.filter_volatility_ema_span = float(st.slider("filter_volatility_ema_span", 1.0, 4000.0, float(l.filter_volatility_ema_span), 1.0, key="long_filter_volatility_ema_span"))
                l.filter_volatility_drop_pct = float(st.slider("filter_volatility_drop_pct", 0.0, 1.0, float(l.filter_volatility_drop_pct), 0.001, key="long_filter_volatility_drop_pct"))
                l.filter_volume_ema_span = float(st.slider("filter_volume_ema_span", 1.0, 10000.0, float(l.filter_volume_ema_span), 1.0, key="long_filter_volume_ema_span"))
                l.filter_volume_drop_pct = float(st.slider("filter_volume_drop_pct", 0.0, 1.0, float(l.filter_volume_drop_pct), 0.001, key="long_filter_volume_drop_pct"))
                st.divider()
                l.unstuck_close_pct = float(st.slider("unstuck_close_pct", -0.1, 0.5, float(l.unstuck_close_pct), 0.001, key="long_unstuck_close_pct"))
                l.unstuck_ema_dist = float(st.slider("unstuck_ema_dist", -0.5, 0.5, float(l.unstuck_ema_dist), 0.001, key="long_unstuck_ema_dist"))
                l.unstuck_loss_allowance_pct = float(st.slider("unstuck_loss_allowance_pct", 0.0, 0.5, float(l.unstuck_loss_allowance_pct), 0.001, key="long_unstuck_loss_allowance_pct"))
                l.unstuck_threshold = float(st.slider("unstuck_threshold", 0.0, 1.0, float(l.unstuck_threshold), 0.01, key="long_unstuck_threshold"))
            else:
                l.total_wallet_exposure_limit = float(st.slider("total_wallet_exposure_limit", 0.0, 10.0, float(l.total_wallet_exposure_limit), 0.05, key="long_total_wallet_exposure_limit"))
                l.n_positions = float(st.slider("n_positions", 1.0, 50.0, float(l.n_positions), 1.0, key="long_n_positions"))
                l.risk_we_excess_allowance_pct = float(st.slider("risk_we_excess_allowance_pct", 0.0, 1.0, float(l.risk_we_excess_allowance_pct), 0.001, key="long_risk_we_excess_allowance_pct"))
                l.risk_wel_enforcer_threshold = float(st.slider("risk_wel_enforcer_threshold", 0.0, 10.0, float(l.risk_wel_enforcer_threshold), 0.01, key="long_risk_wel_enforcer_threshold"))
                l.risk_twel_enforcer_threshold = float(st.slider("risk_twel_enforcer_threshold", 0.0, 10.0, float(l.risk_twel_enforcer_threshold), 0.01, key="long_risk_twel_enforcer_threshold"))

    
    with col3:
        if data.isActive(Side.Short):
            st.subheader("SHORT")
            panel_short = st.segmented_control(
                "Segment",
                options=["Entry grid", "Entry trailing", "Close grid", "Close trailing", "Risk/State", "Filters/Unstuck"],
                default=str(st.session_state.get("gv_tuning_segment_short", "Entry grid")),
                key="gv_tuning_segment_short",
            )
            s = data.normal_bot_params_short
            if panel_short == "Entry grid":
                st.caption(f"Bot Limit: Wallet Exposure {s.total_wallet_exposure_limit:.2f}x")
                s.total_wallet_exposure_limit = float(st.slider("total_wallet_exposure_limit", 0.1, 50.0, float(s.total_wallet_exposure_limit), 0.1, key="short_twe_override_entry"))

                s.entry_initial_qty_pct = float(st.slider("entry_initial_qty_pct", 0.0, 1.0, float(s.entry_initial_qty_pct), 0.001, key="short_entry_initial_qty_pct"))
                s.entry_initial_ema_dist = float(st.slider("entry_initial_ema_dist", -1.0, 1.0, float(s.entry_initial_ema_dist), 0.001, key="short_entry_initial_ema_dist"))
                
                if s.entry_trailing_grid_ratio > 0 and s.entry_trailing_threshold_pct > 0 and s.entry_initial_ema_dist < s.entry_trailing_threshold_pct:
                    st.warning(
                        f"⚠️ **Logic Warning**: Your Grid starts at **{s.entry_initial_ema_dist:.1%}** distance, "
                        f"but Trailing only tries to activate at **{s.entry_trailing_threshold_pct:.1%}**.\n\n"
                        "Since **Trailing First** is active (ratio > 0), the bot may skip initial levels or wait until the threshold is met."
                    )

                s.entry_grid_spacing_pct = float(st.slider("entry_grid_spacing_pct", 0.0, 0.5, float(s.entry_grid_spacing_pct), 0.0005, key="short_entry_grid_spacing_pct", format="%.4f"))
                if s.entry_grid_spacing_pct > 0.05:
                    st.warning(f"Spacing {s.entry_grid_spacing_pct*100:.1f}% is very high! This will result in few grid levels.")

                s.entry_grid_spacing_we_weight = float(st.slider("entry_grid_spacing_we_weight", 0.0, 10.0, float(s.entry_grid_spacing_we_weight), 0.01, key="short_entry_grid_spacing_we_weight"))

                # --- CALC TEOR GRID COUNT ---
                try:
                    twe_budget = s.total_wallet_exposure_limit * data.state_params.balance
                    curr_cost = s.entry_initial_qty_pct * data.state_params.balance
                    
                    cost_limit = data.exchange_params.min_cost
                    
                    count = 1
                    ddf = s.entry_grid_double_down_factor
                    
                    break_reason = "TWE"
                    
                    total_spent = curr_cost
                    while total_spent < twe_budget:
                         next_order_cost = (s.entry_initial_qty_pct * data.state_params.balance) * (ddf ** count)
                         
                         if next_order_cost < cost_limit:
                             break_reason = f"Min Cost ({next_order_cost:.4f} < {cost_limit})"
                             break
                         
                         if total_spent + next_order_cost > twe_budget:
                             break_reason = "Wallet Limit"
                             break
                             
                         total_spent += next_order_cost
                         count += 1
                         if count > 100: 
                             break
                    
                    st.info(f"Theoretical Max Grid Orders: **{count}** (Reason: {break_reason}) | Cost: {total_spent:.2f} / {twe_budget:.2f}")
                except Exception as e:
                    pass
                # -----------------------------
                s.entry_grid_double_down_factor = float(st.slider("entry_grid_double_down_factor", 0.01, 10.0, float(s.entry_grid_double_down_factor), 0.01, key="short_entry_grid_double_down_factor"))
                s.entry_grid_spacing_volatility_weight = float(
                    st.slider(
                        "entry_grid_spacing_volatility_weight",
                        0.0,
                        400.0,
                        float(s.entry_grid_spacing_volatility_weight),
                        1.0,
                        key="short_entry_grid_spacing_volatility_weight"
                    )
                )
                s.entry_volatility_ema_span_hours = float(st.slider("entry_volatility_ema_span_hours", 1.0, 4000.0, float(s.entry_volatility_ema_span_hours), 1.0, key="short_entry_volatility_ema_span_hours"))
                s.ema_span_0 = float(st.slider("ema_span_0", 1.0, 10000.0, float(s.ema_span_0), 1.0, key="short_ema_span_0"))
                s.ema_span_1 = float(st.slider("ema_span_1", 1.0, 10000.0, float(s.ema_span_1), 1.0, key="short_ema_span_1"))
            elif panel_short == "Entry trailing":
                s.entry_trailing_threshold_pct = float(st.slider("entry_trailing_threshold_pct", 0.0, 1.0, float(s.entry_trailing_threshold_pct), 0.001, key="short_entry_trailing_threshold_pct"))
                s.entry_trailing_retracement_pct = float(st.slider("entry_trailing_retracement_pct", 0.0, 1.0, float(s.entry_trailing_retracement_pct), 0.001, key="short_entry_trailing_retracement_pct"))
                s.entry_trailing_grid_ratio = float(st.slider("entry_trailing_grid_ratio", -1.0, 1.0, float(s.entry_trailing_grid_ratio), 0.01, key="short_entry_trailing_grid_ratio"))
                s.entry_trailing_double_down_factor = float(
                    st.slider(
                        "entry_trailing_double_down_factor",
                        0.0,
                        10.0,
                        float(s.entry_trailing_double_down_factor or s.entry_grid_double_down_factor),
                        0.01,
                        key="short_entry_trailing_double_down_factor"
                    )
                )
                s.entry_trailing_threshold_we_weight = float(st.slider("entry_trailing_threshold_we_weight", 0.0, 20.0, float(s.entry_trailing_threshold_we_weight), 0.1, key="short_entry_trailing_threshold_we_weight"))
                s.entry_trailing_threshold_volatility_weight = float(
                    st.slider(
                        "entry_trailing_threshold_volatility_weight",
                        0.0,
                        400.0,
                        float(s.entry_trailing_threshold_volatility_weight),
                        1.0,
                        key="short_entry_trailing_threshold_volatility_weight"
                    )
                )
                s.entry_trailing_retracement_we_weight = float(st.slider("entry_trailing_retracement_we_weight", 0.0, 20.0, float(s.entry_trailing_retracement_we_weight), 0.1, key="short_entry_trailing_retracement_we_weight"))
                s.entry_trailing_retracement_volatility_weight = float(
                    st.slider(
                        "entry_trailing_retracement_volatility_weight",
                        0.0,
                        400.0,
                        float(s.entry_trailing_retracement_volatility_weight),
                        1.0,
                        key="short_entry_trailing_retracement_volatility_weight"
                    )
                )
            elif panel_short == "Close grid":
                s.close_grid_markup_end = float(st.slider("close_grid_markup_end", 0.0, 1.0, float(s.close_grid_markup_end), 0.001, key="short_close_grid_markup_end"))
                s.close_grid_markup_start = float(st.slider("close_grid_markup_start", 0.0, 1.0, float(s.close_grid_markup_start), 0.001, key="short_close_grid_markup_start"))
                s.close_grid_qty_pct = float(st.slider("close_grid_qty_pct", 0.0, 1.0, float(s.close_grid_qty_pct), 0.01, key="short_close_grid_qty_pct"))
                s.close_trailing_grid_ratio = float(st.slider("close_trailing_grid_ratio", -1.0, 1.0, float(s.close_trailing_grid_ratio), 0.01, key="short_close_trailing_grid_ratio"))
            elif panel_short == "Close trailing":
                s.close_trailing_threshold_pct = float(st.slider("close_trailing_threshold_pct", 0.0, 1.0, float(s.close_trailing_threshold_pct), 0.001, key="short_close_trailing_threshold_pct"))
                s.close_trailing_retracement_pct = float(st.slider("close_trailing_retracement_pct", 0.0, 1.0, float(s.close_trailing_retracement_pct), 0.001, key="short_close_trailing_retracement_pct"))
                s.close_trailing_qty_pct = float(st.slider("close_trailing_qty_pct", 0.0, 1.0, float(s.close_trailing_qty_pct), 0.01, key="short_close_trailing_qty_pct"))
                s.close_trailing_grid_ratio = float(st.slider("close_trailing_grid_ratio", -1.0, 1.0, float(s.close_trailing_grid_ratio), 0.01, key="short_close_trailing_grid_ratio"))
            elif panel_short == "Filters/Unstuck":
                s.filter_volatility_ema_span = float(st.slider("filter_volatility_ema_span", 1.0, 4000.0, float(s.filter_volatility_ema_span), 1.0, key="short_filter_volatility_ema_span"))
                s.filter_volatility_drop_pct = float(st.slider("filter_volatility_drop_pct", 0.0, 1.0, float(s.filter_volatility_drop_pct), 0.001, key="short_filter_volatility_drop_pct"))
                s.filter_volume_ema_span = float(st.slider("filter_volume_ema_span", 1.0, 10000.0, float(s.filter_volume_ema_span), 1.0, key="short_filter_volume_ema_span"))
                s.filter_volume_drop_pct = float(st.slider("filter_volume_drop_pct", 0.0, 1.0, float(s.filter_volume_drop_pct), 0.001, key="short_filter_volume_drop_pct"))
                st.divider()
                s.unstuck_close_pct = float(st.slider("unstuck_close_pct", -0.1, 0.5, float(s.unstuck_close_pct), 0.001, key="short_unstuck_close_pct"))
                s.unstuck_ema_dist = float(st.slider("unstuck_ema_dist", -0.5, 0.5, float(s.unstuck_ema_dist), 0.001, key="short_unstuck_ema_dist"))
                s.unstuck_loss_allowance_pct = float(st.slider("unstuck_loss_allowance_pct", 0.0, 0.5, float(s.unstuck_loss_allowance_pct), 0.001, key="short_unstuck_loss_allowance_pct"))
                s.unstuck_threshold = float(st.slider("unstuck_threshold", 0.0, 1.0, float(s.unstuck_threshold), 0.01, key="short_unstuck_threshold"))
            else:
                s.total_wallet_exposure_limit = float(st.slider("total_wallet_exposure_limit", 0.0, 10.0, float(s.total_wallet_exposure_limit), 0.05, key="short_total_wallet_exposure_limit"))
                s.n_positions = float(st.slider("n_positions", 1.0, 50.0, float(s.n_positions), 1.0, key="short_n_positions"))
                s.risk_we_excess_allowance_pct = float(st.slider("risk_we_excess_allowance_pct", 0.0, 1.0, float(s.risk_we_excess_allowance_pct), 0.001, key="short_risk_we_excess_allowance_pct"))
                s.risk_wel_enforcer_threshold = float(st.slider("risk_wel_enforcer_threshold", 0.0, 10.0, float(s.risk_wel_enforcer_threshold), 0.01, key="short_risk_wel_enforcer_threshold"))
                s.risk_twel_enforcer_threshold = float(st.slider("risk_twel_enforcer_threshold", 0.0, 10.0, float(s.risk_twel_enforcer_threshold), 0.01, key="short_risk_twel_enforcer_threshold"))

    # Sync derived params (gridonly clones, modes, and positions that depend on TWE)
    # NOTE: This must run after all Streamlit sliders have been applied.
    data.prepare_data()

    pbr = _try_get_pbr()
    if pbr is None:
        st.error("Passivbot Rust bindings not found! Visualization requires PB7 Rust backend (available in `pb7/passivbot-rust`).")
        return

    pb7_src = _pb7_src_dir()
    ep_json = json.dumps(asdict(data.exchange_params), sort_keys=True)
    tb_json = json.dumps(asdict(data.trailing_price_bundle), sort_keys=True)

    long_active = data.isActive(Side.Long)
    short_active = data.isActive(Side.Short)

    sp_json_long = json.dumps(asdict(data.state_params_long or data.state_params), sort_keys=True)
    sp_json_short = json.dumps(asdict(data.state_params_short or data.state_params), sort_keys=True)

    # Optional: historical candle-walk simulation of entry fills.
    # Uses `data.historical_candles` (slice from selected time) and Rust next-entry per candle.
    sim_enabled = bool(st.session_state.get("gv_hist_sim_enabled", False))
    sim_mode = str(st.session_state.get("gv_hist_sim_mode", "Local (B)") or "Local (B)")
    if sim_enabled:
        sim_max_candles = int(st.session_state.get("gv_hist_sim_max_candles", 2000) or 2000)
        sim_max_orders = int(st.session_state.get("gv_hist_sim_max_orders", 200) or 200)

        if sim_mode == "PB7 backtest engine (C)":
            sel_exc = str(st.session_state.get("gv_hist_exchange", "") or "")
            sel_coin = str(st.session_state.get("gv_hist_coin", "") or "")
            if sel_exc and sel_coin and data.analysis_time is not None:
                hist_df_full = load_historical_ohlcv_v7(sel_exc, sel_coin)
                if not hist_df_full.empty:
                    events_long, events_short = _run_pb7_engine_backtest_for_visualizer(
                        pbr=pbr,
                        exchange=sel_exc,
                        coin=sel_coin,
                        analysis_time=pd.to_datetime(data.analysis_time).to_pydatetime(),
                        hist_df=hist_df_full,
                        exchange_params=data.exchange_params,
                        bot_params_long=data.normal_bot_params_long,
                        bot_params_short=data.normal_bot_params_short,
                        starting_balance=float(data.state_params.balance),
                        max_candles_forward=sim_max_candles,
                    )
                else:
                    events_long, events_short = [], []
            else:
                events_long, events_short = [], []

            data.historical_sim_entries_long = []
            data.historical_sim_closes_long = []
            data.historical_sim_entries_short = []
            data.historical_sim_closes_short = []
            data.historical_sim_fills_long = events_long if long_active else []
            data.historical_sim_fills_short = events_short if short_active else []
        else:
            # Local (B): visualizer candle-walk simulation.
            sel_exc = str(st.session_state.get("gv_hist_exchange", "") or "")
            sel_coin = str(st.session_state.get("gv_hist_coin", "") or "")
            trade_start_time = pd.to_datetime(data.analysis_time) if data.analysis_time is not None else None

            sim_df = pd.DataFrame()
            if sel_exc and sel_coin and trade_start_time is not None:
                hist_df_full = load_historical_ohlcv_v7(sel_exc, sel_coin)
                if hist_df_full is not None and not hist_df_full.empty:
                    base_tf_mins = _infer_hist_base_tf_minutes(hist_df_full)
                    sim_end = trade_start_time + pd.Timedelta(minutes=int(base_tf_mins) * max(0, int(sim_max_candles) - 1))
                    sim_df = _slice_hist_df_for_modeb(
                        hist_df_full,
                        trade_start_time=trade_start_time,
                        end_time=sim_end,
                        include_prev_candle=True,
                    )
            elif data.historical_candles is not None:
                # Fallback: use the precomputed slice only (no warmup)
                sim_df = _historical_dict_to_df(data.historical_candles)

            if not sim_df.empty:
                if long_active:
                    sp0, bal0 = _get_modeb_starting_state(data, Side.Long)
                    events, _frames_ignored = _simulate_backtest_over_historical_candles_replay(
                        pbr=pbr,
                        pb7_src=pb7_src,
                        side=Side.Long,
                        candles=sim_df,
                        exchange_params=data.exchange_params,
                        bot_params=data.normal_bot_params_long,
                        starting_position=sp0,
                        balance=float(bal0),
                        maker_fee=float(_derive_exchange_fees_from_market(sel_exc, sel_coin).get("maker_fee", 0.0) or 0.0),
                        trade_start_time=trade_start_time,
                        max_orders=sim_max_orders,
                        max_candles=int(len(sim_df)),
                        frame_every_n_candles=max(1, int(len(sim_df)) + 1),
                    )
                    data.historical_sim_fills_long = events
                    data.historical_sim_entries_long = []
                    data.historical_sim_closes_long = []
                else:
                    data.historical_sim_entries_long = []
                    data.historical_sim_closes_long = []
                    data.historical_sim_fills_long = []

                if short_active:
                    sp0, bal0 = _get_modeb_starting_state(data, Side.Short)
                    events, _frames_ignored = _simulate_backtest_over_historical_candles_replay(
                        pbr=pbr,
                        pb7_src=pb7_src,
                        side=Side.Short,
                        candles=sim_df,
                        exchange_params=data.exchange_params,
                        bot_params=data.normal_bot_params_short,
                        starting_position=sp0,
                        balance=float(bal0),
                        maker_fee=float(_derive_exchange_fees_from_market(sel_exc, sel_coin).get("maker_fee", 0.0) or 0.0),
                        trade_start_time=trade_start_time,
                        max_orders=sim_max_orders,
                        max_candles=int(len(sim_df)),
                        frame_every_n_candles=max(1, int(len(sim_df)) + 1),
                    )
                    data.historical_sim_fills_short = events
                    data.historical_sim_entries_short = []
                    data.historical_sim_closes_short = []
                else:
                    data.historical_sim_entries_short = []
                    data.historical_sim_closes_short = []
                    data.historical_sim_fills_short = []
            else:
                data.historical_sim_entries_long = []
                data.historical_sim_entries_short = []
                data.historical_sim_closes_long = []
                data.historical_sim_closes_short = []
                data.historical_sim_fills_long = []
                data.historical_sim_fills_short = []

    else:
        data.historical_sim_entries_long = []
        data.historical_sim_entries_short = []
        data.historical_sim_closes_long = []
        data.historical_sim_closes_short = []
        data.historical_sim_fills_long = []
        data.historical_sim_fills_short = []

    # PB7 vs B vs C compare: independent of simulation/movie; runs when checkbox is enabled.
    if bool(st.session_state.get("gv_hist_compare_enabled", False)):
        sel_exc = str(st.session_state.get("gv_hist_exchange", "") or "")
        sel_coin = str(st.session_state.get("gv_hist_coin", "") or "")
        trade_start_time = pd.to_datetime(data.analysis_time) if data.analysis_time is not None else None
        # Parity: Mode C floors analysis_time to minute internally; ensure Mode B compare uses the same.
        if trade_start_time is not None:
            try:
                trade_start_time = pd.Timestamp(trade_start_time).floor("min")
            except Exception:
                pass

        compare_mode = str(st.session_state.get("gv_hist_compare_mode", "PB7 vs B vs C") or "PB7 vs B vs C")
        pb7_dir = str(st.session_state.get("gv_hist_compare_pb7_dir", "") or "")
        use_pb7_range = bool(st.session_state.get("gv_hist_compare_use_pb7_range", True))

        compare_max_candles = int(st.session_state.get("gv_hist_compare_max_candles", 2000) or 2000)
        compare_max_orders = 20000

        pb7_long: list[dict] = []
        pb7_short: list[dict] = []
        b_long: list[dict] = []
        b_short: list[dict] = []
        c_long: list[dict] = []
        c_short: list[dict] = []

        if compare_mode == "PB7 vs B vs C" and pb7_dir and use_pb7_range:
            try:
                (pb7_long, pb7_short), (b_long, b_short), (c_long, c_short), meta = _run_compare_from_pb7_backtest_dir(
                    pbr=pbr,
                    pb7_src=pb7_src,
                    backtest_dir=pb7_dir,
                    max_orders=int(compare_max_orders),
                )
                st.session_state["gv_compare_meta"] = meta
            except Exception as e:
                st.session_state["gv_compare_meta"] = {"error": str(e)}
        else:
            # In B vs C mode, PB7 is intentionally ignored.
            if compare_mode == "PB7 vs B vs C":
                try:
                    pb7_long, pb7_short = _load_pb7_fills_csv_to_events(pb7_dir) if pb7_dir else ([], [])
                except Exception:
                    pb7_long, pb7_short = [], []
            else:
                pb7_long, pb7_short = [], []

            if sel_exc and sel_coin and trade_start_time is not None:
                hist_df_full = load_historical_ohlcv_v7(sel_exc, sel_coin)
                if hist_df_full is not None and not hist_df_full.empty:
                    # Root-cause: Mode B and Mode C must use the same warmup length and candle stream.
                    # If warmup differs, EMA/trailing state can diverge and shift fills by 1 candle.
                    warmup_minutes = int(
                        _compute_warmup_minutes_for_mode_c(
                            data.normal_bot_params_long,
                            data.normal_bot_params_short,
                        )
                        or 0
                    )
                    warm_start = trade_start_time - pd.Timedelta(minutes=max(0, int(warmup_minutes)))
                    # Parity: both Mode B and Mode C step fills using the *next* candle range.
                    # To avoid a missing final fill at the window end, simulate with a small
                    # forward buffer, then filter events back to the intended end.
                    sim_end_target = trade_start_time + pd.Timedelta(minutes=max(0, int(compare_max_candles) - 1))
                    sim_end_run = sim_end_target + pd.Timedelta(minutes=5)

                    # Strict requirement: if there are 1m gaps, compare is not meaningful.
                    # Do NOT auto-fill here; instead abort and ask the user to fix data.
                    try:
                        raw_slice = hist_df_full.loc[pd.Timestamp(warm_start).floor("min") : pd.Timestamp(sim_end_target).floor("min")]
                    except Exception:
                        raw_slice = hist_df_full
                    gaps = _find_1m_gaps(raw_slice, start_ts=pd.Timestamp(warm_start), end_ts=pd.Timestamp(sim_end_target))
                    if bool(gaps.get("has_gaps")):
                        st.session_state["gv_compare_meta"] = {
                            "exchange": sel_exc,
                            "coin": sel_coin,
                            "trade_start_ts": trade_start_time,
                            "start_ts": trade_start_time,
                            "end_ts": sim_end_target,
                            "warmup_minutes": int(warmup_minutes),
                            "compare_mode": compare_mode,
                            "error": "Compare not possible: 1m gaps detected in historical data.",
                            "gaps": {
                                "missing_count": gaps.get("missing_count"),
                                "first_missing": gaps.get("first_missing"),
                                "last_missing": gaps.get("last_missing"),
                                "sample_missing": gaps.get("sample_missing"),
                            },
                        }
                        # Skip running simulations; renderer will show meta + error.
                        pb7_long, pb7_short, b_long, b_short, c_long, c_short = [], [], [], [], [], []
                        # Prevent further work in this branch.
                        sim_df = pd.DataFrame()
                        sim_max_candles_modeb = 0
                    
                    try:
                        sim_df = hist_df_full.loc[warm_start:sim_end_run].copy()
                    except Exception:
                        sim_df = hist_df_full.copy()

                    # If gaps were detected above, sim_df was cleared; otherwise proceed as-is.
                    if sim_df is None:
                        sim_df = pd.DataFrame()

                    # Important: `compare_max_candles` refers to the forward window from `trade_start_time`.
                    # `sim_df` includes warmup candles before `trade_start_time`, so Mode B must NOT truncate
                    # to `compare_max_candles` total candles, otherwise it can cut off the forward range and
                    # produce many `c_only` rows.
                    sim_max_candles_modeb = int(len(sim_df))

                    if not (isinstance(st.session_state.get("gv_compare_meta"), dict) and st.session_state.get("gv_compare_meta", {}).get("error")):
                        st.session_state["gv_compare_meta"] = {
                            "exchange": sel_exc,
                            "coin": sel_coin,
                            "trade_start_ts": trade_start_time,
                            "start_ts": trade_start_time,
                            "end_ts": sim_end_target,
                            "warmup_minutes": int(warmup_minutes),
                            "price_step": float(getattr(data.exchange_params, "price_step", 0.0) or 0.0),
                            "qty_step": float(getattr(data.exchange_params, "qty_step", 0.0) or 0.0),
                            "compare_mode": compare_mode,
                        }

                    # Mode B (local)
                    if not sim_df.empty:
                        # Parity: use the same fee source as Mode C (PB7 engine wrapper).
                        try:
                            maker_fee, _taker_fee = _infer_maker_taker_fees(sel_exc, sel_coin)
                        except Exception:
                            fees = _derive_exchange_fees_from_market(sel_exc, sel_coin)
                            maker_fee = float(fees.get("maker_fee", 0.0) or 0.0)

                        # Parity: Mode C always starts from an empty position.
                        # Keep Mode B compare consistent by starting from size=0, price=0.
                        start_pos_long = Position(size=0.0, price=0.0)
                        start_pos_short = Position(size=0.0, price=0.0)

                        if bool(long_active) and bool(short_active):
                            try:
                                b_long, b_short = _simulate_backtest_over_historical_candles_pair(
                                    pbr=pbr,
                                    pb7_src=pb7_src,
                                    candles=sim_df,
                                    exchange_params=data.exchange_params,
                                    bot_params_long=data.normal_bot_params_long,
                                    bot_params_short=data.normal_bot_params_short,
                                    starting_position_long=start_pos_long,
                                    starting_position_short=start_pos_short,
                                    balance=float(getattr(data.state_params, "balance", 0.0) or 0.0),
                                    maker_fee=maker_fee,
                                    trade_start_time=trade_start_time,
                                    max_orders=int(compare_max_orders),
                                    max_candles=int(sim_max_candles_modeb),
                                )
                            except Exception:
                                b_long, b_short = [], []
                        else:
                            try:
                                if long_active:
                                    b_long = _simulate_backtest_over_historical_candles(
                                        pbr=pbr,
                                        pb7_src=pb7_src,
                                        side=Side.Long,
                                        candles=sim_df,
                                        exchange_params=data.exchange_params,
                                        bot_params_long=data.normal_bot_params_long,
                                        bot_params_short=data.normal_bot_params_short,
                                        starting_position=start_pos_long,
                                        balance=float(getattr(data.state_params, "balance", 0.0) or 0.0),
                                        maker_fee=maker_fee,
                                        trade_start_time=trade_start_time,
                                        max_orders=int(compare_max_orders),
                                        max_candles=int(sim_max_candles_modeb),
                                    )
                            except Exception:
                                b_long = []
                            try:
                                if short_active:
                                    b_short = _simulate_backtest_over_historical_candles(
                                        pbr=pbr,
                                        pb7_src=pb7_src,
                                        side=Side.Short,
                                        candles=sim_df,
                                        exchange_params=data.exchange_params,
                                        bot_params_long=data.normal_bot_params_long,
                                        bot_params_short=data.normal_bot_params_short,
                                        starting_position=start_pos_short,
                                        balance=float(getattr(data.state_params, "balance", 0.0) or 0.0),
                                        maker_fee=maker_fee,
                                        trade_start_time=trade_start_time,
                                        max_orders=int(compare_max_orders),
                                        max_candles=int(sim_max_candles_modeb),
                                    )
                            except Exception:
                                b_short = []

                        # Filter Mode B events back to the intended compare window.
                        def _filter_window(events: list[dict]) -> list[dict]:
                            out: list[dict] = []
                            for e in events or []:
                                try:
                                    t = pd.to_datetime(e.get("timestamp"))
                                except Exception:
                                    continue
                                if t < trade_start_time or t > sim_end_target:
                                    continue
                                out.append(e)
                            return out

                        b_long = _filter_window(b_long)
                        b_short = _filter_window(b_short)

                    # Mode C (PB7 engine)
                    # Root-cause fix: Mode C is sensitive to warmup/state initialization.
                    # In PB7-backed compare we already increase warmup if needed; do the same here
                    # by selecting the warmup which minimizes strict mismatches vs Mode B.
                    c_long, c_short = [], []
                    if not sim_df.empty:
                        try:
                            price_step_for_cmp = float(getattr(data.exchange_params, "price_step", 0.0) or 0.0)
                            qty_step_for_cmp = float(getattr(data.exchange_params, "qty_step", 0.0) or 0.0)
                        except Exception:
                            price_step_for_cmp = 0.0
                            qty_step_for_cmp = 0.0

                        best = {
                            "mismatch_count": None,
                            "warmup_used": None,
                            "c_long": [],
                            "c_short": [],
                            "per_attempt": [],
                        }

                        warmup_base = int(warmup_minutes)
                        for extra in (0, 1000, 2000, 4000, 8000, 12000, 16000):
                            try:
                                warmup_try = int(max(0, warmup_base + int(extra)))
                            except Exception:
                                warmup_try = int(max(0, warmup_base))

                            try:
                                c_l_try, c_s_try = _run_pb7_engine_backtest_for_visualizer(
                                    pbr=pbr,
                                    exchange=sel_exc,
                                    coin=sel_coin,
                                    analysis_time=pd.to_datetime(data.analysis_time).to_pydatetime(),
                                    hist_df=hist_df_full,
                                    exchange_params=data.exchange_params,
                                    bot_params_long=data.normal_bot_params_long,
                                    bot_params_short=data.normal_bot_params_short,
                                    starting_balance=float(data.state_params.balance),
                                    max_candles_forward=int(compare_max_candles) + 5,
                                    warmup_minutes_override=int(warmup_try),
                                )
                            except Exception:
                                c_l_try, c_s_try = [], []

                            c_l_try = _filter_window(c_l_try)
                            c_s_try = _filter_window(c_s_try)

                            # Score vs Mode B (strict compare).
                            mismatch_count = 0
                            try:
                                if b_long or c_l_try:
                                    df_l = _compare_fills_b_c(
                                        b_events=b_long,
                                        c_events=c_l_try,
                                        price_step=price_step_for_cmp,
                                        qty_step=qty_step_for_cmp,
                                    )
                                    mismatch_count += int((df_l["status"] != "match").sum()) if not df_l.empty else 0
                            except Exception:
                                pass
                            try:
                                if b_short or c_s_try:
                                    df_s = _compare_fills_b_c(
                                        b_events=b_short,
                                        c_events=c_s_try,
                                        price_step=price_step_for_cmp,
                                        qty_step=qty_step_for_cmp,
                                    )
                                    mismatch_count += int((df_s["status"] != "match").sum()) if not df_s.empty else 0
                            except Exception:
                                pass

                            best["per_attempt"].append({"warmup": int(warmup_try), "mismatches": int(mismatch_count)})

                            if best["mismatch_count"] is None or int(mismatch_count) < int(best["mismatch_count"]):
                                best["mismatch_count"] = int(mismatch_count)
                                best["warmup_used"] = int(warmup_try)
                                best["c_long"] = c_l_try
                                best["c_short"] = c_s_try

                            # Perfect match; stop early.
                            if int(mismatch_count) == 0:
                                break

                        c_long = list(best.get("c_long") or [])
                        c_short = list(best.get("c_short") or [])

                        try:
                            meta0 = st.session_state.get("gv_compare_meta")
                            if isinstance(meta0, dict) and not meta0.get("error"):
                                meta0["mode_c_warmup_used"] = best.get("warmup_used")
                                meta0["mode_c_mismatches_vs_b"] = best.get("mismatch_count")
                                meta0["mode_c_attempts"] = best.get("per_attempt")
                                st.session_state["gv_compare_meta"] = meta0
                        except Exception:
                            pass

        st.session_state["gv_compare_pb7_long"] = pb7_long
        st.session_state["gv_compare_pb7_short"] = pb7_short
        st.session_state["gv_compare_b_long"] = b_long
        st.session_state["gv_compare_b_short"] = b_short
        st.session_state["gv_compare_c_long"] = c_long
        st.session_state["gv_compare_c_short"] = c_short

        # Render inside the Compare expander
        try:
            if compare_out is not None:
                with compare_out:
                    meta = st.session_state.get("gv_compare_meta")
                    if meta:
                        st.caption(f"Compare meta: {meta}")

                    mismatches_only = bool(st.session_state.get("gv_hist_compare_mismatches_only", True))
                    # Important: use the same tick sizes used to produce B/C events.
                    # Using the current visualizer config's exchange_params can make everything look `pb7_only`.
                    price_step = 0.0
                    qty_step = 0.0
                    try:
                        if isinstance(meta, dict):
                            price_step = float(meta.get("price_step") or 0.0)
                            qty_step = float(meta.get("qty_step") or 0.0)
                    except Exception:
                        price_step = 0.0
                        qty_step = 0.0
                    if not price_step or not qty_step:
                        try:
                            sel_exc = str((meta or {}).get("exchange") or st.session_state.get("gv_hist_exchange") or "")
                            sel_coin = str((meta or {}).get("coin") or st.session_state.get("gv_hist_coin") or "")
                            if sel_exc and sel_coin:
                                market_ep = _derive_exchange_params_from_market(sel_exc, sel_coin)
                                price_step = float(market_ep.get("price_step") or price_step or 0.0)
                                qty_step = float(market_ep.get("qty_step") or qty_step or 0.0)
                        except Exception:
                            pass
                    if not price_step:
                        price_step = float(getattr(data.exchange_params, "price_step", 0.0) or 0.0)
                    if not qty_step:
                        qty_step = float(getattr(data.exchange_params, "qty_step", 0.0) or 0.0)

                    def _render_one(side_key: str, title: str):
                        pb7_events = list(st.session_state.get(f"gv_compare_pb7_{side_key}", []) or [])
                        b_events = list(st.session_state.get(f"gv_compare_b_{side_key}", []) or [])
                        c_events = list(st.session_state.get(f"gv_compare_c_{side_key}", []) or [])
                        if not (pb7_events or b_events or c_events):
                            st.write(f"**{title}:** No events")
                            return
                        if str(st.session_state.get("gv_hist_compare_mode", "PB7 vs B vs C") or "PB7 vs B vs C") == "B vs C only (no PB7)":
                            cmp_df = _compare_fills_b_c(
                                b_events=b_events,
                                c_events=c_events,
                                price_step=price_step,
                                qty_step=qty_step,
                            )
                        else:
                            cmp_df = _compare_fills_pb7_b_c(
                                pb7_events=pb7_events,
                                b_events=b_events,
                                c_events=c_events,
                                price_step=price_step,
                                qty_step=qty_step,
                            )
                        if mismatches_only and not cmp_df.empty:
                            cmp_df = cmp_df[cmp_df["status"] != "match"].copy()
                        st.write(f"**{title}**")
                        st.dataframe(cmp_df, use_container_width=True)

                    _render_one("long", "LONG")
                    _render_one("short", "SHORT")
        except Exception:
            pass

    # --- DEBUG CAPTURE ---
    debug_rust_data = {}

    if long_active:
        # NORMAL LONG ENTRIES
        bp_json = json.dumps(asdict(data.normal_bot_params_long), sort_keys=True)
        raw = _calc_entries_rust_cached(pb7_src, Side.Long.value, ep_json, sp_json_long, tb_json, bp_json, data.position_long_enty.size, data.position_long_enty.price)

        debug_rust_data["long_entry_input"] = {
            "bp": json.loads(bp_json),
            "sp": json.loads(sp_json_long),
            "tb": json.loads(tb_json),
            "ep": json.loads(ep_json),
            "pos": {"size": data.position_long_enty.size, "price": data.position_long_enty.price}
        }
        debug_rust_data["long_entry_output_raw"] = raw

        normal_entries_long = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw]
        data.normal_entries_long = adjust_order_quantities(normal_entries_long)

        # GRIDONLY LONG ENTRIES
        bp_json = json.dumps(asdict(data.gridonly_bot_params_long), sort_keys=True)
        raw_gridonly = _calc_entries_rust_cached(pb7_src, Side.Long.value, ep_json, sp_json_long, tb_json, bp_json, data.position_long_enty.size, data.position_long_enty.price)
        gridonly_entries_long = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw_gridonly]
        data.gridonly_entries_long = adjust_order_quantities(gridonly_entries_long)
        debug_rust_data["long_entry_gridonly_output_raw"] = raw_gridonly

        # Potential trailing entries (GridFirst ratio<0): derived from fullgrid price range
        pot_prices, pot_dbg = _calc_potential_trailing_entry_prices_from_fullgrid(
            side=Side.Long,
            bot_params=data.normal_bot_params_long,
            exchange_params=data.exchange_params,
            balance=float((data.state_params_long or data.state_params).balance),
            fullgrid_orders=data.gridonly_entries_long,
        )
        data.potential_entry_trailing_prices_long = pot_prices
        debug_rust_data["long_entry_trailing_potential"] = pot_dbg

        # Simulated trailing steps: uses Rust next-entry + forced trigger and re-applies fills.
        # This reacts to entry_trailing_double_down_factor via qty -> wallet exposure -> eventual stop.
        data.simulated_entry_trailing_prices_long = []
        data.simulated_entry_trailing_orders_long = []
        data.simulated_entry_trailing_ref_price_long = 0.0
        try:
            bp = data.normal_bot_params_long
            sp = (data.state_params_long or data.state_params)
            ep = data.exchange_params
            mode = get_GridTrailing_mode(float(bp.entry_trailing_grid_ratio))

            start_pos: Position | None = None
            start_dbg: dict = {"mode": str(mode)}

            if mode == GridTrailingMode.GridFirst:
                # derive a position at the point GridFirst would hand off to trailing
                sim_pos, sim_dbg = _simulate_gridfilled_position_for_trailing(
                    side=Side.Long,
                    raw_gridonly=debug_rust_data.get("long_entry_gridonly_output_raw") or [],
                    exchange_params=ep,
                    state_params=sp,
                    bot_params=bp,
                )
                start_pos = sim_pos
                start_dbg.update({"start": "gridfilled", "sim": sim_dbg})
            elif mode in (GridTrailingMode.TrailingOnly, GridTrailingMode.TrailingFirst):
                # start from current position if given, else create initial fill
                start_pos = data.position_long_enty
                start_dbg.update({"start": "current_pos"})

            if start_pos is not None and float(getattr(start_pos, "size", 0.0) or 0.0) == 0.0:
                init_tb = TrailingPriceBundle(min_since_open=0.0, max_since_min=0.0, max_since_open=0.0, min_since_max=0.0)
                init = _calc_next_entry_rust(
                    pbr=pbr,
                    side=Side.Long,
                    exchange_params=ep,
                    state_params=sp,
                    trailing_bundle=init_tb,
                    bot_params=bp,
                    position=Position(size=0.0, price=0.0),
                )
                if float(init.get("qty", 0.0) or 0.0) != 0.0 and float(init.get("price", 0.0) or 0.0) > 0.0:
                    start_pos = _apply_fill_to_position(
                        position=Position(size=0.0, price=0.0),
                        fill_qty=float(init["qty"]),
                        fill_price=float(init["price"]),
                    )
                    start_dbg.update({"start": "initial_fill", "initial_fill": init})

            if start_pos is not None and float(getattr(start_pos, "size", 0.0) or 0.0) != 0.0 and mode != GridTrailingMode.GridOnly:
                data.simulated_entry_trailing_ref_price_long = float(getattr(start_pos, "price", 0.0) or 0.0)
                chain = _simulate_trailing_sequence_forced(
                    pbr=pbr,
                    side=Side.Long,
                    exchange_params=ep,
                    state_params=sp,
                    bot_params=bp,
                    start_position=start_pos,
                    n_steps=25,
                )
                prices: list[float] = []
                orders: list[Order] = []
                for step in chain.get("steps", []) or []:
                    nxt = step.get("next") or {}
                    typ = str(nxt.get("type", ""))
                    if float(nxt.get("qty", 0.0) or 0.0) == 0.0:
                        break
                    # In TrailingFirst mode, stop when Rust switches back to grid entries.
                    if mode == GridTrailingMode.TrailingFirst and ("trailing" not in typ):
                        break
                    p = float(nxt.get("price", 0.0) or 0.0)
                    q = float(nxt.get("qty", 0.0) or 0.0)
                    if p > 0.0:
                        prices.append(p)
                        orders.append(Order(qty=float(q), price=float(p), order_type_str=typ))
                data.simulated_entry_trailing_prices_long = prices
                data.simulated_entry_trailing_orders_long = adjust_order_quantities(orders)
                debug_rust_data["long_entry_trailing_simulated"] = {"start": start_dbg, "count": len(prices), "prices": prices[:50]}
        except Exception as e:
            debug_rust_data["long_entry_trailing_simulated_error"] = str(e)

        # Next-entry (Rust) - show the actual next order type (grid vs trailing)
        try:
            debug_rust_data["long_next_entry_current"] = _calc_next_entry_rust(
                pbr=pbr,
                side=Side.Long,
                exchange_params=data.exchange_params,
                state_params=(data.state_params_long or data.state_params),
                trailing_bundle=data.trailing_price_bundle,
                bot_params=data.normal_bot_params_long,
                position=data.position_long_enty,
            )

            # Simulate: fill grid until GridFirst would switch to trailing, then force trigger.
            bp = data.normal_bot_params_long
            sp = (data.state_params_long or data.state_params)
            ep = data.exchange_params
            sim_pos, sim_dbg = _simulate_gridfilled_position_for_trailing(
                side=Side.Long,
                raw_gridonly=debug_rust_data.get("long_entry_gridonly_output_raw") or [],
                exchange_params=ep,
                state_params=sp,
                bot_params=bp,
            )
            if sim_pos is not None:
                per_pos_wel = float(bp.total_wallet_exposure_limit) / float(bp.n_positions) if bp.n_positions else float(bp.total_wallet_exposure_limit)
                allowed_wel = per_pos_wel * (1.0 + max(0.0, float(getattr(bp, "risk_we_excess_allowance_pct", 0.0) or 0.0))) if per_pos_wel > 0.0 else per_pos_wel
                c_mult = float(ep.c_mult or 1.0) or 1.0
                wallet_exposure = (abs(sim_pos.size) * sim_pos.price * c_mult) / float(sp.balance)
                # Match Rust: effective_wallet_exposure_limit = min(cap, wallet_exposure_limit_with_allowance) where cap=allowed => effective=allowed
                effective_wel = allowed_wel
                we_over = (wallet_exposure / effective_wel) if effective_wel > 0.0 else 0.0
                th_mult = we_over * float(bp.entry_trailing_threshold_we_weight)
                th_log_mult = float(sp.entry_volatility_logrange_ema_1h) * float(bp.entry_trailing_threshold_volatility_weight)
                threshold_pct = float(bp.entry_trailing_threshold_pct) * max(0.0, 1.0 + th_mult + th_log_mult)
                re_mult = we_over * float(bp.entry_trailing_retracement_we_weight)
                re_log_mult = float(sp.entry_volatility_logrange_ema_1h) * float(bp.entry_trailing_retracement_volatility_weight)
                retracement_pct = float(bp.entry_trailing_retracement_pct) * max(0.0, 1.0 + re_mult + re_log_mult)

                if retracement_pct > 0.0:
                    # LONG trigger condition in Rust:
                    # threshold>0: min_since_open < pos.price*(1-threshold) AND max_since_min > min_since_open*(1+retracement)
                    # threshold<=0 (immediate trailing): max_since_min > min_since_open*(1+retracement)
                    if threshold_pct > 0.0:
                        min_since_open = sim_pos.price * (1.0 - threshold_pct) * 0.99
                    else:
                        min_since_open = sim_pos.price * 0.99
                    max_since_min = min_since_open * (1.0 + retracement_pct) * 1.01
                    sim_tb = TrailingPriceBundle(
                        min_since_open=float(min_since_open),
                        max_since_min=float(max_since_min),
                        max_since_open=float(sim_pos.price),
                        min_since_max=float(min_since_open),
                    )
                    # Keep bid at trigger-zone; Rust will choose min(bid, rounded trigger)
                    if threshold_pct > 0.0:
                        sim_bid = sim_pos.price * (1.0 - threshold_pct + retracement_pct)
                    else:
                        sim_bid = float(min_since_open) * (1.0 + retracement_pct)
                    sim_ask = float(max(float(getattr(sp.order_book, "ask", 0.0) or 0.0), float(sim_bid)))
                    sim_sp = StateParams(
                        balance=sp.balance,
                        order_book=OrderBook(bid=float(sim_bid), ask=float(sim_ask)),
                        ema_bands=sp.ema_bands,
                        entry_volatility_logrange_ema_1h=sp.entry_volatility_logrange_ema_1h,
                    )
                    debug_rust_data["long_next_entry_trailing_sim"] = {
                        "sim": sim_dbg,
                        "sim_wallet_exposure": float(wallet_exposure),
                        "sim_threshold_pct": float(threshold_pct),
                        "sim_retracement_pct": float(retracement_pct),
                        "mode": "immediate" if threshold_pct <= 0.0 else "thresholded",
                        "next": _calc_next_entry_rust(
                            pbr=pbr,
                            side=Side.Long,
                            exchange_params=ep,
                            state_params=sim_sp,
                            trailing_bundle=sim_tb,
                            bot_params=bp,
                            position=sim_pos,
                        ),
                    }
                    debug_rust_data["long_trailing_chain_forced"] = _simulate_trailing_sequence_forced(
                        pbr=pbr,
                        side=Side.Long,
                        exchange_params=ep,
                        state_params=sp,
                        bot_params=bp,
                        start_position=sim_pos,
                        n_steps=5,
                    )
                else:
                    debug_rust_data["long_next_entry_trailing_sim"] = {
                        "sim": sim_dbg,
                        "sim_wallet_exposure": float(wallet_exposure),
                        "sim_threshold_pct": float(threshold_pct),
                        "sim_retracement_pct": float(retracement_pct),
                        "note": "not simulated (requires retracement_pct>0)",
                    }
                    debug_rust_data["long_trailing_chain_forced"] = {"note": "not simulated (requires retracement_pct>0)", "sim": sim_dbg}
        except Exception as e:
            debug_rust_data["long_next_entry_error"] = str(e)

        # NORMAL LONG CLOSES
        bp_json = json.dumps(asdict(data.normal_bot_params_long), sort_keys=True)
        raw = _calc_closes_rust_cached(pb7_src, Side.Long.value, ep_json, sp_json_long, tb_json, bp_json, data.position_long_close.size, data.position_long_close.price)
        normal_closes_long = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw]
        data.normal_closes_long = adjust_order_quantities(normal_closes_long)

        # GRIDONLY LONG CLOSES
        bp_json = json.dumps(asdict(data.gridonly_bot_params_long), sort_keys=True)
        raw = _calc_closes_rust_cached(pb7_src, Side.Long.value, ep_json, sp_json_long, tb_json, bp_json, data.position_long_close.size, data.position_long_close.price)
        gridonly_closes_long = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw]
        data.gridonly_closes_long = adjust_order_quantities(gridonly_closes_long)
    else:
        data.normal_entries_long = []
        data.gridonly_entries_long = []
        data.normal_closes_long = []
        data.gridonly_closes_long = []
        data.potential_entry_trailing_prices_long = []
        data.simulated_entry_trailing_prices_long = []
        data.simulated_entry_trailing_orders_long = []
        data.simulated_entry_trailing_ref_price_long = 0.0
    
    if short_active:
        # NORMAL SHORT ENTRIES
        bp_json = json.dumps(asdict(data.normal_bot_params_short), sort_keys=True)
        raw = _calc_entries_rust_cached(pb7_src, Side.Short.value, ep_json, sp_json_short, tb_json, bp_json, data.position_short_entry.size, data.position_short_entry.price)

        debug_rust_data["short_entry_input"] = {
            "bp": json.loads(bp_json),
            "sp": json.loads(sp_json_short),
            "tb": json.loads(tb_json),
            "ep": json.loads(ep_json),
            "pos": {"size": data.position_short_entry.size, "price": data.position_short_entry.price}
        }
        debug_rust_data["short_entry_output_raw"] = raw

        normal_entries_short = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw]
        data.normal_entries_short = adjust_order_quantities(normal_entries_short)

        # GRIDONLY SHORT ENTRIES
        bp_json = json.dumps(asdict(data.gridonly_bot_params_short), sort_keys=True)
        raw_gridonly = _calc_entries_rust_cached(pb7_src, Side.Short.value, ep_json, sp_json_short, tb_json, bp_json, data.position_short_entry.size, data.position_short_entry.price)
        gridonly_entries_short = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw_gridonly]
        data.gridonly_entries_short = adjust_order_quantities(gridonly_entries_short)
        debug_rust_data["short_entry_gridonly_output_raw"] = raw_gridonly

        pot_prices, pot_dbg = _calc_potential_trailing_entry_prices_from_fullgrid(
            side=Side.Short,
            bot_params=data.normal_bot_params_short,
            exchange_params=data.exchange_params,
            balance=float((data.state_params_short or data.state_params).balance),
            fullgrid_orders=data.gridonly_entries_short,
        )
        data.potential_entry_trailing_prices_short = pot_prices
        debug_rust_data["short_entry_trailing_potential"] = pot_dbg

        # Simulated trailing steps (see LONG for rationale).
        data.simulated_entry_trailing_prices_short = []
        data.simulated_entry_trailing_orders_short = []
        data.simulated_entry_trailing_ref_price_short = 0.0
        try:
            bp = data.normal_bot_params_short
            sp = (data.state_params_short or data.state_params)
            ep = data.exchange_params
            mode = get_GridTrailing_mode(float(bp.entry_trailing_grid_ratio))

            start_pos: Position | None = None
            start_dbg: dict = {"mode": str(mode)}

            if mode == GridTrailingMode.GridFirst:
                sim_pos, sim_dbg = _simulate_gridfilled_position_for_trailing(
                    side=Side.Short,
                    raw_gridonly=debug_rust_data.get("short_entry_gridonly_output_raw") or [],
                    exchange_params=ep,
                    state_params=sp,
                    bot_params=bp,
                )
                start_pos = sim_pos
                start_dbg.update({"start": "gridfilled", "sim": sim_dbg})
            elif mode in (GridTrailingMode.TrailingOnly, GridTrailingMode.TrailingFirst):
                start_pos = data.position_short_entry
                start_dbg.update({"start": "current_pos"})

            if start_pos is not None and float(getattr(start_pos, "size", 0.0) or 0.0) == 0.0:
                init_tb = TrailingPriceBundle(min_since_open=0.0, max_since_min=0.0, max_since_open=0.0, min_since_max=0.0)
                init = _calc_next_entry_rust(
                    pbr=pbr,
                    side=Side.Short,
                    exchange_params=ep,
                    state_params=sp,
                    trailing_bundle=init_tb,
                    bot_params=bp,
                    position=Position(size=0.0, price=0.0),
                )
                if float(init.get("qty", 0.0) or 0.0) != 0.0 and float(init.get("price", 0.0) or 0.0) > 0.0:
                    start_pos = _apply_fill_to_position(
                        position=Position(size=0.0, price=0.0),
                        fill_qty=float(init["qty"]),
                        fill_price=float(init["price"]),
                    )
                    start_dbg.update({"start": "initial_fill", "initial_fill": init})

            if start_pos is not None and float(getattr(start_pos, "size", 0.0) or 0.0) != 0.0 and mode != GridTrailingMode.GridOnly:
                data.simulated_entry_trailing_ref_price_short = float(getattr(start_pos, "price", 0.0) or 0.0)
                chain = _simulate_trailing_sequence_forced(
                    pbr=pbr,
                    side=Side.Short,
                    exchange_params=ep,
                    state_params=sp,
                    bot_params=bp,
                    start_position=start_pos,
                    n_steps=25,
                )
                prices: list[float] = []
                orders: list[Order] = []
                for step in chain.get("steps", []) or []:
                    nxt = step.get("next") or {}
                    typ = str(nxt.get("type", ""))
                    if float(nxt.get("qty", 0.0) or 0.0) == 0.0:
                        break
                    if mode == GridTrailingMode.TrailingFirst and ("trailing" not in typ):
                        break
                    p = float(nxt.get("price", 0.0) or 0.0)
                    q = float(nxt.get("qty", 0.0) or 0.0)
                    if p > 0.0:
                        prices.append(p)
                        orders.append(Order(qty=float(q), price=float(p), order_type_str=typ))
                data.simulated_entry_trailing_prices_short = prices
                data.simulated_entry_trailing_orders_short = adjust_order_quantities(orders)
                debug_rust_data["short_entry_trailing_simulated"] = {"start": start_dbg, "count": len(prices), "prices": prices[:50]}
        except Exception as e:
            debug_rust_data["short_entry_trailing_simulated_error"] = str(e)

        try:
            debug_rust_data["short_next_entry_current"] = _calc_next_entry_rust(
                pbr=pbr,
                side=Side.Short,
                exchange_params=data.exchange_params,
                state_params=(data.state_params_short or data.state_params),
                trailing_bundle=data.trailing_price_bundle,
                bot_params=data.normal_bot_params_short,
                position=data.position_short_entry,
            )

            bp = data.normal_bot_params_short
            sp = (data.state_params_short or data.state_params)
            ep = data.exchange_params
            sim_pos, sim_dbg = _simulate_gridfilled_position_for_trailing(
                side=Side.Short,
                raw_gridonly=debug_rust_data.get("short_entry_gridonly_output_raw") or [],
                exchange_params=ep,
                state_params=sp,
                bot_params=bp,
            )
            if sim_pos is not None:
                per_pos_wel = float(bp.total_wallet_exposure_limit) / float(bp.n_positions) if bp.n_positions else float(bp.total_wallet_exposure_limit)
                allowed_wel = per_pos_wel * (1.0 + max(0.0, float(getattr(bp, "risk_we_excess_allowance_pct", 0.0) or 0.0))) if per_pos_wel > 0.0 else per_pos_wel
                c_mult = float(ep.c_mult or 1.0) or 1.0
                wallet_exposure = (abs(sim_pos.size) * sim_pos.price * c_mult) / float(sp.balance)
                effective_wel = allowed_wel
                we_over = (wallet_exposure / effective_wel) if effective_wel > 0.0 else 0.0
                th_mult = we_over * float(bp.entry_trailing_threshold_we_weight)
                th_log_mult = float(sp.entry_volatility_logrange_ema_1h) * float(bp.entry_trailing_threshold_volatility_weight)
                threshold_pct = float(bp.entry_trailing_threshold_pct) * max(0.0, 1.0 + th_mult + th_log_mult)
                re_mult = we_over * float(bp.entry_trailing_retracement_we_weight)
                re_log_mult = float(sp.entry_volatility_logrange_ema_1h) * float(bp.entry_trailing_retracement_volatility_weight)
                retracement_pct = float(bp.entry_trailing_retracement_pct) * max(0.0, 1.0 + re_mult + re_log_mult)

                if retracement_pct > 0.0:
                    # SHORT trigger condition in Rust:
                    # threshold>0: max_since_open > pos.price*(1+threshold) AND min_since_max < max_since_open*(1-retracement)
                    # threshold<=0 (immediate trailing): min_since_max < max_since_open*(1-retracement)
                    if threshold_pct > 0.0:
                        max_since_open = sim_pos.price * (1.0 + threshold_pct) * 1.01
                    else:
                        max_since_open = sim_pos.price * 1.01
                    min_since_max = max_since_open * (1.0 - retracement_pct) * 0.99
                    sim_tb = TrailingPriceBundle(
                        min_since_open=float(min_since_max),
                        max_since_min=float(max_since_open),
                        max_since_open=float(max_since_open),
                        min_since_max=float(min_since_max),
                    )
                    # Keep ask at trigger-zone; Rust will choose max(ask, rounded trigger)
                    if threshold_pct > 0.0:
                        sim_ask = sim_pos.price * (1.0 + threshold_pct - retracement_pct)
                    else:
                        sim_ask = float(max_since_open) * (1.0 - retracement_pct)
                    sim_bid = float(min(float(getattr(sp.order_book, "bid", 0.0) or 0.0), float(sim_ask)))
                    sim_sp = StateParams(
                        balance=sp.balance,
                        order_book=OrderBook(bid=float(sim_bid), ask=float(sim_ask)),
                        ema_bands=sp.ema_bands,
                        entry_volatility_logrange_ema_1h=sp.entry_volatility_logrange_ema_1h,
                    )
                    debug_rust_data["short_next_entry_trailing_sim"] = {
                        "sim": sim_dbg,
                        "sim_wallet_exposure": float(wallet_exposure),
                        "sim_threshold_pct": float(threshold_pct),
                        "sim_retracement_pct": float(retracement_pct),
                        "mode": "immediate" if threshold_pct <= 0.0 else "thresholded",
                        "next": _calc_next_entry_rust(
                            pbr=pbr,
                            side=Side.Short,
                            exchange_params=ep,
                            state_params=sim_sp,
                            trailing_bundle=sim_tb,
                            bot_params=bp,
                            position=sim_pos,
                        ),
                    }
                    debug_rust_data["short_trailing_chain_forced"] = _simulate_trailing_sequence_forced(
                        pbr=pbr,
                        side=Side.Short,
                        exchange_params=ep,
                        state_params=sp,
                        bot_params=bp,
                        start_position=sim_pos,
                        n_steps=5,
                    )
                else:
                    debug_rust_data["short_next_entry_trailing_sim"] = {
                        "sim": sim_dbg,
                        "sim_wallet_exposure": float(wallet_exposure),
                        "sim_threshold_pct": float(threshold_pct),
                        "sim_retracement_pct": float(retracement_pct),
                        "note": "not simulated (requires retracement_pct>0)",
                    }
                    debug_rust_data["short_trailing_chain_forced"] = {
                        "note": "not simulated (requires retracement_pct>0)",
                        "sim": sim_dbg,
                    }
        except Exception as e:
            debug_rust_data["short_next_entry_error"] = str(e)

        # NORMAL SHORT CLOSES
        bp_json = json.dumps(asdict(data.normal_bot_params_short), sort_keys=True)
        raw = _calc_closes_rust_cached(pb7_src, Side.Short.value, ep_json, sp_json_short, tb_json, bp_json, data.position_short_close.size, data.position_short_close.price)
        normal_closes_short = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw]
        data.normal_closes_short = adjust_order_quantities(normal_closes_short)

        # GRIDONLY SHORT CLOSES
        bp_json = json.dumps(asdict(data.gridonly_bot_params_short), sort_keys=True)
        raw = _calc_closes_rust_cached(pb7_src, Side.Short.value, ep_json, sp_json_short, tb_json, bp_json, data.position_short_close.size, data.position_short_close.price)
        gridonly_closes_short = [Order(qty=float(q), price=float(p), order_type_str=_order_type_to_str(pbr, t)) for q, p, t in raw]
        data.gridonly_closes_short = adjust_order_quantities(gridonly_closes_short)
    else:
        data.normal_entries_short = []
        data.gridonly_entries_short = []
        data.normal_closes_short = []
        data.gridonly_closes_short = []
        data.potential_entry_trailing_prices_short = []
        data.simulated_entry_trailing_prices_short = []
        data.simulated_entry_trailing_orders_short = []

    st.session_state.v7_grid_visualizer_data = data
    
    with col2:
        if data.isActive(Side.Long):
            create_plotly_graph(Side.Long, data)
            create_statistics(Side.Long, data)
            with st.expander("Debug Rust Interface (LONG)", expanded=False):
                st.write("**Sent to Rust:**")
                _render_debug_json(debug_rust_data.get("long_entry_input"))
                st.write("**Received from Rust (Raw):**")
                st.write(debug_rust_data.get("long_entry_output_raw"))
                st.write("**Received from Rust (Decoded):**")
                st.dataframe(_decode_rust_orders_for_debug(pbr, debug_rust_data.get("long_entry_output_raw")))
                st.divider()
                st.write("**GridOnly Entries (Raw):**")
                st.write(debug_rust_data.get("long_entry_gridonly_output_raw"))
                st.write("**GridOnly Entries (Decoded):**")
                st.dataframe(_decode_rust_orders_for_debug(pbr, debug_rust_data.get("long_entry_gridonly_output_raw")))
                st.divider()
                st.write("**GridFirst cutoff (from grid-only orders; approx trailing start)**")
                _render_debug_json(debug_rust_data.get("long_entry_trailing_potential"))
                st.divider()
                st.write("**Next Entry (Current State)**")
                _render_debug_json(debug_rust_data.get("long_next_entry_current"))
                st.write("**Next Entry (Simulated: trailing active + triggered)**")
                _render_debug_json(debug_rust_data.get("long_next_entry_trailing_sim"))
                st.write("**Trailing Chain (Forced Trigger, Debug)**")
                _render_debug_json(debug_rust_data.get("long_trailing_chain_forced"))
                if debug_rust_data.get("long_next_entry_error"):
                    st.error(debug_rust_data.get("long_next_entry_error"))
        else:
            st.write("LONG is inactive")
            
    with col3:
        if data.isActive(Side.Short):
            create_plotly_graph(Side.Short, data)
            create_statistics(Side.Short, data)
            with st.expander("Debug Rust Interface (SHORT)", expanded=False):
                st.write("**Sent to Rust:**")
                _render_debug_json(debug_rust_data.get("short_entry_input"))
                st.write("**Received from Rust (Raw):**")
                st.write(debug_rust_data.get("short_entry_output_raw"))
                st.write("**Received from Rust (Decoded):**")
                st.dataframe(_decode_rust_orders_for_debug(pbr, debug_rust_data.get("short_entry_output_raw")))
                st.divider()
                st.write("**GridOnly Entries (Raw):**")
                st.write(debug_rust_data.get("short_entry_gridonly_output_raw"))
                st.write("**GridOnly Entries (Decoded):**")
                st.dataframe(_decode_rust_orders_for_debug(pbr, debug_rust_data.get("short_entry_gridonly_output_raw")))
                st.divider()
                st.write("**GridFirst cutoff (from grid-only orders; approx trailing start)**")
                _render_debug_json(debug_rust_data.get("short_entry_trailing_potential"))
                st.divider()
                st.write("**Next Entry (Current State)**")
                _render_debug_json(debug_rust_data.get("short_next_entry_current"))
                st.write("**Next Entry (Simulated: trailing active + triggered)**")
                _render_debug_json(debug_rust_data.get("short_next_entry_trailing_sim"))
                st.write("**Trailing Chain (Forced Trigger, Debug)**")
                _render_debug_json(debug_rust_data.get("short_trailing_chain_forced"))
                if debug_rust_data.get("short_next_entry_error"):
                    st.error(debug_rust_data.get("short_next_entry_error"))
        else:
            st.write("SHORT is inactive")



def build_sidebar():
    # Navigation
    with st.sidebar:
        if st.button("Reset"):
            if "v7_grid_visualizer_data" in st.session_state:
                del st.session_state.v7_grid_visualizer_data
            if "v7_grid_visualizer_config" in st.session_state:
                del st.session_state.v7_grid_visualizer_config
            st.rerun()

def generate_animation_v7(start_time, frames, step_mins, hist_df, symbol, context_days, side_val, data_template, output_container=None):
    if output_container is None:
        output_container = st.container()
        
    with output_container:
        st.info("Generating Animation... please wait.")
        
        # Pre-calculate indicators on the whole set (or relevant subset)
        # We need data from (start - context) to (start + frames * step)
        end_time = start_time + datetime.timedelta(minutes=frames * step_mins)
        
        calc_params = data_template.normal_bot_params_long if side_val == Side.Long.value else data_template.normal_bot_params_short
        
        df_calc = calculate_v7_indicators(
            hist_df, 
            calc_params.ema_span_0, 
            calc_params.ema_span_1, 
            calc_params.entry_volatility_ema_span_hours
        )
        
        fig_frames = []
        
        prog_bar = st.progress(0)
        
        pbr_slider = _get_passivbot_rust(pb7dir())
        if pbr_slider is None:
             st.error("Passivbot Rust bindings not found!")
             return
        
        # --- OPTIMIZATION START ---
        # Resample data for visualization to prevent "WebSocket Message Tool Large" errors.
        # We target approx 300-400 candles per visible context window.
        # context_days * 1440 mins / X mins = 300  => X = (context_days * 1440) / 300
        total_ctx_mins = context_days * 1440
        opt_res_mins = int(total_ctx_mins / 300)
        if opt_res_mins < 1:
            opt_res_mins = 1
        
        # Create a separated dataframe just for plotting
        if opt_res_mins > 1:
            st.info(f"Optimizing animation: Resampling visualization to {opt_res_mins}m candles for performance.")
            # Define aggregation rules
            agg_dict = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }
            # Add other columns (indicators) as 'last'
            for c in df_calc.columns:
                if c not in agg_dict:
                    agg_dict[c] = 'last'
            
            # Resample and DropNA to remove empty bins
            df_plot_source = df_calc.resample(f"{opt_res_mins}min").agg(agg_dict).dropna()
        else:
            df_plot_source = df_calc
            
        # --- OPTIMIZATION END ---

        # Define helper for grid trace (needs df_window from scope or arg)
        def make_grid_trace(prices, color, name, x0, x1):
            x_vals = []
            y_vals = []
            if not prices:
                return go.Scatter(x=[], y=[])
            
            for p in prices:
                x_vals.extend([x0, x1, None])
                y_vals.extend([p, p, None])
            
            return go.Scatter(
                x=x_vals, y=y_vals, mode='lines', 
                line=dict(color=color, width=1, dash='dot'),
                name=name
            )

        with st.expander("Debug Animation Info"):
            st.write(f"Hist DF Size: {len(hist_df)}")
            if not hist_df.empty:
                 st.write(f"Data Start: {hist_df.index.min()}")
                 st.write(f"Data End: {hist_df.index.max()}")
            st.write(f"Animation Start: {start_time}")
            st.write(f"Animation End: {end_time}")
            st.write(f"Timezone (Data): {getattr(hist_df.index, 'tz', 'None')}")
            st.write(f"Timezone (Start): {getattr(start_time, 'tzinfo', 'None')}")

        debug_log = st.empty()
        
        for i in range(frames):
            try:
                current_time = start_time + datetime.timedelta(minutes=i * step_mins)
                
                # Stop if out of bounds (approx check)
                if not hist_df.empty and current_time > hist_df.index.max() + datetime.timedelta(hours=48):
                     st.warning(f"Stopping at frame {i}: End of data reached ({current_time})")
                     break
    
                # Update progress bar safely
                progress_val = min(1.0, (i + 1) / frames)
                prog_bar.progress(progress_val)
                
                # Ensure current_time is compatible with index (timestamp conversion if needed)
                idx_res = df_calc.index.get_indexer([current_time], method='nearest')
                
                if len(idx_res) == 0 or idx_res[0] == -1:
                    st.warning(f"Frame {i}: Timestamp lookup failed for {current_time}")
                    continue
                    
                idx_loc = idx_res[0]
                row = df_calc.iloc[idx_loc]
                current_idx_time = df_calc.index[idx_loc]
                    
                # Sanity check
                time_diff = abs((current_idx_time - current_time).total_seconds())
                if time_diff > step_mins * 60 * 10:
                    if current_time > current_idx_time:
                         st.info(f"Frame {i}: Reached end of data coverage at {current_time} (Nearest: {current_idx_time}). Stopping.")
                         break
                
                ctx_start = current_time - datetime.timedelta(days=context_days)
                mask = (hist_df.index >= ctx_start) & (hist_df.index <= current_idx_time)
                df_window = hist_df.loc[mask]
                
                if df_window.empty:
                    # st.write(f"Frame {i} empty window")
                    continue
    
                sp_balance = data_template.state_params.balance
                sp_ob_bid = float(row["close"]) # approx
                sp_ob_ask = float(row["close"])
                close_px = float(row["close"])
                e0 = float(row.get("ema_0", close_px))
                e1 = float(row.get("ema_1", close_px))
                e2 = float(row.get("ema_2", close_px))
                sp_ema_lower = float(min(e0, e1, e2))
                sp_ema_upper = float(max(e0, e1, e2))
                
                vol = row["volatility"]
                
                # Create cloned data object with updated state
                c_ob = OrderBook(bid=sp_ob_bid, ask=sp_ob_ask)
                c_ema = EmaBands(lower=sp_ema_lower, upper=sp_ema_upper)
                c_sp = StateParams(balance=sp_balance, order_book=c_ob, ema_bands=c_ema, entry_volatility_logrange_ema_1h=vol)
                ani_data = replace(data_template, state_params=c_sp)
                
                bp = data_template.normal_bot_params_long if side_val == Side.Long.value else data_template.normal_bot_params_short
                c_side = Side.Long if side_val == Side.Long.value else Side.Short
                pos_entry = Position(size=0.0, price=sp_ob_ask if c_side == Side.Long else sp_ob_bid)
    
                # Call standard rust wrapper
                try:
                    entries = _calc_entries_rust(pbr_slider, c_side, ani_data, bp, pos_entry)
                    
                    pos_close = Position(size=100.0, price=sp_ob_ask)
                    closes = _calc_closes_rust(pbr_slider, c_side, ani_data, bp, pos_close)
                except Exception as e:
                    st.error(f"Rust calc error at frame {i}: {e}")
                    # Don't break, just empty orders for this frame? or break?
                    # If rust fails, likely param issue. 
                    entries = []
                    closes = []
                                           
                entry_prices = [p for p in [o.price for o in entries] if p > 0]
                close_prices = [p for p in [o.price for o in closes] if p > 0]
    
                # --- PLOTTING TRACES (Using Optimized Data) ---
                mask_plot = (df_plot_source.index >= ctx_start) & (df_plot_source.index <= current_idx_time)
                df_window_plot = df_plot_source.loc[mask_plot]
                
                if df_window_plot.empty:
                    continue

                trace_candle = go.Candlestick(
                    x=df_window_plot.index,
                    open=df_window_plot['open'],
                    high=df_window_plot['high'],
                    low=df_window_plot['low'],
                    close=df_window_plot['close'],
                    name='Price'
                )
                
                # Check for EMA columns in the plot source
                if "ema_0" in df_window_plot.columns and "ema_1" in df_window_plot.columns:
                    cols = ["ema_0", "ema_1"]
                    if "ema_2" in df_window_plot.columns:
                        cols.append("ema_2")
                    upper_band = df_window_plot[cols].max(axis=1)
                    lower_band = df_window_plot[cols].min(axis=1)
                else:
                    # Fallback if columns missing (should not happen with correct agg)
                    upper_band = df_window_plot['high']
                    lower_band = df_window_plot['low']

                trace_ema_high = go.Scatter(
                    x=df_window_plot.index,
                    y=upper_band,
                    mode='lines',
                    line=dict(color='magenta', width=1, dash='solid'),
                    name='EMA High'
                )
                trace_ema_low = go.Scatter(
                    x=df_window_plot.index,
                    y=lower_band,
                    mode='lines',
                    line=dict(color='cyan', width=1, dash='dot'),
                    name='EMA Low'
                )
    
                x_left = df_window_plot.index[0]
                try:
                    if int(opt_res_mins) > 1:
                        x_left = x_left - pd.Timedelta(minutes=int(opt_res_mins))
                except Exception:
                    pass
                x_right = df_window_plot.index[-1]

                trace_entries = make_grid_trace(entry_prices, 'rgba(255, 0, 0, 0.6)', 'Entry Grid', x_left, x_right)
                trace_closes = make_grid_trace(close_prices, 'rgba(0, 255, 0, 0.6)', 'Close Grid', x_left, x_right)
                
                frame_data = [
                    trace_candle,
                    trace_ema_high,
                    trace_ema_low,
                    trace_entries,
                    trace_closes
                ]
                
                # Explicit ranges for sliding window animation
                y_vals = [
                     df_window_plot['high'].max(),
                     df_window_plot['low'].min(),
                     upper_band.max() if not upper_band.empty else 0,
                     lower_band.min() if not lower_band.empty else 0
                ]
                if entry_prices:
                     y_vals.extend(entry_prices)
                if close_prices:
                     y_vals.extend(close_prices)
                
                # Filter None/NaN from y_vals
                y_vals = [y for y in y_vals if y is not None and not np.isnan(y)]
                
                if not y_vals:
                    # Fallback
                    y_min_frame = 0
                    y_max_frame = 100
                else: 
                    y_min_frame = min(y_vals) * 0.995
                    y_max_frame = max(y_vals) * 1.005
                
                frame_layout = dict(
                    xaxis=dict(range=[x_left, x_right]),
                    yaxis=dict(range=[y_min_frame, y_max_frame])
                )
                
                fig_frames.append(go.Frame(data=frame_data, layout=frame_layout, name=str(i)))
                
            except Exception as e:
                st.error(f"Critical error in frame {i} loop: {e}")
                import traceback
                st.text(traceback.format_exc())
                break


    if not fig_frames:
        st.error("No frames generated")
        return

    init_frame = fig_frames[0]
    init_data = init_frame.data
    
    # Base layout
    layout_args = dict(
        title=f"Animation: {symbol} ({start_time} - {end_time}) | Frames: {len(fig_frames)}",
        xaxis=dict(type='date', rangeslider=dict(visible=False)),
        yaxis=dict(autorange=False, fixedrange=False),
        height=800, 
        margin=dict(l=50, r=50, t=50, b=150),
        legend=dict(x=1, y=1, xanchor='right', yanchor='top', bgcolor='rgba(0,0,0,0.5)'),
        updatemenus=[dict(
            type="buttons",
            direction="left",
            showactive=True,
            x=0.0,
            y=-0.15, 
            xanchor="left",
            yanchor="top",
            pad={"r": 10, "t": 10},
            buttons=[
                dict(label="▶ Play",
                     method="animate",
                     args=[None, dict(frame=dict(duration=100, redraw=True), 
                                      fromcurrent=True, 
                                      transition=dict(duration=0, easing="linear"))]),
                dict(label="⏸ Pause",
                     method="animate",
                     args=[[None], dict(frame=dict(duration=0, redraw=False), 
                                        mode="immediate", 
                                        transition=dict(duration=0))])
            ]
        )],
        sliders = [dict(
            active=0,
            steps=[dict(method='animate',
                        args=[[str(k)], dict(mode='immediate',
                                           frame=dict(duration=0, redraw=True), 
                                           transition=dict(duration=0))],
                        label=f"{k}"
                       ) for k in range(len(fig_frames))], 
            transition=dict(duration=0),
            x=0.15, 
            y=-0.15, 
            currentvalue=dict(font=dict(size=12), prefix='Frame: ', visible=True, xanchor='center'),
            len=0.85,
            pad={"b": 10, "t": 10}
        )]
    )

    # Sync initial view with first frame's range
    if init_frame.layout:
        # Note: Plotly graph objects iterate keys, not items, so dict.update() fails.
        # We manually fetch the range property.
        if 'xaxis' in init_frame.layout and 'range' in init_frame.layout['xaxis']:
             layout_args['xaxis']['range'] = init_frame.layout['xaxis']['range']
        if 'yaxis' in init_frame.layout and 'range' in init_frame.layout['yaxis']:
             layout_args['yaxis']['range'] = init_frame.layout['yaxis']['range']
    
    layout = go.Layout(**layout_args)
    
    fig = go.Figure(data=init_data, layout=layout, frames=fig_frames)
    st.plotly_chart(fig, use_container_width=True)


def generate_animation_v7_modeb(
    *,
    start_time: pd.Timestamp,
    frames: int,
    step_mins: int,
    hist_df: pd.DataFrame,
    exchange: str,
    symbol: str,
    context_days: float,
    side_val: int,
    data_template,
    output_container=None,
    initial_frame_idx: int = 0,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> None:
    """Movie/animation builder driven by Mode B candle-walk state.

    - Candles play (Plotly animation with Play/Pause)
    - Entry/close grids evolve across time (filled orders disappear; new ones appear)
    - Highlights the next trailing entry/close order (if present)
    """
    if output_container is None:
        output_container = st.container()

    with output_container:
        if hist_df is None or hist_df.empty:
            st.error("No historical candles loaded.")
            return

        try:
            pbr = _get_passivbot_rust(_pb7_src_dir())
        except Exception:
            pbr = None
        if pbr is None:
            st.error("Passivbot Rust bindings not found!")
            return

        frames = int(frames or 0)
        step_mins = int(step_mins or 0)
        if frames <= 0 or step_mins <= 0:
            st.error("Invalid animation parameters.")
            return

        start_time = pd.to_datetime(start_time)
        end_time = start_time + datetime.timedelta(minutes=frames * step_mins)
        ctx_days = float(context_days or 0.0)
        if ctx_days <= 0:
            ctx_days = 5.0

        # For coarse steps (notably 4h), we want a reasonable number of visible candles.
        # Ensure we have enough warmup candles to render the viewport from the start.
        if int(step_mins) == 240:
            # 60 x 4h candles = 10 days of lookback.
            ctx_days = max(ctx_days, 10.0)
        warm_start_plot = start_time - datetime.timedelta(days=ctx_days)

        # Plot source may include context before the simulated trading start.
        plot_df = hist_df.loc[(hist_df.index >= warm_start_plot) & (hist_df.index <= end_time)].copy()
        if plot_df.empty:
            st.error("No candles for the selected time window.")
            return

        # Simulation candles will be rebuilt with proper warmup (see below).
        # Use plot_df for base timeframe inference.
        sim_df = plot_df.copy()

        # Infer base timeframe (minutes) for translating step_mins into candle steps
        base_tf_mins = 1
        try:
            idx = pd.to_datetime(sim_df.index)
            deltas = (idx[1: min(len(idx), 200)] - idx[: min(len(idx) - 1, 199)]).total_seconds()
            deltas = [d for d in deltas if d and d > 0]
            if deltas:
                base_tf_mins = int(round(np.median(deltas) / 60.0))
                if base_tf_mins <= 0:
                    base_tf_mins = 1
        except Exception:
            base_tf_mins = 1

        fe = max(1, int(round(step_mins / float(base_tf_mins))))

        # Epsilon for comparing wick-touch to order price
        try:
            _ps = float(getattr(data_template.exchange_params, "price_step", 0.0) or 0.0)
        except Exception:
            _ps = 0.0
        price_eps = (_ps * 1e-6) if _ps > 0.0 else 1e-12

        bp = data_template.normal_bot_params_long if int(side_val) == int(Side.Long.value) else data_template.normal_bot_params_short
        side_obj = Side.Long if int(side_val) == int(Side.Long.value) else Side.Short

        def _disable_bot_params(bp_in: BotParams) -> BotParams:
            d = asdict(bp_in)
            d["total_wallet_exposure_limit"] = 0.0
            if "n_positions" in d:
                d["n_positions"] = 0
            return BotParams(**d)

        # Fee parity: prefer the same inference used by compare/PB7 helpers.
        try:
            maker_fee, _taker_fee = _infer_maker_taker_fees(str(exchange), str(symbol))
            maker_fee = float(maker_fee or 0.0)
        except Exception:
            fees = _derive_exchange_fees_from_market(exchange, symbol)
            maker_fee = float(fees.get("maker_fee", 0.0) or 0.0)

        stage_line = st.empty()
        prog_bar = st.progress(0.0)

        def _progress(frac: float, msg: str) -> None:
            f = float(frac)
            if not math.isfinite(f):
                f = 0.0
            f = max(0.0, min(1.0, f))
            try:
                stage_line.caption(str(msg))
            except Exception:
                pass
            try:
                prog_bar.progress(f)
            except Exception:
                pass
            if progress_cb is not None:
                try:
                    progress_cb(f, str(msg))
                except Exception:
                    pass

        _progress(0.0, "Mode B: simulating...")

        try:
            def _sim_cb(frac: float, msg: str) -> None:
                _progress(0.0 + 0.6 * float(frac), f"Mode B: {msg}")

            # If a PB7 backtest dir is provided (compare panel), run Mode B from backtest.start_date
            # (with warmup) so the state at `start_time` matches PB7. Otherwise, simulate from start_time.
            pb7_dir = str(st.session_state.get("gv_hist_compare_pb7_dir", "") or "")
            trade_start_time_for_sim = start_time
            sim_start_balance = float(getattr(data_template.state_params, "balance", 0.0) or 0.0)
            bp_long_sim = data_template.normal_bot_params_long
            bp_short_sim = data_template.normal_bot_params_short
            using_pb7_config = False
            try:
                if pb7_dir and os.path.isfile(os.path.join(os.path.expanduser(pb7_dir), "config.json")):
                    cfg_path = os.path.join(os.path.expanduser(pb7_dir), "config.json")
                    with open(cfg_path, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                    # Note: do NOT shift `trade_start_time_for_sim` based on backtest.start_date.
                    # Movie builder should start trading at the selected `start_time`.
                    try:
                        sim_start_balance = float((cfg.get("backtest") or {}).get("starting_balance") or sim_start_balance)
                    except Exception:
                        pass
                    try:
                        bot_cfg = cfg.get("bot") or {}
                        bp_long_sim = BotParams(**(bot_cfg.get("long") or {}))
                        bp_short_sim = BotParams(**(bot_cfg.get("short") or {}))
                    except Exception:
                        bp_long_sim = data_template.normal_bot_params_long
                        bp_short_sim = data_template.normal_bot_params_short
                    using_pb7_config = True
            except Exception:
                pass

            # Match Mode C movie behavior: simulate only the selected side.
            # (Otherwise opposite-side fills can change shared balance and alter the selected side's fills.)
            try:
                if side_obj == Side.Long:
                    bp_short_sim = _disable_bot_params(bp_short_sim)
                else:
                    bp_long_sim = _disable_bot_params(bp_long_sim)
            except Exception:
                pass

            # Always include proper warmup candles to match PB7/compare state at `start_time`.
            try:
                if using_pb7_config and isinstance(locals().get("cfg"), dict):
                    warmup_minutes = int(_compute_warmup_minutes_for_mode_c_from_config(cfg, bp_long_sim, bp_short_sim))
                else:
                    warmup_minutes = int(_compute_warmup_minutes_for_mode_c(bp_long_sim, bp_short_sim))
            except Exception:
                warmup_minutes = int(_compute_warmup_minutes_for_mode_c(bp_long_sim, bp_short_sim))

            warm_start = pd.to_datetime(start_time) - pd.Timedelta(minutes=max(0, int(warmup_minutes)))
            try:
                sim_df = hist_df.loc[warm_start:end_time].copy()
                sim_df = _standardize_ohlcv_1m_gaps(sim_df, start_ts=warm_start, end_ts=end_time)
            except Exception:
                sim_df = hist_df.loc[warm_start:end_time].copy()

            if sim_df is None or sim_df.empty or len(sim_df) < 2:
                st.error("Not enough candles for Mode B simulation.")
                return

            # Plot indicators should match the simulation bot params. Otherwise EMA bands may collapse
            # (e.g. if UI params differ from PB7 config used for the sim).
            bp_plot = bp_long_sim if side_obj == Side.Long else bp_short_sim
            if not using_pb7_config:
                bp_plot = bp

            # Start from flat positions; running from trade_start_time_for_sim will build correct state.
            starting_pos_long = Position(size=0.0, price=0.0)
            starting_pos_short = Position(size=0.0, price=0.0)
            # Movie Builder must not be capped by the "Simulation" panel's max fills.
            # If user requests 200 frames @ 4h, we must simulate all fills needed for that full horizon.
            max_orders = 0

            # Capture frames only from (start_time - one step) to keep memory bounded.
            capture_from = start_time - pd.Timedelta(minutes=int(step_mins) * 2)
            ev_l, ev_s, replay_frames = _simulate_backtest_over_historical_candles_replay_orchestrator_pair(
                pbr=pbr,
                pb7_src=_pb7_src_dir(),
                side_for_frames=side_obj,
                candles=sim_df,
                exchange_params=data_template.exchange_params,
                bot_params_long=bp_long_sim,
                bot_params_short=bp_short_sim,
                starting_position_long=starting_pos_long,
                starting_position_short=starting_pos_short,
                balance=float(sim_start_balance),
                maker_fee=maker_fee,
                trade_start_time=trade_start_time_for_sim,
                max_orders=max_orders,
                max_candles=int(len(sim_df)),
                frame_every_n_candles=fe,
                capture_frames_from_time=capture_from,
                progress_cb=_sim_cb,
            )

            sim_events = (ev_l or []) if side_obj == Side.Long else (ev_s or [])
        except Exception as e:
            st.error(f"Mode B simulation failed: {e}")
            return

        if not replay_frames:
            st.error("No frames generated.")
            return

        # Precompute fills table for marker rendering (fills may occur between sampled frames).
        sim_events_df = pd.DataFrame(sim_events or [])
        try:
            if not sim_events_df.empty and "timestamp" in sim_events_df.columns:
                sim_events_df["timestamp"] = pd.to_datetime(sim_events_df["timestamp"])
                sim_events_df = sim_events_df.sort_values("timestamp").reset_index(drop=True)
                sim_events_df["ord_idx"] = np.arange(0, len(sim_events_df), dtype=int)
        except Exception:
            sim_events_df = pd.DataFrame(columns=["timestamp", "event", "price", "qty", "order_type", "pos_size"])

        try:
            if sim_events_df.empty:
                st.caption("Mode B: no fills in simulation window.")
            else:
                # Show count within the movie range for parity with Mode C.
                try:
                    _cnt_df = sim_events_df[(sim_events_df["timestamp"] >= pd.to_datetime(start_time)) & (sim_events_df["timestamp"] <= pd.to_datetime(end_time))]
                    st.caption(f"Mode B: simulated fills (movie range): {len(_cnt_df)}")
                except Exception:
                    st.caption(f"Mode B: simulated fills: {len(sim_events_df)}")
        except Exception:
            pass

        # Build a timestamp index so the movie can start at `start_time` (not at warmup frames).
        try:
            replay_ts = pd.to_datetime([fr.get("timestamp") for fr in replay_frames])
        except Exception:
            replay_ts = pd.to_datetime([])
        if len(replay_ts) != len(replay_frames):
            st.error("Replay frame timestamps malformed.")
            return

        # Indicators for plotting (small window only)
        df_calc = calculate_v7_indicators(
            plot_df,
            float(bp_plot.ema_span_0),
            float(bp_plot.ema_span_1),
            float(bp_plot.entry_volatility_ema_span_hours),
        )

        # Resample plot source to keep payload small
        # Movie: build displayed candles exactly from the 1m source into the chosen step size.
        # (Same behavior as the previous 4h special-case, but for all step sizes.)
        try:
            opt_res_mins = int(max(1, int(step_mins)))
        except Exception:
            opt_res_mins = 1

        # Visible window: default 60 candles (rolling window).
        # For 1m playback, show 120 candles for more context.
        target_visible_candles = 120 if int(opt_res_mins) == 1 else 60
        try:
            window_mins = int(target_visible_candles * int(opt_res_mins))
        except Exception:
            window_mins = int(target_visible_candles)

        if opt_res_mins > 1:
            agg_dict = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
            for c in df_calc.columns:
                if c not in agg_dict:
                    agg_dict[c] = "last"
            # Completed step candles: bins end on (start_time + k*step)
            rs_kwargs = {"origin": pd.to_datetime(start_time), "label": "right", "closed": "right"}
            df_plot_source = df_calc.resample(f"{opt_res_mins}min", **rs_kwargs).agg(agg_dict).dropna()
        else:
            df_plot_source = df_calc

        def make_grid_trace(prices: list[float], color: str, name: str, x0, x1, width: int = 1):
            if not prices:
                return go.Scatter(x=[], y=[])
            x_vals: list = []
            y_vals: list = []
            for p in prices:
                x_vals.extend([x0, x1, None])
                y_vals.extend([p, p, None])
            return go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="lines",
                line=dict(color=color, width=width, dash="dot"),
                name=name,
            )

        fig_frames = []

        # Movie frames are driven by the requested stepping starting at start_time.
        _progress(0.6, "Mode B: building frames...")
        frame_progress_every = max(1, int(int(frames) // 200))
        for i in range(int(frames)):
            if (i == 0) or (i == int(frames) - 1) or (i % frame_progress_every == 0):
                _progress(0.6 + 0.4 * float(i + 1) / float(max(1, int(frames))), f"Mode B: building frames {i + 1}/{int(frames)}")

            current_time = start_time + datetime.timedelta(minutes=int(i) * int(step_mins))

            # Pick the closest captured replay frame to this time.
            try:
                # Use last-known state at or before current_time.
                fr_idx = int(replay_ts.get_indexer([current_time], method="pad")[0])
                if fr_idx < 0:
                    fr_idx = int(replay_ts.get_indexer([current_time], method="nearest")[0])
            except Exception:
                fr_idx = -1
            if fr_idx < 0 or fr_idx >= len(replay_frames):
                break
            fr = replay_frames[fr_idx]

            ts = pd.to_datetime(fr.get("timestamp"))
            # Stop if we drift too far away from the requested stepping (out of coverage).
            try:
                if abs((ts - current_time).total_seconds()) > float(step_mins) * 60.0 * 10.0:
                    break
            except Exception:
                pass

            # Tie the plotted candle window to the chosen replay state timestamp.
            # With coarse stepping, the nearest captured replay frame can lag behind `current_time`.
            # If we plot candles beyond the state timestamp, pending close grids (especially trailing closes)
            # appear "too early" relative to the visible candles.
            upper_time = ts
            try:
                if pd.notna(current_time) and pd.notna(ts):
                    upper_time = min(pd.to_datetime(current_time), pd.to_datetime(ts))
            except Exception:
                upper_time = ts

            ctx_start = pd.to_datetime(upper_time) - datetime.timedelta(minutes=int(window_mins))
            # Grow the viewport from start_time until we reach the target visible candles,
            # then roll (oldest candles disappear to the left).
            if ctx_start < start_time:
                ctx_start = start_time

            try:
                if not df_plot_source.empty and upper_time > df_plot_source.index.max():
                    upper_time = df_plot_source.index.max()
            except Exception:
                pass

            # Use strict lower bound to avoid off-by-one candles (e.g. 4h should show exactly 1 candle in a 4h window).
            mask_plot = (df_plot_source.index > ctx_start) & (df_plot_source.index <= upper_time)
            df_window_plot = df_plot_source.loc[mask_plot]
            if df_window_plot is None or df_window_plot.empty:
                continue

            # Plot PRE-candle grids (orders active during the visible candle interval).
            # Prefer visualization grids (full ladders) so the movie shows all grid steps.
            pending_entries = list(
                fr.get("viz_entries")
                or fr.get("pending_entries")
                or fr.get("viz_entries_post")
                or fr.get("pending_entries_post")
                or []
            )
            pending_closes = list(
                fr.get("viz_closes")
                or fr.get("pending_closes")
                or fr.get("viz_closes_post")
                or fr.get("pending_closes_post")
                or []
            )

            entry_prices = [float(o.get("price")) for o in pending_entries if float(o.get("price", 0.0) or 0.0) > 0.0]
            close_prices_all = [float(o.get("price")) for o in pending_closes if float(o.get("price", 0.0) or 0.0) > 0.0]

            # Next trailing highlight (if any)
            try:
                close_px = float(fr.get("candle", {}).get("close", df_window_plot["close"].iloc[-1]))
            except Exception:
                close_px = float(df_window_plot["close"].iloc[-1])

            trailing_entry_prices: list[float] = []
            for o in pending_entries:
                ot = str(o.get("order_type") or o.get("type") or "")
                if "trail" in ot.lower():
                    try:
                        trailing_entry_prices.append(float(o.get("price")))
                    except Exception:
                        pass
            next_trailing_entry_price = None
            if trailing_entry_prices:
                next_trailing_entry_price = min(trailing_entry_prices, key=lambda p: abs(float(p) - float(close_px)))

            trailing_close_prices: list[float] = []
            for o in pending_closes:
                ot = str(o.get("order_type") or o.get("type") or "")
                if "trail" in ot.lower():
                    try:
                        trailing_close_prices.append(float(o.get("price")))
                    except Exception:
                        pass

            # Apply trailing-close gating to the Close Grid trace as well (not only the highlight line).
            # This is the behavior the user expects: trailing closes should not be shown while price is on the
            # "wrong" side and hasn't yet crossed in the fill direction.
            gated_trailing_close_prices: list[float] = []
            if trailing_close_prices:
                try:
                    # Cache per-level within this frame
                    _dir_cache: dict[float, Optional[str]] = {}
                    for p in trailing_close_prices:
                        lvl = float(p)
                        if lvl not in _dir_cache:
                            _dir_cache[lvl] = _last_cross_direction_in_window(df_window_plot, lvl)
                        last_dir = _dir_cache[lvl]
                        if side_obj == Side.Long:
                            if last_dir == "up":
                                gated_trailing_close_prices.append(lvl)
                        else:
                            if last_dir == "down":
                                gated_trailing_close_prices.append(lvl)
                except Exception:
                    gated_trailing_close_prices = list(trailing_close_prices)

            non_trailing_close_prices = []
            try:
                trailing_set = set(float(x) for x in trailing_close_prices)
                for p in close_prices_all:
                    if float(p) not in trailing_set:
                        non_trailing_close_prices.append(float(p))
            except Exception:
                non_trailing_close_prices = list(close_prices_all)

            close_prices = non_trailing_close_prices + gated_trailing_close_prices
            next_trailing_close_price = None
            if trailing_close_prices:
                # Side-aware: for Long pick the lowest close >= px; for Short pick the highest close <= px.
                if side_obj == Side.Long:
                    above = [p for p in trailing_close_prices if float(p) >= float(close_px) - float(price_eps)]
                    next_trailing_close_price = min(above) if above else min(trailing_close_prices, key=lambda p: abs(float(p) - float(close_px)))
                else:
                    below = [p for p in trailing_close_prices if float(p) <= float(close_px) + float(price_eps)]
                    next_trailing_close_price = max(below) if below else min(trailing_close_prices, key=lambda p: abs(float(p) - float(close_px)))

            def _last_cross_direction_in_window(df_ohlc: pd.DataFrame, level: float) -> Optional[str]:
                """Return 'up' or 'down' for the most recent cross of `level`.

                Uses previous candle close vs current candle high/low, so intra-candle crosses are captured
                even if the close doesn't cross the level.
                """
                try:
                    if df_ohlc is None or df_ohlc.empty:
                        return None
                    if not all(c in df_ohlc.columns for c in ("close", "high", "low")):
                        return None

                    close_s = pd.to_numeric(df_ohlc["close"], errors="coerce").astype(float)
                    high_s = pd.to_numeric(df_ohlc["high"], errors="coerce").astype(float)
                    low_s = pd.to_numeric(df_ohlc["low"], errors="coerce").astype(float)

                    # Match Mode B fill semantics: float32-backed candles and strict inequality.
                    close_s = close_s.astype(np.float32)
                    high_s = high_s.astype(np.float32)
                    low_s = low_s.astype(np.float32)
                    prev_close = close_s.shift(1)
                    lvl = np.float32(float(level))

                    cross_up = (prev_close < lvl) & (high_s > lvl)
                    cross_dn = (prev_close > lvl) & (low_s < lvl)
                    if not (bool(cross_up.any()) or bool(cross_dn.any())):
                        return None
                    last_up = cross_up[cross_up].index.max() if bool(cross_up.any()) else None
                    last_dn = cross_dn[cross_dn].index.max() if bool(cross_dn.any()) else None
                    if last_up is None:
                        return "down"
                    if last_dn is None:
                        return "up"
                    return "up" if pd.to_datetime(last_up) >= pd.to_datetime(last_dn) else "down"
                except Exception:
                    return None

            # If the highlighted close (from actual pending orders) would have triggered on this candle, hide it.
            try:
                ch = float(fr.get("candle", {}).get("high"))
                cl = float(fr.get("candle", {}).get("low"))
            except Exception:
                ch, cl = None, None
            if next_trailing_close_price is not None and ch is not None and cl is not None:
                try:
                    if side_obj == Side.Long:
                        if float(ch) >= float(next_trailing_close_price) - float(price_eps):
                            next_trailing_close_price = None
                    else:
                        if float(cl) <= float(next_trailing_close_price) + float(price_eps):
                            next_trailing_close_price = None
                except Exception:
                    pass

            # Movie UX: show trailing closes only after the most recent price-cross of that level in the fill direction.
            # Example (Long close): if price was above, crossed down, then later crosses up again -> show from that up-cross.
            if next_trailing_close_price is not None:
                try:
                    last_dir = _last_cross_direction_in_window(df_window_plot, float(next_trailing_close_price))
                    if side_obj == Side.Long:
                        # Require an up-cross; hide if last cross is down or there has been no cross yet.
                        if last_dir != "up":
                            next_trailing_close_price = None
                    else:
                        # Require a down-cross; hide if last cross is up or there has been no cross yet.
                        if last_dir != "down":
                            next_trailing_close_price = None
                except Exception:
                    pass

            x_left = df_window_plot.index[0]
            try:
                if int(opt_res_mins) > 1:
                    x_left = x_left - pd.Timedelta(minutes=int(opt_res_mins))
            except Exception:
                pass
            x_right = df_window_plot.index[-1]

            trace_entries = make_grid_trace(entry_prices, "rgba(255, 0, 0, 0.6)", "Entry Grid", x_left, x_right)
            trace_closes = make_grid_trace(close_prices, "rgba(0, 255, 0, 0.6)", "Close Grid", x_left, x_right)

            # Current price line: use the *plotted* candle close.
            # The movie candles may be resampled (e.g. 5m/4h); using the 1m replay close would not match.
            try:
                current_price_plot = float(df_window_plot["close"].iloc[-1])
            except Exception:
                current_price_plot = float(close_px)

            trace_current_price = go.Scatter(
                x=[x_left, x_right],
                y=[float(current_price_plot), float(current_price_plot)],
                mode="lines",
                line=dict(width=2),
                name="Current Price",
                showlegend=True,
            )

            trace_next_trailing_entry = go.Scatter(x=[], y=[])
            if next_trailing_entry_price is not None:
                trace_next_trailing_entry = make_grid_trace(
                    [float(next_trailing_entry_price)],
                    "rgba(255, 165, 0, 0.9)",
                    "Next Trailing Entry",
                    x_left,
                    x_right,
                    width=3,
                )

            trace_next_trailing_close = go.Scatter(x=[], y=[])
            if next_trailing_close_price is not None:
                trace_next_trailing_close = make_grid_trace(
                    [float(next_trailing_close_price)],
                    "rgba(0, 255, 255, 0.9)",
                    "Next Trailing Close",
                    x_left,
                    x_right,
                    width=3,
                )

            # Executed fills up to current_time within the viewport.
            # Important: the movie candle series may be resampled (e.g. 5m/4h), so we snap fill timestamps
            # to the displayed candle bins to avoid markers appearing between candles.
            # Hover should still show the *real* fill timestamp/price, not the snapped/clamped plot coords.
            fill_x: list = []
            fill_y: list[float] = []
            fill_text: list[str] = []
            fill_color: list[str] = []
            fill_custom: list[list] = []
            try:
                if not sim_events_df.empty:
                    x0 = pd.to_datetime(df_window_plot.index[0])
                    x1 = pd.to_datetime(upper_time)
                    # Include one extra candle-bin on the left so fills that snap into the first
                    # visible candle (but happened slightly earlier) are still shown.
                    try:
                        x0_filter = x0 - pd.Timedelta(minutes=int(opt_res_mins))
                    except Exception:
                        x0_filter = x0
                    exec_df = sim_events_df[(sim_events_df["timestamp"] >= x0_filter) & (sim_events_df["timestamp"] <= x1)].copy()
                    # Keep payload bounded
                    if len(exec_df) > 3000:
                        exec_df = exec_df.iloc[-3000:]

                    # Resampled candles are right-labeled (bin end). Snap fills into the corresponding bin.
                    align_method = "backfill" if int(opt_res_mins) > 1 else "pad"
                    # If multiple fills snap to the same plotted candle bin, stack markers slightly so earlier
                    # ones don't get hidden under later ones (common with 4h candles).
                    stack_counts: dict[tuple, int] = {}

                    def _stack_step(n: int) -> int:
                        # 0, +1, -1, +2, -2, ...
                        if n <= 0:
                            return 0
                        k = (n + 1) // 2
                        return k if (n % 2) == 1 else -k

                    # Jitter within candle bin to show markers horizontally side-by-side.
                    # Use a small fraction of the plotted candle width.
                    try:
                        _bin_secs = max(1.0, float(opt_res_mins) * 60.0)
                    except Exception:
                        _bin_secs = 60.0
                    _jitter_secs = max(1.0, _bin_secs * 0.06)
                    for _, r in exec_df.iterrows():
                        try:
                            ts_fill = pd.to_datetime(r.get("timestamp"))
                            px = float(r.get("price") or 0.0)
                            qty = float(r.get("qty") or 0.0)
                        except Exception:
                            continue
                        if px <= 0.0 or qty == 0.0 or (not math.isfinite(px)) or (not math.isfinite(qty)):
                            continue

                        # Snap X to the plotted candle index and clamp Y into candle [low, high]
                        x_plot = ts_fill
                        y_plot = float(px)
                        lo = None
                        hi = None
                        try:
                            ii = int(df_window_plot.index.get_indexer([ts_fill], method=align_method)[0])
                            if ii < 0:
                                ii = int(df_window_plot.index.get_indexer([ts_fill], method="nearest")[0])
                            if 0 <= ii < len(df_window_plot):
                                x_plot = df_window_plot.index[ii]
                                lo = float(df_window_plot.iloc[ii].get("low", y_plot) or y_plot)
                                hi = float(df_window_plot.iloc[ii].get("high", y_plot) or y_plot)
                                if math.isfinite(lo) and math.isfinite(hi):
                                    if lo > hi:
                                        lo, hi = hi, lo
                                    y_plot = float(min(max(y_plot, lo), hi))
                        except Exception:
                            pass

                        is_buy = float(qty) > 0.0

                        # Jitter markers horizontally within the candle bin.
                        # Always separate buys vs sells (buy left, sell right), then stack within each side.
                        try:
                            x_bin = pd.to_datetime(x_plot)
                            key = (x_bin, bool(is_buy))
                            n = int(stack_counts.get(key, 0))
                            stack_counts[key] = n + 1
                            off = int(_stack_step(n))
                            base = (-0.5 if is_buy else 0.5)
                            total_off = float(base) + float(off)
                            if total_off != 0.0:
                                xj = x_bin + pd.Timedelta(seconds=int(total_off * _jitter_secs))
                                # Keep within the candle bin so it still visually belongs to this candle.
                                if align_method == "backfill":
                                    bin_start = x_bin - pd.Timedelta(minutes=int(opt_res_mins))
                                    bin_end = x_bin
                                else:
                                    bin_start = x_bin
                                    bin_end = x_bin + pd.Timedelta(minutes=int(opt_res_mins))
                                eps = pd.Timedelta(seconds=1)
                                if xj < (bin_start + eps):
                                    xj = bin_start + eps
                                if xj > (bin_end - eps):
                                    xj = bin_end - eps
                                x_plot = xj
                        except Exception:
                            pass

                        fill_x.append(x_plot)
                        fill_y.append(y_plot)
                        fill_text.append("B" if is_buy else "S")
                        fill_color.append("rgba(0, 200, 0, 1.0)" if is_buy else "rgba(220, 0, 0, 1.0)")
                        try:
                            ord_idx = int(r.get("ord_idx") or 0)
                        except Exception:
                            ord_idx = 0
                        fill_custom.append([
                            ord_idx,
                            abs(float(qty)),
                            str(r.get("event", "") or ""),
                            str(r.get("order_type", "") or ""),
                            str(ts_fill),
                            float(px),
                        ])
            except Exception:
                pass

            trace_fills = go.Scatter(
                x=fill_x,
                y=fill_y,
                mode="markers+text",
                name="Fills (B/S)",
                showlegend=True,
                text=fill_text,
                textposition="middle center",
                textfont=dict(color="white", size=12),
                marker=dict(
                    symbol="circle",
                    size=18,
                    color=fill_color,
                    line=dict(color="rgba(0, 0, 0, 0.7)", width=1),
                ),
                customdata=fill_custom,
                hovertemplate=(
                    "%{text} #%{customdata[0]} (%{customdata[2]})<br>"
                    "qty=%{customdata[1]:.6f}<br>"
                    "price=%{customdata[5]:.6f}<br>"
                    "type=%{customdata[3]}<br>"
                    "%{customdata[4]}<extra></extra>"
                ),
            )

            cols = [c for c in ["ema_0", "ema_1", "ema_2"] if c in df_window_plot.columns]
            if cols:
                upper_band = df_window_plot[cols].max(axis=1)
                lower_band = df_window_plot[cols].min(axis=1)
            else:
                upper_band = df_window_plot["high"]
                lower_band = df_window_plot["low"]

            # Keep heavy traces (candles + EMAs) static to avoid huge websocket payloads.
            # Frames only update lightweight traces: grids + trailing highlights + fills.
            frame_data = [
                trace_entries,
                trace_closes,
                trace_current_price,
                trace_next_trailing_entry,
                trace_next_trailing_close,
                trace_fills,
            ]

            y_vals: list[float] = []
            try:
                y_vals.extend([float(df_window_plot["high"].max()), float(df_window_plot["low"].min())])
                y_vals.extend([float(upper_band.max()), float(lower_band.min())])
            except Exception:
                pass
            y_vals.extend([float(p) for p in entry_prices])
            y_vals.extend([float(p) for p in close_prices])
            y_vals.append(float(current_price_plot))
            if next_trailing_entry_price is not None:
                y_vals.append(float(next_trailing_entry_price))
            if next_trailing_close_price is not None:
                y_vals.append(float(next_trailing_close_price))
            y_vals.extend([float(p) for p in fill_y if p is not None])
            y_vals = [y for y in y_vals if y is not None and not np.isnan(y)]
            if not y_vals:
                y_min_frame, y_max_frame = 0.0, 1.0
            else:
                y_min_frame = min(y_vals) * 0.995
                y_max_frame = max(y_vals) * 1.005

            bal_now = None
            try:
                if fr.get("balance_after") is not None:
                    bal_now = float(fr.get("balance_after"))
                elif fr.get("balance_before") is not None:
                    bal_now = float(fr.get("balance_before"))
            except Exception:
                bal_now = None
            bal_text = ""
            if bal_now is not None and math.isfinite(float(bal_now)):
                bal_text = f"Wallet: {float(bal_now):.2f}"

            ann = []
            if bal_text:
                ann = [
                    dict(
                        x=1.02,
                        y=0.0,
                        xref="paper",
                        yref="paper",
                        xanchor="left",
                        yanchor="bottom",
                        text=bal_text,
                        showarrow=False,
                        align="left",
                        bgcolor="rgba(0,0,0,0.5)",
                        font=dict(size=12),
                    )
                ]

            frame_layout = dict(
                xaxis=dict(range=[x_left, x_right]),
                yaxis=dict(range=[y_min_frame, y_max_frame]),
                annotations=ann,
            )
            fig_frames.append(go.Frame(data=frame_data, layout=frame_layout, name=str(i), traces=[3, 4, 5, 6, 7, 8]))

        if not fig_frames:
            st.error("No frames generated")
            return

        try:
            prog_bar.empty()
        except Exception:
            pass

        try:
            stage_line.empty()
        except Exception:
            pass

        try:
            initial_frame_idx = int(initial_frame_idx)
        except Exception:
            initial_frame_idx = 0
        if initial_frame_idx < 0:
            initial_frame_idx = 0
        if initial_frame_idx >= len(fig_frames):
            initial_frame_idx = len(fig_frames) - 1

        init_frame = fig_frames[initial_frame_idx]

        # Static plot source (candles + EMAs): match the requested simulation span.
        # Start at start_time so the first visible candles appear one-by-one.
        df_static = df_plot_source.loc[(df_plot_source.index >= start_time) & (df_plot_source.index <= end_time)]
        if df_static is None or df_static.empty:
            df_static = df_plot_source

        trace_candle_static = go.Candlestick(
            x=df_static.index,
            open=df_static["open"],
            high=df_static["high"],
            low=df_static["low"],
            close=df_static["close"],
            name="Price",
        )

        cols_static = [c for c in ["ema_0", "ema_1", "ema_2"] if c in df_static.columns]
        if cols_static:
            upper_band_static = df_static[cols_static].max(axis=1)
            lower_band_static = df_static[cols_static].min(axis=1)
        else:
            upper_band_static = df_static["high"]
            lower_band_static = df_static["low"]

        trace_ema_high_static = go.Scatter(
            x=df_static.index,
            y=upper_band_static,
            mode="lines",
            line=dict(color="magenta", width=1, dash="solid"),
            name="EMA High",
        )
        trace_ema_low_static = go.Scatter(
            x=df_static.index,
            y=lower_band_static,
            mode="lines",
            line=dict(color="cyan", width=1, dash="dot"),
            name="EMA Low",
        )

        # Dynamic traces (initialized from the selected initial frame)
        init_dynamic = list(init_frame.data) if init_frame.data is not None else []
        while len(init_dynamic) < 6:
            init_dynamic.append(go.Scatter(x=[], y=[]))

        init_data = [
            trace_candle_static,
            trace_ema_high_static,
            trace_ema_low_static,
            init_dynamic[0],
            init_dynamic[1],
            init_dynamic[2],
            init_dynamic[3],
            init_dynamic[4],
            init_dynamic[5],
        ]

        # Playback speed: UI timing only (do not scale with candle timeframe).
        play_duration_ms = 120

        layout_args = dict(
            title=f"Animation (Mode B): {symbol} ({start_time} - {end_time}) | Frames: {len(fig_frames)}",
            xaxis=dict(type="date", rangeslider=dict(visible=False)),
            yaxis=dict(autorange=False, fixedrange=False),
            height=800,
            margin=dict(l=50, r=260, t=70, b=170),
            legend=dict(x=1.02, y=1, xanchor="left", yanchor="top", bgcolor="rgba(0,0,0,0.5)"),
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    showactive=False,
                    x=0.01,
                    y=-0.06,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    buttons=[
                        dict(
                            label="▶ Play",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=play_duration_ms, redraw=False),
                                    fromcurrent=True,
                                    mode="immediate",
                                    transition=dict(duration=0, easing="linear"),
                                ),
                            ],
                        ),
                        dict(
                            label="🐢 Slow",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=int(play_duration_ms * 2), redraw=False),
                                    fromcurrent=True,
                                    mode="immediate",
                                    transition=dict(duration=0, easing="linear"),
                                ),
                            ],
                        ),
                        dict(
                            label="🐌 Very Slow",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=int(play_duration_ms * 4), redraw=False),
                                    fromcurrent=True,
                                    mode="immediate",
                                    transition=dict(duration=0, easing="linear"),
                                ),
                            ],
                        ),
                        dict(
                            label="⏸ Pause",
                            method="animate",
                            args=[[None], dict(frame=dict(duration=0, redraw=False), mode="immediate", transition=dict(duration=0))],
                        ),
                    ],
                )
            ],
            sliders=[
                dict(
                    active=int(initial_frame_idx),
                    steps=[
                        dict(
                            method="animate",
                            args=[[str(k)], dict(mode="immediate", frame=dict(duration=0, redraw=False), transition=dict(duration=0))],
                            label=f"{k}",
                        )
                        for k in range(len(fig_frames))
                    ],
                    transition=dict(duration=0),
                    x=0.01,
                    y=-0.15,
                    currentvalue=dict(font=dict(size=12), prefix="Frame: ", visible=True, xanchor="left"),
                    len=0.99,
                    pad={"b": 10, "t": 10},
                )
            ],
        )

        if init_frame.layout:
            if "xaxis" in init_frame.layout and "range" in init_frame.layout["xaxis"]:
                layout_args["xaxis"]["range"] = init_frame.layout["xaxis"]["range"]
            if "yaxis" in init_frame.layout and "range" in init_frame.layout["yaxis"]:
                layout_args["yaxis"]["range"] = init_frame.layout["yaxis"]["range"]
            try:
                if "annotations" in init_frame.layout:
                    layout_args["annotations"] = init_frame.layout["annotations"]
            except Exception:
                pass

        fig = go.Figure(data=init_data, layout=go.Layout(**layout_args), frames=fig_frames)
        try:
            st.session_state["gv_movie_fig_modeb"] = fig
            st.session_state["gv_movie_meta_modeb"] = {
                "start_time": str(start_time),
                "frames": int(frames),
                "step_mins": int(step_mins),
                "exchange": str(exchange),
                "symbol": str(symbol),
                "context_days": float(context_days),
                "side_val": int(side_val),
            }
        except Exception:
            pass
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

        # Fills table under the chart
        try:
            if sim_events_df is not None and not sim_events_df.empty:
                df_fills = sim_events_df.copy()
                try:
                    df_fills = df_fills[(df_fills["timestamp"] >= pd.to_datetime(start_time)) & (df_fills["timestamp"] <= pd.to_datetime(end_time))]
                except Exception:
                    pass
                cols = [
                    c
                    for c in [
                        "ord_idx",
                        "timestamp",
                        "event",
                        "qty",
                        "price",
                        "order_type",
                        "fee_paid",
                        "wallet_balance",
                        "pos_size",
                    ]
                    if c in df_fills.columns
                ]
                if cols:
                    df_fills = df_fills[cols]
                if len(df_fills) > 5000:
                    df_fills = df_fills.iloc[-5000:]
                try:
                    st.session_state["gv_movie_fills_modeb"] = df_fills
                except Exception:
                    pass
                st.dataframe(df_fills, use_container_width=True, height=250)
        except Exception:
            pass


def generate_animation_v7_modec(
    *,
    start_time: pd.Timestamp,
    frames: int,
    step_mins: int,
    hist_df: pd.DataFrame,
    exchange: str,
    symbol: str,
    context_days: float,
    side_val: int,
    data_template,
    output_container=None,
    initial_frame_idx: int = 0,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> None:
    """Movie/animation builder driven by Mode C (PB7 backtest engine).

    The PB7 engine returns fills (timestamp/price/qty/order_type/pos_size). It does not expose
    per-candle open orders like Mode B. To still make the movie useful, we preview the next
    upcoming fills (per position cycle) as horizontal lines and remove them once the timestamp
    is reached.
    """
    if output_container is None:
        output_container = st.container()

    with output_container:
        if hist_df is None or hist_df.empty:
            st.error("No historical candles loaded.")
            return

        try:
            price_step_stack = float(getattr(data_template.exchange_params, "price_step", 0.0) or 0.0)
        except Exception:
            price_step_stack = 0.0
        if (not math.isfinite(price_step_stack)) or price_step_stack < 0.0:
            price_step_stack = 0.0

        try:
            pbr = _get_passivbot_rust(_pb7_src_dir())
        except Exception:
            pbr = None
        if pbr is None:
            st.error("Passivbot Rust bindings not found!")
            return

        frames = int(frames or 0)
        step_mins = int(step_mins or 0)
        if frames <= 0 or step_mins <= 0:
            st.error("Invalid animation parameters.")
            return

        start_time = pd.to_datetime(start_time)
        end_time = start_time + datetime.timedelta(minutes=frames * step_mins)
        ctx_days = float(context_days or 0.0)
        if ctx_days <= 0:
            ctx_days = 5.0
        if int(step_mins) == 240:
            ctx_days = max(ctx_days, 10.0)
        warm_start = start_time - datetime.timedelta(days=ctx_days)

        # Plot candles from (warm_start..end_time)
        sim_df = hist_df.loc[(hist_df.index >= warm_start) & (hist_df.index <= end_time)].copy()
        if sim_df.empty:
            st.error("No candles for the selected time window.")
            return

        # Indicator series for plotting
        bp = data_template.normal_bot_params_long if int(side_val) == int(Side.Long.value) else data_template.normal_bot_params_short
        side_obj = Side.Long if int(side_val) == int(Side.Long.value) else Side.Short

        df_calc = calculate_v7_indicators(
            sim_df,
            float(bp.ema_span_0),
            float(bp.ema_span_1),
            float(bp.entry_volatility_ema_span_hours),
        )

        total_ctx_mins = float(ctx_days) * 1440.0
        opt_res_mins = int(total_ctx_mins / 300.0)
        if opt_res_mins < 1:
            opt_res_mins = 1
        # Special case: for 4h stepping we want true 4h candles (240x1m -> 1 candle) aligned to start_time.
        if int(step_mins) == 240:
            opt_res_mins = 240
        else:
            try:
                opt_res_mins = min(int(opt_res_mins), int(max(1, step_mins)))
            except Exception:
                pass

        # Visible window: for 4h playback show 60 candles (10 days). Otherwise keep 4h cap.
        if int(step_mins) == 240:
            window_mins = int(60 * int(opt_res_mins))
        else:
            max_visible_window_mins = 240
            window_mins = int(min(float(total_ctx_mins), float(max_visible_window_mins), float(max(int(step_mins) * 5, max_visible_window_mins))))

        if opt_res_mins > 1:
            agg_dict = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
            for c in df_calc.columns:
                if c not in agg_dict:
                    agg_dict[c] = "last"
            rs_kwargs = {}
            if int(opt_res_mins) == 240 and int(step_mins) == 240:
                rs_kwargs = {"origin": pd.to_datetime(start_time), "label": "right", "closed": "right"}
            df_plot_source = df_calc.resample(f"{opt_res_mins}min", **rs_kwargs).agg(agg_dict).dropna()
        else:
            df_plot_source = df_calc

        stage_line = st.empty()
        prog_bar = st.progress(0.0)

        def _progress(frac: float, msg: str) -> None:
            f = float(frac)
            if not math.isfinite(f):
                f = 0.0
            f = max(0.0, min(1.0, f))
            try:
                stage_line.caption(str(msg))
            except Exception:
                pass
            try:
                prog_bar.progress(f)
            except Exception:
                pass
            if progress_cb is not None:
                try:
                    progress_cb(f, str(msg))
                except Exception:
                    pass

        # Run PB7 engine for fills
        _progress(0.05, "Mode C: running PB7 engine...")

        def _disable_bot_params(bp_in: BotParams) -> BotParams:
            d = asdict(bp_in)
            d["total_wallet_exposure_limit"] = 0.0
            if "n_positions" in d:
                d["n_positions"] = 0
            return BotParams(**d)

        if side_obj == Side.Long:
            bp_long = bp
            bp_short = _disable_bot_params(data_template.normal_bot_params_short)
        else:
            bp_long = _disable_bot_params(data_template.normal_bot_params_long)
            bp_short = bp

        try:
            warmup_minutes_override = int(_compute_warmup_minutes_for_mode_c(bp_long, bp_short))
        except Exception:
            warmup_minutes_override = None

        try:
            c_long, c_short = _run_pb7_engine_backtest_for_visualizer(
                pbr=pbr,
                exchange=str(exchange),
                coin=str(symbol),
                analysis_time=pd.to_datetime(start_time).to_pydatetime(),
                hist_df=hist_df,
                exchange_params=data_template.exchange_params,
                bot_params_long=bp_long,
                bot_params_short=bp_short,
                starting_balance=float(data_template.state_params.balance),
                max_candles_forward=int(frames * step_mins + 10),
                config=None,
                warmup_minutes_override=warmup_minutes_override,
            )
        except Exception as e:
            st.error(f"Mode C backtest engine failed: {e}")
            return

        _progress(0.25, "Mode C: processing fills...")

        events = c_long if side_obj == Side.Long else c_short
        fills_df = pd.DataFrame(events or [])
        if not fills_df.empty and "timestamp" in fills_df.columns:
            fills_df["timestamp"] = pd.to_datetime(fills_df["timestamp"])
        else:
            fills_df = pd.DataFrame(columns=["timestamp", "event", "price", "qty", "order_type", "pos_size"])

        # Focus fills to the movie range (we preview only within the movie span)
        try:
            fills_df = fills_df[(fills_df["timestamp"] >= start_time) & (fills_df["timestamp"] <= end_time)].copy()
        except Exception:
            pass

        try:
            fills_df = fills_df.sort_values("timestamp").reset_index(drop=True)
            fills_df["ord_idx"] = np.arange(0, len(fills_df), dtype=int)
        except Exception:
            pass

        # Precompute fill cycles (position opens from 0 -> nonzero; cycle ends when pos returns to 0)
        # Mode C preview is based on fills only. To keep previews readable (and payload small),
        # we filter fills into a monotone envelope:
        # - Long closes: keep only strictly higher prices over time
        # - Long entries: keep only strictly lower prices over time
        # (Short is the opposite)
        # Then apply a hard safety cap.
        preview_max_lines = 200
        pos_eps = 1e-12
        try:
            ts_arr = pd.to_datetime(fills_df["timestamp"]).to_numpy(dtype="datetime64[ns]")
            pos_arr = fills_df.get("pos_size", pd.Series([0.0] * len(fills_df))).astype(float).to_numpy()
        except Exception:
            ts_arr = np.array([], dtype="datetime64[ns]")
            pos_arr = np.array([], dtype=float)

        cycle_id = np.full((len(fills_df),), -1, dtype=int)
        cycles: list[tuple[int, int]] = []
        in_cycle = False
        cur_cid = -1
        cur_start = -1
        for idx_ev in range(len(fills_df)):
            ps = float(pos_arr[idx_ev]) if idx_ev < len(pos_arr) else 0.0
            if (not in_cycle) and abs(ps) > pos_eps:
                in_cycle = True
                cur_cid += 1
                cur_start = idx_ev
            if in_cycle:
                cycle_id[idx_ev] = cur_cid
                if abs(ps) <= pos_eps:
                    cycles.append((cur_start, idx_ev))
                    in_cycle = False
                    cur_start = -1
        if in_cycle and cur_start >= 0:
            cycles.append((cur_start, len(fills_df) - 1))

        cycle_start = {}
        cycle_end = {}
        for cid, (s_i, e_i) in enumerate(cycles):
            cycle_start[int(cid)] = int(s_i)
            cycle_end[int(cid)] = int(e_i)

        def _make_fill_lines(idxs: list[int], *, kind: str, x0, x1):
            # kind: 'entry' or 'close'
            if not idxs:
                return go.Scatter(x=[], y=[])
            x_vals: list = []
            y_vals: list = []
            texts: list = []
            for ii in idxs:
                try:
                    r = fills_df.iloc[int(ii)]
                    ts = pd.to_datetime(r.get("timestamp"))
                    price = float(r.get("price") or 0.0)
                    if price <= 0.0:
                        continue
                    qty = float(r.get("qty") or 0.0)
                    ot = str(r.get("order_type") or "")
                    text = f"{ts} | {kind} | {ot} | qty {qty} | px {price}"
                except Exception:
                    continue
                x_vals.extend([x0, x1, None])
                y_vals.extend([price, price, None])
                texts.extend([text, text, None])

            color = "rgba(255, 0, 0, 0.75)" if kind == "entry" else "rgba(0, 255, 0, 0.75)"
            name = "Upcoming Entries" if kind == "entry" else "Upcoming Closes"
            return go.Scatter(
                x=x_vals,
                y=y_vals,
                text=texts,
                mode="lines",
                line=dict(color=color, width=2, dash="dot"),
                name=name,
                hovertemplate="%{text}<extra></extra>",
            )

        def _monotone_filter_idxs(idxs: list[int], *, kind: str) -> list[int]:
            """Filter indices to a monotone price envelope.

            For long:
              - entries: keep new lows only
              - closes:  keep new highs only
            For short:
              - entries: keep new highs only
              - closes:  keep new lows only
            """
            if not idxs:
                return []

            keep_new_high = False
            keep_new_low = False
            if side_obj == Side.Long:
                keep_new_high = kind == "close"
                keep_new_low = kind == "entry"
            else:
                keep_new_high = kind == "entry"
                keep_new_low = kind == "close"

            out: list[int] = []
            best = None
            for ii in idxs:
                try:
                    price = float(fills_df.iloc[int(ii)].get("price") or 0.0)
                except Exception:
                    continue
                if price <= 0.0 or not math.isfinite(price):
                    continue
                if best is None:
                    best = price
                    out.append(int(ii))
                    continue
                if keep_new_high and price > float(best):
                    best = price
                    out.append(int(ii))
                elif keep_new_low and price < float(best):
                    best = price
                    out.append(int(ii))
            return out

        fig_frames: list = []
        _progress(0.3, "Mode C: building frames...")
        frame_progress_every = max(1, int(int(frames) // 200))
        for i in range(int(frames)):
            if (i == 0) or (i == int(frames) - 1) or (i % frame_progress_every == 0):
                _progress(0.3 + 0.7 * float(i + 1) / float(max(1, int(frames))), f"Mode C: building frames {i + 1}/{int(frames)}")
            current_time = start_time + datetime.timedelta(minutes=int(i) * int(step_mins))

            ctx_start = current_time - datetime.timedelta(minutes=int(window_mins))
            if ctx_start < start_time:
                ctx_start = start_time

            upper_time = current_time
            try:
                if not df_plot_source.empty and upper_time > df_plot_source.index.max():
                    upper_time = df_plot_source.index.max()
            except Exception:
                pass

            mask_plot = (df_plot_source.index > ctx_start) & (df_plot_source.index <= upper_time)
            df_window_plot = df_plot_source.loc[mask_plot]
            if df_window_plot is None or df_window_plot.empty:
                continue

            # Determine which upcoming fills to preview at this time.
            upcoming_entry_idxs: list[int] = []
            upcoming_close_idxs: list[int] = []
            if len(fills_df) > 0 and len(ts_arr) == len(fills_df):
                try:
                    ct64 = np.datetime64(pd.to_datetime(current_time).to_datetime64())
                    k = int(np.searchsorted(ts_arr, ct64, side="right") - 1)
                    next_idx = int(k + 1)
                except Exception:
                    k, next_idx = -1, 0

                cur_pos = float(pos_arr[k]) if k >= 0 and k < len(pos_arr) else 0.0

                def _find_next_cycle_start(j: int) -> int:
                    jj = max(0, int(j))
                    while jj < len(fills_df):
                        if int(cycle_id[jj]) >= 0:
                            return jj
                        jj += 1
                    return len(fills_df)

                if abs(cur_pos) <= pos_eps:
                    j0 = _find_next_cycle_start(next_idx)
                    if j0 < len(fills_df):
                        cid = int(cycle_id[j0])
                        end_i = int(cycle_end.get(cid, len(fills_df) - 1))
                        cand = list(range(int(j0), int(end_i) + 1))
                    else:
                        cand = []
                else:
                    cid = int(cycle_id[k]) if k >= 0 else -1
                    if cid < 0 and next_idx < len(fills_df):
                        cid = int(cycle_id[next_idx])
                    if cid >= 0:
                        end_i = int(cycle_end.get(cid, len(fills_df) - 1))
                        j0 = max(int(next_idx), int(cycle_start.get(cid, next_idx)))
                        cand = list(range(int(j0), int(end_i) + 1))
                    else:
                        cand = []

                # When flat (no open position), preview only upcoming entries.
                # Closes are not meaningful until the first entry fill has happened.
                only_entries = abs(cur_pos) <= pos_eps

                for ii in cand:
                    try:
                        ev = str(fills_df.iloc[int(ii)].get("event") or "")
                    except Exception:
                        ev = ""
                    if ev == "entry":
                        upcoming_entry_idxs.append(int(ii))
                    elif (not only_entries) and ev == "close":
                        upcoming_close_idxs.append(int(ii))

                # Apply monotone envelope filtering to keep previews compact
                upcoming_entry_idxs = _monotone_filter_idxs(upcoming_entry_idxs, kind="entry")
                upcoming_close_idxs = _monotone_filter_idxs(upcoming_close_idxs, kind="close")

                # Safety cap (post-filter) to avoid huge Plotly frame payloads
                try:
                    cap = int(preview_max_lines)
                except Exception:
                    cap = 200
                if cap > 0:
                    upcoming_entry_idxs = upcoming_entry_idxs[:cap]
                    upcoming_close_idxs = upcoming_close_idxs[:cap]

            trace_up_entries = _make_fill_lines(upcoming_entry_idxs, kind="entry", x0=df_window_plot.index[0], x1=df_window_plot.index[-1])
            trace_up_closes = _make_fill_lines(upcoming_close_idxs, kind="close", x0=df_window_plot.index[0], x1=df_window_plot.index[-1])

            cols = [c for c in ["ema_0", "ema_1", "ema_2"] if c in df_window_plot.columns]
            if cols:
                upper_band = df_window_plot[cols].max(axis=1)
                lower_band = df_window_plot[cols].min(axis=1)
            else:
                upper_band = df_window_plot["high"]
                lower_band = df_window_plot["low"]

            y_vals: list[float] = []
            try:
                y_vals.extend([float(df_window_plot["high"].max()), float(df_window_plot["low"].min())])
                y_vals.extend([float(upper_band.max()), float(lower_band.min())])
            except Exception:
                pass
            try:
                for ii in (upcoming_entry_idxs + upcoming_close_idxs):
                    y_vals.append(float(fills_df.iloc[int(ii)].get("price") or 0.0))
            except Exception:
                pass
            y_vals = [y for y in y_vals if y is not None and not np.isnan(y)]
            if not y_vals:
                y_min_frame, y_max_frame = 0.0, 1.0
            else:
                y_min_frame = min(y_vals) * 0.995
                y_max_frame = max(y_vals) * 1.005

            frame_layout = dict(
                xaxis=dict(
                    range=[
                        (df_window_plot.index[0] - pd.Timedelta(minutes=int(opt_res_mins)))
                        if (int(step_mins) == 240 and int(opt_res_mins) == 240)
                        else df_window_plot.index[0],
                        df_window_plot.index[-1],
                    ]
                ),
                yaxis=dict(range=[y_min_frame, y_max_frame]),
            )
            # Executed fills up to current_time (B/S markers)
            # Snap to plotted candle bins to avoid X-misalignment when candle series is resampled.
            # Hover should still show the *real* fill timestamp/price, not the snapped/clamped plot coords.
            exec_x: list = []
            exec_y: list[float] = []
            exec_text: list[str] = []
            exec_color: list[str] = []
            exec_custom: list[list] = []
            try:
                if not fills_df.empty and "timestamp" in fills_df.columns:
                    x0 = pd.to_datetime(df_window_plot.index[0])
                    x1 = pd.to_datetime(current_time)
                    try:
                        x0_filter = x0 - pd.Timedelta(minutes=int(opt_res_mins))
                    except Exception:
                        x0_filter = x0
                    mask_exec = (fills_df["timestamp"] >= x0_filter) & (fills_df["timestamp"] <= x1)
                    exec_df = fills_df.loc[mask_exec]
                    # Keep payload bounded
                    if len(exec_df) > 5000:
                        exec_df = exec_df.iloc[-5000:]

                    align_method = "backfill" if (int(step_mins) == 240 and int(opt_res_mins) == 240) else "pad"
                    stack_counts: dict[tuple, int] = {}

                    def _stack_step(n: int) -> int:
                        if n <= 0:
                            return 0
                        k = (n + 1) // 2
                        return k if (n % 2) == 1 else -k

                    try:
                        _bin_secs = max(1.0, float(opt_res_mins) * 60.0)
                    except Exception:
                        _bin_secs = 60.0
                    _jitter_secs = max(1.0, _bin_secs * 0.06)
                    for _, rr in exec_df.iterrows():
                        try:
                            ts_fill = pd.to_datetime(rr.get("timestamp"))
                            px = float(rr.get("price") or 0.0)
                            qty = float(rr.get("qty") or 0.0)
                        except Exception:
                            continue
                        if px <= 0.0 or qty == 0.0 or (not math.isfinite(px)) or (not math.isfinite(qty)):
                            continue

                        x_plot = ts_fill
                        y_plot = float(px)
                        lo = None
                        hi = None
                        try:
                            ii = int(df_window_plot.index.get_indexer([ts_fill], method=align_method)[0])
                            if ii < 0:
                                ii = int(df_window_plot.index.get_indexer([ts_fill], method="nearest")[0])
                            if 0 <= ii < len(df_window_plot):
                                x_plot = df_window_plot.index[ii]
                                lo = float(df_window_plot.iloc[ii].get("low", y_plot) or y_plot)
                                hi = float(df_window_plot.iloc[ii].get("high", y_plot) or y_plot)
                                if math.isfinite(lo) and math.isfinite(hi):
                                    if lo > hi:
                                        lo, hi = hi, lo
                                    y_plot = float(min(max(y_plot, lo), hi))
                        except Exception:
                            pass

                        is_buy = float(qty) > 0.0

                        # Jitter markers horizontally within the candle bin.
                        # Always separate buys vs sells (buy left, sell right), then stack within each side.
                        try:
                            x_bin = pd.to_datetime(x_plot)
                            key = (x_bin, bool(is_buy))
                            n = int(stack_counts.get(key, 0))
                            stack_counts[key] = n + 1
                            off = int(_stack_step(n))
                            base = (-0.5 if is_buy else 0.5)
                            total_off = float(base) + float(off)
                            if total_off != 0.0:
                                xj = x_bin + pd.Timedelta(seconds=int(total_off * _jitter_secs))
                                if align_method == "backfill":
                                    bin_start = x_bin - pd.Timedelta(minutes=int(opt_res_mins))
                                    bin_end = x_bin
                                else:
                                    bin_start = x_bin
                                    bin_end = x_bin + pd.Timedelta(minutes=int(opt_res_mins))
                                eps = pd.Timedelta(seconds=1)
                                if xj < (bin_start + eps):
                                    xj = bin_start + eps
                                if xj > (bin_end - eps):
                                    xj = bin_end - eps
                                x_plot = xj
                        except Exception:
                            pass

                        exec_x.append(x_plot)
                        exec_y.append(y_plot)
                        exec_text.append("B" if is_buy else "S")
                        exec_color.append("rgba(0, 200, 0, 1.0)" if is_buy else "rgba(220, 0, 0, 1.0)")
                        try:
                            ord_idx = int(rr.get("ord_idx") or 0)
                        except Exception:
                            ord_idx = 0
                        exec_custom.append([
                            ord_idx,
                            abs(float(qty)),
                            str(rr.get("event", "") or ""),
                            str(rr.get("order_type", "") or ""),
                            str(ts_fill),
                            float(px),
                        ])
            except Exception:
                pass

            trace_exec = go.Scatter(
                x=exec_x,
                y=exec_y,
                mode="markers+text",
                name="Fills (B/S)",
                text=exec_text,
                textposition="middle center",
                textfont=dict(color="white", size=12),
                marker=dict(
                    symbol="circle",
                    size=18,
                    color=exec_color,
                    line=dict(color="rgba(0, 0, 0, 0.7)", width=1),
                ),
                customdata=exec_custom,
                hovertemplate=(
                    "%{text} #%{customdata[0]} (%{customdata[2]})<br>"
                    "qty=%{customdata[1]:.6f}<br>"
                    "price=%{customdata[5]:.6f}<br>"
                    "type=%{customdata[3]}<br>"
                    "%{customdata[4]}<extra></extra>"
                ),
            )

            # Expand y-range for executed fills as well
            try:
                y_vals.extend([float(v) for v in exec_y if v is not None])
            except Exception:
                pass

            fig_frames.append(go.Frame(data=[trace_up_entries, trace_up_closes, trace_exec], layout=frame_layout, name=str(i), traces=[3, 4, 5]))

        if not fig_frames:
            st.error("No frames generated")
            return

        try:
            prog_bar.empty()
        except Exception:
            pass

        try:
            stage_line.empty()
        except Exception:
            pass

        try:
            initial_frame_idx = int(initial_frame_idx)
        except Exception:
            initial_frame_idx = 0
        if initial_frame_idx < 0:
            initial_frame_idx = 0
        if initial_frame_idx >= len(fig_frames):
            initial_frame_idx = len(fig_frames) - 1

        init_frame = fig_frames[initial_frame_idx]

        df_static = df_plot_source.loc[(df_plot_source.index >= start_time) & (df_plot_source.index <= end_time)]
        if df_static is None or df_static.empty:
            df_static = df_plot_source

        trace_candle_static = go.Candlestick(
            x=df_static.index,
            open=df_static["open"],
            high=df_static["high"],
            low=df_static["low"],
            close=df_static["close"],
            name="Price",
        )

        cols_static = [c for c in ["ema_0", "ema_1", "ema_2"] if c in df_static.columns]
        if cols_static:
            upper_band_static = df_static[cols_static].max(axis=1)
            lower_band_static = df_static[cols_static].min(axis=1)
        else:
            upper_band_static = df_static["high"]
            lower_band_static = df_static["low"]

        trace_ema_high_static = go.Scatter(
            x=df_static.index,
            y=upper_band_static,
            mode="lines",
            line=dict(color="magenta", width=1, dash="solid"),
            name="EMA High",
        )
        trace_ema_low_static = go.Scatter(
            x=df_static.index,
            y=lower_band_static,
            mode="lines",
            line=dict(color="cyan", width=1, dash="dot"),
            name="EMA Low",
        )

        init_dynamic = list(init_frame.data) if init_frame.data is not None else []
        while len(init_dynamic) < 3:
            init_dynamic.append(go.Scatter(x=[], y=[]))

        init_data = [
            trace_candle_static,
            trace_ema_high_static,
            trace_ema_low_static,
            init_dynamic[0],
            init_dynamic[1],
            init_dynamic[2],
        ]

        # Playback speed: UI timing only (do not scale with candle timeframe).
        play_duration_ms = 120

        layout_args = dict(
            title=f"Animation (Mode C, upcoming fills): {symbol} ({start_time} - {end_time}) | Frames: {len(fig_frames)}",
            xaxis=dict(type="date", rangeslider=dict(visible=False)),
            yaxis=dict(autorange=False, fixedrange=False),
            height=800,
            margin=dict(l=50, r=260, t=70, b=170),
            legend=dict(x=1.02, y=1, xanchor="left", yanchor="top", bgcolor="rgba(0,0,0,0.5)"),
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    showactive=False,
                    x=0.01,
                    y=-0.06,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    buttons=[
                        dict(
                            label="▶ Play",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=play_duration_ms, redraw=False),
                                    fromcurrent=True,
                                    mode="immediate",
                                    transition=dict(duration=0, easing="linear"),
                                ),
                            ],
                        ),
                        dict(
                            label="🐢 Slow",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=int(play_duration_ms * 2), redraw=False),
                                    fromcurrent=True,
                                    mode="immediate",
                                    transition=dict(duration=0, easing="linear"),
                                ),
                            ],
                        ),
                        dict(
                            label="🐌 Very Slow",
                            method="animate",
                            args=[
                                None,
                                dict(
                                    frame=dict(duration=int(play_duration_ms * 4), redraw=False),
                                    fromcurrent=True,
                                    mode="immediate",
                                    transition=dict(duration=0, easing="linear"),
                                ),
                            ],
                        ),
                        dict(
                            label="⏸ Pause",
                            method="animate",
                            args=[[None], dict(frame=dict(duration=0, redraw=False), mode="immediate", transition=dict(duration=0))],
                        ),
                    ],
                )
            ],
            sliders=[
                dict(
                    active=int(initial_frame_idx),
                    steps=[
                        dict(
                            method="animate",
                            args=[[str(k)], dict(mode="immediate", frame=dict(duration=0, redraw=False), transition=dict(duration=0))],
                            label=f"{k}",
                        )
                        for k in range(len(fig_frames))
                    ],
                    transition=dict(duration=0),
                    x=0.01,
                    y=-0.15,
                    currentvalue=dict(font=dict(size=12), prefix="Frame: ", visible=True, xanchor="left"),
                    len=0.99,
                    pad={"b": 10, "t": 10},
                )
            ],
        )

        if init_frame.layout:
            if "xaxis" in init_frame.layout and "range" in init_frame.layout["xaxis"]:
                layout_args["xaxis"]["range"] = init_frame.layout["xaxis"]["range"]
            if "yaxis" in init_frame.layout and "range" in init_frame.layout["yaxis"]:
                layout_args["yaxis"]["range"] = init_frame.layout["yaxis"]["range"]

        fig = go.Figure(data=init_data, layout=go.Layout(**layout_args), frames=fig_frames)
        try:
            st.session_state["gv_movie_fig_modec"] = fig
            st.session_state["gv_movie_meta_modec"] = {
                "start_time": str(start_time),
                "frames": int(frames),
                "step_mins": int(step_mins),
                "exchange": str(exchange),
                "symbol": str(symbol),
                "context_days": float(context_days),
                "side_val": int(side_val),
            }
        except Exception:
            pass
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

        # Fills table under the chart
        try:
            if fills_df is not None and not fills_df.empty:
                df_fills = fills_df.copy()
                cols = [
                    c
                    for c in [
                        "ord_idx",
                        "timestamp",
                        "event",
                        "qty",
                        "price",
                        "order_type",
                        "fee_paid",
                        "wallet_balance",
                        "pos_size",
                    ]
                    if c in df_fills.columns
                ]
                if cols:
                    df_fills = df_fills[cols]
                if len(df_fills) > 5000:
                    df_fills = df_fills.iloc[-5000:]
                try:
                    st.session_state["gv_movie_fills_modec"] = df_fills
                except Exception:
                    pass
                st.dataframe(df_fills, use_container_width=True, height=250)
        except Exception:
            pass


def _calc_entries_rust_ani(pbr, side, data, bp, pos, override_state_json=None):
    ep_json = json.dumps(asdict(data.exchange_params))
    sp_json = override_state_json if override_state_json else json.dumps(asdict(data.state_params))
    tb_json = json.dumps({"min_since_open": 0.0, "max_since_min": 0.0, "max_since_open": 0.0, "min_since_max": 0.0})
    bp_json = json.dumps(asdict(bp))
    
    ep = json.loads(ep_json)
    sp = json.loads(sp_json)
    tb = json.loads(tb_json) 
    b_p = json.loads(bp_json)

    if side == Side.Long:
        return pbr.calc_entries_long_py(
             ep["qty_step"], ep["price_step"], ep["min_qty"], ep["min_cost"], ep["c_mult"],
             b_p["entry_grid_double_down_factor"],
             b_p["entry_grid_spacing_volatility_weight"],
             b_p["entry_grid_spacing_we_weight"],
             b_p["entry_grid_spacing_pct"],
             b_p["entry_initial_ema_dist"],
             b_p["entry_initial_qty_pct"],
             (b_p.get("entry_trailing_double_down_factor") or b_p["entry_grid_double_down_factor"]),
             b_p["entry_trailing_grid_ratio"],
             b_p["entry_trailing_retracement_pct"],
             b_p["entry_trailing_retracement_we_weight"],
             b_p["entry_trailing_retracement_volatility_weight"],
             b_p["entry_trailing_threshold_pct"],
             b_p["entry_trailing_threshold_we_weight"],
             b_p["entry_trailing_threshold_volatility_weight"],
             b_p["total_wallet_exposure_limit"],
             b_p.get("risk_we_excess_allowance_pct", 0.0),
             sp["balance"], float(pos.size), float(pos.price),
             tb["min_since_open"], tb["max_since_min"], tb["max_since_open"], tb["min_since_max"],
             sp["ema_bands"]["lower"], sp.get("entry_volatility_logrange_ema_1h", 0.0),
             sp["order_book"]["bid"]
        )
    else:
        return pbr.calc_entries_short_py(
             ep["qty_step"], ep["price_step"], ep["min_qty"], ep["min_cost"], ep["c_mult"],
             b_p["entry_grid_double_down_factor"],
             b_p["entry_grid_spacing_volatility_weight"],
             b_p["entry_grid_spacing_we_weight"],
             b_p["entry_grid_spacing_pct"],
             b_p["entry_initial_ema_dist"],
             b_p["entry_initial_qty_pct"],
             (b_p.get("entry_trailing_double_down_factor") or b_p["entry_grid_double_down_factor"]),
             b_p["entry_trailing_grid_ratio"],
             b_p["entry_trailing_retracement_pct"],
             b_p["entry_trailing_retracement_we_weight"],
             b_p["entry_trailing_retracement_volatility_weight"],
             b_p["entry_trailing_threshold_pct"],
             b_p["entry_trailing_threshold_we_weight"],
             b_p["entry_trailing_threshold_volatility_weight"],
             b_p["total_wallet_exposure_limit"],
             b_p.get("risk_we_excess_allowance_pct", 0.0),
             sp["balance"], float(pos.size), float(pos.price),
             tb["min_since_open"], tb["max_since_min"], tb["max_since_open"], tb["min_since_max"],
             sp["ema_bands"]["upper"], sp.get("entry_volatility_logrange_ema_1h", 0.0),
             sp["order_book"]["ask"]
        )

def _calc_closes_rust_ani(pbr, side, data, bp, pos, override_state_json=None):
    ep_json = json.dumps(asdict(data.exchange_params))
    sp_json = override_state_json if override_state_json else json.dumps(asdict(data.state_params))
    tb_dummy = {"min_since_open":0.0, "max_since_min":0.0, "max_since_open":0.0, "min_since_max":0.0}
    
    ep = json.loads(ep_json)
    sp = json.loads(sp_json)
    tb = tb_dummy
    b_p = json.loads(json.dumps(asdict(bp)))
    
    if side == Side.Long:
        return pbr.calc_closes_long_py(
            ep["qty_step"], ep["price_step"], ep["min_qty"], ep["min_cost"], ep["c_mult"],
            b_p["close_grid_markup_end"], b_p["close_grid_markup_start"], b_p["close_grid_qty_pct"],
            b_p["close_trailing_grid_ratio"], b_p["close_trailing_qty_pct"],
            b_p["close_trailing_retracement_pct"], b_p["close_trailing_threshold_pct"],
            b_p["total_wallet_exposure_limit"], b_p.get("risk_we_excess_allowance_pct", 0.0), b_p.get("risk_wel_enforcer_threshold", 1.0),
            sp["balance"], float(pos.size), float(pos.price),
            tb["min_since_open"], tb["max_since_min"], tb["max_since_open"], tb["min_since_max"],
            sp["order_book"]["ask"]
        )


def _has_streamlit_session_context() -> bool:
    """Return True when running inside an active Streamlit session.

    This module is imported by pytest/utility code; in that case there is no
    ScriptRunContext and any `st.switch_page()` would raise NoSessionContext.
    """
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False
    else:
        return pbr.calc_closes_short_py(
            ep["qty_step"], ep["price_step"], ep["min_qty"], ep["min_cost"], ep["c_mult"],
            b_p["close_grid_markup_end"], b_p["close_grid_markup_start"], b_p["close_grid_qty_pct"],
            b_p["close_trailing_grid_ratio"], b_p["close_trailing_qty_pct"],
            b_p["close_trailing_retracement_pct"], b_p["close_trailing_threshold_pct"],
            b_p["total_wallet_exposure_limit"], b_p.get("risk_we_excess_allowance_pct", 0.0), b_p.get("risk_wel_enforcer_threshold", 1.0),
            sp["balance"], float(pos.size), float(pos.price),
            tb["min_since_open"], tb["max_since_min"], tb["max_since_open"], tb["min_since_max"],
            sp["order_book"]["bid"]
        )

if _has_streamlit_session_context():
    # Redirect to Login if not authenticated or session state not initialized
    if not is_authenticted() or is_session_state_not_initialized():
        st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
        st.stop()

    # Page Setup
    set_page_config("PBv7 Grid Visualizer")
    st.header("PBv7 Grid Visualizer", divider="red")
    st.info(
        "GridVis uses PB7/Rust calc_* when PB7 is installed. Trailing threshold/retracement weights are now tunable via sliders."
    )

    build_sidebar()
    show_visualizer()
