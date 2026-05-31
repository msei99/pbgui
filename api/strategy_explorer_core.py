"""Pure calculation core for the FastAPI Strategy Explorer.

This module intentionally stays independent from removed UI packages so the
FastAPI Strategy Explorer remains self-contained.
"""

from __future__ import annotations

import copy
import datetime
import json
import math
import os
import sys
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, List, Optional

import numpy as np
import pandas as pd

from pb7_config import load_pb7_config
from pbgui_purefunc import PBGDIR, pb7dir
from strategy_explorer_types import (
    BotParams,
    EmaBands,
    ExchangeParams,
    GridTrailingMode,
    Order,
    OrderBook,
    OrderType,
    Position,
    Side,
    StateParams,
    TrailingPriceBundle,
)


def get_GridTrailing_mode(trailing_grid_ratio: float) -> GridTrailingMode:
    if trailing_grid_ratio == 0.0:
        return GridTrailingMode.GridOnly
    if trailing_grid_ratio in (-1.0, 1.0):
        return GridTrailingMode.TrailingOnly
    if trailing_grid_ratio < 0.0:
        return GridTrailingMode.GridFirst
    if trailing_grid_ratio > 0.0:
        return GridTrailingMode.TrailingFirst
    return GridTrailingMode.Unknown


_PASSIVBOT_RUST_CACHE: dict[str, Any] = {}
MAX_OHLCV_SHARD_BYTES = 256 * 1024 * 1024
MAX_PB7_FILLS_BYTES = 256 * 1024 * 1024


def _safe_market_segment(value: str, default: str = "") -> str:
    """Return a path-safe market segment, or default when invalid."""
    text = str(value or "").strip()
    if text in {"", ".", ".."} or any(ch in text for ch in ("/", "\\", "\x00")):
        return default
    return text


def _is_within_any_root(path: str, roots: list[str]) -> bool:
    """Return True when path resolves below one of the allowed roots."""
    try:
        resolved = os.path.realpath(os.path.expanduser(str(path or "")))
    except Exception:
        return False
    for root in roots:
        try:
            root_resolved = os.path.realpath(os.path.expanduser(str(root or "")))
            if root_resolved and os.path.commonpath([resolved, root_resolved]) == root_resolved:
                return True
        except Exception:
            continue
    return False


def _allowed_strategy_roots(*parts: str) -> list[str]:
    """Return existing Strategy Explorer filesystem roots for a subdirectory."""
    roots: list[str] = []
    try:
        roots.append(os.path.join(str(PBGDIR), *parts))
    except Exception:
        pass
    try:
        pb7_base = str(pb7dir() or "")
        if pb7_base:
            roots.append(os.path.join(pb7_base, *parts))
    except Exception:
        pass
    return [os.path.realpath(os.path.expanduser(root)) for root in roots if root]


def _resolve_safe_ohlcv_source_dir(source_dir: str | None) -> str | None:
    """Return an allowed OHLCV source dir, or None when unsafe/missing."""
    raw = str(source_dir or "").strip()
    if not raw:
        return None
    try:
        resolved = os.path.realpath(os.path.expanduser(raw))
    except Exception:
        return None
    allowed_roots = [
        os.path.realpath(os.path.join(str(PBGDIR), "data")),
        *_allowed_strategy_roots("historical_data"),
        *_allowed_strategy_roots("caches", "ohlcv"),
    ]
    if not os.path.isdir(resolved) or not _is_within_any_root(resolved, allowed_roots):
        return None
    return resolved


def _resolve_safe_backtest_dir(backtest_dir: str | None) -> str | None:
    """Return an allowed PB7/backtest result dir, or None when unsafe/missing."""
    raw = str(backtest_dir or "").strip()
    if not raw:
        return None
    try:
        resolved = os.path.realpath(os.path.expanduser(raw))
    except Exception:
        return None
    allowed_roots = [
        os.path.realpath(os.path.join(str(PBGDIR), "data")),
        *_allowed_strategy_roots("backtests"),
    ]
    if not os.path.isdir(resolved) or not _is_within_any_root(resolved, allowed_roots):
        return None
    return resolved


def _import_passivbot_rust(pb7_src_dir: str):
    if pb7_src_dir and pb7_src_dir not in sys.path:
        sys.path.insert(0, pb7_src_dir)
    import passivbot_rust as pbr  # type: ignore
    return pbr


def _get_passivbot_rust(pb7_src_dir: str):
    key = str(pb7_src_dir or "")
    if key not in _PASSIVBOT_RUST_CACHE:
        _PASSIVBOT_RUST_CACHE[key] = _import_passivbot_rust(key)
    return _PASSIVBOT_RUST_CACHE[key]


def _get_config_ohlcv_source_dir(cfg: Any | None = None) -> str | None:
    val = None
    if isinstance(cfg, dict):
        val = (cfg.get("backtest") or {}).get("ohlcv_source_dir")
    else:
        try:
            val = getattr(getattr(cfg, "backtest", None), "ohlcv_source_dir", None)
        except Exception:
            val = None
    return _resolve_safe_ohlcv_source_dir(str(val or "").strip())


def _get_pbgui_ohlcv_dir() -> str | None:
    base = str(PBGDIR or "") or os.getcwd()
    path = os.path.join(base, "data", "ohlcv")
    return path if os.path.isdir(path) else None


def _get_se_ohlcv_source_settings(cfg: Any | None = None) -> tuple[str | None, bool]:
    source_dir = _get_config_ohlcv_source_dir(cfg)
    return source_dir, bool(source_dir)

def get_available_exchanges_v7(source_dir: str | None = None, *, include_pb7: bool = True) -> List[str]:
    exchanges: set[str] = set()
    source_dir = _resolve_safe_ohlcv_source_dir(source_dir)

    if include_pb7:
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

    def _source_has_data(exchange_root: str) -> bool:
        if not os.path.isdir(exchange_root):
            return False
        try:
            for coin_dir in os.listdir(exchange_root):
                p = os.path.join(exchange_root, coin_dir)
                if not os.path.isdir(p) or coin_dir.startswith("."):
                    continue
                try:
                    if any(
                        f.endswith(".npy") or f.endswith(".npz")
                        for f in os.listdir(p)
                        if not f.startswith(".")
                    ):
                        return True
                except Exception:
                    continue
        except Exception:
            return False
        return False

    source_root = str(source_dir or "").strip()
    if source_root and os.path.isdir(source_root):
        try:
            for d in os.listdir(source_root):
                path = os.path.join(source_root, d, "1m")
                if _source_has_data(path):
                    exchanges.add(d)
        except Exception:
            pass

    return sorted(exchanges)

def get_available_symbols_v7(exchange: str) -> List[str]:
    exchange = _safe_market_segment(exchange)
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


def _filter_pb7_events_by_coin(events: list[dict], symbol_or_coin: str) -> list[dict]:
    """Filter PB7 fills events by coin if the events contain a `coin` field.

    Some PB7 backtest folders (suite/multi-coin) write a single fills.csv containing fills for
    multiple coins. Without filtering, the Movie Builder can jump y-scale (e.g. DOGE ↔ SOL).
    """
    try:
        target = _coin_from_symbol_code(str(symbol_or_coin or "")).strip().upper()
    except Exception:
        target = ""
    if not target:
        return list(events or [])

    evs = list(events or [])
    has_coin = False
    for e in evs:
        try:
            c = str((e or {}).get("coin") or "").strip()
        except Exception:
            c = ""
        if c:
            has_coin = True
            break
    if not has_coin:
        return evs

    out: list[dict] = []
    for e in evs:
        if not isinstance(e, dict):
            continue
        c = str(e.get("coin") or "").strip().upper()
        if c == target:
            out.append(e)
    return out


def _exchange_has_local_ohlcv(exchange: str, symbol: str) -> bool:
    """Return True if there are any local 1m OHLCV shards for (exchange, symbol/coin)."""
    exc = _safe_market_segment(exchange)
    sym_raw = _safe_market_segment(symbol)
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
    exc = _safe_market_segment(exchange)
    if not exc:
        return exc

    # Project convention: Binance OHLCV for futures/perps lives under `binanceusdm`.
    # Do not attempt any other aliasing/mapping.
    if exc == "binance":
        return "binanceusdm"

    if _exchange_has_local_ohlcv(exc, symbol):
        return exc
    return exc


def get_available_coins_v7(exchange: str, source_dir: str | None = None, *, include_pb7: bool = True) -> List[str]:
    exchange = _safe_market_segment(exchange)
    source_dir = _resolve_safe_ohlcv_source_dir(source_dir)
    if not exchange:
        return []

    coins: set[str] = set()

    if include_pb7:
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

    source_root = str(source_dir or "").strip()
    if source_root:
        src_dir = os.path.join(source_root, exchange, "1m")
        if os.path.isdir(src_dir):
            try:
                for d in os.listdir(src_dir):
                    p = os.path.join(src_dir, d)
                    if os.path.isdir(p) and not d.startswith("."):
                        try:
                            has_data = any(
                                f.endswith(".npy") or f.endswith(".npz")
                                for f in os.listdir(p)
                                if not f.startswith(".")
                            )
                        except Exception:
                            has_data = False
                        if not has_data:
                            continue
                        c = _coin_from_symbol_code(d)
                        if c:
                            coins.add(c)
            except Exception:
                pass

    return sorted(coins)


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


def _format_gap_summary(meta: dict) -> tuple[str, pd.DataFrame]:
    """Return (summary_text, sample_df) for a compare-gap meta dict."""
    gaps = (meta or {}).get("gaps") if isinstance(meta, dict) else None
    if not isinstance(gaps, dict):
        return "", pd.DataFrame()

    missing_count = int(gaps.get("missing_count") or 0)
    first_missing = gaps.get("first_missing")
    last_missing = gaps.get("last_missing")
    sample = list(gaps.get("sample_missing") or [])

    insufficient_coverage = bool(gaps.get("insufficient_coverage", False))
    missing_before_minutes = int(gaps.get("missing_before_minutes") or 0)
    missing_after_minutes = int(gaps.get("missing_after_minutes") or 0)
    available_start = gaps.get("available_start")
    available_end = gaps.get("available_end")
    recommended_trade_start = gaps.get("recommended_trade_start")

    try:
        fm = str(pd.to_datetime(first_missing)) if first_missing is not None else ""
    except Exception:
        fm = str(first_missing or "")
    try:
        lm = str(pd.to_datetime(last_missing)) if last_missing is not None else ""
    except Exception:
        lm = str(last_missing or "")

    parts: list[str] = []

    if missing_count > 0:
        s0 = f"Missing 1m candles: {missing_count}"
        if fm and lm:
            s0 += f" (range: {fm} → {lm})"
        parts.append(s0)

    if insufficient_coverage:
        try:
            a0 = str(pd.to_datetime(available_start)) if available_start is not None else ""
        except Exception:
            a0 = str(available_start or "")
        try:
            a1 = str(pd.to_datetime(available_end)) if available_end is not None else ""
        except Exception:
            a1 = str(available_end or "")

        s1 = "Insufficient historical coverage"
        if a0 and a1:
            s1 += f": data available {a0} → {a1}"
        if missing_before_minutes > 0:
            s1 += f" | missing {missing_before_minutes}m before requested start"
        if missing_after_minutes > 0:
            s1 += f" | missing {missing_after_minutes}m after requested end"
        try:
            if recommended_trade_start is not None:
                s1 += f" | suggested analysis start: {str(pd.to_datetime(recommended_trade_start))}"
        except Exception:
            pass
        parts.append(s1)

    summary = "\n".join([p for p in parts if p])

    sdf = pd.DataFrame({"timestamp": [pd.to_datetime(x) for x in sample if x is not None]}) if sample else pd.DataFrame()
    return summary, sdf

def load_historical_ohlcv_v7(
    exchange: str,
    symbol: str,
    source_dir: str | None = None,
    *,
    prefer_source_only: bool = False,
) -> pd.DataFrame:
    """Load 1m candles for a coin.

    Supports both PB7 formats:
    - historical_data shards: 2D arrays (N, 6): [ts, o, h, l, c, v]
    - CandlestickManager cache shards: structured arrays with fields (ts,o,h,l,c,bv)

    Merge semantics (PB7 CandlestickManager-style):
    - Legacy downloader shards (`historical_data/`) are canonical where present.
    - Primary CandlestickManager cache (`caches/ohlcv/`) is used to fill legacy gaps.
    - Conflicts are resolved deterministically (stable sort, keep last).
    """
    exchange = _safe_market_segment(exchange)
    symbol = _safe_market_segment(symbol)
    source_dir = _resolve_safe_ohlcv_source_dir(source_dir)
    if not exchange or not symbol:
        return pd.DataFrame()

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

    def _df_from_npz(path: str) -> pd.DataFrame | None:
        try:
            with np.load(path) as data:
                if "candles" not in data:
                    return None
                arr = data["candles"]
            if not isinstance(arr, np.ndarray) or arr.dtype.names is None:
                return None
            required = ("ts", "o", "h", "l", "c", "bv")
            if any(name not in arr.dtype.names for name in required):
                return None
            df = pd.DataFrame(
                {
                    "timestamp": arr["ts"].astype(np.int64),
                    "open": arr["o"].astype(float),
                    "high": arr["h"].astype(float),
                    "low": arr["l"].astype(float),
                    "close": arr["c"].astype(float),
                    "volume": arr["bv"].astype(float),
                }
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df
        except Exception:
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

    def _load_source_dir_df(exchange_dir: str) -> pd.DataFrame:
        source_root = str(source_dir or "").strip()
        if not source_root:
            return pd.DataFrame()
        source_base = os.path.join(source_root, exchange_dir, "1m")
        if not os.path.isdir(source_base):
            return pd.DataFrame()

        candidates: list[str] = []
        try:
            for d in os.listdir(source_base):
                for c in coin_candidates:
                    if _dir_matches_coin(d, c):
                        candidates.append(d)
                        break
        except Exception:
            candidates = []

        target_dir = None
        if candidates:
            best = None
            best_count = -1
            for cdir in candidates:
                pdir = os.path.join(source_base, cdir)
                if not os.path.isdir(pdir):
                    continue
                try:
                    n_shards = sum(
                        1
                        for f in os.listdir(pdir)
                        if (f.endswith(".npy") or f.endswith(".npz")) and not f.startswith(".")
                    )
                except Exception:
                    n_shards = 0
                if n_shards > best_count:
                    best = cdir
                    best_count = n_shards
            if best is None:
                best = candidates[0]
            target_dir = os.path.join(source_base, best)

        dfs_source: list[pd.DataFrame] = []
        if target_dir and os.path.isdir(target_dir):
            try:
                shard_files = sorted(
                    [f for f in os.listdir(target_dir) if (f.endswith(".npy") or f.endswith(".npz")) and not f.startswith(".")]
                )
            except Exception:
                shard_files = []
            for f in shard_files:
                p = os.path.join(target_dir, f)
                try:
                    if os.path.getsize(p) > MAX_OHLCV_SHARD_BYTES:
                        continue
                except Exception:
                    continue
                df_shard = _df_from_npz(p) if f.endswith(".npz") else _df_from_npy(np.load(p))
                if df_shard is not None and not df_shard.empty:
                    dfs_source.append(df_shard)

        return _dedupe_sort(pd.concat(dfs_source)) if dfs_source else pd.DataFrame()

    dfs_cm: list[pd.DataFrame] = []
    dfs_hist: list[pd.DataFrame] = []

    source_df = pd.DataFrame()
    exchange_dir = str(exchange or "").strip().lower()
    exchange_dirs = [exchange_dir] if exchange_dir else []
    if exchange_dir == "binance":
        exchange_dirs.append("binanceusdm")
    elif exchange_dir == "binanceusdm":
        exchange_dirs.append("binance")
    for exc_dir in exchange_dirs:
        source_df = _load_source_dir_df(exc_dir)
        if source_df is not None and not source_df.empty:
            return source_df
    if prefer_source_only and str(source_dir or "").strip():
        return pd.DataFrame()

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
                    if os.path.getsize(p) > MAX_OHLCV_SHARD_BYTES:
                        continue
                except Exception:
                    continue
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
            if os.path.getsize(path) > MAX_OHLCV_SHARD_BYTES:
                continue
        except Exception:
            continue
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

def calculate_v7_indicators(df: pd.DataFrame, ema0: float, ema1: float, vol_span_hours: float):
    # Volatility (PB7 semantics): log-range on 1h candles, EWM span in hours.
    # State param name is `entry_volatility_logrange_ema_1h`.
    df = df.copy()  # Avoid modifying cached df

    # Coerce OHLC numeric (some sources may load as object).
    for c in ("open", "high", "low", "close"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Always compute a 1m fallback; used to fill any missing early 1h values.
    vol_1m = None
    try:
        lr_1m = np.log(df["high"] / df["low"])
        vol_span_min = max(1.0, float(vol_span_hours or 0.0) * 60.0)
        vol_1m = lr_1m.ewm(span=vol_span_min, adjust=True).mean()
    except Exception:
        vol_1m = None

    vol_series = None
    try:
        # Use fully completed 1h candles ending on the hour.
        ohlc_1h = df[["open", "high", "low", "close"]].resample("1h", label="right", closed="right").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        )
        ohlc_1h.dropna(subset=["open", "high", "low", "close"], inplace=True)
        # Guard against non-positive lows.
        ohlc_1h = ohlc_1h[(ohlc_1h["high"] > 0.0) & (ohlc_1h["low"] > 0.0)]
        log_ratio_1h = np.log(ohlc_1h["high"] / ohlc_1h["low"])
        vol_span_h = max(1.0, float(vol_span_hours) if vol_span_hours is not None else 1.0)
        # PB7 uses bias-adjusted EMA (see `update_adjusted_ema` in rust), matching pandas `adjust=True`.
        vol_1h = log_ratio_1h.ewm(span=vol_span_h, adjust=True).mean()
        # Forward-fill the last completed 1h value to 1m timestamps.
        vol_series = vol_1h.reindex(df.index, method="ffill")
    except Exception:
        vol_series = None

    if vol_series is None:
        # If 1h fails entirely, use 1m series (if available).
        df["volatility"] = vol_1m if vol_1m is not None else 0.0
    else:
        # Fill early NaNs (before first completed 1h candle) from 1m series when possible.
        if vol_1m is not None:
            df["volatility"] = vol_series.combine_first(vol_1m)
        else:
            df["volatility"] = vol_series
        df["volatility"] = df["volatility"].fillna(0.0)

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


def _bot_params_dict_for_rust_visualizer(bp: BotParams) -> dict:
    """Serialize BotParams for the bundled Rust visualizer/backtest input schema."""
    d = asdict(bp)
    d["filter_volume_ema_span"] = float(d.get("filter_volume_ema_span") or d.get("forager_volume_ema_span") or 0.0)
    d["filter_volatility_ema_span"] = float(d.get("filter_volatility_ema_span") or d.get("forager_volatility_ema_span") or 0.0)
    d["forager_volume_drop_pct"] = float(d.get("forager_volume_drop_pct") or d.get("filter_volume_drop_pct") or 0.0)
    d.pop("forager_volume_ema_span", None)
    d.pop("forager_volatility_ema_span", None)
    d.pop("filter_volume_drop_pct", None)

    tier_ratios = d.pop("hsl_tier_ratios", {})
    if not isinstance(tier_ratios, dict):
        tier_ratios = {}
    d["hsl_tier_ratio_yellow"] = float(tier_ratios.get("yellow", 0.5) or 0.5)
    d["hsl_tier_ratio_orange"] = float(tier_ratios.get("orange", 0.75) or 0.75)
    return d


def _bot_params_dict_for_orchestrator_single_symbol(bp: BotParams, *, enabled: bool) -> dict:
    """Build a PB7-compatible BotParams dict for orchestrator JSON API.

    Ensures `wallet_exposure_limit` is populated and resolved for the 1-symbol case.
    """
    d = _bot_params_dict_for_rust_visualizer(bp)
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
    warmup_minutes: int | None = None,
) -> dict:
    """Analyze missing candles in [start_ts, end_ts] (inclusive).

    Distinguishes:
    - internal 1m gaps (missing minutes within the available data range)
    - insufficient coverage (requested window extends beyond available history)

    Backwards-compatible keys:
      - has_gaps, missing_count, first_missing, last_missing, sample_missing
    Additional keys:
      - insufficient_coverage, missing_before_minutes, missing_after_minutes
      - available_start, available_end
      - requested_start, requested_end
      - recommended_trade_start (if warmup_minutes provided and warmup coverage is missing)
    """
    try:
        if df is None or df.empty:
            s = pd.Timestamp(start_ts).floor("min")
            e = pd.Timestamp(end_ts).floor("min")
            return {
                "has_gaps": True,
                "missing_count": -1,
                "first_missing": None,
                "last_missing": None,
                "sample_missing": [],
                "insufficient_coverage": True,
                "missing_before_minutes": 0,
                "missing_after_minutes": 0,
                "available_start": None,
                "available_end": None,
                "requested_start": s,
                "requested_end": e,
                "recommended_trade_start": None,
            }

        idx = pd.to_datetime(df.index)
        if idx.tz is not None:
            idx = idx.tz_localize(None)

        s = pd.Timestamp(start_ts).floor("min")
        e = pd.Timestamp(end_ts).floor("min")
        if e < s:
            return {
                "has_gaps": False,
                "missing_count": 0,
                "first_missing": None,
                "last_missing": None,
                "sample_missing": [],
                "insufficient_coverage": False,
                "missing_before_minutes": 0,
                "missing_after_minutes": 0,
                "available_start": None,
                "available_end": None,
                "requested_start": s,
                "requested_end": e,
                "recommended_trade_start": None,
            }

        available_start = pd.Timestamp(pd.to_datetime(idx.min())).floor("min")
        available_end = pd.Timestamp(pd.to_datetime(idx.max())).floor("min")

        missing_before_minutes = 0
        missing_after_minutes = 0
        if s < available_start:
            try:
                missing_before_minutes = int((available_start - s) / pd.Timedelta(minutes=1))
            except Exception:
                missing_before_minutes = 0
        if e > available_end:
            try:
                missing_after_minutes = int((e - available_end) / pd.Timedelta(minutes=1))
            except Exception:
                missing_after_minutes = 0

        effective_s = max(s, available_start)
        effective_e = min(e, available_end)

        missing = pd.DatetimeIndex([])
        if effective_e >= effective_s:
            # Restrict to the intersection window only; don't treat missing history before/after as internal gaps.
            present = pd.DatetimeIndex(idx[(idx >= effective_s) & (idx <= effective_e)]).floor("min")
            present = present.drop_duplicates().sort_values()
            full = pd.date_range(effective_s, effective_e, freq="1min")
            missing = full.difference(present)

        missing_count = int(len(missing))

        insufficient_coverage = bool(missing_before_minutes > 0 or missing_after_minutes > 0 or effective_e < effective_s)
        recommended_trade_start = None
        try:
            if warmup_minutes is not None and missing_before_minutes > 0:
                recommended_trade_start = pd.Timestamp(available_start) + pd.Timedelta(minutes=int(max(0, int(warmup_minutes))))
                recommended_trade_start = pd.Timestamp(recommended_trade_start).floor("min")
        except Exception:
            recommended_trade_start = None

        return {
            "has_gaps": missing_count > 0,
            "missing_count": missing_count,
            "first_missing": (missing[0] if missing_count else None),
            "last_missing": (missing[-1] if missing_count else None),
            "sample_missing": list(missing[:20]),
            "insufficient_coverage": insufficient_coverage,
            "missing_before_minutes": int(missing_before_minutes),
            "missing_after_minutes": int(missing_after_minutes),
            "available_start": available_start,
            "available_end": available_end,
            "requested_start": s,
            "requested_end": e,
            "recommended_trade_start": recommended_trade_start,
        }
    except Exception:
        return {
            "has_gaps": True,
            "missing_count": -1,
            "first_missing": None,
            "last_missing": None,
            "sample_missing": [],
            "insufficient_coverage": True,
            "missing_before_minutes": 0,
            "missing_after_minutes": 0,
            "available_start": None,
            "available_end": None,
            "requested_start": pd.Timestamp(start_ts).floor("min"),
            "requested_end": pd.Timestamp(end_ts).floor("min"),
            "recommended_trade_start": None,
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

    def _fee(value, default: float = 0.0) -> float:
        try:
            fee = float(value) if value is not None else float(default)
        except (TypeError, ValueError):
            fee = float(default)
        return fee if math.isfinite(fee) and fee >= 0.0 else float(default)

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
            maker_f = _fee(maker, 0.0)
            taker_f = _fee(taker, maker_f)
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

    def _num(value, default: float = 0.0) -> float:
        try:
            result = float(value) if value is not None else float(default)
        except (TypeError, ValueError):
            result = float(default)
        return result if math.isfinite(result) else float(default)

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
            "qty": _num(r.get("fill_qty")),
            "price": _num(r.get("fill_price")),
            "order_type": ot,
            "coin": str(r.get("coin") or ""),
            "wallet_balance": _num(r.get("usd_total_balance")),
            "pos_size": _num(r.get("position_size")),
            "pos_price": _num(r.get("position_price")),
            "pnl": _num(r.get("pnl")),
            "fee_paid": _num(r.get("fee_paid")),
            "wallet_exposure": _num(r.get("wallet_exposure")),
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

    backtest_dir = _resolve_safe_backtest_dir(backtest_dir)
    if not backtest_dir:
        return [], []

    candidates = [
        os.path.join(backtest_dir, "fills.csv"),
        os.path.join(backtest_dir, "fills.csv.gz"),
    ]
    fills_path = next((p for p in candidates if os.path.isfile(p)), "")
    if not fills_path:
        return [], []
    try:
        if os.path.getsize(fills_path) > MAX_PB7_FILLS_BYTES:
            return [], []
    except Exception:
        return [], []

    # Some PB7 versions may emit occasional malformed lines; be liberal in parsing.
    df = None
    try:
        df = pd.read_csv(fills_path, on_bad_lines="skip")
    except TypeError:
        # pandas<1.3 fallback
        try:
            df = pd.read_csv(fills_path)
        except Exception:
            df = None
    except Exception:
        df = None
    if df is None:
        return [], []

    # Drop index columns (PB7 commonly writes an unnamed index column)
    try:
        df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].copy()
    except Exception:
        pass

    # Expected columns (varies by PB7 version):
    # - new: timestamp, qty, price, psize, pprice, type, usd_total_balance, pnl, fee_paid, wallet_exposure
    # - old: minute (int minutes since backtest.start_date), qty, price, psize, pprice, type, balance_usd/balance
    if "type" not in df.columns:
        return [], []

    # Ensure we have a timestamp column.
    if "timestamp" in df.columns:
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        except Exception:
            df["timestamp"] = pd.NaT
    elif "minute" in df.columns:
        # Reconstruct timestamp from config.json backtest.start_date
        start_dt = None
        try:
            cfg_path = os.path.join(backtest_dir, "config.json")
            if os.path.isfile(cfg_path):
                cfg = load_pb7_config(cfg_path, neutralize_added=True)
                sd = ((cfg.get("backtest") or {}).get("start_date") or "").strip()
                if sd:
                    start_dt = pd.to_datetime(sd).normalize()
        except Exception:
            start_dt = None

        if start_dt is None:
            # Can't reconstruct; treat as missing
            return [], []
        try:
            mins = pd.to_numeric(df["minute"], errors="coerce")
            df["timestamp"] = start_dt + pd.to_timedelta(mins, unit="m")
        except Exception:
            return [], []
    else:
        return [], []

    # Wallet/balance column varies
    wallet_bal_col = None
    for c in ["usd_total_balance", "balance_usd", "balance", "usd_cash_wallet"]:
        if c in df.columns:
            wallet_bal_col = c
            break

    events_long: list[dict] = []
    events_short: list[dict] = []

    for _, r in df.iterrows():
        ot = str(r.get("type") or "")
        if not ot:
            continue

        ts = r.get("timestamp")
        if pd.isna(ts):
            continue

        ot_low = ot.lower()

        ev_type = "fill"
        if ot_low.startswith("entry") or "_entry" in ot_low or "entry_" in ot_low:
            ev_type = "entry"
        elif ot_low.startswith("close") or "_close" in ot_low or "close_" in ot_low:
            ev_type = "close"

        event = {
            "timestamp": ts,
            "event": ev_type,
            "qty": float(r.get("qty") or 0.0),
            "price": float(r.get("price") or 0.0),
            "order_type": ot,
            "coin": str(r.get("coin") or "") if "coin" in df.columns else "",
            "wallet_balance": float(r.get(wallet_bal_col) or 0.0) if wallet_bal_col else 0.0,
            "pos_size": float(r.get("psize") or 0.0),
            "pos_price": float(r.get("pprice") or 0.0),
            "pnl": float(r.get("pnl") or 0.0),
            "fee_paid": float(r.get("fee_paid") or 0.0),
            "wallet_exposure": float(r.get("wallet_exposure") or 0.0),
        }

        if "_short" in ot_low or ot_low.endswith("short") or ot_low.startswith("short_"):
            events_short.append(event)
        elif "_long" in ot_low or ot_low.endswith("long") or ot_low.startswith("long_"):
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


def _auto_shift_trade_start_for_warmup_coverage(
    hist_df_full: pd.DataFrame,
    trade_start_time: pd.Timestamp,
    *,
    warmup_minutes: int,
    forward_minutes: int,
) -> tuple[pd.Timestamp, dict]:
    """Match Compare's auto-shift behavior when warmup extends beyond available history.

    If the requested warmup window extends before the first available candle (insufficient coverage)
    and there are no internal 1m gaps, suggest shifting trade_start_time forward to:
      available_start + warmup_minutes

    Returns (effective_trade_start_time, gaps_dict).
    """
    try:
        ts = pd.Timestamp(pd.to_datetime(trade_start_time)).floor("min")
    except Exception:
        ts = pd.Timestamp(trade_start_time)

    try:
        warmup_minutes_i = int(max(0, int(warmup_minutes)))
    except Exception:
        warmup_minutes_i = 0
    try:
        forward_minutes_i = int(max(0, int(forward_minutes)))
    except Exception:
        forward_minutes_i = 0

    warm_start = ts - pd.Timedelta(minutes=warmup_minutes_i)
    end_ts = ts + pd.Timedelta(minutes=max(0, forward_minutes_i))

    gaps = _find_1m_gaps(
        hist_df_full,
        start_ts=pd.Timestamp(warm_start),
        end_ts=pd.Timestamp(end_ts),
        warmup_minutes=int(warmup_minutes_i),
    )
    if not isinstance(gaps, dict):
        return ts, {}

    # Internal gaps cannot be auto-fixed.
    if bool(gaps.get("has_gaps")):
        return ts, gaps

    if bool(gaps.get("insufficient_coverage")):
        new_ts = gaps.get("recommended_trade_start")
        if new_ts is None:
            try:
                astart = gaps.get("available_start")
                if astart is not None:
                    new_ts = pd.Timestamp(pd.to_datetime(astart)).floor("min") + pd.Timedelta(
                        minutes=int(warmup_minutes_i)
                    )
            except Exception:
                new_ts = None

        if new_ts is not None:
            try:
                new_ts = pd.Timestamp(pd.to_datetime(new_ts)).floor("min")
            except Exception:
                new_ts = None

        if new_ts is not None and new_ts > ts:
            # Clamp into available candle range (best-effort).
            try:
                min_ts = pd.Timestamp(pd.to_datetime(hist_df_full.index.min())).floor("min")
                max_ts = pd.Timestamp(pd.to_datetime(hist_df_full.index.max())).floor("min")
                if new_ts < min_ts:
                    new_ts = min_ts
                if new_ts > max_ts:
                    new_ts = max_ts
            except Exception:
                pass

            if new_ts > ts:
                return new_ts, gaps

    return ts, gaps


def _run_with_indeterminate_progress(
    *,
    work_fn: Callable[[], Any],
    progress_fn: Callable[[float, str], None],
    base: float,
    span: float,
    label: str,
    update_every_s: float = 0.2,
) -> Any:
    """Run a blocking function while keeping progress updates responsive.

    This helper runs `work_fn` in a worker thread and updates `progress_fn`
    with an indeterminate progress + elapsed time until it finishes.
    """
    try:
        base_f = float(base)
    except Exception:
        base_f = 0.0
    try:
        span_f = float(span)
    except Exception:
        span_f = 0.0
    base_f = max(0.0, min(1.0, base_f))
    span_f = max(0.0, min(1.0 - base_f, span_f))

    try:
        update_s = float(update_every_s)
    except Exception:
        update_s = 0.2
    if not math.isfinite(update_s) or update_s <= 0.0:
        update_s = 0.2

    t0 = time.time()
    period = 3.0

    # Important: use a single worker to avoid parallel PB7 calls.
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(work_fn)
        i = 0
        while True:
            if fut.done():
                break
            elapsed = max(0.0, time.time() - t0)
            # Indeterminate sawtooth: base + span * phase
            phase = (elapsed % period) / period
            # Gentle easing so it doesn't look like it's stuck at the same spot.
            frac = base_f + span_f * float(phase)
            try:
                progress_fn(frac, f"{label} (elapsed {elapsed:,.0f}s)")
            except Exception:
                pass
            # Avoid tight loop
            try:
                time.sleep(update_s)
            except Exception:
                pass
            i += 1
        # Surface exceptions from worker
        out = fut.result()
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
    progress_cb: Optional[Callable[[float, str], None]] = None,
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

    def _report(progress: float, message: str) -> None:
        if not callable(progress_cb):
            return
        try:
            progress_cb(max(0.0, min(1.0, float(progress))), str(message))
        except RuntimeError:
            raise
        except Exception:
            pass

    _report(0.02, "Loading PB7 backtest config...")
    backtest_dir = _resolve_safe_backtest_dir(backtest_dir) or ""
    if not backtest_dir:
        return ([], []), ([], []), ([], []), {"error": "Invalid PB7 backtest folder."}
    cfg_path = os.path.join(backtest_dir, "config.json")
    cfg: dict = {}
    if os.path.isfile(cfg_path):
        try:
            cfg = load_pb7_config(cfg_path, neutralize_added=True)
        except Exception:
            cfg = {}

    _report(0.08, "Loading PB7 fills.csv...")
    pb7_long, pb7_short = _load_pb7_fills_csv_to_events(backtest_dir)
    all_pb7 = (pb7_long or []) + (pb7_short or [])
    if not all_pb7:
        return ([], []), ([], []), ([], []), {"exchange": "", "coin": "", "start_ts": None, "end_ts": None}

    _report(0.12, "Determining fills.csv compare window...")
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
        bp_long = BotParams.from_dict(bot_cfg.get("long") or {})
    except Exception:
        bp_long = BotParams()
    try:
        bot_cfg = cfg.get("bot") or {}
        bp_short = BotParams.from_dict(bot_cfg.get("short") or {})
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
    source_dir, prefer_source_only = _get_se_ohlcv_source_settings(cfg)
    _report(0.18, "Loading compare candles...")
    hist_df_full = load_historical_ohlcv_v7(
        exchange,
        coin,
        source_dir=source_dir,
        prefer_source_only=prefer_source_only,
    )
    if hist_df_full is None or hist_df_full.empty:
        return (pb7_long, pb7_short), ([], []), ([], []), {"exchange": exchange, "coin": coin, "start_ts": start_ts, "end_ts": end_ts}

    warm_start = trade_start_ts - pd.Timedelta(minutes=max(0, warmup_minutes))
    try:
        candles = hist_df_full.loc[warm_start:end_ts].copy()
    except Exception:
        candles = hist_df_full.copy()

    # PB7 CandlestickManager standardizes gaps for 1m by synthesizing flat zero-volume candles.
    # Apply the same semantics here for parity.
    _report(0.24, "Standardizing compare candles...")
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
    warmup_extras = (0, 1000, 2000, 4000, 8000, 12000, 16000)
    total_attempts = max(1, len(warmup_extras))
    for attempt_index, extra in enumerate(warmup_extras, start=1):
        c_attempts += 1
        try:
            c_warmup_used = int(max(0, warmup_base + int(extra)))
        except Exception:
            c_warmup_used = int(max(0, warmup_base))

        _report(
            0.28 + 0.26 * (float(attempt_index - 1) / float(total_attempts)),
            f"Running PB7 Backtest Engine warmup attempt {attempt_index}/{total_attempts}...",
        )
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
        except RuntimeError:
            raise
        except Exception:
            c_long, c_short = [], []

        _report(
            0.28 + 0.26 * (float(attempt_index) / float(total_attempts)),
            f"Finished PB7 Backtest Engine warmup attempt {attempt_index}/{total_attempts}.",
        )

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
        taker_fee = float((cfg.get("backtest") or {}).get("taker_fee") or 0.0)
    except Exception:
        taker_fee = 0.0
    if not math.isfinite(taker_fee) or taker_fee <= 0.0:
        try:
            taker_fee = float(fees.get("taker_fee", 0.0) or maker_fee)
        except Exception:
            taker_fee = float(maker_fee)

    live_cfg = cfg.get("live") if isinstance(cfg, dict) else {}
    bt_cfg = cfg.get("backtest") if isinstance(cfg, dict) else {}
    live_cfg = live_cfg if isinstance(live_cfg, dict) else {}
    bt_cfg = bt_cfg if isinstance(bt_cfg, dict) else {}
    market_orders_allowed = bool(live_cfg.get("market_orders_allowed", bt_cfg.get("market_orders_allowed", False)))
    try:
        market_order_near_touch_threshold = float(
            bt_cfg.get("market_order_near_touch_threshold", live_cfg.get("market_order_near_touch_threshold", 0.001))
        )
    except Exception:
        market_order_near_touch_threshold = 0.001
    try:
        market_order_slippage_pct = float(
            bt_cfg.get("market_order_slippage_pct", live_cfg.get("market_order_slippage_pct", 0.0005))
        )
    except Exception:
        market_order_slippage_pct = 0.0005

    try:
        _report(0.58, f"Simulating PBGui candles 0/{max(1, int(len(candles) if candles is not None else 0) - 1)}...")

        def _b_progress(progress: float, message: str) -> None:
            _report(0.58 + 0.34 * max(0.0, min(1.0, float(progress))), f"PBGui Simulation: {message}")

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
            taker_fee=float(taker_fee),
            market_orders_allowed=bool(market_orders_allowed),
            market_order_near_touch_threshold=float(market_order_near_touch_threshold),
            market_order_slippage_pct=float(market_order_slippage_pct),
            hsl_signal_mode=str(live_cfg.get("hsl_signal_mode", "unified") or "unified"),
            pnls_max_lookback_days=live_cfg.get("pnls_max_lookback_days", bt_cfg.get("pnls_max_lookback_days", 30.0)),
            trade_start_time=pd.to_datetime(trade_start_ts),
            max_orders=int(max_orders),
            max_candles=int(len(candles) if candles is not None else 0),
            progress_cb=_b_progress,
        )
    except RuntimeError:
        raise
    except Exception:
        b_long, b_short = [], []

    # Filter B/C/PB7 events to the fills.csv time range
    _report(0.94, "Filtering Compare events...")
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
    _report(1.0, "Compare fills.csv range completed.")
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
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> tuple[list[dict], list[dict]]:
    """Mode C: run PB7 Rust backtest engine and return (events_long, events_short)."""

    def _report(progress: float, message: str) -> None:
        if not callable(progress_cb):
            return
        try:
            progress_cb(max(0.0, min(1.0, float(progress))), str(message))
        except RuntimeError:
            raise
        except Exception:
            pass

    if hist_df is None or hist_df.empty:
        return [], []

    _report(0.02, "Preparing PB7 Backtest Engine inputs...")
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

    _report(0.12, "Slicing PB7 Backtest Engine candles...")
    try:
        window = hist_df.loc[start_ts:end_ts].copy()
    except Exception:
        window = hist_df.copy()

    if window is None or window.empty:
        return [], []

    _report(0.22, "Standardizing PB7 Backtest Engine candles...")
    try:
        window = _standardize_ohlcv_1m_gaps(window, start_ts=start_ts, end_ts=end_ts)
    except Exception:
        pass

    if window is None or window.empty:
        return [], []

    # Trade starts at the first candle whose timestamp is >= analysis_ts.
    # This matches Mode B's `trading_active = ts >= trade_start_time` semantics.
    try:
        if analysis_ts in window.index:
            trade_start_index = int(window.index.get_loc(analysis_ts))
        else:
            trade_start_index = int(window.index.searchsorted(analysis_ts, side="left"))
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
    source_dir, prefer_source_only = _get_se_ohlcv_source_settings(config)
    # PB7 provides real BTC prices; use best-effort lookup and align to this window.
    _report(0.36, "Aligning PB7 BTC reference candles...")
    btc_usd = np.ones((hlcvs.shape[0],), dtype=np.float64)
    try:
        btc_df = load_historical_ohlcv_v7(
            exchange,
            "BTC",
            source_dir=source_dir,
            prefer_source_only=prefer_source_only,
        )
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

    # Bundle/backtest metadata
    # Parity detail (Mode B vs Rust engine): Rust runs `check_for_fills(k)` *before* `update_open_orders_all(k)`.
    # To allow fills on the very first tradable candle at `analysis_ts`, open orders must be
    # computed on the previous candle (k-1) with `next_candle.tradable=True`.
    # That requires the guard timestamp to allow order updates one minute earlier.
    analysis_ts_ms = int(pd.Timestamp(analysis_ts).value // 1_000_000)
    effective_start_ts_ms = int(ts_ms[0]) if len(ts_ms) else int(analysis_ts_ms)

    # Parity: to allow orders to be computed on the candle before the first tradable candle,
    # use the actual previous candle timestamp (not a hard-coded -60s, which breaks on gaps).
    try:
        prev_i = max(0, int(trade_start_index) - 1)
        requested_start_ts_ms = int(ts_ms[prev_i]) if len(ts_ms) else int(analysis_ts_ms)
    except Exception:
        requested_start_ts_ms = int(analysis_ts_ms)
    if requested_start_ts_ms < int(effective_start_ts_ms):
        requested_start_ts_ms = int(effective_start_ts_ms)

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

    _report(0.50, "Building PB7 Backtest Engine bundle...")
    bundle = pbr.HlcvsBundle(
        np.ascontiguousarray(hlcvs, dtype=np.float64),
        np.ascontiguousarray(btc_usd, dtype=np.float64),
        np.ascontiguousarray(ts_ms, dtype=np.int64),
        bundle_meta,
    )

    def _bp_dict(bp: BotParams, *, enabled: bool) -> dict:
        d = _bot_params_dict_for_rust_visualizer(bp)
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

    # Rust gate uses `k > warmup_bars`; to enable order generation at (trade_start_index - 1)
    # and fills at trade_start_index (analysis_ts), we set warmup_bars = trade_start_index - 2.
    warmup_bars_for_parity = max(0, int(trade_start_index) - 2)

    backtest_params = {
        "starting_balance": float(starting_balance),
        "maker_fee": float(maker_fee),
        "taker_fee": float(taker_fee),
        "coins": [str(coin)],
        "first_timestamp_ms": int(effective_start_ts_ms),
        "requested_start_timestamp_ms": int(requested_start_ts_ms),
        "first_valid_indices": [0],
        "last_valid_indices": [max(0, int(hlcvs.shape[0] - 1))],
        "warmup_minutes": [int(warmup_minutes_req)],
        "trade_start_indices": [int(trade_start_index)],
        "global_warmup_bars": int(warmup_bars_for_parity),
        "btc_collateral_cap": 0.0,
        "btc_collateral_ltv_cap": None,
        "metrics_only": False,
        "filter_by_min_effective_cost": False,
        "dynamic_wel_by_tradability": True,
        "hedge_mode": True,
        "max_realized_loss_pct": 1.0,
        "pnls_max_lookback_days": 30.0,
        "liquidation_threshold": 0.05,
        "equity_hard_stop_loss": {
            "enabled": False,
            "signal_mode": "unified",
            "red_threshold": 0.25,
            "ema_span_minutes": 60.0,
            "cooldown_minutes_after_red": 0.0,
            "no_restart_drawdown_threshold": 1.0,
            "tier_ratios": {"yellow": 0.5, "orange": 0.75},
            "orange_tier_mode": "tp_only_with_active_entry_cancellation",
            "panic_close_order_type": "market",
        },
        "market_orders_allowed": False,
        "market_order_near_touch_threshold": 0.001,
        "market_order_slippage_pct": 0.0005,
        "forager_score_hysteresis_pct": 0.02,
        "candle_interval_minutes": 1,
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
            try:
                live = config.get("live") or {}
                if "market_orders_allowed" in live:
                    backtest_params["market_orders_allowed"] = bool(live.get("market_orders_allowed"))
                elif "market_orders_allowed" in bt:
                    backtest_params["market_orders_allowed"] = bool(bt.get("market_orders_allowed"))
            except Exception:
                pass
            try:
                if "market_order_near_touch_threshold" in bt:
                    backtest_params["market_order_near_touch_threshold"] = float(bt.get("market_order_near_touch_threshold"))
                else:
                    live = config.get("live") or {}
                    if "market_order_near_touch_threshold" in live:
                        backtest_params["market_order_near_touch_threshold"] = float(live.get("market_order_near_touch_threshold"))
            except Exception:
                pass
            try:
                if "market_order_slippage_pct" in bt:
                    backtest_params["market_order_slippage_pct"] = float(bt.get("market_order_slippage_pct"))
                else:
                    live = config.get("live") or {}
                    if "market_order_slippage_pct" in live:
                        backtest_params["market_order_slippage_pct"] = float(live.get("market_order_slippage_pct"))
            except Exception:
                pass
    except Exception:
        pass

    _report(0.66, f"Running PB7 Backtest Engine over {int(max_candles_forward):,} candles...")
    backtest_result = pbr.run_backtest_bundle(
        bundle,
        bot_params_list,
        exchange_params_list,
        backtest_params,
    )
    fills = backtest_result[0]

    _report(0.94, "Normalizing PB7 Backtest Engine fills...")
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


def _disable_bot_params(bp_in: BotParams) -> BotParams:
    """Return a copy of BotParams with the side effectively disabled (no trading)."""
    try:
        d = asdict(bp_in)
        d["total_wallet_exposure_limit"] = 0.0
        if "n_positions" in d:
            d["n_positions"] = 0
        return BotParams(**d)
    except Exception:
        try:
            bp = copy.deepcopy(bp_in)
            bp.total_wallet_exposure_limit = 0.0
            try:
                bp.n_positions = 0
            except Exception:
                pass
            return bp
        except Exception:
            return bp_in

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
    taker_fee: float = 0.0,
    market_orders_allowed: bool = False,
    market_order_near_touch_threshold: float = 0.001,
    market_order_slippage_pct: float = 0.0005,
    hsl_signal_mode: str = "unified",
    pnls_max_lookback_days: Any = 30.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
) -> List[dict]:
    """Mode B candle-walk (single side) implemented via the shared pair-core.

    This avoids code drift: Compare, Movie Builder, and any single-side simulation
    all run through `_simulate_backtest_over_historical_candles_pair_core`.
    """
    if candles is None or candles.empty:
        return []

    bp_l = bot_params_long
    bp_s = bot_params_short
    pos_l = Position(size=0.0, price=0.0)
    pos_s = Position(size=0.0, price=0.0)

    # Disable the opposite side to preserve single-side semantics.
    if side == Side.Long:
        bp_s = _disable_bot_params(bp_s)
        pos_l = Position(size=float(starting_position.size), price=float(starting_position.price))
    else:
        bp_l = _disable_bot_params(bp_l)
        pos_s = Position(size=float(starting_position.size), price=float(starting_position.price))

    ev_l, ev_s, _frames = _simulate_backtest_over_historical_candles_pair_core(
        pbr=pbr,
        pb7_src=pb7_src,
        side_for_frames=side,
        candles=candles,
        exchange_params=exchange_params,
        bot_params_long=bp_l,
        bot_params_short=bp_s,
        starting_position_long=pos_l,
        starting_position_short=pos_s,
        balance=float(balance),
        maker_fee=float(maker_fee or 0.0),
        taker_fee=float(taker_fee or 0.0),
        market_orders_allowed=bool(market_orders_allowed),
        market_order_near_touch_threshold=float(market_order_near_touch_threshold or 0.0),
        market_order_slippage_pct=float(market_order_slippage_pct or 0.0),
        hsl_signal_mode=hsl_signal_mode,
        pnls_max_lookback_days=pnls_max_lookback_days,
        trade_start_time=trade_start_time,
        max_orders=int(max_orders),
        max_candles=int(max_candles),
        capture_frames=False,
        frame_every_n_candles=1,
        include_viz_grids=False,
        progress_cb=None,
    )

    return list(ev_l or []) if side == Side.Long else list(ev_s or [])


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
    taker_fee: float = 0.0,
    market_orders_allowed: bool = False,
    market_order_near_touch_threshold: float = 0.001,
    market_order_slippage_pct: float = 0.0005,
    hsl_signal_mode: str = "unified",
    pnls_max_lookback_days: Any = 30.0,
    trade_start_time: Optional[pd.Timestamp] = None,
    max_orders: int = 200,
    max_candles: int = 2000,
    progress_cb: Optional[Callable[[float, str], None]] = None,
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
        taker_fee=taker_fee,
        market_orders_allowed=bool(market_orders_allowed),
        market_order_near_touch_threshold=float(market_order_near_touch_threshold or 0.0),
        market_order_slippage_pct=float(market_order_slippage_pct or 0.0),
        hsl_signal_mode=hsl_signal_mode,
        pnls_max_lookback_days=pnls_max_lookback_days,
        trade_start_time=trade_start_time,
        max_orders=max_orders,
        max_candles=max_candles,
        capture_frames=False,
        include_viz_grids=False,
        progress_cb=progress_cb,
    )
    return ev_l, ev_s

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
    taker_fee: float = 0.0,
    market_orders_allowed: bool = False,
    market_order_near_touch_threshold: float = 0.001,
    market_order_slippage_pct: float = 0.0005,
    hsl_signal_mode: str = "unified",
    pnls_max_lookback_days: Any = 30.0,
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
            bp_json_long = json.dumps(_bot_params_dict_for_rust_visualizer(bot_params_long), sort_keys=True)
        except Exception:
            bp_json_long = "{}"
        try:
            bp_json_short = json.dumps(_bot_params_dict_for_rust_visualizer(bot_params_short), sort_keys=True)
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
    taker_fee = float(taker_fee or 0.0)
    if not math.isfinite(taker_fee) or taker_fee < 0.0:
        taker_fee = maker_fee
    market_orders_allowed = bool(market_orders_allowed)
    market_order_slippage_pct = float(market_order_slippage_pct or 0.0)
    if not math.isfinite(market_order_slippage_pct) or market_order_slippage_pct < 0.0:
        market_order_slippage_pct = 0.0

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
    pnl_cumsum_running_net = 0.0
    pnl_cumsum_running_net_long = 0.0
    pnl_cumsum_running_net_short = 0.0

    def _normalize_hsl_signal_mode(value: Any) -> str:
        text = str(value or "").strip().lower()
        return text if text in {"pside", "unified"} else "unified"

    def _parse_hsl_lookback_ms(value: Any) -> int:
        if isinstance(value, str) and value.strip().lower() == "all":
            return (2**63) - 1
        try:
            days = float(value)
        except Exception:
            days = 30.0
        if not math.isfinite(days):
            days = 30.0
        if days < 0.0:
            return (2**63) - 1
        return max(60_000, int(round(float(days) * 86_400_000.0)))

    hsl_signal_mode_norm = _normalize_hsl_signal_mode(hsl_signal_mode)
    hsl_lookback_ms = _parse_hsl_lookback_ms(pnls_max_lookback_days)

    def _hsl_cfg(bp: BotParams) -> dict[str, Any]:
        ratios = getattr(bp, "hsl_tier_ratios", {}) or {}
        if not isinstance(ratios, dict):
            ratios = {}
        try:
            yellow = float(ratios.get("yellow", 0.5) or 0.5)
        except Exception:
            yellow = 0.5
        try:
            orange = float(ratios.get("orange", 0.75) or 0.75)
        except Exception:
            orange = 0.75
        if not (math.isfinite(yellow) and math.isfinite(orange) and 0.0 < yellow < orange < 1.0):
            yellow, orange = 0.5, 0.75
        try:
            red_threshold = float(getattr(bp, "hsl_red_threshold", 0.0) or 0.0)
        except Exception:
            red_threshold = 0.0
        try:
            ema_span_minutes = float(getattr(bp, "hsl_ema_span_minutes", 0.0) or 0.0)
        except Exception:
            ema_span_minutes = 0.0
        try:
            no_restart = float(getattr(bp, "hsl_no_restart_drawdown_threshold", 1.0) or 1.0)
        except Exception:
            no_restart = 1.0
        try:
            cooldown = float(getattr(bp, "hsl_cooldown_minutes_after_red", 0.0) or 0.0)
        except Exception:
            cooldown = 0.0
        enabled = bool(getattr(bp, "hsl_enabled", False)) and red_threshold > 0.0 and ema_span_minutes > 0.0
        return {
            "enabled": bool(enabled),
            "red_threshold": float(red_threshold),
            "ema_span_minutes": float(ema_span_minutes),
            "tier_ratio_yellow": float(yellow),
            "tier_ratio_orange": float(orange),
            "no_restart_drawdown_threshold": float(max(no_restart, red_threshold)),
            "cooldown_minutes_after_red": float(cooldown),
            "orange_tier_mode": str(getattr(bp, "hsl_orange_tier_mode", "") or ""),
            "panic_close_order_type": str(getattr(bp, "hsl_panic_close_order_type", "") or ""),
        }

    hsl_cfg_long = _hsl_cfg(bot_params_long)
    hsl_cfg_short = _hsl_cfg(bot_params_short)
    hsl_enabled_any = bool(hsl_cfg_long.get("enabled") or hsl_cfg_short.get("enabled"))

    def _new_hsl_side_state() -> dict[str, Any]:
        runtime = None
        try:
            runtime_cls = getattr(pbr, "EquityHardStopRuntime", None)
            runtime = runtime_cls() if runtime_cls is not None else None
        except Exception:
            runtime = None
        return {
            "runtime": runtime,
            "py_state": {
                "initialized": False,
                "peak_strategy_equity": 0.0,
                "drawdown_ema": 0.0,
                "tier": "green",
                "red_latched": False,
                "last_minute": None,
                "cached_step": None,
            },
            "rolling_peak_strategy_pnl": deque(),
            "tier": "green",
            "halted": False,
            "no_restart_latched": False,
            "cooldown_until_ms": None,
            "flat_confirmations": 0,
            "pending_stop": None,
            "last_stop": None,
            "current_red_start_ms": None,
            "current_halt_start_ms": None,
            "equity_at_halt": 0.0,
            "last_restart_ts_ms": None,
            "no_restart_peak_strategy_equity": 0.0,
        }

    hsl_state_long = _new_hsl_side_state()
    hsl_state_short = _new_hsl_side_state()

    def _reset_hsl_runtime(side_state: dict[str, Any]) -> None:
        runtime = side_state.get("runtime")
        if runtime is not None:
            try:
                runtime.reset()
            except Exception:
                side_state["runtime"] = None
        side_state["py_state"] = {
            "initialized": False,
            "peak_strategy_equity": 0.0,
            "drawdown_ema": 0.0,
            "tier": "green",
            "red_latched": False,
            "last_minute": None,
            "cached_step": None,
        }

    def _hsl_apply_sample_py(
        side_state: dict[str, Any],
        cfg: dict[str, Any],
        *,
        timestamp_ms: int,
        equity: float,
        peak_strategy_equity: float,
    ) -> dict[str, Any]:
        state = side_state["py_state"]
        alpha = 2.0 / (float(cfg["ema_span_minutes"]) + 1.0)
        current_minute = int(timestamp_ms) // 60_000
        prev_tier = str(state.get("tier") or "green")
        if not state.get("initialized"):
            state["initialized"] = True
            state["peak_strategy_equity"] = float(peak_strategy_equity)
            state["drawdown_ema"] = 0.0
            state["last_minute"] = int(current_minute)
            state["tier"] = "red" if bool(state.get("red_latched")) else "green"
            step = {
                "drawdown_raw": 0.0,
                "drawdown_score": 0.0,
                "tier": state["tier"],
                "changed": state["tier"] != prev_tier,
                "alpha": float(alpha),
                "elapsed_minutes": 0,
            }
            state["cached_step"] = dict(step)
            return step

        last_minute = state.get("last_minute")
        if last_minute is None:
            last_minute = current_minute
        elapsed_minutes = max(0, int(current_minute) - int(last_minute))
        if elapsed_minutes == 0:
            step = dict(state.get("cached_step") or {})
            if not step:
                step = {"drawdown_raw": 0.0, "drawdown_score": 0.0, "tier": state.get("tier", "green")}
            step["changed"] = False
            step["elapsed_minutes"] = 0
            return step

        state["peak_strategy_equity"] = float(peak_strategy_equity)
        drawdown_raw = max(0.0, 1.0 - (float(equity) / max(float(peak_strategy_equity), float(np.finfo("float64").eps))))
        decay = (1.0 - float(alpha)) ** float(elapsed_minutes)
        state["drawdown_ema"] = float(drawdown_raw) + (float(state.get("drawdown_ema") or 0.0) - float(drawdown_raw)) * float(decay)
        drawdown_score = min(float(drawdown_raw), float(state["drawdown_ema"]))
        red_threshold = float(cfg["red_threshold"])
        threshold_yellow = float(cfg["tier_ratio_yellow"]) * red_threshold
        threshold_orange = float(cfg["tier_ratio_orange"]) * red_threshold
        cmp_eps = 1e-12
        if bool(state.get("red_latched")) or drawdown_score + cmp_eps >= red_threshold:
            next_tier = "red"
        elif drawdown_score + cmp_eps >= threshold_orange:
            next_tier = "orange"
        elif drawdown_score + cmp_eps >= threshold_yellow:
            next_tier = "yellow"
        else:
            next_tier = "green"
        if next_tier == "red":
            state["red_latched"] = True
        state["tier"] = "red" if bool(state.get("red_latched")) else next_tier
        state["last_minute"] = int(current_minute)
        step = {
            "drawdown_raw": float(drawdown_raw),
            "drawdown_score": float(drawdown_score),
            "tier": str(state["tier"]),
            "changed": str(state["tier"]) != prev_tier,
            "alpha": float(alpha),
            "elapsed_minutes": int(elapsed_minutes),
        }
        state["cached_step"] = dict(step)
        return step

    def _hsl_apply_sample(
        side_state: dict[str, Any],
        cfg: dict[str, Any],
        *,
        timestamp_ms: int,
        equity: float,
        peak_strategy_equity: float,
    ) -> dict[str, Any]:
        runtime = side_state.get("runtime")
        if runtime is not None:
            try:
                out = runtime.apply_sample(
                    timestamp_ms=int(timestamp_ms),
                    equity=float(equity),
                    peak_strategy_equity=float(peak_strategy_equity),
                    red_threshold=float(cfg["red_threshold"]),
                    ema_span_minutes=float(cfg["ema_span_minutes"]),
                    tier_ratio_yellow=float(cfg["tier_ratio_yellow"]),
                    tier_ratio_orange=float(cfg["tier_ratio_orange"]),
                )
                return dict(out)
            except Exception:
                side_state["runtime"] = None
        return _hsl_apply_sample_py(side_state, cfg, timestamp_ms=timestamp_ms, equity=equity, peak_strategy_equity=peak_strategy_equity)

    def _hsl_rolling_peak(side_state: dict[str, Any], timestamp_ms: int, strategy_pnl: float) -> float:
        queue = side_state["rolling_peak_strategy_pnl"]
        while queue:
            old_ts, _old_pnl = queue[0]
            if int(timestamp_ms) - int(old_ts) > int(hsl_lookback_ms):
                queue.popleft()
            else:
                break
        while queue and float(queue[-1][1]) <= float(strategy_pnl):
            queue.pop()
        queue.append((int(timestamp_ms), float(strategy_pnl)))
        return float(queue[0][1]) if queue else float(strategy_pnl)

    def _unrealized_long(close_price: float) -> float:
        if float(pos_long.size) <= 0.0 or float(pos_long.price) <= 0.0:
            return 0.0
        try:
            return float(pbr.calc_pnl_long(float(pos_long.price), float(close_price), float(pos_long.size), float(c_mult)))
        except Exception:
            return float((float(close_price) - float(pos_long.price)) * float(pos_long.size) * float(c_mult))

    def _unrealized_short(close_price: float) -> float:
        if float(pos_short.size) >= 0.0 or float(pos_short.price) <= 0.0:
            return 0.0
        try:
            return float(pbr.calc_pnl_short(float(pos_short.price), float(close_price), float(pos_short.size), float(c_mult)))
        except Exception:
            return float((float(pos_short.price) - float(close_price)) * abs(float(pos_short.size)) * float(c_mult))

    def _orders_have_hsl_blocking(entries: list[dict], closes: list[dict]) -> bool:
        if entries:
            return True
        for order in closes or []:
            try:
                order_type = str(order.get("order_type") or "").strip().lower()
            except Exception:
                order_type = ""
            if order_type and not order_type.startswith("close_panic_"):
                return True
        return False

    def _hsl_update_side(
        side_name: str,
        *,
        timestamp_ms: int,
        close_price: float,
        has_blocking_open_orders: bool,
    ) -> None:
        nonlocal hsl_state_long, hsl_state_short
        if side_name == "long":
            side_state = hsl_state_long
            cfg = hsl_cfg_long
            pos = pos_long
            realized_pnl = pnl_cumsum_running_net_long
            unrealized_pnl = _unrealized_long(close_price)
        else:
            side_state = hsl_state_short
            cfg = hsl_cfg_short
            pos = pos_short
            realized_pnl = pnl_cumsum_running_net_short
            unrealized_pnl = _unrealized_short(close_price)
        if not bool(cfg.get("enabled")) or bool(side_state.get("halted")):
            return

        total_unrealized = _unrealized_long(close_price) + _unrealized_short(close_price)
        equity = float(sim_balance) + float(total_unrealized)
        if hsl_signal_mode_norm == "unified":
            realized_pnl = float(pnl_cumsum_running_net)
            unrealized_pnl = float(equity) - float(sim_balance)
        strategy_pnl = float(realized_pnl) + float(unrealized_pnl)
        baseline_balance = float(sim_balance) - float(pnl_cumsum_running_net)
        strategy_equity = float(baseline_balance) + float(strategy_pnl)
        peak_strategy_pnl = _hsl_rolling_peak(side_state, int(timestamp_ms), float(strategy_pnl))
        peak_strategy_equity = max(float(baseline_balance) + float(peak_strategy_pnl), float(strategy_equity))
        if not (
            math.isfinite(strategy_equity)
            and strategy_equity > 0.0
            and math.isfinite(peak_strategy_equity)
            and peak_strategy_equity > 0.0
        ):
            return
        no_restart_threshold = float(cfg["no_restart_drawdown_threshold"])
        red_threshold = float(cfg["red_threshold"])
        if not (math.isfinite(no_restart_threshold) and red_threshold <= no_restart_threshold <= 1.0):
            return

        prev_tier = str(side_state.get("tier") or "green").lower()
        step = _hsl_apply_sample(
            side_state,
            cfg,
            timestamp_ms=int(timestamp_ms),
            equity=float(strategy_equity),
            peak_strategy_equity=float(peak_strategy_equity),
        )
        tier = str(step.get("tier") or prev_tier or "green").lower()
        side_state["tier"] = tier
        if tier == "red":
            if prev_tier != "red":
                side_state["current_red_start_ms"] = int(timestamp_ms)
        else:
            side_state["current_red_start_ms"] = None

        if tier == "red":
            if float(pos.size) != 0.0 or bool(has_blocking_open_orders):
                side_state["flat_confirmations"] = 0
                side_state["pending_stop"] = None
                return
            side_state["flat_confirmations"] = int(side_state.get("flat_confirmations") or 0) + 1
            if int(side_state["flat_confirmations"]) == 1:
                side_state["pending_stop"] = {
                    "timestamp_ms": int(timestamp_ms),
                    "equity": float(strategy_equity),
                    "peak_strategy_equity": float(peak_strategy_equity),
                    "drawdown_raw": float(step.get("drawdown_raw") or 0.0),
                }
            if int(side_state["flat_confirmations"]) >= 2:
                stop_snapshot = side_state.get("pending_stop") or {
                    "timestamp_ms": int(timestamp_ms),
                    "equity": float(strategy_equity),
                    "peak_strategy_equity": float(peak_strategy_equity),
                    "drawdown_raw": float(step.get("drawdown_raw") or 0.0),
                }
                side_state["last_stop"] = dict(stop_snapshot)
                side_state["pending_stop"] = None
                side_state["halted"] = True
                side_state["current_halt_start_ms"] = int(stop_snapshot["timestamp_ms"])
                side_state["equity_at_halt"] = float(strategy_equity)
                no_restart_peak = max(
                    float(side_state.get("no_restart_peak_strategy_equity") or 0.0),
                    float(stop_snapshot.get("peak_strategy_equity") or 0.0),
                    float(stop_snapshot.get("equity") or 0.0),
                )
                side_state["no_restart_peak_strategy_equity"] = float(no_restart_peak)
                persistent_drawdown = max(
                    0.0,
                    1.0 - float(stop_snapshot.get("equity") or 0.0) / max(float(no_restart_peak), float(np.finfo("float64").eps)),
                )
                if persistent_drawdown >= no_restart_threshold:
                    side_state["no_restart_latched"] = True
                    side_state["cooldown_until_ms"] = None
                else:
                    cooldown_minutes = float(cfg.get("cooldown_minutes_after_red") or 0.0)
                    if math.isfinite(cooldown_minutes) and cooldown_minutes > 0.0:
                        cooldown_ms = max(60_000, int(round(cooldown_minutes * 60_000.0)))
                        side_state["cooldown_until_ms"] = int(stop_snapshot["timestamp_ms"]) + int(cooldown_ms)
                    else:
                        side_state["cooldown_until_ms"] = None
        else:
            side_state["flat_confirmations"] = 0
            side_state["pending_stop"] = None

    def _hsl_try_restart(side_state: dict[str, Any], current_ts_ms: int) -> None:
        if bool(side_state.get("no_restart_latched")):
            return
        cooldown_until_ms = side_state.get("cooldown_until_ms")
        if cooldown_until_ms is None or int(current_ts_ms) < int(cooldown_until_ms):
            return
        side_state["halted"] = False
        side_state["cooldown_until_ms"] = None
        side_state["flat_confirmations"] = 0
        side_state["tier"] = "green"
        _reset_hsl_runtime(side_state)
        side_state["rolling_peak_strategy_pnl"].clear()
        side_state["pending_stop"] = None
        side_state["current_red_start_ms"] = None
        side_state["last_restart_ts_ms"] = int(current_ts_ms)

    def _hsl_mode_for_side(side_name: str, pos: Position) -> Optional[str]:
        if side_name == "long":
            side_state = hsl_state_long
            cfg = hsl_cfg_long
        else:
            side_state = hsl_state_short
            cfg = hsl_cfg_short
        if not bool(cfg.get("enabled")):
            return None
        if bool(side_state.get("halted")):
            return "panic" if float(pos.size) != 0.0 else "graceful_stop"
        tier = str(side_state.get("tier") or "green").lower()
        if tier == "red":
            return "panic"
        if tier == "orange":
            if str(cfg.get("orange_tier_mode") or "").strip().lower() == "graceful_stop":
                return "graceful_stop"
            if float(pos.size) != 0.0:
                return "tp_only"
        return None

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
        # PB7 Rust (passivbot-rust/src/backtest.rs :: order_filled):
        # - buy (qty > 0): candle_low < order_price
        # - sell (qty < 0): candle_high > order_price
        # - strict comparisons (no equality fills)
        # Do not round before this comparison: f32 candle highs/lows can be just beyond
        # the displayed order price, and PB7 fills those strict boundary crossings.
        low = float(low)
        high = float(high)
        price = float(price)
        if qty > 0.0:
            return float(low) < float(price)
        if qty < 0.0:
            return float(high) > float(price)
        return False

    def _execution_type_is_market(value: Any) -> bool:
        try:
            text = str(value or "").strip().lower()
        except Exception:
            return False
        return text == "market" or text.endswith("::market")

    def _panic_close_uses_market(order: dict) -> bool:
        try:
            order_type = str(order.get("order_type") or "").strip().lower()
        except Exception:
            return False
        if order_type == "close_panic_long":
            return str(hsl_cfg_long.get("panic_close_order_type") or "").strip().lower() == "market"
        if order_type == "close_panic_short":
            return str(hsl_cfg_short.get("panic_close_order_type") or "").strip().lower() == "market"
        return False

    def _market_fill_price(qty: float, close_price: float) -> float:
        price_step_local = max(float(price_step or 0.0), float(np.finfo("float64").eps))
        close_local = max(float(close_price), float(np.finfo("float64").eps))
        if float(qty) > 0.0:
            slipped = close_local * (1.0 + float(market_order_slippage_pct))
            return max(price_step_local, math.ceil(slipped / price_step_local) * price_step_local)
        if float(qty) < 0.0:
            slipped = close_local * (1.0 - float(market_order_slippage_pct))
            return max(price_step_local, math.floor(slipped / price_step_local) * price_step_local)
        return close_local

    def _fill_execution(qty: float, price: float, low: float, high: float, close: float, order: dict) -> tuple[bool, float, float, str]:
        if _panic_close_uses_market(order):
            return True, float(_market_fill_price(float(qty), float(close))), float(taker_fee), "taker"
        if market_orders_allowed and _execution_type_is_market(order.get("execution_type")):
            return True, float(_market_fill_price(float(qty), float(close))), float(taker_fee), "taker"
        if _order_filled(low, high, qty, price, str(order.get("order_type") or "")):
            return True, float(price), float(maker_fee), "maker"
        return False, float(price), float(maker_fee), ""

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
    # Ensure symbol-level integer fields are integers (serde expects usize for some fields).
    try:
        bp_long_symbol_dict["n_positions"] = int(bp_long_symbol_dict.get("n_positions") or 0)
    except Exception:
        pass
    try:
        bp_short_symbol_dict["n_positions"] = int(bp_short_symbol_dict.get("n_positions") or 0)
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
        mode_long: Optional[str] = None,
        mode_short: Optional[str] = None,
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        ul, us = _unstuck_allowance()

        def _mode_or_none(value: Any) -> Optional[str]:
            text = str(value or "").strip().lower()
            return text if text in {"normal", "panic", "graceful_stop", "tp_only", "manual"} else None

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

        def _span_key(bp: BotParams, name: str, default: float = 1.0) -> float:
            value = getattr(bp, name, default)
            return float(default if value is None else value)

        m1_volume.append([
            _span_key(bot_params_long, "filter_volume_ema_span"),
            float(ema_row.get("vol_ema_l", 0.0) or 0.0),
        ])
        m1_volume.append([
            _span_key(bot_params_short, "filter_volume_ema_span"),
            float(ema_row.get("vol_ema_s", 0.0) or 0.0),
        ])
        m1_log_range.append([
            _span_key(bot_params_long, "filter_volatility_ema_span"),
            float(ema_row.get("lr_ema_l", 0.0) or 0.0),
        ])
        m1_log_range.append([
            _span_key(bot_params_short, "filter_volatility_ema_span"),
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
                "market_orders_allowed": bool(market_orders_allowed),
                "market_order_near_touch_threshold": float(market_order_near_touch_threshold or 0.0),
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
                        "mode": _mode_or_none(mode_long),
                        "position": {"size": float(pos_l.size), "price": float(pos_l.price)},
                        "trailing": asdict(tb_l),
                        "bot_params": bp_long_symbol_dict,
                    },
                    "short": {
                        "mode": _mode_or_none(mode_short),
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
                rec = {
                    "qty": float(o.get("qty") or 0.0),
                    "price": float(o.get("price") or 0.0),
                    "order_type": ot,
                    "execution_type": o.get("execution_type"),
                }
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
    pending_mode_long = _hsl_mode_for_side("long", pos_long)
    pending_mode_short = _hsl_mode_for_side("short", pos_short)

    for i in range(1, min(len(idx_list), int(candle_cap))):
        if progress_cb is not None and (i == 1 or i == total_steps or (i % progress_every == 0)):
            try:
                progress_cb(min(1.0, float(i) / float(total_steps)), f"Simulating candles {i}/{total_steps}")
            except RuntimeError:
                raise
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
                mode_long=pending_mode_long,
                mode_short=pending_mode_short,
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
                filled, fill_price, fee_rate, liquidity = _fill_execution(q, p, low_px, high_px, close_px, o)
                if not filled:
                    continue
                p = float(fill_price)

                adj_qty = float(q)
                try:
                    new_psize = float(pbr.round_(float(pos_long.size) + float(adj_qty), float(qty_step))) if qty_step > 0.0 else float(pos_long.size) + float(adj_qty)
                    if new_psize < 0.0:
                        new_psize = 0.0
                        adj_qty = -float(pos_long.size)
                except Exception:
                    new_psize = float(pos_long.size) + float(adj_qty)

                fee_paid = -float(pbr.qty_to_cost(float(adj_qty), float(p), float(c_mult))) * float(fee_rate)
                pnl = float(pbr.calc_pnl_long(float(pos_long.price), float(p), float(adj_qty), float(c_mult)))
                pnl_cumsum_running += float(pnl)
                pnl_cumsum_max = max(float(pnl_cumsum_max), float(pnl_cumsum_running))
                pnl_cumsum_running_net += float(pnl) + float(fee_paid)
                pnl_cumsum_running_net_long += float(pnl) + float(fee_paid)
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
                    "liquidity": str(liquidity or "maker"),
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
                filled, fill_price, fee_rate, liquidity = _fill_execution(q, p, low_px, high_px, close_px, o)
                if not filled:
                    continue
                p = float(fill_price)

                adj_qty = float(q)
                try:
                    new_psize = float(pbr.round_(float(pos_short.size) + float(adj_qty), float(qty_step))) if qty_step > 0.0 else float(pos_short.size) + float(adj_qty)
                    if new_psize > 0.0:
                        new_psize = 0.0
                        adj_qty = abs(float(pos_short.size))
                except Exception:
                    new_psize = float(pos_short.size) + float(adj_qty)

                fee_paid = -float(pbr.qty_to_cost(float(adj_qty), float(p), float(c_mult))) * float(fee_rate)
                pnl = float(pbr.calc_pnl_short(float(pos_short.price), float(p), float(adj_qty), float(c_mult)))
                pnl_cumsum_running += float(pnl)
                pnl_cumsum_max = max(float(pnl_cumsum_max), float(pnl_cumsum_running))
                pnl_cumsum_running_net += float(pnl) + float(fee_paid)
                pnl_cumsum_running_net_short += float(pnl) + float(fee_paid)
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
                    "liquidity": str(liquidity or "maker"),
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
                filled, fill_price, fee_rate, liquidity = _fill_execution(q, p, low_px, high_px, close_px, o)
                if not filled:
                    continue
                p = float(fill_price)

                fee_paid = -float(pbr.qty_to_cost(float(q), float(p), float(c_mult))) * float(fee_rate)
                pnl_cumsum_running_net += float(fee_paid)
                pnl_cumsum_running_net_long += float(fee_paid)
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
                    "liquidity": str(liquidity or "maker"),
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
                filled, fill_price, fee_rate, liquidity = _fill_execution(q, p, low_px, high_px, close_px, o)
                if not filled:
                    continue
                p = float(fill_price)

                fee_paid = -float(pbr.qty_to_cost(float(q), float(p), float(c_mult))) * float(fee_rate)
                pnl_cumsum_running_net += float(fee_paid)
                pnl_cumsum_running_net_short += float(fee_paid)
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
                    "liquidity": str(liquidity or "maker"),
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

        # PB7 computes the next open-order set before updating HSL state.  Keep that
        # one-candle lag so a newly red HSL tier affects the same later candle as PB7.
        capture_this_frame = bool(
            capture_frames
            and (i % fe) == 0
            and (capture_from_pd is None or pd.to_datetime(ts) >= capture_from_pd)
        )
        next_mode_long = _hsl_mode_for_side("long", pos_long)
        next_mode_short = _hsl_mode_for_side("short", pos_short)
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
        l_e2: list[dict] = []
        l_c2: list[dict] = []
        s_e2: list[dict] = []
        s_c2: list[dict] = []
        if (hsl_enabled_any or capture_this_frame) and bool(next_trading_active) and ema_now is not None:
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
                    mode_long=next_mode_long,
                    mode_short=next_mode_short,
                )
            except Exception:
                l_e2, l_c2, s_e2, s_c2 = [], [], [], []

        if hsl_enabled_any and trading_active:
            try:
                ts_ms = int(pd.to_datetime(ts).value // 1_000_000)
            except Exception:
                ts_ms = int(i) * 60_000
            _hsl_update_side(
                "long",
                timestamp_ms=int(ts_ms),
                close_price=float(close_px),
                has_blocking_open_orders=_orders_have_hsl_blocking(l_e2, l_c2),
            )
            _hsl_update_side(
                "short",
                timestamp_ms=int(ts_ms),
                close_price=float(close_px),
                has_blocking_open_orders=_orders_have_hsl_blocking(s_e2, s_c2),
            )
            _hsl_try_restart(hsl_state_long, int(ts_ms))
            _hsl_try_restart(hsl_state_short, int(ts_ms))

        pending_mode_long = next_mode_long
        pending_mode_short = next_mode_short

        # Capture frame (optionally skip early warmup frames)
        if capture_this_frame:
            # Compute POST-candle pending orders for immediate rendering.
            if side_for_frames == Side.Long:
                pending_entries_post = list(l_e2 or [])
                pending_closes_post = list(l_c2 or [])
            else:
                pending_entries_post = list(s_e2 or [])
                pending_closes_post = list(s_c2 or [])

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
        # `.../pbgui/api/strategy_explorer_core.py` -> `.../pbgui/api` -> `.../pbgui` -> `.../software`
        sibling = os.path.abspath(os.path.join(os.path.dirname(here), "..", "..", "pb7", "src"))
        if os.path.exists(os.path.join(sibling, "passivbot.py")):
            return sibling
    except Exception:
        pass

    return ""


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

    long_entry_grid: float = 0.0
    long_close_grid: float = 0.0
    short_entry_grid: float = 0.0
    short_close_grid: float = 0.0

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
    # Stored as list of dicts for easy display in result tables.
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
            normal_bot_params_long=BotParams.from_dict(long_data),
            normal_bot_params_short=BotParams.from_dict(short_data)
        )

def adjust_order_quantities(orders: List[Order]) -> List[Order]:
    for order in orders:
        order.qty = abs(order.qty)
    return orders
