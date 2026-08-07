"""Shared PBGui MarketData price-series support for backtest result charts."""

from __future__ import annotations

import datetime
import math
from typing import Any

import numpy as np


SERVICE = "BacktestPrice"


def _safe_identifier(value: object, label: str) -> str:
    """Validate one exchange or coin identifier before filesystem-backed lookup."""
    text = str(value or "").strip()
    if (
        not text
        or text in {".", ".."}
        or any(char in text for char in ("/", "\\", "\x00"))
        or any(ord(char) < 32 for char in text)
    ):
        raise ValueError(f"Invalid {label}")
    return text


def _config_date(config: dict[str, Any], key: str) -> datetime.date:
    """Return one required ISO backtest date."""
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    raw = str(backtest.get(key) or "").strip()[:10]
    try:
        return datetime.date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Backtest result has no valid {key}") from exc


def _storage_coin_dir(exchange: str, coin: str) -> str:
    """Resolve a normalized coin name to its canonical PBGui OHLCV directory."""
    from market_data import get_exchange_raw_root_dir, normalize_market_data_coin_dir
    from PBCoinData import get_symbol_for_coin

    exchange_name = str(exchange or "").strip().lower()
    coin_name = str(coin or "").strip().upper()
    dataset_root = get_exchange_raw_root_dir(exchange_name) / "1m"
    candidates: list[str] = []

    def add_candidate(value: object) -> None:
        candidate = str(value or "").strip().upper()
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    add_candidate(normalize_market_data_coin_dir(exchange_name, coin_name))
    add_candidate(coin_name)
    if not coin_name.endswith(("_USDT:USDT", "_USDC:USDC")):
        preferred_quote = "USDC" if exchange_name == "hyperliquid" else "USDT"
        add_candidate(f"{coin_name}_{preferred_quote}:{preferred_quote}")

    try:
        symbol = str(get_symbol_for_coin(coin_name, f"{exchange_name}.swap") or "").strip().upper()
    except Exception:
        symbol = ""
    if "/" in symbol:
        add_candidate(symbol.replace("/", "_"))
    for quote in ("USDT", "USDC"):
        swap_suffix = f"-{quote}-SWAP"
        if symbol.endswith(swap_suffix):
            add_candidate(f"{symbol[:-len(swap_suffix)]}_{quote}:{quote}")
        elif symbol.endswith(quote) and len(symbol) > len(quote):
            add_candidate(f"{symbol[:-len(quote)]}_{quote}:{quote}")

    for candidate in candidates:
        if (dataset_root / candidate).is_dir():
            return candidate
    return candidates[0] if candidates else coin_name


def build_market_price_payload(
    config: dict[str, Any],
    *,
    exchange: object,
    coin: object,
    max_points: int = 6000,
) -> dict[str, Any]:
    """Load a bounded close-price series from PBGui-managed one-minute OHLCV data."""
    from api.market_data import _inventory_storage_exchange, _load_ohlcv_from_npz_range, _normalize_settings_exchange

    if not isinstance(config, dict):
        raise ValueError("Backtest result config is invalid")
    exchange_name = _safe_identifier(exchange, "exchange")
    coin_name = _safe_identifier(coin, "coin")
    normalized_exchange = _normalize_settings_exchange(exchange_name)
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    raw_exchanges = backtest.get("exchanges") or []
    if isinstance(raw_exchanges, str):
        raw_exchanges = [raw_exchanges]
    configured_exchanges = {
        _normalize_settings_exchange(str(item or ""))
        for item in list(raw_exchanges)
        if str(item or "").strip()
    }
    if configured_exchanges and normalized_exchange not in configured_exchanges:
        raise ValueError("Exchange is not part of this backtest result")

    start_date = _config_date(config, "start_date")
    end_date = _config_date(config, "end_date")
    if end_date < start_date:
        raise ValueError("Backtest result date range is invalid")

    storage_exchange = _inventory_storage_exchange(normalized_exchange)
    frame = _load_ohlcv_from_npz_range(
        exchange=storage_exchange,
        dataset="1m",
        coin=_storage_coin_dir(storage_exchange, coin_name),
        start_day=start_date.strftime("%Y%m%d"),
        end_day=end_date.strftime("%Y%m%d"),
    )
    empty_payload = {
        "available": False,
        "source": "PBGui MarketData",
        "exchange": normalized_exchange,
        "coin": coin_name,
        "requested_start": start_date.isoformat(),
        "requested_end": end_date.isoformat(),
        "coverage_start": None,
        "coverage_end": None,
        "coverage_complete": False,
        "original_points": 0,
        "time": [],
        "close": [],
    }
    if frame is None or frame.empty or "ts" not in frame or "c" not in frame:
        return empty_payload

    timestamps = frame["ts"].to_numpy(dtype="int64", copy=False)
    closes = frame["c"].to_numpy(dtype="float64", copy=False)
    if timestamps.size and int(np.nanmax(timestamps)) < 100_000_000_000:
        timestamps = timestamps * 1000
    start_ms = int(datetime.datetime.combine(start_date, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    end_ms = int(
        datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min, tzinfo=datetime.timezone.utc).timestamp()
        * 1000
    )
    valid = (timestamps >= start_ms) & (timestamps < end_ms) & np.isfinite(closes) & (closes > 0)
    timestamps = timestamps[valid]
    closes = closes[valid]
    if not timestamps.size:
        return empty_payload

    order = np.argsort(timestamps, kind="stable")
    timestamps = timestamps[order]
    closes = closes[order]
    unique = np.concatenate(([True], timestamps[1:] != timestamps[:-1]))
    timestamps = timestamps[unique]
    closes = closes[unique]
    original_points = int(timestamps.size)
    point_limit = max(2, min(10_000, int(max_points or 6000)))
    if original_points > point_limit:
        positions = np.linspace(0, original_points - 1, num=point_limit, dtype=np.int64)
        timestamps = timestamps[positions]
        closes = closes[positions]

    coverage_start = datetime.datetime.fromtimestamp(int(timestamps[0]) / 1000.0, tz=datetime.timezone.utc)
    coverage_end = datetime.datetime.fromtimestamp(int(timestamps[-1]) / 1000.0, tz=datetime.timezone.utc)
    return {
        "available": True,
        "source": "PBGui MarketData",
        "exchange": normalized_exchange,
        "coin": coin_name,
        "requested_start": start_date.isoformat(),
        "requested_end": end_date.isoformat(),
        "coverage_start": coverage_start.isoformat().replace("+00:00", "Z"),
        "coverage_end": coverage_end.isoformat().replace("+00:00", "Z"),
        "coverage_complete": coverage_start.date() <= start_date and coverage_end.date() >= end_date,
        "original_points": original_points,
        "time": [
            datetime.datetime.fromtimestamp(int(value) / 1000.0, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            for value in timestamps.tolist()
        ],
        "close": [float(value) if math.isfinite(float(value)) else None for value in closes.tolist()],
    }
