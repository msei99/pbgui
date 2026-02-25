from __future__ import annotations

import json
import os
import configparser
import re
import calendar
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import numpy as np

from logging_helpers import human_log
from market_data_sources import get_source_minutes_for_range
from PBCoinData import get_symbol_for_coin


_DAY_HOUR_RE = re.compile(r"^(\d{8})-(\d{2})\.(lz4|jsonl|npz)$")
_DAY_RE = re.compile(r"^(\d{8})\.(lz4|jsonl|npz)$")
_DAY_DASH_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\.(lz4|jsonl|npz)$")
_HL_COIN_TO_CCXT_SYMBOL_CACHE: dict[str, str] | None = None
_HL_SYMBOL_TO_CCXT_SYMBOL_CACHE: dict[str, str] | None = None


def _normalize_day_str(day: str) -> str:
    s = str(day or "").strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    return ""


def _parse_day_hour_from_filename(name: str) -> tuple[str, str | None] | None:
    s = str(name or "")
    m = _DAY_HOUR_RE.match(s)
    if m:
        return (m.group(1), m.group(2))
    m = _DAY_RE.match(s)
    if m:
        return (m.group(1), None)
    m = _DAY_DASH_RE.match(s)
    if m:
        return (f"{m.group(1)}{m.group(2)}{m.group(3)}", None)
    return None


def _hours_from_npz(path: Path) -> set[str]:
    hours: set[str] = set()
    try:
        with np.load(path) as data:
            arr = data["candles"] if "candles" in data else None
        if arr is None or len(arr) == 0:
            return hours
        ts = arr["ts"].astype("int64", copy=False)
        hour_vals = ((ts // 3_600_000) % 24).astype(int)
        for h in np.unique(hour_vals):
            hours.add(f"{int(h):02d}")
    except Exception:
        return hours
    return hours


def get_market_data_root_dir() -> Path:
    """Root directory for PBGui-managed market data."""

    return (Path(__file__).resolve().parent / "data" / "ohlcv").resolve()


def get_exchange_raw_root_dir(exchange: str) -> Path:
    exchange = str(exchange or "").strip().lower()
    if not exchange:
        raise ValueError("exchange is empty")
    return get_market_data_root_dir() / exchange


def normalize_market_data_dataset(dataset: str) -> str:
    ds = str(dataset or "").strip()
    if ds.lower() == "candles_1m_api":
        return "1m_api"
    return ds


def _format_ccxt_symbol_dir(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return s
    return s.replace("/", "_")


def _get_hyperliquid_ccxt_symbol_for_coin(coin: str) -> str:
    global _HL_COIN_TO_CCXT_SYMBOL_CACHE
    global _HL_SYMBOL_TO_CCXT_SYMBOL_CACHE
    key = str(coin or "").strip().upper()
    if not key:
        return ""

    try:
        if _HL_COIN_TO_CCXT_SYMBOL_CACHE is None:
            cache: dict[str, str] = {}
            sym_cache: dict[str, str] = {}
            mapping_path = Path(__file__).resolve().parent / "data" / "coindata" / "hyperliquid" / "mapping.json"
            if mapping_path.exists():
                rows = json.loads(mapping_path.read_text(encoding="utf-8"))
                for rec in rows if isinstance(rows, list) else []:
                    c = str(rec.get("coin") or "").strip().upper()
                    s = str(rec.get("symbol") or "").strip().upper()
                    ccxt_symbol = str(rec.get("ccxt_symbol") or "").strip().upper()
                    if c and ccxt_symbol:
                        cache[c] = ccxt_symbol
                    if s and ccxt_symbol:
                        sym_cache[s] = ccxt_symbol
            _HL_COIN_TO_CCXT_SYMBOL_CACHE = cache
            _HL_SYMBOL_TO_CCXT_SYMBOL_CACHE = sym_cache
        return str((_HL_COIN_TO_CCXT_SYMBOL_CACHE or {}).get(key) or "")
    except Exception:
        return ""


def _get_hyperliquid_ccxt_symbol_for_market_id(market_id: str) -> str:
    key = str(market_id or "").strip().upper()
    if not key:
        return ""
    try:
        if _HL_SYMBOL_TO_CCXT_SYMBOL_CACHE is None:
            _get_hyperliquid_ccxt_symbol_for_coin("BTC")
        return str((_HL_SYMBOL_TO_CCXT_SYMBOL_CACHE or {}).get(key) or "")
    except Exception:
        return ""


def normalize_market_data_coin_dir(exchange: str, coin: str) -> str:
    ex = str(exchange or "").strip().lower()
    raw = str(coin or "").strip()
    if not raw:
        return ""
    if ex != "hyperliquid":
        return raw.upper()

    raw_base = str(raw).split("/")[0].strip()
    raw_base_l = raw_base.lower()
    if raw_base_l.startswith("xyz:") or raw_base_l.startswith("xyz-"):
        tail = raw_base[4:].strip()
        tail_u = tail.upper()
        for suffix in ("_USDC:USDC", "_USDT:USDT", "_USDC_USDC", "_USDT_USDT"):
            if tail_u.endswith(suffix):
                tail = tail[: -len(suffix)]
                break
        tail = str(tail).strip(" _:-")
        return f"XYZ-{tail.upper()}_USDC:USDC" if tail else ""

    sym = raw
    if str(raw).strip().isdigit():
        mid_symbol = _get_hyperliquid_ccxt_symbol_for_market_id(raw)
        if mid_symbol:
            sym = mid_symbol
    # Only call get_symbol_for_coin if input is NOT already a symbol (doesn't end with USDC/USDT)
    # This prevents kBONKUSDC from becoming kBONKUSDCUSDC
    looks_like_symbol = any(sym.upper().endswith(q) for q in ("USDC", "USDT"))
    
    if "/" not in sym and ":" not in sym and not ("_" in sym and ":" in sym) and not looks_like_symbol:
        try:
            sym = get_symbol_for_coin(sym.upper(), "hyperliquid.swap")
        except Exception:
            sym = raw

    # Hyperliquid mapping stores `symbol` as numeric market id (e.g. BTC -> "0").
    # If reverse lookup returned that id, resolve coin -> ccxt_symbol instead.
    sym_u = str(sym).strip().upper()
    if sym_u.isdigit():
        ccxt_symbol = _get_hyperliquid_ccxt_symbol_for_market_id(sym_u)
        if not ccxt_symbol:
            ccxt_symbol = _get_hyperliquid_ccxt_symbol_for_coin(raw)
        if ccxt_symbol:
            sym = ccxt_symbol
        else:
            sym = raw

    # Normalize and format for directory
    sym = str(sym).strip().upper()
    
    # If already formatted (e.g., BTC_USDC:USDC), return as-is
    if "/" in sym or "_" in sym:
        return _format_ccxt_symbol_dir(sym)
    
    # Strip quote currency from raw symbols (e.g., kBONKUSDC -> kBONK)
    base = sym
    quote = None
    for q in ("USDC", "USDT"):
        if sym.endswith(q) and len(sym) > len(q):
            base = sym[: -len(q)]
            quote = q
            break
    
    # Format as CCXT symbol: BASE/QUOTE:QUOTE
    if quote:
        sym = f"{base}/{quote}:{quote}"
    else:
        # No quote found - treat as base and add USDC (Hyperliquid default)
        sym = f"{base}/USDC:USDC"

    return _format_ccxt_symbol_dir(sym)


def get_exchange_download_log_path(exchange: str) -> Path:
    return get_exchange_raw_root_dir(exchange) / "download.log"


def append_exchange_download_log(exchange: str, line: str, level: str = None) -> None:
    """Write one line to the MarketData log using the standard log format."""

    ex = str(exchange or "").strip().lower()
    tags = ["market_data"]
    if ex:
        tags.append(f"ex:{ex}")
    human_log("MarketData", str(line or "").rstrip("\n"), tags=tags, level=level)


def get_market_data_config_path() -> Path:
    return get_market_data_root_dir() / "config.json"


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class MarketDataConfig:
    """Minimal config: enabled coins per exchange."""

    version: int
    enabled_coins: dict[str, list[str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": int(self.version),
            "enabled_coins": {
                str(ex): [str(c) for c in coins]
                for ex, coins in (self.enabled_coins or {}).items()
            },
        }


def _canonical_enabled_coin(exchange: str, coin: str) -> str:
    ex = str(exchange or "").strip().lower()
    s = str(coin or "").strip()
    if not s:
        return ""
    if ex == "hyperliquid":
        lower = s.lower()
        if lower.startswith("xyz:") or lower.startswith("xyz-"):
            tail = s[4:].strip().upper()
            return f"xyz:{tail}" if tail else ""
    return s.upper()


def load_market_data_config() -> MarketDataConfig:
    path = get_market_data_config_path()
    if not path.exists():
        return MarketDataConfig(version=1, enabled_coins={})
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return MarketDataConfig(version=1, enabled_coins={})

    version = int(raw.get("version", 1)) if isinstance(raw, dict) else 1
    enabled = raw.get("enabled_coins", {}) if isinstance(raw, dict) else {}
    if not isinstance(enabled, dict):
        enabled = {}

    cleaned: dict[str, list[str]] = {}
    for ex, coins in enabled.items():
        if not isinstance(ex, str):
            continue
        if not isinstance(coins, list):
            continue
        ex_key = ex.strip().lower()
        norm_coins = sorted(
            {
                _canonical_enabled_coin(ex_key, c)
                for c in coins
                if _canonical_enabled_coin(ex_key, c)
            }
        )
        cleaned[ex_key] = norm_coins

    return MarketDataConfig(version=version, enabled_coins=cleaned)


def save_market_data_config(cfg: MarketDataConfig) -> None:
    path = get_market_data_config_path()
    payload = json.dumps(cfg.to_dict(), indent=2, sort_keys=True)
    _atomic_write_text(path, payload)


def set_enabled_coins(exchange: str, coins: list[str]) -> MarketDataConfig:
    cfg = load_market_data_config()
    ex = str(exchange or "").strip().lower()
    if not ex:
        raise ValueError("exchange is empty")
    norm_coins = sorted(
        {
            _canonical_enabled_coin(ex, c)
            for c in (coins or [])
            if _canonical_enabled_coin(ex, c)
        }
    )
    cfg.enabled_coins[ex] = norm_coins
    save_market_data_config(cfg)
    return cfg


def summarize_raw_inventory(
    exchange: str,
    *,
    limit: int = 0,
    skip_coverage: bool = False,
    datasets_filter: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return a lightweight inventory of raw files per dataset+coin.

    Scans:
        data/ohlcv/{exchange}/{dataset}/{coin}/*.{lz4,jsonl,npz}

    Returns rows with:
        exchange, dataset, coin, n_files,
        total_bytes,
        oldest_day, newest_day,
        n_days, expected_hours, coverage_pct (0 if skip_coverage=True),
        missing_days_count (0 if skip_coverage=True), missing_days_sample

    Args:
        skip_coverage: If True, skip expensive coverage/missing days calculation for faster initial load.
        datasets_filter: Optional list of dataset names to include (case-insensitive),
            e.g. ["1m", "candles_1m"]. If None, all datasets are scanned.
    """

    ex = str(exchange or "").strip().lower()
    if not ex:
        raise ValueError("exchange is empty")

    base = get_exchange_raw_root_dir(ex)
    if not base.exists():
        return []

    rows: list[dict[str, Any]] = []
    ds_filter = None
    if isinstance(datasets_filter, list) and datasets_filter:
        ds_filter = {str(x).strip().lower() for x in datasets_filter if str(x).strip()}
    datasets = [p for p in base.iterdir() if p.is_dir()]
    for dataset_dir in sorted(datasets, key=lambda p: p.name):
        ds_l = dataset_dir.name.strip().lower()
        if ds_l.endswith("_src"):
            continue
        if ds_filter is not None and ds_l not in ds_filter:
            continue
        for coin_dir in sorted([p for p in dataset_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
            n_files = 0
            total_bytes = 0
            oldest_day = ""
            newest_day = ""
            n_days = 0
            expected_hours = 0
            coverage_pct = 0.0
            missing_days_count = 0
            missing_days_sample = ""
            try:
                oldest_key: tuple[str, str] | None = None
                newest_key: tuple[str, str] | None = None
                days_present: set[str] = set()
                hours_present: set[tuple[str, str]] = set()
                for p in coin_dir.iterdir():
                    if not p.is_file():
                        continue
                    if p.suffix.lower() not in (".lz4", ".jsonl", ".npz"):
                        continue
                    if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api") and p.suffix.lower() != ".npz":
                        continue
                    parsed = _parse_day_hour_from_filename(p.name)
                    if not parsed:
                        continue
                    day, hour = parsed
                    n_files += 1
                    try:
                        total_bytes += int(p.stat().st_size)
                    except Exception:
                        pass
                    days_present.add(day)
                    
                    # Collect hours for coverage calculation
                    # - l2book: hour from filename (20241205-16.lz4)
                    # - 1m_api: minimal - skip detailed hours for speed
                    # - 1m: will use sources.idx later (fast!)
                    if not skip_coverage:
                        if hour is not None:
                            # l2book or other datasets with hour in filename
                            hours_present.add((day, hour))
                        elif ds_l not in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                            # Other NPZ files: read hours from file
                            hours_in_file = _hours_from_npz(p)
                            for hh in hours_in_file:
                                hours_present.add((day, hh))

                    key = (day, hour or "00")
                    if oldest_key is None or key < oldest_key:
                        oldest_key = key
                    if newest_key is None or key > newest_key:
                        newest_key = key

                if oldest_key:
                    oldest_day = oldest_key[0]
                if newest_key:
                    newest_day = newest_key[0]

                if oldest_day and newest_day:
                    try:
                        dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
                        dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
                        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                            today = date.today()
                            if today > dt1:
                                dt1 = today
                        if dt1 >= dt0:
                            n_days = (dt1 - dt0).days + 1
                            expected_hours = int(n_days) * 24
                            
                            # Fast coverage calculation if requested
                            if not skip_coverage:
                                # For 1m dataset: use sources.idx for FAST hour info!
                                if ds_l in ("1m", "candles_1m"):
                                    try:
                                        from market_data_sources import get_daily_source_counts_for_range
                                        counts = get_daily_source_counts_for_range(
                                            exchange=ex,
                                            coin=coin_dir.name,
                                            start_day=oldest_day,
                                            end_day=newest_day,
                                            lag_minutes=0,
                                            cutoff_ts_ms=None,
                                        )
                                        if counts:
                                            # Count hours from sources.idx
                                            total_minutes = 0
                                            for day_data in counts.values():
                                                total_minutes += sum(day_data.values())
                                            total_hours = total_minutes // 60
                                            if expected_hours > 0:
                                                coverage_pct = (total_hours / float(expected_hours)) * 100.0
                                            
                                            # Count missing days (days with < 1440 minutes)
                                            missing_days = []
                                            cur = dt0
                                            while cur <= dt1:
                                                ds = cur.strftime("%Y%m%d")
                                                day_minutes = sum(counts.get(ds, {}).values())
                                                if day_minutes < 1440:
                                                    missing_days.append(ds)
                                                cur = cur + timedelta(days=1)
                                            missing_days_count = len(missing_days)
                                            if missing_days_count:
                                                missing_days_sample = ",".join(missing_days[:10])
                                                if missing_days_count > 10:
                                                    missing_days_sample += ",..."
                                    except Exception:
                                        pass
                                
                                # For 1m_api: minimal - skip detailed coverage for speed
                                elif ds_l in ("1m_api", "candles_1m_api"):
                                    # Just estimate: file count vs expected
                                    if expected_hours > 0 and n_days > 0:
                                        # Rough estimate: each file ~= 1 day
                                        coverage_pct = min(100.0, (n_files / float(n_days)) * 100.0)
                                
                                # For other datasets (l2book): use collected hours
                                else:
                                    if expected_hours > 0:
                                        coverage_pct = (len(hours_present) / float(expected_hours)) * 100.0

                                    # FAST missing days: pre-group hours by day O(n) instead of O(n²)
                                    hours_by_day: dict[str, int] = {}
                                    for (day, _hour) in hours_present:
                                        hours_by_day[day] = hours_by_day.get(day, 0) + 1
                                    
                                    missing_days: list[str] = []
                                    cur = dt0
                                    while cur <= dt1:
                                        ds = cur.strftime("%Y%m%d")
                                        day_hours = hours_by_day.get(ds, 0)
                                        if day_hours < 24:
                                            missing_days.append(ds)
                                        cur = cur + timedelta(days=1)
                                    missing_days_count = len(missing_days)
                                    if missing_days_count:
                                        missing_days_sample = ",".join(missing_days[:10])
                                        if missing_days_count > 10:
                                            missing_days_sample += ",..."
                    except Exception:
                        pass
            except Exception:
                n_files = int(n_files) if n_files else 0
                total_bytes = int(total_bytes) if total_bytes else 0
            rows.append(
                {
                    "exchange": ex,
                    "dataset": dataset_dir.name,
                    "coin": coin_dir.name,
                    "n_files": int(n_files),
                    "total_bytes": int(total_bytes),
                    "oldest_day": oldest_day,
                    "newest_day": newest_day,
                    "n_days": int(n_days),
                    "expected_hours": int(expected_hours),
                    "coverage_pct": float(round(coverage_pct, 2)),
                    "missing_days_count": int(missing_days_count),
                    "missing_days_sample": missing_days_sample,
                }
            )
            if limit and len(rows) >= int(limit):
                return rows
    return rows


def _get_pb7_root_dir(pb7_root: str | Path | None = None) -> Path | None:
    if pb7_root:
        try:
            p = Path(pb7_root).expanduser().resolve()
            return p if p.exists() else None
        except Exception:
            return None

    try:
        cfg = configparser.ConfigParser()
        cfg.read(Path(__file__).resolve().parent / "pbgui.ini")
        ini_val = str(cfg.get("main", "pb7dir", fallback="") or "").strip()
        if ini_val:
            p = Path(ini_val).expanduser().resolve()
            if p.exists():
                return p
    except Exception:
        pass

    try:
        p = (Path(__file__).resolve().parents[1] / "pb7").resolve()
        return p if p.exists() else None
    except Exception:
        return None


def _parse_pb7_cache_day_from_name(name: str) -> str:
    s = str(name or "")
    stem = Path(s).stem
    try:
        if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
            return stem.replace("-", "")
        if len(stem) == 8 and stem.isdigit():
            return stem
    except Exception:
        pass
    return ""


def summarize_pb7_cache_inventory(
    exchange: str,
    *,
    pb7_root: str | Path | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    ex = str(exchange or "").strip().lower()
    if not ex:
        raise ValueError("exchange is empty")

    root = _get_pb7_root_dir(pb7_root)
    if root is None:
        return []

    base = root / "caches" / "ohlcv" / ex
    if not base.exists() or not base.is_dir():
        return []

    rows: list[dict[str, Any]] = []
    for timeframe_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name):
        tf = str(timeframe_dir.name)
        for coin_dir in sorted([p for p in timeframe_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
            n_files = 0
            total_bytes = 0
            days_present: set[str] = set()
            for f in coin_dir.iterdir():
                if not f.is_file():
                    continue
                if f.suffix.lower() != ".npy":
                    continue
                day = _parse_pb7_cache_day_from_name(f.name)
                if not day:
                    continue
                n_files += 1
                days_present.add(day)
                try:
                    total_bytes += int(f.stat().st_size)
                except Exception:
                    pass

            if n_files <= 0:
                continue

            oldest_day = ""
            newest_day = ""
            n_days = 0
            try:
                oldest_day = min(days_present) if days_present else ""
                newest_day = max(days_present) if days_present else ""
                if oldest_day and newest_day:
                    dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
                    dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
                    if dt1 >= dt0:
                        n_days = (dt1 - dt0).days + 1
            except Exception:
                pass

            rows.append(
                {
                    "exchange": ex,
                    "timeframe": tf,
                    "coin": coin_dir.name,
                    "n_files": int(n_files),
                    "total_bytes": int(total_bytes),
                    "oldest_day": oldest_day,
                    "newest_day": newest_day,
                    "n_days": int(n_days),
                }
            )
            if limit and len(rows) >= int(limit):
                return rows

    return rows


def get_daily_presence_for_pb7_cache(
    exchange: str,
    timeframe: str,
    coin: str,
    *,
    pb7_root: str | Path | None = None,
    start_day: str | None = None,
    end_day: str | None = None,
) -> dict[str, Any]:
    """Return per-day presence for PB7 cache files (status 0/2)."""

    ex = str(exchange or "").strip().lower()
    tf = str(timeframe or "").strip()
    cn = str(coin or "").strip()
    if not ex or not tf or not cn:
        return {"oldest_day": "", "newest_day": "", "days": []}

    root = _get_pb7_root_dir(pb7_root)
    if root is None:
        return {"oldest_day": "", "newest_day": "", "days": []}

    base = root / "caches" / "ohlcv" / ex / tf / cn
    if not base.exists() or not base.is_dir():
        return {"oldest_day": "", "newest_day": "", "days": []}

    days_present: set[str] = set()
    for p in base.iterdir():
        if not p.is_file() or p.suffix.lower() != ".npy":
            continue
        day = _parse_pb7_cache_day_from_name(p.name)
        if day:
            days_present.add(day)

    if not days_present:
        return {"oldest_day": "", "newest_day": "", "days": []}

    try:
        data_oldest = min(days_present)
        data_newest = max(days_present)
        dt0 = datetime.strptime(data_oldest, "%Y%m%d").date()
        dt1 = datetime.strptime(data_newest, "%Y%m%d").date()

        if start_day:
            s0 = _normalize_day_str(start_day)
            if re.fullmatch(r"\d{8}", s0):
                dt0 = datetime.strptime(s0, "%Y%m%d").date()
        if end_day:
            s1 = _normalize_day_str(end_day)
            if re.fullmatch(r"\d{8}", s1):
                dt1 = datetime.strptime(s1, "%Y%m%d").date()

        if dt1 < dt0:
            return {"oldest_day": "", "newest_day": "", "days": []}
    except Exception:
        return {"oldest_day": "", "newest_day": "", "days": []}

    days: list[dict[str, Any]] = []
    cur = dt0
    while cur <= dt1:
        ds = cur.strftime("%Y%m%d")
        present = ds in days_present
        days.append(
            {
                "day": ds,
                "hours": 24 if present else 0,
                "status": 2 if present else 0,
            }
        )
        cur = cur + timedelta(days=1)

    return {"oldest_day": dt0.strftime("%Y%m%d"), "newest_day": dt1.strftime("%Y%m%d"), "days": days}


def get_daily_presence_for_dataset(exchange: str, dataset: str, coin: str) -> dict[str, Any]:
    """Return day-by-day presence for a dataset/coin.

    Output keys: oldest_day, newest_day, days=[{day, present}]
    """

    ex = str(exchange or "").strip().lower()
    ds = normalize_market_data_dataset(dataset)
    cn = normalize_market_data_coin_dir(ex, coin)
    ds_l = ds.strip().lower()
    if not ex or not ds or not cn:
        return {"oldest_day": "", "newest_day": "", "days": []}

    base = get_exchange_raw_root_dir(ex) / ds / cn
    if not base.exists():
        return {"oldest_day": "", "newest_day": "", "days": []}

    days_present: set[str] = set()
    oldest_day = ""
    newest_day = ""

    for p in base.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".lz4", ".jsonl", ".npz"):
            continue
        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api") and p.suffix.lower() != ".npz":
            continue
        parsed = _parse_day_hour_from_filename(p.name)
        if not parsed:
            continue
        day, hour = parsed
        days_present.add(day)

    if not days_present:
        return {"oldest_day": "", "newest_day": "", "days": []}

    try:
        oldest_day = min(days_present)
        newest_day = max(days_present)
        dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
        dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
    except Exception:
        return {"oldest_day": "", "newest_day": "", "days": []}

    days: list[dict[str, Any]] = []
    cur = dt0
    while cur <= dt1:
        ds = cur.strftime("%Y%m%d")
        days.append({"day": ds, "present": ds in days_present})
        cur = cur + timedelta(days=1)

    return {"oldest_day": oldest_day, "newest_day": newest_day, "days": days}


def get_daily_hour_coverage_for_dataset(
    exchange: str,
    dataset: str,
    coin: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
) -> dict[str, Any]:
    """Return per-day hour coverage and status (0=missing, 1=partial, 2=full).

    If start_day/end_day are provided (YYYYMMDD), coverage is limited to that
    inclusive range.

    Output keys: oldest_day, newest_day, days=[{day, hours, status}]
    """

    ex = str(exchange or "").strip().lower()
    ds = normalize_market_data_dataset(dataset)
    cn = normalize_market_data_coin_dir(ex, coin)
    ds_l = ds.strip().lower()
    if not ex or not ds or not cn:
        return {"oldest_day": "", "newest_day": "", "days": []}

    base = get_exchange_raw_root_dir(ex) / ds / cn
    if not base.exists():
        return {"oldest_day": "", "newest_day": "", "days": []}

    hours_present: dict[str, set[str]] = {}
    for p in base.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".lz4", ".jsonl", ".npz"):
            continue
        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api") and p.suffix.lower() != ".npz":
            continue
        parsed = _parse_day_hour_from_filename(p.name)
        if not parsed:
            continue
        day, hour = parsed
        if hour is None and p.suffix.lower() == ".npz":
            hours_present.setdefault(day, set()).update(_hours_from_npz(p))
        elif hour is not None:
            hours_present.setdefault(day, set()).add(hour)

    if not hours_present:
        return {"oldest_day": "", "newest_day": "", "days": []}

    try:
        data_oldest = min(hours_present.keys())
        data_newest = max(hours_present.keys())
        data_dt0 = datetime.strptime(data_oldest, "%Y%m%d").date()
        data_dt1 = datetime.strptime(data_newest, "%Y%m%d").date()

        dt0 = data_dt0
        dt1 = data_dt1
        if start_day:
            s0 = _normalize_day_str(start_day)
            if re.fullmatch(r"\d{8}", s0):
                dt0 = datetime.strptime(s0, "%Y%m%d").date()
        if end_day:
            s1 = _normalize_day_str(end_day)
            if re.fullmatch(r"\d{8}", s1):
                dt1 = datetime.strptime(s1, "%Y%m%d").date()

        if dt1 < dt0:
            return {"oldest_day": "", "newest_day": "", "days": []}
    except Exception:
        return {"oldest_day": "", "newest_day": "", "days": []}

    days: list[dict[str, Any]] = []
    cur = dt0
    while cur <= dt1:
        ds = cur.strftime("%Y%m%d")
        hrs = len(hours_present.get(ds, set()))
        if hrs >= 24:
            status = 2
        elif hrs > 0:
            status = 1
        else:
            status = 0
        days.append({"day": ds, "hours": int(hrs), "status": int(status)})
        cur = cur + timedelta(days=1)

    return {"oldest_day": dt0.strftime("%Y%m%d"), "newest_day": dt1.strftime("%Y%m%d"), "days": days}


def get_monthly_hour_coverage_for_dataset(
    exchange: str,
    dataset: str,
    coin: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
) -> dict[str, Any]:
    """Return per-month hour coverage and status (0=missing, 1=partial, 2=full).

    Months are derived from daily hour files (YYYYMMDD-HH.*).
    If start_day/end_day are provided (YYYYMMDD), coverage is limited to that
    inclusive range.

    Output keys: oldest_month, newest_month, months=[{month, hours, expected_hours, status}]
    where month is YYYYMM.
    """

    cov = get_daily_hour_coverage_for_dataset(
        exchange,
        dataset,
        coin,
        start_day=start_day,
        end_day=end_day,
    )
    days = cov.get("days") if isinstance(cov, dict) else []
    if not isinstance(days, list) or not days:
        return {"oldest_month": "", "newest_month": "", "months": []}

    # Determine range from cov oldest/newest
    oldest_day = str(cov.get("oldest_day") or "")
    newest_day = str(cov.get("newest_day") or "")
    if not (re.fullmatch(r"\d{8}", oldest_day) and re.fullmatch(r"\d{8}", newest_day)):
        return {"oldest_month": "", "newest_month": "", "months": []}

    try:
        dt0 = datetime.strptime(oldest_day, "%Y%m%d").date()
        dt1 = datetime.strptime(newest_day, "%Y%m%d").date()
    except Exception:
        return {"oldest_month": "", "newest_month": "", "months": []}

    # Map day->hours for quick lookup
    day_hours: dict[str, int] = {}
    for d in days:
        if not isinstance(d, dict):
            continue
        ds = str(d.get("day") or "")
        if not re.fullmatch(r"\d{8}", ds):
            continue
        try:
            day_hours[ds] = int(d.get("hours") or 0)
        except Exception:
            day_hours[ds] = 0

    months_out: list[dict[str, Any]] = []
    cur = dt0.replace(day=1)
    end_month = dt1.replace(day=1)
    while cur <= end_month:
        y = cur.year
        m = cur.month
        month_key = f"{y:04d}{m:02d}"
        _, dim = calendar.monthrange(y, m)

        # Only count days inside [dt0, dt1]
        month_start = cur
        month_end = cur.replace(day=dim)
        if month_start < dt0:
            month_start = dt0
        if month_end > dt1:
            month_end = dt1

        total_hours = 0
        expected = 0
        dcur = month_start
        while dcur <= month_end:
            ds = dcur.strftime("%Y%m%d")
            expected += 24
            total_hours += int(day_hours.get(ds, 0))
            dcur = dcur + timedelta(days=1)

        if total_hours >= expected and expected > 0:
            status = 2
        elif total_hours > 0:
            status = 1
        else:
            status = 0

        months_out.append(
            {
                "month": month_key,
                "hours": int(total_hours),
                "expected_hours": int(expected),
                "status": int(status),
            }
        )

        # next month
        if m == 12:
            cur = cur.replace(year=y + 1, month=1, day=1)
        else:
            cur = cur.replace(month=m + 1, day=1)

    if not months_out:
        return {"oldest_month": "", "newest_month": "", "months": []}
    return {
        "oldest_month": str(months_out[0].get("month") or ""),
        "newest_month": str(months_out[-1].get("month") or ""),
        "months": months_out,
    }


def get_hour_presence_for_dataset(
    exchange: str,
    dataset: str,
    coin: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
) -> dict[str, Any]:
    """Return per-day set of present hours.

    Scans files named like YYYYMMDD-HH.(lz4|jsonl|npz) or YYYY-MM-DD.npz.

    If start_day/end_day are provided (YYYYMMDD), only days within that range
    are returned (inclusive).

    Output keys: oldest_day, newest_day, days={YYYYMMDD: [HH, ...]}
    """

    ex = str(exchange or "").strip().lower()
    ds = normalize_market_data_dataset(dataset)
    cn = normalize_market_data_coin_dir(ex, coin)
    ds_l = ds.strip().lower()
    if not ex or not ds or not cn:
        return {"oldest_day": "", "newest_day": "", "days": {}}

    base = get_exchange_raw_root_dir(ex) / ds / cn
    if not base.exists():
        return {"oldest_day": "", "newest_day": "", "days": {}}

    s0 = _normalize_day_str(start_day) if start_day else ""
    s1 = _normalize_day_str(end_day) if end_day else ""

    out: dict[str, set[str]] = {}
    for p in base.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".lz4", ".jsonl", ".npz"):
            continue
        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api") and p.suffix.lower() != ".npz":
            continue
        parsed = _parse_day_hour_from_filename(p.name)
        if not parsed:
            continue
        day, hour = parsed
        if s0 and day < s0:
            continue
        if s1 and day > s1:
            continue
        if hour is None and p.suffix.lower() == ".npz":
            hours = _hours_from_npz(p)
            if hours:
                out.setdefault(day, set()).update(hours)
        elif hour is not None:
            out.setdefault(day, set()).add(hour)

    if not out:
        return {"oldest_day": "", "newest_day": "", "days": {}}

    oldest = min(out.keys())
    newest = max(out.keys())
    return {
        "oldest_day": oldest,
        "newest_day": newest,
        "days": {k: sorted(v) for k, v in out.items()},
    }


def get_minute_presence_for_dataset(
    exchange: str,
    dataset: str,
    coin: str,
    *,
    start_day: str | None = None,
    end_day: str | None = None,
) -> dict[str, Any]:
    """Return per-day/per-hour set of present minutes for JSONL/NPZ candle datasets.

    Expects files named like YYYYMMDD-HH.jsonl, where each line is a JSON candle
    containing a timestamp key 't' in milliseconds, or YYYY-MM-DD.npz with a
    PB7-structured array named 'candles' containing a 'ts' timestamp in ms.

    If start_day/end_day are provided (YYYYMMDD), only days within that range
    are returned (inclusive).

    Output keys:
      oldest_day, newest_day,
      days={YYYYMMDD: {HH: [MM,...]}}
    """

    ex = str(exchange or "").strip().lower()
    ds = normalize_market_data_dataset(dataset)
    cn = normalize_market_data_coin_dir(ex, coin)
    ds_l = ds.strip().lower()
    if not ex or not ds or not cn:
        return {"oldest_day": "", "newest_day": "", "days": {}}

    base = get_exchange_raw_root_dir(ex) / ds / cn
    if not base.exists():
        return {"oldest_day": "", "newest_day": "", "days": {}}

    s0 = _normalize_day_str(start_day) if start_day else ""
    s1 = _normalize_day_str(end_day) if end_day else ""

    if ds_l in ("1m", "candles_1m") and ex == "hyperliquid":
        idx_days = get_source_minutes_for_range(
            exchange=ex,
            coin=cn,
            start_day=s0 or None,
            end_day=s1 or None,
        )
        if idx_days:
            oldest = min(idx_days.keys())
            newest = max(idx_days.keys())
            return {"oldest_day": oldest, "newest_day": newest, "days": idx_days}

    # Return per-minute source mapping: days -> hours -> {minute: src}
    out: dict[str, dict[str, dict[int, str]]] = {}
    for p in base.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".jsonl", ".npz"):
            continue
        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api") and p.suffix.lower() != ".npz":
            continue
        parsed = _parse_day_hour_from_filename(p.name)
        if not parsed:
            continue
        day, hour = parsed
        if day and (s0 and day < s0):
            continue
        if day and (s1 and day > s1):
            continue

        minutes: dict[int, str] = {}
        try:
            if p.suffix.lower() == ".jsonl":
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        if not isinstance(obj, dict):
                            continue
                        t = obj.get("t")
                        if t is None:
                            continue
                        try:
                            ts_ms = int(t)
                        except Exception:
                            continue
                        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                        minute = int(dt.minute)

                        # Determine source label for this minute
                        src = None
                        if isinstance(obj.get("src"), str) and obj.get("src"):
                            src = str(obj.get("src"))
                        else:
                            # default source inference: if dataset is an API dataset, mark as 'api'
                            ds_l = ds.strip().lower()
                            if ds_l.endswith("_api") or ds_l in ("1m_api", "candles_1m_api", "candles_1m", "1m"):
                                src = "api"
                            else:
                                src = "unknown"

                        minutes[minute] = src
            else:
                ds_l = ds.strip().lower()
                if ds_l == "1m_api" or ds_l == "candles_1m_api" or ds_l.endswith("_api"):
                    src = "api"
                elif ds_l in ("candles_1m", "1m"):
                    src = "best"
                else:
                    src = "unknown"
                with np.load(p) as data:
                    arr = data["candles"] if "candles" in data else None
                if arr is not None:
                    for row in arr:
                        try:
                            ts_ms = int(row["ts"])
                        except Exception:
                            continue
                        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                        day_s = dt.strftime("%Y%m%d")
                        if s0 and day_s < s0:
                            continue
                        if s1 and day_s > s1:
                            continue
                        hour_s = f"{int(dt.hour):02d}"
                        out.setdefault(day_s, {}).setdefault(hour_s, {})[int(dt.minute)] = src
        except Exception:
            minutes = {}

        if p.suffix.lower() == ".jsonl" and hour is not None:
            out.setdefault(day, {})[hour] = minutes

    if not out:
        return {"oldest_day": "", "newest_day": "", "days": {}}

    oldest = min(out.keys())
    newest = max(out.keys())
    return {
        "oldest_day": oldest,
        "newest_day": newest,
        "days": {
            d: {
                h: {m: minutes[m] for m in sorted(minutes.keys())}
                for h, minutes in sorted(hh.items())
            }
            for d, hh in sorted(out.items())
        },
    }


def get_aws_credentials_path() -> Path:
    return (Path.home() / ".aws" / "credentials").expanduser()


def get_aws_config_path() -> Path:
    return (Path.home() / ".aws" / "config").expanduser()


def load_aws_profile_credentials(profile: str) -> dict[str, str]:
    """Load credentials for a given AWS profile from ~/.aws/credentials.

    Returns keys:
        aws_access_key_id, aws_secret_access_key
    """

    prof = str(profile or "").strip()
    if not prof:
        raise ValueError("profile is empty")

    path = get_aws_credentials_path()
    if not path.exists():
        return {}

    cp = configparser.RawConfigParser()
    cp.read(path)
    if not cp.has_section(prof):
        return {}

    out: dict[str, str] = {}
    for k in ("aws_access_key_id", "aws_secret_access_key"):
        if cp.has_option(prof, k):
            out[k] = cp.get(prof, k)
    return out


def save_aws_profile_credentials(
    *,
    profile: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
) -> None:
    prof = str(profile or "").strip()
    if not prof:
        raise ValueError("profile is empty")
    if not str(aws_access_key_id or "").strip() or not str(aws_secret_access_key or "").strip():
        raise ValueError("AWS access key id and secret access key are required")

    path = get_aws_credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    cp = configparser.RawConfigParser()
    if path.exists():
        cp.read(path)
    if not cp.has_section(prof):
        cp.add_section(prof)
    cp.set(prof, "aws_access_key_id", str(aws_access_key_id).strip())
    cp.set(prof, "aws_secret_access_key", str(aws_secret_access_key).strip())

    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        cp.write(f)
    os.replace(tmp, path)


def load_aws_profile_region(profile: str) -> str:
    """Load region for a given profile from ~/.aws/config.

    AWS config uses sections like:
        [profile myprofile]
        region = us-east-1
    """

    prof = str(profile or "").strip()
    if not prof:
        raise ValueError("profile is empty")

    path = get_aws_config_path()
    if not path.exists():
        return ""

    cp = configparser.RawConfigParser()
    cp.read(path)

    section = "default" if prof == "default" else f"profile {prof}"
    if not cp.has_section(section):
        return ""
    if not cp.has_option(section, "region"):
        return ""
    return str(cp.get(section, "region") or "").strip()


def save_aws_profile_region(*, profile: str, region: str) -> None:
    prof = str(profile or "").strip()
    if not prof:
        raise ValueError("profile is empty")
    reg = str(region or "").strip()
    if not reg:
        raise ValueError("region is empty")

    path = get_aws_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    cp = configparser.RawConfigParser()
    if path.exists():
        cp.read(path)

    section = "default" if prof == "default" else f"profile {prof}"
    if not cp.has_section(section):
        cp.add_section(section)
    cp.set(section, "region", reg)

    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        cp.write(f)
    os.replace(tmp, path)
