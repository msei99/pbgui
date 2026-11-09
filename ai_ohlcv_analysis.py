"""Bounded, read-only market candle datasets for namespace-free AI analysis."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import ExitStack
from io import BytesIO
import json
import os
from pathlib import Path
import re
import stat
import zipfile

import numpy as np

SERVICE = "AIAnalysisData"
MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_ROWS = 250_000
_MAX_FILE_BYTES = 1024 * 1024
_EXCHANGES = {"binance": "binanceusdm", "bybit": "bybit", "okx": "okx", "bitget": "bitget", "hyperliquid": "hyperliquid"}


def _read(path: Path, root: Path, limit: int) -> bytes:
    """Reject symlinks, non-regular files and oversized source files."""
    try:
        relative = path.relative_to(root)
        if not relative.parts or any(p in {".", ".."} for p in relative.parts):
            raise ValueError("Invalid analysis source")
        with ExitStack() as stack:
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            fd = os.open(root, flags)
            stack.callback(os.close, fd)
            for part in relative.parts[:-1]:
                fd = os.open(part, flags, dir_fd=fd)
                stack.callback(os.close, fd)
            fd = os.open(relative.parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            stack.callback(os.close, fd)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RuntimeError("Not a regular analysis source")
            chunks, remaining = [], limit + 1
            while remaining:
                chunk = os.read(fd, min(remaining, 65536))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
    except FileNotFoundError:
        raise
    except (OSError, RuntimeError) as exc:
        raise ValueError("Analysis source is unsafe or unreadable") from exc
    if len(raw) > limit:
        raise ValueError("Analysis source exceeds its size limit")
    return raw


def _utc(value: object) -> datetime:
    """Require explicit UTC boundaries; the end is exclusive."""
    if not isinstance(value, str) or len(value) > 32:
        raise ValueError("Candle boundaries must be ISO dates or UTC timestamps")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid candle time boundary") from exc
    if result.tzinfo is None:
        if len(value) != 10:
            raise ValueError("Candle timestamps must specify UTC")
        result = result.replace(tzinfo=timezone.utc)
    if result.utcoffset() != timedelta(0) or result.second or result.microsecond:
        raise ValueError("Candle boundaries must be UTC minute boundaries")
    return result


def candle_dataset(root: Path, args: dict) -> dict:
    """Load exact selected local minutes without network, interpolation or directory scans."""
    exchange = args.get("exchange")
    if not isinstance(exchange, str) or exchange not in _EXCHANGES:
        raise ValueError("Unsupported candle exchange")
    view = args.get("dataset", "1m")
    if view not in ("1m", "1m_api") or (view == "1m_api" and exchange != "hyperliquid"):
        raise ValueError("Unsupported candle dataset")
    coins = args.get("coins")
    if (not isinstance(coins, list) or not 1 <= len(coins) <= 20
            or any(not isinstance(c, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", c) for c in coins)):
        raise ValueError("Select 1 to 20 base coin identifiers")
    coins = [c.upper() for c in coins]
    if len(set(coins)) != len(coins):
        raise ValueError("Duplicate candle coins are not allowed")
    start, end = _utc(args.get("start_time")), _utc(args.get("end_time"))
    minutes = int((end - start).total_seconds() // 60)
    if not 0 < minutes <= 366 * 1440:
        raise ValueError("Select a positive candle interval of at most 366 days")
    if minutes * len(coins) > MAX_ROWS:
        raise ValueError("Candle selection exceeds 250000 minutes; split coins or UTC time ranges")
    storage = _EXCHANGES[exchange]
    mapping_path = root / "coindata" / exchange / "mapping.json"
    # Binance market metadata uses the GUI exchange name, storage uses binanceusdm.
    try:
        mapping = json.loads(_read(mapping_path, root, 16 * 1024 * 1024))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise ValueError("Local candle market mapping is unavailable") from exc
    if not isinstance(mapping, list):
        raise ValueError("Local candle market mapping is invalid")
    series = []
    first_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    for coin in coins:
        markets = {row.get("ccxt_symbol") for row in mapping if isinstance(row, dict)
                   and isinstance(row.get("ccxt_symbol"), str)
                   and str(row.get("base", "")).upper() == coin and row.get("swap") is True
                   and row.get("quote") == ("USDC" if exchange == "hyperliquid" else "USDT")}
        if len(markets) != 1:
            raise ValueError(f"No unique local perpetual market mapping for {coin}")
        symbol = markets.pop()
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_-]+:[A-Za-z0-9_-]+", symbol):
            raise ValueError("Unsafe local candle market mapping")
        directory = root / "ohlcv" / storage / view / symbol.replace("/", "_")
        rows, missing_days = [], []
        day = start.date()
        while day <= (end - timedelta(minutes=1)).date():
            path = directory / f"{day.isoformat()}.npz"
            try:
                raw = _read(path, root, _MAX_FILE_BYTES)
            except FileNotFoundError:
                missing_days.append(day.isoformat())
                day += timedelta(days=1)
                continue
            try:
                with zipfile.ZipFile(BytesIO(raw)) as archive:
                    members = archive.infolist()
                    if len(members) != 1 or members[0].filename != "candles.npy" or members[0].file_size > 256 * 1024:
                        raise ValueError("Invalid candle archive layout or size")
                with np.load(BytesIO(raw), allow_pickle=False) as archive:
                    candles = archive["candles"]
                if candles.ndim != 1 or len(candles) > 1440 or not {"ts", "o", "h", "l", "c", "bv"} <= set(candles.dtype.names or ()):
                    raise ValueError("Invalid daily candle array")
                if candles.dtype['ts'].kind not in 'iu':
                    raise ValueError("Candle timestamps must be integers")
                day_start = int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp() * 1000)
                timestamps = set()
                for item in candles:
                    ts = int(item["ts"])
                    values = [float(item[k]) for k in ("o", "h", "l", "c", "bv")]
                    if (ts in timestamps or ts % 60000 or not day_start <= ts < day_start + 86400000
                            or not np.isfinite(values).all() or min(values[:4]) <= 0 or values[4] < 0
                            or values[1] < max(values[0], values[2], values[3]) or values[2] > min(values[0], values[3])):
                        raise ValueError("Invalid or duplicate candle values")
                    timestamps.add(ts)
                    if first_ms <= ts < end_ms:
                        rows.append([ts, *values])
            except (ValueError, TypeError, OverflowError, KeyError, OSError, zipfile.BadZipFile, EOFError) as exc:
                raise ValueError(f"Invalid local candles for {coin} on {day.isoformat()}") from exc
            day += timedelta(days=1)
        rows.sort(key=lambda row: row[0])
        series.append({"coin": coin, "symbol": symbol, "candles": rows,
                       "coverage": {"expected_minutes": minutes, "available_minutes": len(rows),
                                    "missing_minutes": minutes - len(rows), "missing_days": missing_days}})
    if not any(item["candles"] for item in series):
        raise ValueError("No local candles in the selected interval")
    return {"schema_version": 1, "kind": "ohlcv_candles", "exchange": exchange, "dataset": view,
            "start_time": start.isoformat(), "end_time_exclusive": end.isoformat(),
            "columns": ["timestamp_ms", "open", "high", "low", "close", "base_volume"],
            "interval_ms": 60000, "interpolated": False, "series": series}
