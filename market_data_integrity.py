"""OHLCV integrity catalog, validation, snapshots, and read-only comparison."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import numpy as np

from file_lock import advisory_file_lock
from market_data import get_exchange_raw_root_dir, get_market_data_root_dir, normalize_market_data_coin_dir
from market_data_sources import SOURCE_CODE_OTHER, get_source_codes_for_day
from market_symbol_mapping import disambiguate_multiplier_market_coins
from secure_files import atomic_write_private_text, ensure_private_directory, secure_private_file


SCHEMA_VERSION = 1
INITIAL_SCAN_VERSION = 1
TIMEFRAME = "1m"
SUPPORTED_EXCHANGES = ("binanceusdm", "bybit", "okx", "bitget", "hyperliquid")
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BYBIT_MAPPING_STATUS_SIG: tuple[str, int, int] | None = None
_BYBIT_MAPPING_ACTIVE: frozenset[str] = frozenset()
_BYBIT_MAPPING_INACTIVE: frozenset[str] = frozenset()
_BYBIT_MAPPING_ERROR = ""
_MAPPING_STATUS_CACHE: dict[tuple[str, str, int, int], tuple[frozenset[str], frozenset[str], str]] = {}
_KNOWN_SOURCE_GAPS: dict[tuple[str, str, str], frozenset[int]] = {
    ("binanceusdm", "BTC_USDT:USDT", "2019-09-08"): frozenset({1140}),
    ("bybit", "XTZ_USDT:USDT", "2021-01-11"): frozenset({350}),
}
_CANONICAL_DTYPE = np.dtype(
    [
        ("ts", "<i8"),
        ("o", "<f4"),
        ("h", "<f4"),
        ("l", "<f4"),
        ("c", "<f4"),
        ("bv", "<f4"),
    ]
)


@dataclass(frozen=True)
class DayValidation:
    """Validated metadata for one daily OHLCV file."""

    status: str
    candles: int
    missing_minutes: int
    sha256: str
    first_ts: int | None
    last_ts: int | None
    error: str = ""

    @property
    def valid(self) -> bool:
        """Return whether this day is accepted as complete or expected partial data."""
        return self.status in {"valid", "inception_partial", "terminal_partial", "source_gap", "current"}


def checksum_database_path(root: Path | None = None) -> Path:
    """Return the local integrity catalog path."""
    base = Path(root) if root is not None else get_market_data_root_dir()
    return base / "checksums.sqlite"


def reference_database_path(root: Path | None = None) -> Path:
    """Return the installed read-only reference catalog path."""
    base = Path(root) if root is not None else get_market_data_root_dir()
    return base / "reference" / "checksums.sqlite"


def catalog_operation_lock(db_path: Path | None = None):
    """Return the shared lock for catalog mutations and consistent snapshots."""
    path = Path(db_path) if db_path is not None else checksum_database_path()
    return advisory_file_lock(path.with_suffix(path.suffix + ".operation"))


def integrity_job_lock(db_path: Path | None = None):
    """Serialize long-running scan, repair, and removed-coin mutation jobs."""
    path = Path(db_path) if db_path is not None else checksum_database_path()
    return advisory_file_lock(path.with_suffix(path.suffix + ".jobs"))


def reference_operation_lock(path: Path | None = None):
    """Return the shared lock for reference installation and comparison."""
    database = Path(path) if path is not None else reference_database_path()
    return advisory_file_lock(database.parent / ".install")


def _validate_exchange(exchange: str) -> str:
    ex = str(exchange or "").strip().lower()
    if ex not in SUPPORTED_EXCHANGES:
        raise ValueError(f"Unsupported integrity exchange: {ex or 'empty'}")
    return ex


def _require_bybit_mutation(exchange: str) -> str:
    """Validate that a mutation uses the only write-enabled integrity exchange."""
    ex = _validate_exchange(exchange)
    if ex != "bybit":
        raise ValueError("Integrity repair and removal are currently supported only for Bybit")
    return ex


def _validate_day(day: str | date) -> tuple[str, date]:
    if isinstance(day, date):
        day_obj = day
        day_s = day.strftime("%Y-%m-%d")
    else:
        day_s = str(day or "").strip()
        if not _DAY_RE.fullmatch(day_s):
            raise ValueError("Invalid OHLCV day")
        day_obj = datetime.strptime(day_s, "%Y-%m-%d").date()
    return day_s, day_obj


def _validate_coin(coin: str) -> str:
    value = str(coin or "").strip()
    if not value or value in {".", ".."} or any(ch in value for ch in ("/", "\\", "\x00")):
        raise ValueError("Invalid OHLCV coin")
    if any(ord(ch) < 32 for ch in value):
        raise ValueError("Invalid OHLCV coin")
    return value


def known_source_gap_minutes(exchange: str, coin: str, day: str | date) -> frozenset[int]:
    """Return independently verified exchange-native missing minutes for one market day."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin).upper()
    day_s, _ = _validate_day(day)
    return _KNOWN_SOURCE_GAPS.get((ex, coin_s, day_s), frozenset())


def bybit_storage_market_status(coin: str, *, mapping_path: Path | None = None) -> dict[str, str]:
    """Classify one Bybit storage coin against the current local symbol mapping."""
    global _BYBIT_MAPPING_STATUS_SIG, _BYBIT_MAPPING_ACTIVE, _BYBIT_MAPPING_INACTIVE, _BYBIT_MAPPING_ERROR
    coin_s = _validate_coin(coin).upper()
    path = Path(mapping_path) if mapping_path is not None else Path(__file__).resolve().parent / "data" / "coindata" / "bybit" / "mapping.json"
    if not path.is_file() or path.is_symlink():
        return {"status": "unknown", "reason": "Bybit mapping is unavailable"}
    stat = path.stat()
    signature = (str(path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))
    if signature != _BYBIT_MAPPING_STATUS_SIG:
        active: set[str] = set()
        inactive: set[str] = set()
        error = ""
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                raise ValueError("invalid mapping")
            for row in raw:
                if not isinstance(row, dict):
                    continue
                if not bool(row.get("swap")) or str(row.get("quote") or "").strip().upper() != "USDT":
                    continue
                symbol = str(row.get("symbol") or "").strip().upper()
                if not symbol.endswith("USDT") or len(symbol) <= 4:
                    continue
                storage_coin = f"{symbol[:-4]}_USDT:USDT"
                (active if bool(row.get("active", True)) else inactive).add(storage_coin)
            if not active and not inactive:
                error = "Bybit mapping has no eligible USDT perpetual markets"
        except (OSError, ValueError):
            error = "Bybit mapping could not be read"
        _BYBIT_MAPPING_ACTIVE = frozenset(active)
        _BYBIT_MAPPING_INACTIVE = frozenset(inactive)
        _BYBIT_MAPPING_ERROR = error
        _BYBIT_MAPPING_STATUS_SIG = signature

    if _BYBIT_MAPPING_ERROR:
        return {"status": "unknown", "reason": _BYBIT_MAPPING_ERROR}
    if coin_s in _BYBIT_MAPPING_ACTIVE:
        return {"status": "available", "reason": "Market is active in the Bybit mapping"}
    if coin_s in _BYBIT_MAPPING_INACTIVE:
        return {"status": "removed", "reason": "Market is marked inactive in the Bybit mapping"}
    return {"status": "removed", "reason": "Market is absent from the current Bybit mapping"}


def storage_market_status(
    exchange: str,
    coin: str,
    *,
    mapping_path: Path | None = None,
) -> dict[str, str]:
    """Classify one storage coin against its exchange's current local mapping."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin).upper()
    if ex == "bybit":
        return bybit_storage_market_status(coin_s, mapping_path=mapping_path)
    mapping_exchange = "binance" if ex == "binanceusdm" else ex
    path = Path(mapping_path) if mapping_path is not None else Path(__file__).resolve().parent / "data" / "coindata" / mapping_exchange / "mapping.json"
    label = "Binance USDM" if ex == "binanceusdm" else ex.capitalize()
    if not path.is_file() or path.is_symlink():
        return {"status": "unknown", "reason": f"{label} mapping is unavailable"}
    stat = path.stat()
    cache_key = (ex, str(path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))
    cached = _MAPPING_STATUS_CACHE.get(cache_key)
    if cached is None:
        active: set[str] = set()
        inactive: set[str] = set()
        error = ""
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                raise ValueError("invalid mapping")
            for row in raw:
                if not isinstance(row, dict) or not bool(row.get("swap")) or not bool(row.get("linear", True)):
                    continue
                quote = str(row.get("quote") or "").strip().upper()
                expected_quote = "USDC" if ex == "hyperliquid" else "USDT"
                if quote != expected_quote:
                    continue
                if ex == "hyperliquid":
                    source = str(row.get("ccxt_symbol") or row.get("coin") or "").strip()
                    storage_coin = normalize_market_data_coin_dir(ex, source)
                else:
                    symbol = str(row.get("ccxt_symbol") or "").strip()
                    base = symbol.split("/", 1)[0].strip() if "/" in symbol else ""
                    storage_coin = f"{base.upper()}_USDT:USDT" if base else ""
                if not storage_coin:
                    continue
                (active if bool(row.get("active", True)) else inactive).add(storage_coin.upper())
            inactive.difference_update(active)
            if not active and not inactive:
                error = f"{label} mapping has no eligible perpetual markets"
        except (OSError, ValueError):
            error = f"{label} mapping could not be read"
        cached = (frozenset(active), frozenset(inactive), error)
        _MAPPING_STATUS_CACHE.clear()
        _MAPPING_STATUS_CACHE[cache_key] = cached
    active, inactive, error = cached
    if error:
        return {"status": "unknown", "reason": error}
    if coin_s in active:
        return {"status": "available", "reason": f"Market is active in the {label} mapping"}
    if coin_s in inactive:
        return {"status": "removed", "reason": f"Market is marked inactive in the {label} mapping"}
    return {"status": "removed", "reason": f"Market is absent from the current {label} mapping"}


def repair_coin_from_storage(exchange: str, coin: str, *, mapping_path: Path | None = None) -> str:
    """Resolve an on-disk coin directory back to the builder's mapping coin."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin).upper()
    if ex == "hyperliquid":
        return coin_s.split("_", 1)[0]
    mapping_exchange = "binance" if ex == "binanceusdm" else ex
    path = Path(mapping_path) if mapping_path is not None else Path(__file__).resolve().parent / "data" / "coindata" / mapping_exchange / "mapping.json"
    if path.is_file() and not path.is_symlink():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            for row in disambiguate_multiplier_market_coins(raw if isinstance(raw, list) else []):
                if not isinstance(row, dict) or not bool(row.get("swap")) or not bool(row.get("linear", True)):
                    continue
                quote = str(row.get("quote") or "").strip().upper()
                if quote != "USDT":
                    continue
                symbol = str(row.get("ccxt_symbol") or "").strip()
                base = symbol.split("/", 1)[0].strip().upper() if "/" in symbol else ""
                if base and f"{base}_USDT:USDT" == coin_s:
                    mapped_coin = str(row.get("coin") or row.get("base") or base).strip()
                    if mapped_coin:
                        return mapped_coin.upper()
        except (OSError, ValueError):
            pass
    suffix = "_USDT:USDT"
    return coin_s[:-len(suffix)] if coin_s.endswith(suffix) else coin_s.split("_", 1)[0]


def unavailable_coin_data_preview(
    *,
    exchange: str,
    coin: str,
    data_root: Path | None = None,
    mapping_path: Path | None = None,
) -> dict[str, Any]:
    """Return an exchange-neutral preview for unavailable local OHLCV data."""
    return _removed_coin_data_footprint(
        exchange=exchange,
        coin=coin,
        data_root=data_root,
        mapping_path=mapping_path,
    )


def removed_coin_data_preview(
    *,
    exchange: str,
    coin: str,
    data_root: Path | None = None,
    mapping_path: Path | None = None,
) -> dict[str, Any]:
    """Return the safely removable PBGui OHLCV footprint for one removed coin."""
    ex = _require_bybit_mutation(exchange)
    return _removed_coin_data_footprint(
        exchange=ex,
        coin=coin,
        data_root=data_root,
        mapping_path=mapping_path,
    )


def _removed_coin_data_footprint(
    *,
    exchange: str,
    coin: str,
    data_root: Path | None = None,
    mapping_path: Path | None = None,
) -> dict[str, Any]:
    """Return the local footprint for one market confirmed unavailable by mapping."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    market = storage_market_status(ex, coin_s, mapping_path=mapping_path)
    if market["status"] != "removed":
        raise ValueError(market["reason"])
    root = Path(data_root) if data_root is not None else get_exchange_raw_root_dir(ex)
    root_resolved = root.resolve(strict=False)
    files = 0
    total_bytes = 0
    from_day = ""
    to_day = ""
    directories: list[str] = []
    for dataset in ("1m", "candles_1m", "1m_api", "candles_1m_api", "1m_src"):
        target = root / dataset / coin_s
        target_resolved = target.resolve(strict=False)
        if target_resolved.parent != (root_resolved / dataset).resolve(strict=False):
            raise ValueError("Invalid removed-coin data path")
        if target.is_symlink():
            raise ValueError("Refusing to remove a symlinked coin directory")
        if not target.is_dir():
            continue
        directories.append(dataset)
        for entry in target.rglob("*"):
            if entry.is_symlink():
                raise ValueError("Refusing to remove coin data containing symlinks")
            if entry.is_file():
                files += 1
                total_bytes += int(entry.stat().st_size)
                if dataset != "1m_src" and entry.suffix == ".npz":
                    try:
                        day_s, _ = _validate_day(entry.stem)
                    except ValueError:
                        continue
                    from_day = day_s if not from_day or day_s < from_day else from_day
                    to_day = day_s if not to_day or day_s > to_day else to_day
    return {
        "exchange": ex,
        "coin": coin_s,
        "market_status": market["status"],
        "market_reason": market["reason"],
        "files": files,
        "bytes": total_bytes,
        "from_day": from_day,
        "to_day": to_day,
        "directories": directories,
    }


def list_removed_coin_data(
    *,
    exchange: str = "bybit",
    data_root: Path | None = None,
    mapping_path: Path | None = None,
) -> dict[str, Any]:
    """List every local PBGui OHLCV coin absent or inactive in the current mapping."""
    ex = _validate_exchange(exchange)
    root = Path(data_root) if data_root is not None else get_exchange_raw_root_dir(ex)
    coins: set[str] = set()
    for dataset in ("1m", "candles_1m", "1m_api", "candles_1m_api", "1m_src"):
        dataset_root = root / dataset
        if not dataset_root.is_dir() or dataset_root.is_symlink():
            continue
        for entry in dataset_root.iterdir():
            if not entry.is_dir() or entry.is_symlink():
                continue
            try:
                coin = _validate_coin(entry.name)
                if ex == "hyperliquid" and coin.upper().startswith(("XYZ-", "XYZ:")):
                    continue
                coins.add(coin)
            except ValueError:
                continue

    rows: list[dict[str, Any]] = []
    mapping_status = "available"
    mapping_reason = ""
    for coin in sorted(coins):
        market = storage_market_status(ex, coin, mapping_path=mapping_path)
        if market["status"] == "unknown":
            mapping_status = "unknown"
            mapping_reason = market["reason"]
            continue
        if market["status"] != "removed":
            continue
        try:
            preview = _removed_coin_data_footprint(
                exchange=ex,
                coin=coin,
                data_root=root,
                mapping_path=mapping_path,
            )
            rows.append({**preview, "removable": True, "error": ""})
        except ValueError as exc:
            rows.append(
                {
                    "exchange": ex,
                    "coin": coin,
                    "market_status": "removed",
                    "market_reason": market["reason"],
                    "files": 0,
                    "bytes": 0,
                    "from_day": "",
                    "to_day": "",
                    "directories": [],
                    "removable": False,
                    "error": str(exc),
                }
            )
    return {
        "exchange": ex,
        "mapping_status": mapping_status,
        "mapping_reason": mapping_reason,
        "total": len(rows),
        "rows": rows,
    }


def unavailable_coin_data_batch_preview(
    *,
    exchange: str,
    coins: Iterable[str] | None = None,
    data_root: Path | None = None,
    mapping_path: Path | None = None,
) -> dict[str, Any]:
    """Return a revalidated aggregate preview for selected or all unavailable markets."""
    listing = list_removed_coin_data(
        exchange=exchange,
        data_root=data_root,
        mapping_path=mapping_path,
    )
    rows = list(listing["rows"])
    removable = {
        str(row["coin"]): row
        for row in rows
        if bool(row.get("removable"))
    }
    if coins is None:
        selected_coins = sorted(removable)
    else:
        selected_coins = sorted({_validate_coin(coin) for coin in coins})
        stale = [coin for coin in selected_coins if coin not in removable]
        if stale:
            raise ValueError(f"Unavailable market selection is stale or unsafe: {', '.join(stale[:10])}")
    if not selected_coins:
        raise ValueError("No removable unavailable markets were selected")
    selected_rows = [removable[coin] for coin in selected_coins]
    from_days = [str(row.get("from_day") or "") for row in selected_rows if row.get("from_day")]
    to_days = [str(row.get("to_day") or "") for row in selected_rows if row.get("to_day")]
    return {
        "exchange": listing["exchange"],
        "coins": selected_coins,
        "coin_count": len(selected_coins),
        "files": sum(int(row.get("files") or 0) for row in selected_rows),
        "bytes": sum(int(row.get("bytes") or 0) for row in selected_rows),
        "from_day": min(from_days) if from_days else "",
        "to_day": max(to_days) if to_days else "",
        "blocked_count": sum(1 for row in rows if not bool(row.get("removable"))),
    }


def remove_removed_coin_data(
    *,
    exchange: str,
    coin: str,
    data_root: Path | None = None,
    mapping_path: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Remove all PBGui OHLCV data for a market confirmed absent or inactive."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    root = Path(data_root) if data_root is not None else get_exchange_raw_root_dir(ex)
    with catalog_operation_lock(db_path):
        preview = unavailable_coin_data_preview(
            exchange=ex,
            coin=coin_s,
            data_root=root,
            mapping_path=mapping_path,
        )
        for dataset in preview["directories"]:
            target = root / str(dataset) / coin_s
            if target.is_symlink() or not target.is_dir():
                raise RuntimeError("Removed-coin data changed during deletion")
            shutil.rmtree(target)
        with _connect(db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM daily_checksums WHERE exchange=? AND timeframe=? AND coin=?",
                (ex, TIMEFRAME, coin_s),
            )
            conn.commit()
            catalog_rows = max(0, int(cursor.rowcount))
    return {**preview, "catalog_rows": catalog_rows}


def _connect(path: Path | None = None, *, readonly: bool = False) -> sqlite3.Connection:
    db_path = Path(path) if path is not None else checksum_database_path()
    if db_path.is_symlink():
        raise RuntimeError("Refusing symlink checksum database")
    if readonly:
        if not db_path.is_file():
            raise FileNotFoundError(db_path)
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
        conn.execute("PRAGMA query_only=ON")
    else:
        ensure_private_directory(db_path.parent)
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        _initialize_schema(conn)
        secure_private_file(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _initialize_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS daily_checksums (
            exchange TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            coin TEXT NOT NULL,
            day TEXT NOT NULL,
            candles INTEGER NOT NULL,
            missing_minutes INTEGER NOT NULL,
            status TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            first_ts INTEGER,
            last_ts INTEGER,
            file_mtime_ns INTEGER NOT NULL,
            file_size INTEGER NOT NULL,
            validated_at INTEGER NOT NULL,
            error TEXT NOT NULL DEFAULT '',
            scan_id TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (exchange, timeframe, coin, day)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_checksums_status
            ON daily_checksums(exchange, timeframe, status, day, coin);
        """
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_checksums)")}
    if "scan_id" not in columns:
        conn.execute("ALTER TABLE daily_checksums ADD COLUMN scan_id TEXT NOT NULL DEFAULT ''")
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()


def initialize_catalog(path: Path | None = None) -> Path:
    """Create the checksum catalog if needed and return its path."""
    db_path = Path(path) if path is not None else checksum_database_path()
    with _connect(db_path):
        pass
    return db_path


def _invalid(error: str, *, candles: int = 0, missing: int = 1440) -> DayValidation:
    return DayValidation(
        status="invalid",
        candles=max(0, int(candles)),
        missing_minutes=max(0, int(missing)),
        sha256="",
        first_ts=None,
        last_ts=None,
        error=str(error),
    )


def validate_daily_npz(
    path: Path,
    day: str | date,
    *,
    allow_inception_prefix: bool = False,
    allow_terminal_suffix: bool = False,
    allowed_source_gap_minutes: Iterable[int] | None = None,
    current_day: bool = False,
) -> DayValidation:
    """Validate and hash one PBGui daily structured NPZ without modifying it."""
    file_path = Path(path)
    day_s, day_obj = _validate_day(day)
    if file_path.is_symlink() or not file_path.is_file() or file_path.stem != day_s:
        return _invalid("missing or unsafe daily file")
    try:
        with np.load(file_path, allow_pickle=False) as data:
            if "candles" not in data:
                return _invalid("missing candles array")
            source = np.array(data["candles"], copy=True)
    except (OSError, ValueError, EOFError) as exc:
        return _invalid(f"unreadable NPZ: {type(exc).__name__}")

    if source.dtype.names is None:
        return _invalid("candles array is not structured", candles=len(source))
    missing_fields = [name for name in _CANONICAL_DTYPE.names or () if name not in source.dtype.names]
    if missing_fields:
        return _invalid("missing candle fields", candles=len(source))
    if len(source) == 0:
        return _invalid("empty candle day")

    canonical = np.empty(len(source), dtype=_CANONICAL_DTYPE)
    for field in _CANONICAL_DTYPE.names or ():
        canonical[field] = source[field]
    order = np.argsort(canonical["ts"], kind="stable")
    canonical = canonical[order]
    timestamps = canonical["ts"].astype(np.int64, copy=False)
    day_start_ms = int(datetime(day_obj.year, day_obj.month, day_obj.day, tzinfo=timezone.utc).timestamp() * 1000)
    offsets = timestamps - day_start_ms
    indices = offsets // 60_000
    unique_indices = np.unique(indices)
    candles = int(len(canonical))
    missing_minutes = max(0, 1440 - int(len(unique_indices)))
    first_ts = int(timestamps[0])
    last_ts = int(timestamps[-1])

    if np.any(offsets < 0) or np.any(offsets >= 86_400_000) or np.any(offsets % 60_000 != 0):
        return _invalid("timestamp outside or unaligned to UTC day", candles=candles, missing=missing_minutes)
    first_index = int(unique_indices[0])
    last_index = int(unique_indices[-1])
    unique_index_set = {int(minute) for minute in unique_indices}
    actual_missing = set(range(1440)) - unique_index_set
    configured_source_gaps = {int(minute) for minute in (allowed_source_gap_minutes or ())}
    ignored_inception_prefix = set(range(first_index)) if allow_inception_prefix else set()
    damaged_missing = actual_missing - ignored_inception_prefix
    recognized_source_gap = bool(configured_source_gaps) and damaged_missing == configured_source_gaps
    damaged_missing_minutes = missing_minutes
    if allow_inception_prefix:
        damaged_missing_minutes = max(0, (1440 - first_index) - int(len(unique_indices)))
    if recognized_source_gap:
        damaged_missing_minutes = len(configured_source_gaps)
    if len(unique_indices) != candles:
        return _invalid("duplicate timestamps", candles=candles, missing=damaged_missing_minutes)
    if candles > 1 and np.any(np.diff(timestamps) != 60_000) and not recognized_source_gap:
        return _invalid("internal minute gap", candles=candles, missing=damaged_missing_minutes)

    values = np.column_stack([canonical[name] for name in ("o", "h", "l", "c", "bv")])
    if not np.isfinite(values).all():
        return _invalid("non-finite candle value", candles=candles, missing=damaged_missing_minutes)
    if np.any(canonical["bv"] < 0):
        return _invalid("negative volume", candles=candles, missing=damaged_missing_minutes)
    # Passivbot stores and backtests HLCV; exchange Open anomalies do not affect it.
    if (
        np.any(canonical["l"] > canonical["c"])
        or np.any(canonical["h"] < canonical["c"])
        or np.any(canonical["l"] > canonical["h"])
    ):
        return _invalid("invalid HLC bounds", candles=candles, missing=damaged_missing_minutes)

    if current_day:
        status = "current"
    elif first_index == 0 and last_index == 1439 and candles == 1440:
        status = "valid"
    elif allow_inception_prefix and last_index == 1439 and candles == (1440 - first_index):
        status = "inception_partial"
    elif recognized_source_gap:
        status = "source_gap"
    elif allow_terminal_suffix and first_index == 0 and candles == (last_index + 1):
        status = "terminal_partial"
    else:
        return _invalid("closed day is incomplete", candles=candles, missing=damaged_missing_minutes)

    digest = hashlib.sha256()
    digest.update(b"pbgui-ohlcv-day-v1\x00")
    digest.update(canonical.tobytes(order="C"))
    return DayValidation(
        status=status,
        candles=candles,
        missing_minutes=damaged_missing_minutes,
        sha256=digest.hexdigest(),
        first_ts=first_ts,
        last_ts=last_ts,
    )


def _upsert_validation(
    conn: sqlite3.Connection,
    *,
    exchange: str,
    coin: str,
    day: str,
    validation: DayValidation,
    file_path: Path | None,
    file_stat: os.stat_result | None = None,
    scan_id: str = "",
) -> None:
    stat = file_stat if file_stat is not None else (file_path.stat() if file_path is not None and file_path.is_file() else None)
    conn.execute(
        """
        INSERT INTO daily_checksums(
            exchange, timeframe, coin, day, candles, missing_minutes, status,
            sha256, first_ts, last_ts, file_mtime_ns, file_size, validated_at, error, scan_id
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(exchange, timeframe, coin, day) DO UPDATE SET
            candles=excluded.candles,
            missing_minutes=excluded.missing_minutes,
            status=excluded.status,
            sha256=excluded.sha256,
            first_ts=excluded.first_ts,
            last_ts=excluded.last_ts,
            file_mtime_ns=excluded.file_mtime_ns,
            file_size=excluded.file_size,
            validated_at=excluded.validated_at,
            error=excluded.error,
            scan_id=excluded.scan_id
        """,
        (
            exchange,
            TIMEFRAME,
            coin,
            day,
            validation.candles,
            validation.missing_minutes,
            validation.status,
            validation.sha256,
            validation.first_ts,
            validation.last_ts,
            int(stat.st_mtime_ns) if stat else 0,
            int(stat.st_size) if stat else 0,
            int(time.time()),
            validation.error,
            str(scan_id),
        ),
    )


def record_daily_file(
    *,
    exchange: str,
    coin: str,
    day: str | date,
    path: Path,
    allow_inception_prefix: bool = False,
    allow_terminal_suffix: bool = False,
    allowed_source_gap_minutes: Iterable[int] | None = None,
    current_day: bool = False,
    db_path: Path | None = None,
) -> DayValidation:
    """Validate one file and transactionally update its catalog row."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    day_s, _ = _validate_day(day)
    file_path = Path(path)
    with catalog_operation_lock(db_path):
        with advisory_file_lock(file_path):
            validation = validate_daily_npz(
                file_path,
                day_s,
                allow_inception_prefix=allow_inception_prefix,
                allow_terminal_suffix=allow_terminal_suffix,
                allowed_source_gap_minutes=allowed_source_gap_minutes,
                current_day=current_day,
            )
            with _connect(db_path) as conn:
                _upsert_validation(
                    conn,
                    exchange=ex,
                    coin=coin_s,
                    day=day_s,
                    validation=validation,
                    file_path=file_path,
                )
    return validation


def record_proven_source_gap(
    *,
    exchange: str,
    coin: str,
    day: str | date,
    path: Path,
    db_path: Path | None = None,
) -> DayValidation:
    """Catalog an exact post-repair source gap without fabricating missing candles."""
    ex = _validate_exchange(exchange)
    if ex != "hyperliquid":
        raise ValueError("Dynamic source gaps are supported only for Hyperliquid")
    coin_s = _validate_coin(coin)
    day_s, day_obj = _validate_day(day)
    file_path = Path(path)
    if file_path.is_symlink():
        raise RuntimeError("Refusing a symlinked source-gap file")
    if file_path.is_file():
        present, _, _ = _stable_minute_coverage(file_path, day_obj)
        missing = set(range(1440)) - present
        return record_daily_file(
            exchange=ex,
            coin=coin_s,
            day=day_s,
            path=file_path,
            allowed_source_gap_minutes=missing,
            db_path=db_path,
        )
    if file_path.exists():
        raise RuntimeError("Source-gap path is not a regular file")

    digest = hashlib.sha256(
        f"pbgui-ohlcv-source-gap-v1\x00{ex}\x00{coin_s}\x00{day_s}".encode("utf-8")
    ).hexdigest()
    validation = DayValidation(
        status="source_gap",
        candles=0,
        missing_minutes=1440,
        sha256=digest,
        first_ts=None,
        last_ts=None,
        error="verified source unavailable",
    )
    with catalog_operation_lock(db_path):
        with _connect(db_path) as conn:
            _upsert_validation(
                conn,
                exchange=ex,
                coin=coin_s,
                day=day_s,
                validation=validation,
                file_path=None,
            )
            conn.commit()
    return validation


def daily_missing_minutes(path: Path, day: str | date) -> set[int]:
    """Return exact absent UTC minute indices from one safe daily file or missing day."""
    day_s, day_obj = _validate_day(day)
    file_path = Path(path)
    if file_path.is_symlink() or file_path.stem != day_s:
        raise ValueError("Invalid source-gap daily file")
    if not file_path.exists():
        return set(range(1440))
    if not file_path.is_file():
        raise ValueError("Invalid source-gap daily file")
    present, _, _ = _stable_minute_coverage(file_path, day_obj)
    return set(range(1440)) - present


def initial_scan_required(exchange: str, *, db_path: Path | None = None) -> bool:
    """Return whether the current initial-scan version has not completed."""
    ex = _validate_exchange(exchange)
    key = f"initial_scan:{ex}"
    with _connect(db_path) as conn:
        row = conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    return row is None or str(row["value"]) != str(INITIAL_SCAN_VERSION)


def _validate_stable_daily_file(
    path: Path,
    day: date,
    *,
    allow_inception_prefix: bool = False,
    allowed_source_gap_minutes: Iterable[int] | None = None,
    current_day: bool = False,
) -> tuple[DayValidation, os.stat_result | None]:
    """Validate one atomic daily file without creating persistent sidecar locks."""
    for _attempt in range(3):
        try:
            before = path.stat()
        except FileNotFoundError:
            return _invalid("missing daily file"), None
        if path.is_symlink() or not path.is_file():
            return _invalid("daily file is not a regular file"), None
        validation = validate_daily_npz(
            path,
            day,
            allow_inception_prefix=allow_inception_prefix,
            allowed_source_gap_minutes=allowed_source_gap_minutes,
            current_day=current_day,
        )
        try:
            after = path.stat()
        except FileNotFoundError:
            continue
        before_sig = (int(before.st_ino), int(before.st_size), int(before.st_mtime_ns))
        after_sig = (int(after.st_ino), int(after.st_size), int(after.st_mtime_ns))
        if before_sig == after_sig:
            return validation, after
    raise RuntimeError(f"OHLCV file changed repeatedly during scan: {path.name}")


def _cached_scan_status(
    row: sqlite3.Row | None,
    path: Path,
    *,
    allow_inception_prefix: bool,
    allowed_source_gap_minutes: frozenset[int],
    current_day: bool,
) -> str | None:
    """Return a reusable cached status when the file and scan context are unchanged."""
    if row is None or path.is_symlink():
        return None
    try:
        before = path.stat()
    except FileNotFoundError:
        return None
    if not path.is_file():
        return None
    if (
        int(row["file_mtime_ns"]) != int(before.st_mtime_ns)
        or int(row["file_size"]) != int(before.st_size)
    ):
        return None

    status = str(row["status"])
    if current_day:
        reusable = status == "current"
    elif status == "source_gap":
        reusable = True
    elif allowed_source_gap_minutes:
        reusable = status in {"valid", "source_gap"}
    elif allow_inception_prefix:
        reusable = status in {"valid", "inception_partial"}
    else:
        reusable = status in {"valid", "invalid"}
    if not reusable:
        return None

    try:
        after = path.stat()
    except FileNotFoundError:
        return None
    before_sig = (int(before.st_ino), int(before.st_size), int(before.st_mtime_ns))
    after_sig = (int(after.st_ino), int(after.st_size), int(after.st_mtime_ns))
    return status if before_sig == after_sig else None


def scan_exchange(
    exchange: str,
    *,
    db_path: Path | None = None,
    data_root: Path | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Scan existing daily files read-only and populate the integrity catalog."""
    ex = _validate_exchange(exchange)
    root = Path(data_root) if data_root is not None else get_exchange_raw_root_dir(ex)
    one_minute_root = root / TIMEFRAME
    with catalog_operation_lock(db_path):
        with _connect(db_path) as marker_conn:
            marker_conn.execute(
                "DELETE FROM metadata WHERE key IN (?, ?)",
                (f"initial_scan:{ex}", f"initial_scan_id:{ex}"),
            )
            marker_conn.commit()
    coin_dirs = (
        sorted(
            path
            for path in one_minute_root.iterdir()
            if path.is_dir()
            and not path.is_symlink()
            and not (
                ex == "hyperliquid"
                and path.name.upper().startswith(("XYZ-", "XYZ:"))
            )
        )
        if one_minute_root.is_dir()
        else []
    )
    all_files = sum(len(list(path.glob("*.npz"))) for path in coin_dirs)
    files_scanned = 0
    files_validated = 0
    files_reused = 0
    invalid_days = 0
    accepted_days = 0
    missing_files = 0
    today = datetime.now(timezone.utc).date()
    scan_id = uuid.uuid4().hex

    with _connect(db_path) as conn:
        for coin_dir in coin_dirs:
            coin = _validate_coin(coin_dir.name)
            cached_rows = {
                str(row["day"]): row
                for row in conn.execute(
                    """
                    SELECT day, status, file_mtime_ns, file_size
                    FROM daily_checksums
                    WHERE exchange=? AND timeframe=? AND coin=?
                    """,
                    (ex, TIMEFRAME, coin),
                ).fetchall()
            }
            files: list[tuple[date, Path]] = []
            for file_path in sorted(coin_dir.glob("*.npz")):
                try:
                    _, day_obj = _validate_day(file_path.stem)
                except ValueError:
                    continue
                files.append((day_obj, file_path))
            scan_items: list[tuple[date, Path, bool, bool]] = []
            for index, (day_obj, file_path) in enumerate(files):
                if stop_check and stop_check():
                    raise RuntimeError("integrity scan cancelled")
                if index > 0:
                    previous_day = files[index - 1][0]
                    missing_day = previous_day + timedelta(days=1)
                    while missing_day < day_obj:
                        missing_path = coin_dir / f"{missing_day.isoformat()}.npz"
                        scan_items.append((missing_day, missing_path, False, True))
                        missing_day += timedelta(days=1)
                scan_items.append((day_obj, file_path, index == 0, False))

            pending_items: list[tuple[date, Path, bool, bool]] = []
            with catalog_operation_lock(db_path):
                for day_obj, file_path, allow_inception_prefix, is_missing in scan_items:
                    if stop_check and stop_check():
                        raise RuntimeError("integrity scan cancelled")
                    day_s = day_obj.strftime("%Y-%m-%d")
                    source_gap_minutes = known_source_gap_minutes(ex, coin, day_s)
                    cached_row = cached_rows.get(day_s)
                    if is_missing:
                        reusable_status = None
                        if (
                            cached_row is not None
                            and int(cached_row["file_mtime_ns"]) == 0
                            and int(cached_row["file_size"]) == 0
                            and str(cached_row["status"]) in {"invalid", "source_gap"}
                            and not file_path.exists()
                            and not file_path.is_symlink()
                        ):
                            reusable_status = str(cached_row["status"])
                    else:
                        reusable_status = _cached_scan_status(
                            cached_row,
                            file_path,
                            allow_inception_prefix=allow_inception_prefix,
                            allowed_source_gap_minutes=source_gap_minutes,
                            current_day=day_obj >= today,
                        )
                    if reusable_status is None:
                        pending_items.append((day_obj, file_path, allow_inception_prefix, is_missing))
                        continue
                    conn.execute(
                        """
                        UPDATE daily_checksums SET scan_id=?
                        WHERE exchange=? AND timeframe=? AND coin=? AND day=?
                        """,
                        (scan_id, ex, TIMEFRAME, coin, day_s),
                    )
                    if is_missing:
                        missing_files += 1
                    else:
                        files_scanned += 1
                        files_reused += 1
                    if reusable_status in {"valid", "inception_partial", "terminal_partial", "source_gap", "current"}:
                        accepted_days += 1
                    else:
                        invalid_days += 1
                conn.commit()

            if progress_cb and files_scanned:
                progress_cb(
                    {
                        "stage": "scanning",
                        "step": files_scanned,
                        "total": all_files,
                        "exchange": ex,
                        "coin": coin,
                        "files_scanned": files_scanned,
                        "files_validated": files_validated,
                        "files_reused": files_reused,
                        "invalid_days": invalid_days,
                    }
                )

            for day_obj, file_path, allow_inception_prefix, is_missing in pending_items:
                if stop_check and stop_check():
                    raise RuntimeError("integrity scan cancelled")
                with catalog_operation_lock(db_path):
                    source_gap_minutes = known_source_gap_minutes(ex, coin, day_obj)
                    validation, stable_stat = _validate_stable_daily_file(
                        file_path,
                        day_obj,
                        allow_inception_prefix=allow_inception_prefix,
                        allowed_source_gap_minutes=source_gap_minutes,
                        current_day=day_obj >= today,
                    )
                    _upsert_validation(
                        conn,
                        exchange=ex,
                        coin=coin,
                        day=day_obj.strftime("%Y-%m-%d"),
                        validation=validation,
                        file_path=file_path if stable_stat is not None else None,
                        file_stat=stable_stat,
                        scan_id=scan_id,
                    )
                    conn.commit()
                if is_missing:
                    missing_files += 1
                else:
                    files_scanned += 1
                    files_validated += 1
                if validation.valid:
                    accepted_days += 1
                else:
                    invalid_days += 1
                if files_scanned % 250 == 0:
                    if progress_cb:
                        progress_cb(
                            {
                                "stage": "scanning",
                                "step": files_scanned,
                                "total": all_files,
                                "exchange": ex,
                                "coin": coin,
                                "day": day_obj.strftime("%Y-%m-%d"),
                                "files_scanned": files_scanned,
                                "files_validated": files_validated,
                                "files_reused": files_reused,
                                "invalid_days": invalid_days,
                            }
                        )
        with catalog_operation_lock(db_path):
            stale_rows = conn.execute(
                "SELECT coin, day FROM daily_checksums WHERE exchange=? AND scan_id<>?",
                (ex, scan_id),
            ).fetchall()
            for stale_row in stale_rows:
                stale_coin = str(stale_row["coin"])
                stale_day = str(stale_row["day"])
                stale_path = one_minute_root / stale_coin / f"{stale_day}.npz"
                excluded = ex == "hyperliquid" and stale_coin.upper().startswith(("XYZ-", "XYZ:"))
                if excluded or stale_path.is_symlink() or not stale_path.is_file():
                    conn.execute(
                        "DELETE FROM daily_checksums WHERE exchange=? AND timeframe=? AND coin=? AND day=?",
                        (ex, TIMEFRAME, stale_coin, stale_day),
                    )
            conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"initial_scan:{ex}", str(INITIAL_SCAN_VERSION)),
            )
            conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"initial_scan_id:{ex}", scan_id),
            )
            conn.commit()

    result = {
        "exchange": ex,
        "scan_version": INITIAL_SCAN_VERSION,
        "files_scanned": files_scanned,
        "files_validated": files_validated,
        "files_reused": files_reused,
        "accepted_days": accepted_days,
        "invalid_days": invalid_days,
        "missing_files": missing_files,
    }
    if progress_cb:
        progress_cb({"stage": "done", "step": files_scanned, "total": all_files, **result})
    return result


def normalize_hyperliquid_fallback_envelopes(
    *,
    coin: str | None = None,
    day: str | date | None = None,
    db_path: Path | None = None,
    data_root: Path | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    stop_check: Callable[[], bool] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Expand H/L around existing O/C only for Hyperliquid other-exchange candles."""
    root = Path(data_root) if data_root is not None else get_exchange_raw_root_dir("hyperliquid")
    if coin is not None and day is not None:
        candidates: list[Any] = [{"coin": _validate_coin(coin), "day": _validate_day(day)[0]}]
    else:
        clauses = [
            "exchange='hyperliquid'",
            "timeframe=?",
            "status='invalid'",
            "error IN ('invalid OHLC bounds', 'invalid HLC bounds')",
        ]
        params: list[Any] = [TIMEFRAME]
        if coin is not None:
            clauses.append("coin=?")
            params.append(_validate_coin(coin))
        if day is not None:
            clauses.append("day=?")
            params.append(_validate_day(day)[0])
        with _connect(db_path, readonly=True) as read_conn:
            candidates = read_conn.execute(
                f"""
                SELECT coin, day
                FROM daily_checksums
                WHERE {' AND '.join(clauses)}
                ORDER BY coin, day
                """,
                params,
            ).fetchall()
    total = len(candidates)
    files_changed = 0
    candles_changed = 0
    still_invalid = 0

    for index, candidate in enumerate(candidates, start=1):
        if stop_check and stop_check():
            raise RuntimeError("Hyperliquid fallback normalization cancelled")
        coin = _validate_coin(str(candidate["coin"]))
        day_s, day_obj = _validate_day(str(candidate["day"]))
        path = root / TIMEFRAME / coin / f"{day_s}.npz"
        source_codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin, day=day_s)
        changed = 0
        validation: DayValidation | None = None
        if source_codes is not None and path.is_file() and not path.is_symlink():
            with catalog_operation_lock(db_path):
                with advisory_file_lock(path):
                    with np.load(path, allow_pickle=False) as data:
                        if "candles" not in data:
                            raise RuntimeError(f"Missing candles array during normalization: {coin} {day_s}")
                        candles = np.array(data["candles"], copy=True)
                    required = {"ts", "o", "h", "l", "c", "bv"}
                    if candles.dtype.names is None or not required.issubset(candles.dtype.names):
                        raise RuntimeError(f"Invalid candle dtype during normalization: {coin} {day_s}")
                    day_start_ms = int(datetime(day_obj.year, day_obj.month, day_obj.day, tzinfo=timezone.utc).timestamp() * 1000)
                    for row in candles:
                        minute = int((int(row["ts"]) - day_start_ms) // 60_000)
                        if minute < 0 or minute >= len(source_codes) or int(source_codes[minute]) != SOURCE_CODE_OTHER:
                            continue
                        open_price = float(row["o"])
                        high_price = float(row["h"])
                        low_price = float(row["l"])
                        close_price = float(row["c"])
                        corrected_high = max(high_price, open_price, close_price)
                        corrected_low = min(low_price, open_price, close_price)
                        if corrected_high != high_price or corrected_low != low_price:
                            row["h"] = corrected_high
                            row["l"] = corrected_low
                            changed += 1
                    if changed and not dry_run:
                        tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
                        try:
                            with tmp.open("wb") as handle:
                                np.savez_compressed(handle, candles=candles)
                                handle.flush()
                                os.fsync(handle.fileno())
                            os.replace(tmp, path)
                        finally:
                            tmp.unlink(missing_ok=True)
                        validation = validate_daily_npz(path, day_s)
                        with _connect(db_path) as write_conn:
                            _upsert_validation(
                                write_conn,
                                exchange="hyperliquid",
                                coin=coin,
                                day=day_s,
                                validation=validation,
                                file_path=path,
                            )
                            write_conn.commit()
        if changed:
            files_changed += 1
            candles_changed += changed
        if validation is not None and not validation.valid:
            still_invalid += 1
        if progress_cb and (index % 100 == 0 or index == total):
            progress_cb(
                {
                    "stage": "normalizing" if index < total else "done",
                    "step": index,
                    "total": total,
                    "exchange": "hyperliquid",
                    "coin": coin,
                    "day": day_s,
                    "files_changed": files_changed,
                    "candles_changed": candles_changed,
                    "still_invalid": still_invalid,
                    "dry_run": bool(dry_run),
                }
            )

    return {
        "exchange": "hyperliquid",
        "candidates": total,
        "files_changed": files_changed,
        "candles_changed": candles_changed,
        "still_invalid": still_invalid,
        "dry_run": bool(dry_run),
    }


def list_integrity_issues(
    *,
    exchange: str = "bybit",
    limit: int = 500,
    offset: int = 0,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Return invalid catalog rows for the Market Data UI."""
    ex = _validate_exchange(exchange)
    limit_i = min(1_000_000, max(1, int(limit)))
    offset_i = max(0, int(offset))
    with _connect(db_path) as conn:
        total = int(
            conn.execute(
                "SELECT COUNT(*) FROM daily_checksums WHERE exchange=? AND timeframe=? AND status='invalid'",
                (ex, TIMEFRAME),
            ).fetchone()[0]
        )
        rows = conn.execute(
            """
            SELECT exchange, timeframe, coin, day, candles, missing_minutes,
                   status, sha256, validated_at, error
            FROM daily_checksums
            WHERE exchange=? AND timeframe=? AND status='invalid'
            ORDER BY day DESC, coin ASC
            LIMIT ? OFFSET ?
            """,
            (ex, TIMEFRAME, limit_i, offset_i),
        ).fetchall()
    payload_rows = [dict(row) for row in rows]
    market_statuses: dict[str, dict[str, str]] = {}
    repair_enabled = ex in SUPPORTED_EXCHANGES
    for row in payload_rows:
        coin = str(row.get("coin") or "")
        market_statuses.setdefault(coin, storage_market_status(ex, coin))
        row["market_status"] = market_statuses[coin]["status"]
        row["market_reason"] = market_statuses[coin]["reason"]
        row["repair_supported"] = repair_enabled and market_statuses[coin]["status"] != "removed"
    return {
        "exchange": ex,
        "total": total,
        "repair_supported": repair_enabled,
        "rows": payload_rows,
    }


def _stable_minute_coverage(path: Path, day_obj: date) -> tuple[set[int], int, int]:
    """Read one atomic daily file and return aligned minute indices, rows, and duplicates."""
    if not path.is_file():
        return set(), 0, 0
    for attempt in range(3):
        try:
            before = path.stat()
            with np.load(path, allow_pickle=False) as data:
                candles = data["candles"]
                if candles.dtype.names is None or "ts" not in candles.dtype.names:
                    raise ValueError("Daily file has no structured candle timestamps")
                timestamps = np.asarray(candles["ts"], dtype=np.int64)
            after = path.stat()
        except KeyError as exc:
            raise ValueError("Daily file has no candles array") from exc
        except (OSError, EOFError) as exc:
            if attempt == 2:
                raise RuntimeError("Unable to read stable OHLCV day details") from exc
            continue
        before_sig = (int(before.st_ino), int(before.st_size), int(before.st_mtime_ns))
        after_sig = (int(after.st_ino), int(after.st_size), int(after.st_mtime_ns))
        if before_sig != after_sig:
            if attempt == 2:
                raise RuntimeError("OHLCV file changed repeatedly while loading details")
            continue
        day_start_ms = int(datetime(day_obj.year, day_obj.month, day_obj.day, tzinfo=timezone.utc).timestamp() * 1000)
        offsets = timestamps - day_start_ms
        aligned = offsets[(offsets >= 0) & (offsets < 86_400_000) & (offsets % 60_000 == 0)]
        minute_values = [int(value // 60_000) for value in aligned]
        present = set(minute_values)
        return present, int(len(timestamps)), max(0, len(minute_values) - len(present))
    return set(), 0, 0


def daily_gap_details(
    *,
    exchange: str,
    coin: str,
    day: str | date,
    data_root: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Return stable per-minute coverage and gap ranges for one catalog day."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    day_s, day_obj = _validate_day(day)
    root = Path(data_root) if data_root is not None else get_exchange_raw_root_dir(ex)
    coin_root = root / TIMEFRAME / coin_s
    day_path = coin_root / f"{day_s}.npz"
    expected_parent = coin_root.resolve(strict=False)
    if coin_root.is_symlink() or day_path.resolve(strict=False).parent != expected_parent or day_path.is_symlink():
        raise ValueError("Invalid OHLCV detail path")

    context_start = day_obj - timedelta(days=7)
    context_end = day_obj + timedelta(days=7)
    with _connect(db_path, readonly=True) as conn:
        catalog = conn.execute(
            """
            SELECT status, candles, missing_minutes, error
            FROM daily_checksums
            WHERE exchange=? AND timeframe=? AND coin=? AND day=?
            """,
            (ex, TIMEFRAME, coin_s, day_s),
        ).fetchone()
        context_rows = conn.execute(
            """
            SELECT day, status, candles, missing_minutes, error
            FROM daily_checksums
            WHERE exchange=? AND timeframe=? AND coin=? AND day BETWEEN ? AND ?
            """,
            (ex, TIMEFRAME, coin_s, context_start.isoformat(), context_end.isoformat()),
        ).fetchall()
    if catalog is None:
        raise ValueError("OHLCV day is not present in the integrity catalog")

    present, actual_candles, duplicate_minutes = _stable_minute_coverage(day_path, day_obj)

    missing = sorted(set(range(1440)) - present)
    first_minute = min(present) if present else None
    last_minute = max(present) if present else None
    local_days = sorted(
        path.stem
        for path in coin_root.glob("*.npz")
        if path.is_file() and not path.is_symlink() and _DAY_RE.fullmatch(path.stem)
    )
    earliest_local_day = bool(local_days and local_days[0] == day_s)

    ranges: list[dict[str, Any]] = []
    if missing:
        range_start = previous = missing[0]
        for minute in missing[1:] + [None]:
            if minute is not None and minute == previous + 1:
                previous = minute
                continue
            if first_minute is None:
                kind = "missing_day"
            elif previous < first_minute:
                kind = "leading"
            elif range_start > last_minute:
                kind = "trailing"
            else:
                kind = "internal"
            ranges.append(
                {
                    "start_minute": range_start,
                    "end_minute": previous,
                    "start": f"{range_start // 60:02d}:{range_start % 60:02d}",
                    "end": f"{previous // 60:02d}:{previous % 60:02d}",
                    "minutes": previous - range_start + 1,
                    "kind": kind,
                    "possible_inception": kind == "leading" and earliest_local_day,
                }
            )
            if minute is not None:
                range_start = previous = minute

    coverage = ["p" if minute in present else "i" for minute in range(1440)]
    for gap in ranges:
        marker = {"leading": "l", "trailing": "t", "missing_day": "m"}.get(str(gap["kind"]), "i")
        for minute in range(int(gap["start_minute"]), int(gap["end_minute"]) + 1):
            coverage[minute] = marker
    context_catalog = {str(row["day"]): row for row in context_rows}
    day_context: list[dict[str, Any]] = []
    for offset in range(15):
        context_day = context_start + timedelta(days=offset)
        context_day_s = context_day.isoformat()
        context_path = coin_root / f"{context_day_s}.npz"
        context_present: set[int] = set()
        context_candles = 0
        context_error = ""
        if not context_path.is_symlink():
            try:
                context_present, context_candles, _duplicates = _stable_minute_coverage(context_path, context_day)
            except (RuntimeError, ValueError) as exc:
                context_error = str(exc)
        hourly = []
        for hour in range(24):
            count = sum(1 for minute in range(hour * 60, hour * 60 + 60) if minute in context_present)
            hourly.append("p" if count == 60 else ("m" if count == 0 else "x"))
        context_row = context_catalog.get(context_day_s)
        day_context.append(
            {
                "day": context_day_s,
                "selected": context_day_s == day_s,
                "has_file": context_path.is_file() and not context_path.is_symlink(),
                "candles": context_candles,
                "status": str(context_row["status"]) if context_row is not None else "missing",
                "missing_minutes": int(context_row["missing_minutes"]) if context_row is not None else 1440,
                "error": context_error or (str(context_row["error"] or "") if context_row is not None else "No catalog day"),
                "hourly_coverage": "".join(hourly),
            }
        )
    return {
        "exchange": ex,
        "coin": coin_s,
        "day": day_s,
        "status": str(catalog["status"]),
        "error": str(catalog["error"] or ""),
        "catalog_candles": int(catalog["candles"]),
        "actual_candles": actual_candles,
        "missing_minutes": len(missing),
        "damaged_missing_minutes": int(catalog["missing_minutes"]),
        "duplicate_minutes": duplicate_minutes,
        "first": f"{first_minute // 60:02d}:{first_minute % 60:02d}" if first_minute is not None else "",
        "last": f"{last_minute // 60:02d}:{last_minute % 60:02d}" if last_minute is not None else "",
        "earliest_local_day": earliest_local_day,
        "coverage": "".join(coverage),
        "ranges": ranges,
        "day_context": day_context,
    }


def catalog_summary(*, exchange: str = "bybit", db_path: Path | None = None) -> dict[str, Any]:
    """Return compact integrity counts and initial scan status."""
    ex = _validate_exchange(exchange)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS count FROM daily_checksums WHERE exchange=? GROUP BY status",
            (ex,),
        ).fetchall()
        metadata = conn.execute(
            "SELECT key, value FROM metadata WHERE key IN (?, ?)",
            (f"initial_scan:{ex}", f"initial_scan_id:{ex}"),
        ).fetchall()
    return {
        "exchange": ex,
        "counts": {str(row["status"]): int(row["count"]) for row in rows},
        "initial_scan_complete": any(
            row["key"] == f"initial_scan:{ex}" and row["value"] == str(INITIAL_SCAN_VERSION)
            for row in metadata
        ),
    }


def invalidate_catalog_for_deletion(
    *,
    exchange: str,
    coins: Iterable[str] | None = None,
    before_day: str | date | None = None,
    db_path: Path | None = None,
) -> int:
    """Remove deleted-file rows and require a reconciliation scan before publishing."""
    ex = _validate_exchange(exchange)
    coin_values = sorted({_validate_coin(coin) for coin in (coins or [])})
    day_s = _validate_day(before_day)[0] if before_day is not None else ""
    with catalog_operation_lock(db_path):
        with _connect(db_path) as conn:
            conn.execute(
                "DELETE FROM metadata WHERE key IN (?, ?)",
                (f"initial_scan:{ex}", f"initial_scan_id:{ex}"),
            )
            clauses = ["exchange=?", "timeframe=?"]
            params: list[Any] = [ex, TIMEFRAME]
            if coin_values:
                clauses.append(f"coin IN ({','.join('?' for _ in coin_values)})")
                params.extend(coin_values)
            if day_s:
                clauses.append("day<?")
                params.append(day_s)
            cursor = conn.execute(
                f"DELETE FROM daily_checksums WHERE {' AND '.join(clauses)}",
                params,
            )
            conn.commit()
            return max(0, int(cursor.rowcount))


def remove_catalog_before_day(
    *,
    exchange: str,
    coin: str,
    before_day: str | date,
    db_path: Path | None = None,
) -> int:
    """Remove proven non-applicable catalog rows before one exchange inception."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    day_s, _ = _validate_day(before_day)
    with catalog_operation_lock(db_path):
        with _connect(db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM daily_checksums WHERE exchange=? AND timeframe=? AND coin=? AND day<?",
                (ex, TIMEFRAME, coin_s, day_s),
            )
            conn.commit()
            return max(0, int(cursor.rowcount))


def remove_catalog_day(
    *,
    exchange: str,
    coin: str,
    day: str | date,
    db_path: Path | None = None,
) -> int:
    """Remove one proven non-applicable day without invalidating scan completion."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    day_s, _ = _validate_day(day)
    with catalog_operation_lock(db_path):
        with _connect(db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM daily_checksums WHERE exchange=? AND timeframe=? AND coin=? AND day=?",
                (ex, TIMEFRAME, coin_s, day_s),
            )
            conn.commit()
            return max(0, int(cursor.rowcount))


def day_is_finalized(
    *,
    exchange: str,
    coin: str,
    day: str | date,
    path: Path,
    db_path: Path | None = None,
) -> bool:
    """Return whether a valid catalog row still matches the local daily file."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    day_s, _ = _validate_day(day)
    file_path = Path(path)
    if file_path.is_symlink() or not file_path.is_file():
        return False
    with catalog_operation_lock(db_path):
        with advisory_file_lock(file_path):
            if not file_path.is_file() or file_path.is_symlink():
                return False
            stat = file_path.stat()
            with _connect(db_path) as conn:
                row = conn.execute(
                """
                SELECT status, file_mtime_ns, file_size
                FROM daily_checksums
                WHERE exchange=? AND timeframe=? AND coin=? AND day=?
                """,
                (ex, TIMEFRAME, coin_s, day_s),
                ).fetchone()
    return bool(
        row
        and row["status"] in {"valid", "inception_partial"}
        and int(row["file_mtime_ns"]) == int(stat.st_mtime_ns)
        and int(row["file_size"]) == int(stat.st_size)
    )


def oldest_unfinalized_day(
    *,
    exchange: str,
    coin: str,
    through_day: date,
    lookback_days: int,
    path_for_day: Callable[[date], Path],
    db_path: Path | None = None,
) -> date | None:
    """Return the oldest unfinished closed day in a bounded catch-up window."""
    ex = _validate_exchange(exchange)
    coin_s = _validate_coin(coin)
    window = max(1, min(30, int(lookback_days)))
    first_day = through_day - timedelta(days=window - 1)
    with catalog_operation_lock(db_path):
        with _connect(db_path) as conn:
            bounds = conn.execute(
            "SELECT MIN(day), MAX(day) FROM daily_checksums WHERE exchange=? AND timeframe=? AND coin=?",
            (ex, TIMEFRAME, coin_s),
            ).fetchone()
            if bounds and bounds[0]:
                first_known = datetime.strptime(str(bounds[0]), "%Y-%m-%d").date()
                if first_known > through_day:
                    return None
                first_day = max(first_day, first_known)
            for offset in range((through_day - first_day).days + 1):
                candidate = first_day + timedelta(days=offset)
                day_s = candidate.isoformat()
                file_path = Path(path_for_day(candidate))
                with advisory_file_lock(file_path):
                    if file_path.is_symlink() or not file_path.is_file():
                        return candidate
                    stat = file_path.stat()
                    row = conn.execute(
                    """
                    SELECT status, file_mtime_ns, file_size
                    FROM daily_checksums
                    WHERE exchange=? AND timeframe=? AND coin=? AND day=?
                    """,
                    (ex, TIMEFRAME, coin_s, day_s),
                    ).fetchone()
                    if not (
                        row
                        and row["status"] in {"valid", "inception_partial"}
                        and int(row["file_mtime_ns"]) == int(stat.st_mtime_ns)
                        and int(row["file_size"]) == int(stat.st_size)
                    ):
                        return candidate
    return None


def create_gzip_snapshot(
    *,
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Create a WAL-consistent gzip-compressed SQLite backup."""
    source = Path(db_path) if db_path is not None else checksum_database_path()
    if source.is_symlink() or not source.is_file():
        raise FileNotFoundError(source)
    destination = Path(output_path) if output_path is not None else source.parent / "reference" / "checksums.sqlite.gz"
    ensure_private_directory(destination.parent)
    lock_path = destination.with_suffix(destination.suffix + ".lock")
    with advisory_file_lock(lock_path):
        tmp_db = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.sqlite"
        tmp_gz = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.tmp"
        try:
            source_conn = _connect(source, readonly=True)
            target_conn = sqlite3.connect(tmp_db)
            try:
                source_conn.backup(target_conn)
                if target_conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("checksum snapshot quick_check failed")
            finally:
                target_conn.close()
                source_conn.close()
            secure_private_file(tmp_db)
            tmp_fd = os.open(tmp_gz, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with tmp_db.open("rb") as src, os.fdopen(tmp_fd, "wb") as raw_dst:
                with gzip.GzipFile(fileobj=raw_dst, mode="wb", compresslevel=6) as dst:
                    while chunk := src.read(1024 * 1024):
                        dst.write(chunk)
                raw_dst.flush()
                os.fsync(raw_dst.fileno())
            with gzip.open(tmp_gz, "rb") as check:
                if check.read(16) != b"SQLite format 3\x00":
                    raise RuntimeError("invalid compressed checksum snapshot")
            os.replace(tmp_gz, destination)
            secure_private_file(destination)
        finally:
            tmp_db.unlink(missing_ok=True)
            tmp_gz.unlink(missing_ok=True)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    return {"path": str(destination), "bytes": destination.stat().st_size, "sha256": digest}


def _validate_reference_database(path: Path) -> None:
    """Reject malformed or unexpected reference database schemas."""
    conn = _connect(path, readonly=True)
    try:
        conn.execute("PRAGMA trusted_schema=OFF")
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("reference database quick_check failed")
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if tables != {"metadata", "daily_checksums"}:
            raise RuntimeError("reference database has unexpected tables")
        version = conn.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
        if version is None or str(version[0]) != str(SCHEMA_VERSION):
            raise RuntimeError("reference database schema version is unsupported")
        expected_columns = {
            "exchange", "timeframe", "coin", "day", "candles", "missing_minutes",
            "status", "sha256", "first_ts", "last_ts", "file_mtime_ns", "file_size",
            "validated_at", "error", "scan_id",
        }
        column_info = list(conn.execute("PRAGMA table_info(daily_checksums)"))
        columns = {str(row[1]) for row in column_info}
        if columns != expected_columns:
            raise RuntimeError("reference database columns are unsupported")
        primary_key = [
            str(row[1])
            for row in sorted((row for row in column_info if int(row[5]) > 0), key=lambda row: int(row[5]))
        ]
        if primary_key != ["exchange", "timeframe", "coin", "day"]:
            raise RuntimeError("reference database primary key is unsupported")
        expected_types = {
            "exchange": "TEXT", "timeframe": "TEXT", "coin": "TEXT", "day": "TEXT",
            "candles": "INTEGER", "missing_minutes": "INTEGER", "status": "TEXT",
            "sha256": "TEXT", "first_ts": "INTEGER", "last_ts": "INTEGER",
            "file_mtime_ns": "INTEGER", "file_size": "INTEGER", "validated_at": "INTEGER",
            "error": "TEXT", "scan_id": "TEXT",
        }
        if any(str(row[2]).upper() != expected_types[str(row[1])] for row in column_info):
            raise RuntimeError("reference database column types are unsupported")
        required_not_null = expected_columns - {"first_ts", "last_ts"}
        if any(str(row[1]) in required_not_null and int(row[3]) != 1 for row in column_info):
            raise RuntimeError("reference database constraints are unsupported")

        allowed_statuses = {"valid", "invalid", "inception_partial", "terminal_partial", "source_gap", "current"}
        row_count = int(conn.execute("SELECT COUNT(*) FROM daily_checksums").fetchone()[0])
        if row_count > 5_000_000:
            raise RuntimeError("reference database contains too many rows")
        for row in conn.execute(
            """
            SELECT exchange, timeframe, coin, day, candles, missing_minutes, status,
                   sha256, first_ts, last_ts, file_mtime_ns, file_size, validated_at,
                   error, scan_id
            FROM daily_checksums
            """
        ):
            exchange = str(row["exchange"])
            coin = str(row["coin"])
            day = str(row["day"])
            status = str(row["status"])
            digest = str(row["sha256"])
            if exchange not in SUPPORTED_EXCHANGES or row["timeframe"] != TIMEFRAME:
                raise RuntimeError("reference database contains an unsupported market")
            if exchange == "hyperliquid" and coin.upper().startswith(("XYZ-", "XYZ:")):
                raise RuntimeError("reference database contains unsupported Hyperliquid TradFi data")
            if len(coin) > 160 or _validate_coin(coin) != coin:
                raise RuntimeError("reference database contains an invalid coin")
            _validate_day(day)
            candles = int(row["candles"])
            missing = int(row["missing_minutes"])
            if not 0 <= candles <= 1440 or not 0 <= missing <= 1440 or status not in allowed_statuses:
                raise RuntimeError("reference database contains invalid daily counts")
            if int(row["file_mtime_ns"]) < 0 or int(row["file_size"]) < 0 or int(row["validated_at"]) <= 0:
                raise RuntimeError("reference database contains invalid file metadata")
            first_ts = row["first_ts"]
            last_ts = row["last_ts"]
            if (first_ts is None) != (last_ts is None):
                raise RuntimeError("reference database contains incomplete timestamp bounds")
            if first_ts is not None:
                day_obj = datetime.strptime(day, "%Y-%m-%d").date()
                day_start = int(datetime(day_obj.year, day_obj.month, day_obj.day, tzinfo=timezone.utc).timestamp() * 1000)
                first_offset = int(first_ts) - day_start
                last_offset = int(last_ts) - day_start
                if (
                    first_offset < 0
                    or last_offset >= 86_400_000
                    or first_offset % 60_000
                    or last_offset % 60_000
                    or last_offset < first_offset
                ):
                    raise RuntimeError("reference database contains invalid timestamp bounds")
                expected_candles = ((last_offset - first_offset) // 60_000) + 1
                if status == "source_gap":
                    known_gap = known_source_gap_minutes(exchange, coin, day)
                    expected_total = expected_candles if known_gap else 1440
                    if candles + missing != expected_total:
                        raise RuntimeError("reference database contains inconsistent source-gap bounds")
                elif candles != expected_candles:
                    raise RuntimeError("reference database contains inconsistent candle bounds")
            elif status == "source_gap":
                if exchange != "hyperliquid" or candles != 0 or missing != 1440:
                    raise RuntimeError("reference database contains an invalid missing source-gap day")
            elif status != "invalid":
                raise RuntimeError("reference database is missing timestamp bounds")
            if status == "valid" and (candles != 1440 or missing != 0):
                raise RuntimeError("reference database contains an incomplete valid day")
            if status in {"inception_partial", "terminal_partial", "current"} and missing != 1440 - candles:
                raise RuntimeError("reference database contains inconsistent partial counts")
            if status == "source_gap":
                known_gap = known_source_gap_minutes(exchange, coin, day)
                if known_gap and missing != len(known_gap):
                    raise RuntimeError("reference database contains an inconsistent known source gap")
                if not known_gap and exchange != "hyperliquid":
                    raise RuntimeError("reference database contains an unknown source gap")
            if first_ts is not None:
                first_minute = (int(first_ts) - day_start) // 60_000
                last_minute = (int(last_ts) - day_start) // 60_000
                if status == "inception_partial" and not (first_minute > 0 and last_minute == 1439):
                    raise RuntimeError("reference database contains an invalid inception boundary")
                if status == "terminal_partial" and not (first_minute == 0 and last_minute < 1439):
                    raise RuntimeError("reference database contains an invalid terminal boundary")
            if status == "invalid":
                if digest and not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise RuntimeError("reference database contains an invalid checksum")
            elif not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise RuntimeError("reference database contains an invalid checksum")
            if len(str(row["error"])) > 1000 or len(str(row["scan_id"])) > 64:
                raise RuntimeError("reference database contains oversized values")
    finally:
        conn.close()


def install_reference_snapshot(
    *,
    url: str,
    root: Path | None = None,
    compressed_limit: int = 128 * 1024 * 1024,
    decompressed_limit: int = 1024 * 1024 * 1024,
) -> dict[str, Any]:
    """Serialize download and installation of the shared reference catalog."""
    base = Path(root) if root is not None else get_market_data_root_dir()
    reference_dir = ensure_private_directory(base / "reference")
    with reference_operation_lock(reference_database_path(base)):
        return _install_reference_snapshot_unlocked(
            url=url,
            root=base,
            compressed_limit=compressed_limit,
            decompressed_limit=decompressed_limit,
        )


def _install_reference_snapshot_unlocked(
    *,
    url: str,
    root: Path | None = None,
    compressed_limit: int = 128 * 1024 * 1024,
    decompressed_limit: int = 1024 * 1024 * 1024,
) -> dict[str, Any]:
    """Download, validate, and atomically install one public reference snapshot."""
    initial = urlsplit(str(url or ""))
    if initial.scheme != "https" or initial.hostname != "github.com" or initial.username is not None:
        raise ValueError("Invalid public checksum release URL")
    base = Path(root) if root is not None else get_market_data_root_dir()
    reference_dir = ensure_private_directory(base / "reference")
    destination = reference_database_path(base)
    state_path = reference_dir / "state.json"
    tmp_gz = reference_dir / f".checksums.{uuid.uuid4().hex}.gz"
    tmp_db = reference_dir / f".checksums.{uuid.uuid4().hex}.sqlite"
    allowed_hosts = {
        "github.com",
        "objects.githubusercontent.com",
        "release-assets.githubusercontent.com",
    }
    try:
        request = Request(str(url), headers={"User-Agent": "PBGui-OHLCV-Checksums/1"})
        with urlopen(request, timeout=60) as response:
            final_host = urlsplit(response.geturl()).hostname
            if final_host not in allowed_hosts:
                raise RuntimeError("checksum download redirected to an untrusted host")
            fd = os.open(tmp_gz, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            downloaded = 0
            with os.fdopen(fd, "wb") as target:
                while chunk := response.read(1024 * 1024):
                    downloaded += len(chunk)
                    if downloaded > int(compressed_limit):
                        raise RuntimeError("compressed checksum snapshot exceeds size limit")
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
        output_fd = os.open(tmp_db, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        decompressed = 0
        with gzip.open(tmp_gz, "rb") as source, os.fdopen(output_fd, "wb") as target:
            while chunk := source.read(1024 * 1024):
                decompressed += len(chunk)
                if decompressed > int(decompressed_limit):
                    raise RuntimeError("checksum database exceeds size limit")
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        _validate_reference_database(tmp_db)
        os.replace(tmp_db, destination)
        secure_private_file(destination)
        destination_stat = destination.stat()
        state = {
            "source": f"https://github.com/{'/'.join(initial.path.split('/')[1:3])}",
            "downloaded_at": int(time.time()),
            "compressed_bytes": downloaded,
            "database_bytes": decompressed,
            "database_mtime_ns": int(destination_stat.st_mtime_ns),
            "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        }
        atomic_write_private_text(state_path, json.dumps(state, indent=4, sort_keys=True) + "\n")
        return state
    finally:
        tmp_gz.unlink(missing_ok=True)
        tmp_db.unlink(missing_ok=True)


def reference_status(root: Path | None = None) -> dict[str, Any]:
    """Return safe metadata for the installed reference snapshot."""
    base = Path(root) if root is not None else get_market_data_root_dir()
    state_path = base / "reference" / "state.json"
    if state_path.is_symlink() or not state_path.is_file():
        return {"available": False}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"available": False, "error": "Reference state is unreadable"}
    if not isinstance(state, dict):
        return {"available": False, "error": "Reference state is invalid"}
    database_path = reference_database_path(base)
    database_matches_state = False
    if database_path.is_file() and not database_path.is_symlink():
        stat = database_path.stat()
        database_matches_state = (
            int(state.get("database_bytes") or 0) == int(stat.st_size)
            and int(state.get("database_mtime_ns") or 0) == int(stat.st_mtime_ns)
        )
    return {
        "available": database_matches_state,
        "source": str(state.get("source") or ""),
        "downloaded_at": int(state.get("downloaded_at") or 0),
        "compressed_bytes": int(state.get("compressed_bytes") or 0),
        "database_bytes": int(state.get("database_bytes") or 0),
        "database_mtime_ns": int(state.get("database_mtime_ns") or 0),
        "sha256": str(state.get("sha256") or ""),
    }


def compare_catalogs_readonly(
    *,
    local_path: Path | None = None,
    reference_path: Path | None = None,
    exchange: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Serialize comparison against a replaceable reference database."""
    resolved_reference = Path(reference_path) if reference_path is not None else reference_database_path()
    with reference_operation_lock(resolved_reference):
        return _compare_catalogs_readonly_unlocked(
            local_path=local_path,
            reference_path=resolved_reference,
            exchange=exchange,
            limit=limit,
        )


def _compare_catalogs_readonly_unlocked(
    *,
    local_path: Path | None = None,
    reference_path: Path | None = None,
    exchange: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Compare local and reference catalogs without attaching either database."""
    local = _connect(local_path or checksum_database_path(), readonly=True)
    reference = _connect(reference_path or reference_database_path(), readonly=True)
    ex = _validate_exchange(exchange) if exchange is not None else ""
    query = (
        "SELECT exchange, timeframe, coin, day, candles, sha256 FROM daily_checksums "
        "WHERE status IN ('valid', 'inception_partial', 'source_gap')"
    )
    params: tuple[Any, ...] = ()
    if ex:
        query += " AND exchange=?"
        params = (ex,)
    query += " ORDER BY exchange, timeframe, coin, day"
    local_rows = iter(local.execute(query, params))
    reference_rows = iter(reference.execute(query, params))
    local_row = next(local_rows, None)
    reference_row = next(reference_rows, None)
    differences: list[dict[str, Any]] = []
    counts = {"local_only": 0, "reference_only": 0, "mismatch": 0, "match": 0}

    def key(row: sqlite3.Row) -> tuple[str, str, str, str]:
        return (row["exchange"], row["timeframe"], row["coin"], row["day"])

    try:
        while local_row is not None or reference_row is not None:
            if reference_row is None or (local_row is not None and key(local_row) < key(reference_row)):
                counts["local_only"] += 1
                if len(differences) < limit:
                    differences.append({"kind": "local_only", "exchange": local_row["exchange"], "coin": local_row["coin"], "day": local_row["day"]})
                local_row = next(local_rows, None)
            elif local_row is None or key(reference_row) < key(local_row):
                counts["reference_only"] += 1
                if len(differences) < limit:
                    differences.append({"kind": "reference_only", "exchange": reference_row["exchange"], "coin": reference_row["coin"], "day": reference_row["day"]})
                reference_row = next(reference_rows, None)
            else:
                if int(local_row["candles"]) != int(reference_row["candles"]) or local_row["sha256"] != reference_row["sha256"]:
                    counts["mismatch"] += 1
                    if len(differences) < limit:
                        differences.append({"kind": "mismatch", "exchange": local_row["exchange"], "coin": local_row["coin"], "day": local_row["day"]})
                else:
                    counts["match"] += 1
                local_row = next(local_rows, None)
                reference_row = next(reference_rows, None)
    finally:
        local.close()
        reference.close()
    return {"counts": counts, "differences": differences, "truncated": sum(counts[k] for k in ("local_only", "reference_only", "mismatch")) > len(differences)}


def validation_to_dict(validation: DayValidation) -> dict[str, Any]:
    """Return a JSON-safe validation payload."""
    return asdict(validation)
