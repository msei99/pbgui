"""
Pure tradfi (TradFi / stock-perp) helper functions.

No UI dependency — safe to import from FastAPI endpoints.
"""
from __future__ import annotations

import calendar
import json
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta, timezone as _timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    _ZoneInfo = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def tradfi_map_path() -> Path:
    """Return path to the TradFi symbol map JSON."""
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "tradfi_symbol_map.json"


def load_tradfi_map() -> list:
    """Load the TradFi symbol map, returns empty list on missing/broken file."""
    path = tradfi_map_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_tradfi_map(records: list) -> None:
    """Save the TradFi symbol map atomically."""
    path = tradfi_map_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(records or [], indent=4, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


TRADFI_CANONICAL_TYPES = [
    "equity_us", "equity_kr", "equity_jp", "fx", "commodity",
    "commodity_etf", "index_etf", "etf",
]
TRADFI_STATUSES = ["ok", "alias", "pending", "no_provider", "delisted"]
TRADFI_STATUSES_SELECTABLE = ["ok", "alias", "pending", "no_provider"]

TRADFI_KNOWN_DESCRIPTIONS: dict[str, str] = {
    "GOLD": "Gold (XAU/USD spot)",
    "SILVER": "Silver (XAG/USD spot)",
    "PLATINUM": "Platinum (XPT/USD spot)",
    "PALLADIUM": "Palladium (XPD/USD spot)",
    "CL": "WTI Crude Oil (WTIJ6 front-month futures)",
    "NATGAS": "Natural Gas (NGJ26 front-month futures)",
    "COPPER": "Copper (HGK6 front-month futures)",
    "ALUMINIUM": "Aluminium (LME spot commodity)",
    "URANIUM": "Uranium (UX spot price)",
    "EUR": "Euro / US Dollar FX (EUR/USD)",
    "JPY": "Japanese Yen (USD/JPY rate)",
    "GBP": "British Pound / US Dollar FX (GBP/USD)",
    "DXY": "US Dollar Index (DXY basket)",
    "XYZ100": "XYZ100 index (XYZ stock-perps basket, NMH6/USD oracle)",
    "JP225": "Nikkei 225 index (Japan)",
    "KR200": "KOSPI 200 index (South Korea)",
    "URNM": "Sprott Uranium Miners ETF (NASDAQ: URNM)",
    "HYUN": "Hyundai Motor Company (KRX: 005380.KS)",
    "SKHX": "SK Hynix Inc. (KRX: 000660.KS)",
    "SMSN": "Samsung Electronics Co. Ltd. (KRX: 005930.KS)",
    "SOFTBANK": "SoftBank Group Corp. (TYO: 9984)",
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

_SPEC_SOURCE_MAPPING_JSON = "mapping.json"


def _mapping_json_path() -> Path:
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "mapping.json"


def _xyz_spec_cache_path() -> Path:
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "xyz_spec.json"


def tiingo_meta_cache_path() -> Path:
    return Path.cwd() / "data" / "coindata" / "tiingo_meta.json"


def tradfi_quote_cache_path() -> Path:
    return Path.cwd() / "data" / "coindata" / "hyperliquid" / "tradfi_quote_cache.json"


def _load_xyz_spec_cache() -> list[dict]:
    path = _xyz_spec_cache_path()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    instruments = payload.get("instruments") if isinstance(payload, dict) else None
    return [item for item in (instruments or []) if isinstance(item, dict)]


def load_xyz_spec_by_coin() -> dict[str, dict]:
    spec_by_coin: dict[str, dict] = {}
    for row in _load_xyz_spec_cache():
        coin = str(row.get("xyz_coin") or "").strip().upper()
        if coin:
            spec_by_coin[coin] = row
    return spec_by_coin


def _normalize_external_href(raw_href: str) -> str:
    import urllib.parse as _uparse

    href = str(raw_href or "").strip()
    if href.startswith("//"):
        href = "https:" + href
    if href and "://" in href and not href.startswith(("http://", "https://")):
        href = "https://" + href.split("://", 1)[1]

    try:
        parsed = _uparse.urlsplit(href)
        if parsed.netloc == "pythdata.app":
            for marker in ("/explore/", "/price-feeds/"):
                if marker in parsed.path:
                    prefix, symbol = parsed.path.split(marker, 1)
                    fixed_symbol = _uparse.quote(_uparse.unquote(symbol), safe="._-~")
                    return _uparse.urlunsplit(parsed._replace(path=prefix + marker + fixed_symbol))
    except Exception:
        pass

    return href


def _hl_fetch_cached_price_for_xyz(xyz_coin: str) -> float | None:
    """Return latest local Hyperliquid 1m close for an XYZ coin without network I/O."""
    try:
        import numpy as np
    except Exception:
        return None

    base_dir = Path.cwd() / "data" / "ohlcv" / "hyperliquid" / "1m"
    coin_dir = base_dir / f"XYZ-{str(xyz_coin or '').strip().upper()}_USDC:USDC"
    if not coin_dir.is_dir():
        return None
    npz_files = sorted(coin_dir.glob("*.npz"))
    if not npz_files:
        return None
    try:
        with np.load(str(npz_files[-1])) as handle:
            data = handle[handle.files[0]]
            if len(data) > 0:
                return float(data[-1]["c"])
    except Exception:
        return None
    return None


def _row_prices(row: dict[str, Any], quote_cache: dict[str, Any]) -> tuple[float | None, float | None]:
    xyz = str(row.get("xyz_coin") or "").upper()
    hl_price = _hl_fetch_cached_price_for_xyz(xyz)

    tiingo_price = None
    tiingo_ticker = str(row.get("tiingo_ticker") or "").upper()
    tiingo_fx_ticker = str(row.get("tiingo_fx_ticker") or "").lower()
    tiingo_fx_invert = bool(row.get("tiingo_fx_invert", False))
    canonical_type = str(row.get("canonical_type") or "").lower()
    status = str(row.get("status") or "").lower()

    multiplier = None
    try:
        raw_multiplier = row.get("tiingo_price_multiplier")
        if raw_multiplier not in (None, ""):
            parsed_multiplier = float(raw_multiplier)
            if parsed_multiplier > 0:
                multiplier = parsed_multiplier
    except Exception:
        multiplier = None

    if (
        tiingo_ticker
        and status == "alias"
        and canonical_type in {"equity_kr", "equity_jp"}
        and multiplier is None
    ):
        return hl_price, None

    if tiingo_ticker:
        quote = quote_cache.get(tiingo_ticker)
        if isinstance(quote, dict):
            try:
                tiingo_price = float(quote.get("price"))
                if multiplier is not None:
                    tiingo_price = tiingo_price * multiplier
            except Exception:
                tiingo_price = None
    elif tiingo_fx_ticker:
        quote = quote_cache.get(tiingo_fx_ticker)
        if isinstance(quote, dict):
            try:
                raw_price = float(quote.get("price"))
                tiingo_price = (1.0 / raw_price) if (raw_price and tiingo_fx_invert) else raw_price
            except Exception:
                tiingo_price = None

    return hl_price, tiingo_price


def _row_tiingo_symbol(row: dict[str, Any]) -> str:
    tiingo_ticker = str(row.get("tiingo_ticker") or "").strip().upper()
    if tiingo_ticker:
        return f"IEX:{tiingo_ticker}"
    tiingo_fx_ticker = str(row.get("tiingo_fx_ticker") or "").strip().upper()
    if tiingo_fx_ticker:
        return f"FX:{tiingo_fx_ticker}" + (" (inv)" if bool(row.get("tiingo_fx_invert", False)) else "")
    return ""


def _row_fetch_start_date(row: dict[str, Any]) -> str:
    raw_value = str(row.get("tiingo_start_date") or "").strip()
    tiingo_ticker = str(row.get("tiingo_ticker") or "").strip().upper()
    if not raw_value:
        return ""
    try:
        parsed = _date.fromisoformat(raw_value[:10])
    except Exception:
        return ""
    if tiingo_ticker:
        return max(parsed, _date(2016, 12, 12)).isoformat()
    return parsed.isoformat()


def guess_tradfi_canonical_type(xyz_coin: str) -> str:
    coin = str(xyz_coin or "").strip().upper()
    if coin in {"GOLD", "SILVER", "PLATINUM", "PALLADIUM", "CL", "NATGAS", "COPPER", "ALUMINIUM", "URANIUM", "BRENTOIL"}:
        return "commodity"
    if coin in {"EUR", "JPY", "GBP", "DXY"}:
        return "fx"
    if coin in {"JP225", "KR200", "XYZ100"}:
        return "index_etf"
    if coin in {"URNM"}:
        return "commodity_etf"
    if coin in {"HYUN", "SKHX", "SMSN"}:
        return "equity_kr"
    if coin in {"SOFTBANK"}:
        return "equity_jp"
    return "equity_us"


def resolve_tradfi_canonical_type(entry: dict[str, Any] | None, spec_row: dict[str, Any] | None = None) -> str:
    coin = str((entry or {}).get("xyz_coin") or (spec_row or {}).get("xyz_coin") or "").strip().upper()
    spec_source = str((entry or {}).get("spec_source") or "").strip().lower()
    spec_type = str((spec_row or {}).get("canonical_type") or "").strip().lower()
    row_type = str((entry or {}).get("canonical_type") or "").strip().lower()

    if spec_source == _SPEC_SOURCE_MAPPING_JSON and spec_type:
        return spec_type
    if row_type:
        return row_type
    if spec_type:
        return spec_type
    return guess_tradfi_canonical_type(coin)


def apply_tradfi_spec_defaults(entry: dict[str, Any], spec_row: dict[str, Any] | None = None) -> dict[str, Any]:
    row = dict(entry or {})
    spec = dict(spec_row or {})
    if not row:
        return row

    if str(row.get("spec_source") or "").strip().lower() != _SPEC_SOURCE_MAPPING_JSON:
        return row

    spec_description = str(spec.get("description") or "").strip()
    spec_type = str(spec.get("canonical_type") or "").strip().lower()

    if spec_description:
        row["description"] = spec_description
    if spec_type:
        row["canonical_type"] = spec_type
    return row


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


_AUTO_MAP_NOT_FOUND_NOTE = "auto-map: not found"


def normalize_tradfi_note(raw_note: object) -> str:
    parts = [part.strip() for part in str(raw_note or "").split("|") if part.strip()]
    if not parts:
        return ""
    cleaned: list[str] = []
    saw_auto_map_not_found = False
    for part in parts:
        if part == _AUTO_MAP_NOT_FOUND_NOTE:
            if saw_auto_map_not_found:
                continue
            saw_auto_map_not_found = True
        cleaned.append(part)
    return " | ".join(cleaned)


def clear_tradfi_auto_map_not_found_note(raw_note: object) -> str:
    parts = [
        part.strip()
        for part in normalize_tradfi_note(raw_note).split("|")
        if part.strip() and part.strip() != _AUTO_MAP_NOT_FOUND_NOTE
    ]
    return " | ".join(parts)


def add_tradfi_auto_map_not_found_note(raw_note: object) -> str:
    parts = [part.strip() for part in clear_tradfi_auto_map_not_found_note(raw_note).split("|") if part.strip()]
    parts.append(_AUTO_MAP_NOT_FOUND_NOTE)
    return " | ".join(parts)


def normalize_tradfi_map_entry(entry: dict) -> dict:
    key = str((entry or {}).get("xyz_coin") or "").strip().upper()
    if key.startswith("XYZ:") or key.startswith("XYZ-"):
        key = key[4:].strip().upper()
    if not key:
        raise ValueError("xyz_coin cannot be empty")

    canonical_type = str((entry or {}).get("canonical_type") or "").strip()
    if canonical_type not in TRADFI_CANONICAL_TYPES:
        canonical_type = guess_tradfi_canonical_type(key)

    status = str((entry or {}).get("status") or "pending").strip()
    if status not in TRADFI_STATUSES:
        status = "pending"

    tiingo_ticker = str((entry or {}).get("tiingo_ticker") or "").strip().upper() or None
    tiingo_fx_ticker = str((entry or {}).get("tiingo_fx_ticker") or "").strip().upper() or None
    tiingo_start_date = str((entry or {}).get("tiingo_start_date") or "").strip() or None
    description = str((entry or {}).get("description") or "").strip() or TRADFI_KNOWN_DESCRIPTIONS.get(key, "")
    note = normalize_tradfi_note((entry or {}).get("note"))
    spec_source = str((entry or {}).get("spec_source") or "manual").strip() or "manual"
    last_verified = str((entry or {}).get("last_verified") or _date.today().isoformat()).strip()

    return {
        "xyz_coin": key,
        "description": description,
        "canonical_type": canonical_type,
        "tiingo_ticker": tiingo_ticker,
        "tiingo_fx_ticker": tiingo_fx_ticker,
        "tiingo_fx_invert": _normalize_bool((entry or {}).get("tiingo_fx_invert")),
        "tiingo_start_date": tiingo_start_date,
        "status": status,
        "note": note,
        "last_verified": last_verified,
        "spec_source": spec_source,
    }


def upsert_tradfi_map_entry(entry: dict) -> dict:
    normalized = normalize_tradfi_map_entry(entry)
    key = normalized["xyz_coin"]
    records = load_tradfi_map()
    replaced = False
    for index, row in enumerate(records):
        if str((row or {}).get("xyz_coin") or "").strip().upper() == key:
            preserved = dict(row)
            preserved.update(normalized)
            records[index] = preserved
            replaced = True
            break
    if not replaced:
        records.append(normalized)
    records.sort(key=lambda row: str((row or {}).get("xyz_coin") or "").strip().upper())
    save_tradfi_map(records)
    return normalized


def _load_xyz_activity_by_coin() -> dict[str, bool]:
    xyz_coins: dict[str, bool] = {}
    mapping_path = _mapping_json_path()
    if not mapping_path.exists():
        return xyz_coins
    try:
        raw = json.loads(mapping_path.read_text(encoding="utf-8"))
        entries = raw if isinstance(raw, list) else list(raw.values())
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if not (entry.get("is_hip3") and str(entry.get("dex") or "").lower() == "xyz"):
                continue
            coin_field = str(entry.get("coin") or entry.get("base") or "").strip()
            if coin_field.upper().startswith("XYZ-") or coin_field.upper().startswith("XYZ:"):
                coin_name = coin_field[4:].strip().upper()
            else:
                coin_name = coin_field.upper()
            if coin_name:
                xyz_coins[coin_name] = bool(entry.get("active", True))
    except Exception:
        return {}
    return xyz_coins


def effective_tradfi_status(entry: dict[str, Any] | None, *, is_active: bool) -> str:
    row = dict(entry or {})
    raw_status = str(row.get("status") or "").strip().lower()
    tiingo_ticker = str(row.get("tiingo_ticker") or "").strip().upper()
    tiingo_fx_ticker = str(row.get("tiingo_fx_ticker") or "").strip().upper()

    if not is_active:
        return "delisted"
    if raw_status and raw_status != "delisted":
        return raw_status
    if tiingo_ticker or tiingo_fx_ticker:
        return "ok"
    return "pending"


def build_effective_tradfi_status_map() -> dict[str, str]:
    xyz_coins = _load_xyz_activity_by_coin()
    saved_map = {
        str((row or {}).get("xyz_coin") or "").strip().upper(): dict(row)
        for row in load_tradfi_map()
        if str((row or {}).get("xyz_coin") or "").strip()
    }

    statuses: dict[str, str] = {}
    for coin_name, is_active in xyz_coins.items():
        entry = saved_map.get(coin_name)
        statuses[coin_name] = effective_tradfi_status(entry, is_active=is_active)
    return statuses


def build_merged_tradfi_table() -> list[dict]:
    spec_by_coin = load_xyz_spec_by_coin()
    quote_cache = load_tradfi_quote_cache().get("quotes") or {}

    xyz_coins = _load_xyz_activity_by_coin()

    saved_map: dict[str, dict] = {}
    for row in load_tradfi_map():
        key = str((row or {}).get("xyz_coin") or "").strip().upper()
        if key:
            normalized_row = dict(row)
            normalized_row["note"] = normalize_tradfi_note(normalized_row.get("note"))
            saved_map[key] = normalized_row

    rows: list[dict] = []
    for coin_name, is_active in xyz_coins.items():
        spec = spec_by_coin.get(coin_name) or {}
        if coin_name in saved_map:
            row = apply_tradfi_spec_defaults(saved_map[coin_name], spec)
            row["_in_map"] = True
            row["status"] = effective_tradfi_status(row, is_active=is_active)
        else:
            row = {
                "xyz_coin": coin_name,
                "description": TRADFI_KNOWN_DESCRIPTIONS.get(coin_name, ""),
                "canonical_type": str(spec.get("canonical_type") or "").strip() or guess_tradfi_canonical_type(coin_name),
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
        row["pyth_link"] = _normalize_external_href(str((spec_by_coin.get(coin_name) or {}).get("underlying_href") or ""))
        row["hl_link"] = f"https://app.hyperliquid.xyz/trade/xyz:{coin_name}"
        row["hl_price"], row["tiingo_price"] = _row_prices(row, quote_cache)
        row["tiingo_symbol"] = _row_tiingo_symbol(row)
        row["tiingo_fetch_start"] = _row_fetch_start_date(row)
        rows.append(row)

    for coin_name, entry in saved_map.items():
        if coin_name in xyz_coins:
            continue
        row = apply_tradfi_spec_defaults(entry, spec_by_coin.get(coin_name) or {})
        row["_in_map"] = True
        row["pyth_link"] = _normalize_external_href(str((spec_by_coin.get(coin_name) or {}).get("underlying_href") or ""))
        row["hl_link"] = f"https://app.hyperliquid.xyz/trade/xyz:{coin_name}"
        row["hl_price"], row["tiingo_price"] = _row_prices(row, quote_cache)
        row["tiingo_symbol"] = _row_tiingo_symbol(row)
        row["tiingo_fetch_start"] = _row_fetch_start_date(row)
        rows.append(row)

    rows.sort(key=lambda row: str(row.get("xyz_coin") or ""))
    return [row for row in rows if str(row.get("status") or "").strip().lower() != "delisted"]


def load_tiingo_meta_cache() -> dict:
    path = tiingo_meta_cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def build_tiingo_meta_cache_info() -> dict[str, Any]:
    payload = load_tiingo_meta_cache()
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    fetched_at = str(payload.get("fetched_at") or "").strip()
    count = len(meta)
    if count:
        summary = f"Cache: {count:,} tickers from {fetched_at[:10] or 'unknown'}"
    else:
        summary = "No cache — fetched on first Auto-Map"
    return {
        "available": bool(count),
        "count": count,
        "fetched_at": fetched_at,
        "summary": summary,
    }


def load_tradfi_quote_cache() -> dict:
    path = tradfi_quote_cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_tradfi_quote_cache(cache: dict) -> None:
    path = tradfi_quote_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cache, indent=4, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def build_tradfi_quote_cache_info() -> dict[str, Any]:
    payload = load_tradfi_quote_cache()
    quotes = payload.get("quotes") if isinstance(payload, dict) else {}
    quotes = quotes if isinstance(quotes, dict) else {}
    fetched_at = str(payload.get("fetched_at") or "").strip()
    count = len(quotes)
    if count:
        stamp = fetched_at[:19].replace("T", " ") if fetched_at else "unknown"
        summary = f"Price cache: {count:,} quotes · {stamp} UTC"
    else:
        summary = "No price cache — load via 'Refresh prices'"
    return {
        "available": bool(count),
        "count": count,
        "fetched_at": fetched_at,
        "summary": summary,
    }


def build_xyz_spec_cache_info() -> dict[str, Any]:
    path = _xyz_spec_cache_path()
    if not path.exists():
        return {
            "available": False,
            "count": 0,
            "fetched_at": "",
            "summary": "No XYZ spec cache yet.",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    instruments = payload.get("instruments") if isinstance(payload, dict) else []
    instruments = [item for item in (instruments or []) if isinstance(item, dict)]
    fetched_at = str(payload.get("fetched_at") or "").strip() if isinstance(payload, dict) else ""
    count = len(instruments)
    summary = f"XYZ specs: {count:,} instruments" + (f" · {fetched_at[:19].replace('T', ' ')} UTC" if fetched_at else "")
    return {
        "available": bool(count),
        "count": count,
        "fetched_at": fetched_at,
        "summary": summary,
    }


def build_xyz_spec_rows() -> dict[str, Any]:
    path = _xyz_spec_cache_path()
    payload = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

    instruments = payload.get("instruments") if isinstance(payload, dict) else []
    instruments = [item for item in (instruments or []) if isinstance(item, dict)]
    fetched_at = str(payload.get("fetched_at") or "").strip() if isinstance(payload, dict) else ""

    rows: list[dict[str, str]] = []
    for row in instruments:
        coin = str(row.get("xyz_coin") or "").strip().upper()
        underlying_href = _normalize_external_href(str(row.get("underlying_href") or ""))
        rows.append(
            {
                "xyz_coin": coin,
                "canonical_type": str(row.get("canonical_type") or "").strip(),
                "instrument_label": str(row.get("instrument_label") or "").strip(),
                "description": str(row.get("description") or "").strip(),
                "underlying": str(row.get("underlying") or "").strip(),
                "max_leverage": str(row.get("max_leverage") or "").strip(),
                "pyth_symbol": str(row.get("pyth_symbol") or "").strip(),
                "pyth_link": underlying_href,
                "hl_link": f"https://app.hyperliquid.xyz/trade/xyz:{coin}" if coin else "",
            }
        )
    return {
        "fetched_at": fetched_at,
        "rows": rows,
    }


def find_tradfi_row(xyz_coin: str, rows: list[dict] | None = None) -> dict | None:
    key = str(xyz_coin or "").strip().upper()
    if key.startswith("XYZ:") or key.startswith("XYZ-"):
        key = key[4:].strip().upper()
    pool = rows if isinstance(rows, list) else build_merged_tradfi_table()
    return next(
        (row for row in pool if str(row.get("xyz_coin") or "").strip().upper() == key),
        None,
    )


def tiingo_search(query: str, api_key: str, timeout_s: float = 10.0) -> list[dict]:
    import urllib.parse
    import urllib.request

    q = urllib.parse.quote(str(query or "").strip())
    token = str(api_key or "").strip()
    if not q or not token:
        return []
    url = f"https://api.tiingo.com/tiingo/utilities/search/{q}?token={token}"
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
        data = json.loads(resp.read())
    return [item for item in (data or []) if isinstance(item, dict)]


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


def _tiingo_search_quote_keys(ticker: str) -> list[str]:
    raw = str(ticker or "").strip().upper()
    if not raw:
        return []
    keys = [raw]
    if ":" in raw:
        base = raw.split(":", 1)[0].strip().upper()
        if base and base not in keys:
            keys.append(base)
    return keys


def build_tiingo_search_price_map(api_key: str, tickers: list[str], timeout_s: float = 15.0) -> dict[str, dict[str, Any]]:
    import urllib.parse
    import urllib.request

    requested = {
        str(ticker or "").strip().upper()
        for ticker in (tickers or [])
        if str(ticker or "").strip()
    }
    if not requested:
        return {}

    lookup_to_requested: dict[str, set[str]] = {}
    for ticker in requested:
        for key in _tiingo_search_quote_keys(ticker):
            lookup_to_requested.setdefault(key, set()).add(ticker)

    out_quotes: dict[str, dict[str, Any]] = {}
    cache_payload = load_tradfi_quote_cache()
    cache_quotes = cache_payload.get("quotes") if isinstance(cache_payload, dict) else {}
    cache_quotes = cache_quotes if isinstance(cache_quotes, dict) else {}
    cache_fetched_at = str(cache_payload.get("fetched_at") or "").strip() if isinstance(cache_payload, dict) else ""

    for lookup_key, target_tickers in lookup_to_requested.items():
        cached_quote = cache_quotes.get(lookup_key)
        if not isinstance(cached_quote, dict):
            continue
        try:
            price = float(cached_quote.get("price"))
        except Exception:
            continue
        for ticker in target_tickers:
            out_quotes[ticker] = {
                "price": price,
                "source": str(cached_quote.get("source") or "cache"),
                "field": str(cached_quote.get("field") or ""),
                "quote_timestamp": str(cached_quote.get("quote_timestamp") or cache_fetched_at or ""),
            }

    token = str(api_key or "").strip()
    if not token:
        return out_quotes

    remaining_lookup_keys = {
        key
        for key, target_tickers in lookup_to_requested.items()
        if any(ticker not in out_quotes for ticker in target_tickers)
    }
    try:
        iex_url = f"https://api.tiingo.com/iex?token={urllib.parse.quote(token)}"
        iex_req = urllib.request.Request(iex_url, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(iex_req, timeout=float(timeout_s)) as resp:
            data = json.loads(resp.read())
        if isinstance(data, list):
            for quote in data:
                if not isinstance(quote, dict):
                    continue
                ticker = str(quote.get("ticker") or "").strip().upper()
                if not ticker or ticker not in remaining_lookup_keys:
                    continue
                price, field = _tiingo_pick_iex_price(quote)
                if price is None:
                    continue
                for target_ticker in lookup_to_requested.get(ticker, ()):
                    out_quotes[target_ticker] = {
                        "price": price,
                        "source": "iex_search",
                        "field": str(field or ""),
                        "quote_timestamp": str(quote.get("timestamp") or quote.get("lastSaleTimestamp") or ""),
                    }
                remaining_lookup_keys.discard(ticker)
                if not remaining_lookup_keys:
                    break
    except Exception:
        pass

    return out_quotes


def refresh_tradfi_quote_cache(api_key: str, records: list[dict] | None = None) -> dict[str, int]:
    import urllib.parse
    import urllib.request

    rows = records if isinstance(records, list) else load_tradfi_map()
    equity_tickers = {
        str(row.get("tiingo_ticker") or "").strip().upper()
        for row in rows
        if str(row.get("tiingo_ticker") or "").strip()
    }
    fx_tickers = {
        str(row.get("tiingo_fx_ticker") or "").strip().lower()
        for row in rows
        if str(row.get("tiingo_fx_ticker") or "").strip()
    }

    out_quotes: dict[str, dict] = {}
    iex_payload: list[dict] = []
    fx_payload: list[dict] = []

    try:
        iex_url = f"https://api.tiingo.com/iex?token={urllib.parse.quote(api_key)}"
        iex_req = urllib.request.Request(iex_url, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(iex_req, timeout=30) as resp:
            data = json.loads(resp.read())
        if isinstance(data, list):
            iex_payload = [quote for quote in data if isinstance(quote, dict)]
    except Exception:
        iex_payload = []

    for quote in iex_payload:
        ticker = str(quote.get("ticker") or "").strip().upper()
        if not ticker or ticker not in equity_tickers:
            continue
        price, field = _tiingo_pick_iex_price(quote)
        if price is None:
            continue
        out_quotes[ticker] = {
            "price": price,
            "source": "iex_all",
            "field": field,
            "quote_timestamp": str(quote.get("timestamp") or quote.get("lastSaleTimestamp") or ""),
        }

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
                fx_payload = [quote for quote in data if isinstance(quote, dict)]
        except Exception:
            fx_payload = []

    for quote in fx_payload:
        ticker = str(quote.get("ticker") or "").strip().lower()
        if not ticker or ticker not in fx_tickers:
            continue
        price, field = _tiingo_pick_fx_price(quote)
        if price is None:
            continue
        out_quotes[ticker] = {
            "price": price,
            "source": "fx_top",
            "field": field,
            "quote_timestamp": str(quote.get("quoteTimestamp") or ""),
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


def _tiingo_fetch_daily_start_date(ticker: str, api_key: str, timeout_s: float = 15.0) -> str | None:
    import urllib.parse
    import urllib.request

    ticker_clean = str(ticker or "").strip().upper()
    token = str(api_key or "").strip()
    if not ticker_clean or not token:
        return None

    url = f"https://api.tiingo.com/tiingo/daily/{urllib.parse.quote(ticker_clean)}?token={urllib.parse.quote(token)}"
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            payload = json.loads(resp.read())
    except Exception:
        return None

    if isinstance(payload, dict):
        start_date = str(payload.get("startDate") or "").strip()
        return start_date[:10] if start_date else None
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        start_date = str(payload[0].get("startDate") or "").strip()
        return start_date[:10] if start_date else None
    return None


def update_tiingo_start_date_for_selected(*, selected_entry: dict | None, api_key: str) -> dict[str, Any]:
    if not selected_entry:
        return {"updated": 0, "reason": "no selection"}

    xyz = str((selected_entry or {}).get("xyz_coin") or "").strip().upper()
    ticker = str((selected_entry or {}).get("tiingo_ticker") or "").strip().upper()
    if not ticker:
        return {"updated": 0, "reason": "selected symbol has no Tiingo equity ticker"}

    start_date = _tiingo_fetch_daily_start_date(ticker=ticker, api_key=api_key)
    if not start_date:
        return {"updated": 0, "reason": f"no startDate for {ticker}"}

    records = load_tradfi_map()
    index = next(
        (i for i, row in enumerate(records) if str(row.get("xyz_coin") or "").strip().upper() == xyz),
        None,
    )

    if index is None:
        row = dict(selected_entry)
        row.pop("_in_map", None)
        row["tiingo_start_date"] = start_date
        row["last_verified"] = _datetime.now(_timezone.utc).isoformat()
        records.append(normalize_tradfi_map_entry(row))
    else:
        updated_row = dict(records[index])
        updated_row["tiingo_start_date"] = start_date
        updated_row["last_verified"] = _datetime.now(_timezone.utc).isoformat()
        records[index] = normalize_tradfi_map_entry(updated_row)

    save_tradfi_map(records)
    return {
        "updated": 1,
        "xyz_coin": xyz,
        "ticker": ticker,
        "start_date": start_date,
    }


def update_tiingo_start_dates_for_all(*, api_key: str, rows: list[dict]) -> dict[str, Any]:
    by_xyz: dict[str, dict] = {
        str(row.get("xyz_coin") or "").strip().upper(): dict(row)
        for row in load_tradfi_map()
        if str(row.get("xyz_coin") or "").strip()
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
            updated_row = dict(by_xyz[xyz])
            updated_row["tiingo_start_date"] = start_date
            updated_row["last_verified"] = _datetime.now(_timezone.utc).isoformat()
            by_xyz[xyz] = normalize_tradfi_map_entry(updated_row)
        else:
            new_row = dict(row)
            new_row.pop("_in_map", None)
            new_row["tiingo_start_date"] = start_date
            new_row["last_verified"] = _datetime.now(_timezone.utc).isoformat()
            by_xyz[xyz] = normalize_tradfi_map_entry(new_row)
        updated += 1

    save_tradfi_map(list(by_xyz.values()))
    return {"updated": updated, "skipped": skipped, "errors": errors}


# ---------------------------------------------------------------------------
# Stock-perp detection
# ---------------------------------------------------------------------------

def is_hyperliquid_stock_perp_1m(*, exchange: str, dataset: str, coin: str) -> bool:
    """Return True if the coin is a Hyperliquid stock-perp 1m dataset."""
    ex_l = str(exchange or "").strip().lower()
    ds_l = str(dataset or "").strip().lower()
    coin_u = str(coin or "").strip().upper()
    if ex_l != "hyperliquid":
        return False
    if ds_l not in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
        return False
    return coin_u.startswith("XYZ:") or coin_u.startswith("XYZ-")


# ---------------------------------------------------------------------------
# US equity session helpers
# ---------------------------------------------------------------------------

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
    """Anonymous Gregorian algorithm for Easter Sunday."""
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


def is_us_market_holiday(day: _date) -> bool:
    """Return True if *day* is a US equity market holiday."""
    y = int(day.year)
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
        _nth_weekday_of_month(y, 1, 0, 3),   # MLK Day
        _nth_weekday_of_month(y, 2, 0, 3),   # Presidents' Day
        _last_weekday_of_month(y, 5, 0),     # Memorial Day
        _nth_weekday_of_month(y, 9, 0, 1),   # Labor Day
        _nth_weekday_of_month(y, 11, 3, 4),  # Thanksgiving
    }
    floating.add(_easter_sunday(y) - _timedelta(days=2))  # Good Friday
    return day in fixed or day in floating


def is_us_market_early_close(day: _date) -> bool:
    """Return True if the US market closes early on *day*."""
    if int(day.weekday()) >= 5:
        return False
    if is_us_market_holiday(day):
        return False
    thanksgiving = _nth_weekday_of_month(int(day.year), 11, 3, 4)
    if day == (thanksgiving + _timedelta(days=1)):
        return True
    if int(day.month) == 7 and int(day.day) == 3:
        return True
    if int(day.month) == 12 and int(day.day) == 24:
        return True
    return False


def tradfi_expected_minute_indices(day: _date) -> set[int]:
    """US equities regular session (09:30-16:00 ET), converted to UTC."""
    if int(day.weekday()) >= 5:
        return set()
    if _ZoneInfo is None:
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


def tradfi_expected_minute_indices_custom_close(day: _date, *, close_hour: int, close_minute: int = 0) -> set[int]:
    """US equities session with custom close time (DST-aware)."""
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


def tradfi_canonical_type_for_coin(coin: str) -> str:
    """Return the canonical TradFi type string for a coin (e.g. 'equity_us', 'fx')."""
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
    rows = load_tradfi_map()
    for row in rows:
        if str(row.get("xyz_coin") or "").strip().upper() == key:
            return str(row.get("canonical_type") or "").strip().lower()
    return ""


def uses_us_holiday_calendar(canonical_type: str) -> bool:
    return str(canonical_type or "").strip().lower() in {"equity_us", "etf", "commodity_etf", "index_etf", "commodity"}


def is_tradfi_market_holiday(day: _date, canonical_type: str) -> bool:
    ctype = str(canonical_type or "").strip().lower()
    if ctype == "fx":
        return False
    if uses_us_holiday_calendar(ctype):
        return is_us_market_holiday(day)
    return int(day.weekday()) >= 5


def fx_expected_minute_indices(day: _date) -> set[int]:
    """FX session indices (weekend boundary DST-aware)."""
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
    special_open_minute_utc: int | None = None
    special_close_minute_utc: int | None = None
    md = (int(day.month), int(day.day))
    if md == (1, 1):
        special_open_minute_utc = 23 * 60
    elif md == (12, 25):
        special_open_minute_utc = 23 * 60
    elif md in ((12, 24), (12, 31)):
        special_close_minute_utc = 22 * 60

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


def tradfi_expected_indices_for_type(day: _date, canonical_type: str) -> set[int]:
    """Return expected trading minute indices for a canonical TradFi type."""
    ctype = str(canonical_type or "").strip().lower()
    if ctype == "fx":
        return fx_expected_minute_indices(day)
    if is_tradfi_market_holiday(day, ctype):
        return set()
    if uses_us_holiday_calendar(ctype):
        if is_us_market_early_close(day):
            return tradfi_expected_minute_indices_custom_close(day, close_hour=13, close_minute=0)
    return tradfi_expected_minute_indices(day)


def tradfi_expected_minute_indices_from_session(
    *, day: _date, session_start_ms: int, session_end_ms: int
) -> set[int]:
    """Convert session start/end millisecond timestamps to minute indices for *day*."""
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
