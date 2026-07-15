"""TradFi spec sync: read XYZ coins from mapping.json and update tradfi_symbol_map.

Uses the Hyperliquid mapping.json (data/coindata/hyperliquid/mapping.json) as the
authoritative source for which XYZ stock-perp coins exist and whether they are active
or delisted.

fetch_xyz_spec() fetches the live XYZ specification index from docs.trade.xyz and
caches the result in xyz_spec.json.  load_xyz_spec() returns the cached data if
fresh, or re-fetches when stale.

Descriptions for known non-equity symbols (commodities, FX, indices) are filled from
a local lookup table.  US equity names are left empty for the user to fill in manually
via the UI — no external API calls are made during sync.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logging_helpers import human_log as _log

SERVICE = "TradFiSync"

_MAP_FILE = "tradfi_symbol_map.json"
_CACHE_FILE = "xyz_spec_cache.json"
_XYZ_SPEC_CACHE_FILE = "xyz_spec.json"
_XYZ_SPEC_URL = "https://docs.trade.xyz/consolidated-resources/specification-index"
_COINDATA_REL = Path("data") / "coindata" / "hyperliquid"

def _coindata_dir(pbgui_dir: Path | None = None) -> Path:
    return (pbgui_dir or Path.cwd()) / _COINDATA_REL


def load_spec_cache(pbgui_dir: Path | None = None) -> dict | None:
    """Load xyz_spec_cache.json and return it, or None if not found."""
    path = _coindata_dir(pbgui_dir) / _CACHE_FILE
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return None


def sync_tradfi_spec(pbgui_dir: Path | None = None) -> dict[str, Any]:
    """Sync XYZ coin list from mapping.json into tradfi_symbol_map.json.

    Descriptions are filled from the local ``_KNOWN_DESCRIPTIONS`` table for
    commodities/FX/indices.  US equity names are left empty — no API calls are made.

    Returns a summary dict:
      source, active_in_mapping, delisted_in_mapping, added_pending,
      auto_delisted, total_entries, synced_at
    """
    coindata = _coindata_dir(pbgui_dir)
    coindata.mkdir(parents=True, exist_ok=True)
    cache_path = coindata / _CACHE_FILE
    map_path = coindata / _MAP_FILE

    # ── 1. Read mapping.json ──────────────────────────────────────────────────
    mapping_path = coindata / "mapping.json"
    if not mapping_path.exists():
        raise FileNotFoundError(
            f"mapping.json not found at {mapping_path}. "
            "Run a Hyperliquid market data sync first."
        )

    mapping_raw: list[dict] = []
    try:
        raw = json.loads(mapping_path.read_text(encoding="utf-8"))
        mapping_raw = raw if isinstance(raw, list) else list(raw.values())
    except Exception as exc:
        raise RuntimeError(f"Failed to read mapping.json: {exc}") from exc

    # Filter: only XYZ HIP-3 entries
    xyz_entries = [
        e for e in mapping_raw
        if e.get("is_hip3") and str(e.get("dex") or "").lower() == "xyz"
    ]

    _log(
        "tradfi_sync",
        f"mapping.json: {len(mapping_raw)} total entries, {len(xyz_entries)} XYZ HIP-3",
        level="INFO",
    )

    # Build lookup: normalized xyz_coin → active flag
    # mapping.json uses "coin" field which is already normalized (e.g. "XYZ-TSLA").
    # Strip the "XYZ-" prefix to get the raw coin name stored in tradfi_symbol_map.
    active_map: dict[str, bool] = {}
    for e in xyz_entries:
        coin_field = str(e.get("coin") or e.get("base") or "").strip()
        # Strip XYZ- or XYZ: prefix
        if coin_field.upper().startswith("XYZ-"):
            coin_name = coin_field[4:].strip().upper()
        elif coin_field.upper().startswith("XYZ:"):
            coin_name = coin_field[4:].strip().upper()
        else:
            coin_name = coin_field.upper()
        if coin_name:
            active_map[coin_name] = bool(e.get("active", True))

    active_count = sum(1 for v in active_map.values() if v)
    delisted_count = sum(1 for v in active_map.values() if not v)
    _log(
        "tradfi_sync",
        f"XYZ coins from mapping.json: {active_count} active, {delisted_count} delisted",
        level="INFO",
    )

    # ── 2. Load existing tradfi_symbol_map ────────────────────────────────────
    existing_map: list[dict] = []
    if map_path.exists():
        try:
            data = json.loads(map_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                existing_map = data
        except Exception:
            pass

    existing_keys = {str(r.get("xyz_coin") or "").upper() for r in existing_map}

    # ── 3. Add new coins ──────────────────────────────────────────────────────
    added_pending = 0
    added_delisted = 0
    now_utc = datetime.now(timezone.utc)

    # Build index for fast updates
    entry_by_key: dict[str, dict] = {
        str(r.get("xyz_coin") or "").upper(): r for r in existing_map
    }

    # ── 2b. Load xyz_spec cache for description + canonical_type ─────────────
    _spec_by_coin: dict[str, dict] = {}
    _xyz_spec_path = coindata / _XYZ_SPEC_CACHE_FILE
    if _xyz_spec_path.exists():
        try:
            _spec_raw = json.loads(_xyz_spec_path.read_text(encoding="utf-8"))
            for _s in (_spec_raw.get("instruments") or []):
                _k = str(_s.get("xyz_coin") or "").upper()
                if _k:
                    _spec_by_coin[_k] = _s
        except Exception:
            pass

    for coin_name, is_active in active_map.items():
        if coin_name not in existing_keys:
            _spec_entry = _spec_by_coin.get(coin_name, {})
            description = str(_spec_entry.get("description") or "")
            canonical_type = str(_spec_entry.get("canonical_type") or "equity_us")

            entry: dict = {
                "xyz_coin": coin_name,
                "description": description,
                "canonical_type": canonical_type,
                "tiingo_ticker": None,
                "tiingo_fx_ticker": None,
                "tiingo_fx_invert": False,
                "tiingo_start_date": None,
                "status": "pending" if is_active else "delisted",
                "note": "",
                "last_verified": None,
                "spec_source": "mapping.json",
            }
            existing_map.append(entry)
            entry_by_key[coin_name] = entry
            if is_active:
                added_pending += 1
            else:
                added_delisted += 1
        else:
            # Existing mapping.json entry — refresh spec-derived description/type.
            entry = entry_by_key[coin_name]
            spec_entry = _spec_by_coin.get(coin_name) or {}
            spec_desc = str(spec_entry.get("description") or "").strip()
            spec_type = str(spec_entry.get("canonical_type") or "").strip().lower()
            if str(entry.get("spec_source") or "").strip().lower() == "mapping.json":
                if spec_desc:
                    entry["description"] = spec_desc
                if spec_type:
                    entry["canonical_type"] = spec_type
            elif not str(entry.get("description") or "").strip() and spec_desc:
                entry["description"] = spec_desc

    # ── 4. Mark disappeared pending coins as delisted ─────────────────────────
    # We NEVER overwrite manually-verified entries (status not "pending").
    auto_delisted = 0
    for entry in existing_map:
        key = str(entry.get("xyz_coin") or "").upper()
        status = str(entry.get("status") or "").lower()
        if key not in active_map and status == "pending":
            entry["status"] = "delisted"
            auto_delisted += 1

    # Also handle mapping.json active:false for still-pending entries
    for coin_name, is_active in active_map.items():
        if not is_active and coin_name in entry_by_key:
            entry = entry_by_key[coin_name]
            if str(entry.get("status") or "").lower() == "pending":
                entry["status"] = "delisted"
                auto_delisted += 1

    # ── 5. Write updated map atomically ─────────────────────────────────────
    tmp = map_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(existing_map, indent=4, ensure_ascii=False), encoding="utf-8")
    tmp.replace(map_path)

    # ── 6. Write thin sync manifest ──────────────────────────────────────────
    summary: dict[str, Any] = {
        "source": "mapping.json",
        "active_in_mapping": active_count,
        "delisted_in_mapping": delisted_count,
        "added_pending": added_pending,
        "added_delisted": added_delisted,
        "auto_delisted": auto_delisted,
        "total_entries": len(existing_map),
        "synced_at": now_utc.isoformat(),
    }
    cache_tmp = cache_path.with_suffix(".json.tmp")
    cache_tmp.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    cache_tmp.replace(cache_path)

    _log("tradfi_sync", f"Spec sync done: {summary}", level="INFO")
    return summary


# ── XYZ Specification Index (docs.trade.xyz) ──────────────────────────────────

def _pyth_href_to_canonical_type(href: str, underlying: str, coin: str, description: str = "") -> str:
    """Derive canonical_type from Pyth insight URL or underlying/coin fallback."""
    href_lower = href.lower()
    combined_lower = " ".join(
        part.strip().lower()
        for part in (str(underlying or ""), str(description or ""))
        if str(part or "").strip()
    )
    if "/equity.us." in href_lower or "equity.us%2" in href_lower:
        return "equity_us"
    if "/equity.kr." in href_lower or "equity.kr%2" in href_lower:
        return "equity_kr"
    if "/equity.jp." in href_lower or "equity.jp%2" in href_lower:
        return "equity_jp"
    if "/metal." in href_lower:
        return "commodity"
    if "/fx." in href_lower:
        return "fx"
    if "/commodities." in href_lower:
        return "commodity"
    if "/index." in href_lower or "/indices." in href_lower:
        return "index_etf"
    # No Pyth link — fall back on underlying text and coin name
    if underlying.upper().endswith(".KS"):
        return "equity_kr"
    if any(token in combined_lower for token in {"crude oil", "barrel", "precious metal", "natural gas", "copper", "uranium"}):
        return "commodity"
    if any(token in combined_lower for token in {" price-weighted index", " benchmark for the japanese equity market", " benchmark for the south korean equity market", " index of ", " serves as a widely followed benchmark"}):
        return "index_etf"
    upper = coin.upper()
    if upper in {"GOLD", "SILVER", "PLATINUM", "PALLADIUM", "CL", "NATGAS", "COPPER", "ALUMINIUM", "URANIUM", "BRENTOIL"}:
        return "commodity"
    if upper in {"EUR", "JPY", "GBP", "DXY"}:
        return "fx"
    if upper in {"URNM"}:
        return "commodity_etf"
    return "equity_us"


def _normalize_instrument_to_coin(instrument: str) -> str:
    """Normalize XYZ spec instrument label to our internal coin name.

    E.g. 'Gold (XAU/USD)' → 'GOLD', 'Silver (XAG/USD)' → 'SILVER', 'TSLA' → 'TSLA'
    """
    coin = re.sub(r'\s*\(.*?\)\s*$', '', instrument).strip()
    return coin.upper()


def _clean_spec_cell_text(value: str) -> str:
    text = str(value or "").replace("arrow-up-right", " ")
    return re.sub(r"\s+", " ", text).strip()


def _parse_xyz_spec_row(row: Any, fetched_at: str) -> dict[str, Any] | None:
    cells = row.find_all(attrs={"role": "cell"})
    if len(cells) < 4:
        return None

    instrument = _clean_spec_cell_text(cells[0].get_text(" ", strip=True))
    if not instrument:
        return None

    description = _clean_spec_cell_text(cells[1].get_text(" ", strip=True))
    underlying_cell = cells[2]
    underlying_text = _clean_spec_cell_text(underlying_cell.get_text(" ", strip=True))
    link = underlying_cell.find("a")
    underlying_href = link["href"] if (link and link.has_attr("href")) else ""
    max_leverage = _clean_spec_cell_text(cells[3].get_text(" ", strip=True))

    coin = _normalize_instrument_to_coin(instrument)
    canonical_type = _pyth_href_to_canonical_type(underlying_href, underlying_text, coin, description)
    if coin == "XYZ100":
        canonical_type = "index"

    import urllib.parse as _uparse

    pyth_symbol = ""
    if underlying_href and "/explore/" in underlying_href:
        pyth_symbol = _uparse.unquote(underlying_href.split("/explore/")[-1]).strip()
    elif underlying_href and "/price-feeds/" in underlying_href:
        pyth_symbol = _uparse.unquote(underlying_href.split("/price-feeds/")[-1]).strip()

    return {
        "xyz_coin": coin,
        "instrument_label": instrument,
        "underlying": underlying_text,
        "underlying_href": underlying_href,
        "pyth_symbol": pyth_symbol,
        "max_leverage": max_leverage,
        "canonical_type": canonical_type,
        "description": description,
        "fetched_at": fetched_at,
    }


def fetch_xyz_spec(pbgui_dir: Path | None = None) -> list[dict]:
    """Fetch and parse the XYZ Specification Index from docs.trade.xyz.

    Parses the ARIA-role-based div table (SSR HTML) to extract every listed
    instrument with its underlying oracle, max leverage, and derived canonical_type.

    Caches the result to ``xyz_spec.json`` so subsequent calls within 24 h are free.

    Returns a list of dicts with keys:
        xyz_coin, instrument_label, underlying, underlying_href,
        max_leverage, canonical_type, fetched_at
    Raises on network or parse error.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise ImportError(
            f"fetch_xyz_spec requires 'requests' and 'beautifulsoup4': {exc}"
        ) from exc

    coindata = _coindata_dir(pbgui_dir)

    r = requests.get(
        _XYZ_SPEC_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15,
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table_rows = soup.find_all(attrs={"role": "row"})

    results: list[dict] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for row in table_rows[1:]:  # skip header row
        entry = _parse_xyz_spec_row(row, fetched_at)
        if not entry:
            continue

        underlying_href = str(entry.get("underlying_href") or "")
        if underlying_href.startswith("//"):
            underlying_href = "https:" + underlying_href
        elif underlying_href and "://" in underlying_href and not underlying_href.startswith(("http://", "https://")):
            # Garbled scheme (e.g. 'ttps://') — strip the bad prefix, keep the rest
            underlying_href = "https://" + underlying_href.split("://", 1)[1]
        entry["underlying_href"] = underlying_href
        results.append(entry)
        _log(
            "tradfi_sync",
            f"spec: {entry['xyz_coin']} → {entry['canonical_type']} ({entry['underlying']})",
            level="DEBUG",
        )

    _log(
        "tradfi_sync",
        f"XYZ spec fetched: {len(results)} instruments from {_XYZ_SPEC_URL}",
        level="INFO",
    )

    # ── Enrich descriptions from Pyth Hermes ─────────────────────────────────
    try:
        import urllib.request as _urlreq
        _hermes_req = _urlreq.Request(
            "https://hermes.pyth.network/v2/price_feeds",
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
        )
        with _urlreq.urlopen(_hermes_req, timeout=20) as _resp:
            _feeds = json.loads(_resp.read())
        _sym_to_desc: dict[str, str] = {}
        for _f in _feeds:
            _sym = str((_f.get("attributes") or {}).get("symbol") or "").strip()
            _desc = str((_f.get("attributes") or {}).get("description") or "").strip()
            if _sym and _desc:
                _sym_to_desc[_sym] = _desc
        _enriched = 0
        for entry in results:
            ps = entry.get("pyth_symbol", "")
            if ps and ps in _sym_to_desc and not str(entry.get("description") or "").strip():
                entry["description"] = _sym_to_desc[ps]
                _enriched += 1
        _log("tradfi_sync", f"Pyth Hermes: {_enriched}/{len(results)} descriptions enriched", level="INFO")
    except Exception as _exc:
        _log("tradfi_sync", f"Pyth Hermes description lookup skipped: {_exc}", level="WARNING")

    # Cache to disk
    coindata.mkdir(parents=True, exist_ok=True)
    cache_path = coindata / _XYZ_SPEC_CACHE_FILE
    tmp = cache_path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps({"fetched_at": fetched_at, "instruments": results}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp.replace(cache_path)

    return results


# ── Tiingo Auto-Mapping ───────────────────────────────────────────────────────

_TIINGO_META_CACHE_FILE = "tiingo_meta.json"
_TIINGO_META_URL = "https://api.tiingo.com/tiingo/fundamentals/meta"

_TIINGO_DIRECT_TYPES: frozenset[str] = frozenset({
    "equity_us", "etf", "commodity_etf",
})
_NO_PROVIDER_TYPES: frozenset[str] = frozenset({
    "commodity", "fx", "index", "index_etf", "equity_kr", "equity_jp",
})

# ISO 4217 / XAU/XAG static FX map: xyz_coin → tiingo fx pair
_FX_COMMODITY_MAP: dict[str, dict] = {
    "EUR":       {"tiingo_fx_ticker": "EURUSD",  "tiingo_fx_invert": False},
    "GBP":       {"tiingo_fx_ticker": "GBPUSD",  "tiingo_fx_invert": False},
    "JPY":       {"tiingo_fx_ticker": "USDJPY",  "tiingo_fx_invert": False},
    "GOLD":      {"tiingo_fx_ticker": "XAUUSD",  "tiingo_fx_invert": False},
    "SILVER":    {"tiingo_fx_ticker": "XAGUSD",  "tiingo_fx_invert": False},
    "PLATINUM":  {"tiingo_fx_ticker": "XPTUSD",  "tiingo_fx_invert": False},
    "PALLADIUM": {"tiingo_fx_ticker": "XPDUSD",  "tiingo_fx_invert": False},
}

# xyz_coin → Tiingo ticker when the names differ (KR/JP OTC/ADR tickers, ETFs with no fundamentals)
_KNOWN_TICKER_ALIASES: dict[str, str] = {
    "URNM":     "URNM",    # Sprott Uranium Miners ETF — ETFs have no fundamentals
}

# Exchanges considered valid for US equities / ETFs in Tiingo
_US_EXCHANGES: frozenset[str] = frozenset({
    "NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "CBOE", "BATS",
})


def _auto_map_strategy_for_entry(coin: str, canonical_type: str) -> str:
    if coin in _FX_COMMODITY_MAP:
        return "mapped_fx"
    if coin in _KNOWN_TICKER_ALIASES:
        return "alias"

    normalized_type = str(canonical_type or "").strip().lower()
    if normalized_type in _NO_PROVIDER_TYPES:
        return "no_provider"
    if normalized_type in _TIINGO_DIRECT_TYPES:
        return "equity_lookup"
    return "equity_lookup"


def _tiingo_cache_dir(pbgui_dir: Path | str | None = None) -> Path:
    base = Path(pbgui_dir) if pbgui_dir else Path.cwd()
    return base / "data" / "coindata"


def fetch_tiingo_meta(
    api_key: str,
    pbgui_dir: Path | None = None,
    force_refresh: bool = False,
) -> dict[str, dict]:
    """Fetch Tiingo fundamentals/meta and cache permanently (no TTL).

    Returns dict: ticker.upper() → raw entry dict (name, exchangeCode, isActive, ...)

    Re-fetches only when ``force_refresh=True`` or cache file is missing.
    One credit consumed per fetch.
    """
    cache_dir = _tiingo_cache_dir(pbgui_dir)
    cache_path = cache_dir / _TIINGO_META_CACHE_FILE

    if not force_refresh and cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            meta = data.get("meta") or {}
            if meta:
                _log("tradfi_sync", f"Tiingo meta: {len(meta)} tickers from cache", level="INFO")
                return meta
        except Exception:
            pass

    import urllib.request as _urlreq
    url = f"{_TIINGO_META_URL}?token={api_key}"
    _log("tradfi_sync", "Fetching Tiingo fundamentals/meta (one-time API call)…", level="INFO")
    req = _urlreq.Request(
        url,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    with _urlreq.urlopen(req, timeout=60) as resp:
        raw: list[dict] = json.loads(resp.read().decode("utf-8"))

    meta_index: dict[str, dict] = {}
    for entry in raw:
        t = str(entry.get("ticker") or "").upper()
        if t:
            meta_index[t] = entry

    cache_dir.mkdir(parents=True, exist_ok=True)
    tmp = cache_path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(
            {"fetched_at": datetime.now(timezone.utc).isoformat(), "meta": meta_index},
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    tmp.replace(cache_path)
    _log("tradfi_sync", f"Tiingo meta: fetched and cached {len(meta_index)} tickers", level="INFO")
    return meta_index


def _normalize_name(s: str) -> str:
    """Lowercase, remove punctuation, collapse whitespace."""
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _pyth_desc_to_company(description: str) -> str:
    """Extract company from Pyth description: 'TESLA INC / US DOLLAR' → 'tesla inc'."""
    return _normalize_name(description.split(" / ")[0] if " / " in description else description)


_NAME_MATCH_STOPWORDS: frozenset[str] = frozenset({
    "and", "company", "co", "corp", "corporation", "group", "holding", "holdings",
    "inc", "incorporated", "limited", "ltd", "plc", "sa", "spa", "nv",
})


def _name_tokens(value: str) -> set[str]:
    return {
        token for token in _normalize_name(value).split()
        if token and token not in _NAME_MATCH_STOPWORDS
    }


def _names_match(pyth_desc: str, tiingo_name: str) -> bool:
    """Return True when the company name extracted from Pyth description matches Tiingo name."""
    p = _pyth_desc_to_company(pyth_desc)
    t = _normalize_name(tiingo_name)
    if not p or not t:
        return False
    if p == t or t.startswith(p) or p.startswith(t):
        return True

    p_tokens = _name_tokens(pyth_desc)
    t_tokens = _name_tokens(tiingo_name)
    if p_tokens and t_tokens and (t_tokens.issubset(p_tokens) or p_tokens.issubset(t_tokens)):
        return True
    return False


def auto_map_tradfi(
    api_key: str,
    pbgui_dir: Path | None = None,
    force_meta_refresh: bool = False,
) -> dict[str, Any]:
    """Auto-map pending TradFi entries to Tiingo tickers.

    Processing order per entry (only visible, non-delisted rows with ``status="pending"`` are touched):
      1. FX / precious metals  → static ISO 4217 table  → tiingo_fx_ticker, status="ok"
            2. Type-based provider   → status="no_provider" for unsupported Tiingo types
      3. _KNOWN_TICKER_ALIASES → tiingo_ticker (alias), status="alias"
      4. Direct equity lookup  → coin in Tiingo meta + name validation → status="ok"
      5. Not found             → note updated, status stays "pending"

    Returns summary dict with counts per phase plus per-category item details.
    """
    coindata = _coindata_dir(pbgui_dir)
    map_path = coindata / _MAP_FILE
    if not map_path.exists():
        raise FileNotFoundError(f"tradfi_symbol_map.json not found at {map_path}")

    records: list[dict] = json.loads(map_path.read_text(encoding="utf-8"))
    try:
        from market_data_tradfi import (
            add_tradfi_auto_map_not_found_note,
            apply_tradfi_spec_defaults,
            build_effective_tradfi_status_map,
            build_merged_tradfi_table,
            clear_tradfi_auto_map_not_found_note,
            load_xyz_spec_by_coin,
            normalize_tradfi_note,
            resolve_tradfi_canonical_type,
        )

        visible_coins = {
            str((row or {}).get("xyz_coin") or "").strip().upper()
            for row in build_merged_tradfi_table()
            if str((row or {}).get("xyz_coin") or "").strip()
        }
        effective_status_by_coin = build_effective_tradfi_status_map()
        spec_by_coin = load_xyz_spec_by_coin()
    except Exception:
        def normalize_tradfi_note(raw_note: object) -> str:
            parts = [part.strip() for part in str(raw_note or "").split("|") if part.strip()]
            if not parts:
                return ""
            cleaned: list[str] = []
            saw_auto_map_not_found = False
            for part in parts:
                if part == "auto-map: not found":
                    if saw_auto_map_not_found:
                        continue
                    saw_auto_map_not_found = True
                cleaned.append(part)
            return " | ".join(cleaned)

        def clear_tradfi_auto_map_not_found_note(raw_note: object) -> str:
            parts = [
                part.strip()
                for part in normalize_tradfi_note(raw_note).split("|")
                if part.strip() and part.strip() != "auto-map: not found"
            ]
            return " | ".join(parts)

        def add_tradfi_auto_map_not_found_note(raw_note: object) -> str:
            parts = [part.strip() for part in clear_tradfi_auto_map_not_found_note(raw_note).split("|") if part.strip()]
            parts.append("auto-map: not found")
            return " | ".join(parts)

        def apply_tradfi_spec_defaults(entry: dict[str, Any], spec_row: dict[str, Any] | None = None) -> dict[str, Any]:
            row = dict(entry or {})
            spec = dict(spec_row or {})
            if not row:
                return row
            if str(row.get("spec_source") or "").strip().lower() != "mapping.json":
                return row
            spec_description = str(spec.get("description") or "").strip()
            spec_type = str(spec.get("canonical_type") or "").strip().lower()
            if spec_description:
                row["description"] = spec_description
            if spec_type:
                row["canonical_type"] = spec_type
            return row

        def resolve_tradfi_canonical_type(entry: dict[str, Any] | None, spec_row: dict[str, Any] | None = None) -> str:
            spec_source = str((entry or {}).get("spec_source") or "").strip().lower()
            spec_type = str((spec_row or {}).get("canonical_type") or "").strip().lower()
            row_type = str((entry or {}).get("canonical_type") or "").strip().lower()
            if spec_source == "mapping.json" and spec_type:
                return spec_type
            if row_type:
                return row_type
            if spec_type:
                return spec_type
            return "equity_us"

        visible_coins = {
            str((entry or {}).get("xyz_coin") or "").strip().upper()
            for entry in records
            if str((entry or {}).get("status") or "").strip().lower() != "delisted"
        }
        effective_status_by_coin = {}
        spec_by_coin = {}
    meta_index = fetch_tiingo_meta(api_key, pbgui_dir, force_refresh=force_meta_refresh)
    now_utc = datetime.now(timezone.utc).isoformat()

    counts: dict[str, int] = {
        "mapped_equity": 0,
        "mapped_fx": 0,
        "no_provider": 0,
        "not_found": 0,
        "skipped": 0,
    }
    details: dict[str, list[str]] = {
        "mapped_equity": [],
        "mapped_fx": [],
        "no_provider": [],
        "not_found": [],
        "skipped": [],
    }

    for entry in records:
        coin = str(entry.get("xyz_coin") or "").upper()
        entry["note"] = normalize_tradfi_note(entry.get("note"))
        spec_row = spec_by_coin.get(coin) or {}
        entry.update(apply_tradfi_spec_defaults(entry, spec_row))
        canonical_type = resolve_tradfi_canonical_type(entry, spec_row)
        raw_status = str(entry.get("status") or "").strip().lower()
        status = str((effective_status_by_coin or {}).get(coin) or raw_status).strip().lower()
        if status and coin in visible_coins and status != raw_status:
            entry["status"] = status

        if not coin or coin not in visible_coins:
            continue

        if status != "pending":
            entry["note"] = clear_tradfi_auto_map_not_found_note(entry.get("note"))
            counts["skipped"] += 1
            details["skipped"].append(f"{coin} (status: {status or 'unknown'})")
            continue

        entry["note"] = clear_tradfi_auto_map_not_found_note(entry.get("note"))

        strategy = _auto_map_strategy_for_entry(coin, canonical_type)

        # 1. FX / precious metals
        if coin in _FX_COMMODITY_MAP:
            fx = _FX_COMMODITY_MAP[coin]
            entry["tiingo_fx_ticker"] = fx["tiingo_fx_ticker"]
            entry["tiingo_fx_invert"] = fx["tiingo_fx_invert"]
            entry["status"] = "ok"
            entry["last_verified"] = now_utc
            counts["mapped_fx"] += 1
            details["mapped_fx"].append(
                f"{coin} -> FX:{fx['tiingo_fx_ticker']}" + (" (inv)" if fx.get("tiingo_fx_invert") else "")
            )
            _log("tradfi_sync", f"auto-map FX: {coin} → {fx['tiingo_fx_ticker']}", level="DEBUG")
            continue

        # 2. Type-driven no provider
        if strategy == "no_provider":
            entry["status"] = "no_provider"
            entry["last_verified"] = now_utc
            counts["no_provider"] += 1
            details["no_provider"].append(f"{coin} ({canonical_type or 'unknown'})")
            _log("tradfi_sync", f"auto-map no_provider: {coin} ({canonical_type})", level="DEBUG")
            continue

        # 3. Known alias (KR/JP OTC and ETF tickers not covered by fundamentals/meta)
        alias = _KNOWN_TICKER_ALIASES.get(coin)
        if strategy == "alias" and alias:
            entry["tiingo_ticker"] = alias
            entry["status"] = "alias"
            entry["last_verified"] = now_utc
            counts["mapped_equity"] += 1
            details["mapped_equity"].append(f"{coin} -> {alias}")
            _log("tradfi_sync", f"auto-map alias: {coin} → {alias}", level="DEBUG")
            continue

        # 4. Direct equity lookup + name validation
        meta_entry = meta_index.get(coin)
        if meta_entry:
            tiingo_name = str(meta_entry.get("name") or "")
            pyth_desc = str(entry.get("description") or "")
            # Name check guards against false matches (GOLD=Barrick Gold, CL=Colgate)
            if not pyth_desc or _names_match(pyth_desc, tiingo_name):
                entry["tiingo_ticker"] = coin
                entry["status"] = "ok"
                entry["last_verified"] = now_utc
                counts["mapped_equity"] += 1
                details["mapped_equity"].append(coin)
                _log("tradfi_sync", f"auto-map equity: {coin} ('{tiingo_name}')", level="DEBUG")
                continue
            else:
                _log(
                    "tradfi_sync",
                    f"auto-map skipped name mismatch: {coin} pyth='{pyth_desc}' tiingo='{tiingo_name}'",
                    level="DEBUG",
                )

        # 5. Not found
        entry["note"] = add_tradfi_auto_map_not_found_note(entry.get("note"))
        counts["not_found"] += 1
        details["not_found"].append(f"{coin} (name mismatch)" if meta_entry else coin)

    # Save atomically
    tmp = map_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(records, indent=4, ensure_ascii=False), encoding="utf-8")
    tmp.replace(map_path)

    result: dict[str, Any] = dict(counts)
    result["details"] = details
    _log("tradfi_sync", f"auto_map_tradfi done: {counts}", level="INFO")
    return result


def load_xyz_spec(pbgui_dir: Path | None = None, max_age_hours: float = 24.0) -> list[dict] | None:
    """Return the XYZ Specification Index, using a local cache when fresh.

    If the cache is older than ``max_age_hours`` (default 24 h) or missing,
    a fresh fetch is attempted.  Returns ``None`` on error so callers can
    degrade gracefully.
    """
    coindata = _coindata_dir(pbgui_dir)
    cache_path = coindata / _XYZ_SPEC_CACHE_FILE

    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            fetched_at = data.get("fetched_at", "")
            if fetched_at:
                age_s = (
                    datetime.now(timezone.utc)
                    - datetime.fromisoformat(fetched_at)
                ).total_seconds()
                if age_s < max_age_hours * 3600:
                    return data.get("instruments", [])
        except Exception:
            pass

    try:
        return fetch_xyz_spec(pbgui_dir)
    except Exception as exc:
        _log("tradfi_sync", f"Failed to fetch XYZ spec: {exc}", level="WARNING")
        return None
