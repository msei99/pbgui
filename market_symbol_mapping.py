"""Pure helpers for canonical exchange market names."""

from __future__ import annotations

import re


SERVICE = "MarketSymbolMapping"


def _remove_power_of_ten_prefix(value: str) -> str:
    """Remove a leading power-of-ten multiplier from a market base."""
    return re.sub(r"^1(?:0+)(?!\d)", "", value)


def disambiguate_multiplier_market_coins(mapping: list) -> list:
    """Keep a multiplier prefix when the same quote also lists its bare base."""
    rows = [dict(row) if isinstance(row, dict) else row for row in (mapping or [])]
    available_bases: set[tuple[str, str]] = set()
    candidates: list[tuple[dict, str, str, str]] = []

    for row in rows:
        if not isinstance(row, dict) or not bool(row.get("swap")) or row.get("linear") is False:
            continue
        quote = str(row.get("quote") or "").strip().upper()
        base = str(row.get("base") or "").strip().upper()
        if not base:
            ccxt_symbol = str(row.get("ccxt_symbol") or "").strip().upper()
            base = ccxt_symbol.split("/", 1)[0].strip() if "/" in ccxt_symbol else ""
        if not quote or not base:
            continue
        cleaned = _remove_power_of_ten_prefix(base)
        available_bases.add((quote, base))
        candidates.append((row, quote, base, cleaned))

    for row, quote, base, cleaned in candidates:
        if cleaned and cleaned != base and (quote, cleaned) in available_bases:
            row["coin"] = base
    return rows
