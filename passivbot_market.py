"""Shared native Passivbot market catalog, identifier validation and display rules."""
from __future__ import annotations

import asyncio

SERVICE = "PassivbotMarket"


def _load_pb8_market_modules():
    """Import PB8.1's collision-safe market resolver only for market requests."""
    try:
        from utils import (
            AmbiguousMarketIdentifier,
            MarketIdentifierExchangeMismatch,
            UnknownMarketIdentifier,
            _approved_all_market_identifiers,
            coin_to_symbol,
            filter_markets,
            get_quote,
            load_markets,
            looks_like_exact_market_identifier,
            reject_cross_exchange_market_identifier_collisions,
            split_exchange_qualified_market_identifier,
            symbol_to_coin,
            to_standard_exchange_name,
        )
    except ImportError as exc:
        raise RuntimeError(
            "PB8's collision-safe market resolver is unavailable; update PB8 to v8.1.0 or newer"
        ) from exc
    return {
        "AmbiguousMarketIdentifier": AmbiguousMarketIdentifier,
        "MarketIdentifierExchangeMismatch": MarketIdentifierExchangeMismatch,
        "UnknownMarketIdentifier": UnknownMarketIdentifier,
        "approved_all_market_identifiers": _approved_all_market_identifiers,
        "coin_to_symbol": coin_to_symbol,
        "filter_markets": filter_markets,
        "get_quote": get_quote,
        "load_markets": load_markets,
        "looks_like_exact_market_identifier": looks_like_exact_market_identifier,
        "reject_cross_exchange_market_identifier_collisions": reject_cross_exchange_market_identifier_collisions,
        "split_exchange_qualified_market_identifier": split_exchange_qualified_market_identifier,
        "symbol_to_coin": symbol_to_coin,
        "to_standard_exchange_name": to_standard_exchange_name,
    }


def _validated_market_strings(value, field: str, *, maximum: int, max_bytes: int) -> list[str]:
    """Validate one bounded list without changing exact market identifier text."""
    if not isinstance(value, list):
        raise TypeError(f"{field} must be an array")
    if len(value) > maximum:
        raise ValueError(f"{field} exceeds the maximum of {maximum} items")
    result = []
    for item in value:
        if not isinstance(item, str):
            raise TypeError(f"{field} entries must be strings")
        normalized = item.strip()
        if not normalized:
            raise ValueError(f"{field} entries cannot be empty")
        if len(normalized.encode("utf-8")) > max_bytes:
            raise ValueError(f"{field} entries cannot exceed {max_bytes} bytes")
        if any(ord(char) < 32 or ord(char) == 127 for char in normalized):
            raise ValueError(f"{field} entries cannot contain control characters")
        result.append(normalized)
    return list(dict.fromkeys(result))


def _market_display_label(config_id: str, resolutions: list[dict], symbol_to_coin, looks_exact) -> str:
    """Return a compact label while preserving scaled bases for collisions."""
    if "::" not in config_id and not looks_exact(config_id):
        return config_id
    if not resolutions:
        return config_id
    symbol = str(resolutions[0].get("symbol") or "")
    base = symbol.split("/", 1)[0].strip()
    if base.startswith("k") and base[1:].isupper():
        return f"1000{base[1:]}"
    if base and base[0].isdigit():
        return base
    return str(symbol_to_coin(symbol, verbose=False) or base or config_id)


def _resolve_market_identifier(modules: dict, identifier: str, exchanges: list[str], quote: str | None) -> dict:
    """Resolve one identifier through PB8 and retain the submitted config value."""
    qualified_exchange, _unqualified = modules["split_exchange_qualified_market_identifier"](identifier)
    if qualified_exchange is not None and qualified_exchange not in exchanges:
        return {
            "input": identifier,
            "normalized": identifier,
            "status": "invalid",
            "reason": "exchange_mismatch",
            "detail": f"market identifier targets {qualified_exchange}, which is not selected",
            "resolutions": [],
            "display": identifier,
        }

    resolutions = []
    failures = []
    for exchange in exchanges:
        try:
            symbol = modules["coin_to_symbol"](identifier, exchange, quote=quote, verbose=False)
            resolutions.append({"exchange": exchange, "symbol": symbol})
        except modules["AmbiguousMarketIdentifier"] as exc:
            failures.append(("ambiguous", str(exc)))
        except modules["MarketIdentifierExchangeMismatch"] as exc:
            failures.append(("exchange_mismatch", str(exc)))
        except modules["UnknownMarketIdentifier"] as exc:
            failures.append(("unknown", str(exc)))

    ambiguous = next((detail for reason, detail in failures if reason == "ambiguous"), "")
    if ambiguous:
        status = "invalid"
        reason = "ambiguous"
        detail = ambiguous
        resolutions = []
    elif resolutions:
        status = "valid"
        reason = "resolved"
        detail = ""
    else:
        status = "invalid"
        reason = failures[0][0] if failures else "unknown"
        detail = failures[0][1] if failures else f"market identifier {identifier!r} is unavailable"
    return {
        "input": identifier,
        "normalized": identifier,
        "status": status,
        "reason": reason,
        "detail": detail,
        "resolutions": resolutions,
        "display": _market_display_label(
            identifier,
            resolutions,
            modules["symbol_to_coin"],
            modules["looks_like_exact_market_identifier"],
        ),
    }


async def _market_identifiers(modules: dict, payload: dict) -> dict:
    """Return PB8's official collision-aware catalog and submitted-ID statuses."""
    exchanges = _validated_market_strings(payload.get("exchanges"), "exchanges", maximum=16, max_bytes=64)
    standard_exchanges = []
    for exchange in exchanges:
        if exchange.lower() == "fake":
            raise ValueError("fake exchange market resolution requires a PB8 scenario context")
        normalized = str(modules["to_standard_exchange_name"](exchange))
        if not normalized or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for char in normalized):
            raise ValueError(f"unsupported exchange name: {exchange}")
        if normalized not in standard_exchanges:
            standard_exchanges.append(normalized)
    if not standard_exchanges:
        raise ValueError("select at least one exchange")

    identifiers = _validated_market_strings(
        payload.get("identifiers", []), "identifiers", maximum=1000, max_bytes=256
    )
    quote_value = payload.get("quote")
    if quote_value is not None and not isinstance(quote_value, str):
        raise TypeError("quote must be a string or null")
    quote = str(quote_value or "").strip().upper() or None
    if quote is not None and (len(quote) > 16 or not quote.isalnum()):
        raise ValueError("quote must contain at most 16 letters or digits")

    marketss = await asyncio.gather(
        *[modules["load_markets"](exchange, verbose=False, quote=quote) for exchange in standard_exchanges]
    )
    eligible_by_exchange = {}
    exchange_markets_quotes = []
    for exchange, markets in zip(standard_exchanges, marketss):
        eligible = modules["filter_markets"](markets, exchange, quote=quote)[0]
        eligible_by_exchange[exchange] = set(eligible)
        exchange_markets_quotes.append((exchange, eligible, modules["get_quote"](exchange, quote)))
    symbols = sorted(modules["approved_all_market_identifiers"](exchange_markets_quotes))
    if len(standard_exchanges) == 1:
        prefix = f"{standard_exchanges[0]}::"
        symbols = [symbol[len(prefix):] if symbol.startswith(prefix) else symbol for symbol in symbols]
        symbols = sorted(set(symbols))
    catalog = []
    for config_id in symbols:
        resolved = _resolve_market_identifier(modules, config_id, standard_exchanges, quote)
        eligible_resolutions = [
            item
            for item in resolved["resolutions"]
            if item["symbol"] in eligible_by_exchange.get(item["exchange"], set())
        ]
        if not eligible_resolutions:
            continue
        resolved["resolutions"] = eligible_resolutions
        catalog.append(
            {
                "config_id": config_id,
                "coin": resolved["display"],
                "display": resolved["display"],
                "resolutions": eligible_resolutions,
            }
        )
    symbols = sorted(entry["config_id"] for entry in catalog)
    label_counts = {}
    for entry in catalog:
        label_counts[entry["display"]] = label_counts.get(entry["display"], 0) + 1
    for entry in catalog:
        if label_counts[entry["display"]] > 1 and "::" in entry["config_id"]:
            exchange, native_id = entry["config_id"].split("::", 1)
            entry["display"] = f"{entry['display']} ({exchange}: {native_id})"

    statuses = {}
    ambiguous_cross_exchange = {}
    collision_check_failed = False
    try:
        await modules["reject_cross_exchange_market_identifier_collisions"](
            identifiers, standard_exchanges, quote=quote, verbose=False
        )
    except modules["AmbiguousMarketIdentifier"]:
        # PB8 reports one conflicting identifier at a time; classify only on failure.
        collision_check_failed = True
    for identifier in identifiers if collision_check_failed else []:
        try:
            await modules["reject_cross_exchange_market_identifier_collisions"](
                [identifier], standard_exchanges, quote=quote, verbose=False
            )
        except modules["AmbiguousMarketIdentifier"] as exc:
            ambiguous_cross_exchange[identifier] = str(exc)
    for identifier in identifiers:
        if identifier in ambiguous_cross_exchange:
            statuses[identifier] = {
                "input": identifier,
                "normalized": identifier,
                "status": "invalid",
                "reason": "ambiguous",
                "detail": ambiguous_cross_exchange[identifier],
                "resolutions": [],
                "display": identifier,
            }
            continue
        status = _resolve_market_identifier(modules, identifier, standard_exchanges, quote)
        if status["status"] == "valid":
            eligible_resolutions = [
                item
                for item in status["resolutions"]
                if item["symbol"] in eligible_by_exchange.get(item["exchange"], set())
            ]
            if eligible_resolutions:
                status["resolutions"] = eligible_resolutions
            else:
                status.update(
                    status="invalid",
                    reason="inactive_or_ineligible",
                    detail=f"market identifier {identifier!r} does not select an active linear swap",
                    resolutions=[],
                )
        statuses[identifier] = status

    return {
        "contract_version": 1,
        "exchanges": standard_exchanges,
        "symbols": symbols,
        "catalog": catalog,
        "statuses": statuses,
    }

