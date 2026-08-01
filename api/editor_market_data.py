"""Generation-neutral market metadata helpers for config editors."""

from __future__ import annotations

from typing import Any

from logging_helpers import human_log as _log


SERVICE = "EditorMarketData"


def normalize_exchanges(values: Any) -> list[str]:
    """Normalize an exchange string or list without accepting combined aliases."""
    items = values.split(",") if isinstance(values, str) else values if isinstance(values, list) else []
    result: list[str] = []
    for item in items:
        exchange = str(item or "").strip().lower()
        if exchange and exchange != "combined" and exchange not in result:
            result.append(exchange)
    return result


def symbols(exchange: str) -> list[str]:
    """Return active normalized perp symbols for one exchange."""
    from PBCoinData import CoinData

    approved, ignored = CoinData().filter_mapping(
        exchange=exchange,
        market_cap_min_m=0,
        vol_mcap_max=float("inf"),
        only_cpt=False,
        notices_ignore=False,
        tags=[],
        quote_filter=None,
        active_only=True,
        use_cache=True,
    )
    return sorted(set(approved) | set(ignored))


def tags(exchange: str) -> list[str]:
    """Return current CoinData tags for one exchange."""
    from PBCoinData import CoinData

    return CoinData().get_mapping_tags(exchange=exchange, use_cache=True)


def filter_symbols(
    exchange: str,
    market_cap: int,
    vol_mcap: float,
    only_cpt: bool,
    notices_ignore: bool,
    selected_tags: str,
) -> tuple[list[str], list[str]]:
    """Apply the common PBGui coin filters for an editor preview."""
    from PBCoinData import CoinData

    tag_list = [item.strip() for item in selected_tags.split(",") if item.strip()]
    return CoinData().filter_mapping(
        exchange=exchange,
        market_cap_min_m=market_cap,
        vol_mcap_max=vol_mcap,
        only_cpt=only_cpt,
        notices_ignore=notices_ignore,
        tags=tag_list,
        quote_filter=None,
        use_cache=True,
    )


def classify_coins(exchanges: list[str], coins: list[str]) -> dict[str, dict]:
    """Resolve selected coin names against current exchange mappings."""
    from PBCoinData import CoinData, build_symbol_mappings, normalize_symbol

    if not exchanges or not coins:
        return {}
    coin_data = CoinData()
    active_coins: set[str] = set()
    symbol_mappings: dict[str, str] = {}
    for exchange in exchanges:
        try:
            approved, ignored = coin_data.filter_mapping(
                exchange=exchange,
                market_cap_min_m=0,
                vol_mcap_max=float("inf"),
                only_cpt=False,
                notices_ignore=False,
                tags=[],
                quote_filter=None,
                active_only=True,
                use_cache=True,
            )
            active_coins.update(approved)
            active_coins.update(ignored)
            mapping = coin_data.load_mapping(exchange=exchange, use_cache=True)
            raw_symbols = [
                str(record.get("symbol") or "").strip().upper()
                for record in mapping
                if str(record.get("symbol") or "").strip()
            ]
            symbol_mappings.update(build_symbol_mappings(raw_symbols))
        except Exception as exc:
            _log(SERVICE, f"Failed to classify coins for exchange {exchange}: {exc}", level="WARNING")

    statuses: dict[str, dict] = {}
    for raw_coin in coins:
        value = str(raw_coin or "").strip()
        if not value:
            continue
        if value.lower() == "all":
            statuses[value] = {"input": value, "normalized": "all", "status": "valid"}
            continue
        normalized = str(normalize_symbol(value.upper(), symbol_mappings) or value).upper()
        statuses[value] = {
            "input": value,
            "normalized": normalized,
            "status": "valid" if normalized in active_coins else "invalid",
        }
    return statuses
