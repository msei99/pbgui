"""Exchange identities shared by cloud validation, export and metadata preparation."""

SERVICE = "Vast"
# PB8 standard names, CCXT clients and PBGui raw-data directory names differ.
CCXT_EXCHANGES = {
    "binance": "binanceusdm", "bybit": "bybit", "bitget": "bitget",
    "okx": "okx", "hyperliquid": "hyperliquid", "kucoin": "kucoinfutures",
}
SUPPORTED_EXCHANGES = tuple(CCXT_EXCHANGES)


def quote_currency(exchange):
    """Use USDC for Hyperliquid linear perps, USDT for the other supported venues."""
    return "USDC" if exchange == "hyperliquid" else "USDT"
