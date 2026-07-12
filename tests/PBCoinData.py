"""Mock PBCoinData module for testing.

This mock module provides minimal implementations of PBCoinData functions
needed for tests, avoiding dependencies on the full Streamlit environment.
"""


def get_symbol_for_coin(coin: str, exchange: str, use_cache=True) -> str:
    """Mock implementation of get_symbol_for_coin.

    Args:
        coin: The coin symbol (e.g., "BTC", "ETH")
        exchange: The exchange name (e.g., "binance", "hyperliquid")
        use_cache: Whether to use cached data (ignored in mock)

    Returns:
        A mock symbol string (coin + "USDT" for most cases)
    """
    # Simple mock: just return coin + "USDT"
    # This is sufficient for tests that don't rely on actual symbol lookup
    if not coin:
        return ""
    return f"{coin}USDT"
