"""Mock PBCoinData module for testing.

This mock module provides minimal implementations of PBCoinData functions
needed for tests, avoiding dependencies on the full Streamlit environment.
"""

import importlib.util
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def _hip3_module():
    """Use the real pure HIP-3 helpers without constructing CoinData."""
    spec = importlib.util.spec_from_file_location("hip3_test_helpers", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _normalize_hyperliquid_hip3_alias(symbol, mapping_record=None):
    """Forward pure alias validation to production code."""
    return _hip3_module()._normalize_hyperliquid_hip3_alias(symbol, mapping_record)


def _hip3_qualified_base(symbol):
    """Forward syntactic qualification checks to the shared pure parser."""
    return _hip3_module()._hip3_qualified_base(symbol)


def get_hyperliquid_hip3_alias(symbol, mapping=None):
    """Forward local mapping confirmation without a CoinData constructor."""
    return _hip3_module().get_hyperliquid_hip3_alias(symbol, mapping)


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
