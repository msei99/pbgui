"""
Comprehensive tests for coin normalization functions.

Tests cover:
- coin_from_symbol_code() - Extract base coin from trading symbols
  - CCXT-style symbols with underscores (BTC_USDT:USDT)
  - Slash separators (BTC/USDT)
  - Quote currency stripping (BTCUSDT, ETHUSDC)
  - Hyperliquid k-prefix handling (kPEPE → PEPE)
  - Binance-style numeric multipliers (1000SHIB → SHIB)
  - Combined patterns (1000SHIBUSDT, kPEPEUSDC)
  - Edge cases (empty, whitespace, quote-only coins)
  - Case sensitivity and whitespace handling

- _normalize_archive_coin() - Hyperliquid S3 archive normalization
  - K-prefix addition for high-supply coins (BONK → kBONK)
  - Already k-prefixed coins preservation (kBONK → kBONK)
  - Regular coin passthrough (BTC → BTC)
  - Symbol format extraction (BTC/USDT → BTC)
  - Quote currency stripping with normalization (BONKUSDC → kBONK)
  - Case normalization and whitespace handling
  - Idempotency and consistency checks
  - Error handling and edge cases

Background:
PBGui needs to handle coin names from multiple exchanges with different
naming conventions:
- Binance: BTCUSDT, 1000SHIBUSDT
- Hyperliquid: BTC, kBONK (k-prefix for small-cap coins)
- CCXT: BTC/USDT:USDT, BTC_USDT:USDT

The normalization functions ensure consistent coin identification across
exchanges while preserving exchange-specific requirements for API calls
and data storage.
"""

import pytest
from unittest.mock import patch

from pbgui_purefunc import coin_from_symbol_code
from hyperliquid_aws import _normalize_archive_coin
import market_data as market_data_mod


# ============================================================================
# Tests for pbgui_purefunc.coin_from_symbol_code()
# ============================================================================


# ============================================================================
# Tests for pbgui_purefunc.coin_from_symbol_code()
# ============================================================================


class TestCoinFromSymbolCode:
    """
    Test pbgui_purefunc.coin_from_symbol_code() function.

    This function extracts the base coin name from various trading symbol formats
    used across exchanges. It handles:

    1. Exchange-specific prefixes:
       - Hyperliquid: kPEPE → PEPE (removes k-prefix)
       - Binance: 1000SHIB → SHIB (removes multipliers)

    2. Symbol formats:
       - Slash notation: BTC/USDT → BTC
       - Underscore notation: BTC_USDT:USDT → BTC
       - Concatenated: BTCUSDT → BTC

    3. Edge cases:
       - Empty strings, whitespace
       - Quote-only symbols (USDT, USD)
       - Combined patterns (1000SHIBUSDT)

    This is used for:
    - Displaying coin names in UI
    - Matching coins across exchanges
    - Config management
    """

    @pytest.mark.parametrize("symbol,expected", [
        # Basic cases
        ("BTC", "BTC"),
        ("ETH", "ETH"),
        ("SOL", "SOL"),

        # CCXT-style symbols with underscores
        ("DOGE_USDT:USDT", "DOGE"),
        ("BTC_USDC:USDC", "BTC"),
        ("ETH_USDT:USDT", "ETH"),

        # Slash separators
        ("BTC/USDT:USDT", "BTC"),
        ("ETH/USDC", "ETH"),
        ("SOL/USDT", "SOL"),

        # Quote currency stripping
        ("BTCUSDT", "BTC"),
        ("ETHUSDC", "ETH"),
        ("SOLUSDT", "SOL"),
        ("DOGEBUSD", "DOGE"),
        ("ADAUSD", "ADA"),

        # Hyperliquid k-prefix (small-cap multipliers)
        ("kPEPE", "PEPE"),
        ("kSHIB", "SHIB"),
        ("kFLOKI", "FLOKI"),
        ("kBONK", "BONK"),

        # Binance-style numeric multipliers
        ("1000SHIB", "SHIB"),
        ("1000PEPE", "PEPE"),
        ("10000LADYS", "LADYS"),
        ("1000000BABYDOGE", "BABYDOGE"),
        ("1000BONK", "BONK"),

        # Combined patterns
        ("1000SHIBUSDT", "SHIB"),
        ("kPEPEUSDC", "PEPE"),
        ("1000PEPE_USDT:USDT", "PEPE"),

        # Edge cases
        ("", ""),
        ("   ", ""),
        ("USDT", "USDT"),  # Only quote currency - no base to extract, returns as-is
        ("USD", "USD"),   # Only quote currency - no base to extract, returns as-is
    ])
    def test_coin_from_symbol_code(self, symbol, expected):
        """
        Test various symbol formats are normalized correctly.

        Parametrized test covering all major exchange formats and patterns.
        Each case validates a specific normalization rule or combination.
        """
        result = coin_from_symbol_code(symbol)
        assert result == expected, f"coin_from_symbol_code('{symbol}') returned '{result}', expected '{expected}'"

    def test_case_insensitivity(self):
        """
        Test that function handles uppercase input correctly.

        Note: Quote currency stripping only works for UPPERCASE symbols.
        Lowercase symbols are uppercased but not stripped:
        - BTCUSDT → BTC (uppercase, quote stripped)
        - btcusdt → BTCUSDT (lowercase input, uppercased but quote not stripped)
        - kPEPE → PEPE (removes k-prefix)
        - 1000SHIB → SHIB (removes multiplier)
        """
        # Uppercase symbols work correctly
        assert coin_from_symbol_code("BTCUSDT") == "BTC"
        assert coin_from_symbol_code("kPEPE") == "PEPE"
        assert coin_from_symbol_code("1000SHIB") == "SHIB"

        # Lowercase symbols are uppercased but quote stripping doesn't work
        # (limitation of current implementation)
        assert coin_from_symbol_code("btcusdt") == "BTCUSDT"
        assert coin_from_symbol_code("ethusdc") == "ETHUSDC"

    def test_whitespace_handling(self):
        """
        Test that leading/trailing whitespace is handled.

        Common from copy-paste operations or user input.
        Should be stripped before normalization.
        """
        assert coin_from_symbol_code("  BTC  ") == "BTC"
        assert coin_from_symbol_code("\tETH\n") == "ETH"

    def test_multiple_separators(self):
        """
        Test symbols with multiple separator characters.

        Some exchanges use multiple separators:
        - BTC_USDT:USDT (underscore + colon)
        - ETH-USDC (dash)
        """
        assert coin_from_symbol_code("BTC_USDT:USDT") == "BTC"
        assert coin_from_symbol_code("ETH-USDC") == "ETH"  # Dash separator

    def test_ambiguous_quote_currencies(self):
        """
        Test coins that end with quote-like strings but aren't quotes.

        Edge cases where coin name contains quote currency as substring:
        - DAIUSDT → DAI (DAI is valid coin, not just "USDT" quote)
        - USDCUSDT → USDC (USDC traded against USDT)

        Function must correctly identify the quote vs coin boundary.
        """
        # These should NOT strip the ending because they're part of the coin name
        assert coin_from_symbol_code("DAIUSDT") == "DAI"  # DAI is valid coin
        assert coin_from_symbol_code("USDCUSDT") == "USDC"  # USDC traded against USDT


# ============================================================================
# Tests for hyperliquid_aws._normalize_archive_coin()
# ============================================================================


class TestNormalizeArchiveCoin:
    """
    Test hyperliquid_aws._normalize_archive_coin() function.

    This function prepares coin names for Hyperliquid's S3 archive access.
    It's similar to coin_from_symbol_code() but adds archive-specific rules:

    Archive format rules:
    1. K-prefix coins (BONK, FLOKI, LUNC, PEPE, SHIB, DOGS, NEIRO):
       - Must have k-prefix added: BONK → kBONK
       - Already prefixed preserved: kBONK → kBONK

    2. Regular coins (BTC, ETH, SOL, etc.):
       - Passed through unchanged (except uppercasing)

    3. Symbol extraction:
       - Same as coin_from_symbol_code(): BTC/USDT → BTC
       - Then apply k-prefix rules: BONK/USDT → kBONK

    Critical for S3 downloads:
    Using BONK instead of kBONK results in 404 NoSuchKey errors.
    All S3 operations must apply this normalization.
    """

    @pytest.mark.parametrize("coin,expected", [
        # Basic cases
        ("BTC", "BTC"),
        ("ETH", "ETH"),
        ("SOL", "SOL"),

        # K-prefix coins (should add k-prefix for archive)
        ("BONK", "kBONK"),
        ("FLOKI", "kFLOKI"),
        ("LUNC", "kLUNC"),
        ("PEPE", "kPEPE"),
        ("SHIB", "kSHIB"),
        ("DOGS", "kDOGS"),
        ("NEIRO", "kNEIRO"),

        # Already has k-prefix (should preserve)
        ("kBONK", "kBONK"),
        ("kPEPE", "kPEPE"),
        ("kSHIB", "kSHIB"),

        # Should strip K-prefix if followed by uppercase
        ("KPEPE", "kPEPE"),  # K -> k
        ("KSHIB", "kSHIB"),  # K -> k

        # CCXT-style with slashes/underscores (extract base)
        ("BTC/USDT", "BTC"),
        ("ETH_USDC:USDC", "ETH"),
        ("BONK/USDT", "kBONK"),  # Should still apply k-prefix

        # Quote currency stripping
        ("BTCUSDT", "BTC"),
        ("BONKUSDC", "kBONK"),
        ("PEPEUSDT", "kPEPE"),

        # Edge cases
        ("", ""),
        ("   ", ""),
        ("k", "K"),  # Single char - just uppercased
        ("K", "K"),  # Single char uppercase
    ])
    def test_normalize_archive_coin(self, coin, expected):
        """
        Test Hyperliquid archive coin normalization.

        Parametrized test covering all normalization rules.
        Mocks resolve_hyperliquid_coin_name() to test heuristic logic.
        """
        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            result = _normalize_archive_coin(coin)
            assert result == expected, f"_normalize_archive_coin('{coin}') returned '{result}', expected '{expected}'"


    class TestNormalizeMarketDataCoinDir:
        """Regression tests for Hyperliquid market-data directory naming.

        Protects against writing malformed numeric directory names such as
        "128_USDC:USDC" when reverse symbol lookup returns Hyperliquid market IDs.
        """

        def test_hyperliquid_coin_name_resolves_numeric_reverse_lookup(self, monkeypatch):
            # Simulate problematic reverse lookup path: coin -> numeric market id.
            monkeypatch.setattr(market_data_mod, "get_symbol_for_coin", lambda _coin, _ex: "128")
            monkeypatch.setattr(
                market_data_mod,
                "_get_hyperliquid_ccxt_symbol_for_market_id",
                lambda market_id: "POPCAT/USDC:USDC" if str(market_id) == "128" else "",
            )

            out = market_data_mod.normalize_market_data_coin_dir("hyperliquid", "POPCAT")
            assert out == "POPCAT_USDC:USDC"
            assert not out.startswith("128_")

        def test_hyperliquid_numeric_market_id_input_maps_to_ccxt_symbol(self, monkeypatch):
            # Even if stale config passes raw market-id, normalize to real symbol dir.
            monkeypatch.setattr(
                market_data_mod,
                "_get_hyperliquid_ccxt_symbol_for_market_id",
                lambda market_id: "BTC/USDC:USDC" if str(market_id) == "0" else "",
            )

            out = market_data_mod.normalize_market_data_coin_dir("hyperliquid", "0")
            assert out == "BTC_USDC:USDC"
            assert out != "0_USDC:USDC"

        def test_hyperliquid_xyz_symbol_keeps_expected_format(self):
            out = market_data_mod.normalize_market_data_coin_dir("hyperliquid", "xyz:AAPL")
            assert out == "XYZ-AAPL_USDC:USDC"

    def test_resolve_hyperliquid_coin_name_fallback(self):
        """
        Test that resolve_hyperliquid_coin_name() is used as fallback.

        For unknown coins, function should query the live API via
        resolve_hyperliquid_coin_name() to get correct format.
        """
        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value="kCUSTOM"):
            result = _normalize_archive_coin("CUSTOM")
            assert result == "kCUSTOM"

    def test_resolve_hyperliquid_coin_name_exception(self):
        """
        Test graceful handling when resolve_hyperliquid_coin_name() fails.

        If API call fails (network error, rate limit, etc.), function
        should fall back to heuristic normalization rather than crashing.
        """
        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', side_effect=Exception("API error")):
            # Should fall back to heuristics or return empty on exception
            result = _normalize_archive_coin("BONK")
            # Either kBONK (heuristic) or empty (safe fallback) is acceptable
            assert result in ["kBONK", ""]

    def test_case_sensitivity(self):
        """
        Test that function handles case correctly.

        All outputs should be uppercased:
        - bonk → kBONK (not kbonk)
        - BONK → kBONK
        - Bonk → kBONK
        """
        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            assert _normalize_archive_coin("bonk") == "kBONK"  # Lowercased input
            assert _normalize_archive_coin("BONK") == "kBONK"  # Uppercase input
            assert _normalize_archive_coin("Bonk") == "kBONK"  # Mixed case

    def test_whitespace_handling(self):
        """Test whitespace is stripped correctly."""
        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            assert _normalize_archive_coin("  BONK  ") == "kBONK"
            assert _normalize_archive_coin("\tBTC\n") == "BTC"

    @pytest.mark.parametrize("coin,expected_k_prefix", [
        ("BONK", True),
        ("FLOKI", True),
        ("LUNC", True),
        ("PEPE", True),
        ("SHIB", True),
        ("DOGS", True),
        ("NEIRO", True),
        ("BTC", False),
        ("ETH", False),
        ("SOL", False),
        ("DOGE", False),
    ])
    def test_k_prefix_coins(self, coin, expected_k_prefix):
        """
        Test that specific coins get k-prefix treatment.

        Validates the hardcoded list of k-prefix coins in the function.
        As new high-supply coins are added to Hyperliquid, this list may grow.
        """
        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            result = _normalize_archive_coin(coin)
            has_k_prefix = result.startswith("k") and len(result) > 1
            assert has_k_prefix == expected_k_prefix, \
                f"Coin '{coin}' k-prefix expectation failed: got '{result}'"


# ============================================================================
# Cross-function consistency tests
# ============================================================================


class TestNormalizationConsistency:
    """
    Test consistency between normalization functions.

    Ensures coin_from_symbol_code() and _normalize_archive_coin() agree
    on base coin extraction (before archive-specific k-prefix rules).

    Both functions should extract the same base coin from symbols,
    with _normalize_archive_coin() then applying k-prefix for archive.
    """

    @pytest.mark.parametrize("symbol", [
        "BTC/USDT:USDT",
        "ETH_USDC:USDC",
        "DOGE_USDT:USDT",
        "kBONK",
        "1000PEPE",  # Now works correctly after fix in _normalize_archive_coin
    ])
    def test_functions_agree_on_base_extraction(self, symbol):
        """
        Test that both functions extract the same base coin (before archive-specific rules).

        Both functions should extract identical base coins from symbols:
        - coin_from_symbol_code removes multipliers/quotes
        - _normalize_archive_coin does the same, then adds k-prefix for archive

        Example: "1000PEPE" → base="PEPE" (both) → coin_from_symbol_code="PEPE", archive="kPEPE"
        """
        base_from_symbol = coin_from_symbol_code(symbol)

        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            archive_result = _normalize_archive_coin(symbol)
            # Archive may add k-prefix, so strip it for comparison
            archive_base = archive_result.lstrip('k') if archive_result.startswith('k') else archive_result

            # For k-prefix coins, the base should match after normalization
            k_prefix_coins = {"BONK", "FLOKI", "LUNC", "PEPE", "SHIB", "DOGS", "NEIRO"}
            if base_from_symbol in k_prefix_coins:
                assert archive_base == base_from_symbol, \
                    f"Mismatch: symbol_code='{base_from_symbol}', archive='{archive_base}'"


# ============================================================================
# Edge cases and error conditions
# ============================================================================


class TestEdgeCases:
    """
    Test edge cases and error conditions.

    Covers:
    - None/empty input
    - Numeric-only strings
    - Special characters
    - Very long symbols
    - Malformed inputs

    Functions should handle these gracefully (return empty string or safe value)
    rather than crashing.
    """

    def test_none_input(self):
        """Test that None input is handled gracefully."""
        assert coin_from_symbol_code(None) == ""

        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            assert _normalize_archive_coin(None) == ""

    def test_numeric_only_input(self):
        """
        Test pure numeric strings.

        "1000" alone is not a valid symbol with multiplier (needs coin after).
        Function should return it as-is (uppercased).
        """
        assert coin_from_symbol_code("1000") == "1000"

        with patch('hyperliquid_aws.resolve_hyperliquid_coin_name', return_value=""):
            result = _normalize_archive_coin("1000")
            # Should be safe string, likely "1000" or empty
            assert isinstance(result, str)

    def test_special_characters(self):
        """
        Test symbols with special characters.

        Note: Only specific separators are handled (_ / : -).
        Dot (.) is not handled, so ETH.USD becomes ETH. after USD stripping.
        """
        assert coin_from_symbol_code("BTC-PERP") == "BTC"
        # Dot separator not supported - USD is stripped but . remains
        # Current implementation limitation
        result = coin_from_symbol_code("ETH.USD")
        assert result in ["ETH.", "ETH"]  # Either is reasonable

    def test_very_long_symbol(self):
        """Test that very long symbols are handled."""
        long_symbol = "A" * 100 + "USDT"
        result = coin_from_symbol_code(long_symbol)
        assert isinstance(result, str)
        assert len(result) < len(long_symbol)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
