"""
Comprehensive tests for Hyperliquid AWS S3 integration (hyperliquid_aws.py).

Tests cover:
- Coin name normalization for Hyperliquid S3 archive
  - K-prefix handling for high-supply coins (BONK, FLOKI, PEPE, SHIB, LUNC, DOGS, NEIRO)
  - Case normalization and whitespace handling
  - Symbol format parsing (BTC/USDT, BTC_USDC:USDC)
  - Quote currency stripping (USDT, USDC, USD)
  - Idempotency and consistency checks
- S3 object construction for l2Book data
  - Correct S3 key generation with normalized coins
  - Date range handling (multi-day downloads)
  - Hour filtering and full-day downloads
  - Output path structure validation
  - Mixed coin types (k-prefix and regular coins)

Background:
Hyperliquid's public S3 archive uses a special k-prefix notation for certain
high-supply coins (e.g., BONK → kBONK, PEPE → kPEPE). This normalization is
critical for successful S3 downloads, as the archive uses these prefixed names
in S3 keys: market_data/{date}/{hour}/{dataset}/{kBONK}.lz4

All S3 operations must apply normalization before constructing keys to avoid
404 NoSuchKey errors.
"""

import pytest
from datetime import date
from pathlib import Path

from hyperliquid_aws import _normalize_archive_coin, build_hyperliquid_l2book_s3_objects
from market_data import normalize_market_data_coin_dir


# ============================================================================
# Tests for coin normalization
# ============================================================================


class TestNormalizeArchiveCoin:
    """
    Test the _normalize_archive_coin() function.

    This function is responsible for converting coin names to Hyperliquid's
    S3 archive format. Critical for successful S3 downloads as the archive uses
    these normalized names in keys: market_data/{date}/{hour}/{dataset}/{coin}.lz4

    K-prefix rules:
    - BONK, FLOKI, LUNC, PEPE, SHIB, DOGS, NEIRO → get k-prefix
    - Already k-prefixed coins → preserved
    - Regular coins (BTC, ETH, etc.) → unchanged
    - All coins → uppercased
    """

    def test_k_prefix_coins(self):
        """
        Test that high-supply coins receive k-prefix normalization.

        These specific coins use k-prefix in Hyperliquid's S3 archive:
        BONK → kBONK, PEPE → kPEPE, etc.

        This is critical - using "BONK" instead of "kBONK" results in
        404 NoSuchKey errors from S3.
        """
        # Known k-prefix coins
        assert _normalize_archive_coin("BONK") == "kBONK"
        assert _normalize_archive_coin("FLOKI") == "kFLOKI"
        assert _normalize_archive_coin("LUNC") == "kLUNC"
        assert _normalize_archive_coin("PEPE") == "kPEPE"
        assert _normalize_archive_coin("SHIB") == "kSHIB"
        assert _normalize_archive_coin("DOGS") == "kDOGS"
        assert _normalize_archive_coin("NEIRO") == "kNEIRO"

    def test_k_prefix_coins_lowercase(self):
        """
        Test case normalization for k-prefix coins.

        All coin inputs should be uppercased after normalization,
        regardless of input case. Ensures "bonk", "BONK", "Bonk"
        all produce "kBONK".
        """
        assert _normalize_archive_coin("bonk") == "kBONK"
        assert _normalize_archive_coin("pepe") == "kPEPE"
        assert _normalize_archive_coin("shib") == "kSHIB"

    def test_k_prefix_coins_mixed_case(self):
        """Test that mixed case input works for k-prefix coins."""
        assert _normalize_archive_coin("Bonk") == "kBONK"
        assert _normalize_archive_coin("PePe") == "kPEPE"

    def test_already_k_prefixed(self):
        """
        Test idempotency: already k-prefixed coins remain unchanged.

        If a coin is already in archive format (kBONK), it should pass
        through without modification. This ensures the function is safe
        to call multiple times.
        """
        assert _normalize_archive_coin("kBONK") == "kBONK"
        assert _normalize_archive_coin("kPEPE") == "kPEPE"
        assert _normalize_archive_coin("kFLOKI") == "kFLOKI"

    def test_regular_coins(self):
        """
        Test that regular coins pass through unchanged (except case).

        Most coins (BTC, ETH, SOL, etc.) don't require k-prefix.
        They should only be uppercased.
        """
        assert _normalize_archive_coin("BTC") == "BTC"
        assert _normalize_archive_coin("ETH") == "ETH"
        assert _normalize_archive_coin("SOL") == "SOL"
        assert _normalize_archive_coin("AVAX") == "AVAX"

    def test_regular_coins_lowercase(self):
        """Test that lowercase regular coins are uppercased."""
        assert _normalize_archive_coin("btc") == "BTC"
        assert _normalize_archive_coin("eth") == "ETH"
        assert _normalize_archive_coin("sol") == "SOL"

    def test_symbol_formats(self):
        """
        Test extraction of base coin from various symbol formats.

        Handles common exchange notation:
        - BTC/USDT → BTC
        - BTC_USDC:USDC → BTC
        - BONK/USDT → kBONK (with k-prefix applied)

        The function must extract the base coin, then apply normalization rules.
        """
        # Symbol with quote currency
        assert _normalize_archive_coin("BTC/USDT") == "BTC"
        assert _normalize_archive_coin("BONK/USDC") == "kBONK"

        # CCXT-style symbols
        assert _normalize_archive_coin("BTC_USDT:USDT") == "BTC"
        assert _normalize_archive_coin("BONK_USDC:USDC") == "kBONK"

    def test_quote_currency_stripping(self):
        """
        Test removal of quote currency suffixes.

        Handles Binance-style concatenated pairs:
        - BTCUSDT → BTC
        - BONKUSDC → kBONK (strip quote then apply k-prefix)
        - ETHUSDC → ETH
        """
        assert _normalize_archive_coin("BTCUSDT") == "BTC"
        assert _normalize_archive_coin("ETHUSDC") == "ETH"
        assert _normalize_archive_coin("BONKUSDC") == "kBONK"

    def test_whitespace_handling(self):
        """
        Test that leading/trailing whitespace is removed.

        User input may contain whitespace from copy-paste operations.
        Function should strip whitespace before normalization.
        """
        assert _normalize_archive_coin("  BTC  ") == "BTC"
        assert _normalize_archive_coin("  BONK  ") == "kBONK"
        assert _normalize_archive_coin("\tBTC\n") == "BTC"

    def test_empty_input(self):
        """
        Test handling of empty or whitespace-only input.

        Edge case: function should return empty string for empty input
        rather than throwing an exception.
        """
        assert _normalize_archive_coin("") == ""
        assert _normalize_archive_coin("   ") == ""

    def test_invalid_k_prefix(self):
        """Test that invalid K-prefixed names are corrected."""
        # If someone passes "KBONK", it should be normalized to "kBONK"
        assert _normalize_archive_coin("KBONK") == "kBONK"
        assert _normalize_archive_coin("KPEPE") == "kPEPE"

    def test_idempotency(self):
        """
        Test that applying normalization multiple times is safe.

        normalize(normalize(coin)) == normalize(coin)

        This is important for code paths where normalization might be
        applied in multiple places (e.g., UI input + job processing).
        """
        for coin in ["BTC", "BONK", "kBONK", "PEPE", "ETH"]:
            once = _normalize_archive_coin(coin)
            twice = _normalize_archive_coin(once)
            assert once == twice, f"Normalization not idempotent for {coin}"

    def test_real_world_examples(self):
        """Test real-world examples from the archive."""
        # Examples seen in actual S3 bucket structure
        assert _normalize_archive_coin("BTC") == "BTC"
        assert _normalize_archive_coin("ETH") == "ETH"
        assert _normalize_archive_coin("BONK") == "kBONK"
        assert _normalize_archive_coin("PEPE") == "kPEPE"
        assert _normalize_archive_coin("SHIB") == "kSHIB"
        assert _normalize_archive_coin("WIF") == "WIF"
        assert _normalize_archive_coin("POPCAT") == "POPCAT"


# ============================================================================
# Tests for S3 object construction
# ============================================================================


class TestBuildHyperliquidL2BookS3Objects:
    """
    Test the build_hyperliquid_l2book_s3_objects() function.

    This function constructs S3Object instances for downloading l2Book data
    from Hyperliquid's public S3 archive. Each object represents one hour
    of l2Book data for a specific coin.

    S3 key format: market_data/{YYYYMMDD}/{HH}/l2Book/{COIN}.lz4
    Output path: {out_dir}/l2Book/{COIN}/{YYYYMMDD-HH}.lz4

    Critical: Must apply coin normalization before constructing keys, otherwise
    downloads will fail with 404 errors.

    Covers:
    - K-prefix coin handling in S3 keys
    - Regular coin handling
    - Date range expansion (multi-day downloads)
    - Hour filtering vs full-day downloads
    - Output path structure validation
    """

    def test_k_prefix_coins_in_s3_keys(self):
        """
        Test that k-prefix coins generate correct S3 keys.

        Critical test: BONK must become kBONK in the S3 key.
        Using "BONK" instead of "kBONK" causes 404 NoSuchKey errors.
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BONK",
            start_date="20250101",
            end_date="20250101",
            out_dir="/tmp/test",
            hours=[0]
        )

        assert len(objects) == 1
        # Verify S3 key uses kBONK, not BONK
        assert objects[0].key == "market_data/20250101/00/l2Book/kBONK.lz4"

    def test_regular_coins_in_s3_keys(self):
        """
        Test that regular coins generate correct S3 keys.

        Regular coins (BTC, ETH, etc.) should use their name as-is
        (uppercased) in the S3 key.
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BTC",
            start_date="20250101",
            end_date="20250101",
            out_dir="/tmp/test",
            hours=[0]
        )

        assert len(objects) == 1
        assert objects[0].key == "market_data/20250101/00/l2Book/BTC.lz4"

    def test_multiple_hours(self):
        """
        Test that specifying multiple hours generates multiple objects.

        Each hour requires a separate S3 download, so the function should
        generate one S3Object per hour. Hours are zero-padded in keys (00-23).
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BTC",
            start_date="20250101",
            end_date="20250101",
            out_dir="/tmp/test",
            hours=[0, 1, 23]
        )

        assert len(objects) == 3
        assert objects[0].key == "market_data/20250101/00/l2Book/BTC.lz4"
        assert objects[1].key == "market_data/20250101/01/l2Book/BTC.lz4"
        assert objects[2].key == "market_data/20250101/23/l2Book/BTC.lz4"

    def test_multiple_days(self):
        """
        Test that date ranges generate objects for all days.

        start_date="20250101", end_date="20250103" should generate
        objects for Jan 1, 2, and 3 (inclusive range).
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BTC",
            start_date="20250101",
            end_date="20250103",
            out_dir="/tmp/test",
            hours=[0]
        )

        assert len(objects) == 3
        assert objects[0].key == "market_data/20250101/00/l2Book/BTC.lz4"
        assert objects[1].key == "market_data/20250102/00/l2Book/BTC.lz4"
        assert objects[2].key == "market_data/20250103/00/l2Book/BTC.lz4"

    def test_output_path_structure(self):
        """
        Test that output paths follow correct directory structure.

        Output format: {out_dir}/l2Book/{CCXT_COIN_DIR}/{YYYYMMDD-HH}.lz4

        Local paths use normalize_market_data_coin_dir() for consistency with
        other datasets (e.g., 1m/1m_api), while S3 keys still use archive coin
        normalization (kBONK, kPEPE, ...).
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BONK",
            start_date="20250101",
            end_date="20250101",
            out_dir="/tmp/test",
            hours=[0, 12]
        )

        assert len(objects) == 2
        # Verify output paths use current normalized local coin directory (CCXT-style)
        coin_dir = normalize_market_data_coin_dir("hyperliquid", "BONK")
        assert objects[0].out_path == Path(f"/tmp/test/l2Book/{coin_dir}/20250101-00.lz4")
        assert objects[1].out_path == Path(f"/tmp/test/l2Book/{coin_dir}/20250101-12.lz4")

    def test_date_objects(self):
        """
        Test that Python date objects work as input (not just strings).

        Function should accept both:
        - String dates: "20250101"
        - date objects: date(2025, 1, 1)
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BTC",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            out_dir="/tmp/test",
            hours=[0]
        )

        assert len(objects) == 2
        assert objects[0].key == "market_data/20250101/00/l2Book/BTC.lz4"
        assert objects[1].key == "market_data/20250102/00/l2Book/BTC.lz4"

    def test_all_hours_when_not_specified(self):
        """
        Test that hours=None downloads all 24 hours.

        When hours parameter is None/omitted, function should generate
        objects for all 24 hours (00-23) of each day.

        Verifies zero-padding: 00, 01, ..., 09, 10, ..., 23
        """
        objects = build_hyperliquid_l2book_s3_objects(
            coin="BTC",
            start_date="20250101",
            end_date="20250101",
            out_dir="/tmp/test",
            hours=None
        )

        assert len(objects) == 24
        # Verify hour formatting with zero-padding
        assert objects[0].key == "market_data/20250101/00/l2Book/BTC.lz4"
        assert objects[9].key == "market_data/20250101/09/l2Book/BTC.lz4"
        assert objects[10].key == "market_data/20250101/10/l2Book/BTC.lz4"
        assert objects[23].key == "market_data/20250101/23/l2Book/BTC.lz4"

    def test_consistency_across_normalizable_inputs(self):
        """
        Test that different input forms produce the same S3 keys.

        All these inputs should produce identical S3 keys:
        - "BONK", "bonk", "BONKUSDC", "BONK/USDC", "BONK_USDC:USDC"

        This ensures normalization is applied consistently before
        S3 key construction, regardless of user input format.
        """
        # All these should produce the same S3 keys
        for coin_input in ["BONK", "bonk", "BONKUSDC", "BONK/USDC", "BONK_USDC:USDC"]:
            objects = build_hyperliquid_l2book_s3_objects(
                coin=coin_input,
                start_date="20250101",
                end_date="20250101",
                out_dir="/tmp/test",
                hours=[0]
            )
            assert len(objects) == 1
            assert objects[0].key == "market_data/20250101/00/l2Book/kBONK.lz4", \
                f"Failed for input: {coin_input}"
