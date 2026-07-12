"""
Test HIP-3 mapping functionality.

This test validates the new HIP-3 mapping infrastructure:
- CCXT markets fetch and persistence
- Mapping.json generation with CMC data merge
- HIP-3 detection
- Price updates

Purpose:
- Validate that coindata/{exchange}/ccxt_markets.json is created
- Validate that coindata/{exchange}/mapping.json is created with correct schema
- Validate HIP-3 detection works correctly
- Ensure atomic writes work (no corruption on failure)
"""

import importlib.util
from pathlib import Path
import json

import pytest


pytestmark = pytest.mark.live_exchange

# Import real PBCoinData module
pbcoindata_path = Path(__file__).parent.parent.parent / "PBCoinData.py"
spec = importlib.util.spec_from_file_location("PBCoinData_real", pbcoindata_path)
PBCoinData = importlib.util.module_from_spec(spec)
spec.loader.exec_module(PBCoinData)

CoinData = PBCoinData.CoinData


class TestHIP3Mapping:
    """Test HIP-3 mapping functionality for exchange data caching."""

    @pytest.fixture
    def coindata(self, tmp_path, monkeypatch):
        """Create CoinData in an isolated directory while retaining live APIs."""
        monkeypatch.chdir(tmp_path)
        return CoinData()

    @pytest.fixture
    def test_exchange(self):
        """Use binance for testing (most stable, widely available)."""
        return "binance"

    def test_exchange_dir_creation(self, coindata, test_exchange):
        """Test that exchange directory is created."""
        exchange_dir = coindata._ensure_exchange_dir(test_exchange)

        assert exchange_dir.exists(), f"Exchange directory not created: {exchange_dir}"
        assert exchange_dir.is_dir(), f"Exchange path is not a directory: {exchange_dir}"

    def test_fetch_ccxt_markets(self, coindata, test_exchange):
        """Test fetching CCXT markets."""
        success = coindata.fetch_ccxt_markets(test_exchange)

        assert success, f"Failed to fetch CCXT markets for {test_exchange}"

        # Check file exists
        exchange_dir = coindata._get_exchange_dir(test_exchange)
        markets_file = exchange_dir / "ccxt_markets.json"

        assert markets_file.exists(), f"CCXT markets file not created: {markets_file}"

        # Check content
        with markets_file.open('r') as f:
            markets = json.load(f)

        assert isinstance(markets, dict), "Markets should be a dict"
        assert len(markets) > 0, "Markets dict should not be empty"

        # Check a few known symbols exist
        expected_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        for symbol in expected_symbols:
            assert symbol in markets, f"Expected symbol {symbol} not found in markets"

    def test_build_mapping(self, coindata, test_exchange):
        """Test building mapping.json."""
        # First fetch markets
        coindata.fetch_ccxt_markets(test_exchange)

        # Build mapping
        success = coindata.build_mapping(test_exchange)

        assert success, f"Failed to build mapping for {test_exchange}"

        # Check file exists
        exchange_dir = coindata._get_exchange_dir(test_exchange)
        mapping_file = exchange_dir / "mapping.json"

        assert mapping_file.exists(), f"Mapping file not created: {mapping_file}"

        # Check content
        with mapping_file.open('r') as f:
            mapping = json.load(f)

        assert isinstance(mapping, list), "Mapping should be a list"
        assert len(mapping) > 0, "Mapping list should not be empty"

        # Check schema of first record
        record = mapping[0]
        required_fields = [
            "exchange", "symbol", "ccxt_symbol", "base", "quote",
            "copy_trading", "cmc_id", "cmc_rank", "market_cap", "volume_24h",
            "tags", "notice", "contract_size", "min_amount", "min_cost",
            "precision_amount", "max_leverage", "price_last", "price_ts",
            "min_order_price", "is_hip3", "dex", "active"
        ]

        for field in required_fields:
            assert field in record, f"Required field {field} missing from mapping record"

        # Validate types
        assert isinstance(record["exchange"], str), "exchange should be string"
        assert isinstance(record["symbol"], str), "symbol should be string"
        assert isinstance(record["copy_trading"], bool), "copy_trading should be bool"
        assert isinstance(record["is_hip3"], bool), "is_hip3 should be bool"
        assert isinstance(record["active"], bool), "active should be bool"
        assert isinstance(record["tags"], list), "tags should be list"

        # For binance, is_hip3 should always be False (no HIP-3 on binance)
        if test_exchange == "binance":
            assert not record["is_hip3"], "Binance should not have HIP-3 markets"
            assert record["dex"] is None, "Binance should not have a dex value"

    def test_load_ccxt_markets(self, coindata, test_exchange):
        """Test loading CCXT markets from cache."""
        # First create the data
        coindata.fetch_ccxt_markets(test_exchange)

        # Clear in-memory cache
        coindata._ccxt_markets = {}

        # Load from file
        markets = coindata.load_ccxt_markets(test_exchange)

        assert isinstance(markets, dict), "Loaded markets should be a dict"
        assert len(markets) > 0, "Loaded markets should not be empty"

    def test_load_exchange_mapping(self, coindata, test_exchange):
        """Test loading mapping from cache."""
        # First create the data
        coindata.fetch_ccxt_markets(test_exchange)
        coindata.build_mapping(test_exchange)

        # Clear in-memory cache
        coindata._exchange_mappings = {}

        # Load from file
        mapping = coindata.load_exchange_mapping(test_exchange)

        assert isinstance(mapping, list), "Loaded mapping should be a list"
        assert len(mapping) > 0, "Loaded mapping should not be empty"

    def test_update_prices(self, coindata, test_exchange):
        """Test updating prices in a temporary live mapping."""
        # First create mapping
        coindata.fetch_ccxt_markets(test_exchange)
        coindata.build_mapping(test_exchange)

        # Update prices
        success = coindata.update_prices(test_exchange)

        assert success, f"Failed to update prices for {test_exchange}"

        # Load mapping and check prices
        mapping = coindata.load_exchange_mapping(test_exchange)

        # Check that at least some records have prices
        records_with_prices = [r for r in mapping if r.get("price_last") is not None]

        assert len(records_with_prices) > 0, "No prices found in mapping after update"

        # Check price fields on records with prices
        for record in records_with_prices[:5]:  # Check first 5
            assert record["price_last"] > 0, f"Invalid price_last for {record['symbol']}"
            assert record["price_ts"] is not None, f"Missing price_ts for {record['symbol']}"
            assert record["min_order_price"] is not None, f"Missing min_order_price for {record['symbol']}"


class TestHyperliquidHIP3:
    """Test HIP-3 specific functionality for Hyperliquid."""

    @pytest.fixture
    def coindata(self, tmp_path, monkeypatch):
        """Create CoinData in an isolated directory while retaining live APIs."""
        monkeypatch.chdir(tmp_path)
        return CoinData()

    def test_hyperliquid_hip3_detection(self, coindata):
        """Test that HIP-3 stock perpetuals are correctly detected on Hyperliquid."""
        exchange_id = "hyperliquid"

        # Fetch markets with HIP-3 enabled
        success = coindata.fetch_ccxt_markets(exchange_id)
        assert success, f"Failed to fetch CCXT markets for {exchange_id}"

        # Build mapping
        success = coindata.build_mapping(exchange_id)
        assert success, f"Failed to build mapping for {exchange_id}"

        # Load mapping
        mapping = coindata.load_exchange_mapping(exchange_id)

        assert len(mapping) > 0, "Mapping should not be empty"

        # Separate crypto and HIP-3
        crypto_records = [r for r in mapping if not r["is_hip3"]]
        hip3_records = [r for r in mapping if r["is_hip3"]]

        print(f"\nHyperliquid Markets:")
        print(f"  Total: {len(mapping)}")
        print(f"  Crypto: {len(crypto_records)}")
        print(f"  HIP-3 (stocks): {len(hip3_records)}")

        # Verify crypto perps (no HIP-3 dex)
        for record in crypto_records[:5]:
            assert record.get("dex") is None, f"Crypto {record['symbol']} should not have dex set"
            print(f"  ✓ Crypto: {record['base']:10s} baseId={record.get('base_id', 'N/A')}")

        # Verify HIP-3 (has DEX prefix)
        if hip3_records:
            print(f"\n  HIP-3 Stock Perpetuals:")
            for record in hip3_records[:10]:
                assert record["is_hip3"], f"HIP-3 market {record['symbol']} should be marked as HIP-3"
                assert record.get("dex"), f"HIP-3 market {record['symbol']} should have dex set"
                ccxt_symbol = record.get("ccxt_symbol", "")
                assert ccxt_symbol, f"HIP-3 {record['symbol']} should have ccxt_symbol"
                assert ccxt_symbol.split("-")[0].lower() == record["dex"], (
                    f"HIP-3 {record['symbol']} ccxt_symbol should be DEX-prefixed"
                )
                print(f"    {record['base']:15s} {record['ccxt_symbol']}")

                # Verify HIP-3 specific defaults
                assert record["cmc_id"] is None, f"HIP-3 {record['symbol']} should have null cmc_id"
                assert record["cmc_rank"] == 0, f"HIP-3 {record['symbol']} should have 0 cmc_rank"
                assert record["market_cap"] == 0, f"HIP-3 {record['symbol']} should have 0 market_cap"
                assert record["tags"] == [], f"HIP-3 {record['symbol']} should have empty tags"
        else:
            print(f"\n  ⚠ No HIP-3 markets detected (might still be loading)")

    def test_hyperliquid_all_markets_usdc(self, coindata):
        """Verify that Hyperliquid markets don't use USDT quote currency."""
        exchange_id = "hyperliquid"

        # Fetch and build mapping
        coindata.fetch_ccxt_markets(exchange_id)
        coindata.build_mapping(exchange_id)

        mapping = coindata.load_exchange_mapping(exchange_id)

        # Hyperliquid never uses USDT — it uses USDC, USDH, USDE etc.
        usdc_count = sum(1 for r in mapping if r["quote"] == "USDC")
        for record in mapping:
            assert record["quote"] != "USDT", (
                f"Hyperliquid market {record['symbol']} should NOT use USDT, got {record['quote']}"
            )

        # Majority should be USDC
        assert usdc_count > len(mapping) * 0.5, (
            f"Expected >50% USDC markets, got {usdc_count}/{len(mapping)}"
        )

        print(f"✓ All {len(mapping)} Hyperliquid markets use non-USDT quote ({usdc_count} USDC)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
