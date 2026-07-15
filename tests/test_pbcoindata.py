"""
Comprehensive unit tests for PBCoinData.py.

Tests cover the CoinData class and module-level functions WITHOUT
requiring live API calls (CMC, CCXT). All external interactions are mocked.

Areas tested:
- Module-level functions:
  - remove_powers_of_ten()
  - normalize_symbol()
  - get_normalized_coins()
  - get_symbol_for_coin()
  - build_symbol_mappings()
    - data-driven CMC matching behavior

- CoinData class:
  - Constructor and default state
  - Config load/save (pbgui.ini)
  - Property getters/setters with side effects
  - Exchange directory management
  - CCXT markets cache (save/load/atomic writes)
  - Exchange mapping cache (save/load/atomic writes)
  - build_mapping() — record schema and field population
  - _detect_hip3() — HIP-3 detection logic
  - has_new_config/data/metadata — freshness checks
  - load_symbols / load_symbols_all
  - list_symbols / filter_by_market_cap — CMC data filtering
  - PID file management
"""

import sys
import json
import time
import threading
import configparser
import importlib.util
from dataclasses import FrozenInstanceError
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from credential_store import CredentialStore

# Ensure project root is on path
ROOT_DIR = Path(__file__).parent.parent.resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Import CoinData from the real module using importlib to avoid mock module interference
pbcoindata_path = ROOT_DIR / "PBCoinData.py"
spec = importlib.util.spec_from_file_location("PBCoinData_real", pbcoindata_path)
PBCoinData_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(PBCoinData_mod)

CoinData = PBCoinData_mod.CoinData
remove_powers_of_ten = PBCoinData_mod.remove_powers_of_ten
normalize_symbol = PBCoinData_mod.normalize_symbol
compute_coin_name = PBCoinData_mod.compute_coin_name
get_normalized_coins = PBCoinData_mod.get_normalized_coins
get_symbol_for_coin = PBCoinData_mod.get_symbol_for_coin
build_symbol_mappings = PBCoinData_mod.build_symbol_mappings


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def tmp_workdir(tmp_path, monkeypatch):
    """Set up a temporary working directory with minimal pbgui.ini."""
    import pbgui_purefunc

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: tmp_path / "pbgui.ini")
    # Create minimal directory structure
    (tmp_path / "data" / "coindata").mkdir(parents=True)
    (tmp_path / "data" / "pid").mkdir(parents=True)
    # Create minimal pbgui.ini
    config = configparser.ConfigParser()
    config["exchanges"] = {
        "binance.swap": "['BTCUSDT', 'ETHUSDT', '1000SHIBUSDT', 'SOLUSDT']",
        "hyperliquid.swap": "['BTCUSDC', 'ETHUSDC', 'kPEPEUSDC', 'kBONKUSDC']",
    }
    config["coinmarketcap"] = {
        "fetch_limit": "5000",
        "fetch_interval": "24",
        "metadata_interval": "1",
    }
    with open(tmp_path / "pbgui.ini", "w") as f:
        config.write(f)
    CredentialStore(tmp_path / "data" / "credentials").create_cmc("test_key_123")
    return tmp_path


@pytest.fixture
def coindata(tmp_workdir):
    """CoinData instance in a clean temporary directory."""
    return CoinData()


@pytest.fixture
def sample_cmc_data():
    """Sample CMC listings data for testing."""
    return {
        "data": [
            {
                "id": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "slug": "bitcoin",
                "cmc_rank": 1,
                "tags": ["mineable", "pow"],
                "self_reported_market_cap": None,
                "quote": {"USD": {
                    "price": 95000.0,
                    "volume_24h": 50000000000,
                    "market_cap": 1900000000000,
                }},
            },
            {
                "id": 1027,
                "symbol": "ETH",
                "name": "Ethereum",
                "slug": "ethereum",
                "cmc_rank": 2,
                "tags": ["smart-contracts", "defi"],
                "self_reported_market_cap": None,
                "quote": {"USD": {
                    "price": 3200.0,
                    "volume_24h": 20000000000,
                    "market_cap": 380000000000,
                }},
            },
            {
                "id": 5994,
                "symbol": "SHIB",
                "name": "Shiba Inu",
                "slug": "shiba-inu",
                "cmc_rank": 15,
                "tags": ["memes"],
                "self_reported_market_cap": None,
                "quote": {"USD": {
                    "price": 0.000022,
                    "volume_24h": 500000000,
                    "market_cap": 12000000000,
                }},
            },
            {
                "id": 5426,
                "symbol": "SOL",
                "name": "Solana",
                "slug": "solana",
                "cmc_rank": 5,
                "tags": ["smart-contracts"],
                "self_reported_market_cap": None,
                "quote": {"USD": {
                    "price": 200.0,
                    "volume_24h": 5000000000,
                    "market_cap": 90000000000,
                }},
            },
        ]
    }


@pytest.fixture
def sample_metadata():
    """Sample CMC metadata for testing."""
    return {
        "data": {
            "1": {"notice": None},
            "1027": {"notice": None},
            "5994": {"notice": "This coin has a notice"},
            "5426": {"notice": None},
        }
    }


@pytest.fixture
def sample_ccxt_markets():
    """Sample CCXT markets dict for mapping tests."""
    return {
        "BTC/USDT:USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "swap": True,
            "spot": False,
            "active": True,
            "contractSize": 1.0,
            "info": {"maxLeverage": 125},
            "limits": {
                "amount": {"min": 0.001},
                "cost": {"min": 5.0},
                "leverage": {"max": 125}
            },
            "precision": {"amount": 0.001, "price": 0.01},
        },
        "ETH/USDT:USDT": {
            "id": "ETHUSDT",
            "symbol": "ETH/USDT:USDT",
            "base": "ETH",
            "quote": "USDT",
            "swap": True,
            "spot": False,
            "active": True,
            "contractSize": 1.0,
            "info": {"maxLeverage": 100},
            "limits": {
                "amount": {"min": 0.01},
                "cost": {"min": 5.0},
                "leverage": {"max": 100}
            },
            "precision": {"amount": 0.01, "price": 0.01},
        },
        "BTC/USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT",
            "base": "BTC",
            "quote": "USDT",
            "swap": False,
            "spot": True,
            "active": True,
            "contractSize": None,
            "info": {},
            "limits": {"amount": {"min": 0.0001}, "cost": {"min": 10}, "leverage": {"max": None}},
            "precision": {"amount": 0.0001, "price": 0.01},
        },
    }


@pytest.fixture
def sample_hyperliquid_ccxt_markets():
    """Sample Hyperliquid CCXT markets with HIP-3 and crypto."""
    return {
        "BTC/USDC:USDC": {
            "id": "0", "symbol": "BTC/USDC:USDC", "base": "BTC", "quote": "USDC",
            "swap": True, "spot": False, "active": True, "contractSize": 1.0,
            "info": {"maxLeverage": 50, "name": "BTC"},
            "limits": {"amount": {"min": 0.0001}, "cost": {"min": 10}, "leverage": {"max": 50}},
            "precision": {"amount": 0.0001, "price": 0.1},
        },
        "ETH/USDC:USDC": {
            "id": "1", "symbol": "ETH/USDC:USDC", "base": "ETH", "quote": "USDC",
            "swap": True, "spot": False, "active": True, "contractSize": 1.0,
            "info": {"maxLeverage": 50, "name": "ETH"},
            "limits": {"amount": {"min": 0.001}, "cost": {"min": 10}, "leverage": {"max": 50}},
            "precision": {"amount": 0.001, "price": 0.01},
        },
        "XYZ-TSLA/USDC:USDC": {
            "id": "110001", "symbol": "XYZ-TSLA/USDC:USDC", "base": "XYZ-TSLA",
            "quote": "USDC", "swap": True, "spot": False, "active": True,
            "contractSize": 1.0,
            "info": {"hip3": True, "dex": "xyz", "onlyIsolated": True},
            "limits": {"amount": {"min": 0.01}, "cost": {"min": 10}, "leverage": {"max": 10}},
            "precision": {"amount": 0.01, "price": 0.01},
        },
        "CASH-HOOD/USDT0:USDT0": {
            "id": "110050", "symbol": "CASH-HOOD/USDT0:USDT0", "base": "CASH-HOOD",
            "quote": "USDT0", "swap": True, "spot": False, "active": True,
            "contractSize": 1.0,
            "info": {"hip3": True, "dex": "cash", "onlyIsolated": True},
            "limits": {"amount": {"min": 0.1}, "cost": {"min": 10}, "leverage": {"max": 10}},
            "precision": {"amount": 0.1, "price": 0.001},
        },
    }


# ============================================================================
# remove_powers_of_ten() Tests
# ============================================================================

class TestRemovePowersOfTen:
    """Test multiplier prefix removal."""

    @pytest.mark.parametrize("input_str,expected", [
        ("1000SHIB", "SHIB"),
        ("10000SATS", "SATS"),
        ("1000000BABYDOGE", "BABYDOGE"),
        ("100PEPE", "PEPE"),
        ("10WEN", "WEN"),
        ("BTC", "BTC"),
        ("ETH", "ETH"),
        ("", ""),
    ])
    def test_removes_multipliers(self, input_str, expected):
        assert remove_powers_of_ten(input_str) == expected

    def test_does_not_remove_non_powers(self):
        """Regular digits that aren't powers of 10 should be kept."""
        assert remove_powers_of_ten("123ABC") == "123ABC"

    def test_handles_only_number(self):
        """String with only a power of ten."""
        result = remove_powers_of_ten("1000")
        assert result == ""


# ============================================================================
# compute_coin_name() Tests
# ============================================================================

class TestComputeCoinName:
    """Test PB7-compatible coin name computation from market_id + quote.

    compute_coin_name derives the coin name from the exchange market_id
    (not CCXT base), stripping contract suffixes, separators, quote
    currency, k-prefix, and 1000x multipliers.
    """

    @pytest.mark.parametrize("market_id,quote,expected", [
        # Standard USDT pairs (Binance/Bybit/Bitget format)
        ("BTCUSDT", "USDT", "BTC"),
        ("ETHUSDT", "USDT", "ETH"),
        ("SOLUSDT", "USDT", "SOL"),
        # 1000x prefix stripping
        ("1000SHIBUSDT", "USDT", "SHIB"),
        ("1000BONKUSDT", "USDT", "BONK"),
        ("1000PEPEUSDT", "USDT", "PEPE"),
        ("1000000BABYDOGEUSDT", "USDT", "BABYDOGE"),
        ("1000000MOGUSDT", "USDT", "MOG"),
        ("10000SATSUSDT", "USDT", "SATS"),
        ("1000CATUSDT", "USDT", "CAT"),
        # Bybit special format
        ("HPOS10IUSDT", "USDT", "HPOSI"),
        ("SHIB1000USDT", "USDT", "SHIB"),
        # Coins with USD in name — NOT stripped (unlike normalize_symbol)
        ("RLUSDUSDT", "USDT", "RLUSD"),
        ("USDEUSDT", "USDT", "USDE"),
        # Bitget: market_id matches trading symbol, not CCXT base
        ("DEGENUSDT", "USDT", "DEGEN"),
        # Bitget SUSDT-quoted (synthetic)
        ("SBTCSUSDT", "SUSDT", "SBTC"),
        ("SETHSUSDT", "SUSDT", "SETH"),
        ("SXRPSUSDT", "SUSDT", "SXRP"),
        # OKX format: dashes + -SWAP suffix
        ("BTC-USDT-SWAP", "USDT", "BTC"),
        ("ETH-USDT-SWAP", "USDT", "ETH"),
        ("0G-USDT-SWAP", "USDT", "0G"),
        # Gateio format: underscores
        ("BTC_USDT", "USDT", "BTC"),
        ("RED_USDT", "USDT", "RED"),
        # KuCoin futures format: quote + M contract suffix, XBT alias
        ("XBTUSDTM", "USDT", "BTC"),
        ("ETHUSDTM", "USDT", "ETH"),
        ("SOLUSDTM", "USDT", "SOL"),
        # USDC pairs (Hyperliquid)
        ("kPEPEUSDC", "USDC", "PEPE"),
        ("kBONKUSDC", "USDC", "BONK"),
        ("kSHIBUSDC", "USDC", "SHIB"),
        ("KPEPEUSDC", "USDC", "PEPE"),
        ("KBONKUSDC", "USDC", "BONK"),
        ("KSHIBUSDC", "USDC", "SHIB"),
        ("BTCUSDC", "USDC", "BTC"),
        # Bybit/Bitget USDC PERP suffix (bare PERP without separator)
        ("BTCPERP", "USDC", "BTC"),
        ("ETHPERP", "USDC", "ETH"),
        ("AAVEPERP", "USDC", "AAVE"),
        ("1000BONKPERP", "USDC", "BONK"),
        ("1000PEPEPERP", "USDC", "PEPE"),
        # PERP as actual coin name should NOT be stripped
        ("PERPUSDT", "USDT", "PERP"),
        # No quote provided — just strips prefixes
        ("BTC", "", "BTC"),
        ("1000SHIB", "", "SHIB"),
        # Empty
        ("", "", ""),
        ("", "USDT", ""),
    ])
    def test_compute_coin_name(self, market_id, quote, expected):
        """compute_coin_name derives coin from market_id, not CCXT base."""
        assert compute_coin_name(market_id, quote) == expected

    def test_rlusd_not_stripped(self):
        """RLUSD stays intact — normalize_symbol would incorrectly give RL."""
        # market_id RLUSDUSDT: strip USDT → RLUSD (correct)
        assert compute_coin_name("RLUSDUSDT", "USDT") == "RLUSD"

    def test_bitget_degen_correct(self):
        """Bitget DEGENUSDT → DEGEN (not DegenReborn from CCXT base)."""
        assert compute_coin_name("DEGENUSDT", "USDT") == "DEGEN"

    def test_gateio_red_correct(self):
        """Gateio RED_USDT → RED (not RedLang from CCXT base)."""
        assert compute_coin_name("RED_USDT", "USDT") == "RED"


# ============================================================================
# normalize_symbol() Tests
# ============================================================================

class TestNormalizeSymbol:
    """Test symbol normalization across exchanges."""

    @pytest.mark.parametrize("symbol,expected", [
        # Basic USDT stripping
        ("BTCUSDT", "BTC"),
        ("ETHUSDT", "ETH"),
        ("SOLUSDT", "SOL"),
        # USDC stripping
        ("BTCUSDC", "BTC"),
        ("ETHUSDC", "ETH"),
        # Hyperliquid k-prefix
        ("kPEPE", "PEPE"),
        ("kBONK", "BONK"),
        ("kSHIB", "SHIB"),
        ("KPEPE", "PEPE"),
        ("KBONK", "BONK"),
        ("KSHIB", "SHIB"),
        # With quote suffix + k-prefix
        ("kPEPEUSDC", "PEPE"),
        ("KPEPEUSDC", "PEPE"),
        # Multiplier prefixes
        ("1000SHIBUSDT", "SHIB"),
        ("1000PEPEUSDT", "PEPE"),
        ("1000000BABYDOGEUSDT", "BABYDOGE"),
        # Already clean
        ("BTC", "BTC"),
        ("ETH", "ETH"),
        # HIP-3 aliases normalize to PBGui's canonical XYZ-* form
        ("xyz:TSLA", "XYZ-TSLA"),
        ("XYZ:TSLA", "XYZ-TSLA"),
        ("XYZ:TSLA/USDC:USDC", "XYZ-TSLA"),
        ("XYZ-TSLA/USDC:USDC", "XYZ-TSLA"),
        # Hyphenated stock-perp style coins must not be over-stripped
        ("XYZ-EUR", "XYZ-EUR"),
        ("XYZ-HYUNDAI", "XYZ-HYUNDAI"),
        # Empty/edge
        ("", ""),
        (None, ""),
    ])
    def test_normalize(self, symbol, expected):
        assert normalize_symbol(symbol) == expected

    def test_stablecoin_passthrough(self):
        """Stablecoins return as-is, not stripped further."""
        assert normalize_symbol("USDT") == "USDT"
        assert normalize_symbol("USDC") == "USDC"

    def test_with_mappings(self):
        """Pre-built mappings override default normalization."""
        mappings = {"SHIB": "SHIB", "1000SHIB": "SHIB"}
        assert normalize_symbol("1000SHIB", mappings) == "SHIB"


# ============================================================================
# get_normalized_coins() Tests
# ============================================================================

class TestGetNormalizedCoins:
    """Test unique coin extraction from symbol lists."""

    def test_deduplicates(self):
        """Same coin in USDT and USDC variants is deduplicated."""
        symbols = ["BTCUSDT", "BTCUSDC", "ETHUSDT"]
        result = get_normalized_coins(symbols)
        assert result == ["BTC", "ETH"]

    def test_sorted_output(self):
        """Result is sorted alphabetically."""
        symbols = ["SOLUSDT", "BTCUSDT", "ETHUSDT"]
        result = get_normalized_coins(symbols)
        assert result == ["BTC", "ETH", "SOL"]

    def test_empty_input(self):
        """Empty list returns empty list."""
        assert get_normalized_coins([]) == []
        assert get_normalized_coins(None) == []

    def test_multipliers_normalized(self):
        """Multiplier-prefixed symbols normalized before dedup."""
        symbols = ["1000SHIBUSDT", "SHIBUSDT"]
        result = get_normalized_coins(symbols)
        assert result == ["SHIB"]

    def test_k_prefix_normalized(self):
        """K-prefix symbols normalized before dedup."""
        symbols = ["kPEPEUSDC", "PEPEUSDT"]
        result = get_normalized_coins(symbols)
        assert result == ["PEPE"]


# ============================================================================
# get_symbol_for_coin() Tests
# ============================================================================

class TestGetSymbolForCoin:
    """Test reverse coin→symbol mapping."""

    def test_fallback_usdt(self, tmp_workdir):
        """Unknown coin falls back to {coin}USDT for non-Hyperliquid."""
        # Clear cache to avoid cross-test interference
        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        result = get_symbol_for_coin("UNKNOWN", "binance.swap", use_cache=False)
        assert result == "UNKNOWNUSDT"

    def test_fallback_usdc_hyperliquid(self, tmp_workdir):
        """Unknown coin falls back to {coin}USDC for Hyperliquid."""
        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        result = get_symbol_for_coin("UNKNOWN", "hyperliquid.swap", use_cache=False)
        assert result == "UNKNOWNUSDC"

    def test_known_coin_returns_exchange_symbol(self, tmp_workdir):
        """Known coin returns exchange-specific symbol."""
        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        result = get_symbol_for_coin("BTC", "binance.swap", use_cache=False)
        assert result == "BTCUSDT"

    def test_hyperliquid_k_prefix_fallback(self, tmp_workdir):
        """Hyperliquid adds k-prefix for known small-cap coins."""
        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        result = get_symbol_for_coin("SHIB", "hyperliquid.swap", use_cache=False)
        # Should either find kSHIBUSDC in config or fallback to k-prefix
        assert "SHIB" in result

    def test_hyperliquid_uppercase_k_symbol_mapping(self, tmp_workdir):
        """Uppercase K-prefixed symbols still map from short name to exchange symbol."""
        p = Path("pbgui.ini")
        cp = configparser.ConfigParser()
        cp.read(p)
        cp.set("exchanges", "hyperliquid.swap", "['KBONKUSDC', 'KPEPEUSDC', 'BTCUSDC']")
        with open(p, "w") as f:
            cp.write(f)

        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        assert get_symbol_for_coin("BONK", "hyperliquid.swap", use_cache=False) == "KBONKUSDC"
        assert get_symbol_for_coin("PEPE", "hyperliquid.swap", use_cache=False) == "KPEPEUSDC"

    def test_caching_works(self, tmp_workdir):
        """Second call uses cache."""
        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        result1 = get_symbol_for_coin("BTC", "binance.swap", use_cache=True)
        assert "binance.swap" in PBCoinData_mod._COIN_TO_SYMBOL_CACHE
        result2 = get_symbol_for_coin("BTC", "binance.swap", use_cache=True)
        assert result1 == result2

    def test_no_ini_fallback(self, tmp_path, monkeypatch):
        """Missing pbgui.ini uses quote-currency fallback."""
        monkeypatch.chdir(tmp_path)
        PBCoinData_mod._COIN_TO_SYMBOL_CACHE.clear()
        result = get_symbol_for_coin("BTC", "binance.swap", use_cache=False)
        assert result == "BTCUSDT"


# ============================================================================
# CoinData Constructor Tests
# ============================================================================

class TestCoinDataConstructor:
    """Test CoinData initialization and defaults."""

    def test_default_attributes(self, coindata):
        """Constructor sets expected default values."""
        assert coindata._fetch_limit == 5000
        assert coindata._fetch_interval == 24
        assert coindata._metadata_interval == 1
        assert coindata.data is None
        assert coindata.metadata is None
        assert coindata.approved_coins == []
        assert coindata.ignored_coins == []
        assert coindata._ccxt_markets == {}
        assert coindata._exchange_mappings == {}

    def test_loads_config_from_ini(self, coindata):
        """Constructor exposes no direct compatibility credential field."""
        assert not hasattr(coindata, "_api_key")
        assert not hasattr(coindata, "api_key")

    def test_exchanges_list(self, coindata):
        """Exchanges list is populated from Exchanges enum."""
        assert len(coindata.exchanges) > 0
        assert "binance" in coindata.exchanges
        assert "hyperliquid" in coindata.exchanges

    def test_pid_paths(self, coindata, tmp_workdir):
        """PID paths are correctly set."""
        assert coindata.piddir == tmp_workdir / "data" / "pid"
        assert coindata.pidfile == tmp_workdir / "data" / "pid" / "pbcoindata.pid"


# ============================================================================
# Config Load/Save Tests
# ============================================================================

class TestConfig:
    """Test pbgui.ini configuration read/write."""

    def test_constructor_removes_legacy_exchanges_section(self, coindata):
        """Startup migration removes legacy exchanges while preserving non-secret CMC config."""
        config = configparser.ConfigParser()
        config.read("pbgui.ini")

        assert not config.has_section("exchanges")
        assert config.has_section("coinmarketcap")
        assert not config.has_option("coinmarketcap", "api_key")

    def test_load_config(self, coindata):
        """load_config() reads only non-secret CMC settings."""
        assert coindata.cmc_pool_ready is True
        assert coindata.fetch_limit == 5000
        assert coindata.fetch_interval == 24

    def test_load_data_without_api_key_uses_cache_only(self, coindata, tmp_workdir, monkeypatch):
        """Missing CMC key does not block cached coindata or trigger API fetches."""
        for record in coindata.cmc_pool.store.list_cmc():
            coindata.cmc_pool.store.delete_cmc(record["id"])
        data_file = tmp_workdir / "data" / "coindata" / "coindata.json"
        data_file.write_text(json.dumps({"data": [{"symbol": "BTC"}]}), encoding="utf-8")
        calls = []
        monkeypatch.setattr(coindata, "fetch_data", lambda: calls.append("fetch") or False)

        coindata.load_data()

        assert calls == []
        assert coindata.data == {"data": [{"symbol": "BTC"}]}

    def test_load_metadata_without_api_key_uses_cache_only(self, coindata, tmp_workdir, monkeypatch):
        """Missing CMC key does not block cached metadata or trigger API fetches."""
        for record in coindata.cmc_pool.store.list_cmc():
            coindata.cmc_pool.store.delete_cmc(record["id"])
        metadata_file = tmp_workdir / "data" / "coindata" / "metadata.json"
        metadata_file.write_text(json.dumps({"data": {"1": {"notice": "ok"}}}), encoding="utf-8")
        calls = []
        monkeypatch.setattr(coindata, "fetch_metadata", lambda: calls.append("fetch") or False)

        coindata.load_metadata()

        assert calls == []
        assert coindata.metadata == {"data": {"1": {"notice": "ok"}}}

    def test_save_config(self, coindata, tmp_workdir):
        """save_config() persists intervals without writing any credential field."""
        coindata.fetch_limit = 1000
        coindata.save_config()

        # Read back
        config = configparser.ConfigParser()
        config.read("pbgui.ini")
        assert not config.has_option("coinmarketcap", "api_key")
        assert config.get("coinmarketcap", "fetch_limit") == "1000"

    def test_save_config_preserves_concurrent_unrelated_key(self, coindata, tmp_workdir):
        """save_config serializes with an unrelated shared INI transaction."""
        import pbgui_purefunc
        import threading

        barrier = threading.Barrier(2)

        def unrelated_writer():
            barrier.wait()
            pbgui_purefunc.save_ini("other", "value", "concurrent")

        thread = threading.Thread(target=unrelated_writer)
        thread.start()
        barrier.wait()
        coindata.save_config()
        thread.join()

        snapshot = pbgui_purefunc.load_ini_snapshot(tmp_workdir / "pbgui.ini")
        assert not snapshot.has_option("coinmarketcap", "api_key")
        assert snapshot.get("other", "value") == "concurrent"

    def test_has_new_config_detects_changes(self, coindata, tmp_workdir):
        """has_new_config() detects ini modifications."""
        # First call — ini was already read, should be False
        coindata.ini_ts = Path("pbgui.ini").stat().st_mtime
        assert coindata.has_new_config() is False

        # Touch the file to simulate external modification
        import time
        time.sleep(0.1)
        Path("pbgui.ini").touch()
        assert coindata.has_new_config() is True

    def test_all_runtime_config_keys_apply_as_one_candidate(self, coindata, tmp_workdir):
        """Every reloadable key is validated and published together."""
        config = configparser.ConfigParser()
        config["coinmarketcap"] = {
            "fetch_limit": "200",
            "fetch_interval": "2",
            "metadata_interval": "7",
            "mapping_interval": "168",
        }
        with Path("pbgui.ini").open("w") as handle:
            config.write(handle)

        assert coindata.load_config() is True
        assert coindata.fetch_limit == 200
        assert coindata.fetch_interval == 2
        assert coindata.metadata_interval == 7
        assert coindata.mapping_interval == 168
        with pytest.raises(FrozenInstanceError):
            coindata._runtime_config.fetch_limit = 400

    def test_missing_keys_restore_documented_defaults(self, coindata):
        """Deleting settings resets all values rather than retaining stale overrides."""
        coindata._fetch_limit = 200
        coindata._fetch_interval = 2
        coindata._metadata_interval = 7
        coindata._mapping_interval = 168
        Path("pbgui.ini").write_text("[coinmarketcap]\n", encoding="utf-8")

        assert coindata.load_config() is True
        assert (coindata.fetch_limit, coindata.fetch_interval) == (5000, 24)
        assert (coindata.metadata_interval, coindata.mapping_interval) == (1, 24)

    @pytest.mark.parametrize("key,value", [
        ("fetch_limit", "nan"),
        ("fetch_limit", "199"),
        ("fetch_interval", "25"),
        ("metadata_interval", "inf"),
        ("mapping_interval", "0"),
    ])
    def test_invalid_numeric_rejects_whole_generation_and_is_retryable(self, coindata, caplog, key, value):
        """Malformed generations preserve last-good state and are retried unchanged."""
        previous = coindata._runtime_config
        generation = coindata._config_generation
        Path("pbgui.ini").write_text(
            f"[coinmarketcap]\n{key} = {value}\n[other]\nvalue = must-not-appear\n",
            encoding="utf-8",
        )

        assert coindata.load_config() is False
        assert coindata.load_config() is False
        assert coindata._runtime_config == previous
        assert coindata._config_generation == generation
        assert coindata.has_new_config() is True
        assert "must-not-appear" not in caplog.text

    def test_corrected_generation_recovers_after_rejection(self, coindata):
        """A corrected file applies after a malformed generation."""
        Path("pbgui.ini").write_text("[coinmarketcap]\nfetch_interval = bad\n", encoding="utf-8")
        assert coindata.load_config() is False
        Path("pbgui.ini").write_text("[coinmarketcap]\nfetch_interval = 3\n", encoding="utf-8")
        assert coindata.load_config() is True
        assert coindata.fetch_interval == 3
        assert coindata._config_load_failed is False

    def test_load_config_reads_exactly_one_snapshot_per_attempt(self, coindata, monkeypatch):
        """One apply attempt cannot combine values from multiple file generations."""
        snapshot = PBCoinData_mod.load_ini_snapshot(Path("pbgui.ini"))
        calls = []
        coindata._config_generation = None
        monkeypatch.setattr(PBCoinData_mod, "load_ini_snapshot", lambda path: calls.append(path) or snapshot)

        assert coindata.load_config() is True
        assert calls == [coindata._ini_watcher._ini_path]

    def test_copy_trading_user_and_role_remain_on_demand(self, coindata):
        """Operation-scoped settings are not hidden in the runtime candidate cache."""
        Path("pbgui.ini").write_text(
            "[coinmarketcap]\ncpt_user.binance = first\n[main]\nrole = slave\n",
            encoding="utf-8",
        )
        assert coindata._load_cpt_user("binance") == "first"
        assert coindata._is_master() is False
        Path("pbgui.ini").write_text(
            "[coinmarketcap]\ncpt_user.binance = second\n[main]\nrole = master\n",
            encoding="utf-8",
        )
        assert coindata._load_cpt_user("binance") == "second"
        assert coindata._is_master() is True

    def test_daemon_watcher_start_stop_is_idempotent_and_releases_thread(self, coindata):
        """CoinData's sole watcher has deterministic lifecycle cleanup."""
        watcher = coindata._ini_watcher
        watcher.start()
        watcher.start()
        assert watcher.is_running is True
        watcher.stop()
        watcher.stop()
        assert watcher.is_running is False
        assert watcher._thread is None

    def test_config_event_wakes_before_fixed_schedule_without_interrupting_cycle(self, monkeypatch):
        """A change wakes scheduling only after the active cycle completes once."""
        calls = []

        class FakeEvent:
            def clear(self):
                calls.append("clear")

            def wait(self, timeout):
                calls.append(("wait", timeout))
                raise KeyboardInterrupt

        class FakeWatcher:
            def __init__(self):
                self.changed = FakeEvent()

            def start(self):
                calls.append("start")

            def stop(self):
                calls.append("stop")

        class FakeCoinData:
            def __init__(self, defer_config=False):
                assert defer_config is True
                self._ini_watcher = FakeWatcher()
                self._config_load_failed = False
                self._logged_idle = False

            def is_running(self): return False
            def save_pid(self): calls.append("pid")
            def load_config(self): calls.append("config")
            def _is_master(self): return True
            def load_data(self): calls.append("data")
            def load_metadata(self): calls.append("metadata")
            def update_mappings(self): calls.append("mapping")

        monkeypatch.setattr(PBCoinData_mod, "CoinData", FakeCoinData)
        with pytest.raises(KeyboardInterrupt):
            PBCoinData_mod.main()

        assert calls.count("data") == calls.count("metadata") == calls.count("mapping") == 1
        assert calls.index("mapping") < calls.index(("wait", 60))
        assert calls[-1] == "stop"


# ============================================================================
# Property Tests
# ============================================================================

class TestProperties:
    """Test CoinData property getters and setters."""

    def test_exchange_getter(self, coindata):
        """exchange property returns current exchange."""
        assert coindata.exchange in coindata.exchanges

    def test_exchange_setter_updates_symbols(self, coindata):
        """Setting exchange triggers symbol reload."""
        coindata.exchange = "binance"
        assert coindata._exchange == "binance"

    def test_market_cap_filter(self, coindata):
        """market_cap property getter/setter."""
        coindata._market_cap = 100
        assert coindata.market_cap == 100

    def test_vol_mcap_filter(self, coindata):
        """vol_mcap property getter/setter."""
        coindata._vol_mcap = 5.0
        assert coindata.vol_mcap == 5.0

    def test_only_cpt_filter(self, coindata):
        """only_cpt property getter/setter."""
        coindata._only_cpt = True
        assert coindata.only_cpt is True

    def test_notices_ignore_filter(self, coindata):
        """notices_ignore property getter/setter."""
        coindata._notices_ignore = True
        assert coindata.notices_ignore is True


# ============================================================================
# Exchange Directory Management Tests
# ============================================================================

class TestExchangeDirectory:
    """Test exchange-specific directory operations."""

    def test_get_exchange_dir(self, coindata, tmp_workdir):
        """_get_exchange_dir returns correct path."""
        d = coindata._get_exchange_dir("binance")
        assert str(d).endswith("data/coindata/binance")

    def test_ensure_exchange_dir_creates(self, coindata, tmp_workdir):
        """_ensure_exchange_dir creates directory if missing."""
        d = coindata._ensure_exchange_dir("test_exchange")
        assert d.exists()
        assert d.is_dir()

    def test_ensure_exchange_dir_idempotent(self, coindata, tmp_workdir):
        """_ensure_exchange_dir is safe to call multiple times."""
        d1 = coindata._ensure_exchange_dir("test_exchange")
        d2 = coindata._ensure_exchange_dir("test_exchange")
        assert d1 == d2
        assert d1.exists()


# ============================================================================
# CCXT Markets Cache Tests
# ============================================================================

class TestCCXTMarketsCache:
    """Test CCXT markets save/load and caching."""

    def test_save_and_load(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Saved markets can be loaded back."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata._ccxt_markets = {}  # Clear in-memory cache
        loaded = coindata.load_ccxt_markets("binance")
        assert len(loaded) == len(sample_ccxt_markets)
        assert "BTC/USDT:USDT" in loaded

    def test_empty_markets_not_saved(self, coindata, tmp_workdir):
        """Empty markets dict is not written to disk."""
        coindata.save_ccxt_markets("binance", {})
        markets_file = coindata._get_exchange_dir("binance") / "ccxt_markets.json"
        assert not markets_file.exists()

    def test_in_memory_cache(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Second load uses in-memory cache."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        # First load populates cache
        loaded1 = coindata.load_ccxt_markets("binance")
        # Second load from cache (no file I/O)
        loaded2 = coindata.load_ccxt_markets("binance")
        assert loaded1 is loaded2  # Same object (cached)

    def test_load_nonexistent_returns_empty(self, coindata, tmp_workdir):
        """Loading from nonexistent file returns empty dict."""
        result = coindata.load_ccxt_markets("nonexistent_exchange")
        assert result == {}

    def test_atomic_write(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Save uses atomic write (temp file + rename)."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        # Temp file should not exist after successful save
        exchange_dir = coindata._get_exchange_dir("binance")
        temp_file = exchange_dir / "ccxt_markets.json.tmp"
        assert not temp_file.exists()
        # Actual file should exist
        actual_file = exchange_dir / "ccxt_markets.json"
        assert actual_file.exists()


# ============================================================================
# Exchange Mapping Cache Tests
# ============================================================================

class TestExchangeMappingCache:
    """Test exchange mapping save/load and caching."""

    def test_save_and_load(self, coindata, tmp_workdir):
        """Saved mapping can be loaded back."""
        mapping = [
            {"exchange": "binance", "symbol": "BTCUSDT", "is_hip3": False, "dex": None},
            {"exchange": "binance", "symbol": "ETHUSDT", "is_hip3": False, "dex": None},
        ]
        coindata.save_exchange_mapping("binance", mapping)
        coindata._exchange_mappings = {}
        loaded = coindata.load_exchange_mapping("binance")
        assert len(loaded) == 2
        assert loaded[0]["symbol"] == "BTCUSDT"

    def test_empty_mapping_not_saved(self, coindata, tmp_workdir):
        """Empty mapping is not written to disk."""
        coindata.save_exchange_mapping("binance", [])
        mapping_file = coindata._get_exchange_dir("binance") / "mapping.json"
        assert not mapping_file.exists()

    def test_load_nonexistent_returns_empty(self, coindata, tmp_workdir):
        """Loading from nonexistent file returns empty list."""
        result = coindata.load_exchange_mapping("nonexistent_exchange")
        assert result == []

    def test_in_memory_cache(self, coindata, tmp_workdir):
        """Second load uses in-memory cache."""
        mapping = [{"symbol": "BTCUSDT"}]
        coindata.save_exchange_mapping("binance", mapping)
        loaded1 = coindata.load_exchange_mapping("binance")
        loaded2 = coindata.load_exchange_mapping("binance")
        assert loaded1 is loaded2


class TestMappingQueryFunctions:
    """Test mapping-based query/filter helpers (Phase 1)."""

    @staticmethod
    def _sample_mapping():
        return [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "quote": "USDT",
                "copy_trading": True,
                "market_cap": 1_500_000_000_000,
                "volume_24h": 30_000_000_000,
                "tags": ["pow"],
                "notice": "",
            },
            {
                "exchange": "binance",
                "symbol": "MEMEUSDT",
                "quote": "USDT",
                "copy_trading": False,
                "market_cap": 50_000_000,
                "volume_24h": 20_000_000,
                "tags": ["memes"],
                "notice": "temporary warning",
            },
            {
                "exchange": "binance",
                "symbol": "DOGEUSDC",
                "quote": "USDC",
                "copy_trading": True,
                "market_cap": 30_000_000_000,
                "volume_24h": 2_000_000_000,
                "tags": ["memes"],
                "notice": None,
            },
            {
                "exchange": "binance",
                "symbol": "1000SHIBUSDT",
                "quote": "USDT",
                "copy_trading": False,
                "market_cap": 12_000_000_000,
                "volume_24h": 500_000_000,
                "tags": ["memes"],
                "notice": None,
            },
        ]

    def test_load_mapping_reload_on_file_change(self, coindata, tmp_workdir):
        """load_mapping reloads from disk when file mtime changed."""
        coindata.save_exchange_mapping("binance", [{"symbol": "BTCUSDT", "quote": "USDT"}])
        first = coindata.load_mapping("binance", use_cache=True)
        assert len(first) == 1

        mapping_file = coindata._get_exchange_dir("binance") / "mapping.json"
        with mapping_file.open("w") as f:
            json.dump([
                {"symbol": "BTCUSDT", "quote": "USDT"},
                {"symbol": "ETHUSDT", "quote": "USDT"},
            ], f, indent=4)
        coindata._exchange_mapping_ts["binance"] = 0

        second = coindata.load_mapping("binance", use_cache=True)
        assert len(second) == 2

    def test_get_mapping_symbols_with_quote_filter(self, coindata, tmp_workdir):
        """get_mapping_symbols returns deduplicated symbols and respects quote filter."""
        coindata.save_exchange_mapping("binance", self._sample_mapping())

        all_symbols = coindata.get_mapping_symbols("binance")
        usdt_symbols = coindata.get_mapping_symbols("binance", quote_filter=["USDT"])

        assert all_symbols == ["1000SHIBUSDT", "BTCUSDT", "DOGEUSDC", "MEMEUSDT"]
        assert usdt_symbols == ["1000SHIBUSDT", "BTCUSDT", "MEMEUSDT"]

    def test_get_mapping_coins_and_cpt_coins(self, coindata, tmp_workdir):
        """Coin extraction from mapping computes normalized coin names."""
        coindata.save_exchange_mapping("binance", self._sample_mapping())

        coins = coindata.get_mapping_coins("binance", quote_filter=["USDT", "USDC"])
        cpt_coins = coindata.get_cpt_coins("binance", quote_filter=["USDT", "USDC"])

        assert coins == ["BTC", "DOGE", "MEME", "SHIB"]
        assert cpt_coins == ["BTC", "DOGE"]

    def test_filter_mapping(self, coindata, tmp_workdir):
        """filter_mapping applies market_cap, vol/mcap, cpt, tags and notices."""
        coindata.save_exchange_mapping("binance", self._sample_mapping())

        approved, ignored = coindata.filter_mapping(
            "binance",
            market_cap_min_m=100,
            vol_mcap_max=0.1,
            only_cpt=False,
            notices_ignore=False,
            tags=[],
            quote_filter=["USDT"],
        )
        assert approved == ["BTC", "SHIB"]
        assert ignored == ["MEME"]

        approved, ignored = coindata.filter_mapping(
            "binance",
            market_cap_min_m=0,
            vol_mcap_max=10.0,
            only_cpt=False,
            notices_ignore=True,
            tags=["memes"],
            quote_filter=["USDT"],
        )
        assert approved == ["SHIB"]
        assert ignored == ["BTC", "MEME"]

    def test_kucoin_active_filter_matches_passivbot_market_filter(self, coindata, tmp_workdir):
        """KuCoin follows Passivbot's active/swap/linear/USDT market eligibility."""
        coindata.save_exchange_mapping("kucoin", [
            {
                "exchange": "kucoin",
                "symbol": "XBTUSDTM",
                "coin": "BTC",
                "quote": "USDT",
                "active": True,
                "swap": True,
                "linear": True,
                "cmc_id": 1,
                "copy_trading": False,
                "market_cap": 1_500_000_000_000,
                "volume_24h": 30_000_000_000,
                "tags": [],
                "notice": "",
            },
            {
                "exchange": "kucoin",
                "symbol": "AMDUSDTM",
                "coin": "AMD",
                "quote": "USDT",
                "active": True,
                "swap": True,
                "linear": True,
                "cmc_id": None,
                "copy_trading": False,
                "market_cap": 0,
                "volume_24h": 0,
                "tags": [],
                "notice": "",
            },
            {
                "exchange": "kucoin",
                "symbol": "AAPLUSDTM",
                "coin": "AAPL",
                "quote": "USDT",
                "active": True,
                "swap": True,
                "linear": True,
                "cmc_id": 36994,
                "copy_trading": False,
                "market_cap": 144_000_000,
                "volume_24h": 1_000_000,
                "tags": ["tokenized-stock", "xstocks-ecosystem"],
                "notice": "",
            },
            {
                "exchange": "kucoin",
                "symbol": "BZUSDTM",
                "coin": "BZ",
                "quote": "USDT",
                "active": True,
                "swap": True,
                "linear": True,
                "cmc_id": 39804,
                "copy_trading": False,
                "market_cap": 0,
                "volume_24h": 0,
                "tags": ["tradfi-assets-derivatives"],
                "notice": "",
            },
            {
                "exchange": "kucoin",
                "symbol": "PAXGUSDTM",
                "coin": "PAXG",
                "quote": "USDT",
                "active": True,
                "swap": True,
                "linear": True,
                "cmc_id": 4705,
                "copy_trading": False,
                "market_cap": 1_800_000_000,
                "volume_24h": 50_000_000,
                "tags": ["tokenized-assets", "tokenized-gold"],
                "notice": "",
            },
            {
                "exchange": "kucoin",
                "symbol": "XBTUSDCM",
                "coin": "BTC",
                "quote": "USDC",
                "active": True,
                "swap": True,
                "linear": True,
                "cmc_id": 1,
                "copy_trading": False,
                "market_cap": 1_500_000_000_000,
                "volume_24h": 30_000_000_000,
                "tags": [],
                "notice": "",
            },
        ])

        approved, ignored = coindata.filter_mapping(
            "kucoin",
            market_cap_min_m=0,
            vol_mcap_max=float("inf"),
            only_cpt=False,
            notices_ignore=False,
            tags=[],
            quote_filter=None,
            active_only=True,
        )
        assert approved == ["AAPL", "AMD", "BTC", "BZ", "PAXG"]
        assert ignored == []

    def test_filter_by_market_cap_mapping(self, coindata, tmp_workdir):
        """filter_by_market_cap_mapping uses absolute USD threshold."""
        coindata.save_exchange_mapping("binance", self._sample_mapping())

        approved, ignored = coindata.filter_by_market_cap_mapping(
            "binance",
            mc=10_000_000_000,
            quote_filter=["USDT", "USDC"],
        )

        assert approved == ["BTC", "DOGE", "SHIB"]
        assert ignored == ["MEME"]


# ============================================================================
# build_mapping() Tests
# ============================================================================

class TestBuildMapping:
    """Test mapping generation from CCXT markets."""

    def test_build_mapping_schema(self, coindata, tmp_workdir, sample_ccxt_markets):
        """build_mapping produces records with required schema fields."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        result = coindata.build_mapping("binance")
        assert result is True

        mapping = coindata.load_exchange_mapping("binance")
        assert len(mapping) > 0

        # Check required fields on first swap record
        swap_records = [r for r in mapping if r.get("ccxt_symbol", "").endswith(":USDT")]
        assert len(swap_records) > 0
        record = swap_records[0]

        required_fields = [
            "exchange", "symbol", "ccxt_symbol", "base", "quote",
            "copy_trading", "cmc_id", "cmc_rank", "market_cap", "volume_24h",
            "tags", "notice", "contract_size", "min_amount", "min_cost",
            "precision_amount", "max_leverage", "price_last", "price_ts",
            "min_order_price", "is_hip3", "dex", "active"
        ]
        for field in required_fields:
            assert field in record, f"Missing field: {field}"

    def test_build_mapping_field_types(self, coindata, tmp_workdir, sample_ccxt_markets):
        """build_mapping records have correct field types."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        for record in mapping:
            assert isinstance(record["exchange"], str)
            assert isinstance(record["symbol"], str)
            assert isinstance(record["ccxt_symbol"], str)
            assert isinstance(record["copy_trading"], bool)
            assert isinstance(record["is_hip3"], bool)
            assert isinstance(record["active"], bool)
            assert isinstance(record["tags"], list)

    def test_build_mapping_only_swap(self, coindata, tmp_workdir, sample_ccxt_markets):
        """build_mapping only includes swap markets, not spot."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        for record in mapping:
            # All records should come from swap markets
            ccxt_sym = record["ccxt_symbol"]
            source_market = sample_ccxt_markets.get(ccxt_sym, {})
            if source_market:
                assert source_market.get("swap") is True, \
                    f"Non-swap market in mapping: {ccxt_sym}"

    def test_build_mapping_no_markets_returns_false(self, coindata, tmp_workdir):
        """build_mapping returns False when no markets available."""
        result = coindata.build_mapping("nonexistent_exchange")
        assert result is False

    def test_build_mapping_binance_no_hip3(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Binance mapping has no HIP-3 markets."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        hip3 = [r for r in mapping if r["is_hip3"]]
        assert len(hip3) == 0, "Binance should have no HIP-3 markets"

        for record in mapping:
            assert record["dex"] is None, "Binance records should have dex=None"

    def test_build_mapping_hyperliquid_hip3(self, coindata, tmp_workdir, sample_hyperliquid_ccxt_markets):
        """Hyperliquid mapping correctly separates crypto and HIP-3."""
        coindata.save_ccxt_markets("hyperliquid", sample_hyperliquid_ccxt_markets)
        coindata.build_mapping("hyperliquid")
        mapping = coindata.load_exchange_mapping("hyperliquid")

        crypto = [r for r in mapping if not r["is_hip3"]]
        hip3 = [r for r in mapping if r["is_hip3"]]

        assert len(crypto) == 2, "Should have 2 crypto markets"
        assert len(hip3) == 2, "Should have 2 HIP-3 markets"

        # Crypto should have no dex
        for r in crypto:
            assert r["dex"] is None

        # HIP-3 should have dex set
        dexes = {r["dex"] for r in hip3}
        assert "xyz" in dexes
        assert "cash" in dexes

    def test_build_mapping_hip3_cmc_defaults(self, coindata, tmp_workdir, sample_hyperliquid_ccxt_markets):
        """HIP-3 records have null/zero CMC fields."""
        coindata.save_ccxt_markets("hyperliquid", sample_hyperliquid_ccxt_markets)
        coindata.build_mapping("hyperliquid")
        mapping = coindata.load_exchange_mapping("hyperliquid")

        hip3 = [r for r in mapping if r["is_hip3"]]
        for record in hip3:
            assert record["cmc_id"] is None
            assert record["cmc_rank"] == 0
            assert record["market_cap"] == 0
            assert record["volume_24h"] == 0
            assert record["tags"] == []

    def test_build_mapping_fetches_prices_on_first_run(self, coindata, tmp_workdir, sample_ccxt_markets):
        """build_mapping fetches live prices when no previous mapping exists."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        # On first run, prices are fetched live from exchange
        # Some records should have prices (if exchange is reachable)
        has_prices = [r for r in mapping if r["price_last"] is not None]
        # price_ts and min_order_price are populated by update_prices(), not build_mapping()
        for record in mapping:
            assert record["price_ts"] is None
            assert record["min_order_price"] is None

    def test_build_mapping_force_fetch(self, coindata, tmp_workdir, sample_ccxt_markets):
        """force_fetch=True triggers fresh CCXT market fetch."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)

        # Mock fetch_ccxt_markets to track calls
        original_fetch = coindata.fetch_ccxt_markets
        fetch_called = []
        def mock_fetch(exchange_id):
            fetch_called.append(exchange_id)
            return original_fetch(exchange_id)

        # Patch Exchange to avoid live API
        with patch.object(coindata, 'fetch_ccxt_markets', side_effect=mock_fetch):
            coindata.build_mapping("binance", force_fetch=True)
            assert "binance" in fetch_called


# ============================================================================
# _detect_hip3() Tests
# ============================================================================

class TestDetectHIP3:
    """Test HIP-3 detection logic."""

    def test_hyperliquid_hip3_detected(self, coindata):
        """HIP-3 markets on Hyperliquid are detected."""
        market = {"info": {"hip3": True, "dex": "xyz"}, "swap": True}
        assert coindata._detect_hip3("hyperliquid", market, {}) is True

    def test_hyperliquid_normal_not_detected(self, coindata):
        """Normal Hyperliquid markets are not HIP-3."""
        market = {"info": {"maxLeverage": 50}, "swap": True}
        assert coindata._detect_hip3("hyperliquid", market, {}) is False

    def test_non_hyperliquid_always_false(self, coindata):
        """Non-Hyperliquid exchanges always return False."""
        market = {"info": {"hip3": True}, "swap": True}
        for ex in ["binance", "bybit", "bitget", "okx", "gateio"]:
            assert coindata._detect_hip3(ex, market, {}) is False

    def test_hip3_false_not_detected(self, coindata):
        """info.hip3=False is not HIP-3."""
        market = {"info": {"hip3": False}, "swap": True}
        assert coindata._detect_hip3("hyperliquid", market, {}) is False

    def test_missing_info_not_detected(self, coindata):
        """Missing info dict is not HIP-3."""
        market = {"swap": True}
        assert coindata._detect_hip3("hyperliquid", market, {}) is False

    def test_hip3_string_not_detected(self, coindata):
        """info.hip3='True' (string) is not detected — must be boolean True."""
        market = {"info": {"hip3": "True"}, "swap": True}
        assert coindata._detect_hip3("hyperliquid", market, {}) is False

    @pytest.mark.parametrize("dex_name", ["xyz", "flx", "cash", "hyna", "km", "vntl", "abcd"])
    def test_all_dexes_detected(self, coindata, dex_name):
        """All known HIP-3 DEXes are detected."""
        market = {"info": {"hip3": True, "dex": dex_name}, "swap": True}
        assert coindata._detect_hip3("hyperliquid", market, {}) is True


# ============================================================================
# Data Freshness Check Tests
# ============================================================================

class TestDataFreshness:
    """Test has_new_data / has_new_metadata / is_data_fresh."""

    def test_has_new_data_true_when_no_data(self, coindata, tmp_workdir):
        """has_new_data returns True when no data has been loaded."""
        # No coindata.json exists yet
        assert coindata.has_new_data() is True

    def test_has_new_data_new_file(self, coindata, tmp_workdir):
        """has_new_data returns True when file is newer."""
        coin_path = Path("data/coindata/coindata.json")
        coin_path.write_text('{"data": []}')
        coindata.data_ts = 0
        assert coindata.has_new_data() is True

    def test_has_new_data_same_ts(self, coindata, tmp_workdir):
        """has_new_data returns False when ts hasn't changed."""
        coin_path = Path("data/coindata/coindata.json")
        coin_path.write_text('{"data": []}')
        coindata.data_ts = coin_path.stat().st_mtime
        assert coindata.has_new_data() is False

    def test_has_new_metadata_true_when_no_data(self, coindata, tmp_workdir):
        """has_new_metadata returns True when no metadata cached."""
        assert coindata.has_new_metadata() is True

    def test_is_data_fresh_no_file(self, coindata, tmp_workdir):
        """is_data_fresh returns None when no coindata.json."""
        result = coindata.is_data_fresh()
        assert result is None

    def test_is_data_fresh_recent_file(self, coindata, tmp_workdir):
        """is_data_fresh returns True for recently-created file."""
        coin_path = Path("data/coindata/coindata.json")
        coin_path.write_text('{"data": []}')
        result = coindata.is_data_fresh()
        assert result is True


# ============================================================================
# CMC Network/Validation Tests
# ============================================================================

class TestCMCNetworkAndValidation:
    """Test retry behavior and strict payload validation for CMC calls."""

    def test_cmc_get_json_retries_429_then_succeeds(self, coindata, monkeypatch):
        """HTTP 429 is retried and succeeds when a later attempt returns valid payload."""
        coindata.cmc_pool.store.create_cmc("first-pool-secret")
        coindata.cmc_pool.store.create_cmc("second-pool-secret")
        responses = [
            MagicMock(status_code=429, text='{"status": {"error_message": "rate limited"}}'),
            MagicMock(status_code=200, text='{"status": {"credit_count": 2}, "data": []}'),
        ]
        used_keys = []

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                used_keys.append(self.headers["X-CMC_PRO_API_KEY"])
                return responses.pop(0)

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(PBCoinData_mod, "sleep", lambda _: None)

        payload, status_code, attempts, error = coindata._cmc_get_json(
            endpoint="listings",
            url="https://example.com/listings",
            headers={"X-CMC_PRO_API_KEY": "x"},
            params={"limit": 10},
            max_retries=3,
            timeout=1,
        )

        assert payload == {"status": {"credit_count": 2}, "data": []}
        assert status_code == 200
        assert attempts == 2
        assert error is None
        assert len(set(used_keys)) == 2
        pool_status = coindata.cmc_pool.status()
        assert sum(item["total_acquisitions"] for item in pool_status["keys"]) == 2
        assert sum(item["used_credits"] for item in pool_status["keys"]) == 2

    def test_key_info_acquires_zero_credits_and_settles_provider_usage(self, coindata, monkeypatch):
        """Key info counts an attempt without reserving credits and records provider counters."""
        coindata.cmc_pool.store.create_cmc("status-pool-secret")
        response_payload = {
            "status": {"credit_count": 0},
            "data": {
                "plan": {
                    "credit_limit_monthly": 10000,
                    "credit_limit_monthly_reset": "2026-08-01",
                    "credit_limit_monthly_reset_timestamp": 1785542400,
                },
                "usage": {
                    "current_day": {"credits_used": 3},
                    "current_month": {"credits_used": 12, "credits_left": 9988},
                },
            },
        }

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                return MagicMock(status_code=200, text=json.dumps(response_payload), headers={})

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)

        assert coindata.fetch_api_status() is True
        key_status = coindata.cmc_pool.status()["keys"][0]
        assert key_status["total_acquisitions"] == 1
        assert key_status["used_credits"] == 0
        assert key_status["provider_used"] == 12
        assert key_status["provider_remaining"] == 9988
        assert key_status["provider_reset_at"] == 1785542400

    def test_timeout_settles_once_with_conservative_reservation(self, coindata, monkeypatch):
        """An uncertain timeout remains charged at the estimate and closes its acquisition."""
        coindata.cmc_pool.store.create_cmc("timeout-pool-secret")

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                raise PBCoinData_mod.Timeout("provider timed out")

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        payload, status_code, attempts, error = coindata._cmc_get_json(
            endpoint="listings",
            url="https://example.com/v1/cryptocurrency/listings/latest",
            headers={},
            params={"limit": 500},
            max_retries=1,
            timeout=1,
        )

        assert (payload, status_code, attempts) == (None, None, 1)
        assert error == "provider timed out"
        key_status = coindata.cmc_pool.status()["keys"][0]
        assert key_status["used_credits"] == 3
        assert key_status["last_outcome"] == "error"
        state = json.loads(coindata.cmc_pool._state_path.read_text(encoding="utf-8"))
        assert all(item["settled"] is True for item in state["acquisitions"].values())

    def test_malformed_200_and_logs_do_not_expose_pool_secret(self, coindata, monkeypatch):
        """Malformed success is conservative while provider diagnostics redact the selected key."""
        secret = "pool-secret-must-not-leak"
        coindata.cmc_pool.store.create_cmc(secret)
        messages = []

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                return MagicMock(status_code=200, text=f'not-json {secret}', headers={})

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(PBCoinData_mod, "_log", lambda service, message, **kwargs: messages.append(message))
        monkeypatch.setattr(PBCoinData_mod, "sleep", lambda _: None)

        assert coindata.fetch_data() is False
        assert secret not in "\n".join(messages)
        assert secret not in json.dumps(coindata.cmc_pool_status())
        key_status = coindata.cmc_pool.status()["keys"][0]
        assert key_status["used_credits"] == 25

    def test_provider_error_redacts_echoed_pool_secret(self, coindata, monkeypatch):
        """A provider error that echoes its credential is redacted before metrics logging."""
        secret = "echoed-pool-secret"
        for record in coindata.cmc_pool.store.list_cmc():
            coindata.cmc_pool.store.delete_cmc(record["id"])
        coindata.cmc_pool.store.create_cmc(secret)
        messages = []

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                return MagicMock(
                    status_code=401,
                    text=json.dumps({"status": {"error_message": f"Invalid API key: {secret}"}}),
                    headers={},
                )

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(PBCoinData_mod, "_log", lambda service, message, **kwargs: messages.append(message))

        payload, status_code, attempts, error = coindata._cmc_get_json(
            endpoint="listings",
            url="https://example.com/v1/cryptocurrency/listings/latest",
            headers={},
            params={"limit": 5000},
            max_retries=1,
        )
        coindata._log_cmc_metrics("listings", False, attempts, status_code, error=error)

        assert payload is None
        assert error == "Invalid API key: <redacted>"
        assert secret not in "\n".join(messages)
        assert "<redacted>" in "\n".join(messages)

    def test_listings_refresh_is_host_wide_single_flight(self, coindata, tmp_workdir, monkeypatch):
        """Overlapping CoinData owners share the first process's published listings cache."""
        coindata.cmc_pool.store.create_cmc("single-flight-secret")
        other = CoinData(cmc_pool=coindata.cmc_pool)
        entered = threading.Event()
        release = threading.Event()
        calls = []

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                calls.append(url)
                entered.set()
                assert release.wait(timeout=5)
                return MagicMock(
                    status_code=200,
                    text='{"status": {"credit_count": 1}, "data": []}',
                    headers={},
                )

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(coindata, "fetch_api_status", lambda: True)
        monkeypatch.setattr(other, "fetch_api_status", lambda: True)
        results = []
        first = threading.Thread(target=lambda: results.append(coindata.fetch_data()))
        second = threading.Thread(target=lambda: results.append(other.fetch_data()))
        first.start()
        assert entered.wait(timeout=5)
        second.start()
        time.sleep(0.05)
        release.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert results == [True, True]
        assert len(calls) == 1
        assert other.data == {"status": {"credit_count": 1}, "data": []}

    def test_metadata_refresh_is_host_wide_single_flight(self, coindata, tmp_workdir, monkeypatch):
        """Overlapping metadata owners share the first process's published cache."""
        coindata.cmc_pool.store.create_cmc("metadata-flight-secret")
        other = CoinData(cmc_pool=coindata.cmc_pool)
        listing = {"data": [{"id": 1, "symbol": "BTC"}]}
        coindata.data = listing
        other.data = listing
        coindata._symbols_all = ["BTC"]
        other._symbols_all = ["BTC"]
        entered = threading.Event()
        release = threading.Event()
        calls = []

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                calls.append(url)
                entered.set()
                assert release.wait(timeout=5)
                return MagicMock(
                    status_code=200,
                    text='{"status": {"credit_count": 1}, "data": {"1": {"notice": null}}}',
                    headers={},
                )

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(coindata, "fetch_api_status", lambda: True)
        monkeypatch.setattr(other, "fetch_api_status", lambda: True)
        results = []
        first = threading.Thread(target=lambda: results.append(coindata.fetch_metadata()))
        second = threading.Thread(target=lambda: results.append(other.fetch_metadata()))
        first.start()
        assert entered.wait(timeout=5)
        second.start()
        time.sleep(0.05)
        release.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert results == [True, True]
        assert len(calls) == 1
        assert other.metadata["data"]["1"]["notice"] is None

    def test_malformed_key_info_preserves_false_return_contract(self, coindata, monkeypatch):
        """Incomplete key-info HTTP 200 responses fail cleanly instead of raising."""
        coindata.cmc_pool.store.create_cmc("malformed-status-secret")

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                return MagicMock(
                    status_code=200,
                    text='{"status": {"credit_count": 0}, "data": {"plan": {}, "usage": {}}}',
                    headers={},
                )

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(PBCoinData_mod, "sleep", lambda _: None)

        assert coindata.fetch_api_status() is False
        assert coindata.api_error

    def test_fetch_data_rejects_invalid_200_payload(self, coindata, monkeypatch):
        """Malformed successful payload is treated as failure and does not overwrite data."""
        bad_payload = '{"status": {"error_code": 0}}'

        class FakeSession:
            def __init__(self):
                self.headers = {}

            def get(self, url, params=None, timeout=30):
                return MagicMock(status_code=200, text=bad_payload)

        monkeypatch.setattr(PBCoinData_mod, "Session", FakeSession)
        monkeypatch.setattr(PBCoinData_mod, "sleep", lambda _: None)

        ok = coindata.fetch_data()
        assert ok is False
        assert coindata.data is None
        assert coindata._cmc_metrics["listings_fail"] >= 1


# ============================================================================
# PID File Tests
# ============================================================================

class TestPIDManagement:
    """Test PID save/load for process management."""

    def test_save_and_load_pid(self, coindata, tmp_workdir):
        """PID is correctly saved and loaded."""
        import os
        coindata.save_pid()
        assert coindata.my_pid == os.getpid()

        # Reset and reload
        coindata.my_pid = None
        coindata.load_pid()
        assert coindata.my_pid == os.getpid()

    def test_load_pid_no_file(self, coindata, tmp_workdir):
        """load_pid handles missing PID file gracefully."""
        coindata.pidfile = Path("data/pid/nonexistent.pid")
        coindata.load_pid()
        assert coindata.my_pid is None


# ============================================================================
# load_symbols / load_symbols_all Tests
# ============================================================================

class TestLoadSymbols:
    """Test symbol loading from mapping.json."""

    @staticmethod
    def _write_mapping(coindata, exchange: str, rows: list[dict]):
        exchange_dir = coindata._get_exchange_dir(exchange)
        exchange_dir.mkdir(parents=True, exist_ok=True)
        mapping_file = exchange_dir / "mapping.json"
        mapping_file.write_text(json.dumps(rows), encoding="utf-8")

    def test_load_symbols_binance(self, coindata, tmp_workdir):
        """load_symbols loads active swap symbols for current exchange."""
        self._write_mapping(
            coindata,
            "binance",
            [
                {"symbol": "BTCUSDT", "swap": True, "active": True, "linear": True, "copy_trading": True},
                {"symbol": "ETHUSDT", "swap": True, "active": True, "linear": True, "copy_trading": False},
                {"symbol": "XRPUSDT", "swap": True, "active": False, "linear": True, "copy_trading": False},
            ],
        )
        coindata._exchange = "binance"
        coindata.load_symbols()
        assert "BTCUSDT" in coindata._symbols
        assert "ETHUSDT" in coindata._symbols
        assert "XRPUSDT" not in coindata._symbols

    def test_load_symbols_hyperliquid(self, coindata, tmp_workdir):
        """load_symbols loads Hyperliquid symbols."""
        self._write_mapping(
            coindata,
            "hyperliquid",
            [
                {"symbol": "BTCUSDC", "quote": "USDC", "swap": True, "active": True, "linear": True, "copy_trading": False},
            ],
        )
        coindata._exchange = "hyperliquid"
        coindata.load_symbols()
        assert "BTCUSDC" in coindata._symbols

    def test_load_symbols_all_deduplicates(self, coindata, tmp_workdir):
        """load_symbols_all merges all exchanges and deduplicates."""
        self._write_mapping(
            coindata,
            "binance",
            [
                {"symbol": "BTCUSDT", "swap": True, "active": True, "linear": True},
                {"symbol": "ETHUSDT", "swap": True, "active": True, "linear": True},
            ],
        )
        self._write_mapping(
            coindata,
            "bybit",
            [
                {"symbol": "ETHUSDT", "swap": True, "active": True, "linear": True},
                {"symbol": "SOLUSDT", "swap": True, "active": True, "linear": True},
            ],
        )
        coindata.load_symbols_all()
        # Should contain symbols from both exchanges
        assert len(coindata._symbols_all) > 0
        # Should be sorted
        assert coindata._symbols_all == sorted(coindata._symbols_all)
        # No duplicates
        assert len(coindata._symbols_all) == len(set(coindata._symbols_all))

    def test_load_symbols_all_builds_mappings(self, coindata, tmp_workdir):
        """load_symbols_all builds _symbol_mappings."""
        self._write_mapping(
            coindata,
            "binance",
            [
                {"symbol": "BTCUSDT", "swap": True, "active": True, "linear": True},
            ],
        )
        coindata.load_symbols_all()
        assert isinstance(coindata._symbol_mappings, dict)
        assert len(coindata._symbol_mappings) > 0


# ============================================================================
# list_symbols / filter_by_market_cap Tests
# ============================================================================

class TestListSymbols:
    """Test mapping-based symbol filtering."""

    @staticmethod
    def _write_mapping(coindata):
        exchange_dir = coindata._get_exchange_dir("binance")
        exchange_dir.mkdir(parents=True, exist_ok=True)
        mapping_file = exchange_dir / "mapping.json"
        mapping_file.write_text(
            json.dumps(
                [
                    {
                        "symbol": "BTCUSDT",
                        "coin": "BTC",
                        "quote": "USDT",
                        "name": "Bitcoin",
                        "slug": "bitcoin",
                        "swap": True,
                        "active": True,
                        "linear": True,
                        "market_cap": 1_900_000_000_000,
                        "volume_24h": 40_000_000_000,
                        "copy_trading": True,
                        "tags": ["store-of-value"],
                        "notice": None,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "coin": "ETH",
                        "quote": "USDT",
                        "name": "Ethereum",
                        "slug": "ethereum",
                        "swap": True,
                        "active": True,
                        "linear": True,
                        "market_cap": 380_000_000_000,
                        "volume_24h": 20_000_000_000,
                        "copy_trading": False,
                        "tags": ["smart-contracts"],
                        "notice": None,
                    },
                    {
                        "symbol": "SOLUSDT",
                        "coin": "SOL",
                        "quote": "USDT",
                        "name": "Solana",
                        "slug": "solana",
                        "swap": True,
                        "active": True,
                        "linear": True,
                        "market_cap": 90_000_000_000,
                        "volume_24h": 5_000_000_000,
                        "copy_trading": False,
                        "tags": ["smart-contracts"],
                        "notice": None,
                    },
                    {
                        "symbol": "SHIBUSDT",
                        "coin": "SHIB",
                        "quote": "USDT",
                        "name": "Shiba Inu",
                        "slug": "shiba-inu",
                        "swap": True,
                        "active": True,
                        "linear": True,
                        "market_cap": 12_000_000_000,
                        "volume_24h": 700_000_000,
                        "copy_trading": False,
                        "tags": ["meme"],
                        "notice": "This coin has a notice",
                    },
                ]
            ),
            encoding="utf-8",
        )

    def test_list_symbols_requires_data(self, coindata, tmp_workdir):
        """list_symbols works from mapping.json without requiring CMC payloads."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        coindata.list_symbols()
        assert len(coindata._symbols_data) > 0

    def test_list_symbols_populates_data(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """list_symbols populates _symbols_data from mapping data."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        coindata.list_symbols()

        assert len(coindata._symbols_data) > 0
        assert len(coindata.approved_coins) > 0

    def test_list_symbols_approved_sorted(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """Approved coins are sorted."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        coindata.list_symbols()

        assert coindata.approved_coins == sorted(coindata.approved_coins)

    def test_list_symbols_market_cap_filter(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """Market cap filter excludes low-cap coins."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        coindata._market_cap = 100000  # 100B minimum
        coindata.list_symbols()

        # Only BTC (1.9T) and ETH (380B) should pass 100B filter
        for sym_data in coindata._symbols_data:
            assert sym_data["market_cap"] >= 100000 * 1000000

    def test_list_symbols_notices(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """Symbols with notices in mapping are tracked."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        coindata.list_symbols()

        # SHIB has a notice in our sample metadata
        assert "SHIB" in coindata._symbols_notice

    def test_filter_by_market_cap(self, coindata, tmp_workdir, sample_cmc_data):
        """filter_by_market_cap correctly splits coins."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        # 500B threshold: only BTC (1.9T) should pass
        approved, ignored = coindata.filter_by_market_cap(symbols, 500000000000)
        assert "BTC" in approved
        assert "ETH" not in approved
        assert "SOL" not in approved

    def test_filter_by_market_cap_zero(self, coindata, tmp_workdir, sample_cmc_data):
        """filter_by_market_cap with 0 threshold includes all."""
        self._write_mapping(coindata)
        coindata._exchange = "binance"
        symbols = ["BTCUSDT", "ETHUSDT"]

        approved, ignored = coindata.filter_by_market_cap(symbols, 0)
        assert "BTC" in approved
        assert "ETH" in approved


# ============================================================================
# Phase 1.7 Equivalence Tests (mapping vs ini/legacy)
# ============================================================================

class TestMappingEquivalence:
    """Verify mapping-based queries match legacy ini/CMC behavior."""

    @staticmethod
    def _extract_quote(symbol: str) -> str:
        for quote in ("SUSDT", "USDT", "USDC", "USD"):
            if symbol.endswith(quote):
                return quote
        return ""

    @staticmethod
    def _build_mapping_from_symbols(symbols, sample_cmc_data, sample_metadata, cpt_symbols=None):
        cpt_set = set(cpt_symbols or [])
        cmc_by_symbol = {
            str(c.get("symbol", "")).upper(): c
            for c in (sample_cmc_data.get("data", []) if isinstance(sample_cmc_data, dict) else [])
        }
        md_by_id = (sample_metadata or {}).get("data", {}) if isinstance(sample_metadata, dict) else {}

        rows = []
        for sym in symbols:
            quote = TestMappingEquivalence._extract_quote(sym)
            coin = compute_coin_name(sym, quote)
            cmc = cmc_by_symbol.get(str(coin).upper(), {})
            q = (cmc.get("quote") or {}).get("USD") if isinstance(cmc, dict) else {}
            cmc_id = cmc.get("id") if isinstance(cmc, dict) else None
            notice = None
            if cmc_id is not None:
                notice = (md_by_id.get(str(cmc_id)) or {}).get("notice")

            rows.append({
                "exchange": "binance",
                "symbol": sym,
                "quote": quote,
                "copy_trading": sym in cpt_set,
                "cmc_id": cmc_id,
                "cmc_rank": cmc.get("cmc_rank") if isinstance(cmc, dict) else None,
                "market_cap": (q or {}).get("market_cap") or 0,
                "volume_24h": (q or {}).get("volume_24h") or 0,
                "tags": cmc.get("tags") if isinstance(cmc, dict) else [],
                "notice": notice,
            })
        return rows

    def test_mapping_symbols_and_coins_match_ini(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """get_mapping_symbols/get_mapping_coins must match ini-derived symbols/coins."""
        coindata._exchange = "binance"
        coindata.load_symbols()
        symbols_ini = list(coindata._symbols)

        mapping_rows = self._build_mapping_from_symbols(symbols_ini, sample_cmc_data, sample_metadata)
        coindata.save_exchange_mapping("binance", mapping_rows)

        symbols_mapping = coindata.get_mapping_symbols("binance", quote_filter=["USDT"])
        coins_mapping = coindata.get_mapping_coins("binance", quote_filter=["USDT"])

        coins_ini = get_normalized_coins(symbols_ini)

        assert symbols_mapping == sorted(set(symbols_ini))
        assert coins_mapping == sorted(set(coins_ini))

    def test_filter_by_market_cap_mapping_matches_legacy(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """filter_by_market_cap_mapping output matches filter_by_market_cap on same symbols."""
        coindata._exchange = "binance"
        coindata.load_symbols()
        symbols_ini = list(coindata._symbols)

        mapping_rows = self._build_mapping_from_symbols(symbols_ini, sample_cmc_data, sample_metadata)
        coindata.save_exchange_mapping("binance", mapping_rows)

        coindata.data = sample_cmc_data
        coindata.load_data = lambda: None

        threshold = 500_000_000_000
        approved_legacy, ignored_legacy = coindata.filter_by_market_cap(symbols_ini, threshold)
        approved_mapping, ignored_mapping = coindata.filter_by_market_cap_mapping(
            "binance", threshold, quote_filter=["USDT"]
        )

        assert set(approved_mapping) == set(approved_legacy)
        assert set(ignored_mapping) == set(ignored_legacy)

    def test_filter_mapping_matches_list_symbols_default_filters(self, coindata, tmp_workdir, sample_cmc_data, sample_metadata):
        """filter_mapping should match list_symbols approved/ignored with default filter knobs."""
        coindata._exchange = "binance"
        coindata.load_symbols()
        symbols_ini = list(coindata._symbols)

        mapping_rows = self._build_mapping_from_symbols(
            symbols_ini,
            sample_cmc_data,
            sample_metadata,
            cpt_symbols=list(coindata._symbols_cpt),
        )
        coindata.save_exchange_mapping("binance", mapping_rows)

        coindata.data = sample_cmc_data
        coindata.data_ts = datetime.now().timestamp()
        coindata.metadata = sample_metadata
        coindata.metadata_ts = datetime.now().timestamp()
        coindata.has_new_data = lambda: False
        coindata.has_new_metadata = lambda: False

        coindata._market_cap = 0
        coindata._vol_mcap = 10.0
        coindata._only_cpt = False
        coindata._notices_ignore = False
        coindata._tags = []
        coindata.list_symbols()

        approved_mapping, ignored_mapping = coindata.filter_mapping(
            "binance",
            quote_filter=["USDT"],
        )

        assert set(approved_mapping) == set(coindata.approved_coins)
        assert set(ignored_mapping) == set(coindata.ignored_coins)

    @pytest.mark.parametrize(
        "market_cap_m,vol_mcap,only_cpt,notices_ignore,tags",
        [
            (0, 10.0, False, False, []),
            (100000, 10.0, False, False, []),
            (0, 0.05, False, False, []),
            (0, 10.0, False, True, []),
            (0, 10.0, False, False, ["memes"]),
            (0, 10.0, True, False, []),
        ],
    )
    def test_filter_mapping_matches_list_symbols_filter_matrix(
        self,
        coindata,
        tmp_workdir,
        sample_cmc_data,
        sample_metadata,
        market_cap_m,
        vol_mcap,
        only_cpt,
        notices_ignore,
        tags,
    ):
        """filter_mapping should match legacy list_symbols across key filter combinations."""
        coindata._exchange = "binance"
        coindata.load_symbols()
        symbols_ini = list(coindata._symbols)

        # Use a strict CPT subset so only_cpt scenario is meaningful
        cpt_subset = ["BTCUSDT", "1000SHIBUSDT"]
        coindata._symbols_cpt = list(cpt_subset)

        mapping_rows = self._build_mapping_from_symbols(
            symbols_ini,
            sample_cmc_data,
            sample_metadata,
            cpt_symbols=cpt_subset,
        )
        coindata.save_exchange_mapping("binance", mapping_rows)

        coindata.data = sample_cmc_data
        coindata.data_ts = datetime.now().timestamp()
        coindata.metadata = sample_metadata
        coindata.metadata_ts = datetime.now().timestamp()
        coindata.has_new_data = lambda: False
        coindata.has_new_metadata = lambda: False

        # Configure legacy filter knobs
        coindata._market_cap = market_cap_m
        coindata._vol_mcap = vol_mcap
        coindata._only_cpt = only_cpt
        coindata._notices_ignore = notices_ignore
        coindata._tags = list(tags)
        coindata.list_symbols()

        approved_mapping, ignored_mapping = coindata.filter_mapping(
            "binance",
            market_cap_min_m=market_cap_m,
            vol_mcap_max=vol_mcap,
            only_cpt=only_cpt,
            notices_ignore=notices_ignore,
            tags=list(tags),
            quote_filter=["USDT"],
        )

        assert set(approved_mapping) == set(coindata.approved_coins)
        assert set(ignored_mapping) == set(coindata.ignored_coins)


# ============================================================================
# build_symbol_mappings() Tests
# ============================================================================

class TestBuildSymbolMappings:
    """Test dynamic symbol mapping construction."""

    def test_basic_mapping(self):
        """Basic symbols map to themselves."""
        mappings = build_symbol_mappings(["BTCUSDT", "ETHUSDT"])
        assert "BTC" in mappings
        assert "ETH" in mappings

    def test_multiplier_mapping(self):
        """Multiplier prefixes create correct mappings."""
        mappings = build_symbol_mappings(["1000SHIBUSDT"])
        assert mappings.get("SHIB") == "SHIB"

    def test_k_prefix_mapping(self):
        """K-prefix creates correct mappings."""
        mappings = build_symbol_mappings(["kPEPEUSDC"])
        assert "PEPE" in mappings

    def test_empty_input(self):
        """Empty input returns empty mappings."""
        mappings = build_symbol_mappings([])
        assert mappings == {}


# ============================================================================
# CMC Data Enrichment Tests
# ============================================================================

class TestCMCEnrichment:
    """Test CMC data integration in build_mapping()."""

    def test_cmc_data_populates_mapping(self, coindata, tmp_workdir, sample_ccxt_markets, sample_cmc_data):
        """build_mapping enriches records with CMC data when self.data is loaded."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.data = sample_cmc_data  # Simulate loaded CMC data
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        btc = next(r for r in mapping if r["base"] == "BTC")
        assert btc["cmc_id"] == 1
        assert btc["cmc_rank"] == 1
        assert btc["market_cap"] == 1900000000000
        assert btc["volume_24h"] == 50000000000
        assert btc["tags"] == ["mineable", "pow"]

    def test_cmc_data_eth_enriched(self, coindata, tmp_workdir, sample_ccxt_markets, sample_cmc_data):
        """ETH record gets correct CMC data."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.data = sample_cmc_data
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        eth = next(r for r in mapping if r["base"] == "ETH")
        assert eth["cmc_id"] == 1027
        assert eth["cmc_rank"] == 2
        assert eth["market_cap"] == 380000000000
        assert eth["volume_24h"] == 20000000000
        assert "smart-contracts" in eth["tags"]
        assert "defi" in eth["tags"]

    def test_no_cmc_data_uses_defaults(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Without CMC data, mapping uses zero/null defaults."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.data = None
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        for record in mapping:
            assert record["cmc_id"] is None
            assert record["cmc_rank"] == 0
            assert record["market_cap"] == 0
            assert record["volume_24h"] == 0
            assert record["tags"] == []

    def test_empty_cmc_data_uses_defaults(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Empty CMC data dict uses defaults."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        coindata.data = {"data": []}
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        for record in mapping:
            assert record["cmc_id"] is None
            assert record["market_cap"] == 0

    def test_unmatched_coin_gets_defaults(self, coindata, tmp_workdir):
        """Coin not in CMC data gets zero/null defaults."""
        markets = {
            "XYZZY/USDT:USDT": {
                "id": "XYZZYUSDT", "symbol": "XYZZY/USDT:USDT",
                "base": "XYZZY", "quote": "USDT",
                "swap": True, "spot": False, "active": True,
                "contractSize": 1.0, "info": {},
                "limits": {"amount": {"min": 1}, "cost": {"min": 5}, "leverage": {"max": 20}},
                "precision": {"amount": 1, "price": 0.0001},
            }
        }
        coindata.save_ccxt_markets("binance", markets)
        coindata.data = {"data": [{"id": 1, "symbol": "BTC", "cmc_rank": 1,
                                    "tags": [], "quote": {"USD": {"market_cap": 1e12, "volume_24h": 5e10}}}]}
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        xyzzy = mapping[0]
        assert xyzzy["cmc_id"] is None
        assert xyzzy["market_cap"] == 0

    def test_data_driven_matching_for_multiplier_symbol(self, coindata, tmp_workdir, sample_cmc_data):
        """Data-driven matching resolves multiplier-prefixed exchange symbols."""
        # 1000SHIBUSDT on exchange → base="1000SHIB" → normalize to "SHIB" → match CMC "SHIB"
        markets = {
            "1000SHIB/USDT:USDT": {
                "id": "1000SHIBUSDT", "symbol": "1000SHIB/USDT:USDT",
                "base": "1000SHIB", "quote": "USDT",
                "swap": True, "spot": False, "active": True,
                "contractSize": 1.0, "info": {},
                "limits": {"amount": {"min": 1}, "cost": {"min": 5}, "leverage": {"max": 20}},
                "precision": {"amount": 1, "price": 0.0001},
            }
        }
        # Build symbol mappings so normalize_symbol works
        coindata._symbol_mappings = build_symbol_mappings(["1000SHIBUSDT"])
        coindata.save_ccxt_markets("binance", markets)
        coindata.data = sample_cmc_data
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        shib = mapping[0]
        assert shib["cmc_id"] == 5994, "Should match SHIB in CMC via normalize_symbol"
        assert shib["market_cap"] == 12000000000

    def test_data_driven_matches_ronin_without_static_symbolmap(self, coindata, tmp_workdir):
        """RONIN resolves to CMC RON via data-driven name/slug matching."""
        markets = {
            "RONIN/USDT:USDT": {
                "id": "RONINUSDT", "symbol": "RONIN/USDT:USDT",
                "base": "RONIN", "quote": "USDT",
                "swap": True, "spot": False, "active": True,
                "contractSize": 1.0, "info": {},
                "limits": {"amount": {"min": 1}, "cost": {"min": 5}, "leverage": {"max": 20}},
                "precision": {"amount": 1, "price": 0.0001},
            }
        }
        coindata.save_ccxt_markets("binance", markets)
        coindata.data = {
            "data": [
                {
                    "id": 14119,
                    "symbol": "RON",
                    "name": "Ronin",
                    "slug": "ronin",
                    "cmc_rank": 122,
                    "tags": ["gaming"],
                    "self_reported_market_cap": None,
                    "quote": {"USD": {"price": 1.5, "volume_24h": 1_000_000, "market_cap": 500_000_000}},
                }
            ]
        }

        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")
        assert mapping[0]["coin"] == "RONIN"
        assert mapping[0]["cmc_id"] == 14119

    def test_data_driven_alias_set_resolves_without_static_symbolmap(self, coindata, tmp_workdir):
        """Data-driven resolver matches known alias-like symbols without SYMBOLMAP."""
        aliases = {
            "RONIN": "RON",
            "DOP1": "DOP",
            "RAYDIUM": "RAY",
            "XBT": "BTC",
            "OMNI1": "OMNI",
            "VELO1": "VELO",
            "1000NEIROCTO": "NEIRO",
        }

        markets = {}
        market_ids = []
        for alias in aliases:
            market_id = f"{alias}USDT"
            market_ids.append(market_id)
            markets[f"{alias}/USDT:USDT"] = {
                "id": market_id,
                "symbol": f"{alias}/USDT:USDT",
                "base": alias,
                "quote": "USDT",
                "swap": True,
                "spot": False,
                "active": True,
                "contractSize": 1.0,
                "info": {},
                "limits": {"amount": {"min": 1}, "cost": {"min": 5}, "leverage": {"max": 20}},
                "precision": {"amount": 1, "price": 0.0001},
            }

        cmc_meta = {
            "RON": ("Ronin", "ronin"),
            "DOP": ("DOP", "dop"),
            "RAY": ("Raydium", "raydium"),
            "BTC": ("XBT", "xbt"),
            "OMNI": ("Omni", "omni"),
            "VELO": ("Velo", "velo"),
            "NEIRO": ("Neiro", "neiro"),
        }

        cmc_rows = []
        for idx, cmc_symbol in enumerate(sorted(set(v.upper() for v in aliases.values())), start=10_000):
            name, slug = cmc_meta.get(cmc_symbol, (cmc_symbol, cmc_symbol.lower()))
            cmc_rows.append(
                {
                    "id": idx,
                    "symbol": cmc_symbol,
                    "name": name,
                    "slug": slug,
                    "cmc_rank": idx - 9_900,
                    "tags": [],
                    "self_reported_market_cap": None,
                    "quote": {"USD": {"price": 1.0, "volume_24h": 1_000_000, "market_cap": 100_000_000}},
                }
            )

        coindata._symbol_mappings = build_symbol_mappings(market_ids)
        coindata.save_ccxt_markets("binance", markets)
        coindata.data = {"data": cmc_rows}

        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")
        by_symbol = {str(r.get("symbol") or "").upper(): r for r in mapping}

        expected_symbol_to_id = {}
        for row in cmc_rows:
            expected_symbol_to_id[str(row["symbol"]).upper()] = int(row["id"])

        for alias, expected_cmc_symbol in aliases.items():
            symbol = f"{alias}USDT"
            row = by_symbol.get(symbol)
            assert row is not None, f"Missing mapping row for {symbol}"
            assert row.get("cmc_id") == expected_symbol_to_id[expected_cmc_symbol], (
                f"Alias resolve mismatch for {symbol}: "
                f"expected {expected_cmc_symbol}/{expected_symbol_to_id[expected_cmc_symbol]}, got {row.get('cmc_id')}"
            )

    def test_bybit_rlusd_matches_cmc_without_usd_overstrip(self, coindata, tmp_workdir):
        """RLUSD base coin must map to CMC RLUSD (not RL).

        Regression guard: matching via normalize_symbol(base, ...) could strip trailing
        USD from base coin names like RLUSD, producing RL and missing CMC enrichment.
        """
        markets = {
            "RLUSD/USDT:USDT": {
                "id": "RLUSDUSDT", "symbol": "RLUSD/USDT:USDT",
                "base": "RLUSD", "quote": "USDT",
                "swap": True, "spot": False, "active": True, "linear": True,
                "contractSize": 1.0, "info": {},
                "limits": {"amount": {"min": 1}, "cost": {"min": 5}, "leverage": {"max": 20}},
                "precision": {"amount": 1, "price": 0.0001},
            }
        }
        coindata.save_ccxt_markets("bybit", markets)
        coindata.data = {
            "data": [
                {
                    "id": 34387,
                    "symbol": "RLUSD",
                    "name": "Ripple USD",
                    "slug": "ripple-usd",
                    "cmc_rank": 46,
                    "tags": ["stablecoin", "usd-stablecoin"],
                    "self_reported_market_cap": None,
                    "quote": {"USD": {"price": 1.0, "volume_24h": 173719376.28988138, "market_cap": 1522010402.4908466}},
                }
            ]
        }
        coindata.metadata = {"data": {"34387": {"notice": None}}}

        coindata.build_mapping("bybit")
        mapping = coindata.load_exchange_mapping("bybit")

        rlusd = next(r for r in mapping if r["symbol"] == "RLUSDUSDT")
        assert rlusd["cmc_id"] == 34387
        assert rlusd["cmc_rank"] == 46
        assert rlusd["market_cap"] > 300_000_000
        assert "stablecoin" in rlusd["tags"]

    def test_hip3_not_enriched_with_cmc(self, coindata, tmp_workdir, sample_hyperliquid_ccxt_markets, sample_cmc_data):
        """HIP-3 markets never get CMC enrichment, even if CMC data is available."""
        coindata.save_ccxt_markets("hyperliquid", sample_hyperliquid_ccxt_markets)
        coindata.data = sample_cmc_data
        coindata.build_mapping("hyperliquid")
        mapping = coindata.load_exchange_mapping("hyperliquid")

        hip3 = [r for r in mapping if r["is_hip3"]]
        assert len(hip3) > 0
        for record in hip3:
            assert record["cmc_id"] is None
            assert record["cmc_rank"] == 0
            assert record["market_cap"] == 0

    def test_crypto_enriched_hip3_not(self, coindata, tmp_workdir, sample_hyperliquid_ccxt_markets, sample_cmc_data):
        """On Hyperliquid, crypto gets CMC data but HIP-3 doesn't."""
        coindata.save_ccxt_markets("hyperliquid", sample_hyperliquid_ccxt_markets)
        coindata.data = sample_cmc_data
        coindata.build_mapping("hyperliquid")
        mapping = coindata.load_exchange_mapping("hyperliquid")

        btc = next(r for r in mapping if r["base"] == "BTC")
        assert btc["cmc_id"] == 1
        assert btc["market_cap"] > 0

        tsla = next(r for r in mapping if "TSLA" in r["base"])
        assert tsla["cmc_id"] is None
        assert tsla["market_cap"] == 0


# ============================================================================
# Price-based CMC Disambiguation Tests
# ============================================================================

class TestPickCmcByPrice:
    """Test CoinData._pick_cmc_by_price() static method."""

    def test_single_candidate_returned_directly(self):
        """With one candidate, it's returned regardless of price."""
        candidates = [
            {"id": 2502, "symbol": "HOT", "name": "Holo", "cmc_rank": 100,
             "quote": {"USD": {"price": 0.002, "market_cap": 500000000}}}
        ]
        result = CoinData._pick_cmc_by_price(candidates, 0.002, "HOT")
        assert result["id"] == 2502

    def test_picks_closest_price(self):
        """When exchange price matches one entry, that entry is picked."""
        candidates = [
            {"id": 2502, "symbol": "HOT", "name": "Holo", "cmc_rank": 100,
             "quote": {"USD": {"price": 0.002, "market_cap": 500000000}}},
            {"id": 99999, "symbol": "HOT", "name": "HOT Protocol", "cmc_rank": 3000,
             "quote": {"USD": {"price": 0.50, "market_cap": 0}}},
        ]
        # Exchange price ~0.002 → should pick Holo
        result = CoinData._pick_cmc_by_price(candidates, 0.0021, "HOT")
        assert result["id"] == 2502, "Should pick Holo (price ~0.002)"

    def test_picks_higher_price_when_matching(self):
        """When exchange price matches the higher-priced entry."""
        candidates = [
            {"id": 2502, "symbol": "HOT", "name": "Holo", "cmc_rank": 100,
             "quote": {"USD": {"price": 0.002, "market_cap": 500000000}}},
            {"id": 99999, "symbol": "HOT", "name": "HOT Protocol", "cmc_rank": 3000,
             "quote": {"USD": {"price": 0.50, "market_cap": 0}}},
        ]
        # Exchange price ~0.50 → should pick HOT Protocol despite worse rank
        result = CoinData._pick_cmc_by_price(candidates, 0.48, "HOT")
        assert result["id"] == 99999, "Should pick HOT Protocol (price ~0.50)"

    def test_overrides_rank_when_price_matches(self):
        """Price match beats rank-based selection."""
        candidates = [
            {"id": 1, "symbol": "ACT", "name": "Act I: The AI Prophecy", "cmc_rank": 773,
             "quote": {"USD": {"price": 0.016, "market_cap": 15000000}}},
            {"id": 2, "symbol": "ACT", "name": "Acet", "cmc_rank": 1214,
             "quote": {"USD": {"price": 0.004, "market_cap": 5000000}}},
        ]
        # Exchange price ~0.004 → should pick Acet even though it has worse rank
        result = CoinData._pick_cmc_by_price(candidates, 0.0042, "ACT")
        assert result["id"] == 2, "Should pick Acet via price match over rank"

    def test_falls_back_to_best_rank_when_no_cmc_price(self):
        """When CMC entries have no valid price, fall back to best rank."""
        candidates = [
            {"id": 1, "symbol": "XYZ", "name": "Xyz1", "cmc_rank": 500,
             "quote": {"USD": {"price": 0, "market_cap": 0}}},
            {"id": 2, "symbol": "XYZ", "name": "Xyz2", "cmc_rank": 100,
             "quote": {"USD": {"price": None, "market_cap": 0}}},
        ]
        result = CoinData._pick_cmc_by_price(candidates, 1.0, "XYZ")
        assert result["id"] == 2, "Should fall back to best rank (100)"

    def test_falls_back_when_score_too_low(self):
        """When all prices are very different from exchange, fall back to best rank."""
        candidates = [
            {"id": 1, "symbol": "XYZ", "name": "Xyz Best Rank", "cmc_rank": 50,
             "quote": {"USD": {"price": 100000.0, "market_cap": 1e12}}},
            {"id": 2, "symbol": "XYZ", "name": "Xyz Worse Rank", "cmc_rank": 5000,
             "quote": {"USD": {"price": 50000.0, "market_cap": 0}}},
        ]
        # Exchange price is $0.001 — both CMC prices are wildly different
        result = CoinData._pick_cmc_by_price(candidates, 0.001, "XYZ")
        assert result["id"] == 1, "Should fall back to best rank when no good price match"

    def test_three_candidates(self):
        """Works with 3+ candidates, picking best price match."""
        candidates = [
            {"id": 1, "symbol": "BOB", "name": "BOB Bitcoin", "cmc_rank": 795,
             "quote": {"USD": {"price": 0.0067, "market_cap": 15000000}}},
            {"id": 2, "symbol": "BOB", "name": "Build On BNB", "cmc_rank": 1172,
             "quote": {"USD": {"price": 0.00000001, "market_cap": 6000000}}},
            {"id": 3, "symbol": "BOB", "name": "BOB (ETH)", "cmc_rank": 2073,
             "quote": {"USD": {"price": 0.00000112, "market_cap": 768764}}},
        ]
        # Exchange price ~0.0065 → should pick BOB Bitcoin
        result = CoinData._pick_cmc_by_price(candidates, 0.0065, "BOB")
        assert result["id"] == 1

    def test_prefers_best_rank_when_price_scores_are_close(self):
        """When score improvement is tiny, keep best-rank candidate.

        Mirrors real-world duplicate-ticker cases (e.g. RUNE/SHIB variants)
        where both prices are close but rank differs massively.
        """
        candidates = [
            {"id": 4157, "symbol": "RUNE", "name": "THORChain", "cmc_rank": 160,
             "quote": {"USD": {"price": 0.40724767500338027, "market_cap": 142828879.53478754}}},
            {"id": 9905, "symbol": "RUNE", "name": "Rune", "cmc_rank": 3620,
             "quote": {"USD": {"price": 0.40667696559371935, "market_cap": 2022.1310205321477}}},
        ]
        # Tiny score edge for id=9905, but should keep best-rank id=4157
        result = CoinData._pick_cmc_by_price(candidates, 0.4048, "RUNE")
        assert result["id"] == 4157

    def test_overrides_rank_on_strong_mismatch(self):
        """When rank candidate is clearly wrong-priced, use price-matching candidate."""
        candidates = [
            {"id": 6952, "symbol": "FRAX", "name": "Legacy Frax Dollar", "cmc_rank": 211,
             "quote": {"USD": {"price": 0.9915234405573204, "market_cap": 273675524.8959225}}},
            {"id": 6953, "symbol": "FRAX", "name": "Frax (prev. FXS)", "cmc_rank": 327,
             "quote": {"USD": {"price": 0.6359303057029116, "market_cap": 60150389.50400973}}},
        ]
        # Exchange price is close to id=6953 and far from id=6952
        result = CoinData._pick_cmc_by_price(candidates, 0.6482, "FRAX")
        assert result["id"] == 6953


class TestBuildMappingPriceDisambiguation:
    """Test price-based CMC disambiguation in build_mapping()."""

    @pytest.fixture
    def duplicate_cmc_data(self):
        """CMC data with duplicate symbols (HOT: Holo vs HOT Protocol)."""
        return {
            "data": [
                {"id": 2502, "symbol": "HOT", "name": "Holo", "slug": "holo",
                 "cmc_rank": 100, "tags": ["platform"],
                 "self_reported_market_cap": None,
                 "quote": {"USD": {"price": 0.002, "volume_24h": 10000000, "market_cap": 500000000}}},
                {"id": 99999, "symbol": "HOT", "name": "HOT Protocol", "slug": "hot-protocol",
                 "cmc_rank": 3000, "tags": [],
                 "self_reported_market_cap": None,
                 "quote": {"USD": {"price": 0.50, "volume_24h": 1000, "market_cap": 0}}},
                {"id": 1, "symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin",
                 "cmc_rank": 1, "tags": ["pow"],
                 "self_reported_market_cap": None,
                 "quote": {"USD": {"price": 70000.0, "volume_24h": 50000000000, "market_cap": 1400000000000}}},
            ]
        }

    @pytest.fixture
    def hot_market(self):
        """CCXT market data with HOT (which maps to Holo, price ~0.002)."""
        return {
            "HOT/USDT:USDT": {
                "id": "HOTUSDT", "symbol": "HOT/USDT:USDT",
                "base": "HOT", "quote": "USDT",
                "swap": True, "spot": False, "active": True,
                "contractSize": 1.0, "info": {},
                "limits": {"amount": {"min": 1}, "cost": {"min": 5}, "leverage": {"max": 20}},
                "precision": {"amount": 1, "price": 0.0001},
            },
            "BTC/USDT:USDT": {
                "id": "BTCUSDT", "symbol": "BTC/USDT:USDT",
                "base": "BTC", "quote": "USDT",
                "swap": True, "spot": False, "active": True,
                "contractSize": 1.0, "info": {},
                "limits": {"amount": {"min": 0.001}, "cost": {"min": 5}, "leverage": {"max": 125}},
                "precision": {"amount": 0.001, "price": 0.01},
            },
        }

    def test_with_previous_prices_picks_correct_cmc(self, coindata, tmp_workdir, duplicate_cmc_data, hot_market):
        """When previous mapping has prices, correct CMC entry is picked via price match."""
        coindata.save_ccxt_markets("binance", hot_market)
        coindata.data = duplicate_cmc_data

        # Create previous mapping with exchange prices
        prev_mapping = [
            {"symbol": "HOTUSDT", "ccxt_symbol": "HOT/USDT:USDT", "base": "HOT",
             "price_last": 0.0019, "price_ts": 1000000},  # ~0.002 → matches Holo
            {"symbol": "BTCUSDT", "ccxt_symbol": "BTC/USDT:USDT", "base": "BTC",
             "price_last": 69500.0, "price_ts": 1000000},
        ]
        coindata.save_exchange_mapping("binance", prev_mapping)
        # Clear in-memory cache so build_mapping loads from file
        coindata._exchange_mappings = {}

        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        hot = next(r for r in mapping if r["base"] == "HOT")
        assert hot["cmc_id"] == 2502, "Should pick Holo (id=2502) via price match"
        assert hot["market_cap"] == 500000000

    def test_without_previous_prices_falls_back_to_rank(self, coindata, tmp_workdir, duplicate_cmc_data, hot_market):
        """Without previous prices, falls back to best-rank CMC entry."""
        coindata.save_ccxt_markets("binance", hot_market)
        coindata.data = duplicate_cmc_data
        # No previous mapping → no prices
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        hot = next(r for r in mapping if r["base"] == "HOT")
        # Best rank = 100 (Holo, id=2502)
        assert hot["cmc_id"] == 2502, "Should fall back to best rank (Holo)"

    def test_non_duplicate_unaffected(self, coindata, tmp_workdir, duplicate_cmc_data, hot_market):
        """Non-duplicate coins (BTC) are unaffected by disambiguation."""
        coindata.save_ccxt_markets("binance", hot_market)
        coindata.data = duplicate_cmc_data
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        btc = next(r for r in mapping if r["base"] == "BTC")
        assert btc["cmc_id"] == 1
        assert btc["market_cap"] == 1400000000000


# ============================================================================
# Copy Trading Cache Tests
# ============================================================================

class TestCopyTradingCache:
    """Test copy trading symbol cache (save/load/persist)."""

    def test_save_and_load(self, coindata, tmp_workdir):
        """Copy trading symbols can be saved and loaded."""
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        coindata.save_copy_trading_symbols("binance", symbols)
        loaded = coindata.load_copy_trading_symbols("binance")
        assert loaded == sorted(symbols)

    def test_empty_list_saved(self, coindata, tmp_workdir):
        """Empty list is not saved (no file created)."""
        # save_copy_trading_symbols saves sorted list, even empty
        coindata.save_copy_trading_symbols("binance", [])
        # File should still be created with empty list
        loaded = coindata.load_copy_trading_symbols("binance")
        assert loaded == []

    def test_in_memory_cache(self, coindata, tmp_workdir):
        """Subsequent loads use in-memory cache."""
        symbols = ["BTCUSDT", "ETHUSDT"]
        coindata.save_copy_trading_symbols("binance", symbols)

        # First load populates cache
        loaded1 = coindata.load_copy_trading_symbols("binance")
        # Delete file to prove cache is used
        cpt_file = coindata._get_exchange_dir("binance") / "copy_trading.json"
        cpt_file.unlink()
        loaded2 = coindata.load_copy_trading_symbols("binance")
        assert loaded1 == loaded2

    def test_load_nonexistent_returns_empty(self, coindata, tmp_workdir):
        """Loading from non-existent exchange returns empty list."""
        loaded = coindata.load_copy_trading_symbols("nonexistent")
        assert loaded == []

    def test_persists_to_correct_path(self, coindata, tmp_workdir):
        """Copy trading file is saved at coindata/{exchange}/copy_trading.json."""
        coindata.save_copy_trading_symbols("bybit", ["BTCUSDT"])
        cpt_file = tmp_workdir / "data" / "coindata" / "bybit" / "copy_trading.json"
        assert cpt_file.exists()
        data = json.loads(cpt_file.read_text())
        assert data == ["BTCUSDT"]

    def test_sorted_on_save(self, coindata, tmp_workdir):
        """Symbols are sorted when saved."""
        coindata.save_copy_trading_symbols("binance", ["ETHUSDT", "BTCUSDT", "ADAUSDT"])
        loaded = coindata.load_copy_trading_symbols("binance")
        assert loaded == ["ADAUSDT", "BTCUSDT", "ETHUSDT"]


# ============================================================================
# Copy Trading Fetch Tests
# ============================================================================

class TestFetchCopyTradingSymbols:
    """Test copy trading symbol fetching from various sources."""

    def test_bybit_from_market_data(self, coindata, tmp_workdir):
        """Bybit copy trading symbols extracted from CCXT market data."""
        markets = {
            "BTC/USDT:USDT": {
                "id": "BTCUSDT", "symbol": "BTC/USDT:USDT",
                "base": "BTC", "quote": "USDT",
                "swap": True, "active": True, "linear": True,
                "info": {"copyTrading": "both"},
            },
            "ETH/USDT:USDT": {
                "id": "ETHUSDT", "symbol": "ETH/USDT:USDT",
                "base": "ETH", "quote": "USDT",
                "swap": True, "active": True, "linear": True,
                "info": {"copyTrading": "none"},
            },
            "SOL/USDT:USDT": {
                "id": "SOLUSDT", "symbol": "SOL/USDT:USDT",
                "base": "SOL", "quote": "USDT",
                "swap": True, "active": True, "linear": True,
                "info": {"copyTrading": "both"},
            },
            "DOGE/USDT": {
                "id": "DOGEUSDT", "symbol": "DOGE/USDT",
                "base": "DOGE", "quote": "USDT",
                "swap": False, "active": True, "linear": True,
                "info": {},
            },
        }
        result = coindata.fetch_copy_trading_symbols("bybit", markets)
        assert sorted(result) == ["BTCUSDT", "SOLUSDT"]

    def test_bybit_no_linear_excluded(self, coindata, tmp_workdir):
        """Bybit non-linear markets are excluded from copy trading."""
        markets = {
            "BTC/USD:BTC": {
                "id": "BTCUSD", "symbol": "BTC/USD:BTC",
                "base": "BTC", "quote": "USD",
                "swap": True, "active": True, "linear": False,
                "info": {"copyTrading": "both"},
            },
        }
        result = coindata.fetch_copy_trading_symbols("bybit", markets)
        assert result == []

    def test_bybit_inactive_excluded(self, coindata, tmp_workdir):
        """Bybit inactive markets are excluded from copy trading."""
        markets = {
            "BTC/USDT:USDT": {
                "id": "BTCUSDT", "symbol": "BTC/USDT:USDT",
                "base": "BTC", "quote": "USDT",
                "swap": True, "active": False, "linear": True,
                "info": {"copyTrading": "both"},
            },
        }
        result = coindata.fetch_copy_trading_symbols("bybit", markets)
        assert result == []

    def test_bybit_loads_from_cache_if_no_markets(self, coindata, tmp_workdir):
        """Bybit with no markets passed loads from ccxt cache."""
        markets = {
            "BTC/USDT:USDT": {
                "id": "BTCUSDT", "swap": True, "active": True, "linear": True,
                "info": {"copyTrading": "both"},
            },
        }
        coindata.save_ccxt_markets("bybit", markets)
        result = coindata.fetch_copy_trading_symbols("bybit")  # No markets arg
        assert "BTCUSDT" in result

    def test_unknown_exchange_returns_empty(self, coindata, tmp_workdir):
        """Exchanges without copy trading API return empty list."""
        result = coindata.fetch_copy_trading_symbols("gateio")
        assert result == []

    def test_unknown_exchange_no_cache_written(self, coindata, tmp_workdir):
        """Exchanges without copy trading don't write cache files."""
        coindata.fetch_copy_trading_symbols("okx")
        cpt_file = coindata._get_exchange_dir("okx") / "copy_trading.json"
        assert not cpt_file.exists()

    def test_bybit_result_cached(self, coindata, tmp_workdir):
        """Bybit copy trading results are saved to cache."""
        markets = {
            "BTC/USDT:USDT": {
                "id": "BTCUSDT", "swap": True, "active": True, "linear": True,
                "info": {"copyTrading": "both"},
            },
        }
        coindata.fetch_copy_trading_symbols("bybit", markets)
        loaded = coindata.load_copy_trading_symbols("bybit")
        assert "BTCUSDT" in loaded

    def test_fallback_to_cache_on_error(self, coindata, tmp_workdir):
        """On fetch error, falls back to cached copy trading data."""
        # Pre-populate cache
        coindata.save_copy_trading_symbols("binance", ["BTCUSDT", "ETHUSDT"])
        # Clear in-memory cache to force file read
        coindata._copy_trading_cache = {}

        # Mock _fetch_cpt_with_user_discovery to raise
        with patch.object(coindata, '_fetch_cpt_with_user_discovery', side_effect=Exception("API down")):
            result = coindata.fetch_copy_trading_symbols("binance")
        assert sorted(result) == ["BTCUSDT", "ETHUSDT"]


# ============================================================================
# Copy Trading in build_mapping() Tests
# ============================================================================

class TestBuildMappingCopyTrading:
    """Test full copy trading pipeline: fetch → copy_trading.json → build_mapping → mapping.json.

    These tests verify that:
    1. fetch_copy_trading_symbols() writes copy_trading.json to disk
    2. build_mapping() reads copy_trading.json and transfers flags into mapping.json
    3. Both files exist on disk with timestamps from the current test run
    4. Content in copy_trading.json matches copy_trading flags in mapping.json
    """

    def test_fetch_writes_copy_trading_json_to_disk(self, coindata, tmp_workdir, sample_ccxt_markets):
        """fetch_copy_trading_symbols() must write copy_trading.json with correct content."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        exchange_dir = coindata._get_exchange_dir("binance")
        cpt_file = exchange_dir / "copy_trading.json"

        # Ensure no pre-existing file
        if cpt_file.exists():
            cpt_file.unlink()
        coindata._copy_trading_cache.pop("binance", None)

        before_ts = time.time()

        # Mock only the API boundary — everything else runs for real
        with patch.object(coindata, '_fetch_cpt_with_user_discovery', return_value=["BTCUSDT", "ETHUSDT"]):
            result = coindata.fetch_copy_trading_symbols("binance")

        after_ts = time.time()

        # 1. Return value correct
        assert sorted(result) == ["BTCUSDT", "ETHUSDT"], f"Expected [BTCUSDT, ETHUSDT], got {result}"

        # 2. File written to disk
        assert cpt_file.exists(), f"copy_trading.json was not written to {cpt_file}"

        # 3. File content correct
        with cpt_file.open() as f:
            on_disk = json.load(f)
        assert on_disk == ["BTCUSDT", "ETHUSDT"], f"copy_trading.json content wrong: {on_disk}"

        # 4. File timestamp is from this test run
        file_mtime = cpt_file.stat().st_mtime
        assert file_mtime >= before_ts - 1.0, (
            f"copy_trading.json mtime ({file_mtime}) is older than test start ({before_ts})"
        )
        assert file_mtime <= after_ts + 1, (
            f"copy_trading.json mtime ({file_mtime}) is newer than test end ({after_ts})"
        )

    def test_build_mapping_reads_copy_trading_json_into_mapping(self, coindata, tmp_workdir, sample_ccxt_markets):
        """build_mapping() must read copy_trading.json and set flags in mapping.json."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        exchange_dir = coindata._get_exchange_dir("binance")
        cpt_file = exchange_dir / "copy_trading.json"
        mapping_file = exchange_dir / "mapping.json"

        # Clean state
        for f in [cpt_file, mapping_file]:
            if f.exists():
                f.unlink()
        coindata._copy_trading_cache.pop("binance", None)
        coindata._exchange_mappings.pop("binance", None)

        # Step 1: fetch writes copy_trading.json (mock API boundary only)
        with patch.object(coindata, '_fetch_cpt_with_user_discovery', return_value=["BTCUSDT"]):
            coindata.fetch_copy_trading_symbols("binance")

        assert cpt_file.exists(), "copy_trading.json must exist after fetch"

        # Clear in-memory cache so build_mapping must read from FILE
        coindata._copy_trading_cache.pop("binance", None)

        before_build = time.time()

        # Step 2: build_mapping reads copy_trading.json and writes mapping.json
        coindata.build_mapping("binance")

        after_build = time.time()

        # mapping.json must exist on disk
        assert mapping_file.exists(), f"mapping.json was not written to {mapping_file}"

        # mapping.json timestamp must be current
        mapping_mtime = mapping_file.stat().st_mtime
        assert mapping_mtime >= before_build, (
            f"mapping.json mtime ({mapping_mtime}) is older than build start ({before_build})"
        )

        # Read mapping and verify copy_trading flags
        mapping = coindata.load_exchange_mapping("binance")
        btc = next(r for r in mapping if r["symbol"] == "BTCUSDT")
        eth = next(r for r in mapping if r["symbol"] == "ETHUSDT")

        assert btc["copy_trading"] is True, "BTC should be copy_trading=True (was in copy_trading.json)"
        assert eth["copy_trading"] is False, "ETH should be copy_trading=False (not in copy_trading.json)"

    def test_full_pipeline_files_and_content_match(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Full E2E pipeline: fetch → copy_trading.json → build_mapping → mapping.json.

        Verifies both files exist with current timestamps and content is consistent.
        """
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        exchange_dir = coindata._get_exchange_dir("binance")
        cpt_file = exchange_dir / "copy_trading.json"
        mapping_file = exchange_dir / "mapping.json"

        # Clean state
        for f in [cpt_file, mapping_file]:
            if f.exists():
                f.unlink()
        coindata._copy_trading_cache.pop("binance", None)
        coindata._exchange_mappings.pop("binance", None)

        before_ts = time.time()

        # Full pipeline: fetch → build
        with patch.object(coindata, '_fetch_cpt_with_user_discovery', return_value=["BTCUSDT", "ETHUSDT"]):
            fetched = coindata.fetch_copy_trading_symbols("binance")
        coindata._copy_trading_cache.pop("binance", None)  # force file read
        coindata.build_mapping("binance")

        after_ts = time.time()

        # Both files must exist
        assert cpt_file.exists(), "copy_trading.json missing after pipeline"
        assert mapping_file.exists(), "mapping.json missing after pipeline"

        # Both timestamps must be from this test run
        cpt_mtime = cpt_file.stat().st_mtime
        map_mtime = mapping_file.stat().st_mtime
        assert cpt_mtime >= before_ts, "copy_trading.json timestamp too old"
        assert map_mtime >= before_ts, "mapping.json timestamp too old"
        assert cpt_mtime <= after_ts + 1, "copy_trading.json timestamp too new"
        assert map_mtime <= after_ts + 1, "mapping.json timestamp too new"

        # Content consistency: copy_trading.json symbols == mapping copy_trading=True symbols
        with cpt_file.open() as f:
            cpt_on_disk = set(json.load(f))

        with mapping_file.open() as f:
            mapping_on_disk = json.load(f)

        cpt_in_mapping = {r["symbol"] for r in mapping_on_disk if r.get("copy_trading")}

        # Every CPT symbol that exists as a swap market must be flagged in mapping
        swap_symbols = {r["symbol"] for r in mapping_on_disk if r.get("swap")}
        expected_cpt_in_mapping = cpt_on_disk & swap_symbols

        assert cpt_in_mapping == expected_cpt_in_mapping, (
            f"copy_trading.json and mapping.json are inconsistent. "
            f"CPT file: {cpt_on_disk}, mapping flags: {cpt_in_mapping}, "
            f"expected: {expected_cpt_in_mapping}"
        )

    def test_partial_cpt_only_matching_symbols_flagged(self, coindata, tmp_workdir, sample_ccxt_markets):
        """Only symbols present in BOTH copy_trading.json AND markets get flagged."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        exchange_dir = coindata._get_exchange_dir("binance")

        # CPT contains BTCUSDT (exists in markets) and XYZUSDT (does NOT exist)
        with patch.object(coindata, '_fetch_cpt_with_user_discovery', return_value=["BTCUSDT", "XYZUSDT"]):
            coindata.fetch_copy_trading_symbols("binance")

        coindata._copy_trading_cache.pop("binance", None)
        coindata.build_mapping("binance")
        mapping = coindata.load_exchange_mapping("binance")

        btc = next(r for r in mapping if r["symbol"] == "BTCUSDT")
        eth = next(r for r in mapping if r["symbol"] == "ETHUSDT")

        assert btc["copy_trading"] is True, "BTC in CPT and in markets → True"
        assert eth["copy_trading"] is False, "ETH not in CPT → False"
        # XYZUSDT is in CPT but not in markets — no mapping row for it
        xyz_rows = [r for r in mapping if r["symbol"] == "XYZUSDT"]
        assert len(xyz_rows) == 0, "XYZUSDT has no market, should not appear in mapping"

    def test_copy_trading_hip3_never_true(self, coindata, tmp_workdir, sample_hyperliquid_ccxt_markets):
        """HIP-3 markets are never marked as copy trading capable."""
        coindata.save_ccxt_markets("hyperliquid", sample_hyperliquid_ccxt_markets)
        coindata.build_mapping("hyperliquid")
        mapping = coindata.load_exchange_mapping("hyperliquid")

        hip3 = [r for r in mapping if r["is_hip3"]]
        for record in hip3:
            assert record["copy_trading"] is False

    def test_copy_trading_no_legacy_ini_fallback_on_cold_start(self, coindata, tmp_workdir, sample_ccxt_markets):
        """On cold start, legacy binance.cpt from pbgui.ini is ignored.

        Mapping/cache paths are authoritative and legacy [exchanges] fallback is removed.
        """
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        exchange_dir = coindata._get_exchange_dir("binance")

        # Ensure cold-start: no copy_trading.json, no mapping.json
        for fname in ["copy_trading.json", "mapping.json"]:
            fpath = exchange_dir / fname
            if fpath.exists():
                fpath.unlink()
        coindata._copy_trading_cache.pop("binance", None)
        coindata._exchange_mappings.pop("binance", None)

        # Inject legacy CPT symbols into pbgui.ini
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read(tmp_workdir / "pbgui.ini")
        if not cfg.has_section("exchanges"):
            cfg.add_section("exchanges")
        cfg.set("exchanges", "binance.cpt", "['BTCUSDT']")
        with open(tmp_workdir / "pbgui.ini", "w") as f:
            cfg.write(f)

        coindata.build_mapping("binance")

        mapping = coindata.load_exchange_mapping("binance")
        btc = next(r for r in mapping if r["symbol"] == "BTCUSDT")
        eth = next(r for r in mapping if r["symbol"] == "ETHUSDT")

        assert btc["copy_trading"] is False, "Legacy INI CPT is ignored on cold start"
        assert eth["copy_trading"] is False, "Legacy INI CPT is ignored on cold start"

    def test_copy_trading_preserved_from_previous_mapping_when_cpt_unavailable(self, coindata, tmp_workdir, sample_ccxt_markets):
        """When copy_trading.json is missing, preserve CPT flags from previous mapping.json."""
        coindata.save_ccxt_markets("binance", sample_ccxt_markets)
        exchange_dir = coindata._get_exchange_dir("binance")
        cpt_file = exchange_dir / "copy_trading.json"

        # Set up previous mapping with one CPT-enabled symbol
        coindata.save_exchange_mapping("binance", [
            {"symbol": "BTCUSDT", "copy_trading": True, "price_last": 95000.0},
            {"symbol": "ETHUSDT", "copy_trading": False, "price_last": 3200.0},
        ])

        # Remove copy_trading.json (simulates failed fetch)
        if cpt_file.exists():
            cpt_file.unlink()
        coindata._copy_trading_cache.pop("binance", None)

        coindata.build_mapping("binance")

        mapping = coindata.load_exchange_mapping("binance")
        btc = next(r for r in mapping if r["symbol"] == "BTCUSDT")
        eth = next(r for r in mapping if r["symbol"] == "ETHUSDT")

        assert btc["copy_trading"] is True, "BTC was True in previous mapping → preserved"
        assert eth["copy_trading"] is False, "ETH was False in previous mapping → stays False"


# ============================================================================
# CPT User Discovery Tests
# ============================================================================

class TestCPTUserDiscovery:
    """Test smart user caching for copy trading API calls."""

    def test_save_and_load_cpt_user(self, coindata, tmp_workdir):
        """Copy trading user can be saved and loaded from pbgui.ini."""
        coindata._save_cpt_user("bitget", "bitget_CPT")
        loaded = coindata._load_cpt_user("bitget")
        assert loaded == "bitget_CPT"

    def test_load_cpt_user_not_set(self, coindata, tmp_workdir):
        """Returns None when no CPT user is configured."""
        loaded = coindata._load_cpt_user("binance")
        assert loaded is None

    def test_save_cpt_user_per_exchange(self, coindata, tmp_workdir):
        """CPT users are stored per exchange."""
        coindata._save_cpt_user("binance", "binance_main")
        coindata._save_cpt_user("bitget", "bitget_cpt")
        assert coindata._load_cpt_user("binance") == "binance_main"
        assert coindata._load_cpt_user("bitget") == "bitget_cpt"

    def test_save_cpt_user_overwrites(self, coindata, tmp_workdir):
        """Saving a new CPT user overwrites the old one."""
        coindata._save_cpt_user("bitget", "bitget_old")
        coindata._save_cpt_user("bitget", "bitget_new")
        assert coindata._load_cpt_user("bitget") == "bitget_new"

    def test_save_cpt_user_persists_in_ini(self, coindata, tmp_workdir):
        """CPT user is stored in pbgui.ini [coinmarketcap] section."""
        coindata._save_cpt_user("binance", "binance_CPT")
        config = configparser.ConfigParser()
        config.read(tmp_workdir / "pbgui.ini")
        assert config.get("coinmarketcap", "cpt_user.binance") == "binance_CPT"

    def test_get_candidate_users_binance(self, coindata, tmp_workdir):
        """_get_cpt_candidate_users returns all binance users (plural)."""
        mock_user1 = MagicMock()
        mock_user1.name = "binance_main"
        mock_user1.exchange = "binance"
        mock_user2 = MagicMock()
        mock_user2.name = "binance_cpt"
        mock_user2.exchange = "binance"
        mock_users = MagicMock()
        mock_users.find_binance_users.return_value = [mock_user1, mock_user2]

        result = coindata._get_cpt_candidate_users(mock_users, "binance")
        assert len(result) == 2
        assert result[0].name == "binance_main"
        assert result[1].name == "binance_cpt"

    def test_get_candidate_users_bitget_multiple(self, coindata, tmp_workdir):
        """_get_cpt_candidate_users returns all bitget users."""
        mock_user1 = MagicMock()
        mock_user1.name = "bitget_main"
        mock_user2 = MagicMock()
        mock_user2.name = "bitget_cpt"
        mock_users = MagicMock()
        mock_users.find_bitget_users.return_value = [mock_user1, mock_user2]

        result = coindata._get_cpt_candidate_users(mock_users, "bitget")
        assert len(result) == 2

    def test_get_candidate_users_no_users(self, coindata, tmp_workdir):
        """_get_cpt_candidate_users returns empty when no users found."""
        mock_users = MagicMock()
        mock_users.find_binance_users.return_value = []

        result = coindata._get_cpt_candidate_users(mock_users, "binance")
        assert result == []

    def test_get_candidate_users_unknown_exchange(self, coindata, tmp_workdir):
        """_get_cpt_candidate_users returns empty for unknown exchanges."""
        mock_users = MagicMock()
        result = coindata._get_cpt_candidate_users(mock_users, "okx")
        assert result == []

    def test_remembered_user_tried_first(self, coindata, tmp_workdir):
        """The remembered user is tried before others."""
        coindata._save_cpt_user("bitget", "bitget_cpt")

        # Create mock users
        user_main = MagicMock()
        user_main.name = "bitget_main"
        user_cpt = MagicMock()
        user_cpt.name = "bitget_cpt"

        tried_users = []

        def mock_try_fetch(exchange_id, user):
            tried_users.append(user.name)
            if user.name == "bitget_cpt":
                return ["BTCUSDT"]
            return None

        with patch.object(coindata, '_get_cpt_candidate_users', return_value=[user_main, user_cpt]):
            with patch.object(coindata, '_try_fetch_cpt_for_user', side_effect=mock_try_fetch):
                with patch.dict('sys.modules', {'User': MagicMock()}):
                    result = coindata._fetch_cpt_with_user_discovery("bitget")

        assert tried_users[0] == "bitget_cpt", "Remembered user should be tried first"
        assert result == ["BTCUSDT"]

    def test_scan_all_when_remembered_fails(self, coindata, tmp_workdir):
        """When remembered user fails, all users are scanned."""
        coindata._save_cpt_user("bitget", "bitget_old")

        user_old = MagicMock()
        user_old.name = "bitget_old"
        user_new = MagicMock()
        user_new.name = "bitget_new"

        tried_users = []

        def mock_try_fetch(exchange_id, user):
            tried_users.append(user.name)
            if user.name == "bitget_new":
                return ["SOLUSDT"]
            return None

        with patch.object(coindata, '_get_cpt_candidate_users', return_value=[user_old, user_new]):
            with patch.object(coindata, '_try_fetch_cpt_for_user', side_effect=mock_try_fetch):
                with patch.dict('sys.modules', {'User': MagicMock()}):
                    result = coindata._fetch_cpt_with_user_discovery("bitget")

        assert "bitget_old" in tried_users
        assert "bitget_new" in tried_users
        assert result == ["SOLUSDT"]
        # Verify new user was remembered
        assert coindata._load_cpt_user("bitget") == "bitget_new"

    def test_no_candidates_returns_empty(self, coindata, tmp_workdir):
        """No candidate users returns empty list."""
        with patch.object(coindata, '_get_cpt_candidate_users', return_value=[]):
            with patch.dict('sys.modules', {'User': MagicMock()}):
                result = coindata._fetch_cpt_with_user_discovery("bitget")
        assert result == []

    def test_all_users_fail_returns_empty(self, coindata, tmp_workdir):
        """When all users fail, returns empty list."""
        user1 = MagicMock()
        user1.name = "user1"
        user2 = MagicMock()
        user2.name = "user2"

        with patch.object(coindata, '_get_cpt_candidate_users', return_value=[user1, user2]):
            with patch.object(coindata, '_try_fetch_cpt_for_user', return_value=None):
                with patch.dict('sys.modules', {'User': MagicMock()}):
                    result = coindata._fetch_cpt_with_user_discovery("bitget")
        assert result == []


# ============================================================================
# update_prices() Tests
# ============================================================================

class TestUpdatePrices:
    """Test price update pipeline including fallback and runtime constraints."""

    def test_update_prices_all_v7_exchanges_with_timing(self, coindata, tmp_workdir):
        """All V7 exchanges update prices quickly and fill active rows in mocked flow."""

        exchanges = ["binance", "bybit", "bitget", "gateio", "hyperliquid", "okx"]

        # Seed minimal mapping for each exchange
        for ex in exchanges:
            if ex == "binance":
                # Include both linear and inverse to exercise subtype split path
                mapping = [
                    {
                        "exchange": ex,
                        "symbol": "BTCUSDT",
                        "ccxt_symbol": "BTC/USDT:USDT",
                        "active": True,
                        "linear": True,
                        "contract_size": 1.0,
                        "min_amount": 0.001,
                        "min_cost": 5.0,
                        "price_last": None,
                        "price_ts": None,
                        "min_order_price": None,
                    },
                    {
                        "exchange": ex,
                        "symbol": "BTCUSD_PERP",
                        "ccxt_symbol": "BTC/USD:BTC",
                        "active": True,
                        "linear": False,
                        "contract_size": 1.0,
                        "min_amount": 1.0,
                        "min_cost": None,
                        "price_last": None,
                        "price_ts": None,
                        "min_order_price": None,
                    },
                ]
            else:
                quote = "USDT" if ex in ("bybit", "bitget", "gateio", "okx") else "USDC"
                mapping = [
                    {
                        "exchange": ex,
                        "symbol": "BTC",
                        "ccxt_symbol": f"BTC/{quote}:{quote}",
                        "active": True,
                        "linear": True,
                        "contract_size": 1.0,
                        "min_amount": 0.001,
                        "min_cost": 10.0,
                        "price_last": None,
                        "price_ts": None,
                        "min_order_price": None,
                    },
                    {
                        "exchange": ex,
                        "symbol": "ETH",
                        "ccxt_symbol": f"ETH/{quote}:{quote}",
                        "active": True,
                        "linear": True,
                        "contract_size": 1.0,
                        "min_amount": 0.01,
                        "min_cost": 10.0,
                        "price_last": None,
                        "price_ts": None,
                        "min_order_price": None,
                    },
                ]

            coindata.save_exchange_mapping(ex, mapping)

        class FakeExchange:
            fetch_price_calls = 0

            def __init__(self, exchange_id):
                self.exchange_id = exchange_id

            def connect(self):
                return None

            def fetch_prices(self, symbols, market_type):
                # Return complete mocked batch so each exchange fills all prices.
                return {
                    sym: {"last": 60000.0 + idx, "timestamp": 1710000000000 + idx}
                    for idx, sym in enumerate(symbols)
                }

            def fetch_price(self, symbol, market_type):
                FakeExchange.fetch_price_calls += 1
                return {"last": 1.0, "timestamp": 1710000001000}

        durations = {}
        with patch("Exchange.Exchange", FakeExchange):
            total_start = time.perf_counter()
            for ex in exchanges:
                start = time.perf_counter()
                ok = coindata.update_prices(ex)
                durations[ex] = time.perf_counter() - start
                assert ok is True, f"update_prices failed for {ex}"

            total_duration = time.perf_counter() - total_start

        # Should stay very fast under mocked no-network conditions.
        assert total_duration < 1.0, f"All-exchange update_prices too slow: {total_duration:.3f}s"
        for ex, duration in durations.items():
            assert duration < 0.3, f"update_prices too slow for {ex}: {duration:.3f}s"

        # Verify each active row got a price.
        for ex in exchanges:
            updated = coindata.load_exchange_mapping(ex)
            active_rows = [r for r in updated if r.get("active", True)]
            priced_rows = [r for r in active_rows if float(r.get("price_last") or 0) > 0]
            assert len(priced_rows) == len(active_rows), f"Missing prices for {ex}"

        # No per-symbol fallback should be needed in this complete-batch scenario.
        assert FakeExchange.fetch_price_calls == 0

    def test_update_prices_bitget_recovers_missing_batch_prices(self, coindata, tmp_workdir):
        """Bitget recovers missing batch tickers via per-symbol fallback and fills prices."""

        mapping = [
            {
                "exchange": "bitget",
                "symbol": "BTCUSDT",
                "ccxt_symbol": "BTC/USDT:USDT",
                "active": True,
                "linear": True,
                "contract_size": 1.0,
                "min_amount": 0.001,
                "min_cost": 10.0,
                "price_last": None,
                "price_ts": None,
                "min_order_price": None,
            },
            {
                "exchange": "bitget",
                "symbol": "ETHUSDC",
                "ccxt_symbol": "ETH/USDC:USDC",
                "active": True,
                "linear": True,
                "contract_size": 1.0,
                "min_amount": 0.01,
                "min_cost": None,
                "price_last": None,
                "price_ts": None,
                "min_order_price": None,
            },
        ]
        coindata.save_exchange_mapping("bitget", mapping)

        class FakeExchange:
            fetch_price_calls = 0

            def __init__(self, exchange_id):
                self.exchange_id = exchange_id

            def connect(self):
                return None

            def fetch_prices(self, symbols, market_type):
                # Simulate partial batch response: second symbol missing
                return {
                    "BTC/USDT:USDT": {"last": 50000.0, "timestamp": 1710000000000},
                }

            def fetch_price(self, symbol, market_type):
                FakeExchange.fetch_price_calls += 1
                return {"last": 2500.0, "timestamp": 1710000001000}

        with patch("Exchange.Exchange", FakeExchange):
            start = time.perf_counter()
            ok = coindata.update_prices("bitget")
            duration = time.perf_counter() - start

        assert ok is True
        assert duration < 0.5, f"update_prices too slow in mocked bitget path: {duration:.3f}s"
        assert FakeExchange.fetch_price_calls == 1, "Expected per-symbol fallback for one missing ticker"

        updated = coindata.load_exchange_mapping("bitget")
        assert len(updated) == 2
        assert all(float(r.get("price_last") or 0) > 0 for r in updated)
        assert all(r.get("price_ts") is not None for r in updated)

        btc = next(r for r in updated if r["ccxt_symbol"] == "BTC/USDT:USDT")
        eth = next(r for r in updated if r["ccxt_symbol"] == "ETH/USDC:USDC")
        assert btc["min_order_price"] == pytest.approx(max(10.0, 0.001 * 1.0 * 50000.0))
        # min_cost=None must be handled safely as 0.0
        assert eth["min_order_price"] == pytest.approx(max(0.0, 0.01 * 1.0 * 2500.0))

    def test_update_prices_hyperliquid_skips_slow_per_symbol_fallback(self, coindata, tmp_workdir):
        """Hyperliquid must not run slow per-symbol fallback when batch misses symbols."""

        mapping = [
            {
                "exchange": "hyperliquid",
                "symbol": "BTC",
                "ccxt_symbol": "BTC/USDC:USDC",
                "active": True,
                "linear": True,
                "contract_size": 1.0,
                "min_amount": 0.001,
                "min_cost": 10.0,
                "price_last": None,
                "price_ts": None,
                "min_order_price": None,
            },
            {
                "exchange": "hyperliquid",
                "symbol": "XYZ-TSLA",
                "ccxt_symbol": "XYZ-TSLA/USDC:USDC",
                "active": True,
                "linear": True,
                "contract_size": 1.0,
                "min_amount": 0.01,
                "min_cost": 10.0,
                "price_last": None,
                "price_ts": None,
                "min_order_price": None,
            },
            {
                "exchange": "hyperliquid",
                "symbol": "KPEPE",
                "ccxt_symbol": "KPEPE/USDC:USDC",
                "active": True,
                "linear": True,
                "contract_size": 1.0,
                "min_amount": 0.1,
                "min_cost": 10.0,
                "price_last": None,
                "price_ts": None,
                "min_order_price": None,
            },
        ]
        coindata.save_exchange_mapping("hyperliquid", mapping)

        class FakeExchange:
            fetch_price_calls = 0

            def __init__(self, exchange_id):
                self.exchange_id = exchange_id

            def connect(self):
                return None

            def fetch_prices(self, symbols, market_type):
                # Simulate partial batch response (2 symbols missing)
                return {
                    "BTC/USDC:USDC": {"last": 68000.0, "timestamp": 1710000000000},
                }

            def fetch_price(self, symbol, market_type):
                # If this gets called, test should fail on both timing and counter
                FakeExchange.fetch_price_calls += 1
                time.sleep(0.1)
                return {"last": 1.0, "timestamp": 1710000001000}

        with patch("Exchange.Exchange", FakeExchange):
            start = time.perf_counter()
            ok = coindata.update_prices("hyperliquid")
            duration = time.perf_counter() - start

        assert ok is True
        assert FakeExchange.fetch_price_calls == 0, "Hyperliquid should skip per-symbol fallback"
        # The exact counter above proves that the slow fallback was skipped;
        # retain only a broad guard against an unrelated stall on loaded CI hosts.
        assert duration < 1.0, f"Hyperliquid update unexpectedly slow: {duration:.3f}s"

        updated = coindata.load_exchange_mapping("hyperliquid")
        priced = [r for r in updated if float(r.get("price_last") or 0) > 0]
        assert len(priced) == 1
        assert priced[0]["ccxt_symbol"] == "BTC/USDC:USDC"


# ============================================================================
# update_mappings() Integration Tests
# ============================================================================

class TestUpdateMappings:
    """Test the update_mappings() orchestration method."""

    def test_update_mappings_calls_all_steps(self, coindata, tmp_workdir, sample_ccxt_markets):
        """update_mappings calls fetch_ccxt_markets, fetch_copy_trading_symbols, build_mapping."""
        fetch_ccxt_called = []
        fetch_cpt_called = []
        build_called = []

        def mock_fetch_ccxt(exchange_id):
            fetch_ccxt_called.append(exchange_id)
            coindata.save_ccxt_markets(exchange_id, sample_ccxt_markets)
            return True

        def mock_fetch_cpt(exchange_id, markets=None):
            fetch_cpt_called.append(exchange_id)
            return []

        def mock_build(exchange_id):
            build_called.append(exchange_id)
            return True

        with patch.object(coindata, 'fetch_ccxt_markets', side_effect=mock_fetch_ccxt):
            with patch.object(coindata, 'fetch_copy_trading_symbols', side_effect=mock_fetch_cpt):
                with patch.object(coindata, 'build_mapping', side_effect=mock_build):
                    coindata.update_mappings()

        # Only V7-supported exchanges should be processed.
        from Exchange import V7
        v7_exchanges = V7.list()
        for exchange in v7_exchanges:
            assert exchange in fetch_ccxt_called, f"fetch_ccxt_markets not called for {exchange}"
            assert exchange in fetch_cpt_called, f"fetch_copy_trading_symbols not called for {exchange}"
            assert exchange in build_called, f"build_mapping not called for {exchange}"
        # Verify non-V7 exchanges were NOT processed
        for exchange in coindata.exchanges:
            if exchange not in v7_exchanges:
                assert exchange not in fetch_ccxt_called, f"Non-V7 exchange {exchange} should not be processed"

    def test_update_mappings_respects_interval(self, coindata, tmp_workdir):
        """update_mappings only runs when interval has elapsed."""
        call_count = 0

        # Disable self-heal path for this test so we only assert interval behavior.
        from Exchange import V7
        future_ts = datetime.now().timestamp() + 3600
        for exchange in V7.list():
            coindata._mapping_self_heal_state[exchange] = {
                "fails": 1,
                "next_retry_ts": future_ts,
            }

        def mock_fetch_ccxt(exchange_id):
            nonlocal call_count
            call_count += 1
            return True

        with patch.object(coindata, 'fetch_ccxt_markets', side_effect=mock_fetch_ccxt):
            with patch.object(coindata, 'fetch_copy_trading_symbols', return_value=[]):
                with patch.object(coindata, 'build_mapping', return_value=True):
                    coindata.update_mappings()  # First call: runs
                    first_count = call_count
                    coindata.update_mappings()  # Second call: skipped (interval not elapsed)
                    assert call_count == first_count, "Should not run again before interval"

    def test_update_mappings_sets_timestamp(self, coindata, tmp_workdir):
        """update_mappings updates the timestamp after completion."""
        assert coindata.update_mappings_ts == 0

        with patch.object(coindata, 'fetch_ccxt_markets', return_value=True):
            with patch.object(coindata, 'fetch_copy_trading_symbols', return_value=[]):
                with patch.object(coindata, 'build_mapping', return_value=True):
                    coindata.update_mappings()

        assert coindata.update_mappings_ts > 0

    def test_update_mappings_continues_on_exchange_error(self, coindata, tmp_workdir):
        """If one exchange fails, others are still processed."""
        processed = []

        def mock_fetch_ccxt(exchange_id):
            if exchange_id == "binance":
                raise Exception("Binance API error")
            processed.append(exchange_id)
            return True

        with patch.object(coindata, 'fetch_ccxt_markets', side_effect=mock_fetch_ccxt):
            with patch.object(coindata, 'fetch_copy_trading_symbols', return_value=[]):
                with patch.object(coindata, 'build_mapping', return_value=True):
                    coindata.update_mappings()

        # Other exchanges should still be processed
        assert len(processed) > 0
        assert "binance" not in processed

    def test_mapping_interval_configurable(self, coindata, tmp_workdir):
        """mapping_interval property can be set and affects update timing."""
        coindata.mapping_interval = 48
        assert coindata.mapping_interval == 48

    def test_mapping_interval_saved_in_config(self, coindata, tmp_workdir):
        """mapping_interval is persisted in pbgui.ini."""
        coindata.mapping_interval = 12
        coindata.save_config()

        config = configparser.ConfigParser()
        config.read(tmp_workdir / "pbgui.ini")
        assert config.get("coinmarketcap", "mapping_interval") == "12"

    def test_self_heal_failure_does_not_log_success(self, coindata, tmp_workdir, monkeypatch):
        """Self-heal logs failure (not success) and applies backoff when refresh result is not ok."""
        logged = []
        build_calls = []
        price_calls = []

        monkeypatch.setattr(PBCoinData_mod.V7, "list", staticmethod(lambda: ["hyperliquid"]))
        monkeypatch.setattr(PBCoinData_mod, "_log", lambda module, message, level='INFO': logged.append((level, message)))

        def _fetch_fail(exchange_id):
            return False

        monkeypatch.setattr(coindata, "fetch_ccxt_markets", _fetch_fail)
        monkeypatch.setattr(coindata, "build_mapping", lambda exchange_id: build_calls.append(exchange_id) or True)
        monkeypatch.setattr(coindata, "update_prices", lambda exchange_id: price_calls.append(exchange_id) or True)

        coindata.update_mappings()

        state = coindata._mapping_self_heal_state.get("hyperliquid", {})
        assert int(state.get("fails", 0)) >= 1
        assert not any("Self-heal mapping succeeded for hyperliquid" in msg for _, msg in logged)
        assert any("Self-heal mapping failed for hyperliquid" in msg for _, msg in logged)
        assert build_calls == []
        assert price_calls == []


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
