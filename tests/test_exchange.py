"""
Comprehensive unit tests for Exchange.py.

Tests cover the Exchange class and related enums/constants WITHOUT
requiring live API calls. All CCXT interactions are mocked.

Areas tested:
- Enum definitions (Exchanges, V7, Passphrase)
- Exchange constructor and property initialization
- connect() — ccxt instance creation and option configuration
- mapping-only symbol resolution for execution polling
- HIP-3 fetchMarkets config — Hyperliquid-specific
- Module constants (DEFAULT_CCXT_TIMEOUT_MS, MAX_PRIVATE_WS_*)
"""

import sys
import json
import configparser
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on path
ROOT_DIR = Path(__file__).parent.parent.resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import Exchange as ExchangeModule
from Exchange import Exchange, Exchanges, V7, Passphrase


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_ccxt():
    """Create a mock ccxt module with a configurable exchange factory."""
    class MockExchangeInstance:
        def __init__(self):
            self.options = {}
            self.timeout = None
            self.enableRateLimit = None
            self.apiKey = None
            self.secret = None
            self.password = None
            self.walletAddress = None
            self.privateKey = None
            self.timeframes = {'1m': '1m', '5m': '5m', '1h': '1h', '1d': '1d'}
            self.markets = {}

        def checkRequiredCredentials(self):
            pass

        def load_markets(self):
            return self.markets

    mock = MagicMock()
    mock_instance = MockExchangeInstance()

    # Make getattr(ccxt, exchange_id)() return our mock instance
    for ex_id in ['binance', 'bybit', 'bitget', 'hyperliquid', 'okx',
                   'kucoinfutures', 'gateio']:
        mock_class = MagicMock(return_value=mock_instance)
        setattr(mock, ex_id, mock_class)

    return mock, mock_instance


@pytest.fixture
def exchange_binance(mock_ccxt):
    """Create a Binance Exchange instance with mocked ccxt."""
    mock, instance = mock_ccxt
    with patch.object(ExchangeModule, 'ccxt', mock):
        ex = Exchange("binance")
        return ex, instance


@pytest.fixture
def exchange_hyperliquid(mock_ccxt):
    """Create a Hyperliquid Exchange instance with mocked ccxt."""
    mock, instance = mock_ccxt
    with patch.object(ExchangeModule, 'ccxt', mock):
        ex = Exchange("hyperliquid")
        return ex, instance


@pytest.fixture
def sample_binance_markets():
    """Sample Binance markets dict for testing."""
    return {
        "BTC/USDT:USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "swap": True,
            "spot": False,
            "active": True,
            "linear": True,
            "contractSize": 1.0,
            "limits": {
                "amount": {"min": 0.001},
                "cost": {"min": 5.0},
                "leverage": {"max": 125}
            },
            "precision": {"amount": 0.001, "price": 0.01},
            "info": {
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001"},
                ]
            }
        },
        "ETH/USDT:USDT": {
            "id": "ETHUSDT",
            "symbol": "ETH/USDT:USDT",
            "base": "ETH",
            "quote": "USDT",
            "swap": True,
            "spot": False,
            "active": True,
            "linear": True,
            "contractSize": 1.0,
            "limits": {
                "amount": {"min": 0.01},
                "cost": {"min": 5.0},
                "leverage": {"max": 100}
            },
            "precision": {"amount": 0.01, "price": 0.01},
            "info": {
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.01"},
                ]
            }
        },
        "BTC/USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT",
            "base": "BTC",
            "quote": "USDT",
            "swap": False,
            "spot": True,
            "active": True,
            "linear": False,
        },
    }


@pytest.fixture
def sample_hyperliquid_markets():
    """Sample Hyperliquid markets dict with crypto and HIP-3."""
    return {
        "BTC/USDC:USDC": {
            "id": "0",
            "symbol": "BTC/USDC:USDC",
            "base": "BTC",
            "quote": "USDC",
            "swap": True,
            "spot": False,
            "active": True,
            "linear": True,
            "contractSize": 1.0,
            "info": {"maxLeverage": 50, "name": "BTC"},
            "limits": {
                "amount": {"min": 0.0001},
                "cost": {"min": 10.0},
                "leverage": {"max": 50}
            },
            "precision": {"amount": 0.0001, "price": 0.1},
        },
        "ETH/USDC:USDC": {
            "id": "1",
            "symbol": "ETH/USDC:USDC",
            "base": "ETH",
            "quote": "USDC",
            "swap": True,
            "spot": False,
            "active": True,
            "linear": True,
            "contractSize": 1.0,
            "info": {"maxLeverage": 50, "name": "ETH"},
            "limits": {
                "amount": {"min": 0.001},
                "cost": {"min": 10.0},
                "leverage": {"max": 50}
            },
            "precision": {"amount": 0.001, "price": 0.01},
        },
        "XYZ-TSLA/USDC:USDC": {
            "id": "110001",
            "symbol": "XYZ-TSLA/USDC:USDC",
            "base": "XYZ-TSLA",
            "quote": "USDC",
            "swap": True,
            "spot": False,
            "active": True,
            "linear": True,
            "contractSize": 1.0,
            "info": {"hip3": True, "dex": "xyz", "onlyIsolated": True},
            "limits": {
                "amount": {"min": 0.01},
                "cost": {"min": 10.0},
                "leverage": {"max": 10}
            },
            "precision": {"amount": 0.01, "price": 0.01},
        },
    }


@pytest.fixture
def sample_bybit_markets():
    """Sample Bybit markets dict for testing."""
    return {
        "BTC/USDT:USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "swap": True,
            "spot": False,
            "active": True,
            "linear": True,
            "contractSize": 1.0,
            "info": {"copyTrading": "both"},
            "limits": {
                "amount": {"min": 0.001},
                "cost": {"min": None},
                "leverage": {"max": 100}
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
            "linear": True,
            "contractSize": 1.0,
            "info": {"copyTrading": "none"},
            "limits": {
                "amount": {"min": 0.01},
                "cost": {"min": None},
                "leverage": {"max": 100}
            },
            "precision": {"amount": 0.01, "price": 0.01},
        },
    }


# ============================================================================
# Enum Tests
# ============================================================================

class TestEnums:
    """Test exchange enum definitions and their list() methods."""

    def test_exchanges_contains_all_supported(self):
        """All supported exchanges must be in the Exchanges enum."""
        expected = {"binance", "bybit", "bitget", "gateio",
                    "hyperliquid", "okx", "kucoin"}
        assert set(Exchanges.list()) == expected

    def test_v7_exchanges(self):
        """PB7 supported exchanges."""
        result = set(V7.list())
        assert "hyperliquid" in result
        assert "gateio" in result
        assert "kucoin" in result

    def test_passphrase_exchanges(self):
        """Exchanges requiring a passphrase."""
        assert set(Passphrase.list()) == {"bitget", "okx", "kucoin"}

    def test_enum_list_returns_strings(self):
        """All enum list() methods return lists of strings."""
        for enum_cls in [Exchanges, V7, Passphrase]:
            result = enum_cls.list()
            assert isinstance(result, list)
            assert all(isinstance(x, str) for x in result)


# ============================================================================
# Constructor Tests
# ============================================================================

class TestExchangeConstructor:
    """Test Exchange.__init__() behavior."""

    def test_basic_init(self):
        """Constructor sets basic attributes correctly."""
        ex = Exchange("binance")
        assert ex.name == "binance"
        assert ex.id == "binance"
        assert ex.instance is None
        assert ex._markets is None

    def test_kucoin_remapping(self):
        """kucoin is remapped to kucoinfutures internally."""
        ex = Exchange("kucoin")
        assert ex.name == "kucoin"
        assert ex.id == "kucoinfutures"

    def test_user_set(self):
        """User is stored correctly."""
        user = MagicMock()
        user.name = "test_user"
        ex = Exchange("binance", user=user)
        assert ex.user == user

    def test_no_user(self):
        """No user defaults to None."""
        ex = Exchange("binance")
        assert ex.user is None

    @pytest.mark.parametrize("exchange_id", Exchanges.list())
    def test_all_exchanges_can_be_instantiated(self, exchange_id):
        """Every supported exchange can be instantiated without error."""
        ex = Exchange(exchange_id)
        assert ex.name == exchange_id


# ============================================================================
# connect() Tests
# ============================================================================

class TestConnect:
    """Test Exchange.connect() for various exchanges."""

    def test_connect_sets_instance(self, mock_ccxt):
        """connect() creates a ccxt instance."""
        mock, instance = mock_ccxt
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            ex.connect()
            assert ex.instance is not None

    def test_connect_sets_timeout(self, mock_ccxt):
        """connect() applies default timeout."""
        mock, instance = mock_ccxt
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            ex.connect()
            assert ex.instance.timeout == ExchangeModule.DEFAULT_CCXT_TIMEOUT_MS

    def test_connect_enables_rate_limit(self, mock_ccxt):
        """connect() enables rate limiting."""
        mock, instance = mock_ccxt
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            ex.connect()
            assert ex.instance.enableRateLimit is True

    def test_connect_sets_default_type_swap(self, mock_ccxt):
        """connect() sets defaultType to swap."""
        mock, instance = mock_ccxt
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            ex.connect()
            assert ex.instance.options.get('defaultType') == 'swap'

    def test_connect_hyperliquid_sets_hip3_config(self, mock_ccxt):
        """Hyperliquid connect() configures HIP-3 fetchMarkets."""
        mock, instance = mock_ccxt
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("hyperliquid")
            ex.connect()
            fm = ex.instance.options.get('fetchMarkets')
            assert fm is not None
            assert fm['types'] == ['swap', 'hip3']
            assert fm['hip3']['dexes'] == []

    def test_connect_non_hyperliquid_no_hip3(self, mock_ccxt):
        """Non-Hyperliquid exchanges do NOT set HIP-3 config."""
        mock, instance = mock_ccxt
        for ex_id in ["binance", "bybit", "bitget", "okx"]:
            with patch.object(ExchangeModule, 'ccxt', mock):
                ex = Exchange(ex_id)
                ex.connect()
                assert ex.instance.options.get('fetchMarkets') is None

    def test_connect_sets_recv_window(self, mock_ccxt):
        """connect() sets recvWindow."""
        mock, instance = mock_ccxt
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            ex.connect()
            assert ex.instance.options.get('recvWindow') == 10000

    def test_connect_with_user_credentials(self, mock_ccxt):
        """connect() applies user credentials to the instance."""
        mock, instance = mock_ccxt
        user = MagicMock()
        user.key = "test_api_key"
        user.secret = "test_secret"
        user.passphrase = "test_pass"
        user.wallet_address = "0xabc"
        user.private_key = "privkey"

        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance", user=user)
            ex.connect()
            assert ex.instance.apiKey == "test_api_key"
            assert ex.instance.secret == "test_secret"
            assert ex.instance.password == "test_pass"
            assert ex.instance.walletAddress == "0xabc"
            assert ex.instance.privateKey == "privkey"

    def test_connect_no_credentials_when_key_is_default(self, mock_ccxt):
        """connect() skips credentials when user.key == 'key' (default)."""
        mock, instance = mock_ccxt
        user = MagicMock()
        user.key = "key"

        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance", user=user)
            ex.connect()
            assert ex.instance.apiKey is None

# ============================================================================
# Mapping Symbol Resolver Tests
# ============================================================================

class TestMappingSymbolResolver:
    """Test mapping-only execution symbol resolution."""

    def _write_mapping(self, root: Path, exchange: str, rows: list[dict]) -> None:
        """Write a minimal CoinData mapping fixture."""
        mapping_dir = root / "data" / "coindata" / exchange
        mapping_dir.mkdir(parents=True, exist_ok=True)
        (mapping_dir / "mapping.json").write_text(json.dumps(rows), encoding="utf-8")

    def test_resolves_binance_market_id(self, tmp_path):
        """Binance market ID resolves from mapping.json."""
        self._write_mapping(tmp_path, "binance", [{
            "symbol": "BTCUSDT",
            "ccxt_symbol": "BTC/USDT:USDT",
            "coin": "BTC",
            "quote": "USDT",
        }])

        with patch.object(ExchangeModule, 'PBGDIR', tmp_path):
            assert ExchangeModule._resolve_ccxt_symbol_from_mapping("binance", "BTCUSDT") == "BTC/USDT:USDT"

    def test_resolves_bitget_coin_quote(self, tmp_path):
        """Bitget PBGui symbol resolves from mapping.json."""
        self._write_mapping(tmp_path, "bitget", [{
            "symbol": "BTCUSDT",
            "ccxt_symbol": "BTC/USDT:USDT",
            "coin": "BTC",
            "quote": "USDT",
        }])

        with patch.object(ExchangeModule, 'PBGDIR', tmp_path):
            assert ExchangeModule._resolve_ccxt_symbol_from_mapping("bitget", "BTCUSDT") == "BTC/USDT:USDT"

    def test_resolves_kucoinfutures_alias_to_kucoin_mapping(self, tmp_path):
        """KuCoin futures resolves through the kucoin mapping directory."""
        self._write_mapping(tmp_path, "kucoin", [{
            "symbol": "XBTUSDTM",
            "ccxt_symbol": "BTC/USDT:USDT",
            "coin": "BTC",
            "quote": "USDT",
        }])

        with patch.object(ExchangeModule, 'PBGDIR', tmp_path):
            assert ExchangeModule._resolve_ccxt_symbol_from_mapping("kucoinfutures", "XBTUSDTM") == "BTC/USDT:USDT"
            assert ExchangeModule._resolve_ccxt_symbol_from_mapping("kucoinfutures", "BTCUSDT") == "BTC/USDT:USDT"

    def test_missing_mapping_does_not_fallback(self, tmp_path):
        """Missing symbols return None instead of constructing a symbol."""
        self._write_mapping(tmp_path, "kucoin", [{
            "symbol": "ETHUSDTM",
            "ccxt_symbol": "ETH/USDT:USDT",
            "coin": "ETH",
            "quote": "USDT",
        }])

        with patch.object(ExchangeModule, 'PBGDIR', tmp_path):
            assert ExchangeModule._resolve_ccxt_symbol_from_mapping("kucoinfutures", "BTCUSDT") is None


# ============================================================================
# fetch_executions() Tests
# ============================================================================

class TestFetchExecutions:
    """Test execution-level trade normalization."""

    def test_kucoinfutures_fetches_global_fills(self):
        """KuCoin futures executions use global fills and normalize rows."""
        user = MagicMock()
        user.key = "test_key"
        ex = Exchange("kucoin", user=user)

        class FakeKucoinInstance:
            """Minimal KuCoin futures CCXT stub."""

            def milliseconds(self):
                return 2000

            def fetch_my_trades(self, symbol=None, since=None, limit=None, params=None):
                assert symbol is None
                assert since == 1000
                assert limit == 1000
                assert params == {'until': 2000}
                return [
                    {
                        'id': 'trade-1',
                        'timestamp': 1500,
                        'symbol': 'BTC/USDT:USDT',
                        'side': 'buy',
                        'price': '50000',
                        'amount': '0.01',
                        'fee': {'cost': '0.02'},
                        'order': 'order-1',
                        'info': {
                            'tradeId': 'trade-1',
                            'symbol': 'XBTUSDTM',
                            'orderId': 'order-1',
                        },
                    }
                ]

        ex.instance = FakeKucoinInstance()

        rows = ex.fetch_executions(since=1000)

        assert rows == [
            {
                'symbol': 'BTC/USDT:USDT',
                'timestamp': 1500,
                'side': 'buy',
                'price': 50000.0,
                'qty': 0.01,
                'fee': 0.02,
                'realized_pnl': None,
                'order_id': 'order-1',
                'trade_id': 'trade-1',
                'raw_json': json.dumps(
                    {
                        'id': 'trade-1',
                        'timestamp': 1500,
                        'symbol': 'BTC/USDT:USDT',
                        'side': 'buy',
                        'price': '50000',
                        'amount': '0.01',
                        'fee': {'cost': '0.02'},
                        'order': 'order-1',
                        'info': {
                            'tradeId': 'trade-1',
                            'symbol': 'XBTUSDTM',
                            'orderId': 'order-1',
                        },
                    },
                    ensure_ascii=False,
                    default=str,
                ),
            }
        ]


# ============================================================================
# load_market() Tests
# ============================================================================

class TestLoadMarket:
    """Test market loading and caching."""

    def test_load_market_returns_dict(self, mock_ccxt, sample_binance_markets):
        """load_market() returns the markets dict."""
        mock, instance = mock_ccxt
        instance.load_markets = MagicMock(return_value=sample_binance_markets)
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            result = ex.load_market()
            assert isinstance(result, dict)
            assert "BTC/USDT:USDT" in result

    def test_load_market_auto_connects(self, mock_ccxt, sample_binance_markets):
        """load_market() calls connect() if no instance."""
        mock, instance = mock_ccxt
        instance.load_markets = MagicMock(return_value=sample_binance_markets)
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            assert ex.instance is None
            ex.load_market()
            assert ex.instance is not None

    def test_load_market_caches_markets(self, mock_ccxt, sample_binance_markets):
        """load_market() stores result in _markets."""
        mock, instance = mock_ccxt
        instance.load_markets = MagicMock(return_value=sample_binance_markets)
        with patch.object(ExchangeModule, 'ccxt', mock):
            ex = Exchange("binance")
            ex.load_market()
            assert ex._markets == sample_binance_markets


# ============================================================================
# Module Constants Tests
# ============================================================================

class TestModuleConstants:
    """Test module-level constants are correctly defined."""

    def test_default_timeout(self):
        """DEFAULT_CCXT_TIMEOUT_MS is a reasonable value."""
        assert ExchangeModule.DEFAULT_CCXT_TIMEOUT_MS == 120000

    def test_max_private_ws_global(self):
        """MAX_PRIVATE_WS_GLOBAL has a sane default."""
        assert isinstance(ExchangeModule.MAX_PRIVATE_WS_GLOBAL, int)
        assert ExchangeModule.MAX_PRIVATE_WS_GLOBAL > 0

    def test_max_private_ws_per_exchange_is_dict(self):
        """MAX_PRIVATE_WS_PER_EXCHANGE is a dict with exchange keys."""
        caps = ExchangeModule.MAX_PRIVATE_WS_PER_EXCHANGE
        assert isinstance(caps, dict)
        # Should have entries for supported exchanges
        assert len(caps) > 0
        assert caps['kucoinfutures'] > 0

    def test_user_property(self):
        """User property getter/setter works."""
        ex = Exchange("binance")
        assert ex.user is None
        user = MagicMock()
        ex.user = user
        assert ex.user == user
