"""Pytest configuration and shared fixtures for PBGui tests.

This file sets up the test environment, adds the project root to sys.path,
and provides common fixtures used across multiple test modules.

Note: Mock modules (like PBCoinData.py) are provided as separate files in
the tests/ directory to avoid import issues.
"""

import os
import ipaddress
import socket
import sys
from pathlib import Path

import pytest


_OPT_IN_MARKERS = {
    "live_exchange": ("--run-live", "requires --run-live"),
    "external_pb7": ("--run-external-pb7", "requires --run-external-pb7"),
    "local_runtime": ("--run-local-runtime", "requires --run-local-runtime"),
}


@pytest.fixture(autouse=True)
def skip_production_startup_migrations(monkeypatch):
    """Prevent ordinary lifespan tests from touching runtime migration state."""
    monkeypatch.setenv("PBGUI_SKIP_STARTUP_MIGRATIONS", "1")
    api_server = sys.modules.get("PBApiServer")
    if api_server is not None:
        monkeypatch.setattr(
            api_server,
            "bootstrap_local_legacy_credentials",
            lambda _root: {"status": "test_skipped"},
        )


def _mock_normalize_symbol(symbol, symbol_mappings=None):
    """Return a lightweight normalized symbol for fallback test mocks."""
    base = str(symbol or "").strip().upper()
    if symbol_mappings and base in symbol_mappings:
        return str(symbol_mappings[base]).upper()
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if base.endswith(quote) and len(base) > len(quote):
            return base[: -len(quote)]
    return base


def _mock_compute_coin_name(market_id, quote=""):
    """Return a lightweight coin name for fallback test mocks."""
    base = str(market_id or "").strip().upper().replace("-", "").replace("_", "")
    quote = str(quote or "").strip().upper()
    if quote and base.endswith(quote) and len(base) > len(quote):
        base = base[: -len(quote)]
    return base


def _mock_get_symbol_for_coin(coin, exchange, use_cache=True):
    """Return a lightweight exchange symbol for fallback test mocks."""
    quote = "USDC" if "hyperliquid" in str(exchange or "").lower() else "USDT"
    return f"{str(coin or '').upper()}{quote}" if coin else ""


def _mock_build_symbol_mappings(symbols):
    """Return minimal symbol mappings for fallback test mocks."""
    return {str(symbol).upper(): _mock_normalize_symbol(symbol) for symbol in symbols or [] if str(symbol or "").strip()}


class _MockCoinData:
    """Fallback CoinData test double used when a local mock lacks CoinData."""

    def __init__(self):
        """Initialize empty coin lists."""
        self.approved_coins = []
        self.ignored_coins = []

    def load_mapping(self, exchange, use_cache=True):
        """Return an empty test mapping."""
        return []

    def filter_mapping(self, *args, **kwargs):
        """Return empty approved and ignored coin lists."""
        return list(self.approved_coins), list(self.ignored_coins)


def _ensure_pbcointdata_contract():
    """Ensure the active PBCoinData test module exposes commonly imported names."""
    try:
        import PBCoinData
    except ImportError:
        return
    fallbacks = {
        "CoinData": _MockCoinData,
        "normalize_symbol": _mock_normalize_symbol,
        "compute_coin_name": _mock_compute_coin_name,
        "get_symbol_for_coin": _mock_get_symbol_for_coin,
        "build_symbol_mappings": _mock_build_symbol_mappings,
    }
    for name, value in fallbacks.items():
        if not hasattr(PBCoinData, name):
            setattr(PBCoinData, name, value)


def pytest_addoption(parser):
    """Register explicit opt-ins for tests that use external or local runtime data."""
    group = parser.getgroup("pbgui integration tests")
    group.addoption("--run-live", action="store_true", help="run tests against live public market APIs")
    group.addoption(
        "--run-external-pb7",
        action="store_true",
        help="run tests requiring a separate local PB7 installation",
    )
    group.addoption(
        "--run-local-runtime",
        action="store_true",
        help="run read-only tests against local PBGui runtime data",
    )


def pytest_configure(config):
    """
    Pytest hook called before test collection starts.

    This is the earliest point where we can modify sys.path to ensure
    mock modules are found before real ones.
    """
    tests_dir = Path(__file__).parent.resolve()
    root_dir = Path(__file__).parent.parent.resolve()

    # Add root directory for importing pbgui modules
    if str(root_dir) not in sys.path:
        sys.path.append(str(root_dir))

    # Add tests directory FIRST (at index 0) so mock modules are found before real ones
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))

    # Force import of mock modules to cache them before test collection
    _ensure_pbcointdata_contract()


def pytest_collection_modifyitems(config, items):
    """Skip integration categories unless their matching opt-in was supplied."""
    for item in items:
        for marker, (option, reason) in _OPT_IN_MARKERS.items():
            if item.get_closest_marker(marker) and not config.getoption(option):
                item.add_marker(pytest.mark.skip(reason=reason))


def _loopback_or_unix_address(address) -> bool:
    """Return whether a socket target is local and safe for default tests."""
    if isinstance(address, (str, bytes)):
        return True
    if not isinstance(address, tuple) or not address:
        return False
    host = str(address[0]).strip().lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@pytest.fixture(autouse=True)
def block_unapproved_network(request, monkeypatch):
    """Fail closed when an ordinary test attempts an external connection."""
    allowed = any(
        request.node.get_closest_marker(marker) and request.config.getoption(option)
        for marker, (option, _reason) in _OPT_IN_MARKERS.items()
        if marker in {"live_exchange", "external_pb7"}
    )
    if allowed:
        return

    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def guarded_connect(sock, address):
        if not _loopback_or_unix_address(address):
            raise OSError(f"External network access is disabled for this test: {address!r}")
        return real_connect(sock, address)

    def guarded_connect_ex(sock, address):
        if not _loopback_or_unix_address(address):
            raise OSError(f"External network access is disabled for this test: {address!r}")
        return real_connect_ex(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)


# Also set paths at module level as fallback
ROOT_DIR = Path(__file__).parent.parent.resolve()
TESTS_DIR = Path(__file__).parent.resolve()

# Add root directory for importing pbgui modules
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# Add tests directory FIRST (at index 0) so mock modules are found before real ones
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

# Force import of mock modules at module level to cache them early
_ensure_pbcointdata_contract()


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def pbgui_root():
    """Return the absolute path to the PBGui root directory."""
    return ROOT_DIR


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def mock_exchange():
    """Create a minimal exchange mock for testing."""
    class MockExchange:
        id = "test_exchange"
        name = "Test Exchange"

        def __init__(self):
            self.markets = {}

    return MockExchange()


@pytest.fixture
def sample_config():
    """Return a sample configuration dictionary for testing."""
    return {
        "exchange": "hyperliquid",
        "user": "test_user",
        "market_type": "swap",
    }
