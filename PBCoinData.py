import psutil
import subprocess
from time import sleep, time_ns
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import math
import pbgui_purefunc
from pathlib import Path, PurePath
from datetime import datetime
import platform
import sys
import os
import re
import traceback
from dataclasses import dataclass
from functools import wraps
import tempfile
from urllib.parse import urlparse
from Exchange import Exchange, Exchanges, V7
from cmc_pool import CmcPoolClient, CmcPoolExhaustedError
from cmc_runtime import build_cmc_pool_client
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from market_symbol_mapping import disambiguate_multiplier_market_coins
from pbgui_purefunc import IniSnapshot, load_ini_snapshot, save_ini, update_ini
from ini_watcher import IniWatcher

SERVICE = "PBCoinData"

_PRICE_BATCH_MAX_ATTEMPTS = 3
_PRICE_BATCH_BACKOFF_SECONDS = (0.25, 0.5)
_PRICE_INDIVIDUAL_FALLBACK_LIMIT = 20


class CoinDataPersistenceError(RuntimeError):
    """Report a CoinData publication failure to the refresh caller."""


def _exchange_lock_target(exchange: str) -> Path:
    """Return the shared transaction lock target for one exchange."""
    exchange_key = str(exchange or "").strip().lower()
    if (
        not exchange_key
        or exchange_key in {".", ".."}
        or "/" in exchange_key
        or "\\" in exchange_key
        or "\x00" in exchange_key
    ):
        raise ValueError("Invalid exchange identifier")
    return Path.cwd() / "data" / "coindata" / ".locks" / f"exchange-{exchange_key}"


def _exchange_transaction(method):
    """Serialize an exchange mutation across threads and processes."""
    @wraps(method)
    def locked(self, exchange, *args, **kwargs):
        with advisory_file_lock(_exchange_lock_target(exchange)):
            return method(self, exchange, *args, **kwargs)

    return locked


def _atomic_json_write(path: Path, payload) -> None:
    """Durably replace one JSON file using a unique same-directory temporary."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(payload, handle, indent=4)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        if os.name == "posix":
            descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


@dataclass(frozen=True)
class CoinDataRuntimeConfig:
    """Validated settings applied atomically by the CoinData owner."""

    fetch_limit: int = 5000
    fetch_interval: int = 24
    metadata_interval: int = 1
    mapping_interval: int = 24


class CoinDataConfigError(ValueError):
    """Identify a rejected setting without retaining its sensitive value."""

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Invalid INI value [coinmarketcap] {key}")


def _arg_matches_path(arg: str, expected_path: Path) -> bool:
    if not arg:
        return False
    expected = str(expected_path)
    expected_alt = expected.replace("/", "\\")
    return str(arg).endswith(expected) or str(arg).endswith(expected_alt)


def remove_powers_of_ten(text):
    """
    Remove any variant of "10", "100", "1000", "10000", etc. from a string.
    Handles cases like "1000SHIB" -> "SHIB", "1000000BABYDOGE" -> "BABYDOGE".
    Same logic as passivbot's utils.py.
    """
    pattern = r"(?<!\d)1(?:0+)(?!\d)"
    return re.sub(pattern, "", text)


_HYPERLIQUID_K_PREFIX_COINS = {"BONK", "FLOKI", "LUNC", "PEPE", "SHIB", "DOGS", "NEIRO"}
_HYPERLIQUID_HIP3_DEX_PREFIXES = {"XYZ", "FLX", "CASH", "HYNA", "KM", "VNTL", "ABCD"}


def _strip_hyperliquid_k_prefix(name: str) -> str:
    """Normalize Hyperliquid k/K prefix coins to short name (kPEPE/ KPEPE -> PEPE)."""
    if not name:
        return name
    if len(name) <= 1:
        return name
    if name[0] in ("k", "K"):
        tail = name[1:]
        if tail.upper() in _HYPERLIQUID_K_PREFIX_COINS:
            return tail
    return name


def _is_hyperliquid_hip3_base_symbol(name: str) -> bool:
    s = str(name or "").strip().upper()
    if not s or "-" not in s:
        return False
    prefix, tail = s.split("-", 1)
    if not prefix or not tail:
        return False
    return prefix in _HYPERLIQUID_HIP3_DEX_PREFIXES


def _normalize_hyperliquid_hip3_alias(symbol: str) -> str:
    """Normalize supported Hyperliquid HIP-3 aliases to PBGui's XYZ-TICKER form."""
    value = str(symbol or "").strip().upper()
    if not value:
        return ""
    base = value.split("/", 1)[0].strip()
    if base.startswith("XYZ:") and len(base) > 4:
        return f"XYZ-{base[4:].strip()}"
    if _is_hyperliquid_hip3_base_symbol(base):
        return base
    return ""


def compute_coin_name(market_id, quote=""):
    """
    Compute PB7-compatible coin name from exchange market_id and quote currency.
    
    Derives the coin name the same way the ini pipeline does:
    1. Strip contract type suffixes (-SWAP, -PERP, _PERP)
    2. Remove exchange-specific separators (dashes, underscores)
    3. Strip quote currency suffix (USDT, USDC, etc.)
    4. Strip bare PERP suffix (Bybit/Bitget USDC: BTCPERP → BTC)
    5. Handle k-prefix (Hyperliquid: kPEPE → PEPE)
    6. Strip 1000x multiplier prefixes (1000SHIB → SHIB)
    
    Uses market_id (not CCXT base) because CCXT sometimes returns display
    names that differ from the trading symbol (e.g. DegenReborn for DEGENUSDT
    on Bitget, RedLang for RED_USDT on Gateio).
    
    Args:
        market_id: Exchange market ID (e.g., "DEGENUSDT", "BTC-USDT-SWAP",
                   "BTC_USDT", "1000SHIBUSDT", "BTCPERP")
        quote: Quote currency to strip (e.g., "USDT", "USDC", "SUSDT")
    
    Returns:
        str: Normalized coin name, uppercase (e.g., "DEGEN", "BTC", "SHIB")
    """
    if not market_id:
        return ""
    name = market_id
    # Strip contract type suffixes (OKX: -SWAP; some: -PERP, _PERP)
    for suffix in ("-SWAP", "-PERP", "_PERP"):
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    # Remove exchange-specific separators (OKX dashes, Gateio underscores)
    name = name.replace("-", "").replace("_", "")
    # Strip quote currency suffix. KuCoin futures market IDs append an extra
    # contract marker after the quote (e.g. XBTUSDTM, ETHUSDTM).
    if quote and name.upper().endswith(f"{quote.upper()}M"):
        name = name[:-(len(quote) + 1)]
    elif quote and name.upper().endswith(quote.upper()):
        name = name[:-len(quote)]
    # Strip bare PERP suffix (Bybit/Bitget USDC markets: BTCPERP -> BTC)
    if name.upper().endswith("PERP") and len(name) > 4:
        name = name[:-4]
    # Handle Hyperliquid k/K-prefix (kPEPE/KPEPE -> PEPE)
    name = _strip_hyperliquid_k_prefix(name)
    # Strip 1000x multiplier prefixes (1000SHIB -> SHIB)
    name = remove_powers_of_ten(name)
    if name.upper() == "XBT":
        name = "BTC"
    return name.upper()


def _is_tradfi_market(exchange_id: str, market: dict) -> bool:
    """Identify exchange-native TradFi/RWA perpetuals from authoritative metadata."""
    info = market.get("info") or {}
    if exchange_id == "binance":
        return str(info.get("contractType") or "").upper() == "TRADIFI_PERPETUAL"
    if exchange_id == "bitget":
        return str(info.get("isRwa") or "").upper() == "YES"
    return False


def build_symbol_mappings(symbols):
    """
    Build dynamic symbol mappings from exchange symbols.
    Creates variants like passivbot does:
    - Original symbol
    - Without 'k' prefix (kSHIB -> SHIB)
    - Without powers of ten (1000SHIB -> SHIB)
    - Combined (k1000SHIB -> SHIB)
    
    Args:
        symbols: List of trading pair symbols (e.g., ["1000SHIBUSDT", "BTCUSDT"])
    
    Returns:
        dict: Mapping of symbol variants to normalized base coin
    """
    mappings = {}
    
    for symbol in symbols:
        # Remove quote currency suffixes
        base = symbol
        
        # Check for stablecoin/quote-like patterns
        if base in ["USDC", "USDT", "BUSD", "TUSD", "DAI"]:
            continue
        
        for quote in ["USDT", "USDC", "BUSD", "USD"]:
            if base.endswith(quote):
                remaining = base[:-len(quote)]
                if not remaining:
                    continue
                # After stripping, check if result looks like a quote-based coin
                if remaining.startswith(("USD", "EUR", "GBP")) and len(remaining) <= 5:
                    base = remaining
                    break
                base = remaining
                break
        
        # Create variants like passivbot
        variants = set()
        variants.add(base)  # Original: 1000SHIB
        variants.add(base.replace("k", ""))  # Without k: 1000SHIB
        variants.add(remove_powers_of_ten(base))  # Without 1000: SHIB
        cleaned = remove_powers_of_ten(base.replace("k", ""))  # Both: SHIB
        variants.add(cleaned)
        
        # Map all variants to the cleaned base coin
        for variant in variants:
            if variant:  # Skip empty strings
                mappings[variant] = cleaned
    
    return mappings


def normalize_symbol(symbol, symbol_mappings=None):
    """
    Normalize a trading symbol to its base coin name.
    
    Args:
        symbol: Trading pair symbol (e.g., "1000SHIBUSDT", "kPEPE", "BTCUSDT")
        symbol_mappings: Optional pre-built mapping dict from build_symbol_mappings()
    
    Returns:
        str: Normalized base coin (e.g., "SHIB", "PEPE", "BTC")
    """
    if not symbol:
        return ""
    
    # Remove quote currency suffixes
    base = str(symbol).strip().upper()

    hip3_alias = _normalize_hyperliquid_hip3_alias(base)
    if hip3_alias:
        return hip3_alias
    
    # Check for stablecoin/quote-like patterns that should NOT be stripped further
    # These are coins whose names resemble quotes (USDe, USDC as trading pair, etc.)
    if base in ["USDC", "USDT", "BUSD", "TUSD", "DAI"]:
        # These are either stablecoins traded as pairs or the coin itself
        return base

    # Preserve Hyperliquid HIP-3 base symbols (XYZ-TSLA, XYZ-HYUNDAI, FLX-GOLD, ...)
    # as-is to avoid accidental quote stripping (e.g. XYZ-HYUNDAI -> XYZ-HYUN).
    if _is_hyperliquid_hip3_base_symbol(base):
        return str(base).upper()
    
    for quote in ["USDT", "USDC", "BUSD", "TUSD", "USD", "EUR", "GBP", "DAI"]:
        if base.endswith(quote):
            remaining = base[:-len(quote)]
            if not remaining:
                continue  # Don't strip if nothing remains
            # Avoid over-stripping when quote appears as a hyphenated/base suffix,
            # e.g. "XYZ-EUR" -> keep as-is instead of producing "XYZ-".
            if remaining.endswith(("-", "_", ":", "/")):
                continue
            # After stripping, check if result looks like a quote-based coin (USDe, USD1, EURo)
            # Pattern: starts with quote prefix + has only 1-2 additional chars
            if remaining.startswith(("USD", "EUR", "GBP")) and len(remaining) <= 5:
                # This is likely a coin with quote prefix (USDe, USD1, etc.), keep it
                base = remaining
                break
            # Strip the quote
            base = remaining
            break
    
    # Handle Hyperliquid format: kPEPE/KPEPE -> PEPE
    base = _strip_hyperliquid_k_prefix(base)
    
    # Use dynamic mappings if provided (already contains all normalization logic)
    if symbol_mappings and base in symbol_mappings:
        return symbol_mappings[base]
    
    # NOTE: CMC symbol matching is handled in CoinData.build_mapping() using
    # data-driven matching heuristics (no static SYMBOLMAP).
    
    # Dynamic pattern matching for multiplier prefixes (e.g., 1000X, 10000X, 1000000X)
    # This handles cases like 10000ELON -> ELON, 1000PEPE -> PEPE, etc.
    import re
    multiplier_match = re.match(r'^(\d+)([A-Z].*)$', base)
    if multiplier_match:
        multiplier, coin = multiplier_match.groups()
        # Only normalize if multiplier is 1000, 10000, 100000, 1000000, 10000000, etc.
        if multiplier in ['1000', '10000', '100000', '1000000', '10000000', '1000000000']:
            return coin
    
    # Last resort: return base as-is (should rarely happen if mappings are built correctly)
    return base


def get_normalized_coins(symbols, symbol_mappings=None):
    """
    Get unique normalized coin names from a list of trading symbols.
    Removes duplicates (e.g., BTCUSDT and BTCUSDC both become BTC).
    
    Args:
        symbols: List of trading pair symbols
        symbol_mappings: Optional pre-built mapping dict
    
    Returns:
        list: Sorted list of unique normalized coin names
    
    Examples:
        ["BTCUSDT", "BTCUSDC", "1000SHIBUSDT", "kPEPE"] -> ["BTC", "PEPE", "SHIB"]
    """
    if not symbols:
        return []
    
    coins = set()
    for symbol in symbols:
        normalized = normalize_symbol(symbol, symbol_mappings)
        if normalized:
            coins.add(normalized)
    
    return sorted(list(coins))


# Cache for coin_to_symbol mappings
_COIN_TO_SYMBOL_CACHE = {}
_COIN_TO_SYMBOL_CACHE_SIG = {}


def _read_json_with_retry(path: Path, retries: int = 1, delay_s: float = 0.2):
    """Read JSON file with a short retry window for transient partial writes."""
    attempts = max(0, int(retries)) + 1
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            last_error = e
            if attempt < attempts:
                _log(SERVICE, f'Retrying JSON read for {path} ({attempt}/{attempts - 1}) after error: {e}', level='WARNING')
                sleep(delay_s)
                continue
            break
    _log(SERVICE, f'Failed to read JSON file {path}: {last_error}', level='WARNING')
    return None


def get_symbol_for_coin(coin: str, exchange: str, use_cache=True) -> str:
    """
    Convert normalized coin back to exchange-specific trading symbol.
    
    This function performs the reverse operation of normalize_symbol():
    - BTC + binance.swap → BTCUSDT
    - PEPE + binance.swap → 1000PEPEUSDT
    - PEPE + hyperliquid.swap → kPEPEUSDC
    
    Args:
        coin: Normalized coin name (e.g., "BTC", "PEPE", "SHIB")
        exchange: Exchange key from pbgui.ini (e.g., "binance.swap", "hyperliquid.swap")
        use_cache: Whether to use cached mappings (default: True)
    
    Returns:
        Trading symbol for the exchange (e.g., "BTCUSDT", "1000PEPEUSDT")
        Falls back to {coin}USDT if no mapping found.
    
    Examples:
        >>> get_symbol_for_coin("BTC", "binance.swap")
        "BTCUSDT"
        >>> get_symbol_for_coin("PEPE", "binance.swap")
        "1000PEPEUSDT"
        >>> get_symbol_for_coin("PEPE", "hyperliquid.swap")
        "kPEPEUSDC"
    """
    exchange_key = str(exchange or "").strip().lower()
    exchange_id, _, market_type = exchange_key.partition(".")
    if not exchange_id:
        exchange_id = exchange_key
    market_type = market_type or "swap"
    coin_key = str(coin or "").upper()

    mapping_path = Path.cwd() / "data" / "coindata" / exchange_id / "mapping.json"
    mapping_sig = None
    if mapping_path.exists():
        stat = mapping_path.stat()
        mapping_sig = (stat.st_mtime_ns, stat.st_size)

    # Check cache first
    if (
        use_cache
        and exchange_key in _COIN_TO_SYMBOL_CACHE
        and _COIN_TO_SYMBOL_CACHE_SIG.get(exchange_key) == mapping_sig
    ):
        coin_map = _COIN_TO_SYMBOL_CACHE[exchange_key]
        if coin_key in coin_map:
            return coin_map[coin_key]

    coin_map = {}
    if mapping_path.exists():
        mapping = _read_json_with_retry(mapping_path, retries=1, delay_s=0.1)
        if not isinstance(mapping, list):
            mapping = []

        for record in disambiguate_multiplier_market_coins(mapping):
            symbol = str(record.get("symbol") or "").strip().upper()
            if not symbol:
                continue

            if not bool(record.get("swap", False)):
                continue

            quote = str(record.get("quote") or "").strip().upper()
            normalized = str(record.get("coin") or "").strip().upper()
            if not normalized:
                normalized = compute_coin_name(symbol, quote)
            if normalized and normalized not in coin_map:
                coin_map[normalized] = symbol
    
    # Cache the mapping
    if use_cache:
        _COIN_TO_SYMBOL_CACHE[exchange_key] = coin_map
        _COIN_TO_SYMBOL_CACHE_SIG[exchange_key] = mapping_sig
    
    # Return symbol or fallback
    if coin_key in coin_map:
        return coin_map[coin_key]
    else:
        # Fallback: guess quote currency
        quote = "USDC" if "hyperliquid" in exchange_key else "USDT"
        # Special handling for Hyperliquid k-prefix coins
        if "hyperliquid" in exchange_key and coin_key in _HYPERLIQUID_K_PREFIX_COINS:
            return f"K{coin_key}{quote}"
        return f"{coin_key}{quote}"


class CoinData:
    def __init__(self, defer_config: bool = False, cmc_pool: CmcPoolClient | None = None):
        pbgdir = Path.cwd()
        self.cmc_pool = cmc_pool or build_cmc_pool_client(pbgdir)
        self.piddir = Path(f'{pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbcoindata.pid')
        self.my_pid = None
        self._runtime_config = CoinDataRuntimeConfig()
        self._config_generation = None
        self._config_load_failed = False
        self._ini_watcher = IniWatcher(ini_path=pbgui_purefunc.pbgui_ini_path())
        self.api_error = None
        self._fetch_limit = 5000
        self._fetch_interval = 24
        self._metadata_interval = 1
        self._mapping_interval = 24
        self.ini_ts = 0
        self._cleanup_legacy_exchange_ini_entries()
        if not defer_config:
            self.load_config()
        self.data = None
        self.metadata = None
        self.data_ts = 0
        self.metadata_ts = 0
        self._exchange = V7.list()[0]
        self.exchanges = V7.list()
        self.exchange_index = self.exchanges.index(self.exchange)
        self.update_symbols_ts = 0
        self.update_mappings_ts = 0
        self._symbols = []
        self._symbols_cpt = []
        self._symbols_all = []
        self._symbols_notice = []
        self._symbols_notices = {}
        self._symbols_data = []
        self.approved_coins = []
        self.ignored_coins = []
        self._all_tags = []
        self._tags = []
        self._symbol_mappings = {}
        # HIP-3: Exchange-specific data caches
        self._ccxt_markets = {}  # {exchange: markets_dict}
        self._exchange_mappings = {}  # {exchange: [mapping_records]}
        self._exchange_mapping_ts = {}  # {exchange: (mtime_ns, size)}
        self._copy_trading_cache = {}  # {exchange: [symbol_ids]}
        self._mapping_self_heal_state = {}  # {exchange: {fails:int, next_retry_ts:float}}
        self._last_build_mapping_stats = {}  # {exchange: {unmatched_* counters}}
        self._last_price_update_results = {}  # {exchange: {requested/priced/recovered/missing/failed}}
        self._tradfi_symbol_map: list = []
        self._tradfi_symbol_map_ts: tuple | None = None
        self._cmc_metrics = {
            "listings_ok": 0,
            "listings_fail": 0,
            "metadata_ok": 0,
            "metadata_fail": 0,
            "status_ok": 0,
            "status_fail": 0,
        }
        self._cmc_metrics_last_log_ts = 0.0
        self._cmc_metrics_log_interval_s = 0
        self._sync_cmc_metrics_log_interval()
        self.load_symbols()
        self._market_cap = 0
        self._vol_mcap = 10.0
        self._only_cpt = False
        self._notices_ignore = False
        self._logged_idle = False

    def _sync_cmc_metrics_log_interval(self):
        """Align metrics health-log cadence with data-fetch cadence."""
        try:
            fetch_hours = max(1, int(self._fetch_interval))
        except Exception:
            fetch_hours = 24
        self._cmc_metrics_log_interval_s = fetch_hours * 3600
    
    def _get_exchange_dir(self, exchange: str) -> Path:
        """Get coindata directory for a specific exchange."""
        pbgdir = Path.cwd()
        exchange_dir = pbgdir / "data" / "coindata" / exchange
        return exchange_dir
    
    def _ensure_exchange_dir(self, exchange: str) -> Path:
        """Ensure exchange directory exists and return path."""
        exchange_dir = self._get_exchange_dir(exchange)
        if not exchange_dir.exists():
            exchange_dir.mkdir(parents=True, exist_ok=True)
        return exchange_dir
    
    def load_ccxt_markets(self, exchange: str, use_cache: bool = True) -> dict:
        """Load CCXT markets from cache for a specific exchange."""
        if use_cache and exchange in self._ccxt_markets:
            return self._ccxt_markets[exchange]
        
        markets_file = self._get_exchange_dir(exchange) / "ccxt_markets.json"
        if not markets_file.exists():
            return {}
        
        try:
            markets = _read_json_with_retry(markets_file, retries=1, delay_s=0.2)
            if isinstance(markets, dict):
                self._ccxt_markets[exchange] = markets
                return markets
            _log(SERVICE, f'CCXT markets for {exchange} are not a dict, ignoring cache', level='WARNING')
        except Exception as e:
            _log(SERVICE, f'Error loading CCXT markets for {exchange}: {e}', level='ERROR')
            return {}
        return {}
    
    @_exchange_transaction
    def save_ccxt_markets(self, exchange: str, markets: dict):
        """Save CCXT markets to cache. Only writes on success."""
        if not markets:
            _log(SERVICE, f'Empty markets data for {exchange}, not saving', level='WARNING')
            return
        
        exchange_dir = self._ensure_exchange_dir(exchange)
        markets_file = exchange_dir / "ccxt_markets.json"
        
        try:
            _atomic_json_write(markets_file, markets)
            self._ccxt_markets[exchange] = markets
            _log(SERVICE, f'Saved CCXT markets for {exchange}', level='DEBUG')
        except Exception as e:
            _log(SERVICE, f'Error saving CCXT markets for {exchange}: {e}', level='ERROR')
            raise CoinDataPersistenceError(f'Failed to save CCXT markets for {exchange}') from e
    
    def load_mapping(self, exchange: str, use_cache: bool = True) -> list:
        """Load mapping.json for an exchange with optional mtime-aware caching."""
        if not exchange:
            return []
        mapping_file = self._get_exchange_dir(exchange) / "mapping.json"
        if not mapping_file.exists():
            self._exchange_mapping_ts.pop(exchange, None)
            self._exchange_mappings.pop(exchange, None)
            return []

        stat = mapping_file.stat()
        file_sig = (stat.st_mtime_ns, stat.st_size)
        if use_cache and exchange in self._exchange_mappings and self._exchange_mapping_ts.get(exchange) == file_sig:
            return self._exchange_mappings[exchange]

        try:
            mapping = _read_json_with_retry(mapping_file, retries=1, delay_s=0.2)
            if isinstance(mapping, list):
                mapping = disambiguate_multiplier_market_coins(mapping)
                self._exchange_mappings[exchange] = mapping
                self._exchange_mapping_ts[exchange] = file_sig
                return mapping
            _log(SERVICE, f'Mapping for {exchange} is not a list, ignoring cache file', level='WARNING')
            return []
        except Exception as e:
            _log(SERVICE, f'Error loading mapping for {exchange}: {e}', level='ERROR')
            return []

    def load_exchange_mapping(self, exchange: str) -> list:
        """Backward-compatible wrapper for load_mapping()."""
        return self.load_mapping(exchange=exchange, use_cache=True)
    
    @_exchange_transaction
    def save_exchange_mapping(self, exchange: str, mapping: list):
        """Save exchange mapping to cache. Only writes on success."""
        if not mapping:
            _log(SERVICE, f'Empty mapping data for {exchange}, not saving', level='WARNING')
            return
        
        exchange_dir = self._ensure_exchange_dir(exchange)
        mapping_file = exchange_dir / "mapping.json"
        
        try:
            _atomic_json_write(mapping_file, mapping)
            self._exchange_mappings[exchange] = mapping
            stat = mapping_file.stat()
            self._exchange_mapping_ts[exchange] = (stat.st_mtime_ns, stat.st_size)
            _log(SERVICE, f'Saved mapping for {exchange}', level='DEBUG')
        except Exception as e:
            _log(SERVICE, f'Error saving mapping for {exchange}: {e}', level='ERROR')
            raise CoinDataPersistenceError(f'Failed to save mapping for {exchange}') from e

    # ------------------------------------------------------------------
    # TradFi symbol map (Hyperliquid XYZ stock-perps)
    # ------------------------------------------------------------------

    def _tradfi_symbol_map_path(self) -> Path:
        """Path to tradfi_symbol_map.json for Hyperliquid."""
        return self._get_exchange_dir("hyperliquid") / "tradfi_symbol_map.json"

    def load_tradfi_symbol_map(self, use_cache: bool = True) -> list:
        """Load tradfi_symbol_map.json with optional mtime-aware caching."""
        path = self._tradfi_symbol_map_path()
        if not path.exists():
            self._tradfi_symbol_map_ts = None
            self._tradfi_symbol_map = []
            return []

        stat = path.stat()
        file_sig = (stat.st_mtime_ns, stat.st_size)
        if use_cache and self._tradfi_symbol_map_ts == file_sig and self._tradfi_symbol_map is not None:
            return self._tradfi_symbol_map

        try:
            data = _read_json_with_retry(path, retries=1, delay_s=0.2)
            if isinstance(data, list):
                self._tradfi_symbol_map = data
                self._tradfi_symbol_map_ts = file_sig
                return data
            _log(SERVICE, 'tradfi_symbol_map.json is not a list, ignoring', level='WARNING')
            return []
        except Exception as e:
            _log(SERVICE, f'Error loading tradfi_symbol_map.json: {e}', level='ERROR')
            return []

    def save_tradfi_symbol_map(self, records: list):
        """Save tradfi_symbol_map.json atomically. Preserves existing file on failure."""
        if records is None:
            _log(SERVICE, 'tradfi_symbol_map: nothing to save (None)', level='WARNING')
            return

        path = self._tradfi_symbol_map_path()
        with advisory_file_lock(_exchange_lock_target("hyperliquid")):
            try:
                _atomic_json_write(path, records)
                self._tradfi_symbol_map = records
                stat = path.stat()
                self._tradfi_symbol_map_ts = (stat.st_mtime_ns, stat.st_size)
                _log(SERVICE, f'Saved tradfi_symbol_map.json ({len(records)} entries)', level='DEBUG')
            except Exception as e:
                _log(SERVICE, f'Error saving tradfi_symbol_map.json: {e}', level='ERROR')
                raise CoinDataPersistenceError('Failed to save tradfi_symbol_map.json') from e

    def get_tradfi_map_entry(self, xyz_coin: str) -> dict | None:
        """Return the tradfi_symbol_map entry for an xyz_coin (case-insensitive), or None."""
        key = str(xyz_coin or '').strip().upper()
        if not key:
            return None
        records = self.load_tradfi_symbol_map(use_cache=True)
        for r in records:
            if str(r.get('xyz_coin') or '').upper() == key:
                return r
        return None

    def get_mapping_symbols(self, exchange: str, quote_filter: list[str] | None = None, use_cache: bool = True) -> list[str]:
        """Return symbol strings from mapping.json for an exchange."""
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        symbols = []
        for record in mapping:
            quote = (record.get("quote") or "").upper()
            if quote_filter and quote not in {q.upper() for q in quote_filter}:
                continue
            symbol = record.get("symbol")
            if symbol:
                symbols.append(symbol)
        return sorted(set(symbols))

    def get_mapping_coins(self, exchange: str, quote_filter: list[str] | None = None, use_cache: bool = True) -> list[str]:
        """Return normalized coin names computed from mapping symbols."""
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        coins = []
        for record in mapping:
            quote = (record.get("quote") or "").upper()
            if quote_filter and quote not in {q.upper() for q in quote_filter}:
                continue
            coin = (record.get("coin") or "").upper()
            if not coin:
                symbol = record.get("symbol") or ""
                coin = compute_coin_name(symbol, quote)
            if coin:
                coins.append(coin.upper())
        return sorted(set(coins))

    def get_cpt_coins(self, exchange: str, quote_filter: list[str] | None = None, use_cache: bool = True) -> list[str]:
        """Return normalized coin names where mapping marks copy_trading=True."""
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        coins = []
        for record in mapping:
            if not record.get("copy_trading", False):
                continue
            quote = (record.get("quote") or "").upper()
            if quote_filter and quote not in {q.upper() for q in quote_filter}:
                continue
            coin = (record.get("coin") or "").upper()
            if not coin:
                symbol = record.get("symbol") or ""
                coin = compute_coin_name(symbol, quote)
            if coin:
                coins.append(coin.upper())
        return sorted(set(coins))

    def _to_float(self, value):
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _mapping_cmc_metrics(self, record: dict) -> tuple[float | None, float | None, float | None]:
        """Return nullable CMC metrics, including compatibility for old unmatched caches."""
        market_cap = self._to_float(record.get("market_cap"))
        volume_24h = self._to_float(record.get("volume_24h"))
        if market_cap is not None and not math.isfinite(market_cap):
            market_cap = None
        if volume_24h is not None and not math.isfinite(volume_24h):
            volume_24h = None
        if "cmc_id" in record and record.get("cmc_id") is None:
            market_cap = None
            volume_24h = None
        vol_mcap = (
            volume_24h / market_cap
            if market_cap is not None and market_cap > 0 and volume_24h is not None
            else None
        )
        return market_cap, volume_24h, vol_mcap

    def _passes_active_filter(self, exchange: str, record: dict) -> bool:
        if not bool(record.get("active", True)):
            return False
        if not bool(record.get("swap", False)):
            return False
        if not bool(record.get("linear", True)):
            return False

        if exchange == "hyperliquid":
            if bool(record.get("is_hip3", False)):
                dex = str(record.get("dex") or "").strip().lower()
                if dex != "xyz":
                    return False
            open_interest = self._to_float(record.get("open_interest"))
            if open_interest is not None and open_interest <= 0.0:
                return False

        if exchange == "kucoin" and str(record.get("quote") or "").upper() != "USDT":
            return False

        return True

    def filter_mapping(
        self,
        exchange: str,
        market_cap_min_m: int | float | None = None,
        vol_mcap_max: float | None = None,
        only_cpt: bool | None = None,
        notices_ignore: bool | None = None,
        tags: list[str] | None = None,
        active_only: bool | None = None,
        quote_filter: list[str] | None = None,
        use_cache: bool = True,
    ) -> tuple[list[str], list[str]]:
        """Filter mapping records and return (approved_coins, ignored_coins).

        Args mirror existing CoinData filter knobs:
        - market_cap_min_m: minimum market cap in millions of USD (defaults to self.market_cap)
        - vol_mcap_max: maximum volume/market_cap ratio (defaults to self.vol_mcap)
        - only_cpt: include only copy-trading symbols (defaults to self.only_cpt)
        - notices_ignore: exclude records with a notice (defaults to self.notices_ignore)
        - tags: any-tag match; empty means no tag filter (defaults to self.tags)
        - active_only: apply passivbot market eligibility (active/swap/linear and
            exchange-specific checks; defaults to False)
        - quote_filter: optional quote whitelist (e.g. ["USDT"])
        """
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        market_cap_min_m = self.market_cap if market_cap_min_m is None else market_cap_min_m
        vol_mcap_max = self.vol_mcap if vol_mcap_max is None else vol_mcap_max
        only_cpt = self.only_cpt if only_cpt is None else only_cpt
        notices_ignore = self.notices_ignore if notices_ignore is None else notices_ignore
        tags = self.tags if tags is None else tags
        active_only = False if active_only is None else active_only
        quote_whitelist = {q.upper() for q in quote_filter} if quote_filter else None
        market_cap_min = float(market_cap_min_m) * 1_000_000
        vol_mcap_limit = float(vol_mcap_max)
        market_cap_filter_active = market_cap_min > 0
        vol_mcap_filter_active = math.isfinite(vol_mcap_limit) and vol_mcap_limit < 10.0

        approved = set()
        ignored = set()

        for record in mapping:
            quote = (record.get("quote") or "").upper()
            if quote_whitelist and quote not in quote_whitelist:
                continue

            coin = (record.get("coin") or "").upper()
            if not coin:
                symbol = record.get("symbol") or ""
                coin = compute_coin_name(symbol, quote)
            if not coin:
                continue
            coin = coin.upper()

            market_cap, _volume_24h, vol_mcap = self._mapping_cmc_metrics(record)
            has_notice = bool(record.get("notice"))
            is_cpt = bool(record.get("copy_trading", False))
            record_tags = record.get("tags") or []
            is_eligible = self._passes_active_filter(exchange, record)
            if active_only and not is_eligible:
                continue

            passes = (
                (not active_only or is_eligible)
                and (
                    not market_cap_filter_active
                    or (market_cap is not None and market_cap >= market_cap_min)
                )
                and (
                    not vol_mcap_filter_active
                    or (vol_mcap is not None and vol_mcap < vol_mcap_limit)
                )
                and (not only_cpt or is_cpt)
                and (not notices_ignore or not has_notice)
                and (not tags or any(tag in record_tags for tag in tags))
            )

            if passes:
                approved.add(coin)
            else:
                ignored.add(coin)

        ignored -= approved
        return sorted(approved), sorted(ignored)

    def get_mapping_tags(
        self,
        exchange: str,
        quote_filter: list[str] | None = None,
        use_cache: bool = True,
    ) -> list[str]:
        """Return sorted unique tags from mapping records for an exchange."""
        if not exchange:
            return []
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        quote_whitelist = {q.upper() for q in quote_filter} if quote_filter else None

        tags = set()
        for record in mapping:
            quote = (record.get("quote") or "").upper()
            if quote_whitelist and quote not in quote_whitelist:
                continue
            for tag in (record.get("tags") or []):
                if tag:
                    tags.add(tag)
        return sorted(tags)

    def filter_mapping_rows(
        self,
        exchange: str,
        market_cap_min_m: int | float | None = None,
        vol_mcap_max: float | None = None,
        only_cpt: bool | None = None,
        notices_ignore: bool | None = None,
        tags: list[str] | None = None,
        active_only: bool | None = None,
        quote_filter: list[str] | None = None,
        use_cache: bool = True,
    ) -> list[dict]:
        """Filter mapping and return row dicts for table display.

        Uses the same pass/fail logic as filter_mapping(), but returns records
        (one per mapping row) enriched with derived display fields.
        """
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        market_cap_min_m = self.market_cap if market_cap_min_m is None else market_cap_min_m
        vol_mcap_max = self.vol_mcap if vol_mcap_max is None else vol_mcap_max
        only_cpt = self.only_cpt if only_cpt is None else only_cpt
        notices_ignore = self.notices_ignore if notices_ignore is None else notices_ignore
        tags = self.tags if tags is None else tags
        active_only = False if active_only is None else active_only
        quote_whitelist = {q.upper() for q in quote_filter} if quote_filter else None
        market_cap_min = float(market_cap_min_m) * 1_000_000
        vol_mcap_limit = float(vol_mcap_max)
        market_cap_filter_active = market_cap_min > 0
        vol_mcap_filter_active = math.isfinite(vol_mcap_limit) and vol_mcap_limit < 10.0

        filtered_rows = []
        for record in mapping:
            quote = (record.get("quote") or "").upper()
            if quote_whitelist and quote not in quote_whitelist:
                continue

            coin = (record.get("coin") or "").upper()
            if not coin:
                symbol = record.get("symbol") or ""
                coin = compute_coin_name(symbol, quote)
            if not coin:
                continue

            market_cap, volume_24h, vol_mcap = self._mapping_cmc_metrics(record)
            has_notice = bool(record.get("notice"))
            is_cpt = bool(record.get("copy_trading", False))
            record_tags = record.get("tags") or []
            is_eligible = self._passes_active_filter(exchange, record)

            passes = (
                (not active_only or is_eligible)
                and (
                    not market_cap_filter_active
                    or (market_cap is not None and market_cap >= market_cap_min)
                )
                and (
                    not vol_mcap_filter_active
                    or (vol_mcap is not None and vol_mcap < vol_mcap_limit)
                )
                and (not only_cpt or is_cpt)
                and (not notices_ignore or not has_notice)
                and (not tags or any(tag in record_tags for tag in tags))
            )

            if not passes:
                continue

            row = dict(record)
            row["coin"] = coin.upper()
            row["vol/mcap"] = vol_mcap
            row["vol_mcap"] = vol_mcap
            row["market_cap"] = market_cap
            row["volume_24h"] = volume_24h
            row["price"] = row.get("price_last")
            filtered_rows.append(row)

        filtered_rows.sort(
            key=lambda row: (
                row.get("market_cap") is None,
                -float(row.get("market_cap") or 0),
                str(row.get("coin") or "").strip().upper(),
                str(row.get("symbol") or "").strip().upper(),
                str(row.get("ccxt_symbol") or "").strip().upper(),
            )
        )
        return filtered_rows

    def filter_by_market_cap_mapping(
        self,
        exchange: str,
        mc: int,
        active_only: bool | None = None,
        quote_filter: list[str] | None = None,
        use_cache: bool = True,
    ) -> tuple[list[str], list[str]]:
        """Return (approved, ignored) using only an absolute market-cap threshold in USD."""
        mapping = self.load_mapping(exchange=exchange, use_cache=use_cache)
        active_only = False if active_only is None else active_only
        quote_whitelist = {q.upper() for q in quote_filter} if quote_filter else None

        approved = set()
        ignored = set()
        for record in mapping:
            quote = (record.get("quote") or "").upper()
            if quote_whitelist and quote not in quote_whitelist:
                continue
            if active_only and not self._passes_active_filter(exchange, record):
                continue
            coin = (record.get("coin") or "").upper()
            if not coin:
                symbol = record.get("symbol") or ""
                coin = compute_coin_name(symbol, quote)
            if not coin:
                continue
            coin = coin.upper()
            market_cap, _volume_24h, _vol_mcap = self._mapping_cmc_metrics(record)
            threshold = float(mc)
            if threshold <= 0 or (market_cap is not None and market_cap > threshold):
                approved.add(coin)
            else:
                ignored.add(coin)

        ignored -= approved
        return sorted(approved), sorted(ignored)
    
    def _load_copy_trading_symbols_result(
        self,
        exchange: str,
        use_cache: bool = True,
    ) -> tuple[list, bool]:
        """Load copy-trading symbols and report whether an authoritative cache exists."""
        if use_cache and exchange in self._copy_trading_cache:
            return self._copy_trading_cache[exchange], True
        
        cpt_file = self._get_exchange_dir(exchange) / "copy_trading.json"
        if not cpt_file.exists():
            return [], False
        
        try:
            symbols = _read_json_with_retry(cpt_file, retries=1, delay_s=0.2)
            if isinstance(symbols, list):
                self._copy_trading_cache[exchange] = symbols
                return symbols, True
            _log(SERVICE, f'Copy trading cache for {exchange} is not a list, ignoring cache file', level='WARNING')
            return [], False
        except Exception as e:
            _log(SERVICE, f'Error loading copy trading symbols for {exchange}: {e}', level='ERROR')
            return [], False

    def load_copy_trading_symbols(self, exchange: str, use_cache: bool = True) -> list:
        """Load cached copy trading symbols for an exchange."""
        symbols, _available = self._load_copy_trading_symbols_result(exchange, use_cache=use_cache)
        return symbols
    
    @_exchange_transaction
    def save_copy_trading_symbols(self, exchange: str, symbols: list):
        """Save copy trading symbols to cache."""
        exchange_dir = self._ensure_exchange_dir(exchange)
        cpt_file = exchange_dir / "copy_trading.json"
        
        sorted_symbols = sorted(symbols)
        try:
            _atomic_json_write(cpt_file, sorted_symbols)
            self._copy_trading_cache[exchange] = sorted_symbols
            _log(SERVICE, f'Saved {len(symbols)} copy trading symbols for {exchange}', level='DEBUG')
        except Exception as e:
            _log(SERVICE, f'Error saving copy trading symbols for {exchange}: {e}', level='ERROR')
            raise CoinDataPersistenceError(f'Failed to save copy trading symbols for {exchange}') from e
    
    @_exchange_transaction
    def fetch_copy_trading_symbols(self, exchange_id: str, markets: dict = None) -> list:
        """Fetch copy trading symbols for an exchange.
        
        Sources per exchange:
        - bybit: CCXT market data (info.copyTrading == "both"), no auth needed
        - binance: sapi copy trading endpoint (requires authenticated user)
        - bitget: copy trading endpoint (requires authenticated user)
        - others: no known copy trading API
        
        For binance/bitget: remembers working user in pbgui.ini and tries
        that user first on subsequent runs. Falls back to scanning all users
        if the remembered user no longer works.
        
        Args:
            exchange_id: Exchange identifier
            markets: Pre-loaded CCXT markets dict (used for bybit to avoid re-fetch)
        
        Returns:
            List of market IDs (exchange format, e.g. "BTCUSDT")
        """
        cpt_symbols = None
        
        try:
            if exchange_id == 'bybit':
                # bybit: copy trading info is in CCXT market data
                if markets is None:
                    markets = self.load_ccxt_markets(exchange_id, use_cache=False)
                if not markets:
                    _log(SERVICE, f'No markets available for bybit copy trading detection', level='WARNING')
                    cpt_symbols = None
                else:
                    cpt_symbols = []
                
                for symbol, market in (markets or {}).items():
                    if not market.get("swap", False) or not market.get("active", True):
                        continue
                    if not market.get("linear", False):
                        continue
                    info = market.get("info", {})
                    if info.get("copyTrading") == "both":
                        market_id = market.get("id", "")
                        if market_id:
                            cpt_symbols.append(market_id)
                
                if cpt_symbols is not None:
                    _log(SERVICE, f'Found {len(cpt_symbols)} copy trading symbols for bybit (from market data)', level='INFO')
            
            elif exchange_id in ('binance', 'bitget'):
                cpt_symbols = self._fetch_cpt_with_user_discovery(exchange_id)
            
            else:
                # No copy trading API known for this exchange
                _log(SERVICE, f'No copy trading API for {exchange_id}', level='DEBUG')
                return []
            
        except Exception as e:
            _log(SERVICE, f'Error fetching copy trading symbols for {exchange_id}: {e}', level='ERROR')
            cpt_symbols = None

        if cpt_symbols is not None:
            self.save_copy_trading_symbols(exchange_id, cpt_symbols)
            return cpt_symbols

        cached_symbols, cache_available = self._load_copy_trading_symbols_result(
            exchange_id,
            use_cache=False,
        )
        if cache_available:
            _log(SERVICE, f'Using {len(cached_symbols)} cached copy trading symbols for {exchange_id}', level='INFO')
        return cached_symbols
    
    def _fetch_cpt_with_user_discovery(self, exchange_id: str) -> list | None:
        """Fetch copy trading symbols for binance/bitget with smart user caching.
        
        1. Try remembered user from pbgui.ini first
        2. If that fails, scan all eligible users
        3. Remember the working user for next time
        
        Returns:
            List of market IDs or empty list
        """
        from Exchange import Exchange
        from User import Users
        
        # Load remembered user from pbgui.ini
        remembered_user_name = self._load_cpt_user(exchange_id)
        
        # Get all eligible users for this exchange
        users_obj = Users()
        candidate_users = self._get_cpt_candidate_users(users_obj, exchange_id)
        
        if not candidate_users:
            _log(SERVICE, f'No eligible users found for {exchange_id} copy trading', level='WARNING')
            return None
        
        # Build ordered list: remembered user first, then others
        ordered_users = []
        if remembered_user_name:
            for u in candidate_users:
                if u.name == remembered_user_name:
                    ordered_users.append(u)
                    break
        for u in candidate_users:
            if u.name != remembered_user_name:
                ordered_users.append(u)
        
        # Try each user until one works
        for user in ordered_users:
            result = self._try_fetch_cpt_for_user(exchange_id, user)
            if result is not None:
                # Success - remember this user
                if user.name != remembered_user_name:
                    self._save_cpt_user(exchange_id, user.name)
                    _log(SERVICE, f'Remembered {user.name} as copy trading user for {exchange_id}', level='INFO')
                else:
                    _log(SERVICE, f'Using remembered user {user.name} for {exchange_id} copy trading', level='DEBUG')
                _log(SERVICE, f'Fetched {len(result)} copy trading symbols for {exchange_id} via user {user.name}', level='INFO')
                return result
        
        _log(SERVICE, f'No user could fetch copy trading symbols for {exchange_id}', level='WARNING')
        return None
    
    def _get_cpt_candidate_users(self, users_obj, exchange_id: str) -> list:
        """Get users with valid credentials for an exchange."""
        if exchange_id == 'binance':
            users = users_obj.find_binance_users()
            return users if users else []
        elif exchange_id == 'bitget':
            users = users_obj.find_bitget_users()
            return users if users else []
        return []
    
    def _try_fetch_cpt_for_user(self, exchange_id: str, user) -> list | None:
        """Try to fetch copy trading symbols with a specific user.
        
        Returns:
            List of symbols on success, None on failure
        """
        from Exchange import Exchange
        
        try:
            exchange = Exchange(exchange_id, user)
            exchange.connect()
            
            if exchange_id == 'binance':
                symbols = exchange.instance.sapiGetCopytradingFuturesLeadsymbol()
                if symbols and isinstance(symbols.get("data"), list):
                    return [s["symbol"] for s in symbols["data"]]
                return None
            
            elif exchange_id == 'bitget':
                symbols = exchange.instance.privateCopyGetV2CopyMixTraderConfigQuerySymbols(
                    {"productType": "USDT-FUTURES"}
                )
                if symbols and isinstance(symbols.get("data"), list):
                    return [s["symbol"] for s in symbols["data"]]
                return None
            
        except Exception as e:
            _log(SERVICE, f'User {user.name} failed for {exchange_id} copy trading: {e}', level='DEBUG')
            return None
    
    def _load_cpt_user(self, exchange_id: str) -> str | None:
        """Load remembered copy trading user from pbgui.ini."""
        key = f'cpt_user.{exchange_id}'
        value = pbgui_purefunc.load_ini("coinmarketcap", key)
        return value or None
    
    def _save_cpt_user(self, exchange_id: str, user_name: str):
        """Save working copy trading user to pbgui.ini."""
        save_ini("coinmarketcap", f'cpt_user.{exchange_id}', user_name)
    
    def cmc_pool_status(self) -> dict:
        """Return readiness and pool diagnostics without credential secrets."""
        try:
            status = self.cmc_pool.status()
        except Exception as exc:
            _log(SERVICE, f"CMC pool status unavailable: {exc.__class__.__name__}", level="WARNING")
            return {
                "ready": False,
                "active_credentials": 0,
                "keys": [],
                "error": "CMC pool status unavailable",
            }
        safe_status = dict(status) if isinstance(status, dict) else {}
        safe_status["ready"] = int(safe_status.get("active_credentials") or 0) > 0
        return safe_status

    @property
    def cmc_pool_ready(self) -> bool:
        """Return whether at least one active local pool credential exists."""
        return bool(self.cmc_pool_status().get("ready"))

    def _has_cmc_api_key(self) -> bool:
        """Retain the compatibility name while deriving readiness only from the pool."""
        return self.cmc_pool_ready
    
    @property
    def fetch_limit(self):
        return self._fetch_limit
    @fetch_limit.setter
    def fetch_limit(self, new_fetch_limit):
        self._fetch_limit = new_fetch_limit
    
    @property
    def fetch_interval(self):
        return self._fetch_interval
    @fetch_interval.setter
    def fetch_interval(self, new_fetch_interval):
        self._fetch_interval = new_fetch_interval
        self._sync_cmc_metrics_log_interval()

    @property
    def metadata_interval(self):
        return self._metadata_interval
    @metadata_interval.setter
    def metadata_interval(self, new_metadata_interval):
        self._metadata_interval = new_metadata_interval

    @property
    def mapping_interval(self):
        return self._mapping_interval
    @mapping_interval.setter
    def mapping_interval(self, new_mapping_interval):
        self._mapping_interval = new_mapping_interval

    @property
    def exchange(self):
        return self._exchange
    @exchange.setter
    def exchange(self, new_exchange):
        self._exchange = new_exchange
        self.load_symbols()
        self.list_symbols()

    @property
    def symbols(self):
        if not self._symbols:
            self.load_symbols()
        return self._symbols

    @property
    def symbols_cpt(self):
        if not self._symbols_cpt:
            self.load_symbols()
        return self._symbols_cpt

    @property
    def symbols_all(self):
        if not self._symbols_all:
            self.load_symbols_all()
        return self._symbols_all

    @property
    def symbols_notice(self):
        if not self._symbols_notice:
            self.list_symbols()
        return self._symbols_notice

    @property
    def symbols_notices(self):
        if not self._symbols_notices:
            self.list_symbols()
        return self._symbols_notices

    @property
    def symbols_data(self):
        if not self._symbols_data:
            self.list_symbols()
        return self._symbols_data
    
    @property
    def market_cap(self):
        return self._market_cap
    @market_cap.setter
    def market_cap(self, new_market_cap):
        if self._market_cap != new_market_cap:
            self._market_cap = new_market_cap
            self.list_symbols()
    
    @property
    def vol_mcap(self):
        return self._vol_mcap
    @vol_mcap.setter
    def vol_mcap(self, new_vol_mcap):
        if self._vol_mcap != new_vol_mcap:
            self._vol_mcap = new_vol_mcap
            self.list_symbols()
    
    @property
    def only_cpt(self):
        return self._only_cpt
    @only_cpt.setter
    def only_cpt(self, new_only_cpt):
        if self._only_cpt != new_only_cpt:
            self._only_cpt = new_only_cpt
            self.list_symbols()

    @property
    def notices_ignore(self):
        return self._notices_ignore
    @notices_ignore.setter
    def notices_ignore(self, new_notices_ignore):
        if self._notices_ignore != new_notices_ignore:
            self._notices_ignore = new_notices_ignore
            self.list_symbols()

    @property
    def all_tags(self):
        if not self._all_tags:
            self.list_symbols()
        return self._all_tags

    @property
    def tags(self):
        return self._tags
    @tags.setter
    def tags(self, new_tags):
        if self._tags != new_tags:
            self._tags = new_tags
            self.list_symbols()
    

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBCoinData.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    _log(SERVICE, 'Can not start PBCoinData', level='ERROR')
                    break
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log(SERVICE, 'Stop: PBCoinData', level='INFO')
            try:
                psutil.Process(self.my_pid).kill()
            except psutil.NoSuchProcess:
                pass

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbcoindata.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read().strip()
                try:
                    self.my_pid = int(pid) if pid.isnumeric() else None
                except ValueError:
                    self.my_pid = None

    def save_pid(self):
        self.my_pid = os.getpid()
        tmp_path = self.pidfile.with_suffix(self.pidfile.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            f.write(str(self.my_pid))
        tmp_path.replace(self.pidfile)

    def has_new_config(self):
        """Return whether the current file generation has not been applied."""
        return self._ini_watcher._read_signature() != self._config_generation

    def _cleanup_legacy_exchange_ini_entries(self):
        """TEMP migration cleanup: remove legacy [exchanges] entries from pbgui.ini.

        REMOVE AFTER mapping migration is fully rolled out on all environments.
        """
        ini_path = self._ini_watcher._ini_path
        if not ini_path.exists():
            return

        try:
            def mutate(pb_config):
                if not pb_config.has_section("exchanges"):
                    return 0
                removed_count = len(pb_config.options("exchanges"))
                pb_config.remove_section("exchanges")
                return removed_count

            removed_count = update_ini(mutate)
            if removed_count:
                _log(SERVICE, f'Removed legacy [exchanges] section from pbgui.ini ({removed_count} entries)', level='INFO')
        except Exception as e:
            _log(SERVICE, f'Failed to remove legacy [exchanges] section from pbgui.ini: {e}', level='ERROR')
    
    def has_new_data(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/coindata.json').exists():
            data_ts = Path(f'{coin_path}/coindata.json').stat().st_mtime
            if data_ts > self.data_ts:
                return True
            else:
                return False
        return True

    def has_new_metadata(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/metadata.json').exists():
            metadata_ts = Path(f'{coin_path}/metadata.json').stat().st_mtime
            if metadata_ts > self.metadata_ts:
                return True
            else:
                return False
        return True

    @staticmethod
    def _build_config_candidate(snapshot: IniSnapshot) -> CoinDataRuntimeConfig:
        """Build one immutable candidate from exactly one INI snapshot."""
        defaults = CoinDataRuntimeConfig()

        def integer(key: str, minimum: int, maximum: int) -> int:
            if not snapshot.has_option("coinmarketcap", key):
                return getattr(defaults, key)
            try:
                value = int(snapshot.get("coinmarketcap", key))
            except (TypeError, ValueError) as exc:
                raise CoinDataConfigError(key) from exc
            if value < minimum or value > maximum:
                raise CoinDataConfigError(key)
            return value

        return CoinDataRuntimeConfig(
            fetch_limit=integer("fetch_limit", 200, 5000),
            fetch_interval=integer("fetch_interval", 1, 24),
            metadata_interval=integer("metadata_interval", 1, 7),
            mapping_interval=integer("mapping_interval", 1, 168),
        )

    def _apply_config_snapshot(self, snapshot: IniSnapshot) -> bool:
        """Validate and atomically publish one file generation."""
        if snapshot.signature == self._config_generation:
            return False
        candidate = self._build_config_candidate(snapshot)
        self._fetch_limit = candidate.fetch_limit
        self._fetch_interval = candidate.fetch_interval
        self._metadata_interval = candidate.metadata_interval
        self._mapping_interval = candidate.mapping_interval
        self._runtime_config = candidate
        self._sync_cmc_metrics_log_interval()
        self._config_generation = snapshot.signature
        self.ini_ts = snapshot.signature.mtime_ns or 0
        return True

    def load_config(self):
        """Compatibility reload using one snapshot and last-known-good state."""
        try:
            applied = self._apply_config_snapshot(load_ini_snapshot(self._ini_watcher._ini_path))
            self._config_load_failed = False
            return applied
        except Exception as exc:
            key = exc.key if isinstance(exc, CoinDataConfigError) else "snapshot"
            _log(SERVICE, f"Config reload rejected for [coinmarketcap] {key}; keeping last known good settings", level="WARNING")
            self._config_load_failed = True
            return False
    
    def save_config(self):
        values = {
            "fetch_limit": str(self.fetch_limit),
            "fetch_interval": str(self.fetch_interval),
            "metadata_interval": str(self.metadata_interval),
            "mapping_interval": str(self.mapping_interval),
        }

        def mutate(pb_config):
            if not pb_config.has_section("coinmarketcap"):
                pb_config.add_section("coinmarketcap")
            for key, value in values.items():
                pb_config.set("coinmarketcap", key, value)

        update_ini(mutate)
    
    @_exchange_transaction
    def fetch_ccxt_markets(self, exchange_id: str):
        """Fetch CCXT markets for a specific exchange and save to coindata/{exchange}/ccxt_markets.json"""
        from Exchange import Exchange
        
        _log(SERVICE, f'Fetching CCXT markets for {exchange_id}', level='INFO')
        
        try:
            exchange = Exchange(exchange_id)
            exchange.connect()
            markets = exchange.instance.load_markets()
        except Exception as e:
            _log(SERVICE, f'Error fetching CCXT markets for {exchange_id}: {e}', level='ERROR')
            return False

        if not markets:
            _log(SERVICE, f'No markets returned for {exchange_id}', level='WARNING')
            return False

        self.save_ccxt_markets(exchange_id, markets)
        _log(SERVICE, f'Successfully fetched {len(markets)} markets for {exchange_id}', level='INFO')
        return True
    
    @_exchange_transaction
    def build_mapping(self, exchange_id: str, force_fetch: bool = False):
        """Build mapping.json for an exchange by merging CCXT markets + CMC data"""
        _log(SERVICE, f'Building mapping for {exchange_id}', level='INFO')
        
        try:
            from Exchange import Exchange

            # Ensure CMC datasets are available for market-cap/tag enrichment
            if not self.data:
                self.load_data()
            if not self.metadata:
                self.load_metadata()
            
            # Load or fetch CCXT markets
            markets = self.load_ccxt_markets(exchange_id, use_cache=False)
            if not markets or force_fetch:
                success = self.fetch_ccxt_markets(exchange_id)
                if not success:
                    _log(SERVICE, f'Failed to fetch markets for {exchange_id}', level='ERROR')
                    return False
                markets = self.load_ccxt_markets(exchange_id, use_cache=False)
            
            if not markets:
                _log(SERVICE, f'No markets available for {exchange_id}', level='ERROR')
                return False
            
            # Build CMC lookup dicts from self.data
            # cmc_best: best-rank entry per symbol (fallback)
            # cmc_all: ALL entries per symbol (for price-based disambiguation)
            cmc_best = {}
            cmc_all = {}
            cmc_name_index = {}
            cmc_slug_index = {}
            metadata_by_id = {}
            if self.data and "data" in self.data:
                for coin in self.data["data"]:
                    sym = coin["symbol"].upper()
                    # Collect all entries for each symbol
                    if sym not in cmc_all:
                        cmc_all[sym] = []
                    cmc_all[sym].append(coin)
                    # Track best-rank entry as fallback
                    if sym in cmc_best:
                        existing_rank = cmc_best[sym].get("cmc_rank", 99999) or 99999
                        new_rank = coin.get("cmc_rank", 99999) or 99999
                        if new_rank < existing_rank:
                            cmc_best[sym] = coin
                    else:
                        cmc_best[sym] = coin

                    name_key = self._normalize_cmc_lookup_text(coin.get("name", ""))
                    if name_key:
                        cmc_name_index.setdefault(name_key, []).append(coin)

                    slug_key = self._normalize_cmc_lookup_text(coin.get("slug", ""))
                    if slug_key:
                        cmc_slug_index.setdefault(slug_key, []).append(coin)
                dupes = sum(1 for v in cmc_all.values() if len(v) > 1)
                _log(SERVICE, f'CMC data available: {len(cmc_best)} coins ({dupes} with duplicates)', level='DEBUG')
            else:
                _log(SERVICE, 'No CMC data loaded, using defaults', level='WARNING')

            cmc_best_values = list(cmc_best.values())

            if self.metadata and isinstance(self.metadata, dict):
                raw_md = self.metadata.get("data", {})
                if isinstance(raw_md, dict):
                    metadata_by_id = raw_md
            
            # Load previous mapping for price-based CMC disambiguation
            # When multiple CMC entries share the same symbol (e.g. HOT, ACT, BABY),
            # we use the exchange price from the previous mapping to pick the correct one
            prev_prices = {}
            prev_mapping = self.load_mapping(exchange=exchange_id, use_cache=False)
            if prev_mapping:
                for rec in prev_mapping:
                    if rec.get("price_last") and rec["price_last"] > 0:
                        prev_prices[rec["symbol"]] = rec["price_last"]
            
            # If no previous prices available, fetch live prices for disambiguation
            if not prev_prices:
                try:
                    exchange = Exchange(exchange_id)
                    exchange.connect()
                    ccxt_symbols = [s for s, m in markets.items() if m.get("swap")]
                    ticker_data = {}
                    linear_symbols = [s for s in ccxt_symbols if markets.get(s, {}).get("linear", True)]
                    inverse_symbols = [s for s in ccxt_symbols if not markets.get(s, {}).get("linear", True)]
                    if linear_symbols and inverse_symbols:
                        try:
                            ticker_data.update(exchange.fetch_prices(linear_symbols, "swap"))
                        except Exception as e:
                            _log(SERVICE, f'Could not fetch linear prices for {exchange_id} disambiguation: {e}', level='WARNING')
                        try:
                            ticker_data.update(exchange.fetch_prices(inverse_symbols, "swap"))
                        except Exception as e:
                            _log(SERVICE, f'Could not fetch inverse prices for {exchange_id} disambiguation: {e}', level='WARNING')
                    else:
                        ticker_data = exchange.fetch_prices(ccxt_symbols, "swap")
                    for ccxt_sym, data in ticker_data.items():
                        if ccxt_sym in markets:
                            market_id = markets[ccxt_sym].get("id", "")
                            try:
                                price = float(data.get("last", 0) or 0)
                            except Exception:
                                price = 0.0
                            if market_id and price > 0:
                                prev_prices[market_id] = price
                    _log(SERVICE, f'Fetched {len(prev_prices)} live prices for {exchange_id} CMC disambiguation', level='INFO')
                except Exception as e:
                    _log(SERVICE, f'Could not fetch live prices for {exchange_id}: {e}', level='WARNING')
            
            # Load copy trading symbols (from cache, populated by update_mappings)
            cpt_symbols, cpt_cache_available = self._load_copy_trading_symbols_result(
                exchange_id,
                use_cache=False,
            )
            cpt_symbols_set = set(cpt_symbols)

            # Resilience for authenticated CPT sources (binance/bitget):
            # if CPT symbols are unavailable, preserve previous mapping flags
            # to avoid wiping all copy_trading=True records on rebuild.
            previous_cpt_symbols = set()
            if exchange_id in ("binance", "bitget") and not cpt_cache_available and prev_mapping:
                previous_cpt_symbols = {
                    rec.get("symbol")
                    for rec in prev_mapping
                    if rec.get("symbol") and rec.get("copy_trading")
                }
                if previous_cpt_symbols:
                    _log(SERVICE,
                        f'Using {len(previous_cpt_symbols)} copy trading symbols from previous mapping for {exchange_id} (no fresh CPT data available)',
                        level='WARNING'
                    )

            # Legacy ini CPT fallback removed:
            # [exchanges] migration is handled at startup and mapping/cache paths are now authoritative.
            
            # Build mapping records
            mapping = []
            price_disambiguated = 0
            unmatched_cmc_all = []
            unmatched_cmc_relevant = []
            for symbol, market in markets.items():
                # Only process swap/perpetual markets (NOT spot!)
                if not market.get("swap", False):
                    continue
                
                # Get CCXT market ID (exchange format)
                market_id = market.get("id", "")
                if not market_id:
                    continue
                
                # Extract base coin (e.g., "BTC" from "BTC/USDT:USDT")
                base = market.get("base", "")
                
                # Detect non-crypto perpetuals before CMC enrichment.
                is_hip3 = self._detect_hip3(exchange_id, market, cmc_best)
                is_tradfi = _is_tradfi_market(exchange_id, market)
                
                # Find CMC data for this coin
                # Use market_id-derived coin when possible; for exchanges like
                # Hyperliquid where market_id may be numeric, fall back to base.
                cmc_record = {}
                match_method = ""
                coin_from_market_id = compute_coin_name(market_id, market.get("quote", ""))
                if exchange_id == "hyperliquid" and is_hip3:
                    coin_name = normalize_symbol(base or symbol, self._symbol_mappings)
                elif re.search(r"[A-Z]", coin_from_market_id):
                    coin_name = coin_from_market_id
                else:
                    coin_name = normalize_symbol(base, self._symbol_mappings)
                if not is_hip3 and not is_tradfi and cmc_best:
                    exchange_price = prev_prices.get(market_id)
                    cmc_record, match_method, _ = self._resolve_cmc_record_no_symbolmap(
                        coin_name=coin_name,
                        base_coin=base,
                        exchange_price=exchange_price,
                        cmc_all=cmc_all,
                        cmc_best=cmc_best,
                        cmc_name_index=cmc_name_index,
                        cmc_slug_index=cmc_slug_index,
                        cmc_best_values=cmc_best_values,
                    )
                    if match_method and "price" in match_method:
                        price_disambiguated += 1
                    if not cmc_record:
                        unmatched_entry = (str(market_id).upper(), str(coin_name).upper())
                        unmatched_cmc_all.append(unmatched_entry)

                        eligibility_record = {
                            "active": market.get("active", True),
                            "swap": market.get("swap", False),
                            "linear": market.get("linear", True),
                            "is_hip3": is_hip3,
                            "dex": market.get("info", {}).get("dex") if is_hip3 else None,
                            "open_interest": self._to_float(market.get("info", {}).get("openInterest")),
                        }
                        if self._passes_active_filter(exchange_id, eligibility_record):
                            unmatched_cmc_relevant.append(unmatched_entry)
                
                # Build mapping record
                cmc_id = cmc_record.get("id") if cmc_record else None
                notice = ""
                if cmc_id is not None:
                    md = metadata_by_id.get(str(cmc_id), {}) if metadata_by_id else {}
                    if isinstance(md, dict):
                        notice = md.get("notice") or ""

                quote_payload = cmc_record.get("quote") if cmc_record else None
                usd_quote = quote_payload.get("USD", {}) if isinstance(quote_payload, dict) else {}
                if not isinstance(usd_quote, dict):
                    usd_quote = {}
                market_cap = self._to_float(usd_quote.get("market_cap"))
                if market_cap is None and cmc_record:
                    market_cap = self._to_float(cmc_record.get("self_reported_market_cap"))
                volume_24h = self._to_float(usd_quote.get("volume_24h"))
                vol_mcap = (
                    volume_24h / market_cap
                    if market_cap is not None and market_cap > 0 and volume_24h is not None
                    else None
                )

                record = {
                    # Exchange identity
                    "exchange": exchange_id,
                    "symbol": market_id,
                    "ccxt_symbol": symbol,
                    "base": base,
                    "coin": coin_name,
                    "quote": market.get("quote", ""),
                    "swap": market.get("swap", False),
                    "linear": market.get("linear", True),
                    
                    # Copy trading
                    "copy_trading": market_id in cpt_symbols_set or market_id in previous_cpt_symbols,
                    
                    # CMC data
                    "cmc_id": cmc_id,
                    "cmc_rank": cmc_record.get("cmc_rank") if cmc_record else None,
                    "market_cap": market_cap,
                    "volume_24h": volume_24h,
                    "vol_mcap": vol_mcap,
                    "tags": cmc_record.get("tags", []) if cmc_record else [],
                    "notice": notice,
                    
                    # Balance calc fields from CCXT
                    "contract_size": market.get("contractSize", 1.0),
                    "min_amount": market.get("limits", {}).get("amount", {}).get("min") or market.get("precision", {}).get("amount", 0.0),
                    "min_cost": market.get("limits", {}).get("cost", {}).get("min", 0.0),
                    "precision_amount": market.get("precision", {}).get("amount", 0.0),
                    "max_leverage": market.get("limits", {}).get("leverage", {}).get("max"),
                    
                    # Price fields — use fetched prices if available
                    "price_last": prev_prices.get(market_id),
                    "price_ts": None,
                    "min_order_price": None,
                    
                    # HIP-3 flag
                    "is_hip3": is_hip3,
                    "is_tradfi": is_tradfi,
                    "dex": market.get("info", {}).get("dex") if is_hip3 else None,
                    "open_interest": self._to_float(market.get("info", {}).get("openInterest")),
                    
                    # Active flag
                    "active": market.get("active", True),
                }
                
                mapping.append(record)

            mapping = disambiguate_multiplier_market_coins(mapping)
            
            # Save mapping
            self.save_exchange_mapping(exchange_id, mapping)

            all_unique = sorted({coin for _, coin in unmatched_cmc_all if coin})
            relevant_unique = sorted({coin for _, coin in unmatched_cmc_relevant if coin})
            self._last_build_mapping_stats[exchange_id] = {
                "unmatched_all": len(unmatched_cmc_all),
                "unmatched_all_unique": len(all_unique),
                "unmatched_relevant": len(unmatched_cmc_relevant),
                "unmatched_relevant_unique": len(relevant_unique),
            }

            if unmatched_cmc_relevant:
                sample = ", ".join(f"{coin}({symbol})" for symbol, coin in unmatched_cmc_relevant[:20])
                _log(SERVICE,
                    f'CMC match missing for {len(unmatched_cmc_relevant)} relevant market(s) on {exchange_id} '
                    f'({len(relevant_unique)} unique coin(s)). Sample: {sample}',
                    level='WARNING'
                )
            elif unmatched_cmc_all:
                _log(SERVICE,
                    f'CMC match missing only on non-relevant markets for {exchange_id}: '
                    f'{len(unmatched_cmc_all)} market(s), {len(all_unique)} unique coin(s)',
                    level='DEBUG'
                )
            
            if price_disambiguated:
                _log(SERVICE, f'Successfully built mapping with {len(mapping)} records for {exchange_id} ({price_disambiguated} CMC matches resolved by price)', level='INFO')
            else:
                _log(SERVICE, f'Successfully built mapping with {len(mapping)} records for {exchange_id}', level='INFO')
            return True
            
        except CoinDataPersistenceError:
            raise
        except Exception as e:
            self._last_build_mapping_stats[exchange_id] = {
                "unmatched_all": 0,
                "unmatched_all_unique": 0,
                "unmatched_relevant": 0,
                "unmatched_relevant_unique": 0,
            }
            _log(SERVICE, f'Error building mapping for {exchange_id}: {e}', level='ERROR')
            return False
    
    def _detect_hip3(self, exchange_id: str, market: dict, cmc_data: dict) -> bool:
        """
        Detect if a market is a HIP-3 stock perpetual.
        
        HIP-3 markets are perpetual futures on Hyperliquid deployed by third-party
        builder DEXes (xyz, flx, cash, hyna, km, vntl, etc.).
        
        CCXT marks these with info.hip3=True and info.dex=<dex_name>.
        Symbol format: {DEX}-{TICKER}/USDC:USDC (e.g. XYZ-TSLA/USDC:USDC)
        All HIP-3 markets are type=swap, onlyIsolated=True.
        
        Args:
            exchange_id: Exchange identifier
            market: CCXT market dict
            cmc_data: CMC data dict (unused)
        
        Returns:
            True if the market is a HIP-3 perpetual
        """
        if exchange_id != 'hyperliquid':
            return False
        info = market.get('info', {})
        return info.get('hip3', False) is True

    @staticmethod
    def _normalize_cmc_lookup_text(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

    @staticmethod
    def _best_rank_candidate(candidates: list) -> dict:
        if not candidates:
            return {}
        return min(candidates, key=lambda coin: coin.get("cmc_rank", 99999) or 99999)

    def _resolve_cmc_record_no_symbolmap(
        self,
        coin_name: str,
        base_coin: str,
        exchange_price: float | None,
        cmc_all: dict,
        cmc_best: dict,
        cmc_name_index: dict,
        cmc_slug_index: dict,
        cmc_best_values: list,
    ) -> tuple[dict, str, str]:
        def _pick_symbol(symbol: str) -> tuple[dict, str, str]:
            sym = str(symbol or "").upper().strip()
            if not sym:
                return {}, "", ""
            candidates = cmc_all.get(sym, [])
            if not candidates:
                return {}, "", ""
            if len(candidates) == 1:
                return candidates[0], "symbol_exact", sym
            if exchange_price and exchange_price > 0:
                return self._pick_cmc_by_price(candidates, exchange_price, sym), "symbol_price", sym
            return self._best_rank_candidate(candidates), "symbol_best_rank", sym

        variants = []
        for raw in [coin_name, base_coin]:
            value = str(raw or "").upper().strip()
            if not value:
                continue

            for candidate in [
                value,
                re.sub(r"^(?:1(?:0+))", "", value),
                re.sub(r"^1M", "", re.sub(r"^(?:1(?:0+))", "", value)),
                remove_powers_of_ten(re.sub(r"^1M", "", re.sub(r"^(?:1(?:0+))", "", value))),
                re.sub(r"\d+$", "", remove_powers_of_ten(re.sub(r"^1M", "", re.sub(r"^(?:1(?:0+))", "", value)))),
            ]:
                candidate = str(candidate or "").upper().strip()
                if candidate and candidate not in variants:
                    variants.append(candidate)
                if candidate.startswith("K") and len(candidate) > 1:
                    no_k = candidate[1:]
                    if no_k and no_k not in variants:
                        variants.append(no_k)

        for variant in variants:
            record, method, symbol = _pick_symbol(variant)
            if record:
                return record, method, symbol

        for variant in variants:
            key = self._normalize_cmc_lookup_text(variant)
            if not key:
                continue

            name_candidates = cmc_name_index.get(key, [])
            if name_candidates:
                if len(name_candidates) > 1 and exchange_price and exchange_price > 0:
                    chosen = self._pick_cmc_by_price(name_candidates, exchange_price, key.upper())
                else:
                    chosen = self._best_rank_candidate(name_candidates)
                return chosen, "name_or_slug_exact", str(chosen.get("symbol") or "").upper()

            slug_candidates = cmc_slug_index.get(key, [])
            if slug_candidates:
                if len(slug_candidates) > 1 and exchange_price and exchange_price > 0:
                    chosen = self._pick_cmc_by_price(slug_candidates, exchange_price, key.upper())
                else:
                    chosen = self._best_rank_candidate(slug_candidates)
                return chosen, "name_or_slug_exact", str(chosen.get("symbol") or "").upper()

        for variant in variants:
            key = self._normalize_cmc_lookup_text(variant)
            if len(key) < 4:
                continue

            max_cuts = min(6, max(0, len(key) - 4))
            for cut in range(0, max_cuts + 1):
                prefix = key[:-cut] if cut else key
                if len(prefix) < 4:
                    continue

                candidates = []
                for coin in cmc_best_values:
                    slug = self._normalize_cmc_lookup_text(coin.get("slug", ""))
                    name = self._normalize_cmc_lookup_text(coin.get("name", ""))
                    symbol = self._normalize_cmc_lookup_text(coin.get("symbol", ""))
                    if slug.startswith(prefix) or name.startswith(prefix) or symbol.startswith(prefix):
                        candidates.append(coin)

                if candidates:
                    if len(candidates) > 1 and exchange_price and exchange_price > 0:
                        chosen = self._pick_cmc_by_price(candidates, exchange_price, prefix.upper())
                    else:
                        chosen = self._best_rank_candidate(candidates)
                    return chosen, f"prefix_ranked_{prefix}", str(chosen.get("symbol") or "").upper()

        return {}, "", ""
    
    @staticmethod
    def _pick_cmc_by_price(candidates: list, exchange_price: float, symbol: str) -> dict:
        """Pick the CMC entry whose price is closest to the exchange price.
        
        When multiple CMC coins share the same ticker symbol (e.g. HOT: Holo vs
        HOT Protocol, ACT: Act I vs Acet), we use the exchange price to identify
        which CMC entry actually corresponds to the traded asset.
        
        Uses relative price similarity: 1 - |p1-p2| / max(p1,p2).
        Falls back to best-rank entry if no CMC entry has a valid price.
        
        Args:
            candidates: List of CMC coin dicts sharing the same symbol
            exchange_price: Last known exchange price for this symbol
            symbol: Symbol name for logging
        
        Returns:
            Best-matching CMC coin dict
        """
        best_entry = None
        best_score = -1.0
        best_rank_entry = None
        best_rank = 99999
        
        for coin in candidates:
            # Track best-rank as fallback
            rank = coin.get("cmc_rank", 99999) or 99999
            if rank < best_rank:
                best_rank = rank
                best_rank_entry = coin
            
            # Calculate price similarity
            cmc_price = coin.get("quote", {}).get("USD", {}).get("price", 0) or 0
            if cmc_price and cmc_price > 0:
                max_price = max(exchange_price, cmc_price)
                rel_diff = abs(exchange_price - cmc_price) / max_price
                score = 1.0 - min(rel_diff, 1.0)
                
                if score > best_score:
                    best_score = score
                    best_entry = coin
        
        # Rank-first policy:
        # - Prefer best-rank candidate by default.
        # - Override with price-based candidate only if rank candidate deviates
        #   strongly from exchange price and the alternative is materially better.
        if not best_entry:
            return best_rank_entry or candidates[0]

        if not best_rank_entry:
            return best_entry if best_score >= 0.3 else candidates[0]

        rank_price = best_rank_entry.get("quote", {}).get("USD", {}).get("price", 0) or 0
        rank_score = -1.0
        if rank_price and rank_price > 0:
            max_price = max(exchange_price, rank_price)
            rel_diff = abs(exchange_price - rank_price) / max_price
            rank_score = 1.0 - min(rel_diff, 1.0)

        # If rank entry has no price, use best viable price match when available.
        if rank_score < 0:
            if best_score >= 0.3:
                if best_entry != best_rank_entry:
                    _log(SERVICE,
                         f'CMC price disambiguation for {symbol}: '
                         f'rank entry has no valid price, chose "{best_entry.get("name", "?")}" '
                         f'(score={best_score:.2f})',
                         level='DEBUG')
                return best_entry
            return best_rank_entry

        score_gain = best_score - rank_score

        # Override only on clear evidence:
        # 1) rank is a poor fit and best candidate is a strong fit, or
        # 2) best candidate improves score substantially.
        strong_rank_mismatch = rank_score < 0.8 and best_score >= 0.9
        substantial_gain = score_gain >= 0.15 and best_score >= 0.85

        if best_entry != best_rank_entry and (strong_rank_mismatch or substantial_gain):
            _log(SERVICE,
                 f'CMC price disambiguation for {symbol}: '
                 f'chose "{best_entry.get("name", "?")}" (score={best_score:.2f}) '
                 f'over rank-based "{best_rank_entry.get("name", "?")}" '
                 f'(rank_score={rank_score:.2f}, gain={score_gain:.2f})',
                 level='DEBUG')
            return best_entry

        return best_rank_entry
    
    @_exchange_transaction
    def update_prices(self, exchange_id: str):
        """Update mapping prices and retain a structured result for callers."""
        from Exchange import Exchange, _ccxt_should_retry

        def finish(*, requested=0, priced=0, recovered=0, missing=0, failed=0, ok=False):
            result = {
                "ok": bool(ok),
                "partial": bool(priced and missing),
                "requested": int(requested),
                "priced": int(priced),
                "recovered": int(recovered),
                "missing": int(missing),
                "failed": int(failed),
            }
            if not hasattr(self, "_last_price_update_results"):
                self._last_price_update_results = {}
            self._last_price_update_results[exchange_id] = result
            level = "INFO" if result["ok"] else "ERROR"
            _log(
                SERVICE,
                f'Price update summary for {exchange_id}: requested={result["requested"]} '
                f'priced={result["priced"]} recovered={result["recovered"]} '
                f'missing={result["missing"]} failed={result["failed"]}',
                level=level,
            )
            return result["ok"]

        def product_type(row, market):
            settle = str(market.get("settle") or row.get("quote") or "").upper()
            if settle == "USDT":
                return "USDT-FUTURES"
            if settle == "USDC":
                return "USDC-FUTURES"
            if settle == "SUSDT":
                return "SUSDT-FUTURES"
            if settle == "SUSDC":
                return "SUSDC-FUTURES"
            if settle in {"SBTC", "SETH", "SEOS"}:
                return "SCOIN-FUTURES"
            return "COIN-FUTURES"

        def request_dimension(row, market):
            if exchange_id == "bitget":
                return ("product_type", product_type(row, market))
            if exchange_id == "gateio":
                return ("settle", str(market.get("settle") or row.get("quote") or "").upper())
            if exchange_id in {"binance", "bybit"}:
                return ("sub_type", "linear" if row.get("linear", True) else "inverse")
            return ("category", "all")
        
        _log(SERVICE, f'Updating prices for {exchange_id}', level='INFO')
        
        try:
            # Load existing mapping
            mapping = self.load_mapping(exchange=exchange_id, use_cache=False)
            if not mapping:
                _log(SERVICE, f'No mapping found for {exchange_id}', level='ERROR')
                return finish()
            
            # Create exchange instance
            exchange = Exchange(exchange_id)
            exchange.connect()

            def _to_float(value, default=0.0):
                try:
                    if value is None:
                        return float(default)
                    return float(value)
                except Exception:
                    return float(default)
            
            # Each group maps to one actual category-wide CCXT request dimension.
            active_rows = [r for r in mapping if r.get("active", True) and r.get("ccxt_symbol")]
            symbols = list(dict.fromkeys(r["ccxt_symbol"] for r in active_rows))
            if not symbols:
                return finish()

            markets = self.load_ccxt_markets(exchange_id, use_cache=False)
            request_groups = {}
            for row in active_rows:
                symbol = row["ccxt_symbol"]
                market = markets.get(symbol, {}) if isinstance(markets, dict) else {}
                request_groups.setdefault(request_dimension(row, market), []).append(symbol)

            prices = {}
            failed_symbols = set()
            for dimension, group_symbols in request_groups.items():
                group_symbols = list(dict.fromkeys(group_symbols))
                for attempt in range(1, _PRICE_BATCH_MAX_ATTEMPTS + 1):
                    try:
                        fetched = exchange.fetch_prices(group_symbols, "swap")
                        if isinstance(fetched, dict):
                            prices.update(fetched)
                        break
                    except Exception as exc:
                        retry = (
                            attempt < _PRICE_BATCH_MAX_ATTEMPTS
                            and _ccxt_should_retry(getattr(exchange, "instance", None), exc)
                        )
                        if not retry:
                            failed_symbols.update(group_symbols)
                            _log(
                                SERVICE,
                                f'Batch price request failed for {exchange_id} {dimension}: {exc}',
                                level='ERROR',
                            )
                            break
                        delay = _PRICE_BATCH_BACKOFF_SECONDS[attempt - 1]
                        _log(
                            SERVICE,
                            f'Batch price request retry {attempt}/{_PRICE_BATCH_MAX_ATTEMPTS} '
                            f'for {exchange_id} {dimension} in {delay}s: {exc}',
                            level='WARNING',
                        )
                        sleep(delay)

            valid_prices = {}
            for symbol in symbols:
                ticker = prices.get(symbol)
                if isinstance(ticker, dict) and _to_float(ticker.get("last"), 0.0) > 0:
                    valid_prices[symbol] = ticker

            # Fallback: some exchanges do not include all symbols in batch ticker
            # responses. Keep recovery bounded; Hyperliquid is allMids-only.
            missing_symbols = [symbol for symbol in symbols if symbol not in valid_prices]
            recovered = 0
            if missing_symbols and exchange_id == "hyperliquid":
                _log(
                    SERVICE,
                    f'Missing {len(missing_symbols)} prices after allMids on hyperliquid; '
                    'individual fallback is disabled',
                    level='WARNING',
                )
            elif missing_symbols:
                fallback_symbols = missing_symbols[:_PRICE_INDIVIDUAL_FALLBACK_LIMIT]
                for sym in fallback_symbols:
                    try:
                        ticker = exchange.fetch_price(sym, "swap")
                        if isinstance(ticker, dict) and _to_float(ticker.get("last"), 0.0) > 0:
                            valid_prices[sym] = ticker
                            failed_symbols.discard(sym)
                            recovered += 1
                        else:
                            failed_symbols.add(sym)
                    except Exception:
                        failed_symbols.add(sym)
                if recovered:
                    _log(
                        SERVICE,
                        f'Recovered {recovered}/{len(fallback_symbols)} attempted missing prices '
                        f'via per-symbol fallback on {exchange_id}',
                        level='INFO',
                    )
            
            # Update each record
            current_ts = int(datetime.now().timestamp() * 1000)
            for record in mapping:
                ccxt_symbol = record.get("ccxt_symbol")
                if not ccxt_symbol or ccxt_symbol not in valid_prices:
                    continue
                
                price_data = valid_prices[ccxt_symbol]
                price = _to_float(price_data.get("last", 0), 0.0)
                
                if price <= 0:
                    continue
                
                # Update price fields
                record["price_last"] = price
                record["price_ts"] = price_data.get("timestamp", current_ts)
                
                # Calculate min_order_price
                contract_size = _to_float(record.get("contract_size", 1.0), 1.0)
                min_amount = _to_float(record.get("min_amount", 0.0), 0.0)
                min_cost = _to_float(record.get("min_cost", 0.0), 0.0)
                
                min_qty = min_amount * contract_size
                min_price_from_qty = min_qty * price
                
                # Use max of min_cost and calculated min_price
                record["min_order_price"] = max(min_cost, min_price_from_qty)
            
            # Save updated mapping
            priced = len(valid_prices)
            missing = len(symbols) - priced
            if priced:
                self.save_exchange_mapping(exchange_id, mapping)
            return finish(
                requested=len(symbols),
                priced=priced,
                recovered=recovered,
                missing=missing,
                failed=len(failed_symbols.intersection(missing_symbols)),
                ok=priced > 0 and missing == 0,
            )
            
        except CoinDataPersistenceError:
            raise
        except Exception as e:
            _log(SERVICE, f'Error updating prices for {exchange_id}: {e}', level='ERROR')
            requested = len(symbols) if "symbols" in locals() else 0
            return finish(requested=requested, missing=requested, failed=requested)

    def price_update_result(self, exchange_id: str) -> dict:
        """Return the latest structured price-update result without changing bool callers."""
        return dict(getattr(self, "_last_price_update_results", {}).get(exchange_id, {}))

    def fetch_api_status(self):
        if not self._has_cmc_api_key():
            self.api_error = "No API key configured"
            return False
        endpoint = "status"
        url = 'https://pro-api.coinmarketcap.com/v1/key/info'
        headers = {'Accepts': 'application/json'}
        data, status_code, attempts, error = self._cmc_get_json(
            endpoint=endpoint,
            url=url,
            headers=headers,
            params=None,
            max_retries=3,
            timeout=30,
        )
        if data:
            self.credit_limit_monthly = data["data"]["plan"]["credit_limit_monthly"]
            self.credit_limit_monthly_reset = data["data"]["plan"]["credit_limit_monthly_reset"]
            self.credit_limit_monthly_reset_timestamp = data["data"]["plan"]["credit_limit_monthly_reset_timestamp"]
            self.credits_used_day = data["data"]["usage"]["current_day"]["credits_used"]
            self.credits_used_month = data["data"]["usage"]["current_month"]["credits_used"]
            self.credits_left = data["data"]["usage"]["current_month"]["credits_left"]
            self.api_error = None
            self._cmc_metrics["status_ok"] += 1
            self._log_cmc_metrics(endpoint, True, attempts, status_code)
            return True

        self.api_error = error or f"HTTP {status_code}" if status_code else (error or "unknown error")
        self._cmc_metrics["status_fail"] += 1
        self._log_cmc_metrics(endpoint, False, attempts, status_code, error=error)
        return False

    def fetch_data(self):
        """Fetch listings once across all local PBGui processes."""
        started_ns = time_ns()
        lock_target = Path.cwd() / "data" / "coindata" / ".locks" / "cmc-listings-refresh"
        with advisory_file_lock(lock_target):
            if self._reuse_singleflight_cache("listings", started_ns):
                return True
            success = self._fetch_data_once()
            if success:
                self.save_data()
            return success

    def _fetch_data_once(self):
        endpoint = "listings"
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
            'start':'1',
            'limit':self.fetch_limit
        }
        headers = {'Accepts': 'application/json'}
        data, status_code, attempts, error = self._cmc_get_json(
            endpoint=endpoint,
            url=url,
            headers=headers,
            params=parameters,
            max_retries=3,
            timeout=30,
        )
        if data:
            self.data = data
            self.fetch_api_status()
            self._cmc_metrics["listings_ok"] += 1
            self._log_cmc_metrics(endpoint, True, attempts, status_code)
            _log(SERVICE, f'Fetched CoinMarketCap data. Credits left: {getattr(self, "credits_left", "unknown")}', level='INFO')
            return True

        self.data = None
        self._cmc_metrics["listings_fail"] += 1
        self._log_cmc_metrics(endpoint, False, attempts, status_code, error=error)
        return False
    
    def fetch_metadata(self):
        """Fetch metadata once across all local PBGui processes."""
        started_ns = time_ns()
        lock_target = Path.cwd() / "data" / "coindata" / ".locks" / "cmc-metadata-refresh"
        with advisory_file_lock(lock_target):
            if self._reuse_singleflight_cache("metadata", started_ns):
                return True
            success = self._fetch_metadata_once()
            if success:
                self.save_metadata()
            return success

    def _fetch_metadata_once(self):
        endpoint = "metadata"
        # Make sure we have coindata, but never overwrite already loaded data.
        if not self.data and self.has_new_data():
            self.load_data()
            self.load_symbols()
        if not self.data:
            return False
        if "data" not in self.data:
            return False
        # Create symbols_ids list
        symbols_ids = []
        for symbol in self.symbols_all:
            sym = normalize_symbol(symbol, self._symbol_mappings)
            for coin in self.data["data"]:
                if coin["symbol"] == sym:
                    symbols_ids.append(coin["id"])
        # filter out duplicate ids
        symbols_ids = list(set(symbols_ids))
        # Fetch notice from coinmarketcap
        url = 'https://pro-api.coinmarketcap.com//v2/cryptocurrency/info'
        parameters = {
            'id': ','.join(map(str, symbols_ids))
        }
        headers = {'Accepts': 'application/json'}
        data, status_code, attempts, error = self._cmc_get_json(
            endpoint=endpoint,
            url=url,
            headers=headers,
            params=parameters,
            max_retries=3,
            timeout=30,
        )
        if data:
            self.metadata = data
            self.fetch_api_status()
            self._cmc_metrics["metadata_ok"] += 1
            self._log_cmc_metrics(endpoint, True, attempts, status_code)
            _log(SERVICE, f'Fetched CoinMarketCap metadata. Credits left: {getattr(self, "credits_left", "unknown")}', level='INFO')
            return True

        self.metadata = None
        self._cmc_metrics["metadata_fail"] += 1
        self._log_cmc_metrics(endpoint, False, attempts, status_code, error=error)
        return False

    def _reuse_singleflight_cache(self, endpoint: str, started_ns: int) -> bool:
        """Reuse a valid cache published by an overlapping refresh owner."""
        filename = "coindata.json" if endpoint == "listings" else "metadata.json"
        cache_path = Path.cwd() / "data" / "coindata" / filename
        if not cache_path.exists() or cache_path.stat().st_mtime_ns <= started_ns:
            return False
        payload = _read_json_with_retry(cache_path, retries=1, delay_s=0.1)
        valid, _error = self._validate_cmc_payload(endpoint, payload)
        if not valid:
            return False
        if endpoint == "listings":
            self.data = payload
            self.data_ts = cache_path.stat().st_mtime
        else:
            self.metadata = payload
            self.metadata_ts = cache_path.stat().st_mtime
        return True

    @staticmethod
    def _cmc_actual_credits(payload: dict | None) -> float | None:
        """Read CMC's authoritative per-response credit charge."""
        status = payload.get("status") if isinstance(payload, dict) else None
        if not isinstance(status, dict) or status.get("credit_count") is None:
            return None
        try:
            credits = float(status["credit_count"])
        except (TypeError, ValueError):
            return None
        return credits if credits >= 0 else None

    @staticmethod
    def _cmc_provider_status(response, payload: dict | None, endpoint: str) -> dict:
        """Collect provider counters and reset data for pool settlement."""
        result = {}
        response_headers = getattr(response, "headers", None)
        if hasattr(response_headers, "items"):
            result.update(dict(response_headers.items()))
        status = payload.get("status") if isinstance(payload, dict) else None
        if isinstance(status, dict):
            for key in ("remaining", "limit", "used", "reset_at", "state", "error_code"):
                if status.get(key) is not None:
                    result[key] = status[key]
        if endpoint == "status" and isinstance(payload, dict):
            data = payload.get("data")
            plan = data.get("plan") if isinstance(data, dict) else None
            usage = data.get("usage") if isinstance(data, dict) else None
            current_month = usage.get("current_month") if isinstance(usage, dict) else None
            if isinstance(plan, dict):
                if plan.get("credit_limit_monthly") is not None:
                    result["limit"] = plan["credit_limit_monthly"]
                reset = plan.get("credit_limit_monthly_reset_timestamp")
                if reset is None:
                    reset = plan.get("credit_limit_monthly_reset")
                if reset is not None:
                    result["reset_at"] = reset
            if isinstance(current_month, dict):
                if current_month.get("credits_used") is not None:
                    result["used"] = current_month["credits_used"]
                if current_month.get("credits_left") is not None:
                    result["remaining"] = current_month["credits_left"]
        return result

    @staticmethod
    def _redact_cmc_error(error, api_key: str) -> str | None:
        """Remove the attempt credential if a provider echoes it in diagnostics."""
        if error is None:
            return None
        text = str(error)
        if api_key:
            text = text.replace(api_key, "<redacted>")
        return text

    def _cmc_get_json(
        self,
        endpoint: str,
        url: str,
        headers: dict,
        params: dict | None,
        max_retries: int = 3,
        timeout: int = 30,
    ) -> tuple[dict | None, int | None, int, str | None]:
        retryable_statuses = {401, 402, 403, 429, 500, 502, 503, 504}
        attempts = 0
        last_status = None
        last_error = None

        session = Session()
        session.headers.update({
            key: value
            for key, value in headers.items()
            if str(key).lower() != "x-cmc_pro_api_key"
        })
        endpoint_path = urlparse(url).path or endpoint

        for attempt in range(1, max_retries + 1):
            try:
                acquisition = self.cmc_pool.acquire(
                    endpoint_path,
                    params,
                    estimated_credits=0 if endpoint == "status" else None,
                )
            except CmcPoolExhaustedError as exc:
                last_error = str(exc)
                break
            attempts += 1
            session.headers["X-CMC_PRO_API_KEY"] = acquisition.api_key
            response = None
            payload = None
            provider_status = {}
            actual_credits = None
            retry_after = None
            settlement_error = None
            action = "stop"
            try:
                response = session.get(url, params=params, timeout=timeout)
                last_status = response.status_code
                retry_header = getattr(response, "headers", {}).get("Retry-After") if hasattr(getattr(response, "headers", None), "get") else None
                try:
                    retry_after = float(retry_header) if retry_header is not None else None
                except (TypeError, ValueError):
                    retry_after = None

                if response.status_code == 200:
                    try:
                        payload = json.loads(response.text)
                    except Exception as e:
                        last_error = f'invalid json payload: {e}'
                        settlement_error = last_error
                        action = "retry" if attempt < max_retries else "stop"
                    else:
                        provider_status = self._cmc_provider_status(response, payload, endpoint)
                        is_valid, validation_error = self._validate_cmc_payload(endpoint, payload)
                        if is_valid:
                            actual_credits = self._cmc_actual_credits(payload)
                            action = "success"
                        else:
                            payload_status = payload.get("status") if isinstance(payload, dict) else None
                            provider_error = payload_status.get("error_message") if isinstance(payload_status, dict) else None
                            last_error = provider_error or validation_error or 'invalid payload schema'
                            settlement_error = last_error
                            action = "retry" if attempt < max_retries else "stop"
                else:
                    try:
                        payload = json.loads(response.text)
                        last_error = payload.get("status", {}).get("error_message")
                    except Exception:
                        last_error = (response.text or "")[:200]
                    provider_status = self._cmc_provider_status(response, payload, endpoint)
                    actual_credits = self._cmc_actual_credits(payload)
                    if actual_credits is None:
                        actual_credits = 0
                    settlement_error = last_error or f"HTTP {response.status_code}"
                    action = "retry" if response.status_code in retryable_statuses and attempt < max_retries else "stop"

            except (ConnectionError, Timeout, TooManyRedirects) as e:
                last_error = self._redact_cmc_error(e, acquisition.api_key)
                settlement_error = last_error
                action = "retry" if attempt < max_retries else "stop"
            finally:
                safe_error = self._redact_cmc_error(settlement_error, acquisition.api_key)
                self.cmc_pool.settle(
                    acquisition,
                    status_code=getattr(response, "status_code", None),
                    error=safe_error,
                    provider_status=provider_status,
                    actual_credits=actual_credits,
                    retry_after=retry_after,
                )

            last_error = self._redact_cmc_error(last_error, acquisition.api_key)
            if action == "success":
                return payload, last_status, attempts, None
            if action == "retry":
                wait_s = min(8, 2 ** (attempt - 1))
                reason = f"HTTP {last_status}" if last_status and last_status != 200 else (last_error or "provider error")
                _log(SERVICE, f'CMC {endpoint} retry {attempt}/{max_retries} after {reason}, waiting {wait_s}s', level='WARNING')
                sleep(wait_s)
                continue
            break

        return None, last_status, attempts, last_error

    def _validate_cmc_payload(self, endpoint: str, payload: dict | None) -> tuple[bool, str | None]:
        if not isinstance(payload, dict):
            return False, "payload is not an object"

        data = payload.get("data")
        if endpoint == "listings":
            if not isinstance(data, list):
                return False, "listings payload missing data[]"
            return True, None

        if endpoint == "metadata":
            if not isinstance(data, dict):
                return False, "metadata payload missing data{}"
            return True, None

        if endpoint == "status":
            if not isinstance(data, dict):
                return False, "status payload missing data{}"
            plan = data.get("plan")
            usage = data.get("usage")
            if not isinstance(plan, dict):
                return False, "status payload missing data.plan"
            if not isinstance(usage, dict):
                return False, "status payload missing data.usage"
            current_day = usage.get("current_day")
            current_month = usage.get("current_month")
            if not isinstance(current_day, dict):
                return False, "status payload missing data.usage.current_day"
            if not isinstance(current_month, dict):
                return False, "status payload missing data.usage.current_month"
            required_plan = {
                "credit_limit_monthly",
                "credit_limit_monthly_reset",
                "credit_limit_monthly_reset_timestamp",
            }
            if not required_plan.issubset(plan):
                return False, "status payload missing plan credit fields"
            if "credits_used" not in current_day:
                return False, "status payload missing current-day usage"
            if not {"credits_used", "credits_left"}.issubset(current_month):
                return False, "status payload missing current-month usage"
            return True, None

        return True, None

    def _log_cmc_metrics(
        self,
        endpoint: str,
        success: bool,
        attempts: int,
        status_code: int | None,
        error: str | None = None,
    ):
        summary = (
            f'CMC[{endpoint}] success={success} attempts={attempts} status={status_code} '
            f'listings(ok/fail)={self._cmc_metrics["listings_ok"]}/{self._cmc_metrics["listings_fail"]} '
            f'metadata(ok/fail)={self._cmc_metrics["metadata_ok"]}/{self._cmc_metrics["metadata_fail"]} '
            f'status(ok/fail)={self._cmc_metrics["status_ok"]}/{self._cmc_metrics["status_fail"]}'
        )
        if success:
            _log(SERVICE, summary, level='INFO')
        else:
            _log(SERVICE, f'{summary} error={error}', level='WARNING')
            self._maybe_log_cmc_health(force=True)

    def _maybe_log_cmc_health(self, force: bool = False):
        now_ts = datetime.now().timestamp()
        if not force and now_ts - self._cmc_metrics_last_log_ts < self._cmc_metrics_log_interval_s:
            return

        self._cmc_metrics_last_log_ts = now_ts
        _log(SERVICE,
            (
                'CMC health summary '
                f'listings(ok/fail)={self._cmc_metrics["listings_ok"]}/{self._cmc_metrics["listings_fail"]} '
                f'metadata(ok/fail)={self._cmc_metrics["metadata_ok"]}/{self._cmc_metrics["metadata_fail"]} '
                f'status(ok/fail)={self._cmc_metrics["status_ok"]}/{self._cmc_metrics["status_fail"]}'
            ),
            level='INFO' if force else 'DEBUG'
        )

    def save_metadata(self):
        if not self.metadata:
            return
        pbgdir = Path.cwd()
        coin_path = Path(f'{pbgdir}/data/coindata')
        metadata_path = coin_path / 'metadata.json'
        try:
            _atomic_json_write(metadata_path, self.metadata)
        except Exception as e:
            _log(SERVICE, f'Error saving metadata: {e}', level='ERROR')
            raise CoinDataPersistenceError('Failed to save metadata') from e

    def save_data(self):
        if not self.data:
            return
        pbgdir = Path.cwd()
        coin_path = Path(f'{pbgdir}/data/coindata')
        data_path = coin_path / 'coindata.json'
        try:
            _atomic_json_write(data_path, self.data)
        except Exception as e:
            _log(SERVICE, f'Error saving coindata: {e}', level='ERROR')
            raise CoinDataPersistenceError('Failed to save coindata') from e
    
    def load_data(self):
        pbgdir = Path.cwd()
        coin_path = Path(f'{pbgdir}/data/coindata')
        data_ts = 0
        data_file = coin_path / 'coindata.json'
        if data_file.exists():
            data_ts = data_file.stat().st_mtime
        now_ts = datetime.now().timestamp()
        if self._has_cmc_api_key() and data_ts < now_ts - 3600*self.fetch_interval:
            self.fetch_data()
            loadfromfile = False
        else:
            loadfromfile = True
        if (not self.data or loadfromfile) and data_file.exists():
            data = _read_json_with_retry(data_file, retries=1, delay_s=0.2)
            if isinstance(data, dict):
                self.data = data
                self.data_ts = data_ts
                return
    
    def load_metadata(self):
        pbgdir = Path.cwd()
        coin_path = Path(f'{pbgdir}/data/coindata')
        metadata_ts = 0
        metadata_file = coin_path / 'metadata.json'
        if metadata_file.exists():
            metadata_ts = metadata_file.stat().st_mtime
        now_ts = datetime.now().timestamp()
        if self._has_cmc_api_key() and metadata_ts < now_ts - 3600*24*self.metadata_interval:
            self.fetch_metadata()
        if not self.metadata and metadata_file.exists():
            metadata = _read_json_with_retry(metadata_file, retries=1, delay_s=0.2)
            if isinstance(metadata, dict):
                self.metadata = metadata
                self.metadata_ts = metadata_ts
                return

    def is_data_fresh(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/coindata.json').exists():
            data_ts = Path(f'{coin_path}/coindata.json').stat().st_mtime
            now_ts = datetime.now().timestamp()
            if data_ts > now_ts - 3600*self.fetch_interval:
                return True
        return
    
    def is_metadata_fresh(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/metadata.json').exists():
            data_ts = Path(f'{coin_path}/metadata.json').stat().st_mtime
            now_ts = datetime.now().timestamp()
            if data_ts > now_ts - 3600*24*self.metadata_interval:
                return True
        return

    def update_mappings(self):
        """Fetch CCXT markets and build mappings for all V7-supported exchanges.
        
        Runs on its own interval (mapping_interval, in hours).
        Includes HIP-3 stock perpetuals detection.
        Only processes V7-supported exchanges (binance, bybit, bitget, gateio,
        hyperliquid, kucoin, okx). Legacy exchanges are excluded.
        """
        now_ts = datetime.now().timestamp()

        refreshed_in_self_heal = set()

        # Self-heal pass: if mapping is missing/stale, rebuild immediately
        # independent of interval, with exponential backoff up to 24h.
        for exchange in V7.list():
            needs_heal, reason = self._source_is_newer_than_mapping(exchange)
            if not needs_heal:
                continue

            state = self._mapping_self_heal_state.get(exchange, {"fails": 0, "next_retry_ts": 0.0})
            if now_ts < float(state.get("next_retry_ts", 0.0)):
                continue

            _log(SERVICE, f'Self-heal mapping trigger for {exchange}: {reason}', level='WARNING')
            try:
                result = self.refresh_exchange_mapping(exchange)
                if not bool(result.get("ok")):
                    raise RuntimeError(
                        f'refresh result not ok '
                        f'(markets_ok={result.get("markets_ok")}, '
                        f'mapping_ok={result.get("mapping_ok")}, '
                        f'prices_ok={result.get("prices_ok")})'
                    )
                refreshed_in_self_heal.add(exchange)
                if exchange in self._mapping_self_heal_state:
                    del self._mapping_self_heal_state[exchange]
                _log(SERVICE, f'Self-heal mapping succeeded for {exchange}', level='INFO')
            except Exception as e:
                fails = int(state.get("fails", 0)) + 1
                backoff_hours = min(2 ** (fails - 1), 24)
                next_retry_ts = now_ts + 3600 * backoff_hours
                self._mapping_self_heal_state[exchange] = {
                    "fails": fails,
                    "next_retry_ts": next_retry_ts,
                }
                _log(SERVICE,
                    f'Self-heal mapping failed for {exchange}: {e}. '
                    f'Next retry in {backoff_hours}h (fail #{fails})',
                    level='ERROR'
                )

        if self.update_mappings_ts < now_ts - 3600 * self._mapping_interval:
            cycle_started_ts = datetime.now().timestamp()
            cycle_results = []
            _log(SERVICE, 'Starting mapping update for all exchanges', level='INFO')
            for exchange in V7.list():
                try:
                    if exchange in refreshed_in_self_heal:
                        continue
                    cycle_results.append(self.refresh_exchange_mapping(exchange))
                except Exception as e:
                    cycle_results.append({
                        "exchange": exchange,
                        "markets_ok": False,
                        "mapping_ok": False,
                        "prices_ok": False,
                        "active": 0,
                        "priced": 0,
                        "unmatched_relevant": 0,
                        "unmatched_relevant_unique": 0,
                        "elapsed": 0.0,
                        "ok": False,
                    })
                    _log(SERVICE, f'Failed to update mapping for {exchange}: {e}', level='ERROR')
            self.update_mappings_ts = now_ts

            if not cycle_results:
                total_elapsed = datetime.now().timestamp() - cycle_started_ts
                _log(SERVICE,
                    f'Mapping update skipped: all exchanges were already refreshed by self-heal in {total_elapsed:.1f}s',
                    level='INFO'
                )
                _log(SERVICE, 'Mapping update complete', level='INFO')
                self._run_tradfi_sync()
                return

            total_elapsed = datetime.now().timestamp() - cycle_started_ts
            ok_count = sum(1 for r in cycle_results if r.get("ok"))
            total_count = len(cycle_results)
            per_exchange = ", ".join(
                f'{r.get("exchange")}: {"ok" if r.get("ok") else "fail"} '
                f'({r.get("elapsed", 0.0):.1f}s, {r.get("priced", 0)}/{r.get("active", 0)} priced)'
                for r in cycle_results
            )
            _log(SERVICE,
                f'Mapping update summary: {ok_count}/{total_count} exchanges ok in {total_elapsed:.1f}s | {per_exchange}',
                level='INFO'
            )
            unmatched_total = sum(int(r.get("unmatched_relevant", 0) or 0) for r in cycle_results)
            unmatched_unique_total = sum(int(r.get("unmatched_relevant_unique", 0) or 0) for r in cycle_results)
            unmatched_per_exchange = ", ".join(
                f'{r.get("exchange")}: {int(r.get("unmatched_relevant", 0) or 0)} '
                f'({int(r.get("unmatched_relevant_unique", 0) or 0)} unique)'
                for r in cycle_results
            )
            _log(SERVICE,
                f'CMC unmatched summary (relevant markets): {unmatched_total} market(s), '
                f'{unmatched_unique_total} unique coin(s) total | {unmatched_per_exchange}',
                level='INFO'
            )
            _log(SERVICE, 'Mapping update complete', level='INFO')
            self._run_tradfi_sync()

    @staticmethod
    def _is_master() -> bool:
        """Return True if pbgui.ini role == master."""
        try:
            role = pbgui_purefunc.load_ini("main", "role") or "slave"
            return role.strip().lower() == "master"
        except Exception:
            return False

    def _source_is_newer_than_mapping(self, exchange: str) -> tuple[bool, str]:
        exchange_dir = self._get_exchange_dir(exchange)
        mapping_file = exchange_dir / "mapping.json"
        if not mapping_file.exists():
            return True, "missing mapping.json"

        mapping_ts = mapping_file.stat().st_mtime
        pbgdir = Path.cwd()
        coindata_file = pbgdir / "data" / "coindata" / "coindata.json"
        metadata_file = pbgdir / "data" / "coindata" / "metadata.json"
        markets_file = exchange_dir / "ccxt_markets.json"

        if coindata_file.exists() and coindata_file.stat().st_mtime > mapping_ts:
            return True, "coindata.json newer than mapping"
        if metadata_file.exists() and metadata_file.stat().st_mtime > mapping_ts:
            return True, "metadata.json newer than mapping"
        if markets_file.exists() and markets_file.stat().st_mtime > mapping_ts:
            return True, "ccxt_markets.json newer than mapping"
        return False, ""

    @_exchange_transaction
    def refresh_exchange_mapping(
        self,
        exchange: str,
        *,
        progress_cb=None,
        step_offset: int = 0,
        total_steps: int = 5,
    ) -> dict:
        """Refresh CCXT markets, mapping and prices for one exchange."""
        started_ts = datetime.now().timestamp()

        if progress_cb:
            progress_cb(step_offset, total_steps, f"{exchange}: fetching markets...")
        markets_ok = bool(self.fetch_ccxt_markets(exchange))
        if not markets_ok:
            elapsed = datetime.now().timestamp() - started_ts
            return {
                "exchange": exchange,
                "markets_ok": False,
                "mapping_ok": False,
                "prices_ok": False,
                "active": 0,
                "priced": 0,
                "unmatched_relevant": 0,
                "unmatched_relevant_unique": 0,
                "elapsed": elapsed,
                "price_update": {
                    "ok": False,
                    "partial": False,
                    "requested": 0,
                    "priced": 0,
                    "recovered": 0,
                    "missing": 0,
                    "failed": 0,
                },
                "ok": False,
            }

        if progress_cb:
            progress_cb(step_offset + 1, total_steps, f"{exchange}: loading markets...")
        markets = self.load_ccxt_markets(exchange, use_cache=False)
        if progress_cb:
            progress_cb(step_offset + 2, total_steps, f"{exchange}: updating copy-trading cache...")
        self.fetch_copy_trading_symbols(exchange, markets)
        if progress_cb:
            progress_cb(step_offset + 3, total_steps, f"{exchange}: rebuilding mapping...")
        mapping_ok = bool(self.build_mapping(exchange))
        if progress_cb:
            progress_cb(step_offset + 4, total_steps, f"{exchange}: updating prices...")
        prices_ok = bool(self.update_prices(exchange)) if mapping_ok else False
        price_update = self.price_update_result(exchange) if mapping_ok else {
            "ok": False,
            "partial": False,
            "requested": 0,
            "priced": 0,
            "recovered": 0,
            "missing": 0,
            "failed": 0,
        }

        rows = self.load_mapping(exchange=exchange, use_cache=False)
        active = sum(1 for r in rows if r.get("active", True))
        priced = sum(1 for r in rows if r.get("active", True) and float(r.get("price_last") or 0) > 0)
        build_stats = self._last_build_mapping_stats.get(exchange, {})

        elapsed = datetime.now().timestamp() - started_ts
        result = {
            "exchange": exchange,
            "markets_ok": markets_ok,
            "mapping_ok": mapping_ok,
            "prices_ok": prices_ok,
            "active": active,
            "priced": priced,
            "unmatched_relevant": int(build_stats.get("unmatched_relevant", 0) or 0),
            "unmatched_relevant_unique": int(build_stats.get("unmatched_relevant_unique", 0) or 0),
            "elapsed": elapsed,
            "price_update": price_update,
            "partial": bool(price_update.get("partial")),
            "ok": markets_ok and mapping_ok and prices_ok,
        }
        if progress_cb:
            progress_cb(step_offset + 5, total_steps, f"{exchange}: refreshed.")
        return result

    @staticmethod
    def _has_dynamic_ignore_bots() -> bool:
        """Check if any actually-running V7 instance on this node uses dynamic_ignore.

        Scans data/run_v7/*/config.json for dynamic_ignore=True, then verifies
        the bot is really running by checking for main.py + config_run.json in
        the process cmdline (same detection logic as RunV7.pid() in PBRun.py).
        """
        run_v7_dir = Path('data/run_v7')
        if not run_v7_dir.exists():
            return False
        for instance_dir in run_v7_dir.iterdir():
            if not instance_dir.is_dir():
                continue
            config_file = instance_dir / 'config.json'
            if not config_file.exists():
                continue
            try:
                with open(config_file, encoding='utf-8') as f:
                    cfg = json.load(f)
                if not cfg.get('pbgui', {}).get('dynamic_ignore', False):
                    continue
            except Exception:
                continue
            config_run = instance_dir / 'config_run.json'
            if not config_run.exists():
                continue
            for proc in psutil.process_iter():
                try:
                    cmdline = proc.cmdline()
                except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                    continue
                if (
                    any('main.py' in s for s in cmdline)
                    and any(_arg_matches_path(s, config_run) for s in cmdline)
                ):
                    return True
        return False

    def _run_tradfi_sync(self):
        """Sync TradFi symbol map + XYZ spec after a mapping update cycle.

        fetch_xyz_spec (web-scraping docs.trade.xyz) only runs on master —
        it needs bs4 and is only useful for the Market Data UI.
        sync_tradfi_spec runs on all nodes (master + slave).
        """
        try:
            from tradfi_sync import sync_tradfi_spec
            if self._is_master():
                from tradfi_sync import fetch_xyz_spec
                instruments = fetch_xyz_spec(pbgui_dir=Path.cwd())
                _log(SERVICE, f'XYZ spec refreshed: {len(instruments)} instruments', level='INFO')
            summary = sync_tradfi_spec(pbgui_dir=Path.cwd())
            _log(SERVICE,
                f"TradFi sync: +{summary['added_pending']} new pending, "
                f"+{summary['added_delisted']} new delisted, "
                f"{summary['auto_delisted']} auto-delisted, "
                f"total {summary['total_entries']} entries",
                level='INFO',
            )
        except Exception as exc:
            _log(SERVICE, f'TradFi sync failed (non-critical): {exc}', level='WARNING')

    def load_symbols(self):
        exchange = str(self.exchange or "").lower()
        mapping = self.load_mapping(exchange=exchange, use_cache=True)

        swap_symbols = []
        cpt_symbols = []
        for record in mapping:
            if not self._passes_active_filter(exchange, record):
                continue
            symbol = str(record.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            swap_symbols.append(symbol)
            if bool(record.get("copy_trading", False)):
                cpt_symbols.append(symbol)

        self._symbols = sorted(set(swap_symbols))
        self._symbols_cpt = sorted(set(cpt_symbols)) if cpt_symbols else self._symbols
        self._symbol_mappings = build_symbol_mappings(self._symbols)
    
    def load_symbols_all(self):
        all_symbols = []
        for exchange in self.exchanges:
            mapping = self.load_mapping(exchange=str(exchange).lower(), use_cache=True)
            for record in mapping:
                if not self._passes_active_filter(str(exchange).lower(), record):
                    continue
                symbol = str(record.get("symbol") or "").strip().upper()
                if symbol:
                    all_symbols.append(symbol)
        self._symbols_all = sorted(set(all_symbols))
        self._symbol_mappings = build_symbol_mappings(self._symbols_all)

    def list_symbols(self):
        self.load_symbols()

        exchange = str(self.exchange or "").lower()
        mapping = self.load_mapping(exchange=exchange, use_cache=True)

        self._symbols_data = []
        self._symbols_notice = []
        self._symbols_notices = {}
        self._all_tags = []

        approved, ignored = self.filter_mapping(
            exchange=exchange,
            market_cap_min_m=self.market_cap,
            vol_mcap_max=self.vol_mcap,
            only_cpt=self.only_cpt,
            notices_ignore=self.notices_ignore,
            tags=self.tags,
            active_only=True,
            quote_filter=None,
            use_cache=True,
        )
        self.approved_coins = approved
        self.ignored_coins = ignored

        approved_set = set(self.approved_coins)
        row_id = 0
        for record in mapping:
            if not self._passes_active_filter(exchange, record):
                continue

            symbol = str(record.get("symbol") or "").strip().upper()
            quote = str(record.get("quote") or "").strip().upper()
            if not symbol:
                continue

            coin = str(record.get("coin") or "").strip().upper()
            if not coin:
                coin = compute_coin_name(symbol, quote)
            coin = str(coin or "").strip().upper()
            if not coin:
                continue

            tags = list(record.get("tags") or [])
            for tag in tags:
                if tag and tag not in self._all_tags:
                    self._all_tags.append(tag)

            notice = record.get("notice")
            if notice:
                self._symbols_notice.append(coin)
                self._symbols_notices[coin] = notice

            if coin not in approved_set:
                continue

            market_cap, volume_24h, vol_mcap = self._mapping_cmc_metrics(record)
            slug = str(record.get("slug") or "").strip()

            symbol_data = {
                "id": int(record.get("cmc_id") or 999999),
                "symbol": symbol,
                "name": str(record.get("name") or "not found on CoinMarketCap"),
                "tags": tags,
                "price": float(record.get("price_last") or 0),
                "volume_24h": int(volume_24h) if volume_24h is not None else None,
                "market_cap": int(market_cap) if market_cap is not None else None,
                "vol/mcap": vol_mcap,
                "copy_trading": bool(record.get("copy_trading", False)),
                "notice": notice,
                "link": f'https://coinmarketcap.com/currencies/{slug}' if slug else None,
            }
            self._symbols_data.append(symbol_data)
            row_id += 1

        self._symbols_notice = sorted(set(self._symbols_notice))
        self._all_tags = sorted(self._all_tags)
        self._symbols_data = sorted(
            self._symbols_data,
            key=lambda row: (
                row.get("market_cap") is None,
                -float(row.get("market_cap") or 0),
                str(row.get("symbol") or "").strip().upper(),
            ),
        )

    def filter_by_market_cap(self, symbols: list, mc: int):
        symbol_set = {str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()}
        approved_coins = set()
        ignored_coins = set()

        mapping = self.load_mapping(exchange=str(self.exchange).lower(), use_cache=True)
        for record in mapping:
            if not self._passes_active_filter(str(self.exchange).lower(), record):
                continue
            symbol = str(record.get("symbol") or "").strip().upper()
            if symbol not in symbol_set:
                continue

            quote = str(record.get("quote") or "").strip().upper()
            coin = str(record.get("coin") or "").strip().upper()
            if not coin:
                coin = compute_coin_name(symbol, quote)
            coin = str(coin or "").strip().upper()
            if not coin:
                continue

            market_cap, _volume_24h, _vol_mcap = self._mapping_cmc_metrics(record)
            threshold = float(mc)
            if threshold <= 0 or (market_cap is not None and market_cap > threshold):
                approved_coins.add(coin)
            else:
                ignored_coins.add(coin)

        ignored_coins -= approved_coins
        return sorted(approved_coins), sorted(ignored_coins)

def main():
    from credential_process_registry import ProcessCapabilityHeartbeat

    pbcoindata = CoinData(defer_config=True)
    if pbcoindata.is_running():
        _log(SERVICE, 'PBCoinData already started', level='ERROR')
        sys.exit(1)
    _log(SERVICE, 'Start: PBCoinData', level='INFO')
    pbcoindata.save_pid()
    capability = ProcessCapabilityHeartbeat(Path(__file__).resolve().parent, "PBCoinData")
    capability.__enter__()
    pbcoindata._ini_watcher.start()
    config_retry_count = 0
    try:
        while True:
            try:
                pbcoindata._ini_watcher.changed.clear()
                pbcoindata.load_config()
                if pbcoindata._config_load_failed:
                    config_retry_count += 1
                else:
                    config_retry_count = 0
                if not pbcoindata._is_master():
                    if not pbcoindata._has_dynamic_ignore_bots():
                        if not pbcoindata._logged_idle:
                            _log(SERVICE, 'No running dynamic_ignore bots — idle mode', level='INFO')
                            pbcoindata._logged_idle = True
                        delay = min(60, 2 ** min(config_retry_count, 5)) if config_retry_count else 60
                        pbcoindata._ini_watcher.changed.wait(timeout=delay)
                        continue
                    if pbcoindata._logged_idle:
                        _log(SERVICE, 'Running dynamic_ignore bot detected — resuming', level='INFO')
                        pbcoindata._logged_idle = False
                pbcoindata.load_data()
                pbcoindata.load_metadata()
                pbcoindata.update_mappings()
                delay = min(60, 2 ** min(config_retry_count, 5)) if config_retry_count else 60
                pbcoindata._ini_watcher.changed.wait(timeout=delay)
            except Exception as e:
                _log(SERVICE, f'Something went wrong, but continue: {e}', level='ERROR')
                _log(SERVICE, 'PBCoinData main loop traceback', level='DEBUG', meta={'traceback': traceback.format_exc()})
    finally:
        pbcoindata._ini_watcher.stop()
        capability.close()

if __name__ == '__main__':
    main()
