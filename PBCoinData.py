import psutil
import subprocess
from time import sleep
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import configparser
from pathlib import Path, PurePath
from datetime import datetime
import platform
import sys
import os
import re
from Exchange import Exchange, Exchanges, V7
from logging_helpers import human_log as _log


def remove_powers_of_ten(text):
    """
    Remove any variant of "10", "100", "1000", "10000", etc. from a string.
    Handles cases like "1000SHIB" -> "SHIB", "1000000BABYDOGE" -> "BABYDOGE".
    Same logic as passivbot's utils.py.
    """
    pattern = r"(?<!\d)1(?:0+)(?!\d)"
    return re.sub(pattern, "", text)


_HYPERLIQUID_K_PREFIX_COINS = {"BONK", "FLOKI", "LUNC", "PEPE", "SHIB", "DOGS", "NEIRO"}


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
    # Strip quote currency suffix
    if quote and name.upper().endswith(quote.upper()):
        name = name[:-len(quote)]
    # Strip bare PERP suffix (Bybit/Bitget USDC markets: BTCPERP -> BTC)
    if name.upper().endswith("PERP") and len(name) > 4:
        name = name[:-4]
    # Handle Hyperliquid k/K-prefix (kPEPE/KPEPE -> PEPE)
    name = _strip_hyperliquid_k_prefix(name)
    # Strip 1000x multiplier prefixes (1000SHIB -> SHIB)
    name = remove_powers_of_ten(name)
    return name.upper()


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
    base = symbol
    
    # Check for stablecoin/quote-like patterns that should NOT be stripped further
    # These are coins whose names resemble quotes (USDe, USDC as trading pair, etc.)
    if base in ["USDC", "USDT", "BUSD", "TUSD", "DAI"]:
        # These are either stablecoins traded as pairs or the coin itself
        return base
    
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
                _log('PBCoinData', f'Retrying JSON read for {path} ({attempt}/{attempts - 1}) after error: {e}', level='WARNING')
                sleep(delay_s)
                continue
            break
    _log('PBCoinData', f'Failed to read JSON file {path}: {last_error}', level='WARNING')
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

        for record in mapping if isinstance(mapping, list) else []:
            symbol = str(record.get("symbol") or "").strip().upper()
            if not symbol:
                continue

            if market_type == "swap" and not bool(record.get("swap", False)):
                continue
            if market_type == "spot" and not bool(record.get("spot", False)):
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
    def __init__(self):
        pbgdir = Path.cwd()
        self.piddir = Path(f'{pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbcoindata.pid')
        self.my_pid = None
        self._api_key = None
        self.api_error = None
        self._fetch_limit = 5000
        self._fetch_interval = 24
        self._metadata_interval = 1
        self._mapping_interval = 24
        self.ini_ts = 0
        self._cleanup_legacy_exchange_ini_entries()
        self.load_config()
        self.data = None
        self.metadata = None
        self.data_ts = 0
        self.metadata_ts = 0
        self._exchange = Exchanges.list()[0]
        self.exchanges = Exchanges.list()
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
    
    def load_ccxt_markets(self, exchange: str) -> dict:
        """Load CCXT markets from cache for a specific exchange."""
        if exchange in self._ccxt_markets:
            return self._ccxt_markets[exchange]
        
        markets_file = self._get_exchange_dir(exchange) / "ccxt_markets.json"
        if not markets_file.exists():
            return {}
        
        try:
            markets = _read_json_with_retry(markets_file, retries=1, delay_s=0.2)
            if isinstance(markets, dict):
                self._ccxt_markets[exchange] = markets
                return markets
            _log('PBCoinData', f'CCXT markets for {exchange} are not a dict, ignoring cache', level='WARNING')
        except Exception as e:
            _log('PBCoinData', f'Error loading CCXT markets for {exchange}: {e}', level='ERROR')
            return {}
        return {}
    
    def save_ccxt_markets(self, exchange: str, markets: dict):
        """Save CCXT markets to cache. Only writes on success."""
        if not markets:
            _log('PBCoinData', f'Empty markets data for {exchange}, not saving', level='WARNING')
            return
        
        exchange_dir = self._ensure_exchange_dir(exchange)
        markets_file = exchange_dir / "ccxt_markets.json"
        
        try:
            # Atomic write: temp file + rename
            temp_file = markets_file.with_suffix('.json.tmp')
            with temp_file.open('w') as f:
                json.dump(markets, f, indent=4)
            temp_file.replace(markets_file)
            self._ccxt_markets[exchange] = markets
            _log('PBCoinData', f'Saved CCXT markets for {exchange}', level='DEBUG')
        except Exception as e:
            _log('PBCoinData', f'Error saving CCXT markets for {exchange}: {e}', level='ERROR')
            if temp_file.exists():
                temp_file.unlink()
    
    def load_mapping(self, exchange: str, use_cache: bool = True) -> list:
        """Load mapping.json for an exchange with optional mtime-aware caching."""
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
                self._exchange_mappings[exchange] = mapping
                self._exchange_mapping_ts[exchange] = file_sig
                return mapping
            _log('PBCoinData', f'Mapping for {exchange} is not a list, ignoring cache file', level='WARNING')
            return []
        except Exception as e:
            _log('PBCoinData', f'Error loading mapping for {exchange}: {e}', level='ERROR')
            return []

    def load_exchange_mapping(self, exchange: str) -> list:
        """Backward-compatible wrapper for load_mapping()."""
        return self.load_mapping(exchange=exchange, use_cache=True)
    
    def save_exchange_mapping(self, exchange: str, mapping: list):
        """Save exchange mapping to cache. Only writes on success."""
        if not mapping:
            _log('PBCoinData', f'Empty mapping data for {exchange}, not saving', level='WARNING')
            return
        
        exchange_dir = self._ensure_exchange_dir(exchange)
        mapping_file = exchange_dir / "mapping.json"
        
        try:
            # Atomic write: temp file + rename
            temp_file = mapping_file.with_suffix('.json.tmp')
            with temp_file.open('w') as f:
                json.dump(mapping, f, indent=4)
            temp_file.replace(mapping_file)
            self._exchange_mappings[exchange] = mapping
            stat = mapping_file.stat()
            self._exchange_mapping_ts[exchange] = (stat.st_mtime_ns, stat.st_size)
            _log('PBCoinData', f'Saved mapping for {exchange}', level='DEBUG')
        except Exception as e:
            _log('PBCoinData', f'Error saving mapping for {exchange}: {e}', level='ERROR')
            if temp_file.exists():
                temp_file.unlink()

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

            market_cap = float(record.get("market_cap") or 0)
            volume_24h = float(record.get("volume_24h") or 0)
            vol_mcap = volume_24h / market_cap if market_cap > 0 else 0.0
            has_notice = bool(record.get("notice"))
            is_cpt = bool(record.get("copy_trading", False))
            record_tags = record.get("tags") or []
            is_eligible = self._passes_active_filter(exchange, record)

            passes = (
                (not active_only or is_eligible)
                and market_cap >= float(market_cap_min_m) * 1_000_000
                and vol_mcap < float(vol_mcap_max)
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

            market_cap = float(record.get("market_cap") or 0)
            volume_24h = float(record.get("volume_24h") or 0)
            vol_mcap = volume_24h / market_cap if market_cap > 0 else 0.0
            has_notice = bool(record.get("notice"))
            is_cpt = bool(record.get("copy_trading", False))
            record_tags = record.get("tags") or []
            is_eligible = self._passes_active_filter(exchange, record)

            passes = (
                (not active_only or is_eligible)
                and market_cap >= float(market_cap_min_m) * 1_000_000
                and vol_mcap < float(vol_mcap_max)
                and (not only_cpt or is_cpt)
                and (not notices_ignore or not has_notice)
                and (not tags or any(tag in record_tags for tag in tags))
            )

            if not passes:
                continue

            row = dict(record)
            row["coin"] = coin.upper()
            row["vol/mcap"] = vol_mcap
            row["price"] = row.get("price_last")
            filtered_rows.append(row)

        filtered_rows.sort(key=lambda x: float(x.get("market_cap") or 0), reverse=True)
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
            market_cap = float(record.get("market_cap") or 0)
            if market_cap > float(mc):
                approved.add(coin)
            else:
                ignored.add(coin)

        ignored -= approved
        return sorted(approved), sorted(ignored)
    
    def load_copy_trading_symbols(self, exchange: str) -> list:
        """Load cached copy trading symbols for an exchange."""
        if exchange in self._copy_trading_cache:
            return self._copy_trading_cache[exchange]
        
        cpt_file = self._get_exchange_dir(exchange) / "copy_trading.json"
        if not cpt_file.exists():
            return []
        
        try:
            symbols = _read_json_with_retry(cpt_file, retries=1, delay_s=0.2)
            if isinstance(symbols, list):
                self._copy_trading_cache[exchange] = symbols
                return symbols
            _log('PBCoinData', f'Copy trading cache for {exchange} is not a list, ignoring cache file', level='WARNING')
            return []
        except Exception as e:
            _log('PBCoinData', f'Error loading copy trading symbols for {exchange}: {e}', level='ERROR')
            return []
    
    def save_copy_trading_symbols(self, exchange: str, symbols: list):
        """Save copy trading symbols to cache."""
        exchange_dir = self._ensure_exchange_dir(exchange)
        cpt_file = exchange_dir / "copy_trading.json"
        
        try:
            temp_file = cpt_file.with_suffix('.json.tmp')
            with temp_file.open('w') as f:
                json.dump(sorted(symbols), f, indent=4)
            temp_file.replace(cpt_file)
            self._copy_trading_cache[exchange] = sorted(symbols)
            _log('PBCoinData', f'Saved {len(symbols)} copy trading symbols for {exchange}', level='DEBUG')
        except Exception as e:
            _log('PBCoinData', f'Error saving copy trading symbols for {exchange}: {e}', level='ERROR')
            if temp_file.exists():
                temp_file.unlink()
    
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
        cpt_symbols = []
        
        try:
            if exchange_id == 'bybit':
                # bybit: copy trading info is in CCXT market data
                if not markets:
                    markets = self.load_ccxt_markets(exchange_id)
                if not markets:
                    _log('PBCoinData', f'No markets available for bybit copy trading detection', level='WARNING')
                    return []
                
                for symbol, market in markets.items():
                    if not market.get("swap", False) or not market.get("active", True):
                        continue
                    if not market.get("linear", False):
                        continue
                    info = market.get("info", {})
                    if info.get("copyTrading") == "both":
                        market_id = market.get("id", "")
                        if market_id:
                            cpt_symbols.append(market_id)
                
                _log('PBCoinData', f'Found {len(cpt_symbols)} copy trading symbols for bybit (from market data)', level='INFO')
            
            elif exchange_id in ('binance', 'bitget'):
                cpt_symbols = self._fetch_cpt_with_user_discovery(exchange_id)
            
            else:
                # No copy trading API known for this exchange
                _log('PBCoinData', f'No copy trading API for {exchange_id}', level='DEBUG')
            
            # Cache the result
            if cpt_symbols:
                self.save_copy_trading_symbols(exchange_id, cpt_symbols)
            
        except Exception as e:
            _log('PBCoinData', f'Error fetching copy trading symbols for {exchange_id}: {e}', level='ERROR')
            # Fall back to cached data
            cpt_symbols = self.load_copy_trading_symbols(exchange_id)
            if cpt_symbols:
                _log('PBCoinData', f'Using {len(cpt_symbols)} cached copy trading symbols for {exchange_id}', level='INFO')
        
        return cpt_symbols
    
    def _fetch_cpt_with_user_discovery(self, exchange_id: str) -> list:
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
            _log('PBCoinData', f'No eligible users found for {exchange_id} copy trading', level='WARNING')
            return []
        
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
                    _log('PBCoinData', f'Remembered {user.name} as copy trading user for {exchange_id}', level='INFO')
                else:
                    _log('PBCoinData', f'Using remembered user {user.name} for {exchange_id} copy trading', level='DEBUG')
                _log('PBCoinData', f'Fetched {len(result)} copy trading symbols for {exchange_id} via user {user.name}', level='INFO')
                return result
        
        _log('PBCoinData', f'No user could fetch copy trading symbols for {exchange_id}', level='WARNING')
        return []
    
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
                return [s["symbol"] for s in symbols.get("data", [])]
            
            elif exchange_id == 'bitget':
                symbols = exchange.instance.privateCopyGetV2CopyMixTraderConfigQuerySymbols(
                    {"productType": "USDT-FUTURES"}
                )
                if symbols and symbols.get("data"):
                    return [s["symbol"] for s in symbols["data"]]
                return None
            
        except Exception as e:
            _log('PBCoinData', f'User {user.name} failed for {exchange_id} copy trading: {e}', level='DEBUG')
            return None
    
    def _load_cpt_user(self, exchange_id: str) -> str | None:
        """Load remembered copy trading user from pbgui.ini."""
        pb_config = configparser.ConfigParser()
        pb_config.optionxform = str
        pb_config.read('pbgui.ini')
        key = f'cpt_user.{exchange_id}'
        if pb_config.has_option("coinmarketcap", key):
            return pb_config.get("coinmarketcap", key)
        return None
    
    def _save_cpt_user(self, exchange_id: str, user_name: str):
        """Save working copy trading user to pbgui.ini."""
        pb_config = configparser.ConfigParser()
        pb_config.optionxform = str
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("coinmarketcap"):
            pb_config.add_section("coinmarketcap")
        pb_config.set("coinmarketcap", f'cpt_user.{exchange_id}', user_name)
        ini_path = Path('pbgui.ini')
        tmp_path = ini_path.with_suffix(ini_path.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            pb_config.write(f)
        tmp_path.replace(ini_path)
    
    @property
    def api_key(self):
        return self._api_key
    @api_key.setter
    def api_key(self, new_api_key):
        self._api_key = new_api_key
    
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
                    _log('PBCoinData', 'Can not start PBCoinData', level='ERROR')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log('PBCoinData', 'Stop: PBCoinData', level='INFO')
            psutil.Process(self.my_pid).kill()

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
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        tmp_path = self.pidfile.with_suffix(self.pidfile.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            f.write(str(self.my_pid))
        tmp_path.replace(self.pidfile)

    def has_new_config(self):
        if Path('pbgui.ini').exists():
            ini_ts = Path('pbgui.ini').stat().st_mtime
            if self.ini_ts < ini_ts:
                self.ini_ts = ini_ts
                return True
        return False

    def _cleanup_legacy_exchange_ini_entries(self):
        """TEMP migration cleanup: remove legacy [exchanges] entries from pbgui.ini.

        REMOVE AFTER mapping migration is fully rolled out on all environments.
        """
        ini_path = Path('pbgui.ini')
        if not ini_path.exists():
            return

        pb_config = configparser.ConfigParser()
        pb_config.optionxform = str
        pb_config.read(str(ini_path))

        if not pb_config.has_section("exchanges"):
            return

        try:
            removed_count = len(pb_config.options("exchanges"))
        except Exception:
            removed_count = 0

        pb_config.remove_section("exchanges")

        tmp_path = ini_path.with_suffix(ini_path.suffix + ".tmp")
        try:
            with tmp_path.open('w', encoding='utf-8') as f:
                pb_config.write(f)
            tmp_path.replace(ini_path)
            _log('PBCoinData', f'Removed legacy [exchanges] section from pbgui.ini ({removed_count} entries)', level='INFO')
        except Exception as e:
            _log('PBCoinData', f'Failed to remove legacy [exchanges] section from pbgui.ini: {e}', level='ERROR')
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
    
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

    def load_config(self):
        if self.has_new_config():
            pb_config = configparser.ConfigParser()
            pb_config.optionxform = str
            pb_config.read('pbgui.ini')
            if pb_config.has_option("coinmarketcap", "api_key"):
                self._api_key = pb_config.get("coinmarketcap", "api_key")
            if pb_config.has_option("coinmarketcap", "fetch_limit"):
                self._fetch_limit = int(pb_config.get("coinmarketcap", "fetch_limit"))
            if pb_config.has_option("coinmarketcap", "fetch_interval"):
                self._fetch_interval = int(pb_config.get("coinmarketcap", "fetch_interval"))
            if pb_config.has_option("coinmarketcap", "metadata_interval"):
                self._metadata_interval = int(pb_config.get("coinmarketcap", "metadata_interval"))
            if pb_config.has_option("coinmarketcap", "mapping_interval"):
                self._mapping_interval = int(pb_config.get("coinmarketcap", "mapping_interval"))
            self._sync_cmc_metrics_log_interval()
    
    def save_config(self):
        pb_config = configparser.ConfigParser()
        pb_config.optionxform = str
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("coinmarketcap"):
            pb_config.add_section("coinmarketcap")
        pb_config.set("coinmarketcap", "api_key", self.api_key)
        pb_config.set("coinmarketcap", "fetch_limit", str(self.fetch_limit))
        pb_config.set("coinmarketcap", "fetch_interval", str(self.fetch_interval))
        pb_config.set("coinmarketcap", "metadata_interval", str(self.metadata_interval))
        pb_config.set("coinmarketcap", "mapping_interval", str(self.mapping_interval))
        ini_path = Path('pbgui.ini')
        tmp_path = ini_path.with_suffix(ini_path.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as pbgui_configfile:
            pb_config.write(pbgui_configfile)
        tmp_path.replace(ini_path)
    
    def fetch_ccxt_markets(self, exchange_id: str):
        """Fetch CCXT markets for a specific exchange and save to coindata/{exchange}/ccxt_markets.json"""
        from Exchange import Exchange
        
        _log('PBCoinData', f'Fetching CCXT markets for {exchange_id}', level='INFO')
        
        try:
            # Create exchange instance without user (public API)
            exchange = Exchange(exchange_id)
            exchange.connect()
            
            # Load markets from CCXT
            markets = exchange.instance.load_markets()
            
            if not markets:
                _log('PBCoinData', f'No markets returned for {exchange_id}', level='WARNING')
                return False
            
            # Save raw CCXT markets (all types)
            self.save_ccxt_markets(exchange_id, markets)
            
            _log('PBCoinData', f'Successfully fetched {len(markets)} markets for {exchange_id}', level='INFO')
            return True
            
        except Exception as e:
            _log('PBCoinData', f'Error fetching CCXT markets for {exchange_id}: {e}', level='ERROR')
            return False
    
    def build_mapping(self, exchange_id: str, force_fetch: bool = False):
        """Build mapping.json for an exchange by merging CCXT markets + CMC data"""
        _log('PBCoinData', f'Building mapping for {exchange_id}', level='INFO')
        
        try:
            from Exchange import Exchange

            # Ensure CMC datasets are available for market-cap/tag enrichment
            if not self.data:
                self.load_data()
            if not self.metadata:
                self.load_metadata()
            
            # Load or fetch CCXT markets
            markets = self.load_ccxt_markets(exchange_id)
            if not markets or force_fetch:
                success = self.fetch_ccxt_markets(exchange_id)
                if not success:
                    _log('PBCoinData', f'Failed to fetch markets for {exchange_id}', level='ERROR')
                    return False
                markets = self.load_ccxt_markets(exchange_id)
            
            if not markets:
                _log('PBCoinData', f'No markets available for {exchange_id}', level='ERROR')
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
                _log('PBCoinData', f'CMC data available: {len(cmc_best)} coins ({dupes} with duplicates)', level='DEBUG')
            else:
                _log('PBCoinData', 'No CMC data loaded, using defaults', level='WARNING')

            cmc_best_values = list(cmc_best.values())

            if self.metadata and isinstance(self.metadata, dict):
                raw_md = self.metadata.get("data", {})
                if isinstance(raw_md, dict):
                    metadata_by_id = raw_md
            
            # Load previous mapping for price-based CMC disambiguation
            # When multiple CMC entries share the same symbol (e.g. HOT, ACT, BABY),
            # we use the exchange price from the previous mapping to pick the correct one
            prev_prices = {}
            prev_mapping = self.load_exchange_mapping(exchange_id)
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
                            _log('PBCoinData', f'Could not fetch linear prices for {exchange_id} disambiguation: {e}', level='WARNING')
                        try:
                            ticker_data.update(exchange.fetch_prices(inverse_symbols, "swap"))
                        except Exception as e:
                            _log('PBCoinData', f'Could not fetch inverse prices for {exchange_id} disambiguation: {e}', level='WARNING')
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
                    _log('PBCoinData', f'Fetched {len(prev_prices)} live prices for {exchange_id} CMC disambiguation', level='INFO')
                except Exception as e:
                    _log('PBCoinData', f'Could not fetch live prices for {exchange_id}: {e}', level='WARNING')
            
            # Load copy trading symbols (from cache, populated by update_mappings)
            cpt_symbols = self.load_copy_trading_symbols(exchange_id)
            cpt_symbols_set = set(cpt_symbols)

            # Resilience for authenticated CPT sources (binance/bitget):
            # if CPT symbols are unavailable, preserve previous mapping flags
            # to avoid wiping all copy_trading=True records on rebuild.
            previous_cpt_symbols = set()
            if exchange_id in ("binance", "bitget") and not cpt_symbols_set and prev_mapping:
                previous_cpt_symbols = {
                    rec.get("symbol")
                    for rec in prev_mapping
                    if rec.get("symbol") and rec.get("copy_trading")
                }
                if previous_cpt_symbols:
                    _log(
                        'PBCoinData',
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
                
                # Detect HIP-3 (stock perpetuals)
                is_hip3 = self._detect_hip3(exchange_id, market, cmc_best)
                
                # Find CMC data for this coin
                # Use market_id-derived coin when possible; for exchanges like
                # Hyperliquid where market_id may be numeric, fall back to base.
                cmc_record = {}
                match_method = ""
                coin_from_market_id = compute_coin_name(market_id, market.get("quote", ""))
                if re.search(r"[A-Z]", coin_from_market_id):
                    coin_name = coin_from_market_id
                else:
                    coin_name = normalize_symbol(base, self._symbol_mappings)
                if not is_hip3 and cmc_best:
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
                    
                    # CMC data (0/null/[] defaults for HIP-3)
                    "cmc_id": cmc_id,
                    "cmc_rank": cmc_record.get("cmc_rank", 0) if cmc_record else 0,
                    "market_cap": (cmc_record.get("quote", {}).get("USD", {}).get("market_cap", 0) or cmc_record.get("self_reported_market_cap", 0) or 0) if cmc_record else 0,
                    "volume_24h": cmc_record.get("quote", {}).get("USD", {}).get("volume_24h", 0) if cmc_record else 0,
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
                    "dex": market.get("info", {}).get("dex") if is_hip3 else None,
                    "open_interest": self._to_float(market.get("info", {}).get("openInterest")),
                    
                    # Active flag
                    "active": market.get("active", True),
                }
                
                mapping.append(record)
            
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
                _log(
                    'PBCoinData',
                    f'CMC match missing for {len(unmatched_cmc_relevant)} relevant market(s) on {exchange_id} '
                    f'({len(relevant_unique)} unique coin(s)). Sample: {sample}',
                    level='WARNING'
                )
            elif unmatched_cmc_all:
                _log(
                    'PBCoinData',
                    f'CMC match missing only on non-relevant markets for {exchange_id}: '
                    f'{len(unmatched_cmc_all)} market(s), {len(all_unique)} unique coin(s)',
                    level='DEBUG'
                )
            
            if price_disambiguated:
                _log('PBCoinData', f'Successfully built mapping with {len(mapping)} records for {exchange_id} ({price_disambiguated} CMC matches resolved by price)', level='INFO')
            else:
                _log('PBCoinData', f'Successfully built mapping with {len(mapping)} records for {exchange_id}', level='INFO')
            return True
            
        except Exception as e:
            self._last_build_mapping_stats[exchange_id] = {
                "unmatched_all": 0,
                "unmatched_all_unique": 0,
                "unmatched_relevant": 0,
                "unmatched_relevant_unique": 0,
            }
            _log('PBCoinData', f'Error building mapping for {exchange_id}: {e}', level='ERROR')
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
                    _log('PBCoinData',
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
            _log('PBCoinData',
                 f'CMC price disambiguation for {symbol}: '
                 f'chose "{best_entry.get("name", "?")}" (score={best_score:.2f}) '
                 f'over rank-based "{best_rank_entry.get("name", "?")}" '
                 f'(rank_score={rank_score:.2f}, gain={score_gain:.2f})',
                 level='DEBUG')
            return best_entry

        return best_rank_entry
    
    def update_prices(self, exchange_id: str):
        """Update price fields in mapping.json for an exchange"""
        from Exchange import Exchange
        
        _log('PBCoinData', f'Updating prices for {exchange_id}', level='INFO')
        
        try:
            # Load existing mapping
            mapping = self.load_exchange_mapping(exchange_id)
            if not mapping:
                _log('PBCoinData', f'No mapping found for {exchange_id}', level='ERROR')
                return False
            
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
            
            # Get all active symbol IDs for batch price fetch
            active_rows = [r for r in mapping if r.get("active", True) and r.get("ccxt_symbol")]
            symbols = [r["ccxt_symbol"] for r in active_rows]
            
            # Fetch prices in batch
            try:
                prices = {}
                linear_symbols = [r["ccxt_symbol"] for r in active_rows if r.get("linear", True)]
                inverse_symbols = [r["ccxt_symbol"] for r in active_rows if not r.get("linear", True)]
                if linear_symbols and inverse_symbols:
                    if linear_symbols:
                        prices.update(exchange.fetch_prices(linear_symbols, "swap"))
                    if inverse_symbols:
                        prices.update(exchange.fetch_prices(inverse_symbols, "swap"))
                else:
                    prices = exchange.fetch_prices(symbols, "swap")
            except Exception as e:
                _log('PBCoinData', f'Error fetching prices for {exchange_id}: {e}', level='ERROR')
                return False

            # Fallback: some exchanges do not include all symbols in batch ticker
            # responses (notably some USDC/stock-perp markets). Fetch missing
            # symbols individually to maximize price coverage.
            missing_symbols = [s for s in symbols if s not in prices]
            if missing_symbols:
                if exchange_id == "hyperliquid":
                    _log(
                        'PBCoinData',
                        f'Missing {len(missing_symbols)} prices after batch fetch on hyperliquid; skipping slow per-symbol fallback',
                        level='WARNING'
                    )
                    missing_symbols = []
                recovered = 0
                for sym in missing_symbols:
                    try:
                        ticker = exchange.fetch_price(sym, "swap")
                        if ticker and ticker.get("last") is not None:
                            prices[sym] = ticker
                            recovered += 1
                    except Exception:
                        continue
                if recovered:
                    _log(
                        'PBCoinData',
                        f'Recovered {recovered}/{len(missing_symbols)} missing prices via per-symbol fallback on {exchange_id}',
                        level='INFO'
                    )
            
            # Update each record
            current_ts = int(datetime.now().timestamp() * 1000)
            for record in mapping:
                ccxt_symbol = record.get("ccxt_symbol")
                if not ccxt_symbol or ccxt_symbol not in prices:
                    continue
                
                price_data = prices[ccxt_symbol]
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
            self.save_exchange_mapping(exchange_id, mapping)
            
            _log('PBCoinData', f'Successfully updated prices for {len(symbols)} symbols on {exchange_id}', level='INFO')
            return True
            
        except Exception as e:
            _log('PBCoinData', f'Error updating prices for {exchange_id}: {e}', level='ERROR')
            return False

    def fetch_api_status(self):
        endpoint = "status"
        url = 'https://pro-api.coinmarketcap.com/v1/key/info'
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
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
        endpoint = "listings"
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
            'start':'1',
            'limit':self.fetch_limit
        }
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
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
            _log('PBCoinData', f'Fetched CoinMarketCap data. Credits left: {self.credits_left}', level='INFO')
            return True

        self.data = None
        self._cmc_metrics["listings_fail"] += 1
        self._log_cmc_metrics(endpoint, False, attempts, status_code, error=error)
        return False
    
    def fetch_metadata(self):
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
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
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
            _log('PBCoinData', f'Fetched CoinMarketCap metadata. Credits left: {self.credits_left}', level='INFO')
            return True

        self.metadata = None
        self._cmc_metrics["metadata_fail"] += 1
        self._log_cmc_metrics(endpoint, False, attempts, status_code, error=error)
        return False

    def _cmc_get_json(
        self,
        endpoint: str,
        url: str,
        headers: dict,
        params: dict | None,
        max_retries: int = 3,
        timeout: int = 30,
    ) -> tuple[dict | None, int | None, int, str | None]:
        retryable_statuses = {429, 500, 502, 503, 504}
        attempts = 0
        last_status = None
        last_error = None

        session = Session()
        session.headers.update(headers)

        for attempt in range(1, max_retries + 1):
            attempts = attempt
            try:
                response = session.get(url, params=params, timeout=timeout)
                last_status = response.status_code

                if response.status_code == 200:
                    try:
                        payload = json.loads(response.text)
                    except Exception as e:
                        last_error = f'invalid json payload: {e}'
                        if attempt < max_retries:
                            wait_s = min(8, 2 ** (attempt - 1))
                            _log('PBCoinData', f'CMC {endpoint} retry {attempt}/{max_retries} after malformed JSON, waiting {wait_s}s', level='WARNING')
                            sleep(wait_s)
                            continue
                        break

                    is_valid, validation_error = self._validate_cmc_payload(endpoint, payload)
                    if is_valid:
                        return payload, response.status_code, attempts, None

                    last_error = validation_error or 'invalid payload schema'
                    if attempt < max_retries:
                        wait_s = min(8, 2 ** (attempt - 1))
                        _log('PBCoinData', f'CMC {endpoint} retry {attempt}/{max_retries} after invalid payload, waiting {wait_s}s', level='WARNING')
                        sleep(wait_s)
                        continue
                    break

                if response.status_code in retryable_statuses and attempt < max_retries:
                    wait_s = min(8, 2 ** (attempt - 1))
                    _log('PBCoinData', f'CMC {endpoint} retry {attempt}/{max_retries} after HTTP {response.status_code}, waiting {wait_s}s', level='WARNING')
                    sleep(wait_s)
                    continue

                try:
                    body = json.loads(response.text)
                    last_error = body.get("status", {}).get("error_message")
                except Exception:
                    last_error = (response.text or "")[:200]
                break

            except (ConnectionError, Timeout, TooManyRedirects) as e:
                last_error = str(e)
                if attempt < max_retries:
                    wait_s = min(8, 2 ** (attempt - 1))
                    _log('PBCoinData', f'CMC {endpoint} network retry {attempt}/{max_retries} after {e.__class__.__name__}, waiting {wait_s}s', level='WARNING')
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
            _log('PBCoinData', summary, level='INFO')
        else:
            _log('PBCoinData', f'{summary} error={error}', level='WARNING')
            self._maybe_log_cmc_health(force=True)

    def _maybe_log_cmc_health(self, force: bool = False):
        now_ts = datetime.now().timestamp()
        if not force and now_ts - self._cmc_metrics_last_log_ts < self._cmc_metrics_log_interval_s:
            return

        self._cmc_metrics_last_log_ts = now_ts
        _log(
            'PBCoinData',
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
        coin_path.mkdir(parents=True, exist_ok=True)

        metadata_path = coin_path / 'metadata.json'
        temp_path = metadata_path.with_suffix('.json.tmp')
        try:
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(self.metadata, f)
            temp_path.replace(metadata_path)
        except Exception as e:
            _log('PBCoinData', f'Error saving metadata: {e}', level='ERROR')
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

    def save_data(self):
        if not self.data:
            return
        pbgdir = Path.cwd()
        coin_path = Path(f'{pbgdir}/data/coindata')
        coin_path.mkdir(parents=True, exist_ok=True)

        data_path = coin_path / 'coindata.json'
        temp_path = data_path.with_suffix('.json.tmp')
        try:
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(self.data, f)
            temp_path.replace(data_path)
        except Exception as e:
            _log('PBCoinData', f'Error saving coindata: {e}', level='ERROR')
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
    
    def load_data(self):
        pbgdir = Path.cwd()
        coin_path = Path(f'{pbgdir}/data/coindata')
        data_ts = 0
        data_file = coin_path / 'coindata.json'
        if data_file.exists():
            data_ts = data_file.stat().st_mtime
        now_ts = datetime.now().timestamp()
        if data_ts < now_ts - 3600*self.fetch_interval:
            self.fetch_data()
            self.save_data()
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
        if metadata_ts < now_ts - 3600*24*self.metadata_interval:
            self.fetch_metadata()
            self.save_metadata()
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
        hyperliquid, okx). Legacy exchanges (bingx, kucoin) are excluded.
        """
        now_ts = datetime.now().timestamp()

        def _refresh_exchange_mapping(exchange: str):
            started_ts = datetime.now().timestamp()

            markets_ok = bool(self.fetch_ccxt_markets(exchange))
            markets = self.load_ccxt_markets(exchange)
            self.fetch_copy_trading_symbols(exchange, markets)
            mapping_ok = bool(self.build_mapping(exchange))
            prices_ok = bool(self.update_prices(exchange))

            rows = self.load_exchange_mapping(exchange)
            active = sum(1 for r in rows if r.get("active", True))
            priced = sum(1 for r in rows if r.get("active", True) and float(r.get("price_last") or 0) > 0)
            build_stats = self._last_build_mapping_stats.get(exchange, {})

            elapsed = datetime.now().timestamp() - started_ts
            return {
                "exchange": exchange,
                "markets_ok": markets_ok,
                "mapping_ok": mapping_ok,
                "prices_ok": prices_ok,
                "active": active,
                "priced": priced,
                "unmatched_relevant": int(build_stats.get("unmatched_relevant", 0) or 0),
                "unmatched_relevant_unique": int(build_stats.get("unmatched_relevant_unique", 0) or 0),
                "elapsed": elapsed,
                "ok": markets_ok and mapping_ok and prices_ok,
            }

        def _source_is_newer_than_mapping(exchange: str):
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

        refreshed_in_self_heal = set()

        # Self-heal pass: if mapping is missing/stale, rebuild immediately
        # independent of interval, with exponential backoff up to 24h.
        for exchange in V7.list():
            needs_heal, reason = _source_is_newer_than_mapping(exchange)
            if not needs_heal:
                continue

            state = self._mapping_self_heal_state.get(exchange, {"fails": 0, "next_retry_ts": 0.0})
            if now_ts < float(state.get("next_retry_ts", 0.0)):
                continue

            _log('PBCoinData', f'Self-heal mapping trigger for {exchange}: {reason}', level='WARNING')
            try:
                _refresh_exchange_mapping(exchange)
                refreshed_in_self_heal.add(exchange)
                if exchange in self._mapping_self_heal_state:
                    del self._mapping_self_heal_state[exchange]
                _log('PBCoinData', f'Self-heal mapping succeeded for {exchange}', level='INFO')
            except Exception as e:
                fails = int(state.get("fails", 0)) + 1
                backoff_hours = min(2 ** (fails - 1), 24)
                next_retry_ts = now_ts + 3600 * backoff_hours
                self._mapping_self_heal_state[exchange] = {
                    "fails": fails,
                    "next_retry_ts": next_retry_ts,
                }
                _log(
                    'PBCoinData',
                    f'Self-heal mapping failed for {exchange}: {e}. '
                    f'Next retry in {backoff_hours}h (fail #{fails})',
                    level='ERROR'
                )

        if self.update_mappings_ts < now_ts - 3600 * self._mapping_interval:
            cycle_started_ts = datetime.now().timestamp()
            cycle_results = []
            _log('PBCoinData', 'Starting mapping update for all exchanges', level='INFO')
            for exchange in V7.list():
                try:
                    if exchange in refreshed_in_self_heal:
                        continue
                    cycle_results.append(_refresh_exchange_mapping(exchange))
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
                    _log('PBCoinData', f'Failed to update mapping for {exchange}: {e}', level='ERROR')
            self.update_mappings_ts = now_ts

            total_elapsed = datetime.now().timestamp() - cycle_started_ts
            ok_count = sum(1 for r in cycle_results if r.get("ok"))
            total_count = len(cycle_results)
            per_exchange = ", ".join(
                f'{r.get("exchange")}: {"ok" if r.get("ok") else "fail"} '
                f'({r.get("elapsed", 0.0):.1f}s, {r.get("priced", 0)}/{r.get("active", 0)} priced)'
                for r in cycle_results
            )
            _log(
                'PBCoinData',
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
            _log(
                'PBCoinData',
                f'CMC unmatched summary (relevant markets): {unmatched_total} market(s), '
                f'{unmatched_unique_total} unique coin(s) total | {unmatched_per_exchange}',
                level='INFO'
            )
            _log('PBCoinData', 'Mapping update complete', level='INFO')

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

            market_cap = float(record.get("market_cap") or 0)
            volume_24h = float(record.get("volume_24h") or 0)
            vol_mcap = volume_24h / market_cap if market_cap > 0 else 0.0
            slug = str(record.get("slug") or "").strip()

            symbol_data = {
                "id": int(record.get("cmc_id") or 999999),
                "symbol": symbol,
                "name": str(record.get("name") or "not found on CoinMarketCap"),
                "tags": tags,
                "price": float(record.get("price_last") or 0),
                "volume_24h": int(volume_24h),
                "market_cap": int(market_cap),
                "vol/mcap": vol_mcap,
                "copy_trading": bool(record.get("copy_trading", False)),
                "notice": notice,
                "link": f'https://coinmarketcap.com/currencies/{slug}' if slug else None,
            }
            self._symbols_data.append(symbol_data)
            row_id += 1

        self._symbols_notice = sorted(set(self._symbols_notice))
        self._all_tags = sorted(self._all_tags)
        self._symbols_data = sorted(self._symbols_data, key=lambda x: x["market_cap"], reverse=True)

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

            market_cap = float(record.get("market_cap") or 0)
            if market_cap > float(mc):
                approved_coins.add(coin)
            else:
                ignored_coins.add(coin)

        ignored_coins -= approved_coins
        return sorted(approved_coins), sorted(ignored_coins)

def main():
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    _log('PBCoinData', 'Start: PBCoinData', level='INFO')
    pbcoindata = CoinData()
    if pbcoindata.is_running():
        _log('PBCoinData', 'PBCoinData already started', level='ERROR')
        exit(1)
    pbcoindata.save_pid()
    while True:
        try:
            pbcoindata.load_data()
            pbcoindata.load_metadata()
            pbcoindata.update_mappings()
            sleep(60)
            pbcoindata.load_config()
        except Exception as e:
            _log('PBCoinData', f'Something went wrong, but continue: {e}', level='ERROR')

if __name__ == '__main__':
    main()
