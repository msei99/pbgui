import ccxt
import ccxt.pro as ccxt_pro
import configparser
from User import User, Users
from enum import Enum
import json
from pathlib import Path
import time
from time import sleep
from datetime import datetime
from pbgui_purefunc import PBGDIR
from logging_helpers import human_log as _human_log

from ccxt.base.errors import (
    AuthenticationError,
    BadRequest,
    DDoSProtection,
    ExchangeError,
    ExchangeNotAvailable,
    NetworkError,
    OnMaintenance,
    PermissionDenied,
    RateLimitExceeded,
    RequestTimeout,
)

# Default network timeout for ccxt / ccxt.pro clients (milliseconds)
# Increased from 60s to 120s to reduce websocket ping/pong keepalive
# timeouts on resource-constrained VPS instances. Can be made
# configurable via `pbgui.ini` later.
DEFAULT_CCXT_TIMEOUT_MS = 120000


def _extract_ccxt_error_payload(exchange_instance, exc: Exception) -> dict | None:
    """Best-effort extraction of the exchange JSON error payload.

    CCXT exceptions often stringify like: "binance {\"code\":..., ...}".
    Some exchanges also expose `last_http_response` / `last_json_response`.
    """

    candidates = []
    try:
        candidates.append(getattr(exchange_instance, 'last_json_response', None))
    except Exception:
        pass
    try:
        candidates.append(getattr(exchange_instance, 'last_http_response', None))
    except Exception:
        pass
    candidates.append(str(exc))

    for cand in candidates:
        if not cand:
            continue
        if isinstance(cand, dict):
            return cand
        if not isinstance(cand, str):
            cand = str(cand)
        s = cand.strip()
        # Try to locate an embedded JSON object.
        i = s.find('{')
        j = s.rfind('}')
        if i == -1 or j == -1 or j <= i:
            continue
        try:
            return json.loads(s[i : j + 1])
        except Exception:
            continue
    return None


def _ccxt_should_retry(exchange_instance, exc: Exception) -> bool:
    """Return True only for likely-transient errors.

    Non-retryable examples we explicitly know about:
    - Binance: BadRequest code -1023 (startTime > endTime)
    - Bitget: ExchangeError code '00001' (interval cannot be > 90 days)
    - Auth/permission errors
    """

    if isinstance(exc, (AuthenticationError, PermissionDenied)):
        return False

    # Some exchanges (notably Bybit) wrap transient conditions in ExchangeError.
    # Keep this conservative and only enable retry for explicitly known cases.
    try:
        msg_l = str(exc).lower()
    except Exception:
        msg_l = ''

    payload = _extract_ccxt_error_payload(exchange_instance, exc)
    code = None
    if isinstance(payload, dict):
        code = payload.get('code')

    if isinstance(exc, BadRequest) and str(code) == '-1023':
        return False
    if isinstance(exc, ExchangeError) and str(code) == '00001':
        return False

    # Bybit known transient conditions (empirically common):
    # - 10006: rate limit
    # - 10002: invalid request / timestamp / recv_window (often transient; time-sync)
    if isinstance(exc, ExchangeError) and str(code) in ('10006', '10002'):
        return True
    if isinstance(exc, ExchangeError) and 'invalid request, please check your server timestamp or recv_window' in msg_l:
        return True

    # Generic rate-limit detection for exchanges that use ExchangeError
    # (e.g., Gate.io: "Request Rate Limit Exceeded" / "TOO_MANY_REQUESTS").
    if isinstance(exc, ExchangeError) and (
        'rate limit' in msg_l or 'too many requests' in msg_l or 'too many visits' in msg_l
    ):
        return True

    return isinstance(
        exc,
        (
            NetworkError,
            RequestTimeout,
            DDoSProtection,
            RateLimitExceeded,
            ExchangeNotAvailable,
            OnMaintenance,
        ),
    )

# Per-exchange limits for how many private (per-user) websocket clients
# the process will create. When the limit is reached `get_private_ws_client`
# will return None so callers can fallback to REST polling. Tunable later
# via `pbgui.ini`.
MAX_PRIVATE_WS_PER_EXCHANGE = {
    'hyperliquid': 10,
    'bitget': 10,
    'bybit': 10,
    'binance': 10,
    'okx': 10,
}

# Global cap for total number of private websocket clients across all exchanges.
# This is an independent, process-wide limit (not derived from per-exchange caps).
# Tune this value to limit overall memory/socket usage on low-memory hosts.
# Consider exposing this via `pbgui.ini` or an environment variable later.
# Default is set conservatively to 20.
MAX_PRIVATE_WS_GLOBAL = 20

# Runtime overrides (set by PBData). PBData will load `ws_max` from
# `pbgui.ini` and call `set_ws_limits` to pass values into this module.
# When set, these take precedence over the hardcoded defaults above.
_RUNTIME_MAX_PRIVATE_WS_GLOBAL = None
_RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE = None

# Track in-flight private client creations (keys like '<exchange>:<user>')
# so concurrent creation attempts are counted toward caps and avoid races.
_CREATION_INFLIGHT = set()

# Optional observers: allow external modules (PBData) to register a callback
# that is invoked whenever a private ws client is closed/removed. This lets
# callers react (e.g. clear manager warn flags) even if the close was initiated
# outside PBData.
_private_client_close_listeners = []

def register_private_client_close_listener(cb):
    try:
        _private_client_close_listeners.append(cb)
    except Exception:
        pass

def _notify_private_client_closed(exchange_id: str, user_name: str):
    try:
        for cb in list(_private_client_close_listeners):
            try:
                cb(exchange_id, user_name)
            except Exception:
                pass
    except Exception:
        pass


def set_ws_limits(global_max=None, per_exchange=None):
    """Set runtime websocket limits.

    - `global_max` (int|None): global cap for private WS clients.
    - `per_exchange` (dict|None): mapping exchange_id -> int cap.

    PBData should call this after reading `pbgui.ini` so Exchange
    does not read the INI file itself.
    """
    global _RUNTIME_MAX_PRIVATE_WS_GLOBAL, _RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE
    try:
        _RUNTIME_MAX_PRIVATE_WS_GLOBAL = int(global_max) if global_max is not None else None
    except Exception:
        _RUNTIME_MAX_PRIVATE_WS_GLOBAL = None
    try:
        _RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE = dict(per_exchange) if per_exchange else None
    except Exception:
        _RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE = None
    # Schedule pruning of excess private clients if limits decreased.
    try:
        import asyncio as _asyncio
        loop = _asyncio.get_running_loop()
        # Launch pruning in background; don't await here.
        loop.create_task(Exchange._prune_private_ws_clients(_RUNTIME_MAX_PRIVATE_WS_GLOBAL, _RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE))
    except Exception:
        # If no running loop is available, pruning will occur lazily
        # next time clients are acquired.
        pass

class Exchanges(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    BITGET = 'bitget'
    GATEIO = 'gateio'
    HYPERLIQUID = 'hyperliquid'
    OKX = 'okx'
    KUCOIN = 'kucoin'
    BINGX = 'bingx'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Exchanges))

class Spot(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Spot))

class Single(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    BITGET = 'bitget'
    OKX = 'okx'
    KUCOIN = 'kucoin'
    BINGX = 'bingx'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Single))

class V7(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    BITGET = 'bitget'
    GATEIO = 'gateio'
    HYPERLIQUID = 'hyperliquid'
    OKX = 'okx'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, V7))

class Passphrase(Enum):
    BITGET = 'bitget'
    OKX = 'okx'
    KUCOIN = 'kucoin'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Passphrase))

class Exchange:
    # Shared ccxt.pro websocket clients per exchange id
    _shared_ws_clients = {}
    # Per-user private websocket clients keyed by "<exchange>:<user.name>"
    _private_ws_clients = {}
    # Track which exchanges have successfully loaded markets for shared ws clients
    _shared_ws_markets_loaded = set()
    # Per-exchange async locks to serialize shared ws client initialization
    _shared_ws_locks = {}
    # Locks for private ws client initialization
    _private_ws_locks = {}
    # Per-exchange creation locks to serialize private-client creation and enforce caps
    _private_creation_locks = {}

    def __init__(self, id: str, user: User = None):
        self.name = id
        self.id = "kucoinfutures" if id == "kucoin" else id
        self.instance = None
        self._markets = None
        self._tf = None
        self.spot = []
        self.swap = []
        self._user = user
        self.error = None


    # _log removed: Exchange module uses `logging_helpers.human_log` directly

    @property
    def user(self): return self._user

    @property
    def tf(self):
        if not self._tf:
            self.connect()
            self._tf = list(self.instance.timeframes.keys())
            if "1s" in self._tf:
                self._tf.remove('1s')
        return self._tf

    @user.setter
    def user(self, new_user):
        if self._user != new_user:
            self._user = new_user

    def connect(self):
        # Create a ccxt sync instance and apply sensible defaults for timeouts
        self.instance = getattr(ccxt, self.id) ()
        try:
            # Apply default network timeout and rate limit behavior
            self.instance.timeout = DEFAULT_CCXT_TIMEOUT_MS
            self.instance.enableRateLimit = True
            # Default type for futures/swap operations
            if not hasattr(self.instance, 'options') or not isinstance(self.instance.options, dict):
                try:
                    self.instance.options = {'defaultType': 'swap'}
                except Exception:
                    pass
            else:
                try:
                    self.instance.options.setdefault('defaultType', 'swap')
                    # Set a larger recvWindow and enable automatic time adjustment
                    # for all exchanges where ccxt supports it to reduce
                    # timestamp/recv_window related signature errors.
                    try:
                        self.instance.options.setdefault('recvWindow', 10000)
                        self.instance.options.setdefault('adjustForTimeDifference', True)
                    except Exception:
                        pass
                    # HIP-3: Configure Hyperliquid to include stock perpetuals
                    if self.id == 'hyperliquid':
                        try:
                            self.instance.options['fetchMarkets'] = {
                                'types': ['swap', 'hip3'],
                                'hip3': {
                                    'dexes': [],  # Empty = auto-discover all HIP-3 DEXes
                                }
                            }
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
        if self._user and self.user.key != 'key':
            self.instance.apiKey = self.user.key
            self.instance.secret = self.user.secret
            self.instance.password = self.user.passphrase
            self.instance.walletAddress = self.user.wallet_address
            self.instance.privateKey = self.user.private_key
        try:
            self.instance.checkRequiredCredentials()
        except Exception as e:
            self.error = (str(e))
            return

    def create_ws_client(self):
        """Create and return a ccxt.pro client configured for this exchange and user.
        Returns None if ccxtpro is not installed or the exchange is unsupported.
        """
        ex_id = self.id
        if not hasattr(ccxt_pro, ex_id):
            return None
        kwargs = {'enableRateLimit': True, 'timeout': DEFAULT_CCXT_TIMEOUT_MS, 'options': {'defaultType': 'swap'}}
        if self.user:
            if getattr(self.user, 'key', None):
                kwargs['apiKey'] = getattr(self.user, 'key')
            if getattr(self.user, 'secret', None):
                kwargs['secret'] = getattr(self.user, 'secret')
            if getattr(self.user, 'passphrase', None):
                kwargs['password'] = getattr(self.user, 'passphrase')
            if getattr(self.user, 'wallet_address', None):
                kwargs['walletAddress'] = getattr(self.user, 'wallet_address')
            if getattr(self.user, 'private_key', None):
                kwargs['privateKey'] = getattr(self.user, 'private_key')
        # Add recvWindow and time-difference adjustment for all exchanges
        try:
            kwargs.setdefault('options', {}).setdefault('recvWindow', 10000)
            kwargs.setdefault('options', {}).setdefault('adjustForTimeDifference', True)
            # HIP-3: Configure Hyperliquid to include stock perpetuals
            if ex_id == 'hyperliquid':
                kwargs.setdefault('options', {})['fetchMarkets'] = {
                    'types': ['swap', 'hip3'],
                    'hip3': {
                        'dexes': [],  # Empty = auto-discover all HIP-3 DEXes
                    }
                }
        except Exception:
            pass

        ex = getattr(ccxt_pro, ex_id)(kwargs)
        return ex

    @classmethod
    async def get_shared_ws_client(cls, id: str, user: User = None):
        """Return a shared ccxt.pro client per (exchange id, api key).

        Markets are loaded once per shared client to avoid excessive load_markets
        calls (important for strict rate-limited exchanges like Bybit).
        """
        # Build key using normalized exchange id only so all users of the same exchange share a single websocket client.
        import asyncio as _asyncio
        key = "kucoinfutures" if id == "kucoin" else id
        uname = getattr(user, 'name', None)

        # Fast path: client already initialized and markets loaded
        client = cls._shared_ws_clients.get(key)
        if client is not None and key in cls._shared_ws_markets_loaded:
            return client

        # Ensure one async lock per exchange key
        if key not in cls._shared_ws_locks:
            cls._shared_ws_locks[key] = _asyncio.Lock()
        lock = cls._shared_ws_locks[key]

        async with lock:
            # Re-check inside lock in case another task finished while we were waiting
            client = cls._shared_ws_clients.get(key)
            if client is not None and key in cls._shared_ws_markets_loaded:
                return client
            ex_id = "kucoinfutures" if id == "kucoin" else id
            if not hasattr(ccxt_pro, ex_id):
                return None
            kwargs = {'enableRateLimit': True, 'timeout': DEFAULT_CCXT_TIMEOUT_MS, 'options': {'defaultType': 'swap'}}
            if user:
                if getattr(user, 'key', None):
                    kwargs['apiKey'] = getattr(user, 'key')
                if getattr(user, 'secret', None):
                    kwargs['secret'] = getattr(user, 'secret')
                if getattr(user, 'passphrase', None):
                    kwargs['password'] = getattr(user, 'passphrase')
                if getattr(user, 'wallet_address', None):
                    kwargs['walletAddress'] = getattr(user, 'wallet_address')
                if getattr(user, 'private_key', None):
                    kwargs['privateKey'] = getattr(user, 'private_key')
            # Enforce recvWindow and time-difference adjustment for all private clients
            try:
                kwargs.setdefault('options', {}).setdefault('recvWindow', 10000)
                kwargs.setdefault('options', {}).setdefault('adjustForTimeDifference', True)
                # HIP-3: Configure Hyperliquid to include stock perpetuals
                if ex_id == 'hyperliquid':
                    kwargs.setdefault('options', {})['fetchMarkets'] = {
                        'types': ['swap', 'hip3'],
                        'hip3': {
                            'dexes': [],  # Empty = auto-discover all HIP-3 DEXes
                        }
                    }
            except Exception:
                pass

            ex = getattr(ccxt_pro, ex_id)(kwargs)

            # Ensure markets are loaded once for this shared client with stronger backoff
            retries = 0
            max_retries = 5
            while True:
                try:
                    try:
                        tmp = cls(id, user)
                        # INFO for initial attempt, WARNING when retrying
                        _human_log(
                            'Exchange',
                            f"get_shared_ws_client calling load_markets for {id} retry={retries}",
                            level=('WARNING' if retries else 'INFO'),
                            user=user,
                        )
                    except Exception:
                        pass

                    lm = ex.load_markets()
                    if _asyncio.iscoroutine(lm):
                        await lm
                    else:
                        import asyncio
                        await asyncio.to_thread(ex.load_markets)
                    cls._shared_ws_clients[key] = ex
                    cls._shared_ws_markets_loaded.add(key)
                    try:
                        _human_log(
                            'Exchange',
                            f"get_shared_ws_client load_markets succeeded for {id} key={key} user={uname}",
                            level='INFO',
                            user=user,
                        )
                    except Exception:
                        pass
                    return ex
                except Exception as e:
                    msg = str(e)
                    lower = msg.lower()
                    is_rate_limit = ('too many visits' in lower or 'rate limit' in lower or 'too many requests' in lower)
                    # Treat network timeouts as transient and retry with backoff
                    if 'timed out' in lower or 'timeout' in lower or 'requesttimeout' in lower:
                        is_rate_limit = True
                    # Bybit-specific invalid timestamp / recv_window errors can also happen if hammered; treat like rate-limit for backoff
                    if 'invalid request, please check your server timestamp or recv_window' in lower:
                        is_rate_limit = True
                    if is_rate_limit and retries < max_retries:
                        retries += 1
                        delay = min(5 * retries, 30)
                        try:
                            _human_log(
                                'Exchange',
                                f"get_shared_ws_client load_markets rate-limited for {id} user={uname}; retry {retries} in {delay}s: {msg}",
                                level='WARNING',
                                user=user,
                            )
                        except Exception:
                            pass
                        await _asyncio.sleep(delay)
                        continue
                    # If load_markets ultimately fails we don't cache the client; log reason
                    try:
                        _human_log(
                            'Exchange',
                            f"get_shared_ws_client load_markets failed for {id} user={uname} after {retries} retries: {msg}",
                            level='ERROR',
                            user=user,
                        )
                    except Exception:
                        pass
                    try:
                        await ex.close()
                    except Exception:
                        pass
                    return None

    @classmethod
    async def get_private_ws_client(cls, id: str, user: User, caller: str = None):
        """Return a per-user authenticated ccxt.pro client for private streams.

        Keyed by `<exchange_id>:<user.name>` so each user gets their own client.
        """
        import asyncio as _asyncio
        if not user:
            return None
        base_key = "kucoinfutures" if id == "kucoin" else id
        key = f"{base_key}:{user.name}"

        # Fast path
        client = cls._private_ws_clients.get(key)
        if client is not None:
            return client

        # Ensure one async lock per exchange to serialize creation attempts
        # for that exchange. This prevents many concurrent creators for the
        # same exchange but does not by itself prevent global races across
        # different exchanges; for that we also account for in-flight
        # creations below.
        if base_key not in cls._private_creation_locks:
            cls._private_creation_locks[base_key] = _asyncio.Lock()
        creation_lock = cls._private_creation_locks[base_key]

        async with creation_lock:
            # Re-check fast path inside creation lock
            client = cls._private_ws_clients.get(key)
            if client is not None:
                return client

            # Ensure one async lock per private key
            if key not in cls._private_ws_locks:
                cls._private_ws_locks[key] = _asyncio.Lock()
            lock = cls._private_ws_locks[key]

            async with lock:
                client = cls._private_ws_clients.get(key)
                if client is not None:
                    return client
                ex_id = "kucoinfutures" if id == "kucoin" else id
                # Enforce per-exchange private-ws client caps to avoid resource
                # exhaustion on constrained VPS. If the cap is reached return None
                # so callers can fall back to REST polling.
                try:
                    # Per-exchange cap: prefer runtime override if provided
                    if _RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE and base_key in _RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE:
                        cap = int(_RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE.get(base_key))
                    else:
                        cap = MAX_PRIVATE_WS_PER_EXCHANGE.get(base_key)
                except Exception:
                    cap = None
                # Global cap: prefer runtime override if provided
                try:
                    global_cap = int(_RUNTIME_MAX_PRIVATE_WS_GLOBAL) if _RUNTIME_MAX_PRIVATE_WS_GLOBAL is not None else MAX_PRIVATE_WS_GLOBAL
                except Exception:
                    global_cap = MAX_PRIVATE_WS_GLOBAL
                if global_cap is not None:
                    # Count both existing and in-flight creations so concurrent
                    # creators don't overshoot the global cap.
                    total_private = len(cls._private_ws_clients.keys()) + len(_CREATION_INFLIGHT)
                    if total_private >= global_cap:
                        try:
                            msg = f"get_private_ws_client: reached GLOBAL cap ({total_private}/{global_cap}); returning None to allow REST fallback for user={user.name}"
                            if caller:
                                msg = msg + f" caller={caller}"
                            _human_log('Exchange', msg, level='WARNING', user=user)
                        except Exception:
                            pass
                        return None
                if cap is not None:
                    # Count existing + in-flight for this exchange
                    current = 0
                    for k in cls._private_ws_clients.keys():
                        if k.startswith(f"{base_key}:"):
                            current += 1
                    # include in-flight creations for this exchange
                    inflight_for_exch = sum(1 for c in _CREATION_INFLIGHT if c.startswith(f"{base_key}:"))
                    if (current + inflight_for_exch) >= cap:
                        try:
                            msg = f"get_private_ws_client: reached cap for {base_key} ({current + inflight_for_exch}/{cap}); returning None to allow REST fallback for user={user.name}"
                            if caller:
                                msg = msg + f" caller={caller}"
                            _human_log('Exchange', msg, level='WARNING', user=user)
                        except Exception:
                            pass
                        return None
                if not hasattr(ccxt_pro, ex_id):
                    return None
                kwargs = {'enableRateLimit': True, 'timeout': DEFAULT_CCXT_TIMEOUT_MS, 'options': {'defaultType': 'swap'}}
                # attach user creds
                if getattr(user, 'key', None):
                    kwargs['apiKey'] = getattr(user, 'key')
                if getattr(user, 'secret', None):
                    kwargs['secret'] = getattr(user, 'secret')
                if getattr(user, 'passphrase', None):
                    kwargs['password'] = getattr(user, 'passphrase')
                if getattr(user, 'wallet_address', None):
                    kwargs['walletAddress'] = getattr(user, 'wallet_address')
                if getattr(user, 'private_key', None):
                    kwargs['privateKey'] = getattr(user, 'private_key')

                # Apply recvWindow and time-difference adjustment for all clients
                try:
                    kwargs.setdefault('options', {}).setdefault('recvWindow', 10000)
                    kwargs.setdefault('options', {}).setdefault('adjustForTimeDifference', True)
                    # HIP-3: Configure Hyperliquid to include stock perpetuals
                    if ex_id == 'hyperliquid':
                        kwargs.setdefault('options', {})['fetchMarkets'] = {
                            'types': ['swap', 'hip3'],
                            'hip3': {
                                'dexes': [],  # Empty = auto-discover all HIP-3 DEXes
                            }
                        }
                except Exception:
                    pass

                # Reserve an in-flight slot so other concurrent creators count it
                _CREATION_INFLIGHT.add(key)
                try:
                    ex = getattr(ccxt_pro, ex_id)(kwargs)

                    # Attempt to load markets once for this client; treat failures as non-fatal
                    try:
                        lm = ex.load_markets()
                        if _asyncio.iscoroutine(lm):
                            await lm
                        else:
                            await _asyncio.to_thread(ex.load_markets)
                    except Exception:
                        # If load_markets fails, still keep the client but do not cache markets flag
                        pass

                    cls._private_ws_clients[key] = ex
                    return ex
                finally:
                    # Always remove in-flight marker so counts remain correct
                    try:
                        _CREATION_INFLIGHT.discard(key)
                    except Exception:
                        pass

    @classmethod
    async def close_private_ws_client(cls, id: str, user: User):
        """Close and remove a private ws client for the given exchange id and user."""
        if not user:
            return
        base_key = "kucoinfutures" if id == "kucoin" else id
        key = f"{base_key}:{user.name}"
        client = cls._private_ws_clients.pop(key, None)
        if client:
            try:
                await client.close()
            except Exception:
                pass
            # Notify listeners that a private client for this exchange/user was closed
            try:
                _notify_private_client_closed(base_key, user.name)
            except Exception:
                pass

    @classmethod
    async def _prune_private_ws_clients(cls, global_max=None, per_exchange=None):
        """Close excess private ws clients to satisfy new caps.

        This prefers to trim clients from exchanges that exceed their per-exchange
        caps first, then trims arbitrarily to satisfy a reduced global cap.
        """
        try:
            # determine effective caps
            try:
                gcap = int(global_max) if global_max is not None else (_RUNTIME_MAX_PRIVATE_WS_GLOBAL if _RUNTIME_MAX_PRIVATE_WS_GLOBAL is not None else MAX_PRIVATE_WS_GLOBAL)
            except Exception:
                gcap = MAX_PRIVATE_WS_GLOBAL
            pec = per_exchange if per_exchange is not None else (_RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE or {})

            # build per-exchange lists
            by_exch = {}
            for k in list(cls._private_ws_clients.keys()):
                exch = k.split(':', 1)[0]
                by_exch.setdefault(exch, []).append(k)

            # First trim per-exchange overages
            to_close = []
            for exch, keys in by_exch.items():
                try:
                    cap = None
                    if pec and exch in pec:
                        cap = int(pec.get(exch))
                    else:
                        cap = MAX_PRIVATE_WS_PER_EXCHANGE.get(exch)
                except Exception:
                    cap = MAX_PRIVATE_WS_PER_EXCHANGE.get(exch)
                if cap is None:
                    continue
                if len(keys) > cap:
                    # close oldest/any until at or below cap
                    excess = len(keys) - cap
                    to_close.extend(keys[:excess])

            # Then ensure global cap
            total_now = len(cls._private_ws_clients.keys())
            if gcap is not None and total_now - len(to_close) > gcap:
                need = (total_now - len(to_close)) - gcap
                # pick arbitrary remaining keys to close
                remaining_keys = [k for k in cls._private_ws_clients.keys() if k not in to_close]
                to_close.extend(remaining_keys[:need])

            # Close selected clients
            for k in to_close:
                try:
                    client = cls._private_ws_clients.pop(k, None)
                    if client:
                        try:
                            await client.close()
                        except Exception:
                            pass
                        # Notify listeners for this closed key
                        try:
                            parts = k.split(':', 1)
                            if parts:
                                exch = parts[0]
                                uname = parts[1] if len(parts) > 1 else None
                                _notify_private_client_closed(exch, uname)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    @classmethod
    async def close_shared_ws_client(cls, id: str):
        """Close and remove the shared ws client for an exchange id."""
        base_key = "kucoinfutures" if id == "kucoin" else id
        client = cls._shared_ws_clients.pop(base_key, None)
        if client:
            try:
                await client.close()
            except Exception:
                pass

    @classmethod
    async def close_all_ws_clients(cls):
        """Close all cached shared and private ws clients."""
        # Close private clients
        keys = list(cls._private_ws_clients.keys())
        for k in keys:
            client = cls._private_ws_clients.pop(k, None)
            if client:
                try:
                    await client.close()
                except Exception:
                    pass
                try:
                    parts = k.split(':', 1)
                    exch = parts[0]
                    uname = parts[1] if len(parts) > 1 else None
                    _notify_private_client_closed(exch, uname)
                except Exception:
                    pass
        # Close shared clients
        keys = list(cls._shared_ws_clients.keys())
        for k in keys:
            client = cls._shared_ws_clients.pop(k, None)
            if client:
                try:
                    await client.close()
                except Exception:
                    pass

    @classmethod
    def get_client_metrics(cls):
        """Return simple metrics about shared and private ws clients grouped by exchange.

        Returns a dict mapping exchange -> {'shared': bool, 'private_count': int}.
        """
        metrics = {}
        try:
            # Shared clients: keys are exchange ids
            for k in cls._shared_ws_clients.keys():
                metrics.setdefault(k, {'shared': 0, 'private_count': 0})
                metrics[k]['shared'] = 1
            # Private clients: keys are like '<exchange>:<user>'
            for k in cls._private_ws_clients.keys():
                exch = k.split(':', 1)[0]
                metrics.setdefault(exch, {'shared': 0, 'private_count': 0})
                metrics[exch]['private_count'] += 1
        except Exception:
            pass
        return metrics

    def fetch_ohlcv(self, symbol: str, market_type: str, timeframe: str, limit: int, since : int = None):
        if not self.instance: self.connect()
        if since:
            ohlcv = self.instance.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=limit)
        elif self.id == "hyperliquid":
            now = int(datetime.now().timestamp() * 1000)
            if timeframe[-1] == 'm':
                since = now - 1000 * 60 * int(timeframe[0:-1]) * limit
            elif timeframe[-1] == 'h':
                since = now - 1000 * 60 * 60 *int(timeframe[0:-1]) * limit
            elif timeframe[-1] == 'd':
                since = now - 1000 * 60 * 60 * 24 * int(timeframe[0:-1]) * limit
            elif timeframe[-1] == 'w':
                since = now - 1000 * 60 * 60 * 24 * 7 * int(timeframe[0:-1]) * limit
            elif timeframe[-1] == 'M':
                since = now - 1000 * 60 * 60 * 24 * 30 * int(timeframe[0:-1]) * limit
            ohlcv = self.instance.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=limit)
        else:
            ohlcv = self.instance.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)
        return ohlcv

    def fetch_price(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        # if symbol == "ADAUSDT_UMCBL":
        #     symbol = "ADA/USDT:USDT"
        price = self.instance.fetch_ticker(symbol=symbol)
        return price

    def fetch_prices(self, symbols: list, market_type: str):
        if not self.instance: self.connect()
        # Fix for Hyperliquid
        if self.id == "hyperliquid":
            try:
                if not getattr(self.instance, 'markets', None):
                    self.instance.load_markets()
            except Exception:
                pass
            fetched = self.instance.fetch(
                "https://api.hyperliquid.xyz/info",
                method="POST",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"type": "allMids"}),
            )
            prices = {}
            for symbol in symbols:
                base = symbol.split('/')[0] if '/' in symbol else symbol
                candidates = [base]

                # K-prefix variants (KPEPE -> kPEPE)
                if base.startswith('K') and len(base) > 1:
                    candidates.append('k' + base[1:])

                # DEX prefixed symbols (e.g. XYZ-TSLA, FLX-XMR)
                if '-' in base:
                    tail = base.split('-', 1)[1]
                    candidates.append(tail)
                    prefix = base.split('-', 1)[0].lower()
                    candidates.append(f"{prefix}:{tail}")

                # Generic case variants
                candidates.append(base.lower())
                candidates.append(base.upper())

                last = None
                for cand in candidates:
                    if cand in fetched:
                        last = fetched[cand]
                        break

                if last is not None:
                    prices[symbol] = {
                        "timestamp": int(datetime.now().timestamp() * 1000),
                        "last": last,
                    }
                    continue

                # Fallback to market metadata prices (available for HIP-3 and
                # some builder DEX symbols not keyed in allMids).
                market = None
                try:
                    market = self.instance.market(symbol)
                except Exception:
                    market = None
                info = (market or {}).get('info', {}) if market else {}
                md_price = info.get('markPx') or info.get('midPx') or info.get('oraclePx')
                if md_price is not None:
                    prices[symbol] = {
                        "timestamp": int(datetime.now().timestamp() * 1000),
                        "last": md_price,
                    }
        else:
            prices = self.instance.fetch_tickers(symbols=symbols)
        return prices

    def fetch_open_orders(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        if self.id == "bybit" and market_type == "spot":
            orders = self.instance.fetch_open_orders(symbol=symbol, params = {"type": market_type})
        elif self.id == 'bingx':
            orders = self.instance.fetch_open_orders(symbol=symbol)
        else:
            orders = self.instance.fetch_open_orders(symbol=symbol)
        return orders

    def fetch_all_open_orders(self, symbol: str):
        if not self.instance: self.connect()
        orders = self.instance.fetch_open_orders(symbol=symbol)
        return orders

    def fetch_position(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        if self.id in 'binance':
            position = self.instance.fetch_account_positions(symbols=[symbol])
            return position[0]
        elif self.id == 'bingx':
            positions = self.instance.fetch_positions(symbols=[symbol])
            for position in positions:
                if position["symbol"] == symbol:
                    return position
        else:
            position = self.instance.fetch_position(symbol=symbol)
            return position

    def fetch_positions(self):
        if not self.instance:
            self.connect()

        # Wrap fetch_positions with a small retry/backoff loop to handle
        # transient network timeouts on resource-constrained VPS instances.
        retries = 0
        max_retries = 3
        while True:
                try:
                    positions = self.instance.fetch_positions()
                    return positions
                except Exception as e:
                    # Convert common ccxt RequestTimeouts and socket timeouts into retries
                    msg = str(e).lower()
                    is_timeout = ('timed out' in msg or 'timeout' in msg or 'requesttimeout' in msg)
                    retries += 1
                    if not is_timeout or retries > max_retries:
                        # If non-timeout or we exhausted retries, raise the exception
                        raise
                    # Otherwise wait with exponential backoff and retry
                    delay = min(2 ** retries, 10)
                    try:
                        _human_log(
                            'Exchange',
                            f"fetch_positions timed out for {self.id}; retry {retries}/{max_retries} in {delay}s: {e}",
                            level='WARNING',
                            user=self.user,
                        )
                    except Exception:
                        pass
                    try:
                        sleep(delay)
                    except Exception:
                        pass

    def fetch_balance(self, market_type: str, symbol : str = None):
        if not self.instance: self.connect()
        try:
            balance = self.instance.fetch_balance(params = {"type": market_type})
        except Exception as e:
            return e
        if self.id == "hyperliquid":
            return float(balance["total"]["USDC"])
        if self.id == "bitget":
            return float(balance["info"][0]["available"])
        elif self.id == "bybit":
            if market_type == 'swap':
                balinfo = balance["info"]["result"]["list"][0]
                if balinfo["accountType"] == "UNIFIED":
                    return float(balinfo["totalWalletBalance"])
                elif "USDT" in balance["total"]:
                    return float(balance["total"]["USDT"])
                else:
                    return float(0)
            else:
                if symbol:
                    if symbol.endswith('USDT'):
                        symbol = symbol.replace("USDT", "")
                    elif symbol.endswith('USDC'):
                        symbol = symbol.replace("USDC", "")
                    elif symbol.endswith('BTC'):
                        symbol = symbol.replace("BTC", "")
                    elif symbol.endswith('EUR'):
                        symbol = symbol.replace("EUR", "")
                    return float(balance["total"][symbol])
                else:
                    if "USDT" in balance["total"]:
                        return float(balance["total"]["USDT"])
                    else:
                        return float(0)
        elif self.id == "binance":
            if market_type == 'swap': return float(balance["info"]["totalWalletBalance"])
            else:
                if symbol:
                    return float(balance["total"][symbol])
                else:
                    return float(balance["total"]["USDT"])
        elif self.id == "bingx":
            return float(balance["info"]["data"]["balance"]["balance"])
        return float(balance["total"]["USDT"])

    def fetch_timestamp(self):
        if not self.instance: self.connect()
        return self.instance.milliseconds()

    def fetch_spot(self, since: int = None):
        if self.user.key == 'key':
            return []
        all_histories = []
        all = []
        if not self.instance: self.connect()
        if self.id == "bybit":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 2 * 365 * day - day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 100
            end = since + week
            while True:
                trades = self.instance.fetch_my_trades(since=since, limit=limit, params = {'type': 'spot', "endTime": end})
                if trades:
                    first_trade = trades[0]
                    last_trade = trades[-1]
                    all_histories = trades + all_histories
                if len(trades) == limit:
                    print(f'User:{self.user.name} Fetched', len(trades), 'trades from', self.instance.iso8601(first_trade['timestamp']), 'till', self.instance.iso8601(last_trade['timestamp']))
                    end = trades[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = since + week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
            for history in all_histories:
                income = {}
                income["symbol"] = history["info"]["symbol"]
                income["timestamp"] = history["timestamp"]
                income["side"] = history["side"]
                income["income"] = history["cost"]
                income["fee"] = history["info"]["execFee"]
                income["uniqueid"] = history["info"]["orderId"]
                all.append(income)
        return all

    def save_income_other(self, history : list, exchange: str):
        dest = Path(f'{PBGDIR}/data/logs')
        if not dest.exists():
            dest.mkdir(parents=True)
        file = Path(f"{PBGDIR}/data/logs/income_other_{exchange}.json")
        with open(file, 'a') as f:
            json.dump(history, f, indent=4)

    def fetch_history(self, since: int = None):
        if self.user.key == 'key':
            return []
        all_histories = []
        all = []
        if not self.instance: self.connect()
        if self.id == "bybit":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 2 * 365 * day - day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 50
            end = since + week
            if self.instance.is_unified_enabled()[1]:
                UTA = True
            else:
                UTA = False
            cursor = None
            while True:
                for i in range(5):
                            try:
                                if UTA:
                                    transactions = self.instance.privateGetV5AccountTransactionLog(params = {"limit": limit, "startTime": since, "endTime": end, "cursor": cursor})
                                else:
                                    transactions = self.instance.privateGetV5AccountContractTransactionLog(params = {"limit": limit, "startTime": since, "endTime": end, "cursor": cursor})
                            except Exception as e:
                                _human_log(
                                    'Exchange',
                                    f"{e}",
                                    level='WARNING',
                                    user=self.user,
                                )
                                _human_log(
                                    'Exchange',
                                    f'Fetching transactions failed. Retry in 5 seconds',
                                    level='WARNING',
                                    user=self.user,
                                )
                                sleep(5)
                                continue
                cursor = transactions["result"]["nextPageCursor"]
                positions = transactions["result"]["list"]
                # print(positions)
                if positions:
                    first_position = positions[0]
                    last_position = positions[-1]
                    all_histories = positions + all_histories
                if cursor:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(positions)} transactions from "
                        f"{self.instance.iso8601(int(first_position['transactionTime']))} till "
                        f"{self.instance.iso8601(int(last_position['transactionTime']))}",
                        level='INFO',
                        user=self.user,
                    )
                else:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(positions)} transactions from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        level='INFO',
                        user=self.user,
                    )
                    since = since + week
                    end = since + week
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
            # print(all_histories)
            for history in all_histories:
                if history["type"] in ["TRADE","SETTLEMENT"]:
                    income = {}
                    income["symbol"] = history["symbol"]
                    income["timestamp"] = history["transactionTime"]
                    income["income"] = history["change"]
                    income["uniqueid"] = history["id"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        elif self.id == "hyperliquid":
            hour = 60 * 60 * 1000
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 365 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            else:
                # For make sure not to miss any funding or trading history
                since -= hour
            limit = 200
            # start with 1 week pages; on empty results double the page span up to 4 weeks
            initial_span = week
            max_span = week * 4
            page_span = initial_span
            end = since + page_span
            since_trades = since
            end_trades = end
            while True:
                try:
                    start_page_ts = time.time()
                    fundings = self.instance.fetch(
                        "https://api.hyperliquid.xyz/info",
                        method="POST",
                        headers={"Content-Type": "application/json"},
                        body=json.dumps({"type": "userFunding", "user": self.user.wallet_address, "startTime": since, "endTime": end}),
                    )
                    page_dur = time.time() - start_page_ts
                    try:
                        num = len(fundings) if fundings is not None else 0
                    except Exception:
                        num = 0
                    _human_log('Exchange', f"hyperliquid fundings page: user={getattr(self.user,'name',None)} since={since} end={end} span_ms={page_span} duration_s={page_dur:.3f} items={num}", level='DEBUG', user=self.user)
                except Exception as e:
                    import traceback as _tb
                    tb = _tb.format_exc()
                    _human_log('Exchange', f"fetch fundings ERROR for user={getattr(self.user,'name',None)} exchange={self.id} since={since} end={end}: {e}", level='ERROR', user=self.user)
                    _human_log('Exchange', f"Traceback:\n{tb}", level='DEBUG', user=self.user)
                    raise
                if fundings:
                    first_funding = fundings[0]
                    last_funding = fundings[-1]
                    all_histories = fundings + all_histories
                if len(fundings) == limit:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(fundings)} fundings from "
                        f"{self.instance.iso8601(int(first_funding['time']))} till "
                        f"{self.instance.iso8601(int(last_funding['time']))}",
                        level='INFO',
                        user=self.user,
                    )
                    since = int(fundings[-1]['time'])
                else:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(fundings)} fundings from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        level='INFO',
                        user=self.user,
                    )
                    # If the page is empty, increase the page span (double) up to max_span
                    if not fundings:
                        old_span = page_span
                        page_span = min(page_span * 2, max_span)
                        if page_span != old_span:
                            _human_log('Exchange', f"Empty fundings page — doubling page span from {old_span} to {page_span} ms for user={getattr(self.user,'name',None)}", level='DEBUG', user=self.user)
                    else:
                        # reset to initial span after receiving results
                        page_span = initial_span
                    since = end
                    end = since + page_span
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
                sleep(0.5)
            for history in all_histories:
                income = {}
                income["symbol"] = history["delta"]["coin"] + "USDC"
                income["timestamp"] = int(history["time"])
                income["income"] = history["delta"]["usdc"]
                income["uniqueid"] = f"{int(history['time'])}_{history['delta']['coin']}"
                all.append(income)
            since = since_trades
            end = end_trades
            all_histories = []
            # start trades with the initial page span (don't carry over fundings' growth)
            page_span = initial_span
            while True:
                try:
                    start_page_ts = time.time()
                    trades = self.instance.fetch_my_trades(since=since, limit=limit, params = {"endTime": end})
                    page_dur = time.time() - start_page_ts
                    try:
                        num = len(trades) if trades is not None else 0
                    except Exception:
                        num = 0
                    _human_log('Exchange', f"hyperliquid trades page: user={getattr(self.user,'name',None)} since={since} end={end} span_ms={page_span} duration_s={page_dur:.3f} items={num}", level='DEBUG', user=self.user)
                except Exception as e:
                    import traceback as _tb
                    tb = _tb.format_exc()
                    _human_log(
                        'Exchange',
                        f"fetch_my_trades ERROR for user={getattr(self.user,'name',None)} exchange={self.id} since={since} end={end} limit={limit}: {e}",
                        level='ERROR',
                        user=self.user,
                    )
                    # Log traceback and any ccxt last-response attributes available for debugging
                    _human_log('Exchange', f"Traceback:\n{tb}", level='DEBUG', user=self.user)
                    for attr in ('last_http_response', 'last_response', 'last_response_text', 'last_json_response', 'last_response_headers'):
                        try:
                            val = getattr(self.instance, attr, None)
                            if val is not None:
                                summary = str(val)
                                if len(summary) > 1000:
                                    summary = summary[:1000] + '...'
                                _human_log('Exchange', f"{attr}: {summary}", level='DEBUG', user=self.user)
                        except Exception:
                            pass
                    # Re-raise so caller can handle/log as before
                    raise
                # print(trades)
                if trades:
                    first_trade = trades[0]
                    last_trade = trades[-1]
                    all_histories = trades + all_histories
                if len(trades) == limit:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(trades)} trades from "
                        f"{self.instance.iso8601(first_trade['timestamp'])} till "
                        f"{self.instance.iso8601(last_trade['timestamp'])}",
                        level='INFO',
                        user=self.user,
                    )
                    since = trades[-1]['timestamp']
                else:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(trades)} trades from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        level='INFO',
                        user=self.user,
                    )
                    # If the page is empty, increase the page span (double) up to max_span
                    if not trades:
                        old_span = page_span
                        page_span = min(page_span * 2, max_span)
                        if page_span != old_span:
                            _human_log('Exchange', f"Empty trades page — doubling page span from {old_span} to {page_span} ms for user={getattr(self.user,'name',None)}", level='DEBUG', user=self.user)
                    else:
                        # reset to initial span after receiving results
                        page_span = initial_span
                    since = end
                    end = since + page_span
                    if since > now:
                        _human_log('Exchange', 'Done', level='INFO', user=self.user)
                        break
                sleep(0.5)
            # print(all_histories)
            for history in all_histories:
                # if history["side"] == "sell":
                income = {}
                income["symbol"] = history["info"]["coin"] + "USDC"
                income["timestamp"] = history["timestamp"]
                income["income"] = float(history["info"]["closedPnl"]) - float(history["info"]["fee"])
                income["uniqueid"] = history["info"]["tid"]
                all.append(income)
        elif self.id == "kucoinfutures":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 1 * 365 * day - day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 50
            end = since + day
            while True:
                positions = self.instance.futuresPrivateGetTransactionHistory(params = {"maxCount": limit, "startAt": since, "endAt": end})
                positions = positions["data"]["dataList"]
                if positions:
                    first_position = positions[0]
                    last_position = positions[-1]
                    all_histories = positions + all_histories
                if len(positions) == limit:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(positions)} income from "
                        f"{self.instance.iso8601(first_position['time'])} till "
                        f"{self.instance.iso8601(last_position['time'])}",
                        level='INFO',
                        user=self.user,
                    )
                    end = positions[-1]['time']
                else:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(positions)} income from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        level='INFO',
                        user=self.user,
                    )
                    since = since + day
                    end = since + day
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
            for history in all_histories:
                if history["type"] == "RealisedPNL":
                    income = {}
                    income["symbol"] = history["remark"][0:-2]
                    income["timestamp"] = history["time"]
                    income["income"] = history["amount"]
                    income["uniqueid"] = history["offset"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        elif self.id == "okx":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 120 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 100
            end = since + week
            while True:
                ledgers = self.instance.fetch_ledger(since=since, limit=limit, params = {"method": "privateGetAccountBillsArchive", "instType": "SWAP", "end": end})
                if ledgers:
                    first_ledger = ledgers[0]
                    last_ledger = ledgers[-1]
                    all_histories = ledgers + all_histories
                if len(ledgers) == limit:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(ledgers)} ledgers from "
                        f"{self.instance.iso8601(first_ledger['timestamp'])} till "
                        f"{self.instance.iso8601(last_ledger['timestamp'])}",
                        level='INFO',
                        user=self.user,
                    )
                    end = ledgers[0]['timestamp']
                else:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(ledgers)} ledgers from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        level='INFO',
                        user=self.user,
                    )
                    since = since + week
                    end = since + week
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
                sleep(0.5)
            for history in all_histories:
                if history["type"] in ["trade","fee"]:
                    income = {}
                    # income["symbol"] = history["symbol"][0:-5].replace("/", "").replace("-", "")
                    income["symbol"] = history["info"]["instId"][0:-5].replace("/", "").replace("-", "")
                    income["timestamp"] = history["timestamp"]
                    income["income"] = history["amount"]
                    income["uniqueid"] = history["id"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        elif self.id == "bitget":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 365 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            # Clamp to exchange's maximum lookback window to avoid "since too old" errors.
            try:
                since = max(int(since), int(now - max))
            except Exception:
                pass
            limit = 100
            end = since + week
            while True:
                ledgers = None
                last_err = None
                for attempt in range(3):
                    try:
                        ledgers = self.instance.fetch_ledger(since=since, limit=limit, params = {"type": "swap", "endTime": end})
                        last_err = None
                        break
                    except Exception as e:
                        if not _ccxt_should_retry(self.instance, e):
                            raise
                        last_err = e
                        _human_log(
                            'Exchange',
                            f"bitget fetch_ledger error user={self.user.name} since={since} end={end} attempt={attempt+1}/3: {e}",
                            level='WARNING',
                            user=self.user,
                        )
                        sleep(min(2 ** attempt, 5))
                if last_err is not None:
                    raise last_err
                # print(ledgers)
                if ledgers:
                    first_ledger = ledgers[0]
                    last_ledger = ledgers[-1]
                    all_histories = ledgers + all_histories
                if len(ledgers) == limit:
                    _human_log('Exchange',
                        f"Fetched {len(ledgers)} ledgers from "
                        f"{self.instance.iso8601(first_ledger['timestamp'])} till "
                        f"{self.instance.iso8601(last_ledger['timestamp'])}",
                        user=self.user,
                    )
                    end = ledgers[0]['timestamp']
                else:
                    _human_log('Exchange',
                        f"Fetched {len(ledgers)} ledgers from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        user=self.user,
                    )
                    since = since + week
                    end = since + week
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
            for history in all_histories:
                # if history["info"]["symbol"] and history["info"]["amount"] != "0":
                if history["info"]["symbol"]:
                    if history["type"] in ["trade","fee"]:
                        income = {}
                        income["symbol"] = history["info"]["symbol"]
                        income["timestamp"] = history["timestamp"]
                        income["income"] = float(history["info"]["amount"]) + float(history["info"]["fee"])
                        income["uniqueid"] = history["info"]["billId"]
                        all.append(income)
                    else: 
                        self.save_income_other(history, self.user.name)
        elif self.id == "gateio":
            day = 24 * 60 * 60
            week = 7 * day
            max = 365 * day
            now = self.instance.seconds()
            if not since:
                since = now - max
            else:
                since = int(since / 1000)
            limit = 100
            end = since + week
            while True:
                ledgers = self.instance.fetch_ledger(since=since, limit=limit, params = {"type": "swap", "to": end})
                if ledgers:
                    first_ledger = ledgers[0]
                    last_ledger = ledgers[-1]
                    all_histories = ledgers + all_histories
                if len(ledgers) == limit:
                    _human_log('Exchange',
                        f"Fetched {len(ledgers)} ledgers from "
                        f"{self.instance.iso8601(first_ledger['timestamp'])} till "
                        f"{self.instance.iso8601(last_ledger['timestamp'])}",
                        user=self.user,
                    )
                    end = int(ledgers[0]['timestamp']/1000)
                else:
                    _human_log('Exchange',
                        f"Fetched {len(ledgers)} ledgers from "
                        f"{self.instance.iso8601(since*1000)} till {self.instance.iso8601(end*1000)}",
                        user=self.user,
                    )
                    since = since + week
                    end = since + week
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
            for history in all_histories:
                if history["info"]["contract"] and history["amount"] != "0":
                    if history["type"] in ["trade","fee"]:
                        income = {}
                        income["symbol"] = history["info"]["contract"].replace("_", "")
                        income["timestamp"] = history["timestamp"]
                        income["income"] = history["info"]["change"]
                        income["uniqueid"] = history["info"]["id"]
                        all.append(income)
                    else: 
                        self.save_income_other(history, self.user.name)
        elif self.id == "binance":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 240 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            # Clamp to exchange's maximum lookback window to avoid "since too old" errors.
            try:
                since = max(int(since), int(now - max))
            except Exception:
                pass
            limit = 1000
            end = since + week
            while True:
                imcomes = None
                last_err = None
                for attempt in range(3):
                    try:
                        imcomes = self.instance.fapiPrivateGetIncome({                        
                                                                "pageSize": "100",
                                                                "startTime": since,
                                                                "limit": limit,
                                                                "endTime": end,
                                                                "timestamp": self.instance.milliseconds()
                                                                })
                        last_err = None
                        break
                    except Exception as e:
                        if not _ccxt_should_retry(self.instance, e):
                            raise
                        last_err = e
                        _human_log(
                            'Exchange',
                            f"binance fapiPrivateGetIncome error user={self.user.name} since={since} end={end} attempt={attempt+1}/3: {e}",
                            level='WARNING',
                            user=self.user,
                        )
                        sleep(min(2 ** attempt, 5))
                if last_err is not None:
                    raise last_err
                if imcomes:
                    first_imcome = imcomes[0]
                    last_imcome = imcomes[-1]
                    all_histories = imcomes + all_histories
                if len(imcomes) == limit:
                    _human_log('Exchange',
                        f"Fetched {len(imcomes)} incomes from "
                        f"{self.instance.iso8601(int(first_imcome['time']))} till "
                        f"{self.instance.iso8601(int(last_imcome['time']))}",
                        user=self.user,
                    )
                    since = int(imcomes[-1]['time'])
                else:
                    _human_log('Exchange',
                        f"Fetched {len(imcomes)} incomes from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        user=self.user,
                    )
                    since = end
                    end = since + week
                if since > now:
                    _human_log('Exchange', 'Done', level='INFO', user=self.user)
                    break
            for history in all_histories:
                if history["incomeType"] in ["REALIZED_PNL", "COMMISSION", "FUNDING_FEE"]:
                    income = {}
                    income["symbol"] = history["symbol"]
                    income["timestamp"] = history["time"]
                    income["income"] = history["income"]
                    if history["incomeType"] == "REALIZED_PNL":
                        income["uniqueid"] = history["tradeId"]
                    else:
                        income["uniqueid"] = history["tranId"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        return all

    def fetch_executions(self, since: int = None, symbols: list[str] | None = None):
        """Fetch execution-level trades/fills.

        Returns list of dicts with keys:
          symbol, timestamp, side, price, qty, fee, realized_pnl, order_id, trade_id, raw_json
        """
        if self.user.key == 'key':
            return []
        if not self.instance:
            self.connect()

        if self.id == "hyperliquid":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max_age = 365 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max_age

            limit = 200
            initial_span = week
            max_span = week * 4
            page_span = initial_span
            end = since + page_span

            all_histories = []
            while True:
                trades = self.instance.fetch_my_trades(since=since, limit=limit, params={"endTime": end})
                if trades:
                    first_trade = trades[0]
                    last_trade = trades[-1]
                    all_histories = trades + all_histories
                if len(trades) == limit:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(trades)} trades from "
                        f"{self.instance.iso8601(first_trade['timestamp'])} till "
                        f"{self.instance.iso8601(last_trade['timestamp'])}",
                        level='INFO',
                        user=self.user,
                    )
                    since = trades[-1]['timestamp']
                else:
                    _human_log(
                        'Exchange',
                        f"Fetched {len(trades)} trades from "
                        f"{self.instance.iso8601(since)} till {self.instance.iso8601(end)}",
                        level='INFO',
                        user=self.user,
                    )
                    if not trades:
                        page_span = min(page_span * 2, max_span)
                    else:
                        page_span = initial_span
                    since = end
                    end = since + page_span
                    if since > now:
                        _human_log('Exchange', 'Done', level='INFO', user=self.user)
                        break
                sleep(0.5)

            def _to_float(val):
                try:
                    if val is None:
                        return None
                    return float(val)
                except Exception:
                    return None

            executions = []
            for t in all_histories:
                info = t.get('info') or {}
                trade_id = None
                try:
                    trade_id = info.get('tid') or t.get('id')
                except Exception:
                    trade_id = None
                if not trade_id:
                    continue

                symbol = t.get('symbol')
                if not symbol:
                    try:
                        coin = info.get('coin')
                        symbol = f"{coin}USDC" if coin else ''
                    except Exception:
                        symbol = ''

                fee = _to_float(info.get('fee'))
                if fee is None:
                    try:
                        fee_obj = t.get('fee') or {}
                        fee = _to_float(fee_obj.get('cost'))
                    except Exception:
                        fee = None

                price = _to_float(t.get('price')) or _to_float(info.get('px'))
                qty = _to_float(t.get('amount')) or _to_float(info.get('sz'))
                realized_pnl = _to_float(info.get('closedPnl'))
                side = t.get('side') or info.get('side')
                order_id = info.get('orderId') or info.get('oid') or t.get('order')

                try:
                    raw_json = json.dumps(t, default=str)
                except Exception:
                    raw_json = None

                executions.append({
                    'symbol': symbol,
                    'timestamp': t.get('timestamp'),
                    'side': side,
                    'price': price,
                    'qty': qty,
                    'fee': fee,
                    'realized_pnl': realized_pnl,
                    'order_id': order_id,
                    'trade_id': str(trade_id),
                    'raw_json': raw_json,
                })
            return executions

        if self.id == "binance":
            # Binance futures: ccxt requires symbol for private trades.
            # Caller (Database) supplies `symbols` discovered from income/history.
            if not symbols:
                return []

            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max_age = 240 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max_age
            try:
                since = max(int(since), int(now - max_age))
            except Exception:
                pass

            executions = []
            for sym_id in symbols:
                try:
                    sym_id = str(sym_id).strip()
                    if not sym_id:
                        continue

                    ccxt_symbol = self.symbol_to_exchange_symbol(sym_id, 'swap')
                    if not ccxt_symbol:
                        continue

                    cursor = int(since)
                    effective_max_age = int(max_age)
                    while cursor < now:
                        end_time = min(cursor + week, now)
                        trades = None
                        last_err = None
                        for attempt in range(3):
                            try:
                                trades = self.instance.fetch_my_trades(
                                    ccxt_symbol,
                                    cursor,
                                    None,
                                    {
                                        'endTime': int(end_time),
                                    },
                                )
                                last_err = None
                                break
                            except Exception as e:
                                if not _ccxt_should_retry(self.instance, e):
                                    raise
                                last_err = e
                                _human_log(
                                    'Exchange',
                                    f"binance fetch_my_trades error symbol={ccxt_symbol} attempt={attempt+1}/3: {e}",
                                    level='WARNING',
                                    user=self.user,
                                )
                                sleep(min(2 ** attempt, 5))
                        if last_err is not None:
                            raise last_err

                        if trades:
                            max_ts = cursor
                            for t in trades:
                                try:
                                    ts = int(t.get('timestamp') or 0)
                                    if ts <= 0:
                                        continue
                                    if ts > max_ts:
                                        max_ts = ts

                                    info = t.get('info') if isinstance(t.get('info'), dict) else {}

                                    fee_cost = None
                                    try:
                                        fee_obj = t.get('fee')
                                        if isinstance(fee_obj, dict):
                                            fee_cost = fee_obj.get('cost')
                                        else:
                                            fee_cost = fee_obj
                                        if fee_cost is not None:
                                            fee_cost = float(fee_cost)
                                    except Exception:
                                        fee_cost = None

                                    realized = None
                                    for k in ('realizedPnl', 'realizedProfit', 'realizedPNL', 'realized_pnl'):
                                        if k in info and info.get(k) not in (None, ''):
                                            try:
                                                realized = float(info.get(k))
                                            except Exception:
                                                realized = None
                                            break

                                    trade_id = t.get('id') or info.get('tradeId') or info.get('id')
                                    if not trade_id:
                                        # Avoid inserting rows without unique id.
                                        continue

                                    executions.append(
                                        {
                                            'symbol': t.get('symbol') or ccxt_symbol,
                                            'timestamp': ts,
                                            'side': t.get('side'),
                                            'price': t.get('price'),
                                            'qty': t.get('amount'),
                                            'fee': fee_cost,
                                            'realized_pnl': realized,
                                            'order_id': t.get('order') or info.get('orderId') or info.get('order_id'),
                                            'trade_id': str(trade_id),
                                            'raw_json': json.dumps(t, ensure_ascii=False, default=str),
                                        }
                                    )
                                except Exception:
                                    continue

                            cursor = int(max_ts) + 1
                        else:
                            cursor = int(end_time) + 1

                except Exception:
                    continue

            return executions

        if self.id == "bybit":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max_age_total = 2 * 365 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max_age_total
            try:
                since = max(int(since), int(now - max_age_total))
            except Exception:
                pass

            def _to_float(val):
                try:
                    if val is None or val == '':
                        return None
                    return float(val)
                except Exception:
                    return None

            def _extract_next_page_cursor(trades: list[dict]) -> str | None:
                # CCXT Bybit often attaches response metadata into `trade['info']`.
                try:
                    for t in reversed(trades or []):
                        info = t.get('info') if isinstance(t.get('info'), dict) else None
                        if info and info.get('nextPageCursor'):
                            nxt = str(info.get('nextPageCursor'))
                            return nxt if nxt.strip() else None
                except Exception:
                    pass
                # Fallback: inspect last_json_response
                try:
                    lj = getattr(self.instance, 'last_json_response', None)
                    if isinstance(lj, dict):
                        res = lj.get('result')
                        if isinstance(res, dict) and res.get('nextPageCursor'):
                            nxt = str(res.get('nextPageCursor'))
                            return nxt if nxt.strip() else None
                except Exception:
                    pass
                return None

            executions = []
            # Bybit V5 supports querying executions without symbol; the API enforces
            # endTime - startTime <= 7 days. We therefore fetch in fixed 7-day windows
            # and use cursor pagination within each window.

            cursor = int(since)
            seen_trade_ids: set[str] = set()

            while cursor < now:
                # Keep the range strictly below 7 days to avoid retCode 10001.
                end_time = min(int(cursor + week - 1), int(now))

                # First page for this time window
                trades = None
                last_err = None
                for attempt in range(3):
                    try:
                        trades = self.instance.fetch_my_trades(
                            symbol=None,
                            since=int(cursor),
                            limit=100,
                            params={
                                'type': 'swap',
                                'endTime': int(end_time),
                            },
                        )
                        last_err = None
                        break
                    except Exception as e:
                        if not _ccxt_should_retry(self.instance, e):
                            raise
                        last_err = e
                        _human_log(
                            'Exchange',
                            f"bybit fetch_my_trades error (global) attempt={attempt+1}/3: {e}",
                            level='WARNING',
                            user=self.user,
                        )
                        sleep(min(2 ** attempt, 5))
                if last_err is not None:
                    raise last_err

                window_trades: list[dict] = []
                if trades:
                    window_trades.extend(trades)

                    # Cursor pagination within the window
                    seen_page_cursors: set[str] = set()
                    next_cur = _extract_next_page_cursor(trades)
                    while next_cur and next_cur not in seen_page_cursors:
                        seen_page_cursors.add(next_cur)
                        page = None
                        page_err = None
                        for attempt in range(3):
                            try:
                                page = self.instance.fetch_my_trades(
                                    symbol=None,
                                    since=int(cursor),
                                    limit=100,
                                    params={
                                        'type': 'swap',
                                        'cursor': str(next_cur),
                                        'endTime': int(end_time),
                                    },
                                )
                                page_err = None
                                break
                            except Exception as e:
                                if not _ccxt_should_retry(self.instance, e):
                                    raise
                                page_err = e
                                _human_log(
                                    'Exchange',
                                    f"bybit fetch_my_trades page error (global) attempt={attempt+1}/3: {e}",
                                    level='WARNING',
                                    user=self.user,
                                )
                                sleep(min(2 ** attempt, 5))
                        if page_err is not None:
                            raise page_err
                        if not page:
                            break
                        window_trades.extend(page)
                        next_cur = _extract_next_page_cursor(page)

                if window_trades:
                    for t in window_trades:
                        try:
                            ts = int(t.get('timestamp') or 0)
                            if ts <= 0:
                                continue

                            info = t.get('info') if isinstance(t.get('info'), dict) else {}
                            trade_id = (
                                t.get('id')
                                or info.get('execId')
                                or info.get('tradeId')
                                or info.get('id')
                            )
                            if not trade_id:
                                continue
                            trade_id = str(trade_id)
                            if trade_id in seen_trade_ids:
                                continue
                            seen_trade_ids.add(trade_id)

                            # Fee: ccxt can report negative fees on Bybit; prefer info.execFee when available.
                            fee_cost = None
                            if info.get('execFee') not in (None, ''):
                                fee_cost = _to_float(info.get('execFee'))
                            if fee_cost is None:
                                try:
                                    fee_obj = t.get('fee')
                                    if isinstance(fee_obj, dict):
                                        fee_cost = _to_float(fee_obj.get('cost'))
                                    else:
                                        fee_cost = _to_float(fee_obj)
                                except Exception:
                                    fee_cost = None
                            if fee_cost is not None:
                                try:
                                    fee_cost = abs(float(fee_cost))
                                except Exception:
                                    pass

                            realized = None
                            for k in (
                                'closedPnl', 'execPnl', 'realisedPnl', 'realizedPnl', 'realizedProfit', 'pnl', 'profit'
                            ):
                                if k in info and info.get(k) not in (None, ''):
                                    realized = _to_float(info.get(k))
                                    break

                            executions.append(
                                {
                                    'symbol': t.get('symbol') or '',
                                    'timestamp': ts,
                                    'side': t.get('side') or info.get('side'),
                                    'price': _to_float(t.get('price')),
                                    'qty': _to_float(t.get('amount')),
                                    'fee': fee_cost,
                                    'realized_pnl': realized,
                                    'order_id': t.get('order') or info.get('orderId') or info.get('order_id'),
                                    'trade_id': trade_id,
                                    'raw_json': json.dumps(t, ensure_ascii=False, default=str),
                                }
                            )
                        except Exception:
                            continue

                cursor = int(end_time) + 1
                sleep(0.2)

            return executions

        if self.id == "okx":
            # OKX supports fetchMyTrades without symbol (global fills), but CCXT's
            # implementation is backed by fills-history with ~90-day lookback.
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max_age_total = 90 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max_age_total
            try:
                since = max(int(since), int(now - max_age_total))
            except Exception:
                pass

            def _to_float(val):
                try:
                    if val is None or val == '':
                        return None
                    return float(val)
                except Exception:
                    return None

            executions = []
            cursor = int(since)
            seen_trade_ids: set[str] = set()

            # OKX returns at most 100 fills per request. We slice by time window and
            # shrink the window if we hit the cap, to avoid missing fills in dense periods.
            initial_span = week
            max_span = week * 4
            min_span = 60 * 60 * 1000  # 1 hour
            page_span = initial_span

            while cursor < now:
                span = int(page_span)
                trades = None
                end_time = None

                # Adaptive span shrinking when we hit the endpoint cap.
                while True:
                    end_time = min(int(cursor + span - 1), int(now))

                    last_err = None
                    for attempt in range(3):
                        try:
                            # Use market type routing via ccxt "type" param, and "until" for end.
                            trades = self.instance.fetch_my_trades(
                                symbol=None,
                                since=int(cursor),
                                limit=100,
                                params={
                                    'type': 'swap',
                                    'until': int(end_time),
                                },
                            )
                            last_err = None
                            break
                        except Exception as e:
                            if not _ccxt_should_retry(self.instance, e):
                                raise
                            last_err = e
                            _human_log(
                                'Exchange',
                                f"okx fetch_my_trades error (global) attempt={attempt+1}/3: {e}",
                                level='WARNING',
                                user=self.user,
                            )
                            sleep(min(2 ** attempt, 5))
                    if last_err is not None:
                        raise last_err

                    # If we hit the max-per-page cap, shrink window and retry.
                    try:
                        if trades and len(trades) >= 100 and span > min_span:
                            span = max(min_span, span // 2)
                            continue
                    except Exception:
                        pass
                    break

                if trades:
                    # Reset span after a non-empty page
                    page_span = initial_span
                    for t in trades:
                        try:
                            ts = int(t.get('timestamp') or 0)
                            if ts <= 0:
                                continue

                            info = t.get('info') if isinstance(t.get('info'), dict) else {}
                            trade_id = (
                                info.get('billId')
                                or t.get('id')
                                or info.get('tradeId')
                                or info.get('id')
                            )
                            if not trade_id:
                                continue
                            trade_id = str(trade_id)
                            if trade_id in seen_trade_ids:
                                continue
                            seen_trade_ids.add(trade_id)

                            fee_cost = None
                            try:
                                # OKX may return fee as negative; store as positive cost.
                                if info.get('fee') not in (None, ''):
                                    fee_cost = _to_float(info.get('fee'))
                                if fee_cost is None:
                                    fee_obj = t.get('fee')
                                    if isinstance(fee_obj, dict):
                                        fee_cost = _to_float(fee_obj.get('cost'))
                                    else:
                                        fee_cost = _to_float(fee_obj)
                                if fee_cost is not None:
                                    fee_cost = abs(float(fee_cost))
                            except Exception:
                                fee_cost = None

                            symbol = t.get('symbol')
                            if not symbol:
                                try:
                                    symbol = str(info.get('instId') or '')
                                except Exception:
                                    symbol = ''

                            executions.append(
                                {
                                    'symbol': symbol or '',
                                    'timestamp': ts,
                                    'side': t.get('side') or info.get('side'),
                                    'price': _to_float(t.get('price')) or _to_float(info.get('fillPx')),
                                    'qty': _to_float(t.get('amount')) or _to_float(info.get('fillSz')),
                                    'fee': fee_cost,
                                    'realized_pnl': None,
                                    'order_id': t.get('order') or info.get('ordId') or info.get('orderId') or info.get('order_id'),
                                    'trade_id': trade_id,
                                    'raw_json': json.dumps(t, ensure_ascii=False, default=str),
                                }
                            )
                        except Exception:
                            continue
                else:
                    # If empty, expand the span a bit to reduce request count.
                    try:
                        page_span = min(int(page_span) * 2, int(max_span))
                    except Exception:
                        page_span = initial_span

                cursor = int(end_time) + 1
                sleep(0.2)

            return executions

        if self.id == "gateio":
            # Gate.io supports fetching swap trades without symbol using `fetch_my_trades`.
            # The underlying endpoint caps results, so we slice time windows and shrink
            # the window when we hit the cap, to avoid missing executions.
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            # Gate docs for personal trades mention a default history window of ~6 months for the
            # non-time-range endpoint. PBGui uses 365 days as default only when `since` is not provided.
            default_max_age_total = 365 * day
            now = self.instance.milliseconds()
            if since is None:
                since = max(0, int(now - default_max_age_total))
            else:
                try:
                    since = max(0, int(since))
                except Exception:
                    since = max(0, int(now - default_max_age_total))

            def _to_float(val):
                try:
                    if val is None or val == '':
                        return None
                    return float(val)
                except Exception:
                    return None

            executions = []
            seen_trade_ids: set[str] = set()

            limit = 100
            # Window slicing is done in seconds because Gate.io uses `to` as seconds.
            initial_span_s = int(week / 1000)
            max_span_s = int(4 * week / 1000)
            min_span_s = 60 * 60  # 1 hour
            span_s = initial_span_s

            end_s = int(now / 1000)
            since_s = int(since / 1000)

            while end_s > since_s:
                start_s = max(since_s, end_s - int(span_s))

                trades = None
                last_err = None
                for attempt in range(6):
                    try:
                        trades = self.instance.fetch_my_trades(
                            symbol=None,
                            since=int(start_s * 1000),
                            limit=int(limit),
                            params={
                                'type': 'swap',
                                'to': int(end_s),
                            },
                        )
                        last_err = None
                        break
                    except Exception as e:
                        if not _ccxt_should_retry(self.instance, e):
                            raise
                        last_err = e
                        _human_log(
                            'Exchange',
                            f"gateio fetch_my_trades error (global) attempt={attempt+1}/6: {e}",
                            level='WARNING',
                            user=self.user,
                        )
                        sleep(min(2 ** attempt, 30))
                if last_err is not None:
                    raise last_err

                # Extra safety: Gate.io private endpoints can be stricter than ccxt's internal limiter.
                try:
                    rl_ms = int(getattr(self.instance, 'rateLimit', 0) or 0)
                except Exception:
                    rl_ms = 0
                if rl_ms > 0:
                    sleep(max(0.5, rl_ms / 1000.0))

                # If we hit the cap, shrink the time window and retry.
                try:
                    if trades and len(trades) >= limit and span_s > min_span_s:
                        span_s = max(min_span_s, span_s // 2)
                        continue
                except Exception:
                    pass

                if trades:
                    span_s = initial_span_s
                    for t in trades:
                        try:
                            ts = int(t.get('timestamp') or 0)
                            if ts <= 0:
                                continue

                            info = t.get('info') if isinstance(t.get('info'), dict) else {}
                            trade_id = (
                                t.get('id')
                                or info.get('trade_id')
                                or info.get('id')
                            )
                            if not trade_id:
                                continue
                            trade_id = str(trade_id)
                            if trade_id in seen_trade_ids:
                                continue
                            seen_trade_ids.add(trade_id)

                            fee_cost = None
                            try:
                                fee_obj = t.get('fee')
                                if isinstance(fee_obj, dict):
                                    fee_cost = _to_float(fee_obj.get('cost'))
                                else:
                                    fee_cost = _to_float(fee_obj)
                                if fee_cost is not None:
                                    fee_cost = abs(float(fee_cost))
                            except Exception:
                                fee_cost = None

                            executions.append(
                                {
                                    'symbol': t.get('symbol') or str(info.get('contract') or ''),
                                    'timestamp': ts,
                                    'side': t.get('side') or info.get('side'),
                                    'price': _to_float(t.get('price')),
                                    'qty': _to_float(t.get('amount')),
                                    'fee': fee_cost,
                                    'realized_pnl': None,
                                    'order_id': t.get('order') or info.get('order_id') or info.get('orderId'),
                                    'trade_id': trade_id,
                                    'raw_json': json.dumps(t, ensure_ascii=False, default=str),
                                }
                            )
                        except Exception:
                            continue
                else:
                    # Empty window: expand span to reduce request count.
                    try:
                        span_s = min(int(span_s) * 2, int(max_span_s))
                    except Exception:
                        span_s = initial_span_s

                end_s = int(start_s) - 1
                sleep(0.5)

            return executions

        if self.id == "bitget":
            # Bitget: safest to fetch per-symbol (keeps requests bounded and avoids spot/swap ambiguity).
            # Caller (Database) supplies `symbols` discovered from income/history.
            if not symbols:
                return []

            day = 24 * 60 * 60 * 1000
            max_age_total = 365 * day
            page_span = 90 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max_age_total
            try:
                since = max(int(since), int(now - max_age_total))
            except Exception:
                pass

            def _to_float(val):
                try:
                    if val is None or val == '':
                        return None
                    return float(val)
                except Exception:
                    return None

            def _bitget_order_side(raw_side: str | None, trade_side: str | None) -> str | None:
                """Bitget swap trade semantics.

                In Bitget swap trades we commonly see:
                - info.side: 'buy'/'sell' (position direction)
                - info.tradeSide: 'open'/'close'

                For our matching logic we want the actual order side (buy/sell):
                - open: same as info.side
                - close: opposite of info.side
                """

                try:
                    rs = str(raw_side or '').strip().lower()
                    ts = str(trade_side or '').strip().lower()
                    if rs not in ('buy', 'sell'):
                        return rs or None
                    if ts == 'close':
                        return 'sell' if rs == 'buy' else 'buy'
                    if ts == 'open':
                        return rs
                    return rs
                except Exception:
                    return None

            executions = []
            for sym_id in symbols:
                try:
                    sym_id = str(sym_id).strip()
                    if not sym_id:
                        continue

                    ccxt_symbol = self.symbol_to_exchange_symbol(sym_id, 'swap')
                    if not ccxt_symbol:
                        continue

                    cursor = int(since)
                    # Avoid missing trades when many fills share the same timestamp.
                    # Bitget may return >limit trades at the same millisecond; if we advance
                    # `cursor = max_ts + 1` we can skip remaining trades at `max_ts`.
                    # Keep cursor inclusive and dedupe by trade_id.
                    seen_trade_ids: set[str] = set()
                    while cursor < now:
                        end_time = min(cursor + page_span, now)
                        trades = None
                        last_err = None
                        for attempt in range(3):
                            try:
                                trades = self.instance.fetch_my_trades(
                                    symbol=ccxt_symbol,
                                    since=int(cursor),
                                    limit=100,
                                    params={
                                        'type': 'swap',
                                        'endTime': int(end_time),
                                    },
                                )
                                last_err = None
                                break
                            except Exception as e:
                                if not _ccxt_should_retry(self.instance, e):
                                    raise
                                last_err = e
                                _human_log(
                                    'Exchange',
                                    f"bitget fetch_my_trades error symbol={ccxt_symbol} attempt={attempt+1}/3: {e}",
                                    level='WARNING',
                                    user=self.user,
                                )
                                sleep(min(2 ** attempt, 5))
                        if last_err is not None:
                            # Abort instead of returning partial executions; caller should retry later.
                            raise last_err

                        if trades:
                            max_ts = cursor
                            n_new = 0
                            for t in trades:
                                try:
                                    ts = int(t.get('timestamp') or 0)
                                    if ts <= 0:
                                        continue
                                    if ts > max_ts:
                                        max_ts = ts

                                    info = t.get('info') if isinstance(t.get('info'), dict) else {}

                                    trade_id = t.get('id') or info.get('tradeId') or info.get('id')
                                    if not trade_id:
                                        continue
                                    trade_id = str(trade_id)
                                    if trade_id in seen_trade_ids:
                                        continue
                                    seen_trade_ids.add(trade_id)
                                    n_new += 1

                                    fee_cost = None
                                    try:
                                        fee_obj = t.get('fee')
                                        if isinstance(fee_obj, dict):
                                            fee_cost = fee_obj.get('cost')
                                        else:
                                            fee_cost = fee_obj
                                        fee_cost = _to_float(fee_cost)
                                    except Exception:
                                        fee_cost = None

                                    realized = None
                                    for k in (
                                        'realizedPnl', 'realizedPNL', 'realizedProfit', 'pnl', 'profit', 'closedPnl'
                                    ):
                                        if k in info and info.get(k) not in (None, ''):
                                            realized = _to_float(info.get(k))
                                            break

                                    executions.append(
                                        {
                                            'symbol': t.get('symbol') or ccxt_symbol,
                                            'timestamp': ts,
                                            'side': _bitget_order_side(t.get('side') or info.get('side'), info.get('tradeSide')),
                                            'price': _to_float(t.get('price')),
                                            'qty': _to_float(t.get('amount')),
                                            'fee': fee_cost,
                                            'realized_pnl': realized,
                                            'order_id': t.get('order') or info.get('orderId') or info.get('order_id') or info.get('orderId'),
                                            'trade_id': trade_id,
                                            'raw_json': json.dumps(t, ensure_ascii=False, default=str),
                                        }
                                    )
                                except Exception:
                                    continue

                            # If we got no new trade_ids, force progress by jumping beyond end_time.
                            if n_new == 0:
                                cursor = int(end_time) + 1
                            else:
                                # Keep cursor inclusive to avoid skipping trades at max_ts.
                                if int(max_ts) <= int(cursor):
                                    cursor = int(cursor) + 1
                                else:
                                    cursor = int(max_ts)
                        else:
                            cursor = int(end_time) + 1

                except Exception:
                    continue

            return executions

        return []
    
    def fetch_trades(self, symbol: str, market_type: str, since: int):
        all_trades = []
        last_trade_id = ""
        if not self.instance: self.connect()
        if self.instance.has['fetchMyTrades'] or self.instance.has['fetchTrades']:
            end_time = self.instance.milliseconds()
            # With ccxt >= 4.1.7 we can use pagination in one line
            # trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "paginate": True, "paginationDirection": "forward", "until": self.instance.milliseconds()})
            if self.id == "binance":
                if market_type == "futures":
                    week = 7 * 24 * 60 * 60 * 1000
                else:
                    week = 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                all_trades = []
                if since == 1577840461000:
                    first_trade = self.instance.fetch_my_trades(symbol, None, None, {'fromId': 0})
                    if first_trade:
                        since = first_trade[0]["timestamp"]
                while since < now:
                    _human_log('Exchange', f'Symbol:{symbol} Fetching trades from {self.instance.iso8601(since)}', level='INFO', user=self.user)
                    end_time = since + week
                    if end_time > now:
                        end_time = now
                    trades = self.instance.fetch_my_trades(symbol, since, None, {
                        'endTime': end_time,
                    })
                    if len(trades):
                        last_trade = trades[len(trades) - 1]
                        since = last_trade['timestamp'] + 1
                        all_trades = all_trades + trades
                    else:
                        since = end_time
            elif self.id == "bybit":
                day = 24 * 60 * 60 * 1000
                week = 7 * day
                year = 365 * day
                now = self.instance.milliseconds()
                all_trades = []
                if since == 1577840461000:
                    since = now - 2 * year + day
                    end_time = since + week
                    first_trade = self.instance.fetch_my_trades(symbol, since, 100, params = {'type': market_type, "paginate": True, 'endTime': end_time })
                    if first_trade:
                        since = first_trade[0]["timestamp"]
                while since < now:
                    _human_log('Exchange', f'Symbol:{symbol} Fetching trades from {self.instance.iso8601(since)}', level='INFO', user=self.user)
                    end_time = since + week
                    if end_time > now:
                        end_time = now
                    trades = self.instance.fetch_my_trades(symbol, since, 100, params = {'type': market_type, 'endTime': end_time })
                    if len(trades):
                        last_trade = trades[len(trades) - 1]
                        if "nextPageCursor" in last_trade["info"]:
                            cursor = last_trade["info"]["nextPageCursor"]
                            while True:
                                _human_log('Exchange', f'Symbol:{symbol} Fetching trades from {cursor}', level='INFO', user=self.user)
                                all_trades = all_trades + trades
                                trades = self.instance.fetch_my_trades(symbol, since, 100, params = {'type': market_type, 'cursor': cursor, 'endTime': end_time })
                                if len(trades):
                                    lpage = trades[len(trades) - 1]
                                    if "nextPageCursor" in lpage["info"]:
                                        cursor = lpage["info"]["nextPageCursor"]
                                    else:
                                        break
                                else:
                                    break
                        since = last_trade['timestamp'] + 1
                        all_trades = all_trades + trades
                    else:
                        since = end_time
            elif self.id == "kucoinfutures":
                week = 7 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                limit = 50
                end = since + week
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"endAt": end})
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {first_trade["timestamp"]} till {last_trade["timestamp"]}', level='INFO', user=self.user)
                    if len(trades) == limit:
                        end = trades[0]['timestamp']
                    else:
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {self.instance.iso8601(since)} till {self.instance.iso8601(end)}', level='INFO', user=self.user)
                        since += week
                        end = since + week
                    if since > now:
                        _human_log('Exchange', f'Symbol:{symbol} Done', level='INFO', user=self.user)
                        break
            elif self.id == "okx":
                week = 7 * 24 * 60 * 60 * 1000
                max = 90 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                if since == 1577840461000:
                    since = now - max
                limit = 50
                end = since + week
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"end": end})
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {first_trade["timestamp"]} till {last_trade["timestamp"]}', level='INFO', user=self.user)
                    if len(trades) == limit:
                        end = trades[0]['timestamp']
                    else:
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {self.instance.iso8601(since)} till {self.instance.iso8601(end)}', level='INFO', user=self.user)
                        since += week
                        end = since + week
                    if since > now:
                        _human_log('Exchange', f'Symbol:{symbol} Done', level='INFO', user=self.user)
                        break
            elif self.id == "bingx":
                week = 7 * 24 * 60 * 60 * 1000
                max = 90 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                if since == 1577840461000:
                    since = now - max
                limit = 500
                end = since + week
                bingx_symbol = f'{symbol.split("/")[0]}-{symbol.split(":")[-1]}'
                while True:
                    now = self.instance.milliseconds()
                    orders = self.instance.swapV2PrivateGetTradeAllOrders({"symbol": bingx_symbol, "limit": limit, "startTime": since, "endTime": end, "timestamp": now})
                    trades = orders["data"]["orders"]
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {first_trade["time"]} till {last_trade["time"]}', level='INFO', user=self.user)
                    if len(trades) == limit:
                        since = int(trades[-1]['time'])
                    else:
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {self.instance.iso8601(since)} till {self.instance.iso8601(end)}', level='INFO', user=self.user)
                        since += week
                        end = since + week
                    if since > now:
                        _human_log('Exchange', f'Symbol:{symbol} Done', level='INFO', user=self.user)
                        break
                bingx_trades = []
                for trade in all_trades:
                    if trade["status"] == "FILLED":
                        trade["id"] = trade["orderId"]
                        trade["timestamp"] = int(trade["time"])
                        trade["amount"] = float(trade["executedQty"])
                        trade["fee"] = float(trade["commission"])
                        trade["price"] = float(trade["price"])
                        bingx_trades.append(trade)
                all_trades = bingx_trades
            elif self.id == "bitget":
                # week = 7 * 24 * 60 * 60 * 1000
                max = 90 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                end = since + max
                limit = 100
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"type": market_type, "endTime": end})
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {first_trade["timestamp"]} till {last_trade["timestamp"]}', level='INFO', user=self.user)
                    if len(trades) == limit:
                        end = trades[0]['timestamp']
                    else:
                        _human_log('Exchange', f'Symbol:{symbol} Fetched {len(trades)} trades from {self.instance.iso8601(since)} till {self.instance.iso8601(end)}', level='INFO', user=self.user)
                        since += max
                        end = since + max
                    if since > now:
                        _human_log('Exchange', f'Symbol:{symbol} Done', level='INFO', user=self.user)
                        break
        if all_trades:
            sort_trades = sorted(all_trades, key=lambda d: d['timestamp'])
            return sort_trades

    def symbol_to_exchange_symbol(self, symbol: str, market_type: str):
        if self.id == 'binance':
            if not self.instance: self.connect()
            if not self._markets: self._markets = self.instance.load_markets()
            for (k,v) in list(self._markets.items()):
                if market_type == "spot":
                    if v["id"] == symbol and v["spot"]:
                        return v["symbol"]
                if market_type == "swap":
                    if v["id"] == symbol and v["swap"]:
                        return v["symbol"]
        elif self.id == 'hyperliquid':
            return f'{symbol[0:-4]}/USDC:USDC'
        elif self.id == 'bitget':
            return f'{symbol[0:-4]}/USDT:USDT'
        elif self.id == 'kucoinfutures':
            return f'{symbol}M'
        elif self.id == 'okx':
            return f'{symbol[0:-4]}-USDT-SWAP'
        elif self.id == 'bingx':
            return f'{symbol[0:-4]}/USDT:USDT'
        else:
            if market_type == "spot":
                return f'{symbol[0:-4]}/USDT'
            else:
                return symbol

    def load_market(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        return self._markets

    def fetch_symbol_info(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        if not self._markets: self._markets = self.instance.load_markets()
        if market_type == "spot":
            symbol = f'{symbol[0:-4]}/USDT'
        else:
            if symbol[-4:] == 'USDC':
                symbol = f'{symbol[0:-4]}/USDC:USDC'
            else:
                symbol = f'{symbol[0:-4]}/USDT:USDT'
        symbol_info = self._markets[symbol]
        if self.id == 'binance':
            if market_type == "futures":
                min_costs = (
                    0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
                )
                min_qtys = symbol_info["limits"]["amount"]["min"]
                for felm in symbol_info["info"]["filters"]:
                    if felm["filterType"] == "PRICE_FILTER":
                        price_steps = float(felm["tickSize"])
                    elif felm["filterType"] == "MARKET_LOT_SIZE":
                        qty_steps = float(felm["stepSize"])
                c_mults = symbol_info["contractSize"]
            else:
                for q in symbol_info["info"]["filters"]:
                    if q["filterType"] == "LOT_SIZE":
                        min_qtys = symbol_info["min_qty"] = float(q["minQty"])
                        qty_steps = symbol_info["qty_step"] = float(q["stepSize"])
                    elif q["filterType"] == "PRICE_FILTER":
                        price_steps = symbol_info["price_step"] = float(q["tickSize"])
                    elif q["filterType"] == "NOTIONAL":
                        min_costs = symbol_info["min_cost"] = float(q["minNotional"])
                c_mults = 1.0
        elif self.id == 'bybit':
            if market_type == "futures":
                min_costs = (
                    0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
                )
                min_qtys = symbol_info["limits"]["amount"]["min"]
                qty_steps = symbol_info["precision"]["amount"]
                price_steps = symbol_info["precision"]["price"]
                c_mults = symbol_info["contractSize"]
            else:
                min_costs = (
                    0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
                )
                min_qtys = symbol_info["limits"]["amount"]["min"]
                qty_steps = symbol_info["precision"]["amount"]
                price_steps = symbol_info["precision"]["price"]
                c_mults = 1.0
        else:
            min_costs = max(
                5.1, 0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
            )
            min_qtys = symbol_info["limits"]["amount"]["min"]
            qty_steps = symbol_info["precision"]["amount"]
            price_steps = symbol_info["precision"]["price"]
            c_mults = symbol_info["contractSize"]
        return symbol_info, min_costs, min_qtys, price_steps, qty_steps, c_mults

    def fetch_copytrading_symbols(self):
        if not self.instance: self.connect()
        # print(self.instance.__dir__())
        cpSymbols = []
        if self.id == 'binance':
            users = Users()
            all_users = users.find_binance_users()
            if all_users:
                for user in all_users:
                    self.user = user
                    self.connect()
                    try:
                        symbols = self.instance.sapiGetCopytradingFuturesLeadsymbol()
                        if symbols and symbols.get("data"):
                            for symbol in symbols["data"]:
                                cpSymbols.append(symbol["symbol"])
                            break
                    except Exception as e:
                        _human_log('Exchange', f'User:{self.user.name} Error: {e}', level='ERROR', user=self.user)
        elif self.id == 'bybit':
            # print(self.instance.__dir__())
            symbols = self.instance.publicGetContractV3PublicCopytradingSymbolList()
            for symbol in symbols["result"]["list"]:
                cpSymbols.append(symbol["symbol"])
        elif self.id == 'bitget':
            users = Users()
            users = users.find_bitget_users()
            if users:
                for user in users:
                    self.user = user
                    self.connect()
                    try:
                        # print(self.instance.__dir__())
                        symbols = self.instance.privateCopyGetV2CopyMixTraderConfigQuerySymbols({"productType": "USDT-FUTURES"})
                        if symbols:
                            for symbol in symbols["data"]:
                                cpSymbols.append(symbol["symbol"])
                            break
                    except Exception as e:
                        print(f'User:{self.user.name} Error:', e)
        cpSymbols.sort()
        return cpSymbols

    def fetch_symbols(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        self.swap = []
        self.spot = []
        self.cpt = []
        for (k,v) in list(self._markets.items()):
            if v["swap"] and v["active"] and v["linear"]:
                if self.id == "hyperliquid":
                    # Skip HIP-3 stock perpetuals — only regular crypto swaps belong in ini
                    info = v.get("info", {})
                    if isinstance(info, dict) and info.get("hip3"):
                        continue
                    if v["symbol"].endswith('USDC'):
                        self.swap.append(v["symbol"][0:-5].replace("/", "").replace("-", ""))
                if self.id == "bitget":
                    if v["id"][-4:] == 'USDT':
                        self.swap.append(v["id"])
                elif self.id == "kucoinfutures":
                    if v["id"][-5:] == 'USDTM':
                        self.swap.append(v["id"][:len(v["id"])-1])
                elif self.id == "okx":
                    if v["id"].split("-")[1] == 'USDT':
                        # print(v)
                        self.swap.append(''.join(v["id"].split("-")[0:2]))
                elif self.id == "bybit":
                    if v["id"].endswith('USDT'):
                        if v["info"]["copyTrading"] == "both":
                            self.cpt.append(v["id"])
                        self.swap.append(v["id"])
                elif self.id == "binance":
                    if v["id"].endswith('USDT'):
                        # print(v)
                        self.swap.append(v["id"])
                elif self.id == "bingx":
                    if v["id"].endswith('USDT'):
                        self.swap.append(''.join(v["id"].split("-")))
                elif self.id == "gateio":
                    if v["id"].endswith('USDT'):
                        self.swap.append(''.join(v["id"].split("_")))
                        # print(v)
            if v["spot"] and v["active"] and (self.id == "bybit" or self.id == "binance"):
                self.spot.append(v["id"])
        if self.id in ["bitget", "binance"]:
            self.cpt = self.fetch_copytrading_symbols()
        self.spot.sort()
        self.swap.sort()
        if self.cpt:
            self.cpt.sort()
        # print(self.spot)
        # print(self.swap)
        # print(self.cpt)
        self.save_symbols()

    def save_symbols(self):
        pb_config = configparser.ConfigParser()
        pb_config.optionxform = str
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("exchanges"):
            pb_config.add_section("exchanges")
        pb_config.set("exchanges", f'{self.id}.swap', f'{self.swap}')
        if self.spot:
            pb_config.set("exchanges", f'{self.id}.spot', f'{self.spot}')
        if self.cpt:
            pb_config.set("exchanges", f'{self.id}.cpt', f'{self.cpt}')
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    def load_symbols(self):
        pb_config = configparser.ConfigParser()
        pb_config.optionxform = str
        pb_config.read('pbgui.ini')
        if pb_config.has_option("exchanges", f'{self.id}.spot'):
            self.spot = eval(pb_config.get("exchanges", f'{self.id}.spot'))
        if pb_config.has_option("exchanges", f'{self.id}.swap'):
            self.swap = eval(pb_config.get("exchanges", f'{self.id}.swap'))
        if not self.spot and not self.swap:
            self.fetch_symbols()
    
    def fetch_symbol_infos(self, symbol: str):
        if not self.instance:
            self.connect()
            self._markets = self.instance.load_markets()
        # symbol = self.symbol_to_exchange_symbol(symbol, "swap")
        if self.id == 'hyperliquid':
            symbol = f'{symbol[0:-4]}/USDC:USDC'
        else:
            symbol = f'{symbol[0:-4]}/USDT:USDT'
        # print(symbol)
        if symbol not in self._markets:
            return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        symbol_info = self._markets[symbol]
        # print(symbol_info)
        if symbol_info["limits"]["leverage"]["max"] is None:
            lev = "unknown"
        else:
            lev = symbol_info["limits"]["leverage"]["max"]
        contractSize = symbol_info["contractSize"]
        if symbol_info["limits"]["amount"]["min"]:
            min_amount = symbol_info["limits"]["amount"]["min"]
        elif symbol_info["precision"]["amount"]:
            min_amount = symbol_info["precision"]["amount"]
            
        min_qty = min_amount * contractSize
        price = self.fetch_price(symbol, "swap")['last']
        # print(f'Price for {symbol} is {price}')
        min_price = min_qty * price
        # min_cost = 0.0
        if symbol_info["limits"]["cost"]["min"]:
            min_cost = symbol_info["limits"]["cost"]["min"]
        else:
            min_cost = 0.0
        if min_cost > min_price:
            min_price = min_cost
        return min_price, price, contractSize, min_amount, min_cost, lev

    def calculate_balance_needed(self, symbols: list, twe: float, entry_initial_qty_pct: float):
        balance_needed = 0.0
        we = twe / len(symbols)
        for symbol in symbols:
            min_price = self.fetch_symbol_min_order_price(symbol)
            balance_needed_symbol = min_price / we / entry_initial_qty_pct
            balance_needed += balance_needed_symbol
            # print(symbol, we, min_price, balance_needed_symbol)
        return balance_needed

            
def main():
    print("Don't Run this Class from CLI")
    # exchange = Exchange("gateio", None)
    # exchange.fetch_symbols()
    # print(exchange.swap)
    # users = Users()
    # exchange = Exchange("bitget", users.find_user("bitget_CPT"))
    # exchange.fetch_symbols()
    # print(exchange.fetch_copytrading_symbols())
    # exchange = Exchange("hyperliquid", users.find_user("hl_HYPErQuantum"))
    # print(exchange.fetch_prices(["DOGE/USDC:USDC", "WIF/USDC:USDC"], "swap"))
    # exchange = Exchange("binance", users.find_user("binance_CPT"))
    # exchange = Exchange("bybit", users.find_user("HYPErQuantum"))
    # exchange = Exchange("hyperliquid", users.find_user("hl_mani05_DOGE"))
    # exchange = Exchange("bitget", users.find_user("bitget_HYPErQuantum"))
    # exchange = Exchange("okx", users.find_user("okx_MAINCPT"))
    # symbols = ["BTCUSDT"]
    # symbols = ["DOGEUSDT", "VETUSDT", "ICPUSDT", "INJUSDT"]
    # exchange = Exchange("hyperliquid", None)
    # balance_needed = exchange.calculate_balance_needed(symbols, 12.0, 0.03215)
    # print(f'Balance needed on {exchange.id} for {symbols} is {balance_needed:.2f} USDC')
    # exchange = Exchange("okx", None)
    # balance_needed = exchange.calculate_balance_needed(symbols, 12.0, 0.03215)
    # print(f'Balance needed on {exchange.id} for {symbols} is {balance_needed:.2f} USDT')
    # exchange = Exchange("binance", None)
    # balance_needed = exchange.calculate_balance_needed(symbols, 12.0, 0.03215)
    # print(f'Balance needed on {exchange.id} for {symbols} is {balance_needed:.2f} USDT')
    # exchange = Exchange("bybit", None)
    # balance_needed = exchange.calculate_balance_needed(symbols, 12.0, 0.03215)
    # print(f'Balance needed on {exchange.id} for {symbols} is {balance_needed:.2f} USDT')
    # exchange = Exchange("bitget", None)
    # balance_needed = exchange.calculate_balance_needed(symbols, 12.0, 0.03215)
    # print(f'Balance needed on {exchange.id} for {symbols} is {balance_needed:.2f} USDT')
    # exchange = Exchange("gateio", None)
    # balance_needed = exchange.calculate_balance_needed(symbols, 12.0, 0.03215)
    # print(f'Balance needed on {exchange.id} for {symbols} is {balance_needed:.2f} USDT')
    # exchange.load_market()
    # exchange.fetch_symbol_min_order_price("BTCUSDT")
    # exchange.fetch_symbol_min_order_price("ETHUSDT")
    # exchange.fetch_symbol_min_order_price("SOLUSDT")
    # exchange.fetch_symbol_min_order_price("DOGEUSDT")
    # save markets as json
    # with open('binance_markets.json', 'w') as f:
    #     json.dump(exchange._markets, f, indent=4)
    

    # exchange.fetch_symbols()
    # print(exchange.fetch_copytrading_symbols())
    # exchange = Exchange("bybit", users.find_user("bybit_CPTV7HR"))
    # print(exchange.fetch_balance("swap"))
    # exchange.fetch_symbols()
    # exchange = Exchange("okx", users.find_user("okx_MAINCPT"))
    # exchange.fetch_symbols()
    # print(allowed_symbols)
    # print(exchange.swap)
    # print(exchange.fetch_positions())
    # print(exchange.fetch_all_open_orders("DOGE/USDC:USDC"))
    # print(exchange.fetch_prices(["DOGE/USDC:USDC"], "swap"))
    # print(exchange.fetch_prices(["DOGE/USDT:USDT", "WIF/USDT:USDT"], "swap"))
    # print(exchange.fetch_balance("swap"))

    # print(exchange.symbol_to_exchange_symbol("BTCUSDC", "swap"))
    # print(exchange.fetch_symbol_info("DOGEUSDC", "swap"))
    # print(exchange.fetch_price("DOGE/USDC:USDC", "swap"))
    # exchange.fetch_symbols()
    # spot = exchange.fetch_spot()
    # print(exchange.fetch_history(1749323083834))
    # print(exchange.fetch_balance("swap"))
    # print(spot)

if __name__ == '__main__':
    main()
