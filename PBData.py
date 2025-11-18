
import psutil
import subprocess
import sys
import os
from pathlib import Path, PurePath
from time import sleep
from io import TextIOWrapper
from datetime import datetime
import platform
import traceback
from pbgui_func import PBGDIR
import json
from pathlib import Path as _Path
from Database import Database
from User import Users
import configparser
from collections import defaultdict
import asyncio

class PBData():
    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbdata.pid')
        self.my_pid = None
        self.db = Database()
        self.users = Users()
        self._fetch_users = []
        self.load_fetch_users()
        self._balance_ws_tasks = {}
        self._position_ws_tasks = {}
        self._order_ws_tasks = {}
        self._price_exchange_tasks = {}
        self._price_exchange_config = {}
        # Track which symbols we have already subscribed to per exchange
        self._price_subscribed_symbols = {}

        self._history_rest_last = {}
        self._last_fetch_users_snapshot = set()
        self._last_exchange_queue_counts = {}
        self._last_queue_log_ts = 0.0
        self._queue_log_every_secs = 60.0
        self._last_loop_log_ts = 0.0
        self._loop_log_every_secs = 60.0
        self._last_mapping_log_ts_by_exchange = {}
        self._price_ticks_count = {}
        self._mapping_rebuild_min_interval = 300.0  # seconds per exchange
        self._pollers_delay_seconds = 60.0
        self._pollers_enabled_after_ts = datetime.now().timestamp() + self._pollers_delay_seconds
        # Track which user/exchange pairs have already logged 'watch_positions not supported'
        self._watch_positions_not_supported_logged = set()
        # Debug flag for websocket payload logging (can be toggled via pbgui.ini)
        self._debug_ws = False
        self._pbgui_ini_mtime = None
        # Load initial debug setting
        try:
            self._load_debug_setting()
        except Exception:
            pass

    def _log(self, msg: str):
        """Print a log line with timestamp and module tag."""
        try:
            ts = datetime.now().isoformat(sep=' ', timespec='seconds')
        except TypeError:
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{ts} [PBData] {msg}")

    def _load_debug_setting(self):
        """Read pbgui.ini and update websocket debug flag when file changes.

        Use section `pbdata` and option `debug_ws` (true/false).
        """
        try:
            p = Path('pbgui.ini')
            if not p.exists():
                return
            mtime = p.stat().st_mtime
            if self._pbgui_ini_mtime is not None and mtime == self._pbgui_ini_mtime:
                return
            self._pbgui_ini_mtime = mtime
            cfg = configparser.ConfigParser()
            cfg.read('pbgui.ini')
            debug = False
            if cfg.has_option('pbdata', 'debug_ws'):
                try:
                    debug = cfg.getboolean('pbdata', 'debug_ws')
                except Exception:
                    debug = False
            # Log changes
            if debug != getattr(self, '_debug_ws', False):
                self._log(f"[DEBUG] websocket payload logging set to {debug} via pbgui.ini")
            self._debug_ws = debug
        except Exception:
            return

    # fetch_users
    @property
    def fetch_users(self):
        return self._fetch_users
    @fetch_users.setter
    def fetch_users(self, new_fetch_users):
        self._fetch_users = new_fetch_users
        self.save_fetch_users()

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/PBData.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBData')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBData')
            psutil.Process(self.my_pid).kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbdata.py") for sub in psutil.Process(self.my_pid).cmdline()):
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
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))
    
    def load_fetch_users(self):
        pb_config = configparser.ConfigParser()
        try:
            pb_config.read('pbgui.ini')
        except Exception as e:
            self._log(f"Warning: failed reading pbgui.ini ({e}); keeping previous fetch_users: {self._fetch_users}")
            return
        if pb_config.has_option("pbdata", "fetch_users"):
            users = eval(pb_config.get("pbdata", "fetch_users"))
            for user in users.copy():
                if user not in self.users.list():
                    users.remove(user)
            self._fetch_users = users
        else:
            self._fetch_users = []  # Default to empty list if not set
    
    def save_fetch_users(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("pbdata"):
            pb_config.add_section("pbdata")
        pb_config.set("pbdata", "fetch_users", f'{self.fetch_users}')
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    async def _ensure_balance_watcher(self, user):
        if user.name not in self.fetch_users:
            return
        if user.name in self._balance_ws_tasks:
            task = self._balance_ws_tasks[user.name]
            if task and not task.done():
                return
        task = asyncio.create_task(self._balance_ws_loop(user))
        self._balance_ws_tasks[user.name] = task

    async def _reconcile_balance_watchers(self, desired_user_names: set):
        for uname, task in list(self._balance_ws_tasks.items()):
            if uname not in desired_user_names:
                try:
                    if task and not task.done():
                        task.cancel()
                except Exception:
                    pass
                self._balance_ws_tasks.pop(uname, None)

    async def _reconcile_position_watchers(self, desired_user_names: set):
        for uname, task in list(self._position_ws_tasks.items()):
            if uname not in desired_user_names:
                try:
                    if task and not task.done():
                        task.cancel()
                except Exception:
                    pass
                self._position_ws_tasks.pop(uname, None)

    async def _reconcile_order_watchers(self, desired_user_names: set):
        for uname, task in list(self._order_ws_tasks.items()):
            if uname not in desired_user_names:
                try:
                    if task and not task.done():
                        task.cancel()
                except Exception:
                    pass
                self._order_ws_tasks.pop(uname, None)

    async def _balance_ws_loop(self, user):
        from Exchange import Exchange
        await asyncio.sleep((hash(user.name) % 5000) / 1000.0)
        exch = Exchange(user.exchange, user)
        ex = await Exchange.get_private_ws_client(user.exchange, user)
        if not ex:
            self._log(f"[ws] ccxtpro unavailable or unsupported for {user.name} ({user.exchange}); relying on shared balances poller")
            return
        supports_balance = False
        try:
            if hasattr(ex, 'has'):
                if isinstance(ex.has, dict):
                    supports_balance = ex.has.get('watchBalance', False)
                else:
                    supports_balance = getattr(ex.has, 'watchBalance', False)
        except Exception:
            supports_balance = False
        if not supports_balance:
            key = (user.name, exch.id)
            if key not in self._watch_positions_not_supported_logged:
                self._log(f"[ws] watch_balance not supported for {user.name} ({exch.id}); relying on shared balances poller")
                self._watch_positions_not_supported_logged.add(key)
            return
        self._log(f"[ws] Starting balance watcher for {user.name} ({exch.id})")
        try:
            while True:
                # Reload debug_ws from pbgui.ini each loop so GUI toggles
                # take effect quickly for payload logging.
                try:
                    self._load_debug_setting()
                except Exception:
                    pass
                try:
                    # Watch balance; details vary across exchanges
                    bal = await ex.watch_balance()
                    # Debug: optionally log payload type and a short preview so we can
                    # see whether the WS watcher actually returns balance data.
                    if getattr(self, '_debug_ws', False):
                        try:
                            btype = type(bal)
                            preview = repr(bal)
                            if len(preview) > 300:
                                preview = preview[:300] + '...'
                            self._log(f"[ws] watch_balance payload for {user.name}: type={btype} preview={preview}")
                        except Exception:
                            try:
                                self._log(f"[ws] watch_balance payload for {user.name}: (unrepresentable)")
                            except Exception:
                                pass

                    # On any balance update, persist balances (REST fallback)
                    try:
                        await asyncio.to_thread(self.db.update_balances, user)
                    except Exception as e:
                        self._log(f"[ws] DB balance update failed for {user.name}: {e}")
                except Exception as e:
                    raw = str(e)
                    lower = raw.lower()
                    # Hyperliquid (and possibly others) may enforce a limit on
                    # the number of user-specific websockets: detect that and
                    # fall back to the shared serial REST poller for this user.
                    if 'cannot track more than' in lower or ('cannot track' in lower and 'user' in lower):
                        self._log(f"[ws] watch_balance user-limit reached for {user.name}: {e}; closing private ws client and falling back to REST")
                        try:
                            await Exchange.close_private_ws_client(user.exchange, user)
                        except Exception:
                            pass
                        return
                    self._log(f"[ws] watch_balance error for {user.name}: {e}")
                    await asyncio.sleep(2)
        finally:
            # Intentionally not closing `ex` here. Shared websocket clients are
            # kept open to avoid disrupting other watchers that may be using
            # the same client instance.
            try:
                self._log(f"[DEBUG] Leaving ws client open in _balance_ws_loop for {user.name} ({exch.id})")
            except Exception:
                pass
            

    async def _ensure_position_watcher(self, user):
        # Start one WS task per user if not running
        if user.name not in self.fetch_users:
            return
        if user.name in self._position_ws_tasks:
            task = self._position_ws_tasks[user.name]
            if task and not task.done():
                return
        task = asyncio.create_task(self._position_ws_loop(user))
        self._position_ws_tasks[user.name] = task

    async def _ensure_order_watcher(self, user):
        # Start one WS task per user if not running
        if user.name not in self.fetch_users:
            return
        if user.name in self._order_ws_tasks:
            task = self._order_ws_tasks[user.name]
            if task and not task.done():
                return
        task = asyncio.create_task(self._order_ws_loop(user))
        self._order_ws_tasks[user.name] = task

    async def _position_ws_loop(self, user):
        from Exchange import Exchange
        await asyncio.sleep((hash(user.name) % 5000) / 1000.0)
        exch = Exchange(user.exchange, user)
        ex = await Exchange.get_private_ws_client(user.exchange, user)
        if not ex:
            self._log(f"[ws] ccxtpro unavailable or unsupported (positions) for {user.name} ({user.exchange})")
            return
        supports_positions = False
        try:
            if hasattr(ex, 'has'):
                if isinstance(ex.has, dict):
                    supports_positions = ex.has.get('watchPositions', False)
                else:
                    supports_positions = getattr(ex.has, 'watchPositions', False)
        except Exception:
            supports_positions = False
        if not supports_positions:
            key = (user.name, exch.id)
            if key not in self._watch_positions_not_supported_logged:
                # Instead of starting a per-user REST poller (which can create many
                # concurrent requests), rely on the shared serial poller to update
                # positions for exchanges that don't support watchPositions.
                self._log(f"[ws] watch_positions not supported for {user.name} ({exch.id}); relying on shared positions poller")
                self._watch_positions_not_supported_logged.add(key)
            return
        self._log(f"[ws] Starting positions watcher for {user.name} ({exch.id})")
        min_positions_refresh_interval = 10
        last_positions_refresh = 0
        try:
            while True:
                # Reload debug_ws setting so payload logging respects latest ini
                try:
                    self._load_debug_setting()
                except Exception:
                    pass
                try:
                    _ = await ex.watch_positions()
                    now_sec = int(datetime.now().timestamp())
                    if now_sec - last_positions_refresh >= min_positions_refresh_interval:
                        last_positions_refresh = now_sec
                        try:
                            await asyncio.to_thread(self.db.update_positions, user)
                        except Exception as e:
                            self._log(f"[ws] DB positions update failed for {user.name}: {e}")
                    # Debug: optionally log the positions payload
                    if getattr(self, '_debug_ws', False):
                        try:
                            preview = repr(_)
                            if len(preview) > 300:
                                preview = preview[:300] + '...'
                            self._log(f"[ws] watch_positions payload for {user.name}: type={type(_)} preview={preview}")
                        except Exception:
                            try:
                                self._log(f"[ws] watch_positions payload for {user.name}: (unrepresentable)")
                            except Exception:
                                pass
                except Exception as e:
                    raw = str(e)
                    lower = raw.lower()
                    if 'cannot track more than' in lower or ('cannot track' in lower and 'user' in lower):
                        self._log(f"[ws] watch_positions user-limit reached for {user.name}: {e}; closing private ws client and falling back to REST")
                        try:
                            await Exchange.close_private_ws_client(user.exchange, user)
                        except Exception:
                            pass
                        return
                    self._log(f"[ws] watch_positions error for {user.name}: {e}")
                    await asyncio.sleep(2)
        finally:
            # Intentionally not closing `ex` here. Keep shared websocket clients
            # open so other watchers are not interrupted.
            try:
                self._log(f"[DEBUG] Leaving ws client open in _position_ws_loop for {user.name} ({exch.id})")
            except Exception:
                pass

    async def _order_ws_loop(self, user):
        from Exchange import Exchange
        await asyncio.sleep((hash(user.name) % 5000) / 1000.0)
        exch = Exchange(user.exchange, user)
        ex = await Exchange.get_private_ws_client(user.exchange, user)
        if not ex:
            self._log(f"[ws] ccxtpro unavailable or unsupported (orders) for {user.name} ({user.exchange}); relying on shared orders poller")
            return
        supports_orders = False
        try:
            if hasattr(ex, 'has'):
                if isinstance(ex.has, dict):
                    supports_orders = ex.has.get('watchOrders', False)
                else:
                    supports_orders = getattr(ex.has, 'watchOrders', False)
        except Exception:
            supports_orders = False
        if not supports_orders:
            key = (user.name, exch.id)
            if key not in self._watch_positions_not_supported_logged:
                self._log(f"[ws] watch_orders not supported for {user.name} ({exch.id}); relying on shared orders poller")
                self._watch_positions_not_supported_logged.add(key)
            return
        self._log(f"[ws] Starting orders watcher for {user.name} ({exch.id})")
        # Throttle REST order updates so we don't hammer the exchange when
        # websockets produce many events in a short time.
        min_orders_refresh_interval = 20
        last_orders_refresh = 0
        try:
            while True:
                # Reload debug_ws setting so payload logging respects latest ini
                try:
                    self._load_debug_setting()
                except Exception:
                    pass
                try:
                    orders = await ex.watch_orders()
                    # On orders updates, persist orders but no more often than
                    # min_orders_refresh_interval seconds to avoid excessive
                    # REST calls when many users or frequent WS updates.
                    # Debug: optionally log the orders payload
                    if getattr(self, '_debug_ws', False):
                        try:
                            preview = repr(orders)
                            if len(preview) > 300:
                                preview = preview[:300] + '...'
                            self._log(f"[ws] watch_orders payload for {user.name}: type={type(orders)} preview={preview}")
                        except Exception:
                            try:
                                self._log(f"[ws] watch_orders payload for {user.name}: (unrepresentable)")
                            except Exception:
                                pass
                    now_sec = int(datetime.now().timestamp())
                    if now_sec - last_orders_refresh >= min_orders_refresh_interval:
                        last_orders_refresh = now_sec
                        try:
                            await asyncio.to_thread(self.db.update_orders, user)
                        except Exception as e:
                            self._log(f"[ws] DB orders update failed for {user.name}: {e}")
                except Exception as e:
                    raw = str(e)
                    lower = raw.lower()
                    if 'cannot track more than' in lower or ('cannot track' in lower and 'user' in lower):
                        self._log(f"[ws] watch_orders user-limit reached for {user.name}: {e}; closing private ws client and falling back to REST")
                        try:
                            await Exchange.close_private_ws_client(user.exchange, user)
                        except Exception:
                            pass
                        return
                    raw_msg = str(e)
                    exc_type = type(e).__name__
                    self._log(f"[ws] watch_orders error for {user.name}: {raw_msg} (type={exc_type})")
                    await asyncio.sleep(2)
        finally:
            # Intentionally not closing `ex` here. Keep shared websocket clients
            # open so other watchers are not interrupted.
            try:
                self._log(f"[DEBUG] Leaving ws client open in _order_ws_loop for {user.name} ({exch.id})")
            except Exception:
                pass

    async def _order_poll_loop(self, user, interval_seconds: int = 5):
        self._log(f"[poll] Starting orders poller for {user.name}")
        while True:
            try:
                await asyncio.to_thread(self.db.update_orders, user)
            except Exception as e:
                self._log(f"[poll] Orders poll failed for {user.name}: {e}")
            await asyncio.sleep(interval_seconds)

    async def _position_poll_loop(self, user, interval_seconds: int = 5):
        self._log(f"[poll] Starting positions poller for {user.name}")
        while True:
            try:
                await asyncio.to_thread(self.db.update_positions, user)
            except Exception as e:
                self._log(f"[poll] Positions poll failed for {user.name}: {e}")
            await asyncio.sleep(interval_seconds)

    async def _shared_poll_serial(self, kind: str, interval_seconds: int, per_exchange: bool = True):
        """Generic serial poller for 'positions', 'history', 'orders', or 'balances'."""
        backoff = 0
        max_backoff = 600
        base_interval = max(10, interval_seconds)
        self._log(f"[poll] Starting shared serial poller kind={kind} interval={base_interval}s")
        while True:
            delay = base_interval + backoff
            await asyncio.sleep(delay)
            users = [u for u in self.users if u.name in self.fetch_users]
            if not users:
                continue
            if per_exchange:
                groups = {}
                for u in users:
                    groups.setdefault(u.exchange, []).append(u)
                batches = groups.items()
            else:
                batches = [(None, users)]
            had_rate_limit = False
            for exch, batch_users in batches:
                for user in batch_users:
                    try:
                        if kind == 'positions':
                            # If a per-user active websocket positions watcher exists,
                            # let the WS task drive position updates — skip REST here
                            # to avoid duplicate parallel requests that can trigger rate limits.
                            ws_task = self._position_ws_tasks.get(user.name)
                            if ws_task and not ws_task.done():
                                # WS active for this user; skip shared REST update
                                # (WS will call db.update_positions on events).
                                # Log occasionally for visibility.
                                last_log = getattr(self, '_last_skipped_position_log', 0)
                                now_ts = datetime.now().timestamp()
                                if now_ts - last_log > 300:
                                    self._log(f"[poll] Skipping shared positions update for {user.name} because WS watcher active")
                                    self._last_skipped_position_log = now_ts
                                continue
                            await asyncio.to_thread(self.db.update_positions, user)
                        elif kind == 'orders':
                            # Skip shared orders poll if user has active WS orders watcher
                            ws_task = self._order_ws_tasks.get(user.name)
                            if ws_task and not ws_task.done():
                                last_log = getattr(self, '_last_skipped_order_log', 0)
                                now_ts = datetime.now().timestamp()
                                if now_ts - last_log > 300:
                                    self._log(f"[poll] Skipping shared orders update for {user.name} because WS watcher active")
                                    self._last_skipped_order_log = now_ts
                                continue
                            await asyncio.to_thread(self.db.update_orders, user)
                        elif kind == 'history':
                            # Instrument shared history polling for debugging/tracing
                            key = (user.name, user.exchange)
                            start_ts = datetime.now().timestamp()
                            self._log(f"[poll] Shared history poll START for {user.name} ({user.exchange})")
                            await asyncio.to_thread(self.db.update_history, user)
                            dur_ms = int((datetime.now().timestamp() - start_ts) * 1000)
                            # record last successful REST history poll time per user/exchange
                            try:
                                self._history_rest_last[key] = start_ts
                            except Exception:
                                pass
                            self._log(f"[poll] Shared history poll DONE for {user.name} ({user.exchange}) dur={dur_ms}ms")
                        elif kind == 'balances':
                            # Skip shared balances poll if user has active WS balances watcher
                            ws_task = self._balance_ws_tasks.get(user.name)
                            if ws_task and not ws_task.done():
                                last_log = getattr(self, '_last_skipped_balance_log', 0)
                                now_ts = datetime.now().timestamp()
                                if now_ts - last_log > 300:
                                    self._log(f"[poll] Skipping shared balances update for {user.name} because WS watcher active")
                                    self._last_skipped_balance_log = now_ts
                                continue
                            await asyncio.to_thread(self.db.update_balances, user)
                        # (duplicate branches removed)
                    except Exception as e:
                        msg = str(e)
                        self._log(f"[poll] Shared {kind} poll failed for {user.name}: {e}")
                        lower = msg.lower()
                        if '429' in lower or 'too many requests' in lower or 'rate limit' in lower:
                            had_rate_limit = True
            if had_rate_limit:
                backoff = min(max_backoff, backoff + 30)
            else:
                if backoff:
                    self._log(f"[poll] Shared {kind} poll recovered; resetting backoff")
                backoff = 0

    async def _balance_poll_loop(self, user, interval_seconds: int = 30):
        self._log(f"[poll] Starting balance poller for {user.name}")
        while True:
            try:
                await asyncio.to_thread(self.db.update_balances, user)
            except Exception as e:
                self._log(f"[poll] Balance poll failed for {user.name}: {e}")
            await asyncio.sleep(interval_seconds)

    def _to_ccxt_symbol(self, symbol: str) -> str:
        if symbol.endswith('USDT'):
            return f"{symbol[:-4]}/USDT:USDT"
        if symbol.endswith('USDC'):
            return f"{symbol[:-4]}/USDC:USDC"
        return symbol

    async def _price_exchange_ws_loop(self, exchange: str):
        from Exchange import Exchange
        last_price_write_ts = {}
        throttle_interval_sec = 1.0

        while True:
            exch = Exchange(exchange, None)
            ex = exch.create_ws_client()
            if not ex:
                self._log(f"[ws] ccxtpro unavailable (price) for exchange {exchange}; retrying in 10s")
                await asyncio.sleep(10)
                continue
            self._log(f"[ws] Starting price watcher for exchange {exchange}")
            started_logged = False
            last_heartbeat_ts = 0
            self._price_ticks_count[exchange] = 0
            try:
                if hasattr(ex, 'load_markets'):
                    lm = ex.load_markets()
                    import asyncio as _asyncio  # local alias to check coroutine
                    if _asyncio.iscoroutine(lm):
                        await lm
                    else:
                        # Run sync load_markets without blocking loop
                        await asyncio.to_thread(ex.load_markets)
            except Exception as e:
                self._log(f"[ws] load_markets failed for exchange {exchange}: {e}")
            try:
                while True:
                    cfg = self._price_exchange_config.get(exchange, {})
                    symbols = list(cfg.get('symbols', set()))
                    mapping = cfg.get('mapping', {})
                    if not symbols:
                        await asyncio.sleep(1)
                        continue
                    supports_batch = False
                    try:
                        if hasattr(ex, 'has'):
                            has = ex.has
                            if isinstance(has, dict):
                                supports_batch = has.get('watchTickers', False)
                            else:
                                supports_batch = getattr(has, 'watchTickers', False)
                    except Exception:
                        supports_batch = False
                    now_ts = int(datetime.now().timestamp())
                    if now_ts - last_heartbeat_ts >= 120:
                        tick_count = self._price_ticks_count.get(exchange, 0)
                        self._log(f"[ws] Price heartbeat for exchange {exchange}: {len(symbols)} symbol(s), mode={'batch' if supports_batch else 'per-symbol'}, ticks_since_last={tick_count}")
                        self._price_ticks_count[exchange] = 0
                        last_heartbeat_ts = now_ts
                    if not started_logged:
                        mode = 'batch watch_tickers' if supports_batch else 'per-symbol watch_ticker'
                        self._log(f"[ws] Price stream active for exchange {exchange}: {len(symbols)} symbol(s), mode={mode}")
                        started_logged = True

                    if supports_batch:
                        # Ensure we only attempt to subscribe to newly added
                        # symbols to avoid re-subscribing and triggering
                        # 'already subscribed' errors from exchanges.
                        requested_set = set(symbols)
                        subscribed = self._price_subscribed_symbols.get(exchange, set())
                        added = list(requested_set - subscribed)
                        if added:
                            try:
                                # Subscribe only to newly added symbols
                                await asyncio.wait_for(ex.watch_tickers(added), timeout=65)
                                # Update locally tracked subscribed set
                                subscribed = subscribed.union(set(added))
                                self._price_subscribed_symbols[exchange] = subscribed
                            except Exception as e:
                                raw = str(e)
                                if 'already subscribed' in raw.lower() or 'already subscribed' in raw:
                                    # Non-fatal: someone else already subscribed; merge sets
                                    subscribed = subscribed.union(set(added))
                                    self._price_subscribed_symbols[exchange] = subscribed
                                    self._log(f"[ws] watch_tickers subscribe: already subscribed for exchange {exchange}: {e}; continuing")
                                else:
                                    self._log(f"[ws] watch_tickers subscribe ERROR for exchange {exchange}: {e}")
                                    # Attempt to recreate client but do not close shared instance
                                    ex = exch.create_ws_client()
                                    if not ex:
                                        self._log(f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
                                        return
                                    await asyncio.sleep(1)
                                    continue
                        # Now wait for tickers for the currently requested set. Use the
                        # requested_set (not 'added') to receive updates for all
                        # symbols of interest.
                        try:
                            tickers = await asyncio.wait_for(ex.watch_tickers(list(requested_set)), timeout=65)
                        except asyncio.TimeoutError:
                            self._log(f"[ws] watch_tickers TIMEOUT for exchange {exchange}; reconnecting price client")
                            ex = exch.create_ws_client()
                            if not ex:
                                self._log(f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                return
                            continue
                        except Exception as e:
                            raw = str(e)
                            lower = raw.lower()
                            if 'already subscribed' in lower or 'already subscribed' in raw:
                                self._log(f"[ws] watch_tickers: already subscribed for exchange {exchange}: {e}; ignoring and continuing")
                                await asyncio.sleep(1)
                                continue
                            self._log(f"[ws] watch_tickers ERROR for exchange {exchange}: {e}")
                            ex = exch.create_ws_client()
                            if not ex:
                                self._log(f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
                                return
                            await asyncio.sleep(1)
                            continue
                        except asyncio.TimeoutError:
                            self._log(f"[ws] watch_tickers TIMEOUT for exchange {exchange}; reconnecting price client")
                            # Avoid closing the websocket client here because it may be
                            # a shared client used by other watchers. Recreate a new
                            # client instance instead and leave the existing client
                            # open to avoid interrupting other tasks.
                            ex = exch.create_ws_client()
                            if not ex:
                                self._log(f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                return
                            continue
                        except Exception as e:
                            raw = str(e)
                            lower = raw.lower()
                            # Some exchange implementations (e.g. bybit) raise an
                            # 'already subscribed' error when a topic is re-subscribed.
                            # Treat this as non-fatal: wait briefly and continue
                            # without force-closing the client to avoid races.
                            if 'already subscribed' in lower or 'already subscribed' in raw:
                                self._log(f"[ws] watch_tickers: already subscribed for exchange {exchange}: {e}; ignoring and continuing")
                                await asyncio.sleep(1)
                                continue
                            self._log(f"[ws] watch_tickers ERROR for exchange {exchange}: {e}")
                            # Do not close shared client; instead recreate a fresh
                            # client for subsequent iterations. This avoids races
                            # where closing a shared client interrupts other tasks.
                            ex = exch.create_ws_client()
                            if not ex:
                                self._log(f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
                                return
                            await asyncio.sleep(1)
                            continue
                        self._price_ticks_count[exchange] = self._price_ticks_count.get(exchange, 0) + len(tickers)
                        ts_now = int(datetime.now().timestamp() * 1000)
                        for ccxt_symbol, ticker in tickers.items():
                            last = ticker.get('last')
                            ts = ticker.get('timestamp') or ts_now
                            if last is None:
                                continue
                            for user_name, internal_symbol in mapping.get(ccxt_symbol, []):
                                try:
                                    user = self.users.find_user(user_name)
                                    if user:
                                        key = (user.name, internal_symbol)
                                        now_sec = datetime.now().timestamp()
                                        if now_sec - last_price_write_ts.get(key, 0.0) >= throttle_interval_sec:
                                            last_price_write_ts[key] = now_sec
                                            await asyncio.to_thread(self.db.upsert_price, user, internal_symbol, ts, last)
                                except Exception as e:
                                    self._log(f"[ws] upsert_price failed {user_name} {internal_symbol}: {e}")
                    else:
                        # Fallback: iterate symbols and watch individually (still single task)
                        ts_now = int(datetime.now().timestamp() * 1000)
                        for ccxt_symbol in symbols:
                            try:
                                try:
                                    ticker = await asyncio.wait_for(ex.watch_ticker(ccxt_symbol), timeout=65)
                                except asyncio.TimeoutError:
                                    self._log(f"[ws] watch_ticker TIMEOUT exchange {exchange} {ccxt_symbol}; reconnecting price client")
                                    # Avoid closing shared client; recreate instead.
                                    ex = exch.create_ws_client()
                                    if not ex:
                                        self._log(f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                        raise RuntimeError("price client unavailable after watch_ticker timeout")
                                    continue
                                except Exception as e:
                                    raw = str(e)
                                    lower = raw.lower()
                                    if 'already subscribed' in lower or 'already subscribed' in raw:
                                        self._log(f"[ws] watch_ticker: already subscribed exchange {exchange} {ccxt_symbol}: {e}; ignoring and continuing")
                                        await asyncio.sleep(0.5)
                                        continue
                                    self._log(f"[ws] watch_ticker ERROR exchange {exchange} {ccxt_symbol}: {e}; reconnecting price client")
                                    # Avoid closing shared client; recreate instead.
                                    ex = exch.create_ws_client()
                                    if not ex:
                                        self._log(f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
                                        raise RuntimeError("price client unavailable after watch_ticker error")
                                    await asyncio.sleep(1)
                                    continue
                                last = ticker.get('last')
                                ts = ticker.get('timestamp') or ts_now
                                if last is None:
                                    continue
                                for user_name, internal_symbol in mapping.get(ccxt_symbol, []):
                                    try:
                                        user = self.users.find_user(user_name)
                                        if user:
                                            key = (user.name, internal_symbol)
                                            now_sec = datetime.now().timestamp()
                                            if now_sec - last_price_write_ts.get(key, 0.0) >= throttle_interval_sec:
                                                last_price_write_ts[key] = now_sec
                                                await asyncio.to_thread(self.db.upsert_price, user, internal_symbol, ts, last)
                                                # self._log(f"[ws] upsert_price wrote {user.name} {internal_symbol} price={last} ts={ts}")
                                    except Exception as e:
                                        self._log(f"[ws] upsert_price failed {user_name} {internal_symbol}: {e}")
                            except Exception as e:
                                self._log(f"[ws] watch_ticker error exchange {exchange} {ccxt_symbol}: {e}")
                                await asyncio.sleep(0.5)
            except Exception as e:
                self._log(f"[ws] price loop error exchange {exchange}: {e}; restarting price watcher")
            finally:
                # Do not close the websocket client here. Closing a shared
                # client can abort other watchers that rely on the same
                # underlying connection. Leave cleanup to a centralized
                # manager (Exchange) or process shutdown logic.
                try:
                    self._log(f"[DEBUG] Leaving price ws client open for exchange {exchange}")
                except Exception:
                    pass
            # Small delay before recreating client to avoid tight restart loops
            await asyncio.sleep(5)

    async def _ensure_exchange_price_watcher(self, exchange: str, mapping: dict):
        # mapping: { ccxt_symbol: [(user_name, internal_symbol), ...] }
        symbols = set(mapping.keys())
        # If no symbols for this exchange, stop existing watcher if any
        if not symbols:
            task = self._price_exchange_tasks.pop(exchange, None)
            if task and not task.done():
                try:
                    task.cancel()
                except Exception:
                    pass
            self._log(f"[ws] Stopped price watcher for exchange {exchange}: no symbols for fetch_users")
            self._price_exchange_config.pop(exchange, None)
            return
        cfg = self._price_exchange_config.get(exchange)
        if not cfg:
            self._price_exchange_config[exchange] = {'mapping': mapping, 'symbols': symbols}
            self._log(f"[ws] Preparing price watcher for exchange {exchange}: {len(symbols)} symbol(s)")
        else:
            # Update mapping and symbols; log changes if any
            old_symbols = cfg.get('symbols', set())
            if old_symbols != symbols:
                added = symbols - old_symbols
                removed = old_symbols - symbols
                self._log(f"[ws] Updated symbols for exchange {exchange}: {len(symbols)} now; +{len(added)} / -{len(removed)}")
            cfg['mapping'] = mapping
            cfg['symbols'] = symbols
        task = self._price_exchange_tasks.get(exchange)
        if not task or task.done():
            task = asyncio.create_task(self._price_exchange_ws_loop(exchange))
            self._price_exchange_tasks[exchange] = task

    async def _build_price_mapping_for_exchange(self, exchange: str, users: list):
        """Build and apply price mapping for a single exchange in the background.

        This runs in its own task so that expensive DB work (fetch_positions
        per user) does not block the main update_db_async loop.
        """
        try:
            now_ts = datetime.now().timestamp()
            # Throttle how often we actually rebuild the mapping per exchange.
            # When many background tasks are active, this prevents continuous fetch_positions load and lets the
            # price websocket loop run freely.
            last = self._last_mapping_log_ts_by_exchange.get(exchange, 0.0)
            if now_ts - last < self._mapping_rebuild_min_interval:
                return
            self._last_mapping_log_ts_by_exchange[exchange] = now_ts
            self._log(f"[async] Building price mapping for exchange {exchange} with {len(users)} user(s)")
            mapping = {}
            for user in users:
                try:
                    positions = await asyncio.to_thread(self.db.fetch_positions, user)
                except Exception as e:
                    self._log(f"[async] fetch_positions failed for {user.name} ({exchange}): {e}")
                    continue
                for pos in positions:
                    internal_symbol = pos[1]
                    ccxt_symbol = self._to_ccxt_symbol(internal_symbol)
                    mapping.setdefault(ccxt_symbol, []).append((user.name, internal_symbol))
            # Always log the final symbol count so we know whether the
            # mapping contains anything for this exchange.
            self._log(f"[async] Built price mapping for exchange {exchange}: {len(mapping)} symbol(s)")
            await self._ensure_exchange_price_watcher(exchange, mapping)
        except Exception as e:
            self._log(f"[async] _build_price_mapping_for_exchange error {exchange}: {e}")

    async def update_db_async(self):
        # Load users first so filtering in load_fetch_users is correct
        self.users.load()
        self.load_fetch_users()
        # Reload debug setting if pbgui.ini changed
        try:
            self._load_debug_setting()
        except Exception:
            pass
        now_ts = datetime.now().timestamp()
        # Only log when the set changes or periodically
        current_users_set = set(self.fetch_users)
        if current_users_set != self._last_fetch_users_snapshot or (now_ts - self._last_queue_log_ts) >= self._queue_log_every_secs:
            self._log(f"[async] Will process users: {self.fetch_users}")
            self._last_fetch_users_snapshot = current_users_set
            self._last_queue_log_ts = now_ts
            try:
                # Print a summary of which users use websockets vs shared REST
                self.print_fetch_method_summary()
            except Exception:
                pass

        # Group users by exchange
        users_by_exchange = defaultdict(list)
        for user in self.users:
            if user.name in self.fetch_users:
                users_by_exchange[user.exchange].append(user)

        # Determine desired watcher set directly from configured fetch users
        desired_user_names = set(self.fetch_users)

        # Stop watchers for users no longer configured
        await self._reconcile_balance_watchers(desired_user_names)
        await self._reconcile_position_watchers(desired_user_names)
        await self._reconcile_order_watchers(desired_user_names)

        for exchange, users in users_by_exchange.items():
            # Log only when count changes for this exchange or periodically
            count = len(users)
            prev = self._last_exchange_queue_counts.get(exchange)
            if prev != count or (now_ts - self._last_queue_log_ts) >= self._queue_log_every_secs:
                self._log(f"[async] Queueing {count} user(s) for exchange: {exchange}")
                self._last_exchange_queue_counts[exchange] = count

        # Phase websocket watcher startup per user to avoid bursts:
        #   - Phase 1: balance + positions
        #   - Phase 2: orders
        phase1_users = []
        phase2_users = []
        for uname in self.fetch_users:
            u = self.users.find_user(uname)
            if not u:
                continue
            phase1_users.append(u)
            phase2_users.append(u)

        # Phase 1: start balance and positions watchers
        for u in phase1_users:
            await self._ensure_balance_watcher(u)
            await self._ensure_position_watcher(u)
            # Small stagger to avoid starting many watchers at once
            await asyncio.sleep(0.02)

        # Phase 2: start orders watchers
        for u in phase2_users:
            await self._ensure_order_watcher(u)
            # Small stagger to avoid bursts of order watchers
            await asyncio.sleep(0.02)

        # Start shared serial pollers only after a grace period so that
        # websocket startup and initial subscriptions don't coincide with
        # heavy REST history/position/order/balance traffic.
        now_ts = datetime.now().timestamp()
        if now_ts >= self._pollers_enabled_after_ts:
            try:
                if not hasattr(self, "_shared_positions_task") or self._shared_positions_task is None or self._shared_positions_task.done():
                    self._shared_positions_task = asyncio.create_task(self._shared_poll_serial('positions', 60, per_exchange=True))
                if not hasattr(self, "_shared_history_task") or self._shared_history_task is None or self._shared_history_task.done():
                    self._shared_history_task = asyncio.create_task(self._shared_poll_serial('history', 90, per_exchange=True))
                if not hasattr(self, "_shared_orders_task") or self._shared_orders_task is None or self._shared_orders_task.done():
                    self._shared_orders_task = asyncio.create_task(self._shared_poll_serial('orders', 60, per_exchange=True))
                if not hasattr(self, "_shared_balances_task") or self._shared_balances_task is None or self._shared_balances_task.done():
                    self._shared_balances_task = asyncio.create_task(self._shared_poll_serial('balances', 30, per_exchange=True))
            except Exception as e:
                self._log(f"[DEBUG] Error starting shared pollers: {e}")

        # Ensure one price watcher per exchange with combined symbols across its users.
        # Run mapping building in background tasks so update_db_async returns quickly.
        for exchange, users in users_by_exchange.items():
            asyncio.create_task(self._build_price_mapping_for_exchange(exchange, users))

    def print_fetch_method_summary(self):
        """Log a summary which users use websocket watchers vs shared history/REST.

        For each configured fetch user, indicate whether balances/positions/orders
        are being updated via a running websocket watcher (ws) or via the
        shared REST poller (rest). History is always 'rest' (shared poller).
        """
        try:
            # Ensure user list is fresh
            try:
                self.users.load()
            except Exception:
                pass
            self.load_fetch_users()
            balances_ws = []
            balances_rest = []
            positions_ws = []
            positions_rest = []
            orders_ws = []
            orders_rest = []
            all_users = []
            for u in self.users:
                if u.name not in self.fetch_users:
                    continue
                all_users.append(u.name)
                if self._balance_ws_tasks.get(u.name) and not self._balance_ws_tasks.get(u.name).done():
                    balances_ws.append(u.name)
                else:
                    balances_rest.append(u.name)
                if self._position_ws_tasks.get(u.name) and not self._position_ws_tasks.get(u.name).done():
                    positions_ws.append(u.name)
                else:
                    positions_rest.append(u.name)
                if self._order_ws_tasks.get(u.name) and not self._order_ws_tasks.get(u.name).done():
                    orders_ws.append(u.name)
                else:
                    orders_rest.append(u.name)

            # Build a single multi-line, human-friendly summary block
            def join_csv(lst):
                return ', '.join(lst) if lst else '(none)'

            summary_lines = []
            summary_lines.append("[summary] Fetch method summary:")
            summary_lines.append(f"[summary] Balances: ws={len(balances_ws)} rest={len(balances_rest)}")
            summary_lines.append(f"[summary]   ws: {join_csv(balances_ws)}")
            summary_lines.append(f"[summary]   rest: {join_csv(balances_rest)}")
            summary_lines.append(f"[summary] Positions: ws={len(positions_ws)} rest={len(positions_rest)}")
            summary_lines.append(f"[summary]   ws: {join_csv(positions_ws)}")
            summary_lines.append(f"[summary]   rest: {join_csv(positions_rest)}")
            summary_lines.append(f"[summary] Orders: ws={len(orders_ws)} rest={len(orders_rest)}")
            summary_lines.append(f"[summary]   ws: {join_csv(orders_ws)}")
            summary_lines.append(f"[summary]   rest: {join_csv(orders_rest)}")
            summary_lines.append(f"[summary] History (rest): {join_csv(all_users)}")
            try:
                # Also write machine-readable JSON summary for GUI/monitoring
                try:
                    summary_obj = {
                        'timestamp': datetime.now().isoformat(sep=' ', timespec='seconds'),
                        'balances': {'ws': balances_ws, 'rest': balances_rest},
                        'positions': {'ws': positions_ws, 'rest': positions_rest},
                        'orders': {'ws': orders_ws, 'rest': orders_rest},
                        'history': all_users,
                    }
                    logs_dir = _Path(f"{PBGDIR}/data/logs")
                    if not logs_dir.exists():
                        try:
                            logs_dir.mkdir(parents=True, exist_ok=True)
                        except Exception:
                            pass
                    summary_path = logs_dir / 'fetch_summary.json'
                    try:
                        with open(summary_path, 'w') as _f:
                            json.dump(summary_obj, _f, indent=2, ensure_ascii=False)
                    except Exception:
                        pass
                except Exception:
                    pass
                self._log("\n".join(summary_lines))
            except Exception:
                for sl in summary_lines:
                    try:
                        self._log(sl)
                    except Exception:
                        pass
        except Exception as e:
            try:
                self._log(f"[summary] Failed to build fetch method summary: {e}")
            except Exception:
                pass

def main():
    """Entry point kept synchronous; spins up an async loop internally."""
    dest = Path(f'{PBGDIR}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBData.log')
    sys.stdout = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBData')
    pbdata = PBData()
    if pbdata.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBData already started')
        exit(1)
    pbdata.save_pid()

    async def run_loop():
        while True:
            try:
                if logfile.exists() and logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
                try:
                    await pbdata.update_db_async()
                except Exception as e:
                    print(f"{datetime.now().isoformat(sep=' ', timespec='seconds')} [PBData] [loop] update_db_async ERROR: {e}")
                    traceback.print_exc()
                await asyncio.sleep(1)
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

    asyncio.run(run_loop())

if __name__ == '__main__':
    main()