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
import random
from logging_helpers import human_log as _human_log, set_service_min_level, is_debug_enabled
from Exchange import set_ws_limits

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

        # In-memory buffer for latest prices per (user, symbol).
        # Key: (user_name, symbol) -> (timestamp, price)
        self._price_buffer = {}
        # Async lock protecting _price_buffer
        try:
            self._price_buffer_lock = asyncio.Lock()
        except Exception:
            # Fallback to a dummy lock if asyncio not fully available
            import threading as _th
            self._price_buffer_lock = _th.Lock()
        # Flush interval in seconds for buffered price writes
        self._price_flush_interval = 10.0
        # Background writer task handle
        self._price_writer_task = None

        # Enable/disable price buffering via env var `PB_PRICE_BUFFER` (default: enabled)
        self._price_buffer_enabled = True

        # (IO tracking removed) -- process/db IO debugging variables removed

        self._history_rest_last = {}
        self._last_fetch_users_snapshot = set()
        self._last_exchange_queue_counts = {}
        self._last_queue_log_ts = 0.0
        self._queue_log_every_secs = 60.0
        self._last_loop_log_ts = 0.0
        self._loop_log_every_secs = 60.0
        self._last_mapping_log_ts_by_exchange = {}
        self._price_ticks_count = {}
        # network error log throttle map: (exchange,user) -> ts
        self._last_network_error_log_ts = {}
        self._network_error_log_throttle = 30.0
        # Max number of symbols to subscribe in one watch_tickers call
        self._price_subscribe_chunk_size = 20
        # Per-exchange overrides for subscribe chunk sizes (symbols per watch_tickers call)
        self._price_subscribe_chunk_size_by_exchange = {
            'hyperliquid': 5,
            'bitget': 5,
            'binance': 5,
            # bybit can be sensitive to large batch subscribes — use smaller chunks
            'bybit': 10,
        }
        # Stagger (ms) between starting private ws watchers to avoid bursts
        self._private_ws_stagger_ms = 200
        # Pause (s) between per-user REST calls in shared pollers to avoid bursts
        # Default small pause to reduce rate-limit triggers; can be overridden per-exchange
        # Tuned defaults to reduce observed 429s; adjust via pbgui.ini later if needed
        self._shared_rest_user_pause = 0.75
        self._shared_rest_pause_by_exchange = {
            'hyperliquid': 1.5,
            # Increased to reduce REST fallback bursts that triggered 429s
            # (mitigation A): raised from 1.0 -> 3.0 seconds
            'bybit': 3.0,
        }
        # Per-exchange limit for how many distinct users the price watcher may track
        # Some exchanges (hyperliquid) enforce a hard cap on tracked users for websocket topics.
        self._price_subscribe_user_limit_by_exchange = {
            'hyperliquid': 10,
        }
        self._mapping_rebuild_min_interval = 300.0  # seconds per exchange
        self._pollers_delay_seconds = 60.0
        self._pollers_enabled_after_ts = datetime.now().timestamp() + self._pollers_delay_seconds
        # Track which user/exchange pairs have already logged 'watch_positions not supported'
        self._watch_positions_not_supported_logged = set()
        self._pbgui_ini_mtime = None
        # Last loaded ws_max value from pbgui.ini (so we only reapply when changed)
        self._ws_max_loaded = None
        # Last loaded log_level for PBData (string like 'DEBUG'/'INFO')
        self._log_level_loaded = None
        # Snapshot of last trimmed allowed users per exchange to avoid repeated logs
        self._price_subscribe_trim_snapshot = {}
        # Track recent network-demoted users per exchange to avoid mass demotion
        self._exchange_network_error_users = defaultdict(dict)  # exchange -> {user_name: timestamp}
        self._network_error_locks = {}
        # Time window (seconds) during which only one demotion is allowed per exchange
        self._network_demotion_window = 60
        # Per-exchange backoff state and error tracking
        self._exchange_backoff_until = {}  # exchange -> timestamp until which we should backoff
        self._exchange_error_timestamps = defaultdict(list)  # exchange -> [ts1, ts2, ...]
        self._error_window_seconds = 30
        self._error_threshold = 6
        self._backoff_duration_seconds = 60
        # Timeout (seconds) used for asyncio.wait_for around ccxt.pro watch_* calls
        # Increase on slow VPS if you see ping-pong RequestTimeouts.
        self._price_watch_timeout = 120
        # Websocket restart-once state: track one restart per (exchange,user)
        # and consecutive successful watch messages to clear the restart marker.
        self._ws_restarted_once = set()  # set of (exchange, user_name)
        self._ws_success_counts = defaultdict(int)  # (exchange, user_name) -> consecutive successes
        self._ws_success_required = 3  # successes required to clear restart marker
        self._ws_restart_sleep = 0.5  # base sleep (s) before re-creating client
        # Per-user last fetch timestamps (key: (user_name, kind) -> epoch seconds)
        # kind in {'balances','positions','orders','history'}
        self._last_fetch_ts = defaultdict(dict)
        # If a shared history poll takes longer than this (s) consider exchange overloaded
        # Increase threshold to avoid overly-aggressive backoffs for slower history endpoints
        self._long_poll_threshold_seconds = 60
        # Metrics task handle
        self._metrics_task = None
        # Metrics sampling interval (seconds), configurable via env PB_METRICS_INTERVAL
        self._metrics_interval = 60
        # (IO debugging disabled) -- per-metrics-cycle DB/process IO logging removed
        # Load initial settings (ws_max, log_level, ...)
        try:
            self._load_settings()
        except Exception:
            pass

        # Caller-side private-client manager (Queue + background manager task)
        # This manager serializes private-client creation requests from PBData
        # so that check+reserve logic can be performed atomically on the caller
        # side and duplicate cap-warning log spam is avoided.
        self._private_client_queue = None
        self._private_client_manager_task = None
        # manager_state contains transient reservation set and warned flags
        self._private_client_manager_state = {
            'inflight': set(),                # set of keys currently reserved by manager
            'warned_global': False,           # whether a global-cap warning was emitted
            'warned_per_exch': {},            # exchange -> bool
        }

        # Register with Exchange to be notified when private clients are closed
        try:
            from Exchange import register_private_client_close_listener
            # register a small synchronous callback that schedules the async clear
            def _on_closed_cb(exchange_id, user_name):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._private_manager_maybe_clear_flags_for_exchange(exchange_id))
                except Exception:
                    pass
            register_private_client_close_listener(_on_closed_cb)
        except Exception:
            pass





    # Logging is centralized via the module-level `_log_central` function.

    def _load_settings(self):
        """Read `pbgui.ini` and update runtime settings when file changes.

                Currently loads:
                    - pbdata.ws_max (int) -> passed to Exchange.set_ws_limits(global_max=...)
                    - pbdata.log_level (str) -> sets minimum log level for PBData

        The function uses the file mtime to avoid re-reading the INI too often.
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

            # Note: payload debug flag removed — use log_level to control DEBUG logging.

            # ws_max (integer) - global cap for private websocket clients
            ws_max = None
            if cfg.has_option('pbdata', 'ws_max'):
                try:
                    raw = cfg.get('pbdata', 'ws_max')
                    sval = str(raw).strip() if raw is not None else ''
                    if sval != '':
                        try:
                            ws_max = int(sval)
                        except Exception:
                            ws_max = None
                except Exception:
                    ws_max = None

            if ws_max is not None and ws_max != getattr(self, '_ws_max_loaded', None):
                try:
                    set_ws_limits(global_max=ws_max)
                    self._ws_max_loaded = ws_max
                    _human_log('PBData', f"Set Exchange.ws global cap via pbgui.ini [pbdata] ws_max={ws_max}", level='DEBUG')
                except Exception:
                    try:
                        _human_log('PBData', f"Failed to call Exchange.set_ws_limits with ws_max={ws_max}", level='WARNING')
                    except Exception:
                        pass
            # log_level (string) - minimum log level for PBData
            log_level = None
            if cfg.has_option('pbdata', 'log_level'):
                try:
                    raw = cfg.get('pbdata', 'log_level')
                    s = str(raw).strip() if raw is not None else ''
                    if s != '':
                        log_level = s.upper()
                except Exception:
                    log_level = None

            if log_level is not None and log_level != getattr(self, '_log_level_loaded', None):
                try:
                    set_service_min_level('PBData', log_level)
                    self._log_level_loaded = log_level
                    _human_log('PBData', f"PBData log level set via pbgui.ini [pbdata] log_level={log_level}", level='DEBUG')
                except Exception:
                    try:
                        _human_log('PBData', f"Failed to set PBData log level to {log_level}", level='WARNING')
                    except Exception:
                        pass
        except Exception:
            return

    # -----------------------------
    # Private client manager (caller-side)
    # -----------------------------
    def _start_private_client_manager(self):
        """Lazily start the private client manager background task."""
        try:
            if self._private_client_queue is None:
                self._private_client_queue = asyncio.Queue()
            if self._private_client_manager_task is None or self._private_client_manager_task.done():
                self._private_client_manager_task = asyncio.create_task(self._private_client_manager_loop())
        except Exception:
            pass

    async def request_private_client(self, exchange_id: str, user, caller: str = None):
        """Public API for callers to request a private client.

        Returns a client instance or None. Internally queues request to
        manager which performs serialized check+reserve and calls
        Exchange.get_private_ws_client.
        """
        try:
            # Ensure manager task running
            self._start_private_client_manager()
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            await self._private_client_queue.put((exchange_id, user, caller, fut))
            return await fut
        except Exception:
            return None

    async def _private_client_manager_loop(self):
        """Background manager: serially process creation requests.

        For each request we compute current counts (via Exchange.get_client_metrics()),
        decide whether to reserve a slot or return None, and call
        Exchange.get_private_ws_client while the reservation is held.
        """
        from Exchange import Exchange as _ExchCls
        import Exchange as _ExMod
        while True:
            try:
                exchange_id, user, caller, fut = await self._private_client_queue.get()
            except asyncio.CancelledError:
                break
            except Exception:
                # Shouldn't happen, but protect the loop
                try:
                    fut.set_result(None)
                except Exception:
                    pass
                continue

            key = f"{('kucoinfutures' if exchange_id == 'kucoin' else exchange_id)}:{getattr(user,'name',None)}"
            try:
                # Fast path: if Exchange already has a private client, return it
                try:
                    client = _ExchCls._private_ws_clients.get(key)
                    if client is not None:
                        fut.set_result(client)
                        continue
                except Exception:
                    pass

                # Compute effective caps
                try:
                    global_cap = int(getattr(_ExMod, '_RUNTIME_MAX_PRIVATE_WS_GLOBAL')) if getattr(_ExMod, '_RUNTIME_MAX_PRIVATE_WS_GLOBAL', None) is not None else getattr(_ExMod, 'MAX_PRIVATE_WS_GLOBAL')
                except Exception:
                    global_cap = getattr(_ExMod, 'MAX_PRIVATE_WS_GLOBAL')

                try:
                    runtime_per = getattr(_ExMod, '_RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE') or {}
                except Exception:
                    runtime_per = {}

                # Count existing private clients
                try:
                    metrics = _ExchCls.get_client_metrics()
                    total_now = sum(v.get('private_count', 0) for v in metrics.values())
                except Exception:
                    total_now = len(_ExchCls._private_ws_clients.keys())

                inflight_count = len(self._private_client_manager_state['inflight'])

                # Global cap check
                if global_cap is not None and (total_now + inflight_count) >= global_cap:
                    # emit warning once
                    if not self._private_client_manager_state.get('warned_global'):
                        try:
                            _human_log('PBData', f"[ws-manager] reached GLOBAL cap ({total_now + inflight_count}/{global_cap}); returning None for user={getattr(user,'name',None)}", level='WARNING', user=user)
                        except Exception:
                            pass
                        self._private_client_manager_state['warned_global'] = True
                    try:
                        fut.set_result(None)
                    except Exception:
                        pass
                    continue

                # Per-exchange cap
                base_key = 'kucoinfutures' if exchange_id == 'kucoin' else exchange_id
                try:
                    if runtime_per and base_key in runtime_per:
                        cap = int(runtime_per.get(base_key))
                    else:
                        cap = getattr(_ExMod, 'MAX_PRIVATE_WS_PER_EXCHANGE').get(base_key)
                except Exception:
                    cap = None

                if cap is not None:
                    try:
                        # current per-exchange existing
                        current = 0
                        for k in _ExchCls._private_ws_clients.keys():
                            if k.startswith(f"{base_key}:"):
                                current += 1
                    except Exception:
                        current = 0
                    inflight_for_exch = sum(1 for c in self._private_client_manager_state['inflight'] if c.startswith(f"{base_key}:"))
                    if (current + inflight_for_exch) >= cap:
                        warned = self._private_client_manager_state['warned_per_exch'].get(base_key, False)
                        if not warned:
                            try:
                                _human_log('PBData', f"[ws-manager] reached cap for {base_key} ({current + inflight_for_exch}/{cap}); returning None for user={getattr(user,'name',None)}", level='WARNING', user=user)
                            except Exception:
                                pass
                            self._private_client_manager_state['warned_per_exch'][base_key] = True
                        try:
                            fut.set_result(None)
                        except Exception:
                            pass
                        continue

                # Reserve an in-flight slot and perform creation
                try:
                    self._private_client_manager_state['inflight'].add(key)
                except Exception:
                    pass

                try:
                    # Call into Exchange to create/get client. Exchange itself may
                    # perform its own protections; manager ensures callers are
                    # serialized enough to avoid duplicate logs and overshoot.
                    client = await _ExchCls.get_private_ws_client(exchange_id, user, caller=caller)
                    try:
                        fut.set_result(client)
                    except Exception:
                        pass
                except Exception:
                    try:
                        fut.set_result(None)
                    except Exception:
                        pass
                finally:
                    try:
                        self._private_client_manager_state['inflight'].discard(key)
                    except Exception:
                        pass
                    # Maybe clear warning flags if capacity freed
                    try:
                        # Recompute counts and clear flags if below caps
                        metrics = _ExchCls.get_client_metrics()
                        total_now = sum(v.get('private_count', 0) for v in metrics.values())
                        if global_cap is not None and total_now + len(self._private_client_manager_state['inflight']) < global_cap:
                            self._private_client_manager_state['warned_global'] = False
                        # per-exchange
                        try:
                            curr_ex = 0
                            for k in _ExchCls._private_ws_clients.keys():
                                if k.startswith(f"{base_key}:"):
                                    curr_ex += 1
                        except Exception:
                            curr_ex = 0
                        if cap is not None and (curr_ex + sum(1 for c in self._private_client_manager_state['inflight'] if c.startswith(f"{base_key}:"))) < cap:
                            self._private_client_manager_state['warned_per_exch'][base_key] = False
                    except Exception:
                        pass
            except Exception:
                try:
                    fut.set_result(None)
                except Exception:
                    pass

    async def _private_manager_maybe_clear_flags_for_exchange(self, exchange_id: str):
        """Recompute client counts and clear manager warning flags if capacity freed.

        This is called when Exchange notifies PBData that a private client was
        closed (or by internal manager after finishing a creation) so that
        the manager's 'warned' flags don't stay stuck.
        """
        try:
            from Exchange import Exchange as _ExchCls
            import Exchange as _ExMod
            base_key = 'kucoinfutures' if exchange_id == 'kucoin' else exchange_id
            try:
                metrics = _ExchCls.get_client_metrics()
                total_now = sum(v.get('private_count', 0) for v in metrics.values())
            except Exception:
                total_now = len(_ExchCls._private_ws_clients.keys())

            # global cap
            try:
                global_cap = int(getattr(_ExMod, '_RUNTIME_MAX_PRIVATE_WS_GLOBAL')) if getattr(_ExMod, '_RUNTIME_MAX_PRIVATE_WS_GLOBAL', None) is not None else getattr(_ExMod, 'MAX_PRIVATE_WS_GLOBAL')
            except Exception:
                global_cap = getattr(_ExMod, 'MAX_PRIVATE_WS_GLOBAL')
            if global_cap is not None and total_now + len(self._private_client_manager_state['inflight']) < global_cap:
                self._private_client_manager_state['warned_global'] = False

            # per-exchange cap
            try:
                runtime_per = getattr(_ExMod, '_RUNTIME_MAX_PRIVATE_WS_PER_EXCHANGE') or {}
            except Exception:
                runtime_per = {}
            try:
                if runtime_per and base_key in runtime_per:
                    cap = int(runtime_per.get(base_key))
                else:
                    cap = getattr(_ExMod, 'MAX_PRIVATE_WS_PER_EXCHANGE').get(base_key)
            except Exception:
                cap = None
            try:
                curr_ex = metrics.get(base_key, {}).get('private_count', 0)
            except Exception:
                curr_ex = 0
            if cap is not None and (curr_ex + sum(1 for c in self._private_client_manager_state['inflight'] if c.startswith(f"{base_key}:"))) < cap:
                try:
                    self._private_client_manager_state['warned_per_exch'][base_key] = False
                except Exception:
                    pass
        except Exception:
            pass


    def _set_exchange_backoff(self, exchange: str, reason: str = None, duration: int = None, user=None):
        try:
            now = datetime.now().timestamp()
            dur = duration if duration is not None else self._backoff_duration_seconds
            until = now + dur
            self._exchange_backoff_until[exchange] = until
            # Pass username as `user` kwarg to human_log (human_log supports `user`)
            _human_log('PBData', f"[BACKOFF] Entering backoff for exchange {exchange} for {dur}s (reason={reason})", level='WARNING', user=getattr(user, 'name', None))
        except Exception:
            pass

    def _is_exchange_in_backoff(self, exchange: str) -> bool:
        """Return True if we are currently backing off for this exchange."""
        try:
            if not exchange:
                return False
            until = self._exchange_backoff_until.get(exchange, 0)
            return datetime.now().timestamp() < until
        except Exception:
            return False

    # Small helper: treat some close messages as "normal" (don't immediately demote)
    def _is_normal_ws_close(self, msg: str) -> bool:
        if not msg:
            return False
        lower = msg.lower()
        # Common benign closure patterns (code 1000 = normal closure)
        if "code 1000" in lower or "closing code 1000" in lower or "normal closure" in lower:
            return True
        return False

    # Throttled logging for repeated network errors (reduce log spam)
    def _throttled_log_network(self, key, msg: str, throttle: float = 30.0):
        now = datetime.now().timestamp()
        last = self._last_network_error_log_ts.get(key, 0.0)
        if now - last >= throttle:
            try:
                _human_log('PBData', msg, level='WARNING')
            except Exception:
                pass
            self._last_network_error_log_ts[key] = now

    async def _metrics_loop(self):
        """Periodic metrics logger: logs counts of shared/private clients and backoff states."""
        while True:
            try:
                from Exchange import Exchange
                metrics = Exchange.get_client_metrics()
                lines = []
                for exch, vals in metrics.items():
                    lines.append(f"{exch}: shared={vals.get('shared',0)} private={vals.get('private_count',0)}")
                # Also include exchanges in backoff
                backoffs = []
                now = datetime.now().timestamp()
                for exch, until in list(self._exchange_backoff_until.items()):
                    if until > now:
                        backoffs.append(f"{exch}:until={int(until-now)}s")
                if lines or backoffs:
                    _human_log('PBData', f"[METRICS] Clients: {', '.join(lines)}; Backoffs: {', '.join(backoffs) if backoffs else '(none)'}", level='INFO')
                # IO debugging removed: process/db IO summary logging disabled
            except Exception:
                try:
                    _human_log('PBData', f"[METRICS] failed to collect metrics", level='WARNING')
                except Exception:
                    pass
            await asyncio.sleep(self._metrics_interval)

    # fetch_users

    # --- Price buffering and background writer ---
    async def buffer_price(self, user, symbol: str, timestamp: int, price_value: float):
        """Buffer the latest price for (user.name, symbol).

        This stores only the most recent tick per (user,symbol).
        """
        try:
            lock = getattr(self, '_price_buffer_lock', None)
            if lock is None:
                # lazily create if missing
                self._price_buffer_lock = asyncio.Lock()
                lock = self._price_buffer_lock
            if hasattr(lock, '__aenter__'):
                async with lock:
                    self._price_buffer[(user.name, symbol)] = (timestamp, price_value)
            else:
                # synchronous lock (fallback)
                with lock:
                    self._price_buffer[(user.name, symbol)] = (timestamp, price_value)
        except Exception:
            try:
                _human_log('PBData', f"[price_buffer] buffer_price error for {getattr(user,'name',user)} {symbol}", level='DEBUG')
            except Exception:
                pass

    async def _price_writer_loop(self):
        """Background task: periodically flush buffered prices to DB."""
        while True:
            try:
                await asyncio.sleep(self._price_flush_interval)
                try:
                    await self._flush_price_buffer()
                except Exception as e:
                    try:
                        _human_log('PBData', f"[price_writer] flush failed: {e}", level='WARNING')
                    except Exception:
                        pass
            except asyncio.CancelledError:
                # On cancellation, attempt a final flush then exit
                try:
                    await self._flush_price_buffer()
                except Exception:
                    pass
                raise
            except Exception:
                # swallow and continue
                try:
                    _human_log('PBData', "[price_writer] loop encountered error; continuing", level='DEBUG')
                except Exception:
                    pass

    async def _flush_price_buffer(self):
        """Snapshot buffer and flush via blocking batch DB call."""
        try:
            lock = getattr(self, '_price_buffer_lock', None)
            if lock is None:
                self._price_buffer_lock = asyncio.Lock()
                lock = self._price_buffer_lock
            if hasattr(lock, '__aenter__'):
                async with lock:
                    if not self._price_buffer:
                        return
                    snapshot = [(uname, sym, ts, pr) for (uname, sym), (ts, pr) in self._price_buffer.items()]
                    self._price_buffer = {}
            else:
                with lock:
                    if not self._price_buffer:
                        return
                    snapshot = [(uname, sym, ts, pr) for (uname, sym), (ts, pr) in self._price_buffer.items()]
                    self._price_buffer = {}
            # Perform blocking DB batch write in thread
            await asyncio.to_thread(self._write_prices_batch_sync, snapshot)
        except Exception as e:
            try:
                _human_log('PBData', f"[price_writer] _flush_price_buffer error: {e}", level='ERROR')
            except Exception:
                pass

    def _write_prices_batch_sync(self, rows: list):
        """Blocking function run in thread to write batch rows to DB.

        rows: list of (user_name, symbol, timestamp, price)
        """
        try:
            # Database.batch_upsert_prices expects rows as (user, symbol, timestamp, price)
            formatted = [(r[0], r[1], r[2], r[3]) for r in rows]
            try:
                # Use batch method if available
                if hasattr(self.db, 'batch_upsert_prices'):
                    self.db.batch_upsert_prices(formatted)
                else:
                    # Fallback: call upsert_price per row
                    for user, symbol, ts, pr in formatted:
                        try:
                            u = self.users.find_user(user)
                            if u:
                                self.db.upsert_price(u, symbol, ts, pr)
                        except Exception:
                            pass
            except Exception as e:
                try:
                    _human_log('PBData', f"[price_writer] DB batch write failed: {e}", level='ERROR')
                except Exception:
                    pass
        except Exception:
            pass

    @property
    def fetch_users(self):
        return self._fetch_users
    @fetch_users.setter
    def fetch_users(self, new_fetch_users):
        self._fetch_users = new_fetch_users
        self.save_fetch_users()

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', str(PurePath(f'{PBGDIR}/PBData.py'))]
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
            _human_log('PBData', f"Warning: failed reading pbgui.ini ({e}); keeping previous fetch_users: {self._fetch_users}", level='WARNING')
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
                # Close any private ws client for this user to release resources
                try:
                    u = self.users.find_user(uname)
                    if u:
                        from Exchange import Exchange
                        try:
                            await Exchange.close_private_ws_client(u.exchange, u)
                        except Exception:
                            pass
                except Exception:
                    pass

    async def _reconcile_position_watchers(self, desired_user_names: set):
        for uname, task in list(self._position_ws_tasks.items()):
            if uname not in desired_user_names:
                try:
                    if task and not task.done():
                        task.cancel()
                except Exception:
                    pass
                self._position_ws_tasks.pop(uname, None)
                # Close any private ws client for this user to release resources
                try:
                    u = self.users.find_user(uname)
                    if u:
                        from Exchange import Exchange
                        try:
                            await Exchange.close_private_ws_client(u.exchange, u)
                        except Exception:
                            pass
                except Exception:
                    pass

    async def _reconcile_order_watchers(self, desired_user_names: set):
        for uname, task in list(self._order_ws_tasks.items()):
            if uname not in desired_user_names:
                try:
                    if task and not task.done():
                        task.cancel()
                except Exception:
                    pass
                self._order_ws_tasks.pop(uname, None)
                # Close any private ws client for this user to release resources
                try:
                    u = self.users.find_user(uname)
                    if u:
                        from Exchange import Exchange
                        try:
                            await Exchange.close_private_ws_client(u.exchange, u)
                        except Exception:
                            pass
                except Exception:
                    pass

    async def _balance_ws_loop(self, user):
        from Exchange import Exchange
        await asyncio.sleep((hash(user.name) % 5000) / 1000.0)
        exch = Exchange(user.exchange, user)
        ex = await self.request_private_client(user.exchange, user, caller='PBData._balance_ws_loop')
        if not ex:
            _human_log('PBData', f"[ws] ccxtpro unavailable or unsupported for {user.name} ({user.exchange}); relying on shared balances poller", level='DEBUG')
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
                _human_log('PBData', f"[ws] watch_balance not supported for {user.name} ({exch.id}); relying on shared balances poller", level='DEBUG')
                self._watch_positions_not_supported_logged.add(key)
            return
        _human_log('PBData', f"[ws] Starting balance watcher for {user.name} ({exch.id})", level='INFO')
        try:
            while True:
                # Reload settings from pbgui.ini each loop so GUI toggles
                # (e.g. ws_max or log_level) take effect quickly.
                try:
                    self._load_settings()
                except Exception:
                    pass
                try:
                    # Watch balance; details vary across exchanges
                    bal = await ex.watch_balance()
                    # Debug: optionally log payload type and a short preview so we can
                    # see whether the WS watcher actually returns balance data.
                    try:
                        if is_debug_enabled('PBData'):
                            btype = type(bal)
                            preview = repr(bal)
                            if len(preview) > 300:
                                preview = preview[:300] + '...'
                            _human_log('PBData', f"[ws] watch_balance payload for {user.name}: type={btype} preview={preview}", level='DEBUG')
                    except Exception:
                        if is_debug_enabled('PBData'):
                            try:
                                _human_log('PBData', f"[ws] watch_balance payload for {user.name}: (unrepresentable)", level='DEBUG')
                            except Exception:
                                pass

                    # On any balance update, persist balances (REST fallback)
                    try:
                        await asyncio.to_thread(self.db.update_balances, user)
                        # record last fetch timestamp for balances
                        try:
                            self._last_fetch_ts[(user.name, 'balances')] = datetime.now().timestamp()
                        except Exception:
                            pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                    except Exception as e:
                        _human_log('PBData', f"[ws->REST] DB balance update failed (REST fallback) for {user.name}: {e}", level='ERROR')
                        try:
                            await asyncio.to_thread(self.db.update_positions, user)
                            try:
                                self._last_fetch_ts[(user.name, 'positions')] = datetime.now().timestamp()
                            except Exception:
                                pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                        except Exception as e:
                            _human_log('PBData', f"[ws->REST] DB positions update failed (REST fallback) for {user.name}: {e}", level='ERROR')
                except Exception as e:
                    raw = str(e)
                    lower = raw.lower()
                    # If this was a benign/normal close (e.g. code 1000) try a reconnect
                    if self._is_normal_ws_close(raw):
                        self._throttled_log_network((user.exchange, user.name), f"[ws] normal websocket close for {user.name}: {e}; attempting reconnect", self._network_error_log_throttle)
                        try:
                            ex2 = await self.request_private_client(user.exchange, user, caller='PBData._balance_ws_loop')
                            if not ex2:
                                _human_log('PBData', f"[ws] Could not re-acquire private client for {user.name}; falling back to REST", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                return
                            await asyncio.sleep(1)
                            continue
                        except Exception:
                            try:
                                await Exchange.close_private_ws_client(user.exchange, user)
                            except Exception:
                                pass
                            return
                    # Detect keepalive/ping-pong style timeouts and attempt one restart
                    try:
                        key = (user.exchange, user.name)
                        # reset consecutive success counter on any exception
                        try:
                            self._ws_success_counts[key] = 0
                        except Exception:
                            pass
                        keepalive_triggers = ['ping-pong', 'pingpong', 'keepalive', 'requesttimeout']
                        if any(k in lower for k in keepalive_triggers) or ('timed out' in lower and 'ping' in lower):
                            if key not in self._ws_restarted_once:
                                _human_log('PBData', f"[ws] Keepalive timeout detected; restarting private ws client for {user.name} ({user.exchange})", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                await asyncio.sleep(self._ws_restart_sleep + random.random() * 0.5)
                                try:
                                    ex2 = await self.request_private_client(user.exchange, user, caller='PBData._balance_ws_loop')
                                    if ex2:
                                        self._ws_restarted_once.add(key)
                                        ex = ex2
                                        _human_log('PBData', f"[ws] Restarted private ws client for {user.name} ({user.exchange}); will not restart again until {self._ws_success_required} successful messages", level='INFO')
                                        continue
                                except Exception:
                                    pass
                        # If restart already used or recreate failed, fall through to normal handling
                    except Exception:
                        pass
                    # Detect network-level errors (connection closed/reset, remote abort)
                    network_triggers = ['connection closed', 'networkerror', 'connection reset', 'remote server', 'eof', 'connection aborted', 'broken pipe']
                    if any(k in lower for k in network_triggers) or isinstance(e, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
                        _human_log('PBData', f"[ws] watch_balance network error for {user.name}: {e}; considering demotion to REST", level='WARNING')
                        # Track recent network errors for this exchange and trigger backoff if threshold exceeded
                        try:
                            now_ts = datetime.now().timestamp()
                            l = self._exchange_error_timestamps.get(user.exchange, [])
                            l.append(now_ts)
                            # prune
                            l = [ts for ts in l if now_ts - ts <= self._error_window_seconds]
                            self._exchange_error_timestamps[user.exchange] = l
                            if len(l) >= self._error_threshold:
                                try:
                                    # include user so backoff log shows which user triggered it
                                    self._set_exchange_backoff(user.exchange, reason='network_errors', user=user)
                                    # also close shared client to force reconnect
                                    from Exchange import Exchange as _ExchCls
                                    try:
                                        await _ExchCls.close_shared_ws_client(user.exchange)
                                    except Exception:
                                        pass
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        # Coordinate demotion per-exchange so only one user is demoted
                        exch_key = user.exchange
                        # Ensure a lock exists for this exchange
                        lock = self._network_error_locks.get(exch_key)
                        if lock is None:
                            lock = asyncio.Lock()
                            self._network_error_locks[exch_key] = lock
                        async with lock:
                            now_ts = datetime.now().timestamp()
                            # Prune stale demotion entries
                            existing = self._exchange_network_error_users.get(exch_key, {})
                            stale = [uname for uname, ts in existing.items() if now_ts - ts > self._network_demotion_window]
                            for s in stale:
                                existing.pop(s, None)
                            # If no recent demotions, demote this user
                            if not existing:
                                existing[user.name] = now_ts
                                self._exchange_network_error_users[exch_key] = existing
                                _human_log('PBData', f"[ws] Demoting {user.name} to REST for exchange {exch_key} (first in window)", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                return
                            else:
                                # Another user was recently demoted; attempt to keep this user's WS alive
                                _human_log('PBData', f"[ws] Recent demotion exists for exchange {exch_key}; attempting to keep {user.name} on websocket", level='INFO')
                                try:
                                    # Try to re-acquire or recreate a private client for this user
                                    ex2 = await Exchange.get_private_ws_client(user.exchange, user, caller='PBData._balance_ws_loop')
                                    if not ex2:
                                        _human_log('PBData', f"[ws] Could not re-acquire private client for {user.name}; falling back to REST", level='WARNING')
                                        try:
                                            await Exchange.close_private_ws_client(user.exchange, user)
                                        except Exception:
                                            pass
                                        return
                                    # Short backoff before continuing loop to avoid tight error loops
                                    await asyncio.sleep(1)
                                    continue
                                except Exception:
                                    try:
                                        await Exchange.close_private_ws_client(user.exchange, user)
                                    except Exception:
                                        pass
                                    return
        finally:
            # Intentionally not closing `ex` here. Shared websocket clients are
            # kept open to avoid disrupting other watchers that may be using
            # the same client instance.
            try:
                _human_log('PBData', f"Leaving ws client open in _balance_ws_loop for {user.name} ({exch.id})", level='DEBUG')
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
        ex = await self.request_private_client(user.exchange, user, caller='PBData._position_ws_loop')
        if not ex:
            _human_log('PBData', f"[ws] ccxtpro unavailable or unsupported (positions) for {user.name} ({user.exchange})", level='DEBUG')
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
                if key not in self._watch_positions_not_supported_logged:
                    # Instead of starting a per-user REST poller (which can create many
                    # concurrent requests), rely on the shared serial poller to update
                    # positions for exchanges that don't support watchPositions.
                    _human_log('PBData', f"[ws] watch_positions not supported for {user.name} ({exch.id}); relying on shared positions poller", level='INFO')
                self._watch_positions_not_supported_logged.add(key)
            return
        _human_log('PBData', f"[ws] Starting positions watcher for {user.name} ({exch.id})", level='INFO')
        min_positions_refresh_interval = 10
        last_positions_refresh = 0
        try:
            while True:
                # Reload settings so runtime changes (ws_max, log_level) take effect
                try:
                    self._load_settings()
                except Exception:
                    pass
                try:
                    _ = await ex.watch_positions()
                    # Successful watch_positions: increment success counter and clear restart marker after threshold
                    try:
                        key = (user.exchange, user.name)
                        self._ws_success_counts[key] = self._ws_success_counts.get(key, 0) + 1
                        if self._ws_success_counts.get(key, 0) >= self._ws_success_required:
                            if key in self._ws_restarted_once:
                                self._ws_restarted_once.discard(key)
                                _human_log('PBData', f"[ws] Restart state cleared for {user.name} ({user.exchange}) after {self._ws_success_required} successful watch events", level='INFO')
                            self._ws_success_counts[key] = 0
                    except Exception:
                        pass
                    now_sec = int(datetime.now().timestamp())
                    if now_sec - last_positions_refresh >= min_positions_refresh_interval:
                        last_positions_refresh = now_sec
                        try:
                            await asyncio.to_thread(self.db.update_positions, user)
                            try:
                                self._last_fetch_ts[(user.name, 'positions')] = datetime.now().timestamp()
                            except Exception:
                                pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                        except Exception as e:
                            _human_log('PBData', f"[ws] DB positions update failed for {user.name}: {e}", level='ERROR')
                    # Debug: optionally log the positions payload
                    # Debug: optionally log the positions payload
                    try:
                        if is_debug_enabled('PBData'):
                            preview = repr(_)
                            if len(preview) > 300:
                                preview = preview[:300] + '...'
                            _human_log('PBData', f"[ws] watch_positions payload for {user.name}: type={type(_)} preview={preview}", level='DEBUG')
                    except Exception:
                        if is_debug_enabled('PBData'):
                            try:
                                _human_log('PBData', f"[ws] watch_positions payload for {user.name}: (unrepresentable)", level='DEBUG')
                            except Exception:
                                pass
                except Exception as e:
                    raw = str(e)
                    lower = raw.lower()
                    # treat normal websocket close (1000) as a reconnect opportunity
                    if self._is_normal_ws_close(raw):
                        self._throttled_log_network((user.exchange, user.name), f"[ws] normal websocket close (positions) for {user.name}: {e}; attempting reconnect", self._network_error_log_throttle)
                        try:
                            ex2 = await self.request_private_client(user.exchange, user, caller='PBData._position_ws_loop')
                            if not ex2:
                                _human_log('PBData', f"[ws] Could not re-acquire private client for {user.name} (positions); falling back to REST", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                return
                            await asyncio.sleep(1)
                            continue
                        except Exception:
                            try:
                                await Exchange.close_private_ws_client(user.exchange, user)
                            except Exception:
                                pass
                            return
                    if 'cannot track more than' in lower or ('cannot track' in lower and 'user' in lower):
                        _human_log('PBData', f"[ws] watch_positions user-limit reached for {user.name}: {e}; closing private ws client and falling back to REST", level='WARNING')
                        try:
                            await Exchange.close_private_ws_client(user.exchange, user)
                        except Exception:
                            pass
                        return
                    # Detect keepalive/ping-pong style timeouts and attempt single restart before demotion
                    try:
                        key = (user.exchange, user.name)
                        try:
                            self._ws_success_counts[key] = 0
                        except Exception:
                            pass
                        keepalive_triggers = ['ping-pong', 'pingpong', 'keepalive', 'requesttimeout']
                        if any(k in lower for k in keepalive_triggers) or ('timed out' in lower and 'ping' in lower):
                            if key not in self._ws_restarted_once:
                                _human_log('PBData', f"[ws] Keepalive timeout detected (positions); restarting private ws client for {user.name} ({user.exchange})", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                await asyncio.sleep(self._ws_restart_sleep + random.random() * 0.5)
                                try:
                                    ex2 = await self.request_private_client(user.exchange, user, caller='PBData._position_ws_loop')
                                    if ex2:
                                        self._ws_restarted_once.add(key)
                                        ex = ex2
                                        _human_log('PBData', f"[ws] Restarted private ws client for {user.name} ({user.exchange}); will not restart again until {self._ws_success_required} successful messages", level='INFO')
                                        continue
                                except Exception:
                                    pass
                            # else: fall through to normal handling
                    except Exception:
                        pass
                    # Network-level failures should cause this user to fall back to REST
                    network_triggers = ['connection closed', 'networkerror', 'connection reset', 'remote server', 'eof', 'connection aborted', 'broken pipe']
                    if any(k in lower for k in network_triggers) or isinstance(e, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
                        _human_log('PBData', f"[ws] watch_positions network error for {user.name}: {e}; considering demotion to REST", level='WARNING')
                        exch_key = user.exchange
                        lock = self._network_error_locks.get(exch_key)
                        if lock is None:
                            lock = asyncio.Lock()
                            self._network_error_locks[exch_key] = lock
                        async with lock:
                            now_ts = datetime.now().timestamp()
                            existing = self._exchange_network_error_users.get(exch_key, {})
                            stale = [uname for uname, ts in existing.items() if now_ts - ts > self._network_demotion_window]
                            for s in stale:
                                existing.pop(s, None)
                            if not existing:
                                existing[user.name] = now_ts
                                self._exchange_network_error_users[exch_key] = existing
                                _human_log('PBData', f"[ws] Demoting {user.name} to REST for exchange {exch_key} (first in window)", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                return
                            else:
                                _human_log('PBData', f"[ws] Recent demotion exists for exchange {exch_key}; attempting to keep {user.name} on websocket", level='INFO')
                                try:
                                    ex2 = await self.request_private_client(user.exchange, user, caller='PBData._position_ws_loop')
                                    if not ex2:
                                        _human_log('PBData', f"[ws] Could not re-acquire private client for {user.name}; falling back to REST")
                                        try:
                                            await Exchange.close_private_ws_client(user.exchange, user)
                                        except Exception:
                                            pass
                                        return
                                    await asyncio.sleep(1)
                                    continue
                                except Exception:
                                    try:
                                        await Exchange.close_private_ws_client(user.exchange, user)
                                    except Exception:
                                        pass
                                    return
                    _human_log('PBData', f"[ws] watch_positions error for {user.name}: {e}")
                    # Add jittered backoff to avoid synchronized reconnect storms
                    await asyncio.sleep(1 + random.random() * 4)
        finally:
            # Intentionally not closing `ex` here. Keep shared websocket clients
            # open so other watchers are not interrupted.
            try:
                _human_log('PBData', f"Leaving ws client open in _position_ws_loop for {user.name} ({exch.id})", level='DEBUG')
            except Exception:
                pass

    async def _order_ws_loop(self, user):
        from Exchange import Exchange
        await asyncio.sleep((hash(user.name) % 5000) / 1000.0)
        exch = Exchange(user.exchange, user)
        ex = await self.request_private_client(user.exchange, user, caller='PBData._order_ws_loop')
        if not ex:
            _human_log('PBData', f"[ws] ccxtpro unavailable or unsupported (orders) for {user.name} ({user.exchange}); relying on shared orders poller", level='DEBUG')
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
                _human_log('PBData', f"[ws] watch_orders not supported for {user.name} ({exch.id}); relying on shared orders poller", level='INFO')
                self._watch_positions_not_supported_logged.add(key)
            return
        _human_log('PBData', f"[ws] Starting orders watcher for {user.name} ({exch.id})", level='INFO')
        # Throttle REST order updates so we don't hammer the exchange when
        # websockets produce many events in a short time.
        min_orders_refresh_interval = 20
        last_orders_refresh = 0
        try:
            while True:
                # Reload settings so runtime changes (ws_max, log_level) take effect
                try:
                    self._load_settings()
                except Exception:
                    pass
                try:
                    orders = await ex.watch_orders()
                    # Successful watch_orders: increment success counter and clear restart marker after threshold
                    try:
                        key = (user.exchange, user.name)
                        self._ws_success_counts[key] = self._ws_success_counts.get(key, 0) + 1
                        if self._ws_success_counts.get(key, 0) >= self._ws_success_required:
                            if key in self._ws_restarted_once:
                                self._ws_restarted_once.discard(key)
                                _human_log('PBData', f"[ws] Restart state cleared for {user.name} ({user.exchange}) after {self._ws_success_required} successful watch events")
                            self._ws_success_counts[key] = 0
                    except Exception:
                        pass
                    # On orders updates, persist orders but no more often than
                    # min_orders_refresh_interval seconds to avoid excessive
                    # REST calls when many users or frequent WS updates.
                    # Debug: optionally log the orders payload
                    try:
                        if is_debug_enabled('PBData'):
                            preview = repr(orders)
                            if len(preview) > 300:
                                preview = preview[:300] + '...'
                            _human_log('PBData', f"[ws] watch_orders payload for {user.name}: type={type(orders)} preview={preview}", level='DEBUG')
                    except Exception:
                        if is_debug_enabled('PBData'):
                            try:
                                _human_log('PBData', f"[ws] watch_orders payload for {user.name}: (unrepresentable)", level='DEBUG')
                            except Exception:
                                pass
                    now_sec = int(datetime.now().timestamp())
                    if now_sec - last_orders_refresh >= min_orders_refresh_interval:
                        last_orders_refresh = now_sec
                        try:
                            await asyncio.to_thread(self.db.update_orders, user)
                            try:
                                self._last_fetch_ts[(user.name, 'orders')] = datetime.now().timestamp()
                            except Exception:
                                pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                                try:
                                    self._write_fetch_summary()
                                except Exception:
                                    pass
                        except Exception as e:
                            _human_log('PBData', f"[ws->REST] DB orders update failed (REST fallback) for {user.name}: {e}")
                except Exception as e:
                    raw = str(e)
                    lower = raw.lower()
                    # treat normal websocket close (1000) as a reconnect opportunity
                    if self._is_normal_ws_close(raw):
                        self._throttled_log_network((user.exchange, user.name), f"[ws] normal websocket close (orders) for {user.name}: {e}; attempting reconnect", self._network_error_log_throttle)
                        try:
                            ex2 = await self.request_private_client(user.exchange, user, caller='PBData._order_ws_loop')
                            if not ex2:
                                _human_log('PBData', f"[ws] Could not re-acquire private client for {user.name} (orders); falling back to REST", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                return
                            await asyncio.sleep(1)
                            continue
                        except Exception:
                            try:
                                await Exchange.close_private_ws_client(user.exchange, user)
                            except Exception:
                                pass
                            return
                    # existing network error handling follows
                    if 'cannot track more than' in lower or ('cannot track' in lower and 'user' in lower):
                        _human_log('PBData', f"[ws] watch_orders user-limit reached for {user.name}: {e}; closing private ws client and falling back to REST", level='WARNING')
                        try:
                            await Exchange.close_private_ws_client(user.exchange, user)
                        except Exception:
                            pass
                        return
                    # Detect keepalive/ping-pong style timeouts and attempt a single restart before demotion
                    try:
                        key = (user.exchange, user.name)
                        try:
                            self._ws_success_counts[key] = 0
                        except Exception:
                            pass
                        keepalive_triggers = ['ping-pong', 'pingpong', 'keepalive', 'requesttimeout']
                        if any(k in lower for k in keepalive_triggers) or ('timed out' in lower and 'ping' in lower):
                            if key not in self._ws_restarted_once:
                                _human_log('PBData', f"[ws] Keepalive timeout detected (orders); restarting private ws client for {user.name} ({user.exchange})", level='WARNING')
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                await asyncio.sleep(self._ws_restart_sleep + random.random() * 0.5)
                                try:
                                    ex2 = await self.request_private_client(user.exchange, user, caller='PBData._order_ws_loop')
                                    if ex2:
                                        self._ws_restarted_once.add(key)
                                        ex = ex2
                                        _human_log('PBData', f"[ws] Restarted private ws client for {user.name} ({user.exchange}); will not restart again until {self._ws_success_required} successful messages", level='INFO')
                                        continue
                                except Exception:
                                    pass
                            # else: fall through to normal handling
                    except Exception:
                        pass
                    # Network-level failures should cause this user to fall back to REST
                    network_triggers = ['connection closed', 'networkerror', 'connection reset', 'remote server', 'eof', 'connection aborted', 'broken pipe']
                    if any(k in lower for k in network_triggers) or isinstance(e, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
                        _human_log('PBData', f"[ws] watch_orders network error for {user.name}: {e}; considering demotion to REST", level='WARNING')
                        exch_key = user.exchange
                        lock = self._network_error_locks.get(exch_key)
                        if lock is None:
                            lock = asyncio.Lock()
                            self._network_error_locks[exch_key] = lock
                        async with lock:
                            now_ts = datetime.now().timestamp()
                            existing = self._exchange_network_error_users.get(exch_key, {})
                            stale = [uname for uname, ts in existing.items() if now_ts - ts > self._network_demotion_window]
                            for s in stale:
                                existing.pop(s, None)
                            if not existing:
                                existing[user.name] = now_ts
                                self._exchange_network_error_users[exch_key] = existing
                                _human_log('PBData', f"[ws] Demoting {user.name} to REST for exchange {exch_key} (first in window)")
                                try:
                                    await Exchange.close_private_ws_client(user.exchange, user)
                                except Exception:
                                    pass
                                return
                            else:
                                _human_log('PBData', f"[ws] Recent demotion exists for exchange {exch_key}; attempting to keep {user.name} on websocket")
                                try:
                                    ex2 = await self.request_private_client(user.exchange, user, caller='PBData._order_ws_loop')
                                    if not ex2:
                                        _human_log('PBData', f"[ws] Could not re-acquire private client for {user.name}; falling back to REST")
                                        try:
                                            await Exchange.close_private_ws_client(user.exchange, user)
                                        except Exception:
                                            pass
                                        return
                                    await asyncio.sleep(1)
                                    continue
                                except Exception:
                                    try:
                                        await Exchange.close_private_ws_client(user.exchange, user)
                                    except Exception:
                                        pass
                                    return
                    raw_msg = str(e)
                    exc_type = type(e).__name__
                    _human_log('PBData', f"[ws] watch_orders error for {user.name}: {raw_msg} (type={exc_type})")
                    # Add jittered backoff to avoid synchronized reconnect storms
                    await asyncio.sleep(1 + random.random() * 4)
        finally:
            # Intentionally not closing `ex` here. Keep shared websocket clients
            # open so other watchers are not interrupted.
            try:
                _human_log('PBData', f"Leaving ws client open in _order_ws_loop for {user.name} ({exch.id})", level='DEBUG')
            except Exception:
                pass

    async def _order_poll_loop(self, user, interval_seconds: int = 5):
        _human_log('PBData', f"[poll] Starting orders poller for {user.name}")
        while True:
            try:
                await asyncio.to_thread(self.db.update_orders, user)
                try:
                    self._last_fetch_ts[(user.name, 'orders')] = datetime.now().timestamp()
                except Exception:
                    pass
                try:
                    self._write_fetch_summary()
                except Exception:
                    pass
            except Exception as e:
                _human_log('PBData', f"[poll] Orders poll failed for {user.name}: {e}")
            await asyncio.sleep(interval_seconds)

    async def _position_poll_loop(self, user, interval_seconds: int = 5):
        _human_log('PBData', f"[poll] Starting positions poller for {user.name}")
        while True:
            try:
                await asyncio.to_thread(self.db.update_positions, user)
                try:
                    self._last_fetch_ts[(user.name, 'positions')] = datetime.now().timestamp()
                except Exception:
                    pass
                try:
                    self._write_fetch_summary()
                except Exception:
                    pass
            except Exception as e:
                _human_log('PBData', f"[poll] Positions poll failed for {user.name}: {e}")
            await asyncio.sleep(interval_seconds)

    async def _shared_poll_serial(self, kind: str, interval_seconds: int, per_exchange: bool = True):
        """Generic serial poller for 'positions', 'history', 'orders', or 'balances'."""
        backoff = 0
        max_backoff = 600
        base_interval = max(10, interval_seconds)
        _human_log('PBData', f"[poll] Starting shared serial poller kind={kind} interval={base_interval}s")
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
                # Skip this exchange while in backoff
                if exch and self._is_exchange_in_backoff(exch):
                    _human_log('PBData', f"[poll] Skipping shared {kind} poll for {exch} due to backoff")
                    continue
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
                                    _human_log('PBData', f"[poll] Skipping shared positions update for {user.name} because WS watcher active")
                                    self._last_skipped_position_log = now_ts
                                continue
                            await asyncio.to_thread(self.db.update_positions, user)
                            try:
                                self._last_fetch_ts[(user.name, 'positions')] = datetime.now().timestamp()
                            except Exception:
                                pass
                        elif kind == 'orders':
                            # Skip shared orders poll if user has active WS orders watcher
                            ws_task = self._order_ws_tasks.get(user.name)
                            if ws_task and not ws_task.done():
                                last_log = getattr(self, '_last_skipped_order_log', 0)
                                now_ts = datetime.now().timestamp()
                                if now_ts - last_log > 300:
                                    _human_log('PBData', f"[poll] Skipping shared orders update for {user.name} because WS watcher active")
                                    self._last_skipped_order_log = now_ts
                                continue
                            await asyncio.to_thread(self.db.update_orders, user)
                            try:
                                self._last_fetch_ts[(user.name, 'orders')] = datetime.now().timestamp()
                            except Exception:
                                pass
                        elif kind == 'history':
                            # Instrument shared history polling for debugging/tracing
                            key = (user.name, user.exchange)
                            start_ts = datetime.now().timestamp()
                            _human_log('PBData', f"[poll] Shared history poll START for {user.name} ({user.exchange})")
                            # Time the history update; if it is very long, mark exchange backoff
                            try:
                                await asyncio.to_thread(self.db.update_history, user)
                            except Exception as e:
                                raise
                            dur_ms = int((datetime.now().timestamp() - start_ts) * 1000)
                            if dur_ms / 1000.0 > self._long_poll_threshold_seconds:
                                # mark exchange as overloaded and backoff longer (include user for logging)
                                try:
                                    self._set_exchange_backoff(user.exchange, reason='long_history_poll', duration=self._backoff_duration_seconds * 2, user=user)
                                except Exception:
                                    pass
                            # record last successful REST history poll time per user/exchange
                            try:
                                self._history_rest_last[key] = start_ts
                                try:
                                    self._last_fetch_ts[(user.name, 'history')] = start_ts
                                except Exception:
                                    pass
                            except Exception:
                                pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                            _human_log('PBData', f"[poll] Shared history poll DONE for {user.name} ({user.exchange}) dur={dur_ms}ms")
                        elif kind == 'balances':
                            # Skip shared balances poll if user has active WS balances watcher
                            ws_task = self._balance_ws_tasks.get(user.name)
                            if ws_task and not ws_task.done():
                                last_log = getattr(self, '_last_skipped_balance_log', 0)
                                now_ts = datetime.now().timestamp()
                                if now_ts - last_log > 300:
                                    _human_log('PBData', f"[poll] Skipping shared balances update for {user.name} because WS watcher active")
                                    self._last_skipped_balance_log = now_ts
                                continue
                            await asyncio.to_thread(self.db.update_balances, user)
                            try:
                                self._last_fetch_ts[(user.name, 'balances')] = datetime.now().timestamp()
                            except Exception:
                                pass
                            try:
                                self._write_fetch_summary()
                            except Exception:
                                pass
                        # (duplicate branches removed)
                    except Exception as e:
                        msg = str(e)
                        _human_log('PBData', f"[poll] Shared {kind} poll failed for {user.name}: {e}")
                        lower = msg.lower()
                        if '429' in lower or 'too many requests' in lower or 'rate limit' in lower:
                            had_rate_limit = True
                    # Small stagger between per-user REST requests to avoid bursts
                    try:
                        pause_val = self._shared_rest_pause_by_exchange.get(exch, self._shared_rest_user_pause)
                        if pause_val and pause_val > 0:
                            jitter = random.uniform(0, pause_val * 0.2)
                            await asyncio.sleep(pause_val + jitter)
                    except Exception:
                        pass
            if had_rate_limit:
                backoff = min(max_backoff, backoff + 30)
            else:
                if backoff:
                    _human_log('PBData', f"[poll] Shared {kind} poll recovered; resetting backoff")
                backoff = 0

    async def _shared_combined_poll_serial(self, interval_seconds: int = 60, per_exchange: bool = True):
        """Shared poller that sequentially runs balances, positions and orders per user.

        This ensures only one REST "connection" worth of work is performed at a time
        for these combined kinds (per exchange), reducing parallel HTTP load.
        History polling remains separate because it can be long-running.
        """
        backoff = 0
        max_backoff = 600
        base_interval = max(10, interval_seconds)
        _human_log('PBData', f"[poll] Starting shared COMBINED poller interval={base_interval}s")
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
                if exch and self._is_exchange_in_backoff(exch):
                    _human_log('PBData', f"[poll] Skipping shared COMBINED poll for {exch} due to backoff")
                    continue
                for user in batch_users:
                    try:
                        # balances -> positions -> orders (sequential)
                        # Skip each part if a WS watcher exists for that kind
                        ws_bal = self._balance_ws_tasks.get(user.name)
                        if not (ws_bal and not ws_bal.done()):
                            await asyncio.to_thread(self.db.update_balances, user)
                            try:
                                self._last_fetch_ts[(user.name, 'balances')] = datetime.now().timestamp()
                            except Exception:
                                pass
                        ws_pos = self._position_ws_tasks.get(user.name)
                        if not (ws_pos and not ws_pos.done()):
                            await asyncio.to_thread(self.db.update_positions, user)
                            try:
                                self._last_fetch_ts[(user.name, 'positions')] = datetime.now().timestamp()
                            except Exception:
                                pass
                        ws_ord = self._order_ws_tasks.get(user.name)
                        if not (ws_ord and not ws_ord.done()):
                            await asyncio.to_thread(self.db.update_orders, user)
                            try:
                                self._last_fetch_ts[(user.name, 'orders')] = datetime.now().timestamp()
                            except Exception:
                                pass
                        try:
                            self._write_fetch_summary()
                        except Exception:
                            pass
                    except Exception as e:
                        msg = str(e)
                        _human_log('PBData', f"[poll] Shared COMBINED poll failed for {user.name}: {e}")
                        lower = msg.lower()
                        if '429' in lower or 'too many requests' in lower or 'rate limit' in lower:
                            had_rate_limit = True
                    # Small stagger between per-user REST requests to avoid bursts
                    try:
                        pause_val = self._shared_rest_pause_by_exchange.get(exch, self._shared_rest_user_pause)
                        if pause_val and pause_val > 0:
                            jitter = random.uniform(0, pause_val * 0.2)
                            await asyncio.sleep(pause_val + jitter)
                    except Exception:
                        pass
            if had_rate_limit:
                backoff = min(max_backoff, backoff + 30)
            else:
                if backoff:
                    _human_log('PBData', f"[poll] Shared COMBINED poll recovered; resetting backoff")
                backoff = 0

    async def _balance_poll_loop(self, user, interval_seconds: int = 30):
        _human_log('PBData', f"[poll] Starting balance poller for {user.name}")
        while True:
            try:
                await asyncio.to_thread(self.db.update_balances, user)
                try:
                    self._last_fetch_ts[(user.name, 'balances')] = datetime.now().timestamp()
                except Exception:
                    pass
            except Exception as e:
                _human_log('PBData', f"[poll] Balance poll failed for {user.name}: {e}")
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
            ex = await Exchange.get_shared_ws_client(exchange)
            if not ex:
                _human_log('PBData', f"[ws] ccxtpro unavailable (price) for exchange {exchange}; retrying in 10s")
                await asyncio.sleep(10)
                continue
            _human_log('PBData', f"[ws] Starting price watcher for exchange {exchange}")
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
                _human_log('PBData', f"[ws] load_markets failed for exchange {exchange}: {e}")
            try:
                subscribe_backoff = 0
                while True:
                    cfg = self._price_exchange_config.get(exchange, {})
                    symbols = list(cfg.get('symbols', set()))
                    mapping = cfg.get('mapping', {})
                    # Build a full list of unique users for this mapping so
                    # we can log and react deterministically to subscribe
                    # rejection errors (some exchanges impose a hard limit).
                    unique_users_all = []
                    seen_all = set()
                    for ccxt_sym, lst in mapping.items():
                        for user_name, _ in lst:
                            if user_name not in seen_all:
                                seen_all.add(user_name)
                                unique_users_all.append(user_name)
                    # Apply per-exchange user tracking limits if configured.
                    user_limit = self._price_subscribe_user_limit_by_exchange.get(exchange)
                    if user_limit is not None:
                        # Build ordered list of unique users from mapping and trim if necessary
                        unique_users = []
                        seen = set()
                        for ccxt_sym, lst in mapping.items():
                            for user_name, _ in lst:
                                if user_name not in seen:
                                    seen.add(user_name)
                                    unique_users.append(user_name)
                        if len(unique_users) > user_limit:
                            allowed_list = unique_users[:user_limit]
                            blocked_list = unique_users[user_limit:]
                            allowed = set(allowed_list)
                            # Build reduced mapping that only includes allowed users
                            reduced = {}
                            for ccxt_sym, lst in mapping.items():
                                filtered = [t for t in lst if t[0] in allowed]
                                if filtered:
                                    reduced[ccxt_sym] = filtered
                            # Persist reduced mapping back into the exchange config so
                            # subsequent iterations don't re-evaluate the full mapping
                            try:
                                cfg['mapping'] = reduced
                                cfg['symbols'] = set(reduced.keys())
                                self._price_exchange_config[exchange] = cfg
                            except Exception:
                                pass
                            # Log the trimming only when the allowed set changed
                            prev = self._price_subscribe_trim_snapshot.get(exchange)
                            curr_snapshot = tuple(allowed_list)
                            if prev != curr_snapshot:
                                _human_log('PBData', f"[ws] Exchange {exchange} has {len(unique_users)} users but limit is {user_limit}; subscribing only for {len(allowed_list)} users and falling back to REST for others")
                                try:
                                    _human_log('PBData', f"[ws] Allowed users for {exchange}: {', '.join(allowed_list)}")
                                    _human_log('PBData', f"[ws] Blocked users for {exchange}: {', '.join(blocked_list)}")
                                except Exception:
                                    pass
                                self._price_subscribe_trim_snapshot[exchange] = curr_snapshot
                            mapping = reduced
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
                        _human_log('PBData', f"[ws] Price heartbeat for exchange {exchange}: {len(symbols)} symbol(s), mode={'batch' if supports_batch else 'per-symbol'}, ticks_since_last={tick_count}")
                        self._price_ticks_count[exchange] = 0
                        last_heartbeat_ts = now_ts
                    if not started_logged:
                        mode = 'batch watch_tickers' if supports_batch else 'per-symbol watch_ticker'
                        _human_log('PBData', f"[ws] Price stream active for exchange {exchange}: {len(symbols)} symbol(s), mode={mode}")
                        started_logged = True

                    if supports_batch:
                        # Ensure we only attempt to subscribe to newly added
                        # symbols to avoid re-subscribing and triggering
                        # 'already subscribed' errors from exchanges.
                        requested_set = set(symbols)
                        subscribed = self._price_subscribed_symbols.get(exchange, set())
                        added = list(requested_set - subscribed)
                        if added:
                            # Subscribe in chunks to avoid sending very large batch
                            # subscriptions which some exchanges (e.g. hyperliquid)
                            # may reject or rate-limit. Allow per-exchange overrides.
                            per_ex_chunk = getattr(self, '_price_subscribe_chunk_size_by_exchange', None)
                            if per_ex_chunk and exchange in per_ex_chunk:
                                chunk_size = per_ex_chunk[exchange]
                            else:
                                chunk_size = getattr(self, '_price_subscribe_chunk_size', 20)
                            try:
                                for i in range(0, len(added), chunk_size):
                                    chunk = added[i:i+chunk_size]
                                    try:
                                        # give a bit more timeout headroom for some exchanges
                                        timeout_val = getattr(self, '_price_watch_timeout', None)
                                        if timeout_val:
                                            await asyncio.wait_for(ex.watch_tickers(chunk), timeout=timeout_val)
                                        else:
                                            await ex.watch_tickers(chunk)
                                        # Merge successful chunk into subscribed set
                                        subscribed = subscribed.union(set(chunk))
                                        self._price_subscribed_symbols[exchange] = subscribed
                                        # reset backoff on success of any chunk
                                        subscribe_backoff = 0
                                    except Exception as e:
                                        raw = str(e)
                                        lower = raw.lower()
                                        # Treat transient/connect timeouts specially (ccxt RequestTimeout / "connection timeout")
                                        if 'timeout' in lower or 'requesttimeout' in lower or 'connection timeout' in lower:
                                            _human_log('PBData', f"[ws] watch_tickers subscribe TIMEOUT for exchange {exchange} (chunk {i}-{i+chunk_size}): {e}; backing off")
                                            subscribe_backoff = min(subscribe_backoff + 1, 6)
                                            # if repeated timeouts, nudge a reconnect of shared client
                                            if subscribe_backoff >= 3:
                                                try:
                                                    await Exchange.close_shared_ws_client(exchange)
                                                except Exception:
                                                    pass
                                                await asyncio.sleep(30)
                                            else:
                                                await asyncio.sleep(min(5 * subscribe_backoff, 30))
                                            # abort subscription iteration to allow outer loop to re-evaluate
                                            raise RuntimeError("subscribe_chunk_failed")
                                        # Treat 'already subscribed' as non-fatal and merge
                                        if 'already subscribed' in lower or 'already subscribed' in raw:
                                            subscribed = subscribed.union(set(chunk))
                                            self._price_subscribed_symbols[exchange] = subscribed
                                            _human_log('PBData', f"[ws] watch_tickers subscribe: already subscribed for exchange {exchange}: {e}; continuing")
                                            continue
                                        # Detect exchange-enforced subscribe limits (e.g. hyperliquid)
                                        if 'cannot track more than' in lower or 'cannot track more than' in raw:
                                            try:
                                                _human_log('PBData', f"[ws] watch_tickers subscribe REJECTED by exchange {exchange}: {e}; users={len(unique_users_all)}; closing shared client and entering backoff")
                                            except Exception:
                                                pass
                                            try:
                                                await Exchange.close_shared_ws_client(exchange)
                                            except Exception:
                                                pass
                                            # Put the exchange into backoff to avoid rapid retry
                                            try:
                                                self._set_exchange_backoff(exchange, reason='subscribe_limit')
                                            except Exception:
                                                pass
                                            # Allow time for exchange/client to settle
                                            await asyncio.sleep(30 + random.uniform(0, 5))
                                            # Abort current subscription loop and continue outer loop
                                            raise RuntimeError("subscribe_chunk_failed")
                                        # For other errors, log full traceback to aid debugging
                                        tb = traceback.format_exc()
                                        _human_log('PBData', f"[ws] watch_tickers subscribe ERROR for exchange {exchange} (chunk {i}-{i+chunk_size}): {e}\n{tb}")
                                        # If we repeatedly fail, close shared client to force a fresh reconnect
                                        subscribe_backoff = min(subscribe_backoff + 1, 6)
                                        if subscribe_backoff >= 3:
                                            try:
                                                await Exchange.close_shared_ws_client(exchange)
                                            except Exception:
                                                pass
                                            # longer sleep to allow network/exchange to recover
                                            await asyncio.sleep(30)
                                        else:
                                            delay = min(5 * subscribe_backoff, 30)
                                            await asyncio.sleep(delay)
                                        # Abort current subscription loop and continue outer loop
                                        raise RuntimeError("subscribe_chunk_failed")
                            except RuntimeError:
                                # Subscription chunk failed; go to next outer iteration
                                continue
                        # Now wait for tickers for the currently requested set. Use the
                        # requested_set (not 'added') to receive updates for all
                        # symbols of interest.
                        try:
                            timeout_val = getattr(self, '_price_watch_timeout', None)
                            if timeout_val:
                                tickers = await asyncio.wait_for(ex.watch_tickers(list(requested_set)), timeout=timeout_val)
                            else:
                                tickers = await ex.watch_tickers(list(requested_set))
                        except asyncio.TimeoutError:
                            _human_log('PBData', f"[ws] watch_tickers TIMEOUT for exchange {exchange}; reconnecting price client")
                            ex = await Exchange.get_shared_ws_client(exchange)
                            if not ex:
                                _human_log('PBData', f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                return
                            subscribe_backoff = min(subscribe_backoff + 1, 6)
                            delay = min(5 * subscribe_backoff, 30)
                            await asyncio.sleep(delay)
                            continue
                        except Exception as e:
                            raw = str(e)
                            lower = raw.lower()
                            # If this was a benign/normal close (e.g. code 1000) attempt lightweight reconnect
                            if self._is_normal_ws_close(raw):
                                _human_log('PBData', f"[ws] watch_tickers normal websocket close for exchange {exchange}: {e}; attempting reconnect")
                                ex = await Exchange.get_shared_ws_client(exchange)
                                if not ex:
                                    _human_log('PBData', f"[ws] ccxtpro unavailable (price) after normal close reconnect for exchange {exchange}")
                                    return
                                await asyncio.sleep(1)
                                continue
                            # If the exchange enforces a hard subscribe limit, close client and backoff
                            if 'cannot track more than' in lower or 'cannot track more than' in raw:
                                try:
                                    _human_log('PBData', f"[ws] watch_tickers subscribe REJECTED by exchange {exchange}: {e}; users={len(unique_users_all)}; closing shared client and entering backoff")
                                except Exception:
                                    pass
                                try:
                                    await Exchange.close_shared_ws_client(exchange)
                                except Exception:
                                    pass
                                try:
                                    self._set_exchange_backoff(exchange, reason='subscribe_limit')
                                except Exception:
                                    pass
                                await asyncio.sleep(30 + random.uniform(0, 5))
                                continue
                            # Treat ccxt RequestTimeout / connection timeouts as transient here too
                            if 'timeout' in lower or 'requesttimeout' in lower or 'connection timeout' in lower:
                                _human_log('PBData', f"[ws] watch_tickers REQUESTTIMEOUT for exchange {exchange}: {e}; reconnecting price client (transient)")
                                ex = await Exchange.get_shared_ws_client(exchange)
                                if not ex:
                                    _human_log('PBData', f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                    return
                                subscribe_backoff = min(subscribe_backoff + 1, 6)
                                delay = min(5 * subscribe_backoff, 30)
                                await asyncio.sleep(delay)
                                continue
                            _human_log('PBData', f"[ws] watch_tickers ERROR for exchange {exchange}: {e}")
                            ex = await Exchange.get_shared_ws_client(exchange)
                            if not ex:
                                _human_log('PBData', f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
                                return
                            subscribe_backoff = min(subscribe_backoff + 1, 6)
                            delay = min(5 * subscribe_backoff, 30)
                            await asyncio.sleep(delay)
                            continue
                        except asyncio.TimeoutError:
                            _human_log('PBData', f"[ws] watch_tickers TIMEOUT for exchange {exchange}; reconnecting price client")
                            # Re-acquire shared client instance; don't close existing shared instance here.
                            ex = await Exchange.get_shared_ws_client(exchange)
                            if not ex:
                                _human_log('PBData', f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                return
                            subscribe_backoff = min(subscribe_backoff + 1, 6)
                            delay = min(5 * subscribe_backoff, 30)
                            await asyncio.sleep(delay)
                            continue
                        except Exception as e:
                            raw = str(e)
                            lower = raw.lower()
                            # Some exchange implementations (e.g. bybit) raise an
                            # 'already subscribed' error when a topic is re-subscribed.
                            # Treat this as non-fatal: wait briefly and continue
                            # without force-closing the client to avoid races.
                            if 'already subscribed' in lower or 'already subscribed' in raw:
                                _human_log('PBData', f"[ws] watch_tickers: already subscribed for exchange {exchange}: {e}; ignoring and continuing")
                                await asyncio.sleep(1)
                                continue
                            _human_log('PBData', f"[ws] watch_tickers ERROR for exchange {exchange}: {e}")
                            # Re-acquire shared client instance for subsequent iterations.
                            ex = await Exchange.get_shared_ws_client(exchange)
                            if not ex:
                                _human_log('PBData', f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
                                return
                            subscribe_backoff = min(subscribe_backoff + 1, 6)
                            delay = min(5 * subscribe_backoff, 30)
                            await asyncio.sleep(delay)
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
                                            await self.buffer_price(user, internal_symbol, ts, last)
                                except Exception as e:
                                    _human_log('PBData', f"[ws] upsert_price failed {user_name} {internal_symbol}: {e}")
                    else:
                        # Fallback: iterate symbols and watch individually (still single task)
                        ts_now = int(datetime.now().timestamp() * 1000)
                        for ccxt_symbol in symbols:
                            try:
                                timeout_val = getattr(self, '_price_watch_timeout', None)
                                if timeout_val:
                                    try:
                                        ticker = await asyncio.wait_for(ex.watch_ticker(ccxt_symbol), timeout=timeout_val)
                                    except asyncio.TimeoutError:
                                        _human_log('PBData', f"[ws] watch_ticker TIMEOUT exchange {exchange} {ccxt_symbol}; reconnecting price client")
                                        ex = await Exchange.get_shared_ws_client(exchange)
                                        if not ex:
                                            _human_log('PBData', f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                            raise RuntimeError("price client unavailable after watch_ticker timeout")
                                        continue
                                else:
                                    try:
                                        ticker = await ex.watch_ticker(ccxt_symbol)
                                    except Exception as e:
                                        raw = str(e)
                                        lower = raw.lower()
                                        if 'timeout' in lower or 'requesttimeout' in lower or 'connection timeout' in lower:
                                            _human_log('PBData', f"[ws] watch_ticker REQUESTTIMEOUT exchange {exchange} {ccxt_symbol}: {e}; reconnecting price client (transient)")
                                            ex = await Exchange.get_shared_ws_client(exchange)
                                            if not ex:
                                                _human_log('PBData', f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                                raise RuntimeError("price client unavailable after watch_ticker error")
                                            await asyncio.sleep(1)
                                            continue
                                        if self._is_normal_ws_close(raw):
                                            _human_log('PBData', f"[ws] watch_ticker normal websocket close exchange {exchange} {ccxt_symbol}: {e}; attempting reconnect")
                                            ex = await Exchange.get_shared_ws_client(exchange)
                                            if not ex:
                                                _human_log('PBData', f"[ws] ccxtpro unavailable (price) after reconnect for exchange {exchange}")
                                                raise RuntimeError("price client unavailable after watch_ticker error")
                                            await asyncio.sleep(1)
                                            continue
                                        _human_log('PBData', f"[ws] watch_ticker ERROR exchange {exchange} {ccxt_symbol}: {e}; reconnecting price client")
                                        ex = await Exchange.get_shared_ws_client(exchange)
                                        if not ex:
                                            _human_log('PBData', f"[ws] ccxtpro unavailable (price) after error reconnect for exchange {exchange}")
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
                                                await self.buffer_price(user, internal_symbol, ts, last)
                                                # _human_log('PBData', f"[ws] upsert_price wrote {user.name} {internal_symbol} price={last} ts={ts}")
                                    except Exception as e:
                                        _human_log('PBData', f"[ws] upsert_price failed {user_name} {internal_symbol}: {e}")
                            except Exception as e:
                                _human_log('PBData', f"[ws] watch_ticker error exchange {exchange} {ccxt_symbol}: {e}")
                                await asyncio.sleep(0.5)
            except Exception as e:
                raw = str(e)
                # treat normal closes as less severe and attempt graceful reconnect
                if self._is_normal_ws_close(raw):
                    _human_log('PBData', f"[ws] price loop normal websocket close exchange {exchange}: {e}; restarting price watcher after short pause")
                    await asyncio.sleep(2)
                    continue
                _human_log('PBData', f"[ws] price loop error exchange {exchange}: {e}; restarting price watcher")
            finally:
                # Do not close the websocket client here. Closing a shared
                # client can abort other watchers that rely on the same
                # underlying connection. Leave cleanup to a centralized
                # manager (Exchange) or process shutdown logic.
                try:
                        _human_log('PBData', f"Leaving price ws client open for exchange {exchange}", level='DEBUG')
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
            _human_log('PBData', f"[ws] Stopped price watcher for exchange {exchange}: no symbols for fetch_users")
            self._price_exchange_config.pop(exchange, None)
            return
        cfg = self._price_exchange_config.get(exchange)
        if not cfg:
            self._price_exchange_config[exchange] = {'mapping': mapping, 'symbols': symbols}
            _human_log('PBData', f"[ws] Preparing price watcher for exchange {exchange}: {len(symbols)} symbol(s)")
        else:
            # Update mapping and symbols; log changes if any
            old_symbols = cfg.get('symbols', set())
            if old_symbols != symbols:
                added = symbols - old_symbols
                removed = old_symbols - symbols
                _human_log('PBData', f"[ws] Updated symbols for exchange {exchange}: {len(symbols)} now; +{len(added)} / -{len(removed)}")
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
            _human_log('PBData', f"[async] Building price mapping for exchange {exchange} with {len(users)} user(s)")
            mapping = {}
            for user in users:
                try:
                    positions = await asyncio.to_thread(self.db.fetch_positions, user)
                except Exception as e:
                    _human_log('PBData', f"[async] fetch_positions failed for {user.name} ({exchange}): {e}")
                    continue
                for pos in positions:
                    internal_symbol = pos[1]
                    ccxt_symbol = self._to_ccxt_symbol(internal_symbol)
                    mapping.setdefault(ccxt_symbol, []).append((user.name, internal_symbol))
            # Always log the final symbol count so we know whether the
            # mapping contains anything for this exchange.
            _human_log('PBData', f"[async] Built price mapping for exchange {exchange}: {len(mapping)} symbol(s)")
            await self._ensure_exchange_price_watcher(exchange, mapping)
        except Exception as e:
            _human_log('PBData', f"[async] _build_price_mapping_for_exchange error {exchange}: {e}")

    async def update_db_async(self):
        # Load users first so filtering in load_fetch_users is correct
        self.users.load()
        self.load_fetch_users()
        # Reload debug setting if pbgui.ini changed
        try:
            self._load_settings()
        except Exception:
            pass
        now_ts = datetime.now().timestamp()
        # Only log when the set changes or periodically
        current_users_set = set(self.fetch_users)
        if current_users_set != self._last_fetch_users_snapshot or (now_ts - self._last_queue_log_ts) >= self._queue_log_every_secs:
            _human_log('PBData', f"[async] Will process users: {self.fetch_users}")
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
                _human_log('PBData', f"[async] Queueing {count} user(s) for exchange: {exchange}")
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
            await asyncio.sleep(self._private_ws_stagger_ms / 1000.0)

        # Phase 2: start orders watchers
        for u in phase2_users:
            await self._ensure_order_watcher(u)
            # Small stagger to avoid bursts of order watchers
            await asyncio.sleep(self._private_ws_stagger_ms / 1000.0)

        # Start shared serial pollers only after a grace period so that
        # websocket startup and initial subscriptions don't coincide with
        # heavy REST history/position/order/balance traffic.
        now_ts = datetime.now().timestamp()
        if now_ts >= self._pollers_enabled_after_ts:
            try:
                # Start a combined poller that sequentially runs balances, positions
                # and orders to avoid parallel REST connections, plus a separate
                # history poller (history can be long-running and is kept separate).
                if not hasattr(self, "_shared_combined_task") or self._shared_combined_task is None or self._shared_combined_task.done():
                    # Use a slightly longer interval for the combined poller to reduce REST load
                    self._shared_combined_task = asyncio.create_task(self._shared_combined_poll_serial(90, per_exchange=True))
                if not hasattr(self, "_shared_history_task") or self._shared_history_task is None or self._shared_history_task.done():
                    self._shared_history_task = asyncio.create_task(self._shared_poll_serial('history', 90, per_exchange=True))
            except Exception as e:
                _human_log('PBData', f"Error starting shared pollers: {e}", level='DEBUG')
        else:
            try:
                remaining = int(self._pollers_enabled_after_ts - now_ts)
                _human_log('PBData', f"[poll] Shared pollers delayed: starting in {remaining}s (now={int(now_ts)} enable_after={int(self._pollers_enabled_after_ts)})", level='DEBUG')
            except Exception:
                pass

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

            # Include last-fetch (minutes ago) per user/kind where available
            now_ts = datetime.now().timestamp()
            def last_minutes(user_name, kind):
                try:
                    ts = self._last_fetch_ts.get((user_name, kind))
                    if not ts:
                        return '(never)'
                    mins = int((now_ts - ts) // 60)
                    if mins <= 0:
                        return '0m'
                    return f'{mins}m'
                except Exception:
                    return '(unknown)'

            # Build a single multi-line, human-friendly summary block
            def join_csv(lst):
                return ', '.join(lst) if lst else '(none)'

            summary_lines = []
            summary_lines.append("[summary] Fetch method summary:")
            summary_lines.append(f"[summary] Balances: ws={len(balances_ws)} rest={len(balances_rest)}")
            summary_lines.append(f"[summary]   ws: {join_csv(balances_ws)}")
            # append last-fetch per user for balances
            summary_lines.append(f"[summary]   last_fetch_balances: " + ", ".join([f"{u}={last_minutes(u,'balances')}" for u in all_users]))
            summary_lines.append(f"[summary]   rest: {join_csv(balances_rest)}")
            summary_lines.append(f"[summary] Positions: ws={len(positions_ws)} rest={len(positions_rest)}")
            summary_lines.append(f"[summary]   ws: {join_csv(positions_ws)}")
            summary_lines.append(f"[summary]   last_fetch_positions: " + ", ".join([f"{u}={last_minutes(u,'positions')}" for u in all_users]))
            summary_lines.append(f"[summary]   rest: {join_csv(positions_rest)}")
            summary_lines.append(f"[summary] Orders: ws={len(orders_ws)} rest={len(orders_rest)}")
            summary_lines.append(f"[summary]   ws: {join_csv(orders_ws)}")
            summary_lines.append(f"[summary]   last_fetch_orders: " + ", ".join([f"{u}={last_minutes(u,'orders')}" for u in all_users]))
            summary_lines.append(f"[summary]   rest: {join_csv(orders_rest)}")
            summary_lines.append(f"[summary] History last_fetch: " + ", ".join([f"{u}={last_minutes(u,'history')}" for u in all_users]))
            summary_lines.append(f"[summary] History (rest): {join_csv(all_users)}")
            try:
                # Also write machine-readable JSON summary for GUI/monitoring
                try:
                    # Build machine-readable summary including last-fetch timestamps
                    lf = {}
                    try:
                        for u in all_users:
                            # Prefer the unified _last_fetch_ts mapping, but history
                            # timestamps may also be tracked in _history_rest_last
                            # under keys (user, exchange). Use a fallback to fill
                            # history if the primary mapping is missing.
                            hist_ts = self._last_fetch_ts.get((u, 'history'))
                            if not hist_ts:
                                # search _history_rest_last for any matching user
                                try:
                                    found = None
                                    for (uname, exch), ts in list(self._history_rest_last.items()):
                                        if uname == u and ts:
                                            if not found or (ts and ts > found):
                                                found = ts
                                    if found:
                                        hist_ts = found
                                except Exception:
                                    pass
                            lf[u] = {
                                'balances': self._last_fetch_ts.get((u, 'balances')),
                                'positions': self._last_fetch_ts.get((u, 'positions')),
                                'orders': self._last_fetch_ts.get((u, 'orders')),
                                'history': hist_ts,
                            }
                    except Exception:
                        pass
                    summary_obj = {
                        'timestamp': datetime.now().isoformat(sep=' ', timespec='seconds'),
                        'balances': {'ws': balances_ws, 'rest': balances_rest},
                        'positions': {'ws': positions_ws, 'rest': positions_rest},
                        'orders': {'ws': orders_ws, 'rest': orders_rest},
                        'history': all_users,
                        'last_fetch_ts': lf,
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
                # Note: remove multi-line human-readable summary log to avoid
                # making a single log entry out of many lines (which breaks
                # leading-tag parsing). The machine-readable `fetch_summary.json`
                # remains as the canonical summary.
            except Exception:
                # If writing JSON fails, fall back to logging a minimal error
                try:
                    _human_log('PBData', f"[summary] Failed to write fetch_summary.json")
                except Exception:
                    pass
        except Exception as e:
            try:
                _human_log('PBData', f"[summary] Failed to build fetch method summary: {e}")
            except Exception:
                pass

    def _write_fetch_summary(self):
        """Write machine-readable `fetch_summary.json` from current in-memory state.

        Safe to call from websocket/poller loops; exceptions are swallowed.
        """
        try:
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

            # Compose last-fetch mapping with history fallback to _history_rest_last
            lf = {}
            for u in all_users:
                hist_ts = self._last_fetch_ts.get((u, 'history'))
                if not hist_ts:
                    try:
                        found = None
                        for (uname, exch), ts in list(self._history_rest_last.items()):
                            if uname == u and ts:
                                if not found or (ts and ts > found):
                                    found = ts
                        if found:
                            hist_ts = found
                    except Exception:
                        pass
                lf[u] = {
                    'balances': self._last_fetch_ts.get((u, 'balances')),
                    'positions': self._last_fetch_ts.get((u, 'positions')),
                    'orders': self._last_fetch_ts.get((u, 'orders')),
                    'history': hist_ts,
                }

            summary_obj = {
                'timestamp': datetime.now().isoformat(sep=' ', timespec='seconds'),
                'balances': {'ws': balances_ws, 'rest': balances_rest},
                'positions': {'ws': positions_ws, 'rest': positions_rest},
                'orders': {'ws': orders_ws, 'rest': orders_rest},
                'history': all_users,
                'last_fetch_ts': lf,
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
            # never raise from this helper
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
        # Start metrics loop so periodic client/backoff metrics appear in logs
        try:
            if not hasattr(pbdata, '_metrics_task') or pbdata._metrics_task is None or pbdata._metrics_task.done():
                pbdata._metrics_task = asyncio.create_task(pbdata._metrics_loop())
        except Exception:
            pass

        # Start price writer background task if not already running
        try:
            if not hasattr(pbdata, '_price_writer_task') or pbdata._price_writer_task is None or getattr(pbdata, '_price_writer_task').done():
                pbdata._price_writer_task = asyncio.create_task(pbdata._price_writer_loop())
        except Exception:
            pass

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

    async def _shutdown():
        try:
            print(f"{datetime.now().isoformat(sep=' ', timespec='seconds')} [PBData] Shutdown requested: closing ws clients")
            from Exchange import Exchange
            try:
                await Exchange.close_all_ws_clients()
            except Exception:
                pass
            # Disable further buffering and attempt a final flush before cancelling tasks
            try:
                try:
                    pbdata._price_buffer_enabled = False
                except Exception:
                    pass
                if hasattr(pbdata, '_price_writer_task') and pbdata._price_writer_task is not None:
                    # Ask PBData to flush buffer synchronously with a timeout
                    try:
                        await asyncio.wait_for(pbdata._flush_price_buffer(), timeout=10)
                    except Exception as e:
                        try:
                            _human_log('PBData', f"[shutdown] final price buffer flush failed/timeout: {e}", level='WARNING')
                        except Exception:
                            pass
                    # Cancel the writer task if it's still running and wait briefly
                    try:
                        if not pbdata._price_writer_task.done():
                            pbdata._price_writer_task.cancel()
                            try:
                                await asyncio.wait_for(pbdata._price_writer_task, timeout=5)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
            # Cancel remaining tasks (except current)
            try:
                cur = asyncio.current_task()
                for t in list(asyncio.all_tasks()):
                    if t is not cur:
                        try:
                            t.cancel()
                        except Exception:
                            pass
            except Exception:
                pass
            # Close any per-thread cached DB connections to release fds
            try:
                if hasattr(pbdata, 'db') and hasattr(pbdata.db, 'close_thread_connections'):
                    try:
                        pbdata.db.close_thread_connections()
                    except Exception:
                        pass
            except Exception:
                pass
        finally:
            try:
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
            except Exception:
                pass
            try:
                print(f"{datetime.now().isoformat(sep=' ', timespec='seconds')} [PBData] Exiting")
            except Exception:
                pass
            try:
                os._exit(0)
            except Exception:
                pass

    async def _runner_with_signals():
        try:
            loop = asyncio.get_running_loop()
            import signal as _signal
            for s in (_signal.SIGINT, _signal.SIGTERM):
                try:
                    loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(_shutdown()))
                except Exception:
                    pass
        except Exception:
            pass
        await run_loop()

    try:
        asyncio.run(_runner_with_signals())
    except Exception:
        try:
            asyncio.run(run_loop())
        except Exception:
            pass

if __name__ == '__main__':
    main()