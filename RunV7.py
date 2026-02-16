import streamlit as st
import streamlit_scrollable_textbox as stx
import pbgui_help
from pbgui_func import pbdir, PBGDIR, load_symbols_from_ini, validateHJSON, st_file_selector, info_popup, error_popup
from PBCoinData import CoinData, normalize_symbol
from PBRemote import PBRemote
from User import Users
from Config import Config, ConfigV7, Logging
from Exchange import Exchange
from pathlib import Path
import glob
import json
from shutil import rmtree

class V7Instance():
    MODE_OPTIONS = {
        '': "",
        'n': "normal",
        'm': "manual",
        'gs': "graceful_stop",
        'p': "panic",
        't': "take_profit_only",
        }
    MODE = ['','n','m','gs','p','t']
    TIME_IN_FORCE = ['good_till_cancelled', 'post_only']

    def __init__(self):
        self.instance_path = None
        self._users = Users()
        v7_users = self._users.list_v7()
        if v7_users:
            self._user = v7_users[0]
        else:
            self._user = ""
        self.config = ConfigV7()
        self.initialize()

    # user
    @property
    def user(self): return self._user
    @user.setter
    def user(self, new_user):
        if new_user != self._user:
            self._user = new_user
            self.initialize()
    @property
    def version(self): return self.config.pbgui.version
    @property
    def enabled_on(self): return self.config.pbgui.enabled_on
    @property
    def running_version(self):
        if self.enabled_on == self.remote.name:
            version = self.remote.local_run.instances_status_v7.find_version(self.user)
        elif self.enabled_on in self.remote.list():
            self.remote.find_server(self.enabled_on).instances_status_v7.load()
            version = self.remote.find_server(self.enabled_on).instances_status_v7.find_version(self.user)
        else:
            version = 0
        return version

    def initialize(self):
        # Init config
        self.config.live.user = self._user
        # Init PBremote
        if 'remote' not in st.session_state:
            st.session_state.remote = PBRemote()
        self.remote = st.session_state.remote

    def is_running(self):
        if self.enabled_on == self.remote.name:
            return self.remote.local_run.instances_status_v7.is_running(self.user)
        elif self.enabled_on in self.remote.list():
            return self.remote.find_server(self.enabled_on).instances_status_v7.is_running(self.user)
        return False

    def is_running_on(self):
        running_on = []
        if self.remote.local_run.instances_status_v7.is_running(self.user):
            running_on.append(self.remote.name)
        for server in self.remote.list():
            if self.remote.find_server(server).instances_status_v7.is_running(self.user):
                running_on.append(server)
        return running_on

    def remove(self):
        rmtree(self.instance_path, ignore_errors=True)

    def view_log(self):
        logfile = Path(f'{self.instance_path}/passivbot.log')
        logr = ""
        if logfile.exists():
            with open(logfile, 'r', encoding='utf-8') as f:
                log = f.readlines()
                for line in reversed(log):
                    logr = logr+line
        log = logr
        st.button(':recycle: **passivbot logfile**')
        stx.scrollableTextbox(log,height="500")

    def load(self, path: Path):
        self._user = path.split('/')[-1]
        self.instance_path = Path(f'{PBGDIR}/data/run_v7/{self.user}')
        self.config.config_file = Path(f'{self.instance_path}/config.json') 
        self.config.load_config()
        self.initialize()

    def save(self):
        self.config.pbgui.version += 1
        self.config.backtest.exchange = self._users.find_exchange(self.user)
        if self.config.backtest.exchange in ['bitget', 'okx','hyperliquid']:
            self.config.backtest.exchange = 'binance'
        self.config.backtest.base_dir = f'backtests/pbgui/{self.user}'
        self.instance_path = Path(f'{PBGDIR}/data/run_v7/{self.user}')
        self.config.config_file = Path(f'{self.instance_path}/config.json') 
        self.config.save_config()
        if "edit_run_v7_version" in st.session_state:
            del st.session_state.edit_run_v7_version

    @st.fragment
    def fragment_enabled_on(self):
        slist = sorted(self.remote.list())
        enabled_on = ["disabled",self.remote.name] + slist
        if "edit_run_v7_enabled_on" in st.session_state:
            if st.session_state.edit_run_v7_enabled_on != self.config.pbgui.enabled_on:
                self.config.pbgui.enabled_on = st.session_state.edit_run_v7_enabled_on
        else:
            if self.config.pbgui.enabled_on in enabled_on:
                st.session_state.edit_run_v7_enabled_on = self.config.pbgui.enabled_on
            else:
                st.session_state.edit_run_v7_enabled_on = "disabled"
        st.selectbox('Enabled on', enabled_on, key="edit_run_v7_enabled_on")

    @st.fragment
    def fragment_leverage(self):
        if "edit_run_v7_leverage" in st.session_state:
            if st.session_state.edit_run_v7_leverage != self.config.live.leverage:
                self.config.live.leverage = st.session_state.edit_run_v7_leverage
        else:
            st.session_state.edit_run_v7_leverage = float(round(self.config.live.leverage, 0))
        st.number_input("leverage", min_value=0.0, max_value=100.0, step=1.0, format="%.1f", key="edit_run_v7_leverage", help=pbgui_help.leverage)

    @st.fragment
    def fragment_logging(self):
        if "edit_run_v7_logging_level" in st.session_state:
            if st.session_state.edit_run_v7_logging_level != self.config.logging.level:
                self.config.logging.level = st.session_state.edit_run_v7_logging_level
        else:
            st.session_state.edit_run_v7_logging_level = self.config.logging.level
        st.selectbox('logging level', Logging.LEVEL, format_func=lambda x: Logging.LEVEL.get(x), key="edit_run_v7_logging_level", help=pbgui_help.logging_level)

    @st.fragment
    def fragment_minimum_coin_age_days(self):
        if "edit_run_v7_minimum_coin_age_days" in st.session_state:
            if st.session_state.edit_run_v7_minimum_coin_age_days != self.config.live.minimum_coin_age_days:
                self.config.live.minimum_coin_age_days = st.session_state.edit_run_v7_minimum_coin_age_days
        else:
            st.session_state.edit_run_v7_minimum_coin_age_days = float(round(self.config.live.minimum_coin_age_days, 0))
        st.number_input("minimum_coin_age_days", min_value=0.0, step=1.0, format="%.1f", key="edit_run_v7_minimum_coin_age_days", help=pbgui_help.minimum_coin_age_days)

    @st.fragment
    def fragement_pnls_max_lookback_days(self):
        if "edit_run_v7_pnls_max_lookback_days" in st.session_state:
            if st.session_state.edit_run_v7_pnls_max_lookback_days != self.config.live.pnls_max_lookback_days:
                self.config.live.pnls_max_lookback_days = st.session_state.edit_run_v7_pnls_max_lookback_days
        else:
            st.session_state.edit_run_v7_pnls_max_lookback_days = float(round(self.config.live.pnls_max_lookback_days, 0))
        st.number_input("pnls_max_lookback_days", min_value=0.0, max_value=365.0, step=1.0, format="%.1f", key="edit_run_v7_pnls_max_lookback_days", help=pbgui_help.pnls_max_lookback_days)

    # warmup_ratio
    @st.fragment
    def fragment_warmup_ratio(self):
        if "edit_run_v7_warmup_ratio" in st.session_state:
            if st.session_state.edit_run_v7_warmup_ratio != self.config.live.warmup_ratio:
                self.config.live.warmup_ratio = st.session_state.edit_run_v7_warmup_ratio
        else:
            st.session_state.edit_run_v7_warmup_ratio = float(round(self.config.live.warmup_ratio, 2))
        st.number_input("warmup_ratio", min_value=0.0, step=0.1, max_value=1.0, format="%.2f", key="edit_run_v7_warmup_ratio", help=pbgui_help.warmup_ratio)

    @st.fragment
    def fragment_warmup_jitter_seconds(self):
        if "edit_run_v7_warmup_jitter_seconds" in st.session_state:
            if st.session_state.edit_run_v7_warmup_jitter_seconds != self.config.live.warmup_jitter_seconds:
                self.config.live.warmup_jitter_seconds = st.session_state.edit_run_v7_warmup_jitter_seconds
        else:
            st.session_state.edit_run_v7_warmup_jitter_seconds = float(round(self.config.live.warmup_jitter_seconds, 1))
        st.number_input(
            "warmup_jitter_seconds",
            min_value=0.0,
            step=1.0,
            format="%.1f",
            key="edit_run_v7_warmup_jitter_seconds",
            help=pbgui_help.warmup_jitter_seconds,
        )

    @st.fragment
    def fragment_warmup_concurrency(self):
        if "edit_run_v7_warmup_concurrency" in st.session_state:
            if int(st.session_state.edit_run_v7_warmup_concurrency) != int(self.config.live.warmup_concurrency):
                self.config.live.warmup_concurrency = int(st.session_state.edit_run_v7_warmup_concurrency)
        else:
            st.session_state.edit_run_v7_warmup_concurrency = int(self.config.live.warmup_concurrency)
        st.number_input(
            "warmup_concurrency",
            min_value=0,
            step=1,
            format="%d",
            key="edit_run_v7_warmup_concurrency",
            help=pbgui_help.warmup_concurrency,
        )

    @st.fragment
    def fragment_max_warmup_minutes(self):
        if "edit_run_v7_max_warmup_minutes" in st.session_state:
            if st.session_state.edit_run_v7_max_warmup_minutes != self.config.live.max_warmup_minutes:
                self.config.live.max_warmup_minutes = st.session_state.edit_run_v7_max_warmup_minutes
        else:
            st.session_state.edit_run_v7_max_warmup_minutes = self.config.live.max_warmup_minutes
        st.number_input("max_warmup_minutes", min_value=0, step=100, format="%.d", key="edit_run_v7_max_warmup_minutes", help=pbgui_help.max_warmup_minutes)

    @st.fragment
    def fragment_max_concurrent_api_requests(self):
        if "edit_run_v7_max_concurrent_api_requests" in st.session_state:
            if int(st.session_state.edit_run_v7_max_concurrent_api_requests) != self.config.live.max_concurrent_api_requests:
                self.config.live.max_concurrent_api_requests = int(st.session_state.edit_run_v7_max_concurrent_api_requests)
        else:
            st.session_state.edit_run_v7_max_concurrent_api_requests = int(self.config.live.max_concurrent_api_requests)
        st.number_input(
            "max_concurrent_api_requests",
            min_value=0,
            step=1,
            format="%d",
            key="edit_run_v7_max_concurrent_api_requests",
            help=pbgui_help.max_concurrent_api_requests,
        )

    @st.fragment
    def fragment_enable_archive_candle_fetch(self):
        if "edit_run_v7_enable_archive_candle_fetch" in st.session_state:
            if st.session_state.edit_run_v7_enable_archive_candle_fetch != self.config.live.enable_archive_candle_fetch:
                self.config.live.enable_archive_candle_fetch = st.session_state.edit_run_v7_enable_archive_candle_fetch
        else:
            st.session_state.edit_run_v7_enable_archive_candle_fetch = bool(self.config.live.enable_archive_candle_fetch)
        st.checkbox(
            "enable_archive_candle_fetch",
            key="edit_run_v7_enable_archive_candle_fetch",
            help=pbgui_help.enable_archive_candle_fetch,
        )

    @st.fragment
    def fragment_max_ohlcv_fetches_per_minute(self):
        if "edit_run_v7_max_ohlcv_fetches_per_minute" in st.session_state:
            if int(st.session_state.edit_run_v7_max_ohlcv_fetches_per_minute) != self.config.live.max_ohlcv_fetches_per_minute:
                self.config.live.max_ohlcv_fetches_per_minute = int(st.session_state.edit_run_v7_max_ohlcv_fetches_per_minute)
        else:
            st.session_state.edit_run_v7_max_ohlcv_fetches_per_minute = int(self.config.live.max_ohlcv_fetches_per_minute)
        st.number_input(
            "max_ohlcv_fetches_per_minute",
            min_value=1,
            step=1,
            format="%d",
            key="edit_run_v7_max_ohlcv_fetches_per_minute",
            help=pbgui_help.max_ohlcv_fetches_per_minute,
        )

    @st.fragment
    def fragment_memory_snapshot_interval_minutes(self):
        if "edit_run_v7_memory_snapshot_interval_minutes" in st.session_state:
            if st.session_state.edit_run_v7_memory_snapshot_interval_minutes != self.config.logging.memory_snapshot_interval_minutes:
                self.config.logging.memory_snapshot_interval_minutes = st.session_state.edit_run_v7_memory_snapshot_interval_minutes
        else:
            st.session_state.edit_run_v7_memory_snapshot_interval_minutes = self.config.logging.memory_snapshot_interval_minutes
        st.number_input("memory_snapshot_interval_minutes", min_value=1, step=5, format="%.d", key="edit_run_v7_memory_snapshot_interval_minutes", help=pbgui_help.memory_snapshot_interval_minutes)

    @st.fragment
    def fragment_volume_refresh_info_threshold_seconds(self):
        if "edit_run_v7_volume_refresh_info_threshold_seconds" in st.session_state:
            if st.session_state.edit_run_v7_volume_refresh_info_threshold_seconds != self.config.logging.volume_refresh_info_threshold_seconds:
                self.config.logging.volume_refresh_info_threshold_seconds = st.session_state.edit_run_v7_volume_refresh_info_threshold_seconds
        else:
            st.session_state.edit_run_v7_volume_refresh_info_threshold_seconds = self.config.logging.volume_refresh_info_threshold_seconds
        st.number_input("volume_refresh_info_threshold_seconds", min_value=0, step=5, format="%.d", key="edit_run_v7_volume_refresh_info_threshold_seconds", help=pbgui_help.volume_refresh_info_threshold_seconds)

    @st.fragment
    def fragment_note(self):
        if "edit_run_v7_note" in st.session_state:
            if st.session_state.edit_run_v7_note != self.config.pbgui.note:
                self.config.pbgui.note = st.session_state.edit_run_v7_note
        else:
            st.session_state.edit_run_v7_note = self.config.pbgui.note
        st.text_input("note", key="edit_run_v7_note", help=pbgui_help.instance_note)

    @st.fragment
    def fragment_price_distance_threshold(self):
        if "edit_run_v7_price_distance_threshold" in st.session_state:
            if st.session_state.edit_run_v7_price_distance_threshold != self.config.live.price_distance_threshold:
                self.config.live.price_distance_threshold = st.session_state.edit_run_v7_price_distance_threshold
        else:
            st.session_state.edit_run_v7_price_distance_threshold = self.config.live.price_distance_threshold
        st.number_input("price_distance_threshold", min_value=0.0, max_value=1.0, step=0.001, format="%.3f", key="edit_run_v7_price_distance_threshold", help=pbgui_help.price_distance_threshold)

    @st.fragment
    def fragment_execution_delay_seconds(self):
        if "edit_run_v7_execution_delay_seconds" in st.session_state:
            if st.session_state.edit_run_v7_execution_delay_seconds != self.config.live.execution_delay_seconds:
                self.config.live.execution_delay_seconds = st.session_state.edit_run_v7_execution_delay_seconds
        else:
            st.session_state.edit_run_v7_execution_delay_seconds = float(self.config.live.execution_delay_seconds)
        st.number_input("execution_delay_seconds", min_value=1.0, max_value=60.0, step=1.0, format="%.1f", key="edit_run_v7_execution_delay_seconds", help=pbgui_help.execution_delay_seconds)

    @st.fragment
    def fragment_filter_by_min_effective_cost(self):
        if "edit_run_v7_filter_by_min_effective_cost" in st.session_state:
            if st.session_state.edit_run_v7_filter_by_min_effective_cost != self.config.live.filter_by_min_effective_cost:
                self.config.live.filter_by_min_effective_cost = st.session_state.edit_run_v7_filter_by_min_effective_cost
        else:
            st.session_state.edit_run_v7_filter_by_min_effective_cost = self.config.live.filter_by_min_effective_cost
        st.checkbox("filter_by_min_effective_cost", help=pbgui_help.filter_by_min_effective_cost, key="edit_run_v7_filter_by_min_effective_cost")

    @st.fragment
    def fragment_market_orders_allowed(self):
        if "edit_run_v7_market_orders_allowed" in st.session_state:
            if st.session_state.edit_run_v7_market_orders_allowed != self.config.live.market_orders_allowed:
                self.config.live.market_orders_allowed = st.session_state.edit_run_v7_market_orders_allowed
        else:
            st.session_state.edit_run_v7_market_orders_allowed = self.config.live.market_orders_allowed
        st.checkbox("market_orders_allowed", help=pbgui_help.market_orders_allowed, key="edit_run_v7_market_orders_allowed")

    @st.fragment
    def fragment_auto_gs(self):
        if "edit_run_v7_auto_gs" in st.session_state:
            if st.session_state.edit_run_v7_auto_gs != self.config.live.auto_gs:
                self.config.live.auto_gs = st.session_state.edit_run_v7_auto_gs
        else:
            st.session_state.edit_run_v7_auto_gs = self.config.live.auto_gs
        st.checkbox("auto_gs", help=pbgui_help.auto_gs, key="edit_run_v7_auto_gs")

    @st.fragment
    def fragment_hedge_mode(self):
        if "edit_run_v7_hedge_mode" in st.session_state:
            if st.session_state.edit_run_v7_hedge_mode != self.config.live.hedge_mode:
                self.config.live.hedge_mode = st.session_state.edit_run_v7_hedge_mode
        else:
            st.session_state.edit_run_v7_hedge_mode = self.config.live.hedge_mode
        st.checkbox("hedge_mode", help=pbgui_help.hedge_mode, key="edit_run_v7_hedge_mode")

    @st.fragment
    def inactive_coin_candle_ttl_minutes(self):
        if "edit_run_v7_inactive_coin_candle_ttl_minutes" in st.session_state:
            if st.session_state.edit_run_v7_inactive_coin_candle_ttl_minutes != self.config.live.inactive_coin_candle_ttl_minutes:
                self.config.live.inactive_coin_candle_ttl_minutes = st.session_state.edit_run_v7_inactive_coin_candle_ttl_minutes
        else:
            st.session_state.edit_run_v7_inactive_coin_candle_ttl_minutes = float(round(self.config.live.inactive_coin_candle_ttl_minutes, 0))
        st.number_input("inactive_coin_candle_ttl_minutes", min_value=0.0, step=1.0, format="%.1f", key="edit_run_v7_inactive_coin_candle_ttl_minutes", help=pbgui_help.inactive_coin_candle_ttl_minutes)

    @st.fragment
    def fragment_max_n_cancellations_per_batch(self):
        if "edit_run_v7_max_n_cancellations_per_batch" in st.session_state:
            if st.session_state.edit_run_v7_max_n_cancellations_per_batch != self.config.live.max_n_cancellations_per_batch:
                self.config.live.max_n_cancellations_per_batch = st.session_state.edit_run_v7_max_n_cancellations_per_batch
        else:
            st.session_state.edit_run_v7_max_n_cancellations_per_batch = self.config.live.max_n_cancellations_per_batch
        st.number_input("max_n_cancellations_per_batch", min_value=0, max_value=100, step=1, format="%.d", key="edit_run_v7_max_n_cancellations_per_batch", help=pbgui_help.max_n_per_batch)

    @st.fragment
    def fragment_max_n_creations_per_batch(self):
        if "edit_run_v7_max_n_creations_per_batch" in st.session_state:
            if st.session_state.edit_run_v7_max_n_creations_per_batch != self.config.live.max_n_creations_per_batch:
                self.config.live.max_n_creations_per_batch = st.session_state.edit_run_v7_max_n_creations_per_batch
        else:
            st.session_state.edit_run_v7_max_n_creations_per_batch = self.config.live.max_n_creations_per_batch
        st.number_input("max_n_creations_per_batch", min_value=0, max_value=100, step=1, format="%.d", key="edit_run_v7_max_n_creations_per_batch", help=pbgui_help.max_n_per_batch)

    @st.fragment
    def fragment_forced_mode_long(self):
        if "edit_run_v7_forced_mode_long" in st.session_state:
            if st.session_state.edit_run_v7_forced_mode_long != self.config.live.forced_mode_long:
                self.config.live.forced_mode_long = st.session_state.edit_run_v7_forced_mode_long
        else:
            st.session_state.edit_run_v7_forced_mode_long = self.config.live.forced_mode_long
        st.selectbox('forced_mode_long',self.MODE, format_func=lambda x: self.MODE_OPTIONS.get(x), key="edit_run_v7_forced_mode_long", help=pbgui_help.forced_mode_long_short)

    @st.fragment
    def fragment_forced_mode_short(self):
        if "edit_run_v7_forced_mode_short" in st.session_state:
            if st.session_state.edit_run_v7_forced_mode_short != self.config.live.forced_mode_short:
                self.config.live.forced_mode_short = st.session_state.edit_run_v7_forced_mode_short
        else:
            st.session_state.edit_run_v7_forced_mode_short = self.config.live.forced_mode_short
        st.selectbox('forced_mode_short',self.MODE, format_func=lambda x: self.MODE_OPTIONS.get(x), key="edit_run_v7_forced_mode_short", help=pbgui_help.forced_mode_long_short)

    @st.fragment
    def fragment_max_n_restarts_per_day(self):
        if "edit_run_v7_max_n_restarts_per_day" in st.session_state:
            if st.session_state.edit_run_v7_max_n_restarts_per_day != self.config.live.max_n_restarts_per_day:
                self.config.live.max_n_restarts_per_day = st.session_state.edit_run_v7_max_n_restarts_per_day
        else:
            st.session_state.edit_run_v7_max_n_restarts_per_day = self.config.live.max_n_restarts_per_day
        st.number_input("max_n_restarts_per_day", min_value=0, max_value=100, step=1, format="%.d", key="edit_run_v7_max_n_restarts_per_day", help=pbgui_help.max_n_restarts_per_day)

    @st.fragment
    def fragement_max_disk_candles_per_symbol_per_tf(self):
        if "edit_run_v7_max_disk_candles_per_symbol_per_tf" in st.session_state:
            if st.session_state.edit_run_v7_max_disk_candles_per_symbol_per_tf != self.config.live.max_disk_candles_per_symbol_per_tf:
                self.config.live.max_disk_candles_per_symbol_per_tf = st.session_state.edit_run_v7_max_disk_candles_per_symbol_per_tf
        else:
            st.session_state.edit_run_v7_max_disk_candles_per_symbol_per_tf = self.config.live.max_disk_candles_per_symbol_per_tf
        st.number_input("max_disk_candles_per_symbol_per_tf", min_value=0, max_value=10000000, step=10000, format="%.d", key="edit_run_v7_max_disk_candles_per_symbol_per_tf", help=pbgui_help.max_disk_candles_per_symbol_per_tf)
    @st.fragment
    def fragment_max_memory_candles_per_symbol(self):
        if "edit_run_v7_max_memory_candles_per_symbol" in st.session_state:
            if st.session_state.edit_run_v7_max_memory_candles_per_symbol != self.config.live.max_memory_candles_per_symbol:
                self.config.live.max_memory_candles_per_symbol = st.session_state.edit_run_v7_max_memory_candles_per_symbol
        else:
            st.session_state.edit_run_v7_max_memory_candles_per_symbol = self.config.live.max_memory_candles_per_symbol
        st.number_input("max_memory_candles_per_symbol", min_value=0, max_value=10000000, step=10000, format="%.d", key="edit_run_v7_max_memory_candles_per_symbol", help=pbgui_help.max_memory_candles_per_symbol)

    @st.fragment
    def fragment_time_in_force(self):
        if "edit_run_v7_time_in_force" in st.session_state:
            if st.session_state.edit_run_v7_time_in_force != self.config.live.time_in_force:
                self.config.live.time_in_force = st.session_state.edit_run_v7_time_in_force
        else:
            if self.config.live.time_in_force in self.TIME_IN_FORCE:
                st.session_state.edit_run_v7_time_in_force = self.config.live.time_in_force
            else:
                st.session_state.edit_run_v7_time_in_force = self.TIME_IN_FORCE[0]
        st.selectbox('time_in_force', self.TIME_IN_FORCE, key="edit_run_v7_time_in_force", help=pbgui_help.time_in_force)

    @st.fragment
    def fragment_recv_window_ms(self):
        if "edit_run_v7_recv_window_ms" in st.session_state:
            if st.session_state.edit_run_v7_recv_window_ms != self.config.live.recv_window_ms:
                self.config.live.recv_window_ms = st.session_state.edit_run_v7_recv_window_ms
        else:
            st.session_state.edit_run_v7_recv_window_ms = self.config.live.recv_window_ms
        st.number_input("recv_window_ms", min_value=1000, max_value=60000, step=1000, format="%.d", key="edit_run_v7_recv_window_ms", help=pbgui_help.recv_window_ms)

    @st.fragment
    def fragment_order_match_tolerance_pct(self):
        if "edit_run_v7_order_match_tolerance_pct" in st.session_state:
            if st.session_state.edit_run_v7_order_match_tolerance_pct != self.config.live.order_match_tolerance_pct:
                self.config.live.order_match_tolerance_pct = st.session_state.edit_run_v7_order_match_tolerance_pct
        else:
            st.session_state.edit_run_v7_order_match_tolerance_pct = self.config.live.order_match_tolerance_pct
        st.number_input("order_match_tolerance_pct", min_value=0.0, max_value=0.01, step=0.0001, format="%.4f", key="edit_run_v7_order_match_tolerance_pct", help=pbgui_help.order_match_tolerance_pct)

    @st.fragment
    def fragment_balance_override(self):
        if "edit_run_v7_balance_override" in st.session_state:
            value = st.session_state.edit_run_v7_balance_override
            if value == 0.0:
                self.config.live.balance_override = None
            else:
                self.config.live.balance_override = value
        else:
            st.session_state.edit_run_v7_balance_override = self.config.live.balance_override if self.config.live.balance_override is not None else 0.0
        st.number_input("balance_override", min_value=0.0, step=100.0, format="%.2f", key="edit_run_v7_balance_override", help=pbgui_help.balance_override)

    @st.fragment
    def fragment_balance_hysteresis_snap_pct(self):
        if "edit_run_v7_balance_hysteresis_snap_pct" in st.session_state:
            if st.session_state.edit_run_v7_balance_hysteresis_snap_pct != self.config.live.balance_hysteresis_snap_pct:
                self.config.live.balance_hysteresis_snap_pct = st.session_state.edit_run_v7_balance_hysteresis_snap_pct
        else:
            st.session_state.edit_run_v7_balance_hysteresis_snap_pct = self.config.live.balance_hysteresis_snap_pct
        st.number_input("balance_hysteresis_snap_pct", min_value=0.0, max_value=0.5, step=0.01, format="%.2f", key="edit_run_v7_balance_hysteresis_snap_pct", help=pbgui_help.balance_hysteresis_snap_pct)

    def fragment_filter_coins(self):
        if "pbcoindata" not in st.session_state:
            st.session_state.pbcoindata = CoinData()
        coindata = st.session_state.pbcoindata
        exchange_id = self._users.find_exchange(self.user)

        col1, col2, col3, col4, col5 = st.columns([1,1,1,0.5,0.5], vertical_alignment="bottom")
        with col1:
            self.fragment_market_cap()
        with col2:
            self.fragment_vol_mcap()
        with col3:
            self.fragment_tags()
        with col4:
            self.fragment_only_cpt()
            self.fragment_notices_ignore()
        with col5:
            st.checkbox("apply_filters", value=False, help=pbgui_help.apply_filters, key="edit_run_v7_apply_filters")
        def _normalize_list(items):
            normalized = []
            for item in items:
                base = normalize_symbol(item)
                if base and base not in normalized:
                    normalized.append(base)
            return normalized
        # Init session state for approved_coins
        if "edit_run_v7_approved_coins_long" in st.session_state:
            if st.session_state.edit_run_v7_approved_coins_long != self.config.live.approved_coins.long:
                self.config.live.approved_coins.long = st.session_state.edit_run_v7_approved_coins_long
        else:
            self.config.live.approved_coins.long = _normalize_list(self.config.live.approved_coins.long)
            st.session_state.edit_run_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_run_v7_approved_coins_short" in st.session_state:
            if st.session_state.edit_run_v7_approved_coins_short != self.config.live.approved_coins.short:
                self.config.live.approved_coins.short = st.session_state.edit_run_v7_approved_coins_short
        else:
            self.config.live.approved_coins.short = _normalize_list(self.config.live.approved_coins.short)
            st.session_state.edit_run_v7_approved_coins_short = self.config.live.approved_coins.short
        # Init session state for ignored_coins
        if "edit_run_v7_ignored_coins_long" in st.session_state:
            if st.session_state.edit_run_v7_ignored_coins_long != self.config.live.ignored_coins.long:
                self.config.live.ignored_coins.long = st.session_state.edit_run_v7_ignored_coins_long
        else:
            self.config.live.ignored_coins.long = _normalize_list(self.config.live.ignored_coins.long)
            st.session_state.edit_run_v7_ignored_coins_long = self.config.live.ignored_coins.long
        if "edit_run_v7_ignored_coins_short" in st.session_state:
            if st.session_state.edit_run_v7_ignored_coins_short != self.config.live.ignored_coins.short:
                self.config.live.ignored_coins.short = st.session_state.edit_run_v7_ignored_coins_short
        else:
            self.config.live.ignored_coins.short = _normalize_list(self.config.live.ignored_coins.short)
            st.session_state.edit_run_v7_ignored_coins_short = self.config.live.ignored_coins.short
        # Appliy filters
        if st.session_state.edit_run_v7_apply_filters:
            approved, ignored = coindata.filter_mapping(
                exchange=exchange_id,
                market_cap_min_m=self.config.pbgui.market_cap,
                vol_mcap_max=self.config.pbgui.vol_mcap,
                only_cpt=self.config.pbgui.only_cpt,
                notices_ignore=self.config.pbgui.notices_ignore,
                tags=self.config.pbgui.tags,
                quote_filter=None,
                use_cache=True,
            )
            self.config.live.approved_coins.long = approved
            self.config.live.approved_coins.short = approved
            self.config.live.ignored_coins.long = ignored
            self.config.live.ignored_coins.short = ignored
        # Remove unavailable coins
        mapping = coindata.load_mapping(exchange=exchange_id, use_cache=True)
        symbols = sorted({(record.get("coin") or "").upper() for record in mapping if record.get("coin")})
        notices_by_coin = {}
        for record in mapping:
            coin = (record.get("coin") or "").upper()
            notice = str(record.get("notice") or "").strip()
            if coin and notice and coin not in notices_by_coin:
                notices_by_coin[coin] = notice
        symbol_set = set(symbols)

        def is_available(coin: str) -> bool:
            if not coin:
                return False
            base = normalize_symbol(coin)
            return coin in symbol_set or base in symbol_set

        for symbol in self.config.live.approved_coins.long.copy():
            if not is_available(symbol):
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.approved_coins.short.copy():
            if not is_available(symbol):
                self.config.live.approved_coins.short.remove(symbol)
        for symbol in self.config.live.ignored_coins.long.copy():
            if not is_available(symbol):
                self.config.live.ignored_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short.copy():
            if not is_available(symbol):
                self.config.live.ignored_coins.short.remove(symbol)
        # Remove from approved_coins when in ignored coins
        for symbol in self.config.live.ignored_coins.long:
            if symbol in self.config.live.approved_coins.long:
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short:
            if symbol in self.config.live.approved_coins.short:
                self.config.live.approved_coins.short.remove(symbol)
        # Correct Display of Symbols
        if "edit_run_v7_approved_coins_long" in st.session_state:
            st.session_state.edit_run_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_run_v7_approved_coins_short" in st.session_state:
            st.session_state.edit_run_v7_approved_coins_short = self.config.live.approved_coins.short
        if "edit_run_v7_ignored_coins_long" in st.session_state:
            st.session_state.edit_run_v7_ignored_coins_long = self.config.live.ignored_coins.long
        if "edit_run_v7_ignored_coins_short" in st.session_state:
            st.session_state.edit_run_v7_ignored_coins_short = self.config.live.ignored_coins.short
        # Find coins with notices
        for coin in list(set(self.config.live.approved_coins.long + self.config.live.approved_coins.short)):
            base = normalize_symbol(coin)
            notice = notices_by_coin.get(coin) or notices_by_coin.get(base)
            if notice:
                st.warning(f'{base}: {notice}')
        # Select approved and ignored coins
        col1, col2 = st.columns([1,1], vertical_alignment="bottom")
        with col1:
            st.multiselect('approved_coins_long', symbols, key="edit_run_v7_approved_coins_long", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_long', symbols, key="edit_run_v7_ignored_coins_long", help=pbgui_help.ignored_coins)
        with col2:
            st.multiselect('approved_coins_short', symbols, key="edit_run_v7_approved_coins_short", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_short', symbols, key="edit_run_v7_ignored_coins_short", help=pbgui_help.ignored_coins)

    def fragment_market_cap(self):
        if "edit_run_v7_market_cap" in st.session_state:
            if st.session_state.edit_run_v7_market_cap != self.config.pbgui.market_cap:
                self.config.pbgui.market_cap = st.session_state.edit_run_v7_market_cap
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_market_cap = round(self.config.pbgui.market_cap, 2)
        st.number_input("market_cap", min_value=0, step=50, format="%.d", key="edit_run_v7_market_cap", help=pbgui_help.market_cap)

    def fragment_vol_mcap(self):
        if "edit_run_v7_vol_mcap" in st.session_state:
            if st.session_state.edit_run_v7_vol_mcap != self.config.pbgui.vol_mcap:
                self.config.pbgui.vol_mcap = st.session_state.edit_run_v7_vol_mcap
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_vol_mcap = round(self.config.pbgui.vol_mcap, 2)
        st.number_input("vol/mcap", min_value=0.0, step=0.05, format="%.2f", key="edit_run_v7_vol_mcap", help=pbgui_help.vol_mcap)

    def fragment_tags(self):
        if "pbcoindata" not in st.session_state:
            st.session_state.pbcoindata = CoinData()
        coindata = st.session_state.pbcoindata
        exchange_id = self._users.find_exchange(self.user)
        available_tags = coindata.get_mapping_tags(exchange=exchange_id, use_cache=True)

        if "edit_run_v7_tags" in st.session_state:
            if st.session_state.edit_run_v7_tags != self.config.pbgui.tags:
                self.config.pbgui.tags = st.session_state.edit_run_v7_tags
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            # Remove tags that are no longer available
            self.config.pbgui.tags = [tag for tag in self.config.pbgui.tags if tag in available_tags]
            st.session_state.edit_run_v7_tags = self.config.pbgui.tags
        st.multiselect("Tags", available_tags, key="edit_run_v7_tags", help=pbgui_help.coindata_tags)

    def fragment_only_cpt(self):
        if "edit_run_v7_only_cpt" in st.session_state:
            if st.session_state.edit_run_v7_only_cpt != self.config.pbgui.only_cpt:
                self.config.pbgui.only_cpt = st.session_state.edit_run_v7_only_cpt
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_only_cpt = self.config.pbgui.only_cpt
        st.checkbox("only_cpt", help=pbgui_help.only_cpt, key="edit_run_v7_only_cpt")
    
    def fragment_notices_ignore(self):
        if "edit_run_v7_notices_ignore" in st.session_state:
            if st.session_state.edit_run_v7_notices_ignore != self.config.pbgui.notices_ignore:
                self.config.pbgui.notices_ignore = st.session_state.edit_run_v7_notices_ignore
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_notices_ignore = self.config.pbgui.notices_ignore
        st.checkbox("notices ignore", help=pbgui_help.notices_ignore, key="edit_run_v7_notices_ignore")

    @st.fragment
    def fragment_empty_means_all_approved(self):
        if "edit_run_v7_empty_means_all_approved" in st.session_state:
            if st.session_state.edit_run_v7_empty_means_all_approved != self.config.live.empty_means_all_approved:
                self.config.live.empty_means_all_approved = st.session_state.edit_run_v7_empty_means_all_approved
        else:
            st.session_state.edit_run_v7_empty_means_all_approved = self.config.live.empty_means_all_approved
        st.checkbox("empty_means_all_approved", help=pbgui_help.empty_means_all_approved, key="edit_run_v7_empty_means_all_approved")

    def edit(self):
        # Change ignored_coins back to empty list if we changed it to a path
        if type(self.config.live.ignored_coins.long) == str:
            self.config.live.ignored_coins.long = []
            self.config.live.ignored_coins.short = []
        # Change approved_coins back to empty list if we changed it to a path
        if type(self.config.live.approved_coins.long) == str:
            self.config.live.approved_coins.long = []
            self.config.live.approved_coins.short = []
        # Display Editor
        col1, col2, col3, col4, col5 = st.columns([1,1,0.5,0.5,1])
        with col1:
            # Select User
            if "edit_run_v7_user" in st.session_state:
                if st.session_state.edit_run_v7_user != self.user:
                    self.user = st.session_state.edit_run_v7_user
                    st.session_state.pbcoindata.exchange = self._users.find_exchange(self.user)
            else:
                if self.user in self._users.list_v7():
                    st.session_state.edit_run_v7_user = self.user
                else:
                    st.session_state.edit_run_v7_user = self._users.list_v7()[0]
                st.session_state.pbcoindata.exchange = self._users.find_exchange(self.user)
            st.selectbox('User',self._users.list_v7(), key="edit_run_v7_user")
        with col2:
            self.fragment_enabled_on()
        with col3:
            # Config Version
            if "edit_run_v7_version" in st.session_state:
                if st.session_state.edit_run_v7_version != self.config.pbgui.version:
                    self.config.pbgui.version = st.session_state.edit_run_v7_version
            else:
                st.session_state.edit_run_v7_version = self.config.pbgui.version
            st.number_input("config version", min_value=self.config.pbgui.version, step=1, format="%.d", key="edit_run_v7_version", help=pbgui_help.config_version)
        with col4:
            self.fragment_leverage()
        with col5:
            self.fragment_logging()
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_minimum_coin_age_days()
        with col2:
            self.fragement_pnls_max_lookback_days()
        with col3:
            self.fragment_warmup_ratio()
        with col4:
            self.fragment_note()
        col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
        with col1:
            self.fragment_price_distance_threshold()
        with col2:
            self.fragment_execution_delay_seconds()
        with col3:
            self.fragment_filter_by_min_effective_cost()
            self.fragment_market_orders_allowed()
        with col4:
            self.fragment_hedge_mode()
            self.fragment_auto_gs()

        # Advanced Settings
        with st.expander("Advanced Settings", expanded=False):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                self.fragment_max_n_cancellations_per_batch()
            with col2:
                self.fragment_max_n_creations_per_batch()
            with col3:
                self.fragment_forced_mode_long()
            with col4:  
                self.fragment_forced_mode_short()
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                self.fragment_max_n_restarts_per_day()
            with col2:
                self.fragement_max_disk_candles_per_symbol_per_tf()
            with col3:
                self.fragment_max_memory_candles_per_symbol()
            with col4:
                self.fragment_time_in_force()
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                self.inactive_coin_candle_ttl_minutes()
            with col2:
                self.fragment_recv_window_ms()
            with col3:
                self.fragment_order_match_tolerance_pct()
            with col4:
                self.fragment_balance_hysteresis_snap_pct()
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                self.fragment_balance_override()
            with col2:
                self.fragment_max_warmup_minutes()
            with col3:
                self.fragment_memory_snapshot_interval_minutes()
            with col4:
                self.fragment_volume_refresh_info_threshold_seconds()

            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                self.fragment_warmup_jitter_seconds()
            with col2:
                self.fragment_warmup_concurrency()
            with col3:
                self.fragment_enable_archive_candle_fetch()
            with col4:
                self.fragment_max_ohlcv_fetches_per_minute()

            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                self.fragment_max_concurrent_api_requests()

        #Filters
        self.fragment_filter_coins()

        # Dynamic Ignore
        if "edit_run_v7_dynamic_ignore" in st.session_state:
            if st.session_state.edit_run_v7_dynamic_ignore != self.config.pbgui.dynamic_ignore:
                self.config.pbgui.dynamic_ignore = st.session_state.edit_run_v7_dynamic_ignore
        else:
            st.session_state.edit_run_v7_dynamic_ignore = self.config.pbgui.dynamic_ignore
        col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
        with col1:
            st.checkbox("dynamic_ignore", help=pbgui_help.dynamic_ignore, key="edit_run_v7_dynamic_ignore")
        with col2:
            self.fragment_empty_means_all_approved()
       
        # Display dynamic_ignore
        if self.config.pbgui.dynamic_ignore:
            if "pbcoindata" not in st.session_state:
                st.session_state.pbcoindata = CoinData()
            coindata = st.session_state.pbcoindata
            exchange_id = self._users.find_exchange(self.user)
            approved, ignored = coindata.filter_mapping(
                exchange=exchange_id,
                market_cap_min_m=self.config.pbgui.market_cap,
                vol_mcap_max=self.config.pbgui.vol_mcap,
                only_cpt=self.config.pbgui.only_cpt,
                notices_ignore=self.config.pbgui.notices_ignore,
                tags=self.config.pbgui.tags,
                quote_filter=None,
                use_cache=True,
            )
            notices_by_coin = {}
            for record in coindata.load_mapping(exchange=exchange_id, use_cache=True):
                coin = (record.get("coin") or "").upper()
                notice = str(record.get("notice") or "").strip()
                if coin and notice and coin not in notices_by_coin:
                    notices_by_coin[coin] = notice
            for coin in approved:
                if coin in notices_by_coin:
                    st.warning(f'{coin}: {notices_by_coin[coin]}')
            st.code(f'approved_symbols: {approved}', wrap_lines=True)
            st.code(f'dynamic_ignored symbols: {ignored}', wrap_lines=True)

        # Edit coin_overrides
        self.config.view_coin_overrides()

        # Edit long / short
        self.config.bot.edit()
        # View log
        self.view_log()

    @st.dialog("Paste config", width="large")
    def import_instance(self):
        users = self._users.list_v7()

        reset_flag_key = "import_run_v7_config_reset"

        # Keep the target user stable while the dialog is open.
        if "import_run_v7_user" not in st.session_state:
            st.session_state.import_run_v7_user = self.user

        selected_user = st.selectbox(
            "User",
            users,
            index=users.index(st.session_state.import_run_v7_user) if st.session_state.import_run_v7_user in users else 0,
            key="import_run_v7_user",
            disabled=not bool(users),
        )

        # If the user selection changed, retarget the instance and reset the textarea.
        if selected_user and selected_user != self.user:
            self.user = selected_user
            st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)

        # If a previous action requested a reset, seed the textarea BEFORE creating the widget.
        if st.session_state.get(reset_flag_key, False):
            st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)
            st.session_state.pop(reset_flag_key, None)

        # Keep the raw text stable while the user is editing/pasting.
        if "import_run_v7_config" not in st.session_state:
            st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)

        st.text_area(
            "config",
            key="import_run_v7_config",
            height=500,
            help="Paste full JSON config here",
        )

        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("OK"):
                try:
                    raw_txt = str(st.session_state.get("import_run_v7_config") or "")
                    parsed = json.loads(raw_txt)
                except Exception:
                    st.error('Invalid JSON (use true/false/null, not True/False/None)', icon="⚠️")
                    return

                # Keep instance identity stable: importing a config should not switch the instance user.
                if isinstance(parsed, dict):
                    parsed.setdefault("live", {})
                    if isinstance(parsed.get("live"), dict):
                        parsed["live"]["user"] = self.user

                # Apply config via setter
                self.config.config = parsed

                # Reset widget state so UI reflects the imported config values.
                self._clear_config_widget_session_state_after_import()

                # Request textarea reset on next run (can't mutate widget key after instantiation).
                st.session_state[reset_flag_key] = True
                st.rerun()

        with col2:
            if st.button("Cancel"):
                # Request textarea reset on next run (can't mutate widget key after instantiation).
                st.session_state[reset_flag_key] = True
                st.rerun()

    def _clear_config_widget_session_state_after_import(self) -> None:
        """Clear Streamlit widget state so imported configs are reflected in UI.

        Without this, existing session_state keys (especially edit_configv7_* bot
        parameters) can override freshly imported JSON and get written back to config.
        """

        for k in list(st.session_state.keys()):
            if k.startswith("edit_run_v7_") or k.startswith("edit_configv7_"):
                st.session_state.pop(k, None)

    def activate(self):
        self.remote.local_run.activate(self.user, False, "7")

class V7Instances:
    def __init__(self):
        self.instances = []
        self.index = 0
        self.instances_path = f'{PBGDIR}/data/run_v7'
        self.load()

    def __iter__(self):
        return iter(self.instances)

    def __next__(self):
        if self.index > len(self.instances):
            raise StopIteration
        self.index += 1
        return next(self)
    
    def list(self):
        return list(map(lambda c: c.user, self.instances))
    
    def remove(self, instance: V7Instance):
        instance.remove()
        self.instances.remove(instance)
    
    def activate_all(self):
        for instance in self.instances:
            running_on = instance.is_running_on()
            if instance.enabled_on == 'disabled' and running_on:
                instance.remote.local_run.activate(instance.user, True, "7")
            elif instance.enabled_on not in running_on:
                instance.remote.local_run.activate(instance.user, True, "7")
            elif instance.is_running() and (instance.version != instance.running_version):
                instance.remote.local_run.activate(instance.user, True, "7")

    def restart_instance(self, user: str):
        for instance in self.instances:
            if user == instance.user:
                # Restart
                instance.save()
                instance.remote.local_run.activate(user, True, "7")
    
    def fetch_instance_version(self, user: str):
        for instance in self.instances:
            if user == instance.user:
                return instance.running_version
        return 0

    def load(self):
        p = str(Path(f'{self.instances_path}/*'))
        instances = glob.glob(p)
        for instance in instances:
            inst = V7Instance()
            inst.load(instance)
            self.instances.append(inst)
        self.instances = sorted(self.instances, key=lambda d: d.user) 

    def is_user_used(self, user: str):
        for instance in self.instances:
           if user == instance.user:
               return True
        return False


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
