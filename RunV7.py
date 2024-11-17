import streamlit as st
import streamlit_scrollable_textbox as stx
import pbgui_help
from pbgui_func import pbdir, PBGDIR, load_symbols_from_ini, validateHJSON, st_file_selector, info_popup, error_popup
from PBRemote import PBRemote
from User import Users
from Config import Config, ConfigV7
from Exchange import Exchange
from pathlib import Path
import glob
import json
from shutil import rmtree

class V7Instance():
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

    def edit(self):
        # Change ignored_coins back to empty list if we changed it to a path
        if type(self.config.live.ignored_coins.long) == str:
            self.config.live.ignored_coins.long = []
            self.config.live.ignored_coins.short = []
        # Init coindata
        coindata = st.session_state.pbcoindata
        if coindata.exchange != self._users.find_exchange(self.user):
            coindata.exchange = self._users.find_exchange(self.user)
        if coindata.market_cap != self.config.pbgui.market_cap:
            coindata.market_cap = self.config.pbgui.market_cap
        if coindata.vol_mcap != self.config.pbgui.vol_mcap:
            coindata.vol_mcap = self.config.pbgui.vol_mcap
        # Init session_state for keys
        if "edit_run_v7_user" in st.session_state:
            if st.session_state.edit_run_v7_user != self.user:
                self.user = st.session_state.edit_run_v7_user
                coindata.exchange = self._users.find_exchange(self.user)
        if "edit_run_v7_enabled_on" in st.session_state:
            if st.session_state.edit_run_v7_enabled_on != self.config.pbgui.enabled_on:
                self.config.pbgui.enabled_on = st.session_state.edit_run_v7_enabled_on
        if "edit_run_v7_version" in st.session_state:
            if st.session_state.edit_run_v7_version != self.config.pbgui.version:
                self.config.pbgui.version = st.session_state.edit_run_v7_version
        if "edit_run_v7_note" in st.session_state:
            if st.session_state.edit_run_v7_note != self.config.pbgui.note:
                self.config.pbgui.note = st.session_state.edit_run_v7_note
        if "edit_run_v7_leverage" in st.session_state:
            if st.session_state.edit_run_v7_leverage != self.config.live.leverage:
                self.config.live.leverage = st.session_state.edit_run_v7_leverage
        if "edit_run_v7_pnls_max_lookback_days" in st.session_state:
            if st.session_state.edit_run_v7_pnls_max_lookback_days != self.config.live.pnls_max_lookback_days:
                self.config.live.pnls_max_lookback_days = st.session_state.edit_run_v7_pnls_max_lookback_days
        if "edit_run_v7_minimum_coin_age_days" in st.session_state:
            if st.session_state.edit_run_v7_minimum_coin_age_days != self.config.live.minimum_coin_age_days:
                self.config.live.minimum_coin_age_days = st.session_state.edit_run_v7_minimum_coin_age_days
        # if "edit_run_v7_relative_volume_filter_clip_pct" in st.session_state:
        #     if st.session_state.edit_run_v7_relative_volume_filter_clip_pct != self.config.live.relative_volume_filter_clip_pct:
        #         self.config.live.relative_volume_filter_clip_pct = st.session_state.edit_run_v7_relative_volume_filter_clip_pct
        if "edit_run_v7_price_distance_threshold" in st.session_state:
            if st.session_state.edit_run_v7_price_distance_threshold != self.config.live.price_distance_threshold:
                self.config.live.price_distance_threshold = st.session_state.edit_run_v7_price_distance_threshold
        if "edit_run_v7_execution_delay_seconds" in st.session_state:
            if st.session_state.edit_run_v7_execution_delay_seconds != self.config.live.execution_delay_seconds:
                self.config.live.execution_delay_seconds = st.session_state.edit_run_v7_execution_delay_seconds
        if "edit_run_v7_filter_by_min_effective_cost" in st.session_state:
            if st.session_state.edit_run_v7_filter_by_min_effective_cost != self.config.live.filter_by_min_effective_cost:
                self.config.live.filter_by_min_effective_cost = st.session_state.edit_run_v7_filter_by_min_effective_cost
        if "edit_run_v7_auto_gs" in st.session_state:
            if st.session_state.edit_run_v7_auto_gs != self.config.live.auto_gs:
                self.config.live.auto_gs = st.session_state.edit_run_v7_auto_gs
        # Advanced Options
        if "edit_run_v7_max_n_cancellations_per_batch" in st.session_state:
            if st.session_state.edit_run_v7_max_n_cancellations_per_batch != self.config.live.max_n_cancellations_per_batch:
                self.config.live.max_n_cancellations_per_batch = st.session_state.edit_run_v7_max_n_cancellations_per_batch
        if "edit_run_v7_max_n_creations_per_batch" in st.session_state:
            if st.session_state.edit_run_v7_max_n_creations_per_batch != self.config.live.max_n_creations_per_batch:
                self.config.live.max_n_creations_per_batch = st.session_state.edit_run_v7_max_n_creations_per_batch
        if "edit_run_v7_forced_mode_long" in st.session_state:
            if st.session_state.edit_run_v7_forced_mode_long != self.config.live.forced_mode_long:
                self.config.live.forced_mode_long = st.session_state.edit_run_v7_forced_mode_long
        if "edit_run_v7_forced_mode_short" in st.session_state:
            if st.session_state.edit_run_v7_forced_mode_short != self.config.live.forced_mode_short:
                self.config.live.forced_mode_short = st.session_state.edit_run_v7_forced_mode_short
        if "edit_run_v7_max_n_restarts_per_day" in st.session_state:
            if st.session_state.edit_run_v7_max_n_restarts_per_day != self.config.live.max_n_restarts_per_day:
                self.config.live.max_n_restarts_per_day = st.session_state.edit_run_v7_max_n_restarts_per_day
        if "edit_run_v7_ohlcvs_1m_rolling_window_days" in st.session_state:
            if st.session_state.edit_run_v7_ohlcvs_1m_rolling_window_days != self.config.live.ohlcvs_1m_rolling_window_days:
                self.config.live.ohlcvs_1m_rolling_window_days = st.session_state.edit_run_v7_ohlcvs_1m_rolling_window_days
        if "edit_run_v7_ohlcvs_1m_update_after_minutes" in st.session_state:
            if st.session_state.edit_run_v7_ohlcvs_1m_update_after_minutes != self.config.live.ohlcvs_1m_update_after_minutes:
                self.config.live.ohlcvs_1m_update_after_minutes = st.session_state.edit_run_v7_ohlcvs_1m_update_after_minutes
        if "edit_run_v7_time_in_force" in st.session_state:
            if st.session_state.edit_run_v7_time_in_force != self.config.live.time_in_force:
                self.config.live.time_in_force = st.session_state.edit_run_v7_time_in_force
        # Filters
        if "edit_run_v7_dynamic_ignore" in st.session_state:
            if st.session_state.edit_run_v7_dynamic_ignore != self.config.pbgui.dynamic_ignore:
                self.config.pbgui.dynamic_ignore = st.session_state.edit_run_v7_dynamic_ignore
        if "edit_run_v7_empty_means_all_approved" in st.session_state:
            if st.session_state.edit_run_v7_empty_means_all_approved != self.config.live.empty_means_all_approved:
                self.config.live.empty_means_all_approved = st.session_state.edit_run_v7_empty_means_all_approved
        if "edit_run_v7_market_cap" in st.session_state:
            if st.session_state.edit_run_v7_market_cap != self.config.pbgui.market_cap:
                self.config.pbgui.market_cap = st.session_state.edit_run_v7_market_cap
                coindata.market_cap = self.config.pbgui.market_cap
        if "edit_run_v7_vol_mcap" in st.session_state:
            if st.session_state.edit_run_v7_vol_mcap != self.config.pbgui.vol_mcap:
                self.config.pbgui.vol_mcap = st.session_state.edit_run_v7_vol_mcap
                coindata.vol_mcap = self.config.pbgui.vol_mcap
        # Symbol config
        if "edit_run_v7_approved_coins_long" in st.session_state:
            if st.session_state.edit_run_v7_approved_coins_long != self.config.live.approved_coins.long:
                self.config.live.approved_coins.long = st.session_state.edit_run_v7_approved_coins_long
                if 'All' in self.config.live.approved_coins.long:
                    self.config.live.approved_coins.long = coindata.symbols.copy()
                elif 'CPT' in self.config.live.approved_coins.long:
                    self.config.live.approved_coins.long = coindata.symbols_cpt.copy()
        if "edit_run_v7_approved_coins_short" in st.session_state:
            if st.session_state.edit_run_v7_approved_coins_short != self.config.live.approved_coins.short:
                self.config.live.approved_coins.short = st.session_state.edit_run_v7_approved_coins_short
                if 'All' in self.config.live.approved_coins.short:
                    self.config.live.approved_coins.short = coindata.symbols.copy()
                elif 'CPT' in self.config.live.approved_coins.short:
                    self.config.live.approved_coins.short = coindata.symbols_cpt.copy()
        if "edit_run_v7_ignored_coins_long" in st.session_state:
            if st.session_state.edit_run_v7_ignored_coins_long != self.config.live.ignored_coins.long:
                self.config.live.ignored_coins.long = st.session_state.edit_run_v7_ignored_coins_long
        if "edit_run_v7_ignored_coins_short" in st.session_state:
            if st.session_state.edit_run_v7_ignored_coins_short != self.config.live.ignored_coins.short:
                self.config.live.ignored_coins.short = st.session_state.edit_run_v7_ignored_coins_short
        # Display Editor
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            if self.user in self._users.list_v7():
                index = self._users.list_v7().index(self.user)
            else:
                index = 0
            st.selectbox('User',self._users.list_v7(), index = index, key="edit_run_v7_user")
        with col2:
            slist = sorted(self.remote.list())
            enabled_on = ["disabled",self.remote.name] + slist
            enabled_on_index = enabled_on.index(self.config.pbgui.enabled_on)
            st.selectbox('Enabled on',enabled_on, index = enabled_on_index, key="edit_run_v7_enabled_on")
        with col3:
            st.number_input("config version", min_value=self.config.pbgui.version, value=self.config.pbgui.version, step=1, format="%.d", key="edit_run_v7_version", help=pbgui_help.config_version)
        with col4:
            st.number_input("leverage", min_value=0.0, max_value=10.0, value=float(round(self.config.live.leverage, 0)), step=1.0, format="%.1f", key="edit_run_v7_leverage", help=pbgui_help.leverage)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("minimum_coin_age_days", min_value=0.0, max_value=365.0, value=float(round(self.config.live.minimum_coin_age_days, 0)), step=1.0, format="%.1f", key="edit_run_v7_minimum_coin_age_days", help=pbgui_help.minimum_coin_age_days)
        with col2:
            st.number_input("pnls_max_lookback_days", min_value=0.0, max_value=365.0, value=float(round(self.config.live.pnls_max_lookback_days, 0)), step=1.0, format="%.1f", key="edit_run_v7_pnls_max_lookback_days", help=pbgui_help.pnls_max_lookback_days)
        with col3:
            st.text_input("note", value=self.config.pbgui.note, key="edit_run_v7_note", help=pbgui_help.instance_note)
            # st.number_input("relative_volume_filter_clip_pct", min_value=0.0, max_value=1.0, value=float(round(self.config.live.relative_volume_filter_clip_pct, 2)), step=0.1, format="%.2f", key="edit_run_v7_relative_volume_filter_clip_pct", help=pbgui_help.relative_volume_filter_clip_pct)
        # with col3:
        #     st.number_input("pnls_max_lookback_days", min_value=0.0, max_value=365.0, value=float(round(self.config.live.pnls_max_lookback_days, 0)), step=1.0, format="%.1f", key="edit_run_v7_pnls_max_lookback_days", help=pbgui_help.pnls_max_lookback_days)
        # with col4:
        #     st.number_input("ohlcv_rolling_window", min_value=0, max_value=100, value=self.config.live.ohlcv_rolling_window, step=1, format="%.d", key="edit_run_v7_ohlcv_rolling_window", help=pbgui_help.ohlcv_rolling_window)
        col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
        with col1:
            st.number_input("price_distance_threshold", min_value=0.0, max_value=1.0, value=self.config.live.price_distance_threshold, step=0.001, format="%.3f", key="edit_run_v7_price_distance_threshold", help=pbgui_help.price_distance_threshold)
        with col2:
            st.number_input("execution_delay_seconds", min_value=1.0, max_value=60.0, value=float(self.config.live.execution_delay_seconds), step=1.0, format="%.1f", key="edit_run_v7_execution_delay_seconds", help=pbgui_help.execution_delay_seconds)
        with col3:
            st.checkbox("filter_by_min_effective_cost", value=self.config.live.filter_by_min_effective_cost, help=pbgui_help.filter_by_min_effective_cost, key="edit_run_v7_filter_by_min_effective_cost")
        with col4:
            st.checkbox("auto_gs", value=self.config.live.auto_gs, help=pbgui_help.auto_gs, key="edit_run_v7_auto_gs")
        # Advanced Settings
        # Init mode
        mode_options = {
            '': "",
            'n': "normal",
            'm': "manual",
            'gs': "graceful_stop",
            'p': "panic",
            't': "take_profit_only",
            }
        forced_mode = ['','n','m','gs','p','t']
        with st.expander("Advanced Settings", expanded=False):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                st.number_input("max_n_cancellations_per_batch", min_value=0, max_value=100, value=self.config.live.max_n_cancellations_per_batch, step=1, format="%.d", key="edit_run_v7_max_n_cancellations_per_batch", help=pbgui_help.max_n_per_batch)
            with col2:
                st.number_input("max_n_creations_per_batch", min_value=0, max_value=100, value=self.config.live.max_n_creations_per_batch, step=1, format="%.d", key="edit_run_v7_max_n_creations_per_batch", help=pbgui_help.max_n_per_batch)
            with col3:
                st.selectbox('forced_mode_long',forced_mode, index = forced_mode.index(self.config.live.forced_mode_long), format_func=lambda x: mode_options.get(x), key="edit_run_v7_forced_mode_long", help=pbgui_help.forced_mode_long_short)
            with col4:
                st.selectbox('forced_mode_short',forced_mode, index = forced_mode.index(self.config.live.forced_mode_short), format_func=lambda x: mode_options.get(x) , key="edit_run_v7_forced_mode_short", help=pbgui_help.forced_mode_long_short)
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                st.number_input("max_n_restarts_per_day", min_value=0, max_value=100, value=self.config.live.max_n_restarts_per_day, step=1, format="%.d", key="edit_run_v7_max_n_restarts_per_day", help=pbgui_help.max_n_restarts_per_day)
            with col2:
                st.number_input("ohlcvs_1m_rolling_window_days", min_value=0.0, value=round(float(self.config.live.ohlcvs_1m_rolling_window_days),1), step=1.0, format="%.1f", key="edit_run_v7_ohlcvs_1m_rolling_window_days", help=pbgui_help.ohlcvs_1m_rolling_window_days)
            with col3:
                st.number_input("ohlcvs_1m_update_after_minutes", min_value=0.0, value=round(float(self.config.live.ohlcvs_1m_update_after_minutes),1), step=1.0, format="%.1f", key="edit_run_v7_ohlcvs_1m_update_after_minutes", help=pbgui_help.ohlcvs_1m_update_after_minutes)
            with col4:
                time_in_force = ['good_till_cancelled', 'post_only']
                st.selectbox('time_in_force', time_in_force, index = time_in_force.index(self.config.live.time_in_force), key="edit_run_v7_time_in_force", help=pbgui_help.time_in_force)
        #Filters
        col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
        with col1:
            st.number_input("market_cap", min_value=0, value=self.config.pbgui.market_cap, step=50, format="%.d", key="edit_run_v7_market_cap", help=pbgui_help.market_cap)
        with col2:
            st.number_input("vol/mcap", min_value=0.0, value=round(float(self.config.pbgui.vol_mcap),2), step=0.05, format="%.2f", key="edit_run_v7_vol_mcap", help=pbgui_help.vol_mcap)
        with col3:
            st.checkbox("dynamic_ignore", value=self.config.pbgui.dynamic_ignore, help=pbgui_help.dynamic_ignore, key="edit_run_v7_dynamic_ignore")
        with col4:
            st.checkbox("empty_means_all_approved", value=self.config.live.empty_means_all_approved, help=pbgui_help.empty_means_all_approved, key="edit_run_v7_empty_means_all_approved")
        # Apply filters
        for symbol in coindata.ignored_coins:
            if symbol not in self.config.live.ignored_coins.long:
                self.config.live.ignored_coins.long.append(symbol)
            if symbol not in self.config.live.ignored_coins.short:
                self.config.live.ignored_coins.short.append(symbol)
            if symbol in self.config.live.approved_coins.long:
                self.config.live.approved_coins.long.remove(symbol)
            if symbol in self.config.live.approved_coins.short:
                self.config.live.approved_coins.short.remove(symbol)
        # Remove unavailable symbols
        for symbol in self.config.live.approved_coins.long.copy():
            if symbol not in coindata.symbols:
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.approved_coins.short.copy():
            if symbol not in coindata.symbols:
                self.config.live.approved_coins.short.remove(symbol)
        for symbol in self.config.live.ignored_coins.long.copy():
            if symbol not in coindata.symbols:
                self.config.live.ignored_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short.copy():
            if symbol not in coindata.symbols:
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
        col1, col2 = st.columns([1,1], vertical_alignment="bottom")
        with col1:
            st.multiselect('approved_coins_long', ['All', 'CPT'] + coindata.symbols, default=self.config.live.approved_coins.long, key="edit_run_v7_approved_coins_long", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_long', coindata.symbols, default=self.config.live.ignored_coins.long, key="edit_run_v7_ignored_coins_long", help=pbgui_help.ignored_coins)
        with col2:
            st.multiselect('approved_coins_short', ['All', 'CPT'] + coindata.symbols, default=self.config.live.approved_coins.short, key="edit_run_v7_approved_coins_short", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_short', coindata.symbols, default=self.config.live.ignored_coins.short, key="edit_run_v7_ignored_coins_short", help=pbgui_help.ignored_coins)
        # Edit long / short
        self.config.bot.edit()
        # View log
        self.view_log()

    @st.dialog("Paste config", width="large")
    def import_instance(self):
        # Init session_state for keys
        if "import_run_v7_user" in st.session_state:
            if st.session_state.import_run_v7_user != self.user:
                self.user = st.session_state.import_run_v7_user
                st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)
        if "import_run_v7_config" in st.session_state:
            if st.session_state.import_run_v7_config != json.dumps(self.config.config, indent=4):
                try:
                    self.config.config = json.loads(st.session_state.import_run_v7_config)
                except:
                    error_popup("Invalid JSON")
            st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)
            if self.config.live.user in self._users.list_v7():
                self._user = self.config.live.user
        # Display import
        st.selectbox('User',self._users.list_v7(), index = self._users.list_v7().index(self.user), key="import_run_v7_user")
        st.text_area(f'config', json.dumps(self.config.config, indent=4), key="import_run_v7_config", height=1200)
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("OK"):
                self.initialize()
                del st.session_state.edit_run_v7_user
                del st.session_state.edit_run_v7_enabled_on
                del st.session_state.edit_run_v7_version
                del st.session_state.edit_run_v7_leverage
                del st.session_state.edit_run_v7_pnls_max_lookback_days
                del st.session_state.edit_run_v7_minimum_coin_age_days
                del st.session_state.edit_run_v7_price_distance_threshold
                del st.session_state.edit_run_v7_execution_delay_seconds
                del st.session_state.edit_run_v7_filter_by_min_effective_cost
                del st.session_state.edit_run_v7_empty_means_all_approved
                del st.session_state.edit_run_v7_auto_gs
                del st.session_state.edit_run_v7_max_n_cancellations_per_batch
                del st.session_state.edit_run_v7_max_n_creations_per_batch
                del st.session_state.edit_run_v7_forced_mode_long
                del st.session_state.edit_run_v7_forced_mode_short
                del st.session_state.edit_run_v7_time_in_force
                del st.session_state.edit_run_v7_max_n_restarts_per_day
                del st.session_state.edit_run_v7_ohlcvs_1m_rolling_window_days
                del st.session_state.edit_run_v7_ohlcvs_1m_update_after_minutes
                del st.session_state.edit_run_v7_market_cap
                del st.session_state.edit_run_v7_vol_mcap
                del st.session_state.edit_run_v7_dynamic_ignore
                del st.session_state.edit_run_v7_approved_coins_long
                del st.session_state.edit_run_v7_approved_coins_short
                del st.session_state.edit_run_v7_ignored_coins_long
                del st.session_state.edit_run_v7_ignored_coins_short
                del st.session_state.edit_configv7_long_twe
                del st.session_state.edit_configv7_short_twe
                del st.session_state.edit_configv7_long_positions
                del st.session_state.edit_configv7_short_positions
                del st.session_state.edit_configv7_long
                del st.session_state.edit_configv7_short
                st.rerun()
        with col2:
            if st.button("Cancel"):
                st.rerun()

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
