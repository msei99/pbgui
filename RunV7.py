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
    def fragment_mimic_backtest_1m_delay(self):
        if "edit_run_v7_mimic_backtest_1m_delay" in st.session_state:
            if st.session_state.edit_run_v7_mimic_backtest_1m_delay != self.config.live.mimic_backtest_1m_delay:
                self.config.live.mimic_backtest_1m_delay = st.session_state.edit_run_v7_mimic_backtest_1m_delay
        else:
            st.session_state.edit_run_v7_mimic_backtest_1m_delay = self.config.live.mimic_backtest_1m_delay
        st.checkbox("mimic_backtest_1m_delay", help=pbgui_help.mimic_backtest_1m_delay, key="edit_run_v7_mimic_backtest_1m_delay")

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
    def fragement_ohlcvs_1m_rolling_window_days(self):
        if "edit_run_v7_ohlcvs_1m_rolling_window_days" in st.session_state:
            if st.session_state.edit_run_v7_ohlcvs_1m_rolling_window_days != self.config.live.ohlcvs_1m_rolling_window_days:
                self.config.live.ohlcvs_1m_rolling_window_days = st.session_state.edit_run_v7_ohlcvs_1m_rolling_window_days
        else:
            st.session_state.edit_run_v7_ohlcvs_1m_rolling_window_days = float(round(self.config.live.ohlcvs_1m_rolling_window_days, 0))
        st.number_input("ohlcvs_1m_rolling_window_days", min_value=0.0, max_value=365.0, step=1.0, format="%.1f", key="edit_run_v7_ohlcvs_1m_rolling_window_days", help=pbgui_help.ohlcvs_1m_rolling_window_days)

    @st.fragment
    def fragment_ohlcvs_1m_update_after_minutes(self):
        if "edit_run_v7_ohlcvs_1m_update_after_minutes" in st.session_state:
            if st.session_state.edit_run_v7_ohlcvs_1m_update_after_minutes != self.config.live.ohlcvs_1m_update_after_minutes:
                self.config.live.ohlcvs_1m_update_after_minutes = st.session_state.edit_run_v7_ohlcvs_1m_update_after_minutes
        else:
            st.session_state.edit_run_v7_ohlcvs_1m_update_after_minutes = float(round(self.config.live.ohlcvs_1m_update_after_minutes, 0))
        st.number_input("ohlcvs_1m_update_after_minutes", min_value=0.0, max_value=60.0, step=1.0, format="%.1f", key="edit_run_v7_ohlcvs_1m_update_after_minutes", help=pbgui_help.ohlcvs_1m_update_after_minutes)

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

    def fragment_filter_coins(self):
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
        # Init session state for approved_coins
        if "edit_run_v7_approved_coins_long" in st.session_state:
            if st.session_state.edit_run_v7_approved_coins_long != self.config.live.approved_coins.long:
                self.config.live.approved_coins.long = st.session_state.edit_run_v7_approved_coins_long
        else:
            st.session_state.edit_run_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_run_v7_approved_coins_short" in st.session_state:
            if st.session_state.edit_run_v7_approved_coins_short != self.config.live.approved_coins.short:
                self.config.live.approved_coins.short = st.session_state.edit_run_v7_approved_coins_short
        else:
            st.session_state.edit_run_v7_approved_coins_short = self.config.live.approved_coins.short
        # Init session state for ignored_coins
        if "edit_run_v7_ignored_coins_long" in st.session_state:
            if st.session_state.edit_run_v7_ignored_coins_long != self.config.live.ignored_coins.long:
                self.config.live.ignored_coins.long = st.session_state.edit_run_v7_ignored_coins_long
        else:
            st.session_state.edit_run_v7_ignored_coins_long = self.config.live.ignored_coins.long
        if "edit_run_v7_ignored_coins_short" in st.session_state:
            if st.session_state.edit_run_v7_ignored_coins_short != self.config.live.ignored_coins.short:
                self.config.live.ignored_coins.short = st.session_state.edit_run_v7_ignored_coins_short
        else:
            st.session_state.edit_run_v7_ignored_coins_short = self.config.live.ignored_coins.short
        # Appliy filters
        if st.session_state.edit_run_v7_apply_filters:
            self.config.live.approved_coins.long = st.session_state.pbcoindata.approved_coins
            self.config.live.approved_coins.short = st.session_state.pbcoindata.approved_coins
            self.config.live.ignored_coins.long = st.session_state.pbcoindata.ignored_coins
            self.config.live.ignored_coins.short = st.session_state.pbcoindata.ignored_coins
        # Remove unavailable coins
        for symbol in self.config.live.approved_coins.long.copy():
            if symbol not in st.session_state.pbcoindata.symbols:
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.approved_coins.short.copy():
            if symbol not in st.session_state.pbcoindata.symbols:
                self.config.live.approved_coins.short.remove(symbol)
        for symbol in self.config.live.ignored_coins.long.copy():
            if symbol not in st.session_state.pbcoindata.symbols:
                self.config.live.ignored_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short.copy():
            if symbol not in st.session_state.pbcoindata.symbols:
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
            if coin in st.session_state.pbcoindata.symbols_notices:
                st.warning(f'{coin}: {st.session_state.pbcoindata.symbols_notices[coin]}')
        # Select approved and ignored coins
        col1, col2 = st.columns([1,1], vertical_alignment="bottom")
        with col1:
            st.multiselect('approved_coins_long', st.session_state.pbcoindata.symbols, key="edit_run_v7_approved_coins_long", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_long', st.session_state.pbcoindata.symbols, key="edit_run_v7_ignored_coins_long", help=pbgui_help.ignored_coins)
        with col2:
            st.multiselect('approved_coins_short', st.session_state.pbcoindata.symbols, key="edit_run_v7_approved_coins_short", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_short', st.session_state.pbcoindata.symbols, key="edit_run_v7_ignored_coins_short", help=pbgui_help.ignored_coins)

    def fragment_market_cap(self):
        if "edit_run_v7_market_cap" in st.session_state:
            if st.session_state.edit_run_v7_market_cap != self.config.pbgui.market_cap:
                self.config.pbgui.market_cap = st.session_state.edit_run_v7_market_cap
                st.session_state.pbcoindata.market_cap = self.config.pbgui.market_cap
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_market_cap = round(self.config.pbgui.market_cap, 2)
            st.session_state.pbcoindata.market_cap = self.config.pbgui.market_cap
        st.number_input("market_cap", min_value=0, step=50, format="%.d", key="edit_run_v7_market_cap", help=pbgui_help.market_cap)

    def fragment_vol_mcap(self):
        if "edit_run_v7_vol_mcap" in st.session_state:
            if st.session_state.edit_run_v7_vol_mcap != self.config.pbgui.vol_mcap:
                self.config.pbgui.vol_mcap = st.session_state.edit_run_v7_vol_mcap
                st.session_state.pbcoindata.vol_mcap = self.config.pbgui.vol_mcap
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_vol_mcap = round(self.config.pbgui.vol_mcap, 2)
            st.session_state.pbcoindata.vol_mcap = self.config.pbgui.vol_mcap
        st.number_input("vol/mcap", min_value=0.0, step=0.05, format="%.2f", key="edit_run_v7_vol_mcap", help=pbgui_help.vol_mcap)

    def fragment_tags(self):
        if "edit_run_v7_tags" in st.session_state:
            if st.session_state.edit_run_v7_tags != self.config.pbgui.tags:
                self.config.pbgui.tags = st.session_state.edit_run_v7_tags
                st.session_state.pbcoindata.tags = self.config.pbgui.tags
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            # Remove tags that are no longer available
            for tag in self.config.pbgui.tags:
                if tag not in st.session_state.pbcoindata.all_tags:
                    self.config.pbgui.tags.remove(tag)
            st.session_state.edit_run_v7_tags = self.config.pbgui.tags
            st.session_state.pbcoindata.tags = self.config.pbgui.tags
        st.multiselect("Tags", st.session_state.pbcoindata.all_tags, key="edit_run_v7_tags", help=pbgui_help.coindata_tags)

    def fragment_only_cpt(self):
        if "edit_run_v7_only_cpt" in st.session_state:
            if st.session_state.edit_run_v7_only_cpt != self.config.pbgui.only_cpt:
                self.config.pbgui.only_cpt = st.session_state.edit_run_v7_only_cpt
                st.session_state.pbcoindata.only_cpt = self.config.pbgui.only_cpt
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_only_cpt = self.config.pbgui.only_cpt
            st.session_state.pbcoindata.only_cpt = self.config.pbgui.only_cpt
        st.checkbox("only_cpt", help=pbgui_help.only_cpt, key="edit_run_v7_only_cpt")
    
    def fragment_notices_ignore(self):
        if "edit_run_v7_notices_ignore" in st.session_state:
            if st.session_state.edit_run_v7_notices_ignore != self.config.pbgui.notices_ignore:
                self.config.pbgui.notices_ignore = st.session_state.edit_run_v7_notices_ignore
                st.session_state.pbcoindata.notices_ignore = self.config.pbgui.notices_ignore
                if st.session_state.edit_run_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_run_v7_notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.pbcoindata.notices_ignore = self.config.pbgui.notices_ignore
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
        col1, col2, col3, col4 = st.columns([1,1,1,1])
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
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_minimum_coin_age_days()
        with col2:
            self.fragement_pnls_max_lookback_days()
        with col3:
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
            self.fragment_auto_gs()
            self.fragment_mimic_backtest_1m_delay()

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
                self.fragement_ohlcvs_1m_rolling_window_days()
            with col3:
                self.fragment_ohlcvs_1m_update_after_minutes()
            with col4:
                self.fragment_time_in_force()

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
            for coin in st.session_state.pbcoindata.approved_coins:
                if coin in st.session_state.pbcoindata.symbols_notices:
                    st.warning(f'{coin}: {st.session_state.pbcoindata.symbols_notices[coin]}')
            st.code(f'approved_symbols: {st.session_state.pbcoindata.approved_coins}', wrap_lines=True)
            st.code(f'dynamic_ignored symbols: {st.session_state.pbcoindata.ignored_coins}', wrap_lines=True)

        # Edit coin_overrides
        self.config.view_coin_overrides()

        # Edit coin_flags
        # if self.config.live.coin_flags:
        #     flags = True
        # else:
        #     flags = False
        # with st.expander("Coin Flags", expanded=flags):
        #     # Init
        #     if not "ed_key" in st.session_state:
        #         st.session_state.ed_key = 0
        #     ed_key = st.session_state.ed_key
        #     if f'select_cf_coin_{ed_key}' in st.session_state:
        #         ed = st.session_state[f'select_cf_coin_{ed_key}']
        #         for row in ed["edited_rows"]:
        #             if "edit" in ed["edited_rows"][row]:
        #                 if ed["edited_rows"][row]["edit"]:
        #                     st.session_state.edit_coin_flag = st.session_state.cf_data[row]["coin"]
        #     if not "cf_data" in st.session_state:
        #         cf_data = []
        #         if self.config.live.coin_flags:
        #             for coin in self.config.live.coin_flags:
        #                 lm = {
        #                     "n": "normal",
        #                     "m": "manual",
        #                     "gs": "graceful_stop",
        #                     "p": "panic"
        #                 }.get(self.config.live.coin_flags[coin].split("-lm")[1].split()[0], "") if "-lm" in self.config.live.coin_flags[coin] else ""
        #                 lw = self.config.live.coin_flags[coin].split("-lw")[1].split()[0] if "-lw" in self.config.live.coin_flags[coin] else ""
        #                 sm = {
        #                     "n": "normal",
        #                     "m": "manual",
        #                     "gs": "graceful_stop",
        #                     "p": "panic"
        #                 }.get(self.config.live.coin_flags[coin].split("-sm")[1].split()[0], "") if "-sm" in self.config.live.coin_flags[coin] else ""
        #                 sw = self.config.live.coin_flags[coin].split("-sw")[1].split()[0] if "-sw" in self.config.live.coin_flags[coin] else ""
        #                 lev = self.config.live.coin_flags[coin].split("-lev")[1].split()[0] if "-lev" in self.config.live.coin_flags[coin] else ""
        #                 config = self.config.live.coin_flags[coin].split("-lc")[1].split()[0] if "-lc" in self.config.live.coin_flags[coin] else ""
        #                 cf_data.append({
        #                     'edit': False,
        #                     'coin': coin,
        #                     'long_mode': lm,
        #                     'long_we': lw,
        #                     'short_mode': sm,
        #                     'short_we': sw,
        #                     'leverage': lev,
        #                     'config_file': config,
        #                     'flags': self.config.live.coin_flags[coin]
        #                 })
        #         st.session_state.cf_data = cf_data
        #     # Display coin_flags
        #     if st.session_state.cf_data and not "edit_coin_flag" in st.session_state:
        #         d = st.session_state.cf_data
        #         st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_cf_coin_{ed_key}', disabled=['coin','flags'])
        #     if "edit_run_v7_add_coin_flag_button" in st.session_state:
        #         if st.session_state.edit_run_v7_add_coin_flag_button:
        #             st.session_state.edit_coin_flag = st.session_state.edit_run_v7_add_coin_flag
        #     if "edit_coin_flag" in st.session_state:
        #         self.edit_coin_flag(st.session_state.edit_coin_flag)
        #     else:
        #         col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
        #         with col1:
        #             st.selectbox('Symbol', st.session_state.pbcoindata.symbols, key="edit_run_v7_add_coin_flag")
        #         with col2:
        #             st.button("Add Coin Flag", key="edit_run_v7_add_coin_flag_button")
        # Edit long / short
        self.config.bot.edit()
        # View log
        self.view_log()

    # def edit_coin_flag(self, symbol):
    #     # Init
    #     # mode_long = ""
    #     # we_long = None
    #     # mode_short = ""
    #     # we_short = None
    #     lev = None
    #     config = False
    #     # Init from config
    #     if self.config.live.coin_flags and "edit_run_v7_cf_mode_long" not in st.session_state:
    #         if symbol in self.config.live.coin_flags:
    #             flags = self.config.live.coin_flags[symbol]
    #             # if -nm in flags then get mode_long
    #             if "-lm" in flags:
    #                 st.session_state.edit_run_v7_cf_mode_long = flags.split("-lm")[1].split()[0]
    #             # if -lw in flags then get we_long
    #             if "-lw" in flags:
    #                 st.session_state.edit_run_v7_cf_we_long = float(flags.split("-lw")[1].split()[0])
    #             # if -sm in flags then get mode_short
    #             if "-sm" in flags:
    #                 st.session_state.edit_run_v7_cf_mode_short = flags.split("-sm")[1].split()[0]
    #             # if -sw in flags then get we_short
    #             if "-sw" in flags:
    #                 st.session_state.edit_run_v7_cf_we_short = float(flags.split("-sw")[1].split()[0])
    #             # if -lev in flags then get leverage
    #             if "-lev" in flags:
    #                 st.session_state.edit_run_v7_cf_lev = float(flags.split("-lev")[1].split()[0])
    #             if "-lc" in flags:
    #                 config = True
    #                 if "cf_config" not in st.session_state:
    #                     st.session_state.cf_config = ConfigV7(file_name=f'{PBGDIR}/data/run_v7/{self.user}/{symbol}.json')
    #                     st.session_state.cf_config.load_config()
    #                     if "edit_cf_configv7_long" in st.session_state:
    #                         del st.session_state.edit_cf_configv7_long
    #                     if "edit_cf_configv7_short" in st.session_state:
    #                         del st.session_state.edit_cf_configv7_short
    #     # Init session_state for keys
    #     if "edit_run_v7_cf_config" in st.session_state:
    #         if st.session_state.edit_run_v7_cf_config != config:
    #             config = st.session_state.edit_run_v7_cf_config
    #     st.write(f"{symbol}")
    #     col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
    #     with col1:
    #         st.selectbox('mode_long',self.MODE, format_func=lambda x: self.MODE_OPTIONS.get(x), key="edit_run_v7_cf_mode_long", help=pbgui_help.coin_flags_mode)
    #     with col2:
    #         st.number_input("long_we", min_value=0.0, step=0.05, format="%0.2f", key="edit_run_v7_cf_we_long", help=pbgui_help.coin_flags_we)
    #     with col3:
    #         st.selectbox('mode_short',self.MODE, format_func=lambda x: self.MODE_OPTIONS.get(x), key="edit_run_v7_cf_mode_short", help=pbgui_help.coin_flags_mode)
    #     with col4:
    #         st.number_input("short_we", min_value=0.0, step=0.05, format="%0.2f", key="edit_run_v7_cf_we_short", help=pbgui_help.coin_flags_we)
    #     with col5:
    #         st.number_input("leverage", min_value=0.0, max_value=100.0, step=1.0, format="%.1f", key="edit_run_v7_cf_lev", help=pbgui_help.coin_flags_lev)
    #     st.checkbox("Config", value=config, key="edit_run_v7_cf_config", help=pbgui_help.coin_flags_config)
    #     if config:
    #         if "cf_config" not in st.session_state:
    #             st.session_state.cf_config = ConfigV7()
    #         st.session_state.cf_config.bot.edit_cf()
    #     col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
    #     with col1:
    #         if st.button("Save"):
    #             # coin_flags: {"ETH": "-sm n -lm gs", "XRP": "-lm p -lc path/to/other_config.json"}
    #             lm = ""
    #             lw = ""
    #             sm = ""
    #             sw = ""
    #             lc = ""
    #             lev = ""
    #             if self.config.live.coin_flags:
    #                 if symbol in self.config.live.coin_flags:
    #                     del self.config.live.coin_flags[symbol]
    #             if st.session_state.edit_run_v7_cf_mode_long:
    #                 lm = f'-lm {st.session_state.edit_run_v7_cf_mode_long} '
    #             if abs(st.session_state.edit_run_v7_cf_we_long) > 1e-9:
    #                 lw = f'-lw {round(st.session_state.edit_run_v7_cf_we_long,2)} '
    #             if st.session_state.edit_run_v7_cf_mode_short:
    #                 sm = f'-sm {st.session_state.edit_run_v7_cf_mode_short} '
    #             if abs(st.session_state.edit_run_v7_cf_we_short) > 1e-9:
    #                 sw = f'-sw {round(st.session_state.edit_run_v7_cf_we_short,2)} '
    #             if abs(st.session_state.edit_run_v7_cf_lev) > 1e-9:
    #                 lev = f'-lev {round(st.session_state.edit_run_v7_cf_lev,1)} '
    #             if st.session_state.edit_run_v7_cf_config:
    #                 lc = f'-lc {symbol}.json'
    #                 st.session_state.cf_config.config_file = Path(f'{PBGDIR}/data/run_v7/{self.user}/{symbol}.json')
    #                 st.session_state.cf_config.save_config()
    #             else:
    #                 Path(f'{PBGDIR}/data/run_v7/{self.user}/{symbol}.json').unlink(missing_ok=True)
    #             if lm or lw or sm or sw:
    #                 flags = f"{lm}{lw}{sm}{sw}{lev}{lc}"
    #                 if flags[-1] == " ":
    #                     flags = flags[:-1]
    #                 self.config.live.coin_flags[symbol] = flags
    #                 # sort coin_flags
    #                 self.config.live.coin_flags = dict(sorted(self.config.live.coin_flags.items()))
    #             self.save()
    #             self.clean_cf_session_state()
    #             st.rerun()
    #     with col2:
    #         if st.button("Cancel"):
    #             self.clean_cf_session_state()
    #             st.rerun()
    #     with col3:
    #         if st.button("Remove"):
    #             if self.config.live.coin_flags:
    #                 if symbol in self.config.live.coin_flags:
    #                     del self.config.live.coin_flags[symbol]
    #             Path(f'{PBGDIR}/data/run_v7/{self.user}/{symbol}.json').unlink(missing_ok=True)
    #             self.save()
    #             self.clean_cf_session_state()
    #             st.rerun()
    
    # def clean_cf_session_state(self):
    #     if "cf_config" in st.session_state:
    #         del st.session_state.cf_config
    #     if "edit_run_v7_cf_mode_long" in st.session_state:
    #         del st.session_state.edit_run_v7_cf_mode_long
    #     if "edit_run_v7_cf_we_long" in st.session_state:
    #         del st.session_state.edit_run_v7_cf_we_long
    #     if "edit_run_v7_cf_mode_short" in st.session_state:
    #         del st.session_state.edit_run_v7_cf_mode_short
    #     if "edit_run_v7_cf_we_short" in st.session_state:
    #         del st.session_state.edit_run_v7_cf_we_short
    #     if "edit_run_v7_cf_lev" in st.session_state:
    #         del st.session_state.edit_run_v7_cf_lev
    #     if "edit_run_v7_cf_config" in st.session_state:
    #         del st.session_state.edit_run_v7_cf_config
    #     if "edit_coin_flag" in st.session_state:
    #         del st.session_state.edit_coin_flag
    #     if "cf_data" in st.session_state:
    #         del st.session_state.cf_data
    #     if "ed_key" in st.session_state:
    #         st.session_state.ed_key += 1

    @st.dialog("Paste config", width="large")
    def import_instance(self):
        # Init session_state for keys
        if "import_run_v7_user" in st.session_state:
            if st.session_state.import_run_v7_user != self.user:
                self.user = st.session_state.import_run_v7_user
                st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)
        else:
            st.session_state.import_run_v7_user = self.user
        if "import_run_v7_config" in st.session_state:
            if st.session_state.import_run_v7_config != json.dumps(self.config.config, indent=4):
                try:
                    self.config.config = json.loads(st.session_state.import_run_v7_config)
                except:
                    error_popup("Invalid JSON")
            st.session_state.import_run_v7_config = json.dumps(self.config.config, indent=4)
            if self.config.live.user in self._users.list_v7():
                self._user = self.config.live.user
        else:
            st.session_state.import_run_v7_config = ""
        # Display import
        st.selectbox('User',self._users.list_v7(), key="import_run_v7_user")
        st.text_area(f'config', key="import_run_v7_config", height=500)
        # st.text_area(f'config', json.dumps(self.config.config, indent=4), key="import_run_v7_config", height=500)
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("OK"):
                self.initialize()
                del st.session_state.edit_run_v7_user
                del st.session_state.edit_run_v7_enabled_on
                del st.session_state.edit_run_v7_version
                del st.session_state.edit_run_v7_leverage
                del st.session_state.edit_run_v7_market_orders_allowed
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
