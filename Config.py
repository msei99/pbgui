import streamlit as st
from pathlib import Path
import json
from pbgui_func import validateJSON, config_pretty_str, error_popup
import pbgui_help
import traceback
import multiprocessing
import datetime

class Config:
    def __init__(self, file_name = None, config = None):
        self._config_file = file_name
        self._long_we = 1.0
        self._short_we = 1.0
        self._long_enabled = True
        self._short_enabled = False
        self._type = None
        self._preview_grid = False
        if config:
            self.config = config
        else:
            self._config = None

    @property
    def type(self): return self._type

    @property
    def config_file(self): return self._config_file

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file
        
    @property
    def config(self): return self._config

    @config.setter
    def config(self, new_config):
        if new_config != "None":
            if validateJSON(new_config):
                self._config = new_config
                self.update_config()
                if "error_config" in st.session_state:
                    del st.session_state.error_config
            else:
                st.session_state.error_config = "Config is invalid"

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file

    @property
    def long_we(self): return self._long_we

    @long_we.setter
    def long_we(self, new_long_we):
        self._long_we = round(new_long_we,2)
        if self._config:
            t = json.loads(self._config)
            t["long"]["wallet_exposure_limit"] = self._long_we
            self._config = config_pretty_str(t)
    
    @property
    def long_enabled(self): return self._long_enabled

    @long_enabled.setter
    def long_enabled(self, new_long_enabled):
        self._long_enabled = new_long_enabled
        if self._config:
            t = json.loads(self._config)
            t["long"]["enabled"] = self._long_enabled
            self._config = config_pretty_str(t)

    @property
    def short_enabled(self): return self._short_enabled

    @short_enabled.setter
    def short_enabled(self, new_short_enabled):
        self._short_enabled = new_short_enabled
        if self._config:
            t = json.loads(self._config)
            t["short"]["enabled"] = self._short_enabled
            self._config = config_pretty_str(t)

    @property
    def short_we(self): return self._short_we

    @short_we.setter
    def short_we(self, new_short_we):
        self._short_we = round(new_short_we,2)
        if self._config:
            t = json.loads(self._config)
            t["short"]["wallet_exposure_limit"] = self._short_we
            self._config = config_pretty_str(t)

    @property
    def preview_grid(self): return self._preview_grid
    @preview_grid.setter
    def preview_grid(self, new_preview_grid):
        self._preview_grid = new_preview_grid

    def update_config(self):
        self.long_we = json.loads(self._config)["long"]["wallet_exposure_limit"]
        self.short_we = json.loads(self._config)["short"]["wallet_exposure_limit"]
        self.long_enabled = json.loads(self._config)["long"]["enabled"]
        self.short_enabled = json.loads(self._config)["short"]["enabled"]
        long = json.loads(self._config)["long"]
        if "ddown_factor" in long:
            self._type = "recursive_grid"
        elif "qty_pct_entry" in long:
            self._type = "clock"
        elif "grid_span" in long:
            self._type = "neat_grid"

    def load_config(self):
        file =  Path(f'{self._config_file}')
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                self._config = f.read()
                self.update_config()

    def save_config(self):
        if self._config != None and self._config_file != None:
            file = Path(f'{self._config_file}')
            with open(file, "w", encoding='utf-8') as f:
                f.write(self._config)

    def edit_config(self):
        # Init session_state for keys
        if "config_long_enabled" in st.session_state:
            if st.session_state.config_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.config_long_enabled
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_short_enabled" in st.session_state:
            if st.session_state.config_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.config_short_enabled
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_long_we" in st.session_state:
            if st.session_state.config_long_we != self.long_we:
                self.long_we = st.session_state.config_long_we
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_short_we" in st.session_state:
            if st.session_state.config_short_we != self.short_we:
                self.short_we = st.session_state.config_short_we
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_preview_grid" in st.session_state:
            if st.session_state.config_preview_grid != self.preview_grid:
                self.preview_grid = st.session_state.config_preview_grid
        if "config_instance_config" in st.session_state:
            if st.session_state.config_instance_config != self.config:
                self.config = st.session_state.config_instance_config
                st.session_state.config_long_enabled = self.long_enabled
                st.session_state.config_short_enabled = self.short_enabled
                st.session_state.config_long_we = self.long_we
                st.session_state.config_short_we = self.short_we
            else:
                if validateJSON(st.session_state.config_instance_config):
                    if "error_config" in st.session_state:
                        del st.session_state.error_config
        # if self.config:
        #     self.config = st.session_state.config_instance_config
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            st.toggle("Long enabled", value=self.long_enabled, key="config_long_enabled", help=None)
            st.number_input("LONG_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=100.0, value=float(round(self.long_we,2)), step=0.05, format="%.2f", key="config_long_we", help=pbgui_help.exposure)
        with col2:
            st.toggle("Short enabled", value=self.short_enabled, key="config_short_enabled", help=None)
            st.number_input("SHORT_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=100.0, value=float(round(self.short_we,2)), step=0.05, format="%.2f", key="config_short_we", help=pbgui_help.exposure)
        with col3:
            st.toggle("Preview Grid", value=self.preview_grid, key="config_preview_grid", help=None)
            st.selectbox("Config Type", [self.type], index=0, key="config_type", help=None, disabled=True)
        # Init height and color with defaults
        height = 600
        color = None
        # Display Error
        if "error_config" in st.session_state:
            st.error(st.session_state.error_config, icon="🚨")
            color = "red"
        if not self.config is None:
            height = len(self.config.splitlines()) *23
        if height < 600:
            height = 600
        if not self.config:
            color = "red"
        st.text_area(f':{color}[config]', self.config, key="config_instance_config", height=height)

# config template
# {"backtest": {"base_dir": "backtests",
#               "compress_cache": true,
#               "end_date": "now",
#               "exchange": "binance",
#               "start_date": "2021-05-01",
#               "starting_balance": 100000.0},
#  "bot": {"long": {"close_grid_markup_range": 0.0016219,
#                   "close_grid_min_markup": 0.012842,
#                   "close_grid_qty_pct": 0.65242,
#                   "close_trailing_grid_ratio": 0.021638,
#                   "close_trailing_qty_pct": 0.88439,
#                   "close_trailing_retracement_pct": 0.028672,
#                   "close_trailing_threshold_pct": 0.065293,
#                   "ema_span_0": 465.26,
#                   "ema_span_1": 1120.5,
#                   "entry_grid_double_down_factor": 2.3744,
#                   "entry_grid_spacing_pct": 0.052341,
#                   "entry_grid_spacing_weight": 0.070271,
#                   "entry_initial_ema_dist": -0.0059754,
#                   "entry_initial_qty_pct": 0.029454,
#                   "entry_trailing_grid_ratio": -0.28169,
#                   "entry_trailing_retracement_pct": 0.0024748,
#                   "entry_trailing_threshold_pct": -0.051708,
#                   "filter_relative_volume_clip_pct": 0.51416,
#                   "filter_rolling_window": 60.0,
#                   "n_positions": 10.675,
#                   "total_wallet_exposure_limit": 0.95859,
#                   "unstuck_close_pct": 0.071741,
#                   "unstuck_ema_dist": -0.053527,
#                   "unstuck_loss_allowance_pct": 0.033558,
#                   "unstuck_threshold": 0.49002},
#          "short": {"close_grid_markup_range": 0.0049057,
#                    "close_grid_min_markup": 0.013579,
#                    "close_grid_qty_pct": 0.6168,
#                    "close_trailing_grid_ratio": 0.88873,
#                    "close_trailing_qty_pct": 0.97705,
#                    "close_trailing_retracement_pct": 0.095287,
#                    "close_trailing_threshold_pct": -0.060579,
#                    "ema_span_0": 819.23,
#                    "ema_span_1": 246.39,
#                    "entry_grid_double_down_factor": 2.3062,
#                    "entry_grid_spacing_pct": 0.072015,
#                    "entry_grid_spacing_weight": 1.4565,
#                    "entry_initial_ema_dist": -0.072047,
#                    "entry_initial_qty_pct": 0.072205,
#                    "entry_trailing_grid_ratio": -0.02319,
#                    "entry_trailing_retracement_pct": 0.017338,
#                    "entry_trailing_threshold_pct": -0.084177,
#                    "filter_relative_volume_clip_pct": 0.5183,
#                    "filter_rolling_window": 68.072,
#                    "n_positions": 1.1534,
#                    "total_wallet_exposure_limit": 0.209,
#                    "unstuck_close_pct": 0.052695,
#                    "unstuck_ema_dist": -0.026947,
#                    "unstuck_loss_allowance_pct": 0.046017,
#                    "unstuck_threshold": 0.58422}},
#  "live": {"approved_coins": [],
#           "auto_gs": true,
#           "coin_flags": {},
#           "empty_means_all_approved": false,
#           "execution_delay_seconds": 2.0,
#           "filter_by_min_effective_cost": true,
#           "forced_mode_long": "",
#           "forced_mode_short": "",
#           "ignored_coins": [],
#           "leverage": 10.0,
#           "max_n_cancellations_per_batch": 5,
#           "max_n_creations_per_batch": 3,
#           "max_n_restarts_per_day": 10,
#           "minimum_coin_age_days": 30.0,
#           "ohlcvs_1m_rolling_window_days": 4.0,
#           "ohlcvs_1m_update_after_minutes": 10.0,
#           "pnls_max_lookback_days": 30.0,
#           "price_distance_threshold": 0.002,
#           "time_in_force": "good_till_cancelled",
#           "user": "bybit_01"},
#  "optimize": {"bounds": {"long_close_grid_markup_range": [0.0, 0.03],
#                          "long_close_grid_min_markup": [0.001, 0.03],
#                          "long_close_grid_qty_pct": [0.05, 1.0],
#                          "long_close_trailing_grid_ratio": [-1.0, 1.0],
#                          "long_close_trailing_qty_pct": [0.05, 1.0],
#                          "long_close_trailing_retracement_pct": [0.0, 0.1],
#                          "long_close_trailing_threshold_pct": [-0.1, 0.1],
#                          "long_ema_span_0": [200.0, 1440.0],
#                          "long_ema_span_1": [200.0, 1440.0],
#                          "long_entry_grid_double_down_factor": [0.1, 3.0],
#                          "long_entry_grid_spacing_pct": [0.001, 0.12],
#                          "long_entry_grid_spacing_weight": [0.0, 10.0],
#                          "long_entry_initial_ema_dist": [-0.1, 0.003],
#                          "long_entry_initial_qty_pct": [0.005, 0.1],
#                          "long_entry_trailing_grid_ratio": [-1.0, 1.0],
#                          "long_entry_trailing_retracement_pct": [0.0, 0.1],
#                          "long_entry_trailing_threshold_pct": [-0.1, 0.1],
#                          "long_filter_relative_volume_clip_pct": [0.0, 1.0],
#                          "long_filter_rolling_window": [10.0, 360.0],
#                          "long_n_positions": [1.0, 20.0],
#                          "long_total_wallet_exposure_limit": [0.0, 5.0],
#                          "long_unstuck_close_pct": [0.001, 0.1],
#                          "long_unstuck_ema_dist": [-0.1, 0.01],
#                          "long_unstuck_loss_allowance_pct": [0.0, 0.05],
#                          "long_unstuck_threshold": [0.4, 0.95],
#                          "short_close_grid_markup_range": [0.0, 0.03],
#                          "short_close_grid_min_markup": [0.001, 0.03],
#                          "short_close_grid_qty_pct": [0.05, 1.0],
#                          "short_close_trailing_grid_ratio": [-1.0, 1.0],
#                          "short_close_trailing_qty_pct": [0.05, 1.0],
#                          "short_close_trailing_retracement_pct": [0.0, 0.1],
#                          "short_close_trailing_threshold_pct": [-0.1, 0.1],
#                          "short_ema_span_0": [200.0, 1440.0],
#                          "short_ema_span_1": [200.0, 1440.0],
#                          "short_entry_grid_double_down_factor": [0.1, 3.0],
#                          "short_entry_grid_spacing_pct": [0.001, 0.12],
#                          "short_entry_grid_spacing_weight": [0.0, 10.0],
#                          "short_entry_initial_ema_dist": [-0.1, 0.003],
#                          "short_entry_initial_qty_pct": [0.005, 0.1],
#                          "short_entry_trailing_grid_ratio": [-1.0, 1.0],
#                          "short_entry_trailing_retracement_pct": [0.0, 0.1],
#                          "short_entry_trailing_threshold_pct": [-0.1, 0.1],
#                          "short_filter_relative_volume_clip_pct": [0.0, 1.0],
#                          "short_filter_rolling_window": [10.0, 360.0],
#                          "short_n_positions": [1.0, 20.0],
#                          "short_total_wallet_exposure_limit": [0.0, 5.0],
#                          "short_unstuck_close_pct": [0.001, 0.1],
#                          "short_unstuck_ema_dist": [-0.1, 0.01],
#                          "short_unstuck_loss_allowance_pct": [0.0, 0.05],
#                          "short_unstuck_threshold": [0.4, 0.95]},
#               "crossover_probability": 0.7,
#               "iters": 30000,
#               "limits": {"lower_bound_drawdown_worst": 0.25,
#                          "lower_bound_equity_balance_diff_mean": 0.01,
#                          "lower_bound_loss_profit_ratio": 0.6},
#               "mutation_probability": 0.2,
#               "n_cpus": 5,
#               "population_size": 500,
#               "scoring": ["mdg", "sharpe_ratio"]}}
class Backtest:
    def __init__(self):
        self._base_dir = "backtests"
        self._compress_cache = True
        self._end_date = "now"
        self._exchange = "binance"
        self._start_date = "2020-01-01"
        self._starting_balance = 1000.0
        self._backtest = {
            "base_dir": self._base_dir,
            "compress_cache": self._compress_cache,
            "end_date": self._end_date,
            "exchange": self._exchange,
            "start_date": self._start_date,
            "starting_balance": self._starting_balance
        }
    
    def __repr__(self):
        return str(self._backtest)
    
    @property
    def backtest(self): return self._backtest
    @backtest.setter
    def backtest(self, new_backtest):
        if "base_dir" in new_backtest:
            self.base_dir = new_backtest["base_dir"]
        if "compress_cache" in new_backtest:
            self.compress_cache = new_backtest["compress_cache"]
        if "end_date" in new_backtest:
            self.end_date = new_backtest["end_date"]
        if "exchange" in new_backtest:
            self.exchange = new_backtest["exchange"]
        if "start_date" in new_backtest:
            self.start_date = new_backtest["start_date"]
        if "starting_balance" in new_backtest:
            self.starting_balance = new_backtest["starting_balance"]
    
    @property
    def base_dir(self): return self._base_dir
    @property
    def compress_cache(self): return self._compress_cache
    @property
    def end_date(self):
        if self._end_date == "now":
            return datetime.datetime.now().strftime("%Y-%m-%d")
        return self._end_date
    @property
    def exchange(self): return self._exchange
    @property
    def start_date(self): return self._start_date
    @property
    def starting_balance(self): return self._starting_balance

    @base_dir.setter
    def base_dir(self, new_base_dir):
        self._base_dir = new_base_dir
        self._backtest["base_dir"] = self._base_dir
    @compress_cache.setter
    def compress_cache(self, new_compress_cache):
        self._compress_cache = new_compress_cache
        self._backtest["compress_cache"] = self._compress_cache
    @end_date.setter
    def end_date(self, new_end_date):
        self._end_date = new_end_date
        self._backtest["end_date"] = self._end_date
    @exchange.setter
    def exchange(self, new_exchange):
        self._exchange = new_exchange
        self._backtest["exchange"] = self._exchange
    @start_date.setter
    def start_date(self, new_start_date):
        self._start_date = new_start_date
        self._backtest["start_date"] = self._start_date
    @starting_balance.setter
    def starting_balance(self, new_starting_balance):
        self._starting_balance = new_starting_balance
        self._backtest["starting_balance"] = self._starting_balance

class Bot:
    def __init__(self):
        self._long = Long()
        self._short = Short()
        self._bot = {
            "long": self._long._long,
            "short": self._short._short
        }    

    def __repr__(self):
        return str(self._bot)
    
    @property
    def bot(self): return self._bot
    @bot.setter
    def bot(self, new_bot):
        if "long" in new_bot:
            self.long = new_bot["long"]
        if "short" in new_bot:
            self.short = new_bot["short"]
    
    @property
    def long(self): return self._long
    @property
    def short(self): return self._short

    @long.setter
    def long(self, new_long):
        self._long.long = new_long
        self._bot["long"] = self._long.long
    @short.setter
    def short(self, new_short):
        self._short.short = new_short
        self._bot["short"] = self._short.short
    
    def edit(self):
        # Init session_state for keys
        if "edit_configv7_long_twe" in st.session_state:
            if st.session_state.edit_configv7_long_twe != self.long.total_wallet_exposure_limit:
                self.long.total_wallet_exposure_limit = round(st.session_state.edit_configv7_long_twe,2)
                st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_configv7_long_positions" in st.session_state:
            if st.session_state.edit_configv7_long_positions != self.long.n_positions:
                self.long.n_positions = round(st.session_state.edit_configv7_long_positions,0)
                st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_configv7_short_twe" in st.session_state:
            if st.session_state.edit_configv7_short_twe != self.short.total_wallet_exposure_limit:
                self.short.total_wallet_exposure_limit = round(st.session_state.edit_configv7_short_twe,2)
                st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
        if "edit_configv7_short_positions" in st.session_state:
            if st.session_state.edit_configv7_short_positions != self.short.n_positions:
                self.short.n_positions = round(st.session_state.edit_configv7_short_positions,0)
                st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)        
        if "edit_configv7_long" in st.session_state:
            if st.session_state.edit_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_configv7_long)
                except:
                    error_popup("Invalid JSON")
            st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_configv7_short" in st.session_state:
            if st.session_state.edit_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_configv7_short)
                except:
                    error_popup("Invalid JSON")
            st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("long twe", min_value=0.0, max_value=100.0, value=float(self.long.total_wallet_exposure_limit), step=0.05, format="%.2f", key="edit_configv7_long_twe", help=pbgui_help.total_wallet_exposure_limit)
        with col2:
            st.number_input("long positions", min_value=0.0, max_value=100.0, value=float(self.long.n_positions), step=1.0, format="%.2f", key="edit_configv7_long_positions", help=pbgui_help.n_positions)
        with col3:
            st.number_input("short twe", min_value=0.0, max_value=100.0, value=float(self.short.total_wallet_exposure_limit), step=0.05, format="%.2f", key="edit_configv7_short_twe", help=pbgui_help.total_wallet_exposure_limit)
        with col4:
            st.number_input("short positions", min_value=0.0, max_value=100.0, value=float(self.short.n_positions), step=1.0, format="%.2f", key="edit_configv7_short_positions", help=pbgui_help.n_positions)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', json.dumps(self.bot["long"], indent=4), key="edit_configv7_long", height=600)
        with col2:
            st.text_area(f'short', json.dumps(self.bot["short"], indent=4), key="edit_configv7_short", height=600)

class Long:
    def __init__(self):
        self._close_grid_markup_range = 0.0015976
        self._close_grid_min_markup = 0.012839
        self._close_grid_qty_pct = 0.8195
        self._close_trailing_grid_ratio = 0.042114
        self._close_trailing_qty_pct = 1
        self._close_trailing_retracement_pct = 0.066097
        self._close_trailing_threshold_pct = 0.06726
        self._ema_span_0 = 469.02
        self._ema_span_1 = 1118.9
        self._entry_grid_double_down_factor = 2.3738
        self._entry_grid_spacing_pct = 0.052372
        self._entry_grid_spacing_weight = 0.17715
        self._entry_initial_ema_dist = -0.0060574
        self._entry_initial_qty_pct = 0.019955
        self._entry_trailing_grid_ratio = -0.28053
        self._entry_trailing_retracement_pct = 0.0024762
        self._entry_trailing_threshold_pct = 0.014956
        self._filter_relative_volume_clip_pct = 0.51416
        self._filter_rolling_window = 60.0
        self._n_positions = 9.6662
        self._total_wallet_exposure_limit = 0.8536
        self._unstuck_close_pct = 0.049593
        self._unstuck_ema_dist = -0.051669
        self._unstuck_loss_allowance_pct = 0.044329
        self._unstuck_threshold = 0.46953
        self._long = {
            "close_grid_markup_range": self._close_grid_markup_range,
            "close_grid_min_markup": self._close_grid_min_markup,
            "close_grid_qty_pct": self._close_grid_qty_pct,
            "close_trailing_grid_ratio": self._close_trailing_grid_ratio,
            "close_trailing_qty_pct": self._close_trailing_qty_pct,
            "close_trailing_retracement_pct": self._close_trailing_retracement_pct,
            "close_trailing_threshold_pct": self._close_trailing_threshold_pct,
            "ema_span_0": self._ema_span_0,
            "ema_span_1": self._ema_span_1,
            "entry_grid_double_down_factor": self._entry_grid_double_down_factor,
            "entry_grid_spacing_pct": self._entry_grid_spacing_pct,
            "entry_grid_spacing_weight": self._entry_grid_spacing_weight,
            "entry_initial_ema_dist": self._entry_initial_ema_dist,
            "entry_initial_qty_pct": self._entry_initial_qty_pct,
            "entry_trailing_grid_ratio": self._entry_trailing_grid_ratio,
            "entry_trailing_retracement_pct": self._entry_trailing_retracement_pct,
            "entry_trailing_threshold_pct": self._entry_trailing_threshold_pct,
            "filter_relative_volume_clip_pct": self._filter_relative_volume_clip_pct,
            "filter_rolling_window": self._filter_rolling_window,
            "n_positions": self._n_positions,
            "total_wallet_exposure_limit": self._total_wallet_exposure_limit,
            "unstuck_close_pct": self._unstuck_close_pct,
            "unstuck_ema_dist": self._unstuck_ema_dist,
            "unstuck_loss_allowance_pct": self._unstuck_loss_allowance_pct,
            "unstuck_threshold": self._unstuck_threshold
        }

    def __repr__(self):
        return str(self._long)
    
    @property
    def long(self): return self._long
    @long.setter
    def long(self, new_long):
        if "close_grid_markup_range" in new_long:
            self.close_grid_markup_range = new_long["close_grid_markup_range"]
        if "close_grid_min_markup" in new_long:
            self.close_grid_min_markup = new_long["close_grid_min_markup"]
        if "close_grid_qty_pct" in new_long:
            self.close_grid_qty_pct = new_long["close_grid_qty_pct"]
        if "close_trailing_grid_ratio" in new_long:
            self.close_trailing_grid_ratio = new_long["close_trailing_grid_ratio"]
        if "close_trailing_qty_pct" in new_long:
            self.close_trailing_qty_pct = new_long["close_trailing_qty_pct"]
        if "close_trailing_retracement_pct" in new_long:
            self.close_trailing_retracement_pct = new_long["close_trailing_retracement_pct"]
        if "close_trailing_threshold_pct" in new_long:
            self.close_trailing_threshold_pct = new_long["close_trailing_threshold_pct"]
        if "ema_span_0" in new_long:
            self.ema_span_0 = new_long["ema_span_0"]
        if "ema_span_1" in new_long:
            self.ema_span_1 = new_long["ema_span_1"]
        if "entry_grid_double_down_factor" in new_long:
            self.entry_grid_double_down_factor = new_long["entry_grid_double_down_factor"]
        if "entry_grid_spacing_pct" in new_long:
            self.entry_grid_spacing_pct = new_long["entry_grid_spacing_pct"]
        if "entry_grid_spacing_weight" in new_long:
            self.entry_grid_spacing_weight = new_long["entry_grid_spacing_weight"]
        if "entry_initial_ema_dist" in new_long:
            self.entry_initial_ema_dist = new_long["entry_initial_ema_dist"]
        if "entry_initial_qty_pct" in new_long:
            self.entry_initial_qty_pct = new_long["entry_initial_qty_pct"]
        if "entry_trailing_grid_ratio" in new_long:
            self.entry_trailing_grid_ratio = new_long["entry_trailing_grid_ratio"]
        if "entry_trailing_retracement_pct" in new_long:
            self.entry_trailing_retracement_pct = new_long["entry_trailing_retracement_pct"]
        if "entry_trailing_threshold_pct" in new_long:
            self.entry_trailing_threshold_pct = new_long["entry_trailing_threshold_pct"]
        if "filter_relative_volume_clip_pct" in new_long:
            self.filter_relative_volume_clip_pct = new_long["filter_relative_volume_clip_pct"]
        if "filter_rolling_window" in new_long:
            self.filter_rolling_window = new_long["filter_rolling_window"]
        if "n_positions" in new_long:
            self.n_positions = new_long["n_positions"]
        if "total_wallet_exposure_limit" in new_long:
            self.total_wallet_exposure_limit = new_long["total_wallet_exposure_limit"]
        if "unstuck_close_pct" in new_long:
            self.unstuck_close_pct = new_long["unstuck_close_pct"]
        if "unstuck_ema_dist" in new_long:
            self.unstuck_ema_dist = new_long["unstuck_ema_dist"]
        if "unstuck_loss_allowance_pct" in new_long:
            self.unstuck_loss_allowance_pct = new_long["unstuck_loss_allowance_pct"]
        if "unstuck_threshold" in new_long:
            self.unstuck_threshold = new_long["unstuck_threshold"]

    @property
    def close_grid_markup_range(self): return self._close_grid_markup_range
    @property
    def close_grid_min_markup(self): return self._close_grid_min_markup
    @property
    def close_grid_qty_pct(self): return self._close_grid_qty_pct
    @property
    def close_trailing_grid_ratio(self): return self._close_trailing_grid_ratio
    @property
    def close_trailing_qty_pct(self): return self._close_trailing_qty_pct
    @property
    def close_trailing_retracement_pct(self): return self._close_trailing_retracement_pct
    @property
    def close_trailing_threshold_pct(self): return self._close_trailing_threshold_pct
    @property
    def ema_span_0(self): return self._ema_span_0
    @property
    def ema_span_1(self): return self._ema_span_1
    @property
    def entry_grid_double_down_factor(self): return self._entry_grid_double_down_factor
    @property
    def entry_grid_spacing_pct(self): return self._entry_grid_spacing_pct
    @property
    def entry_grid_spacing_weight(self): return self._entry_grid_spacing_weight
    @property
    def entry_initial_ema_dist(self): return self._entry_initial_ema_dist
    @property
    def entry_initial_qty_pct(self): return self._entry_initial_qty_pct
    @property
    def entry_trailing_grid_ratio(self): return self._entry_trailing_grid_ratio
    @property
    def entry_trailing_retracement_pct(self): return self._entry_trailing_retracement_pct
    @property
    def entry_trailing_threshold_pct(self): return self._entry_trailing_threshold_pct
    @property
    def filter_relative_volume_clip_pct(self): return self._filter_relative_volume_clip_pct
    @property
    def filter_rolling_window(self): return self._filter_rolling_window
    @property
    def n_positions(self): return self._n_positions
    @property
    def total_wallet_exposure_limit(self): return self._total_wallet_exposure_limit
    @property
    def unstuck_close_pct(self): return self._unstuck_close_pct
    @property
    def unstuck_ema_dist(self): return self._unstuck_ema_dist
    @property
    def unstuck_loss_allowance_pct(self): return self._unstuck_loss_allowance_pct
    @property
    def unstuck_threshold(self): return self._unstuck_threshold

    @close_grid_markup_range.setter
    def close_grid_markup_range(self, new_close_grid_markup_range):
        self._close_grid_markup_range = new_close_grid_markup_range
        self._long["close_grid_markup_range"] = self._close_grid_markup_range
    @close_grid_min_markup.setter
    def close_grid_min_markup(self, new_close_grid_min_markup):
        self._close_grid_min_markup = new_close_grid_min_markup
        self._long["close_grid_min_markup"] = self._close_grid_min_markup
    @close_grid_qty_pct.setter
    def close_grid_qty_pct(self, new_close_grid_qty_pct):
        self._close_grid_qty_pct = new_close_grid_qty_pct
        self._long["close_grid_qty_pct"] = self._close_grid_qty_pct
    @close_trailing_grid_ratio.setter
    def close_trailing_grid_ratio(self, new_close_trailing_grid_ratio):
        self._close_trailing_grid_ratio = new_close_trailing_grid_ratio
        self._long["close_trailing_grid_ratio"] = self._close_trailing_grid_ratio
    @close_trailing_qty_pct.setter
    def close_trailing_qty_pct(self, new_close_trailing_qty_pct):
        self._close_trailing_qty_pct = new_close_trailing_qty_pct
        self._long["close_trailing_qty_pct"] = self._close_trailing_qty_pct
    @close_trailing_retracement_pct.setter
    def close_trailing_retracement_pct(self, new_close_trailing_retracement_pct):
        self._close_trailing_retracement_pct = new_close_trailing_retracement_pct
        self._long["close_trailing_retracement_pct"] = self._close_trailing_retracement_pct
    @close_trailing_threshold_pct.setter
    def close_trailing_threshold_pct(self, new_close_trailing_threshold_pct):
        self._close_trailing_threshold_pct = new_close_trailing_threshold_pct
        self._long["close_trailing_threshold_pct"] = self._close_trailing_threshold_pct
    @ema_span_0.setter
    def ema_span_0(self, new_ema_span_0):
        self._ema_span_0 = new_ema_span_0
        self._long["ema_span_0"] = self._ema_span_0
    @ema_span_1.setter
    def ema_span_1(self, new_ema_span_1):
        self._ema_span_1 = new_ema_span_1
        self._long["ema_span_1"] = self._ema_span_1
    @entry_grid_double_down_factor.setter
    def entry_grid_double_down_factor(self, new_entry_grid_double_down_factor):
        self._entry_grid_double_down_factor = new_entry_grid_double_down_factor
        self._long["entry_grid_double_down_factor"] = self._entry_grid_double_down_factor
    @entry_grid_spacing_pct.setter
    def entry_grid_spacing_pct(self, new_entry_grid_spacing_pct):
        self._entry_grid_spacing_pct = new_entry_grid_spacing_pct
        self._long["entry_grid_spacing_pct"] = self._entry_grid_spacing_pct
    @entry_grid_spacing_weight.setter
    def entry_grid_spacing_weight(self, new_entry_grid_spacing_weight):
        self._entry_grid_spacing_weight = new_entry_grid_spacing_weight
        self._long["entry_grid_spacing_weight"] = self._entry_grid_spacing_weight
    @entry_initial_ema_dist.setter
    def entry_initial_ema_dist(self, new_entry_initial_ema_dist):
        self._entry_initial_ema_dist = new_entry_initial_ema_dist
        self._long["entry_initial_ema_dist"] = self._entry_initial_ema_dist
    @entry_initial_qty_pct.setter
    def entry_initial_qty_pct(self, new_entry_initial_qty_pct):
        self._entry_initial_qty_pct = new_entry_initial_qty_pct
        self._long["entry_initial_qty_pct"] = self._entry_initial_qty_pct
    @entry_trailing_grid_ratio.setter
    def entry_trailing_grid_ratio(self, new_entry_trailing_grid_ratio):
        self._entry_trailing_grid_ratio = new_entry_trailing_grid_ratio
        self._long["entry_trailing_grid_ratio"] = self._entry_trailing_grid_ratio
    @entry_trailing_retracement_pct.setter
    def entry_trailing_retracement_pct(self, new_entry_trailing_retracement_pct):
        self._entry_trailing_retracement_pct = new_entry_trailing_retracement_pct
        self._long["entry_trailing_retracement_pct"] = self._entry_trailing_retracement_pct
    @entry_trailing_threshold_pct.setter
    def entry_trailing_threshold_pct(self, new_entry_trailing_threshold_pct):
        self._entry_trailing_threshold_pct = new_entry_trailing_threshold_pct
        self._long["entry_trailing_threshold_pct"] = self._entry_trailing_threshold_pct
    @filter_relative_volume_clip_pct.setter
    def filter_relative_volume_clip_pct(self, new_filter_relative_volume_clip_pct):
        self._filter_relative_volume_clip_pct = new_filter_relative_volume_clip_pct
        self._long["filter_relative_volume_clip_pct"] = self._filter_relative_volume_clip_pct
    @filter_rolling_window.setter
    def filter_rolling_window(self, new_filter_rolling_window):
        self._filter_rolling_window = new_filter_rolling_window
        self._long["filter_rolling_window"] = self._filter_rolling_window
    @n_positions.setter
    def n_positions(self, new_n_positions):
        self._n_positions = new_n_positions
        self._long["n_positions"] = self._n_positions
    @total_wallet_exposure_limit.setter
    def total_wallet_exposure_limit(self, new_total_wallet_exposure_limit):
        self._total_wallet_exposure_limit = new_total_wallet_exposure_limit
        self._long["total_wallet_exposure_limit"] = self._total_wallet_exposure_limit
    @unstuck_close_pct.setter
    def unstuck_close_pct(self, new_unstuck_close_pct):
        self._unstuck_close_pct = new_unstuck_close_pct
        self._long["unstuck_close_pct"] = self._unstuck_close_pct
    @unstuck_ema_dist.setter
    def unstuck_ema_dist(self, new_unstuck_ema_dist):
        self._unstuck_ema_dist = new_unstuck_ema_dist
        self._long["unstuck_ema_dist"] = self._unstuck_ema_dist
    @unstuck_loss_allowance_pct.setter
    def unstuck_loss_allowance_pct(self, new_unstuck_loss_allowance_pct):
        self._unstuck_loss_allowance_pct = new_unstuck_loss_allowance_pct
        self._long["unstuck_loss_allowance_pct"] = self._unstuck_loss_allowance_pct
    @unstuck_threshold.setter
    def unstuck_threshold(self, new_unstuck_threshold):
        self._unstuck_threshold = new_unstuck_threshold
        self._long["unstuck_threshold"] = self._unstuck_threshold

class Short:
    def __init__(self):
        self._close_grid_markup_range = 0.028266
        self._close_grid_min_markup = 0.013899
        self._close_grid_qty_pct = 0.63174
        self._close_trailing_grid_ratio = 0.93658
        self._close_trailing_qty_pct = 1
        self._close_trailing_retracement_pct = 0.098179
        self._close_trailing_threshold_pct = -0.059383
        self._ema_span_0 = 794.32
        self._ema_span_1 = 1176.7
        self._entry_grid_double_down_factor = 2.1256
        self._entry_grid_spacing_pct = 0.072906
        self._entry_grid_spacing_weight = 0.98867
        self._entry_initial_ema_dist = -0.060333
        self._entry_initial_qty_pct = 0.066426
        self._entry_trailing_grid_ratio = -0.026647
        self._entry_trailing_retracement_pct = 0.016626
        self._entry_trailing_threshold_pct = 0.052728
        self._filter_relative_volume_clip_pct = 0.50001
        self._filter_rolling_window = 68.072
        self._n_positions = 0.0
        self._total_wallet_exposure_limit = 0.0
        self._unstuck_close_pct = 0.052992
        self._unstuck_ema_dist = -0.0465
        self._unstuck_loss_allowance_pct = 0.045415
        self._unstuck_threshold = 0.92228
        self._short = {
            "close_grid_markup_range": self._close_grid_markup_range,
            "close_grid_min_markup": self._close_grid_min_markup,
            "close_grid_qty_pct": self._close_grid_qty_pct,
            "close_trailing_grid_ratio": self._close_trailing_grid_ratio,
            "close_trailing_qty_pct": self._close_trailing_qty_pct,
            "close_trailing_retracement_pct": self._close_trailing_retracement_pct,
            "close_trailing_threshold_pct": self._close_trailing_threshold_pct,
            "ema_span_0": self._ema_span_0,
            "ema_span_1": self._ema_span_1,
            "entry_grid_double_down_factor": self._entry_grid_double_down_factor,
            "entry_grid_spacing_pct": self._entry_grid_spacing_pct,
            "entry_grid_spacing_weight": self._entry_grid_spacing_weight,
            "entry_initial_ema_dist": self._entry_initial_ema_dist,
            "entry_initial_qty_pct": self._entry_initial_qty_pct,
            "entry_trailing_grid_ratio": self._entry_trailing_grid_ratio,
            "entry_trailing_retracement_pct": self._entry_trailing_retracement_pct,
            "entry_trailing_threshold_pct": self._entry_trailing_threshold_pct,
            "filter_relative_volume_clip_pct": self._filter_relative_volume_clip_pct,
            "filter_rolling_window": self._filter_rolling_window,
            "n_positions": self._n_positions,
            "total_wallet_exposure_limit": self._total_wallet_exposure_limit,
            "unstuck_close_pct": self._unstuck_close_pct,
            "unstuck_ema_dist": self._unstuck_ema_dist,
            "unstuck_loss_allowance_pct": self._unstuck_loss_allowance_pct,
            "unstuck_threshold": self._unstuck_threshold
        }

    def __repr__(self):
        return str(self._short)

    @property
    def short(self): return self._short
    @short.setter
    def short(self, new_short):
        if "close_grid_markup_range" in new_short:
            self.close_grid_markup_range = new_short["close_grid_markup_range"]
        if "close_grid_min_markup" in new_short:
            self.close_grid_min_markup = new_short["close_grid_min_markup"]
        if "close_grid_qty_pct" in new_short:
            self.close_grid_qty_pct = new_short["close_grid_qty_pct"]
        if "close_trailing_grid_ratio" in new_short:
            self.close_trailing_grid_ratio = new_short["close_trailing_grid_ratio"]
        if "close_trailing_qty_pct" in new_short:
            self.close_trailing_qty_pct = new_short["close_trailing_qty_pct"]
        if "close_trailing_retracement_pct" in new_short:
            self.close_trailing_retracement_pct = new_short["close_trailing_retracement_pct"]
        if "close_trailing_threshold_pct" in new_short:
            self.close_trailing_threshold_pct = new_short["close_trailing_threshold_pct"]
        if "ema_span_0" in new_short:
            self.ema_span_0 = new_short["ema_span_0"]
        if "ema_span_1" in new_short:
            self.ema_span_1 = new_short["ema_span_1"]
        if "entry_grid_double_down_factor" in new_short:
            self.entry_grid_double_down_factor = new_short["entry_grid_double_down_factor"]
        if "entry_grid_spacing_pct" in new_short:
            self.entry_grid_spacing_pct = new_short["entry_grid_spacing_pct"]
        if "entry_grid_spacing_weight" in new_short:
            self.entry_grid_spacing_weight = new_short["entry_grid_spacing_weight"]
        if "entry_initial_ema_dist" in new_short:
            self.entry_initial_ema_dist = new_short["entry_initial_ema_dist"]
        if "entry_initial_qty_pct" in new_short:
            self.entry_initial_qty_pct = new_short["entry_initial_qty_pct"]
        if "entry_trailing_grid_ratio" in new_short:
            self.entry_trailing_grid_ratio = new_short["entry_trailing_grid_ratio"]
        if "entry_trailing_retracement_pct" in new_short:
            self.entry_trailing_retracement_pct = new_short["entry_trailing_retracement_pct"]
        if "entry_trailing_threshold_pct" in new_short:
            self.entry_trailing_threshold_pct = new_short["entry_trailing_threshold_pct"]
        if "filter_relative_volume_clip_pct" in new_short:
            self.filter_relative_volume_clip_pct = new_short["filter_relative_volume_clip_pct"]
        if "filter_rolling_window" in new_short:
            self.filter_rolling_window = new_short["filter_rolling_window"]
        if "n_positions" in new_short:
            self.n_positions = new_short["n_positions"]
        if "total_wallet_exposure_limit" in new_short:
            self.total_wallet_exposure_limit = new_short["total_wallet_exposure_limit"]
        if "unstuck_close_pct" in new_short:
            self.unstuck_close_pct = new_short["unstuck_close_pct"]
        if "unstuck_ema_dist" in new_short:
            self.unstuck_ema_dist = new_short["unstuck_ema_dist"]
        if "unstuck_loss_allowance_pct" in new_short:
            self.unstuck_loss_allowance_pct = new_short["unstuck_loss_allowance_pct"]
        if "unstuck_threshold" in new_short:
            self.unstuck_threshold = new_short["unstuck_threshold"]

    @property
    def close_grid_markup_range(self): return self._close_grid_markup_range
    @property
    def close_grid_min_markup(self): return self._close_grid_min_markup
    @property
    def close_grid_qty_pct(self): return self._close_grid_qty_pct
    @property
    def close_trailing_grid_ratio(self): return self._close_trailing_grid_ratio
    @property
    def close_trailing_qty_pct(self): return self._close_trailing_qty_pct
    @property
    def close_trailing_retracement_pct(self): return self._close_trailing_retracement_pct
    @property
    def close_trailing_threshold_pct(self): return self._close_trailing_threshold_pct
    @property
    def ema_span_0(self): return self._ema_span_0
    @property
    def ema_span_1(self): return self._ema_span_1
    @property
    def entry_grid_double_down_factor(self): return self._entry_grid_double_down_factor
    @property
    def entry_grid_spacing_pct(self): return self._entry_grid_spacing_pct
    @property
    def entry_grid_spacing_weight(self): return self._entry_grid_spacing_weight
    @property
    def entry_initial_ema_dist(self): return self._entry_initial_ema_dist
    @property
    def entry_initial_qty_pct(self): return self._entry_initial_qty_pct
    @property
    def entry_trailing_grid_ratio(self): return self._entry_trailing_grid_ratio
    @property
    def entry_trailing_retracement_pct(self): return self._entry_trailing_retracement_pct
    @property
    def entry_trailing_threshold_pct(self): return self._entry_trailing_threshold_pct
    @property
    def filter_relative_volume_clip_pct(self): return self._filter_relative_volume_clip_pct
    @property
    def filter_rolling_window(self): return self._filter_rolling_window
    @property
    def n_positions(self): return self._n_positions
    @property
    def total_wallet_exposure_limit(self): return self._total_wallet_exposure_limit
    @property
    def unstuck_close_pct(self): return self._unstuck_close_pct
    @property
    def unstuck_ema_dist(self): return self._unstuck_ema_dist
    @property
    def unstuck_loss_allowance_pct(self): return self._unstuck_loss_allowance_pct
    @property
    def unstuck_threshold(self): return self._unstuck_threshold

    @close_grid_markup_range.setter
    def close_grid_markup_range(self, new_close_grid_markup_range):
        self._close_grid_markup_range = new_close_grid_markup_range
        self._short["close_grid_markup_range"] = self._close_grid_markup_range
    @close_grid_min_markup.setter
    def close_grid_min_markup(self, new_close_grid_min_markup):
        self._close_grid_min_markup = new_close_grid_min_markup
        self._short["close_grid_min_markup"] = self._close_grid_min_markup
    @close_grid_qty_pct.setter
    def close_grid_qty_pct(self, new_close_grid_qty_pct):
        self._close_grid_qty_pct = new_close_grid_qty_pct
        self._short["close_grid_qty_pct"] = self._close_grid_qty_pct
    @close_trailing_grid_ratio.setter
    def close_trailing_grid_ratio(self, new_close_trailing_grid_ratio):
        self._close_trailing_grid_ratio = new_close_trailing_grid_ratio
        self._short["close_trailing_grid_ratio"] = self._close_trailing_grid_ratio
    @close_trailing_qty_pct.setter
    def close_trailing_qty_pct(self, new_close_trailing_qty_pct):
        self._close_trailing_qty_pct = new_close_trailing_qty_pct
        self._short["close_trailing_qty_pct"] = self._close_trailing_qty_pct
    @close_trailing_retracement_pct.setter
    def close_trailing_retracement_pct(self, new_close_trailing_retracement_pct):
        self._close_trailing_retracement_pct = new_close_trailing_retracement_pct
        self._short["close_trailing_retracement_pct"] = self._close_trailing_retracement_pct
    @close_trailing_threshold_pct.setter
    def close_trailing_threshold_pct(self, new_close_trailing_threshold_pct):
        self._close_trailing_threshold_pct = new_close_trailing_threshold_pct
        self._short["close_trailing_threshold_pct"] = self._close_trailing_threshold_pct
    @ema_span_0.setter
    def ema_span_0(self, new_ema_span_0):
        self._ema_span_0 = new_ema_span_0
        self._short["ema_span_0"] = self._ema_span_0
    @ema_span_1.setter
    def ema_span_1(self, new_ema_span_1):
        self._ema_span_1 = new_ema_span_1
        self._short["ema_span_1"] = self._ema_span_1
    @entry_grid_double_down_factor.setter
    def entry_grid_double_down_factor(self, new_entry_grid_double_down_factor):
        self._entry_grid_double_down_factor = new_entry_grid_double_down_factor
        self._short["entry_grid_double_down_factor"] = self._entry_grid_double_down_factor
    @entry_grid_spacing_pct.setter
    def entry_grid_spacing_pct(self, new_entry_grid_spacing_pct):
        self._entry_grid_spacing_pct = new_entry_grid_spacing_pct
        self._short["entry_grid_spacing_pct"] = self._entry_grid_spacing_pct
    @entry_grid_spacing_weight.setter
    def entry_grid_spacing_weight(self, new_entry_grid_spacing_weight):
        self._entry_grid_spacing_weight = new_entry_grid_spacing_weight
        self._short["entry_grid_spacing_weight"] = self._entry_grid_spacing_weight
    @entry_initial_ema_dist.setter
    def entry_initial_ema_dist(self, new_entry_initial_ema_dist):
        self._entry_initial_ema_dist = new_entry_initial_ema_dist
        self._short["entry_initial_ema_dist"] = self._entry_initial_ema_dist
    @entry_initial_qty_pct.setter
    def entry_initial_qty_pct(self, new_entry_initial_qty_pct):
        self._entry_initial_qty_pct = new_entry_initial_qty_pct
        self._short["entry_initial_qty_pct"] = self._entry_initial_qty_pct
    @entry_trailing_grid_ratio.setter
    def entry_trailing_grid_ratio(self, new_entry_trailing_grid_ratio):
        self._entry_trailing_grid_ratio = new_entry_trailing_grid_ratio
        self._short["entry_trailing_grid_ratio"] = self._entry_trailing_grid_ratio
    @entry_trailing_retracement_pct.setter
    def entry_trailing_retracement_pct(self, new_entry_trailing_retracement_pct):
        self._entry_trailing_retracement_pct = new_entry_trailing_retracement_pct
        self._short["entry_trailing_retracement_pct"] = self._entry_trailing_retracement_pct
    @entry_trailing_threshold_pct.setter
    def entry_trailing_threshold_pct(self, new_entry_trailing_threshold_pct):
        self._entry_trailing_threshold_pct = new_entry_trailing_threshold_pct
        self._short["entry_trailing_threshold_pct"] = self._entry_trailing_threshold_pct
    @filter_relative_volume_clip_pct.setter
    def filter_relative_volume_clip_pct(self, new_filter_relative_volume_clip_pct):
        self._filter_relative_volume_clip_pct = new_filter_relative_volume_clip_pct
        self._short["filter_relative_volume_clip_pct"] = self._filter_relative_volume_clip_pct
    @filter_rolling_window.setter
    def filter_rolling_window(self, new_filter_rolling_window):
        self._filter_rolling_window = new_filter_rolling_window
        self._short["filter_rolling_window"] = self._filter_rolling_window
    @n_positions.setter
    def n_positions(self, new_n_positions):
        self._n_positions = new_n_positions
        self._short["n_positions"] = self._n_positions
    @total_wallet_exposure_limit.setter
    def total_wallet_exposure_limit(self, new_total_wallet_exposure_limit):
        self._total_wallet_exposure_limit = new_total_wallet_exposure_limit
        self._short["total_wallet_exposure_limit"] = self._total_wallet_exposure_limit
    @unstuck_close_pct.setter
    def unstuck_close_pct(self, new_unstuck_close_pct):
        self._unstuck_close_pct = new_unstuck_close_pct
        self._short["unstuck_close_pct"] = self._unstuck_close_pct
    @unstuck_ema_dist.setter
    def unstuck_ema_dist(self, new_unstuck_ema_dist):
        self._unstuck_ema_dist = new_unstuck_ema_dist
        self._short["unstuck_ema_dist"] = self._unstuck_ema_dist
    @unstuck_loss_allowance_pct.setter
    def unstuck_loss_allowance_pct(self, new_unstuck_loss_allowance_pct):
        self._unstuck_loss_allowance_pct = new_unstuck_loss_allowance_pct
        self._short["unstuck_loss_allowance_pct"] = self._unstuck_loss_allowance_pct
    @unstuck_threshold.setter
    def unstuck_threshold(self, new_unstuck_threshold):
        self._unstuck_threshold = new_unstuck_threshold
        self._short["unstuck_threshold"] = self._unstuck_threshold

class ApprovedCoins:
    def __init__(self):
        self._long = []
        self._short = []
        self._approved_coins = {
            "long": self._long,
            "short": self._short
        }

    def __repr__(self):
        return str(self._approved_coins)
    
    @property
    def approved_coins(self): return self._approved_coins
    @approved_coins.setter
    def approved_coins(self, new_approved_coins):
        if "long" in new_approved_coins:
            self.long = new_approved_coins["long"]
        else:
            self.long = new_approved_coins
        if "short" in new_approved_coins:
            self.short = new_approved_coins["short"]
        else:
            self.short = new_approved_coins
    
    @property
    def long(self): return self._long
    @property
    def short(self): return self._short
    @long.setter
    def long(self, new_long):
        self._long = new_long
        self._approved_coins["long"] = self._long
    @short.setter
    def short(self, new_short):
        self._short = new_short
        self._approved_coins["short"] = self._short

class IgnoredCoins:
    def __init__(self):
        self._long = []
        self._short = []
        self._ignored_coins = {
            "long": self._long,
            "short": self._short
        }
    
    def __repr__(self):
        return str(self._ignored_coins)

    @property
    def ignored_coins(self): return self._ignored_coins
    @ignored_coins.setter
    def ignored_coins(self, new_ignored_coins):
        if "long" in new_ignored_coins:
            self.long = new_ignored_coins["long"]
        else:
            self.long = new_ignored_coins
        if "short" in new_ignored_coins:
            self.short = new_ignored_coins["short"]
        else:
            self.short = new_ignored_coins
    
    @property
    def long(self): return self._long
    @property
    def short(self): return self._short
    @long.setter
    def long(self, new_long):
        self._long = new_long
        self._ignored_coins["long"] = self._long
    @short.setter
    def short(self, new_short):
        self._short = new_short
        self._ignored_coins["short"] = self._short

class Live:
    def __init__(self):
        self._approved_coins = ApprovedCoins()
        self._auto_gs = True
        self._coin_flags = {}
        self._empty_means_all_approved = False
        self._execution_delay_seconds = 2.0
        self._filter_by_min_effective_cost = True
        self._forced_mode_long = ""
        self._forced_mode_short = ""
        self._ignored_coins = IgnoredCoins()
        self._leverage = 10.0
        self._max_n_cancellations_per_batch = 5
        self._max_n_creations_per_batch = 3
        self._max_n_restarts_per_day = 10
        self._minimum_coin_age_days = 30.0
        self._ohlcvs_1m_rolling_window_days = 4.0
        self._ohlcvs_1m_update_after_minutes = 10.0
        self._pnls_max_lookback_days = 30.0
        self._price_distance_threshold = 0.002
        self._time_in_force = "good_till_cancelled"
        self._user = "bybit_01"

        self._live = {
            "approved_coins": self._approved_coins._approved_coins,
            "auto_gs": self._auto_gs,
            "coin_flags": self._coin_flags,
            "empty_means_all_approved": self._empty_means_all_approved,
            "execution_delay_seconds": self._execution_delay_seconds,
            "filter_by_min_effective_cost": self._filter_by_min_effective_cost,
            "forced_mode_long": self._forced_mode_long,
            "forced_mode_short": self._forced_mode_short,
            "ignored_coins": self._ignored_coins._ignored_coins,
            "leverage": self._leverage,
            "max_n_cancellations_per_batch": self._max_n_cancellations_per_batch,
            "max_n_creations_per_batch": self._max_n_creations_per_batch,
            "max_n_restarts_per_day": self._max_n_restarts_per_day,
            "minimum_coin_age_days": self._minimum_coin_age_days,
            "ohlcvs_1m_rolling_window_days": self._ohlcvs_1m_rolling_window_days,
            "ohlcvs_1m_update_after_minutes": self._ohlcvs_1m_update_after_minutes,
            "pnls_max_lookback_days": self._pnls_max_lookback_days,
            "price_distance_threshold": self._price_distance_threshold,
            "time_in_force": self._time_in_force,
            "user": self._user
        }
    
    def __repr__(self):
        return str(self._live)

    @property
    def live(self): return self._live
    @live.setter
    def live(self, new_live):
        if "approved_coins" in new_live:
            self.approved_coins = new_live["approved_coins"]
        if "auto_gs" in new_live:
            self.auto_gs = new_live["auto_gs"]
        if "coin_flags" in new_live:
            self.coin_flags = new_live["coin_flags"]
        if "empty_means_all_approved" in new_live:
            self.empty_means_all_approved = new_live["empty_means_all_approved"]
        if "execution_delay_seconds" in new_live:
            self.execution_delay_seconds = new_live["execution_delay_seconds"]
        if "filter_by_min_effective_cost" in new_live:
            self.filter_by_min_effective_cost = new_live["filter_by_min_effective_cost"]
        if "forced_mode_long" in new_live:
            self.forced_mode_long = new_live["forced_mode_long"]
        if "forced_mode_short" in new_live:
            self.forced_mode_short = new_live["forced_mode_short"]
        if "ignored_coins" in new_live:
            self.ignored_coins = new_live["ignored_coins"]
        if "leverage" in new_live:
            self.leverage = new_live["leverage"]
        if "max_n_cancellations_per_batch" in new_live:
            self.max_n_cancellations_per_batch = new_live["max_n_cancellations_per_batch"]
        if "max_n_creations_per_batch" in new_live:
            self.max_n_creations_per_batch = new_live["max_n_creations_per_batch"]
        if "max_n_restarts_per_day" in new_live:
            self.max_n_restarts_per_day = new_live["max_n_restarts_per_day"]
        if "minimum_coin_age_days" in new_live:
            self.minimum_coin_age_days = new_live["minimum_coin_age_days"]
        if "ohlcvs_1m_rolling_window_days" in new_live:
            self.ohlcvs_1m_rolling_window_days = new_live["ohlcvs_1m_rolling_window_days"]
        if "ohlcvs_1m_update_after_minutes" in new_live:
            self.ohlcvs_1m_update_after_minutes = new_live["ohlcvs_1m_update_after_minutes"]
        if "pnls_max_lookback_days" in new_live:
            self.pnls_max_lookback_days = new_live["pnls_max_lookback_days"]
        if "price_distance_threshold" in new_live:
            self.price_distance_threshold = new_live["price_distance_threshold"]
        if "time_in_force" in new_live:
            self.time_in_force = new_live["time_in_force"]
        if "user" in new_live:
            self.user = new_live["user"]
    
    @property
    def approved_coins(self): return self._approved_coins
    @property
    def auto_gs(self): return self._auto_gs
    @property
    def coin_flags(self): return self._coin_flags
    @property
    def empty_means_all_approved(self): return self._empty_means_all_approved
    @property
    def execution_delay_seconds(self): return self._execution_delay_seconds
    @property
    def filter_by_min_effective_cost(self): return self._filter_by_min_effective_cost
    @property
    def forced_mode_long(self): return self._forced_mode_long
    @property
    def forced_mode_short(self): return self._forced_mode_short
    @property
    def ignored_coins(self): return self._ignored_coins
    @property
    def leverage(self): return self._leverage
    @property
    def max_n_cancellations_per_batch(self): return self._max_n_cancellations_per_batch
    @property
    def max_n_creations_per_batch(self): return self._max_n_creations_per_batch
    @property
    def max_n_restarts_per_day(self): return self._max_n_restarts_per_day
    @property
    def minimum_coin_age_days(self): return self._minimum_coin_age_days
    @property
    def ohlcvs_1m_rolling_window_days(self): return self._ohlcvs_1m_rolling_window_days
    @property
    def ohlcvs_1m_update_after_minutes(self): return self._ohlcvs_1m_update_after_minutes
    @property
    def pnls_max_lookback_days(self): return self._pnls_max_lookback_days
    @property
    def price_distance_threshold(self): return self._price_distance_threshold
    @property
    def time_in_force(self): return self._time_in_force
    @property
    def user(self): return self._user

    @approved_coins.setter
    def approved_coins(self, new_approved_coins):
        self._approved_coins.approved_coins = new_approved_coins
        self._live["approved_coins"] = self._approved_coins.approved_coins
    @auto_gs.setter
    def auto_gs(self, new_auto_gs):
        self._auto_gs = new_auto_gs
        self._live["auto_gs"] = self._auto_gs
    @coin_flags.setter
    def coin_flags(self, new_coin_flags):
        self._coin_flags = new_coin_flags
        self._live["coin_flags"] = self._coin_flags
    @empty_means_all_approved.setter
    def empty_means_all_approved(self, new_empty_means_all_approved):
        self._empty_means_all_approved = new_empty_means_all_approved
        self._live["empty_means_all_approved"] = self._empty_means_all_approved
    @execution_delay_seconds.setter
    def execution_delay_seconds(self, new_execution_delay_seconds):
        self._execution_delay_seconds = new_execution_delay_seconds
        self._live["execution_delay_seconds"] = self._execution_delay_seconds
    @filter_by_min_effective_cost.setter
    def filter_by_min_effective_cost(self, new_filter_by_min_effective_cost):
        self._filter_by_min_effective_cost = new_filter_by_min_effective_cost
        self._live["filter_by_min_effective_cost"] = self._filter_by_min_effective_cost
    @forced_mode_long.setter
    def forced_mode_long(self, new_forced_mode_long):
        self._forced_mode_long = new_forced_mode_long
        self._live["forced_mode_long"] = self._forced_mode_long
    @forced_mode_short.setter
    def forced_mode_short(self, new_forced_mode_short):
        self._forced_mode_short = new_forced_mode_short
        self._live["forced_mode_short"] = self._forced_mode_short
    @ignored_coins.setter
    def ignored_coins(self, new_ignored_coins):
        self._ignored_coins.ignored_coins = new_ignored_coins
        self._live["ignored_coins"] = self._ignored_coins.ignored_coins
    @leverage.setter
    def leverage(self, new_leverage):
        self._leverage = new_leverage
        self._live["leverage"] = self._leverage
    @max_n_cancellations_per_batch.setter
    def max_n_cancellations_per_batch(self, new_max_n_cancellations_per_batch):
        self._max_n_cancellations_per_batch = new_max_n_cancellations_per_batch
        self._live["max_n_cancellations_per_batch"] = self._max_n_cancellations_per_batch
    @max_n_creations_per_batch.setter
    def max_n_creations_per_batch(self, new_max_n_creations_per_batch):
        self._max_n_creations_per_batch = new_max_n_creations_per_batch
        self._live["max_n_creations_per_batch"] = self._max_n_creations_per_batch
    @max_n_restarts_per_day.setter
    def max_n_restarts_per_day(self, new_max_n_restarts_per_day):
        self._max_n_restarts_per_day = new_max_n_restarts_per_day
        self._live["max_n_restarts_per_day"] = self._max_n_restarts_per_day
    @minimum_coin_age_days.setter
    def minimum_coin_age_days(self, new_minimum_coin_age_days):
        self._minimum_coin_age_days = new_minimum_coin_age_days
        self._live["minimum_coin_age_days"] = self._minimum_coin_age_days
    @ohlcvs_1m_rolling_window_days.setter
    def ohlcvs_1m_rolling_window_days(self, new_ohlcvs_1m_rolling_window_days):
        self._ohlcvs_1m_rolling_window_days = new_ohlcvs_1m_rolling_window_days
        self._live["ohlcvs_1m_rolling_window_days"] = self._ohlcvs_1m_rolling_window_days
    @ohlcvs_1m_update_after_minutes.setter
    def ohlcvs_1m_update_after_minutes(self, new_ohlcvs_1m_update_after_minutes):
        self._ohlcvs_1m_update_after_minutes = new_ohlcvs_1m_update_after_minutes
        self._live["ohlcvs_1m_update_after_minutes"] = self._ohlcvs_1m_update_after_minutes
    @pnls_max_lookback_days.setter
    def pnls_max_lookback_days(self, new_pnls_max_lookback_days):
        self._pnls_max_lookback_days = new_pnls_max_lookback_days
        self._live["pnls_max_lookback_days"] = self._pnls_max_lookback_days
    @price_distance_threshold.setter
    def price_distance_threshold(self, new_price_distance_threshold):
        self._price_distance_threshold = new_price_distance_threshold
        self._live["price_distance_threshold"] = self._price_distance_threshold
    @time_in_force.setter
    def time_in_force(self, new_time_in_force):
        self._time_in_force = new_time_in_force
        self._live["time_in_force"] = self._time_in_force
    @user.setter
    def user(self, new_user):
        self._user = new_user
        self._live["user"] = self._user

class Optimize:
    def __init__(self):
        self._bounds = Bounds()
        self._limits = Limits()
        # optimize
        self._crossover_probability = 0.7
        self._iters = 100000
        self._mutation_probability = 0.2
        self._n_cpus = 5
        self._population_size = 500
        # scoring
        self._scoring = ["mdg", "sharpe_ratio"]

        self._optimize = {
            "bounds": self._bounds._bounds,
            "crossover_probability": self._crossover_probability,
            "iters": self._iters,
            "limits": self._limits._limits,
            "mutation_probability": self._mutation_probability,
            "n_cpus": self._n_cpus,
            "population_size": self._population_size,
            "scoring": self._scoring
        }
    
    def __repr__(self):
        return str(self._optimize)

    @property
    def optimize(self): return self._optimize
    @optimize.setter
    def optimize(self, new_optimize):
        if "bounds" in new_optimize:
            self.bounds = new_optimize["bounds"]
        if "crossover_probability" in new_optimize:
            self.crossover_probability = new_optimize["crossover_probability"]
        if "iters" in new_optimize:
            self.iters = new_optimize["iters"]
        if "limits" in new_optimize:
            self.limits = new_optimize["limits"]
        if "mutation_probability" in new_optimize:
            self.mutation_probability = new_optimize["mutation_probability"]
        if "n_cpus" in new_optimize:
            self.n_cpus = new_optimize["n_cpus"]
        if "population_size" in new_optimize:
            self.population_size = new_optimize["population_size"]
        if "scoring" in new_optimize:
            self.scoring = new_optimize["scoring"]
    
    @property
    def bounds(self): return self._bounds
    @property
    def limits(self): return self._limits
    @property
    def crossover_probability(self): return self._crossover_probability
    @property
    def iters(self): return self._iters
    @property
    def mutation_probability(self): return self._mutation_probability
    @property
    def n_cpus(self):
        if self._n_cpus > multiprocessing.cpu_count():
            self.n_cpus = multiprocessing.cpu_count()
        return self._n_cpus
    @property
    def population_size(self): return self._population_size
    @property
    def scoring(self): return self._scoring

    @bounds.setter
    def bounds(self, new_bounds):
        self._bounds.bounds = new_bounds
        self._optimize["bounds"] = self._bounds.bounds
    @limits.setter
    def limits(self, new_limits):
        self._limits.limits = new_limits
        self._optimize["limits"] = self._limits.limits
    @crossover_probability.setter
    def crossover_probability(self, new_crossover_probability):
        self._crossover_probability = new_crossover_probability
        self._optimize["crossover_probability"] = self._crossover_probability
    @iters.setter
    def iters(self, new_iters):
        self._iters = new_iters
        self._optimize["iters"] = self._iters
    @mutation_probability.setter
    def mutation_probability(self, new_mutation_probability):
        self._mutation_probability = new_mutation_probability
        self._optimize["mutation_probability"] = self._mutation_probability
    @n_cpus.setter
    def n_cpus(self, new_n_cpus):
        self._n_cpus = new_n_cpus
        self._optimize["n_cpus"] = self._n_cpus
        if self._n_cpus > multiprocessing.cpu_count():
            self.n_cpus = multiprocessing.cpu_count()
    @population_size.setter
    def population_size(self, new_population_size):
        self._population_size = new_population_size
        self._optimize["population_size"] = self._population_size
    @scoring.setter
    def scoring(self, new_scoring):
        self._scoring = new_scoring
        self._optimize["scoring"] = self._scoring

class Limits:
    def __init__(self):
        self._lower_bound_drawdown_worst = 0.25
        self._lower_bound_equity_balance_diff_mean = 0.01
        self._lower_bound_loss_profit_ratio = 0.6
        self._limits = {
            "lower_bound_drawdown_worst": self._lower_bound_drawdown_worst,
            "lower_bound_equity_balance_diff_mean": self._lower_bound_equity_balance_diff_mean,
            "lower_bound_loss_profit_ratio": self._lower_bound_loss_profit_ratio
        }
    
    def __repr__(self):
        return str(self._limits)
    
    @property
    def limits(self): return self._limits
    @limits.setter
    def limits(self, new_limits):
        if "lower_bound_drawdown_worst" in new_limits:
            self.lower_bound_drawdown_worst = new_limits["lower_bound_drawdown_worst"]
        if "lower_bound_equity_balance_diff_mean" in new_limits:
            self.lower_bound_equity_balance_diff_mean = new_limits["lower_bound_equity_balance_diff_mean"]
        if "lower_bound_loss_profit_ratio" in new_limits:
            self.lower_bound_loss_profit_ratio = new_limits["lower_bound_loss_profit_ratio"]
    
    @property
    def lower_bound_drawdown_worst(self): return self._lower_bound_drawdown_worst
    @property
    def lower_bound_equity_balance_diff_mean(self): return self._lower_bound_equity_balance_diff_mean
    @property
    def lower_bound_loss_profit_ratio(self): return self._lower_bound_loss_profit_ratio

    @lower_bound_drawdown_worst.setter
    def lower_bound_drawdown_worst(self, new_lower_bound_drawdown_worst):
        self._lower_bound_drawdown_worst = new_lower_bound_drawdown_worst
        self._limits["lower_bound_drawdown_worst"] = self._lower_bound_drawdown_worst
    @lower_bound_equity_balance_diff_mean.setter
    def lower_bound_equity_balance_diff_mean(self, new_lower_bound_equity_balance_diff_mean):
        self._lower_bound_equity_balance_diff_mean = new_lower_bound_equity_balance_diff_mean
        self._limits["lower_bound_equity_balance_diff_mean"] = self._lower_bound_equity_balance_diff_mean
    @lower_bound_loss_profit_ratio.setter
    def lower_bound_loss_profit_ratio(self, new_lower_bound_loss_profit_ratio):
        self._lower_bound_loss_profit_ratio = new_lower_bound_loss_profit_ratio
        self._limits["lower_bound_loss_profit_ratio"] = self._lower_bound_loss_profit_ratio

class Bounds:

    CLOSE_GRID_MARKUP_RANGE_MIN = 0.0
    CLOSE_GRID_MARKUP_RANGE_MAX = 1.0
    CLOSE_GRID_MARKUP_RANGE_STEP = 0.01
    CLOSE_GRID_MARKUP_RANGE_ROUND = 2
    CLOSE_GRID_MARKUP_RANGE_FORMAT = f'%.{CLOSE_GRID_MARKUP_RANGE_ROUND}f'

    CLOSE_GRID_MIN_MARKUP_MIN = 0.0
    CLOSE_GRID_MIN_MARKUP_MAX = 1.0
    CLOSE_GRID_MIN_MARKUP_STEP = 0.001
    CLOSE_GRID_MIN_MARKUP_ROUND = 3
    CLOSE_GRID_MIN_MARKUP_FORMAT = f'%.{CLOSE_GRID_MIN_MARKUP_ROUND}f'

    CLOSE_GRID_QTY_PCT_MIN = 0.0
    CLOSE_GRID_QTY_PCT_MAX = 1.0
    CLOSE_GRID_QTY_PCT_STEP = 0.05
    CLOSE_GRID_QTY_PCT_ROUND = 2
    CLOSE_GRID_QTY_PCT_FORMAT = f'%.{CLOSE_GRID_QTY_PCT_ROUND}f'

    CLOSE_TRAILING_GRID_RATIO_MIN = -1.0
    CLOSE_TRAILING_GRID_RATIO_MAX = 1.0
    CLOSE_TRAILING_GRID_RATIO_STEP = 0.01
    CLOSE_TRAILING_GRID_RATIO_ROUND = 2
    CLOSE_TRAILING_GRID_RATIO_FORMAT = f'%.{CLOSE_TRAILING_GRID_RATIO_ROUND}f'

    CLOSE_TRAILING_QTY_PCT_MIN = 0.0
    CLOSE_TRAILING_QTY_PCT_MAX = 1.0
    CLOSE_TRAILING_QTY_PCT_STEP = 0.05
    CLOSE_TRAILING_QTY_PCT_ROUND = 2
    CLOSE_TRAILING_QTY_PCT_FORMAT = f'%.{CLOSE_TRAILING_QTY_PCT_ROUND}f'

    CLOSE_TRAILING_RETRACEMENT_PCT_MIN = 0.0
    CLOSE_TRAILING_RETRACEMENT_PCT_MAX = 1.0
    CLOSE_TRAILING_RETRACEMENT_PCT_STEP = 0.01
    CLOSE_TRAILING_RETRACEMENT_PCT_ROUND = 2
    CLOSE_TRAILING_RETRACEMENT_PCT_FORMAT = f'%.{CLOSE_TRAILING_RETRACEMENT_PCT_ROUND}f'

    CLOSE_TRAILING_THRESHOLD_PCT_MIN = -1.0
    CLOSE_TRAILING_THRESHOLD_PCT_MAX = 1.0
    CLOSE_TRAILING_THRESHOLD_PCT_STEP = 0.01
    CLOSE_TRAILING_THRESHOLD_PCT_ROUND = 2
    CLOSE_TRAILING_THRESHOLD_PCT_FORMAT = f'%.{CLOSE_TRAILING_THRESHOLD_PCT_ROUND}f'

    EMA_SPAN_0_MIN = 1.0
    EMA_SPAN_0_MAX = 10000.0
    EMA_SPAN_0_STEP = 100.0
    EMA_SPAN_0_ROUND = 1
    EMA_SPAN_0_FORMAT = f'%.{EMA_SPAN_0_ROUND}f'

    EMA_SPAN_1_MIN = 1.0
    EMA_SPAN_1_MAX = 10000.0
    EMA_SPAN_1_STEP = 100.0
    EMA_SPAN_1_ROUND = 1
    EMA_SPAN_1_FORMAT = f'%.{EMA_SPAN_1_ROUND}f'

    ENTRY_GRID_DOUBLE_DOWN_FACTOR_MIN = 0.0
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_MAX = 10.0
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_STEP = 0.05
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_ROUND = 2
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_FORMAT = f'%.{ENTRY_GRID_DOUBLE_DOWN_FACTOR_ROUND}f'

    ENTRY_GRID_SPACING_PCT_MIN = 0.0
    ENTRY_GRID_SPACING_PCT_MAX = 1.0
    ENTRY_GRID_SPACING_PCT_STEP = 0.01
    ENTRY_GRID_SPACING_PCT_ROUND = 3
    ENTRY_GRID_SPACING_PCT_FORMAT = f'%.{ENTRY_GRID_SPACING_PCT_ROUND}f'

    ENTRY_GRID_SPACING_WEIGHT_MIN = 0.0
    ENTRY_GRID_SPACING_WEIGHT_MAX = 100.0
    ENTRY_GRID_SPACING_WEIGHT_STEP = 1.0
    ENTRY_GRID_SPACING_WEIGHT_ROUND = 1
    ENTRY_GRID_SPACING_WEIGHT_FORMAT = f'%.{ENTRY_GRID_SPACING_WEIGHT_ROUND}f'

    ENTRY_INITIAL_EMA_DIST_MIN = -1.0
    ENTRY_INITIAL_EMA_DIST_MAX = 1.0
    ENTRY_INITIAL_EMA_DIST_STEP = 0.001
    ENTRY_INITIAL_EMA_DIST_ROUND = 3
    ENTRY_INITIAL_EMA_DIST_FORMAT = f'%.{ENTRY_INITIAL_EMA_DIST_ROUND}f'

    ENTRY_INITIAL_QTY_PCT_MIN = 0.0
    ENTRY_INITIAL_QTY_PCT_MAX = 1.0
    ENTRY_INITIAL_QTY_PCT_STEP = 0.001
    ENTRY_INITIAL_QTY_PCT_ROUND = 3
    ENTRY_INITIAL_QTY_PCT_FORMAT = f'%.{ENTRY_INITIAL_QTY_PCT_ROUND}f'

    ENTRY_TRAILING_GRID_RATIO_MIN = -1.0
    ENTRY_TRAILING_GRID_RATIO_MAX = 1.0
    ENTRY_TRAILING_GRID_RATIO_STEP = 0.01
    ENTRY_TRAILING_GRID_RATIO_ROUND = 2
    ENTRY_TRAILING_GRID_RATIO_FORMAT = f'%.{ENTRY_TRAILING_GRID_RATIO_ROUND}f'

    ENTRY_TRAILING_RETRACEMENT_PCT_MIN = 0.0
    ENTRY_TRAILING_RETRACEMENT_PCT_MAX = 1.0
    ENTRY_TRAILING_RETRACEMENT_PCT_STEP = 0.01
    ENTRY_TRAILING_RETRACEMENT_PCT_ROUND = 2
    ENTRY_TRAILING_RETRACEMENT_PCT_FORMAT = f'%.{ENTRY_TRAILING_RETRACEMENT_PCT_ROUND}f'

    ENTRY_TRAILING_THRESHOLD_PCT_MIN = -1.0
    ENTRY_TRAILING_THRESHOLD_PCT_MAX = 1.0
    ENTRY_TRAILING_THRESHOLD_PCT_STEP = 0.01
    ENTRY_TRAILING_THRESHOLD_PCT_ROUND = 2
    ENTRY_TRAILING_THRESHOLD_PCT_FORMAT = f'%.{ENTRY_TRAILING_THRESHOLD_PCT_ROUND}f'

    FILTER_RELATIVE_VOLUME_CLIP_PCT_MIN = 0.0
    FILTER_RELATIVE_VOLUME_CLIP_PCT_MAX = 1.0
    FILTER_RELATIVE_VOLUME_CLIP_PCT_STEP = 0.01
    FILTER_RELATIVE_VOLUME_CLIP_PCT_ROUND = 2
    FILTER_RELATIVE_VOLUME_CLIP_PCT_FORMAT = f'%.{FILTER_RELATIVE_VOLUME_CLIP_PCT_ROUND}f'

    FILTER_ROLLING_WINDOW_MIN = 1.0
    FILTER_ROLLING_WINDOW_MAX = 1440.0
    FILTER_ROLLING_WINDOW_STEP = 1.0
    FILTER_ROLLING_WINDOW_ROUND = 0
    FILTER_ROLLING_WINDOW_FORMAT = f'%.{FILTER_ROLLING_WINDOW_ROUND}f'

    N_POSITIONS_MIN = 0.0
    N_POSITIONS_MAX = 100.0
    N_POSITIONS_STEP = 0.0
    N_POSITIONS_ROUND = 0
    N_POSITIONS_FORMAT = f'%.{N_POSITIONS_ROUND}f'

    TOTAL_WALLET_EXPOSURE_LIMIT_MIN = 0.0
    TOTAL_WALLET_EXPOSURE_LIMIT_MAX = 100.0
    TOTAL_WALLET_EXPOSURE_LIMIT_STEP = 0.1
    TOTAL_WALLET_EXPOSURE_LIMIT_ROUND = 1
    TOTAL_WALLET_EXPOSURE_LIMIT_FORMAT = f'%.{TOTAL_WALLET_EXPOSURE_LIMIT_ROUND}f'

    UNSTUCK_CLOSE_PCT_MIN = 0.0
    UNSTUCK_CLOSE_PCT_MAX = 1.0
    UNSTUCK_CLOSE_PCT_STEP = 0.001
    UNSTUCK_CLOSE_PCT_ROUND = 3
    UNSTUCK_CLOSE_PCT_FORMAT = f'%.{UNSTUCK_CLOSE_PCT_ROUND}f'

    UNSTUCK_EMA_DIST_MIN = -1.0
    UNSTUCK_EMA_DIST_MAX = 1.0
    UNSTUCK_EMA_DIST_STEP = 0.001
    UNSTUCK_EMA_DIST_ROUND = 3
    UNSTUCK_EMA_DIST_FORMAT = f'%.{UNSTUCK_EMA_DIST_ROUND}f'

    UNSTUCK_LOSS_ALLOWANCE_PCT_MIN = 0.0
    UNSTUCK_LOSS_ALLOWANCE_PCT_MAX = 1.0
    UNSTUCK_LOSS_ALLOWANCE_PCT_STEP = 0.001
    UNSTUCK_LOSS_ALLOWANCE_PCT_ROUND = 3
    UNSTUCK_LOSS_ALLOWANCE_PCT_FORMAT = f'%.{UNSTUCK_LOSS_ALLOWANCE_PCT_ROUND}f'

    UNSTUCK_THRESHOLD_MIN = 0.0
    UNSTUCK_THRESHOLD_MAX = 1.0
    UNSTUCK_THRESHOLD_STEP = 0.01
    UNSTUCK_THRESHOLD_ROUND = 2
    UNSTUCK_THRESHOLD_FORMAT = f'%.{UNSTUCK_THRESHOLD_ROUND}f'

    def __init__(self):
        # bounds long
        self._long_close_grid_markup_range_0 = 0.0
        self._long_close_grid_markup_range_1 = 0.03
        self._long_close_grid_min_markup_0 = 0.001
        self._long_close_grid_min_markup_1 = 0.03
        self._long_close_grid_qty_pct_0 = 0.05
        self._long_close_grid_qty_pct_1 = 1.0
        self._long_close_trailing_grid_ratio_0 = 0.0
        self._long_close_trailing_grid_ratio_1 = 1.0
        self._long_close_trailing_qty_pct_0 = 0.05
        self._long_close_trailing_qty_pct_1 = 1.0
        self._long_close_trailing_retracement_pct_0 = 0.0
        self._long_close_trailing_retracement_pct_1 = 0.1
        self._long_close_trailing_threshold_pct_0 = -0.1
        self._long_close_trailing_threshold_pct_1 = 0.1
        self._long_ema_span_0_0 = 200.0
        self._long_ema_span_0_1 = 1440.0
        self._long_ema_span_1_0 = 200.0
        self._long_ema_span_1_1 = 1440.0
        self._long_entry_grid_double_down_factor_0 = 0.1
        self._long_entry_grid_double_down_factor_1 = 3.0
        self._long_entry_grid_spacing_pct_0 = 0.001
        self._long_entry_grid_spacing_pct_1 = 0.12
        self._long_entry_grid_spacing_weight_0 = 0.0
        self._long_entry_grid_spacing_weight_1 = 10.0
        self._long_entry_initial_ema_dist_0 = -0.1
        self._long_entry_initial_ema_dist_1 = 0.003
        self._long_entry_initial_qty_pct_0 = 0.005
        self._long_entry_initial_qty_pct_1 = 0.1
        self._long_entry_trailing_grid_ratio_0 = -1.0
        self._long_entry_trailing_grid_ratio_1 = 1.0
        self._long_entry_trailing_retracement_pct_0 = 0.0
        self._long_entry_trailing_retracement_pct_1 = 0.1
        self._long_entry_trailing_threshold_pct_0 = -0.1
        self._long_entry_trailing_threshold_pct_1 = 0.1
        self._long_filter_relative_volume_clip_pct_0 = 0.0
        self._long_filter_relative_volume_clip_pct_1 = 1.0
        self._long_filter_rolling_window_0 = 10.0
        self._long_filter_rolling_window_1 = 360.0
        self._long_n_positions_0 = 1.0
        self._long_n_positions_1 = 20.0
        self._long_total_wallet_exposure_limit_0 = 0.0
        self._long_total_wallet_exposure_limit_1 = 5.0
        self._long_unstuck_close_pct_0 = 0.001
        self._long_unstuck_close_pct_1 = 0.1
        self._long_unstuck_ema_dist_0 = -0.1
        self._long_unstuck_ema_dist_1 = 0.01
        self._long_unstuck_loss_allowance_pct_0 = 0.0
        self._long_unstuck_loss_allowance_pct_1 = 0.05
        self._long_unstuck_threshold_0 = 0.4
        self._long_unstuck_threshold_1 = 0.95
        # bounds short
        self._short_close_grid_markup_range_0 = 0.0
        self._short_close_grid_markup_range_1 = 0.03
        self._short_close_grid_min_markup_0 = 0.001
        self._short_close_grid_min_markup_1 = 0.03
        self._short_close_grid_qty_pct_0 = 0.05
        self._short_close_grid_qty_pct_1 = 1.0
        self._short_close_trailing_grid_ratio_0 = -1.0
        self._short_close_trailing_grid_ratio_1 = 1.0
        self._short_close_trailing_qty_pct_0 = 0.05
        self._short_close_trailing_qty_pct_1 = 1.0
        self._short_close_trailing_retracement_pct_0 = 0.0
        self._short_close_trailing_retracement_pct_1 = 0.1
        self._short_close_trailing_threshold_pct_0 = -0.1
        self._short_close_trailing_threshold_pct_1 = 0.1
        self._short_ema_span_0_0 = 200.0
        self._short_ema_span_0_1 = 1440.0
        self._short_ema_span_1_0 = 200.0
        self._short_ema_span_1_1 = 1440.0
        self._short_entry_grid_double_down_factor_0 = 0.1
        self._short_entry_grid_double_down_factor_1 = 3.0
        self._short_entry_grid_spacing_pct_0 = 0.001
        self._short_entry_grid_spacing_pct_1 = 0.12
        self._short_entry_grid_spacing_weight_0 = 0.0
        self._short_entry_grid_spacing_weight_1 = 10.0
        self._short_entry_initial_ema_dist_0 = -0.1
        self._short_entry_initial_ema_dist_1 = 0.003
        self._short_entry_initial_qty_pct_0 = 0.005
        self._short_entry_initial_qty_pct_1 = 0.1
        self._short_entry_trailing_grid_ratio_0 = -1.0
        self._short_entry_trailing_grid_ratio_1 = 1.0
        self._short_entry_trailing_retracement_pct_0 = 0.0
        self._short_entry_trailing_retracement_pct_1 = 0.1
        self._short_entry_trailing_threshold_pct_0 = -0.1
        self._short_entry_trailing_threshold_pct_1 = 0.1
        self._short_filter_relative_volume_clip_pct_0 = 0.0
        self._short_filter_relative_volume_clip_pct_1 = 1.0
        self._short_filter_rolling_window_0 = 10.0
        self._short_filter_rolling_window_1 = 360.0
        self._short_n_positions_0 = 1.0
        self._short_n_positions_1 = 20.0
        self._short_total_wallet_exposure_limit_0 = 0.0
        self._short_total_wallet_exposure_limit_1 = 5.0
        self._short_unstuck_close_pct_0 = 0.001
        self._short_unstuck_close_pct_1 = 0.1
        self._short_unstuck_ema_dist_0 = -0.1
        self._short_unstuck_ema_dist_1 = 0.01
        self._short_unstuck_loss_allowance_pct_0 = 0.0
        self._short_unstuck_loss_allowance_pct_1 = 0.05
        self._short_unstuck_threshold_0 = 0.4
        self._short_unstuck_threshold_1 = 0.95
        self._bounds = {
                "long_close_grid_markup_range": [self._long_close_grid_markup_range_0, self._long_close_grid_markup_range_1],
                "long_close_grid_min_markup": [self._long_close_grid_min_markup_0, self._long_close_grid_min_markup_1],
                "long_close_grid_qty_pct": [self._long_close_grid_qty_pct_0, self._long_close_grid_qty_pct_1],
                "long_close_trailing_grid_ratio": [self._long_close_trailing_grid_ratio_0, self._long_close_trailing_grid_ratio_1],
                "long_close_trailing_qty_pct": [self._long_close_trailing_qty_pct_0, self._long_close_trailing_qty_pct_1],
                "long_close_trailing_retracement_pct": [self._long_close_trailing_retracement_pct_0, self._long_close_trailing_retracement_pct_1],
                "long_close_trailing_threshold_pct": [self._long_close_trailing_threshold_pct_0, self._long_close_trailing_threshold_pct_1],
                "long_ema_span_0": [self._long_ema_span_0_0, self._long_ema_span_0_1],
                "long_ema_span_1": [self._long_ema_span_1_0, self._long_ema_span_1_1],
                "long_entry_grid_double_down_factor": [self._long_entry_grid_double_down_factor_0, self._long_entry_grid_double_down_factor_1],
                "long_entry_grid_spacing_pct": [self._long_entry_grid_spacing_pct_0, self._long_entry_grid_spacing_pct_1],
                "long_entry_grid_spacing_weight": [self._long_entry_grid_spacing_weight_0, self._long_entry_grid_spacing_weight_1],
                "long_entry_initial_ema_dist": [self._long_entry_initial_ema_dist_0, self._long_entry_initial_ema_dist_1],
                "long_entry_initial_qty_pct": [self._long_entry_initial_qty_pct_0, self._long_entry_initial_qty_pct_1],
                "long_entry_trailing_grid_ratio": [self._long_entry_trailing_grid_ratio_0, self._long_entry_trailing_grid_ratio_1],
                "long_entry_trailing_retracement_pct": [self._long_entry_trailing_retracement_pct_0, self._long_entry_trailing_retracement_pct_1],
                "long_entry_trailing_threshold_pct": [self._long_entry_trailing_threshold_pct_0, self._long_entry_trailing_threshold_pct_1],
                "long_filter_relative_volume_clip_pct": [self._long_filter_relative_volume_clip_pct_0, self._long_filter_relative_volume_clip_pct_1],
                "long_filter_rolling_window": [self._long_filter_rolling_window_0, self._long_filter_rolling_window_1],
                "long_n_positions": [self._long_n_positions_0, self._long_n_positions_1],
                "long_total_wallet_exposure_limit": [self._long_total_wallet_exposure_limit_0, self._long_total_wallet_exposure_limit_1],
                "long_unstuck_close_pct": [self._long_unstuck_close_pct_0, self._long_unstuck_close_pct_1],
                "long_unstuck_ema_dist": [self._long_unstuck_ema_dist_0, self._long_unstuck_ema_dist_1],
                "long_unstuck_loss_allowance_pct": [self._long_unstuck_loss_allowance_pct_0, self._long_unstuck_loss_allowance_pct_1],
                "long_unstuck_threshold": [self._long_unstuck_threshold_0, self._long_unstuck_threshold_1],
                "short_close_grid_markup_range": [self._short_close_grid_markup_range_0, self._short_close_grid_markup_range_1],
                "short_close_grid_min_markup": [self._short_close_grid_min_markup_0, self._short_close_grid_min_markup_1],
                "short_close_grid_qty_pct": [self._short_close_grid_qty_pct_0, self._short_close_grid_qty_pct_1],
                "short_close_trailing_grid_ratio": [self._short_close_trailing_grid_ratio_0, self._short_close_trailing_grid_ratio_1],
                "short_close_trailing_qty_pct": [self._short_close_trailing_qty_pct_0, self._short_close_trailing_qty_pct_1],
                "short_close_trailing_retracement_pct": [self._short_close_trailing_retracement_pct_0, self._short_close_trailing_retracement_pct_1],
                "short_close_trailing_threshold_pct": [self._short_close_trailing_threshold_pct_0, self._short_close_trailing_threshold_pct_1],
                "short_ema_span_0": [self._short_ema_span_0_0, self._short_ema_span_0_1],
                "short_ema_span_1": [self._short_ema_span_1_0, self._short_ema_span_1_1],
                "short_entry_grid_double_down_factor": [self._short_entry_grid_double_down_factor_0, self._short_entry_grid_double_down_factor_1],
                "short_entry_grid_spacing_pct": [self._short_entry_grid_spacing_pct_0, self._short_entry_grid_spacing_pct_1],
                "short_entry_grid_spacing_weight": [self._short_entry_grid_spacing_weight_0, self._short_entry_grid_spacing_weight_1],
                "short_entry_initial_ema_dist": [self._short_entry_initial_ema_dist_0, self._short_entry_initial_ema_dist_1],
                "short_entry_initial_qty_pct": [self._short_entry_initial_qty_pct_0, self._short_entry_initial_qty_pct_1],
                "short_entry_trailing_grid_ratio": [self._short_entry_trailing_grid_ratio_0, self._short_entry_trailing_grid_ratio_1],
                "short_entry_trailing_retracement_pct": [self._short_entry_trailing_retracement_pct_0, self._short_entry_trailing_retracement_pct_1],
                "short_entry_trailing_threshold_pct": [self._short_entry_trailing_threshold_pct_0, self._short_entry_trailing_threshold_pct_1],
                "short_filter_relative_volume_clip_pct": [self._short_filter_relative_volume_clip_pct_0, self._short_filter_relative_volume_clip_pct_1],
                "short_filter_rolling_window": [self._short_filter_rolling_window_0, self._short_filter_rolling_window_1],
                "short_n_positions": [self._short_n_positions_0, self._short_n_positions_1],
                "short_total_wallet_exposure_limit": [self._short_total_wallet_exposure_limit_0, self._short_total_wallet_exposure_limit_1],
                "short_unstuck_close_pct": [self._short_unstuck_close_pct_0, self._short_unstuck_close_pct_1],
                "short_unstuck_ema_dist": [self._short_unstuck_ema_dist_0, self._short_unstuck_ema_dist_1],
                "short_unstuck_loss_allowance_pct": [self._short_unstuck_loss_allowance_pct_0, self._short_unstuck_loss_allowance_pct_1],
                "short_unstuck_threshold": [self._short_unstuck_threshold_0, self._short_unstuck_threshold_1]
            }
    
    def __repr__(self):
        return str(self._bounds)

    @property
    def bounds(self): return self._bounds
    
    @bounds.setter
    def bounds(self, new_bounds):
        if "long_close_grid_markup_range" in new_bounds:
            self.long_close_grid_markup_range_0 = new_bounds["long_close_grid_markup_range"][0]
            self.long_close_grid_markup_range_1 = new_bounds["long_close_grid_markup_range"][1]
        if "long_close_grid_min_markup" in new_bounds:
            self.long_close_grid_min_markup_0 = new_bounds["long_close_grid_min_markup"][0]
            self.long_close_grid_min_markup_1 = new_bounds["long_close_grid_min_markup"][1]
        if "long_close_grid_qty_pct" in new_bounds:
            self.long_close_grid_qty_pct_0 = new_bounds["long_close_grid_qty_pct"][0]
            self.long_close_grid_qty_pct_1 = new_bounds["long_close_grid_qty_pct"][1]
        if "long_close_trailing_grid_ratio" in new_bounds:
            self.long_close_trailing_grid_ratio_0 = new_bounds["long_close_trailing_grid_ratio"][0]
            self.long_close_trailing_grid_ratio_1 = new_bounds["long_close_trailing_grid_ratio"][1]
        if "long_close_trailing_qty_pct" in new_bounds:
            self.long_close_trailing_qty_pct_0 = new_bounds["long_close_trailing_qty_pct"][0]
            self.long_close_trailing_qty_pct_1 = new_bounds["long_close_trailing_qty_pct"][1]
        if "long_close_trailing_retracement_pct" in new_bounds:
            self.long_close_trailing_retracement_pct_0 = new_bounds["long_close_trailing_retracement_pct"][0]
            self.long_close_trailing_retracement_pct_1 = new_bounds["long_close_trailing_retracement_pct"][1]
        if "long_close_trailing_threshold_pct" in new_bounds:
            self.long_close_trailing_threshold_pct_0 = new_bounds["long_close_trailing_threshold_pct"][0]
            self.long_close_trailing_threshold_pct_1 = new_bounds["long_close_trailing_threshold_pct"][1]
        if "long_ema_span_0" in new_bounds:
            self.long_ema_span_0_0 = new_bounds["long_ema_span_0"][0]
            self.long_ema_span_0_1 = new_bounds["long_ema_span_0"][1]
        if "long_ema_span_1" in new_bounds:
            self.long_ema_span_1_0 = new_bounds["long_ema_span_1"][0]
            self.long_ema_span_1_1 = new_bounds["long_ema_span_1"][1]
        if "long_entry_grid_double_down_factor" in new_bounds:
            self.long_entry_grid_double_down_factor_0 = new_bounds["long_entry_grid_double_down_factor"][0]
            self.long_entry_grid_double_down_factor_1 = new_bounds["long_entry_grid_double_down_factor"][1]
        if "long_entry_grid_spacing_pct" in new_bounds:
            self.long_entry_grid_spacing_pct_0 = new_bounds["long_entry_grid_spacing_pct"][0]
            self.long_entry_grid_spacing_pct_1 = new_bounds["long_entry_grid_spacing_pct"][1]
        if "long_entry_grid_spacing_weight" in new_bounds:
            self.long_entry_grid_spacing_weight_0 = new_bounds["long_entry_grid_spacing_weight"][0]
            self.long_entry_grid_spacing_weight_1 = new_bounds["long_entry_grid_spacing_weight"][1]
        if "long_entry_initial_ema_dist" in new_bounds:
            self.long_entry_initial_ema_dist_0 = new_bounds["long_entry_initial_ema_dist"][0]
            self.long_entry_initial_ema_dist_1 = new_bounds["long_entry_initial_ema_dist"][1]
        if "long_entry_initial_qty_pct" in new_bounds:
            self.long_entry_initial_qty_pct_0 = new_bounds["long_entry_initial_qty_pct"][0]
            self.long_entry_initial_qty_pct_1 = new_bounds["long_entry_initial_qty_pct"][1]
        if "long_entry_trailing_grid_ratio" in new_bounds:
            self.long_entry_trailing_grid_ratio_0 = new_bounds["long_entry_trailing_grid_ratio"][0]
            self.long_entry_trailing_grid_ratio_1 = new_bounds["long_entry_trailing_grid_ratio"][1]
        if "long_entry_trailing_retracement_pct" in new_bounds:
            self.long_entry_trailing_retracement_pct_0 = new_bounds["long_entry_trailing_retracement_pct"][0]
            self.long_entry_trailing_retracement_pct_1 = new_bounds["long_entry_trailing_retracement_pct"][1]
        if "long_entry_trailing_threshold_pct" in new_bounds:
            self.long_entry_trailing_threshold_pct_0 = new_bounds["long_entry_trailing_threshold_pct"][0]
            self.long_entry_trailing_threshold_pct_1 = new_bounds["long_entry_trailing_threshold_pct"][1]
        if "long_filter_relative_volume_clip_pct" in new_bounds:
            self.long_filter_relative_volume_clip_pct_0 = new_bounds["long_filter_relative_volume_clip_pct"][0]
            self.long_filter_relative_volume_clip_pct_1 = new_bounds["long_filter_relative_volume_clip_pct"][1]
        if "long_filter_rolling_window" in new_bounds:
            self.long_filter_rolling_window_0 = new_bounds["long_filter_rolling_window"][0]
            self.long_filter_rolling_window_1 = new_bounds["long_filter_rolling_window"][1]
        if "long_n_positions" in new_bounds:
            self.long_n_positions_0 = new_bounds["long_n_positions"][0]
            self.long_n_positions_1 = new_bounds["long_n_positions"][1]
        if "long_total_wallet_exposure_limit" in new_bounds:
            self.long_total_wallet_exposure_limit_0 = new_bounds["long_total_wallet_exposure_limit"][0]
            self.long_total_wallet_exposure_limit_1 = new_bounds["long_total_wallet_exposure_limit"][1]
        if "long_unstuck_close_pct" in new_bounds:
            self.long_unstuck_close_pct_0 = new_bounds["long_unstuck_close_pct"][0]
            self.long_unstuck_close_pct_1 = new_bounds["long_unstuck_close_pct"][1]
        if "long_unstuck_ema_dist" in new_bounds:
            self.long_unstuck_ema_dist_0 = new_bounds["long_unstuck_ema_dist"][0]
            self.long_unstuck_ema_dist_1 = new_bounds["long_unstuck_ema_dist"][1]
        if "long_unstuck_loss_allowance_pct" in new_bounds:
            self.long_unstuck_loss_allowance_pct_0 = new_bounds["long_unstuck_loss_allowance_pct"][0]
            self.long_unstuck_loss_allowance_pct_1 = new_bounds["long_unstuck_loss_allowance_pct"][1]
        if "long_unstuck_threshold" in new_bounds:
            self.long_unstuck_threshold_0 = new_bounds["long_unstuck_threshold"][0]
            self.long_unstuck_threshold_1 = new_bounds["long_unstuck_threshold"][1]
    
        # Short parameters
        if "short_close_grid_markup_range" in new_bounds:
            self.short_close_grid_markup_range_0 = new_bounds["short_close_grid_markup_range"][0]
            self.short_close_grid_markup_range_1 = new_bounds["short_close_grid_markup_range"][1]
        if "short_close_grid_min_markup" in new_bounds:
            self.short_close_grid_min_markup_0 = new_bounds["short_close_grid_min_markup"][0]
            self.short_close_grid_min_markup_1 = new_bounds["short_close_grid_min_markup"][1]
        if "short_close_grid_qty_pct" in new_bounds:
            self.short_close_grid_qty_pct_0 = new_bounds["short_close_grid_qty_pct"][0]
            self.short_close_grid_qty_pct_1 = new_bounds["short_close_grid_qty_pct"][1]
        if "short_close_trailing_grid_ratio" in new_bounds:
            self.short_close_trailing_grid_ratio_0 = new_bounds["short_close_trailing_grid_ratio"][0]
            self.short_close_trailing_grid_ratio_1 = new_bounds["short_close_trailing_grid_ratio"][1]
        if "short_close_trailing_qty_pct" in new_bounds:
            self.short_close_trailing_qty_pct_0 = new_bounds["short_close_trailing_qty_pct"][0]
            self.short_close_trailing_qty_pct_1 = new_bounds["short_close_trailing_qty_pct"][1]
        if "short_close_trailing_retracement_pct" in new_bounds:
            self.short_close_trailing_retracement_pct_0 = new_bounds["short_close_trailing_retracement_pct"][0]
            self.short_close_trailing_retracement_pct_1 = new_bounds["short_close_trailing_retracement_pct"][1]
        if "short_close_trailing_threshold_pct" in new_bounds:
            self.short_close_trailing_threshold_pct_0 = new_bounds["short_close_trailing_threshold_pct"][0]
            self.short_close_trailing_threshold_pct_1 = new_bounds["short_close_trailing_threshold_pct"][1]
        if "short_ema_span_0" in new_bounds:
            self.short_ema_span_0_0 = new_bounds["short_ema_span_0"][0]
            self.short_ema_span_0_1 = new_bounds["short_ema_span_0"][1]
        if "short_ema_span_1" in new_bounds:
            self.short_ema_span_1_0 = new_bounds["short_ema_span_1"][0]
            self.short_ema_span_1_1 = new_bounds["short_ema_span_1"][1]
        if "short_entry_grid_double_down_factor" in new_bounds:
            self.short_entry_grid_double_down_factor_0 = new_bounds["short_entry_grid_double_down_factor"][0]
            self.short_entry_grid_double_down_factor_1 = new_bounds["short_entry_grid_double_down_factor"][1]
        if "short_entry_grid_spacing_pct" in new_bounds:
            self.short_entry_grid_spacing_pct_0 = new_bounds["short_entry_grid_spacing_pct"][0]
            self.short_entry_grid_spacing_pct_1 = new_bounds["short_entry_grid_spacing_pct"][1]
        if "short_entry_grid_spacing_weight" in new_bounds:
            self.short_entry_grid_spacing_weight_0 = new_bounds["short_entry_grid_spacing_weight"][0]
            self.short_entry_grid_spacing_weight_1 = new_bounds["short_entry_grid_spacing_weight"][1]
        if "short_entry_initial_ema_dist" in new_bounds:
            self.short_entry_initial_ema_dist_0 = new_bounds["short_entry_initial_ema_dist"][0]
            self.short_entry_initial_ema_dist_1 = new_bounds["short_entry_initial_ema_dist"][1]
        if "short_entry_initial_qty_pct" in new_bounds:
            self.short_entry_initial_qty_pct_0 = new_bounds["short_entry_initial_qty_pct"][0]
            self.short_entry_initial_qty_pct_1 = new_bounds["short_entry_initial_qty_pct"][1]
        if "short_entry_trailing_grid_ratio" in new_bounds:
            self.short_entry_trailing_grid_ratio_0 = new_bounds["short_entry_trailing_grid_ratio"][0]
            self.short_entry_trailing_grid_ratio_1 = new_bounds["short_entry_trailing_grid_ratio"][1]
        if "short_entry_trailing_retracement_pct" in new_bounds:
            self.short_entry_trailing_retracement_pct_0 = new_bounds["short_entry_trailing_retracement_pct"][0]
            self.short_entry_trailing_retracement_pct_1 = new_bounds["short_entry_trailing_retracement_pct"][1]
        if "short_entry_trailing_threshold_pct" in new_bounds:
            self.short_entry_trailing_threshold_pct_0 = new_bounds["short_entry_trailing_threshold_pct"][0]
            self.short_entry_trailing_threshold_pct_1 = new_bounds["short_entry_trailing_threshold_pct"][1]
        if "short_filter_relative_volume_clip_pct" in new_bounds:
            self.short_filter_relative_volume_clip_pct_0 = new_bounds["short_filter_relative_volume_clip_pct"][0]
            self.short_filter_relative_volume_clip_pct_1 = new_bounds["short_filter_relative_volume_clip_pct"][1]
        if "short_filter_rolling_window" in new_bounds:
            self.short_filter_rolling_window_0 = new_bounds["short_filter_rolling_window"][0]
            self.short_filter_rolling_window_1 = new_bounds["short_filter_rolling_window"][1]
        if "short_n_positions" in new_bounds:
            self.short_n_positions_0 = new_bounds["short_n_positions"][0]
            self.short_n_positions_1 = new_bounds["short_n_positions"][1]
        if "short_total_wallet_exposure_limit" in new_bounds:
            self.short_total_wallet_exposure_limit_0 = new_bounds["short_total_wallet_exposure_limit"][0]
            self.short_total_wallet_exposure_limit_1 = new_bounds["short_total_wallet_exposure_limit"][1]
        if "short_unstuck_close_pct" in new_bounds:
            self.short_unstuck_close_pct_0 = new_bounds["short_unstuck_close_pct"][0]
            self.short_unstuck_close_pct_1 = new_bounds["short_unstuck_close_pct"][1]
        if "short_unstuck_ema_dist" in new_bounds:
            self.short_unstuck_ema_dist_0 = new_bounds["short_unstuck_ema_dist"][0]
            self.short_unstuck_ema_dist_1 = new_bounds["short_unstuck_ema_dist"][1]
        if "short_unstuck_loss_allowance_pct" in new_bounds:
            self.short_unstuck_loss_allowance_pct_0 = new_bounds["short_unstuck_loss_allowance_pct"][0]
            self.short_unstuck_loss_allowance_pct_1 = new_bounds["short_unstuck_loss_allowance_pct"][1]
        if "short_unstuck_threshold" in new_bounds:
            self.short_unstuck_threshold_0 = new_bounds["short_unstuck_threshold"][0]
            self.short_unstuck_threshold_1 = new_bounds["short_unstuck_threshold"][1]
        
    # Long parameters
    @property
    def long_close_grid_markup_range_0(self): return self._long_close_grid_markup_range_0
    @property
    def long_close_grid_markup_range_1(self): return self._long_close_grid_markup_range_1
    @property
    def long_close_grid_min_markup_0(self): return self._long_close_grid_min_markup_0
    @property
    def long_close_grid_min_markup_1(self): return self._long_close_grid_min_markup_1
    @property
    def long_close_grid_qty_pct_0(self): return self._long_close_grid_qty_pct_0
    @property
    def long_close_grid_qty_pct_1(self): return self._long_close_grid_qty_pct_1
    @property
    def long_close_trailing_grid_ratio_0(self): return self._long_close_trailing_grid_ratio_0
    @property
    def long_close_trailing_grid_ratio_1(self): return self._long_close_trailing_grid_ratio_1
    @property
    def long_close_trailing_qty_pct_0(self): return self._long_close_trailing_qty_pct_0
    @property
    def long_close_trailing_qty_pct_1(self): return self._long_close_trailing_qty_pct_1
    @property
    def long_close_trailing_retracement_pct_0(self): return self._long_close_trailing_retracement_pct_0
    @property
    def long_close_trailing_retracement_pct_1(self): return self._long_close_trailing_retracement_pct_1
    @property
    def long_close_trailing_threshold_pct_0(self): return self._long_close_trailing_threshold_pct_0
    @property
    def long_close_trailing_threshold_pct_1(self): return self._long_close_trailing_threshold_pct_1
    @property
    def long_ema_span_0_0(self): return self._long_ema_span_0_0
    @property
    def long_ema_span_0_1(self): return self._long_ema_span_0_1
    @property
    def long_ema_span_1_0(self): return self._long_ema_span_1_0
    @property
    def long_ema_span_1_1(self): return self._long_ema_span_1_1
    @property
    def long_entry_grid_double_down_factor_0(self): return self._long_entry_grid_double_down_factor_0
    @property
    def long_entry_grid_double_down_factor_1(self): return self._long_entry_grid_double_down_factor_1
    @property
    def long_entry_grid_spacing_pct_0(self): return self._long_entry_grid_spacing_pct_0
    @property
    def long_entry_grid_spacing_pct_1(self): return self._long_entry_grid_spacing_pct_1
    @property
    def long_entry_grid_spacing_weight_0(self): return self._long_entry_grid_spacing_weight_0
    @property
    def long_entry_grid_spacing_weight_1(self): return self._long_entry_grid_spacing_weight_1
    @property
    def long_entry_initial_ema_dist_0(self): return self._long_entry_initial_ema_dist_0
    @property
    def long_entry_initial_ema_dist_1(self): return self._long_entry_initial_ema_dist_1
    @property
    def long_entry_initial_qty_pct_0(self): return self._long_entry_initial_qty_pct_0
    @property
    def long_entry_initial_qty_pct_1(self): return self._long_entry_initial_qty_pct_1
    @property
    def long_entry_trailing_grid_ratio_0(self): return self._long_entry_trailing_grid_ratio_0
    @property
    def long_entry_trailing_grid_ratio_1(self): return self._long_entry_trailing_grid_ratio_1
    @property
    def long_entry_trailing_retracement_pct_0(self): return self._long_entry_trailing_retracement_pct_0
    @property
    def long_entry_trailing_retracement_pct_1(self): return self._long_entry_trailing_retracement_pct_1
    @property
    def long_entry_trailing_threshold_pct_0(self): return self._long_entry_trailing_threshold_pct_0
    @property
    def long_entry_trailing_threshold_pct_1(self): return self._long_entry_trailing_threshold_pct_1
    @property
    def long_filter_relative_volume_clip_pct_0(self): return self._long_filter_relative_volume_clip_pct_0
    @property
    def long_filter_relative_volume_clip_pct_1(self): return self._long_filter_relative_volume_clip_pct_1
    @property
    def long_filter_rolling_window_0(self): return self._long_filter_rolling_window_0
    @property
    def long_filter_rolling_window_1(self): return self._long_filter_rolling_window_1
    @property
    def long_n_positions_0(self): return self._long_n_positions_0
    @property
    def long_n_positions_1(self): return self._long_n_positions_1
    @property
    def long_total_wallet_exposure_limit_0(self): return self._long_total_wallet_exposure_limit_0
    @property
    def long_total_wallet_exposure_limit_1(self): return self._long_total_wallet_exposure_limit_1
    @property
    def long_unstuck_close_pct_0(self): return self._long_unstuck_close_pct_0
    @property
    def long_unstuck_close_pct_1(self): return self._long_unstuck_close_pct_1
    @property
    def long_unstuck_ema_dist_0(self): return self._long_unstuck_ema_dist_0
    @property
    def long_unstuck_ema_dist_1(self): return self._long_unstuck_ema_dist_1
    @property
    def long_unstuck_loss_allowance_pct_0(self): return self._long_unstuck_loss_allowance_pct_0
    @property
    def long_unstuck_loss_allowance_pct_1(self): return self._long_unstuck_loss_allowance_pct_1
    @property
    def long_unstuck_threshold_0(self): return self._long_unstuck_threshold_0
    
    # Short parameters
    @property
    def long_unstuck_threshold_1(self): return self._long_unstuck_threshold_1
    @property
    def short_close_grid_markup_range_0(self): return self._short_close_grid_markup_range_0
    @property
    def short_close_grid_markup_range_1(self): return self._short_close_grid_markup_range_1
    @property
    def short_close_grid_min_markup_0(self): return self._short_close_grid_min_markup_0
    @property
    def short_close_grid_min_markup_1(self): return self._short_close_grid_min_markup_1
    @property
    def short_close_grid_qty_pct_0(self): return self._short_close_grid_qty_pct_0
    @property
    def short_close_grid_qty_pct_1(self): return self._short_close_grid_qty_pct_1
    @property
    def short_close_trailing_grid_ratio_0(self): return self._short_close_trailing_grid_ratio_0
    @property
    def short_close_trailing_grid_ratio_1(self): return self._short_close_trailing_grid_ratio_1
    @property
    def short_close_trailing_qty_pct_0(self): return self._short_close_trailing_qty_pct_0
    @property
    def short_close_trailing_qty_pct_1(self): return self._short_close_trailing_qty_pct_1
    @property
    def short_close_trailing_retracement_pct_0(self): return self._short_close_trailing_retracement_pct_0
    @property
    def short_close_trailing_retracement_pct_1(self): return self._short_close_trailing_retracement_pct_1
    @property
    def short_close_trailing_threshold_pct_0(self): return self._short_close_trailing_threshold_pct_0
    @property
    def short_close_trailing_threshold_pct_1(self): return self._short_close_trailing_threshold_pct_1
    @property
    def short_ema_span_0_0(self): return self._short_ema_span_0_0
    @property
    def short_ema_span_0_1(self): return self._short_ema_span_0_1
    @property
    def short_ema_span_1_0(self): return self._short_ema_span_1_0
    @property
    def short_ema_span_1_1(self): return self._short_ema_span_1_1
    @property
    def short_entry_grid_double_down_factor_0(self): return self._short_entry_grid_double_down_factor_0
    @property
    def short_entry_grid_double_down_factor_1(self): return self._short_entry_grid_double_down_factor_1
    @property
    def short_entry_grid_spacing_pct_0(self): return self._short_entry_grid_spacing_pct_0
    @property
    def short_entry_grid_spacing_pct_1(self): return self._short_entry_grid_spacing_pct_1
    @property
    def short_entry_grid_spacing_weight_0(self): return self._short_entry_grid_spacing_weight_0
    @property
    def short_entry_grid_spacing_weight_1(self): return self._short_entry_grid_spacing_weight_1
    @property
    def short_entry_initial_ema_dist_0(self): return self._short_entry_initial_ema_dist_0
    @property
    def short_entry_initial_ema_dist_1(self): return self._short_entry_initial_ema_dist_1
    @property
    def short_entry_initial_qty_pct_0(self): return self._short_entry_initial_qty_pct_0
    @property
    def short_entry_initial_qty_pct_1(self): return self._short_entry_initial_qty_pct_1
    @property
    def short_entry_trailing_grid_ratio_0(self): return self._short_entry_trailing_grid_ratio_0
    @property
    def short_entry_trailing_grid_ratio_1(self): return self._short_entry_trailing_grid_ratio_1
    @property
    def short_entry_trailing_retracement_pct_0(self): return self._short_entry_trailing_retracement_pct_0
    @property
    def short_entry_trailing_retracement_pct_1(self): return self._short_entry_trailing_retracement_pct_1
    @property
    def short_entry_trailing_threshold_pct_0(self): return self._short_entry_trailing_threshold_pct_0
    @property
    def short_entry_trailing_threshold_pct_1(self): return self._short_entry_trailing_threshold_pct_1
    @property
    def short_filter_relative_volume_clip_pct_0(self): return self._short_filter_relative_volume_clip_pct_0
    @property
    def short_filter_relative_volume_clip_pct_1(self): return self._short_filter_relative_volume_clip_pct_1
    @property
    def short_filter_rolling_window_0(self): return self._short_filter_rolling_window_0
    @property
    def short_filter_rolling_window_1(self): return self._short_filter_rolling_window_1
    @property
    def short_n_positions_0(self): return self._short_n_positions_0
    @property
    def short_n_positions_1(self): return self._short_n_positions_1
    @property
    def short_total_wallet_exposure_limit_0(self): return self._short_total_wallet_exposure_limit_0
    @property
    def short_total_wallet_exposure_limit_1(self): return self._short_total_wallet_exposure_limit_1
    @property
    def short_unstuck_close_pct_0(self): return self._short_unstuck_close_pct_0
    @property
    def short_unstuck_close_pct_1(self): return self._short_unstuck_close_pct_1
    @property
    def short_unstuck_ema_dist_0(self): return self._short_unstuck_ema_dist_0
    @property
    def short_unstuck_ema_dist_1(self): return self._short_unstuck_ema_dist_1
    @property
    def short_unstuck_loss_allowance_pct_0(self): return self._short_unstuck_loss_allowance_pct_0
    @property
    def short_unstuck_loss_allowance_pct_1(self): return self._short_unstuck_loss_allowance_pct_1
    @property
    def short_unstuck_threshold_0(self): return self._short_unstuck_threshold_0
    @property
    def short_unstuck_threshold_1(self): return self._short_unstuck_threshold_1

    # Long setters
    @long_close_grid_markup_range_0.setter
    def long_close_grid_markup_range_0(self, new_value):
        self._long_close_grid_markup_range_0 = new_value
        self._bounds["long_close_grid_markup_range"][0] = new_value
    @long_close_grid_markup_range_1.setter
    def long_close_grid_markup_range_1(self, new_value):
        self._long_close_grid_markup_range_1 = new_value
        self._bounds["long_close_grid_markup_range"][1] = new_value
    @long_close_grid_min_markup_0.setter
    def long_close_grid_min_markup_0(self, new_value):
        self._long_close_grid_min_markup_0 = new_value
        self._bounds["long_close_grid_min_markup"][0] = new_value
    @long_close_grid_min_markup_1.setter
    def long_close_grid_min_markup_1(self, new_value):
        self._long_close_grid_min_markup_1 = new_value
        self._bounds["long_close_grid_min_markup"][1] = new_value
    @long_close_grid_qty_pct_0.setter
    def long_close_grid_qty_pct_0(self, new_value):
        self._long_close_grid_qty_pct_0 = new_value
        self._bounds["long_close_grid_qty_pct"][0] = new_value
    @long_close_grid_qty_pct_1.setter
    def long_close_grid_qty_pct_1(self, new_value):
        self._long_close_grid_qty_pct_1 = new_value
        self._bounds["long_close_grid_qty_pct"][1] = new_value
    @long_close_trailing_grid_ratio_0.setter
    def long_close_trailing_grid_ratio_0(self, new_value):
        self._long_close_trailing_grid_ratio_0 = new_value
        self._bounds["long_close_trailing_grid_ratio"][0] = new_value
    @long_close_trailing_grid_ratio_1.setter
    def long_close_trailing_grid_ratio_1(self, new_value):
        self._long_close_trailing_grid_ratio_1 = new_value
        self._bounds["long_close_trailing_grid_ratio"][1] = new_value
    @long_close_trailing_qty_pct_0.setter
    def long_close_trailing_qty_pct_0(self, new_value):
        self._long_close_trailing_qty_pct_0 = new_value
        self._bounds["long_close_trailing_qty_pct"][0] = new_value
    @long_close_trailing_qty_pct_1.setter
    def long_close_trailing_qty_pct_1(self, new_value):
        self._long_close_trailing_qty_pct_1 = new_value
        self._bounds["long_close_trailing_qty_pct"][1] = new_value
    @long_close_trailing_retracement_pct_0.setter
    def long_close_trailing_retracement_pct_0(self, new_value):
        self._long_close_trailing_retracement_pct_0 = new_value
        self._bounds["long_close_trailing_retracement_pct"][0] = new_value
    @long_close_trailing_retracement_pct_1.setter
    def long_close_trailing_retracement_pct_1(self, new_value):
        self._long_close_trailing_retracement_pct_1 = new_value
        self._bounds["long_close_trailing_retracement_pct"][1] = new_value
    @long_close_trailing_threshold_pct_0.setter
    def long_close_trailing_threshold_pct_0(self, new_value):
        self._long_close_trailing_threshold_pct_0 = new_value
        self._bounds["long_close_trailing_threshold_pct"][0] = new_value
    @long_close_trailing_threshold_pct_1.setter
    def long_close_trailing_threshold_pct_1(self, new_value):
        self._long_close_trailing_threshold_pct_1 = new_value
        self._bounds["long_close_trailing_threshold_pct"][1] = new_value
    @long_ema_span_0_0.setter
    def long_ema_span_0_0(self, new_value):
        self._long_ema_span_0_0 = new_value
        self._bounds["long_ema_span_0"][0] = new_value
    @long_ema_span_0_1.setter
    def long_ema_span_0_1(self, new_value):
        self._long_ema_span_0_1 = new_value
        self._bounds["long_ema_span_0"][1] = new_value
    @long_ema_span_1_0.setter
    def long_ema_span_1_0(self, new_value):
        self._long_ema_span_1_0 = new_value
        self._bounds["long_ema_span_1"][0] = new_value
    @long_ema_span_1_1.setter
    def long_ema_span_1_1(self, new_value):
        self._long_ema_span_1_1 = new_value
        self._bounds["long_ema_span_1"][1] = new_value
    @long_entry_grid_double_down_factor_0.setter
    def long_entry_grid_double_down_factor_0(self, new_value):
        self._long_entry_grid_double_down_factor_0 = new_value
        self._bounds["long_entry_grid_double_down_factor"][0] = new_value
    @long_entry_grid_double_down_factor_1.setter
    def long_entry_grid_double_down_factor_1(self, new_value):
        self._long_entry_grid_double_down_factor_1 = new_value
        self._bounds["long_entry_grid_double_down_factor"][1] = new_value
    @long_entry_grid_spacing_pct_0.setter
    def long_entry_grid_spacing_pct_0(self, new_value):
        self._long_entry_grid_spacing_pct_0 = new_value
        self._bounds["long_entry_grid_spacing_pct"][0] = new_value
    @long_entry_grid_spacing_pct_1.setter
    def long_entry_grid_spacing_pct_1(self, new_value):
        self._long_entry_grid_spacing_pct_1 = new_value
        self._bounds["long_entry_grid_spacing_pct"][1] = new_value
    @long_entry_grid_spacing_weight_0.setter
    def long_entry_grid_spacing_weight_0(self, new_value):
        self._long_entry_grid_spacing_weight_0 = new_value
        self._bounds["long_entry_grid_spacing_weight"][0] = new_value
    @long_entry_grid_spacing_weight_1.setter
    def long_entry_grid_spacing_weight_1(self, new_value):
        self._long_entry_grid_spacing_weight_1 = new_value
        self._bounds["long_entry_grid_spacing_weight"][1] = new_value
    @long_entry_initial_ema_dist_0.setter
    def long_entry_initial_ema_dist_0(self, new_value):
        self._long_entry_initial_ema_dist_0 = new_value
        self._bounds["long_entry_initial_ema_dist"][0] = new_value
    @long_entry_initial_ema_dist_1.setter
    def long_entry_initial_ema_dist_1(self, new_value):
        self._long_entry_initial_ema_dist_1 = new_value
        self._bounds["long_entry_initial_ema_dist"][1] = new_value
    @long_entry_initial_qty_pct_0.setter
    def long_entry_initial_qty_pct_0(self, new_value):
        self._long_entry_initial_qty_pct_0 = new_value
        self._bounds["long_entry_initial_qty_pct"][0] = new_value
    @long_entry_initial_qty_pct_1.setter
    def long_entry_initial_qty_pct_1(self, new_value):
        self._long_entry_initial_qty_pct_1 = new_value
        self._bounds["long_entry_initial_qty_pct"][1] = new_value
    @long_entry_trailing_grid_ratio_0.setter
    def long_entry_trailing_grid_ratio_0(self, new_value):
        self._long_entry_trailing_grid_ratio_0 = new_value
        self._bounds["long_entry_trailing_grid_ratio"][0] = new_value
    @long_entry_trailing_grid_ratio_1.setter
    def long_entry_trailing_grid_ratio_1(self, new_value):
        self._long_entry_trailing_grid_ratio_1 = new_value
        self._bounds["long_entry_trailing_grid_ratio"][1] = new_value
    @long_entry_trailing_retracement_pct_0.setter
    def long_entry_trailing_retracement_pct_0(self, new_value):
        self._long_entry_trailing_retracement_pct_0 = new_value
        self._bounds["long_entry_trailing_retracement_pct"][0] = new_value
    @long_entry_trailing_retracement_pct_1.setter
    def long_entry_trailing_retracement_pct_1(self, new_value):
        self._long_entry_trailing_retracement_pct_1 = new_value
        self._bounds["long_entry_trailing_retracement_pct"][1] = new_value
    @long_entry_trailing_threshold_pct_0.setter
    def long_entry_trailing_threshold_pct_0(self, new_value):
        self._long_entry_trailing_threshold_pct_0 = new_value
        self._bounds["long_entry_trailing_threshold_pct"][0] = new_value
    @long_entry_trailing_threshold_pct_1.setter
    def long_entry_trailing_threshold_pct_1(self, new_value):
        self._long_entry_trailing_threshold_pct_1 = new_value
        self._bounds["long_entry_trailing_threshold_pct"][1] = new_value
    @long_filter_relative_volume_clip_pct_0.setter
    def long_filter_relative_volume_clip_pct_0(self, new_value):
        self._long_filter_relative_volume_clip_pct_0 = new_value
        self._bounds["long_filter_relative_volume_clip_pct"][0] = new_value
    @long_filter_relative_volume_clip_pct_1.setter
    def long_filter_relative_volume_clip_pct_1(self, new_value):
        self._long_filter_relative_volume_clip_pct_1 = new_value
        self._bounds["long_filter_relative_volume_clip_pct"][1] = new_value
    @long_filter_rolling_window_0.setter
    def long_filter_rolling_window_0(self, new_value):
        self._long_filter_rolling_window_0 = new_value
        self._bounds["long_filter_rolling_window"][0] = new_value
    @long_filter_rolling_window_1.setter
    def long_filter_rolling_window_1(self, new_value):
        self._long_filter_rolling_window_1 = new_value
        self._bounds["long_filter_rolling_window"][1] = new_value
    @long_n_positions_0.setter
    def long_n_positions_0(self, new_value):
        self._long_n_positions_0 = new_value
        self._bounds["long_n_positions"][0] = new_value
    @long_n_positions_1.setter
    def long_n_positions_1(self, new_value):
        self._long_n_positions_1 = new_value
        self._bounds["long_n_positions"][1] = new_value
    @long_total_wallet_exposure_limit_0.setter
    def long_total_wallet_exposure_limit_0(self, new_value):
        self._long_total_wallet_exposure_limit_0 = new_value
        self._bounds["long_total_wallet_exposure_limit"][0] = new_value
    @long_total_wallet_exposure_limit_1.setter
    def long_total_wallet_exposure_limit_1(self, new_value):
        self._long_total_wallet_exposure_limit_1 = new_value
        self._bounds["long_total_wallet_exposure_limit"][1] = new_value
    @long_unstuck_close_pct_0.setter
    def long_unstuck_close_pct_0(self, new_value):
        self._long_unstuck_close_pct_0 = new_value
        self._bounds["long_unstuck_close_pct"][0] = new_value
    @long_unstuck_close_pct_1.setter
    def long_unstuck_close_pct_1(self, new_value):
        self._long_unstuck_close_pct_1 = new_value
        self._bounds["long_unstuck_close_pct"][1] = new_value
    @long_unstuck_ema_dist_0.setter
    def long_unstuck_ema_dist_0(self, new_value):
        self._long_unstuck_ema_dist_0 = new_value
        self._bounds["long_unstuck_ema_dist"][0] = new_value
    @long_unstuck_ema_dist_1.setter
    def long_unstuck_ema_dist_1(self, new_value):
        self._long_unstuck_ema_dist_1 = new_value
        self._bounds["long_unstuck_ema_dist"][1] = new_value
    @long_unstuck_loss_allowance_pct_0.setter
    def long_unstuck_loss_allowance_pct_0(self, new_value):
        self._long_unstuck_loss_allowance_pct_0 = new_value
        self._bounds["long_unstuck_loss_allowance_pct"][0] = new_value
    @long_unstuck_loss_allowance_pct_1.setter
    def long_unstuck_loss_allowance_pct_1(self, new_value):
        self._long_unstuck_loss_allowance_pct_1 = new_value
        self._bounds["long_unstuck_loss_allowance_pct"][1] = new_value
    @long_unstuck_threshold_0.setter
    def long_unstuck_threshold_0(self, new_value):
        self._long_unstuck_threshold_0 = new_value
        self._bounds["long_unstuck_threshold"][0] = new_value
    @long_unstuck_threshold_1.setter
    def long_unstuck_threshold_1(self, new_value):
        self._long_unstuck_threshold_1 = new_value
        self._bounds["long_unstuck_threshold"][1] = new_value

    # Short setters
    @short_close_grid_markup_range_0.setter
    def short_close_grid_markup_range_0(self, new_value):
        self._short_close_grid_markup_range_0 = new_value
        self._bounds["short_close_grid_markup_range"][0] = new_value
    @short_close_grid_markup_range_1.setter
    def short_close_grid_markup_range_1(self, new_value):
        self._short_close_grid_markup_range_1 = new_value
        self._bounds["short_close_grid_markup_range"][1] = new_value
    @short_close_grid_min_markup_0.setter
    def short_close_grid_min_markup_0(self, new_value):
        self._short_close_grid_min_markup_0 = new_value
        self._bounds["short_close_grid_min_markup"][0] = new_value
    @short_close_grid_min_markup_1.setter
    def short_close_grid_min_markup_1(self, new_value):
        self._short_close_grid_min_markup_1 = new_value
        self._bounds["short_close_grid_min_markup"][1] = new_value
    @short_close_grid_qty_pct_0.setter
    def short_close_grid_qty_pct_0(self, new_value):
        self._short_close_grid_qty_pct_0 = new_value
        self._bounds["short_close_grid_qty_pct"][0] = new_value
    @short_close_grid_qty_pct_1.setter
    def short_close_grid_qty_pct_1(self, new_value):
        self._short_close_grid_qty_pct_1 = new_value
        self._bounds["short_close_grid_qty_pct"][1] = new_value
    @short_close_trailing_grid_ratio_0.setter
    def short_close_trailing_grid_ratio_0(self, new_value):
        self._short_close_trailing_grid_ratio_0 = new_value
        self._bounds["short_close_trailing_grid_ratio"][0] = new_value
    @short_close_trailing_grid_ratio_1.setter
    def short_close_trailing_grid_ratio_1(self, new_value):
        self._short_close_trailing_grid_ratio_1 = new_value
        self._bounds["short_close_trailing_grid_ratio"][1] = new_value
    @short_close_trailing_qty_pct_0.setter
    def short_close_trailing_qty_pct_0(self, new_value):
        self._short_close_trailing_qty_pct_0 = new_value
        self._bounds["short_close_trailing_qty_pct"][0] = new_value
    @short_close_trailing_qty_pct_1.setter
    def short_close_trailing_qty_pct_1(self, new_value):
        self._short_close_trailing_qty_pct_1 = new_value
        self._bounds["short_close_trailing_qty_pct"][1] = new_value
    @short_close_trailing_retracement_pct_0.setter
    def short_close_trailing_retracement_pct_0(self, new_value):
        self._short_close_trailing_retracement_pct_0 = new_value
        self._bounds["short_close_trailing_retracement_pct"][0] = new_value
    @short_close_trailing_retracement_pct_1.setter
    def short_close_trailing_retracement_pct_1(self, new_value):
        self._short_close_trailing_retracement_pct_1 = new_value
        self._bounds["short_close_trailing_retracement_pct"][1] = new_value
    @short_close_trailing_threshold_pct_0.setter
    def short_close_trailing_threshold_pct_0(self, new_value):
        self._short_close_trailing_threshold_pct_0 = new_value
        self._bounds["short_close_trailing_threshold_pct"][0] = new_value
    @short_close_trailing_threshold_pct_1.setter
    def short_close_trailing_threshold_pct_1(self, new_value):
        self._short_close_trailing_threshold_pct_1 = new_value
        self._bounds["short_close_trailing_threshold_pct"][1] = new_value
    @short_ema_span_0_0.setter
    def short_ema_span_0_0(self, new_value):
        self._short_ema_span_0_0 = new_value
        self._bounds["short_ema_span_0"][0] = new_value
    @short_ema_span_0_1.setter
    def short_ema_span_0_1(self, new_value):
        self._short_ema_span_0_1 = new_value
        self._bounds["short_ema_span_0"][1] = new_value
    @short_ema_span_1_0.setter
    def short_ema_span_1_0(self, new_value):
        self._short_ema_span_1_0 = new_value
        self._bounds["short_ema_span_1"][0] = new_value
    @short_ema_span_1_1.setter
    def short_ema_span_1_1(self, new_value):
        self._short_ema_span_1_1 = new_value
        self._bounds["short_ema_span_1"][1] = new_value
    @short_entry_grid_double_down_factor_0.setter
    def short_entry_grid_double_down_factor_0(self, new_value):
        self._short_entry_grid_double_down_factor_0 = new_value
        self._bounds["short_entry_grid_double_down_factor"][0] = new_value
    @short_entry_grid_double_down_factor_1.setter
    def short_entry_grid_double_down_factor_1(self, new_value):
        self._short_entry_grid_double_down_factor_1 = new_value
        self._bounds["short_entry_grid_double_down_factor"][1] = new_value
    @short_entry_grid_spacing_pct_0.setter
    def short_entry_grid_spacing_pct_0(self, new_value):
        self._short_entry_grid_spacing_pct_0 = new_value
        self._bounds["short_entry_grid_spacing_pct"][0] = new_value
    @short_entry_grid_spacing_pct_1.setter
    def short_entry_grid_spacing_pct_1(self, new_value):
        self._short_entry_grid_spacing_pct_1 = new_value
        self._bounds["short_entry_grid_spacing_pct"][1] = new_value
    @short_entry_grid_spacing_weight_0.setter
    def short_entry_grid_spacing_weight_0(self, new_value):
        self._short_entry_grid_spacing_weight_0 = new_value
        self._bounds["short_entry_grid_spacing_weight"][0] = new_value
    @short_entry_grid_spacing_weight_1.setter
    def short_entry_grid_spacing_weight_1(self, new_value):
        self._short_entry_grid_spacing_weight_1 = new_value
        self._bounds["short_entry_grid_spacing_weight"][1] = new_value
    @short_entry_initial_ema_dist_0.setter
    def short_entry_initial_ema_dist_0(self, new_value):
        self._short_entry_initial_ema_dist_0 = new_value
        self._bounds["short_entry_initial_ema_dist"][0] = new_value
    @short_entry_initial_ema_dist_1.setter 
    def short_entry_initial_ema_dist_1(self, new_value):
        self._short_entry_initial_ema_dist_1 = new_value
        self._bounds["short_entry_initial_ema_dist"][1] = new_value
    @short_entry_initial_qty_pct_0.setter
    def short_entry_initial_qty_pct_0(self, new_value):
        self._short_entry_initial_qty_pct_0 = new_value
        self._bounds["short_entry_initial_qty_pct"][0] = new_value
    @short_entry_initial_qty_pct_1.setter
    def short_entry_initial_qty_pct_1(self, new_value):
        self._short_entry_initial_qty_pct_1 = new_value
        self._bounds["short_entry_initial_qty_pct"][1] = new_value
    @short_entry_trailing_grid_ratio_0.setter
    def short_entry_trailing_grid_ratio_0(self, new_value):
        self._short_entry_trailing_grid_ratio_0 = new_value
        self._bounds["short_entry_trailing_grid_ratio"][0] = new_value
    @short_entry_trailing_grid_ratio_1.setter
    def short_entry_trailing_grid_ratio_1(self, new_value):
        self._short_entry_trailing_grid_ratio_1 = new_value
        self._bounds["short_entry_trailing_grid_ratio"][1] = new_value
    @short_entry_trailing_retracement_pct_0.setter
    def short_entry_trailing_retracement_pct_0(self, new_value):
        self._short_entry_trailing_retracement_pct_0 = new_value
        self._bounds["short_entry_trailing_retracement_pct"][0] = new_value
    @short_entry_trailing_retracement_pct_1.setter
    def short_entry_trailing_retracement_pct_1(self, new_value):
        self._short_entry_trailing_retracement_pct_1 = new_value
        self._bounds["short_entry_trailing_retracement_pct"][1] = new_value
    @short_entry_trailing_threshold_pct_0.setter
    def short_entry_trailing_threshold_pct_0(self, new_value):
        self._short_entry_trailing_threshold_pct_0 = new_value
        self._bounds["short_entry_trailing_threshold_pct"][0] = new_value
    @short_entry_trailing_threshold_pct_1.setter
    def short_entry_trailing_threshold_pct_1(self, new_value):
        self._short_entry_trailing_threshold_pct_1 = new_value
        self._bounds["short_entry_trailing_threshold_pct"][1] = new_value
    @short_filter_relative_volume_clip_pct_0.setter
    def short_filter_relative_volume_clip_pct_0(self, new_value):
        self._short_filter_relative_volume_clip_pct_0 = new_value
        self._bounds["short_filter_relative_volume_clip_pct"][0] = new_value
    @short_filter_relative_volume_clip_pct_1.setter
    def short_filter_relative_volume_clip_pct_1(self, new_value):
        self._short_filter_relative_volume_clip_pct_1 = new_value
        self._bounds["short_filter_relative_volume_clip_pct"][1] = new_value
    @short_filter_rolling_window_0.setter
    def short_filter_rolling_window_0(self, new_value):
        self._short_filter_rolling_window_0 = new_value
        self._bounds["short_filter_rolling_window"][0] = new_value
    @short_filter_rolling_window_1.setter
    def short_filter_rolling_window_1(self, new_value):
        self._short_filter_rolling_window_1 = new_value
        self._bounds["short_filter_rolling_window"][1] = new_value
    @short_n_positions_0.setter
    def short_n_positions_0(self, new_value):
        self._short_n_positions_0 = new_value
        self._bounds["short_n_positions"][0] = new_value
    @short_n_positions_1.setter
    def short_n_positions_1(self, new_value):
        self._short_n_positions_1 = new_value
        self._bounds["short_n_positions"][1] = new_value
    @short_total_wallet_exposure_limit_0.setter
    def short_total_wallet_exposure_limit_0(self, new_value):
        self._short_total_wallet_exposure_limit_0 = new_value
        self._bounds["short_total_wallet_exposure_limit"][0] = new_value
    @short_total_wallet_exposure_limit_1.setter
    def short_total_wallet_exposure_limit_1(self, new_value):
        self._short_total_wallet_exposure_limit_1 = new_value
        self._bounds["short_total_wallet_exposure_limit"][1] = new_value
    @short_unstuck_close_pct_0.setter
    def short_unstuck_close_pct_0(self, new_value):
        self._short_unstuck_close_pct_0 = new_value
        self._bounds["short_unstuck_close_pct"][0] = new_value
    @short_unstuck_close_pct_1.setter
    def short_unstuck_close_pct_1(self, new_value):
        self._short_unstuck_close_pct_1 = new_value
        self._bounds["short_unstuck_close_pct"][1] = new_value
    @short_unstuck_ema_dist_0.setter
    def short_unstuck_ema_dist_0(self, new_value):
        self._short_unstuck_ema_dist_0 = new_value
        self._bounds["short_unstuck_ema_dist"][0] = new_value
    @short_unstuck_ema_dist_1.setter
    def short_unstuck_ema_dist_1(self, new_value):
        self._short_unstuck_ema_dist_1 = new_value
        self._bounds["short_unstuck_ema_dist"][1] = new_value
    @short_unstuck_loss_allowance_pct_0.setter
    def short_unstuck_loss_allowance_pct_0(self, new_value):
        self._short_unstuck_loss_allowance_pct_0 = new_value
        self._bounds["short_unstuck_loss_allowance_pct"][0] = new_value
    @short_unstuck_loss_allowance_pct_1.setter
    def short_unstuck_loss_allowance_pct_1(self, new_value):
        self._short_unstuck_loss_allowance_pct_1 = new_value
        self._bounds["short_unstuck_loss_allowance_pct"][1] = new_value
    @short_unstuck_threshold_0.setter
    def short_unstuck_threshold_0(self, new_value):
        self._short_unstuck_threshold_0 = new_value
        self._bounds["short_unstuck_threshold"][0] = new_value
    @short_unstuck_threshold_1.setter
    def short_unstuck_threshold_1(self, new_value):
        self._short_unstuck_threshold_1 = new_value
        self._bounds["short_unstuck_threshold"][1] = new_value

class PBGui:
    def __init__(self):
        self._version = 0
        self._enabled_on = "disabled"
        self._starting_config = False
        self._market_cap = 0
        self._vol_mcap = 10.0
        self._dynamic_ignore = False
        self._note = ''
        self._pbgui = {
            "version": self._version,
            "enabled_on": self._enabled_on,
            "starting_config": self._starting_config,
            "market_cap": self._market_cap,
            "vol_mcap": self._vol_mcap,
            "dynamic_ignore": self._dynamic_ignore,
            "note": self._note,
        }
    
    def __repr__(self):
        return str(self._pbgui)
    
    @property
    def pbgui(self): return self._pbgui
    @pbgui.setter
    def pbgui(self, new_pbgui):
        if "version" in new_pbgui:
            self.version = new_pbgui["version"]
        if "enabled_on" in new_pbgui:
            self.enabled_on = new_pbgui["enabled_on"]
        if "starting_config" in new_pbgui:
            self.starting_config = new_pbgui["starting_config"]
        if "market_cap" in new_pbgui:
            self.market_cap = new_pbgui["market_cap"]
        if "vol_mcap" in new_pbgui:
            self.vol_mcap = new_pbgui["vol_mcap"]
        if "dynamic_ignore" in new_pbgui:
            self.dynamic_ignore = new_pbgui["dynamic_ignore"]
        if "note" in new_pbgui:
            self.note = new_pbgui["note"]
    
    @property
    def version(self): return self._version
    @property
    def enabled_on(self): return self._enabled_on
    @property
    def starting_config(self): return self._starting_config
    @property
    def market_cap(self): return self._market_cap
    @property
    def vol_mcap(self): return self._vol_mcap
    @property
    def dynamic_ignore(self): return self._dynamic_ignore
    @property
    def note(self): return self._note

    @version.setter
    def version(self, new_version):
        self._version = new_version
        self._pbgui["version"] = self._version
    @enabled_on.setter
    def enabled_on(self, new_enabled_on):
        self._enabled_on = new_enabled_on
        self._pbgui["enabled_on"] = self._enabled_on
    @starting_config.setter
    def starting_config(self, new_starting_config):
        self._starting_config = new_starting_config
        self._pbgui["starting_config"] = self._starting_config
    @market_cap.setter
    def market_cap(self, new_market_cap):
        self._market_cap = new_market_cap
        self._pbgui["market_cap"] = self._market_cap
    @vol_mcap.setter
    def vol_mcap(self, new_vol_mcap):
        self._vol_mcap = new_vol_mcap
        self._pbgui["vol_mcap"] = self._vol_mcap
    @dynamic_ignore.setter
    def dynamic_ignore(self, new_dynamic_ignore):
        self._dynamic_ignore = new_dynamic_ignore
        self._pbgui["dynamic_ignore"] = self._dynamic_ignore
    @note.setter
    def note(self, new_note):
        self._note = new_note
        self._pbgui["note"] = self._note

class ConfigV7():
    def __init__(self, file_name = None):
        self._config_file = file_name
        self._backtest = Backtest()
        self._bot = Bot()
        self._live = Live()
        self._optimize = Optimize()
        self._pbgui = PBGui()

        self._config = {
            "backtest": self._backtest._backtest,
            "bot": self._bot._bot,
            "live": self._live._live,
            "optimize": self._optimize._optimize,
            "pbgui": self._pbgui._pbgui
        }

    @property
    def config_file(self): return self._config_file
    @config_file.setter
    def config_file(self, new_value):
        self._config_file = new_value

    @property
    def backtest(self): return self._backtest
    @backtest.setter
    def backtest(self, new_value):
        self._backtest.backtest = new_value
        self._config["backtest"] = self._backtest.backtest

    @property
    def bot(self): return self._bot
    @bot.setter
    def bot(self, new_value):
        self._bot.bot = new_value
        self._config["bot"] = self._bot.bot

    @property
    def live(self): return self._live
    @live.setter
    def live(self, new_value):
        self._live.live = new_value
        self._config["live"] = self._live.live

    @property
    def optimize(self): return self._optimize
    @optimize.setter
    def optimize(self, new_value):
        self._optimize.optimize = new_value
        self._config["optimize"] = self._optimize.optimize

    @property
    def pbgui(self): return self._pbgui
    @pbgui.setter
    def pbgui(self, new_value):
        self._pbgui.pbgui = new_value
        self._config["pbgui"] = self._pbgui.pbgui

    @property
    def config(self): return self._config
    @config.setter
    def config(self, new_value):
        if "backtest" in new_value:
            self.backtest = new_value["backtest"]
        if "bot" in new_value:
            self.bot = new_value["bot"]
        if "live" in new_value:
            self.live = new_value["live"]
        if "optimize" in new_value:
            self.optimize = new_value["optimize"]
        if "pbgui" in new_value:
            self.pbgui = new_value["pbgui"]
        
    
    def load_config(self):
        file =  Path(f'{self._config_file}')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    config = json.load(f)
                self.config = config
            except Exception as e:
                print(f'Error loding v7 config: {e}')
                traceback.print_exc()


    def save_config(self):
        if self._config != None and self._config_file != None:
            file = Path(f'{self._config_file}')
            file.parent.mkdir(parents=True, exist_ok=True)
            with open(file, "w", encoding='utf-8') as f:
                json.dump(self._config, f, indent=4)
    

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
