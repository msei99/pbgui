import streamlit as st
from pathlib import Path
import json
from pbgui_func import validateJSON, config_pretty_str, error_popup
import pbgui_help
import traceback
import multiprocessing
import datetime
from Exchange import Exchange, V7
from PBCoinData import CoinData
from time import sleep
import math

class Config:
    def __init__(self, file_name = None, config = None):
        self._config_file = file_name
        self._long_we = 1.0
        self._short_we = 1.0
        self._long_enabled = True
        self._short_enabled = False
        self._type = None
        self._preview_grid = False
        self._config_v7 = ConfigV7()
        self._config_v7.bot.long.n_positions = 1.0
        self._config_v7.bot.short.n_positions = 1.0
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

    @property
    def config_v7(self):
        if self._config:
            # Check if config is a recursive grid config
            config = json.loads(self._config)
            if "long" in config:
                if not "ddown_factor" in config["long"]:
                    return None
            # long settings
            self._config_v7.bot.long.close_grid_markup_start = json.loads(self._config)["long"]["min_markup"] + json.loads(self._config)["long"]["markup_range"]
            self._config_v7.bot.long.close_grid_markup_end = json.loads(self._config)["long"]["min_markup"]
            self._config_v7.bot.long.close_grid_qty_pct = 1.0 / float(json.loads(self._config)["long"]["n_close_orders"])
            self._config_v7.bot.long.close_trailing_grid_ratio = 0
            self._config_v7.bot.long.close_trailing_qty_pct = 1
            self._config_v7.bot.long.close_trailing_retracement_pct = 0
            self._config_v7.bot.long.close_trailing_threshold_pct = 0
            self._config_v7.bot.long.ema_span_0 = json.loads(self._config)["long"]["ema_span_0"]
            self._config_v7.bot.long.ema_span_1 = json.loads(self._config)["long"]["ema_span_1"]
            self._config_v7.bot.long.entry_grid_double_down_factor = json.loads(self._config)["long"]["ddown_factor"]
            self._config_v7.bot.long.entry_grid_spacing_pct = json.loads(self._config)["long"]["rentry_pprice_dist"]
            self._config_v7.bot.long.entry_grid_spacing_weight = json.loads(self._config)["long"]["rentry_pprice_dist_wallet_exposure_weighting"]
            self._config_v7.bot.long.entry_initial_ema_dist = json.loads(self._config)["long"]["initial_eprice_ema_dist"]
            self._config_v7.bot.long.entry_initial_qty_pct = json.loads(self._config)["long"]["initial_qty_pct"]
            self._config_v7.bot.long.entry_trailing_grid_ratio = 0
            self._config_v7.bot.long.entry_trailing_retracement_pct = 0
            self._config_v7.bot.long.entry_trailing_threshold_pct = 0
            self._config_v7.bot.long.entry_trailing_double_down_factor = 0
            # self._config_v7.bot.long.total_wallet_exposure_limit = json.loads(self._config)["long"]["wallet_exposure_limit"]
            try:
                self._config_v7.bot.long.unstuck_close_pct = json.loads(self._config)["long"]["auto_unstuck_qty_pct"]
            except:
                self._config_v7.bot.long.unstuck_close_pct = 0.025
            self._config_v7.bot.long.unstuck_ema_dist = json.loads(self._config)["long"]["auto_unstuck_ema_dist"]
            # short settings
            self._config_v7.bot.short.close_grid_markup_start = json.loads(self._config)["short"]["min_markup"] + json.loads(self._config)["short"]["markup_range"]
            self._config_v7.bot.short.close_grid_markup_end = json.loads(self._config)["short"]["min_markup"]
            self._config_v7.bot.short.close_grid_qty_pct = 1.0 / float(json.loads(self._config)["short"]["n_close_orders"])
            self._config_v7.bot.short.close_trailing_grid_ratio = 0
            self._config_v7.bot.short.close_trailing_qty_pct = 1
            self._config_v7.bot.short.close_trailing_retracement_pct = 0
            self._config_v7.bot.short.close_trailing_threshold_pct = 0
            self._config_v7.bot.short.ema_span_0 = json.loads(self._config)["short"]["ema_span_0"]
            self._config_v7.bot.short.ema_span_1 = json.loads(self._config)["short"]["ema_span_1"]
            self._config_v7.bot.short.entry_grid_double_down_factor = json.loads(self._config)["short"]["ddown_factor"]
            self._config_v7.bot.short.entry_grid_spacing_pct = json.loads(self._config)["short"]["rentry_pprice_dist"]
            self._config_v7.bot.short.entry_grid_spacing_weight = json.loads(self._config)["short"]["rentry_pprice_dist_wallet_exposure_weighting"]
            self._config_v7.bot.short.entry_initial_ema_dist = json.loads(self._config)["short"]["initial_eprice_ema_dist"]
            self._config_v7.bot.short.entry_initial_qty_pct = json.loads(self._config)["short"]["initial_qty_pct"]
            self._config_v7.bot.short.entry_trailing_grid_ratio = 0
            self._config_v7.bot.short.entry_trailing_retracement_pct = 0
            self._config_v7.bot.short.entry_trailing_threshold_pct = 0
            # self._config_v7.bot.short.total_wallet_exposure_limit = json.loads(self._config)["short"]["wallet_exposure_limit"]
            try:
                self._config_v7.bot.short.unstuck_close_pct = json.loads(self._config)["short"]["auto_unstuck_qty_pct"]
            except:
                self._config_v7.bot.short.unstuck_close_pct = 0.025
            self._config_v7.bot.short.unstuck_ema_dist = json.loads(self._config)["short"]["auto_unstuck_ema_dist"]
            return json.dumps(self._config_v7.config, indent=4)
        return None

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
            self._config_v7.bot.long.total_wallet_exposure_limit = self.long_we
            if self.long_enabled:
                self._config_v7.bot.long.n_positions = 1.0
            else:
                self._config_v7.bot.long.n_positions = 0.0

    @property
    def short_enabled(self): return self._short_enabled

    @short_enabled.setter
    def short_enabled(self, new_short_enabled):
        self._short_enabled = new_short_enabled
        if self._config:
            t = json.loads(self._config)
            t["short"]["enabled"] = self._short_enabled
            self._config = config_pretty_str(t)
            self._config_v7.bot.short.total_wallet_exposure_limit = self.short_we
            if self.short_enabled:
                self._config_v7.bot.short.n_positions = 1.0
            else:
                self._config_v7.bot.short.n_positions = 0.0

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
        self._config_v7.bot.long.total_wallet_exposure_limit = self.long_we
        self._config_v7.bot.short.total_wallet_exposure_limit = self.short_we
        self.long_enabled = json.loads(self._config)["long"]["enabled"]
        self.short_enabled = json.loads(self._config)["short"]["enabled"]
        if not self.long_enabled:
            self._config_v7.bot.long.n_positions = 0.0
        if not self.short_enabled:
            self._config_v7.bot.short.n_positions = 0.0
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
        col1, col2 = st.columns([1,1])
        with col1:
            if color:
                st.text_area(f':{color}[config]', self.config, key="config_instance_config", height=height)
            else:
                st.text_area(f'config', self.config, key="config_instance_config", height=height)
        with col2:
            st.text_area(f'config converted to v7', self.config_v7, key="config_instance_config_v7", height=height, disabled=True)

# config template
# {"backtest": {"base_dir": "backtests",
#               "combine_ohlcvs": true,
#               "compress_cache": true,
#               "end_date": "now",
#               "exchanges": ["binance", "bybit"],
#               "gap_tolerance_ohlcvs_minutes": 120,
#               "start_date": "2020-04-01",
#               "starting_balance": 100000,
#               "use_btc_collateral": true},
#  "bot": {"long": {"close_grid_markup_end": 0.003354,
#                   "close_grid_markup_start": 0.006385,
#                   "close_grid_qty_pct": 0.9429,
#                   "close_trailing_grid_ratio": -0.006244,
#                   "close_trailing_qty_pct": 0.2677,
#                   "close_trailing_retracement_pct": 0.0007421,
#                   "close_trailing_threshold_pct": 0.05089,
#                   "ema_span_0": 279,
#                   "ema_span_1": 475.6,
#                   "enforce_exposure_limit": true,
#                   "entry_grid_double_down_factor": 0.8473,
#                   "entry_grid_spacing_pct": 0.02744,
#                   "entry_grid_spacing_weight": 0.313,
#                   "entry_initial_ema_dist": -0.00364,
#                   "entry_initial_qty_pct": 0.01633,
#                   "entry_trailing_double_down_factor": 3.042,
#                   "entry_trailing_grid_ratio": 0.007105,
#                   "entry_trailing_retracement_pct": 0.01368,
#                   "entry_trailing_threshold_pct": 0.06497,
#                   "filter_noisiness_rolling_window": 39.82,
#                   "filter_volume_drop_pct": 0.5077,
#                   "filter_volume_rolling_window": 1886,
#                   "n_positions": 7.057,
#                   "total_wallet_exposure_limit": 2,
#                   "unstuck_close_pct": 0.04102,
#                   "unstuck_ema_dist": 0.002879,
#                   "unstuck_loss_allowance_pct": 0.002158,
#                   "unstuck_threshold": 0.6771},
#          "short": {"close_grid_markup_end": 0.001698,
#                    "close_grid_markup_start": 0.02009,
#                    "close_grid_qty_pct": 0.05233,
#                    "close_trailing_grid_ratio": -0.1265,
#                    "close_trailing_qty_pct": 0.07382,
#                    "close_trailing_retracement_pct": 0.003562,
#                    "close_trailing_threshold_pct": -0.009199,
#                    "ema_span_0": 1365,
#                    "ema_span_1": 985.1,
#                    "enforce_exposure_limit": true,
#                    "entry_grid_double_down_factor": 3.41,
#                    "entry_grid_spacing_pct": 0.03714,
#                    "entry_grid_spacing_weight": 1.656,
#                    "entry_initial_ema_dist": -0.09702,
#                    "entry_initial_qty_pct": 0.009357,
#                    "entry_trailing_double_down_factor": 0.01,
#                    "entry_trailing_grid_ratio": -0.9565,
#                    "entry_trailing_retracement_pct": 0.08704,
#                    "entry_trailing_threshold_pct": 0.04012,
#                    "filter_noisiness_rolling_window": 335.7,
#                    "filter_volume_drop_pct": 0.5257,
#                    "filter_volume_rolling_window": 2643,
#                    "n_positions": 7.211,
#                    "total_wallet_exposure_limit": 0,
#                    "unstuck_close_pct": 0.074,
#                    "unstuck_ema_dist": -0.02015,
#                    "unstuck_loss_allowance_pct": 0.003297,
#                    "unstuck_threshold": 0.898}},
#  "coin_overrides": {},
#  "live": {"approved_coins": "configs/approved_coins_top23mcap.json",
#           "auto_gs": true,
#           "empty_means_all_approved": true,
#           "execution_delay_seconds": 2,
#           "filter_by_min_effective_cost": true,
#           "forced_mode_long": "",
#           "forced_mode_short": "",
#           "ignored_coins": {"long": [], "short": []},
#           "leverage": 10,
#           "market_orders_allowed": true,
#           "max_n_cancellations_per_batch": 5,
#           "max_n_creations_per_batch": 3,
#           "max_n_restarts_per_day": 10,
#           "mimic_backtest_1m_delay": false,
#           "minimum_coin_age_days": 180,
#           "ohlcvs_1m_rolling_window_days": 4,
#           "ohlcvs_1m_update_after_minutes": 10,
#           "pnls_max_lookback_days": 30,
#           "price_distance_threshold": 0.002,
#           "time_in_force": "good_till_cancelled",
#           "user": "bybit_01"},
#  "optimize": {"bounds": {"long_close_grid_markup_end": [0.001, 0.03],
#                          "long_close_grid_markup_start": [0.001, 0.03],
#                          "long_close_grid_qty_pct": [0.05, 1],
#                          "long_close_trailing_grid_ratio": [-1, 1],
#                          "long_close_trailing_qty_pct": [0.05, 1],
#                          "long_close_trailing_retracement_pct": [0.0001, 0.1],
#                          "long_close_trailing_threshold_pct": [-0.01, 0.1],
#                          "long_ema_span_0": [200, 1440],
#                          "long_ema_span_1": [200, 1440],
#                          "long_entry_grid_double_down_factor": [0.01, 4],
#                          "long_entry_grid_spacing_pct": [0.001, 0.06],
#                          "long_entry_grid_spacing_weight": [0, 10],
#                          "long_entry_initial_ema_dist": [-0.1, 0.003],
#                          "long_entry_initial_qty_pct": [0.004, 0.02],
#                          "long_entry_trailing_double_down_factor": [0.01, 4],
#                          "long_entry_trailing_grid_ratio": [-1, 1],
#                          "long_entry_trailing_retracement_pct": [0.0001, 0.1],
#                          "long_entry_trailing_threshold_pct": [-0.01, 0.1],
#                          "long_filter_noisiness_rolling_window": [10, 360],
#                          "long_filter_volume_drop_pct": [0.5, 1],
#                          "long_filter_volume_rolling_window": [360, 2880],
#                          "long_n_positions": [7, 20],
#                          "long_total_wallet_exposure_limit": [2, 2],
#                          "long_unstuck_close_pct": [0.001, 0.1],
#                          "long_unstuck_ema_dist": [-0.1, 0.01],
#                          "long_unstuck_loss_allowance_pct": [0.001, 0.05],
#                          "long_unstuck_threshold": [0.4, 0.95],
#                          "short_close_grid_markup_end": [0.001, 0.03],
#                          "short_close_grid_markup_start": [0.001, 0.03],
#                          "short_close_grid_qty_pct": [0.05, 1],
#                          "short_close_trailing_grid_ratio": [-1, 1],
#                          "short_close_trailing_qty_pct": [0.05, 1],
#                          "short_close_trailing_retracement_pct": [0.0001, 0.1],
#                          "short_close_trailing_threshold_pct": [-0.01, 0.1],
#                          "short_ema_span_0": [200, 1440],
#                          "short_ema_span_1": [200, 1440],
#                          "short_entry_grid_double_down_factor": [0.01, 4],
#                          "short_entry_grid_spacing_pct": [0.001, 0.06],
#                          "short_entry_grid_spacing_weight": [0, 10],
#                          "short_entry_initial_ema_dist": [-0.1, 0.003],
#                          "short_entry_initial_qty_pct": [0.004, 0.02],
#                          "short_entry_trailing_double_down_factor": [0.01, 4],
#                          "short_entry_trailing_grid_ratio": [-1, 1],
#                          "short_entry_trailing_retracement_pct": [0.0001, 0.1],
#                          "short_entry_trailing_threshold_pct": [-0.01, 0.1],
#                          "short_filter_noisiness_rolling_window": [10, 360],
#                          "short_filter_volume_drop_pct": [0.5, 1],
#                          "short_filter_volume_rolling_window": [360, 2880],
#                          "short_n_positions": [0, 0],
#                          "short_total_wallet_exposure_limit": [0, 0],
#                          "short_unstuck_close_pct": [0.001, 0.1],
#                          "short_unstuck_ema_dist": [-0.1, 0.01],
#                          "short_unstuck_loss_allowance_pct": [0.001, 0.05],
#                          "short_unstuck_threshold": [0.4, 0.95]},
#               "compress_results_file": true,
#               "crossover_probability": 0.64,
#               "enable_overrides": [],
#               "iters": 300000,
#               "limits": {"penalize_if_greater_than_btc_drawdown_worst": 0.5,
#                          "penalize_if_greater_than_loss_profit_ratio": 0.2,
#                          "penalize_if_greater_than_position_unchanged_hours_max": 504},
#               "mutation_probability": 0.34,
#               "n_cpus": 5,
#               "population_size": 1000,
#               "round_to_n_significant_digits": 4,
#               "scoring": ["btc_adg_w",
#                           "btc_mdg_w",
#                           "equity_balance_diff_neg_mean"],
#               "write_all_results": false}}


class Backtest:
    def __init__(self):
        self._base_dir = "backtests"
        self._combine_ohlcvs = True
        self._compress_cache = True
        self._end_date = "now"
        self._exchanges = ["binance", "bybit"]
        self._gap_tolerance_ohlcvs_minutes = 120.0
        self._start_date = "2020-01-01"
        self._starting_balance = 1000.0
        self._use_btc_collateral = False
        self._backtest = {
            "base_dir": self._base_dir,
            "combine_ohlcvs": self._combine_ohlcvs,
            "compress_cache": self._compress_cache,
            "end_date": self._end_date,
            "exchanges": self._exchanges,
            "gap_tolerance_ohlcvs_minutes": self._gap_tolerance_ohlcvs_minutes,
            "start_date": self._start_date,
            "starting_balance": self._starting_balance,
            "use_btc_collateral": self._use_btc_collateral
        }
    
    def __repr__(self):
        return str(self._backtest)
    
    @property
    def backtest(self): return self._backtest
    @backtest.setter
    def backtest(self, new_backtest):
        if "base_dir" in new_backtest:
            self.base_dir = new_backtest["base_dir"]
        if "combine_ohlcvs" in new_backtest:
            self.combine_ohlcvs = new_backtest["combine_ohlcvs"]
        if "compress_cache" in new_backtest:
            self.compress_cache = new_backtest["compress_cache"]
        if "end_date" in new_backtest:
            self.end_date = new_backtest["end_date"]
        if "exchanges" in new_backtest:
            self.exchanges = new_backtest["exchanges"]
        if "gap_tolerance_ohlcvs_minutes" in new_backtest:
            self.gap_tolerance_ohlcvs_minutes = new_backtest["gap_tolerance_ohlcvs_minutes"]
        if "start_date" in new_backtest:
            self.start_date = new_backtest["start_date"]
        if "starting_balance" in new_backtest:
            self.starting_balance = new_backtest["starting_balance"]
        if "use_btc_collateral" in new_backtest:
            self.use_btc_collateral = new_backtest["use_btc_collateral"]
    
    @property
    def base_dir(self): return self._base_dir
    @property
    def combine_ohlcvs(self): return self._combine_ohlcvs
    @property
    def compress_cache(self): return self._compress_cache
    @property
    def end_date(self):
        if self._end_date == "now":
            return datetime.datetime.now().strftime("%Y-%m-%d")
        return self._end_date
    @property
    def exchanges(self): return self._exchanges
    @property
    def gap_tolerance_ohlcvs_minutes(self): return self._gap_tolerance_ohlcvs_minutes
    @property
    def start_date(self): return self._start_date
    @property
    def starting_balance(self): return self._starting_balance
    @property
    def use_btc_collateral(self): return self._use_btc_collateral

    @base_dir.setter
    def base_dir(self, new_base_dir):
        self._base_dir = new_base_dir
        self._backtest["base_dir"] = self._base_dir
    @combine_ohlcvs.setter
    def combine_ohlcvs(self, new_combine_ohlcvs):
        self._combine_ohlcvs = new_combine_ohlcvs
        self._backtest["combine_ohlcvs"] = self._combine_ohlcvs
    @compress_cache.setter
    def compress_cache(self, new_compress_cache):
        self._compress_cache = new_compress_cache
        self._backtest["compress_cache"] = self._compress_cache
    @end_date.setter
    def end_date(self, new_end_date):
        self._end_date = new_end_date
        self._backtest["end_date"] = self._end_date
    @exchanges.setter
    def exchanges(self, new_exchanges):
        self._exchanges = new_exchanges
        self._backtest["exchanges"] = self._exchanges
    @gap_tolerance_ohlcvs_minutes.setter
    def gap_tolerance_ohlcvs_minutes(self, new_gap_tolerance_ohlcvs_minutes):
        self._gap_tolerance_ohlcvs_minutes = new_gap_tolerance_ohlcvs_minutes
        self._backtest["gap_tolerance_ohlcvs_minutes"] = self._gap_tolerance_ohlcvs_minutes
    @start_date.setter
    def start_date(self, new_start_date):
        self._start_date = new_start_date
        self._backtest["start_date"] = self._start_date
    @starting_balance.setter
    def starting_balance(self, new_starting_balance):
        self._starting_balance = new_starting_balance
        self._backtest["starting_balance"] = self._starting_balance
    @use_btc_collateral.setter
    def use_btc_collateral(self, new_use_btc_collateral):
        self._use_btc_collateral = new_use_btc_collateral
        self._backtest["use_btc_collateral"] = self._use_btc_collateral

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
    
    @st.fragment
    def edit(self):
        # Init session_state for keys
        if "edit_configv7_long_twe" in st.session_state:
            if st.session_state.edit_configv7_long_twe != self.long.total_wallet_exposure_limit:
                self.long.total_wallet_exposure_limit = round(st.session_state.edit_configv7_long_twe,2)
                st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
            if "edit_configv7_long" in st.session_state:
                try:
                    long = json.loads(st.session_state.edit_configv7_long)
                    if st.session_state.edit_configv7_long_twe != float(long["total_wallet_exposure_limit"]):
                        st.session_state.edit_configv7_long_twe = float(long["total_wallet_exposure_limit"])
                except:
                    st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
                    error_popup("Invalid JSON long | RESET")
        else:
            st.session_state.edit_configv7_long_twe = float(self.long.total_wallet_exposure_limit)

        if "edit_configv7_long_positions" in st.session_state:
            if st.session_state.edit_configv7_long_positions != self.long.n_positions:
                self.long.n_positions = round(st.session_state.edit_configv7_long_positions,0)
                st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
            if "edit_configv7_long" in st.session_state:
                try:
                    long = json.loads(st.session_state.edit_configv7_long)
                    if st.session_state.edit_configv7_long_positions != float(long["n_positions"]):
                        st.session_state.edit_configv7_long_positions = float(long["n_positions"])
                except:
                    st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
                    error_popup("Invalid JSON long | RESET")
        else:
            st.session_state.edit_configv7_long_positions = float(self.long.n_positions)

        if "edit_configv7_short_twe" in st.session_state:
            if st.session_state.edit_configv7_short_twe != self.short.total_wallet_exposure_limit:
                self.short.total_wallet_exposure_limit = round(st.session_state.edit_configv7_short_twe,2)
                st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
            if "edit_configv7_short" in st.session_state:
                try:
                    short = json.loads(st.session_state.edit_configv7_short)
                    if st.session_state.edit_configv7_short_twe != float(short["total_wallet_exposure_limit"]):
                        st.session_state.edit_configv7_short_twe = float(short["total_wallet_exposure_limit"])
                except:
                    st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
                    error_popup("Invalid JSON short | RESET")
        else:
            st.session_state.edit_configv7_short_twe = float(self.short.total_wallet_exposure_limit)

        if "edit_configv7_short_positions" in st.session_state:
            if st.session_state.edit_configv7_short_positions != self.short.n_positions:
                self.short.n_positions = round(st.session_state.edit_configv7_short_positions,0)
                st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
            if "edit_configv7_short" in st.session_state:
                try:
                    short = json.loads(st.session_state.edit_configv7_short)
                    if st.session_state.edit_configv7_short_positions != float(short["n_positions"]):
                        st.session_state.edit_configv7_short_positions = float(short["n_positions"])
                except:
                    st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
                    error_popup("Invalid JSON short | RESET")   
        else:
            st.session_state.edit_configv7_short_positions = float(self.short.n_positions)

        if "edit_configv7_long" in st.session_state:
            if st.session_state.edit_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_configv7_long)
                except:
                    st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)
                    error_popup("Invalid JSON long | RESET")
        else:
            st.session_state.edit_configv7_long = json.dumps(self.bot["long"], indent=4)

        if "edit_configv7_short" in st.session_state:
            if st.session_state.edit_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_configv7_short)
                except:
                    st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
                    error_popup("Invalid JSON short | RESET")
        else:
            st.session_state.edit_configv7_short = json.dumps(self.bot["short"], indent=4)
        # Display config
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("long twe", min_value=0.0, max_value=100.0, step=0.05, format="%.2f", key="edit_configv7_long_twe", help=pbgui_help.total_wallet_exposure_limit)
        with col2:
            st.number_input("long positions", min_value=0.0, max_value=100.0, step=1.0, format="%.2f", key="edit_configv7_long_positions", help=pbgui_help.n_positions)
        with col3:
            st.number_input("short twe", min_value=0.0, max_value=100.0, step=0.05, format="%.2f", key="edit_configv7_short_twe", help=pbgui_help.total_wallet_exposure_limit)
        with col4:
            st.number_input("short positions", min_value=0.0, max_value=100.0, step=1.0, format="%.2f", key="edit_configv7_short_positions", help=pbgui_help.n_positions)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', key="edit_configv7_long", height=600)
        with col2:
            st.text_area(f'short', key="edit_configv7_short", height=600)

    def edit_cf(self):
        # Init session_state for keys
        if "edit_cf_configv7_long" in st.session_state:
            if st.session_state.edit_cf_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_cf_configv7_long)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_cf_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_cf_configv7_short" in st.session_state:
            if st.session_state.edit_cf_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_cf_configv7_short)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_cf_configv7_short = json.dumps(self.bot["short"], indent=4)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', key="edit_cf_configv7_long", height=640)
        with col2:
            st.text_area(f'short', key="edit_cf_configv7_short", height=640)
    
    def edit_co(self):
        # Init session_state for keys
        if "edit_co_configv7_long" in st.session_state:
            if st.session_state.edit_co_configv7_long != json.dumps(self.bot["long"], indent=4):
                try:
                    self.long = json.loads(st.session_state.edit_co_configv7_long)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_co_configv7_long = json.dumps(self.bot["long"], indent=4)
        if "edit_co_configv7_short" in st.session_state:
            if st.session_state.edit_co_configv7_short != json.dumps(self.bot["short"], indent=4):
                try:
                    self.short = json.loads(st.session_state.edit_co_configv7_short)
                except:
                    error_popup("Invalid JSON | RESET")
        else:
            st.session_state.edit_co_configv7_short = json.dumps(self.bot["short"], indent=4)
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_area(f'long', key="edit_co_configv7_long", height=640)
        with col2:
            st.text_area(f'short', key="edit_co_configv7_short", height=640)

class Long:
    def __init__(self):
        # self._close_grid_markup_range = 0.0015976
        # self._close_grid_min_markup = 0.012839
        self._close_grid_markup_end = 0.001161
        self._close_grid_markup_start = 0.009675
        self._close_grid_qty_pct = 0.8195
        self._close_trailing_grid_ratio = 0.042114
        self._close_trailing_qty_pct = 1
        self._close_trailing_retracement_pct = 0.066097
        self._close_trailing_threshold_pct = 0.06726
        self._ema_span_0 = 469.02
        self._ema_span_1 = 1118.9
        self._enforce_exposure_limit = True
        self._entry_grid_double_down_factor = 2.3738
        self._entry_grid_spacing_pct = 0.052372
        self._entry_grid_spacing_weight = 0.17715
        self._entry_initial_ema_dist = -0.0060574
        self._entry_initial_qty_pct = 0.019955
        self._entry_trailing_double_down_factor = 1.441
        self._entry_trailing_grid_ratio = -0.28053
        self._entry_trailing_retracement_pct = 0.0024762
        self._entry_trailing_threshold_pct = 0.014956
        self._filter_volume_drop_pct = 0.7021
        self._filter_volume_rolling_window = 303.6
        self._filter_noisiness_rolling_window = 303.6
        self._n_positions = 9.6662
        self._total_wallet_exposure_limit = 0.8536
        self._unstuck_close_pct = 0.049593
        self._unstuck_ema_dist = -0.051669
        self._unstuck_loss_allowance_pct = 0.044329
        self._unstuck_threshold = 0.46953
        self._long = {
            "close_grid_markup_end": self._close_grid_markup_end,
            "close_grid_markup_start": self._close_grid_markup_start,
            # "close_grid_markup_range": self._close_grid_markup_range,
            # "close_grid_min_markup": self._close_grid_min_markup,
            "close_grid_qty_pct": self._close_grid_qty_pct,
            "close_trailing_grid_ratio": self._close_trailing_grid_ratio,
            "close_trailing_qty_pct": self._close_trailing_qty_pct,
            "close_trailing_retracement_pct": self._close_trailing_retracement_pct,
            "close_trailing_threshold_pct": self._close_trailing_threshold_pct,
            "ema_span_0": self._ema_span_0,
            "ema_span_1": self._ema_span_1,
            "enforce_exposure_limit": self._enforce_exposure_limit,
            "entry_grid_double_down_factor": self._entry_grid_double_down_factor,
            "entry_grid_spacing_pct": self._entry_grid_spacing_pct,
            "entry_grid_spacing_weight": self._entry_grid_spacing_weight,
            "entry_initial_ema_dist": self._entry_initial_ema_dist,
            "entry_initial_qty_pct": self._entry_initial_qty_pct,
            "entry_trailing_double_down_factor": self._entry_trailing_double_down_factor,
            "entry_trailing_grid_ratio": self._entry_trailing_grid_ratio,
            "entry_trailing_retracement_pct": self._entry_trailing_retracement_pct,
            "entry_trailing_threshold_pct": self._entry_trailing_threshold_pct,
            "filter_noisiness_rolling_window": self._filter_noisiness_rolling_window,
            "filter_volume_drop_pct": self._filter_volume_drop_pct,
            "filter_volume_rolling_window": self._filter_volume_rolling_window,
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
        #Fix for old markup parameters
        if "close_grid_markup_range" in new_long and "close_grid_min_markup" in new_long:
            self.close_grid_markup_start = new_long["close_grid_min_markup"] + new_long["close_grid_markup_range"]
            self.close_grid_markup_end = new_long["close_grid_min_markup"]
        if "close_grid_markup_end" in new_long:
            self.close_grid_markup_end = new_long["close_grid_markup_end"]
        if "close_grid_markup_start" in new_long:
            self.close_grid_markup_start = new_long["close_grid_markup_start"]
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
        if "enforce_exposure_limit" in new_long:
            self.enforce_exposure_limit = new_long["enforce_exposure_limit"]
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
        if "entry_trailing_double_down_factor" in new_long:
            self.entry_trailing_double_down_factor = new_long["entry_trailing_double_down_factor"]
        else:
            self.entry_trailing_double_down_factor = self.entry_grid_double_down_factor
        if "entry_trailing_grid_ratio" in new_long:
            self.entry_trailing_grid_ratio = new_long["entry_trailing_grid_ratio"]
        if "entry_trailing_retracement_pct" in new_long:
            self.entry_trailing_retracement_pct = new_long["entry_trailing_retracement_pct"]
        if "entry_trailing_threshold_pct" in new_long:
            self.entry_trailing_threshold_pct = new_long["entry_trailing_threshold_pct"]
        if "filter_noisiness_rolling_window" in new_long:
            self.filter_noisiness_rolling_window = new_long["filter_noisiness_rolling_window"]
        # Fix for old configs
        elif "filter_rolling_window" in new_long:
            self.filter_noisiness_rolling_window = new_long["filter_rolling_window"]
        if "filter_volume_drop_pct" in new_long:
            self.filter_volume_drop_pct = new_long["filter_volume_drop_pct"]
        # Fix for old configs
        elif "filter_relative_volume_clip_pct" in new_long:
            self.filter_volume_drop_pct = new_long["filter_relative_volume_clip_pct"]
        if "filter_volume_rolling_window" in new_long:
            self.filter_volume_rolling_window = new_long["filter_volume_rolling_window"]
        # Fix for old configs
        elif "filter_rolling_window" in new_long:
            self.filter_volume_rolling_window = new_long["filter_rolling_window"]
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

    # @property
    # def close_grid_markup_range(self): return self._close_grid_markup_range
    # @property
    # def close_grid_min_markup(self): return self._close_grid_min_markup
    @property
    def close_grid_markup_end(self): return self._close_grid_markup_end
    @property
    def close_grid_markup_start(self): return self._close_grid_markup_start
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
    def enforce_exposure_limit(self): return self._enforce_exposure_limit
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
    def entry_trailing_double_down_factor(self): return self._entry_trailing_double_down_factor
    @property
    def entry_trailing_grid_ratio(self): return self._entry_trailing_grid_ratio
    @property
    def entry_trailing_retracement_pct(self): return self._entry_trailing_retracement_pct
    @property
    def entry_trailing_threshold_pct(self): return self._entry_trailing_threshold_pct
    @property
    def filter_noisiness_rolling_window(self): return self._filter_noisiness_rolling_window
    @property
    def filter_volume_drop_pct(self): return self._filter_volume_drop_pct
    @property
    def filter_volume_rolling_window(self): return self._filter_volume_rolling_window
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

    @close_grid_markup_end.setter
    def close_grid_markup_end(self, new_close_grid_markup_end):
        self._close_grid_markup_end = new_close_grid_markup_end
        self._long["close_grid_markup_end"] = self._close_grid_markup_end
    @close_grid_markup_start.setter
    def close_grid_markup_start(self, new_close_grid_markup_start):
        self._close_grid_markup_start = new_close_grid_markup_start
        self._long["close_grid_markup_start"] = self._close_grid_markup_start
    # @close_grid_markup_range.setter
    # def close_grid_markup_range(self, new_close_grid_markup_range):
    #     self._close_grid_markup_range = new_close_grid_markup_range
    #     self._long["close_grid_markup_range"] = self._close_grid_markup_range
    # @close_grid_min_markup.setter
    # def close_grid_min_markup(self, new_close_grid_min_markup):
    #     self._close_grid_min_markup = new_close_grid_min_markup
    #     self._long["close_grid_min_markup"] = self._close_grid_min_markup
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
    @enforce_exposure_limit.setter
    def enforce_exposure_limit(self, new_enforce_exposure_limit):
        self._enforce_exposure_limit = new_enforce_exposure_limit
        self._long["enforce_exposure_limit"] = self._enforce_exposure_limit
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
    @entry_trailing_double_down_factor.setter
    def entry_trailing_double_down_factor(self, new_entry_trailing_double_down_factor):
        self._entry_trailing_double_down_factor = new_entry_trailing_double_down_factor
        self._long["entry_trailing_double_down_factor"] = self._entry_trailing_double_down_factor
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
    @filter_noisiness_rolling_window.setter
    def filter_noisiness_rolling_window(self, new_filter_noisiness_rolling_window):
        self._filter_noisiness_rolling_window = new_filter_noisiness_rolling_window
        self._long["filter_noisiness_rolling_window"] = self._filter_noisiness_rolling_window
    @filter_volume_drop_pct.setter
    def filter_volume_drop_pct(self, new_filter_volume_drop_pct):
        self._filter_volume_drop_pct = new_filter_volume_drop_pct
        self._long["filter_volume_drop_pct"] = self._filter_volume_drop_pct
    @filter_volume_rolling_window.setter
    def filter_volume_rolling_window(self, new_filter_volume_rolling_window):
        self._filter_volume_rolling_window = new_filter_volume_rolling_window
        self._long["filter_volume_rolling_window"] = self._filter_volume_rolling_window
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
        # self._close_grid_markup_range = 0.028266
        # self._close_grid_min_markup = 0.013899
        self._close_grid_markup_end = 0.001
        self._close_grid_markup_start = 0.001
        self._close_grid_qty_pct = 0.05
        self._close_trailing_grid_ratio = 0.93658
        self._close_trailing_qty_pct = 1
        self._close_trailing_retracement_pct = 0.098179
        self._close_trailing_threshold_pct = -0.059383
        self._ema_span_0 = 794.32
        self._ema_span_1 = 1176.7
        self._enforce_exposure_limit = True
        self._entry_grid_double_down_factor = 2.1256
        self._entry_grid_spacing_pct = 0.072906
        self._entry_grid_spacing_weight = 0.98867
        self._entry_initial_ema_dist = -0.060333
        self._entry_initial_qty_pct = 0.066426
        self._entry_trailing_double_down_factor = 0.72508
        self._entry_trailing_grid_ratio = -0.026647
        self._entry_trailing_retracement_pct = 0.016626
        self._entry_trailing_threshold_pct = 0.052728
        self._filter_noisiness_rolling_window = 320.18
        self._filter_volume_drop_pct = 0.57973
        self._filter_volume_rolling_window = 320.18
        self._n_positions = 0.0
        self._total_wallet_exposure_limit = 0.0
        self._unstuck_close_pct = 0.052992
        self._unstuck_ema_dist = -0.0465
        self._unstuck_loss_allowance_pct = 0.045415
        self._unstuck_threshold = 0.92228
        self._short = {
            "close_grid_markup_end": self._close_grid_markup_end,
            "close_grid_markup_start": self._close_grid_markup_start,
            # "close_grid_markup_range": self._close_grid_markup_range,
            # "close_grid_min_markup": self._close_grid_min_markup,
            "close_grid_qty_pct": self._close_grid_qty_pct,
            "close_trailing_grid_ratio": self._close_trailing_grid_ratio,
            "close_trailing_qty_pct": self._close_trailing_qty_pct,
            "close_trailing_retracement_pct": self._close_trailing_retracement_pct,
            "close_trailing_threshold_pct": self._close_trailing_threshold_pct,
            "ema_span_0": self._ema_span_0,
            "ema_span_1": self._ema_span_1,
            "enforce_exposure_limit": self._enforce_exposure_limit,
            "entry_grid_double_down_factor": self._entry_grid_double_down_factor,
            "entry_grid_spacing_pct": self._entry_grid_spacing_pct,
            "entry_grid_spacing_weight": self._entry_grid_spacing_weight,
            "entry_initial_ema_dist": self._entry_initial_ema_dist,
            "entry_initial_qty_pct": self._entry_initial_qty_pct,
            "entry_trailing_double_down_factor": self._entry_trailing_double_down_factor,
            "entry_trailing_grid_ratio": self._entry_trailing_grid_ratio,
            "entry_trailing_retracement_pct": self._entry_trailing_retracement_pct,
            "entry_trailing_threshold_pct": self._entry_trailing_threshold_pct,
            "filter_noisiness_rolling_window": self._filter_noisiness_rolling_window,
            "filter_volume_drop_pct": self._filter_volume_drop_pct,
            "filter_volume_rolling_window": self._filter_volume_rolling_window,
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
        #Fix for old markup parameters
        if "close_grid_markup_range" in new_short and "close_grid_min_markup" in new_short:
            self.close_grid_markup_start = new_short["close_grid_min_markup"] + new_short["close_grid_markup_range"]
            self.close_grid_markup_end = new_short["close_grid_min_markup"]
        if "close_grid_markup_end" in new_short:
            self.close_grid_markup_end = new_short["close_grid_markup_end"]
        if "close_grid_markup_start" in new_short:
            self.close_grid_markup_start = new_short["close_grid_markup_start"]
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
        if "enforce_exposure_limit" in new_short:
            self.enforce_exposure_limit = new_short["enforce_exposure_limit"]
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
        if "entry_trailing_double_down_factor" in new_short:
            self.entry_trailing_double_down_factor = new_short["entry_trailing_double_down_factor"]
        else:
            self.entry_trailing_double_down_factor = self.entry_grid_double_down_factor
        if "entry_trailing_grid_ratio" in new_short:
            self.entry_trailing_grid_ratio = new_short["entry_trailing_grid_ratio"]
        if "entry_trailing_retracement_pct" in new_short:
            self.entry_trailing_retracement_pct = new_short["entry_trailing_retracement_pct"]
        if "entry_trailing_threshold_pct" in new_short:
            self.entry_trailing_threshold_pct = new_short["entry_trailing_threshold_pct"]
        if "filter_noisiness_rolling_window" in new_short:
            self.filter_noisiness_rolling_window = new_short["filter_noisiness_rolling_window"]
        # Fix for old configs
        elif "filter_rolling_window" in new_short:
            self.filter_noisiness_rolling_window = new_short["filter_rolling_window"]
        if "filter_volume_drop_pct" in new_short:
            self.filter_volume_drop_pct = new_short["filter_volume_drop_pct"]
        # Fix for old configs
        elif "filter_relative_volume_clip_pct" in new_short:
            self.filter_volume_drop_pct = new_short["filter_relative_volume_clip_pct"]
        if "filter_volume_rolling_window" in new_short:
            self.filter_volume_rolling_window = new_short["filter_volume_rolling_window"]
        # Fix for old configs
        elif "filter_rolling_window" in new_short:
            self.filter_volume_rolling_window = new_short["filter_rolling_window"]
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
    def close_grid_markup_end(self): return self._close_grid_markup_end
    @property
    def close_grid_markup_start(self): return self._close_grid_markup_start
    # @property
    # def close_grid_markup_range(self): return self._close_grid_markup_range
    # @property
    # def close_grid_min_markup(self): return self._close_grid_min_markup
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
    def enforce_exposure_limit(self): return self._enforce_exposure_limit
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
    def entry_trailing_double_down_factor(self): return self._entry_trailing_double_down_factor
    @property
    def entry_trailing_grid_ratio(self): return self._entry_trailing_grid_ratio
    @property
    def entry_trailing_retracement_pct(self): return self._entry_trailing_retracement_pct
    @property
    def entry_trailing_threshold_pct(self): return self._entry_trailing_threshold_pct
    @property
    def filter_noisiness_rolling_window(self): return self._filter_noisiness_rolling_window
    @property
    def filter_volume_drop_pct(self): return self._filter_volume_drop_pct
    @property
    def filter_volume_rolling_window(self): return self._filter_volume_rolling_window
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

    @close_grid_markup_end.setter
    def close_grid_markup_end(self, new_close_grid_markup_end):
        self._close_grid_markup_end = new_close_grid_markup_end
        self._short["close_grid_markup_end"] = self._close_grid_markup_end
    @close_grid_markup_start.setter
    def close_grid_markup_start(self, new_close_grid_markup_start):
        self._close_grid_markup_start = new_close_grid_markup_start
        self._short["close_grid_markup_start"] = self._close_grid_markup_start
    # @close_grid_markup_range.setter
    # def close_grid_markup_range(self, new_close_grid_markup_range):
    #     self._close_grid_markup_range = new_close_grid_markup_range
    #     self._short["close_grid_markup_range"] = self._close_grid_markup_range
    # @close_grid_min_markup.setter
    # def close_grid_min_markup(self, new_close_grid_min_markup):
    #     self._close_grid_min_markup = new_close_grid_min_markup
    #     self._short["close_grid_min_markup"] = self._close_grid_min_markup
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
    @enforce_exposure_limit.setter
    def enforce_exposure_limit(self, new_enforce_exposure_limit):
        self._enforce_exposure_limit = new_enforce_exposure_limit
        self._short["enforce_exposure_limit"] = self._enforce_exposure_limit
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
    @entry_trailing_double_down_factor.setter
    def entry_trailing_double_down_factor(self, new_entry_trailing_double_down_factor):
        self._entry_trailing_double_down_factor = new_entry_trailing_double_down_factor
        self._short["entry_trailing_double_down_factor"] = self._entry_trailing_double_down_factor
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
    @filter_noisiness_rolling_window.setter
    def filter_noisiness_rolling_window(self, new_filter_noisiness_rolling_window):
        self._filter_noisiness_rolling_window = new_filter_noisiness_rolling_window
        self._short["filter_noisiness_rolling_window"] = self._filter_noisiness_rolling_window
    @filter_volume_drop_pct.setter
    def filter_volume_drop_pct(self, new_filter_volume_drop_pct):
        self._filter_volume_drop_pct = new_filter_volume_drop_pct
        self._short["filter_volume_drop_pct"] = self._filter_volume_drop_pct
    @filter_volume_rolling_window.setter
    def filter_volume_rolling_window(self, new_filter_volume_rolling_window):
        self._filter_volume_rolling_window = new_filter_volume_rolling_window
        self._short["filter_volume_rolling_window"] = self._filter_volume_rolling_window
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
        # Add 'USDT' to each coin if it does not already end with 'USDT'
        updated_long = [
            coin if coin.endswith("USDT") or coin.endswith("USDC") else coin + "USDT"
            for coin in new_long
        ]
        self._long = updated_long
        self._approved_coins["long"] = self._long
    @short.setter
    def short(self, new_short):
        # Add 'USDT' to each coin if it does not already end with 'USDT'
        updated_short = [
            coin if coin.endswith("USDT") or coin.endswith("USDC") else coin + "USDT"
            for coin in new_short
        ]
        self._short = updated_short
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
        # self._coin_flags = {}
        self._empty_means_all_approved = False
        self._execution_delay_seconds = 2.0
        self._filter_by_min_effective_cost = True
        self._forced_mode_long = ""
        self._forced_mode_short = ""
        self._ignored_coins = IgnoredCoins()
        self._leverage = 10.0
        self._market_orders_allowed = True
        self._max_n_cancellations_per_batch = 5
        self._max_n_creations_per_batch = 3
        self._max_n_restarts_per_day = 10
        self._mimic_backtest_1m_delay = False
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
            # "coin_flags": self._coin_flags,
            "empty_means_all_approved": self._empty_means_all_approved,
            "execution_delay_seconds": self._execution_delay_seconds,
            "filter_by_min_effective_cost": self._filter_by_min_effective_cost,
            "forced_mode_long": self._forced_mode_long,
            "forced_mode_short": self._forced_mode_short,
            "ignored_coins": self._ignored_coins._ignored_coins,
            "leverage": self._leverage,
            "market_orders_allowed": self._market_orders_allowed,
            "max_n_cancellations_per_batch": self._max_n_cancellations_per_batch,
            "max_n_creations_per_batch": self._max_n_creations_per_batch,
            "max_n_restarts_per_day": self._max_n_restarts_per_day,
            "mimic_backtest_1m_delay": self._mimic_backtest_1m_delay,
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
        # if "coin_flags" in new_live:
        #     self.coin_flags = new_live["coin_flags"]
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
        if "market_orders_allowed" in new_live:
            self.market_orders_allowed = new_live["market_orders_allowed"]
        if "max_n_cancellations_per_batch" in new_live:
            self.max_n_cancellations_per_batch = new_live["max_n_cancellations_per_batch"]
        if "max_n_creations_per_batch" in new_live:
            self.max_n_creations_per_batch = new_live["max_n_creations_per_batch"]
        if "max_n_restarts_per_day" in new_live:
            self.max_n_restarts_per_day = new_live["max_n_restarts_per_day"]
        if "mimic_backtest_1m_delay" in new_live:
            self.mimic_backtest_1m_delay = new_live["mimic_backtest_1m_delay"]
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
    # @property
    # def coin_flags(self): return self._coin_flags
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
    def market_orders_allowed(self): return self._market_orders_allowed
    @property
    def max_n_cancellations_per_batch(self): return self._max_n_cancellations_per_batch
    @property
    def max_n_creations_per_batch(self): return self._max_n_creations_per_batch
    @property
    def max_n_restarts_per_day(self): return self._max_n_restarts_per_day
    @property
    def mimic_backtest_1m_delay(self): return self._mimic_backtest_1m_delay
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
    # @coin_flags.setter
    # def coin_flags(self, new_coin_flags):
    #     self._coin_flags = new_coin_flags
    #     self._live["coin_flags"] = self._coin_flags
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
    @market_orders_allowed.setter
    def market_orders_allowed(self, new_market_orders_allowed):
        self._market_orders_allowed = new_market_orders_allowed
        self._live["market_orders_allowed"] = self._market_orders_allowed
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
    @mimic_backtest_1m_delay.setter
    def mimic_backtest_1m_delay(self, new_mimic_backtest_1m_delay):
        self._mimic_backtest_1m_delay = new_mimic_backtest_1m_delay
        self._live["mimic_backtest_1m_delay"] = self._mimic_backtest_1m_delay
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
        self._limits = {}
        # optimize
        self._compress_results_file = True
        self._crossover_probability = 0.7
        self._enable_overrides = []
        self._iters = 100000
        self._mutation_probability = 0.2
        self._n_cpus = 5
        self._population_size = 500
        self._round_to_n_significant_digits = 5
        # scoring
        self._scoring = ["loss_profit_ratio", "mdg_w", "sharpe_ratio"]

        self._optimize = {
            "bounds": self._bounds._bounds,
            "compress_results_file": self._compress_results_file,
            "crossover_probability": self._crossover_probability,
            "enable_overrides": self._enable_overrides,
            "iters": self._iters,
            "limits": self._limits,
            "mutation_probability": self._mutation_probability,
            "n_cpus": self._n_cpus,
            "population_size": self._population_size,
            "round_to_n_significant_digits": self._round_to_n_significant_digits,
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
        if "compress_results_file" in new_optimize:
            self.compress_results_file = new_optimize["compress_results_file"]
        if "crossover_probability" in new_optimize:
            self.crossover_probability = new_optimize["crossover_probability"]
        if "enable_overrides" in new_optimize:
            self.enable_overrides = new_optimize["enable_overrides"]
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
        if "round_to_n_significant_digits" in new_optimize:
            self.round_to_n_significant_digits = new_optimize["round_to_n_significant_digits"]
        if "scoring" in new_optimize:
            self.scoring = new_optimize["scoring"]
    
    @property
    def bounds(self): return self._bounds
    @property
    def compress_results_file(self): return self._compress_results_file
    @property
    def limits(self): return self._limits
    @property
    def crossover_probability(self): return self._crossover_probability
    @property
    def enable_overrides(self): return self._enable_overrides
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
    def round_to_n_significant_digits(self): return self._round_to_n_significant_digits
    @property
    def scoring(self): return self._scoring

    @bounds.setter
    def bounds(self, new_bounds):
        self._bounds.bounds = new_bounds
        self._optimize["bounds"] = self._bounds.bounds
    @compress_results_file.setter
    def compress_results_file(self, new_compress_results_file):
        self._compress_results_file = new_compress_results_file
        self._optimize["compress_results_file"] = self._compress_results_file
    @limits.setter
    def limits(self, new_limits):
        self._limits = new_limits
        self._optimize["limits"] = self._limits
    @crossover_probability.setter
    def crossover_probability(self, new_crossover_probability):
        self._crossover_probability = new_crossover_probability
        self._optimize["crossover_probability"] = self._crossover_probability
    @enable_overrides.setter
    def enable_overrides(self, new_enable_overrides):
        self._enable_overrides = new_enable_overrides
        self._optimize["enable_overrides"] = self._enable_overrides
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
    @round_to_n_significant_digits.setter
    def round_to_n_significant_digits(self, new_round_to_n_significant_digits):
        self._round_to_n_significant_digits = new_round_to_n_significant_digits
        self._optimize["round_to_n_significant_digits"] = self._round_to_n_significant_digits
    @scoring.setter
    def scoring(self, new_scoring):
        self._scoring = new_scoring
        self._optimize["scoring"] = self._scoring

class Bounds:

    CLOSE_GRID_MARKUP_END_MIN = 0.0
    CLOSE_GRID_MARKUP_END_MAX = 1.0
    CLOSE_GRID_MARKUP_END_STEP = 0.001
    CLOSE_GRID_MARKUP_END_ROUND = 3
    CLOSE_GRID_MARKUP_END_FORMAT = f'%.{CLOSE_GRID_MARKUP_END_ROUND}f'

    CLOSE_GRID_MARKUP_START_MIN = 0.0
    CLOSE_GRID_MARKUP_START_MAX = 1.0
    CLOSE_GRID_MARKUP_START_STEP = 0.001
    CLOSE_GRID_MARKUP_START_ROUND = 3
    CLOSE_GRID_MARKUP_START_FORMAT = f'%.{CLOSE_GRID_MARKUP_START_ROUND}f'
    
    # CLOSE_GRID_MARKUP_RANGE_MIN = 0.0
    # CLOSE_GRID_MARKUP_RANGE_MAX = 1.0
    # CLOSE_GRID_MARKUP_RANGE_STEP = 0.01
    # CLOSE_GRID_MARKUP_RANGE_ROUND = 2
    # CLOSE_GRID_MARKUP_RANGE_FORMAT = f'%.{CLOSE_GRID_MARKUP_RANGE_ROUND}f'

    # CLOSE_GRID_MIN_MARKUP_MIN = 0.0
    # CLOSE_GRID_MIN_MARKUP_MAX = 1.0
    # CLOSE_GRID_MIN_MARKUP_STEP = 0.001
    # CLOSE_GRID_MIN_MARKUP_ROUND = 3
    # CLOSE_GRID_MIN_MARKUP_FORMAT = f'%.{CLOSE_GRID_MIN_MARKUP_ROUND}f'

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
    CLOSE_TRAILING_QTY_PCT_STEP = 0.001
    CLOSE_TRAILING_QTY_PCT_ROUND = 3
    CLOSE_TRAILING_QTY_PCT_FORMAT = f'%.{CLOSE_TRAILING_QTY_PCT_ROUND}f'

    CLOSE_TRAILING_RETRACEMENT_PCT_MIN = 0.0
    CLOSE_TRAILING_RETRACEMENT_PCT_MAX = 1.0
    CLOSE_TRAILING_RETRACEMENT_PCT_STEP = 0.001
    CLOSE_TRAILING_RETRACEMENT_PCT_ROUND = 3
    CLOSE_TRAILING_RETRACEMENT_PCT_FORMAT = f'%.{CLOSE_TRAILING_RETRACEMENT_PCT_ROUND}f'

    CLOSE_TRAILING_THRESHOLD_PCT_MIN = -1.0
    CLOSE_TRAILING_THRESHOLD_PCT_MAX = 1.0
    CLOSE_TRAILING_THRESHOLD_PCT_STEP = 0.0001
    CLOSE_TRAILING_THRESHOLD_PCT_ROUND = 4
    CLOSE_TRAILING_THRESHOLD_PCT_FORMAT = f'%.{CLOSE_TRAILING_THRESHOLD_PCT_ROUND}f'

    EMA_SPAN_0_MIN = 1.0
    EMA_SPAN_0_MAX = 10000.0
    EMA_SPAN_0_STEP = 1.0
    EMA_SPAN_0_ROUND = 1
    EMA_SPAN_0_FORMAT = f'%.{EMA_SPAN_0_ROUND}f'

    EMA_SPAN_1_MIN = 1.0
    EMA_SPAN_1_MAX = 10000.0
    EMA_SPAN_1_STEP = 1.0
    EMA_SPAN_1_ROUND = 1
    EMA_SPAN_1_FORMAT = f'%.{EMA_SPAN_1_ROUND}f'

    ENTRY_GRID_DOUBLE_DOWN_FACTOR_MIN = 0.0
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_MAX = 10.0
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_STEP = 0.05
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_ROUND = 2
    ENTRY_GRID_DOUBLE_DOWN_FACTOR_FORMAT = f'%.{ENTRY_GRID_DOUBLE_DOWN_FACTOR_ROUND}f'

    ENTRY_GRID_SPACING_PCT_MIN = 0.0
    ENTRY_GRID_SPACING_PCT_MAX = 1.0
    ENTRY_GRID_SPACING_PCT_STEP = 0.001
    ENTRY_GRID_SPACING_PCT_ROUND = 3
    ENTRY_GRID_SPACING_PCT_FORMAT = f'%.{ENTRY_GRID_SPACING_PCT_ROUND}f'

    ENTRY_GRID_SPACING_WEIGHT_MIN = 0.0
    ENTRY_GRID_SPACING_WEIGHT_MAX = 100.0
    ENTRY_GRID_SPACING_WEIGHT_STEP = 0.01
    ENTRY_GRID_SPACING_WEIGHT_ROUND = 2
    ENTRY_GRID_SPACING_WEIGHT_FORMAT = f'%.{ENTRY_GRID_SPACING_WEIGHT_ROUND}f'

    ENTRY_INITIAL_EMA_DIST_MIN = -1.0
    ENTRY_INITIAL_EMA_DIST_MAX = 1.0
    ENTRY_INITIAL_EMA_DIST_STEP = 0.0001
    ENTRY_INITIAL_EMA_DIST_ROUND = 4
    ENTRY_INITIAL_EMA_DIST_FORMAT = f'%.{ENTRY_INITIAL_EMA_DIST_ROUND}f'

    ENTRY_INITIAL_QTY_PCT_MIN = 0.0
    ENTRY_INITIAL_QTY_PCT_MAX = 1.0
    ENTRY_INITIAL_QTY_PCT_STEP = 0.001
    ENTRY_INITIAL_QTY_PCT_ROUND = 3
    ENTRY_INITIAL_QTY_PCT_FORMAT = f'%.{ENTRY_INITIAL_QTY_PCT_ROUND}f'

    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MIN = 0.0
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MAX = 10.0
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_STEP = 0.05
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_ROUND = 2
    ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_FORMAT = f'%.{ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_ROUND}f'
    
    ENTRY_TRAILING_GRID_RATIO_MIN = -1.0
    ENTRY_TRAILING_GRID_RATIO_MAX = 1.0
    ENTRY_TRAILING_GRID_RATIO_STEP = 0.01
    ENTRY_TRAILING_GRID_RATIO_ROUND = 2
    ENTRY_TRAILING_GRID_RATIO_FORMAT = f'%.{ENTRY_TRAILING_GRID_RATIO_ROUND}f'

    ENTRY_TRAILING_RETRACEMENT_PCT_MIN = 0.0
    ENTRY_TRAILING_RETRACEMENT_PCT_MAX = 1.0
    ENTRY_TRAILING_RETRACEMENT_PCT_STEP = 0.001
    ENTRY_TRAILING_RETRACEMENT_PCT_ROUND = 3
    ENTRY_TRAILING_RETRACEMENT_PCT_FORMAT = f'%.{ENTRY_TRAILING_RETRACEMENT_PCT_ROUND}f'

    ENTRY_TRAILING_THRESHOLD_PCT_MIN = -1.0
    ENTRY_TRAILING_THRESHOLD_PCT_MAX = 1.0
    ENTRY_TRAILING_THRESHOLD_PCT_STEP = 0.0001
    ENTRY_TRAILING_THRESHOLD_PCT_ROUND = 4
    ENTRY_TRAILING_THRESHOLD_PCT_FORMAT = f'%.{ENTRY_TRAILING_THRESHOLD_PCT_ROUND}f'

    FILTER_NOISINESS_ROLLING_WINDOW_MIN = 0.0
    FILTER_NOISINESS_ROLLING_WINDOW_MAX = 10000.0
    FILTER_NOISINESS_ROLLING_WINDOW_STEP = 1.0
    FILTER_NOISINESS_ROLLING_WINDOW_ROUND = 0
    FILTER_NOISINESS_ROLLING_WINDOW_FORMAT = f'%.{FILTER_NOISINESS_ROLLING_WINDOW_ROUND}f'

    FILTER_VOLUME_DROP_PCT_MIN = 0.0
    FILTER_VOLUME_DROP_PCT_MAX = 1.0
    FILTER_VOLUME_DROP_PCT_STEP = 0.01
    FILTER_VOLUME_DROP_PCT_ROUND = 2
    FILTER_VOLUME_DROP_PCT_FORMAT = f'%.{FILTER_VOLUME_DROP_PCT_ROUND}f'

    FILTER_VOLUME_ROLLING_WINDOW_MIN = 0.0
    FILTER_VOLUME_ROLLING_WINDOW_MAX = 10000.0
    FILTER_VOLUME_ROLLING_WINDOW_STEP = 1.0
    FILTER_VOLUME_ROLLING_WINDOW_ROUND = 0
    FILTER_VOLUME_ROLLING_WINDOW_FORMAT = f'%.{FILTER_VOLUME_ROLLING_WINDOW_ROUND}f'

    N_POSITIONS_MIN = 0.0
    N_POSITIONS_MAX = 100.0
    N_POSITIONS_STEP = 1.0
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
        # self._long_close_grid_markup_range_0 = 0.0
        # self._long_close_grid_markup_range_1 = 0.03
        # self._long_close_grid_min_markup_0 = 0.001
        # self._long_close_grid_min_markup_1 = 0.03
        self._long_close_grid_markup_end_0 = 0.001
        self._long_close_grid_markup_end_1 = 0.03
        self._long_close_grid_markup_start_0 = 0.001
        self._long_close_grid_markup_start_1 = 0.03
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
        self._long_entry_trailing_double_down_factor_0 = 0.1
        self._long_entry_trailing_double_down_factor_1 = 3.0
        self._long_entry_trailing_grid_ratio_0 = -1.0
        self._long_entry_trailing_grid_ratio_1 = 1.0
        self._long_entry_trailing_retracement_pct_0 = 0.0
        self._long_entry_trailing_retracement_pct_1 = 0.1
        self._long_entry_trailing_threshold_pct_0 = -0.1
        self._long_entry_trailing_threshold_pct_1 = 0.1
        self._long_filter_noisiness_rolling_window_0 = 10.0
        self._long_filter_noisiness_rolling_window_1 = 360.0
        self._long_filter_volume_drop_pct_0 = 0.5
        self._long_filter_volume_drop_pct_1 = 1.0
        self._long_filter_volume_rolling_window_0 = 10.0
        self._long_filter_volume_rolling_window_1 = 360.0
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
        # self._short_close_grid_markup_range_0 = 0.0
        # self._short_close_grid_markup_range_1 = 0.03
        # self._short_close_grid_min_markup_0 = 0.001
        # self._short_close_grid_min_markup_1 = 0.03
        self._short_close_grid_markup_end_0 = 0.001
        self._short_close_grid_markup_end_1 = 0.03
        self._short_close_grid_markup_start_0 = 0.001
        self._short_close_grid_markup_start_1 = 0.03
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
        self._short_entry_trailing_double_down_factor_0 = 0.1
        self._short_entry_trailing_double_down_factor_1 = 3.0
        self._short_entry_trailing_grid_ratio_0 = -1.0
        self._short_entry_trailing_grid_ratio_1 = 1.0
        self._short_entry_trailing_retracement_pct_0 = 0.0
        self._short_entry_trailing_retracement_pct_1 = 0.1
        self._short_entry_trailing_threshold_pct_0 = -0.1
        self._short_entry_trailing_threshold_pct_1 = 0.1
        self._short_filter_noisiness_rolling_window_0 = 10.0
        self._short_filter_noisiness_rolling_window_1 = 360.0
        self._short_filter_volume_drop_pct_0 = 0.5
        self._short_filter_volume_drop_pct_1 = 1.0
        self._short_filter_volume_rolling_window_0 = 10.0
        self._short_filter_volume_rolling_window_1 = 360.0
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
                # "long_close_grid_markup_range": [self._long_close_grid_markup_range_0, self._long_close_grid_markup_range_1],
                # "long_close_grid_min_markup": [self._long_close_grid_min_markup_0, self._long_close_grid_min_markup_1],
                "long_close_grid_markup_end": [self._long_close_grid_markup_end_0, self._long_close_grid_markup_end_1],
                "long_close_grid_markup_start": [self._long_close_grid_markup_start_0, self._long_close_grid_markup_start_1],
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
                "long_entry_trailing_double_down_factor": [self._long_entry_trailing_double_down_factor_0, self._long_entry_trailing_double_down_factor_1],
                "long_entry_trailing_grid_ratio": [self._long_entry_trailing_grid_ratio_0, self._long_entry_trailing_grid_ratio_1],
                "long_entry_trailing_retracement_pct": [self._long_entry_trailing_retracement_pct_0, self._long_entry_trailing_retracement_pct_1],
                "long_entry_trailing_threshold_pct": [self._long_entry_trailing_threshold_pct_0, self._long_entry_trailing_threshold_pct_1],
                "long_filter_noisiness_rolling_window": [self._long_filter_noisiness_rolling_window_0, self._long_filter_noisiness_rolling_window_1],
                "long_filter_volume_drop_pct": [self._long_filter_volume_drop_pct_0, self._long_filter_volume_drop_pct_1],
                "long_filter_volume_rolling_window": [self._long_filter_volume_rolling_window_0, self._long_filter_volume_rolling_window_1],
                "long_n_positions": [self._long_n_positions_0, self._long_n_positions_1],
                "long_total_wallet_exposure_limit": [self._long_total_wallet_exposure_limit_0, self._long_total_wallet_exposure_limit_1],
                "long_unstuck_close_pct": [self._long_unstuck_close_pct_0, self._long_unstuck_close_pct_1],
                "long_unstuck_ema_dist": [self._long_unstuck_ema_dist_0, self._long_unstuck_ema_dist_1],
                "long_unstuck_loss_allowance_pct": [self._long_unstuck_loss_allowance_pct_0, self._long_unstuck_loss_allowance_pct_1],
                "long_unstuck_threshold": [self._long_unstuck_threshold_0, self._long_unstuck_threshold_1],
                # "short_close_grid_markup_range": [self._short_close_grid_markup_range_0, self._short_close_grid_markup_range_1],
                # "short_close_grid_min_markup": [self._short_close_grid_min_markup_0, self._short_close_grid_min_markup_1],
                "short_close_grid_markup_end": [self._short_close_grid_markup_end_0, self._short_close_grid_markup_end_1],
                "short_close_grid_markup_start": [self._short_close_grid_markup_start_0, self._short_close_grid_markup_start_1],
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
                "short_entry_trailing_double_down_factor": [self._short_entry_trailing_double_down_factor_0, self._short_entry_trailing_double_down_factor_1],
                "short_entry_trailing_grid_ratio": [self._short_entry_trailing_grid_ratio_0, self._short_entry_trailing_grid_ratio_1],
                "short_entry_trailing_retracement_pct": [self._short_entry_trailing_retracement_pct_0, self._short_entry_trailing_retracement_pct_1],
                "short_entry_trailing_threshold_pct": [self._short_entry_trailing_threshold_pct_0, self._short_entry_trailing_threshold_pct_1],
                "short_filter_noisiness_rolling_window": [self._short_filter_noisiness_rolling_window_0, self._short_filter_noisiness_rolling_window_1],
                "short_filter_volume_drop_pct": [self._short_filter_volume_drop_pct_0, self._short_filter_volume_drop_pct_1],
                "short_filter_volume_rolling_window": [self._short_filter_volume_rolling_window_0, self._short_filter_volume_rolling_window_1],
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
        # if "long_close_grid_markup_range" in new_bounds:
        #     self.long_close_grid_markup_range_0 = new_bounds["long_close_grid_markup_range"][0]
        #     self.long_close_grid_markup_range_1 = new_bounds["long_close_grid_markup_range"][1]
        # if "long_close_grid_min_markup" in new_bounds:
        #     self.long_close_grid_min_markup_0 = new_bounds["long_close_grid_min_markup"][0]
        #     self.long_close_grid_min_markup_1 = new_bounds["long_close_grid_min_markup"][1]
        if "long_close_grid_markup_end" in new_bounds:
            self.long_close_grid_markup_end_0 = new_bounds["long_close_grid_markup_end"][0]
            self.long_close_grid_markup_end_1 = new_bounds["long_close_grid_markup_end"][1]
        if "long_close_grid_markup_start" in new_bounds:
            self.long_close_grid_markup_start_0 = new_bounds["long_close_grid_markup_start"][0]
            self.long_close_grid_markup_start_1 = new_bounds["long_close_grid_markup_start"][1]
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
        if "long_entry_trailing_double_down_factor" in new_bounds:
            self.long_entry_trailing_double_down_factor_0 = new_bounds["long_entry_trailing_double_down_factor"][0]
            self.long_entry_trailing_double_down_factor_1 = new_bounds["long_entry_trailing_double_down_factor"][1]
        if "long_entry_trailing_grid_ratio" in new_bounds:
            self.long_entry_trailing_grid_ratio_0 = new_bounds["long_entry_trailing_grid_ratio"][0]
            self.long_entry_trailing_grid_ratio_1 = new_bounds["long_entry_trailing_grid_ratio"][1]
        if "long_entry_trailing_retracement_pct" in new_bounds:
            self.long_entry_trailing_retracement_pct_0 = new_bounds["long_entry_trailing_retracement_pct"][0]
            self.long_entry_trailing_retracement_pct_1 = new_bounds["long_entry_trailing_retracement_pct"][1]
        if "long_entry_trailing_threshold_pct" in new_bounds:
            self.long_entry_trailing_threshold_pct_0 = new_bounds["long_entry_trailing_threshold_pct"][0]
            self.long_entry_trailing_threshold_pct_1 = new_bounds["long_entry_trailing_threshold_pct"][1]
        if "long_filter_noisiness_rolling_window" in new_bounds:
            self.long_filter_noisiness_rolling_window_0 = new_bounds["long_filter_noisiness_rolling_window"][0]
            self.long_filter_noisiness_rolling_window_1 = new_bounds["long_filter_noisiness_rolling_window"][1]
        # Fix for old configs
        elif "long_filter_rolling_window" in new_bounds:
            self.long_filter_noisiness_rolling_window_0 = new_bounds["long_filter_rolling_window"][0]
            self.long_filter_noisiness_rolling_window_1 = new_bounds["long_filter_rolling_window"][1]
        if "long_filter_volume_drop_pct" in new_bounds:
            self.long_filter_volume_drop_pct_0 = new_bounds["long_filter_volume_drop_pct"][0]
            self.long_filter_volume_drop_pct_1 = new_bounds["long_filter_volume_drop_pct"][1]
        # Fix for old configs
        elif "long_filter_relative_volume_clip_pct" in new_bounds:
            self.long_filter_volume_drop_pct_0 = new_bounds["long_filter_relative_volume_clip_pct"][0]
            self.long_filter_volume_drop_pct_1 = new_bounds["long_filter_relative_volume_clip_pct"][1]
        if "long_filter_volume_rolling_window" in new_bounds:
            self.long_filter_volume_rolling_window_0 = new_bounds["long_filter_volume_rolling_window"][0]
            self.long_filter_volume_rolling_window_1 = new_bounds["long_filter_volume_rolling_window"][1]
        # Fix for old configs
        elif "long_filter_rolling_window" in new_bounds:
            self.long_filter_volume_rolling_window_0 = new_bounds["long_filter_rolling_window"][0]
            self.long_filter_volume_rolling_window_1 = new_bounds["long_filter_rolling_window"][1]
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
        # if "short_close_grid_markup_range" in new_bounds:
        #     self.short_close_grid_markup_range_0 = new_bounds["short_close_grid_markup_range"][0]
        #     self.short_close_grid_markup_range_1 = new_bounds["short_close_grid_markup_range"][1]
        # if "short_close_grid_min_markup" in new_bounds:
        #     self.short_close_grid_min_markup_0 = new_bounds["short_close_grid_min_markup"][0]
        #     self.short_close_grid_min_markup_1 = new_bounds["short_close_grid_min_markup"][1]
        if "short_close_grid_markup_end" in new_bounds:
            self.short_close_grid_markup_end_0 = new_bounds["short_close_grid_markup_end"][0]
            self.short_close_grid_markup_end_1 = new_bounds["short_close_grid_markup_end"][1]
        if "short_close_grid_markup_start" in new_bounds:
            self.short_close_grid_markup_start_0 = new_bounds["short_close_grid_markup_start"][0]
            self.short_close_grid_markup_start_1 = new_bounds["short_close_grid_markup_start"][1]
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
        if "short_entry_trailing_double_down_factor" in new_bounds:
            self.short_entry_trailing_double_down_factor_0 = new_bounds["short_entry_trailing_double_down_factor"][0]
            self.short_entry_trailing_double_down_factor_1 = new_bounds["short_entry_trailing_double_down_factor"][1]
        if "short_entry_trailing_grid_ratio" in new_bounds:
            self.short_entry_trailing_grid_ratio_0 = new_bounds["short_entry_trailing_grid_ratio"][0]
            self.short_entry_trailing_grid_ratio_1 = new_bounds["short_entry_trailing_grid_ratio"][1]
        if "short_entry_trailing_retracement_pct" in new_bounds:
            self.short_entry_trailing_retracement_pct_0 = new_bounds["short_entry_trailing_retracement_pct"][0]
            self.short_entry_trailing_retracement_pct_1 = new_bounds["short_entry_trailing_retracement_pct"][1]
        if "short_entry_trailing_threshold_pct" in new_bounds:
            self.short_entry_trailing_threshold_pct_0 = new_bounds["short_entry_trailing_threshold_pct"][0]
            self.short_entry_trailing_threshold_pct_1 = new_bounds["short_entry_trailing_threshold_pct"][1]
        if "short_filter_noisiness_rolling_window" in new_bounds:
            self.short_filter_noisiness_rolling_window_0 = new_bounds["short_filter_noisiness_rolling_window"][0]
            self.short_filter_noisiness_rolling_window_1 = new_bounds["short_filter_noisiness_rolling_window"][1]
        # Fix for old configs
        elif "short_filter_rolling_window" in new_bounds:
            self.short_filter_noisiness_rolling_window_0 = new_bounds["short_filter_rolling_window"][0]
            self.short_filter_noisiness_rolling_window_1 = new_bounds["short_filter_rolling_window"][1]
        if "short_filter_volume_drop_pct" in new_bounds:
            self.short_filter_volume_drop_pct_0 = new_bounds["short_filter_volume_drop_pct"][0]
            self.short_filter_volume_drop_pct_1 = new_bounds["short_filter_volume_drop_pct"][1]
        # Fix for old configs
        elif "short_filter_relative_volume_clip_pct" in new_bounds:
            self.short_filter_volume_drop_pct_0 = new_bounds["short_filter_relative_volume_clip_pct"][0]
            self.short_filter_volume_drop_pct_1 = new_bounds["short_filter_relative_volume_clip_pct"][1]
        if "short_filter_volume_rolling_window" in new_bounds:
            self.short_filter_volume_rolling_window_0 = new_bounds["short_filter_volume_rolling_window"][0]
            self.short_filter_volume_rolling_window_1 = new_bounds["short_filter_volume_rolling_window"][1]
        # Fix for old configs
        elif "short_filter_rolling_window" in new_bounds:
            self.short_filter_volume_rolling_window_0 = new_bounds["short_filter_rolling_window"][0]
            self.short_filter_volume_rolling_window_1 = new_bounds["short_filter_rolling_window"][1]
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
    # @property
    # def long_close_grid_markup_range_0(self): return self._long_close_grid_markup_range_0
    # @property
    # def long_close_grid_markup_range_1(self): return self._long_close_grid_markup_range_1
    # @property
    # def long_close_grid_min_markup_0(self): return self._long_close_grid_min_markup_0
    # @property
    # def long_close_grid_min_markup_1(self): return self._long_close_grid_min_markup_1
    @property
    def long_close_grid_markup_end_0(self): return self._long_close_grid_markup_end_0
    @property
    def long_close_grid_markup_end_1(self): return self._long_close_grid_markup_end_1
    @property
    def long_close_grid_markup_start_0(self): return self._long_close_grid_markup_start_0
    @property
    def long_close_grid_markup_start_1(self): return self._long_close_grid_markup_start_1
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
    def long_entry_trailing_double_down_factor_0(self): return self._long_entry_trailing_double_down_factor_0
    @property
    def long_entry_trailing_double_down_factor_1(self): return self._long_entry_trailing_double_down_factor_1
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
    def long_filter_noisiness_rolling_window_0(self): return self._long_filter_noisiness_rolling_window_0
    @property
    def long_filter_noisiness_rolling_window_1(self): return self._long_filter_noisiness_rolling_window_1
    @property
    def long_filter_volume_drop_pct_0(self): return self._long_filter_volume_drop_pct_0
    @property
    def long_filter_volume_drop_pct_1(self): return self._long_filter_volume_drop_pct_1
    @property
    def long_filter_volume_rolling_window_0(self): return self._long_filter_volume_rolling_window_0
    @property
    def long_filter_volume_rolling_window_1(self): return self._long_filter_volume_rolling_window_1
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
    @property
    def long_unstuck_threshold_1(self): return self._long_unstuck_threshold_1
    
    # Short parameters
    # @property
    # def long_unstuck_threshold_1(self): return self._long_unstuck_threshold_1
    # @property
    # def short_close_grid_markup_range_0(self): return self._short_close_grid_markup_range_0
    # @property
    # def short_close_grid_markup_range_1(self): return self._short_close_grid_markup_range_1
    # @property
    # def short_close_grid_min_markup_0(self): return self._short_close_grid_min_markup_0
    @property
    def short_close_grid_markup_end_0(self): return self._short_close_grid_markup_end_0
    @property
    def short_close_grid_markup_end_1(self): return self._short_close_grid_markup_end_1
    @property
    def short_close_grid_markup_start_0(self): return self._short_close_grid_markup_start_0
    @property
    def short_close_grid_markup_start_1(self): return self._short_close_grid_markup_start_1
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
    def short_entry_trailing_double_down_factor_0(self): return self._short_entry_trailing_double_down_factor_0
    @property
    def short_entry_trailing_double_down_factor_1(self): return self._short_entry_trailing_double_down_factor_1
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
    def short_filter_noisiness_rolling_window_0(self): return self._short_filter_noisiness_rolling_window_0
    @property
    def short_filter_noisiness_rolling_window_1(self): return self._short_filter_noisiness_rolling_window_1
    @property
    def short_filter_volume_drop_pct_0(self): return self._short_filter_volume_drop_pct_0
    @property
    def short_filter_volume_drop_pct_1(self): return self._short_filter_volume_drop_pct_1
    @property
    def short_filter_volume_rolling_window_0(self): return self._short_filter_volume_rolling_window_0
    @property
    def short_filter_volume_rolling_window_1(self): return self._short_filter_volume_rolling_window_1
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
    # @long_close_grid_markup_range_0.setter
    # def long_close_grid_markup_range_0(self, new_value):
    #     self._long_close_grid_markup_range_0 = new_value
    #     self._bounds["long_close_grid_markup_range"][0] = new_value
    # @long_close_grid_markup_range_1.setter
    # def long_close_grid_markup_range_1(self, new_value):
    #     self._long_close_grid_markup_range_1 = new_value
    #     self._bounds["long_close_grid_markup_range"][1] = new_value
    # @long_close_grid_min_markup_0.setter
    # def long_close_grid_min_markup_0(self, new_value):
    #     self._long_close_grid_min_markup_0 = new_value
    #     self._bounds["long_close_grid_min_markup"][0] = new_value
    # @long_close_grid_min_markup_1.setter
    # def long_close_grid_min_markup_1(self, new_value):
    #     self._long_close_grid_min_markup_1 = new_value
    #     self._bounds["long_close_grid_min_markup"][1] = new_value
    @long_close_grid_markup_end_0.setter
    def long_close_grid_markup_end_0(self, new_value):
        self._long_close_grid_markup_end_0 = new_value
        self._bounds["long_close_grid_markup_end"][0] = new_value
    @long_close_grid_markup_end_1.setter
    def long_close_grid_markup_end_1(self, new_value):
        self._long_close_grid_markup_end_1 = new_value
        self._bounds["long_close_grid_markup_end"][1] = new_value
    @long_close_grid_markup_start_0.setter
    def long_close_grid_markup_start_0(self, new_value):
        self._long_close_grid_markup_start_0 = new_value
        self._bounds["long_close_grid_markup_start"][0] = new_value
    @long_close_grid_markup_start_1.setter
    def long_close_grid_markup_start_1(self, new_value):
        self._long_close_grid_markup_start_1 = new_value
        self._bounds["long_close_grid_markup_start"][1] = new_value
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
    @long_entry_trailing_double_down_factor_0.setter
    def long_entry_trailing_double_down_factor_0(self, new_value):
        self._long_entry_trailing_double_down_factor_0 = new_value
        self._bounds["long_entry_trailing_double_down_factor"][0] = new_value
    @long_entry_trailing_double_down_factor_1.setter
    def long_entry_trailing_double_down_factor_1(self, new_value):
        self._long_entry_trailing_double_down_factor_1 = new_value
        self._bounds["long_entry_trailing_double_down_factor"][1] = new_value
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
    @long_filter_noisiness_rolling_window_0.setter
    def long_filter_noisiness_rolling_window_0(self, new_value):
        self._long_filter_noisiness_rolling_window_0 = new_value
        self._bounds["long_filter_noisiness_rolling_window"][0] = new_value
    @long_filter_noisiness_rolling_window_1.setter
    def long_filter_noisiness_rolling_window_1(self, new_value):
        self._long_filter_noisiness_rolling_window_1 = new_value
        self._bounds["long_filter_noisiness_rolling_window"][1] = new_value
    @long_filter_volume_drop_pct_0.setter
    def long_filter_volume_drop_pct_0(self, new_value):
        self._long_filter_volume_drop_pct_0 = new_value
        self._bounds["long_filter_volume_drop_pct"][0] = new_value
    @long_filter_volume_drop_pct_1.setter
    def long_filter_volume_drop_pct_1(self, new_value):
        self._long_filter_volume_drop_pct_1 = new_value
        self._bounds["long_filter_volume_drop_pct"][1] = new_value
    @long_filter_volume_rolling_window_0.setter
    def long_filter_volume_rolling_window_0(self, new_value):
        self._long_filter_volume_rolling_window_0 = new_value
        self._bounds["long_filter_volume_rolling_window"][0] = new_value
    @long_filter_volume_rolling_window_1.setter
    def long_filter_volume_rolling_window_1(self, new_value):
        self._long_filter_volume_rolling_window_1 = new_value
        self._bounds["long_filter_volume_rolling_window"][1] = new_value
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
    # @short_close_grid_markup_range_0.setter
    # def short_close_grid_markup_range_0(self, new_value):
    #     self._short_close_grid_markup_range_0 = new_value
    #     self._bounds["short_close_grid_markup_range"][0] = new_value
    # @short_close_grid_markup_range_1.setter
    # def short_close_grid_markup_range_1(self, new_value):
    #     self._short_close_grid_markup_range_1 = new_value
    #     self._bounds["short_close_grid_markup_range"][1] = new_value
    # @short_close_grid_min_markup_0.setter
    # def short_close_grid_min_markup_0(self, new_value):
    #     self._short_close_grid_min_markup_0 = new_value
    #     self._bounds["short_close_grid_min_markup"][0] = new_value
    # @short_close_grid_min_markup_1.setter
    # def short_close_grid_min_markup_1(self, new_value):
    #     self._short_close_grid_min_markup_1 = new_value
    #     self._bounds["short_close_grid_min_markup"][1] = new_value
    @short_close_grid_markup_end_0.setter
    def short_close_grid_markup_end_0(self, new_value):
        self._short_close_grid_markup_end_0 = new_value
        self._bounds["short_close_grid_markup_end"][0] = new_value
    @short_close_grid_markup_end_1.setter
    def short_close_grid_markup_end_1(self, new_value):
        self._short_close_grid_markup_end_1 = new_value
        self._bounds["short_close_grid_markup_end"][1] = new_value
    @short_close_grid_markup_start_0.setter
    def short_close_grid_markup_start_0(self, new_value):
        self._short_close_grid_markup_start_0 = new_value
        self._bounds["short_close_grid_markup_start"][0] = new_value
    @short_close_grid_markup_start_1.setter
    def short_close_grid_markup_start_1(self, new_value):
        self._short_close_grid_markup_start_1 = new_value
        self._bounds["short_close_grid_markup_start"][1] = new_value
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
    @short_entry_trailing_double_down_factor_0.setter
    def short_entry_trailing_double_down_factor_0(self, new_value):
        self._short_entry_trailing_double_down_factor_0 = new_value
        self._bounds["short_entry_trailing_double_down_factor"][0] = new_value
    @short_entry_trailing_double_down_factor_1.setter
    def short_entry_trailing_double_down_factor_1(self, new_value):
        self._short_entry_trailing_double_down_factor_1 = new_value
        self._bounds["short_entry_trailing_double_down_factor"][1] = new_value
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
    @short_filter_noisiness_rolling_window_0.setter
    def short_filter_noisiness_rolling_window_0(self, new_value):
        self._short_filter_noisiness_rolling_window_0 = new_value
        self._bounds["short_filter_noisiness_rolling_window"][0] = new_value
    @short_filter_noisiness_rolling_window_1.setter
    def short_filter_noisiness_rolling_window_1(self, new_value):
        self._short_filter_noisiness_rolling_window_1 = new_value
        self._bounds["short_filter_noisiness_rolling_window"][1] = new_value
    @short_filter_volume_drop_pct_0.setter
    def short_filter_volume_drop_pct_0(self, new_value):
        self._short_filter_volume_drop_pct_0 = new_value
        self._bounds["short_filter_volume_drop_pct"][0] = new_value
    @short_filter_volume_drop_pct_1.setter
    def short_filter_volume_drop_pct_1(self, new_value):
        self._short_filter_volume_drop_pct_1 = new_value
        self._bounds["short_filter_volume_drop_pct"][1] = new_value
    @short_filter_volume_rolling_window_0.setter
    def short_filter_volume_rolling_window_0(self, new_value):
        self._short_filter_volume_rolling_window_0 = new_value
        self._bounds["short_filter_volume_rolling_window"][0] = new_value
    @short_filter_volume_rolling_window_1.setter
    def short_filter_volume_rolling_window_1(self, new_value):
        self._short_filter_volume_rolling_window_1 = new_value
        self._bounds["short_filter_volume_rolling_window"][1] = new_value
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
        self._only_cpt = False
        self._starting_config = False
        self._market_cap = 0
        self._vol_mcap = 10.0
        self._tags = []
        self._dynamic_ignore = False
        self._notices_ignore = False
        self._note = ''
        self._pbgui = {
            "version": self._version,
            "enabled_on": self._enabled_on,
            "only_cpt": self._only_cpt,
            "starting_config": self._starting_config,
            "market_cap": self._market_cap,
            "vol_mcap": self._vol_mcap,
            "tags": self._tags,
            "dynamic_ignore": self._dynamic_ignore,
            "notices_ignore": self._notices_ignore,
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
        if "only_cpt" in new_pbgui:
            self.only_cpt = new_pbgui["only_cpt"]
        if "starting_config" in new_pbgui:
            self.starting_config = new_pbgui["starting_config"]
        if "market_cap" in new_pbgui:
            self.market_cap = new_pbgui["market_cap"]
        if "vol_mcap" in new_pbgui:
            self.vol_mcap = new_pbgui["vol_mcap"]
        if "tags" in new_pbgui:
            self.tags = new_pbgui["tags"]
        if "dynamic_ignore" in new_pbgui:
            self.dynamic_ignore = new_pbgui["dynamic_ignore"]
        if "notices_ignore" in new_pbgui:
            self.notices_ignore = new_pbgui["notices_ignore"]
        if "note" in new_pbgui:
            self.note = new_pbgui["note"]
    
    @property
    def version(self): return self._version
    @property
    def enabled_on(self): return self._enabled_on
    @property
    def only_cpt(self): return self._only_cpt
    @property
    def starting_config(self): return self._starting_config
    @property
    def market_cap(self): return self._market_cap
    @property
    def vol_mcap(self): return self._vol_mcap
    @property
    def tags(self): return self._tags
    @property
    def dynamic_ignore(self): return self._dynamic_ignore
    @property
    def notices_ignore(self): return self._notices_ignore
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
    @only_cpt.setter
    def only_cpt(self, new_only_cpt):
        self._only_cpt = new_only_cpt
        self._pbgui["only_cpt"] = self._only_cpt
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
    @tags.setter
    def tags(self, new_tags):
        self._tags = new_tags
        self._pbgui["tags"] = self._tags
    @dynamic_ignore.setter
    def dynamic_ignore(self, new_dynamic_ignore):
        self._dynamic_ignore = new_dynamic_ignore
        self._pbgui["dynamic_ignore"] = self._dynamic_ignore
    @notices_ignore.setter
    def notices_ignore(self, new_notices_ignore):
        self._notices_ignore = new_notices_ignore
        self._pbgui["notices_ignore"] = self._notices_ignore
    @note.setter
    def note(self, new_note):
        self._note = new_note
        self._pbgui["note"] = self._note

class ConfigV7():
    def __init__(self, file_name = None):
        self._config_file = file_name
        self._backtest = Backtest()
        self._bot = Bot()
        self._coin_overrides = {}
        self._live = Live()
        self._optimize = Optimize()
        self._pbgui = PBGui()

        self._config = {
            "backtest": self._backtest._backtest,
            "bot": self._bot._bot,
            "coin_overrides": self._coin_overrides,
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
    def coin_overrides(self): return self._coin_overrides
    @coin_overrides.setter
    def coin_overrides(self, new_value):
        self._coin_overrides = new_value
        self._config["coin_overrides"] = self._coin_overrides

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
        if "coin_overrides" in new_value:
            self.coin_overrides = new_value["coin_overrides"]
        if "live" in new_value:
            self.live = new_value["live"]
        if "optimize" in new_value:
            self.optimize = new_value["optimize"]
        if "pbgui" in new_value:
            self.pbgui = new_value["pbgui"]
        # Convert coin_flags to coin_overrides
        if "coin_flags" in new_value["live"]:
            if new_value["live"]["coin_flags"]:
                for symbol, flags in new_value["live"]["coin_flags"].items():
                    # remove USDT and USDC from symbol
                    # if symbol.endswith("USDT"):
                    #     symbol = symbol[:-4]
                    # elif symbol.endswith("USDC"):
                    #     symbol = symbol[:-4]
                    # print(symbol, flags)
                    if symbol not in self.coin_overrides:
                        self.coin_overrides[symbol] = {}
                    lm = {
                        "n": "normal",
                        "normal": "normal",
                        "m": "manual",
                        "manual": "manual",
                        "gs": "graceful_stop",
                        "graceful-stop": "graceful_stop",
                        "graceful_stop": "graceful_stop",
                        "p": "panic",
                        "panic": "panic",
                        "t": "tp_only",
                        "tp": "tp_only",
                        "tp-only": "tp_only",
                        "tp_only": "tp_only"
                    }.get(flags.split("-lm")[1].split()[0], "") if "-lm" in flags else ""
                    if lm:
                        if "live" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["live"] = {}
                        self.coin_overrides[symbol]["live"]["forced_mode_long"] = lm

                    lw = flags.split("-lw")[1].split()[0] if "-lw" in flags else ""
                    if lw:
                        if "bot" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["bot"] = {}
                        if "long" not in self.coin_overrides[symbol]["bot"]:
                            self.coin_overrides[symbol]["bot"]["long"] = {}
                        self.coin_overrides[symbol]["bot"]["long"]["wallet_exposure_limit"] = float(lw)

                    sm = {
                        "n": "normal",
                        "normal": "normal",
                        "m": "manual",
                        "manual": "manual",
                        "gs": "graceful_stop",
                        "graceful-stop": "graceful_stop",
                        "graceful_stop": "graceful_stop",
                        "p": "panic",
                        "panic": "panic",
                        "t": "tp_only",
                        "tp": "tp_only",
                        "tp-only": "tp_only",
                        "tp_only": "tp_only"
                    }.get(flags.split("-sm")[1].split()[0], "") if "-sm" in flags else ""
                    if sm:
                        if "live" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["live"] = {}
                        self.coin_overrides[symbol]["live"]["forced_mode_short"] = sm

                    sw = flags.split("-sw")[1].split()[0] if "-sw" in flags else ""
                    if sw:
                        if "bot" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["bot"] = {}
                        if "short" not in self.coin_overrides[symbol]["bot"]:
                            self.coin_overrides[symbol]["bot"]["short"] = {}
                        self.coin_overrides[symbol]["bot"]["short"]["wallet_exposure_limit"] = float(sw)

                    lev = flags.split("-lev")[1].split()[0] if "-lev" in flags else ""
                    if lev:
                        if "live" not in self.coin_overrides[symbol]:
                            self.coin_overrides[symbol]["live"] = {}
                        self.coin_overrides[symbol]["live"]["leverage"] = float(lev)

                    config = flags.split("-lc")[1].split()[0] if "-lc" in flags else ""
                    if config:
                        self.coin_overrides[symbol]["override_config_path"] = config

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

    def view_coin_overrides(self):
        if self.config["coin_overrides"]:
            overrides = True
        else:
            overrides = False
        with st.expander("Coin Overrides", expanded=overrides):
            # Init
            if not "ed_key" in st.session_state:
                st.session_state.ed_key = 0
            ed_key = st.session_state.ed_key
            if f'select_coins_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_coins_{ed_key}']
                for row in ed["edited_rows"]:
                    if "edit" in ed["edited_rows"][row]:
                        if ed["edited_rows"][row]["edit"]:
                            st.session_state.edit_coin_override = st.session_state.co_data[row]["coin"]
            # if not "co_data" in st.session_state:
            co_data = []
            if self.config["coin_overrides"]:
                for coin in self.config["coin_overrides"]:
                    co_data.append({
                        'edit': False,
                        'coin': coin,
                        'override_config_path': self.config["coin_overrides"][coin].get('override_config_path', False),
                        'config.bot.long parameters': self.config["coin_overrides"][coin].get('bot', {}).get('long', {}),
                        'config.bot.short parameters': self.config["coin_overrides"][coin].get('bot', {}).get('short', {}),
                        'config.live parameters': self.config["coin_overrides"][coin].get('live', {}),
                    })
            st.session_state.co_data = co_data
            # Display coin_overrides
            if st.session_state.co_data and not "edit_coin_override" in st.session_state:
                d = st.session_state.co_data
                st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_coins_{ed_key}', disabled=['coin', 'override_config_path', 'config.bot.long parameters', 'config.bot.short parameters', 'config.live parameters'])
            if "edit_run_v7_add_coin_override_button" in st.session_state:
                if st.session_state.edit_run_v7_add_coin_override_button:
                    if self.config_file is None:
                        error_popup("Please save config, before editing coin overrides.")
                    else:
                        st.session_state.edit_coin_override = st.session_state.edit_run_v7_add_coin_override
                        st.rerun()
            if "edit_coin_override" in st.session_state:
                self.edit_coin_override(st.session_state.edit_coin_override)
            else:
                col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment="bottom")
                with col1:
                    st.selectbox('Symbol', st.session_state.pbcoindata.symbols, key="edit_run_v7_add_coin_override")
                with col2:
                    st.button("Add Coin Override", key="edit_run_v7_add_coin_override_button")

    def edit_coin_override(self, symbol):
        # reove USDT or USDC from symbol
        # if symbol.endswith("USDT"):
        #     symbol = symbol[:-4]
        # elif symbol.endswith("USDC"):
        #     symbol = symbol[:-4]
        OVERRIDES_LIVE = [
            "forced_mode_long",
            "forced_mode_short",
            "leverage"
        ]
        OVERRIDES = [
            "close_grid_markup_end",
            "close_grid_markup_start",
            "close_grid_qty_pct",
            "close_trailing_grid_ratio",
            "close_trailing_qty_pct",
            "close_trailing_retracement_pct",
            "close_trailing_threshold_pct",
            "ema_span_0",
            "ema_span_1",
            "enforce_exposure_limit",
            "entry_grid_double_down_factor",
            "entry_grid_spacing_pct",
            "entry_grid_spacing_weight",
            "entry_initial_ema_dist",
            "entry_initial_qty_pct",
            "entry_trailing_double_down_factor",
            "entry_trailing_grid_ratio",
            "entry_trailing_retracement_pct",
            "entry_trailing_threshold_pct",
            "unstuck_close_pct",
            "unstuck_ema_dist",
            "unstuck_threshold",
            "wallet_exposure_limit"
        ]
        MODE = [
            "normal",
            "manual",
            "graceful_stop",
            "panic",
            "tp_only"
        ]
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'edit_run_v7_co_parameters_{ed_key}' in st.session_state:
            ed = st.session_state[f'edit_run_v7_co_parameters_{ed_key}']
            for row in ed["edited_rows"]:
                if "delete" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["delete"]:
                        if st.session_state.co_parameters[row]["section"] == "bot":
                            self.config["coin_overrides"][symbol]["bot"][st.session_state.co_parameters[row]["side"]].pop(st.session_state.co_parameters[row]["parameter"])
                            # cleanup empty sections
                            if self.config["coin_overrides"][symbol]["bot"][st.session_state.co_parameters[row]["side"]] == {}:
                                del self.config["coin_overrides"][symbol]["bot"][st.session_state.co_parameters[row]["side"]]
                            if self.config["coin_overrides"][symbol]["bot"] == {}:
                                del self.config["coin_overrides"][symbol]["bot"]
                        elif st.session_state.co_parameters[row]["section"] == "live":
                            self.config["coin_overrides"][symbol]["live"].pop(st.session_state.co_parameters[row]["parameter"])
                            # cleanup empty sections
                            if self.config["coin_overrides"][symbol]["live"] == {}:
                                del self.config["coin_overrides"][symbol]["live"]
                        # clear co_parameters
                        if "co_parameters" in st.session_state:
                            del st.session_state.co_parameters
                        st.rerun()

        config = False
        # Init from config
        if self.config["coin_overrides"] and "edit_run_v7_co_config" not in st.session_state:
            if symbol in self.config["coin_overrides"]:
                if "override_config_path" in self.config["coin_overrides"][symbol]:
                    config = True
                    if "co_config" not in st.session_state:
                        st.session_state.co_config = ConfigV7(file_name=Path(Path(self.config_file).parent, f'{symbol}.json'))
                        st.session_state.co_config.load_config()
                        if "edit_co_configv7_long" in st.session_state:
                            del st.session_state.edit_co_configv7_long
                        if "edit_co_configv7_short" in st.session_state:
                            del st.session_state.edit_co_configv7_short
        # Init session_state for keys
        if "edit_run_v7_co_config" in st.session_state:
            if st.session_state.edit_run_v7_co_config != config:
                config = st.session_state.edit_run_v7_co_config
        if "edit_run_v7_co_parameter" in st.session_state:
            if st.session_state.edit_run_v7_co_add_parameter and st.session_state.edit_run_v7_co_side and st.session_state.edit_run_v7_co_value:
                # Ensure nested dicts exist
                if symbol not in self.config["coin_overrides"]:
                    self.config["coin_overrides"][symbol] = {}
                if "bot" not in self.config["coin_overrides"][symbol]:
                    self.config["coin_overrides"][symbol]["bot"] = {}
                if st.session_state.edit_run_v7_co_side not in self.config["coin_overrides"][symbol]["bot"]:
                    self.config["coin_overrides"][symbol]["bot"][st.session_state.edit_run_v7_co_side] = {}
                self.config["coin_overrides"][symbol]["bot"][st.session_state.edit_run_v7_co_side][st.session_state.edit_run_v7_co_parameter] = st.session_state.edit_run_v7_co_value
                if "co_parameters" in st.session_state:
                    del st.session_state.co_parameters
        if "edit_run_v7_co_parameter_live" in st.session_state:
            if st.session_state.edit_run_v7_co_add_parameter_live and st.session_state.edit_run_v7_co_value_live:
                # Ensure nested dicts exist
                if symbol not in self.config["coin_overrides"]:
                    self.config["coin_overrides"][symbol] = {}
                if "live" not in self.config["coin_overrides"][symbol]:
                    self.config["coin_overrides"][symbol]["live"] = {}
                self.config["coin_overrides"][symbol]["live"][st.session_state.edit_run_v7_co_parameter_live] = st.session_state.edit_run_v7_co_value_live
                if "co_parameters" in st.session_state:
                    del st.session_state.co_parameters
        if not "co_parameters" in st.session_state:
            co_parameters = []
            for parameter in self.config["coin_overrides"].get(symbol, {}).get('bot', {}).get('long', {}):
                co_parameters.append({
                    'section': 'bot',
                    'parameter': parameter,
                    'side': 'long',
                    'value': self.config["coin_overrides"][symbol]['bot']['long'][parameter],
                    'delete': False,
                })
            for parameter in self.config["coin_overrides"].get(symbol, {}).get('bot', {}).get('short', {}):
                co_parameters.append({
                    'section': 'bot',
                    'parameter': parameter,
                    'side': 'short',
                    'value': self.config["coin_overrides"][symbol]['bot']['short'][parameter],
                    'delete': False,
                })
            for parameter in self.config["coin_overrides"].get(symbol, {}).get('live', {}):
                co_parameters.append({
                    'section': 'live',
                    'parameter': parameter,
                    'side': 'live',
                    'value': self.config["coin_overrides"][symbol]['live'][parameter],
                    'delete': False,
                })
            st.session_state.co_parameters = co_parameters
        # Display coin_overrides
        st.write(f"{symbol}")
        if st.session_state.co_parameters:
            d = st.session_state.co_parameters
            st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'edit_run_v7_co_parameters_{ed_key}', disabled=['parameter', 'side', 'value'])
        # config.live parameters
        col1, col2, col3, col4 = st.columns([1,1,1,3], vertical_alignment="bottom")
        with col1:
            st.selectbox('config.live override parameter', OVERRIDES_LIVE, key="edit_run_v7_co_parameter_live")
        with col2:
            if st.session_state.edit_run_v7_co_parameter_live == "leverage":
                st.number_input("value", min_value=0.0, max_value=100.0, step=1.0, format="%.1f", key="edit_run_v7_co_value_live")
            else:
                st.selectbox("mode", MODE, key="edit_run_v7_co_value_live")
        with col3:
            st.button("Add", key="edit_run_v7_co_add_parameter_live")

        # config.bot parameters
        col1, col2, col3, col4 = st.columns([1,1,1,3], vertical_alignment="bottom")
        with col1:
            st.selectbox('config.bot override parameter', OVERRIDES, key="edit_run_v7_co_parameter")
        with col2:
            st.selectbox("side", ["long", "short"], key="edit_run_v7_co_side")
        with col3:
            if st.session_state.edit_run_v7_co_parameter == "enforce_exposure_limit":
                st.selectbox("enforce_exposure_limit", ["true", "false"], key="edit_run_v7_co_value")
            else:
                st.number_input("value", format="%.8f", key="edit_run_v7_co_value")
        with col4:
            st.button("Add", key="edit_run_v7_co_add_parameter")

        st.checkbox("Config", value=config, key="edit_run_v7_co_config", help=pbgui_help.coin_overrides_config)
        if config:
            if "co_config" not in st.session_state:
                st.session_state.co_config = ConfigV7()
            st.session_state.co_config.bot.edit_co()
        # print(self.config.coin_overrides)
        col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
        with col1:
            if st.button("OK"):
                # {"COIN1": {"override_config_path": "path/to/override_config.json"}}
                # {"COIN2": {"override_config_path": "path/to/other_override_config.json", {"bot": {"long": {"close_grid_markup_start": 0.005}}}}}
                # {"COIN3": {"bot": {"short": {"entry_initial_qty_pct": 0.01}}, "live": {"forced_mode_long": "panic"}}}
                if st.session_state.edit_run_v7_co_config:
                    st.session_state.co_config.config_file = Path(Path(self.config_file).parent, f'{symbol}.json')
                    st.session_state.co_config.save_config()
                    if symbol not in self.config["coin_overrides"]:
                        self.config["coin_overrides"][symbol] = {}
                    self.config["coin_overrides"][symbol]["override_config_path"] = f'{symbol}.json'
                else:
                    Path(Path(self.config_file).parent, f'{symbol}.json').unlink(missing_ok=True)
                    if symbol in self.config["coin_overrides"]:
                        if "override_config_path" in self.config["coin_overrides"][symbol]:
                            del self.config["coin_overrides"][symbol]["override_config_path"]
                # Remove symbol from coin_overrides if it has no parameters
                if symbol in self.config["coin_overrides"] and self.config["coin_overrides"][symbol] == {}:
                    del self.config["coin_overrides"][symbol]
                # self.save()
                self.clean_co_session_state()
                st.rerun()
        with col2:
            if st.button("Cancel"):
                self.clean_co_session_state()
                st.rerun()
        with col3:
            if st.button("Remove"):
                if self.config["coin_overrides"]:
                    if symbol in self.config["coin_overrides"]:
                        del self.config["coin_overrides"][symbol]
                Path(Path(self.config_file).parent, f'{symbol}.json').unlink(missing_ok=True)
                # self.save()
                self.clean_co_session_state()
                st.rerun()

    def clean_co_session_state(self):
        if "co_config" in st.session_state:
            del st.session_state.co_config
        if "edit_run_v7_co_config" in st.session_state:
            del st.session_state.edit_run_v7_co_config
        if "edit_coin_override" in st.session_state:
            del st.session_state.edit_coin_override
        if "co_data" in st.session_state:
            del st.session_state.co_data
        if "ed_key" in st.session_state:
            st.session_state.ed_key += 1
        if "co_parameters" in st.session_state:
            del st.session_state.co_parameters
        if "edit_run_v7_co_parameter" in st.session_state:
            del st.session_state.edit_run_v7_co_parameter
        if "edit_run_v7_co_parameter_live" in st.session_state:
            del st.session_state.edit_run_v7_co_parameter_live
        if "edit_run_v7_co_side" in st.session_state:
            del st.session_state.edit_run_v7_co_side
        if "edit_run_v7_co_value" in st.session_state:
            del st.session_state.edit_run_v7_co_value
        if "edit_run_v7_co_value_live" in st.session_state:
            del st.session_state.edit_run_v7_co_value_live

class BalanceCalculator:
    def __init__(self, config_file: str = None):
        self.config = ConfigV7()
        if config_file:
            self.config.config_file = config_file
            self.config.load_config()
        self.exchange = Exchange("binance", None)
        self.coin_infos = []
        self.balance_long = []
        self.balance_short = []
    
    @property
    def balance(self):
        return self.balance_long + self.balance_short

    def init_coindata(self):
        if "pbcoindata" not in st.session_state:
            st.session_state.pbcoindata = CoinData()
        st.session_state.pbcoindata.exchange = self.exchange.id
        if self.config.pbgui.dynamic_ignore:
            st.session_state.pbcoindata.tags = self.config.pbgui.tags
            st.session_state.pbcoindata.only_cpt = self.config.pbgui.only_cpt
            st.session_state.pbcoindata.market_cap = self.config.pbgui.market_cap
            st.session_state.pbcoindata.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.pbcoindata.notices_ignore = self.config.pbgui.notices_ignore
            self.config.live.approved_coins = st.session_state.pbcoindata.approved_coins

    def view(self):
        # Init coindata
        self.init_coindata()
        if "edit_bc_config" in st.session_state:
            if st.session_state.edit_bc_config != json.dumps(self.config.config, indent=4):
                try:
                    self.config.config = json.loads(st.session_state.edit_bc_config)
                    self.init_coindata()
                except:
                    error_popup("Invalid JSON")
                    st.session_state.edit_bc_config = json.dumps(self.config.config, indent=4)
        else:
            st.session_state.edit_bc_config = json.dumps(self.config.config, indent=4)

        if "bc_exchange_id" in st.session_state:
            if st.session_state.bc_exchange_id != self.exchange.id:
                self.exchange = Exchange(st.session_state.bc_exchange_id, None)
                # st.session_state.bc_exchange = bc_exchange
        else:
            if self.config.backtest.exchanges:
                st.session_state.bc_exchange_id = self.config.backtest.exchanges[0]
        col1, col2 = st.columns([1, 1])
        with col1:
            st.text_area(f'config', key="edit_bc_config", height=500)
        with col2:
            st.markdown("### Balance Calculator")
            st.markdown("This tool allows you to calculate the balance for a given configuration.")
            st.markdown("You can edit the configuration in the left text area and click on 'Calculate' to see the results.")
            st.selectbox("Exchange", V7.list(), key="bc_exchange_id")
            if st.button("Calculate"):
                coins = set(self.config.live.approved_coins.long + self.config.live.approved_coins.short)
                self.coin_infos = []
                self.balance_long = []
                self.balance_short = []
                with st.spinner(text=f'fetching coin infos from exchange...'):
                    with st.empty():
                        for counter, coin in enumerate(coins):
                            st.text(f'{counter + 1}/{len(coins)}: {coin}')
                            min_order_price, price, contractSize, min_amount, min_cost, lev = self.exchange.fetch_symbol_infos(coin)
                            self.coin_infos.append({
                                "coin": coin,
                                "currentPrice": price,
                                "contractSize": contractSize,
                                "min_amount": min_amount,
                                "min_cost": min_cost,
                                "min_order_price": min_order_price,
                                "max lev": lev
                            })
                            if coin in self.config.live.approved_coins.long:
                                if self.config.bot.long.n_positions > 0 and self.config.bot.long.total_wallet_exposure_limit > 0:
                                    we = self.config.bot.long.total_wallet_exposure_limit / self.config.bot.long.n_positions
                                    balance = min_order_price / (we * self.config.bot.long.entry_initial_qty_pct)
                                    self.balance_long.append({
                                        "coin": coin,
                                        "balance": balance
                                    })
                            if coin in self.config.live.approved_coins.short:
                                if self.config.bot.short.n_positions > 0 and self.config.bot.short.total_wallet_exposure_limit > 0:
                                    we = self.config.bot.short.total_wallet_exposure_limit / self.config.bot.short.n_positions
                                    balance = min_order_price / (we * self.config.bot.short.entry_initial_qty_pct)
                                    self.balance_short.append({
                                        "coin": coin,
                                        "balance": balance
                                    })
                            sleep(0.1)  # to avoid rate limit issues

        # sort coin_infos by min_order_price
        self.coin_infos = sorted(self.coin_infos, key=lambda x: x['min_order_price'], reverse=True)
        if self.coin_infos:
            st.write("### Coin Information")
            st.dataframe(self.coin_infos, hide_index=True)

        # find highest balance in short and long
        self.balance_long = sorted(self.balance_long, key=lambda x: x['balance'], reverse=True)
        self.balance_short = sorted(self.balance_short, key=lambda x: x['balance'], reverse=True)
        side = None
        if self.balance_long:
            if self.balance_short:
                if self.balance_long[0]['balance'] > self.balance_short[0]['balance']:
                    side = "long"
                else:
                    side = "short"
            else:
                side = "long"
        else:
            if self.balance_short:
                side = "short"
        if side in ["long", "short"]:
            # Select the correct attributes based on side
            balance_list = self.balance_long if side == "long" else self.balance_short
            bot_side = self.config.bot.long if side == "long" else self.config.bot.short
            # Get symbol name with highest balance
            symbol = balance_list[0]['coin']
            # get min order price for symbol from coin_infos
            min_order_price = next((coin['min_order_price'] for coin in self.coin_infos if coin['coin'] == symbol), 0)
            # Display calculated balance with formula
            st.write(f"### Balance needed for {symbol} ({side.capitalize()} Side)")
            st.write(f"**Minimum Order Price:** `{min_order_price:.2f}`")
            st.write(f"**Total Wallet Exposure Limit:** `{bot_side.total_wallet_exposure_limit:.2f}`")
            st.write(f"**Number of Positions:** `{bot_side.n_positions}`")
            st.write(f"**Entry Initial Quantity Percentage:** `{bot_side.entry_initial_qty_pct:.2f}`")
            st.write(f"To calculate the balance needed for {symbol} on the {side} side, use the formula:")
            st.write(f"**Formula:** `min_order_price / ((total_wallet_exposure_limit / n_positions) * entry_initial_qty_pct)`")
            result = min_order_price / ((bot_side.total_wallet_exposure_limit / bot_side.n_positions) * bot_side.entry_initial_qty_pct)
            st.write(f"**Calculation:** `{min_order_price} / (({bot_side.total_wallet_exposure_limit} / {bot_side.n_positions}) * {bot_side.entry_initial_qty_pct}) = {result:.2f}`")
            recommended_balance = math.ceil(result * 1.1 / 10) * 10
            st.write(f"### Recommended Balance (10% more): :green[{int(recommended_balance)} USDT]")

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
