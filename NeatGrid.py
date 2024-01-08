import streamlit as st

class NeatGrid:
    GRID_SPAN_MIN = 0.0
    GRID_SPAN_MAX = 10.0
    GRID_SPAN_STEP = 0.01
    GRID_SPAN_ROUND = 2
    GRID_SPAN_FORMAT = f'%.{GRID_SPAN_ROUND}f'
    EMA_SPAN_0_MIN = 1.0
    EMA_SPAN_0_MAX = 1000000.0
    EMA_SPAN_0_STEP = 24.0
    EMA_SPAN_0_ROUND = 1
    EMA_SPAN_0_FORMAT = f'%.{EMA_SPAN_0_ROUND}f'
    EMA_SPAN_1_MIN = 1.0
    EMA_SPAN_1_MAX = 1000000.0
    EMA_SPAN_1_STEP = 24.0
    EMA_SPAN_1_ROUND = 1
    EMA_SPAN_1_FORMAT = f'%.{EMA_SPAN_1_ROUND}f'
    WALLET_EXPOSURE_LIMIT_MIN = 0.0
    WALLET_EXPOSURE_LIMIT_MAX = 10000.0
    WALLET_EXPOSURE_LIMIT_STEP = 0.05
    WALLET_EXPOSURE_LIMIT_ROUND = 2
    WALLET_EXPOSURE_LIMIT_FORMAT = f'%.{WALLET_EXPOSURE_LIMIT_ROUND}f'
    MAX_N_ENTRY_ORDERS_MIN = 1
    MAX_N_ENTRY_ORDERS_MAX = 100
    MAX_N_ENTRY_ORDERS_STEP = 1
    MAX_N_ENTRY_ORDERS_FORMAT = '%d'
    INITIAL_QTY_PCT_MIN = 0.0
    INITIAL_QTY_PCT_MAX = 1.0
    INITIAL_QTY_PCT_STEP = 0.005
    INITIAL_QTY_PCT_ROUND = 4
    INITIAL_QTY_PCT_FORMAT = f'%.{INITIAL_QTY_PCT_ROUND}f'
    INITIAL_EPRICE_EMA_DIST_MIN = -10.0
    INITIAL_EPRICE_EMA_DIST_MAX = 10.0
    INITIAL_EPRICE_EMA_DIST_STEP = 0.001
    INITIAL_EPRICE_EMA_DIST_ROUND = 4
    INITIAL_EPRICE_EMA_DIST_FORMAT = f'%.{INITIAL_EPRICE_EMA_DIST_ROUND}f'
    EQTY_EXP_BASE_MIN = 0.0
    EQTY_EXP_BASE_MAX = 100.0
    EQTY_EXP_BASE_STEP = 0.05
    EQTY_EXP_BASE_ROUND = 2
    EQTY_EXP_BASE_FORMAT = f'%.{EQTY_EXP_BASE_ROUND}f'
    EPRICE_EXP_BASE_MIN = 0.0
    EPRICE_EXP_BASE_MAX = 100.0
    EPRICE_EXP_BASE_STEP = 0.05
    EPRICE_EXP_BASE_ROUND = 2
    EPRICE_EXP_BASE_FORMAT = f'%.{EPRICE_EXP_BASE_ROUND}f'
    MIN_MARKUP_MIN = 0.0
    MIN_MARKUP_MAX = 10.0
    MIN_MARKUP_STEP = 0.005
    MIN_MARKUP_ROUND = 4
    MIN_MARKUP_FORMAT = f'%.{MIN_MARKUP_ROUND}f'
    MARKUP_RANGE_MIN = 0.0
    MARKUP_RANGE_MAX = 10.0
    MARKUP_RANGE_STEP = 0.01
    MARKUP_RANGE_ROUND = 2
    MARKUP_RANGE_FORMAT = f'%.{MARKUP_RANGE_ROUND}f'
    N_CLOSE_ORDERS_MIN = 1
    N_CLOSE_ORDERS_MAX = 100
    N_CLOSE_ORDERS_STEP = 1
    N_CLOSE_ORDERS_FORMAT = '%d'
    AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN = 0.0
    AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX = 1.0
    AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP = 0.05
    AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND = 2
    AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT = f'%.{AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND}f'
    AUTO_UNSTUCK_EMA_DIST_MIN = -10.0
    AUTO_UNSTUCK_EMA_DIST_MAX = 10.0
    AUTO_UNSTUCK_EMA_DIST_STEP = 0.0005
    AUTO_UNSTUCK_EMA_DIST_ROUND = 4
    AUTO_UNSTUCK_EMA_DIST_FORMAT = f'%.{AUTO_UNSTUCK_EMA_DIST_ROUND}f'
    AUTO_UNSTUCK_DELAY_MINUTES_MIN = 0.0
    AUTO_UNSTUCK_DELAY_MINUTES_MAX = 1000000.0
    AUTO_UNSTUCK_DELAY_MINUTES_STEP = 10.0
    AUTO_UNSTUCK_DELAY_MINUTES_ROUND = 1
    AUTO_UNSTUCK_DELAY_MINUTES_FORMAT = f'%.{AUTO_UNSTUCK_DELAY_MINUTES_ROUND}f'
    AUTO_UNSTUCK_QTY_PCT_MIN = 0.0
    AUTO_UNSTUCK_QTY_PCT_MAX = 1.0
    AUTO_UNSTUCK_QTY_PCT_STEP = 0.01
    AUTO_UNSTUCK_QTY_PCT_ROUND = 3
    AUTO_UNSTUCK_QTY_PCT_FORMAT = f'%.{AUTO_UNSTUCK_QTY_PCT_ROUND}f'

    def __init__(self):
        self._config = None
        self._grid_span_0 = 0.05
        self._grid_span_1 = 0.7
        self._ema_span_0_0 = 5.0
        self._ema_span_0_1 = 1440.0
        self._ema_span_1_0 = 5.0
        self._ema_span_1_1 = 1440.0
        self._wallet_exposure_limit_0 = 1.0
        self._wallet_exposure_limit_1 = 1.0
        self._max_n_entry_orders_0 = 7
        self._max_n_entry_orders_1 = 22
        self._initial_qty_pct_0 = 0.01
        self._initial_qty_pct_1 = 0.05
        self._initial_eprice_ema_dist_0 = -0.1
        self._initial_eprice_ema_dist_1 = 0.003
        self._eqty_exp_base_0 = 1.0
        self._eqty_exp_base_1 = 3.0
        self._eprice_exp_base_0 = 1.0
        self._eprice_exp_base_1 = 3.0
        self._min_markup_0 = 0.001
        self._min_markup_1 = 0.01
        self._markup_range_0 = 0.0
        self._markup_range_1 = 0.06
        self._n_close_orders_0 = 2
        self._n_close_orders_1 = 16
        self._auto_unstuck_wallet_exposure_threshold_0 = 0.1
        self._auto_unstuck_wallet_exposure_threshold_1 = 0.9
        self._auto_unstuck_ema_dist_0 = -0.1
        self._auto_unstuck_ema_dist_1 = 0.003
        self._auto_unstuck_delay_minutes_0 = 1.0
        self._auto_unstuck_delay_minutes_1 = 1440.0
        self._auto_unstuck_qty_pct_0 = 0.01
        self._auto_unstuck_qty_pct_1 = 0.1

    @property
    def config(self): return self._config
    @config.setter
    def config(self, new_config):
        self._config = new_config
        if "grid_span" in new_config:
            self._grid_span_0 = float(self._config["grid_span"][0])
            self._grid_span_1 = float(self._config["grid_span"][1])
            if self._grid_span_0 < self.GRID_SPAN_MIN:
                self._grid_span_0 = self.GRID_SPAN_MIN
            if self._grid_span_1 > self.GRID_SPAN_MAX:
                self._grid_span_1 = self.GRID_SPAN_MAX
        if "ema_span_0" in new_config:
            self._ema_span_0_0 = float(self._config["ema_span_0"][0])
            self._ema_span_0_1 = float(self._config["ema_span_0"][1])
            if self._ema_span_0_0 < self.EMA_SPAN_0_MIN:
                self._ema_span_0_0 = self.EMA_SPAN_0_MIN
            if self._ema_span_0_1 > self.EMA_SPAN_0_MAX:
                self._ema_span_0_1 = self.EMA_SPAN_0_MAX
        if "ema_span_1" in new_config:
            self._ema_span_1_0 = float(self._config["ema_span_1"][0])
            self._ema_span_1_1 = float(self._config["ema_span_1"][1])
            if self._ema_span_1_0 < self.EMA_SPAN_1_MIN:
                self._ema_span_1_0 = self.EMA_SPAN_1_MIN
            if self._ema_span_1_1 > self.EMA_SPAN_1_MAX:
                self._ema_span_1_1 = self.EMA_SPAN_1_MAX
        if "wallet_exposure_limit" in new_config:
            self._wallet_exposure_limit_0 = float(self._config["wallet_exposure_limit"][0])
            self._wallet_exposure_limit_1 = float(self._config["wallet_exposure_limit"][1])
            if self._wallet_exposure_limit_0 < self.WALLET_EXPOSURE_LIMIT_MIN:
                self._wallet_exposure_limit_0 = self.WALLET_EXPOSURE_LIMIT_MIN
            if self._wallet_exposure_limit_1 > self.WALLET_EXPOSURE_LIMIT_MAX:
                self._wallet_exposure_limit_1 = self.WALLET_EXPOSURE_LIMIT_MAX
        if "max_n_entry_orders" in new_config:
            self._max_n_entry_orders_0 = int(self._config["max_n_entry_orders"][0])
            self._max_n_entry_orders_1 = int(self._config["max_n_entry_orders"][1])
            if self._max_n_entry_orders_0 < self.MAX_N_ENTRY_ORDERS_MIN:
                self._max_n_entry_orders_0 = self.MAX_N_ENTRY_ORDERS_MIN
            if self._max_n_entry_orders_1 > self.MAX_N_ENTRY_ORDERS_MAX:
                self._max_n_entry_orders_1 = self.MAX_N_ENTRY_ORDERS_MAX
        if "initial_qty_pct" in new_config:
            self._initial_qty_pct_0 = float(self._config["initial_qty_pct"][0])
            self._initial_qty_pct_1 = float(self._config["initial_qty_pct"][1])
            if self._initial_qty_pct_0 < self.INITIAL_QTY_PCT_MIN:
                self._initial_qty_pct_0 = self.INITIAL_QTY_PCT_MIN
            if self._initial_qty_pct_1 > self.INITIAL_QTY_PCT_MAX:
                self._initial_qty_pct_1 = self.INITIAL_QTY_PCT_MAX
        if "initial_eprice_ema_dist" in new_config:
            self._initial_eprice_ema_dist_0 = float(self._config["initial_eprice_ema_dist"][0])
            self._initial_eprice_ema_dist_1 = float(self._config["initial_eprice_ema_dist"][1])
            if self._initial_eprice_ema_dist_0 < self.INITIAL_EPRICE_EMA_DIST_MIN:
                self._initial_eprice_ema_dist_0 = self.INITIAL_EPRICE_EMA_DIST_MIN
            if self._initial_eprice_ema_dist_1 > self.INITIAL_EPRICE_EMA_DIST_MAX:
                self._initial_eprice_ema_dist_1 = self.INITIAL_EPRICE_EMA_DIST_MAX
        if "eqty_exp_base" in new_config:
            self._eqty_exp_base_0 = float(self._config["eqty_exp_base"][0])
            self._eqty_exp_base_1 = float(self._config["eqty_exp_base"][1])
            if self._eqty_exp_base_0 < self.EQTY_EXP_BASE_MIN:
                self._eqty_exp_base_0 = self.EQTY_EXP_BASE_MIN
            if self._eqty_exp_base_1 > self.EQTY_EXP_BASE_MAX:
                self._eqty_exp_base_1 = self.EQTY_EXP_BASE_MAX
        if "eprice_exp_base" in new_config:
            self._eprice_exp_base_0 = float(self._config["eprice_exp_base"][0])
            self._eprice_exp_base_1 = float(self._config["eprice_exp_base"][1])
            if self._eprice_exp_base_0 < self.EPRICE_EXP_BASE_MIN:
                self._eprice_exp_base_0 = self.EPRICE_EXP_BASE_MIN
            if self._eprice_exp_base_1 > self.EPRICE_EXP_BASE_MAX:
                self._eprice_exp_base_1 = self.EPRICE_EXP_BASE_MAX
        if "min_markup" in new_config:
            self._min_markup_0 = float(self._config["min_markup"][0])
            self._min_markup_1 = float(self._config["min_markup"][1])
            if self._min_markup_0 < self.MIN_MARKUP_MIN:
                self._min_markup_0 = self.MIN_MARKUP_MIN
            if self._min_markup_1 > self.MIN_MARKUP_MAX:
                self._min_markup_1 = self.MIN_MARKUP_MAX
        if "markup_range" in new_config:
            self._markup_range_0 = float(self._config["markup_range"][0])
            self._markup_range_1 = float(self._config["markup_range"][1])
            if self._markup_range_0 < self.MARKUP_RANGE_MIN:
                self._markup_range_0 = self.MARKUP_RANGE_MIN
            if self._markup_range_1 > self.MARKUP_RANGE_MAX:
                self._markup_range_1 = self.MARKUP_RANGE_MAX
        if "n_close_orders" in new_config:
            self._n_close_orders_0 = int(self._config["n_close_orders"][0])
            self._n_close_orders_1 = int(self._config["n_close_orders"][1])
            if self._n_close_orders_0 < self.N_CLOSE_ORDERS_MIN:
                self._n_close_orders_0 = self.N_CLOSE_ORDERS_MIN
            if self._n_close_orders_1 > self.N_CLOSE_ORDERS_MAX:
                self._n_close_orders_1 = self.N_CLOSE_ORDERS_MAX
        if "auto_unstuck_wallet_exposure_threshold" in new_config:
            self._auto_unstuck_wallet_exposure_threshold_0 = float(self._config["auto_unstuck_wallet_exposure_threshold"][0])
            self._auto_unstuck_wallet_exposure_threshold_1 = float(self._config["auto_unstuck_wallet_exposure_threshold"][1])
            if self._auto_unstuck_wallet_exposure_threshold_0 < self.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN:
                self._auto_unstuck_wallet_exposure_threshold_0 = self.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN
            if self._auto_unstuck_wallet_exposure_threshold_1 > self.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX:
                self._auto_unstuck_wallet_exposure_threshold_1 = self.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX
        if "auto_unstuck_ema_dist" in new_config:
            self._auto_unstuck_ema_dist_0 = float(self._config["auto_unstuck_ema_dist"][0])
            self._auto_unstuck_ema_dist_1 = float(self._config["auto_unstuck_ema_dist"][1])
            if self._auto_unstuck_ema_dist_0 < self.AUTO_UNSTUCK_EMA_DIST_MIN:
                self._auto_unstuck_ema_dist_0 = self.AUTO_UNSTUCK_EMA_DIST_MIN
            if self._auto_unstuck_ema_dist_1 > self.AUTO_UNSTUCK_EMA_DIST_MAX:
                self._auto_unstuck_ema_dist_1 = self.AUTO_UNSTUCK_EMA_DIST_MAX
        if "auto_unstuck_delay_minutes" in new_config:
            self._auto_unstuck_delay_minutes_0 = float(self._config["auto_unstuck_delay_minutes"][0])
            self._auto_unstuck_delay_minutes_1 = float(self._config["auto_unstuck_delay_minutes"][1])
            if self._auto_unstuck_delay_minutes_0 < self.AUTO_UNSTUCK_DELAY_MINUTES_MIN:
                self._auto_unstuck_delay_minutes_0 = self.AUTO_UNSTUCK_DELAY_MINUTES_MIN
            if self._auto_unstuck_delay_minutes_1 > self.AUTO_UNSTUCK_DELAY_MINUTES_MAX:
                self._auto_unstuck_delay_minutes_1 = self.AUTO_UNSTUCK_DELAY_MINUTES_MAX
        if "auto_unstuck_qty_pct" in new_config:
            self._auto_unstuck_qty_pct_0 = float(self._config["auto_unstuck_qty_pct"][0])
            self._auto_unstuck_qty_pct_1 = float(self._config["auto_unstuck_qty_pct"][1])
            if self._auto_unstuck_qty_pct_0 < self.AUTO_UNSTUCK_QTY_PCT_MIN:
                self._auto_unstuck_qty_pct_0 = self.AUTO_UNSTUCK_QTY_PCT_MIN
            if self._auto_unstuck_qty_pct_1 > self.AUTO_UNSTUCK_QTY_PCT_MAX:
                self._auto_unstuck_qty_pct_1 = self.AUTO_UNSTUCK_QTY_PCT_MAX

    @property
    def grid_span_0(self): return self._grid_span_0
    @property
    def grid_span_1(self): return self._grid_span_1
    @property
    def ema_span_0_0(self): return self._ema_span_0_0
    @property
    def ema_span_0_1(self): return self._ema_span_0_1
    @property
    def ema_span_1_0(self): return self._ema_span_1_0
    @property
    def ema_span_1_1(self): return self._ema_span_1_1
    @property
    def wallet_exposure_limit_0(self): return self._wallet_exposure_limit_0
    @property
    def wallet_exposure_limit_1(self): return self._wallet_exposure_limit_1
    @property
    def max_n_entry_orders_0(self): return self._max_n_entry_orders_0
    @property
    def max_n_entry_orders_1(self): return self._max_n_entry_orders_1
    @property
    def initial_qty_pct_0(self): return self._initial_qty_pct_0
    @property
    def initial_qty_pct_1(self): return self._initial_qty_pct_1
    @property
    def initial_eprice_ema_dist_0(self): return self._initial_eprice_ema_dist_0
    @property
    def initial_eprice_ema_dist_1(self): return self._initial_eprice_ema_dist_1
    @property
    def eqty_exp_base_0(self): return self._eqty_exp_base_0
    @property
    def eqty_exp_base_1(self): return self._eqty_exp_base_1
    @property
    def eprice_exp_base_0(self): return self._eprice_exp_base_0
    @property
    def eprice_exp_base_1(self): return self._eprice_exp_base_1
    @property
    def min_markup_0(self): return self._min_markup_0
    @property
    def min_markup_1(self): return self._min_markup_1
    @property
    def markup_range_0(self): return self._markup_range_0
    @property
    def markup_range_1(self): return self._markup_range_1
    @property
    def n_close_orders_0(self): return self._n_close_orders_0
    @property
    def n_close_orders_1(self): return self._n_close_orders_1
    @property
    def auto_unstuck_wallet_exposure_threshold_0(self): return self._auto_unstuck_wallet_exposure_threshold_0
    @property
    def auto_unstuck_wallet_exposure_threshold_1(self): return self._auto_unstuck_wallet_exposure_threshold_1
    @property
    def auto_unstuck_ema_dist_0(self): return self._auto_unstuck_ema_dist_0
    @property
    def auto_unstuck_ema_dist_1(self): return self._auto_unstuck_ema_dist_1
    @property
    def auto_unstuck_delay_minutes_0(self): return self._auto_unstuck_delay_minutes_0
    @property
    def auto_unstuck_delay_minutes_1(self): return self._auto_unstuck_delay_minutes_1
    @property
    def auto_unstuck_qty_pct_0(self): return self._auto_unstuck_qty_pct_0
    @property
    def auto_unstuck_qty_pct_1(self): return self._auto_unstuck_qty_pct_1

    @grid_span_0.setter
    def grid_span_0(self, new_grid_span_0):
        if self._grid_span_0 != new_grid_span_0:
            self._grid_span_0 = new_grid_span_0
            self._config["grid_span"][0] = self._grid_span_0
            st.experimental_rerun()
    @grid_span_1.setter
    def grid_span_1(self, new_grid_span_1):
        if self._grid_span_1 != new_grid_span_1:
            self._grid_span_1 = new_grid_span_1
            self._config["grid_span"][1] = self._grid_span_1
            st.experimental_rerun()
    @ema_span_0_0.setter
    def ema_span_0_0(self, new_ema_span_0_0):
        if self._ema_span_0_0 != new_ema_span_0_0:
            self._ema_span_0_0 = new_ema_span_0_0
            self._config["ema_span_0"][0] = self._ema_span_0_0
            st.experimental_rerun()
    @ema_span_0_1.setter
    def ema_span_0_1(self, new_ema_span_0_1):
        if self._ema_span_0_1 != new_ema_span_0_1:
            self._ema_span_0_1 = new_ema_span_0_1
            self._config["ema_span_0"][1] = self._ema_span_0_1
            st.experimental_rerun()
    @ema_span_1_0.setter
    def ema_span_1_0(self, new_ema_span_1_0):
        if self._ema_span_1_0 != new_ema_span_1_0:
            self._ema_span_1_0 = new_ema_span_1_0
            self._config["ema_span_1"][0] = self._ema_span_1_0
            st.experimental_rerun()
    @ema_span_1_1.setter
    def ema_span_1_1(self, new_ema_span_1_1):
        if self._ema_span_1_1 != new_ema_span_1_1:
            self._ema_span_1_1 = new_ema_span_1_1
            self._config["ema_span_1"][1] = self._ema_span_1_1
            st.experimental_rerun()
    @wallet_exposure_limit_0.setter
    def wallet_exposure_limit_0(self, new_wallet_exposure_limit_0):
        if self._wallet_exposure_limit_0 != new_wallet_exposure_limit_0:
            self._wallet_exposure_limit_0 = new_wallet_exposure_limit_0
            self._config["wallet_exposure_limit"][0] = self._wallet_exposure_limit_0
            st.experimental_rerun()
    @wallet_exposure_limit_1.setter
    def wallet_exposure_limit_1(self, new_wallet_exposure_limit_1):
        if self._wallet_exposure_limit_1 != new_wallet_exposure_limit_1:
            self._wallet_exposure_limit_1 = new_wallet_exposure_limit_1
            self._config["wallet_exposure_limit"][1] = self._wallet_exposure_limit_1
            st.experimental_rerun()
    @max_n_entry_orders_0.setter
    def max_n_entry_orders_0(self, new_max_n_entry_orders_0):
        if self._max_n_entry_orders_0 != new_max_n_entry_orders_0:
            self._max_n_entry_orders_0 = new_max_n_entry_orders_0
            self._config["max_n_entry_orders"][0] = self._max_n_entry_orders_0
            st.experimental_rerun()
    @max_n_entry_orders_1.setter
    def max_n_entry_orders_1(self, new_max_n_entry_orders_1):
        if self._max_n_entry_orders_1 != new_max_n_entry_orders_1:
            self._max_n_entry_orders_1 = new_max_n_entry_orders_1
            self._config["max_n_entry_orders"][1] = self._max_n_entry_orders_1
            st.experimental_rerun()
    @initial_qty_pct_0.setter
    def initial_qty_pct_0(self, new_initial_qty_pct_0):
        if self._initial_qty_pct_0 != new_initial_qty_pct_0:
            self._initial_qty_pct_0 = new_initial_qty_pct_0
            self._config["initial_qty_pct"][0] = self._initial_qty_pct_0
            st.experimental_rerun()
    @initial_qty_pct_1.setter
    def initial_qty_pct_1(self, new_initial_qty_pct_1):
        if self._initial_qty_pct_1 != new_initial_qty_pct_1:
            self._initial_qty_pct_1 = new_initial_qty_pct_1
            self._config["initial_qty_pct"][1] = self._initial_qty_pct_1
            st.experimental_rerun()
    @initial_eprice_ema_dist_0.setter
    def initial_eprice_ema_dist_0(self, new_initial_eprice_ema_dist_0):
        if self._initial_eprice_ema_dist_0 != new_initial_eprice_ema_dist_0:
            self._initial_eprice_ema_dist_0 = new_initial_eprice_ema_dist_0
            self._config["initial_eprice_ema_dist"][0] = self._initial_eprice_ema_dist_0
            st.experimental_rerun()
    @initial_eprice_ema_dist_1.setter
    def initial_eprice_ema_dist_1(self, new_initial_eprice_ema_dist_1):
        if self._initial_eprice_ema_dist_1 != new_initial_eprice_ema_dist_1:
            self._initial_eprice_ema_dist_1 = new_initial_eprice_ema_dist_1
            self._config["initial_eprice_ema_dist"][1] = self._initial_eprice_ema_dist_1
            st.experimental_rerun()
    @eqty_exp_base_0.setter
    def eqty_exp_base_0(self, new_eqty_exp_base_0):
        if self._eqty_exp_base_0 != new_eqty_exp_base_0:
            self._eqty_exp_base_0 = new_eqty_exp_base_0
            self._config["eqty_exp_base"][0] = self._eqty_exp_base_0
            st.experimental_rerun()
    @eqty_exp_base_1.setter
    def eqty_exp_base_1(self, new_eqty_exp_base_1):
        if self._eqty_exp_base_1 != new_eqty_exp_base_1:
            self._eqty_exp_base_1 = new_eqty_exp_base_1
            self._config["eqty_exp_base"][1] = self._eqty_exp_base_1
            st.experimental_rerun()
    @eprice_exp_base_0.setter
    def eprice_exp_base_0(self, new_eprice_exp_base_0):
        if self._eprice_exp_base_0 != new_eprice_exp_base_0:
            self._eprice_exp_base_0 = new_eprice_exp_base_0
            self._config["eprice_exp_base"][0] = self._eprice_exp_base_0
            st.experimental_rerun()
    @eprice_exp_base_1.setter
    def eprice_exp_base_1(self, new_eprice_exp_base_1):
        if self._eprice_exp_base_1 != new_eprice_exp_base_1:
            self._eprice_exp_base_1 = new_eprice_exp_base_1
            self._config["eprice_exp_base"][1] = self._eprice_exp_base_1
            st.experimental_rerun()
    @min_markup_0.setter
    def min_markup_0(self, new_min_markup_0):
        if self._min_markup_0 != new_min_markup_0:
            self._min_markup_0 = new_min_markup_0
            self._config["min_markup"][0] = self._min_markup_0
            st.experimental_rerun()
    @min_markup_1.setter
    def min_markup_1(self, new_min_markup_1):
        if self._min_markup_1 != new_min_markup_1:
            self._min_markup_1 = new_min_markup_1
            self._config["min_markup"][1] = self._min_markup_1
            st.experimental_rerun()
    @markup_range_0.setter
    def markup_range_0(self, new_markup_range_0):
        if self._markup_range_0 != new_markup_range_0:
            self._markup_range_0 = new_markup_range_0
            self._config["markup_range"][0] = self._markup_range_0
            st.experimental_rerun()
    @markup_range_1.setter
    def markup_range_1(self, new_markup_range_1):
        if self._markup_range_1 != new_markup_range_1:
            self._markup_range_1 = new_markup_range_1
            self._config["markup_range"][1] = self._markup_range_1
            st.experimental_rerun()
    @n_close_orders_0.setter
    def n_close_orders_0(self, new_n_close_orders_0):
        if self._n_close_orders_0 != new_n_close_orders_0:
            self._n_close_orders_0 = new_n_close_orders_0
            self._config["n_close_orders"][0] = self._n_close_orders_0
            st.experimental_rerun()
    @n_close_orders_1.setter
    def n_close_orders_1(self, new_n_close_orders_1):
        if self._n_close_orders_1 != new_n_close_orders_1:
            self._n_close_orders_1 = new_n_close_orders_1
            self._config["n_close_orders"][1] = self._n_close_orders_1
            st.experimental_rerun()
    @auto_unstuck_wallet_exposure_threshold_0.setter
    def auto_unstuck_wallet_exposure_threshold_0(self, new_auto_unstuck_wallet_exposure_threshold_0):
        if self._auto_unstuck_wallet_exposure_threshold_0 != new_auto_unstuck_wallet_exposure_threshold_0:
            self._auto_unstuck_wallet_exposure_threshold_0 = new_auto_unstuck_wallet_exposure_threshold_0
            self._config["auto_unstuck_wallet_exposure_threshold"][0] = self._auto_unstuck_wallet_exposure_threshold_0
            st.experimental_rerun()
    @auto_unstuck_wallet_exposure_threshold_1.setter
    def auto_unstuck_wallet_exposure_threshold_1(self, new_auto_unstuck_wallet_exposure_threshold_1):
        if self._auto_unstuck_wallet_exposure_threshold_1 != new_auto_unstuck_wallet_exposure_threshold_1:
            self._auto_unstuck_wallet_exposure_threshold_1 = new_auto_unstuck_wallet_exposure_threshold_1
            self._config["auto_unstuck_wallet_exposure_threshold"][1] = self._auto_unstuck_wallet_exposure_threshold_1
            st.experimental_rerun()
    @auto_unstuck_ema_dist_0.setter
    def auto_unstuck_ema_dist_0(self, new_auto_unstuck_ema_dist_0):
        if self._auto_unstuck_ema_dist_0 != new_auto_unstuck_ema_dist_0:
            self._auto_unstuck_ema_dist_0 = new_auto_unstuck_ema_dist_0
            self._config["auto_unstuck_ema_dist"][0] = self._auto_unstuck_ema_dist_0
            st.experimental_rerun()
    @auto_unstuck_ema_dist_1.setter
    def auto_unstuck_ema_dist_1(self, new_auto_unstuck_ema_dist_1):
        if self._auto_unstuck_ema_dist_1 != new_auto_unstuck_ema_dist_1:
            self._auto_unstuck_ema_dist_1 = new_auto_unstuck_ema_dist_1
            self._config["auto_unstuck_ema_dist"][1] = self._auto_unstuck_ema_dist_1
            st.experimental_rerun()
    @auto_unstuck_delay_minutes_0.setter
    def auto_unstuck_delay_minutes_0(self, new_auto_unstuck_delay_minutes_0):
        if self._auto_unstuck_delay_minutes_0 != new_auto_unstuck_delay_minutes_0:
            self._auto_unstuck_delay_minutes_0 = new_auto_unstuck_delay_minutes_0
            self._config["auto_unstuck_delay_minutes"][0] = self._auto_unstuck_delay_minutes_0
            st.experimental_rerun()
    @auto_unstuck_delay_minutes_1.setter
    def auto_unstuck_delay_minutes_1(self, new_auto_unstuck_delay_minutes_1):
        if self._auto_unstuck_delay_minutes_1 != new_auto_unstuck_delay_minutes_1:
            self._auto_unstuck_delay_minutes_1 = new_auto_unstuck_delay_minutes_1
            self._config["auto_unstuck_delay_minutes"][1] = self._auto_unstuck_delay_minutes_1
            st.experimental_rerun()
    @auto_unstuck_qty_pct_0.setter
    def auto_unstuck_qty_pct_0(self, new_auto_unstuck_qty_pct_0):
        if self._auto_unstuck_qty_pct_0 != new_auto_unstuck_qty_pct_0:
            self._auto_unstuck_qty_pct_0 = new_auto_unstuck_qty_pct_0
            self._config["auto_unstuck_qty_pct"][0] = self._auto_unstuck_qty_pct_0
            st.experimental_rerun()
    @auto_unstuck_qty_pct_1.setter
    def auto_unstuck_qty_pct_1(self, new_auto_unstuck_qty_pct_1):
        if self._auto_unstuck_qty_pct_1 != new_auto_unstuck_qty_pct_1:
            self._auto_unstuck_qty_pct_1 = new_auto_unstuck_qty_pct_1
            self._config["auto_unstuck_qty_pct"][1] = self._auto_unstuck_qty_pct_1
            st.experimental_rerun()
