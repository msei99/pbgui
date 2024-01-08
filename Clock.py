import streamlit as st


class Clock:
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
    EMA_DIST_ENTRY_MIN = -10.0
    EMA_DIST_ENTRY_MAX = 10.0
    EMA_DIST_ENTRY_STEP = 0.001
    EMA_DIST_ENTRY_ROUND = 4
    EMA_DIST_ENTRY_FORMAT = f'%.{EMA_DIST_ENTRY_ROUND}f'
    EMA_DIST_CLOSE_MIN = -10.0
    EMA_DIST_CLOSE_MAX = 10.0
    EMA_DIST_CLOSE_STEP = 0.001
    EMA_DIST_CLOSE_ROUND = 4
    EMA_DIST_CLOSE_FORMAT = f'%.{EMA_DIST_CLOSE_ROUND}f'
    QTY_PCT_ENTRY_MIN = 0.0
    QTY_PCT_ENTRY_MAX = 1.0
    QTY_PCT_ENTRY_STEP = 0.001
    QTY_PCT_ENTRY_ROUND = 4
    QTY_PCT_ENTRY_FORMAT = f'%.{QTY_PCT_ENTRY_ROUND}f'
    QTY_PCT_CLOSE_MIN = 0.0
    QTY_PCT_CLOSE_MAX = 1.0
    QTY_PCT_CLOSE_STEP = 0.001
    QTY_PCT_CLOSE_ROUND = 4
    QTY_PCT_CLOSE_FORMAT = f'%.{QTY_PCT_CLOSE_ROUND}f'
    WE_MULTIPLIER_ENTRY_MIN = 0.0
    WE_MULTIPLIER_ENTRY_MAX = 100.0
    WE_MULTIPLIER_ENTRY_STEP = 1.0
    WE_MULTIPLIER_ENTRY_ROUND = 1
    WE_MULTIPLIER_ENTRY_FORMAT = f'%.{WE_MULTIPLIER_ENTRY_ROUND}f'
    WE_MULTIPLIER_CLOSE_MIN = 0.0
    WE_MULTIPLIER_CLOSE_MAX = 100.0
    WE_MULTIPLIER_CLOSE_STEP = 1.0
    WE_MULTIPLIER_CLOSE_ROUND = 1
    WE_MULTIPLIER_CLOSE_FORMAT = f'%.{WE_MULTIPLIER_CLOSE_ROUND}f'
    DELAY_WEIGHT_ENTRY_MIN = 0.0
    DELAY_WEIGHT_ENTRY_MAX = 100.0
    DELAY_WEIGHT_ENTRY_STEP = 10.0
    DELAY_WEIGHT_ENTRY_ROUND = 1
    DELAY_WEIGHT_ENTRY_FORMAT = f'%.{DELAY_WEIGHT_ENTRY_ROUND}f'
    DELAY_WEIGHT_CLOSE_MIN = 0.0
    DELAY_WEIGHT_CLOSE_MAX = 100.0
    DELAY_WEIGHT_CLOSE_STEP = 10.0
    DELAY_WEIGHT_CLOSE_ROUND = 1
    DELAY_WEIGHT_CLOSE_FORMAT = f'%.{DELAY_WEIGHT_CLOSE_ROUND}f'
    DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MIN = 1.0
    DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MAX = 1000000.0
    DELAY_BETWEEN_FILLS_MINUTES_ENTRY_STEP = 10.0
    DELAY_BETWEEN_FILLS_MINUTES_ENTRY_ROUND = 1
    DELAY_BETWEEN_FILLS_MINUTES_ENTRY_FORMAT = f'%.{DELAY_BETWEEN_FILLS_MINUTES_ENTRY_ROUND}f'
    DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MIN = 1.0
    DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MAX = 1000000.0
    DELAY_BETWEEN_FILLS_MINUTES_CLOSE_STEP = 10.0
    DELAY_BETWEEN_FILLS_MINUTES_CLOSE_ROUND = 1
    DELAY_BETWEEN_FILLS_MINUTES_CLOSE_FORMAT = f'%.{DELAY_BETWEEN_FILLS_MINUTES_CLOSE_ROUND}f'
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
    WALLET_EXPOSURE_LIMIT_MIN = 0.0
    WALLET_EXPOSURE_LIMIT_MAX = 10000.0
    WALLET_EXPOSURE_LIMIT_STEP = 0.05
    WALLET_EXPOSURE_LIMIT_ROUND = 2
    WALLET_EXPOSURE_LIMIT_FORMAT = f'%.{WALLET_EXPOSURE_LIMIT_ROUND}f'

    def __init__(self):
        self._config = None
        self._ema_span_0_0 = 5.0
        self._ema_span_0_1 = 1440.0
        self._ema_span_1_0 = 5.0
        self._ema_span_1_1 = 1440.0
        self._ema_dist_entry_0 = -0.05
        self._ema_dist_entry_1 = 0.003
        self._ema_dist_close_0 = -0.01
        self._ema_dist_close_1 = 0.003
        self._qty_pct_entry_0 = 0.01
        self._qty_pct_entry_1 = 0.05
        self._qty_pct_close_0 = 0.01
        self._qty_pct_close_1 = 0.05
        self._we_multiplier_entry_0 = 0.0
        self._we_multiplier_entry_1 = 10.0
        self._we_multiplier_close_0 = 0.0
        self._we_multiplier_close_1 = 0.0
        self._delay_weight_entry_0 = 0.0
        self._delay_weight_entry_1 = 100.0
        self._delay_weight_close_0 = 0.0
        self._delay_weight_close_1 = 100.0
        self._delay_between_fills_minutes_entry_0 = 1.0
        self._delay_between_fills_minutes_entry_1 = 1440.0
        self._delay_between_fills_minutes_close_0 = 120.0
        self._delay_between_fills_minutes_close_1 = 1440.0
        self._min_markup_0 = 0.001
        self._min_markup_1 = 0.01
        self._markup_range_0 = 0.0
        self._markup_range_1 = 0.06
        self._n_close_orders_0 = 2
        self._n_close_orders_1 = 16
        self._wallet_exposure_limit_0 = 1.0
        self._wallet_exposure_limit_1 = 1.0

    @property
    def config(self): return self._config
    @config.setter
    def config(self, new_config):
        self._config = new_config
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
        if "ema_dist_entry" in new_config:
            self._ema_dist_entry_0 = float(self._config["ema_dist_entry"][0])
            self._ema_dist_entry_1 = float(self._config["ema_dist_entry"][1])
            if self._ema_dist_entry_0 < self.EMA_DIST_ENTRY_MIN:
                self._ema_dist_entry_0 = self.EMA_DIST_ENTRY_MIN
            if self._ema_dist_entry_1 > self.EMA_DIST_ENTRY_MAX:
                self._ema_dist_entry_1 = self.EMA_DIST_ENTRY_MAX
        if "ema_dist_close" in new_config:
            self._ema_dist_close_0 = float(self._config["ema_dist_close"][0])
            self._ema_dist_close_1 = float(self._config["ema_dist_close"][1])
            if self._ema_dist_close_0 < self.EMA_DIST_CLOSE_MIN:
                self._ema_dist_close_0 = self.EMA_DIST_CLOSE_MIN
            if self._ema_dist_close_1 > self.EMA_DIST_CLOSE_MAX:
                self._ema_dist_close_1 = self.EMA_DIST_CLOSE_MAX
        if "qty_pct_entry" in new_config:
            self._qty_pct_entry_0 = float(self._config["qty_pct_entry"][0])
            self._qty_pct_entry_1 = float(self._config["qty_pct_entry"][1])
            if self._qty_pct_entry_0 < self.QTY_PCT_ENTRY_MIN:
                self._qty_pct_entry_0 = self.QTY_PCT_ENTRY_MIN
            if self._qty_pct_entry_1 > self.QTY_PCT_ENTRY_MAX:
                self._qty_pct_entry_1 = self.QTY_PCT_ENTRY_MAX
        if "qty_pct_close" in new_config:
            self._qty_pct_close_0 = float(self._config["qty_pct_close"][0])
            self._qty_pct_close_1 = float(self._config["qty_pct_close"][1])
            if self._qty_pct_close_0 < self.QTY_PCT_CLOSE_MIN:
                self._qty_pct_close_0 = self.QTY_PCT_CLOSE_MIN
            if self._qty_pct_close_1 > self.QTY_PCT_CLOSE_MAX:
                self._qty_pct_close_1 = self.QTY_PCT_CLOSE_MAX
        if "we_multiplier_entry" in new_config:
            self._we_multiplier_entry_0 = float(self._config["we_multiplier_entry"][0])
            self._we_multiplier_entry_1 = float(self._config["we_multiplier_entry"][1])
            if self._we_multiplier_entry_0 < self.WE_MULTIPLIER_ENTRY_MIN:
                self._we_multiplier_entry_0 = self.WE_MULTIPLIER_ENTRY_MIN
            if self._we_multiplier_entry_1 > self.WE_MULTIPLIER_ENTRY_MAX:
                self._we_multiplier_entry_1 = self.WE_MULTIPLIER_ENTRY_MAX
        if "we_multiplier_close" in new_config:
            self._we_multiplier_close_0 = float(self._config["we_multiplier_close"][0])
            self._we_multiplier_close_1 = float(self._config["we_multiplier_close"][1])
            if self._we_multiplier_close_0 < self.WE_MULTIPLIER_CLOSE_MIN:
                self._we_multiplier_close_0 = self.WE_MULTIPLIER_CLOSE_MIN
            if self._we_multiplier_close_1 > self.WE_MULTIPLIER_CLOSE_MAX:
                self._we_multiplier_close_1 = self.WE_MULTIPLIER_CLOSE_MAX
        if "delay_weight_entry" in new_config:
            self._delay_weight_entry_0 = float(self._config["delay_weight_entry"][0])
            self._delay_weight_entry_1 = float(self._config["delay_weight_entry"][1])
            if self._delay_weight_entry_0 < self.DELAY_WEIGHT_ENTRY_MIN:
                self._delay_weight_entry_0 = self.DELAY_WEIGHT_ENTRY_MIN
            if self._delay_weight_entry_1 > self.DELAY_WEIGHT_ENTRY_MAX:
                self._delay_weight_entry_1 = self.DELAY_WEIGHT_ENTRY_MAX
        if "delay_weight_close" in new_config:
            self._delay_weight_close_0 = float(self._config["delay_weight_close"][0])
            self._delay_weight_close_1 = float(self._config["delay_weight_close"][1])
            if self._delay_weight_close_0 < self.DELAY_WEIGHT_CLOSE_MIN:
                self._delay_weight_close_0 = self.DELAY_WEIGHT_CLOSE_MIN
            if self._delay_weight_close_1 > self.DELAY_WEIGHT_CLOSE_MAX:
                self._delay_weight_close_1 = self.DELAY_WEIGHT_CLOSE_MAX
        if "delay_between_fills_minutes_entry" in new_config:
            self._delay_between_fills_minutes_entry_0 = float(self._config["delay_between_fills_minutes_entry"][0])
            self._delay_between_fills_minutes_entry_1 = float(self._config["delay_between_fills_minutes_entry"][1])
            if self._delay_between_fills_minutes_entry_0 < self.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MIN:
                self._delay_between_fills_minutes_entry_0 = self.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MIN
            if self._delay_between_fills_minutes_entry_1 > self.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MAX:
                self._delay_between_fills_minutes_entry_1 = self.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MAX
        if "delay_between_fills_minutes_close" in new_config:
            self._delay_between_fills_minutes_close_0 = float(self._config["delay_between_fills_minutes_close"][0])
            self._delay_between_fills_minutes_close_1 = float(self._config["delay_between_fills_minutes_close"][1])
            if self._delay_between_fills_minutes_close_0 < self.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MIN:
                self._delay_between_fills_minutes_close_0 = self.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MIN
            if self._delay_between_fills_minutes_close_1 > self.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MAX:
                self._delay_between_fills_minutes_close_1 = self.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MAX
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
        if "wallet_exposure_limit" in new_config:
            self._wallet_exposure_limit_0 = float(self._config["wallet_exposure_limit"][0])
            self._wallet_exposure_limit_1 = float(self._config["wallet_exposure_limit"][1])
            if self._wallet_exposure_limit_0 < self.WALLET_EXPOSURE_LIMIT_MIN:
                self._wallet_exposure_limit_0 = self.WALLET_EXPOSURE_LIMIT_MIN
            if self._wallet_exposure_limit_1 > self.WALLET_EXPOSURE_LIMIT_MAX:
                self._wallet_exposure_limit_1 = self.WALLET_EXPOSURE_LIMIT_MAX

    @property
    def ema_span_0_0(self): return self._ema_span_0_0
    @property
    def ema_span_0_1(self): return self._ema_span_0_1
    @property
    def ema_span_1_0(self): return self._ema_span_1_0
    @property
    def ema_span_1_1(self): return self._ema_span_1_1
    @property
    def ema_dist_entry_0(self): return self._ema_dist_entry_0
    @property
    def ema_dist_entry_1(self): return self._ema_dist_entry_1
    @property
    def ema_dist_close_0(self): return self._ema_dist_close_0
    @property
    def ema_dist_close_1(self): return self._ema_dist_close_1
    @property
    def qty_pct_entry_0(self): return self._qty_pct_entry_0
    @property
    def qty_pct_entry_1(self): return self._qty_pct_entry_1
    @property
    def qty_pct_close_0(self): return self._qty_pct_close_0
    @property
    def qty_pct_close_1(self): return self._qty_pct_close_1
    @property
    def we_multiplier_entry_0(self): return self._we_multiplier_entry_0
    @property
    def we_multiplier_entry_1(self): return self._we_multiplier_entry_1
    @property
    def we_multiplier_close_0(self): return self._we_multiplier_close_0
    @property
    def we_multiplier_close_1(self): return self._we_multiplier_close_1
    @property
    def delay_weight_entry_0(self): return self._delay_weight_entry_0
    @property
    def delay_weight_entry_1(self): return self._delay_weight_entry_1
    @property
    def delay_weight_close_0(self): return self._delay_weight_close_0
    @property
    def delay_weight_close_1(self): return self._delay_weight_close_1
    @property
    def delay_between_fills_minutes_entry_0(self): return self._delay_between_fills_minutes_entry_0
    @property
    def delay_between_fills_minutes_entry_1(self): return self._delay_between_fills_minutes_entry_1
    @property
    def delay_between_fills_minutes_close_0(self): return self._delay_between_fills_minutes_close_0
    @property
    def delay_between_fills_minutes_close_1(self): return self._delay_between_fills_minutes_close_1
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
    def wallet_exposure_limit_0(self): return self._wallet_exposure_limit_0
    @property
    def wallet_exposure_limit_1(self): return self._wallet_exposure_limit_1

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
    @ema_dist_entry_0.setter
    def ema_dist_entry_0(self, new_ema_dist_entry_0):
        if self._ema_dist_entry_0 != new_ema_dist_entry_0:
            self._ema_dist_entry_0 = new_ema_dist_entry_0
            self._config["ema_dist_entry"][0] = self._ema_dist_entry_0
            st.experimental_rerun()
    @ema_dist_entry_1.setter
    def ema_dist_entry_1(self, new_ema_dist_entry_1):
        if self._ema_dist_entry_1 != new_ema_dist_entry_1:
            self._ema_dist_entry_1 = new_ema_dist_entry_1
            self._config["ema_dist_entry"][1] = self._ema_dist_entry_1
            st.experimental_rerun()
    @ema_dist_close_0.setter
    def ema_dist_close_0(self, new_ema_dist_close_0):
        if self._ema_dist_close_0 != new_ema_dist_close_0:
            self._ema_dist_close_0 = new_ema_dist_close_0
            self._config["ema_dist_close"][0] = self._ema_dist_close_0
            st.experimental_rerun()
    @ema_dist_close_1.setter
    def ema_dist_close_1(self, new_ema_dist_close_1):
        if self._ema_dist_close_1 != new_ema_dist_close_1:
            self._ema_dist_close_1 = new_ema_dist_close_1
            self._config["ema_dist_close"][1] = self._ema_dist_close_1
            st.experimental_rerun()
    @qty_pct_entry_0.setter
    def qty_pct_entry_0(self, new_qty_pct_entry_0):
        if self._qty_pct_entry_0 != new_qty_pct_entry_0:
            self._qty_pct_entry_0 = new_qty_pct_entry_0
            self._config["qty_pct_entry"][0] = self._qty_pct_entry_0
            st.experimental_rerun()
    @qty_pct_entry_1.setter
    def qty_pct_entry_1(self, new_qty_pct_entry_1):
        if self._qty_pct_entry_1 != new_qty_pct_entry_1:
            self._qty_pct_entry_1 = new_qty_pct_entry_1
            self._config["qty_pct_entry"][1] = self._qty_pct_entry_1
            st.experimental_rerun()
    @qty_pct_close_0.setter
    def qty_pct_close_0(self, new_qty_pct_close_0):
        if self._qty_pct_close_0 != new_qty_pct_close_0:
            self._qty_pct_close_0 = new_qty_pct_close_0
            self._config["qty_pct_close"][0] = self._qty_pct_close_0
            st.experimental_rerun()
    @qty_pct_close_1.setter
    def qty_pct_close_1(self, new_qty_pct_close_1):
        if self._qty_pct_close_1 != new_qty_pct_close_1:
            self._qty_pct_close_1 = new_qty_pct_close_1
            self._config["qty_pct_close"][1] = self._qty_pct_close_1
            st.experimental_rerun()
    @we_multiplier_entry_0.setter
    def we_multiplier_entry_0(self, new_we_multiplier_entry_0):
        if self._we_multiplier_entry_0 != new_we_multiplier_entry_0:
            self._we_multiplier_entry_0 = new_we_multiplier_entry_0
            self._config["we_multiplier_entry"][0] = self._we_multiplier_entry_0
            st.experimental_rerun()
    @we_multiplier_entry_1.setter
    def we_multiplier_entry_1(self, new_we_multiplier_entry_1):
        if self._we_multiplier_entry_1 != new_we_multiplier_entry_1:
            self._we_multiplier_entry_1 = new_we_multiplier_entry_1
            self._config["we_multiplier_entry"][1] = self._we_multiplier_entry_1
            st.experimental_rerun()
    @we_multiplier_close_0.setter
    def we_multiplier_close_0(self, new_we_multiplier_close_0):
        if self._we_multiplier_close_0 != new_we_multiplier_close_0:
            self._we_multiplier_close_0 = new_we_multiplier_close_0
            self._config["we_multiplier_close"][0] = self._we_multiplier_close_0
            st.experimental_rerun()
    @we_multiplier_close_1.setter
    def we_multiplier_close_1(self, new_we_multiplier_close_1):
        if self._we_multiplier_close_1 != new_we_multiplier_close_1:
            self._we_multiplier_close_1 = new_we_multiplier_close_1
            self._config["we_multiplier_close"][1] = self._we_multiplier_close_1
            st.experimental_rerun()
    @delay_weight_entry_0.setter
    def delay_weight_entry_0(self, new_delay_weight_entry_0):
        if self._delay_weight_entry_0 != new_delay_weight_entry_0:
            self._delay_weight_entry_0 = new_delay_weight_entry_0
            self._config["delay_weight_entry"][0] = self._delay_weight_entry_0
            st.experimental_rerun()
    @delay_weight_entry_1.setter
    def delay_weight_entry_1(self, new_delay_weight_entry_1):
        if self._delay_weight_entry_1 != new_delay_weight_entry_1:
            self._delay_weight_entry_1 = new_delay_weight_entry_1
            self._config["delay_weight_entry"][1] = self._delay_weight_entry_1
            st.experimental_rerun()
    @delay_weight_close_0.setter
    def delay_weight_close_0(self, new_delay_weight_close_0):
        if self._delay_weight_close_0 != new_delay_weight_close_0:
            self._delay_weight_close_0 = new_delay_weight_close_0
            self._config["delay_weight_close"][0] = self._delay_weight_close_0
            st.experimental_rerun()
    @delay_weight_close_1.setter
    def delay_weight_close_1(self, new_delay_weight_close_1):
        if self._delay_weight_close_1 != new_delay_weight_close_1:
            self._delay_weight_close_1 = new_delay_weight_close_1
            self._config["delay_weight_close"][1] = self._delay_weight_close_1
            st.experimental_rerun()
    @delay_between_fills_minutes_entry_0.setter
    def delay_between_fills_minutes_entry_0(self, new_delay_between_fills_minutes_entry_0):
        if self._delay_between_fills_minutes_entry_0 != new_delay_between_fills_minutes_entry_0:
            self._delay_between_fills_minutes_entry_0 = new_delay_between_fills_minutes_entry_0
            self._config["delay_between_fills_minutes_entry"][0] = self._delay_between_fills_minutes_entry_0
            st.experimental_rerun()
    @delay_between_fills_minutes_entry_1.setter
    def delay_between_fills_minutes_entry_1(self, new_delay_between_fills_minutes_entry_1):
        if self._delay_between_fills_minutes_entry_1 != new_delay_between_fills_minutes_entry_1:
            self._delay_between_fills_minutes_entry_1 = new_delay_between_fills_minutes_entry_1
            self._config["delay_between_fills_minutes_entry"][1] = self._delay_between_fills_minutes_entry_1
            st.experimental_rerun()
    @delay_between_fills_minutes_close_0.setter
    def delay_between_fills_minutes_close_0(self, new_delay_between_fills_minutes_close_0):
        if self._delay_between_fills_minutes_close_0 != new_delay_between_fills_minutes_close_0:
            self._delay_between_fills_minutes_close_0 = new_delay_between_fills_minutes_close_0
            self._config["delay_between_fills_minutes_close"][0] = self._delay_between_fills_minutes_close_0
            st.experimental_rerun()
    @delay_between_fills_minutes_close_1.setter
    def delay_between_fills_minutes_close_1(self, new_delay_between_fills_minutes_close_1):
        if self._delay_between_fills_minutes_close_1 != new_delay_between_fills_minutes_close_1:
            self._delay_between_fills_minutes_close_1 = new_delay_between_fills_minutes_close_1
            self._config["delay_between_fills_minutes_close"][1] = self._delay_between_fills_minutes_close_1
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
