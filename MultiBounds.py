import streamlit as st

class MultiBounds:
    GLOBAL_TWE_LONG_MIN = 0.0
    GLOBAL_TWE_LONG_MAX = 100.0
    GLOBAL_TWE_LONG_STEP = 0.01
    GLOBAL_TWE_LONG_ROUND = 2
    GLOBAL_TWE_LONG_FORMAT = f'%.{GLOBAL_TWE_LONG_ROUND}f'

    GLOBAL_TWE_SHORT_MIN = 0.0
    GLOBAL_TWE_SHORT_MAX = 100.0
    GLOBAL_TWE_SHORT_STEP = 0.01
    GLOBAL_TWE_SHORT_ROUND = 2
    GLOBAL_TWE_SHORT_FORMAT = f'%.{GLOBAL_TWE_SHORT_ROUND}f'

    GLOBAL_LOSS_ALLOWANCE_PCT_MIN = 0.0
    GLOBAL_LOSS_ALLOWANCE_PCT_MAX = 1.0
    GLOBAL_LOSS_ALLOWANCE_PCT_STEP = 0.005
    GLOBAL_LOSS_ALLOWANCE_PCT_ROUND = 3
    GLOBAL_LOSS_ALLOWANCE_PCT_FORMAT = f'%.{GLOBAL_LOSS_ALLOWANCE_PCT_ROUND}f'

    GLOBAL_STUCK_THRESHOLD_MIN = 0.0
    GLOBAL_STUCK_THRESHOLD_MAX = 1.0
    GLOBAL_STUCK_THRESHOLD_STEP = 0.005
    GLOBAL_STUCK_THRESHOLD_ROUND = 3
    GLOBAL_STUCK_THRESHOLD_FORMAT = f'%.{GLOBAL_STUCK_THRESHOLD_ROUND}f'

    GLOBAL_UNSTUCK_CLOSE_PCT_MIN = 0.0
    GLOBAL_UNSTUCK_CLOSE_PCT_MAX = 1.0
    GLOBAL_UNSTUCK_CLOSE_PCT_STEP = 0.005
    GLOBAL_UNSTUCK_CLOSE_PCT_ROUND = 3
    GLOBAL_UNSTUCK_CLOSE_PCT_FORMAT = f'%.{GLOBAL_UNSTUCK_CLOSE_PCT_ROUND}f'

    DDOWN_FACTOR_MIN = 0.0
    DDOWN_FACTOR_MAX = 1000.0
    DDOWN_FACTOR_STEP = 0.05
    DDOWN_FACTOR_ROUND = 2
    DDOWN_FACTOR_FORMAT = f'%.{DDOWN_FACTOR_ROUND}f'

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

    INITIAL_EPRICE_EMA_DIST_MIN = -10.0
    INITIAL_EPRICE_EMA_DIST_MAX = 10.0
    INITIAL_EPRICE_EMA_DIST_STEP = 0.001
    INITIAL_EPRICE_EMA_DIST_ROUND = 4
    INITIAL_EPRICE_EMA_DIST_FORMAT = f'%.{INITIAL_EPRICE_EMA_DIST_ROUND}f'

    INITIAL_QTY_PCT_MIN = 0.0
    INITIAL_QTY_PCT_MAX = 1.0
    INITIAL_QTY_PCT_STEP = 0.005
    INITIAL_QTY_PCT_ROUND = 4
    INITIAL_QTY_PCT_FORMAT = f'%.{INITIAL_QTY_PCT_ROUND}f'

    MARKUP_RANGE_MIN = 0.0
    MARKUP_RANGE_MAX = 10.0
    MARKUP_RANGE_STEP = 0.01
    MARKUP_RANGE_ROUND = 2
    MARKUP_RANGE_FORMAT = f'%.{MARKUP_RANGE_ROUND}f'

    MIN_MARKUP_MIN = 0.0
    MIN_MARKUP_MAX = 10.0
    MIN_MARKUP_STEP = 0.001
    MIN_MARKUP_ROUND = 4
    MIN_MARKUP_FORMAT = f'%.{MIN_MARKUP_ROUND}f'

    N_CLOSE_ORDERS_MIN = 1
    N_CLOSE_ORDERS_MAX = 100
    N_CLOSE_ORDERS_STEP = 1
    N_CLOSE_ORDERS_FORMAT = '%d'

    RENTRY_PPRICE_DIST_MIN = 0.0
    RENTRY_PPRICE_DIST_MAX = 100.0
    RENTRY_PPRICE_DIST_STEP = 0.005
    RENTRY_PPRICE_DIST_ROUND = 4
    RENTRY_PPRICE_DIST_FORMAT = f'%.{RENTRY_PPRICE_DIST_ROUND}f'

    RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN = 0.0
    RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX = 1000000.0
    RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP = 0.1
    RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND = 1
    RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT = f'%.{RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND}f'

    def __init__(self):
        self._global_TWE_long_0 = 2.0
        self._global_TWE_long_1 = 2.0
        self._global_TWE_short_0 = 0.0
        self._global_TWE_short_1 = 10.0
        self._global_loss_allowance_pct_0 = 0.01
        self._global_loss_allowance_pct_1 = 0.1
        self._global_stuck_threshold_0 = 0.7
        self._global_stuck_threshold_1 = 0.95
        self._global_unstuck_close_pct_0 = 0.005
        self._global_unstuck_close_pct_1 = 0.05

        self._long_ddown_factor_0 = 0.1
        self._long_ddown_factor_1 = 2.0
        self._long_ema_span_0_0 = 200.0
        self._long_ema_span_0_1 = 2000.0
        self._long_ema_span_1_0 = 200.0
        self._long_ema_span_1_1 = 2000.0
        self._long_initial_eprice_ema_dist_0 = -0.1
        self._long_initial_eprice_ema_dist_1 = 0.02
        self._long_initial_qty_pct_0 = 0.025
        self._long_initial_qty_pct_1 = 0.1
        self._long_markup_range_0 = 0.01
        self._long_markup_range_1 = 0.15
        self._long_min_markup_0 = 0.002
        self._long_min_markup_1 = 0.05
        self._long_n_close_orders_0 = 2
        self._long_n_close_orders_1 = 5
        self._long_rentry_pprice_dist_0 = 0.01
        self._long_rentry_pprice_dist_1 = 0.1
        self._long_rentry_pprice_dist_wallet_exposure_weighting_0 = 0.2
        self._long_rentry_pprice_dist_wallet_exposure_weighting_1 = 5.0

        self._short_ddown_factor_0 = 0.1
        self._short_ddown_factor_1 = 3.0
        self._short_ema_span_0_0 = 5.0
        self._short_ema_span_0_1 = 1440.0
        self._short_ema_span_1_0 = 5.0
        self._short_ema_span_1_1 = 1440.0
        self._short_initial_eprice_ema_dist_0 = -0.1
        self._short_initial_eprice_ema_dist_1 = 0.003
        self._short_initial_qty_pct_0 = 0.01
        self._short_initial_qty_pct_1 = 0.05
        self._short_markup_range_0 = 0.0
        self._short_markup_range_1 = 0.06
        self._short_min_markup_0 = 0.001
        self._short_min_markup_1 = 0.01
        self._short_n_close_orders_0 = 2
        self._short_n_close_orders_1 = 16
        self._short_rentry_pprice_dist_0 = 0.005
        self._short_rentry_pprice_dist_1 = 0.05
        self._short_rentry_pprice_dist_wallet_exposure_weighting_0 = 0.0
        self._short_rentry_pprice_dist_wallet_exposure_weighting_1 = 20.0

        self._config =   {
                    "global_TWE_long": [self._global_TWE_long_0, self._global_TWE_long_1],
                    "global_TWE_short": [self._global_TWE_short_0, self._global_TWE_short_1],
                    "global_loss_allowance_pct": [self._global_loss_allowance_pct_0, self._global_loss_allowance_pct_1],
                    "global_stuck_threshold": [self._global_stuck_threshold_0, self._global_stuck_threshold_1],
                    "global_unstuck_close_pct": [self._global_unstuck_close_pct_0, self._global_unstuck_close_pct_1],

                    "long_ddown_factor": [self._long_ddown_factor_0, self._long_ddown_factor_1],
                    "long_ema_span_0": [self._long_ema_span_0_0, self._long_ema_span_0_1],
                    "long_ema_span_1": [self._long_ema_span_1_0, self._long_ema_span_1_1],
                    "long_initial_eprice_ema_dist": [self._long_initial_eprice_ema_dist_0, self._long_initial_eprice_ema_dist_1],
                    "long_initial_qty_pct": [self._long_initial_qty_pct_0, self._long_initial_qty_pct_1],
                    "long_markup_range": [self._long_markup_range_0, self._long_markup_range_1],
                    "long_min_markup": [self._long_min_markup_0, self._long_min_markup_1],
                    "long_n_close_orders": [self._long_n_close_orders_0, self._long_n_close_orders_1],
                    "long_rentry_pprice_dist": [self._long_rentry_pprice_dist_0, self._long_rentry_pprice_dist_1],
                    "long_rentry_pprice_dist_wallet_exposure_weighting": [self._long_rentry_pprice_dist_wallet_exposure_weighting_0, self._long_rentry_pprice_dist_wallet_exposure_weighting_1],

                    "short_ddown_factor": [self._short_ddown_factor_0, self._short_ddown_factor_1],
                    "short_ema_span_0": [self._short_ema_span_0_0, self._short_ema_span_0_1],
                    "short_ema_span_1": [self._short_ema_span_1_0, self._short_ema_span_1_1],
                    "short_initial_eprice_ema_dist": [self._short_initial_eprice_ema_dist_0, self._short_initial_eprice_ema_dist_1],
                    "short_initial_qty_pct": [self._short_initial_qty_pct_0, self._short_initial_qty_pct_1],
                    "short_markup_range": [self._short_markup_range_0, self._short_markup_range_1],
                    "short_min_markup": [self._short_min_markup_0, self._short_min_markup_1],
                    "short_n_close_orders": [self._short_n_close_orders_0, self._short_n_close_orders_1],
                    "short_rentry_pprice_dist": [self._short_rentry_pprice_dist_0, self._short_rentry_pprice_dist_1],
                    "short_rentry_pprice_dist_wallet_exposure_weighting": [self._short_rentry_pprice_dist_wallet_exposure_weighting_0, self._short_rentry_pprice_dist_wallet_exposure_weighting_1]
                }

    @property
    def config(self): return self._config
    @config.setter
    def config(self, new_config):
        self._config = new_config
        if "global_TWE_long" in new_config:
            self._global_TWE_long_0 = float(self._config["global_TWE_long"][0])
            self._global_TWE_long_1 = float(self._config["global_TWE_long"][1])
            if self._global_TWE_long_0 < self.GLOBAL_TWE_LONG_MIN:
                self._global_TWE_long_0 = self.GLOBAL_TWE_LONG_MIN
            if self._global_TWE_long_1 > self.GLOBAL_TWE_LONG_MAX:
                self._global_TWE_long_1 = self.GLOBAL_TWE_LONG_MAX
        if "global_TWE_short" in new_config:
            self._global_TWE_short_0 = float(self._config["global_TWE_short"][0])
            self._global_TWE_short_1 = float(self._config["global_TWE_short"][1])
            if self._global_TWE_short_0 < self.GLOBAL_TWE_SHORT_MIN:
                self._global_TWE_short_0 = self.GLOBAL_TWE_SHORT_MIN
            if self._global_TWE_short_1 > self.GLOBAL_TWE_SHORT_MAX:
                self._global_TWE_short_1 = self.GLOBAL_TWE_SHORT_MAX
        if "global_loss_allowance_pct" in new_config:
            self._global_loss_allowance_pct_0 = float(self._config["global_loss_allowance_pct"][0])
            self._global_loss_allowance_pct_1 = float(self._config["global_loss_allowance_pct"][1])
            if self._global_loss_allowance_pct_0 < self.GLOBAL_LOSS_ALLOWANCE_PCT_MIN:
                self._global_loss_allowance_pct_0 = self.GLOBAL_LOSS_ALLOWANCE_PCT_MIN
            if self._global_loss_allowance_pct_1 > self.GLOBAL_LOSS_ALLOWANCE_PCT_MAX:
                self._global_loss_allowance_pct_1 = self.GLOBAL_LOSS_ALLOWANCE_PCT_MAX
        if "global_stuck_threshold" in new_config:
            self._global_stuck_threshold_0 = float(self._config["global_stuck_threshold"][0])
            self._global_stuck_threshold_1 = float(self._config["global_stuck_threshold"][1])
            if self._global_stuck_threshold_0 < self.GLOBAL_STUCK_THRESHOLD_MIN:
                self._global_stuck_threshold_0 = self.GLOBAL_STUCK_THRESHOLD_MIN
            if self._global_stuck_threshold_1 > self.GLOBAL_STUCK_THRESHOLD_MAX:
                self._global_stuck_threshold_1 = self.GLOBAL_STUCK_THRESHOLD_MAX
        if "global_unstuck_close_pct" in new_config:
            self._global_unstuck_close_pct_0 = float(self._config["global_unstuck_close_pct"][0])
            self._global_unstuck_close_pct_1 = float(self._config["global_unstuck_close_pct"][1])
            if self._global_unstuck_close_pct_0 < self.GLOBAL_UNSTUCK_CLOSE_PCT_MIN:
                self._global_unstuck_close_pct_0 = self.GLOBAL_UNSTUCK_CLOSE_PCT_MIN
            if self._global_unstuck_close_pct_1 > self.GLOBAL_UNSTUCK_CLOSE_PCT_MAX:
                self._global_unstuck_close_pct_1 = self.GLOBAL_UNSTUCK_CLOSE_PCT_MAX

        if "long_ddown_factor" in new_config:
            self._long_ddown_factor_0 = float(self._config["long_ddown_factor"][0])
            self._long_ddown_factor_1 = float(self._config["long_ddown_factor"][1])
            if self._long_ddown_factor_0 < self.DDOWN_FACTOR_MIN:
                self._long_ddown_factor_0 = self.DDOWN_FACTOR_MIN
            if self._long_ddown_factor_1 > self.DDOWN_FACTOR_MAX:
                self._long_ddown_factor_1 = self.DDOWN_FACTOR_MAX
        if "long_ema_span_0" in new_config:
            self._long_ema_span_0_0 = float(self._config["long_ema_span_0"][0])
            self._long_ema_span_0_1 = float(self._config["long_ema_span_0"][1])
            if self._long_ema_span_0_0 < self.EMA_SPAN_0_MIN:
                self._long_ema_span_0_0 = self.EMA_SPAN_0_MIN
            if self._long_ema_span_0_1 > self.EMA_SPAN_0_MAX:
                self._long_ema_span_0_1 = self.EMA_SPAN_0_MAX
        if "long_ema_span_1" in new_config:
            self._long_ema_span_1_0 = float(self._config["long_ema_span_1"][0])
            self._long_ema_span_1_1 = float(self._config["long_ema_span_1"][1])
            if self._long_ema_span_1_0 < self.EMA_SPAN_1_MIN:
                self._long_ema_span_1_0 = self.EMA_SPAN_1_MIN
            if self._long_ema_span_1_1 > self.EMA_SPAN_1_MAX:
                self._long_ema_span_1_1 = self.EMA_SPAN_1_MAX
        if "long_initial_eprice_ema_dist" in new_config:
            self._long_initial_eprice_ema_dist_0 = float(self._config["long_initial_eprice_ema_dist"][0])
            self._long_initial_eprice_ema_dist_1 = float(self._config["long_initial_eprice_ema_dist"][1])
            if self._long_initial_eprice_ema_dist_0 < self.INITIAL_EPRICE_EMA_DIST_MIN:
                self._long_initial_eprice_ema_dist_0 = self.INITIAL_EPRICE_EMA_DIST_MIN
            if self._long_initial_eprice_ema_dist_1 > self.INITIAL_EPRICE_EMA_DIST_MAX:
                self._long_initial_eprice_ema_dist_1 = self.INITIAL_EPRICE_EMA_DIST_MAX
        if "long_initial_qty_pct" in new_config:
            self._long_initial_qty_pct_0 = float(self._config["long_initial_qty_pct"][0])
            self._long_initial_qty_pct_1 = float(self._config["long_initial_qty_pct"][1])
            if self._long_initial_qty_pct_0 < self.INITIAL_QTY_PCT_MIN:
                self._long_initial_qty_pct_0 = self.INITIAL_QTY_PCT_MIN
            if self._long_initial_qty_pct_1 > self.INITIAL_QTY_PCT_MAX:
                self._long_initial_qty_pct_1 = self.INITIAL_QTY_PCT_MAX
        if "long_markup_range" in new_config:
            self._long_markup_range_0 = float(self._config["long_markup_range"][0])
            self._long_markup_range_1 = float(self._config["long_markup_range"][1])
            if self._long_markup_range_0 < self.MARKUP_RANGE_MIN:
                self._long_markup_range_0 = self.MARKUP_RANGE_MIN
            if self._long_markup_range_1 > self.MARKUP_RANGE_MAX:
                self._long_markup_range_1 = self.MARKUP_RANGE_MAX
        if "long_min_markup" in new_config:
            self._long_min_markup_0 = float(self._config["long_min_markup"][0])
            self._long_min_markup_1 = float(self._config["long_min_markup"][1])
            if self._long_min_markup_0 < self.MIN_MARKUP_MIN:
                self._long_min_markup_0 = self.MIN_MARKUP_MIN
            if self._long_min_markup_1 > self.MIN_MARKUP_MAX:
                self._long_min_markup_1 = self.MIN_MARKUP_MAX
        if "long_n_close_orders" in new_config:
            self._long_n_close_orders_0 = int(self._config["long_n_close_orders"][0])
            self._long_n_close_orders_1 = int(self._config["long_n_close_orders"][1])
            if self._long_n_close_orders_0 < self.N_CLOSE_ORDERS_MIN:
                self._long_n_close_orders_0 = self.N_CLOSE_ORDERS_MIN
            if self._long_n_close_orders_1 > self.N_CLOSE_ORDERS_MAX:
                self._long_n_close_orders_1 = self.N_CLOSE_ORDERS_MAX
        if "long_rentry_pprice_dist" in new_config:
            self._long_rentry_pprice_dist_0 = float(self._config["long_rentry_pprice_dist"][0])
            self._long_rentry_pprice_dist_1 = float(self._config["long_rentry_pprice_dist"][1])
            if self._long_rentry_pprice_dist_0 < self.RENTRY_PPRICE_DIST_MIN:
                self._long_rentry_pprice_dist_0 = self.RENTRY_PPRICE_DIST_MIN
            if self._long_rentry_pprice_dist_1 > self.RENTRY_PPRICE_DIST_MAX:
                self._long_rentry_pprice_dist_1 = self.RENTRY_PPRICE_DIST_MAX
        if "long_rentry_pprice_dist_wallet_exposure_weighting" in new_config:
            self._long_rentry_pprice_dist_wallet_exposure_weighting_0 = float(self._config["long_rentry_pprice_dist_wallet_exposure_weighting"][0])
            self._long_rentry_pprice_dist_wallet_exposure_weighting_1 = float(self._config["long_rentry_pprice_dist_wallet_exposure_weighting"][1])
            if self._long_rentry_pprice_dist_wallet_exposure_weighting_0 < self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN:
                self._long_rentry_pprice_dist_wallet_exposure_weighting_0 = self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN
            if self._long_rentry_pprice_dist_wallet_exposure_weighting_1 > self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX:
                self._long_rentry_pprice_dist_wallet_exposure_weighting_1 = self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX

        if "short_ddown_factor" in new_config:
            self._short_ddown_factor_0 = float(self._config["short_ddown_factor"][0])
            self._short_ddown_factor_1 = float(self._config["short_ddown_factor"][1])
            if self._short_ddown_factor_0 < self.DDOWN_FACTOR_MIN:
                self._short_ddown_factor_0 = self.DDOWN_FACTOR_MIN
            if self._short_ddown_factor_1 > self.DDOWN_FACTOR_MAX:
                self._short_ddown_factor_1 = self.DDOWN_FACTOR_MAX
        if "short_ema_span_0" in new_config:
            self._short_ema_span_0_0 = float(self._config["short_ema_span_0"][0])
            self._short_ema_span_0_1 = float(self._config["short_ema_span_0"][1])
            if self._short_ema_span_0_0 < self.EMA_SPAN_0_MIN:
                self._short_ema_span_0_0 = self.EMA_SPAN_0_MIN
            if self._short_ema_span_0_1 > self.EMA_SPAN_0_MAX:
                self._short_ema_span_0_1 = self.EMA_SPAN_0_MAX
        if "short_ema_span_1" in new_config:
            self._short_ema_span_1_0 = float(self._config["short_ema_span_1"][0])
            self._short_ema_span_1_1 = float(self._config["short_ema_span_1"][1])
            if self._short_ema_span_1_0 < self.EMA_SPAN_1_MIN:
                self._short_ema_span_1_0 = self.EMA_SPAN_1_MIN
            if self._short_ema_span_1_1 > self.EMA_SPAN_1_MAX:
                self._short_ema_span_1_1 = self.EMA_SPAN_1_MAX
        if "short_initial_eprice_ema_dist" in new_config:
            self._short_initial_eprice_ema_dist_0 = float(self._config["short_initial_eprice_ema_dist"][0])
            self._short_initial_eprice_ema_dist_1 = float(self._config["short_initial_eprice_ema_dist"][1])
            if self._short_initial_eprice_ema_dist_0 < self.INITIAL_EPRICE_EMA_DIST_MIN:
                self._short_initial_eprice_ema_dist_0 = self.INITIAL_EPRICE_EMA_DIST_MIN
            if self._short_initial_eprice_ema_dist_1 > self.INITIAL_EPRICE_EMA_DIST_MAX:
                self._short_initial_eprice_ema_dist_1 = self.INITIAL_EPRICE_EMA_DIST_MAX
        if "short_initial_qty_pct" in new_config:
            self._short_initial_qty_pct_0 = float(self._config["short_initial_qty_pct"][0])
            self._short_initial_qty_pct_1 = float(self._config["short_initial_qty_pct"][1])
            if self._short_initial_qty_pct_0 < self.INITIAL_QTY_PCT_MIN:
                self._short_initial_qty_pct_0 = self.INITIAL_QTY_PCT_MIN
            if self._short_initial_qty_pct_1 > self.INITIAL_QTY_PCT_MAX:
                self._short_initial_qty_pct_1 = self.INITIAL_QTY_PCT_MAX
        if "short_markup_range" in new_config:
            self._short_markup_range_0 = float(self._config["short_markup_range"][0])
            self._short_markup_range_1 = float(self._config["short_markup_range"][1])
            if self._short_markup_range_0 < self.MARKUP_RANGE_MIN:
                self._short_markup_range_0 = self.MARKUP_RANGE_MIN
            if self._short_markup_range_1 > self.MARKUP_RANGE_MAX:
                self._short_markup_range_1 = self.MARKUP_RANGE_MAX
        if "short_min_markup" in new_config:
            self._short_min_markup_0 = float(self._config["short_min_markup"][0])
            self._short_min_markup_1 = float(self._config["short_min_markup"][1])
            if self._short_min_markup_0 < self.MIN_MARKUP_MIN:
                self._short_min_markup_0 = self.MIN_MARKUP_MIN
            if self._short_min_markup_1 > self.MIN_MARKUP_MAX:
                self._short_min_markup_1 = self.MIN_MARKUP_MAX
        if "short_n_close_orders" in new_config:
            self._short_n_close_orders_0 = int(self._config["short_n_close_orders"][0])
            self._short_n_close_orders_1 = int(self._config["short_n_close_orders"][1])
            if self._short_n_close_orders_0 < self.N_CLOSE_ORDERS_MIN:
                self._short_n_close_orders_0 = self.N_CLOSE_ORDERS_MIN
            if self._short_n_close_orders_1 > self.N_CLOSE_ORDERS_MAX:
                self._short_n_close_orders_1 = self.N_CLOSE_ORDERS_MAX
        if "short_rentry_pprice_dist" in new_config:
            self._short_rentry_pprice_dist_0 = float(self._config["short_rentry_pprice_dist"][0])
            self._short_rentry_pprice_dist_1 = float(self._config["short_rentry_pprice_dist"][1])
            if self._short_rentry_pprice_dist_0 < self.RENTRY_PPRICE_DIST_MIN:
                self._short_rentry_pprice_dist_0 = self.RENTRY_PPRICE_DIST_MIN
            if self._short_rentry_pprice_dist_1 > self.RENTRY_PPRICE_DIST_MAX:
                self._short_rentry_pprice_dist_1 = self.RENTRY_PPRICE_DIST_MAX
        if "short_rentry_pprice_dist_wallet_exposure_weighting" in new_config:
            self._short_rentry_pprice_dist_wallet_exposure_weighting_0 = float(self._config["short_rentry_pprice_dist_wallet_exposure_weighting"][0])
            self._short_rentry_pprice_dist_wallet_exposure_weighting_1 = float(self._config["short_rentry_pprice_dist_wallet_exposure_weighting"][1])
            if self._short_rentry_pprice_dist_wallet_exposure_weighting_0 < self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN:
                self._short_rentry_pprice_dist_wallet_exposure_weighting_0 = self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN
            if self._short_rentry_pprice_dist_wallet_exposure_weighting_1 > self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX:
                self._short_rentry_pprice_dist_wallet_exposure_weighting_1 = self.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX

    @property
    def global_TWE_long_0(self): return self._global_TWE_long_0
    @property
    def global_TWE_long_1(self): return self._global_TWE_long_1
    @property
    def global_TWE_short_0(self): return self._global_TWE_short_0
    @property
    def global_TWE_short_1(self): return self._global_TWE_short_1
    @property
    def global_loss_allowance_pct_0(self): return self._global_loss_allowance_pct_0
    @property
    def global_loss_allowance_pct_1(self): return self._global_loss_allowance_pct_1
    @property
    def global_stuck_threshold_0(self): return self._global_stuck_threshold_0
    @property
    def global_stuck_threshold_1(self): return self._global_stuck_threshold_1
    @property
    def global_unstuck_close_pct_0(self): return self._global_unstuck_close_pct_0
    @property
    def global_unstuck_close_pct_1(self): return self._global_unstuck_close_pct_1

    @property
    def long_ddown_factor_0(self): return self._long_ddown_factor_0
    @property
    def long_ddown_factor_1(self): return self._long_ddown_factor_1
    @property
    def long_ema_span_0_0(self): return self._long_ema_span_0_0
    @property
    def long_ema_span_0_1(self): return self._long_ema_span_0_1
    @property
    def long_ema_span_1_0(self): return self._long_ema_span_1_0
    @property
    def long_ema_span_1_1(self): return self._long_ema_span_1_1
    @property
    def long_initial_eprice_ema_dist_0(self): return self._long_initial_eprice_ema_dist_0
    @property
    def long_initial_eprice_ema_dist_1(self): return self._long_initial_eprice_ema_dist_1
    @property
    def long_initial_qty_pct_0(self): return self._long_initial_qty_pct_0
    @property
    def long_initial_qty_pct_1(self): return self._long_initial_qty_pct_1
    @property
    def long_markup_range_0(self): return self._long_markup_range_0
    @property
    def long_markup_range_1(self): return self._long_markup_range_1
    @property
    def long_min_markup_0(self): return self._long_min_markup_0
    @property
    def long_min_markup_1(self): return self._long_min_markup_1
    @property
    def long_n_close_orders_0(self): return self._long_n_close_orders_0
    @property
    def long_n_close_orders_1(self): return self._long_n_close_orders_1
    @property
    def long_rentry_pprice_dist_0(self): return self._long_rentry_pprice_dist_0
    @property
    def long_rentry_pprice_dist_1(self): return self._long_rentry_pprice_dist_1
    @property
    def long_rentry_pprice_dist_wallet_exposure_weighting_0(self): return self._long_rentry_pprice_dist_wallet_exposure_weighting_0
    @property
    def long_rentry_pprice_dist_wallet_exposure_weighting_1(self): return self._long_rentry_pprice_dist_wallet_exposure_weighting_1

    @property
    def short_ddown_factor_0(self): return self._short_ddown_factor_0
    @property
    def short_ddown_factor_1(self): return self._short_ddown_factor_1
    @property
    def short_ema_span_0_0(self): return self._short_ema_span_0_0
    @property
    def short_ema_span_0_1(self): return self._short_ema_span_0_1
    @property
    def short_ema_span_1_0(self): return self._short_ema_span_1_0
    @property
    def short_ema_span_1_1(self): return self._short_ema_span_1_1
    @property
    def short_initial_eprice_ema_dist_0(self): return self._short_initial_eprice_ema_dist_0
    @property
    def short_initial_eprice_ema_dist_1(self): return self._short_initial_eprice_ema_dist_1
    @property
    def short_initial_qty_pct_0(self): return self._short_initial_qty_pct_0
    @property
    def short_initial_qty_pct_1(self): return self._short_initial_qty_pct_1
    @property
    def short_markup_range_0(self): return self._short_markup_range_0
    @property
    def short_markup_range_1(self): return self._short_markup_range_1
    @property
    def short_min_markup_0(self): return self._short_min_markup_0
    @property
    def short_min_markup_1(self): return self._short_min_markup_1
    @property
    def short_n_close_orders_0(self): return self._short_n_close_orders_0
    @property
    def short_n_close_orders_1(self): return self._short_n_close_orders_1
    @property
    def short_rentry_pprice_dist_0(self): return self._short_rentry_pprice_dist_0
    @property
    def short_rentry_pprice_dist_1(self): return self._short_rentry_pprice_dist_1
    @property
    def short_rentry_pprice_dist_wallet_exposure_weighting_0(self): return self._short_rentry_pprice_dist_wallet_exposure_weighting_0
    @property
    def short_rentry_pprice_dist_wallet_exposure_weighting_1(self): return self._short_rentry_pprice_dist_wallet_exposure_weighting_1

    @global_TWE_long_0.setter
    def global_TWE_long_0(self, new_global_TWE_long_0):
        self._global_TWE_long_0 = new_global_TWE_long_0
        self._config["global_TWE_long"][0] = self._global_TWE_long_0
    @global_TWE_long_1.setter
    def global_TWE_long_1(self, new_global_TWE_long_1):
        self._global_TWE_long_1 = new_global_TWE_long_1
        self._config["global_TWE_long"][1] = self._global_TWE_long_1

    @global_TWE_short_0.setter
    def global_TWE_short_0(self, new_global_TWE_short_0):
        self._global_TWE_short_0 = new_global_TWE_short_0
        self._config["global_TWE_short"][0] = self._global_TWE_short_0
    @global_TWE_short_1.setter
    def global_TWE_short_1(self, new_global_TWE_short_1):
        self._global_TWE_short_1 = new_global_TWE_short_1
        self._config["global_TWE_short"][1] = self._global_TWE_short_1

    @global_loss_allowance_pct_0.setter
    def global_loss_allowance_pct_0(self, new_global_loss_allowance_pct_0):
        self._global_loss_allowance_pct_0 = new_global_loss_allowance_pct_0
        self._config["global_loss_allowance_pct"][0] = self._global_loss_allowance_pct_0
    @global_loss_allowance_pct_1.setter
    def global_loss_allowance_pct_1(self, new_global_loss_allowance_pct_1):
        self._global_loss_allowance_pct_1 = new_global_loss_allowance_pct_1
        self._config["global_loss_allowance_pct"][1] = self._global_loss_allowance_pct_1

    @global_stuck_threshold_0.setter
    def global_stuck_threshold_0(self, new_global_stuck_threshold_0):
        self._global_stuck_threshold_0 = new_global_stuck_threshold_0
        self._config["global_stuck_threshold"][0] = self._global_stuck_threshold_0
    @global_stuck_threshold_1.setter
    def global_stuck_threshold_1(self, new_global_stuck_threshold_1):
        self._global_stuck_threshold_1 = new_global_stuck_threshold_1
        self._config["global_stuck_threshold"][1] = self._global_stuck_threshold_1

    @global_unstuck_close_pct_0.setter
    def global_unstuck_close_pct_0(self, new_global_unstuck_close_pct_0):
        self._global_unstuck_close_pct_0 = new_global_unstuck_close_pct_0
        self._config["global_unstuck_close_pct"][0] = self._global_unstuck_close_pct_0
    @global_unstuck_close_pct_1.setter
    def global_unstuck_close_pct_1(self, new_global_unstuck_close_pct_1):
        self._global_unstuck_close_pct_1 = new_global_unstuck_close_pct_1
        self._config["global_unstuck_close_pct"][1] = self._global_unstuck_close_pct_1

    @long_ddown_factor_0.setter
    def long_ddown_factor_0(self, new_long_ddown_factor_0):
        self._long_ddown_factor_0 = new_long_ddown_factor_0
        self._config["long_ddown_factor"][0] = self._long_ddown_factor_0
    @long_ddown_factor_1.setter
    def long_ddown_factor_1(self, new_long_ddown_factor_1):
        self._long_ddown_factor_1 = new_long_ddown_factor_1
        self._config["long_ddown_factor"][1] = self._long_ddown_factor_1
    @long_ema_span_0_0.setter
    def long_ema_span_0_0(self, new_long_ema_span_0_0):
        self._long_ema_span_0_0 = new_long_ema_span_0_0
        self._config["long_ema_span_0"][0] = self._long_ema_span_0_0
    @long_ema_span_0_1.setter
    def long_ema_span_0_1(self, new_long_ema_span_0_1):
        self._long_ema_span_0_1 = new_long_ema_span_0_1
        self._config["long_ema_span_0"][1] = self._long_ema_span_0_1
    @long_ema_span_1_0.setter
    def long_ema_span_1_0(self, new_long_ema_span_1_0):
        self._long_ema_span_1_0 = new_long_ema_span_1_0
        self._config["long_ema_span_1"][0] = self._long_ema_span_1_0
    @long_ema_span_1_1.setter
    def long_ema_span_1_1(self, new_long_ema_span_1_1):
        self._long_ema_span_1_1 = new_long_ema_span_1_1
        self._config["long_ema_span_1"][1] = self._long_ema_span_1_1
    @long_initial_eprice_ema_dist_0.setter
    def long_initial_eprice_ema_dist_0(self, new_long_initial_eprice_ema_dist_0):
        self._long_initial_eprice_ema_dist_0 = new_long_initial_eprice_ema_dist_0
        self._config["long_initial_eprice_ema_dist"][0] = self._long_initial_eprice_ema_dist_0
    @long_initial_eprice_ema_dist_1.setter
    def long_initial_eprice_ema_dist_1(self, new_long_initial_eprice_ema_dist_1):
        self._long_initial_eprice_ema_dist_1 = new_long_initial_eprice_ema_dist_1
        self._config["long_initial_eprice_ema_dist"][1] = self._long_initial_eprice_ema_dist_1
    @long_initial_qty_pct_0.setter
    def long_initial_qty_pct_0(self, new_long_initial_qty_pct_0):
        self._long_initial_qty_pct_0 = new_long_initial_qty_pct_0
        self._config["long_initial_qty_pct"][0] = self._long_initial_qty_pct_0
    @long_initial_qty_pct_1.setter
    def long_initial_qty_pct_1(self, new_long_initial_qty_pct_1):
        self._long_initial_qty_pct_1 = new_long_initial_qty_pct_1
        self._config["long_initial_qty_pct"][1] = self._long_initial_qty_pct_1
    @long_markup_range_0.setter
    def long_markup_range_0(self, new_long_markup_range_0):
        self._long_markup_range_0 = new_long_markup_range_0
        self._config["long_markup_range"][0] = self._long_markup_range_0
    @long_markup_range_1.setter
    def long_markup_range_1(self, new_long_markup_range_1):
        self._long_markup_range_1 = new_long_markup_range_1
        self._config["long_markup_range"][1] = self._long_markup_range_1
    @long_min_markup_0.setter
    def long_min_markup_0(self, new_long_min_markup_0):
        self._long_min_markup_0 = new_long_min_markup_0
        self._config["long_min_markup"][0] = self._long_min_markup_0
    @long_min_markup_1.setter
    def long_min_markup_1(self, new_long_min_markup_1):
        self._long_min_markup_1 = new_long_min_markup_1
        self._config["long_min_markup"][1] = self._long_min_markup_1
    @long_n_close_orders_0.setter
    def long_n_close_orders_0(self, new_long_n_close_orders_0):
        self._long_n_close_orders_0 = new_long_n_close_orders_0
        self._config["long_n_close_orders"][0] = self._long_n_close_orders_0
    @long_n_close_orders_1.setter
    def long_n_close_orders_1(self, new_long_n_close_orders_1):
        self._long_n_close_orders_1 = new_long_n_close_orders_1
        self._config["long_n_close_orders"][1] = self._long_n_close_orders_1
    @long_rentry_pprice_dist_0.setter
    def long_rentry_pprice_dist_0(self, new_long_rentry_pprice_dist_0):
        self._long_rentry_pprice_dist_0 = new_long_rentry_pprice_dist_0
        self._config["long_rentry_pprice_dist"][0] = self._long_rentry_pprice_dist_0
    @long_rentry_pprice_dist_1.setter
    def long_rentry_pprice_dist_1(self, new_long_rentry_pprice_dist_1):
        self._long_rentry_pprice_dist_1 = new_long_rentry_pprice_dist_1
        self._config["long_rentry_pprice_dist"][1] = self._long_rentry_pprice_dist_1
    @long_rentry_pprice_dist_wallet_exposure_weighting_0.setter
    def long_rentry_pprice_dist_wallet_exposure_weighting_0(self, new_long_rentry_pprice_dist_wallet_exposure_weighting_0):
        self._long_rentry_pprice_dist_wallet_exposure_weighting_0 = new_long_rentry_pprice_dist_wallet_exposure_weighting_0
        self._config["long_rentry_pprice_dist_wallet_exposure_weighting"][0] = self._long_rentry_pprice_dist_wallet_exposure_weighting_0
    @long_rentry_pprice_dist_wallet_exposure_weighting_1.setter
    def long_rentry_pprice_dist_wallet_exposure_weighting_1(self, new_long_rentry_pprice_dist_wallet_exposure_weighting_1):
        self._long_rentry_pprice_dist_wallet_exposure_weighting_1 = new_long_rentry_pprice_dist_wallet_exposure_weighting_1
        self._config["long_rentry_pprice_dist_wallet_exposure_weighting"][1] = self._long_rentry_pprice_dist_wallet_exposure_weighting_1

    @short_ddown_factor_0.setter
    def short_ddown_factor_0(self, new_short_ddown_factor_0):
        self._short_ddown_factor_0 = new_short_ddown_factor_0
        self._config["short_ddown_factor"][0] = self._short_ddown_factor_0
    @short_ddown_factor_1.setter
    def short_ddown_factor_1(self, new_short_ddown_factor_1):
        self._short_ddown_factor_1 = new_short_ddown_factor_1
        self._config["short_ddown_factor"][1] = self._short_ddown_factor_1
    @short_ema_span_0_0.setter
    def short_ema_span_0_0(self, new_short_ema_span_0_0):
        self._short_ema_span_0_0 = new_short_ema_span_0_0
        self._config["short_ema_span_0"][0] = self._short_ema_span_0_0
    @short_ema_span_0_1.setter
    def short_ema_span_0_1(self, new_short_ema_span_0_1):
        self._short_ema_span_0_1 = new_short_ema_span_0_1
        self._config["short_ema_span_0"][1] = self._short_ema_span_0_1
    @short_ema_span_1_0.setter
    def short_ema_span_1_0(self, new_short_ema_span_1_0):
        self._short_ema_span_1_0 = new_short_ema_span_1_0
        self._config["short_ema_span_1"][0] = self._short_ema_span_1_0
    @short_ema_span_1_1.setter
    def short_ema_span_1_1(self, new_short_ema_span_1_1):
        self._short_ema_span_1_1 = new_short_ema_span_1_1
        self._config["short_ema_span_1"][1] = self._short_ema_span_1_1
    @short_initial_eprice_ema_dist_0.setter
    def short_initial_eprice_ema_dist_0(self, new_short_initial_eprice_ema_dist_0):
        self._short_initial_eprice_ema_dist_0 = new_short_initial_eprice_ema_dist_0
        self._config["short_initial_eprice_ema_dist"][0] = self._short_initial_eprice_ema_dist_0
    @short_initial_eprice_ema_dist_1.setter
    def short_initial_eprice_ema_dist_1(self, new_short_initial_eprice_ema_dist_1):
        self._short_initial_eprice_ema_dist_1 = new_short_initial_eprice_ema_dist_1
        self._config["short_initial_eprice_ema_dist"][1] = self._short_initial_eprice_ema_dist_1
    @short_initial_qty_pct_0.setter
    def short_initial_qty_pct_0(self, new_short_initial_qty_pct_0):
        self._short_initial_qty_pct_0 = new_short_initial_qty_pct_0
        self._config["short_initial_qty_pct"][0] = self._short_initial_qty_pct_0
    @short_initial_qty_pct_1.setter
    def short_initial_qty_pct_1(self, new_short_initial_qty_pct_1):
        self._short_initial_qty_pct_1 = new_short_initial_qty_pct_1
        self._config["short_initial_qty_pct"][1] = self._short_initial_qty_pct_1
    @short_markup_range_0.setter
    def short_markup_range_0(self, new_short_markup_range_0):
        self._short_markup_range_0 = new_short_markup_range_0
        self._config["short_markup_range"][0] = self._short_markup_range_0
    @short_markup_range_1.setter
    def short_markup_range_1(self, new_short_markup_range_1):
        self._short_markup_range_1 = new_short_markup_range_1
        self._config["short_markup_range"][1] = self._short_markup_range_1
    @short_min_markup_0.setter
    def short_min_markup_0(self, new_short_min_markup_0):
        self._short_min_markup_0 = new_short_min_markup_0
        self._config["short_min_markup"][0] = self._short_min_markup_0
    @short_min_markup_1.setter
    def short_min_markup_1(self, new_short_min_markup_1):
        self._short_min_markup_1 = new_short_min_markup_1
        self._config["short_min_markup"][1] = self._short_min_markup_1
    @short_n_close_orders_0.setter
    def short_n_close_orders_0(self, new_short_n_close_orders_0):
        self._short_n_close_orders_0 = new_short_n_close_orders_0
        self._config["short_n_close_orders"][0] = self._short_n_close_orders_0
    @short_n_close_orders_1.setter
    def short_n_close_orders_1(self, new_short_n_close_orders_1):
        self._short_n_close_orders_1 = new_short_n_close_orders_1
        self._config["short_n_close_orders"][1] = self._short_n_close_orders_1
    @short_rentry_pprice_dist_0.setter
    def short_rentry_pprice_dist_0(self, new_short_rentry_pprice_dist_0):
        self._short_rentry_pprice_dist_0 = new_short_rentry_pprice_dist_0
        self._config["short_rentry_pprice_dist"][0] = self._short_rentry_pprice_dist_0
    @short_rentry_pprice_dist_1.setter
    def short_rentry_pprice_dist_1(self, new_short_rentry_pprice_dist_1):
        self._short_rentry_pprice_dist_1 = new_short_rentry_pprice_dist_1
        self._config["short_rentry_pprice_dist"][1] = self._short_rentry_pprice_dist_1
    @short_rentry_pprice_dist_wallet_exposure_weighting_0.setter
    def short_rentry_pprice_dist_wallet_exposure_weighting_0(self, new_short_rentry_pprice_dist_wallet_exposure_weighting_0):
        self._short_rentry_pprice_dist_wallet_exposure_weighting_0 = new_short_rentry_pprice_dist_wallet_exposure_weighting_0
        self._config["short_rentry_pprice_dist_wallet_exposure_weighting"][0] = self._short_rentry_pprice_dist_wallet_exposure_weighting_0
    @short_rentry_pprice_dist_wallet_exposure_weighting_1.setter
    def short_rentry_pprice_dist_wallet_exposure_weighting_1(self, new_short_rentry_pprice_dist_wallet_exposure_weighting_1):
        self._short_rentry_pprice_dist_wallet_exposure_weighting_1 = new_short_rentry_pprice_dist_wallet_exposure_weighting_1
        self._config["short_rentry_pprice_dist_wallet_exposure_weighting"][1] = self._short_rentry_pprice_dist_wallet_exposure_weighting_1
