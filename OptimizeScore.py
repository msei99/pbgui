import streamlit as st

class OptimizeScore:
    MAXIMUM_DRAWDOWN_MAX_LONG_MIN = -1.0
    MAXIMUM_DRAWDOWN_MAX_LONG_MAX = 1.0
    MAXIMUM_DRAWDOWN_MAX_LONG_STEP = 0.01
    MAXIMUM_DRAWDOWN_MAX_LONG_ROUND = 2
    MAXIMUM_DRAWDOWN_MAX_LONG_FORMAT = f'%.{MAXIMUM_DRAWDOWN_MAX_LONG_ROUND}f'
    MAXIMUM_DRAWDOWN_MAX_SHORT_MIN = -1.0
    MAXIMUM_DRAWDOWN_MAX_SHORT_MAX = 1.0
    MAXIMUM_DRAWDOWN_MAX_SHORT_STEP = 0.01
    MAXIMUM_DRAWDOWN_MAX_SHORT_ROUND = 2
    MAXIMUM_DRAWDOWN_MAX_SHORT_FORMAT = f'%.{MAXIMUM_DRAWDOWN_MAX_SHORT_ROUND}f'
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MIN = -1.0
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MAX = 1.0
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_STEP = 0.01
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_ROUND = 2
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_FORMAT = f'%.{MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_ROUND}f'
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MIN = -1.0
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MAX = 1.0
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_STEP = 0.01
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_ROUND = 2
    MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_FORMAT = f'%.{MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_ROUND}f'
    MAXIMUM_PA_DISTANCE_STD_LONG_MIN = -1.0
    MAXIMUM_PA_DISTANCE_STD_LONG_MAX = 1000.0
    MAXIMUM_PA_DISTANCE_STD_LONG_STEP = 0.001
    MAXIMUM_PA_DISTANCE_STD_LONG_ROUND = 3
    MAXIMUM_PA_DISTANCE_STD_LONG_FORMAT = f'%.{MAXIMUM_PA_DISTANCE_STD_LONG_ROUND}f'
    MAXIMUM_PA_DISTANCE_STD_SHORT_MIN = -1.0
    MAXIMUM_PA_DISTANCE_STD_SHORT_MAX = 1000.0
    MAXIMUM_PA_DISTANCE_STD_SHORT_STEP = 0.001
    MAXIMUM_PA_DISTANCE_STD_SHORT_ROUND = 3
    MAXIMUM_PA_DISTANCE_STD_SHORT_FORMAT = f'%.{MAXIMUM_PA_DISTANCE_STD_SHORT_ROUND}f'
    MAXIMUM_PA_DISTANCE_MEAN_LONG_MIN = -1.0
    MAXIMUM_PA_DISTANCE_MEAN_LONG_MAX = 1000.0
    MAXIMUM_PA_DISTANCE_MEAN_LONG_STEP = 0.001
    MAXIMUM_PA_DISTANCE_MEAN_LONG_ROUND = 3
    MAXIMUM_PA_DISTANCE_MEAN_LONG_FORMAT = f'%.{MAXIMUM_PA_DISTANCE_MEAN_LONG_ROUND}f'
    MAXIMUM_PA_DISTANCE_MEAN_SHORT_MIN = -1.0
    MAXIMUM_PA_DISTANCE_MEAN_SHORT_MAX = 1000.0
    MAXIMUM_PA_DISTANCE_MEAN_SHORT_STEP = 0.001
    MAXIMUM_PA_DISTANCE_MEAN_SHORT_ROUND = 3
    MAXIMUM_PA_DISTANCE_MEAN_SHORT_FORMAT = f'%.{MAXIMUM_PA_DISTANCE_MEAN_SHORT_ROUND}f'
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MIN = -1.0
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MAX = 1000.0
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_STEP = 0.001
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_ROUND = 3
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_FORMAT = f'%.{MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_ROUND}f'
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MIN = -1.0
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MAX = 1000.0
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_STEP = 0.001
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_ROUND = 3
    MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_FORMAT = f'%.{MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_ROUND}f'
    MAXIMUM_LOSS_PROFIT_RATIO_LONG_MIN = -1.0
    MAXIMUM_LOSS_PROFIT_RATIO_LONG_MAX = 1.0
    MAXIMUM_LOSS_PROFIT_RATIO_LONG_STEP = 0.001
    MAXIMUM_LOSS_PROFIT_RATIO_LONG_ROUND = 3
    MAXIMUM_LOSS_PROFIT_RATIO_LONG_FORMAT = f'%.{MAXIMUM_LOSS_PROFIT_RATIO_LONG_ROUND}f'
    MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MIN = -1.0
    MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MAX = 1.0
    MAXIMUM_LOSS_PROFIT_RATIO_SHORT_STEP = 0.001
    MAXIMUM_LOSS_PROFIT_RATIO_SHORT_ROUND = 3
    MAXIMUM_LOSS_PROFIT_RATIO_SHORT_FORMAT = f'%.{MAXIMUM_LOSS_PROFIT_RATIO_SHORT_ROUND}f'
    MAXIMUM_HRS_STUCK_MAX_LONG_MIN = -1.0
    MAXIMUM_HRS_STUCK_MAX_LONG_MAX = 1000000.0
    MAXIMUM_HRS_STUCK_MAX_LONG_STEP = 10.0
    MAXIMUM_HRS_STUCK_MAX_LONG_ROUND = 1
    MAXIMUM_HRS_STUCK_MAX_LONG_FORMAT = f'%.{MAXIMUM_HRS_STUCK_MAX_LONG_ROUND}f'
    MAXIMUM_HRS_STUCK_MAX_SHORT_MIN = -1.0
    MAXIMUM_HRS_STUCK_MAX_SHORT_MAX = 1000000.0
    MAXIMUM_HRS_STUCK_MAX_SHORT_STEP = 10.0
    MAXIMUM_HRS_STUCK_MAX_SHORT_ROUND = 1
    MAXIMUM_HRS_STUCK_MAX_SHORT_FORMAT = f'%.{MAXIMUM_HRS_STUCK_MAX_SHORT_ROUND}f'
    MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MIN = -1.0
    MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MAX = 1.0
    MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_STEP = 0.001
    MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_ROUND = 3
    MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_FORMAT = f'%.{MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_ROUND}f'
    MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MIN = -1.0
    MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MAX = 1.0
    MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_STEP = 0.001
    MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_ROUND = 3
    MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_FORMAT = f'%.{MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_ROUND}f'
    MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MIN = -1.0
    MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MAX = 1.0
    MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_STEP = 0.001
    MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_ROUND = 3
    MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_FORMAT = f'%.{MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_ROUND}f'
    MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MIN = -1.0
    MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MAX = 1.0
    MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_STEP = 0.001
    MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_ROUND = 3
    MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_FORMAT = f'%.{MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_ROUND}f'

    def __init__(self):
        self._config = None
        self._maximum_drawdown_max_long = -1
        self._maximum_drawdown_max_long_enable = False
        self._maximum_drawdown_max_short = -1
        self._maximum_drawdown_max_short_enable = False
        self._maximum_drawdown_1pct_worst_mean_long = -1
        self._maximum_drawdown_1pct_worst_mean_long_enable = False
        self._maximum_drawdown_1pct_worst_mean_short = -1
        self._maximum_drawdown_1pct_worst_mean_short_enable = False
        self._maximum_pa_distance_std_long = -1
        self._maximum_pa_distance_std_long_enable = False
        self._maximum_pa_distance_std_short = -1
        self._maximum_pa_distance_std_short_enable = False
        self._maximum_pa_distance_mean_long = -1
        self._maximum_pa_distance_mean_long_enable = False
        self._maximum_pa_distance_mean_short = -1
        self._maximum_pa_distance_mean_short_enable = False
        self._maximum_pa_distance_1pct_worst_mean_long = -1
        self._maximum_pa_distance_1pct_worst_mean_long_enable = False
        self._maximum_pa_distance_1pct_worst_mean_short = -1
        self._maximum_pa_distance_1pct_worst_mean_short_enable = False
        self._maximum_loss_profit_ratio_long = -1
        self._maximum_loss_profit_ratio_long_enable = False
        self._maximum_loss_profit_ratio_short = -1
        self._maximum_loss_profit_ratio_short_enable = False
        self._maximum_hrs_stuck_max_long = -1
        self._maximum_hrs_stuck_max_long_enable = False
        self._maximum_hrs_stuck_max_short = -1
        self._maximum_hrs_stuck_max_short_enable = False
        self._maximum_exposure_ratios_mean_long = -1
        self._maximum_exposure_ratios_mean_long_enable = False
        self._maximum_exposure_ratios_mean_short = -1
        self._maximum_exposure_ratios_mean_short_enable = False
        self._maximum_time_at_max_exposure_long = -1
        self._maximum_time_at_max_exposure_long_enable = False
        self._maximum_time_at_max_exposure_short = -1
        self._maximum_time_at_max_exposure_short_enable = False

    @property
    def config(self): return self._config
    @config.setter
    def config(self, new_config):
        if "maximum_drawdown_max_long" in new_config:
            self.maximum_drawdown_max_long = new_config["maximum_drawdown_max_long"]
            if self._maximum_drawdown_max_long < self.MAXIMUM_DRAWDOWN_MAX_LONG_MIN:
                self._maximum_drawdown_max_long = self.MAXIMUM_DRAWDOWN_MAX_LONG_MIN
            if self._maximum_drawdown_max_long > self.MAXIMUM_DRAWDOWN_MAX_LONG_MAX:
                self._maximum_drawdown_max_long = self.MAXIMUM_DRAWDOWN_MAX_LONG_MAX
        if "maximum_drawdown_max_short" in new_config:
            self.maximum_drawdown_max_short = new_config["maximum_drawdown_max_short"]
            if self._maximum_drawdown_max_short < self.MAXIMUM_DRAWDOWN_MAX_SHORT_MIN:
                self._maximum_drawdown_max_short = self.MAXIMUM_DRAWDOWN_MAX_SHORT_MIN
            if self._maximum_drawdown_max_short > self.MAXIMUM_DRAWDOWN_MAX_SHORT_MAX:
                self._maximum_drawdown_max_short = self.MAXIMUM_DRAWDOWN_MAX_SHORT_MAX
        if "maximum_drawdown_1pct_worst_mean_long" in new_config:
            self.maximum_drawdown_1pct_worst_mean_long = new_config["maximum_drawdown_1pct_worst_mean_long"]
            if self._maximum_drawdown_1pct_worst_mean_long < self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MIN:
                self._maximum_drawdown_1pct_worst_mean_long = self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MIN
            if self._maximum_drawdown_1pct_worst_mean_long > self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MAX:
                self._maximum_drawdown_1pct_worst_mean_long = self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MAX
        if "maximum_drawdown_1pct_worst_mean_short" in new_config:
            self.maximum_drawdown_1pct_worst_mean_short = new_config["maximum_drawdown_1pct_worst_mean_short"]
            if self._maximum_drawdown_1pct_worst_mean_short < self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MIN:
                self._maximum_drawdown_1pct_worst_mean_short = self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MIN
            if self._maximum_drawdown_1pct_worst_mean_short > self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MAX:
                self._maximum_drawdown_1pct_worst_mean_short = self.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MAX
        if "maximum_pa_distance_std_long" in new_config:
            self.maximum_pa_distance_std_long = new_config["maximum_pa_distance_std_long"]
            if self._maximum_pa_distance_std_long < self.MAXIMUM_PA_DISTANCE_STD_LONG_MIN:
                self._maximum_pa_distance_std_long = self.MAXIMUM_PA_DISTANCE_STD_LONG_MIN
            if self._maximum_pa_distance_std_long > self.MAXIMUM_PA_DISTANCE_STD_LONG_MAX:
                self._maximum_pa_distance_std_long = self.MAXIMUM_PA_DISTANCE_STD_LONG_MAX
        if "maximum_pa_distance_std_short" in new_config:
            self.maximum_pa_distance_std_short = new_config["maximum_pa_distance_std_short"]
            if self._maximum_pa_distance_std_short < self.MAXIMUM_PA_DISTANCE_STD_SHORT_MIN:
                self._maximum_pa_distance_std_short = self.MAXIMUM_PA_DISTANCE_STD_SHORT_MIN
            if self._maximum_pa_distance_std_short > self.MAXIMUM_PA_DISTANCE_STD_SHORT_MAX:
                self._maximum_pa_distance_std_short = self.MAXIMUM_PA_DISTANCE_STD_SHORT_MAX
        if "maximum_pa_distance_mean_long" in new_config:
            self.maximum_pa_distance_mean_long = new_config["maximum_pa_distance_mean_long"]
            if self._maximum_pa_distance_mean_long < self.MAXIMUM_PA_DISTANCE_MEAN_LONG_MIN:
                self._maximum_pa_distance_mean_long = self.MAXIMUM_PA_DISTANCE_MEAN_LONG_MIN
            if self._maximum_pa_distance_mean_long > self.MAXIMUM_PA_DISTANCE_MEAN_LONG_MAX:
                self._maximum_pa_distance_mean_long = self.MAXIMUM_PA_DISTANCE_MEAN_LONG_MAX
        if "maximum_pa_distance_mean_short" in new_config:
            self.maximum_pa_distance_mean_short = new_config["maximum_pa_distance_mean_short"]
            if self._maximum_pa_distance_mean_short < self.MAXIMUM_PA_DISTANCE_MEAN_SHORT_MIN:
                self._maximum_pa_distance_mean_short = self.MAXIMUM_PA_DISTANCE_MEAN_SHORT_MIN
            if self._maximum_pa_distance_mean_short > self.MAXIMUM_PA_DISTANCE_MEAN_SHORT_MAX:
                self._maximum_pa_distance_mean_short = self.MAXIMUM_PA_DISTANCE_MEAN_SHORT_MAX
        if "maximum_pa_distance_1pct_worst_mean_long" in new_config:
            self.maximum_pa_distance_1pct_worst_mean_long = new_config["maximum_pa_distance_1pct_worst_mean_long"]
            if self._maximum_pa_distance_1pct_worst_mean_long < self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MIN:
                self._maximum_pa_distance_1pct_worst_mean_long = self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MIN
            if self._maximum_pa_distance_1pct_worst_mean_long > self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MAX:
                self._maximum_pa_distance_1pct_worst_mean_long = self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MAX
        if "maximum_pa_distance_1pct_worst_mean_short" in new_config:
            self.maximum_pa_distance_1pct_worst_mean_short = new_config["maximum_pa_distance_1pct_worst_mean_short"]
            if self._maximum_pa_distance_1pct_worst_mean_short < self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MIN:
                self._maximum_pa_distance_1pct_worst_mean_short = self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MIN
            if self._maximum_pa_distance_1pct_worst_mean_short > self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MAX:
                self._maximum_pa_distance_1pct_worst_mean_short = self.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MAX
        if "maximum_loss_profit_ratio_long" in new_config:
            self.maximum_loss_profit_ratio_long = new_config["maximum_loss_profit_ratio_long"]
            if self._maximum_loss_profit_ratio_long < self.MAXIMUM_LOSS_PROFIT_RATIO_LONG_MIN:
                self._maximum_loss_profit_ratio_long = self.MAXIMUM_LOSS_PROFIT_RATIO_LONG_MIN
            if self._maximum_loss_profit_ratio_long > self.MAXIMUM_LOSS_PROFIT_RATIO_LONG_MAX:
                self._maximum_loss_profit_ratio_long = self.MAXIMUM_LOSS_PROFIT_RATIO_LONG_MAX
        if "maximum_loss_profit_ratio_short" in new_config:
            self.maximum_loss_profit_ratio_short = new_config["maximum_loss_profit_ratio_short"]
            if self._maximum_loss_profit_ratio_short < self.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MIN:
                self._maximum_loss_profit_ratio_short = self.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MIN
            if self._maximum_loss_profit_ratio_short > self.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MAX:
                self._maximum_loss_profit_ratio_short = self.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MAX
        if "maximum_hrs_stuck_max_long" in new_config:
            self.maximum_hrs_stuck_max_long = new_config["maximum_hrs_stuck_max_long"]
            if self._maximum_hrs_stuck_max_long < self.MAXIMUM_HRS_STUCK_MAX_LONG_MIN:
                self._maximum_hrs_stuck_max_long = self.MAXIMUM_HRS_STUCK_MAX_LONG_MIN
            if self._maximum_hrs_stuck_max_long > self.MAXIMUM_HRS_STUCK_MAX_LONG_MAX:
                self._maximum_hrs_stuck_max_long = self.MAXIMUM_HRS_STUCK_MAX_LONG_MAX
        if "maximum_hrs_stuck_max_short" in new_config:
            self.maximum_hrs_stuck_max_short = new_config["maximum_hrs_stuck_max_short"]
            if self._maximum_hrs_stuck_max_short < self.MAXIMUM_HRS_STUCK_MAX_SHORT_MIN:
                self._maximum_hrs_stuck_max_short = self.MAXIMUM_HRS_STUCK_MAX_SHORT_MIN
            if self._maximum_hrs_stuck_max_short > self.MAXIMUM_HRS_STUCK_MAX_SHORT_MAX:
                self._maximum_hrs_stuck_max_short = self.MAXIMUM_HRS_STUCK_MAX_SHORT_MAX
        if "maximum_exposure_ratios_mean_long" in new_config:
            self.maximum_exposure_ratios_mean_long = new_config["maximum_exposure_ratios_mean_long"]
            if self._maximum_exposure_ratios_mean_long < self.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MIN:
                self._maximum_exposure_ratios_mean_long = self.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MIN
            if self._maximum_exposure_ratios_mean_long > self.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MAX:
                self._maximum_exposure_ratios_mean_long = self.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MAX
        if "maximum_exposure_ratios_mean_short" in new_config:
            self.maximum_exposure_ratios_mean_short = new_config["maximum_exposure_ratios_mean_short"]
            if self._maximum_exposure_ratios_mean_short < self.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MIN:
                self._maximum_exposure_ratios_mean_short = self.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MIN
            if self._maximum_exposure_ratios_mean_short > self.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MAX:
                self._maximum_exposure_ratios_mean_short = self.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MAX
        if "maximum_time_at_max_exposure_long" in new_config:
            self.maximum_time_at_max_exposure_long = new_config["maximum_time_at_max_exposure_long"]
            if self._maximum_time_at_max_exposure_long < self.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MIN:
                self._maximum_time_at_max_exposure_long = self.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MIN
            if self._maximum_time_at_max_exposure_long > self.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MAX:
                self._maximum_time_at_max_exposure_long = self.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MAX
        if "maximum_time_at_max_exposure_short" in new_config:
            self.maximum_time_at_max_exposure_short = new_config["maximum_time_at_max_exposure_short"]
            if self._maximum_time_at_max_exposure_short < self.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MIN:
                self._maximum_time_at_max_exposure_short = self.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MIN
            if self._maximum_time_at_max_exposure_short > self.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MAX:
                self._maximum_time_at_max_exposure_short = self.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MAX
        self._config = new_config

    @property
    def maximum_drawdown_max_long(self): return self._maximum_drawdown_max_long
    @property
    def maximum_drawdown_max_long_enable(self): return self._maximum_drawdown_max_long_enable
    @property
    def maximum_drawdown_max_short(self): return self._maximum_drawdown_max_short
    @property
    def maximum_drawdown_max_short_enable(self): return self._maximum_drawdown_max_short_enable
    @property
    def maximum_drawdown_1pct_worst_mean_long(self): return self._maximum_drawdown_1pct_worst_mean_long
    @property
    def maximum_drawdown_1pct_worst_mean_long_enable(self): return self._maximum_drawdown_1pct_worst_mean_long_enable
    @property
    def maximum_drawdown_1pct_worst_mean_short(self): return self._maximum_drawdown_1pct_worst_mean_short
    @property
    def maximum_drawdown_1pct_worst_mean_short_enable(self): return self._maximum_drawdown_1pct_worst_mean_short_enable
    @property
    def maximum_pa_distance_std_long(self): return self._maximum_pa_distance_std_long
    @property
    def maximum_pa_distance_std_long_enable(self): return self._maximum_pa_distance_std_long_enable
    @property
    def maximum_pa_distance_std_short(self): return self._maximum_pa_distance_std_short
    @property
    def maximum_pa_distance_std_short_enable(self): return self._maximum_pa_distance_std_short_enable
    @property
    def maximum_pa_distance_mean_long(self): return self._maximum_pa_distance_mean_long
    @property
    def maximum_pa_distance_mean_long_enable(self): return self._maximum_pa_distance_mean_long_enable
    @property
    def maximum_pa_distance_mean_short(self): return self._maximum_pa_distance_mean_short
    @property
    def maximum_pa_distance_mean_short_enable(self): return self._maximum_pa_distance_mean_short_enable
    @property
    def maximum_pa_distance_1pct_worst_mean_long(self): return self._maximum_pa_distance_1pct_worst_mean_long
    @property
    def maximum_pa_distance_1pct_worst_mean_long_enable(self): return self._maximum_pa_distance_1pct_worst_mean_long_enable
    @property
    def maximum_pa_distance_1pct_worst_mean_short(self): return self._maximum_pa_distance_1pct_worst_mean_short
    @property
    def maximum_pa_distance_1pct_worst_mean_short_enable(self): return self._maximum_pa_distance_1pct_worst_mean_short_enable
    @property
    def maximum_loss_profit_ratio_long(self): return self._maximum_loss_profit_ratio_long
    @property
    def maximum_loss_profit_ratio_long_enable(self): return self._maximum_loss_profit_ratio_long_enable
    @property
    def maximum_loss_profit_ratio_short(self): return self._maximum_loss_profit_ratio_short
    @property
    def maximum_loss_profit_ratio_short_enable(self): return self._maximum_loss_profit_ratio_short_enable
    @property
    def maximum_hrs_stuck_max_long(self): return self._maximum_hrs_stuck_max_long
    @property
    def maximum_hrs_stuck_max_long_enable(self): return self._maximum_hrs_stuck_max_long_enable
    @property
    def maximum_hrs_stuck_max_short(self): return self._maximum_hrs_stuck_max_short
    @property
    def maximum_hrs_stuck_max_short_enable(self): return self._maximum_hrs_stuck_max_short_enable
    @property
    def maximum_exposure_ratios_mean_long(self): return self._maximum_exposure_ratios_mean_long
    @property
    def maximum_exposure_ratios_mean_long_enable(self): return self._maximum_exposure_ratios_mean_long_enable
    @property
    def maximum_exposure_ratios_mean_short(self): return self._maximum_exposure_ratios_mean_short
    @property
    def maximum_exposure_ratios_mean_short_enable(self): return self._maximum_exposure_ratios_mean_short_enable
    @property
    def maximum_time_at_max_exposure_long(self): return self._maximum_time_at_max_exposure_long
    @property
    def maximum_time_at_max_exposure_long_enable(self): return self._maximum_time_at_max_exposure_long_enable
    @property
    def maximum_time_at_max_exposure_short(self): return self._maximum_time_at_max_exposure_short
    @property
    def maximum_time_at_max_exposure_short_enable(self): return self._maximum_time_at_max_exposure_short_enable

    @maximum_drawdown_max_long.setter
    def maximum_drawdown_max_long(self, new_maximum_drawdown_max_long):
        if new_maximum_drawdown_max_long <= 0:
            self._maximum_drawdown_max_long_enable = False
            self._maximum_drawdown_max_long = -1
        else:
            self._maximum_drawdown_max_long_enable = True
            self._maximum_drawdown_max_long = new_maximum_drawdown_max_long
        if self._config:
            self._config["maximum_drawdown_max_long"] = self._maximum_drawdown_max_long
    @maximum_drawdown_max_long_enable.setter
    def maximum_drawdown_max_long_enable(self, new_maximum_drawdown_max_long_enable):
        self._maximum_drawdown_max_long_enable = new_maximum_drawdown_max_long_enable
        if self._maximum_drawdown_max_long_enable:
            self.maximum_drawdown_max_long = 0.25
        else:
            self.maximum_drawdown_max_long = -1
    @maximum_drawdown_max_short.setter
    def maximum_drawdown_max_short(self, new_maximum_drawdown_max_short):
        if new_maximum_drawdown_max_short <= 0:
            self._maximum_drawdown_max_short_enable = False
            self._maximum_drawdown_max_short = -1
        else:
            self._maximum_drawdown_max_short_enable = True
            self._maximum_drawdown_max_short = new_maximum_drawdown_max_short
        if self._config:
            self._config["maximum_drawdown_max_short"] = self._maximum_drawdown_max_short
    @maximum_drawdown_max_short_enable.setter
    def maximum_drawdown_max_short_enable(self, new_maximum_drawdown_max_short_enable):
        self._maximum_drawdown_max_short_enable = new_maximum_drawdown_max_short_enable
        if self._maximum_drawdown_max_short_enable:
            self.maximum_drawdown_max_short = 0.25
        else:
            self.maximum_drawdown_max_short = -1
    @maximum_drawdown_1pct_worst_mean_long.setter
    def maximum_drawdown_1pct_worst_mean_long(self, new_maximum_drawdown_1pct_worst_mean_long):
        if new_maximum_drawdown_1pct_worst_mean_long <= 0:
            self._maximum_drawdown_1pct_worst_mean_long_enable = False
            self._maximum_drawdown_1pct_worst_mean_long = -1
        else:
            self._maximum_drawdown_1pct_worst_mean_long_enable = True
            self._maximum_drawdown_1pct_worst_mean_long = new_maximum_drawdown_1pct_worst_mean_long
        if self._config:
            self._config["maximum_drawdown_1pct_worst_mean_long"] = self._maximum_drawdown_1pct_worst_mean_long
    @maximum_drawdown_1pct_worst_mean_long_enable.setter
    def maximum_drawdown_1pct_worst_mean_long_enable(self, new_maximum_drawdown_1pct_worst_mean_long_enable):
        self._maximum_drawdown_1pct_worst_mean_long_enable = new_maximum_drawdown_1pct_worst_mean_long_enable
        if self._maximum_drawdown_1pct_worst_mean_long_enable:
            self.maximum_drawdown_1pct_worst_mean_long = 0.1
        else:
            self.maximum_drawdown_1pct_worst_mean_long = -1
    @maximum_drawdown_1pct_worst_mean_short.setter
    def maximum_drawdown_1pct_worst_mean_short(self, new_maximum_drawdown_1pct_worst_mean_short):
        if new_maximum_drawdown_1pct_worst_mean_short <= 0:
            self._maximum_drawdown_1pct_worst_mean_short_enable = False
            self._maximum_drawdown_1pct_worst_mean_short = -1
        else:
            self._maximum_drawdown_1pct_worst_mean_short_enable = True
            self._maximum_drawdown_1pct_worst_mean_short = new_maximum_drawdown_1pct_worst_mean_short
        if self._config:
            self._config["maximum_drawdown_1pct_worst_mean_short"] = self._maximum_drawdown_1pct_worst_mean_short
    @maximum_drawdown_1pct_worst_mean_short_enable.setter
    def maximum_drawdown_1pct_worst_mean_short_enable(self, new_maximum_drawdown_1pct_worst_mean_short_enable):
        self._maximum_drawdown_1pct_worst_mean_short_enable = new_maximum_drawdown_1pct_worst_mean_short_enable
        if self._maximum_drawdown_1pct_worst_mean_short_enable:
            self.maximum_drawdown_1pct_worst_mean_short = 0.1
        else:
            self.maximum_drawdown_1pct_worst_mean_short = -1
    @maximum_pa_distance_std_long.setter
    def maximum_pa_distance_std_long(self, new_maximum_pa_distance_std_long):
        if new_maximum_pa_distance_std_long <= 0:
            self._maximum_pa_distance_std_long_enable = False
            self._maximum_pa_distance_std_long = -1
        else:
            self._maximum_pa_distance_std_long_enable = True
            self._maximum_pa_distance_std_long = new_maximum_pa_distance_std_long
        if self._config:
            self._config["maximum_pa_distance_std_long"] = self._maximum_pa_distance_std_long
    @maximum_pa_distance_std_long_enable.setter
    def maximum_pa_distance_std_long_enable(self, new_maximum_pa_distance_std_long_enable):
        self._maximum_pa_distance_std_long_enable = new_maximum_pa_distance_std_long_enable
        if self._maximum_pa_distance_std_long_enable:
            self.maximum_pa_distance_std_long = 0.1
        else:
            self.maximum_pa_distance_std_long = -1
    @maximum_pa_distance_std_short.setter
    def maximum_pa_distance_std_short(self, new_maximum_pa_distance_std_short):
        if new_maximum_pa_distance_std_short <= 0:
            self._maximum_pa_distance_std_short_enable = False
            self._maximum_pa_distance_std_short = -1
        else:
            self._maximum_pa_distance_std_short_enable = True
            self._maximum_pa_distance_std_short = new_maximum_pa_distance_std_short
        if self._config:
            self._config["maximum_pa_distance_std_short"] = self._maximum_pa_distance_std_short
    @maximum_pa_distance_std_short_enable.setter
    def maximum_pa_distance_std_short_enable(self, new_maximum_pa_distance_std_short_enable):
        self._maximum_pa_distance_std_short_enable = new_maximum_pa_distance_std_short_enable
        if self._maximum_pa_distance_std_short_enable:
            self.maximum_pa_distance_std_short = 0.1
        else:
            self.maximum_pa_distance_std_short = -1
    @maximum_pa_distance_mean_long.setter
    def maximum_pa_distance_mean_long(self, new_maximum_pa_distance_mean_long):
        if new_maximum_pa_distance_mean_long <= 0:
            self._maximum_pa_distance_mean_long_enable = False
            self._maximum_pa_distance_mean_long = -1
        else:
            self._maximum_pa_distance_mean_long_enable = True
            self._maximum_pa_distance_mean_long = new_maximum_pa_distance_mean_long
        if self._config:
            self._config["maximum_pa_distance_mean_long"] = self._maximum_pa_distance_mean_long
    @maximum_pa_distance_mean_long_enable.setter
    def maximum_pa_distance_mean_long_enable(self, new_maximum_pa_distance_mean_long_enable):
        self._maximum_pa_distance_mean_long_enable = new_maximum_pa_distance_mean_long_enable
        if self._maximum_pa_distance_mean_long_enable:
            self.maximum_pa_distance_mean_long = 0.1
        else:
            self.maximum_pa_distance_mean_long = -1
    @maximum_pa_distance_mean_short.setter
    def maximum_pa_distance_mean_short(self, new_maximum_pa_distance_mean_short):
        if new_maximum_pa_distance_mean_short <= 0:
            self._maximum_pa_distance_mean_short_enable = False
            self._maximum_pa_distance_mean_short = -1
        else:
            self._maximum_pa_distance_mean_short_enable = True
            self._maximum_pa_distance_mean_short = new_maximum_pa_distance_mean_short
        if self._config:
            self._config["maximum_pa_distance_mean_short"] = self._maximum_pa_distance_mean_short
    @maximum_pa_distance_mean_short_enable.setter
    def maximum_pa_distance_mean_short_enable(self, new_maximum_pa_distance_mean_short_enable):
        self._maximum_pa_distance_mean_short_enable = new_maximum_pa_distance_mean_short_enable
        if self._maximum_pa_distance_mean_short_enable:
            self.maximum_pa_distance_mean_short = 0.1
        else:
            self.maximum_pa_distance_mean_short = -1
    @maximum_pa_distance_1pct_worst_mean_long.setter
    def maximum_pa_distance_1pct_worst_mean_long(self, new_maximum_pa_distance_1pct_worst_mean_long):
        if new_maximum_pa_distance_1pct_worst_mean_long <= 0:
            self._maximum_pa_distance_1pct_worst_mean_long_enable = False
            self._maximum_pa_distance_1pct_worst_mean_long = -1
        else:
            self._maximum_pa_distance_1pct_worst_mean_long_enable = True
            self._maximum_pa_distance_1pct_worst_mean_long = new_maximum_pa_distance_1pct_worst_mean_long
        if self._config:
            self._config["maximum_pa_distance_1pct_worst_mean_long"] = self._maximum_pa_distance_1pct_worst_mean_long
    @maximum_pa_distance_1pct_worst_mean_long_enable.setter
    def maximum_pa_distance_1pct_worst_mean_long_enable(self, new_maximum_pa_distance_1pct_worst_mean_long_enable):
        self._maximum_pa_distance_1pct_worst_mean_long_enable = new_maximum_pa_distance_1pct_worst_mean_long_enable
        if self._maximum_pa_distance_1pct_worst_mean_long_enable:
            self.maximum_pa_distance_1pct_worst_mean_long = 0.1
        else:
            self.maximum_pa_distance_1pct_worst_mean_long = -1
    @maximum_pa_distance_1pct_worst_mean_short.setter
    def maximum_pa_distance_1pct_worst_mean_short(self, new_maximum_pa_distance_1pct_worst_mean_short):
        if new_maximum_pa_distance_1pct_worst_mean_short <= 0:
            self._maximum_pa_distance_1pct_worst_mean_short_enable = False
            self._maximum_pa_distance_1pct_worst_mean_short = -1
        else:
            self._maximum_pa_distance_1pct_worst_mean_short_enable = True
            self._maximum_pa_distance_1pct_worst_mean_short = new_maximum_pa_distance_1pct_worst_mean_short
        if self._config:
            self._config["maximum_pa_distance_1pct_worst_mean_short"] = self._maximum_pa_distance_1pct_worst_mean_short
    @maximum_pa_distance_1pct_worst_mean_short_enable.setter
    def maximum_pa_distance_1pct_worst_mean_short_enable(self, new_maximum_pa_distance_1pct_worst_mean_short_enable):
        self._maximum_pa_distance_1pct_worst_mean_short_enable = new_maximum_pa_distance_1pct_worst_mean_short_enable
        if self._maximum_pa_distance_1pct_worst_mean_short_enable:
            self.maximum_pa_distance_1pct_worst_mean_short = 0.1
        else:
            self.maximum_pa_distance_1pct_worst_mean_short = -1
    @maximum_loss_profit_ratio_long.setter
    def maximum_loss_profit_ratio_long(self, new_maximum_loss_profit_ratio_long):
        if new_maximum_loss_profit_ratio_long <= 0:
            self._maximum_loss_profit_ratio_long_enable = False
            self._maximum_loss_profit_ratio_long = -1
        else:
            self._maximum_loss_profit_ratio_long_enable = True
            self._maximum_loss_profit_ratio_long = new_maximum_loss_profit_ratio_long
        if self._config:
            self._config["maximum_loss_profit_ratio_long"] = self._maximum_loss_profit_ratio_long
    @maximum_loss_profit_ratio_long_enable.setter
    def maximum_loss_profit_ratio_long_enable(self, new_maximum_loss_profit_ratio_long_enable):
        self._maximum_loss_profit_ratio_long_enable = new_maximum_loss_profit_ratio_long_enable
        if self._maximum_loss_profit_ratio_long_enable:
            self.maximum_loss_profit_ratio_long = 0.1
        else:
            self.maximum_loss_profit_ratio_long = -1
    @maximum_loss_profit_ratio_short.setter
    def maximum_loss_profit_ratio_short(self, new_maximum_loss_profit_ratio_short):
        if new_maximum_loss_profit_ratio_short <= 0:
            self._maximum_loss_profit_ratio_short_enable = False
            self._maximum_loss_profit_ratio_short = -1
        else:
            self._maximum_loss_profit_ratio_short_enable = True
            self._maximum_loss_profit_ratio_short = new_maximum_loss_profit_ratio_short
        if self._config:
            self._config["maximum_loss_profit_ratio_short"] = self._maximum_loss_profit_ratio_short
    @maximum_loss_profit_ratio_short_enable.setter
    def maximum_loss_profit_ratio_short_enable(self, new_maximum_loss_profit_ratio_short_enable):
        self._maximum_loss_profit_ratio_short_enable = new_maximum_loss_profit_ratio_short_enable
        if self._maximum_loss_profit_ratio_short_enable:
            self.maximum_loss_profit_ratio_short = 0.1
        else:
            self.maximum_loss_profit_ratio_short = -1
    @maximum_hrs_stuck_max_long.setter
    def maximum_hrs_stuck_max_long(self, new_maximum_hrs_stuck_max_long):
        if new_maximum_hrs_stuck_max_long <= 0:
            self._maximum_hrs_stuck_max_long_enable = False
            self._maximum_hrs_stuck_max_long = -1
        else:
            self._maximum_hrs_stuck_max_long_enable = True
            self._maximum_hrs_stuck_max_long = new_maximum_hrs_stuck_max_long
        if self._config:
            self._config["maximum_hrs_stuck_max_long"] = self._maximum_hrs_stuck_max_long
    @maximum_hrs_stuck_max_long_enable.setter
    def maximum_hrs_stuck_max_long_enable(self, new_maximum_hrs_stuck_max_long_enable):
        self._maximum_hrs_stuck_max_long_enable = new_maximum_hrs_stuck_max_long_enable
        if self._maximum_hrs_stuck_max_long_enable:
            self.maximum_hrs_stuck_max_long = 168.0
        else:
            self.maximum_hrs_stuck_max_long = -1
    @maximum_hrs_stuck_max_short.setter
    def maximum_hrs_stuck_max_short(self, new_maximum_hrs_stuck_max_short):
        if new_maximum_hrs_stuck_max_short <= 0:
            self._maximum_hrs_stuck_max_short_enable = False
            self._maximum_hrs_stuck_max_short = -1
        else:
            self._maximum_hrs_stuck_max_short_enable = True
            self._maximum_hrs_stuck_max_short = new_maximum_hrs_stuck_max_short
        if self._config:
            self._config["maximum_hrs_stuck_max_short"] = self._maximum_hrs_stuck_max_short
    @maximum_hrs_stuck_max_short_enable.setter
    def maximum_hrs_stuck_max_short_enable(self, new_maximum_hrs_stuck_max_short_enable):
        self._maximum_hrs_stuck_max_short_enable = new_maximum_hrs_stuck_max_short_enable
        if self._maximum_hrs_stuck_max_short_enable:
            self.maximum_hrs_stuck_max_short = 168.0
        else:
            self.maximum_hrs_stuck_max_short = -1
    @maximum_exposure_ratios_mean_long.setter
    def maximum_exposure_ratios_mean_long(self, new_maximum_exposure_ratios_mean_long):
        if new_maximum_exposure_ratios_mean_long <= 0:
            self._maximum_exposure_ratios_mean_long_enable = False
            self._maximum_exposure_ratios_mean_long = -1
        else:
            self._maximum_exposure_ratios_mean_long_enable = True
            self._maximum_exposure_ratios_mean_long = new_maximum_exposure_ratios_mean_long
        if self._config:
            self._config["maximum_exposure_ratios_mean_long"] = self._maximum_exposure_ratios_mean_long
    @maximum_exposure_ratios_mean_long_enable.setter
    def maximum_exposure_ratios_mean_long_enable(self, new_maximum_exposure_ratios_mean_long_enable):
        self._maximum_exposure_ratios_mean_long_enable = new_maximum_exposure_ratios_mean_long_enable
        if self._maximum_exposure_ratios_mean_long_enable:
            self.maximum_exposure_ratios_mean_long = 0.1
        else:
            self.maximum_exposure_ratios_mean_long = -1
    @maximum_exposure_ratios_mean_short.setter
    def maximum_exposure_ratios_mean_short(self, new_maximum_exposure_ratios_mean_short):
        if new_maximum_exposure_ratios_mean_short <= 0:
            self._maximum_exposure_ratios_mean_short_enable = False
            self._maximum_exposure_ratios_mean_short = -1
        else:
            self._maximum_exposure_ratios_mean_short_enable = True
            self._maximum_exposure_ratios_mean_short = new_maximum_exposure_ratios_mean_short
        if self._config:
            self._config["maximum_exposure_ratios_mean_short"] = self._maximum_exposure_ratios_mean_short
    @maximum_exposure_ratios_mean_short_enable.setter
    def maximum_exposure_ratios_mean_short_enable(self, new_maximum_exposure_ratios_mean_short_enable):
        self._maximum_exposure_ratios_mean_short_enable = new_maximum_exposure_ratios_mean_short_enable
        if self._maximum_exposure_ratios_mean_short_enable:
            self.maximum_exposure_ratios_mean_short = 0.1
        else:
            self.maximum_exposure_ratios_mean_short = -1
    @maximum_time_at_max_exposure_long.setter
    def maximum_time_at_max_exposure_long(self, new_maximum_time_at_max_exposure_long):
        if new_maximum_time_at_max_exposure_long <= 0:
            self._maximum_time_at_max_exposure_long_enable = False
            self._maximum_time_at_max_exposure_long = -1
        else:
            self._maximum_time_at_max_exposure_long_enable = True
            self._maximum_time_at_max_exposure_long = new_maximum_time_at_max_exposure_long
        if self._config:
            self._config["maximum_time_at_max_exposure_long"] = self._maximum_time_at_max_exposure_long
    @maximum_time_at_max_exposure_long_enable.setter
    def maximum_time_at_max_exposure_long_enable(self, new_maximum_time_at_max_exposure_long_enable):
        self._maximum_time_at_max_exposure_long_enable = new_maximum_time_at_max_exposure_long_enable
        if self._maximum_time_at_max_exposure_long_enable:
            self.maximum_time_at_max_exposure_long = 0.1
        else:
            self.maximum_time_at_max_exposure_long = -1
    @maximum_time_at_max_exposure_short.setter
    def maximum_time_at_max_exposure_short(self, new_maximum_time_at_max_exposure_short):
        if new_maximum_time_at_max_exposure_short <= 0:
            self._maximum_time_at_max_exposure_short_enable = False
            self._maximum_time_at_max_exposure_short = -1
        else:
            self._maximum_time_at_max_exposure_short_enable = True
            self._maximum_time_at_max_exposure_short = new_maximum_time_at_max_exposure_short
        if self._config:
            self._config["maximum_time_at_max_exposure_short"] = self._maximum_time_at_max_exposure_short
    @maximum_time_at_max_exposure_short_enable.setter
    def maximum_time_at_max_exposure_short_enable(self, new_maximum_time_at_max_exposure_short_enable):
        self._maximum_time_at_max_exposure_short_enable = new_maximum_time_at_max_exposure_short_enable
        if self._maximum_time_at_max_exposure_short_enable:
            self.maximum_time_at_max_exposure_short = 0.1
        else:
            self.maximum_time_at_max_exposure_short = -1
