import hjson
import streamlit as st
from pathlib import Path, PurePath
import configparser
import glob
import pbgui_help
import multiprocessing
from Clock import Clock
from NeatGrid import NeatGrid
from RecursiveGrid import RecursiveGrid
from OptimizeScore import OptimizeScore

class OptimizeConfig:
    CLIP_THRESHOLD_MIN = 0.0
    CLIP_THRESHOLD_MAX = 100.0
    CLIP_THRESHOLD_STEP = 0.1
    CLIP_THRESHOLD_ROUND = 1
    CLIP_THRESHOLD_FORMAT = f'%.{CLIP_THRESHOLD_ROUND}f'
    N_BACKTEST_SLICES_MIN = 0
    N_BACKTEST_SLICES_MAX = 20
    N_BACKTEST_SLICES_STEP = 1
    N_BACKTEST_SLICES_FORMAT = '%d'

    def __init__(self):
        self._config_file = None
        self._name = None
        self._config = None

        self.passivbot_mode_index = 0
        self.algorithm_index = 0
        self._do_long = True
        self._do_short = False
        self._backwards_tp_long = True
        self._backwards_tp_short = True
        self._passivbot_mode = None
#        self._passivbot_mode = "recursive_grid"
        self._algorithm = "harmony_search"
        self._iters = 4000
        self._n_cpus = multiprocessing.cpu_count()-2

        self.score = OptimizeScore()
        self.rg_long = RecursiveGrid()
        self.rg_short = RecursiveGrid()
        self.ng_long = NeatGrid()
        self.ng_short = NeatGrid()
        self.cl_long = Clock()
        self.cl_short = Clock()

        self._clip_threshold = 0.0
        self._clip_threshold_enable = False
        self._n_backtest_slices = 10
        self._n_backtest_slices_enable = True

        self._n_harmonies = 32
        self._hm_considering_rate = 0.9
        self._bandwidth = 0.07
        self._pitch_adjusting_rate = 0.24
        self._n_particles = 36
        self._w = 0.73
        self._c0 = 1.0
        self._c1 = 0.4

    @property
    def name(self): return self._name
    @name.setter
    def name(self, new_name):
        self._name = new_name
        if self.config_file:
            self._config_file = f'{PurePath(self.config_file).parent}/{new_name}'

    @property
    def config_file(self): return self._config_file
    @config_file.setter
    def config_file(self, new_config_file : str):
        file = Path(new_config_file)
        if file.exists():
            self._config_file = new_config_file
            self._name = PurePath(new_config_file).name

    @property
    def config(self): return self._config
    @config.setter
    def config(self, new_config):
        if self._config != new_config:
            self._config = new_config

    # do_long
    @property
    def do_long(self): return self._do_long
    @do_long.setter
    def do_long(self, new_do_long):
        self._do_long = new_do_long

    # do_short
    @property
    def do_short(self): return self._do_short
    @do_short.setter
    def do_short(self, new_do_short):
        self._do_short = new_do_short

    #backwards_tp_long
    @property
    def backwards_tp_long(self): return self._backwards_tp_long
    @backwards_tp_long.setter
    def backwards_tp_long(self, new_backwards_tp_long):
        self._backwards_tp_long = new_backwards_tp_long

    #backwards_tp_short
    @property
    def backwards_tp_short(self): return self._backwards_tp_short
    @backwards_tp_short.setter
    def backwards_tp_short(self, new_backwards_tp_short):
        self._backwards_tp_short = new_backwards_tp_short

    #passivbot_mode
    @property
    def passivbot_mode(self): return self._passivbot_mode
    @passivbot_mode.setter
    def passivbot_mode(self, new_passivbot_mode):
        self._passivbot_mode = new_passivbot_mode
        if self._passivbot_mode in ["recursive_grid", "r"]:
            self.passivbot_mode_index = 0
        elif self._passivbot_mode in ["neat_grid", "n"]:
            self.passivbot_mode_index = 1
        elif self._passivbot_mode in ["clock", "c"]:
            self.passivbot_mode_index = 2

    #algorithm
    @property
    def algorithm(self): return self._algorithm
    @algorithm.setter
    def algorithm(self, new_algorithm):
        self._algorithm = new_algorithm
        if self._algorithm in ["harmony_search", "hs", "h"]:
            self.algorithm_index = 0
        elif self._algorithm in ["particle_swarm_optimization", "pso", "p"]:
            self.algorithm_index = 1

    # iters
    @property
    def iters(self): return self._iters
    @iters.setter
    def iters(self, new_iters):
        self._iters = new_iters

    # n_cpus
    @property
    def n_cpus(self): return self._n_cpus
    @n_cpus.setter
    def n_cpus(self, new_n_cpus):
        self._n_cpus = new_n_cpus
        if self._n_cpus > multiprocessing.cpu_count():
            self.n_cpus = multiprocessing.cpu_count()

    # clip_threshold
    @property
    def clip_threshold(self): return self._clip_threshold
    @clip_threshold.setter
    def clip_threshold(self, new_clip_threshold):
        self._clip_threshold_enable = True
        self._clip_threshold = new_clip_threshold
        if new_clip_threshold <= 0:
            self._clip_threshold_enable = False
            self._clip_threshold = 0.0
    @property
    def clip_threshold_enable(self): return self._clip_threshold_enable
    @clip_threshold_enable.setter
    def clip_threshold_enable(self, new_clip_threshold_enable):
        self._clip_threshold_enable = new_clip_threshold_enable
        if self._clip_threshold_enable:
            self.clip_threshold = 0.1
        else:
            self.clip_threshold = 0.0

    # n_backtest_slices
    @property
    def n_backtest_slices(self): return self._n_backtest_slices
    @n_backtest_slices.setter
    def n_backtest_slices(self, new_n_backtest_slices):
        if new_n_backtest_slices == 0:
            self._n_backtest_slices_enable = False
            self._n_backtest_slices = 0
        elif new_n_backtest_slices >= 1:
            self._n_backtest_slices_enable = True
            self._n_backtest_slices = new_n_backtest_slices
    @property
    def n_backtest_slices_enable(self): return self._n_backtest_slices_enable
    @n_backtest_slices_enable.setter
    def n_backtest_slices_enable(self, new_n_backtest_slices_enable):
        self._n_backtest_slices_enable = new_n_backtest_slices_enable
        if self._n_backtest_slices_enable:
            self.n_backtest_slices = 10
        else:
            self.n_backtest_slices = 0

    # n_harmonies
    @property
    def n_harmonies(self): return self._n_harmonies
    @n_harmonies.setter
    def n_harmonies(self, new_n_harmonies):
        self._n_harmonies = new_n_harmonies

    # hm_considering_rate
    @property
    def hm_considering_rate(self): return self._hm_considering_rate
    @hm_considering_rate.setter
    def hm_considering_rate(self, new_hm_considering_rate):
        self._hm_considering_rate = new_hm_considering_rate

    # bandwidth
    @property
    def bandwidth(self): return self._bandwidth
    @bandwidth.setter
    def bandwidth(self, new_bandwidth):
        self._bandwidth = new_bandwidth

    # pitch_adjusting_rate
    @property
    def pitch_adjusting_rate(self): return self._pitch_adjusting_rate
    @pitch_adjusting_rate.setter
    def pitch_adjusting_rate(self, new_pitch_adjusting_rate):
        self._pitch_adjusting_rate = new_pitch_adjusting_rate

    # n_particles
    @property
    def n_particles(self): return self._n_particles
    @n_particles.setter
    def n_particles(self, new_n_particles):
        self._n_particles = new_n_particles

    # w
    @property
    def w(self): return self._w
    @w.setter
    def w(self, new_w):
        self._w = new_w

    # c0
    @property
    def c0(self): return self._c0
    @c0.setter
    def c0(self, new_c0):
        self._c0 = new_c0

    # c1
    @property
    def c1(self): return self._c1
    @c1.setter
    def c1(self, new_c1):
        self._c1 = new_c1

    def initialize(self):
        t = hjson.loads(self._config)
        if "bounds_recursive_grid" in t:
            if "long" in t["bounds_recursive_grid"]:
                self.rg_long.config = t["bounds_recursive_grid"]["long"]
            if "short" in t["bounds_recursive_grid"]:
                self.rg_short.config = t["bounds_recursive_grid"]["short"]
        if "bounds_neat_grid" in t:
            if "long" in t["bounds_neat_grid"]:
                self.ng_long.config = t["bounds_neat_grid"]["long"]
            if "short" in t["bounds_neat_grid"]:
                self.ng_short.config = t["bounds_neat_grid"]["short"]
        if "bounds_clock" in t:
            if "long" in t["bounds_clock"]:
                self.cl_long.config = t["bounds_clock"]["long"]
            if "short" in t["bounds_clock"]:
                self.cl_short.config = t["bounds_clock"]["short"]
        if "do_long" in t:
            self._do_long = t["do_long"]
        if "do_short" in t:
            self._do_short = t["do_short"]
        if "backwards_tp_long" in t:
            self._backwards_tp_long = t["backwards_tp_long"]
        if "backwards_tp_short" in t:
            self._backwards_tp_short = t["backwards_tp_short"]
        if "passivbot_mode" in t:
            self._passivbot_mode = t["passivbot_mode"]
        if "algorithm" in t:
            self._algorithm = t["algorithm"]
        if "iters" in t:
            self._iters = t["iters"]
        if "n_cpus" in t:
            self.n_cpus = t["n_cpus"]
        if "clip_threshold" in t:
            self._clip_threshold = t["clip_threshold"]
            if self._clip_threshold < self.CLIP_THRESHOLD_MIN:
                self._clip_threshold = self.CLIP_THRESHOLD_MIN
            if self._clip_threshold > self.CLIP_THRESHOLD_MAX:
                self._clip_threshold = self.CLIP_THRESHOLD_MAX
        if "n_backtest_slices" in t:
            self._n_backtest_slices = t["n_backtest_slices"]
            if self._n_backtest_slices < self.N_BACKTEST_SLICES_MIN:
                self._n_backtest_slices = self.N_BACKTEST_SLICES_MIN
            if self._n_backtest_slices > self.N_BACKTEST_SLICES_MAX:
                self._n_backtest_slices = self.N_BACKTEST_SLICES_MAX
        if "n_harmonies" in t:
            self._n_harmonies = t["n_harmonies"]
        if "hm_considering_rate" in t:
            self._hm_considering_rate = t["hm_considering_rate"]
        if "bandwidth" in t:
            self._bandwidth = t["bandwidth"]
        if "pitch_adjusting_rate" in t:
            self._pitch_adjusting_rate = t["pitch_adjusting_rate"]
        if "n_particles" in t:
            self._n_particles = t["n_particles"]
        if "w" in t:
            self._w = t["w"]
        if "c0" in t:
            self._c0 = t["c0"]
        if "c1" in t:
            self._c1 = t["c1"]
        self.score.config = t

    def remove(self):
        file =  Path(f'{self._config_file}')
        file.unlink(missing_ok=True)

    def load(self):
        file =  Path(f'{self._config_file}')
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                self._config = f.read()
                self.initialize()

    def save(self):
        file =  Path(f'{self._config_file}')
        t = self.score.config
        t["do_long"] = self.do_long
        t["do_short"] = self.do_short
        t["backwards_tp_long"] = self.backwards_tp_long
        t["backwards_tp_short"] = self.backwards_tp_short
        t["passivbot_mode"] = self.passivbot_mode
        t["algorithm"] = self.algorithm
        t["iters"] = self.iters
        t["n_cpus"] = self.n_cpus
        t["clip_threshold"] = self.clip_threshold
        t["n_backtest_slices"] = self.n_backtest_slices
        t["n_harmonies"] = self.n_harmonies
        t["hm_considering_rate"] = self.hm_considering_rate
        t["bandwidth"] = self.bandwidth
        t["pitch_adjusting_rate"] = self.pitch_adjusting_rate
        t["n_particles"] = self.n_particles
        t["w"] = self.w
        t["c0"] = self.c0
        t["c1"] = self.c1
        t["bounds_recursive_grid"]["long"] = self.rg_long.config
        t["bounds_recursive_grid"]["short"] = self.rg_short.config
        t["bounds_neat_grid"]["long"] = self.ng_long.config
        t["bounds_neat_grid"]["short"] = self.ng_short.config
        t["bounds_clock"]["long"] = self.cl_long.config
        t["bounds_clock"]["short"] = self.cl_short.config
        self.config = hjson.dumps(t)
        with open(file, "w", encoding='utf-8') as f:
            f.write(self._config)

    def edit(self):
        if not self.config:
            self.load()
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            if  "config_do_long" in st.session_state:
                self.do_long = st.session_state.config_do_long
            st.toggle("do_long", value=self.do_long, key="config_do_long", help=None)
            if  "config_passivbot_mode" in st.session_state:
                self.passivbot_mode = st.session_state.config_passivbot_mode
            st.radio('passivbot_mode',('recursive_grid', 'neat_grid', 'clock'), index=self.passivbot_mode_index, key="config_passivbot_mode")
        with col2:
            if  "config_do_short" in st.session_state:
                self.do_short = st.session_state.config_do_short
            st.toggle("do_short", value=self.do_short, key="config_do_short", help=None)
            if  "config_algorithm" in st.session_state:
                self.algorithm = st.session_state.config_algorithm
            st.radio("algorithm",('harmony_search', 'particle_swarm_optimization'), index=self.algorithm_index, key="config_algorithm")
        with col3:
            if  "backwards_tp_long" in st.session_state:
                self.backwards_tp_long = st.session_state.backwards_tp_long
            st.toggle("backwards_tp_long", value=self.backwards_tp_long, key="backwards_tp_long", help=None)
            if  "config_iters" in st.session_state:
                self.iters = st.session_state.config_iters
            st.number_input('iters',value=self.iters, step=1000, help=pbgui_help.opt_iters, key="config_iters")
        with col4:
            if  "backwards_tp_short" in st.session_state:
                self.backwards_tp_short = st.session_state.backwards_tp_short
            st.toggle("backwards_tp_short", value=self.backwards_tp_short, key="backwards_tp_short", help=None)
            if  "config_cpu" in st.session_state:
                self.n_cpus = st.session_state.config_cpu
            st.number_input('n_cpus',value=self.n_cpus, min_value=1, max_value=multiprocessing.cpu_count(), step=1, help=None, key="config_cpu")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            if  "config_maximum_drawdown_max_long_enable" in st.session_state:
                if self.score.maximum_drawdown_max_long_enable != st.session_state.config_maximum_drawdown_max_long_enable:
                    self.score.maximum_drawdown_max_long_enable = st.session_state.config_maximum_drawdown_max_long_enable
                    st.session_state.config_maximum_drawdown_max_long = self.score.maximum_drawdown_max_long
                else:
                    self.score.maximum_drawdown_max_long = st.session_state.config_maximum_drawdown_max_long
            st.checkbox("maximum_drawdown_max_long_enable", value=self.score.maximum_drawdown_max_long_enable, key="config_maximum_drawdown_max_long_enable", label_visibility="hidden")
            st.number_input("maximum_drawdown_max_long", min_value=self.score.MAXIMUM_DRAWDOWN_MAX_LONG_MIN, max_value=self.score.MAXIMUM_DRAWDOWN_MAX_LONG_MAX, value=float(round(self.score.maximum_drawdown_max_long,self.score.MAXIMUM_DRAWDOWN_MAX_LONG_ROUND)), step=self.score.MAXIMUM_DRAWDOWN_MAX_LONG_STEP, format=self.score.MAXIMUM_DRAWDOWN_MAX_LONG_FORMAT, disabled=not self.score.maximum_drawdown_max_long_enable, key="config_maximum_drawdown_max_long", help=pbgui_help.score_maximum)
            if  "config_maximum_pa_distance_std_long_enable" in st.session_state:
                if self.score.maximum_pa_distance_std_long_enable != st.session_state.config_maximum_pa_distance_std_long_enable:
                    self.score.maximum_pa_distance_std_long_enable = st.session_state.config_maximum_pa_distance_std_long_enable
                    st.session_state.config_maximum_pa_distance_std_long = self.score.maximum_pa_distance_std_long
                else:
                    self.score.maximum_pa_distance_std_long = st.session_state.config_maximum_pa_distance_std_long
            st.checkbox("maximum_pa_distance_std_long_enable", value=self.score.maximum_pa_distance_std_long_enable, key="config_maximum_pa_distance_std_long_enable", label_visibility="hidden")
            st.number_input("maximum_pa_distance_std_long", min_value=self.score.MAXIMUM_PA_DISTANCE_STD_LONG_MIN, max_value=self.score.MAXIMUM_PA_DISTANCE_STD_LONG_MAX, value=float(round(self.score.maximum_pa_distance_std_long,self.score.MAXIMUM_PA_DISTANCE_STD_LONG_ROUND)), step=self.score.MAXIMUM_PA_DISTANCE_STD_LONG_STEP, format=self.score.MAXIMUM_PA_DISTANCE_STD_LONG_FORMAT, disabled=not self.score.maximum_pa_distance_std_long_enable, key="config_maximum_pa_distance_std_long", help=pbgui_help.score_maximum)
            if  "config_maximum_pa_distance_1pct_worst_mean_long_enable" in st.session_state:
                if self.score.maximum_pa_distance_1pct_worst_mean_long_enable != st.session_state.config_maximum_pa_distance_1pct_worst_mean_long_enable:
                    self.score.maximum_pa_distance_1pct_worst_mean_long_enable = st.session_state.config_maximum_pa_distance_1pct_worst_mean_long_enable
                    st.session_state.config_maximum_pa_distance_1pct_worst_mean_long = self.score.maximum_pa_distance_1pct_worst_mean_long
                else:
                    self.score.maximum_pa_distance_1pct_worst_mean_long = st.session_state.config_maximum_pa_distance_1pct_worst_mean_long
            st.checkbox("maximum_pa_distance_1pct_worst_mean_long_enable", value=self.score.maximum_pa_distance_1pct_worst_mean_long_enable, key="config_maximum_pa_distance_1pct_worst_mean_long_enable", label_visibility="hidden")
            st.number_input("maximum_pa_distance_1pct_worst_mean_long", min_value=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MIN, max_value=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_MAX, value=float(round(self.score.maximum_pa_distance_1pct_worst_mean_long,self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_ROUND)), step=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_STEP, format=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_LONG_FORMAT, disabled=not self.score.maximum_pa_distance_1pct_worst_mean_long_enable, key="config_maximum_pa_distance_1pct_worst_mean_long", help=pbgui_help.score_maximum)
            if  "config_maximum_hrs_stuck_max_long_enable" in st.session_state:
                if self.score.maximum_hrs_stuck_max_long_enable != st.session_state.config_maximum_hrs_stuck_max_long_enable:
                    self.score.maximum_hrs_stuck_max_long_enable = st.session_state.config_maximum_hrs_stuck_max_long_enable
                    st.session_state.config_maximum_hrs_stuck_max_long = self.score.maximum_hrs_stuck_max_long
                else:
                    self.score.maximum_hrs_stuck_max_long = st.session_state.config_maximum_hrs_stuck_max_long
            st.checkbox("maximum_hrs_stuck_max_long_enable", value=self.score.maximum_hrs_stuck_max_long_enable, key="config_maximum_hrs_stuck_max_long_enable", label_visibility="hidden")
            st.number_input("maximum_hrs_stuck_max_long", min_value=self.score.MAXIMUM_HRS_STUCK_MAX_LONG_MIN, max_value=self.score.MAXIMUM_HRS_STUCK_MAX_LONG_MAX, value=float(round(self.score.maximum_hrs_stuck_max_long,self.score.MAXIMUM_HRS_STUCK_MAX_LONG_ROUND)), step=self.score.MAXIMUM_HRS_STUCK_MAX_LONG_STEP, format=self.score.MAXIMUM_HRS_STUCK_MAX_LONG_FORMAT, disabled=not self.score.maximum_hrs_stuck_max_long_enable, key="config_maximum_hrs_stuck_max_long", help=pbgui_help.score_maximum)
            if  "config_maximum_time_at_max_exposure_long_enable" in st.session_state:
                if self.score.maximum_time_at_max_exposure_long_enable != st.session_state.config_maximum_time_at_max_exposure_long_enable:
                    self.score.maximum_time_at_max_exposure_long_enable = st.session_state.config_maximum_time_at_max_exposure_long_enable
                    st.session_state.config_maximum_time_at_max_exposure_long = self.score.maximum_time_at_max_exposure_long
                else:
                    self.score.maximum_time_at_max_exposure_long = st.session_state.config_maximum_time_at_max_exposure_long
            st.checkbox("maximum_time_at_max_exposure_long_enable", value=self.score.maximum_time_at_max_exposure_long_enable, key="config_maximum_time_at_max_exposure_long_enable", label_visibility="hidden")
            st.number_input("maximum_time_at_max_exposure_long", min_value=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MIN, max_value=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_MAX, value=float(round(self.score.maximum_time_at_max_exposure_long,self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_ROUND)), step=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_STEP, format=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_LONG_FORMAT, disabled=not self.score.maximum_time_at_max_exposure_long_enable, key="config_maximum_time_at_max_exposure_long", help=pbgui_help.score_maximum)
        with col2:
            if  "config_maximum_drawdown_max_short_enable" in st.session_state:
                if self.score.maximum_drawdown_max_short_enable != st.session_state.config_maximum_drawdown_max_short_enable:
                    self.score.maximum_drawdown_max_short_enable = st.session_state.config_maximum_drawdown_max_short_enable
                    st.session_state.config_maximum_drawdown_max_short = self.score.maximum_drawdown_max_short
                else:
                    self.score.maximum_drawdown_max_short = st.session_state.config_maximum_drawdown_max_short
            st.checkbox("maximum_drawdown_max_short_enable", value=self.score.maximum_drawdown_max_short_enable, key="config_maximum_drawdown_max_short_enable", label_visibility="hidden")
            st.number_input("maximum_drawdown_max_short", min_value=self.score.MAXIMUM_DRAWDOWN_MAX_SHORT_MIN, max_value=self.score.MAXIMUM_DRAWDOWN_MAX_SHORT_MAX, value=float(round(self.score.maximum_drawdown_max_short,self.score.MAXIMUM_DRAWDOWN_MAX_SHORT_ROUND)), step=self.score.MAXIMUM_DRAWDOWN_MAX_SHORT_STEP, format=self.score.MAXIMUM_DRAWDOWN_MAX_SHORT_FORMAT, disabled=not self.score.maximum_drawdown_max_short_enable, key="config_maximum_drawdown_max_short", help=pbgui_help.score_maximum)
            if  "config_maximum_pa_distance_std_short_enable" in st.session_state:
                if self.score.maximum_pa_distance_std_short_enable != st.session_state.config_maximum_pa_distance_std_short_enable:
                    self.score.maximum_pa_distance_std_short_enable = st.session_state.config_maximum_pa_distance_std_short_enable
                    st.session_state.config_maximum_pa_distance_std_short = self.score.maximum_pa_distance_std_short
                else:
                    self.score.maximum_pa_distance_std_short = st.session_state.config_maximum_pa_distance_std_short
            st.checkbox("maximum_pa_distance_std_short_enable", value=self.score.maximum_pa_distance_std_short_enable, key="config_maximum_pa_distance_std_short_enable", label_visibility="hidden")
            st.number_input("maximum_pa_distance_std_short", min_value=self.score.MAXIMUM_PA_DISTANCE_STD_SHORT_MIN, max_value=self.score.MAXIMUM_PA_DISTANCE_STD_SHORT_MAX, value=float(round(self.score.maximum_pa_distance_std_short,self.score.MAXIMUM_PA_DISTANCE_STD_SHORT_ROUND)), step=self.score.MAXIMUM_PA_DISTANCE_STD_SHORT_STEP, format=self.score.MAXIMUM_PA_DISTANCE_STD_SHORT_FORMAT, disabled=not self.score.maximum_pa_distance_std_short_enable, key="config_maximum_pa_distance_std_short", help=pbgui_help.score_maximum)
            if  "config_maximum_pa_distance_1pct_worst_mean_short_enable" in st.session_state:
                if self.score.maximum_pa_distance_1pct_worst_mean_short_enable != st.session_state.config_maximum_pa_distance_1pct_worst_mean_short_enable:
                    self.score.maximum_pa_distance_1pct_worst_mean_short_enable = st.session_state.config_maximum_pa_distance_1pct_worst_mean_short_enable
                    st.session_state.config_maximum_pa_distance_1pct_worst_mean_short = self.score.maximum_pa_distance_1pct_worst_mean_short
                else:
                    self.score.maximum_pa_distance_1pct_worst_mean_short = st.session_state.config_maximum_pa_distance_1pct_worst_mean_short
            st.checkbox("maximum_pa_distance_1pct_worst_mean_short_enable", value=self.score.maximum_pa_distance_1pct_worst_mean_short_enable, key="config_maximum_pa_distance_1pct_worst_mean_short_enable", label_visibility="hidden")
            st.number_input("maximum_pa_distance_1pct_worst_mean_short", min_value=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MIN, max_value=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_MAX, value=float(round(self.score.maximum_pa_distance_1pct_worst_mean_short,self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_ROUND)), step=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_STEP, format=self.score.MAXIMUM_PA_DISTANCE_1PCT_WORST_MEAN_SHORT_FORMAT, disabled=not self.score.maximum_pa_distance_1pct_worst_mean_short_enable, key="config_maximum_pa_distance_1pct_worst_mean_short", help=pbgui_help.score_maximum)
            if  "config_maximum_hrs_stuck_max_short_enable" in st.session_state:
                if self.score.maximum_hrs_stuck_max_short_enable != st.session_state.config_maximum_hrs_stuck_max_short_enable:
                    self.score.maximum_hrs_stuck_max_short_enable = st.session_state.config_maximum_hrs_stuck_max_short_enable
                    st.session_state.config_maximum_hrs_stuck_max_short = self.score.maximum_hrs_stuck_max_short
                else:
                    self.score.maximum_hrs_stuck_max_short = st.session_state.config_maximum_hrs_stuck_max_short
            st.checkbox("maximum_hrs_stuck_max_short_enable", value=self.score.maximum_hrs_stuck_max_short_enable, key="config_maximum_hrs_stuck_max_short_enable", label_visibility="hidden")
            st.number_input("maximum_hrs_stuck_max_short", min_value=self.score.MAXIMUM_HRS_STUCK_MAX_SHORT_MIN, max_value=self.score.MAXIMUM_HRS_STUCK_MAX_SHORT_MAX, value=float(round(self.score.maximum_hrs_stuck_max_short,self.score.MAXIMUM_HRS_STUCK_MAX_SHORT_ROUND)), step=self.score.MAXIMUM_HRS_STUCK_MAX_SHORT_STEP, format=self.score.MAXIMUM_HRS_STUCK_MAX_SHORT_FORMAT, disabled=not self.score.maximum_hrs_stuck_max_short_enable, key="config_maximum_hrs_stuck_max_short", help=pbgui_help.score_maximum)
            if  "config_maximum_time_at_max_exposure_short_enable" in st.session_state:
                if self.score.maximum_time_at_max_exposure_short_enable != st.session_state.config_maximum_time_at_max_exposure_short_enable:
                    self.score.maximum_time_at_max_exposure_short_enable = st.session_state.config_maximum_time_at_max_exposure_short_enable
                    st.session_state.config_maximum_time_at_max_exposure_short = self.score.maximum_time_at_max_exposure_short
                else:
                    self.score.maximum_time_at_max_exposure_short = st.session_state.config_maximum_time_at_max_exposure_short
            st.checkbox("maximum_time_at_max_exposure_short_enable", value=self.score.maximum_time_at_max_exposure_short_enable, key="config_maximum_time_at_max_exposure_short_enable", label_visibility="hidden")
            st.number_input("maximum_time_at_max_exposure_short", min_value=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MIN, max_value=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_MAX, value=float(round(self.score.maximum_time_at_max_exposure_short,self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_ROUND)), step=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_STEP, format=self.score.MAXIMUM_TIME_AT_MAX_EXPOSURE_SHORT_FORMAT, disabled=not self.score.maximum_time_at_max_exposure_short_enable, key="config_maximum_time_at_max_exposure_short", help=pbgui_help.score_maximum)
        with col3:
            if  "config_maximum_drawdown_1pct_worst_mean_long_enable" in st.session_state:
                if self.score.maximum_drawdown_1pct_worst_mean_long_enable != st.session_state.config_maximum_drawdown_1pct_worst_mean_long_enable:
                    self.score.maximum_drawdown_1pct_worst_mean_long_enable = st.session_state.config_maximum_drawdown_1pct_worst_mean_long_enable
                    st.session_state.config_maximum_drawdown_1pct_worst_mean_long = self.score.maximum_drawdown_1pct_worst_mean_long
                else:
                    self.score.maximum_drawdown_1pct_worst_mean_long = st.session_state.config_maximum_drawdown_1pct_worst_mean_long
            st.checkbox("maximum_drawdown_1pct_worst_mean_long_enable", value=self.score.maximum_drawdown_1pct_worst_mean_long_enable, key="config_maximum_drawdown_1pct_worst_mean_long_enable", label_visibility="hidden")
            st.number_input("maximum_drawdown_1pct_worst_mean_long", min_value=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MIN, max_value=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_MAX, value=float(round(self.score.maximum_drawdown_1pct_worst_mean_long,self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_ROUND)), step=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_STEP, format=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_LONG_FORMAT, disabled=not self.score.maximum_drawdown_1pct_worst_mean_long_enable, key="config_maximum_drawdown_1pct_worst_mean_long", help=pbgui_help.score_maximum)
            if  "config_maximum_pa_distance_mean_long_enable" in st.session_state:
                if self.score.maximum_pa_distance_mean_long_enable != st.session_state.config_maximum_pa_distance_mean_long_enable:
                    self.score.maximum_pa_distance_mean_long_enable = st.session_state.config_maximum_pa_distance_mean_long_enable
                    st.session_state.config_maximum_pa_distance_mean_long = self.score.maximum_pa_distance_mean_long
                else:
                    self.score.maximum_pa_distance_mean_long = st.session_state.config_maximum_pa_distance_mean_long
            st.checkbox("maximum_pa_distance_mean_long_enable", value=self.score.maximum_pa_distance_mean_long_enable, key="config_maximum_pa_distance_mean_long_enable", label_visibility="hidden")
            st.number_input("maximum_pa_distance_mean_long", min_value=self.score.MAXIMUM_PA_DISTANCE_MEAN_LONG_MIN, max_value=self.score.MAXIMUM_PA_DISTANCE_MEAN_LONG_MAX, value=float(round(self.score.maximum_pa_distance_mean_long,self.score.MAXIMUM_PA_DISTANCE_MEAN_LONG_ROUND)), step=self.score.MAXIMUM_PA_DISTANCE_MEAN_LONG_STEP, format=self.score.MAXIMUM_PA_DISTANCE_MEAN_LONG_FORMAT, disabled=not self.score.maximum_pa_distance_mean_long_enable, key="config_maximum_pa_distance_mean_long", help=pbgui_help.score_maximum)
            if  "config_maximum_loss_profit_ratio_long_enable" in st.session_state:
                if self.score.maximum_loss_profit_ratio_long_enable != st.session_state.config_maximum_loss_profit_ratio_long_enable:
                    self.score.maximum_loss_profit_ratio_long_enable = st.session_state.config_maximum_loss_profit_ratio_long_enable
                    st.session_state.config_maximum_loss_profit_ratio_long = self.score.maximum_loss_profit_ratio_long
                else:
                    self.score.maximum_loss_profit_ratio_long = st.session_state.config_maximum_loss_profit_ratio_long
            st.checkbox("maximum_loss_profit_ratio_long_enable", value=self.score.maximum_loss_profit_ratio_long_enable, key="config_maximum_loss_profit_ratio_long_enable", label_visibility="hidden")
            st.number_input("maximum_loss_profit_ratio_long", min_value=self.score.MAXIMUM_LOSS_PROFIT_RATIO_LONG_MIN, max_value=self.score.MAXIMUM_LOSS_PROFIT_RATIO_LONG_MAX, value=float(round(self.score.maximum_loss_profit_ratio_long,self.score.MAXIMUM_LOSS_PROFIT_RATIO_LONG_ROUND)), step=self.score.MAXIMUM_LOSS_PROFIT_RATIO_LONG_STEP, format=self.score.MAXIMUM_LOSS_PROFIT_RATIO_LONG_FORMAT, disabled=not self.score.maximum_loss_profit_ratio_long_enable, key="config_maximum_loss_profit_ratio_long", help=pbgui_help.score_maximum)
            if  "config_maximum_exposure_ratios_mean_long_enable" in st.session_state:
                if self.score.maximum_exposure_ratios_mean_long_enable != st.session_state.config_maximum_exposure_ratios_mean_long_enable:
                    self.score.maximum_exposure_ratios_mean_long_enable = st.session_state.config_maximum_exposure_ratios_mean_long_enable
                    st.session_state.config_maximum_exposure_ratios_mean_long = self.score.maximum_exposure_ratios_mean_long
                else:
                    self.score.maximum_exposure_ratios_mean_long = st.session_state.config_maximum_exposure_ratios_mean_long
            st.checkbox("maximum_exposure_ratios_mean_long_enable", value=self.score.maximum_exposure_ratios_mean_long_enable, key="config_maximum_exposure_ratios_mean_long_enable", label_visibility="hidden")
            st.number_input("maximum_exposure_ratios_mean_long", min_value=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MIN, max_value=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_MAX, value=float(round(self.score.maximum_exposure_ratios_mean_long,self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_ROUND)), step=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_STEP, format=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_LONG_FORMAT, disabled=not self.score.maximum_exposure_ratios_mean_long_enable, key="config_maximum_exposure_ratios_mean_long", help=pbgui_help.score_maximum)
            if  "config_clip_threshold_enable" in st.session_state:
                if self.clip_threshold_enable != st.session_state.config_clip_threshold_enable:
                    self.clip_threshold_enable = st.session_state.config_clip_threshold_enable
                    st.session_state.config_clip_threshold = self.clip_threshold
                else:
                    self.clip_threshold = st.session_state.config_clip_threshold
            st.checkbox("clip_threshold_enable", value=self.clip_threshold_enable, key="config_clip_threshold_enable", label_visibility="hidden")
            st.number_input("clip_threshold", min_value=self.CLIP_THRESHOLD_MIN, max_value=self.CLIP_THRESHOLD_MAX, value=float(round(self.clip_threshold,self.CLIP_THRESHOLD_ROUND)), step=self.CLIP_THRESHOLD_STEP, format=self.CLIP_THRESHOLD_FORMAT, disabled=not self.clip_threshold_enable, key="config_clip_threshold", help=pbgui_help.clip_threshold)
        with col4:
            if  "config_maximum_drawdown_1pct_worst_mean_short_enable" in st.session_state:
                if self.score.maximum_drawdown_1pct_worst_mean_short_enable != st.session_state.config_maximum_drawdown_1pct_worst_mean_short_enable:
                    self.score.maximum_drawdown_1pct_worst_mean_short_enable = st.session_state.config_maximum_drawdown_1pct_worst_mean_short_enable
                    st.session_state.config_maximum_drawdown_1pct_worst_mean_short = self.score.maximum_drawdown_1pct_worst_mean_short
                else:
                    self.score.maximum_drawdown_1pct_worst_mean_short = st.session_state.config_maximum_drawdown_1pct_worst_mean_short
            st.checkbox("maximum_drawdown_1pct_worst_mean_short_enable", value=self.score.maximum_drawdown_1pct_worst_mean_short_enable, key="config_maximum_drawdown_1pct_worst_mean_short_enable", label_visibility="hidden")
            st.number_input("maximum_drawdown_1pct_worst_mean_short", min_value=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MIN, max_value=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_MAX, value=float(round(self.score.maximum_drawdown_1pct_worst_mean_short,self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_ROUND)), step=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_STEP, format=self.score.MAXIMUM_DRAWDOWN_1PCT_WORST_MEAN_SHORT_FORMAT, disabled=not self.score.maximum_drawdown_1pct_worst_mean_short_enable, key="config_maximum_drawdown_1pct_worst_mean_short", help=pbgui_help.score_maximum)
            if  "config_maximum_pa_distance_mean_short_enable" in st.session_state:
                if self.score.maximum_pa_distance_mean_short_enable != st.session_state.config_maximum_pa_distance_mean_short_enable:
                    self.score.maximum_pa_distance_mean_short_enable = st.session_state.config_maximum_pa_distance_mean_short_enable
                    st.session_state.config_maximum_pa_distance_mean_short = self.score.maximum_pa_distance_mean_short
                else:
                    self.score.maximum_pa_distance_mean_short = st.session_state.config_maximum_pa_distance_mean_short
            st.checkbox("maximum_pa_distance_mean_short_enable", value=self.score.maximum_pa_distance_mean_short_enable, key="config_maximum_pa_distance_mean_short_enable", label_visibility="hidden")
            st.number_input("maximum_pa_distance_mean_short", min_value=self.score.MAXIMUM_PA_DISTANCE_MEAN_SHORT_MIN, max_value=self.score.MAXIMUM_PA_DISTANCE_MEAN_SHORT_MAX, value=float(round(self.score.maximum_pa_distance_mean_short,self.score.MAXIMUM_PA_DISTANCE_MEAN_SHORT_ROUND)), step=self.score.MAXIMUM_PA_DISTANCE_MEAN_SHORT_STEP, format=self.score.MAXIMUM_PA_DISTANCE_MEAN_SHORT_FORMAT, disabled=not self.score.maximum_pa_distance_mean_short_enable, key="config_maximum_pa_distance_mean_short", help=pbgui_help.score_maximum)
            if  "config_maximum_loss_profit_ratio_short_enable" in st.session_state:
                if self.score.maximum_loss_profit_ratio_short_enable != st.session_state.config_maximum_loss_profit_ratio_short_enable:
                    self.score.maximum_loss_profit_ratio_short_enable = st.session_state.config_maximum_loss_profit_ratio_short_enable
                    st.session_state.config_maximum_loss_profit_ratio_short = self.score.maximum_loss_profit_ratio_short
                else:
                    self.score.maximum_loss_profit_ratio_short = st.session_state.config_maximum_loss_profit_ratio_short
            st.checkbox("maximum_loss_profit_ratio_short_enable", value=self.score.maximum_loss_profit_ratio_short_enable, key="config_maximum_loss_profit_ratio_short_enable", label_visibility="hidden")
            st.number_input("maximum_loss_profit_ratio_short", min_value=self.score.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MIN, max_value=self.score.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_MAX, value=float(round(self.score.maximum_loss_profit_ratio_short,self.score.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_ROUND)), step=self.score.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_STEP, format=self.score.MAXIMUM_LOSS_PROFIT_RATIO_SHORT_FORMAT, disabled=not self.score.maximum_loss_profit_ratio_short_enable, key="config_maximum_loss_profit_ratio_short", help=pbgui_help.score_maximum)
            if  "config_maximum_exposure_ratios_mean_short_enable" in st.session_state:
                if self.score.maximum_exposure_ratios_mean_short_enable != st.session_state.config_maximum_exposure_ratios_mean_short_enable:
                    self.score.maximum_exposure_ratios_mean_short_enable = st.session_state.config_maximum_exposure_ratios_mean_short_enable
                    st.session_state.config_maximum_exposure_ratios_mean_short = self.score.maximum_exposure_ratios_mean_short
                else:
                    self.score.maximum_exposure_ratios_mean_short = st.session_state.config_maximum_exposure_ratios_mean_short
            st.checkbox("maximum_exposure_ratios_mean_short_enable", value=self.score.maximum_exposure_ratios_mean_short_enable, key="config_maximum_exposure_ratios_mean_short_enable", label_visibility="hidden")
            st.number_input("maximum_exposure_ratios_mean_short", min_value=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MIN, max_value=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_MAX, value=float(round(self.score.maximum_exposure_ratios_mean_short,self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_ROUND)), step=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_STEP, format=self.score.MAXIMUM_EXPOSURE_RATIOS_MEAN_SHORT_FORMAT, disabled=not self.score.maximum_exposure_ratios_mean_short_enable, key="config_maximum_exposure_ratios_mean_short", help=pbgui_help.score_maximum)
            if  "config_n_backtest_slices_enable" in st.session_state:
                if self.n_backtest_slices_enable != st.session_state.config_n_backtest_slices_enable:
                    self.n_backtest_slices_enable = st.session_state.config_n_backtest_slices_enable
                    st.session_state.config_n_backtest_slices = self.n_backtest_slices
                else:
                    self.n_backtest_slices = st.session_state.config_n_backtest_slices
            st.checkbox("n_backtest_slices", value=self.n_backtest_slices_enable, key="config_n_backtest_slices_enable", label_visibility="hidden")
            st.number_input("n_backtest_slices", min_value=self.N_BACKTEST_SLICES_MIN, max_value=self.N_BACKTEST_SLICES_MAX, value=self.n_backtest_slices, step=self.N_BACKTEST_SLICES_STEP, format=self.N_BACKTEST_SLICES_FORMAT, disabled=not self.n_backtest_slices, key="config_n_backtest_slices", help=pbgui_help.backtest_slices)
        with st.expander("recursive grid", expanded = True if self.passivbot_mode == "recursive_grid" else False):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                if  "config_rg_long_ema_span_0_0" in st.session_state:
                    self.rg_long.ema_span_0_0 = st.session_state.config_rg_long_ema_span_0_0
                st.number_input("long_ema_span_0 min", min_value=self.rg_long.EMA_SPAN_0_MIN, max_value=self.rg_long.ema_span_0_1, value=float(round(self.rg_long.ema_span_0_0,self.rg_long.EMA_SPAN_0_ROUND)), step=self.rg_long.EMA_SPAN_0_STEP, format=self.rg_long.EMA_SPAN_0_FORMAT, key="config_rg_long_ema_span_0_0", help=pbgui_help.ema_span)
                if  "config_rg_long_ema_span_1_0" in st.session_state:
                    self.rg_long.ema_span_1_0 = st.session_state.config_rg_long_ema_span_1_0
                st.number_input("long_ema_span_1 min", min_value=self.rg_long.EMA_SPAN_1_MIN, max_value=self.rg_long.ema_span_1_1, value=float(round(self.rg_long.ema_span_1_0,self.rg_long.EMA_SPAN_1_ROUND)), step=self.rg_long.EMA_SPAN_1_STEP, format=self.rg_long.EMA_SPAN_1_FORMAT, key="config_rg_long_ema_span_1_0", help=pbgui_help.ema_span)
                if  "config_rg_long_initial_qty_pct_0" in st.session_state:
                    self.rg_long.initial_qty_pct_0 = st.session_state.config_rg_long_initial_qty_pct_0
                st.number_input("long_initial_qty_pct min", min_value=self.rg_long.INITIAL_QTY_PCT_MIN, max_value=self.rg_long.initial_qty_pct_1, value=float(round(self.rg_long.initial_qty_pct_0,self.rg_long.INITIAL_QTY_PCT_ROUND)), step=self.rg_long.INITIAL_QTY_PCT_STEP, format=self.rg_long.INITIAL_QTY_PCT_FORMAT, key="config_rg_long_initial_qty_pct_0", help=pbgui_help.initial_qty_pct)
                if  "config_rg_long_initial_eprice_ema_dist_0" in st.session_state:
                    self.rg_long.initial_eprice_ema_dist_0 = st.session_state.config_rg_long_initial_eprice_ema_dist_0
                st.number_input("long_initial_eprice_ema_dist min", min_value=self.rg_long.INITIAL_EPRICE_EMA_DIST_MIN, max_value=self.rg_long.initial_eprice_ema_dist_1, value=float(round(self.rg_long.initial_eprice_ema_dist_0,self.rg_long.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.rg_long.INITIAL_EPRICE_EMA_DIST_STEP, format=self.rg_long.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_rg_long_initial_eprice_ema_dist_0", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_rg_long_wallet_exposure_limit_0" in st.session_state:
                    self.rg_long.wallet_exposure_limit_0 = st.session_state.config_rg_long_wallet_exposure_limit_0
                st.number_input("long_wallet_exposure_limit min", min_value=self.rg_long.WALLET_EXPOSURE_LIMIT_MIN, max_value=self.rg_long.wallet_exposure_limit_1, value=float(round(self.rg_long.wallet_exposure_limit_0,self.rg_long.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.rg_long.WALLET_EXPOSURE_LIMIT_STEP, format=self.rg_long.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_rg_long_wallet_exposure_limit_0", help=pbgui_help.wallet_exposure_limit)
                if  "config_rg_long_ddown_factor_0" in st.session_state:
                    self.rg_long.ddown_factor_0 = st.session_state.config_rg_long_ddown_factor_0
                st.number_input("long_ddown_factor min", min_value=self.rg_long.DDOWN_FACTOR_MIN, max_value=self.rg_long.ddown_factor_1, value=float(round(self.rg_long.ddown_factor_0,self.rg_long.DDOWN_FACTOR_ROUND)), step=self.rg_long.DDOWN_FACTOR_STEP, format=self.rg_long.DDOWN_FACTOR_FORMAT, key="config_rg_long_ddown_factor_0", help=pbgui_help.ddown_factor)
                if  "config_rg_long_rentry_pprice_dist_0" in st.session_state:
                    self.rg_long.rentry_pprice_dist_0 = st.session_state.config_rg_long_rentry_pprice_dist_0
                st.number_input("long_rentry_pprice_dist min", min_value=self.rg_long.RENTRY_PPRICE_DIST_MIN, max_value=self.rg_long.rentry_pprice_dist_1, value=float(round(self.rg_long.rentry_pprice_dist_0,self.rg_long.RENTRY_PPRICE_DIST_ROUND)), step=self.rg_long.RENTRY_PPRICE_DIST_STEP, format=self.rg_long.RENTRY_PPRICE_DIST_FORMAT, key="config_rg_long_rentry_pprice_dist_0", help=pbgui_help.rentry_pprice_dist)
                if  "config_rg_long_rentry_pprice_dist_wallet_exposure_weighting_0" in st.session_state:
                    self.rg_long.rentry_pprice_dist_wallet_exposure_weighting_0 = st.session_state.config_rg_long_rentry_pprice_dist_wallet_exposure_weighting_0
                st.number_input("long_rentry_pprice_dist_wallet_exposure_weighting min", min_value=self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN, max_value=self.rg_long.rentry_pprice_dist_wallet_exposure_weighting_1, value=float(round(self.rg_long.rentry_pprice_dist_wallet_exposure_weighting_0,self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="config_rg_long_rentry_pprice_dist_wallet_exposure_weighting_0", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
                if  "config_rg_long_min_markup_0" in st.session_state:
                    self.rg_long.min_markup_0 = st.session_state.config_rg_long_min_markup_0
                st.number_input("long_min_markup min", min_value=self.rg_long.MIN_MARKUP_MIN, max_value=self.rg_long.min_markup_1, value=float(round(self.rg_long.min_markup_0,self.rg_long.MIN_MARKUP_ROUND)), step=self.rg_long.MIN_MARKUP_STEP, format=self.rg_long.MIN_MARKUP_FORMAT, key="config_rg_long_min_markup_0", help=pbgui_help.min_markup)
                if  "config_rg_long_markup_range_0" in st.session_state:
                    self.rg_long.markup_range_0 = st.session_state.config_rg_long_markup_range_0
                st.number_input("long_markup_range min", min_value=self.rg_long.MARKUP_RANGE_MIN, max_value=self.rg_long.markup_range_1, value=float(round(self.rg_long.markup_range_0,self.rg_long.MARKUP_RANGE_ROUND)), step=self.rg_long.MARKUP_RANGE_STEP, format=self.rg_long.MARKUP_RANGE_FORMAT, key="config_rg_long_markup_range_0", help=pbgui_help.markup_range)
                if  "config_rg_long_n_close_orders_0" in st.session_state:
                    self.rg_long.n_close_orders_0 = st.session_state.config_rg_long_n_close_orders_0
                st.number_input("long_n_close_orders min", min_value=self.rg_long.N_CLOSE_ORDERS_MIN, max_value=self.rg_long.n_close_orders_1, value=self.rg_long.n_close_orders_0, step=self.rg_long.N_CLOSE_ORDERS_STEP, format=self.rg_long.N_CLOSE_ORDERS_FORMAT, key="config_rg_long_n_close_orders_0", help=pbgui_help.n_close_orders)
                if  "config_rg_long_auto_unstuck_wallet_exposure_threshold_0" in st.session_state:
                    self.rg_long.auto_unstuck_wallet_exposure_threshold_0 = st.session_state.config_rg_long_auto_unstuck_wallet_exposure_threshold_0
                st.number_input("long_auto_unstuck_wallet_exposure_threshold min", min_value=self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN, max_value=self.rg_long.auto_unstuck_wallet_exposure_threshold_1, value=float(round(self.rg_long.auto_unstuck_wallet_exposure_threshold_0,self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_rg_long_auto_unstuck_wallet_exposure_threshold_0", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_rg_long_auto_unstuck_ema_dist_0" in st.session_state:
                    self.rg_long.auto_unstuck_ema_dist_0 = st.session_state.config_rg_long_auto_unstuck_ema_dist_0
                self.rg_long.auto_unstuck_ema_dist_0 = st.number_input("long_auto_unstuck_ema_dist min", min_value=self.rg_long.AUTO_UNSTUCK_EMA_DIST_MIN, max_value=self.rg_long.auto_unstuck_ema_dist_1, value=float(round(self.rg_long.auto_unstuck_ema_dist_0,self.rg_long.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.rg_long.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.rg_long.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_rg_long_auto_unstuck_ema_dist_0", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_rg_long_auto_unstuck_delay_minutes_0" in st.session_state:
                    self.rg_long.auto_unstuck_delay_minutes_0 = st.session_state.config_rg_long_auto_unstuck_delay_minutes_0
                self.rg_long.auto_unstuck_delay_minutes_0 = st.number_input("long_auto_unstuck_delay_minutes min", min_value=self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_MIN, max_value=self.rg_long.auto_unstuck_delay_minutes_1, value=float(round(self.rg_long.auto_unstuck_delay_minutes_0,self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_rg_long_auto_unstuck_delay_minutes_0", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_rg_long_auto_unstuck_qty_pct_0" in st.session_state:
                    self.rg_long.auto_unstuck_qty_pct_0 = st.session_state.config_rg_long_auto_unstuck_qty_pct_0
                st.number_input("long_auto_unstuck_qty_pct min", min_value=self.rg_long.AUTO_UNSTUCK_QTY_PCT_MIN, max_value=self.rg_long.auto_unstuck_qty_pct_1, value=float(round(self.rg_long.auto_unstuck_qty_pct_0,self.rg_long.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.rg_long.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.rg_long.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_rg_long_auto_unstuck_qty_pct_0", help=pbgui_help.auto_unstuck_qty_pct)
            with col2:
                if  "config_rg_long_ema_span_0_1" in st.session_state:
                    self.rg_long.ema_span_0_1 = st.session_state.config_rg_long_ema_span_0_1
                st.number_input("long_ema_span_0 max", min_value=self.rg_long.ema_span_0_0, max_value=self.rg_long.EMA_SPAN_0_MAX, value=float(round(self.rg_long.ema_span_0_1,self.rg_long.EMA_SPAN_0_ROUND)), step=self.rg_long.EMA_SPAN_0_STEP, format=self.rg_long.EMA_SPAN_0_FORMAT, key="config_rg_long_ema_span_0_1", help=pbgui_help.ema_span)
                if  "config_rg_long_ema_span_1_1" in st.session_state:
                    self.rg_long.ema_span_1_1 = st.session_state.config_rg_long_ema_span_1_1
                st.number_input("long_ema_span_0 max", min_value=self.rg_long.ema_span_1_0, max_value=self.rg_long.EMA_SPAN_1_MAX, value=float(round(self.rg_long.ema_span_1_1,self.rg_long.EMA_SPAN_1_ROUND)), step=self.rg_long.EMA_SPAN_1_STEP, format=self.rg_long.EMA_SPAN_1_FORMAT, key="config_rg_long_ema_span_1_1", help=pbgui_help.ema_span)
                if  "config_rg_long_initial_qty_pct_1" in st.session_state:
                    self.rg_long.initial_qty_pct_1 = st.session_state.config_rg_long_initial_qty_pct_1
                st.number_input("long_initial_qty_pct max", min_value=self.rg_long.initial_qty_pct_0, max_value=self.rg_long.INITIAL_QTY_PCT_MAX, value=float(round(self.rg_long.initial_qty_pct_1,self.rg_long.INITIAL_QTY_PCT_ROUND)), step=self.rg_long.INITIAL_QTY_PCT_STEP, format=self.rg_long.INITIAL_QTY_PCT_FORMAT, key="config_rg_long_initial_qty_pct_1", help=pbgui_help.initial_qty_pct)
                if  "config_rg_long_initial_eprice_ema_dist_1" in st.session_state:
                    self.rg_long.initial_eprice_ema_dist_1 = st.session_state.config_rg_long_initial_eprice_ema_dist_1
                st.number_input("long_initial_eprice_ema_dist max", min_value=self.rg_long.initial_eprice_ema_dist_0, max_value=self.rg_long.INITIAL_EPRICE_EMA_DIST_MAX, value=float(round(self.rg_long.initial_eprice_ema_dist_1,self.rg_long.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.rg_long.INITIAL_EPRICE_EMA_DIST_STEP, format=self.rg_long.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_rg_long_initial_eprice_ema_dist_1", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_rg_long_wallet_exposure_limit_1" in st.session_state:
                    self.rg_long.wallet_exposure_limit_1 = st.session_state.config_rg_long_wallet_exposure_limit_1
                st.number_input("long_wallet_exposure_limit max", min_value=self.rg_long.wallet_exposure_limit_0, max_value=self.rg_long.WALLET_EXPOSURE_LIMIT_MAX, value=float(round(self.rg_long.wallet_exposure_limit_1,self.rg_long.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.rg_long.WALLET_EXPOSURE_LIMIT_STEP, format=self.rg_long.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_rg_long_wallet_exposure_limit_1", help=pbgui_help.wallet_exposure_limit)
                if  "config_rg_long_ddown_factor_1" in st.session_state:
                    self.rg_long.ddown_factor_1 = st.session_state.config_rg_long_ddown_factor_1
                st.number_input("long_ddown_factor max", min_value=self.rg_long.ddown_factor_0, max_value=self.rg_long.DDOWN_FACTOR_MAX, value=float(round(self.rg_long.ddown_factor_1,self.rg_long.DDOWN_FACTOR_ROUND)), step=self.rg_long.DDOWN_FACTOR_STEP, format=self.rg_long.DDOWN_FACTOR_FORMAT, key="config_rg_long_ddown_factor_1", help=pbgui_help.ddown_factor)
                if  "config_rg_long_rentry_pprice_dist_1" in st.session_state:
                    self.rg_long.rentry_pprice_dist_1 = st.session_state.config_rg_long_rentry_pprice_dist_1
                st.number_input("long_rentry_pprice_dist max", min_value=self.rg_long.rentry_pprice_dist_0, max_value=self.rg_long.RENTRY_PPRICE_DIST_MAX, value=float(round(self.rg_long.rentry_pprice_dist_1,self.rg_long.RENTRY_PPRICE_DIST_ROUND)), step=self.rg_long.RENTRY_PPRICE_DIST_STEP, format=self.rg_long.RENTRY_PPRICE_DIST_FORMAT, key="config_rg_long_rentry_pprice_dist_1", help=pbgui_help.rentry_pprice_dist)
                if  "config_rg_long_rentry_pprice_dist_wallet_exposure_weighting_1" in st.session_state:
                    self.rg_long.rentry_pprice_dist_wallet_exposure_weighting_1 = st.session_state.config_rg_long_rentry_pprice_dist_wallet_exposure_weighting_1
                st.number_input("long_rentry_pprice_dist_wallet_exposure_weighting max", min_value=self.rg_long.rentry_pprice_dist_wallet_exposure_weighting_0, max_value=self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX, value=float(round(self.rg_long.rentry_pprice_dist_wallet_exposure_weighting_1,self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.rg_long.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="config_rg_long_rentry_pprice_dist_wallet_exposure_weighting_1", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
                if  "config_rg_long_min_markup_1" in st.session_state:
                    self.rg_long.min_markup_1 = st.session_state.config_rg_long_min_markup_1
                st.number_input("long_min_markup max", min_value=self.rg_long.min_markup_0, max_value=self.rg_long.MIN_MARKUP_MAX, value=float(round(self.rg_long.min_markup_1,self.rg_long.MIN_MARKUP_ROUND)), step=self.rg_long.MIN_MARKUP_STEP, format=self.rg_long.MIN_MARKUP_FORMAT, key="config_rg_long_min_markup_1", help=pbgui_help.min_markup)
                if  "config_rg_long_markup_range_1" in st.session_state:
                    self.rg_long.markup_range_1 = st.session_state.config_rg_long_markup_range_1
                st.number_input("long_markup_range max", min_value=self.rg_long.markup_range_0, max_value=self.rg_long.MARKUP_RANGE_MAX, value=float(round(self.rg_long.markup_range_1,self.rg_long.MARKUP_RANGE_ROUND)), step=self.rg_long.MARKUP_RANGE_STEP, format=self.rg_long.MARKUP_RANGE_FORMAT, key="config_rg_long_markup_range_1", help=pbgui_help.markup_range)
                if  "config_rg_long_n_close_orders_1" in st.session_state:
                    self.rg_long.n_close_orders_1 = st.session_state.config_rg_long_n_close_orders_1
                st.number_input("long_n_close_orders max", min_value=self.rg_long.n_close_orders_1, max_value=self.rg_long.N_CLOSE_ORDERS_MAX, value=self.rg_long.n_close_orders_1, step=self.rg_long.N_CLOSE_ORDERS_STEP, format=self.rg_long.N_CLOSE_ORDERS_FORMAT, key="config_rg_long_n_close_orders_1", help=pbgui_help.n_close_orders)
                if  "config_rg_long_auto_unstuck_wallet_exposure_threshold_1" in st.session_state:
                    self.rg_long.auto_unstuck_wallet_exposure_threshold_1 = st.session_state.config_rg_long_auto_unstuck_wallet_exposure_threshold_1
                st.number_input("long_auto_unstuck_wallet_exposure_threshold max", min_value=self.rg_long.auto_unstuck_wallet_exposure_threshold_0, max_value=self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX, value=float(round(self.rg_long.auto_unstuck_wallet_exposure_threshold_1,self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.rg_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_rg_long_auto_unstuck_wallet_exposure_threshold_1", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_rg_long_auto_unstuck_ema_dist_1" in st.session_state:
                    self.rg_long.auto_unstuck_ema_dist_1 = st.session_state.config_rg_long_auto_unstuck_ema_dist_1
                st.number_input("long_auto_unstuck_ema_dist max", min_value=self.rg_long.auto_unstuck_ema_dist_0, max_value=self.rg_long.AUTO_UNSTUCK_EMA_DIST_MAX, value=float(round(self.rg_long.auto_unstuck_ema_dist_1,self.rg_long.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.rg_long.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.rg_long.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_rg_long_auto_unstuck_ema_dist_1", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_rg_long_auto_unstuck_delay_minutes_1" in st.session_state:
                    self.rg_long.auto_unstuck_delay_minutes_1 = st.session_state.config_rg_long_auto_unstuck_delay_minutes_1
                st.number_input("long_auto_unstuck_delay_minutes max", min_value=self.rg_long.auto_unstuck_delay_minutes_0, max_value=self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_MAX, value=float(round(self.rg_long.auto_unstuck_delay_minutes_1,self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.rg_long.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_rg_long_auto_unstuck_delay_minutes_1", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_rg_long_auto_unstuck_qty_pct_1" in st.session_state:
                    self.rg_long.auto_unstuck_qty_pct_1 = st.session_state.config_rg_long_auto_unstuck_qty_pct_1
                st.number_input("long_auto_unstuck_qty_pct max", min_value=self.rg_long.auto_unstuck_qty_pct_0, max_value=self.rg_long.AUTO_UNSTUCK_QTY_PCT_MAX, value=float(round(self.rg_long.auto_unstuck_qty_pct_1,self.rg_long.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.rg_long.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.rg_long.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_rg_long_auto_unstuck_qty_pct_1", help=pbgui_help.auto_unstuck_qty_pct)
            with col3:
                if  "config_rg_short_ema_span_0_0" in st.session_state:
                    self.rg_short.ema_span_0_0 = st.session_state.config_rg_short_ema_span_0_0
                st.number_input("short_ema_span_0 min", min_value=self.rg_short.EMA_SPAN_0_MIN, max_value=self.rg_short.ema_span_0_1, value=float(round(self.rg_short.ema_span_0_0,1)), step=self.rg_short.EMA_SPAN_0_STEP, format=self.rg_short.EMA_SPAN_0_FORMAT, key="config_rg_short_ema_span_0_0", help=pbgui_help.ema_span)
                if  "config_rg_short_ema_span_1_0" in st.session_state:
                    self.rg_short.ema_span_1_0 = st.session_state.config_rg_short_ema_span_1_0
                st.number_input("short_ema_span_1 min", min_value=self.rg_short.EMA_SPAN_1_MIN, max_value=self.rg_short.ema_span_1_1, value=float(round(self.rg_short.ema_span_1_0,1)), step=self.rg_short.EMA_SPAN_1_STEP, format=self.rg_short.EMA_SPAN_1_FORMAT, key="config_rg_short_ema_span_1_0", help=pbgui_help.ema_span)
                if  "config_rg_short_initial_qty_pct_0" in st.session_state:
                    self.rg_short.initial_qty_pct_0 = st.session_state.config_rg_short_initial_qty_pct_0
                st.number_input("short_initial_qty_pct min", min_value=self.rg_short.INITIAL_QTY_PCT_MIN, max_value=self.rg_short.initial_qty_pct_1, value=float(round(self.rg_short.initial_qty_pct_0,self.rg_short.INITIAL_QTY_PCT_ROUND)), step=self.rg_short.INITIAL_QTY_PCT_STEP, format=self.rg_short.INITIAL_QTY_PCT_FORMAT, key="config_rg_short_initial_qty_pct_0", help=pbgui_help.initial_qty_pct)
                if  "config_rg_short_initial_eprice_ema_dist_0" in st.session_state:
                    self.rg_short.initial_eprice_ema_dist_0 = st.session_state.config_rg_short_initial_eprice_ema_dist_0
                st.number_input("short_initial_eprice_ema_dist min", min_value=self.rg_short.INITIAL_EPRICE_EMA_DIST_MIN, max_value=self.rg_short.initial_eprice_ema_dist_1, value=float(round(self.rg_short.initial_eprice_ema_dist_0,self.rg_short.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.rg_short.INITIAL_EPRICE_EMA_DIST_STEP, format=self.rg_short.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_rg_short_initial_eprice_ema_dist_0", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_rg_short_wallet_exposure_limit_0" in st.session_state:
                    self.rg_short.wallet_exposure_limit_0 = st.session_state.config_rg_short_wallet_exposure_limit_0
                st.number_input("short_wallet_exposure_limit min", min_value=self.rg_short.WALLET_EXPOSURE_LIMIT_MIN, max_value=self.rg_short.wallet_exposure_limit_1, value=float(round(self.rg_short.wallet_exposure_limit_0,self.rg_short.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.rg_short.WALLET_EXPOSURE_LIMIT_STEP, format=self.rg_short.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_rg_short_wallet_exposure_limit_0", help=pbgui_help.wallet_exposure_limit)
                if  "config_rg_short_ddown_factor_0" in st.session_state:
                    self.rg_short.ddown_factor_0 = st.session_state.config_rg_short_ddown_factor_0
                st.number_input("short_ddown_factor min", min_value=self.rg_short.DDOWN_FACTOR_MIN, max_value=self.rg_short.ddown_factor_1, value=float(round(self.rg_short.ddown_factor_0,self.rg_short.DDOWN_FACTOR_ROUND)), step=self.rg_short.DDOWN_FACTOR_STEP, format=self.rg_short.DDOWN_FACTOR_FORMAT, key="config_rg_short_ddown_factor_0", help=pbgui_help.ddown_factor)
                if  "config_rg_short_rentry_pprice_dist_0" in st.session_state:
                    self.rg_short.rentry_pprice_dist_0 = st.session_state.config_rg_short_rentry_pprice_dist_0
                st.number_input("short_rentry_pprice_dist min", min_value=self.rg_short.RENTRY_PPRICE_DIST_MIN, max_value=self.rg_short.rentry_pprice_dist_1, value=float(round(self.rg_short.rentry_pprice_dist_0,self.rg_short.RENTRY_PPRICE_DIST_ROUND)), step=self.rg_short.RENTRY_PPRICE_DIST_STEP, format=self.rg_short.RENTRY_PPRICE_DIST_FORMAT, key="config_rg_short_rentry_pprice_dist_0", help=pbgui_help.rentry_pprice_dist)
                if  "config_rg_short_rentry_pprice_dist_wallet_exposure_weighting_0" in st.session_state:
                    self.rg_short.rentry_pprice_dist_wallet_exposure_weighting_0 = st.session_state.config_rg_short_rentry_pprice_dist_wallet_exposure_weighting_0
                st.number_input("short_rentry_pprice_dist_wallet_exposure_weighting min", min_value=self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN, max_value=self.rg_short.rentry_pprice_dist_wallet_exposure_weighting_1, value=float(round(self.rg_short.rentry_pprice_dist_wallet_exposure_weighting_0,self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="config_rg_short_rentry_pprice_dist_wallet_exposure_weighting_0", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
                if  "config_rg_short_min_markup_0" in st.session_state:
                    self.rg_short.min_markup_0 = st.session_state.config_rg_short_min_markup_0
                st.number_input("short_min_markup min", min_value=self.rg_short.MIN_MARKUP_MIN, max_value=self.rg_short.min_markup_1, value=float(round(self.rg_short.min_markup_0,self.rg_short.MIN_MARKUP_ROUND)), step=self.rg_short.MIN_MARKUP_STEP, format=self.rg_short.MIN_MARKUP_FORMAT, key="config_rg_short_min_markup_0", help=pbgui_help.min_markup)
                if  "config_rg_short_markup_range_0" in st.session_state:
                    self.rg_short.markup_range_0 = st.session_state.config_rg_short_markup_range_0
                st.number_input("short_markup_range min", min_value=self.rg_short.MARKUP_RANGE_MIN, max_value=self.rg_short.markup_range_1, value=float(round(self.rg_short.markup_range_0,self.rg_short.MARKUP_RANGE_ROUND)), step=self.rg_short.MARKUP_RANGE_STEP, format=self.rg_short.MARKUP_RANGE_FORMAT, key="config_rg_short_markup_range_0", help=pbgui_help.markup_range)
                if  "config_rg_short_n_close_orders_0" in st.session_state:
                    self.rg_short.n_close_orders_0 = st.session_state.config_rg_short_n_close_orders_0
                st.number_input("short_n_close_orders min", min_value=self.rg_short.N_CLOSE_ORDERS_MIN, max_value=self.rg_short.n_close_orders_1, value=self.rg_short.n_close_orders_0, step=self.rg_short.N_CLOSE_ORDERS_STEP, format=self.rg_short.N_CLOSE_ORDERS_FORMAT, key="config_rg_short_n_close_orders_0", help=pbgui_help.n_close_orders)
                if  "config_rg_short_auto_unstuck_wallet_exposure_threshold_0" in st.session_state:
                    self.rg_short.auto_unstuck_wallet_exposure_threshold_0 = st.session_state.config_rg_short_auto_unstuck_wallet_exposure_threshold_0
                st.number_input("short_auto_unstuck_wallet_exposure_threshold min", min_value=self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN, max_value=self.rg_short.auto_unstuck_wallet_exposure_threshold_1, value=float(round(self.rg_short.auto_unstuck_wallet_exposure_threshold_0,self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_rg_short_auto_unstuck_wallet_exposure_threshold_0", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_rg_short_auto_unstuck_ema_dist_0" in st.session_state:
                    self.rg_short.auto_unstuck_ema_dist_0 = st.session_state.config_rg_short_auto_unstuck_ema_dist_0
                st.number_input("short_auto_unstuck_ema_dist min", min_value=self.rg_short.AUTO_UNSTUCK_EMA_DIST_MIN, max_value=self.rg_short.auto_unstuck_ema_dist_1, value=float(round(self.rg_short.auto_unstuck_ema_dist_0,self.rg_short.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.rg_short.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.rg_short.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_rg_short_auto_unstuck_ema_dist_0", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_rg_short_auto_unstuck_delay_minutes_0" in st.session_state:
                    self.rg_short.auto_unstuck_delay_minutes_0 = st.session_state.config_rg_short_auto_unstuck_delay_minutes_0
                st.number_input("short_auto_unstuck_delay_minutes min", min_value=self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_MIN, max_value=self.rg_short.auto_unstuck_delay_minutes_1, value=float(round(self.rg_short.auto_unstuck_delay_minutes_0,self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_rg_short_auto_unstuck_delay_minutes_0", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_rg_short_auto_unstuck_qty_pct_0" in st.session_state:
                    self.rg_short.auto_unstuck_qty_pct_0 = st.session_state.config_rg_short_auto_unstuck_qty_pct_0
                st.number_input("short_auto_unstuck_qty_pct min", min_value=self.rg_short.AUTO_UNSTUCK_QTY_PCT_MIN, max_value=self.rg_short.auto_unstuck_qty_pct_1, value=float(round(self.rg_short.auto_unstuck_qty_pct_0,self.rg_short.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.rg_short.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.rg_short.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_rg_short_auto_unstuck_qty_pct_0", help=pbgui_help.auto_unstuck_qty_pct)
            with col4:
                if  "config_rg_short_ema_span_0_1" in st.session_state:
                    self.rg_short.ema_span_0_1 = st.session_state.config_rg_short_ema_span_0_1
                st.number_input("short_ema_span_0 max", min_value=self.rg_short.ema_span_0_0, max_value=self.rg_short.EMA_SPAN_0_MAX, value=float(round(self.rg_short.ema_span_0_1,1)), step=self.rg_short.EMA_SPAN_0_STEP, format=self.rg_short.EMA_SPAN_0_FORMAT, key="config_rg_short_ema_span_0_1", help=pbgui_help.ema_span)
                if  "config_rg_short_ema_span_1_1" in st.session_state:
                    self.rg_short.ema_span_1_1 = st.session_state.config_rg_short_ema_span_1_1
                st.number_input("short_ema_span_0 max", min_value=self.rg_short.ema_span_1_0, max_value=self.rg_short.EMA_SPAN_1_MAX, value=float(round(self.rg_short.ema_span_1_1,1)), step=self.rg_short.EMA_SPAN_1_STEP, format=self.rg_short.EMA_SPAN_1_FORMAT, key="config_rg_short_ema_span_1_1", help=pbgui_help.ema_span)
                if  "config_rg_short_initial_qty_pct_1" in st.session_state:
                    self.rg_short.initial_qty_pct_1 = st.session_state.config_rg_short_initial_qty_pct_1
                st.number_input("short_initial_qty_pct max", min_value=self.rg_short.initial_qty_pct_0, max_value=self.rg_short.INITIAL_QTY_PCT_MAX, value=float(round(self.rg_short.initial_qty_pct_1,self.rg_short.INITIAL_QTY_PCT_ROUND)), step=self.rg_short.INITIAL_QTY_PCT_STEP, format=self.rg_short.INITIAL_QTY_PCT_FORMAT, key="config_rg_short_initial_qty_pct_1", help=pbgui_help.initial_qty_pct)
                if  "config_rg_short_initial_eprice_ema_dist_1" in st.session_state:
                    self.rg_short.initial_eprice_ema_dist_1 = st.session_state.config_rg_short_initial_eprice_ema_dist_1
                st.number_input("short_initial_eprice_ema_dist max", min_value=self.rg_short.initial_eprice_ema_dist_0, max_value=self.rg_short.INITIAL_EPRICE_EMA_DIST_MAX, value=float(round(self.rg_short.initial_eprice_ema_dist_1,self.rg_short.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.rg_short.INITIAL_EPRICE_EMA_DIST_STEP, format=self.rg_short.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_rg_short_initial_eprice_ema_dist_1", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_rg_short_wallet_exposure_limit_1" in st.session_state:
                    self.rg_short.wallet_exposure_limit_1 = st.session_state.config_rg_short_wallet_exposure_limit_1
                st.number_input("short_wallet_exposure_limit max", min_value=self.rg_short.wallet_exposure_limit_0, max_value=self.rg_short.WALLET_EXPOSURE_LIMIT_MAX, value=float(round(self.rg_short.wallet_exposure_limit_1,self.rg_short.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.rg_short.WALLET_EXPOSURE_LIMIT_STEP, format=self.rg_short.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_rg_short_wallet_exposure_limit_1", help=pbgui_help.wallet_exposure_limit)
                if  "config_rg_short_ddown_factor_1" in st.session_state:
                    self.rg_short.ddown_factor_1 = st.session_state.config_rg_short_ddown_factor_1
                st.number_input("short_ddown_factor max", min_value=self.rg_short.ddown_factor_0, max_value=self.rg_short.DDOWN_FACTOR_MAX, value=float(round(self.rg_short.ddown_factor_1,self.rg_short.DDOWN_FACTOR_ROUND)), step=self.rg_short.DDOWN_FACTOR_STEP, format=self.rg_short.DDOWN_FACTOR_FORMAT, key="config_rg_short_ddown_factor_1", help=pbgui_help.ddown_factor)
                if  "config_rg_short_rentry_pprice_dist_1" in st.session_state:
                    self.rg_short.rentry_pprice_dist_1 = st.session_state.config_rg_short_rentry_pprice_dist_1
                st.number_input("short_rentry_pprice_dist max", min_value=self.rg_short.rentry_pprice_dist_0, max_value=self.rg_short.RENTRY_PPRICE_DIST_MAX, value=float(round(self.rg_short.rentry_pprice_dist_1,self.rg_short.RENTRY_PPRICE_DIST_ROUND)), step=self.rg_short.RENTRY_PPRICE_DIST_STEP, format=self.rg_short.RENTRY_PPRICE_DIST_FORMAT, key="config_rg_short_rentry_pprice_dist_1", help=pbgui_help.rentry_pprice_dist)
                if  "config_rg_short_rentry_pprice_dist_wallet_exposure_weighting_1" in st.session_state:
                    self.rg_short.rentry_pprice_dist_wallet_exposure_weighting_1 = st.session_state.config_rg_short_rentry_pprice_dist_wallet_exposure_weighting_1
                st.number_input("short_rentry_pprice_dist_wallet_exposure_weighting max", min_value=self.rg_short.rentry_pprice_dist_wallet_exposure_weighting_0, max_value=self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX, value=float(round(self.rg_short.rentry_pprice_dist_wallet_exposure_weighting_1,self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.rg_short.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="config_rg_short_rentry_pprice_dist_wallet_exposure_weighting_1", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
                if  "config_rg_short_min_markup_1" in st.session_state:
                    self.rg_short.min_markup_1 = st.session_state.config_rg_short_min_markup_1
                st.number_input("short_min_markup max", min_value=self.rg_short.min_markup_0, max_value=self.rg_short.MIN_MARKUP_MAX, value=float(round(self.rg_short.min_markup_1,self.rg_short.MIN_MARKUP_ROUND)), step=self.rg_short.MIN_MARKUP_STEP, format=self.rg_short.MIN_MARKUP_FORMAT, key="config_rg_short_min_markup_1", help=pbgui_help.min_markup)
                if  "config_rg_short_markup_range_1" in st.session_state:
                    self.rg_short.markup_range_1 = st.session_state.config_rg_short_markup_range_1
                st.number_input("short_markup_range max", min_value=self.rg_short.markup_range_0, max_value=self.rg_short.MARKUP_RANGE_MAX, value=float(round(self.rg_short.markup_range_1,self.rg_short.MARKUP_RANGE_ROUND)), step=self.rg_short.MARKUP_RANGE_STEP, format=self.rg_short.MARKUP_RANGE_FORMAT, key="config_rg_short_markup_range_1", help=pbgui_help.markup_range)
                if  "config_rg_short_n_close_orders_1" in st.session_state:
                    self.rg_short.n_close_orders_1 = st.session_state.config_rg_short_n_close_orders_1
                st.number_input("short_n_close_orders max", min_value=self.rg_short.n_close_orders_1, max_value=self.rg_short.N_CLOSE_ORDERS_MAX, value=self.rg_short.n_close_orders_1, step=self.rg_short.N_CLOSE_ORDERS_STEP, format=self.rg_short.N_CLOSE_ORDERS_FORMAT, key="config_rg_short_n_close_orders_1", help=pbgui_help.n_close_orders)
                if  "config_rg_short_auto_unstuck_wallet_exposure_threshold_1" in st.session_state:
                    self.rg_short.auto_unstuck_wallet_exposure_threshold_1 = st.session_state.config_rg_short_auto_unstuck_wallet_exposure_threshold_1
                st.number_input("short_auto_unstuck_wallet_exposure_threshold max", min_value=self.rg_short.auto_unstuck_wallet_exposure_threshold_0, max_value=self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX, value=float(round(self.rg_short.auto_unstuck_wallet_exposure_threshold_1,self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.rg_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_rg_short_auto_unstuck_wallet_exposure_threshold_1", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_rg_short_auto_unstuck_ema_dist_1" in st.session_state:
                    self.rg_short.auto_unstuck_ema_dist_1 = st.session_state.config_rg_short_auto_unstuck_ema_dist_1
                st.number_input("short_auto_unstuck_ema_dist max", min_value=self.rg_short.auto_unstuck_ema_dist_0, max_value=self.rg_short.AUTO_UNSTUCK_EMA_DIST_MAX, value=float(round(self.rg_short.auto_unstuck_ema_dist_1,self.rg_short.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.rg_short.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.rg_short.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_rg_short_auto_unstuck_ema_dist_1", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_rg_short_auto_unstuck_delay_minutes_1" in st.session_state:
                    self.rg_short.auto_unstuck_delay_minutes_1 = st.session_state.config_rg_short_auto_unstuck_delay_minutes_1
                st.number_input("short_auto_unstuck_delay_minutes max", min_value=self.rg_short.auto_unstuck_delay_minutes_0, max_value=self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_MAX, value=float(round(self.rg_short.auto_unstuck_delay_minutes_1,self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.rg_short.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_rg_short_auto_unstuck_delay_minutes_1", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_rg_short_auto_unstuck_qty_pct_1" in st.session_state:
                    self.rg_short.auto_unstuck_qty_pct_1 = st.session_state.config_rg_short_auto_unstuck_qty_pct_1
                st.number_input("short_auto_unstuck_qty_pct max", min_value=self.rg_short.auto_unstuck_qty_pct_0, max_value=self.rg_short.AUTO_UNSTUCK_QTY_PCT_MAX, value=float(round(self.rg_short.auto_unstuck_qty_pct_1,self.rg_short.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.rg_short.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.rg_short.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_rg_short_auto_unstuck_qty_pct_1", help=pbgui_help.auto_unstuck_qty_pct)
        with st.expander("neat grid", expanded = True if self.passivbot_mode == "neat_grid" else False):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                if  "config_ng_long_grid_span_0" in st.session_state:
                    self.ng_long.grid_span_0 = st.session_state.config_ng_long_grid_span_0
                st.number_input("long_grid_span min", min_value=self.ng_long.GRID_SPAN_MIN, max_value=self.ng_long.grid_span_1, value=float(round(self.ng_long.grid_span_0,self.ng_long.GRID_SPAN_ROUND)), step=self.ng_long.GRID_SPAN_STEP, format=self.ng_long.GRID_SPAN_FORMAT, key="config_ng_long_grid_span_0", help=pbgui_help.grid_span)
                if  "config_ng_long_ema_span_0_0" in st.session_state:
                    self.ng_long.ema_span_0_0 = st.session_state.config_ng_long_ema_span_0_0
                st.number_input("long_ema_span_0 min", min_value=self.ng_long.EMA_SPAN_0_MIN, max_value=self.ng_long.ema_span_0_1, value=float(round(self.ng_long.ema_span_0_0,self.ng_long.EMA_SPAN_0_ROUND)), step=self.ng_long.EMA_SPAN_0_STEP, format=self.ng_long.EMA_SPAN_0_FORMAT, key="config_ng_long_ema_span_0_0", help=pbgui_help.ema_span)
                if  "config_ng_long_ema_span_1_0" in st.session_state:
                    self.ng_long.ema_span_1_0 = st.session_state.config_ng_long_ema_span_1_0
                st.number_input("long_ema_span_1 min", min_value=self.ng_long.EMA_SPAN_1_MIN, max_value=self.ng_long.ema_span_1_1, value=float(round(self.ng_long.ema_span_1_0,self.ng_long.EMA_SPAN_1_ROUND)), step=self.ng_long.EMA_SPAN_1_STEP, format=self.ng_long.EMA_SPAN_1_FORMAT, key="config_ng_long_ema_span_1_0", help=pbgui_help.ema_span)
                if  "config_ng_long_wallet_exposure_limit_0" in st.session_state:
                    self.ng_long.wallet_exposure_limit_0 = st.session_state.config_ng_long_wallet_exposure_limit_0
                st.number_input("long_wallet_exposure_limit min", min_value=self.ng_long.WALLET_EXPOSURE_LIMIT_MIN, max_value=self.ng_long.wallet_exposure_limit_1, value=float(round(self.ng_long.wallet_exposure_limit_0,self.ng_long.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.ng_long.WALLET_EXPOSURE_LIMIT_STEP, format=self.ng_long.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_ng_long_wallet_exposure_limit_0", help=pbgui_help.wallet_exposure_limit)
                if  "config_ng_long_max_n_entry_orders_0" in st.session_state:
                    self.ng_long.max_n_entry_orders_0 = st.session_state.config_ng_long_max_n_entry_orders_0
                st.number_input("long_max_n_entry_orders min", min_value=self.ng_long.MAX_N_ENTRY_ORDERS_MIN, max_value=self.ng_long.max_n_entry_orders_1, value=self.ng_long.max_n_entry_orders_0, step=self.ng_long.MAX_N_ENTRY_ORDERS_STEP, format=self.ng_long.MAX_N_ENTRY_ORDERS_FORMAT, key="config_ng_long_max_n_entry_orders_0", help=pbgui_help.max_n_entry_orders)
                if  "config_ng_long_initial_qty_pct_0" in st.session_state:
                    self.ng_long.initial_qty_pct_0 = st.session_state.config_ng_long_initial_qty_pct_0
                st.number_input("long_initial_qty_pct min", min_value=self.ng_long.INITIAL_QTY_PCT_MIN, max_value=self.ng_long.initial_qty_pct_1, value=float(round(self.ng_long.initial_qty_pct_0,self.ng_long.INITIAL_QTY_PCT_ROUND)), step=self.ng_long.INITIAL_QTY_PCT_STEP, format=self.ng_long.INITIAL_QTY_PCT_FORMAT, key="config_ng_long_initial_qty_pct_0", help=pbgui_help.initial_qty_pct)
                if  "config_ng_long_initial_eprice_ema_dist_0" in st.session_state:
                    self.ng_long.initial_eprice_ema_dist_0 = st.session_state.config_ng_long_initial_eprice_ema_dist_0
                st.number_input("long_initial_eprice_ema_dist min", min_value=self.ng_long.INITIAL_EPRICE_EMA_DIST_MIN, max_value=self.ng_long.initial_eprice_ema_dist_1, value=float(round(self.ng_long.initial_eprice_ema_dist_0,self.ng_long.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.ng_long.INITIAL_EPRICE_EMA_DIST_STEP, format=self.ng_long.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_ng_long_initial_eprice_ema_dist_0", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_ng_long_eqty_exp_base_0" in st.session_state:
                    self.ng_long.eqty_exp_base_0 = st.session_state.config_ng_long_eqty_exp_base_0
                st.number_input("long_eqty_exp_base min", min_value=self.ng_long.EQTY_EXP_BASE_MIN, max_value=self.ng_long.eqty_exp_base_1, value=float(round(self.ng_long.eqty_exp_base_0,self.ng_long.EQTY_EXP_BASE_ROUND)), step=self.ng_long.EQTY_EXP_BASE_STEP, format=self.ng_long.EQTY_EXP_BASE_FORMAT, key="config_ng_long_eqty_exp_base_0", help=pbgui_help.eqty_exp_base)
                if  "config_ng_long_eprice_exp_base_0" in st.session_state:
                    self.ng_long.eprice_exp_base_0 = st.session_state.config_ng_long_eprice_exp_base_0
                st.number_input("long_eprice_exp_base min", min_value=self.ng_long.EPRICE_EXP_BASE_MIN, max_value=self.ng_long.eprice_exp_base_1, value=float(round(self.ng_long.eprice_exp_base_0,self.ng_long.EPRICE_EXP_BASE_ROUND)), step=self.ng_long.EPRICE_EXP_BASE_STEP, format=self.ng_long.EPRICE_EXP_BASE_FORMAT, key="config_ng_long_eprice_exp_base_0", help=pbgui_help.eprice_exp_base)
                if  "config_ng_long_min_markup_0" in st.session_state:
                    self.ng_long.min_markup_0 = st.session_state.config_ng_long_min_markup_0
                st.number_input("long_min_markup min", min_value=self.ng_long.MIN_MARKUP_MIN, max_value=self.ng_long.min_markup_1, value=float(round(self.ng_long.min_markup_0,self.ng_long.MIN_MARKUP_ROUND)), step=self.ng_long.MIN_MARKUP_STEP, format=self.ng_long.MIN_MARKUP_FORMAT, key="config_ng_long_min_markup_0", help=pbgui_help.min_markup)
                if  "config_ng_long_markup_range_0" in st.session_state:
                    self.ng_long.markup_range_0 = st.session_state.config_ng_long_markup_range_0
                st.number_input("long_markup_range min", min_value=self.ng_long.MARKUP_RANGE_MIN, max_value=self.ng_long.markup_range_1, value=float(round(self.ng_long.markup_range_0,self.ng_long.MARKUP_RANGE_ROUND)), step=self.ng_long.MARKUP_RANGE_STEP, format=self.ng_long.MARKUP_RANGE_FORMAT, key="config_ng_long_markup_range_0", help=pbgui_help.markup_range)
                if  "config_ng_long_n_close_orders_0" in st.session_state:
                    self.ng_long.n_close_orders_0 = st.session_state.config_ng_long_n_close_orders_0
                st.number_input("long_n_close_orders min", min_value=self.ng_long.N_CLOSE_ORDERS_MIN, max_value=self.ng_long.n_close_orders_1, value=self.ng_long.n_close_orders_0, step=self.ng_long.N_CLOSE_ORDERS_STEP, format=self.ng_long.N_CLOSE_ORDERS_FORMAT, key="config_ng_long_n_close_orders_0", help=pbgui_help.n_close_orders)
                if  "config_ng_long_auto_unstuck_wallet_exposure_threshold_0" in st.session_state:
                    self.ng_long.auto_unstuck_wallet_exposure_threshold_0 = st.session_state.config_ng_long_auto_unstuck_wallet_exposure_threshold_0
                st.number_input("long_auto_unstuck_wallet_exposure_threshold min", min_value=self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN, max_value=self.ng_long.auto_unstuck_wallet_exposure_threshold_1, value=float(round(self.ng_long.auto_unstuck_wallet_exposure_threshold_0,self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_ng_long_auto_unstuck_wallet_exposure_threshold_0", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_ng_long_auto_unstuck_ema_dist_0" in st.session_state:
                    self.ng_long.auto_unstuck_ema_dist_0 = st.session_state.config_ng_long_auto_unstuck_ema_dist_0
                st.number_input("long_auto_unstuck_ema_dist min", min_value=self.ng_long.AUTO_UNSTUCK_EMA_DIST_MIN, max_value=self.ng_long.auto_unstuck_ema_dist_1, value=float(round(self.ng_long.auto_unstuck_ema_dist_0,self.ng_long.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.ng_long.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.ng_long.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_ng_long_auto_unstuck_ema_dist_0", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_ng_long_auto_unstuck_delay_minutes_0" in st.session_state:
                    self.ng_long.auto_unstuck_delay_minutes_0 = st.session_state.config_ng_long_auto_unstuck_delay_minutes_0
                st.number_input("long_auto_unstuck_delay_minutes min", min_value=self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_MIN, max_value=self.ng_long.auto_unstuck_delay_minutes_1, value=float(round(self.ng_long.auto_unstuck_delay_minutes_0,self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_ng_long_auto_unstuck_delay_minutes_0", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_ng_long_auto_unstuck_qty_pct_0" in st.session_state:
                    self.ng_long.auto_unstuck_qty_pct_0 = st.session_state.config_ng_long_auto_unstuck_qty_pct_0
                st.number_input("long_auto_unstuck_qty_pct min", min_value=self.ng_long.AUTO_UNSTUCK_QTY_PCT_MIN, max_value=self.ng_long.auto_unstuck_qty_pct_1, value=float(round(self.ng_long.auto_unstuck_qty_pct_0,self.ng_long.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.ng_long.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.ng_long.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_ng_long_auto_unstuck_qty_pct_0", help=pbgui_help.auto_unstuck_qty_pct)
            with col2:
                if  "config_ng_long_grid_span_1" in st.session_state:
                    self.ng_long.grid_span_1 = st.session_state.config_ng_long_grid_span_1
                st.number_input("long_grid_span max", min_value=self.ng_long.grid_span_0, max_value=self.ng_long.GRID_SPAN_MAX, value=float(round(self.ng_long.grid_span_1,self.ng_long.GRID_SPAN_ROUND)), step=self.ng_long.GRID_SPAN_STEP, format=self.ng_long.GRID_SPAN_FORMAT, key="config_ng_long_grid_span_1", help=pbgui_help.grid_span)
                if  "config_ng_long_ema_span_0_1" in st.session_state:
                    self.ng_long.ema_span_0_1 = st.session_state.config_ng_long_ema_span_0_1
                st.number_input("long_ema_span_0 max", min_value=self.ng_long.ema_span_0_0, max_value=self.ng_long.EMA_SPAN_0_MAX, value=float(round(self.ng_long.ema_span_0_1,self.ng_long.EMA_SPAN_0_ROUND)), step=self.ng_long.EMA_SPAN_0_STEP, format=self.ng_long.EMA_SPAN_0_FORMAT, key="config_ng_long_ema_span_0_1", help=pbgui_help.ema_span)
                if  "config_ng_long_ema_span_1_1" in st.session_state:
                    self.ng_long.ema_span_1_1 = st.session_state.config_ng_long_ema_span_1_1
                st.number_input("long_ema_span_0 max", min_value=self.ng_long.ema_span_1_0, max_value=self.ng_long.EMA_SPAN_1_MAX, value=float(round(self.ng_long.ema_span_1_1,self.ng_long.EMA_SPAN_1_ROUND)), step=self.ng_long.EMA_SPAN_1_STEP, format=self.ng_long.EMA_SPAN_1_FORMAT, key="config_ng_long_ema_span_1_1", help=pbgui_help.ema_span)
                if  "config_ng_long_wallet_exposure_limit_1" in st.session_state:
                    self.ng_long.wallet_exposure_limit_1 = st.session_state.config_ng_long_wallet_exposure_limit_1
                st.number_input("long_wallet_exposure_limit max", min_value=self.ng_long.wallet_exposure_limit_0, max_value=self.ng_long.WALLET_EXPOSURE_LIMIT_MAX, value=float(round(self.ng_long.wallet_exposure_limit_1,self.ng_long.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.ng_long.WALLET_EXPOSURE_LIMIT_STEP, format=self.ng_long.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_ng_long_wallet_exposure_limit_1", help=pbgui_help.wallet_exposure_limit)
                if  "config_ng_long_max_n_entry_orders_1" in st.session_state:
                    self.ng_long.max_n_entry_orders_1 = st.session_state.config_ng_long_max_n_entry_orders_1
                st.number_input("long_max_n_entry_orders max", min_value=self.ng_long.max_n_entry_orders_1, max_value=self.ng_long.MAX_N_ENTRY_ORDERS_MAX, value=self.ng_long.max_n_entry_orders_1, step=self.ng_long.MAX_N_ENTRY_ORDERS_STEP, format=self.ng_long.MAX_N_ENTRY_ORDERS_FORMAT, key="config_ng_long_max_n_entry_orders_1", help=pbgui_help.max_n_entry_orders)
                if  "config_ng_long_initial_qty_pct_1" in st.session_state:
                    self.ng_long.initial_qty_pct_1 = st.session_state.config_ng_long_initial_qty_pct_1
                st.number_input("long_initial_qty_pct max", min_value=self.ng_long.initial_qty_pct_0, max_value=self.ng_long.INITIAL_QTY_PCT_MAX, value=float(round(self.ng_long.initial_qty_pct_1,self.ng_long.INITIAL_QTY_PCT_ROUND)), step=self.ng_long.INITIAL_QTY_PCT_STEP, format=self.ng_long.INITIAL_QTY_PCT_FORMAT, key="config_ng_long_initial_qty_pct_1", help=pbgui_help.initial_qty_pct)
                if  "config_ng_long_initial_eprice_ema_dist_1" in st.session_state:
                    self.ng_long.initial_eprice_ema_dist_1 = st.session_state.config_ng_long_initial_eprice_ema_dist_1
                st.number_input("long_initial_eprice_ema_dist max", min_value=self.ng_long.initial_eprice_ema_dist_0, max_value=self.ng_long.INITIAL_EPRICE_EMA_DIST_MAX, value=float(round(self.ng_long.initial_eprice_ema_dist_1,self.ng_long.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.ng_long.INITIAL_EPRICE_EMA_DIST_STEP, format=self.ng_long.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_ng_long_initial_eprice_ema_dist_1", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_ng_long_eqty_exp_base_1" in st.session_state:
                    self.ng_long.eqty_exp_base_1 = st.session_state.config_ng_long_eqty_exp_base_1
                st.number_input("long_eqty_exp_base max", min_value=self.ng_long.eqty_exp_base_0, max_value=self.ng_long.EQTY_EXP_BASE_MAX, value=float(round(self.ng_long.eqty_exp_base_1,self.ng_long.EQTY_EXP_BASE_ROUND)), step=self.ng_long.EQTY_EXP_BASE_STEP, format=self.ng_long.EQTY_EXP_BASE_FORMAT, key="config_ng_long_eqty_exp_base_1", help=pbgui_help.eqty_exp_base)
                if  "config_ng_long_eprice_exp_base_1" in st.session_state:
                    self.ng_long.eprice_exp_base_1 = st.session_state.config_ng_long_eprice_exp_base_1
                st.number_input("long_eprice_exp_base max", min_value=self.ng_long.eprice_exp_base_0, max_value=self.ng_long.EPRICE_EXP_BASE_MAX, value=float(round(self.ng_long.eprice_exp_base_1,self.ng_long.EPRICE_EXP_BASE_ROUND)), step=self.ng_long.EPRICE_EXP_BASE_STEP, format=self.ng_long.EPRICE_EXP_BASE_FORMAT, key="config_ng_long_eprice_exp_base_1", help=pbgui_help.eprice_exp_base)
                if  "config_ng_long_min_markup_1" in st.session_state:
                    self.ng_long.min_markup_1 = st.session_state.config_ng_long_min_markup_1
                st.number_input("long_min_markup max", min_value=self.ng_long.min_markup_0, max_value=self.ng_long.MIN_MARKUP_MAX, value=float(round(self.ng_long.min_markup_1,self.ng_long.MIN_MARKUP_ROUND)), step=self.ng_long.MIN_MARKUP_STEP, format=self.ng_long.MIN_MARKUP_FORMAT, key="config_ng_long_min_markup_1", help=pbgui_help.min_markup)
                if  "config_ng_long_markup_range_1" in st.session_state:
                    self.ng_long.markup_range_1 = st.session_state.config_ng_long_markup_range_1
                st.number_input("long_markup_range max", min_value=self.ng_long.markup_range_0, max_value=self.ng_long.MARKUP_RANGE_MAX, value=float(round(self.ng_long.markup_range_1,self.ng_long.MARKUP_RANGE_ROUND)), step=self.ng_long.MARKUP_RANGE_STEP, format=self.ng_long.MARKUP_RANGE_FORMAT, key="config_ng_long_markup_range_1", help=pbgui_help.markup_range)
                if  "config_ng_long_n_close_orders_1" in st.session_state:
                    self.ng_long.n_close_orders_1 = st.session_state.config_ng_long_n_close_orders_1
                st.number_input("long_n_close_orders max", min_value=self.ng_long.n_close_orders_1, max_value=self.ng_long.N_CLOSE_ORDERS_MAX, value=self.ng_long.n_close_orders_1, step=self.ng_long.N_CLOSE_ORDERS_STEP, format=self.ng_long.N_CLOSE_ORDERS_FORMAT, key="config_ng_long_n_close_orders_1", help=pbgui_help.n_close_orders)
                if  "config_ng_long_auto_unstuck_wallet_exposure_threshold_1" in st.session_state:
                    self.ng_long.auto_unstuck_wallet_exposure_threshold_1 = st.session_state.config_ng_long_auto_unstuck_wallet_exposure_threshold_1
                st.number_input("long_auto_unstuck_wallet_exposure_threshold max", min_value=self.ng_long.auto_unstuck_wallet_exposure_threshold_0, max_value=self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX, value=float(round(self.ng_long.auto_unstuck_wallet_exposure_threshold_1,self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.ng_long.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_ng_long_auto_unstuck_wallet_exposure_threshold_1", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_ng_long_auto_unstuck_ema_dist_1" in st.session_state:
                    self.ng_long.auto_unstuck_ema_dist_1 = st.session_state.config_ng_long_auto_unstuck_ema_dist_1
                st.number_input("long_auto_unstuck_ema_dist max", min_value=self.ng_long.auto_unstuck_ema_dist_0, max_value=self.ng_long.AUTO_UNSTUCK_EMA_DIST_MAX, value=float(round(self.ng_long.auto_unstuck_ema_dist_1,self.ng_long.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.ng_long.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.ng_long.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_ng_long_auto_unstuck_ema_dist_1", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_ng_long_auto_unstuck_delay_minutes_1" in st.session_state:
                    self.ng_long.auto_unstuck_delay_minutes_1 = st.session_state.config_ng_long_auto_unstuck_delay_minutes_1
                st.number_input("long_auto_unstuck_delay_minutes max", min_value=self.ng_long.auto_unstuck_delay_minutes_0, max_value=self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_MAX, value=float(round(self.ng_long.auto_unstuck_delay_minutes_1,self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.ng_long.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_ng_long_auto_unstuck_delay_minutes_1", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_ng_long_auto_unstuck_qty_pct_1" in st.session_state:
                    self.ng_long.auto_unstuck_qty_pct_1 = st.session_state.config_ng_long_auto_unstuck_qty_pct_1
                st.number_input("long_auto_unstuck_qty_pct max", min_value=self.ng_long.auto_unstuck_qty_pct_0, max_value=self.ng_long.AUTO_UNSTUCK_QTY_PCT_MAX, value=float(round(self.ng_long.auto_unstuck_qty_pct_1,self.ng_long.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.ng_long.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.ng_long.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_ng_long_auto_unstuck_qty_pct_1", help=pbgui_help.auto_unstuck_qty_pct)
            with col3:
                if  "config_ng_short_grid_span_0" in st.session_state:
                    self.ng_short.grid_span_0 = st.session_state.config_ng_short_grid_span_0
                st.number_input("short_grid_span min", min_value=self.ng_short.GRID_SPAN_MIN, max_value=self.ng_short.grid_span_1, value=float(round(self.ng_short.grid_span_0,self.ng_short.GRID_SPAN_ROUND)), step=self.ng_short.GRID_SPAN_STEP, format=self.ng_short.GRID_SPAN_FORMAT, key="config_ng_short_grid_span_0", help=pbgui_help.grid_span)
                if  "config_ng_short_ema_span_0_0" in st.session_state:
                    self.ng_short.ema_span_0_0 = st.session_state.config_ng_short_ema_span_0_0
                st.number_input("short_ema_span_0 min", min_value=self.ng_short.EMA_SPAN_0_MIN, max_value=self.ng_short.ema_span_0_1, value=float(round(self.ng_short.ema_span_0_0,self.ng_short.EMA_SPAN_0_ROUND)), step=self.ng_short.EMA_SPAN_0_STEP, format=self.ng_short.EMA_SPAN_0_FORMAT, key="config_ng_short_ema_span_0_0", help=pbgui_help.ema_span)
                if  "config_ng_short_ema_span_1_0" in st.session_state:
                    self.ng_short.ema_span_1_0 = st.session_state.config_ng_short_ema_span_1_0
                st.number_input("short_ema_span_1 min", min_value=self.ng_short.EMA_SPAN_1_MIN, max_value=self.ng_short.ema_span_1_1, value=float(round(self.ng_short.ema_span_1_0,self.ng_short.EMA_SPAN_1_ROUND)), step=self.ng_short.EMA_SPAN_1_STEP, format=self.ng_short.EMA_SPAN_1_FORMAT, key="config_ng_short_ema_span_1_0", help=pbgui_help.ema_span)
                if  "config_ng_short_wallet_exposure_limit_0" in st.session_state:
                    self.ng_short.wallet_exposure_limit_0 = st.session_state.config_ng_short_wallet_exposure_limit_0
                st.number_input("short_wallet_exposure_limit min", min_value=self.ng_short.WALLET_EXPOSURE_LIMIT_MIN, max_value=self.ng_short.wallet_exposure_limit_1, value=float(round(self.ng_short.wallet_exposure_limit_0,self.ng_short.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.ng_short.WALLET_EXPOSURE_LIMIT_STEP, format=self.ng_short.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_ng_short_wallet_exposure_limit_0", help=pbgui_help.wallet_exposure_limit)
                if  "config_ng_short_max_n_entry_orders_0" in st.session_state:
                    self.ng_short.max_n_entry_orders_0 = st.session_state.config_ng_short_max_n_entry_orders_0
                st.number_input("short_max_n_entry_orders min", min_value=self.ng_short.MAX_N_ENTRY_ORDERS_MIN, max_value=self.ng_short.max_n_entry_orders_1, value=self.ng_short.max_n_entry_orders_0, step=self.ng_short.MAX_N_ENTRY_ORDERS_STEP, format=self.ng_short.MAX_N_ENTRY_ORDERS_FORMAT, key="config_ng_short_max_n_entry_orders_0", help=pbgui_help.max_n_entry_orders)
                if  "config_ng_short_initial_qty_pct_0" in st.session_state:
                    self.ng_short.initial_qty_pct_0 = st.session_state.config_ng_short_initial_qty_pct_0
                st.number_input("short_initial_qty_pct min", min_value=self.ng_short.INITIAL_QTY_PCT_MIN, max_value=self.ng_short.initial_qty_pct_1, value=float(round(self.ng_short.initial_qty_pct_0,self.ng_short.INITIAL_QTY_PCT_ROUND)), step=self.ng_short.INITIAL_QTY_PCT_STEP, format=self.ng_short.INITIAL_QTY_PCT_FORMAT, key="config_ng_short_initial_qty_pct_0", help=pbgui_help.initial_qty_pct)
                if  "config_ng_short_initial_eprice_ema_dist_0" in st.session_state:
                    self.ng_short.initial_eprice_ema_dist_0 = st.session_state.config_ng_short_initial_eprice_ema_dist_0
                st.number_input("short_initial_eprice_ema_dist min", min_value=self.ng_short.INITIAL_EPRICE_EMA_DIST_MIN, max_value=self.ng_short.initial_eprice_ema_dist_1, value=float(round(self.ng_short.initial_eprice_ema_dist_0,self.ng_short.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.ng_short.INITIAL_EPRICE_EMA_DIST_STEP, format=self.ng_short.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_ng_short_initial_eprice_ema_dist_0", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_ng_short_eqty_exp_base_0" in st.session_state:
                    self.ng_short.eqty_exp_base_0 = st.session_state.config_ng_short_eqty_exp_base_0
                st.number_input("short_eqty_exp_base min", min_value=self.ng_short.EQTY_EXP_BASE_MIN, max_value=self.ng_short.eqty_exp_base_1, value=float(round(self.ng_short.eqty_exp_base_0,self.ng_short.EQTY_EXP_BASE_ROUND)), step=self.ng_short.EQTY_EXP_BASE_STEP, format=self.ng_short.EQTY_EXP_BASE_FORMAT, key="config_ng_short_eqty_exp_base_0", help=pbgui_help.eqty_exp_base)
                if  "config_ng_short_eprice_exp_base_0" in st.session_state:
                    self.ng_short.eprice_exp_base_0 = st.session_state.config_ng_short_eprice_exp_base_0
                st.number_input("short_eprice_exp_base min", min_value=self.ng_short.EPRICE_EXP_BASE_MIN, max_value=self.ng_short.eprice_exp_base_1, value=float(round(self.ng_short.eprice_exp_base_0,self.ng_short.EPRICE_EXP_BASE_ROUND)), step=self.ng_short.EPRICE_EXP_BASE_STEP, format=self.ng_short.EPRICE_EXP_BASE_FORMAT, key="config_ng_short_eprice_exp_base_0", help=pbgui_help.eprice_exp_base)
                if  "config_ng_short_min_markup_0" in st.session_state:
                    self.ng_short.min_markup_0 = st.session_state.config_ng_short_min_markup_0
                st.number_input("short_min_markup min", min_value=self.ng_short.MIN_MARKUP_MIN, max_value=self.ng_short.min_markup_1, value=float(round(self.ng_short.min_markup_0,self.ng_short.MIN_MARKUP_ROUND)), step=self.ng_short.MIN_MARKUP_STEP, format=self.ng_short.MIN_MARKUP_FORMAT, key="config_ng_short_min_markup_0", help=pbgui_help.min_markup)
                if  "config_ng_short_markup_range_0" in st.session_state:
                    self.ng_short.markup_range_0 = st.session_state.config_ng_short_markup_range_0
                st.number_input("short_markup_range min", min_value=self.ng_short.MARKUP_RANGE_MIN, max_value=self.ng_short.markup_range_1, value=float(round(self.ng_short.markup_range_0,self.ng_short.MARKUP_RANGE_ROUND)), step=self.ng_short.MARKUP_RANGE_STEP, format=self.ng_short.MARKUP_RANGE_FORMAT, key="config_ng_short_markup_range_0", help=pbgui_help.markup_range)
                if  "config_ng_short_n_close_orders_0" in st.session_state:
                    self.ng_short.n_close_orders_0 = st.session_state.config_ng_short_n_close_orders_0
                st.number_input("short_n_close_orders min", min_value=self.ng_short.N_CLOSE_ORDERS_MIN, max_value=self.ng_short.n_close_orders_1, value=self.ng_short.n_close_orders_0, step=self.ng_short.N_CLOSE_ORDERS_STEP, format=self.ng_short.N_CLOSE_ORDERS_FORMAT, key="config_ng_short_n_close_orders_0", help=pbgui_help.n_close_orders)
                if  "config_ng_short_auto_unstuck_wallet_exposure_threshold_0" in st.session_state:
                    self.ng_short.auto_unstuck_wallet_exposure_threshold_0 = st.session_state.config_ng_short_auto_unstuck_wallet_exposure_threshold_0
                st.number_input("short_auto_unstuck_wallet_exposure_threshold min", min_value=self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MIN, max_value=self.ng_short.auto_unstuck_wallet_exposure_threshold_1, value=float(round(self.ng_short.auto_unstuck_wallet_exposure_threshold_0,self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_ng_short_auto_unstuck_wallet_exposure_threshold_0", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_ng_short_auto_unstuck_ema_dist_0" in st.session_state:
                    self.ng_short.auto_unstuck_ema_dist_0 = st.session_state.config_ng_short_auto_unstuck_ema_dist_0
                st.number_input("short_auto_unstuck_ema_dist min", min_value=self.ng_short.AUTO_UNSTUCK_EMA_DIST_MIN, max_value=self.ng_short.auto_unstuck_ema_dist_1, value=float(round(self.ng_short.auto_unstuck_ema_dist_0,self.ng_short.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.ng_short.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.ng_short.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_ng_short_auto_unstuck_ema_dist_0", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_ng_short_auto_unstuck_delay_minutes_0" in st.session_state:
                    self.ng_short.auto_unstuck_delay_minutes_0 = st.session_state.config_ng_short_auto_unstuck_delay_minutes_0
                st.number_input("short_auto_unstuck_delay_minutes min", min_value=self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_MIN, max_value=self.ng_short.auto_unstuck_delay_minutes_1, value=float(round(self.ng_short.auto_unstuck_delay_minutes_0,self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_ng_short_auto_unstuck_delay_minutes_0", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_ng_short_auto_unstuck_qty_pct_0" in st.session_state:
                    self.ng_short.auto_unstuck_qty_pct_0 = st.session_state.config_ng_short_auto_unstuck_qty_pct_0
                st.number_input("short_auto_unstuck_qty_pct min", min_value=self.ng_short.AUTO_UNSTUCK_QTY_PCT_MIN, max_value=self.ng_short.auto_unstuck_qty_pct_1, value=float(round(self.ng_short.auto_unstuck_qty_pct_0,self.ng_short.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.ng_short.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.ng_short.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_ng_short_auto_unstuck_qty_pct_0", help=pbgui_help.auto_unstuck_qty_pct)
            with col4:
                if  "config_ng_short_grid_span_1" in st.session_state:
                    self.ng_short.grid_span_1 = st.session_state.config_ng_short_grid_span_1
                st.number_input("short_grid_span max", min_value=self.ng_short.grid_span_0, max_value=self.ng_short.GRID_SPAN_MAX, value=float(round(self.ng_short.grid_span_1,self.ng_short.GRID_SPAN_ROUND)), step=self.ng_short.GRID_SPAN_STEP, format=self.ng_short.GRID_SPAN_FORMAT, key="config_ng_short_grid_span_1", help=pbgui_help.grid_span)
                if  "config_ng_short_ema_span_0_1" in st.session_state:
                    self.ng_short.ema_span_0_1 = st.session_state.config_ng_short_ema_span_0_1
                st.number_input("short_ema_span_0 max", min_value=self.ng_short.ema_span_0_0, max_value=self.ng_short.EMA_SPAN_0_MAX, value=float(round(self.ng_short.ema_span_0_1,self.ng_short.EMA_SPAN_0_ROUND)), step=self.ng_short.EMA_SPAN_0_STEP, format=self.ng_short.EMA_SPAN_0_FORMAT, key="config_ng_short_ema_span_0_1", help=pbgui_help.ema_span)
                if  "config_ng_short_ema_span_1_1" in st.session_state:
                    self.ng_short.ema_span_1_1 = st.session_state.config_ng_short_ema_span_1_1
                st.number_input("short_ema_span_0 max", min_value=self.ng_short.ema_span_1_0, max_value=self.ng_short.EMA_SPAN_1_MAX, value=float(round(self.ng_short.ema_span_1_1,self.ng_short.EMA_SPAN_1_ROUND)), step=self.ng_short.EMA_SPAN_1_STEP, format=self.ng_short.EMA_SPAN_1_FORMAT, key="config_ng_short_ema_span_1_1", help=pbgui_help.ema_span)
                if  "config_ng_short_wallet_exposure_limit_1" in st.session_state:
                    self.ng_short.wallet_exposure_limit_1 = st.session_state.config_ng_short_wallet_exposure_limit_1
                st.number_input("short_wallet_exposure_limit max", min_value=self.ng_short.wallet_exposure_limit_0, max_value=self.ng_short.WALLET_EXPOSURE_LIMIT_MAX, value=float(round(self.ng_short.wallet_exposure_limit_1,self.ng_short.WALLET_EXPOSURE_LIMIT_ROUND)), step=self.ng_short.WALLET_EXPOSURE_LIMIT_STEP, format=self.ng_short.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_ng_short_wallet_exposure_limit_1", help=pbgui_help.wallet_exposure_limit)
                if  "config_ng_short_max_n_entry_orders_1" in st.session_state:
                    self.ng_short.max_n_entry_orders_1 = st.session_state.config_ng_short_max_n_entry_orders_1
                st.number_input("short_max_n_entry_orders max", min_value=self.ng_short.max_n_entry_orders_1, max_value=self.ng_short.MAX_N_ENTRY_ORDERS_MAX, value=self.ng_short.max_n_entry_orders_1, step=self.ng_short.MAX_N_ENTRY_ORDERS_STEP, format=self.ng_short.MAX_N_ENTRY_ORDERS_FORMAT, key="config_ng_short_max_n_entry_orders_1", help=pbgui_help.max_n_entry_orders)
                if  "config_ng_short_initial_qty_pct_1" in st.session_state:
                    self.ng_short.initial_qty_pct_1 = st.session_state.config_ng_short_initial_qty_pct_1
                st.number_input("short_initial_qty_pct max", min_value=self.ng_short.initial_qty_pct_0, max_value=self.ng_short.INITIAL_QTY_PCT_MAX, value=float(round(self.ng_short.initial_qty_pct_1,self.ng_short.INITIAL_QTY_PCT_ROUND)), step=self.ng_short.INITIAL_QTY_PCT_STEP, format=self.ng_short.INITIAL_QTY_PCT_FORMAT, key="config_ng_short_initial_qty_pct_1", help=pbgui_help.initial_qty_pct)
                if  "config_ng_short_initial_eprice_ema_dist_1" in st.session_state:
                    self.ng_short.initial_eprice_ema_dist_1 = st.session_state.config_ng_short_initial_eprice_ema_dist_1
                st.number_input("short_initial_eprice_ema_dist max", min_value=self.ng_short.initial_eprice_ema_dist_0, max_value=self.ng_short.INITIAL_EPRICE_EMA_DIST_MAX, value=float(round(self.ng_short.initial_eprice_ema_dist_1,self.ng_short.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.ng_short.INITIAL_EPRICE_EMA_DIST_STEP, format=self.ng_short.INITIAL_EPRICE_EMA_DIST_FORMAT, key="config_ng_short_initial_eprice_ema_dist_1", help=pbgui_help.initial_eprice_ema_dist)
                if  "config_ng_short_eqty_exp_base_1" in st.session_state:
                    self.ng_short.eqty_exp_base_1 = st.session_state.config_ng_short_eqty_exp_base_1
                st.number_input("short_eqty_exp_base max", min_value=self.ng_short.eqty_exp_base_0, max_value=self.ng_short.EQTY_EXP_BASE_MAX, value=float(round(self.ng_short.eqty_exp_base_1,self.ng_short.EQTY_EXP_BASE_ROUND)), step=self.ng_short.EQTY_EXP_BASE_STEP, format=self.ng_short.EQTY_EXP_BASE_FORMAT, key="config_ng_short_eqty_exp_base_1", help=pbgui_help.eqty_exp_base)
                if  "config_ng_short_eprice_exp_base_1" in st.session_state:
                    self.ng_short.eprice_exp_base_1 = st.session_state.config_ng_short_eprice_exp_base_1
                st.number_input("short_eprice_exp_base max", min_value=self.ng_short.eprice_exp_base_0, max_value=self.ng_short.EPRICE_EXP_BASE_MAX, value=float(round(self.ng_short.eprice_exp_base_1,self.ng_short.EPRICE_EXP_BASE_ROUND)), step=self.ng_short.EPRICE_EXP_BASE_STEP, format=self.ng_short.EPRICE_EXP_BASE_FORMAT, key="config_ng_short_eprice_exp_base_1", help=pbgui_help.eprice_exp_base)
                if  "config_ng_short_min_markup_1" in st.session_state:
                    self.ng_short.min_markup_1 = st.session_state.config_ng_short_min_markup_1
                st.number_input("short_min_markup max", min_value=self.ng_short.min_markup_0, max_value=self.ng_short.MIN_MARKUP_MAX, value=float(round(self.ng_short.min_markup_1,self.ng_short.MIN_MARKUP_ROUND)), step=self.ng_short.MIN_MARKUP_STEP, format=self.ng_short.MIN_MARKUP_FORMAT, key="config_ng_short_min_markup_1", help=pbgui_help.min_markup)
                if  "config_ng_short_markup_range_1" in st.session_state:
                    self.ng_short.markup_range_1 = st.session_state.config_ng_short_markup_range_1
                st.number_input("short_markup_range max", min_value=self.ng_short.markup_range_0, max_value=self.ng_short.MARKUP_RANGE_MAX, value=float(round(self.ng_short.markup_range_1,self.ng_short.MARKUP_RANGE_ROUND)), step=self.ng_short.MARKUP_RANGE_STEP, format=self.ng_short.MARKUP_RANGE_FORMAT, key="config_ng_short_markup_range_1", help=pbgui_help.markup_range)
                if  "config_ng_short_n_close_orders_1" in st.session_state:
                    self.ng_short.n_close_orders_1 = st.session_state.config_ng_short_n_close_orders_1
                st.number_input("short_n_close_orders max", min_value=self.ng_short.n_close_orders_1, max_value=self.ng_short.N_CLOSE_ORDERS_MAX, value=self.ng_short.n_close_orders_1, step=self.ng_short.N_CLOSE_ORDERS_STEP, format=self.ng_short.N_CLOSE_ORDERS_FORMAT, key="config_ng_short_n_close_orders_1", help=pbgui_help.n_close_orders)
                if  "config_ng_short_auto_unstuck_wallet_exposure_threshold_1" in st.session_state:
                    self.ng_short.auto_unstuck_wallet_exposure_threshold_1 = st.session_state.config_ng_short_auto_unstuck_wallet_exposure_threshold_1
                st.number_input("short_auto_unstuck_wallet_exposure_threshold max", min_value=self.ng_short.auto_unstuck_wallet_exposure_threshold_0, max_value=self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_MAX, value=float(round(self.ng_short.auto_unstuck_wallet_exposure_threshold_1,self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_ROUND)), step=self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_STEP, format=self.ng_short.AUTO_UNSTUCK_WALLET_EXPOSURE_THRESHOLD_FORMAT, key="config_ng_short_auto_unstuck_wallet_exposure_threshold_1", help=pbgui_help.auto_unstuck_wallet_exposure_threshold)
                if  "config_ng_short_auto_unstuck_ema_dist_1" in st.session_state:
                    self.ng_short.auto_unstuck_ema_dist_1 = st.session_state.config_ng_short_auto_unstuck_ema_dist_1
                st.number_input("short_auto_unstuck_ema_dist max", min_value=self.ng_short.auto_unstuck_ema_dist_0, max_value=self.ng_short.AUTO_UNSTUCK_EMA_DIST_MAX, value=float(round(self.ng_short.auto_unstuck_ema_dist_1,self.ng_short.AUTO_UNSTUCK_EMA_DIST_ROUND)), step=self.ng_short.AUTO_UNSTUCK_EMA_DIST_STEP, format=self.ng_short.AUTO_UNSTUCK_EMA_DIST_FORMAT, key="config_ng_short_auto_unstuck_ema_dist_1", help=pbgui_help.auto_unstuck_ema_dist)
                if  "config_ng_short_auto_unstuck_delay_minutes_1" in st.session_state:
                    self.ng_short.auto_unstuck_delay_minutes_1 = st.session_state.config_ng_short_auto_unstuck_delay_minutes_1
                st.number_input("short_auto_unstuck_delay_minutes max", min_value=self.ng_short.auto_unstuck_delay_minutes_0, max_value=self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_MAX, value=float(round(self.ng_short.auto_unstuck_delay_minutes_1,self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_ROUND)), step=self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_STEP, format=self.ng_short.AUTO_UNSTUCK_DELAY_MINUTES_FORMAT, key="config_ng_short_auto_unstuck_delay_minutes_1", help=pbgui_help.auto_unstuck_delay_minutes)
                if  "config_ng_short_auto_unstuck_qty_pct_1" in st.session_state:
                    self.ng_short.auto_unstuck_qty_pct_1 = st.session_state.config_ng_short_auto_unstuck_qty_pct_1
                st.number_input("short_auto_unstuck_qty_pct max", min_value=self.ng_short.auto_unstuck_qty_pct_0, max_value=self.ng_short.AUTO_UNSTUCK_QTY_PCT_MAX, value=float(round(self.ng_short.auto_unstuck_qty_pct_1,self.ng_short.AUTO_UNSTUCK_QTY_PCT_ROUND)), step=self.ng_short.AUTO_UNSTUCK_QTY_PCT_STEP, format=self.ng_short.AUTO_UNSTUCK_QTY_PCT_FORMAT, key="config_ng_short_auto_unstuck_qty_pct_1", help=pbgui_help.auto_unstuck_qty_pct)
        with st.expander("clock", expanded = True if self.passivbot_mode == "clock" else False):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                if  "config_cl_long_ema_span_0_0" in st.session_state:
                    self.cl_long.ema_span_0_0 = st.session_state.config_cl_long_ema_span_0_0
                st.number_input("long_ema_span_0 min", value=float(round(self.cl_long.ema_span_0_0,self.cl_long.EMA_SPAN_0_ROUND)), min_value=self.cl_long.EMA_SPAN_0_MIN, max_value=self.cl_long.ema_span_0_1, step=self.cl_long.EMA_SPAN_0_STEP, format=self.cl_long.EMA_SPAN_0_FORMAT, key="config_cl_long_ema_span_0_0", help=pbgui_help.ema_span)
                if  "config_cl_long_ema_span_1_0" in st.session_state:
                    self.cl_long.ema_span_1_0 = st.session_state.config_cl_long_ema_span_1_0
                st.number_input("long_ema_span_1 min", value=float(round(self.cl_long.ema_span_1_0,self.cl_long.EMA_SPAN_1_ROUND)), min_value=self.cl_long.EMA_SPAN_1_MIN, max_value=self.cl_long.ema_span_1_1, step=self.cl_long.EMA_SPAN_1_STEP, format=self.cl_long.EMA_SPAN_1_FORMAT, key="config_cl_long_ema_span_1_0", help=pbgui_help.ema_span)
                if  "config_cl_long_ema_dist_entry_0" in st.session_state:
                    self.cl_long.ema_dist_entry_0 = st.session_state.config_cl_long_ema_dist_entry_0
                st.number_input("long_ema_dist_entry min", value=float(round(self.cl_long.ema_dist_entry_0,self.cl_long.EMA_DIST_ENTRY_ROUND)), min_value=self.cl_long.EMA_DIST_ENTRY_MIN, max_value=self.cl_long.ema_dist_entry_1, step=self.cl_long.EMA_DIST_ENTRY_STEP, format=self.cl_long.EMA_DIST_ENTRY_FORMAT, key="config_cl_long_ema_dist_entry_0", help=pbgui_help.ema_dist)
                if  "config_cl_long_ema_dist_close_0" in st.session_state:
                    self.cl_long.ema_dist_close_0 = st.session_state.config_cl_long_ema_dist_close_0
                st.number_input("long_ema_dist_close min", value=float(round(self.cl_long.ema_dist_close_0,self.cl_long.EMA_DIST_CLOSE_ROUND)), min_value=self.cl_long.EMA_DIST_CLOSE_MIN, max_value=self.cl_long.ema_dist_close_1, step=self.cl_long.EMA_DIST_CLOSE_STEP, format=self.cl_long.EMA_DIST_CLOSE_FORMAT, key="config_cl_long_ema_dist_close_0", help=pbgui_help.ema_dist)
                if  "config_cl_long_qty_pct_entry_0" in st.session_state:
                    self.cl_long.qty_pct_entry_0 = st.session_state.config_cl_long_qty_pct_entry_0
                st.number_input("long_qty_pct_entry min", value=float(round(self.cl_long.qty_pct_entry_0,self.cl_long.QTY_PCT_ENTRY_ROUND)), min_value=self.cl_long.QTY_PCT_ENTRY_MIN, max_value=self.cl_long.qty_pct_entry_1, step=self.cl_long.QTY_PCT_ENTRY_STEP, format=self.cl_long.QTY_PCT_ENTRY_FORMAT, key="config_cl_long_qty_pct_entry_0", help=pbgui_help.qty_pct)
                if  "config_cl_long_qty_pct_close_0" in st.session_state:
                    self.cl_long.qty_pct_close_0 = st.session_state.config_cl_long_qty_pct_close_0
                st.number_input("long_qty_pct_close min", value=float(round(self.cl_long.qty_pct_close_0,self.cl_long.QTY_PCT_CLOSE_ROUND)), min_value=self.cl_long.QTY_PCT_CLOSE_MIN, max_value=self.cl_long.qty_pct_close_1, step=self.cl_long.QTY_PCT_CLOSE_STEP, format=self.cl_long.QTY_PCT_CLOSE_FORMAT, key="config_cl_long_qty_pct_close_0", help=pbgui_help.qty_pct)
                if  "config_cl_long_we_multiplier_entry_0" in st.session_state:
                    self.cl_long.we_multiplier_entry_0 = st.session_state.config_cl_long_we_multiplier_entry_0
                st.number_input("long_we_multiplier_entry min", value=float(round(self.cl_long.we_multiplier_entry_0,self.cl_long.WE_MULTIPLIER_ENTRY_ROUND)), min_value=self.cl_long.WE_MULTIPLIER_ENTRY_MIN, max_value=self.cl_long.we_multiplier_entry_1, step=self.cl_long.WE_MULTIPLIER_ENTRY_STEP, format=self.cl_long.WE_MULTIPLIER_ENTRY_FORMAT, key="config_cl_long_we_multiplier_entry_0", help=pbgui_help.we_multiplier)
                if  "config_cl_long_we_multiplier_close_0" in st.session_state:
                    self.cl_long.we_multiplier_close_0 = st.session_state.config_cl_long_we_multiplier_close_0
                st.number_input("long_we_multiplier_close min", value=float(round(self.cl_long.we_multiplier_close_0,self.cl_long.WE_MULTIPLIER_CLOSE_ROUND)), min_value=self.cl_long.WE_MULTIPLIER_CLOSE_MIN, max_value=self.cl_long.we_multiplier_close_1, step=self.cl_long.WE_MULTIPLIER_CLOSE_STEP, format=self.cl_long.WE_MULTIPLIER_CLOSE_FORMAT, key="config_cl_long_we_multiplier_close_0", help=pbgui_help.we_multiplier)
                if  "config_cl_long_delay_weight_entry_0" in st.session_state:
                    self.cl_long.delay_weight_entry_0 = st.session_state.config_cl_long_delay_weight_entry_0
                st.number_input("long_delay_weight_entry min", value=float(round(self.cl_long.delay_weight_entry_0,self.cl_long.DELAY_WEIGHT_ENTRY_ROUND)), min_value=self.cl_long.DELAY_WEIGHT_ENTRY_MIN, max_value=self.cl_long.delay_weight_entry_1, step=self.cl_long.DELAY_WEIGHT_ENTRY_STEP, format=self.cl_long.DELAY_WEIGHT_ENTRY_FORMAT, key="config_cl_long_delay_weight_entry_0", help=pbgui_help.delay_weight)
                if  "config_cl_long_delay_weight_close_0" in st.session_state:
                    self.cl_long.delay_weight_close_0 = st.session_state.config_cl_long_delay_weight_close_0
                st.number_input("long_delay_weight_close min", value=float(round(self.cl_long.delay_weight_close_0,self.cl_long.DELAY_WEIGHT_CLOSE_ROUND)), min_value=self.cl_long.DELAY_WEIGHT_CLOSE_MIN, max_value=self.cl_long.delay_weight_close_1, step=self.cl_long.DELAY_WEIGHT_CLOSE_STEP, format=self.cl_long.DELAY_WEIGHT_CLOSE_FORMAT, key="config_cl_long_delay_weight_close_0", help=pbgui_help.delay_weight)
                if  "config_cl_long_delay_between_fills_minutes_entry_0" in st.session_state:
                    self.cl_long.delay_between_fills_minutes_entry_0 = st.session_state.config_cl_long_delay_between_fills_minutes_entry_0
                st.number_input("long_delay_between_fills_minutes_entry min", value=float(round(self.cl_long.delay_between_fills_minutes_entry_0,self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_ROUND)), min_value=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MIN, max_value=self.cl_long.delay_between_fills_minutes_entry_1, step=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_STEP, format=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_FORMAT, key="config_cl_long_delay_between_fills_minutes_entry_0", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_long_delay_between_fills_minutes_close_0" in st.session_state:
                    self.cl_long.delay_between_fills_minutes_close_0 = st.session_state.config_cl_long_delay_between_fills_minutes_close_0
                st.number_input("long_delay_between_fills_minutes_close min", value=float(round(self.cl_long.delay_between_fills_minutes_close_0,self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_ROUND)), min_value=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MIN, max_value=self.cl_long.delay_between_fills_minutes_close_1, step=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_STEP, format=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_FORMAT, key="config_cl_long_delay_between_fills_minutes_close_0", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_long_min_markup_0" in st.session_state:
                    self.cl_long.min_markup_0 = st.session_state.config_cl_long_min_markup_0
                st.number_input("long_min_markup min", value=float(round(self.cl_long.min_markup_0,self.cl_long.MIN_MARKUP_ROUND)), min_value=self.cl_long.MIN_MARKUP_MIN, max_value=self.cl_long.min_markup_1, step=self.cl_long.MIN_MARKUP_STEP, format=self.cl_long.MIN_MARKUP_FORMAT, key="config_cl_long_min_markup_0", help=pbgui_help.min_markup)
                if  "config_cl_long_markup_range_0" in st.session_state:
                    self.cl_long.markup_range_0 = st.session_state.config_cl_long_markup_range_0
                st.number_input("long_markup_range min", value=float(round(self.cl_long.markup_range_0,self.cl_long.MARKUP_RANGE_ROUND)), min_value=self.cl_long.MARKUP_RANGE_MIN, max_value=self.cl_long.markup_range_1, step=self.cl_long.MARKUP_RANGE_STEP, format=self.cl_long.MARKUP_RANGE_FORMAT, key="config_cl_long_markup_range_0", help=pbgui_help.markup_range)
                if  "config_cl_long_n_close_orders_0" in st.session_state:
                    self.cl_long.n_close_orders_0 = st.session_state.config_cl_long_n_close_orders_0
                st.number_input("long_n_close_orders min", value=self.cl_long.n_close_orders_0, min_value=self.cl_long.N_CLOSE_ORDERS_MIN, max_value=self.cl_long.n_close_orders_1, step=self.cl_long.N_CLOSE_ORDERS_STEP, format=self.cl_long.N_CLOSE_ORDERS_FORMAT, key="config_cl_long_n_close_orders_0", help=pbgui_help.n_close_orders)
                if  "config_cl_long_wallet_exposure_limit_0" in st.session_state:
                    self.cl_long.wallet_exposure_limit_0 = st.session_state.config_cl_long_wallet_exposure_limit_0
                st.number_input("long_wallet_exposure_limit min", value=float(round(self.cl_long.wallet_exposure_limit_0, self.cl_long.WALLET_EXPOSURE_LIMIT_ROUND)), min_value=self.cl_long.WALLET_EXPOSURE_LIMIT_MIN, max_value=self.cl_long.wallet_exposure_limit_1, step=self.cl_long.WALLET_EXPOSURE_LIMIT_STEP, format=self.cl_long.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_cl_long_wallet_exposure_limit_0", help=pbgui_help.wallet_exposure_limit)
            with col2:
                if  "config_cl_long_ema_span_0_1" in st.session_state:
                    self.cl_long.ema_span_0_1 = st.session_state.config_cl_long_ema_span_0_1
                st.number_input("long_ema_span_0 max", value=float(round(self.cl_long.ema_span_0_1,self.cl_long.EMA_SPAN_0_ROUND)), min_value=self.cl_long.ema_span_0_0, max_value=self.cl_long.EMA_SPAN_0_MAX, step=self.cl_long.EMA_SPAN_0_STEP, format=self.cl_long.EMA_SPAN_0_FORMAT, key="config_cl_long_ema_span_0_1", help=pbgui_help.ema_span)
                if  "config_cl_long_ema_span_1_1" in st.session_state:
                    self.cl_long.ema_span_1_1 = st.session_state.config_cl_long_ema_span_1_1
                st.number_input("long_ema_span_1 max", value=float(round(self.cl_long.ema_span_1_1,self.cl_long.EMA_SPAN_1_ROUND)), min_value=self.cl_long.ema_span_1_0, max_value=self.cl_long.EMA_SPAN_1_MAX, step=self.cl_long.EMA_SPAN_1_STEP, format=self.cl_long.EMA_SPAN_1_FORMAT, key="config_cl_long_ema_span_1_1", help=pbgui_help.ema_span)
                if  "config_cl_long_ema_dist_entry_1" in st.session_state:
                    self.cl_long.ema_dist_entry_1 = st.session_state.config_cl_long_ema_dist_entry_1
                st.number_input("long_ema_dist_entry max", value=float(round(self.cl_long.ema_dist_entry_1,self.cl_long.EMA_DIST_ENTRY_ROUND)), min_value=self.cl_long.ema_dist_entry_0, max_value=self.cl_long.EMA_DIST_ENTRY_MAX, step=self.cl_long.EMA_DIST_ENTRY_STEP, format=self.cl_long.EMA_DIST_ENTRY_FORMAT, key="config_cl_long_ema_dist_entry_1", help=pbgui_help.ema_dist)
                if  "config_cl_long_ema_dist_close_1" in st.session_state:
                    self.cl_long.ema_dist_close_1 = st.session_state.config_cl_long_ema_dist_close_1
                st.number_input("long_ema_dist_close max", value=float(round(self.cl_long.ema_dist_close_1,self.cl_long.EMA_DIST_CLOSE_ROUND)), min_value=self.cl_long.ema_dist_close_0, max_value=self.cl_long.EMA_DIST_CLOSE_MAX, step=self.cl_long.EMA_DIST_CLOSE_STEP, format=self.cl_long.EMA_DIST_CLOSE_FORMAT, key="config_cl_long_ema_dist_close_1", help=pbgui_help.ema_dist)
                if  "config_cl_long_qty_pct_entry_1" in st.session_state:
                    self.cl_long.qty_pct_entry_1 = st.session_state.config_cl_long_qty_pct_entry_1
                st.number_input("long_qty_pct_entry max", value=float(round(self.cl_long.qty_pct_entry_1,self.cl_long.QTY_PCT_ENTRY_ROUND)), min_value=self.cl_long.qty_pct_entry_0, max_value=self.cl_long.QTY_PCT_ENTRY_MAX, step=self.cl_long.QTY_PCT_ENTRY_STEP, format=self.cl_long.QTY_PCT_ENTRY_FORMAT, key="config_cl_long_qty_pct_entry_1", help=pbgui_help.qty_pct)
                if  "config_cl_long_qty_pct_close_1" in st.session_state:
                    self.cl_long.qty_pct_close_1 = st.session_state.config_cl_long_qty_pct_close_1
                st.number_input("long_qty_pct_close max", value=float(round(self.cl_long.qty_pct_close_1,self.cl_long.QTY_PCT_CLOSE_ROUND)), min_value=self.cl_long.qty_pct_close_0, max_value=self.cl_long.QTY_PCT_CLOSE_MAX, step=self.cl_long.QTY_PCT_CLOSE_STEP, format=self.cl_long.QTY_PCT_CLOSE_FORMAT, key="config_cl_long_qty_pct_close_1", help=pbgui_help.qty_pct)
                if  "config_cl_long_we_multiplier_entry_1" in st.session_state:
                    self.cl_long.we_multiplier_entry_1 = st.session_state.config_cl_long_we_multiplier_entry_1
                st.number_input("long_we_multiplier_entry max", value=float(round(self.cl_long.we_multiplier_entry_1,self.cl_long.WE_MULTIPLIER_ENTRY_ROUND)), min_value=self.cl_long.we_multiplier_entry_0, max_value=self.cl_long.WE_MULTIPLIER_ENTRY_MAX, step=self.cl_long.WE_MULTIPLIER_ENTRY_STEP, format=self.cl_long.WE_MULTIPLIER_ENTRY_FORMAT, key="config_cl_long_we_multiplier_entry_1", help=pbgui_help.we_multiplier)
                if  "config_cl_long_we_multiplier_close_1" in st.session_state:
                    self.cl_long.we_multiplier_close_1 = st.session_state.config_cl_long_we_multiplier_close_1
                st.number_input("long_we_multiplier_close max", value=float(round(self.cl_long.we_multiplier_close_1,self.cl_long.WE_MULTIPLIER_CLOSE_ROUND)), min_value=self.cl_long.we_multiplier_close_0, max_value=self.cl_long.WE_MULTIPLIER_CLOSE_MAX, step=self.cl_long.WE_MULTIPLIER_CLOSE_STEP, format=self.cl_long.WE_MULTIPLIER_CLOSE_FORMAT, key="config_cl_long_we_multiplier_close_1", help=pbgui_help.we_multiplier)
                if  "config_cl_long_delay_weight_entry_1" in st.session_state:
                    self.cl_long.delay_weight_entry_1 = st.session_state.config_cl_long_delay_weight_entry_1
                st.number_input("long_delay_weight_entry max", value=float(round(self.cl_long.delay_weight_entry_1,self.cl_long.DELAY_WEIGHT_ENTRY_ROUND)), min_value=self.cl_long.delay_weight_entry_0, max_value=self.cl_long.DELAY_WEIGHT_ENTRY_MAX, step=self.cl_long.DELAY_WEIGHT_ENTRY_STEP, format=self.cl_long.DELAY_WEIGHT_ENTRY_FORMAT, key="config_cl_long_delay_weight_entry_1", help=pbgui_help.delay_weight)
                if  "config_cl_long_delay_weight_close_1" in st.session_state:
                    self.cl_long.delay_weight_close_1 = st.session_state.config_cl_long_delay_weight_close_1
                st.number_input("long_delay_weight_close max", value=float(round(self.cl_long.delay_weight_close_1,self.cl_long.DELAY_WEIGHT_CLOSE_ROUND)), min_value=self.cl_long.delay_weight_close_0, max_value=self.cl_long.DELAY_WEIGHT_CLOSE_MAX, step=self.cl_long.DELAY_WEIGHT_CLOSE_STEP, format=self.cl_long.DELAY_WEIGHT_CLOSE_FORMAT, key="config_cl_long_delay_weight_close_1", help=pbgui_help.delay_weight)
                if  "config_cl_long_delay_between_fills_minutes_entry_1" in st.session_state:
                    self.cl_long.delay_between_fills_minutes_entry_1 = st.session_state.config_cl_long_delay_between_fills_minutes_entry_1
                st.number_input("long_delay_between_fills_minutes_entry max", value=float(round(self.cl_long.delay_between_fills_minutes_entry_1,self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_ROUND)), min_value=self.cl_long.delay_between_fills_minutes_entry_0, max_value=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MAX, step=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_STEP, format=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_FORMAT, key="config_cl_long_delay_between_fills_minutes_entry_1", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_long_delay_between_fills_minutes_close_1" in st.session_state:
                    self.cl_long.delay_between_fills_minutes_close_1 = st.session_state.config_cl_long_delay_between_fills_minutes_close_1
                st.number_input("long_delay_between_fills_minutes_close max", value=float(round(self.cl_long.delay_between_fills_minutes_close_1,self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_ROUND)), min_value=self.cl_long.delay_between_fills_minutes_close_0, max_value=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MAX, step=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_STEP, format=self.cl_long.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_FORMAT, key="config_cl_long_delay_between_fills_minutes_close_1", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_long_min_markup_1" in st.session_state:
                    self.cl_long.min_markup_1 = st.session_state.config_cl_long_min_markup_1
                st.number_input("long_min_markup max", value=float(round(self.cl_long.min_markup_1,self.cl_long.MIN_MARKUP_ROUND)), min_value=self.cl_long.min_markup_0, max_value=self.cl_long.MIN_MARKUP_MAX, step=self.cl_long.MIN_MARKUP_STEP, format=self.cl_long.MIN_MARKUP_FORMAT, key="config_cl_long_min_markup_1", help=pbgui_help.min_markup)
                if  "config_cl_long_markup_range_1" in st.session_state:
                    self.cl_long.markup_range_1 = st.session_state.config_cl_long_markup_range_1
                st.number_input("long_markup_range max", value=float(round(self.cl_long.markup_range_1,self.cl_long.MARKUP_RANGE_ROUND)), min_value=self.cl_long.markup_range_0, max_value=self.cl_long.MARKUP_RANGE_MAX, step=self.cl_long.MARKUP_RANGE_STEP, format=self.cl_long.MARKUP_RANGE_FORMAT, key="config_cl_long_markup_range_1", help=pbgui_help.markup_range)
                if  "config_cl_long_n_close_orders_1" in st.session_state:
                    self.cl_long.n_close_orders_1 = st.session_state.config_cl_long_n_close_orders_1
                st.number_input("long_n_close_orders max", value=self.cl_long.n_close_orders_1, min_value=self.cl_long.n_close_orders_1, max_value=self.cl_long.N_CLOSE_ORDERS_MAX, step=self.cl_long.N_CLOSE_ORDERS_STEP, format=self.cl_long.N_CLOSE_ORDERS_FORMAT, key="config_cl_long_n_close_orders_1", help=pbgui_help.n_close_orders)
                if  "config_cl_long_wallet_exposure_limit_1" in st.session_state:
                    self.cl_long.wallet_exposure_limit_1 = st.session_state.config_cl_long_wallet_exposure_limit_1
                st.number_input("long_wallet_exposure_limit max", value=float(round(self.cl_long.wallet_exposure_limit_1,self.cl_long.WALLET_EXPOSURE_LIMIT_ROUND)), min_value=self.cl_long.wallet_exposure_limit_0, max_value=self.cl_long.WALLET_EXPOSURE_LIMIT_MAX, step=self.cl_long.WALLET_EXPOSURE_LIMIT_STEP, format=self.cl_long.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_cl_long_wallet_exposure_limit_1", help=pbgui_help.wallet_exposure_limit)
            with col3:
                if  "config_cl_short_ema_span_0_0" in st.session_state:
                    self.cl_short.ema_span_0_0 = st.session_state.config_cl_short_ema_span_0_0
                st.number_input("short_ema_span_0 min", value=float(round(self.cl_short.ema_span_0_0,self.cl_short.EMA_SPAN_0_ROUND)), min_value=self.cl_short.EMA_SPAN_0_MIN, max_value=self.cl_short.ema_span_0_1, step=self.cl_short.EMA_SPAN_0_STEP, format=self.cl_short.EMA_SPAN_0_FORMAT, key="config_cl_short_ema_span_0_0", help=pbgui_help.ema_span)
                if  "config_cl_short_ema_span_1_0" in st.session_state:
                    self.cl_short.ema_span_1_0 = st.session_state.config_cl_short_ema_span_1_0
                st.number_input("short_ema_span_1 min", value=float(round(self.cl_short.ema_span_1_0,self.cl_short.EMA_SPAN_1_ROUND)), min_value=self.cl_short.EMA_SPAN_1_MIN, max_value=self.cl_short.ema_span_1_1, step=self.cl_short.EMA_SPAN_1_STEP, format=self.cl_short.EMA_SPAN_1_FORMAT, key="config_cl_short_ema_span_1_0", help=pbgui_help.ema_span)
                if  "config_cl_short_ema_dist_entry_0" in st.session_state:
                    self.cl_short.ema_dist_entry_0 = st.session_state.config_cl_short_ema_dist_entry_0
                st.number_input("short_ema_dist_entry min", value=float(round(self.cl_short.ema_dist_entry_0,self.cl_short.EMA_DIST_ENTRY_ROUND)), min_value=self.cl_short.EMA_DIST_ENTRY_MIN, max_value=self.cl_short.ema_dist_entry_1, step=self.cl_short.EMA_DIST_ENTRY_STEP, format=self.cl_short.EMA_DIST_ENTRY_FORMAT, key="config_cl_short_ema_dist_entry_0", help=pbgui_help.ema_dist)
                if  "config_cl_short_ema_dist_close_0" in st.session_state:
                    self.cl_short.ema_dist_close_0 = st.session_state.config_cl_short_ema_dist_close_0
                st.number_input("short_ema_dist_close min", value=float(round(self.cl_short.ema_dist_close_0,self.cl_short.EMA_DIST_CLOSE_ROUND)), min_value=self.cl_short.EMA_DIST_CLOSE_MIN, max_value=self.cl_short.ema_dist_close_1, step=self.cl_short.EMA_DIST_CLOSE_STEP, format=self.cl_short.EMA_DIST_CLOSE_FORMAT, key="config_cl_short_ema_dist_close_0", help=pbgui_help.ema_dist)
                if  "config_cl_short_qty_pct_entry_0" in st.session_state:
                    self.cl_short.qty_pct_entry_0 = st.session_state.config_cl_short_qty_pct_entry_0
                st.number_input("short_qty_pct_entry min", value=float(round(self.cl_short.qty_pct_entry_0,self.cl_short.QTY_PCT_ENTRY_ROUND)), min_value=self.cl_short.QTY_PCT_ENTRY_MIN, max_value=self.cl_short.qty_pct_entry_1, step=self.cl_short.QTY_PCT_ENTRY_STEP, format=self.cl_short.QTY_PCT_ENTRY_FORMAT, key="config_cl_short_qty_pct_entry_0", help=pbgui_help.qty_pct)
                if  "config_cl_short_qty_pct_close_0" in st.session_state:
                    self.cl_short.qty_pct_close_0 = st.session_state.config_cl_short_qty_pct_close_0
                st.number_input("short_qty_pct_close min", value=float(round(self.cl_short.qty_pct_close_0,self.cl_short.QTY_PCT_CLOSE_ROUND)), min_value=self.cl_short.QTY_PCT_CLOSE_MIN, max_value=self.cl_short.qty_pct_close_1, step=self.cl_short.QTY_PCT_CLOSE_STEP, format=self.cl_short.QTY_PCT_CLOSE_FORMAT, key="config_cl_short_qty_pct_close_0", help=pbgui_help.qty_pct)
                if  "config_cl_short_we_multiplier_entry_0" in st.session_state:
                    self.cl_short.we_multiplier_entry_0 = st.session_state.config_cl_short_we_multiplier_entry_0
                st.number_input("short_we_multiplier_entry min", value=float(round(self.cl_short.we_multiplier_entry_0,self.cl_short.WE_MULTIPLIER_ENTRY_ROUND)), min_value=self.cl_short.WE_MULTIPLIER_ENTRY_MIN, max_value=self.cl_short.we_multiplier_entry_1, step=self.cl_short.WE_MULTIPLIER_ENTRY_STEP, format=self.cl_short.WE_MULTIPLIER_ENTRY_FORMAT, key="config_cl_short_we_multiplier_entry_0", help=pbgui_help.we_multiplier)
                if  "config_cl_short_we_multiplier_close_0" in st.session_state:
                    self.cl_short.we_multiplier_close_0 = st.session_state.config_cl_short_we_multiplier_close_0
                st.number_input("short_we_multiplier_close min", value=float(round(self.cl_short.we_multiplier_close_0,self.cl_short.WE_MULTIPLIER_CLOSE_ROUND)), min_value=self.cl_short.WE_MULTIPLIER_CLOSE_MIN, max_value=self.cl_short.we_multiplier_close_1, step=self.cl_short.WE_MULTIPLIER_CLOSE_STEP, format=self.cl_short.WE_MULTIPLIER_CLOSE_FORMAT, key="config_cl_short_we_multiplier_close_0", help=pbgui_help.we_multiplier)
                if  "config_cl_short_delay_weight_entry_0" in st.session_state:
                    self.cl_short.delay_weight_entry_0 = st.session_state.config_cl_short_delay_weight_entry_0
                st.number_input("short_delay_weight_entry min", value=float(round(self.cl_short.delay_weight_entry_0,self.cl_short.DELAY_WEIGHT_ENTRY_ROUND)), min_value=self.cl_short.DELAY_WEIGHT_ENTRY_MIN, max_value=self.cl_short.delay_weight_entry_1, step=self.cl_short.DELAY_WEIGHT_ENTRY_STEP, format=self.cl_short.DELAY_WEIGHT_ENTRY_FORMAT, key="config_cl_short_delay_weight_entry_0", help=pbgui_help.delay_weight)
                if  "config_cl_short_delay_weight_close_0" in st.session_state:
                    self.cl_short.delay_weight_close_0 = st.session_state.config_cl_short_delay_weight_close_0
                st.number_input("short_delay_weight_close min", value=float(round(self.cl_short.delay_weight_close_0,self.cl_short.DELAY_WEIGHT_CLOSE_ROUND)), min_value=self.cl_short.DELAY_WEIGHT_CLOSE_MIN, max_value=self.cl_short.delay_weight_close_1, step=self.cl_short.DELAY_WEIGHT_CLOSE_STEP, format=self.cl_short.DELAY_WEIGHT_CLOSE_FORMAT, key="config_cl_short_delay_weight_close_0", help=pbgui_help.delay_weight)
                if  "config_cl_short_delay_between_fills_minutes_entry_0" in st.session_state:
                    self.cl_short.delay_between_fills_minutes_entry_0 = st.session_state.config_cl_short_delay_between_fills_minutes_entry_0
                st.number_input("short_delay_between_fills_minutes_entry min", value=float(round(self.cl_short.delay_between_fills_minutes_entry_0,self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_ROUND)), min_value=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MIN, max_value=self.cl_short.delay_between_fills_minutes_entry_1, step=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_STEP, format=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_FORMAT, key="config_cl_short_delay_between_fills_minutes_entry_0", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_short_delay_between_fills_minutes_close_0" in st.session_state:
                    self.cl_short.delay_between_fills_minutes_close_0 = st.session_state.config_cl_short_delay_between_fills_minutes_close_0
                st.number_input("short_delay_between_fills_minutes_close min", value=float(round(self.cl_short.delay_between_fills_minutes_close_0,self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_ROUND)), min_value=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MIN, max_value=self.cl_short.delay_between_fills_minutes_close_1, step=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_STEP, format=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_FORMAT, key="config_cl_short_delay_between_fills_minutes_close_0", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_short_min_markup_0" in st.session_state:
                    self.cl_short.min_markup_0 = st.session_state.config_cl_short_min_markup_0
                st.number_input("short_min_markup min", value=float(round(self.cl_short.min_markup_0,self.cl_short.MIN_MARKUP_ROUND)), min_value=self.cl_short.MIN_MARKUP_MIN, max_value=self.cl_short.min_markup_1, step=self.cl_short.MIN_MARKUP_STEP, format=self.cl_short.MIN_MARKUP_FORMAT, key="config_cl_short_min_markup_0", help=pbgui_help.min_markup)
                if  "config_cl_short_markup_range_0" in st.session_state:
                    self.cl_short.markup_range_0 = st.session_state.config_cl_short_markup_range_0
                st.number_input("short_markup_range min", value=float(round(self.cl_short.markup_range_0,self.cl_short.MARKUP_RANGE_ROUND)), min_value=self.cl_short.MARKUP_RANGE_MIN, max_value=self.cl_short.markup_range_1, step=self.cl_short.MARKUP_RANGE_STEP, format=self.cl_short.MARKUP_RANGE_FORMAT, key="config_cl_short_markup_range_0", help=pbgui_help.markup_range)
                if  "config_cl_short_n_close_orders_0" in st.session_state:
                    self.cl_short.n_close_orders_0 = st.session_state.config_cl_short_n_close_orders_0
                st.number_input("short_n_close_orders min", value=self.cl_short.n_close_orders_0, min_value=self.cl_short.N_CLOSE_ORDERS_MIN, max_value=self.cl_short.n_close_orders_1, step=self.cl_short.N_CLOSE_ORDERS_STEP, format=self.cl_short.N_CLOSE_ORDERS_FORMAT, key="config_cl_short_n_close_orders_0", help=pbgui_help.n_close_orders)
                if  "config_cl_short_wallet_exposure_limit_0" in st.session_state:
                    self.cl_short.wallet_exposure_limit_0 = st.session_state.config_cl_short_wallet_exposure_limit_0
                st.number_input("short_wallet_exposure_limit min", value=float(round(self.cl_short.wallet_exposure_limit_0,self.cl_short.WALLET_EXPOSURE_LIMIT_ROUND)), min_value=self.cl_short.WALLET_EXPOSURE_LIMIT_MIN, max_value=self.cl_short.wallet_exposure_limit_1, step=self.cl_short.WALLET_EXPOSURE_LIMIT_STEP, format=self.cl_short.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_cl_short_wallet_exposure_limit_0", help=pbgui_help.wallet_exposure_limit)
            with col4:
                if  "config_cl_short_ema_span_0_1" in st.session_state:
                    self.cl_short.ema_span_0_1 = st.session_state.config_cl_short_ema_span_0_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.ema_span_0_1,self.cl_short.EMA_SPAN_0_ROUND)), min_value=self.cl_short.ema_span_0_0, max_value=self.cl_short.EMA_SPAN_0_MAX, step=self.cl_short.EMA_SPAN_0_STEP, format=self.cl_short.EMA_SPAN_0_FORMAT, key="config_cl_short_ema_span_0_1", help=pbgui_help.ema_span)
                if  "config_cl_short_ema_span_1_1" in st.session_state:
                    self.cl_short.ema_span_1_1 = st.session_state.config_cl_short_ema_span_1_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.ema_span_1_1,self.cl_short.EMA_SPAN_1_ROUND)), min_value=self.cl_short.ema_span_1_0, max_value=self.cl_short.EMA_SPAN_1_MAX, step=self.cl_short.EMA_SPAN_1_STEP, format=self.cl_short.EMA_SPAN_1_FORMAT, key="config_cl_short_ema_span_1_1", help=pbgui_help.ema_span)
                if  "config_cl_short_ema_dist_entry_1" in st.session_state:
                    self.cl_short.ema_dist_entry_1 = st.session_state.config_cl_short_ema_dist_entry_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.ema_dist_entry_1,self.cl_short.EMA_DIST_ENTRY_ROUND)), min_value=self.cl_short.ema_dist_entry_0, max_value=self.cl_short.EMA_DIST_ENTRY_MAX, step=self.cl_short.EMA_DIST_ENTRY_STEP, format=self.cl_short.EMA_DIST_ENTRY_FORMAT, key="config_cl_short_ema_dist_entry_1", help=pbgui_help.ema_dist)
                if  "config_cl_short_ema_dist_close_1" in st.session_state:
                    self.cl_short.ema_dist_close_1 = st.session_state.config_cl_short_ema_dist_close_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.ema_dist_close_1,self.cl_short.EMA_DIST_CLOSE_ROUND)), min_value=self.cl_short.ema_dist_close_0, max_value=self.cl_short.EMA_DIST_CLOSE_MAX, step=self.cl_short.EMA_DIST_CLOSE_STEP, format=self.cl_short.EMA_DIST_CLOSE_FORMAT, key="config_cl_short_ema_dist_close_1", help=pbgui_help.ema_dist)
                if  "config_cl_short_qty_pct_entry_1" in st.session_state:
                    self.cl_short.qty_pct_entry_1 = st.session_state.config_cl_short_qty_pct_entry_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.qty_pct_entry_1,self.cl_short.QTY_PCT_ENTRY_ROUND)), min_value=self.cl_short.qty_pct_entry_0, max_value=self.cl_short.QTY_PCT_ENTRY_MAX, step=self.cl_short.QTY_PCT_ENTRY_STEP, format=self.cl_short.QTY_PCT_ENTRY_FORMAT, key="config_cl_short_qty_pct_entry_1", help=pbgui_help.qty_pct)
                if  "config_cl_short_qty_pct_close_1" in st.session_state:
                    self.cl_short.qty_pct_close_1 = st.session_state.config_cl_short_qty_pct_close_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.qty_pct_close_1,self.cl_short.QTY_PCT_CLOSE_ROUND)), min_value=self.cl_short.qty_pct_close_0, max_value=self.cl_short.QTY_PCT_CLOSE_MAX, step=self.cl_short.QTY_PCT_CLOSE_STEP, format=self.cl_short.QTY_PCT_CLOSE_FORMAT, key="config_cl_short_qty_pct_close_1", help=pbgui_help.qty_pct)
                if  "config_cl_short_we_multiplier_entry_1" in st.session_state:
                    self.cl_short.we_multiplier_entry_1 = st.session_state.config_cl_short_we_multiplier_entry_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.we_multiplier_entry_1,self.cl_short.WE_MULTIPLIER_ENTRY_ROUND)), min_value=self.cl_short.we_multiplier_entry_0, max_value=self.cl_short.WE_MULTIPLIER_ENTRY_MAX, step=self.cl_short.WE_MULTIPLIER_ENTRY_STEP, format=self.cl_short.WE_MULTIPLIER_ENTRY_FORMAT, key="config_cl_short_we_multiplier_entry_1", help=pbgui_help.we_multiplier)
                if  "config_cl_short_we_multiplier_close_1" in st.session_state:
                    self.cl_short.we_multiplier_close_1 = st.session_state.config_cl_short_we_multiplier_close_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.we_multiplier_close_1,self.cl_short.WE_MULTIPLIER_CLOSE_ROUND)), min_value=self.cl_short.we_multiplier_close_0, max_value=self.cl_short.WE_MULTIPLIER_CLOSE_MAX, step=self.cl_short.WE_MULTIPLIER_CLOSE_STEP, format=self.cl_short.WE_MULTIPLIER_CLOSE_FORMAT, key="config_cl_short_we_multiplier_close_1", help=pbgui_help.we_multiplier)
                if  "config_cl_short_delay_weight_entry_1" in st.session_state:
                    self.cl_short.delay_weight_entry_1 = st.session_state.config_cl_short_delay_weight_entry_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.delay_weight_entry_1,self.cl_short.DELAY_WEIGHT_ENTRY_ROUND)), min_value=self.cl_short.delay_weight_entry_0, max_value=self.cl_short.DELAY_WEIGHT_ENTRY_MAX, step=self.cl_short.DELAY_WEIGHT_ENTRY_STEP, format=self.cl_short.DELAY_WEIGHT_ENTRY_FORMAT, key="config_cl_short_delay_weight_entry_1", help=pbgui_help.delay_weight)
                if  "config_cl_short_delay_weight_close_1" in st.session_state:
                    self.cl_short.delay_weight_close_1 = st.session_state.config_cl_short_delay_weight_close_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.delay_weight_close_1,self.cl_short.DELAY_WEIGHT_CLOSE_ROUND)), min_value=self.cl_short.delay_weight_close_0, max_value=self.cl_short.DELAY_WEIGHT_CLOSE_MAX, step=self.cl_short.DELAY_WEIGHT_CLOSE_STEP, format=self.cl_short.DELAY_WEIGHT_CLOSE_FORMAT, key="config_cl_short_delay_weight_close_1", help=pbgui_help.delay_weight)
                if  "config_cl_short_delay_between_fills_minutes_entry_1" in st.session_state:
                    self.cl_short.delay_between_fills_minutes_entry_1 = st.session_state.config_cl_short_delay_between_fills_minutes_entry_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.delay_between_fills_minutes_entry_1,self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_ROUND)), min_value=self.cl_short.delay_between_fills_minutes_entry_0, max_value=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_MAX, step=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_STEP, format=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_ENTRY_FORMAT, key="config_cl_short_delay_between_fills_minutes_entry_1", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_short_delay_between_fills_minutes_close_1" in st.session_state:
                    self.cl_short.delay_between_fills_minutes_close_1 = st.session_state.config_cl_short_delay_between_fills_minutes_close_1
                st.number_input("short_ema_span_0 max", value=float(round(self.cl_short.delay_between_fills_minutes_close_1,self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_ROUND)), min_value=self.cl_short.delay_between_fills_minutes_close_0, max_value=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_MAX, step=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_STEP, format=self.cl_short.DELAY_BETWEEN_FILLS_MINUTES_CLOSE_FORMAT, key="config_cl_short_delay_between_fills_minutes_close_1", help=pbgui_help.delay_between_fills_minutes)
                if  "config_cl_short_min_markup_1" in st.session_state:
                    self.cl_short.min_markup_1 = st.session_state.config_cl_short_min_markup_1
                st.number_input("short_min_markup max", value=float(round(self.cl_short.min_markup_1,self.cl_short.MIN_MARKUP_ROUND)), min_value=self.cl_short.min_markup_0, max_value=self.cl_short.MIN_MARKUP_MAX, step=self.cl_short.MIN_MARKUP_STEP, format=self.cl_short.MIN_MARKUP_FORMAT, key="config_cl_short_min_markup_1", help=pbgui_help.min_markup)
                if  "config_cl_short_markup_range_1" in st.session_state:
                    self.cl_short.markup_range_1 = st.session_state.config_cl_short_markup_range_1
                st.number_input("short_markup_range max", value=float(round(self.cl_short.markup_range_1,self.cl_short.MARKUP_RANGE_ROUND)), min_value=self.cl_short.markup_range_0, max_value=self.cl_short.MARKUP_RANGE_MAX, step=self.cl_short.MARKUP_RANGE_STEP, format=self.cl_short.MARKUP_RANGE_FORMAT, key="config_cl_short_markup_range_1", help=pbgui_help.markup_range)
                if  "config_cl_short_n_close_orders_1" in st.session_state:
                    self.cl_short.n_close_orders_1 = st.session_state.config_cl_short_n_close_orders_1
                st.number_input("short_n_close_orders max", value=self.cl_short.n_close_orders_1, min_value=self.cl_short.n_close_orders_1, max_value=self.cl_short.N_CLOSE_ORDERS_MAX, step=self.cl_short.N_CLOSE_ORDERS_STEP, format=self.cl_short.N_CLOSE_ORDERS_FORMAT, key="config_cl_short_n_close_orders_1", help=pbgui_help.n_close_orders)
                if  "config_cl_short_wallet_exposure_limit_1" in st.session_state:
                    self.cl_short.wallet_exposure_limit_1 = float(round(self.cl_short.wallet_exposure_limit_1,self.cl_short.WALLET_EXPOSURE_LIMIT_ROUND))
                st.number_input("short_wallet_exposure_limit max", value=float(round(self.cl_short.wallet_exposure_limit_1,self.cl_short.WALLET_EXPOSURE_LIMIT_ROUND)), min_value=self.cl_short.wallet_exposure_limit_0, max_value=self.cl_short.WALLET_EXPOSURE_LIMIT_MAX, step=self.cl_short.WALLET_EXPOSURE_LIMIT_STEP, format=self.cl_short.WALLET_EXPOSURE_LIMIT_FORMAT, key="config_cl_short_wallet_exposure_limit_1", help=pbgui_help.wallet_exposure_limit)
        with st.expander("Advanced parameters", expanded = False):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                if  "config_n_harmonies" in st.session_state:
                    self.n_harmonies = st.session_state.config_n_harmonies
                st.number_input("n_harmonies", min_value=1, max_value=256, value=self.n_harmonies, step=1, format='%d', key="config_n_harmonies", help=pbgui_help.harmony_search)
                if  "config_n_particles" in st.session_state:
                    self.n_particles = st.session_state.config_n_particles
                st.number_input("n_particles", min_value=1, max_value=256, value=self.n_particles, step=1, format='%d', key="config_n_particles", help=pbgui_help.particle_swarm)
            with col2:
                if  "config_hm_considering_rate" in st.session_state:
                    self.hm_considering_rate = st.session_state.config_hm_considering_rate
                st.number_input("hm_considering_rate", min_value=0.1, max_value=5.0, value=float(round(self.hm_considering_rate,1)), step=0.1, format='%.1f', key="config_hm_considering_rate", help=pbgui_help.harmony_search)
                if  "config_w" in st.session_state:
                    self.w = st.session_state.config_w
                st.number_input("w", min_value=0.01, max_value=5.0, value=float(round(self.w,2)), step=0.01, format='%.2f', key="config_w", help=pbgui_help.particle_swarm)
            with col3:
                if  "config_bandwidth" in st.session_state:
                    self.bandwidth = st.session_state.config_bandwidth
                st.number_input("bandwidth", min_value=0.01, max_value=5.00, value=float(round(self.bandwidth,2)), step=0.01, format='%.2f', key="config_bandwidth", help=pbgui_help.harmony_search)
                if  "config_c0" in st.session_state:
                    self.c0 = st.session_state.config_c0
                st.number_input("c0", min_value=0.1, max_value=5.0, value=float(round(self.c0,1)), step=0.1, format='%.1f', key="config_c0", help=pbgui_help.particle_swarm)
            with col4:
                if  "config_pitch_adjusting_rate" in st.session_state:
                    self.pitch_adjusting_rate = st.session_state.config_pitch_adjusting_rate
                st.number_input("pitch_adjusting_rate", min_value=0.01, max_value=5.00, value=float(round(self.pitch_adjusting_rate,2)), step=0.01, format='%.2f', key="config_pitch_adjusting_rate", help=pbgui_help.harmony_search)
                if  "config_c1" in st.session_state:
                    self.c1 = st.session_state.config_c1
                st.number_input("c1", min_value=0.1, max_value=5.0, value=float(round(self.c1,1)), step=0.1, format='%.1f', key="config_c1", help=pbgui_help.particle_swarm)

class OptimizeConfigs:
    def __init__(self):
        self.configs = []
        self.index = 0
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pbdir = pb_config.get("main", "pbdir")
        self.config_path = f'{pbdir}/configs/optimize'
        self.load()

    def __iter__(self):
        return iter(self.configs)

    def __next__(self):
        if self.index > len(self.configs):
            raise StopIteration
        self.index += 1
        return next(self)
    
    def list(self):
        return list(map(lambda c: c.name, self.configs))

    def default(self):
        if self.configs:
            if "default.hjson" in self.list():
                return "default.hjson"
            return self.configs[0].name
        else:
            return None

    def find_config(self, name: str):
        for config in self.configs:
            if config.name == name:
                return config

    def load(self):
        p = str(Path(f'{self.config_path}/*.hjson'))
        configs = glob.glob(p)
        for config in configs:
            opt_config = OptimizeConfig()
            opt_config.config_file = config
            self.configs.append(opt_config)
        self.configs = sorted(self.configs, key=lambda d: d.name) 
