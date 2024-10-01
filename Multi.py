import streamlit as st
import streamlit_scrollable_textbox as stx
import pbgui_help
from pbgui_func import pbdir, PBGDIR, load_symbols_from_ini, validateHJSON, st_file_selector, info_popup, error_popup
import os
from PBRemote import PBRemote
from User import Users
from Config import Config
from Exchange import Exchange
from pathlib import Path
import hjson
import glob
from shutil import rmtree
import traceback
import shutil

class MultiInstance():
    def __init__(self):
        self.instance_path = None
        self._users = Users()
        self._user = self._users.list()[0]
        self._multi_config = {}
        self.initialize()

    # user
    @property
    def user(self): return self._user
    @user.setter
    def user(self, new_user):
        if new_user != self._user:
            self._user = new_user
            # Reset GUI
            if "edit_multi_user" in st.session_state and "edit_multi_loss_allowance_pct" in st.session_state:
                del st.session_state.edit_multi_enabled_on
                del st.session_state.edit_multi_version
                del st.session_state.edit_multi_leverage
                del st.session_state.edit_multi_loss_allowance_pct
                del st.session_state.edit_multi_pnls_max_lookback_days
                del st.session_state.edit_multi_stuck_threshold
                del st.session_state.edit_multi_unstuck_close_pct
                del st.session_state.edit_multi_execution_delay_seconds
                del st.session_state.edit_multi_price_distance_threshold
                del st.session_state.edit_multi_auto_gs
                del st.session_state.edit_multi_TWE_long
                del st.session_state.edit_multi_TWE_short
                del st.session_state.edit_multi_long_enabled
                del st.session_state.edit_multi_short_enabled
                if "edit_multi_universal_live_config" in st.session_state:
                    del st.session_state.edit_multi_universal_live_config
                del st.session_state.edit_multi_approved_symbols
                del st.session_state.edit_multi_ignored_symbols
                del st.session_state.edit_multi_n_longs
                del st.session_state.edit_multi_n_shorts
                del st.session_state.edit_multi_minimum_market_age_days
                del st.session_state.edit_multi_ohlcv_interval
                del st.session_state.edit_multi_n_ohlcvs
                del st.session_state.edit_multi_relative_volume_filter_clip_pct
                del st.session_state.edit_multi_max_n_cancellations_per_batch
                del st.session_state.edit_multi_max_n_creations_per_batch
                del st.session_state.edit_multi_forced_mode_long
                del st.session_state.edit_multi_forced_mode_short
                del st.session_state.edit_multi_filter_by_min_effective_cost
            # Init
            self._multi_config = {}
            self.initialize()
            # Load Multi config if available
            pbgdir = Path.cwd()
            self.load(Path(f'{pbgdir}/data/multi/{self._user}'))
    # enabled_on
    @property
    def enabled_on(self): return self._enabled_on
    @enabled_on.setter
    def enabled_on(self, new_enabled_on):
        self._enabled_on = new_enabled_on
    # version 
    @property
    def version(self): return self._version
    @version.setter
    def version(self, new_version):
        self._version = new_version
    # leverage
    @property
    def leverage(self): return self._leverage
    @leverage.setter
    def leverage(self, new_leverage):
        self._leverage = new_leverage
    # loss_allowance_pct
    @property
    def loss_allowance_pct(self): return self._loss_allowance_pct
    @loss_allowance_pct.setter
    def loss_allowance_pct(self, new_loss_allowance_pct):
        self._loss_allowance_pct = new_loss_allowance_pct
    # pnls_max_lookback_days
    @property
    def pnls_max_lookback_days(self): return self._pnls_max_lookback_days
    @pnls_max_lookback_days.setter
    def pnls_max_lookback_days(self, new_pnls_max_lookback_days):
        self._pnls_max_lookback_days = new_pnls_max_lookback_days
    # stuck_threshold
    @property
    def stuck_threshold(self): return self._stuck_threshold
    @stuck_threshold.setter
    def stuck_threshold(self, new_stuck_threshold):
        self._stuck_threshold = new_stuck_threshold
    # unstuck_close_pct
    @property
    def unstuck_close_pct(self): return self._unstuck_close_pct
    @unstuck_close_pct.setter
    def unstuck_close_pct(self, new_unstuck_close_pct):
        self._unstuck_close_pct = new_unstuck_close_pct
    # execution_delay_seconds
    @property
    def execution_delay_seconds(self): return self._execution_delay_seconds
    @execution_delay_seconds.setter
    def execution_delay_seconds(self, new_execution_delay_seconds):
        self._execution_delay_seconds = new_execution_delay_seconds
    # price_distance_threshold
    @property
    def price_distance_threshold(self): return self._price_distance_threshold
    @price_distance_threshold.setter
    def price_distance_threshold(self, new_price_distance_threshold):
        self._price_distance_threshold = new_price_distance_threshold
    # auto_gs
    @property
    def auto_gs(self): return self._auto_gs
    @auto_gs.setter
    def auto_gs(self, new_auto_gs):
        self._auto_gs = new_auto_gs
    # TWE_long
    @property
    def TWE_long(self): return self._TWE_long
    @TWE_long.setter
    def TWE_long(self, new_TWE_long):
        self._TWE_long = round(new_TWE_long,10)
    # TWE_long
    @property
    def TWE_short(self): return self._TWE_short
    @TWE_short.setter
    def TWE_short(self, new_TWE_short):
        self._TWE_short = round(new_TWE_short,10)
    # TWE enabled
    @property
    def TWE_enabled(self): return self._TWE_enabled
    @TWE_enabled.setter
    def TWE_enabled(self, new_TWE_enabled):
        self._TWE_enabled = new_TWE_enabled
    # long_enabled
    @property
    def long_enabled(self): return self._long_enabled
    @long_enabled.setter
    def long_enabled(self, new_long_enabled):
        self._long_enabled = new_long_enabled
    # short_enabled
    @property
    def short_enabled(self): return self._short_enabled
    @short_enabled.setter
    def short_enabled(self, new_short_enabled):
        self._short_enabled = new_short_enabled
    # n_longs
    @property
    def n_longs(self): return self._n_longs
    @n_longs.setter
    def n_longs(self, new_n_longs):
        self._n_longs = new_n_longs
    # n_shorts
    @property
    def n_shorts(self): return self._n_shorts
    @n_shorts.setter
    def n_shorts(self, new_n_shorts):
        self._n_shorts = new_n_shorts
    # minimum_market_age_days
    @property
    def minimum_market_age_days(self): return self._minimum_market_age_days
    @minimum_market_age_days.setter
    def minimum_market_age_days(self, new_minimum_market_age_days):
        self._minimum_market_age_days = new_minimum_market_age_days
    # ohlcv_interval
    @property
    def ohlcv_interval(self): return self._ohlcv_interval
    @ohlcv_interval.setter
    def ohlcv_interval(self, new_ohlcv_interval):
        self._ohlcv_interval = new_ohlcv_interval
    # n_ohlcvs
    @property
    def n_ohlcvs(self): return self._n_ohlcvs
    @n_ohlcvs.setter
    def n_ohlcvs(self, new_n_ohlcvs):
        self._n_ohlcvs = new_n_ohlcvs
    # relative_volume_filter_clip_pct
    @property
    def relative_volume_filter_clip_pct(self): return self._relative_volume_filter_clip_pct
    @relative_volume_filter_clip_pct.setter
    def relative_volume_filter_clip_pct(self, new_relative_volume_filter_clip_pct):
        self._relative_volume_filter_clip_pct = new_relative_volume_filter_clip_pct
    # max_n_cancellations_per_batch
    @property
    def max_n_cancellations_per_batch(self): return self._max_n_cancellations_per_batch
    @max_n_cancellations_per_batch.setter
    def max_n_cancellations_per_batch(self, new_max_n_cancellations_per_batch):
        self._max_n_cancellations_per_batch = new_max_n_cancellations_per_batch
    # max_n_creations_per_batch
    @property
    def max_n_creations_per_batch(self): return self._max_n_creations_per_batch
    @max_n_creations_per_batch.setter
    def max_n_creations_per_batch(self, new_max_n_creations_per_batch):
        self._max_n_creations_per_batch = new_max_n_creations_per_batch
    # forced_mode_long
    @property
    def forced_mode_long(self): return self._forced_mode_long
    @forced_mode_long.setter
    def forced_mode_long(self, new_forced_mode_long):
        self._forced_mode_long = new_forced_mode_long
    # forced_mode_short
    @property
    def forced_mode_short(self): return self._forced_mode_short
    @forced_mode_short.setter
    def forced_mode_short(self, new_forced_mode_short):
        self._forced_mode_short = new_forced_mode_short
    # filter_by_min_effective_cost
    @property
    def filter_by_min_effective_cost(self): return self._filter_by_min_effective_cost
    @filter_by_min_effective_cost.setter
    def filter_by_min_effective_cost(self, new_filter_by_min_effective_cost):
        self._filter_by_min_effective_cost = new_filter_by_min_effective_cost
    # default_config_path
    @property
    def default_config_path(self): return self._default_config_path
    @default_config_path.setter
    def default_config_path(self, new_default_config_path):
        self._default_config_path = new_default_config_path
    # universal_live_config
    @property
    def universal_live_config(self): return self._universal_live_config
    @universal_live_config.setter
    def universal_live_config(self, new_universal_live_config):
        if new_universal_live_config:
            if validateHJSON(new_universal_live_config):
                self._universal_live_config = new_universal_live_config
                if "error_config" in st.session_state:
                    del st.session_state.error_config
            else:
                st.session_state.error_config = "Universal Config is invalid"
        else:
            if "error_config" in st.session_state:
                del st.session_state.error_config
    # running_version
    @property
    def running_version(self):
        if self.enabled_on == self.remote.name:
            version = self.remote.local_run.instances_status.find_version(self.user)
        elif self.enabled_on in self.remote.list():
            version = self.remote.find_server(self.enabled_on).instances_status.find_version(self.user)
        else:
            version = 0
        return version

    def initialize(self):
        # Init defaults
        self._enabled_on = "disabled"
        self._version = 0
        self._TWE_enabled = False
        self._leverage = 10.0
        self._loss_allowance_pct = 0.002
        self._pnls_max_lookback_days = 30
        self._stuck_threshold = 0.9
        self._unstuck_close_pct = 0.01
        self._execution_delay_seconds = 2
        self._price_distance_threshold = 0.002
        self._auto_gs = True
        self._TWE_long = 0.0
        self._TWE_short = 0.0
        self._long_enabled = True
        self._short_enabled = False
        self._symbols = []
        self._ignored_symbols = []
        self._default_config = ""
        self._default_config_path = f'{self.instance_path}/default.json'
        default_config = Path(f'{self.instance_path}/default.json')
        if default_config.exists():
            self.default_config = Config(default_config)
        else:
            default_config =  Path(f'{pbdir()}/configs/live/recursive_grid_mode.example.json')
            if default_config.exists():
                self.default_config = Config(default_config)
            else:
                self.default_config = Config()
        self.default_config.load_config()
        self._universal_live_config = ""
        self._n_longs = 0
        self._n_shorts = 0
        self._minimum_market_age_days = 3
        self._ohlcv_interval = '15m'
        self._n_ohlcvs = 24
        self._relative_volume_filter_clip_pct = 0.1
        self._max_n_cancellations_per_batch = 8
        self._max_n_creations_per_batch = 4
        self._forced_mode_long = ""
        self._forced_mode_short = ""
        self._filter_by_min_effective_cost = False
        # Load options from Multi config if available
        if "user" in self._multi_config:
            self._user = self._multi_config["user"]
        if "enabled_on" in self._multi_config:
            self._enabled_on = self._multi_config["enabled_on"]
        if "version" in self._multi_config:
            self._version = self._multi_config["version"]
        if "leverage" in self._multi_config:
            self._leverage = float(self._multi_config["leverage"])
        if "loss_allowance_pct" in self._multi_config:
            self._loss_allowance_pct = float(self._multi_config["loss_allowance_pct"])
        if "pnls_max_lookback_days" in self._multi_config:
            self._pnls_max_lookback_days = self._multi_config["pnls_max_lookback_days"]
        if "stuck_threshold" in self._multi_config:
            self._stuck_threshold = float(self._multi_config["stuck_threshold"])
        if "unstuck_close_pct" in self._multi_config:
            self._unstuck_close_pct = float(self._multi_config["unstuck_close_pct"])
        if "execution_delay_seconds" in self._multi_config:
            self._execution_delay_seconds = self._multi_config["execution_delay_seconds"]
        if "price_distance_threshold" in self._multi_config:
            self._price_distance_threshold = float(self._multi_config["price_distance_threshold"])
        if "auto_gs" in self._multi_config:
            self._auto_gs = self._multi_config["auto_gs"]
        if "TWE_long" in self._multi_config:
            self._TWE_long = float(self._multi_config["TWE_long"])
        if "TWE_short" in self._multi_config:
            self._TWE_short = float(self._multi_config["TWE_short"])
        if "long_enabled" in self._multi_config:
            self._long_enabled = self._multi_config["long_enabled"]
        if "short_enabled" in self._multi_config:
            self._short_enabled = self._multi_config["short_enabled"]
        if "n_longs" in self._multi_config:
            self._n_longs = self._multi_config["n_longs"]
        if "n_shorts" in self._multi_config:
            self._n_shorts = self._multi_config["n_shorts"]
        if "minimum_market_age_days" in self._multi_config:
            self._minimum_market_age_days = self._multi_config["minimum_market_age_days"]
        if "ohlcv_interval" in self._multi_config:
            self._ohlcv_interval = self._multi_config["ohlcv_interval"]
        if "n_ohlcvs" in self._multi_config:
            self._n_ohlcvs = self._multi_config["n_ohlcvs"]
        if "relative_volume_filter_clip_pct" in self._multi_config:
            self._relative_volume_filter_clip_pct = self._multi_config["relative_volume_filter_clip_pct"]
        if "max_n_cancellations_per_batch" in self._multi_config:
            self._max_n_cancellations_per_batch = self._multi_config["max_n_cancellations_per_batch"]
        if "max_n_creations_per_batch" in self._multi_config:
            self._max_n_creations_per_batch = self._multi_config["max_n_creations_per_batch"]
        if "forced_mode_long" in self._multi_config:
            self._forced_mode_long = self._multi_config["forced_mode_long"]
        if "forced_mode_short" in self._multi_config:
            self._forced_mode_short = self._multi_config["forced_mode_short"]
        if "filter_by_min_effective_cost" in self._multi_config:
            self._filter_by_min_effective_cost = self._multi_config["filter_by_min_effective_cost"]
        if "default_config_path" in self._multi_config:
            self._default_config_path = self._multi_config["default_config_path"]
        if "universal_live_config" in self._multi_config:
            self._universal_live_config = hjson.dumps(self._multi_config["universal_live_config"],indent=2)
        if "ignored_symbols" in self._multi_config:
            self._ignored_symbols = self._multi_config["ignored_symbols"]
        # Load available symbols
        self._available_symbols = load_symbols_from_ini(exchange=self._users.find_exchange(self.user), market_type='swap')
        # Load cpt allowed symbols
        self._cpt_allowed_symbols = load_symbols_from_ini(exchange=self._users.find_exchange(self.user), market_type='cpt')
        # Load instances from user
        for instance in st.session_state.pbgui_instances:
            if instance.user == self.user and instance.market_type == "futures" :
                self._symbols.append(instance.symbol)
        # Add symbols from config
        if "approved_symbols" in self._multi_config:
            for symbol in self._multi_config["approved_symbols"]:
                if symbol not in self._symbols:
                    self._symbols.append(symbol)
        # Old config Versions using "symbol" as key
        if "symbols" in self._multi_config:
            for symbol in self._multi_config["symbols"]:
                if symbol not in self._symbols:
                    self._symbols.append(symbol)
        # Init PBremote
        if 'remote' not in st.session_state:
            st.session_state.remote = PBRemote()
        self.remote = st.session_state.remote

    def is_running(self):
        if self.enabled_on == self.remote.name:
            return self.remote.local_run.instances_status.is_running(self.user)
        elif self.enabled_on in self.remote.list():
            return self.remote.find_server(self.enabled_on).instances_status.is_running(self.user)
        return False

    def is_running_on(self):
        running_on = []
        if self.remote.local_run.instances_status.is_running(self.user):
            running_on.append(self.remote.name)
        for server in self.remote.list():
            if self.remote.find_server(server).instances_status.is_running(self.user):
                running_on.append(server)
        return running_on

    def generate_active_symbols(self):
        symbols = {}
        user_symbols = []
        for instance in st.session_state.pbgui_instances:
            if instance.user == self.user and instance.market_type == "futures":
                user_symbols.append(instance.symbol)
                if instance.multi:
                    instance.enabled_on = self.enabled_on
                    instance.save()
                    if instance._config.long_enabled:
                        lm = f'-lm n'
                        lw = f'-lw {instance._config.long_we}'
                    else:
                        if self.auto_gs:
                            lm = f'-lm gs'
                        else:
                            lm = f'-lm m'
                        lw = f'-lw 0.0'
                    if instance.long_mode == "graceful_stop":
                        lm = f'-lm gs'
                    elif instance.long_mode == "panic":
                        lm = f'-lm p'
                    elif instance.long_mode == "tp_only":
                        lm = f'-lm t'
                    if instance._config.short_enabled:
                        sm = f'-sm n'
                        sw = f'-sw {instance._config.short_we}'
                    else:
                        if self.auto_gs:
                            sm = f'-sm gs'
                        else:
                            sm = f'-sm m'
                        sw = f'-sw 0.0'
                    if instance.short_mode == "graceful_stop":
                        sm = f'-sm gs'
                    elif instance.short_mode == "panic":
                        sm = f'-sm p'
                    elif instance.short_mode == "tp_only":
                        sm = f'-sm t'
                    symbols[instance.symbol] = f'{lm} {lw} {sm} {sw}'    
                    shutil.copy(f'{instance.instance_path}/config.json', f'{self.instance_path}/{instance.symbol}.json')
                else:
                    Path(f'{self.instance_path}/{instance.symbol}.json').unlink(missing_ok=True)
        for symbol in self._symbols:
            default_config = False
            if symbol not in user_symbols:
                config_file = Path(f'{self.instance_path}/{symbol}.json')
                if config_file.exists():
                    multi_config = Config(config_file)
                    multi_config.load_config()
                else:
                    multi_config = self.default_config
                    default_config = True
                if multi_config.long_enabled:
                    lm = f'-lm n'
                    lw = f'-lw {multi_config.long_we}'
                else:
                    if self.auto_gs:
                        lm = f'-lm gs'
                    else:
                        lm = f'-lm m'
                    lw = f'-lw 0.0'
                if multi_config.short_enabled:
                    sm = f'-sm n'
                    sw = f'-sw {multi_config.short_we}'
                else:
                    if self.auto_gs:
                        sm = f'-sm gs'
                    else:
                        sm = f'-sm m'
                    sw = f'-sw 0.0'
                if default_config:
                    symbols[symbol] = ''
                else:
                    symbols[symbol] = f'{lm} {lw} {sm} {sw}'
        return symbols

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
        self.instance_path = path
        file = Path(f'{path}/multi.hjson')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    multi_config = f.read()
                self._multi_config = hjson.loads(multi_config)
                self.initialize()
                return True
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

    def save(self):
        pbgdir = Path.cwd()
        multi_path = Path(f'{pbgdir}/data/multi/{self._user}')
        if not multi_path.exists():
            multi_path.mkdir(parents=True)
        multi_config = Path(f'{str(multi_path)}/multi.hjson')
        self._multi_config["user"] = self.user
        self.version += 1
        if "edit_multi_version" in st.session_state:
            del st.session_state.edit_multi_version
        self._multi_config["version"] = self.version
        self._multi_config["enabled_on"] = self.enabled_on
        self._multi_config["leverage"] = self.leverage
        self._multi_config["loss_allowance_pct"] = self.loss_allowance_pct
        self._multi_config["pnls_max_lookback_days"] = self.pnls_max_lookback_days
        self._multi_config["stuck_threshold"] = self.stuck_threshold
        self._multi_config["unstuck_close_pct"] = self.unstuck_close_pct
        self._multi_config["execution_delay_seconds"] = self.execution_delay_seconds
        self._multi_config["price_distance_threshold"] = self.price_distance_threshold
        self._multi_config["auto_gs"] = self.auto_gs
        self._multi_config["approved_symbols"] = self.generate_active_symbols()
        # Remove old symbols key from config
        if "symbols" in self._multi_config:
            del self._multi_config["symbols"]
        self._multi_config["TWE_long"] = self.TWE_long
        self._multi_config["TWE_short"] = self.TWE_short
        self._multi_config["long_enabled"] = self.long_enabled
        self._multi_config["short_enabled"] = self.short_enabled
        if st.session_state.edit_multi_config_type == "default":
            self._multi_config["default_config_path"] = f'{self.instance_path}/default.json'
        else:
            self._multi_config["default_config_path"] = ""
        self._multi_config["universal_live_config"] = hjson.loads(self.universal_live_config)
        self._multi_config["live_configs_dir"] = f'{self.instance_path}'
        self._multi_config["ignored_symbols"] = self._ignored_symbols
        self._multi_config["n_longs"] = self.n_longs
        self._multi_config["n_shorts"] = self.n_shorts
        self._multi_config["minimum_market_age_days"] = self.minimum_market_age_days
        self._multi_config["ohlcv_interval"] = self.ohlcv_interval
        self._multi_config["n_ohlcvs"] = self.n_ohlcvs
        self._multi_config["relative_volume_filter_clip_pct"] = self.relative_volume_filter_clip_pct
        self._multi_config["max_n_cancellations_per_batch"] = self.max_n_cancellations_per_batch
        self._multi_config["max_n_creations_per_batch"] = self.max_n_creations_per_batch
        self._multi_config["forced_mode_long"] = self.forced_mode_long
        self._multi_config["forced_mode_short"] = self.forced_mode_short
        self._multi_config["filter_by_min_effective_cost"] = self.filter_by_min_effective_cost
        config = hjson.dumps(self._multi_config)
        with open(multi_config, "w", encoding='utf-8') as f:
            f.write(config)
        # Save default config
        self.default_config.config_file = Path(f'{self.instance_path}/default.json')
        self.default_config.save_config()

    def edit(self):
        # Init session_state for keys
        if "edit_multi_user" in st.session_state:
            if st.session_state.edit_multi_user != self.user:
                self.user = st.session_state.edit_multi_user
        if self._users.find_exchange(self.user) in ["kucoin","bingx"]:
            st.write("Exchnage not supported by passivbot_multi")
            return
        if "edit_multi_enabled_on" in st.session_state:
            if st.session_state.edit_multi_enabled_on != self.enabled_on:
                self.enabled_on = st.session_state.edit_multi_enabled_on
        if "edit_multi_version" in st.session_state:
            if st.session_state.edit_multi_version != self.version:
                self.version = st.session_state.edit_multi_version
        if "edit_multi_leverage" in st.session_state:
            if st.session_state.edit_multi_leverage != self.leverage:
                self.leverage = st.session_state.edit_multi_leverage
        if "edit_multi_loss_allowance_pct" in st.session_state:
            if st.session_state.edit_multi_loss_allowance_pct != self.loss_allowance_pct:
                self.loss_allowance_pct = st.session_state.edit_multi_loss_allowance_pct
        if "edit_multi_pnls_max_lookback_days" in st.session_state:
            if st.session_state.edit_multi_pnls_max_lookback_days != self.pnls_max_lookback_days:
                self.pnls_max_lookback_days = st.session_state.edit_multi_pnls_max_lookback_days
        if "edit_multi_stuck_threshold" in st.session_state:
            if st.session_state.edit_multi_stuck_threshold != self.stuck_threshold:
                self.stuck_threshold = st.session_state.edit_multi_stuck_threshold
        if "edit_multi_unstuck_close_pct" in st.session_state:
            if st.session_state.edit_multi_unstuck_close_pct != self.unstuck_close_pct:
                self.unstuck_close_pct = st.session_state.edit_multi_unstuck_close_pct
        if "edit_multi_execution_delay_seconds" in st.session_state:
            if st.session_state.edit_multi_execution_delay_seconds != self.execution_delay_seconds:
                self.execution_delay_seconds = st.session_state.edit_multi_execution_delay_seconds
        if "edit_multi_price_distance_threshold" in st.session_state:
            if st.session_state.edit_multi_price_distance_threshold != self.price_distance_threshold:
                self.price_distance_threshold = st.session_state.edit_multi_price_distance_threshold
        if "edit_multi_auto_gs" in st.session_state:
            if st.session_state.edit_multi_auto_gs != self.auto_gs:
                self.auto_gs = st.session_state.edit_multi_auto_gs
        if "edit_multi_TWE_long" in st.session_state:
            if st.session_state.edit_multi_TWE_long != self.TWE_long:
                self.TWE_long = st.session_state.edit_multi_TWE_long
        if "edit_multi_TWE_short" in st.session_state:
            if st.session_state.edit_multi_TWE_short != self.TWE_short:
                self.TWE_short = st.session_state.edit_multi_TWE_short
        if "edit_multi_long_enabled" in st.session_state:
            if st.session_state.edit_multi_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.edit_multi_long_enabled
        if "edit_multi_short_enabled" in st.session_state:
            if st.session_state.edit_multi_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.edit_multi_short_enabled
        if "edit_multi_universal_live_config" in st.session_state:
            if st.session_state.edit_multi_universal_live_config != self.universal_live_config:
                self.universal_live_config = st.session_state.edit_multi_universal_live_config
            else:
                if validateHJSON(st.session_state.edit_multi_universal_live_config):
                    if "error_config" in st.session_state:
                        del st.session_state.error_config
        if "edit_multi_approved_symbols" in st.session_state:
            if st.session_state.edit_multi_approved_symbols != self._symbols:
                self._symbols = st.session_state.edit_multi_approved_symbols
        if "edit_multi_ignored_symbols" in st.session_state:
            if st.session_state.edit_multi_ignored_symbols != self._ignored_symbols:
                self._ignored_symbols = st.session_state.edit_multi_ignored_symbols
        if "edit_multi_n_longs" in st.session_state:
            if st.session_state.edit_multi_n_longs != self.n_longs:
                self.n_longs = st.session_state.edit_multi_n_longs
        if "edit_multi_n_shorts" in st.session_state:
            if st.session_state.edit_multi_n_shorts != self.n_shorts:
                self.n_shorts = st.session_state.edit_multi_n_shorts
        if "edit_multi_minimum_market_age_days" in st.session_state:
            if st.session_state.edit_multi_minimum_market_age_days != self.minimum_market_age_days:
                self.minimum_market_age_days = st.session_state.edit_multi_minimum_market_age_days
        if "edit_multi_ohlcv_interval" in st.session_state:
            if st.session_state.edit_multi_ohlcv_interval != self.ohlcv_interval:
                self.ohlcv_interval = st.session_state.edit_multi_ohlcv_interval
        if "edit_multi_n_ohlcvs" in st.session_state:
            if st.session_state.edit_multi_n_ohlcvs != self.n_ohlcvs:
                self.n_ohlcvs = st.session_state.edit_multi_n_ohlcvs
        if "edit_multi_relative_volume_filter_clip_pct" in st.session_state:
            if st.session_state.edit_multi_relative_volume_filter_clip_pct != self.relative_volume_filter_clip_pct:
                self.relative_volume_filter_clip_pct = st.session_state.edit_multi_relative_volume_filter_clip_pct
        if "edit_multi_max_n_cancellations_per_batch" in st.session_state:
            if st.session_state.edit_multi_max_n_cancellations_per_batch != self.max_n_cancellations_per_batch:
                self.max_n_cancellations_per_batch = st.session_state.edit_multi_max_n_cancellations_per_batch
        if "edit_multi_max_n_creations_per_batch" in st.session_state:
            if st.session_state.edit_multi_max_n_creations_per_batch != self.max_n_creations_per_batch:
                self.max_n_creations_per_batch = st.session_state.edit_multi_max_n_creations_per_batch
        if "edit_multi_forced_mode_long" in st.session_state:
            if st.session_state.edit_multi_forced_mode_long != self.forced_mode_long:
                self.forced_mode_long = st.session_state.edit_multi_forced_mode_long
        if "edit_multi_forced_mode_short" in st.session_state:
            if st.session_state.edit_multi_forced_mode_short != self.forced_mode_short:
                self.forced_mode_short = st.session_state.edit_multi_forced_mode_short
        if "edit_multi_filter_by_min_effective_cost" in st.session_state:
            if st.session_state.edit_multi_filter_by_min_effective_cost != self.filter_by_min_effective_cost:
                self.filter_by_min_effective_cost = st.session_state.edit_multi_filter_by_min_effective_cost
        # Init symbols
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_symbol_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_symbol_{ed_key}']
            for row in ed["edited_rows"]:
                if "enable" in ed["edited_rows"][row]:
                    single = False
                    for instance in st.session_state.pbgui_instances:
                        if instance.user == self.user and instance.symbol == self._symbols[row]:
                            single = True
                            if not instance.multi and instance.enabled_on == "disabled":
                                instance.enabled_on = self.enabled_on
                                instance.multi = True
                            elif instance.multi:
                                instance.enabled_on = "disabled"
                                instance.multi = False
                            # else:
                            #     ed_key += 1
                    if not single:
                        config_file = Path(f'{self.instance_path}/{self._symbols[row]}.json')
                        config_file.unlink(missing_ok=True)
                        self._symbols.remove(self._symbols[row])
                if "edit" in ed["edited_rows"][row]:
                    for instance in st.session_state.pbgui_instances:
                        if instance.user == self.user and instance.symbol == self._symbols[row]:
                            st.session_state.edit_instance = instance
                            st.switch_page("pages/10_Single Run.py")
                    # Edit Symbol config without single instance
                    if not Path(self.instance_path).exists():
                        info_popup("You need to save the Multi config first, before editing a symbol")
                        return
                    config_file = Path(f'{self.instance_path}/{self._symbols[row]}.json')
                    config = Config(config_file)
                    config.load_config()
                    st.session_state.edit_multi_config = config
                    st.rerun()
        slist = []
        inactive_long = 0
        inactive_short = 0
        for id, symbol in enumerate(self._symbols):
            config = 'default'
            for instance in st.session_state.pbgui_instances:
                # Single instance config
                if instance.user == self.user and instance.symbol == symbol:
                    config = 'single'
                    if instance.multi or (not instance.multi and instance.enabled_on == "disabled"):
                        enable_multi = instance.multi
                    else:
                        enable_multi = None
                    multi_config = instance._config
                    # Setup mode
                    if multi_config.long_enabled:
                        long_mode = instance.long_mode
                        # Setup WE
                        long_we = multi_config.long_we
                    else:
                        long_we = 0.0
                        inactive_long += 1
                        if self.auto_gs:
                            long_mode = 'graceful_stop'
                        else:
                            long_mode = 'manual'
                    if multi_config.short_enabled:
                        short_mode = instance.short_mode
                        # Setup WE
                        short_we = multi_config.short_we
                    else:
                        short_we = 0.0
                        inactive_short += 1
                        if self.auto_gs:
                            short_mode = 'graceful_stop'
                        else:
                            short_mode = 'manual'
            if config == 'default':
                # local and default config are always enabled
                enable_multi = True
                # Local config
                config_file = Path(f'{self.instance_path}/{symbol}.json')
                if config_file.exists():
                    multi_config = Config(config_file)
                    multi_config.load_config()
                    config = "local"
                    # Setup mode
                    if multi_config.long_enabled:
                        long_mode = 'normal'
                        # Setup WE
                        long_we = multi_config.long_we
                    else:
                        long_we = 0.0
                        inactive_short += 1
                        if self.auto_gs:
                            long_mode = 'graceful_stop'
                        else:
                            long_mode = 'manual'
                    if multi_config.short_enabled:
                        short_mode = 'normal'
                        # Setup WE
                        short_we = multi_config.short_we
                    else:
                        short_we = 0.0
                        inactive_short += 1
                        if self.auto_gs:
                            short_mode = 'graceful_stop'
                        else:
                            short_mode = 'manual'
            if config == "default":
                multi_config = self.default_config
                if self.n_longs > 0:
                    long_we = self.TWE_long / self.n_longs
                    long_mode = 'normal'
                else:
                    if self.long_enabled:
                        long_mode = 'normal'
                        if len(self._symbols) == 0:
                            long_we = 0.0
                        else:
                            long_we = self.TWE_long / len(self._symbols)
                    else:
                        long_we = 0.0
                        if self.auto_gs:
                            long_mode = 'graceful_stop'
                        else:
                            long_mode = 'manual'  
                if self.n_shorts > 0:
                    short_we = self.TWE_short / self.n_shorts
                    short_mode = 'normal'
                else:
                    if self.short_enabled:
                        short_mode = 'normal'
                        if len(self._symbols) == 0:
                            short_we = 0.0
                        else:
                            short_we = self.TWE_short / len(self._symbols)
                    else:
                        short_we = 0.0
                        if self.auto_gs:
                            short_mode = 'graceful_stop'
                        else:
                            short_mode = 'manual'
            slist.append({
                'id': id,
                'enable': enable_multi,
                'edit': False,
                'symbol': symbol,
                'config': config,
                'long_mode' : long_mode,
                'long_we' : long_we,
                'short_mode' : short_mode,
                'short_we' : short_we
            })
        # recalculate long_we and short_we
        real_TWE_long = 0
        real_TWE_short = 0
        not_defaults_long = 0
        not_defaults_short = 0
        for id, symbol in enumerate(slist):
            if symbol["enable"]:
                if symbol['config'] != 'default':
                    if symbol['long_mode'] == 'normal':
                        not_defaults_long += 1
                        real_TWE_long += symbol['long_we']
                    if symbol['short_mode'] == 'normal':
                        not_defaults_short += 1
                        real_TWE_short += symbol['short_we']
                else:
                    if self.n_longs == 0 and self.n_shorts == 0:
                        if symbol['long_mode'] == 'normal':
                            # Correct long_we calculated from inactive symbols
                            if len(self._symbols) - inactive_long > 0:
                                slist[id]["long_we"] = self.TWE_long / (len(self._symbols) - inactive_long)
                                real_TWE_long += symbol['long_we']
                        if symbol['short_mode'] == 'normal':
                            # Correct short_we calculated from inactive symbols
                            if len(self._symbols) - inactive_short > 0:
                                slist[id]["short_we"] = self.TWE_short / (len(self._symbols) - inactive_short)
                                real_TWE_short += symbol['short_we']
                    else:
                        # Correct we calculated from inactive symbols
                        if self.n_longs - inactive_long > 0:
                            slist[id]["long_we"] = self.TWE_long / (self.n_longs - inactive_long)
                        if self.n_shorts - inactive_short > 0:
                            slist[id]["short_we"] = self.TWE_short / (self.n_shorts - inactive_short)
        # Calculate real TWE with inactive symbols
        if self.n_longs > 0:
            if self.n_longs > not_defaults_long:
                if (self.n_longs - inactive_long) * (self.n_longs - inactive_long - not_defaults_long) > 0:
                    real_TWE_long +=  self.TWE_long / (self.n_longs - inactive_long) * (self.n_longs - inactive_long - not_defaults_long)
        if self.n_shorts > 0:
            if self.n_shorts > not_defaults_short:
                if (self.n_shorts - inactive_short) * (self.n_shorts - inactive_short - not_defaults_short) > 0:
                    real_TWE_short +=  self.TWE_short / (self.n_shorts - inactive_short) * (self.n_shorts - inactive_short - not_defaults_short)
        column_config = {
            "id": None,
            "enable": st.column_config.CheckboxColumn(label="enable on multi", help="If no Checkbox is shown, Symbol is running as a Single Instance and can not be enabled on this Multi"),
            }
        # Display Editor
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.selectbox('User',self._users.list(), index = self._users.list().index(self.user), key="edit_multi_user")
        with col2:
            enabled_on = ["disabled",self.remote.name] + self.remote.list()
            enabled_on_index = enabled_on.index(self.enabled_on)
            st.selectbox('Enabled on',enabled_on, index = enabled_on_index, key="edit_multi_enabled_on")
            st.empty()
        with col3:
            st.number_input("config version", min_value=self.version, value=self.version, step=1, format="%.d", key="edit_multi_version", help=pbgui_help.config_version)
        with col4:
            st.number_input("leverage", min_value=0.0, max_value=10.0, value=self.leverage, step=1.0, format="%.1f", key="edit_multi_leverage", help=pbgui_help.leverage)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("loss_allowance_pct", min_value=0.0, max_value=100.0, value=self.loss_allowance_pct, step=0.001, format="%.3f", key="edit_multi_loss_allowance_pct", help=pbgui_help.loss_allowance_pct)
        with col2:
            st.number_input("pnls_max_lookback_days", min_value=0, max_value=365, value=self.pnls_max_lookback_days, step=1, format="%.d", key="edit_multi_pnls_max_lookback_days", help=pbgui_help.pnls_max_lookback_days)
        with col3:
            st.number_input("stuck_threshold", min_value=0.0, max_value=1.0, value=self.stuck_threshold, step=0.01, format="%.2f", key="edit_multi_stuck_threshold", help=pbgui_help.stuck_threshold)
        with col4:
            st.number_input("unstuck_close_pct", min_value=0.0, max_value=1.0, value=self.unstuck_close_pct, step=0.01, format="%.3f", key="edit_multi_unstuck_close_pct", help=pbgui_help.unstuck_close_pct)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.checkbox("long_enabled", value=self.long_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_multi_long_enabled")
        with col2:
            st.checkbox("short_enabled", value=self.short_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_multi_short_enabled")
        with col3:
            st.empty()
        with col4:
            st.checkbox("auto_gs", value=self.auto_gs, help=pbgui_help.auto_gs, key="edit_multi_auto_gs")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input(f"TWE_long (Real: {round(real_TWE_long,2)})", min_value=0.0, max_value=1000.0, value=self.TWE_long, step=0.1, format="%.2f", key="edit_multi_TWE_long", help=pbgui_help.TWE_long_short)
        with col2:
            st.number_input(f"TWE_short (Real: {round(real_TWE_short,2)})", min_value=0.0, max_value=1000.0, value=self.TWE_short, step=0.1, format="%.2f", key="edit_multi_TWE_short", help=pbgui_help.TWE_long_short)
        with col3:
            st.number_input("price_distance_threshold", min_value=0.0, max_value=1.0, value=self.price_distance_threshold, step=0.001, format="%.3f", key="edit_multi_price_distance_threshold", help=pbgui_help.price_distance_threshold)
        with col4:
            st.number_input("execution_delay_seconds", min_value=1, max_value=60, value=self.execution_delay_seconds, step=1, format="%.d", key="edit_multi_execution_delay_seconds", help=pbgui_help.execution_delay_seconds)
        # Forager Settings
        if self.n_longs == 0 and self.n_shorts == 0:
            forager_expanded = False
        else:
            forager_expanded = True
        with st.expander("Forager Settings", expanded=forager_expanded):
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                st.number_input("n_longs", min_value=0, max_value=100, value=self.n_longs, step=1, format="%.d", key="edit_multi_n_longs", help=pbgui_help.n_longs_shorts)
            with col2:
                st.number_input("n_shorts", min_value=0, max_value=100, value=self.n_shorts, step=1, format="%.d", key="edit_multi_n_shorts", help=pbgui_help.n_longs_shorts)
            with col3:
                st.number_input("minimum_market_age_days", min_value=0, max_value=365, value=self.minimum_market_age_days, step=1, format="%.d", key="edit_multi_minimum_market_age_days", help=pbgui_help.minimum_market_age_days)
            with col4:
                ohlcv_intervals = ['5m','15m','1h','4h','1d']
                st.selectbox('ohlcv_interval',ohlcv_intervals, index = ohlcv_intervals.index(self.ohlcv_interval), key="edit_multi_ohlcv_interval", help=pbgui_help.ohlcv_interval)
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                st.number_input("n_ohlcvs", min_value=0, max_value=100, value=self.n_ohlcvs, step=1, format="%.d", key="edit_multi_n_ohlcvs", help=pbgui_help.n_ohlcvs)
            with col2:
                st.number_input("relative_volume_filter_clip_pct", min_value=0.0, max_value=1.0, value=self.relative_volume_filter_clip_pct, step=0.01, format="%.2f", key="edit_multi_relative_volume_filter_clip_pct", help=pbgui_help.relative_volume_filter_clip_pct)
            with col3:
                st.empty()
            with col4:
                st.empty()
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
                st.number_input("max_n_cancellations_per_batch", min_value=0, max_value=100, value=self.max_n_cancellations_per_batch, step=1, format="%.d", key="edit_multi_max_n_cancellations_per_batch", help=pbgui_help.max_n_per_batch)
            with col2:
                st.number_input("max_n_creations_per_batch", min_value=0, max_value=100, value=self.max_n_creations_per_batch, step=1, format="%.d", key="edit_multi_max_n_creations_per_batch", help=pbgui_help.max_n_per_batch)
            with col3:
                st.selectbox('forced_mode_long',forced_mode, index = forced_mode.index(self.forced_mode_long), format_func=lambda x: mode_options.get(x), key="edit_multi_forced_mode_long", help=pbgui_help.forced_mode_long_short)
            with col4:
                st.selectbox('forced_mode_short',forced_mode, index = forced_mode.index(self.forced_mode_short), format_func=lambda x: mode_options.get(x) , key="edit_multi_forced_mode_short", help=pbgui_help.forced_mode_long_short)
            col1, col2, col3, col4 = st.columns([1,1,1,1])
            with col1:
                st.checkbox("filter_by_min_effective_cost", value=self.filter_by_min_effective_cost, help=pbgui_help.filter_by_min_effective_cost, key="edit_multi_filter_by_min_effective_cost")
            with col2:
                st.empty()
            with col3:
                st.empty()
            with col4:
                st.empty()            
        # Display Symbols
        st.data_editor(data=slist, height=36+(len(slist))*35, use_container_width=True, key=f'select_symbol_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['symbol','long','long_mode','long_we','short','short_mode','short_we'])
        # Remove unavailable symbols
        for symbol in self._symbols.copy():
            if symbol not in self._available_symbols:
                self._symbols.remove(symbol)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            if st.button("Add All to approved_symbols", key="edit_multi_add_all_to_approved"):
                for symbol in self._available_symbols:
                    if symbol not in self._symbols:
                        self._symbols.append(symbol)
                st.rerun()
        with col2:
            if st.button("Add CPT allowed to approved_symbols", key="edit_multi_add_cpt_to_approved"):
                for symbol in self._cpt_allowed_symbols:
                    if symbol not in self._symbols:
                        self._symbols.append(symbol)
                st.rerun()
        with col4:
            if st.button("Update Symbols from Exchange"):
                exchange = self._users.find_exchange(self.user)
                Exchange(exchange, self._users.find_user(self._user)).fetch_symbols()
                st.rerun()
        st.multiselect('approved_symbols', self._available_symbols, default=self._symbols, key="edit_multi_approved_symbols", help=pbgui_help.multi_approved_symbols)
        # Add Symbol to ignored_symbols
        for symbol in self._ignored_symbols:
            if symbol not in self._available_symbols:
                self._ignored_symbols.remove(symbol)
        if st.button("Add All to ignored_symbols", key="edit_multi_add_all_to_ignored"):
            for symbol in self._available_symbols:
                if symbol not in self._ignored_symbols:
                    self._ignored_symbols.append(symbol)
            st.rerun()
        st.multiselect('ignored_symbols', self._available_symbols, default=self._ignored_symbols, key="edit_multi_ignored_symbols", help=pbgui_help.multi_ignored_symbols)
        # Import configs
        import_path = os.path.abspath(st_file_selector(st, path=pbdir(), key = 'multi_import_config', label = 'Import from directory'))
        if st.button("Import Configs"):
            self.import_configs(import_path)
        with st.container(border=True):
            if self.default_config_path == "":
                config_type_index = 1
            else:
                config_type_index = 0
            st.radio("Select config type", ["default", "universal"], index=config_type_index, key="edit_multi_config_type", help=pbgui_help.multi_config_type,  captions=None, label_visibility="visible")
            if st.session_state.edit_multi_config_type == "default":
                self.default_config.edit_config()
            else:
                height = 600
                if not self.universal_live_config is None:
                    height = len(self.universal_live_config.splitlines()) *23
                if height < 600:
                    height = 600
                # Display Error
                if "error_config" in st.session_state:
                    st.error(st.session_state.error_config, icon="🚨")
                st.text_area("universal_live_config", self.universal_live_config, key="edit_multi_universal_live_config", help=pbgui_help.multi_universal_config, height=height)
        self.view_log()

    def import_configs(self, import_path: str):
        for symbol in self._symbols:
            config_file = Path(f'{import_path}/{symbol}.json')
            if config_file.exists():
                shutil.copy(config_file, f'{self.instance_path}/{symbol}.json')
        st.rerun()

    def activate(self):
        self.remote.local_run.activate(self.user, True)

class MultiInstances:
    def __init__(self):
        self.instances = []
        self.index = 0
        pbgdir = Path.cwd()
        # if not ipath:
        self.instances_path = f'{pbgdir}/data/multi'
        # else:
        #     self.instances_path = f'{pbgdir}/data/remote/multi_{ipath}'
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
    
    def remove(self, instance: MultiInstance):
        instance.remove()
        self.instances.remove(instance)
    
    def activate_all(self):
        for instance in self.instances:
            running_on = instance.is_running_on()
            if instance.enabled_on == 'disabled' and running_on:
                instance.remote.local_run.activate(instance.user, True)
            elif instance.enabled_on not in running_on:
                instance.remote.local_run.activate(instance.user, True)
            elif instance.is_running() and (instance.version != instance.running_version):
                instance.remote.local_run.activate(instance.user, True)

    def load(self):
        p = str(Path(f'{self.instances_path}/*'))
        instances = glob.glob(p)
        for instance in instances:
            inst = MultiInstance()
            if inst.load(instance):
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
