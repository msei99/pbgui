import streamlit as st
import streamlit_scrollable_textbox as stx
import pbgui_help
from PBRemote import PBRemote
from User import Users
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
#        self._user = "manibybit08"
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
                del st.session_state.edit_multi_loss_allowance_pct
                del st.session_state.edit_multi_pnls_max_lookback_days
                del st.session_state.edit_multi_stuck_threshold
                del st.session_state.edit_multi_unstuck_close_pct
                del st.session_state.edit_multi_execution_delay_seconds
                del st.session_state.edit_multi_auto_gs
                del st.session_state.edit_multi_TWE_long
                del st.session_state.edit_multi_TWE_short
                del st.session_state.edit_multi_long_enabled
                del st.session_state.edit_multi_short_enabled
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
        self._loss_allowance_pct = 0.002
        self._pnls_max_lookback_days = 30
        self._stuck_threshold = 0.9
        self._unstuck_close_pct = 0.01
        self._execution_delay_seconds = 2
        self._auto_gs = True
        self._TWE_long = 2.0
        self._TWE_short = 0.1
        self._long_enabled = True
        self._short_enabled = False
        self._symbols = {}
        if "user" in self._multi_config:
            self._user = self._multi_config["user"]
        if "enabled_on" in self._multi_config:
            self._enabled_on = self._multi_config["enabled_on"]
        if "version" in self._multi_config:
            self._version = self._multi_config["version"]
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
        # Load instances from user
        for instance in st.session_state.pbgui_instances:
            if instance.user == self.user and instance.market_type == "futures" :
                self._symbols[instance.symbol] = True
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
        self.TWE_long = 0.0
        self.TWE_short = 0.0
        for instance in st.session_state.pbgui_instances:
            if instance.user == self.user and instance.market_type == "futures":
                if instance.multi:
                    if instance._config.long_enabled:
                        lm = f'-lm n'
                        lw = f'-lw {instance._config.long_we}'
                        self.TWE_long += {instance._config.long_we}
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
                        self.TWE_short += {instance._config.short_we}
                    else:
                        sm = f'-sm m'
                        sw = f'-sw 0.0'
                    if instance.short_mode == "graceful_stop":
                        sm = f'-sm gs'
                    elif instance.short_mode == "panic":
                        sm = f'-sm p'
                    elif instance.short_mode == "tp_only":
                        sm = f'-sm t'
                    if instance.price_precision != 0.0:
                        pp = f' -pp {instance.price_precision} '
                    else:
                        pp = ""
                    if instance.price_step != 0.0:
                        ps = f' -ps {instance.price_step} '
                    else:
                        ps = ""
                    symbols[instance.symbol] = f'{lm} {lw} {sm} {sw}{pp}{ps}'.rstrip()
                    shutil.copy(f'{instance.instance_path}/config.json', f'{self.instance_path}/{instance.symbol}.json')
                else:
                    Path(f'{self.instance_path}/{instance.symbol}.json').unlink(missing_ok=True)
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
        # print(f'Error load Instance: {str(file)}')
        # return False

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
        self._multi_config["loss_allowance_pct"] = self.loss_allowance_pct
        self._multi_config["pnls_max_lookback_days"] = self.pnls_max_lookback_days
        self._multi_config["stuck_threshold"] = self.stuck_threshold
        self._multi_config["unstuck_close_pct"] = self.unstuck_close_pct
        self._multi_config["execution_delay_seconds"] = self.execution_delay_seconds
        self._multi_config["auto_gs"] = self.auto_gs
        self._multi_config["symbols"] = self.generate_active_symbols()
        self._multi_config["TWE_long"] = self.TWE_long
        self._multi_config["TWE_short"] = self.TWE_short
        self._multi_config["long_enabled"] = self.long_enabled
        self._multi_config["short_enabled"] = self.short_enabled
        config = hjson.dumps(self._multi_config)
        with open(multi_config, "w", encoding='utf-8') as f:
            f.write(config)

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
        if "edit_multi_auto_gs" in st.session_state:
            if st.session_state.edit_multi_auto_gs != self.auto_gs:
                self.auto_gs = st.session_state.edit_multi_auto_gs
        if "edit_multi_TWE_long" in st.session_state:
            if st.session_state.edit_multi_TWE_long != self.TWE_long:
                self.TWE_long = st.session_state.edit_multi_TWE_long
        if "edit_multi_TWE_short" in st.session_state:
            if st.session_state.edit_multi_TWE_long != self.TWE_short:
                self.TWE_short = st.session_state.edit_multi_TWE_long
        if "edit_multi_long_enabled" in st.session_state:
            if st.session_state.edit_multi_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.edit_multi_long_enabled
        if "edit_multi_short_enabled" in st.session_state:
            if st.session_state.edit_multi_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.edit_multi_short_enabled
        # Init symbols
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_symbol_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_symbol_{ed_key}']
            for row in ed["edited_rows"]:
                if "enable" in ed["edited_rows"][row]:
                    for instance in st.session_state.pbgui_instances:
                        if instance.user == self.user and instance.symbol == list(self._symbols.keys())[row]:
                            instance.multi = not instance.multi
#                            st.rerun()
                if "edit" in ed["edited_rows"][row]:
                    for instance in st.session_state.pbgui_instances:
                        if instance.user == self.user and instance.symbol == list(self._symbols.keys())[row]:
                            st.session_state.edit_instance = instance
                            st.switch_page("pages/1_Live.py")
        slist = []
        self.TWE_long = 0.0
        self.TWE_short = 0.0
        for id, symbol in enumerate(self._symbols):
            for instance in st.session_state.pbgui_instances:
                if instance.user == self.user and instance.symbol == symbol:
                    enable_multi = instance.multi
                    long_enabled = instance._config.long_enabled
                    long_we = instance._config.long_we
                    long_mode = instance.long_mode
                    short_enabled = instance._config.short_enabled
                    short_we = instance._config.short_we
                    short_mode = instance.short_mode
                    if long_enabled and enable_multi:
                        self.TWE_long += long_we
                    if not long_enabled: 
                        long_we = 0.0
                        if long_mode == "normal":
                            long_mode = "manual"
                    if short_enabled and enable_multi:
                        self.TWE_short += short_we
                    if not short_enabled: 
                        short_we = 0.0
                        if short_mode == "normal":
                            short_mode = "manual"
                    if not long_enabled and long_mode == "normal":
                        short_mode = "manual"
                    if not long_enabled and long_mode == "normal":
                        short_mode = "manual"
            slist.append({
                'id': id,
                'enable': enable_multi,
                'edit': False,
                'symbol': symbol,
                'long' : long_enabled,
                'long_mode' : long_mode,
                'long_we' : long_we,
                'short' : short_enabled,
                'short_mode' : short_mode,
                'short_we' : short_we
            })
        column_config = {
            "id": None,
            "inst": None
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
            st.empty()
        with col4:
            st.number_input("config version", min_value=self.version, value=self.version, step=1, format="%.d", key="edit_multi_version", help=pbgui_help.config_version)
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
            st.number_input("TWE_long", min_value=0.0, max_value=100.0, value=self.TWE_long, step=0.1, format="%.2f", key="edit_multi_TWE_long", disabled= True, help=pbgui_help.TWE_long_short)
        with col2:
            st.checkbox("short_enabled", value=self.short_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_multi_short_enabled")
            st.number_input("TWE_short", min_value=0.0, max_value=100.0, value=self.TWE_short, step=0.1, format="%.2f", key="edit_multi_TWE_short", disabled= True, help=pbgui_help.TWE_long_short)
        with col3:
            st.empty()
        with col4:
            st.checkbox("auto_gs", value=self.auto_gs, help=pbgui_help.auto_gs, key="edit_multi_auto_gs")
            st.number_input("execution_delay_seconds", min_value=1, max_value=60, value=self.execution_delay_seconds, step=1, format="%.d", key="edit_multi_execution_delay_seconds", help=pbgui_help.execution_delay_seconds)
        # Display Symbols
        st.data_editor(data=slist, height=36+(len(slist))*35, use_container_width=True, key=f'select_symbol_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['symbol','long','long_mode','long_we','short','short_mode','short_we'])
        # display passivbot.log
        self.view_log()

    def activate(self):
        self.remote.local_run.activate(self.user, True)

class MultiInstances:
    def __init__(self, ipath: str = None):
        self.instances = []
        self.index = 0
        pbgdir = Path.cwd()
        if not ipath:
            self.instances_path = f'{pbgdir}/data/multi'
        else:
            self.instances_path = f'{pbgdir}/data/remote/multi_{ipath}'
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
