import streamlit as st
from bokeh.plotting import figure
from bokeh.palettes import Category20_20, Category20b_20, Category20c_20
from bokeh.models import NumeralTickFormatter, HoverTool
import pbgui_help
import json
import psutil
import sys
import platform
import hjson
import traceback
import subprocess
import shlex
import glob
import configparser
import time
import multiprocessing
import pandas as pd
from pbgui_func import PBDIR, PBGDIR, validateJSON, config_pretty_str
import uuid
from Base import Base
from Config import Config
from pathlib import Path, PurePath
from User import Users
from shutil import rmtree
import datetime

class BacktestMultiQueueItem():
    def __init__(self):
        self.name = None
        self.filename = None
        self.hjson = None
        self.exchange = None
        self.parameters = None
        self.log = None
        self.pid = None
        self.pidfile = None

    def remove(self):
        self.stop()
        file = Path(f'{PBGDIR}/data/bt_multi_queue/{self.filename}.json')
        file.unlink(missing_ok=True)
        self.log.unlink(missing_ok=True)
        self.pidfile.unlink(missing_ok=True)

    def load_log(self):
        if self.log:
            if self.log.exists():
                with open(self.log, 'r', encoding='utf-8') as f:
                    return f.read()

    def view_log(self):
        logfile = self.load_log()
        st.code(logfile)

    def status(self):
        if self.is_backtesting():
            return "backtesting..."
        if self.is_running():
            return "running"
        if self.is_finish():
            return "complete"
        if self.is_error():
            return "error"
        else:
            return "not started"

    def is_running(self):
        if not self.pid:
            self.load_pid()
        try:
            if self.pid and psutil.pid_exists(self.pid) and any(sub.lower().endswith("backtest_multi.py") for sub in psutil.Process(self.pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            pass
        return False

    def is_finish(self):
        log = self.load_log()
        if log:
            if "plotting for" in log:
                return True
            else:
                return False
        else:
            return False

    def is_error(self):
        log = self.load_log()
        if log:
            if "plotting for" in log:
                return False
            else:
                return True
        else:
            return False

    def is_backtesting(self):
        if self.is_running():
            log = self.load_log()
            if log:
                if "plotting for" in log:
                    return False
                elif "backtesting..." in log:
                    return True
            else:
                return False

    def stop(self):
        if self.is_running():
            p = psutil.Process(self.pid)
            p.kill()

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        with open(self.pidfile, 'w') as f:
            f.write(str(self.pid))

    def run(self):
        if not self.is_finish() and not self.is_running():
            if self.parameters:
                cmd = [sys.executable, '-u', PurePath(f'{PBDIR}/backtest_multi.py')]
                cmd.extend(shlex.split(self.parameters))
                cmd.extend(['-bc', self.hjson])
            else:
                cmd = [sys.executable, '-u', PurePath(f'{PBDIR}/backtest_multi.py'), '-bc', str(PurePath(f'{self.hjson}'))]
            log = open(self.log,"w")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                btm = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=PBDIR, text=True, creationflags=creationflags)
            else:
                btm = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=PBDIR, text=True, start_new_session=True)
            self.pid = btm.pid
            self.save_pid()

class BacktestMultiQueue:
    def __init__(self):
        self.items = []
        self.pb_config = configparser.ConfigParser()
        self.pb_config.read('pbgui.ini')
        if not self.pb_config.has_section("backtest_multi"):
            self.pb_config.add_section("backtest_multi")
        if not self.pb_config.has_option("backtest_multi", "cpu"):
            self.pb_config.set("backtest_multi", "autostart", "False")
            self.pb_config.set("backtest_multi", "cpu", "1")
        self._autostart = eval(self.pb_config.get("backtest_multi", "autostart"))
        self._cpu = int(self.pb_config.get("backtest_multi", "cpu"))
        if self._autostart:
            self.run()

    @property
    def cpu(self):
        self.pb_config.read('pbgui.ini')
        self._cpu = int(self.pb_config.get("backtest_multi", "cpu"))
        if self._cpu > multiprocessing.cpu_count():
            self._cpu = multiprocessing.cpu_count()
        return self._cpu

    @cpu.setter
    def cpu(self, new_cpu):
        self._cpu = new_cpu
        self.pb_config.set("backtest_multi", "cpu", str(self._cpu))
        with open('pbgui.ini', 'w') as f:
            self.pb_config.write(f)

    @property
    def autostart(self):
        return self._autostart

    @autostart.setter
    def autostart(self, new_autostart):
        self._autostart = new_autostart
        self.pb_config.set("backtest_multi", "autostart", str(self._autostart))
        with open('pbgui.ini', 'w') as f:
            self.pb_config.write(f)
        if self._autostart:
            self.run()
        else:
            self.stop()

    def add(self, qitem : BacktestMultiQueueItem):
        for index, item in enumerate(self.items):
            if item.filename == qitem.filename:
                return
        self.items.append(qitem)

    def remove_finish(self, all : bool = False):
        if all:
            self.stop()
        for item in self.items[:]:
            if item.is_finish():
                item.remove()
                self.items.remove(item)
            else:
                if all:
                    item.stop()
                    item.remove()
                    self.items.remove(item)
        if self._autostart:
            self.run()

    def remove_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'view_bt_queue_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    self.items[row].remove()
        self.items = []

    def running(self):
        r = 0
        for item in self.items:
            if item.is_running():
                r+=1
        return r

    def downloading(self):
        for item in self.items:
            if item.is_running() and not item.is_backtesting():
                return True
        return False
        
    def load(self):
        dest = Path(f'{PBGDIR}/data/bt_multi_queue')
        p = str(Path(f'{dest}/*.json'))
        items = glob.glob(p)
        # self.items = []
        for item in items:
            with open(item, "r", encoding='utf-8') as f:
                config = json.load(f)
                qitem = BacktestMultiQueueItem()
                qitem.name = config["name"]
                qitem.filename = config["filename"]
                qitem.hjson = config["hjson"]
                qitem.exchange = config["exchange"]
                qitem.parameters = config["parameters"]
                qitem.log = Path(f'{PBGDIR}/data/bt_multi_queue/{qitem.filename}.log')
                qitem.pidfile = Path(f'{PBGDIR}/data/bt_multi_queue/{qitem.filename}.pid')
                self.add(qitem)

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/BacktestMulti.py')]
            dest = Path(f'{PBGDIR}/data/logs')
            if not dest.exists():
                dest.mkdir(parents=True)
            logfile = Path(f'{dest}/BacktestMulti.log')
            if logfile.exists():
                if logfile.stat().st_size >= 1048576:
                    logfile.replace(f'{str(logfile)}.old')
            log = open(logfile,"a")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=PBGDIR, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=PBGDIR, text=True, start_new_session=True)

    def stop(self):
        if self.is_running():
            self.pid().kill()

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.AccessDenied:
                continue
            if any("BacktestMulti.py" in sub for sub in cmdline):
                return process

    def view(self):
        if not self.items:
            self.load()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'view_bt_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_bt_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if self.items[row].is_running():
                        self.items[row].stop()
                    else:
                        self.items[row].run()
                    # st.session_state.bt_multi = self.backtests[row]
                    # del st.session_state.bt_multi_list
                    # st.rerun()
                if "view" in ed["edited_rows"][row]:
                    bt = BacktestMultiItem(f'{PBGDIR}/data/bt_multi/{self.items[row].name}')
                    bt.load()
                    bt.load_results()
                    st.session_state.bt_multi_results = bt
                    del st.session_state.bt_multi_queue
                    st.rerun()
        d = []
        for id, bt in enumerate(self.items):
            d.append({
                'id': id,
                'run': False,
                'Status': bt.status(),
                'view': False,
                'log': False,
                'delete': False,
                'name': bt.name,
                'filename': bt.filename,
                'exchange': bt.exchange,
                'finish': bt.is_finish(),
            })
        column_config = {
            # "id": None,
            "run": st.column_config.CheckboxColumn('Start/Stop', default=False),
            "view": st.column_config.CheckboxColumn(label="View Results"),
            "log": st.column_config.CheckboxColumn(label="View Logfile"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display Queue
        height = 36+(len(d))*35
        if height > 1000: height = 1016
        st.data_editor(data=d, height=height, use_container_width=True, key=f'view_bt_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','filename','name','finish','running'])
        if f'view_bt_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_bt_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "log" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["log"]:
                        self.items[row].view_log()

class BacktestMultiItem:
    def __init__(self, backtest_path: str = None):
        self.path = backtest_path
        self.hjson = None
        self.log = None
        self.pbdir = None
        self.users = Users()
        self.backtest_results = []
        self.initialize()

    @property
    def exchange(self): return self._exchange
    @exchange.setter
    def exchange(self, new_exchange):
        if new_exchange != self._exchange:
            if new_exchange != "bybit":
                self._exchange = 'binance'
            else:
                self._exchange = new_exchange
            self.exchange_symbols = self.load_symbols()
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

    def initialize(self):
        self.name = ""
        self.sd = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.ed = datetime.date.today().strftime("%Y-%m-%d")
        self.sb = 1000
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
        self._exchange = 'binance'
        self.symbols = {}
        self.backtest_symbols = {}
        self.exchange_symbols = self.load_symbols()
        self.loss_allowance_pct_min = 0.01
        self.loss_allowance_pct_max = 0.1
        self.loss_allowance_pct_step = 0.01
        self.stuck_threshold_min = 0.86
        self.stuck_threshold_max = 0.95
        self.stuck_threshold_step = 0.01
        self.unstuck_close_pct_min = 0.005
        self.unstuck_close_pct_max = 0.05
        self.unstuck_close_pct_step = 0.005

    def load_symbols(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("exchanges", f'{self.exchange}.swap'):
            return eval(pb_config.get("exchanges", f'{self.exchange}.swap'))
        else:
            return []

    def create_from_multi(self, path: str):
        self.name = PurePath(path).name
        file = Path(f'{path}/multi.hjson')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    config = f.read()
                backtest_config = hjson.loads(config)
                if "version" in backtest_config:
                    del backtest_config["version"]
                if "enabled_on" in backtest_config:
                    del backtest_config["enabled_on"]
                if "user" in backtest_config:
                    user = backtest_config["user"]
                    self.exchange = self.users.find_exchange(user)
                if "loss_allowance_pct" in backtest_config:
                    self._loss_allowance_pct = float(backtest_config["loss_allowance_pct"])
                if "pnls_max_lookback_days" in backtest_config:
                    self._pnls_max_lookback_days = backtest_config["pnls_max_lookback_days"]
                if "stuck_threshold" in backtest_config:
                    self._stuck_threshold = float(backtest_config["stuck_threshold"])
                if "unstuck_close_pct" in backtest_config:
                    self._unstuck_close_pct = float(backtest_config["unstuck_close_pct"])
                if "execution_delay_seconds" in backtest_config:
                    self._execution_delay_seconds = backtest_config["execution_delay_seconds"]
                if "auto_gs" in backtest_config:
                    self._auto_gs = backtest_config["auto_gs"]
                if "TWE_long" in backtest_config:
                    self._TWE_long = float(backtest_config["TWE_long"])
                if "TWE_short" in backtest_config:
                    self._TWE_short = float(backtest_config["TWE_short"])
                if "long_enabled" in backtest_config:
                    self._long_enabled = backtest_config["long_enabled"]
                if "short_enabled" in backtest_config:
                    self._short_enabled = backtest_config["short_enabled"]
                if "symbols" in backtest_config:
                    symbols = backtest_config["symbols"]
                    for symbol, parameters in symbols.items():
                        config_file = Path(f'{path}/{symbol}.json')
                        if config_file.exists():
                            config = Config(config_file)
                            config.load_config()
                            self.symbols[symbol] = config
#                            self.configs.append(config)
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

    def optimize(self):
        LOSS_ALLOWANCE_PCT_MIN = 0.0
        LOSS_ALLOWANCE_PCT_MAX = 1.0
        STUCK_THRESHOLD_MIN = 0.0
        STUCK_THRESHOLD_MAX = 1.0
        UNSTUCK_CLOSE_PCT_MIN = 0.0
        UNSTUCK_CLOSE_PCT_MAX = 1.0

        # Init session_state for keys
        if "edit_bt_multi_loss_allowance_pct_min" in st.session_state:
            if st.session_state.edit_bt_multi_loss_allowance_pct_min != self.loss_allowance_pct_min:
                self.loss_allowance_pct_min = st.session_state.edit_bt_multi_loss_allowance_pct_min
        if "edit_bt_multi_loss_allowance_pct_max" in st.session_state:
            if st.session_state.edit_bt_multi_loss_allowance_pct_max != self.loss_allowance_pct_max:
                self.loss_allowance_pct_max = st.session_state.edit_bt_multi_loss_allowance_pct_max
        if "edit_bt_multi_loss_allowance_pct_step" in st.session_state:
            if st.session_state.edit_bt_multi_loss_allowance_pct_step != self.loss_allowance_pct_step:
                self.loss_allowance_pct_step = st.session_state.edit_bt_multi_loss_allowance_pct_step
        if "edit_bt_multi_stuck_threshold_min" in st.session_state:
            if st.session_state.edit_bt_multi_stuck_threshold_min != self.stuck_threshold_min:
                self.stuck_threshold_min = st.session_state.edit_bt_multi_stuck_threshold_min
        if "edit_bt_multi_stuck_threshold_max" in st.session_state:
            if st.session_state.edit_bt_multi_stuck_threshold_max != self.stuck_threshold_max:
                self.stuck_threshold_max = st.session_state.edit_bt_multi_stuck_threshold_max
        if "edit_bt_multi_stuck_threshold_step" in st.session_state:
            if st.session_state.edit_bt_multi_stuck_threshold_step != self.stuck_threshold_step:
                self.stuck_threshold_step = st.session_state.edit_bt_multi_stuck_threshold_step
        if "edit_bt_multi_unstuck_close_pct_min" in st.session_state:
            if st.session_state.edit_bt_multi_unstuck_close_pct_min != self.unstuck_close_pct_min:
                self.unstuck_close_pct_min = st.session_state.edit_bt_multi_unstuck_close_pct_min
        if "edit_bt_multi_unstuck_close_pct_max" in st.session_state:
            if st.session_state.edit_bt_multi_unstuck_close_pct_max != self.unstuck_close_pct_max:
                self.unstuck_close_pct_max = st.session_state.edit_bt_multi_unstuck_close_pct_max
        if "edit_bt_multi_unstuck_close_pct_step" in st.session_state:
            if st.session_state.edit_bt_multi_unstuck_close_pct_step != self.unstuck_close_pct_step:
                self.unstuck_close_pct_step = st.session_state.edit_bt_multi_unstuck_close_pct_step
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.loss_allowance_pct_counter = int(round((self.loss_allowance_pct_max - self.loss_allowance_pct_min) / self.loss_allowance_pct_step + 1, 0 ))
            st.number_input("Backtests", value=self.loss_allowance_pct_counter, format="%.d", key="edit_bt_multi_loss_allowance_pct_counter", disabled= True)
            self.stuck_threshold_counter = int(round((self.stuck_threshold_max - self.stuck_threshold_min) / self.stuck_threshold_step + 1, 0))
            st.number_input("Backtests", value=self.stuck_threshold_counter, format="%.d", key="edit_bt_multi_stuck_threshold_counter", disabled= True)
            self.unstuck_close_pct_counter = int(round((self.unstuck_close_pct_max - self.unstuck_close_pct_min) / self.unstuck_close_pct_step + 1, 0))
            st.number_input("Backtests", value=self.unstuck_close_pct_counter, format="%.d", key="edit_bt_multi_unstuck_close_pct_counter", disabled= True)
            backtests = self.loss_allowance_pct_counter * self.stuck_threshold_counter * self.unstuck_close_pct_counter
            st.number_input("Total Backtests", value=backtests, format="%.d", key="edit_bt_multi_backtests", disabled= True)
        with col2:
            st.number_input("loss_allowance_pct_min", min_value=LOSS_ALLOWANCE_PCT_MIN, max_value=self.loss_allowance_pct_max, value=self.loss_allowance_pct_min, step=0.01, format="%.3f", key="edit_bt_multi_loss_allowance_pct_min")
            st.number_input("stuck_threshold_min", min_value=STUCK_THRESHOLD_MIN, max_value=self.stuck_threshold_max, value=self.stuck_threshold_min, step=0.01, format="%.3f", key="edit_bt_multi_stuck_threshold_min")
            st.number_input("unstuck_close_pct_min", min_value=UNSTUCK_CLOSE_PCT_MIN, max_value=self.unstuck_close_pct_max, value=self.unstuck_close_pct_min, step=0.01, format="%.3f", key="edit_bt_multi_unstuck_close_pct_min")
            st.write(" ")
            st.write(" ")
            if self.name and self.hjson and self.symbols:
                if st.button("Generate Backtests"):
                    self.save()
                    self.generate_backtests()
        with col3:
            st.number_input("loss_allowance_pct_max", min_value=self.loss_allowance_pct_min, max_value=LOSS_ALLOWANCE_PCT_MAX, value=self.loss_allowance_pct_max, step=0.01, format="%.3f", key="edit_bt_multi_loss_allowance_pct_max")
            st.number_input("stuck_threshold_max", min_value=self.stuck_threshold_min, max_value=STUCK_THRESHOLD_MAX, value=self.stuck_threshold_max, step=0.01, format="%.3f", key="edit_bt_multi_stuck_threshold_max")
            st.number_input("unstuck_close_pct_max", min_value=self.unstuck_close_pct_min, max_value=UNSTUCK_CLOSE_PCT_MAX, value=self.unstuck_close_pct_max, step=0.01, format="%.3f", key="edit_bt_multi_unstuck_close_pct_max")
        with col4:
            st.number_input("loss_allowance_pct_step", min_value=0.001, max_value=0.1, value=self.loss_allowance_pct_step, step=0.005, format="%.3f", key="edit_bt_multi_loss_allowance_pct_step")
            st.number_input("stuck_threshold_step", min_value=0.01, max_value=0.1, value=self.stuck_threshold_step, step=0.01, format="%.3f", key="edit_bt_multi_stuck_threshold_step")
            st.number_input("unstuck_close_pct_step", min_value=0.001, max_value=0.1, value=self.unstuck_close_pct_step, step=0.005, format="%.3f", key="edit_bt_multi_unstuck_close_pct_step")

    def generate_backtests(self):
        count = 0
        for lap in range(0, self.loss_allowance_pct_counter):
            for st in range(0, self.stuck_threshold_counter):
                for ucp in range(0, self.unstuck_close_pct_counter):
                    bt_lap = round(self.loss_allowance_pct_min + lap * self.loss_allowance_pct_step, 3)
                    bt_st = round(self.stuck_threshold_min + st * self.stuck_threshold_step, 3)
                    bt_ucp = round(self.unstuck_close_pct_min + ucp * self.unstuck_close_pct_step, 3)
                    parameters = f'-lap {bt_lap} -st {bt_st} -ucp {bt_ucp}'
                    self.save_queue(parameters)

    def edit(self):
        # Init session_state for keys
        if "edit_bt_multi_exchange" in st.session_state:
            if st.session_state.edit_bt_multi_exchange != self.exchange:
                self.exchange = st.session_state.edit_bt_multi_exchange
        if "edit_bt_multi_name" in st.session_state:
            if st.session_state.edit_bt_multi_name != self.name:
                self.name = st.session_state.edit_bt_multi_name
        if "edit_bt_multi_sb" in st.session_state:
            if st.session_state.edit_bt_multi_sb != self.sb:
                self.sb = st.session_state.edit_bt_multi_sb
        if "edit_bt_multi_sd" in st.session_state:
            if st.session_state.edit_bt_multi_sd.strftime("%Y-%m-%d") != self.sd:
                self.sd = st.session_state.edit_bt_multi_sd.strftime("%Y-%m-%d")
        if "edit_bt_multi_ed" in st.session_state:
            if st.session_state.edit_bt_multi_ed.strftime("%Y-%m-%d") != self.ed:
                self.ed = st.session_state.edit_bt_multi_ed.strftime("%Y-%m-%d")
        if "edit_bt_multi_loss_allowance_pct" in st.session_state:
            if st.session_state.edit_bt_multi_loss_allowance_pct != self.loss_allowance_pct:
                self.loss_allowance_pct = st.session_state.edit_bt_multi_loss_allowance_pct
        if "edit_bt_multi_pnls_max_lookback_days" in st.session_state:
            if st.session_state.edit_bt_multi_pnls_max_lookback_days != self.pnls_max_lookback_days:
                self.pnls_max_lookback_days = st.session_state.edit_bt_multi_pnls_max_lookback_days
        if "edit_bt_multi_stuck_threshold" in st.session_state:
            if st.session_state.edit_bt_multi_stuck_threshold != self.stuck_threshold:
                self.stuck_threshold = st.session_state.edit_bt_multi_stuck_threshold
        if "edit_bt_multi_unstuck_close_pct" in st.session_state:
            if st.session_state.edit_bt_multi_unstuck_close_pct != self.unstuck_close_pct:
                self.unstuck_close_pct = st.session_state.edit_bt_multi_unstuck_close_pct
        if "edit_bt_multi_execution_delay_seconds" in st.session_state:
            if st.session_state.edit_bt_multi_execution_delay_seconds != self.execution_delay_seconds:
                self.execution_delay_seconds = st.session_state.edit_bt_multi_execution_delay_seconds
        if "edit_bt_multi_auto_gs" in st.session_state:
            if st.session_state.edit_bt_multi_auto_gs != self.auto_gs:
                self.auto_gs = st.session_state.edit_bt_multi_auto_gs
        if "edit_bt_multi_TWE_long" in st.session_state:
            if st.session_state.edit_bt_multi_TWE_long != self.TWE_long:
                self.TWE_long = st.session_state.edit_bt_multi_TWE_long
        if "edit_bt_multi_TWE_short" in st.session_state:
            if st.session_state.edit_bt_multi_TWE_long != self.TWE_short:
                self.TWE_short = st.session_state.edit_bt_multi_TWE_long
        if "edit_bt_multi_long_enabled" in st.session_state:
            if st.session_state.edit_bt_multi_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.edit_bt_multi_long_enabled
        if "edit_bt_multi_short_enabled" in st.session_state:
            if st.session_state.edit_bt_multi_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.edit_bt_multi_short_enabled
        #Init symbols
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        slist = []
        self.TWE_long = 0.0
        self.TWE_short = 0.0
        for id, symbol in enumerate(self.symbols):
            long_enabled = self.symbols[symbol].long_enabled
            long_we = self.symbols[symbol].long_we
            short_enabled = self.symbols[symbol].short_enabled
            short_we = self.symbols[symbol].short_we
            if long_enabled:
                self.TWE_long += long_we
            if short_enabled:
                self.TWE_short += short_we
            slist.append({
                'id': id,
                'edit': False,
                'delete': False,
                'symbol': symbol,
                'long' : long_enabled,
                'long_we' : long_we,
                'short' : short_enabled,
                'short_we' : short_we
            })
        column_config = {
            "id": None,
            }
        # Display Editor
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            exchange_index = 0
            if self.exchange == "bybit":
                exchange_index = 1
            st.selectbox('Exchange',['binance', 'bybit'], index = exchange_index, key="edit_bt_multi_exchange")
        with col2:
            if not self.name:
                color = "red"
            else:
                color = None
            st.text_input(f":{color}[Backtest Name]", value=self.name, max_chars=64, key="edit_bt_multi_name")
        with col3:
            st.date_input("START_DATE", datetime.datetime.strptime(self.sd, '%Y-%m-%d'), format="YYYY-MM-DD", key="edit_bt_multi_sd")
        with col4:
            st.date_input("END_DATE", datetime.datetime.strptime(self.ed, '%Y-%m-%d'), format="YYYY-MM-DD", key="edit_bt_multi_ed")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("loss_allowance_pct", min_value=0.0, max_value=100.0, value=self.loss_allowance_pct, step=0.001, format="%.3f", key="edit_bt_multi_loss_allowance_pct", help=pbgui_help.loss_allowance_pct)
        with col2:
            st.number_input("pnls_max_lookback_days", min_value=0, max_value=365, value=self.pnls_max_lookback_days, step=1, format="%.d", key="edit_bt_multi_pnls_max_lookback_days", help=pbgui_help.pnls_max_lookback_days)
        with col3:
            st.number_input("stuck_threshold", min_value=0.0, max_value=1.0, value=self.stuck_threshold, step=0.01, format="%.2f", key="edit_bt_multi_stuck_threshold", help=pbgui_help.stuck_threshold)
        with col4:
            st.number_input("unstuck_close_pct", min_value=0.0, max_value=1.0, value=self.unstuck_close_pct, step=0.01, format="%.3f", key="edit_bt_multi_unstuck_close_pct", help=pbgui_help.unstuck_close_pct)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.checkbox("long_enabled", value=self.long_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_bt_multi_long_enabled")
        with col2:
            st.checkbox("short_enabled", value=self.short_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_bt_multi_short_enabled")
        with col3:
            st.empty()
        with col4:
            st.checkbox("auto_gs", value=self.auto_gs, help=pbgui_help.auto_gs, key="edit_bt_multi_auto_gs")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input("TWE_long", min_value=0.0, max_value=100.0, value=self.TWE_long, step=0.1, format="%.2f", key="edit_bt_multi_TWE_long", disabled= True, help=pbgui_help.TWE_long_short)
        with col2:
            st.number_input("TWE_short", min_value=0.0, max_value=100.0, value=self.TWE_short, step=0.1, format="%.2f", key="edit_bt_multi_TWE_short", disabled= True, help=pbgui_help.TWE_long_short)
        with col3:
            st.number_input('STARTING_BALANCE',value=self.sb,step=500, key="edit_bt_multi_sb")
        with col4:
            st.number_input("execution_delay_seconds", min_value=1, max_value=60, value=self.execution_delay_seconds, step=1, format="%.d", key="edit_bt_multi_execution_delay_seconds", help=pbgui_help.execution_delay_seconds)
        # Display Symbols
        st.data_editor(data=slist, height=36+(len(slist))*35, use_container_width=True, key=f'select_symbol_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['symbol','long','long_we','short','short_we'])
        if f'select_symbol_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_symbol_{ed_key}']
            for row in ed["edited_rows"]:
                if "delete" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["delete"]:
                        del self.symbols[list(self.symbols.keys())[row]]
                        st.rerun()
                if "edit" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["edit"]:
                        st.session_state.bt_multi_edit_symbol = list(self.symbols.keys())[row]
                        st.session_state.ed_key += 1
                        st.rerun()
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.selectbox('SYMBOL', self.exchange_symbols, key="edit_bt_multi_exchange_symbol")
            if self.path:
                if Path(self.path).exists:
                    with col2:
                        st.write(" ")
                        st.write(" ")
                        if st.button("Add Symbol", key="button_add_symbol_backtest_multi"):
                            new_symbol = st.session_state.edit_bt_multi_exchange_symbol
                            if new_symbol not in self.symbols:
                                config_file = Path(f'{self.path}/{new_symbol}.json')
                                config = Config(config_file)
                                config.load_config()
                                self.symbols[new_symbol] = config   
                                st.session_state.bt_multi_edit_symbol = new_symbol
                                st.rerun()

    def remove_selected_results(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btmulti_result_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    self.backtest_results[row].remove()
        for result in self.backtest_results[:]:
            if not Path(result.result_path).exists():
                self.backtest_results.remove(result)

    def remove_all_results(self):
        rmtree(f'{PBDIR}/backtests/pbgui_multi/{self.name}/multisymbol', ignore_errors=True)
        self.backtest_results = []

    def view_results(self):
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        d = []
        for id, result in enumerate(self.backtest_results):
            d.append({
                'id': id,
                'view': False,
                'plot': False,
                'drawdowns': False,
                'delete': False,
                'starting_balance': result.starting_balance,
                'final_balance': result.final_balance,
                'drawdown_max': result.drawdown_max,
            })
        column_config = {
            "id": None,
            "view": st.column_config.CheckboxColumn(label="View Result"),
            "plot": st.column_config.CheckboxColumn(label="View Plots"),
            "drawdowns": st.column_config.CheckboxColumn(label="View Drawdowns"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display Backtests
        height = 36+(len(d))*35
        if height > 1000: height = 1016
        st.data_editor(data=d, height=height, use_container_width=True, key=f'select_btmulti_result_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','drawdown_max','final_balance'])
        if f'select_btmulti_result_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_btmulti_result_{ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        self.backtest_results[row].load_stats()
                        self.backtest_results[row].load_fills()
                        self.backtest_results[row].create_chart_be()
                        self.backtest_results[row].create_chart_sym(self.symbols)
                        self.backtest_results[row].view()
                if "plot" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["plot"]:
                        self.backtest_results[row].view_plots()
                if "drawdowns" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["drawdowns"]:
                        self.backtest_results[row].view_drawdowns()

    def create_chart(self):
        hover_be = HoverTool(
            tooltips=[
                ( 'name',   '$name'            ),
                ( 'date',   '@x{%F}'            ),
                ( 'total', '@y{0.00} $'      ),
            ],
            formatters={
                '@x'           : 'datetime', # use 'datetime' formatter for '@date' field
            },
            # display a tooltip whenever the cursor is vertically in line with a glyph
            mode='mouse'
        )
        self.color = 0
        self.be = None
        self.be = figure(
            x_axis_label='date',
            y_axis_label='USDT',
            x_axis_type='datetime',
            tools = "pan,box_zoom,wheel_zoom,save,reset",
            active_scroll="wheel_zoom")
        self.be.add_tools(hover_be)

    def add_chart(self, fills: pd):
        self.be.line(fills['time'], fills['balance'], legend_label='balance', line_width=2, color=Category20b_20[0], name=f'balance')
        self.be.line(fills['time'], fills['equity'], legend_label='equity', line_width=1, color=Category20b_20[1], name=f'equity')
        for symbol, config in self.symbols.items():
            symbol_df = fills[fills['symbol'] == symbol]
            symbol_df["sym_balance"] = symbol_df["pnl"].cumsum()
            # symbol_df["sym_balance"] = symbol_df["sym_balance"].apply(lambda x: x + 0 + symbol_df["pnl"].sum())
            self.be.line(symbol_df['time'], symbol_df['sym_balance'], legend_label=f'{symbol} pnl', line_width=1, color=Category20_20[self.color], name=f'{symbol}')
            self.color +=1

    def create_backtest_symbols(self):
        for symbol, config in self.symbols.items():
            config.config_file = Path(f'{PBGDIR}/data/bt_multi/{self.name}/{symbol}.json')
            config.save_config()
            if config.long_enabled:
                lw = config.long_we
            else:
                lw = 0.0
            if config.short_enabled:
                sw = config.short_we
            else:
                sw = 0.0
            self.backtest_symbols[symbol] = f'-lw {lw} -sw {sw} -lc {config.config_file}'

    def create_backtest_config(self):
        self.create_backtest_symbols()
        base_dir = f'backtests/pbgui_multi/{self.name}'
        with open(self.hjson, "w", encoding='utf-8') as f:
            f.write(hjson.dumps({
                "exchange": self.exchange,
                "loss_allowance_pct": self.loss_allowance_pct,
                "stuck_threshold": self.stuck_threshold,
                "unstuck_close_pct": self.unstuck_close_pct,
                "TWE_long": self.TWE_long,
                "TWE_short": self.TWE_short,
                "long_enabled": self.long_enabled,
                "short_enabled": self.short_enabled,
                "start_date": self.sd,
                "end_date": self.ed,
                "starting_balance": self.sb,
                "symbols": self.backtest_symbols,
                "live_configs_dir": "",
                "default_config_path": "",
                "base_dir": base_dir
            }, indent=4))

    def load(self):
        self.name = PurePath(self.path).name
        self.hjson = Path(f'{self.path}/backtest.hjson')
        if self.hjson.exists():
            try:
                with open(self.hjson, "r", encoding='utf-8') as f:
                    config = f.read()
                backtest_config = hjson.loads(config)
                if "exchange" in backtest_config:
                    self.exchange = backtest_config["exchange"]
                if "loss_allowance_pct" in backtest_config:
                    self._loss_allowance_pct = float(backtest_config["loss_allowance_pct"])
                if "pnls_max_lookback_days" in backtest_config:
                    self._pnls_max_lookback_days = backtest_config["pnls_max_lookback_days"]
                if "stuck_threshold" in backtest_config:
                    self._stuck_threshold = float(backtest_config["stuck_threshold"])
                if "unstuck_close_pct" in backtest_config:
                    self._unstuck_close_pct = float(backtest_config["unstuck_close_pct"])
                if "execution_delay_seconds" in backtest_config:
                    self._execution_delay_seconds = backtest_config["execution_delay_seconds"]
                if "auto_gs" in backtest_config:
                    self._auto_gs = backtest_config["auto_gs"]
                if "TWE_long" in backtest_config:
                    self._TWE_long = float(backtest_config["TWE_long"])
                if "TWE_short" in backtest_config:
                    self._TWE_short = float(backtest_config["TWE_short"])
                if "long_enabled" in backtest_config:
                    self._long_enabled = backtest_config["long_enabled"]
                if "short_enabled" in backtest_config:
                    self._short_enabled = backtest_config["short_enabled"]
                if "symbols" in backtest_config:
                    symbols = backtest_config["symbols"]
                    for symbol, parameters in symbols.items():
                        config_file = Path(f'{self.path}/{symbol}.json')
                        if config_file.exists():
                            config = Config(config_file)
                            config.load_config()
                            self.symbols[symbol] = config
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()
    
    def calculate_results(self):
        p = str(Path(f'{PBDIR}/backtests/pbgui_multi/{self.name}/multisymbol/{self.exchange}/**/analysis.json'))
        files = glob.glob(p, recursive=False)
        return len(files)

    def load_results(self):
        p = str(Path(f'{PBDIR}/backtests/pbgui_multi/{self.name}/multisymbol/{self.exchange}/**/analysis.json'))
        files = glob.glob(p, recursive=False)
        for file in files:
            result_path = PurePath(file).parent
            bt_result = BacktestMultiResult(result_path)
            self.backtest_results.append(bt_result)
        
    def save(self):
        self.path = Path(f'{PBGDIR}/data/bt_multi/{self.name}')
        if not self.path.exists():
            self.path.mkdir(parents=True)
        self.hjson = Path(f'{self.path}/backtest.hjson')
        self.create_backtest_config()

    def save_queue(self, parameters : str = None):
        dest = Path(f'{PBGDIR}/data/bt_multi_queue')
        unique_filename = str(uuid.uuid4())
        file = Path(f'{dest}/{unique_filename}.json') 
        bt_dict = {
            "name": self.name,
            "filename": unique_filename,
            "hjson": str(self.hjson),
            "parameters": parameters,
            "exchange": self.exchange,
        }
        if not dest.exists():
            dest.mkdir(parents=True)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(bt_dict, f, indent=4)

class BacktestMultiResult:
    def __init__(self, result_path: str = None):
        self.result_path = result_path
        self.initialize()
    
    def initialize(self):
        self.result = self.load_result()
        self.backtest_config = self.load_backtest_config()
        self.sd = self.backtest_config["start_date"]
        self.drawdown_max = self.result["drawdown_max"]
        self.final_balance = self.result["final_balance"]
        self.starting_balance = self.result["starting_balance"]
        self.stats = None
        self.fills = None
    
    def remove(self):
        rmtree(self.result_path)

    def load_result(self):
        r = Path(f'{self.result_path}/analysis.json')
        try:
            with open(r, "r", encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f'{str(r)} is corrupted {e}')
    
    def load_backtest_config(self):
        r = Path(f'{self.result_path}/backtest_config.hjson')
        try:
            with open(r, "r", encoding='utf-8') as f:
                return hjson.load(f)
        except Exception as e:
            print(f'{str(r)} is corrupted {e}')

    def load_stats(self):
        if self.stats is None:
            stats = f'{self.result_path}/stats.csv'
            self.stats = pd.read_csv(stats)
            self.stats['time'] = datetime.datetime.strptime(self.sd, '%Y-%m-%d') + pd.TimedeltaIndex(self.stats['minute'], unit='m')

    def load_fills(self):
        if self.fills is None:
            fills = f'{self.result_path}/fills.csv'
            self.fills = pd.read_csv(fills)
            self.fills['time'] = datetime.datetime.strptime(self.sd, '%Y-%m-%d') + pd.TimedeltaIndex(self.fills['minute'], unit='m')

    def view_plots(self):
        balance_and_equity = Path(f'{self.result_path}/balance_and_equity.png')
        cumulative_pnls = Path(f'{self.result_path}/cumulative_pnls.png')
        cumulative_pnls_long_short = Path(f'{self.result_path}/cumulative_pnls_long_short.png')
        drawdowns = Path(f'{self.result_path}/drawdowns.png')
        worst_drawdown = Path(f'{self.result_path}/worst_drawdown.png')
        if balance_and_equity.exists():
            st.image(str(balance_and_equity))
        if cumulative_pnls.exists():
            st.image(str(cumulative_pnls))
        if cumulative_pnls_long_short.exists():
            st.image(str(cumulative_pnls_long_short))
        if drawdowns.exists():
            st.image(str(drawdowns))
        if worst_drawdown.exists():
            st.image(str(worst_drawdown))
    
    def view_drawdowns(self):
        p = str(Path(f'{self.result_path}/drawdown_inspections/*.png'))
        drawdowns = glob.glob(p)
        for drawdown in drawdowns:
            st.image(drawdown)

    def view(self):
        col1, col2 = st.columns([1,1])
        with col1:
            st.code(json.dumps(self.result, indent=4))
        with col2:
            st.code(hjson.dumps(self.backtest_config, indent=4))

    def create_chart_be(self):
        # self.be = None
        hover = HoverTool(
            tooltips=[
                ( 'name',   '$name'            ),
                ( 'date',   '@x{%F}'            ),
                ( 'total', '@y{0,0.00} $'      ),
            ],
            formatters={
                '@x'           : 'datetime', # use 'datetime' formatter for '@date' field
            },
            # display a tooltip whenever the cursor is vertically in line with a glyph
            mode='mouse'
        )
        self.be = figure(
            plot_height=800,
            x_axis_label='date',
            y_axis_label='USDT',
            x_axis_type='datetime',
            tools = "pan,box_zoom,wheel_zoom,save,reset",
            active_scroll="wheel_zoom")
        self.be.add_tools(hover)
        self.be.line(self.stats['time'], self.stats['balance'], legend_label='balance', line_width=2, color=Category20_20[0], name=f'balance')
        self.be.line(self.stats['time'], self.stats['equity'], legend_label='equity', line_width=1, color=Category20_20[1], name=f'equity')
        be_leg = self.be.legend[0]
        self.be.add_layout(be_leg,'right')
        self.be.legend.location = "top_left"
        self.be.legend.click_policy="hide"
        self.be.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        st.bokeh_chart(self.be, use_container_width=True)

    def create_chart_sym(self, symbols: dict):
        hover = HoverTool(
            tooltips=[
                ( 'name',   '$name'            ),
                ( 'date',   '@x{%F}'            ),
                ( 'total', '@y{0,0.00} $'      ),
            ],
            formatters={
                '@x'           : 'datetime', # use 'datetime' formatter for '@date' field
            },
            # display a tooltip whenever the cursor is vertically in line with a glyph
            mode='mouse'
        )
        self.sym_color = 0
        # self.sym = None
        self.sym = figure(
            plot_height=800,
            x_axis_label='date',
            y_axis_label='USDT',
            x_axis_type='datetime',
            tools = "pan,box_zoom,wheel_zoom,save,reset",
            active_scroll="wheel_zoom")
        self.sym.add_tools(hover)
        for symbol in symbols:
            symbol_df = self.fills[self.fills['symbol'] == symbol].copy()
            symbol_df["sym_balance"] = symbol_df["pnl"].cumsum()
            pd.options.display.float_format = '{:.2f}'.format
            self.sym.line(symbol_df['time'], symbol_df['sym_balance'], legend_label=f'{symbol}', line_width=2, color=Category20_20[self.sym_color], name=f'{symbol}')
            self.sym_color +=1
        sym_leg = self.sym.legend[0]
        self.sym.add_layout(sym_leg,'right')
        self.sym.legend.location = "top_left"
        self.sym.legend.click_policy="hide"
        self.sym.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        st.bokeh_chart(self.sym, use_container_width=True)


class BacktestsMulti:
    def __init__(self):
        self.backtests = []

    def view_backtests(self):
        # Init
        if not self.backtests:
            self.find_backtests()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_backtest_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_backtest_{ed_key}']
            for row in ed["edited_rows"]:
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.bt_multi = self.backtests[row]
                    st.rerun()
                if "view" in ed["edited_rows"][row]:
                    st.session_state.bt_multi_results = self.backtests[row]
                    st.rerun()
        d = []
        for id, bt in enumerate(self.backtests):
            d.append({
                'id': id,
                'edit': False,
                'Name': bt.name,
                'view': False,
                'Backtests': bt.calculate_results(),
            })
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Results"),
            }
        #Display Backtests
        st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_backtest_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def find_backtests(self):
        p = str(Path(f'{PBGDIR}/data/bt_multi/**/backtest.hjson'))
        found_bt = glob.glob(p, recursive=False)
        if found_bt:
            for p in found_bt:
                bt = BacktestMultiItem(PurePath(p).parent)
                bt.load()
                self.backtests.append(bt)
    
def main():
    bt = BacktestMultiQueue()
    while True:
        bt.load()
        for item in bt.items:
            while bt.running() == bt.cpu:
                time.sleep(5)
            while bt.downloading():
                time.sleep(5)
            bt.pb_config.read('pbgui.ini')
            if not eval(bt.pb_config.get("backtest_multi", "autostart")):
                return
            if item.status() == "not started":
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Backtesting {item.filename} started')
                item.run()
                time.sleep(1)
        time.sleep(60)

if __name__ == '__main__':
    main()