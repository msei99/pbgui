import streamlit as st
import pbgui_help
import json
import psutil
import sys
import platform
import hjson
import traceback
import subprocess
import glob
import configparser
import time
import multiprocessing
from pbgui_func import pbdir, pbvenv, PBGDIR, load_symbols_from_ini, error_popup, info_popup, get_navi_paths, replace_special_chars
import uuid
from pathlib import Path, PurePath
from User import Users
from shutil import rmtree
import datetime
from MultiBounds import MultiBounds
from BacktestMulti import BacktestMultiItem
import logging

class OptimizeMultiQueueItem():
    def __init__(self):
        self.name = None
        self.filename = None
        self.hjson = None
        self.exchange = None
        self.log = None
        self.pid = None
        self.pidfile = None

    def remove(self):
        self.stop()
        file = Path(f'{PBGDIR}/data/opt_multi_queue/{self.filename}.json')
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
        if self.is_optimizing():
            return "optimizing..."
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
            if self.pid and psutil.pid_exists(self.pid) and any(sub.lower().endswith("optimize_multi.py") for sub in psutil.Process(self.pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            pass
        return False

    def is_finish(self):
        log = self.load_log()
        if log:
            if "clean shutdown" in log:
                return True
            else:
                return False
        else:
            return False

    def is_error(self):
        log = self.load_log()
        if log:
            if "clean shutdown" in log:
                return False
            else:
                return True
        else:
            return False

    def is_optimizing(self):
        if self.is_running():
            log = self.load_log()
            if log:
                if "clean shutdown" in log:
                    return False
                elif "starting optimize" in log:
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
            cmd = [pbvenv(), '-u', PurePath(f'{pbdir()}/optimize_multi.py'), '-oc', str(PurePath(f'{self.hjson}'))]
            log = open(self.log,"w")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                btm = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pbdir(), text=True, creationflags=creationflags)
            else:
                btm = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pbdir(), text=True, start_new_session=True)
            self.pid = btm.pid
            self.save_pid()

class OptimizeMultiQueue:
    def __init__(self):
        self.items = []
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("optimize_multi"):
            pb_config.add_section("optimize_multi")
        # Ensure option exists with default
        if not pb_config.has_option("optimize_multi", "autostart"):
            pb_config.set("optimize_multi", "autostart", "False")
            with open('pbgui.ini', 'w') as f:
                pb_config.write(f)
        self._autostart = eval(pb_config.get("optimize_multi", "autostart", fallback="False"))
        if self._autostart:
            self.run()

    @property
    def autostart(self):
        return self._autostart

    @autostart.setter
    def autostart(self, new_autostart):
        self._autostart = new_autostart
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pb_config.set("optimize_multi", "autostart", str(self._autostart))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)
        if self._autostart:
            self.run()
        else:
            self.stop()

    def add(self, qitem : OptimizeMultiQueueItem):
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
        ed = st.session_state[f'view_opt_queue_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    self.items[row].remove()
        self.items = []

    def running(self):
        for item in self.items:
            if item.is_running():
                return True
        return False

    def downloading(self):
        for item in self.items:
            if item.is_running() and not item.is_optimizing():
                return True
        return False
        
    def load(self):
        dest = Path(f'{PBGDIR}/data/opt_multi_queue')
        p = str(Path(f'{dest}/*.json'))
        items = glob.glob(p)
        self.items = []
        for item in items:
            with open(item, "r", encoding='utf-8') as f:
                config = json.load(f)
                qitem = OptimizeMultiQueueItem()
                qitem.name = config["name"]
                qitem.filename = config["filename"]
                qitem.hjson = config["hjson"]
                qitem.exchange = config["exchange"]
                qitem.log = Path(f'{PBGDIR}/data/opt_multi_queue/{qitem.filename}.log')
                qitem.pidfile = Path(f'{PBGDIR}/data/opt_multi_queue/{qitem.filename}.pid')
                self.add(qitem)

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/OptimizeMulti.py')]
            dest = Path(f'{PBGDIR}/data/logs')
            if not dest.exists():
                dest.mkdir(parents=True)
            logfile = Path(f'{dest}/OptimizeMulti.log')
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
            if any("OptimizeMulti.py" in sub for sub in cmdline):
                return process

    def view(self):
        if not self.items:
            self.load()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'view_opt_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_opt_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if self.items[row].is_running():
                        self.items[row].stop()
                    else:
                        self.items[row].run()
                if "view" in ed["edited_rows"][row]:
                    opt = OptimizeMultiItem(f'{PBGDIR}/data/opt_multi/{self.items[row].name}')
                    opt.load()
                    opt.load_results()
                    st.session_state.opt_multi_results = opt
                    del st.session_state.opt_multi_queue
                    st.rerun()
        d = []
        for id, opt in enumerate(self.items):
            d.append({
                'id': id,
                'run': False,
                'Status': opt.status(),
                'log': False,
                'delete': False,
                'name': opt.name,
                'filename': opt.filename,
                'exchange': opt.exchange,
                'finish': opt.is_finish(),
            })
        column_config = {
            # "id": None,
            "run": st.column_config.CheckboxColumn('Start/Stop', default=False),
            "log": st.column_config.CheckboxColumn(label="View Logfile"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display Queue
        height = 36+(len(d))*35
        if height > 1000: height = 1016
        st.data_editor(data=d, height=height, key=f'view_opt_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','filename','name','finish','running'])
        if f'view_opt_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_opt_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "log" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["log"]:
                        self.items[row].view_log()

class OptimizeMultiResults:
    def __init__(self):
        self.results_path = Path(f'{pbdir()}/results_multi')
        self.analysis_path = Path(f'{pbdir()}/results_multi_analysis')
        self.results = []
        self.initialize()
    
    def initialize(self):
        self.find_results()
    
    def remove(self, file_name):
        Path(file_name).unlink(missing_ok=True)
        analysis = PurePath(file_name).stem[0:19]
        analysis = str(self.analysis_path) + f'/{analysis}*.json'
        analysis = glob.glob(analysis, recursive=False)
        if analysis:
            Path(analysis[0]).unlink(missing_ok=True)

    def find_results(self):
        self.results = []
        if self.results_path.exists():
            p = str(self.results_path) + "/*.txt"
            self.results = glob.glob(p, recursive=False)

    def view_analysis(self, analysis):
        with open(analysis, "r", encoding='utf-8') as f:
            config = json.load(f)
            st.code(json.dumps(config, indent=4))

    def view_results(self):
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        d = []
        for id, opt in enumerate(self.results):
            analysis = PurePath(opt).stem[0:19]
            analysis = str(self.analysis_path) + f'/{analysis}*.json'
            analysis = glob.glob(analysis, recursive=False)
            analysis = analysis[0] if analysis else None
            d.append({
                'id': id,
                'Result': opt,
                'Analysis': analysis,
                'view': False,
                "generate": False,
                'backtest': False,
                'delete' : False,
            })
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Analysis"),
            "generate": st.column_config.CheckboxColumn(label="Generate Analysis"),
            "backtest": st.column_config.CheckboxColumn(label="Backtest"),
            }
        #Display optimizes
        st.data_editor(data=d, height=36+(len(d))*35, key=f'select_optresults_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])
        if f'select_optresults_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_optresults_{ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        if d[row]["Analysis"]:
                            self.view_analysis(d[row]["Analysis"])
                if "generate" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["generate"]:
                        self.generate_analysis(d[row]["Result"])
                        st.session_state.ed_key += 1
                        # st.rerun()
                if "backtest" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["backtest"]:
                        st.session_state.bt_multi = BacktestMultiItem()
                        st.session_state.bt_multi.create_from_multi_optimize(d[row]["Analysis"])
                        if "bt_multi_queue" in st.session_state:
                            del st.session_state.bt_multi_queue
                        if "bt_multi_results" in st.session_state:
                            del st.session_state.bt_multi_results
                        if "bt_multi_edit_symbol" in st.session_state:
                            del st.session_state.bt_multi_edit_symbol
                        st.switch_page(get_navi_paths()["V6_MULTI_BACKTEST"])

    def generate_analysis(self, result_file):
        cmd = [st.session_state.pbvenv, '-u', PurePath(f'{pbdir()}/tools/extract_best_multi_config.py'), str(result_file)]
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NO_WINDOW
            result = subprocess.run(cmd, capture_output=True, cwd=pbdir(), text=True, creationflags=creationflags)
        else:
            result = subprocess.run(cmd, capture_output=True, cwd=pbdir(), text=True, start_new_session=True)
        if "error" in result.stdout:
            error_popup(result.stdout)
        else:
            info_popup(f"Analysis Generated {result.stdout}")

    def remove_selected_results(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_optresults_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    self.remove(self.results[row])
        self.results = []
        self.find_results()
    
    def remove_all_results(self):
        rmtree(self.results_path, ignore_errors=True)
        rmtree(self.analysis_path, ignore_errors=True)
        self.results = []

class OptimizeMultiItem:
    def __init__(self, optimize_file: str = None):
        self.log = None
        self.bounds = MultiBounds()
        self.users = Users()
        self.backtest_results = []
        self.hjson = None
        self.initialize()
        if optimize_file:
            self.hjson = Path(optimize_file)
            if self.hjson.exists():
                self.load()

    @property
    def exchange(self): return self._exchange
    @exchange.setter
    def exchange(self, new_exchange):
        if new_exchange != self._exchange:
            if new_exchange != "bybit":
                self._exchange = 'binance'
            else:
                self._exchange = new_exchange
            self._available_symbols = load_symbols_from_ini(exchange=self.exchange, market_type='swap')

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

    # worst_drawdown_lower_bound
    @property
    def worst_drawdown_lower_bound(self): return self._worst_drawdown_lower_bound
    @worst_drawdown_lower_bound.setter
    def worst_drawdown_lower_bound(self, new_worst_drawdown_lower_bound):
        self._worst_drawdown_lower_bound = new_worst_drawdown_lower_bound

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
        self._n_cpus = multiprocessing.cpu_count()
        self._iters = 10000
        self._worst_drawdown_lower_bound = 0.25
        self._long_enabled = True
        self._short_enabled = False
        self._exchange = 'binance'
        self.symbols = []
        # Load available symbols
        self._available_symbols = load_symbols_from_ini(exchange=self.exchange, market_type='swap')

    def edit(self):
        # Init session_state for keys
        if "edit_opt_multi_exchange" in st.session_state:
            if st.session_state.edit_opt_multi_exchange != self.exchange:
                self.exchange = st.session_state.edit_opt_multi_exchange
        if "edit_opt_multi_name" in st.session_state:
            if st.session_state.edit_opt_multi_name != self.name:
                st.session_state.edit_opt_multi_name = replace_special_chars(st.session_state.edit_opt_multi_name)
                self.name = st.session_state.edit_opt_multi_name
        if "edit_opt_multi_sd" in st.session_state:
            if st.session_state.edit_opt_multi_sd.strftime("%Y-%m-%d") != self.sd:
                self.sd = st.session_state.edit_opt_multi_sd.strftime("%Y-%m-%d")
        if "edit_opt_multi_ed" in st.session_state:
            if st.session_state.edit_opt_multi_ed.strftime("%Y-%m-%d") != self.ed:
                self.ed = st.session_state.edit_opt_multi_ed.strftime("%Y-%m-%d")
        if "edit_opt_multi_sb" in st.session_state:
            if st.session_state.edit_opt_multi_sb != self.sb:
                self.sb = st.session_state.edit_opt_multi_sb
        if  "edit_opt_multi_iters" in st.session_state:
            if st.session_state.edit_opt_multi_iters != self.iters:
                self.iters = st.session_state.edit_opt_multi_iters
        if  "edit_opt_multi_n_cpu" in st.session_state:
            if st.session_state.edit_opt_multi_n_cpu != self.n_cpus:
                self.n_cpus = st.session_state.edit_opt_multi_n_cpu
        if "edit_opt_multi_worst_drawdown_lower_bound" in st.session_state:
            if st.session_state.edit_opt_multi_worst_drawdown_lower_bound != self.worst_drawdown_lower_bound:
                self.worst_drawdown_lower_bound = st.session_state.edit_opt_multi_worst_drawdown_lower_bound
        if "edit_opt_multi_long_enabled" in st.session_state:
            if st.session_state.edit_opt_multi_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.edit_opt_multi_long_enabled
        if "edit_opt_multi_short_enabled" in st.session_state:
            if st.session_state.edit_opt_multi_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.edit_opt_multi_short_enabled
        if "edit_opt_multi_symbols" in st.session_state:
            if st.session_state.edit_opt_multi_symbols != self.symbols:
                self.symbols = st.session_state.edit_opt_multi_symbols
        # Display Editor
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            exchange_index = 0
            if self.exchange == "bybit":
                exchange_index = 1
            st.selectbox('Exchange',['binance', 'bybit'], index = exchange_index, key="edit_opt_multi_exchange")
        with col2:
            if not self.name:
                st.text_input(f":red[Optimize Name]", value=self.name, max_chars=64, help=pbgui_help.task_name, key="edit_opt_multi_name")
            else:
                st.text_input(f"Optimize Name", value=self.name, max_chars=64, key="edit_opt_multi_name")
        with col3:
            st.date_input("START_DATE", datetime.datetime.strptime(self.sd, '%Y-%m-%d'), format="YYYY-MM-DD", key="edit_opt_multi_sd")
        with col4:
            st.date_input("END_DATE", datetime.datetime.strptime(self.ed, '%Y-%m-%d'), format="YYYY-MM-DD", key="edit_opt_multi_ed")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('STARTING_BALANCE',value=self.sb,step=500, key="edit_opt_multi_sb")
        with col2:
            st.number_input('iters',value=self.iters, step=1000, help=pbgui_help.opt_iters, key="edit_opt_multi_iters")
        with col3:
            st.number_input('n_cpus',value=self.n_cpus, min_value=1, max_value=multiprocessing.cpu_count(), step=1, help=None, key="edit_opt_multi_n_cpu")
        with col4:
            st.number_input("worst_drawdown_lower_bound", min_value=0.0, max_value=1.0, value=self.worst_drawdown_lower_bound, step=0.01, format="%.2f", key="edit_opt_multi_worst_drawdown_lower_bound", help=pbgui_help.worst_drawdown_lower_bound)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.checkbox("long_enabled", value=self.long_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_opt_multi_long_enabled")
        with col2:
            st.checkbox("short_enabled", value=self.short_enabled, help=pbgui_help.multi_long_short_enabled, key="edit_opt_multi_short_enabled")
        with col3:
            st.empty()
        with col4:
            st.empty()
        st.multiselect('symbols', self._available_symbols, default=self.symbols, key="edit_opt_multi_symbols")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            # _global_TWE_long_0
            if "edit_opt_multi_global_TWE_long_0" in st.session_state:
                self.bounds.global_TWE_long_0 = st.session_state.edit_opt_multi_global_TWE_long_0
            # _global_TWE_long_1
            if "edit_opt_multi_global_TWE_long_1" in st.session_state:
                self.bounds.global_TWE_long_1 = st.session_state.edit_opt_multi_global_TWE_long_1
            # _global_TWE_short_0
            if "edit_opt_multi_global_TWE_short_0" in st.session_state:
                self.bounds.global_TWE_short_0 = st.session_state.edit_opt_multi_global_TWE_short_0
            # _global_TWE_short_1
            if "edit_opt_multi_global_TWE_short_1" in st.session_state:
                self.bounds.global_TWE_short_1 = st.session_state.edit_opt_multi_global_TWE_short_1
            # _global_loss_allowance_pct_0
            if "edit_opt_multi_global_loss_allowance_pct_0" in st.session_state:
                self.bounds.global_loss_allowance_pct_0 = st.session_state.edit_opt_multi_global_loss_allowance_pct_0
            # _global_loss_allowance_pct_1
            if "edit_opt_multi_global_loss_allowance_pct_1" in st.session_state:
                self.bounds.global_loss_allowance_pct_1 = st.session_state.edit_opt_multi_global_loss_allowance_pct_1
            # _global_stuck_threshold_0
            if "edit_opt_multi_global_stuck_threshold_0" in st.session_state:
                self.bounds.global_stuck_threshold_0 = st.session_state.edit_opt_multi_global_stuck_threshold_0
            # _global_stuck_threshold_1
            if "edit_opt_multi_global_stuck_threshold_1" in st.session_state:
                self.bounds.global_stuck_threshold_1 = st.session_state.edit_opt_multi_global_stuck_threshold_1
            # _global_unstuck_close_pct_0
            if "edit_opt_multi_global_unstuck_close_pct_0" in st.session_state:
                self.bounds.global_unstuck_close_pct_0 = st.session_state.edit_opt_multi_global_unstuck_close_pct_0
            # _global_unstuck_close_pct_1
            if "edit_opt_multi_global_unstuck_close_pct_1" in st.session_state:
                self.bounds.global_unstuck_close_pct_1 = st.session_state.edit_opt_multi_global_unstuck_close_pct_1
            # Edit Global
            st.number_input("global_TWE_long min", min_value=self.bounds.GLOBAL_TWE_LONG_MIN, max_value=self.bounds.global_TWE_long_1, value=float(round(self.bounds.global_TWE_long_0,self.bounds.GLOBAL_TWE_LONG_ROUND)), step=self.bounds.GLOBAL_TWE_LONG_STEP, format=self.bounds.GLOBAL_TWE_LONG_FORMAT, key="edit_opt_multi_global_TWE_long_0")
            st.number_input("global_loss_allowance_pct min", min_value=self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_MIN, max_value=self.bounds.global_loss_allowance_pct_1, value=float(round(self.bounds.global_loss_allowance_pct_0,self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_ROUND)), step=self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_STEP, format=self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_FORMAT, key="edit_opt_multi_global_loss_allowance_pct_0")
            st.number_input("global_unstuck_close_pct min", min_value=self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_MIN, max_value=self.bounds.global_unstuck_close_pct_1, value=float(round(self.bounds.global_unstuck_close_pct_0,self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_ROUND)), step=self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_STEP, format=self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_FORMAT, key="edit_opt_multi_global_unstuck_close_pct_0")
        with col2:
            st.number_input("global_TWE_long max", min_value=self.bounds.global_TWE_long_0, max_value=self.bounds.GLOBAL_TWE_LONG_MAX, value=float(round(self.bounds.global_TWE_long_1,self.bounds.GLOBAL_TWE_LONG_ROUND)), step=self.bounds.GLOBAL_TWE_LONG_STEP, format=self.bounds.GLOBAL_TWE_LONG_FORMAT, key="edit_opt_multi_global_TWE_long_1")
            st.number_input("global_loss_allowance_pct max", min_value=self.bounds.global_loss_allowance_pct_0, max_value=self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_MAX, value=float(round(self.bounds.global_loss_allowance_pct_1,self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_ROUND)), step=self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_STEP, format=self.bounds.GLOBAL_LOSS_ALLOWANCE_PCT_FORMAT, key="edit_opt_multi_global_loss_allowance_pct_1")
            st.number_input("global_unstuck_close_pct max", min_value=self.bounds.global_unstuck_close_pct_0, max_value=self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_MAX, value=float(round(self.bounds.global_unstuck_close_pct_1,self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_ROUND)), step=self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_STEP, format=self.bounds.GLOBAL_UNSTUCK_CLOSE_PCT_FORMAT, key="edit_opt_multi_global_unstuck_close_pct_1")
        with col3:
            st.number_input("global_TWE_short min", min_value=self.bounds.GLOBAL_TWE_SHORT_MIN, max_value=self.bounds.global_TWE_short_1, value=float(round(self.bounds.global_TWE_short_0,self.bounds.GLOBAL_TWE_SHORT_ROUND)), step=self.bounds.GLOBAL_TWE_SHORT_STEP, format=self.bounds.GLOBAL_TWE_SHORT_FORMAT, key="edit_opt_multi_global_TWE_short_0")
            st.number_input("global_stuck_threshold min", min_value=self.bounds.GLOBAL_STUCK_THRESHOLD_MIN, max_value=self.bounds.global_stuck_threshold_1, value=float(round(self.bounds.global_stuck_threshold_0,self.bounds.GLOBAL_STUCK_THRESHOLD_ROUND)), step=self.bounds.GLOBAL_STUCK_THRESHOLD_STEP, format=self.bounds.GLOBAL_STUCK_THRESHOLD_FORMAT, key="edit_opt_multi_global_stuck_threshold_0")
        with col4:
            st.number_input("global_TWE_short max", min_value=self.bounds.global_TWE_short_0, max_value=self.bounds.GLOBAL_TWE_SHORT_MAX, value=float(round(self.bounds.global_TWE_short_1,self.bounds.GLOBAL_TWE_SHORT_ROUND)), step=self.bounds.GLOBAL_TWE_SHORT_STEP, format=self.bounds.GLOBAL_TWE_SHORT_FORMAT, key="edit_opt_multi_global_TWE_short_1")
            st.number_input("global_stuck_threshold max", min_value=self.bounds.global_stuck_threshold_0, max_value=self.bounds.GLOBAL_STUCK_THRESHOLD_MAX, value=float(round(self.bounds.global_stuck_threshold_1,self.bounds.GLOBAL_STUCK_THRESHOLD_ROUND)), step=self.bounds.GLOBAL_STUCK_THRESHOLD_STEP, format=self.bounds.GLOBAL_STUCK_THRESHOLD_FORMAT, key="edit_opt_multi_global_stuck_threshold_1")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            # _long_ddown_factor
            if  "edit_opt_multi_bounds_long_ddown_factor_0" in st.session_state:
                self.bounds.long_ddown_factor_0 = st.session_state.edit_opt_multi_bounds_long_ddown_factor_0
            if  "edit_opt_multi_bounds_long_ddown_factor_1" in st.session_state:
                self.bounds.long_ddown_factor_1 = st.session_state.edit_opt_multi_bounds_long_ddown_factor_1
            # _long_ema_span_0
            if "edit_opt_multi_bounds_long_ema_span_0_0" in st.session_state:
                self.bounds.long_ema_span_0_0 = st.session_state.edit_opt_multi_bounds_long_ema_span_0_0
            if "edit_opt_multi_bounds_long_ema_span_0_1" in st.session_state:
                self.bounds.long_ema_span_0_1 = st.session_state.edit_opt_multi_bounds_long_ema_span_0_1
            # _long_ema_span_1
            if "edit_opt_multi_bounds_long_ema_span_1_0" in st.session_state:
                self.bounds.long_ema_span_1_0 = st.session_state.edit_opt_multi_bounds_long_ema_span_1_0
            if "edit_opt_multi_bounds_long_ema_span_1_1" in st.session_state:
                self.bounds.long_ema_span_1_1 = st.session_state.edit_opt_multi_bounds_long_ema_span_1_1
            # _long_initial_eprice_ema_dist
            if "edit_opt_multi_bounds_long_initial_eprice_ema_dist_0" in st.session_state:
                self.bounds.long_initial_eprice_ema_dist_0 = st.session_state.edit_opt_multi_bounds_long_initial_eprice_ema_dist_0
            if "edit_opt_multi_bounds_long_initial_eprice_ema_dist_1" in st.session_state:
                self.bounds.long_initial_eprice_ema_dist_1 = st.session_state.edit_opt_multi_bounds_long_initial_eprice_ema_dist_1
            # _long_initial_qty_pct
            if "edit_opt_multi_bounds_long_initial_qty_pct_0" in st.session_state:
                self.bounds.long_initial_qty_pct_0 = st.session_state.edit_opt_multi_bounds_long_initial_qty_pct_0
            if "edit_opt_multi_bounds_long_initial_qty_pct_1" in st.session_state:
                self.bounds.long_initial_qty_pct_1 = st.session_state.edit_opt_multi_bounds_long_initial_qty_pct_1
            # _long_markup_range
            if "edit_opt_multi_bounds_long_markup_range_0" in st.session_state:
                self.bounds.long_markup_range_0 = st.session_state.edit_opt_multi_bounds_long_markup_range_0
            if "edit_opt_multi_bounds_long_markup_range_1" in st.session_state:
                self.bounds.long_markup_range_1 = st.session_state.edit_opt_multi_bounds_long_markup_range_1
            # _long_min_markup
            if "edit_opt_multi_bounds_long_min_markup_0" in st.session_state:
                self.bounds.long_min_markup_0 = st.session_state.edit_opt_multi_bounds_long_min_markup_0
            if "edit_opt_multi_bounds_long_min_markup_1" in st.session_state:
                self.bounds.long_min_markup_1 = st.session_state.edit_opt_multi_bounds_long_min_markup_1
            # _long_n_close_orders
            if "edit_opt_multi_bounds_long_n_close_orders_0" in st.session_state:
                self.bounds.long_n_close_orders_0 = st.session_state.edit_opt_multi_bounds_long_n_close_orders_0
            if "edit_opt_multi_bounds_long_n_close_orders_1" in st.session_state:
                self.bounds.long_n_close_orders_1 = st.session_state.edit_opt_multi_bounds_long_n_close_orders_1
            # _long_rentry_pprice_dist
            if "edit_opt_multi_bounds_long_rentry_pprice_dist_0" in st.session_state:
                self.bounds.long_rentry_pprice_dist_0 = st.session_state.edit_opt_multi_bounds_long_rentry_pprice_dist_0
            if "edit_opt_multi_bounds_long_rentry_pprice_dist_1" in st.session_state:
                self.bounds.long_rentry_pprice_dist_1 = st.session_state.edit_opt_multi_bounds_long_rentry_pprice_dist_1
            # _long_rentry_pprice_dist_wallet_exposure_weighting
            if "edit_opt_multi_bounds_long_rentry_pprice_dist_wallet_exposure_weighting_0" in st.session_state:
                self.bounds.long_rentry_pprice_dist_wallet_exposure_weighting_0 = st.session_state.edit_opt_multi_bounds_long_rentry_pprice_dist_wallet_exposure_weighting_0
            if "edit_opt_multi_bounds_long_rentry_pprice_dist_wallet_exposure_weighting_1" in st.session_state:
                self.bounds.long_rentry_pprice_dist_wallet_exposure_weighting_1 = st.session_state.edit_opt_multi_bounds_long_rentry_pprice_dist_wallet_exposure_weighting_1
            # Edit Bounds long min
            st.number_input("long_ddown_factor min", min_value=self.bounds.DDOWN_FACTOR_MIN, max_value=self.bounds.long_ddown_factor_1, value=float(round(self.bounds.long_ddown_factor_0,self.bounds.DDOWN_FACTOR_ROUND)), step=self.bounds.DDOWN_FACTOR_STEP, format=self.bounds.DDOWN_FACTOR_FORMAT, key="edit_opt_multi_bounds_long_ddown_factor_0", help=pbgui_help.ddown_factor)
            st.number_input("long_ema_span_0 min", min_value=self.bounds.EMA_SPAN_0_MIN, max_value=self.bounds.long_ema_span_0_1, value=float(round(self.bounds.long_ema_span_0_0,self.bounds.EMA_SPAN_0_ROUND)), step=self.bounds.EMA_SPAN_0_STEP, format=self.bounds.EMA_SPAN_0_FORMAT, key="edit_opt_multi_bounds_long_ema_span_0_0", help=pbgui_help.ema_span)
            st.number_input("long_ema_span_1 min", min_value=self.bounds.EMA_SPAN_1_MIN, max_value=self.bounds.long_ema_span_1_1, value=float(round(self.bounds.long_ema_span_1_0,self.bounds.EMA_SPAN_1_ROUND)), step=self.bounds.EMA_SPAN_1_STEP, format=self.bounds.EMA_SPAN_1_FORMAT, key="edit_opt_multi_bounds_long_ema_span_1_0", help=pbgui_help.ema_span)
            st.number_input("long_initial_eprice_ema_dist min", min_value=self.bounds.INITIAL_EPRICE_EMA_DIST_MIN, max_value=self.bounds.long_initial_eprice_ema_dist_1, value=float(round(self.bounds.long_initial_eprice_ema_dist_0,self.bounds.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.bounds.INITIAL_EPRICE_EMA_DIST_STEP, format=self.bounds.INITIAL_EPRICE_EMA_DIST_FORMAT, key="edit_opt_multi_bounds_long_initial_eprice_ema_dist_0", help=pbgui_help.initial_eprice_ema_dist)
            st.number_input("long_initial_qty_pct min", min_value=self.bounds.INITIAL_QTY_PCT_MIN, max_value=self.bounds.long_initial_qty_pct_1, value=float(round(self.bounds.long_initial_qty_pct_0,self.bounds.INITIAL_QTY_PCT_ROUND)), step=self.bounds.INITIAL_QTY_PCT_STEP, format=self.bounds.INITIAL_QTY_PCT_FORMAT, key="edit_opt_multi_bounds_long_initial_qty_pct_0", help=pbgui_help.initial_qty_pct)
            st.number_input("long_markup_range min", min_value=self.bounds.MARKUP_RANGE_MIN, max_value=self.bounds.long_markup_range_1, value=float(round(self.bounds.long_markup_range_0,self.bounds.MARKUP_RANGE_ROUND)), step=self.bounds.MARKUP_RANGE_STEP, format=self.bounds.MARKUP_RANGE_FORMAT, key="edit_opt_multi_bounds_long_markup_range_0", help=pbgui_help.markup_range)
            st.number_input("long_min_markup min", min_value=self.bounds.MIN_MARKUP_MIN, max_value=self.bounds.long_min_markup_1, value=float(round(self.bounds.long_min_markup_0,self.bounds.MIN_MARKUP_ROUND)), step=self.bounds.MIN_MARKUP_STEP, format=self.bounds.MIN_MARKUP_FORMAT, key="edit_opt_multi_bounds_long_min_markup_0", help=pbgui_help.min_markup)
            st.number_input("long_n_close_orders min", min_value=self.bounds.N_CLOSE_ORDERS_MIN, max_value=self.bounds.long_n_close_orders_1, value=self.bounds.long_n_close_orders_0, step=self.bounds.N_CLOSE_ORDERS_STEP, format=self.bounds.N_CLOSE_ORDERS_FORMAT, key="edit_opt_multi_bounds_long_n_close_orders_0", help=pbgui_help.n_close_orders)
            st.number_input("long_rentry_pprice_dist min", min_value=self.bounds.RENTRY_PPRICE_DIST_MIN, max_value=self.bounds.long_rentry_pprice_dist_1, value=float(round(self.bounds.long_rentry_pprice_dist_0,self.bounds.RENTRY_PPRICE_DIST_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_STEP, format=self.bounds.RENTRY_PPRICE_DIST_FORMAT, key="edit_opt_multi_bounds_long_rentry_pprice_dist_0", help=pbgui_help.rentry_pprice_dist)
            st.number_input("long_rentry_pprice_dist_wallet_exposure_weighting min", min_value=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN, max_value=self.bounds.long_rentry_pprice_dist_wallet_exposure_weighting_1, value=float(round(self.bounds.long_rentry_pprice_dist_wallet_exposure_weighting_0,self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="edit_opt_multi_bounds_long_rentry_pprice_dist_wallet_exposure_weighting_0", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
        with col2:
            # Edit Bounds long max
            st.number_input("long_ddown_factor max", min_value=self.bounds.long_ddown_factor_0, max_value=self.bounds.DDOWN_FACTOR_MAX, value=float(round(self.bounds.long_ddown_factor_1,self.bounds.DDOWN_FACTOR_ROUND)), step=self.bounds.DDOWN_FACTOR_STEP, format=self.bounds.DDOWN_FACTOR_FORMAT, key="edit_opt_multi_bounds_long_ddown_factor_1", help=pbgui_help.ddown_factor)
            st.number_input("long_ema_span_0 max", min_value=self.bounds.long_ema_span_0_0, max_value=self.bounds.EMA_SPAN_0_MAX, value=float(round(self.bounds.long_ema_span_0_1,self.bounds.EMA_SPAN_0_ROUND)), step=self.bounds.EMA_SPAN_0_STEP, format=self.bounds.EMA_SPAN_0_FORMAT, key="edit_opt_multi_bounds_long_ema_span_0_1", help=pbgui_help.ema_span)
            st.number_input("long_ema_span_1 max", min_value=self.bounds.long_ema_span_1_0, max_value=self.bounds.EMA_SPAN_1_MAX, value=float(round(self.bounds.long_ema_span_1_1,self.bounds.EMA_SPAN_1_ROUND)), step=self.bounds.EMA_SPAN_1_STEP, format=self.bounds.EMA_SPAN_1_FORMAT, key="edit_opt_multi_bounds_long_ema_span_1_1", help=pbgui_help.ema_span)
            st.number_input("long_initial_eprice_ema_dist max", min_value=self.bounds.long_initial_eprice_ema_dist_0, max_value=self.bounds.INITIAL_EPRICE_EMA_DIST_MAX, value=float(round(self.bounds.long_initial_eprice_ema_dist_1,self.bounds.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.bounds.INITIAL_EPRICE_EMA_DIST_STEP, format=self.bounds.INITIAL_EPRICE_EMA_DIST_FORMAT, key="edit_opt_multi_bounds_long_initial_eprice_ema_dist_1", help=pbgui_help.initial_eprice_ema_dist)
            st.number_input("long_initial_qty_pct max", min_value=self.bounds.long_initial_qty_pct_0, max_value=self.bounds.INITIAL_QTY_PCT_MAX, value=float(round(self.bounds.long_initial_qty_pct_1,self.bounds.INITIAL_QTY_PCT_ROUND)), step=self.bounds.INITIAL_QTY_PCT_STEP, format=self.bounds.INITIAL_QTY_PCT_FORMAT, key="edit_opt_multi_bounds_long_initial_qty_pct_1", help=pbgui_help.initial_qty_pct)
            st.number_input("long_markup_range max", min_value=self.bounds.long_markup_range_0, max_value=self.bounds.MARKUP_RANGE_MAX, value=float(round(self.bounds.long_markup_range_1,self.bounds.MARKUP_RANGE_ROUND)), step=self.bounds.MARKUP_RANGE_STEP, format=self.bounds.MARKUP_RANGE_FORMAT, key="edit_opt_multi_bounds_long_markup_range_1", help=pbgui_help.markup_range)
            st.number_input("long_min_markup max", min_value=self.bounds.long_min_markup_0, max_value=self.bounds.MIN_MARKUP_MAX, value=float(round(self.bounds.long_min_markup_1,self.bounds.MIN_MARKUP_ROUND)), step=self.bounds.MIN_MARKUP_STEP, format=self.bounds.MIN_MARKUP_FORMAT, key="edit_opt_multi_bounds_long_min_markup_1", help=pbgui_help.min_markup)
            st.number_input("long_n_close_orders max", min_value=self.bounds.long_n_close_orders_0, max_value=self.bounds.N_CLOSE_ORDERS_MAX, value=self.bounds.long_n_close_orders_1, step=self.bounds.N_CLOSE_ORDERS_STEP, format=self.bounds.N_CLOSE_ORDERS_FORMAT, key="edit_opt_multi_bounds_long_n_close_orders_1", help=pbgui_help.n_close_orders)
            st.number_input("long_rentry_pprice_dist max", min_value=self.bounds.long_rentry_pprice_dist_0, max_value=self.bounds.RENTRY_PPRICE_DIST_MAX, value=float(round(self.bounds.long_rentry_pprice_dist_1,self.bounds.RENTRY_PPRICE_DIST_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_STEP, format=self.bounds.RENTRY_PPRICE_DIST_FORMAT, key="edit_opt_multi_bounds_long_rentry_pprice_dist_1", help=pbgui_help.rentry_pprice_dist)
            st.number_input("long_rentry_pprice_dist_wallet_exposure_weighting max", min_value=self.bounds.long_rentry_pprice_dist_wallet_exposure_weighting_0, max_value=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX, value=float(round(self.bounds.long_rentry_pprice_dist_wallet_exposure_weighting_1,self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="edit_opt_multi_bounds_long_rentry_pprice_dist_wallet_exposure_weighting_1", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
        with col3:
            # _short_ddown_factor
            if  "edit_opt_multi_bounds_short_ddown_factor_0" in st.session_state:
                self.bounds.short_ddown_factor_0 = st.session_state.edit_opt_multi_bounds_short_ddown_factor_0
            if  "edit_opt_multi_bounds_short_ddown_factor_1" in st.session_state:
                self.bounds.short_ddown_factor_1 = st.session_state.edit_opt_multi_bounds_short_ddown_factor_1
            # _short_ema_span_0
            if "edit_opt_multi_bounds_short_ema_span_0_0" in st.session_state:
                self.bounds.short_ema_span_0_0 = st.session_state.edit_opt_multi_bounds_short_ema_span_0_0
            if "edit_opt_multi_bounds_short_ema_span_0_1" in st.session_state:
                self.bounds.short_ema_span_0_1 = st.session_state.edit_opt_multi_bounds_short_ema_span_0_1
            # _short_ema_span_1
            if "edit_opt_multi_bounds_short_ema_span_1_0" in st.session_state:
                self.bounds.short_ema_span_1_0 = st.session_state.edit_opt_multi_bounds_short_ema_span_1_0
            if "edit_opt_multi_bounds_short_ema_span_1_1" in st.session_state:
                self.bounds.short_ema_span_1_1 = st.session_state.edit_opt_multi_bounds_short_ema_span_1_1
            # _short_initial_eprice_ema_dist
            if "edit_opt_multi_bounds_short_initial_eprice_ema_dist_0" in st.session_state:
                self.bounds.short_initial_eprice_ema_dist_0 = st.session_state.edit_opt_multi_bounds_short_initial_eprice_ema_dist_0
            if "edit_opt_multi_bounds_short_initial_eprice_ema_dist_1" in st.session_state:
                self.bounds.short_initial_eprice_ema_dist_1 = st.session_state.edit_opt_multi_bounds_short_initial_eprice_ema_dist_1
            # _short_initial_qty_pct
            if "edit_opt_multi_bounds_short_initial_qty_pct_0" in st.session_state:
                self.bounds.short_initial_qty_pct_0 = st.session_state.edit_opt_multi_bounds_short_initial_qty_pct_0
            if "edit_opt_multi_bounds_short_initial_qty_pct_1" in st.session_state:
                self.bounds.short_initial_qty_pct_1 = st.session_state.edit_opt_multi_bounds_short_initial_qty_pct_1
            # _short_markup_range
            if "edit_opt_multi_bounds_short_markup_range_0" in st.session_state:
                self.bounds.short_markup_range_0 = st.session_state.edit_opt_multi_bounds_short_markup_range_0
            if "edit_opt_multi_bounds_short_markup_range_1" in st.session_state:
                self.bounds.short_markup_range_1 = st.session_state.edit_opt_multi_bounds_short_markup_range_1
            # _short_min_markup
            if "edit_opt_multi_bounds_short_min_markup_0" in st.session_state:
                self.bounds.short_min_markup_0 = st.session_state.edit_opt_multi_bounds_short_min_markup_0
            if "edit_opt_multi_bounds_short_min_markup_1" in st.session_state:
                self.bounds.short_min_markup_1 = st.session_state.edit_opt_multi_bounds_short_min_markup_1
            # _short_n_close_orders
            if "edit_opt_multi_bounds_short_n_close_orders_0" in st.session_state:
                self.bounds.short_n_close_orders_0 = st.session_state.edit_opt_multi_bounds_short_n_close_orders_0
            if "edit_opt_multi_bounds_short_n_close_orders_1" in st.session_state:
                self.bounds.short_n_close_orders_1 = st.session_state.edit_opt_multi_bounds_short_n_close_orders_1
            # _short_rentry_pprice_dist
            if "edit_opt_multi_bounds_short_rentry_pprice_dist_0" in st.session_state:
                self.bounds.short_rentry_pprice_dist_0 = st.session_state.edit_opt_multi_bounds_short_rentry_pprice_dist_0
            if "edit_opt_multi_bounds_short_rentry_pprice_dist_1" in st.session_state:
                self.bounds.short_rentry_pprice_dist_1 = st.session_state.edit_opt_multi_bounds_short_rentry_pprice_dist_1
            # _short_rentry_pprice_dist_wallet_exposure_weighting
            if "edit_opt_multi_bounds_short_rentry_pprice_dist_wallet_exposure_weighting_0" in st.session_state:
                self.bounds.short_rentry_pprice_dist_wallet_exposure_weighting_0 = st.session_state.edit_opt_multi_bounds_short_rentry_pprice_dist_wallet_exposure_weighting_0
            if "edit_opt_multi_bounds_short_rentry_pprice_dist_wallet_exposure_weighting_1" in st.session_state:
                self.bounds.short_rentry_pprice_dist_wallet_exposure_weighting_1 = st.session_state.edit_opt_multi_bounds_short_rentry_pprice_dist_wallet_exposure_weighting_1
            # Edit Bounds short min
            st.number_input("short_ddown_factor min", min_value=self.bounds.DDOWN_FACTOR_MIN, max_value=self.bounds.short_ddown_factor_1, value=float(round(self.bounds.short_ddown_factor_0,self.bounds.DDOWN_FACTOR_ROUND)), step=self.bounds.DDOWN_FACTOR_STEP, format=self.bounds.DDOWN_FACTOR_FORMAT, key="edit_opt_multi_bounds_short_ddown_factor_0", help=pbgui_help.ddown_factor)
            st.number_input("short_ema_span_0 min", min_value=self.bounds.EMA_SPAN_0_MIN, max_value=self.bounds.short_ema_span_0_1, value=float(round(self.bounds.short_ema_span_0_0,self.bounds.EMA_SPAN_0_ROUND)), step=self.bounds.EMA_SPAN_0_STEP, format=self.bounds.EMA_SPAN_0_FORMAT, key="edit_opt_multi_bounds_short_ema_span_0_0", help=pbgui_help.ema_span)
            st.number_input("short_ema_span_1 min", min_value=self.bounds.EMA_SPAN_1_MIN, max_value=self.bounds.short_ema_span_1_1, value=float(round(self.bounds.short_ema_span_1_0,self.bounds.EMA_SPAN_1_ROUND)), step=self.bounds.EMA_SPAN_1_STEP, format=self.bounds.EMA_SPAN_1_FORMAT, key="edit_opt_multi_bounds_short_ema_span_1_0", help=pbgui_help.ema_span)
            st.number_input("short_initial_eprice_ema_dist min", min_value=self.bounds.INITIAL_EPRICE_EMA_DIST_MIN, max_value=self.bounds.short_initial_eprice_ema_dist_1, value=float(round(self.bounds.short_initial_eprice_ema_dist_0,self.bounds.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.bounds.INITIAL_EPRICE_EMA_DIST_STEP, format=self.bounds.INITIAL_EPRICE_EMA_DIST_FORMAT, key="edit_opt_multi_bounds_short_initial_eprice_ema_dist_0", help=pbgui_help.initial_eprice_ema_dist)
            st.number_input("short_initial_qty_pct min", min_value=self.bounds.INITIAL_QTY_PCT_MIN, max_value=self.bounds.short_initial_qty_pct_1, value=float(round(self.bounds.short_initial_qty_pct_0,self.bounds.INITIAL_QTY_PCT_ROUND)), step=self.bounds.INITIAL_QTY_PCT_STEP, format=self.bounds.INITIAL_QTY_PCT_FORMAT, key="edit_opt_multi_bounds_short_initial_qty_pct_0", help=pbgui_help.initial_qty_pct)
            st.number_input("short_markup_range min", min_value=self.bounds.MARKUP_RANGE_MIN, max_value=self.bounds.short_markup_range_1, value=float(round(self.bounds.short_markup_range_0,self.bounds.MARKUP_RANGE_ROUND)), step=self.bounds.MARKUP_RANGE_STEP, format=self.bounds.MARKUP_RANGE_FORMAT, key="edit_opt_multi_bounds_short_markup_range_0", help=pbgui_help.markup_range)
            st.number_input("short_min_markup min", min_value=self.bounds.MIN_MARKUP_MIN, max_value=self.bounds.short_min_markup_1, value=float(round(self.bounds.short_min_markup_0,self.bounds.MIN_MARKUP_ROUND)), step=self.bounds.MIN_MARKUP_STEP, format=self.bounds.MIN_MARKUP_FORMAT, key="edit_opt_multi_bounds_short_min_markup_0", help=pbgui_help.min_markup)
            st.number_input("short_n_close_orders min", min_value=self.bounds.N_CLOSE_ORDERS_MIN, max_value=self.bounds.short_n_close_orders_1, value=self.bounds.short_n_close_orders_0, step=self.bounds.N_CLOSE_ORDERS_STEP, format=self.bounds.N_CLOSE_ORDERS_FORMAT, key="edit_opt_multi_bounds_short_n_close_orders_0", help=pbgui_help.n_close_orders)
            st.number_input("short_rentry_pprice_dist min", min_value=self.bounds.RENTRY_PPRICE_DIST_MIN, max_value=self.bounds.short_rentry_pprice_dist_1, value=float(round(self.bounds.short_rentry_pprice_dist_0,self.bounds.RENTRY_PPRICE_DIST_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_STEP, format=self.bounds.RENTRY_PPRICE_DIST_FORMAT, key="edit_opt_multi_bounds_short_rentry_pprice_dist_0", help=pbgui_help.rentry_pprice_dist)
            st.number_input("short_rentry_pprice_dist_wallet_exposure_weighting min", min_value=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MIN, max_value=self.bounds.short_rentry_pprice_dist_wallet_exposure_weighting_1, value=float(round(self.bounds.short_rentry_pprice_dist_wallet_exposure_weighting_0,self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="edit_opt_multi_bounds_short_rentry_pprice_dist_wallet_exposure_weighting_0", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)
        with col4:
            # Edit Bounds short max
            st.number_input("short_ddown_factor max", min_value=self.bounds.short_ddown_factor_0, max_value=self.bounds.DDOWN_FACTOR_MAX, value=float(round(self.bounds.short_ddown_factor_1,self.bounds.DDOWN_FACTOR_ROUND)), step=self.bounds.DDOWN_FACTOR_STEP, format=self.bounds.DDOWN_FACTOR_FORMAT, key="edit_opt_multi_bounds_short_ddown_factor_1", help=pbgui_help.ddown_factor)
            st.number_input("short_ema_span_0 max", min_value=self.bounds.short_ema_span_0_0, max_value=self.bounds.EMA_SPAN_0_MAX, value=float(round(self.bounds.short_ema_span_0_1,self.bounds.EMA_SPAN_0_ROUND)), step=self.bounds.EMA_SPAN_0_STEP, format=self.bounds.EMA_SPAN_0_FORMAT, key="edit_opt_multi_bounds_short_ema_span_0_1", help=pbgui_help.ema_span)
            st.number_input("short_ema_span_1 max", min_value=self.bounds.short_ema_span_1_0, max_value=self.bounds.EMA_SPAN_1_MAX, value=float(round(self.bounds.short_ema_span_1_1,self.bounds.EMA_SPAN_1_ROUND)), step=self.bounds.EMA_SPAN_1_STEP, format=self.bounds.EMA_SPAN_1_FORMAT, key="edit_opt_multi_bounds_short_ema_span_1_1", help=pbgui_help.ema_span)
            st.number_input("short_initial_eprice_ema_dist max", min_value=self.bounds.short_initial_eprice_ema_dist_0, max_value=self.bounds.INITIAL_EPRICE_EMA_DIST_MAX, value=float(round(self.bounds.short_initial_eprice_ema_dist_1,self.bounds.INITIAL_EPRICE_EMA_DIST_ROUND)), step=self.bounds.INITIAL_EPRICE_EMA_DIST_STEP, format=self.bounds.INITIAL_EPRICE_EMA_DIST_FORMAT, key="edit_opt_multi_bounds_short_initial_eprice_ema_dist_1", help=pbgui_help.initial_eprice_ema_dist)
            st.number_input("short_initial_qty_pct max", min_value=self.bounds.short_initial_qty_pct_0, max_value=self.bounds.INITIAL_QTY_PCT_MAX, value=float(round(self.bounds.short_initial_qty_pct_1,self.bounds.INITIAL_QTY_PCT_ROUND)), step=self.bounds.INITIAL_QTY_PCT_STEP, format=self.bounds.INITIAL_QTY_PCT_FORMAT, key="edit_opt_multi_bounds_short_initial_qty_pct_1", help=pbgui_help.initial_qty_pct)
            st.number_input("short_markup_range max", min_value=self.bounds.short_markup_range_0, max_value=self.bounds.MARKUP_RANGE_MAX, value=float(round(self.bounds.short_markup_range_1,self.bounds.MARKUP_RANGE_ROUND)), step=self.bounds.MARKUP_RANGE_STEP, format=self.bounds.MARKUP_RANGE_FORMAT, key="edit_opt_multi_bounds_short_markup_range_1", help=pbgui_help.markup_range)
            st.number_input("short_min_markup max", min_value=self.bounds.short_min_markup_0, max_value=self.bounds.MIN_MARKUP_MAX, value=float(round(self.bounds.short_min_markup_1,self.bounds.MIN_MARKUP_ROUND)), step=self.bounds.MIN_MARKUP_STEP, format=self.bounds.MIN_MARKUP_FORMAT, key="edit_opt_multi_bounds_short_min_markup_1", help=pbgui_help.min_markup)
            st.number_input("short_n_close_orders max", min_value=self.bounds.short_n_close_orders_0, max_value=self.bounds.N_CLOSE_ORDERS_MAX, value=self.bounds.short_n_close_orders_1, step=self.bounds.N_CLOSE_ORDERS_STEP, format=self.bounds.N_CLOSE_ORDERS_FORMAT, key="edit_opt_multi_bounds_short_n_close_orders_1", help=pbgui_help.n_close_orders)
            st.number_input("short_rentry_pprice_dist max", min_value=self.bounds.short_rentry_pprice_dist_0, max_value=self.bounds.RENTRY_PPRICE_DIST_MAX, value=float(round(self.bounds.short_rentry_pprice_dist_1,self.bounds.RENTRY_PPRICE_DIST_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_STEP, format=self.bounds.RENTRY_PPRICE_DIST_FORMAT, key="edit_opt_multi_bounds_short_rentry_pprice_dist_1", help=pbgui_help.rentry_pprice_dist)
            st.number_input("short_rentry_pprice_dist_wallet_exposure_weighting max", min_value=self.bounds.short_rentry_pprice_dist_wallet_exposure_weighting_0, max_value=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_MAX, value=float(round(self.bounds.short_rentry_pprice_dist_wallet_exposure_weighting_1,self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_ROUND)), step=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_STEP, format=self.bounds.RENTRY_PPRICE_DIST_WALLET_EXPOSURE_WEIGHTING_FORMAT, key="edit_opt_multi_bounds_short_rentry_pprice_dist_wallet_exposure_weighting_1", help=pbgui_help.rentry_pprice_dist_wallet_exposure_weighting)

    def load(self):
        self.name = PurePath(self.hjson).stem
        if self.hjson.exists():
            try:
                with open(self.hjson, "r", encoding='utf-8') as f:
                    config = f.read()
                opt_config = hjson.loads(config)
                if "exchange" in opt_config:
                    self.exchange = opt_config["exchange"]
                if "start_date" in opt_config:
                    self.sd = opt_config["start_date"]
                if "end_date" in opt_config:
                    self.ed = opt_config["end_date"]
                if "symbols" in opt_config:
                    self.symbols = opt_config["symbols"]
                # if "base_dir" in opt_config:
                #     self.base_dir = opt_config["base_dir"]
                if "n_cpus" in opt_config:
                    self.n_cpus = opt_config["n_cpus"]
                if "iters" in opt_config:
                    self.iters = opt_config["iters"]
                if "starting_balance" in opt_config:
                    self.sb = opt_config["starting_balance"]
                # if "market_type" in opt_config:
                #     self.market_type = opt_config["market_type"]
                if "worst_drawdown_lower_bound" in opt_config:
                    self.worst_drawdown_lower_bound = opt_config["worst_drawdown_lower_bound"]
                if "long_enabled" in opt_config:
                    self._long_enabled = opt_config["long_enabled"]
                if "short_enabled" in opt_config:
                    self._short_enabled = opt_config["short_enabled"]
                if "symbols" in opt_config:
                    symbols = opt_config["symbols"]
                if "bounds" in opt_config:
                    self.bounds.config = opt_config["bounds"] 
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()
    
    def save(self):
        self.path = Path(f'{PBGDIR}/data/opt_multi')
        base_dir = f'backtests/pbgui_opt_multi'
        if not self.path.exists():
            self.path.mkdir(parents=True)
        self.hjson = Path(f'{self.path}/{self.name}.hjson')
        with open(self.hjson, "w", encoding='utf-8') as f:
            f.write(hjson.dumps({
                "exchange": self.exchange,
                "start_date": self.sd,
                "end_date": self.ed,
                "symbols": self.symbols,
                "base_dir": base_dir,
                "n_cpus": self.n_cpus,
                "iters": self.iters,
                "starting_balance": self.sb,
                "market_type": "futures",
                "worst_drawdown_lower_bound": self.worst_drawdown_lower_bound,
                "long_enabled": self.long_enabled,
                "short_enabled": self.short_enabled,
                "bounds": self.bounds.config
            }, indent=4))

    def save_queue(self):
        dest = Path(f'{PBGDIR}/data/opt_multi_queue')
        unique_filename = str(uuid.uuid4())
        file = Path(f'{dest}/{unique_filename}.json') 
        bt_dict = {
            "name": self.name,
            "filename": unique_filename,
            "hjson": str(self.hjson),
            "exchange": self.exchange,
        }
        dest.mkdir(parents=True, exist_ok=True)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(bt_dict, f, indent=4)

    def remove(self):
        self.hjson.unlink(missing_ok=True)

class OptimizesMulti:
    def __init__(self):
        self.optimizes = []

    def view_optimizes(self):
        # Init
        if not self.optimizes:
            self.find_optimizes()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_optimize_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_optimize_{ed_key}']
            for row in ed["edited_rows"]:
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.opt_multi = self.optimizes[row]
                    st.rerun()
                if 'delete' in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]['delete']:
                        self.optimizes[row].remove()
                        self.optimizes.pop(row)
                        st.rerun()
        d = []
        for id, opt in enumerate(self.optimizes):
            d.append({
                'id': id,
                'edit': False,
                'Name': opt.name,
                'delete' : False,
            })
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            }
        #Display optimizes
        st.data_editor(data=d, height=36+(len(d))*35, key=f'select_optimize_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def find_optimizes(self):
        p = str(Path(f'{PBGDIR}/data/opt_multi/*.hjson'))
        found_opt = glob.glob(p, recursive=False)
        if found_opt:
            for p in found_opt:
                opt = OptimizeMultiItem(p)
                self.optimizes.append(opt)
    
def main():
    # Disable Streamlit Warnings when running directly
    logging.getLogger("streamlit.runtime.state.session_state_proxy").disabled=True
    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").disabled=True
    opt = OptimizeMultiQueue()
    while True:
        opt.load()
        for item in opt.items:
            while opt.running():
                time.sleep(5)
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if not eval(pb_config.get("optimize_multi", "autostart", fallback="False")):
                return
            if item.status() == "not started":
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Optimizing {item.filename} started')
                item.run()
                time.sleep(1)
        time.sleep(60)

if __name__ == '__main__':
    main()