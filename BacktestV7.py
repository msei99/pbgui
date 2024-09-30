import streamlit as st
import plotly.graph_objects as go
import pbgui_help
import json
import psutil
import sys
import platform
import subprocess
import glob
import configparser
import time
import multiprocessing
import pandas as pd
from pbgui_func import PBGDIR, pb7dir, pb7venv, validateJSON, config_pretty_str, load_symbols_from_ini, error_popup
import uuid
from Base import Base
from Exchange import Exchange
from Config import Config, ConfigV7
from pathlib import Path, PurePath
from shutil import rmtree
from RunV7 import V7Instance
import datetime
import logging

class BacktestV7QueueItem():
    def __init__(self):
        self.name = None
        self.filename = None
        self.json = None
        self.exchange = None
        self.log = None
        self.pid = None
        self.pidfile = None

    def remove(self):
        self.stop()
        file = Path(f'{PBGDIR}/data/bt_v7_queue/{self.filename}.json')
        file.unlink(missing_ok=True)
        self.log.unlink(missing_ok=True)
        self.pidfile.unlink(missing_ok=True)

    def load_log(self):
        if self.log:
            if self.log.exists():
                with open(self.log, 'r', encoding='utf-8') as f:
                    return f.read()

    @st.fragment
    def view_log(self):
        logfile = self.load_log()
        st.code(logfile)
        if st.button(":material/refresh:", key=f'refresh_view_log_{self.name}'):
            st.rerun()

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
            if self.pid and psutil.pid_exists(self.pid) and any(sub.lower().endswith("backtest.py") for sub in psutil.Process(self.pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            pass
        return False

    def is_finish(self):
        log = self.load_log()
        if log:
            if "Plotting fills" in log:
                return True
            else:
                return False
        else:
            return False

    def is_error(self):
        log = self.load_log()
        if log:
            if "Plotting fills" in log:
                return False
            else:
                return True
        else:
            return False

    def is_backtesting(self):
        if self.is_running():
            log = self.load_log()
            if log:
                if "Plotting fills" in log:
                    return False
                elif "Starting backtest..." in log:
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
            cmd = [pb7venv(), '-u', PurePath(f'{pb7dir()}/src/backtest.py'), str(PurePath(f'{self.json}'))]
            log = open(self.log,"w")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                btm = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pb7dir(), text=True, creationflags=creationflags)
            else:
                btm = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pb7dir(), text=True, start_new_session=True)
            self.pid = btm.pid
            self.save_pid()

class BacktestV7Queue:
    def __init__(self):
        self.items = []
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("backtest_v7"):
            pb_config.add_section("backtest_v7")
            pb_config.set("backtest_v7", "autostart", "False")
            pb_config.set("backtest_v7", "cpu", "1")
            with open('pbgui.ini', 'w') as f:
                pb_config.write(f)
        self._autostart = eval(pb_config.get("backtest_v7", "autostart"))
        self._cpu = int(pb_config.get("backtest_v7", "cpu"))
        if self._autostart:
            self.run()

    @property
    def cpu(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self._cpu = int(pb_config.get("backtest_v7", "cpu"))
        if self._cpu > multiprocessing.cpu_count():
            self._cpu = multiprocessing.cpu_count()
        return self._cpu

    @cpu.setter
    def cpu(self, new_cpu):
        self._cpu = new_cpu
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pb_config.set("backtest_v7", "cpu", str(self._cpu))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    @property
    def autostart(self):
        return self._autostart

    @autostart.setter
    def autostart(self, new_autostart):
        self._autostart = new_autostart
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pb_config.set("backtest_v7", "autostart", str(self._autostart))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)
        if self._autostart:
            self.run()
        else:
            self.stop()

    def add(self, qitem : BacktestV7QueueItem):
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
        ed = st.session_state[f'view_bt_v7_queue_{ed_key}']
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
        dest = Path(f'{PBGDIR}/data/bt_v7_queue')
        p = str(Path(f'{dest}/*.json'))
        items = glob.glob(p)
        self.items = []
        for item in items:
            with open(item, "r", encoding='utf-8') as f:
                config = json.load(f)
                qitem = BacktestV7QueueItem()
                qitem.name = config["name"]
                qitem.filename = config["filename"]
                qitem.json = config["json"]
                qitem.exchange = config["exchange"]
                qitem.log = Path(f'{PBGDIR}/data/bt_v7_queue/{qitem.filename}.log')
                qitem.pidfile = Path(f'{PBGDIR}/data/bt_v7_queue/{qitem.filename}.pid')
                self.add(qitem)

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/BacktestV7.py')]
            dest = Path(f'{PBGDIR}/data/logs')
            if not dest.exists():
                dest.mkdir(parents=True)
            logfile = Path(f'{dest}/BacktestV7.log')
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
            if any("BacktestV7.py" in sub for sub in cmdline):
                return process

    def view(self):
        if not self.items:
            self.load()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'view_bt_v7_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_bt_v7_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if self.items[row].is_running():
                        self.items[row].stop()
                    else:
                        self.items[row].run()
                if "view" in ed["edited_rows"][row]:
                    bt = BacktestV7Item(f'{PBGDIR}/data/bt_v7/{self.items[row].name}/backtest.json')
                    # bt.load()
                    bt.load_results()
                    st.session_state.bt_v7_results = bt
                    del st.session_state.bt_v7_queue
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
        st.data_editor(data=d, height=height, use_container_width=True, key=f'view_bt_v7_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','filename','name','finish','running'])
        if f'view_bt_v7_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_bt_v7_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "log" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["log"]:
                        self.items[row].view_log()

class BacktestV7Item:
    def __init__(self, backtest_path: str = None):
        self.path = backtest_path
        # self.json = None
        self.config = ConfigV7()
        self.log = None
        self.backtest_results = []
        if backtest_path:
            self.config.config_file = backtest_path
            self.config.load_config()
            self.name = self.config.backtest.base_dir.split('/')[-1]
            self._available_symbols = load_symbols_from_ini(exchange=self.config.backtest.exchange, market_type='swap')
        else:
            self.initialize()

    def initialize(self):
        self.name = ""
        self.config.backtest.start_date = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.config.backtest.end_date = datetime.date.today().strftime("%Y-%m-%d")
        self.config.optimize.n_cpus = multiprocessing.cpu_count()
        self._available_symbols = load_symbols_from_ini(exchange=self.config.backtest.exchange, market_type='swap')

    def edit(self):
        # Init session_state for keys
        if "edit_bt_v7_exchange" in st.session_state:
            if st.session_state.edit_bt_v7_exchange != self.config.backtest.exchange:
                self.config.backtest.exchange = st.session_state.edit_bt_v7_exchange
                self._available_symbols = load_symbols_from_ini(exchange=self.config.backtest.exchange, market_type='swap')
        if "edit_bt_v7_name" in st.session_state:
            if st.session_state.edit_bt_v7_name != self.name:
                self.name = st.session_state.edit_bt_v7_name
                self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
        if "edit_bt_v7_sd" in st.session_state:
            if st.session_state.edit_bt_v7_sd.strftime("%Y-%m-%d") != self.config.backtest.start_date:
                self.config.backtest.start_date = st.session_state.edit_bt_v7_sd.strftime("%Y-%m-%d")
        if "edit_bt_v7_ed" in st.session_state:
            if st.session_state.edit_bt_v7_ed.strftime("%Y-%m-%d") != self.config.backtest.end_date:
                self.config.backtest.end_date = st.session_state.edit_bt_v7_ed.strftime("%Y-%m-%d")
        if "edit_bt_v7_sb" in st.session_state:
            if st.session_state.edit_bt_v7_sb != self.config.backtest.starting_balance:
                self.config.backtest.starting_balance = st.session_state.edit_bt_v7_sb
        if "edit_bt_v7_minimum_coin_age_days" in st.session_state:
            if st.session_state.edit_bt_v7_minimum_coin_age_days != self.config.live.minimum_coin_age_days:
                self.config.live.minimum_coin_age_days = st.session_state.edit_bt_v7_minimum_coin_age_days
        if "edit_bt_v7_ohlcv_rolling_window" in st.session_state:
            if st.session_state.edit_bt_v7_ohlcv_rolling_window != self.config.live.ohlcv_rolling_window:
                self.config.live.ohlcv_rolling_window = st.session_state.edit_bt_v7_ohlcv_rolling_window
        if "relative_volume_filter_clip_pct" in st.session_state:
            if st.session_state.edit_bt_v7_relative_volume_filter_clip_pct != self.config.live.relative_volume_filter_clip_pct:
                self.config.live.relative_volume_filter_clip_pct = st.session_state.edit_bt_v7_relative_volume_filter_clip_pct
        if "edit_bt_v7_approved_coins" in st.session_state:
            if st.session_state.edit_bt_v7_approved_coins != self.config.live.approved_coins:
                self.config.live.approved_coins = st.session_state.edit_bt_v7_approved_coins
        # Display Editor
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            exchange_index = 0
            if self.config.backtest.exchange == "bybit":
                exchange_index = 1
            st.selectbox('Exchange',['binance', 'bybit'], index = exchange_index, key="edit_bt_v7_exchange")
        with col2:
            if not self.name:
                st.text_input(f":red[Backtest Name]", value=self.name, max_chars=64, key="edit_bt_v7_name")
            else:
                st.text_input(f"Backtest Name", value=self.name, max_chars=64, key="edit_bt_v7_name")
        with col3:
            st.date_input("START_DATE", datetime.datetime.strptime(self.config.backtest.start_date, '%Y-%m-%d'), format="YYYY-MM-DD", key="edit_bt_v7_sd")
        with col4:
            st.date_input("END_DATE", datetime.datetime.strptime(self.config.backtest.end_date, '%Y-%m-%d'), format="YYYY-MM-DD", key="edit_bt_v7_ed")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('STARTING_BALANCE',value=float(self.config.backtest.starting_balance),step=500.0, key="edit_bt_v7_sb")
        with col2:
            st.number_input("minimum_coin_age_days", value=float(round(self.config.live.minimum_coin_age_days, 1)), step=1.0, format="%.1f", key="edit_bt_v7_minimum_coin_age_days", help=pbgui_help.minimum_coin_age_days)
        with col3:
            st.number_input("ohlcv_rolling_window", value=self.config.live.ohlcv_rolling_window, step=1, format="%.d", key="edit_bt_v7_ohlcv_rolling_window", help=pbgui_help.ohlcv_rolling_window)
        with col4:
            st.number_input("relative_volume_filter_clip_pct", value=float(round(self.config.live.relative_volume_filter_clip_pct, 2)), min_value=0.0, max_value=1.0, step=0.01, format="%.2f", key="edit_bt_v7_relative_volume_filter_clip_pct", help=pbgui_help.relative_volume_filter_clip_pct)
        # symbol configuration
        for symbol in self.config.live.approved_coins.copy():
            if symbol not in self._available_symbols:
                self.config.live.approved_coins.remove(symbol)
        col1, col2 = st.columns([3,1], vertical_alignment="bottom")
        with col1:
            st.multiselect('symbols', self._available_symbols, default=self.config.live.approved_coins, key="edit_bt_v7_approved_coins")
        with col2:
            if st.button("Update Symbols", key="edit_bt_update_symbols"):
                exchange = Exchange(self.config.backtest.exchange)
                exchange.fetch_symbols()
                self._available_symbols = exchange.swap
                st.rerun()
        self.config.bot.edit()

    def remove_selected_results(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    self.backtest_results[row].remove()
        for result in self.backtest_results[:]:
            if not Path(result.result_path).exists():
                self.backtest_results.remove(result)

    def remove_all_results(self):
        rmtree(f'{pb7dir()}/backtests/pbgui/{self.name}', ignore_errors=True)
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
                'fills': False,
                'create_run': False,
                'delete': False,
                'adg': result.adg,
                'drawdown_worst': result.drawdown_worst,
                'sharpe_ratio': result.sharpe_ratio,
                'starting_balance': result.starting_balance,
                # 'final_balance': result.final_balance,
            })
        column_config = {
            "id": None,
            "view": st.column_config.CheckboxColumn(label="View Result"),
            "plot": st.column_config.CheckboxColumn(label="View be Plot"),
            "fills": st.column_config.CheckboxColumn(label="View Fills"),
            "create_run": st.column_config.CheckboxColumn(label="Create Run"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display Backtests
        height = 36+(len(d))*35
        if height > 1000: height = 1016
        st.data_editor(data=d, height=height, use_container_width=True, key=f'select_btv7_result_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','drawdown_max','final_balance'])
        if f'select_btv7_result_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_btv7_result_{ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        self.backtest_results[row].load_fills()
                        self.backtest_results[row].load_be()
                        self.backtest_results[row].view_chart_be()
                        self.backtest_results[row].view_chart_symbol()
                        self.backtest_results[row].view()
                if "plot" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["plot"]:
                        self.backtest_results[row].view_plot()
                if "fills" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["fills"]:
                        self.backtest_results[row].view_fills()
                if "create_run" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["create_run"]:
                        st.session_state.edit_v7_instance = V7Instance()
                        st.session_state.edit_v7_instance.config = self.backtest_results[row].config
                        st.session_state.edit_v7_instance.user = st.session_state.edit_v7_instance.config.live.user
                        st.switch_page("pages/70_V7 Run.py")

    def calculate_results(self):
        p = str(Path(f'{pb7dir()}/backtests/pbgui/{self.name}/{self.config.backtest.exchange}/**/analysis.json'))
        files = glob.glob(p, recursive=False)
        return len(files)

    def load_results(self):
        p = str(Path(f'{pb7dir()}/backtests/pbgui/{self.name}/{self.config.backtest.exchange}/**/analysis.json'))
        files = glob.glob(p, recursive=False)
        for file in files:
            result_path = PurePath(file).parent
            bt_result = BacktestV7Result(result_path)
            self.backtest_results.append(bt_result)
        
    def save(self):
        self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
        self.path = Path(f'{PBGDIR}/data/bt_v7/{self.name}')
        if not self.path.exists():
            self.path.mkdir(parents=True)
        self.config.config_file = Path(f'{self.path}/backtest.json')
        
        self.config.save_config()

    def save_queue(self, parameters : str = None):
        dest = Path(f'{PBGDIR}/data/bt_v7_queue')
        unique_filename = str(uuid.uuid4())
        file = Path(f'{dest}/{unique_filename}.json') 
        bt_dict = {
            "name": self.name,
            "filename": unique_filename,
            "json": str(self.config.config_file),
            "exchange": self.config.backtest.exchange,
        }
        if not dest.exists():
            dest.mkdir(parents=True)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(bt_dict, f, indent=4)

    def remove(self):
        self.remove_all_results()
        path = Path(self.path).parent
        rmtree(path, ignore_errors=True)

    @st.dialog("Paste config", width="large")
    def import_instance(self):
        # Init session_state for keys
        if "import_backtest_v7_config" in st.session_state:
            if st.session_state.import_backtest_v7_config != json.dumps(self.config.config, indent=4):
                try:
                    self.config.config = json.loads(st.session_state.import_backtest_v7_config)
                except:
                    error_popup("Invalid JSON")
            st.session_state.import_backtest_v7_config = json.dumps(self.config.config, indent=4)
        # Display import
        st.text_area(f'config', json.dumps(self.config.config, indent=4), key="import_backtest_v7_config", height=1200)
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("OK"):
                del st.session_state.edit_bt_v7_exchange
                del st.session_state.edit_bt_v7_name
                del st.session_state.edit_bt_v7_sd
                del st.session_state.edit_bt_v7_ed
                del st.session_state.edit_bt_v7_sb
                del st.session_state.edit_bt_v7_minimum_coin_age_days
                del st.session_state.edit_bt_v7_ohlcv_rolling_window
                del st.session_state.edit_bt_v7_relative_volume_filter_clip_pct
                del st.session_state.edit_bt_v7_approved_coins
                del st.session_state.edit_configv7_long_twe
                del st.session_state.edit_configv7_short_twe
                del st.session_state.edit_configv7_long_positions
                del st.session_state.edit_configv7_short_positions
                del st.session_state.edit_configv7_long
                del st.session_state.edit_configv7_short
                st.rerun()
        with col2:
            if st.button("Cancel"):
                st.rerun()


class BacktestV7Result:
    def __init__(self, result_path: str = None):
        self.result_path = result_path
        self.initialize()
    
    def initialize(self):
        self.result = self.load_result()
        self.config = ConfigV7(PurePath(f'{self.result_path}/config.json'))
        self.config.load_config()
        self.backtest_config = self.load_backtest_config()
        # self.sd = self.backtest_config["start_date"]
        self.ed = self.config.backtest.end_date
        self.adg = self.result["adg"]
        self.drawdown_worst = self.result["drawdown_worst"]
        self.sharpe_ratio = self.result["sharpe_ratio"]
        # self.final_balance = self.result["final_balance"]
        self.starting_balance = self.config.backtest.starting_balance
        self.be = None
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
        r = Path(f'{self.result_path}/config.json')
        try:
            with open(r, "r", encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f'{str(r)} is corrupted {e}')

    def load_be(self):
        if self.be is None:
            be = f'{self.result_path}/balance_and_equity.csv'
            self.be = pd.read_csv(be)
            timestamp = datetime.datetime.strptime(self.ed, '%Y-%m-%d').timestamp()
            start_time = timestamp - (self.be.iloc[:, 0].iloc[-1] * 60)
            self.be['time'] = datetime.datetime.fromtimestamp(start_time) + pd.to_timedelta(self.be.iloc[:, 0], unit='m')

    def load_fills(self):
        if self.fills is None:
            fills = f'{self.result_path}/fills.csv'
            self.fills = pd.read_csv(fills)
            timestamp = datetime.datetime.strptime(self.ed, '%Y-%m-%d').timestamp()
            start_time = timestamp - (self.fills['minute'].iloc[-1] * 60)
            self.fills['time'] = datetime.datetime.fromtimestamp(start_time) + pd.to_timedelta(self.fills['minute'], unit='m')

    def view_plot(self):
        balance_and_equity = Path(f'{self.result_path}/balance_and_equity.png')
        if balance_and_equity.exists():
            st.image(str(balance_and_equity), use_column_width=True)
    
    def view_fills(self):
        p = str(Path(f'{self.result_path}/fills_plots/*.png'))
        fills = glob.glob(p)
        for fill in fills:
            st.image(fill, use_column_width=True)

    def view(self):
        col1, col2 = st.columns([1,1])
        with col1:
            st.code(json.dumps(self.result, indent=4))
        with col2:
            st.code(config_pretty_str(self.config.config))

    # Create Chart with plotly
    def view_chart_be(self):
        fig = go.Figure()
        fig.update_layout(yaxis_title='Balance')
        fig.add_trace(go.Scatter(x=self.be['time'], y=self.be['equity'], name="equity", line=dict(width=0.75)))
        fig.add_trace(go.Scatter(x=self.be['time'], y=self.be['balance'], name="balance", line=dict(width=2.5)))
        fig.update_layout(yaxis_title='Balance', height=800)
        fig.update_xaxes(showgrid=True, griddash="dot")
        st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_be")

    # Create Symbol Chart with plotly
    def view_chart_symbol(self):
        fig = go.Figure()
        for symbol in self.fills['symbol'].unique():
            symbol_df = self.fills[self.fills['symbol'] == symbol].copy()
            symbol_df["sym_pnl"] = symbol_df["pnl"].cumsum()
            fig.add_trace(go.Scatter(x=symbol_df['time'], y=symbol_df['sym_pnl'], name=symbol))
        fig.update_layout(yaxis_title='PnL', height=800, )
        fig['data'][0]['showlegend'] = True
        fig.update_xaxes(showgrid=True, griddash="dot")
        st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_symbols")

class BacktestsV7:
    def __init__(self):
        self.backtests = []

    def view_backtests(self):
        # Init
        if not self.backtests:
            self.find_backtests()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_backtest_v7_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_backtest_v7_{ed_key}']
            for row in ed["edited_rows"]:
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.bt_v7 = self.backtests[row]
                    st.rerun()
                if "view" in ed["edited_rows"][row]:
                    st.session_state.bt_v7_results = self.backtests[row]
                    st.rerun()
                if 'delete' in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]['delete']:
                        self.backtests[row].remove()
                        self.backtests.pop(row)
                        st.rerun()
        d = []
        for id, bt in enumerate(self.backtests):
            d.append({
                'id': id,
                'edit': False,
                'Name': bt.name,
                'view': False,
                'Backtests': bt.calculate_results(),
                'delete' : False,
            })
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Results"),
            }
        #Display Backtests
        st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_backtest_v7_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def find_backtests(self):
        p = str(Path(f'{PBGDIR}/data/bt_v7/**/backtest.json'))
        found_bt = glob.glob(p, recursive=False)
        if found_bt:
            for p in found_bt:
                bt = BacktestV7Item(p)
                self.backtests.append(bt)
    
def main():
    # Disable Streamlit Warnings when running directly
    logging.getLogger("streamlit.runtime.state.session_state_proxy").disabled=True
    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").disabled=True
    bt = BacktestV7Queue()
    while True:
        bt.load()
        for item in bt.items:
            while bt.running() >= bt.cpu:
                time.sleep(5)
            while bt.downloading():
                time.sleep(5)
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if not eval(pb_config.get("backtest_v7", "autostart")):
                return
            if item.status() == "not started":
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Backtesting {item.filename} started')
                item.run()
                time.sleep(1)
        time.sleep(60)

if __name__ == '__main__':
    main()