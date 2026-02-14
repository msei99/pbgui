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
from pbgui_func import PBGDIR, pb7dir, pb7venv, validateJSON, load_symbols_from_ini, error_popup, info_popup, get_navi_paths, replace_special_chars
from pbgui_purefunc import config_pretty_str, pb7_suite_preflight_errors
from pbgui_purefunc import load_ini, save_ini
from PBCoinData import CoinData, normalize_symbol
import uuid
from Base import Base
from Exchange import Exchange, V7
from Config import Config, ConfigV7, BalanceCalculator, Logging, ConfigV7Editor
from pathlib import Path, PurePath
from shutil import rmtree, copytree
import shutil
from RunV7 import V7Instance
import OptimizeV7
import datetime
import logging
import os
import fnmatch

class BacktestV7QueueItem():
    def __init__(self):
        self.name = None
        self.filename = None
        self.json = None
        self.exchange = None
        self.log = None
        self.log_show = False
        self.pid = None
        self.pidfile = None
        self.config = ConfigV7()

    def remove(self):
        self.stop()
        file = Path(f'{PBGDIR}/data/bt_v7_queue/{self.filename}.json')
        file.unlink(missing_ok=True)
        self.log.unlink(missing_ok=True)
        self.pidfile.unlink(missing_ok=True)

    def load_log(self, log_size: int = 50):
        if self.log:
            if self.log.exists():
                # Open the file in binary mode to handle raw bytes
                with open(self.log, 'rb') as f:
                    # Move the pointer to the last log_size KB (100 * 1024 bytes)
                    f.seek(0, 2)  # Move to the end of the file
                    file_size = f.tell()
                    # Ensure that we don't try to read more than the file size
                    start_pos = max(file_size - log_size * 1024, 0)
                    f.seek(start_pos)
                    # Read the last 100 KB (or less if the file is smaller)
                    return f.read().decode('utf-8', errors='ignore')  # Decode and ignore errors

    @st.fragment
    def view_log(self):
        col1, col2, col3 = st.columns([1,1,8], vertical_alignment="bottom")
        with col1:
            st.checkbox("Reverse", value=True , key=f'reverse_view_log_{self.name}', )
        with col2:
            st.selectbox("view last kB", [50, 100, 250, 500, 1000, 2000, 5000, 10000, 100000], key=f'size_view_log_{self.name}')
        with col3:
            if st.button(":material/refresh:", key=f'refresh_view_log_{self.name}'):
                st.rerun(scope="fragment")
        logfile = self.load_log(st.session_state[f'size_view_log_{self.name}'])
        if logfile:
            if st.session_state[f'reverse_view_log_{self.name}']:
                logfile = '\n'.join(logfile.split('\n')[::-1])
        with st.container(height=1200):
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
            if self.pid and psutil.pid_exists(self.pid) and (any(sub.lower().endswith("backtest.py") for sub in psutil.Process(self.pid).cmdline())):
                return True
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            pass
        return False

    def is_finish(self):
        # Can only be finished if the process is not running anymore
        if self.is_running():
            return False
        
        log = self.load_log()
        if log:
            # Check for completion markers
            # Regular backtest: "seconds elapsed for backtest:"
            # Suite backtest: "Suite ... completed"
            if "seconds elapsed for backtest:" in log or "Suite" in log and "completed" in log:
                return True
        return False

    def is_error(self):
        log = self.load_log()
        if log:
            # If backtest finished successfully, no error
            # Check both regular and Suite completion markers
            if "seconds elapsed for backtest:" in log or ("Suite" in log and "completed" in log):
                return False
            # If process is still running, not an error yet
            elif self.is_running():
                return False
            else:
                return True
        else:
            return False

    def is_backtesting(self):
        if self.is_running():
            log = self.load_log()
            if log:
                # If finished, not backtesting anymore
                # Check both regular and Suite completion markers
                if "seconds elapsed for backtest:" in log or ("Suite" in log and "completed" in log):
                    return False
                # If we see "Backtesting " or "Running scenario" in the log, we're in backtesting phase
                elif "Backtesting " in log or "Running scenario" in log:
                    return True
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
            old_os_path = os.environ.get('PATH', '')
            new_os_path = os.path.dirname(pb7venv()) + os.pathsep + old_os_path
            os.environ['PATH'] = new_os_path
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
            os.environ['PATH'] = old_os_path

class BacktestV7Queue:
    def __init__(self):
        self.items = []
        self.d = []
        self.sort = "Time"
        self.sort_order = True
        self.load_sort_queue()
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
                    self.d[row]['item'].remove()
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
                qitem.config.config_file = qitem.json
                qitem.config.load_config()
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
        p = self.pid()
        if p:
            try:
                p.kill()
            except Exception:
                # Can't kill (e.g., zombie or already gone) — ignore
                pass

    def is_running(self):
        p = self.pid()
        return bool(p)

    def pid(self):
        # Return a psutil.Process for the running BacktestV7 process, or None.
        for process in psutil.process_iter():
            try:
                # If the process is a zombie, skip it (cannot be reaped here).
                try:
                    if process.status() == psutil.STATUS_ZOMBIE:
                        continue
                except Exception:
                    # status() can raise for short-lived processes; ignore.
                    pass

                cmdline = process.cmdline()
            except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
                continue
            if any("BacktestV7.py" in sub for sub in cmdline):
                return process
        return None

    def refresh(self):
        # Remove items from d that are not in items anymore
        self.d = [item for item in self.d if item.get('filename') in [i.filename for i in self.items]]
        # Add items to d that are in items but not in d
        for item in self.items:
            if not any(d_item.get('filename') == item.filename for d_item in self.d):
                self.d.append({
                    'id': len(self.d),
                    'run': item.is_running(),
                    'Status': item.status(),
                    'view': False,
                    'log': item.log_show,
                    'delete': False,
                    'Name': item.name,
                    'filename': item.filename,
                    'Time': datetime.datetime.fromtimestamp(Path(f'{PBGDIR}/data/bt_v7_queue/{item.filename}.json').stat().st_mtime),
                    'exchange': item.exchange,
                    'finish': item.is_finish(),
                    'item': item,
                })
        # Update status of all items in d
        for row in self.d:
            for item in self.items:
                if row['filename'] == item.filename:
                    row['run'] = item.is_running()
                    row['Status'] = item.status()
                    row['log'] = item.log_show
                    row['finish'] = item.is_finish()

    def view(self):
        if not self.items:
            self.load()
            self.refresh()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'view_bt_v7_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_bt_v7_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["run"]:
                        self.d[row]["item"].run()
                    else:
                        self.d[row]["item"].stop()
                    self.refresh()
                if "view" in ed["edited_rows"][row]:
                    bt = BacktestV7Results()
                    bt.results_path = f'{pb7dir()}/backtests/pbgui/{self.d[row]["item"].name}'
                    st.session_state.bt_v7_results = bt
                    del st.session_state.bt_v7_queue
                    st.rerun()
                if "log" in ed["edited_rows"][row]:
                    self.d[row]["item"].log_show = ed["edited_rows"][row]["log"]
                    self.d[row]["log"] = ed["edited_rows"][row]["log"]
        column_config = {
            # "id": None,
            "run": st.column_config.CheckboxColumn('Start/Stop', default=False),
            "view": st.column_config.CheckboxColumn(label="View Results"),
            "log": st.column_config.CheckboxColumn(label="View Logfile"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            "id": st.column_config.NumberColumn(format="%.0f", label="ID"),
            "Name": st.column_config.TextColumn(label="Name"),
            "filename": st.column_config.TextColumn(label="Filename"),
            "exchange": st.column_config.ListColumn(label="Exchange"),
            "finish": st.column_config.CheckboxColumn(label="Finished"),
            }
        #Display Queue
        height = 36+(len(self.d))*35
        if "sort_bt_v7_queue" in st.session_state:
            if st.session_state.sort_bt_v7_queue != self.sort:
                self.sort = st.session_state.sort_bt_v7_queue
                self.save_sort_queue()
        else:
            st.session_state.sort_bt_v7_queue = self.sort
        if "sort_bt_v7_queue_order" in st.session_state:
            if st.session_state.sort_bt_v7_queue_order != self.sort_order:
                self.sort_order = st.session_state.sort_bt_v7_queue_order
                self.save_sort_queue()
        else:
            st.session_state.sort_bt_v7_queue_order = self.sort_order
        # Display sort options
        col1, col2 = st.columns([1, 9], vertical_alignment="bottom")
        with col1:
            st.selectbox("Sort by:", ['Time', 'Name', 'Status'], key=f'sort_bt_v7_queue', index=0)
        with col2:
            st.checkbox("Reverse", value=True, key=f'sort_bt_v7_queue_order')
        # Sort results
        self.d = sorted(self.d, key=lambda x: x[st.session_state[f'sort_bt_v7_queue']], reverse=st.session_state[f'sort_bt_v7_queue_order'])
        if height > 1000: height = 1016
        st.data_editor(data=self.d, height="auto", key=f'view_bt_v7_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','filename','name','finish','running'])
        for item in self.items:
            if item.log_show:
                item.view_log()

    def load_sort_queue(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.sort = pb_config.get("backtest_v7", "sort_queue") if pb_config.has_option("backtest_v7", "sort_queue") else "Time"
        self.sort_order = eval(pb_config.get("backtest_v7", "sort_queue_order")) if pb_config.has_option("backtest_v7", "sort_queue_order") else True

    def save_sort_queue(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("backtest_v7"):
            pb_config.add_section("backtest_v7")
        pb_config.set("backtest_v7", "sort_queue", str(self.sort))
        pb_config.set("backtest_v7", "sort_queue_order", str(self.sort_order))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

class BacktestV7Item(ConfigV7Editor):
    def __init__(self, backtest_path: str = None):
        self.path = backtest_path
        self.config = ConfigV7()
        self.log = None
        self.results = BacktestV7Results()
        if backtest_path:
            self.config.config_file = backtest_path
            self.config.load_config()
            self.name = self.config.backtest.base_dir.split('/')[-1]
            self.results.results_path = str(Path(f'{pb7dir()}/backtests/pbgui/{self.name}'))
            self.date = Path(self.path).stat().st_mtime
            self.results.name = self.name
        else:
            self.initialize()

    def initialize(self):
        self.name = ""
        self.config.backtest.start_date = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.config.backtest.end_date = datetime.date.today().strftime("%Y-%m-%d")
        self.config.optimize.n_cpus = multiprocessing.cpu_count()
    
    # ============ ABSTRACT METHOD IMPLEMENTATIONS ============
    
    def _get_key_prefix(self):
        """Return key prefix for streamlit widgets."""
        return "bt_"

    # Exchanges
    @st.fragment
    def fragment_exchanges(self):
        if "edit_bt_v7_exchanges" in st.session_state:
            if st.session_state.edit_bt_v7_exchanges != self.config.backtest.exchanges:
                self.config.backtest.exchanges = st.session_state.edit_bt_v7_exchanges
                st.rerun()
        else:
            st.session_state.edit_bt_v7_exchanges = self.config.backtest.exchanges
        # PB7 supports additional v7 exchanges; also allow the special "combined" dataset.
        # Note: "combined" uses coin_sources to select the data feed per coin.
        options = []
        try:
            options = list(V7.list())
        except Exception:
            options = ["binance", "bybit", "gateio", "bitget", "hyperliquid", "okx"]
        if "combined" not in options:
            options = ["combined"] + options
        st.multiselect('Exchanges', options, key="edit_bt_v7_exchanges")

    # name
    @st.fragment
    def fragment_name(self):
        if "edit_bt_v7_name" in st.session_state:
            if st.session_state.edit_bt_v7_name != self.name:
                # Avoid creation of unwanted subfolders
                st.session_state.edit_bt_v7_name = replace_special_chars(st.session_state.edit_bt_v7_name)
                self.name = st.session_state.edit_bt_v7_name
                self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
        else:
            st.session_state.edit_bt_v7_name = self.name
        if not self.name:
            st.text_input(f":red[Backtest Name]",max_chars=64, key="edit_bt_v7_name")
        else:
            st.text_input("Backtest Name", max_chars=64, help=pbgui_help.task_name, key="edit_bt_v7_name")

    # start_data
    @st.fragment
    def fragment_start_date(self):
        if "edit_bt_v7_start_date" in st.session_state:
            if st.session_state.edit_bt_v7_start_date.strftime("%Y-%m-%d") != self.config.backtest.start_date:
                self.config.backtest.start_date = st.session_state.edit_bt_v7_start_date.strftime("%Y-%m-%d")
        else:
            st.session_state.edit_bt_v7_start_date = datetime.datetime.strptime(self.config.backtest.start_date, '%Y-%m-%d')
        st.date_input("start_date", format="YYYY-MM-DD", key="edit_bt_v7_start_date")

    # end_date
    @st.fragment
    def fragment_end_date(self):
        if "edit_bt_v7_end_date" in st.session_state:
            if st.session_state.edit_bt_v7_end_date.strftime("%Y-%m-%d") != self.config.backtest.end_date:
                self.config.backtest.end_date = st.session_state.edit_bt_v7_end_date.strftime("%Y-%m-%d")
        else:
            st.session_state.edit_bt_v7_end_date = datetime.datetime.strptime(self.config.backtest.end_date, '%Y-%m-%d')
        st.date_input("end_date", format="YYYY-MM-DD", key="edit_bt_v7_end_date")

    # balance_sample_divider
    @st.fragment
    def fragment_balance_sample_divider(self):
        if "edit_bt_v7_balance_sample_divider" in st.session_state:
            if st.session_state.edit_bt_v7_balance_sample_divider != self.config.backtest.balance_sample_divider:
                self.config.backtest.balance_sample_divider = st.session_state.edit_bt_v7_balance_sample_divider
        else:
            st.session_state.edit_bt_v7_balance_sample_divider = self.config.backtest.balance_sample_divider
        st.number_input("balance_sample_divider", min_value=1, step=1, key="edit_bt_v7_balance_sample_divider", help=pbgui_help.backtest_balance_sample_divider)

    # logging
    @st.fragment
    def fragment_logging(self):
        if "edit_bt_v7_logging_level" in st.session_state:
            if st.session_state.edit_bt_v7_logging_level != self.config.logging.level:
                self.config.logging.level = st.session_state.edit_bt_v7_logging_level
        else:
            st.session_state.edit_bt_v7_logging_level = self.config.logging.level
        st.selectbox("logging level", Logging.LEVEL, format_func=lambda x: Logging.LEVEL.get(x), key="edit_bt_v7_logging_level", help=pbgui_help.logging_level)

    # starting_balance
    @st.fragment
    def fragment_starting_balance(self):
        if "edit_bt_v7_starting_balance" in st.session_state:
            if st.session_state.edit_bt_v7_starting_balance != self.config.backtest.starting_balance:
                self.config.backtest.starting_balance = st.session_state.edit_bt_v7_starting_balance
        else:
            st.session_state.edit_bt_v7_starting_balance = float(self.config.backtest.starting_balance)
        st.number_input("starting_balance", step=500.0, key="edit_bt_v7_starting_balance")

    # minimum_coin_aga_days
    @st.fragment
    def fragment_minimum_coin_age_days(self):
        if "edit_bt_v7_minimum_coin_age_days" in st.session_state:
            if st.session_state.edit_bt_v7_minimum_coin_age_days != self.config.live.minimum_coin_age_days:
                self.config.live.minimum_coin_age_days = st.session_state.edit_bt_v7_minimum_coin_age_days
        else:
            st.session_state.edit_bt_v7_minimum_coin_age_days = self.config.live.minimum_coin_age_days
        st.number_input("minimum_coin_age_days", min_value=0, step=1, key="edit_bt_v7_minimum_coin_age_days", help=pbgui_help.minimum_coin_age_days)

    # gap_tolerance_ohlcvs_minutes
    @st.fragment
    def fragment_gap_tolerance_ohlcvs_minutes(self):
        if "edit_bt_v7_gap_tolerance_ohlcvs_minutes" in st.session_state:
            if st.session_state.edit_bt_v7_gap_tolerance_ohlcvs_minutes != self.config.backtest.gap_tolerance_ohlcvs_minutes:
                self.config.backtest.gap_tolerance_ohlcvs_minutes = st.session_state.edit_bt_v7_gap_tolerance_ohlcvs_minutes
        else:
            st.session_state.edit_bt_v7_gap_tolerance_ohlcvs_minutes = self.config.backtest.gap_tolerance_ohlcvs_minutes
        st.number_input("gap_tolerance_ohlcvs_minutes", min_value=0, step=1, key="edit_bt_v7_gap_tolerance_ohlcvs_minutes", help=pbgui_help.gap_tolerance_ohlcvs_minutes)

    # ohlcv_source_dir
    @st.fragment
    def fragment_ohlcv_source_dir(self):
        from pbgui_func import PBGDIR
        
        key = "edit_bt_v7_ohlcv_source_dir"
        use_pbgui_key = "edit_bt_v7_use_pbgui_ohlcv"
        pbgui_ohlcv_path = str(PBGDIR / "data" / "ohlcv")
        
        if not hasattr(self.config.backtest, "ohlcv_source_dir"):
            setattr(self.config.backtest, "_ohlcv_source_dir", None)
            if hasattr(self.config.backtest, "_backtest") and isinstance(self.config.backtest._backtest, dict):
                self.config.backtest._backtest.setdefault("ohlcv_source_dir", None)
        
        # Initialize session state for checkbox
        if use_pbgui_key not in st.session_state:
            current = getattr(self.config.backtest, "ohlcv_source_dir", None)
            if current is None and hasattr(self.config.backtest, "_backtest"):
                current = self.config.backtest._backtest.get("ohlcv_source_dir")
            # Auto-detect if current value is PBGui path
            st.session_state[use_pbgui_key] = current == pbgui_ohlcv_path
        
        # Initialize session state for text input
        if key not in st.session_state:
            current = getattr(self.config.backtest, "ohlcv_source_dir", None)
            if current is None and hasattr(self.config.backtest, "_backtest"):
                current = self.config.backtest._backtest.get("ohlcv_source_dir")
            st.session_state[key] = current or ""
        
        # Determine the actual value to use
        use_pbgui = st.session_state[use_pbgui_key]
        if use_pbgui:
            value = pbgui_ohlcv_path
            st.session_state[key] = value
        else:
            value = str(st.session_state.get(key) or "").strip()
        
        # Update config
        if hasattr(type(self.config.backtest), "ohlcv_source_dir"):
            self.config.backtest.ohlcv_source_dir = value if value else None
        else:
            self.config.backtest._ohlcv_source_dir = value if value else None
            if hasattr(self.config.backtest, "_backtest") and isinstance(self.config.backtest._backtest, dict):
                self.config.backtest._backtest["ohlcv_source_dir"] = self.config.backtest._ohlcv_source_dir
        
        # Layout: Checkbox and text input side by side
        col_chk, col_val = st.columns([1, 3], vertical_alignment="top")
        with col_chk:
            st.checkbox(
                "Use PBGui OHLCV data",
                key=use_pbgui_key,
                help="When enabled, use PBGui's market data from data/ohlcv instead of PB7's cache/historical_data"
            )
        with col_val:
            st.text_input(
                "ohlcv_source_dir",
                key=key,
                help=pbgui_help.ohlcv_source_dir,
                disabled=use_pbgui
            )

    # maker_fee_override
    @st.fragment
    def fragment_maker_fee_override(self):
        enabled_key = "edit_bt_v7_maker_fee_override_enabled"
        value_key = "edit_bt_v7_maker_fee_override"

        if enabled_key not in st.session_state:
            st.session_state[enabled_key] = self.config.backtest.maker_fee_override is not None

        if value_key not in st.session_state:
            st.session_state[value_key] = (
                float(self.config.backtest.maker_fee_override)
                if self.config.backtest.maker_fee_override is not None
                else 0.0
            )

        col_chk, col_val = st.columns([1, 1], vertical_alignment="center")
        with col_chk:
            st.checkbox("maker_fee_override", key=enabled_key, help=pbgui_help.maker_fee_override)
        with col_val:
            st.number_input(
                "maker_fee_override value",
                min_value=0.0,
                max_value=0.01,
                step=0.00001,
                format="%.5f",
                key=value_key,
                disabled=not st.session_state[enabled_key],
                label_visibility="collapsed",
            )

        if st.session_state[enabled_key]:
            # Ensure config is updated on first enable (before any widget interaction)
            self.config.backtest.maker_fee_override = float(st.session_state[value_key])
        else:
            self.config.backtest.maker_fee_override = None

    # max_warmup_minutes
    @st.fragment
    def fragment_max_warmup_minutes(self):
        if "edit_bt_v7_max_warmup_minutes" in st.session_state:
            if st.session_state.edit_bt_v7_max_warmup_minutes != self.config.backtest.max_warmup_minutes:
                self.config.backtest.max_warmup_minutes = st.session_state.edit_bt_v7_max_warmup_minutes
        else:
            st.session_state.edit_bt_v7_max_warmup_minutes = self.config.backtest.max_warmup_minutes
        st.number_input("max_warmup_minutes", min_value=0.0, step=1440.0, key="edit_bt_v7_max_warmup_minutes", help=pbgui_help.max_warmup_minutes)

    # filter_by_min_effective_cost
    @st.fragment
    def fragment_filter_by_min_effective_cost(self):
        if "edit_bt_v7_filter_by_min_effective_cost" in st.session_state:
            if st.session_state.edit_bt_v7_filter_by_min_effective_cost != self.config.backtest.filter_by_min_effective_cost:
                self.config.backtest.filter_by_min_effective_cost = st.session_state.edit_bt_v7_filter_by_min_effective_cost
        else:
            st.session_state.edit_bt_v7_filter_by_min_effective_cost = self.config.backtest.filter_by_min_effective_cost
        st.checkbox("filter_by_min_effective_cost", key="edit_bt_v7_filter_by_min_effective_cost", help=pbgui_help.bt_filter_by_min_effective_cost)

    # combine_ohlcvs
    @st.fragment
    def fragment_combine_ohlcvs(self):
        if "edit_bt_v7_combine_ohlcvs" in st.session_state:
            if st.session_state.edit_bt_v7_combine_ohlcvs != self.config.backtest.combine_ohlcvs:
                self.config.backtest.combine_ohlcvs = st.session_state.edit_bt_v7_combine_ohlcvs
        else:
            st.session_state.edit_bt_v7_combine_ohlcvs = self.config.backtest.combine_ohlcvs
        st.checkbox("combine_ohlcvs", key="edit_bt_v7_combine_ohlcvs", help=pbgui_help.combine_ohlcvs)

    # compress_cache
    @st.fragment
    def fragment_compress_cache(self):
        if "edit_bt_v7_compress_cache" in st.session_state:
            if st.session_state.edit_bt_v7_compress_cache != self.config.backtest.compress_cache:
                self.config.backtest.compress_cache = st.session_state.edit_bt_v7_compress_cache
        else:
            st.session_state.edit_bt_v7_compress_cache = self.config.backtest.compress_cache
        st.checkbox("compress_cache", key="edit_bt_v7_compress_cache", help=pbgui_help.compress_cache)
    
    # coin_sources
    @st.fragment
    def fragment_coin_sources(self):
        # Collect all scenario coin_sources to prevent conflicts
        # Like Passivbot's collect_suite_coin_sources(), we need to detect when
        # a coin is assigned to different exchanges across scenarios
        all_suite_sources = {}
        if self.config.backtest.suite:
            for scenario in self.config.backtest.suite.scenarios:
                if scenario.coin_sources:
                    for coin, exchange in scenario.coin_sources.items():
                        if coin in all_suite_sources and all_suite_sources[coin] != exchange:
                            # Conflict detected - mark as conflicted by storing None
                            # This prevents base from adding this coin with ANY exchange
                            all_suite_sources[coin] = None
                        elif coin not in all_suite_sources:
                            all_suite_sources[coin] = exchange
        
        self._edit_coin_sources_ui(
            self.config.backtest.coin_sources,
            # coin_sources defines the *data feed* per coin; allow choosing from all exchanges
            # regardless of which execution exchange(s) are selected.
            V7.list(),
            key_prefix="bt_",
            save_callback=lambda cs: setattr(self.config.backtest, 'coin_sources', cs),
            current_exchanges=[e for e in (self.config.backtest.exchanges or []) if e in set(V7.list())],
            all_suite_coin_sources=all_suite_sources
        )
    
    def _get_available_symbols(self, exchanges=None):
        """Get available symbols from coindata for specified exchanges.
        Returns normalized coin names (without USDT/USDC suffixes).
        
        Args:
            exchanges: List of exchange names. If None, uses config.backtest.exchanges
        """
        if exchanges is None:
            exchanges = self.config.backtest.exchanges
        
        symbols = []
        for exchange in V7.list():
            if exchange in exchanges and f"coindata_{exchange}" in st.session_state:
                symbols.extend(st.session_state[f"coindata_{exchange}"].symbols)
        
        # Normalize and deduplicate (BTCUSDT + BTCUSDC -> BTC)
        normalized = [normalize_symbol(s) for s in symbols]
        return sorted(list(set(normalized)))
    
    # All Suite and coin_sources methods are now inherited from ConfigV7Editor:
    # - fragment_coin_sources()
    # - _edit_coin_sources_ui()
    # - _get_exchanges_for_coin()
    # - _get_override_parameters()
    # - _get_aggregate_metrics()
    # - _edit_aggregate_ui()
    # - _edit_scenario_start_date()
    # - _edit_scenario_end_date()
    # - _edit_scenario_ui()
    # - _add_scenario_ui()
    # - fragment_suite()
    
    # btc_collateral_cap
    @st.fragment
    def fragment_btc_collateral_cap(self):
        if "edit_bt_v7_btc_collateral_cap" in st.session_state:
            if st.session_state.edit_bt_v7_btc_collateral_cap != self.config.backtest.btc_collateral_cap:
                self.config.backtest.btc_collateral_cap = st.session_state.edit_bt_v7_btc_collateral_cap
        else:
            st.session_state.edit_bt_v7_btc_collateral_cap = self.config.backtest.btc_collateral_cap
        st.number_input("btc_collateral_cap", min_value=0.0, max_value=10.0, step=0.1, format="%.2f", key="edit_bt_v7_btc_collateral_cap", help=pbgui_help.btc_collateral_cap)

    # btc_collateral_ltv_cap
    @st.fragment
    def fragment_btc_collateral_ltv_cap(self):
        if "edit_bt_v7_btc_collateral_ltv_cap" in st.session_state:
            new_val = st.session_state.edit_bt_v7_btc_collateral_ltv_cap
            # Convert 0 to None for the config
            config_val = None if new_val == 0.0 else new_val
            if config_val != self.config.backtest.btc_collateral_ltv_cap:
                self.config.backtest.btc_collateral_ltv_cap = config_val
        else:
            # Convert None to 0 for the UI
            st.session_state.edit_bt_v7_btc_collateral_ltv_cap = self.config.backtest.btc_collateral_ltv_cap if self.config.backtest.btc_collateral_ltv_cap is not None else 0.0
        st.number_input("btc_collateral_ltv_cap", min_value=0.0, max_value=1.0, step=0.1, format="%.2f", key="edit_bt_v7_btc_collateral_ltv_cap", help=pbgui_help.btc_collateral_ltv_cap)

    # filters
    def fragment_filter_coins(self):
        col1, col2, col3, col4, col5 = st.columns([1,1,1,0.5,0.5], vertical_alignment="bottom")
        with col1:
            self.fragment_market_cap()
        with col2:
            self.fragment_vol_mcap()
        with col3:
            self.fragment_tags()
        with col4:
            self.fragment_only_cpt()
            self.fragment_notices_ignore()
        with col5:
            st.checkbox("apply_filters", value=False, help=pbgui_help.apply_filters, key="edit_bt_v7_apply_filters")
        def _normalize_list(items):
            normalized = []
            for item in items:
                base = normalize_symbol(item)
                if base and base not in normalized:
                    normalized.append(base)
            return normalized
        # Init session state for approved_coins
        if "edit_bt_v7_approved_coins_long" in st.session_state:
            if st.session_state.edit_bt_v7_approved_coins_long != self.config.live.approved_coins.long:
                self.config.live.approved_coins.long = st.session_state.edit_bt_v7_approved_coins_long
        else:
            self.config.live.approved_coins.long = _normalize_list(self.config.live.approved_coins.long)
            st.session_state.edit_bt_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_bt_v7_approved_coins_short" in st.session_state:
            if st.session_state.edit_bt_v7_approved_coins_short != self.config.live.approved_coins.short:
                self.config.live.approved_coins.short = st.session_state.edit_bt_v7_approved_coins_short
        else:
            self.config.live.approved_coins.short = _normalize_list(self.config.live.approved_coins.short)
            st.session_state.edit_bt_v7_approved_coins_short = self.config.live.approved_coins.short
        # Init session state for ignored_coins
        if "edit_bt_v7_ignored_coins_long" in st.session_state:
            if st.session_state.edit_bt_v7_ignored_coins_long != self.config.live.ignored_coins.long:
                self.config.live.ignored_coins.long = st.session_state.edit_bt_v7_ignored_coins_long
        else:
            self.config.live.ignored_coins.long = _normalize_list(self.config.live.ignored_coins.long)
            st.session_state.edit_bt_v7_ignored_coins_long = self.config.live.ignored_coins.long
        if "edit_bt_v7_ignored_coins_short" in st.session_state:
            if st.session_state.edit_bt_v7_ignored_coins_short != self.config.live.ignored_coins.short:
                self.config.live.ignored_coins.short = st.session_state.edit_bt_v7_ignored_coins_short
        else:
            self.config.live.ignored_coins.short = _normalize_list(self.config.live.ignored_coins.short)
            st.session_state.edit_bt_v7_ignored_coins_short = self.config.live.ignored_coins.short
        # Apply filters
        if st.session_state.edit_bt_v7_apply_filters:
            def _extend_coins(target, items):
                # Items are already normalized SHORT names from PBCoinData
                for item in items:
                    if item:
                        target.append(item)

            approved = []
            ignored = []
            if "bybit" in self.config.backtest.exchanges:
                _extend_coins(approved, st.session_state.coindata_bybit.approved_coins)
                _extend_coins(ignored, st.session_state.coindata_bybit.ignored_coins)
            if "binance" in self.config.backtest.exchanges:
                _extend_coins(approved, st.session_state.coindata_binance.approved_coins)
                _extend_coins(ignored, st.session_state.coindata_binance.ignored_coins)
            if "gateio" in self.config.backtest.exchanges:
                _extend_coins(approved, st.session_state.coindata_gateio.approved_coins)
                _extend_coins(ignored, st.session_state.coindata_gateio.ignored_coins)
            if "bitget" in self.config.backtest.exchanges:
                _extend_coins(approved, st.session_state.coindata_bitget.approved_coins)
                _extend_coins(ignored, st.session_state.coindata_bitget.ignored_coins)
            if "hyperliquid" in self.config.backtest.exchanges and "coindata_hyperliquid" in st.session_state:
                _extend_coins(approved, st.session_state.coindata_hyperliquid.approved_coins)
                _extend_coins(ignored, st.session_state.coindata_hyperliquid.ignored_coins)
            self.config.live.approved_coins.long = sorted(set(approved))
            self.config.live.approved_coins.short = sorted(set(approved))
            self.config.live.ignored_coins.long = sorted(set(ignored))
            self.config.live.ignored_coins.short = sorted(set(ignored))
        # Remove unavailable symbols
        symbols_raw = []
        if "bybit" in self.config.backtest.exchanges:
            symbols_raw.extend(st.session_state.coindata_bybit.symbols)
        if "binance" in self.config.backtest.exchanges:
            symbols_raw.extend(st.session_state.coindata_binance.symbols)
        if "gateio" in self.config.backtest.exchanges:
            symbols_raw.extend(st.session_state.coindata_gateio.symbols)
        if "bitget" in self.config.backtest.exchanges:
            symbols_raw.extend(st.session_state.coindata_bitget.symbols)
        if "hyperliquid" in self.config.backtest.exchanges and "coindata_hyperliquid" in st.session_state:
            symbols_raw.extend(st.session_state.coindata_hyperliquid.symbols)
        symbols_raw = list(set(symbols_raw))
        base_to_full = {}
        for sym in symbols_raw:
            base = normalize_symbol(sym)
            if base:
                base_to_full.setdefault(base, set()).add(sym)
        symbols = sorted({normalize_symbol(sym) for sym in symbols_raw if sym})

        def is_available(coin: str) -> bool:
            if not coin:
                return False
            if coin in symbols_raw:
                return True
            base = normalize_symbol(coin)
            return base in base_to_full

        for symbol in self.config.live.approved_coins.long.copy():
            if not is_available(symbol):
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.approved_coins.short.copy():
            if not is_available(symbol):
                self.config.live.approved_coins.short.remove(symbol)
        for symbol in self.config.live.ignored_coins.long.copy():
            if not is_available(symbol):
                self.config.live.ignored_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short.copy():
            if not is_available(symbol):
                self.config.live.ignored_coins.short.remove(symbol)
        # Remove from approved_coins when in ignored coins
        for symbol in self.config.live.ignored_coins.long:
            if symbol in self.config.live.approved_coins.long:
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short:
            if symbol in self.config.live.approved_coins.short:
                self.config.live.approved_coins.short.remove(symbol)
        # Correct Display of Symbols
        if "edit_bt_v7_approved_coins_long" in st.session_state:
            st.session_state.edit_bt_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_bt_v7_approved_coins_short" in st.session_state:
            st.session_state.edit_bt_v7_approved_coins_short = self.config.live.approved_coins.short
        if "edit_bt_v7_ignored_coins_long" in st.session_state:
            st.session_state.edit_bt_v7_ignored_coins_long = self.config.live.ignored_coins.long
        if "edit_bt_v7_ignored_coins_short" in st.session_state:
            st.session_state.edit_bt_v7_ignored_coins_short = self.config.live.ignored_coins.short
        # Find coins with notices
        def _emit_notice(sym: str) -> bool:
            if sym in st.session_state.coindata_bybit.symbols_notices:
                st.warning(f'{sym}: {st.session_state.coindata_bybit.symbols_notices[sym]}')
                return True
            if sym in st.session_state.coindata_binance.symbols_notices:
                st.warning(f'{sym}: {st.session_state.coindata_binance.symbols_notices[sym]}')
                return True
            if sym in st.session_state.coindata_gateio.symbols_notices:
                st.warning(f'{sym}: {st.session_state.coindata_gateio.symbols_notices[sym]}')
                return True
            if sym in st.session_state.coindata_bitget.symbols_notices:
                st.warning(f'{sym}: {st.session_state.coindata_bitget.symbols_notices[sym]}')
                return True
            if "coindata_hyperliquid" in st.session_state and sym in st.session_state.coindata_hyperliquid.symbols_notices:
                st.warning(f'{sym}: {st.session_state.coindata_hyperliquid.symbols_notices[sym]}')
                return True
            return False

        for coin in list(set(self.config.live.approved_coins.long + self.config.live.approved_coins.short)):
            base = normalize_symbol(coin)
            raw_candidates = set(base_to_full.get(base, set()))
            if coin in symbols_raw:
                raw_candidates.add(coin)
            for sym in raw_candidates:
                if _emit_notice(sym):
                    break
        # Select approved coins
        col1, col2 = st.columns([1,1], vertical_alignment="bottom")
        with col1:
            st.multiselect('approved_coins_long', symbols, key="edit_bt_v7_approved_coins_long", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_long', symbols, key="edit_bt_v7_ignored_coins_long", help=pbgui_help.ignored_coins)
        with col2:
            st.multiselect('approved_coins_short', symbols, key="edit_bt_v7_approved_coins_short", help=pbgui_help.approved_coins)
            st.multiselect('ignored_symbols_short', symbols, key="edit_bt_v7_ignored_coins_short", help=pbgui_help.ignored_coins)

    @st.fragment
    # market_cap
    def fragment_market_cap(self):
        if "edit_bt_v7_market_cap" in st.session_state:
            if st.session_state.edit_bt_v7_market_cap != self.config.pbgui.market_cap:
                self.config.pbgui.market_cap = st.session_state.edit_bt_v7_market_cap
                st.session_state.coindata_binance.market_cap = self.config.pbgui.market_cap
                st.session_state.coindata_bybit.market_cap = self.config.pbgui.market_cap
                st.session_state.coindata_gateio.market_cap = self.config.pbgui.market_cap
                st.session_state.coindata_bitget.market_cap = self.config.pbgui.market_cap
                if "coindata_hyperliquid" in st.session_state:
                    st.session_state.coindata_hyperliquid.market_cap = self.config.pbgui.market_cap
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_bybit.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_binance.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_gateio.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_bitget.market_cap = self.config.pbgui.market_cap
            if "coindata_hyperliquid" in st.session_state:
                st.session_state.coindata_hyperliquid.market_cap = self.config.pbgui.market_cap
        st.number_input("market_cap", min_value=0, step=50, format="%.d", key="edit_bt_v7_market_cap", help=pbgui_help.market_cap)
    
    @st.fragment
    # vol_mcap
    def fragment_vol_mcap(self):
        if "edit_bt_v7_vol_mcap" in st.session_state:
            if st.session_state.edit_bt_v7_vol_mcap != self.config.pbgui.vol_mcap:
                self.config.pbgui.vol_mcap = st.session_state.edit_bt_v7_vol_mcap
                st.session_state.coindata_bybit.vol_mcap = self.config.pbgui.vol_mcap
                st.session_state.coindata_binance.vol_mcap = self.config.pbgui.vol_mcap
                st.session_state.coindata_gateio.vol_mcap = self.config.pbgui.vol_mcap
                st.session_state.coindata_bitget.vol_mcap = self.config.pbgui.vol_mcap
                if "coindata_hyperliquid" in st.session_state:
                    st.session_state.coindata_hyperliquid.vol_mcap = self.config.pbgui.vol_mcap
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_vol_mcap = round(float(self.config.pbgui.vol_mcap),2)
            st.session_state.coindata_bybit.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_binance.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_gateio.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_bitget.vol_mcap = self.config.pbgui.vol_mcap
            if "coindata_hyperliquid" in st.session_state:
                st.session_state.coindata_hyperliquid.vol_mcap = self.config.pbgui.vol_mcap
        st.number_input("vol/mcap", min_value=0.0, step=0.05, format="%.2f", key="edit_bt_v7_vol_mcap", help=pbgui_help.vol_mcap)

    @st.fragment
    # tags
    def fragment_tags(self):
        if "edit_bt_v7_tags" in st.session_state:
            if st.session_state.edit_bt_v7_tags != self.config.pbgui.tags:
                self.config.pbgui.tags = st.session_state.edit_bt_v7_tags
                st.session_state.coindata_bybit.tags = self.config.pbgui.tags
                st.session_state.coindata_binance.tags = self.config.pbgui.tags
                st.session_state.coindata_gateio.tags = self.config.pbgui.tags
                st.session_state.coindata_bitget.tags = self.config.pbgui.tags
                if "coindata_hyperliquid" in st.session_state:
                    st.session_state.coindata_hyperliquid.tags = self.config.pbgui.tags
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_tags = self.config.pbgui.tags
            st.session_state.coindata_bybit.tags = self.config.pbgui.tags
            st.session_state.coindata_binance.tags = self.config.pbgui.tags
            st.session_state.coindata_gateio.tags = self.config.pbgui.tags
            st.session_state.coindata_bitget.tags = self.config.pbgui.tags
            if "coindata_hyperliquid" in st.session_state:
                st.session_state.coindata_hyperliquid.tags = self.config.pbgui.tags
        # remove duplicates from tags and sort them
        hyperliquid_tags = []
        if "coindata_hyperliquid" in st.session_state:
            hyperliquid_tags = st.session_state.coindata_hyperliquid.all_tags
        tags = sorted(list(set(st.session_state.coindata_bybit.all_tags + st.session_state.coindata_binance.all_tags + st.session_state.coindata_gateio.all_tags + st.session_state.coindata_bitget.all_tags + hyperliquid_tags)))
        st.multiselect("tags", tags, key="edit_bt_v7_tags", help=pbgui_help.coindata_tags)

    # only_cpt
    @st.fragment
    def fragment_only_cpt(self):
        if "edit_bt_v7_only_cpt" in st.session_state:
            if st.session_state.edit_bt_v7_only_cpt != self.config.pbgui.only_cpt:
                self.config.pbgui.only_cpt = st.session_state.edit_bt_v7_only_cpt
                st.session_state.coindata_bybit.only_cpt = self.config.pbgui.only_cpt
                st.session_state.coindata_binance.only_cpt = self.config.pbgui.only_cpt
                st.session_state.coindata_bitget.only_cpt = self.config.pbgui.only_cpt
                if "coindata_hyperliquid" in st.session_state:
                    st.session_state.coindata_hyperliquid.only_cpt = self.config.pbgui.only_cpt
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_bybit.only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_binance.only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_bitget.only_cpt = self.config.pbgui.only_cpt
            if "coindata_hyperliquid" in st.session_state:
                st.session_state.coindata_hyperliquid.only_cpt = self.config.pbgui.only_cpt
        st.checkbox("only_cpt", key="edit_bt_v7_only_cpt", help=pbgui_help.only_cpt)
    
    # notices_ignore
    @st.fragment
    def fragment_notices_ignore(self):
        if "edit_bt_v7_notices_ignore" in st.session_state:
            if st.session_state.edit_bt_v7_notices_ignore != self.config.pbgui.notices_ignore:
                self.config.pbgui.notices_ignore = st.session_state.edit_bt_v7_notices_ignore
                st.session_state.coindata_bybit.notices_ignore = self.config.pbgui.notices_ignore
                st.session_state.coindata_binance.notices_ignore = self.config.pbgui.notices_ignore
                st.session_state.coindata_gateio.notices_ignore = self.config.pbgui.notices_ignore
                st.session_state.coindata_bitget.notices_ignore = self.config.pbgui.notices_ignore
                if "coindata_hyperliquid" in st.session_state:
                    st.session_state.coindata_hyperliquid.notices_ignore = self.config.pbgui.notices_ignore
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_bybit.notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_binance.notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_gateio.notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_bitget.notices_ignore = self.config.pbgui.notices_ignore
            if "coindata_hyperliquid" in st.session_state:
                st.session_state.coindata_hyperliquid.notices_ignore = self.config.pbgui.notices_ignore
        st.checkbox("notices_ignore", key="edit_bt_v7_notices_ignore", help=pbgui_help.notices_ignore)

    def edit(self):
        # Init coindata
        for exchange in V7.list():
            coindata_key = f"coindata_{exchange}"
            if coindata_key not in st.session_state:
                st.session_state[coindata_key] = CoinData()
                st.session_state[coindata_key].exchange = exchange
        # Display Editor
        col1, col2, col3, col4, col5, col6 = st.columns([1,1,0.5,0.5,0.5,0.5])
        with col1:
            self.fragment_exchanges()
        with col2:
            self.fragment_name()
        with col3:
            self.fragment_start_date()
        with col4:
            self.fragment_end_date()
        with col5:
            self.fragment_btc_collateral_cap()
        with col6:
            self.fragment_btc_collateral_ltv_cap()
        col1, col2, col3, col4, col5, col6 = st.columns([1,1,0.5,0.5,0.5,0.5])
        with col1:
            self.fragment_starting_balance()
        with col2:
            self.fragment_minimum_coin_age_days()
        with col3:
            self.fragment_gap_tolerance_ohlcvs_minutes()
        with col4:
            self.fragment_max_warmup_minutes()
        with col5:
            self.fragment_balance_sample_divider()
        with col6:
            self.fragment_logging()
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            self.fragment_ohlcv_source_dir()
        # Backtest Options
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_combine_ohlcvs()
        with col2:
            self.fragment_compress_cache()
        with col3:
            self.fragment_filter_by_min_effective_cost()
        with col4:
            self.fragment_maker_fee_override()
        # coin_sources (full width)
        self.fragment_coin_sources()
        # Suite (multi-scenario)
        self.fragment_suite()
        # PBGui Filter
        st.markdown("---")
        st.markdown("##### PBGui Filter")
        #Filters
        self.fragment_filter_coins()
        # coin_overrides
        self.config.view_coin_overrides()
        # Config
        self.config.bot.edit()

    def save(self):
        # Create the backtest directory if it does not exist
        preflight_errors = pb7_suite_preflight_errors(self.config.config)
        if preflight_errors:
            error_popup("\n\n".join(preflight_errors))
            return
        
        self.path = Path(f'{PBGDIR}/data/bt_v7/{self.name}')
        if not self.path.exists():
            self.path.mkdir(parents=True)
        # Copy the individual config file to the backtest path
        if self.config.coin_overrides:
            for coin in self.config.coin_overrides:
                override_config_path = self.config.coin_overrides[coin].get('override_config_path', False)
                if override_config_path:
                    # concate self.config.config_file and override_config_path
                    src = Path(Path(self.config.config_file).parent, override_config_path)
                    dest = Path(self.path, override_config_path)
                    # check if src exists
                    if src.exists():
                        # check if src and dest is not the same
                        if src != dest:
                            # remove dest if it exists
                            if dest.exists():
                                dest.unlink()
                            # copy config
                            shutil.copy(src, dest)
        self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
        self.config.config_file = Path(f'{self.path}/backtest.json')
        
        self.config.save_config()

    def save_queue(self, parameters : str = None):
        preflight_errors = pb7_suite_preflight_errors(self.config.config)
        if preflight_errors:
            error_popup("\n\n".join(preflight_errors))
            return

        dest = Path(f'{PBGDIR}/data/bt_v7_queue')
        unique_filename = str(uuid.uuid4())
        file = Path(f'{dest}/{unique_filename}.json') 
        bt_dict = {
            "name": self.name,
            "filename": unique_filename,
            "json": str(self.config.config_file),
            "exchange": self.config.backtest.exchanges,
        }
        if not dest.exists():
            dest.mkdir(parents=True)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(bt_dict, f, indent=4)

    def remove(self):
        path = Path(self.path).parent
        rmtree(path, ignore_errors=True)

    def _clear_config_widget_session_state_after_import(self) -> None:
        """Clear Streamlit widget state so imported configs are reflected in UI.

        Copy/paste import happens without rebuilding the ConfigV7 object from disk, so any
        existing widget keys (especially suite_* keys) will override the new defaults.
        """

        # Clear all backtest editor keys
        for k in list(st.session_state.keys()):
            if k.startswith("edit_bt_v7_"):
                st.session_state.pop(k, None)

        # Clear bot editor keys
        for k in list(st.session_state.keys()):
            if k.startswith("edit_configv7_"):
                st.session_state.pop(k, None)

        # Clear suite/scenario editor keys (these otherwise force old values)
        for k in list(st.session_state.keys()):
            if (
                k.startswith("suite_")
                or k.startswith("bt_suite_")
                or k.startswith("bt_edit_suite_agg_")
                or k.startswith("select_scenarios_")
                or k.startswith("bt_select_scenarios_")
                or k.startswith("select_aggregates_")
                or k.startswith("bt_select_aggregates_")
                or k.startswith("edit_scenario_")
                or k.startswith("bt_edit_scenario_")
                or k.startswith("add_scenario_")
                or k.startswith("scenario_")
            ):
                if k == "bt_suite_key_ver":
                    continue
                st.session_state.pop(k, None)

        # Clear coin_sources editor keys (ConfigV7Editor uses bt_ prefix)
        for k in list(st.session_state.keys()):
            if (
                k.startswith("bt_coin_sources_")
                or k.startswith("bt_new_coin_source_")
                or k.startswith("bt_add_new_coin_source")
                or k == "bt_coin_sources_table"
            ):
                st.session_state.pop(k, None)

    @st.dialog("Paste config", width="large")
    def import_instance(self):
        # Keep the raw text stable while the user is editing/pasting.
        if "import_backtest_v7_config" not in st.session_state:
            st.session_state.import_backtest_v7_config = json.dumps(self.config.config, indent=4)

        st.text_area(
            "config",
            key="import_backtest_v7_config",
            height=500,
            help="Paste full JSON config here",
        )

        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("OK"):
                try:
                    raw_txt = str(st.session_state.get("import_backtest_v7_config") or "")
                    parsed = json.loads(raw_txt)
                except Exception:
                    st.error('Invalid JSON (use true/false/null, not True/False/None)', icon="⚠️")
                    return

                # Apply config via setter (ensures backtest/suite/pbgui are parsed into objects)
                self.config.config = parsed

                # Reset widget state so UI reflects the imported config values.
                # IMPORTANT: don't wipe bt_suite_key_ver.
                self._clear_config_widget_session_state_after_import()

                # Force fresh suite widget keys after import so stale frontend widget state
                # cannot override the imported config (Enable Suite etc.).
                st.session_state["bt_suite_key_ver"] = int(st.session_state.get("bt_suite_key_ver", 0) or 0) + 1
                ver = int(st.session_state.get("bt_suite_key_ver", 0) or 0)

                # Seed suite widgets for this new version from the imported config.
                try:
                    suite_obj = self.config.backtest.suite
                    st.session_state[f"bt_suite_enabled_{ver}"] = bool(getattr(suite_obj, "enabled", False))
                    st.session_state[f"bt_suite_include_base_{ver}"] = bool(getattr(suite_obj, "include_base_scenario", True))
                    st.session_state[f"bt_suite_base_label_{ver}"] = str(getattr(suite_obj, "base_label", "base") or "base")
                    st.session_state[f"bt_suite_aggregate_default_{ver}"] = str(getattr(getattr(suite_obj, "aggregate", {}) or {}, "get", lambda *_: "mean")("default", "mean"))
                except Exception:
                    pass
                st.rerun()
        with col2:
            if st.button("Cancel"):
                # Restore the textarea to current config for next open
                st.session_state.import_backtest_v7_config = json.dumps(self.config.config, indent=4)
                st.rerun()

class BacktestV7Result:
    def __init__(self, result_path: str = None):
        self.result_path = result_path
        self.initialize()
    
    def initialize(self):
        self.time = None
        self.result = self.load_result()
        self.config = ConfigV7(PurePath(f'{self.result_path}/config.json'))
        self.config.load_config()
        self.backtest_config = self.load_backtest_config()
        self.ed = self.config.backtest.end_date
        # Support both old and new JSON formats
        # New format has _usd/_btc suffixes, old format has no suffix
        if self.result:
            self.adg = self.result.get("adg_usd") or self.result.get("adg", 0)
            self.drawdown_worst = self.result.get("drawdown_worst_usd") or self.result.get("drawdown_worst", 0)
            self.sharpe_ratio = self.result.get("sharpe_ratio_usd") or self.result.get("sharpe_ratio", 0)
            # safe read: prefer numeric value, fallback to None if missing/invalid
            self.equity_balance_diff_neg_max = None
            val = self.result.get("equity_balance_diff_neg_max_usd") or self.result.get("equity_balance_diff_neg_max")
            if val not in (None, ""):
                try:
                    self.equity_balance_diff_neg_max = float(val)
                except (TypeError, ValueError):
                    # keep None on conversion failure
                    self.equity_balance_diff_neg_max = None
        else:
            self.adg = 0
            self.drawdown_worst = 0
            self.sharpe_ratio = 0
            self.equity_balance_diff_neg_max = None
        self.starting_balance = self.config.backtest.starting_balance
        self.be = None
        self.final_balance, self.final_balance_btc = self.load_final_balance()
        self.fills = None
        # If the analysis indicates liquidation, reflect that in the final balances
        try:
            if self.is_liquidated():
                self.final_balance = 0
                self.final_balance_btc = 0
        except Exception:
            pass

    def is_liquidated(self) -> bool:
        """Return True if the backtest ended in liquidation.

        The analysis writes `equity_balance_diff_neg_max` as the maximum negative
        relative difference between equity and balance. If this value is >= 1.0
        the account reached zero (or worse) and can be considered liquidated.
        """
        try:
            if self.equity_balance_diff_neg_max is None:
                return False
            return float(self.equity_balance_diff_neg_max) >= 1.0
        except Exception:
            return False
    
    def remove(self):
        rmtree(self.result_path)

    def load_result(self):
        r = Path(f'{self.result_path}/analysis.json')
        try:
            self.time = datetime.datetime.fromtimestamp(r.stat().st_mtime)
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

    def load_final_balance(self):
        balance = Path(f'{self.result_path}/balance_and_equity.csv')
        balance_gz = Path(f'{self.result_path}/balance_and_equity.csv.gz')
        
        file_path = balance if balance.exists() else (balance_gz if balance_gz.exists() else None)
        if not file_path:
            return 0, 0
        
        try:
            import gzip
            open_func = (lambda p: gzip.open(p, 'rt', encoding='utf-8')) if str(file_path).endswith('.gz') else (lambda p: open(p, 'r', encoding='utf-8'))
            
            with open_func(file_path) as f:
                header = f.readline().strip().split(',')
                
                # Seek to end and read backwards to find last line
                if not str(file_path).endswith('.gz'):
                    f.seek(0, 2)
                    end_of_file = f.tell()
                    for num in range(1, min(end_of_file + 1, 10000)):
                        f.seek(max(0, end_of_file - num))
                        lines = f.readlines()
                        if len(lines) >= 2:
                            last_line = lines[-1].strip().split(',')
                            break
                else:
                    # For .gz files, just read all lines
                    last_line = None
                    for line in f:
                        last_line = line.strip().split(',')
                
                if not last_line:
                    return 0, 0
                
                # New format: columns like "usd_total_balance"
                if 'usd_total_balance' in header:
                    idx = header.index('usd_total_balance')
                    btc_idx = header.index('btc_total_balance') if 'btc_total_balance' in header else None
                    return (last_line[idx] if idx < len(last_line) else 0,
                            last_line[btc_idx] if btc_idx and btc_idx < len(last_line) else None)
                
                # Old format: 3 or 5 columns
                if len(header) in (3, 5):
                    return (last_line[1] if len(last_line) > 1 else 0,
                            last_line[3] if len(header) == 5 and len(last_line) > 3 else None)
                
        except Exception:
            pass
        
        return 0, 0

    def load_be(self):
        print(f"Loading balance and equity data for {self.result_path}")
        if self.be is None:
            print("Balance and equity data not loaded, loading now...")
            # Try both .csv and .csv.gz
            be = f'{self.result_path}/balance_and_equity.csv'
            be_gz = f'{self.result_path}/balance_and_equity.csv.gz'
            
            file_path = None
            compression = None
            
            if Path(be).exists():
                print("Balance and equity file exists, reading...") 
                file_path = be
                compression = None
            elif Path(be_gz).exists():
                print("Balance and equity .gz file exists, reading...")
                file_path = be_gz
                compression = 'gzip'
            
            if file_path:
                self.be = pd.read_csv(file_path, compression=compression, index_col=0)
                
                # Check if new format (timestamps) or old format (minutes)
                # New format: index is timestamp string like "2020-01-03 19:00:00"
                # Old format: index is numeric (minutes)
                try:
                    # Try to convert index to datetime (new format)
                    self.be.index = pd.to_datetime(self.be.index)
                    # New format: columns are usd_total_balance, usd_total_equity, etc.
                    if 'usd_total_balance' in self.be.columns and 'usd_total_equity' in self.be.columns:
                        self.be['balance'] = self.be['usd_total_balance']
                        self.be['equity'] = self.be['usd_total_equity']
                    if 'btc_total_balance' in self.be.columns and 'btc_total_equity' in self.be.columns:
                        self.be['balance_btc'] = self.be['btc_total_balance']
                        self.be['equity_btc'] = self.be['btc_total_equity']
                    self.be['time'] = self.be.index
                except (ValueError, TypeError):
                    # Old format: index is minutes (numeric)
                    print("Using old format (minutes-based index)")
                    timestamp = datetime.datetime.strptime(self.ed, '%Y-%m-%d').timestamp()
                    start_time = timestamp - (float(self.be.index[-1]) * 60)
                    self.be['time'] = datetime.datetime.fromtimestamp(start_time) + pd.to_timedelta(self.be.index, unit='m')

    def load_fills(self):
        if self.fills is None:
            # Try both .csv and .csv.gz
            fills = f'{self.result_path}/fills.csv'
            fills_gz = f'{self.result_path}/fills.csv.gz'
            
            if Path(fills).exists():
                self.fills = pd.read_csv(fills)
                timestamp = datetime.datetime.strptime(self.ed, '%Y-%m-%d').timestamp()
                start_time = timestamp - (self.fills['minute'].iloc[-1] * 60)
                self.fills['time'] = datetime.datetime.fromtimestamp(start_time) + pd.to_timedelta(self.fills['minute'], unit='m')
            elif Path(fills_gz).exists():
                self.fills = pd.read_csv(fills_gz, compression='gzip')
                timestamp = datetime.datetime.strptime(self.ed, '%Y-%m-%d').timestamp()
                start_time = timestamp - (self.fills['minute'].iloc[-1] * 60)
                self.fills['time'] = datetime.datetime.fromtimestamp(start_time) + pd.to_timedelta(self.fills['minute'], unit='m')

    def view_plot(self):
        # Note: liquidation banner is displayed once above `view_chart_be`
        balance_and_equity = Path(f'{self.result_path}/balance_and_equity.png')
        balance_and_equity_btc = Path(f'{self.result_path}/balance_and_equity_btc.png')
        if balance_and_equity.exists():
            st.image(str(balance_and_equity), width="stretch")
        else:
            st.warning("No balance and equity plot found")
        if balance_and_equity_btc.exists():
            st.image(str(balance_and_equity_btc), width="stretch")

    def view_fills(self):
        # Fills are always shown; liquidation banner shown above charts
        p = str(Path(f'{self.result_path}/fills_plots/*.png'))
        fills = glob.glob(p)
        if fills:
            for fill in fills:
                st.image(fill, width="stretch")
        else:
            st.warning("No fills plot found")

    def view(self):
        # Overview: show result/config; liquidation banner is shown above charts
        col1, col2 = st.columns([1,1])
        with col1:
            st.code(json.dumps(self.result, indent=4))
        with col2:
            st.code(config_pretty_str(self.config.config))

    # Create Chart with plotly
    def view_chart_be(self):
        if self.be is not None:
            if self.is_liquidated():
                st.error(f"Backtest ended in liquidation (equity_balance_diff_neg_max={self.equity_balance_diff_neg_max})")
            col1, col2 = st.columns([1,9], vertical_alignment="bottom")
            with col1:
                st.checkbox("logarithmic", key=f"backtest_v7_{self.result_path}_be_log")
            fig = go.Figure()
            if st.session_state[f"backtest_v7_{self.result_path}_be_log"]:
                fig.update_layout(yaxis_type="log")
            else:
                fig.update_layout(yaxis_type="linear")
            fig.add_trace(go.Scatter(x=self.be['time'], y=self.be['equity'], name="equity", line=dict(width=0.75)))
            fig.add_trace(go.Scatter(x=self.be['time'], y=self.be['balance'], name="balance", line=dict(width=2.5)))
            fig.update_layout(yaxis_title='Balance', height=800)
            fig.update_xaxes(showgrid=True, griddash="dot")
            name = PurePath(*self.result_path.parts[-3:-2])
            formatted_time = self.time.strftime("%Y-%m-%d %H:%M:%S")
            fig.update_layout(title_text=f'{name} {formatted_time}', title_x=0.5)
            st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_be")
        else:
            st.error("No balance and equity data found")

    # Create be_btc Chart with plotly
    def view_chart_be_btc(self):
        if self.be is not None:
            # liquidation banner displayed in `view_chart_be`
            col1, col2 = st.columns([1,9], vertical_alignment="bottom")
            with col1:
                st.checkbox("logarithmic", key=f"backtest_v7_{self.result_path}_be_btc_log")
            fig = go.Figure()
            if st.session_state[f"backtest_v7_{self.result_path}_be_btc_log"]:
                fig.update_layout(yaxis_type="log")
            else:
                fig.update_layout(yaxis_type="linear")
            fig.add_trace(go.Scatter(x=self.be['time'], y=self.be['equity_btc'], name="equity_btc", line=dict(width=0.75)))
            fig.add_trace(go.Scatter(x=self.be['time'], y=self.be['balance_btc'], name="balance_btc", line=dict(width=2.5)))
            fig.update_layout(yaxis_title='Balance', height=800)
            fig.update_xaxes(showgrid=True, griddash="dot")
            name = PurePath(*self.result_path.parts[-3:-2])
            formatted_time = self.time.strftime("%Y-%m-%d %H:%M:%S")
            fig.update_layout(title_text=f'{name} {formatted_time}', title_x=0.5)
            st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_be_btc")
        else:
            st.error("No balance and equity data found")


    # Create Drawdown Chart with plotly
    def view_chart_drawdown(self):
        if self.be is not None:
            # liquidation banner displayed in `view_chart_be`
            fig = go.Figure()

            equity = self.be['equity']
            # Calculate the drawdown: normalized equity from 1 to 0
            max_equity = equity.cummax()
            drawdown =  (equity - max_equity) / max_equity
            normalized_drawdown = 1 + drawdown  # To get values from 1 down to 0

            # Plot Drawdown
            fig.add_trace(go.Scatter(
                x=self.be['time'],
                y=normalized_drawdown,
                name='Drawdown',
                line=dict(width=1.5)
            ))

            fig.update_layout(yaxis_title='Drawdown', height=800)
            fig.update_xaxes(showgrid=True, griddash="dot")
            name = PurePath(*self.result_path.parts[-3:-2])
            formatted_time = self.time.strftime("%Y-%m-%d %H:%M:%S")
            fig.update_layout(title_text=f'{name} {formatted_time}', title_x=0.5)
            fig['data'][0]['showlegend'] = True
            st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_drawdown")
        else:
            st.error("No balance and equity data found")

    # Create Drawdown Chart with plotly
    def view_chart_drawdown_btc(self):
        if self.be is not None:
            # liquidation banner displayed in `view_chart_be`
            fig = go.Figure()

            equity = self.be['equity_btc']
            # Calculate the drawdown: normalized equity from 1 to 0
            max_equity = equity.cummax()
            drawdown =  (equity - max_equity) / max_equity
            normalized_drawdown = 1 + drawdown  # To get values from 1 down to 0

            # Plot Drawdown
            fig.add_trace(go.Scatter(
                x=self.be['time'],
                y=normalized_drawdown,
                name='Drawdown BTC',
                line=dict(width=1.5)
            ))

            fig.update_layout(yaxis_title='Drawdown BTC', height=800)
            fig.update_xaxes(showgrid=True, griddash="dot")
            name = PurePath(*self.result_path.parts[-3:-2])
            formatted_time = self.time.strftime("%Y-%m-%d %H:%M:%S")
            fig.update_layout(title_text=f'{name} {formatted_time}', title_x=0.5)
            fig['data'][0]['showlegend'] = True
            st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_drawdown_btc")
        else:
            st.error("No balance and equity data found")

    # Create Symbol Chart with plotly
    def view_chart_symbol(self):
        if self.fills is not None:
            col1, col2 = st.columns([1,9], vertical_alignment="bottom")
            with col1:
                st.checkbox("logarithmic", key=f"backtest_v7_{self.result_path}_symbol_log")
            fig = go.Figure()
            if st.session_state[f"backtest_v7_{self.result_path}_symbol_log"]:
                fig.update_layout(yaxis_type="log")
            else:
                fig.update_layout(yaxis_type="linear")
            if "symbol" in self.fills:
                coin_or_symbol = "symbol"
            elif "coin" in self.fills:
                coin_or_symbol = "coin"
            for symbol in self.fills[coin_or_symbol].unique():
                symbol_df = self.fills[self.fills[coin_or_symbol] == symbol].copy()
                symbol_df["sym_pnl"] = symbol_df["pnl"].cumsum()
                fig.add_trace(go.Scatter(x=symbol_df['time'], y=symbol_df['sym_pnl'], name=symbol))
            fig.update_layout(yaxis_title='PnL', height=800, )
            fig['data'][0]['showlegend'] = True
            fig.update_xaxes(showgrid=True, griddash="dot")
            st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_symbols")
        else:
            st.error("No fills data found")

    def view_chart_twe(self):
        if self.fills is not None:
            col1, col2 = st.columns([1,9], vertical_alignment="bottom")
            with col1:
                #Add selectbot for resolution
                st.selectbox("resolution in minutes", [1440, 720, 240, 60, 30, 15, 10, 5, 2, 1], key=f"backtest_v7_{self.result_path}_resolution", help=pbgui_help.backtest_twe_resolution)
            # Add a spinner
            with st.spinner("Calculating WE and TWE", show_time=True):
                fig = go.Figure()

                # Determine whether to use 'symbol' or 'coin'
                coin_or_symbol = "symbol" if "symbol" in self.fills else "coin"
                
                # Find the balance column (try different names for compatibility)
                balance_col = None
                for col_name in ["balance", "usd_total_balance"]:
                    if col_name in self.fills.columns:
                        balance_col = col_name
                        break
                
                if balance_col:
                    self.fills['we'] = 1 / self.fills[balance_col] * self.fills['psize'] * self.fills['pprice']
                else:
                    st.error(f"Balance column not found. Available columns: {list(self.fills.columns)}")
                    return

                #write self.fills to csv
                # self.fills.to_csv(f'{self.result_path}/fills_with_we.csv', index=False)

                # Create pivot table
                # Create separate pivot tables for long and short positions based on 'type' column
                if 'type' in self.fills.columns:
                    long_fills = self.fills[self.fills['type'].str.endswith('long')]
                    short_fills = self.fills[self.fills['type'].str.endswith('short')]
                    exposure_by_currency_long = long_fills.pivot_table(
                        index='time',
                        columns=coin_or_symbol,
                        values='we',
                        aggfunc='last'
                    ).ffill()
                    exposure_by_currency_short = short_fills.pivot_table(
                        index='time',
                        columns=coin_or_symbol,
                        values='we',
                        aggfunc='last'
                    ).ffill()
                else:
                    exposure_by_currency_long = pd.DataFrame()
                    exposure_by_currency_short = pd.DataFrame()

                #write exposure_by_currency to csv
                # exposure_by_currency.to_csv(f'{self.result_path}/exposure_by_currency_pivot.csv')

                # Calculate total exposure and fill missing values with last value
                exposure_by_currency_long['twe'] = exposure_by_currency_long.sum(axis=1).ffill()
                exposure_by_currency_short['twe'] = exposure_by_currency_short.sum(axis=1).ffill()

                #write exposure_by_currency to csv
                # exposure_by_currency.to_csv(f'{self.result_path}/exposure_by_currency.csv')

                # Fill missing time slots with a custom function
                resolution = st.session_state[f"backtest_v7_{self.result_path}_resolution"]
                exposure_by_currency_long = exposure_by_currency_long.resample(f'{resolution}min').max().ffill().fillna(0)
                exposure_by_currency_short = exposure_by_currency_short.resample(f'{resolution}min').max().ffill().fillna(0)

                # Plot total exposure
                fig.add_trace(go.Scatter(x=exposure_by_currency_long.index, y=exposure_by_currency_long['twe'], name="Long TWE"))
                fig.add_trace(go.Scatter(x=exposure_by_currency_short.index, y=exposure_by_currency_short['twe'], name="Short TWE"))

                # Plot each coin's exposure
                for coin in exposure_by_currency_long.columns[:-1]:  # Exclude 'twe' column
                    fig.add_trace(go.Scatter(x=exposure_by_currency_long.index, y=exposure_by_currency_long[coin], name=f"{coin} Long WE"))
                for coin in exposure_by_currency_short.columns[:-1]:  # Exclude 'twe' column
                    fig.add_trace(go.Scatter(x=exposure_by_currency_short.index, y=exposure_by_currency_short[coin], name=f"{coin} Short WE"))

                fig.update_layout(yaxis_title='Exposure', height=800)
                fig.update_xaxes(showgrid=True, griddash="dot")
                st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_we")
        else:
            st.error("No fills data found")

class ConfigV7Archives:
    def __init__(self):
        self.archives = []
        self.my_archive = ""
        self.my_archive_username = ""
        self.my_archive_email = ""
        self.my_archive_path = "pbgui/configs/pb7"
        self.my_archive_access_token = ""
        self.load()
        self.load_config()

    def load(self):
        p = str(Path(f'{PBGDIR}/data/archives/*/.git/config'))
        files = glob.glob(p, recursive=True)
        self.archives = []
        for file in files:
            git_config = configparser.ConfigParser()
            git_config.read(file)
            if git_config.has_section('remote "origin"'):
                if git_config.has_option('remote "origin"', 'url'):
                    remote_url = git_config.get('remote "origin"', 'url')
                    if remote_url:
                        self.archives.append({
                            "url": remote_url,
                            "name": PurePath(file).parent.parent.name,
                            "path": PurePath(file).parent.parent
                        })

    def setup(self):
        if not self.archives:
            st.warning("No archives found\n Please add your own github archive.")
            return
        # Init session states for keys
        if "edit_my_archive" in st.session_state:
            if st.session_state.edit_my_archive != self.my_archive:
                self.my_archive = st.session_state.edit_my_archive
        else:
            st.session_state.edit_my_archive = self.my_archive
        if "edit_my_archive_path" in st.session_state:
            if st.session_state.edit_my_archive_path != self.my_archive_path:
                self.my_archive_path = st.session_state.edit_my_archive_path
        else:
            st.session_state.edit_my_archive_path = self.my_archive_path
        if "edit_my_archive_username" in st.session_state:
            if st.session_state.edit_my_archive_username != self.my_archive_username:
                self.my_archive_username = st.session_state.edit_my_archive_username
        else:
            st.session_state.edit_my_archive_username = self.my_archive_username
        if "edit_my_archive_email" in st.session_state:
            if st.session_state.edit_my_archive_email != self.my_archive_email:
                self.my_archive_email = st.session_state.edit_my_archive_email
        else:
            st.session_state.edit_my_archive_email = self.my_archive_email
        if "edit_my_archive_access_token" in st.session_state:
            if st.session_state.edit_my_archive_access_token != self.my_archive_access_token:
                self.my_archive_access_token = st.session_state.edit_my_archive_access_token
        else:
            st.session_state.edit_my_archive_access_token = self.my_archive_access_token
        # Display Editor
        col1, col2 = st.columns([1,1])
        with col1:
            options = [""] + [archive["name"] for archive in self.archives]        
            st.selectbox("Select your own Archive", options=options, key="edit_my_archive", help=pbgui_help.my_archive)
        with col2:
            st.text_input("Archive Path", value=self.my_archive_path, key="edit_my_archive_path", help=pbgui_help.my_archive_path)
        # Archive Username and Email
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_input("Archive Username", value=self.my_archive_username, key="edit_my_archive_username", help=pbgui_help.my_archive_username)
        with col2:
            st.text_input("Archive Email", value=self.my_archive_email, key="edit_my_archive_email", help=pbgui_help.my_archive_email)
        # Archive Token
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_input("Archive Access Token", value=self.my_archive_access_token, type="password", key="edit_my_archive_access_token", help=pbgui_help.my_archive_access_token)
        if st.button("Test"):
            self.git_push_test()

    def load_config(self):
        self.my_archive = load_ini("config_archive", "my_archive")
        self.my_archive_path = load_ini("config_archive", "my_archive_path")
        self.my_archive_username = load_ini("config_archive", "my_archive_username")
        self.my_archive_email = load_ini("config_archive", "my_archive_email")
        self.my_archive_access_token = load_ini("config_archive", "my_archive_access_token")

    def save_config(self):
        save_ini("config_archive", "my_archive", self.my_archive)
        save_ini("config_archive", "my_archive_path", self.my_archive_path)
        save_ini("config_archive", "my_archive_username", self.my_archive_username)
        save_ini("config_archive", "my_archive_email", self.my_archive_email)
        save_ini("config_archive", "my_archive_access_token", self.my_archive_access_token)

    def list(self):
        if not self.archives:
            st.warning("No archives found")
            return
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_config_v7_archives_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_config_v7_archives_{ed_key}']
            for row in ed["edited_rows"]:
                if 'view' in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]['view']:
                        results =  BacktestV7Results()
                        results.results_path = self.archives[row]['path']
                        results.name = self.archives[row]['name']
                        st.session_state.config_v7_config_archive = results
                        st.session_state.ed_key += 1
                        st.rerun()
                if 'delete' in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]['delete']:
                        self.remove_archive(self.archives[row]['path'])
                        self.load()
                        st.session_state.ed_key += 1
                        st.rerun()
        data = [
            {
                "view": False,
                "Name": archive["name"],
                "URL": archive["url"],
                "Path": str(archive["path"]),
                "delete": False,
            }
            for archive in self.archives
        ]
        column_config={
            "view": st.column_config.CheckboxColumn(label="View Archive"),
            "Name": st.column_config.TextColumn(label="Archive Name"),
            "URL": st.column_config.LinkColumn(label="Repository URL"),
            "Path": st.column_config.TextColumn(label="Local Path"),
            "delete": st.column_config.CheckboxColumn(label="Delete Archive"),
        }
        # Display Config Archives
        st.data_editor(
            data,
            column_config=column_config,
            key=f"select_config_v7_archives_{ed_key}",
            hide_index=True
        )
    
    def remove_archive(self, path: str):
        if path:
            rmtree(path, ignore_errors=True)

    def add_config(self, path: str):
        if path:
            if Path(path).exists():
                # create dest_name from path
                dest_name = str(path).split("/pbgui/")[-1]
                if self.my_archive_path:
                    dest = Path(f'{PBGDIR}/data/archives/{self.my_archive}/{self.my_archive_path}/{dest_name}')
                else:
                    dest = Path(f'{PBGDIR}/data/archives/{self.my_archive}/{dest_name}')
                # copy path to dest
                copytree(path, dest, dirs_exist_ok=True)


    def add(self):
        if "edit_bt_v7_archive_name" not in st.session_state:
            st.session_state.edit_bt_v7_archive_name = ""
        if "edit_bt_v7_archive_url" not in st.session_state:
            st.session_state.edit_bt_v7_archive_url = ""
        col1, col2 = st.columns([1,1])
        with col1:
            st.text_input("Name", value=st.session_state.edit_bt_v7_archive_name, key="edit_bt_v7_archive_name", help=pbgui_help.archive_name)
        with col2:
            st.text_input("URL", value=st.session_state.edit_bt_v7_archive_url, key="edit_bt_v7_archive_url", help=pbgui_help.archive_url)
        if st.button("Add Archive"):
            self.git_add(st.session_state.edit_bt_v7_archive_name, st.session_state.edit_bt_v7_archive_url)
            self.load()

    def git_add(self, archive_name: str, url: str):
        if not archive_name or not url:
            st.error("Please enter a name and URL")
            return
        cmd = ["git", "clone", url, f"{PBGDIR}/data/archives/{archive_name}"]
        try:
            log = ""
            result = subprocess.run(cmd, capture_output=True, check=True, text=True)
            log = result.stdout + "\n"
            if result.stderr:
                log = log + result.stderr + "\n"
        except subprocess.CalledProcessError as e:
            log = f"Error adding {archive_name}: {e.stderr}"
        if log:
            info_popup(log)
    
    def git_pull(self):
        log = ""
        for archive in self.archives:
            path = archive["path"]
            cmd = ["git", "-C", path, "pull"]
            log = log + f'Pulling {archive["name"]}...' + "\n\n"
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
                log = log + result.stdout + "\n"
                if result.stderr:
                    log = log + result.stderr + "\n"
            except subprocess.CalledProcessError as e:
                log = log + f"Error pulling {archive['name']}: {e.stderr}"
        if log:
            info_popup(log)
    
    def git_push_test(self):
        if self.my_archive_access_token and self.my_archive:
            archive = next((a for a in self.archives if a["name"] == self.my_archive), None)
            if archive:
                path = archive["path"]
                url = archive["url"]
                # add token to url
                if url.startswith("http://"):
                    url = url.replace("http://", f"http://{self.my_archive_access_token}@")
                elif url.startswith("https://"):
                    url = url.replace("https://", f"https://{self.my_archive_access_token}@")
            else:
                st.error(f"Archive '{self.my_archive}' not found.")
                return
            # Configure username and email
            cmd = ["git", "-C", path, "config", "user.name", self.my_archive_username]
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
            except subprocess.CalledProcessError as e:
                error_popup(f"Error configuring username for {self.my_archive}: {e.stderr}")
                return
            cmd = ["git", "-C", path, "config", "user.email", self.my_archive_email]
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
            except subprocess.CalledProcessError as e:
                error_popup(f"Error configuring email for {self.my_archive}: {e.stderr}")
                return
            # Test push
            cmd = ["git", "-C", path, "push", url, "--dry-run"]
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
                log = result.stdout + "\n" + result.stderr
                if result.returncode == 0:
                    info_popup(log)
                else:
                    error_popup(log)
            except subprocess.CalledProcessError as e:
                error_popup(f"Error pushing to {self.my_archive}: {e.stderr}")
        else:
            st.error("Please enter a name, user and access token")

    def git_push(self):
        if self.my_archive_access_token and self.my_archive:
            archive = next((a for a in self.archives if a["name"] == self.my_archive), None)
            if archive:
                path = archive["path"]
                url = archive["url"]
                # add token to url
                if url.startswith("http://"):
                    url = url.replace("http://", f"http://{self.my_archive_access_token}@")
                elif url.startswith("https://"):
                    url = url.replace("https://", f"https://{self.my_archive_access_token}@")
            else:
                st.error(f"Archive '{self.my_archive}' not found.")
                return

            # Init emtpy log
            log = ""

            # git pull before push
            cmd = ["git", "-C", path, "pull"]
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
                log = log + f"Git pull changes\n"
                log = log + result.stdout + "\n"
                if result.stderr:
                    log = log + result.stderr + "\n"
            except subprocess.CalledProcessError as e:
                error_popup(f"Error pulling {self.my_archive}: {e.stderr}")
                return
           
            # add all files to git
            cmd = ["git", "-C", path, "add", "-A"]
            log = ""
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
                log = "Git add all files to archive\n"
                log = log + result.stdout + "\n"
                if result.stderr:
                    log = log + result.stderr + "\n"
            except subprocess.CalledProcessError as e:
                error_popup(f"Error adding files to {self.my_archive}: {e.stderr}")
                return

            # commit changes
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cmd = ["git", "-C", path, "commit", "-m", f"Update {self.my_archive} at {current_time}"]
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
                log = log + "Git commit changes\n"
                log = log + result.stdout + "\n"
                if result.stderr:
                    log = log + result.stderr + "\n"
            except subprocess.CalledProcessError as e:
                error_popup(f"Error committing to {self.my_archive}: {e.stderr}")
                return

            # push changes
            cmd = ["git", "-C", path, "push", url]
            try:
                result = subprocess.run(cmd, capture_output=True, check=True, text=True)
                log = log + "Git push changes\n"
                log = log + result.stdout + "\n"
                if result.stderr:
                    log = log + result.stderr + "\n"
            except subprocess.CalledProcessError as e:
                error_popup(f"Error pushing to {self.my_archive}: {e.stderr}")
                return
            if log:
                info_popup(log)

class BacktestV7Results:

    def __init__(self):
        self.results = []
        self.results_d = []
        self.sort_results = "Result Time"
        self.sort_results_order = True
        self.load_sort_results()
        self.filter = ""
        self.results_path = None
        self.name = None
        if "btv7_compare_results" not in st.session_state:
            st.session_state.btv7_compare_results = []

    def calculate_results(self):
        p = str(Path(f'{self.results_path}/**/analysis.json'))
        files = glob.glob(p, recursive=True)
        return len(files)

    def load(self):       
        p = str(Path(f'{self.results_path}/**/analysis.json'))
        files = glob.glob(p, recursive=True)
        self.results = []
        for file in files:
            result_path = PurePath(file).parent
            bt_result = BacktestV7Result(result_path)
            self.results.append(bt_result)

    def view(self):
        if "select_btv7_result_filter" in st.session_state:
            if st.session_state.select_btv7_result_filter != self.filter:
                self.filter = st.session_state.select_btv7_result_filter
                self.results_d = []
                self.results = []
                self.load()
        else:
            st.session_state.select_btv7_result_filter = self.filter
            
        # Remove results by filter
        if not self.filter == "":
            for result in self.results.copy():
                # remove archive_path from result_path
                result_path = str(result.result_path)
                if result_path.startswith(f'{PBGDIR}/data/archives/'):
                    # remove archives path
                    result_path = result_path.replace(f'{PBGDIR}/data/archives/', '')
                    result_path = result_path.split('/')
                    result_path = '/'.join(result_path[1:])
                else:
                    # remove backtests path
                    result_path = result_path.replace(f'{pb7dir()}/backtests/pbgui/', '')
                # target = result.config.backtest.base_dir.split('/')[-1]
                if not fnmatch.fnmatch(result_path.lower(), self.filter.lower()):
                    self.results.remove(result)

        st.text_input("Filter by Backtest Name", value="", help=pbgui_help.smart_filter, key="select_btv7_result_filter")
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if not self.results_d:
            for id, result in enumerate(self.results):
                compare = False
                if st.session_state.btv7_compare_results:
                    for r in st.session_state.btv7_compare_results:
                        if r.result_path == result.result_path:
                            compare = True
                
                starting_balance_float = float(result.starting_balance)
                final_balance_float = float(result.final_balance)
                # Support both old and new JSON formats for gain
                if result.result:
                    gain = result.result.get("gain_usd") or result.result.get("gain", 0)
                else:
                    gain = 0
                # remove archive_path from result_path
                result_path = str(result.result_path)
                if result_path.startswith(f'{PBGDIR}/data/archives/'):
                    # remove archives path
                    result_path = result_path.replace(f'{PBGDIR}/data/archives/', '')
                    result_path = result_path.split('/')
                    result_path = '/'.join(result_path[1:])
                else:
                    # remove backtests path
                    result_path = result_path.replace(f'{pb7dir()}/backtests/pbgui/', '')
                self.results_d.append({
                    'Select': False,
                    'id': id,
                    'View': False,
                    'WE': False,
                    'Plot': False,
                    'Fills': False,
                    'Backtest Name': result_path,
                    'Exch.': result.config.backtest.exchanges,
                    'Result Time': result.time,
                    'ADG': float(f"{result.adg:.4f}"),
                    'Gain': float(f"{gain:.2f}"),
                    'Drawdown Worst': float(f"{result.drawdown_worst:.4f}"),
                    'Sharpe Ratio': float(f"{result.sharpe_ratio:.4f}"),
                    'Starting Balance': float(f"{starting_balance_float:.0f}"),
                    'Final Balance': float(f"{final_balance_float:.0f}"),
                    'Final Balance BTC': float(result.final_balance_btc) if result.final_balance_btc is not None else 0,
                    'TWE': f"{result.config.bot.long.total_wallet_exposure_limit:.2f} / {result.config.bot.short.total_wallet_exposure_limit:.2f}",
                    'POS': f"{result.config.bot.long.n_positions:.2f} / {result.config.bot.short.n_positions:.2f}",
                    'index': result,
                })
        column_config = {
            "id": None,
            "index": None,
            'Select': st.column_config.CheckboxColumn(label="Select"),
            'View': st.column_config.CheckboxColumn(label="View"),
            'WE': st.column_config.CheckboxColumn(label="WE"),
            'Plot': st.column_config.CheckboxColumn(label="BE Plot"),
            'Fills': st.column_config.CheckboxColumn(label="Fills"),
            'ADG': st.column_config.NumberColumn(format="%.4f"),
            'Result Time': st.column_config.DatetimeColumn(label="Result Time", format="YYYY-MM-DD HH:mm:ss"),
            'Gain': st.column_config.NumberColumn(label="Gain", format="%.2f"),
            'Drawdown Worst': st.column_config.NumberColumn(label="Worst DD", format="%.4f"),
            'Sharpe Ratio': st.column_config.NumberColumn(label="Sharpe", format="%.4f"),
            'Starting Balance': st.column_config.NumberColumn(label="Start B."),
            'Final Balance': st.column_config.NumberColumn(label="Final B."),
            'Final Balance BTC': st.column_config.NumberColumn(label="Final B. BTC"),
            }
        if "sort_bt_v7_results" in st.session_state:
            if st.session_state.sort_bt_v7_results != self.sort_results:
                self.sort_results = st.session_state.sort_bt_v7_results
                self.save_sort_results()
        else:
            st.session_state.sort_bt_v7_results = self.sort_results
        if "sort_bt_v7_results_order" in st.session_state:
            if st.session_state.sort_bt_v7_results_order != self.sort_results_order:
                self.sort_results_order = st.session_state.sort_bt_v7_results_order
                self.save_sort_results()
        else:
            st.session_state.sort_bt_v7_results_order = self.sort_results_order
        # Display sort options
        col1, col2 = st.columns([1, 9], vertical_alignment="bottom")
        with col1:
            st.selectbox("Sort by:", ['Result Time', 'Backtest Name', 'ADG', 'Gain', 'Drawdown Worst', 'Sharpe Ratio', 'Starting Balance', 'Final Balance', 'Final Balance BTC'], key=f'sort_bt_v7_results', index=0)
        with col2:
            st.checkbox("Reverse", value=True, key=f'sort_bt_v7_results_order')
        self.results_d = sorted(self.results_d, key=lambda x: x[st.session_state[f'sort_bt_v7_results']], reverse=st.session_state[f'sort_bt_v7_results_order'])

        #Display Backtests
        height = 36+(len(self.results_d))*35
        # if height > 1000: height = 1016
        if height > 1000: height = 386
        st.data_editor(data=self.results_d, height="auto", key=f'select_btv7_result_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','drawdown_max','final_balance'])
        if st.session_state.btv7_compare_results:
            self.view_compare()
        if f'select_btv7_result_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_btv7_result_{ed_key}']
            for row in ed["edited_rows"]:
                if "View" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["View"]:
                        self.results_d[row]["index"].load_fills()
                        self.results_d[row]["index"].load_be()
                        self.results_d[row]["index"].view_chart_be()
                        self.results_d[row]["index"].view_chart_drawdown()
                        if self.results_d[row]["index"].config.backtest.btc_collateral_cap > 0:
                            self.results_d[row]["index"].view_chart_be_btc()
                            self.results_d[row]["index"].view_chart_drawdown_btc()
                        self.results_d[row]["index"].view_chart_symbol()
                        if "WE" in ed["edited_rows"][row]:
                            if ed["edited_rows"][row]["WE"]:
                                self.results_d[row]["index"].view_chart_twe()
                        self.results_d[row]["index"].view()
                if "Plot" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Plot"]:
                        self.results_d[row]["index"].view_plot()
                if "Fills" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Fills"]:
                        self.results_d[row]["index"].view_fills()

    def load_sort_results(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.sort_results = pb_config.get("backtest_v7", "sort_results") if pb_config.has_option("backtest_v7", "sort_results") else "Result Time"
        self.sort_results_order = eval(pb_config.get("backtest_v7", "sort_results_order")) if pb_config.has_option("backtest_v7", "sort_results_order") else True

    def save_sort_results(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("backtest_v7"):
            pb_config.add_section("backtest_v7")
        pb_config.set("backtest_v7", "sort_results", str(self.sort_results))
        pb_config.set("backtest_v7", "sort_results_order", str(self.sort_results_order))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    def add_to_compare(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    self.add_compare(self.results_d[row]["index"])

    def strategy_explorer(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        if selected_count > 1:
            error_popup("Please select only one Backtest to calculate balance")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    sel_result = self.results_d[row]["index"]
                    st.session_state.v7_strategy_explorer_config = sel_result.config
                    st.session_state.v7_strategy_explorer_config.pbgui.note = f'{sel_result.config.backtest.base_dir.split("/")[-1]}'

                    # When opening Strategy Explorer from a backtest result, auto-wire the PB7 backtest folder
                    # so Compare/Movie Builder can load `fills.csv` without manual copy/paste.
                    try:
                        st.session_state["se_hist_compare_pb7_dir"] = str(sel_result.result_path)
                        st.session_state["se_hist_compare_mode"] = "PB7 vs B vs C"
                        st.session_state["se_hist_compare_use_pb7_range"] = True
                        # Auto-initialize Strategy Explorer time range + Movie Builder from this backtest.
                        st.session_state["se_open_from_backtest_result"] = True
                        st.session_state["se_open_from_backtest_dir"] = str(sel_result.result_path)
                        st.session_state["se_movie_engine"] = "PB7 fills.csv (from backtest)"
                    except Exception:
                        pass
                    st.switch_page(get_navi_paths()["V7_STRATEGY_EXPLORER"])

    def optimize_from_result(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        if selected_count > 1:
            error_popup("Please select only one Backtest to calculate balance")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.opt_v7 = OptimizeV7.OptimizeV7Item()
                    st.session_state.opt_v7.config = self.results_d[row]["index"].config
                    st.session_state.opt_v7.config.pbgui.starting_config = True
                    st.session_state.opt_v7.name = self.results_d[row]["index"].config.backtest.base_dir.split('/')[-1]
                    if "opt_v7_list" in st.session_state:
                        del st.session_state.opt_v7_list
                    if "opt_v7_queue" in st.session_state:
                        del st.session_state.opt_v7_queue
                    if "opt_v7_results" in st.session_state:    
                        del st.session_state.opt_v7_results
                    if "opt_v7_pareto" in st.session_state:
                        del st.session_state.opt_v7_pareto
                    # if "limits_data" in st.session_state:
                    #     del st.session_state.limits_data
                    st.switch_page(get_navi_paths()["V7_OPTIMIZE"])

    def add_to_run(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        if selected_count > 1:
            error_popup("Please select only one Backtest to calculate balance")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.edit_v7_instance = V7Instance()
                    st.session_state.edit_v7_instance.config = self.results_d[row]["index"].config
                    st.session_state.edit_v7_instance.user = st.session_state.edit_v7_instance.config.live.user
                    st.switch_page(get_navi_paths()["V7_RUN"])

    def add_to_config_archive(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    ConfigV7Archives().add_config(self.results_d[row]["index"].result_path)
        info_popup(f"Selected Backtests added to config archive")

    @st.dialog("Select backtest parameters", width="large")
    def select_parameters(self, ed):
        st.session_state.select_bt_v7_run = False
        start_date = st.date_input("start_date", value="2020-01-01", format="YYYY-MM-DD", key="select_bt_v7_start_date")
        end_date = st.date_input("end_date", value="today", format="YYYY-MM-DD", key="select_bt_v7_end_date")
        starting_balance = st.number_input("Starting Balance", value=1000, step=1000, key="select_bt_v7_starting_balance")  
        exchanges = st.multiselect('Exchanges', ["binance", "bybit", "gateio", "bitget"], default=["binance", "bybit"], key="select_bt_v7_exchanges")
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("OK"):
                for row in ed["edited_rows"]:
                    if "Select" in ed["edited_rows"][row]:
                        if ed["edited_rows"][row]["Select"]:
                            bt_v7 = BacktestV7Item(f'{self.results_d[row]["index"].result_path}/config.json')
                            bt_v7.config.backtest.start_date = start_date.strftime("%Y-%m-%d")
                            bt_v7.config.backtest.end_date = end_date.strftime("%Y-%m-%d")
                            bt_v7.config.backtest.starting_balance = starting_balance
                            bt_v7.config.backtest.exchanges = exchanges
                            bt_v7.name = f'{bt_v7.config.backtest.base_dir.split("/")[-1]}'
                            bt_v7.save()
                            bt_v7.save_queue()
                if "bt_v7_results" in st.session_state:
                    del st.session_state.bt_v7_results
                if "config_v7_config_archive" in st.session_state:
                    del st.session_state.config_v7_config_archive
                st.session_state.bt_v7_queue = BacktestV7Queue()
                st.rerun()
        with col2:
            if st.button("Cancel"):
                st.rerun()

    def backtest_selected_results(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        if selected_count > 1:
            self.select_parameters(ed)
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.bt_v7 = BacktestV7Item(f'{self.results_d[row]["index"].result_path}/config.json')
                    st.session_state.bt_v7.config.backtest.end_date = "now"
                    if "bt_v7_results" in st.session_state:
                        del st.session_state.bt_v7_results
                    if "config_v7_config_archive" in st.session_state:
                        del st.session_state.config_v7_config_archive
                    st.rerun()
    
    def calculate_balance(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        if selected_count > 1:
            error_popup("Please select only one Backtest to calculate balance")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.balance_calc = BalanceCalculator(f'{self.results_d[row]["index"].result_path}/config.json')
                    st.switch_page(get_navi_paths()["V7_BALANCE_CALC"])

    def remove_selected_results(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    self.results_d[row]["index"].remove()
        for result in self.results[:]:
            if not Path(result.result_path).exists():
                self.results.remove(result)

    def remove_all_results(self):
        rmtree(f'{self.results_path}', ignore_errors=True)
        self.results = []

    def remove_compare(self, result):
        if st.session_state.btv7_compare_results:
            for r in st.session_state.btv7_compare_results.copy():
                if r.result_path == result.result_path:
                    st.session_state.btv7_compare_results.remove(r)

    def add_compare(self, result):
        if st.session_state.btv7_compare_results:
            for r in st.session_state.btv7_compare_results.copy():
                if r.result_path == result.result_path:
                    return
        st.session_state.btv7_compare_results.append(result)

    @st.fragment
    def view_compare(self):
        self.compare_fig = go.Figure()
        self.compare_fig.update_layout(yaxis_title='Balance')
        self.compare_fig.update_layout(title_text="Compare Results", title_x=0.5)
        self.compare_fig.update_layout(yaxis_title='Balance', height=800)
        self.compare_fig.update_xaxes(showgrid=True, griddash="dot")
        self.compare_fig_btc = go.Figure()
        self.compare_fig_btc.update_layout(yaxis_title='Balance')
        self.compare_fig_btc.update_layout(title_text="Compare Results BTC", title_x=0.5)
        self.compare_fig_btc.update_layout(yaxis_title='Balance', height=800)
        self.compare_fig_btc.update_xaxes(showgrid=True, griddash="dot")
        view_btc = False
        if st.session_state.btv7_compare_results:
            if st.button("Clear Compare"):
                st.session_state.btv7_compare_results = []
                st.rerun()
            for result in st.session_state.btv7_compare_results:
                result.load_be()
                if result.be is not None:
                    name = PurePath(*result.result_path.parts[-3:-2])
                    formatted_time = result.time.strftime("%Y-%m-%d %H:%M:%S")
                    self.compare_fig.add_trace(go.Scatter(x=result.be['time'], y=result.be['equity'], name=f"{name} {formatted_time} equity", line=dict(width=0.75)))
                    self.compare_fig.add_trace(go.Scatter(x=result.be['time'], y=result.be['balance'], name=f"{name} {formatted_time} balance", line=dict(width=2.5)))
                    if result.config.backtest.btc_collateral_cap > 0:
                        name = PurePath(*result.result_path.parts[-3:-2])
                        formatted_time = result.time.strftime("%Y-%m-%d %H:%M:%S")
                        self.compare_fig_btc.add_trace(go.Scatter(x=result.be['time'], y=result.be['equity_btc'], name=f"{name} {formatted_time} equity_btc", line=dict(width=0.75)))
                        self.compare_fig_btc.add_trace(go.Scatter(x=result.be['time'], y=result.be['balance_btc'], name=f"{name} {formatted_time} balance_btc", line=dict(width=2.5)))
                        view_btc = True
            st.plotly_chart(self.compare_fig, key=f"backtest_v7_compare_be")
            if view_btc:
                st.plotly_chart(self.compare_fig_btc, key=f"backtest_v7_compare_be_btc")

class BacktestsV7:
    def __init__(self):
        self.backtests = []
        self.d = []
        self.sort = "Time"
        self.sort_order = True
        self.load_sort()

    def view_backtests(self):
        # Init
        if not self.backtests:
            self.find_backtests()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if not self.d:
            for id, bt in enumerate(self.backtests):
                self.d.append({
                    'id': id,
                    'Select': False,
                    'Results': bt.results.calculate_results(),
                    'Name': bt.name,
                    'Time': datetime.datetime.fromtimestamp(bt.date),
                    'Exchange': bt.config.backtest.exchanges,
                    'item': bt,
                })
        column_config = {
            "id": None,
            "item": None,
            "Select": st.column_config.CheckboxColumn(label="Select"),
            "Time": st.column_config.DatetimeColumn(label="Time", format="YYYY-MM-DD HH:mm:ss"),
            }
        # Display Backtests
        if "sort_bt_v7" in st.session_state:
            if st.session_state.sort_bt_v7 != self.sort:
                self.sort = st.session_state.sort_bt_v7
                self.save_sort()
        else:
            st.session_state.sort_bt_v7 = self.sort
        if "sort_bt_v7_order" in st.session_state:
            if st.session_state.sort_bt_v7_order != self.sort_order:
                self.sort_order = st.session_state.sort_bt_v7_order
                self.save_sort()
        else:
            st.session_state.sort_bt_v7_order = self.sort_order
        # Display sort options
        col1, col2 = st.columns([1, 9], vertical_alignment="bottom")
        with col1:
            st.selectbox("Sort by:", ['Time', 'Name', 'Results', 'Exchange'], key=f'sort_bt_v7', index=0)
        with col2:
            st.checkbox("Reverse", value=True, key=f'sort_bt_v7_order')
        self.d = sorted(self.d, key=lambda x: x[st.session_state[f'sort_bt_v7']], reverse=st.session_state[f'sort_bt_v7_order'])
        height = 36+(len(self.d))*35
        if height > 1000: height = 1016
        st.data_editor(data=self.d, height=height, key=f'select_backtest_v7_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def load_sort(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.sort = pb_config.get("backtest_v7", "sort") if pb_config.has_option("backtest_v7", "sort") else "Time"
        self.sort_order = eval(pb_config.get("backtest_v7", "sort_order")) if pb_config.has_option("backtest_v7", "sort_order") else True

    def save_sort(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("backtest_v7"):
            pb_config.add_section("backtest_v7")
        pb_config.set("backtest_v7", "sort", str(self.sort))
        pb_config.set("backtest_v7", "sort_order", str(self.sort_order))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    def find_backtests(self):
        self.backtests = []
        p = str(Path(f'{PBGDIR}/data/bt_v7/**/backtest.json'))
        found_bt = glob.glob(p, recursive=False)
        if found_bt:
            for p in found_bt:
                bt = BacktestV7Item(p)
                self.backtests.append(bt)

    def view_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_backtest_v7_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        elif selected_count > 1:
            error_popup("Please select only one Backtest to view")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.bt_v7_results = self.d[row]["item"].results
                    st.rerun()

    def edit_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_backtest_v7_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Backtests selected")
            return
        elif selected_count > 1:
            error_popup("Please select only one Backtest to view")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.bt_v7 = self.d[row]["item"]
                    st.rerun()

    @st.dialog("No Backtest selected. Delete all?")
    def remove_all(self):
        st.warning(f"Delete all Backtests?", icon="⚠️")
        # reason = st.text_input("Because...")
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button(":green[Yes]"):
                rmtree(f'{PBGDIR}/data/bt_v7', ignore_errors=True)
                if "bt_v7_remove_results" in st.session_state:
                    if st.session_state.bt_v7_remove_results:
                        for bt in self.backtests:
                            bt.results.remove_all_results()
                self.d = []
                self.backtests = []
                st.rerun()
        with col2:
            if st.button(":red[No]"):
                st.rerun()

    def remove_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_backtest_v7_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            self.remove_all()
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    self.d[row]["item"].remove()
                    if "bt_v7_remove_results" in st.session_state:
                        if st.session_state.bt_v7_remove_results:
                            self.d[row]["item"].results.remove_all_results()
        self.d = []
        self.backtests = []
        st.rerun()

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
        time.sleep(15)

if __name__ == '__main__':
    main()