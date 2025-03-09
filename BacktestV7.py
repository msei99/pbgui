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
from pbgui_func import PBGDIR, pb7dir, pb7venv, validateJSON, config_pretty_str, load_symbols_from_ini, error_popup, get_navi_paths, replace_special_chars
from PBCoinData import CoinData
import uuid
from Base import Base
from Exchange import Exchange
from Config import Config, ConfigV7
from pathlib import Path, PurePath
from shutil import rmtree
from RunV7 import V7Instance
import OptimizeV7
import datetime
import logging
import os
import datetime
import fnmatch

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
                    bt = BacktestV7Results()
                    bt.results_path = f'{pb7dir()}/backtests/pbgui/{self.items[row].name}'
                    st.session_state.bt_v7_results = bt
                    del st.session_state.bt_v7_queue
                    st.rerun()
        d = []
        for id, bt in enumerate(self.items):
            if type(bt.exchange) != list:
                bt.exchange = [bt.exchange]
            d.append({
                'id': id,
                'run': False,
                'Status': bt.status(),
                'view': False,
                'log': False,
                'delete': False,
                'name': bt.name,
                'filename': bt.filename,
                'Time': datetime.datetime.fromtimestamp(Path(f'{PBGDIR}/data/bt_v7_queue/{bt.filename}.json').stat().st_mtime),
                'exchange': bt.exchange,
                'finish': bt.is_finish(),
            })
        column_config = {
            # "id": None,
            "run": st.column_config.CheckboxColumn('Start/Stop', default=False),
            "view": st.column_config.CheckboxColumn(label="View Results"),
            "log": st.column_config.CheckboxColumn(label="View Logfile"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            "id": st.column_config.NumberColumn(format="%.0f", label="ID"),
            "name": st.column_config.TextColumn(label="Name"),
            "filename": st.column_config.TextColumn(label="Filename"),
            "exchange": st.column_config.ListColumn(label="Exchange"),
            "finish": st.column_config.CheckboxColumn(label="Finished"),
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
        self.config = ConfigV7()
        self.log = None
        self.results = BacktestV7Results()
        if backtest_path:
            self.config.config_file = backtest_path
            self.config.load_config()
            self.name = self.config.backtest.base_dir.split('/')[-1]
            self.results.results_path = str(Path(f'{pb7dir()}/backtests/pbgui/{self.name}'))
            self.results.name = self.name
        else:
            self.initialize()

    def initialize(self):
        self.name = ""
        self.config.backtest.start_date = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.config.backtest.end_date = datetime.date.today().strftime("%Y-%m-%d")
        self.config.optimize.n_cpus = multiprocessing.cpu_count()

    # Exchanges
    @st.fragment
    def fragment_exchanges(self):
        if "edit_bt_v7_exchanges" in st.session_state:
            if st.session_state.edit_bt_v7_exchanges != self.config.backtest.exchanges:
                self.config.backtest.exchanges = st.session_state.edit_bt_v7_exchanges
                st.rerun()
        else:
            st.session_state.edit_bt_v7_exchanges = self.config.backtest.exchanges
        st.multiselect('Exchanges',["binance", "bybit", "gateio", "bitget"], key="edit_bt_v7_exchanges")

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
        # Init session state for approved_coins
        if "edit_bt_v7_approved_coins_long" in st.session_state:
            if st.session_state.edit_bt_v7_approved_coins_long != self.config.live.approved_coins.long:
                self.config.live.approved_coins.long = st.session_state.edit_bt_v7_approved_coins_long
        else:
            st.session_state.edit_bt_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_bt_v7_approved_coins_short" in st.session_state:
            if st.session_state.edit_bt_v7_approved_coins_short != self.config.live.approved_coins.short:
                self.config.live.approved_coins.short = st.session_state.edit_bt_v7_approved_coins_short
        else:
            st.session_state.edit_bt_v7_approved_coins_short = self.config.live.approved_coins.short
        # Init session state for ignored_coins
        if "edit_bt_v7_ignored_coins_long" in st.session_state:
            if st.session_state.edit_bt_v7_ignored_coins_long != self.config.live.ignored_coins.long:
                self.config.live.ignored_coins.long = st.session_state.edit_bt_v7_ignored_coins_long
        else:
            st.session_state.edit_bt_v7_ignored_coins_long = self.config.live.ignored_coins.long
        if "edit_bt_v7_ignored_coins_short" in st.session_state:
            if st.session_state.edit_bt_v7_ignored_coins_short != self.config.live.ignored_coins.short:
                self.config.live.ignored_coins.short = st.session_state.edit_bt_v7_ignored_coins_short
        else:
            st.session_state.edit_bt_v7_ignored_coins_short = self.config.live.ignored_coins.short
        # Apply filters
        if st.session_state.edit_bt_v7_apply_filters:
            self.config.live.approved_coins.long = sorted(list(set(st.session_state.coindata_bybit.approved_coins + st.session_state.coindata_binance.approved_coins + st.session_state.coindata_gateio.approved_coins + st.session_state.coindata_bitget.approved_coins)))
            self.config.live.approved_coins.short = sorted(list(set(st.session_state.coindata_bybit.approved_coins + st.session_state.coindata_binance.approved_coins + st.session_state.coindata_gateio.approved_coins + st.session_state.coindata_bitget.approved_coins)))
            self.config.live.ignored_coins.long = sorted(list(set(st.session_state.coindata_bybit.ignored_coins + st.session_state.coindata_binance.ignored_coins + st.session_state.coindata_gateio.ignored_coins + st.session_state.coindata_bitget.ignored_coins)))
            self.config.live.ignored_coins.short = sorted(list(set(st.session_state.coindata_bybit.ignored_coins + st.session_state.coindata_binance.ignored_coins + st.session_state.coindata_gateio.ignored_coins + st.session_state.coindata_bitget.ignored_coins)))
        # Remove unavailable symbols
        symbols = []
        if "bybit" in self.config.backtest.exchanges:
            symbols.extend(st.session_state.coindata_bybit.symbols)
        if "binance" in self.config.backtest.exchanges:
            symbols.extend(st.session_state.coindata_binance.symbols)
        if "gateio" in self.config.backtest.exchanges:
            symbols.extend(st.session_state.coindata_gateio.symbols)
        if "bitget" in self.config.backtest.exchanges:
            symbols.extend(st.session_state.coindata_bitget.symbols)
        symbols = list(set(symbols))
        # sort symbols
        symbols = sorted(symbols)
        for symbol in self.config.live.approved_coins.long.copy():
            if symbol not in symbols:
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.approved_coins.short.copy():
            if symbol not in symbols:
                self.config.live.approved_coins.short.remove(symbol)
        for symbol in self.config.live.ignored_coins.long.copy():
            if symbol not in symbols:
                self.config.live.ignored_coins.long.remove(symbol)
        for symbol in self.config.live.ignored_coins.short.copy():
            if symbol not in symbols:
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
        for coin in list(set(self.config.live.approved_coins.long + self.config.live.approved_coins.short)):
            if coin in st.session_state.coindata_bybit.symbols_notices:
                st.warning(f'{coin}: {st.session_state.coindata_bybit.symbols_notices[coin]}')
            elif coin in st.session_state.coindata_binance.symbols_notices:
                st.warning(f'{coin}: {st.session_state.coindata_binance.symbols_notices[coin]}')
            elif coin in st.session_state.coindata_gateio.symbols_notices:
                st.warning(f'{coin}: {st.session_state.coindata_gateio.symbols_notices[coin]}')
            elif coin in st.session_state.coindata_bitget.symbols_notices:
                st.warning(f'{coin}: {st.session_state.coindata_bitget.symbols_notices[coin]}')
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
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_bybit.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_binance.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_gateio.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_bitget.market_cap = self.config.pbgui.market_cap
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
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_vol_mcap = round(float(self.config.pbgui.vol_mcap),2)
            st.session_state.coindata_bybit.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_binance.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_gateio.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_bitget.vol_mcap = self.config.pbgui.vol_mcap
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
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_tags = self.config.pbgui.tags
            st.session_state.coindata_bybit.tags = self.config.pbgui.tags
            st.session_state.coindata_binance.tags = self.config.pbgui.tags
            st.session_state.coindata_gateio.tags = self.config.pbgui.tags
            st.session_state.coindata_bitget.tags = self.config.pbgui.tags
        # remove duplicates from tags and sort them
        tags = sorted(list(set(st.session_state.coindata_bybit.all_tags + st.session_state.coindata_binance.all_tags + st.session_state.coindata_gateio.all_tags + st.session_state.coindata_bitget.all_tags)))
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
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_bybit.only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_binance.only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_bitget.only_cpt = self.config.pbgui.only_cpt
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
                if st.session_state.edit_bt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_bt_v7_notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_bybit.notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_binance.notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_gateio.notices_ignore = self.config.pbgui.notices_ignore
            st.session_state.coindata_bitget.notices_ignore = self.config.pbgui.notices_ignore
        st.checkbox("notices_ignore", key="edit_bt_v7_notices_ignore", help=pbgui_help.notices_ignore)

    def edit(self):
        # Init coindata
        if "coindata_bybit" not in st.session_state:
            st.session_state.coindata_bybit = CoinData()
            st.session_state.coindata_bybit.exchange = "bybit"
        if "coindata_binance" not in st.session_state:
            st.session_state.coindata_binance = CoinData()
            st.session_state.coindata_binance.exchange = "binance"
        if "coindata_gateio" not in st.session_state:
            st.session_state.coindata_gateio = CoinData()
            st.session_state.coindata_gateio.exchange = "gateio"
        if "coindata_bitget" not in st.session_state:
            st.session_state.coindata_bitget = CoinData()
            st.session_state.coindata_bitget.exchange = "bitget"
        # Display Editor
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_exchanges()
        with col2:
            self.fragment_name()
        with col3:
            self.fragment_start_date()
        with col4:
            self.fragment_end_date()
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_starting_balance()
        with col2:
            self.fragment_minimum_coin_age_days()
        with col3:
            self.fragment_gap_tolerance_ohlcvs_minutes()
        with col4:
            self.fragment_combine_ohlcvs()
            self.fragment_compress_cache()
        #Filters
        self.fragment_filter_coins()
        # Config
        self.config.bot.edit()

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
            "exchange": self.config.backtest.exchanges,
        }
        if not dest.exists():
            dest.mkdir(parents=True)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(bt_dict, f, indent=4)

    def remove(self):
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
        st.text_area(f'config', json.dumps(self.config.config, indent=4), key="import_backtest_v7_config", height=500)
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("OK"):
                del st.session_state.edit_bt_v7_exchanges
                del st.session_state.edit_bt_v7_name
                del st.session_state.edit_bt_v7_start_date
                del st.session_state.edit_bt_v7_end_date
                del st.session_state.edit_bt_v7_starting_balance
                del st.session_state.edit_bt_v7_minimum_coin_age_days
                del st.session_state.edit_bt_v7_compress_cache
                del st.session_state.edit_bt_v7_market_cap
                del st.session_state.edit_bt_v7_vol_mcap
                del st.session_state.edit_bt_v7_approved_coins_long
                del st.session_state.edit_bt_v7_approved_coins_short
                del st.session_state.edit_bt_v7_ignored_coins_long
                del st.session_state.edit_bt_v7_ignored_coins_short
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
        self.time = None
        self.result = self.load_result()
        self.config = ConfigV7(PurePath(f'{self.result_path}/config.json'))
        self.config.load_config()
        self.backtest_config = self.load_backtest_config()
        self.ed = self.config.backtest.end_date
        self.adg = self.result["adg"]
        self.drawdown_worst = self.result["drawdown_worst"]
        self.sharpe_ratio = self.result["sharpe_ratio"]
        self.starting_balance = self.config.backtest.starting_balance
        self.be = None
        self.final_balance = self.load_final_balance()
        self.fills = None
    
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
        if balance.exists():
            with open(balance, "r", encoding='utf-8') as file:
                end_of_file = file.seek(0, 2)
                file.seek(end_of_file)
                n = 0
                for num in range(end_of_file+1):            
                    file.seek(end_of_file - num)    
                    last_line = file.read()
                    if last_line.count('\n') == 1: 
                        if len(last_line.split(',')) == 3:
                            final_balance = last_line.split(',')[1]
                            return final_balance
                        else: last_line = None
        return None

    def load_be(self):
        if self.be is None:
            be = f'{self.result_path}/balance_and_equity.csv'
            if Path(be).exists():
                self.be = pd.read_csv(be)
                timestamp = datetime.datetime.strptime(self.ed, '%Y-%m-%d').timestamp()
                start_time = timestamp - (self.be.iloc[:, 0].iloc[-1] * 60)
                self.be['time'] = datetime.datetime.fromtimestamp(start_time) + pd.to_timedelta(self.be.iloc[:, 0], unit='m')

    def load_fills(self):
        if self.fills is None:
            fills = f'{self.result_path}/fills.csv'
            if Path(fills).exists():
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
        if self.be is not None:
            fig = go.Figure()
            fig.update_layout(yaxis_title='Balance')
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

    # Create Symbol Chart with plotly
    def view_chart_symbol(self):
        if self.fills is not None:
            fig = go.Figure()
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
                self.fills['we'] = 1 / self.fills["balance"] * self.fills['psize'] * self.fills['pprice']

                # Create pivot table
                exposure_by_currency = self.fills.pivot_table(
                    index='time',
                    columns=coin_or_symbol,
                    values='we',
                    aggfunc='last'
                ).fillna(0)

                # Calculate total exposure and fill missing values
                exposure_by_currency['twe'] = exposure_by_currency.sum(axis=1).ffill().fillna(0)

                # Fill missing time slots with a custom function
                resolution = st.session_state[f"backtest_v7_{self.result_path}_resolution"]
                exposure_by_currency = exposure_by_currency.resample(f'{resolution}T').agg(lambda x: x.ffill().max() if not x.empty else 0)
                
                # Ensure 'twe' is filled and retains maximum values too
                exposure_by_currency['twe'] = exposure_by_currency['twe'].resample(f'{resolution}T').agg(lambda x: x.ffill().max() if not x.empty else 0)

                # Plot total exposure
                fig.add_trace(go.Scatter(x=exposure_by_currency.index, y=exposure_by_currency['twe'], name="Total Exposure"))

                # Plot each coin's exposure
                for coin in exposure_by_currency.columns[:-1]:  # Exclude 'twe' column
                    fig.add_trace(go.Scatter(x=exposure_by_currency.index, y=exposure_by_currency[coin], name=f"{coin} Exposure"))

                fig.update_layout(yaxis_title='Exposure', height=800)
                fig.update_xaxes(showgrid=True, griddash="dot")
                st.plotly_chart(fig, key=f"backtest_v7_{self.result_path}_we")
        else:
            st.error("No fills data found")

class BacktestV7Results:

    def __init__(self):
        self.results = []
        self.results_d = []
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
                if not self.filter == "":
                    for result in self.results.copy():
                        target = result.config.backtest.base_dir.split('/')[-1]
                        if not fnmatch.fnmatch(target.lower(), self.filter.lower()):
                            self.results.remove(result)
        else:
            st.session_state.select_btv7_result_filter = self.filter
            
        st.text_input("Filter by Backtest Name", value="", help=pbgui_help.smart_filter, key="select_btv7_result_filter")
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_btv7_result_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_btv7_result_{ed_key}']
            for row in ed["edited_rows"]:
                if "Compare" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Compare"]:
                        self.add_compare(self.results[row])
                    else:
                        self.remove_compare(self.results[row])
                if "Create Run" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Create Run"]:
                        st.session_state.edit_v7_instance = V7Instance()
                        st.session_state.edit_v7_instance.config = self.results[row].config
                        st.session_state.edit_v7_instance.user = st.session_state.edit_v7_instance.config.live.user
                        st.switch_page(get_navi_paths()["V7_RUN"])
                if "BT" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["BT"]:
                        st.session_state.bt_v7 = BacktestV7Item(f'{self.results[row].result_path}/config.json')
                        del st.session_state.bt_v7_results
                        st.rerun()
                if "Optimize" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Optimize"]:
                        st.session_state.opt_v7 = OptimizeV7.OptimizeV7Item()
                        st.session_state.opt_v7.config = self.results[row].config
                        st.session_state.opt_v7.config.pbgui.starting_config = True
                        st.session_state.opt_v7.name = self.results[row].config.backtest.base_dir.split('/')[-1]
                        if "opt_v7_list" in st.session_state:
                            del st.session_state.opt_v7_list
                        if "opt_v7_queue" in st.session_state:
                            del st.session_state.opt_v7_queue
                        if "opt_v7_results" in st.session_state:    
                            del st.session_state.opt_v7_results
                        st.switch_page(get_navi_paths()["V7_OPTIMIZE"])
                if "GridVis" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["GridVis"]:
                        st.session_state.v7_grid_visualizer_config = self.results[row].config
                        st.session_state.v7_grid_visualizer_config.pbgui.note = f'{self.results[row].config.backtest.base_dir.split("/")[-1]}' 
                        st.switch_page(get_navi_paths()["V7_GRID_VISUALIZER"])
        if not self.results_d:
            for id, result in enumerate(self.results):
                compare = False
                if st.session_state.btv7_compare_results:
                    for r in st.session_state.btv7_compare_results:
                        if r.result_path == result.result_path:
                            compare = True
                
                starting_balance_float = float(result.starting_balance)
                final_balance_float = float(result.final_balance)
                self.results_d.append({
                    'id': id,
                    'Backtest Name': result.config.backtest.base_dir.split('/')[-1],
                    'Exch.': str(result.result_path).split('/')[-2],
                    'Result Time': result.time.strftime("%Y-%m-%d %H:%M:%S") if result.time else '',
                    'ADG': f"{result.adg:.4f}",
                    'Drawdown Worst': f"{result.drawdown_worst:.4f}",
                    'Sharpe Ratio': f"{result.sharpe_ratio:.4f}",
                    'Starting Balance': f"{starting_balance_float:,.0f}",
                    'Final Balance': f"{final_balance_float:,.0f}",
                    'TWE': f"{result.config.bot.long.total_wallet_exposure_limit:.2f} / {result.config.bot.short.total_wallet_exposure_limit:.2f}",
                    'POS': f"{result.config.bot.long.n_positions:.2f} / {result.config.bot.short.n_positions:.2f}",
                    'View': False,
                    'WE': False,
                    'Plot': False,
                    'Fills': False,
                    'Create Run': False,
                    'BT': False,
                    'Optimize': False,
                    'GridVis': False,
                    'Delete': False,
                    'Compare': compare,  # Add Compare field
                })
        column_config = {
            "id": None,
            'View': st.column_config.CheckboxColumn(label="Results"),
            'WE': st.column_config.CheckboxColumn(label="WE"),
            'Plot': st.column_config.CheckboxColumn(label="BE Plot"),
            'Fills': st.column_config.CheckboxColumn(label="Fills"),
            'Create Run': st.column_config.CheckboxColumn(label="Run"),
            'BT': st.column_config.CheckboxColumn(label="BT"),
            'Optimize': st.column_config.CheckboxColumn(label="Opt"),
            'Delete': st.column_config.CheckboxColumn(label="Del"),
            'Compare': st.column_config.CheckboxColumn(label="Comp"),
            'ADG': st.column_config.NumberColumn(format="%.4f"),
            'Drawdown Worst': st.column_config.NumberColumn(label="Worst DD", format="%.4f"),
            'Sharpe Ratio': st.column_config.NumberColumn(label="Sharpe", format="%.4f"),
            'Starting Balance': st.column_config.NumberColumn(label="Start B."),
            'Final Balance': st.column_config.NumberColumn(label="Final B."),
            }
        #Display Backtests
        height = 36+(len(self.results_d))*35
        if height > 1000: height = 1016
        st.data_editor(data=self.results_d, height=height, use_container_width=True, key=f'select_btv7_result_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','drawdown_max','final_balance'])
        if st.session_state.btv7_compare_results:
            self.view_compare()
        if f'select_btv7_result_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_btv7_result_{ed_key}']
            for row in ed["edited_rows"]:
                if "View" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["View"]:
                        self.results[row].load_fills()
                        self.results[row].load_be()
                        self.results[row].view_chart_be()
                        self.results[row].view_chart_symbol()
                        if "WE" in ed["edited_rows"][row]:
                            if ed["edited_rows"][row]["WE"]:
                                self.results[row].view_chart_twe()
                        self.results[row].view()
                if "Plot" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Plot"]:
                        self.results[row].view_plot()
                if "Fills" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["Fills"]:
                        self.results[row].view_fills()

    def remove_selected_results(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_btv7_result_{ed_key}']
        for row in ed["edited_rows"]:
            if "Delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Delete"]:
                    self.results[row].remove()
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
            self.compare_fig.update_yaxes(tickformat=".2f")
            st.plotly_chart(self.compare_fig, key=f"backtest_v7_compare_be")

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
                    # st.session_state.bt_v7_results = self.backtests[row]
                    st.session_state.bt_v7_results = self.backtests[row].results
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
                'Exchange': bt.config.backtest.exchanges,
                'view': False,
                # 'Backtests': bt.calculate_results(),
                'Backtests': bt.results.calculate_results(),
                'delete' : False,
            })
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Results"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display Backtests
        st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_backtest_v7_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def find_backtests(self):
        self.backtests = []
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