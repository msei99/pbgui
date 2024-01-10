import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from Base import Base
from Backtest import BacktestItem, BacktestResults
from OptimizeConfig import OptimizeConfigs, OptimizeConfig
from pathlib import Path, PurePath
from shutil import rmtree
import json
import glob
import datetime
import time
import uuid
import psutil
import shlex
import subprocess
import sys
import multiprocessing
import configparser
import pbgui_help
from time import sleep
import traceback

class OptimizeItem(Base):
    def __init__(self):
        super().__init__()
        self.file = None
        self.log = None
        self.oc = OptimizeConfig()
        self.sd = None
        self.ed = None
        self.sb = None
        self.reruns = 1
        self.finish = 0
        self.position = None
        self.pbdir = None
        self.results = []
        self.best_long = []
        self.best_short = []
        self.sharp_long = []
        self.sharp_short = []
        self.adg_long = []
        self.adg_short = []
        self.drawdown_long = []
        self.drawdown_short = []
        self.stuck_long = []
        self.stuck_short = []
        self.initialize()

    def initialize(self):
        self.oc = OptimizeConfigs().find_config(OptimizeConfigs().default())
        self.oc.load()
        self.sd = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.ed = datetime.date.today().strftime("%Y-%m-%d")
        self.sb = 1000
        self.backtest_best = 1
        self.backtest_sharp = 0
        self.backtest_adg = 0
        self.backtest_drawdown = 0
        self.backtest_stuck = 0
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("main", "pbdir"):
            self.pbdir = pb_config.get("main", "pbdir")
   
    def is_finish(self):
        if self.finish < self.reruns:
            return False
        return True

    def is_running(self):
        if self.pid():
            return True
        return False

    def stop(self):
        while self.is_running():
            self.pid().kill()

    def pid(self):
        if self.file:
            for process in psutil.process_iter():
                try:
                    cmdline = process.cmdline()
                except psutil.NoSuchProcess:
                    pass
                except psutil.AccessDenied:
                    pass
                if any(str(self.symbol) in sub for sub in cmdline) and any(str(self.oc.passivbot_mode) in sub for sub in cmdline) and any(str(self.oc.algorithm) in sub for sub in cmdline) and any("optimize.py" in sub for sub in cmdline):
                    return process

    def start(self, cpu: int):
        if not self.is_running():
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if self.pbdir:
                cmd = [sys.executable, '-u', PurePath(f'{self.pbdir}/optimize.py')]
                cmd_end = f'-u {self.user} -s {self.symbol} -i {self.oc.iters} -pm {self.oc.passivbot_mode} -a {self.oc.algorithm} -sd {self.sd} -ed {self.ed} -sb {self.sb} -m {self.market_type} -oh {self.ohlcv} -c {cpu} -le {self.oc.do_long} -se {self.oc.do_short} -oc {self.oc.config_file}'
                cmd.extend(shlex.split(cmd_end))
                if self.oc.do_long and not self.oc.do_short:
                    cmd_end = f'-le y -se n'
                    cmd.extend(shlex.split(cmd_end))
                if self.oc.do_short and not self.oc.do_long:
                    cmd_end = f'-le n -se y'
                    cmd.extend(shlex.split(cmd_end))
                if self.oc.do_short and self.oc.do_long:
                    cmd_end = f'-le y -se y'
                    cmd.extend(shlex.split(cmd_end))
                cmd.extend(['-bd', str(PurePath(f'{self.pbdir}/backtests/pbgui'))])
                log = open(self.log,"w")
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Start: {cmd}')
                subprocess.run(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True)
                self.generate_backtest()
                self.finish +=1
                self.save(self.position)

    def remove(self):
        self.file.unlink(missing_ok=True)
        self.log.unlink(missing_ok=True)

    def load_options(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("optimize", "backtest_best"):
            self.backtest_best = int(pb_config.get("optimize", "backtest_best"))
        if pb_config.has_option("optimize", "backtest_sharp"):
            self.backtest_sharp = int(pb_config.get("optimize", "backtest_sharp"))
        if pb_config.has_option("optimize", "backtest_adg"):
            self.backtest_adg = int(pb_config.get("optimize", "backtest_adg"))
        if pb_config.has_option("optimize", "backtest_drawdown"):
            self.backtest_drawdown = int(pb_config.get("optimize", "backtest_drawdown"))
        if pb_config.has_option("optimize", "backtest_stuck"):
            self.backtest_stuck = int(pb_config.get("optimize", "backtest_stuck"))

    def generate_backtest(self):
        self.load_options()
        self.load_results()
        self.find_best()
        backtests = []
        if self.backtest_best > 0:
            if self.oc.do_long:
                if self.best_long:
                    for result in self.best_long:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
            if self.oc.do_short:
                if self.best_short:
                    for result in self.best_short:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
        if self.backtest_sharp > 0:
            if self.oc.do_long:
                if self.sharp_long:
                    for result in self.sharp_long:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
            if self.oc.do_short:
                if self.sharp_short:
                    for result in self.sharp_short:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
        if self.backtest_adg > 0:
            if self.oc.do_long:
                if self.adg_long:
                    for result in self.adg_long:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
            if self.oc.do_short:
                if self.adg_short:
                    for result in self.adg_short:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
        if self.backtest_drawdown > 0:
            if self.oc.do_long:
                if self.drawdown_long:
                    for result in self.drawdown_long:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
            if self.oc.do_short:
                if self.drawdown_short:
                    for result in self.drawdown_short:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
        if self.backtest_stuck > 0:
            if self.oc.do_long:
                if self.stuck_long:
                    for result in self.stuck_long:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
            if self.oc.do_short:
                if self.stuck_short:
                    for result in self.stuck_short:
                        if not result["id"] in [sub["id"] for sub in backtests]:
                            backtests.append(result)
        for backtest in backtests:
            dir = PurePath(backtest["path"]).parent
            name = PurePath(backtest["path"]).name
            bname = name.split("_")[0]
            p = f'{dir}/{bname}*config*'
            config = glob.glob(p)
            self.add_to_backtest(config[0])
    
    def add_to_backtest(self, config : str):
        config_file = Path(config)
        if config_file.exists():
            with open(config_file, "r", encoding='utf-8') as f:
                config = f.read()
            bt = BacktestItem(config)
            bt.user = self.user
            bt.symbol = self.symbol
            bt.market_type = self.market_type
            bt.sb = self.sb
            bt.sd = self.sd
            bt.ed = self.ed
            bt.ohlcv = self.ohlcv
            bt.save()

    def find_best(self):
        results = sorted(self.results, key=lambda d: d['path'])
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_long.json'):
                    if len(self.best_long) < self.backtest_best:
                        self.best_long.append(result)
                elif result["path"].endswith('_result_short.json'):
                    if len(self.best_short) < self.backtest_best:
                        self.best_short.append(result)
        results = sorted(self.results, key=lambda d: d['sharpe_ratio_long'])
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_long.json'):
                    if len(self.sharp_long) < self.backtest_sharp:
                        self.sharp_long.append(result)
        results = sorted(self.results, key=lambda d: d['sharpe_ratio_short'])
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_short.json'):
                    if len(self.sharp_short) < self.backtest_sharp:
                        self.sharp_short.append(result)
        results = sorted(self.results, key=lambda d: d['adg_per_exposure_long'])
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_long.json'):
                    if len(self.adg_long) < self.backtest_adg:
                        self.adg_long.append(result)
        results = sorted(self.results, key=lambda d: d['adg_per_exposure_short'])
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_short.json'):
                    if len(self.adg_short) < self.backtest_adg:
                        self.adg_short.append(result)
        results = sorted(self.results, key=lambda d: d['drawdown_max_long'], reverse=True)
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_long.json'):
                    if len(self.drawdown_long) < self.backtest_drawdown:
                        self.drawdown_long.append(result)
        results = sorted(self.results, key=lambda d: d['drawdown_max_short'], reverse=True)
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_short.json'):
                    if len(self.drawdown_short) < self.backtest_drawdown:
                        self.drawdown_short.append(result)
        results = sorted(self.results, key=lambda d: d['hrs_stuck_max_long'], reverse=True)
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_long.json'):
                    if len(self.stuck_long) < self.backtest_stuck:
                        self.stuck_long.append(result)
        results = sorted(self.results, key=lambda d: d['hrs_stuck_max_short'], reverse=True)
        if results:
            while len(results) > 0:
                result = results.pop()
                if result["path"].endswith('_result_short.json'):
                    if len(self.stuck_short) < self.backtest_stuck:
                        self.stuck_short.append(result)

    def load_results(self):
        fpath = self.fetch_results_fpath()
        p = f'{self.pbdir}/{fpath}*_result_*.json'
        results = glob.glob(p, recursive=True)
        self.results = []
        for i, result in enumerate(results):
            if Path(result).exists():
                try:
                    with open(result, "r", encoding='utf-8') as f:
                        results_dict = {}
                        r = json.load(f)
                        for symbol in list(r.keys()):
                            r_keys = list(r[symbol].keys())
                            for k in r_keys:
                                results_dict["id"] = i
                                results_dict["path"] = result
                                results_dict["symbol"] = symbol
                                results_dict[k] = r[symbol][k]
                except Exception as e:
                    print(f'{str(result)} is corrupted {e}')
                    traceback.print_exc()
            self.results.append(results_dict)

    def fetch_results_fpath(self):
        if self.log.exists():
            with open(self.log, "r", encoding='utf-8') as f:
                log = f.readlines()
                for line in log:
                    if line.startswith('results_fpath'):
                        return line.split(' ')[-1].strip() 
        return None

    def edit_item(self):
        col_1, col_2, col_3 = st.columns([1,1,1])
        with col_1:
            self.sb = st.number_input('STARTING_BALANCE',value=self.sb,step=500)
            self.oc.do_long = st.toggle("Long enabled", value=self.oc.do_long, key="opt_long_enabled", help=None)
            self.oc.passivbot_mode = st.radio('PASSIVBOT_MODE',('recursive_grid', 'neat_grid', 'clock'), index=self.oc.passivbot_mode_index)
        with col_2:
            self.sd = st.date_input("START_DATE", datetime.datetime.strptime(self.sd, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
            self.oc.do_short = st.toggle("Short enabled", value=self.oc.do_short, key="opt_short_enabled", help=None)
            self.oc.algorithm = st.radio("ALGORITHM",('harmony_search', 'particle_swarm_optimization'),index=self.oc.algorithm_index)
        with col_3:
            self.ed = st.date_input("END_DATE", datetime.datetime.strptime(self.ed, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
            self.oc.iters = st.number_input('ITERS',value=self.oc.iters,step=1000, help=pbgui_help.opt_iters)
            self.reruns = st.number_input('Reruns',value=self.reruns,step=5, help=pbgui_help.opt_reruns)

    def load(self, file: str):
        self.file = Path(file)
        self.log = Path(f'{self.file}.log')
        try:
            with open(self.file, "r", encoding='utf-8') as f:
                t = json.load(f)
                if t["market_type"] == "futures":
                    self._market_type = "swap"
                else:
                    self._market_type = "spot"
                self.user = t["user"]
                self.symbol = t["symbol"]
                self.sd = t["sd"]
                self.ed = t["ed"]
                self.sb = t["sb"]
                self.ohlcv = t["ohlcv"]
                self.oc = OptimizeConfigs().find_config(t["oc"])
                self.oc._passivbot_mode = t["mode"]
                self.oc._algorithm = t["algo"]
                self.oc._iters = t["iters"]
                self.oc._do_long = t["long_enabled"]
                self.oc._do_short = t["short_enabled"]
                self.reruns = t["reruns"]
                self.finish = t["finish"]
                self.position = t["position"]
                return True
        except Exception as e:
            print(f'{str(file)} is corrupted {e}')
            return False

    def save(self, pos: int):
        opt_dict = {
            "user": self.user,
            "symbol": self.symbol,
            "sd": self.sd,
            "ed": self.ed,
            "sb": self.sb,
            "market_type": self.market_type,
            "ohlcv": self.ohlcv,
            "mode": self.oc.passivbot_mode,
            "algo": self.oc.algorithm,
            "iters": self.oc.iters,
            "long_enabled": self.oc.do_long,
            "short_enabled": self.oc.do_short,
            "oc": self.oc.name,
            "reruns": self.reruns,
            "finish": self.finish,
            "position": pos,
        }
        with open(self.file, "w", encoding='utf-8') as f:
            json.dump(opt_dict, f, indent=4)

class OptimizeQueue:
    def __init__(self):
        self.items = []
        self.pb_config = configparser.ConfigParser()
        self.pb_config.read('pbgui.ini')
        if not self.pb_config.has_section("optimize"):
            self.pb_config.add_section("optimize")
        if not self.pb_config.has_option("optimize", "cpu"):
            self.pb_config.set("optimize", "cpu", str(multiprocessing.cpu_count()-2))
        if not self.pb_config.has_option("optimize", "mode"):
            self.pb_config.set("optimize", "mode", "linear")
        if not self.pb_config.has_option("optimize", "backtest_best"):
            self.pb_config.set("optimize", "backtest_best", "1")
        if not self.pb_config.has_option("optimize", "backtest_sharp"):
            self.pb_config.set("optimize", "backtest_sharp", "0")
        if not self.pb_config.has_option("optimize", "backtest_adg"):
            self.pb_config.set("optimize", "backtest_adg", "0")
        if not self.pb_config.has_option("optimize", "backtest_drawdown"):
            self.pb_config.set("optimize", "backtest_drawdown", "0")
        if not self.pb_config.has_option("optimize", "backtest_stuck"):
            self.pb_config.set("optimize", "backtest_stuck", "0")
        self.load_options()
        self.pbgdir = Path.cwd()
        self.dest = Path(f'{self.pbgdir}/data/opt_queue')
        if not self.dest.exists():
            self.dest.mkdir(parents=True)
        self.load()

    @property
    def cpu(self): return self._cpu
    @property
    def mode(self): return self._mode
    @property
    def backtest_best(self): return self._backtest_best
    @property
    def backtest_sharp(self): return self._backtest_sharp
    @property
    def backtest_adg(self): return self._backtest_adg
    @property
    def backtest_drawdown(self): return self._backtest_drawdown
    @property
    def backtest_stuck(self): return self._backtest_stuck

    @backtest_best.setter
    def backtest_best(self, new_backtest_best):
        if self._backtest_best != new_backtest_best:
            self._backtest_best = new_backtest_best
            self.save_options()
            st.experimental_rerun()
    @backtest_sharp.setter
    def backtest_sharp(self, new_backtest_sharp):
        if self._backtest_sharp != new_backtest_sharp:
            self._backtest_sharp = new_backtest_sharp
            self.save_options()
            st.experimental_rerun()
    @backtest_adg.setter
    def backtest_adg(self, new_backtest_adg):
        if self._backtest_adg != new_backtest_adg:
            self._backtest_adg = new_backtest_adg
            self.save_options()
            st.experimental_rerun()
    @backtest_drawdown.setter
    def backtest_drawdown(self, new_backtest_drawdown):
        if self._backtest_drawdown != new_backtest_drawdown:
            self._backtest_drawdown = new_backtest_drawdown
            self.save_options()
            st.experimental_rerun()
    @backtest_stuck.setter
    def backtest_stuck(self, new_backtest_stuck):
        if self._backtest_stuck != new_backtest_stuck:
            self._backtest_stuck = new_backtest_stuck
            self.save_options()
            st.experimental_rerun()
    @cpu.setter
    def cpu(self, new_cpu):
        if new_cpu != self._cpu:
            self._cpu = new_cpu
            self.save_options()
            st.experimental_rerun()
    @mode.setter
    def mode(self, new_mode):
        if new_mode != self._mode:
            self._mode = new_mode
            self.save_options()
            st.experimental_rerun()

    def load_options(self):
        self._cpu = int(self.pb_config.get("optimize", "cpu"))
        if self._cpu > multiprocessing.cpu_count():
            self.cpu = multiprocessing.cpu_count()
        self._mode = str(self.pb_config.get("optimize", "mode"))
        self._backtest_best = int(self.pb_config.get("optimize", "backtest_best"))
        self._backtest_sharp = int(self.pb_config.get("optimize", "backtest_sharp"))
        self._backtest_adg = int(self.pb_config.get("optimize", "backtest_adg"))
        self._backtest_drawdown = int(self.pb_config.get("optimize", "backtest_drawdown"))
        self._backtest_stuck = int(self.pb_config.get("optimize", "backtest_stuck"))

    def save_options(self):
        self.pb_config.set("optimize", "cpu", str(self._cpu))
        self.pb_config.set("optimize", "mode", str(self._mode))
        self.pb_config.set("optimize", "backtest_best", str(self._backtest_best))
        self.pb_config.set("optimize", "backtest_sharp", str(self._backtest_sharp))
        self.pb_config.set("optimize", "backtest_adg", str(self._backtest_adg))
        self.pb_config.set("optimize", "backtest_drawdown", str(self._backtest_drawdown))
        self.pb_config.set("optimize", "backtest_stuck", str(self._backtest_stuck))
        with open('pbgui.ini', 'w') as f:
            self.pb_config.write(f)

    def is_running(self):
        if self.pid():
            return True
        return False

    def stop(self):
        if self.is_running():
            self.pid().kill()

    def pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.AccessDenied:
                continue
            if any("Optimize.py" in sub for sub in cmdline):
                return process

    def start(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{self.pbgdir}/Optimize.py')]
            dest = Path(f'{self.pbgdir}/data/logs')
            if not dest.exists():
                dest.mkdir(parents=True)
            logfile = Path(f'{dest}/Optimizer.log')
            if logfile.exists():
                if logfile.stat().st_size >= 1048576:
                    logfile.replace(f'{str(logfile)}.old')
            log = open(logfile,"a")
            subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbgdir, text=True)

    def add(self, item: OptimizeItem = None):
        if item:
            self.items.append(item)
    
    def move(self, pos: int, direction: str):
        if direction == "up":
            if pos > 0:
                self.items.insert(pos-1, self.items.pop(pos))
                self.save()
                self.load()
        elif direction == "down":
            if pos != len(self.items):
                self.items.insert(pos+1, self.items.pop(pos))
                self.save()
                self.load()

    def load(self):
        p = str(Path(f'{self.dest}/*.json'))
        items = glob.glob(p)
        self.items = []
        for item in items:
            opt_item = OptimizeItem()
            if opt_item.load(item):
                self.add(opt_item)
        self.items = sorted(self.items, key=lambda d: d.position) 

    def save(self):
        for pos, item in enumerate(self.items):
            item.save(pos)
    
    def add_item(self, item: OptimizeItem):
        unique_filename = str(uuid.uuid4())
        item.file = Path(f'{self.dest}/{unique_filename}.json') 
        item.save(len(self.items))
        self.load()
    
    def remove_item(self, item: OptimizeItem):
        item.remove()
        self.load()

    def options(self):
        # Options
        col_run, col_mode, col_cpu, col_best = st.columns([1,1,1,1])
        with col_run:
            if st.toggle("Run Optimizer", value=self.is_running(), key="opt_run", help=None):
                if not self.is_running():
                    self.start()
                    st.experimental_rerun()
            else:
                if self.is_running():
                    self.stop()
                    st.experimental_rerun()
        with col_mode:
            if self.mode == 'linear':
                queue_mode = 0
            else:
                queue_mode = 1
            self.mode = st.radio("Queue Mode", ('linear', 'circular'), index=queue_mode, key="opt_mode", help=None, horizontal=False)
        with col_cpu:
            self.cpu = st.number_input(f'CPU used for Optimizer(1 - {multiprocessing.cpu_count()})', min_value=1, max_value=multiprocessing.cpu_count(), value=self.cpu, step=1)
        with col_best:
            self.backtest_best = st.number_input("backtest_best", min_value=0, max_value=1000, value=self.backtest_best, step=1, format='%d', key="opt_backtest_best", help=pbgui_help.backtest_best)
        col_sharp, col_adg, col_drawdown, col_stuck = st.columns([1,1,1,1])
        with col_sharp:
            self.backtest_sharp = st.number_input("backtest_sharp", min_value=0, max_value=1000, value=self.backtest_sharp, step=1, format='%d', key="opt_backtest_sharp", help=pbgui_help.backtest_sharp)
        with col_adg:
            self.backtest_adg = st.number_input("backtest_adg", min_value=0, max_value=1000, value=self.backtest_adg, step=1, format='%d', key="opt_backtest_adg", help=pbgui_help.backtest_adg)
        with col_drawdown:
            self.backtest_drawdown = st.number_input("backtest_drawdown", min_value=0, max_value=1000, value=self.backtest_drawdown, step=1, format='%d', key="opt_backtest_drawdown", help=pbgui_help.backtest_drawdown)
        with col_stuck:
            self.backtest_stuck = st.number_input("backtest_stuck", min_value=0, max_value=1000, value=self.backtest_stuck, step=1, format='%d', key="opt_backtest_stuck", help=pbgui_help.backtest_stuck)

    def view_log(self, item: OptimizeItem):
        if item.log.exists():
            with open(item.log, "r", encoding='utf-8') as f:
                log = f.read()
                st.code(log)

    def view_queue(self):
        # Init
        self.load()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'editor_opt_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'editor_opt_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if self.items[row].is_running():
                        self.items[row].stop()
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.my_opt = self.items[row]
                    del st.session_state.opt_queue
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "log" in ed["edited_rows"][row]:
                    st.session_state.view_opt_log = self.items[row]
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "up" in ed["edited_rows"][row]:
                    self.move(row, "up")
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "down" in ed["edited_rows"][row]:
                    self.move(row, "down")
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "remove" in ed["edited_rows"][row]:
                    if self.items[row].is_running():
                        self.items[row].stop()
                    self.remove_item(self.items[row])
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
        d = []
        column_config = {
            "Stop": st.column_config.CheckboxColumn('Stop', default=False),
            "Edit": st.column_config.CheckboxColumn('Stop', default=False),
            "log": st.column_config.CheckboxColumn('Log', default=False),
            "up": st.column_config.CheckboxColumn('🔼', default=False),
            "down": st.column_config.CheckboxColumn('🔽', default=False),
            "remove": st.column_config.CheckboxColumn('Remove', default=False),
            }
        for item in self.items:
            d.append({
                'pos': item.position,
                'up': False,
                'down': False,
                'edit': False,
                'user': item.user,
                'symbol': item.symbol,
                'config': item.oc.name,
                'sd': item.sd,
                'ed': item.ed,
                'sb': item.sb,
                'market_type': item.market_type,
                'ohlcv': item.ohlcv,
                'mode': item.oc.passivbot_mode,
                'algo': item.oc.algorithm,
                'iters': item.oc.iters,
                'long': item.oc.do_long,
                'short': item.oc.do_short,
                'log': False,
                'run': item.is_running(),
                'reruns': item.reruns,
                'finish': item.finish,
                'remove': False,
            })
        st.data_editor(data=d, width=None, height=(len(self.items)+1)*36, use_container_width=True, key=f'editor_opt_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['user','symbol'])
        if "view_opt_log" in st.session_state:
            if st.button(f':negative_squared_cross_mark:', key="close_view_opt_log"):
                del st.session_state.view_opt_log
                st.experimental_rerun()
            self.view_log(st.session_state.view_opt_log)

class OptimizeResults:
    def __init__(self):
        self.pbdir = None
        self.layer = 1
        self.almo = None
        self.show_result = None
        self.bt_results = None
        self.l2_paths = []
        self.results = []
        self.results_d = []
        self.hs_rg = []
        self.hs_ng = []
        self.hs_cl = []
        self.ps_rg = []
        self.ps_ng = []
        self.ps_cl = []
        self.symbols = {}
        self.symbol_names = []
        self.initialize()

    def initialize(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("main", "pbdir"):
            self.pbdir = pb_config.get("main", "pbdir")
        if "bt_results" in st.session_state:
            self.bt_results = st.session_state.bt_results
        else:     
            st.session_state.bt_results = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
            self.bt_results = st.session_state.bt_results
    
    def find_results_l1(self):
        p_hs_rg = str(Path(f'{self.pbdir}/results_harmony_search_recursive_grid/**/*_result_*.json'))
        p_hs_ng = str(Path(f'{self.pbdir}/results_harmony_search_neat_grid/**/*_result_*.json'))
        p_hs_cl = str(Path(f'{self.pbdir}/results_harmony_search_clock/**/*_result_*.json'))
        p_ps_rg = str(Path(f'{self.pbdir}/results_particle_swarm_optimization_recursive_grid/**/*_result_*.json'))
        p_ps_ng = str(Path(f'{self.pbdir}/results_particle_swarm_optimization_neat_grid/**/*_result_*.json'))
        p_ps_cl = str(Path(f'{self.pbdir}/results_particle_swarm_optimization_clock/**/*_result_*.json'))
        self.hs_rg = glob.glob(p_hs_rg, recursive=True)
        self.hs_ng = glob.glob(p_hs_ng, recursive=True)
        self.hs_cl = glob.glob(p_hs_cl, recursive=True)
        self.ps_rg = glob.glob(p_ps_rg, recursive=True)
        self.ps_ng = glob.glob(p_ps_ng, recursive=True)
        self.ps_cl = glob.glob(p_ps_cl, recursive=True)
        results = self.hs_rg + self.hs_ng + self.hs_cl + self.ps_rg + self.ps_ng + self.ps_cl
        symbols = {}
        for result in results:
            fullname = PurePath(result).parts[-2].split("_")
            if fullname[-1] == "PERP" or fullname[-1] == "symbols":
                symbol = fullname[-2] + "_" + fullname[-1]
            else:
                symbol = fullname[-1]
            if symbol in symbols:
                symbol_list = symbols[symbol]
            else:
                symbol_list = []
            symbol_list.append(result)
            symbols.update({
                            symbol: symbol_list
                            })
        self.symbols = symbols

    def find_results_l2(self):
        if self.almo == 0:
            p = str(Path(f'{self.pbdir}/results_harmony_search_recursive_grid/*'))
        elif self.almo == 1:
            p = str(Path(f'{self.pbdir}/results_harmony_search_neat_grid/*'))
        elif self.almo == 2:
            p = str(Path(f'{self.pbdir}/results_harmony_search_clock/*'))
        elif self.almo == 3:
            p = str(Path(f'{self.pbdir}/results_particle_swarm_optimization_recursive_grid/*'))
        elif self.almo == 4:
            p = str(Path(f'{self.pbdir}/results_particle_swarm_optimization_neat_grid/*'))
        elif self.almo == 5:
            p = str(Path(f'{self.pbdir}/results_particle_swarm_optimization_clock/*'))
        self.l2_paths = glob.glob(p, recursive=True)
        self.l2_paths.sort(reverse=True)

    def find_results_l3(self):
        if self.results: return
        p = str(Path(f'{self.l3_path}/*_result_*.json'))
        self.results = glob.glob(p, recursive=True)
        self.results.sort(reverse=True)

    def fetch_results(self, path = str):
        file = Path(path)
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    results = json.load(f)
                    symbols = []
                    for symbol in list(results.keys()):
                        symbols.append(symbol)
                    return symbols, results[list(results.keys())[0]]
            except Exception as e:
                print(f'{str(file)} is corrupted {e}')

    def load_result(self, file = str):
        p = Path(file)
        if p.exists():
            with open(p, 'r', encoding='utf-8') as f:
                return f.read()

    def load_config(self, file = str):
        dir = PurePath(file).parent
        result = PurePath(file).name.split('_')[0]
        p = str(Path(f'{dir}/{result}_best_config_*.json'))
        config = glob.glob(p)
        if config:
            config_file = Path(config[0])
            if config_file.exists():
                with open(config_file, 'r', encoding='utf-8') as f:
                    return f.read()

    def remove_results(self, path = str):
        if PurePath(path).name.startswith("results_"):
            paths = glob.glob(f'{path}/*')
            for path in paths:
                rmtree(path, ignore_errors=True)
        else:
            rmtree(path, ignore_errors=True)

    def view_results_l1(self):
        # Init
        self.find_results_l1()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'editor_opt_results_{ed_key}' in st.session_state:
            ed = st.session_state[f'editor_opt_results_{ed_key}']
            for row in ed["edited_rows"]:
                if "View" in ed["edited_rows"][row]:
                    if row > 5:
                        self.almo = row
                        self.layer = 3
                        self.results = self.symbols[list(self.symbols.keys())[row-6]]
                        st.session_state.ed_key += 1
                        st.experimental_rerun()
                    self.layer = 2
                    self.almo = row
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "Remove" in ed["edited_rows"][row]:
                    if row == 0:
                        self.remove_results(f'{self.pbdir}/results_harmony_search_recursive_grid')
                    elif row == 1:
                        self.remove_results(f'{self.pbdir}/results_harmony_search_neat_grid')
                    elif row == 2:
                        self.remove_results(f'{self.pbdir}/results_harmony_search_clock')
                    elif row == 3:
                        self.remove_results(f'{self.pbdir}/results_particle_swarm_optimization_recursive_grid')
                    elif row == 4:
                        self.remove_results(f'{self.pbdir}/results_particle_swarm_optimization_neat_grid')
                    elif row == 5:
                        self.remove_results(f'{self.pbdir}/results_particle_swarm_optimization_clock')
                    elif row > 5:
                        for result in self.symbols[list(self.symbols.keys())[row-6]]:
                            self.remove_results(str(PurePath(result).parent))
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
        d = []
        column_config = {
            "View": st.column_config.CheckboxColumn('View', default=False),
            "Remove": st.column_config.CheckboxColumn('Remove All', default=False),
            }
        d.append({
            'Algorithm / Mode': "Harmony Search / Reverse Grid",
            'Results': len(self.hs_rg),
            'View': False,
            'Remove': False,
        })
        d.append({
            'Algorithm / Mode': "Harmony Search / Neat Grid",
            'Results': len(self.hs_ng),
            'View': False,
            'Remove': False,
        })
        d.append({
            'Algorithm / Mode': "Harmony Search / Clock",
            'Results': len(self.hs_cl),
            'View': False,
            'Remove': False,
        })
        d.append({
            'Algorithm / Mode': "Particle Swarm / Reverse Grid",
            'Results': len(self.ps_rg),
            'View': False,
            'Remove': False,
        })
        d.append({
            'Algorithm / Mode': "Particle Swarm / Neat Grid",
            'Results': len(self.ps_ng),
            'View': False,
            'Remove': False,
        })
        d.append({
            'Algorithm / Mode': "Particle Swarm / Clock",
            'Results': len(self.ps_cl),
            'View': False,
            'Remove': False,
        })
        for symbol in self.symbols:
            d.append({
                'Algorithm / Mode': symbol,
                'Results': len(self.symbols[symbol]),
                'View': False,
                'Remove': False,
            })
        st.data_editor(data=d, width=None, height=(len(d)+1)*36, use_container_width=True, key=f'editor_opt_results_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['Algorithm / Mode','Results'])

    def view_results_l2(self):
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'editor_opt_results_l2_{ed_key}' in st.session_state:
            ed = st.session_state[f'editor_opt_results_l2_{ed_key}']
            for row in ed["edited_rows"]:
                if "View" in ed["edited_rows"][row]:
                    self.layer = 3
                    self.l3_path = self.l2_paths[row]
                    self.results = []
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
                if "Remove" in ed["edited_rows"][row]:
                    self.remove_results(self.l2_paths[row])
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
        d = []
        column_config = {
            "View": st.column_config.CheckboxColumn('View', default=False),
            "Remove": st.column_config.CheckboxColumn('Remove Results', default=False),
            }
        self.find_results_l2()
        for item in self.l2_paths:
            self.l3_path = item
            self.results = []
            self.find_results_l3()
            d.append({
                'Name': item,
                'Results': len(self.results),
                'View': False,
                'Remove': False,
            })
        st.data_editor(data=d, width=None, height=(len(d)+1)*36, use_container_width=True, key=f'editor_opt_results_l2_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['Name'])

    def view_results_l3(self):
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'editor_opt_results_l3_{ed_key}' in st.session_state:
            ed = st.session_state[f'editor_opt_results_l3_{ed_key}']
            for row in ed["edited_rows"]:
                if "Backtest" in ed["edited_rows"][row]:
                    if not self.results_d[row]["Backtest"]:
                        symbols, results = self.fetch_results(self.results_d[row]["path"])
                        config = self.load_config(self.results_d[row]["path"])
                        if config:
                            st.session_state.my_bt = BacktestItem(config)
                            st.session_state.my_bt.symbol = symbols[0]
                            if "bt_queue" in st.session_state:
                                del st.session_state.bt_queue
                            if "bt_view" in st.session_state:
                                del st.session_state.bt_view
                            if "bt_compare" in st.session_state:
                                del st.session_state.bt_compare
                            if "bt_import" in st.session_state:
                                del st.session_state.bt_import
                            switch_page("Backtest")
        column_config = {
            "View": st.column_config.CheckboxColumn('View', default=False),
            "path": None,
            }
        if not self.results_d:
            self.find_results_l3()
            if len(self.results) == 0:
                st.write('### No Results found')
                return
            for item in self.results:
                symbols, results = self.fetch_results(item)
                config = self.load_config(item)
                has_backtest = False
                self.symbol_names = []
                for symbol in symbols:
                    if symbol not in self.symbol_names:
                        self.symbol_names.append(symbol)
                    if self.bt_results.has_backtest(symbol, config):
                        has_backtest = True
                r_dict = {
                        'path': item,
                        'Name': PurePath(item).name,
                        'View': False,
                        'Backtest': has_backtest}
                for r in results:
                    if results["adg_per_exposure_long"] != 0:
                        if r.endswith("_long"):
                            r_dict[r] = results[r]
                    if results["adg_per_exposure_short"] != 0:
                        if r.endswith("_short"):
                            r_dict[r] = results[r]
                self.results_d.append(r_dict)
            st.session_state.ed_key += 1
            st.experimental_rerun()
        symbol_names = ''
        for symbol_name in self.symbol_names:
            symbol_names += "- " + symbol_name + "\n"
        if len(self.symbol_names) > 1:
            st.markdown('#### Symbols:')
            st.markdown(symbol_names)
        else:
            st.markdown(f'#### Symbol: {self.symbol_names[0]}')
        height = round(st.session_state.height*0.65) if st.session_state.height else 1080
        results_d = st.data_editor(data=self.results_d, width=None, height=height, use_container_width=True, key=f'editor_opt_results_l3_{ed_key}', column_config=column_config, disabled=['path'])
        self.bt_results.backtests = []
        for view in results_d:
            if view["View"]:
                st.info(f'{view["Name"]}', icon=None)
                if view["Backtest"]:
                    symbols, results = self.fetch_results(view["path"])
                    config = self.load_config(view["path"])
                    for symbol in symbols:
                        self.bt_results.match_config(symbol, config)
                    for bt in self.bt_results.backtests:
                        if not bt.name:
                            bt.name = view["Name"]
                col_result, col_config = st.columns([1,1]) 
                with col_result:
                    st.code(self.load_result(view["path"]))
                with col_config:
                    st.code(self.load_config(view["path"]))
        for bt in self.bt_results.backtests:
            bt.selected = True
            bt.load_stats()
        self.bt_results.view(only=True)

def main():
    opt = OptimizeQueue()
    while True:
        for item in opt.items:
            while not item.is_finish():
                if not item.is_running():
                    print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Optimizing {item.file} started')
                    item.start(opt.cpu)
                    if opt.mode == "circular":
                        break
                else:
                    time.sleep(1)
        print("sleep 60")
        time.sleep(60)

if __name__ == '__main__':
    main()