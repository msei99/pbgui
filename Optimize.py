import streamlit as st
from Base import Base
from Backtest import BacktestItem, BacktestResults
from OptimizeConfig import OptimizeConfigs, OptimizeConfig
from pathlib import Path, PurePath
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

class OptimizeItem(Base):
    def __init__(self):
        super().__init__()
        self.file = None
        self.log = None
        self.oc = OptimizeConfig()
        self.sd = None
        self.ed = None
        self.sb = None
        self.long_enabled = True
        self.short_enabled = False
        self.mode = "recursive_grid"
        self.algo = "harmony_search"
        self.iters = 10000
        self.reruns = 1
        self.finish = 0
        self.position = None
        self.pbdir = None
        self.initialize()

    def initialize(self):
        self.oc.name = OptimizeConfigs().default()
        self.sd = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.ed = datetime.date.today().strftime("%Y-%m-%d")
        self.sb = 1000
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
                if any(str(self.symbol) in sub for sub in cmdline) and any("optimize.py" in sub for sub in cmdline):
                    return process

    def start(self, cpu: int):
        if not self.is_running():
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if self.pbdir:
                cmd = [sys.executable, '-u', PurePath(f'{self.pbdir}/optimize.py')]
                cmd_end = f'-u {self.user} -s {self.symbol} -i {self.iters} -pm {self.mode} -a {self.algo} -sd {self.sd} -ed {self.ed} -sb {self.sb} -m {self.market_type} -oh {self.ohlcv} -c {cpu} -le {self.long_enabled} -se {self.short_enabled} -oc {self.oc.config_file}'
                cmd.extend(shlex.split(cmd_end))
                if self.long_enabled and not self.short_enabled:
                    cmd_end = f'-le y -se n'
                    cmd.extend(shlex.split(cmd_end))
                if self.short_enabled and not self.long_enabled:
                    cmd_end = f'-le n -se y'
                    cmd.extend(shlex.split(cmd_end))
                if self.short_enabled and self.long_enabled:
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

    def generate_backtest(self):
        if self.long_enabled:
            long = self.find_best("long")
            if long:
                self.add_to_backtest(long)
        if self.short_enabled:
            short = self.find_best("short")
            if short:
                self.add_to_backtest(short)
        if self.short_enabled and self.long_enabled:
            long_short = self.find_best("long_short")
            if long_short:
                self.add_to_backtest(long_short)
    
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

    def find_best(self, sl : str):
        results_fpath = self.fetch_results_fpath()
        if results_fpath and self.pbdir:
            dirs = glob.glob(f'{self.pbdir}/{results_fpath}*_best_*')
            dirs.sort(reverse=True)
            for dir in dirs:
                if dir.endswith('_best_config_long.json') and sl == "long":
                    return dir
                elif dir.endswith('_best_config_short.json') and sl == "short":
                    return dir
                elif dir.endswith('_best_config_long_short.json') and sl == "long_short":
                    return dir
        return None

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
            self.long_enabled = st.toggle("Long enabled", value=self.long_enabled, key="opt_long_enabled", help=None)
            if self.mode == "recursive_grid":
                mode_index = 0
            elif self.mode == "neat_grid":
                mode_index = 1
            else:
                mode_index = 2
            self.mode = st.radio('PASSIVBOT_MODE',('recursive_grid', 'neat_grid', 'clock'), index=mode_index)
        with col_2:
            self.sd = st.date_input("START_DATE", datetime.datetime.strptime(self.sd, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
            self.short_enabled = st.toggle("Short enabled", value=self.short_enabled, key="opt_short_enabled", help=None)
            if self.algo == "harmony_search":
                algo_index = 0
            else:
                algo_index = 1
            self.algo = st.radio("ALGORITHM",('harmony_search', 'particle_swarm_optimization'),index=algo_index)
        with col_3:
            self.ed = st.date_input("END_DATE", datetime.datetime.strptime(self.ed, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
            self.iters = st.number_input('ITERS',value=self.iters,step=1000, help=pbgui_help.opt_iters)
            self.reruns = st.number_input('Reruns',value=self.reruns,step=5, help=pbgui_help.opt_reruns)

    def load(self, file: str):
        self.file = Path(file)
        self.log = Path(f'{self.file}.log')
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
            self.mode = t["mode"]
            self.algo = t["algo"]
            self.iters = t["iters"]
            self.long_enabled = t["long_enabled"]
            self.short_enabled = t["short_enabled"]
            self.reruns = t["reruns"]
            self.finish = t["finish"]
            self.position = t["position"]
            self.oc = OptimizeConfigs().find_config(t["oc"])


    def save(self, pos: int):
        opt_dict = {
            "user": self.user,
            "symbol": self.symbol,
            "sd": self.sd,
            "ed": self.ed,
            "sb": self.sb,
            "market_type": self.market_type,
            "ohlcv": self.ohlcv,
            "mode": self.mode,
            "algo": self.algo,
            "iters": self.iters,
            "long_enabled": self.long_enabled,
            "short_enabled": self.short_enabled,
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
            self.pb_config.set("optimize", "cpu", str(multiprocessing.cpu_count()-2))
            self.pb_config.set("optimize", "mode", "linear")
        self._cpu = int(self.pb_config.get("optimize", "cpu"))
        self._mode = str(self.pb_config.get("optimize", "mode"))
        self.pbgdir = Path.cwd()
        self.dest = Path(f'{self.pbgdir}/data/opt_queue')
        if not self.dest.exists():
            self.dest.mkdir(parents=True)
        self.load()

    @property
    def cpu(self):
        self.pb_config.read('pbgui.ini')
        self._cpu = int(self.pb_config.get("optimize", "cpu"))
        return self._cpu

    @cpu.setter
    def cpu(self, new_cpu):
        if new_cpu != self._cpu:
            self._cpu = new_cpu
            self.pb_config.set("optimize", "cpu", str(self._cpu))
            with open('pbgui.ini', 'w') as f:
                self.pb_config.write(f)
            st.experimental_rerun()

    @property
    def mode(self):
        self.pb_config.read('pbgui.ini')
        self._mode = str(self.pb_config.get("optimize", "mode"))
        return self._mode

    @mode.setter
    def mode(self, new_mode):
        if new_mode != self._mode:
            self._mode = new_mode
            self.pb_config.set("optimize", "mode", str(self._mode))
            with open('pbgui.ini', 'w') as f:
                self.pb_config.write(f)
            st.experimental_rerun()

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
            log = open(Path(f'{dest}/Optimizer.log'),"a")
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
            opt_item.load(item)
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
        col_run, col_mode, col_cpu = st.columns([1,1,1]) 
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
                'mode': item.mode,
                'algo': item.algo,
                'iters': item.iters,
                'long': item.long_enabled,
                'short': item.short_enabled,
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