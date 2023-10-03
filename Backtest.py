import streamlit as st
from bokeh.plotting import figure
from bokeh.palettes import Category20_20
from bokeh.models import NumeralTickFormatter, HoverTool
import json
import psutil
import sys
import subprocess
import shlex
import glob
import configparser
import time
import multiprocessing
import pandas as pd
from pbgui_func import config_pretty_str

from User import Users
from streamlit import experimental_rerun
from pathlib import Path, PurePath
from Exchange import Exchange
from shutil import rmtree
import datetime

class BacktestItem:
    def __init__(self, config: str = None):
        self.config = config
        self.file = None
        self.log = None
        self.user = None
        self.symbol = None
        self.sd = None
        self.ed = None
        self.sb = None
        self.pbdir = None
        self.exchange = None
        self.spot = None
        self.swap = None
        self._market_type = None
        self._config_file = None
        self.initialize()

    @property
    def market_type(self): return self._market_type
    @property
    def config_file(self): return self._config_file

    @market_type.setter
    def market_type(self, new_market_type):
        if self._market_type != new_market_type:
            self._market_type = new_market_type
            st.experimental_rerun()
    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file
            self.load_config()

    def initialize(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.pbdir = pb_config.get("main", "pbdir")
        users = Users(f'{self.pbdir}/api-keys.json')
        self.user = users.default()
        self.symbol = "BTCUSDT"
        self.sd = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.ed = datetime.date.today().strftime("%Y-%m-%d")
        self.sb = 1000
        self._market_type = "futures"
        self.exchange = Exchange(users.find_exchange(self.user))
        self.exchange.load_symbols()
        self.swap = self.exchange.swap
        self.spot = self.exchange.spot

    def load_config(self):
        with open(self.config_file, "r", encoding='utf-8') as f:
            self.config = config_pretty_str(json.load(f))

    def load(self, file: str):
        self.file = Path(file)
        self.config_file = Path(f'{self.file}.cfg')
        self.log = Path(f'{self.file}.log')
        with open(self.file, "r", encoding='utf-8') as f:
            t = json.load(f)
            self.user = t["user"]
            self.symbol = t["symbol"]
            self.sd = t["sd"]
            self.ed = t["ed"]
            self.sb = t["sb"]
            self.market_type = t["market_type"]
        users = Users(f'{self.pbdir}/api-keys.json')
        if self.user not in users.list():
            self.user = users.default()
        self.exchange = Exchange(users.find_exchange(self.user))
        self.exchange.load_symbols()
        if self.market_type == "spot":
            if self.exchange.name not in ['binance', 'bybit']:
                self.market_type = "futures"
            else:
                self.spot = self.exchange.spot
                if self.symbol not in self.spot:
                    self.symbol = "BTCUSDT"
        elif self.market_type == "future":
            self.swap = self.exchange.swap
            if self.symbol not in self.swap:
                self.symbol = "BTCUSDT"

    def save(self):
        pbgdir = Path.cwd()
        dest = Path(f'{pbgdir}/data/bt_queue')
        now = datetime.datetime.now().isoformat(timespec='microseconds')
        if not self.file:
            self.file = Path(f'{dest}/{now}.json') 
        bt_dict = {
            "user": self.user,
            "symbol": self.symbol,
            "sd": self.sd,
            "ed": self.ed,
            "sb": self.sb,
            "market_type": self.market_type,
        }
        if not dest.exists():
            dest.mkdir(parents=True)
        config_file = Path(f'{self.file}.cfg')
        with open(config_file, "w", encoding='utf-8') as f:
            f.write(self.config)
        self.config_file = config_file
        with open(self.file, "w", encoding='utf-8') as f:
            json.dump(bt_dict, f, indent=4)

    def remove(self):
        self.file.unlink(missing_ok=True)
        self.log.unlink(missing_ok=True)
        self.config_file.unlink(missing_ok=True)

    def remove_log(self):
        self.log.unlink(missing_ok=True)

    def load_log(self):
        if self.log.exists():
            with open(self.log, 'r', encoding='utf-8') as f:
                return f.read()

    def status(self):
        if self.is_running():
            return "running"
        if self.is_finish():
            return "complete"
        if self.is_error():
            return "error"
        else:
            return "not started"

    def is_running(self):
        if self.pid():
            return True
        return False

    def is_finish(self):
        log = self.load_log()
        if log:
            if "Summary" in log:
                return True
            else:
                return False
        else:
            return False

    def is_error(self):
        log = self.load_log()
        if log:
            if "Summary" in log:
                return False
            else:
                return True
        else:
            return False

    def stop(self):
        if self.is_running():
            self.pid().kill()

    def pid(self):
        if self.file:
            for process in psutil.process_iter():
                try:
                    cmdline = process.cmdline()
                except psutil.NoSuchProcess:
                    pass
                if any(str(self.file) in sub for sub in cmdline) and any("backtest.py" in sub for sub in cmdline):
                    return process

    def run(self):
        if not self.is_finish() and not self.is_running():
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if pb_config.has_option("main", "pbdir"):
                pbdir = pb_config.get("main", "pbdir")
                cmd = f'{sys.executable} -u {pbdir}/backtest.py -dp -u {self.user} -s {self.symbol} -sd {self.sd} -ed {self.ed} -sb {self.sb} -m {self.market_type} -bd ./backtests/pbgui {str(self.config_file)}'
                log = open(self.log,"w")
                subprocess.Popen(shlex.split(cmd), stdout=log, stderr=log, cwd=pbdir, text=True)

class BacktestQueue:
    def __init__(self):
        self.items = []
        self.pb_config = configparser.ConfigParser()
        self.pb_config.read('pbgui.ini')
        if not self.pb_config.has_section("backtest"):
            self.pb_config.add_section("backtest")
            self.pb_config.set("backtest", "autostart", "False")
            self.pb_config.set("backtest", "cpu", str(multiprocessing.cpu_count()-1))
        self._autostart = eval(self.pb_config.get("backtest", "autostart"))
        self._cpu = int(self.pb_config.get("backtest", "cpu"))
        if self._autostart:
            self.run()

    @property
    def cpu(self):
        return self._cpu

    @cpu.setter
    def cpu(self, new_cpu):
        if new_cpu != self._cpu:
            self._cpu = new_cpu
            self.pb_config.set("backtest", "cpu", str(self._cpu))
            with open('pbgui.ini', 'w') as f:
                self.pb_config.write(f)
            experimental_rerun()

    @property
    def autostart(self):
        return self._autostart

    @autostart.setter
    def autostart(self, new_autostart):
        if new_autostart != self._autostart:
            self._autostart = new_autostart
            self.pb_config.set("backtest", "autostart", str(self._autostart))
            with open('pbgui.ini', 'w') as f:
                self.pb_config.write(f)
            if self._autostart:
                self.run()
            experimental_rerun()

    def add(self, item: BacktestItem = None):
        if item:
            self.items.append(item)

    def running(self):
        r = 0
        for item in self.items:
            if item.is_running():
                r+=1
        return r
        
    def load(self):
        pbgdir = Path.cwd()
        dest = Path(f'{pbgdir}/data/bt_queue')
        p = str(Path(f'{dest}/*.json'))
        items = glob.glob(p)
        for t_item in self.items:
            if not any(str(t_item.file) in sub for sub in items):
                self.items.remove(t_item)
        for item in items:
            bt_item = BacktestItem()
            bt_item.load(item)
            if self.items:
                if not any(str(bt_item.file) in str(sub.file) for sub in self.items):
                    self.add(bt_item)
            else:
                self.add(bt_item)

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = f'{sys.executable} -u {pbgdir}/Backtest.py'
            log = open(f'{pbgdir}/data/bt_queue/Backtest.log',"a")
            subprocess.Popen(shlex.split(cmd), stdout=log, stderr=log, cwd=pbgdir, text=True)

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        for process in psutil.process_iter():
            cmdline = process.cmdline()
            if any("Backtest.py" in sub for sub in cmdline):
                return process

class BacktestResult:
    def __init__(self, backtest_path: str = None):
        self.backtest_path = backtest_path
        self.config = self.load_config()
        self.result = self.load_result()
        self.result_txt = self.load_result_txt()
        self.long = self.result["long"]
        self.short = self.result["short"]
        self.long_enabled = self.result["long"]["enabled"]
        self.short_enabled = self.result["short"]["enabled"]
        self.symbol = self.result["symbol"]
        self.sd = self.result["start_date"]
        self.ed = self.result["end_date"]
        self.sb = self.result["starting_balance"]
        self.exchange = self.result["exchange"]
        self.market_type = self.result["market_type"]
        self.stats = None
        self.selected = False

    def load_config(self):
        r = Path(f'{self.backtest_path}/live_config.json')
        with open(r, "r", encoding='utf-8') as f:
            return f.read()
    def load_result(self):
        r = Path(f'{self.backtest_path}/result.json')
        with open(r, "r", encoding='utf-8') as f:
            return json.load(f)
    def load_result_txt(self):
        r = Path(f'{self.backtest_path}/backtest_result.txt')
        with open(r, "r", encoding='utf-8') as f:
            return f.read()
    def load_stats(self):
        stats = f'{self.backtest_path}/stats.csv'
        self.stats = pd.read_csv(stats)

class BacktestResults:
    def __init__(self, backtest_path: str = None):
        self.backtest_path = backtest_path
        self.backtests = []
        self.symbols = []
        self.exchanges = []

    def remove(self, bt_result: BacktestResult):
        rmtree(bt_result.backtest_path, ignore_errors=True)
        self.backtests.remove(bt_result)

    def view(self, symbols: list = [], exchanges: list = []):
        if self.backtests:
            d = []
            column_config = {
                "Show": st.column_config.CheckboxColumn('Show', default=False),
                "Delete": st.column_config.CheckboxColumn('Delete', default=False),
                }
            for bt in self.backtests:
                if (bt.symbol in symbols or not symbols) and (bt.exchange in exchanges or not exchanges):
                    filename = str(bt.backtest_path).partition(f'{bt.exchange}/')[-1]
                    d.append({
                            'id': self.backtests.index(bt),
                            'Show': bt.selected,
                            'Symbol': bt.symbol,
                            'Exchange': bt.exchange,
                            'Start':  bt.sd,
                            'End': bt.ed,
                            'Balance': bt.sb,
                            'Market': bt.market_type,
                            'LE': bt.long_enabled,
                            'SE': bt.short_enabled,
                            'Name': filename,
                            'Delete': False,
                        }
                    )
            new_bt = st.data_editor(data=d, width=None, height=None, use_container_width=True, hide_index=None, column_order=None, column_config=column_config, disabled=['Symbol','Exchange','Start','End','Balance','Market','LE','SE','Name'])
            if new_bt != d:
                for line in new_bt:
                    if line["Delete"] == True:
                        self.remove(self.backtests[line["id"]])
                        st.experimental_rerun()
                    elif line["Show"] == True:
                        self.backtests[line["id"]].load_stats()
                        self.backtests[line["id"]].selected = True
                    else:
                        self.backtests[line["id"]].selected = False
                st.experimental_rerun()
        else:
            return
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
        hover_we = HoverTool(
            tooltips=[
                ( 'name',   '$name'            ),
                ( 'date',   '@x{%F}'            ),
                ( 'total', '@y{0.00} WE'      ),
            ],

            formatters={
                '@x'           : 'datetime', # use 'datetime' formatter for '@date' field
            },

            # display a tooltip whenever the cursor is vertically in line with a glyph
            mode='mouse'
        )
        be = figure(
            x_axis_label='date',
            y_axis_label='USDT',
            x_axis_type='datetime',
            tools = "pan,box_zoom,wheel_zoom,save,reset",
            active_scroll="wheel_zoom")

        we = figure(
            x_axis_label='time',
            y_axis_label='WE',
            x_axis_type='datetime',
            tools = "pan,box_zoom,wheel_zoom,save,reset",
            active_scroll="wheel_zoom")
        b_long = {}
        e_long = {}
        b_short = {}
        e_short = {}
        we_long = {}
        we_short = {}
        color_b = -2
        color_e = -1
        for idx, bt in enumerate(self.backtests):
            if bt.selected and bt.long_enabled:
                color_b += 2
                color_e += 2
                x = bt.stats["timestamp"]
                b_long[idx] = bt.stats["balance_long"]
                e_long[idx] = bt.stats["equity_long"]
                we_long[idx] = bt.stats["wallet_exposure_long"]
                be.line(x, b_long[idx], legend_label=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} long_balance',color=Category20_20[color_b], line_width=2, name=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} long_balance')
                be.line(x, e_long[idx], legend_label=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} long_equity',color=Category20_20[color_e], line_width=1, name=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} long_equity')
                we.line(x, we_long[idx], legend_label=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} wallet_exposure_long',color=Category20_20[color_b], line_width=1, name=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} wallet_exposure_long')
            if bt.selected and bt.short_enabled:
                x = bt.stats["timestamp"]
                b_short[idx] = bt.stats["balance_short"]
                e_short[idx] = bt.stats["equity_short"]
                we_short[idx] = -abs(bt.stats["wallet_exposure_short"])
                be.line(x, b_short[idx], legend_label=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} short_balance',color=Category20_20[color_b], line_width=2, name=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} short_balance')
                be.line(x, e_short[idx], legend_label=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} short_equity',color=Category20_20[color_e], line_width=1, name=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} short_equity')
                we.line(x, we_short[idx], legend_label=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} wallet_exposure_short',color=Category20_20[color_b], line_width=1, name=f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed} wallet_exposure_short')
        if be.legend:
            be.yaxis[0].formatter = NumeralTickFormatter(format="$ 0")
            be_leg = be.legend[0]
            we_leg = we.legend[0]
            be.add_layout(be_leg,'above')
            we.add_layout(we_leg,'above')
            be.add_tools(hover_be)
            we.add_tools(hover_we)
            be.legend.location = "top_left"
            we.legend.location = "top_left"
            be.legend.click_policy="hide"
            we.legend.click_policy="hide"
            st.bokeh_chart(be, use_container_width=True)
            st.bokeh_chart(we, use_container_width=True)
        idx = 0
        col_r1, col_r2 = st.columns([1,1]) 
        for bt in self.backtests:
            if bt.selected:
                idx +=1
                if idx == 3: idx = 1
                if idx == 1:
                    with col_r1:
                        st.write(f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed}')
                        st.code(bt.result_txt)
                        st.code(bt.config)
                if idx == 2:
                    with col_r2:
                        st.write(f'{self.backtests.index(bt)}: {bt.exchange} {bt.symbol} {bt.sd} {bt.ed}')
                        st.code(bt.result_txt)
                        st.code(bt.config)


    def find_all(self):
        p = str(Path(f'{self.backtest_path}/*/*/plots/*/result.json'))
        found_bt = glob.glob(p, recursive=True)
        if found_bt:
            for p in found_bt:
                bt = BacktestResult(PurePath(p).parent)
                self.backtests.append(bt)
                if bt.symbol not in self.symbols:
                    self.symbols.append(bt.symbol)
                if bt.exchange not in self.exchanges:
                    self.exchanges.append(bt.exchange)

    def match_item(self, item: BacktestItem = None):
        long = json.loads(item.config)["long"]
        short = json.loads(item.config)["short"]
        p = str(Path(f'{self.backtest_path}/{item.exchange.name}/{item.symbol}/plots/*/result.json'))
        found_bt = glob.glob(p, recursive=True)
        if found_bt:
            for p in found_bt:
                bt = BacktestResult(PurePath(p).parent)
                if (
                    item.symbol == bt.symbol
                    and item.sd == bt.sd
                    and item.ed == bt.ed
                    and item.sb == bt.sb
                    and item.market_type == bt.market_type
                    and long == bt.long
                    and short == bt.short
                ):
                    self.backtests.append(bt)
        else:
            st.write("Backtest result not found. Please Run it again")
            item.remove_log()


    def match_config(self, symbol, config: json = None):
        long = json.loads(config)["long"]
        short = json.loads(config)["short"]
        p = str(Path(f'{self.backtest_path}/*/{symbol}/plots/*/result.json'))
        found_bt = glob.glob(p, recursive=True)
        if found_bt:
            for p in found_bt:
                bt = BacktestResult(PurePath(p).parent)
                if symbol == bt.symbol and long == bt.long and short == bt.short:
                    self.backtests.append(bt)
                    if bt.symbol not in self.symbols:
                        self.symbols.append(bt.symbol)
                    if bt.exchange not in self.exchanges:
                        self.exchanges.append(bt.exchange)

def main():
    bt = BacktestQueue()
    while True:
        bt.load()
        for item in bt.items:
            while bt.running() == bt.cpu:
                time.sleep(5)
            bt.pb_config.read('pbgui.ini')
            if not eval(bt.pb_config.get("backtest", "autostart")):
                return
            if item.status() == "not started":
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Backtesting {item.file} started')
                item.run()
        time.sleep(60)

if __name__ == '__main__':
    main()