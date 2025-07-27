import streamlit as st
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
from Exchange import Exchange
from PBCoinData import CoinData
from pbgui_func import pb7dir, pb7venv, PBGDIR, load_symbols_from_ini, error_popup, info_popup, get_navi_paths, replace_special_chars
import uuid
from pathlib import Path, PurePath
from User import Users
import shutil
import datetime
import BacktestV7
from Config import ConfigV7, Bounds
import logging
import os
import fnmatch

class OptimizeV7QueueItem:
    def __init__(self):
        self.name = None
        self.filename = None
        self.json = None
        self.exchange = None
        self.starting_config = False
        self.log = None
        self.pid = None
        self.pidfile = None

    def remove(self):
        self.stop()
        file = Path(f'{PBGDIR}/data/opt_v7_queue/{self.filename}.json')
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
            st.checkbox("Reverse", value=True, key=f'reverse_view_log_{self.filename}')
        with col2:
            st.selectbox("view last kB", [50, 100, 250, 500, 1000, 2000, 5000, 10000, 100000], key=f'size_view_log_{self.filename}')
        with col3:
            if st.button(":material/refresh:", key=f'refresh_view_log_{self.filename}'):
                st.rerun(scope="fragment")
        logfile = self.load_log(st.session_state[f'size_view_log_{self.filename}'])
        if logfile:
            if st.session_state[f'reverse_view_log_{self.filename}']:
                logfile = '\n'.join(logfile.split('\n')[::-1])
        with st.container(height=1200):
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
            if self.pid and psutil.pid_exists(self.pid) and any(sub.lower().endswith("optimize.py") for sub in psutil.Process(self.pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            pass
        return False

    def is_finish(self):
        log = self.load_log(log_size=1000)
        if log:
            if "successfully processed optimize_results" in log or "Optimization complete" in log:
                return True
            else:
                return False
        else:
            return False

    def is_error(self):
        log = self.load_log(log_size=1000)
        if log:
            if "successfully processed optimize_results" in log or "Optimization complete" in log:
                return False
            else:
                return True
        else:
            return False

    def is_optimizing(self):
        if self.is_running():
            log = self.load_log(log_size=1000)
            if log:
                if "Optimization complete" in log:
                    return False
                elif "Initial population size" in log:
                    return True
            else:
                return False

    def stop(self):
        if self.is_running():
            parent = psutil.Process(self.pid)
            children = parent.children(recursive=True)
            children.append(parent)
            for p in children:
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass


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
            if self.starting_config:
                cmd = [pb7venv(), '-u', PurePath(f'{pb7dir()}/src/optimize.py'), '-t', str(PurePath(f'{self.json}')), str(PurePath(f'{self.json}'))]
            else:
                cmd = [pb7venv(), '-u', PurePath(f'{pb7dir()}/src/optimize.py'), str(PurePath(f'{self.json}'))]
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

class OptimizeV7Queue:
    def __init__(self):
        self.items = []
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("optimize_v7"):
            pb_config.add_section("optimize_v7")
            pb_config.set("optimize_v7", "autostart", "False")
            with open('pbgui.ini', 'w') as f:
                pb_config.write(f)
        self._autostart = eval(pb_config.get("optimize_v7", "autostart"))
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
        pb_config.set("optimize_v7", "autostart", str(self._autostart))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)
        if self._autostart:
            self.run()
        else:
            self.stop()

    def add(self, qitem : OptimizeV7QueueItem):
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
        ed = st.session_state[f'view_opt_v7_queue_{ed_key}']
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
        dest = Path(f'{PBGDIR}/data/opt_v7_queue')
        p = str(Path(f'{dest}/*.json'))
        items = glob.glob(p)
        self.items = []
        for item in items:
            with open(item, "r", encoding='utf-8') as f:
                q_config = json.load(f)
                qitem = OptimizeV7QueueItem()
                qitem.name = q_config["name"]
                qitem.filename = q_config["filename"]
                qitem.json = q_config["json"]
                config = OptimizeV7Item(qitem.json)
                qitem.exchange = q_config["exchange"]
                qitem.starting_config = config.config.pbgui.starting_config
                qitem.log = Path(f'{PBGDIR}/data/opt_v7_queue/{qitem.filename}.log')
                qitem.pidfile = Path(f'{PBGDIR}/data/opt_v7_queue/{qitem.filename}.pid')
                self.add(qitem)

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/OptimizeV7.py')]
            dest = Path(f'{PBGDIR}/data/logs')
            if not dest.exists():
                dest.mkdir(parents=True)
            logfile = Path(f'{dest}/OptimizeV7.log')
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
            if any("OptimizeV7.py" in sub for sub in cmdline):
                return process

    def view(self):
        if not self.items:
            self.load()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'view_opt_v7_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_opt_v7_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if self.items[row].is_running():
                        self.items[row].stop()
                    else:
                        self.items[row].run()
                if "view" in ed["edited_rows"][row]:
                    opt = OptimizeV7Item(f'{PBGDIR}/data/opt_v7/{self.items[row].name}')
                    opt.load()
                    opt.load_results()
                    st.session_state.opt_v7_results = opt
                    del st.session_state.opt_v7_queue
                    st.rerun()
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.opt_v7 = OptimizeV7Item(f'{PBGDIR}/data/opt_v7/{self.items[row].name}.json')
                    del st.session_state.opt_v7_queue
                    st.rerun()
        d = []
        for id, opt in enumerate(self.items):
            if type(opt.exchange) != list:
                opt.exchange = [opt.exchange]
            d.append({
                'id': id,
                'run': False,
                'Status': opt.status(),
                'edit': False,
                'log': False,
                'delete': False,
                'starting_config': opt.starting_config,
                'name': opt.name,
                'filename': opt.filename,
                'Time': datetime.datetime.fromtimestamp(Path(f'{PBGDIR}/data/opt_v7_queue/{opt.filename}.json').stat().st_mtime),
                'exchange': opt.exchange,
                'finish': opt.is_finish(),
            })
        column_config = {
            # "id": None,
            "run": st.column_config.CheckboxColumn('Start/Stop', default=False),
            "edit": st.column_config.CheckboxColumn('Edit'),
            "log": st.column_config.CheckboxColumn(label="View Logfile"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display Queue
        height = 36+(len(d))*35
        if height > 1000: height = 1016
        st.data_editor(data=d, height=height, use_container_width=True, key=f'view_opt_v7_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','filename','starting_config','name','finish','running'])
        if f'view_opt_v7_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_opt_v7_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "log" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["log"]:
                        self.items[row].view_log()

class OptimizeV7Results:
    def __init__(self):
        self.results_path = Path(f'{pb7dir()}/optimize_results')
        self.analysis_path = Path(f'{pb7dir()}/optimize_results_analysis')
        self.selected_analysis = "analyses_combined"
        self.results = []
        self.results_new = []
        self.paretos = []
        self.filter = ""
        self.initialize()
    
    def initialize(self):
        self.find_results()
    
    def remove(self, file_name):
        Path(file_name).unlink(missing_ok=True)
        Path(f'{file_name}.bak').unlink(missing_ok=True)
        analysis = PurePath(file_name).stem[0:19]
        analysis = str(self.analysis_path) + f'/{analysis}*.json'
        analysis = glob.glob(analysis, recursive=False)
        if analysis:
            for a in analysis:
                Path(a).unlink(missing_ok=True)

    def find_results(self):
        self.results = []
        if self.results_path.exists():
            p = str(self.results_path) + "/*.txt"
            self.results = glob.glob(p, recursive=False)
            p = str(self.results_path) + "/*/all_results.bin"
            self.results_new = glob.glob(p, recursive=False)
            if "opt_v7_results_d_new" in st.session_state:
                del st.session_state.opt_v7_results_d_new
            if "opt_v7_results_d" in st.session_state:
                del st.session_state.opt_v7_results_d
    
    def find_result_name(self, result_file):
        with open(result_file, "r", encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line:
                return "Empty Result"
            try:
                config = json.loads(first_line)
            except Exception as e:
                return "Corrupt Result"
            if "config" not in config:
                if "backtest" not in config:
                    return "Corrupt Result"
                else:
                    backtest_name = config["backtest"]["base_dir"].split("/")[-1]
                    return backtest_name
            else:
                backtest_name = config["config"]["backtest"]["base_dir"].split("/")[-1]
                return backtest_name
    
    def find_result_name_new(self, result_file):
        p = str(PurePath(result_file).parent / "pareto" / "*.json")
        files = glob.glob(p, recursive=False)
        if files:
            config = ConfigV7(files[0])
            config.load_config()
            result_time = Path(files[0]).stat().st_mtime
            return config.backtest.base_dir.split("/")[-1], result_time
        else:
            return None, None

    def view_analysis(self, analysis):
        file = Path(f'{self.analysis_path}/{analysis}.json')
        with open(file, "r", encoding='utf-8') as f:
            config = json.load(f)
            st.code(json.dumps(config, indent=4))

    def view_results(self):
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key

        if "select_opt_v7_result_filter" in st.session_state:
            if st.session_state.select_opt_v7_result_filter != self.filter:
                self.filter = st.session_state.select_opt_v7_result_filter
                self.results = []
                self.results_new = []
                del st.session_state.opt_v7_results_d
                del st.session_state.opt_v7_results_d_new
                self.find_results()
        else:
            st.session_state.select_opt_v7_result_filter = self.filter

        # Remove results that are not in the filter
        if not self.filter == "":
            for result in self.results.copy():
                name = self.find_result_name(result)
                if not fnmatch.fnmatch(name.lower(), self.filter.lower()):
                    self.results.remove(result)
            for result in self.results_new.copy():
                name, result_time = self.find_result_name_new(result)
                if not fnmatch.fnmatch(name.lower(), self.filter.lower()):
                    self.results_new.remove(result)

        st.text_input("Filter by Optimize Name", value="", help=pbgui_help.smart_filter, key="select_opt_v7_result_filter")

        if not "opt_v7_results_d_new" in st.session_state:
            d = []
            for id, opt in enumerate(self.results_new):
                name, result_time = self.find_result_name_new(opt)
                if name:
                    result = PurePath(opt).parent.name
                    d.append({
                        'id': id,
                        'Name': name,
                        'Result Time': datetime.datetime.fromtimestamp(result_time),
                        'view': False,
                        '3d plot': False,
                        'delete' : False,
                        'Result': result,
                        'index': opt,
                    })
            st.session_state.opt_v7_results_d_new = d
        d_new = st.session_state.opt_v7_results_d_new
        column_config_new = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Paretos"),
            "Result Time": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            "Result": st.column_config.TextColumn(label="Result Directory", width="50px"),
        }
        #Display optimizes
        st.data_editor(data=d_new, height=36+(len(d_new))*35, use_container_width=True, key=f'select_optresults_new_{st.session_state.ed_key}', hide_index=None, column_order=None, column_config=column_config_new, disabled=['id','name','index'])
        if f'select_optresults_new_{st.session_state.ed_key}' in st.session_state:
            ed = st.session_state[f'select_optresults_new_{st.session_state.ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        if d_new[row]["Result"]:
                            st.session_state.opt_v7_pareto = d_new[row]["index"]
                            st.session_state.opt_v7_pareto_name = d_new[row]["Name"]
                            st.session_state.opt_v7_pareto_directory = d_new[row]["Result"]
                            st.rerun()
                if "3d plot" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["3d plot"]:
                        self.run_3d_plot(d_new[row]["index"])
                        st.session_state.ed_key += 1
        
        if not "opt_v7_results_d" in st.session_state:
            d = []
            for id, opt in enumerate(self.results):
                name = self.find_result_name(opt)
                opt_item = OptimizeV7Item(name)
                name = self.find_result_name(opt)
                analysis = PurePath(opt).stem[0:19]
                analysis = str(self.analysis_path) + f'/{analysis}*.json'
                analysis = glob.glob(analysis, recursive=False)
                if analysis:
                    # find newest analysis file
                    analysis_time = 0
                    for a in analysis.copy():
                        mtime = Path(a).stat().st_mtime
                        if mtime > analysis_time:
                            analysis_time = mtime
                            analysis = PurePath(a).stem
                else:
                    analysis = None
                result = PurePath(opt).stem
                result_time = Path(opt).stat().st_mtime
                d.append({
                    'id': id,
                    'Name': name,
                    'Result Time': datetime.datetime.fromtimestamp(result_time),
                    'BT Count': opt_item.backtest_count,
                    'view': False,
                    "generate": False,
                    'backtest': False,
                    'delete' : False,
                    'Result': result,
                    'Analysis': analysis,
                    'Analysis Time': datetime.datetime.fromtimestamp(analysis_time) if analysis else None,
                })
            st.session_state.opt_v7_results_d = d
        d = st.session_state.opt_v7_results_d
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Analysis"),
            "generate": st.column_config.CheckboxColumn(label="Generate Analysis"),
            "backtest": st.column_config.CheckboxColumn(label="Backtest"),
            "Result Time": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            "Analysis Time": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            "Analysis": st.column_config.TextColumn(label="Analysis File", width="50px"),
            "Result": st.column_config.TextColumn(label="Result File", width="50px"),
            }
        #Display optimizes
        st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_optresults_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])
        if f'select_optresults_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_optresults_{ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        if d[row]["Analysis"]:
                            self.view_analysis(d[row]["Analysis"])
                if "generate" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["generate"]:
                        result_name = PurePath(f'{self.results_path}/{d[row]["Result"]}.txt')
                        self.generate_analysis(result_name)
                        st.session_state.ed_key += 1
                        if "opt_v7_results_d" in st.session_state:
                            del st.session_state.opt_v7_results_d
                        # st.rerun()
                if "backtest" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["backtest"]:
                        backtest_name = PurePath(f'{self.analysis_path}/{d[row]["Analysis"]}.json')
                        st.session_state.bt_v7 = BacktestV7.BacktestV7Item(backtest_name)
                        if "bt_v7_queue" in st.session_state:
                            del st.session_state.bt_v7_queue
                        if "bt_v7_results" in st.session_state:
                            del st.session_state.bt_v7_results
                        if "bt_v7_edit_symbol" in st.session_state:
                            del st.session_state.bt_v7_edit_symbol
                        st.switch_page(get_navi_paths()["V7_BACKTEST"])

    def run_3d_plot(self, index):
        # run 3d plot
        directory = Path(index).parent / "pareto"
        cmd = [pb7venv(), '-u', PurePath(f'{pb7dir()}/src/pareto_store.py'), str(directory)]
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NO_WINDOW
            result = subprocess.run(cmd, capture_output=True, cwd=pb7dir(), text=True, creationflags=creationflags)
        else:
            result = subprocess.run(cmd, capture_output=True, cwd=pb7dir(), text=True, start_new_session=True)
        info_popup(f"3D Plot Generated {result.stdout}")

    def load_paretos(self, index):
        self.paretos = []
        paretos_path = PurePath(f'{index}').parent / "pareto"
        if Path(paretos_path).exists():
            # find all json files in paretos_path
            p = str(paretos_path) + "/*.json"
            paretos = glob.glob(p, recursive=False)
        for pareto in paretos:
            with open(pareto, "r", encoding='utf-8') as f:
                pareto_data = json.load(f)
                pareto_data['index_filename'] = pareto
                self.paretos.append(pareto_data)
        # sort by index_filename
        self.paretos.sort(key=lambda x: x['index_filename'])

    def view_pareto(self, index):
        if not self.paretos:
            self.load_paretos(index)
        select_analysis = []
        if "analyses_combined" in self.paretos[0]:
            select_analysis.append("analyses_combined")
        if "analyses" in self.paretos[0]:
            for analyse in self.paretos[0]["analyses"]:
                select_analysis.append(analyse)
        def clear_paretos():
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
        col1, col2 = st.columns([1, 3], gap="small")
        with col1:
            if "opt_v7_pareto_select_analysis" in st.session_state:
                if st.session_state.opt_v7_pareto_select_analysis != self.selected_analysis:
                    self.selected_analysis = st.session_state.opt_v7_pareto_select_analysis
            else:
                st.session_state.opt_v7_pareto_select_analysis = self.selected_analysis
            st.selectbox('analyses', options=select_analysis, key="opt_v7_pareto_select_analysis", on_change=clear_paretos)
        if not "d_paretos" in st.session_state:
            d = []
            for id, pareto in enumerate(self.paretos):
                if select_analysis:
                    name = pareto["index_filename"].split("/")[-1]
                    if st.session_state.opt_v7_pareto_select_analysis == "analyses_combined":
                        analysis = pareto["analyses_combined"]
                        d.append({
                            'Select': False,
                            'id': id,
                            'view': False,
                            'adg': analysis["adg_max"],
                            'mdg': analysis["mdg_max"],
                            'drawdown_worst': analysis["drawdown_worst_max"],
                            'gain': analysis["gain_max"],
                            'loss_profit_ratio': analysis["loss_profit_ratio_max"],
                            'position_held_hours_max': analysis["position_held_hours_max_max"],
                            'sharpe_ratio': analysis["sharpe_ratio_max"],
                            'Name': name,
                            'file': pareto["index_filename"],
                        })
                    else:
                        analysis = pareto["analyses"][st.session_state.opt_v7_pareto_select_analysis]
                        d.append({
                            'Select': False,
                            'id': id,
                            'view': False,
                            'adg': analysis["adg"],
                            'mdg': analysis["mdg"],
                            'drawdown_worst': analysis["drawdown_worst"],
                            'gain': analysis["gain"],
                            'loss_profit_ratio': analysis["loss_profit_ratio"],
                            'position_held_hours_max': analysis["position_held_hours_max"],
                            'sharpe_ratio': analysis["sharpe_ratio"],
                            'Name': name,
                            'file': pareto["index_filename"],
                        })
            st.session_state.d_paretos = d
        d_paretos = st.session_state.d_paretos
        column_config = {
            "id": None,
            "Select": st.column_config.CheckboxColumn(label="Select"),
            "file": None,
            "view": st.column_config.CheckboxColumn(label="View"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            }
        #Display paretos
        height = 36+(len(d_paretos))*35
        if height > 1000: height = 1016
        st.data_editor(data=d_paretos, height=height, use_container_width=True, key=f'select_paretos_{st.session_state.ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','file'])
        if f'select_paretos_{st.session_state.ed_key}' in st.session_state:
            ed = st.session_state[f'select_paretos_{st.session_state.ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        st.write(f"Pareto {d_paretos[row]['Name']}")
                        st.code(json.dumps(self.paretos[row], indent=4))

    def cleanup_bt_session_state(self):
        if "bt_v7_queue" in st.session_state:
            del st.session_state.bt_v7_queue
        if "bt_v7_results" in st.session_state:
            del st.session_state.bt_v7_results
        if "bt_v7_edit_symbol" in st.session_state:
            del st.session_state.bt_v7_edit_symbol
        if "config_v7_archives" in st.session_state:
            del st.session_state.config_v7_archives
        if "config_v7_config_archive" in st.session_state:
            del st.session_state.config_v7_config_archive

    def backtest_selected(self):
        if "d_paretos" in st.session_state:
            d_paretos = st.session_state.d_paretos
        else:
            return
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_paretos_{st.session_state.ed_key}']
        # Get number of selected paretos
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        print(selected_count)
        if selected_count == 0:
            error_popup("No paretos selected")
            return
        self.cleanup_bt_session_state()
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    backtest_name = d_paretos[row]["file"]
                    # run backtest on selected pareto
                    if selected_count == 1:
                        st.session_state.bt_v7 = BacktestV7.BacktestV7Item(backtest_name)
                        st.switch_page(get_navi_paths()["V7_BACKTEST"])
                    else:
                        bt_v7 = BacktestV7.BacktestV7Item(backtest_name)
                        bt_v7.save_queue()
        st.session_state.bt_v7_queue = BacktestV7.BacktestV7Queue()
        st.switch_page(get_navi_paths()["V7_BACKTEST"])
    
    def backtest_all(self):
        if "d_paretos" in st.session_state:
            d_paretos = st.session_state.d_paretos
        else:
            return
        for row in range(len(d_paretos)):
            backtest_name = d_paretos[row]["file"]
            # run backtest on selected pareto
            bt_v7 = BacktestV7.BacktestV7Item(backtest_name)
            bt_v7.save_queue()
        if "bt_v7_results" in st.session_state:
            del st.session_state.bt_v7_results
        if "bt_v7_edit_symbol" in st.session_state:
            del st.session_state.bt_v7_edit_symbol
        st.session_state.bt_v7_queue = BacktestV7.BacktestV7Queue()
        st.switch_page(get_navi_paths()["V7_BACKTEST"])


    def generate_analysis(self, result_file):
        # create a copy of result_file
        result_file = Path(result_file)
        result_file_copy = Path(f'{result_file}.bak')
        shutil.copy(result_file, result_file_copy)
        # run extract_best_config.py on result_file_copy
        cmd = [pb7venv(), '-u', PurePath(f'{pb7dir()}/src/tools/extract_best_config.py'), str(result_file_copy)]
        with st.spinner('Generating Result...'):
            if platform.system() == "Windows":
                creationflags = subprocess.CREATE_NO_WINDOW
                result = subprocess.run(cmd, capture_output=True, cwd=pb7dir(), text=True, creationflags=creationflags)
            else:
                result = subprocess.run(cmd, capture_output=True, cwd=pb7dir(), text=True, start_new_session=True)
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
        ed = st.session_state[f'select_optresults_new_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    directory = Path(self.results_new[row]).parent
                    shutil.rmtree(directory, ignore_errors=True)
        self.results = []
        self.results_new = []
        self.find_results()

    def remove_all_results(self):
        shutil.rmtree(self.results_path, ignore_errors=True)
        shutil.rmtree(self.analysis_path, ignore_errors=True)
        self.results = []
        self.results_new = []

class OptimizeV7Item:
    def __init__(self, optimize_file: str = None):
        self.name = ""
        self.log = None
        self.config = ConfigV7()
        self.users = Users()
        self.backtest_count:int = 0
        if optimize_file:
            self.name = PurePath(optimize_file).stem
            self.config.config_file = optimize_file
            self.config.load_config()
            if Path(optimize_file).exists():
                self.time = datetime.datetime.fromtimestamp(Path(optimize_file).stat().st_mtime)
            else:
                self.time = datetime.datetime.now()
        else:
            self.initialize()
        self._calculate_results()
        if "limits_data" in st.session_state:
            del st.session_state.limits_data

    def _calculate_results(self):
        if self.name:
            base_path = Path(f'{pb7dir()}/backtests/pbgui/{self.name}')
            p = str(Path(f'{base_path}/**/analysis.json'))
            files = glob.glob(p, recursive=True)
            self.backtest_count = len(files)
            
    def initialize(self):
        self.config.backtest.start_date = (datetime.date.today() - datetime.timedelta(days=365*4)).strftime("%Y-%m-%d")
        self.config.backtest.end_date = datetime.date.today().strftime("%Y-%m-%d")
        self.config.optimize.n_cpus = multiprocessing.cpu_count()
    
    def find_presets(self):
        dest = Path(f'{PBGDIR}/data/opt_v7_presets')
        p = str(Path(f'{dest}/*.json'))
        presets = glob.glob(p)
        presets = [PurePath(p).stem for p in presets]
        return presets
    
    def preset_load(self, preset):
        dest = Path(f'{PBGDIR}/data/opt_v7_presets')
        file = Path(f'{dest}/{preset}.json')
        self.config = ConfigV7()
        self.config.config_file = file
        self.config.load_config()
        self.name = PurePath(self.config.config_file).stem
        
        if "edit_opt_v7_name" in st.session_state:
            st.session_state.edit_opt_v7_name = self.name
        
    def preset_save(self) -> bool:
        if self.name == "":
            error_popup("Name is empty")
            return False
        
        dest = Path(f'{PBGDIR}/data/opt_v7_presets')
        if not dest.exists():
            dest.mkdir(parents=True)
        
        # Prevent creating directories with / in the name
        self.name = self.name.replace("/", "_")
        
        file = Path(f'{dest}/{self.name}.json')   
        self.config.config_file = file
        self.config.save_config()
        return True
    
    def preset_remove(self, preset):
        dest = Path(f'{PBGDIR}/data/opt_v7_presets')
        file = Path(f'{dest}/{preset}.json')
        file.unlink(missing_ok=True)
                
    # Exchanges
    @st.fragment
    def fragment_exchanges(self):
        if "edit_opt_v7_exchanges" in st.session_state:
            if st.session_state.edit_opt_v7_exchanges != self.config.backtest.exchanges:
                self.config.backtest.exchanges = st.session_state.edit_opt_v7_exchanges
                st.rerun()
        else:
            st.session_state.edit_opt_v7_exchanges = self.config.backtest.exchanges
        st.multiselect('Exchanges',["binance", "bybit", "gateio", "bitget"], key="edit_opt_v7_exchanges")

    # name
    @st.fragment
    def fragment_name(self):
        if "edit_opt_v7_name" in st.session_state:
            if st.session_state.edit_opt_v7_name != self.name:
                # Avoid creation of unwanted subfolders
                st.session_state.edit_opt_v7_name = replace_special_chars(st.session_state.edit_opt_v7_name)
                self.name = st.session_state.edit_opt_v7_name
                self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
        else:
            st.session_state.edit_opt_v7_name = self.name
        if not self.name:
            st.text_input(f":red[Optimize Name]",max_chars=64, key="edit_opt_v7_name")
        else:
            st.text_input("Optimize Name", max_chars=64, help=pbgui_help.task_name, key="edit_opt_v7_name")

    # start_data
    @st.fragment
    def fragment_start_date(self):
        if "edit_opt_v7_start_date" in st.session_state:
            if st.session_state.edit_opt_v7_start_date.strftime("%Y-%m-%d") != self.config.backtest.start_date:
                self.config.backtest.start_date = st.session_state.edit_opt_v7_start_date.strftime("%Y-%m-%d")
        else:
            st.session_state.edit_opt_v7_start_date = datetime.datetime.strptime(self.config.backtest.start_date, '%Y-%m-%d')
        st.date_input("start_date", format="YYYY-MM-DD", key="edit_opt_v7_start_date")

    # end_date
    @st.fragment
    def fragment_end_date(self):
        if "edit_opt_v7_end_date" in st.session_state:
            if st.session_state.edit_opt_v7_end_date.strftime("%Y-%m-%d") != self.config.backtest.end_date:
                self.config.backtest.end_date = st.session_state.edit_opt_v7_end_date.strftime("%Y-%m-%d")
        else:
            st.session_state.edit_opt_v7_end_date = datetime.datetime.strptime(self.config.backtest.end_date, '%Y-%m-%d')
        st.date_input("end_date", format="YYYY-MM-DD", key="edit_opt_v7_end_date")

    # starting_balance
    @st.fragment
    def fragment_starting_balance(self):
        if "edit_opt_v7_starting_balance" in st.session_state:
            if st.session_state.edit_opt_v7_starting_balance != self.config.backtest.starting_balance:
                self.config.backtest.starting_balance = st.session_state.edit_opt_v7_starting_balance
        else:
            st.session_state.edit_opt_v7_starting_balance = float(self.config.backtest.starting_balance)
        st.number_input("starting_balance", step=500.0, key="edit_opt_v7_starting_balance")

    # iters
    @st.fragment
    def fragment_iters(self):
        if "edit_opt_v7_iters" in st.session_state:
            if st.session_state.edit_opt_v7_iters != self.config.optimize.iters:
                self.config.optimize.iters = st.session_state.edit_opt_v7_iters
        else:
            st.session_state.edit_opt_v7_iters = self.config.optimize.iters
        st.number_input("iters", step=1000, key="edit_opt_v7_iters", help=pbgui_help.opt_iters)

    # n_cpus
    @st.fragment
    def fragment_n_cpus(self):
        if "edit_opt_v7_n_cpus" in st.session_state:
            if st.session_state.edit_opt_v7_n_cpus != self.config.optimize.n_cpus:
                self.config.optimize.n_cpus = st.session_state.edit_opt_v7_n_cpus
        else:
            st.session_state.edit_opt_v7_n_cpus = self.config.optimize.n_cpus
        st.number_input("n_cpus", min_value=1, max_value=multiprocessing.cpu_count(), step=1, key="edit_opt_v7_n_cpus")

    # starting_config
    @st.fragment
    def fragment_starting_config(self):
        if "edit_opt_v7_starting_config" in st.session_state:
            if st.session_state.edit_opt_v7_starting_config != self.config.pbgui.starting_config:
                self.config.pbgui.starting_config = st.session_state.edit_opt_v7_starting_config
        else:
            st.session_state.edit_opt_v7_starting_config = self.config.pbgui.starting_config
        st.checkbox("starting_config", key="edit_opt_v7_starting_config", help=pbgui_help.starting_config)

    # combine_ohlcvs
    @st.fragment
    def fragment_combine_ohlcvs(self):
        if "edit_opt_v7_combine_ohlcvs" in st.session_state:
            if st.session_state.edit_opt_v7_combine_ohlcvs != self.config.backtest.combine_ohlcvs:
                self.config.backtest.combine_ohlcvs = st.session_state.edit_opt_v7_combine_ohlcvs
        else:
            st.session_state.edit_opt_v7_combine_ohlcvs = self.config.backtest.combine_ohlcvs
        st.checkbox("combine_ohlcvs", key="edit_opt_v7_combine_ohlcvs", help=pbgui_help.combine_ohlcvs)

    # compress_results_file
    @st.fragment
    def fragment_compress_results_file(self):
        if "edit_opt_v7_compress_results_file" in st.session_state:
            if st.session_state.edit_opt_v7_compress_results_file != self.config.optimize.compress_results_file:
                self.config.optimize.compress_results_file = st.session_state.edit_opt_v7_compress_results_file
        else:
            st.session_state.edit_opt_v7_compress_results_file = self.config.optimize.compress_results_file
        st.checkbox("compress_results_file", key="edit_opt_v7_compress_results_file", help=pbgui_help.compress_results_file)
    
    # use_btc_collateral
    @st.fragment
    def fragment_use_btc_collateral(self):
        if "edit_opt_v7_use_btc_collateral" in st.session_state:
            if st.session_state.edit_opt_v7_use_btc_collateral != self.config.backtest.use_btc_collateral:
                self.config.backtest.use_btc_collateral = st.session_state.edit_opt_v7_use_btc_collateral
        else:
            st.session_state.edit_opt_v7_use_btc_collateral = self.config.backtest.use_btc_collateral
        st.checkbox("use_btc_collateral", key="edit_opt_v7_use_btc_collateral", help=pbgui_help.use_btc_collateral)

    # mutation_probability
    @st.fragment
    def fragment_mutation_probability(self):
        if "edit_opt_v7_mutation_probability" in st.session_state:
            if st.session_state.edit_opt_v7_mutation_probability != self.config.optimize.mutation_probability:
                self.config.optimize.mutation_probability = st.session_state.edit_opt_v7_mutation_probability
        else:
            st.session_state.edit_opt_v7_mutation_probability = self.config.optimize.mutation_probability
        st.number_input("mutation_probability", min_value=0.0, max_value=1.0, step=0.01, format="%.2f", key="edit_opt_v7_mutation_probability", help=pbgui_help.mutation_probability)

    # population_size
    @st.fragment
    def fragment_population_size(self):
        if "edit_opt_v7_population_size" in st.session_state:
            if st.session_state.edit_opt_v7_population_size != self.config.optimize.population_size:
                self.config.optimize.population_size = st.session_state.edit_opt_v7_population_size
        else:
            st.session_state.edit_opt_v7_population_size = self.config.optimize.population_size
        st.number_input("population_size", min_value=1, max_value=10000, step=1, format="%d", key="edit_opt_v7_population_size", help=pbgui_help.population_size)

    # crossover_probability
    @st.fragment
    def fragment_crossover_probability(self):
        if "edit_opt_v7_crossover_probability" in st.session_state:
            if st.session_state.edit_opt_v7_crossover_probability != self.config.optimize.crossover_probability:
                self.config.optimize.crossover_probability = st.session_state.edit_opt_v7_crossover_probability
        else:
            st.session_state.edit_opt_v7_crossover_probability = self.config.optimize.crossover_probability
        st.number_input("crossover_probability", min_value=0.0, max_value=1.0, step=0.01, format="%.2f", key="edit_opt_v7_crossover_probability", help=pbgui_help.crossover_probability)
    
    # scoring
    @st.fragment
    def fragment_scoring(self):
        if "edit_opt_v7_scoring" in st.session_state:
            if st.session_state.edit_opt_v7_scoring != self.config.optimize.scoring:
                self.config.optimize.scoring = st.session_state.edit_opt_v7_scoring
        else:
            st.session_state.edit_opt_v7_scoring = self.config.optimize.scoring
        st.multiselect(
            "scoring", 
            [
            "adg",
            "adg_w",
            "btc_adg",
            "btc_adg_w",
            "btc_calmar_ratio",
            "btc_calmar_ratio_w",
            "btc_drawdown_worst",
            "btc_drawdown_worst_mean_1pct",
            "btc_equity_balance_diff_neg_max",
            "btc_equity_balance_diff_neg_mean",
            "btc_equity_balance_diff_pos_max",
            "btc_equity_balance_diff_pos_mean",
            "btc_equity_choppiness",
            "btc_equity_choppiness_w",
            "btc_equity_jerkiness",
            "btc_equity_jerkiness_w",
            "btc_expected_shortfall_1pct",
            "btc_exponential_fit_error",
            "btc_exponential_fit_error_w",
            "btc_gain",
            "btc_loss_profit_ratio",
            "btc_loss_profit_ratio_w",
            "btc_mdg",
            "btc_mdg_w",
            "btc_omega_ratio",
            "btc_omega_ratio_w",
            "btc_sharpe_ratio",
            "btc_sharpe_ratio_w",
            "btc_sortino_ratio",
            "btc_sortino_ratio_w",
            "btc_sterling_ratio",
            "btc_sterling_ratio_w",
            "calmar_ratio",
            "calmar_ratio_w",
            "drawdown_worst",
            "drawdown_worst_mean_1pct",
            "equity_balance_diff_neg_max",
            "equity_balance_diff_neg_mean",
            "equity_balance_diff_pos_max",
            "equity_balance_diff_pos_mean",
            "equity_choppiness",
            "equity_choppiness_w",
            "equity_jerkiness",
            "equity_jerkiness_w",
            "expected_shortfall_1pct",
            "exponential_fit_error",
            "exponential_fit_error_w",
            "gain",
            "loss_profit_ratio",
            "loss_profit_ratio_w",
            "mdg",
            "mdg_w",
            "omega_ratio",
            "omega_ratio_w",
            "position_held_hours_max",
            "position_held_hours_mean",
            "position_held_hours_median",
            "position_unchanged_hours_max",
            "positions_held_per_day",
            "sharpe_ratio",
            "sharpe_ratio_w",
            "sortino_ratio",
            "sortino_ratio_w",
            "sterling_ratio",
            "sterling_ratio_w",
            ], 
            key="edit_opt_v7_scoring",
            help=pbgui_help.scoring,
        )

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
        with col5:
            st.checkbox("apply_filters", value=False, help=pbgui_help.apply_filters, key="edit_opt_v7_apply_filters")
        # Init session state for approved_coins
        if "edit_opt_v7_approved_coins_long" in st.session_state:
            if st.session_state.edit_opt_v7_approved_coins_long != self.config.live.approved_coins.long:
                self.config.live.approved_coins.long = st.session_state.edit_opt_v7_approved_coins_long
        else:
            st.session_state.edit_opt_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_opt_v7_approved_coins_short" in st.session_state:
            if st.session_state.edit_opt_v7_approved_coins_short != self.config.live.approved_coins.short:
                self.config.live.approved_coins.short = st.session_state.edit_opt_v7_approved_coins_short
        else:
            st.session_state.edit_opt_v7_approved_coins_short = self.config.live.approved_coins.short
        # Apply filters
        if st.session_state.edit_opt_v7_apply_filters:
            self.config.live.approved_coins.long = list(set(st.session_state.coindata_bybit.approved_coins + st.session_state.coindata_binance.approved_coins + st.session_state.coindata_gateio.approved_coins + st.session_state.coindata_bitget.approved_coins))
            self.config.live.approved_coins.short = list(set(st.session_state.coindata_bybit.approved_coins + st.session_state.coindata_binance.approved_coins + st.session_state.coindata_gateio.approved_coins + st.session_state.coindata_bitget.approved_coins))
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
        symbols.sort()
        for symbol in self.config.live.approved_coins.long.copy():
            if symbol not in symbols:
                self.config.live.approved_coins.long.remove(symbol)
        for symbol in self.config.live.approved_coins.short.copy():
            if symbol not in symbols:
                self.config.live.approved_coins.short.remove(symbol)
        # Correct Display of Symbols
        if "edit_opt_v7_approved_coins_long" in st.session_state:
            st.session_state.edit_opt_v7_approved_coins_long = self.config.live.approved_coins.long
        if "edit_opt_v7_approved_coins_short" in st.session_state:
            st.session_state.edit_opt_v7_approved_coins_short = self.config.live.approved_coins.short
        # Select approved coins
        col1, col2 = st.columns([1,1], vertical_alignment="bottom")
        with col1:
            st.multiselect('approved_coins_long', symbols, key="edit_opt_v7_approved_coins_long")
        with col2:
            st.multiselect('approved_coins_short', symbols, key="edit_opt_v7_approved_coins_short")

    @st.fragment
    # market_cap
    def fragment_market_cap(self):
        if "edit_opt_v7_market_cap" in st.session_state:
            if st.session_state.edit_opt_v7_market_cap != self.config.pbgui.market_cap:
                self.config.pbgui.market_cap = st.session_state.edit_opt_v7_market_cap
                st.session_state.coindata_binance.market_cap = self.config.pbgui.market_cap
                st.session_state.coindata_bybit.market_cap = self.config.pbgui.market_cap
                st.session_state.coindata_gateio.market_cap = self.config.pbgui.market_cap
                st.session_state.coindata_bitget.market_cap = self.config.pbgui.market_cap
                if st.session_state.edit_opt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_opt_v7_market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_binance.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_bybit.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_gateio.market_cap = self.config.pbgui.market_cap
            st.session_state.coindata_bitget.market_cap = self.config.pbgui.market_cap
        st.number_input("market_cap", min_value=0, step=50, format="%.d", key="edit_opt_v7_market_cap", help=pbgui_help.market_cap)
    
    @st.fragment
    # vol_mcap
    def fragment_vol_mcap(self):
        if "edit_opt_v7_vol_mcap" in st.session_state:
            if st.session_state.edit_opt_v7_vol_mcap != self.config.pbgui.vol_mcap:
                self.config.pbgui.vol_mcap = st.session_state.edit_opt_v7_vol_mcap
                st.session_state.coindata_bybit.vol_mcap = self.config.pbgui.vol_mcap
                st.session_state.coindata_binance.vol_mcap = self.config.pbgui.vol_mcap
                st.session_state.coindata_gateio.vol_mcap = self.config.pbgui.vol_mcap
                st.session_state.coindata_bitget.vol_mcap = self.config.pbgui.vol_mcap
                if st.session_state.edit_opt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_opt_v7_vol_mcap = round(float(self.config.pbgui.vol_mcap),2)
            st.session_state.coindata_bybit.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_binance.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_gateio.vol_mcap = self.config.pbgui.vol_mcap
            st.session_state.coindata_bitget.vol_mcap = self.config.pbgui.vol_mcap
        st.number_input("vol/mcap", min_value=0.0, step=0.05, format="%.2f", key="edit_opt_v7_vol_mcap", help=pbgui_help.vol_mcap)

    @st.fragment
    # tags
    def fragment_tags(self):
        if "edit_opt_v7_tags" in st.session_state:
            if st.session_state.edit_opt_v7_tags != self.config.pbgui.tags:
                self.config.pbgui.tags = st.session_state.edit_opt_v7_tags
                st.session_state.coindata_bybit.tags = self.config.pbgui.tags
                st.session_state.coindata_binance.tags = self.config.pbgui.tags
                st.session_state.coindata_gateio.tags = self.config.pbgui.tags
                st.session_state.coindata_bitget.tags = self.config.pbgui.tags
                if st.session_state.edit_opt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_opt_v7_tags = self.config.pbgui.tags
            st.session_state.coindata_bybit.tags = self.config.pbgui.tags
            st.session_state.coindata_binance.tags = self.config.pbgui.tags
            st.session_state.coindata_gateio.tags = self.config.pbgui.tags
            st.session_state.coindata_bitget.tags = self.config.pbgui.tags
        # remove duplicates from tags and sort them
        tags = sorted(list(set(st.session_state.coindata_bybit.all_tags + st.session_state.coindata_binance.all_tags + st.session_state.coindata_gateio.all_tags + st.session_state.coindata_bitget.all_tags)))
        st.multiselect("tags", tags, key="edit_opt_v7_tags", help=pbgui_help.coindata_tags)

    # only_cpt
    @st.fragment
    def fragment_only_cpt(self):
        if "edit_opt_v7_only_cpt" in st.session_state:
            if st.session_state.edit_opt_v7_only_cpt != self.config.pbgui.only_cpt:
                self.config.pbgui.only_cpt = st.session_state.edit_opt_v7_only_cpt
                st.session_state.coindata_bybit.only_cpt = self.config.pbgui.only_cpt
                st.session_state.coindata_binance.only_cpt = self.config.pbgui.only_cpt
                st.session_state.coindata_bitget.only_cpt = self.config.pbgui.only_cpt
                if st.session_state.edit_opt_v7_apply_filters:
                    st.rerun()
        else:
            st.session_state.edit_opt_v7_only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_bybit.only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_binance.only_cpt = self.config.pbgui.only_cpt
            st.session_state.coindata_bitget.only_cpt = self.config.pbgui.only_cpt
        st.checkbox("only_cpt", key="edit_opt_v7_only_cpt", help=pbgui_help.only_cpt)

    # # long_close_grid_markup_range
    # @st.fragment
    # def fragment_long_close_grid_markup_range(self):
    #     if "edit_opt_v7_long_close_grid_markup_range" in st.session_state:
    #         if st.session_state.edit_opt_v7_long_close_grid_markup_range != (self.config.optimize.bounds.long_close_grid_markup_range_0, self.config.optimize.bounds.long_close_grid_markup_range_1):
    #             self.config.optimize.bounds.long_close_grid_markup_range_0 = st.session_state.edit_opt_v7_long_close_grid_markup_range[0]
    #             self.config.optimize.bounds.long_close_grid_markup_range_1 = st.session_state.edit_opt_v7_long_close_grid_markup_range[1]
    #     else:
    #         st.session_state.edit_opt_v7_long_close_grid_markup_range = (self.config.optimize.bounds.long_close_grid_markup_range_0, self.config.optimize.bounds.long_close_grid_markup_range_1)
    #     st.slider(
    #         "long_close_grid_markup_range",
    #         min_value=Bounds.CLOSE_GRID_MARKUP_RANGE_MIN,
    #         max_value=Bounds.CLOSE_GRID_MARKUP_RANGE_MAX,
    #         step=Bounds.CLOSE_GRID_MARKUP_RANGE_STEP,
    #         format=Bounds.CLOSE_GRID_MARKUP_RANGE_FORMAT,
    #         key="edit_opt_v7_long_close_grid_markup_range",
    #         help=pbgui_help.close_grid_parameters)
    
    # # long_close_grid_min_markup
    # @st.fragment
    # def fragment_long_close_grid_min_markup(self):
    #     if "edit_opt_v7_long_close_grid_min_markup" in st.session_state:
    #         if st.session_state.edit_opt_v7_long_close_grid_min_markup != (self.config.optimize.bounds.long_close_grid_min_markup_0, self.config.optimize.bounds.long_close_grid_min_markup_1):
    #             self.config.optimize.bounds.long_close_grid_min_markup_0 = st.session_state.edit_opt_v7_long_close_grid_min_markup[0]
    #             self.config.optimize.bounds.long_close_grid_min_markup_1 = st.session_state.edit_opt_v7_long_close_grid_min_markup[1]
    #     else:
    #         st.session_state.edit_opt_v7_long_close_grid_min_markup = (self.config.optimize.bounds.long_close_grid_min_markup_0, self.config.optimize.bounds.long_close_grid_min_markup_1)
    #     st.slider(
    #         "long_close_grid_min_markup",
    #         min_value=Bounds.CLOSE_GRID_MIN_MARKUP_MIN,
    #         max_value=Bounds.CLOSE_GRID_MIN_MARKUP_MAX,
    #         step=Bounds.CLOSE_GRID_MIN_MARKUP_STEP,
    #         format=Bounds.CLOSE_GRID_MIN_MARKUP_FORMAT,
    #         key="edit_opt_v7_long_close_grid_min_markup",
    #         help=pbgui_help.close_grid_parameters)

    # long_close_grid_markup_end
    @st.fragment
    def fragment_long_close_grid_markup_end(self):
        if "edit_opt_v7_long_close_grid_markup_end" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_grid_markup_end != (self.config.optimize.bounds.long_close_grid_markup_end_0, self.config.optimize.bounds.long_close_grid_markup_end_1):
                self.config.optimize.bounds.long_close_grid_markup_end_0 = st.session_state.edit_opt_v7_long_close_grid_markup_end[0]
                self.config.optimize.bounds.long_close_grid_markup_end_1 = st.session_state.edit_opt_v7_long_close_grid_markup_end[1]
        else:
            st.session_state.edit_opt_v7_long_close_grid_markup_end = (self.config.optimize.bounds.long_close_grid_markup_end_0, self.config.optimize.bounds.long_close_grid_markup_end_1)
        st.slider(
            "long_close_grid_markup_end",
            min_value=Bounds.CLOSE_GRID_MARKUP_END_MIN,
            max_value=Bounds.CLOSE_GRID_MARKUP_END_MAX,
            step=Bounds.CLOSE_GRID_MARKUP_END_STEP,
            format=Bounds.CLOSE_GRID_MARKUP_END_FORMAT,
            key="edit_opt_v7_long_close_grid_markup_end",
            help=pbgui_help.close_grid_parameters)  
    
    # long_close_grid_markup_start
    @st.fragment
    def fragment_long_close_grid_markup_start(self):
        if "edit_opt_v7_long_close_grid_markup_start" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_grid_markup_start != (self.config.optimize.bounds.long_close_grid_markup_start_0, self.config.optimize.bounds.long_close_grid_markup_start_1):
                self.config.optimize.bounds.long_close_grid_markup_start_0 = st.session_state.edit_opt_v7_long_close_grid_markup_start[0]
                self.config.optimize.bounds.long_close_grid_markup_start_1 = st.session_state.edit_opt_v7_long_close_grid_markup_start[1]
        else:
            st.session_state.edit_opt_v7_long_close_grid_markup_start = (self.config.optimize.bounds.long_close_grid_markup_start_0, self.config.optimize.bounds.long_close_grid_markup_start_1)
        st.slider(
            "long_close_grid_markup_start",
            min_value=Bounds.CLOSE_GRID_MARKUP_START_MIN,
            max_value=Bounds.CLOSE_GRID_MARKUP_START_MAX,
            step=Bounds.CLOSE_GRID_MARKUP_START_STEP,
            format=Bounds.CLOSE_GRID_MARKUP_START_FORMAT,
            key="edit_opt_v7_long_close_grid_markup_start",
            help=pbgui_help.close_grid_parameters)

    # long_close_grid_qty_pct
    @st.fragment
    def fragment_long_close_grid_qty_pct(self):
        if "edit_opt_v7_long_close_grid_qty_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_grid_qty_pct != (self.config.optimize.bounds.long_close_grid_qty_pct_0, self.config.optimize.bounds.long_close_grid_qty_pct_1):
                self.config.optimize.bounds.long_close_grid_qty_pct_0 = st.session_state.edit_opt_v7_long_close_grid_qty_pct[0]
                self.config.optimize.bounds.long_close_grid_qty_pct_1 = st.session_state.edit_opt_v7_long_close_grid_qty_pct[1]
        else:
            st.session_state.edit_opt_v7_long_close_grid_qty_pct = (self.config.optimize.bounds.long_close_grid_qty_pct_0, self.config.optimize.bounds.long_close_grid_qty_pct_1)
        st.slider(
            "long_close_grid_qty_pct",
            min_value=Bounds.CLOSE_GRID_QTY_PCT_MIN,
            max_value=Bounds.CLOSE_GRID_QTY_PCT_MAX,
            step=Bounds.CLOSE_GRID_QTY_PCT_STEP,
            format=Bounds.CLOSE_GRID_QTY_PCT_FORMAT,
            key="edit_opt_v7_long_close_grid_qty_pct",
            help=pbgui_help.close_grid_parameters)

    # long_close_trailing_grid_ratio
    @st.fragment
    def fragment_long_close_trailing_grid_ratio(self):
        if "edit_opt_v7_long_close_trailing_grid_ratio" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_trailing_grid_ratio != (self.config.optimize.bounds.long_close_trailing_grid_ratio_0, self.config.optimize.bounds.long_close_trailing_grid_ratio_1):
                self.config.optimize.bounds.long_close_trailing_grid_ratio_0 = st.session_state.edit_opt_v7_long_close_trailing_grid_ratio[0]
                self.config.optimize.bounds.long_close_trailing_grid_ratio_1 = st.session_state.edit_opt_v7_long_close_trailing_grid_ratio[1]
        else:
            st.session_state.edit_opt_v7_long_close_trailing_grid_ratio = (self.config.optimize.bounds.long_close_trailing_grid_ratio_0, self.config.optimize.bounds.long_close_trailing_grid_ratio_1)
        st.slider(
            "long_close_trailing_grid_ratio",
            min_value=Bounds.CLOSE_TRAILING_GRID_RATIO_MIN,
            max_value=Bounds.CLOSE_TRAILING_GRID_RATIO_MAX,
            step=Bounds.CLOSE_TRAILING_GRID_RATIO_STEP,
            format=Bounds.CLOSE_TRAILING_GRID_RATIO_FORMAT,
            key="edit_opt_v7_long_close_trailing_grid_ratio",
            help=pbgui_help.close_grid_parameters)

    # long_close_trailing_qty_pct
    @st.fragment
    def fragment_long_close_trailing_qty_pct(self):
        if "edit_opt_v7_long_close_trailing_qty_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_trailing_qty_pct != (self.config.optimize.bounds.long_close_trailing_qty_pct_0, self.config.optimize.bounds.long_close_trailing_qty_pct_1):
                self.config.optimize.bounds.long_close_trailing_qty_pct_0 = st.session_state.edit_opt_v7_long_close_trailing_qty_pct[0]
                self.config.optimize.bounds.long_close_trailing_qty_pct_1 = st.session_state.edit_opt_v7_long_close_trailing_qty_pct[1]
        else:
            st.session_state.edit_opt_v7_long_close_trailing_qty_pct = (self.config.optimize.bounds.long_close_trailing_qty_pct_0, self.config.optimize.bounds.long_close_trailing_qty_pct_1)
        st.slider(
            "long_close_trailing_qty_pct",
            min_value=Bounds.CLOSE_TRAILING_QTY_PCT_MIN,
            max_value=Bounds.CLOSE_TRAILING_QTY_PCT_MAX,
            step=Bounds.CLOSE_TRAILING_QTY_PCT_STEP,
            format=Bounds.CLOSE_TRAILING_QTY_PCT_FORMAT,
            key="edit_opt_v7_long_close_trailing_qty_pct",
            help=pbgui_help.close_grid_parameters)

    # long_close_trailing_retracement_pct
    @st.fragment
    def fragment_long_close_trailing_retracement_pct(self):
        if "edit_opt_v7_long_close_trailing_retracement_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_trailing_retracement_pct != (self.config.optimize.bounds.long_close_trailing_retracement_pct_0, self.config.optimize.bounds.long_close_trailing_retracement_pct_1):
                self.config.optimize.bounds.long_close_trailing_retracement_pct_0 = st.session_state.edit_opt_v7_long_close_trailing_retracement_pct[0]
                self.config.optimize.bounds.long_close_trailing_retracement_pct_1 = st.session_state.edit_opt_v7_long_close_trailing_retracement_pct[1]
        else:
            st.session_state.edit_opt_v7_long_close_trailing_retracement_pct = (self.config.optimize.bounds.long_close_trailing_retracement_pct_0, self.config.optimize.bounds.long_close_trailing_retracement_pct_1)
        st.slider(
            "long_close_trailing_retracement_pct",
            min_value=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_MIN,
            max_value=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_MAX,
            step=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_STEP,
            format=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_FORMAT,
            key="edit_opt_v7_long_close_trailing_retracement_pct",
            help=pbgui_help.close_grid_parameters)

    # long_close_trailing_threshold_pct
    @st.fragment
    def fragment_long_close_trailing_threshold_pct(self):
        if "edit_opt_v7_long_close_trailing_threshold_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_close_trailing_threshold_pct != (self.config.optimize.bounds.long_close_trailing_threshold_pct_0, self.config.optimize.bounds.long_close_trailing_threshold_pct_1):
                self.config.optimize.bounds.long_close_trailing_threshold_pct_0 = st.session_state.edit_opt_v7_long_close_trailing_threshold_pct[0]
                self.config.optimize.bounds.long_close_trailing_threshold_pct_1 = st.session_state.edit_opt_v7_long_close_trailing_threshold_pct[1]
        else:
            st.session_state.edit_opt_v7_long_close_trailing_threshold_pct = (self.config.optimize.bounds.long_close_trailing_threshold_pct_0, self.config.optimize.bounds.long_close_trailing_threshold_pct_1)
        st.slider(
            "long_close_trailing_threshold_pct",
            min_value=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_MIN,
            max_value=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_MAX,
            step=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_STEP,
            format=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_FORMAT,
            key="edit_opt_v7_long_close_trailing_threshold_pct",
            help=pbgui_help.close_grid_parameters)

    # long_ema_span_0
    @st.fragment
    def fragment_long_ema_span_0(self):
        if "edit_opt_v7_long_ema_span_0" in st.session_state:
            if st.session_state.edit_opt_v7_long_ema_span_0 != (self.config.optimize.bounds.long_ema_span_0_0, self.config.optimize.bounds.long_ema_span_0_1):
                self.config.optimize.bounds.long_ema_span_0_0 = st.session_state.edit_opt_v7_long_ema_span_0[0]
                self.config.optimize.bounds.long_ema_span_0_1 = st.session_state.edit_opt_v7_long_ema_span_0[1]
        else:
            st.session_state.edit_opt_v7_long_ema_span_0 = (self.config.optimize.bounds.long_ema_span_0_0, self.config.optimize.bounds.long_ema_span_0_1)
        st.slider(
            "long_ema_span_0",
            min_value=Bounds.EMA_SPAN_0_MIN,
            max_value=Bounds.EMA_SPAN_0_MAX,
            step=Bounds.EMA_SPAN_0_STEP,
            format=Bounds.EMA_SPAN_0_FORMAT,
            key="edit_opt_v7_long_ema_span_0",
            help=pbgui_help.ema_span)
    
    # long_ema_span_1
    @st.fragment
    def fragment_long_ema_span_1(self):
        if "edit_opt_v7_long_ema_span_1" in st.session_state:
            if st.session_state.edit_opt_v7_long_ema_span_1 != (self.config.optimize.bounds.long_ema_span_1_0, self.config.optimize.bounds.long_ema_span_1_1):
                self.config.optimize.bounds.long_ema_span_1_0 = st.session_state.edit_opt_v7_long_ema_span_1[0]
                self.config.optimize.bounds.long_ema_span_1_1 = st.session_state.edit_opt_v7_long_ema_span_1[1]
        else:
            st.session_state.edit_opt_v7_long_ema_span_1 = (self.config.optimize.bounds.long_ema_span_1_0, self.config.optimize.bounds.long_ema_span_1_1)
        st.slider(
            "long_ema_span_1",
            min_value=Bounds.EMA_SPAN_1_MIN,
            max_value=Bounds.EMA_SPAN_1_MAX,
            step=Bounds.EMA_SPAN_1_STEP,
            format=Bounds.EMA_SPAN_1_FORMAT,
            key="edit_opt_v7_long_ema_span_1",
            help=pbgui_help.ema_span)
    
    # long_entry_grid_double_down_factor
    @st.fragment
    def fragment_long_entry_grid_double_down_factor(self):
        if "edit_opt_v7_long_entry_grid_double_down_factor" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_grid_double_down_factor != (self.config.optimize.bounds.long_entry_grid_double_down_factor_0, self.config.optimize.bounds.long_entry_grid_double_down_factor_1):
                self.config.optimize.bounds.long_entry_grid_double_down_factor_0 = st.session_state.edit_opt_v7_long_entry_grid_double_down_factor[0]
                self.config.optimize.bounds.long_entry_grid_double_down_factor_1 = st.session_state.edit_opt_v7_long_entry_grid_double_down_factor[1]
        else:
            st.session_state.edit_opt_v7_long_entry_grid_double_down_factor = (self.config.optimize.bounds.long_entry_grid_double_down_factor_0, self.config.optimize.bounds.long_entry_grid_double_down_factor_1)
        st.slider(
            "long_entry_grid_double_down_factor",
            min_value=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_MIN,
            max_value=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_MAX,
            step=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_STEP,
            format=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_FORMAT,
            key="edit_opt_v7_long_entry_grid_double_down_factor",
            help=pbgui_help.entry_grid_double_down_factor)
    
    # long_entry_grid_spacing_pct
    @st.fragment
    def fragment_long_entry_grid_spacing_pct(self):
        if "edit_opt_v7_long_entry_grid_spacing_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_grid_spacing_pct != (self.config.optimize.bounds.long_entry_grid_spacing_pct_0, self.config.optimize.bounds.long_entry_grid_spacing_pct_1):
                self.config.optimize.bounds.long_entry_grid_spacing_pct_0 = st.session_state.edit_opt_v7_long_entry_grid_spacing_pct[0]
                self.config.optimize.bounds.long_entry_grid_spacing_pct_1 = st.session_state.edit_opt_v7_long_entry_grid_spacing_pct[1]
        else:
            st.session_state.edit_opt_v7_long_entry_grid_spacing_pct = (self.config.optimize.bounds.long_entry_grid_spacing_pct_0, self.config.optimize.bounds.long_entry_grid_spacing_pct_1)
        st.slider(
            "long_entry_grid_spacing_pct",
            min_value=Bounds.ENTRY_GRID_SPACING_PCT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_PCT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_PCT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_PCT_FORMAT,
            key="edit_opt_v7_long_entry_grid_spacing_pct",
            help=pbgui_help.entry_grid_spacing)
    
    # long_entry_grid_spacing_weight
    @st.fragment
    def fragment_long_entry_grid_spacing_weight(self):
        if "edit_opt_v7_long_entry_grid_spacing_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_grid_spacing_weight != (self.config.optimize.bounds.long_entry_grid_spacing_weight_0, self.config.optimize.bounds.long_entry_grid_spacing_weight_1):
                self.config.optimize.bounds.long_entry_grid_spacing_weight_0 = st.session_state.edit_opt_v7_long_entry_grid_spacing_weight[0]
                self.config.optimize.bounds.long_entry_grid_spacing_weight_1 = st.session_state.edit_opt_v7_long_entry_grid_spacing_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_grid_spacing_weight = (self.config.optimize.bounds.long_entry_grid_spacing_weight_0, self.config.optimize.bounds.long_entry_grid_spacing_weight_1)
        st.slider(
            "long_entry_grid_spacing_weight",
            min_value=Bounds.ENTRY_GRID_SPACING_WEIGHT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_WEIGHT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_WEIGHT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_grid_spacing_weight",
            help=pbgui_help.entry_grid_spacing)
    
    # long_entry_initial_ema_dist
    @st.fragment
    def fragment_long_entry_initial_ema_dist(self):
        if "edit_opt_v7_long_entry_initial_ema_dist" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_initial_ema_dist != (self.config.optimize.bounds.long_entry_initial_ema_dist_0, self.config.optimize.bounds.long_entry_initial_ema_dist_1):
                self.config.optimize.bounds.long_entry_initial_ema_dist_0 = st.session_state.edit_opt_v7_long_entry_initial_ema_dist[0]
                self.config.optimize.bounds.long_entry_initial_ema_dist_1 = st.session_state.edit_opt_v7_long_entry_initial_ema_dist[1]
        else:
            st.session_state.edit_opt_v7_long_entry_initial_ema_dist = (self.config.optimize.bounds.long_entry_initial_ema_dist_0, self.config.optimize.bounds.long_entry_initial_ema_dist_1)
        st.slider(
            "long_entry_initial_ema_dist",
            min_value=Bounds.ENTRY_INITIAL_EMA_DIST_MIN,
            max_value=Bounds.ENTRY_INITIAL_EMA_DIST_MAX,
            step=Bounds.ENTRY_INITIAL_EMA_DIST_STEP,
            format=Bounds.ENTRY_INITIAL_EMA_DIST_FORMAT,
            key="edit_opt_v7_long_entry_initial_ema_dist",
            help=pbgui_help.entry_initial_ema_dist)
    
    # long_entry_initial_qty_pct
    @st.fragment
    def fragment_long_entry_initial_qty_pct(self):
        if "edit_opt_v7_long_entry_initial_qty_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_initial_qty_pct != (self.config.optimize.bounds.long_entry_initial_qty_pct_0, self.config.optimize.bounds.long_entry_initial_qty_pct_1):
                self.config.optimize.bounds.long_entry_initial_qty_pct_0 = st.session_state.edit_opt_v7_long_entry_initial_qty_pct[0]
                self.config.optimize.bounds.long_entry_initial_qty_pct_1 = st.session_state.edit_opt_v7_long_entry_initial_qty_pct[1]
        else:
            st.session_state.edit_opt_v7_long_entry_initial_qty_pct = (self.config.optimize.bounds.long_entry_initial_qty_pct_0, self.config.optimize.bounds.long_entry_initial_qty_pct_1)
        st.slider(
            "long_entry_initial_qty_pct",
            min_value=Bounds.ENTRY_INITIAL_QTY_PCT_MIN,
            max_value=Bounds.ENTRY_INITIAL_QTY_PCT_MAX,
            step=Bounds.ENTRY_INITIAL_QTY_PCT_STEP,
            format=Bounds.ENTRY_INITIAL_QTY_PCT_FORMAT,
            key="edit_opt_v7_long_entry_initial_qty_pct",
            help=pbgui_help.entry_initial_qty_pct)
    
    # long_entry_trailing_double_down_factor
    @st.fragment
    def fragment_long_entry_trailing_double_down_factor(self):
        if "edit_opt_v7_long_entry_trailing_double_down_factor" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_double_down_factor != (self.config.optimize.bounds.long_entry_trailing_double_down_factor_0, self.config.optimize.bounds.long_entry_trailing_double_down_factor_1):
                self.config.optimize.bounds.long_entry_trailing_double_down_factor_0 = st.session_state.edit_opt_v7_long_entry_trailing_double_down_factor[0]
                self.config.optimize.bounds.long_entry_trailing_double_down_factor_1 = st.session_state.edit_opt_v7_long_entry_trailing_double_down_factor[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_double_down_factor = (self.config.optimize.bounds.long_entry_trailing_double_down_factor_0, self.config.optimize.bounds.long_entry_trailing_double_down_factor_1)
        st.slider(
            "long_entry_trailing_double_down_factor",
            min_value=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MIN,
            max_value=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MAX,
            step=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_STEP,
            format=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_FORMAT,
            key="edit_opt_v7_long_entry_trailing_double_down_factor",
            help=pbgui_help.trailing_parameters)
    
    # long_entry_trailing_grid_ratio
    @st.fragment
    def fragment_long_entry_trailing_grid_ratio(self):
        if "edit_opt_v7_long_entry_trailing_grid_ratio" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_grid_ratio != (self.config.optimize.bounds.long_entry_trailing_grid_ratio_0, self.config.optimize.bounds.long_entry_trailing_grid_ratio_1):
                self.config.optimize.bounds.long_entry_trailing_grid_ratio_0 = st.session_state.edit_opt_v7_long_entry_trailing_grid_ratio[0]
                self.config.optimize.bounds.long_entry_trailing_grid_ratio_1 = st.session_state.edit_opt_v7_long_entry_trailing_grid_ratio[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_grid_ratio = (self.config.optimize.bounds.long_entry_trailing_grid_ratio_0, self.config.optimize.bounds.long_entry_trailing_grid_ratio_1)
        st.slider(
            "long_entry_trailing_grid_ratio",
            min_value=Bounds.ENTRY_TRAILING_GRID_RATIO_MIN,
            max_value=Bounds.ENTRY_TRAILING_GRID_RATIO_MAX,
            step=Bounds.ENTRY_TRAILING_GRID_RATIO_STEP,
            format=Bounds.ENTRY_TRAILING_GRID_RATIO_FORMAT,
            key="edit_opt_v7_long_entry_trailing_grid_ratio",
            help=pbgui_help.trailing_parameters)
    
    # long_entry_trailing_retracement_pct
    @st.fragment
    def fragment_long_entry_trailing_retracement_pct(self):
        if "edit_opt_v7_long_entry_trailing_retracement_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_retracement_pct != (self.config.optimize.bounds.long_entry_trailing_retracement_pct_0, self.config.optimize.bounds.long_entry_trailing_retracement_pct_1):
                self.config.optimize.bounds.long_entry_trailing_retracement_pct_0 = st.session_state.edit_opt_v7_long_entry_trailing_retracement_pct[0]
                self.config.optimize.bounds.long_entry_trailing_retracement_pct_1 = st.session_state.edit_opt_v7_long_entry_trailing_retracement_pct[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_retracement_pct = (self.config.optimize.bounds.long_entry_trailing_retracement_pct_0, self.config.optimize.bounds.long_entry_trailing_retracement_pct_1)
        st.slider(
            "long_entry_trailing_retracement_pct",
            min_value=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_MIN,
            max_value=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_MAX,
            step=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_STEP,
            format=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_FORMAT,
            key="edit_opt_v7_long_entry_trailing_retracement_pct",
            help=pbgui_help.trailing_parameters)
    
    # long_entry_trailing_threshold_pct
    @st.fragment
    def fragment_long_entry_trailing_threshold_pct(self):
        if "edit_opt_v7_long_entry_trailing_threshold_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_threshold_pct != (self.config.optimize.bounds.long_entry_trailing_threshold_pct_0, self.config.optimize.bounds.long_entry_trailing_threshold_pct_1):
                self.config.optimize.bounds.long_entry_trailing_threshold_pct_0 = st.session_state.edit_opt_v7_long_entry_trailing_threshold_pct[0]
                self.config.optimize.bounds.long_entry_trailing_threshold_pct_1 = st.session_state.edit_opt_v7_long_entry_trailing_threshold_pct[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_threshold_pct = (self.config.optimize.bounds.long_entry_trailing_threshold_pct_0, self.config.optimize.bounds.long_entry_trailing_threshold_pct_1)
        st.slider(
            "long_entry_trailing_threshold_pct",
            min_value=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_MIN,
            max_value=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_MAX,
            step=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_STEP,
            format=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_FORMAT,
            key="edit_opt_v7_long_entry_trailing_threshold_pct",
            help=pbgui_help.trailing_parameters)
    
    # long_filter_noisiness_rolling_window
    @st.fragment
    def fragment_long_filter_noisiness_rolling_window(self):
        if "edit_opt_v7_long_filter_noisiness_rolling_window" in st.session_state:
            if st.session_state.edit_opt_v7_long_filter_noisiness_rolling_window != (self.config.optimize.bounds.long_filter_noisiness_rolling_window_0, self.config.optimize.bounds.long_filter_noisiness_rolling_window_1):
                self.config.optimize.bounds.long_filter_noisiness_rolling_window_0 = st.session_state.edit_opt_v7_long_filter_noisiness_rolling_window[0]
                self.config.optimize.bounds.long_filter_noisiness_rolling_window_1 = st.session_state.edit_opt_v7_long_filter_noisiness_rolling_window[1]
        else:
            st.session_state.edit_opt_v7_long_filter_noisiness_rolling_window = (self.config.optimize.bounds.long_filter_noisiness_rolling_window_0, self.config.optimize.bounds.long_filter_noisiness_rolling_window_1)
        st.slider(
            "long_filter_noisiness_rolling_window",
            min_value=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_MIN,
            max_value=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_MAX,
            step=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_STEP,
            format=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_FORMAT,
            key="edit_opt_v7_long_filter_noisiness_rolling_window",
            help=pbgui_help.filter_rolling_window)

    # long_filter_volume_drop_pct
    @st.fragment
    def fragment_long_filter_volume_drop_pct(self):
        if "edit_opt_v7_long_filter_volume_drop_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_filter_volume_drop_pct != (self.config.optimize.bounds.long_filter_volume_drop_pct_0, self.config.optimize.bounds.long_filter_volume_drop_pct_1):
                self.config.optimize.bounds.long_filter_volume_drop_pct_0 = st.session_state.edit_opt_v7_long_filter_volume_drop_pct[0]
                self.config.optimize.bounds.long_filter_volume_drop_pct_1 = st.session_state.edit_opt_v7_long_filter_volume_drop_pct[1]
        else:
            st.session_state.edit_opt_v7_long_filter_volume_drop_pct = (self.config.optimize.bounds.long_filter_volume_drop_pct_0, self.config.optimize.bounds.long_filter_volume_drop_pct_1)
        st.slider(
            "long_filter_volume_drop_pct",
            min_value=Bounds.FILTER_VOLUME_DROP_PCT_MIN,
            max_value=Bounds.FILTER_VOLUME_DROP_PCT_MAX,
            step=Bounds.FILTER_VOLUME_DROP_PCT_STEP,
            format=Bounds.FILTER_VOLUME_DROP_PCT_FORMAT,
            key="edit_opt_v7_long_filter_volume_drop_pct",
            help=pbgui_help.filter_volume_drop_pct)
    
    # long_filter_volume_rolling_window
    @st.fragment
    def fragment_long_filter_volume_rolling_window(self):
        if "edit_opt_v7_long_filter_volume_rolling_window" in st.session_state:
            if st.session_state.edit_opt_v7_long_filter_volume_rolling_window != (self.config.optimize.bounds.long_filter_volume_rolling_window_0, self.config.optimize.bounds.long_filter_volume_rolling_window_1):
                self.config.optimize.bounds.long_filter_volume_rolling_window_0 = st.session_state.edit_opt_v7_long_filter_volume_rolling_window[0]
                self.config.optimize.bounds.long_filter_volume_rolling_window_1 = st.session_state.edit_opt_v7_long_filter_volume_rolling_window[1]
        else:
            st.session_state.edit_opt_v7_long_filter_volume_rolling_window = (self.config.optimize.bounds.long_filter_volume_rolling_window_0, self.config.optimize.bounds.long_filter_volume_rolling_window_1)
        st.slider(
            "long_filter_volume_rolling_window",
            min_value=Bounds.FILTER_VOLUME_ROLLING_WINDOW_MIN,
            max_value=Bounds.FILTER_VOLUME_ROLLING_WINDOW_MAX,
            step=Bounds.FILTER_VOLUME_ROLLING_WINDOW_STEP,
            format=Bounds.FILTER_VOLUME_ROLLING_WINDOW_FORMAT,
            key="edit_opt_v7_long_filter_volume_rolling_window",
            help=pbgui_help.filter_rolling_window)

    # long_n_positions
    @st.fragment
    def fragment_long_n_positions(self):
        if "edit_opt_v7_long_n_positions" in st.session_state:
            if st.session_state.edit_opt_v7_long_n_positions != (self.config.optimize.bounds.long_n_positions_0, self.config.optimize.bounds.long_n_positions_1):
                self.config.optimize.bounds.long_n_positions_0 = st.session_state.edit_opt_v7_long_n_positions[0]
                self.config.optimize.bounds.long_n_positions_1 = st.session_state.edit_opt_v7_long_n_positions[1]
        else:
            st.session_state.edit_opt_v7_long_n_positions = (self.config.optimize.bounds.long_n_positions_0, self.config.optimize.bounds.long_n_positions_1)
        st.slider(
            "long_n_positions",
            min_value=Bounds.N_POSITIONS_MIN,
            max_value=Bounds.N_POSITIONS_MAX,
            step=Bounds.N_POSITIONS_STEP,
            format=Bounds.N_POSITIONS_FORMAT,
            key="edit_opt_v7_long_n_positions",
            help=pbgui_help.n_positions)
    
    # long_total_wallet_exposure_limit
    @st.fragment
    def fragment_long_total_wallet_exposure_limit(self):
        if "edit_opt_v7_long_total_wallet_exposure_limit" in st.session_state:
            if st.session_state.edit_opt_v7_long_total_wallet_exposure_limit != (self.config.optimize.bounds.long_total_wallet_exposure_limit_0, self.config.optimize.bounds.long_total_wallet_exposure_limit_1):
                self.config.optimize.bounds.long_total_wallet_exposure_limit_0 = st.session_state.edit_opt_v7_long_total_wallet_exposure_limit[0]
                self.config.optimize.bounds.long_total_wallet_exposure_limit_1 = st.session_state.edit_opt_v7_long_total_wallet_exposure_limit[1]
        else:
            st.session_state.edit_opt_v7_long_total_wallet_exposure_limit = (self.config.optimize.bounds.long_total_wallet_exposure_limit_0, self.config.optimize.bounds.long_total_wallet_exposure_limit_1)
        st.slider(
            "long_total_wallet_exposure_limit",
            min_value=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_MIN,
            max_value=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_MAX,
            step=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_STEP,
            format=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_FORMAT,
            key="edit_opt_v7_long_total_wallet_exposure_limit",
            help=pbgui_help.total_wallet_exposure_limit)

    # long_unstuck_close_pct
    @st.fragment
    def fragment_long_unstuck_close_pct(self):
        if "edit_opt_v7_long_unstuck_close_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_unstuck_close_pct != (self.config.optimize.bounds.long_unstuck_close_pct_0, self.config.optimize.bounds.long_unstuck_close_pct_1):
                self.config.optimize.bounds.long_unstuck_close_pct_0 = st.session_state.edit_opt_v7_long_unstuck_close_pct[0]
                self.config.optimize.bounds.long_unstuck_close_pct_1 = st.session_state.edit_opt_v7_long_unstuck_close_pct[1]
        else:
            st.session_state.edit_opt_v7_long_unstuck_close_pct = (self.config.optimize.bounds.long_unstuck_close_pct_0, self.config.optimize.bounds.long_unstuck_close_pct_1)
        st.slider(
            "long_unstuck_close_pct",
            min_value=Bounds.UNSTUCK_CLOSE_PCT_MIN,
            max_value=Bounds.UNSTUCK_CLOSE_PCT_MAX,
            step=Bounds.UNSTUCK_CLOSE_PCT_STEP,
            format=Bounds.UNSTUCK_CLOSE_PCT_FORMAT,
            key="edit_opt_v7_long_unstuck_close_pct",
            help=pbgui_help.unstuck_close_pct)

    # long_unstuck_ema_dist
    @st.fragment
    def fragment_long_unstuck_ema_dist(self):
        if "edit_opt_v7_long_unstuck_ema_dist" in st.session_state:
            if st.session_state.edit_opt_v7_long_unstuck_ema_dist != (self.config.optimize.bounds.long_unstuck_ema_dist_0, self.config.optimize.bounds.long_unstuck_ema_dist_1):
                self.config.optimize.bounds.long_unstuck_ema_dist_0 = st.session_state.edit_opt_v7_long_unstuck_ema_dist[0]
                self.config.optimize.bounds.long_unstuck_ema_dist_1 = st.session_state.edit_opt_v7_long_unstuck_ema_dist[1]
        else:
            st.session_state.edit_opt_v7_long_unstuck_ema_dist = (self.config.optimize.bounds.long_unstuck_ema_dist_0, self.config.optimize.bounds.long_unstuck_ema_dist_1)
        st.slider(
            "long_unstuck_ema_dist",
            min_value=Bounds.UNSTUCK_EMA_DIST_MIN,
            max_value=Bounds.UNSTUCK_EMA_DIST_MAX,
            step=Bounds.UNSTUCK_EMA_DIST_STEP,
            format=Bounds.UNSTUCK_EMA_DIST_FORMAT,
            key="edit_opt_v7_long_unstuck_ema_dist",
            help=pbgui_help.unstuck_ema_dist)

    # long_unstuck_loss_allowance_pct
    @st.fragment
    def fragment_long_unstuck_loss_allowance_pct(self):
        if "edit_opt_v7_long_unstuck_loss_allowance_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_unstuck_loss_allowance_pct != (self.config.optimize.bounds.long_unstuck_loss_allowance_pct_0, self.config.optimize.bounds.long_unstuck_loss_allowance_pct_1):
                self.config.optimize.bounds.long_unstuck_loss_allowance_pct_0 = st.session_state.edit_opt_v7_long_unstuck_loss_allowance_pct[0]
                self.config.optimize.bounds.long_unstuck_loss_allowance_pct_1 = st.session_state.edit_opt_v7_long_unstuck_loss_allowance_pct[1]
        else:
            st.session_state.edit_opt_v7_long_unstuck_loss_allowance_pct = (self.config.optimize.bounds.long_unstuck_loss_allowance_pct_0, self.config.optimize.bounds.long_unstuck_loss_allowance_pct_1)
        st.slider(
            "long_unstuck_loss_allowance_pct",
            min_value=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_MIN,
            max_value=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_MAX,
            step=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_STEP,
            format=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_FORMAT,
            key="edit_opt_v7_long_unstuck_loss_allowance_pct",
            help=pbgui_help.unstuck_loss_allowance_pct)

    # long_unstuck_threshold
    @st.fragment
    def fragment_long_unstuck_threshold(self):
        if "edit_opt_v7_long_unstuck_threshold" in st.session_state:
            if st.session_state.edit_opt_v7_long_unstuck_threshold != (self.config.optimize.bounds.long_unstuck_threshold_0, self.config.optimize.bounds.long_unstuck_threshold_1):
                self.config.optimize.bounds.long_unstuck_threshold_0 = st.session_state.edit_opt_v7_long_unstuck_threshold[0]
                self.config.optimize.bounds.long_unstuck_threshold_1 = st.session_state.edit_opt_v7_long_unstuck_threshold[1]
        else:
            st.session_state.edit_opt_v7_long_unstuck_threshold = (self.config.optimize.bounds.long_unstuck_threshold_0, self.config.optimize.bounds.long_unstuck_threshold_1)
        st.slider(
            "long_unstuck_threshold",
            min_value=Bounds.UNSTUCK_THRESHOLD_MIN,
            max_value=Bounds.UNSTUCK_THRESHOLD_MAX,
            step=Bounds.UNSTUCK_THRESHOLD_STEP,
            format=Bounds.UNSTUCK_THRESHOLD_FORMAT,
            key="edit_opt_v7_long_unstuck_threshold",
            help=pbgui_help.unstuck_threshold)

    # # short_close_grid_markup_range
    # @st.fragment
    # def fragment_short_close_grid_markup_range(self):
    #     if "edit_opt_v7_short_close_grid_markup_range" in st.session_state:
    #         if st.session_state.edit_opt_v7_short_close_grid_markup_range != (self.config.optimize.bounds.short_close_grid_markup_range_0, self.config.optimize.bounds.short_close_grid_markup_range_1):
    #             self.config.optimize.bounds.short_close_grid_markup_range_0 = st.session_state.edit_opt_v7_short_close_grid_markup_range[0]
    #             self.config.optimize.bounds.short_close_grid_markup_range_1 = st.session_state.edit_opt_v7_short_close_grid_markup_range[1]
    #     else:
    #         st.session_state.edit_opt_v7_short_close_grid_markup_range = (self.config.optimize.bounds.short_close_grid_markup_range_0, self.config.optimize.bounds.short_close_grid_markup_range_1)
    #     st.slider(
    #         "short_close_grid_markup_range",
    #         min_value=Bounds.CLOSE_GRID_MARKUP_RANGE_MIN,
    #         max_value=Bounds.CLOSE_GRID_MARKUP_RANGE_MAX,
    #         step=Bounds.CLOSE_GRID_MARKUP_RANGE_STEP,
    #         format=Bounds.CLOSE_GRID_MARKUP_RANGE_FORMAT,
    #         key="edit_opt_v7_short_close_grid_markup_range",
    #         help=pbgui_help.close_grid_parameters)
    
    # # short_close_grid_min_markup
    # @st.fragment
    # def fragment_short_close_grid_min_markup(self):
    #     if "edit_opt_v7_short_close_grid_min_markup" in st.session_state:
    #         if st.session_state.edit_opt_v7_short_close_grid_min_markup != (self.config.optimize.bounds.short_close_grid_min_markup_0, self.config.optimize.bounds.short_close_grid_min_markup_1):
    #             self.config.optimize.bounds.short_close_grid_min_markup_0 = st.session_state.edit_opt_v7_short_close_grid_min_markup[0]
    #             self.config.optimize.bounds.short_close_grid_min_markup_1 = st.session_state.edit_opt_v7_short_close_grid_min_markup[1]
    #     else:
    #         st.session_state.edit_opt_v7_short_close_grid_min_markup = (self.config.optimize.bounds.short_close_grid_min_markup_0, self.config.optimize.bounds.short_close_grid_min_markup_1)
    #     st.slider(
    #         "short_close_grid_min_markup",
    #         min_value=Bounds.CLOSE_GRID_MIN_MARKUP_MIN,
    #         max_value=Bounds.CLOSE_GRID_MIN_MARKUP_MAX,
    #         step=Bounds.CLOSE_GRID_MIN_MARKUP_STEP,
    #         format=Bounds.CLOSE_GRID_MIN_MARKUP_FORMAT,
    #         key="edit_opt_v7_short_close_grid_min_markup",
    #         help=pbgui_help.close_grid_parameters)
    
    # short_close_grid_markup_end
    @st.fragment
    def fragment_short_close_grid_markup_end(self):
        if "edit_opt_v7_short_close_grid_markup_end" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_grid_markup_end != (self.config.optimize.bounds.short_close_grid_markup_end_0, self.config.optimize.bounds.short_close_grid_markup_end_1):
                self.config.optimize.bounds.short_close_grid_markup_end_0 = st.session_state.edit_opt_v7_short_close_grid_markup_end[0]
                self.config.optimize.bounds.short_close_grid_markup_end_1 = st.session_state.edit_opt_v7_short_close_grid_markup_end[1]
        else:
            st.session_state.edit_opt_v7_short_close_grid_markup_end = (self.config.optimize.bounds.short_close_grid_markup_end_0, self.config.optimize.bounds.short_close_grid_markup_end_1)
        st.slider(
            "short_close_grid_markup_end",
            min_value=Bounds.CLOSE_GRID_MARKUP_END_MIN,
            max_value=Bounds.CLOSE_GRID_MARKUP_END_MAX,
            step=Bounds.CLOSE_GRID_MARKUP_END_STEP,
            format=Bounds.CLOSE_GRID_MARKUP_END_FORMAT,
            key="edit_opt_v7_short_close_grid_markup_end",
            help=pbgui_help.close_grid_parameters)
    
    # short_close_grid_markup_start
    @st.fragment
    def fragment_short_close_grid_markup_start(self):
        if "edit_opt_v7_short_close_grid_markup_start" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_grid_markup_start != (self.config.optimize.bounds.short_close_grid_markup_start_0, self.config.optimize.bounds.short_close_grid_markup_start_1):
                self.config.optimize.bounds.short_close_grid_markup_start_0 = st.session_state.edit_opt_v7_short_close_grid_markup_start[0]
                self.config.optimize.bounds.short_close_grid_markup_start_1 = st.session_state.edit_opt_v7_short_close_grid_markup_start[1]
        else:
            st.session_state.edit_opt_v7_short_close_grid_markup_start = (self.config.optimize.bounds.short_close_grid_markup_start_0, self.config.optimize.bounds.short_close_grid_markup_start_1)
        st.slider(
            "short_close_grid_markup_start",
            min_value=Bounds.CLOSE_GRID_MARKUP_START_MIN,
            max_value=Bounds.CLOSE_GRID_MARKUP_START_MAX,
            step=Bounds.CLOSE_GRID_MARKUP_START_STEP,
            format=Bounds.CLOSE_GRID_MARKUP_START_FORMAT,
            key="edit_opt_v7_short_close_grid_markup_start",
            help=pbgui_help.close_grid_parameters)

    # short_close_grid_qty_pct
    @st.fragment
    def fragment_short_close_grid_qty_pct(self):
        if "edit_opt_v7_short_close_grid_qty_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_grid_qty_pct != (self.config.optimize.bounds.short_close_grid_qty_pct_0, self.config.optimize.bounds.short_close_grid_qty_pct_1):
                self.config.optimize.bounds.short_close_grid_qty_pct_0 = st.session_state.edit_opt_v7_short_close_grid_qty_pct[0]
                self.config.optimize.bounds.short_close_grid_qty_pct_1 = st.session_state.edit_opt_v7_short_close_grid_qty_pct[1]
        else:
            st.session_state.edit_opt_v7_short_close_grid_qty_pct = (self.config.optimize.bounds.short_close_grid_qty_pct_0, self.config.optimize.bounds.short_close_grid_qty_pct_1)
        st.slider(
            "short_close_grid_qty_pct",
            min_value=Bounds.CLOSE_GRID_QTY_PCT_MIN,
            max_value=Bounds.CLOSE_GRID_QTY_PCT_MAX,
            step=Bounds.CLOSE_GRID_QTY_PCT_STEP,
            format=Bounds.CLOSE_GRID_QTY_PCT_FORMAT,
            key="edit_opt_v7_short_close_grid_qty_pct",
            help=pbgui_help.close_grid_parameters)

    # short_close_trailing_grid_ratio
    @st.fragment
    def fragment_short_close_trailing_grid_ratio(self):
        if "edit_opt_v7_short_close_trailing_grid_ratio" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_trailing_grid_ratio != (self.config.optimize.bounds.short_close_trailing_grid_ratio_0, self.config.optimize.bounds.short_close_trailing_grid_ratio_1):
                self.config.optimize.bounds.short_close_trailing_grid_ratio_0 = st.session_state.edit_opt_v7_short_close_trailing_grid_ratio[0]
                self.config.optimize.bounds.short_close_trailing_grid_ratio_1 = st.session_state.edit_opt_v7_short_close_trailing_grid_ratio[1]
        else:
            st.session_state.edit_opt_v7_short_close_trailing_grid_ratio = (self.config.optimize.bounds.short_close_trailing_grid_ratio_0, self.config.optimize.bounds.short_close_trailing_grid_ratio_1)
        st.slider(
            "short_close_trailing_grid_ratio",
            min_value=Bounds.CLOSE_TRAILING_GRID_RATIO_MIN,
            max_value=Bounds.CLOSE_TRAILING_GRID_RATIO_MAX,
            step=Bounds.CLOSE_TRAILING_GRID_RATIO_STEP,
            format=Bounds.CLOSE_TRAILING_GRID_RATIO_FORMAT,
            key="edit_opt_v7_short_close_trailing_grid_ratio",
            help=pbgui_help.close_grid_parameters)
    
    # short_close_trailing_qty_pct
    @st.fragment
    def fragment_short_close_trailing_qty_pct(self):
        if "edit_opt_v7_short_close_trailing_qty_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_trailing_qty_pct != (self.config.optimize.bounds.short_close_trailing_qty_pct_0, self.config.optimize.bounds.short_close_trailing_qty_pct_1):
                self.config.optimize.bounds.short_close_trailing_qty_pct_0 = st.session_state.edit_opt_v7_short_close_trailing_qty_pct[0]
                self.config.optimize.bounds.short_close_trailing_qty_pct_1 = st.session_state.edit_opt_v7_short_close_trailing_qty_pct[1]
        else:
            st.session_state.edit_opt_v7_short_close_trailing_qty_pct = (self.config.optimize.bounds.short_close_trailing_qty_pct_0, self.config.optimize.bounds.short_close_trailing_qty_pct_1)
        st.slider(
            "short_close_trailing_qty_pct",
            min_value=Bounds.CLOSE_TRAILING_QTY_PCT_MIN,
            max_value=Bounds.CLOSE_TRAILING_QTY_PCT_MAX,
            step=Bounds.CLOSE_TRAILING_QTY_PCT_STEP,
            format=Bounds.CLOSE_TRAILING_QTY_PCT_FORMAT,
            key="edit_opt_v7_short_close_trailing_qty_pct",
            help=pbgui_help.trailing_parameters)
    
    # short_close_trailing_retracement_pct
    @st.fragment
    def fragment_short_close_trailing_retracement_pct(self):
        if "edit_opt_v7_short_close_trailing_retracement_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_trailing_retracement_pct != (self.config.optimize.bounds.short_close_trailing_retracement_pct_0, self.config.optimize.bounds.short_close_trailing_retracement_pct_1):
                self.config.optimize.bounds.short_close_trailing_retracement_pct_0 = st.session_state.edit_opt_v7_short_close_trailing_retracement_pct[0]
                self.config.optimize.bounds.short_close_trailing_retracement_pct_1 = st.session_state.edit_opt_v7_short_close_trailing_retracement_pct[1]
        else:
            st.session_state.edit_opt_v7_short_close_trailing_retracement_pct = (self.config.optimize.bounds.short_close_trailing_retracement_pct_0, self.config.optimize.bounds.short_close_trailing_retracement_pct_1)
        st.slider(
            "short_close_trailing_retracement_pct",
            min_value=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_MIN,
            max_value=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_MAX,
            step=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_STEP,
            format=Bounds.CLOSE_TRAILING_RETRACEMENT_PCT_FORMAT,
            key="edit_opt_v7_short_close_trailing_retracement_pct",
            help=pbgui_help.trailing_parameters)
    
    # short_close_trailing_threshold_pct
    @st.fragment
    def fragment_short_close_trailing_threshold_pct(self):
        if "edit_opt_v7_short_close_trailing_threshold_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_close_trailing_threshold_pct != (self.config.optimize.bounds.short_close_trailing_threshold_pct_0, self.config.optimize.bounds.short_close_trailing_threshold_pct_1):
                self.config.optimize.bounds.short_close_trailing_threshold_pct_0 = st.session_state.edit_opt_v7_short_close_trailing_threshold_pct[0]
                self.config.optimize.bounds.short_close_trailing_threshold_pct_1 = st.session_state.edit_opt_v7_short_close_trailing_threshold_pct[1]
        else:
            st.session_state.edit_opt_v7_short_close_trailing_threshold_pct = (self.config.optimize.bounds.short_close_trailing_threshold_pct_0, self.config.optimize.bounds.short_close_trailing_threshold_pct_1)
        st.slider(
            "short_close_trailing_threshold_pct",
            min_value=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_MIN,
            max_value=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_MAX,
            step=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_STEP,
            format=Bounds.CLOSE_TRAILING_THRESHOLD_PCT_FORMAT,
            key="edit_opt_v7_short_close_trailing_threshold_pct",
            help=pbgui_help.trailing_parameters)
    
    # short_ema_span_0
    @st.fragment
    def fragment_short_ema_span_0(self):
        if "edit_opt_v7_short_ema_span_0" in st.session_state:
            if st.session_state.edit_opt_v7_short_ema_span_0 != (self.config.optimize.bounds.short_ema_span_0_0, self.config.optimize.bounds.short_ema_span_0_1):
                self.config.optimize.bounds.short_ema_span_0_0 = st.session_state.edit_opt_v7_short_ema_span_0[0]
                self.config.optimize.bounds.short_ema_span_0_1 = st.session_state.edit_opt_v7_short_ema_span_0[1]
        else:
            st.session_state.edit_opt_v7_short_ema_span_0 = (self.config.optimize.bounds.short_ema_span_0_0, self.config.optimize.bounds.short_ema_span_0_1)
        st.slider(
            "short_ema_span_0",
            min_value=Bounds.EMA_SPAN_0_MIN,
            max_value=Bounds.EMA_SPAN_0_MAX,
            step=Bounds.EMA_SPAN_0_STEP,
            format=Bounds.EMA_SPAN_0_FORMAT,
            key="edit_opt_v7_short_ema_span_0",
            help=pbgui_help.ema_span)
    
    # short_ema_span_1
    @st.fragment
    def fragment_short_ema_span_1(self):
        if "edit_opt_v7_short_ema_span_1" in st.session_state:
            if st.session_state.edit_opt_v7_short_ema_span_1 != (self.config.optimize.bounds.short_ema_span_1_0, self.config.optimize.bounds.short_ema_span_1_1):
                self.config.optimize.bounds.short_ema_span_1_0 = st.session_state.edit_opt_v7_short_ema_span_1[0]
                self.config.optimize.bounds.short_ema_span_1_1 = st.session_state.edit_opt_v7_short_ema_span_1[1]
        else:
            st.session_state.edit_opt_v7_short_ema_span_1 = (self.config.optimize.bounds.short_ema_span_1_0, self.config.optimize.bounds.short_ema_span_1_1)
        st.slider(
            "short_ema_span_1",
            min_value=Bounds.EMA_SPAN_1_MIN,
            max_value=Bounds.EMA_SPAN_1_MAX,
            step=Bounds.EMA_SPAN_1_STEP,
            format=Bounds.EMA_SPAN_1_FORMAT,
            key="edit_opt_v7_short_ema_span_1",
            help=pbgui_help.ema_span)
    
    # short_entry_grid_double_down_factor
    @st.fragment
    def fragment_short_entry_grid_double_down_factor(self):
        if "edit_opt_v7_short_entry_grid_double_down_factor" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_grid_double_down_factor != (self.config.optimize.bounds.short_entry_grid_double_down_factor_0, self.config.optimize.bounds.short_entry_grid_double_down_factor_1):
                self.config.optimize.bounds.short_entry_grid_double_down_factor_0 = st.session_state.edit_opt_v7_short_entry_grid_double_down_factor[0]
                self.config.optimize.bounds.short_entry_grid_double_down_factor_1 = st.session_state.edit_opt_v7_short_entry_grid_double_down_factor[1]
        else:
            st.session_state.edit_opt_v7_short_entry_grid_double_down_factor = (self.config.optimize.bounds.short_entry_grid_double_down_factor_0, self.config.optimize.bounds.short_entry_grid_double_down_factor_1)
        st.slider(
            "short_entry_grid_double_down_factor",
            min_value=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_MIN,
            max_value=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_MAX,
            step=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_STEP,
            format=Bounds.ENTRY_GRID_DOUBLE_DOWN_FACTOR_FORMAT,
            key="edit_opt_v7_short_entry_grid_double_down_factor",
            help=pbgui_help.entry_grid_double_down_factor)

    # short_entry_grid_spacing_pct
    @st.fragment
    def fragment_short_entry_grid_spacing_pct(self):
        if "edit_opt_v7_short_entry_grid_spacing_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_grid_spacing_pct != (self.config.optimize.bounds.short_entry_grid_spacing_pct_0, self.config.optimize.bounds.short_entry_grid_spacing_pct_1):
                self.config.optimize.bounds.short_entry_grid_spacing_pct_0 = st.session_state.edit_opt_v7_short_entry_grid_spacing_pct[0]
                self.config.optimize.bounds.short_entry_grid_spacing_pct_1 = st.session_state.edit_opt_v7_short_entry_grid_spacing_pct[1]
        else:
            st.session_state.edit_opt_v7_short_entry_grid_spacing_pct = (self.config.optimize.bounds.short_entry_grid_spacing_pct_0, self.config.optimize.bounds.short_entry_grid_spacing_pct_1)
        st.slider(
            "short_entry_grid_spacing_pct",
            min_value=Bounds.ENTRY_GRID_SPACING_PCT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_PCT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_PCT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_PCT_FORMAT,
            key="edit_opt_v7_short_entry_grid_spacing_pct",
            help=pbgui_help.entry_grid_spacing)
    
    # short_entry_grid_spacing_weight
    @st.fragment
    def fragment_short_entry_grid_spacing_weight(self):
        if "edit_opt_v7_short_entry_grid_spacing_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_grid_spacing_weight != (self.config.optimize.bounds.short_entry_grid_spacing_weight_0, self.config.optimize.bounds.short_entry_grid_spacing_weight_1):
                self.config.optimize.bounds.short_entry_grid_spacing_weight_0 = st.session_state.edit_opt_v7_short_entry_grid_spacing_weight[0]
                self.config.optimize.bounds.short_entry_grid_spacing_weight_1 = st.session_state.edit_opt_v7_short_entry_grid_spacing_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_grid_spacing_weight = (self.config.optimize.bounds.short_entry_grid_spacing_weight_0, self.config.optimize.bounds.short_entry_grid_spacing_weight_1)
        st.slider(
            "short_entry_grid_spacing_weight",
            min_value=Bounds.ENTRY_GRID_SPACING_WEIGHT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_WEIGHT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_WEIGHT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_grid_spacing_weight",
            help=pbgui_help.entry_grid_spacing)
    
    # short_entry_initial_ema_dist
    @st.fragment
    def fragment_short_entry_initial_ema_dist(self):
        if "edit_opt_v7_short_entry_initial_ema_dist" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_initial_ema_dist != (self.config.optimize.bounds.short_entry_initial_ema_dist_0, self.config.optimize.bounds.short_entry_initial_ema_dist_1):
                self.config.optimize.bounds.short_entry_initial_ema_dist_0 = st.session_state.edit_opt_v7_short_entry_initial_ema_dist[0]
                self.config.optimize.bounds.short_entry_initial_ema_dist_1 = st.session_state.edit_opt_v7_short_entry_initial_ema_dist[1]
        else:
            st.session_state.edit_opt_v7_short_entry_initial_ema_dist = (self.config.optimize.bounds.short_entry_initial_ema_dist_0, self.config.optimize.bounds.short_entry_initial_ema_dist_1)
        st.slider(
            "short_entry_initial_ema_dist",
            min_value=Bounds.ENTRY_INITIAL_EMA_DIST_MIN,
            max_value=Bounds.ENTRY_INITIAL_EMA_DIST_MAX,
            step=Bounds.ENTRY_INITIAL_EMA_DIST_STEP,
            format=Bounds.ENTRY_INITIAL_EMA_DIST_FORMAT,
            key="edit_opt_v7_short_entry_initial_ema_dist",
            help=pbgui_help.entry_initial_ema_dist)

    # short_entry_initial_qty_pct
    @st.fragment
    def fragment_short_entry_initial_qty_pct(self):
        if "edit_opt_v7_short_entry_initial_qty_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_initial_qty_pct != (self.config.optimize.bounds.short_entry_initial_qty_pct_0, self.config.optimize.bounds.short_entry_initial_qty_pct_1):
                self.config.optimize.bounds.short_entry_initial_qty_pct_0 = st.session_state.edit_opt_v7_short_entry_initial_qty_pct[0]
                self.config.optimize.bounds.short_entry_initial_qty_pct_1 = st.session_state.edit_opt_v7_short_entry_initial_qty_pct[1]
        else:
            st.session_state.edit_opt_v7_short_entry_initial_qty_pct = (self.config.optimize.bounds.short_entry_initial_qty_pct_0, self.config.optimize.bounds.short_entry_initial_qty_pct_1)
        st.slider(
            "short_entry_initial_qty_pct",
            min_value=Bounds.ENTRY_INITIAL_QTY_PCT_MIN,
            max_value=Bounds.ENTRY_INITIAL_QTY_PCT_MAX,
            step=Bounds.ENTRY_INITIAL_QTY_PCT_STEP,
            format=Bounds.ENTRY_INITIAL_QTY_PCT_FORMAT,
            key="edit_opt_v7_short_entry_initial_qty_pct",
            help=pbgui_help.entry_initial_qty_pct)
    
    # short_entry_trailing_double_down_factor
    @st.fragment
    def fragment_short_entry_trailing_double_down_factor(self):
        if "edit_opt_v7_short_entry_trailing_double_down_factor" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_double_down_factor != (self.config.optimize.bounds.short_entry_trailing_double_down_factor_0, self.config.optimize.bounds.short_entry_trailing_double_down_factor_1):
                self.config.optimize.bounds.short_entry_trailing_double_down_factor_0 = st.session_state.edit_opt_v7_short_entry_trailing_double_down_factor[0]
                self.config.optimize.bounds.short_entry_trailing_double_down_factor_1 = st.session_state.edit_opt_v7_short_entry_trailing_double_down_factor[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_double_down_factor = (self.config.optimize.bounds.short_entry_trailing_double_down_factor_0, self.config.optimize.bounds.short_entry_trailing_double_down_factor_1)
        st.slider(
            "short_entry_trailing_double_down_factor",
            min_value=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MIN,
            max_value=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_MAX,
            step=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_STEP,
            format=Bounds.ENTRY_TRAILING_DOUBLE_DOWN_FACTOR_FORMAT,
            key="edit_opt_v7_short_entry_trailing_double_down_factor",
            help=pbgui_help.trailing_parameters)
    
    # short_entry_trailing_grid_ratio
    @st.fragment
    def fragment_short_entry_trailing_grid_ratio(self):
        if "edit_opt_v7_short_entry_trailing_grid_ratio" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_grid_ratio != (self.config.optimize.bounds.short_entry_trailing_grid_ratio_0, self.config.optimize.bounds.short_entry_trailing_grid_ratio_1):
                self.config.optimize.bounds.short_entry_trailing_grid_ratio_0 = st.session_state.edit_opt_v7_short_entry_trailing_grid_ratio[0]
                self.config.optimize.bounds.short_entry_trailing_grid_ratio_1 = st.session_state.edit_opt_v7_short_entry_trailing_grid_ratio[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_grid_ratio = (self.config.optimize.bounds.short_entry_trailing_grid_ratio_0, self.config.optimize.bounds.short_entry_trailing_grid_ratio_1)
        st.slider(
            "short_entry_trailing_grid_ratio",
            min_value=Bounds.ENTRY_TRAILING_GRID_RATIO_MIN,
            max_value=Bounds.ENTRY_TRAILING_GRID_RATIO_MAX,
            step=Bounds.ENTRY_TRAILING_GRID_RATIO_STEP,
            format=Bounds.ENTRY_TRAILING_GRID_RATIO_FORMAT,
            key="edit_opt_v7_short_entry_trailing_grid_ratio",
            help=pbgui_help.trailing_parameters)
    
    # short_entry_trailing_retracement_pct
    @st.fragment
    def fragment_short_entry_trailing_retracement_pct(self):
        if "edit_opt_v7_short_entry_trailing_retracement_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_retracement_pct != (self.config.optimize.bounds.short_entry_trailing_retracement_pct_0, self.config.optimize.bounds.short_entry_trailing_retracement_pct_1):
                self.config.optimize.bounds.short_entry_trailing_retracement_pct_0 = st.session_state.edit_opt_v7_short_entry_trailing_retracement_pct[0]
                self.config.optimize.bounds.short_entry_trailing_retracement_pct_1 = st.session_state.edit_opt_v7_short_entry_trailing_retracement_pct[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_retracement_pct = (self.config.optimize.bounds.short_entry_trailing_retracement_pct_0, self.config.optimize.bounds.short_entry_trailing_retracement_pct_1)
        st.slider(
            "short_entry_trailing_retracement_pct",
            min_value=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_MIN,
            max_value=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_MAX,
            step=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_STEP,
            format=Bounds.ENTRY_TRAILING_RETRACEMENT_PCT_FORMAT,
            key="edit_opt_v7_short_entry_trailing_retracement_pct",
            help=pbgui_help.trailing_parameters)
    
    # short_entry_trailing_threshold_pct
    @st.fragment
    def fragment_short_entry_trailing_threshold_pct(self):
        if "edit_opt_v7_short_entry_trailing_threshold_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_threshold_pct != (self.config.optimize.bounds.short_entry_trailing_threshold_pct_0, self.config.optimize.bounds.short_entry_trailing_threshold_pct_1):
                self.config.optimize.bounds.short_entry_trailing_threshold_pct_0 = st.session_state.edit_opt_v7_short_entry_trailing_threshold_pct[0]
                self.config.optimize.bounds.short_entry_trailing_threshold_pct_1 = st.session_state.edit_opt_v7_short_entry_trailing_threshold_pct[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_threshold_pct = (self.config.optimize.bounds.short_entry_trailing_threshold_pct_0, self.config.optimize.bounds.short_entry_trailing_threshold_pct_1)
        st.slider(
            "short_entry_trailing_threshold_pct",
            min_value=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_MIN,
            max_value=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_MAX,
            step=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_STEP,
            format=Bounds.ENTRY_TRAILING_THRESHOLD_PCT_FORMAT,
            key="edit_opt_v7_short_entry_trailing_threshold_pct",
            help=pbgui_help.trailing_parameters)
    
    # short_filter_noisiness_rolling_window
    @st.fragment
    def fragment_short_filter_noisiness_rolling_window(self):
        if "edit_opt_v7_short_filter_noisiness_rolling_window" in st.session_state:
            if st.session_state.edit_opt_v7_short_filter_noisiness_rolling_window != (self.config.optimize.bounds.short_filter_noisiness_rolling_window_0, self.config.optimize.bounds.short_filter_noisiness_rolling_window_1):
                self.config.optimize.bounds.short_filter_noisiness_rolling_window_0 = st.session_state.edit_opt_v7_short_filter_noisiness_rolling_window[0]
                self.config.optimize.bounds.short_filter_noisiness_rolling_window_1 = st.session_state.edit_opt_v7_short_filter_noisiness_rolling_window[1]
        else:
            st.session_state.edit_opt_v7_short_filter_noisiness_rolling_window = (self.config.optimize.bounds.short_filter_noisiness_rolling_window_0, self.config.optimize.bounds.short_filter_noisiness_rolling_window_1)
        st.slider(
            "short_filter_noisiness_rolling_window",
            min_value=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_MIN,
            max_value=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_MAX,
            step=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_STEP,
            format=Bounds.FILTER_NOISINESS_ROLLING_WINDOW_FORMAT,
            key="edit_opt_v7_short_filter_noisiness_rolling_window",
            help=pbgui_help.filter_rolling_window)
    
    # short_filter_volume_drop_pct
    @st.fragment
    def fragment_short_filter_volume_drop_pct(self):
        if "edit_opt_v7_short_filter_volume_drop_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_filter_volume_drop_pct != (self.config.optimize.bounds.short_filter_volume_drop_pct_0, self.config.optimize.bounds.short_filter_volume_drop_pct_1):
                self.config.optimize.bounds.short_filter_volume_drop_pct_0 = st.session_state.edit_opt_v7_short_filter_volume_drop_pct[0]
                self.config.optimize.bounds.short_filter_volume_drop_pct_1 = st.session_state.edit_opt_v7_short_filter_volume_drop_pct[1]
        else:
            st.session_state.edit_opt_v7_short_filter_volume_drop_pct = (self.config.optimize.bounds.short_filter_volume_drop_pct_0, self.config.optimize.bounds.short_filter_volume_drop_pct_1)
        st.slider(
            "short_filter_volume_drop_pct",
            min_value=Bounds.FILTER_VOLUME_DROP_PCT_MIN,
            max_value=Bounds.FILTER_VOLUME_DROP_PCT_MAX,
            step=Bounds.FILTER_VOLUME_DROP_PCT_STEP,
            format=Bounds.FILTER_VOLUME_DROP_PCT_FORMAT,
            key="edit_opt_v7_short_filter_volume_drop_pct",
            help=pbgui_help.filter_volume_drop_pct)

    # short_filter_volume_rolling_window
    @st.fragment
    def fragment_short_filter_volume_rolling_window(self):
        if "edit_opt_v7_short_filter_volume_rolling_window" in st.session_state:
            if st.session_state.edit_opt_v7_short_filter_volume_rolling_window != (self.config.optimize.bounds.short_filter_volume_rolling_window_0, self.config.optimize.bounds.short_filter_volume_rolling_window_1):
                self.config.optimize.bounds.short_filter_volume_rolling_window_0 = st.session_state.edit_opt_v7_short_filter_volume_rolling_window[0]
                self.config.optimize.bounds.short_filter_volume_rolling_window_1 = st.session_state.edit_opt_v7_short_filter_volume_rolling_window[1]
        else:
            st.session_state.edit_opt_v7_short_filter_volume_rolling_window = (self.config.optimize.bounds.short_filter_volume_rolling_window_0, self.config.optimize.bounds.short_filter_volume_rolling_window_1)
        st.slider(
            "short_filter_volume_rolling_window",
            min_value=Bounds.FILTER_VOLUME_ROLLING_WINDOW_MIN,
            max_value=Bounds.FILTER_VOLUME_ROLLING_WINDOW_MAX,
            step=Bounds.FILTER_VOLUME_ROLLING_WINDOW_STEP,
            format=Bounds.FILTER_VOLUME_ROLLING_WINDOW_FORMAT,
            key="edit_opt_v7_short_filter_volume_rolling_window",
            help=pbgui_help.filter_rolling_window)

    # short_n_positions
    @st.fragment
    def fragment_short_n_positions(self):
        if "edit_opt_v7_short_n_positions" in st.session_state:
            if st.session_state.edit_opt_v7_short_n_positions != (self.config.optimize.bounds.short_n_positions_0, self.config.optimize.bounds.short_n_positions_1):
                self.config.optimize.bounds.short_n_positions_0 = st.session_state.edit_opt_v7_short_n_positions[0]
                self.config.optimize.bounds.short_n_positions_1 = st.session_state.edit_opt_v7_short_n_positions[1]
        else:
            st.session_state.edit_opt_v7_short_n_positions = (self.config.optimize.bounds.short_n_positions_0, self.config.optimize.bounds.short_n_positions_1)
        st.slider(
            "short_n_positions",
            min_value=Bounds.N_POSITIONS_MIN,
            max_value=Bounds.N_POSITIONS_MAX,
            step=Bounds.N_POSITIONS_STEP,
            format=Bounds.N_POSITIONS_FORMAT,
            key="edit_opt_v7_short_n_positions",
            help=pbgui_help.n_positions)
    
    # short_total_wallet_exposure_limit
    @st.fragment
    def fragment_short_total_wallet_exposure_limit(self):
        if "edit_opt_v7_short_total_wallet_exposure_limit" in st.session_state:
            if st.session_state.edit_opt_v7_short_total_wallet_exposure_limit != (self.config.optimize.bounds.short_total_wallet_exposure_limit_0, self.config.optimize.bounds.short_total_wallet_exposure_limit_1):
                self.config.optimize.bounds.short_total_wallet_exposure_limit_0 = st.session_state.edit_opt_v7_short_total_wallet_exposure_limit[0]
                self.config.optimize.bounds.short_total_wallet_exposure_limit_1 = st.session_state.edit_opt_v7_short_total_wallet_exposure_limit[1]
        else:
            st.session_state.edit_opt_v7_short_total_wallet_exposure_limit = (self.config.optimize.bounds.short_total_wallet_exposure_limit_0, self.config.optimize.bounds.short_total_wallet_exposure_limit_1)
        st.slider(
            "short_total_wallet_exposure_limit",
            min_value=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_MIN,
            max_value=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_MAX,
            step=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_STEP,
            format=Bounds.TOTAL_WALLET_EXPOSURE_LIMIT_FORMAT,
            key="edit_opt_v7_short_total_wallet_exposure_limit",
            help=pbgui_help.total_wallet_exposure_limit)
    
    # short_unstuck_close_pct
    @st.fragment
    def fragment_short_unstuck_close_pct(self):
        if "edit_opt_v7_short_unstuck_close_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_unstuck_close_pct != (self.config.optimize.bounds.short_unstuck_close_pct_0, self.config.optimize.bounds.short_unstuck_close_pct_1):
                self.config.optimize.bounds.short_unstuck_close_pct_0 = st.session_state.edit_opt_v7_short_unstuck_close_pct[0]
                self.config.optimize.bounds.short_unstuck_close_pct_1 = st.session_state.edit_opt_v7_short_unstuck_close_pct[1]
        else:
            st.session_state.edit_opt_v7_short_unstuck_close_pct = (self.config.optimize.bounds.short_unstuck_close_pct_0, self.config.optimize.bounds.short_unstuck_close_pct_1)
        st.slider(
            "short_unstuck_close_pct",
            min_value=Bounds.UNSTUCK_CLOSE_PCT_MIN,
            max_value=Bounds.UNSTUCK_CLOSE_PCT_MAX,
            step=Bounds.UNSTUCK_CLOSE_PCT_STEP,
            format=Bounds.UNSTUCK_CLOSE_PCT_FORMAT,
            key="edit_opt_v7_short_unstuck_close_pct",
            help=pbgui_help.unstuck_close_pct)

    # short_unstuck_ema_dist
    @st.fragment
    def fragment_short_unstuck_ema_dist(self):
        if "edit_opt_v7_short_unstuck_ema_dist" in st.session_state:
            if st.session_state.edit_opt_v7_short_unstuck_ema_dist != (self.config.optimize.bounds.short_unstuck_ema_dist_0, self.config.optimize.bounds.short_unstuck_ema_dist_1):
                self.config.optimize.bounds.short_unstuck_ema_dist_0 = st.session_state.edit_opt_v7_short_unstuck_ema_dist[0]
                self.config.optimize.bounds.short_unstuck_ema_dist_1 = st.session_state.edit_opt_v7_short_unstuck_ema_dist[1]
        else:
            st.session_state.edit_opt_v7_short_unstuck_ema_dist = (self.config.optimize.bounds.short_unstuck_ema_dist_0, self.config.optimize.bounds.short_unstuck_ema_dist_1)
        st.slider(
            "short_unstuck_ema_dist",
            min_value=Bounds.UNSTUCK_EMA_DIST_MIN,
            max_value=Bounds.UNSTUCK_EMA_DIST_MAX,
            step=Bounds.UNSTUCK_EMA_DIST_STEP,
            format=Bounds.UNSTUCK_EMA_DIST_FORMAT,
            key="edit_opt_v7_short_unstuck_ema_dist",
            help=pbgui_help.unstuck_ema_dist)
    
    # short_unstuck_loss_allowance_pct
    @st.fragment
    def fragment_short_unstuck_loss_allowance_pct(self):
        if "edit_opt_v7_short_unstuck_loss_allowance_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_unstuck_loss_allowance_pct != (self.config.optimize.bounds.short_unstuck_loss_allowance_pct_0, self.config.optimize.bounds.short_unstuck_loss_allowance_pct_1):
                self.config.optimize.bounds.short_unstuck_loss_allowance_pct_0 = st.session_state.edit_opt_v7_short_unstuck_loss_allowance_pct[0]
                self.config.optimize.bounds.short_unstuck_loss_allowance_pct_1 = st.session_state.edit_opt_v7_short_unstuck_loss_allowance_pct[1]
        else:
            st.session_state.edit_opt_v7_short_unstuck_loss_allowance_pct = (self.config.optimize.bounds.short_unstuck_loss_allowance_pct_0, self.config.optimize.bounds.short_unstuck_loss_allowance_pct_1)
        st.slider(
            "short_unstuck_loss_allowance_pct",
            min_value=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_MIN,
            max_value=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_MAX,
            step=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_STEP,
            format=Bounds.UNSTUCK_LOSS_ALLOWANCE_PCT_FORMAT,
            key="edit_opt_v7_short_unstuck_loss_allowance_pct",
            help=pbgui_help.unstuck_loss_allowance_pct)
    
    # short_unstuck_threshold
    @st.fragment
    def fragment_short_unstuck_threshold(self):
        if "edit_opt_v7_short_unstuck_threshold" in st.session_state:
            if st.session_state.edit_opt_v7_short_unstuck_threshold != (self.config.optimize.bounds.short_unstuck_threshold_0, self.config.optimize.bounds.short_unstuck_threshold_1):
                self.config.optimize.bounds.short_unstuck_threshold_0 = st.session_state.edit_opt_v7_short_unstuck_threshold[0]
                self.config.optimize.bounds.short_unstuck_threshold_1 = st.session_state.edit_opt_v7_short_unstuck_threshold[1]
        else:
            st.session_state.edit_opt_v7_short_unstuck_threshold = (self.config.optimize.bounds.short_unstuck_threshold_0, self.config.optimize.bounds.short_unstuck_threshold_1)
        st.slider(
            "short_unstuck_threshold",
            min_value=Bounds.UNSTUCK_THRESHOLD_MIN,
            max_value=Bounds.UNSTUCK_THRESHOLD_MAX,
            step=Bounds.UNSTUCK_THRESHOLD_STEP,
            format=Bounds.UNSTUCK_THRESHOLD_FORMAT,
            key="edit_opt_v7_short_unstuck_threshold",
            help=pbgui_help.unstuck_threshold)

    @st.fragment
    def fragment_limits(self):
        LIMITS = [
            "adg",
            "adg_w",
            "calmar_ratio",
            "calmar_ratio_w",
            "drawdown_worst",
            "drawdown_worst_mean_1pct",
            "equity_balance_diff_neg_max",
            "equity_balance_diff_neg_mean",
            "equity_balance_diff_pos_max",
            "equity_balance_diff_pos_mean",
            "equity_choppiness",
            "equity_choppiness_w",
            "equity_jerkiness",
            "equity_jerkiness_w",
            "expected_shortfall_1pct",
            "exponential_fit_error",
            "exponential_fit_error_w",
            "gain",
            "loss_profit_ratio",
            "loss_profit_ratio_w",
            "mdg",
            "mdg_w",
            "omega_ratio",
            "omega_ratio_w",
            "position_held_hours_max",
            "position_held_hours_mean",
            "position_held_hours_median",
            "position_unchanged_hours_max",
            "positions_held_per_day",
            "sharpe_ratio",
            "sharpe_ratio_w",
            "sortino_ratio",
            "sortino_ratio_w",
            "sterling_ratio",
            "sterling_ratio_w",
        ]
        LIMITS_BTC = [
            "adg",
            "adg_w",
            "calmar_ratio",
            "calmar_ratio_w",
            "drawdown_worst",
            "drawdown_worst_mean_1pct",
            "equity_balance_diff_neg_max",
            "equity_balance_diff_neg_mean",
            "equity_balance_diff_pos_max",
            "equity_balance_diff_pos_mean",
            "equity_choppiness",
            "equity_choppiness_w",
            "equity_jerkiness",
            "equity_jerkiness_w",
            "expected_shortfall_1pct",
            "exponential_fit_error",
            "exponential_fit_error_w",
            "gain",
            "loss_profit_ratio",
            "loss_profit_ratio_w",
            "mdg",
            "mdg_w",
            "omega_ratio",
            "omega_ratio_w",
            "sharpe_ratio",
            "sharpe_ratio_w",
            "sortino_ratio",
            "sortino_ratio_w",
            "sterling_ratio",
            "sterling_ratio_w",
        ]
        # Edit limits
        if self.config.optimize.limits:
            limits = True
        else:
            limits = False
        with st.expander("Edit Limits", expanded=limits):
            # Init
            if not "ed_key" in st.session_state:
                st.session_state.ed_key = 0
            ed_key = st.session_state.ed_key
            if f'select_limits_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_limits_{ed_key}']
                for row in ed["edited_rows"]:
                    if "edit" in ed["edited_rows"][row]:
                        if ed["edited_rows"][row]["edit"]:
                            st.session_state.edit_limits = st.session_state.limits_data[row]["limit"]
                    if "delete" in ed["edited_rows"][row]:
                        if ed["edited_rows"][row]["delete"]:
                            self.config.optimize.limits.pop(st.session_state.limits_data[row]["limit"])
                            self.clean_limits_session_state()
                            st.rerun()
            if not "limits_data" in st.session_state:
                limits_data = []
                if self.config.optimize.limits:
                    for limit, value in self.config.optimize.limits.items():
                        limits_data.append({
                            "limit": limit,
                            "value": value,
                            "edit": False,
                            "delete": False
                        })
                st.session_state.limits_data = limits_data
            # Display limits
            if st.session_state.limits_data and not "edit_limits" in st.session_state:
                d = st.session_state.limits_data
                st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_limits_{ed_key}', disabled=['limit','value'])
            if "edit_opt_v7_add_limits_greater" in st.session_state:
                if st.session_state.edit_opt_v7_add_limits_greater_button:
                    self.config.optimize.limits[f'penalize_if_greater_than_{st.session_state.edit_opt_v7_add_limits_greater}'] = st.session_state.edit_opt_v7_add_limits_greater_value
                    self.clean_limits_session_state()
                    st.rerun()
            if "edit_opt_v7_add_limits_lower" in st.session_state:
                if st.session_state.edit_opt_v7_add_limits_lower_button:
                    self.config.optimize.limits[f'penalize_if_lower_than_{st.session_state.edit_opt_v7_add_limits_lower}'] = st.session_state.edit_opt_v7_add_limits_lower_value
                    self.clean_limits_session_state()
                    st.rerun()
            if "edit_opt_v7_add_limits_greater_btc" in st.session_state:
                if st.session_state.edit_opt_v7_add_limits_greater_btc_button:
                    self.config.optimize.limits[f'penalize_if_greater_than_btc_{st.session_state.edit_opt_v7_add_limits_greater_btc}'] = st.session_state.edit_opt_v7_add_limits_greater_value_btc
                    self.clean_limits_session_state()
                    st.rerun()
            if "edit_opt_v7_add_limits_lower_btc" in st.session_state:
                if st.session_state.edit_opt_v7_add_limits_lower_btc_button:
                    self.config.optimize.limits[f'penalize_if_lower_than_btc_{st.session_state.edit_opt_v7_add_limits_lower_btc}'] = st.session_state.edit_opt_v7_add_limits_lower_value_btc
                    self.clean_limits_session_state()
                    st.rerun()
            if "edit_limits" in st.session_state:
                if st.session_state.edit_limits in self.config.optimize.limits:
                    self.edit_limits(st.session_state.edit_limits, self.config.optimize.limits[st.session_state.edit_limits])
                else:
                    self.edit_limits(st.session_state.edit_limits)
            else:
                col1, col2, col3, col4, col5, col6 = st.columns([1,0.5,0.5,1,0.5,0.5], vertical_alignment="bottom")
                with col1:
                    st.selectbox('penalize_if_greater_than', LIMITS, key="edit_opt_v7_add_limits_greater", help=pbgui_help.limits)
                with col2:
                    st.number_input("limit", format="%.5f", key="edit_opt_v7_add_limits_greater_value")
                with col3:
                    st.button("Add Limit", key="edit_opt_v7_add_limits_greater_button")
                with col4:
                    st.selectbox('penalize_if_lower_than', LIMITS, key="edit_opt_v7_add_limits_lower", help=pbgui_help.limits)
                with col5:
                    st.number_input("limit", format="%.5f", key="edit_opt_v7_add_limits_lower_value")
                with col6:
                    st.button("Add Limit", key="edit_opt_v7_add_limits_lower_button")
                # btc
                col1, col2, col3, col4, col5, col6 = st.columns([1,0.5,0.5,1,0.5,0.5], vertical_alignment="bottom")
                with col1:
                    st.selectbox('penalize_if_greater_than_btc', LIMITS_BTC, key="edit_opt_v7_add_limits_greater_btc", help=pbgui_help.limits)
                with col2:
                    st.number_input("limit", format="%.5f", key="edit_opt_v7_add_limits_greater_value_btc")
                with col3:
                    st.button("Add Limit", key="edit_opt_v7_add_limits_greater_btc_button")
                with col4:
                    st.selectbox('penalize_if_lower_than_btc', LIMITS_BTC, key="edit_opt_v7_add_limits_lower_btc", help=pbgui_help.limits)
                with col5:
                    st.number_input("limit", format="%.5f", key="edit_opt_v7_add_limits_lower_value_btc")
                with col6:
                    st.button("Add Limit", key="edit_opt_v7_add_limits_lower_btc_button")


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
        col1, col2, col3, col4, col5 = st.columns([1,1,1,0.5,0.5], vertical_alignment="bottom")
        with col1:
            self.fragment_starting_balance()
        with col2:
            self.fragment_iters()
        with col3:
            self.fragment_n_cpus()
        with col4:
            self.fragment_starting_config()
            self.fragment_use_btc_collateral()
        with col5:
            self.fragment_combine_ohlcvs()
            self.fragment_compress_results_file()
        with st.expander("Edit Config", expanded=False):
            self.config.bot.edit()

        # Limits
        self.fragment_limits()
        
        # Optimizer
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_population_size()
        with col2:
            self.fragment_crossover_probability()
        with col3:
            self.fragment_mutation_probability()
        with col4:
            self.fragment_scoring()

        # Filters
        self.fragment_filter_coins()

        # Optimizer Bounds
        col1, col2 = st.columns([1,1])
        with col1:
            with st.container(border=True):
                st.write("Bounds long")
                # self.fragment_long_close_grid_markup_range()
                # self.fragment_long_close_grid_min_markup()
                self.fragment_long_close_grid_markup_end()
                self.fragment_long_close_grid_markup_start()
                self.fragment_long_close_grid_qty_pct()
                self.fragment_long_close_trailing_grid_ratio()
                self.fragment_long_close_trailing_qty_pct()
                self.fragment_long_close_trailing_retracement_pct()
                self.fragment_long_close_trailing_threshold_pct()
                self.fragment_long_ema_span_0()
                self.fragment_long_ema_span_1()
                self.fragment_long_entry_grid_double_down_factor()
                self.fragment_long_entry_grid_spacing_pct()
                self.fragment_long_entry_grid_spacing_weight()
                self.fragment_long_entry_initial_ema_dist()
                self.fragment_long_entry_initial_qty_pct()
                self.fragment_long_entry_trailing_double_down_factor()
                self.fragment_long_entry_trailing_grid_ratio()
                self.fragment_long_entry_trailing_retracement_pct()
                self.fragment_long_entry_trailing_threshold_pct()
                self.fragment_long_filter_noisiness_rolling_window()
                self.fragment_long_filter_volume_drop_pct()
                self.fragment_long_filter_volume_rolling_window()
                self.fragment_long_n_positions()
                self.fragment_long_total_wallet_exposure_limit()
                self.fragment_long_unstuck_close_pct()
                self.fragment_long_unstuck_ema_dist()
                self.fragment_long_unstuck_loss_allowance_pct()
                self.fragment_long_unstuck_threshold()

        with col2:
            with st.container(border=True):
                st.write("Bounds short")
                # self.fragment_short_close_grid_markup_range()
                # self.fragment_short_close_grid_min_markup()
                self.fragment_short_close_grid_markup_end()
                self.fragment_short_close_grid_markup_start()
                self.fragment_short_close_grid_qty_pct()
                self.fragment_short_close_trailing_grid_ratio()
                self.fragment_short_close_trailing_qty_pct()
                self.fragment_short_close_trailing_retracement_pct()
                self.fragment_short_close_trailing_threshold_pct()
                self.fragment_short_ema_span_0()
                self.fragment_short_ema_span_1()
                self.fragment_short_entry_grid_double_down_factor()
                self.fragment_short_entry_grid_spacing_pct()
                self.fragment_short_entry_grid_spacing_weight()
                self.fragment_short_entry_initial_ema_dist()
                self.fragment_short_entry_initial_qty_pct()
                self.fragment_short_entry_trailing_double_down_factor()
                self.fragment_short_entry_trailing_grid_ratio()
                self.fragment_short_entry_trailing_retracement_pct()
                self.fragment_short_entry_trailing_threshold_pct()
                self.fragment_short_filter_noisiness_rolling_window()
                self.fragment_short_filter_volume_drop_pct()
                self.fragment_short_filter_volume_rolling_window()
                self.fragment_short_n_positions()
                self.fragment_short_total_wallet_exposure_limit()
                self.fragment_short_unstuck_close_pct()
                self.fragment_short_unstuck_ema_dist()
                self.fragment_short_unstuck_loss_allowance_pct()
                self.fragment_short_unstuck_threshold()

    def edit_limits(self, limit, value = 0.0):
        value = float(value)
        st.number_input(limit, value=value, format="%.5f", key="edit_opt_v7_limits", help=pbgui_help.limits)
        col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
        with col1:
            if st.button("Save"):
                self.config.optimize.limits[limit] = st.session_state.edit_opt_v7_limits
                self.clean_limits_session_state()
                st.rerun()
        with col2:
            if st.button("Cancel"):
                self.clean_limits_session_state()
                st.rerun()
        with col3:
            if st.button("Delete"):
                self.config.optimize.limits.pop(limit)
                self.clean_limits_session_state()
                st.rerun()

    def clean_limits_session_state(self):
        if "edit_opt_v7_limits" in st.session_state:
            del st.session_state.edit_opt_v7_limits
        if "limits_data" in st.session_state:
            del st.session_state.limits_data
        if "ed_key" in st.session_state:
            del st.session_state.ed_key
        if "edit_limits" in st.session_state:
            del st.session_state.edit_limits
        if "edit_opt_v7_add_limits_greater_button" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_greater_button
        if "edit_opt_v7_add_limits_greater_value" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_greater_value
        if "edit_opt_v7_add_limits_lower_button" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_lower_button
        if "edit_opt_v7_add_limits_lower_value" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_lower_value
        if "edit_opt_v7_add_limits_greater_btc_button" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_greater_btc_button
        if "edit_opt_v7_add_limits_greater_value_btc" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_greater_value_btc
        if "edit_opt_v7_add_limits_lower_btc_button" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_lower_btc_button
        if "edit_opt_v7_add_limits_lower_value_btc" in st.session_state:
            del st.session_state.edit_opt_v7_add_limits_lower_value_btc

    def save(self):
        self.path = Path(f'{PBGDIR}/data/opt_v7')
        if not self.path.exists():
            self.path.mkdir(parents=True)
        
        # Prevent creating directories with / in the name
        self.name = self.name.replace("/", "_")
        
        self.config.config_file = Path(f'{self.path}/{self.name}.json')
        self.config.save_config()

    def save_queue(self):
        dest = Path(f'{PBGDIR}/data/opt_v7_queue')
        unique_filename = str(uuid.uuid4())
        file = Path(f'{dest}/{unique_filename}.json') 
        opt_dict = {
            "name": self.name,
            "filename": unique_filename,
            "json": str(self.config.config_file),
            "exchange": self.config.backtest.exchanges
        }
        dest.mkdir(parents=True, exist_ok=True)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(opt_dict, f, indent=4)

    def remove(self):
        Path(self.config.config_file).unlink(missing_ok=True)

class OptimizesV7:
    def __init__(self):
        self.optimizes = []

    def view_optimizes(self):
        # Init
        if not self.optimizes:
            self.find_optimizes()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if f'select_optimize_v7_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_optimize_v7_{ed_key}']
            for row in ed["edited_rows"]:
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.opt_v7 = self.optimizes[row]
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
                'Date': opt.time,
                'Exchange': opt.config.backtest.exchanges,
                'BT Count': opt.backtest_count,
                'delete' : False,
            })
        column_config = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "Date": st.column_config.DatetimeColumn(label="Date", format="YYYY-MM-DD HH:mm:ss"),
            }
        #Display optimizes
        st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f'select_optimize_v7_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def find_optimizes(self):
        p = str(Path(f'{PBGDIR}/data/opt_v7/*.json'))
        found_opt = glob.glob(p, recursive=False)
        if found_opt:
            for p in found_opt:
                opt = OptimizeV7Item(p)
                self.optimizes.append(opt)
    
def main():
    # Disable Streamlit Warnings when running directly
    logging.getLogger("streamlit.runtime.state.session_state_proxy").disabled=True
    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").disabled=True
    opt = OptimizeV7Queue()
    while True:
        opt.load()
        for item in opt.items:
            while opt.running():
                time.sleep(5)
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if not eval(pb_config.get("optimize_v7", "autostart")):
                return
            if item.status() == "not started" or item.status() == "error":
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Optimizing {item.filename} started')
                item.run()
                time.sleep(1)
        time.sleep(60)

if __name__ == '__main__':
    main()