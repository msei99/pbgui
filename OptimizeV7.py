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
from Exchange import Exchange, V7
from PBCoinData import CoinData
from pbgui_func import pb7dir, pb7venv, PBGDIR, load_symbols_from_ini, error_popup, info_popup, get_navi_paths, replace_special_chars
import uuid
from pathlib import Path, PurePath
from User import Users
import shutil
import datetime
import BacktestV7
from Config import ConfigV7, Bounds, Logging, SHARED_METRICS, CURRENCY_METRICS, get_all_metrics_list, is_currency_metric, ALLOWED_OVERRIDES, get_aggregate_metrics, ConfigV7Editor
from PBCoinData import normalize_symbol
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
        self.log_show = False
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

    def is_existing(self):
        if Path(f'{PBGDIR}/data/opt_v7_queue/{self.filename}.json').exists():
            return True
        return False

    def is_running(self):
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
        self.d = []
        self.sort = "Time"
        self.sort_order = True
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("optimize_v7"):
            pb_config.add_section("optimize_v7")
            pb_config.set("optimize_v7", "autostart", "False")
            with open('pbgui.ini', 'w') as f:
                pb_config.write(f)
        self._autostart = eval(pb_config.get("optimize_v7", "autostart"))
        self.load_sort_queue()
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
        self.refresh()

    def remove_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'view_opt_v7_queue_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    self.d[row]["item"].remove()
                    self.items.remove(self.d[row]["item"])
        self.refresh()

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
        # Remove items that are not existing anymore
        for item in self.items[:]:
            if not Path(f'{PBGDIR}/data/opt_v7_queue/{item.filename}.json').exists():
                item.remove()
                self.items.remove(item)

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
                    'edit': False,
                    'log': item.log_show,
                    'delete': False,
                    'starting_config': item.starting_config,
                    'name': item.name,
                    'filename': item.filename,
                    'Time': datetime.datetime.fromtimestamp(Path(f'{PBGDIR}/data/opt_v7_queue/{item.filename}.json').stat().st_mtime),
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
        if f'view_opt_v7_queue_{ed_key}' in st.session_state:
            ed = st.session_state[f'view_opt_v7_queue_{ed_key}']
            for row in ed["edited_rows"]:
                if "run" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["run"]:
                        self.d[row]["item"].run()
                    else:
                        self.d[row]["item"].stop()
                    self.refresh()
                if "edit" in ed["edited_rows"][row]:
                    st.session_state.opt_v7 = OptimizeV7Item(f'{PBGDIR}/data/opt_v7/{self.d[row]["item"].name}.json')
                    del st.session_state.opt_v7_queue
                    st.rerun()
                if "log" in ed["edited_rows"][row]:
                    self.d[row]["item"].log_show = ed["edited_rows"][row]["log"]
                    self.d[row]["log"] = ed["edited_rows"][row]["log"]
        column_config = {
            # "id": None,
            "run": st.column_config.CheckboxColumn('Start/Stop', default=False),
            "edit": st.column_config.CheckboxColumn('Edit'),
            "log": st.column_config.CheckboxColumn(label="View Logfile"),
            "delete": st.column_config.CheckboxColumn(label="Delete"),
            "item": None,
            }
        #Display Queue
        height = 36+(len(self.d))*35
        if height > 1000: height = 1016
        if "sort_opt_v7_queue" in st.session_state:
            if st.session_state.sort_opt_v7_queue != self.sort:
                self.sort = st.session_state.sort_opt_v7_queue
                self.save_sort_queue()
        else:
            st.session_state.sort_opt_v7_queue = self.sort
        if "sort_opt_v7_queue_order" in st.session_state:
            if st.session_state.sort_opt_v7_queue_order != self.sort_order:
                self.sort_order = st.session_state.sort_opt_v7_queue_order
                self.save_sort_queue()
        else:
            st.session_state.sort_opt_v7_queue_order = self.sort_order
        # Display sort options
        col1, col2 = st.columns([1, 9], vertical_alignment="bottom")
        with col1:
            st.selectbox("Sort by:", ['Time', 'name', 'Status', 'exchange', 'finish'], key=f'sort_opt_v7_queue', index=0)
        with col2:
            st.checkbox("Reverse", value=True, key=f'sort_opt_v7_queue_order')
        self.d = sorted(self.d, key=lambda x: x[st.session_state[f'sort_opt_v7_queue']], reverse=st.session_state[f'sort_opt_v7_queue_order'])
        st.data_editor(data=self.d, height="auto", key=f'view_opt_v7_queue_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','filename','starting_config','name','finish','running'])
        for item in self.items:
            if item.log_show:
                item.view_log()

    def load_sort_queue(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.sort = pb_config.get("optimize_v7", "sort_queue") if pb_config.has_option("optimize_v7", "sort_queue") else "Time"
        self.sort_order = eval(pb_config.get("optimize_v7", "sort_queue_order")) if pb_config.has_option("optimize_v7", "sort_queue_order") else True

    def save_sort_queue(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("optimize_v7"):
            pb_config.add_section("optimize_v7")
        pb_config.set("optimize_v7", "sort_queue", str(self.sort))
        pb_config.set("optimize_v7", "sort_queue_order", str(self.sort_order))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

class OptimizeV7Results:
    def __init__(self):
        self.results_path = Path(f'{pb7dir()}/optimize_results')
        self.selected_analysis = "analyses_combined"
        self.results_new = []
        self.results_d = []
        self.sort_results = "Result Time"
        self.sort_results_order = True
        self.paretos = []
        self.filter = ""
        self.initialize()
        self.load_sort_results()
    
    def initialize(self):
        self.find_results()
    
    def find_results(self):
        if self.results_path.exists():
            p = str(self.results_path) + "/*/all_results.bin"
            self.results_new = glob.glob(p, recursive=False)
            self.results_d = []
    
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

    def view_results(self):
        # Init
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key

        if "select_opt_v7_result_filter" in st.session_state:
            if st.session_state.select_opt_v7_result_filter != self.filter:
                self.filter = st.session_state.select_opt_v7_result_filter
                self.results_new = []
                self.results_d = []
                self.find_results()
        else:
            st.session_state.select_opt_v7_result_filter = self.filter

        # Remove results that are not in the filter
        if not self.filter == "":
            for result in self.results_new.copy():
                name, result_time = self.find_result_name_new(result)
                if not name:
                    self.results_new.remove(result)
                    continue
                if not fnmatch.fnmatch(name.lower(), self.filter.lower()):
                    self.results_new.remove(result)

        st.text_input("Filter by Optimize Name", value="", help=pbgui_help.smart_filter, key="select_opt_v7_result_filter")

        if not self.results_d:
            for id, opt in enumerate(self.results_new):
                name, result_time = self.find_result_name_new(opt)
                if name:
                    result = PurePath(opt).parent.name
                    self.results_d.append({
                        'id': id,
                        'Name': name,
                        'Result Time': datetime.datetime.fromtimestamp(result_time),
                        'view': False,
                        '3d plot': False,
                        '🎯 explorer': False,
                        'delete' : False,
                        'Result': result,
                        'index': opt,
                    })
        column_config_new = {
            "id": None,
            "edit": st.column_config.CheckboxColumn(label="Edit"),
            "view": st.column_config.CheckboxColumn(label="View Paretos"),
            "🎯 explorer": st.column_config.CheckboxColumn(label="🎯 Genius Explorer"),
            "Result Time": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss"),
            "Result": st.column_config.TextColumn(label="Result Directory", width="50px"),
        }
        if "sort_opt_v7_results" in st.session_state:
            if st.session_state.sort_opt_v7_results != self.sort_results:
                self.sort_results = st.session_state.sort_opt_v7_results
                self.save_sort_results()
        else:
            st.session_state.sort_opt_v7_results = self.sort_results
        if "sort_opt_v7_results_order" in st.session_state:
            if st.session_state.sort_opt_v7_results_order != self.sort_results_order:
                self.sort_results_order = st.session_state.sort_opt_v7_results_order
                self.save_sort_results()
        else:
            st.session_state.sort_opt_v7_results_order = self.sort_results_order
        # Display sort options
        col1, col2 = st.columns([1, 9], vertical_alignment="bottom")
        with col1:
            st.selectbox("Sort by:", ['Result Time', 'Name'], key=f'sort_opt_v7_results', index=0)
        with col2:
            st.checkbox("Reverse", value=True, key=f'sort_opt_v7_results_order')
        # Sort results
        self.results_d = sorted(self.results_d, key=lambda x: x[st.session_state[f'sort_opt_v7_results']], reverse=st.session_state[f'sort_opt_v7_results_order'])
        #Display optimizes
        st.data_editor(data=self.results_d, height=36+(len(self.results_d))*35, key=f'select_optresults_new_{st.session_state.ed_key}', hide_index=None, column_order=None, column_config=column_config_new, disabled=['id','name','index'])
        if f'select_optresults_new_{st.session_state.ed_key}' in st.session_state:
            ed = st.session_state[f'select_optresults_new_{st.session_state.ed_key}']
            for row in ed["edited_rows"]:
                if "view" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["view"]:
                        if self.results_d[row]["Result"]:
                            st.session_state.opt_v7_pareto = self.results_d[row]["index"]
                            st.session_state.opt_v7_pareto_name = self.results_d[row]["Name"]
                            st.session_state.opt_v7_pareto_directory = self.results_d[row]["Result"]
                            st.rerun()
                if "3d plot" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["3d plot"]:
                        self.run_3d_plot(self.results_d[row]["index"])
                        st.session_state.ed_key += 1
                if "🎯 explorer" in ed["edited_rows"][row]:
                    if ed["edited_rows"][row]["🎯 explorer"]:
                        self.run_pareto_explorer(self.results_d[row]["index"])
                        st.session_state.ed_key += 1

    def load_sort_results(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.sort_results = pb_config.get("optimize_v7", "sort_results") if pb_config.has_option("optimize_v7", "sort_results") else "Result Time"
        self.sort_results_order = eval(pb_config.get("optimize_v7", "sort_results_order")) if pb_config.has_option("optimize_v7", "sort_results_order") else True

    def save_sort_results(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("optimize_v7"):
            pb_config.add_section("optimize_v7")
        pb_config.set("optimize_v7", "sort_results", str(self.sort_results))
        pb_config.set("optimize_v7", "sort_results_order", str(self.sort_results_order))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

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
    
    def run_pareto_explorer(self, index):
        """Open Genius Pareto Explorer within PBGui"""
        results_dir = Path(index).parent
        
        # Check if all_results.bin exists
        all_results_path = results_dir / "all_results.bin"
        if not all_results_path.exists():
            error_popup(f"❌ all_results.bin not found in {results_dir}")
            return
        
        # Store path in session state and navigate to explorer
        st.session_state.pareto_explorer_path = str(results_dir)
        st.switch_page("navi/v7_pareto_explorer.py")

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
        
        # Detect format:
        # - Suite format: has "suite_metrics" with scenarios
        # - Non-suite format: has "metrics" with stats but no suite_metrics
        # - Old format: has "analyses_combined"
        has_suite_metrics = "suite_metrics" in self.paretos[0]
        has_metrics_only = "metrics" in self.paretos[0] and not has_suite_metrics
        is_new_format = has_suite_metrics or has_metrics_only
        
        if has_suite_metrics:
            # Suite format: get scenario labels
            scenario_labels = self.paretos[0].get("suite_metrics", {}).get("scenario_labels", [])
        elif has_metrics_only:
            # Non-suite format: no scenarios, only single result
            scenario_labels = []
        else:
            # Old format
            if "analyses_combined" in self.paretos[0]:
                select_analysis.append("analyses_combined")
            if "analyses" in self.paretos[0]:
                for analyse in self.paretos[0]["analyses"]:
                    select_analysis.append(analyse)
            
            # Validate and fix the selected analysis BEFORE using it
            if select_analysis:
                if "opt_v7_pareto_select_analysis" not in st.session_state:
                    st.session_state.opt_v7_pareto_select_analysis = select_analysis[0]
                    self.selected_analysis = select_analysis[0]
                elif st.session_state.opt_v7_pareto_select_analysis not in select_analysis:
                    # Current selection is invalid, reset to first available
                    st.session_state.opt_v7_pareto_select_analysis = select_analysis[0]
                    self.selected_analysis = select_analysis[0]
                    if "d_paretos" in st.session_state:
                        del st.session_state.d_paretos
        
        def clear_paretos():
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
        
        # New format: Show dropdowns based on whether suite is enabled
        if is_new_format:
            if has_suite_metrics:
                # Suite format: Two dropdowns for Scenario and Statistic
                if "opt_v7_pareto_scenario" not in st.session_state:
                    st.session_state.opt_v7_pareto_scenario = "Aggregated"
                if "opt_v7_pareto_statistic" not in st.session_state:
                    st.session_state.opt_v7_pareto_statistic = "mean"
                
                col1, col2, col3 = st.columns([1, 1, 2], gap="small")
                with col1:
                    scenario_options = ["Aggregated"] + scenario_labels
                    st.selectbox('Scenario', options=scenario_options, key="opt_v7_pareto_scenario", on_change=clear_paretos)
                with col2:
                    # Statistic dropdown - only enabled for Aggregated
                    is_aggregated = st.session_state.opt_v7_pareto_scenario == "Aggregated"
                    stat_options = ["mean", "min", "max", "std"]
                    st.selectbox('Statistic', options=stat_options, key="opt_v7_pareto_statistic", 
                                disabled=not is_aggregated, on_change=clear_paretos,
                                help=pbgui_help.stats_aggregation_help)
            else:
                # Non-suite format: Only Statistic dropdown (no scenarios)
                if "opt_v7_pareto_statistic" not in st.session_state:
                    st.session_state.opt_v7_pareto_statistic = "mean"
                
                col1, col2 = st.columns([1, 3], gap="small")
                with col1:
                    stat_options = ["mean", "min", "max", "std"]
                    st.selectbox('Statistic', options=stat_options, key="opt_v7_pareto_statistic", 
                                on_change=clear_paretos,
                                help=pbgui_help.stats_value_help)
        else:
            # Old format: single dropdown
            col1, col2 = st.columns([1, 3], gap="small")
            with col1:
                if select_analysis:
                    if st.session_state.opt_v7_pareto_select_analysis != self.selected_analysis:
                        self.selected_analysis = st.session_state.opt_v7_pareto_select_analysis
                    st.selectbox('analyses', options=select_analysis, key="opt_v7_pareto_select_analysis", on_change=clear_paretos)
        
        if not "d_paretos" in st.session_state:
            d = []
            for id, pareto in enumerate(self.paretos):
                name = pareto["index_filename"].split("/")[-1]
                
                if is_new_format:
                    if has_suite_metrics:
                        # Suite format: extract from suite_metrics
                        suite_metrics = pareto.get("suite_metrics", {}).get("metrics", {})
                        
                        # Determine which value to extract based on scenario/statistic selection
                        selected_scenario = st.session_state.opt_v7_pareto_scenario
                        selected_stat = st.session_state.opt_v7_pareto_statistic
                        
                        # Helper to get the correct value based on scenario and statistic
                        def get_metric_value(metric_name, default=0):
                            metric_data = suite_metrics.get(metric_name, {})
                            if selected_scenario == "Aggregated":
                                # Use the stats aggregation
                                return metric_data.get("stats", {}).get(selected_stat, default)
                            else:
                                # Use the specific scenario value
                                return metric_data.get("scenarios", {}).get(selected_scenario, default)
                        
                        d.append({
                            'Select': False,
                            'id': id,
                            'view': False,
                            'adg': get_metric_value("adg_usd"),
                            'mdg': get_metric_value("mdg_usd"),
                            'drawdown_worst': get_metric_value("drawdown_worst_usd"),
                            'gain': get_metric_value("gain_usd"),
                            'loss_profit_ratio': get_metric_value("loss_profit_ratio"),
                            'position_held_hours_max': get_metric_value("position_held_hours_max"),
                            'sharpe_ratio': get_metric_value("sharpe_ratio_usd"),
                            'Name': name,
                            'file': pareto["index_filename"],
                        })
                    else:
                        # Non-suite format: extract from metrics.stats
                        metrics_stats = pareto.get("metrics", {}).get("stats", {})
                        selected_stat = st.session_state.opt_v7_pareto_statistic
                        
                        # Helper to get the stat value
                        def get_stat_value(metric_name, default=0):
                            return metrics_stats.get(metric_name, {}).get(selected_stat, default)
                        
                        d.append({
                            'Select': False,
                            'id': id,
                            'view': False,
                            'adg': get_stat_value("adg_usd"),
                            'mdg': get_stat_value("mdg_usd"),
                            'drawdown_worst': get_stat_value("drawdown_worst_usd"),
                            'gain': get_stat_value("gain_usd"),
                            'loss_profit_ratio': get_stat_value("loss_profit_ratio"),
                            'position_held_hours_max': get_stat_value("position_held_hours_max"),
                            'sharpe_ratio': get_stat_value("sharpe_ratio_usd"),
                            'Name': name,
                            'file': pareto["index_filename"],
                        })
                else:
                    # Old format
                    if select_analysis and st.session_state.opt_v7_pareto_select_analysis in select_analysis:
                        if st.session_state.opt_v7_pareto_select_analysis == "analyses_combined":
                            analysis = pareto["analyses_combined"]
                            # Support both old format (_max suffix) and new format (_mean suffix)
                            adg = analysis.get("adg_max", analysis.get("adg_mean", 0))
                            mdg = analysis.get("mdg_max", analysis.get("mdg_mean", 0))
                            drawdown_worst = analysis.get("drawdown_worst_max", analysis.get("drawdown_worst_mean", 0))
                            gain = analysis.get("gain_max", analysis.get("gain_mean", 0))
                            loss_profit_ratio = analysis.get("loss_profit_ratio_max", analysis.get("loss_profit_ratio_mean", 0))
                            position_held_hours_max = analysis.get("position_held_hours_max_max", analysis.get("position_held_hours_max_mean", 0))
                            sharpe_ratio = analysis.get("sharpe_ratio_max", analysis.get("sharpe_ratio_mean", 0))
                            d.append({
                                'Select': False,
                                'id': id,
                                'view': False,
                                'adg': adg,
                                'mdg': mdg,
                                'drawdown_worst': drawdown_worst,
                                'gain': gain,
                                'loss_profit_ratio': loss_profit_ratio,
                                'position_held_hours_max': position_held_hours_max,
                                'sharpe_ratio': sharpe_ratio,
                                'Name': name,
                                'file': pareto["index_filename"],
                            })
                        else:
                            # Check if the selected analysis exists in this pareto's analyses
                            if st.session_state.opt_v7_pareto_select_analysis in pareto.get("analyses", {}):
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
        st.data_editor(data=d_paretos, height="auto", key=f'select_paretos_{st.session_state.ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','file'])
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


    def remove_selected_results(self):
        ed_key = st.session_state.ed_key
        if not self.results_d:
            return
        ed = st.session_state[f'select_optresults_new_{ed_key}']
        for row in ed["edited_rows"]:
            if "delete" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["delete"]:
                    directory = Path(self.results_d[row]["index"]).parent
                    shutil.rmtree(directory, ignore_errors=True)
        self.find_results()

    def remove_all_results(self):
        shutil.rmtree(self.results_path, ignore_errors=True)
        self.results_d = []
        self.results_new = []

class OptimizeV7Item(ConfigV7Editor):
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
            # Correct base_dir based on name
            self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
            if Path(optimize_file).exists():
                self.time = datetime.datetime.fromtimestamp(Path(optimize_file).stat().st_mtime)
            else:
                self.time = datetime.datetime.now()
        else:
            self.initialize()
        self._calculate_results()
        # Clean up limits session state when loading new config
        for key in list(st.session_state.keys()):
            if key.startswith("limits_") or key.startswith("edit_limit") or key.startswith("add_limit") or key.startswith("select_limits_"):
                del st.session_state[key]

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
    
    # ============ ABSTRACT METHOD IMPLEMENTATIONS ============
    
    def _get_key_prefix(self):
        """Return key prefix for streamlit widgets."""
        return "opt_"
    
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
        # Correct base_dir based on name
        self.config.backtest.base_dir = f'backtests/pbgui/{self.name}'
        
        if "edit_opt_v7_name" in st.session_state:
            st.session_state.edit_opt_v7_name = self.name
        # Clean up limits session state when loading preset
        for key in list(st.session_state.keys()):
            if key.startswith("limits_") or key.startswith("edit_limit") or key.startswith("add_limit") or key.startswith("select_limits_"):
                del st.session_state[key]
        
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
        st.multiselect('Exchanges',["binance", "bybit", "gateio", "bitget"], key="edit_opt_v7_exchanges", help=pbgui_help.exchanges)
    
    # Coin Sources
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
            self.config.backtest.exchanges if self.config.backtest.exchanges else V7.list(),
            save_callback=lambda cs: setattr(self.config.backtest, 'coin_sources', cs),
            current_exchanges=self.config.backtest.exchanges if self.config.backtest.exchanges else V7.list(),
            all_suite_coin_sources=all_suite_sources
        )

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
        st.date_input("start_date", format="YYYY-MM-DD", key="edit_opt_v7_start_date", help=pbgui_help.backtest_start_date)

    # end_date
    @st.fragment
    def fragment_end_date(self):
        if "edit_opt_v7_end_date" in st.session_state:
            if st.session_state.edit_opt_v7_end_date.strftime("%Y-%m-%d") != self.config.backtest.end_date:
                self.config.backtest.end_date = st.session_state.edit_opt_v7_end_date.strftime("%Y-%m-%d")
        else:
            st.session_state.edit_opt_v7_end_date = datetime.datetime.strptime(self.config.backtest.end_date, '%Y-%m-%d')
        st.date_input("end_date", format="YYYY-MM-DD", key="edit_opt_v7_end_date", help=pbgui_help.backtest_end_date)

    # logging
    @st.fragment
    def fragment_logging(self):
        if "edit_opt_v7_logging_level" in st.session_state:
            if st.session_state.edit_opt_v7_logging_level != self.config.logging.level:
                self.config.logging.level = st.session_state.edit_opt_v7_logging_level
        else:
            st.session_state.edit_opt_v7_logging_level = self.config.logging.level
        st.selectbox("logging level", Logging.LEVEL, format_func=lambda x: Logging.LEVEL.get(x), key="edit_opt_v7_logging_level", help=pbgui_help.logging_level)

    # starting_balance
    @st.fragment
    def fragment_starting_balance(self):
        if "edit_opt_v7_starting_balance" in st.session_state:
            if st.session_state.edit_opt_v7_starting_balance != self.config.backtest.starting_balance:
                self.config.backtest.starting_balance = st.session_state.edit_opt_v7_starting_balance
        else:
            st.session_state.edit_opt_v7_starting_balance = float(self.config.backtest.starting_balance)
        st.number_input("starting_balance", step=500.0, key="edit_opt_v7_starting_balance", help=pbgui_help.starting_balance)

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
        st.number_input("n_cpus", min_value=1, max_value=multiprocessing.cpu_count(), step=1, key="edit_opt_v7_n_cpus", help=pbgui_help.n_cpus)

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
    
    # btc_collateral_cap
    @st.fragment
    def fragment_btc_collateral_cap(self):
        if "edit_opt_v7_btc_collateral_cap" in st.session_state:
            if st.session_state.edit_opt_v7_btc_collateral_cap != self.config.backtest.btc_collateral_cap:
                self.config.backtest.btc_collateral_cap = st.session_state.edit_opt_v7_btc_collateral_cap
        else:
            st.session_state.edit_opt_v7_btc_collateral_cap = self.config.backtest.btc_collateral_cap
        st.number_input("btc_collateral_cap", min_value=0.0, max_value=10.0, step=0.1, format="%.2f", key="edit_opt_v7_btc_collateral_cap", help=pbgui_help.btc_collateral_cap)

    # btc_collateral_ltv_cap
    @st.fragment
    def fragment_btc_collateral_ltv_cap(self):
        if "edit_opt_v7_btc_collateral_ltv_cap" in st.session_state:
            new_val = st.session_state.edit_opt_v7_btc_collateral_ltv_cap
            # Convert 0 to None for the config
            config_val = None if new_val == 0.0 else new_val
            if config_val != self.config.backtest.btc_collateral_ltv_cap:
                self.config.backtest.btc_collateral_ltv_cap = config_val
        else:
            # Convert None to 0 for the UI
            st.session_state.edit_opt_v7_btc_collateral_ltv_cap = self.config.backtest.btc_collateral_ltv_cap if self.config.backtest.btc_collateral_ltv_cap is not None else 0.0
        st.number_input("btc_collateral_ltv_cap", min_value=0.0, max_value=1.0, step=0.1, format="%.2f", key="edit_opt_v7_btc_collateral_ltv_cap", help=pbgui_help.btc_collateral_ltv_cap)

    # write_all_results
    @st.fragment
    def fragment_write_all_results(self):
        if "edit_opt_v7_write_all_results" in st.session_state:
            if st.session_state.edit_opt_v7_write_all_results != self.config.optimize.write_all_results:
                self.config.optimize.write_all_results = st.session_state.edit_opt_v7_write_all_results
        else:
            st.session_state.edit_opt_v7_write_all_results = self.config.optimize.write_all_results
        st.checkbox("write_all_results", key="edit_opt_v7_write_all_results", help=pbgui_help.write_all_results)

    # population_size
    @st.fragment
    def fragment_population_size(self):
        if "edit_opt_v7_population_size" in st.session_state:
            if st.session_state.edit_opt_v7_population_size != self.config.optimize.population_size:
                self.config.optimize.population_size = st.session_state.edit_opt_v7_population_size
        else:
            st.session_state.edit_opt_v7_population_size = self.config.optimize.population_size
        st.number_input("population_size", min_value=1, max_value=10000, step=1, format="%d", key="edit_opt_v7_population_size", help=pbgui_help.population_size)

    # pareto_max_size
    @st.fragment
    def fragment_pareto_max_size(self):
        if "edit_opt_v7_pareto_max_size" in st.session_state:
            if st.session_state.edit_opt_v7_pareto_max_size != self.config.optimize.pareto_max_size:
                self.config.optimize.pareto_max_size = st.session_state.edit_opt_v7_pareto_max_size
        else:
            st.session_state.edit_opt_v7_pareto_max_size = self.config.optimize.pareto_max_size
        st.number_input("pareto_max_size", min_value=1, max_value=10000, step=10, format="%d", key="edit_opt_v7_pareto_max_size", help=pbgui_help.pareto_max_size)

    # offspring_multiplier
    @st.fragment
    def fragment_offspring_multiplier(self):
        if "edit_opt_v7_offspring_multiplier" in st.session_state:
            if st.session_state.edit_opt_v7_offspring_multiplier != self.config.optimize.offspring_multiplier:
                self.config.optimize.offspring_multiplier = st.session_state.edit_opt_v7_offspring_multiplier
        else:
            st.session_state.edit_opt_v7_offspring_multiplier = self.config.optimize.offspring_multiplier
        st.number_input("offspring_multiplier", min_value=0.0, max_value=10000.0, step=0.1, format="%.2f", key="edit_opt_v7_offspring_multiplier", help=pbgui_help.offspring_multiplier)

    # crossover_probability
    @st.fragment
    def fragment_crossover_probability(self):
        if "edit_opt_v7_crossover_probability" in st.session_state:
            if st.session_state.edit_opt_v7_crossover_probability != self.config.optimize.crossover_probability:
                self.config.optimize.crossover_probability = st.session_state.edit_opt_v7_crossover_probability
        else:
            st.session_state.edit_opt_v7_crossover_probability = self.config.optimize.crossover_probability
        st.number_input("crossover_probability", min_value=0.0, max_value=1.0, step=0.01, format="%.2f", key="edit_opt_v7_crossover_probability", help=pbgui_help.crossover_probability)
    
    # crossover_eta
    @st.fragment
    def fragment_crossover_eta(self):
        if "edit_opt_v7_crossover_eta" in st.session_state:
            if st.session_state.edit_opt_v7_crossover_eta != self.config.optimize.crossover_eta:
                self.config.optimize.crossover_eta = st.session_state.edit_opt_v7_crossover_eta
        else:
            st.session_state.edit_opt_v7_crossover_eta = self.config.optimize.crossover_eta
        st.number_input("crossover_eta", min_value=0.0, max_value=10000.0, step=1.0, format="%.2f", key="edit_opt_v7_crossover_eta", help=pbgui_help.crossover_eta)
    
    # mutation_probability
    @st.fragment
    def fragment_mutation_probability(self):
        if "edit_opt_v7_mutation_probability" in st.session_state:
            if st.session_state.edit_opt_v7_mutation_probability != self.config.optimize.mutation_probability:
                self.config.optimize.mutation_probability = st.session_state.edit_opt_v7_mutation_probability
        else:
            st.session_state.edit_opt_v7_mutation_probability = self.config.optimize.mutation_probability
        st.number_input("mutation_probability", min_value=0.0, max_value=1.0, step=0.01, format="%.2f", key="edit_opt_v7_mutation_probability", help=pbgui_help.mutation_probability)

    # mutation_eta
    @st.fragment
    def fragment_mutation_eta(self):
        if "edit_opt_v7_mutation_eta" in st.session_state:
            if st.session_state.edit_opt_v7_mutation_eta != self.config.optimize.mutation_eta:
                self.config.optimize.mutation_eta = st.session_state.edit_opt_v7_mutation_eta
        else:
            st.session_state.edit_opt_v7_mutation_eta = self.config.optimize.mutation_eta
        st.number_input("mutation_eta", min_value=0.0, max_value=10000.0, step=1.0, format="%.2f", key="edit_opt_v7_mutation_eta", help=pbgui_help.mutation_eta)

    # mutation_indpb
    @st.fragment
    def fragment_mutation_indpb(self):
        if "edit_opt_v7_mutation_indpb" in st.session_state:
            if st.session_state.edit_opt_v7_mutation_indpb != self.config.optimize.mutation_indpb:
                self.config.optimize.mutation_indpb = st.session_state.edit_opt_v7_mutation_indpb
        else:
            st.session_state.edit_opt_v7_mutation_indpb = self.config.optimize.mutation_indpb
        st.number_input("mutation_indpb", min_value=0.0, max_value=1.0, step=0.01, format="%.2f", key="edit_opt_v7_mutation_indpb", help=pbgui_help.mutation_indpb)
    
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
            "adg_per_exposure_long",
            "adg_per_exposure_short",
            "adg_w",
            "adg_w_per_exposure_long",
            "adg_w_per_exposure_short",
            "btc_adg",
            "btc_adg_per_exposure_long",
            "btc_adg_per_exposure_short",
            "btc_adg_w",
            "btc_adg_w_per_exposure_long",
            "btc_adg_w_per_exposure_short",
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
            "btc_gain_per_exposure_long",
            "btc_gain_per_exposure_short",
            "btc_loss_profit_ratio",
            "btc_loss_profit_ratio_w",
            "btc_mdg",
            "btc_mdg_per_exposure_long",
            "btc_mdg_per_exposure_short",
            "btc_mdg_w",
            "btc_mdg_w_per_exposure_long",
            "btc_mdg_w_per_exposure_short",
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
            "flat_btc_balance_hours",
            "gain",
            "gain_per_exposure_long",
            "gain_per_exposure_short",
            "loss_profit_ratio",
            "loss_profit_ratio_w",
            "mdg",
            "mdg_per_exposure_long",
            "mdg_per_exposure_short",
            "mdg_w",
            "mdg_w_per_exposure_long",
            "mdg_w_per_exposure_short",
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
            "volume_pct_per_day_avg",
            "volume_pct_per_day_avg_w",
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
            st.multiselect('approved_coins_long', symbols, key="edit_opt_v7_approved_coins_long", help=pbgui_help.approved_coins_long)
        with col2:
            st.multiselect('approved_coins_short', symbols, key="edit_opt_v7_approved_coins_short", help=pbgui_help.approved_coins_short)

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
    
    # long_entry_volatility_ema_span_hours
    @st.fragment
    def fragment_long_entry_volatility_ema_span_hours(self):
        if "edit_opt_v7_long_entry_volatility_ema_span_hours" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_volatility_ema_span_hours != (self.config.optimize.bounds.long_entry_volatility_ema_span_hours_0, self.config.optimize.bounds.long_entry_volatility_ema_span_hours_1):
                self.config.optimize.bounds.long_entry_volatility_ema_span_hours_0 = st.session_state.edit_opt_v7_long_entry_volatility_ema_span_hours[0]
                self.config.optimize.bounds.long_entry_volatility_ema_span_hours_1 = st.session_state.edit_opt_v7_long_entry_volatility_ema_span_hours[1]
        else:
            st.session_state.edit_opt_v7_long_entry_volatility_ema_span_hours = (self.config.optimize.bounds.long_entry_volatility_ema_span_hours_0, self.config.optimize.bounds.long_entry_volatility_ema_span_hours_1)
        st.slider(
            "long_entry_volatility_ema_span_hours",
            min_value=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_MIN,
            max_value=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_MAX,
            step=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_STEP,
            format=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_FORMAT,
            key="edit_opt_v7_long_entry_volatility_ema_span_hours",
            help=pbgui_help.entry_volatility_ema_span_hours)

    # long_entry_grid_spacing_volatility_weight
    @st.fragment
    def fragment_long_entry_grid_spacing_volatility_weight(self):
        if "edit_opt_v7_long_entry_grid_spacing_volatility_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_grid_spacing_volatility_weight != (self.config.optimize.bounds.long_entry_grid_spacing_volatility_weight_0, self.config.optimize.bounds.long_entry_grid_spacing_volatility_weight_1):
                self.config.optimize.bounds.long_entry_grid_spacing_volatility_weight_0 = st.session_state.edit_opt_v7_long_entry_grid_spacing_volatility_weight[0]
                self.config.optimize.bounds.long_entry_grid_spacing_volatility_weight_1 = st.session_state.edit_opt_v7_long_entry_grid_spacing_volatility_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_grid_spacing_volatility_weight = (self.config.optimize.bounds.long_entry_grid_spacing_volatility_weight_0, self.config.optimize.bounds.long_entry_grid_spacing_volatility_weight_1)
        st.slider(
            "long_entry_grid_spacing_volatility_weight",
            min_value=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_grid_spacing_volatility_weight",
            help=pbgui_help.entry_grid_spacing_volatility_weight)

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
    
    # long_entry_grid_spacing_we_weight
    @st.fragment
    def fragment_long_entry_grid_spacing_we_weight(self):
        if "edit_opt_v7_long_entry_grid_spacing_we_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_grid_spacing_we_weight != (self.config.optimize.bounds.long_entry_grid_spacing_we_weight_0, self.config.optimize.bounds.long_entry_grid_spacing_we_weight_1):
                self.config.optimize.bounds.long_entry_grid_spacing_we_weight_0 = st.session_state.edit_opt_v7_long_entry_grid_spacing_we_weight[0]
                self.config.optimize.bounds.long_entry_grid_spacing_we_weight_1 = st.session_state.edit_opt_v7_long_entry_grid_spacing_we_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_grid_spacing_we_weight = (self.config.optimize.bounds.long_entry_grid_spacing_we_weight_0, self.config.optimize.bounds.long_entry_grid_spacing_we_weight_1)
        st.slider(
            "long_entry_grid_spacing_we_weight",
            min_value=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_grid_spacing_we_weight",
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

    # long_entry_trailing_retracement_we_weight
    @st.fragment
    def fragment_long_entry_trailing_retracement_we_weight(self):
        if "edit_opt_v7_long_entry_trailing_retracement_we_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_retracement_we_weight != (self.config.optimize.bounds.long_entry_trailing_retracement_we_weight_0, self.config.optimize.bounds.long_entry_trailing_retracement_we_weight_1):
                self.config.optimize.bounds.long_entry_trailing_retracement_we_weight_0 = st.session_state.edit_opt_v7_long_entry_trailing_retracement_we_weight[0]
                self.config.optimize.bounds.long_entry_trailing_retracement_we_weight_1 = st.session_state.edit_opt_v7_long_entry_trailing_retracement_we_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_retracement_we_weight = (self.config.optimize.bounds.long_entry_trailing_retracement_we_weight_0, self.config.optimize.bounds.long_entry_trailing_retracement_we_weight_1)
        st.slider(
            "long_entry_trailing_retracement_we_weight",
            min_value=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_trailing_retracement_we_weight",
            help=pbgui_help.entry_trailing_retracement_we_weight)

    # long_entry_trailing_retracement_volatility_weight
    @st.fragment
    def fragment_long_entry_trailing_retracement_volatility_weight(self):
        if "edit_opt_v7_long_entry_trailing_retracement_volatility_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_retracement_volatility_weight != (self.config.optimize.bounds.long_entry_trailing_retracement_volatility_weight_0, self.config.optimize.bounds.long_entry_trailing_retracement_volatility_weight_1):
                self.config.optimize.bounds.long_entry_trailing_retracement_volatility_weight_0 = st.session_state.edit_opt_v7_long_entry_trailing_retracement_volatility_weight[0]
                self.config.optimize.bounds.long_entry_trailing_retracement_volatility_weight_1 = st.session_state.edit_opt_v7_long_entry_trailing_retracement_volatility_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_retracement_volatility_weight = (self.config.optimize.bounds.long_entry_trailing_retracement_volatility_weight_0, self.config.optimize.bounds.long_entry_trailing_retracement_volatility_weight_1)
        st.slider(
            "long_entry_trailing_retracement_volatility_weight",
            min_value=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_trailing_retracement_volatility_weight",
            help=pbgui_help.entry_trailing_retracement_volatility_weight)

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

    # long_entry_trailing_threshold_we_weight
    @st.fragment
    def fragment_long_entry_trailing_threshold_we_weight(self):
        if "edit_opt_v7_long_entry_trailing_threshold_we_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_threshold_we_weight != (self.config.optimize.bounds.long_entry_trailing_threshold_we_weight_0, self.config.optimize.bounds.long_entry_trailing_threshold_we_weight_1):
                self.config.optimize.bounds.long_entry_trailing_threshold_we_weight_0 = st.session_state.edit_opt_v7_long_entry_trailing_threshold_we_weight[0]
                self.config.optimize.bounds.long_entry_trailing_threshold_we_weight_1 = st.session_state.edit_opt_v7_long_entry_trailing_threshold_we_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_threshold_we_weight = (self.config.optimize.bounds.long_entry_trailing_threshold_we_weight_0, self.config.optimize.bounds.long_entry_trailing_threshold_we_weight_1)
        st.slider(
            "long_entry_trailing_threshold_we_weight",
            min_value=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_trailing_threshold_we_weight",
            help=pbgui_help.entry_trailing_threshold_we_weight)

    # long_entry_trailing_threshold_volatility_weight
    @st.fragment
    def fragment_long_entry_trailing_threshold_volatility_weight(self):
        if "edit_opt_v7_long_entry_trailing_threshold_volatility_weight" in st.session_state:
            if st.session_state.edit_opt_v7_long_entry_trailing_threshold_volatility_weight != (self.config.optimize.bounds.long_entry_trailing_threshold_volatility_weight_0, self.config.optimize.bounds.long_entry_trailing_threshold_volatility_weight_1):
                self.config.optimize.bounds.long_entry_trailing_threshold_volatility_weight_0 = st.session_state.edit_opt_v7_long_entry_trailing_threshold_volatility_weight[0]
                self.config.optimize.bounds.long_entry_trailing_threshold_volatility_weight_1 = st.session_state.edit_opt_v7_long_entry_trailing_threshold_volatility_weight[1]
        else:
            st.session_state.edit_opt_v7_long_entry_trailing_threshold_volatility_weight = (self.config.optimize.bounds.long_entry_trailing_threshold_volatility_weight_0, self.config.optimize.bounds.long_entry_trailing_threshold_volatility_weight_1)
        st.slider(
            "long_entry_trailing_threshold_volatility_weight",
            min_value=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_FORMAT,
            key="edit_opt_v7_long_entry_trailing_threshold_volatility_weight",
            help=pbgui_help.entry_trailing_threshold_volatility_weight)

    # long_filter_volatility_ema_span
    @st.fragment
    def fragment_long_filter_volatility_ema_span(self):
        if "edit_opt_v7_long_filter_volatility_ema_span" in st.session_state:
            if st.session_state.edit_opt_v7_long_filter_volatility_ema_span != (self.config.optimize.bounds.long_filter_volatility_ema_span_0, self.config.optimize.bounds.long_filter_volatility_ema_span_1):
                self.config.optimize.bounds.long_filter_volatility_ema_span_0 = st.session_state.edit_opt_v7_long_filter_volatility_ema_span[0]
                self.config.optimize.bounds.long_filter_volatility_ema_span_1 = st.session_state.edit_opt_v7_long_filter_volatility_ema_span[1]
        else:
            st.session_state.edit_opt_v7_long_filter_volatility_ema_span = (self.config.optimize.bounds.long_filter_volatility_ema_span_0, self.config.optimize.bounds.long_filter_volatility_ema_span_1)
        st.slider(
            "long_filter_volatility_ema_span",
            min_value=Bounds.FILTER_VOLATILITY_EMA_SPAN_MIN,
            max_value=Bounds.FILTER_VOLATILITY_EMA_SPAN_MAX,
            step=Bounds.FILTER_VOLATILITY_EMA_SPAN_STEP,
            format=Bounds.FILTER_VOLATILITY_EMA_SPAN_FORMAT,
            key="edit_opt_v7_long_filter_volatility_ema_span",
            help=pbgui_help.filter_ema_span)

    # long_filter_volatility_drop_pct
    @st.fragment
    def fragment_long_filter_volatility_drop_pct(self):
        if "edit_opt_v7_long_filter_volatility_drop_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_filter_volatility_drop_pct != (self.config.optimize.bounds.long_filter_volatility_drop_pct_0, self.config.optimize.bounds.long_filter_volatility_drop_pct_1):
                self.config.optimize.bounds.long_filter_volatility_drop_pct_0 = st.session_state.edit_opt_v7_long_filter_volatility_drop_pct[0]
                self.config.optimize.bounds.long_filter_volatility_drop_pct_1 = st.session_state.edit_opt_v7_long_filter_volatility_drop_pct[1]
        else:
            st.session_state.edit_opt_v7_long_filter_volatility_drop_pct = (self.config.optimize.bounds.long_filter_volatility_drop_pct_0, self.config.optimize.bounds.long_filter_volatility_drop_pct_1)
        st.slider(
            "long_filter_volatility_drop_pct",
            min_value=Bounds.FILTER_VOLATILITY_DROP_PCT_MIN,
            max_value=Bounds.FILTER_VOLATILITY_DROP_PCT_MAX,
            step=Bounds.FILTER_VOLATILITY_DROP_PCT_STEP,
            format=Bounds.FILTER_VOLATILITY_DROP_PCT_FORMAT,
            key="edit_opt_v7_long_filter_volatility_drop_pct",
            help=pbgui_help.filter_volatility_drop_pct)

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
    
    # long_filter_volume_ema_span
    @st.fragment
    def fragment_long_filter_volume_ema_span(self):
        if "edit_opt_v7_long_filter_volume_ema_span" in st.session_state:
            if st.session_state.edit_opt_v7_long_filter_volume_ema_span != (self.config.optimize.bounds.long_filter_volume_ema_span_0, self.config.optimize.bounds.long_filter_volume_ema_span_1):
                self.config.optimize.bounds.long_filter_volume_ema_span_0 = st.session_state.edit_opt_v7_long_filter_volume_ema_span[0]
                self.config.optimize.bounds.long_filter_volume_ema_span_1 = st.session_state.edit_opt_v7_long_filter_volume_ema_span[1]
        else:
            st.session_state.edit_opt_v7_long_filter_volume_ema_span = (self.config.optimize.bounds.long_filter_volume_ema_span_0, self.config.optimize.bounds.long_filter_volume_ema_span_1)
        st.slider(
            "long_filter_volume_ema_span",
            min_value=Bounds.FILTER_VOLUME_EMA_SPAN_MIN,
            max_value=Bounds.FILTER_VOLUME_EMA_SPAN_MAX,
            step=Bounds.FILTER_VOLUME_EMA_SPAN_STEP,
            format=Bounds.FILTER_VOLUME_EMA_SPAN_FORMAT,
            key="edit_opt_v7_long_filter_volume_ema_span",
            help=pbgui_help.filter_ema_span)

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

    # long_risk_wel_enforcer_threshold
    @st.fragment
    def fragment_long_risk_wel_enforcer_threshold(self):
        if "edit_opt_v7_long_risk_wel_enforcer_threshold" in st.session_state:
            if st.session_state.edit_opt_v7_long_risk_wel_enforcer_threshold != (self.config.optimize.bounds.long_risk_wel_enforcer_threshold_0, self.config.optimize.bounds.long_risk_wel_enforcer_threshold_1):
                self.config.optimize.bounds.long_risk_wel_enforcer_threshold_0 = st.session_state.edit_opt_v7_long_risk_wel_enforcer_threshold[0]
                self.config.optimize.bounds.long_risk_wel_enforcer_threshold_1 = st.session_state.edit_opt_v7_long_risk_wel_enforcer_threshold[1]
        else:
            st.session_state.edit_opt_v7_long_risk_wel_enforcer_threshold = (self.config.optimize.bounds.long_risk_wel_enforcer_threshold_0, self.config.optimize.bounds.long_risk_wel_enforcer_threshold_1)
        st.slider(
            "long_risk_wel_enforcer_threshold",
            min_value=Bounds.RISK_WEL_ENFORCER_THRESHOLD_MIN,
            max_value=Bounds.RISK_WEL_ENFORCER_THRESHOLD_MAX,
            step=Bounds.RISK_WEL_ENFORCER_THRESHOLD_STEP,
            format=Bounds.RISK_WEL_ENFORCER_THRESHOLD_FORMAT,
            key="edit_opt_v7_long_risk_wel_enforcer_threshold",
            help=pbgui_help.risk_wel_enforcer_threshold)

    # long_risk_we_excess_allowance_pct
    @st.fragment
    def fragment_long_risk_we_excess_allowance_pct(self):
        if "edit_opt_v7_long_risk_we_excess_allowance_pct" in st.session_state:
            if st.session_state.edit_opt_v7_long_risk_we_excess_allowance_pct != (self.config.optimize.bounds.long_risk_we_excess_allowance_pct_0, self.config.optimize.bounds.long_risk_we_excess_allowance_pct_1):
                self.config.optimize.bounds.long_risk_we_excess_allowance_pct_0 = st.session_state.edit_opt_v7_long_risk_we_excess_allowance_pct[0]
                self.config.optimize.bounds.long_risk_we_excess_allowance_pct_1 = st.session_state.edit_opt_v7_long_risk_we_excess_allowance_pct[1]
        else:
            st.session_state.edit_opt_v7_long_risk_we_excess_allowance_pct = (self.config.optimize.bounds.long_risk_we_excess_allowance_pct_0, self.config.optimize.bounds.long_risk_we_excess_allowance_pct_1)
        st.slider(
            "long_risk_we_excess_allowance_pct",
            min_value=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_MIN,
            max_value=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_MAX,
            step=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_STEP,
            format=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_FORMAT,
            key="edit_opt_v7_long_risk_we_excess_allowance_pct",
            help=pbgui_help.risk_we_excess_allowance_pct)

    # long_risk_twel_enforcer_threshold
    @st.fragment
    def fragment_long_risk_twel_enforcer_threshold(self):
        if "edit_opt_v7_long_risk_twel_enforcer_threshold" in st.session_state:
            if st.session_state.edit_opt_v7_long_risk_twel_enforcer_threshold != (self.config.optimize.bounds.long_risk_twel_enforcer_threshold_0, self.config.optimize.bounds.long_risk_twel_enforcer_threshold_1):
                self.config.optimize.bounds.long_risk_twel_enforcer_threshold_0 = st.session_state.edit_opt_v7_long_risk_twel_enforcer_threshold[0]
                self.config.optimize.bounds.long_risk_twel_enforcer_threshold_1 = st.session_state.edit_opt_v7_long_risk_twel_enforcer_threshold[1]
        else:
            st.session_state.edit_opt_v7_long_risk_twel_enforcer_threshold = (self.config.optimize.bounds.long_risk_twel_enforcer_threshold_0, self.config.optimize.bounds.long_risk_twel_enforcer_threshold_1)
        st.slider(
            "long_risk_twel_enforcer_threshold",
            min_value=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_MIN,
            max_value=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_MAX,
            step=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_STEP,
            format=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_FORMAT,
            key="edit_opt_v7_long_risk_twel_enforcer_threshold",
            help=pbgui_help.risk_twel_enforcer_threshold)

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

    # short_entry_volatility_ema_span_hours
    @st.fragment
    def fragment_short_entry_volatility_ema_span_hours(self):
        if "edit_opt_v7_short_entry_volatility_ema_span_hours" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_volatility_ema_span_hours != (self.config.optimize.bounds.short_entry_volatility_ema_span_hours_0, self.config.optimize.bounds.short_entry_volatility_ema_span_hours_1):
                self.config.optimize.bounds.short_entry_volatility_ema_span_hours_0 = st.session_state.edit_opt_v7_short_entry_volatility_ema_span_hours[0]
                self.config.optimize.bounds.short_entry_volatility_ema_span_hours_1 = st.session_state.edit_opt_v7_short_entry_volatility_ema_span_hours[1]
        else:
            st.session_state.edit_opt_v7_short_entry_volatility_ema_span_hours = (self.config.optimize.bounds.short_entry_volatility_ema_span_hours_0, self.config.optimize.bounds.short_entry_volatility_ema_span_hours_1)
        st.slider(
            "short_entry_volatility_ema_span_hours",
            min_value=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_MIN,
            max_value=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_MAX,
            step=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_STEP,
            format=Bounds.ENTRY_VOLATILITY_EMA_SPAN_HOURS_FORMAT,
            key="edit_opt_v7_short_entry_volatility_ema_span_hours",
            help=pbgui_help.entry_volatility_ema_span_hours)

    # short_entry_grid_spacing_volatility_weight
    @st.fragment
    def fragment_short_entry_grid_spacing_volatility_weight(self):
        if "edit_opt_v7_short_entry_grid_spacing_volatility_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_grid_spacing_volatility_weight != (self.config.optimize.bounds.short_entry_grid_spacing_volatility_weight_0, self.config.optimize.bounds.short_entry_grid_spacing_volatility_weight_1):
                self.config.optimize.bounds.short_entry_grid_spacing_volatility_weight_0 = st.session_state.edit_opt_v7_short_entry_grid_spacing_volatility_weight[0]
                self.config.optimize.bounds.short_entry_grid_spacing_volatility_weight_1 = st.session_state.edit_opt_v7_short_entry_grid_spacing_volatility_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_grid_spacing_volatility_weight = (self.config.optimize.bounds.short_entry_grid_spacing_volatility_weight_0, self.config.optimize.bounds.short_entry_grid_spacing_volatility_weight_1)
        st.slider(
            "short_entry_grid_spacing_volatility_weight",
            min_value=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_VOLATILITY_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_grid_spacing_volatility_weight",
            help=pbgui_help.entry_grid_spacing_volatility_weight)

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
    
    # short_entry_grid_spacing_we_weight
    @st.fragment
    def fragment_short_entry_grid_spacing_we_weight(self):
        if "edit_opt_v7_short_entry_grid_spacing_we_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_grid_spacing_we_weight != (self.config.optimize.bounds.short_entry_grid_spacing_we_weight_0, self.config.optimize.bounds.short_entry_grid_spacing_we_weight_1):
                self.config.optimize.bounds.short_entry_grid_spacing_we_weight_0 = st.session_state.edit_opt_v7_short_entry_grid_spacing_we_weight[0]
                self.config.optimize.bounds.short_entry_grid_spacing_we_weight_1 = st.session_state.edit_opt_v7_short_entry_grid_spacing_we_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_grid_spacing_we_weight = (self.config.optimize.bounds.short_entry_grid_spacing_we_weight_0, self.config.optimize.bounds.short_entry_grid_spacing_we_weight_1)
        st.slider(
            "short_entry_grid_spacing_we_weight",
            min_value=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_MIN,
            max_value=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_MAX,
            step=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_STEP,
            format=Bounds.ENTRY_GRID_SPACING_WE_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_grid_spacing_we_weight",
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

    # short_entry_trailing_retracement_we_weight
    @st.fragment
    def fragment_short_entry_trailing_retracement_we_weight(self):
        if "edit_opt_v7_short_entry_trailing_retracement_we_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_retracement_we_weight != (self.config.optimize.bounds.short_entry_trailing_retracement_we_weight_0, self.config.optimize.bounds.short_entry_trailing_retracement_we_weight_1):
                self.config.optimize.bounds.short_entry_trailing_retracement_we_weight_0 = st.session_state.edit_opt_v7_short_entry_trailing_retracement_we_weight[0]
                self.config.optimize.bounds.short_entry_trailing_retracement_we_weight_1 = st.session_state.edit_opt_v7_short_entry_trailing_retracement_we_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_retracement_we_weight = (self.config.optimize.bounds.short_entry_trailing_retracement_we_weight_0, self.config.optimize.bounds.short_entry_trailing_retracement_we_weight_1)
        st.slider(
            "short_entry_trailing_retracement_we_weight",
            min_value=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_RETRACEMENT_WE_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_trailing_retracement_we_weight",
            help=pbgui_help.entry_trailing_retracement_we_weight)

    # short_entry_trailing_retracement_volatility_weight
    @st.fragment
    def fragment_short_entry_trailing_retracement_volatility_weight(self):
        if "edit_opt_v7_short_entry_trailing_retracement_volatility_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_retracement_volatility_weight != (self.config.optimize.bounds.short_entry_trailing_retracement_volatility_weight_0, self.config.optimize.bounds.short_entry_trailing_retracement_volatility_weight_1):
                self.config.optimize.bounds.short_entry_trailing_retracement_volatility_weight_0 = st.session_state.edit_opt_v7_short_entry_trailing_retracement_volatility_weight[0]
                self.config.optimize.bounds.short_entry_trailing_retracement_volatility_weight_1 = st.session_state.edit_opt_v7_short_entry_trailing_retracement_volatility_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_retracement_volatility_weight = (self.config.optimize.bounds.short_entry_trailing_retracement_volatility_weight_0, self.config.optimize.bounds.short_entry_trailing_retracement_volatility_weight_1)
        st.slider(
            "short_entry_trailing_retracement_volatility_weight",
            min_value=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_RETRACEMENT_VOLATILITY_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_trailing_retracement_volatility_weight",
            help=pbgui_help.entry_trailing_retracement_volatility_weight)

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

    # short_entry_trailing_threshold_we_weight
    @st.fragment
    def fragment_short_entry_trailing_threshold_we_weight(self):
        if "edit_opt_v7_short_entry_trailing_threshold_we_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_threshold_we_weight != (self.config.optimize.bounds.short_entry_trailing_threshold_we_weight_0, self.config.optimize.bounds.short_entry_trailing_threshold_we_weight_1):
                self.config.optimize.bounds.short_entry_trailing_threshold_we_weight_0 = st.session_state.edit_opt_v7_short_entry_trailing_threshold_we_weight[0]
                self.config.optimize.bounds.short_entry_trailing_threshold_we_weight_1 = st.session_state.edit_opt_v7_short_entry_trailing_threshold_we_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_threshold_we_weight = (self.config.optimize.bounds.short_entry_trailing_threshold_we_weight_0, self.config.optimize.bounds.short_entry_trailing_threshold_we_weight_1)
        st.slider(
            "short_entry_trailing_threshold_we_weight",
            min_value=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_THRESHOLD_WE_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_trailing_threshold_we_weight",
            help=pbgui_help.entry_trailing_threshold_we_weight)

    # short_entry_trailing_threshold_volatility_weight
    @st.fragment
    def fragment_short_entry_trailing_threshold_volatility_weight(self):
        if "edit_opt_v7_short_entry_trailing_threshold_volatility_weight" in st.session_state:
            if st.session_state.edit_opt_v7_short_entry_trailing_threshold_volatility_weight != (self.config.optimize.bounds.short_entry_trailing_threshold_volatility_weight_0, self.config.optimize.bounds.short_entry_trailing_threshold_volatility_weight_1):
                self.config.optimize.bounds.short_entry_trailing_threshold_volatility_weight_0 = st.session_state.edit_opt_v7_short_entry_trailing_threshold_volatility_weight[0]
                self.config.optimize.bounds.short_entry_trailing_threshold_volatility_weight_1 = st.session_state.edit_opt_v7_short_entry_trailing_threshold_volatility_weight[1]
        else:
            st.session_state.edit_opt_v7_short_entry_trailing_threshold_volatility_weight = (self.config.optimize.bounds.short_entry_trailing_threshold_volatility_weight_0, self.config.optimize.bounds.short_entry_trailing_threshold_volatility_weight_1)
        st.slider(
            "short_entry_trailing_threshold_volatility_weight",
            min_value=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_MIN,
            max_value=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_MAX,
            step=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_STEP,
            format=Bounds.ENTRY_TRAILING_THRESHOLD_VOLATILITY_WEIGHT_FORMAT,
            key="edit_opt_v7_short_entry_trailing_threshold_volatility_weight",
            help=pbgui_help.entry_trailing_threshold_volatility_weight)

    # short_filter_volatility_ema_span
    @st.fragment
    def fragment_short_filter_volatility_ema_span(self):
        if "edit_opt_v7_short_filter_volatility_ema_span" in st.session_state:
            if st.session_state.edit_opt_v7_short_filter_volatility_ema_span != (self.config.optimize.bounds.short_filter_volatility_ema_span_0, self.config.optimize.bounds.short_filter_volatility_ema_span_1):
                self.config.optimize.bounds.short_filter_volatility_ema_span_0 = st.session_state.edit_opt_v7_short_filter_volatility_ema_span[0]
                self.config.optimize.bounds.short_filter_volatility_ema_span_1 = st.session_state.edit_opt_v7_short_filter_volatility_ema_span[1]
        else:
            st.session_state.edit_opt_v7_short_filter_volatility_ema_span = (self.config.optimize.bounds.short_filter_volatility_ema_span_0, self.config.optimize.bounds.short_filter_volatility_ema_span_1)
        st.slider(
            "short_filter_volatility_ema_span",
            min_value=Bounds.FILTER_VOLATILITY_EMA_SPAN_MIN,
            max_value=Bounds.FILTER_VOLATILITY_EMA_SPAN_MAX,
            step=Bounds.FILTER_VOLATILITY_EMA_SPAN_STEP,
            format=Bounds.FILTER_VOLATILITY_EMA_SPAN_FORMAT,
            key="edit_opt_v7_short_filter_volatility_ema_span",
            help=pbgui_help.filter_ema_span)
    
    # short_filter_volatility_drop_pct
    @st.fragment
    def fragment_short_filter_volatility_drop_pct(self):
        if "edit_opt_v7_short_filter_volatility_drop_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_filter_volatility_drop_pct != (self.config.optimize.bounds.short_filter_volatility_drop_pct_0, self.config.optimize.bounds.short_filter_volatility_drop_pct_1):
                self.config.optimize.bounds.short_filter_volatility_drop_pct_0 = st.session_state.edit_opt_v7_short_filter_volatility_drop_pct[0]
                self.config.optimize.bounds.short_filter_volatility_drop_pct_1 = st.session_state.edit_opt_v7_short_filter_volatility_drop_pct[1]
        else:
            st.session_state.edit_opt_v7_short_filter_volatility_drop_pct = (self.config.optimize.bounds.short_filter_volatility_drop_pct_0, self.config.optimize.bounds.short_filter_volatility_drop_pct_1)
        st.slider(
            "short_filter_volatility_drop_pct",
            min_value=Bounds.FILTER_VOLATILITY_DROP_PCT_MIN,
            max_value=Bounds.FILTER_VOLATILITY_DROP_PCT_MAX,
            step=Bounds.FILTER_VOLATILITY_DROP_PCT_STEP,
            format=Bounds.FILTER_VOLATILITY_DROP_PCT_FORMAT,
            key="edit_opt_v7_short_filter_volatility_drop_pct",
            help=pbgui_help.filter_volatility_drop_pct)
    
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

    # short_filter_volume_ema_span
    @st.fragment
    def fragment_short_filter_volume_ema_span(self):
        if "edit_opt_v7_short_filter_volume_ema_span" in st.session_state:
            if st.session_state.edit_opt_v7_short_filter_volume_ema_span != (self.config.optimize.bounds.short_filter_volume_ema_span_0, self.config.optimize.bounds.short_filter_volume_ema_span_1):
                self.config.optimize.bounds.short_filter_volume_ema_span_0 = st.session_state.edit_opt_v7_short_filter_volume_ema_span[0]
                self.config.optimize.bounds.short_filter_volume_ema_span_1 = st.session_state.edit_opt_v7_short_filter_volume_ema_span[1]
        else:
            st.session_state.edit_opt_v7_short_filter_volume_ema_span = (self.config.optimize.bounds.short_filter_volume_ema_span_0, self.config.optimize.bounds.short_filter_volume_ema_span_1)
        st.slider(
            "short_filter_volume_ema_span",
            min_value=Bounds.FILTER_VOLUME_EMA_SPAN_MIN,
            max_value=Bounds.FILTER_VOLUME_EMA_SPAN_MAX,
            step=Bounds.FILTER_VOLUME_EMA_SPAN_STEP,
            format=Bounds.FILTER_VOLUME_EMA_SPAN_FORMAT,
            key="edit_opt_v7_short_filter_volume_ema_span",
            help=pbgui_help.filter_ema_span)

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

    # short_risk_wel_enforcer_threshold
    @st.fragment
    def fragment_short_risk_wel_enforcer_threshold(self):
        if "edit_opt_v7_short_risk_wel_enforcer_threshold" in st.session_state:
            if st.session_state.edit_opt_v7_short_risk_wel_enforcer_threshold != (self.config.optimize.bounds.short_risk_wel_enforcer_threshold_0, self.config.optimize.bounds.short_risk_wel_enforcer_threshold_1):
                self.config.optimize.bounds.short_risk_wel_enforcer_threshold_0 = st.session_state.edit_opt_v7_short_risk_wel_enforcer_threshold[0]
                self.config.optimize.bounds.short_risk_wel_enforcer_threshold_1 = st.session_state.edit_opt_v7_short_risk_wel_enforcer_threshold[1]
        else:
            st.session_state.edit_opt_v7_short_risk_wel_enforcer_threshold = (self.config.optimize.bounds.short_risk_wel_enforcer_threshold_0, self.config.optimize.bounds.short_risk_wel_enforcer_threshold_1)
        st.slider(
            "short_risk_wel_enforcer_threshold",
            min_value=Bounds.RISK_WEL_ENFORCER_THRESHOLD_MIN,
            max_value=Bounds.RISK_WEL_ENFORCER_THRESHOLD_MAX,
            step=Bounds.RISK_WEL_ENFORCER_THRESHOLD_STEP,
            format=Bounds.RISK_WEL_ENFORCER_THRESHOLD_FORMAT,
            key="edit_opt_v7_short_risk_wel_enforcer_threshold",
            help=pbgui_help.risk_wel_enforcer_threshold)

    # short_risk_we_excess_allowance_pct
    @st.fragment
    def fragment_short_risk_we_excess_allowance_pct(self):
        if "edit_opt_v7_short_risk_we_excess_allowance_pct" in st.session_state:
            if st.session_state.edit_opt_v7_short_risk_we_excess_allowance_pct != (self.config.optimize.bounds.short_risk_we_excess_allowance_pct_0, self.config.optimize.bounds.short_risk_we_excess_allowance_pct_1):
                self.config.optimize.bounds.short_risk_we_excess_allowance_pct_0 = st.session_state.edit_opt_v7_short_risk_we_excess_allowance_pct[0]
                self.config.optimize.bounds.short_risk_we_excess_allowance_pct_1 = st.session_state.edit_opt_v7_short_risk_we_excess_allowance_pct[1]
        else:
            st.session_state.edit_opt_v7_short_risk_we_excess_allowance_pct = (self.config.optimize.bounds.short_risk_we_excess_allowance_pct_0, self.config.optimize.bounds.short_risk_we_excess_allowance_pct_1)
        st.slider(
            "short_risk_we_excess_allowance_pct",
            min_value=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_MIN,
            max_value=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_MAX,
            step=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_STEP,
            format=Bounds.RISK_WE_EXCESS_ALLOWANCE_PCT_FORMAT,
            key="edit_opt_v7_short_risk_we_excess_allowance_pct",
            help=pbgui_help.risk_we_excess_allowance_pct)

    # short_risk_twel_enforcer_threshold
    @st.fragment
    def fragment_short_risk_twel_enforcer_threshold(self):
        if "edit_opt_v7_short_risk_twel_enforcer_threshold" in st.session_state:
            if st.session_state.edit_opt_v7_short_risk_twel_enforcer_threshold != (self.config.optimize.bounds.short_risk_twel_enforcer_threshold_0, self.config.optimize.bounds.short_risk_twel_enforcer_threshold_1):
                self.config.optimize.bounds.short_risk_twel_enforcer_threshold_0 = st.session_state.edit_opt_v7_short_risk_twel_enforcer_threshold[0]
                self.config.optimize.bounds.short_risk_twel_enforcer_threshold_1 = st.session_state.edit_opt_v7_short_risk_twel_enforcer_threshold[1]
        else:
            st.session_state.edit_opt_v7_short_risk_twel_enforcer_threshold = (self.config.optimize.bounds.short_risk_twel_enforcer_threshold_0, self.config.optimize.bounds.short_risk_twel_enforcer_threshold_1)
        st.slider(
            "short_risk_twel_enforcer_threshold",
            min_value=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_MIN,
            max_value=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_MAX,
            step=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_STEP,
            format=Bounds.RISK_TWEL_ENFORCER_THRESHOLD_FORMAT,
            key="edit_opt_v7_short_risk_twel_enforcer_threshold",
            help=pbgui_help.risk_twel_enforcer_threshold)

    @st.fragment
    @st.fragment
    def fragment_limits(self):
        # Use centralized metrics from Config module
        ALL_BASE_METRICS = get_all_metrics_list()
        CURRENCY_METRICS_SET = CURRENCY_METRICS
        
        PENALIZE_IF_OPTIONS = ["greater_than", "less_than", "outside_range", "inside_range", "auto"]
        STAT_OPTIONS = ["", "mean", "min", "max", "std"]
        CURRENCY_OPTIONS = ["usd", "btc"]
        
        # Edit limits
        has_limits = bool(self.config.optimize.limits)
        with st.expander("Edit Limits", expanded=has_limits):
            # Init session state
            if "limits_ed_key" not in st.session_state:
                st.session_state.limits_ed_key = 0
            ed_key = st.session_state.limits_ed_key
            
            # Handle data_editor events
            if f'select_limits_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_limits_{ed_key}']
                for row in ed["edited_rows"]:
                    if ed["edited_rows"][row].get("delete"):
                        limits_list = self.config.optimize.limits
                        if 0 <= row < len(limits_list):
                            limits_list.pop(row)
                            self.config.optimize.limits = limits_list
                        self.clean_limits_session_state()
                        st.rerun()
                    if ed["edited_rows"][row].get("edit"):
                        st.session_state.edit_limit_idx = row
            
            # Build display data with separate columns
            if "limits_display_data" not in st.session_state:
                limits_display_data = []
                for entry in self.config.optimize.limits:
                    metric = entry.get("metric", "?")
                    penalize_if = entry.get("penalize_if", "?")
                    stat = entry.get("stat", "")
                    
                    # Format value/range
                    if penalize_if in ("outside_range", "inside_range"):
                        rng = entry.get("range", [0, 0])
                        value_str = f"[{rng[0]}, {rng[1]}]"
                    else:
                        value_str = str(entry.get("value", 0))
                    
                    limits_display_data.append({
                        "metric": metric,
                        "penalize_if": penalize_if,
                        "stat": stat,
                        "value": value_str,
                        "edit": False,
                        "delete": False
                    })
                st.session_state.limits_display_data = limits_display_data
            
            # Display limits table
            if st.session_state.limits_display_data and "edit_limit_idx" not in st.session_state:
                d = st.session_state.limits_display_data
                column_config = {
                    "metric": st.column_config.TextColumn("Metric", width="medium"),
                    "penalize_if": st.column_config.TextColumn("Penalize If", width="small"),
                    "stat": st.column_config.TextColumn("Stat", width="small"),
                    "value": st.column_config.TextColumn("Value", width="small"),
                    "edit": st.column_config.CheckboxColumn("Edit", width="small"),
                    "delete": st.column_config.CheckboxColumn("Delete", width="small"),
                }
                st.data_editor(data=d, height=36+(len(d))*35, key=f'select_limits_{ed_key}', 
                              disabled=['metric', 'penalize_if', 'stat', 'value'],
                              column_config=column_config)
            
            # Edit single limit
            if "edit_limit_idx" in st.session_state:
                idx = st.session_state.edit_limit_idx
                limits_list = self.config.optimize.limits
                if 0 <= idx < len(limits_list):
                    entry = limits_list[idx]
                    self._edit_single_limit(entry, idx, ALL_BASE_METRICS, CURRENCY_METRICS_SET, CURRENCY_OPTIONS, PENALIZE_IF_OPTIONS, STAT_OPTIONS)
            else:
                # Add new limit UI
                self._add_new_limit_ui(ALL_BASE_METRICS, CURRENCY_METRICS_SET, CURRENCY_OPTIONS, PENALIZE_IF_OPTIONS, STAT_OPTIONS)
    
    def _format_limit_display(self, entry: dict) -> str:
        """Format a limit entry for display in the table."""
        metric = entry.get("metric", "?")
        penalize_if = entry.get("penalize_if", "?")
        stat = entry.get("stat", "")
        stat_str = f" ({stat})" if stat else ""
        
        if penalize_if in ("outside_range", "inside_range"):
            rng = entry.get("range", [0, 0])
            return f"{metric}{stat_str} {penalize_if} [{rng[0]}, {rng[1]}]"
        else:
            value = entry.get("value", 0)
            return f"{metric}{stat_str} {penalize_if} {value}"
    
    def _edit_single_limit(self, entry: dict, idx: int, base_metrics: list, currency_metrics_set: set, currency_options: list, penalize_options: list, stat_options: list):
        """UI for editing a single limit entry with split metric/currency selection."""
        st.subheader(f"Edit Limit #{idx + 1}")
        
        # Split current metric into base and currency
        current_full_metric = entry.get("metric", base_metrics[0])
        current_base = current_full_metric
        current_currency = "usd"
        for suffix in ('_usd', '_btc'):
            if current_full_metric.endswith(suffix):
                base = current_full_metric[:-len(suffix)]
                if is_currency_metric(base):
                    current_base = base
                    current_currency = suffix[1:]  # Remove leading underscore
                    break
        
        # Get current values
        current_penalize = entry.get("penalize_if", "greater_than")
        current_stat = entry.get("stat", "")
        is_range = current_penalize in ("outside_range", "inside_range")
        
        # Dynamic column layout
        if is_range:
            col1, col2, col3, col4, col5, col6 = st.columns([1.5, 0.6, 1, 0.6, 0.8, 0.8])
        else:
            col1, col2, col3, col4, col5 = st.columns([1.5, 0.6, 1, 0.6, 1])
        
        with col1:
            base_idx = base_metrics.index(current_base) if current_base in base_metrics else 0
            new_base = st.selectbox("Metric", base_metrics, index=base_idx, key="edit_limit_base_metric", help=pbgui_help.limits)
        
        # Check if newly selected base is a currency metric
        new_is_currency = is_currency_metric(new_base)
        
        with col2:
            if new_is_currency:
                curr_idx = currency_options.index(current_currency) if current_currency in currency_options else 0
                new_currency = st.selectbox("Currency", currency_options, index=curr_idx, key="edit_limit_currency", help=pbgui_help.limit_currency)
            else:
                st.write("")  # Empty placeholder
                new_currency = None
        
        with col3:
            penalize_idx = penalize_options.index(current_penalize) if current_penalize in penalize_options else 0
            new_penalize = st.selectbox("Penalize If", penalize_options, index=penalize_idx, key="edit_limit_penalize", help=pbgui_help.limits_penalize_if)
        
        with col4:
            stat_idx = stat_options.index(current_stat) if current_stat in stat_options else 0
            new_stat = st.selectbox("Stat", stat_options, index=stat_idx, key="edit_limit_stat", help=pbgui_help.limits_stat)
        
        # Combine base + currency for final metric
        if new_is_currency and new_currency:
            new_metric = f"{new_base}_{new_currency}"
        else:
            new_metric = new_base
        
        # Value or range input
        if is_range:
            current_range = entry.get("range", [0.0, 1.0])
            with col5:
                range_low = st.number_input("Range Low", value=float(current_range[0]), format="%.6f", key="edit_limit_range_low", help=pbgui_help.limit_range_low)
            with col6:
                range_high = st.number_input("Range High", value=float(current_range[1]), format="%.6f", key="edit_limit_range_high", help=pbgui_help.limit_range_high)
        else:
            current_value = entry.get("value", 0.0)
            with col5:
                new_value = st.number_input("Value", value=float(current_value), format="%.6f", key="edit_limit_value", help=pbgui_help.limit_value)
        
        # Buttons row
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("OK", key="edit_limit_save"):
                new_entry = {"metric": new_metric, "penalize_if": new_penalize}
                if new_stat:
                    new_entry["stat"] = new_stat
                if new_penalize in ("outside_range", "inside_range"):
                    new_entry["range"] = [range_low, range_high]
                else:
                    new_entry["value"] = new_value
                
                limits_list = self.config.optimize.limits
                limits_list[idx] = new_entry
                self.config.optimize.limits = limits_list
                self.clean_limits_session_state()
                st.rerun()
        with col2:
            if st.button("Cancel", key="edit_limit_cancel"):
                self.clean_limits_session_state()
                st.rerun()
        with col3:
            if st.button("Delete", key="edit_limit_delete"):
                limits_list = self.config.optimize.limits
                limits_list.pop(idx)
                self.config.optimize.limits = limits_list
                self.clean_limits_session_state()
                st.rerun()

    def _add_new_limit_ui(self, base_metrics: list, currency_metrics_set: set, currency_options: list, penalize_options: list, stat_options: list):
        """UI for adding a new limit with split metric/currency selection."""
        st.subheader("Add New Limit")
        
        # Check if currently selected base metric is a currency metric
        current_base = st.session_state.get("add_limit_base_metric", base_metrics[0])
        is_currency = is_currency_metric(current_base)
        is_range = st.session_state.get("add_limit_penalize", "greater_than") in ("outside_range", "inside_range")
        
        # Dynamic column layout
        if is_range:
            col1, col2, col3, col4, col5, col6, col7 = st.columns([1, 0.4, 0.7, 0.4, 0.6, 0.6, 0.6], vertical_alignment="bottom")
        else:
            col1, col2, col3, col4, col5, col6 = st.columns([1.2, 0.5, 0.9, 0.5, 2.3, 0.6], vertical_alignment="bottom")
        
        with col1:
            new_base = st.selectbox("Metric", base_metrics, key="add_limit_base_metric", help=pbgui_help.limits)
        
        # Recheck after selectbox - it may have changed
        is_currency = is_currency_metric(new_base)
        
        with col2:
            if is_currency:
                new_currency = st.selectbox("Currency", currency_options, key="add_limit_currency", help=pbgui_help.limit_currency)
            else:
                st.write("")  # Empty placeholder
                new_currency = None
        
        with col3:
            new_penalize = st.selectbox("Penalize If", penalize_options, key="add_limit_penalize", help=pbgui_help.limits_penalize_if)
        
        with col4:
            new_stat = st.selectbox("Stat", stat_options, key="add_limit_stat", help=pbgui_help.limits_stat)
        
        if is_range:
            with col5:
                range_low = st.number_input("Range Low", value=0.0, format="%.6f", key="add_limit_range_low", help=pbgui_help.limit_range_low)
            with col6:
                range_high = st.number_input("Range High", value=1.0, format="%.6f", key="add_limit_range_high", help=pbgui_help.limit_range_high)
            with col7:
                add_button = st.button("➕", key="add_limit_button", help=pbgui_help.add_limit_button)
        else:
            with col5:
                new_value = st.number_input("Value", value=0.0, format="%.6f", key="add_limit_value", help=pbgui_help.limit_value)
            with col6:
                add_button = st.button("➕", key="add_limit_button", help=pbgui_help.add_limit_button)
        
        if add_button:
            # Combine base and currency for currency metrics
            final_base = st.session_state.get("add_limit_base_metric", base_metrics[0])
            if is_currency_metric(final_base):
                curr = st.session_state.get("add_limit_currency", "usd")
                final_metric = f"{final_base}_{curr}"
            else:
                final_metric = final_base
            
            new_entry = {"metric": final_metric, "penalize_if": st.session_state.get("add_limit_penalize", "greater_than")}
            stat_val = st.session_state.get("add_limit_stat", "")
            if stat_val:
                new_entry["stat"] = stat_val
            if st.session_state.get("add_limit_penalize", "greater_than") in ("outside_range", "inside_range"):
                new_entry["range"] = [st.session_state.get("add_limit_range_low", 0.0), st.session_state.get("add_limit_range_high", 1.0)]
            else:
                new_entry["value"] = st.session_state.get("add_limit_value", 0.0)
            
            limits_list = self.config.optimize.limits
            limits_list.append(new_entry)
            self.config.optimize.limits = limits_list
            self.clean_limits_session_state()
            st.rerun()

    def clean_suite_session_state(self):
        """Clean up suite-related session state keys."""
        keys_to_remove = [k for k in st.session_state.keys() if k.startswith(("suite_", "edit_scenario_", "add_scenario_", "scenario_"))]
        for key in keys_to_remove:
            del st.session_state[key]

    @st.fragment
    def fragment_suite(self):
        """UI for configuring multi-scenario suite for backtesting/optimization."""
        suite = self.config.backtest.suite
        has_scenarios = bool(suite.scenarios)
        
        with st.expander("Suite Configuration", expanded=suite.enabled or has_scenarios):
            # Init session state
            if "suite_ed_key" not in st.session_state:
                st.session_state.suite_ed_key = 0
            
            # Main suite controls
            col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
            with col1:
                new_enabled = st.checkbox("Enable Suite", value=suite.enabled, key="suite_enabled", help=pbgui_help.suite_enabled)
                if new_enabled != suite.enabled:
                    suite.enabled = new_enabled
                    self.config.backtest.suite = suite
            with col2:
                new_include_base = st.checkbox("Include Base Scenario", value=suite.include_base_scenario, key="suite_include_base", help=pbgui_help.suite_include_base_scenario)
                if new_include_base != suite.include_base_scenario:
                    suite.include_base_scenario = new_include_base
                    self.config.backtest.suite = suite
            with col3:
                new_base_label = st.text_input("Base Label", value=suite.base_label, key="suite_base_label", help=pbgui_help.suite_base_label)
                if new_base_label != suite.base_label:
                    suite.base_label = new_base_label
                    self.config.backtest.suite = suite
            with col4:
                aggregate_options = ["mean", "min", "max", "std", "median"]
                current_default = suite.aggregate.get("default", "mean")
                new_default = st.selectbox("Default Aggregate", aggregate_options, index=aggregate_options.index(current_default) if current_default in aggregate_options else 0, key="suite_aggregate_default", help=pbgui_help.suite_aggregate)
                if new_default != current_default:
                    new_agg = dict(suite.aggregate)
                    new_agg["default"] = new_default
                    suite.aggregate = new_agg
                    self.config.backtest.suite = suite
            
            # Metric-specific aggregation rules
            self._edit_aggregate_ui(suite)
            
            # Scenario editing mode check
            if st.session_state.get("edit_scenario_idx") is not None:
                self._edit_scenario_ui(suite)
                return
            
            # Scenarios table
            st.subheader("Scenarios")
            if has_scenarios:
                # Build display data
                d_scenarios = []
                for i, scenario in enumerate(suite.scenarios):
                    coins_display = f"{len(scenario.coins)} coins" if scenario.coins else "base"
                    dates_display = f"{scenario.start_date or 'base'} → {scenario.end_date or 'base'}"
                    if scenario.start_date is None and scenario.end_date is None:
                        dates_display = "base"
                    exchanges_display = ", ".join(scenario.exchanges) if scenario.exchanges else "base"
                    coin_sources_display = f"{len(scenario.coin_sources)} sources" if scenario.coin_sources else "-"
                    overrides_display = f"{len(scenario.overrides)} overrides" if scenario.overrides else "-"
                    d_scenarios.append({
                        "label": scenario.label,
                        "coins": coins_display,
                        "dates": dates_display,
                        "exchanges": exchanges_display,
                        "coin_sources": coin_sources_display,
                        "overrides": overrides_display,
                        "edit": False,
                        "delete": False
                    })
                
                ed_key = st.session_state.suite_ed_key
                
                # Handle data_editor events
                if f'select_scenarios_{ed_key}' in st.session_state:
                    ed = st.session_state[f'select_scenarios_{ed_key}']
                    for row in ed.get("edited_rows", {}):
                        if ed["edited_rows"][row].get("delete"):
                            suite.remove_scenario(row)
                            # Trigger setter to update _backtest dict (like limits pattern)
                            self.config.backtest.suite = suite
                            st.session_state.suite_ed_key += 1
                            st.rerun()
                        if ed["edited_rows"][row].get("edit"):
                            st.session_state.edit_scenario_idx = row
                            st.rerun()
                
                # Display scenarios table
                column_config = {
                    "label": st.column_config.TextColumn("Label", width="medium"),
                    "coins": st.column_config.TextColumn("Coins", width="small"),
                    "dates": st.column_config.TextColumn("Date Range", width="medium"),
                    "exchanges": st.column_config.TextColumn("Exchanges", width="small"),
                    "coin_sources": st.column_config.TextColumn("Coin Sources", width="small"),
                    "overrides": st.column_config.TextColumn("Overrides", width="small"),
                    "edit": st.column_config.CheckboxColumn("Edit", width="small"),
                    "delete": st.column_config.CheckboxColumn("Del", width="small"),
                }
                st.data_editor(d_scenarios, column_config=column_config, hide_index=True, key=f'select_scenarios_{ed_key}', use_container_width=True)
            else:
                st.info("No scenarios configured. Add a scenario below to test your config across different coin sets, date ranges, or parameter variations.")
            
            # Add new scenario UI
            self._add_scenario_ui(suite)

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
    
    def _get_exchanges_for_coin(self, coin: str, available_exchanges: list) -> list:
        """Get list of exchanges that have the specified coin.
        
        Args:
            coin: Coin symbol (e.g., "BTC")
            available_exchanges: List of exchanges to check
            
        Returns:
            List of exchanges that have this coin
        """
        exchanges_with_coin = []
        for exchange in available_exchanges:
            if f"coindata_{exchange}" in st.session_state:
                symbols = st.session_state[f"coindata_{exchange}"].symbols
                # Normalize and check if coin exists
                normalized = [normalize_symbol(s) for s in symbols]
                if coin in normalized:
                    exchanges_with_coin.append(exchange)
        return exchanges_with_coin

    def _edit_coin_sources_ui(self, coin_sources_dict: dict, available_exchanges: list, key_prefix: str = "", save_callback=None, current_exchanges: list = None, all_suite_coin_sources: dict = None):
        """
        UI for editing coin_sources with read-only data_editor and add section.
        
        Args:
            coin_sources_dict: {"BTC": "binance", "SOL": "bybit"}
            available_exchanges: ["binance", "bybit", ...]
            key_prefix: Unique prefix for widget keys (e.g., "scenario_" or "")
            save_callback: Function to call when changes are made (receives updated dict)
            current_exchanges: List of currently selected exchanges (for context)
            all_suite_coin_sources: Merged coin_sources from base + all other scenarios (to prevent conflicts)
        
        Returns:
            Updated coin_sources_dict
        """
        if current_exchanges is None:
            current_exchanges = available_exchanges
        if all_suite_coin_sources is None:
            all_suite_coin_sources = {}
        import pandas as pd
        
        # Expander always visible, expanded only if coin_sources configured
        has_sources = bool(coin_sources_dict)
        expander_title = f"**Coin Sources** ({len(coin_sources_dict)} configured)" if has_sources else "**Coin Sources**"
        
        with st.expander(expander_title, expanded=has_sources):
            st.caption("Override automatic exchange selection for specific coins")
            
            # Display existing mappings in read-only data_editor with delete checkbox
            if coin_sources_dict:
                # Build DataFrame from current dict
                rows = []
                for coin, exchange in sorted(coin_sources_dict.items()):
                    rows.append({
                        "Delete": False,
                        "Coin": coin,
                        "Exchange": exchange
                    })
                df = pd.DataFrame(rows)
                
                # Display as read-only table with only Delete column editable
                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    num_rows="fixed",
                    hide_index=True,
                    column_config={
                        "Delete": st.column_config.CheckboxColumn(
                            "Delete",
                            help=pbgui_help.coin_sources_delete,
                            default=False
                        ),
                        "Coin": st.column_config.TextColumn(
                            "Coin",
                            disabled=True,
                            help=pbgui_help.coin_sources_coin
                        ),
                        "Exchange": st.column_config.TextColumn(
                            "Exchange",
                            disabled=True,
                            help=pbgui_help.coin_sources_exchange
                        )
                    },
                    key=f"{key_prefix}coin_sources_table"
                )
                
                # Process deletions
                coins_to_delete = []
                for _, row in edited_df.iterrows():
                    if row["Delete"]:
                        coins_to_delete.append(row["Coin"])
                
                if coins_to_delete:
                    for coin in coins_to_delete:
                        if coin in coin_sources_dict:
                            del coin_sources_dict[coin]
                    if save_callback:
                        save_callback(coin_sources_dict)
                    st.rerun()
                
            # Add new mapping section
            st.caption("Add new coin source mapping:")
            
            col1, col2, col3 = st.columns([1, 1, 2], vertical_alignment="bottom")
            
            with col1:
                # Step 1: Select exchange - show ALL available exchanges
                all_exchanges = V7.list()  # Always show all exchanges
                selected_exchange = st.selectbox(
                    "Exchange",
                    options=all_exchanges,
                    key=f"{key_prefix}new_coin_source_exchange",
                    help=pbgui_help.coin_sources_select_exchange
                )
            
            with col2:
                # Step 2: Get coins for selected exchange (filtered)
                if selected_exchange:
                    available_coins = self._get_available_symbols([selected_exchange])
                    # Filter: coins already configured in THIS coin_sources (no duplicates within same context)
                    available_coins = [c for c in available_coins if c not in coin_sources_dict]
                    
                    # CRITICAL: Filter coins that exist in suite with DIFFERENT exchange
                    # Passivbot merges all coin_sources and rejects conflicts
                    # Allow coin if: not in suite, OR same exchange
                    # Conflicted coins (None value) are excluded automatically
                    if all_suite_coin_sources:
                        available_coins = [c for c in available_coins 
                                         if c not in all_suite_coin_sources 
                                         or all_suite_coin_sources[c] == selected_exchange]
                    
                    if available_coins:
                        selected_coin = st.selectbox(
                            "Coin",
                            options=available_coins,
                            key=f"{key_prefix}new_coin_source_coin",
                            help=pbgui_help.coin_sources_select_coin
                        )
                    else:
                        st.info(f"All coins from {selected_exchange} are already configured or would conflict with other scenarios")
                        selected_coin = None
                else:
                    st.info("Select exchange first")
                    selected_coin = None
            
            with col3:
                if st.button("➕", key=f"{key_prefix}add_new_coin_source", 
                            disabled=not (selected_exchange and selected_coin),
                            help=pbgui_help.add_coin_source_button):
                    if selected_coin not in coin_sources_dict:
                        coin_sources_dict[selected_coin] = selected_exchange
                        if save_callback:
                            save_callback(coin_sources_dict)
                        st.rerun()
                    else:
                        st.warning(f"{selected_coin} already mapped")
        
        return coin_sources_dict

    def _get_override_parameters(self):
        """Get list of bot parameters that can be overridden in scenarios."""
        return ALLOWED_OVERRIDES

    def _get_aggregate_metrics(self):
        """Get list of metrics that can have custom aggregation."""
        return get_aggregate_metrics()

    def _edit_aggregate_ui(self, suite):
        """UI for editing metric-specific aggregation rules."""
        aggregate_options = ["mean", "min", "max", "std", "median"]
        
        # For aggregation, use base metric names without currency suffix
        metrics = sorted(list(CURRENCY_METRICS) + list(SHARED_METRICS))
        
        # Get current metric-specific aggregations (exclude "default")
        current_aggregates = {k: v for k, v in suite.aggregate.items() if k != "default"}
        
        # Init editor key
        if "suite_agg_ed_key" not in st.session_state:
            st.session_state.suite_agg_ed_key = 0
        
        if current_aggregates:
            st.caption("Metric-specific aggregation rules:")
            # Build display data
            d_aggregates = []
            for metric, agg_method in current_aggregates.items():
                d_aggregates.append({
                    "metric": metric,
                    "aggregation": agg_method,
                    "delete": False
                })
            
            ed_key = st.session_state.suite_agg_ed_key
            
            # Handle data_editor events
            if f'select_aggregates_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_aggregates_{ed_key}']
                changes_made = False
                new_agg = dict(suite.aggregate)
                
                # Handle deletions
                for row in ed.get("edited_rows", {}):
                    if ed["edited_rows"][row].get("delete"):
                        metric_to_delete = d_aggregates[row]["metric"]
                        if metric_to_delete in new_agg:
                            del new_agg[metric_to_delete]
                            changes_made = True
                    # Handle edits
                    elif "metric" in ed["edited_rows"][row] or "aggregation" in ed["edited_rows"][row]:
                        old_metric = d_aggregates[row]["metric"]
                        new_metric = ed["edited_rows"][row].get("metric", old_metric)
                        new_method = ed["edited_rows"][row].get("aggregation", d_aggregates[row]["aggregation"])
                        # Remove old key if metric changed
                        if old_metric != new_metric and old_metric in new_agg:
                            del new_agg[old_metric]
                        new_agg[new_metric] = new_method
                        changes_made = True
                
                if changes_made:
                    suite.aggregate = new_agg
                    self.config.backtest.suite = suite
                    st.session_state.suite_agg_ed_key += 1
                    st.rerun()
            
            # Display aggregates table
            column_config = {
                "metric": st.column_config.SelectboxColumn("Metric", options=metrics, required=True, width="medium"),
                "aggregation": st.column_config.SelectboxColumn("Aggregation", options=aggregate_options, required=True),
                "delete": st.column_config.CheckboxColumn("Del", width="small"),
            }
            st.data_editor(d_aggregates, column_config=column_config, hide_index=True, key=f'select_aggregates_{ed_key}', use_container_width=True)
        
        # Add new metric-specific aggregation
        st.subheader("Add Metric")
        col1, col2, col3 = st.columns([1, 1, 2], vertical_alignment="bottom")
        with col1:
            new_metric = st.selectbox("Metric", metrics, key="suite_agg_new_metric", label_visibility="visible", help=pbgui_help.suite_add_metric)
        with col2:
            new_agg = st.selectbox("Aggregation", aggregate_options, key="suite_agg_new_method", label_visibility="visible", help=pbgui_help.suite_add_aggregation)
        with col3:
            if st.button("➕", key="suite_agg_add", help=pbgui_help.suite_add_button):
                updated_agg = dict(suite.aggregate)
                updated_agg[new_metric] = new_agg
                suite.aggregate = updated_agg
                self.config.backtest.suite = suite
                st.session_state.suite_agg_ed_key += 1
                st.rerun()

    @st.fragment
    def _edit_scenario_start_date(self, scenario, suite, idx):
        """Fragment for editing scenario start date."""
        # Get fresh scenario reference to ensure we have latest data
        scenario = suite.get_scenario(idx)
        if not scenario:
            return None
        
        # Initialize counter for forcing widget refresh
        if "start_date_counter" not in st.session_state:
            st.session_state.start_date_counter = 0
            
        # Prepare default value
        if scenario.start_date:
            try:
                default_start = datetime.datetime.strptime(scenario.start_date[:10], '%Y-%m-%d').date()
            except:
                default_start = None
        else:
            default_start = None
        
        subcol1, subcol2 = st.columns([4, 1], vertical_alignment="bottom")
        with subcol1:
            new_start_date = st.date_input(
                "Start Date (empty = base config)", 
                value=default_start, 
                format="YYYY-MM-DD", 
                key=f"edit_scenario_start_{st.session_state.start_date_counter}",
                help=pbgui_help.scenario_start_date
            )
        with subcol2:
            # Always show button, but check the actual scenario data
            if st.button("🗑️", key="clear_start_date", help=pbgui_help.scenario_clear_date, disabled=(scenario.start_date is None)):
                scenario.start_date = None
                suite.update_scenario(idx, scenario)
                self.config.backtest.suite = suite
                # Increment counter to force widget recreation
                st.session_state.start_date_counter += 1
                st.rerun()
        
        return new_start_date

    @st.fragment
    def _edit_scenario_end_date(self, scenario, suite, idx):
        """Fragment for editing scenario end date."""
        # Get fresh scenario reference to ensure we have latest data
        scenario = suite.get_scenario(idx)
        if not scenario:
            return None
        
        # Initialize counter for forcing widget refresh
        if "end_date_counter" not in st.session_state:
            st.session_state.end_date_counter = 0
            
        # Prepare default value
        if scenario.end_date:
            try:
                default_end = datetime.datetime.strptime(scenario.end_date[:10], '%Y-%m-%d').date()
            except:
                default_end = None
        else:
            default_end = None
        
        subcol1, subcol2 = st.columns([4, 1], vertical_alignment="bottom")
        with subcol1:
            new_end_date = st.date_input(
                "End Date (empty = base config)", 
                value=default_end, 
                format="YYYY-MM-DD", 
                key=f"edit_scenario_end_{st.session_state.end_date_counter}",
                help=pbgui_help.scenario_end_date
            )
        with subcol2:
            # Always show button, but check the actual scenario data
            if st.button("🗑️", key="clear_end_date", help=pbgui_help.scenario_clear_date, disabled=(scenario.end_date is None)):
                scenario.end_date = None
                suite.update_scenario(idx, scenario)
                self.config.backtest.suite = suite
                # Increment counter to force widget recreation
                st.session_state.end_date_counter += 1
                st.rerun()
        
        return new_end_date

    def _edit_scenario_ui(self, suite):
        """UI for editing an existing scenario."""
        idx = st.session_state.edit_scenario_idx
        scenario = suite.get_scenario(idx)
        if not scenario:
            st.session_state.edit_scenario_idx = None
            st.rerun()
            return
        
        st.subheader(f"Edit Scenario: {scenario.label}")
        
        # Label, Exchanges, and Date range on one line
        col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
        with col1:
            new_label = st.text_input("Label", value=scenario.label, key="edit_scenario_label", help=pbgui_help.scenario_label)
        with col2:
            available_exchanges = V7.list()
            current_exchanges = scenario.exchanges if scenario.exchanges else []
            new_exchanges = st.multiselect("Exchanges (leave empty for base)", available_exchanges, default=current_exchanges, key="edit_scenario_exchanges", help=pbgui_help.scenario_exchanges)
        with col3:
            new_start_date = self._edit_scenario_start_date(scenario, suite, idx)
        with col4:
            new_end_date = self._edit_scenario_end_date(scenario, suite, idx)
        
        # Get symbols based on selected exchanges (or base config exchanges if none selected)
        exchanges_for_symbols = new_exchanges if new_exchanges else None
        symbols = self._get_available_symbols(exchanges_for_symbols)
        
        # Coins - Multi-Select
        current_coins = scenario.coins if scenario.coins else []
        # Filter out coins not in symbols list
        valid_coins = [c for c in current_coins if c in symbols]
        col1, col2 = st.columns(2)
        with col1:
            new_coins = st.multiselect("Coins (leave empty for base)", symbols, default=valid_coins, key="edit_scenario_coins", help=pbgui_help.scenario_coins)
        with col2:
            current_ignored = scenario.ignored_coins if scenario.ignored_coins else []
            valid_ignored = [c for c in current_ignored if c in symbols]
            new_ignored = st.multiselect("Ignored Coins", symbols, default=valid_ignored, key="edit_scenario_ignored", help=pbgui_help.scenario_ignored_coins)
        
        # Coin Sources
        exchanges_for_sources = new_exchanges if new_exchanges else available_exchanges
        
        # Collect all suite coin_sources EXCEPT this scenario to check for conflicts
        # Like Passivbot's collect_suite_coin_sources(), detect conflicts
        all_suite_sources = {}
        # Add base coin_sources
        if self.config.backtest.coin_sources:
            all_suite_sources.update(self.config.backtest.coin_sources)
        # Add all other scenarios' coin_sources (exclude current)
        for i, s in enumerate(suite.scenarios):
            if i != idx and s.coin_sources:  # Exclude current scenario
                for coin, exchange in s.coin_sources.items():
                    if coin in all_suite_sources and all_suite_sources[coin] != exchange:
                        # Conflict detected - mark as conflicted
                        all_suite_sources[coin] = None
                    elif coin not in all_suite_sources:
                        all_suite_sources[coin] = exchange
        
        self._edit_coin_sources_ui(
            scenario.coin_sources if scenario.coin_sources else {},
            exchanges_for_sources,
            key_prefix="scenario_",
            save_callback=lambda cs: setattr(scenario, 'coin_sources', cs),
            current_exchanges=new_exchanges if new_exchanges else available_exchanges,
            all_suite_coin_sources=all_suite_sources
        )
        
        # Overrides - GUI-based
        st.write("**Parameter Overrides**")
        override_params = self._get_override_parameters()
        sides = ["long", "short"]
        
        # Initialize overrides editor key
        if "edit_scenario_overrides_ed_key" not in st.session_state:
            st.session_state.edit_scenario_overrides_ed_key = 0
        
        # Build display data from scenario overrides
        d_overrides = []
        if scenario.overrides:
            for key, value in scenario.overrides.items():
                parts = key.split(".")
                if len(parts) == 3 and parts[0] == "bot" and parts[1] in sides:
                    d_overrides.append({
                        "side": parts[1],
                        "parameter": parts[2],
                        "value": float(value) if isinstance(value, (int, float)) else 0.0,
                        "delete": False
                    })
        
        ed_key = st.session_state.edit_scenario_overrides_ed_key
        
        if d_overrides:
            # Handle data_editor events
            if f'select_overrides_{ed_key}' in st.session_state:
                ed = st.session_state[f'select_overrides_{ed_key}']
                changes_made = False
                new_overrides = {}
                
                for row_idx, override_data in enumerate(d_overrides):
                    # Check if this row was deleted
                    if row_idx in ed.get("edited_rows", {}) and ed["edited_rows"][row_idx].get("delete"):
                        changes_made = True
                        continue
                    
                    # Get edited or original values
                    side = ed.get("edited_rows", {}).get(row_idx, {}).get("side", override_data["side"])
                    param = ed.get("edited_rows", {}).get(row_idx, {}).get("parameter", override_data["parameter"])
                    value = ed.get("edited_rows", {}).get(row_idx, {}).get("value", override_data["value"])
                    
                    # Check if anything changed
                    if (side != override_data["side"] or param != override_data["parameter"] or value != override_data["value"]):
                        changes_made = True
                    
                    # Convert to int if whole number
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    key = f"bot.{side}.{param}"
                    new_overrides[key] = value
                
                if changes_made:
                    scenario.overrides = new_overrides
                    st.session_state.edit_scenario_overrides_ed_key += 1
                    st.rerun()
            
            # Display overrides table
            column_config = {
                "side": st.column_config.SelectboxColumn("Side", options=sides, required=True),
                "parameter": st.column_config.SelectboxColumn("Parameter", options=override_params, required=True, width="large"),
                "value": st.column_config.NumberColumn("Value", format="%.6f"),
                "delete": st.column_config.CheckboxColumn("Del", width="small"),
            }
            st.data_editor(d_overrides, column_config=column_config, hide_index=True, key=f'select_overrides_{ed_key}', use_container_width=True)
        
        # Add new override with selection
        col1, col2, col3, col4 = st.columns([2, 4, 1, 1], vertical_alignment="bottom")
        with col1:
            new_side = st.selectbox("Side", sides, key="edit_scenario_add_override_side", help=pbgui_help.scenario_override_side)
        with col2:
            new_param = st.selectbox("Parameter", override_params, key="edit_scenario_add_override_param", help=pbgui_help.scenario_override_param)
        with col3:
            new_value = st.number_input("Value", value=0.0, format="%.6f", key="edit_scenario_add_override_value", help=pbgui_help.scenario_override_value)
        with col4:
            if st.button("➕", key="edit_scenario_add_override", help=pbgui_help.add_scenario_override_button):
                new_overrides = dict(scenario.overrides) if scenario.overrides else {}
                new_key = f"bot.{new_side}.{new_param}"
                # Convert to int if whole number
                if isinstance(new_value, float) and new_value.is_integer():
                    new_value = int(new_value)
                new_overrides[new_key] = new_value
                scenario.overrides = new_overrides
                st.session_state.edit_scenario_overrides_ed_key += 1
                st.rerun()
        
        # Buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("OK", key="edit_scenario_ok"):
                # Save
                scenario.label = new_label.strip()
                scenario.coins = new_coins if new_coins else []
                scenario.ignored_coins = new_ignored if new_ignored else []
                scenario.start_date = new_start_date.strftime("%Y-%m-%d") if new_start_date else None
                scenario.end_date = new_end_date.strftime("%Y-%m-%d") if new_end_date else None
                scenario.exchanges = new_exchanges if new_exchanges else None
                # Overrides are already saved in the table editor
                # Just make sure the suite is updated
                
                suite.update_scenario(idx, scenario)
                # Trigger setter to update _backtest dict (like limits pattern)
                self.config.backtest.suite = suite
                # Clean up session state
                st.session_state.edit_scenario_idx = None
                st.session_state.suite_ed_key += 1
                st.rerun()
        with col2:
            if st.button("Cancel", key="edit_scenario_cancel"):
                st.session_state.edit_scenario_idx = None
                st.rerun()
        with col3:
            if st.button("Delete", key="edit_scenario_delete"):
                suite.remove_scenario(idx)
                # Trigger setter to update _backtest dict (like limits pattern)
                self.config.backtest.suite = suite
                st.session_state.edit_scenario_idx = None
                st.session_state.suite_ed_key += 1
                st.rerun()

    def _add_scenario_ui(self, suite):
        """UI for adding a new scenario."""
        st.subheader("Add Scenario")
        
        col1, col2, col3 = st.columns([1, 2, 1], vertical_alignment="bottom")
        with col1:
            new_label = st.text_input("Label", key="add_scenario_label", help=pbgui_help.scenario_label, placeholder="e.g., bull_market_2024")
        with col2:
            # Get symbols based on base config exchanges (since no exchanges selected yet in new scenario)
            symbols = self._get_available_symbols()
            new_coins = st.multiselect("Coins (optional)", symbols, key="add_scenario_coins", help=pbgui_help.scenario_coins)
        with col3:
            if st.button("➕", key="add_scenario_button", help=pbgui_help.add_scenario_button):
                if new_label.strip():
                    new_scenario = {
                        "label": new_label.strip(),
                        "coins": new_coins if new_coins else []
                    }
                    suite.add_scenario(new_scenario)
                    # Trigger setter to update _backtest dict (like limits pattern)
                    self.config.backtest.suite = suite
                    st.session_state.suite_ed_key += 1
                    # Clear add fields
                    if "add_scenario_label" in st.session_state:
                        del st.session_state["add_scenario_label"]
                    if "add_scenario_coins" in st.session_state:
                        del st.session_state["add_scenario_coins"]
                    st.rerun()
                else:
                    error_popup("Label is required")

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
        col1, col2, col3, col4, col5, col6 = st.columns([1,1,0.5,0.5,0.5,0.5], vertical_alignment="bottom")
        with col1:
            self.fragment_starting_balance()
        with col2:
            self.fragment_iters()
        with col3:
            self.fragment_n_cpus()
        with col4:
            self.fragment_logging()
        with col5:
            self.fragment_starting_config()
            self.fragment_combine_ohlcvs()
        with col6:
            self.fragment_compress_results_file()
            self.fragment_write_all_results()
        
        # Coin Sources - full width for better layout consistency with scenarios
        self.fragment_coin_sources()
        
        with st.expander("Edit Config", expanded=False):
            self.config.bot.edit()

        # Limits
        self.fragment_limits()
        
        # Suite (multi-scenario)
        self.fragment_suite()
        
        # Optimizer
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_population_size()
        with col2:
            self.fragment_pareto_max_size()
        with col3:
            self.fragment_offspring_multiplier()
        with col4:
            self.fragment_mutation_indpb()
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            self.fragment_crossover_probability()
        with col2:
            self.fragment_crossover_eta()
        with col3:
            self.fragment_mutation_probability()
        with col4:
            self.fragment_mutation_eta()
        self.fragment_scoring()

        # Filters
        self.fragment_filter_coins()

        # Optimizer Bounds
        col1, col2 = st.columns([1,1])
        with col1:
            with st.container(border=True):
                st.write("Bounds long")
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
                self.fragment_long_entry_grid_spacing_volatility_weight()
                self.fragment_long_entry_grid_spacing_we_weight()
                self.fragment_long_entry_initial_ema_dist()
                self.fragment_long_entry_initial_qty_pct()
                self.fragment_long_entry_trailing_double_down_factor()
                self.fragment_long_entry_trailing_grid_ratio()
                self.fragment_long_entry_trailing_retracement_pct()
                self.fragment_long_entry_trailing_retracement_volatility_weight()
                self.fragment_long_entry_trailing_retracement_we_weight()
                self.fragment_long_entry_trailing_threshold_pct()
                self.fragment_long_entry_trailing_threshold_volatility_weight()
                self.fragment_long_entry_trailing_threshold_we_weight()
                self.fragment_long_entry_volatility_ema_span_hours()
                self.fragment_long_filter_volatility_drop_pct()
                self.fragment_long_filter_volatility_ema_span()
                self.fragment_long_filter_volume_drop_pct()
                self.fragment_long_filter_volume_ema_span()
                self.fragment_long_n_positions()
                self.fragment_long_risk_twel_enforcer_threshold()
                self.fragment_long_risk_we_excess_allowance_pct()
                self.fragment_long_risk_wel_enforcer_threshold()
                self.fragment_long_total_wallet_exposure_limit()
                self.fragment_long_unstuck_close_pct()
                self.fragment_long_unstuck_ema_dist()
                self.fragment_long_unstuck_loss_allowance_pct()
                self.fragment_long_unstuck_threshold()

        with col2:
            with st.container(border=True):
                st.write("Bounds short")
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
                self.fragment_short_entry_grid_spacing_volatility_weight()
                self.fragment_short_entry_grid_spacing_we_weight()
                self.fragment_short_entry_initial_ema_dist()
                self.fragment_short_entry_initial_qty_pct()
                self.fragment_short_entry_trailing_double_down_factor()
                self.fragment_short_entry_trailing_grid_ratio()
                self.fragment_short_entry_trailing_retracement_pct()
                self.fragment_short_entry_trailing_retracement_volatility_weight()
                self.fragment_short_entry_trailing_retracement_we_weight()
                self.fragment_short_entry_trailing_threshold_pct()
                self.fragment_short_entry_trailing_threshold_volatility_weight()
                self.fragment_short_entry_trailing_threshold_we_weight()
                self.fragment_short_entry_volatility_ema_span_hours()
                self.fragment_short_filter_volatility_drop_pct()
                self.fragment_short_filter_volatility_ema_span()
                self.fragment_short_filter_volume_drop_pct()
                self.fragment_short_filter_volume_ema_span()
                self.fragment_short_n_positions()
                self.fragment_short_risk_twel_enforcer_threshold()
                self.fragment_short_risk_we_excess_allowance_pct()
                self.fragment_short_risk_wel_enforcer_threshold()
                self.fragment_short_total_wallet_exposure_limit()
                self.fragment_short_unstuck_close_pct()
                self.fragment_short_unstuck_ema_dist()
                self.fragment_short_unstuck_loss_allowance_pct()
                self.fragment_short_unstuck_threshold()

    def clean_limits_session_state(self):
        # Remove all limits-related session state keys
        keys_to_remove = [k for k in st.session_state.keys() if 
                         k.startswith("limits_") or 
                         k.startswith("edit_limit") or 
                         k.startswith("add_limit") or
                         k.startswith("select_limits_")]
        for key in keys_to_remove:
            del st.session_state[key]

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
        self.d = []
        self.sort = "Time"
        self.sort_order = True
        self.load_sort()

    def view_optimizes(self):
        # Init
        if not self.optimizes:
            self.find_optimizes()
        if not "ed_key" in st.session_state:
            st.session_state.ed_key = 0
        ed_key = st.session_state.ed_key
        if not self.d:
            for id, opt in enumerate(self.optimizes):
                self.d.append({
                    'id': id,
                    'Select': False,
                    'Name': opt.name,
                    'Time': opt.time,
                    'Exchange': opt.config.backtest.exchanges,
                    'BT Count': opt.backtest_count,
                    'item': opt,
                })
        column_config = {
            "id": None,
            'item': None,
            "Select": st.column_config.CheckboxColumn(label="Select"),
            "Time": st.column_config.DatetimeColumn(label="Time", format="YYYY-MM-DD HH:mm:ss"),
            }
        # Display optimizes
        if "sort_opt_v7" in st.session_state:
            if st.session_state.sort_opt_v7 != self.sort:
                self.sort = st.session_state.sort_opt_v7
                self.save_sort()
        else:
            st.session_state.sort_opt_v7 = self.sort
        if "sort_opt_v7_order" in st.session_state:
            if st.session_state.sort_opt_v7_order != self.sort_order:
                self.sort_order = st.session_state.sort_opt_v7_order
                self.save_sort()
        else:
            st.session_state.sort_opt_v7_order = self.sort_order
        # Display sort options
        col1, col2 = st.columns([1, 9], vertical_alignment="bottom")
        with col1:
            st.selectbox("Sort by:", ['Time', 'Name', 'BT Count', 'Exchange'], key=f'sort_opt_v7', index=0)
        with col2:
            st.checkbox("Reverse", value=True, key=f'sort_opt_v7_order')
        self.d = sorted(self.d, key=lambda x: x[st.session_state[f'sort_opt_v7']], reverse=st.session_state[f'sort_opt_v7_order'])
        height = 36+(len(self.d))*35
        if height > 1000: height = 1016
        st.data_editor(data=self.d, height=height, key=f'select_optimize_v7_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','name'])

    def load_sort(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        self.sort = pb_config.get("optimize_v7", "sort") if pb_config.has_option("optimize_v7", "sort") else "Time"
        self.sort_order = eval(pb_config.get("optimize_v7", "sort_order")) if pb_config.has_option("optimize_v7", "sort_order") else True

    def save_sort(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("optimize_v7"):
            pb_config.add_section("optimize_v7")
        pb_config.set("optimize_v7", "sort", str(self.sort))
        pb_config.set("optimize_v7", "sort_order", str(self.sort_order))
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    def find_optimizes(self):
        p = str(Path(f'{PBGDIR}/data/opt_v7/*.json'))
        found_opt = glob.glob(p, recursive=False)
        if found_opt:
            for p in found_opt:
                opt = OptimizeV7Item(p)
                self.optimizes.append(opt)

    def edit_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_optimize_v7_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            error_popup("No Optimizes selected")
            return
        elif selected_count > 1:
            error_popup("Please select only one Optimize to view")
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    st.session_state.opt_v7 = self.d[row]["item"]
                    st.rerun()

    @st.dialog("No Optimize selected. Delete all?")
    def remove_all(self):
        st.warning(f"Delete all Optimizes?", icon="⚠️")
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button(":green[Yes]"):
                shutil.rmtree(f'{PBGDIR}/data/opt_v7', ignore_errors=True)
                self.d = []
                self.optimizes = []
                st.rerun()
        with col2:
            if st.button(":red[No]"):
                st.rerun()

    def remove_selected(self):
        ed_key = st.session_state.ed_key
        ed = st.session_state[f'select_optimize_v7_{ed_key}']
        # Get number of selected results
        selected_count = sum(1 for row in ed["edited_rows"] if "Select" in ed["edited_rows"][row] and ed["edited_rows"][row]["Select"])
        if selected_count == 0:
            self.remove_all()
            return
        for row in ed["edited_rows"]:
            if "Select" in ed["edited_rows"][row]:
                if ed["edited_rows"][row]["Select"]:
                    self.d[row]["item"].remove()
        self.d = []
        self.optimizes = []
        st.rerun()


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
            if item.is_existing():
                if item.status() == "not started" or item.status() == "error":
                    print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Optimizing {item.filename} started')
                    item.run()
                    time.sleep(1)
            else:
                print(f'{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")} Optimize config file for {item.filename} not found, jumping to next in queue')
        time.sleep(60)

if __name__ == '__main__':
    main()