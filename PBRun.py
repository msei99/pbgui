"""
PBRun is the main bit of PBGui, being split in 3 main parts, PBRun and RunSingle/RunMulti, the two last doing the same things respective to their config.

PBRun checks for status and activate files, updating the old ones to the newest, and starting functions from RunSingle or RunMulti if there are any needed.

RunMulti and Single do start and stop passivbot programs.
"""
import psutil
import subprocess
import configparser
import shlex
import sys
from pathlib import Path, PurePath
from time import sleep, mktime
import glob
import json
import hjson
from io import TextIOWrapper
from datetime import datetime, date, timedelta
import platform
from shutil import copy, copytree, rmtree
import os
import traceback
import uuid
from Status import InstanceStatus, InstancesStatus
import re

class RunSingle():
    def __init__(self):
        self.user = None
        self.path = None
        self._single_config = {}
        self._parameters = None
        self.name = None
        self.multi = False
        self.version = None
        self.pbdir = None
        self.pbvenv = None
        self.pbgdir = None
    
    def watch(self):
        if not self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start Single from watch: {self.user} {self.symbol}')
            self.start()

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied:
                pass
            if self.user in cmdline and self.symbol in cmdline and any("passivbot.py" in sub for sub in cmdline):
                return process

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: {self.user} {self.symbol}')
            self.pid().kill()

    def start(self):
        if not self.is_running():
            config = PurePath(f'{self.path}/config.json')
            cmd = [self.pbvenv, '-u', PurePath(f'{self.pbdir}/passivbot.py')]
            cmd_end = f'{self.parameters} {self.user} {self.symbol} '.lstrip(' ')
            cmd.extend(shlex.split(cmd_end))
            cmd.extend([config])
            logfile = Path(f'{self.path}/passivbot.log')
            log = open(logfile,"ab")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True, start_new_session=True)
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start Single: {cmd_end}')

    def clean_log(self):
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile_old = Path(f'{str(logfile)}.old')
                copy(logfile,logfile_old)
                with open(logfile,'r+') as file:
                    file.truncate()

    def create_parameters(self):
        """Create the list of parameters used when running passivbot single instance.

        Parameters :
            - "_long_mode": Determines long mode parameters ("-lm gs", "-lm p", or "-lm t").
            - "_short_mode": Determines short mode parameters ("-sm gs", "-sm p", or "-sm t").
            - "_market_type": If not "swap", adds "-m spot" to parameters.
            - "_ohlcv": If False, adds "-oh n" to parameters.
            - "_co": Adds "-co {value}" if "_co" is present and not equal to -1.
            - "_leverage": Adds "-lev {value}" if "_leverage" is present and not equal to 7.
            - "_assigned_balance": Adds "-ab {value}" if "_assigned_balance" is present and not equal to 0.
            - "_price_distance_threshold": Adds "-pt {value}" if "_price_distance_threshold" is present and not equal to 0.5.
            - "_price_precision": Adds "-pp {value}" if "_price_precision" is present and not equal to 0.0.
            - "_price_step": Adds "-ps {value}" if "_price_step" is present and not equal to 0.0.
        """
        # Write running Version to file
        if "_version" in self._single_config:
            version = str(self._single_config["_version"])
        else:
            version = 0
        version_file = Path(f'{self.path}/running_version.txt')
        with open(version_file, "w", encoding='utf-8') as f:
            f.write(version)
        # Generate parameters
        self.parameters = ""
        if "_long_mode" in self._single_config:
            if self._single_config["_long_mode"] == "graceful_stop":
                self.parameters = (self.parameters + f' -lm gs').lstrip(' ')
            if self._single_config["_long_mode"] == "panic":
                self.parameters = (self.parameters + f' -lm p').lstrip(' ')
            if self._single_config["_long_mode"] == "tp_only":
                self.parameters = (self.parameters + f' -lm t').lstrip(' ')
        if "_short_mode" in self._single_config:
            if self._single_config["_short_mode"] == "graceful_stop":
                self.parameters = (self.parameters + f' -sm gs').lstrip(' ')
            if self._single_config["_short_mode"] == "panic":
                self.parameters = (self.parameters + f' -sm p').lstrip(' ')
            if self._single_config["_short_mode"] == "tp_only":
                self.parameters = (self.parameters + f' -sm t').lstrip(' ')
        if "_market_type" in self._single_config:
            if self._single_config["_market_type"] != "swap":
                self.parameters = (self.parameters + f' -m spot').lstrip(' ')
        if "_ohlcv" in self._single_config:
            if not self._single_config["_ohlcv"]:
                self.parameters = (self.parameters + f' -oh n').lstrip(' ')
        if "_co" in self._single_config:
            if self._single_config["_co"] != -1:
                self.parameters = (self.parameters + f' -co {self._single_config["_co"]}').lstrip(' ')
        if "_leverage" in self._single_config:
            if self._single_config["_leverage"] != 7:
                self.parameters = (self.parameters + f' -lev {self._single_config["_leverage"]}').lstrip(' ')
        if "_assigned_balance" in self._single_config:
            if self._single_config["_assigned_balance"] != 0:
                self.parameters = (self.parameters + f' -ab {self._single_config["_assigned_balance"]}').lstrip(' ')
        if "_price_distance_threshold" in self._single_config:
            if self._single_config["_price_distance_threshold"] != 0.5:
                self.parameters = (self.parameters + f' -pt {self._single_config["_price_distance_threshold"]}').lstrip(' ')
        if "_price_precision" in self._single_config:
            if self._single_config["_price_precision"] != 0.0:
                self.parameters = (self.parameters + f' -pp {self._single_config["_price_precision"]}').lstrip(' ')
        if "_price_step" in self._single_config:
            if self._single_config["_price_step"] != 0.0:
                self.parameters = (self.parameters + f' -ps {self._single_config["_price_step"]}').lstrip(' ')

    def load(self):
        file = Path(f'{self.path}/instance.cfg')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    single_config = f.read()
                self._single_config = json.loads(single_config)
                if "_version" in self._single_config:
                    self.version = self._single_config["_version"]
                else:
                    self.version = 0
                # Load user from config
                if "_user" in self._single_config:
                    self.user = self._single_config["_user"]
                # Load symbol from config
                if "_symbol" in self._single_config:
                    self.symbol = self._single_config["_symbol"]
                if "_multi" in self._single_config:
                    self.multi = self._single_config["_multi"]
                if "_enabled_on" in self._single_config:
                    if self.name == self._single_config["_enabled_on"]:
                        if self.multi:
                            return False
                        else:
                            return True
                    else:                        
                        self.name = self._single_config["_enabled_on"]
                else:
                    self.name = "disabled"
                return False
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

class RunMulti():
    def __init__(self):
        self.user = None
        self.path = None
        self._multi_config = {}
        self.name = None
        self.version = None
        self.pbdir = None
        self.pbvenv = None
        self.pbgdir = None
    
    def watch(self):
        if not self.is_running():
            self.start()

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied:
                pass
            if any(self.user in sub for sub in cmdline) and any("passivbot_multi.py" in sub for sub in cmdline):
                return process

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: passivbot_multi.py {self.path}/multi_run.hjson')
            self.pid().kill()

    def start(self):
        if not self.is_running():
            cmd = [self.pbvenv, '-u', PurePath(f'{self.pbdir}/passivbot_multi.py'), PurePath(f'{self.path}/multi_run.hjson')]
            logfile = Path(f'{self.path}/passivbot.log')
            log = open(logfile,"ab")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True, start_new_session=True)
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: passivbot_multi.py {self.path}/multi_run.hjson')

    def clean_log(self):
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile_old = Path(f'{str(logfile)}.old')
                copy(logfile,logfile_old)
                with open(logfile,'r+') as file:
                    file.truncate()

    def create_multi_hjson(self):
        # Write running Version to file
        version = str(self._multi_config["version"])
        version_file = Path(f'{self.path}/running_version.txt')
        with open(version_file, "w", encoding='utf-8') as f:
            f.write(version)
        # Generate clean multi_run.hjson file
        del self._multi_config["enabled_on"]
        del self._multi_config["version"]
        self._multi_config["live_configs_dir"] = self.path
        if "default_config_path" in self._multi_config:
            if self._multi_config["default_config_path"] != "":
                self._multi_config["default_config_path"] = f'{self.path}/default.json'
        run_config = hjson.dumps(self._multi_config)
        config_file = Path(f'{self.path}/multi_run.hjson')
        with open(config_file, "w", encoding='utf-8') as f:
            f.write(run_config)

    def load(self):
        """Load config for PB multi."""
        file = Path(f'{self.path}/multi.hjson')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    multi_config = f.read()
                self._multi_config = hjson.loads(multi_config)
                if "version" in self._multi_config:
                    self.version = self._multi_config["version"]
                if "enabled_on" in self._multi_config:
                    if self.name == self._multi_config["enabled_on"]:
                        return True
                    else:                        
                        self.name = self._multi_config["enabled_on"]
                else:
                    self.name = "disabled"
                return False
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

class RunV7():
    def __init__(self):
        self.user = None
        self.path = None
        self._v7_config = {}
        self.name = None
        self.version = None
        self.pbdir = None
        self.pbvenv = None
        self.pbgdir = None
        self.log_lp = None
        self.start_time = 0
        self.memory = 0
        self.cpu = 0
        self.log_error = None
        self.log_info = None
        self.log_traceback = None
        self.log_watch_ts = 0
        self.error_time = 0
        self.errors_today = 0
        self.errors_yesterday = 0
        self.info_time = 0
        self.infos_today = 0
        self.infos_yesterday = 0
        self.traceback_time = 0
        self.tracebacks_today = 0
        self.tracebacks_yesterday = 0
        self.pnl_today = 0
        self.pnl_yesterday = 0
        self.pnl_counter_today = 0
        self.pnl_counter_yesterday = 0

    def watch(self):
        if not self.is_running():
            self.start()

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied:
                pass
            if any(self.user in sub for sub in cmdline) and any("main.py" in sub for sub in cmdline):
                if cmdline[-1].endswith(f'{self.user}/config.json') or cmdline[-1].endswith(f'{self.user}\config.json'):
                    self.start_time = process.create_time()
                    self.memory = process.memory_info()
                    self.cpu = process.cpu_percent()
                    return process

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: passivbot v7 {self.path}/config.json')
            self.pid().kill()

    def start(self):
        if not self.is_running():
            old_os_path = os.environ.get('PATH', '')
            new_os_path = os.path.dirname(self.pbvenv) + os.pathsep + old_os_path
            os.environ['PATH'] = new_os_path
            cmd = [self.pbvenv, '-u', PurePath(f'{self.pbdir}/src/main.py'), PurePath(f'{self.path}/config.json')]
            logfile = Path(f'{self.path}/passivbot.log')
            log = open(logfile,"ab")
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pbdir, text=True, start_new_session=True)
            os.environ['PATH'] = old_os_path
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: passivbot_v7 {self.path}/config.json')
        # wait until passivbot is running
        for i in range(10):
            if self.is_running():
                break
            sleep(1)

    def clean_log(self):
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile_old = Path(f'{str(logfile)}.old')
                copy(logfile,logfile_old)
                with open(logfile,'r+') as file:
                    file.truncate()

    def watch_log(self):
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            seek = False
            if not self.log_lp:
                self.log_lp = 0
                seek = True
            current_position = logfile.stat().st_size
            with open(logfile, "r") as f:
                f.seek(self.log_lp)
                new_content = f.read().splitlines()
            self.log_lp = current_position
            tb_found = False
            today_ts = int(mktime(date.today().timetuple()))
            yesterday_ts = today_ts - 86400
            if self.log_watch_ts != 0 and self.log_watch_ts < today_ts:
                self.log_error = None
                self.log_info = None
                self.log_traceback = None
                self.errors_yesterday = self.errors_today
                self.errors_today = 0
                self.infos_yesterday = self.infos_today
                self.infos_today = 0
                self.tracebacks_yesterday = self.tracebacks_today
                self.tracebacks_today = 0
                self.pnl_yesterday = self.pnl_today
                self.pnl_today = 0
                self.pnl_counter_yesterday = self.pnl_counter_today
                self.pnl_counter_today = 0
            for line in new_content:
                elements = line.split()
                if len(elements) > 1:
                    if elements[1] == "ERROR" or elements[1] == "INFO":
                        # check elements[0] for correct isoformat
                        if len(elements[0]) == 19:
                            if elements[0][4] == "-" and elements[0][7] == "-" and elements[0][10] == "T" and elements[0][13] == ":" and elements[0][16] == ":":
                                ts = int(datetime.fromisoformat(elements[0]).timestamp())
                                if ts < yesterday_ts:
                                    continue
                                else:
                                    seek = False
                                if ts < today_ts:
                                    yesterday = True
                                else:
                                    yesterday = False
                if seek:
                    continue
                if tb_found:
                    if not "ERROR" in line and not "INFO" in line and not "Traceback" in line:
                        self.log_traceback.append(line)
                    else:
                        tb_found = False
                        self.tracebacks_today += 1
                if "ERROR" in line:
                    if yesterday:
                        self.errors_yesterday += 1
                    else:
                        self.log_error = line
                        self.errors_today += 1
                elif "INFO" in line:                 
                    if yesterday:
                        self.infos_yesterday += 1
                    else:
                        self.log_info = line
                        self.infos_today += 1
                    if "new pnl" in line:
                        if len(elements) == 7:
                            if yesterday:
                                self.pnl_yesterday += float(elements[5])
                                self.pnl_counter_yesterday += int(elements[2])
                            else:
                                self.pnl_today += float(elements[5])
                                self.pnl_counter_today += int(elements[2])
                elif "Traceback" in line:
                    if yesterday:
                        self.tracebacks_yesterday += 1
                    else:
                        self.log_traceback = []
                        self.log_traceback.append(line)
                        tb_found = True
            self.log_watch_ts = int(datetime.now().timestamp())
            self.save_monitor()

    def save_monitor(self):
        monitor_file = Path(f'{self.path}/monitor.json')
        monitor = ({
            "user": self.user,
            "start_time": self.start_time,
            "memory": self.memory,
            "cpu": self.cpu,
            "log_info": self.log_info,
            "log_infos_today": self.infos_today,
            "log_infos_yesterday": self.infos_yesterday,
            "log_error": self.log_error,
            "log_errors_today": self.errors_today,
            "log_errors_yesterday": self.errors_yesterday,
            "log_traceback": self.log_traceback,
            "log_tracebacks_today": self.tracebacks_today,
            "log_tracebacks_yesterday": self.tracebacks_yesterday,
            "pnls_today": self.pnl_today,
            "pnls_yesterday": self.pnl_yesterday,
            "pnl_counter_today": self.pnl_counter_today,
            "pnl_counter_yesterday": self.pnl_counter_yesterday
            })
        with open(monitor_file, "w", encoding='utf-8') as f:
            json.dump(monitor, f)

    def create_v7_running_version(self):
        # Write running Version to file
        version_file = Path(f'{self.path}/running_version.txt')
        with open(version_file, "w", encoding='utf-8') as f:
            f.write(str(self.version))

    def load(self):
        """Load version for PB v7."""
        file = Path(f'{self.path}/config.json')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    v7_config = f.read()
                self._v7_config = json.loads(v7_config)
                self.version = self._v7_config["pbgui"]["version"]
                if self.name == self._v7_config["pbgui"]["enabled_on"]:
                    return True
                else:                        
                    self.name = self._v7_config["pbgui"]["enabled_on"]
                    return False
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

class PBRun():
    """PBRun links together PBRemote, PBGui and Passivbot, while being independant and can maintain passivbot working by itself.

    It does so with update_status_*.cmd, and activate_*.cmd. These files are created while using PBGui, and when PBRun receives activate_*.cmd, it creates the single of multi instances for passivbot, when it receives update_status_*.cmd, it inform on the status of this instances, so the bot specified in the status can start instances of passivbot.
    """
    def __init__(self):
        # self.run_instances = []
        self.run_multi = []
        self.run_single = []
        self.run_v7 = []
        self.index = 0
        self.pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        # Init activate_ts and pbname
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        if pb_config.has_option("main", "activate_ts"):
            self.activate_ts = int(pb_config.get("main", "activate_ts"))
        else:
            self.activate_ts = 0
        if pb_config.has_option("main", "activate_single_ts"):
            self.activate_single_ts = int(pb_config.get("main", "activate_single_ts"))
        else:
            self.activate_single_ts = 0
        if pb_config.has_option("main", "activate_v7_ts"):
            self.activate_v7_ts = int(pb_config.get("main", "activate_v7_ts"))
        else:
            self.activate_v7_ts = 0
        self.instances_status = InstancesStatus(f'{self.pbgdir}/data/cmd/status.json')
        self.instances_status.pbname = self.name
        self.instances_status.activate_ts = self.activate_ts
        self.instances_status_single = InstancesStatus(f'{self.pbgdir}/data/cmd/status_single.json')
        self.instances_status_single.pbname = self.name
        self.instances_status_single.activate_ts = self.activate_single_ts
        self.instances_status_v7 = InstancesStatus(f'{self.pbgdir}/data/cmd/status_v7.json')
        self.instances_status_v7.pbname = self.name
        self.instances_status_v7.activate_ts = self.activate_v7_ts
        # Init pbdirs
        self.pbdir = None
        self.pb7dir = None
        if pb_config.has_option("main", "pbdir"):
            self.pbdir = pb_config.get("main", "pbdir")
        if pb_config.has_option("main", "pb7dir"):
            self.pb7dir = pb_config.get("main", "pb7dir")
        if not any([self.pbdir, self.pb7dir]):
            if __name__ == '__main__':
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: No passivbot directory configured in pbgui.ini')
                exit(1)
            else:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: No passivbot directory configured in pbgui.ini')
                return
        # Print Warning if only pbdir or pb7dir configured
        if not self.pbdir:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Warning: No passivbot directory configured in pbgui.ini')
        if not self.pb7dir:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Warning: No passivbot v7 directory configured in pbgui.ini')
        # Init pbvenvs
        self.pbvenv = None
        self.pb7venv = None
        if pb_config.has_option("main", "pbvenv"):
            self.pbvenv = pb_config.get("main", "pbvenv")
        if pb_config.has_option("main", "pb7venv"):
            self.pb7venv = pb_config.get("main", "pb7venv")
        if not any([self.pbvenv, self.pb7venv]):
            if __name__ == '__main__':
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: No passivbot venv python interpreter configured in pbgui.ini')
                exit(1)
            else:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: No passivbot venv python interpreter configured in pbgui.ini')
                return
        # Print Warning if only pbvenv or pb7venv configured
        if not self.pbvenv:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Warning: No passivbot venv python interpreter configured in pbgui.ini')
        if not self.pb7venv:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Warning: No passivbot v7 venv python interpreter configured in pbgui.ini')
        # Init paths
        self.multi_path = f'{self.pbgdir}/data/multi'
        self.single_path = f'{self.pbgdir}/data/instances'
        self.v7_path = f'{self.pbgdir}/data/run_v7'
        self.cmd_path = f'{self.pbgdir}/data/cmd'
        if not Path(self.cmd_path).exists():
            Path(self.cmd_path).mkdir(parents=True)            
        # Init pid
        self.piddir = Path(f'{self.pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbrun.pid')
        self.my_pid = None

    def add_v7(self, run_v7: RunV7):
        if run_v7:
            for v7 in self.run_v7:
                if v7.path == run_v7.path:
                    v7.version = run_v7.version
                    return
            self.run_v7.append(run_v7)
    
    def remove_v7(self, run_v7: RunV7):
        if run_v7:
            for v7 in self.run_v7:
                if v7.path == run_v7.path:
                    self.run_v7.remove(v7)
                    return

    def add_multi(self, run_multi: RunMulti):
        if run_multi:
            for multi in self.run_multi:
                if multi.path == run_multi.path:
                    multi.version = run_multi.version
                    return
            self.run_multi.append(run_multi)

    def remove_multi(self, run_multi: RunMulti):
        if run_multi:
            for multi in self.run_multi:
                if multi.path == run_multi.path:
                    self.run_multi.remove(multi)
                    return

    def add_single(self, run_single: RunSingle):
        if run_single:
            for single in self.run_single:
                if single.path == run_single.path:
                    single.version = run_single.version
                    return
            self.run_single.append(run_single)

    def remove_single(self, run_single: RunSingle):
        if run_single:
            for single in self.run_single:
                if single.path == run_single.path:
                    self.run_single.remove(single)
                    return

    def find_running_version(self, path: str):
        version = 0
        version_file = Path(f'{path}/running_version.txt')
        if version_file.exists():
            with open(version_file, "r", encoding='utf-8') as f:
                version = f.read()
        return int(version)

    def update_status(self, status_file : str, rserver : str):
        """Function only called on PBRemote"""
        unique = str(uuid.uuid4())
        cfile = Path(f'{self.cmd_path}/update_status_{unique}.cmd')
        cfg = ({
            "rserver": rserver,
            "status_file": str(status_file)})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)

    def has_update_status(self):
        """Checks for new status, and update the status files accordingly.
        
        Checks for any file called update_status.cmd, and adds it to the status.json file already existant, required to run PB single and multi.
        """
        p = str(Path(f'{self.cmd_path}/update_status_*.cmd'))
        status_files = glob.glob(p)
        for cfile in status_files:
            cfile = Path(cfile)
            if cfile.exists():
                with open(cfile, "r", encoding='utf-8') as f:
                    cfg = json.load(f)
                    rserver = cfg["rserver"]
                    status_file = cfg["status_file"]
                    if status_file.split('/')[-1] == 'status.json':
                        self.update_from_status(status_file, rserver)
                    elif status_file.split('/')[-1] == 'status_single.json':
                        self.update_from_status_single(status_file, rserver)
                    elif status_file.split('/')[-1] == 'status_v7.json':
                        self.update_from_status_v7(status_file, rserver)
                cfile.unlink(missing_ok=True)

    def update_from_status_v7(self, status_file : str, rserver : str):
        """Updates the v7 status based on the provided status file.

        Notes:
            - Compares the new coming status timestamp with the current one.
            - Installs new v7 versions or instances if some.
            - Removes old *.json configuration files.
            - Removes instances not found in the new status.

        Args:
            status_file (str): Path to the status file.
            rserver (str): Name of the remote server.
        """
        new_status = InstancesStatus(status_file)
        if new_status.activate_ts > self.activate_v7_ts:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Activate: from {new_status.activate_pbname} Date: {datetime.fromtimestamp(new_status.activate_ts).isoformat(sep=" ", timespec="seconds")}')
            for instance in new_status:
                status = self.instances_status_v7.find_name(instance.name)
                if status is not None:
                    if instance.version > status.version:
                        # Install new v7 version
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New V7 Version {instance.name} Old: {status.version} New: {instance.version}')
                        # Remove old *.json configs
                        dest = f'{self.v7_path}/{instance.name}'
                        p = str(Path(f'{dest}/*'))
                        items = glob.glob(p)
                        for item in items:
                            if item.endswith('.json'):
                                Path(item).unlink(missing_ok=True)
                        src = f'{self.pbgdir}/data/remote/run_v7_{rserver}/{instance.name}'
                        dest = f'{self.v7_path}/{instance.name}'
                        if Path(src).exists():
                            copytree(src, dest, dirs_exist_ok=True)
                            self.watch_v7([f'{self.v7_path}/{instance.name}'])
                else:
                    # Install new v7 instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New V7 Instance {instance.name} from {rserver} Version: {instance.version}')
                    src = f'{self.pbgdir}/data/remote/run_v7_{rserver}/{instance.name}'
                    dest = f'{self.v7_path}/{instance.name}'
                    if Path(src).exists():
                        copytree(src, dest, dirs_exist_ok=True)
                        self.watch_v7([f'{self.v7_path}/{instance.name}'])
            remove_instances = []
            for instance in self.instances_status_v7:
                status = new_status.find_name(instance.name)
                if status is None:
                    # Remove v7 instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Remove: V7 Instance {instance.name}')
                    if instance.running:
                        for v7 in self.run_v7:
                            name = v7.path.split('/')[-1]
                            if name == instance.name:
                                v7.stop()
                                self.remove_v7(v7)
                    source = f'{self.v7_path}/{instance.name}'
                    if Path(source).exists():
                        # Backup v7 config
                        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                        destination = Path(f'{self.pbgdir}/data/backup/v7/{instance.name}/{date}')
                        if not destination.exists():
                            destination.mkdir(parents=True)
                        copytree(source, destination, dirs_exist_ok=True)
                        rmtree(source, ignore_errors=True)
                        remove_instances.append(instance)
            if remove_instances:
                for instance in remove_instances:
                    self.instances_status_v7.remove(instance)
                self.instances_status_v7.save()

    def update_from_status_single(self, status_file : str, rserver : str):
        """Updates the single status based on the provided status file.

        Notes:
            - Compares the new coming status timestamp with the current one.
            - Installs new single versions or instances if some.
            - Removes old *.json configuration files.
            - Removes instances not found in the new status.

        Args:
            status_file (str): Path to the status file.
            rserver (str): Name of the remote server.
        """
        new_status = InstancesStatus(status_file)
        if new_status.activate_ts > self.activate_single_ts:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Activate: from {new_status.activate_pbname} Date: {datetime.fromtimestamp(new_status.activate_ts).isoformat(sep=" ", timespec="seconds")}')
            for instance in new_status:
                status = self.instances_status_single.find_name(instance.name)
                if status is not None:
                    if instance.version > status.version:
                        # Install new single version
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New Single Version {instance.name} Old: {status.version} New: {instance.version}')
                        src = f'{self.pbgdir}/data/remote/instances_{rserver}/{instance.name}'
                        dest = f'{self.single_path}/{instance.name}'
                        if Path(src).exists():
                            copytree(src, dest, dirs_exist_ok=True)
                            self.watch_single([f'{self.single_path}/{instance.name}'])
                else:
                    # Install new single instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New Single Instance {instance.name} from {rserver} Version: {instance.version}')
                    src = f'{self.pbgdir}/data/remote/instances_{rserver}/{instance.name}'
                    dest = f'{self.single_path}/{instance.name}'
                    if Path(src).exists():
                        copytree(src, dest, dirs_exist_ok=True)
                        self.watch_single([f'{self.single_path}/{instance.name}'])
            remove_instances = []
            for instance in self.instances_status_single:
                status = new_status.find_name(instance.name)
                if status is None:
                    # Remove single instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Remove: Single Instance {instance.name}')
                    if instance.running:
                        for single in self.run_single:
                            name = single.path.split('/')[-1]
                            if name == instance.name:
                                single.stop()
                                self.remove_single(single)
                    source = f'{self.single_path}/{instance.name}'
                    if Path(source).exists():
                        # Backup single config
                        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                        destination = Path(f'{self.pbgdir}/data/backup/single/{instance.name}/{date}')
                        if not destination.exists():
                            destination.mkdir(parents=True)
                        copytree(source, destination, dirs_exist_ok=True)
                        rmtree(source, ignore_errors=True)
                        remove_instances.append(instance)
            if remove_instances:
                for instance in remove_instances:
                    self.instances_status_single.remove(instance)
                self.instances_status_single.save()

    def update_from_status(self, status_file : str, rserver : str):
        """Updates the multi status based on the provided status file.

        Notes:
            - Compares the new coming status timestamp with the current one.
            - Installs new multi versions or instances if some.
            - Removes old *.json configuration files.
            - Removes instances not found in the new status.

        Args:
            status_file (str): Path to the status file.
            rserver (str): Name of the remote server.
        """
        new_status = InstancesStatus(status_file)
        if new_status.activate_ts > self.activate_ts:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Activate: from {new_status.activate_pbname} Date: {datetime.fromtimestamp(new_status.activate_ts).isoformat(sep=" ", timespec="seconds")}')
            for instance in new_status:
                status = self.instances_status.find_name(instance.name)
                if status is not None:
                    if instance.version > status.version:
                        # Install new multi version
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New Multi Version {instance.name} Old: {status.version} New: {instance.version}')
                        # Remove old *.json configs
                        dest = f'{self.multi_path}/{instance.name}'
                        p = str(Path(f'{dest}/*'))
                        items = glob.glob(p)
                        for item in items:
                            if item.endswith('.json'):
                                Path(item).unlink(missing_ok=True)
                        src = f'{self.pbgdir}/data/remote/multi_{rserver}/{instance.name}'
                        dest = f'{self.multi_path}/{instance.name}'
                        if Path(src).exists():
                            copytree(src, dest, dirs_exist_ok=True)
                            self.watch_multi([f'{self.multi_path}/{instance.name}'])
                else:
                    # Install new multi instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New Multi Instance {instance.name} from {rserver} Version: {instance.version}')
                    src = f'{self.pbgdir}/data/remote/multi_{rserver}/{instance.name}'
                    dest = f'{self.multi_path}/{instance.name}'
                    if Path(src).exists():
                        copytree(src, dest, dirs_exist_ok=True)
                        self.watch_multi([f'{self.multi_path}/{instance.name}'])
            remove_instances = []
            for instance in self.instances_status:
                status = new_status.find_name(instance.name)
                if status is None:
                    # Remove multi instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Remove: Multi Instance {instance.name}')
                    if instance.running:
                        for multi in self.run_multi:
                            if multi.user == instance.name:
                                multi.stop()
                                self.remove_multi(multi)
                    source = f'{self.multi_path}/{instance.name}'
                    if Path(source).exists():
                        # Backup multi config
                        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                        destination = Path(f'{self.pbgdir}/data/backup/multi/{instance.name}/{date}')
                        if not destination.exists():
                            destination.mkdir(parents=True)
                        copytree(source, destination, dirs_exist_ok=True)
                        rmtree(source, ignore_errors=True)
                        remove_instances.append(instance)
            if remove_instances:
                for instance in remove_instances:
                    self.instances_status.remove(instance)
                self.instances_status.save()

    def activate(self, instance : str, multi : bool, version : int = None):
        unique = str(uuid.uuid4())
        cfile = Path(f'{self.cmd_path}/activate_{unique}.cmd')
        cfg = ({
            "instance": instance,
            "multi": multi,
            "version": version})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)

    def has_activate(self):
        """Checks for activation file

        This method scans for activation files (activate_*.cmd) in the cmd directory. If an activation file exists, it reads the new configuration to activate and start it as a single, multi or v7 config. Depending on the configuration, it either create a single, multi or v7 config using their respective watch function.
        """
        p = str(Path(f'{self.cmd_path}/activate_*.cmd'))
        activates = glob.glob(p)
        for cfile in activates:
            cfile = Path(cfile)
            if cfile.exists():
                with open(cfile, "r", encoding='utf-8') as f:
                    cfg = json.load(f)
                    instance = cfg["instance"]
                    multi = cfg["multi"]
                    if "version" in cfg:
                        version = cfg["version"]
                    else:
                        version = None
                    if version == "7":
                        self.update_activate_v7()
                        self.watch_v7([f'{self.v7_path}/{instance}'])
                    elif multi:
                        self.update_activate()
                        self.watch_multi([f'{self.multi_path}/{instance}'])
                    else:
                        self.update_activate_single()
                        self.watch_single([f'{self.single_path}/{instance}'])
                cfile.unlink(missing_ok=True)
    
    def update_activate_v7(self):
        self.activate_v7_ts = int(datetime.now().timestamp())
        self.instances_status_v7.activate_ts = self.activate_v7_ts
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pb_config.set("main", "activate_v7_ts", str(self.activate_v7_ts))
        with open('pbgui.ini', 'w') as pbgui_configfile:
            pb_config.write(pbgui_configfile)

    def update_activate(self):
        self.activate_ts = int(datetime.now().timestamp())
        self.instances_status.activate_ts = self.activate_ts
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pb_config.set("main", "activate_ts", str(self.activate_ts))
        with open('pbgui.ini', 'w') as pbgui_configfile:
            pb_config.write(pbgui_configfile)

    def update_activate_single(self):
        self.activate_single_ts = int(datetime.now().timestamp())
        self.instances_status_single.activate_ts = self.activate_single_ts
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pb_config.set("main", "activate_single_ts", str(self.activate_single_ts))
        with open('pbgui.ini', 'w') as pbgui_configfile:
            pb_config.write(pbgui_configfile)

    def watch_v7(self, v7_instances : list = None):
        """Create or delete v7 instances and activate them or not depending on their status.

        Args:
            v7_instances (list, optional): List of v7-instance paths. Defaults to None.
        """
        if not v7_instances:
            p = str(Path(f'{self.v7_path}/*'))
            v7_instances = glob.glob(p)
            # Remove all existing instances from status
            self.instances_status_v7.instances = []
        for v7_instance in v7_instances:
            file = Path(f'{v7_instance}/config.json')
            if file.exists():
                run_v7 = RunV7()
                status = InstanceStatus()
                run_v7.path = v7_instance
                run_v7.user = v7_instance.split('/')[-1]
                status.name = run_v7.user
                run_v7.name = self.name
                run_v7.pbdir = self.pb7dir
                run_v7.pbvenv = self.pb7venv
                run_v7.pbgdir = self.pbgdir
                if run_v7.load():
                    if run_v7.is_running():
                        running_version = self.find_running_version(v7_instance)
                        if running_version < run_v7.version:
                            run_v7.stop()
                            run_v7.create_v7_running_version()
                            run_v7.start()
                    else:
                        run_v7.create_v7_running_version()
                        run_v7.start()
                    self.add_v7(run_v7)
                    status.running = True
                else:
                    self.remove_v7(run_v7)
                    status.running = False
                    run_v7.stop()
                status.version = run_v7.version
                status.enabled_on = run_v7.name
                self.instances_status_v7.add(status)
        # Remove non existing instances from status
        for instance in self.instances_status_v7:
            instance_path = f'{self.pbgdir}/data/run_v7/{instance.name}'
            if not Path(instance_path).exists():
                self.instances_status_v7.remove(instance)
        self.instances_status_v7.save()

    def watch_single(self, single_instances : list = None):
        """Create or delete single instances and activate them or not depending on their status.

        Args:
            single_instances (list, optional): List of single-instance paths. Defaults to None.
        """
        if not single_instances:
            p = str(Path(f'{self.single_path}/*'))
            single_instances = glob.glob(p)
            # Remove all existing instances from status
            self.instances_status_single.instances = []
        for single_instance in single_instances:
            file = Path(f'{single_instance}/instance.cfg')
            if file.exists():
                run_single = RunSingle()
                status = InstanceStatus()
                run_single.path = single_instance
                run_single.name = self.name
                run_single.pbdir = self.pbdir
                run_single.pbvenv = self.pbvenv
                run_single.pbgdir = self.pbgdir
                if run_single.load():
                    if run_single.is_running():
                        running_version = self.find_running_version(single_instance)
                        if running_version < run_single.version:
                            run_single.stop()
                            run_single.create_parameters()
                            run_single.start()
                    else:
                        run_single.create_parameters()
                        run_single.start()
                    self.add_single(run_single)
                    status.running = True
                else:
                    self.remove_single(run_single)
                    status.running = False
                    run_single.stop()
                status.name = single_instance.split('/')[-1]
                status.multi = run_single.multi
                status.version = run_single.version
                status.enabled_on = run_single.name
                self.instances_status_single.add(status)
        # Remove non existing instances from status
        for instance in self.instances_status_single:
            instance_path = f'{self.pbgdir}/data/instances/{instance.name}'
            if not Path(instance_path).exists():
                self.instances_status_single.remove(instance)
        self.instances_status_single.save()

    def watch_multi(self, multi_instances : list = None):
        """Create or delete multi instances and activate them or not depending on their status.

        Args:
            multi_instance (list, optional): List of muilti-instance paths. Defaults to None.
        """
        if not multi_instances:
            p = str(Path(f'{self.multi_path}/*'))
            multi_instances = glob.glob(p)
        for multi_instance in multi_instances:
            file = Path(f'{multi_instance}/multi.hjson')
            if file.exists():
                run_multi = RunMulti()
                status = InstanceStatus()
                status.multi = True
                run_multi.path = multi_instance
                run_multi.user = multi_instance.split('/')[-1]
                status.name = run_multi.user
                run_multi.name = self.name
                run_multi.pbdir = self.pbdir
                run_multi.pbvenv = self.pbvenv
                run_multi.pbgdir = self.pbgdir
                if run_multi.load():
                    if run_multi.is_running():
                        running_version = self.find_running_version(multi_instance)
                        if running_version < run_multi.version:
                            run_multi.stop()
                            run_multi.create_multi_hjson()
                            run_multi.start()
                    else:
                        run_multi.create_multi_hjson()
                        run_multi.start()
                    self.add_multi(run_multi)
                    status.running = True
                else:
                    self.remove_multi(run_multi)
                    status.running = False
                    run_multi.stop()
                status.version = run_multi.version
                status.enabled_on = run_multi.name
                self.instances_status.add(status)
        # Remove non existing instances from status
        for instance in self.instances_status:
            instance_path = f'{self.pbgdir}/data/multi/{instance.name}'
            if not Path(instance_path).exists():
                self.instances_status.remove(instance)
        self.instances_status.save()

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBRun.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBRun')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBRun')
            psutil.Process(self.my_pid).kill()

    def restart_pbrun(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbrun.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False
    
    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        """Saves the process ID into /data/pid/pbrun.pid."""
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))


def main():
    """
    Main function of PBRun, responsible for starting and logging passivbot instances.

    ### Usage : 
    - Run PBRun and save its process ID to pbrun.pid.
    - Logs in pbgui/data/logs/PBRun.log and creates a .old if the file is too heavy.
    - Create and monitor single, multi and instances of passivbot. (Instances will be deleted in future versions)
    """
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBRun.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBRun')
    run = PBRun()
    if run.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBRun already started')
        exit(1)
    run.save_pid()
    run.watch_v7()
    run.watch_multi()
    run.watch_single()
    count = 0
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 1048576:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            run.has_activate()
            run.has_update_status()
            for run_v7 in run.run_v7:
                run_v7.watch()
                run_v7.watch_log()
            for run_multi in run.run_multi:
                run_multi.watch()
            for run_single in run.run_single:
                run_single.watch()
            if count%2 == 0:
                for run_v7 in run.run_v7:
                    run_v7.clean_log()
                for run_multi in run.run_multi:
                    run_multi.clean_log()
                for run_single in run.run_single:
                    run_single.clean_log()
            sleep(5)
            count += 1
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()

if __name__ == '__main__':
    main()