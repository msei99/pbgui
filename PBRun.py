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
from time import sleep
import glob
import json
import hjson
from io import TextIOWrapper
from datetime import datetime
import platform
from shutil import copy, copytree, rmtree
import os
import traceback
import uuid
from Status import InstanceStatus, InstancesStatus

# class RunInstance():
#     def __init__(self):
#         self._enabled = False
#         self._multi = False
#         self.enabled_on = "disabled"
#         self._user = None
#         self._symbol = None
#         self._parameter = None
#         self._path = None
    
#     @property
#     def enabled(self): return self._enabled
#     @property
#     def multi(self): return self._multi
#     @property
#     def user(self): return self._user
#     @property
#     def symbol(self): return self._symbol
#     @property
#     def parameter(self): return self._parameter
#     @property
#     def path(self): return self._path

#     @enabled.setter
#     def enabled(self, new_enabled):
#         if self._enabled != new_enabled:
#             self._enabled = new_enabled
#     @multi.setter
#     def multi(self, new_multi):
#         if self._multi != new_multi:
#             self._multi = new_multi
#     @user.setter
#     def user(self, new_user):
#         if self._user != new_user:
#             self._user = new_user
#     @symbol.setter
#     def symbol(self, new_symbol):
#         if self._symbol != new_symbol:
#             self._symbol = new_symbol
#     @parameter.setter
#     def parameter(self, new_parameter):
#         if self._parameter != new_parameter:
#             self._parameter = new_parameter
#     @path.setter
#     def path(self, new_path):
#         if self._path != new_path:
#             self._path = new_path


#     def watch(self):
#         if not self.is_running():
#             self.start()

#     def is_running(self):
#         if self.pid():
#             return True
#         return False

#     def pid(self):
#         for process in psutil.process_iter():
#             try:
#                 cmdline = process.cmdline()
#             except psutil.NoSuchProcess:
#                 pass
#             except psutil.AccessDenied:
#                 pass
#             if self.user in cmdline and self.symbol in cmdline and any("passivbot.py" in sub for sub in cmdline):
#                 return process

#     def stop(self):
#         if self.is_running():
#             print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: Old Instance {self.user} {self.symbol}')
#             self.pid().kill()

#     def start(self):
#         if not self.is_running():
#             pb_config = configparser.ConfigParser()
#             pb_config.read('pbgui.ini')
#             if pb_config.has_option("main", "pbdir"):
#                 pbdir = pb_config.get("main", "pbdir")
#                 config = PurePath(f'{self.path}/config.json')
#                 cmd = [sys.executable, '-u', PurePath(f'{pbdir}/passivbot.py')]
#                 cmd_end = f'{self.parameter} {self.user} {self.symbol} '.lstrip(' ')
#                 cmd.extend(shlex.split(cmd_end))
#                 cmd.extend([config])
#                 logfile = Path(f'{self.path}/passivbot.log')
#                 log = open(logfile,"ab")
#                 if platform.system() == "Windows":
#                     creationflags = subprocess.DETACHED_PROCESS
#                     creationflags |= subprocess.CREATE_NO_WINDOW
#                     subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pbdir, text=True, creationflags=creationflags)
#                 else:
#                     subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pbdir, text=True, start_new_session=True)
#                 print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: Old Instance {cmd_end}')

#     def clean_log(self):
#         logfile = Path(f'{self.path}/passivbot.log')
#         if logfile.exists():
#             if logfile.stat().st_size >= 10485760:
#                 logfile_old = Path(f'{str(logfile)}.old')
#                 copy(logfile,logfile_old)
#                 with open(logfile,'r+') as file:
#                     file.truncate()

#     def load(self):
#         """Load the instance file with passivbot's parameters in self"""
#         file = Path(f'{self.path}/instance.cfg')
#         with open(file, "r", encoding='utf-8') as f:
#             instance_cfg = json.load(f)
#             if "_enabled" in instance_cfg:
#                 self.enabled = instance_cfg["_enabled"]
#             if "_multi" in instance_cfg:
#                 self.multi = instance_cfg["_multi"]
#             if "_enabled_on" in instance_cfg:
#                 self.enabled_on = instance_cfg["_enabled_on"]
#             self.user = instance_cfg["_user"]
#             self.symbol = instance_cfg["_symbol"]
#             self.parameter = ""
#             if instance_cfg["_long_mode"] == "graceful_stop":
#                 self.parameter = (self.parameter + f' -lm gs').lstrip(' ')
#             if instance_cfg["_long_mode"] == "panic":
#                 self.parameter = (self.parameter + f' -lm p').lstrip(' ')
#             if instance_cfg["_long_mode"] == "tp_only":
#                 self.parameter = (self.parameter + f' -lm t').lstrip(' ')
#             if instance_cfg["_short_mode"] == "graceful_stop":
#                 self.parameter = (self.parameter + f' -sm gs').lstrip(' ')
#             if instance_cfg["_short_mode"] == "panic":
#                 self.parameter = (self.parameter + f' -sm p').lstrip(' ')
#             if instance_cfg["_short_mode"] == "tp_only":
#                 self.parameter = (self.parameter + f' -sm t').lstrip(' ')
#             if instance_cfg["_market_type"] != "swap":
#                 self.parameter = (self.parameter + f' -m spot').lstrip(' ')
#             if not instance_cfg["_ohlcv"]:
#                 self.parameter = (self.parameter + f' -oh n').lstrip(' ')
#             if instance_cfg["_co"] != -1:
#                 self.parameter = (self.parameter + f' -co {instance_cfg["_co"]}').lstrip(' ')
#             if instance_cfg["_leverage"] != 7:
#                 self.parameter = (self.parameter + f' -lev {instance_cfg["_leverage"]}').lstrip(' ')
#             if instance_cfg["_assigned_balance"] != 0:
#                 self.parameter = (self.parameter + f' -ab {instance_cfg["_assigned_balance"]}').lstrip(' ')
#             if instance_cfg["_price_distance_threshold"] != 0.5:
#                 self.parameter = (self.parameter + f' -pt {instance_cfg["_price_distance_threshold"]}').lstrip(' ')
#             if instance_cfg["_price_precision"] != 0.0:
#                 self.parameter = (self.parameter + f' -pp {instance_cfg["_price_precision"]}').lstrip(' ')
#             if instance_cfg["_price_step"] != 0.0:
#                 self.parameter = (self.parameter + f' -ps {instance_cfg["_price_step"]}').lstrip(' ')

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
            cmd = [sys.executable, '-u', PurePath(f'{self.pbdir}/passivbot.py')]
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
            config = PurePath(f'{self.path}/config.json')
            cmd = [sys.executable, '-u', PurePath(f'{self.pbdir}/passivbot_multi.py'), PurePath(f'{self.path}/multi_run.hjson')]
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

class PBRun():
    """PBRun links together PBRemote, PBGui and Passivbot, while being independant and can maintain passivbot working by itself.

    It does so with update_status_*.cmd, and activate_*.cmd. These files are created while using PBGui, and when PBRun receives activate_*.cmd, it creates the single of multi instances for passivbot, when it receives update_status_*.cmd, it inform on the status of this instances, so the bot specified in the status can start instances of passivbot.
    """
    def __init__(self):
        # self.run_instances = []
        self.run_multi = []
        self.run_single = []
        self.index = 0
        self.pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
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
        self.instances_status = InstancesStatus(f'{self.pbgdir}/data/cmd/status.json')
        self.instances_status.pbname = self.name
        self.instances_status.activate_ts = self.activate_ts
        self.instances_status_single = InstancesStatus(f'{self.pbgdir}/data/cmd/status_single.json')
        self.instances_status_single.pbname = self.name
        self.instances_status_single.activate_ts = self.activate_single_ts
        if pb_config.has_option("main", "pbdir"):
            self.pbdir = pb_config.get("main", "pbdir")
        else:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: No passivbot directory configured in pbgui.ini')
            exit(1)
        self.instances_path = f'{self.pbgdir}/data/instances'
        self.multi_path = f'{self.pbgdir}/data/multi'
        self.single_path = f'{self.pbgdir}/data/instances'
        self.cmd_path = f'{self.pbgdir}/data/cmd'
        if not Path(self.cmd_path).exists():
            Path(self.cmd_path).mkdir(parents=True)            
        self.piddir = Path(f'{self.pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbrun.pid')
        self.my_pid = None

    # def __iter__(self):
    #     return iter(self.run_instances)

    # def __next__(self):
    #     if self.index > len(self.run_instances):
    #         raise StopIteration
    #     self.index += 1
    #     return next(self)

    # def add(self, run_instance: RunInstance): # Deprecated (Instances)
    #     if run_instance:
    #         if run_instance.path:
    #             for instance in self.run_instances:
    #                 if instance.path == run_instance.path:
    #                     return
    #             self.run_instances.append(run_instance)

    # def remove(self, run_instance: RunInstance): # Deprecated (Instances)
    #     if run_instance:
    #         self.run_instances.remove(run_instance)

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

    # def stop_instance(self, instance):
    #     self.change_enabled(instance, False)
    #     ipath = f'{self.instances_path}/{instance}'
    #     self.update(ipath, False)   # No function ? Deprecated?

    # def disable_instance(self, instance): # Can be removed with the removal of Instances from the code.
    #     self.change_enabled(instance, False)

    # def change_enabled(self, instance : str, enabled : bool):
    #     # May be useless if disable_instance and stop_instance are.
    #     ipath = f'{self.instances_path}/{instance}'
    #     ifile = Path(f'{ipath}/instance.cfg')
    #     with open(ifile, "r", encoding='utf-8') as f:
    #         inst = json.load(f)
    #         inst["_enabled"] = enabled
    #         f.close()
    #     with open(ifile, "w", encoding='utf-8') as f:
    #         json.dump(inst, f, indent=4)

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
                cfile.unlink(missing_ok=True)

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

    def activate(self, instance : str, multi : bool):
        unique = str(uuid.uuid4())
        cfile = Path(f'{self.cmd_path}/activate_{unique}.cmd')
        cfg = ({
            "instance": instance,
            "multi": multi})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)

    def has_activate(self):
        """Checks for activation file

        This method scans for activation files (activate_*.cmd) in the cmd directory. If an activation file exists, it reads the new configuration to activate and start it as a single or multi config. Depending on the configuration, it either create a single or multi config using their respective watch function.
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
                    if multi:
                        self.update_activate()
                        self.watch_multi([f'{self.multi_path}/{instance}'])
                    else:
                        self.update_activate_single()
                        self.watch_single([f'{self.single_path}/{instance}'])
                cfile.unlink(missing_ok=True)
    
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

    # def load(self, instance: str): # Deprecated (Instances)
    #     file = Path(f'{instance}/instance.cfg')
    #     if file.exists():
    #         run_instance = RunInstance()
    #         run_instance.path = instance
    #         run_instance.load()
    #         if run_instance.enabled and not run_instance.multi:
    #             self.add(run_instance)
    #         elif run_instance.enabled_on != self.name:
    #             run_instance.stop()

    # def load_all(self): # Deprecated (Instances)
    #     self.run_instances = []
    #     p = str(Path(f'{self.instances_path}/*'))
    #     instances = glob.glob(p)
    #     for instance in instances:
    #         self.load(instance)

    # # can be removed on a future version
    # def stop_old_instance(self, path: str):
    #     for instance in self.run_instances:
    #         if instance.path == path:
    #             instance.stop()
    #             self.disable_instance(instance.path.split('/')[-1])
    #             self.remove(instance)
    
    # # can be removed on a future version
    # def is_old_instance(self, path: str):
    #     for instance in self.run_instances:
    #         if instance.path == path:
    #             return True
    #     return False

    def watch_single(self, single_instances : list = None):
        """Create of delete single instances and activate them or not depending on their status.

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
                run_single.pbgdir = self.pbgdir
                if run_single.load():
                    # # Stop old instance if we start them as a new single instance (can be removed in a future version)
                    # if run_single.name != "disabled":
                    #     self.stop_old_instance(single_instance)
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
                    # # Stop old instance if started as a new single instance somewhere else
                    # if run_single.name != "disabled":
                    #     self.stop_old_instance(single_instance)
                    self.remove_single(run_single)
                    status.running = False
                    # if not self.is_old_instance(single_instance):
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
        """Create of delete multi instances and activate them or not depending on their status.

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
    # run.load_all() # Deprecated (Instances)
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
            # for run_instance in run: # Deprecated (Instances)
            #     run_instance.watch()
            for run_multi in run.run_multi:
                run_multi.watch()
            for run_single in run.run_single:
                run_single.watch()
            if count%2 == 0:
                # for run_instance in run:
                #     run_instance.clean_log()
                for run_multi in run.run_multi:
                    run_multi.clean_log()
                for run_single in run.run_single:
                    run_single.clean_log()
            sleep(5)
            count += 1
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()
#            exit()

if __name__ == '__main__':
    main()