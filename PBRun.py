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
import shutil
from io import TextIOWrapper
from datetime import datetime
import platform
from shutil import copy
import os
import traceback
import uuid
from Status import InstanceStatus, InstancesStatus

class RunInstance():
    def __init__(self):
        self._enabled = False
        self._multi = False
        self.enabled_on = "disabled"
        self._user = None
        self._symbol = None
        self._parameter = None
        self._path = None
    
    @property
    def enabled(self): return self._enabled
    @property
    def multi(self): return self._multi
    @property
    def user(self): return self._user
    @property
    def symbol(self): return self._symbol
    @property
    def parameter(self): return self._parameter
    @property
    def path(self): return self._path

    @enabled.setter
    def enabled(self, new_enabled):
        if self._enabled != new_enabled:
            self._enabled = new_enabled
    @multi.setter
    def multi(self, new_multi):
        if self._multi != new_multi:
            self._multi = new_multi
    @user.setter
    def user(self, new_user):
        if self._user != new_user:
            self._user = new_user
    @symbol.setter
    def symbol(self, new_symbol):
        if self._symbol != new_symbol:
            self._symbol = new_symbol
    @parameter.setter
    def parameter(self, new_parameter):
        if self._parameter != new_parameter:
            self._parameter = new_parameter
    @path.setter
    def path(self, new_path):
        if self._path != new_path:
            self._path = new_path


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
            if self.user in cmdline and self.symbol in cmdline and any("passivbot.py" in sub for sub in cmdline):
                return process

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: Old Instance {self.user} {self.symbol}')
            self.pid().kill()

    def start(self):
        if not self.is_running():
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if pb_config.has_option("main", "pbdir"):
                pbdir = pb_config.get("main", "pbdir")
                config = PurePath(f'{self.path}/config.json')
                cmd = [sys.executable, '-u', PurePath(f'{pbdir}/passivbot.py')]
                cmd_end = f'{self.parameter} {self.user} {self.symbol} '.lstrip(' ')
                cmd.extend(shlex.split(cmd_end))
                cmd.extend([config])
                logfile = Path(f'{self.path}/passivbot.log')
                log = open(logfile,"ab")
                if platform.system() == "Windows":
                    creationflags = subprocess.DETACHED_PROCESS
                    creationflags |= subprocess.CREATE_NO_WINDOW
                    subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pbdir, text=True, creationflags=creationflags)
                else:
                    subprocess.Popen(cmd, stdout=log, stderr=log, cwd=pbdir, text=True, start_new_session=True)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: Old Instance {cmd_end}')

    def clean_log(self):
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile_old = Path(f'{str(logfile)}.old')
                copy(logfile,logfile_old)
                with open(logfile,'r+') as file:
                    file.truncate()

    def load(self):
        file = Path(f'{self.path}/instance.cfg')
        with open(file, "r", encoding='utf-8') as f:
            instance_cfg = json.load(f)
            if "_enabled" in instance_cfg:
                self.enabled = instance_cfg["_enabled"]
            if "_multi" in instance_cfg:
                self.multi = instance_cfg["_multi"]
            if "_enabled_on" in instance_cfg:
                self.enabled_on = instance_cfg["_enabled_on"]
            self.user = instance_cfg["_user"]
            self.symbol = instance_cfg["_symbol"]
            self.parameter = ""
            if instance_cfg["_long_mode"] == "graceful_stop":
                self.parameter = (self.parameter + f' -lm gs').lstrip(' ')
            if instance_cfg["_long_mode"] == "panic":
                self.parameter = (self.parameter + f' -lm p').lstrip(' ')
            if instance_cfg["_long_mode"] == "tp_only":
                self.parameter = (self.parameter + f' -lm t').lstrip(' ')
            if instance_cfg["_short_mode"] == "graceful_stop":
                self.parameter = (self.parameter + f' -sm gs').lstrip(' ')
            if instance_cfg["_short_mode"] == "panic":
                self.parameter = (self.parameter + f' -sm p').lstrip(' ')
            if instance_cfg["_short_mode"] == "tp_only":
                self.parameter = (self.parameter + f' -sm t').lstrip(' ')
            if instance_cfg["_market_type"] != "swap":
                self.parameter = (self.parameter + f' -m spot').lstrip(' ')
            if not instance_cfg["_ohlcv"]:
                self.parameter = (self.parameter + f' -oh n').lstrip(' ')
            if instance_cfg["_co"] != -1:
                self.parameter = (self.parameter + f' -co {instance_cfg["_co"]}').lstrip(' ')
            if instance_cfg["_leverage"] != 7:
                self.parameter = (self.parameter + f' -lev {instance_cfg["_leverage"]}').lstrip(' ')
            if instance_cfg["_assigned_balance"] != 0:
                self.parameter = (self.parameter + f' -ab {instance_cfg["_assigned_balance"]}').lstrip(' ')
            if instance_cfg["_price_distance_threshold"] != 0.5:
                self.parameter = (self.parameter + f' -pt {instance_cfg["_price_distance_threshold"]}').lstrip(' ')
            if instance_cfg["_price_precision"] != 0.0:
                self.parameter = (self.parameter + f' -pp {instance_cfg["_price_precision"]}').lstrip(' ')
            if instance_cfg["_price_step"] != 0.0:
                self.parameter = (self.parameter + f' -ps {instance_cfg["_price_step"]}').lstrip(' ')

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
        self._multi_config["default_config_path"] = f'{self.pbdir}/configs/live/recursive_grid_mode.example.json'
        run_config = hjson.dumps(self._multi_config)
        config_file = Path(f'{self.path}/multi_run.hjson')
        with open(config_file, "w", encoding='utf-8') as f:
            f.write(run_config)

    def load(self):
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
    def __init__(self):
        self.run_instances = []
        self.run_multi = []
        self.run_single = []
        self.index = 0
        self.pbgdir = Path.cwd()
        self.pb_config = configparser.ConfigParser()
        self.pb_config.read('pbgui.ini')
        if self.pb_config.has_option("main", "pbname"):
            self.name = self.pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        if self.pb_config.has_option("main", "activate_ts"):
            self.activate_ts = int(self.pb_config.get("main", "activate_ts"))
        else:
            self.activate_ts = 0
        if self.pb_config.has_option("main", "activate_single_ts"):
            self.activate_single_ts = int(self.pb_config.get("main", "activate_single_ts"))
        else:
            self.activate_single_ts = 0
        self.instances_status = InstancesStatus(f'{self.pbgdir}/data/cmd/status.json')
        self.instances_status.pbname = self.name
        self.instances_status.activate_ts = self.activate_ts
        self.instances_status_single = InstancesStatus(f'{self.pbgdir}/data/cmd/status_single.json')
        self.instances_status_single.pbname = self.name
        self.instances_status_single.activate_ts = self.activate_single_ts
        if self.pb_config.has_option("main", "pbdir"):
            self.pbdir = self.pb_config.get("main", "pbdir")
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

    def __iter__(self):
        return iter(self.run_instances)

    def __next__(self):
        if self.index > len(self.run_instances):
            raise StopIteration
        self.index += 1
        return next(self)

    def add(self, run_instance: RunInstance):
        if run_instance:
            if run_instance.path:
                for instance in self.run_instances:
                    if instance.path == run_instance.path:
                        return
                self.run_instances.append(run_instance)

    def remove(self, run_instance: RunInstance):
        if run_instance:
            self.run_instances.remove(run_instance)

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

    def stop_instance(self, instance):
        self.change_enabled(instance, False)
        ipath = f'{self.instances_path}/{instance}'
        self.update(ipath, False)

    def disable_instance(self, instance):
        self.change_enabled(instance, False)

    def change_enabled(self, instance : str, enabled : bool):
        ipath = f'{self.instances_path}/{instance}'
        ifile = Path(f'{ipath}/instance.cfg')
        with open(ifile, "r", encoding='utf-8') as f:
            inst = json.load(f)
            inst["_enabled"] = enabled
            f.close()
        with open(ifile, "w", encoding='utf-8') as f:
            json.dump(inst, f, indent=4)

    def update_status(self, status_file : str, rserver : str):
        unique = str(uuid.uuid4())
        cfile = Path(f'{self.cmd_path}/update_status_{unique}.cmd')
        cfg = ({
            "rserver": rserver,
            "status_file": str(status_file)})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)

    def has_update_status(self):
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
                            shutil.copytree(src, dest, dirs_exist_ok=True)
                            self.watch_single([f'{self.single_path}/{instance.name}'])
                else:
                    # Install new single instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New Single Instance {instance.name} from {rserver} Version: {instance.version}')
                    src = f'{self.pbgdir}/data/remote/instances_{rserver}/{instance.name}'
                    dest = f'{self.single_path}/{instance.name}'
                    if Path(src).exists():
                        shutil.copytree(src, dest, dirs_exist_ok=True)
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
                        shutil.copytree(source, destination, dirs_exist_ok=True)
                        shutil.rmtree(source, ignore_errors=True)
                        remove_instances.append(instance)
            if remove_instances:
                for instance in remove_instances:
                    self.instances_status_single.remove(instance)
                self.instances_status_single.save()

    def update_from_status(self, status_file : str, rserver : str):
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
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                        self.watch_multi([f'{self.multi_path}/{instance.name}'])
                else:
                    # Install new multi instance
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install: New Multi Instance {instance.name} from {rserver} Version: {instance.version}')
                    src = f'{self.pbgdir}/data/remote/multi_{rserver}/{instance.name}'
                    dest = f'{self.multi_path}/{instance.name}'
                    shutil.copytree(src, dest, dirs_exist_ok=True)
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
                        destination = Path(f'{self.pbgdir}/data/backup/mult/{instance.name}/{date}')
                        if not destination.exists():
                            destination.mkdir(parents=True)
                        shutil.copytree(source, destination, dirs_exist_ok=True)
                        shutil.rmtree(source, ignore_errors=True)
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
                        self.watch_multi([f'{self.multi_path}/{instance}'])
                        self.update_activate()
                    else:
                        self.watch_single([f'{self.single_path}/{instance}'])
                        self.update_activate_single()
                cfile.unlink(missing_ok=True)
    
    def update_activate(self):
        self.activate_ts = int(datetime.now().timestamp())
        self.instances_status.activate_ts = self.activate_ts
        self.pb_config.set("main", "activate_ts", str(self.activate_ts))
        with open('pbgui.ini', 'w') as pbgui_configfile:
            self.pb_config.write(pbgui_configfile)

    def update_activate_single(self):
        self.activate_single_ts = int(datetime.now().timestamp())
        self.instances_status_single.activate_ts = self.activate_single_ts
        self.pb_config.set("main", "activate_single_ts", str(self.activate_single_ts))
        with open('pbgui.ini', 'w') as pbgui_configfile:
            self.pb_config.write(pbgui_configfile)

    def load(self, instance: str):
        file = Path(f'{instance}/instance.cfg')
        if file.exists():
            run_instance = RunInstance()
            run_instance.path = instance
            run_instance.load()
            if run_instance.enabled and not run_instance.multi:
                self.add(run_instance)
            elif run_instance.enabled_on != self.name:
                run_instance.stop()

    def load_all(self):
        self.run_instances = []
        p = str(Path(f'{self.instances_path}/*'))
        instances = glob.glob(p)
        for instance in instances:
            self.load(instance)

    # can be removed on a future version
    def stop_old_instance(self, path: str):
        for instance in self.run_instances:
            if instance.path == path:
                instance.stop()
                self.disable_instance(instance.path.split('/')[-1])
                self.remove(instance)
    
    def watch_single(self, single_instances : list = None):
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
                    # Stop old instance if we start them as a new single instance (can be removed in a future version)
                    if run_single.name != "disabled":
                        self.stop_old_instance(single_instance)
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
        self.instances_status_single.save()

    def watch_multi(self, multi_instances : list = None):
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
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))


def main():
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
    run.load_all()
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
            for run_instance in run:
                run_instance.watch()
            for run_multi in run.run_multi:
                run_multi.watch()
            for run_single in run.run_single:
                run_single.watch()
            if count%2 == 0:
                for run_instance in run:
                    run_instance.clean_log()
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