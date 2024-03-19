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

class RunInstance():
    def __init__(self):
        self._enabled = None
        self._multi = False
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
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: {self.user} {self.symbol}')
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
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: {cmd_end}')

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
            self.enabled = instance_cfg["_enabled"]
            if "_multi" in instance_cfg:
                self.multi = instance_cfg["_multi"]
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
                return False
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

class InstanceStatus():
    def __init__(self):
        self.version = None
        self.name = None
        self.multi = None
        self.running = None
        self.enabled_on = None

class PBRun():
    def __init__(self):
        self.run_instances = []
        self.run_multi = []
        self.all_status = []
        self.index = 0
        self.pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        if pb_config.has_option("main", "pbdir"):
            self.pbdir = pb_config.get("main", "pbdir")
        else:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: No passivbot directory configured in pbgui.ini')
            exit(1)
        self.instances_path = f'{self.pbgdir}/data/instances'
        self.multi_path = f'{self.pbgdir}/data/multi'
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

    def update_status(self, status: InstanceStatus):
        if status:
            for index, instance in enumerate(self.all_status):
                if instance.name == status.name:
                    self.all_status[index] = status
                    return
            self.all_status.append(status)
    
    def find_running_version(self, path: str):
        version = 0
        version_file = Path(f'{path}/running_version.txt')
        if version_file.exists():
            with open(version_file, "r", encoding='utf-8') as f:
                version = f.read()
        return int(version)

    def start_instance(self, instance):
        self.change_enabled(instance, True)
        ipath = f'{self.instances_path}/{instance}'
        self.update(ipath, True)

    def stop_instance(self, instance):
        self.change_enabled(instance, False)
        ipath = f'{self.instances_path}/{instance}'
        self.update(ipath, False)

    def restart_instance(self, instance):
        user = "_".join(instance.split("_")[0:-2])
        symbol = instance.split("_")[-2]
        self.restart(user, symbol)

    def disable_instance(self, instance):
        self.change_enabled(instance, False)

    def enable_instance(self, instance):
        self.change_enabled(instance, True)

    def is_enabled_instance(self, instance):
        ipath = f'{self.instances_path}/{instance}'
        ifile = Path(f'{ipath}/instance.cfg')
        if ifile.exists():
            with open(ifile, "r", encoding='utf-8') as f:
                inst = json.load(f)
            if inst["_enabled"]:
                return True
        return False

    def change_enabled(self, instance : str, enabled : bool):
        ipath = f'{self.instances_path}/{instance}'
        ifile = Path(f'{ipath}/instance.cfg')
        with open(ifile, "r", encoding='utf-8') as f:
            inst = json.load(f)
            inst["_enabled"] = enabled
            f.close()
        with open(ifile, "w", encoding='utf-8') as f:
            json.dump(inst, f, indent=4)

    def restart(self, user : str, symbol : str):
        cfile = Path(f'{self.cmd_path}/restart.cmd')
        cfg = ({
            "user": user,
            "symbol": symbol})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)

    def has_restart(self):
        cfile = Path(f'{self.cmd_path}/restart.cmd')
        if cfile.exists():
            with open(cfile, "r", encoding='utf-8') as f:
                cfg = json.load(f)
                for instance in self.run_instances:
                    if instance.user == cfg["user"] and instance.symbol == cfg["symbol"]:
                        instance.stop()
                        instance.load()
                        instance.start()
            cfile.unlink(missing_ok=True)
    
    def update(self, instance_path : str, enabled : bool):
        cfile = Path(f'{self.cmd_path}/update.cmd')
        cfg = ({
            "path": str(PurePath(instance_path)),
            "enabled": enabled})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)

    def has_update(self):
        cfile = Path(f'{self.cmd_path}/update.cmd')
        if cfile.exists():
            with open(cfile, "r", encoding='utf-8') as f:
                cfg = json.load(f)
                if cfg["enabled"]:
                    self.load(cfg["path"])
                else:
                    for instance in self.run_instances:
                        if instance.path == cfg["path"]:
                            instance.stop()
                            self.remove(instance)
            cfile.unlink(missing_ok=True)

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
                    self.update_from_status(status_file, rserver)
                cfile.unlink(missing_ok=True)

    def update_from_status(self, status_file : str, rserver : str):
        status_file = Path(status_file)
        if status_file.exists():
            with open(status_file, "r", encoding='utf-8') as f:
                new_status = json.load(f)
                for instance in new_status:
                    if instance not in self.all_status:
                        print(f"new instance: {instance} from {status_file}")
                        src = f'{self.pbgdir}/data/remote/multi_{rserver}/{instance}'
                        dest = self.multi_path
                        print(f'copy {src} {dest}')
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                        self.watch_multi([f'{self.multi_path}/{instance}'])
                    else:
                        for status in self.all_status:
                            if status not in new_status:
                                print(f"remove instance: {status} from {status_file}")
                                if status.running:
                                    for multi in self.run_multi:
                                        if multi.user == status.name:
                                            multi.stop()
                                            self.remove_multi(multi)
                                dest = f'{self.multi_path}/{status}'
                                print(dest)
#                                shutil.rmtree(dest, ignore_errors=True)
                                    
                        #     if status == instance:
                        #         # if info for me
                        #         if instance["enabled_on"] == self.name:
                        #             # if new version
                        #             if instance["version"] > status.version:
                        #                 print(f"new version for {instance} new:{instance["version"]} my:{status.version}")
                        #             else:
                        #                 print(f"nothing to do for {instance}")

                        # if new_status[instance]["enabled_on"] == self.name and new_status[instance]["multi"]:
                        #     for multi in self.


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
                cfile.unlink(missing_ok=True)

    def load(self, instance: str):
        file = Path(f'{instance}/instance.cfg')
        if file.exists():
            run_instance = RunInstance()
            run_instance.path = instance
            run_instance.load()
            if run_instance.enabled and not run_instance.multi:
                self.add(run_instance)
            else:
                run_instance.stop()

    def load_all(self):
        self.run_instances = []
        p = str(Path(f'{self.instances_path}/*'))
        instances = glob.glob(p)
        for instance in instances:
            self.load(instance)

    def save_all_status(self):
        file = str(Path(f'{self.cmd_path}/status.json'))
        status = {}
        with open(file, "w", encoding='utf-8') as f:
            for instance in self.all_status:
                status[instance.name] = ({
                    "enabled_on" : instance.enabled_on,
                    "version": instance.version,
                    "multi": instance.multi,
                    "running": instance.running
                })
            json.dump(status, f, indent=4)

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
                self.update_status(status)
        self.save_all_status()

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
#    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
#    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
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
    count = 0
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 1048576:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            run.has_restart()
            run.has_update()
            run.has_activate()
            run.has_update_status()
            for run_instance in run:
                run_instance.watch()
            for run_multi in run.run_multi:
                run_multi.watch()
            if count%2 == 0:
                for run_instance in run:
                    run_instance.clean_log()
                for run_multi in run.run_multi:
                    run_multi.clean_log()
            sleep(5)
            count += 1
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()
#            exit()

if __name__ == '__main__':
    main()