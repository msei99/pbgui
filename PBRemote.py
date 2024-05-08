import psutil
import subprocess
import configparser
import sys
import os
from pathlib import Path, PurePath
from time import sleep
import glob
import json
from io import TextIOWrapper
from datetime import datetime
import platform
from PBRun import PBRun, RunMulti, RunInstance
from Status import InstanceStatus, InstancesStatus
import uuid
import shutil
import hashlib
import traceback

class RemoteServer():
    def __init__(self, path: str):
        self._name = None
        self._ts = None
        self._startts = 0
        self._rtd = None
        self._run = None
        self._edit = False
        self._path = path
        self._unique = []
        self._api_md5 = None
        self._pbdir = None
        self._bucket = None
        self._instances = []
        self._mem = []
        self._swap = []
        self._disk = []
        self._cpu = None
        self._boot = None
#        self.status_ts = 0
        self.pbname = None
        self.instances_status = InstancesStatus(f'{self.path}/status.json')
        self.instances_status.load()
        self.instances_status_single = InstancesStatus(f'{self.path}/status_single.json')
        self.instances_status_single.load()

    @property
    def name(self): return self._name
    @property
    def ts(self): return self._ts
    @property
    def startts(self): return self._startts
    @property
    def rtd(self): return self._rtd
    @property
    def run(self): return self._run
    @property
    def edit(self): return self._edit
    @property
    def path(self): return self._path
    @property
    def api_md5(self): return self._api_md5
    @property
    def pbdir(self): return self._pbdir
    @property
    def bucket(self): return self._bucket
    @property
    def mem(self): return self._mem
    @property
    def swap(self): return self._swap
    @property
    def disk(self): return self._disk
    @property
    def cpu(self): return self._cpu
    @property
    def boot(self): return self._boot

    @name.setter
    def name(self, new_name):
        if self._name != new_name:
            self._name = new_name
    @ts.setter
    def ts(self, new_ts):
        if self._ts != new_ts:
            self._ts = new_ts
    @startts.setter
    def startts(self, new_startts):
        if self._startts != new_startts:
            self._startts = new_startts
    @edit.setter
    def edit(self, new_edit):
        if self._edit != new_edit:
            self._edit = new_edit
    @path.setter
    def path(self, new_path):
        if self._path != new_path:
            self._path = new_path
    @pbdir.setter
    def pbdir(self, new_pbdir):
        if self._pbdir != new_pbdir:
            self._pbdir = new_pbdir
    @bucket.setter
    def bucket(self, new_bucket):
        if self._bucket != new_bucket:
            self._bucket = new_bucket

    def is_running(self, user : str, symbol : str):
        self.load()
        if self.run:
            for running in self.run:
                if running["user"] == user and running["symbol"] == symbol:
                    return True
        if self.has_instance(user,symbol):
            return False
        else:
            return None

    def has_instance(self, user : str, symbol : str):
        inst = {
            "user": user,
            "symbol": symbol
        }
        if inst in self._instances:
            return True
        return False

    def is_api_md5_same(self, api_md5 : str):
        if self.api_md5 == api_md5:
            return True
        return False

    def is_online(self):
        self.load()
        timestamp = round(datetime.now().timestamp())
        self._rtd = timestamp - self.ts
        if self._rtd < 60:
            return True
        return False

    def load_instances(self):
        self._instances = []
        p = str(Path(f'{self._path}/../instances_{self.name}/*'))
        instances = glob.glob(p)
        for instance in instances:
            file = Path(f'{instance}/instance.cfg')
            if file.exists():
                try:
                    with open(file, "r", encoding='utf-8') as f:
                        config = json.load(f)
                        inst = {
                            "user": config["_user"],
                            "symbol": config["_symbol"]
                        }
                        self._instances.append(inst)
                except Exception as e:
                    print(f'{str(file)} is corrupted {e}')

    def load(self):
        p = str(Path(f'{self._path}/alive_*.cmd'))
        alive_remote = glob.glob(p)
        alive_remote.sort()
        if alive_remote:
            while len(alive_remote) > 0:
                remote = Path(alive_remote.pop())
                try:
                    with open(remote, "r", encoding='utf-8') as f:
                        cfg = json.load(f)
                        if "name" in cfg and "timestamp" in cfg:
                            self._name = cfg["name"]
                            self._ts = cfg["timestamp"]
                        if "startts" in cfg:
                            self._startts = cfg["startts"]
                        if "api_md5" in cfg:
                            self._api_md5 = cfg["api_md5"]
                        if "run" in cfg:
                            self._run = cfg["run"]
                        if "mem" in cfg:
                            self._mem = cfg["mem"]
                        if "swap" in cfg:
                            self._swap = cfg["swap"]
                        if "disk" in cfg:
                            self._disk = cfg["disk"]
                        if "cpu" in cfg:
                            self._cpu = cfg["cpu"]
                        if "boot" in cfg:
                            self._boot = cfg["boot"]
                        return
                except Exception as e:
                    print(f'{str(remote)} is corrupted {e}')

    def send_to(self, command : str, user : str = None, symbol : str = None, market_type : str = None):
        if command == "sync_api":
            dest = Path(f'{self._path}/../../cmd/{self.name}_api-keys.json')
            if dest.exists():
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} api sync_to: {self.name} already started')
                return
        unique = str(uuid.uuid4())
        timestamp = round(datetime.now().timestamp())
        if user:
            instance = f'{user}_{symbol}_{market_type}'
        else:
            instance = "all"
        cfile = str(Path(f'{self._path}/../../cmd/send_{self.name}_{unique}.cmd'))
        cfg = ({
            "timestamp": timestamp,
            "unique": unique,
            "to": self.name,
            "command": command,
            "instance": instance
            })
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
#        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_to: {self.name} {command} {instance}')

    def ack_to(self, command : str, instance : str, unique : str):
        timestamp = round(datetime.now().timestamp())
        cfile = str(Path(f'{self._path}/../../cmd/{self.name}_{unique}.ack'))
        cfg = ({
            "timestamp": timestamp,
            "unique": unique,
            "to": self.name,
            "command": command,
            "instance": instance
            })
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} ack_to: {self.name} {command} {instance} {unique}')

    def ack_from(self, pbname : str):
        p = str(Path(f'{self._path}/{pbname}_*.ack'))
        ack_remote = glob.glob(p)
        if ack_remote:
            for ack in ack_remote:
                remote = Path(ack)
                try:
                    with open(remote, "r", encoding='utf-8') as f:
                        cfg = json.load(f)
                        if "to" in cfg and "unique" in cfg:
                            to = cfg["to"]
                            if to == pbname:
                                unique = cfg["unique"]
                                instance = cfg["instance"]
                                command = cfg["command"]
                                if command == "sync_api":
                                    cfile = Path(f'{self._path}/../../cmd/{self.name}_api-keys.json')
                                    if cfile.exists():
                                        cfile.unlink(missing_ok=True)
                                cfile = Path(f'{self._path}/../../cmd/sync_{self.name}_{unique}.cmd')
                                if cfile.exists():
                                    cfile.unlink(missing_ok=True)
                                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} ack_from: {self.name} {command} {instance} {unique}')
                                    return True
                except Exception as e:
                    print(f'{str(remote)} is corrupted {e}')

    def sync_from(self, pbname : str):
        p = str(Path(f'{self._path}/sync_{pbname}_*.cmd'))
        sync_remote = glob.glob(p)
        if sync_remote:
            for sync in sync_remote:
                remote = Path(sync)
                try:
                    with open(remote, "r", encoding='utf-8') as f:
                        cfg = json.load(f)
                        if "to" in cfg and "instance" in cfg and "unique" in cfg:
                            to = cfg["to"]
                            if to == pbname:
                                command = cfg["command"]
                                instance = cfg["instance"]
                                unique = cfg["unique"]
                                if unique not in self._unique:
                                    if command == "sync_api":
                                        api_keys = PurePath(f'{self._pbdir}/api-keys.json')
                                        # Backup api-keys
                                        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                                        api_backup = Path(f'{self._path}/../../api-keys')
                                        if not api_backup.exists():
                                            api_backup.mkdir(parents=True)
                                        backup_dest = Path(f'{api_backup}/api-keys_{date}.json')
                                        if Path(api_keys).exists():
                                            shutil.copy(api_keys, backup_dest)
                                        # Copy new api-keys
                                        src = PurePath(f'{self._path}/{to}_api-keys.json')
                                        shutil.copy(src, api_keys)
                                    elif command == "sync":
                                        self.sync(pbname)
                                        src = PurePath(f'{self._path}/../instances_{self.name}/{instance}')
                                        dest = PurePath(f'{self._path}/../../instances/{instance}')
                                        if PBRun().is_enabled_instance(instance):
                                            shutil.copytree(src, dest, dirs_exist_ok=True)
                                            PBRun().enable_instance(instance)
                                            PBRun().restart_instance(instance)
                                        else:
                                            shutil.copytree(src, dest, dirs_exist_ok=True)
                                            PBRun().disable_instance(instance)
                                    elif command == "remove":
                                        dest = PurePath(f'{self._path}/../../instances/{instance}')
                                        shutil.rmtree(dest, ignore_errors=True)
                                    elif command == "start":
                                        PBRun().start_instance(instance)
                                    elif command == "stop":
                                        PBRun().stop_instance(instance)
                                    else:
                                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_from: unknown command {self.name} {command} {instance} {unique}')    
                                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_from: {self.name} {command} {instance} {unique}')
                                    self.ack_to(command, instance, unique)
                                    self._unique.append(unique)
                                    return True
                except Exception as e:
                    print(f'{str(remote)} is corrupted {e}')

        else:
            p = str(Path(f'{self.path}/../../cmd/*.ack'))
            sync_ack = glob.glob(p)
            if sync_ack:
                for file in sync_ack:
                    afile = Path(file)
                    try:
                        with open(afile, "r", encoding='utf-8') as f:
                            cfg = json.load(f)
                        if cfg:
                            to = cfg["to"]
                            unique = cfg["unique"]
                            instance = cfg["instance"]
                            command = cfg["command"]
                            if to == self.name:
                                if unique in self._unique:
                                    self._unique.remove(unique)
                                afile.unlink(missing_ok=True)
                                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} remove_ack: {self.name} {command} {instance} {unique}')
                    except Exception as e:
                        print(f'{str(afile)} is corrupted {e}')
                        traceback.print_exc()

    def sync(self, pbname: str):
        pbgdir = Path.cwd()
        spath = 'instances'
        cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{pbname}/*,cmd_**}}', f'{self.bucket}', PurePath(f'{pbgdir}/data/remote')]
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        log = open(logfile,"ab")
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NO_WINDOW
            subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
        else:
            subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)

    def sync_multi_down(self):
        if self.instances_status.has_new_status():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} New status.json from: {self.name}')
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync multi from: {self.name}')
            pbgdir = Path.cwd()
            cmd = ['rclone', 'sync', '-v', '--include', f'{{multi.hjson,*.json}}', f'{self.bucket}/multi_{self.name}', PurePath(f'{pbgdir}/data/remote/multi_{self.name}')]
            logfile = Path(f'{pbgdir}/data/logs/sync.log')
            log = open(logfile,"ab")
            if platform.system() == "Windows":
                creationflags = subprocess.CREATE_NO_WINDOW
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
            PBRun().update_status(self.instances_status.status_file, self.name)
            status_ts = self.instances_status.status_ts
            self.instances_status.update_status()
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Update status ts: {self.name} old: {status_ts} new: {self.instances_status.status_ts}')

    def sync_single_down(self):
        if self.instances_status_single.has_new_status():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} New status_single.json from: {self.name}')
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync single from: {self.name}')
            pbgdir = Path.cwd()
            cmd = ['rclone', 'sync', '-v', '--include', f'{{instance.cfg,config.json}}', f'{self.bucket}/instances_{self.name}', PurePath(f'{pbgdir}/data/remote/instances_{self.name}')]
            logfile = Path(f'{pbgdir}/data/logs/sync.log')
            log = open(logfile,"ab")
            if platform.system() == "Windows":
                creationflags = subprocess.CREATE_NO_WINDOW
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
            PBRun().update_status(self.instances_status_single.status_file, self.name)
            status_ts = self.instances_status_single.status_ts
            self.instances_status_single.update_status()
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Update status_single ts: {self.name} old: {status_ts} new: {self.instances_status_single.status_ts}')

    def sync_api(self):
        api_file = Path(f'{self._path}/api-keys.json')
        if api_file.exists():
            api_keys = Path(f'{self._pbdir}/api-keys.json')
            if self.calculate_md5(api_file) != self.calculate_md5(api_keys):
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Install new API Keys from: {self.name}')
                # Backup api-keys
                date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                pbgdir = Path.cwd()
                destination = Path(f'{pbgdir}/data/backup/api-keys/{date}')
                if not destination.exists():
                    destination.mkdir(parents=True)
                shutil.copy(api_keys, destination)
                # Copy new api-keys
                shutil.copy(api_file, api_keys)

    def calculate_md5(self, file: Path):
        if file.exists():
            with open(file, 'rb') as file_obj:
                file_contents = file_obj.read()
            return hashlib.md5(file_contents).hexdigest()
        return None

class PBRemote():
    def __init__(self):
        self.error = None          
        self.remote_servers = []
        self.local_run = PBRun()
        self.index = 0
        self.api_md5 = None
#        self.status_ts = 0
        self.startts = None
        self.sync_downts = None
        pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        self.pbdir = pb_config.get("main", "pbdir")
        self.instances_path = f'{pbgdir}/data/instances'
        self.cmd_path = f'{pbgdir}/data/cmd'
        self.remote_path = f'{pbgdir}/data/remote'
        if not Path(self.cmd_path).exists():
            Path(self.cmd_path).mkdir(parents=True)  
        self.piddir = Path(f'{pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbremote.pid')
        self.my_pid = None
        self.bucket = None
        self.rclone_installed = self.is_rclone_installed()
        if not self.rclone_installed:
            self.error = "rclone not installed"
            return
        self.buckets = self.fetch_buckets()
        if not self.buckets:
            self.error = "Rclone not configured. No buckets found."
            return
        self.load_config()
        if not self.bucket:
            self.error = "bucket not configured. Please configure bucket in pbgui.ini\n[pbremote]\nbucket = <bucket_name>:"
            return
        self.bucket_dir = f'{self.bucket}{self.bucket.split(":")[0]}'
        self.load_remote()
        self.load_local()

    def __iter__(self):
        return iter(self.remote_servers)

    def __next__(self):
        if self.index > len(self.remote_servers):
            raise StopIteration
        self.index += 1
        return next(self)

    def list(self):
        return list(map(lambda c: c.name, self.remote_servers))

    def find_server(self, name: str):
        for server in self.remote_servers:
            if server.name == name:
                return server

    def add(self, remote_servers: RemoteServer):
        if remote_servers:
            self.remote_servers.append(remote_servers)

    def remove(self, remote_servers: RemoteServer):
        if remote_servers:
            self.remote_servers.remove(remote_servers)

    def is_sync_running(self):
        if self.sync_pid():
            return True
        return False

    def sync_pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.AccessDenied:
                continue
            if any("rclone" in sub for sub in cmdline) and any(f'{self.bucket_dir}' in sub for sub in cmdline):
                return process

    def sync(self, direction: str, spath: str):
        pbgdir = Path.cwd()
        if direction == 'up' and spath == 'cmd':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{alive_*.cmd,sync_*.cmd,*.ack,api-keys.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'{self.bucket_dir}/{spath}_{self.name}']
        elif direction == 'up' and spath == 'instances':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{instance.cfg,config.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'{self.bucket_dir}/{spath}_{self.name}']
        elif direction == 'up' and spath == 'status':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{alive_*.cmd,sync_*.cmd,*.ack,api-keys.json,status.json}}', PurePath(f'{pbgdir}/data/cmd'), f'{self.bucket_dir}/cmd_{self.name}']
        elif direction == 'up' and spath == 'status_single':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{alive_*.cmd,status_single.json}}', PurePath(f'{pbgdir}/data/cmd'), f'{self.bucket_dir}/cmd_{self.name}']
        elif direction == 'up' and spath == 'multi':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{multi.hjson,*.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'{self.bucket_dir}/{spath}_{self.name}']
        elif direction == 'down' and spath == 'cmd':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{self.name}/*,instances_**,multi_**}}', f'{self.bucket_dir}', PurePath(f'{pbgdir}/data/remote')]
        elif direction == 'down' and spath == 'instances':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{self.name}/*,cmd_**,multi_**}}', f'{self.bucket_dir}', PurePath(f'{pbgdir}/data/remote')]
            self.sync_downts = round(datetime.now().timestamp())
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile.replace(f'{pbgdir}/data/logs/sync.log.old')
                logfile = Path(f'{pbgdir}/data/logs/sync.log')
        log = open(logfile,"ab")
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NO_WINDOW
            subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
        else:
            subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
#        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: {cmd}')

    def sync_to(self):
        p = str(Path(f'{self.cmd_path}/send_*.cmd'))
        sync_cmd = glob.glob(p)
        if sync_cmd:
            for file in sync_cmd:
                cfile = Path(file)
                try:
                    with open(cfile, "r", encoding='utf-8') as f:
                        cfg = json.load(f)
                    to = cfg["to"]
                    unique = cfg["unique"]
                    instance = cfg["instance"]
                    command = cfg["command"]
                    if command == "sync_api":
                        src = PurePath(f'{self.pbdir}/api-keys.json')
                        dest = PurePath(f'{self.cmd_path}/{to}_api-keys.json')
                        shutil.copy(src, dest)
                    if command == "copy":
                        src = PurePath(f'{self.remote_path}/instances_{to}/{instance}')
                        dest = PurePath(f'{self.instances_path}/{instance}')
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                        PBRun().disable_instance(instance)
                        cfile.unlink(missing_ok=True)
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_from: {to} {command} {instance} {unique}')
                    if command == "sync":
                        self.sync('up', 'instances')
                    if command in ['start','stop','sync','sync_api','remove']:
                        cfile.rename(PurePath(f'{self.cmd_path}/sync_{to}_{unique}.cmd'))
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_to: {to} {command} {instance} {unique}')
                except Exception as e:
                    print(f'{str(cfile)} is corrupted {e}')

    def sync_multi_up(self):
        if self.local_run.instances_status.has_new_status():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} New status.json from: {self.name}')
            status_ts = self.local_run.instances_status.status_ts
            self.local_run.instances_status.update_status()
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Update status ts: {self.name} old: {status_ts} new: {self.local_run.instances_status.status_ts}')
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync multi up: {self.name}')
            self.sync('up', 'multi')
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync status.json up: {self.name}')
            self.sync('up', 'status')
    
    def sync_single_up(self):
        if self.local_run.instances_status_single.has_new_status():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} New status_single.json from: {self.name}')
            status_ts = self.local_run.instances_status_single.status_ts
            self.local_run.instances_status_single.update_status()
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Update status_single ts: {self.name} old: {status_ts} new: {self.local_run.instances_status_single.status_ts}')
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync single up: {self.name}')
            self.sync('up', 'instances')
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync status_single.json up: {self.name}')
            self.sync('up', 'status_single')

    def sync_api_up(self):
        pbgdir = Path.cwd()
        api_file = Path(f'{pbgdir}/data/cmd/api-keys.json')
        source = Path(f'{self.pbdir}/api-keys.json')
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync api-keys.json to all remote servers')
        shutil.copy(source, api_file)
    
    def check_if_api_synced(self):
        for server in self.remote_servers:
            if not server.is_api_md5_same(self.api_md5):
                return False
        pbgdir = Path.cwd()
        api_file = Path(f'{pbgdir}/data/cmd/api-keys.json')
        if api_file.exists():
            api_file.unlink(missing_ok=True)
        return True

    def alive(self):
        timestamp = round(datetime.now().timestamp())
        cfile = Path(f'{self.cmd_path}/alive_{timestamp}.cmd')
        run = []
        for instance in self.local_run:
            inst = ({
                "user": instance.user,
                "symbol": instance.symbol
            })
            run.append(inst)
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk = psutil.disk_usage('/')
        cpu = psutil.cpu_percent()
        boot = psutil.boot_time()
        cfg = ({
            "timestamp": timestamp,
            "startts": self.startts,
            "name": self.name,
            "api_md5": self.api_md5,
            "run": run,
            "mem": mem,
            "swap": swap,
            "disk": disk,
            "cpu": cpu,
            "boot": boot
            })
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
        self.sync('up', 'cmd')
        p = str(Path(f'{self.cmd_path}/alive_*.cmd'))
        found_local = glob.glob(p)
        found_local.sort()
        while len(found_local) > 9:
            local = Path(found_local.pop(0))
            local.unlink(missing_ok=True)

    def calculate_api_md5(self):
        file = Path(f'{self.pbdir}/api-keys.json')
        if file.exists():
            with open(file, 'rb') as file_obj:
                file_contents = file_obj.read()
            return hashlib.md5(file_contents).hexdigest()
        return None

    # def find_bucket(self):
    #     cmd = ['rclone', 'listremotes']
    #     try:
    #         if platform.system() == "Windows":
    #             creationflags = subprocess.CREATE_NO_WINDOW
    #             result = subprocess.run(cmd, capture_output=True, text=True, creationflags=creationflags)
    #         else:
    #             result = subprocess.run(cmd, capture_output=True, text=True)
    #     except Exception as e:
    #         self.error = "rclone not installed"
    #         print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: {self.error} {e}')
    #         return None
    #     if result.returncode == 0:
    #         if result.stdout:
    #             bucket = result.stdout.strip().split(':')[0]
    #             return f'{bucket}:{bucket}'
    #     self.error = "Can not find bucket name"
    #     print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: {self.error}')
    #     return None

    def load_remote(self):
        pbgdir = Path.cwd()
        self.remote_servers = []
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
            rserver.pbdir = self.pbdir
            rserver.bucket = self.bucket_dir
            rserver.pbname = self.name
            rserver.load()
            rserver.load_instances()
            self.add(rserver)

    def load_local(self):
        self.local_run.load_all()
        self.api_md5 = self.calculate_api_md5()

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBRemote.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBRemote')
                sleep(2)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBRemote')
            psutil.Process(self.my_pid).kill()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbremote.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))

    def load_config(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_section("pbremote"):
            if pb_config.has_option("pbremote", "bucket"):
                self.bucket = pb_config.get("pbremote", "bucket")
            else:
                self.bucket = None

    def save_config(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("pbremote"):
            pb_config.add_section("pbremote")
        pb_config.set("pbremote", "bucket", self.bucket)
        with open('pbgui.ini', 'w') as configfile:
            pb_config.write(configfile)

    def is_rclone_installed(self):
        cmd = ['rclone', 'version']
        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return True
        except FileNotFoundError:
            return False
    
    def fetch_buckets(self):
        if self.is_rclone_installed():
            cmd = ['rclone', 'listremotes']
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode == 0:
                    return result.stdout.splitlines()
            except FileNotFoundError:
                pass
        return []


def main():
    # print("Start PBRemote")
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBRemote.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Init: PBRemote')
    remote = PBRemote()
    if remote.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBRemote already started')
        exit(1)
    remote.save_pid()
    if not remote.bucket:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'Error: {remote.error}')
        exit(1)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBRemote {remote.bucket}')
    remote.startts = round(datetime.now().timestamp())
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync instances up: {remote.name}')
    remote.sync('up', 'instances')
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Sync instances down: {remote.name}')
    remote.sync('down', 'instances')
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            remote.sync_multi_up()
            remote.sync_single_up()
            remote.check_if_api_synced()
            remote.alive()
            remote.sync_to()
            remote.sync('down', 'cmd')
            for server in remote.remote_servers:
                server.load()
                server.sync_multi_down()
                server.sync_single_down()
                server.sync_api()
                if server.sync_from(remote.name):
                    remote.sync("up", 'instances')
                    remote.load_local()
                # Sync from Cloud Storage when we get an .ack from remote Server or when remote server was restarted
                if server.ack_from(remote.name) or server.startts > remote.sync_downts:
                    remote.sync("down", 'instances')
                    server.load_instances()
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()

if __name__ == '__main__':
    main()