"""
PBRemote manages cloud storage synchronization for v7 passivbot instances.

RemoteServer() imports v7 configs and alive data from remote storage.
PBRemote() exports v7 configs, status and alive data to remote storage.
"""
import psutil
import subprocess
import configparser
import sys
import os
from pathlib import Path, PurePath
from time import sleep
import glob
import json
from datetime import datetime
import platform
from PBRun import PBRun
from Status import InstancesStatus
import shutil
import hashlib
import traceback
import gzip
from MonitorConfig import MonitorConfig
from logging_helpers import human_log as _log

class RemoteServer():
    def __init__(self, path: str):
        """
        Initialize a RemoteServer instance to manage v7 PB configurations.

        Args:
            path (str): Path to the remote server configuration.
        """
        self._name = None
        self._ts = 0
        self._startts = 0
        self._rtd = None
        self._edit = False
        self._path = path
        self._unique = []
        self._api_md5 = None
        self._pb7dir = None
        self._bucket = None
        # self._instances = []
        self._mem = []
        self._swap = []
        self._disk = []
        self._cpu = 0
        self._boot = 0
        self._monitor = []
        self._upgrades = 0
        self._reboot = False
        self._cmc_credits = 0
        self._role = None
        self._pbgui_version = "N/A"
        self._pbgui_commit = None
        self._pbgui_branch = "unknown"
        self._pbgui_python = "N/A"
        self._pb7_version = "N/A"
        self._pb7_commit = None
        self._pb7_branch = "unknown"
        self._pb7_python = "N/A"
        self.pbname = None
        self.instances_status_v7 = InstancesStatus(f'{self.path}/status_v7.json')
        self.instances_status_v7.load()

    @property
    def name(self): return self._name
    @property
    def ts(self): return self._ts
    @property
    def startts(self): return self._startts
    @property
    def rtd(self): return self._rtd
    @property
    def edit(self): return self._edit
    @property
    def path(self): return self._path
    @property
    def api_md5(self): return self._api_md5
    @property
    def pb7dir(self): return self._pb7dir
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
    @property
    def monitor(self): return self._monitor
    @property
    def upgrades(self): return self._upgrades
    @property
    def reboot(self): return self._reboot
    @property
    def cmc_credits(self): return self._cmc_credits
    @property
    def role(self): return self._role
    @property
    def pbgui_version(self): return self._pbgui_version
    @property
    def pbgui_commit(self): return self._pbgui_commit
    @property
    def pbgui_branch(self): return self._pbgui_branch
    @property
    def pbgui_python(self): return self._pbgui_python
    @property
    def pb7_version(self): return self._pb7_version
    @property
    def pb7_commit(self): return self._pb7_commit
    @property
    def pb7_branch(self): return self._pb7_branch
    @property
    def pb7_python(self): return self._pb7_python

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
    @pb7dir.setter
    def pb7dir(self, new_pb7dir):
        if self._pb7dir != new_pb7dir:
            self._pb7dir = new_pb7dir
    @bucket.setter
    def bucket(self, new_bucket):
        if self._bucket != new_bucket:
            self._bucket = new_bucket

    def is_api_md5_same(self, api_md5: str):
        """
        Check if the API MD5 hash is the same as the stored one.

        Args:
            api_md5 (str): The API MD5 hash to compare.

        Returns:
            bool: True if the API MD5 hash is the same, False otherwise.
        """
        if self.api_md5 == api_md5:
            return True
        return False

    def is_online(self):
        """
        Check if the remote server is online by loading the alive_*.cmd file and checking if the latest is less than 300 seconds ago.

        Returns:
            bool: True if the remote server is online, False otherwise.
        """
        self.load()
        timestamp = round(datetime.now().timestamp())
        self._rtd = timestamp - self.ts
        if self._rtd < 300:
            return True
        return False

    def load(self):
        """
        Load the server's configuration.
        """
        p = str(Path(f'{self._path}/alive_*.cmd*'))
        alive_remote = glob.glob(p)
        alive_remote.sort()
        self._name = PurePath(self._path).name[4:]
        if alive_remote:
            while len(alive_remote) > 0:
                remote = Path(alive_remote.pop())
                try:
                    if str(remote).endswith('.gz'):
                        with gzip.open(remote, "rt", encoding='utf-8') as f:
                            cfg = json.load(f)
                    else:
                        with open(remote, "r", encoding='utf-8') as f:
                            cfg = json.load(f)
                    if "name" in cfg and "timestamp" in cfg:
                        self._ts = cfg["timestamp"]
                    if "startts" in cfg:
                        self._startts = cfg["startts"]
                    if "api_md5" in cfg:
                        self._api_md5 = cfg["api_md5"]
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
                    if "monitor" in cfg:
                        self._monitor = cfg["monitor"]
                    if "upgrades" in cfg:
                        self._upgrades = cfg["upgrades"]
                    if "reboot" in cfg:
                        self._reboot = cfg["reboot"]
                    if "cmc" in cfg:
                        self._cmc_credits = cfg["cmc"]
                    if "role" in cfg:
                        self._role = cfg["role"]
                    if "pbgv" in cfg:
                        self._pbgui_version = cfg["pbgv"]
                    if "pbgc" in cfg:
                        self._pbgui_commit = cfg["pbgc"]
                    if "pbgb" in cfg:
                        self._pbgui_branch = cfg["pbgb"]
                    else:
                        # Reset to unknown if pbgb not in alive file (old version)
                        self._pbgui_branch = "unknown"
                    if "pbgpy" in cfg:
                        self._pbgui_python = cfg["pbgpy"]
                    else:
                        self._pbgui_python = "N/A"
                    if "pb7v" in cfg:
                        self._pb7_version = cfg["pb7v"]
                    if "pb7c" in cfg:
                        self._pb7_commit = cfg["pb7c"]
                    if "pb7b" in cfg:
                        self._pb7_branch = cfg["pb7b"]
                    else:
                        # Reset to unknown if pb7b not in alive file (old version)
                        self._pb7_branch = "unknown"
                    if "pb7py" in cfg:
                        self._pb7_python = cfg["pb7py"]
                    else:
                        self._pb7_python = "N/A"
                    return
                except Exception as e:
                    _log('PBRemote', f'{str(remote)} is corrupted {e}', level='ERROR')

    def sync_v7_down(self, role: str):
        """Sync v7 configs + alive from remote storage to local machine."""
        if self.instances_status_v7.has_new_status():
            _log('PBRemote', f'New status_v7.json from: {self.name}', level='INFO')
            _log('PBRemote', f'Sync v7 from: {self.name}', level='INFO')
            pbgdir = Path.cwd()
            if role == "master":
                cmd = ['rclone', 'sync', '-v', '--filter', f'- cmd_{self.pbname}/*', '--filter', '+ cmd_**/alive_*.cmd*', '--filter', f'+ run_v7_{self.name}/**/*.json', '--filter', '- *', f'{self.bucket}', PurePath(f'{pbgdir}/data/remote')]
            else:
                cmd = ['rclone', 'sync', '-v', '--include', f'{{*.json}}', f'{self.bucket}/run_v7_{self.name}', PurePath(f'{pbgdir}/data/remote/run_v7_{self.name}')]
            logfile = Path(f'{pbgdir}/data/logs/sync.log')
            with open(logfile, "ab") as log:
                if platform.system() == "Windows":
                    creationflags = subprocess.CREATE_NO_WINDOW
                    subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
                else:
                    subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
            PBRun().update_status(self.instances_status_v7.status_file, self.name)
            status_ts = self.instances_status_v7.status_ts
            self.instances_status_v7.update_status()
            _log('PBRemote', f'Update status_v7 ts: {self.name} old: {status_ts} new: {self.instances_status_v7.status_ts}', level='INFO')

    def sync_api(self):
        """
        Sync the API keys from the remote storage to the local machine.
        """
        api_file = Path(f'{self._path}/api-keys.json')
        if api_file.exists():
            if self.pb7dir:
                api_keys = Path(f'{self._pb7dir}/api-keys.json')
                self.update_api(api_file, api_keys)

    def update_api(self, api_file: Path, api_keys: Path):
        """
        Checks if the api-keys.json from pbgui (self._path) is different from api-keys.json from passivbot.
        If different, It creates a backup of the passivbot api-keys.json to pbgui/data/backup, and then copy the new api-keys.json file to the passivbot directory.
        """
        if self.calculate_md5(api_file) != self.calculate_md5(api_keys):
            _log('PBRemote', f'Install new API Keys from: {self.name} to {api_keys}', level='INFO')
            # Backup api-keys
            date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            pbgdir = Path.cwd()
            destination = Path(f'{pbgdir}/data/backup/api-keys_v7/{date}')
            if not destination.exists():
                destination.mkdir(parents=True)
            if api_keys.exists():
                shutil.copy(api_keys, destination)
            # Copy new api-keys atomically (write to temp, then rename)
            tmp = api_keys.with_suffix('.tmp')
            shutil.copy(api_file, tmp)
            tmp.replace(api_keys)

    def calculate_md5(self, file: Path):
        """Checks if the two API files have the same hash using md5 protocol."""
        if file.exists():
            with open(file, 'rb') as file_obj:
                file_contents = file_obj.read()
            return hashlib.md5(file_contents).hexdigest()
        return None

    def delete_server(self):
        """
        Delete the server from the remote storage.
        """
        pbgdir = Path.cwd()
        # rclone delete pbgui:pbgui --include *manibot51*/**
        cmd = ['rclone', 'delete', '-v', f'{self.bucket}', '--include', f'*{self.name}*/**']
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        with open(logfile, "ab") as log:
            if platform.system() == "Windows":
                creationflags = subprocess.CREATE_NO_WINDOW
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
        # delete local files
        shutil.rmtree(f'{pbgdir}/data/remote/cmd_{self.name}', ignore_errors=True)
        shutil.rmtree(f'{pbgdir}/data/remote/run_v7_{self.name}', ignore_errors=True)

class PBRemote():
    """
    PBRemote class is used to manage the local server and synchronizing data from the remote storage.
    """
    def __init__(self):
        """
        Initializes the PBRemote instance, sets up directories, loads configuration,
        and checks for rclone installation and configuration.
        """
        self.error = None          
        self.remote_servers = []
        self.local_run = PBRun()
        self.index = 0
        self.startts = None
        self.alivets = 0
        self.systemts = 0
        self.rtd = 0
        pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        # Init pbname
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        # Init role
        if pb_config.has_option("main", "role"):
            self.role = pb_config.get("main", "role")
        else:
            self.role = "slave"
        # Init pb7dir
        self.pb7dir = None
        if pb_config.has_option("main", "pb7dir"):
            self.pb7dir = pb_config.get("main", "pb7dir")
        if not self.pb7dir:
            _log('PBRemote', 'Error: No passivbot v7 directory configured in pbgui.ini', level='ERROR')
            self.error = "No passivbot v7 directory configured in pbgui.ini"
            return
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
        self.bucket_type = "s3"
        self.bucket_endpoint = None
        self.bucket_no_check_bucket = "true"
        self.bucket_access_key_id = None
        self.bucket_secret_access_key = None
        self.bucket_provider = "Synology"
        self.bucket_region = None
        self.rclone_installed = self.is_rclone_installed()
        if not self.rclone_installed:
            _log('PBRemote', 'Error: rclone not installed', level='ERROR')
            self.error = "rclone not installed"
            return
        self.fetch_buckets()
        if not self.buckets:
            _log('PBRemote', 'Error: No buckets found', level='ERROR')
            self.error = "Rclone not configured. No buckets found."
            return
        self.load_config()
        if not self.bucket:
            _log('PBRemote', 'Error: bucket not configured. Please configure bucket in pbgui.ini\n[pbremote]\nbucket = <bucket_name>:', level='ERROR')
            self.error = "bucket not configured. Please configure bucket in pbgui.ini\n[pbremote]\nbucket = <bucket_name>:"
            return
        self.bucket_dir = f'{self.bucket}{self.bucket.split(":")[0]}'
        self.load_remote()

    @property
    def pbgui_version(self):
        return self.local_run.pbgui_version
    @property
    def pbgui_commit(self):
        return self.local_run.pbgui_commit

    @property
    def pbgui_python(self):
        return getattr(self.local_run, 'pbgui_python', 'N/A')
    @property
    def mem(self):
        return psutil.virtual_memory()
    @property
    def swap(self):
        return psutil.swap_memory()
    @property
    def disk(self):
        return psutil.disk_usage('/')
    @property
    def cpu(self):
        return psutil.cpu_percent()
    @property
    def boot(self):
        return psutil.boot_time()
    @property
    def monitor(self):
        return self.load_monitor()
    @property
    def pb7_version(self):
        return self.local_run.pb7_version

    @property
    def pb7_python(self):
        if hasattr(self.local_run, 'update_pb7_python_version'):
            self.local_run.update_pb7_python_version()
        return getattr(self.local_run, 'pb7_python', 'N/A')
    @property
    def pb7_commit(self):
        return self.local_run.pb7_commit
    #unsynced api
    @property
    def unsynced_api(self):
        unsynced = 0
        for server in self.remote_servers:
            server.load()
            if not server.is_api_md5_same(self.api_md5):
                unsynced += 1
        return unsynced

    # api_md5
    @property
    def api_md5(self): return self.calculate_api_md5()

    def __iter__(self):
        return iter(self.remote_servers)

    def list(self):
        return list(map(lambda c: c.name, self.remote_servers))

    def find_server(self, name: str):
        """Find the server by name"""
        for server in self.remote_servers:
            if server.name == name:
                return server

    def add(self, remote_servers: RemoteServer):
        if remote_servers:
            self.remote_servers.append(remote_servers)

    def remove(self, remote_servers: RemoteServer):
        if remote_servers:
            self.remote_servers.remove(remote_servers)

    def has_error(self):
        # Load MonitorConfig
        monitor_config = MonitorConfig()
        # Check if servers has errors or tracebacks
        errors = []
        for server in self.remote_servers:
            if not server.is_online():
                error = ({
                    "server": server.name,
                    "name": "offline",
                    "mem": 0,
                    "cpu": 0,
                    "error": 0,
                    "traceback": 0
                })
                errors.append(error)
            else:
                if (
                    int(server.mem[1] / 1024 / 1024) <= monitor_config.mem_error_server or
                    int(server.swap[2] / 1024 / 1024) <= monitor_config.swap_error_server or
                    int(server.disk[2] / 1024 / 1024) <= monitor_config.disk_error_server or
                    server.cpu >= monitor_config.cpu_error_server
                ):
                    if int(server.mem[1] / 1024 / 1024) <= monitor_config.mem_error_server:
                        color_mem = "red"
                    elif int(server.mem[1] / 1024 / 1024) <= monitor_config.mem_warning_server:
                        color_mem = "orange"
                    else:
                        color_mem = "green"
                    if int(server.swap[2] / 1024 / 1024) <= monitor_config.swap_error_server:
                        color_swap = "red"
                    elif int(server.swap[2] / 1024 / 1024) <= monitor_config.swap_warning_server:
                        color_swap = "orange"
                    else:
                        color_swap = "green"
                    if int(server.disk[2] / 1024 / 1024) <= monitor_config.disk_error_server:
                        color_disk = "red"
                    elif int(server.disk[2] / 1024 / 1024) <= monitor_config.disk_warning_server:
                        color_disk = "orange"
                    else:
                        color_disk = "green"
                    if server.cpu >= monitor_config.cpu_error_server:
                        color_cpu = "red"
                    elif server.cpu >= monitor_config.cpu_warning_server:
                        color_cpu = "orange"
                    else:
                        color_cpu = "green"
                    error = ({
                        "server": server.name,
                        "name": "system",
                        "mem": f':{color_mem}[{int(server.mem[1] / 1024 / 1024)}]',
                        "cpu": f':{color_cpu}[{server.cpu}]',
                        "swap": f':{color_swap}[{int(server.swap[2] / 1024 / 1024)}]',
                        "disk": f':{color_disk}[{int(server.disk[2] / 1024 / 1024)}]'
                    })
                    errors.append(error)
            if server.monitor:
                for monitor in server.monitor:
                    # Determine swap value, fix if vps not reporting swap
                    if len(monitor["m"]) == 10:
                        if monitor["m"][9] > 0:
                            swap_value = round(monitor["m"][9]/1024/1024, 1)
                        else:
                            swap_value = 0
                    else:
                        swap_value = 0
                    if monitor["p"] == "7":
                        if (
                            monitor["m"][0]/1024/1024 > monitor_config.mem_error_v7 or
                            swap_value > monitor_config.swap_error_v7 or
                            monitor["c"] > monitor_config.cpu_error_v7 or
                            monitor["et"] > monitor_config.error_error_v7 or
                            monitor["tt"] > monitor_config.traceback_error_v7
                        ):
                            if monitor["m"][0]/1024/1024 > monitor_config.mem_error_v7:
                                color_mem = "red"
                            elif monitor["m"][0]/1024/1024 > monitor_config.mem_warning_v7:
                                color_mem = "orange"
                            else:
                                color_mem = "green"
                            if swap_value > monitor_config.swap_error_v7:
                                color_swap = "red"
                            elif swap_value > monitor_config.swap_warning_v7:
                                color_swap = "orange"
                            else:
                                color_swap = "green"
                            if monitor["c"] > monitor_config.cpu_error_v7:
                                color_cpu = "red"
                            elif monitor["c"] > monitor_config.cpu_warning_v7:
                                color_cpu = "orange"
                            else:
                                color_cpu = "green"
                            if monitor["et"] > monitor_config.error_error_v7:
                                color_error = "red"
                            elif monitor["et"] > monitor_config.error_warning_v7:
                                color_error = "orange"
                            else:
                                color_error = "green"
                            if monitor["tt"] > monitor_config.traceback_error_v7:
                                color_traceback = "red"
                            elif monitor["tt"] > monitor_config.traceback_warning_v7:
                                color_traceback = "orange"
                            else:
                                color_traceback = "green"
                            error = ({
                                "server": f':blue[{server.name}]',
                                "name": f':blue[{monitor["u"]}]',
                                "mem": f':{color_mem}[{round(monitor["m"][0]/1024/1024,1)}]',
                                "swap": f':{color_swap}[{swap_value}]',
                                "cpu": f':{color_cpu}[{monitor["c"]}]',
                                "error": f':{color_error}[{monitor["et"]}]',
                                "traceback": f':{color_traceback}[{monitor["tt"]}]'
                            })
                            errors.append(error)
        return errors

    def is_sync_running(self):
        if self.sync_pid():
            return True
        return False

    def sync_pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                continue
            if any("rclone" in sub for sub in cmdline) and any(f'{self.bucket_dir}' in sub for sub in cmdline):
                return process

    def is_online(self):
        if self.is_running() and self.local_run.is_running():
            return True
        else:
            return False

    def sync(self, direction: str, spath: str):
        """
        Synchronise between local server and remote storage.
        
        Supported sync paths:
            up/cmd: alive_*.cmd, api-keys.json
            up/status_v7: status_v7.json, alive_*.cmd
            up/run_v7: *.json (v7 configs)
            down/master: all except own cmd, instances, multi, run_v7
            down/slave: same but also excludes alive files from other servers
        
        Args:
            direction (str): Either "up" (local to remote) or "down" (remote to local).
            spath (str): The specific path to synchronize.
        """
        pbgdir = Path.cwd()
        cmd = None
        if direction == 'up' and spath == 'cmd':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{alive_*.cmd*,api-keys.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'{self.bucket_dir}/{spath}_{self.name}']
        elif direction == 'up' and spath == 'status_v7':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{alive_*.cmd*,status_v7.json}}', PurePath(f'{pbgdir}/data/cmd'), f'{self.bucket_dir}/cmd_{self.name}']
        elif direction == 'up' and spath == 'run_v7':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{*.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'{self.bucket_dir}/{spath}_{self.name}']
        elif direction == 'down' and spath == 'master':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{cmd_{self.name}/*,instances_**,multi_**,run_v7_**}}', f'{self.bucket_dir}', PurePath(f'{pbgdir}/data/remote')]
        elif direction == 'down' and spath == 'slave':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{cmd_{self.name}/*,cmd_**/alive_*.cmd*,instances_**,multi_**,run_v7_**}}', f'{self.bucket_dir}', PurePath(f'{pbgdir}/data/remote')]
        if cmd is None:
            _log('PBRemote', f'sync() called with unknown combination: direction={direction!r} spath={spath!r}', level='ERROR')
            return
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile.replace(f'{pbgdir}/data/logs/sync.log.old')
                logfile = Path(f'{pbgdir}/data/logs/sync.log')
        with open(logfile, "ab") as log:
            if platform.system() == "Windows":
                creationflags = subprocess.CREATE_NO_WINDOW
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)

    def sync_status_down(self):
        if self.role == "master":
            self.sync('down', 'master')
        else:
            self.sync('down', 'slave')

    def sync_v7_up(self):
        if self.role != "master":
            # Slave: nothing to do — alive is handled by remote.alive()
            return
        if self.local_run.instances_status_v7.has_new_status():
            _log('PBRemote', f'New status_v7.json from: {self.name}', level='INFO')
            status_ts = self.local_run.instances_status_v7.status_ts
            self.local_run.instances_status_v7.update_status()
            _log('PBRemote', f'Update status_v7 ts: {self.name} old: {status_ts} new: {self.local_run.instances_status_v7.status_ts}', level='INFO')
            _log('PBRemote', f'Sync v7 up: {self.name}', level='INFO')
            self.sync('up', 'run_v7')
            _log('PBRemote', f'Sync status_v7.json up: {self.name}', level='INFO')
            self.sync('up', 'status_v7')

    def sync_api_up(self):
        """Takes the api-keys.json from passivbot folder to sync it to other remotes by putting it in data/cmd/api-keys.json."""
        pbgdir = Path.cwd()
        api_file = Path(f'{pbgdir}/data/cmd/api-keys.json')
        source = Path(f'{self.pb7dir}/api-keys.json') if self.pb7dir else None
        if source and source.exists():
            _log('PBRemote', 'Sync api-keys.json to all remote servers', level='INFO')
            shutil.copy(source, api_file)
    
    def check_if_api_synced(self):
        """Verify that the API keys are the same in PBGUI and PB folders and deletes PBGUI folder's file if api are synced."""
        for server in self.remote_servers:
            server.load()
            if not server.is_api_md5_same(self.api_md5):
                return False
        pbgdir = Path.cwd()
        api_file = Path(f'{pbgdir}/data/cmd/api-keys.json')
        if api_file.exists():
            api_file.unlink(missing_ok=True)
        return True

    def load_monitor(self):
        monitor = []
        pbgdir = Path.cwd()
        path_v7 = PurePath(f'{pbgdir}/data/run_v7/')
        for instance in self.local_run.instances_status_v7.instances:
            if instance.running:
                monitor_file = Path(f'{path_v7}/{instance.name}/monitor.json')
                if Path(monitor_file).exists():
                    try:
                        with open(monitor_file, "r", encoding='utf-8') as f:
                            monitor.append(json.load(f))
                    except Exception as e:
                        _log('PBRemote', f'load_monitor: skipping corrupt {monitor_file}: {e}', level='WARNING')
        return monitor

    def alive(self):
        """
        Saves system informations like the name, memory, swaps, disk space and cpu usage to an alive file that is then synchronised with rclone from local to the remote storage.
        If there are more than 9 alive files, it will delete the oldest one.
        """
        timestamp = round(datetime.now().timestamp())
        if timestamp - self.systemts > 3600:
            self.local_run.has_upgrades()
            self.local_run.has_reboot()
            self.local_run.fetch_cmc_credits()
            self.systemts = timestamp
        self.local_run.load_versions()
        self.local_run.load_git_commits()
        if hasattr(self.local_run, 'update_pb7_python_version'):
            self.local_run.update_pb7_python_version()
        if timestamp - self.alivets < 60:
            return
        self.alivets = timestamp
        # self.mem = psutil.virtual_memory()
        # self.swap = psutil.swap_memory()
        # self.disk = psutil.disk_usage('/')
        # self.cpu = psutil.cpu_percent()
        # self.boot = psutil.boot_time()
        # self.monitor = self.load_monitor()
        cfg = ({
            "timestamp": timestamp,
            "startts": self.startts,
            "name": self.name,
            "api_md5": self.api_md5,
            "mem": self.mem,
            "swap": self.swap,
            "disk": self.disk,
            "cpu": self.cpu,
            "boot": self.boot,
            "monitor": self.monitor,
            "upgrades": self.local_run.upgrades,
            "reboot": self.local_run.reboot,
            "role": self.role,
            "cmc": self.local_run.coindata.credits_left,
            "pbgv": self.local_run.pbgui_version,
            "pbgc": self.local_run.pbgui_commit,
            "pbgb": getattr(self.local_run, 'pbgui_branch', 'unknown'),
            "pbgpy": getattr(self.local_run, 'pbgui_python', 'N/A'),
            "pb7v": self.local_run.pb7_version,
            "pb7c": self.local_run.pb7_commit,
            "pb7b": getattr(self.local_run, 'pb7_branch', 'unknown'),
            "pb7py": getattr(self.local_run, 'pb7_python', 'N/A'),
            })
        # Save the JSON data as a gzip file
        cfile = Path(f'{self.cmd_path}/alive_{timestamp}.cmd.gz')
        with gzip.open(cfile, "wt", encoding='utf-8') as f:
            json.dump(cfg, f)
        # with open(cfile, "w", encoding='utf-8') as f:
        #     json.dump(cfg, f)
        self.sync('up', 'cmd')
        p = str(Path(f'{self.cmd_path}/alive_*.cmd*'))
        found_local = glob.glob(p)
        found_local.sort()
        while len(found_local) > 9:
            local = Path(found_local.pop(0))
            local.unlink(missing_ok=True)

    def calculate_api_md5(self):
        """Makes a md5 hash from the api-keys.json in passivbot v7 folder."""
        file = Path(f'{self.pb7dir}/api-keys.json') if self.pb7dir else None
        if file and file.exists():
            with open(file, 'rb') as file_obj:
                file_contents = file_obj.read()
            return hashlib.md5(file_contents).hexdigest()
        return None

    def load_remote(self):
        """
        Loads every cmd files and create a new RemoteServer instance for each new possible instances, and tries to start instances with load_instances(). 
        It then adds the RemoteServer to remote_servers if the RemoteServer exists.
        """
        pbgdir = Path.cwd()
        self.remote_servers = []
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
            rserver.pb7dir = self.pb7dir
            rserver.bucket = self.bucket_dir
            rserver.pbname = self.name
            rserver.load()
            _log('PBRemote', f'Add Server: {rserver.name}', level='INFO')
            self.add(rserver)

    def update_remote_servers(self):
        """
        Loads every cmd files and create a new RemoteServer instance for each new possible instances, and tries to start instances with load_instances(). 
        It then adds the RemoteServer to remote_servers if the RemoteServer exists.
        """
        pbgdir = Path.cwd()
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
            rserver.pb7dir = self.pb7dir
            rserver.bucket = self.bucket_dir
            rserver.pbname = self.name
            rserver.load()
            add = True
            for server in self.remote_servers:
                if rserver.name == server.name:
                    add = False
            if add:
                _log('PBRemote', f'Add New Server: {rserver.name}', level='INFO')
                self.add(rserver)
        # Remove servers that are not in the remote anymore
        for server in list(self.remote_servers):
            if not Path(f'{pbgdir}/data/remote/cmd_{server.name}').exists():
                _log('PBRemote', f'Remove Server: {server.name}', level='INFO')
                self.remove(server)

    def run(self):
        """Starts PBRemote in unbuffered mode, and send an error message if it does not open every 10 secondes."""
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
                    _log('PBRemote', 'Error: Can not start PBRemote', level='ERROR')
                    break
                sleep(2)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log('PBRemote', 'Stop: PBRemote', level='INFO')
            try:
                psutil.Process(self.my_pid).kill()
            except psutil.NoSuchProcess:
                pass

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
                pid = f.read().strip()
                try:
                    self.my_pid = int(pid) if pid.isnumeric() else None
                except ValueError:
                    self.my_pid = None

    def save_pid(self):
        self.my_pid = os.getpid()
        tmp_path = self.pidfile.with_suffix(self.pidfile.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            f.write(str(self.my_pid))
        tmp_path.replace(self.pidfile)

    def load_config(self):
        """Load the bucket name used in the remote storage from pbgui.ini."""
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_section("pbremote"):
            if pb_config.has_option("pbremote", "bucket"):
                self.bucket = pb_config.get("pbremote", "bucket")
            else:
                self.bucket = None

    def save_config(self):
        """Save the bucket name used in the remote storage in pbgui.ini."""
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("pbremote"):
            pb_config.add_section("pbremote")
        pb_config.set("pbremote", "bucket", self.bucket)
        with open('pbgui.ini', 'w') as configfile:
            pb_config.write(configfile)

    def is_rclone_installed(self):
        """Checks the installation by running 'rclone version' as a process."""
        cmd = ['rclone', 'version']
        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return True
        except FileNotFoundError:
            return False
    
    def fetch_buckets(self):
        """Checks if rclone is installed and return all the buckets available in the remote server as an array."""
        if self.is_rclone_installed():
            cmd = ['rclone', 'listremotes']
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode == 0:
                    self.buckets = result.stdout.splitlines()
                    return True
            except Exception as e:
                return False
        return False

    def fetch_bucket_config(self):
        """Checks if rclone is installed and return all the buckets available in the remote server as an array."""
        if self.is_rclone_installed():
            cmd = ['rclone', 'config', 'dump']
            rcfile = None
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode == 0:
                    rcfile = result.stdout
            except FileNotFoundError:
                pass
            if not rcfile:
                return None
            config = json.loads(rcfile)
            bucket = self.bucket[0:-1]
            if bucket in config:
                bconfig = config[bucket]
                if "type" in bconfig:
                    self.bucket_type = bconfig["type"]
                if "endpoint" in bconfig:
                    self.bucket_endpoint = bconfig["endpoint"]
                if "no_check_bucket" in bconfig:
                    self.bucket_no_check_bucket = bconfig["no_check_bucket"]
                if "access_key_id" in bconfig:
                    self.bucket_access_key_id = bconfig["access_key_id"]
                if "provider" in bconfig:
                    self.bucket_provider = bconfig["provider"]
                if "region" in bconfig:
                    self.bucket_region = bconfig["region"]
                if "secret_access_key" in bconfig:
                    self.bucket_secret_access_key = bconfig["secret_access_key"]
                return bconfig
        return None
    
    def save_bucket_config(self):
        """Checks if rclone is installed and save the bucket configuration."""
        if self.is_rclone_installed():
            cmd = [
                'rclone', 'config', 'create',
                self.bucket[0:-1],
                self.bucket_type,
                f'provider={self.bucket_provider}',
                f'region={self.bucket_region}',
                f'endpoint={self.bucket_endpoint}',
                f'no_check_bucket={self.bucket_no_check_bucket}',
                f'access_key_id={self.bucket_access_key_id}',
                f'secret_access_key={self.bucket_secret_access_key}'
                ]
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode == 0:
                    return True, result.stdout
                else:
                    return False, result.stderr
            except Exception as e:
                return False, f'Error: {e}'

    def test_bucket(self):
        """Tests the bucket configuration by running 'rclone ls' as a process."""
        cmd = ['rclone', 'ls', self.bucket]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
        except Exception as e:
            return False, f'Error: {e}'
    
    def delete_bucket(self):
        """Deletes the bucket configuration by running 'rclone config delete' as a process."""
        cmd = ['rclone', 'config', 'delete', self.bucket[0:-1]]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
        except Exception as e:
            return False, f'Error: {e}'

def main():
    """
    Main function of PBRemote, responsible for sharing data from one server to another.

    ### Usage : 
    - Run PBRemote and save its process ID to pbremote.pid.
    - Logs in pbgui/data/logs/PBRemote.log (rotation handled by logging_helpers).
    """
    remote = PBRemote()
    if remote.is_running():
        _log('PBRemote', 'Error: PBRemote already started', level='ERROR')
        sys.exit(1)
    if not remote.bucket:
        _log('PBRemote', f'Error: {remote.error}', level='ERROR')
        sys.exit(1)
    _log('PBRemote', f'Start: PBRemote {remote.bucket}', level='INFO')
    remote.save_pid()
    remote.startts = round(datetime.now().timestamp())
    while True:
        try:
            remote.sync_v7_up()
            remote.check_if_api_synced()
            remote.sync_status_down()
            remote.update_remote_servers()
            remote.alive()
            for server in remote.remote_servers:
                for s in remote.remote_servers:
                    s.load()
                server.sync_v7_down(remote.role)
                server.sync_api()
        except Exception as e:
            _log('PBRemote', f'Something went wrong, but continue: {e}', level='ERROR')
            _log('PBRemote', 'PBRemote main loop traceback', level='DEBUG', meta={'traceback': traceback.format_exc()})

if __name__ == '__main__':
    main()