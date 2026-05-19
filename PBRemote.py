"""PBRemote manages fallback cloud storage sync for PB config/status data."""
import psutil
import subprocess
import configparser
import os
import sys
from pathlib import Path, PurePath
from time import monotonic, sleep
import glob
import json
from datetime import datetime
import platform
from PBRun import PBRun
from Status import InstancesStatus
import shutil
import traceback
from logging_helpers import (
    human_log as _log,
    get_rotate_settings,
    rotate_logfile_if_oversize,
)


def _prepare_sync_log(logfile: Path) -> None:
    max_bytes, backup_count = get_rotate_settings(logfile=logfile)
    rotate_logfile_if_oversize(str(logfile), max_bytes, backup_count)


class RemoteServer():
    def __init__(self, path: str):
        """
        Initialize a RemoteServer instance to manage v7 PB configurations.

        Args:
            path (str): Path to the remote server configuration.
        """
        self._name = None
        self._edit = False
        self._path = path
        self._unique = []
        self._bucket = None
        self._role = None
        self.pbname = None
        self.instances_status_v7 = InstancesStatus(f'{self.path}/status_v7.json')
        self.instances_status_v7.load()

    @property
    def name(self): return self._name
    @property
    def edit(self): return self._edit
    @property
    def path(self): return self._path
    @property
    def bucket(self): return self._bucket
    @property
    def role(self): return self._role

    @name.setter
    def name(self, new_name):
        if self._name != new_name:
            self._name = new_name
    @edit.setter
    def edit(self, new_edit):
        if self._edit != new_edit:
            self._edit = new_edit
    @path.setter
    def path(self, new_path):
        if self._path != new_path:
            self._path = new_path
    @bucket.setter
    def bucket(self, new_bucket):
        if self._bucket != new_bucket:
            self._bucket = new_bucket

    def load(self):
        """
        Load the server's configuration.
        """
        self._name = PurePath(self._path).name[4:]
        self._role = None

    def sync_v7_down(self, role: str):
        """Sync v7 configs from remote storage to local machine."""
        if self.instances_status_v7.has_new_status():
            _log('PBRemote', f'New status_v7.json from: {self.name}', level='INFO')
            _log('PBRemote', f'Sync v7 from: {self.name}', level='INFO')
            pbgdir = Path.cwd()
            if role == "master":
                cmd = ['rclone', 'sync', '-v', '--filter', f'- cmd_{self.pbname}/*', '--filter', f'+ run_v7_{self.name}/**/*.json', '--filter', '- *', f'{self.bucket}', PurePath(f'{pbgdir}/data/remote')]
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
    def pb7_version(self):
        return "N/A"
    @property
    def pb7_commit(self):
        return ""
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
            up/status_v7: status_v7.json
            up/run_v7: *.json (v7 configs)
            down/master: all except own cmd, instances, multi, run_v7
            down/slave: same without pulling other hosts' cmd payloads
        
        Args:
            direction (str): Either "up" (local to remote) or "down" (remote to local).
            spath (str): The specific path to synchronize.
        """
        pbgdir = Path.cwd()
        cmd = None
        if direction == 'up' and spath == 'status_v7':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{status_v7.json}}', PurePath(f'{pbgdir}/data/cmd'), f'{self.bucket_dir}/cmd_{self.name}']
        elif direction == 'up' and spath == 'run_v7':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{*.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'{self.bucket_dir}/{spath}_{self.name}']
        elif direction == 'down' and spath == 'master':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{cmd_{self.name}/*,instances_**,multi_**,run_v7_**}}', f'{self.bucket_dir}', PurePath(f'{pbgdir}/data/remote')]
        elif direction == 'down' and spath == 'slave':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{cmd_{self.name}/*,instances_**,multi_**,run_v7_**}}', f'{self.bucket_dir}', PurePath(f'{pbgdir}/data/remote')]
        if cmd is None:
            _log('PBRemote', f'sync() called with unknown combination: direction={direction!r} spath={spath!r}', level='ERROR')
            return
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        logfile.parent.mkdir(parents=True, exist_ok=True)
        _prepare_sync_log(logfile)
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
            # Slave: nothing to do.
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

    def cleanup_legacy_cmd_artifacts_once(self):
        pbgdir = Path.cwd()
        marker = Path(f'{pbgdir}/data/state/pbremote/pbremote_legacy_cmd_cleanup.json')
        legacy_marker = Path(f'{pbgdir}/data/pbremote_legacy_cmd_cleanup.json')
        if not marker.exists() and legacy_marker.exists():
            marker = legacy_marker
        if marker.exists():
            return

        _log('PBRemote', 'Run one-time cleanup for legacy cmd artifacts', level='INFO')
        local_removed = 0
        cleanup_failed = False

        for pattern in (
            f'{pbgdir}/data/cmd/alive_*.cmd*',
            f'{pbgdir}/data/cmd/status.json',
            f'{pbgdir}/data/cmd/status_single.json',
        ):
            for alive_path in glob.glob(pattern):
                try:
                    Path(alive_path).unlink(missing_ok=True)
                    local_removed += 1
                except Exception:
                    cleanup_failed = True
                    _log('PBRemote', f'Failed to remove legacy cmd artifact: {alive_path}', level='ERROR', meta={'traceback': traceback.format_exc()})

        if cleanup_failed:
            return

        marker.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = marker.with_suffix('.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            json.dump(
                {
                    'completed_at': datetime.now().isoformat(timespec='seconds'),
                    'local_removed': local_removed,
                },
                f,
                indent=4,
            )
        tmp_path.replace(marker)
        _log('PBRemote', f'Finished one-time legacy cmd cleanup (local_removed={local_removed})', level='INFO')

    def load_remote(self):
        """
        Load remote cmd directories and create one RemoteServer view per host.
        """
        pbgdir = Path.cwd()
        self.remote_servers = []
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
            rserver.bucket = self.bucket_dir
            rserver.pbname = self.name
            rserver.load()
            _log('PBRemote', f'Add Server: {rserver.name}', level='INFO')
            self.add(rserver)

    def update_remote_servers(self):
        """
        Refresh remote cmd-directory views.
        """
        pbgdir = Path.cwd()
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
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

    def list_bucket_entries(self):
        """List top-level bucket entries using rclone lsf."""
        target = self.bucket_dir if getattr(self, 'bucket_dir', None) else self.bucket
        cmd = ['rclone', 'lsf', target]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
            if result.returncode == 0:
                entries = [line.strip().rstrip('/') for line in str(result.stdout or '').splitlines() if line.strip()]
                return True, entries
            return False, result.stderr
        except Exception as e:
            return False, f'Error: {e}'

    def cleanup_bucket_host_entries(self, hostname: str):
        """Delete fallback bucket entries for a single host."""
        host = str(hostname or '').strip()
        if not host:
            return {"ok": False, "hostname": host, "error": "Hostname is required.", "deleted": [], "operations": []}
        target = self.bucket_dir if getattr(self, 'bucket_dir', None) else self.bucket
        deleted = []
        errors = []
        operations = []
        for prefix in (f'cmd_{host}/**', f'run_v7_{host}/**'):
            cmd = ['rclone', 'delete', '-v', target, '--include', prefix]
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
                stdout_text = str(result.stdout or '').strip()
                stderr_text = str(result.stderr or '').strip()
                operations.append({
                    "prefix": prefix,
                    "command": list(cmd),
                    "returncode": int(result.returncode),
                    "stdout": stdout_text,
                    "stderr": stderr_text,
                    "ok": result.returncode == 0,
                })
                if result.returncode == 0:
                    deleted.append(prefix)
                else:
                    detail = stderr_text or stdout_text or 'rclone delete failed'
                    errors.append(f'{prefix}: {detail}')
            except Exception as e:
                operations.append({
                    "prefix": prefix,
                    "command": list(cmd),
                    "returncode": None,
                    "stdout": "",
                    "stderr": str(e),
                    "ok": False,
                })
                errors.append(f'{prefix}: {e}')
        return {
            "ok": not errors,
            "hostname": host,
            "deleted": deleted,
            "error": ' | '.join(errors),
            "operations": operations,
        }

    def cleanup_bucket_host_entries_dry_run(self, hostname: str):
        """Preview bucket objects matched by the cleanup rules for one host."""
        host = str(hostname or '').strip()
        if not host:
            return {"ok": False, "hostname": host, "error": "Hostname is required.", "matches": []}
        target = self.bucket_dir if getattr(self, 'bucket_dir', None) else self.bucket
        matches: list[str] = []
        errors: list[str] = []
        for prefix in (f'cmd_{host}/**', f'run_v7_{host}/**'):
            cmd = ['rclone', 'lsf', target, '--recursive', '--include', prefix]
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
                if result.returncode != 0:
                    detail = str(result.stderr or result.stdout or '').strip() or 'rclone dry-run failed'
                    errors.append(f'{prefix}: {detail}')
                    continue
                for line in str(result.stdout or '').splitlines():
                    entry = str(line or '').strip()
                    if not entry:
                        continue
                    matches.append(entry.rstrip('/'))
            except Exception as e:
                errors.append(f'{prefix}: {e}')
        deduped = sorted(dict.fromkeys(matches))
        return {
            "ok": not errors,
            "hostname": host,
            "matches": deduped,
            "error": ' | '.join(errors),
        }
    
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
    Main function of PBRemote, responsible for fallback config/status sync.

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
    remote.cleanup_legacy_cmd_artifacts_once()

    def _apply_remote_updates():
        remote.update_remote_servers()
        for server in remote.remote_servers:
            for s in remote.remote_servers:
                s.load()
            server.sync_v7_down(remote.role)

    remote.sync_status_down()
    _apply_remote_updates()

    local_sync_interval = 5.0
    remote_sync_interval = 15.0
    next_local_sync = monotonic() + local_sync_interval
    next_remote_sync = monotonic() + remote_sync_interval

    while True:
        try:
            now = monotonic()

            if now >= next_local_sync:
                remote.sync_v7_up()
                next_local_sync = now + local_sync_interval

            if now >= next_remote_sync:
                if not remote.is_sync_running():
                    remote.sync_status_down()
                    _apply_remote_updates()
                next_remote_sync = now + remote_sync_interval

            sleep(1)
        except Exception as e:
            _log('PBRemote', f'Something went wrong, but continue: {e}', level='ERROR')
            _log('PBRemote', 'PBRemote main loop traceback', level='DEBUG', meta={'traceback': traceback.format_exc()})
            sleep(1)

if __name__ == '__main__':
    main()
