import psutil
import subprocess
import sys
import os
from pathlib import Path, PurePath
from time import sleep
from Instance import Instances
import platform
import traceback
from logging_helpers import human_log as _log

class PBStat(Instances):
    def __init__(self):
        super().__init__()
        pbgdir = Path.cwd()
        self.piddir = Path(f'{pbgdir}/data/pid')
        self.piddir.mkdir(parents=True, exist_ok=True)
        self.pidfile = Path(f'{self.piddir}/pbstat.pid')
        self.my_pid = None

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBStat.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 10:
                    _log('PBStat', 'Error: Can not start PBStat', level='ERROR')
                    break
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log('PBStat', 'Stop: PBStat', level='INFO')
            try:
                psutil.Process(self.my_pid).kill()
            except psutil.NoSuchProcess:
                pass

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbstat.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

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

    def fetch_all(self):
        self.fetch_status()
        _log('PBStat', 'Fetch trades and funding fees', level='INFO')
        for instance in self.instances:
            if instance.market_type == "spot":
                instance.save_status()
                instance.fetch_trades()
    def fetch_status(self):
        _log('PBStat', 'Start Fetch status', level='INFO')
        for instance in self.instances:
            if instance.market_type == "spot":
                _log('PBStat', f'Start Save Status {instance.user} {instance.symbol}', level='DEBUG')
                instance.save_status()
        _log('PBStat', 'End Fetch status', level='INFO')

def main():
    stat = PBStat()
    if stat.is_running():
        _log('PBStat', 'Error: PBStat already started', level='ERROR')
        sys.exit(1)
    _log('PBStat', 'Start: PBStat', level='INFO')
    stat.save_pid()
    trade_count = 0
    while True:
        try:
            if trade_count % 5 == 0:
                stat.fetch_all()
            else:
                stat.fetch_status()
            trade_count += 1
            sleep(60)
            # Refresh Instances if there are some new or removed
            stat.instances = []
            stat.load()
        except Exception as e:
            _log('PBStat', f'Something went wrong, but continue: {e}', level='WARNING')
            _log('PBStat', 'PBStat main loop traceback', level='DEBUG', meta={'traceback': traceback.format_exc()})

if __name__ == '__main__':
    main()