import psutil
import subprocess
import sys
import os
from pathlib import Path, PurePath
from time import sleep
from io import TextIOWrapper
from datetime import datetime
from Instance import Instances
import platform
import traceback

class PBStat(Instances):
    def __init__(self):
        super().__init__()
        pbgdir = Path.cwd()
        self.piddir = Path(f'{pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
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
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBStat')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBStat')
            psutil.Process(self.my_pid).kill()

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
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))

    def fetch_all(self):
        self.fetch_status()
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch trades and funding fees')
        for instance in self.instances:
#            if instance.exchange.id in ["bybit", "bitget", "binance", "kucoinfutures", "bingx"]:
            if instance.exchange.id in ["bybit", "bitget", "binance", "kucoinfutures", "okx"]:
                instance.save_status()
                instance.fetch_trades()
                instance.fetch_fundings()

    def fetch_status(self):
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start Fetch status')
        for instance in self.instances:
            if instance.exchange.id in ["bybit", "bitget", "binance", "kucoinfutures", "okx"]:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start Save Status {instance.user} {instance.symbol}')
                instance.save_status()
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} End Fetch status')

def main():
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBStat.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBStat')
    stat = PBStat()
    if stat.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBStat already started')
        exit(1)
    stat.save_pid()
    trade_count = 0
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            if trade_count%5 == 0:
                stat.fetch_all()
            else:
                stat.fetch_status()
            trade_count += 1
            if len(stat.instances) < 20:
                sleep(60)
            # Refresh Instances if there are some new or removed
            stat.instances = []
            stat.load()
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()

if __name__ == '__main__':
    main()