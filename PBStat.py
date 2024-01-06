import psutil
import subprocess
import sys
from pathlib import Path, PurePath
from time import sleep
from io import TextIOWrapper
from datetime import datetime
from Instance import Instances

class PBStat(Instances):
    def __init__(self):
        super().__init__()

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBStat.py')]
            subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBStat')
            self.pid().kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

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
            if any("PBStat.py" in sub for sub in cmdline):
                return process

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
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch status')
        for instance in self.instances:
            if instance.exchange.id in ["bybit", "bitget", "binance", "kucoinfutures", "okx"]:
                instance.save_status()

def main():
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBStat.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBStat')
    run = PBStat()
    trade_count = 0
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            if trade_count%5 == 0:
                run.fetch_all()
            else:
                run.fetch_status()
            trade_count += 1
            if len(run.instances) < 20:
                sleep(60)
        except Exception as e:
            print(f'Something went wrong, but continue {e}')

if __name__ == '__main__':
    main()