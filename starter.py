import argparse
import os
import subprocess
import sys
from pathlib import Path

from PBRun import PBRun
from PBRemote import PBRemote
from PBCoinData import CoinData

SYSTEMD_UNITS = {
    'PBRun': 'pbgui-pbrun.service',
    'PBRemote': 'pbgui-pbremote.service',
    'PBCoinData': 'pbgui-pbcoindata.service',
}


def _systemd_env():
    """Build an environment for the current user's systemd manager."""
    env = os.environ.copy()
    env.setdefault('XDG_RUNTIME_DIR', f'/run/user/{os.getuid()}')
    return env


def _systemd_unit_exists(unit: str) -> bool:
    """Return whether a PBGui systemd user unit exists on this host."""
    unit_path = Path.home() / '.config' / 'systemd' / 'user' / unit
    if unit_path.exists():
        return True
    try:
        result = subprocess.run(
            ['systemctl', '--user', 'show', unit, '-p', 'LoadState', '--value', '--no-pager'],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
            env=_systemd_env(),
        )
    except Exception:
        return False
    state = (result.stdout or '').strip()
    return result.returncode == 0 and bool(state) and state != 'not-found'


def _systemd_action(service: str, action: str) -> tuple[bool, bool]:
    """Run a service action through systemd when its user unit is installed."""
    unit = SYSTEMD_UNITS.get(service)
    if not unit or not _systemd_unit_exists(unit):
        return False, False
    try:
        result = subprocess.run(
            ['systemctl', '--user', action, unit],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=20,
            env=_systemd_env(),
        )
    except Exception:
        return True, False
    return True, result.returncode == 0


def _legacy_action(service: str, action: str) -> None:
    """Run a service action with the legacy PBGui process wrappers."""
    if action == 'start':
        if service == 'PBRun':
            print('Start PBRun')
            PBRun().run()
        elif service == 'PBRemote':
            print('Start PBRemote')
            PBRemote().run()
        elif service == 'PBCoinData':
            print('Start PBCoinData')
            CoinData().run()
    elif action == 'stop':
        if service == 'PBRun':
            print('Stop PBRun')
            PBRun().stop()
        elif service == 'PBRemote':
            print('Stop PBRemote')
            PBRemote().stop()
        elif service == 'PBCoinData':
            print('Stop PBCoinData')
            CoinData().stop()
    elif action == 'restart':
        if service == 'PBRun':
            print('Restart PBRun')
            PBRun().stop()
            PBRun().run()
        elif service == 'PBRemote':
            print('Restart PBRemote')
            PBRemote().stop()
            PBRemote().run()
        elif service == 'PBCoinData':
            print('Restart PBCoinData')
            CoinData().stop()
            CoinData().run()


def main():
    """Dispatch service actions through systemd when available, otherwise legacy wrappers."""
    parser = argparse.ArgumentParser(description='starter')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--start', action='store_true', help='Start')
    group.add_argument('-k', '--stop', action='store_true', help='Stop')
    group.add_argument('-r', '--restart', action='store_true', help='Restart')
    parser.add_argument('command', choices=['PBRun', 'PBRemote', 'PBCoinData'], nargs='+')

    args = parser.parse_args()
    action = 'start' if args.start else 'stop' if args.stop else 'restart'
    failed = False

    for service in args.command:
        handled, ok = _systemd_action(service, action)
        if handled:
            if not ok:
                failed = True
            continue
        _legacy_action(service, action)

    if failed:
        sys.exit(1)

if __name__ == '__main__':
    main()
