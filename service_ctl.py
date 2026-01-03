#!/usr/bin/env python3
"""Small helper to start/stop/status PBGui background services.

Intended for migration scripts (py3.10 <-> py3.12) so we can:
- detect which services are running
- stop only those
- restart only those

Detection uses pidfiles in `./data/pid` and verifies the process cmdline
ends with the expected `*.py`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ServiceSpec:
    name: str
    script: str
    pidfile: str
    expected_py: str


SERVICES: dict[str, ServiceSpec] = {
    "PBRun": ServiceSpec("PBRun", "PBRun.py", "pbrun.pid", "pbrun.py"),
    "PBRemote": ServiceSpec("PBRemote", "PBRemote.py", "pbremote.pid", "pbremote.py"),
    "PBMon": ServiceSpec("PBMon", "PBMon.py", "pbmon.pid", "pbmon.py"),
    "PBStat": ServiceSpec("PBStat", "PBStat.py", "pbstat.pid", "pbstat.py"),
    "PBData": ServiceSpec("PBData", "PBData.py", "pbdata.pid", "pbdata.py"),
    "PBCoinData": ServiceSpec("PBCoinData", "PBCoinData.py", "pbcoindata.pid", "pbcoindata.py"),
}


def _pid_dir(root: Path) -> Path:
    return root / "data" / "pid"


def _read_pid(pidfile: Path) -> int | None:
    try:
        raw = pidfile.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return int(raw) if raw.isnumeric() else None


def _is_running(spec: ServiceSpec, root: Path) -> tuple[bool, int | None]:
    pid = _read_pid(_pid_dir(root) / spec.pidfile)
    if not pid:
        return (False, None)

    try:
        import psutil  # type: ignore

        if not psutil.pid_exists(pid):
            return (False, pid)

        proc = psutil.Process(pid)
        cmdline = []
        try:
            cmdline = proc.cmdline()
        except Exception:
            cmdline = []

        expected = spec.expected_py.lower()
        if any(str(arg).lower().endswith(expected) for arg in cmdline):
            return (True, pid)
        return (False, pid)
    except Exception:
        # If psutil isn't available, fall back to optimistic false.
        return (False, pid)


def cmd_status(root: Path, fmt: str) -> int:
    running = [name for name, spec in SERVICES.items() if _is_running(spec, root)[0]]

    if fmt == "space":
        sys.stdout.write(" ".join(running))
        if running:
            sys.stdout.write("\n")
    else:  # lines
        for name in running:
            sys.stdout.write(f"{name}\n")
    return 0


def cmd_stop(root: Path, names: list[str]) -> int:
    try:
        import psutil  # type: ignore
    except Exception:
        return 1

    rc = 0
    for name in names:
        spec = SERVICES.get(name)
        if not spec:
            rc = 2
            continue

        is_up, pid = _is_running(spec, root)
        if not is_up or not pid:
            continue
        try:
            psutil.Process(pid).kill()
        except Exception:
            rc = 1
    return rc


def cmd_start(root: Path, names: list[str]) -> int:
    rc = 0
    for name in names:
        spec = SERVICES.get(name)
        if not spec:
            rc = 2
            continue

        is_up, _pid = _is_running(spec, root)
        if is_up:
            continue

        script_path = root / spec.script
        if not script_path.exists():
            rc = 1
            continue

        try:
            subprocess.Popen(
                [sys.executable, "-u", str(script_path)],
                cwd=str(root),
                stdout=None,
                stderr=None,
                text=True,
                start_new_session=True,
            )
        except Exception:
            rc = 1
    return rc


def main() -> int:
    parser = argparse.ArgumentParser(prog="service_ctl.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="print running services")
    p_status.add_argument("--format", choices=["space", "lines"], default="space")

    p_stop = sub.add_parser("stop", help="stop services by name")
    p_stop.add_argument("services", nargs="+", choices=sorted(SERVICES.keys()))

    p_start = sub.add_parser("start", help="start services by name")
    p_start.add_argument("services", nargs="+", choices=sorted(SERVICES.keys()))

    args = parser.parse_args()

    root = Path.cwd()
    # require we are run from PBGUI_DIR (has PBRun.py etc)
    if not (root / "pbgui.py").exists():
        # try relative to this file
        root = Path(__file__).resolve().parent

    # Ensure pid dir exists
    _pid_dir(root).mkdir(parents=True, exist_ok=True)

    if args.cmd == "status":
        return cmd_status(root, args.format)
    if args.cmd == "stop":
        return cmd_stop(root, args.services)
    if args.cmd == "start":
        return cmd_start(root, args.services)

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
