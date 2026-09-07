"""PBCluster lightweight Cluster Sync daemon."""

from __future__ import annotations

import argparse
import os
import platform
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from credential_process_registry import ProcessCapabilityHeartbeat
from master.cluster_sync_worker import ClusterSyncWorker
from pbgui_purefunc import PBGDIR
from process_identity import (
    ProcessIdentity,
    clear_process_identity,
    identities_match,
    process_identity,
    read_process_identity,
    signal_exact_process,
    write_process_identity,
)
from secure_files import atomic_write_private_text

SERVICE = "PBCluster"
_RUNTIME_SERIAL_POLL_SECONDS = 1.0


def _atomic_write_text(path: Path, value: str) -> None:
    """Atomically write one small text file."""
    atomic_write_private_text(path, value)


class PBCluster:
    """Owns the PBCluster daemon lifecycle."""

    def __init__(self, pbgdir: Path | str | None = None, *, interval: int = 60, boot_window: int = 20) -> None:
        """Initialize service paths and worker settings."""

        self.pbgdir = Path(pbgdir or PBGDIR)
        self.pidfile = self.pbgdir / "data" / "pid" / "pbcluster.pid"
        self.worker = ClusterSyncWorker(self.pbgdir, interval=interval, boot_window=boot_window)
        self.runtime_serial = self._read_runtime_serial()

    def run(self) -> None:
        """Start PBCluster in the background for legacy starter.py usage."""

        if self.is_running():
            return
        cmd = [sys.executable, "-u", str(self.pbgdir / "PBCluster.py")]
        if platform.system() == "Windows":
            creationflags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
            subprocess.Popen(cmd, stdout=None, stderr=None, cwd=self.pbgdir, text=True, creationflags=creationflags)
        else:
            subprocess.Popen(cmd, stdout=None, stderr=None, cwd=self.pbgdir, text=True, start_new_session=True)
        for _ in range(6):
            time.sleep(1)
            if self.is_running():
                return
        _log(SERVICE, "Can not start PBCluster", level="ERROR")

    def run_foreground(self) -> None:
        """Run PBCluster until it receives SIGTERM or SIGINT."""

        with advisory_file_lock(self.pidfile):
            if self.is_running():
                _log(SERVICE, "Already running - exit", level="INFO")
                return
            try:
                _atomic_write_text(self.pidfile, f"{os.getpid()}\n")
                identity = process_identity(os.getpid())
                if identity is None:
                    raise RuntimeError("Could not persist PBCluster process identity")
                write_process_identity(self.pidfile, identity)
            except Exception:
                if self._read_pid() == os.getpid():
                    self.pidfile.unlink(missing_ok=True)
                    clear_process_identity(self.pidfile)
                raise
        watcher_stop = threading.Event()
        reload_requested = threading.Event()

        def _handle_stop(_signum, _frame) -> None:
            self.worker.stop()

        def _watch_runtime_serial() -> None:
            while not watcher_stop.wait(_RUNTIME_SERIAL_POLL_SECONDS):
                current = self._read_runtime_serial()
                if current is None:
                    continue
                if self.runtime_serial is None:
                    self.runtime_serial = current
                    continue
                if current == self.runtime_serial:
                    continue
                reload_requested.set()
                self.worker.stop()
                _log(SERVICE, f"Runtime serial changed from {self.runtime_serial} to {current}; reloading PBCluster")
                return

        signal.signal(signal.SIGTERM, _handle_stop)
        signal.signal(signal.SIGINT, _handle_stop)
        watcher = threading.Thread(target=_watch_runtime_serial, name="pbcluster-runtime-serial", daemon=False)
        watcher.start()
        with ProcessCapabilityHeartbeat(self.pbgdir, SERVICE):
            try:
                self.worker.run_forever()
            finally:
                watcher_stop.set()
                watcher.join(timeout=max(2.0, _RUNTIME_SERIAL_POLL_SECONDS * 2))
                try:
                    with advisory_file_lock(self.pidfile):
                        if self._read_pid() == os.getpid():
                            self.pidfile.unlink(missing_ok=True)
                            clear_process_identity(self.pidfile)
                except OSError:
                    pass
        if reload_requested.is_set():
            self._reload_process()

    def _read_runtime_serial(self) -> str | None:
        """Read the deployment serial without treating a transient missing file as a change."""

        try:
            value = (self.pbgdir / "api" / "serial.txt").read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return value or None

    def _reload_process(self) -> None:
        """Replace this daemon with the current on-disk PBCluster runtime."""

        script = str(self.pbgdir / "PBCluster.py")
        os.execv(sys.executable, [sys.executable, "-u", script, *sys.argv[1:]])

    def run_once(self) -> dict:
        """Run one local sync pass for diagnostics or tests."""

        return self.worker.run_once(reason="manual")

    def stop(self) -> None:
        """Stop a running PBCluster process recorded in the pid file."""

        with advisory_file_lock(self.pidfile):
            pid = self._read_pid()
            if pid is None:
                return
            if pid == os.getpid():
                self.worker.stop()
                return
            owner = self._process_identity(pid)
            if owner is None:
                return
            persisted = read_process_identity(self.pidfile)
            if persisted is not None and not identities_match(persisted, owner):
                return
            if persisted is None:
                write_process_identity(self.pidfile, owner)
        signal_exact_process(owner, signal.SIGTERM)

    def is_running(self) -> bool:
        """Return whether the pid file points to a live PBCluster process."""

        pid = self._read_pid()
        if pid is None:
            return False
        if self._pid_matches(pid):
            return True
        try:
            if not psutil.pid_exists(pid):
                with advisory_file_lock(self.pidfile):
                    if self._read_pid() == pid:
                        self.pidfile.unlink(missing_ok=True)
                        clear_process_identity(self.pidfile)
        except OSError:
            pass
        return False

    def _read_pid(self) -> int | None:
        """Read the current positive PID value without changing ownership state."""
        try:
            pid = int(self.pidfile.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return None
        return pid if pid > 0 else None

    def _pid_matches(self, pid: int) -> bool:
        """Verify that PID executes this installation's PBCluster script."""
        current = self._process_identity(pid)
        if current is None:
            return False
        persisted = read_process_identity(self.pidfile)
        return persisted is None or identities_match(persisted, current)

    def _process_identity(self, pid: int) -> ProcessIdentity | None:
        """Return PID/create-time only for this installation's PBCluster script."""
        try:
            process = psutil.Process(pid)
            identity = ProcessIdentity(int(pid), float(process.create_time()))
            cwd = Path(process.cwd())
            expected = (self.pbgdir / "PBCluster.py").resolve(strict=False)
            for raw_arg in process.cmdline():
                arg = Path(str(raw_arg))
                if arg.name.lower() != "pbcluster.py":
                    continue
                candidate = arg if arg.is_absolute() else cwd / arg
                if candidate.resolve(strict=False) == expected:
                    return identity
        except (OSError, psutil.Error, ValueError):
            return None
        return None


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point."""

    parser = argparse.ArgumentParser(description="PBGui Cluster Sync daemon")
    parser.add_argument("--once", action="store_true", help="Run one local sync pass and exit")
    parser.add_argument("--interval", type=int, default=60, help="Periodic sync interval in seconds")
    parser.add_argument("--boot-window", type=int, default=20, help="Reserved boot sync window in seconds")
    args = parser.parse_args(argv)

    service = PBCluster(interval=args.interval, boot_window=args.boot_window)
    if args.once:
        with ProcessCapabilityHeartbeat(service.pbgdir, SERVICE):
            result = service.run_once()
        return 0 if result.get("ok") or result.get("status") == "not_configured" else 1
    service.run_foreground()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
