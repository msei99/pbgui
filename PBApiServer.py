"""
PBApiServer — FastAPI REST + WebSocket server for PBGui.

Provides:
- REST API for job operations (list, cancel, delete, retry, requeue)
- WebSocket endpoint for real-time job updates
- Static file serving for Vanilla JS frontend
- Token-based authentication

Runs as a background daemon, following the same pattern as PBRun etc.
"""

import os
import subprocess
import sys
from pathlib import Path, PurePath
from time import sleep

import psutil

from pbgui_purefunc import PBGDIR, load_ini, save_ini
from logging_helpers import human_log as _log

SERVICE = "PBApiServer"


class PBApiServer:
    """
    FastAPI server daemon for PBGui.

    Lifecycle (same pattern as other services):
        api = PBApiServer()
        api.run()        # Start daemon in background
        api.stop()       # Stop daemon
        api.is_running() # Check if running
        api.restart()    # Stop + start
    """

    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/api_server.pid')
        self.my_pid = None

        # Configuration (persisted in pbgui.ini)
        self._host = None
        self._port = None

    # ── Configuration properties (persisted in pbgui.ini) ──

    @property
    def host(self) -> str:
        """Bind address (0.0.0.0 = all interfaces, 127.0.0.1 = localhost only)."""
        if self._host is None:
            val = load_ini("api_server", "host")
            self._host = val.strip() if val and val.strip() else "0.0.0.0"
        return self._host

    @host.setter
    def host(self, value: str):
        if self._host != value:
            self._host = value.strip() if value else "0.0.0.0"
            save_ini("api_server", "host", self._host)

    @property
    def port(self) -> int:
        """API server port (default: 8000)."""
        if self._port is None:
            val = load_ini("api_server", "port")
            self._port = int(val) if val and val.isdigit() else 8000
        return self._port

    @port.setter
    def port(self, value: int):
        if self._port != value:
            self._port = max(1024, min(65535, value))
            save_ini("api_server", "port", str(self._port))

    # ── PID management (same pattern as PBRun, PBData, etc.) ──

    def load_pid(self):
        """Load PID from pidfile into self.my_pid."""
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read().strip()
                try:
                    self.my_pid = int(pid) if pid.isnumeric() else None
                except ValueError:
                    self.my_pid = None

    def save_pid(self):
        """Write current process PID to pidfile. Called from the daemon process."""
        self.my_pid = os.getpid()
        tmp_path = self.pidfile.with_suffix(self.pidfile.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            f.write(str(self.my_pid))
        tmp_path.replace(self.pidfile)

    # ── Daemon lifecycle ──

    def is_running(self) -> bool:
        """Check if the API server daemon is running."""
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(
                sub.lower().endswith("api_server.py") for sub in psutil.Process(self.my_pid).cmdline()
            ):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def run(self):
        """Start the API server daemon in the background."""
        if not self.is_running():
            pbgdir = Path.cwd()
            venv_python = self._get_venv_python()
            cmd = [venv_python, '-u', str(PurePath(f'{pbgdir}/api_server.py'))]

            # Set environment variables for config
            env = os.environ.copy()
            env["PBGUI_API_HOST"] = self.host
            env["PBGUI_API_PORT"] = str(self.port)

            subprocess.Popen(
                cmd,
                stdout=None,
                stderr=None,
                cwd=pbgdir,
                text=True,
                env=env,
                start_new_session=True,
            )
            count = 0
            while True:
                if count > 5:
                    _log(SERVICE, 'Error: Can not start API server', level='ERROR')
                    break
                sleep(2)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        """Stop the API server daemon."""
        if self.is_running():
            _log(SERVICE, 'Stop: API server', level='INFO')
            try:
                psutil.Process(self.my_pid).terminate()
                psutil.Process(self.my_pid).wait(timeout=5)
            except psutil.TimeoutExpired:
                try:
                    psutil.Process(self.my_pid).kill()
                except psutil.NoSuchProcess:
                    pass
            except psutil.NoSuchProcess:
                pass
            self.pidfile.unlink(missing_ok=True)

    def restart(self):
        """Restart the API server daemon (stop if running, then start)."""
        if self.is_running():
            self.stop()
        self.run()

    def _get_venv_python(self) -> str:
        """Get the path to the virtual environment Python executable."""
        venv_candidates = [
            Path(f"{PBGDIR}/../venv_pbgui/bin/python"),
            Path(f"{PBGDIR}/../venv_pbgui312/bin/python"),
            Path(f"{PBGDIR}/../venv/bin/python"),
        ]
        for venv_py in venv_candidates:
            if venv_py.exists():
                # Return the venv path (not .resolve()) so that the
                # venv's site-packages are on sys.path at runtime.
                return str(venv_py)
        return sys.executable
