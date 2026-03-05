"""
PBApiServer — FastAPI REST + WebSocket server for PBGui.

Provides:
- REST API for job operations (list, cancel, delete, retry, requeue)
- WebSocket endpoint for real-time job updates
- Static file serving for Vanilla JS frontend
- Token-based authentication

Runs as a background daemon, following the same pattern as PBRun/PBMaster etc.
"""

import os
import subprocess
import sys
from pathlib import Path
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

    # ── Daemon lifecycle ──

    def is_running(self) -> bool:
        """Check if the API server daemon is running."""
        if not self.pidfile.exists():
            return False
        try:
            with open(self.pidfile, 'r') as f:
                pid = int(f.read().strip())
            # Check if process exists and matches our command
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                cmdline = ' '.join(proc.cmdline())
                if 'api_server.py' in cmdline:
                    return True
            # Stale PID file
            self.pidfile.unlink(missing_ok=True)
            return False
        except Exception:
            return False

    def run(self):
        """Start the API server daemon in the background."""
        if self.is_running():
            _log(SERVICE, "API server is already running", level="INFO")
            return

        venv_python = self._get_venv_python()
        api_script = Path(__file__).resolve().parent / "api_server.py"
        log_file = Path(PBGDIR) / "data" / "logs" / "api_server.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Set environment variables for config
        env = os.environ.copy()
        env["PBGUI_API_HOST"] = self.host
        env["PBGUI_API_PORT"] = str(self.port)

        try:
            # Start as detached background process.
            # Logging goes through human_log → data/logs/PBApiServer.log,
            # so stdout/stderr are discarded.
            proc = subprocess.Popen(
                [venv_python, str(api_script)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                start_new_session=True,  # Detach from parent
            )
            # Write PID file
            with open(self.pidfile, 'w') as f:
                f.write(str(proc.pid))
            self.my_pid = proc.pid

            _log(SERVICE, f"API server started (PID: {proc.pid}, {self.host}:{self.port})", level="INFO")
            sleep(2)  # Give it time to bind to port

            # Verify it's actually running
            if not self.is_running():
                _log(SERVICE, "API server failed to start - check logs", level="ERROR")
                return

        except Exception as e:
            _log(SERVICE, f"Failed to start API server: {e}", level="ERROR",
                 meta={'traceback': __import__('traceback').format_exc()})

    def stop(self):
        """Stop the API server daemon."""
        if not self.is_running():
            _log(SERVICE, "API server is not running", level="INFO")
            return

        try:
            with open(self.pidfile, 'r') as f:
                pid = int(f.read().strip())

            proc = psutil.Process(pid)
            proc.terminate()  # SIGTERM
            proc.wait(timeout=5)  # Wait for graceful shutdown

            self.pidfile.unlink(missing_ok=True)
            _log(SERVICE, f"API server stopped (PID: {pid})", level="INFO")

        except psutil.TimeoutExpired:
            # Force kill if it doesn't stop gracefully
            proc.kill()
            proc.wait()
            self.pidfile.unlink(missing_ok=True)
            _log(SERVICE, "API server force-killed (didn't respond to TERM)", level="WARNING")

        except Exception as e:
            _log(SERVICE, f"Error stopping API server: {e}", level="ERROR",
                 meta={'traceback': __import__('traceback').format_exc()})
            # Clean up stale PID file
            self.pidfile.unlink(missing_ok=True)

    def _get_venv_python(self) -> str:
        """Get the path to the virtual environment Python executable."""
        # Try pbgui venv first
        venv_candidates = [
            Path(f"{PBGDIR}/../venv_pbgui/bin/python"),
            Path(f"{PBGDIR}/../venv_pbgui312/bin/python"),
            Path(f"{PBGDIR}/../venv/bin/python"),
        ]
        for venv_py in venv_candidates:
            if venv_py.exists():
                return str(venv_py.resolve())
        # Fallback to system python
        return sys.executable
