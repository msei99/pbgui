"""
Service Monitor for PBMaster.

Monitors PBRun, PBRemote, and PBCoinData services on remote VPS servers.
Checks if services are running via PID files and process checks.
Can auto-restart failed services.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from master.command_executor import CommandExecutor
from logging_helpers import human_log as _log


SERVICE = "PBMaster"

# Remote PBGui directory on VPS (relative to home)
REMOTE_PBGUI_DIR = "software/pbgui"


class ServiceStatus(Enum):
    """Status of a service on a VPS."""
    RUNNING = "running"
    STOPPED = "stopped"
    UNKNOWN = "unknown"
    RESTARTING = "restarting"


@dataclass
class ServiceInfo:
    """Information about a monitored service."""
    name: str
    pid_file: str          # Relative to PBGUI dir: data/pid/xxx.pid
    script_file: str       # Python script to run: PBRun.py
    process_match: str     # String to grep in process cmdline


# Services we monitor on each VPS
MONITORED_SERVICES = {
    "PBRun": ServiceInfo(
        name="PBRun",
        pid_file="data/pid/pbrun.pid",
        script_file="PBRun.py",
        process_match="pbrun.py",
    ),
    "PBRemote": ServiceInfo(
        name="PBRemote",
        pid_file="data/pid/pbremote.pid",
        script_file="PBRemote.py",
        process_match="pbremote.py",
    ),
    "PBCoinData": ServiceInfo(
        name="PBCoinData",
        pid_file="data/pid/pbcoindata.pid",
        script_file="PBCoinData.py",
        process_match="pbcoindata.py",
    ),
}


@dataclass
class ServiceCheckResult:
    """Result of checking a service on a VPS."""
    hostname: str
    service: str
    status: ServiceStatus
    pid: Optional[int] = None
    error: Optional[str] = None
    timestamp: Optional[datetime] = None
    was_restarted: bool = False


class ServiceMonitor:
    """
    Monitors and manages PBGui services on remote VPS servers.

    Usage:
        monitor = ServiceMonitor(executor)
        result = monitor.check_service("myvps", "PBRun")
        results = monitor.check_all("myvps")
        monitor.restart_service("myvps", "PBRun")
        monitor.auto_heal("myvps")       # Check + restart all failed
        monitor.auto_heal_all()           # All VPS
    """

    def __init__(self, executor: CommandExecutor, auto_restart: bool = True):
        self._executor = executor
        self._auto_restart = auto_restart
        # Track restart history to avoid restart loops
        self._restart_history: dict[str, dict[str, list[datetime]]] = {}
        # Max restarts per service per hour
        self.max_restarts_per_hour = 3

    def check_service(self, hostname: str, service_name: str) -> ServiceCheckResult:
        """
        Check if a specific service is running on a VPS.

        Args:
            hostname: VPS hostname
            service_name: One of "PBRun", "PBRemote", "PBCoinData"

        Returns:
            ServiceCheckResult
        """
        result = ServiceCheckResult(
            hostname=hostname,
            service=service_name,
            status=ServiceStatus.UNKNOWN,
            timestamp=datetime.now(),
        )

        svc = MONITORED_SERVICES.get(service_name)
        if not svc:
            result.error = f"Unknown service: {service_name}"
            _log(SERVICE, f"Unknown service: {service_name}", level="ERROR")
            return result

        pid_path = f"{REMOTE_PBGUI_DIR}/{svc.pid_file}"

        # Step 1: Read PID file
        pid = self._executor.read_pid_file(hostname, pid_path)
        if pid is None:
            result.status = ServiceStatus.STOPPED
            result.error = "No PID file or invalid PID"
            return result

        result.pid = pid

        # Step 2: Check if process is actually running with matching name
        if self._executor.is_process_running(hostname, pid, svc.process_match):
            result.status = ServiceStatus.RUNNING
        else:
            result.status = ServiceStatus.STOPPED
            result.error = f"PID {pid} not running or not matching {svc.process_match}"

        return result

    def check_all(self, hostname: str) -> dict[str, ServiceCheckResult]:
        """
        Check all monitored services on a single VPS.
        Returns dict of {service_name: ServiceCheckResult}.
        """
        results = {}
        for service_name in MONITORED_SERVICES:
            results[service_name] = self.check_service(hostname, service_name)
        return results

    def check_all_vps(self, hostnames: list[str]) -> dict[str, dict[str, ServiceCheckResult]]:
        """
        Check all services on all VPS servers.
        Returns {hostname: {service_name: ServiceCheckResult}}.
        """
        results = {}
        for hostname in hostnames:
            results[hostname] = self.check_all(hostname)
        return results

    def restart_service(self, hostname: str, service_name: str) -> bool:
        """
        Restart a service on a VPS.

        This starts the Python script as a background process on the VPS.
        The script manages its own PID file (same pattern as local Services).

        Returns True if restart command was sent successfully.
        """
        svc = MONITORED_SERVICES.get(service_name)
        if not svc:
            _log(SERVICE, f"Cannot restart unknown service: {service_name}", level="ERROR")
            return False

        if not self._can_restart(hostname, service_name):
            _log(SERVICE,
                 f"[service] Restart limit reached for {service_name} on {hostname} "
                 f"(max {self.max_restarts_per_hour}/hour)",
                 level="WARNING")
            return False

        _log(SERVICE, f"[service] Restarting {service_name} on {hostname}")

        # First, kill the old process if PID file exists
        pid_path = f"{REMOTE_PBGUI_DIR}/{svc.pid_file}"
        pid = self._executor.read_pid_file(hostname, pid_path)
        if pid:
            self._executor.execute(hostname, f"kill {pid} 2>/dev/null", timeout=5)
            # Wait briefly for process to die
            self._executor.execute(hostname, "sleep 1", timeout=5)

        # Start the service as a background process
        # Use the same pattern as the local run() methods:
        # python -u Script.py, detached, with nohup
        venv_python = f"{REMOTE_PBGUI_DIR}/.venv/bin/python"
        system_python = "python3"

        # Check which python is available
        check_result = self._executor.execute(
            hostname,
            f'test -f ~/{venv_python} && echo "venv" || echo "system"',
            timeout=5
        )
        python_cmd = f"~/{venv_python}" if check_result.stdout.strip() == "venv" else system_python

        start_cmd = (
            f"cd ~/{REMOTE_PBGUI_DIR} && "
            f"nohup {python_cmd} -u {svc.script_file} "
            f"> /dev/null 2>&1 &"
        )
        result = self._executor.execute(hostname, start_cmd, timeout=10)

        if result.success:
            self._record_restart(hostname, service_name)
            _log(SERVICE, f"[service] {service_name} restart command sent to {hostname}")
            return True
        else:
            _log(SERVICE,
                 f"[service] Failed to restart {service_name} on {hostname}: {result.stderr}",
                 level="ERROR")
            return False

    def auto_heal(self, hostname: str) -> list[ServiceCheckResult]:
        """
        Check all services on a VPS and restart any that are stopped.
        Only restarts if auto_restart is enabled.

        Returns list of ServiceCheckResults (including any restarts).
        """
        results = []
        checks = self.check_all(hostname)

        for service_name, check in checks.items():
            if check.status == ServiceStatus.STOPPED and self._auto_restart:
                _log(SERVICE,
                     f"[service] {service_name} down on {hostname}, attempting restart")
                restarted = self.restart_service(hostname, service_name)
                check.was_restarted = restarted
                if restarted:
                    check.status = ServiceStatus.RESTARTING
            results.append(check)

        return results

    def auto_heal_all(self, hostnames: list[str]) -> dict[str, list[ServiceCheckResult]]:
        """
        Auto-heal all services on all VPS servers.
        Returns {hostname: [ServiceCheckResult]}.
        """
        results = {}
        for hostname in hostnames:
            results[hostname] = self.auto_heal(hostname)
        return results

    def _can_restart(self, hostname: str, service_name: str) -> bool:
        """Check if we're within the restart limit (prevent restart loops)."""
        key = f"{hostname}/{service_name}"
        history = self._restart_history.get(hostname, {}).get(service_name, [])

        # Clean old entries (older than 1 hour)
        now = datetime.now()
        history = [ts for ts in history if (now - ts).total_seconds() < 3600]

        # Update cleaned history
        if hostname not in self._restart_history:
            self._restart_history[hostname] = {}
        self._restart_history[hostname][service_name] = history

        return len(history) < self.max_restarts_per_hour

    def _record_restart(self, hostname: str, service_name: str):
        """Record a restart event."""
        if hostname not in self._restart_history:
            self._restart_history[hostname] = {}
        if service_name not in self._restart_history[hostname]:
            self._restart_history[hostname][service_name] = []
        self._restart_history[hostname][service_name].append(datetime.now())

    def get_status_summary(self, hostnames: list[str]) -> dict:
        """
        Get a summary of all services across all VPS for UI display.
        Returns structured data for Streamlit rendering.
        """
        summary = {}
        all_checks = self.check_all_vps(hostnames)

        for hostname, checks in all_checks.items():
            summary[hostname] = {}
            for service_name, check in checks.items():
                summary[hostname][service_name] = {
                    "status": check.status.value,
                    "pid": check.pid,
                    "error": check.error,
                    "was_restarted": check.was_restarted,
                }
        return summary
