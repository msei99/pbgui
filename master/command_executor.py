"""
Remote Command Executor for PBMaster.

Provides a safe, timeout-protected interface for executing commands
on remote VPS servers via SSH. All commands are logged.
This is the base building block for ServiceMonitor and future extensions.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from master.connection_pool import SSHConnectionPool
from logging_helpers import human_log as _log


SERVICE = "PBMaster"


@dataclass
class CommandResult:
    """Result of a remote command execution."""
    hostname: str
    command: str
    stdout: str
    stderr: str
    exit_code: int
    success: bool
    error: Optional[str] = None
    duration_ms: int = 0
    timestamp: Optional[datetime] = None


class CommandExecutor:
    """
    Executes commands on remote VPS servers via SSH.

    Usage:
        executor = CommandExecutor(pool)
        result = executor.execute("myvps", "uptime")
        results = executor.execute_on_all("df -h")
    """

    # Default command timeout (seconds)
    DEFAULT_TIMEOUT = 30

    def __init__(self, pool: SSHConnectionPool):
        self._pool = pool

    def execute(self, hostname: str, command: str,
                timeout: int = None, log_output: bool = False) -> CommandResult:
        """
        Execute a command on a single VPS.

        Args:
            hostname: VPS hostname
            command: Shell command to execute
            timeout: Command timeout in seconds (default: 30)
            log_output: If True, log stdout/stderr at DEBUG level

        Returns:
            CommandResult with stdout, stderr, exit_code
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        start = datetime.now()
        result = CommandResult(
            hostname=hostname,
            command=command,
            stdout="",
            stderr="",
            exit_code=-1,
            success=False,
            timestamp=start,
        )

        client = self._pool.get_or_reconnect(hostname)
        if not client:
            result.error = f"No SSH connection to {hostname}"
            _log(SERVICE, f"[cmd] Cannot execute on {hostname}: no connection",
                 level="WARNING")
            return result

        try:
            _log(SERVICE, f"[cmd] {hostname}: {command}", level="DEBUG")
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)

            result.stdout = stdout.read().decode('utf-8', errors='replace').strip()
            result.stderr = stderr.read().decode('utf-8', errors='replace').strip()
            result.exit_code = stdout.channel.recv_exit_status()
            result.success = result.exit_code == 0

            elapsed = (datetime.now() - start).total_seconds() * 1000
            result.duration_ms = int(elapsed)

            if log_output:
                if result.stdout:
                    _log(SERVICE, f"[cmd] {hostname} stdout: {result.stdout}", level="DEBUG")
                if result.stderr:
                    _log(SERVICE, f"[cmd] {hostname} stderr: {result.stderr}", level="DEBUG")

            if not result.success:
                _log(SERVICE, f"[cmd] {hostname}: '{command}' exited with {result.exit_code}",
                     level="WARNING")

        except Exception as e:
            result.error = str(e)
            result.success = False
            elapsed = (datetime.now() - start).total_seconds() * 1000
            result.duration_ms = int(elapsed)
            _log(SERVICE, f"[cmd] {hostname}: '{command}' failed: {e}", level="ERROR")

        return result

    def execute_on_all(self, command: str, timeout: int = None,
                       hostnames: list[str] = None) -> dict[str, CommandResult]:
        """
        Execute a command on all (or specified) VPS servers.

        Args:
            command: Shell command to execute
            timeout: Command timeout in seconds
            hostnames: Optional list of specific hostnames (default: all)

        Returns:
            dict of {hostname: CommandResult}
        """
        targets = hostnames or self._pool.hostnames()
        results = {}
        for hostname in targets:
            results[hostname] = self.execute(hostname, command, timeout=timeout)
        return results

    def file_exists(self, hostname: str, path: str) -> bool:
        """Check if a file exists on a remote VPS."""
        result = self.execute(hostname, f'test -f {path} && echo "yes" || echo "no"',
                              timeout=10)
        return result.success and result.stdout.strip() == "yes"

    def read_file(self, hostname: str, path: str) -> Optional[str]:
        """Read a file from a remote VPS. Returns None if file doesn't exist."""
        result = self.execute(hostname, f'cat {path}', timeout=10)
        if result.success:
            return result.stdout
        return None

    def read_pid_file(self, hostname: str, pid_file: str) -> Optional[int]:
        """Read a PID file from a remote VPS. Returns PID or None."""
        content = self.read_file(hostname, pid_file)
        if content and content.strip().isdigit():
            return int(content.strip())
        return None

    def is_process_running(self, hostname: str, pid: int, process_name: str = None) -> bool:
        """
        Check if a process is running on a remote VPS.

        Args:
            hostname: VPS hostname
            pid: Process ID to check
            process_name: Optional process name to verify (e.g., "PBRun.py")
        """
        if process_name:
            # Check PID exists AND command matches
            result = self.execute(
                hostname,
                f'ps -p {pid} -o cmd= 2>/dev/null | grep -q "{process_name}" && echo "yes" || echo "no"',
                timeout=10
            )
        else:
            result = self.execute(
                hostname,
                f'ps -p {pid} > /dev/null 2>&1 && echo "yes" || echo "no"',
                timeout=10
            )
        return result.success and result.stdout.strip() == "yes"
