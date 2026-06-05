"""Paramiko-based SSH helpers for the PBGui installer."""

from __future__ import annotations

from pathlib import Path
import select
import socket
import time
from typing import Callable

try:
    import paramiko
except ImportError as exc:  # pragma: no cover - bootstrap installs this
    raise RuntimeError("paramiko is required. Run setup/master_installer.sh so the installer venv is prepared.") from exc


class SSHConnection:
    """Small SSH/SFTP wrapper used by the installer."""

    def __init__(self, *, host: str, port: int, username: str, password: str) -> None:
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.client: paramiko.SSHClient | None = None

    def __enter__(self) -> "SSHConnection":
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            look_for_keys=False,
            allow_agent=False,
            timeout=20,
            banner_timeout=20,
            auth_timeout=20,
        )
        self.client = client
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def _require_client(self) -> paramiko.SSHClient:
        if not self.client:
            raise RuntimeError("SSH connection is not open.")
        return self.client

    def put_file(self, local_path: Path, remote_path: str, *, mode: int = 0o600) -> None:
        client = self._require_client()
        with client.open_sftp() as sftp:
            sftp.put(str(local_path), remote_path)
            sftp.chmod(remote_path, mode)

    def put_text(self, remote_path: str, content: str, *, mode: int = 0o600) -> None:
        client = self._require_client()
        with client.open_sftp() as sftp:
            with sftp.open(remote_path, "w") as handle:
                handle.write(content)
            sftp.chmod(remote_path, mode)

    def get_file(self, remote_path: str, local_path: Path) -> None:
        client = self._require_client()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with client.open_sftp() as sftp:
            sftp.get(remote_path, str(local_path))

    def read_text(self, remote_path: str) -> str:
        client = self._require_client()
        with client.open_sftp() as sftp:
            with sftp.open(remote_path, "r") as handle:
                return handle.read().decode("utf-8", errors="replace")

    def run_stream(
        self,
        command: str,
        *,
        log: Callable[[str], None],
        sudo_password: str | None = None,
        timeout: int | None = None,
    ) -> int:
        """Run command and stream remote output."""
        client = self._require_client()
        transport = client.get_transport()
        if not transport:
            raise RuntimeError("SSH transport is not open.")
        channel = transport.open_session()
        channel.get_pty()
        channel.exec_command(command)
        if sudo_password:
            channel.send(sudo_password + "\n")
        started = time.monotonic()
        buffer = b""
        while True:
            if timeout is not None and time.monotonic() - started > timeout:
                channel.close()
                raise TimeoutError(f"Remote command timed out after {timeout}s")
            read_ready, _, _ = select.select([channel], [], [], 0.2)
            if read_ready and channel.recv_ready():
                chunk = channel.recv(4096)
                if chunk:
                    buffer += chunk
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        log(line.decode("utf-8", errors="replace").rstrip("\r"))
            if channel.recv_stderr_ready():
                chunk = channel.recv_stderr(4096)
                if chunk:
                    log(chunk.decode("utf-8", errors="replace").rstrip())
            if channel.exit_status_ready():
                while channel.recv_ready():
                    buffer += channel.recv(4096)
                if buffer:
                    for line in buffer.decode("utf-8", errors="replace").splitlines():
                        log(line.rstrip("\r"))
                return int(channel.recv_exit_status())
            if channel.closed:
                return int(channel.recv_exit_status()) if channel.exit_status_ready() else 255
            time.sleep(0.05)
