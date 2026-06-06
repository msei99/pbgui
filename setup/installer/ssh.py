"""Paramiko-based SSH helpers for the PBGui installer."""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path
import select
import socket
import time
from typing import Callable

try:
    import paramiko
except ImportError as exc:  # pragma: no cover - bootstrap installs this
    raise RuntimeError("paramiko is required. Run setup/master_installer.sh so the installer venv is prepared.") from exc


class UnknownSSHHostKeyError(RuntimeError):
    """Raised when an SSH host key is unknown and needs user confirmation."""

    def __init__(self, *, host: str, port: int, key_type: str, fingerprint: str) -> None:
        self.host = host
        self.port = int(port)
        self.key_type = key_type
        self.fingerprint = fingerprint
        super().__init__(
            f"Unknown SSH host key for {host}:{port} ({key_type} {fingerprint}). "
            "Confirm this fingerprint before continuing."
        )


class SSHHostKeyMismatchError(RuntimeError):
    """Raised when a known host presents a different key of the same type."""

    def __init__(self, *, host: str, port: int, key_type: str, fingerprint: str) -> None:
        super().__init__(
            f"SSH host key mismatch for {host}:{port} ({key_type} {fingerprint}). "
            "Refusing to connect because this can indicate a MITM attack or rebuilt server."
        )


def _fingerprint_sha256(key: "paramiko.PKey") -> str:
    digest = hashlib.sha256(key.asbytes()).digest()
    encoded = base64.b64encode(digest).decode("ascii").rstrip("=")
    return f"SHA256:{encoded}"


def _normalize_fingerprint(value: str) -> str:
    text = str(value or "").strip()
    return text[7:] if text.startswith("SHA256:") else text


def _fingerprints_match(expected: str, actual: str) -> bool:
    return bool(expected) and _normalize_fingerprint(expected) == _normalize_fingerprint(actual)


def _known_hosts() -> "paramiko.HostKeys":
    host_keys = paramiko.HostKeys()
    for path in (Path("/etc/ssh/ssh_known_hosts"), Path.home() / ".ssh" / "known_hosts"):
        try:
            if path.exists():
                host_keys.load(str(path))
        except Exception:
            continue
    return host_keys


def _host_key_names(host: str, port: int) -> list[str]:
    names = [str(host)]
    if int(port) != 22:
        names.insert(0, f"[{host}]:{int(port)}")
    return names


def _known_host_key_status(host: str, port: int, key: "paramiko.PKey") -> str:
    host_keys = _known_hosts()
    mismatch = False
    for name in _host_key_names(host, port):
        entries = host_keys.lookup(name)
        if not entries:
            continue
        known_key = entries.get(key.get_name())
        if known_key is None:
            continue
        if known_key.asbytes() == key.asbytes():
            return "known"
        mismatch = True
    return "mismatch" if mismatch else "unknown"


def _fetch_remote_host_key(host: str, port: int, timeout: int = 10) -> "paramiko.PKey":
    sock = socket.create_connection((host, int(port)), timeout=timeout)
    transport = paramiko.Transport(sock)
    try:
        transport.start_client(timeout=timeout)
        return transport.get_remote_server_key()
    finally:
        transport.close()


def _user_known_hosts_path() -> Path:
    return Path.home() / ".ssh" / "known_hosts"


def _remember_known_host_key(host: str, port: int, key: "paramiko.PKey") -> None:
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    try:
        ssh_dir.chmod(0o700)
    except OSError:
        pass
    path = _user_known_hosts_path()
    host_keys = paramiko.HostKeys()
    if path.exists():
        host_keys.load(str(path))
    host_keys.add(_host_key_names(host, int(port))[0], key.get_name(), key)
    host_keys.save(str(path))
    try:
        path.chmod(0o600)
    except OSError:
        pass


def probe_ssh_host_key(host: str, port: int = 22, timeout: int = 10) -> dict[str, object]:
    """Return remote SSH host-key metadata and local known_hosts status."""
    key = _fetch_remote_host_key(host, int(port), timeout=timeout)
    status = _known_host_key_status(host, int(port), key)
    return {
        "host": str(host),
        "port": int(port),
        "key_type": key.get_name(),
        "fingerprint": _fingerprint_sha256(key),
        "known": status == "known",
        "mismatch": status == "mismatch",
        "status": status,
    }


class SSHConnection:
    """Small SSH/SFTP wrapper used by the installer."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        accept_unknown_host: bool = False,
        expected_host_key_fingerprint: str = "",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.accept_unknown_host = bool(accept_unknown_host)
        self.expected_host_key_fingerprint = str(expected_host_key_fingerprint or "").strip()
        self.client: paramiko.SSHClient | None = None

    def __enter__(self) -> "SSHConnection":
        remote_key = _fetch_remote_host_key(self.host, self.port)
        status = _known_host_key_status(self.host, self.port, remote_key)
        fingerprint = _fingerprint_sha256(remote_key)
        if status == "mismatch":
            raise SSHHostKeyMismatchError(
                host=self.host,
                port=self.port,
                key_type=remote_key.get_name(),
                fingerprint=fingerprint,
            )
        if status != "known":
            if not self.accept_unknown_host or not _fingerprints_match(self.expected_host_key_fingerprint, fingerprint):
                raise UnknownSSHHostKeyError(
                    host=self.host,
                    port=self.port,
                    key_type=remote_key.get_name(),
                    fingerprint=fingerprint,
                )
            _remember_known_host_key(self.host, self.port, remote_key)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        try:
            known_hosts = _user_known_hosts_path()
            if known_hosts.exists():
                client.load_host_keys(str(known_hosts))
        except Exception:
            pass
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
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
