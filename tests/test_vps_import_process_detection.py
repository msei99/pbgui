"""Regression tests for Existing VPS import process and key handling."""

import asyncio
import builtins
import io
import json
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import api.v7_instances as v7_instances
import api.vps_manager as vps_manager_api
import master.async_monitor as monitor_mod
import master.async_logs as async_logs
import master.async_pool as async_pool
import monitor_agent
from master.async_monitor import INSTANCE_COLLECT_SCRIPT, CpuHistoryStore, VPSMonitor, collect_live_alerts
from master.async_store import SystemMetrics
import vps_manager_core as core
import vps_manager_service as service_mod
from vps_manager_service import VPSManagerService, _ensure_import_public_key, _import_process_line_is_legacy, _set_import_key_check


@pytest.mark.parametrize("value", ["198.51.100.1", "10.8.0.0/24", "10.8.0.1/32", "0.0.0.0/0"])
def test_firewall_source_accepts_ipv4_addresses_and_cidr_networks(value: str) -> None:
    """Accept individual IPv4 addresses and CIDR sources supported by UFW."""
    assert service_mod._valid_ipv4_or_cidr(value) is True


@pytest.mark.parametrize("value", ["", "10.8.0.0/33", "10.8.0/24", "2001:db8::/32", "not-an-ip"])
def test_firewall_source_rejects_invalid_or_non_ipv4_values(value: str) -> None:
    """Reject malformed, out-of-range, and IPv6 firewall sources."""
    assert service_mod._valid_ipv4_or_cidr(value) is False


def test_async_pool_verifies_ip_connections_with_inventory_hostname() -> None:
    """Use the confirmed VPS hostname key while connecting to its configured IP."""
    source = Path(async_pool.__file__).read_text(encoding="utf-8")

    assert "host=cfg.ip" in source
    assert "host_key_alias=cfg.hostname" in source


class _FakeStdout:
    """Small stdout double returning a fixed byte payload."""

    def __init__(self, payload: bytes) -> None:
        """Store the payload returned by read()."""
        self._payload = payload

    def read(self) -> bytes:
        """Return the configured payload."""
        return self._payload


class _FakeStdin:
    """Small stdin double capturing written sudo passwords."""

    def __init__(self) -> None:
        """Initialize the write capture."""
        self.writes: list[str] = []

    def write(self, value: str) -> None:
        """Capture stdin writes."""
        self.writes.append(value)

    def flush(self) -> None:
        """No-op flush."""
        return None


def test_fetch_package_status_uses_trusted_inventory_hostname(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify live package probes against the inventory alias instead of a stale IP key."""
    connect_kwargs: dict[str, object] = {}

    class FakeSshClient:
        """SSH double returning a current package status."""

        def connect(self, **kwargs) -> None:
            """Capture the host identity used for verification."""
            connect_kwargs.update(kwargs)

        def exec_command(self, command: str, **kwargs):
            """Return package or reboot status according to the command."""
            del kwargs
            output = b"0 upgraded, 0 newly installed, 0 to remove\n" if "dist-upgrade" in command else b"no\n"
            return _FakeStdin(), _FakeStdout(output), _FakeStdout(b"")

        def close(self) -> None:
            """No-op close."""
            return None

    monkeypatch.setattr(core, "_strict_ssh_client", lambda: FakeSshClient())
    vps = core.VPS()
    vps.hostname = "trusted-vps"
    vps.ip = "192.0.2.10"
    vps.user = "mani"
    vps.user_pw = "secret"
    vps.firewall_ssh_port = 2222

    result = vps.fetch_package_status()

    assert result == {"upgrades": 0, "reboot": False}
    assert connect_kwargs["hostname"] == "trusted-vps"
    assert connect_kwargs["port"] == 2222


class _FakeUfwSshClient:
    """Paramiko SSHClient double for fetch_ufw_settings()."""

    output = ""
    errors = ""
    command = ""
    kwargs: dict[str, object] = {}
    stdin = _FakeStdin()

    def load_system_host_keys(self) -> None:
        """No-op known-hosts load."""
        return None

    def set_missing_host_key_policy(self, policy) -> None:
        """Accept the configured host-key policy."""
        del policy

    def connect(self, *args, **kwargs) -> None:
        """No-op SSH connection."""
        del args, kwargs

    def exec_command(self, command: str, **kwargs):
        """Return fake UFW output."""
        self.__class__.command = command
        self.__class__.kwargs = dict(kwargs)
        self.__class__.stdin = _FakeStdin()
        return self.__class__.stdin, _FakeStdout(self.output.encode("utf-8")), _FakeStdout(self.errors.encode("utf-8"))

    def close(self) -> None:
        """No-op close."""
        return None


class _FakeSftpFile:
    """Context-manager file double for SFTP reads."""

    def __init__(self, content: str) -> None:
        """Store text content for the fake remote file."""
        self._content = content

    def __enter__(self) -> "_FakeSftpFile":
        """Return the fake file handle."""
        return self

    def __exit__(self, *args) -> None:
        """No-op context-manager exit."""
        return None

    def read(self) -> bytes:
        """Return encoded fake file content."""
        return self._content.encode("utf-8")


class _FakeSftp:
    """SFTP double serving one pbgui.ini payload."""

    def __init__(self, content: str) -> None:
        """Store remote config content."""
        self._content = content

    def file(self, path: str, mode: str = "r") -> _FakeSftpFile:
        """Return a fake file when pbgui.ini is requested."""
        del mode
        if not path.endswith("/pbgui.ini"):
            raise FileNotFoundError(path)
        return _FakeSftpFile(self._content)

    def close(self) -> None:
        """No-op close."""
        return None


class _FakeSshClient:
    """Paramiko SSHClient double for fetch_vps_info()."""

    config_content = ""

    def load_system_host_keys(self) -> None:
        """No-op known-hosts load."""
        return None

    def set_missing_host_key_policy(self, policy) -> None:
        """Accept the configured host-key policy."""
        del policy

    def connect(self, *args, **kwargs) -> None:
        """No-op SSH connection."""
        del args, kwargs

    def exec_command(self, command: str):
        """Return a fake swap command result."""
        del command
        return None, _FakeStdout(b"2G\n"), _FakeStdout(b"")

    def open_sftp(self) -> _FakeSftp:
        """Return fake SFTP access."""
        return _FakeSftp(self.config_content)

    def close(self) -> None:
        """No-op close."""
        return None


def test_import_process_line_ignores_systemd_managed_process() -> None:
    """A process row marked as systemd is not a legacy process."""
    line = "123\t/home/mani/software/pbgui\tsystemd\tpython -u /home/mani/software/pbgui/PBRun.py"

    assert not _import_process_line_is_legacy(line, "/home/mani/software/pbgui")


def test_import_process_line_reports_matching_legacy_process() -> None:
    """A matching process row marked as legacy is reported."""
    line = "123\t/home/mani/software/pbgui\tlegacy\tpython -u /home/mani/software/pbgui/PBRun.py"

    assert _import_process_line_is_legacy(line, "/home/mani/software/pbgui")


def test_import_process_line_preserves_old_three_column_rows() -> None:
    """Old probe rows without manager metadata remain legacy-compatible."""
    line = "123\t/home/mani/software/pbgui\tpython -u /home/mani/software/pbgui/PBRun.py"

    assert _import_process_line_is_legacy(line, "/home/mani/software/pbgui")


def test_ensure_import_public_key_uses_existing_default_key(tmp_path: Path, monkeypatch) -> None:
    """Existing default key pairs are reused for import monitoring."""
    ssh_dir = tmp_path / ".ssh"
    ssh_dir.mkdir()
    private_key = ssh_dir / "id_ed25519"
    public_key = ssh_dir / "id_ed25519.pub"
    private_key.write_text("private-key-placeholder\n", encoding="utf-8")
    public_key.write_text("ssh-ed25519 AAAATEST pbgui-test\n", encoding="utf-8")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

    key_path, key = _ensure_import_public_key()

    assert key_path == public_key
    assert key == "ssh-ed25519 AAAATEST pbgui-test"


def test_set_import_key_check_updates_existing_check() -> None:
    """The save flow can replace the probe key-login check after installing a key."""
    probe = {"checks": [{"label": "SSH key login for monitoring", "ok": False, "detail": "failed"}]}

    _set_import_key_check(probe, True, "Key authentication succeeded.")

    assert probe["checks"] == [
        {"label": "SSH key login for monitoring", "ok": True, "detail": "Key authentication succeeded."}
    ]


def test_existing_vps_probe_allows_missing_local_hosts_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Existing VPS probe can continue via entered IPv4 before local /etc/hosts is updated."""
    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(list=lambda: [])

    class FakeKey:
        """Minimal SSH key double for host-key probe."""

        def get_name(self) -> str:
            """Return a deterministic key type."""
            return "ssh-ed25519"

    fetched_hosts: list[str] = []

    def fake_fetch_remote_host_key(host: str, port: int = 22, timeout: int = 10) -> FakeKey:
        """Capture the host used for key lookup."""
        del port, timeout
        fetched_hosts.append(host)
        return FakeKey()

    monkeypatch.setattr(service_mod, "_hosts_entry_status", lambda hostname, ip: {"ok": False, "has_hostname": False, "current_ip": ""})
    monkeypatch.setattr(service_mod, "_fetch_remote_host_key", fake_fetch_remote_host_key)
    monkeypatch.setattr(service_mod, "_ssh_fingerprint_sha256", lambda key: "SHA256:test")
    monkeypatch.setattr(service_mod, "_known_host_key_status", lambda host, port, key: "unknown")

    result = service.probe_existing_vps_import({
        "hostname": "manibot90",
        "ip": "23.94.74.212",
        "user": "mani",
        "user_pw": "fresh-password",
        "install_dir": "/home/mani/software",
    })

    assert fetched_hosts == ["23.94.74.212"]
    assert result["local_hosts_ok"] is False
    assert result["local_hosts_update_required"] is True
    assert result["needs_host_key_confirmation"] is True
    assert result["blockers"] == []
    assert any("saving the import will add it" in warning for warning in result["warnings"])


def test_install_import_monitoring_key_rejects_unknown_host_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monitoring key install requires prior fingerprint confirmation."""

    import paramiko

    service = object.__new__(VPSManagerService)
    remembered: list[tuple[str, int]] = []
    connects: list[dict[str, object]] = []

    class FakeKey:
        """Minimal SSH key double for known-hosts handling."""

        def get_name(self) -> str:
            """Return a deterministic key type."""
            return "ssh-ed25519"

    class FakeChannel:
        """stdout channel double returning a successful exit code."""

        def recv_exit_status(self) -> int:
            """Return successful command status."""
            return 0

    class FakeCommandOutput:
        """Command output double with Paramiko-like channel."""

        channel = FakeChannel()

        def __init__(self, payload: bytes = b"") -> None:
            """Store the payload returned by read()."""
            self._payload = payload

        def read(self) -> bytes:
            """Return command output bytes."""
            return self._payload

    class FakeSshClient:
        """Paramiko SSHClient double for key installation."""

        def load_system_host_keys(self) -> None:
            """No-op system key load."""
            return None

        def load_host_keys(self, path: str) -> None:
            """No-op user key load."""
            del path

        def set_missing_host_key_policy(self, policy) -> None:
            """Accept the configured host-key policy."""
            del policy

        def connect(self, **kwargs) -> None:
            """Capture SSH connect arguments."""
            connects.append(dict(kwargs))

        def exec_command(self, command: str, timeout: int = 10, get_pty: bool = False):
            """Return a successful authorized_keys install result."""
            del command, timeout, get_pty
            return _FakeStdin(), FakeCommandOutput(b"SSH key added\n"), FakeCommandOutput()

        def close(self) -> None:
            """No-op close."""
            return None

    monkeypatch.setattr(paramiko, "SSHClient", FakeSshClient)
    monkeypatch.setattr(service_mod, "_ensure_import_public_key", lambda: (Path("/tmp/id_ed25519.pub"), "ssh-ed25519 AAAATEST pbgui"))
    monkeypatch.setattr(service_mod, "_fetch_remote_host_key", lambda host, port=22, timeout=10: FakeKey())
    monkeypatch.setattr(service_mod, "_known_host_key_status", lambda host, port, key: "unknown")
    monkeypatch.setattr(service_mod, "_remember_known_host_key", lambda host, port, key: remembered.append((host, port)))
    monkeypatch.setattr(VPSManagerService, "_test_import_key_login", lambda self, **kwargs: (True, "Key authentication succeeded."))

    ok, detail = service._install_import_monitoring_key(ssh_host="85.215.157.244", user="mani", user_pw="secret")

    assert ok is False
    assert "fingerprint confirmation" in detail.lower()
    assert remembered == []
    assert connects == []


def test_install_import_monitoring_key_blocks_host_key_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monitoring key install does not overwrite mismatched known_hosts entries."""

    service = object.__new__(VPSManagerService)

    class FakeKey:
        """Minimal SSH key double for known-hosts handling."""

    monkeypatch.setattr(service_mod, "_ensure_import_public_key", lambda: (Path("/tmp/id_ed25519.pub"), "ssh-ed25519 AAAATEST pbgui"))
    monkeypatch.setattr(service_mod, "_fetch_remote_host_key", lambda host, port=22, timeout=10: FakeKey())
    monkeypatch.setattr(service_mod, "_known_host_key_status", lambda host, port, key: "mismatch")

    ok, detail = service._install_import_monitoring_key(ssh_host="85.215.157.244", user="mani", user_pw="secret")

    assert ok is False
    assert "host key mismatch" in detail.lower()


def test_host_key_probe_requires_exact_fingerprint_before_trust(monkeypatch: pytest.MonkeyPatch) -> None:
    """Do not add an unknown host key without exact fingerprint confirmation."""
    remembered: list[tuple[str, int]] = []

    class FakeKey:
        """Minimal SSH host-key double."""

        def get_name(self) -> str:
            """Return a deterministic key type."""
            return "ssh-ed25519"

    monkeypatch.setattr(service_mod, "_fetch_remote_host_key", lambda host, port, timeout=8: FakeKey())
    monkeypatch.setattr(service_mod, "_ssh_fingerprint_sha256", lambda key: "SHA256:expected")
    monkeypatch.setattr(service_mod, "_known_host_key_status", lambda host, port, key: "unknown")
    monkeypatch.setattr(service_mod, "_remember_known_host_key", lambda host, port, key: remembered.append((host, port)))

    result = service_mod._probe_and_maybe_trust_host_key(
        "192.0.2.10",
        22,
        ["test-vps"],
        accept_unknown_host=True,
        expected_fingerprint="SHA256:wrong",
    )

    assert result["needs_confirmation"] is True
    assert result["fingerprint"] == "SHA256:expected"
    assert remembered == []


def test_host_key_probe_trusts_confirmed_fingerprint_and_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persist a confirmed key for both the connection address and hostname."""
    remembered: list[tuple[str, int]] = []

    class FakeKey:
        """Minimal SSH host-key double."""

        def get_name(self) -> str:
            """Return a deterministic key type."""
            return "ssh-ed25519"

    monkeypatch.setattr(service_mod, "_fetch_remote_host_key", lambda host, port, timeout=8: FakeKey())
    monkeypatch.setattr(service_mod, "_ssh_fingerprint_sha256", lambda key: "SHA256:expected")
    monkeypatch.setattr(service_mod, "_known_host_key_status", lambda host, port, key: "unknown")
    monkeypatch.setattr(service_mod, "_remember_known_host_key", lambda host, port, key: remembered.append((host, port)))

    result = service_mod._probe_and_maybe_trust_host_key(
        "192.0.2.10",
        2222,
        ["test-vps"],
        accept_unknown_host=True,
        expected_fingerprint="SHA256:expected",
    )

    assert result["known"] is True
    assert remembered == [("192.0.2.10", 2222), ("test-vps", 2222)]


def test_host_key_probe_never_replaces_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject a changed known host key even when confirmation flags are supplied."""

    class FakeKey:
        """Minimal SSH host-key double."""

        def get_name(self) -> str:
            """Return a deterministic key type."""
            return "ssh-ed25519"

    monkeypatch.setattr(service_mod, "_fetch_remote_host_key", lambda host, port, timeout=8: FakeKey())
    monkeypatch.setattr(service_mod, "_ssh_fingerprint_sha256", lambda key: "SHA256:changed")
    monkeypatch.setattr(service_mod, "_known_host_key_status", lambda host, port, key: "mismatch")

    with pytest.raises(ValueError, match="host key mismatch"):
        service_mod._probe_and_maybe_trust_host_key(
            "192.0.2.10",
            22,
            ["test-vps"],
            accept_unknown_host=True,
            expected_fingerprint="SHA256:changed",
        )


def test_replace_known_host_keys_updates_vps_aliases_atomically(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace stale hostname and IP entries without changing unrelated hosts."""
    import paramiko

    known_hosts_path = tmp_path / ".ssh" / "known_hosts"
    old_key = paramiko.RSAKey.generate(1024)
    new_key = paramiko.RSAKey.generate(1024)
    other_key = paramiko.RSAKey.generate(1024)
    stale_alternative_key = paramiko.ECDSAKey.generate(bits=256)
    host_keys = paramiko.HostKeys()
    host_keys.add("repair-vps", old_key.get_name(), old_key)
    host_keys.add("repair-vps", stale_alternative_key.get_name(), stale_alternative_key)
    host_keys.add("192.0.2.10", old_key.get_name(), old_key)
    host_keys.add("192.0.2.10", stale_alternative_key.get_name(), stale_alternative_key)
    host_keys.add("other-vps", other_key.get_name(), other_key)
    monkeypatch.setattr(service_mod, "_user_known_hosts_path", lambda: known_hosts_path)
    service_mod._save_user_known_hosts(host_keys)

    service_mod._replace_known_host_keys(["repair-vps", "192.0.2.10"], 22, new_key)

    loaded = paramiko.HostKeys(str(known_hosts_path))
    assert loaded.lookup("repair-vps")[new_key.get_name()].asbytes() == new_key.asbytes()
    assert loaded.lookup("192.0.2.10")[new_key.get_name()].asbytes() == new_key.asbytes()
    assert stale_alternative_key.get_name() not in loaded.lookup("repair-vps")
    assert stale_alternative_key.get_name() not in loaded.lookup("192.0.2.10")
    assert loaded.lookup("other-vps")[other_key.get_name()].asbytes() == other_key.asbytes()
    assert not list(known_hosts_path.parent.glob(".known_hosts.*.tmp"))


def test_known_host_alias_status_reports_changed_unknown_and_trusted() -> None:
    """Overview distinguishes stale, missing, and matching IP aliases."""
    import paramiko

    current_key = paramiko.RSAKey.generate(1024)
    stale_key = paramiko.RSAKey.generate(1024)
    preferred_key = paramiko.ECDSAKey.generate(bits=256)
    stale_alternative_key = paramiko.RSAKey.generate(1024)
    host_keys = paramiko.HostKeys()
    host_keys.add("changed-vps", current_key.get_name(), current_key)
    host_keys.add("192.0.2.10", stale_key.get_name(), stale_key)
    host_keys.add("unknown-vps", current_key.get_name(), current_key)
    host_keys.add("trusted-vps", current_key.get_name(), current_key)
    host_keys.add("192.0.2.30", current_key.get_name(), current_key)
    host_keys.add("preferred-vps", preferred_key.get_name(), preferred_key)
    host_keys.add("preferred-vps", current_key.get_name(), current_key)
    host_keys.add("192.0.2.40", preferred_key.get_name(), preferred_key)
    host_keys.add("192.0.2.40", stale_alternative_key.get_name(), stale_alternative_key)

    assert service_mod._known_host_alias_status("changed-vps", "192.0.2.10", 22, host_keys) == "mismatch"
    assert service_mod._known_host_alias_status("unknown-vps", "192.0.2.20", 22, host_keys) == "unknown"
    assert service_mod._known_host_alias_status("trusted-vps", "192.0.2.30", 22, host_keys) == "known"
    assert service_mod._known_host_alias_status("preferred-vps", "192.0.2.40", 22, host_keys) == "known"


def test_trust_vps_host_key_replaces_confirmed_mismatch_and_reconnects(monkeypatch: pytest.MonkeyPatch) -> None:
    """GUI confirmation replaces an exact changed key and reconnects monitoring."""
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="repair-vps", ip="192.0.2.10", firewall_ssh_port=22)
    service._require_vps = lambda hostname: vps
    refreshed: list[str] = []
    replaced: list[tuple[list[str], int, object]] = []

    class FakeKey:
        """Minimal SSH key used by the trust flow."""

    key = FakeKey()
    probes = [
        {
            "status": "mismatch",
            "fingerprint": "SHA256:confirmed",
            "target_statuses": {"repair-vps": "mismatch", "192.0.2.10": "mismatch"},
            "_key": key,
        },
        {
            "status": "known",
            "fingerprint": "SHA256:confirmed",
            "target_statuses": {"repair-vps": "known", "192.0.2.10": "known"},
            "_key": key,
        },
    ]
    monkeypatch.setattr(service_mod, "_probe_host_key", lambda host, port, aliases: dict(probes.pop(0)))
    monkeypatch.setattr(service_mod, "_replace_known_host_keys", lambda hosts, port, remote_key: replaced.append((hosts, port, remote_key)))
    service._refresh_vps_monitor_connection = lambda hostname: refreshed.append(hostname)

    result = service.trust_vps_host_key(
        "repair-vps",
        "SHA256:confirmed",
        replace_existing=True,
    )

    assert replaced == [(["repair-vps", "192.0.2.10"], 22, key)]
    assert refreshed == ["repair-vps"]
    assert result["status"] == "known"
    assert result["reconnect_requested"] is True


def test_trust_vps_host_key_rejects_changed_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Do not write known_hosts when the key changed after GUI review."""
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="repair-vps", ip="192.0.2.10", firewall_ssh_port=22)
    service._require_vps = lambda hostname: vps
    replaced: list[object] = []
    monkeypatch.setattr(service_mod, "_probe_host_key", lambda host, port, aliases: {
        "status": "mismatch",
        "fingerprint": "SHA256:new",
        "target_statuses": {"repair-vps": "mismatch"},
        "_key": object(),
    })
    monkeypatch.setattr(service_mod, "_replace_known_host_keys", lambda *args: replaced.append(args))

    with pytest.raises(ValueError, match="changed while the confirmation dialog was open"):
        service.trust_vps_host_key(
            "repair-vps",
            "SHA256:reviewed",
            replace_existing=True,
        )

    assert replaced == []


def test_vps_monitor_refresh_enabled_host_reconnects_after_auth_fix(monkeypatch: pytest.MonkeyPatch) -> None:
    """The running monitor can reconnect one enabled host after SSH keys change."""

    monitor = object.__new__(VPSMonitor)
    monitor._enabled_hosts = {"manibot50"}
    monitor._last_host_meta_collect = {"manibot50": 1.0}
    monitor._last_package_status_collect = {"manibot50": 1.0}
    started: list[str] = []

    class FakePool:
        """Small pool double for refresh_enabled_host()."""

        def __init__(self) -> None:
            """Initialize call capture."""
            self.loaded = False
            self.removed: list[str] = []
            self.connected: list[str] = []
            self._hosts = ["manibot50", "disabled-host"]

        def load_vps_configs(self) -> list[str]:
            """Pretend configs were reloaded from disk."""
            self.loaded = True
            return list(self._hosts)

        def hostnames(self) -> list[str]:
            """Return currently known hosts."""
            return list(self._hosts)

        def remove_host(self, hostname: str) -> None:
            """Capture removed disabled hosts."""
            self.removed.append(hostname)
            self._hosts = [host for host in self._hosts if host != hostname]

        async def connect(self, hostname: str) -> bool:
            """Capture reconnect request."""
            self.connected.append(hostname)
            return True

    pool = FakePool()
    monitor.pool = pool
    monitor._start_metrics_stream = lambda hostname: started.append(hostname)
    monkeypatch.setattr(monitor_mod, "load_ini", lambda section, parameter: "manibot50" if (section, parameter) == ("vps_monitor", "enabled_hosts") else "")

    ok = asyncio.run(monitor.refresh_enabled_host("manibot50"))

    assert ok is True
    assert pool.loaded is True
    assert pool.removed == ["disabled-host"]
    assert pool.connected == ["manibot50"]
    assert started == ["manibot50"]
    assert monitor._last_host_meta_collect == {}
    assert monitor._last_package_status_collect == {}


def test_stale_metrics_stream_does_not_disconnect_ssh() -> None:
    """A missing/stale monitor-agent stream must not make reachable SSH rows flap red."""
    monitor = object.__new__(VPSMonitor)
    monitor._stream_stale_counts = {"manibot50": monitor_mod.METRICS_STREAM_RECONNECT_AFTER_STALE_RESTARTS - 1}
    monitor._stream_stale_last_logged = {}
    updates: list[tuple[str, dict]] = []
    started: list[str] = []
    disconnected: list[str] = []

    class FakeStore:
        """Capture stream status updates."""

        def update_stream_info(self, hostname: str, payload: dict) -> None:
            """Store stream diagnostics updates."""
            updates.append((hostname, payload))

    class FakePool:
        """Fail the test if stale metrics force an SSH disconnect."""

        async def disconnect(self, hostname: str) -> None:
            """Capture unwanted disconnect calls."""
            disconnected.append(hostname)

    monitor.store = FakeStore()
    monitor.pool = FakePool()
    monitor._start_metrics_stream = lambda hostname: started.append(hostname)

    asyncio.run(monitor._restart_stale_metrics_stream("manibot50", stale_age=90.0, now=1000.0))

    assert disconnected == []
    assert started == ["manibot50"]
    assert updates[0][0] == "manibot50"
    assert updates[0][1]["stale"] is True
    assert monitor._stream_stale_counts["manibot50"] == monitor_mod.METRICS_STREAM_RECONNECT_AFTER_STALE_RESTARTS


def test_save_existing_vps_import_writes_missing_hosts_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Saving an import writes /etc/hosts with local sudo when the probe requires it."""
    writes: list[tuple[str, str, str]] = []
    saved: list[str] = []
    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(list=lambda: [], vpss=[])
    service.probe_existing_vps_import = lambda form: {
        "hostname": "manibot90",
        "ip": "23.94.74.212",
        "user": "mani",
        "local_hosts_update_required": True,
        "needs_host_key_confirmation": False,
        "blockers": [],
        "detected": {
            "remote_pbgui_dir": "/home/mani/software/pbgui",
            "swap": "2G",
            "coinmarketcap_api_key": "",
            "firewall": True,
            "firewall_ssh_port": 22,
            "firewall_ssh_ips": "198.51.100.1",
            "key_auth_ok": True,
        },
    }
    service.write_hosts_entry = lambda ip, hostname, sudo_pw: writes.append((ip, hostname, sudo_pw)) or {"ok": True}
    service._store_session_secrets = lambda token, hostname, values: None
    service._set_vps_monitor_enabled = lambda hostname, enabled: None
    service._build_vps_config = lambda token, vps: {"hostname": vps.hostname, "ip": vps.ip}

    def fake_save(self) -> None:
        """Capture saved VPS hostnames."""
        saved.append(self.hostname)

    monkeypatch.setattr(service_mod.VPS, "save", fake_save)

    result = service.save_existing_vps_import(
        "token",
        {
            "hostname": "manibot90",
            "ip": "23.94.74.212",
            "user_pw": "fresh-password",
            "local_sudo_pw": "local-sudo-password",
        },
    )

    assert writes == [("23.94.74.212", "manibot90", "local-sudo-password")]
    assert saved == ["manibot90"]
    assert result["hostname"] == "manibot90"


def test_update_vps_passes_empty_optional_values(monkeypatch, tmp_path: Path) -> None:
    """Update playbooks receive explicit empty optional values after clearing CoinData."""
    captured: dict[str, object] = {}

    def fake_run_async(**kwargs) -> None:
        """Capture the ansible-runner call instead of executing a playbook."""
        captured.update(kwargs)

    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    monkeypatch.setattr(core.ansible_runner, "run_async", fake_run_async)
    monkeypatch.setattr(core, "_ansible_envvars", lambda: {})

    vps = core.VPS()
    vps.hostname = "test-vps"
    vps.user = "mani"
    vps.user_pw = None
    vps.swap = "2G"
    vps.coinmarketcap_api_key = ""
    vps.command = "vps-update-pbgui"

    manager = core.VPSManager()
    manager.update_vps(vps)

    extravars = captured["extravars"]
    assert extravars["coinmarketcap_api_key"] == ""


def test_update_vps_cleans_only_its_runner_private_dir(monkeypatch, tmp_path: Path) -> None:
    """Overlapping VPS runs do not let one callback delete another run's tmp dir."""
    observed: dict[str, Path] = {}

    def fake_run_async(**kwargs) -> None:
        """Capture runner dirs and invoke the finish callback immediately."""
        private_dir = Path(kwargs["private_data_dir"])
        other_dir = private_dir.parent / "other-run"
        other_dir.mkdir(parents=True)
        observed["private_dir"] = private_dir
        observed["other_dir"] = other_dir
        vps.privat_data_dir = other_dir
        kwargs["finished_callback"]()

    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    monkeypatch.setattr(core.ansible_runner, "run_async", fake_run_async)
    monkeypatch.setattr(core, "_ansible_envvars", lambda: {})

    vps = core.VPS()
    vps.hostname = "test-vps"
    vps.user = "mani"
    vps.command = "vps-update-pb"

    manager = core.VPSManager()
    manager.update_vps(vps)

    assert observed["private_dir"].parent == tmp_path / "data" / "vpsmanager" / "hosts" / "test-vps" / "tmp"
    assert observed["private_dir"].name.startswith("run-")
    assert not observed["private_dir"].exists()
    assert observed["other_dir"].exists()


def test_fetch_vps_info_reads_optional_settings_from_remote_ini(monkeypatch) -> None:
    """Remote settings refresh reads supported VPS settings from pbgui.ini."""
    _FakeSshClient.config_content = """
[coinmarketcap]
api_key = remote-api-key

[firewall]
enabled = true
ssh_port = 2222
ssh_ips = 198.51.100.1,203.0.113.7
"""
    monkeypatch.setattr(core.paramiko, "SSHClient", _FakeSshClient)

    vps = core.VPS()
    vps.hostname = "test-vps"
    vps.ip = "127.0.0.1"
    vps.user = "mani"
    vps.remote_pbgui_dir = "software/pbgui"

    info = vps.fetch_vps_info()

    assert info["coinmarketcap"] == "remote-api-key"
    assert info["swap"] == "2G"
    assert info["firewall"] is True
    assert info["firewall_ssh_port"] == "2222"
    assert info["firewall_ssh_ips"] == "198.51.100.1,203.0.113.7"


def test_fetch_ufw_settings_reads_specific_ips_without_pbgui_ini(monkeypatch) -> None:
    """Remote UFW reads parse real firewall rules independently of pbgui.ini."""
    _FakeUfwSshClient.output = """
Status: active

To                         Action      From
--                         ------      ----
22/tcp                     ALLOW IN    198.51.100.1
22/tcp                     ALLOW       203.0.113.7
22/tcp (v6)                ALLOW IN    Anywhere (v6)
"""
    _FakeUfwSshClient.errors = ""
    monkeypatch.setattr(core.paramiko, "SSHClient", _FakeUfwSshClient)
    vps = core.VPS()
    vps.hostname = "test-vps"
    vps.ip = "127.0.0.1"
    vps.user = "mani"
    vps.user_pw = "fresh-password"
    vps.firewall_ssh_port = 22

    enabled, allowed_ips = vps.fetch_ufw_settings()

    assert enabled is True
    assert allowed_ips == "198.51.100.1,203.0.113.7"
    assert vps.firewall_ssh_port == 22
    assert _FakeUfwSshClient.command == "LANG=C sudo -S -p '' ufw status"
    assert _FakeUfwSshClient.kwargs["get_pty"] is True
    assert _FakeUfwSshClient.stdin.writes == ["fresh-password\n"]


def test_fetch_ufw_settings_maps_anywhere_to_empty_ip_list(monkeypatch) -> None:
    """Open-to-any IPv4 UFW rules map to the existing empty Allowed SSH IPs form value."""
    _FakeUfwSshClient.output = """
Status: active

To                         Action      From
--                         ------      ----
22/tcp                     ALLOW IN    Anywhere
22/tcp (v6)                ALLOW IN    Anywhere (v6)
"""
    _FakeUfwSshClient.errors = ""
    monkeypatch.setattr(core.paramiko, "SSHClient", _FakeUfwSshClient)
    vps = core.VPS()
    vps.hostname = "test-vps"
    vps.ip = "127.0.0.1"
    vps.user = "mani"
    vps.user_pw = "fresh-password"

    enabled, allowed_ips = vps.fetch_ufw_settings()

    assert enabled is True
    assert allowed_ips == ""


def test_parse_ufw_numbered_status_returns_acl_rows() -> None:
    """Numbered UFW status output is parsed as ACL rows with comments."""
    output = """
Status: active

     To                         Action      From
     --                         ------      ----
[ 1] 1194/udp                   ALLOW IN    Anywhere                   # OpenVPN UDP
[ 2] 22/tcp                     ALLOW IN    213.188.243.180            # SSH from public IP
[ 3] 8000/tcp                   ALLOW IN    10.8.1.0/24                # PBGui via VPN
[ 4] 1194/udp (v6)              ALLOW IN    Anywhere (v6)              # OpenVPN UDP
"""

    parsed = service_mod._parse_ufw_numbered_status(output)

    assert parsed["enabled"] is True
    assert parsed["rules"][0] == {
        "number": 1,
        "to": "1194/udp",
        "action": "ALLOW IN",
        "from": "Anywhere",
        "comment": "OpenVPN UDP",
    }
    assert parsed["rules"][2]["from"] == "10.8.1.0/24"
    assert parsed["fingerprint"]


def test_ufw_safety_blocks_current_ssh_and_vpn_lockout() -> None:
    """UFW safety blocks changes that would drop the current SSH/VPN path."""
    rules = [
        {"number": 1, "to": "22/tcp", "action": "ALLOW IN", "from": "10.8.1.0/24", "comment": "SSH from VPN"},
        {"number": 2, "to": "1194/udp", "action": "ALLOW IN", "from": "Anywhere", "comment": "OpenVPN UDP"},
        {"number": 3, "to": "8000/tcp", "action": "ALLOW IN", "from": "10.8.1.0/24", "comment": "PBGui via VPN"},
    ]

    ssh_blocked = service_mod._simulate_ufw_changes(rules, [1], [], True, "10.8.1.23")
    vpn_blocked = service_mod._simulate_ufw_changes(rules, [2], [], True, "10.8.1.23")
    pbgui_warning = service_mod._simulate_ufw_changes(rules, [3], [], True, "10.8.1.23")

    assert ssh_blocked["ok"] is False
    assert any("SSH" in item for item in ssh_blocked["blocking"])
    assert vpn_blocked["ok"] is False
    assert any("1194/udp" in item for item in vpn_blocked["blocking"])
    assert pbgui_warning["ok"] is True
    assert any("8000/tcp" in item for item in pbgui_warning["warnings"])


def test_apply_ufw_rules_rejects_stale_fingerprint(monkeypatch) -> None:
    """Applying UFW changes rejects stale rule fingerprints before running commands."""
    service = object.__new__(VPSManagerService)
    current = {
        "enabled": True,
        "rules": [{"number": 1, "to": "22/tcp", "action": "ALLOW IN", "from": "Anywhere", "comment": "SSH"}],
        "fingerprint": "fresh",
        "ssh_client_ip": "203.0.113.5",
    }

    monkeypatch.setattr(service, "read_ufw_rules", lambda hostname, sudo_pw=None: current)
    monkeypatch.setattr(service, "_run_ufw_shell", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not run ufw")))

    with pytest.raises(ValueError, match="changed since they were loaded"):
        service.apply_ufw_rules("", {"fingerprint": "stale", "enabled": True, "delete_numbers": [], "add_rules": []}, "pw")


def test_build_ufw_apply_script_deletes_descending_and_never_resets() -> None:
    """UFW apply script deletes numbered rules descending without resetting the firewall."""
    service = object.__new__(VPSManagerService)

    script = service._build_ufw_apply_script(
        True,
        [2, 10, 2],
        [{"port": "8000", "proto": "tcp", "from": "10.8.1.0/24", "comment": "PBGui via VPN"}],
    )

    assert "ufw reset" not in script
    assert script.index("ufw delete 10") < script.index("ufw delete 2")
    assert "ufw allow from 10.8.1.0/24 to any port 8000 proto tcp comment 'PBGui via VPN'" in script
    assert "ufw --force enable" in script


def test_vps_status_keeps_coindata_expected_without_cmc_key() -> None:
    """A second master still displays CoinData as expected without a CMC key."""
    service = object.__new__(VPSManagerService)
    service._vps_package_status_cache = {}
    service._vps_ssh_ok_cache = {}
    service._build_vps_overview_row = lambda hostname, host_state: {"updates": "N/A"}
    service._get_live_vps_package_status = lambda vps, host_state: None
    service._build_remote_pbgui_github_status = lambda host_state: ""
    service._build_remote_pb7_github_status = lambda host_state: ""
    service._host_online = lambda host_state: True
    service._host_telemetry_fresh = lambda host_state: True
    service._host_telemetry_age = lambda host_state: 1.0
    service._host_meta = lambda host_state: {"coindata_configured": False}
    service._build_remote_server_metrics = lambda hostname, host_state: None
    service._get_vps_systemd_migration_status = lambda vps, host_state, quick=False: {}
    vps = SimpleNamespace(
        hostname="test-vps",
        coinmarketcap_api_key="old-api-key",
        init_status="successful",
        setup_status="successful",
        update_status="successful",
        user_pw="pw",
        command_text="Update PBGui",
        last_update="",
        last_setup="",
        last_init="",
        remote_pbgui_dir="software/pbgui",
        user="mani",
        is_vps_ssh_open=lambda: True,
        is_vps_in_hosts=lambda: True,
    )

    status = service._build_vps_status(vps, {}, False)

    assert status["coindata_configured"] is True


def test_master_overview_row_is_online_without_remote_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    """The local master overview row reflects the responding local API."""
    service = object.__new__(VPSManagerService)
    service._get_pbgui_release = lambda: {"version": "v1.0", "current_branch": "main", "current_commit": "abcdef123"}
    service._get_pb7_release = lambda: {"version": "v7.0", "current_branch": "master", "current_commit": "123abcdef"}
    service._get_local_package_status = lambda: {"reboot": False, "upgrades": "0"}
    service._build_master_pbgui_github_status = lambda branch, commit: ""
    service._build_master_pb7_github_status = lambda branch, commit: ""
    monkeypatch.setattr(service_mod, "load_ini", lambda section, parameter: "")

    row = service._build_master_overview_row()

    assert row["online"] is True


def test_run_vps_command_blocks_stale_optional_reenable_from_remote_meta() -> None:
    """A second master does not re-enable VPS optional services from stale local config."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture the command extra vars instead of running Ansible."""
        del vps, debug
        captured["extra_vars"] = extra_vars or {}

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._require_vps = lambda hostname: SimpleNamespace(
        hostname=hostname,
        user_pw=None,
        coinmarketcap_api_key="old-api-key",
    )
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._get_monitor_state = lambda: {"host_meta": {"test-vps": {"coindata_configured": False}}}
    service._load_vps_optional_config_pending = lambda vps: {}

    service.run_vps_command(
        token="token",
        hostname="test-vps",
        command="vps-update-pbgui",
        command_text="Update PBGui",
    )

    assert "bucket" not in captured["extra_vars"]
    assert captured["extra_vars"]["coinmarketcap_api_key"] == ""


def test_start_vps_deploy_host_blocks_active_vps_task() -> None:
    """A VPS deploy cannot start a second playbook for the same active host."""
    calls: list[str] = []
    service = object.__new__(VPSManagerService)
    service._host_task_start_locks = {}
    service._host_task_start_locks_lock = threading.Lock()
    service.vpsmanager = SimpleNamespace(update_vps=lambda *args, **kwargs: calls.append("update"))
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._should_treat_vps_process_as_active = lambda vps: True
    service._vps_playbook_process_exists = lambda vps: False
    service._require_vps = lambda hostname: SimpleNamespace(
        hostname=hostname,
        path=Path("/tmp") / hostname,
        user_pw="pw",
        command="vps-update-pb",
        command_text="Update PBGui and PB7",
    )

    with pytest.raises(ValueError, match="already has an active VPS task"):
        service._start_vps_deploy_host(
            "token",
            hostname="test-vps",
            command="vps-update-pb",
            debug=False,
            extra_vars=None,
        )

    assert calls == []


def test_run_vps_deploy_skips_active_hosts_and_starts_free_hosts() -> None:
    """Overview deploys continue with free hosts and report already-running hosts."""
    service = object.__new__(VPSManagerService)
    service.get_vps_deploy_settings = lambda: {}
    service._vps_deploy_extra_vars = lambda command, settings: {}

    def fake_start(token, *, hostname, command, debug, extra_vars):
        """Start free hosts and reject one active host."""
        del token, debug, extra_vars
        if hostname == "busy-vps":
            raise ValueError("busy-vps already has an active VPS task. Wait for it to finish before starting Update PBGui and PB7.")
        return {
            "command": command,
            "command_text": "Update PBGui and PB7",
            "started_at": "2026-06-11 18:00:00",
            "run_id": f"run-{hostname}",
            "filename": f"{command}--run-{hostname}.log",
            "file_alias": f"VPSAction:{hostname}:{command}--run-{hostname}.log",
        }

    service._start_vps_deploy_host = fake_start

    result = service.run_vps_deploy(
        "token",
        ["busy-vps", "free-vps"],
        command="vps-update-pb",
        mode="parallel",
        record_history=False,
    )

    assert result["count"] == 2
    assert result["started_count"] == 1
    assert result["skipped_hosts"] == [{
        "hostname": "busy-vps",
        "reason": "busy-vps already has an active VPS task. Wait for it to finish before starting Update PBGui and PB7.",
    }]
    assert result["entry"]["host_logs"]["busy-vps"]["status"] == "skipped"
    assert result["entry"]["host_logs"]["free-vps"]["run_id"] == "run-free-vps"


def test_validate_and_stage_vps_deploy_host_skips_active_host() -> None:
    """Password deploy flow returns skipped for active hosts instead of failing the batch."""
    updates: list[dict[str, dict[str, object]]] = []
    service = object.__new__(VPSManagerService)
    service._deploy_sessions = {}
    service._deploy_sessions_lock = threading.Lock()
    service._validate_vps_user_password = lambda hostname, password, **kwargs: None
    service._store_session_secrets = lambda token, hostname, values: None
    service.get_vps_deploy_settings = lambda: {}
    service._vps_deploy_extra_vars = lambda command, settings: {}
    service._record_vps_deploy = lambda **kwargs: {
        "id": "entry-1",
        "command": kwargs["command"],
        "command_text": "Update Linux",
        "mode": kwargs["mode"],
        "hostnames": list(kwargs["hostnames"]),
        "host_logs": dict(kwargs["host_logs"]),
    }

    def fake_update(entry_id, *, host_logs=None):
        """Capture host-log updates."""
        updates.append(dict(host_logs or {}))
        return {
            "id": entry_id,
            "command": "vps-update",
            "command_text": "Update Linux",
            "mode": "parallel",
            "hostnames": ["busy-vps"],
            "host_logs": dict(host_logs or {}),
        }

    service._update_vps_deploy_entry = fake_update
    service._start_vps_deploy_host = lambda *args, **kwargs: (_ for _ in ()).throw(
        ValueError("busy-vps already has an active VPS task. Wait for it to finish before starting Update Linux.")
    )

    result = service.validate_and_stage_vps_deploy_host(
        "token",
        hostnames=["busy-vps"],
        hostname="busy-vps",
        password="pw",
        command="vps-update",
        mode="parallel",
    )

    assert result["started"] is False
    assert result["skipped"] is True
    assert result["hostname"] == "busy-vps"
    assert updates[0]["busy-vps"]["status"] == "skipped"


def test_run_vps_command_keeps_pending_optional_values() -> None:
    """Pending local CoinData values override stale missing remote metadata during updates."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture update arguments instead of running Ansible."""
        del debug
        captured["coinmarketcap_api_key"] = vps.coinmarketcap_api_key
        captured["extra_vars"] = extra_vars

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    vps = SimpleNamespace(
        hostname="test-vps",
        user_pw=None,
        coinmarketcap_api_key="new-api-key",
        save=lambda: None,
    )
    service._require_vps = lambda hostname: vps
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._get_monitor_state = lambda: {"host_meta": {"test-vps": {"coindata_configured": False}}}
    service._host_telemetry_fresh = lambda state: True
    service._load_vps_optional_config_pending = lambda vps: {"coinmarketcap_api_key": "new-api-key"}
    service._write_vps_optional_config_pending = lambda vps, values: None

    service.run_vps_command(
        token="token",
        hostname="test-vps",
        command="vps-update-pbgui",
        command_text="Update PBGui",
    )

    assert captured["coinmarketcap_api_key"] == "new-api-key"
    assert captured["extra_vars"] is None


def test_sync_vps_config_from_host_meta_persists_remote_optional_values() -> None:
    """Fresh VPS metadata updates the local master's saved optional settings."""
    saves: list[str] = []
    service = object.__new__(VPSManagerService)
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(
        coinmarketcap_api_key="old-api-key",
        save=lambda: saves.append(vps.coinmarketcap_api_key),
    )
    host_state = {
        "connection": {"status": "connected"},
        "stream": {"last_update": 1_700_000_000},
        "meta": {
            "coinmarketcap_api_key": "remote-api-key",
        },
    }
    service._host_telemetry_fresh = lambda state: True

    service._sync_vps_config_from_host_meta(vps, host_state)

    assert vps.coinmarketcap_api_key == "remote-api-key"
    assert saves == ["remote-api-key"]


def test_sync_vps_config_from_host_meta_keeps_pending_local_optional_values(tmp_path: Path) -> None:
    """Stale live metadata cannot overwrite locally saved pending optional settings."""
    saves: list[str] = []
    service = object.__new__(VPSManagerService)
    service._host_telemetry_fresh = lambda state: True
    vps = SimpleNamespace(
        hostname="test-vps",
        path=tmp_path,
        coinmarketcap_api_key="",
        save=lambda: saves.append(vps.coinmarketcap_api_key),
    )
    service._write_vps_optional_config_pending(vps, {"coinmarketcap_api_key": ""})
    stale_host_state = {
        "meta": {
            "coinmarketcap_api_key": "old-api-key",
        },
    }

    service._sync_vps_config_from_host_meta(vps, stale_host_state)

    assert vps.coinmarketcap_api_key == ""
    assert service._load_vps_optional_config_pending(vps) == {"coinmarketcap_api_key": ""}
    assert saves == []

    confirmed_host_state = {
        "meta": {
            "coinmarketcap_api_key": "",
        },
    }
    service._sync_vps_config_from_host_meta(vps, confirmed_host_state)

    assert service._load_vps_optional_config_pending(vps) == {}
    assert saves == []


def test_sync_vps_config_from_host_meta_persists_remote_firewall_values() -> None:
    """Fresh VPS metadata updates locally saved firewall settings on other masters."""
    saves: list[tuple[bool, int, str]] = []
    service = object.__new__(VPSManagerService)
    service._host_telemetry_fresh = lambda state: True
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(
        firewall=False,
        firewall_ssh_port=22,
        firewall_ssh_ips="",
        coinmarketcap_api_key="",
        save=lambda: saves.append((vps.firewall, vps.firewall_ssh_port, vps.firewall_ssh_ips)),
    )
    host_state = {
        "connection": {"status": "connected"},
        "stream": {"last_update": 1_700_000_000},
        "meta": {
            "firewall_settings_present": True,
            "firewall": True,
            "firewall_ssh_port": "2222",
            "firewall_ssh_ips": "198.51.100.1,203.0.113.7",
        },
    }

    service._sync_vps_config_from_host_meta(vps, host_state)

    assert vps.firewall is True
    assert vps.firewall_ssh_port == 2222
    assert vps.firewall_ssh_ips == "198.51.100.1,203.0.113.7"
    assert saves == [(True, 2222, "198.51.100.1,203.0.113.7")]


def test_sync_vps_config_from_host_meta_ignores_missing_firewall_section() -> None:
    """Old VPS metadata without [firewall] does not overwrite saved firewall settings."""
    saves: list[tuple[bool, int, str]] = []
    service = object.__new__(VPSManagerService)
    service._host_telemetry_fresh = lambda state: True
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(
        firewall=True,
        firewall_ssh_port=2222,
        firewall_ssh_ips="198.51.100.1",
        coinmarketcap_api_key="",
        save=lambda: saves.append((vps.firewall, vps.firewall_ssh_port, vps.firewall_ssh_ips)),
    )
    host_state = {
        "connection": {"status": "connected"},
        "stream": {"last_update": 1_700_000_000},
        "meta": {
            "firewall_settings_present": False,
            "firewall": False,
            "firewall_ssh_port": "22",
            "firewall_ssh_ips": "",
        },
    }

    service._sync_vps_config_from_host_meta(vps, host_state)

    assert vps.firewall is True
    assert vps.firewall_ssh_port == 2222
    assert vps.firewall_ssh_ips == "198.51.100.1"
    assert saves == []


def test_run_vps_command_uses_fresh_remote_optional_values() -> None:
    """A second master's update command uses the VPS-reported optional values."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture the updated VPS object instead of running Ansible."""
        del debug
        captured["coinmarketcap_api_key"] = vps.coinmarketcap_api_key
        captured["extra_vars"] = extra_vars

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(
        hostname="test-vps",
        user_pw=None,
        coinmarketcap_api_key="old-api-key",
        save=lambda: None,
    )
    service._require_vps = lambda hostname: vps
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._get_monitor_state = lambda: {
        "connections": {"connections": {"test-vps": {"status": "connected"}}},
        "streams": {"test-vps": {"last_update": 1_700_000_000}},
        "host_meta": {
            "test-vps": {
                "coindata_configured": True,
                "coinmarketcap_api_key": "remote-api-key",
            }
        },
    }
    service._host_telemetry_fresh = lambda state: True

    service.run_vps_command(
        token="token",
        hostname="test-vps",
        command="vps-update-pbgui",
        command_text="Update PBGui",
    )

    assert captured["coinmarketcap_api_key"] == "remote-api-key"
    assert captured["extra_vars"] is None


def test_run_vps_command_uses_master_playbook_for_remote_master_updates() -> None:
    """Remote master updates use master playbooks, not VPS playbooks."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture extra vars instead of running Ansible."""
        captured["command"] = vps.command
        del debug
        captured["extra_vars"] = extra_vars

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(hostname="manibot02", user="mani", remote_pbgui_dir="", user_pw=None, coinmarketcap_api_key="", save=lambda: None)
    service._require_vps = lambda hostname: vps
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._get_monitor_state = lambda: {
        "connections": {"connections": {"manibot02": {"status": "connected"}}},
        "streams": {"manibot02": {"last_update": 1_700_000_000}},
        "host_meta": {"manibot02": {"role": "master", "coindata_configured": False}},
    }
    service._host_telemetry_fresh = lambda state: True

    service.run_vps_command(token="token", hostname="manibot02", command="vps-update-pbgui", command_text="Update PBGui")

    assert captured["command"] == "master-update-pbgui"
    assert captured["extra_vars"] == {
        "target_hosts": "manibot02",
        "pbgdir": "/home/mani/software/pbgui",
        "pbgui_python": "/home/mani/software/venv_pbgui/bin/python",
        "pb7dir": "/home/mani/software/pb7",
        "pb7venv": "/home/mani/software/venv_pb7",
        "coinmarketcap_api_key": "",
    }


def test_vps_action_log_resolves_remote_master_task_logs() -> None:
    """VPS task log aliases can point at remote-master master-update logs."""
    resolved = async_logs.resolve_local_log_path("VPSAction:manibot02:master-update-pbgui")

    assert resolved is not None
    assert resolved.as_posix().endswith("data/vpsmanager/hosts/manibot02/master-update-pbgui.log")


def test_pbcoindata_is_expected_without_coinmarketcap_key() -> None:
    """PBCoinData remains expected because it updates exchange mappings without CMC."""

    monitor = object.__new__(VPSMonitor)
    monitor.store = SimpleNamespace(host_meta={"manibot90": {"optional_services": {"PBCoinData": False}}})
    monitor._local_optional_service_expected = lambda hostname, service_name: True

    assert monitor._optional_service_expected("manibot90", "PBCoinData") is True


def test_optional_pbrun_remote_unconfigured_prevents_restart() -> None:
    """Remote PBRun=false prevents auto-restarting PBRun on hosts without local bots."""

    monitor = object.__new__(VPSMonitor)
    monitor.store = SimpleNamespace(host_meta={"manibot01": {"optional_services": {"PBRun": False}}})
    monitor._local_optional_service_expected = lambda hostname, service_name: None

    assert monitor._optional_service_expected("manibot01", "PBRun") is False
    assert monitor._disabled_service_check("PBRun")["reason"] == "No local V7 run configs are enabled for this host"


def test_optional_pbdata_remote_unconfigured_prevents_restart() -> None:
    """Remote PBData=false prevents auto-restarting PBData when no users need it."""

    monitor = object.__new__(VPSMonitor)
    monitor.store = SimpleNamespace(host_meta={"manibot01": {"optional_services": {"PBData": False}}})
    monitor._local_optional_service_expected = lambda hostname, service_name: None

    assert monitor._optional_service_expected("manibot01", "PBData") is False
    assert monitor._disabled_service_check("PBData")["reason"] == "No PBData fetch_users or trades_users are configured"


def test_auto_heal_skips_disabled_optional_service() -> None:
    """Auto-heal does not restart services that are explicitly not expected."""

    monitor = object.__new__(VPSMonitor)
    monitor._auto_restart = True
    restarted: list[str] = []

    async def fake_restart(hostname: str, service_name: str) -> bool:
        restarted.append(f"{hostname}:{service_name}")
        return True

    async def fake_read_agent_json(hostname: str, filename: str, **kwargs) -> dict:
        del hostname, filename, kwargs
        return {"services": {
            "PBCluster": {"status": monitor_mod.ServiceStatus.RUNNING.value, "pid": 1, "error": None, "was_restarted": False},
            "PBRun": {"status": monitor_mod.ServiceStatus.STOPPED.value, "pid": None, "error": "down", "was_restarted": False},
            "PBData": {"status": monitor_mod.ServiceStatus.RUNNING.value, "pid": 2, "error": None, "was_restarted": False},
            "PBCoinData": {"status": monitor_mod.ServiceStatus.RUNNING.value, "pid": 3, "error": None, "was_restarted": False},
        }}

    monitor._read_monitor_agent_json = fake_read_agent_json
    monitor._optional_service_expected = lambda hostname, service_name: service_name != "PBRun"
    monitor._restart_service = fake_restart

    result = asyncio.run(monitor._check_and_heal_services(["manibot01"]))

    assert restarted == []
    assert result["manibot01"]["PBRun"]["status"] == monitor_mod.ServiceStatus.DISABLED.value


def test_auto_heal_does_not_restart_remote_services() -> None:
    """Remote service checks alert without restarting services owned by another host."""

    monitor = object.__new__(VPSMonitor)
    monitor._auto_restart = True
    restarted: list[str] = []

    async def fake_restart(hostname: str, service_name: str) -> bool:
        restarted.append(f"{hostname}:{service_name}")
        return True

    async def fake_read_agent_json(hostname: str, filename: str, **kwargs) -> dict:
        del hostname, filename, kwargs
        return {"services": {
            "PBCluster": {"status": monitor_mod.ServiceStatus.RUNNING.value, "pid": 1, "error": None, "was_restarted": False},
            "PBRun": {"status": monitor_mod.ServiceStatus.RUNNING.value, "pid": 2, "error": None, "was_restarted": False},
            "PBData": {"status": monitor_mod.ServiceStatus.STOPPED.value, "pid": None, "error": "down", "was_restarted": False},
            "PBCoinData": {"status": monitor_mod.ServiceStatus.RUNNING.value, "pid": 3, "error": None, "was_restarted": False},
        }}

    monitor._local_master_hostname = lambda: "local-master"
    monitor._read_monitor_agent_json = fake_read_agent_json
    monitor._optional_service_expected = lambda hostname, service_name: True
    monitor._restart_service = fake_restart

    result = asyncio.run(monitor._check_and_heal_services(["manibot02"]))

    assert restarted == []
    assert result["manibot02"]["PBData"]["status"] == monitor_mod.ServiceStatus.STOPPED.value
    assert result["manibot02"]["PBData"]["was_restarted"] is False


def test_host_meta_falls_back_to_direct_probe_when_agent_cache_missing() -> None:
    """Host metadata refresh reads directly over SSH when the agent cache is unavailable."""

    class FakeStore:
        """Minimal monitor store for host metadata collection."""

        def __init__(self) -> None:
            """Initialize captured metadata."""

            self.host_meta = {}
            self.streams = {}
            self.changed = SimpleNamespace(set=lambda: None)

        def update_host_meta(self, hostname: str, data: dict) -> None:
            """Capture host metadata updates."""

            self.host_meta[hostname] = data

        def update_stream_info(self, hostname: str, info: dict) -> None:
            """Capture monitor-agent diagnostics."""

            self.streams[hostname] = info

    class FakePool:
        """Return a missing cache first and fresh direct metadata second."""

        def __init__(self) -> None:
            """Initialize executed command capture."""

            self.commands = []

        def get_remote_pbgui_dir(self, hostname: str) -> str:
            """Return the configured remote PBGui checkout."""

            del hostname
            return "software/pbgui"

        async def run(self, hostname: str, command: str, timeout: float = 0, check: bool = True):
            """Simulate cache miss followed by direct SSH host-meta output."""

            del hostname, timeout, check
            self.commands.append(command)
            if "host_meta.json" in command:
                return SimpleNamespace(exit_status=1, stdout="", stderr="missing")
            return SimpleNamespace(
                exit_status=0,
                stdout=json.dumps({"pbgv": "v1.90.8", "pbgc": "4573316", "pbgb": "main"}),
                stderr="",
            )

    monitor = object.__new__(VPSMonitor)
    monitor.pool = FakePool()
    monitor.store = FakeStore()
    monitor._last_host_meta_collect = {}
    monitor._last_package_status_collect = {}
    monitor._debug_logging = False
    monitor._cache_host_snapshot = lambda hostname: None

    asyncio.run(monitor._collect_host_meta("manibot01", force=True))

    assert monitor.store.host_meta["manibot01"]["pbgv"] == "v1.90.8"
    assert monitor.store.host_meta["manibot01"]["source"] == "direct-ssh"
    assert len(monitor.pool.commands) == 2
    assert "python3 -u" in monitor.pool.commands[1]


def test_vps_manager_unknown_context_host_resets_to_overview() -> None:
    """A stale URL hash for a deleted VPS does not raise through the websocket push path."""

    class FakeService:
        """Service double raising the same error as a deleted VPS detail lookup."""

        def build_vps_detail(self, token: str, hostname: str, quick: bool = False) -> dict:
            """Raise for the deleted host."""

            del token, quick
            raise ValueError(f"Unknown VPS: {hostname}")

    context = {"view": "vps", "hostname": "manibot93", "token": "token"}

    assert vps_manager_api._build_quick_detail_for_context(FakeService(), context) is None
    assert context["view"] == "overview"
    assert context["hostname"] == ""


def test_pbcluster_expected_only_for_sync_enabled_nodes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PBCluster service alerts are tied to explicit Cluster Sync membership."""
    cluster_dir = tmp_path / "data" / "cluster"
    cluster_dir.mkdir(parents=True)
    (cluster_dir / "cluster_nodes.json").write_text(json.dumps({
        "nodes": {
            "node-a": {"hostname": "manibot90", "pbname": "manibot90", "enabled": True, "sync_enabled": True},
            "node-b": {"hostname": "manibot01", "pbname": "manibot01", "enabled": True, "sync_enabled": False},
        }
    }), encoding="utf-8")
    monkeypatch.setattr(monitor_mod, "PBGDIR", str(tmp_path))
    monitor = object.__new__(VPSMonitor)

    assert monitor._optional_service_expected("manibot90", "PBCluster") is True
    assert monitor._optional_service_expected("manibot01", "PBCluster") is False
    assert monitor._optional_service_expected("unknown-host", "PBCluster") is False
    assert monitor._disabled_service_check("PBCluster")["reason"] == "Cluster Sync is not enabled for this node"


def test_pbcluster_sync_off_alerts_clear_without_recovery_telegram(monkeypatch: pytest.MonkeyPatch) -> None:
    """False PBCluster alerts from Sync Off nodes clear silently instead of spamming recoveries."""
    monitor = object.__new__(VPSMonitor)
    monitor._alerts = {
        "service:manibot01:PBCluster": monitor_mod.AlertRecord(
            id="service:manibot01:PBCluster",
            kind=monitor_mod.ALERT_KIND_SERVICE,
            host="manibot01",
            name="PBCluster",
            severity="error",
            summary="PBCluster is down on manibot01",
            details="PBCluster is down on manibot01.",
            active=True,
        )
    }
    monitor.pool = SimpleNamespace(get_status_summary=lambda: {"connections": {}})
    monitor.store = SimpleNamespace(
        system={},
        instances={},
        services={},
        streams={},
        changed=SimpleNamespace(set=lambda: None),
    )
    monitor._load_alert_routes = lambda: None
    monitor._prune_alert_history = lambda now=None: False
    monitor._save_alert_state = lambda: None
    monitor._optional_service_expected = lambda host, service: False if service == "PBCluster" else True
    recoveries: list[str] = []

    async def fake_recovery(alert) -> None:
        recoveries.append(alert.name)

    monkeypatch.setattr(monitor, "_emit_recovery_event", fake_recovery)

    asyncio.run(monitor._sync_live_alerts())

    assert monitor._alerts["service:manibot01:PBCluster"].active is False
    assert recoveries == []


def test_pbcluster_check_requires_systemd_unit() -> None:
    """PBCluster is not considered healthy from a legacy PID fallback."""
    commands: list[str] = []

    class FakePool:
        """Return a missing systemd unit and fail if legacy fallback is used."""

        async def run(self, hostname: str, command: str, timeout: int = 0):
            """Capture commands and report the PBCluster unit as missing."""
            del hostname, timeout
            commands.append(command)
            return SimpleNamespace(exit_status=0, stdout="LoadState=not-found\n", stderr="")

        def get_remote_pbgui_dirs(self, hostname: str) -> list[str]:
            """Legacy PID fallback must not be used for PBCluster."""
            del hostname
            raise AssertionError("PBCluster used legacy PID fallback")

    monitor = object.__new__(VPSMonitor)
    monitor.pool = FakePool()
    monitor._optional_service_expected = lambda hostname, service: True

    result = asyncio.run(monitor._check_service("manibot90", monitor_mod.MONITORED_SERVICES["PBCluster"]))

    assert result["status"] == monitor_mod.ServiceStatus.STOPPED.value
    assert result["manager"] == "systemd"
    assert "systemd user unit" in result["error"]
    assert not any("data/pid/pbcluster.pid" in command for command in commands)


def test_monitor_agent_service_requires_systemd_unit() -> None:
    """PBMonitorAgent is exposed as a systemd-only monitored service."""
    commands: list[str] = []

    class FakePool:
        """Return a missing systemd unit and fail if PID fallback is used."""

        async def run(self, hostname: str, command: str, timeout: int = 0):
            """Capture commands and report the monitor-agent unit as missing."""
            del hostname, timeout
            commands.append(command)
            return SimpleNamespace(exit_status=0, stdout="LoadState=not-found\n", stderr="")

        def get_remote_pbgui_dirs(self, hostname: str) -> list[str]:
            """Legacy PID fallback must not be used for PBMonitorAgent."""
            del hostname
            raise AssertionError("PBMonitorAgent used legacy PID fallback")

    monitor = object.__new__(VPSMonitor)
    monitor.pool = FakePool()
    monitor._optional_service_expected = lambda hostname, service: True

    result = asyncio.run(monitor._check_service("manibot90", monitor_mod.MONITORED_SERVICES["PBMonitorAgent"]))

    assert monitor_mod.MONITORED_SERVICE_SYSTEMD_UNITS["PBMonitorAgent"] == "pbgui-monitor-agent.service"
    assert result["status"] == monitor_mod.ServiceStatus.STOPPED.value
    assert result["manager"] == "systemd"
    assert result["unit"] == "pbgui-monitor-agent.service"
    assert "systemd user unit" in result["error"]
    assert not any("data/pid/pbmonitoragent.pid" in command for command in commands)


def test_monitor_agent_prefers_live_legacy_process_over_inactive_systemd(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live legacy PBData process must not be reported down because its unit is inactive."""

    monkeypatch.setattr(
        monitor_agent,
        "_systemd_service_status",
        lambda unit: {
            "status": "stopped",
            "pid": None,
            "error": "systemd inactive",
            "was_restarted": False,
            "manager": "systemd",
            "unit": unit,
        },
    )
    monkeypatch.setattr(
        monitor_agent,
        "_pid_file_service_status",
        lambda pid_file, process_match: {
            "status": "running",
            "pid": 123,
            "error": None,
            "was_restarted": False,
        },
    )

    status = monitor_agent._service_status("pbgui-pbdata.service", "data/pid/pbdata.pid", "pbdata.py")

    assert status["status"] == "running"
    assert status["pid"] == 123
    assert status["manager"] == "legacy"
    assert status["unit"] == "pbgui-pbdata.service"


def test_monitor_agent_locally_restarts_expected_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """PBMonitorAgent heals expected local app services and disables unconfigured ones."""

    (tmp_path / "pbgui.ini").write_text(
        "[main]\npbname=local-node\n[pbdata]\nfetch_users=['user1']\n[vps_monitor]\nauto_restart=true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(monitor_agent, "PBGDIR", tmp_path)
    monkeypatch.setattr(monitor_agent, "DATA_DIR", tmp_path / "data" / "monitor_agent")

    def fake_status(unit: str, pid_file: str, process_match: str) -> dict:
        del pid_file, process_match
        if unit == "pbgui-monitor-agent.service":
            return {"status": "running", "pid": 10, "error": None, "was_restarted": False, "manager": "systemd", "unit": unit}
        return {"status": "stopped", "pid": None, "error": "down", "was_restarted": False, "manager": "systemd", "unit": unit}

    restarted: list[str] = []

    def fake_restart(service_name: str, unit: str) -> tuple[bool, str]:
        restarted.append(f"{service_name}:{unit}")
        return True, ""

    monkeypatch.setattr(monitor_agent, "_service_status", fake_status)
    monkeypatch.setattr(monitor_agent, "_restart_systemd_service", fake_restart)

    monitor_agent._run_service_status()

    payload = json.loads((tmp_path / "data" / "monitor_agent" / "service_status.json").read_text(encoding="utf-8"))
    services = payload["services"]

    assert restarted == ["PBData:pbgui-pbdata.service"]
    assert services["PBData"]["status"] == "restarting"
    assert services["PBData"]["was_restarted"] is True
    assert services["PBRun"]["status"] == "disabled"
    assert services["PBCoinData"]["status"] == "disabled"
    assert services["PBMonitorAgent"]["status"] == "running"


def test_monitor_agent_first_local_restart_is_immediate(monkeypatch: pytest.MonkeyPatch) -> None:
    """The first detected local service failure is not delayed by cooldown."""

    monkeypatch.setattr(monitor_agent, "_SERVICE_RESTART_HISTORY", {})

    allowed, reason = monitor_agent._restart_allowed("PBData", now=1000.0)

    assert allowed is True
    assert reason == ""


def test_pbcluster_restart_does_not_use_legacy_starter_fallback() -> None:
    """PBCluster auto-restart does not create orphan starter.py processes."""
    commands: list[str] = []

    class FakePool:
        """Return systemd-unit-missing for restart attempts."""

        async def run(self, hostname: str, command: str, timeout: int = 0):
            """Capture commands and simulate a missing unit from the systemd probe."""
            del hostname, timeout
            commands.append(command)
            return SimpleNamespace(exit_status=3, stdout="", stderr="")

        def get_remote_pbgui_dirs(self, hostname: str) -> list[str]:
            """Legacy starter fallback must not be used for PBCluster."""
            del hostname
            return ["/home/mani/software/pbgui"]

    monitor = object.__new__(VPSMonitor)
    monitor.pool = FakePool()
    monitor._optional_service_expected = lambda hostname, service: True
    monitor._can_restart = lambda hostname, service: True

    restarted = asyncio.run(monitor._restart_service("manibot90", "PBCluster"))

    assert restarted is False
    assert not any("starter.py" in command for command in commands)


def test_save_vps_config_starts_remote_optional_apply(tmp_path: Path) -> None:
    """Saving changed optional settings starts the targeted remote apply playbook."""
    captured: dict[str, object] = {}
    saves: list[str] = []

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture the targeted remote apply instead of running Ansible."""
        del debug
        captured["command"] = vps.command
        captured["command_text"] = vps.command_text
        captured["extra_vars"] = extra_vars or {}
        vps.command_run_id = "run-123"

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._store_session_secrets = lambda token, hostname, form: None
    service._session_secret_value = lambda token, hostname, field: ""
    service._ensure_coinmarketcap_key_clear_allowed = lambda vps, next_key: None
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._build_vps_config = lambda token, vps: {"coinmarketcap_api_key": vps.coinmarketcap_api_key}
    vps = SimpleNamespace(
        hostname="test-vps",
        path=tmp_path,
        user="mani",
        user_pw=None,
        swap="2G",
        coinmarketcap_api_key="old-api-key",
        remote_pbgui_dir="/home/mani/software/pbgui",
        firewall=True,
        firewall_ssh_port=22,
        firewall_ssh_ips="",
        command_run_id="",
        save=lambda: saves.append(vps.coinmarketcap_api_key),
        _task_log_path=lambda command, fallback: tmp_path / f"{command}.log",
    )
    service._require_vps = lambda hostname: vps

    result = service.save_vps_config(
        "token",
        "test-vps",
        {
            "swap": "2G",
            "coinmarketcap_api_key": "",
            "install_dir": "/home/mani/software",
            "firewall": True,
            "firewall_ssh_port": 22,
            "firewall_ssh_ips": "",
        },
    )

    assert captured["command"] == "vps-apply-config"
    assert captured["command_text"] == "Apply VPS Config"
    assert captured["extra_vars"] == {
        "apply_optional_config": True,
        "apply_firewall": False,
        "apply_swap": False,
        "coinmarketcap_api_key": "",
    }
    assert result["optional_changed"] is True
    assert result["remote_apply"]["started"] is True
    assert result["remote_apply"]["run_id"] == "run-123"
    assert result["config"] == {"coinmarketcap_api_key": ""}
    assert service._load_vps_optional_config_pending(vps) == {"coinmarketcap_api_key": ""}
    assert saves


def test_save_vps_config_starts_remote_firewall_apply_without_optional_pending(tmp_path: Path) -> None:
    """Saving changed firewall settings starts targeted apply without optional pending state."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture the targeted firewall apply instead of running Ansible."""
        del debug
        captured["command"] = vps.command
        captured["command_text"] = vps.command_text
        captured["extra_vars"] = extra_vars or {}
        vps.command_run_id = "run-firewall"

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._store_session_secrets = lambda token, hostname, form: None
    service._session_secret_value = lambda token, hostname, field: "fresh-password"
    service._ensure_coinmarketcap_key_clear_allowed = lambda vps, next_key: None
    service._apply_session_secrets_to_vps = lambda token, vps: setattr(vps, "user_pw", "fresh-password")
    service._build_vps_config = lambda token, vps: {
        "coinmarketcap_api_key": vps.coinmarketcap_api_key,
        "firewall": vps.firewall,
        "firewall_ssh_ips": vps.firewall_ssh_ips,
    }
    vps = SimpleNamespace(
        hostname="test-vps",
        path=tmp_path,
        user="mani",
        user_pw=None,
        swap="2G",
        coinmarketcap_api_key="same-api-key",
        remote_pbgui_dir="/home/mani/software/pbgui",
        firewall=False,
        firewall_ssh_port=22,
        firewall_ssh_ips="",
        command_run_id="",
        save=lambda: None,
        _task_log_path=lambda command, fallback: tmp_path / f"{command}.log",
    )
    service._require_vps = lambda hostname: vps

    result = service.save_vps_config(
        "token",
        "test-vps",
        {
            "swap": "2G",
            "coinmarketcap_api_key": "same-api-key",
            "install_dir": "/home/mani/software",
            "firewall": True,
            "firewall_ssh_port": 22,
            "firewall_ssh_ips": "198.51.100.1",
        },
    )

    assert captured["command"] == "vps-apply-config"
    assert captured["command_text"] == "Apply VPS Config"
    assert captured["extra_vars"] == {"apply_optional_config": False, "apply_firewall": True, "apply_swap": False}
    assert result["optional_changed"] is False
    assert result["firewall_changed"] is True
    assert result["remote_apply"]["started"] is True
    assert result["remote_apply"]["run_id"] == "run-firewall"
    assert result["config"]["firewall"] is True
    assert result["config"]["firewall_ssh_ips"] == "198.51.100.1"
    assert service._load_vps_optional_config_pending(vps) == {}


def test_save_vps_config_starts_remote_swap_apply(tmp_path: Path) -> None:
    """Saving a changed swap size starts targeted apply through Save VPS."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture the targeted swap apply instead of running Ansible."""
        del debug
        captured["command"] = vps.command
        captured["command_text"] = vps.command_text
        captured["extra_vars"] = extra_vars or {}
        vps.command_run_id = "run-swap"

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._store_session_secrets = lambda token, hostname, form: None
    service._session_secret_value = lambda token, hostname, field: "fresh-password"
    service._ensure_coinmarketcap_key_clear_allowed = lambda vps, next_key: None
    service._apply_session_secrets_to_vps = lambda token, vps: setattr(vps, "user_pw", "fresh-password")
    service._build_vps_config = lambda token, vps: {"swap": vps.swap}
    vps = SimpleNamespace(
        hostname="test-vps",
        path=tmp_path,
        user="mani",
        user_pw=None,
        swap="2G",
        coinmarketcap_api_key="same-api-key",
        remote_pbgui_dir="/home/mani/software/pbgui",
        firewall=True,
        firewall_ssh_port=22,
        firewall_ssh_ips="",
        command_run_id="",
        save=lambda: None,
        _task_log_path=lambda command, fallback: tmp_path / f"{command}.log",
    )
    service._require_vps = lambda hostname: vps

    result = service.save_vps_config(
        "token",
        "test-vps",
        {
            "swap": "4G",
            "coinmarketcap_api_key": "same-api-key",
            "install_dir": "/home/mani/software",
            "firewall": True,
            "firewall_ssh_port": 22,
            "firewall_ssh_ips": "",
        },
    )

    assert captured["command"] == "vps-apply-config"
    assert captured["command_text"] == "Apply VPS Config"
    assert captured["extra_vars"] == {"apply_optional_config": False, "apply_firewall": False, "apply_swap": True}
    assert result["optional_changed"] is False
    assert result["firewall_changed"] is False
    assert result["swap_changed"] is True
    assert result["remote_apply"]["started"] is True
    assert result["remote_apply"]["run_id"] == "run-swap"
    assert result["config"]["swap"] == "4G"
    assert service._load_vps_optional_config_pending(vps) == {}


def test_read_vps_settings_uses_form_user_password() -> None:
    """Read VPS settings accepts a freshly entered form password."""
    saves: list[tuple[str, bool, str]] = []
    progress: list[tuple[str, str, str]] = []
    service = object.__new__(VPSManagerService)
    service._session_secrets = {}
    vps = SimpleNamespace(
        hostname="test-vps",
        user_pw=None,
        coinmarketcap_api_key="",
        swap="0",
        can_login_ssh=lambda: vps.user_pw == "fresh-password",
        fetch_vps_info=lambda: {"coinmarketcap": "remote-cmc", "swap": "2G"},
        fetch_ufw_settings=lambda: (True, "82.165.176.129"),
        write_vps_firewall_info=lambda: True,
        save=lambda: saves.append((vps.coinmarketcap_api_key, vps.firewall, vps.firewall_ssh_ips)),
    )
    service._require_vps = lambda hostname: vps
    service._clear_vps_optional_config_pending = lambda vps: None
    service._build_vps_config = lambda token, vps: {
        "coinmarketcap_api_key": vps.coinmarketcap_api_key,
        "swap": vps.swap,
        "firewall": vps.firewall,
        "firewall_ssh_ips": vps.firewall_ssh_ips,
    }

    result = service.read_vps_settings(
        "token",
        "test-vps",
        {"user_pw": "fresh-password"},
        lambda step, label, status: progress.append((step, label, status)),
    )

    assert vps.user_pw == "fresh-password"
    assert service._session_secret_value("token", "test-vps", "user_pw") == "fresh-password"
    assert result["coinmarketcap_api_key"] == "remote-cmc"
    assert result["swap"] == "2G"
    assert result["firewall"] is True
    assert result["firewall_ssh_ips"] == "82.165.176.129"
    assert saves == [("remote-cmc", True, "82.165.176.129")]
    assert [item[0] for item in progress] == ["start", "password", "ssh", "remote_config", "firewall", "save", "done"]
    assert progress[-1] == ("done", "VPS settings refreshed", "done")


def test_write_hosts_entry_returns_success_without_password_in_content(monkeypatch: pytest.MonkeyPatch) -> None:
    """Writing /etc/hosts returns ok and never sends the sudo password to tee."""
    captured: dict[str, object] = {}
    service = object.__new__(VPSManagerService)
    monkeypatch.setattr(
        VPSManagerService,
        "validate_local_sudo_password",
        lambda self, sudo_pw: {"ok": True},
    )

    def fake_open(path: str, mode: str = "r", *args, **kwargs):
        """Serve a minimal /etc/hosts file for the test."""
        del args, kwargs
        assert path == "/etc/hosts"
        assert mode == "r"
        return io.StringIO("127.0.0.1 localhost\n")

    class FakePopen:
        """Capture sudo tee invocation without touching the real file."""

        returncode = 0

        def __init__(self, args, **kwargs) -> None:
            """Store process construction arguments."""
            captured["args"] = args
            captured["kwargs"] = kwargs

        def communicate(self, input=None, timeout=None):
            """Record stdin payload and pretend tee succeeded."""
            captured["input"] = input
            captured["timeout"] = timeout
            return "", ""

    monkeypatch.setattr(builtins, "open", fake_open)
    monkeypatch.setattr(subprocess, "Popen", FakePopen)

    result = service.write_hosts_entry("82.165.176.129", "manibot02", "local-sudo-password")

    assert result == {"ok": True, "ip": "82.165.176.129", "hostname": "manibot02"}
    assert captured["args"] == ["sudo", "-n", "tee", "/etc/hosts"]
    assert "local-sudo-password" not in str(captured["input"])
    assert "82.165.176.129\tmanibot02" in str(captured["input"])


def test_vps_load_recovers_missing_hostname_from_file_path(tmp_path: Path) -> None:
    """Legacy/corrupt host JSON with missing hostname is recovered from file name."""
    host_dir = tmp_path / "manibot90"
    host_dir.mkdir()
    path = host_dir / "manibot90.json"
    path.write_text('{"_hostname": null, "ip": "23.94.74.212", "user": "mani"}', encoding="utf-8")

    vps = core.VPS()
    vps.load(str(path))

    assert vps.hostname == "manibot90"
    assert vps.ip == "23.94.74.212"


@pytest.mark.parametrize("hostname", ["../outside", "a/b", "a\\b", ".", "..", "bad\x00host", "bad\nhost"])
def test_vps_hostname_rejects_inventory_traversal(hostname: str) -> None:
    """Reject hostnames which cannot map to one inventory child directory."""
    vps = core.VPS()

    with pytest.raises(ValueError):
        vps.hostname = hostname


def test_vps_save_and_delete_revalidate_tampered_hostname(monkeypatch, tmp_path: Path) -> None:
    """Protect persistence even if an internal caller bypasses the hostname setter."""
    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    outside = tmp_path / "data" / "outside"
    outside.mkdir(parents=True)
    sentinel = outside / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    vps = core.VPS()
    vps._hostname = "../outside"
    vps.path = outside

    with pytest.raises(ValueError):
        vps.save()
    with pytest.raises(ValueError):
        vps.delete()

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_vps_service_hostname_validation_uses_inventory_guard(monkeypatch, tmp_path: Path) -> None:
    """Apply the same traversal guard before service-level VPS creation."""
    monkeypatch.setattr(core, "PBGDIR", tmp_path)

    with pytest.raises(ValueError):
        service_mod._validate_import_hostname("../../outside")


def test_vps_manager_skips_inventory_with_traversal_hostname(monkeypatch, tmp_path: Path) -> None:
    """Do not hydrate persisted inventory entries containing unsafe hostnames."""
    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    host_dir = tmp_path / "data" / "vpsmanager" / "hosts" / "safe-name"
    host_dir.mkdir(parents=True)
    (host_dir / "safe-name.json").write_text(
        json.dumps({"_hostname": "../../outside", "ip": "192.0.2.10"}),
        encoding="utf-8",
    )
    manager = object.__new__(core.VPSManager)
    manager.vpss = []

    manager.find_vps()

    assert manager.vpss == []


def test_vps_manager_context_detail_uses_quick_detail_for_fluid_switching() -> None:
    """WebSocket context changes should stay quick so host switching remains fluid."""
    source = Path("api/vps_manager.py").read_text(encoding="utf-8")
    start = source.index("async def _send_current_context_detail")
    next_def = source.find("\n\n", start + 1)
    end = next_def if next_def >= 0 else len(source)
    body = source[start:end]

    assert "_build_quick_detail_for_context" in body


def test_systemd_migration_complete_with_unconfigured_pbrun_pbdata_units() -> None:
    """Unconfigured PBRun/PBData units do not keep systemd migration pending."""
    service = object.__new__(VPSManagerService)
    output = """KV\tpbgui_dir_exists\tyes
KV\tpython_exists\tyes
KV\tstart_sh_exists\tno
KV\tsystemctl_exists\tyes
KV\tsystemctl_path\t/usr/bin/systemctl
KV\tpbrun_configured\tno
KV\tpbdata_configured\tno
KV\tcoindata_configured\tno
KV\tsystemd_user_manager\tyes
KV\tsystemd_user_manager_detail\tactive
SECTION\tunits\tBEGIN
pbgui-pbcluster.service\tyes\tenabled\tactive
pbgui-pbrun.service\tyes\tdisabled\tinactive
pbgui-pbdata.service\tyes\tdisabled\tinactive
pbgui-pbcoindata.service\tyes\tenabled\tactive
SECTION\tunits\tEND
SECTION\tcron\tBEGIN
SECTION\tcron\tEND
SECTION\tprocesses\tBEGIN
SECTION\tprocesses\tEND
"""

    parsed = service._parse_vps_systemd_migration_preview(output)
    status = service._build_vps_systemd_migration_status_from_preview(parsed)

    assert status["migration_complete"] is True
    assert status["migration_needed"] is False
    assert status["units_ready"] is True


def test_systemd_migration_requires_pbcluster_unit() -> None:
    """PBCluster is a required VPS systemd service, not an optional unit."""
    service = object.__new__(VPSManagerService)
    output = """KV\tpbgui_dir_exists\tyes
KV\tpython_exists\tyes
KV\tstart_sh_exists\tno
KV\tsystemctl_exists\tyes
KV\tsystemctl_path\t/usr/bin/systemctl
KV\tpbrun_configured\tyes
KV\tpbdata_configured\tno
KV\tcoindata_configured\tno
KV\tsystemd_user_manager\tyes
KV\tsystemd_user_manager_detail\tactive
SECTION\tunits\tBEGIN
pbgui-pbcluster.service\tno\tnot-found\tinactive
pbgui-pbrun.service\tyes\tenabled\tactive
pbgui-pbdata.service\tyes\tdisabled\tinactive
pbgui-pbcoindata.service\tno\tnot-found\tinactive
SECTION\tunits\tEND
SECTION\tcron\tBEGIN
SECTION\tcron\tEND
SECTION\tprocesses\tBEGIN
SECTION\tprocesses\tEND
"""

    parsed = service._parse_vps_systemd_migration_preview(output)
    status = service._build_vps_systemd_migration_status_from_preview(parsed)

    assert status["migration_complete"] is False
    assert status["migration_needed"] is True
    assert status["units_ready"] is False
    assert [item["unit"] for item in status["required_units"]] == ["pbgui-pbcluster.service", "pbgui-pbrun.service", "pbgui-pbcoindata.service"]


def test_systemd_migration_running_status_does_not_reuse_stale_cache() -> None:
    """A running migration clears stale preview cache instead of keeping the sidebar orange."""
    service = object.__new__(VPSManagerService)
    service._vps_systemd_migration_status_cache = {
        "manibot92": {
            "state": "needed",
            "available": True,
            "migration_complete": False,
            "migration_needed": True,
            "units_ready": False,
            "checked_at": 9999999999,
        }
    }
    vps = SimpleNamespace(hostname="manibot92", command="vps-migrate-systemd", update_status="running")

    status = service._get_vps_systemd_migration_status(vps, {}, quick=False)

    assert status["state"] == "running"
    assert status["available"] is True
    assert status["migration_complete"] is False
    assert "manibot92" not in service._vps_systemd_migration_status_cache


def test_systemd_migration_status_uses_monitor_host_meta() -> None:
    """Migration status is read from monitor host metadata."""
    service = object.__new__(VPSManagerService)
    service._vps_systemd_migration_status_cache = {}
    vps = SimpleNamespace(
        hostname="manibot72",
        ip="167.86.69.219",
        user="mani",
        user_pw=None,
        remote_pbgui_dir="/home/mani/software/pbgui",
        command="vps-update",
        update_status="successful",
    )
    host_state = {
        "connection": {"status": "connected"},
        "meta": {
            "systemd_migration": {
                "state": "needed",
                "available": True,
                "migration_complete": False,
                "migration_needed": True,
                "units_ready": False,
                "legacy_start_sh_exists": True,
            }
        },
    }

    status = service._get_vps_systemd_migration_status(vps, host_state, quick=False)

    assert status["state"] == "needed"
    assert status["migration_needed"] is True
    assert status["legacy_start_sh_exists"] is True


def test_systemd_migration_status_unknown_without_monitor_meta() -> None:
    """Missing monitor host metadata stays neutral/unknown."""
    service = object.__new__(VPSManagerService)
    service._vps_systemd_migration_status_cache = {}
    service._host_online = lambda host_state: True
    vps = SimpleNamespace(hostname="manibot72", command="vps-update", update_status="successful")

    status = service._get_vps_systemd_migration_status(vps, {"connection": {"status": "connected"}}, quick=False)

    assert status["state"] == "unknown"
    assert status["migration_needed"] is False
    assert status["available"] is False


def test_monitor_host_meta_collects_systemd_migration_status() -> None:
    """Host metadata collector publishes systemd migration status for the sidebar."""
    source = Path("master/async_monitor.py").read_text(encoding="utf-8")
    assert "result['systemd_migration'] = build_systemd_migration_status" in source
    assert "required_units" in source
    assert "pbgui-pbcluster.service" in source
    assert "legacy_process_count" in source


def test_systemd_migration_playbook_enables_only_configured_optional_units() -> None:
    """The migration playbook must not always start optional PBCoinData."""
    playbook = Path("vps-migrate-systemd.yml").read_text(encoding="utf-8")

    assert "read PBGui optional service config" in playbook
    assert "pbgui-pbcluster.service" in playbook
    assert "pbgui_enabled_services" in playbook
    assert "{{ pbgui_enabled_services | join(',') }}" in playbook
    assert "- pbrun,pbremote,pbcoindata" not in playbook
    assert "for unit in {{ all_systemd_units | join(' ') }}" in playbook
    assert "pbgui-pbremote.service" in playbook
    assert "PBRemote.py" in playbook


@pytest.mark.parametrize("playbook_path", ["vps-update-pbgui.yml", "vps-update-pb.yml", "vps-switch-pbgui-branch.yml"])
def test_pbgui_code_update_playbooks_sync_systemd_units(playbook_path: str) -> None:
    """PBGui code updates install new systemd units before restarting services."""
    playbook = Path(playbook_path).read_text(encoding="utf-8")
    systemd_setup_block = playbook.split("register: systemd_setup_result", 1)[1].split("listen: \"restart pbgui\"", 1)[0]

    assert "Check required PBGui systemd units" in playbook
    assert "force_handlers: true" in playbook
    assert "pbgui-pbcluster.service pbgui-pbrun.service" in playbook or "pbgui-pbcluster.service" in playbook
    assert "required_systemd_units" in playbook
    assert 'user: "{{ user }}"' not in playbook
    assert "target_user={{ user | default('', true) | quote }}" in playbook
    assert 'getent passwd "$target_user"' in playbook
    assert "Read PBGui optional service config" in playbook
    assert "pbgui_enabled_services" in playbook
    assert "{{ pbgui_enabled_services | join(',') }}" in playbook
    assert "pbgui_role" not in playbook
    assert "Restart remote master PBApiServer" not in playbook
    assert "setup/setup_systemd.sh" in playbook
    assert "--include-pbremote" not in playbook
    assert "--no-start" in playbook
    assert "failed_when: false" not in systemd_setup_block
    assert "setup/vps_service_control.sh restart PBCluster PBRun PBCoinData PBMonitorAgent" in playbook
    assert "setup/vps_service_control.sh restart PBCluster PBRun PBRemote PBCoinData" not in playbook


def test_metrics_stream_reads_monitor_agent_cache() -> None:
    """Master-side live metrics must tail the monitor-agent cache, not run collectors."""

    command = monitor_mod._monitor_agent_tail_command("software/pbgui")

    assert "data/monitor_agent/live_metrics.ndjson" in command
    assert "tail -n 1 -F" in command
    assert "python3 -u -c" not in command


@pytest.mark.parametrize("filename", [
    "instance_snapshot.json",
    "host_meta.json",
    "service_status.json",
    "package_status.json",
])
def test_slow_monitor_reads_use_agent_cache(filename: str) -> None:
    """Master-side slow monitor paths read agent JSON cache files only."""

    command = monitor_mod._monitor_agent_cache_read_command("software/pbgui", filename)

    assert f"data/monitor_agent/{filename}" in command
    assert command.startswith("cat ")
    assert "python3 -u -c" not in command
    assert "systemctl" not in command


def test_vps_api_does_not_import_instance_collector_script() -> None:
    """The VPS API must not send the old instance collector over SSH."""

    source = Path("api/vps.py").read_text(encoding="utf-8")

    assert "INSTANCE_COLLECT_SCRIPT" not in source
    assert "get_recent_logs" in source
    assert "get_bot_log" in source


def test_host_meta_migration_status_requires_monitor_agent_unit() -> None:
    """Remote systemd migration status includes the monitor-agent unit."""

    source = Path("master/async_monitor.py").read_text(encoding="utf-8")
    agent_source = Path("monitor_agent.py").read_text(encoding="utf-8")

    assert "pbgui-monitor-agent.service" in source
    assert "required_unit_names = ['pbgui-pbcluster.service', 'pbgui-monitor-agent.service']" in source
    assert '"PBMonitorAgent": ServiceInfo("PBMonitorAgent"' in source
    assert '"PBMonitorAgent": "pbgui-monitor-agent.service"' in source
    assert '"PBMonitorAgent": ("pbgui-monitor-agent.service"' in agent_source


@pytest.mark.parametrize("playbook_path", ["master-update-pbgui.yml", "master-update-pb.yml", "master-switch-pbgui-branch.yml"])
def test_master_update_playbooks_repair_required_systemd_units(playbook_path: str) -> None:
    """Master updates must repair required systemd units even without git changes."""
    playbook = Path(playbook_path).read_text(encoding="utf-8")
    systemd_setup_block = playbook.split("register: systemd_setup_result", 1)[1].split("listen: \"restart pbgui\"", 1)[0]

    assert "Check required PBGui systemd units" in playbook
    assert "{{ target_hosts | default('localhost') }}" in playbook
    assert "pbgui_python | default(ansible_playbook_python)" in playbook
    assert "force_handlers: true" in playbook
    assert "pbgui-pbcluster.service" in playbook
    assert "pbgui-pbcoindata.service" in playbook
    assert "pbgui-monitor-agent.service" in playbook
    assert "pbcoindata" in playbook
    assert "monitor-agent" in playbook
    assert "PBMonitorAgent" in playbook
    assert "is-enabled" in playbook
    assert "is-active" in playbook
    assert "repair=disabled" in playbook
    assert "repair=inactive" in playbook
    assert 'if [ "$unit" != "pbgui-api.service" ]' not in playbook
    assert "required_systemd_units" in playbook
    assert "PBGUI_REQUIRE_PBCOINDATA" in playbook
    assert "Restart PBApiServer" in playbook
    assert "systemd-run --user" in playbook
    assert "systemctl --user enable pbgui-api.service" in playbook
    assert "systemctl --user restart pbgui-api.service" in playbook
    assert 'user: "{{ user }}"' not in playbook
    assert "target_user={{ user | default('', true) | quote }}" in playbook
    assert 'getent passwd "$target_user"' in playbook
    assert "setup/setup_systemd.sh" in playbook
    assert "--no-start" in playbook
    assert "api,pbcluster,pbcoindata,monitor-agent" in playbook
    assert "failed_when: false" not in systemd_setup_block


def test_master_restart_control_keeps_required_pbcoindata_enabled() -> None:
    """Master restarts must not disable required PBCoinData without a CMC key."""
    script = Path("setup/vps_service_control.sh").read_text(encoding="utf-8")

    assert "PBGUI_REQUIRE_PBCOINDATA" in script
    assert 'case "$require_coindata" in 1|true|yes|on) return 0 ;; esac' in script
    assert 'PBCoinData)\n      local require_coindata=' in script


def test_local_master_metrics_are_recorded_in_host_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local master telemetry uses the same host metric history stores as VPS hosts."""
    service = object.__new__(VPSManagerService)
    recorded: list[tuple[str, str, object, bool, str]] = []

    class FakeStore:
        """Capture host metric history writes."""

        def __init__(self, name: str) -> None:
            """Store the metric name for assertions."""
            self.name = name
            self.flushes = 0

        def record(self, hostname: str, **kwargs) -> None:
            """Capture a history sample."""
            recorded.append((
                self.name,
                hostname,
                kwargs.get("value"),
                bool(kwargs.get("confirmed")),
                str(kwargs.get("same_minute_mode") or ""),
            ))

        def maybe_flush(self) -> None:
            """Capture flush attempts."""
            self.flushes += 1

    stores = {name: FakeStore(name) for name in ("cpu", "memory", "disk", "swap")}
    monkeypatch.setattr(service_mod, "get_monitor", lambda: SimpleNamespace(_host_metric_history=stores))

    service._record_local_master_server_metric_history(
        "magicnucpro",
        {
            "cpu_60s": 12.5,
            "cpu_60s_window": 60.0,
            "mem": {"usage_pct": 27, "total_mb": 63852},
            "disk": {"usage_pct": 58, "total_mb": 936737},
            "swap": {"usage_pct": 43, "total_mb": 8191},
        },
    )

    assert recorded == [
        ("cpu", "magicnucpro", 12.5, True, ""),
        ("memory", "magicnucpro", 27, True, "peak"),
        ("disk", "magicnucpro", 58, True, "peak"),
        ("swap", "magicnucpro", 43, True, "peak"),
    ]
    assert all(store.flushes == 1 for store in stores.values())


def test_system_cpu_alert_requires_sustained_live_cpu() -> None:
    """CPU alerts require a continuous over-threshold streak, not only a high 60s average."""
    config = SimpleNamespace(mem_error_server=128, swap_error_server=128, disk_error_server=128, cpu_error_server=95.0)
    metrics = {
        "timestamp": monitor_mod.time.time(),
        "cpu": 96.4,
        "cpu_60s": 96.4,
        "cpu_60s_window": 60.0,
        "cpu_threshold_duration": 59.0,
        "mem_total": 1024 * 1024 * 1024,
        "mem_available": 512 * 1024 * 1024,
        "swap_total": 2048 * 1024 * 1024,
        "swap_free": 1024 * 1024 * 1024,
        "disk_total": 10 * 1024 * 1024 * 1024,
        "disk_free": 2 * 1024 * 1024 * 1024,
    }

    assert collect_live_alerts({"manibot50": {"status": "connected"}}, {"manibot50": metrics}, {}, {}, config) == []

    metrics["cpu_threshold_duration"] = 60.0
    alerts = collect_live_alerts({"manibot50": {"status": "connected"}}, {"manibot50": metrics}, {}, {}, config)

    assert len(alerts) == 1
    assert alerts[0]["triggered_thresholds"] == ["cpu"]
    assert "for 60s" in alerts[0]["details"]


def test_vps_monitor_tracks_cpu_threshold_streak() -> None:
    """The monitor resets CPU alert duration as soon as live CPU falls below threshold."""
    monitor = object.__new__(VPSMonitor)
    metrics = SystemMetrics(timestamp=100.0, cpu=96.0)
    monitor.store = SimpleNamespace(system={"manibot50": metrics})
    config = SimpleNamespace(cpu_error_server=95.0)

    monitor._update_cpu_threshold_state(config, now=100.0)
    assert metrics.cpu_threshold_since == 100.0
    assert metrics.cpu_threshold_duration == 0.0

    metrics.timestamp = 160.0
    monitor._update_cpu_threshold_state(config, now=160.0)
    assert metrics.cpu_threshold_since == 100.0
    assert metrics.cpu_threshold_duration == 60.0

    metrics.timestamp = 161.0
    metrics.cpu = 40.0
    monitor._update_cpu_threshold_state(config, now=161.0)
    assert metrics.cpu_threshold_since == 0.0
    assert metrics.cpu_threshold_duration == 0.0


def test_cpu_history_store_keeps_confirmed_sample_for_same_minute(tmp_path: Path) -> None:
    """Unconfirmed same-minute CPU samples must not erase a confirmed value."""
    store = CpuHistoryStore(tmp_path, "cpu_history")

    store.record("master", minute=12345, value=12.5, confirmed=True)
    store.record("master", minute=12345, value=0.0, confirmed=False)

    payload = store.build_payload("master", hostname="master", end_minute=12345)
    assert payload["points"][-1] == 12.5


def test_vps_monitor_records_local_master_history_without_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    """The monitor loop writes local master samples without relying on VPS Manager UI polling."""
    monitor = object.__new__(VPSMonitor)
    recorded: list[tuple[str, str, object, bool, str]] = []
    updated: list[tuple[str, SystemMetrics]] = []

    class FakeStore:
        """Capture monitor history writes."""

        def __init__(self, name: str) -> None:
            """Store the metric name for assertions."""
            self.name = name
            self.flushes = 0

        def record(self, hostname: str, **kwargs) -> None:
            """Capture a history sample."""
            recorded.append((self.name, hostname, kwargs.get("value"), bool(kwargs.get("confirmed")), str(kwargs.get("same_minute_mode") or "")))

        def maybe_flush(self, **kwargs) -> None:
            """Capture flush attempts."""
            del kwargs
            self.flushes += 1

    stores = {name: FakeStore(name) for name in ("cpu", "memory", "disk", "swap")}
    metrics = SystemMetrics(
        timestamp=120.0,
        cpu_60s=14.5,
        cpu_60s_window=61.0,
        mem_total=1024,
        mem_percent=25.0,
        disk_total=2048,
        disk_percent=50.0,
        swap_total=4096,
        swap_percent=5.0,
    )
    monitor.store = SimpleNamespace(
        update_system=lambda hostname, item: updated.append((hostname, item))
    )
    monitor._host_metric_history = stores
    monkeypatch.setattr(monitor, "_local_master_hostname", lambda: "master01")
    monkeypatch.setattr(monitor, "_build_local_master_system_metrics", lambda: metrics)

    monitor._record_local_master_metric_history()

    assert updated == [("master01", metrics)]
    assert recorded == [
        ("cpu", "master01", 14.5, True, ""),
        ("memory", "master01", 25.0, True, "peak"),
        ("disk", "master01", 50.0, True, "peak"),
        ("swap", "master01", 5.0, True, "peak"),
    ]
    assert all(store.flushes == 1 for store in stores.values())


def test_vps_monitor_initializes_local_master_history_buffers() -> None:
    """VPSMonitor startup must create local master history buffers before the loop runs."""
    monitor = VPSMonitor()

    assert monitor._local_master_cpu_history == []
    assert monitor._local_master_metric_history == {
        "memory": [],
        "disk": [],
        "swap": [],
    }


def test_instance_collect_script_reports_dynamic_ignore_flag() -> None:
    """Remote v7 instance telemetry includes dynamic_ignore state."""
    assert "dynamic_ignore = bool(pbgui.get('dynamic_ignore'))" in INSTANCE_COLLECT_SCRIPT
    assert "'di': dynamic_ignore" in INSTANCE_COLLECT_SCRIPT


def test_cannot_clear_cmc_key_while_dynamic_ignore_bot_runs() -> None:
    """Clearing a VPS CMC key is blocked while a dynamic_ignore bot is running."""
    service = object.__new__(VPSManagerService)
    service._refresh_vps_instances_now = lambda hostname: None
    service._get_monitor_state = lambda: {
        "connections": {"connections": {"test-vps": {"status": "connected"}}},
        "v7_instances": {"test-vps": [{"name": "dynbot", "running": True, "di": True}]},
    }
    vps = SimpleNamespace(hostname="test-vps", coinmarketcap_api_key="old-api-key")

    with pytest.raises(ValueError, match="dynbot"):
        service._ensure_coinmarketcap_key_clear_allowed(vps, "")


def test_can_clear_cmc_key_when_dynamic_ignore_bot_is_stopped() -> None:
    """A stopped dynamic_ignore bot does not block CMC key removal."""
    service = object.__new__(VPSManagerService)
    service._refresh_vps_instances_now = lambda hostname: None
    service._get_monitor_state = lambda: {
        "connections": {"connections": {"test-vps": {"status": "connected"}}},
        "v7_instances": {"test-vps": [{"name": "dynbot", "running": False, "di": True}]},
    }
    vps = SimpleNamespace(hostname="test-vps", coinmarketcap_api_key="old-api-key")

    service._ensure_coinmarketcap_key_clear_allowed(vps, "")


def test_v7_host_dropdown_blocks_pending_cmc_removal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pending CMC key removal disables dynamic_ignore even with stale live metadata."""
    host = "test-vps"
    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    pending_dir = tmp_path / "data" / "vpsmanager" / "hosts" / host
    pending_dir.mkdir(parents=True)
    (pending_dir / "optional_config_pending.json").write_text(
        '{"coinmarketcap_api_key": ""}',
        encoding="utf-8",
    )
    monitor = SimpleNamespace(
        store=SimpleNamespace(host_meta={host: {"optional_services": {"PBCoinData": True}}})
    )
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", monitor)

    detail = v7_instances._host_dropdown_detail(host)
    message = asyncio.run(v7_instances._target_dynamic_ignore_incompatibility_detail(
        "dynbot",
        {"pbgui": {"dynamic_ignore": True, "enabled_on": host}},
    ))

    assert detail["coinmarketcap_configured"] is False
    assert detail["dynamic_ignore_allowed"] is False
    assert "has no CoinMarketCap API key" in message


def test_v7_get_hosts_keeps_host_list_and_adds_details(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The v7 hosts endpoint keeps hosts[] and adds dropdown capability details."""
    host = "test-vps"
    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monitor = SimpleNamespace(
        pool=True,
        enabled_hosts={host},
        store=SimpleNamespace(host_meta={host: {"coindata_configured": True, "optional_services": {"PBCoinData": True}}}),
    )
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", monitor)

    response = v7_instances.get_hosts(session=SimpleNamespace())

    assert response["hosts"] == ["disabled", "master", host]
    details = {item["name"]: item for item in response["host_details"]}
    assert details[host]["coinmarketcap_configured"] is True
    assert details[host]["dynamic_ignore_allowed"] is True


def test_v7_dynamic_ignore_does_not_treat_coindata_service_as_cmc_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The CoinData service being expected does not prove CMC enrichment is configured."""
    host = "test-vps"
    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monitor = SimpleNamespace(
        store=SimpleNamespace(host_meta={host: {"optional_services": {"PBCoinData": True}}})
    )
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", monitor)

    detail = v7_instances._host_dropdown_detail(host)

    assert detail["coinmarketcap_configured"] is None
    assert detail["dynamic_ignore_allowed"] is False


def test_v7_dynamic_ignore_blocks_unknown_cmc_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A target host with unknown CMC status is not accepted for dynamic_ignore."""
    host = "test-vps"
    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monitor = SimpleNamespace(store=SimpleNamespace(host_meta={host: {}}))
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", monitor)

    detail = v7_instances._host_dropdown_detail(host)
    message = asyncio.run(v7_instances._target_dynamic_ignore_incompatibility_detail(
        "dynbot",
        {"pbgui": {"dynamic_ignore": True, "enabled_on": host}},
    ))

    assert detail["coinmarketcap_configured"] is None
    assert detail["dynamic_ignore_allowed"] is False
    assert "has no confirmed CoinMarketCap API key" in message


def test_vps_deploy_shutdown_joins_controller_without_stopping_remote_job() -> None:
    """VPS Manager shutdown joins API controllers and blocks new Ansible launches."""
    service = object.__new__(VPSManagerService)
    service._deploy_shutdown = threading.Event()
    service._deploy_sessions_lock = threading.Lock()
    service._deploy_sessions = {}
    worker = threading.Thread(target=service._deploy_shutdown.wait)
    cluster_worker = threading.Thread(target=service._deploy_shutdown.wait)
    service._deploy_threads = {"deploy": worker}
    service._cluster_import_jobs_lock = threading.Lock()
    service._cluster_import_threads = {"cluster": cluster_worker}
    worker.start()
    cluster_worker.start()

    service.shutdown()

    assert not worker.is_alive()
    assert not cluster_worker.is_alive()
    assert service._deploy_threads == {}
    assert service._cluster_import_threads == {}
    with pytest.raises(ValueError, match="shutting down"):
        service._start_vps_deploy_host(
            "token",
            hostname="host",
            command="update",
            debug=False,
            extra_vars=None,
        )
