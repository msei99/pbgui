"""Regression tests for Existing VPS import process and key handling."""

import asyncio
import builtins
import io
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import api.v7_instances as v7_instances
from master.async_monitor import INSTANCE_COLLECT_SCRIPT
import vps_manager_core as core
import vps_manager_service as service_mod
from vps_manager_service import VPSManagerService, _ensure_import_public_key, _import_process_line_is_legacy, _set_import_key_check


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


class _FakeUfwSshClient:
    """Paramiko SSHClient double for fetch_ufw_settings()."""

    output = ""
    errors = ""
    command = ""
    kwargs: dict[str, object] = {}
    stdin = _FakeStdin()

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
            "bucket": "",
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
    service._invalidate_bucket_cleanup_indicator = lambda: None
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
    """Update playbooks receive explicit empty optional values after clearing them."""
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
    vps.bucket = ""
    vps.coinmarketcap_api_key = ""
    vps.command = "vps-update-pbgui"

    manager = core.VPSManager()
    manager.update_vps(vps)

    extravars = captured["extravars"]
    assert extravars["bucket"] == ""
    assert extravars["coinmarketcap_api_key"] == ""


def test_fetch_vps_info_reads_bucket_from_remote_ini(monkeypatch) -> None:
    """Remote settings refresh reads PBRemote bucket from the VPS pbgui.ini."""
    _FakeSshClient.config_content = """
[pbremote]
bucket = remote-bucket:

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

    assert info["bucket"] == "remote-bucket:"
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


def test_vps_status_prefers_remote_optional_meta_over_stale_local_config() -> None:
    """A second master displays the VPS-reported optional service state."""
    service = object.__new__(VPSManagerService)
    service._vps_package_status_cache = {}
    service._vps_ssh_ok_cache = {}
    service._build_vps_overview_row = lambda pbremote, hostname, host_state: {"updates": "N/A"}
    service._get_live_vps_package_status = lambda vps, host_state: None
    service._build_remote_pbgui_github_status = lambda pbremote, host_state: ""
    service._build_remote_pb7_github_status = lambda pbremote, host_state: ""
    service._host_online = lambda host_state: True
    service._host_telemetry_fresh = lambda host_state: True
    service._host_telemetry_age = lambda host_state: 1.0
    service._host_meta = lambda host_state: {"pbremote_configured": False, "coindata_configured": False}
    service._build_remote_server_metrics = lambda hostname, host_state: None
    service._get_vps_systemd_migration_status = lambda vps, host_state, quick=False: {}
    vps = SimpleNamespace(
        hostname="test-vps",
        bucket="old-bucket:",
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

    status = service._build_vps_status(vps, {}, SimpleNamespace(), True)

    assert status["pbremote_configured"] is False
    assert status["coindata_configured"] is False


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
        bucket="old-bucket:",
        coinmarketcap_api_key="old-api-key",
    )
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._get_monitor_state = lambda: {"host_meta": {"test-vps": {"pbremote_configured": False, "coindata_configured": False}}}
    service._load_vps_optional_config_pending = lambda vps: {}

    service.run_vps_command(
        token="token",
        hostname="test-vps",
        command="vps-update-pbgui",
        command_text="Update PBGui",
    )

    assert captured["extra_vars"]["bucket"] == ""
    assert captured["extra_vars"]["coinmarketcap_api_key"] == ""


def test_run_vps_command_keeps_pending_optional_values() -> None:
    """Pending local optional values override stale missing remote metadata during updates."""
    captured: dict[str, object] = {}

    def fake_update_vps(vps, debug=False, extra_vars=None) -> None:
        """Capture update arguments instead of running Ansible."""
        del debug
        captured["bucket"] = vps.bucket
        captured["coinmarketcap_api_key"] = vps.coinmarketcap_api_key
        captured["extra_vars"] = extra_vars

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    vps = SimpleNamespace(
        hostname="test-vps",
        user_pw=None,
        bucket="new-bucket:",
        coinmarketcap_api_key="new-api-key",
        save=lambda: None,
    )
    service._require_vps = lambda hostname: vps
    service._apply_session_secrets_to_vps = lambda token, vps: None
    service._get_monitor_state = lambda: {"host_meta": {"test-vps": {"pbremote_configured": False, "coindata_configured": False}}}
    service._host_telemetry_fresh = lambda state: True
    service._load_vps_optional_config_pending = lambda vps: {"bucket": "new-bucket:", "coinmarketcap_api_key": "new-api-key"}
    service._write_vps_optional_config_pending = lambda vps, values: None

    service.run_vps_command(
        token="token",
        hostname="test-vps",
        command="vps-update-pbgui",
        command_text="Update PBGui",
    )

    assert captured["bucket"] == "new-bucket:"
    assert captured["coinmarketcap_api_key"] == "new-api-key"
    assert captured["extra_vars"] is None


def test_sync_vps_config_from_host_meta_persists_remote_optional_values() -> None:
    """Fresh VPS metadata updates the local master's saved optional settings."""
    saves: list[tuple[str, str]] = []
    service = object.__new__(VPSManagerService)
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(
        bucket="old-bucket:",
        coinmarketcap_api_key="old-api-key",
        save=lambda: saves.append((vps.bucket, vps.coinmarketcap_api_key)),
    )
    host_state = {
        "connection": {"status": "connected"},
        "stream": {"last_update": 1_700_000_000},
        "meta": {
            "pbremote_bucket": "remote-bucket:",
            "coinmarketcap_api_key": "remote-api-key",
        },
    }
    service._host_telemetry_fresh = lambda state: True

    service._sync_vps_config_from_host_meta(vps, host_state)

    assert vps.bucket == "remote-bucket:"
    assert vps.coinmarketcap_api_key == "remote-api-key"
    assert saves == [("remote-bucket:", "remote-api-key")]


def test_sync_vps_config_from_host_meta_keeps_pending_local_optional_values(tmp_path: Path) -> None:
    """Stale live metadata cannot overwrite locally saved pending optional settings."""
    saves: list[tuple[str, str]] = []
    service = object.__new__(VPSManagerService)
    service._host_telemetry_fresh = lambda state: True
    vps = SimpleNamespace(
        hostname="test-vps",
        path=tmp_path,
        bucket="",
        coinmarketcap_api_key="",
        save=lambda: saves.append((vps.bucket, vps.coinmarketcap_api_key)),
    )
    service._write_vps_optional_config_pending(vps, {"bucket": "", "coinmarketcap_api_key": ""})
    stale_host_state = {
        "meta": {
            "pbremote_bucket": "old-bucket:",
            "coinmarketcap_api_key": "old-api-key",
        },
    }

    service._sync_vps_config_from_host_meta(vps, stale_host_state)

    assert vps.bucket == ""
    assert vps.coinmarketcap_api_key == ""
    assert service._load_vps_optional_config_pending(vps) == {"bucket": "", "coinmarketcap_api_key": ""}
    assert saves == []

    confirmed_host_state = {
        "meta": {
            "pbremote_bucket": "",
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
        bucket="",
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
        bucket="",
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
        captured["bucket"] = vps.bucket
        captured["coinmarketcap_api_key"] = vps.coinmarketcap_api_key
        captured["extra_vars"] = extra_vars

    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_vps=fake_update_vps)
    service._load_vps_optional_config_pending = lambda vps: {}
    service._write_vps_optional_config_pending = lambda vps, values: None
    vps = SimpleNamespace(
        hostname="test-vps",
        user_pw=None,
        bucket="old-bucket:",
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
                "pbremote_configured": True,
                "coindata_configured": True,
                "pbremote_bucket": "remote-bucket:",
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

    assert captured["bucket"] == "remote-bucket:"
    assert captured["coinmarketcap_api_key"] == "remote-api-key"
    assert captured["extra_vars"] is None


def test_save_vps_config_starts_remote_optional_apply(tmp_path: Path) -> None:
    """Saving changed optional settings starts the targeted remote apply playbook."""
    captured: dict[str, object] = {}
    saves: list[tuple[str, str]] = []

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
    service._build_vps_config = lambda token, vps: {"bucket": vps.bucket, "coinmarketcap_api_key": vps.coinmarketcap_api_key}
    vps = SimpleNamespace(
        hostname="test-vps",
        path=tmp_path,
        user="mani",
        user_pw=None,
        swap="2G",
        bucket="old-bucket:",
        coinmarketcap_api_key="old-api-key",
        remote_pbgui_dir="/home/mani/software/pbgui",
        firewall=True,
        firewall_ssh_port=22,
        firewall_ssh_ips="",
        command_run_id="",
        save=lambda: saves.append((vps.bucket, vps.coinmarketcap_api_key)),
        _task_log_path=lambda command, fallback: tmp_path / f"{command}.log",
    )
    service._require_vps = lambda hostname: vps

    result = service.save_vps_config(
        "token",
        "test-vps",
        {
            "swap": "2G",
            "bucket": "",
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
        "bucket": "",
        "coinmarketcap_api_key": "",
    }
    assert result["optional_changed"] is True
    assert result["remote_apply"]["started"] is True
    assert result["remote_apply"]["run_id"] == "run-123"
    assert result["config"] == {"bucket": "", "coinmarketcap_api_key": ""}
    assert service._load_vps_optional_config_pending(vps) == {"bucket": "", "coinmarketcap_api_key": ""}
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
        "bucket": vps.bucket,
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
        bucket="same-bucket:",
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
            "bucket": "same-bucket:",
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
        bucket="same-bucket:",
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
            "bucket": "same-bucket:",
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
    saves: list[tuple[str, str, bool, str]] = []
    progress: list[tuple[str, str, str]] = []
    service = object.__new__(VPSManagerService)
    service._session_secrets = {}
    vps = SimpleNamespace(
        hostname="test-vps",
        user_pw=None,
        bucket="",
        coinmarketcap_api_key="",
        swap="0",
        can_login_ssh=lambda: vps.user_pw == "fresh-password",
        fetch_vps_info=lambda: {"bucket": "remote-bucket:", "coinmarketcap": "remote-cmc", "swap": "2G"},
        fetch_ufw_settings=lambda: (True, "82.165.176.129"),
        write_vps_firewall_info=lambda: True,
        save=lambda: saves.append((vps.bucket, vps.coinmarketcap_api_key, vps.firewall, vps.firewall_ssh_ips)),
    )
    service._require_vps = lambda hostname: vps
    service._clear_vps_optional_config_pending = lambda vps: None
    service._build_vps_config = lambda token, vps: {
        "bucket": vps.bucket,
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
    assert result["bucket"] == "remote-bucket:"
    assert result["coinmarketcap_api_key"] == "remote-cmc"
    assert result["swap"] == "2G"
    assert result["firewall"] is True
    assert result["firewall_ssh_ips"] == "82.165.176.129"
    assert saves == [("remote-bucket:", "remote-cmc", True, "82.165.176.129")]
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


def test_vps_manager_context_detail_uses_quick_detail_for_fluid_switching() -> None:
    """WebSocket context changes should stay quick so host switching remains fluid."""
    source = Path("api/vps_manager.py").read_text(encoding="utf-8")
    start = source.index("async def _send_current_context_detail")
    next_def = source.find("\n\n", start + 1)
    end = next_def if next_def >= 0 else len(source)
    body = source[start:end]

    assert "_build_quick_detail_for_context" in body


def test_systemd_migration_complete_with_unconfigured_optional_units() -> None:
    """Unconfigured PBRemote/CoinData units do not keep systemd migration pending."""
    service = object.__new__(VPSManagerService)
    output = """KV\tpbgui_dir_exists\tyes
KV\tpython_exists\tyes
KV\tstart_sh_exists\tno
KV\tsystemctl_exists\tyes
KV\tsystemctl_path\t/usr/bin/systemctl
KV\tpbremote_configured\tno
KV\tcoindata_configured\tno
KV\tsystemd_user_manager\tyes
KV\tsystemd_user_manager_detail\tactive
SECTION\tunits\tBEGIN
pbgui-pbrun.service\tyes\tenabled\tactive
pbgui-pbremote.service\tno\tnot-found\tinactive
pbgui-pbcoindata.service\tyes\tdisabled\tinactive
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
    assert "legacy_process_count" in source


def test_systemd_migration_playbook_enables_only_configured_optional_units() -> None:
    """The migration playbook must not always start PBRemote/PBCoinData."""
    playbook = Path("vps-migrate-systemd.yml").read_text(encoding="utf-8")

    assert "read PBGui optional service config" in playbook
    assert "pbgui_enabled_services" in playbook
    assert "{{ pbgui_enabled_services | join(',') }}" in playbook
    assert "- pbrun,pbremote,pbcoindata" not in playbook
    assert "for unit in {{ all_systemd_units | join(' ') }}" in playbook


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
            recorded.append((self.name, hostname, kwargs.get("value"), bool(kwargs.get("confirmed")), str(kwargs.get("same_minute_mode") or "")))

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
        store=SimpleNamespace(host_meta={host: {"optional_services": {"PBCoinData": True}}}),
    )
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", monitor)

    response = v7_instances.get_hosts(session=SimpleNamespace())

    assert response["hosts"] == ["disabled", "master", host]
    details = {item["name"]: item for item in response["host_details"]}
    assert details[host]["coinmarketcap_configured"] is True
    assert details[host]["dynamic_ignore_allowed"] is True


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
