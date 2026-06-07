"""Regression tests for Existing VPS import process and key handling."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from master.async_monitor import INSTANCE_COLLECT_SCRIPT
import vps_manager_core as core
from vps_manager_service import VPSManagerService, _ensure_import_public_key, _import_process_line_is_legacy, _set_import_key_check


class _FakeStdout:
    """Small stdout double returning a fixed byte payload."""

    def __init__(self, payload: bytes) -> None:
        """Store the payload returned by read()."""
        self._payload = payload

    def read(self) -> bytes:
        """Return the configured payload."""
        return self._payload


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

    service.run_vps_command(
        token="token",
        hostname="test-vps",
        command="vps-update-pbgui",
        command_text="Update PBGui",
    )

    assert captured["extra_vars"]["bucket"] == ""
    assert captured["extra_vars"]["coinmarketcap_api_key"] == ""


def test_sync_vps_config_from_host_meta_persists_remote_optional_values() -> None:
    """Fresh VPS metadata updates the local master's saved optional settings."""
    saves: list[tuple[str, str]] = []
    service = object.__new__(VPSManagerService)
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
