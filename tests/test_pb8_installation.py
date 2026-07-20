"""Regression tests for PB8 master installation and update boundaries."""

from __future__ import annotations

import configparser
from pathlib import Path
from types import SimpleNamespace

import pytest

from setup.installer import core
import vps_manager_service as service_mod
from vps_manager_service import VPSManagerService


def test_local_installer_configures_and_uninstalls_separate_pb8_paths(tmp_path: Path) -> None:
    """Fresh local installs persist PB8 paths and local uninstall owns both targets."""
    install_dir = tmp_path / "software"
    pbgui_dir = install_dir / "pbgui"
    pbgui_dir.mkdir(parents=True)
    config = core.LocalMasterConfig(install_dir=str(install_dir), master_name="master-a")

    core._write_pbgui_config(config, install_dir, pbgui_dir)

    parser = configparser.ConfigParser()
    parser.read(pbgui_dir / "pbgui.ini")
    assert parser.get("main", "pb7dir") == str(install_dir / "pb7")
    assert parser.get("main", "pb7venv") == str(install_dir / "venv_pb7" / "bin" / "python")
    assert parser.get("main", "pb8dir") == str(install_dir / "pb8")
    assert parser.get("main", "pb8venv") == str(install_dir / "venv_pb8" / "bin" / "python")
    assert core._local_install_targets(install_dir)["PB8"] == install_dir / "pb8"
    assert core._local_install_targets(install_dir)["PB8 venv"] == install_dir / "venv_pb8"


def test_websetup_installs_pb7_pin_and_latest_pb8() -> None:
    """Browser and remote master setup keep PB7 pinned while adding PB8 master."""
    local_source = Path("setup/installer/core.py").read_text(encoding="utf-8")
    remote_source = Path("setup/installer/scripts/remote_master_bootstrap.sh").read_text(encoding="utf-8")
    web_source = Path("setup/installer/web.py").read_text(encoding="utf-8")

    assert "revision=PB7_PINNED_COMMIT" in local_source
    assert 'branch="master"' in local_source
    assert 'f"{pb8_dir}[full]"' in local_source
    assert "_validate_pb8_install(pb8_dir, pb8_venv, log)" in local_source
    assert "git clone --no-checkout" in remote_source
    assert "--ref refs/remotes/pbgui-pb7-pin/master --expected-major 8 --fetch-url" in remote_source
    assert "git reset --hard origin/master" not in remote_source
    assert "venv_pb8/bin/python' -m pip install --upgrade -e" in remote_source
    assert "--expected-major 8" in remote_source
    assert "PBGui/PB7/PB8" in web_source
    assert "pb8-runtime-invalid" in local_source
    assert "wait_for_master_update_barrier(pbgui_dir)" in local_source


@pytest.mark.parametrize("playbook_path", ["master-update-pb8.yml", "vps-update-pb8.yml"])
def test_pb8_playbooks_gate_role_validate_and_leave_processes_running(playbook_path: str) -> None:
    """PB8 updates validate master role before writes and never signal bot processes."""
    source = Path(playbook_path).read_text(encoding="utf-8")

    role_index = source.index("Read ")
    assert_index = source.index("Require ")
    verify_index = source.index("Fetch and verify latest official Passivbot v8")
    checkout_index = source.index("Checkout verified official Passivbot v8 commit")
    assert role_index < assert_index < verify_index < checkout_index
    assert "https://github.com/enarjord/passivbot.git" in source
    assert "--expected-major" in source
    assert '"8"' in source
    assert "force: yes" not in source
    assert "pip install --upgrade -e" in source
    assert "Validate PB8 CLI" in source
    assert "import passivbot_rust" in source
    assert "Save PB8 runtime paths after validation" in source
    assert source.index("Mark PB8 runtime unavailable") < checkout_index
    assert source.index("Mark validated PB8 runtime available") > source.index("Validate PB8 Rust module")
    assert "pb8-runtime-invalid" in source
    assert "Acquire PB8 update writer ownership" in source
    assert "Release PB8 update writer ownership" in source
    assert "force_handlers: true" in source
    if playbook_path == "vps-update-pb8.yml":
        assert "master_update_lock.py" in source
        assert "--barrier" in source
    assert "stop-processes" not in source
    assert "kill all" not in source
    assert "starter.py" not in source


def test_pb8_runtime_info_requires_source_schema_interpreter_and_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A partial PB8 checkout remains installable instead of being reported ready."""
    repo = tmp_path / "pb8"
    schema = repo / "src" / "config" / "schema.py"
    schema.parent.mkdir(parents=True)
    schema.write_text('CONFIG_SCHEMA_VERSION = "v8.0.0"\n', encoding="utf-8")
    python_path = tmp_path / "venv_pb8" / "bin" / "python"
    python_path.parent.mkdir(parents=True)
    python_path.symlink_to(Path(service_mod.sys.executable))
    monkeypatch.setattr(service_mod, "get_current_pb7_status", lambda _repo: ("master", "a" * 40))
    monkeypatch.setattr(service_mod, "read_local_pb7_version", lambda _repo: "v8.0.0")

    partial = service_mod._pb8_runtime_info(str(repo), str(python_path))
    (python_path.parent / "passivbot").write_text("#!/bin/sh\n", encoding="utf-8")
    ready = service_mod._pb8_runtime_info(str(repo), str(python_path))

    assert partial["installed"] is False
    assert ready["installed"] is True
    assert ready["config_version"] == "v8.0.0"


def test_local_pb8_command_requires_master_and_rejects_custom_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct local PB8 requests cannot bypass role or inject playbook variables."""
    calls: list[dict] = []
    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_master=lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(service_mod, "load_ini", lambda _section, _parameter: "slave")

    with pytest.raises(ValueError, match="master"):
        service.run_master_command(command="master-update-pb8", command_text="Install PB8")

    monkeypatch.setattr(service_mod, "load_ini", lambda _section, _parameter: "master")
    with pytest.raises(ValueError, match="custom playbook variables"):
        service.run_master_command(
            command="master-update-pb8",
            command_text="Install PB8",
            extra_vars={"pb8dir": "/tmp/other"},
        )
    assert calls == []


@pytest.mark.parametrize(("fresh", "role"), [(False, "master"), (True, "vps"), (True, "slave")])
def test_remote_pb8_command_requires_fresh_master_telemetry(fresh: bool, role: str) -> None:
    """Selected remote hosts fail closed unless fresh telemetry confirms master role."""
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="remote-a", user_pw=None)
    service.vpsmanager = SimpleNamespace(update_vps=lambda *args, **kwargs: None)
    service._require_vps = lambda _hostname: vps
    service._apply_session_secrets_to_vps = lambda _token, _vps: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda _state, _hostname: {"meta": {"role": role}}
    service._host_telemetry_fresh = lambda _state: fresh

    with pytest.raises(ValueError, match="telemetry|master"):
        service.run_vps_command(
            token="token",
            hostname="remote-a",
            command="vps-update-pb8",
            command_text="Install PB8",
        )


def test_remote_pb8_command_starts_only_for_fresh_master() -> None:
    """Fresh master telemetry permits the dedicated non-bulk PB8 playbook."""
    captured: dict[str, object] = {}
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="remote-master", user_pw=None, command_run_id="run-8")
    vps._task_log_path = lambda command, _fallback: Path(f"{command}--run-8.log")
    service.vpsmanager = SimpleNamespace(
        update_vps=lambda target, debug=False, extra_vars=None, command=None, command_text=None: captured.update(
            command=command,
            command_text=command_text,
            debug=debug,
            extra_vars=extra_vars,
        )
    )
    service._require_vps = lambda _hostname: vps
    service._apply_session_secrets_to_vps = lambda _token, _vps: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda _state, _hostname: {
        "meta": {"role": "master", "pb8ready": False}
    }
    service._host_telemetry_fresh = lambda _state: True
    service._credential_playbook_vars = lambda _hostname, _state: {}
    service._raise_if_vps_task_active = lambda _vps, _label: None

    result = service.run_vps_command(
        token="token",
        hostname="remote-master",
        command="vps-update-pb8",
        command_text="Install PB8",
    )

    assert captured["command"] == "vps-update-pb8"
    assert captured["command_text"] == "Install PB8"
    assert result["command"] == "vps-update-pb8"


def test_pb8_is_master_only_and_absent_from_bulk_actions() -> None:
    """The PB8 action is visible only in master detail sidebars, never bulk deploys."""
    source = Path("frontend/vps_manager.html").read_text(encoding="utf-8")

    assert 'runMasterWithLog("master-update-pb8"' in source
    assert "data-command='vps-update-pb8'" in source
    assert "isRemoteMaster && st.pb8_action_allowed" in source
    assert "COMMAND_VPS_UPDATE_PB8" not in service_mod.VPS_DEPLOY_ACTIONS
