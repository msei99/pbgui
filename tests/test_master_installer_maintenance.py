"""Regression tests for safe existing-master maintenance in the installer."""

from __future__ import annotations

from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from master_update_lock import (
    MasterUpdateBusyError,
    acquire_master_update_lock,
    acquire_pb8_update_writer,
    release_pb8_update_writer,
)
from setup.installer import core
import vps_manager_core as vps_core


def _init_checkout(path: Path, *, origin: str = "") -> None:
    """Create a clean local Git checkout without network access."""
    path.mkdir(parents=True)
    subprocess.run(["git", "init", "-b", "main", str(path)], check=True, capture_output=True)
    (path / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "tracked.txt"], check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(path),
            "-c",
            "user.name=PBGui Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-m",
            "test",
        ],
        check=True,
        capture_output=True,
    )
    if origin:
        subprocess.run(["git", "-C", str(path), "remote", "add", "origin", origin], check=True, capture_output=True)


def _existing_master(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build an isolated coherent local-master layout for inspection tests."""
    install_dir = tmp_path / "software"
    pbgui_dir = install_dir / "pbgui"
    pb7_dir = install_dir / "pb7"
    _init_checkout(pbgui_dir, origin="https://github.com/msei99/pbgui.git")
    _init_checkout(pb7_dir)
    (pb7_dir / "monitor").mkdir()
    (pb7_dir / "monitor" / "runtime.json").write_text("{}\n", encoding="utf-8")

    pbgui_python = install_dir / "venv_pbgui" / "bin" / "python"
    pbgui_python.parent.mkdir(parents=True)
    pbgui_python.symlink_to(Path(sys.executable))
    ansible = pbgui_python.parent / "ansible-playbook"
    ansible.write_text("#!/bin/sh\n", encoding="utf-8")
    ansible.chmod(0o700)
    pb7_python = install_dir / "venv_pb7" / "bin" / "python"
    pb7_python.parent.mkdir(parents=True)
    pb7_python.symlink_to(Path(sys.executable))

    (pbgui_dir / "pbgui.ini").write_text(
        "[main]\n"
        "role = master\n"
        f"pb7dir = {pb7_dir}\n"
        f"pb7venv = {pb7_python}\n",
        encoding="utf-8",
    )
    (pbgui_dir / ".git" / "info" / "exclude").write_text("pbgui.ini\n", encoding="utf-8")
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "pbgui-api.service").write_text(
        "[Service]\n"
        f"WorkingDirectory={pbgui_dir}\n"
        f"ExecStart={pbgui_python} {pbgui_dir / 'PBApiServer.py'}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(core, "_local_systemd_unit_dir", lambda: unit_dir)
    return install_dir


def test_existing_master_detection_enables_safe_maintenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A coherent clean master enables PB8 maintenance and Update All."""
    install_dir = _existing_master(tmp_path, monkeypatch)

    status = core.inspect_local_master_install(str(install_dir))

    assert status["installed"] is True
    assert status["can_maintain_pb8"] is True
    assert status["can_update_all"] is True
    assert status["pb8_installed"] is False
    assert status["maintenance_errors"] == []
    assert status["update_all_errors"] == []


def test_dirty_pbgui_blocks_update_all_but_not_pb8_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PBGui changes are preserved while independent PB8 installation remains available."""
    install_dir = _existing_master(tmp_path, monkeypatch)
    (install_dir / "pbgui" / "tracked.txt").write_text("changed\n", encoding="utf-8")

    status = core.inspect_local_master_install(str(install_dir))

    assert status["can_maintain_pb8"] is True
    assert status["can_update_all"] is False
    assert any("PBGui has local changes" in error for error in status["update_all_errors"])


@pytest.mark.parametrize(
    ("action", "expected_playbooks"),
    [
        ("local-pb8", ["master-update-pb8.yml"]),
        ("local-update-all", ["master-update-pb.yml", "master-update-pb8.yml"]),
    ],
)
def test_maintenance_dispatches_only_expected_playbooks(
    action: str,
    expected_playbooks: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB8-only and Update All preserve their explicit playbook boundaries."""
    install_dir = tmp_path / "software"
    pbgui_dir = install_dir / "pbgui"
    pbgui_dir.mkdir(parents=True)
    (pbgui_dir / "master-update-pb8.yml").write_text("---\n", encoding="utf-8")
    (pbgui_dir / "pb7_guard.py").write_text("", encoding="utf-8")
    status = {
        "installed": True,
        "maintenance_errors": [],
        "update_all_errors": [],
        "pbgui_dir": str(pbgui_dir),
        "pbgui_python": "/venv/bin/python",
        "ansible_playbook": "/venv/bin/ansible-playbook",
        "pb7_dir": str(install_dir / "pb7"),
        "pb7_venv": str(install_dir / "venv_pb7"),
        "pb8_dir": str(install_dir / "pb8"),
    }
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_run(args, _log, **_kwargs) -> None:
        """Capture playbook name and private extra-vars document."""
        vars_path = Path(str(args[args.index("--extra-vars") + 1])[1:])
        calls.append((Path(args[-1]).name, json.loads(vars_path.read_text(encoding="utf-8"))))

    monkeypatch.setattr(core, "inspect_local_master_install", lambda _path: dict(status))
    monkeypatch.setattr(core, "acquire_master_update_lock", lambda _path: nullcontext())
    monkeypatch.setattr(core, "_run_streaming_command", fake_run)

    result = core.run_local_master_maintenance(
        core.LocalMasterMaintenanceConfig(install_dir=str(install_dir), action=action),
        lambda _message: None,
        tmp_path / "artifacts",
    )

    assert [name for name, _variables in calls] == expected_playbooks
    assert all(variables["pbgdir"] == str(pbgui_dir) for _name, variables in calls)
    assert result["mode"] == action


def test_master_update_lock_rejects_a_concurrent_owner(tmp_path: Path) -> None:
    """Installer and VPS Manager cannot update one local master concurrently."""
    pbgui_dir = tmp_path / "pbgui"
    first = acquire_master_update_lock(pbgui_dir)
    try:
        with pytest.raises(MasterUpdateBusyError, match="already running"):
            acquire_master_update_lock(pbgui_dir)
    finally:
        first.release()

    second = acquire_master_update_lock(pbgui_dir)
    second.release()
    assert os.stat(second.path).st_mode & 0o777 == 0o600


def test_pb8_writer_directory_rejects_live_owner_and_recovers_old_empty_remnant(tmp_path: Path) -> None:
    """Writer exclusion remains fail-closed but a hard-crash remnant is not permanent."""
    owner = acquire_pb8_update_writer(tmp_path, stale_after=60)
    with pytest.raises(RuntimeError, match="already active"):
        acquire_pb8_update_writer(tmp_path, stale_after=60)
    old = 1_600_000_000
    os.utime(owner, (old, old))

    recovered = acquire_pb8_update_writer(tmp_path, stale_after=60)

    assert recovered == owner
    release_pb8_update_writer(tmp_path)


def test_vps_manager_holds_shared_lock_until_runner_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The existing VPS Manager update path shares the installer lease lifetime."""
    callbacks: dict[str, object] = {}

    class FakeLease:
        """Record release without opening a production lock file."""

        released = False

        def release(self) -> None:
            self.released = True

    lease = FakeLease()
    monkeypatch.setattr(vps_core, "PBGDIR", tmp_path)
    monkeypatch.setattr(vps_core, "acquire_master_update_lock", lambda _path: lease)
    monkeypatch.setattr(vps_core, "_ansible_envvars", lambda: {})
    monkeypatch.setattr(vps_core.ansible_runner, "run_async", lambda **kwargs: callbacks.update(kwargs))

    manager = object.__new__(vps_core.VPSManager)
    manager.command = "master-update-pb8"
    manager.update_status = None
    manager.privat_data_dir = None
    manager.update_log = ""
    manager._master_update_lease = None
    manager.remove_update_log = lambda: None
    manager.update_finished = lambda _runner=None: callbacks.update(finished=True)

    manager.update_master()

    assert lease.released is False
    callbacks["finished_callback"]()
    assert callbacks["finished"] is True
    assert lease.released is True
    assert manager._master_update_lease is None


def test_rejected_parallel_master_command_does_not_replace_running_command(tmp_path: Path, monkeypatch) -> None:
    """A failed lock acquisition must leave the accepted master's command metadata untouched."""
    monkeypatch.setattr(vps_core, "PBGDIR", tmp_path)
    manager = object.__new__(vps_core.VPSManager)
    manager.command = "master-update-pb8"
    manager.command_text = "Update PB8"
    lease = acquire_master_update_lock(tmp_path)
    try:
        with pytest.raises(MasterUpdateBusyError):
            manager.update_master(command="master-update", command_text="Update PBGui")
    finally:
        lease.release()

    assert manager.command == "master-update-pb8"
    assert manager.command_text == "Update PB8"


def test_master_update_releases_lease_when_setup_fails(tmp_path: Path, monkeypatch) -> None:
    """Failures before Ansible startup must not leave the in-process update lease registered."""
    class FakeLease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = FakeLease()
    monkeypatch.setattr(vps_core, "PBGDIR", tmp_path)
    monkeypatch.setattr(vps_core, "acquire_master_update_lock", lambda _path: lease)
    manager = object.__new__(vps_core.VPSManager)
    manager.command = "master-update-pb8"
    manager.command_text = "Update PB8"
    manager._master_update_lease = None
    manager.remove_update_log = lambda: (_ for _ in ()).throw(RuntimeError("setup failed"))

    with pytest.raises(RuntimeError, match="setup failed"):
        manager.update_master()

    assert lease.released is True
    assert manager._master_update_lease is None


def test_web_installer_exposes_existing_master_maintenance_actions() -> None:
    """Browser wizard includes separate PB8-only and Update All choices."""
    source = Path("setup/installer/web.py").read_text(encoding="utf-8")

    assert '<option value="local-pb8"' in source
    assert '<option value="local-update-all">Update PBGui, PB7 and PB8</option>' in source
    assert "Fresh/Reinstall is disruptive" in source
    assert "/api/local-master-status" in source
