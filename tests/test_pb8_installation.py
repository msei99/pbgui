"""Regression tests for PB8 master and VPS-runner installation boundaries."""

from __future__ import annotations

import configparser
from pathlib import Path
from types import SimpleNamespace

import pytest

from setup.installer import core
import pb8_live_stop
import vps_manager_core as core_mod
import vps_manager_service as service_mod
from vps_manager_service import VPSManagerService


@pytest.mark.parametrize(
    ("logical_command", "profile", "expected"),
    [
        ("vps-update-runtime", "pb7", "vps-update-pb7"),
        ("vps-update-runtime", "pb8", "vps-update-pb8"),
        ("vps-update-runtime", "pb7_pb8", "vps-update-pb7-pb8"),
        ("vps-update-pbgui-runtime", "pb7", "vps-update-pb"),
        ("vps-update-pbgui-runtime", "pb8", "vps-update-pbgui-pb8"),
        ("vps-update-pbgui-runtime", "pb7_pb8", "vps-update-pbgui-pb7-pb8"),
    ],
)
def test_bulk_runtime_update_resolves_each_host_profile(logical_command: str, profile: str, expected: str) -> None:
    """Mixed bulk selections dispatch the correct playbook per persisted host profile."""
    assert service_mod._profile_aware_vps_update_command(logical_command, profile) == expected


@pytest.mark.parametrize(
    ("pb7_installed", "pb8_installed", "expected"),
    [
        (True, True, "vps-update-pb7-pb8"),
        (True, False, "vps-update-pb7"),
        (False, True, "vps-update-pb8"),
    ],
)
def test_bulk_runtime_update_prefers_confirmed_installs_over_stale_profile(
    pb7_installed: bool,
    pb8_installed: bool,
    expected: str,
) -> None:
    """Legacy PB7 profiles must not suppress a confirmed installed PB8 runtime."""
    assert service_mod._profile_aware_vps_update_command(
        "vps-update-runtime",
        "pb7",
        pb7_installed=pb7_installed,
        pb8_installed=pb8_installed,
    ) == expected


def test_combined_runtime_playbooks_compose_existing_safe_updates() -> None:
    """Dual-runtime updates reuse the reviewed PB7 and PB8 playbooks in order."""
    runtime_only = Path("vps-update-pb7-pb8.yml").read_text(encoding="utf-8")
    with_pbgui = Path("vps-update-pbgui-pb7-pb8.yml").read_text(encoding="utf-8")

    assert runtime_only.index("vps-update-pb7.yml") < runtime_only.index("vps-update-pb8.yml")
    assert with_pbgui.index("vps-update-pb.yml") < with_pbgui.index("vps-update-pb8.yml")
    assert core_mod._command_updates_pbgui("vps-update-pbgui-pb7-pb8") is True


def test_local_master_restart_waits_for_imported_pb8_update() -> None:
    """The local API restart must not kill a later PB8 import in the same playbook."""
    source = Path("master-update-pbgui.yml").read_text(encoding="utf-8")
    handler = source.split("Restart PBApiServer", 1)[1]

    assert 'ansible-playboo*) update_pid="$ancestor"' in handler
    assert "--expand-environment=no" in handler
    assert '--setenv=PBGUI_UPDATE_PID="$update_pid"' in handler
    assert '--setenv=PBGUI_UPDATE_START="$update_start"' in handler
    assert 'while [ -r "/proc/$PBGUI_UPDATE_PID/stat" ]' in handler
    assert '[ "${current_stat[21]}" = "$PBGUI_UPDATE_START" ]' in handler
    assert "sleep 1\n          cd \"$PBGUI_DIR\"" not in handler


@pytest.mark.parametrize(
    "playbook_path",
    [
        "master-update-pbgui.yml",
        "master-update-pb.yml",
        "master-switch-pbgui-branch.yml",
        "vps-update-pbgui.yml",
        "vps-update-pb.yml",
        "vps-switch-pbgui-branch.yml",
    ],
)
def test_delayed_api_restart_disables_systemd_environment_expansion(playbook_path: str) -> None:
    """systemd must pass Bash variables through instead of expanding restart scripts itself."""
    source = Path(playbook_path).read_text(encoding="utf-8")

    assert "systemd-run --user" in source
    assert "--expand-environment=no" in source


@pytest.mark.parametrize(
    ("role", "expected"),
    [("master", "master"), ("slave", "slave"), ("vps", "slave"), ("", "slave")],
)
def test_public_runtime_role_only_exposes_master_or_slave(role: str, expected: str) -> None:
    """The internal Cluster runner role must not become a third PBGui runtime role."""
    assert service_mod._public_runtime_role(role) == expected


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


@pytest.mark.parametrize("playbook_path", ["master-update-pbgui.yml", "master-update-pb.yml"])
def test_master_updates_migrate_missing_legacy_master_role(playbook_path: str) -> None:
    """Master updates repair old configs without replacing an explicit role."""
    source = Path(playbook_path).read_text(encoding="utf-8")

    task = source.split("- name: Migrate missing legacy master role", 1)[1]
    task = task.split("\n    - name:", 1)[0]
    assert "role=load_ini('main', 'role').strip()" in task
    assert "changed=not role" in task
    assert "save_ini('main', 'role', 'master') if changed else None" in task
    assert "'changed=true' in (legacy_master_role.stdout | default(''))" in task


@pytest.mark.parametrize("playbook_path", ["master-update-pb8.yml", "vps-update-pb8.yml"])
def test_pb8_playbooks_validate_before_restarting_live_processes(playbook_path: str) -> None:
    """PB8 updates restart managed live bots only after complete runtime validation."""
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
    assert "pip install" in source
    assert "--upgrade -e" in source
    assert 'if [ -f "$HOME/.cargo/env" ]; then' in source
    assert "Validate PB8 CLI" in source
    assert "import passivbot_rust" in source
    assert "Save PB8 runtime paths after validation" in source
    assert source.index("Mark PB8 runtime unavailable") < checkout_index
    assert "Validate PB8 Rust module and config schema" not in source
    stamp_task = source.split("- name: Stamp and validate PB8 Rust source fingerprint", 1)[1]
    stamp_task = stamp_task.split("\n    - name:", 1)[0]
    assert "stamp_compiled_extensions(source_fingerprint())" in stamp_task
    assert "check_and_maybe_compile(fail_on_stale=True)" in stamp_task
    assert "\n      when:" not in stamp_task
    assert source.index("Stamp and validate PB8 Rust source fingerprint") < source.index("Mark validated PB8 runtime available")
    assert source.index("Mark validated PB8 runtime available") < source.index(
        "Restart managed PB8 live bots after successful update"
    )
    assert "pb8_live_stop.py" in source
    assert "pb8-runtime-invalid" in source
    assert "Acquire PB8 update writer ownership" in source
    assert "Release PB8 update writer ownership" in source
    assert "force_handlers: true" in source
    if playbook_path == "vps-update-pb8.yml":
        assert "master_update_lock.py" in source
        assert "--barrier" in source
        assert "['master', 'vps', 'slave']" in source
        assert "pb8_install_profile" in source
        assert "Install PB8 live profile" in source
        assert 'pip install --no-cache-dir --upgrade -e "{{ pb8dir }}"' in source
        assert "Remove PB8 Rust build artifacts on live-only runners" in source
        assert "Measure PB8 disk state before changes" in source
        assert "Measure PB8 disk state after validation" in source
        assert "pb8_min_free_bytes | default(3221225472)" in source
    else:
        assert "pb8_role_probe.stdout | trim | lower == 'master'" in source
    assert "stop-processes" not in source
    assert "kill all" not in source
    assert "starter.py" not in source


def test_pb8_live_stop_targets_only_exact_managed_config_directories(tmp_path: Path) -> None:
    """The update helper must delegate exact process matching to PBRun's PB8 runner."""
    pbgdir = tmp_path / "pbgui"
    run_root = pbgdir / "data" / "run_v8"
    managed = run_root / "alice"
    managed.mkdir(parents=True)
    (managed / "config.json").write_text("{}", encoding="utf-8")
    ignored = run_root / "incomplete"
    ignored.mkdir()
    calls: list[tuple] = []

    class FakeRunV8:
        """Capture the identity fields supplied to PBRun's exact matcher."""

        def pid(self):
            calls.append((self.path, self.user, self.pb8dir, self.pb8venv, self.pbgdir))
            return SimpleNamespace(pid=321)

        def stop(self, process) -> None:
            calls.append(("stop", process.pid))

    stopped = pb8_live_stop.stop_managed_pb8_live_processes(
        pbgdir,
        tmp_path / "pb8",
        tmp_path / "venv_pb8" / "bin" / "python",
        run_v8_class=FakeRunV8,
    )

    assert stopped == [321]
    assert calls == [
        (
            str(managed),
            "alice",
            str(tmp_path / "pb8"),
            str(tmp_path / "venv_pb8" / "bin" / "python"),
            str(pbgdir),
        ),
        ("stop", 321),
    ]


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


def test_pb8_runtime_info_keeps_installed_runtime_visible_when_update_marker_blocks_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed update remains an installed PB8 runtime with an explicit repair blocker."""
    repo = tmp_path / "pb8"
    schema = repo / "src" / "config" / "schema.py"
    schema.parent.mkdir(parents=True)
    schema.write_text('CONFIG_SCHEMA_VERSION = "v8.0.0"\n', encoding="utf-8")
    python_path = tmp_path / "venv_pb8" / "bin" / "python"
    python_path.parent.mkdir(parents=True)
    python_path.symlink_to(Path(service_mod.sys.executable))
    (python_path.parent / "passivbot").write_text("#!/bin/sh\n", encoding="utf-8")
    pbgui_dir = tmp_path / "pbgui"
    marker = pbgui_dir / "data" / "locks" / "pb8-runtime-invalid"
    marker.parent.mkdir(parents=True)
    marker.touch()
    monkeypatch.setattr(service_mod, "get_current_pb7_status", lambda _repo: ("master", "a" * 40))
    monkeypatch.setattr(service_mod, "read_local_pb7_version", lambda _repo: "v8.0.0")

    blocked = service_mod._pb8_runtime_info(str(repo), str(python_path), pbgui_dir=pbgui_dir)

    assert blocked["installed"] is True
    assert blocked["runtime_ready"] is False
    assert blocked["runtime_blocked"] is True
    assert blocked["runtime_reason"] == "PB8 installation or update did not complete; run Update PB8."


def test_verified_detached_pb8_upstream_is_labelled_master() -> None:
    """The hardened detached checkout should not be presented as an unknown branch."""
    assert service_mod._pb8_branch_label("unknown", "✅") == "master"
    assert service_mod._pb8_branch_label("HEAD", "✅") == "master"
    assert service_mod._pb8_branch_label("HEAD", "❌ abcdef0") == "unknown"
    assert service_mod._pb8_branch_label("unknown", "❌ v8.1.0 (abcdef0)") == "unknown"
    assert service_mod._pb8_branch_label("feature", "✅") == "feature"


def test_master_pb8_branch_state_exposes_runtime_remotes_and_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """Master details provide the complete PB8 branch-management contract."""
    service = object.__new__(VPSManagerService)
    service._pb8_branches = {"feature": [{"full": "b" * 40}]}
    service._build_pb8_github_status = lambda _commit: "❌ different"
    service._get_local_host_meta = lambda: {"pb8b": "feature", "pb8c": "a" * 40}
    monkeypatch.setattr(service_mod, "configured_pb8dir", lambda: "/runtime/pb8")
    monkeypatch.setattr(service_mod, "configured_pb8venv", lambda: "/runtime/venv_pb8/bin/python")
    monkeypatch.setattr(
        service_mod,
        "_pb8_runtime_info",
        lambda _repo, _python: {"branch": "feature", "commit": "a" * 40},
    )
    monkeypatch.setattr(service_mod, "list_git_remotes", lambda _repo: ["origin", "fork"])
    monkeypatch.setattr(service_mod, "get_git_remote_url", lambda _repo, remote: f"https://example.test/{remote}.git")
    monkeypatch.setattr(service_mod, "get_git_branch_remote", lambda _repo, _branch: "fork")
    monkeypatch.setattr(service_mod, "get_git_branch_remotes", lambda _repo, _branches: {"feature": "fork"})

    state = service._build_master_pb8_branch_state()

    assert state["current_branch"] == "feature"
    assert state["current_commit"] == "a" * 40
    assert state["branches"] == {"feature": [{"full": "b" * 40}]}
    assert state["default_remote_name"] == "fork"
    assert state["branch_tracking_remotes"] == {"feature": "fork"}
    assert state["upstream_remote_url"] == "https://github.com/enarjord/passivbot.git"


def test_remote_pb8_branch_state_does_not_copy_controller_remotes() -> None:
    """Remote PB8 branch controls start from telemetry without controller Git configuration."""
    service = object.__new__(VPSManagerService)
    service._host_meta = lambda _state: {
        "pb8b": "feature",
        "pb8c": "a" * 40,
    }
    service._build_pb8_github_status = lambda _commit: "❌ different"

    state = service._build_vps_pb8_branch_state({}, "runner-a")

    assert state["current_branch"] == "feature"
    assert state["current_commit"] == "a" * 40
    assert state["branches"] == {
        "feature": [{"short": "aaaaaaa", "full": "a" * 40}],
    }
    assert state["known_remotes"] == ["origin", "fork"]
    assert state["remote_urls"] == {}
    assert state["branch_tracking_remotes"] == {}


@pytest.mark.parametrize("playbook_path", ["master-update-pb8.yml", "vps-update-pb8.yml"])
def test_pb8_playbooks_validate_optional_branch_before_checkout(playbook_path: str) -> None:
    """Explicit PB8 branch changes preserve the official path and validate v8 before checkout."""
    source = Path(playbook_path).read_text(encoding="utf-8")

    assert "pb8_branch_switch" in source
    assert "Check PB8 checkout for tracked changes" in source
    assert "Refuse PB8 branch switch with tracked changes" in source
    assert "Require selected PB8 source branch" in source
    assert "Resolve selected PB8 ref" in source
    assert "Require selected PB8 commit to belong to source branch" in source
    assert "Verify selected PB8 commit before checkout" in source
    assert "Mark PB8 runtime unavailable for branch checkout" in source
    assert "Checkout selected PB8 branch at verified commit" in source
    assert "Set selected PB8 branch upstream" in source
    verify = source.index("Verify selected PB8 commit before checkout")
    checkout = source.index("Checkout selected PB8 branch at verified commit")
    assert source.index("Check PB8 checkout for tracked changes") < source.index("Inspect selected PB8 remote")
    assert source.index("Require selected PB8 commit to belong to source branch") < verify
    assert verify < source.index("Mark PB8 runtime unavailable for branch checkout") < checkout
    assert "--expected-major\n          - \"8\"" in source
    assert "Fetch and verify latest official Passivbot v8" in source
    assert "Checkout verified official Passivbot v8 commit" in source


def test_local_pb8_command_requires_master_and_rejects_unrelated_custom_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct local PB8 requests cannot bypass role or inject unrelated playbook variables."""
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


def test_local_pb8_branch_command_passes_only_validated_switch_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """A local PB8 branch switch preserves its label and normalized safe variables."""
    calls: list[dict] = []
    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_master=lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(service_mod, "load_ini", lambda _section, _parameter: "master")
    monkeypatch.setattr(
        service_mod,
        "_pb8_runtime_info",
        lambda _repo, _python: {"installed": True},
    )

    service.run_master_command(
        command="master-update-pb8",
        command_text="Sync fork/feature -> feature",
        extra_vars={
            "pb8_branch": "feature",
            "pb8_source_branch": "feature/source",
            "pb8_commit": "A" * 40,
            "pb8_remote_name": "fork",
            "pb8_remote_url": "https://github.com/example/passivbot.git",
        },
    )

    assert calls[0]["command_text"] == "Sync fork/feature -> feature"
    assert calls[0]["extra_vars"] == {
        "pb8_branch": "feature",
        "pb8_source_branch": "feature/source",
        "pb8_commit": "a" * 40,
        "pb8_remote_name": "fork",
        "pb8_remote_url": "https://github.com/example/passivbot.git",
    }


@pytest.mark.parametrize(
    "extra_vars",
    [
        {"pb8_branch": "../master"},
        {"pb8_branch": "feature/.hidden"},
        {"pb8_branch": "feature", "pb8_source_branch": "bad branch"},
        {"pb8_branch": "feature", "pb8_remote_name": "-fork"},
        {"pb8_branch": "feature", "pb8_remote_name": ".fork"},
        {"pb8_branch": "feature", "pb8_remote_name": "foo..bar"},
        {"pb8_branch": "feature", "pb8_remote_url": "https://example.test/repo.git\n--upload-pack=x"},
        {"pb8_branch": "feature", "pb8_remote_url": "file:///tmp/passivbot.git"},
        {"pb8_branch": "feature", "pb8_remote_url": "https://token@example.test/passivbot.git"},
        {"pb8_branch": "feature", "pb8_commit": "abc123"},
        {"pb8_branch": "feature", "target_hosts": "all"},
    ],
)
def test_pb8_branch_vars_reject_invalid_git_identifiers(extra_vars: dict[str, str]) -> None:
    """PB8 branch variables fail closed before reaching Git or Ansible."""
    with pytest.raises(ValueError):
        service_mod._validate_pb8_branch_extra_vars(extra_vars)


@pytest.mark.parametrize(
    ("fresh", "role", "disk_free", "message"),
    [
        (False, "master", 8 * 1024**3, "telemetry"),
        (True, "unknown", 8 * 1024**3, "master or VPS runner"),
        (True, "vps", 2 * 1024**3, "3 GiB"),
    ],
)
def test_remote_pb8_command_requires_fresh_master_telemetry(
    fresh: bool,
    role: str,
    disk_free: int,
    message: str,
) -> None:
    """Selected remote hosts fail closed on stale, unknown, or low-disk telemetry."""
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="remote-a", user_pw=None)
    service.vpsmanager = SimpleNamespace(update_vps=lambda *args, **kwargs: None)
    service._require_vps = lambda _hostname: vps
    service._apply_session_secrets_to_vps = lambda _token, _vps: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda _state, _hostname: {
        "meta": {"role": role},
        "system": {"disk_free": disk_free},
    }
    service._host_telemetry_fresh = lambda _state: fresh

    with pytest.raises(ValueError, match=message):
        service.run_vps_command(
            token="token",
            hostname="remote-a",
            command="vps-update-pb8",
            command_text="Install PB8",
        )


def test_remote_pb7_install_is_blocked_below_disk_reserve_with_pb8_present() -> None:
    """A PB8-only small VPS cannot start a PB7 first installation below 3 GiB free."""
    state = {
        "meta": {
            "role": "vps",
            "pb8ready": True,
            "pb7v": "N/A",
            "pb7c": "",
            "pb7py": "N/A",
        },
        "system": {"disk_free": 2944 * 1024**2},
    }

    result = service_mod._pb7_remote_action_status(state, telemetry_fresh=True)

    assert result["allowed"] is False
    assert result["installed"] is False
    assert result["required_free_disk_bytes"] == service_mod.PB7_MIN_FREE_DISK_BYTES
    assert "requires at least 3 GiB" in result["reason"]
    assert "currently 2.88 GiB" in result["reason"]


def test_installed_remote_pb7_update_does_not_require_installation_disk_reserve() -> None:
    """A confirmed PB7 checkout remains updateable below the first-install reserve."""
    result = service_mod._pb7_remote_action_status(
        {
            "meta": {"pb7v": "v7.8.3", "pb7c": "a" * 40, "pb7py": "3.12"},
            "system": {"disk_free": 1 * 1024**3},
        },
        telemetry_fresh=True,
    )

    assert result["allowed"] is True
    assert result["installed"] is True
    assert result["required_free_disk_bytes"] == 0


def test_remote_pb7_command_rejects_low_disk_first_install() -> None:
    """The backend blocks direct PB7 install requests even if the disabled UI is bypassed."""
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="small-pb8", user_pw=None)
    service.vpsmanager = SimpleNamespace(update_vps=lambda *args, **kwargs: None)
    service._require_vps = lambda _hostname: vps
    service._apply_session_secrets_to_vps = lambda _token, _vps: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda _state, _hostname: {
        "meta": {"role": "vps", "pb8ready": True},
        "system": {"disk_free": 2944 * 1024**2},
    }
    service._host_telemetry_fresh = lambda _state: True

    with pytest.raises(ValueError, match="PB7 installation requires at least 3 GiB"):
        service.run_vps_command(
            token="token",
            hostname="small-pb8",
            command="vps-update-pb7",
            command_text="Install PB7",
        )


def test_remote_pb8_command_starts_only_for_fresh_master() -> None:
    """Fresh runner telemetry permits the dedicated live-only PB8 playbook."""
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
        "meta": {"role": "vps", "pb8ready": False},
        "system": {"disk_free": 4 * 1024**3},
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
    assert captured["extra_vars"] == {"pb8_min_free_bytes": service_mod.PB8_MIN_FREE_DISK_BYTES}
    assert result["command"] == "vps-update-pb8"


def test_remote_pb8_branch_command_uses_installed_runtime_without_disk_reserve() -> None:
    """An installed remote PB8 runtime accepts a validated branch switch payload."""
    captured: dict[str, object] = {}
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(hostname="runner-a", user_pw=None, command_run_id="run-branch")
    vps._task_log_path = lambda command, _fallback: Path(f"{command}--run-branch.log")
    service.vpsmanager = SimpleNamespace(
        update_vps=lambda target, **kwargs: captured.update(kwargs)
    )
    service._require_vps = lambda _hostname: vps
    service._apply_session_secrets_to_vps = lambda _token, _vps: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda _state, _hostname: {
        "meta": {"role": "vps", "pb8ready": True},
        "system": {"disk_free": 1 * 1024**3},
    }
    service._host_telemetry_fresh = lambda _state: True
    service._credential_playbook_vars = lambda _hostname, _state: {}
    service._raise_if_vps_task_active = lambda _vps, _label: None

    service.run_vps_command(
        token="token",
        hostname="runner-a",
        command="vps-update-pb8",
        command_text="Sync fork/feature -> feature",
        extra_vars={
            "pb8_branch": "feature",
            "pb8_source_branch": "feature",
            "pb8_commit": "b" * 40,
            "pb8_remote_name": "fork",
            "pb8_remote_url": "git@github.com:example/passivbot.git",
        },
    )

    assert captured["command"] == "vps-update-pb8"
    assert captured["command_text"] == "Sync fork/feature -> feature"
    assert captured["extra_vars"] == {
        "pb8_branch": "feature",
        "pb8_source_branch": "feature",
        "pb8_commit": "b" * 40,
        "pb8_remote_name": "fork",
        "pb8_remote_url": "git@github.com:example/passivbot.git",
        "pb8_min_free_bytes": 0,
    }


def test_installed_remote_pb8_update_does_not_require_installation_disk_reserve() -> None:
    """A validated PB8 runtime remains updateable below the first-install disk threshold."""
    result = service_mod._pb8_remote_action_status(
        {
            "meta": {"role": "vps", "pb8ready": True},
            "system": {"disk_free": 2 * 1024**3},
        },
        telemetry_fresh=True,
    )

    assert result["allowed"] is True
    assert result["installed"] is True
    assert result["required_free_disk_bytes"] == 0
    assert result["reason"] == ""


def test_pb8_only_remote_master_combined_update_uses_combined_master_playbook() -> None:
    """The PBGui+PB8 action must not run its PB8 half on the local controller host."""
    captured: dict[str, object] = {}
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(
        hostname="remote-master",
        user="mani",
        remote_pbgui_dir="/home/mani/software/pbgui",
        user_pw=None,
        command_run_id="run-8",
    )
    vps._task_log_path = lambda command, _fallback: Path(f"{command}--run-8.log")
    service.vpsmanager = SimpleNamespace(
        update_vps=lambda target, **kwargs: captured.update(kwargs)
    )
    service._require_vps = lambda _hostname: vps
    service._apply_session_secrets_to_vps = lambda _token, _vps: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda _state, _hostname: {
        "meta": {"role": "master", "pb8ready": True},
        "system": {"disk_free": 1 * 1024**3},
    }
    service._host_telemetry_fresh = lambda _state: True
    service._credential_playbook_vars = lambda _hostname, _state: {}
    service._sync_vps_config_from_host_meta = lambda _vps, _state: None
    service._raise_if_vps_task_active = lambda _vps, _label: None

    result = service.run_vps_command(
        token="token",
        hostname="remote-master",
        command="vps-update-pbgui-pb8",
        command_text="Update PBGui and PB8",
    )

    assert captured["command"] == "master-update-pbgui-pb8"
    assert captured["command_text"] == "Update PBGui and PB8"
    assert captured["extra_vars"]["target_hosts"] == "remote-master"
    assert captured["extra_vars"]["pb8_min_free_bytes"] == 0
    assert result["command"] == "master-update-pbgui-pb8"


def test_remote_monitor_payload_keeps_pb7_and_pb8_processes_separate() -> None:
    """VPS detail monitoring must preserve runtime identity for metrics and fallbacks."""
    service = object.__new__(VPSManagerService)
    service.monitor_config = SimpleNamespace(
        cpu_warning_v7=80,
        cpu_error_v7=95,
        mem_warning_v7=1000,
        mem_error_v7=2000,
        swap_warning_v7=100,
        swap_error_v7=200,
        error_warning_v7=1,
        error_error_v7=5,
        traceback_warning_v7=1,
        traceback_error_v7=5,
    )
    service._build_remote_server_metrics = lambda _hostname, _state: {}
    history_lookups = []
    service._bot_pnl_total = lambda _hostname, name: (history_lookups.append(("pnl", name)) or (0.0, 0))
    service._bot_count_total = lambda _hostname, name, kind: (history_lookups.append((kind, name)) or 0)
    metrics = [0] * 10
    host_state = {
        "meta": {"pb7v": "v7.7.7", "pb8v": "v8.0.0"},
        "instances": [
            {"p": "7", "u": "same", "m": metrics},
            {"p": "8", "u": "same", "m": metrics},
        ],
        "v7_instances": [],
        "v8_instances": [{"name": "v8-fallback", "running": True, "cv": 3, "eo": "host-a"}],
    }

    payload = service._build_remote_monitor_payload("host-a", host_state)

    assert [(item["name"], item["pb_version"], item["version"]) for item in payload["v7"]] == [
        ("same", "7", "v7.7.7")
    ]
    assert [(item["name"], item["pb_version"], item["version"]) for item in payload["v8"]] == [
        ("same", "8", "v8.0.0")
    ]
    assert ("pnl", "same") in history_lookups
    assert ("pnl", "8:same") in history_lookups
    assert ("errors", "8:same") in history_lookups
    assert ("tracebacks", "8:same") in history_lookups
    assert payload["v8_running"] == [{
        "name": "v8-fallback",
        "version": 3,
        "enabled_on": "host-a",
        "blocked": False,
        "blocked_reason": "",
        "cluster_gate": "",
        "pb_version": "8",
    }]


def test_host_telemetry_includes_pb8_instance_snapshot() -> None:
    """VPS Manager must carry the monitor store's PB8 desired/runtime snapshot into details."""
    service = object.__new__(VPSManagerService)
    monitor_state = {
        "connections": {"connections": {"host-a": {"status": "connected"}}},
        "system": {"host-a": {"disk_free": 1}},
        "instances": {"host-a": []},
        "v7_instances": {"host-a": [{"name": "seven"}]},
        "v8_instances": {"host-a": [{"name": "eight"}]},
        "host_meta": {"host-a": {"role": "vps"}},
        "streams": {"host-a": {"last_update": 1}},
    }

    result = service._get_host_telemetry(monitor_state, "host-a")

    assert result["v7_instances"] == [{"name": "seven"}]
    assert result["v8_instances"] == [{"name": "eight"}]


@pytest.mark.parametrize(
    ("command", "initial_profile"),
    [
        ("vps-update-pb7", "pb8"),
        ("vps-update-pb8", "pb7"),
        ("vps-update-pbgui-pb8", "pb7"),
    ],
)
def test_successful_runtime_install_expands_saved_vps_profile(command, initial_profile, tmp_path) -> None:
    """Dedicated runtime installs must make later host-capability checks recognize both runtimes."""
    vps = object.__new__(core_mod.VPS)
    vps.command = command
    vps.command_text = command
    vps.update_status = "successful"
    vps.runtime_profile = initial_profile
    vps.privat_data_dir = None
    vps.path = tmp_path
    vps.save = lambda: None

    vps.update_finished(private_data_dir=tmp_path / "missing")

    assert vps.runtime_profile == "pb7_pb8"


def test_pb8_actions_support_single_and_profile_aware_bulk_updates() -> None:
    """PB8 actions support eligible runners and profile-aware bulk dispatch."""
    source = Path("frontend/vps_manager.html").read_text(encoding="utf-8")

    assert 'runMasterWithLog("master-update-pb8"' in source
    assert "data-command='vps-update-pb8'" in source
    assert "isRemoteMaster && st.pb8_action_allowed" not in source
    assert "st.pb8_action_allowed ?" in source
    assert "st.pb8_install_profile === 'live'" in source
    assert "st.pb8_action_reason" in source
    assert "pb8_reason: String(st.pb8_action_reason || '')" in source
    assert "pb8_profile: String(st.pb8_install_profile || '')" in source
    assert "vps-update-pbgui-pb8" in source
    assert "Update PBGui and PB8" in source
    assert service_mod.COMMAND_VPS_UPDATE_PB8 in service_mod.VPS_DEPLOY_ACTIONS
    assert service_mod.COMMAND_VPS_UPDATE_RUNTIME in service_mod.VPS_DEPLOY_ACTIONS


def test_pb7_playbook_checks_first_install_disk_before_downloads() -> None:
    """The remote PB7 playbook fails before rust/git downloads on insufficient disk."""
    source = Path("vps-update-pb7.yml").read_text(encoding="utf-8")

    gate = source.index("Require free disk space for a new PB7 installation")
    assert gate < source.index("- name: Update rust")
    assert gate < source.index("Clone official Passivbot for a new PB7 installation")
    assert "pb7_min_free_bytes | default(3221225472)" in source
    assert "when: not pb7_git_before_disk_gate.stat.exists" in source


def test_pb8_remote_action_status_uses_role_and_free_disk_only() -> None:
    """Remote PB8 eligibility accepts low-RAM runners when fresh disk telemetry is sufficient."""
    runner = service_mod._pb8_remote_action_status(
        {"meta": {"role": "vps"}, "system": {"disk_free": 4 * 1024**3}},
        telemetry_fresh=True,
    )
    master = service_mod._pb8_remote_action_status(
        {"meta": {"role": "master"}, "system": {"disk_free": 4 * 1024**3}},
        telemetry_fresh=True,
    )
    low_disk = service_mod._pb8_remote_action_status(
        {"meta": {"role": "slave"}, "system": {"disk_free": 1 * 1024**3}},
        telemetry_fresh=True,
    )

    assert runner["allowed"] is True
    assert runner["profile"] == "live"
    assert master["allowed"] is True
    assert master["profile"] == "full"
    assert low_disk["allowed"] is False
    assert "currently 1.00 GiB" in low_disk["reason"]


def test_pb8_runner_playbook_sources_optional_cargo_environment() -> None:
    """Distro rustup installs do not emit a missing ~/.cargo/env warning."""
    source = Path("vps-update-pb8.yml").read_text(encoding="utf-8")

    assert source.count('if [ -f "$HOME/.cargo/env" ]; then') == 3
    assert source.count('source "$HOME/.cargo/env"') == 3


def test_runtime_update_playbooks_support_pb8_only_maintenance_and_pb7_install() -> None:
    """Dedicated maintenance paths must cover PB8-only combined updates and later PB7 installation."""
    remote_combined = Path("vps-update-pbgui-pb8.yml").read_text(encoding="utf-8")
    master_combined = Path("master-update-pbgui-pb8.yml").read_text(encoding="utf-8")
    master_pb8 = Path("master-update-pb8.yml").read_text(encoding="utf-8")
    pb7_update = Path("vps-update-pb7.yml").read_text(encoding="utf-8")
    pb8_update = Path("vps-update-pb8.yml").read_text(encoding="utf-8")

    assert "import_playbook: vps-update-pbgui.yml" in remote_combined
    assert "import_playbook: vps-update-pb8.yml" in remote_combined
    assert "import_playbook: master-update-pbgui.yml" in master_combined
    assert "import_playbook: master-update-pb8.yml" in master_combined
    assert 'hosts: "{{ target_hosts | default(\'localhost\') }}"' in master_pb8
    assert "Clone official Passivbot for a new PB7 installation" in pb7_update
    assert "Refuse a non-Git PB7 target" in pb7_update
    assert "Ensure PBRun is enabled for PB8 live supervision" in pb8_update
    assert "--no-disable-excluded" in pb8_update
    assert "--no-start" in pb8_update
    assert pb8_update.index("Ensure PBRun is enabled for PB8 live supervision") < pb8_update.index(
        "Check whether PBRun is already active"
    ) < pb8_update.index("Start PBRun when inactive")
    assert "pb8_pbrun_active.rc != 0" in pb8_update
    assert "Detect PB8-only support in installed PBRun" in pb8_update
    assert '"self.pb8_ready = bool"' in pb8_update
    assert "Disable incompatible PBRun on PB8-only hosts" in pb8_update
    assert "Update PBGui before enabling PB8 live instances" in pb8_update
    assert "pb8_pbrun_compatibility_probe.rc == 0" in pb8_update
    assert "pb8_pbrun_compatibility_probe.rc != 0" in pb8_update
    assert "register: pb8_role_probe" in pb8_update
    assert "pb8_role_probe.stdout | trim | lower" in pb8_update
    assert "pbgui_role.stdout" not in pb8_update
    assert "register: pb8_role_probe" in master_pb8
    assert "pbgui_role.stdout" not in master_pb8


def test_vps_setup_pb8_profile_skips_every_pb7_runtime_task() -> None:
    """PB8-only setup skips PB7 artifacts while enabling the shared PBRun service."""
    source = Path("vps-setup.yml").read_text(encoding="utf-8")
    pb7_tasks = (
        "Validate pinned PB7 commit",
        "Check for existing PB7 checkout",
        "Verify pinned PB7 commit before changing an existing runtime",
        "Stop PBRun before replacing an existing PB7 runtime",
        "Stop processes from the existing PB7 runtime",
        "Clone Passivbot without checking out upstream master",
        "Fetch, verify and checkout pinned Passivbot v7",
        "create python3.12 venv for pb7",
        "Build passivbot-rust with maturin",
        "Write rust source stamp after maturin build",
    )

    for task_name in pb7_tasks:
        block = source.split(f"- name: {task_name}", 1)[1].split("\n    - name:", 1)[0]
        assert "when:" in block
        assert "install_pb7 | bool" in block
    assert "remove PB7 runtime paths for PB8-only setup" in source
    assert "(['pbrun'] if ((install_pb7 | bool) or (install_pb8 | bool)) else [])" in source
    assert "(['pbgui-pbrun.service'] if ((install_pb7 | bool) or (install_pb8 | bool)) else [])" in source
    assert "in ['pb7', 'pb7_pb8']" in source
    assert "option: role\n        value: slave" in source


def test_vps_setup_runs_pb8_inside_the_same_ansible_run() -> None:
    """PB8 setup is a second play in vps-setup, not a Python follow-up job."""
    setup_source = Path("vps-setup.yml").read_text(encoding="utf-8")
    pb8_source = Path("vps-update-pb8.yml").read_text(encoding="utf-8")
    core_source = Path("vps_manager_core.py").read_text(encoding="utf-8")
    setup_method = core_source.split("    def setup_vps(self, vps: VPS", 1)[1].split(
        "    def update_vps(self, vps: VPS", 1
    )[0]

    assert "install_pb8:" in setup_source
    assert "groups: pbgui_pb8_setup" in setup_source
    assert "when: install_pb8 | bool" in setup_source
    assert "import_playbook: vps-update-pb8.yml" in setup_source
    assert "pb8_target_group: pbgui_pb8_setup" in setup_source
    assert "pb8_target_group if pb8_target_group is defined else hostname" in pb8_source
    assert "self.update_vps(" not in setup_method
    assert "finished_callback=vps.setup_finished" in setup_method


@pytest.mark.parametrize("runtime_profile", ["pb8", "pb7_pb8"])
def test_vps_runtime_profile_is_validated_and_exposed(runtime_profile: str) -> None:
    """Only supported setup profiles cross the form-to-inventory boundary."""
    service = object.__new__(VPSManagerService)
    service._store_session_secrets = lambda token, hostname, form: None
    service._session_secret_value = lambda token, hostname, field: ""
    service._session_secret_meta = lambda token, hostname: {}
    vps = SimpleNamespace(
        hostname="fresh-runner",
        user="bot",
        user_pw=None,
        swap="2G",
        runtime_profile="pb7",
        remote_pbgui_dir="/home/bot/software/pbgui",
        firewall=False,
        firewall_ssh_port=22,
        firewall_ssh_ips="",
        ip="192.0.2.20",
        init_methode="root",
        remove_user=False,
        save=lambda: None,
    )
    form = {
        "runtime_profile": runtime_profile,
        "swap": "2G",
        "install_dir": "/home/bot/software",
        "firewall": False,
        "firewall_ssh_port": 22,
        "firewall_ssh_ips": "",
    }

    service._apply_vps_setup_form("token", vps, form)

    assert vps.runtime_profile == runtime_profile
    assert service._build_vps_config("token", vps)["runtime_profile"] == runtime_profile
    with pytest.raises(ValueError, match="runtime profile"):
        service._apply_vps_setup_form("token", vps, {**form, "runtime_profile": "full"})
