"""Regression tests for idempotent VPS monitor-agent deployment."""

from __future__ import annotations

import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import textwrap
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
SETUP_SCRIPT = ROOT / "setup" / "setup_systemd.sh"
API_HANDOFF_SCRIPT = ROOT / "setup" / "stop_legacy_api.sh"
UPDATE_PLAYBOOKS = (
    "vps-update-pbgui.yml",
    "vps-update-pb.yml",
    "master-update-pbgui.yml",
    "master-update-pb.yml",
)


def _write_executable(path: Path, source: str) -> None:
    """Write an executable used by the isolated shell-script harness."""

    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _fake_systemd_environment(tmp_path: Path) -> tuple[dict[str, str], Path, Path, Path]:
    """Create fake account and systemctl commands backed by temporary state."""

    home = tmp_path / "home" / "agent-user"
    pbgui_dir = tmp_path / "custom-root" / "pbgui"
    python_bin = tmp_path / "custom-root" / "venv_pbgui" / "bin" / "python"
    fake_bin = tmp_path / "bin"
    state_dir = tmp_path / "systemd-state"
    calls_path = tmp_path / "systemctl.calls"
    for path in (home, pbgui_dir, python_bin.parent, fake_bin, state_dir / "enabled", state_dir / "active"):
        path.mkdir(parents=True, exist_ok=True)
    python_bin.touch()
    python_bin.chmod(0o755)

    _write_executable(
        fake_bin / "id",
        """#!/usr/bin/env bash
set -e
if [[ "${1:-}" == "-u" ]]; then
  printf '%s\n' "$FAKE_TEST_UID"
elif [[ "${1:-}" == "-g" ]]; then
  printf '%s\n' "$FAKE_TEST_GID"
elif [[ "${1:-}" == "-gn" ]]; then
  printf 'agent-user\n'
else
  printf 'uid=1000(agent-user) gid=1000(agent-user) groups=1000(agent-user)\n'
fi
""",
    )
    _write_executable(
        fake_bin / "getent",
        f"""#!/usr/bin/env bash
if [[ "${{1:-}}" == "passwd" && "${{2:-}}" == "agent-user" ]]; then
  printf 'agent-user:x:1000:1000::%s:/bin/bash\n' {str(home)!r}
  exit 0
fi
exit 2
""",
    )
    _write_executable(
        fake_bin / "systemctl",
        """#!/usr/bin/env bash
set -u
printf '%s\n' "$*" >> "$FAKE_SYSTEMCTL_CALLS"
if [[ "${1:-}" == "--user" ]]; then
  shift
fi
command="${1:-}"
[[ $# -gt 0 ]] && shift
state_for() {
  local unit="$1"
  if [[ -f "$FAKE_SYSTEMD_STATE/active/$unit" ]]; then
    cat "$FAKE_SYSTEMD_STATE/active/$unit"
  else
    printf 'inactive\n'
  fi
}
restart_count_for() {
  local unit="$1"
  if [[ -f "$FAKE_SYSTEMD_STATE/restarts/$unit" ]]; then
    cat "$FAKE_SYSTEMD_STATE/restarts/$unit"
  else
    printf '0\n'
  fi
}
case "$command" in
  show)
    unit=""
    if [[ "${1:-}" != --* ]]; then
      unit="$1"
      shift
    fi
    properties="$*"
    if [[ -z "$unit" ]]; then
      [[ "$properties" == *"NeedDaemonReload"* ]] && printf '%s\n' "${FAKE_NEED_DAEMON_RELOAD:-no}"
    elif [[ "$properties" == *"--value"* ]]; then
      active="$(state_for "$unit")"
      case "$properties" in
        *NRestarts*) restart_count_for "$unit" ;;
        *ActiveState*) printf '%s\n' "$active" ;;
        *SubState*) [[ "$active" == active ]] && printf 'running\n' || printf '%s\n' "$active" ;;
        *Result*) [[ "$active" == failed ]] && printf 'exit-code\n' || printf 'success\n' ;;
        *ExecMainStatus*) [[ "$active" == failed ]] && printf '1\n' || printf '0\n' ;;
      esac
    else
      active="$(state_for "$unit")"
      result="$([[ "$active" == failed ]] && printf exit-code || printf success)"
      status="$([[ "$active" == failed ]] && printf 1 || printf 0)"
      printf 'LoadState=loaded\nUnitFileState=enabled\nActiveState=%s\nSubState=%s\nResult=%s\nExecMainStatus=%s\nNRestarts=%s\nFragmentPath=fake\nNeedDaemonReload=no\n' "$active" "$([[ "$active" == active ]] && printf running || printf "$active")" "$result" "$status" "$(restart_count_for "$unit")"
    fi
    ;;
  is-enabled)
    unit="${1:-}"
    if [[ -f "$FAKE_SYSTEMD_STATE/enabled/$unit" ]]; then
      printf 'enabled\n'
      exit 0
    fi
    printf 'disabled\n'
    exit 1
    ;;
  enable)
    if [[ "${FAKE_FAIL_ACTION:-}" == enable ]]; then
      printf 'failed\n' > "$FAKE_SYSTEMD_STATE/active/${1:?}"
      exit 1
    fi
    touch "$FAKE_SYSTEMD_STATE/enabled/${1:?}"
    ;;
  disable)
    rm -f "$FAKE_SYSTEMD_STATE/enabled/${1:?}"
    ;;
  restart|start)
    if [[ "${FAKE_FAIL_ACTION:-}" == "$command" ]]; then
      printf 'failed\n' > "$FAKE_SYSTEMD_STATE/active/${1:?}"
      exit 1
    fi
    for unit in "$@"; do
      printf 'active\n' > "$FAKE_SYSTEMD_STATE/active/$unit"
    done
    ;;
  stop)
    printf 'inactive\n' > "$FAKE_SYSTEMD_STATE/active/${1:?}"
    ;;
  reset-failed|daemon-reload|status|show-environment)
    ;;
  is-active)
    if [[ "${1:-}" == "--quiet" ]]; then
      shift
    fi
    unit="${1:?}"
    [[ "$(state_for "$unit")" == "active" ]]
    ;;
  *)
    printf 'unsupported fake systemctl command: %s\n' "$command" >&2
    exit 2
    ;;
esac
""",
    )
    (state_dir / "restarts").mkdir()
    _write_executable(
        fake_bin / "sleep",
        """#!/usr/bin/env bash
if [[ -n "${FAKE_CRASH_LOOP_UNIT:-}" ]]; then
  path="$FAKE_SYSTEMD_STATE/restarts/$FAKE_CRASH_LOOP_UNIT"
  current=0
  [[ -f "$path" ]] && current="$(cat "$path")"
  printf '%s\n' "$((current + 1))" > "$path"
fi
""",
    )

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "HOME": str(home),
        "USER": "agent-user",
        "FAKE_SYSTEMCTL_CALLS": str(calls_path),
        "FAKE_SYSTEMD_STATE": str(state_dir),
        "FAKE_TEST_UID": str(os.getuid()),
        "FAKE_TEST_GID": str(os.getgid()),
    }
    return env, pbgui_dir, python_bin, calls_path


def _run_setup(
    env: dict[str, str],
    pbgui_dir: Path,
    python_bin: Path,
    *extra: str,
    enable: str = "monitor-agent",
) -> subprocess.CompletedProcess[str]:
    """Run setup_systemd.sh against the fake systemd manager."""

    return subprocess.run(
        [
            "bash",
            str(SETUP_SCRIPT),
            "--user",
            "agent-user",
            "--pbgui-dir",
            str(pbgui_dir),
            "--python",
            str(python_bin),
            "--enable",
            enable,
            "--no-disable-excluded",
            *extra,
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_setup_systemd_first_run_then_idempotent_second_run(tmp_path: Path) -> None:
    """A healthy second reconcile performs no writes, reloads, enables, or restarts."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    first = _run_setup(env, pbgui_dir, python_bin)
    assert first.returncode == 0, first.stderr
    assert first.stdout.rstrip().endswith("changed=true")
    first_calls = calls_path.read_text(encoding="utf-8")
    assert "--user daemon-reload" in first_calls
    assert "--user enable pbgui-monitor-agent.service" in first_calls
    assert "--user restart pbgui-monitor-agent.service" in first_calls

    unit_path = tmp_path / "home" / "agent-user" / ".config" / "systemd" / "user" / "pbgui-monitor-agent.service"
    unit_dir = unit_path.parent
    expected_scripts = {
        "pbgui-api.service": "PBApiServer.py",
        "pbgui-pbcluster.service": "PBCluster.py",
        "pbgui-pbrun.service": "PBRun.py",
        "pbgui-pbdata.service": "PBData.py",
        "pbgui-pbcoindata.service": "PBCoinData.py",
        "pbgui-monitor-agent.service": "monitor_agent.py",
    }
    for unit_name, script_name in expected_scripts.items():
        unit_source = (unit_dir / unit_name).read_text(encoding="utf-8")
        assert f"WorkingDirectory={pbgui_dir}" in unit_source
        assert f"ExecStart={python_bin} -u {pbgui_dir}/{script_name}" in unit_source
    api_source = (unit_dir / "pbgui-api.service").read_text(encoding="utf-8")
    assert f"ExecStartPre=/bin/bash {pbgui_dir}/setup/stop_legacy_api.sh --pbgui-dir {pbgui_dir}" in api_source
    first_inode = unit_path.stat().st_ino
    calls_path.write_text("", encoding="utf-8")
    second = _run_setup(env, pbgui_dir, python_bin)
    assert second.returncode == 0, second.stderr
    assert second.stdout.rstrip().endswith("changed=false")
    assert unit_path.stat().st_ino == first_inode
    second_calls = calls_path.read_text(encoding="utf-8")
    for action in ("daemon-reload", " enable ", " restart ", " start ", "reset-failed"):
        assert action not in second_calls
    assert not list(unit_path.parent.glob(".pbgui-*.tmp.*"))


def test_api_handoff_stops_only_exact_legacy_process(tmp_path: Path) -> None:
    """The API pre-start helper terminates only the exact legacy checkout process."""
    pbgui_dir = tmp_path / "pbgui"
    api_script = pbgui_dir / "PBApiServer.py"
    decoy_script = tmp_path / "other" / "PBApiServer.py"
    pidfile = pbgui_dir / "data" / "pid" / "api_server.pid"
    api_script.parent.mkdir(parents=True)
    decoy_script.parent.mkdir(parents=True)
    pidfile.parent.mkdir(parents=True)
    api_script.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    decoy_script.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")

    legacy = subprocess.Popen([sys.executable, str(api_script)])
    decoy = subprocess.Popen([sys.executable, str(decoy_script)])
    try:
        pidfile.write_text(f"{legacy.pid}\n", encoding="utf-8")

        result = subprocess.run(
            ["bash", str(API_HANDOFF_SCRIPT), "--pbgui-dir", str(pbgui_dir)],
            text=True,
            capture_output=True,
            check=False,
            timeout=40,
        )

        assert result.returncode == 0, result.stderr
        legacy.wait(timeout=5)
        assert decoy.poll() is None
        assert not pidfile.exists()
    finally:
        for process in (legacy, decoy):
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)


def test_setup_systemd_restarts_only_a_changed_requested_unit(tmp_path: Path) -> None:
    """A changed monitor-agent fragment is atomically restored and restarted."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    assert _run_setup(env, pbgui_dir, python_bin).returncode == 0
    unit_path = tmp_path / "home" / "agent-user" / ".config" / "systemd" / "user" / "pbgui-monitor-agent.service"
    unit_path.write_text(unit_path.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8")
    calls_path.write_text("", encoding="utf-8")

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip().endswith("changed=true")
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user daemon-reload" in calls
    assert "--user restart pbgui-monitor-agent.service" in calls
    assert "--user enable pbgui-monitor-agent.service" not in calls
    assert "# drift" not in unit_path.read_text(encoding="utf-8")


def test_setup_systemd_recovers_inactive_monitor_without_healthy_restart(tmp_path: Path) -> None:
    """An unchanged inactive unit is reset and started rather than restarted."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    assert _run_setup(env, pbgui_dir, python_bin).returncode == 0
    state_path = tmp_path / "systemd-state" / "active" / "pbgui-monitor-agent.service"
    state_path.write_text("inactive\n", encoding="utf-8")
    calls_path.write_text("", encoding="utf-8")

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip().endswith("changed=true")
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user reset-failed pbgui-monitor-agent.service" in calls
    assert "--user start pbgui-monitor-agent.service" in calls
    assert "--user restart pbgui-monitor-agent.service" not in calls
    assert "--user daemon-reload" not in calls


def test_setup_systemd_no_start_changes_no_runtime_state(tmp_path: Path) -> None:
    """--no-start still installs, reloads, and enables without runtime actions."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    result = _run_setup(env, pbgui_dir, python_bin, "--no-start")

    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip().endswith("changed=true")
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user daemon-reload" in calls
    assert "--user enable pbgui-monitor-agent.service" in calls
    for action in (" restart ", " start ", " stop ", "reset-failed", "is-active"):
        assert action not in calls


def test_setup_systemd_honors_manager_need_daemon_reload(tmp_path: Path) -> None:
    """Manager-reported fragment drift reloads without restarting a healthy unit."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    assert _run_setup(env, pbgui_dir, python_bin).returncode == 0
    calls_path.write_text("", encoding="utf-8")
    env = {**env, "FAKE_NEED_DAEMON_RELOAD": "yes"}

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip().endswith("changed=true")
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user show --property=NeedDaemonReload --value" in calls
    assert "--user daemon-reload" in calls
    for action in (" enable ", " restart ", " start ", "reset-failed"):
        assert action not in calls


def test_setup_systemd_rejects_unknown_enable_before_mutation(tmp_path: Path) -> None:
    """An invalid enable list fails before creating or changing systemd state."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)

    result = _run_setup(env, pbgui_dir, python_bin, enable="monitor-agent,not-a-service")

    assert result.returncode == 2
    assert "Invalid service in --enable: not-a-service" in result.stderr
    assert not (tmp_path / "home" / "agent-user" / ".config").exists()
    assert not calls_path.exists()


def test_setup_systemd_repairs_unit_mode_without_replacing_content(tmp_path: Path) -> None:
    """Matching unit content still has its required owner-only metadata repaired."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    assert _run_setup(env, pbgui_dir, python_bin).returncode == 0
    unit_path = tmp_path / "home" / "agent-user" / ".config" / "systemd" / "user" / "pbgui-monitor-agent.service"
    inode_before = unit_path.stat().st_ino
    unit_path.chmod(0o600)
    calls_path.write_text("", encoding="utf-8")

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip().endswith("changed=true")
    assert stat.S_IMODE(unit_path.stat().st_mode) == 0o644
    assert unit_path.stat().st_ino == inode_before
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user daemon-reload" in calls
    assert "--user restart pbgui-monitor-agent.service" not in calls


def test_setup_systemd_replaces_unit_symlink_with_regular_file(tmp_path: Path) -> None:
    """A managed unit symlink is atomically replaced by a regular 0644 file."""

    env, pbgui_dir, python_bin, _calls_path = _fake_systemd_environment(tmp_path)
    assert _run_setup(env, pbgui_dir, python_bin).returncode == 0
    unit_path = tmp_path / "home" / "agent-user" / ".config" / "systemd" / "user" / "pbgui-monitor-agent.service"
    symlink_target = tmp_path / "foreign.service"
    symlink_target.write_text(unit_path.read_text(encoding="utf-8"), encoding="utf-8")
    unit_path.unlink()
    unit_path.symlink_to(symlink_target)

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 0, result.stderr
    assert not unit_path.is_symlink()
    assert unit_path.is_file()
    assert stat.S_IMODE(unit_path.stat().st_mode) == 0o644


def test_setup_systemd_action_failure_prints_unit_diagnostics(tmp_path: Path) -> None:
    """A failed systemctl mutation reports unit properties and full status."""

    env, pbgui_dir, python_bin, calls_path = _fake_systemd_environment(tmp_path)
    env = {**env, "FAKE_FAIL_ACTION": "restart"}

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 1
    assert "systemctl --user restart failed for pbgui-monitor-agent.service" in result.stderr
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user show pbgui-monitor-agent.service --no-pager" in calls
    assert "--user status pbgui-monitor-agent.service --no-pager -l" in calls


def test_setup_systemd_detects_delayed_restart_increment(tmp_path: Path) -> None:
    """A unit that auto-restarts during the delayed window fails deployment."""

    env, pbgui_dir, python_bin, _calls_path = _fake_systemd_environment(tmp_path)
    env = {**env, "FAKE_CRASH_LOOP_UNIT": "pbgui-monitor-agent.service"}

    result = _run_setup(env, pbgui_dir, python_bin)

    assert result.returncode == 1
    assert "failed the 12-second stability check" in result.stderr
    assert "restarts_before=0 restarts_after=1" in result.stdout


def test_service_control_failure_prints_properties_and_full_status(tmp_path: Path) -> None:
    """Failed systemd verification emits actionable properties and full status."""

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    _write_executable(
        fake_bin / "systemctl",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_SYSTEMCTL_CALLS"
if [[ "$*" == *"show-environment"* ]]; then
  exit 0
fi
if [[ "$*" == *"is-active"* ]]; then
  exit 3
fi
if [[ "$*" == *" show "* ]]; then
  printf 'ActiveState=failed\nSubState=failed\nResult=exit-code\nExecMainStatus=1\nNRestarts=2\n'
fi
if [[ "$*" == *" status "* ]]; then
  printf 'full fake systemd status\n'
fi
exit 0
""",
    )
    _write_executable(fake_bin / "sleep", "#!/usr/bin/env bash\nexit 0\n")
    unit_dir = tmp_path / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True)
    (unit_dir / "pbgui-monitor-agent.service").write_text("[Service]\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(ROOT / "setup" / "vps_service_control.sh"), "start", "PBMonitorAgent"],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "HOME": str(tmp_path),
            "PBGUI_DIR": str(tmp_path / "custom-root" / "pbgui"),
            "FAKE_SYSTEMCTL_CALLS": str(calls_path),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Systemd stability verification failed for pbgui-monitor-agent.service" in result.stderr
    assert "ActiveState=failed" in result.stderr
    assert "ExecMainStatus=1" in result.stderr
    assert "full fake systemd status" in result.stderr
    calls = calls_path.read_text(encoding="utf-8")
    assert "--user show pbgui-monitor-agent.service --no-pager" in calls
    assert "--user status pbgui-monitor-agent.service --no-pager -l" in calls


@pytest.mark.parametrize("playbook_name", UPDATE_PLAYBOOKS)
def test_update_playbooks_unconditionally_reconcile_monitor_agent(playbook_name: str) -> None:
    """Every PBGui update reconciles monitor-agent outside update-only handlers."""

    source = (ROOT / playbook_name).read_text(encoding="utf-8")
    tasks, handlers = source.split("  handlers:", 1)
    reconcile = tasks.split("- name: Reconcile PBGui systemd user services", 1)[1]
    reconcile = reconcile.split("\n    - name:", 1)[0]
    assert "setup/setup_systemd.sh" in reconcile
    assert "monitor-agent" in tasks
    assert "--enable" in reconcile
    if playbook_name.startswith("master-"):
        assert "--no-start" in reconcile
        assert "Recover and verify monitor-agent without touching PBApiServer" in tasks
    else:
        assert "['--no-start'] if (pbgui_role | default('') | string | lower) == 'master'" in reconcile
        assert "Recover and verify master monitor-agent without touching PBApiServer" in tasks
    assert "pbgui_repo.changed" not in reconcile
    assert "pbgui_requirements.changed" not in reconcile
    assert "'changed=true' in (systemd_reconcile_result.stdout | default(''))" in reconcile
    assert "- name: Sync PBGui systemd user services" in handlers
    assert "--no-start" in handlers
    assert "'changed=true' in (systemd_setup_result.stdout | default(''))" in handlers
    assert 'listen: "restart pbgui"' in handlers
    assert "PBMonitorAgent" in handlers


def test_initial_setup_has_delayed_systemd_stability_verification() -> None:
    """Fresh VPS setup verifies all enabled core services remain stable."""

    source = (ROOT / "vps-setup.yml").read_text(encoding="utf-8")
    for expected in (
        "systemd_units:",
        "pbgui-pbcluster.service",
        "pbgui-pbrun.service",
        "pbgui-monitor-agent.service",
        "pbgui-pbcoindata.service",
        "sleep 12",
        "NRestarts",
        "ActiveState",
        "SubState",
        "Result",
        "ExecMainStatus",
        'run_systemctl status "$unit" --no-pager -l',
    ):
        assert expected in source
    assert "credential_active | default(false) | default(false, true)" in source


def test_migration_purge_and_verification_diagnostics_cover_monitor_agent() -> None:
    """Migration consumes markers and purge/diagnostics explicitly cover monitor-agent."""

    migration = (ROOT / "vps-migrate-systemd.yml").read_text(encoding="utf-8")
    purge = (ROOT / "vps-purge-install.yml").read_text(encoding="utf-8")
    service_control = (ROOT / "setup" / "vps_service_control.sh").read_text(encoding="utf-8")
    assert migration.count("'changed=true' in (systemd_") >= 2
    assert "pbgui-monitor-agent.service" in migration
    assert "credential_active | default('', true)" in migration
    assert "sleep 12" in migration
    assert "NRestarts" in migration
    assert 'run_systemctl status "$unit" --no-pager -l' in migration
    assert "str(install / 'pbgui' / 'monitor_agent.py')" in purge
    for process_pattern in ("PBData.py", "PBRemote.py", "venv_pbgui312"):
        assert process_pattern in purge
    assert "remaining_pids=" in purge
    assert "collect_targets()" in purge
    for expected in ("ActiveState", "SubState", "Result", "ExecMainStatus", "NRestarts", "status \"$unit\" --no-pager -l"):
        assert expected in service_control


def test_setup_script_uses_atomic_compare_and_preserves_enable_links() -> None:
    """Unit rendering is compare-and-replace and enabling has no manual link churn."""

    source = SETUP_SCRIPT.read_text(encoding="utf-8")
    assert 'mktemp "$unit_dir/.${unit_name}.tmp.XXXXXX"' in source
    assert 'cmp -s "$temp_path" "$unit_path"' in source
    assert 'mv -f -- "$temp_path" "$unit_path"' in source
    assert 'run_unit_action enable "$unit"' in source
    assert 'rm -f "$wants_dir/$unit"\n  run_user_systemctl enable' not in source
    assert "NeedDaemonReload" in source
    assert "printf 'changed=%s\\n' \"$CHANGED\"" in source


def test_vps_setup_only_creates_pbgui_ini_when_absent() -> None:
    """Fresh setup never replaces an existing pbgui.ini with the example."""

    source = (ROOT / "vps-setup.yml").read_text(encoding="utf-8")
    create_block = source.split("- name: create pbgui.ini from the example only when absent", 1)[1]
    create_block = create_block.split("\n    - name:", 1)[0]

    assert "remote_src: true" in create_block
    assert "force: false" in create_block
    assert "when: not pbgui_ini_stat.stat.exists" in create_block
    assert "command: cp" not in source


@pytest.mark.parametrize("payload", ["/tmp/pb7;touch /tmp/PWNED", "/tmp/$(touch PWNED)"])
def test_custom_pb7_paths_with_metacharacters_never_enter_shell_source(payload: str) -> None:
    """Custom PB7 paths remain argv data even when they contain shell metacharacters."""

    source = (ROOT / "master-update-pb.yml").read_text(encoding="utf-8")
    rendered = source.replace("{{ pb7dir }}", payload).replace("{{ pb7venv }}", payload)
    shell_blocks = re.findall(r"^      shell: \|\n((?:        .*\n)*)", rendered, flags=re.MULTILINE)

    assert shell_blocks
    assert all(payload not in block for block in shell_blocks)
    assert '          - "{{ pb7dir }}"' in source
    assert '          - "{{ pb7venv }}"' in source
    assert 'source "{{ pb7venv }}/bin/activate"' not in source
    assert 'grep "{{ pb7dir }}' not in source


@pytest.mark.parametrize(
    ("playbook_name", "variables"),
    [
        ("master-update-pb.yml", ("pbgdir", "pb7dir", "pb7venv")),
        ("vps-update-pb.yml", ("install_dir",)),
        ("vps-setup.yml", ("install_dir", "user")),
        ("vps-migrate-systemd.yml", ("pbgui_dir", "user", "credential_active")),
        ("vps-purge-install.yml", ("pbgui_dir", "install_dir", "user")),
    ],
)
def test_touched_shell_blocks_do_not_interpolate_path_or_identity_variables(
    playbook_name: str,
    variables: tuple[str, ...],
) -> None:
    """Touched shell bodies receive untrusted values through environment variables."""

    source = (ROOT / playbook_name).read_text(encoding="utf-8")
    shell_source = "\n".join(re.findall(r"^      shell: \|\n((?:        .*\n)*)", source, flags=re.MULTILINE))

    for variable in variables:
        assert f"{{{{ {variable}" not in shell_source


def _embedded_python(playbook_name: str, task_name: str) -> str:
    """Extract one Python heredoc from a deployment task for isolated execution."""

    source = (ROOT / playbook_name).read_text(encoding="utf-8")
    task = source.split(f"- name: {task_name}", 1)[1].split("\n    - name:", 1)[0]
    body = task.split("python3 - <<'PY'\n", 1)[1].split("\n        PY", 1)[0]
    return textwrap.dedent(body)


def test_authorized_keys_purge_is_locked_atomic_and_preserves_metadata(tmp_path: Path) -> None:
    """Authorized-key filtering executes with locking, atomic replace, and metadata retention."""

    home = tmp_path / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True)
    authorized_keys = ssh_dir / "authorized_keys"
    pbgui_dir = "/srv/custom;safe/pbgui"
    removed = f'command="{pbgui_dir}/cluster_sync_forced_command.sh" ssh-ed25519 AAAA pbgui-cluster:node'
    kept = "ssh-ed25519 BBBB unrelated"
    authorized_keys.write_text(f"{removed}\n{kept}\n", encoding="utf-8")
    authorized_keys.chmod(0o640)
    before = authorized_keys.stat()
    code = _embedded_python("vps-purge-install.yml", "remove Cluster Sync authorized_keys entries for this install")

    result = subprocess.run(
        ["python3", "-c", code],
        env={**os.environ, "HOME": str(home), "PBGUI_PURGE_DIR": pbgui_dir},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "changed=1 removed=1" in result.stdout
    assert authorized_keys.read_text(encoding="utf-8") == f"{kept}\n"
    after = authorized_keys.stat()
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
    assert stat.S_IMODE(after.st_mode) == 0o640
    lock_path = ssh_dir / "authorized_keys.lock"
    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
    assert not list(ssh_dir.glob(".authorized_keys.pbgui.*"))
    source = (ROOT / "vps-purge-install.yml").read_text(encoding="utf-8")
    file_lock_source = (ROOT / "file_lock.py").read_text(encoding="utf-8")
    assert 'target_path.with_name(f"{target_path.name}.lock")' in file_lock_source
    assert "path.with_name(f'{path.name}.lock')" in source
    assert ".authorized_keys.pbgui.lock" not in source
    assert "fcntl.flock(lock_fd, fcntl.LOCK_EX)" in source
    assert "tempfile.mkstemp" in source
    assert "os.replace(temp_name, path)" in source
    assert "path.write_text" not in source


@pytest.mark.parametrize(
    "relative_pattern",
    [
        "pbgui/PBData.py",
        "pbgui/PBRemote.py",
        "pbgui/monitor_agent.py",
        "venv_pbgui312/bin/python",
    ],
)
def test_purge_terminates_and_verifies_every_removed_runtime_pattern(
    tmp_path: Path,
    relative_pattern: str,
) -> None:
    """Purge terminates representative legacy daemon and venv command lines."""

    install_dir = tmp_path / "custom;install"
    install_dir.mkdir()
    marker = str(install_dir / relative_pattern)
    target = subprocess.Popen(["python3", "-c", "import time; time.sleep(60)", marker])
    code = _embedded_python("vps-purge-install.yml", "stop leftover PBGui and PB7 processes from this install")
    try:
        time.sleep(0.1)
        result = subprocess.run(
            ["python3", "-c", code],
            env={**os.environ, "PBGUI_PURGE_INSTALL_DIR": str(install_dir)},
            text=True,
            capture_output=True,
            check=False,
            timeout=15,
        )
        assert result.returncode == 0, result.stderr
        assert "changed=1" in result.stdout
        assert "remaining_pids=" not in result.stdout
        target.wait(timeout=3)
    finally:
        if target.poll() is None:
            target.kill()
            target.wait(timeout=3)
