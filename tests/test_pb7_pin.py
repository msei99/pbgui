"""Regression tests for the fail-closed PB7 revision pin."""

from pathlib import Path
import signal
import subprocess

import pytest

import pb7_guard
from setup.installer import core


PINNED_COMMIT = "befaa9b7aa89e00ee55704221b39621ad700ac36"
UPDATE_PLAYBOOKS = (
    "master-update-pb.yml",
    "master-update-pb7.yml",
    "master-update-pbonly.yml",
    "vps-update-pb.yml",
    "vps-update-pb7.yml",
    "vps-pb7-python312.yml",
)
SWITCH_PLAYBOOKS = (
    "master-switch-pb7-branch.yml",
    "vps-switch-pb7-branch.yml",
)
REBUILD_PLAYBOOKS = UPDATE_PLAYBOOKS[:-1] + SWITCH_PLAYBOOKS


def _git(repo: Path, *args: str) -> str:
    """Run a deterministic Git command for an isolated test repository."""
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def test_pb7_pin_is_the_known_v7_fleet_commit() -> None:
    """The default pin remains an exact, deliberately reviewed V7 commit."""
    assert pb7_guard.PB7_PINNED_COMMIT == PINNED_COMMIT


@pytest.mark.parametrize("value", ["", "master", "ABCDEF", "a" * 39, "g" * 40])
def test_load_pb7_pinned_commit_rejects_non_commit_values(tmp_path: Path, value: str) -> None:
    """Branch names, abbreviated hashes, and malformed hashes fail closed."""
    pin_path = tmp_path / "pb7_ref.txt"
    pin_path.write_text(value + "\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="lowercase 40-character commit SHA"):
        pb7_guard.load_pb7_pinned_commit(pin_path)


@pytest.mark.parametrize(
    ("version", "expected_major"),
    [("7.12.0", 7), ("8.0.0", 8)],
)
def test_verify_passivbot_major_accepts_matching_checkout(
    tmp_path: Path,
    version: str,
    expected_major: int,
) -> None:
    """A checkout passes only when its declared major matches the request."""
    version_path = tmp_path / "src" / "passivbot_version.py"
    version_path.parent.mkdir()
    version_path.write_text(f'__version__ = "{version}"\n', encoding="utf-8")

    assert pb7_guard.verify_passivbot_major(tmp_path, expected_major) == version


def test_verify_passivbot_major_rejects_v8_checkout(tmp_path: Path) -> None:
    """A PB8 source tree cannot pass the PB7 guard."""
    version_path = tmp_path / "src" / "passivbot_version.py"
    version_path.parent.mkdir()
    version_path.write_text('__version__ = "8.0.0"\n', encoding="utf-8")

    with pytest.raises(RuntimeError, match="expected Passivbot v7, found v8.0.0"):
        pb7_guard.verify_passivbot_major(tmp_path, 7)


def test_verify_passivbot_major_ignores_commented_spoof(tmp_path: Path) -> None:
    """A commented V7-looking assignment cannot hide a real PB8 declaration."""
    version_path = tmp_path / "src" / "passivbot_version.py"
    version_path.parent.mkdir()
    version_path.write_text(
        '# __version__ = "7.12.0"\n__version__ = "8.0.0"\n',
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="expected Passivbot v7, found v8.0.0"):
        pb7_guard.verify_passivbot_major(tmp_path, 7)


def test_ensure_git_checkout_uses_detached_revision_for_existing_repo(tmp_path: Path, monkeypatch) -> None:
    """Existing PB7 checkouts fetch and detach at the exact pinned commit."""
    target = tmp_path / "pb7"
    (target / ".git").mkdir(parents=True)
    calls: list[list[str]] = []
    fetches: list[tuple[Path, str]] = []

    def fake_run(args, log, **kwargs):
        """Capture installer Git commands without touching a real repository."""
        del log, kwargs
        calls.append([str(arg) for arg in args])
        return ""

    monkeypatch.setattr(core, "_run_command", fake_run)
    monkeypatch.setattr(
        core,
        "fetch_passivbot_ref",
        lambda repo, url, **_kwargs: fetches.append((Path(repo), url)),
    )
    monkeypatch.setattr(core, "verify_passivbot_major", lambda *_args, **_kwargs: "7.12.0")
    core._ensure_git_checkout(
        "https://github.com/enarjord/passivbot.git",
        target,
        lambda _message: None,
        revision=PINNED_COMMIT,
        expected_major=7,
    )

    assert fetches == [(target, "https://github.com/enarjord/passivbot.git")]
    assert calls == [["git", "checkout", "--detach", PINNED_COMMIT]]


def test_official_fetch_unshallows_without_replacing_custom_origin(tmp_path: Path) -> None:
    """The pin fetch reaches old commits while preserving a configured origin."""
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    _git(upstream, "init", "-b", "master")
    _git(upstream, "config", "user.email", "tests@example.invalid")
    _git(upstream, "config", "user.name", "PBGui Tests")
    version_path = upstream / "src" / "passivbot_version.py"
    version_path.parent.mkdir()
    version_path.write_text('__version__ = "7.12.0"\n', encoding="utf-8")
    _git(upstream, "add", ".")
    _git(upstream, "commit", "-m", "PB7")
    pinned = _git(upstream, "rev-parse", "HEAD")
    version_path.write_text('__version__ = "8.0.0"\n', encoding="utf-8")
    _git(upstream, "commit", "-am", "PB8")

    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "clone", "--depth", "1", f"file://{upstream}", str(checkout)],
        check=True,
        capture_output=True,
        text=True,
    )
    custom_origin = "https://example.invalid/custom-passivbot.git"
    _git(checkout, "remote", "set-url", "origin", custom_origin)

    pb7_guard.fetch_passivbot_ref(checkout, f"file://{upstream}")

    assert _git(checkout, "remote", "get-url", "origin") == custom_origin
    assert pb7_guard.verify_passivbot_major(checkout, 7, ref=pinned) == "7.12.0"
    assert pb7_guard.checkout_passivbot_ref(checkout, pinned) is True
    assert pb7_guard.verify_passivbot_major(checkout, 7) == "7.12.0"
    assert pb7_guard.checkout_passivbot_ref(checkout, pinned) is False
    version_checkout = checkout / "src" / "passivbot_version.py"
    version_checkout.write_text('__version__ = "7.12.0-local"\n', encoding="utf-8")
    assert pb7_guard.passivbot_ref_differs(checkout, pinned) is True
    assert pb7_guard.checkout_passivbot_ref(checkout, pinned) is True
    assert version_checkout.read_text(encoding="utf-8") == '__version__ = "7.12.0"\n'


def test_stop_passivbot_processes_matches_only_selected_runtime(tmp_path: Path, monkeypatch) -> None:
    """Installer cleanup targets only the selected checkout and virtualenv."""
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    repo = tmp_path / "pb7"
    venv = tmp_path / "venv_pb7"
    commands = {
        101: [venv / "bin" / "python", repo / "src" / "main.py"],
        102: [Path("/usr/bin/python3"), repo / "src" / "backtest.py"],
        103: [Path("/usr/bin/python3"), tmp_path / "pb8" / "src" / "main.py"],
        104: [Path("/usr/bin/vim"), repo / "src" / "main.py"],
    }
    for pid, argv in commands.items():
        process_dir = proc_root / str(pid)
        process_dir.mkdir()
        (process_dir / "cmdline").write_bytes(
            b"\0".join(str(item).encode() for item in argv) + b"\0"
        )
        (process_dir / "stat").write_text(
            f"{pid} (python) S " + " ".join(["0"] * 18 + [str(pid * 10)]) + "\n",
            encoding="utf-8",
        )
    signals: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(pb7_guard.os, "kill", lambda pid, sig: signals.append((pid, sig)))

    stopped = pb7_guard.stop_passivbot_processes(repo, venv, proc_root=proc_root, timeout=0)

    assert stopped == [101, 102]
    assert (101, signal.SIGTERM) in signals
    assert (102, signal.SIGTERM) in signals
    assert not any(pid == 103 for pid, _sig in signals)
    assert not any(pid == 104 for pid, _sig in signals)


def test_stop_passivbot_processes_matches_symlinked_runtime_paths(tmp_path: Path, monkeypatch) -> None:
    """Processes remain stoppable when the configured install parent is a symlink."""
    real_root = tmp_path / "real"
    real_root.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(real_root, target_is_directory=True)
    repo = linked_root / "pb7"
    venv = linked_root / "venv_pb7"
    (real_root / "pb7" / "src").mkdir(parents=True)
    (real_root / "venv_pb7" / "bin").mkdir(parents=True)
    proc_root = tmp_path / "proc"
    process_dir = proc_root / "201"
    process_dir.mkdir(parents=True)
    (process_dir / "cmdline").write_bytes(
        f"{venv}/bin/python\0{repo}/src/main.py\0".encode()
    )
    (process_dir / "stat").write_text(
        "201 (python) S " + " ".join(["0"] * 18 + ["2010"]) + "\n",
        encoding="utf-8",
    )
    signals: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(pb7_guard.os, "kill", lambda pid, sig: signals.append((pid, sig)))

    stopped = pb7_guard.stop_passivbot_processes(repo, venv, proc_root=proc_root, timeout=0)

    assert stopped == [201]
    assert (201, signal.SIGTERM) in signals


@pytest.mark.parametrize("playbook_path", UPDATE_PLAYBOOKS)
def test_pb7_update_playbooks_preflight_and_use_exact_pin(playbook_path: str) -> None:
    """Every PB7 updater verifies v7 before checking out the shared pin."""
    source = Path(playbook_path).read_text(encoding="utf-8")

    assert "lookup('file', playbook_dir + '/pb7_ref.txt')" in source
    assert "pb7_guard.py" in source
    assert "--ref" in source
    assert "--expected-major" in source
    assert "--fetch-url" in source
    assert "--checkout" in source
    assert "current_pb7_branch" not in source
    if playbook_path == "vps-pb7-python312.yml":
        preflight_index = source.index("Verify pinned commit is Passivbot v7 before stopping services")
        stop_index = source.index("name: Stop PBRun")
        checkout_index = source.index("checkout verified pinned PB7 commit")
    else:
        preflight_index = source.index("Fetch and verify pinned Passivbot v7")
        stop_index = source.index("Stop PBRun before changing PB7 checkout")
        checkout_index = source.index("Checkout verified pinned Passivbot v7")
    assert preflight_index < stop_index < checkout_index


@pytest.mark.parametrize("playbook_path", SWITCH_PLAYBOOKS)
def test_pb7_branch_switches_guard_target_before_checkout(playbook_path: str) -> None:
    """Advanced branch switching cannot check out a PB8 ref into the PB7 slot."""
    source = Path(playbook_path).read_text(encoding="utf-8")
    guard_index = source.index("Verify selected commit is Passivbot v7 before checkout")
    checkout_index = source.index("Checkout target branch directly at verified commit")

    assert "pb7_guard.py" in source
    assert "--expected-major" in source
    assert "Resolve selected PB7 ref to one immutable commit" in source
    assert 'git checkout -B "{{ target_branch }}" "{{ resolved_commit.stdout }}"' in source
    assert 'git checkout "{{ target_branch }}"' not in source
    stop_index = source.index("Stop PBRun before changing PB7 checkout")
    assert guard_index < stop_index < checkout_index


@pytest.mark.parametrize("playbook_path", REBUILD_PLAYBOOKS)
def test_pb7_rebuilds_stop_supervisor_and_processes_before_dependencies(playbook_path: str) -> None:
    """PBRun and PB7 children stop before requirements or Rust are replaced."""
    source = Path(playbook_path).read_text(encoding="utf-8")
    stop_index = source.index("Stop PBRun before PB7 rebuild")
    kill_index = source.index("name: kill all pb7 processes", stop_index)
    install_index = source.index("name: Install pb7 requirements", kill_index)
    build_index = source.index("name: Build passivbot-rust", install_index)

    assert stop_index < kill_index < install_index < build_index


def test_fresh_install_paths_use_the_pb7_pin_and_guard() -> None:
    """Fresh local, remote, legacy, and VPS installs cannot follow master."""
    core_source = Path("setup/installer/core.py").read_text(encoding="utf-8")
    remote_source = Path("setup/installer/scripts/remote_master_bootstrap.sh").read_text(encoding="utf-8")
    legacy_source = Path("install.sh").read_text(encoding="utf-8")
    vps_source = Path("vps-setup.yml").read_text(encoding="utf-8")

    assert "revision=PB7_PINNED_COMMIT" in core_source
    assert 'payload["pb7_ref"] = PB7_PINNED_COMMIT' in core_source
    assert "--fetch-url https://github.com/enarjord/passivbot.git --checkout" in remote_source
    assert "pb7_guard.py" in remote_source
    assert f'PB7_REF="{PINNED_COMMIT}"' in legacy_source
    assert "pb7_ref.txt" not in legacy_source
    assert 'git -C pb7 checkout --detach "$PB7_REF"' in legacy_source
    assert "Clone Passivbot without checking out upstream master" in vps_source
    assert "Fetch, verify and checkout pinned Passivbot v7" in vps_source
    assert "--fetch-url" in vps_source
