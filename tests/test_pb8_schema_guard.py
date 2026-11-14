"""Offline checks that v9 refs cannot enter a PB8 installation slot."""

import os
from pathlib import Path
import subprocess
import sys

import pytest

import pb7_guard
from setup.installer import core


@pytest.fixture
def git_env():
    """Use no inherited Git repository or user/system configuration in fixtures."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"})
    return env


def _git(repo, env, *args):
    """Run bounded Git commands only within an isolated fixture repository."""
    return subprocess.run(
        ["git", "-C", str(repo), *args], env=env, check=True,
        capture_output=True, text=True, timeout=5,
    ).stdout.strip()


@pytest.fixture
def checkout(tmp_path, git_env, monkeypatch):
    """Create v8/v9 refs sharing a package version, with v8 checked out."""
    for key in list(os.environ):
        if key.startswith("GIT_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    root = tmp_path / "repo"
    schema = root / "src" / "config" / "schema.py"
    schema.parent.mkdir(parents=True)
    (root / "src" / "passivbot_version.py").write_text(
        '__version__ = "8.2.0.dev0"\n', encoding="utf-8",
    )
    _git(root, git_env, "init", "--quiet")
    _git(root, git_env, "config", "user.name", "PBGui Test")
    _git(root, git_env, "config", "user.email", "test@example.invalid")
    for ref, version in (("v8", "v8.6.0"), ("v9", "v9.0.0")):
        schema.write_text(f'CONFIG_SCHEMA_VERSION = "{version}"\n', encoding="utf-8")
        _git(root, git_env, "add", "src")
        _git(root, git_env, "commit", "--quiet", "-m", ref)
        _git(root, git_env, "tag", ref)
    _git(root, git_env, "checkout", "--detach", "v8")
    return root


def test_v9_ref_is_rejected_even_with_v8_package_version(checkout):
    """The schema in the requested ref, not the checked-out tree, decides."""
    with pytest.raises(RuntimeError, match="expected Passivbot v8 config schema, found v9.0.0"):
        pb7_guard.verify_passivbot_major(checkout, 8, ref="v9")


def test_cli_rejects_v9_before_stopping_or_checking_out(checkout, git_env, monkeypatch, capsys):
    """A failed preflight cannot stop processes or change HEAD/the worktree."""
    before_head = _git(checkout, git_env, "rev-parse", "HEAD")
    before_files = {path: path.read_bytes() for path in (checkout / "src").rglob("*.py")}

    def forbidden(*args, **kwargs):
        """Fail if rejection is delayed until after a destructive step."""
        pytest.fail("stop/checkout was called before schema rejection")

    monkeypatch.setattr(pb7_guard, "stop_passivbot_processes", forbidden)
    monkeypatch.setattr(pb7_guard, "checkout_passivbot_ref", forbidden)
    monkeypatch.setattr(sys, "argv", [
        "pb7_guard.py", "--repo", str(checkout), "--ref", "v9",
        "--expected-major", "8", "--checkout", "--stop-processes",
        "--venv", str(checkout.parent / "venv"),
    ])
    assert pb7_guard.main() == 1
    output = capsys.readouterr()
    assert "found v9.0.0" in output.err
    assert output.out == ""
    assert _git(checkout, git_env, "rev-parse", "HEAD") == before_head
    assert _git(checkout, git_env, "status", "--porcelain") == ""
    assert {path: path.read_bytes() for path in before_files} == before_files


def test_standalone_guard_can_import_helpers_and_reject_v9(checkout, git_env):
    """The script entry point works outside PBGui's cwd without extra packages."""
    proc = subprocess.run(
        [sys.executable, str(Path(pb7_guard.__file__).resolve()), "--repo", str(checkout),
         "--ref", "v9", "--expected-major", "8", "--checkout"],
        cwd=checkout.parent, env=git_env, capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode == 1
    assert "expected Passivbot v8 config schema, found v9.0.0" in proc.stderr
    assert _git(checkout, git_env, "rev-parse", "HEAD") == _git(checkout, git_env, "rev-parse", "v8")


def test_installer_rejects_v9_before_checkout(checkout, git_env, monkeypatch):
    """The existing installer preflight uses the strengthened guard before mutation."""
    target_ref = "refs/remotes/pbgui-pb7-pin/v9"
    _git(checkout, git_env, "update-ref", target_ref, "v9")

    def forbidden(*args, **kwargs):
        """Fail if the installer reaches its checkout command."""
        pytest.fail("installer checkout ran before schema rejection")

    monkeypatch.setattr(core, "fetch_passivbot_ref", lambda *args, **kwargs: None)
    monkeypatch.setattr(core, "_run_command", forbidden)
    with pytest.raises(RuntimeError, match="found v9.0.0"):
        core._ensure_git_checkout(
            "https://github.com/enarjord/passivbot.git", checkout, lambda _message: None,
            branch="v9", expected_major=8,
        )
    assert _git(checkout, git_env, "rev-parse", "HEAD") == _git(checkout, git_env, "rev-parse", "v8")
    assert _git(checkout, git_env, "status", "--porcelain") == ""


@pytest.mark.parametrize("schema", ["v8.0.0", "v8.6.0"])
@pytest.mark.parametrize("use_ref", [False, True])
def test_released_pb8_schemas_preserve_return_value(checkout, git_env, schema, use_ref):
    """Supported v8 schema majors preserve the declared package string."""
    (checkout / "src" / "config" / "schema.py").write_text(
        f'CONFIG_SCHEMA_VERSION = "{schema}"\n', encoding="utf-8",
    )
    if use_ref:
        _git(checkout, git_env, "add", "src")
        _git(checkout, git_env, "commit", "--quiet", "--allow-empty", "-m", "schema")
    assert pb7_guard.verify_passivbot_major(checkout, 8, ref="HEAD" if use_ref else None) == "8.2.0.dev0"


def test_v8_cli_preserves_checkout_output(checkout, monkeypatch, capsys):
    """Existing automation still receives both unchanged checkout markers."""
    monkeypatch.setattr(sys, "argv", [
        "pb7_guard.py", "--repo", str(checkout), "--ref", "v8",
        "--expected-major", "8", "--checkout",
    ])
    assert pb7_guard.main() == 0
    assert capsys.readouterr().out == (
        "Passivbot v8.2.0.dev0 verified.\ncheckout_required=false\ncheckout_changed=false\n"
    )


@pytest.mark.parametrize("source", [
    None, 'CONFIG_SCHEMA_VERSION = str("v8.6.0")\n',
    'CONFIG_SCHEMA_VERSION = "v8.0.0"\nCONFIG_SCHEMA_VERSION = "v8.6.0"\n',
    'CONFIG_SCHEMA_VERSION = "v10.0.0"\n', "CONFIG_SCHEMA_VERSION = (\n",
])
@pytest.mark.parametrize("use_ref", [False, True])
def test_invalid_schema_fails_closed_for_worktree_and_ref(checkout, git_env, source, use_ref):
    """A missing, computed, ambiguous or unknown schema cannot use package fallback."""
    schema = checkout / "src" / "config" / "schema.py"
    if source is None:
        schema.unlink()
    else:
        schema.write_text(source, encoding="utf-8")
    if use_ref:
        _git(checkout, git_env, "add", "-u")
        _git(checkout, git_env, "commit", "--quiet", "-m", "invalid schema")
    with pytest.raises(RuntimeError):
        pb7_guard.verify_passivbot_major(checkout, 8, ref="HEAD" if use_ref else None)


@pytest.mark.parametrize("schema", [None, "v7.12.0"])
def test_pb7_never_requires_a_config_schema(tmp_path, schema):
    """Historical PB7 trees remain valid whether or not schema metadata exists."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "passivbot_version.py").write_text('__version__ = "7.12.0"\n', encoding="utf-8")
    if schema is not None:
        (src / "config").mkdir()
        (src / "config" / "schema.py").write_text(
            f'CONFIG_SCHEMA_VERSION = "{schema}"\n', encoding="utf-8",
        )
    assert pb7_guard.verify_passivbot_major(tmp_path, 7) == "7.12.0"


def test_schema_source_in_ref_is_never_executed(checkout, git_env, tmp_path):
    """A literal is inspected without executing surrounding Python code."""
    marker = tmp_path / "executed"
    (checkout / "src" / "config" / "schema.py").write_text(
        f'CONFIG_SCHEMA_VERSION = "v8.6.0"\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\n',
        encoding="utf-8",
    )
    _git(checkout, git_env, "commit", "--quiet", "-am", "side effect")
    assert pb7_guard.verify_passivbot_major(checkout, 8, ref="HEAD") == "8.2.0.dev0"
    assert not marker.exists()


def test_worktree_schema_cannot_escape_source_root(checkout, tmp_path):
    """New schema reads must not follow a symlink into an unrelated directory."""
    external = tmp_path / "external.py"
    external.write_text('CONFIG_SCHEMA_VERSION = "v8.6.0"\n', encoding="utf-8")
    schema = checkout / "src" / "config" / "schema.py"
    schema.unlink()
    schema.symlink_to(external)
    with pytest.raises(RuntimeError, match="Could not read Passivbot schema file"):
        pb7_guard.verify_passivbot_major(checkout, 8)


def test_v9_target_is_not_enabled_by_schema_guard(checkout):
    """Expected major 9 remains blocked by the existing package-version rule."""
    with pytest.raises(RuntimeError, match="expected Passivbot v9, found v8.2.0.dev0"):
        pb7_guard.verify_passivbot_major(checkout, 9, ref="v9")
