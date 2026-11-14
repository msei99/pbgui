"""Offline PB9 layout detection without executing or modifying real runtimes."""

import configparser
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import pb9_runtime


@pytest.fixture
def layout(tmp_path):
    """Create source, environment and inert native/entry-point artifacts."""
    source = tmp_path / "pb9"
    schema = source / "src" / "config" / "schema.py"
    schema.parent.mkdir(parents=True)
    marker = tmp_path / "executed"
    schema.write_text(
        f'CONFIG_SCHEMA_VERSION = "v9.0.0"\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\n',
        encoding="utf-8",
    )
    (source / "src" / "passivbot_version.py").write_text('__version__ = "8.2.0.dev0"\n', encoding="utf-8")
    venv = tmp_path / "venv_pb9"
    python = venv / "bin" / "python"
    python.parent.mkdir(parents=True)
    for path in (python, python.parent / "passivbot"):
        path.write_text(f'#!/bin/sh\ntouch "{marker}"\n', encoding="utf-8")
        path.chmod(0o700)
    cfg = venv / "pyvenv.cfg"
    cfg.write_text("version = 3.12.3\n", encoding="utf-8")
    rust = venv / "lib" / "python3.12" / "site-packages" / "passivbot_rust" / "passivbot_rust.abi3.so"
    rust.parent.mkdir(parents=True)
    rust.write_bytes(b"not a loadable binary")
    return SimpleNamespace(source=source, venv=venv, python=python, cli=python.parent / "passivbot",
                           cfg=cfg, schema=schema, rust=rust, marker=marker)


def test_detects_layout_without_claiming_ready_or_requiring_stamp(layout):
    """Missing commit/stamp is not a readiness failure for this static detector."""
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is True
    assert result["errors"] == []
    assert result["config_schema"] == "v9.0.0"
    assert result["package_version"] == "8.2.0.dev0"
    assert result["commit"] is None
    assert result["warning_codes"] == ["commit_unavailable"]
    assert result["rust_exists"] is True
    assert result["rust_stamp_present"] is False
    assert "ready" not in result


def test_reports_fixture_commit_and_optional_stamp(layout):
    """A committed source reports real provenance without importing its Python."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"})
    for args in (("init", "--quiet"), ("add", "src"),
                 ("-c", "user.name=PBGui Test", "-c", "user.email=test@example.invalid",
                  "commit", "--quiet", "-m", "fixture")):
        subprocess.run(["git", "-C", str(layout.source), *args], env=env, check=True,
                       capture_output=True, timeout=5)
    expected = subprocess.run(["git", "-C", str(layout.source), "rev-parse", "HEAD"],
                              env=env, check=True, capture_output=True, text=True, timeout=5).stdout.strip()
    layout.rust.with_name(layout.rust.name + ".rust-src-sha256").write_text("unverified\n", encoding="utf-8")
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is True
    assert result["commit"] == expected
    assert result["commit_available"] is True
    assert result["warnings"] == []
    assert result["rust_stamp_present"] is True
    assert not layout.marker.exists()


def test_v8_source_is_rejected_despite_identical_package_version(layout):
    """Package version 8.2.0.dev0 cannot make a v8 schema into PB9."""
    layout.schema.write_text('CONFIG_SCHEMA_VERSION = "v8.6.0"\n', encoding="utf-8")
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is False
    assert result["error_codes"] == ["source_invalid"]


@pytest.mark.parametrize("missing", ["schema", "version"])
def test_source_read_errors_are_blocking(layout, missing):
    """Unreadable source declarations cannot be downgraded to Git warnings."""
    (layout.schema if missing == "schema" else layout.source / "src" / "passivbot_version.py").unlink()
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is False
    assert "source_invalid" in result["error_codes"]
    assert result["source_diagnostics"]


@pytest.mark.parametrize("slot", ["pb7", "pb8"])
@pytest.mark.parametrize("relationship", ["same", "child", "parent", "alias"])
def test_source_trees_cannot_overlap(layout, tmp_path, slot, relationship):
    """Source ownership cannot overlap a configured existing bot slot."""
    other = {"same": layout.source, "child": layout.source / "child", "parent": tmp_path}.get(relationship)
    if relationship == "alias":
        other = tmp_path / "alias"
        other.symlink_to(layout.source, target_is_directory=True)
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python, {slot + "dir": other})
    assert result["detected"] is False
    assert "source_overlap" in result["error_codes"]


@pytest.mark.parametrize("slot", ["pb7", "pb8"])
def test_shared_environment_is_rejected_without_resolving_python(layout, slot):
    """Different executable filenames in the same environment remain shared."""
    result = pb9_runtime.inspect_pb9_runtime(
        layout.source, layout.python, {slot + "venv": layout.python.with_name("python3")})
    assert result["detected"] is False
    assert "venv_shared" in result["error_codes"]


def test_separate_environments_may_share_system_python_symlink(layout, tmp_path):
    """Resolving the executable itself would falsely merge these distinct venvs."""
    layout.python.unlink()
    layout.python.symlink_to(sys.executable)
    other = tmp_path / "venv_pb8" / "bin" / "python"
    other.parent.mkdir(parents=True)
    other.symlink_to(sys.executable)
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python, {"pb8venv": other})
    assert result["detected"] is True
    assert result["errors"] == []


def test_pbgui_environment_cannot_be_reused(layout, monkeypatch):
    """The running GUI's environment is reserved independently of INI values."""
    monkeypatch.setattr(pb9_runtime.sys, "prefix", str(layout.venv))
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is False
    assert "venv_shared" in result["error_codes"]


@pytest.mark.parametrize(("source", "python"), [(None, None), ("", ""), (" ", " ")])
def test_unconfigured_slots_never_probe_files(source, python, monkeypatch):
    """Absent settings return not-configured before any filesystem operation."""
    def forbidden(*args, **kwargs):
        """Reject probes even if cwd happens to contain a bot checkout."""
        pytest.fail("unconfigured slot accessed the filesystem")

    for name in ("resolve", "is_file", "is_dir", "open"):
        monkeypatch.setattr(Path, name, forbidden)
    result = pb9_runtime.inspect_pb9_runtime(source, python)
    assert result["configured"] is False
    assert result["detected"] is False
    assert result["errors"] == []


@pytest.mark.parametrize(("part", "code"), [
    ("python", "python_missing"), ("cli", "cli_missing"),
    ("rust", "rust_missing"), ("cfg", "python_metadata_invalid"),
])
def test_missing_artifacts_have_separate_diagnostics(layout, part, code):
    """Each required artifact produces a distinct failure without execution."""
    getattr(layout, part).unlink()
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is False
    assert code in result["error_codes"]


@pytest.mark.parametrize("part", ["python", "cli"])
def test_entry_points_must_be_executable(layout, part):
    """Presence alone does not satisfy entry-point permissions."""
    getattr(layout, part).chmod(0o600)
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["detected"] is False
    assert ("python_not_executable" if part == "python" else "cli_not_executable") in result["error_codes"]


@pytest.mark.parametrize(("version", "supported"), [
    ("3.12.3", True), ("3.14.0", True), ("3.11.9", False),
    ("3.13.0", False), ("3.15.0", False),
])
def test_python_version_comes_from_static_venv_metadata(layout, version, supported):
    """Support follows the v9 Python range, excluding Python 3.13."""
    layout.cfg.write_text(f"version = {version}\n", encoding="utf-8")
    result = pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)
    assert result["python_version"] == version
    assert result["detected"] is supported
    assert ("python_version_unsupported" in result["error_codes"]) is not supported


def test_inspection_never_executes_source_interpreter_or_cli(layout, monkeypatch):
    """Marker source and executable files stay inert throughout inspection."""
    def forbidden(*args, **kwargs):
        """This fixture has no Git, so no subprocess of any kind is necessary."""
        pytest.fail("inspection launched a process")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    assert pb9_runtime.inspect_pb9_runtime(layout.source, layout.python)["detected"] is True
    assert not layout.marker.exists()


def test_wrapper_reads_only_allowlisted_paths_from_one_snapshot(layout, monkeypatch):
    """The INI wrapper never requests credentials or serializes other settings."""
    import pbgui_purefunc

    calls = []
    values = {"pb9dir": str(layout.source), "pb9venv": str(layout.python)}

    def get(section, key):
        """Reject any access beyond the six non-secret path settings."""
        assert section == "main"
        assert key in {"pb9dir", "pb9venv", "pb7dir", "pb7venv", "pb8dir", "pb8venv"}
        calls.append(key)
        return values[key]

    snapshot = SimpleNamespace(get=get, has_option=lambda section, key: key in values)
    monkeypatch.setattr(pbgui_purefunc, "load_ini_snapshot", lambda: snapshot)
    result = pb9_runtime.pb9_runtime_status()
    assert result["detected"] is True
    assert calls == ["pb9dir", "pb9venv"]


def test_v9_ini_keys_leave_pb8_status_unchanged(layout, tmp_path, monkeypatch):
    """Existing PB8 inspection ignores the new, unconnected INI keys."""
    import pbgui_purefunc

    layout.schema.write_text('CONFIG_SCHEMA_VERSION = "v8.6.0"\n', encoding="utf-8")
    layout.rust.with_name(layout.rust.name + ".rust-src-sha256").write_text("stamp\n", encoding="utf-8")
    parser = configparser.ConfigParser(interpolation=None)
    parser["main"] = {"pb8dir": str(layout.source), "pb8venv": str(layout.python)}
    snapshot = SimpleNamespace(get=parser.get, has_option=parser.has_option)
    monkeypatch.setattr(pbgui_purefunc, "load_ini_snapshot", lambda: snapshot)
    monkeypatch.setattr(pbgui_purefunc, "PBGDIR", tmp_path / "gui")
    before = pbgui_purefunc.pb8_runtime_status()
    parser.set("main", "pb9dir", str(tmp_path / "other_pb9"))
    parser.set("main", "pb9venv", str(tmp_path / "other_venv" / "bin" / "python"))
    assert pbgui_purefunc.pb8_runtime_status() == before
    assert before["ready"] is True
