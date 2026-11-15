"""Offline source classification tests using isolated Passivbot fixture trees."""

import os
from pathlib import Path
import subprocess

import pytest

import passivbot_runtime
from passivbot_runtime import classify_passivbot_source


@pytest.mark.parametrize("source", [
    '\ufeffCONFIG_SCHEMA_VERSION = "v9.0.0"\n',
    b'\xef\xbb\xbfCONFIG_SCHEMA_VERSION = "v9.0.0"\r\n',
])
def test_literal_reader_accepts_utf8_bom(source):
    """Both decoded and byte source may carry the conventional UTF-8 BOM."""
    assert passivbot_runtime.read_source_string_literal(source, "CONFIG_SCHEMA_VERSION") == "v9.0.0"


@pytest.mark.parametrize("source", [
    b"#" + b"x" * passivbot_runtime.MAX_SOURCE_BYTES,
    "#" + "\u00e9" * (passivbot_runtime.MAX_SOURCE_BYTES // 2),
])
def test_literal_reader_rejects_oversized_bytes_before_ast(source, monkeypatch):
    """The size gate measures UTF-8 bytes and runs before AST allocation."""
    def forbidden(*args, **kwargs):
        """Ensure oversized input never reaches ast.parse."""
        pytest.fail("oversized source was parsed")

    monkeypatch.setattr(passivbot_runtime.ast, "parse", forbidden)
    with pytest.raises(ValueError, match="exceeds the 256 KiB source limit"):
        passivbot_runtime.read_source_string_literal(source, "CONFIG_SCHEMA_VERSION")


def test_literal_reader_accepts_exact_size_limit():
    """A source exactly at the byte limit remains parseable."""
    literal = b'CONFIG_SCHEMA_VERSION = "v8.6.0"\n#'
    source = literal + b"x" * (passivbot_runtime.MAX_SOURCE_BYTES - len(literal))
    assert passivbot_runtime.read_source_string_literal(source, "CONFIG_SCHEMA_VERSION") == "v8.6.0"


@pytest.fixture
def source_tree(tmp_path: Path) -> Path:
    """Create a minimal source tree without reading a real bot installation."""
    root = tmp_path / "passivbot"
    (root / "src" / "config").mkdir(parents=True)
    (root / "src" / "passivbot_version.py").write_text(
        '__version__ = "8.2.0.dev0"\n', encoding="utf-8",
    )
    (root / "src" / "config" / "schema.py").write_text(
        'CONFIG_SCHEMA_VERSION = "v8.6.0"\n', encoding="utf-8",
    )
    return root


@pytest.fixture
def git_env():
    """Prevent fixture commands from using inherited repository/global settings."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"})
    return env


def _git(repo, env, *args):
    """Run bounded Git commands exclusively against temporary fixture repositories."""
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True,
        text=True, timeout=5, env=env,
    ).stdout


@pytest.fixture
def committed_source(source_tree, git_env):
    """Commit fixture metadata without consulting user identity, signing or hooks."""
    _git(source_tree, git_env, "init", "--quiet")
    _git(source_tree, git_env, "add", "src")
    _git(source_tree, git_env, "-c", "user.name=PBGui Test",
         "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "fixture")
    return _git(source_tree, git_env, "rev-parse", "HEAD").strip()


@pytest.mark.parametrize(("schema", "generation"), [("v8.6.0", "pb8"), ("v9.0.0", "pb9")])
def test_generation_uses_schema_with_identical_package_versions(source_tree, schema, generation):
    """Identical development package versions cannot collapse v9 into PB8."""
    (source_tree / "src" / "config" / "schema.py").write_text(
        f'CONFIG_SCHEMA_VERSION = "{schema}"\n', encoding="utf-8",
    )
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == generation
    assert result["package_version"] == "8.2.0.dev0"
    assert result["config_schema"] == schema
    assert result["commit"] is None
    assert any("Git checkout" in error for error in result["errors"])


@pytest.mark.parametrize("source", [
    None, "", 'CONFIG_SCHEMA_VERSION = str("v9.0.0")\n',
    'CONFIG_SCHEMA_VERSION = "v10.0.0"\n', 'CONFIG_SCHEMA_VERSION = "v7.0.0"\n',
    'CONFIG_SCHEMA_VERSION = "v9"\n', 'CONFIG_SCHEMA_VERSION = "9.0.0"\n',
    'CONFIG_SCHEMA_VERSION = "v9.0.0.dev0"\n', 'CONFIG_SCHEMA_VERSION = 9\n',
    'CONFIG_SCHEMA_VERSION = "v9.0.0"\nCONFIG_SCHEMA_VERSION = "v8.6.0"\n',
    'CONFIG_SCHEMA_VERSION = "v9.0.0"\nif True:\n    CONFIG_SCHEMA_VERSION = "v8.6.0"\n',
    'CONFIG_SCHEMA_VERSION = other = "v9.0.0"\n',
    'if True:\n    CONFIG_SCHEMA_VERSION = "v9.0.0"\n',
    'CONFIG_SCHEMA_VERSION = (\n',
])
def test_unknown_schema_never_falls_back_to_package_version(source_tree, source):
    """Missing, computed, ambiguous and unsupported schemas fail closed."""
    schema_path = source_tree / "src" / "config" / "schema.py"
    if source is None:
        schema_path.unlink()
    else:
        schema_path.write_text(source, encoding="utf-8")
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == "unknown"
    assert any("schema" in error for error in result["errors"])


def test_classification_never_executes_source(source_tree, tmp_path):
    """Neither schema/version code nor package initializers may run."""
    marker = tmp_path / "executed"
    side_effect = f'from pathlib import Path\nPath({str(marker)!r}).touch()\nraise RuntimeError("executed")\n'
    for path in (source_tree / "src" / "__init__.py", source_tree / "src" / "config" / "__init__.py"):
        path.write_text(side_effect, encoding="utf-8")
    version_path = source_tree / "src" / "passivbot_version.py"
    version_path.write_text(version_path.read_text() + side_effect, encoding="utf-8")
    (source_tree / "src" / "config" / "schema.py").write_text(
        'CONFIG_SCHEMA_VERSION: str = "v9.0.0"\n' + side_effect, encoding="utf-8",
    )
    assert classify_passivbot_source(source_tree)["generation"] == "pb9"
    assert not marker.exists()


def test_missing_package_version_does_not_override_schema(source_tree):
    """Package metadata failures are diagnostic, not an alternate classifier."""
    (source_tree / "src" / "passivbot_version.py").unlink()
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == "pb8"
    assert result["package_version"] is None
    assert any("passivbot_version.py" in error for error in result["errors"])


def test_source_symlink_cannot_escape_checkout(source_tree, tmp_path):
    """Schema inspection cannot follow a reference outside the selected root."""
    external = tmp_path / "outside.py"
    external.write_text('CONFIG_SCHEMA_VERSION = "v9.0.0"\n', encoding="utf-8")
    schema = source_tree / "src" / "config" / "schema.py"
    schema.unlink()
    schema.symlink_to(external)
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == "unknown"
    assert any("outside" in error for error in result["errors"])


def test_commit_is_read_from_isolated_checkout(source_tree, committed_source, git_env):
    """Report the fixture's actual commit without changing its working tree."""
    result = classify_passivbot_source(source_tree)
    assert result["commit"] == committed_source
    assert result["errors"] == []
    assert _git(source_tree, git_env, "status", "--porcelain") == ""


@pytest.mark.parametrize("modified", [False, True])
def test_crlf_checkout_preserves_identity_but_not_semantic_edits(
    source_tree, committed_source, git_env, modified,
):
    """Git's autocrlf checkout keeps its commit; actual schema edits do not."""
    metadata_paths = ["src/config/schema.py", "src/passivbot_version.py"]
    _git(source_tree, git_env, "config", "core.autocrlf", "true")
    for name in metadata_paths:
        (source_tree / name).unlink()
    _git(source_tree, git_env, "checkout", "--", *metadata_paths)
    for name in metadata_paths:
        assert b"\r\n" in (source_tree / name).read_bytes()
    assert _git(source_tree, git_env, "status", "--porcelain") == ""
    if modified:
        schema = source_tree / metadata_paths[0]
        schema.write_bytes(schema.read_bytes().replace(b"v8.6.0", b"v9.0.0"))
    result = classify_passivbot_source(source_tree)
    if modified:
        assert result["generation"] == "pb9"
        assert result["commit"] is None
        assert result["errors"] == ["Inspected source differs from HEAD: src/config/schema.py; commit is unavailable"]
    else:
        assert result["generation"] == "pb8"
        assert result["commit"] == committed_source
        assert result["errors"] == []


@pytest.mark.parametrize("with_work_tree", [False, True])
def test_inherited_git_environment_cannot_redirect_commit(
    source_tree, committed_source, git_env, tmp_path, monkeypatch, with_work_tree,
):
    """Foreign GIT_DIR and related overrides cannot attach another repo's HEAD."""
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    _git(foreign, git_env, "init", "--quiet")
    _git(foreign, git_env, "-c", "user.name=PBGui Test",
         "-c", "user.email=test@example.invalid", "commit", "--quiet",
         "--allow-empty", "-m", "foreign")
    assert _git(foreign, git_env, "rev-parse", "HEAD").strip() != committed_source
    monkeypatch.setenv("GIT_DIR", str(foreign / ".git"))
    if with_work_tree:
        monkeypatch.setenv("GIT_WORK_TREE", str(source_tree))
        monkeypatch.setenv("GIT_COMMON_DIR", str(foreign / ".git"))
        monkeypatch.setenv("GIT_INDEX_FILE", str(foreign / ".git" / "index"))
        monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
        monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.worktree")
        monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(source_tree))
    result = classify_passivbot_source(source_tree)
    assert result["commit"] == committed_source
    assert result["errors"] == []


@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize(("relative_path", "source", "generation"), [
    ("src/config/schema.py", 'CONFIG_SCHEMA_VERSION = "v9.0.0"\n', "pb9"),
    ("src/passivbot_version.py", '__version__ = "9.0.0.dev0"\n', "pb8"),
])
def test_modified_metadata_is_not_attributed_to_head(
    source_tree, committed_source, git_env, relative_path, source, generation, staged,
):
    """Both staged and unstaged metadata changes invalidate the commit association."""
    (source_tree / relative_path).write_text(source, encoding="utf-8")
    if staged:
        _git(source_tree, git_env, "add", relative_path)
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == generation
    assert result["commit"] is None
    assert result["errors"] == [f"Inspected source differs from HEAD: {relative_path}; commit is unavailable"]


def test_commit_check_uses_inspected_bytes_during_update(source_tree, committed_source, monkeypatch):
    """Restoring the worktree after a read cannot hide an inconsistent snapshot."""
    original_reader = passivbot_runtime._read_string_literal

    def read_during_update(root, relative_path, name):
        """Read a temporary v9 schema, then restore the v8 file before Git reads."""
        if name != "CONFIG_SCHEMA_VERSION":
            return original_reader(root, relative_path, name)
        schema = root / relative_path
        original = schema.read_bytes()
        try:
            schema.write_text('CONFIG_SCHEMA_VERSION = "v9.0.0"\n', encoding="utf-8")
            return original_reader(root, relative_path, name)
        finally:
            schema.write_bytes(original)

    monkeypatch.setattr(passivbot_runtime, "_read_string_literal", read_during_update)
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == "pb9"
    assert result["commit"] is None
    assert result["errors"] == ["Inspected source differs from HEAD: src/config/schema.py; commit is unavailable"]


@pytest.mark.parametrize("repo_dir", [None, "", " ", "\t"])
def test_unset_path_never_inspects_current_directory(
    source_tree, committed_source, monkeypatch, repo_dir,
):
    """Unset paths return diagnostics even when cwd is a valid bot checkout."""
    monkeypatch.chdir(source_tree)
    result = classify_passivbot_source(repo_dir)
    assert result["generation"] == "unknown"
    assert result["package_version"] is None
    assert result["config_schema"] is None
    assert result["commit"] is None
    assert result["errors"] == ["Invalid source directory (ValueError): Source directory must be explicitly set"]


@pytest.mark.parametrize("repo_dir", [Path(""), ".", "src/..", Path("src/.."), "relative"])
def test_relative_path_never_uses_current_checkout(
    source_tree, committed_source, monkeypatch, repo_dir,
):
    """Relative paths, including Path(''), cannot accidentally select cwd."""
    monkeypatch.chdir(source_tree)
    result = classify_passivbot_source(repo_dir)
    assert result["generation"] == "unknown"
    assert result["package_version"] is None
    assert result["config_schema"] is None
    assert result["commit"] is None
    assert result["errors"] == ["Invalid source directory (ValueError): Source directory must be an absolute path"]


def test_linked_worktree_keeps_its_own_identity(source_tree, committed_source, git_env, tmp_path):
    """A .git file in a linked worktree remains a supported source checkout."""
    worktree = tmp_path / "worktree"
    _git(source_tree, git_env, "worktree", "add", "--detach", str(worktree), committed_source)
    assert (worktree / ".git").is_file()
    result = classify_passivbot_source(worktree)
    assert result["generation"] == "pb8"
    assert result["commit"] == committed_source
    assert result["errors"] == []


@pytest.mark.parametrize("failure", [FileNotFoundError(), subprocess.TimeoutExpired("git", 5)])
def test_git_failure_preserves_schema_classification(source_tree, monkeypatch, failure):
    """Unavailable Git metadata never invents a commit or a different generation."""
    (source_tree / ".git").mkdir()

    def fail_git(*args, **kwargs):
        """Simulate a missing or unresponsive Git executable."""
        raise failure

    monkeypatch.setattr(passivbot_runtime.subprocess, "run", fail_git)
    result = classify_passivbot_source(source_tree)
    assert result["generation"] == "pb8"
    assert result["commit"] is None
    assert any("Cannot read source Git commit" in error for error in result["errors"])


def test_missing_source_directory_is_unknown(tmp_path):
    """An absent source root returns a useful diagnostic instead of raising."""
    result = classify_passivbot_source(tmp_path / "absent")
    assert result["generation"] == "unknown"
    assert result["errors"]


@pytest.mark.parametrize("stop_after", [0, 1, 2, 3])
def test_source_cancellation_prevents_late_git_reads(source_tree, committed_source, monkeypatch, stop_after):
    """Each subsequent Git invocation is gated, including both metadata reads."""
    original = subprocess.run
    calls = []

    def run(*args, **kwargs):
        """Complete one real fixture Git read before requesting cancellation."""
        calls.append(kwargs["timeout"])
        return original(*args, **kwargs)

    monkeypatch.setattr(passivbot_runtime.subprocess, "run", run)
    result = classify_passivbot_source(source_tree, git_timeout=1,
                                      cancel_requested=lambda: len(calls) >= stop_after)
    assert len(calls) == stop_after
    assert all(timeout == 1 for timeout in calls)
    assert result["commit"] is None
    assert any("Source inspection cancelled" in error for error in result["errors"])


@pytest.mark.parametrize("timeout", [1, 5])
def test_source_git_timeout_is_reported_without_further_commands(source_tree, committed_source, monkeypatch, timeout):
    """A timed-out command prevents all subsequent reads and retains schema evidence."""
    calls = []

    def expire(arguments, **kwargs):
        """Model subprocess.run's bounded kill-and-wait timeout result."""
        calls.append(kwargs["timeout"])
        raise subprocess.TimeoutExpired(arguments, kwargs["timeout"])

    monkeypatch.setattr(passivbot_runtime.subprocess, "run", expire)
    kwargs = {} if timeout == 5 else {"git_timeout": timeout}
    result = classify_passivbot_source(source_tree, **kwargs)
    assert calls == [timeout]
    assert result["generation"] == "pb8"
    assert result["commit"] is None
    assert result["errors"] == ["Cannot read source Git commit (TimeoutExpired)"]


@pytest.mark.parametrize("timeout", [0, -1, 6, True, float("nan"), float("inf"), "1", None, 10**400])
def test_invalid_git_timeout_is_diagnostic_before_io(source_tree, monkeypatch, timeout):
    """Even huge integers fail closed without conversion overflow or filesystem reads."""
    def forbidden(*args, **kwargs):
        """Invalid inspection options cannot start metadata or Git reads."""
        pytest.fail("invalid timeout reached filesystem or subprocess access")

    monkeypatch.setattr(passivbot_runtime, "_read_string_literal", forbidden)
    monkeypatch.setattr(passivbot_runtime.subprocess, "run", forbidden)
    result = classify_passivbot_source(source_tree, git_timeout=timeout)
    assert result["generation"] == "unknown"
    assert result["commit"] is None
    assert len(result["errors"]) == 1
    assert "Git timeout must be positive and at most five seconds" in result["errors"][0]


@pytest.mark.parametrize("callback", [True, 1, object()])
def test_invalid_cancellation_callback_is_diagnostic(source_tree, callback):
    """A non-callable stop predicate is rejected before source classification."""
    result = classify_passivbot_source(source_tree, cancel_requested=callback)
    assert result["generation"] == "unknown"
    assert result["commit"] is None
    assert len(result["errors"]) == 1
    assert "Cancellation callback must be callable" in result["errors"][0]
