"""Regression tests for PBGui release branch discovery."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pbgui_release


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a git command in a test repository."""
    return subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True, check=True)


def _git_commit(cwd: Path, message: str) -> None:
    """Create a deterministic test commit."""
    subprocess.run(
        ["git", "-c", "user.name=PBGui", "-c", "user.email=pbgui@example.invalid", "commit", "-m", message],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=True,
    )


def test_pbgui_branch_history_discovers_single_branch_clone_remote_heads(tmp_path: Path) -> None:
    """PBGui branch history discovers remote heads even in single-branch checkouts."""
    source = tmp_path / "source"
    checkout = tmp_path / "checkout"
    source.mkdir()

    _git(source, "init")
    _git(source, "checkout", "-b", "main")
    (source / "pbgui_purefunc.py").write_text('PBGUI_VERSION = "test-main"\n', encoding="utf-8")
    _git(source, "add", "pbgui_purefunc.py")
    _git_commit(source, "main branch")

    _git(source, "checkout", "-b", "db_manager")
    (source / "pbgui_purefunc.py").write_text('PBGUI_VERSION = "test-db"\n', encoding="utf-8")
    _git(source, "add", "pbgui_purefunc.py")
    _git_commit(source, "db branch")
    _git(source, "checkout", "main")

    _git(tmp_path, "clone", "--single-branch", "--branch", "main", source.resolve().as_uri(), str(checkout))
    fetch_refspec = _git(checkout, "config", "--get-all", "remote.origin.fetch").stdout.strip()
    assert fetch_refspec == "+refs/heads/main:refs/remotes/origin/main"
    assert "origin/db_manager" not in _git(checkout, "branch", "-a").stdout

    commits = pbgui_release.load_more_pbgui_commits("db_manager", checkout, limit=5)
    history = pbgui_release.load_pbgui_branch_history(checkout, limit=5)

    assert commits[0]["message"] == "db branch"
    assert "db_manager" in history
    assert "main" in history
    assert "origin/db_manager" in _git(checkout, "branch", "-a").stdout
    assert _git(checkout, "config", "--get-all", "remote.origin.fetch").stdout.strip() == fetch_refspec
