"""Tests for secure GitHub archive resolution and release publishing."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import github_archive


@pytest.mark.parametrize(
    ("origin", "slug"),
    [
        ("https://github.com/example/archive.git", "example/archive"),
        ("git@github.com:example/archive.git", "example/archive"),
        ("ssh://git@github.com/example/archive", "example/archive"),
    ],
)
def test_parse_github_repository_origin(origin: str, slug: str) -> None:
    """Supported credential-free GitHub origins normalize to one slug."""
    assert github_archive.parse_github_repository_origin(origin).slug == slug


@pytest.mark.parametrize(
    "origin",
    [
        "http://github.com/example/archive.git",
        "https://token@github.com/example/archive.git",
        "https://github.com/example/archive.git?token=secret",
        "https://github.com/example/archive/extra",
        "https://gitlab.com/example/archive.git",
        "ssh://root@github.com/example/archive.git",
        "https://github.com/example%2Farchive",
    ],
)
def test_parse_github_repository_origin_rejects_unsafe_values(origin: str) -> None:
    """Origins that could expose credentials or escape the repository are rejected."""
    with pytest.raises(ValueError):
        github_archive.parse_github_repository_origin(origin)


def test_publish_release_asset_keeps_token_out_of_argv(monkeypatch, tmp_path: Path) -> None:
    """GitHub publishing passes credentials only through the child environment."""
    asset = tmp_path / "checksums.sqlite.gz"
    asset.write_bytes(b"snapshot")
    token = "secret-token-value"
    calls = []
    monkeypatch.setattr(github_archive, "own_archive_name", lambda: "mine")
    monkeypatch.setattr(github_archive, "archive_access_token", lambda: token)
    monkeypatch.setattr(
        github_archive,
        "resolve_archive_repository",
        lambda *_args, **_kwargs: github_archive.GitHubRepository("owner", "archive"),
    )
    monkeypatch.setattr(github_archive.shutil, "which", lambda _name: "/usr/bin/gh")

    def run(argv, **kwargs):
        calls.append((list(argv), dict(kwargs.get("env") or {})))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(github_archive.subprocess, "run", run)
    result = github_archive.publish_release_asset(archive_name="mine", asset_path=asset)

    assert result["repository"] == "owner/archive"
    assert len(calls) == 2
    assert all(token not in " ".join(argv) for argv, _env in calls)
    assert all(env["GH_TOKEN"] == token for _argv, env in calls)
    assert calls[-1][0][-2:] == ["--repo", "owner/archive"]
