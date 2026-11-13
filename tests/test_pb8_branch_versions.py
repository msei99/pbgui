"""Tests for PB8 version branch timestamps."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from pbgui_purefunc import list_remote_git_branch_heads
import vps_manager_service


def _git(*args: str, cwd: Path, env: dict[str, str] | None = None) -> None:
    """Run a Git command in an isolated test repository."""
    subprocess.run(["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True)


def test_remote_branch_heads_include_commit_times(tmp_path: Path) -> None:
    """The controller fetches every branch head and its commit timestamp."""
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    source.mkdir()
    _git("init", "--bare", str(remote), cwd=tmp_path)
    _git("init", "-b", "master", cwd=source)
    _git("config", "user.name", "PBGui Test", cwd=source)
    _git("config", "user.email", "test@example.invalid", cwd=source)
    (source / "README").write_text("first\n", encoding="utf-8")
    _git("add", "README", cwd=source)
    first_env = dict(os.environ, GIT_AUTHOR_DATE="@1700000000 +0000", GIT_COMMITTER_DATE="@1700000000 +0000")
    _git("commit", "-m", "first", cwd=source, env=first_env)
    _git("checkout", "-b", "v9", cwd=source)
    (source / "README").write_text("second\n", encoding="utf-8")
    _git("add", "README", cwd=source)
    second_env = dict(os.environ, GIT_AUTHOR_DATE="@1700003600 +0000", GIT_COMMITTER_DATE="@1700003600 +0000")
    _git("commit", "-m", "second", cwd=source, env=second_env)
    _git("remote", "add", "origin", str(remote), cwd=source)
    _git("push", "origin", "master", "v9", cwd=source)

    versions = list_remote_git_branch_heads(str(remote))

    assert {row["name"]: row["updated_at"] for row in versions} == {
        "master": 1700000000,
        "v9": 1700003600,
    }


def test_branch_names_remain_available_without_dates(monkeypatch) -> None:
    """A failed metadata fetch falls back to names for the PB8 picker."""
    monkeypatch.setattr(vps_manager_service, "list_remote_git_branch_heads", lambda _url: (_ for _ in ()).throw(RuntimeError("offline")))
    monkeypatch.setattr(vps_manager_service, "list_remote_git_branches", lambda _url: ["master", "v9"])
    monkeypatch.setattr(vps_manager_service, "_log", lambda *_args, **_kwargs: None)

    versions = vps_manager_service.VPSManagerService.load_remote_versions(
        object(), "https://github.com/enarjord/passivbot.git"
    )

    assert versions == [{"name": "master", "updated_at": None}, {"name": "v9", "updated_at": None}]


def test_pbgui_versions_accept_only_controller_remotes(monkeypatch) -> None:
    """PBGui installs can use configured fork remotes but reject arbitrary URLs."""
    urls = {
        "origin": "https://github.com/msei99/pbgui.git",
        "fork": "https://github.com/example/pbgui.git",
        "private": "https://token@github.com/example/pbgui.git",
    }
    monkeypatch.setattr(vps_manager_service, "list_git_remotes", lambda _path: list(urls))
    monkeypatch.setattr(vps_manager_service, "get_git_remote_url", lambda _path, name: urls[name])

    sources = vps_manager_service._pbgui_version_sources()
    assert {source["name"] for source in sources} == {"origin", "fork"}
    assert vps_manager_service._validate_pbgui_version_extra_vars({
        "branch": "feature/new-ui", "source_url": urls["fork"],
    }) == {"branch": "feature/new-ui", "source_url": urls["fork"]}

    with pytest.raises(ValueError, match="not configured"):
        vps_manager_service._validate_pbgui_version_extra_vars({
            "branch": "master", "source_url": "https://github.com/unknown/pbgui.git",
        })


def test_pbgui_official_source_remains_available_after_fork_switch(monkeypatch) -> None:
    """Switching the controller origin to a fork keeps official PBGui selectable."""
    monkeypatch.setattr(vps_manager_service, "list_git_remotes", lambda _path: ["origin"])
    monkeypatch.setattr(
        vps_manager_service,
        "get_git_remote_url",
        lambda _path, _name: "https://github.com/example/pbgui.git",
    )
    sources = vps_manager_service._pbgui_version_sources()
    assert {source["name"] for source in sources} == {"origin", "official"}


def test_pb8_versions_reject_unconfigured_remote_url(monkeypatch) -> None:
    """A direct API request cannot build an arbitrary fork on the controller."""
    official = "https://github.com/enarjord/passivbot.git"
    fork = "https://github.com/example/passivbot.git"
    monkeypatch.setattr(vps_manager_service, "_pb8_version_sources", lambda: [{"url": official}])
    with pytest.raises(ValueError, match="not configured"):
        vps_manager_service._validate_pb8_branch_extra_vars({
            "pb8_branch": "feature", "pb8_remote_url": fork,
        })
    assert vps_manager_service._validate_pb8_branch_extra_vars({
        "pb8_branch": "master", "pb8_remote_url": official,
    })["pb8_remote_url"] == official


@pytest.mark.parametrize("playbook", ["master-switch-pbgui-branch.yml", "vps-switch-pbgui-branch.yml"])
def test_pbgui_version_switch_remembers_selected_source(playbook: str) -> None:
    """The chosen repository is saved after a successful checkout for later updates."""
    source = Path(playbook).read_text(encoding="utf-8")
    assert source.index("Reset to selected branch HEAD") < source.index("Remember selected PBGui source")
    assert "argv: [git, remote, set-url, origin, \"{{ selected_source }}\"]" in source


@pytest.mark.parametrize("playbook", [
    "master-update-pbgui.yml", "vps-update-pbgui.yml", "master-update-pb.yml",
    "vps-update-pb.yml", "vps-pbgui-python312.yml",
])
def test_normal_pbgui_updates_follow_selected_source(playbook: str) -> None:
    """Every normal PBGui update path reads the source saved in origin."""
    source = Path(playbook).read_text(encoding="utf-8")
    assert source.index("Get selected PBGui source") < source.index('repo: "{{ current_pbgui_source.stdout }}"')
    assert "argv: [git, remote, get-url, origin]" in source


@pytest.mark.parametrize("playbook", ["master-switch-pbgui-branch.yml", "vps-switch-pbgui-branch.yml"])
def test_older_pbgui_commit_is_checked_before_checkout(playbook: str) -> None:
    """A wrong-branch commit fails before the target discards local tracked changes."""
    source = Path(playbook).read_text(encoding="utf-8")
    verify = source.index("Verify selected PBGui commit belongs to the selected branch")
    discard = source.index("Discard uncommitted tracked changes when forced")
    reset = source.index("Reset to specific commit if provided")
    assert "git\n          - merge-base\n          - --is-ancestor" in source
    assert verify < discard < reset
