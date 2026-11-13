"""Tests for VPSMonitor-owned upstream release collection."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from master import upstream_releases


def _heads(*items: tuple[str, str]) -> str:
    """Build deterministic ls-remote output."""
    return "\n".join(f"{commit}\trefs/heads/{branch}" for branch, commit in items) + "\n"


def test_collector_keeps_pb7_and_pb8_independent_without_local_pb7(
    tmp_path: Path, monkeypatch
) -> None:
    """A PB8-only master still receives independent PB7 and PB8 upstream heads."""
    pbgui = tmp_path / "pbgui"
    pb8 = tmp_path / "pb8"
    (pbgui / ".git").mkdir(parents=True)
    (pb8 / ".git").mkdir(parents=True)
    monkeypatch.setattr(upstream_releases, "pb7dir", lambda: "")
    monkeypatch.setattr(upstream_releases, "pb8dir", lambda: str(pb8))
    pbgui_commit = "a" * 40
    passivbot_commit = "b" * 40

    def fake_run(command, **_kwargs):
        output = (
            _heads(("main", pbgui_commit))
            if upstream_releases.PBGUI_UPSTREAM_URL in command
            else _heads(("master", passivbot_commit), ("v7", "c" * 40))
        )
        return SimpleNamespace(returncode=0, stdout=output)

    monkeypatch.setattr(upstream_releases.subprocess, "run", fake_run)
    collector = upstream_releases.UpstreamReleaseCollector(
        pbgui_dir=pbgui,
        state_path=tmp_path / "state" / "upstream.json",
        clock=lambda: 100.0,
    )

    snapshot = collector._collect_sync()

    assert snapshot["repositories"]["pbgui"]["target_commit"] == pbgui_commit
    assert snapshot["repositories"]["pb7"]["target_commit"] == passivbot_commit
    assert snapshot["repositories"]["pb8"]["target_commit"] == passivbot_commit
    assert snapshot["repositories"]["pb7"] is not snapshot["repositories"]["pb8"]


def test_pbgui_collector_uses_official_url_even_when_master_has_fork_origin(
    tmp_path: Path, monkeypatch
) -> None:
    """A fork selected on the master cannot redirect every host's update status."""
    repo = tmp_path / "pbgui"
    (repo / ".git").mkdir(parents=True)
    monkeypatch.setattr(upstream_releases, "pb7dir", lambda: "")
    monkeypatch.setattr(upstream_releases, "pb8dir", lambda: "")
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout=_heads(("main", "a" * 40)))

    monkeypatch.setattr(upstream_releases.subprocess, "run", fake_run)
    collector = upstream_releases.UpstreamReleaseCollector(
        pbgui_dir=repo, state_path=tmp_path / "state.json", clock=lambda: 100.0,
    )
    snapshot = collector._collect_sync()

    assert commands[0] == ["git", "ls-remote", "--heads", upstream_releases.PBGUI_UPSTREAM_URL]
    assert snapshot["repositories"]["pbgui"]["source_url"] == upstream_releases.PBGUI_UPSTREAM_URL


def test_pbgui_collector_drops_legacy_fork_heads_when_official_probe_fails(
    tmp_path: Path, monkeypatch
) -> None:
    """An old fork snapshot cannot masquerade as stale official PBGui data."""
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({
        "schema_version": upstream_releases.SCHEMA_VERSION,
        "repositories": {"pbgui": {
            "state": "ok", "target_commit": "b" * 40,
            "heads": {"main": "b" * 40},
        }},
    }), encoding="utf-8")
    monkeypatch.setattr(upstream_releases, "pb7dir", lambda: "")
    monkeypatch.setattr(upstream_releases, "pb8dir", lambda: "")
    monkeypatch.setattr(
        upstream_releases.subprocess, "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout=""),
    )
    collector = upstream_releases.UpstreamReleaseCollector(
        pbgui_dir=tmp_path / "pbgui", state_path=state_path, clock=lambda: 100.0,
    )

    pbgui = collector._collect_sync()["repositories"]["pbgui"]
    assert pbgui["state"] == "error"
    assert pbgui["target_commit"] == ""
    assert pbgui["source_url"] == upstream_releases.PBGUI_UPSTREAM_URL


def test_collector_retains_last_known_heads_when_remote_check_fails(
    tmp_path: Path, monkeypatch
) -> None:
    """Transient Git failures expose stale last-known data instead of unknown status."""
    repo = tmp_path / "pbgui"
    (repo / ".git").mkdir(parents=True)
    state_path = tmp_path / "state" / "upstream.json"
    previous = {
        "schema_version": 1,
        "source": "vps-monitor",
        "generated_at": 50.0,
        "repositories": {
            name: {
                "name": name,
                "state": "ok",
                "default_branch": "main" if name == "pbgui" else "master",
                "target_commit": "d" * 40,
                "heads": {"main" if name == "pbgui" else "master": "d" * 40},
                "last_success_at": 50.0,
                **({"source_url": upstream_releases.PBGUI_UPSTREAM_URL} if name == "pbgui" else {}),
            }
            for name in ("pbgui", "pb7", "pb8")
        },
    }
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps(previous), encoding="utf-8")
    monkeypatch.setattr(upstream_releases, "pb7dir", lambda: "")
    monkeypatch.setattr(upstream_releases, "pb8dir", lambda: "")
    monkeypatch.setattr(
        upstream_releases.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout=""),
    )
    collector = upstream_releases.UpstreamReleaseCollector(
        pbgui_dir=repo,
        state_path=state_path,
        clock=lambda: 100.0,
    )

    snapshot = collector._collect_sync()

    for name in ("pbgui", "pb7", "pb8"):
        assert snapshot["repositories"][name]["state"] == "stale"
        assert snapshot["repositories"][name]["target_commit"] == "d" * 40
        assert snapshot["repositories"][name]["last_success_at"] == 50.0


def test_collector_finds_default_branch_after_first_thousand_heads(tmp_path: Path, monkeypatch) -> None:
    """A busy upstream keeps its default head even when the bounded cache fills first."""
    pbgui = tmp_path / "pbgui"
    pb8 = tmp_path / "pb8"
    (pbgui / ".git").mkdir(parents=True)
    (pb8 / ".git").mkdir(parents=True)
    monkeypatch.setattr(upstream_releases, "pb7dir", lambda: "")
    monkeypatch.setattr(upstream_releases, "pb8dir", lambda: str(pb8))
    passivbot_commit = "b" * 40
    busy_heads = _heads(*[(f"agent/{index:04d}", "a" * 40) for index in range(1020)],
                        ("master", passivbot_commit))

    def fake_run(command, **_kwargs):
        """Return a short PBGui ref and the crowded PB7/PB8 upstream."""
        return SimpleNamespace(returncode=0, stdout=(
            _heads(("main", "c" * 40)) if upstream_releases.PBGUI_UPSTREAM_URL in command else busy_heads
        ))

    monkeypatch.setattr(upstream_releases.subprocess, "run", fake_run)
    collector = upstream_releases.UpstreamReleaseCollector(
        pbgui_dir=pbgui, state_path=tmp_path / "state" / "upstream.json", clock=lambda: 100.0)
    snapshot = collector._collect_sync()
    for name in ("pb7", "pb8"):
        entry = snapshot["repositories"][name]
        assert entry["state"] == "ok"
        assert entry["target_commit"] == passivbot_commit
        assert len(entry["heads"]) == upstream_releases.MAX_CACHED_HEADS


def test_collector_refresh_wakeup_is_thread_safe_and_shutdown_is_awaited(tmp_path: Path) -> None:
    """API-thread wakeups trigger the owned loop and stop leaves no collector task."""
    collector = upstream_releases.UpstreamReleaseCollector(
        pbgui_dir=tmp_path,
        state_path=tmp_path / "state.json",
        interval=60,
    )
    calls = 0

    def collect() -> dict:
        nonlocal calls
        calls += 1
        return {
            "schema_version": 1,
            "source": "vps-monitor",
            "generated_at": float(calls),
            "repositories": {},
        }

    collector._collect_and_persist_sync = collect

    async def scenario() -> None:
        await collector.start()
        while calls < 1:
            await asyncio.sleep(0.01)
        await asyncio.to_thread(collector.request_refresh)
        while calls < 2:
            await asyncio.sleep(0.01)
        await collector.stop()

    asyncio.run(scenario())
    assert calls == 2
    assert collector._task is None
    assert collector._loop_ref is None
