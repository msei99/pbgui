"""Focused tests for Hyperliquid expiry warnings and live bot state."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from master import async_monitor
from master.async_monitor import VPSMonitor


def _write_live_config(
    root: Path,
    runtime: str,
    instance_name: str,
    user_name: str,
    enabled_on: str,
) -> None:
    """Write one minimal PB7 or PB8 live config fixture."""

    run_name = "run_v7" if runtime == "7" else "run_v8"
    instance_dir = root / "data" / run_name / instance_name
    instance_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "live": {"user": user_name},
        "pbgui": {"runtime": f"pb{runtime}", "enabled_on": enabled_on},
    }
    (instance_dir / "config.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_desired_state(root: Path, desired: dict[str, Any]) -> None:
    """Write a complete minimal Cluster desired-state fixture."""

    cluster_root = root / "data" / "cluster"
    cluster_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "instances": {},
        "tombstones": {},
        "pb8_instances": {},
        "pb8_tombstones": {},
        **desired,
    }
    (cluster_root / "desired_state.json").write_text(json.dumps(payload), encoding="utf-8")


def _make_store(observations: list[tuple[str, str, Any]]) -> SimpleNamespace:
    """Build a minimal VPSStore-shaped object with runtime-qualified observations."""

    v7_items = [{"name": name, "running": running} for runtime, name, running in observations if runtime == "7"]
    v8_items = [{"name": name, "running": running} for runtime, name, running in observations if runtime == "8"]
    return SimpleNamespace(
        instances={},
        v7_instances={"host": v7_items} if v7_items else {},
        v8_instances={"host": v8_items} if v8_items else {},
    )


def _run_expiry_check(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    user_names: list[str],
    observations: list[tuple[str, str, Any]],
) -> tuple[VPSMonitor, list[str]]:
    """Run one expiry check with isolated users, state, config roots, and alerts."""

    users = [SimpleNamespace(name=name, exchange="hyperliquid") for name in user_names]
    expiry_ms = int((datetime.now(tz=timezone.utc) + timedelta(days=2)).timestamp() * 1000)
    user_module = types.ModuleType("User")
    user_module.Users = lambda: users
    state_module = types.ModuleType("api_key_state")
    state_module.get_user_state = lambda _name: {"hl_valid_until": expiry_ms}
    monkeypatch.setitem(sys.modules, "User", user_module)
    monkeypatch.setitem(sys.modules, "api_key_state", state_module)
    monkeypatch.setattr(async_monitor, "PBGDIR", str(root))
    monkeypatch.setattr(async_monitor, "load_ini", lambda section, key: "7" if (section, key) == ("hl_expiry", "telegram_warning_days") else "")

    monitor = object.__new__(VPSMonitor)
    monitor._telegram_token = "telegram-token"
    monitor._telegram_chat_id = "telegram-chat"
    monitor._hl_expiry_last_warned = {}
    monitor.store = _make_store(observations)
    sent: list[str] = []

    async def capture(message: str) -> None:
        """Capture one Telegram alert without network access."""

        sent.append(message)

    monitor._send_alert = capture
    monitor._save_alert_state = lambda: None
    asyncio.run(monitor.check_hl_expiry())
    return monitor, sent


@pytest.mark.parametrize(
    ("configs", "desired", "observations", "user_names", "expected_warned"),
    [
        ([], None, [], ["alice"], set()),
        (
            [("7", "first", "alice", "disabled"), ("7", "second", "alice", "disabled")],
            None,
            [],
            ["alice"],
            set(),
        ),
        (
            [("8", "first", "alice", "disabled"), ("8", "second", "alice", "disabled")],
            None,
            [],
            ["alice"],
            set(),
        ),
        (
            [("7", "stopped", "alice", "disabled"), ("8", "active", "bob", "runner")],
            None,
            [],
            ["alice", "bob"],
            {"bob"},
        ),
        (
            [("7", "stopped", "alice", "disabled"), ("8", "active", "alice", "runner")],
            None,
            [],
            ["alice"],
            {"alice"},
        ),
        (
            [("7", "directory-is-not-user", "alice", "runner")],
            None,
            [],
            ["alice", "directory-is-not-user"],
            {"alice"},
        ),
        (
            [("7", "cluster-stopped", "alice", "historical-runner")],
            {"instances": {"cluster-stopped": {"desired_state": "stopped", "conflicted": False}}},
            [],
            ["alice"],
            set(),
        ),
        (
            [("7", "conflicted", "alice", "disabled")],
            {"instances": {"conflicted": {"desired_state": "stopped", "conflicted": True}}},
            [],
            ["alice"],
            {"alice"},
        ),
        (
            [
                ("7", "still-running", "alice", "disabled"),
                ("8", "still-running", "bob", "disabled"),
            ],
            {"instances": {"still-running": {"desired_state": "stopped", "conflicted": False}}},
            [("7", "still-running", True)],
            ["alice", "bob"],
            {"alice"},
        ),
    ],
    ids=[
        "no-configs",
        "all-pb7-stopped",
        "all-pb8-stopped",
        "mixed-one-active",
        "shared-user-one-active",
        "live-user-differs-from-directory",
        "cluster-stop-overrides-enabled-on",
        "conflicted-state-warns",
        "actual-running-warns",
    ],
)
def test_hl_expiry_warning_follows_live_config_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    configs: list[tuple[str, str, str, str]],
    desired: dict[str, Any] | None,
    observations: list[tuple[str, str, Any]],
    user_names: list[str],
    expected_warned: set[str],
) -> None:
    """Warn only users referenced by a bot that is not conclusively stopped."""

    for config in configs:
        _write_live_config(tmp_path, *config)
    if desired is not None:
        _write_desired_state(tmp_path, desired)

    _monitor, sent = _run_expiry_check(monkeypatch, tmp_path, user_names, observations)

    message = "\n".join(sent)
    warned = {name for name in user_names if f"*{name}*" in message}
    assert warned == expected_warned


@pytest.mark.parametrize("runtime", ["7", "8"], ids=["pb7", "pb8"])
def test_suppressed_hl_user_is_not_daily_deduped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runtime: str,
) -> None:
    """A stopped bot may start later and still trigger its first warning that day."""

    _write_live_config(tmp_path, runtime, "bot", "alice", "disabled")
    monitor, sent = _run_expiry_check(monkeypatch, tmp_path, ["alice"], [])

    assert sent == []
    assert "alice" not in monitor._hl_expiry_last_warned

    _write_live_config(tmp_path, runtime, "bot", "alice", "runner")
    asyncio.run(monitor.check_hl_expiry())

    assert len(sent) == 1
    assert "*alice*" in sent[0]
    assert "alice" in monitor._hl_expiry_last_warned
