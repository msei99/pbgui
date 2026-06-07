"""Regression tests for V7 config synchronization startup behavior."""

import asyncio
import json
from types import MethodType

import pytest

from master import v7_config_sync
from master.v7_config_sync import V7ConfigSyncWorker


class _FakePool:
    """Minimal async pool stub for V7ConfigSyncWorker tests."""

    def connected_hosts(self) -> list[str]:
        """Return one connected host."""
        return ["manibot93"]

    def get_connection(self, hostname: str) -> object | None:
        """Return a truthy connection for the test host."""
        return object() if hostname == "manibot93" else None

    def get_remote_pbgui_dir(self, hostname: str) -> str:
        """Return the remote PBGui directory."""
        return "/home/mani/software/pbgui"

    async def stat_remote(self, hostname: str, path: str) -> bool:
        """Only the remote cmd directory exists for this test."""
        return path.endswith("/data/cmd")

    async def list_remote_dir(self, hostname: str, path: str) -> list[str]:
        """No per-instance running_version watches are needed for this test."""
        return []


class _StatusPool:
    """Pool stub that serves one remote status_v7.json payload."""

    def __init__(self, remote_status: dict) -> None:
        """Store the remote status payload returned by read_remote_file."""
        self.remote_status = remote_status

    def get_remote_pbgui_dir(self, hostname: str) -> str:
        """Return the remote PBGui directory."""
        return "/remote/pbgui"

    async def read_remote_file(self, hostname: str, path: str) -> bytes:
        """Return the configured status_v7.json bytes."""
        return json.dumps(self.remote_status).encode("utf-8")


def test_start_watchers_runs_initial_reconcile() -> None:
    """Starting a watcher performs an initial status_v7 reconcile."""
    worker = V7ConfigSyncWorker(_FakePool(), object(), object())
    reconciled: list[str] = []

    async def fake_watcher_loop(self: V7ConfigSyncWorker, hostname: str, paths: list[str]) -> None:
        return None

    async def fake_reconcile(self: V7ConfigSyncWorker, hostname: str) -> None:
        reconciled.append(hostname)

    worker._watcher_loop = MethodType(fake_watcher_loop, worker)
    worker._reconcile_status_v7 = MethodType(fake_reconcile, worker)

    asyncio.run(worker.start_watchers(["manibot93"]))

    assert reconciled == ["manibot93"]


def test_reconcile_pulls_missing_legacy_instance_without_activate_ts(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing local instance is pulled even from legacy status entries."""
    local_run_v7 = tmp_path / "run_v7"
    status_file = tmp_path / "cmd" / "status_v7.json"
    status_file.parent.mkdir(parents=True)

    monkeypatch.setattr(v7_config_sync, "LOCAL_RUN_V7", local_run_v7)
    monkeypatch.setattr(v7_config_sync, "STATUS_V7_FILE", status_file)

    remote_status = {
        "activate_ts": 200,
        "activate_pbname": "remote_master",
        "instances": {
            "legacy_bot": {
                "enabled_on": "manibot93",
                "version": 1,
                "multi": None,
                "running": False,
            }
        },
    }
    worker = V7ConfigSyncWorker(_StatusPool(remote_status), object(), object())
    pulled: list[str] = []

    async def fake_pull(self: V7ConfigSyncWorker, hostname: str, instance_name: str) -> None:
        pulled.append(instance_name)

    worker._pull_instance_configs = MethodType(fake_pull, worker)

    asyncio.run(worker._reconcile_status_v7("manibot93"))

    saved_status = json.loads(status_file.read_text(encoding="utf-8"))
    assert pulled == ["legacy_bot"]
    assert "legacy_bot" in saved_status["instances"]


def test_reconcile_pulls_missing_directory_for_existing_legacy_status(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing local directory is pulled even when status names match."""
    local_run_v7 = tmp_path / "run_v7"
    status_file = tmp_path / "cmd" / "status_v7.json"
    status_file.parent.mkdir(parents=True)
    legacy_entry = {
        "enabled_on": "manibot93",
        "version": 1,
        "multi": None,
        "running": False,
    }
    status_file.write_text(
        json.dumps(
            {
                "activate_ts": 100,
                "activate_pbname": "local_master",
                "instances": {"legacy_bot": legacy_entry},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(v7_config_sync, "LOCAL_RUN_V7", local_run_v7)
    monkeypatch.setattr(v7_config_sync, "STATUS_V7_FILE", status_file)

    remote_status = {
        "activate_ts": 200,
        "activate_pbname": "remote_master",
        "instances": {"legacy_bot": legacy_entry},
    }
    worker = V7ConfigSyncWorker(_StatusPool(remote_status), object(), object())
    pulled: list[str] = []

    async def fake_pull(self: V7ConfigSyncWorker, hostname: str, instance_name: str) -> None:
        pulled.append(instance_name)

    worker._pull_instance_configs = MethodType(fake_pull, worker)

    asyncio.run(worker._reconcile_status_v7("manibot93"))

    assert pulled == ["legacy_bot"]


def test_reconcile_keeps_local_instances_missing_from_stale_remote(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Older remote status must not delete local v7 directories or status."""
    local_run_v7 = tmp_path / "run_v7"
    local_instance = local_run_v7 / "local_bot"
    local_instance.mkdir(parents=True)
    (local_instance / "config.json").write_text("{}", encoding="utf-8")

    status_file = tmp_path / "cmd" / "status_v7.json"
    status_file.parent.mkdir(parents=True)
    status_file.write_text(
        json.dumps(
            {
                "activate_ts": 100,
                "activate_pbname": "local_master",
                "instances": {
                    "local_bot": {
                        "enabled_on": "disabled",
                        "version": 1,
                        "multi": None,
                        "running": False,
                        "activate_ts": 100,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(v7_config_sync, "LOCAL_RUN_V7", local_run_v7)
    monkeypatch.setattr(v7_config_sync, "STATUS_V7_FILE", status_file)

    remote_status = {
        "activate_ts": 50,
        "activate_pbname": "remote_master",
        "instances": {},
    }
    worker = V7ConfigSyncWorker(_StatusPool(remote_status), object(), object())

    async def fake_pull(self: V7ConfigSyncWorker, hostname: str, instance_name: str) -> None:
        return None

    worker._pull_instance_configs = MethodType(fake_pull, worker)

    asyncio.run(worker._reconcile_status_v7("manibot93"))

    saved_status = json.loads(status_file.read_text(encoding="utf-8"))
    assert local_instance.is_dir()
    assert "local_bot" in saved_status["instances"]
    assert saved_status["activate_ts"] == 100
