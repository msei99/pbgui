"""Regression tests for disabled legacy V7 config synchronization."""

import asyncio
import json

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
    """Pool stub that must not be read while legacy sync is disabled."""

    def __init__(self, remote_status: dict) -> None:
        """Store the remote status payload returned by read_remote_file."""
        self.remote_status = remote_status

    def get_remote_pbgui_dir(self, hostname: str) -> str:
        """Return the remote PBGui directory."""
        return "/remote/pbgui"

    async def read_remote_file(self, hostname: str, path: str) -> bytes:
        """Fail if disabled reconcile attempts to read remote status."""
        raise AssertionError("legacy V7 sync should not read remote status")


def test_start_watchers_is_noop_when_legacy_sync_disabled() -> None:
    """Starting legacy V7 watchers does not create tasks on cluster-mode."""

    worker = V7ConfigSyncWorker(_FakePool(), object(), object())

    asyncio.run(worker.start_watchers(["manibot93"]))

    assert worker._watchers == {}


def test_start_watchdog_is_noop_when_legacy_sync_disabled() -> None:
    """Starting the legacy V7 watchdog does not create a background task."""

    worker = V7ConfigSyncWorker(_FakePool(), object(), object())

    worker.start_watchdog()

    assert worker._watchdog is None


def test_reconcile_is_noop_when_legacy_sync_disabled(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disabled legacy reconcile does not pull configs or write local status."""

    local_run_v7 = tmp_path / "run_v7"
    status_file = tmp_path / "cmd" / "status_v7.json"
    status_file.parent.mkdir(parents=True)
    status_file.write_text(json.dumps({"instances": {}}), encoding="utf-8")

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

    asyncio.run(worker._reconcile_status_v7("manibot93"))

    saved_status = json.loads(status_file.read_text(encoding="utf-8"))
    assert saved_status == {"instances": {}}
    assert not local_run_v7.exists()
