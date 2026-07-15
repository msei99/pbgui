"""Tests for private, process-safe VPS inventory persistence."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
from pathlib import Path

import pytest

from vps_inventory_store import delete_inventory_path, patch_inventory_json, write_inventory_json
import master.async_pool as async_pool
import vps_manager_core as core


def _patch_worker(root: str, path: str, key: str) -> None:
    """Patch one distinct field from a child process."""
    patch_inventory_json(Path(root), Path(path), {key: key})


def test_inventory_write_and_patch_are_private_atomic_and_lossless(tmp_path: Path) -> None:
    """Concurrent patches retain every field and repair private modes."""
    root = tmp_path / "data" / "vpsmanager"
    path = root / "hosts" / "node-a" / "node-a.json"
    write_inventory_json(root, path, {"hostname": "node-a"})
    os.chmod(path, 0o644)
    os.chmod(path.parent, 0o755)

    process_context = multiprocessing.get_context("spawn")
    workers = [
        process_context.Process(target=_patch_worker, args=(str(root), str(path), f"field_{index}"))
        for index in range(8)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
        assert worker.exitcode == 0

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["hostname"] == "node-a"
    assert all(payload[f"field_{index}"] == f"field_{index}" for index in range(8))
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert root.stat().st_mode & 0o777 == 0o700


def test_inventory_delete_uses_validated_inventory_boundary(tmp_path: Path) -> None:
    """Deletion removes an inventory child but rejects paths outside the root."""
    root = tmp_path / "data" / "vpsmanager"
    path = root / "hosts" / "node-a" / "node-a.json"
    write_inventory_json(root, path, {"hostname": "node-a"})

    delete_inventory_path(root, path.parent)
    assert not path.parent.exists()

    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        delete_inventory_path(root, outside)
    except RuntimeError as exc:
        assert "outside" in str(exc).lower()
    else:
        raise AssertionError("outside inventory path was accepted")


def test_inventory_store_rejects_symlinked_host_directory(tmp_path: Path) -> None:
    """A host symlink cannot redirect sensitive inventory persistence."""
    root = tmp_path / "data" / "vpsmanager"
    hosts = root / "hosts"
    hosts.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (hosts / "node-a").symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeError, match="symlink"):
        write_inventory_json(root, hosts / "node-a" / "node-a.json", {"hostname": "node-a"})
    assert list(outside.iterdir()) == []


def test_vps_and_async_pool_share_private_inventory_store(tmp_path: Path, monkeypatch) -> None:
    """Core saves and async field patches use one lossless host record."""
    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    monkeypatch.setattr(async_pool, "PBGDIR", str(tmp_path))
    vps = core.VPS()
    vps.hostname = "node-a"
    vps.ip = "192.0.2.1"
    vps.user = "bot"
    vps.save()

    pool = object.__new__(async_pool.AsyncSSHPool)
    asyncio.run(pool._persist_remote_pbgui_dir("node-a", "/home/bot/software/pbgui"))
    vps.ip = "192.0.2.2"
    vps.save()

    path = tmp_path / "data" / "vpsmanager" / "hosts" / "node-a" / "node-a.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ip"] == "192.0.2.2"
    assert payload["remote_pbgui_dir"] == "/home/bot/software/pbgui"
    assert payload["_revision"] == 3
    assert path.stat().st_mode & 0o777 == 0o600


def test_master_inventory_save_is_private(tmp_path: Path, monkeypatch) -> None:
    """Master status uses the same atomic owner-only persistence."""
    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    manager = object.__new__(core.VPSManager)
    manager.hostname = "master-a"
    manager.last_update = "now"
    manager.update_status = "successful"
    manager.command = "update"
    manager.command_text = "Update"

    manager.save_master()

    path = tmp_path / "data" / "vpsmanager" / "master-a.json"
    assert json.loads(path.read_text(encoding="utf-8"))["update_status"] == "successful"
    assert path.stat().st_mode & 0o777 == 0o600


def test_stale_vps_objects_merge_independent_field_changes(tmp_path: Path, monkeypatch) -> None:
    """Two stale full-record saves retain independent changes from both objects."""
    monkeypatch.setattr(core, "PBGDIR", tmp_path)
    initial = core.VPS()
    initial.hostname = "node-a"
    initial.ip = "192.0.2.1"
    initial.user = "bot"
    initial.save()
    path = tmp_path / "data" / "vpsmanager" / "hosts" / "node-a" / "node-a.json"

    first = core.VPS()
    first.load(path)
    second = core.VPS()
    second.load(path)
    first.ip = "192.0.2.2"
    first.save()
    second.user = "operator"
    second.save()
    second.swap = "4G"
    second.save()

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ip"] == "192.0.2.2"
    assert payload["user"] == "operator"
    assert payload["swap"] == "4G"
