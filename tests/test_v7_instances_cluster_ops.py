"""Tests for V7 instance cluster operation recording."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from api import v7_instances


def _write_config(instance_dir: Path, version: int, enabled_on: str) -> dict:
    """Write a minimal V7 config and return the config dict."""

    instance_dir.mkdir(parents=True, exist_ok=True)
    cfg = {"live": {}, "pbgui": {"version": version, "enabled_on": enabled_on}}
    (instance_dir / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    return cfg


def _read_json(path: Path) -> dict:
    """Read a JSON object from disk."""

    return json.loads(path.read_text(encoding="utf-8"))


def test_record_cluster_config_upsert_materializes_disabled_instance(monkeypatch, tmp_path: Path) -> None:
    """A disabled V7 config records a stopped desired state on the local node."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    instance_dir = tmp_path / "data" / "run_v7" / "test_inst"
    cfg = _write_config(instance_dir, 4, "disabled")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)

    v7_instances._record_cluster_config_upsert("test_inst", instance_dir, cfg, parent_version=3)

    cluster_root = tmp_path / "data" / "cluster"
    desired = _read_json(cluster_root / "desired_state.json")
    nodes = _read_json(cluster_root / "cluster_nodes.json")["nodes"]
    instance = desired["instances"]["test_inst"]

    assert instance["version"] == "4"
    assert instance["desired_state"] == "stopped"
    assert instance["assigned_host"] in nodes
    assert nodes[instance["assigned_host"]]["role"] == "master"
    assert nodes[instance["assigned_host"]]["pbname"] == "master"


def test_record_cluster_config_upsert_uses_stable_node_id_for_remote_host(monkeypatch, tmp_path: Path) -> None:
    """A remote enabled_on host receives one persistent local node_id mapping."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    instance_dir = tmp_path / "data" / "run_v7" / "test_inst"
    cfg = _write_config(instance_dir, 5, "vps-a")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)

    v7_instances._record_cluster_config_upsert("test_inst", instance_dir, cfg, parent_version=4)
    first_mapping = _read_json(tmp_path / "data" / "cluster" / "host_node_ids.json")
    first_node_id = first_mapping["hosts"]["vps-a"]["node_id"]

    cfg = _write_config(instance_dir, 6, "vps-a")
    v7_instances._record_cluster_config_upsert("test_inst", instance_dir, cfg, parent_version=5)

    cluster_root = tmp_path / "data" / "cluster"
    second_mapping = _read_json(cluster_root / "host_node_ids.json")
    desired = _read_json(cluster_root / "desired_state.json")
    nodes = _read_json(cluster_root / "cluster_nodes.json")["nodes"]
    instance = desired["instances"]["test_inst"]

    assert second_mapping["hosts"]["vps-a"]["node_id"] == first_node_id
    assert instance["version"] == "6"
    assert instance["desired_state"] == "running"
    assert instance["assigned_host"] == first_node_id
    assert nodes[first_node_id]["role"] == "vps"
    assert nodes[first_node_id]["pbname"] == "vps-a"


def test_record_cluster_instance_delete_materializes_tombstone(monkeypatch, tmp_path: Path) -> None:
    """A V7 delete operation records a tombstone in desired_state."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)

    v7_instances._record_cluster_instance_delete("test_inst", 7)

    desired = _read_json(tmp_path / "data" / "cluster" / "desired_state.json")

    assert "test_inst" not in desired["instances"]
    assert desired["tombstones"]["test_inst"]["version"] == "7"


def test_restore_marked_upsert_recreates_deleted_instance(monkeypatch, tmp_path: Path) -> None:
    """A backup restore can recreate a V7 instance after a local delete."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    instance_dir = tmp_path / "data" / "run_v7" / "test_inst"
    cfg = _write_config(instance_dir, 8, "disabled")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)

    v7_instances._record_cluster_instance_delete("test_inst", 7)
    v7_instances._record_cluster_config_upsert(
        "test_inst",
        instance_dir,
        cfg,
        parent_version=7,
        allow_tombstone_recreate=True,
    )

    desired = _read_json(tmp_path / "data" / "cluster" / "desired_state.json")

    assert "test_inst" in desired["instances"]
    assert "test_inst" not in desired["tombstones"]
    assert desired["instances"]["test_inst"]["version"] == "8"


def test_record_cluster_config_upsert_missing_instance_dir_is_warning_only(monkeypatch, tmp_path: Path) -> None:
    """Cluster recording failures do not raise into V7 config write endpoints."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))

    v7_instances._record_cluster_config_upsert(
        "missing_inst",
        tmp_path / "data" / "run_v7" / "missing_inst",
        {"pbgui": {"version": 1, "enabled_on": "disabled"}},
        parent_version=0,
    )

    assert not (tmp_path / "data" / "cluster" / "desired_state.json").exists()


def test_backup_draft_save_clears_tombstone(monkeypatch, tmp_path: Path) -> None:
    """Saving a backup draft is an explicit restore and removes the tombstone."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    backup_dir = tmp_path / "data" / "backup" / "v7" / "test_inst" / "7"
    backup_dir.mkdir(parents=True)
    (backup_dir / "config.json").write_text(json.dumps({"pbgui": {"version": 7}, "live": {}}), encoding="utf-8")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)
    monkeypatch.setattr(v7_instances, "_update_status_v7", lambda name: None)
    monkeypatch.setattr(v7_instances, "save_pb7_config", lambda cfg, path: path.write_text(json.dumps(cfg), encoding="utf-8"))

    async def noop_runtime_check(name: str, cfg: dict) -> None:
        return None

    async def noop_ssh_sync(name: str) -> dict:
        return {"ok": 0, "failed": 0, "hosts": []}

    class FakeUsers:
        """Minimal Users replacement for save_instance_config."""

        def find_exchange(self, user: str) -> str:
            return ""

    async def request_json() -> dict:
        return {
            "config": {
                "live": {"user": "test_inst"},
                "pbgui": {
                    "version": 8,
                    "enabled_on": "disabled",
                    "from_backup_config": {"name": "test_inst", "timestamp": "7"},
                },
            }
        }

    monkeypatch.setattr(v7_instances, "_ensure_target_runtime_compatible", noop_runtime_check)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", noop_ssh_sync)
    monkeypatch.setitem(sys.modules, "User", SimpleNamespace(Users=lambda: FakeUsers()))
    v7_instances._record_cluster_instance_delete("test_inst", 7)

    asyncio.run(v7_instances.save_instance_config("test_inst", SimpleNamespace(json=request_json), session=None))

    desired = _read_json(tmp_path / "data" / "cluster" / "desired_state.json")

    assert "test_inst" in desired["instances"]
    assert "test_inst" not in desired["tombstones"]
    assert desired["instances"]["test_inst"]["version"] == "8"
