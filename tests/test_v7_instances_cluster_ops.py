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
    assert nodes[instance["assigned_host"]]["sync_mode"] == "outbound_only"
    assert nodes[instance["assigned_host"]]["sync_enabled"] is True


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
    assert nodes[first_node_id]["sync_mode"] == "disabled"
    assert nodes[first_node_id]["sync_enabled"] is False


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


def test_legacy_v7_api_ssh_sync_is_disabled(monkeypatch, tmp_path: Path) -> None:
    """The old V7 API sync path must not write to VPS hosts on cluster-mode."""

    instance_dir = tmp_path / "data" / "run_v7" / "test_inst"
    _write_config(instance_dir, 9, "vps-a")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))

    result = asyncio.run(v7_instances._ssh_sync_instance("test_inst"))

    assert result["disabled"] is True
    assert result["hosts"] == {}
    assert result["ok"] == 0
    assert result["failed"] == 0


def test_backup_draft_save_clears_tombstone(monkeypatch, tmp_path: Path) -> None:
    """Saving a backup draft is an explicit restore and removes the tombstone."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    backup_dir = tmp_path / "data" / "backup" / "v7" / "test_inst" / "7"
    backup_dir.mkdir(parents=True)
    (backup_dir / "config.json").write_text(json.dumps({"pbgui": {"version": 7}, "live": {}}), encoding="utf-8")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)
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


def test_save_after_deleted_instance_uses_highest_cluster_version(monkeypatch, tmp_path: Path) -> None:
    """Recreating an instance name advances beyond deleted cluster history."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)
    monkeypatch.setattr(v7_instances, "save_pb7_config", lambda cfg, path: path.write_text(json.dumps(cfg), encoding="utf-8"))

    async def noop_runtime_check(name: str, cfg: dict) -> None:
        return None

    async def noop_ssh_sync(name: str) -> dict:
        return {"ok": 0, "failed": 0, "hosts": []}

    class FakeUsers:
        """Minimal Users replacement for save_instance_config."""

        def find_exchange(self, user: str) -> str:
            return ""

    v7_instances._record_cluster_config_upsert(
        "test_inst",
        tmp_path / "data" / "run_v7" / "test_inst",
        _write_config(tmp_path / "data" / "run_v7" / "test_inst", 5, "disabled"),
        parent_version=4,
    )
    v7_instances._record_cluster_instance_delete("test_inst", 5)
    shutil_dir = tmp_path / "data" / "run_v7" / "test_inst"
    for item in shutil_dir.iterdir():
        item.unlink()
    shutil_dir.rmdir()

    async def request_json() -> dict:
        return {"config": {"live": {"user": "test_inst"}, "pbgui": {"version": 1, "enabled_on": "disabled"}}}

    monkeypatch.setattr(v7_instances, "_ensure_target_runtime_compatible", noop_runtime_check)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", noop_ssh_sync)
    monkeypatch.setitem(sys.modules, "User", SimpleNamespace(Users=lambda: FakeUsers()))

    result = asyncio.run(v7_instances.save_instance_config("test_inst", SimpleNamespace(json=request_json), session=None))
    desired = _read_json(tmp_path / "data" / "cluster" / "desired_state.json")
    operation = sorted((tmp_path / "data" / "cluster" / "oplog").glob("*/*.json"))[-1]
    payload = _read_json(operation)

    assert result["version"] == 6
    assert desired["instances"]["test_inst"]["version"] == "6"
    assert payload["parent_version"] == "5"


def test_save_imported_config_ignores_submitted_version(monkeypatch, tmp_path: Path) -> None:
    """Saving an imported config writes exactly highest local/cluster version + 1."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    instance_dir = tmp_path / "data" / "run_v7" / "test_inst"
    _write_config(instance_dir, 4, "disabled")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)
    monkeypatch.setattr(v7_instances, "save_pb7_config", lambda cfg, path: path.write_text(json.dumps(cfg), encoding="utf-8"))

    async def noop_runtime_check(name: str, cfg: dict) -> None:
        return None

    async def noop_ssh_sync(name: str) -> dict:
        return {"ok": 0, "failed": 0, "hosts": []}

    class FakeUsers:
        """Minimal Users replacement for save_instance_config."""

        def find_exchange(self, user: str) -> str:
            return ""

    v7_instances._record_cluster_config_upsert("test_inst", instance_dir, _write_config(instance_dir, 9, "disabled"), parent_version=8)

    async def request_json() -> dict:
        return {"config": {"live": {"user": "test_inst"}, "pbgui": {"version": 999, "enabled_on": "disabled"}}}

    monkeypatch.setattr(v7_instances, "_ensure_target_runtime_compatible", noop_runtime_check)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", noop_ssh_sync)
    monkeypatch.setitem(sys.modules, "User", SimpleNamespace(Users=lambda: FakeUsers()))

    next_version = v7_instances.get_instance_next_version("test_inst", session=None)
    result = asyncio.run(v7_instances.save_instance_config("test_inst", SimpleNamespace(json=request_json), session=None))
    saved = _read_json(instance_dir / "config.json")

    assert next_version == {"name": "test_inst", "next_version": 10}
    assert result["version"] == 10
    assert saved["pbgui"]["version"] == 10


def test_copy_instance_config_copies_referenced_override_files(monkeypatch, tmp_path: Path) -> None:
    """Copying an instance config writes target config and referenced override files."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    source_dir = tmp_path / "data" / "run_v7" / "source_user"
    source_dir.mkdir(parents=True)
    source_cfg = {
        "live": {"user": "source_user"},
        "pbgui": {"version": 3, "enabled_on": "vps-a"},
        "coin_overrides": {"BTC": {"override_config_path": "BTC.json"}},
    }
    source_override = {"bot": {"long": {"total_wallet_exposure_limit": 1.2}}, "custom": "keep"}
    (source_dir / "config.json").write_text(json.dumps(source_cfg), encoding="utf-8")
    (source_dir / "BTC.json").write_text(json.dumps(source_override), encoding="utf-8")
    target_dir = tmp_path / "data" / "run_v7" / "target_user"
    target_dir.mkdir()
    (target_dir / "config.json").write_text(
        json.dumps({"live": {"user": "target_user"}, "pbgui": {"version": 1, "enabled_on": "disabled"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)

    def fake_load(path: Path, neutralize_added: bool = False) -> dict:
        """Read JSON without invoking the pb7_config migration pipeline."""

        return json.loads(Path(path).read_text(encoding="utf-8"))

    def fake_save(cfg: dict, path: Path) -> None:
        """Write JSON without invoking the pb7_config migration pipeline."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cfg), encoding="utf-8")

    async def noop_runtime_check(name: str, cfg: dict) -> None:
        return None

    async def noop_ssh_sync(name: str) -> dict:
        return {"ok": 0, "failed": 0, "hosts": []}

    class FakeUsers:
        """Minimal Users replacement for copy_instance_config."""

        def find_exchange(self, user: str) -> str:
            return "binance"

    async def request_json() -> dict:
        return {
            "target_user": "target_user",
            "config": {
                "live": {"user": "source_user"},
                "pbgui": {"version": 3, "enabled_on": "vps-a"},
                "coin_overrides": {"BTC": {"override_config_path": "BTC.json"}},
            },
        }

    monkeypatch.setattr(v7_instances, "load_pb7_config", fake_load)
    monkeypatch.setattr(v7_instances, "save_pb7_config", fake_save)
    monkeypatch.setattr(v7_instances, "_ensure_target_runtime_compatible", noop_runtime_check)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", noop_ssh_sync)
    monkeypatch.setitem(sys.modules, "User", SimpleNamespace(Users=lambda: FakeUsers()))

    result = asyncio.run(v7_instances.copy_instance_config("source_user", SimpleNamespace(json=request_json), session=None))
    target_cfg = _read_json(tmp_path / "data" / "run_v7" / "target_user" / "config.json")
    target_override = _read_json(tmp_path / "data" / "run_v7" / "target_user" / "BTC.json")

    assert result["name"] == "target_user"
    assert result["source"] == "source_user"
    assert result["override_copy"] == {"copied": ["BTC.json"], "missing": []}
    assert target_cfg["live"]["user"] == "target_user"
    assert target_cfg["pbgui"]["version"] == 2
    assert target_cfg["pbgui"]["enabled_on"] == "disabled"
    assert target_cfg["backtest"]["base_dir"] == "backtests/pbgui/target_user"
    assert target_override == source_override
