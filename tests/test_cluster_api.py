"""Tests for the read-only Cluster Sync API helpers."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import cluster
from api import v7_instances
from master.cluster_state import append_operation, ensure_local_identity, load_operations


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
HASH_A = "sha256:" + "a" * 64


def _init_cluster(tmp_path: Path) -> Path:
    """Create deterministic local cluster state for API tests."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="master",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    return root


def _write_v7_config(tmp_path: Path, name: str, version: int, enabled_on: str = "disabled") -> Path:
    """Write a minimal local V7 config for bootstrap tests."""

    instance_dir = tmp_path / "data" / "run_v7" / name
    instance_dir.mkdir(parents=True, exist_ok=True)
    cfg = {"live": {"user": name}, "pbgui": {"version": version, "enabled_on": enabled_on}}
    (instance_dir / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    return instance_dir


def _write_vps_config(
    tmp_path: Path,
    hostname: str,
    *,
    ip: str = "192.0.2.10",
    user: str = "pbuser",
    ssh_port: int | str = 22,
) -> Path:
    """Write a minimal VPS Manager host config for bootstrap tests."""

    host_dir = tmp_path / "data" / "vpsmanager" / "hosts" / hostname
    host_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "_hostname": hostname,
        "ip": ip,
        "user": user,
        "firewall_ssh_port": ssh_port,
        "remote_pbgui_dir": f"/home/{user}/software/pbgui",
    }
    config_path = host_dir / f"{hostname}.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def _read_json(path: Path) -> dict:
    """Read a JSON object from disk."""

    return json.loads(path.read_text(encoding="utf-8"))


def _patch_cluster_config_loader(monkeypatch) -> None:
    """Use a lightweight config loader for bootstrap tests."""

    monkeypatch.setattr(cluster, "load_pb7_config", lambda path, neutralize_added=False: _read_json(Path(path)))


def test_get_status_reports_materialized_counts(monkeypatch, tmp_path: Path) -> None:
    """Cluster status summarizes nodes, instances, conflicts, tombstones, and oplog size."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=102)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTCUSDT",
            "parent_version": "1",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=103,
    )

    status = cluster.get_status(session=None)

    assert status["read_only"] is True
    assert status["identity"]["cluster_id"] == CLUSTER_ID
    assert status["counts"] == {"nodes": 2, "instances": 1, "conflicts": 0, "tombstones": 0, "oplog": 3}
    assert status["warnings"] == []


def test_get_desired_state_returns_instances_and_tombstones(monkeypatch, tmp_path: Path) -> None:
    """Desired state endpoint returns V7 instances and explicit delete tombstones."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_ETHUSDT",
            "version": "4",
            "assigned_host": NODE_A,
            "desired_state": "stopped",
            "config_manifest_hash": HASH_A,
        },
        created_at=101,
    )
    append_operation(root, "DELETE_INSTANCE", {"instance": "bybit_BTCUSDT", "version": "7"}, created_at=102)

    payload = cluster.get_desired_state(session=None)

    assert payload["instances"] == [
        {
            "instance": "bybit_ETHUSDT",
            "version": "4",
            "desired_state": "stopped",
            "assigned_host": NODE_A,
            "config_manifest_hash": HASH_A,
            "updated_by": NODE_A,
            "updated_at": 101,
            "conflicted": False,
        }
    ]
    assert payload["tombstones"] == [
        {
            "instance": "bybit_BTCUSDT",
            "version": "7",
            "deleted_by": NODE_A,
            "deleted_at": 102,
            "op_id": f"{NODE_A}:00000002",
        }
    ]


def test_get_oplog_returns_recent_operations_newest_first(monkeypatch, tmp_path: Path) -> None:
    """Oplog endpoint returns operations sorted newest first and honors the limit."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "STOP_INSTANCE", {"instance": "bybit_BTCUSDT"}, created_at=103)
    append_operation(root, "START_INSTANCE", {"instance": "bybit_BTCUSDT"}, created_at=102)

    payload = cluster.get_oplog(limit=2, session=None)

    assert payload["count"] == 3
    assert [op["op"] for op in payload["operations"]] == ["STOP_INSTANCE", "START_INSTANCE"]


def test_get_status_initializes_empty_cluster_identity(monkeypatch, tmp_path: Path) -> None:
    """Status endpoint initializes local identity without appending any cluster operations."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    status = cluster.get_status(session=None)

    assert status["identity"]["cluster_id"].startswith("pbgui-cluster-")
    assert status["identity"]["node_id"].startswith("pbgui-node-")
    assert status["counts"] == {"nodes": 0, "instances": 0, "conflicts": 0, "tombstones": 0, "oplog": 0}
    assert status["warnings"] == ["No cluster node membership operation has been recorded yet."]
    assert not (tmp_path / "data" / "cluster" / "cluster_nodes.json").exists()
    assert not (tmp_path / "data" / "cluster" / "desired_state.json").exists()


def test_bootstrap_preview_reports_missing_local_config(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap preview lists local V7 configs not yet in desired state without writing state files."""

    _init_cluster(tmp_path)
    _write_v7_config(tmp_path, "test_inst", 3)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    _patch_cluster_config_loader(monkeypatch)

    preview = cluster.get_bootstrap_preview(session=None)

    assert preview["counts"]["add"] == 1
    assert preview["can_apply"] is True
    assert preview["items"][0]["instance"] == "test_inst"
    assert preview["items"][0]["action"] == "add"
    assert not (tmp_path / "data" / "cluster" / "desired_state.json").exists()


def test_apply_bootstrap_records_missing_local_config(monkeypatch, tmp_path: Path) -> None:
    """Applying bootstrap writes UPSERT_CONFIG for local configs and materializes desired state."""

    _init_cluster(tmp_path)
    _write_v7_config(tmp_path, "test_inst", 4)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_monitor", None)
    _patch_cluster_config_loader(monkeypatch)

    result = cluster.apply_bootstrap(session=None)
    desired = _read_json(tmp_path / "data" / "cluster" / "desired_state.json")

    assert result["result"]["counts"]["applied"] == 1
    assert result["after"]["counts"]["skip"] == 1
    assert desired["instances"]["test_inst"]["version"] == "4"
    assert "test_inst" not in desired["tombstones"]


def test_remote_status_reports_successful_hello(monkeypatch, tmp_path: Path) -> None:
    """Remote status uses the existing SSH pool to run a read-only cluster hello."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "vps", "pbname": "vps-a", "remote_pbgui_dir": "software/pbgui"},
        created_at=101,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[tuple[str, str, int]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command, timeout))
            return SimpleNamespace(
                exit_status=0,
                stdout=json.dumps({
                    "ok": True,
                    "cluster_id": CLUSTER_ID,
                    "node_id": NODE_B,
                    "protocol_version": 1,
                    "role": "vps",
                }),
                stderr="",
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.get_remote_status(session=None))

    assert payload["probes"][0]["status"] == "ok"
    assert payload["probes"][0]["remote_node_id"] == NODE_B
    assert calls[0][0] == "vps-a"
    assert calls[0][2] == 10
    assert "cluster_sync_command.py" in calls[0][1]
    assert "venv_pbgui/bin/python" in calls[0][1]
    assert "--allow-join hello" in calls[0][1]


def test_remote_status_reports_uninitialized_remote(monkeypatch, tmp_path: Path) -> None:
    """Remote status classifies an uninitialized remote cluster identity."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            return SimpleNamespace(
                exit_status=1,
                stdout="",
                stderr=json.dumps({"ok": False, "error": "cluster identity is not initialized"}),
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.get_remote_status(session=None))

    assert payload["probes"][0]["status"] == "not_initialized"
    assert payload["probes"][0]["ok"] is False


def test_remote_status_reports_node_mismatch(monkeypatch, tmp_path: Path) -> None:
    """Remote status detects when a host reports a different node identity."""

    root = _init_cluster(tmp_path)
    unexpected_node = "pbgui-node-00000000-0000-4000-8000-000000000099"
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            return SimpleNamespace(
                exit_status=0,
                stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": unexpected_node}),
                stderr="",
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.get_remote_status(session=None))

    assert payload["probes"][0]["status"] == "node_mismatch"
    assert payload["probes"][0]["remote_node_id"] == unexpected_node


def test_remote_join_writes_identity_for_known_node(monkeypatch, tmp_path: Path) -> None:
    """Remote join invokes the restricted wrapper for a known uninitialized node."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[tuple[str, str, int]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command, timeout))
            return SimpleNamespace(
                exit_status=0,
                stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "vps"}),
                stderr="",
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.join_remote_identity(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["remote_node_id"] == NODE_B
    assert calls[0][0] == "vps-a"
    assert calls[0][2] == 15
    assert f"join {CLUSTER_ID} {NODE_B} vps vps-a" in calls[0][1]


def test_remote_join_rejects_foreign_remote_identity(monkeypatch, tmp_path: Path) -> None:
    """Remote join surfaces wrapper rejections without hiding the safety error."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            return SimpleNamespace(
                exit_status=1,
                stdout="",
                stderr=json.dumps({"ok": False, "error": "existing cluster_id differs from requested cluster_id"}),
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(cluster.join_remote_identity(NODE_B, session=None))

    assert exc.value.status_code == 409
    assert "existing cluster_id differs" in exc.value.detail


def test_bootstrap_preview_reports_known_vps_without_local_configs(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap preview includes VPS Manager hosts even when no local bots use them yet."""

    _init_cluster(tmp_path)
    _write_vps_config(tmp_path, "vps-a", ip="203.0.113.10", user="bot", ssh_port="2222")
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    preview = cluster.get_bootstrap_preview(session=None)

    assert preview["counts"]["add"] == 1
    assert preview["can_apply"] is True
    assert preview["items"] == [{
        "type": "node",
        "node_role": "vps",
        "hostname": "vps-a",
        "config_path": str(tmp_path / "data" / "vpsmanager" / "hosts" / "vps-a" / "vps-a.json"),
        "node_id": "",
        "pbname": "vps-a",
        "ssh_host": "203.0.113.10",
        "ssh_user": "bot",
        "ssh_port": 2222,
        "remote_pbgui_dir": "/home/bot/software/pbgui",
        "will_create_node_mapping": True,
        "action": "add",
        "reason": "VPS host is not present in cluster nodes",
    }]


def test_apply_bootstrap_records_known_vps_node(monkeypatch, tmp_path: Path) -> None:
    """Applying bootstrap writes ADD_NODE for known VPS hosts without touching remotes."""

    root = _init_cluster(tmp_path)
    _write_vps_config(tmp_path, "vps-a", ip="203.0.113.10", user="bot", ssh_port=2222)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    result = cluster.apply_bootstrap(session=None)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    node = next(iter(nodes.values()))

    assert result["result"]["counts"]["applied"] == 1
    assert result["after"]["counts"]["skip"] == 1
    assert operations[0]["op"] == "ADD_NODE"
    assert node["role"] == "vps"
    assert node["pbname"] == "vps-a"
    assert node["ssh_host"] == "203.0.113.10"
    assert node["ssh_user"] == "bot"
    assert node["ssh_port"] == 2222
    mapping = _read_json(tmp_path / "data" / "cluster" / "host_node_ids.json")
    assert mapping["hosts"]["vps-a"]["node_id"] == node["node_id"]


def test_apply_bootstrap_uses_monitor_master_role(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap preserves remote master roles reported by VPS Manager monitor metadata."""

    root = _init_cluster(tmp_path)
    _write_vps_config(tmp_path, "remote-master", ip="203.0.113.11", user="bot", ssh_port=2222)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "get_monitor_state_snapshot",
        lambda: {"host_meta": {"remote-master": {"role": "master"}}},
    )

    preview = cluster.get_bootstrap_preview(session=None)
    result = cluster.apply_bootstrap(session=None)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    node = next(iter(nodes.values()))
    mapping = _read_json(tmp_path / "data" / "cluster" / "host_node_ids.json")

    assert preview["items"][0]["node_role"] == "master"
    assert result["result"]["counts"]["applied"] == 1
    assert operations[0]["role"] == "master"
    assert node["role"] == "master"
    assert mapping["hosts"]["remote-master"]["role"] == "master"


def test_bootstrap_preview_skips_registered_vps_node(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap preview skips a VPS host already present in nodes and host mapping."""

    root = _init_cluster(tmp_path)
    _write_vps_config(tmp_path, "vps-a", ip="203.0.113.10", user="bot", ssh_port=2222)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(
        root,
        "ADD_NODE",
        {
            "node_id": NODE_B,
            "role": "vps",
            "pbname": "vps-a",
            "hostname": "vps-a",
            "sync_enabled": True,
            "ssh_host": "203.0.113.10",
            "ssh_user": "bot",
            "ssh_port": 2222,
            "remote_pbgui_dir": "/home/bot/software/pbgui",
        },
        created_at=101,
    )
    (tmp_path / "data" / "cluster" / "host_node_ids.json").write_text(
        json.dumps({"schema_version": 1, "hosts": {"vps-a": {"node_id": NODE_B, "role": "vps"}}}),
        encoding="utf-8",
    )

    preview = cluster.get_bootstrap_preview(session=None)

    assert preview["counts"]["skip"] == 1
    assert preview["can_apply"] is False
    assert preview["items"][0]["action"] == "skip"
    assert preview["items"][0]["reason"] == "VPS node already registered"


def test_bootstrap_preview_preserves_registered_master_without_monitor_role(monkeypatch, tmp_path: Path) -> None:
    """Missing monitor role metadata must not downgrade an existing master node."""

    root = _init_cluster(tmp_path)
    _write_vps_config(tmp_path, "remote-master", ip="203.0.113.11", user="bot", ssh_port=2222)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(cluster, "get_monitor_state_snapshot", lambda: {"host_meta": {}})
    append_operation(
        root,
        "ADD_NODE",
        {
            "node_id": NODE_B,
            "role": "master",
            "pbname": "remote-master",
            "hostname": "remote-master",
            "sync_enabled": True,
            "ssh_host": "203.0.113.11",
            "ssh_user": "bot",
            "ssh_port": 2222,
            "remote_pbgui_dir": "/home/bot/software/pbgui",
        },
        created_at=101,
    )
    (tmp_path / "data" / "cluster" / "host_node_ids.json").write_text(
        json.dumps({"schema_version": 1, "hosts": {"remote-master": {"node_id": NODE_B, "role": "master"}}}),
        encoding="utf-8",
    )

    preview = cluster.get_bootstrap_preview(session=None)

    assert preview["counts"]["skip"] == 1
    assert preview["can_apply"] is False
    assert preview["items"][0]["node_role"] == "master"
    assert preview["items"][0]["action"] == "skip"
    assert preview["items"][0]["reason"] == "VPS node already registered"


def test_bootstrap_preview_ignores_auxiliary_vps_json(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap reads only the main VPS host JSON and ignores pending helper files."""

    _init_cluster(tmp_path)
    config_path = _write_vps_config(tmp_path, "vps-a", ssh_port="not-a-port")
    (config_path.parent / "optional_config_pending.json").write_text(
        json.dumps({"coinmarketcap_api_key": "<api_key>"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    preview = cluster.get_bootstrap_preview(session=None)

    assert len(preview["items"]) == 1
    assert preview["items"][0]["hostname"] == "vps-a"
    assert preview["items"][0]["ssh_port"] == 22


def test_bootstrap_apply_does_not_clear_tombstones(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap skips tombstoned local configs so stale files cannot resurrect deletes."""

    root = _init_cluster(tmp_path)
    _write_v7_config(tmp_path, "test_inst", 5)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    _patch_cluster_config_loader(monkeypatch)
    append_operation(root, "DELETE_INSTANCE", {"instance": "test_inst", "version": "5"}, created_at=101)

    preview = cluster.get_bootstrap_preview(session=None)
    result = cluster.apply_bootstrap(session=None)

    assert preview["counts"]["blocked_tombstone"] == 1
    assert preview["items"][0]["action"] == "blocked_tombstone"
    assert result["result"]["counts"]["applied"] == 0
    assert result["after"]["counts"]["blocked_tombstone"] == 1
