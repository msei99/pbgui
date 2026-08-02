"""Tests for importing reachable Cluster nodes into VPS Manager."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

import vps_manager_core
import vps_manager_service
from api import cluster
from master.cluster_state import (
    append_operation as _append_operation,
    cluster_node_was_removed,
    default_cluster_root,
    ensure_local_identity,
    read_local_identity,
    rebuild_materialized_state,
    write_operation,
)
from vps_manager_service import VPSManagerService


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000011"
NODE_LOCAL = "pbgui-node-00000000-0000-4000-8000-000000000101"
NODE_REMOTE = "pbgui-node-00000000-0000-4000-8000-000000000102"
NODE_OUTBOUND = "pbgui-node-00000000-0000-4000-8000-000000000103"
NODE_DISABLED = "pbgui-node-00000000-0000-4000-8000-000000000104"
NODE_REMOTE_B = "pbgui-node-00000000-0000-4000-8000-000000000105"


def append_operation(root: Path, op: str, payload: dict, **kwargs) -> dict:
    """Use historical v1 records for remote inventory fixtures."""

    identity = read_local_identity(root)
    target = str(payload.get("node_id") or "")
    if op == "ADD_NODE" and target and target != str(identity["node_id"]):
        actor = str(identity["node_id"])
        actor_dir = Path(root) / "oplog" / actor
        seq = max((int(path.stem) for path in actor_dir.glob("*.json")), default=0) + 1
        operation = {
            **payload,
            "schema_version": 1,
            "cluster_id": str(identity["cluster_id"]),
            "op_id": f"{actor}:{seq:08d}",
            "actor": actor,
            "seq": seq,
            "op": op,
            "created_at": int(kwargs.get("created_at", 100 + seq)),
        }
        write_operation(root, operation, allow_legacy_membership=True)
        return operation
    return _append_operation(root, op, payload, **kwargs)


def _prepare_service(monkeypatch, tmp_path: Path) -> tuple[VPSManagerService, dict[str, str]]:
    """Create an isolated VPSManagerService with a small Cluster state."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    monkeypatch.setattr(vps_manager_core, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(vps_manager_service, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monitor_ini = {"enabled_hosts": ""}
    monkeypatch.setattr(vps_manager_core, "load_ini", lambda section, parameter: monitor_ini.get(parameter, ""))
    monkeypatch.setattr(vps_manager_core, "save_ini", lambda section, parameter, value: monitor_ini.__setitem__(parameter, value))
    monkeypatch.setattr(vps_manager_service, "load_ini", lambda section, parameter: monitor_ini.get(parameter, ""))
    monkeypatch.setattr(vps_manager_service, "save_ini", lambda section, parameter, value: monitor_ini.__setitem__(parameter, value))
    monkeypatch.setattr(vps_manager_service, "_hosts_entry_status", lambda hostname, ip: {"ok": True})
    monkeypatch.setattr(vps_manager_core.VPS, "fetch_vps_info", lambda self: {})
    monkeypatch.setattr(vps_manager_core.VPS, "fetch_ufw_settings", lambda self: (False, []))
    monkeypatch.setattr(VPSManagerService, "_test_import_key_login", lambda self, **kwargs: (True, "ok"))

    root = default_cluster_root(tmp_path)
    ensure_local_identity(
        root,
        role="master",
        pbname="second-master",
        cluster_id=CLUSTER_ID,
        node_id=NODE_LOCAL,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_LOCAL, "role": "master", "pbname": "second-master", "sync_mode": "outbound_only"}, created_at=101)
    append_operation(root, "ADD_NODE", {
        "node_id": NODE_REMOTE,
        "role": "vps",
        "pbname": "runner-a",
        "hostname": "runner-a",
        "sync_mode": "reachable",
        "sync_enabled": True,
        "ssh_host": "203.0.113.20",
        "ssh_user": "pbuser",
        "ssh_port": 2222,
        "remote_pbgui_dir": "software/pbgui",
    }, created_at=102)
    append_operation(root, "ADD_NODE", {"node_id": NODE_OUTBOUND, "role": "master", "pbname": "remote-master", "sync_mode": "outbound_only"}, created_at=103)
    append_operation(root, "ADD_NODE", {"node_id": NODE_DISABLED, "role": "vps", "hostname": "disabled-vps", "sync_mode": "disabled"}, created_at=104)
    append_operation(root, "ADD_NODE", {
        "node_id": NODE_REMOTE_B,
        "role": "vps",
        "pbname": "runner-b",
        "hostname": "runner-b",
        "sync_mode": "disabled",
        "ssh_host": "203.0.113.21",
        "ssh_user": "pbuser",
        "ssh_port": 22,
        "remote_pbgui_dir": "software/pbgui-b",
    }, created_at=105)
    rebuild_materialized_state(root)
    return VPSManagerService(), monitor_ini


def _write_successful_vps(service: VPSManagerService, hostname: str) -> vps_manager_core.VPS:
    """Create one setup-complete VPS Manager host entry."""

    vps = vps_manager_core.VPS()
    vps.hostname = hostname
    vps.ip = "203.0.113.40"
    vps.user = "bot"
    vps.remote_pbgui_dir = "/home/bot/software/pbgui"
    vps.firewall_ssh_port = 2222
    vps.setup_status = "successful"
    vps.save()
    service.vpsmanager.vpss.append(vps)
    service.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")
    return vps


def test_add_vps_to_cluster_completes_remote_join(monkeypatch, tmp_path: Path) -> None:
    """VPS Manager fully configures and joins one setup-complete VPS."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)
    _write_successful_vps(service, "new-runner")
    calls: list[str] = []

    async def fake_repair(node, identity, nodes, *, ssh_passwords=None):
        calls.append("repair")
        assert node["sync_mode"] == "reachable"
        assert node["ssh_host"] == "203.0.113.40"
        return {"ok": True, "node_id": node["node_id"]}

    async def fake_probe(node, identity):
        calls.append("probe")
        return {"ok": False, "status": "not_initialized", "node_id": node["node_id"]}

    async def fake_join(node, identity, *, progress_callback=None):
        calls.append("join")
        return {
            "ok": True,
            "node_id": node["node_id"],
            "completion": {"ok": True, "pbrun_start": {"attempted": True, "started": True}},
        }

    monkeypatch.setattr(cluster, "_repair_node_cluster_ssh", fake_repair)
    monkeypatch.setattr(cluster, "_probe_cluster_node", fake_probe)
    monkeypatch.setattr(cluster, "_run_remote_join", fake_join)
    monkeypatch.setattr(cluster, "_request_pbcluster_sync", lambda root: calls.append("sync"))

    result = asyncio.run(service.add_vps_to_cluster("session", "new-runner"))
    materialized = rebuild_materialized_state(default_cluster_root(tmp_path))
    nodes = materialized["cluster_nodes"]["nodes"]
    new_node = next(node for node in nodes.values() if node.get("pbname") == "new-runner")

    assert result["cluster"]["ok"] is True
    assert result["cluster_node"]["registered"] is True
    assert result["cluster_node"]["action"] == "join"
    assert result["cluster"]["node_id"] == new_node["node_id"]
    assert calls == ["repair", "probe", "join", "sync"]
    assert new_node["sync_mode"] == "reachable"
    assert new_node["ssh_host"] == "203.0.113.40"
    assert new_node["ssh_user"] == "bot"
    assert new_node["ssh_port"] == 2222


def test_successful_setup_finished_auto_adds_vps_to_cluster(monkeypatch, tmp_path: Path) -> None:
    """The VPS setup completion callback registers the host in local Cluster metadata."""

    service, monitor_ini = _prepare_service(monkeypatch, tmp_path)
    vps = _write_successful_vps(service, "auto-runner")

    vps.setup_finished()
    materialized = rebuild_materialized_state(default_cluster_root(tmp_path))
    nodes = materialized["cluster_nodes"]["nodes"]

    assert any(node.get("pbname") == "auto-runner" for node in nodes.values())
    assert service._cluster_node_status("auto-runner")["action"] == "join"
    assert monitor_ini["enabled_hosts"] == "auto-runner"


def test_add_vps_to_cluster_rejects_conflicting_remote_identity(monkeypatch, tmp_path: Path) -> None:
    """One-click onboarding never overwrites a foreign remote identity."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)
    _write_successful_vps(service, "foreign-runner")

    async def fake_repair(node, identity, nodes, *, ssh_passwords=None):
        return {"ok": True, "node_id": node["node_id"]}

    async def fake_probe(node, identity):
        return {
            "ok": False,
            "status": "foreign_cluster",
            "remote_cluster_id": "pbgui-cluster-00000000-0000-4000-8000-000000000099",
        }

    monkeypatch.setattr(cluster, "_repair_node_cluster_ssh", fake_repair)
    monkeypatch.setattr(cluster, "_probe_cluster_node", fake_probe)

    with pytest.raises(ValueError, match="Cannot join foreign-runner: foreign_cluster"):
        asyncio.run(service.add_vps_to_cluster("session", "foreign-runner"))


def test_add_vps_to_cluster_uses_authenticated_v2_capability_when_resuming(monkeypatch, tmp_path: Path) -> None:
    """A resumed join does not treat a fresh v2 node as a protocol downgrade."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)
    _write_successful_vps(service, "resumed-runner")
    node_id = ""

    async def fake_repair(node, identity, nodes, *, ssh_passwords=None):
        nonlocal node_id
        node_id = str(node["node_id"])
        return {"ok": True, "node_id": node_id}

    async def fake_probe(node, identity):
        return {
            "ok": True,
            "status": "ok",
            "node_id": node["node_id"],
            "remote_cluster_id": identity["cluster_id"],
            "remote_node_id": node["node_id"],
            "credential_protocol_version": 2,
        }

    async def fake_complete(node, identity, *, progress_callback=None):
        assert node["credential_protocol_version"] == 2
        assert node["state_replica"] is True
        return {"ok": True, "pbrun_start": {"attempted": True, "started": True}}

    monkeypatch.setattr(cluster, "_repair_node_cluster_ssh", fake_repair)
    monkeypatch.setattr(cluster, "_probe_cluster_node", fake_probe)
    monkeypatch.setattr(cluster, "_complete_remote_join_sync", fake_complete)
    monkeypatch.setattr(cluster, "_request_pbcluster_sync", lambda root: None)

    result = asyncio.run(service.add_vps_to_cluster("session", "resumed-runner"))
    materialized = rebuild_materialized_state(default_cluster_root(tmp_path), write=False)

    assert result["cluster"]["node_id"] == node_id
    assert result["cluster"]["join"]["already_joined"] is True
    assert materialized["cluster_nodes"]["nodes"][node_id]["state_replica"] is True


def test_reinstalled_hostname_gets_new_node_id_after_old_node_was_removed(monkeypatch, tmp_path: Path) -> None:
    """Reusing a hostname never reuses the immutable ID of its removed predecessor."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)
    root = default_cluster_root(tmp_path)
    old_node_id = "pbgui-node-00000000-0000-4000-8000-000000000199"
    append_operation(root, "ADD_NODE", {
        "node_id": old_node_id,
        "role": "vps",
        "pbname": "reinstalled-runner",
        "hostname": "reinstalled-runner",
        "sync_mode": "reachable",
        "ssh_host": "203.0.113.39",
    }, created_at=106)
    _append_operation(root, "REMOVE_NODE", {"node_id": old_node_id}, created_at=107)
    (root / "host_node_ids.json").write_text(json.dumps({
        "schema_version": 1,
        "hosts": {"reinstalled-runner": {"node_id": old_node_id, "created_at": 106, "role": "vps"}},
    }), encoding="utf-8")
    rebuild_materialized_state(root)
    vps = _write_successful_vps(service, "reinstalled-runner")

    vps.setup_finished()

    materialized = rebuild_materialized_state(root)
    replacements = [
        (node_id, node)
        for node_id, node in materialized["cluster_nodes"]["nodes"].items()
        if node.get("pbname") == "reinstalled-runner"
    ]
    mapping = json.loads((root / "host_node_ids.json").read_text(encoding="utf-8"))["hosts"]["reinstalled-runner"]
    assert cluster_node_was_removed(root, old_node_id) is True
    assert len(replacements) == 1
    assert replacements[0][0] != old_node_id
    assert replacements[0][1]["state_replica"] is False
    assert mapping["node_id"] == replacements[0][0]


def test_setup_finished_persists_success_before_optional_registration(monkeypatch, tmp_path: Path) -> None:
    """Optional monitor or Cluster failures cannot erase a successful setup result."""
    monkeypatch.setattr(vps_manager_core, "PBGDIR", tmp_path)
    vps = vps_manager_core.VPS()
    vps.hostname = "new-runner"
    vps.setup_status = "starting"
    vps.save()
    vps.setup_status = "successful"
    attempted: list[str] = []

    def fail_monitor(hostname: str, *, enabled: bool) -> None:
        """Simulate an optional monitor-registration failure."""
        attempted.append(f"monitor:{hostname}:{enabled}")
        raise RuntimeError("monitor unavailable")

    def fail_cluster(hostname: str) -> dict:
        """Simulate an optional Cluster-registration failure."""
        attempted.append(f"cluster:{hostname}")
        raise RuntimeError("cluster unavailable")

    monkeypatch.setattr(vps_manager_core, "_set_vps_monitor_enabled", fail_monitor)
    monkeypatch.setattr(vps_manager_core, "_register_vps_cluster_node", fail_cluster)

    vps.setup_finished()

    loaded = vps_manager_core.VPS()
    loaded.load(tmp_path / "data" / "vpsmanager" / "hosts" / "new-runner" / "new-runner.json")
    assert loaded.setup_status == "successful"
    assert loaded.last_setup
    assert attempted == ["monitor:new-runner:True", "cluster:new-runner"]


def test_cluster_nodes_import_preview_nodes_with_ssh_metadata(monkeypatch, tmp_path: Path) -> None:
    """Preview imports non-local Cluster nodes with SSH metadata."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)

    preview = service.preview_cluster_nodes_import()

    assert preview["can_apply"] is True
    assert preview["counts"] == {"add": 2, "update": 0, "skip": 3, "error": 0, "hosts_update": 0}
    by_host = {item["hostname"]: item for item in preview["items"]}
    assert by_host["runner-a"]["action"] == "add"
    assert by_host["runner-a"]["ssh_host"] == "203.0.113.20"
    assert by_host["runner-b"]["action"] == "add"
    assert by_host["remote-master"]["action"] == "skip"
    assert any(item["node_id"] == NODE_DISABLED and item["action"] == "skip" for item in preview["items"])
    assert not (tmp_path / "data" / "vpsmanager" / "hosts" / "runner-a" / "runner-a.json").exists()


def test_cluster_nodes_import_writes_only_rows_with_passwords(monkeypatch, tmp_path: Path) -> None:
    """Apply imports only rows that received a VPS user password."""

    service, monitor_ini = _prepare_service(monkeypatch, tmp_path)

    result = service.import_cluster_nodes("test-token", {"passwords": {"runner-a": "secret-pw"}})

    assert result["counts"] == {"imported": 1, "skipped": 4, "hosts_updated": 0, "settings_refreshed": 1, "monitoring_ready": 1}
    host_config = tmp_path / "data" / "vpsmanager" / "hosts" / "runner-a" / "runner-a.json"
    payload = json.loads(host_config.read_text(encoding="utf-8"))
    assert payload["_hostname"] == "runner-a"
    assert payload["ip"] == "203.0.113.20"
    assert payload["user"] == "pbuser"
    assert payload["firewall_ssh_port"] == 2222
    assert payload["remote_pbgui_dir"] == "software/pbgui"
    assert "coinmarketcap_api_key" not in payload
    assert "user_pw" not in payload
    assert "root_pw" not in payload
    assert "user_sudo_pw" not in payload
    assert not (tmp_path / "data" / "vpsmanager" / "hosts" / "runner-b" / "runner-b.json").exists()
    assert any(item["hostname"] == "runner-b" and item["reason"] == "No VPS user password entered." for item in result["skipped"])
    assert monitor_ini["enabled_hosts"] == "runner-a"


def test_cluster_nodes_import_refreshes_monitor_connection(monkeypatch, tmp_path: Path) -> None:
    """Apply asks the running monitor to reconnect hosts after key setup succeeds."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)
    refreshed: list[str] = []
    monkeypatch.setattr(VPSManagerService, "_refresh_vps_monitor_connection", lambda self, hostname: refreshed.append(hostname))

    service.import_cluster_nodes("test-token", {"passwords": {"runner-a": "secret-pw"}})

    assert refreshed == ["runner-a"]


def test_cluster_nodes_import_job_reports_progress(monkeypatch, tmp_path: Path) -> None:
    """Background Cluster node import exposes real progress events while it runs."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)

    started = service.start_cluster_nodes_import("test-token", {"passwords": {"runner-a": "secret-pw"}})
    assert started["job_id"]

    progress = started
    for _ in range(100):
        progress = service.get_cluster_nodes_import_progress(started["job_id"])
        if progress["status"] in {"successful", "error"}:
            break
        time.sleep(0.02)

    assert progress["status"] == "successful"
    assert progress["percent"] == 100
    assert progress["done"] == progress["total"]
    assert progress["result"]["counts"]["imported"] == 1
    labels = [str(item.get("label") or "") for item in progress["events"]]
    assert any("Prepared 1 selected Cluster node" in label for label in labels)
    assert any("Refreshing remote settings" in label for label in labels)
    assert any("Saved VPS Manager host entry" in label for label in labels)
