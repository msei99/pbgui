"""Tests for importing reachable Cluster nodes into VPS Manager."""

from __future__ import annotations

import json
from pathlib import Path

import vps_manager_core
import vps_manager_service
from master.cluster_state import append_operation, default_cluster_root, ensure_local_identity, rebuild_materialized_state
from vps_manager_service import VPSManagerService


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000011"
NODE_LOCAL = "pbgui-node-00000000-0000-4000-8000-000000000101"
NODE_REMOTE = "pbgui-node-00000000-0000-4000-8000-000000000102"
NODE_OUTBOUND = "pbgui-node-00000000-0000-4000-8000-000000000103"
NODE_DISABLED = "pbgui-node-00000000-0000-4000-8000-000000000104"


def _prepare_service(monkeypatch, tmp_path: Path) -> tuple[VPSManagerService, dict[str, str]]:
    """Create an isolated VPSManagerService with a small Cluster state."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    monkeypatch.setattr(vps_manager_core, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(vps_manager_service, "PBGDIR", str(tmp_path))
    monitor_ini = {"enabled_hosts": ""}
    monkeypatch.setattr(vps_manager_service, "load_ini", lambda section, parameter: monitor_ini.get(parameter, ""))
    monkeypatch.setattr(vps_manager_service, "save_ini", lambda section, parameter, value: monitor_ini.__setitem__(parameter, value))

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
    append_operation(root, "ADD_NODE", {"node_id": NODE_DISABLED, "role": "vps", "sync_mode": "disabled"}, created_at=104)
    rebuild_materialized_state(root)
    return VPSManagerService(), monitor_ini


def test_cluster_nodes_import_preview_only_reachable_nodes(monkeypatch, tmp_path: Path) -> None:
    """Preview imports reachable Cluster nodes and skips local/outbound nodes."""

    service, _monitor_ini = _prepare_service(monkeypatch, tmp_path)

    preview = service.preview_cluster_nodes_import()

    assert preview["can_apply"] is True
    assert preview["counts"] == {"add": 1, "update": 0, "skip": 3, "error": 0}
    by_host = {item["hostname"]: item for item in preview["items"]}
    assert by_host["runner-a"]["action"] == "add"
    assert by_host["runner-a"]["ssh_host"] == "203.0.113.20"
    assert by_host["remote-master"]["action"] == "skip"
    assert any(item["node_id"] == NODE_DISABLED and item["action"] == "skip" for item in preview["items"])
    assert not (tmp_path / "data" / "vpsmanager" / "hosts" / "runner-a" / "runner-a.json").exists()


def test_cluster_nodes_import_writes_safe_vps_metadata(monkeypatch, tmp_path: Path) -> None:
    """Apply writes safe VPS Manager metadata without credentials or optional secrets."""

    service, monitor_ini = _prepare_service(monkeypatch, tmp_path)

    result = service.import_cluster_nodes("test-token")

    assert result["counts"] == {"imported": 1, "skipped": 3}
    host_config = tmp_path / "data" / "vpsmanager" / "hosts" / "runner-a" / "runner-a.json"
    payload = json.loads(host_config.read_text(encoding="utf-8"))
    assert payload["_hostname"] == "runner-a"
    assert payload["ip"] == "203.0.113.20"
    assert payload["user"] == "pbuser"
    assert payload["firewall_ssh_port"] == 2222
    assert payload["remote_pbgui_dir"] == "software/pbgui"
    assert payload["coinmarketcap_api_key"] is None
    assert "user_pw" not in payload
    assert "root_pw" not in payload
    assert "user_sudo_pw" not in payload
    assert monitor_ini["enabled_hosts"] == "runner-a"
