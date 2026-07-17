"""Tests for the read-only Cluster Sync API helpers."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import stat
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from api import cluster
from api import v7_instances
from cluster_credential_publisher import ClusterCredentialPublisher
from cluster_credentials import ensure_node_key_material, sign_operation
from credential_store import CredentialStore
from master.cluster_state import (
    append_operation as _append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    ensure_local_identity,
    load_operations,
    read_local_identity,
    write_operation,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
OTHER_CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000099"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
NODE_C = "pbgui-node-00000000-0000-4000-8000-00000000000c"
HASH_A = "sha256:" + "a" * 64
LOCAL_CLUSTER_PUBLIC_KEY = "ssh-ed25519 aGVsbG8= pbgui-cluster:local"
REMOTE_CLUSTER_PUBLIC_KEY = "ssh-ed25519 d29ybGQ= pbgui-cluster:remote"


def append_operation(root: Path, op: str, payload: dict, **kwargs) -> dict:
    """Keep unrelated API fixtures on historical v1 remote membership records."""

    identity = read_local_identity(root)
    actor = str(identity["node_id"])
    target = str(payload.get("node_id") or "")
    if op == "ADD_NODE" and target and target != actor:
        actor_dir = Path(root) / "oplog" / actor
        existing = [int(path.stem) for path in actor_dir.glob("*.json")] if actor_dir.exists() else []
        seq = max(existing, default=0) + 1
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


def _signed_bootstrap_operation(key_root: Path, **metadata) -> tuple[dict, Any]:
    """Build a signed upstream bootstrap membership operation for join tests."""

    keys = ensure_node_key_material(key_root)
    bundle = keys.public_bundle(NODE_B, "master")
    operation = {
        **bundle,
        **metadata,
        "schema_version": 1,
        "cluster_id": CLUSTER_ID,
        "op_id": f"{NODE_B}:00000001",
        "actor": NODE_B,
        "seq": 1,
        "op": "ADD_NODE",
        "created_at": 101,
        "node_id": NODE_B,
        "role": "master",
        "state_replica": True,
        "membership_authorization": {"kind": "bootstrap"},
    }
    return sign_operation(operation, keys.signing_private_key, signer_id=NODE_B), keys


def _signed_join_authorization(keys: Any, node_id: str) -> dict:
    """Build the explicit upstream approval returned by join-hello fixtures."""

    return sign_operation(
        {
            "kind": "join",
            "cluster_id": CLUSTER_ID,
            "node_id": node_id,
            "role": "master",
            "created_at": 102,
        },
        keys.signing_private_key,
        signer_id=NODE_B,
    )


def test_pulled_secret_blob_uses_owner_only_tree(tmp_path: Path) -> None:
    """API-pulled secret blobs are private from directory creation onward."""
    base = tmp_path / "secret_blobs"
    raw = b'{"secret":"value"}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    cluster._write_cluster_blob(base, blob_hash, raw, secret=True)

    digest = blob_hash.removeprefix("sha256:")
    path = base / "sha256" / digest[:2] / f"{digest}.json"
    assert path.read_bytes() == raw
    assert stat.S_IMODE(base.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


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


def test_self_join_discovers_remote_pbgui_dir_like_vps_manager() -> None:
    """Self-join probes the same remote PBGui path candidates as VPS Manager."""

    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "bad/pbgui" in command:
                return SimpleNamespace(exit_status=1, stdout="", stderr="missing")
            if "software/pbgui" in command:
                return SimpleNamespace(exit_status=0, stdout="", stderr="")
            return SimpleNamespace(exit_status=1, stdout="", stderr="unexpected command")

    result = asyncio.run(cluster._discover_remote_pbgui_dir_for_self_join(FakePool(), "upstream-master", "bad/pbgui"))

    assert result["remote_pbgui_dir"] == "software/pbgui"
    assert result["candidates"] == ["bad/pbgui", "software/pbgui", "pbgui"]
    assert len(calls) == 2
    assert "bad/pbgui" in calls[0]
    assert "software/pbgui" in calls[1]


def _reachable_vps_payload(**overrides) -> dict:
    """Return a node payload that is explicitly reachable over SSH."""

    payload = {
        "node_id": NODE_B,
        "role": "vps",
        "pbname": "vps-a",
        "sync_mode": "reachable",
        "sync_enabled": True,
        "ssh_host": "vps-a",
    }
    payload.update(overrides)
    return payload


class _JsonRequest:
    """Minimal async request object for direct route-handler tests."""

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    async def json(self) -> dict:
        """Return the configured request payload."""

        return self._payload


def _write_cluster_blob(root: Path, base: str, raw: bytes) -> str:
    """Write one content-addressed cluster blob and return its sha256 hash."""

    digest = hashlib.sha256(raw).hexdigest()
    path = root / base / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return f"sha256:{digest}"


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


def test_retention_policy_is_signed_cluster_state_with_report_only_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The API publishes retention through signed CAS state and defaults safely."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    assert cluster.get_status(session=None)["retention_policy"] == {
        "generation": 0,
        "mode": "report_only",
        "history_days": 7,
        "conflicted": False,
    }

    result = cluster.save_retention_settings(
        cluster.ClusterRetentionSettingsIn(
            mode="oplog",
            history_days=30,
            expected_generation=0,
        ),
        session=None,
    )

    assert result["changed"] is True
    assert result["policy"]["mode"] == "oplog"
    assert result["policy"]["history_days"] == 30
    operation = load_operations(root)[-1]
    assert operation["op"] == "SET_RETENTION_POLICY"
    assert operation["signature_algorithm"] == "Ed25519"
    with pytest.raises(HTTPException) as exc_info:
        cluster.save_retention_settings(
            cluster.ClusterRetentionSettingsIn(
                mode="report_only",
                history_days=7,
                expected_generation=0,
            ),
            session=None,
        )
    assert exc_info.value.status_code == 409


def test_retention_report_is_read_only_and_main_page_exposes_no_session_token(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Reports do not mutate history and the browser relies on its HttpOnly cookie."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    before = sorted(path.read_bytes() for path in (root / "oplog").glob("*/*.json"))

    report = asyncio.run(cluster.get_retention_report(session=None))
    response = cluster.get_main_page(
        SimpleNamespace(url=SimpleNamespace(scheme="https", hostname="localhost", port=None)),
        session=SimpleNamespace(token="must-not-appear"),
    )

    assert report["read_only"] is True
    assert report["policy"]["mode"] == "report_only"
    assert sorted(path.read_bytes() for path in (root / "oplog").glob("*/*.json")) == before
    html = response.body.decode("utf-8")
    assert "must-not-appear" not in html
    assert "%%TOKEN%%" not in html
    assert "headers.Authorization" not in html


def test_retention_report_builds_remote_preview_command(monkeypatch, tmp_path: Path) -> None:
    """Retention reports allow the read-only remote preview command."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=102)

    async def remote_read(node: dict, identity: dict, verb: str) -> dict:
        command = cluster._cluster_state_read_command("software/pbgui", identity["node_id"], verb)
        assert "retention-preview" in command
        return {
            "status": "report_only",
            "eligible_operations": 0,
            "eligible_bytes": 0,
            "items": [],
        }

    monkeypatch.setattr(cluster, "_run_remote_read_command", remote_read)

    report = asyncio.run(cluster.get_retention_report(session=None))

    assert report["counts"]["nodes_total"] == 2
    assert report["counts"]["nodes_reported"] == 2
    assert report["counts"]["nodes_unavailable"] == 0


def test_get_status_includes_pbcluster_sync_status(monkeypatch, tmp_path: Path) -> None:
    """Cluster status exposes the latest PBCluster peer sync result for UI key-login checks."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    (root / "sync_status.json").write_text(
        json.dumps(
            {
                "ok": True,
                "status": "local_reconciled",
                "finished_at": 123,
                "peers_ok": 1,
                "peers_total": 2,
                "peers": [
                    {
                        "node_id": NODE_B,
                        "pbname": "vps-a",
                        "ok": False,
                        "status": "error",
                        "reason": "mani@192.0.2.10: Permission denied (publickey,password).",
                        "retry_delay": 30,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    status = cluster.get_status(session=None)

    assert status["sync_status"]["status"] == "local_reconciled"
    assert status["sync_status"]["peers_ok"] == 1
    assert status["sync_status"]["peers_total"] == 2
    assert status["sync_status"]["peers"][0] == {
        "node_id": NODE_B,
        "pbname": "vps-a",
        "ok": False,
        "status": "error",
        "reason": "mani@192.0.2.10: Permission denied (publickey,password).",
        "remote_node_id": "",
        "last_seen": 0,
        "next_retry": 0,
        "retry_delay": 30,
    }


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


def test_get_nodes_defaults_local_connection_metadata(monkeypatch, tmp_path: Path) -> None:
    """The nodes API exposes local checkout, IP and user defaults when membership omits them."""

    home = tmp_path / "home" / "mani"
    pbgui_dir = home / "test" / "pbgui"
    pbgui_dir.mkdir(parents=True)
    root = _init_cluster(pbgui_dir)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(cluster, "PBGDIR", str(pbgui_dir))
    monkeypatch.setattr(cluster, "_local_ssh_host_value", lambda *args, **kwargs: "10.9.1.31")
    monkeypatch.setattr(cluster, "_local_ssh_user_value", lambda: "mani")
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {"ok": True, "fingerprint": "SHA256:local", "public_key_path": str(pbgui_dir / "cluster.pub")},
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)

    payload = cluster.get_nodes(session=None)
    local_node = next(item for item in payload["nodes"] if item["node_id"] == NODE_A)

    assert local_node["remote_pbgui_dir"] == "test/pbgui"
    assert local_node["ssh_host"] == "10.9.1.31"
    assert local_node["ssh_user"] == "mani"


def test_credential_status_endpoints_expose_ids_but_no_keys_ciphertext_or_secrets(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Cluster status surfaces lifecycle metadata without raw cryptographic material."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {"ok": True, "fingerprint": "SHA256:local"},
    )
    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc("never-return-this-cluster-secret")
    ClusterCredentialPublisher(root, store).publish_cmc(record["id"])

    status = cluster.get_status(session=None)
    nodes = cluster.get_nodes(session=None)
    desired = cluster.get_desired_state(session=None)
    rendered = json.dumps({"status": status, "nodes": nodes, "desired": desired})

    assert status["credentials"]["protocol_version"] == 2
    assert status["credentials"]["cmc"]["catalog"][record["id"]]["catalog_generation"] == 1
    assert status["credentials"]["cmc"]["catalog"][record["id"]]["recipient_generation"] == 1
    assert status["credentials"]["authorities"][0]["authority_node_id"] == NODE_A
    assert status["credentials"]["authorities"][0]["authority_epoch"] == 1
    assert status["credentials"]["migration"]["cutoff_status"] == "not_set"
    assert status["credentials"]["migration"]["unfreeze_status"] == "not_started"
    assert nodes["nodes"][0]["encryption_key_id"].startswith("x25519:")
    assert "signing_public_key" not in rendered
    assert "encryption_public_key" not in rendered
    assert "cluster_ssh_public_key" not in rendered
    assert "ciphertext" not in rendered
    assert "never-return-this-cluster-secret" not in rendered


def test_rewrap_endpoint_materializes_and_acknowledges_exact_local_state(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The explicit rewrap route completes local wrapper verification and ACK."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc("route-rewrap-secret")
    publisher = ClusterCredentialPublisher(root, store)
    publisher.publish_cmc(record["id"])
    before = cluster.get_status(session=None)["credentials"]
    assert before["nodes"][NODE_A]["credential_active"] is False

    response = asyncio.run(
        cluster.rewrap_cluster_credentials(
            _JsonRequest({"operation_id": "rewrap-idempotent-1"}),
            session=None,
        )
    )
    repeated = asyncio.run(
        cluster.rewrap_cluster_credentials(
            _JsonRequest({"operation_id": "rewrap-idempotent-1"}),
            session=None,
        )
    )

    assert response["ok"] is True
    assert repeated == response
    assert response["operation_id"]
    assert response["result"]["status"] == "current"
    assert response["materialization"]["migration_ack"]["recipient_generations"] == {
        record["id"]: 1
    }
    assert response["credentials"]["nodes"][NODE_A]["credential_active"] is True
    assert "route-rewrap-secret" not in json.dumps(response)
    assert cluster.get_cluster_credential_operation(
        "rewrap-idempotent-1",
        session=None,
    )["status"] == "complete"


def test_local_key_rotation_retry_cannot_rotate_twice(monkeypatch, tmp_path: Path) -> None:
    """A repeated client operation ID returns the first durable key rotation result."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    first = cluster.rotate_local_cluster_credential_key(
        session=None,
        operation_id="key-rotation-idempotent-1",
    )
    second = cluster.rotate_local_cluster_credential_key(
        session=None,
        operation_id="key-rotation-idempotent-1",
    )
    rotations = [operation for operation in load_operations(root) if operation["op"] == "UPDATE_NODE_KEY"]

    assert second == first
    assert len(rotations) == 1
    assert first["result"]["rotation"]["signing_key_id"] == rotations[0]["signing_key_id"]


def test_credential_status_names_missing_migration_nodes_and_unfreeze_state() -> None:
    """Migration diagnostics identify each missing active node without credential values."""

    node_a = {
        "role": "master",
        "credential_protocol_version": 2,
        "credential_capable": True,
        "signing_key_id": "ed25519:a",
        "encryption_key_id": "x25519:a",
    }
    node_b = {
        "role": "vps",
        "credential_protocol_version": 2,
        "credential_capable": True,
        "signing_key_id": "ed25519:b",
        "encryption_key_id": "x25519:b",
    }
    snapshot = {
        "cluster_nodes": {
            "credential_membership_generation": 2,
            "nodes": {NODE_A: node_a, NODE_B: node_b},
        },
        "desired_state": {
            "tradfi_active_profiles": {
                "tiingo": {"profile_id": "tradfi-id", "activation_generation": 2},
            },
            "tradfi_projection_acks": {
                NODE_A: {
                    "membership_generation": 2,
                    "active_profile_generations": {"tiingo": 2},
                    "projection_status": "current",
                    "projection_applied_generation": 2,
                },
            },
            "credential_migration": {
                "frozen": True,
                "freeze_generation": 3,
                "freeze_acks": {NODE_A: {}},
                "materialization_acks": {},
                "cutoff": {"cutoff_generation": 4, "min_protocol": 2},
                "cleanup_acks": {NODE_A: {}},
                "scan_acks": {NODE_A: {
                    "freeze_generation": 3,
                    "cutoff_generation": 4,
                    "status": "clean",
                    "findings": [],
                }},
                "candidates": {
                    "candidate-a": {
                        "candidate_id": "candidate-a",
                        "candidate_kind": "tradfi_profile",
                        "submitted_by": NODE_A,
                    },
                },
                "candidate_acceptances": {
                    "candidate-a": {"candidate_id": "candidate-a", "status": "accepted"},
                },
            }
        },
    }

    status = cluster._credential_status(snapshot)

    migration = status["migration"]
    assert migration["missing_node_ids"]["freeze_acks"] == [NODE_B]
    assert migration["missing_node_ids"]["materialization_acks"] == [NODE_A, NODE_B]
    assert migration["missing_node_ids"]["cleanup_acks"] == [NODE_B]
    assert migration["missing_node_ids"]["scan_acks"] == [NODE_B]
    assert migration["cutoff_status"] == "active"
    assert migration["unfreeze_status"] == "pending"
    assert {item["reason"] for item in migration["blocker_details"]} == {
        "freeze_acks",
        "materialization_acks",
        "cleanup_acks",
        "scan_acks",
    }
    assert status["nodes"][NODE_A]["migration_candidate"]["status"] == "accepted"
    assert status["nodes"][NODE_A]["migration_scan_ack"]["current"] is True
    assert status["nodes"][NODE_A]["tradfi_projection_ack"]["current"] is True
    assert status["nodes"][NODE_B]["migration_scan_ack"]["status"] == "pending"
    assert "scan_ack" in status["nodes"][NODE_B]["migration_blockers"]


def test_credential_status_keeps_completed_scan_current_after_unfreeze() -> None:
    """Unfreeze advances the generation without invalidating its clean scan ACK."""

    snapshot = {
        "cluster_nodes": {
            "credential_membership_generation": 1,
            "nodes": {
                NODE_A: {
                    "role": "master",
                    "credential_protocol_version": 2,
                    "credential_capable": True,
                    "signing_key_id": "ed25519:a",
                    "encryption_key_id": "x25519:a",
                },
            },
        },
        "desired_state": {
            "credential_migration": {
                "frozen": False,
                "freeze_generation": 4,
                "cutoff": {"cutoff_generation": 2, "min_protocol": 2},
                "cleanup_acks": {NODE_A: {}},
                "scan_acks": {
                    NODE_A: {
                        "freeze_generation": 3,
                        "cutoff_generation": 2,
                        "status": "clean",
                        "findings": [],
                    },
                },
            },
        },
    }

    status = cluster._credential_status(snapshot)

    assert status["migration"]["status"] == "complete"
    assert status["migration"]["unfreeze_status"] == "complete"
    assert status["migration"]["blocked"] is False
    assert status["nodes"][NODE_A]["migration_scan_ack"]["current"] is True
    assert status["nodes"][NODE_A]["migration_blockers"] == []


def test_credential_status_reports_only_service_names_for_local_process_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rolling status exposes blocked service names without PID or command metadata."""
    import credential_process_registry

    monkeypatch.setattr(cluster, "read_local_identity", lambda _root: {"node_id": NODE_A})
    monkeypatch.setattr(
        credential_process_registry,
        "process_barrier_readiness",
        lambda _root: {
            "ready": False,
            "services": [],
            "waiting_services": ["PBApiServer", "PBCoinData"],
        },
    )
    snapshot = {
        "cluster_nodes": {
            "credential_membership_generation": 1,
            "nodes": {NODE_A: {
                "role": "master",
                "credential_protocol_version": 2,
                "credential_capable": True,
                "signing_key_id": "ed25519:a",
                "encryption_key_id": "x25519:a",
            }},
        },
        "desired_state": {"credential_migration": {}},
    }

    migration = cluster._credential_status(snapshot)["migration"]

    assert migration["status"] == "waiting_for_upgrade"
    assert migration["outdated_services"] == ["PBApiServer", "PBCoinData"]
    assert all("pid" not in str(item).lower() for item in migration["blocker_details"])


def test_remote_credential_materialization_requires_eligible_pending_node(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Remote credential materialization is guarded and returns a server operation ID."""

    remote = {
        "node_id": NODE_B,
        "role": "vps",
        "enabled": True,
        "state_replica": True,
        "sync_mode": "reachable",
        "credential_protocol_version": 2,
        "credential_capable": True,
        "signing_key_id": "ed25519:remote",
        "encryption_key_id": "x25519:remote",
        "pbname": "runner-b",
    }
    snapshot = {
        "identity": {"node_id": NODE_A},
        "cluster_nodes": {"nodes": {NODE_A: {"node_id": NODE_A}, NODE_B: remote}},
        "desired_state": {},
    }
    calls: list[str] = []
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    async def require_current(*_args, **_kwargs) -> None:
        return None

    async def materialize(_node, _identity, verb, **_kwargs) -> dict:
        calls.append(verb)
        return {"ok": True, "counts": {"written": 1}}

    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: snapshot)
    monkeypatch.setattr(
        cluster,
        "_credential_status",
        lambda _snapshot: {
            "cmc": {"count": 1},
            "tradfi": {"count": 0},
            "nodes": {NODE_B: {"materialization_ack": {"current": False}}},
        },
    )
    monkeypatch.setattr(cluster, "_require_remote_state_current", require_current)
    monkeypatch.setattr(cluster, "_run_remote_materialize_command", materialize)

    payload = asyncio.run(
        cluster.materialize_remote_credentials(
            NODE_B,
            session=None,
            operation_id="remote-materialize-1",
        )
    )
    repeated = asyncio.run(
        cluster.materialize_remote_credentials(
            NODE_B,
            session=None,
            operation_id="remote-materialize-1",
        )
    )

    assert payload["ok"] is True
    assert payload["operation_id"]
    assert repeated == payload
    assert calls == ["materialize-credentials"]

    monkeypatch.setattr(
        cluster,
        "_credential_status",
        lambda _snapshot: {
            "cmc": {"count": 1},
            "tradfi": {"count": 0},
            "nodes": {NODE_B: {"materialization_ack": {"current": True}}},
        },
    )
    with pytest.raises(cluster.HTTPException) as exc_info:
        asyncio.run(cluster.materialize_remote_credentials(NODE_B, session=None))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["operation_id"]


def test_local_pbgui_dir_value_uses_absolute_path_outside_home(monkeypatch, tmp_path: Path) -> None:
    """Local PBGui path detection falls back to an absolute path outside HOME."""

    home = tmp_path / "home" / "mani"
    pbgui_dir = tmp_path / "opt" / "pbgui"
    home.mkdir(parents=True)
    pbgui_dir.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(cluster, "PBGDIR", str(pbgui_dir))

    assert cluster._local_pbgui_dir_value() == str(pbgui_dir.resolve(strict=False))


def test_set_node_sync_updates_membership_only(monkeypatch, tmp_path: Path) -> None:
    """Cluster node sync toggles write UPDATE_NODE without changing role metadata."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(
        root,
        "ADD_NODE",
        {
            "node_id": NODE_B,
            "role": "vps",
            "pbname": "vps-a",
            "sync_enabled": True,
            "ssh_host": "203.0.113.10",
        },
        created_at=102,
    )

    result = cluster.set_node_sync(NODE_B, False, session=None)
    repeat = cluster.set_node_sync(NODE_B, False, session=None)
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]

    assert result["changed"] is True
    assert repeat["changed"] is False
    assert operations[-1]["op"] == "UPDATE_NODE"
    assert operations[-1]["node_id"] == NODE_B
    assert operations[-1]["sync_enabled"] is False
    assert operations[-1]["sync_mode"] == "disabled"
    assert len(operations) == 3
    assert nodes[NODE_B]["role"] == "vps"
    assert nodes[NODE_B]["ssh_host"] == "203.0.113.10"
    assert nodes[NODE_B]["sync_enabled"] is False
    assert nodes[NODE_B]["sync_mode"] == "disabled"


def test_set_node_sync_rejects_disabling_local_node(monkeypatch, tmp_path: Path) -> None:
    """The local cluster node remains an active member for safety."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)

    with pytest.raises(HTTPException) as exc:
        cluster.set_node_sync(NODE_A, False, session=None)

    assert exc.value.status_code == 400


def test_update_node_settings_records_reachable_sync_mode(monkeypatch, tmp_path: Path) -> None:
    """Node settings updates record sync mode and SSH endpoint metadata."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "vps", "pbname": "vps-a", "sync_mode": "outbound_only"},
        created_at=102,
    )

    result = asyncio.run(
        cluster.update_node_settings(
            NODE_B,
            _JsonRequest({"sync_mode": "reachable", "remote_pbgui_dir": "test/pbgui", "ssh_host": "203.0.113.10", "ssh_user": "bot", "ssh_port": "2222", "sync_peers": [NODE_A]}),
            session=None,
        )
    )
    repeat = asyncio.run(
        cluster.update_node_settings(
            NODE_B,
            _JsonRequest({"sync_mode": "reachable", "remote_pbgui_dir": "test/pbgui", "ssh_host": "203.0.113.10", "ssh_user": "bot", "ssh_port": 2222, "sync_peers": [NODE_A]}),
            session=None,
        )
    )
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]

    assert result["changed"] is True
    assert repeat["changed"] is False
    assert operations[-1]["op"] == "UPDATE_NODE"
    assert operations[-1]["sync_mode"] == "reachable"
    assert operations[-1]["sync_enabled"] is True
    assert operations[-1]["remote_pbgui_dir"] == "test/pbgui"
    assert operations[-1]["ssh_host"] == "203.0.113.10"
    assert operations[-1]["ssh_user"] == "bot"
    assert operations[-1]["ssh_port"] == 2222
    assert operations[-1]["sync_peers"] == [NODE_A]
    assert len(operations) == 3
    assert nodes[NODE_B]["sync_mode"] == "reachable"
    assert nodes[NODE_B]["sync_enabled"] is True
    assert nodes[NODE_B]["remote_pbgui_dir"] == "test/pbgui"
    assert nodes[NODE_B]["ssh_host"] == "203.0.113.10"
    assert nodes[NODE_B]["sync_peers"] == [NODE_A]


def test_update_node_settings_records_disabled_sync_mode(monkeypatch, tmp_path: Path) -> None:
    """Saving node settings as disabled persists sync_mode and exposes removal."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "vps", "pbname": "vps-a", "sync_mode": "reachable", "sync_enabled": True, "ssh_host": "203.0.113.10", "cluster_ssh_public_key": REMOTE_CLUSTER_PUBLIC_KEY},
        created_at=102,
    )
    revoked: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cluster,
        "remove_authorized_cluster_key",
        lambda **kwargs: revoked.append(kwargs) or {"ok": True, "changed": True},
    )

    result = asyncio.run(
        cluster.update_node_settings(
            NODE_B,
            _JsonRequest({"sync_mode": "disabled", "remote_pbgui_dir": "", "ssh_host": "203.0.113.10", "ssh_user": "bot", "ssh_port": 22, "sync_peers": []}),
            session=None,
        )
    )
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]

    assert result["changed"] is True
    assert result["authorized_key_removed"] is True
    assert revoked[0]["source_node_id"] == NODE_B
    assert operations[-1]["op"] == "UPDATE_NODE"
    assert operations[-1]["sync_mode"] == "disabled"
    assert operations[-1]["sync_enabled"] is False
    assert nodes[NODE_B]["sync_mode"] == "disabled"
    assert nodes[NODE_B]["sync_enabled"] is False


def test_update_local_node_settings_persists_detected_connection_metadata(monkeypatch, tmp_path: Path) -> None:
    """Saving local node settings fills local path, IP and user metadata."""

    home = tmp_path / "home" / "mani"
    pbgui_dir = home / "test" / "pbgui"
    pbgui_dir.mkdir(parents=True)
    root = _init_cluster(pbgui_dir)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(cluster, "PBGDIR", str(pbgui_dir))
    monkeypatch.setattr(cluster, "_local_ssh_host_value", lambda *args, **kwargs: "10.9.1.31")
    monkeypatch.setattr(cluster, "_local_ssh_user_value", lambda: "mani")
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "master", "pbname": "upstream-master"}, created_at=102)

    result = asyncio.run(
        cluster.update_node_settings(
            NODE_A,
            _JsonRequest({"sync_mode": "outbound_only", "remote_pbgui_dir": "", "ssh_host": "", "ssh_user": "", "ssh_port": 22, "sync_peers": [NODE_B]}),
            session=None,
        )
    )
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    nodes = _read_json(pbgui_dir / "data" / "cluster" / "cluster_nodes.json")["nodes"]

    assert result["changed"] is True
    assert operations[-1]["op"] == "UPDATE_NODE"
    assert operations[-1]["node_id"] == NODE_A
    assert operations[-1]["remote_pbgui_dir"] == "test/pbgui"
    assert operations[-1]["ssh_host"] == "10.9.1.31"
    assert operations[-1]["ssh_user"] == "mani"
    assert nodes[NODE_A]["remote_pbgui_dir"] == "test/pbgui"
    assert nodes[NODE_A]["ssh_host"] == "10.9.1.31"
    assert nodes[NODE_A]["ssh_user"] == "mani"
    assert nodes[NODE_A]["sync_peers"] == [NODE_B]


def test_update_node_settings_rejects_reachable_without_host(monkeypatch, tmp_path: Path) -> None:
    """Reachable nodes require an explicit SSH host instead of hostname fallback."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=102)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(cluster.update_node_settings(NODE_B, _JsonRequest({"sync_mode": "reachable", "ssh_host": ""}), session=None))

    assert exc.value.status_code == 400


def test_update_node_settings_rejects_disabling_local_node(monkeypatch, tmp_path: Path) -> None:
    """The settings endpoint keeps the local node active."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(cluster.update_node_settings(NODE_A, _JsonRequest({"sync_mode": "disabled"}), session=None))

    assert exc.value.status_code == 400


def test_update_node_settings_rejects_unknown_sync_peer(monkeypatch, tmp_path: Path) -> None:
    """Node settings only accept peer allowlist entries for known cluster nodes."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-a"}, created_at=102)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            cluster.update_node_settings(
                NODE_B,
                _JsonRequest({"sync_mode": "outbound_only", "sync_peers": ["pbgui-node-00000000-0000-4000-8000-000000000099"]}),
                session=None,
            )
        )

    assert exc.value.status_code == 400


def test_remove_cluster_node_records_remove_operation(monkeypatch, tmp_path: Path) -> None:
    """Removing a stale node revokes its exact local key before membership removal."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "master", "pbname": "old-master", "sync_mode": "disabled", "sync_enabled": False, "cluster_ssh_public_key": REMOTE_CLUSTER_PUBLIC_KEY},
        created_at=102,
    )
    revoked: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cluster,
        "remove_authorized_cluster_key",
        lambda **kwargs: revoked.append(kwargs) or {"ok": True, "changed": True},
    )

    result = cluster.remove_cluster_node(NODE_B, session=None)
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]

    assert result["changed"] is True
    assert result["removed_node_id"] == NODE_B
    assert result["authorized_key_removed"] is True
    assert result["rotation_recommended"] is False
    assert result["affected_credentials"] == {"cmc": 0, "tradfi": 0, "total": 0}
    assert revoked == [{
        "pbgdir": Path(tmp_path),
        "source_node_id": NODE_B,
        "source_public_key": REMOTE_CLUSTER_PUBLIC_KEY,
    }]
    assert operations[-1]["op"] == "REMOVE_NODE"
    assert operations[-1]["node_id"] == NODE_B
    assert NODE_B not in nodes


def test_remove_cluster_node_reports_secret_free_rotation_impact(monkeypatch) -> None:
    """Removal reports affected current recipient counts without credential material."""

    remote = {
        "node_id": NODE_B,
        "role": "master",
        "enabled": True,
        "sync_mode": "disabled",
        "sync_enabled": False,
    }
    snapshot = {
        "identity": {"node_id": NODE_A},
        "cluster_nodes": {"nodes": {NODE_A: {"node_id": NODE_A}, NODE_B: remote}},
        "desired_state": {
            "instances": {},
            "secrets": {
                "cmc-id": {"secret_kind": "cmc_api_key", "recipient_ids": [NODE_A, NODE_B]},
                "tradfi-id": {"secret_kind": "tradfi_profile", "recipient_ids": [NODE_B]},
                "other-id": {"secret_kind": "cmc_api_key", "recipient_ids": [NODE_A]},
            },
        },
    }
    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: snapshot)
    monkeypatch.setattr(cluster, "_revoke_local_cluster_ssh_key", lambda _node: {"changed": False})
    monkeypatch.setattr(cluster, "append_operation", lambda *_args, **_kwargs: {"op_id": "remove-op"})
    monkeypatch.setattr(cluster, "rebuild_materialized_state", lambda *_args, **_kwargs: {"cluster_nodes": {"nodes": {NODE_A: {"node_id": NODE_A}}}})

    payload = cluster.remove_cluster_node(NODE_B, session=None)

    assert payload["rotation_recommended"] is True
    assert payload["affected_credentials"] == {"cmc": 1, "tradfi": 1, "total": 2}
    assert "secret" not in json.dumps(payload).lower()


def test_remove_cluster_node_rejects_local_active_or_assigned_nodes(monkeypatch, tmp_path: Path) -> None:
    """Node removal is limited to disabled non-local nodes without assigned configs."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "active", "sync_mode": "reachable", "ssh_host": "203.0.113.10"}, created_at=102)
    append_operation(root, "ADD_NODE", {"node_id": NODE_C, "role": "vps", "pbname": "assigned", "sync_mode": "disabled", "sync_enabled": False}, created_at=103)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTCUSDT",
            "version": "1",
            "assigned_host": NODE_C,
            "desired_state": "stopped",
            "config_manifest_hash": HASH_A,
        },
        created_at=104,
    )

    with pytest.raises(HTTPException) as local_exc:
        cluster.remove_cluster_node(NODE_A, session=None)
    with pytest.raises(HTTPException) as active_exc:
        cluster.remove_cluster_node(NODE_B, session=None)
    with pytest.raises(HTTPException) as assigned_exc:
        cluster.remove_cluster_node(NODE_C, session=None)

    assert local_exc.value.status_code == 400
    assert active_exc.value.status_code == 400
    assert "Only disabled" in active_exc.value.detail
    assert assigned_exc.value.status_code == 400
    assert "assigned V7 configs" in assigned_exc.value.detail


def test_repair_node_cluster_ssh_reads_remote_key_and_installs_master_key(monkeypatch, tmp_path: Path) -> None:
    """Cluster SSH repair stores the remote public key and installs the master key."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(remote_pbgui_dir="software/pbgui"), created_at=102)
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "ensure-local" in command:
                payload = {"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}
            elif "install-authorized-key" in command:
                payload = {"ok": True, "changed": True}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    result = asyncio.run(cluster.repair_node_cluster_ssh(NODE_B, _JsonRequest({}), session=None))
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]

    assert result["ok"] is True
    assert result["changed"] is True
    assert result["installed"] == [{"source_node_id": NODE_A, "changed": True, "role": "master"}]
    assert nodes[NODE_B]["cluster_ssh_public_key"] == REMOTE_CLUSTER_PUBLIC_KEY
    assert nodes[NODE_B]["cluster_ssh_fingerprint"] == "SHA256:remote"
    assert nodes[NODE_B]["cluster_ssh_mode"] == "forced"
    assert len(calls) == 2
    assert "ensure-local" in calls[0]
    assert "install-authorized-key" in calls[1]


def test_repair_node_cluster_ssh_installs_node_key_on_sync_peers(monkeypatch, tmp_path: Path) -> None:
    """Cluster SSH repair installs the repaired node key on its outbound sync peers."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(remote_pbgui_dir="software/pbgui", sync_peers=[NODE_C]), created_at=102)
    append_operation(
        root,
        "ADD_NODE",
        _reachable_vps_payload(node_id=NODE_C, pbname="vps-c", ssh_host="vps-c", remote_pbgui_dir="software/pbgui"),
        created_at=103,
    )
    calls: list[tuple[str, str]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command))
            if "ensure-local" in command:
                payload = {"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}
            elif "install-authorized-key" in command:
                payload = {"ok": True, "changed": True}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    result = asyncio.run(cluster.repair_node_cluster_ssh(NODE_B, _JsonRequest({}), session=None))

    assert result["outbound_installed"] == [{"target_node_id": NODE_C, "changed": True, "role": "vps"}]
    assert result["outbound_install_errors"] == []
    assert any(hostname == "vps-c" and NODE_B in command and REMOTE_CLUSTER_PUBLIC_KEY in command for hostname, command in calls)


def test_repair_local_cluster_ssh_installs_local_key_on_sync_peers(monkeypatch, tmp_path: Path) -> None:
    """Repairing the local node installs its Cluster SSH key on outbound sync peers."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master", "sync_peers": [NODE_B]}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(remote_pbgui_dir="software/pbgui"), created_at=102)
    calls: list[tuple[str, str]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command))
            return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "changed": True}), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    result = asyncio.run(cluster.repair_node_cluster_ssh(NODE_A, _JsonRequest({}), session=None))

    assert result["local"] is True
    assert result["outbound_installed"] == [{"target_node_id": NODE_B, "changed": True, "role": "vps"}]
    assert result["outbound_install_errors"] == []
    assert any(hostname == "vps-a" and NODE_A in command and LOCAL_CLUSTER_PUBLIC_KEY in command for hostname, command in calls)


def test_repair_all_cluster_ssh_repairs_active_nodes(monkeypatch, tmp_path: Path) -> None:
    """Repair All SSH runs the GUI repair flow for active reachable nodes."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master", "sync_peers": [NODE_B]}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(remote_pbgui_dir="software/pbgui"), created_at=102)
    calls: list[tuple[str, str]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command))
            if "ensure-local" in command:
                payload = {"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}
            elif "install-authorized-key" in command:
                payload = {"ok": True, "changed": True}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    result = asyncio.run(cluster.repair_all_cluster_ssh(_JsonRequest({}), session=None))

    assert result["ok"] is True
    assert result["counts"]["repaired"] == 2
    assert result["counts"]["failed"] == 0
    assert result["counts"]["outbound_errors"] == 0
    assert any(hostname == "vps-a" and NODE_A in command and LOCAL_CLUSTER_PUBLIC_KEY in command for hostname, command in calls)


def test_repair_all_cluster_ssh_reports_progress(monkeypatch, tmp_path: Path) -> None:
    """Repair All SSH reports real per-node progress for UI polling."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master", "sync_peers": [NODE_B]}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(remote_pbgui_dir="software/pbgui"), created_at=102)
    progress: list[dict[str, Any]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            if "ensure-local" in command:
                payload = {"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}
            elif "install-authorized-key" in command:
                payload = {"ok": True, "changed": True}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    result = asyncio.run(cluster._repair_all_cluster_ssh_run({}, progress_callback=progress.append))

    assert result["ok"] is True
    assert progress[0]["phase"] == "starting"
    assert progress[0]["total"] == 2
    assert any(item.get("current_pbname") == "vps-a" for item in progress)
    assert progress[-1]["phase"] == "done"
    assert progress[-1]["done"] == progress[-1]["total"] == 2


def test_repair_node_cluster_ssh_falls_back_to_node_ssh_metadata(monkeypatch, tmp_path: Path) -> None:
    """Cluster SSH repair uses stored SSH metadata when the VPS pool has no host entry."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(ssh_host="198.51.100.23", ssh_user="bot", ssh_port=2222, remote_pbgui_dir="software/pbgui"), created_at=102)
    connect_calls: list[dict[str, Any]] = []
    commands: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            return None

    class FakeConn:
        async def run(self, command: str, check: bool = False):
            commands.append(command)
            if "ensure-local" in command:
                payload = {"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}
            elif "install-authorized-key" in command:
                payload = {"ok": True, "changed": True}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    async def fake_connect(**kwargs):
        connect_calls.append(kwargs)
        return FakeConn()

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))
    monkeypatch.setattr(cluster.asyncssh, "connect", fake_connect)

    result = asyncio.run(cluster.repair_node_cluster_ssh(NODE_B, _JsonRequest({"ssh_passwords": {NODE_B: "secret"}}), session=None))

    assert result["ok"] is True
    assert result["installed"] == [{"source_node_id": NODE_A, "changed": True, "role": "master"}]
    assert connect_calls
    assert connect_calls[0]["host"] == "198.51.100.23"
    assert connect_calls[0]["username"] == "bot"
    assert connect_calls[0]["port"] == 2222
    assert connect_calls[0]["password"] == "secret"
    assert connect_calls[0]["known_hosts"] is not None
    assert any("ensure-local" in command for command in commands)
    assert any("install-authorized-key" in command for command in commands)


def test_repair_all_cluster_ssh_uses_temporary_passwords(monkeypatch, tmp_path: Path) -> None:
    """Repair All SSH passes prompted passwords only to direct SSH fallback connections."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {
            "node_id": NODE_A,
            "public_key": LOCAL_CLUSTER_PUBLIC_KEY,
            "fingerprint": "SHA256:local",
            "public_key_path": str(tmp_path / "local.pub"),
        },
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master", "sync_peers": [NODE_B]}, created_at=101)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(ssh_host="198.51.100.23", ssh_user="bot", ssh_port=2222, remote_pbgui_dir="software/pbgui"), created_at=102)
    connect_calls: list[dict[str, Any]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            return None

    class FakeConn:
        async def run(self, command: str, check: bool = False):
            if "ensure-local" in command:
                payload = {"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}
            elif "install-authorized-key" in command:
                payload = {"ok": True, "changed": True}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    async def fake_connect(**kwargs):
        connect_calls.append(kwargs)
        return FakeConn()

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))
    monkeypatch.setattr(cluster.asyncssh, "connect", fake_connect)

    payload = {"ssh_passwords": {NODE_B: "secret-password"}}
    result = asyncio.run(cluster.repair_all_cluster_ssh(_JsonRequest(payload), session=None))

    assert result["ok"] is True
    assert connect_calls
    assert {item.get("password") for item in connect_calls} == {"secret-password"}


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


def test_bootstrap_preview_errors_on_unknown_folder_without_config(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap preview reports unknown run_v7 folders that have no config.json."""

    _init_cluster(tmp_path)
    (tmp_path / "data" / "run_v7" / "empty_inst").mkdir(parents=True)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    preview = cluster.get_bootstrap_preview(session=None)

    assert preview["counts"]["error"] == 1
    item = next(item for item in preview["items"] if item.get("instance") == "empty_inst")
    assert item["action"] == "error"
    assert item["reason"] == "missing config.json"


def test_bootstrap_preview_skips_joined_instance_folder_without_config(monkeypatch, tmp_path: Path) -> None:
    """Bootstrap preview does not flag empty local folders already tracked by desired state."""

    root = _init_cluster(tmp_path)
    (tmp_path / "data" / "run_v7" / "joined_inst").mkdir(parents=True)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "joined_inst",
            "version": "5",
            "assigned_host": NODE_A,
            "desired_state": "stopped",
            "config_manifest_hash": HASH_A,
        },
        created_at=101,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    preview = cluster.get_bootstrap_preview(session=None)

    assert preview["counts"]["error"] == 0
    item = next(item for item in preview["items"] if item.get("instance") == "joined_inst")
    assert item["action"] == "skip"
    assert "desired state already tracks" in item["reason"]
    assert item["current_version"] == "5"


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
        _reachable_vps_payload(remote_pbgui_dir="software/pbgui"),
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


def test_credential_status_passively_lists_only_active_outdated_replicas() -> None:
    """Rolling status waits for active v1 nodes while disabled nodes do not block."""

    snapshot = {
        "cluster_nodes": {
            "nodes": {
                "master-v2": {
                    "role": "master",
                    "state_replica": True,
                    "enabled": True,
                    "credential_protocol_version": 2,
                    "credential_capable": True,
                    "signing_key_id": "sign-v2",
                    "encryption_key_id": "enc-v2",
                },
                "vps-v1": {
                    "role": "vps",
                    "state_replica": True,
                    "enabled": True,
                    "credential_protocol_version": 1,
                },
                "removed-v1": {
                    "role": "vps",
                    "state_replica": True,
                    "enabled": False,
                    "credential_protocol_version": 1,
                },
            }
        },
        "desired_state": {},
    }

    migration = cluster._credential_status(snapshot)["migration"]

    assert migration["status"] == "waiting_for_upgrade"
    assert migration["outdated_node_ids"] == ["vps-v1"]
    assert migration["frozen"] is False


def test_remote_status_reports_uninitialized_remote(monkeypatch, tmp_path: Path) -> None:
    """Remote status classifies an uninitialized remote cluster identity."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
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
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
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
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=100)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[tuple[str, str, int]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command, timeout))
            if "stop PBRun" in command:
                return SimpleNamespace(exit_status=0, stdout="PBRun stopped", stderr="")
            if "join " in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "vps"}),
                    stderr="",
                )
            if "get-state-vector" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {}}), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 1, "operations": [{"actor": NODE_A, "seq": 1}]}), stderr="")
            if "rebuild" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "generation": 1, "nodes": 2, "instances": 0}), stderr="")
            if "materialize-v7-preview" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"add": 0, "update": 0, "error": 0}, "can_apply": False}), stderr="")
            if "materialize-api-keys-preview" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"write": 0, "error": 0}, "can_apply": False}), stderr="")
            if "start PBRun" in command:
                return SimpleNamespace(exit_status=0, stdout="PBRun started", stderr="")
            return SimpleNamespace(
                exit_status=1,
                stdout="",
                stderr=f"unexpected command: {command}",
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.join_remote_identity(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["remote_node_id"] == NODE_B
    assert payload["pbrun_stopped"] is True
    assert calls[0][0] == "vps-a"
    assert calls[0][2] == 20
    assert "stop PBRun" in calls[0][1]
    assert calls[1][0] == "vps-a"
    assert calls[1][2] == 15
    assert f"join {CLUSTER_ID} {NODE_B} vps vps-a" in calls[1][1]
    assert payload["completion"]["ok"] is True
    assert payload["completion"]["pbrun_start"]["started"] is True
    assert len(calls) == 8
    assert "get-state-vector" in calls[2][1]
    assert "put-ops" in calls[3][1]
    assert "rebuild" in calls[4][1]
    assert "materialize-v7-preview" in calls[5][1]
    assert "materialize-api-keys-preview" in calls[6][1]
    assert "start PBRun" in calls[7][1]


def test_remote_join_does_not_stop_pbrun_for_master_node(monkeypatch, tmp_path: Path) -> None:
    """Remote join leaves PBRun alone for remote master nodes."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=100)
    append_operation(
        root,
        "ADD_NODE",
        _reachable_vps_payload(role="master", pbname="remote-master", ssh_host="remote-master"),
        created_at=101,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[tuple[str, str, int]] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command, timeout))
            if "join " in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "master"}),
                    stderr="",
                )
            if "get-state-vector" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {}}), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 1, "operations": [{"actor": NODE_A, "seq": 1}]}), stderr="")
            if "rebuild" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "generation": 1, "nodes": 2, "instances": 0}), stderr="")
            if "materialize-v7-preview" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"add": 0, "update": 0, "error": 0}, "can_apply": False}), stderr="")
            if "materialize-api-keys-preview" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"write": 0, "error": 0}, "can_apply": False}), stderr="")
            return SimpleNamespace(
                exit_status=1,
                stdout="",
                stderr=f"unexpected command: {command}",
            )

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.join_remote_identity(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["remote_node_id"] == NODE_B
    assert payload["pbrun_stopped"] is False
    assert payload["completion"]["ok"] is True
    assert payload["completion"]["pbrun_start"]["reason"] == "not_vps_runner"
    assert len(calls) == 6
    assert "stop PBRun" not in calls[0][1]
    assert not any("start PBRun" in item[1] for item in calls)
    assert f"join {CLUSTER_ID} {NODE_B} master remote-master" in calls[0][1]


def test_remote_join_rejects_foreign_remote_identity(monkeypatch, tmp_path: Path) -> None:
    """Remote join surfaces wrapper rejections without hiding the safety error."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=100)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            if "join " not in command:
                return SimpleNamespace(exit_status=0, stdout="PBRun stopped", stderr="")
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


def test_self_join_adopts_empty_local_identity_and_registers_master(monkeypatch, tmp_path: Path) -> None:
    """Self-join pulls upstream state, registers the local master and pushes it back."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="second-master",
        cluster_id=OTHER_CLUSTER_ID,
        node_id=NODE_C,
        created_at=100,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(cluster, "_local_ssh_host_value", lambda *args, **kwargs: "10.9.1.31")
    monkeypatch.setattr(cluster, "_local_ssh_user_value", lambda: "mani")
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {"public_key": LOCAL_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:local"},
    )
    calls: list[tuple[str, str, int]] = []
    upstream_op, upstream_keys = _signed_bootstrap_operation(
        tmp_path / "upstream-keys",
        pbname="upstream-master",
        hostname="upstream-master",
        sync_mode="reachable",
        sync_enabled=True,
        ssh_host="198.51.100.10",
        ssh_port=22,
    )

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append((hostname, command, timeout))
            if "pbgui.ini" in command:
                return SimpleNamespace(exit_status=0, stdout="", stderr="")
            if "cluster_ssh_setup.py" in command and "ensure-local" in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}),
                    stderr="",
                )
            if "cluster_ssh_setup.py" in command and "install-authorized-key" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "changed": True}), stderr="")
            if "hello" in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({
                        "ok": True,
                        "cluster_id": CLUSTER_ID,
                        "node_id": NODE_B,
                        "role": "master",
                        "join_authorization": _signed_join_authorization(upstream_keys, NODE_C),
                        "cluster_ssh_public_key": REMOTE_CLUSTER_PUBLIC_KEY,
                        "cluster_ssh_fingerprint": "SHA256:remote",
                    }),
                    stderr="",
                )
            if "get-state-vector" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_B: 1}}), stderr="")
            if "get-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "operations": [upstream_op], "missing": []}), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 2, "operations": []}), stderr="")
            if "rebuild" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "generation": 3, "nodes": 2, "instances": 0}), stderr="")
            return SimpleNamespace(exit_status=1, stdout="", stderr=f"unexpected command: {command}")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.self_join_existing_cluster(_JsonRequest({
        "hostname": "upstream-master",
        "ssh_host": "198.51.100.10",
        "ssh_user": "mani",
        "ssh_port": 22,
        "remote_pbgui_dir": "/home/mani/software/pbgui",
    }), session=None))

    assert payload["ok"] is True
    assert payload["adopted_local_identity"] is True
    assert payload["local_node_id"] == NODE_C
    assert payload["upstream_node_id"] == NODE_B
    assert payload["pull"]["pulled_ops"] == 1
    assert payload["membership"]["local"]["operation"] == "ADD_NODE"
    assert payload["push"]["counts"]["pushed"] == 2
    assert any("install-authorized-key" in command and LOCAL_CLUSTER_PUBLIC_KEY in command for _host, command, _timeout in calls)
    assert (root / "cluster_id").read_text(encoding="utf-8") == CLUSTER_ID
    materialized = cluster.rebuild_materialized_state(root, write=False)
    nodes = materialized["cluster_nodes"]["nodes"]
    assert nodes[NODE_C]["sync_mode"] == "outbound_only"
    assert nodes[NODE_C]["remote_pbgui_dir"] == cluster._local_pbgui_dir_value()
    assert nodes[NODE_C]["ssh_host"] == "10.9.1.31"
    assert nodes[NODE_C]["ssh_user"] == "mani"
    assert nodes[NODE_C]["sync_peers"] == [NODE_B]
    assert nodes[NODE_C]["cluster_ssh_fingerprint"] == "SHA256:local"
    assert nodes[NODE_B]["ssh_user"] == "mani"
    assert nodes[NODE_B]["cluster_ssh_fingerprint"] == "SHA256:remote"


def test_self_join_start_job_reports_progress(monkeypatch) -> None:
    """Self-join start route returns a job that can be polled until completion."""

    cluster._SELF_JOIN_JOBS.clear()

    async def fake_self_join(settings: dict[str, Any], progress_callback=None) -> dict[str, Any]:
        assert settings["hostname"] == "upstream-master"
        if progress_callback:
            progress_callback({"phase": "hello", "done": 1, "total": 9, "remaining": 8})
        return {"ok": True, "cluster_id": CLUSTER_ID, "pull": {"pulled_ops": 0}, "push": {"counts": {"pushed": 0}}}

    async def exercise() -> tuple[dict[str, Any], dict[str, Any]]:
        start = await cluster.start_self_join_existing_cluster(_JsonRequest({
            "hostname": "upstream-master",
            "ssh_host": "198.51.100.10",
        }), session=None)
        await asyncio.sleep(0)
        polled = cluster.get_self_join_job(start["job_id"], session=None)
        return start, polled

    monkeypatch.setattr(cluster, "_self_join_existing_cluster", fake_self_join)

    try:
        start_job, polled_job = asyncio.run(exercise())
    finally:
        cluster._SELF_JOIN_JOBS.clear()

    assert start_job["status"] == "queued"
    assert start_job["total"] == 9
    assert polled_job["status"] == "done"
    assert polled_job["phase"] == "done"
    assert polled_job["done"] == 9
    assert polled_job["result"]["ok"] is True


def test_self_join_defers_missing_historical_config_blobs(monkeypatch, tmp_path: Path) -> None:
    """Self-join skips lost historical config blobs but pulls current desired blobs."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="second-master",
        cluster_id=OTHER_CLUSTER_ID,
        node_id=NODE_C,
        created_at=100,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(cluster, "_local_ssh_host_value", lambda *args, **kwargs: "10.9.1.31")
    monkeypatch.setattr(cluster, "_local_ssh_user_value", lambda: "mani")
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {"public_key": LOCAL_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:local"},
    )
    old_manifest_hash = "sha256:c2c814513553736dd8693dccd67a08cd84e06e949597b8c96737924285b89595"
    current_raw = b'{"live":{"user":"current"}}'
    current_file_sha = hashlib.sha256(current_raw).hexdigest()
    current_file_hash = f"sha256:{current_file_sha}"
    current_manifest = {
        "schema_version": 1,
        "files": {"config.json": {"sha256": current_file_sha, "size": len(current_raw)}},
    }
    current_manifest_raw = cluster._canonical_json_bytes(current_manifest)
    current_manifest_hash = "sha256:" + hashlib.sha256(current_manifest_raw).hexdigest()
    upstream_membership, upstream_keys = _signed_bootstrap_operation(
        tmp_path / "upstream-keys",
        pbname="upstream-master",
        hostname="upstream-master",
        sync_mode="reachable",
        sync_enabled=True,
        ssh_host="198.51.100.10",
        ssh_port=22,
    )
    upstream_ops = [
        upstream_membership,
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_B}:00000002",
            "actor": NODE_B,
            "seq": 2,
            "op": "UPSERT_CONFIG",
            "created_at": 102,
            "instance": "local_inst",
            "version": "1",
            "assigned_host": NODE_C,
            "desired_state": "running",
            "config_manifest_hash": old_manifest_hash,
        },
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_B}:00000003",
            "actor": NODE_B,
            "seq": 3,
            "op": "UPSERT_CONFIG",
            "created_at": 103,
            "instance": "local_inst",
            "parent_version": "1",
            "version": "2",
            "assigned_host": NODE_C,
            "desired_state": "running",
            "config_manifest_hash": current_manifest_hash,
        },
    ]

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            del hostname, timeout
            if "pbgui.ini" in command:
                return SimpleNamespace(exit_status=0, stdout="", stderr="")
            if "cluster_ssh_setup.py" in command and "ensure-local" in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}),
                    stderr="",
                )
            if "cluster_ssh_setup.py" in command and "install-authorized-key" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "changed": True}), stderr="")
            if "hello" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "master", "join_authorization": _signed_join_authorization(upstream_keys, NODE_C)}), stderr="")
            if "get-state-vector" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_B: 3}}), stderr="")
            if "get-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "operations": upstream_ops, "missing": []}), stderr="")
            if old_manifest_hash in command and "get-blob" in command:
                return SimpleNamespace(exit_status=1, stdout="", stderr=json.dumps({"ok": False, "error": f"missing config blob: {old_manifest_hash}"}))
            if current_manifest_hash in command and "get-blob" in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "hash": current_manifest_hash, "content_b64": base64.b64encode(current_manifest_raw).decode("ascii")}),
                    stderr="",
                )
            if current_file_hash in command and "get-blob" in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "hash": current_file_hash, "content_b64": base64.b64encode(current_raw).decode("ascii")}),
                    stderr="",
                )
            if "put-blobs" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 2, "blobs": []}), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 2, "operations": []}), stderr="")
            if "rebuild" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "generation": 5, "nodes": 2, "instances": 1}), stderr="")
            return SimpleNamespace(exit_status=1, stdout="", stderr=f"unexpected command: {command}")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.self_join_existing_cluster(_JsonRequest({
        "hostname": "upstream-master",
        "ssh_host": "198.51.100.10",
    }), session=None))

    assert payload["ok"] is True
    assert payload["pull"]["deferred_missing_config_blobs"] == 1
    assert payload["pull"]["pulled_config_blobs"] == 2
    assert payload["pull"]["pulled_ops"] == 3
    assert payload["local_materialization"]["counts"]["written_instances"] == 1
    assert cluster._read_cluster_blob(root / "config_blobs", current_manifest_hash) == current_manifest_raw
    assert cluster._read_cluster_blob(root / "config_blobs", current_file_hash) == current_raw
    assert (root.parent / "run_v7" / "local_inst" / "config.json").read_bytes() == current_raw
    desired = cluster.rebuild_materialized_state(root, write=False)["desired_state"]
    assert desired["instances"]["local_inst"]["config_manifest_hash"] == current_manifest_hash


def test_self_join_uses_password_runner_without_monitor_pool(monkeypatch, tmp_path: Path) -> None:
    """Self-join can use a one-shot SSH password before monitor keys exist."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="second-master",
        cluster_id=OTHER_CLUSTER_ID,
        node_id=NODE_C,
        created_at=100,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(cluster, "_local_ssh_host_value", lambda *args, **kwargs: "10.9.1.31")
    monkeypatch.setattr(cluster, "_local_ssh_user_value", lambda: "mani")
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {"public_key": LOCAL_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:local"},
    )
    upstream_op, upstream_keys = _signed_bootstrap_operation(
        tmp_path / "upstream-keys",
        pbname="upstream-master",
        hostname="upstream-master",
        sync_mode="reachable",
        sync_enabled=True,
        ssh_host="198.51.100.10",
        ssh_port=22,
    )
    runners: list[Any] = []

    class FakePasswordRunner:
        def __init__(self, *, hostname: str, ssh_host: str, ssh_user: str, ssh_port: int, ssh_password: str) -> None:
            self.hostname = hostname
            self.ssh_host = ssh_host
            self.ssh_user = ssh_user
            self.ssh_port = ssh_port
            self.ssh_password = ssh_password
            self.closed = False
            runners.append(self)

        async def run(self, hostname: str, command: str, timeout: int = 30, check: bool = False):
            del hostname, timeout, check
            if "pbgui.ini" in command:
                return SimpleNamespace(exit_status=0, stdout="", stderr="")
            if "cluster_ssh_setup.py" in command and "ensure-local" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}), stderr="")
            if "cluster_ssh_setup.py" in command and "install-authorized-key" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "changed": True}), stderr="")
            if "hello" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "master", "join_authorization": _signed_join_authorization(upstream_keys, NODE_C)}), stderr="")
            if "get-state-vector" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_B: 1}}), stderr="")
            if "get-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "operations": [upstream_op], "missing": []}), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 2, "operations": []}), stderr="")
            if "rebuild" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "generation": 3, "nodes": 2, "instances": 0}), stderr="")
            return SimpleNamespace(exit_status=1, stdout="", stderr=f"unexpected command: {command}")

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(cluster, "_SelfJoinPasswordSSHRunner", FakePasswordRunner)
    monkeypatch.setattr(cluster, "get_monitor", lambda: None)

    payload = asyncio.run(cluster.self_join_existing_cluster(_JsonRequest({
        "hostname": "upstream-master",
        "ssh_host": "198.51.100.10",
        "ssh_user": "mani",
        "ssh_password": "secret-password",
        "ssh_port": 22,
        "remote_pbgui_dir": "/home/mani/software/pbgui",
    }), session=None))

    assert payload["ok"] is True
    assert len(runners) == 1
    assert runners[0].ssh_host == "198.51.100.10"
    assert runners[0].ssh_user == "mani"
    assert runners[0].ssh_password == "secret-password"
    assert runners[0].closed is True
    nodes = cluster.rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"]
    assert "ssh_password" not in nodes[NODE_B]


def test_self_join_refuses_to_adopt_non_empty_foreign_cluster(monkeypatch, tmp_path: Path) -> None:
    """Self-join does not overwrite a local cluster that already has operations."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="second-master",
        cluster_id=OTHER_CLUSTER_ID,
        node_id=NODE_C,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_C, "role": "master", "pbname": "second-master"}, created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            if "pbgui.ini" in command:
                return SimpleNamespace(exit_status=0, stdout="", stderr="")
            if "hello" in command:
                return SimpleNamespace(
                    exit_status=0,
                    stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "master"}),
                    stderr="",
                )
            return SimpleNamespace(exit_status=1, stdout="", stderr="unexpected command")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(cluster.self_join_existing_cluster(_JsonRequest({
            "hostname": "upstream-master",
            "ssh_host": "198.51.100.10",
        }), session=None))

    assert exc.value.status_code == 409
    assert "local oplog is not empty" in exc.value.detail


def test_self_join_recovery_archives_non_empty_foreign_cluster(monkeypatch, tmp_path: Path) -> None:
    """Self-join recovery archives accidental local state before adopting upstream."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=second-master\n", encoding="utf-8")
    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="second-master",
        cluster_id=OTHER_CLUSTER_ID,
        node_id=NODE_C,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_C, "role": "master", "pbname": "wrong-cluster"}, created_at=101)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        cluster,
        "ensure_local_cluster_ssh_material",
        lambda *args, **kwargs: {"public_key": LOCAL_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:local"},
    )
    upstream_op, upstream_keys = _signed_bootstrap_operation(
        tmp_path / "upstream-keys",
        pbname="upstream-master",
        hostname="upstream-master",
        sync_mode="reachable",
        sync_enabled=True,
        ssh_host="198.51.100.10",
    )

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            if "pbgui.ini" in command:
                return SimpleNamespace(exit_status=0, stdout="", stderr="")
            if "cluster_ssh_setup.py" in command and "ensure-local" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "public_key": REMOTE_CLUSTER_PUBLIC_KEY, "fingerprint": "SHA256:remote"}), stderr="")
            if "cluster_ssh_setup.py" in command and "install-authorized-key" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "changed": True}), stderr="")
            if "hello" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "role": "master", "join_authorization": _signed_join_authorization(upstream_keys, NODE_C)}), stderr="")
            if "get-state-vector" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_B: 1}}), stderr="")
            if "get-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "operations": [upstream_op], "missing": []}), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "count": 2, "operations": []}), stderr="")
            if "rebuild" in command:
                return SimpleNamespace(exit_status=0, stdout=json.dumps({"ok": True, "generation": 2, "nodes": 2, "instances": 0}), stderr="")
            return SimpleNamespace(exit_status=1, stdout="", stderr=f"unexpected command: {command}")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.self_join_existing_cluster(_JsonRequest({
        "hostname": "upstream-master",
        "ssh_host": "198.51.100.10",
        "reset_local_cluster_state": True,
    }), session=None))

    assert payload["ok"] is True
    assert payload["adopted_local_identity"] is True
    archive = payload["archived_local_cluster_state"]
    assert archive["changed"] is True
    archive_path = Path(archive["path"])
    assert archive_path.name.startswith("self-join-")
    assert (archive_path / "oplog" / NODE_C / "00000001.json").is_file()
    assert (root / "cluster_id").read_text(encoding="utf-8") == CLUSTER_ID
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    assert {item["node_id"] for item in operations if item["op"] == "ADD_NODE"} == {NODE_B, NODE_C}


def test_remote_preview_compares_state_without_writes(monkeypatch, tmp_path: Path) -> None:
    """Remote preview reads vector and desired state and returns a compact diff."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "materialize-api-keys-preview" in command:
                payload = {
                    "ok": True,
                    "cluster_id": CLUSTER_ID,
                    "node_id": NODE_B,
                    "read_only": True,
                    "can_apply": False,
                    "counts": {"current": 1, "write": 0, "error": 0},
                    "status": "current",
                }
            elif "materialize-v7-preview" in command:
                payload = {
                    "ok": True,
                    "cluster_id": CLUSTER_ID,
                    "node_id": NODE_B,
                    "read_only": True,
                    "can_apply": True,
                    "counts": {"add": 1, "update": 0, "skip": 0, "error": 0, "files_to_write": 1},
                    "items": [],
                }
            elif "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 1, NODE_B: 1}}
            else:
                payload = {
                    "ok": True,
                    "cluster_id": CLUSTER_ID,
                    "node_id": NODE_B,
                    "desired_state": {
                        "schema_version": 1,
                        "cluster_id": CLUSTER_ID,
                        "generated_at": 103,
                        "instances": {
                            "remote_inst": {
                                "version": "1",
                                "desired_state": "stopped",
                                "assigned_host": NODE_B,
                                "config_manifest_hash": HASH_A,
                            }
                        },
                        "tombstones": {},
                    },
                }
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.get_remote_preview(NODE_B, session=None))

    assert payload["read_only"] is True
    assert payload["remote_node_id"] == NODE_B
    assert payload["state_vector"]["counts"] == {"equal": 0, "local_ahead": 1, "remote_ahead": 1}
    assert payload["desired_state"]["instances"]["missing_on_remote"] == ["local_inst"]
    assert payload["desired_state"]["instances"]["missing_locally"] == ["remote_inst"]
    assert payload["operation_sync"]["counts"]["local_ops_to_push"] == 1
    assert payload["operation_sync"]["counts"]["remote_ops_to_pull"] == 1
    assert payload["operation_sync"]["push_by_op"] == {"UPSERT_CONFIG": 1}
    assert payload["operation_sync"]["local_ops_missing_on_remote"][0]["target"] == "local_inst"
    assert payload["materialization"]["counts"]["add"] == 1
    assert payload["api_key_materialization"]["status"] == "current"
    assert len(calls) == 5
    assert "materialize-credentials-preview" in calls[-1]
    assert "get-state-vector" in calls[0]
    assert "get-desired-state" in calls[1]
    assert "materialize-v7-preview" in calls[2]
    assert "materialize-api-keys-preview" in calls[3]


def test_remote_materialize_v7_requires_synchronized_state(monkeypatch, tmp_path: Path) -> None:
    """Remote materialization refuses to write when the remote state vector is stale."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 1}}
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(cluster.materialize_remote_v7_configs(NODE_B, session=None))

    assert exc.value.status_code == 409
    assert "not synchronized" in exc.value.detail
    assert len(calls) == 1
    assert "materialize-v7" not in calls[0]


def test_remote_materialize_v7_writes_after_state_match(monkeypatch, tmp_path: Path) -> None:
    """Remote materialization runs only after vector and desired state match local state."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    local = cluster.rebuild_materialized_state(root, write=False)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": local["state_vector"]}
            elif "get-desired-state" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "desired_state": local["desired_state"]}
            elif "materialize-v7-preview" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"add": 0, "update": 0, "error": 0, "current": 1}, "can_apply": False}
            elif "materialize-api-keys-preview" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"write": 0, "error": 0, "missing": 1}, "can_apply": False}
            elif "materialize-v7" in command:
                payload = {
                    "ok": True,
                    "cluster_id": CLUSTER_ID,
                    "node_id": NODE_B,
                    "counts": {"written_instances": 1, "written_files": 2},
                    "written": [{"instance": "local_inst", "files": 2}],
                }
            elif "start PBRun" in command:
                return SimpleNamespace(exit_status=0, stdout="PBRun started", stderr="")
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.materialize_remote_v7_configs(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["materialization"]["counts"]["written_files"] == 2
    assert payload["pbrun_start"]["started"] is True
    assert len(calls) == 6
    assert "get-state-vector" in calls[0]
    assert "get-desired-state" in calls[1]
    assert "materialize-v7" in calls[2]
    assert "materialize-v7-preview" in calls[3]
    assert "materialize-api-keys-preview" in calls[4]
    assert "start PBRun" in calls[5]


def test_remote_materialize_v7_waits_to_start_pbrun_when_api_keys_pending(monkeypatch, tmp_path: Path) -> None:
    """PBRun must not restart after V7 materialization while API-key writes are pending."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    local = cluster.rebuild_materialized_state(root, write=False)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": local["state_vector"]}
            elif "get-desired-state" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "desired_state": local["desired_state"]}
            elif "materialize-v7-preview" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"add": 0, "update": 0, "error": 0}, "can_apply": False}
            elif "materialize-api-keys-preview" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"write": 1, "error": 0}, "can_apply": True}
            elif "materialize-v7" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"written_instances": 1, "written_files": 2}}
            elif "start PBRun" in command:
                raise AssertionError("PBRun must not start while API-key materialization is pending")
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.materialize_remote_v7_configs(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["pbrun_start"]["started"] is False
    assert payload["pbrun_start"]["reason"] == "api_key_materialization_pending"
    assert not any("start PBRun" in command for command in calls)


def test_remote_materialize_api_keys_writes_after_state_match(monkeypatch, tmp_path: Path) -> None:
    """Remote API-key materialization runs only after vector and desired state match local state."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 7, "payload_hash": HASH_A, "secret_blob_hash": "sha256:" + "b" * 64},
        created_at=102,
    )
    local = cluster.rebuild_materialized_state(root, write=False)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": local["state_vector"]}
            elif "get-desired-state" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "desired_state": local["desired_state"]}
            elif "materialize-v7-preview" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"add": 0, "update": 0, "error": 0, "current": 1}, "can_apply": False}
            elif "materialize-api-keys-preview" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "counts": {"write": 0, "error": 0, "current": 1}, "can_apply": False}
            elif "materialize-api-keys" in command:
                payload = {
                    "ok": True,
                    "cluster_id": CLUSTER_ID,
                    "node_id": NODE_B,
                    "counts": {"written": 1},
                    "status": "written",
                }
            elif "start PBRun" in command:
                return SimpleNamespace(exit_status=0, stdout="PBRun started", stderr="")
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.materialize_remote_api_keys(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["materialization"]["status"] == "written"
    assert payload["pbrun_start"]["started"] is True
    assert len(calls) == 6
    assert "get-state-vector" in calls[0]
    assert "get-desired-state" in calls[1]
    assert "materialize-api-keys" in calls[2]
    assert "materialize-v7-preview" in calls[3]
    assert "materialize-api-keys-preview" in calls[4]
    assert "start PBRun" in calls[5]


def test_remote_push_ops_writes_missing_local_ops_and_rebuilds(monkeypatch, tmp_path: Path) -> None:
    """Remote push sends missing local operations and then rebuilds remote state."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    op = append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 1}}
            elif "put-ops" in command:
                payload = {"ok": True, "count": 1, "operations": [{"op_id": op["op_id"], "actor": op["actor"], "seq": op["seq"]}]}
            elif "rebuild" in command:
                payload = {"ok": True, "generation": 2, "nodes": 1, "instances": 1}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.push_remote_operations(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["counts"]["pushed"] == 1
    assert payload["counts"]["rebuilt"] == 1
    assert payload["pushed"] == [{"op_id": op["op_id"], "actor": NODE_A, "seq": 2, "op": "UPSERT_CONFIG", "target": "local_inst"}]
    assert len(calls) == 3
    assert "get-state-vector" in calls[0]
    assert "put-ops" in calls[1]
    assert op["op_id"] in calls[1]
    assert "rebuild" in calls[2]


def test_cluster_payload_command_streams_payload_over_stdin() -> None:
    """Upload payloads must not be embedded in the SSH command line."""

    commands: list[str] = []
    received: list[str] = []

    class FakeProc:
        exit_status = 0

        async def communicate(self, input=None):
            received.append(str(input or ""))
            return json.dumps({"ok": True}), ""

        def close(self):
            return None

    class FakePool:
        async def start_process(self, hostname: str, command: str):
            commands.append(command)
            return FakeProc()

    payload = json.dumps({"blobs": [{"content_b64": "A" * 4096}]})

    result = asyncio.run(
        cluster._run_cluster_payload_command(
            FakePool(),
            "vps-a",
            "software/pbgui",
            NODE_A,
            "put-blobs",
            payload,
        )
    )

    assert result.exit_status == 0
    assert received == [payload]
    assert "put-blobs" in commands[0]
    assert "A" * 128 not in commands[0]


def test_remote_push_ops_uploads_current_config_blobs_before_ops(monkeypatch, tmp_path: Path) -> None:
    """Remote push sends current config manifest and file blobs before the oplog."""

    root = _init_cluster(tmp_path)
    instance_dir = _write_v7_config(tmp_path, "local_inst", 2)
    (instance_dir / "override.json").write_text(json.dumps({"enabled": True}), encoding="utf-8")
    manifest_hash = compute_config_manifest_hash(build_config_manifest(instance_dir))
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    op = append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 1}}
            elif "put-blobs" in command:
                payload = {"ok": True, "count": 3, "blobs": []}
            elif "put-ops" in command:
                payload = {"ok": True, "count": 1, "operations": [{"op_id": op["op_id"], "actor": op["actor"], "seq": op["seq"]}]}
            elif "rebuild" in command:
                payload = {"ok": True, "generation": 2, "nodes": 1, "instances": 1}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.push_remote_operations(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["counts"]["config_blobs_pushed"] == 3
    assert payload["counts"]["config_blobs_total"] == 3
    assert payload["counts"]["config_blobs_skipped"] == 0
    assert "put-blobs" in calls[1]
    assert manifest_hash in calls[1]
    assert "put-ops" in calls[2]
    assert "rebuild" in calls[3]


def test_remote_push_ops_uploads_api_key_secret_blobs_before_ops(monkeypatch, tmp_path: Path) -> None:
    """Remote push sends API-key payload and secret blobs before the oplog."""

    root = _init_cluster(tmp_path)
    payload_hash = _write_cluster_blob(root, "config_blobs", b'{"redacted":true}')
    secret_hash = _write_cluster_blob(root, "secret_blobs", b'{"_api_serial":7,"user":{"secret":"s"}}')
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    op = append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 7, "payload_hash": payload_hash, "secret_blob_hash": secret_hash},
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 1}}
            elif "put-blobs" in command:
                payload = {"ok": True, "count": 1, "blobs": []}
            elif "put-secret-blob" in command:
                payload = {"ok": True, "hash": secret_hash, "path": "secret_blobs/sha256/x.json"}
            elif "put-ops" in command:
                payload = {"ok": True, "count": 1, "operations": [{"op_id": op["op_id"], "actor": op["actor"], "seq": op["seq"]}]}
            elif "rebuild" in command:
                payload = {"ok": True, "generation": 2, "nodes": 1, "instances": 0}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.push_remote_operations(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["counts"]["secret_blobs_pushed"] == 1
    assert payload["counts"]["secret_blobs_total"] == 1
    assert "put-blobs" in calls[1]
    assert payload_hash in calls[1]
    assert "put-secret-blob" in calls[2]
    assert secret_hash in calls[2]
    assert "put-ops" in calls[3]
    assert "rebuild" in calls[4]


def test_remote_push_ops_rejects_when_remote_has_unknown_ops(monkeypatch, tmp_path: Path) -> None:
    """Remote push refuses to write when the remote has operations missing locally."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 2, NODE_B: 1}}
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(cluster.push_remote_operations(NODE_B, session=None))

    assert exc.value.status_code == 409
    assert "Remote has operations missing locally" in exc.value.detail
    assert len(calls) == 1
    assert "get-state-vector" in calls[0]


def test_remote_push_ops_noops_when_remote_is_current(monkeypatch, tmp_path: Path) -> None:
    """Remote push avoids put-op and rebuild when the remote already has all local operations."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 2}}
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.push_remote_operations(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["counts"] == {"pushed": 0, "rebuilt": 0, "local_ops_remaining": 0, "total_missing_before": 0}
    assert payload["pushed"] == []
    assert len(calls) == 1
    assert "get-state-vector" in calls[0]


def test_remote_push_ops_can_defer_rebuild_for_progress_batches(monkeypatch, tmp_path: Path) -> None:
    """Remote push can send a bounded batch without rebuilding until the final batch."""

    root = _init_cluster(tmp_path)
    op1 = append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {}}
            elif "put-ops" in command:
                payload = {"ok": True, "op_id": op1["op_id"], "actor": op1["actor"], "seq": op1["seq"]}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.push_remote_operations(NODE_B, limit=1, rebuild=False, session=None))

    assert payload["ok"] is True
    assert payload["counts"]["pushed"] == 1
    assert payload["counts"]["rebuilt"] == 0
    assert payload["counts"]["local_ops_remaining"] == 1
    assert payload["counts"]["total_missing_before"] == 2
    assert len(calls) == 2
    assert "get-state-vector" in calls[0]
    assert "put-ops" in calls[1]
    assert "rebuild" not in " ".join(calls)


def test_remote_push_ops_falls_back_when_bulk_is_unavailable(monkeypatch, tmp_path: Path) -> None:
    """Remote push falls back to put-op when the remote wrapper lacks put-ops."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    op = append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {NODE_A: 1}}
                return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")
            if "put-ops" in command:
                return SimpleNamespace(exit_status=1, stdout="", stderr=json.dumps({"ok": False, "error": "unsupported command: put-ops"}))
            if "put-op" in command:
                payload = {"ok": True, "op_id": op["op_id"], "actor": op["actor"], "seq": op["seq"]}
                return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")
            if "rebuild" in command:
                payload = {"ok": True, "generation": 2, "nodes": 1, "instances": 1}
                return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")
            raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))

    payload = asyncio.run(cluster.push_remote_operations(NODE_B, session=None))

    assert payload["ok"] is True
    assert payload["counts"]["pushed"] == 1
    assert len(calls) == 4
    assert "put-ops" in calls[1]
    assert "put-op" in calls[2]
    assert "rebuild" in calls[3]


def test_remote_push_job_reports_progress_without_splitting_frontend_batches(monkeypatch, tmp_path: Path) -> None:
    """Remote push jobs run the full backend push while exposing local progress."""

    cluster._REMOTE_PUSH_JOBS.clear()
    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", _reachable_vps_payload(), created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    calls: list[str] = []
    scheduled: list = []

    class FakePool:
        async def run(self, hostname: str, command: str, timeout: int = 30):
            calls.append(command)
            if "get-state-vector" in command:
                payload = {"ok": True, "cluster_id": CLUSTER_ID, "node_id": NODE_B, "state_vector": {}}
            elif "put-ops" in command:
                payload = {"ok": True, "count": 2, "operations": []}
            elif "rebuild" in command:
                payload = {"ok": True, "generation": 2, "nodes": 1, "instances": 1}
            else:
                raise AssertionError(f"unexpected command: {command}")
            return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")

    def fake_track_background_job(kind: str, job_id: str, coro):
        assert kind == "remote_push"
        assert job_id
        scheduled.append(coro)
        return SimpleNamespace()

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=FakePool()))
    monkeypatch.setattr(cluster, "_track_background_job", fake_track_background_job)

    start = asyncio.run(cluster.start_remote_push_operations(NODE_B, session=None))
    assert start["status"] == "queued"
    assert len(scheduled) == 1

    asyncio.run(scheduled[0])
    job = cluster.get_remote_push_job(start["job_id"], session=None)

    assert job["status"] == "done"
    assert job["phase"] == "done"
    assert job["done"] == 2
    assert job["total"] == 2
    assert job["remaining"] == 0
    assert job["result"]["counts"]["rebuilt"] == 1
    assert len(calls) == 3
    assert "get-state-vector" in calls[0]
    assert "put-ops" in calls[1]
    assert "rebuild" in calls[-1]
    cluster._REMOTE_PUSH_JOBS.clear()


def test_cluster_shutdown_cancels_jobs_and_marks_them_interrupted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown awaits all API-owned Cluster jobs and leaves terminal poll states."""
    monkeypatch.setattr(cluster, "_REMOTE_PUSH_JOBS", {})
    monkeypatch.setattr(cluster, "_SELF_JOIN_JOBS", {})
    monkeypatch.setattr(cluster, "_REPAIR_ALL_SSH_JOBS", {})
    monkeypatch.setattr(cluster, "_BACKGROUND_JOBS", {})
    monkeypatch.setattr(cluster, "_log", lambda *_args, **_kwargs: None)

    async def exercise() -> list[asyncio.Task]:
        blocker = asyncio.Event()
        remote = cluster._create_remote_push_job({"node_id": NODE_B, "hostname": "remote"})
        self_join = cluster._create_self_join_job({"hostname": "upstream"})
        repair = cluster._create_repair_all_ssh_job()
        tasks = [
            cluster._track_background_job("remote_push", remote["job_id"], blocker.wait()),
            cluster._track_background_job("self_join", self_join["job_id"], blocker.wait()),
            cluster._track_background_job("repair_all_ssh", repair["job_id"], blocker.wait()),
        ]
        await asyncio.sleep(0)
        assert "Cluster operation" in cluster.restart_block_reason()

        await cluster.shutdown()
        return tasks

    tasks = asyncio.run(exercise())

    assert all(task.cancelled() for task in tasks)
    assert cluster._BACKGROUND_JOBS == {}
    assert cluster.restart_block_reason() == ""
    for jobs in (cluster._REMOTE_PUSH_JOBS, cluster._SELF_JOIN_JOBS, cluster._REPAIR_ALL_SSH_JOBS):
        job = next(iter(jobs.values()))
        assert job["status"] == "error"
        assert job["phase"] == "interrupted"
        assert job["error"] == "Interrupted by API shutdown"


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
        "sync_mode": "disabled",
        "sync_enabled": False,
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
    node = next(item for item in nodes.values() if item.get("pbname") == "vps-a")

    assert result["result"]["counts"]["applied"] == 1
    assert result["after"]["counts"]["skip"] == 1
    assert operations[-1]["op"] == "UPDATE_NODE"
    assert operations[-1]["sync_mode"] == "disabled"
    assert operations[-1]["sync_enabled"] is False
    assert node["role"] == "vps"
    assert node["pbname"] == "vps-a"
    assert node["sync_mode"] == "disabled"
    assert node["sync_enabled"] is False
    assert node["state_replica"] is False
    assert node["ssh_host"] == "203.0.113.10"
    assert node["ssh_user"] == "bot"
    assert node["ssh_port"] == 2222
    mapping = _read_json(tmp_path / "data" / "cluster" / "host_node_ids.json")
    assert mapping["hosts"]["vps-a"]["node_id"] == node["node_id"]


def test_apply_bootstrap_node_records_only_selected_known_vps(monkeypatch, tmp_path: Path) -> None:
    """The targeted VPS node bootstrap endpoint writes only the requested host."""

    root = _init_cluster(tmp_path)
    _write_vps_config(tmp_path, "vps-a", ip="203.0.113.10", user="bot", ssh_port=2222)
    _write_vps_config(tmp_path, "vps-b", ip="203.0.113.11", user="bot", ssh_port=2223)
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))

    result = cluster.apply_bootstrap_node("vps-a", session=None)
    nodes = _read_json(tmp_path / "data" / "cluster" / "cluster_nodes.json")["nodes"]
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    mapping = _read_json(tmp_path / "data" / "cluster" / "host_node_ids.json")

    assert result["changed"] is True
    assert result["result"]["counts"] == {"applied": 1, "skipped": 0, "failed": 0}
    assert len(nodes) == 2
    assert any(node.get("pbname") == "vps-a" and node.get("state_replica") is False for node in nodes.values())
    assert [op["pbname"] for op in operations if op["op"] == "UPDATE_NODE"] == ["vps-a"]
    assert sorted(mapping["hosts"]) == ["vps-a"]
    assert result["after"]["counts"]["add"] == 1


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
    node = next(item for item in nodes.values() if item.get("pbname") == "remote-master")
    mapping = _read_json(tmp_path / "data" / "cluster" / "host_node_ids.json")

    assert preview["items"][0]["node_role"] == "master"
    assert result["result"]["counts"]["applied"] == 1
    assert operations[-1]["role"] == "master"
    assert operations[-1]["sync_mode"] == "disabled"
    assert operations[-1]["sync_enabled"] is False
    assert node["role"] == "master"
    assert node["sync_mode"] == "disabled"
    assert node["sync_enabled"] is False
    assert node["state_replica"] is False
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


def test_v7_enabled_host_prefers_existing_reachable_node(monkeypatch, tmp_path: Path) -> None:
    """Saving a V7 config does not create or reuse a disabled duplicate host node."""

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    (root / "cluster_nodes.json").write_text(
        json.dumps({
            "schema_version": 1,
            "nodes": {
                NODE_B: {
                    "node_id": NODE_B,
                    "role": "vps",
                    "hostname": "manibot40",
                    "pbname": "manibot40",
                    "sync_enabled": True,
                    "sync_mode": "reachable",
                },
                NODE_C: {
                    "node_id": NODE_C,
                    "role": "vps",
                    "hostname": "manibot40",
                    "pbname": "manibot40",
                    "sync_enabled": False,
                    "sync_mode": "disabled",
                },
            },
        }),
        encoding="utf-8",
    )
    (root / "host_node_ids.json").write_text(
        json.dumps({"schema_version": 1, "hosts": {"manibot40": {"node_id": NODE_C, "role": "vps"}}}),
        encoding="utf-8",
    )

    node_id, hostname, role = v7_instances._cluster_node_for_enabled_host(root, {"node_id": NODE_A}, "manibot40")

    mapping = _read_json(root / "host_node_ids.json")
    assert (node_id, hostname, role) == (NODE_B, "manibot40", "vps")
    assert mapping["hosts"]["manibot40"]["node_id"] == NODE_B


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
