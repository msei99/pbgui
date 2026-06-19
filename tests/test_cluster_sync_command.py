"""Tests for the restricted Cluster Sync command wrapper."""

from __future__ import annotations

import base64
import hashlib
import json
import stat
import sys
from pathlib import Path

import pytest

from cluster_sync_command import ClusterSyncCommandError, main, run_command
from master.cluster_state import append_operation, ensure_local_identity, load_operations, read_local_identity


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
FOREIGN_CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000099"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
HASH_A = "sha256:" + "a" * 64


def _init_cluster(tmp_path: Path) -> Path:
    """Create a deterministic cluster with a registered remote node."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="master-a",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-b"}, created_at=101)
    return root


def _operation(cluster_id: str = CLUSTER_ID) -> dict:
    """Build one deterministic remote operation."""

    return {
        "schema_version": 1,
        "cluster_id": cluster_id,
        "op_id": f"{NODE_B}:00000001",
        "actor": NODE_B,
        "seq": 1,
        "op": "ADD_NODE",
        "created_at": 102,
        "node_id": NODE_B,
        "role": "vps",
        "pbname": "vps-b",
    }


def _canonical_json_bytes(value: dict) -> bytes:
    """Return the canonical JSON bytes used by config blob manifests."""

    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _write_config_blob(root: Path, blob_hash: str, raw: bytes) -> None:
    """Write one content-addressed config blob for materialization tests."""

    digest = blob_hash.removeprefix("sha256:")
    path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


def _write_secret_blob(root: Path, blob_hash: str, raw: bytes) -> None:
    """Write one content-addressed secret blob for materialization tests."""

    digest = blob_hash.removeprefix("sha256:")
    path = root / "secret_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


def _write_config_blob_set(root: Path, files: dict[str, bytes]) -> str:
    """Write file blobs plus their manifest and return the manifest hash."""

    manifest_files = {}
    for name, raw in files.items():
        digest = hashlib.sha256(raw).hexdigest()
        manifest_files[name] = {"sha256": digest, "size": len(raw)}
        _write_config_blob(root, f"sha256:{digest}", raw)
    manifest_raw = _canonical_json_bytes({"schema_version": 1, "files": manifest_files})
    manifest_hash = "sha256:" + hashlib.sha256(manifest_raw).hexdigest()
    _write_config_blob(root, manifest_hash, manifest_raw)
    return manifest_hash


def _write_local_run_v7_config(root: Path, instance: str, raw_config: bytes) -> tuple[str, str, bytes]:
    """Write a local run_v7 config without writing config blobs."""

    instance_dir = root.parent / "run_v7" / instance
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_bytes(raw_config)
    file_hash = hashlib.sha256(raw_config).hexdigest()
    manifest_raw = _canonical_json_bytes({
        "schema_version": 1,
        "files": {"config.json": {"sha256": file_hash, "size": len(raw_config)}},
    })
    manifest_hash = "sha256:" + hashlib.sha256(manifest_raw).hexdigest()
    return manifest_hash, file_hash, manifest_raw


def test_hello_returns_identity_for_registered_peer(tmp_path: Path) -> None:
    """hello returns local identity and protocol information for known peers."""

    root = _init_cluster(tmp_path)

    payload = run_command(root, NODE_B, "hello")

    assert payload["ok"] is True
    assert payload["protocol_version"] == 1
    assert payload["cluster_id"] == CLUSTER_ID
    assert payload["node_id"] == NODE_A
    assert payload["remote_node"] == NODE_B


def test_unknown_peer_is_rejected_without_join_mode(tmp_path: Path) -> None:
    """Only registered peers can use the restricted command unless join mode is explicit."""

    root = _init_cluster(tmp_path)
    unknown = "pbgui-node-00000000-0000-4000-8000-000000000099"

    with pytest.raises(ClusterSyncCommandError, match="not registered"):
        run_command(root, unknown, "hello")

    assert run_command(root, unknown, "hello", allow_join=True)["remote_node"] == unknown


def test_join_initializes_identity_for_approved_remote(tmp_path: Path) -> None:
    """join writes identity files only when explicit join mode is enabled."""

    root = tmp_path / "cluster"

    payload = run_command(root, NODE_A, f"join {CLUSTER_ID} {NODE_B} vps vps-b", allow_join=True)
    identity = read_local_identity(root)

    assert payload["ok"] is True
    assert payload["cluster_id"] == CLUSTER_ID
    assert payload["node_id"] == NODE_B
    assert payload["joined_by"] == NODE_A
    assert identity["cluster_id"] == CLUSTER_ID
    assert identity["node_id"] == NODE_B


def test_join_rejects_overwriting_existing_identity(tmp_path: Path) -> None:
    """join is idempotent for the same ids but refuses foreign identities."""

    root = tmp_path / "cluster"
    run_command(root, NODE_A, f"join {CLUSTER_ID} {NODE_B} vps vps-b", allow_join=True)

    with pytest.raises(ClusterSyncCommandError, match="existing cluster_id differs"):
        run_command(root, NODE_A, f"join {FOREIGN_CLUSTER_ID} {NODE_B} vps vps-b", allow_join=True)


def test_join_requires_explicit_join_mode(tmp_path: Path) -> None:
    """join cannot run unless the wrapper is invoked with allow_join."""

    root = tmp_path / "cluster"

    with pytest.raises(ClusterSyncCommandError, match="requires allow_join"):
        run_command(root, NODE_A, f"join {CLUSTER_ID} {NODE_B} vps vps-b")


def test_unknown_command_is_rejected(tmp_path: Path) -> None:
    """Unsupported commands are rejected before any state access."""

    root = _init_cluster(tmp_path)

    with pytest.raises(ClusterSyncCommandError, match="unsupported command"):
        run_command(root, NODE_B, "rm -rf /")


def test_put_op_writes_valid_operation(tmp_path: Path) -> None:
    """put-op validates and writes one remote operation file."""

    root = _init_cluster(tmp_path)

    payload = run_command(root, NODE_B, "put-op", json.dumps(_operation()).encode("utf-8"))
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)

    assert payload == {"ok": True, "op_id": f"{NODE_B}:00000001", "actor": NODE_B, "seq": 1}
    assert any(item["op_id"] == f"{NODE_B}:00000001" for item in operations)


def test_put_ops_writes_valid_operation_batch(tmp_path: Path) -> None:
    """put-ops validates and writes a batch of remote operation files."""

    root = _init_cluster(tmp_path)
    op1 = _operation()
    op2 = dict(op1)
    op2.update({"op_id": f"{NODE_B}:00000002", "seq": 2, "node_id": NODE_A})

    payload = run_command(root, NODE_B, "put-ops", json.dumps({"operations": [op1, op2]}).encode("utf-8"))
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)

    assert payload["ok"] is True
    assert payload["count"] == 2
    assert [item["op_id"] for item in payload["operations"]] == [f"{NODE_B}:00000001", f"{NODE_B}:00000002"]
    assert any(item["op_id"] == f"{NODE_B}:00000001" for item in operations)
    assert any(item["op_id"] == f"{NODE_B}:00000002" for item in operations)


def test_get_ops_returns_bounded_operation_range(tmp_path: Path) -> None:
    """get-ops returns existing operations and reports missing seq numbers."""

    root = _init_cluster(tmp_path)
    op1 = _operation()
    op3 = dict(op1)
    op3.update({"op_id": f"{NODE_B}:00000003", "seq": 3, "node_id": NODE_A})
    run_command(root, NODE_B, "put-ops", json.dumps({"operations": [op1, op3]}).encode("utf-8"))

    payload = run_command(root, NODE_B, f"get-ops {NODE_B} 1 3")

    assert payload["ok"] is True
    assert [item["seq"] for item in payload["operations"]] == [1, 3]
    assert payload["missing"] == [2]


def test_put_op_rejects_foreign_cluster(tmp_path: Path) -> None:
    """put-op rejects operations from another cluster before writing."""

    root = _init_cluster(tmp_path)

    with pytest.raises(ClusterSyncCommandError, match="foreign cluster_id"):
        run_command(root, NODE_B, "put-op", json.dumps(_operation(FOREIGN_CLUSTER_ID)).encode("utf-8"))

    assert not (root / "oplog" / NODE_B).exists()


def test_rebuild_and_get_state_vector(tmp_path: Path) -> None:
    """rebuild materializes state and get-state-vector returns current counters."""

    root = _init_cluster(tmp_path)
    run_command(root, NODE_B, "put-op", json.dumps(_operation()).encode("utf-8"))

    rebuild = run_command(root, NODE_B, "rebuild")
    state = run_command(root, NODE_B, "get-state-vector")

    assert rebuild["generation"] == 2
    assert state["cluster_id"] == CLUSTER_ID
    assert state["node_id"] == NODE_A
    assert state["state_vector"] == {NODE_A: 1, NODE_B: 1}


def test_put_blob_validates_hash_and_writes_content_addressed_file(tmp_path: Path) -> None:
    """put-blob writes only when the provided hash matches the payload."""

    root = _init_cluster(tmp_path)
    raw = b'{"config": true}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    payload = run_command(root, NODE_B, f"put-blob {blob_hash}", raw)
    path = root / payload["path"]

    assert path.read_bytes() == raw
    assert payload["path"].startswith("config_blobs/sha256/")
    with pytest.raises(ClusterSyncCommandError, match="blob hash mismatch"):
        run_command(root, NODE_B, f"put-blob {'sha256:' + '0' * 64}", raw)


def test_put_blobs_writes_valid_blob_batch(tmp_path: Path) -> None:
    """put-blobs validates and writes multiple config blobs in one command."""

    root = _init_cluster(tmp_path)
    raw_a = b'{"config":true}'
    raw_b = b'{"override":true}'
    hash_a = "sha256:" + hashlib.sha256(raw_a).hexdigest()
    hash_b = "sha256:" + hashlib.sha256(raw_b).hexdigest()
    request = {
        "blobs": [
            {"hash": hash_a, "content_b64": base64.b64encode(raw_a).decode("ascii")},
            {"hash": hash_b, "content_b64": base64.b64encode(raw_b).decode("ascii")},
        ]
    }

    payload = run_command(root, NODE_B, "put-blobs", json.dumps(request).encode("utf-8"))

    assert payload["ok"] is True
    assert payload["count"] == 2
    for blob_hash, raw in ((hash_a, raw_a), (hash_b, raw_b)):
        expected = blob_hash.removeprefix("sha256:")
        path = root / "config_blobs" / "sha256" / expected[:2] / f"{expected}.json"
        assert path.read_bytes() == raw

    single = run_command(root, NODE_B, f"get-blob {hash_a}")
    batch = run_command(root, NODE_B, f"get-blobs {hash_a} {hash_b}")

    assert base64.b64decode(single["content_b64"]) == raw_a
    assert [item["hash"] for item in batch["blobs"]] == [hash_a, hash_b]
    assert [base64.b64decode(item["content_b64"]) for item in batch["blobs"]] == [raw_a, raw_b]


def test_get_blob_repairs_missing_manifest_from_existing_run_v7_config(tmp_path: Path) -> None:
    """get-blob rebuilds a missing manifest blob from matching local run_v7 files."""

    root = _init_cluster(tmp_path)
    raw_config = b'{"live":{"user":"local"}}'
    manifest_hash, file_hash, manifest_raw = _write_local_run_v7_config(root, "local_inst", raw_config)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )

    payload = run_command(root, NODE_B, f"get-blob {manifest_hash}")

    assert payload["hash"] == manifest_hash
    assert base64.b64decode(payload["content_b64"]) == manifest_raw
    assert (root / "config_blobs" / "sha256" / file_hash[:2] / f"{file_hash}.json").read_bytes() == raw_config


def test_get_blob_repairs_missing_file_blob_from_existing_run_v7_config(tmp_path: Path) -> None:
    """get-blob rebuilds missing file blobs referenced by matching local configs."""

    root = _init_cluster(tmp_path)
    raw_config = b'{"live":{"user":"local"}}'
    manifest_hash, file_hash, manifest_raw = _write_local_run_v7_config(root, "local_inst", raw_config)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )

    payload = run_command(root, NODE_B, f"get-blob sha256:{file_hash}")
    manifest_digest = manifest_hash.removeprefix("sha256:")

    assert payload["hash"] == f"sha256:{file_hash}"
    assert base64.b64decode(payload["content_b64"]) == raw_config
    assert (
        root / "config_blobs" / "sha256" / manifest_digest[:2] / f"{manifest_digest}.json"
    ).read_bytes() == manifest_raw


def test_put_secret_blob_uses_owner_only_permissions(tmp_path: Path) -> None:
    """Secret blobs are stored with owner-only permissions."""

    root = _init_cluster(tmp_path)
    raw = b'{"secret": true}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    payload = run_command(root, NODE_B, f"put-secret-blob {blob_hash}", raw)
    path = root / payload["path"]

    mode = stat.S_IMODE(path.stat().st_mode)

    assert path.read_bytes() == raw
    assert payload["path"].startswith("secret_blobs/sha256/")
    assert mode == 0o600

    fetched = run_command(root, NODE_B, f"get-secret-blob {blob_hash}")
    assert fetched["hash"] == blob_hash
    assert base64.b64decode(fetched["content_b64"]) == raw


def test_get_desired_state_returns_materialized_snapshot(tmp_path: Path) -> None:
    """get-desired-state returns a deterministic snapshot rebuilt from oplog."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTCUSDT",
            "version": "7",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )

    payload = run_command(root, NODE_B, "get-desired-state")

    assert payload["desired_state"]["instances"]["bybit_BTCUSDT"]["version"] == "7"


def test_materialize_v7_preview_and_apply_master_writes_all_config_blobs(tmp_path: Path) -> None:
    """materialize-v7 writes all clean V7 JSON configs on master nodes."""

    root = _init_cluster(tmp_path)
    manifest_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"local"}}'})
    other_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"other"}}'})
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "other_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": other_hash,
        },
        created_at=103,
    )
    append_operation(root, "DELETE_INSTANCE", {"instance": "deleted_inst", "version": "1"}, created_at=104)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "conflict_inst",
            "parent_version": "1",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=105,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "conflict_inst",
            "parent_version": "1",
            "version": "3",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": other_hash,
        },
        actor=NODE_B,
        created_at=106,
    )

    preview = run_command(root, NODE_B, "materialize-v7-preview")
    result = run_command(root, NODE_B, "materialize-v7")

    assert preview["read_only"] is True
    assert preview["materialize_all"] is True
    assert preview["counts"]["add"] == 2
    assert preview["counts"]["not_assigned"] == 0
    assert preview["counts"]["conflicted"] == 1
    assert preview["counts"]["tombstoned"] == 1
    assert result["counts"]["written_instances"] == 2
    assert result["counts"]["written_files"] == 2
    assert (root.parent / "run_v7" / "local_inst" / "config.json").read_bytes() == b'{"live":{"user":"local"}}'
    assert (root.parent / "run_v7" / "other_inst" / "config.json").read_bytes() == b'{"live":{"user":"other"}}'
    assert not (root.parent / "run_v7" / "conflict_inst" / "config.json").exists()
    assert not (root.parent / "run_v7" / "deleted_inst" / "config.json").exists()
    after = run_command(root, NODE_B, "materialize-v7-preview")
    assert after["counts"]["add"] == 0
    assert after["counts"]["skip"] >= 1


def test_materialize_v7_vps_writes_only_assigned_config_blobs(tmp_path: Path) -> None:
    """VPS nodes materialize only their assigned V7 configs."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="vps",
        pbname="vps-b",
        cluster_id=CLUSTER_ID,
        node_id=NODE_B,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master-a"}, created_at=101)
    manifest_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"local"}}'})
    other_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"other"}}'})
    append_operation(
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
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "other_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": other_hash,
        },
        created_at=103,
    )

    preview = run_command(root, NODE_A, "materialize-v7-preview")
    result = run_command(root, NODE_A, "materialize-v7")

    assert preview["materialize_all"] is False
    assert preview["counts"]["add"] == 1
    assert preview["counts"]["not_assigned"] == 1
    assert result["counts"]["written_instances"] == 1
    assert (root.parent / "run_v7" / "local_inst" / "config.json").read_bytes() == b'{"live":{"user":"local"}}'
    assert not (root.parent / "run_v7" / "other_inst" / "config.json").exists()


def test_materialize_v7_blocks_when_required_blob_is_missing(tmp_path: Path) -> None:
    """materialize-v7 refuses to write when an assigned config blob is missing."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-v7-preview")

    assert preview["counts"]["error"] == 1
    assert preview["can_apply"] is False
    with pytest.raises(ClusterSyncCommandError, match="materialization blocked"):
        run_command(root, NODE_B, "materialize-v7")
    assert not (root.parent / "run_v7" / "local_inst" / "config.json").exists()


def test_materialize_v7_repairs_missing_local_blobs_from_existing_config(tmp_path: Path) -> None:
    """materialize-v7 rebuilds missing blobs from matching local run_v7 files."""

    root = _init_cluster(tmp_path)
    instance_dir = root.parent / "run_v7" / "local_inst"
    instance_dir.mkdir(parents=True)
    raw_config = b'{"live":{"user":"local"}}'
    (instance_dir / "config.json").write_bytes(raw_config)
    file_hash = hashlib.sha256(raw_config).hexdigest()
    manifest_raw = _canonical_json_bytes({"schema_version": 1, "files": {"config.json": {"sha256": file_hash, "size": len(raw_config)}}})
    manifest_hash = "sha256:" + hashlib.sha256(manifest_raw).hexdigest()
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-v7-preview")
    result = run_command(root, NODE_B, "materialize-v7")
    after = run_command(root, NODE_B, "materialize-v7-preview")

    assert preview["counts"]["error"] == 1
    assert result["counts"]["error"] == 0
    assert result["counts"]["written_instances"] == 0
    assert result["repaired_config_blobs"] == 1
    assert after["counts"]["error"] == 0
    assert after["counts"]["skip"] >= 1


@pytest.mark.parametrize(("role", "expects_backup"), [("master", True), ("vps", False)])
def test_materialize_api_keys_preview_and_apply_writes_secret_blob(
    monkeypatch,
    tmp_path: Path,
    role: str,
    expects_backup: bool,
) -> None:
    """materialize-api-keys writes api-keys.json and backs up only on masters."""

    root = _init_cluster(tmp_path)
    if role != "master":
        ensure_local_identity(
            root,
            role=role,
            pbname="vps-a",
            cluster_id=CLUSTER_ID,
            node_id=NODE_A,
            created_at=100,
        )
    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    target = pb7 / "api-keys.json"
    target.write_text('{"_api_serial":1}', encoding="utf-8")
    monkeypatch.setattr("cluster_sync_command.PBGDIR", str(tmp_path))
    monkeypatch.setattr("cluster_sync_command.pb7dir", lambda: str(pb7))
    raw_secret = b'{"_api_serial":7,"user":{"exchange":"binance","secret":"s"}}'
    secret_hash = "sha256:" + hashlib.sha256(raw_secret).hexdigest()
    payload_hash = "sha256:" + hashlib.sha256(b'{"redacted":true}').hexdigest()
    _write_secret_blob(root, secret_hash, raw_secret)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 7, "payload_hash": payload_hash, "secret_blob_hash": secret_hash},
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-api-keys-preview")
    result = run_command(root, NODE_B, "materialize-api-keys")

    assert preview["can_apply"] is True
    assert preview["counts"]["write"] == 1
    assert result["status"] == "written"
    assert target.read_bytes() == raw_secret
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    if expects_backup:
        backup_path = Path(result["backup"])
        assert backup_path.parent == tmp_path / "data" / "api-keys"
        assert backup_path.name.startswith("api-keys7_cluster-materialize_")
        assert backup_path.read_text(encoding="utf-8") == '{"_api_serial":1}'
    else:
        assert result["backup_skipped"] == "vps_runner"
        assert "backup" not in result
        assert not (tmp_path / "data" / "api-keys").exists()
    after = run_command(root, NODE_B, "materialize-api-keys-preview")
    assert after["counts"]["current"] == 1


def test_materialize_api_keys_blocks_when_secret_blob_is_missing(tmp_path: Path) -> None:
    """materialize-api-keys refuses to write when the desired secret blob is missing."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 7, "payload_hash": HASH_A, "secret_blob_hash": "sha256:" + "d" * 64},
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-api-keys-preview")

    assert preview["can_apply"] is False
    assert preview["counts"]["error"] == 1
    with pytest.raises(ClusterSyncCommandError, match="missing api-keys secret blob"):
        run_command(root, NODE_B, "materialize-api-keys")


def test_main_does_not_read_stdin_for_hello(monkeypatch, tmp_path: Path, capsys) -> None:
    """The CLI hello command must not block waiting for stdin."""

    class BlockingStdin:
        class Buffer:
            def read(self, size: int = -1) -> bytes:
                raise AssertionError("stdin should not be read")

        buffer = Buffer()

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(sys, "stdin", BlockingStdin())

    exit_code = main(["--cluster-root", str(root), "--remote-node", NODE_B, "hello"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["ok"] is True
    assert output["remote_node"] == NODE_B
