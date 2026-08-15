"""Focused tests for the PB8 PBCluster operation and materialization family."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import threading

import pytest

import cluster_sync_command
import master.cluster_sync_worker as cluster_sync_worker
from cluster_sync_command import run_command
from file_lock import advisory_file_lock
from master.cluster_state import (
    PB8_OPERATION_CAPABILITY,
    append_node_placeholder,
    append_operation,
    ensure_local_identity,
    rebuild_materialized_state,
)
from master.cluster_sync_worker import (
    ClusterSyncWorker,
    _partition_operations_for_peer_capabilities,
    _pb8_projection_blockers,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000081"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000008a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000008b"
HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64


def _cluster(tmp_path: Path, *, role: str = "master") -> Path:
    """Create one local cluster member for PB8 operation tests."""

    root = tmp_path / "data" / "cluster"
    ensure_local_identity(
        root,
        role=role,
        pbname="node-a",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": role}, created_at=101)
    return root


def _write_manifest(root: Path, files: dict[str, bytes]) -> str:
    """Write verified config file blobs and their canonical manifest."""

    manifest_files = {}
    for filename, raw in files.items():
        digest = hashlib.sha256(raw).hexdigest()
        manifest_files[filename] = {"sha256": digest, "size": len(raw)}
        path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    manifest_raw = json.dumps(
        {"schema_version": 1, "files": manifest_files},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(manifest_raw).hexdigest()
    path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(manifest_raw)
    return f"sha256:{digest}"


def _project_pb8_exchange_keys(monkeypatch, tmp_path: Path, root: Path) -> Path:
    """Configure a cloned PB8 runtime and materialize its exchange-key projection."""

    pbgui = tmp_path / "pbgui"
    pb7 = tmp_path / "pb7"
    pb8 = tmp_path / "pb8"
    for directory in (pbgui, pb7, pb8):
        directory.mkdir(exist_ok=True)
    (pb8 / ".git").mkdir(exist_ok=True)
    monkeypatch.setattr(cluster_sync_command, "PBGDIR", str(pbgui))
    monkeypatch.setattr(cluster_sync_command, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(cluster_sync_command, "pb8dir", lambda: str(pb8))
    raw_secret = b'{"_api_serial":3,"pb8-user":{"exchange":"bybit","secret":"s"}}'
    secret_hash = "sha256:" + hashlib.sha256(raw_secret).hexdigest()
    secret_path = root / "secret_blobs" / "sha256" / secret_hash[7:9] / f"{secret_hash[7:]}.json"
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    secret_path.write_bytes(raw_secret)
    append_operation(root, "UPSERT_API_KEYS", {
        "api_serial": 3,
        "payload_hash": HASH_A,
        "secret_blob_hash": secret_hash,
    })
    run_command(root, NODE_A, "materialize-api-keys")
    return pb8


def _append_pb8_config(root: Path, raw: bytes, *, version: str = "1", parent_version: str = "0") -> str:
    """Publish one local PB8 desired config backed by verified blobs."""

    manifest_hash = _write_manifest(root, {"config.json": raw})
    append_operation(root, "UPSERT_PB8_CONFIG", {
        "instance": "pb8_bot",
        "version": version,
        "parent_version": parent_version,
        "assigned_host": NODE_A,
        "desired_state": "running",
        "config_manifest_hash": manifest_hash,
    })
    return manifest_hash


def test_pb8_operations_are_isolated_from_legacy_v7_state(tmp_path: Path) -> None:
    """Explicit PB8 names update only PB8 maps while old names remain V7-only."""

    root = _cluster(tmp_path)
    append_operation(root, "UPSERT_CONFIG", {
        "instance": "shared",
        "version": "7",
        "assigned_host": NODE_A,
        "desired_state": "running",
        "config_manifest_hash": HASH_A,
    })
    append_operation(root, "UPSERT_PB8_CONFIG", {
        "instance": "shared",
        "version": "1",
        "parent_version": "0",
        "assigned_host": NODE_A,
        "desired_state": "stopped",
        "config_manifest_hash": HASH_B,
    })
    append_operation(root, "STOP_INSTANCE", {"instance": "shared"})
    append_operation(root, "START_PB8_INSTANCE", {"instance": "shared"})

    desired = rebuild_materialized_state(root, write=False)["desired_state"]

    assert desired["instances"]["shared"]["version"] == "7"
    assert desired["instances"]["shared"]["desired_state"] == "stopped"
    assert desired["pb8_instances"]["shared"]["version"] == "1"
    assert desired["pb8_instances"]["shared"]["desired_state"] == "running"
    assert desired["tombstones"] == {}
    assert desired["pb8_tombstones"] == {}


def test_pb8_move_stop_delete_recreate_and_tombstone_lifecycle(tmp_path: Path) -> None:
    """The separate PB8 family preserves V7 lifecycle semantics under PB8 names."""

    root = _cluster(tmp_path)
    append_operation(root, "UPSERT_PB8_CONFIG", {
        "instance": "pb8_bot",
        "version": "1",
        "parent_version": "0",
        "assigned_host": NODE_A,
        "desired_state": "running",
        "config_manifest_hash": HASH_A,
    })
    append_operation(root, "MOVE_PB8_INSTANCE", {
        "instance": "pb8_bot",
        "version": "2",
        "parent_version": "1",
        "from": NODE_A,
        "to": NODE_B,
    })
    append_operation(root, "STOP_PB8_INSTANCE", {"instance": "pb8_bot"})
    append_operation(root, "DELETE_PB8_INSTANCE", {"instance": "pb8_bot", "version": "2"})
    append_operation(root, "UPSERT_PB8_CONFIG", {
        "instance": "pb8_bot",
        "version": "3",
        "parent_version": "2",
        "assigned_host": NODE_A,
        "desired_state": "stopped",
        "config_manifest_hash": HASH_B,
        "allow_tombstone_recreate": True,
    })
    append_operation(root, "TOMBSTONE_PB8_INSTANCE", {"instance": "pb8_bot", "version": "3"})

    desired = rebuild_materialized_state(root, write=False)["desired_state"]

    assert "pb8_bot" not in desired["pb8_instances"]
    assert desired["pb8_tombstones"]["pb8_bot"]["version"] == "3"
    assert desired["instances"] == {}
    assert desired["tombstones"] == {}


def test_materialize_v8_exactly_reconciles_json_and_backs_up_removals(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """PB8 apply writes verified files, removes stale JSON, and preserves unmanaged files."""

    root = _cluster(tmp_path)
    _project_pb8_exchange_keys(monkeypatch, tmp_path, root)
    config_raw = b'{"live":{"user":"pb8"}}'
    _append_pb8_config(root, config_raw)
    target = root.parent / "run_v8" / "pb8_bot"
    target.mkdir(parents=True)
    (target / "stale.json").write_text('{"stale":true}', encoding="utf-8")
    (target / "monitor.json").write_text('{"runtime":true}', encoding="utf-8")
    (target / "runtime.log").write_text("keep", encoding="utf-8")

    preview = run_command(root, NODE_A, "materialize-v8-preview")
    result = run_command(root, NODE_A, "materialize-v8")

    assert preview["read_only"] is True
    assert preview["run_v8_root"] == str(root.parent / "run_v8")
    assert preview["counts"]["files_to_write"] == 1
    assert preview["counts"]["files_to_remove"] == 1
    assert (target / "config.json").read_bytes() == config_raw
    assert not (target / "stale.json").exists()
    assert (target / "monitor.json").exists()
    assert (target / "runtime.log").exists()
    backup = Path(result["reconciled"][0]["backup"])
    assert (backup / "stale.json").read_text(encoding="utf-8") == '{"stale":true}'
    assert run_command(root, NODE_A, "materialize-v8-preview")["can_apply"] is False

    append_operation(root, "DELETE_PB8_INSTANCE", {"instance": "pb8_bot", "version": "1"})
    deleted = run_command(root, NODE_A, "materialize-v8")
    tombstone_backup = Path(deleted["deleted"][0]["backup"])
    assert not target.exists()
    assert (tombstone_backup / "config.json").read_bytes() == config_raw
    assert (tombstone_backup / "runtime.log").read_text(encoding="utf-8") == "keep"


def test_apply_bundle_materializes_targeted_pb8_config(monkeypatch, tmp_path: Path) -> None:
    """The single-command fast path writes its PB8 config before returning."""

    root = _cluster(tmp_path)
    _project_pb8_exchange_keys(monkeypatch, tmp_path, root)
    config_raw = b'{"live":{"user":"pb8"}}'
    _append_pb8_config(root, config_raw)
    operation = max(
        (
            json.loads(path.read_text(encoding="utf-8"))
            for path in root.joinpath("oplog").glob("*/*.json")
            if json.loads(path.read_text(encoding="utf-8")).get("op") == "UPSERT_PB8_CONFIG"
        ),
        key=lambda item: int(item["seq"]),
    )
    config_blobs, secret_blobs, sealed_blobs = cluster_sync_worker._collect_local_blobs_for_operations(
        root,
        [operation],
    )
    payload = cluster_sync_worker._apply_bundle_payload(
        [operation],
        config_blobs,
        secret_blobs,
        sealed_blobs,
    ).encode("utf-8")

    result = run_command(root, NODE_A, "apply-bundle", payload)

    assert result["pb8_materialization"]["counts"]["written_instances"] == 1
    assert result["materialization"] == result["pb8_materialization"]
    assert (root.parent / "run_v8" / "pb8_bot" / "config.json").read_bytes() == config_raw


def test_push_pb8_activation_uses_one_bounded_apply_bundle(monkeypatch, tmp_path: Path) -> None:
    """PB8 fast activation skips a full peer pass and targets only its assigned VPS."""

    root = _cluster(tmp_path)
    append_node_placeholder(root, {
        "node_id": NODE_B,
        "role": "vps",
        "pbname": "runner-b",
        "ssh_host": "runner-b",
    })
    manifest_hash = _write_manifest(root, {"config.json": b'{"live":{"user":"pb8"}}'})
    operation = append_operation(root, "UPSERT_PB8_CONFIG", {
        "instance": "pb8_bot",
        "version": "1",
        "parent_version": "0",
        "assigned_host": NODE_B,
        "desired_state": "running",
        "config_manifest_hash": manifest_hash,
    })
    rebuild_materialized_state(root)
    calls: list[tuple[str, dict]] = []
    settings: list[tuple[int, int, Path]] = []

    class ClientStub:
        """Capture one direct PB8 bundle request."""

        def __init__(self, *, timeout: int, connect_timeout: int, cluster_root: Path) -> None:
            settings.append((timeout, connect_timeout, cluster_root))

        def run(self, peer, local_node_id, command_text, payload=None) -> dict:
            calls.append((command_text, json.loads(payload)))
            return {"ok": True, "materialization": {"ok": True}}

    monkeypatch.setattr(cluster_sync_worker, "SshClusterPeerClient", ClientStub)

    result = cluster_sync_worker.push_pb8_activation(root, operation, timeout=4)

    assert settings == [(4, 2, root)]
    assert [command for command, _payload in calls] == ["apply-bundle"]
    assert calls[0][1]["operations"] == [operation]
    assert result["status"] == "activated"
    assert result["pbname"] == "runner-b"


@pytest.mark.parametrize(
    ("projection_state", "reason"),
    [
        ("missing_metadata", "desired api_keys metadata is missing"),
        ("non_current", "PB8 exchange-key projection is not current"),
        ("error", "PB8 exchange-key projection check failed"),
        ("omitted", "PB8 exchange-key projection target is omitted"),
    ],
)
def test_materialize_v8_blocks_without_current_pb8_exchange_projection(
    monkeypatch,
    tmp_path: Path,
    projection_state: str,
    reason: str,
) -> None:
    """PB8 config writes require desired metadata and the exact current PB8 projection."""

    root = _cluster(tmp_path)
    pbgui = tmp_path / "pbgui"
    pb7 = tmp_path / "pb7"
    pb8 = tmp_path / "pb8"
    for directory in (pbgui, pb7, pb8):
        directory.mkdir()
    (pb8 / ".git").mkdir()
    monkeypatch.setattr(cluster_sync_command, "PBGDIR", str(pbgui))
    monkeypatch.setattr(cluster_sync_command, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(cluster_sync_command, "pb8dir", lambda: str(pb8))
    if projection_state != "missing_metadata":
        raw_secret = b'{"_api_serial":4,"pb8-user":{"exchange":"bybit","secret":"s"}}'
        secret_hash = "sha256:" + hashlib.sha256(raw_secret).hexdigest()
        if projection_state != "error":
            secret_path = root / "secret_blobs" / "sha256" / secret_hash[7:9] / f"{secret_hash[7:]}.json"
            secret_path.parent.mkdir(parents=True, exist_ok=True)
            secret_path.write_bytes(raw_secret)
        append_operation(root, "UPSERT_API_KEYS", {
            "api_serial": 4,
            "payload_hash": HASH_A,
            "secret_blob_hash": secret_hash,
        })
        if projection_state == "omitted":
            run_command(root, NODE_A, "materialize-api-keys")
            original_targets = cluster_sync_command._api_keys_projection_targets
            monkeypatch.setattr(
                cluster_sync_command,
                "_api_keys_projection_targets",
                lambda cluster_root: [
                    target for target in original_targets(cluster_root) if target[0] != "pb8"
                ],
            )
    _append_pb8_config(root, b'{"live":{"user":"pb8"}}')

    preview = run_command(root, NODE_A, "materialize-v8-preview")

    assert preview["ok"] is False
    assert preview["status"] == "blocked"
    assert preview["can_apply"] is False
    assert reason in preview["reason"]
    with pytest.raises(cluster_sync_command.ClusterSyncCommandError, match=reason):
        run_command(root, NODE_A, "materialize-v8")
    assert not (root.parent / "run_v8" / "pb8_bot" / "config.json").exists()


def test_materialize_v8_requires_clone_without_creating_pb8_runtime(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """An uncloned configured PB8 path blocks config writes and is never created."""

    root = _cluster(tmp_path)
    pbgui = tmp_path / "pbgui"
    pb7 = tmp_path / "pb7"
    pb8 = tmp_path / "pb8"
    pbgui.mkdir()
    pb7.mkdir()
    monkeypatch.setattr(cluster_sync_command, "PBGDIR", str(pbgui))
    monkeypatch.setattr(cluster_sync_command, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(cluster_sync_command, "pb8dir", lambda: str(pb8))
    raw_secret = b'{"_api_serial":5,"pb8-user":{"exchange":"bybit","secret":"s"}}'
    secret_hash = "sha256:" + hashlib.sha256(raw_secret).hexdigest()
    secret_path = root / "secret_blobs" / "sha256" / secret_hash[7:9] / f"{secret_hash[7:]}.json"
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    secret_path.write_bytes(raw_secret)
    append_operation(root, "UPSERT_API_KEYS", {
        "api_serial": 5,
        "payload_hash": HASH_A,
        "secret_blob_hash": secret_hash,
    })
    run_command(root, NODE_A, "materialize-api-keys")
    _append_pb8_config(root, b'{"live":{"user":"pb8"}}')

    preview = run_command(root, NODE_A, "materialize-v8-preview")

    assert preview["status"] == "blocked"
    assert "PB8 runtime is not cloned" in preview["reason"]
    assert not (root.parent / "run_v8").exists()
    assert not pb8.exists()
    with pytest.raises(cluster_sync_command.ClusterSyncCommandError, match="PB8 runtime is not cloned"):
        run_command(root, NODE_A, "materialize-v8")
    assert not pb8.exists()


def test_materialize_v8_locks_plan_and_apply_against_api_saves(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A save completed under data/run_v8/.write is included in the apply plan."""

    root = _cluster(tmp_path)
    _project_pb8_exchange_keys(monkeypatch, tmp_path, root)
    _append_pb8_config(root, b'{"live":{"user":"old"}}')
    run_root = root.parent / "run_v8"
    target = run_root / "pb8_bot" / "config.json"
    lock_target = run_root / ".write"
    attempted = threading.Event()
    results: list[dict] = []
    errors: list[BaseException] = []

    @contextmanager
    def observed_lock(path: Path):
        if Path(path) == lock_target:
            attempted.set()
        with advisory_file_lock(path):
            yield

    monkeypatch.setattr(cluster_sync_command, "advisory_file_lock", observed_lock)

    def materialize() -> None:
        try:
            results.append(run_command(root, NODE_A, "materialize-v8"))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    with advisory_file_lock(lock_target):
        worker = threading.Thread(target=materialize)
        worker.start()
        assert attempted.wait(timeout=2)
        assert worker.is_alive()
        latest = b'{"live":{"user":"new"}}'
        _append_pb8_config(root, latest, version="2", parent_version="1")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(latest)
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert errors == []
    assert results[0]["counts"]["written_files"] == 0
    assert target.read_bytes() == b'{"live":{"user":"new"}}'


def test_worker_materializes_pb8_only_after_api_key_projections(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A worker turn orders PB8 after exchange and credential projections."""

    root = _cluster(tmp_path)
    calls: list[str] = []
    monkeypatch.setattr(cluster_sync_worker, "credential_migration_is_complete", lambda _root: True)

    monkeypatch.setattr(
        cluster_sync_worker,
        "_materialize_v7_configs",
        lambda _root, *, write: calls.append("v7") or {"ok": True, "can_apply": False, "counts": {}},
    )
    monkeypatch.setattr(
        cluster_sync_worker,
        "_materialize_api_keys",
        lambda _root, *, write: calls.append("api_keys") or {
            "ok": True,
            "can_apply": False,
            "counts": {},
            "projection_paths": {"pb8": "/runtime/pb8/api-keys.json"},
            "projections": {"pb8": "current"},
        },
    )
    monkeypatch.setattr(
        cluster_sync_worker,
        "_materialize_credentials",
        lambda _root, *, write: calls.append("credentials") or {
            "ok": True,
            "can_apply": False,
            "counts": {},
            "tradfi_projection": {"status": "current"},
        },
    )
    monkeypatch.setattr(
        cluster_sync_worker,
        "_materialize_pb8_configs",
        lambda _root, *, write: calls.append("pb8") or {"ok": True, "can_apply": False, "counts": {}},
    )

    status = ClusterSyncWorker(tmp_path).run_once(reason="test")

    assert root == cluster_sync_worker.default_cluster_root(tmp_path)
    assert calls[:4] == ["v7", "api_keys", "credentials", "pb8"]
    assert status["pb8_materialization"]["ok"] is True


@pytest.mark.parametrize(
    ("api_result", "expected"),
    [
        (
            {"ok": True, "status": "missing", "counts": {}, "projection_paths": {"pb8": "/pb8/api-keys.json"}},
            "pb8_exchange_api_key_metadata_missing",
        ),
        (
            {"ok": True, "status": "current", "counts": {}, "projection_paths": {}, "projections": {}},
            "pb8_exchange_api_key_projection_target_missing",
        ),
        (
            {
                "ok": True,
                "status": "ready",
                "counts": {},
                "projection_paths": {"pb8": "/pb8/api-keys.json"},
                "projections": {"pb8": "write"},
            },
            "pb8_exchange_api_key_projection_not_current",
        ),
        (
            {"ok": False, "status": "error", "counts": {"error": 1}},
            "pb8_exchange_api_key_projection_error",
        ),
    ],
)
def test_worker_blocks_pb8_for_incomplete_exchange_projection(
    api_result: dict,
    expected: str,
) -> None:
    """Worker preflight reports each PB8 exchange projection failure without secrets."""

    credential_result = {
        "ok": True,
        "counts": {},
        "tradfi_projection": {"status": "current"},
    }

    assert expected in _pb8_projection_blockers(api_result, credential_result)


def test_worker_accepts_verified_pb8_exchange_projection_write() -> None:
    """A successful apply result is current even though it retains pre-write labels."""

    api_result = {
        "ok": True,
        "status": "written",
        "counts": {"written": 2},
        "projection_paths": {"pb8": "/pb8/api-keys.json"},
        "projections": {"pb8": "write"},
    }
    credential_result = {
        "ok": True,
        "counts": {},
        "tradfi_projection": {"status": "current"},
    }

    assert _pb8_projection_blockers(api_result, credential_result) == []


def test_capability_partition_defers_pb8_and_later_same_actor_operations() -> None:
    """An incapable peer receives no operation after an unsupported PB8 sequence."""

    operations = [
        {"actor": NODE_A, "seq": 1, "op": "STOP_INSTANCE"},
        {"actor": NODE_A, "seq": 2, "op": "UPSERT_PB8_CONFIG"},
        {"actor": NODE_A, "seq": 3, "op": "START_INSTANCE"},
        {"actor": NODE_B, "seq": 1, "op": "START_INSTANCE"},
    ]

    accepted, deferred = _partition_operations_for_peer_capabilities(operations, [])
    capable, capable_deferred = _partition_operations_for_peer_capabilities(
        operations,
        [PB8_OPERATION_CAPABILITY],
    )

    assert [(item["actor"], item["seq"]) for item in accepted] == [(NODE_A, 1), (NODE_B, 1)]
    assert deferred == 2
    assert capable == operations
    assert capable_deferred == 0
