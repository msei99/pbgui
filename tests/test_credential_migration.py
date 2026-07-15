"""Regression coverage for resumable legacy credential migration."""

from __future__ import annotations

import asyncio
import configparser
import json
import stat
import time
from pathlib import Path

import pytest

import PBApiServer
import credential_migration
from cluster_credential_publisher import ClusterCredentialPublisher
from cluster_credentials import (
    SecretContext,
    canonical_json_bytes,
    ensure_node_key_material,
    seal_secret,
    serialize_sealed_secret,
    sign_operation,
)
from cluster_sync_command import _append_credential_migration_acks, _materialize_credentials
from credential_migration import (
    CredentialMigrationBlocked,
    CredentialMigrationCoordinator,
    credential_migration_restart_block_reason,
    persist_credential_migration_error,
)
from credential_store import CredentialStore
from master.cluster_state import (
    append_operation,
    create_join_authorization,
    ensure_local_identity,
    load_operations,
    read_local_identity,
    rebuild_materialized_state,
    rotate_local_node_keys,
    write_operation,
)
import pbgui_purefunc


REMOTE_NODE_ID = "pbgui-node-22222222-2222-4222-8222-222222222222"


def test_non_elected_master_does_not_coordinate_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only one deterministic active master may publish cluster migration phases."""

    local_node_id = "pbgui-node-bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    elected_node_id = "pbgui-node-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    class FakeCoordinator:
        """Coordinator double that must not run on a non-elected master."""

        cluster_root = tmp_path / "data" / "cluster"

        @staticmethod
        def run() -> dict:
            pytest.fail("non-elected master must not run the migration coordinator")

    monkeypatch.setattr(credential_migration, "CredentialMigrationCoordinator", lambda *_args, **_kwargs: FakeCoordinator())
    monkeypatch.setattr(credential_migration, "read_local_identity", lambda _root: {"node_id": local_node_id})
    monkeypatch.setattr(
        credential_migration,
        "rebuild_materialized_state",
        lambda *_args, **_kwargs: {
            "cluster_nodes": {
                "nodes": {
                    local_node_id: {"role": "master", "enabled": True, "state_replica": True},
                    elected_node_id: {"role": "master", "enabled": True, "state_replica": True},
                    "pbgui-node-00000000-0000-4000-8000-000000000000": {
                        "role": "master",
                        "enabled": False,
                        "state_replica": True,
                    },
                },
            },
            "desired_state": {"credential_migration": {"freeze_generation": 9}},
        },
    )

    result = credential_migration.run_credential_migration(tmp_path)

    assert result == {
        "version": 1,
        "phase": "protocol_barrier",
        "status": "not_coordinator",
        "coordinator_node_id": elected_node_id,
        "freeze_generation": 9,
    }


def _write_ini(root: Path, body: str) -> Path:
    """Write one isolated migration INI fixture."""

    path = root / "pbgui.ini"
    path.write_text(body.strip() + "\n", encoding="utf-8")
    return path


def _json(path: Path, payload: object) -> None:
    """Write a JSON fixture after creating its parent directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=4) + "\n", encoding="utf-8")


def _mode(path: Path) -> int:
    """Return only permission bits for one fixture path."""

    return stat.S_IMODE(path.stat().st_mode)


def _signed_remote_operation(
    cluster_root: Path,
    remote_key_root: Path,
    op: str,
    payload: dict[str, object],
    seq: int,
) -> dict[str, object]:
    """Build a signed remote operation accepted by the local replica."""

    identity = read_local_identity(cluster_root)
    operation = {
        **payload,
        "schema_version": 1,
        "cluster_id": str(identity["cluster_id"]),
        "op_id": f"{REMOTE_NODE_ID}:{seq:08d}",
        "actor": REMOTE_NODE_ID,
        "seq": seq,
        "op": op,
        "created_at": int(time.time()) + 100 + seq,
    }
    if op != "ADD_NODE":
        operation.update({
            "actor_role_epoch": 1,
            "actor_membership_op_id": f"{REMOTE_NODE_ID}:00000001",
        })
    keys = ensure_node_key_material(remote_key_root)
    return sign_operation(
        operation,
        keys.signing_private_key,
        signer_id=REMOTE_NODE_ID,
    )


def test_all_legacy_sources_import_cleanup_scan_and_private_backups(tmp_path: Path) -> None:
    """INI, VPS JSON, and historical PB7 credentials migrate without sentinel residue."""

    sentinels = {
        "cmc-ini-secret",
        "cmc-host-secret",
        "cmc-master-secret",
        "cmc-pending-secret",
        "alpaca-key-secret",
        "alpaca-secret-secret",
        "polygon-key-secret",
    }
    ini_path = _write_ini(
        tmp_path,
        """
        [main]
        role = master
        pbname = local-test

        [coinmarketcap]
        api_key = cmc-ini-secret
        credits_used_day = 4
        credits_left = 9996
        fetch_limit = 1000

        [tradfi_profiles]
        alpaca_key = alpaca-key-secret
        alpaca_secret = alpaca-secret-secret
        display_mode = compact
        """,
    )
    inventory_root = tmp_path / "data" / "vpsmanager"
    host_path = inventory_root / "hosts" / "node.json"
    master_path = inventory_root / "masters" / "master.json"
    pending_path = inventory_root / "pending" / "change.json"
    _json(host_path, {"name": "node", "coinmarketcap_api_key": "cmc-host-secret"})
    _json(master_path, {"settings": {"coinmarketcap_api_key": "cmc-master-secret"}})
    _json(pending_path, {"patch": [{"coinmarketcap_api_key": "cmc-pending-secret"}]})
    pb7_root = tmp_path / "pb7"
    pb7_root.mkdir()
    pb7_path = pb7_root / "api-keys.json"
    _json(
        pb7_path,
        {
            "binance": {"key": "exchange-key"},
            "tradfi": {
                "polygon": {"api_key": "polygon-key-secret", "enabled": "1"},
            },
        },
    )

    result = CredentialMigrationCoordinator(tmp_path, pb7_root=pb7_root).run()

    assert result["phase"] == "complete", result
    store = CredentialStore(tmp_path / "data" / "credentials")
    assert {store.load_cmc_key(item["id"]) for item in store.list_cmc(active_only=True)} == {
        "cmc-ini-secret",
        "cmc-host-secret",
        "cmc-master-secret",
        "cmc-pending-secret",
    }
    tradfi = {
        item["provider"]: store.load_tradfi_credentials(item["id"])
        for item in store.list_tradfi(active_only=True)
    }
    assert tradfi == {
        "alpaca": {"api_key": "alpaca-key-secret", "api_secret": "alpaca-secret-secret"},
        "polygon": {"api_key": "polygon-key-secret"},
    }

    parser = configparser.ConfigParser()
    parser.read(ini_path, encoding="utf-8")
    assert parser.get("coinmarketcap", "fetch_limit") == "1000"
    assert not parser.has_option("coinmarketcap", "api_key")
    assert not parser.has_option("coinmarketcap", "credits_used_day")
    assert not parser.has_option("coinmarketcap", "credits_left")
    assert parser.get("tradfi_profiles", "display_mode") == "compact"
    assert not parser.has_option("tradfi_profiles", "alpaca_key")
    assert not parser.has_option("tradfi_profiles", "alpaca_secret")
    for path in (host_path, master_path, pending_path):
        assert "coinmarketcap_api_key" not in path.read_text(encoding="utf-8")
    projected = json.loads(pb7_path.read_text(encoding="utf-8"))
    assert projected["binance"] == {"key": "exchange-key"}
    assert projected["tradfi"]["_projection_generation"] >= 1
    assert projected["tradfi"]["_source_fingerprint"]

    state_path = tmp_path / "data" / "credentials" / "migration" / "state.json"
    state_text = state_path.read_text(encoding="utf-8")
    assert not any(sentinel in state_text for sentinel in sentinels)
    assert _mode(state_path) == 0o600
    migration_root = state_path.parent
    assert _mode(migration_root) == 0o700
    backups = list((migration_root / "backups").rglob("*.bak"))
    assert len(backups) == 5
    assert all(_mode(path) == 0o600 for path in backups)
    assert all(_mode(path.parent) == 0o700 for path in backups)
    assert CredentialMigrationCoordinator(tmp_path, pb7_root=pb7_root).run()["phase"] == "complete"


def test_clean_install_initializes_local_cluster_crypto(tmp_path: Path) -> None:
    """A no-source migration still establishes the required local v2 identity."""

    _write_ini(tmp_path, "[main]\nrole = master\npbname = clean-install")

    result = CredentialMigrationCoordinator(tmp_path).run()

    assert result["phase"] == "complete"
    cluster_root = tmp_path / "data" / "cluster"
    identity = read_local_identity(cluster_root)
    node = rebuild_materialized_state(cluster_root, write=False)["cluster_nodes"]["nodes"][identity["node_id"]]
    assert node["credential_protocol_version"] == 2
    assert node["credential_capable"] is True
    assert node["signing_public_key"]
    assert node["encryption_public_key"]


def test_publish_crash_retries_without_duplicate_import(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A crash after publication resumes with the same imported credential generation."""

    ini_path = _write_ini(tmp_path, "[coinmarketcap]\napi_key = crash-secret")
    original = ClusterCredentialPublisher.publish_cmc
    crashed = False

    def publish_then_crash(self, *args, **kwargs):
        nonlocal crashed
        result = original(self, *args, **kwargs)
        if not crashed:
            crashed = True
            raise RuntimeError("simulated post-publish crash")
        return result

    monkeypatch.setattr(ClusterCredentialPublisher, "publish_cmc", publish_then_crash)
    first = CredentialMigrationCoordinator(tmp_path).run()
    assert first["phase"] == "import_publish"
    assert "simulated post-publish crash" in first["blocker_reason"]
    assert "crash-secret" in ini_path.read_text(encoding="utf-8")

    monkeypatch.setattr(ClusterCredentialPublisher, "publish_cmc", original)
    resumed = CredentialMigrationCoordinator(tmp_path).run()
    store = CredentialStore(tmp_path / "data" / "credentials")
    assert resumed["phase"] == "complete"
    assert len(store.list_cmc()) == 1
    assert store.load_cmc_key(store.list_cmc()[0]["id"]) == "crash-secret"


def test_cleanup_crash_recognizes_already_written_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A crash after atomic cleanup does not restore or re-delete the legacy file."""

    ini_path = _write_ini(tmp_path, "[coinmarketcap]\napi_key = cleanup-crash-secret")
    original = credential_migration.atomic_write_private_bytes
    crashed = False

    def write_then_crash(path: Path, payload: bytes) -> None:
        nonlocal crashed
        original(path, payload)
        if Path(path) == ini_path and not crashed:
            crashed = True
            raise RuntimeError("simulated post-cleanup crash")

    monkeypatch.setattr(credential_migration, "atomic_write_private_bytes", write_then_crash)
    first = CredentialMigrationCoordinator(tmp_path).run()
    assert first["phase"] == "legacy_cleanup"
    assert "cleanup-crash-secret" not in ini_path.read_text(encoding="utf-8")

    monkeypatch.setattr(credential_migration, "atomic_write_private_bytes", original)
    resumed = CredentialMigrationCoordinator(tmp_path).run()
    assert resumed["phase"] == "complete"
    assert resumed["scan"]["status"] == "clean"


def test_fingerprint_change_restarts_inventory_without_deleting_new_value(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A changed source pauses at a new inventory generation before any cleanup."""

    ini_path = _write_ini(tmp_path, "[coinmarketcap]\napi_key = first-generation")
    original = CredentialMigrationCoordinator._phase_inventory

    def stop_before_inventory(self, state):
        desired = rebuild_materialized_state(self.cluster_root, write=False)["desired_state"]
        assert desired["credential_migration"]["freeze_acks"]
        raise CredentialMigrationBlocked("pause before inventory")

    monkeypatch.setattr(CredentialMigrationCoordinator, "_phase_inventory", stop_before_inventory)
    first = CredentialMigrationCoordinator(tmp_path).run()
    assert first["phase"] == "inventory"
    ini_path.write_text("[coinmarketcap]\napi_key = second-generation\n", encoding="utf-8")

    monkeypatch.setattr(CredentialMigrationCoordinator, "_phase_inventory", original)
    changed = CredentialMigrationCoordinator(tmp_path).run()
    store = CredentialStore(tmp_path / "data" / "credentials")
    assert changed["phase"] == "complete"
    assert {store.load_cmc_key(item["id"]) for item in store.list_cmc()} == {"second-generation"}


def test_remote_freeze_and_materialization_acks_gate_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Every active v2 node must ACK both exact barriers before legacy deletion."""

    ini_path = _write_ini(tmp_path, "[main]\nrole = master\n[coinmarketcap]\napi_key = remote-barrier-secret")
    cluster_root = tmp_path / "data" / "cluster"
    store = CredentialStore(tmp_path / "data" / "credentials")
    local_identity = ensure_local_identity(cluster_root, role="master", pbname="local")
    ClusterCredentialPublisher(cluster_root, store)._ensure_local_crypto_membership()
    remote_key_root = tmp_path / "remote-cluster"
    remote_keys = ensure_node_key_material(remote_key_root)
    authorization = create_join_authorization(cluster_root, REMOTE_NODE_ID, "vps")
    write_operation(
        cluster_root,
        _signed_remote_operation(
            cluster_root,
            remote_key_root,
            "ADD_NODE",
            {
                **remote_keys.public_bundle(REMOTE_NODE_ID, "vps"),
                "node_id": REMOTE_NODE_ID,
                "role": "vps",
                "credential_protocol_version": 2,
                "credential_capable": True,
                "membership_authorization": authorization,
            },
            1,
        ),
        network_input=True,
    )

    inventory_reads = 0
    original_inventory = CredentialMigrationCoordinator._inventory_sources

    def observed_inventory(self):
        nonlocal inventory_reads
        inventory_reads += 1
        return original_inventory(self)

    monkeypatch.setattr(CredentialMigrationCoordinator, "_inventory_sources", observed_inventory)
    waiting_freeze = CredentialMigrationCoordinator(tmp_path).run()
    assert waiting_freeze["phase"] == "protocol_barrier", waiting_freeze
    assert REMOTE_NODE_ID in waiting_freeze["blocker_reason"]
    assert "remote-barrier-secret" in ini_path.read_text(encoding="utf-8")
    assert store.list_cmc() == []
    assert inventory_reads == 0

    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    migration = desired["credential_migration"]
    freeze_ack = _signed_remote_operation(
        cluster_root,
        remote_key_root,
        "WRITER_FREEZE_ACK",
        {
            "freeze_generation": migration["freeze_generation"],
            "node_id": REMOTE_NODE_ID,
            "frozen": True,
        },
        2,
    )
    write_operation(cluster_root, freeze_ack)

    waiting_inventory = CredentialMigrationCoordinator(tmp_path).run()
    assert waiting_inventory["phase"] == "inventory"
    assert REMOTE_NODE_ID in waiting_inventory["blocker_reason"]
    assert inventory_reads == 1
    inventory_ack = _signed_remote_operation(
        cluster_root,
        remote_key_root,
        "CREDENTIAL_INVENTORY_ACK",
        {
            "freeze_generation": migration["freeze_generation"],
            "node_id": REMOTE_NODE_ID,
            "source_fingerprints": {"remote-source": "b" * 64},
            "source_generations": {"remote-source": 9},
        },
        3,
    )
    write_operation(cluster_root, inventory_ack)

    waiting_materialization = CredentialMigrationCoordinator(tmp_path).run()
    assert waiting_materialization["phase"] == "materialization_confirmation"
    assert REMOTE_NODE_ID in waiting_materialization["blocker_reason"]
    assert "remote-barrier-secret" in ini_path.read_text(encoding="utf-8")
    assert len(store.list_cmc(active_only=True)) == 1

    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    expected = {
        secret_id: int(secret["generation"])
        for secret_id, secret in desired["secrets"].items()
        if secret["audience"] == "cluster"
    }
    materialization_ack = _signed_remote_operation(
        cluster_root,
        remote_key_root,
        "CREDENTIAL_MATERIALIZATION_ACK",
        {
            "freeze_generation": desired["credential_migration"]["freeze_generation"],
            "node_id": REMOTE_NODE_ID,
            "credential_generations": expected,
        },
        4,
    )
    write_operation(cluster_root, materialization_ack)

    completed = CredentialMigrationCoordinator(tmp_path).run()
    assert completed["phase"] == "cleanup_confirmation"
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    cutoff = desired["credential_migration"]["cutoff"]
    cleanup_ack = _signed_remote_operation(
        cluster_root,
        remote_key_root,
        "CREDENTIAL_CUTOFF_ACK",
        {
            "cutoff_generation": cutoff["cutoff_generation"],
            "node_id": REMOTE_NODE_ID,
            "state_vector": cutoff["state_vector"],
            "removed_secret_blob_hashes": cutoff["obsolete_secret_blob_hashes"],
        },
        5,
    )
    write_operation(cluster_root, cleanup_ack)
    completed = CredentialMigrationCoordinator(tmp_path).run()
    assert completed["phase"] == "scan"
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    blocked_scan_ack = _signed_remote_operation(
        cluster_root,
        remote_key_root,
        "CREDENTIAL_SCAN_ACK",
        {
            "freeze_generation": desired["credential_migration"]["freeze_generation"],
            "cutoff_generation": desired["credential_migration"]["cutoff"]["cutoff_generation"],
            "node_id": REMOTE_NODE_ID,
            "status": "blocked",
            "clean": False,
            "findings": [{"path_category": "pbgui.ini:legacy-field"}],
        },
        6,
    )
    write_operation(cluster_root, blocked_scan_ack)
    blocked = CredentialMigrationCoordinator(tmp_path).run()
    assert blocked["phase"] == "scan"
    assert REMOTE_NODE_ID in blocked["blocker_reason"]
    clean_scan_ack = _signed_remote_operation(
        cluster_root,
        remote_key_root,
        "CREDENTIAL_SCAN_ACK",
        {
            "freeze_generation": desired["credential_migration"]["freeze_generation"],
            "cutoff_generation": desired["credential_migration"]["cutoff"]["cutoff_generation"],
            "node_id": REMOTE_NODE_ID,
            "status": "clean",
            "clean": True,
            "findings": [],
        },
        7,
    )
    write_operation(cluster_root, clean_scan_ack)
    completed = CredentialMigrationCoordinator(tmp_path).run()
    assert completed["phase"] == "complete"
    assert "remote-barrier-secret" not in ini_path.read_text(encoding="utf-8")
    assert str(local_identity["node_id"]) in desired["credential_migration"]["freeze_acks"]


def test_empty_materialization_ack_is_exact_and_idempotent(tmp_path: Path) -> None:
    """A usage-only migration can ACK an empty credential generation map once."""

    cluster_root = tmp_path / "data" / "cluster"
    store = CredentialStore(tmp_path / "data" / "credentials")
    ensure_local_identity(cluster_root, role="master", pbname="local")
    ClusterCredentialPublisher(cluster_root, store)._ensure_local_crypto_membership()
    append_operation(
        cluster_root,
        "WRITER_FREEZE",
        {
            "freeze_generation": 1,
            "frozen": True,
            "migration_operation_id": "usage-only",
            "source_fingerprints": {},
            "source_generations": {},
        },
    )

    first = _append_credential_migration_acks(cluster_root)
    second = _append_credential_migration_acks(cluster_root)

    assert first["status"] == "acked"
    assert first["credential_generations"] == {}
    assert second == {
        "status": "already_acked",
        "recipient_ack": {"status": "already_acked"},
        "credential_generations": {},
        "recipient_generations": {},
    }
    operations = load_operations(cluster_root)
    assert sum(item["op"] == "WRITER_FREEZE_ACK" for item in operations) == 1
    assert sum(item["op"] == "CREDENTIAL_MATERIALIZATION_ACK" for item in operations) == 1


def test_freeze_and_inventory_acks_are_separate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Freeze ACK is value-blind and the later inventory ACK contains only local metadata."""

    cluster_root = tmp_path / "data" / "cluster"
    store = CredentialStore(tmp_path / "data" / "credentials")
    identity = ensure_local_identity(cluster_root, role="master", pbname="local")
    ClusterCredentialPublisher(cluster_root, store)._ensure_local_crypto_membership()
    append_operation(
        cluster_root,
        "WRITER_FREEZE",
        {
            "freeze_generation": 1,
            "frozen": True,
            "source_fingerprints": {"initiator": "a" * 64},
            "source_generations": {"initiator": 7},
        },
    )
    monkeypatch.setattr(
        credential_migration,
        "local_legacy_credential_inventory",
        lambda _root, **_kwargs: ({"local": "b" * 64}, {"local": 9}),
    )

    _append_credential_migration_acks(cluster_root)
    migration = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["credential_migration"]
    freeze_ack = migration["freeze_acks"][identity["node_id"]]
    inventory_ack = migration["inventory_acks"][identity["node_id"]]
    assert freeze_ack["frozen"] is True
    assert freeze_ack["process_readiness"] == []
    assert "source_fingerprints" not in freeze_ack
    assert inventory_ack["source_fingerprints"] == {"local": "b" * 64}
    assert inventory_ack["source_generations"] == {"local": 9}
    assert inventory_ack["process_readiness"] == []


def test_no_source_coordinator_waits_for_active_v1_replica_upgrade(tmp_path: Path) -> None:
    """A source-free coordinator reports passive upgrade wait without freezing."""

    _write_ini(tmp_path, "[main]\nrole = master")
    cluster_root = tmp_path / "data" / "cluster"
    store = CredentialStore(tmp_path / "data" / "credentials")
    ensure_local_identity(cluster_root, role="master", pbname="local")
    ClusterCredentialPublisher(cluster_root, store)._ensure_local_crypto_membership()
    remote_key_root = tmp_path / "remote-cluster"
    remote_keys = ensure_node_key_material(remote_key_root)
    authorization = create_join_authorization(cluster_root, REMOTE_NODE_ID, "vps")
    write_operation(
        cluster_root,
        _signed_remote_operation(
            cluster_root,
            remote_key_root,
            "ADD_NODE",
            {
                **remote_keys.public_bundle(REMOTE_NODE_ID, "vps"),
                "node_id": REMOTE_NODE_ID,
                "role": "vps",
                "credential_protocol_version": 1,
                "credential_capable": True,
                "membership_authorization": authorization,
            },
            1,
        ),
        network_input=True,
    )

    status = CredentialMigrationCoordinator(tmp_path).run()
    assert status["phase"] == "protocol_barrier"
    assert status["status"] == "waiting_for_upgrade"
    assert status["outdated_node_ids"] == [REMOTE_NODE_ID]
    assert REMOTE_NODE_ID in status["blocker_reason"]


def test_scan_covers_only_managed_logs_tasks_blobs_and_owned_argv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Scan findings expose managed paths and field names, never migrated values."""

    coordinator = CredentialMigrationCoordinator(tmp_path)
    record = coordinator.store.create_cmc("scan-sentinel")
    state = {
        "source_results": {
            "source": {"credentials": {"item": {
                "credential_id": record["id"],
                "generation": 1,
                "kind": "cmc_api_key",
            }}}
        }
    }
    log_path = tmp_path / "data" / "logs" / "managed.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text("redacted scan-sentinel payload", encoding="utf-8")
    task_path = tmp_path / "data" / "ohlcv" / "_tasks" / "task.json"
    task_path.parent.mkdir(parents=True)
    task_path.write_text(json.dumps({"metadata": {"api_key": "scan-sentinel"}}), encoding="utf-8")
    blob_path = tmp_path / "data" / "cluster" / "config_blobs" / "managed.json"
    blob_path.parent.mkdir(parents=True)
    blob_path.write_text("scan-sentinel", encoding="utf-8")

    class FakeProcess:
        """Current-user PBGui process metadata without exposing argv values in output."""

        info = {
            "pid": 42,
            "uids": type("Uids", (), {"real": credential_migration.os.getuid()})(),
            "cmdline": [str(tmp_path / "PBApiServer.py"), "scan-sentinel"],
        }

    monkeypatch.setattr(credential_migration.psutil, "process_iter", lambda _fields: [FakeProcess()])
    findings = coordinator._scan_for_legacy_values(state)
    rendered = json.dumps(findings)
    assert "data/logs/managed.log:managed-log" in findings
    assert "data/ohlcv/_tasks/task.json:$.metadata.api_key" in findings
    assert "data/cluster/config_blobs/managed.json:cluster-blob" in findings
    assert "process:42:argv[1]" in findings
    assert "scan-sentinel" not in rendered


def test_remote_managed_scan_uses_post_freeze_local_sentinels(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Replica scan parity covers managed log, task, blob, and owned argv paths."""

    ini_path = _write_ini(tmp_path, "[coinmarketcap]\napi_key = remote-scan-sentinel")
    cluster_root = tmp_path / "data" / "cluster"
    ensure_local_identity(cluster_root, role="master", pbname="remote")
    ClusterCredentialPublisher(
        cluster_root,
        CredentialStore(tmp_path / "data" / "credentials"),
    )._ensure_local_crypto_membership()
    append_operation(
        cluster_root,
        "WRITER_FREEZE",
        {"freeze_generation": 1, "frozen": True, "migration_operation_id": "scan-parity"},
    )
    _append_credential_migration_acks(cluster_root)
    log_path = tmp_path / "data" / "logs" / "remote.log"
    task_path = tmp_path / "data" / "ohlcv" / "_tasks" / "remote.json"
    blob_path = cluster_root / "config_blobs" / "remote.json"
    for path, content in (
        (log_path, "remote-scan-sentinel"),
        (task_path, json.dumps({"payload": {"token": "remote-scan-sentinel"}})),
        (blob_path, "remote-scan-sentinel"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    class FakeProcess:
        """Current-user PBGui process carrying the remote sentinel in argv."""

        info = {
            "pid": 77,
            "uids": type("Uids", (), {"real": credential_migration.os.getuid()})(),
            "cmdline": [str(tmp_path / "PBRun.py"), "remote-scan-sentinel"],
        }

    monkeypatch.setattr(credential_migration.psutil, "process_iter", lambda _fields: [FakeProcess()])
    findings = credential_migration.local_managed_credential_scan(tmp_path)

    assert "data/logs/remote.log:managed-log" in findings
    assert "data/ohlcv/_tasks/remote.json:$.payload.token" in findings
    assert "data/cluster/config_blobs/remote.json:cluster-blob" in findings
    assert "process:77:argv[1]" in findings
    assert "remote-scan-sentinel" not in json.dumps(findings)
    assert "remote-scan-sentinel" in ini_path.read_text(encoding="utf-8")


def test_existing_current_generation_is_reused_and_published(tmp_path: Path) -> None:
    """A matching existing key publishes its current generation, never stale generation one."""

    _write_ini(tmp_path, "[coinmarketcap]\napi_key = current-generation-secret")
    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc("old-generation-secret")
    record = store.update_cmc(record["id"], api_key="current-generation-secret")
    assert record["generation"] == 2

    result = CredentialMigrationCoordinator(tmp_path, credential_store=store).run()

    assert result["phase"] == "complete"
    assert len(store.list_cmc()) == 1
    desired = rebuild_materialized_state(tmp_path / "data" / "cluster", write=False)["desired_state"]
    assert desired["secrets"][record["id"]]["generation"] == 2
    assert desired["cmc_pool"]["entries"][record["id"]]["catalog_generation"] == 2


def test_coordinator_cross_source_values_share_one_credential(tmp_path: Path) -> None:
    """Equal INI, inventory, and PB7 values dedupe across coordinator origins."""

    _write_ini(
        tmp_path,
        """
        [coinmarketcap]
        api_key = shared-coordinator-cmc

        [tradfi_profiles]
        tiingo_api_key = shared-coordinator-tradfi
        """,
    )
    _json(
        tmp_path / "data" / "vpsmanager" / "hosts" / "node.json",
        {"coinmarketcap_api_key": "shared-coordinator-cmc"},
    )
    pb7_root = tmp_path / "pb7"
    pb7_root.mkdir()
    _json(
        pb7_root / "api-keys.json",
        {"tradfi": {"provider": "tiingo", "api_key": "shared-coordinator-tradfi"}},
    )

    result = CredentialMigrationCoordinator(tmp_path, pb7_root=pb7_root).run()
    store = CredentialStore(tmp_path / "data" / "credentials")

    assert result["phase"] == "complete", result
    assert len(store.list_cmc()) == 1
    assert len(store.list_tradfi()) == 1
    credential_ids = [
        record["credential_id"]
        for source in result["source_results"].values()
        for record in source["credentials"].values()
    ]
    assert len(credential_ids) == 4
    assert len(set(credential_ids)) == 2

    log_path = tmp_path / "data" / "logs" / "post-completion.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("shared-coordinator-cmc", encoding="utf-8")
    rescanned = CredentialMigrationCoordinator(tmp_path, pb7_root=pb7_root).run()
    assert rescanned["phase"] == "protocol_barrier"
    assert "post-completion.log" in rescanned["blocker_reason"]
    assert "shared-coordinator-cmc" not in json.dumps(rescanned)


@pytest.mark.parametrize(
    ("kind", "provider", "value"),
    [
        ("cmc_api_key", "", "same-cmc-value"),
        ("tradfi_profile", "tiingo", {"api_key": "same-tradfi-value"}),
    ],
)
def test_candidate_plaintext_dedupes_to_existing_id(
    tmp_path: Path,
    kind: str,
    provider: str,
    value: object,
) -> None:
    """CMC and same-provider TradFi candidates reuse an identical existing vault ID."""

    root = tmp_path / "data" / "cluster"
    ensure_local_identity(root, role="master", pbname="master")
    store = CredentialStore(tmp_path / "data" / "credentials")
    publisher = ClusterCredentialPublisher(root, store)
    keys, materialized = publisher._ensure_local_crypto_membership()
    existing = (
        store.create_cmc(str(value))
        if kind == "cmc_api_key"
        else store.create_tradfi(provider, value)
    )
    candidate_id = f"candidate_{kind}_dedupe"
    context = SecretContext(
        cluster_id=materialized["desired_state"]["cluster_id"],
        secret_id=candidate_id,
        kind=f"migration_{kind}",
        generation=1,
        audience="masters",
    )
    recipients, _roles = publisher._audience_recipients(materialized, "masters")
    plaintext = {"kind": kind, "value": value}
    if provider:
        plaintext["provider"] = provider
    envelope = seal_secret(
        canonical_json_bytes(plaintext),
        context,
        recipients,
        keys.signing_private_key,
        signer_id=read_local_identity(root)["node_id"],
    )
    blob_hash, _path, _created = publisher._write_sealed_blob(serialize_sealed_secret(envelope))
    candidate = {
        "candidate_id": candidate_id,
        "candidate_kind": kind,
        "freeze_generation": 1,
        "sealed_blob_hash": blob_hash,
        "recipient_ids": [recipient.node_id for recipient in recipients],
        "submitted_by": read_local_identity(root)["node_id"],
    }

    credential_id, generation = credential_migration._materialize_candidate(
        CredentialMigrationCoordinator(tmp_path, credential_store=store),
        materialized,
        candidate,
    )

    assert (credential_id, generation) == (existing["id"], existing["generation"])
    assert len(store.list_cmc() if kind == "cmc_api_key" else store.list_tradfi()) == 1


def test_differing_tradfi_candidate_requires_secret_free_resolution(tmp_path: Path) -> None:
    """Differing same-provider credentials stay inactive until a signed admin choice."""

    root = tmp_path / "data" / "cluster"
    ensure_local_identity(root, role="master", pbname="master")
    store = CredentialStore(tmp_path / "data" / "credentials")
    existing = store.create_tradfi("tiingo", {"api_key": "existing-tradfi-secret"})
    publisher = ClusterCredentialPublisher(root, store)
    keys, materialized = publisher._ensure_local_crypto_membership()
    publisher.publish_tradfi(existing["id"])
    publisher.set_tradfi_active_profile("tiingo", existing["id"])
    materialized = rebuild_materialized_state(root, write=False)
    candidate_id = "candidate_tradfi_conflict"
    context = SecretContext(
        cluster_id=materialized["desired_state"]["cluster_id"],
        secret_id=candidate_id,
        kind="migration_tradfi_profile",
        generation=1,
        audience="masters",
    )
    recipients, _roles = publisher._audience_recipients(materialized, "masters")
    envelope = seal_secret(
        canonical_json_bytes({
            "kind": "tradfi_profile",
            "provider": "tiingo",
            "value": {"api_key": "candidate-tradfi-secret"},
        }),
        context,
        recipients,
        keys.signing_private_key,
        signer_id=read_local_identity(root)["node_id"],
    )
    blob_hash, _path, _created = publisher._write_sealed_blob(serialize_sealed_secret(envelope))
    candidate = {
        "candidate_id": candidate_id,
        "candidate_kind": "tradfi_profile",
        "freeze_generation": 1,
        "sealed_blob_hash": blob_hash,
        "recipient_ids": [recipient.node_id for recipient in recipients],
        "submitted_by": read_local_identity(root)["node_id"],
    }
    coordinator = CredentialMigrationCoordinator(tmp_path, credential_store=store)

    assert credential_migration._materialize_candidate(coordinator, materialized, candidate) == ("", 0)
    migration = rebuild_materialized_state(root, write=False)["desired_state"]["credential_migration"]
    conflict = next(iter(migration["conflicts"].values()))
    rendered = json.dumps(conflict)
    assert conflict["status"] == "unresolved"
    assert store.list_tradfi(active_only=True) == []
    assert "existing-tradfi-secret" not in rendered
    assert "candidate-tradfi-secret" not in rendered

    resolution = credential_migration.resolve_migration_conflict(
        tmp_path,
        conflict["conflict_id"],
        "candidate",
        resolution_id="admin-resolution-1",
    )
    materialized = rebuild_materialized_state(root, write=False)
    credential_id, generation = credential_migration._materialize_candidate(
        coordinator,
        materialized,
        candidate,
    )

    assert resolution["resolution"]["choice"] == "candidate"
    assert credential_id.startswith("tradfi_") and credential_id != existing["id"]
    assert generation == 1
    assert store.get_tradfi(credential_id)["pending"] is True


def test_local_master_and_vps_tradfi_conflict_blocks_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Divergent forced-command and VPS TradFi origins emit a signed cleanup blocker."""

    cluster_root = tmp_path / "data" / "cluster"
    pb7_root = tmp_path / "pb7"
    pb7_root.mkdir()
    _json(
        pb7_root / "api-keys.json",
        {"tradfi": {"provider": "tiingo", "api_key": "local-master-tradfi"}},
    )
    _write_ini(tmp_path, f"[main]\nrole = master\npb7dir = {pb7_root}")
    store = CredentialStore(tmp_path / "data" / "credentials")
    ensure_local_identity(cluster_root, role="master", pbname="master")
    publisher = ClusterCredentialPublisher(cluster_root, store)
    keys, _materialized = publisher._ensure_local_crypto_membership()
    append_operation(
        cluster_root,
        "WRITER_FREEZE",
        {"freeze_generation": 1, "frozen": True, "migration_operation_id": "cross-origin"},
    )
    monkeypatch.setattr("credential_reconciler.pb7dir", lambda: str(pb7_root))
    coordinator = CredentialMigrationCoordinator(tmp_path, pb7_root=pb7_root, credential_store=store)
    sources = coordinator._inventory_sources()
    migration = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["credential_migration"]

    assert credential_migration._import_local_master_sources(
        coordinator,
        sources,
        migration,
        max_items=8,
    ) == 1
    materialized = rebuild_materialized_state(cluster_root, write=False)
    candidate_id = "candidate_cross_origin_tradfi"
    context = SecretContext(
        cluster_id=materialized["desired_state"]["cluster_id"],
        secret_id=candidate_id,
        kind="migration_tradfi_profile",
        generation=1,
        audience="masters",
    )
    recipients, _roles = publisher._audience_recipients(materialized, "masters")
    envelope = seal_secret(
        canonical_json_bytes({
            "kind": "tradfi_profile",
            "provider": "tiingo",
            "value": {"api_key": "remote-vps-tradfi"},
        }),
        context,
        recipients,
        keys.signing_private_key,
        signer_id=read_local_identity(cluster_root)["node_id"],
    )
    blob_hash, _path, _created = publisher._write_sealed_blob(serialize_sealed_secret(envelope))
    candidate = {
        "candidate_id": candidate_id,
        "candidate_kind": "tradfi_profile",
        "freeze_generation": 1,
        "sealed_blob_hash": blob_hash,
        "recipient_ids": [recipient.node_id for recipient in recipients],
        "submitted_by": read_local_identity(cluster_root)["node_id"],
    }

    assert credential_migration._materialize_candidate(coordinator, materialized, candidate) == ("", 0)
    materialized = rebuild_materialized_state(cluster_root, write=False)
    conflicts = materialized["desired_state"]["credential_migration"]["conflicts"]
    conflict_operation = next(
        operation for operation in load_operations(cluster_root)
        if operation["op"] == "MIGRATION_SECRET_CONFLICT"
    )
    assert next(iter(conflicts.values()))["status"] == "unresolved"
    assert conflict_operation["signature"]
    assert store.list_tradfi(active_only=True) == []
    cutoff_base = rebuild_materialized_state(cluster_root, write=False)
    cutoff = append_operation(
        cluster_root,
        "CREDENTIAL_CUTOFF",
        {
            "cutoff_generation": 1,
            "parent_generation": 0,
            "state_vector": cutoff_base["state_vector"],
            "min_protocol": 2,
            "obsolete_secret_blob_hashes": [],
        },
    )
    append_operation(
        cluster_root,
        "CREDENTIAL_CUTOFF_ACK",
        {
            "cutoff_generation": 1,
            "node_id": read_local_identity(cluster_root)["node_id"],
            "state_vector": cutoff["state_vector"],
            "removed_secret_blob_hashes": [],
        },
    )
    materialized = rebuild_materialized_state(cluster_root, write=False)
    assert credential_migration._cleanup_accepted_local_sources(
        coordinator,
        materialized,
        sources,
        role="master",
    ) == 0
    assert "local-master-tradfi" in (pb7_root / "api-keys.json").read_text(encoding="utf-8")
    assert "local-master-tradfi" not in json.dumps(conflicts)
    assert "remote-vps-tradfi" not in json.dumps(conflicts)


def test_writer_denylists_reject_bulk_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exact retired CMC and TradFi fields fail atomically while safe fields remain writable."""

    ini_path = _write_ini(tmp_path, "[safe]\nvalue = original")
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    before = ini_path.read_bytes()

    with pytest.raises(ValueError, match="permanently read-only"):
        pbgui_purefunc.save_ini_section(
            "tradfi_profiles",
            {"display_mode": "compact", "alpaca_api_secret_key": "blocked"},
        )
    assert ini_path.read_bytes() == before

    with pytest.raises(ValueError, match="permanently read-only"):
        pbgui_purefunc.save_ini("coinmarketcap", "credits_used_month", "12")
    assert ini_path.read_bytes() == before

    pbgui_purefunc.save_ini("coinmarketcap", "cpt_user.binance", "safe-user")
    parser = configparser.ConfigParser()
    parser.read(ini_path, encoding="utf-8")
    assert parser.get("safe", "value") == "original"
    assert parser.get("coinmarketcap", "cpt_user.binance") == "safe-user"


def test_migration_blocker_is_persisted_and_blocks_api_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Credential startup failures survive process state and return an API restart blocker."""

    persist_credential_migration_error("recovery required", tmp_path)
    assert "recovery required" in credential_migration_restart_block_reason(tmp_path)
    monkeypatch.setattr(PBApiServer, "PBGDIR", str(tmp_path))

    blocked, reason = asyncio.run(PBApiServer._restart_block_state())

    assert blocked is True
    assert "recovery required" in reason


def test_waiting_for_upgrade_does_not_block_required_api_restart(tmp_path: Path) -> None:
    """An outdated-process barrier must allow the restart that resolves it."""

    state_path = tmp_path / "data" / "credentials" / "migration" / "state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({
            "version": 1,
            "phase": "protocol_barrier",
            "status": "waiting_for_upgrade",
            "blocker_reason": "Local credential services are waiting for protocol v2: PBApiServer",
        }),
        encoding="utf-8",
    )

    assert credential_migration_restart_block_reason(tmp_path) == ""


@pytest.mark.parametrize("reason", [
    "Waiting for writer-freeze ACK from active Cluster nodes: node-a",
    "Waiting for credential inventory ACK from active Cluster nodes: node-a",
    "Waiting for credential materialization ACK from active Cluster nodes: node-a",
    "Waiting for credential cutoff cleanup ACK from active Cluster nodes: node-a",
    "Waiting for credential scan ACK from active Cluster nodes: node-a",
])
def test_passive_cluster_ack_wait_does_not_block_api_restart(tmp_path: Path, reason: str) -> None:
    """Persisted remote-ACK waits are restart-safe and resume from migration state."""

    state_path = tmp_path / "data" / "credentials" / "migration" / "state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({
            "version": 1,
            "phase": "protocol_barrier",
            "status": "pending",
            "blocker_reason": reason,
        }),
        encoding="utf-8",
    )

    assert credential_migration_restart_block_reason(tmp_path) == ""


def test_migration_exception_replaces_stale_waiting_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A real coordinator exception remains restart-blocking after upgrade waiting."""

    coordinator = CredentialMigrationCoordinator(tmp_path)
    coordinator._write_state({
        "version": 1,
        "phase": "protocol_barrier",
        "status": "waiting_for_upgrade",
        "operation_id": "migration-op",
        "sources": {},
        "source_results": {},
        "operation_ids": [],
    })
    monkeypatch.setattr(
        coordinator,
        "_phase_protocol_barrier",
        lambda _state: (_ for _ in ()).throw(RuntimeError("broken barrier")),
    )

    result = coordinator.run()

    assert result["status"] == "error"
    assert "broken barrier" in credential_migration_restart_block_reason(tmp_path)


def test_post_completion_startup_error_remains_a_restart_blocker(tmp_path: Path) -> None:
    """A later credential startup failure is not hidden by an earlier completed phase."""

    _write_ini(tmp_path, "[main]\nrole = master")
    assert CredentialMigrationCoordinator(tmp_path).run()["phase"] == "complete"

    persist_credential_migration_error("post-completion failure", tmp_path)

    assert credential_migration_restart_block_reason(tmp_path) == "post-completion failure"


def test_sentinel_scan_blocks_reintroduced_legacy_field(tmp_path: Path) -> None:
    """The final scan reports a migrated value reintroduced into a forbidden legacy field."""

    ini_path = _write_ini(tmp_path, "[coinmarketcap]\napi_key = scan-sentinel")
    coordinator = CredentialMigrationCoordinator(tmp_path)
    result = coordinator.run()
    assert result["phase"] == "complete"
    ini_path.write_text("[coinmarketcap]\napi_key = scan-sentinel\n", encoding="utf-8")

    findings = coordinator._scan_for_legacy_values(result)

    assert "pbgui.ini:[coinmarketcap] api_key" in findings
    assert "pbgui.ini:migrated-value" in findings

    rerun = coordinator.run()
    assert rerun["phase"] == "protocol_barrier"
    assert "reappeared after migration" in rerun["blocker_reason"]


def test_malformed_ini_error_never_persists_secret_source_text(tmp_path: Path) -> None:
    """ConfigParser diagnostics are replaced before migration state is persisted."""

    secret_line = "api_key = parser-must-not-echo"
    _write_ini(tmp_path, f"[coinmarketcap]\n{secret_line}\nmalformed continuation")

    result = CredentialMigrationCoordinator(tmp_path).run()
    state_path = tmp_path / "data" / "credentials" / "migration" / "state.json"
    state_text = state_path.read_text(encoding="utf-8")

    assert result["phase"] == "inventory"
    assert "Unable to parse legacy INI source" in result["blocker_reason"]
    assert "parser-must-not-echo" not in state_text
    assert "malformed continuation" not in state_text


def test_legacy_api_key_checkpoint_excludes_tradfi_and_prunes_old_blob(tmp_path: Path) -> None:
    """Migration advances desired API keys before deleting the obsolete local plaintext blob."""

    _write_ini(tmp_path, "[coinmarketcap]\napi_key = checkpoint-cmc")
    cluster_root = tmp_path / "data" / "cluster"
    ensure_local_identity(cluster_root, role="master", pbname="local")
    paths = credential_migration.ClusterPaths.from_root(cluster_root)
    legacy_payload = {
        "_api_serial": 1,
        "alice": {"exchange": "binance", "secret": "exchange-secret"},
        "tradfi": {"provider": "tiingo", "api_key": "legacy-tradfi-secret"},
    }
    legacy_raw = json.dumps(legacy_payload, indent=4).encode("utf-8")
    legacy_hash = credential_migration._write_content_blob(paths.secret_blobs, legacy_raw)
    config_hash = credential_migration._write_content_blob(paths.config_blobs, b"{}")
    append_operation(
        cluster_root,
        "UPSERT_API_KEYS",
        {
            "api_serial": 1,
            "payload_hash": config_hash,
            "secret_blob_hash": legacy_hash,
        },
    )

    result = CredentialMigrationCoordinator(tmp_path).run()
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["api_keys"]
    current_raw = credential_migration._read_content_blob(paths.secret_blobs, desired["secret_blob_hash"])
    current = json.loads(current_raw.decode("utf-8"))

    assert result["phase"] == "complete"
    assert "tradfi" not in current
    assert current["alice"]["secret"] == "exchange-secret"
    assert desired["secret_blob_hash"] != legacy_hash
    assert not credential_migration._content_blob_path(paths.secret_blobs, legacy_hash).exists()
    migration = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["credential_migration"]
    assert migration["frozen"] is False
    assert migration["cutoff"]["min_protocol"] == 2
    assert list(migration["cleanup_acks"]) == [read_local_identity(cluster_root)["node_id"]]


def test_exact_legacy_writer_reports_active_freeze(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The permanent INI guard also enforces the materialized migration freeze flag."""

    ini_path = _write_ini(tmp_path, "[main]\nrole = master")
    desired_path = tmp_path / "data" / "cluster" / "desired_state.json"
    _json(desired_path, {"credential_migration": {"frozen": True}})
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)

    with pytest.raises(ValueError, match="frozen during migration"):
        pbgui_purefunc.save_ini("coinmarketcap", "api_key", "blocked-secret")
    assert "blocked-secret" not in ini_path.read_text(encoding="utf-8")


def test_vps_candidates_relay_import_accept_and_cleanup_without_plaintext(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A frozen VPS relays CMC/TradFi candidates and cleans only after acceptance/cutoff."""

    cluster_id = "pbgui-cluster-11111111-1111-4111-8111-111111111111"
    master_node = "pbgui-node-11111111-1111-4111-8111-111111111111"
    master_root = tmp_path / "master"
    vps_root = tmp_path / "vps"
    master_cluster = master_root / "data" / "cluster"
    vps_cluster = vps_root / "data" / "cluster"
    ensure_local_identity(master_cluster, role="master", pbname="master", cluster_id=cluster_id, node_id=master_node)
    master_membership = append_operation(master_cluster, "ADD_NODE", {"node_id": master_node, "role": "master"})
    authorization = create_join_authorization(master_cluster, REMOTE_NODE_ID, "vps")
    ensure_local_identity(vps_cluster, role="vps", pbname="vps", cluster_id=cluster_id, node_id=REMOTE_NODE_ID)
    write_operation(vps_cluster, master_membership, network_input=True)
    vps_membership = append_operation(
        vps_cluster,
        "ADD_NODE",
        {"node_id": REMOTE_NODE_ID, "role": "vps", "membership_authorization": authorization},
    )
    write_operation(master_cluster, vps_membership, network_input=True)
    rebuild_materialized_state(master_cluster)
    rebuild_materialized_state(vps_cluster)

    master_pb7 = master_root / "pb7"
    vps_pb7 = vps_root / "pb7"
    master_pb7.mkdir()
    vps_pb7.mkdir()
    _json(vps_pb7 / "api-keys.json", {"tradfi": {"provider": "tiingo", "api_key": "remote-tradfi-secret"}})
    _write_ini(vps_root, f"[main]\nrole = vps\npb7dir = {vps_pb7}\n[coinmarketcap]\napi_key = remote-cmc-secret")
    _write_ini(master_root, f"[main]\nrole = master\npb7dir = {master_pb7}")
    monkeypatch.setattr("credential_reconciler.pb7dir", lambda: str(master_pb7))
    monkeypatch.setattr("cluster_sync_command.pb7dir", lambda: str(master_pb7))

    freeze = append_operation(
        master_cluster,
        "WRITER_FREEZE",
        {
            "freeze_generation": 1,
            "frozen": True,
            "migration_operation_id": "distributed-candidate-test",
            "source_fingerprints": {},
            "source_generations": {},
        },
    )
    write_operation(vps_cluster, freeze, network_input=True)
    rebuild_materialized_state(master_cluster)
    rebuild_materialized_state(vps_cluster)

    _append_credential_migration_acks(master_cluster)
    _append_credential_migration_acks(vps_cluster)
    master_freeze_ack = next(
        operation for operation in load_operations(master_cluster)
        if operation["actor"] == master_node and operation["op"] == "WRITER_FREEZE_ACK"
    )
    vps_freeze_ack = next(
        operation for operation in load_operations(vps_cluster)
        if operation["actor"] == REMOTE_NODE_ID and operation["op"] == "WRITER_FREEZE_ACK"
    )
    write_operation(vps_cluster, master_freeze_ack, network_input=True)
    write_operation(master_cluster, vps_freeze_ack, network_input=True)

    published = credential_migration.advance_local_credential_migration(vps_root)
    assert published["published_candidates"] == 2
    candidate_ops = [operation for operation in load_operations(vps_cluster) if operation["op"] == "MIGRATION_SECRET_CANDIDATE"]
    assert {operation["candidate_kind"] for operation in candidate_ops} == {"cmc_api_key", "tradfi_profile"}
    assert all(operation["audience"] == "masters" for operation in candidate_ops)
    assert "remote-cmc-secret" not in json.dumps(candidate_ops)
    assert "remote-tradfi-secret" not in json.dumps(candidate_ops)

    rotation = rotate_local_node_keys(master_cluster)
    master_key_update = next(
        operation for operation in reversed(load_operations(master_cluster))
        if operation["op"] == "UPDATE_NODE_KEY"
        and operation.get("encryption_key_id") == rotation["encryption_key_id"]
    )
    write_operation(vps_cluster, master_key_update, network_input=True)
    churn = credential_migration.advance_local_credential_migration(vps_root)
    assert churn["published_candidates"] == 2
    candidate_ops = [
        operation for operation in load_operations(vps_cluster)
        if operation["op"] == "MIGRATION_SECRET_CANDIDATE"
    ]
    assert len(candidate_ops) == 4
    assert len({operation["candidate_id"] for operation in candidate_ops}) == 2
    assert all(
        operation["recipient_key_ids"][master_node] == rotation["encryption_key_id"]
        for operation in candidate_ops[-2:]
    )

    def replicate(source: Path, destination: Path, actor: str, after: int) -> None:
        """Copy signed operation metadata and referenced sealed blobs between replicas."""

        for operation in load_operations(source):
            if operation["actor"] != actor or int(operation["seq"]) <= after:
                continue
            blob_hash = operation.get("sealed_blob_hash")
            if blob_hash:
                digest = str(blob_hash).removeprefix("sha256:")
                source_blob = source / "sealed_blobs" / "sha256" / digest[:2] / f"{digest}.json"
                target_blob = destination / "sealed_blobs" / "sha256" / digest[:2] / f"{digest}.json"
                target_blob.parent.mkdir(parents=True, exist_ok=True)
                target_blob.write_bytes(source_blob.read_bytes())
                assert b"remote-cmc-secret" not in source_blob.read_bytes()
                assert b"remote-tradfi-secret" not in source_blob.read_bytes()
            write_operation(destination, operation, network_input=True)

    replicate(vps_cluster, master_cluster, REMOTE_NODE_ID, 1)
    staged = credential_migration.advance_local_credential_migration(master_root)
    assert staged["accepted_candidates"] == 1
    master_store = CredentialStore(master_root / "data" / "credentials")
    assert len(master_store.list_cmc()) == 1
    assert len(master_store.list_tradfi()) == 1

    replicate(master_cluster, vps_cluster, master_node, int(freeze["seq"]))
    _materialize_credentials(vps_cluster, write=True)
    last_candidate_seq = max(int(operation["seq"]) for operation in candidate_ops)
    replicate(vps_cluster, master_cluster, REMOTE_NODE_ID, last_candidate_seq)
    accepted = credential_migration.advance_local_credential_migration(master_root)
    assert accepted["accepted_candidates"] == 1
    materialized = rebuild_materialized_state(master_cluster, write=False)
    acceptances = materialized["desired_state"]["credential_migration"]["candidate_acceptances"]
    assert set(acceptances) == {operation["candidate_id"] for operation in candidate_ops}

    last_master_seq = max(
        int(operation["seq"])
        for operation in load_operations(vps_cluster)
        if operation["actor"] == master_node
    )
    replicate(master_cluster, vps_cluster, master_node, last_master_seq)
    current = rebuild_materialized_state(master_cluster, write=False)
    cutoff = append_operation(
        master_cluster,
        "CREDENTIAL_CUTOFF",
        {
            "cutoff_generation": 1,
            "parent_generation": 0,
            "state_vector": current["state_vector"],
            "min_protocol": 2,
            "obsolete_secret_blob_hashes": [],
        },
    )
    replicate(master_cluster, vps_cluster, master_node, int(cutoff["seq"]) - 1)

    first_cleanup = credential_migration.advance_local_credential_migration(vps_root)
    cleaned = credential_migration.advance_local_credential_migration(vps_root)
    assert first_cleanup["cleaned_sources"] + cleaned["cleaned_sources"] == 2
    assert "remote-cmc-secret" not in (vps_root / "pbgui.ini").read_text(encoding="utf-8")
    assert "tradfi" not in json.loads((vps_pb7 / "api-keys.json").read_text(encoding="utf-8"))

    log_path = vps_root / "data" / "logs" / "post-cleanup.log"
    task_path = vps_root / "data" / "ohlcv" / "_tasks" / "post-cleanup.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    task_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("remote-cmc-secret", encoding="utf-8")
    task_path.write_text(json.dumps({"payload": {"token": "remote-tradfi-secret"}}), encoding="utf-8")

    findings = credential_migration.local_managed_credential_scan(vps_root)
    rendered = json.dumps(findings)
    assert "data/logs/post-cleanup.log:managed-log" in findings
    assert "data/ohlcv/_tasks/post-cleanup.json:$.payload.token" in findings
    assert "data/credentials" not in rendered
    assert "remote-local" not in rendered
    assert "remote-cmc-secret" not in rendered
    assert "remote-tradfi-secret" not in rendered
